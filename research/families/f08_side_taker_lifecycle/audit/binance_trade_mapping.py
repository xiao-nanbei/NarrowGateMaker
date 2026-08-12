#!/usr/bin/env python3
"""Map Binance USD-M individual trades to their public aggTrade blocks.

Historical individual trades are exchange-time outcome truth.  They must not
become policy-visible before the corresponding live-supported ``aggTrade``
message is ready.  This module freezes and validates that bridge.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "binance_individual_aggtrade_mapping.v1"
SOURCE_CONTRACT_ID = "binance_usdm_vision_trades_to_aggtrade.v1"


def _column(frame: pd.DataFrame, *names: str) -> pd.Series:
    for name in names:
        if name in frame.columns:
            return frame[name]
    raise ValueError(f"missing required column; expected one of {names}")


def _numeric(frame: pd.DataFrame, *names: str) -> pd.Series:
    return pd.to_numeric(_column(frame, *names), errors="raise")


def _boolean(frame: pd.DataFrame, *names: str) -> pd.Series:
    values = _column(frame, *names)
    if pd.api.types.is_bool_dtype(values):
        return values.astype(bool)
    normalized = values.astype(str).str.strip().str.lower()
    valid = normalized.isin({"true", "false", "1", "0"})
    if not bool(valid.all()):
        raise ValueError("is_buyer_maker contains unrecognized values")
    return normalized.isin({"true", "1"})


def normalize_individual_trades(frame: pd.DataFrame) -> pd.DataFrame:
    """Return the canonical exchange-time individual-trade schema."""

    out = pd.DataFrame(
        {
            "trade_id": _numeric(frame, "trade_id", "id").astype(np.int64),
            "price": _numeric(frame, "price").astype(float),
            "quantity": _numeric(frame, "quantity", "qty", "size").astype(float),
            "exchange_ts_ms": _numeric(
                frame,
                "exchange_ts_ms",
                "transact_time",
                "time",
            ).astype(np.int64),
            "is_buyer_maker": _boolean(frame, "is_buyer_maker"),
        }
    )
    if out.empty:
        raise ValueError("individual trade input is empty")
    if bool(out["trade_id"].duplicated().any()):
        duplicate = int(out.loc[out["trade_id"].duplicated(), "trade_id"].iloc[0])
        raise ValueError(f"duplicate individual trade id {duplicate}")
    if bool((out["quantity"] <= 0.0).any()) or bool((out["price"] <= 0.0).any()):
        raise ValueError("individual price and quantity must be positive")
    return out.sort_values(["trade_id", "exchange_ts_ms"], kind="mergesort").reset_index(
        drop=True
    )


def normalize_aggtrades(frame: pd.DataFrame) -> pd.DataFrame:
    """Return the canonical public aggTrade schema."""

    out = pd.DataFrame(
        {
            "agg_trade_id": _numeric(
                frame,
                "agg_trade_id",
                "aggregate_trade_id",
                "a",
            ).astype(np.int64),
            "price": _numeric(frame, "price", "p").astype(float),
            "quantity": _numeric(frame, "quantity", "size", "q").astype(float),
            "first_trade_id": _numeric(frame, "first_trade_id", "f").astype(np.int64),
            "last_trade_id": _numeric(frame, "last_trade_id", "l").astype(np.int64),
            "exchange_ts_ms": _numeric(
                frame,
                "exchange_ts_ms",
                "transact_time",
                "time",
                "T",
            ).astype(np.int64),
            "is_buyer_maker": _boolean(frame, "is_buyer_maker", "m"),
        }
    )
    if "normal_quantity" in frame.columns or "nq" in frame.columns:
        out["normal_quantity"] = _numeric(
            frame,
            "normal_quantity",
            "nq",
        ).astype(float)
    else:
        out["normal_quantity"] = np.nan
    if "feature_ready_ts_ns" in frame.columns:
        out["feature_ready_ts_ns"] = pd.to_numeric(
            frame["feature_ready_ts_ns"], errors="raise"
        ).astype(np.int64)
        out["feature_ready_source"] = "explicit_feature_ready_ts_ns"
    elif "feature_ready_ts_ms" in frame.columns:
        out["feature_ready_ts_ns"] = (
            pd.to_numeric(frame["feature_ready_ts_ms"], errors="raise").astype(np.int64)
            * 1_000_000
        )
        out["feature_ready_source"] = "explicit_feature_ready_ts_ms"
    else:
        out["feature_ready_ts_ns"] = 0
        out["feature_ready_source"] = "infer_after_last_child"
    if out.empty:
        raise ValueError("aggTrade input is empty")
    if bool(out["agg_trade_id"].duplicated().any()):
        duplicate = int(out.loc[out["agg_trade_id"].duplicated(), "agg_trade_id"].iloc[0])
        raise ValueError(f"duplicate aggregate trade id {duplicate}")
    invalid = out["last_trade_id"] < out["first_trade_id"]
    if bool(invalid.any()):
        raise ValueError("aggregate trade has last_trade_id < first_trade_id")
    return out.sort_values(
        ["first_trade_id", "last_trade_id", "agg_trade_id"], kind="mergesort"
    ).reset_index(drop=True)


def build_individual_aggtrade_mapping(
    individual: pd.DataFrame,
    aggregate: pd.DataFrame,
    *,
    quantity_abs_tolerance: float = 1e-10,
    require_full_individual_coverage: bool = True,
    require_exact_trade_id_coverage: bool = False,
    feature_ready_latency_ms: float = 0.0,
    feature_ready_latency_profile_id: str = "",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Validate exact ID ranges and assign one aggTrade visibility clock.

    The returned child rows retain their individual exchange timestamp but use
    the aggregate message's feature-ready timestamp.  A mismatch fails closed.
    """

    children = normalize_individual_trades(individual)
    parents = normalize_aggtrades(aggregate)

    starts = parents["first_trade_id"].to_numpy(dtype=np.int64)
    ends = parents["last_trade_id"].to_numpy(dtype=np.int64)
    if len(parents) > 1 and bool((starts[1:] <= ends[:-1]).any()):
        raise ValueError("aggTrade ID ranges overlap or are not strictly ordered")

    trade_ids = children["trade_id"].to_numpy(dtype=np.int64)
    parent_index = np.searchsorted(starts, trade_ids, side="right") - 1
    covered = (parent_index >= 0) & (trade_ids <= ends[np.maximum(parent_index, 0)])
    if require_full_individual_coverage and not bool(covered.all()):
        missing = trade_ids[~covered]
        raise ValueError(
            "individual trades are outside aggTrade ranges; first="
            f"{int(missing[0])} count={len(missing)}"
        )

    mapped = children.loc[covered].copy()
    mapped_parent_index = parent_index[covered]
    parent_rows = parents.iloc[mapped_parent_index].reset_index(drop=True)
    mapped["agg_trade_id"] = parent_rows["agg_trade_id"].to_numpy(dtype=np.int64)
    mapped["agg_first_trade_id"] = parent_rows["first_trade_id"].to_numpy(
        dtype=np.int64
    )
    mapped["agg_last_trade_id"] = parent_rows["last_trade_id"].to_numpy(
        dtype=np.int64
    )
    mapped["agg_exchange_ts_ms"] = parent_rows["exchange_ts_ms"].to_numpy(
        dtype=np.int64
    )
    mapped["feature_ready_ts_ns"] = parent_rows["feature_ready_ts_ns"].to_numpy(
        dtype=np.int64
    )
    mapped["feature_ready_source"] = parent_rows["feature_ready_source"].to_numpy()
    mapped["trade_stream_type"] = "individual_historical_exchange_truth"
    mapped["policy_visibility_source"] = "mapped_binance_usdm_aggtrade"
    mapped["source_contract_id"] = SOURCE_CONTRACT_ID

    grouped = mapped.groupby("agg_trade_id", sort=False)
    child_stats = grouped.agg(
        mapped_individual_count=("trade_id", "size"),
        first_child_trade_id=("trade_id", "min"),
        last_child_trade_id=("trade_id", "max"),
        first_child_exchange_ts_ms=("exchange_ts_ms", "min"),
        last_child_exchange_ts_ms=("exchange_ts_ms", "max"),
        child_price_min=("price", "min"),
        child_price_max=("price", "max"),
        child_side_first=("is_buyer_maker", "first"),
        child_side_count=("is_buyer_maker", "nunique"),
        individual_quantity=("quantity", "sum"),
    ).reset_index()
    aggregate_audit = parents.rename(
        columns={
            "price": "aggregate_price",
            "quantity": "aggregate_quantity",
            "normal_quantity": "aggregate_normal_quantity",
            "exchange_ts_ms": "aggregate_exchange_ts_ms",
            "is_buyer_maker": "aggregate_is_buyer_maker",
        }
    ).merge(child_stats, on="agg_trade_id", how="left", validate="one_to_one")
    aggregate_audit["mapped_individual_count"] = aggregate_audit[
        "mapped_individual_count"
    ].fillna(0).astype(np.int64)
    aggregate_audit["expected_individual_count"] = (
        aggregate_audit["last_trade_id"]
        - aggregate_audit["first_trade_id"]
        + 1
    ).astype(np.int64)
    aggregate_audit["trade_ids_contiguous"] = (
        aggregate_audit["mapped_individual_count"].eq(
            aggregate_audit["expected_individual_count"]
        )
        & aggregate_audit["first_child_trade_id"].eq(
            aggregate_audit["first_trade_id"]
        )
        & aggregate_audit["last_child_trade_id"].eq(
            aggregate_audit["last_trade_id"]
        )
    )
    aggregate_audit["price_match"] = (
        np.isclose(
            aggregate_audit["child_price_min"],
            aggregate_audit["aggregate_price"],
            rtol=0.0,
            atol=1e-12,
        )
        & np.isclose(
            aggregate_audit["child_price_max"],
            aggregate_audit["aggregate_price"],
            rtol=0.0,
            atol=1e-12,
        )
    )
    aggregate_audit["aggressor_side_match"] = (
        aggregate_audit["child_side_count"].eq(1)
        & aggregate_audit["child_side_first"].eq(
            aggregate_audit["aggregate_is_buyer_maker"]
        )
    )
    aggregate_audit["aggregate_timestamp_in_child_span"] = (
        aggregate_audit["aggregate_exchange_ts_ms"].ge(
            aggregate_audit["first_child_exchange_ts_ms"]
        )
        & aggregate_audit["aggregate_exchange_ts_ms"].le(
            aggregate_audit["last_child_exchange_ts_ms"]
        )
    )
    inferred = aggregate_audit["feature_ready_source"].eq(
        "infer_after_last_child"
    )
    inferred_ready = (
        aggregate_audit[
            ["aggregate_exchange_ts_ms", "last_child_exchange_ts_ms"]
        ].max(axis=1)
        + float(feature_ready_latency_ms)
    ) * 1_000_000.0
    aggregate_audit.loc[inferred, "feature_ready_ts_ns"] = inferred_ready.loc[
        inferred
    ].round().astype(np.int64)
    aggregate_audit.loc[inferred, "feature_ready_source"] = (
        "last_child_plus_frozen_latency"
    )
    aggregate_audit["feature_ready_causal"] = aggregate_audit[
        "feature_ready_ts_ns"
    ].ge(aggregate_audit["last_child_exchange_ts_ms"] * 1_000_000.0)
    total_match = np.isclose(
        aggregate_audit["individual_quantity"],
        aggregate_audit["aggregate_quantity"],
        rtol=1e-9,
        atol=float(quantity_abs_tolerance),
    )
    normal_match = (
        aggregate_audit["aggregate_normal_quantity"].notna()
        & np.isclose(
            aggregate_audit["individual_quantity"],
            aggregate_audit["aggregate_normal_quantity"],
            rtol=1e-9,
            atol=float(quantity_abs_tolerance),
        )
    )
    aggregate_audit["quantity_identity"] = np.select(
        [total_match & normal_match, total_match, normal_match],
        ["q_and_nq", "q_total", "nq_normal_only"],
        default="mismatch",
    )
    aggregate_audit["mapping_valid"] = (
        aggregate_audit["price_match"]
        & aggregate_audit["aggressor_side_match"]
        & aggregate_audit["aggregate_timestamp_in_child_span"]
        & aggregate_audit["feature_ready_causal"]
        & (total_match | normal_match)
    )
    aggregate_audit["queue_outcome_exact"] = (
        aggregate_audit["mapping_valid"]
        & aggregate_audit["trade_ids_contiguous"]
    )
    ready_by_aggregate = aggregate_audit.set_index("agg_trade_id")
    mapped["feature_ready_ts_ns"] = mapped["agg_trade_id"].map(
        ready_by_aggregate["feature_ready_ts_ns"]
    ).astype(np.int64)
    mapped["feature_ready_source"] = mapped["agg_trade_id"].map(
        ready_by_aggregate["feature_ready_source"]
    )
    mapped["queue_outcome_exact"] = mapped["agg_trade_id"].map(
        ready_by_aggregate["queue_outcome_exact"]
    ).astype(bool)
    failures = [
        str(int(value))
        for value in aggregate_audit.loc[
            ~aggregate_audit["mapping_valid"], "agg_trade_id"
        ].tolist()
    ]
    if require_exact_trade_id_coverage:
        failures.extend(
            str(int(value))
            for value in aggregate_audit.loc[
                ~aggregate_audit["trade_ids_contiguous"], "agg_trade_id"
            ].tolist()
        )
        failures = sorted(set(failures), key=int)
    if failures:
        preview = ",".join(failures[:8])
        reason_counts = {
            name: int((~aggregate_audit[name].astype(bool)).sum())
            for name in (
                "trade_ids_contiguous",
                "price_match",
                "aggressor_side_match",
                "aggregate_timestamp_in_child_span",
                "feature_ready_causal",
            )
        }
        reason_counts["quantity_match"] = int(
            (~(total_match | normal_match)).sum()
        )
        raise ValueError(
            "strict individual↔aggTrade mapping failed for aggregate ids "
            f"{preview}; failures={len(failures)} reasons={reason_counts}"
        )

    summary = {
        "schema_version": SCHEMA_VERSION,
        "source_contract_id": SOURCE_CONTRACT_ID,
        "status": "passed",
        "individual_rows": int(len(children)),
        "mapped_individual_rows": int(len(mapped)),
        "aggregate_rows": int(len(parents)),
        "mapped_aggregate_rows": int(len(aggregate_audit)),
        "exact_trade_id_range_aggregate_rows": int(
            aggregate_audit["trade_ids_contiguous"].sum()
        ),
        "nonexact_trade_id_range_aggregate_rows": int(
            (~aggregate_audit["trade_ids_contiguous"]).sum()
        ),
        "queue_outcome_exact_rate": float(
            aggregate_audit["queue_outcome_exact"].mean()
        ),
        "queue_outcome_day_strict_eligible": bool(
            aggregate_audit["queue_outcome_exact"].all()
        ),
        "quantity_identity_counts": {
            str(key): int(value)
            for key, value in aggregate_audit["quantity_identity"].value_counts().items()
        },
        "feature_ready_source_counts": {
            str(key): int(value)
            for key, value in aggregate_audit["feature_ready_source"]
            .value_counts()
            .items()
        },
        "feature_ready_latency_ms": float(feature_ready_latency_ms),
        "feature_ready_latency_profile_id": str(feature_ready_latency_profile_id),
        "policy_feature_timing_eligible": bool(
            aggregate_audit["feature_ready_source"]
            .astype(str)
            .str.startswith("explicit_")
            .all()
            or str(feature_ready_latency_profile_id).strip()
        ),
        "policy_visibility_semantics": (
            "individual exchange-time outcomes; all policy features become visible "
            "at mapped aggTrade feature_ready_ts_ns"
        ),
        "rpi_contract": (
            "q includes RPI while nq excludes RPI when nq is present; strict "
            "mapping accepts only a documented q-total or nq-normal identity. "
            "An internal f..l ID gap is retained for aggregate feature timing "
            "but marked queue_outcome_exact=false."
        ),
    }
    return mapped.reset_index(drop=True), summary


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path.expanduser().resolve())


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--individual", type=Path, required=True)
    parser.add_argument("--aggtrade", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--feature-ready-latency-ms", type=float, default=0.0)
    parser.add_argument("--feature-ready-latency-profile-id", default="")
    parser.add_argument("--require-exact-trade-id-coverage", action="store_true")
    args = parser.parse_args()

    individual_path = args.individual.expanduser().resolve()
    aggregate_path = args.aggtrade.expanduser().resolve()
    mapped, summary = build_individual_aggtrade_mapping(
        _read_csv(individual_path),
        _read_csv(aggregate_path),
        feature_ready_latency_ms=float(args.feature_ready_latency_ms),
        feature_ready_latency_profile_id=str(
            args.feature_ready_latency_profile_id
        ),
        require_exact_trade_id_coverage=bool(
            args.require_exact_trade_id_coverage
        ),
    )
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    mapped.to_parquet(output, index=False)
    summary["inputs"] = {
        "individual": {
            "path": str(individual_path),
            "sha256": _sha256_file(individual_path),
        },
        "aggregate": {
            "path": str(aggregate_path),
            "sha256": _sha256_file(aggregate_path),
        },
    }
    summary["output"] = {
        "path": str(output),
        "sha256": _sha256_file(output),
    }
    summary_path = args.summary.expanduser().resolve()
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
