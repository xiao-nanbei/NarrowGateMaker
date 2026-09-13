#!/usr/bin/env python3
"""Audit historical Binance aggTrades against the live receive-time stream.

Binance USD-M exposes ``aggTrade`` as the supported public trade stream.  The
historical individual-trade file is therefore exchange-time outcome truth,
not a second live information source.  This audit proves that the historical
aggregate parent used to reveal child outcomes matches what the strategy saw
online before any aggregate-reproducible taker-flow feature is promoted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.families.f08_side_taker_lifecycle.audit.binance_trade_mapping import normalize_aggtrades
from research.system_engineering.audit.receive_time_tape import expand_inputs, iter_rows

SCHEMA_VERSION = "historical_live_aggtrade_parity.v3"
EXPERIMENT_ID = "historical_live_aggtrade_parity_v3"
LIVE_SOURCE_CONTRACT_ID = "binance_usdm_public_aggtrade_receive_time.v1"
VISION_PARENT_CONTRACT_ID = "binance_usdm_vision_aggtrade_exchange_time.v1"
REST_PARENT_CONTRACT_ID = "binance_usdm_rest_aggtrade_same_day.v1"
SUPPORTED_PARENT_CONTRACT_IDS = frozenset(
    {VISION_PARENT_CONTRACT_ID, REST_PARENT_CONTRACT_ID}
)

POLICY_FEATURES = (
    "aggregate_message_count",
    "aggregate_quantity",
    "aggregate_quote_notional",
    "aggregate_aggressor_side",
    "aggregate_price_path",
    "aggregate_receive_interarrival",
    "aggregate_side_run",
    "individual_count_from_f_l_range",
)

HISTORICAL_PARENT_EXTENSION_SENSITIVE_FEATURES = (
    "aggregate_quantity",
    "aggregate_quote_notional",
    "individual_count_from_f_l_range",
)

DIAGNOSTIC_ONLY_FEATURES = (
    "individual_receive_timestamp",
    "within_aggregate_child_interarrival",
    "within_aggregate_child_receive_order",
    "within_aggregate_child_feature_ready_time",
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _int_value(row: dict[str, Any], *names: str) -> int | None:
    for name in names:
        value = row.get(name)
        if value is None or value == "":
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


def _float_value(row: dict[str, Any], *names: str) -> float:
    for name in names:
        value = row.get(name)
        if value is None or value == "":
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return math.nan
        return numeric if math.isfinite(numeric) else math.nan
    return math.nan


def _live_aggregate_rows(
    rows: Iterable[dict[str, Any]],
    *,
    market_id: str,
) -> pd.DataFrame:
    output: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("market_id", "")) != market_id:
            continue
        if str(row.get("event_type", "")).lower() != "trade":
            continue
        aggregate_id = _int_value(row, "aggregate_trade_id", "trade_id")
        if aggregate_id is None:
            continue
        stream_type = str(row.get("trade_stream_type", "")).strip().lower()
        payload_schema = str(row.get("trade_payload_schema_version", ""))
        source_contract = str(row.get("trade_source_contract_id", ""))
        first_id = _int_value(row, "first_trade_id")
        last_id = _int_value(row, "last_trade_id")
        output.append(
            {
                "agg_trade_id": aggregate_id,
                "live_price": _float_value(row, "price"),
                "live_quantity": _float_value(row, "size", "quantity"),
                "live_normal_quantity": _float_value(row, "normal_quantity"),
                "live_aggressor_side": str(row.get("aggressor_side", "")).lower(),
                "live_exchange_ts_ns": _int_value(row, "exchange_event_ts_ns") or 0,
                "live_receive_ts_ns": _int_value(row, "local_receive_ts_ns") or 0,
                "live_feature_ready_ts_ns": _int_value(
                    row, "feature_ready_ts_ns"
                )
                or 0,
                "live_sequence_number": _int_value(row, "sequence_number"),
                "live_first_trade_id": first_id,
                "live_last_trade_id": last_id,
                "live_child_count": _int_value(
                    row, "individual_trade_count_from_id_range"
                ),
                "live_stream_type": stream_type,
                "live_payload_schema": payload_schema,
                "live_source_contract": source_contract,
            }
        )
    return pd.DataFrame(output)


def audit_historical_live_aggtrade_parity(
    historical: pd.DataFrame,
    live_rows: Iterable[dict[str, Any]],
    *,
    market_id: str = "binance:perp:BTCUSDC",
    min_matched_aggregates: int = 1_000,
    quantity_abs_tolerance: float = 1e-10,
    historical_parent_contract_id: str = VISION_PARENT_CONTRACT_ID,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Compare canonical same-day aggTrades with a live capture window."""

    if historical_parent_contract_id not in SUPPORTED_PARENT_CONTRACT_IDS:
        raise ValueError(
            "unsupported historical parent contract "
            f"{historical_parent_contract_id!r}"
        )

    history = normalize_aggtrades(historical).rename(
        columns={
            "price": "historical_price",
            "quantity": "historical_quantity",
            "normal_quantity": "historical_normal_quantity",
            "first_trade_id": "historical_first_trade_id",
            "last_trade_id": "historical_last_trade_id",
            "exchange_ts_ms": "historical_exchange_ts_ms",
            "is_buyer_maker": "historical_is_buyer_maker",
        }
    )
    live = _live_aggregate_rows(live_rows, market_id=market_id)
    if live.empty:
        raise ValueError(f"live tape has no aggregate trade rows for {market_id}")
    for column in (
        "live_first_trade_id",
        "live_last_trade_id",
        "live_child_count",
    ):
        live[column] = pd.to_numeric(live[column], errors="coerce")

    duplicate_live = int(live["agg_trade_id"].duplicated().sum())
    duplicate_history = int(history["agg_trade_id"].duplicated().sum())
    live_unique = live.drop_duplicates("agg_trade_id", keep="first")
    joined = live_unique.merge(
        history,
        on="agg_trade_id",
        how="left",
        validate="one_to_one",
        indicator=True,
    )
    matched = joined["_merge"].eq("both")
    historical_side = np.where(
        joined["historical_is_buyer_maker"].fillna(False).astype(bool),
        "sell",
        "buy",
    )
    joined["price_match"] = matched & np.isclose(
        joined["live_price"],
        joined["historical_price"],
        rtol=0.0,
        atol=1e-12,
        equal_nan=False,
    )
    joined["quantity_match"] = matched & np.isclose(
        joined["live_quantity"],
        joined["historical_quantity"],
        rtol=1e-9,
        atol=float(quantity_abs_tolerance),
        equal_nan=False,
    )
    joined["aggressor_side_match"] = matched & joined[
        "live_aggressor_side"
    ].eq(historical_side)
    joined["exchange_timestamp_match"] = matched & joined[
        "live_exchange_ts_ns"
    ].eq(joined["historical_exchange_ts_ms"] * 1_000_000)
    joined["first_trade_id_match"] = matched & joined[
        "live_first_trade_id"
    ].eq(joined["historical_first_trade_id"])
    joined["last_trade_id_match"] = matched & joined[
        "live_last_trade_id"
    ].eq(joined["historical_last_trade_id"])
    expected_child_count = (
        joined["historical_last_trade_id"]
        - joined["historical_first_trade_id"]
        + 1
    )
    joined["child_count_match"] = matched & joined["live_child_count"].eq(
        expected_child_count
    )
    live_child_count = (
        joined["live_last_trade_id"] - joined["live_first_trade_id"] + 1
    )
    joined["live_internal_lineage_valid"] = (
        joined["live_first_trade_id"].notna()
        & joined["live_last_trade_id"].ge(joined["live_first_trade_id"])
        & joined["live_child_count"].eq(live_child_count)
    )
    joined["canonical_parent_prefix_match"] = (
        matched
        & joined["first_trade_id_match"]
        & joined["live_internal_lineage_valid"]
        & joined["live_last_trade_id"].le(joined["historical_last_trade_id"])
        & joined["live_quantity"].le(
            joined["historical_quantity"] + float(quantity_abs_tolerance)
        )
    )
    joined["causal_live_clock"] = (
        joined["live_exchange_ts_ns"].gt(0)
        & joined["live_receive_ts_ns"].ge(joined["live_exchange_ts_ns"])
        & joined["live_feature_ready_ts_ns"].ge(joined["live_receive_ts_ns"])
    )
    joined["aggregate_sequence_match"] = joined[
        "live_sequence_number"
    ].eq(joined["agg_trade_id"])
    joined["live_contract_match"] = (
        joined["live_stream_type"].eq("aggregate")
        & joined["live_payload_schema"].eq("binance_usdm_aggtrade.v2")
        & joined["live_source_contract"].eq(LIVE_SOURCE_CONTRACT_ID)
    )
    joined["lineage_match"] = (
        joined["first_trade_id_match"]
        & joined["last_trade_id_match"]
        & joined["child_count_match"]
    )
    joined["exact_parent_payload_match"] = (
        joined["lineage_match"] & joined["quantity_match"]
    )
    joined["canonical_parent_extension"] = (
        joined["canonical_parent_prefix_match"]
        & ~joined["exact_parent_payload_match"]
    )
    joined["canonical_quantity_extension"] = (
        joined["historical_quantity"] - joined["live_quantity"]
    ).where(joined["canonical_parent_extension"])
    joined["canonical_child_extension"] = (
        joined["historical_last_trade_id"] - joined["live_last_trade_id"]
    ).where(joined["canonical_parent_extension"])

    matched_rows = joined.loc[matched]

    def all_matched(name: str) -> bool:
        return bool(len(matched_rows)) and bool(matched_rows[name].all())

    recorder_gates = {
        "minimum_same_window_support": int(matched.sum())
        >= max(1, int(min_matched_aggregates)),
        "all_live_aggregates_found_in_history": bool(matched.all()),
        "unique_live_aggregate_ids": duplicate_live == 0,
        "unique_historical_aggregate_ids": duplicate_history == 0,
        "price_parity": all_matched("price_match"),
        "aggressor_side_parity": all_matched("aggressor_side_match"),
        "exchange_timestamp_parity": all_matched("exchange_timestamp_match"),
        "aggregate_sequence_parity": all_matched("aggregate_sequence_match"),
        "canonical_parent_prefix_compatibility": all_matched(
            "canonical_parent_prefix_match"
        ),
        "supported_live_source_contract": all_matched("live_contract_match"),
        "causal_receive_feature_clock": all_matched("causal_live_clock"),
    }
    exact_replay_gates = {
        "quantity_parity": all_matched("quantity_match"),
        "complete_f_l_lineage": all_matched("lineage_match"),
    }
    failed = sorted(
        name for name, passed in recorder_gates.items() if not passed
    )
    failed_exact_replay = sorted(
        name for name, passed in exact_replay_gates.items() if not passed
    )

    def rate(column: str) -> float | None:
        if matched_rows.empty:
            return None
        return float(matched_rows[column].mean())

    transport_ms = (
        joined.loc[matched, "live_receive_ts_ns"]
        - joined.loc[matched, "live_exchange_ts_ns"]
    ) / 1_000_000.0
    feature_us = (
        joined.loc[matched, "live_feature_ready_ts_ns"]
        - joined.loc[matched, "live_receive_ts_ns"]
    ) / 1_000.0
    extension_rows = joined.loc[
        matched & joined["canonical_parent_extension"]
    ]

    def quantile(values: pd.Series, probability: float) -> float | None:
        finite = pd.to_numeric(values, errors="coerce")
        finite = finite[np.isfinite(finite)]
        return float(finite.quantile(probability)) if len(finite) else None

    summary = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "status": "passed" if not failed else "blocked",
        "market_id": market_id,
        "source_contract": {
            "live": LIVE_SOURCE_CONTRACT_ID,
            "historical_parent": historical_parent_contract_id,
            "historical_child": "binance_usdm_vision_individual_trade_exchange_time.v1",
            "individual_receive_stream_required": False,
        },
        "rows": {
            "historical_aggregate": int(len(history)),
            "live_aggregate": int(len(live)),
            "matched_aggregate": int(matched.sum()),
            "unmatched_live_aggregate": int((~matched).sum()),
            "duplicate_live_aggregate_ids": duplicate_live,
            "duplicate_historical_aggregate_ids": duplicate_history,
        },
        "parity": {
            "price_match_rate": rate("price_match"),
            "quantity_match_rate": rate("quantity_match"),
            "aggressor_side_match_rate": rate("aggressor_side_match"),
            "exchange_timestamp_match_rate": rate("exchange_timestamp_match"),
            "lineage_match_rate": rate("lineage_match"),
            "exact_parent_payload_match_rate": rate(
                "exact_parent_payload_match"
            ),
            "canonical_parent_prefix_match_rate": rate(
                "canonical_parent_prefix_match"
            ),
            "canonical_parent_extension_count": int(len(extension_rows)),
            "canonical_parent_extension_rate": (
                float(len(extension_rows) / len(matched_rows))
                if len(matched_rows)
                else None
            ),
            "canonical_quantity_extension_max": (
                float(extension_rows["canonical_quantity_extension"].max())
                if len(extension_rows)
                else 0.0
            ),
            "canonical_child_extension_max": (
                int(extension_rows["canonical_child_extension"].max())
                if len(extension_rows)
                else 0
            ),
            "live_contract_match_rate": rate("live_contract_match"),
            "trade_to_receive_lag_semantics": (
                "live local_receive_ts_ns minus Binance trade timestamp T; "
                "this includes exchange aggregation/publication time and is "
                "not pure network latency"
            ),
            "trade_to_receive_lag_ms_p50": quantile(transport_ms, 0.50),
            "trade_to_receive_lag_ms_p95": quantile(transport_ms, 0.95),
            "transport_lag_ms_p50": quantile(transport_ms, 0.50),
            "transport_lag_ms_p95": quantile(transport_ms, 0.95),
            "feature_latency_us_p50": quantile(feature_us, 0.50),
            "feature_latency_us_p95": quantile(feature_us, 0.95),
        },
        "policy_feature_contract": {
            "live_receive_tape_eligible": list(POLICY_FEATURES),
            "historical_parent_extension_sensitive": list(
                HISTORICAL_PARENT_EXTENSION_SENSITIVE_FEATURES
            ),
            "historical_parent_exact_payload_required": not bool(
                failed_exact_replay
            ),
            "diagnostic_only": list(DIAGNOSTIC_ONLY_FEATURES),
            "visibility_rule": (
                "individual outcomes may update exchange matching/queue state at "
                "their exchange timestamps; strategy features update only when "
                "the mapped aggTrade feature_ready_ts_ns is visible"
            ),
        },
        "gates": recorder_gates,
        "failed_gates": failed,
        "historical_exact_replay_gates": exact_replay_gates,
        "failed_historical_exact_replay_gates": failed_exact_replay,
        "historical_live_aggtrade_parity_passed": not failed,
        "recorder_contract_parity_passed": not failed,
        "historical_parent_exact_payload_replay_passed": not bool(
            failed_exact_replay
        ),
        "dynamic_fill_hazard_allowed": False,
        "action_family_allowed": False,
        "next_gate": (
            "event_identity_and_riskset_v2 plus same-denominator chronological "
            "replication using aggregate-reproducible features"
        ),
    }
    return joined.drop(columns="_merge"), summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", help="Live tape files/directories/globs")
    parser.add_argument("--historical-aggtrade", type=Path, required=True)
    parser.add_argument("--market-id", default="binance:perp:BTCUSDC")
    parser.add_argument("--min-matched-aggregates", type=int, default=1_000)
    parser.add_argument(
        "--historical-parent-contract-id",
        choices=sorted(SUPPORTED_PARENT_CONTRACT_IDS),
        default=VISION_PARENT_CONTRACT_ID,
    )
    parser.add_argument("--output-rows", type=Path, required=True)
    parser.add_argument("--output-summary", type=Path, required=True)
    args = parser.parse_args()

    paths = expand_inputs(args.inputs)
    if not paths:
        parser.error("no live tape files matched")
    historical_path = args.historical_aggtrade.expanduser().resolve()
    rows, summary = audit_historical_live_aggtrade_parity(
        pd.read_csv(historical_path),
        iter_rows(paths),
        market_id=str(args.market_id),
        min_matched_aggregates=int(args.min_matched_aggregates),
        historical_parent_contract_id=str(args.historical_parent_contract_id),
    )
    output_rows = args.output_rows.expanduser().resolve()
    output_rows.parent.mkdir(parents=True, exist_ok=True)
    rows.to_parquet(output_rows, index=False)
    summary["inputs"] = {
        "historical_aggtrade": {
            "path": str(historical_path),
            "sha256": _sha256_file(historical_path),
        },
        "live_tapes": [
            {"path": str(path), "sha256": _sha256_file(path)}
            for path in paths
        ],
    }
    summary["output_rows"] = {
        "path": str(output_rows),
        "sha256": _sha256_file(output_rows),
    }
    output_summary = args.output_summary.expanduser().resolve()
    output_summary.parent.mkdir(parents=True, exist_ok=True)
    output_summary.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
