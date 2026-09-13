#!/usr/bin/env python3
"""Build order-value and complete lifecycle panels from Python tick replay.

This command is observation-only. It does not enable K0/K1, external market
features, or any other randomized action family. Each order is recorded once
when it becomes active.  A separate transition log preserves submit through
terminal order state plus the later campaign-repair path.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import backtest_tick as bt  # noqa: E402
from research.families.f01_fixed_parameter_racing import daily_smoke_sweep as smoke  # noqa: E402
from models.active_order_queue import (  # noqa: E402
    load_active_order_queue_data,
)
from research.families.f08_side_taker_lifecycle.audit.event_identity_and_riskset_v2 import (  # noqa: E402
    LABEL_IDENTITY as LIFECYCLE_LABEL_IDENTITY,
)
from research.families.f08_side_taker_lifecycle.audit.event_identity_and_riskset_v2 import (  # noqa: E402
    aggregate_lifecycle_audits,
    audit_lifecycle_events,
)
from models.audit.evidence_split import load_evidence_panel  # noqa: E402
from models.audit.experiment_manifest import (  # noqa: E402
    build_manifest,
    git_workspace_identity,
    write_code_checkpoint,
    write_manifest,
)
from research.families.f07_active_order_continuation.audit.local_order_value_panel import (  # noqa: E402
    COMPETING_RISK_LABEL_IDENTITY,
    add_competing_risk_labels,
    add_native_first_mid_hit_labels,
    write_panel,
)
from models.audit.order_lifecycle import (  # noqa: E402
    DEFAULT_RISK_SNAPSHOT_EDGES_MS,
    INTERVAL_SCHEMA_VERSION,
)
from models.audit.order_lifecycle import (  # noqa: E402
    SCHEMA_VERSION as LIFECYCLE_SCHEMA_VERSION,
)
from research.families.f07_active_order_continuation.audit.queue_value_models import (  # noqa: E402
    fit_empirical_microprice,
    fit_queue_reactive_hawkes,
)
from models.backtest_config import (  # noqa: E402
    load_tick_base_params,
    validate_formal_replay_calibration,
)
from models.data_windows import load_tick_window_dict  # noqa: E402
from models.exchange_book_replay import (  # noqa: E402
    CryptoHFTExchangeBookTape,
)

SCHEMA_VERSION = "local_order_value_replay.v6"
HISTORICAL_BOOK_VISIBILITY = (
    "exchange_time_asof_le_ideal_latency_diagnostic"
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _formal_quality_context_identity(
    manifest_path: Path | None,
    expected_sha256: str,
    *,
    target_days: Sequence[str],
    warmup_days: int,
) -> tuple[tuple[str, ...], dict[str, Any]]:
    if manifest_path is None:
        if expected_sha256:
            raise ValueError(
                "formal quality manifest SHA was supplied without a manifest"
            )
        return (), {"enabled": False}
    if not expected_sha256:
        raise ValueError(
            "--formal-quality-day-manifest-sha256 is required with the manifest"
        )
    resolved = manifest_path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"formal quality day manifest not found: {resolved}")
    actual_sha256 = _sha256(resolved)
    if actual_sha256 != expected_sha256:
        raise ValueError(
            "formal quality day manifest identity changed: "
            f"expected={expected_sha256} actual={actual_sha256}"
        )
    frame = pd.read_csv(resolved, dtype={"day": str})
    if "day" not in frame.columns:
        raise ValueError("formal quality day manifest must contain a day column")
    manifest_days = frozenset(str(day)[:10] for day in frame["day"])
    missing = sorted(set(target_days) - manifest_days)
    if missing:
        raise ValueError(
            "requested replay days are absent from the frozen formal manifest: "
            + ",".join(missing)
        )

    context_days: set[str] = set()
    for day in target_days:
        target = pd.Timestamp(day, tz="UTC")
        for offset in range(max(0, int(warmup_days)), -1, -1):
            context_days.add(
                (target - pd.Timedelta(days=offset)).strftime("%Y-%m-%d")
            )
    allowed = tuple(sorted(context_days))
    return allowed, {
        "enabled": True,
        "path": str(resolved),
        "sha256": actual_sha256,
        "target_days": list(target_days),
        "allowed_context_days": list(allowed),
        "warmup_days": int(warmup_days),
        "authority": "frozen_native_strict_manifest_with_gap_censoring",
    }


def _align_table_schema(
    table: pa.Table,
    schema: pa.Schema,
    *,
    context: str,
) -> pa.Table:
    if table.schema == schema:
        return table
    if set(table.schema.names) != set(schema.names):
        missing = sorted(set(schema.names) - set(table.schema.names))
        extra = sorted(set(table.schema.names) - set(schema.names))
        raise ValueError(
            f"{context} schema fields differ: "
            f"missing={missing} extra={extra}"
        )
    return table.select(schema.names).cast(schema)


def _stream_concat_parquet(paths: Sequence[Path], output: Path) -> None:
    writer: pq.ParquetWriter | None = None
    schema: pa.Schema | None = None
    try:
        for path in paths:
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(batch_size=100_000):
                table = pa.Table.from_batches([batch])
                if writer is None:
                    schema = table.schema
                    writer = pq.ParquetWriter(
                        output,
                        schema,
                        compression="zstd",
                    )
                elif table.schema != schema:
                    table = _align_table_schema(
                        table,
                        schema,
                        context="lifecycle parquet",
                    )
                writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise ValueError("no parquet rows were available to concatenate")


def _active_order_queue_identity(
    directory_template: str,
    days: Sequence[str],
) -> list[dict[str, Any]]:
    if not directory_template:
        return []
    identities = []
    for day in days:
        directory = (
            Path(directory_template.format(day=day))
            .expanduser()
            .resolve()
        )
        files = {}
        for name in (
            "summary.json",
            "sequence_audit.json",
            "seeds.parquet",
            "level_events.parquet",
        ):
            path = directory / name
            if not path.is_file():
                raise FileNotFoundError(
                    f"active-order queue artifact missing: {path}"
                )
            files[name] = {
                "path": str(path),
                "sha256": _sha256(path),
                "size_bytes": int(path.stat().st_size),
            }
        identities.append(
            {
                "day": day,
                "directory": str(directory),
                "files": files,
            }
        )
    return identities


def _exchange_book_identity(
    raw_root: Path | None,
    days: Sequence[str],
    *,
    symbol: str,
    exchange: str,
    tick_size: float,
    warmup_hours: int,
    strict_complete: bool,
) -> list[dict[str, Any]]:
    if raw_root is None:
        return []
    identities = []
    for day in days:
        tape = CryptoHFTExchangeBookTape(
            raw_root=raw_root,
            day=day,
            symbol=symbol,
            exchange=exchange,
            tick_size=tick_size,
            warmup_hours=warmup_hours,
            strict_complete=strict_complete,
        )
        identities.append(tape.identity(include_sha256=False))
    return identities


def build_watch_manifest(
    quote_rows: pd.DataFrame,
    *,
    tick_size: float,
    trajectory_id: str = "baseline_discovery",
) -> pd.DataFrame:
    """Collapse completed quote traces into strict active-order intervals."""

    if quote_rows.empty:
        raise ValueError("quote trace produced no watchable order lifetimes")
    if tick_size <= 0.0:
        raise ValueError("tick_size must be positive")
    trajectory_id = str(trajectory_id).strip()
    if not trajectory_id:
        raise ValueError("trajectory_id must be non-empty")
    required = {
        "day",
        "order_id",
        "side",
        "price",
        "submit_ts",
        "activate_ts",
        "outcome_ts",
        "outcome",
    }
    missing = sorted(required - set(quote_rows.columns))
    if missing:
        raise ValueError(
            f"quote trace is missing watch fields: {missing}"
        )

    source = quote_rows.copy()
    for column in (
        "order_id",
        "submit_ts",
        "activate_ts",
        "outcome_ts",
    ):
        source[column] = pd.to_numeric(
            source[column],
            errors="coerce",
        )
    source["price"] = pd.to_numeric(source["price"], errors="coerce")
    source["side"] = source["side"].astype(str).str.upper()
    source = source[
        source["day"].notna()
        & source["order_id"].notna()
        & source["side"].isin(("BUY", "SELL"))
        & source["price"].gt(0.0)
        & source["activate_ts"].notna()
        & source["outcome_ts"].notna()
    ].copy()
    if source.empty:
        raise ValueError("quote trace has no valid active-order rows")

    rows: list[dict[str, Any]] = []
    group_columns = ["day", "order_id"]
    for (day, order_id), group in source.groupby(
        group_columns,
        sort=True,
        observed=True,
    ):
        ordered = group.sort_values(
            ["activate_ts", "outcome_ts"],
            kind="stable",
        )
        first = ordered.iloc[0]
        side_values = set(ordered["side"].astype(str))
        price_values = pd.to_numeric(
            ordered["price"],
            errors="coerce",
        ).dropna()
        if len(side_values) != 1 or price_values.empty:
            raise ValueError(
                f"{day}/{int(order_id)} has inconsistent quote identity"
            )
        price = float(price_values.iloc[0])
        if not bool(
            np.allclose(
                price_values.to_numpy(dtype=float),
                price,
                rtol=0.0,
                atol=tick_size * 0.01,
            )
        ):
            raise ValueError(
                f"{day}/{int(order_id)} changes price within one order"
            )
        activate_ts = int(first["activate_ts"])
        stop_ts = int(ordered["outcome_ts"].max())
        if stop_ts <= activate_ts:
            continue
        last = ordered.sort_values("outcome_ts", kind="stable").iloc[-1]
        price_tick = int(round(price / tick_size))
        if not np.isclose(
            price_tick * tick_size,
            price,
            rtol=0.0,
            atol=tick_size * 0.01,
        ):
            raise ValueError(
                f"{day}/{int(order_id)} price is not tick aligned: {price}"
            )
        rows.append(
            {
                "schema_version": "active_order_watch_manifest.v1",
                "trajectory_id": trajectory_id,
                "watch_id": (
                    f"{trajectory_id}:{day}:{int(order_id)}"
                ),
                "day": str(day),
                "order_id": int(order_id),
                "side": str(first["side"]),
                "price": price,
                "price_tick": price_tick,
                "submit_ts_ms": int(first["submit_ts"]),
                "activate_ts_ms": activate_ts,
                "stop_ts_ms": stop_ts,
                "stop_reason": str(last.get("outcome", "")),
                "cancel_reason": str(last.get("cancel_reason", "")),
                "campaign_id": int(
                    first.get("campaign_id_at_submit", 0) or 0
                ),
                "inventory_role": str(
                    first.get("inventory_role_at_submit", "")
                ),
                "reduce_only": bool(first.get("reduce_only", False)),
                "source_row_count": int(len(ordered)),
            }
        )
    watch = pd.DataFrame(rows)
    if watch.empty:
        raise ValueError("quote trace has no positive active intervals")
    if not watch["watch_id"].is_unique:
        raise ValueError("watch_id must be unique")
    if not bool(
        (watch["activate_ts_ms"] < watch["stop_ts_ms"]).all()
    ):
        raise ValueError("watch intervals must have positive duration")
    return watch.sort_values(
        ["day", "activate_ts_ms", "order_id"],
        kind="stable",
    ).reset_index(drop=True)


def _run_day(task: tuple[str, str, dict[str, Any]]) -> dict[str, Any]:
    day, symbol, raw_params = task
    params = dict(raw_params)
    model_dir = params.get("resolved_model_dir") or params.get("model_dir")
    bt.configure_symbol(symbol, model_dir_override=model_dir)
    if params.get("_historical_bbo_dir"):
        bt.BBO_DIR = Path(str(params["_historical_bbo_dir"])).resolve()
    if params.get("_historical_l2_dir"):
        bt.L2_DIR = Path(str(params["_historical_l2_dir"])).resolve()
    active_order_queue_data = None
    queue_source = str(params.get("_active_order_queue_dir", "") or "")
    if queue_source:
        queue_dir = Path(queue_source.format(day=day)).expanduser().resolve()
        active_order_queue_data = load_active_order_queue_data(
            queue_dir,
            expected_day=day,
            expected_symbol=symbol,
            expected_tick_size=float(
                params.get("_active_order_queue_tick_size", 0.1)
            ),
        )
    exchange_book_event_tape = None
    exchange_book_raw_root = str(
        params.get("_exchange_book_raw_root", "") or ""
    )
    if exchange_book_raw_root:
        exchange_book_event_tape = CryptoHFTExchangeBookTape(
            raw_root=Path(exchange_book_raw_root),
            day=day,
            symbol=symbol,
            tick_size=float(
                params.get("_exchange_book_tick_size", 0.1)
            ),
            exchange=str(
                params.get(
                    "_exchange_book_exchange",
                    "binance_futures",
                )
            ),
            warmup_hours=int(
                params.get("_exchange_book_warmup_hours", 24)
            ),
            strict_complete=bool(
                params.get("_exchange_book_strict_complete", True)
            ),
        )
    started = time.perf_counter()
    ml_inference_enabled = bool(params.get("ml_enabled", True))
    feature_context_required = bool(
        ml_inference_enabled
        or params.get("buy_fill_selection_live_enabled", False)
    )
    window = load_tick_window_dict(
        day,
        params,
        load_ml=feature_context_required,
        require_ml=feature_context_required,
        run_ml_inference=ml_inference_enabled,
        feature_dir=params.get("_feature_context_dir"),
        require_target_feature_files=feature_context_required,
        cross_market_enabled=True,
        require_historical_bbo=True,
        cache_dir=params.get("_window_cache_dir"),
        refresh_cache=bool(params.get("_refresh_window_cache", False)),
    )
    result = bt._simulate_tick_with_engine(
        "python",
        window["trades"],
        window["var_ts_ms"],
        window["var_ssq"],
        params,
        ml_data=window["ml_data"],
        bbo_data=window["bbo_data"],
        l2_data=window["l2_data"],
        var_ti=window["var_ti"],
        var_retsq=window["var_retsq"],
        active_order_queue_data=active_order_queue_data,
        exchange_book_event_tape=exchange_book_event_tape,
    )
    rows = [
        {"day": day, **row}
        for row in result.get("_local_order_value_trace", [])
    ]
    lifecycle_rows = [
        {"day": day, **row}
        for row in result.get("_local_order_lifecycle_trace", [])
    ]
    quote_rows = [
        {"day": day, **row}
        for row in result.get("_quote_trace", [])
    ]
    decision_rows = [
        {"day": day, **row}
        for row in result.get("_decision_trace", [])
    ]
    queue_missing_rows = [
        {"day": day, "queue_source": "sparse_watch", **row}
        for row in result.get("_active_order_queue_missing_trace", [])
    ]
    queue_missing_rows.extend(
        {
            "day": day,
            "queue_source": "native_exchange_book",
            **row,
        }
        for row in result.get("_exchange_book_queue_missing_trace", [])
    )
    return {
        "day": day,
        "rows": rows,
        "lifecycle_rows": lifecycle_rows,
        "quote_rows": quote_rows,
        "decision_rows": decision_rows,
        "queue_missing_rows": queue_missing_rows,
        "daily": {
            "day": day,
            "rows": len(rows),
            "lifecycle_rows": len(lifecycle_rows),
            "quote_rows": len(quote_rows),
            "decision_rows": len(decision_rows),
            "fills": int(result.get("fills_total", 0) or 0),
            "placed": int(result.get("decision_place_count", 0) or 0),
            "campaigns": int(result.get("campaign_count", 0) or 0),
            "active_order_queue_mode": str(
                result.get("active_order_queue_mode", "disabled")
            ),
            "active_order_queue_scope": str(
                result.get("active_order_queue_scope", "seed_only_v1")
            ),
            "active_order_queue_lookup_count": int(
                result.get("active_order_queue_lookup_count", 0) or 0
            ),
            "active_order_queue_exact_count": int(
                result.get("active_order_queue_exact_count", 0) or 0
            ),
            "active_order_queue_known_zero_count": int(
                result.get("active_order_queue_known_zero_count", 0) or 0
            ),
            "active_order_queue_missing_count": int(
                result.get("active_order_queue_missing_count", 0) or 0
            ),
            "active_order_queue_unusable_count": int(
                result.get("active_order_queue_unusable_count", 0) or 0
            ),
            "exchange_book_queue_mode": str(
                result.get("exchange_book_queue_mode", "disabled")
            ),
            "exchange_book_queue_scope": str(
                result.get("exchange_book_queue_scope", "disabled")
            ),
            "exchange_book_queue_lookup_count": int(
                result.get("exchange_book_queue_lookup_count", 0) or 0
            ),
            "exchange_book_queue_exact_count": int(
                result.get("exchange_book_queue_exact_count", 0) or 0
            ),
            "exchange_book_queue_known_zero_count": int(
                result.get(
                    "exchange_book_queue_known_zero_count",
                    0,
                )
                or 0
            ),
            "exchange_book_queue_missing_count": int(
                result.get("exchange_book_queue_missing_count", 0) or 0
            ),
            "exchange_book_events_consumed": int(
                result.get("exchange_book_events_consumed", 0) or 0
            ),
            "exchange_book_events_accepted": int(
                result.get("exchange_book_events_accepted", 0) or 0
            ),
            "exchange_book_events_rejected": int(
                result.get("exchange_book_events_rejected", 0) or 0
            ),
            "exchange_book_source_gap_events": int(
                result.get("exchange_book_source_gap_events", 0) or 0
            ),
            "exchange_book_invalid_sequence_messages": int(
                result.get(
                    "exchange_book_invalid_sequence_messages",
                    0,
                )
                or 0
            ),
            "exchange_book_sequence_gaps": int(
                result.get("exchange_book_sequence_gaps", 0) or 0
            ),
            "exchange_book_message_time_reversals": int(
                result.get(
                    "exchange_book_message_time_reversals",
                    0,
                )
                or 0
            ),
            "exchange_book_snapshot_events": int(
                result.get("exchange_book_snapshot_events", 0) or 0
            ),
            "exchange_book_delta_events": int(
                result.get("exchange_book_delta_events", 0) or 0
            ),
            "exchange_book_delta_bootstrap_events": int(
                result.get(
                    "exchange_book_delta_bootstrap_events",
                    0,
                )
                or 0
            ),
            "exchange_book_queue_invalidated_order_count": int(
                result.get(
                    "exchange_book_queue_invalidated_order_count",
                    0,
                )
                or 0
            ),
            "exchange_book_queue_ambiguous_event_count": int(
                result.get(
                    "exchange_book_queue_ambiguous_event_count",
                    0,
                )
                or 0
            ),
            "exchange_book_cancel_trade_ambiguous_order_count": int(
                result.get(
                    "exchange_book_cancel_trade_ambiguous_order_count",
                    0,
                )
                or 0
            ),
            "exchange_book_cancel_book_ambiguous_order_count": int(
                result.get(
                    "exchange_book_cancel_book_ambiguous_order_count",
                    0,
                )
                or 0
            ),
            "exchange_book_transaction_timestamp_events": int(
                result.get(
                    "exchange_book_transaction_timestamp_events",
                    0,
                )
                or 0
            ),
            "exchange_book_event_timestamp_fallback_events": int(
                result.get(
                    "exchange_book_event_timestamp_fallback_events",
                    0,
                )
                or 0
            ),
            "exchange_book_receive_timestamp_fallback_events": int(
                result.get(
                    "exchange_book_receive_timestamp_fallback_events",
                    0,
                )
                or 0
            ),
            "exchange_book_unknown_timestamp_source_events": int(
                result.get(
                    "exchange_book_unknown_timestamp_source_events",
                    0,
                )
                or 0
            ),
            "exchange_book_queue_cancel_ahead_event_count": int(
                result.get(
                    "exchange_book_queue_cancel_ahead_event_count",
                    0,
                )
                or 0
            ),
            "exchange_book_queue_cancel_ahead_qty": float(
                result.get(
                    "exchange_book_queue_cancel_ahead_qty",
                    0.0,
                )
                or 0.0
            ),
            "runtime_s": time.perf_counter() - started,
        },
    }


def _atomic_write_parquet_rows(
    rows: list[dict[str, Any]],
    path: Path,
) -> None:
    _atomic_write_parquet_frame(pd.DataFrame(rows), path)


def _atomic_write_parquet_frame(
    frame: pd.DataFrame,
    path: Path,
) -> None:
    temporary = path.with_name(
        f".{path.stem}.tmp.{os.getpid()}{path.suffix}"
    )
    try:
        frame.to_parquet(temporary, index=False)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_write_json(payload: dict[str, Any], path: Path) -> None:
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _run_lifecycle_day_to_partition(
    task: tuple[str, str, dict[str, Any], str],
) -> dict[str, Any]:
    """Run one day and persist the large lifecycle payload in its worker."""

    day, symbol, params, partial_dir_raw = task
    partial_dir = Path(partial_dir_raw)
    item = _run_day((day, symbol, params))
    lifecycle_path = partial_dir / f"{day}.lifecycle.parquet"
    _atomic_write_parquet_rows(item["lifecycle_rows"], lifecycle_path)
    queue_missing_rows = item.get("queue_missing_rows", [])
    if queue_missing_rows:
        _atomic_write_parquet_rows(
            queue_missing_rows,
            partial_dir / f"{day}.queue_missing.parquet",
        )
    _atomic_write_json(item["daily"], partial_dir / f"{day}.daily.json")
    return {
        "day": day,
        "rows": [],
        "lifecycle_rows": [],
        "lifecycle_path": str(lifecycle_path),
        "quote_rows": [],
        "queue_missing_rows": queue_missing_rows,
        "daily": item["daily"],
    }


def _audit_lifecycle_partition_to_disk(
    task: tuple[str, str, str, bool],
) -> dict[str, Any]:
    """Build one day's risk intervals without retaining prior-day memory."""

    day, lifecycle_path_raw, partial_dir_raw, require_native_book = task
    lifecycle_path = Path(lifecycle_path_raw)
    partial_dir = Path(partial_dir_raw)
    risk_path = partial_dir / f"{day}.risk_intervals.parquet"
    audit_path = partial_dir / f"{day}.lifecycle_audit.json"
    lifecycle_sha256 = _sha256(lifecycle_path)
    if risk_path.is_file() and audit_path.is_file():
        audit = json.loads(audit_path.read_text(encoding="utf-8"))
        if str(audit.get("lifecycle_sha256", "")) == lifecycle_sha256:
            return {
                "day": day,
                "risk_path": str(risk_path),
                "audit_path": str(audit_path),
                "audit": audit,
                "reused": True,
            }

    lifecycle = pd.read_parquet(lifecycle_path)
    intervals, audit = audit_lifecycle_events(
        lifecycle,
        require_native_book=bool(require_native_book),
    )
    audit["partition_day"] = str(day)
    audit["lifecycle_path"] = str(lifecycle_path)
    audit["lifecycle_sha256"] = lifecycle_sha256
    _atomic_write_parquet_frame(intervals, risk_path)
    _atomic_write_json(audit, audit_path)
    return {
        "day": day,
        "risk_path": str(risk_path),
        "audit_path": str(audit_path),
        "audit": audit,
        "reused": False,
    }


def _read_days(args: argparse.Namespace) -> tuple[list[str], dict[str, Any]]:
    identity: dict[str, Any] = {}
    if args.evidence_split_manifest is not None:
        if args.days or args.days_file is not None:
            raise SystemExit(
                "--evidence-split-manifest cannot be combined with "
                "--days/--days-file"
            )
        if args.evidence_panel is None:
            raise SystemExit("--evidence-panel is required with a frozen split")
        days, identity = load_evidence_panel(
            args.evidence_split_manifest,
            args.evidence_panel,
            allow_sealed_holdout=bool(args.allow_sealed_holdout),
            access_decision_path=args.panel_access_decision,
        )
        return smoke._normalize_days(days), identity
    requested = list(args.days)
    if args.days_file is not None:
        frame = pd.read_csv(args.days_file.expanduser().resolve())
        if "day" not in frame:
            raise SystemExit("--days-file must contain a day column")
        requested.extend(frame["day"].astype(str).tolist())
    if not requested:
        raise SystemExit(
            "provide --days/--days-file or --evidence-split-manifest"
        )
    return smoke._normalize_days(requested), identity


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", nargs="+", default=[])
    parser.add_argument("--days-file", type=Path, default=None)
    parser.add_argument("--evidence-split-manifest", type=Path, default=None)
    parser.add_argument("--panel-access-decision", type=Path, default=None)
    parser.add_argument(
        "--evidence-panel",
        choices=("development", "validation", "sealed_holdout"),
        default=None,
    )
    parser.add_argument("--allow-sealed-holdout", action="store_true")
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--trace-max-per-day", type=int, default=100_000)
    parser.add_argument(
        "--decision-trace-max-per-day",
        type=int,
        default=0,
        help=(
            "Optional causal side-decision trace used by downstream paired "
            "placement shadows; zero keeps the existing output contract."
        ),
    )
    parser.add_argument("--fill-horizon-ms", type=int, default=1_000)
    parser.add_argument("--price-jump-ticks", type=float, default=1.0)
    parser.add_argument(
        "--risk-snapshot-edges-ms",
        nargs="+",
        type=int,
        default=list(DEFAULT_RISK_SNAPSHOT_EDGES_MS),
        help=(
            "Pre-registered elapsed-time boundaries for causal dynamic order "
            "risk snapshots. Edges must be unique, non-negative, and start at 0."
        ),
    )
    parser.add_argument(
        "--lifecycle-event-profile",
        choices=("full", "placement_start_stop"),
        default="full",
        help=(
            "full records dynamic risk/jump/repair transitions; "
            "placement_start_stop records only submit, activation, rejection, "
            "cancel request/ACK, fills, and administrative censoring."
        ),
    )
    parser.add_argument("--window-cache-dir", type=Path, default=None)
    parser.add_argument("--refresh-window-cache", action="store_true")
    parser.add_argument("--refresh-partials", action="store_true")
    parser.add_argument("--strict-calibration", action="store_true")
    parser.add_argument(
        "--feature-context-dir",
        type=Path,
        default=None,
        help=(
            "Versioned causal daily feature panel. This remains required for "
            "the BUY scorer even when 13-head ML inference is disabled."
        ),
    )
    parser.add_argument(
        "--feature-context-manifest-sha256",
        default="",
        help="Required causal_feature_manifest.json identity.",
    )
    parser.add_argument(
        "--queue-calibration-artifact",
        type=Path,
        default=None,
        help=(
            "Explicit queue-calibration v3 artifact. Native formal replay "
            "requires this path so calibration identity cannot depend on an "
            "ambient environment variable or mutable default."
        ),
    )
    parser.add_argument("--live-perf-telemetry", type=Path, default=None)
    parser.add_argument(
        "--live-perf-latency-mode",
        choices=("avg", "max", "sum"),
        default="avg",
    )
    parser.add_argument("--latency-profile-id", default="")
    parser.add_argument("--exec-book-visibility-profile", type=Path, default=None)
    parser.add_argument("--exec-book-visibility-profile-id", default="")
    parser.add_argument(
        "--exec-book-visibility-delay-seed",
        type=int,
        default=20260718,
    )
    parser.add_argument("--fit-artifacts", action="store_true")
    parser.add_argument(
        "--lifecycle-only",
        action="store_true",
        help=(
            "Emit lifecycle v2 and dynamic risk artifacts only. This skips "
            "the legacy static order-value panel and its second native-L2 pass."
        ),
    )
    parser.add_argument(
        "--trace-export-only",
        action="store_true",
        help=(
            "Export baseline decision/quote/lifecycle traces without the "
            "legacy static order-value labels or native first-hit second pass."
        ),
    )
    parser.add_argument(
        "--watch-manifest-out",
        type=Path,
        default=None,
        help=(
            "Optional baseline-discovery watch manifest built from complete "
            "order lifetimes in the quote trace"
        ),
    )
    parser.add_argument(
        "--watch-trace-max-per-day",
        type=int,
        default=100_000,
        help="Maximum completed order lifetimes retained per day",
    )
    parser.add_argument(
        "--watch-trajectory-id",
        default="baseline_discovery",
        help="Frozen trajectory identity written into the watch manifest",
    )
    parser.add_argument("--tick-size", type=float, default=0.1)
    parser.add_argument(
        "--execution-trade-source",
        choices=("aggTrades", "trades"),
        default="trades",
        help=(
            "Execution event tape. Use individual trades for sub-aggregate "
            "queue/flow timing; aggTrades remains available for legacy parity."
        ),
    )
    parser.add_argument(
        "--formal-quality-day-manifest",
        type=Path,
        default=None,
        help=(
            "Frozen native-strict day CSV. Listed target days, plus their causal "
            "warmup context, may supersede the legacy whole-day book audit."
        ),
    )
    parser.add_argument(
        "--formal-quality-day-manifest-sha256",
        default="",
        help="Required SHA256 identity for --formal-quality-day-manifest",
    )
    parser.add_argument(
        "--bbo-dir",
        type=Path,
        default=None,
        help="Optional historical BBO directory override",
    )
    parser.add_argument(
        "--l2-dir",
        type=Path,
        default=None,
        help="Optional historical L2 directory override",
    )
    parser.add_argument(
        "--active-order-queue-dir",
        default="",
        help=(
            "Sparse queue tape directory, or a path template containing "
            "{day}; Python replay only"
        ),
    )
    parser.add_argument(
        "--active-order-queue-mode",
        choices=("disabled", "diagnostic", "strict_sparse"),
        default="disabled",
    )
    parser.add_argument(
        "--exchange-book-raw-root",
        type=Path,
        default=None,
        help=(
            "CryptoHFT raw root. When supplied, native snapshot/delta "
            "messages become the strategy-independent exchange-time queue "
            "state stream."
        ),
    )
    parser.add_argument(
        "--exchange-book-mode",
        choices=("disabled", "diagnostic", "strict"),
        default="disabled",
    )
    parser.add_argument(
        "--exchange-book-exchange",
        default="binance_futures",
    )
    parser.add_argument(
        "--exchange-book-warmup-hours",
        type=int,
        default=24,
    )
    parser.add_argument(
        "--dynamic-fill-hazard-action-mode",
        choices=("require_disabled", "separate_frozen_treatment"),
        default="require_disabled",
        help=(
            "The live BUY q90 cancel/re-entry path is not replayed here. "
            "Use separate_frozen_treatment only when the experiment manifest "
            "must exclude it explicitly instead of silently ignoring it."
        ),
    )
    parser.add_argument(
        "--allow-missing-exchange-book-hours",
        action="store_true",
        help="Diagnostic only: emit source-gap events instead of failing early.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if args.trace_max_per_day <= 0:
        raise SystemExit("--trace-max-per-day must be positive")
    if args.decision_trace_max_per_day < 0:
        raise SystemExit("--decision-trace-max-per-day cannot be negative")
    if (
        args.watch_manifest_out is not None
        and args.watch_trace_max_per_day <= 0
    ):
        raise SystemExit("--watch-trace-max-per-day must be positive")
    if args.lifecycle_only and (
        args.watch_manifest_out is not None or args.fit_artifacts
    ):
        raise SystemExit(
            "--lifecycle-only cannot be combined with --watch-manifest-out "
            "or --fit-artifacts"
        )
    if args.trace_export_only and (
        args.lifecycle_only
        or args.fit_artifacts
        or args.watch_manifest_out is None
        or args.decision_trace_max_per_day <= 0
    ):
        raise SystemExit(
            "--trace-export-only requires decision tracing and a watch "
            "manifest, and cannot be combined with lifecycle-only/artifact fit"
        )
    if args.fill_horizon_ms <= 0 or args.tick_size <= 0.0:
        raise SystemExit("--fill-horizon-ms and --tick-size must be positive")
    risk_snapshot_edges_ms = tuple(
        sorted({int(edge) for edge in args.risk_snapshot_edges_ms})
    )
    if (
        not risk_snapshot_edges_ms
        or risk_snapshot_edges_ms[0] != 0
        or any(edge < 0 for edge in risk_snapshot_edges_ms)
        or len(risk_snapshot_edges_ms) != len(args.risk_snapshot_edges_ms)
    ):
        raise SystemExit(
            "--risk-snapshot-edges-ms must be unique, non-negative, and start at 0"
        )
    if (args.bbo_dir is None) != (args.l2_dir is None):
        raise SystemExit("--bbo-dir and --l2-dir must be supplied together")
    if bool(args.active_order_queue_dir) != (
        args.active_order_queue_mode != "disabled"
    ):
        raise SystemExit(
            "--active-order-queue-dir and a non-disabled mode are required "
            "together"
        )
    if (args.exchange_book_raw_root is not None) != (
        args.exchange_book_mode != "disabled"
    ):
        raise SystemExit(
            "--exchange-book-raw-root and a non-disabled "
            "--exchange-book-mode are required together"
        )
    if (
        args.active_order_queue_mode != "disabled"
        and args.exchange_book_mode != "disabled"
    ):
        raise SystemExit(
            "sparse active-order queue and native exchange-book modes are "
            "mutually exclusive"
        )
    if args.exchange_book_warmup_hours < 0:
        raise SystemExit("--exchange-book-warmup-hours must be non-negative")
    if (
        args.strict_calibration
        and args.exchange_book_mode != "disabled"
        and args.queue_calibration_artifact is None
    ):
        raise SystemExit(
            "native strict replay requires --queue-calibration-artifact"
        )
    if args.live_perf_telemetry is not None and not args.latency_profile_id:
        raise SystemExit("--latency-profile-id is required with telemetry")
    if (
        args.exec_book_visibility_profile is not None
        and not args.exec_book_visibility_profile_id.strip()
    ):
        raise SystemExit(
            "--exec-book-visibility-profile-id is required with an "
            "execution-book visibility profile"
        )
    if args.formal_quality_day_manifest is not None and (
        args.bbo_dir is None or args.l2_dir is None
    ):
        raise SystemExit(
            "formal quality manifest override requires explicit --bbo-dir and "
            "--l2-dir"
        )

    days, split_identity = _read_days(args)
    active_order_queue_identity = _active_order_queue_identity(
        str(args.active_order_queue_dir),
        days,
    )
    exchange_book_identity = _exchange_book_identity(
        (
            args.exchange_book_raw_root.expanduser().resolve()
            if args.exchange_book_raw_root is not None
            else None
        ),
        days,
        symbol=str(args.symbol),
        exchange=str(args.exchange_book_exchange),
        tick_size=float(args.tick_size),
        warmup_hours=int(args.exchange_book_warmup_hours),
        strict_complete=not bool(
            args.allow_missing_exchange_book_hours
        ),
    )
    config = args.config.expanduser().resolve()
    output_prefix = args.output_prefix.expanduser().resolve()
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    code_identity = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        output_prefix.parent / f"{output_prefix.name}.code_checkpoint",
        repo_root=ROOT,
        code_identity=code_identity,
    )

    bt.configure_symbol(args.symbol)
    base = load_tick_base_params(
        symbol=args.symbol,
        config_path=config,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        queue_calibration_path=args.queue_calibration_artifact,
        strict_calibration=bool(args.strict_calibration),
    )
    feature_context_required = bool(
        base.get("ml_enabled", True)
        or base.get("buy_fill_selection_live_enabled", False)
    )
    feature_context_identity: dict[str, Any] = {
        "required": feature_context_required,
        "path": "",
        "manifest_path": "",
        "manifest_sha256": "",
        "ml_inference_enabled": bool(base.get("ml_enabled", True)),
        "buy_fill_selection_enabled": bool(
            base.get("buy_fill_selection_live_enabled", False)
        ),
    }
    if feature_context_required and args.feature_context_dir is None:
        raise SystemExit(
            "current replay mechanisms require --feature-context-dir; "
            "ml.enabled and BUY scorer context are separate contracts"
        )
    if args.feature_context_dir is not None:
        feature_dir = args.feature_context_dir.expanduser().resolve()
        feature_manifest = feature_dir / "causal_feature_manifest.json"
        expected_feature_sha = str(
            args.feature_context_manifest_sha256
        ).strip()
        if not expected_feature_sha:
            raise SystemExit(
                "--feature-context-manifest-sha256 is required with "
                "--feature-context-dir"
            )
        if not feature_manifest.is_file():
            raise SystemExit(
                f"feature context manifest is missing: {feature_manifest}"
            )
        actual_feature_sha = _sha256(feature_manifest)
        if actual_feature_sha != expected_feature_sha:
            raise SystemExit(
                "feature context manifest identity changed: "
                f"expected={expected_feature_sha} actual={actual_feature_sha}"
            )
        base["_feature_context_dir"] = str(feature_dir)
        base["_feature_context_manifest_sha256"] = actual_feature_sha
        feature_context_identity.update(
            {
                "path": str(feature_dir),
                "manifest_path": str(feature_manifest),
                "manifest_sha256": actual_feature_sha,
            }
        )
    formal_quality_allowed_days, formal_quality_identity = (
        _formal_quality_context_identity(
            args.formal_quality_day_manifest,
            str(args.formal_quality_day_manifest_sha256).strip(),
            target_days=days,
            warmup_days=int(base.get("market_context_warmup_days", 1) or 0),
        )
    )
    q90_separate_treatment = {
        "treatment_id": "buy_exposure_adverse_q90_cancel_reentry_v1",
        "enabled_in_operational_config": bool(
            base.get("dynamic_fill_hazard_action_enabled", False)
        ),
        "policy_path": str(
            base.get("dynamic_fill_hazard_action_policy_path", "") or ""
        ),
        "policy_sha256": str(
            base.get("dynamic_fill_hazard_action_policy_sha256", "") or ""
        ),
        "replay_status": "excluded_frozen_separate_treatment",
    }
    if (
        q90_separate_treatment["enabled_in_operational_config"]
        and args.dynamic_fill_hazard_action_mode != "separate_frozen_treatment"
    ):
        raise SystemExit(
            "the operational config enables BUY q90 cancel/re-entry, but this "
            "replay does not implement it; pass "
            "--dynamic-fill-hazard-action-mode separate_frozen_treatment to "
            "exclude and hash it explicitly"
        )
    base.update(
        {
            "trace_quotes_max": (
                int(args.watch_trace_max_per_day)
                if args.watch_manifest_out is not None
                else 0
            ),
            "trace_decisions_max": int(args.decision_trace_max_per_day),
            "trace_queue_events_max": 0,
            "trace_fills_max": 0,
            "trace_safe_add_rearm_max": 0,
            "trace_local_order_value_max": (
                0 if args.lifecycle_only else int(args.trace_max_per_day)
            ),
            "trace_local_order_lifecycle_max": int(args.trace_max_per_day),
            "local_order_value_fill_horizon_ms": int(args.fill_horizon_ms),
            "local_order_value_label_identity": "fixed_horizon_exchange_bbo_mid.v2",
            "local_order_value_price_jump_ticks": float(
                args.price_jump_ticks
            ),
            "local_order_lifecycle_snapshot_edges_ms": list(
                risk_snapshot_edges_ms
            ),
            "local_order_lifecycle_event_profile": str(
                args.lifecycle_event_profile
            ),
            "local_action_ope_enabled": False,
            "trace_local_action_ope_max": 0,
            "sell_add_skip_ope_enabled": False,
            "trace_sell_add_skip_ope_max": 0,
            "queue_value_keep_cancel_enabled": False,
            "trace_queue_value_keep_cancel_max": 0,
            "dynamic_fill_hazard_action_enabled": False,
            "_frozen_separate_treatments": [q90_separate_treatment],
            "state_conditioned_policy_mode": "disabled",
            "replay_event_clock": "merged",
            "replay_clock_interval_ms": 100,
            "execution_trade_source": str(args.execution_trade_source),
            "_formal_quality_allowed_days": list(
                formal_quality_allowed_days
            ),
            "_formal_quality_day_manifest_sha256": str(
                formal_quality_identity.get("sha256", "")
            ),
            "active_order_queue_mode": str(args.active_order_queue_mode),
            "exchange_book_queue_mode": str(args.exchange_book_mode),
            "trace_active_order_queue_missing_max": int(
                args.watch_trace_max_per_day
            ),
        }
    )
    if args.active_order_queue_dir:
        base["_active_order_queue_dir"] = str(args.active_order_queue_dir)
        base["_active_order_queue_tick_size"] = float(args.tick_size)
    if args.exchange_book_raw_root is not None:
        base["_exchange_book_raw_root"] = str(
            args.exchange_book_raw_root.expanduser().resolve()
        )
        base["_exchange_book_tick_size"] = float(args.tick_size)
        base["_exchange_book_exchange"] = str(
            args.exchange_book_exchange
        )
        base["_exchange_book_warmup_hours"] = int(
            args.exchange_book_warmup_hours
        )
        base["_exchange_book_strict_complete"] = not bool(
            args.allow_missing_exchange_book_hours
        )
    if args.bbo_dir is not None:
        base["_historical_bbo_dir"] = str(
            args.bbo_dir.expanduser().resolve()
        )
    if args.l2_dir is not None:
        base["_historical_l2_dir"] = str(
            args.l2_dir.expanduser().resolve()
        )
    base["fill_cooldown_reset_consec_on_expiry"] = False
    base["queue_regime_calibration_enabled"] = True
    if args.live_perf_telemetry is not None:
        telemetry = args.live_perf_telemetry.expanduser().resolve()
        samples = bt._load_live_perf_latency_samples(
            telemetry,
            mode=args.live_perf_latency_mode,
        )
        base["_new_order_latency_samples_ms"] = samples[
            "new_order_latency_samples_ms"
        ]
        base["_cancel_order_latency_samples_ms"] = samples[
            "cancel_order_latency_samples_ms"
        ]
    if args.exec_book_visibility_profile is not None:
        visibility_profile = (
            args.exec_book_visibility_profile.expanduser().resolve()
        )
        visibility = bt._load_exec_book_visibility_profile(
            visibility_profile
        )
        base["_exec_book_visibility_delay_samples_ms"] = visibility.pop(
            "exec_book_visibility_delay_samples_ms"
        )
        base.update(visibility)
        base["exec_book_visibility_delay_profile_path"] = str(
            visibility_profile
        )
        base["exec_book_visibility_delay_profile_id"] = str(
            args.exec_book_visibility_profile_id
        )
        base["exec_book_visibility_delay_seed"] = int(
            args.exec_book_visibility_delay_seed
        )
    if args.strict_calibration:
        validate_formal_replay_calibration(base, require_latency=True)
    if args.window_cache_dir is not None:
        base["_window_cache_dir"] = str(
            args.window_cache_dir.expanduser().resolve()
        )
    if args.refresh_window_cache:
        base["_refresh_window_cache"] = True

    partial_dir = output_prefix.parent / f"{output_prefix.name}.partial"
    partial_dir.mkdir(parents=True, exist_ok=True)
    historical_book_visibility = (
        "empirical_receive_time_delay_profile"
        if args.exec_book_visibility_profile is not None
        else HISTORICAL_BOOK_VISIBILITY
    )
    identity = {
        "schema_version": SCHEMA_VERSION,
        "workspace_sha256": code_identity["workspace_sha256"],
        "config_sha256": _sha256(config),
        "days": days,
        "split_identity": split_identity,
        "fill_horizon_ms": int(args.fill_horizon_ms),
        "price_jump_ticks": float(args.price_jump_ticks),
        "risk_snapshot_edges_ms": list(risk_snapshot_edges_ms),
        "lifecycle_event_profile": str(args.lifecycle_event_profile),
        "trace_max_per_day": int(args.trace_max_per_day),
        "decision_trace_max_per_day": int(
            args.decision_trace_max_per_day
        ),
        "lifecycle_only": bool(args.lifecycle_only),
        "trace_export_only": bool(args.trace_export_only),
        "watch_manifest_enabled": bool(
            args.watch_manifest_out is not None
        ),
        "watch_trace_max_per_day": int(args.watch_trace_max_per_day),
        "watch_trajectory_id": str(args.watch_trajectory_id),
        "latency_profile_id": str(args.latency_profile_id),
        "strict_calibration": bool(args.strict_calibration),
        "queue_calibration_path": str(
            Path(str(base.get("queue_calibration_path", ""))).resolve()
            if base.get("queue_calibration_path")
            else ""
        ),
        "queue_calibration_sha256": (
            _sha256(Path(str(base["queue_calibration_path"])))
            if base.get("queue_calibration_path")
            else ""
        ),
        "execution_trade_source": str(args.execution_trade_source),
        "feature_context": feature_context_identity,
        "formal_quality_day_manifest": formal_quality_identity,
        "active_order_queue_dir": str(args.active_order_queue_dir),
        "active_order_queue_mode": str(args.active_order_queue_mode),
        "active_order_queue_scope": "seed_only_v1",
        "active_order_queue_artifacts": active_order_queue_identity,
        "exchange_book_mode": str(args.exchange_book_mode),
        "exchange_book_scope": (
            "strategy_independent_native_snapshot_delta_exchange_time_v1"
            if args.exchange_book_mode != "disabled"
            else "disabled"
        ),
        "exchange_book_raw_root": (
            str(args.exchange_book_raw_root.expanduser().resolve())
            if args.exchange_book_raw_root is not None
            else ""
        ),
        "exchange_book_exchange": str(args.exchange_book_exchange),
        "exchange_book_warmup_hours": int(
            args.exchange_book_warmup_hours
        ),
        "exchange_book_strict_complete": not bool(
            args.allow_missing_exchange_book_hours
        ),
        "exchange_book_artifacts": exchange_book_identity,
        "bbo_dir": str(base.get("_historical_bbo_dir", bt.BBO_DIR)),
        "l2_dir": str(base.get("_historical_l2_dir", bt.L2_DIR)),
        "historical_book_visibility": historical_book_visibility,
        "frozen_separate_treatments": [q90_separate_treatment],
        "exec_book_visibility_profile_id": str(
            base.get("exec_book_visibility_delay_profile_id", "")
        ),
        "exec_book_visibility_profile_path": str(
            base.get("exec_book_visibility_delay_profile_path", "")
        ),
        "exec_book_visibility_profile_sha256": (
            _sha256(
                Path(
                    str(base["exec_book_visibility_delay_profile_path"])
                )
            )
            if base.get("exec_book_visibility_delay_profile_path")
            else ""
        ),
        "exec_book_visibility_delay_seed": int(
            base.get("exec_book_visibility_delay_seed", 20260718)
        ),
        "exec_book_visibility_delay_sample_count": int(
            len(base.get("_exec_book_visibility_delay_samples_ms", []))
        ),
    }
    identity_path = partial_dir / "run_identity.json"
    if identity_path.exists() and not args.refresh_partials:
        existing = json.loads(identity_path.read_text(encoding="utf-8"))
        if existing != identity:
            raise RuntimeError(
                "partial identity differs; choose a new prefix or refresh"
            )
    identity_path.write_text(
        json.dumps(identity, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    results: list[dict[str, Any]] = []
    pending: list[str] = []
    for day in days:
        row_path = partial_dir / f"{day}.orders.parquet"
        lifecycle_path = partial_dir / f"{day}.lifecycle.parquet"
        quote_path = partial_dir / f"{day}.quotes.parquet"
        decision_path = partial_dir / f"{day}.decisions.parquet"
        queue_missing_path = partial_dir / f"{day}.queue_missing.parquet"
        daily_path = partial_dir / f"{day}.daily.json"
        if (
            not args.refresh_partials
            and (args.lifecycle_only or row_path.exists())
            and lifecycle_path.exists()
            and daily_path.exists()
            and (
                args.watch_manifest_out is None
                or quote_path.exists()
            )
            and (
                args.decision_trace_max_per_day == 0
                or decision_path.exists()
            )
        ):
            results.append(
                {
                    "day": day,
                    "rows": (
                        []
                        if args.lifecycle_only
                        else pd.read_parquet(row_path).to_dict("records")
                    ),
                    "lifecycle_rows": (
                        []
                        if args.lifecycle_only
                        else pd.read_parquet(lifecycle_path).to_dict("records")
                    ),
                    "lifecycle_path": str(lifecycle_path),
                    "quote_rows": (
                        pd.read_parquet(quote_path).to_dict("records")
                        if args.watch_manifest_out is not None
                        else []
                    ),
                    "decision_rows": (
                        pd.read_parquet(decision_path).to_dict("records")
                        if args.decision_trace_max_per_day > 0
                        else []
                    ),
                    "queue_missing_rows": (
                        pd.read_parquet(queue_missing_path).to_dict("records")
                        if queue_missing_path.exists()
                        else []
                    ),
                    "daily": json.loads(
                        daily_path.read_text(encoding="utf-8")
                    ),
                }
            )
        else:
            pending.append(day)

    tasks = [(day, args.symbol, base) for day in pending]
    workers = max(1, min(int(args.workers), max(1, len(tasks))))

    def persist(item: dict[str, Any]) -> None:
        if not args.lifecycle_only:
            pd.DataFrame(item["rows"]).to_parquet(
                partial_dir / f"{item['day']}.orders.parquet",
                index=False,
            )
        item_lifecycle_path = partial_dir / f"{item['day']}.lifecycle.parquet"
        lifecycle_row_count = len(item["lifecycle_rows"])
        pd.DataFrame(item["lifecycle_rows"]).to_parquet(
            item_lifecycle_path,
            index=False,
        )
        if args.watch_manifest_out is not None:
            pd.DataFrame(item["quote_rows"]).to_parquet(
                partial_dir / f"{item['day']}.quotes.parquet",
                index=False,
            )
        if args.decision_trace_max_per_day > 0:
            pd.DataFrame(item["decision_rows"]).to_parquet(
                partial_dir / f"{item['day']}.decisions.parquet",
                index=False,
            )
        if item.get("queue_missing_rows"):
            pd.DataFrame(item["queue_missing_rows"]).to_parquet(
                partial_dir / f"{item['day']}.queue_missing.parquet",
                index=False,
            )
        (partial_dir / f"{item['day']}.daily.json").write_text(
            json.dumps(item["daily"], indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if args.lifecycle_only:
            item["lifecycle_rows"] = []
        item["lifecycle_path"] = str(item_lifecycle_path)
        results.append(item)
        print(
            f"{item['day']}: lifecycle_rows={lifecycle_row_count} "
            f"runtime={item['daily']['runtime_s']:.1f}s",
            flush=True,
        )

    def accept_persisted_lifecycle(item: dict[str, Any]) -> None:
        results.append(item)
        print(
            f"{item['day']}: "
            f"lifecycle_rows={item['daily']['lifecycle_rows']} "
            f"runtime={item['daily']['runtime_s']:.1f}s",
            flush=True,
        )

    if args.lifecycle_only:
        lifecycle_tasks = [
            (day, symbol, params, str(partial_dir))
            for day, symbol, params in tasks
        ]
        for offset in range(0, len(lifecycle_tasks), workers):
            batch = lifecycle_tasks[offset : offset + workers]
            if len(batch) == 1:
                with concurrent.futures.ProcessPoolExecutor(
                    max_workers=1
                ) as executor:
                    future = executor.submit(
                        _run_lifecycle_day_to_partition,
                        batch[0],
                    )
                    accept_persisted_lifecycle(future.result())
                continue
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=len(batch)
            ) as executor:
                futures = [
                    executor.submit(_run_lifecycle_day_to_partition, task)
                    for task in batch
                ]
                for future in concurrent.futures.as_completed(futures):
                    accept_persisted_lifecycle(future.result())
    elif workers == 1:
        for task in tasks:
            persist(_run_day(task))
    elif tasks:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=workers
        ) as executor:
            futures = [executor.submit(_run_day, task) for task in tasks]
            for future in concurrent.futures.as_completed(futures):
                persist(future.result())

    results.sort(key=lambda item: item["day"])
    source = pd.DataFrame(
        [row for item in results for row in item["rows"]]
    )
    if source.empty and not args.lifecycle_only:
        raise RuntimeError("replay produced no active-order observations")
    panel = pd.DataFrame()
    panel_path: Path | None = None
    panel_summary: dict[str, Any] = {"event_counts": {}}
    if not args.lifecycle_only and not args.trace_export_only:
        if args.exchange_book_raw_root is not None:
            source = add_native_first_mid_hit_labels(
                source,
                raw_root=args.exchange_book_raw_root.expanduser().resolve(),
                symbol=str(args.symbol),
                tick_size=float(args.tick_size),
                horizon_ms=int(args.fill_horizon_ms),
                exchange=str(args.exchange_book_exchange),
                warmup_hours=int(args.exchange_book_warmup_hours),
                workers=max(1, int(args.workers)),
            )
        panel = add_competing_risk_labels(source)
        panel_path = output_prefix.with_suffix(".panel.parquet")
        panel_summary = write_panel(panel, panel_path)
    lifecycle_path = output_prefix.with_suffix(".lifecycle.parquet")
    risk_interval_path = output_prefix.with_suffix(".risk_intervals.parquet")
    lifecycle_audit_path = output_prefix.with_suffix(
        ".event_identity_and_riskset_v2.json"
    )
    if args.lifecycle_only:
        lifecycle_partitions = [
            Path(str(item["lifecycle_path"])) for item in results
        ]
        _stream_concat_parquet(lifecycle_partitions, lifecycle_path)
        audit_tasks = [
            (
                str(item["day"]),
                str(lifecycle_partitions[index]),
                str(partial_dir),
                args.exchange_book_raw_root is not None,
            )
            for index, item in enumerate(results)
        ]
        audit_results: list[dict[str, Any]] = []
        for task in audit_tasks:
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=1
            ) as executor:
                future = executor.submit(
                    _audit_lifecycle_partition_to_disk,
                    task,
                )
                audit_results.append(future.result())
        risk_partitions = [
            Path(str(item["risk_path"])) for item in audit_results
        ]
        if not risk_partitions:
            raise RuntimeError("replay produced no lifecycle risk intervals")
        _stream_concat_parquet(risk_partitions, risk_interval_path)
        audit_parts = [dict(item["audit"]) for item in audit_results]
        lifecycle_audit = aggregate_lifecycle_audits(audit_parts)
        lifecycle_row_count = int(lifecycle_audit["rows"])
        lifecycle_order_count = int(lifecycle_audit["orders"])
        risk_interval_row_count = int(lifecycle_audit["risk_interval_rows"])
    else:
        lifecycle = pd.DataFrame(
            [row for item in results for row in item.get("lifecycle_rows", [])]
        )
        if lifecycle.empty:
            raise RuntimeError("replay produced no order lifecycle transitions")
        lifecycle.to_parquet(lifecycle_path, index=False)
        risk_intervals, lifecycle_audit = audit_lifecycle_events(
            lifecycle,
            require_native_book=args.exchange_book_raw_root is not None,
        )
        risk_intervals.to_parquet(risk_interval_path, index=False)
        lifecycle_row_count = int(len(lifecycle))
        lifecycle_order_count = int(
            lifecycle[["day", "order_id"]].drop_duplicates().shape[0]
            if "day" in lifecycle.columns
            else lifecycle["order_id"].nunique()
        )
        risk_interval_row_count = int(len(risk_intervals))
    lifecycle_audit_path.write_text(
        json.dumps(lifecycle_audit, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    daily_path = output_prefix.with_suffix(".daily.csv")
    pd.DataFrame([item["daily"] for item in results]).to_csv(
        daily_path,
        index=False,
    )
    days_path = output_prefix.with_suffix(".days.csv")
    pd.DataFrame({"day": days}).to_csv(days_path, index=False)

    artifact_paths: list[Path] = [
        lifecycle_path,
        risk_interval_path,
        lifecycle_audit_path,
        daily_path,
        days_path,
    ]
    if panel_path is not None:
        artifact_paths.insert(0, panel_path)
    decision_trace_path: Path | None = None
    if args.decision_trace_max_per_day > 0:
        decisions = pd.DataFrame(
            [
                row
                for item in results
                for row in item.get("decision_rows", [])
            ]
        )
        if decisions.empty:
            raise RuntimeError("replay produced no side-decision trace")
        decision_trace_path = output_prefix.with_suffix(
            ".decisions.parquet"
        )
        decisions.to_parquet(decision_trace_path, index=False)
        artifact_paths.append(decision_trace_path)
    queue_missing_path: Path | None = None
    queue_missing = pd.DataFrame(
        [
            row
            for item in results
            for row in item.get("queue_missing_rows", [])
        ]
    )
    if not queue_missing.empty:
        queue_missing_path = output_prefix.with_suffix(
            ".queue_missing.parquet"
        )
        queue_missing.to_parquet(queue_missing_path, index=False)
        artifact_paths.append(queue_missing_path)
    watch_manifest_path: Path | None = None
    watch_summary_path: Path | None = None
    if args.watch_manifest_out is not None:
        watch_manifest_path = (
            args.watch_manifest_out.expanduser().resolve()
        )
        watch_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        quote_source = pd.DataFrame(
            [
                row
                for item in results
                for row in item.get("quote_rows", [])
            ]
        )
        if args.trace_export_only:
            quote_trace_path = output_prefix.with_suffix(".quotes.parquet")
            quote_source.to_parquet(quote_trace_path, index=False)
            artifact_paths.append(quote_trace_path)
        watch = build_watch_manifest(
            quote_source,
            tick_size=float(args.tick_size),
            trajectory_id=str(args.watch_trajectory_id),
        )
        watch.to_parquet(watch_manifest_path, index=False)
        watch_summary_path = watch_manifest_path.with_suffix(
            watch_manifest_path.suffix + ".summary.json"
        )
        watch_summary_path.write_text(
            json.dumps(
                {
                    "schema_version": "active_order_watch_manifest.v1",
                    "trajectory_id": str(args.watch_trajectory_id),
                    "days": sorted(watch["day"].unique().tolist()),
                    "rows": int(len(watch)),
                    "orders": int(watch["order_id"].nunique()),
                    "side_counts": {
                        str(key): int(value)
                        for key, value in watch["side"].value_counts().items()
                    },
                    "inventory_role_counts": {
                        str(key): int(value)
                        for key, value in (
                            watch["inventory_role"]
                            .value_counts()
                            .items()
                        )
                    },
                    "watch_manifest_sha256": _sha256(
                        watch_manifest_path
                    ),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        artifact_paths.extend((watch_manifest_path, watch_summary_path))
    queue_path: Path | None = None
    microprice_path: Path | None = None
    if args.fit_artifacts:
        queue = fit_queue_reactive_hawkes(
            panel,
            input_scope="local_only",
        )
        microprice = fit_empirical_microprice(
            panel,
            input_scope="local_only",
            tick_size=float(args.tick_size),
            horizon_ms=int(args.fill_horizon_ms),
        )
        queue_path = output_prefix.with_suffix(".queue_hawkes.json")
        microprice_path = output_prefix.with_suffix(
            ".empirical_microprice.json"
        )
        queue.save(queue_path)
        microprice.save(microprice_path)
        artifact_paths.extend((queue_path, microprice_path))

    metadata_path = output_prefix.with_suffix(".metadata.json")
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "engine": "python_authoritative_observation_replay",
        "input_scope": "local_only",
        "external_reference_used": False,
        "execution_trade_source": str(args.execution_trade_source),
        "active_order_queue_dir": str(args.active_order_queue_dir),
        "active_order_queue_mode": str(args.active_order_queue_mode),
        "active_order_queue_artifacts": active_order_queue_identity,
        "exchange_book_mode": str(args.exchange_book_mode),
        "exchange_book_scope": (
            "strategy_independent_native_snapshot_delta_exchange_time_v1"
            if args.exchange_book_mode != "disabled"
            else "disabled"
        ),
        "exchange_book_raw_root": (
            str(args.exchange_book_raw_root.expanduser().resolve())
            if args.exchange_book_raw_root is not None
            else ""
        ),
        "exchange_book_exchange": str(args.exchange_book_exchange),
        "exchange_book_warmup_hours": int(
            args.exchange_book_warmup_hours
        ),
        "exchange_book_strict_complete": not bool(
            args.allow_missing_exchange_book_hours
        ),
        "exchange_book_artifacts": exchange_book_identity,
        "active_order_queue_missing_rows": int(len(queue_missing)),
        "active_order_queue_missing_path": str(queue_missing_path or ""),
        "bbo_dir": str(base.get("_historical_bbo_dir", bt.BBO_DIR)),
        "l2_dir": str(base.get("_historical_l2_dir", bt.L2_DIR)),
        "historical_book_visibility": historical_book_visibility,
        "frozen_separate_treatments": [q90_separate_treatment],
        "exec_book_visibility_profile_id": str(
            base.get("exec_book_visibility_delay_profile_id", "")
        ),
        "exec_book_visibility_profile_path": str(
            base.get("exec_book_visibility_delay_profile_path", "")
        ),
        "exec_book_visibility_profile_sha256": (
            _sha256(
                Path(
                    str(base["exec_book_visibility_delay_profile_path"])
                )
            )
            if base.get("exec_book_visibility_delay_profile_path")
            else ""
        ),
        "exec_book_visibility_delay_seed": int(
            base.get("exec_book_visibility_delay_seed", 20260718)
        ),
        "exec_book_visibility_delay_sample_count": int(
            len(base.get("_exec_book_visibility_delay_samples_ms", []))
        ),
        "days": days,
        "rows": int(len(panel)),
        "orders": (
            int(panel["order_id"].nunique())
            if "order_id" in panel.columns
            else 0
        ),
        "lifecycle_only": bool(args.lifecycle_only),
        "lifecycle_schema_version": LIFECYCLE_SCHEMA_VERSION,
        "lifecycle_rows": lifecycle_row_count,
        "lifecycle_orders": lifecycle_order_count,
        "risk_interval_schema_version": INTERVAL_SCHEMA_VERSION,
        "risk_interval_rows": risk_interval_row_count,
        "event_identity_and_riskset_v2_status": str(
            lifecycle_audit["status"]
        ),
        "event_identity_and_riskset_v2_failed_checks": list(
            lifecycle_audit["failed_checks"]
        ),
        "event_counts": panel_summary["event_counts"],
        "fill_horizon_ms": int(args.fill_horizon_ms),
        "price_jump_ticks": float(args.price_jump_ticks),
        "risk_snapshot_edges_ms": list(risk_snapshot_edges_ms),
        "trace_max_per_day": int(args.trace_max_per_day),
        "watch_trajectory_id": str(args.watch_trajectory_id),
        "watch_manifest_path": str(watch_manifest_path or ""),
        "watch_manifest_sha256": (
            _sha256(watch_manifest_path)
            if watch_manifest_path is not None
            else ""
        ),
        "config_path": str(config),
        "config_sha256": _sha256(config),
        "queue_calibration_path": str(
            base.get("queue_calibration_path", "")
        ),
        "queue_calibration_sha256": (
            _sha256(Path(str(base["queue_calibration_path"])))
            if base.get("queue_calibration_path")
            else ""
        ),
        "workspace_sha256": code_identity["workspace_sha256"],
        "code_checkpoint": checkpoint,
        "split_identity": split_identity,
        "queue_artifact": str(queue_path or ""),
        "queue_artifact_sha256": (
            _sha256(queue_path) if queue_path is not None else ""
        ),
        "microprice_artifact": str(microprice_path or ""),
        "microprice_artifact_sha256": (
            _sha256(microprice_path)
            if microprice_path is not None
            else ""
        ),
        "promotion_status": "diagnostic_only",
    }
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifact_paths.append(metadata_path)

    manifest = build_manifest(
        {
            "experiment_id": output_prefix.name,
            "config_path": str(config),
            "dataset_manifest_path": str(days_path),
            "feature_schema_version": (
                "local_order_value_panel.v1+local_order_lifecycle.v2+"
                "fixed_horizon_exchange_bbo_mid.v2"
            ),
            "model_versions": {
                "queue_reactive": (
                    "queue_reactive_hawkes.v1"
                    if queue_path is not None
                    else "not_fitted"
                ),
                "microprice": (
                    "empirical_microprice.v1"
                    if microprice_path is not None
                    else "not_fitted"
                ),
                "active_order_watch_manifest": (
                    "active_order_watch_manifest.v1"
                    if watch_manifest_path is not None
                    else "not_built"
                ),
                "native_exchange_book": (
                    "native_exchange_book_tape.v1"
                    if args.exchange_book_mode != "disabled"
                    else "disabled"
                ),
            },
            "label_versions": {
                "competing_risk": COMPETING_RISK_LABEL_IDENTITY,
                "order_lifecycle": LIFECYCLE_SCHEMA_VERSION,
                "risk_interval": INTERVAL_SCHEMA_VERSION,
                "event_identity_and_riskset": LIFECYCLE_LABEL_IDENTITY,
            },
            "splits": split_identity or {"diagnostic_days": days},
            "baseline_definition": {
                "name": "rolling baseline; one observation per activated order",
                "latency_profile_id": str(args.latency_profile_id),
                "exec_book_visibility_profile_id": str(
                    base.get("exec_book_visibility_delay_profile_id", "")
                ),
            },
            "action_definition": "none; observation only",
            "input_paths": [
                str(path)
                for path in (
                    args.live_perf_telemetry,
                    args.exec_book_visibility_profile,
                    args.evidence_split_manifest,
                    args.queue_calibration_artifact,
                    *[
                        Path(file_identity["path"])
                        for queue_identity in active_order_queue_identity
                        for file_identity in queue_identity["files"].values()
                    ],
                    *[
                        Path(str(file_identity["path"]))
                        for tape_identity in exchange_book_identity
                        for file_identity in tape_identity["files"]
                    ],
                )
                if path is not None
            ],
            "artifact_paths": [str(path) for path in artifact_paths],
            "engine": "python_authoritative_tick_replay",
            "promotion_status": "diagnostic_only",
            "notes": (
                "Local M0 only. K0/K1 and external M1 are disabled while "
                "training the state artifacts. Native snapshot/delta queue "
                "state is reconstructed independently of strategy trajectory "
                "on exchange time; policy-visible BBO/L2 still uses the frozen "
                "execution-book visibility profile when one is supplied. "
                "Exchange-time-only queue evidence remains diagnostic rather "
                "than executable action uplift."
            ),
        },
        repo_root=ROOT,
        code_identity=code_identity,
    )
    manifest_path = output_prefix.with_suffix(".experiment_manifest.json")
    write_manifest(manifest_path, manifest)
    print(
        json.dumps(
            {
                "panel": str(panel_path),
                "rows": len(panel),
                "queue_artifact": str(queue_path or ""),
                "microprice_artifact": str(microprice_path or ""),
                "watch_manifest": str(watch_manifest_path or ""),
                "manifest": str(manifest_path),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
