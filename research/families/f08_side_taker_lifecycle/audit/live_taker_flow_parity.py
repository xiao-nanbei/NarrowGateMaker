#!/usr/bin/env python3
"""Audit same-window individual-trade and aggTrade receive-time parity.

The public Binance USD-M stream currently supplies ``aggTrade`` but not a
documented raw individual-trade stream.  This audit therefore requires an
explicit, frozen identity for any individual receive-time source.  Historical
exchange-time trades alone cannot satisfy the gate.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import numpy as np

from research.system_engineering.audit.receive_time_tape import expand_inputs, iter_rows

SCHEMA_VERSION = "live_taker_flow_parity.v1"
EXPERIMENT_ID = "live_taker_flow_parity_v1"
BINANCE_USDM_STREAM_CATALOG_ID = "binance_usdm_public_ws_catalog_20260723.v1"
BINANCE_USDM_STREAM_CATALOG_URL = (
    "https://developers.binance.com/en/docs/catalog/"
    "core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public"
)
DEFAULT_MIN_MATCHED_AGGREGATES = 1_000


def _stream_type(row: dict[str, Any]) -> str:
    value = str(row.get("trade_stream_type", "")).strip().lower()
    if value in {"aggregate", "aggtrade", "agg_trade"}:
        return "aggregate"
    if value in {"individual", "raw_trade", "trade"}:
        return "individual"
    return "untyped"


def _int_value(row: dict[str, Any], name: str) -> int | None:
    value = row.get(name)
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_value(row: dict[str, Any], name: str) -> float:
    try:
        value = float(row.get(name, math.nan))
    except (TypeError, ValueError):
        return math.nan
    return value if math.isfinite(value) else math.nan


def _causal_timestamps_valid(row: dict[str, Any]) -> bool:
    exchange = _int_value(row, "exchange_event_ts_ns") or 0
    receive = _int_value(row, "local_receive_ts_ns") or 0
    ready = _int_value(row, "feature_ready_ts_ns") or 0
    return exchange > 0 and receive > 0 and ready >= receive


def _right_edge_bucket(ts_ns: int, bucket_ms: int) -> int:
    width = int(bucket_ms) * 1_000_000
    return ((int(ts_ns) + width - 1) // width) * width


def _finite_or_none(value: float) -> float | None:
    return float(value) if math.isfinite(value) else None


def audit_trade_rows(
    rows: Iterable[dict[str, Any]],
    *,
    market_id: str,
    individual_source_identity: str = "",
    min_matched_aggregates: int = DEFAULT_MIN_MATCHED_AGGREGATES,
    feature_bucket_ms: int = 100,
    quantity_abs_tolerance: float = 1e-10,
) -> dict[str, Any]:
    filtered = [
        row
        for row in rows
        if str(row.get("market_id", "")) == market_id
        and str(row.get("event_type", "")).lower() == "trade"
    ]
    typed: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in filtered:
        typed[_stream_type(row)].append(row)
    aggregate_rows = typed["aggregate"]
    individual_rows = typed["individual"]
    untyped_rows = typed["untyped"]

    individual_by_id: dict[int, dict[str, Any]] = {}
    duplicate_individual_ids = 0
    for row in individual_rows:
        trade_id = _int_value(row, "trade_id")
        if trade_id is None:
            continue
        if trade_id in individual_by_id:
            duplicate_individual_ids += 1
        else:
            individual_by_id[trade_id] = row

    matched_aggregates = 0
    complete_ranges = 0
    quantity_matches = 0
    side_matches = 0
    feature_bucket_matches = 0
    invalid_ranges = 0
    visibility_deltas_ms: list[float] = []
    child_ids_matched = 0
    child_ids_expected = 0
    for aggregate in aggregate_rows:
        first_id = _int_value(aggregate, "first_trade_id")
        last_id = _int_value(aggregate, "last_trade_id")
        if first_id is None or last_id is None or last_id < first_id:
            invalid_ranges += 1
            continue
        range_size = last_id - first_id + 1
        if range_size > 100_000:
            invalid_ranges += 1
            continue
        child_ids_expected += range_size
        children = [
            individual_by_id[trade_id]
            for trade_id in range(first_id, last_id + 1)
            if trade_id in individual_by_id
        ]
        child_ids_matched += len(children)
        if not children:
            continue
        matched_aggregates += 1
        complete = len(children) == range_size
        complete_ranges += int(complete)
        if not complete:
            continue

        aggregate_qty = _float_value(aggregate, "size")
        child_qty = sum(_float_value(row, "size") for row in children)
        qty_match = math.isfinite(aggregate_qty) and math.isclose(
            aggregate_qty,
            child_qty,
            rel_tol=1e-9,
            abs_tol=float(quantity_abs_tolerance),
        )
        quantity_matches += int(qty_match)
        aggregate_side = str(aggregate.get("aggressor_side", "")).lower()
        child_sides = {str(row.get("aggressor_side", "")).lower() for row in children}
        side_matches += int(len(child_sides) == 1 and aggregate_side in child_sides)

        aggregate_ready = _int_value(aggregate, "feature_ready_ts_ns") or 0
        child_ready = [
            _int_value(row, "feature_ready_ts_ns") or 0 for row in children
        ]
        if aggregate_ready > 0 and child_ready and min(child_ready) > 0:
            aggregate_bucket = _right_edge_bucket(aggregate_ready, feature_bucket_ms)
            child_buckets = {
                _right_edge_bucket(timestamp, feature_bucket_ms)
                for timestamp in child_ready
            }
            feature_bucket_matches += int(child_buckets == {aggregate_bucket})
            visibility_deltas_ms.append(
                (aggregate_ready - max(child_ready)) / 1_000_000.0
            )

    aggregate_timestamp_valid = sum(
        _causal_timestamps_valid(row) for row in aggregate_rows
    )
    individual_timestamp_valid = sum(
        _causal_timestamps_valid(row) for row in individual_rows
    )
    aggregate_lineage_complete = sum(
        _int_value(row, "aggregate_trade_id") is not None
        and _int_value(row, "first_trade_id") is not None
        and _int_value(row, "last_trade_id") is not None
        for row in aggregate_rows
    )
    individual_lineage_complete = sum(
        _int_value(row, "trade_id") is not None for row in individual_rows
    )

    def rate(numerator: int, denominator: int) -> float:
        return numerator / denominator if denominator else math.nan

    gates = {
        "explicit_individual_source_identity": bool(
            str(individual_source_identity).strip()
        ),
        "aggregate_rows_present": bool(aggregate_rows),
        "individual_rows_present": bool(individual_rows),
        "no_untyped_trade_rows": not untyped_rows,
        "aggregate_lineage_complete": (
            bool(aggregate_rows) and aggregate_lineage_complete == len(aggregate_rows)
        ),
        "individual_lineage_complete": (
            bool(individual_rows) and individual_lineage_complete == len(individual_rows)
        ),
        "causal_timestamps_complete": (
            bool(aggregate_rows)
            and bool(individual_rows)
            and aggregate_timestamp_valid == len(aggregate_rows)
            and individual_timestamp_valid == len(individual_rows)
        ),
        "minimum_matched_aggregate_support": (
            matched_aggregates >= max(1, int(min_matched_aggregates))
        ),
        "exact_trade_id_range_coverage": (
            child_ids_expected > 0 and child_ids_matched == child_ids_expected
        ),
        "aggregate_quantity_parity": (
            complete_ranges > 0 and quantity_matches == complete_ranges
        ),
        "aggressor_side_parity": (
            complete_ranges > 0 and side_matches == complete_ranges
        ),
        "feature_ready_100ms_bucket_parity": (
            complete_ranges > 0 and feature_bucket_matches == complete_ranges
        ),
        "unique_individual_trade_ids": duplicate_individual_ids == 0,
    }
    failed_gates = sorted(name for name, passed in gates.items() if not passed)
    visibility = np.asarray(visibility_deltas_ms, dtype=float)

    return {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "status": "passed" if not failed_gates else "blocked",
        "market_id": market_id,
        "individual_source_identity": str(individual_source_identity),
        "official_source_catalog": {
            "identity": BINANCE_USDM_STREAM_CATALOG_ID,
            "url": BINANCE_USDM_STREAM_CATALOG_URL,
            "public_aggtrade_documented": True,
            "public_individual_trade_stream_documented": False,
            "note": (
                "Do not infer a futures raw @trade stream from the spot API; "
                "an explicit supported source identity is required."
            ),
        },
        "rows": {
            "all_market_trades": len(filtered),
            "aggregate": len(aggregate_rows),
            "individual": len(individual_rows),
            "untyped": len(untyped_rows),
            "matched_aggregates": matched_aggregates,
            "complete_id_ranges": complete_ranges,
            "invalid_aggregate_ranges": invalid_ranges,
            "duplicate_individual_ids": duplicate_individual_ids,
        },
        "parity": {
            "child_id_coverage": _finite_or_none(
                rate(child_ids_matched, child_ids_expected)
            ),
            "quantity_match_rate": _finite_or_none(
                rate(quantity_matches, complete_ranges)
            ),
            "aggressor_side_match_rate": _finite_or_none(
                rate(side_matches, complete_ranges)
            ),
            "feature_ready_bucket_ms": int(feature_bucket_ms),
            "feature_ready_bucket_match_rate": _finite_or_none(
                rate(feature_bucket_matches, complete_ranges)
            ),
            "aggregate_minus_last_individual_ready_ms_p50": (
                float(np.quantile(visibility, 0.50)) if visibility.size else None
            ),
            "aggregate_minus_last_individual_ready_ms_p95": (
                float(np.quantile(visibility, 0.95)) if visibility.size else None
            ),
        },
        "gates": gates,
        "failed_gates": failed_gates,
        "live_taker_flow_parity_passed": not failed_gates,
        "dynamic_fill_hazard_allowed": False,
        "dynamic_fill_hazard_block_reason": (
            "requires_live_taker_flow_parity_v1_and_event_identity_and_riskset_v1"
        ),
        "action_family_allowed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", help="Tape files, directories, or globs")
    parser.add_argument("--market-id", default="binance:perp:BTCUSDC")
    parser.add_argument("--individual-source-identity", default="")
    parser.add_argument(
        "--min-matched-aggregates",
        type=int,
        default=DEFAULT_MIN_MATCHED_AGGREGATES,
    )
    parser.add_argument("--feature-bucket-ms", type=int, default=100)
    parser.add_argument("--output-summary", type=Path, required=True)
    args = parser.parse_args()

    paths = expand_inputs(args.inputs)
    if not paths:
        parser.error("no input tape files matched")
    summary = audit_trade_rows(
        iter_rows(paths),
        market_id=args.market_id,
        individual_source_identity=args.individual_source_identity,
        min_matched_aggregates=args.min_matched_aggregates,
        feature_bucket_ms=args.feature_bucket_ms,
    )
    summary["input_paths"] = [str(path) for path in paths]
    output = args.output_summary.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
