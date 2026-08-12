"""Audit quote-snapshot atomicity without reading economic outcomes."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import sys
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from live.config import Config  # noqa: E402
from strategy.maker_engine import MakerEngine  # noqa: E402
from strategy.quote_core import microprice_from_book  # noqa: E402
from strategy.signal import SignalEngine  # noqa: E402


def _percentile(values: Iterable[float], percentile: float) -> float:
    ordered = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not ordered:
        return math.nan
    rank = max(0.0, min(1.0, percentile)) * (len(ordered) - 1)
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    if low == high:
        return ordered[low]
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def _distribution(values: Iterable[float]) -> dict[str, float]:
    finite = [float(value) for value in values if math.isfinite(float(value))]
    return {
        "p50": statistics.median(finite) if finite else math.nan,
        "p99": _percentile(finite, 0.99),
        "max": max(finite, default=math.nan),
    }


def _depth_event(ts_ms: int, center: float) -> dict:
    bids = [[f"{center - 0.1 * (level + 1):.1f}", "1.0"] for level in range(20)]
    asks = [[f"{center + 0.1 * (level + 1):.1f}", "1.0"] for level in range(20)]
    return {"T": ts_ms, "b": bids, "a": asks}


def _book_event(ts_ms: int, center: float, sequence: int) -> dict:
    return {
        "E": ts_ms,
        "s": "BTCUSDC",
        "b": f"{center - 0.05:.2f}",
        "B": "1.0",
        "a": f"{center + 0.05:.2f}",
        "A": "1.0",
        "u": sequence,
    }


def run_synthetic(iterations: int) -> dict[str, object]:
    signal = SignalEngine(enable_ml=False)
    cfg = Config()
    engine = object.__new__(MakerEngine)
    engine.cfg = cfg
    start_ns = time.time_ns()
    start_ms = start_ns // 1_000_000
    signal.on_book_ticker(
        _book_event(start_ms, 60_000.0, 1),
        receive_ts_ns=start_ns,
        sequence_number=1,
    )
    signal.on_depth(
        _depth_event(start_ms, 60_000.0),
        receive_ts_ns=start_ns + 1,
    )

    stop = threading.Event()

    def produce() -> None:
        sequence = 2
        while not stop.is_set():
            receive_ns = time.time_ns()
            center = 60_000.0 + (sequence % 40) * 0.1
            exchange_ms = receive_ns // 1_000_000
            signal.on_book_ticker(
                _book_event(exchange_ms, center, sequence),
                receive_ts_ns=receive_ns,
                sequence_number=sequence,
            )
            signal.on_depth(
                _depth_event(exchange_ms, center),
                receive_ts_ns=time.time_ns(),
            )
            sequence += 1
            time.sleep(0)

    producer = threading.Thread(target=produce, daemon=True)
    producer.start()

    invalid_reasons: Counter[str] = Counter()
    fallback_reasons: Counter[str] = Counter()
    mid_identity_violations = 0
    microprice_violations = 0
    routing_violations = 0
    quote_identity_violations = 0
    lock_wait_us: list[float] = []
    lock_hold_us: list[float] = []
    try:
        for _ in range(max(1, int(iterations))):
            snapshot = signal.quote_decision_snapshot()
            if not snapshot.valid:
                invalid_reasons[snapshot.invalid_reason] += 1
                continue
            guard = snapshot.post_only_guard(
                max_visible_age_s=5.0,
                max_source_lag_s=5.0,
            )
            if guard.fallback_reason:
                fallback_reasons[guard.fallback_reason] += 1
            tolerance = cfg.tick_size * 1e-9
            expected_mid = 0.5 * (snapshot.best_bid + snapshot.best_ask)
            if abs(snapshot.mid - expected_mid) > tolerance:
                mid_identity_violations += 1
            microprice = microprice_from_book(snapshot.bids, snapshot.asks, levels=3)
            if not (
                snapshot.best_bid - tolerance
                <= microprice
                <= snapshot.best_ask + tolerance
            ):
                microprice_violations += 1
            bid = math.floor((snapshot.mid - cfg.tick_size) / cfg.tick_size) * cfg.tick_size
            ask = math.ceil((snapshot.mid + cfg.tick_size) / cfg.tick_size) * cfg.tick_size
            bid = min(
                bid,
                math.floor(
                    (guard.best_ask - cfg.tick_size) / cfg.tick_size
                )
                * cfg.tick_size,
            )
            ask = max(
                ask,
                math.ceil(
                    (guard.best_bid + cfg.tick_size) / cfg.tick_size
                )
                * cfg.tick_size,
            )
            routing_error = engine._quote_routing_contract_error(
                bid_price=bid,
                ask_price=ask,
                can_bid=True,
                can_ask=True,
                post_only_guard=guard,
            )
            if routing_error:
                routing_violations += 1
            identity_error = (ask - snapshot.mid) + (snapshot.mid - bid) - (ask - bid)
            if abs(identity_error) > tolerance:
                quote_identity_violations += 1
            lock_wait_us.append(snapshot.lock_wait_ns / 1_000.0)
            lock_hold_us.append(snapshot.lock_hold_ns / 1_000.0)
    finally:
        stop.set()
        producer.join(timeout=2.0)

    report = {
        "schema_version": "quote_snapshot_integrity_synthetic.v1",
        "iterations": int(iterations),
        "invalid_reasons": dict(invalid_reasons),
        "guard_fallback_reasons": dict(fallback_reasons),
        "mid_identity_violations": mid_identity_violations,
        "microprice_violations": microprice_violations,
        "routing_violations": routing_violations,
        "quote_identity_violations": quote_identity_violations,
        "lock_wait_us_p50": statistics.median(lock_wait_us) if lock_wait_us else math.nan,
        "lock_wait_us_p99": _percentile(lock_wait_us, 0.99),
        "lock_wait_us_max": max(lock_wait_us, default=math.nan),
        "lock_hold_us_p50": statistics.median(lock_hold_us) if lock_hold_us else math.nan,
        "lock_hold_us_p99": _percentile(lock_hold_us, 0.99),
        "lock_hold_us_max": max(lock_hold_us, default=math.nan),
    }
    report["gate_passed"] = bool(
        not invalid_reasons
        and mid_identity_violations == 0
        and microprice_violations == 0
        and routing_violations == 0
        and quote_identity_violations == 0
        and float(report["lock_wait_us_p99"]) <= 5_000.0
        and float(report["lock_hold_us_p99"]) <= 5_000.0
    )
    return report


def audit_telemetry(path: Path, *, tick_size: float = 0.1) -> dict[str, object]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError(f"quote snapshot telemetry is empty: {path}")

    def numbers(field: str) -> list[float]:
        values = []
        for row in rows:
            try:
                values.append(float(row.get(field, "nan")))
            except (TypeError, ValueError):
                continue
        return values

    timestamps = numbers("timestamp")
    duration_h = max(0.0, max(timestamps) - min(timestamps)) / 3_600.0
    status_counts = Counter(str(row.get("status", "")) for row in rows)
    fallback_counts = Counter(
        str(row.get("guard_fallback_reason", ""))
        for row in rows
        if str(row.get("guard_fallback_reason", ""))
    )
    post_only_violations = sum(int(float(row["post_only_violation_count"])) for row in rows)
    guard_source_counts = Counter(str(row.get("guard_source", "")) for row in rows)
    tick_mismatches = 0
    for row in rows:
        for field in ("final_bid", "final_ask"):
            price = float(row.get(field, 0.0) or 0.0)
            if price > 0.0 and abs(price / tick_size - round(price / tick_size)) > 1e-7:
                tick_mismatches += 1
    identity_errors = [abs(value) for value in numbers("quote_identity_error_ticks")]
    valid_generation_rows = sum(
        int(float(row.get("snapshot_valid", 0) or 0)) == 1
        and int(float(row.get("market_generation", 0) or 0)) > 0
        and int(float(row.get("depth_generation", 0) or 0)) > 0
        for row in rows
    )
    depth_spread_ticks = [
        (float(row.get("depth_ask", 0.0)) - float(row.get("depth_bid", 0.0)))
        / tick_size
        for row in rows
        if float(row.get("depth_ask", 0.0) or 0.0)
        > float(row.get("depth_bid", 0.0) or 0.0)
        > 0.0
    ]
    book_spread_ticks = [
        (
            float(row.get("book_ticker_ask", 0.0))
            - float(row.get("book_ticker_bid", 0.0))
        )
        / tick_size
        for row in rows
        if float(row.get("book_ticker_ask", 0.0) or 0.0)
        > float(row.get("book_ticker_bid", 0.0) or 0.0)
        > 0.0
    ]
    depth_book_bid_delta_ticks = [
        abs(
            float(row.get("book_ticker_bid", 0.0))
            - float(row.get("depth_bid", 0.0))
        )
        / tick_size
        for row in rows
        if float(row.get("book_ticker_bid", 0.0) or 0.0) > 0.0
        and float(row.get("depth_bid", 0.0) or 0.0) > 0.0
    ]
    depth_book_ask_delta_ticks = [
        abs(
            float(row.get("book_ticker_ask", 0.0))
            - float(row.get("depth_ask", 0.0))
        )
        / tick_size
        for row in rows
        if float(row.get("book_ticker_ask", 0.0) or 0.0) > 0.0
        and float(row.get("depth_ask", 0.0) or 0.0) > 0.0
    ]
    cancels = sum(int(float(row.get("rest_cancel_count", 0) or 0)) for row in rows)
    report = {
        "schema_version": "quote_snapshot_integrity_telemetry_audit.v1",
        "path": str(path.resolve()),
        "rows": len(rows),
        "duration_h": duration_h,
        "status_counts": dict(status_counts),
        "guard_source_counts": dict(guard_source_counts),
        "guard_fallback_reasons": dict(fallback_counts),
        "valid_generation_rows": valid_generation_rows,
        "post_only_violation_count": post_only_violations,
        "final_tick_mismatch_count": tick_mismatches,
        "max_quote_identity_error_ticks": max(identity_errors, default=math.nan),
        "snapshot_lock_wait_us_p99": _percentile(numbers("snapshot_lock_wait_us"), 0.99),
        "snapshot_lock_hold_us_p99": _percentile(numbers("snapshot_lock_hold_us"), 0.99),
        "depth_spread_ticks": _distribution(depth_spread_ticks),
        "book_ticker_spread_ticks": _distribution(book_spread_ticks),
        "depth_book_bid_delta_ticks": _distribution(depth_book_bid_delta_ticks),
        "depth_book_ask_delta_ticks": _distribution(depth_book_ask_delta_ticks),
        "depth_visible_age_s": _distribution(numbers("depth_visible_age_s")),
        "depth_source_lag_s": _distribution(numbers("depth_source_lag_s")),
        "book_ticker_visible_age_s": _distribution(
            numbers("book_ticker_visible_age_s")
        ),
        "book_ticker_source_lag_s": _distribution(
            numbers("book_ticker_source_lag_s")
        ),
        "max_consecutive_snapshot_blocks": max(
            (int(value) for value in numbers("consecutive_snapshot_blocks")),
            default=0,
        ),
        "cancel_per_hour": cancels / duration_h if duration_h > 0.0 else math.nan,
    }
    report["gate_passed"] = bool(
        post_only_violations == 0
        and tick_mismatches == 0
        and valid_generation_rows == len(rows)
        and max(identity_errors, default=0.0) <= 1e-6
        and not any(status.startswith("blocked:") for status in status_counts)
        and float(report["snapshot_lock_wait_us_p99"]) <= 5_000.0
        and float(report["snapshot_lock_hold_us_p99"]) <= 5_000.0
    )
    return report


def audit_perf_cancel_rates(
    path: Path,
    *,
    window_start_ts: float,
    window_end_ts: float,
) -> dict[str, float | int | str]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    duration_s = max(0.0, float(window_end_ts) - float(window_start_ts))
    prior_start_ts = float(window_start_ts) - duration_s

    def summarize(
        start: float,
        end: float,
        *,
        include_end: bool,
    ) -> tuple[int, int, float]:
        selected = []
        for row in rows:
            try:
                timestamp = float(row.get("timestamp", "nan"))
            except (TypeError, ValueError):
                continue
            if start <= timestamp and (
                timestamp <= end if include_end else timestamp < end
            ):
                selected.append(row)
        cancels = sum(
            int(float(row.get("rest_cancel_count", 0) or 0))
            + int(float(row.get("rest_cancel_all_count", 0) or 0))
            for row in selected
        )
        hours = duration_s / 3_600.0
        return len(selected), cancels, cancels / hours if hours > 0.0 else math.nan

    current_rows, current_cancels, current_rate = summarize(
        float(window_start_ts), float(window_end_ts), include_end=True
    )
    prior_rows, prior_cancels, prior_rate = summarize(
        prior_start_ts,
        window_start_ts,
        include_end=False,
    )
    return {
        "schema_version": "quote_snapshot_cancel_rate_comparison.v1",
        "current_rows": current_rows,
        "current_cancels": current_cancels,
        "current_cancel_per_hour": current_rate,
        "prior_rows": prior_rows,
        "prior_cancels": prior_cancels,
        "prior_cancel_per_hour": prior_rate,
        "incremental_cancel_per_hour": current_rate - prior_rate,
    }


def _health_metric(lines: list[str], name: str) -> float:
    pattern = re.compile(rf"(?:^| ){re.escape(name)}=([^ ]+)")
    values: list[float] = []
    for line in lines:
        match = pattern.search(line)
        if match is None:
            continue
        try:
            values.append(float(match.group(1)))
        except ValueError:
            continue
    return max(values) if values else 0.0


def audit_health_log(path: Path) -> dict[str, object]:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    health = [line for line in lines if "HEALTH " in line]
    severe = [
        line
        for line in lines
        if re.search(r"\b(ERROR|CRITICAL)\b", line)
        or "Traceback (most recent call last)" in line
    ]
    report = {
        "schema_version": "quote_snapshot_runtime_health.v1",
        "path": str(path.resolve()),
        "health_rows": len(health),
        "severe_rows": len(severe),
        "market_tape_dropped_max": _health_metric(health, "marketTapeDropped"),
        "market_tape_invalid_max": _health_metric(health, "marketTapeInvalid"),
        "market_tape_queue_hwm_max": _health_metric(health, "marketTapeQueueHwm"),
        "market_tape_max_queue_age_ms": _health_metric(
            health, "marketTapeMaxQueueAgeMs"
        ),
        "external_record_dropped_max": _health_metric(
            health, "externalRecordDropped"
        ),
        "external_record_hwm_max": _health_metric(health, "externalRecordHwm"),
        "external_record_max_age_ms": _health_metric(
            health, "externalRecordMaxAgeMs"
        ),
        "deep_book_buffer_max": _health_metric(health, "deepBookBuffer"),
        "deep_book_gap_max": _health_metric(health, "deepBookGaps"),
    }
    report["gate_passed"] = bool(
        health
        and not severe
        and float(report["market_tape_dropped_max"]) == 0.0
        and float(report["market_tape_invalid_max"]) == 0.0
        and float(report["external_record_dropped_max"]) == 0.0
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--synthetic-iterations", type=int, default=0)
    parser.add_argument("--telemetry-csv", type=Path)
    parser.add_argument("--perf-csv", type=Path)
    parser.add_argument("--health-log", type=Path)
    parser.add_argument("--tick-size", type=float, default=0.1)
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()
    if (
        args.synthetic_iterations <= 0
        and args.telemetry_csv is None
        and args.health_log is None
    ):
        parser.error("select --synthetic-iterations, --telemetry-csv, or --health-log")

    payload: dict[str, object] = {}
    if args.synthetic_iterations > 0:
        payload["synthetic"] = run_synthetic(args.synthetic_iterations)
    if args.telemetry_csv is not None:
        payload["telemetry"] = audit_telemetry(
            args.telemetry_csv,
            tick_size=float(args.tick_size),
        )
        if args.perf_csv is not None:
            with args.telemetry_csv.open(newline="", encoding="utf-8") as handle:
                telemetry_rows = list(csv.DictReader(handle))
            timestamps = [float(row["timestamp"]) for row in telemetry_rows]
            payload["cancel_rate"] = audit_perf_cancel_rates(
                args.perf_csv,
                window_start_ts=min(timestamps),
                window_end_ts=max(timestamps),
            )
    if args.health_log is not None:
        payload["health"] = audit_health_log(args.health_log)
    payload["gate_passed"] = all(
        bool(value.get("gate_passed", False))
        for value in payload.values()
        if isinstance(value, dict) and "gate_passed" in value
    )
    rendered = json.dumps(payload, indent=2, sort_keys=True)
    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if payload["gate_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
