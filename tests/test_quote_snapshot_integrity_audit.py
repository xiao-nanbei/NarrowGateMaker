from __future__ import annotations

import csv
from pathlib import Path

from scripts.audit_quote_snapshot_integrity import (
    audit_health_log,
    audit_perf_cancel_rates,
    audit_telemetry,
    run_synthetic,
)
from strategy.maker_engine import QuoteSnapshotIntegrityLogRow


def test_synthetic_quote_snapshot_integrity_gate() -> None:
    report = run_synthetic(100)

    assert report["gate_passed"] is True
    assert report["invalid_reasons"] == {}
    assert report["mid_identity_violations"] == 0
    assert report["microprice_violations"] == 0
    assert report["routing_violations"] == 0


def test_quote_snapshot_telemetry_audit(tmp_path: Path) -> None:
    path = tmp_path / "quote_snapshot_integrity.csv"
    fields = list(QuoteSnapshotIntegrityLogRow.__dataclass_fields__)
    rows = []
    for index, timestamp in enumerate((1_000.0, 1_010.0), start=1):
        row = {field: 0 for field in fields}
        row.update(
            timestamp=timestamp,
            symbol="BTCUSDC",
            requote_id=index,
            status="ok",
            snapshot_valid=1,
            market_generation=index,
            depth_generation=index,
            guard_source="book_ticker",
            guard_fallback_reason="",
            pricing_mid=100.0,
            final_bid=99.9,
            final_ask=100.1,
            snapshot_lock_wait_us=10.0,
            snapshot_lock_hold_us=20.0,
            bid_action="place",
            ask_action="place",
        )
        rows.append(row)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    report = audit_telemetry(path)

    assert report["gate_passed"] is True
    assert report["rows"] == 2
    assert report["post_only_violation_count"] == 0
    assert report["final_tick_mismatch_count"] == 0
    assert report["valid_generation_rows"] == 2


def test_perf_cancel_rate_comparison_uses_equal_windows(tmp_path: Path) -> None:
    path = tmp_path / "live_perf_telemetry.csv"
    fields = [
        "timestamp",
        "rest_cancel_count",
        "rest_cancel_all_count",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(
            [
                {"timestamp": 900.0, "rest_cancel_count": 1, "rest_cancel_all_count": 0},
                {"timestamp": 950.0, "rest_cancel_count": 1, "rest_cancel_all_count": 0},
                {"timestamp": 1_000.0, "rest_cancel_count": 2, "rest_cancel_all_count": 0},
                {"timestamp": 1_050.0, "rest_cancel_count": 2, "rest_cancel_all_count": 0},
            ]
        )

    report = audit_perf_cancel_rates(
        path,
        window_start_ts=1_000.0,
        window_end_ts=1_100.0,
    )

    assert report["current_cancels"] == 4
    assert report["prior_cancels"] == 2
    assert report["incremental_cancel_per_hour"] == 72.0


def test_runtime_health_audit_reports_queue_metrics(tmp_path: Path) -> None:
    path = tmp_path / "maker-window.log"
    path.write_text(
        "2026-08-04 INFO HEALTH marketTapeDropped=0 marketTapeInvalid=0 "
        "marketTapeQueueHwm=7 marketTapeMaxQueueAgeMs=1.5 "
        "externalRecordDropped=0 externalRecordHwm=4 "
        "externalRecordMaxAgeMs=2.5 deepBookBuffer=3 deepBookGaps=0\n",
        encoding="utf-8",
    )

    report = audit_health_log(path)

    assert report["gate_passed"] is True
    assert report["market_tape_queue_hwm_max"] == 7.0
    assert report["external_record_max_age_ms"] == 2.5
