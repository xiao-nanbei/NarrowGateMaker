from models.audit.native_exchange_book_sequence_audit_v2 import (
    _contiguous_ranges,
    evaluate_target_day_audit,
)


def test_sequence_ranges_do_not_bridge_missing_good_days() -> None:
    ranges = _contiguous_ranges(
        [
            "2026-01-02",
            "2026-01-03",
            "2026-01-05",
            "2026-01-06",
            "2026-01-07",
        ],
        max_days=2,
    )

    assert ranges == [
        ["2026-01-02", "2026-01-03"],
        ["2026-01-05", "2026-01-06"],
        ["2026-01-07"],
    ]


def test_target_day_requires_snapshot_seed_and_continuous_updates() -> None:
    eligible, reasons = evaluate_target_day_audit(
        {
            "target_initialized_at_start": True,
            "target_initialization_source_at_start": "snapshot",
            "target_accepted_updates": 100,
            "target_sequence_gaps": 0,
            "target_invalid_sequence_messages": 0,
            "target_message_time_reversals": 0,
        }
    )

    assert eligible
    assert reasons == []


def test_target_day_reports_each_strict_sequence_failure() -> None:
    eligible, reasons = evaluate_target_day_audit(
        {
            "target_initialized_at_start": False,
            "target_initialization_source_at_start": "",
            "target_accepted_updates": 0,
            "target_sequence_gaps": 1,
            "target_invalid_sequence_messages": 2,
            "target_message_time_reversals": 3,
        }
    )

    assert not eligible
    assert reasons == [
        "not_initialized_at_target_start",
        "target_not_snapshot_seeded",
        "no_target_updates",
        "target_sequence_gap",
        "target_invalid_sequence",
        "target_time_reversal",
    ]
