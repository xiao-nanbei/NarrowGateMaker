import json

from models.audit.native_lifecycle_universe_v1 import (
    STRICT_QUEUE_SCOPE,
    freeze_native_lifecycle_universe,
    lifecycle_integrity_reasons,
)


def _record(day: str, **overrides):
    values = {
        "day": day,
        "lifecycle_rows": 10,
        "exchange_book_events_accepted": 100,
        "exchange_book_snapshot_events": 1,
        "exchange_book_queue_mode": "strict",
        "exchange_book_queue_scope": STRICT_QUEUE_SCOPE,
        "exchange_book_source_gap_events": 0,
        "exchange_book_invalid_sequence_messages": 0,
        "exchange_book_sequence_gaps": 0,
        "exchange_book_message_time_reversals": 0,
        "exchange_book_delta_bootstrap_events": 0,
        "exchange_book_event_timestamp_fallback_events": 0,
        "exchange_book_receive_timestamp_fallback_events": 0,
        "exchange_book_unknown_timestamp_source_events": 0,
        "exchange_book_queue_lookup_count": 100,
        "exchange_book_queue_exact_count": 70,
        "exchange_book_queue_known_zero_count": 30,
        "exchange_book_queue_missing_count": 0,
    }
    values.update(overrides)
    return values


def test_lifecycle_integrity_rejects_bootstrap_and_missing_queue() -> None:
    reasons = lifecycle_integrity_reasons(
        _record(
            "2026-01-01",
            exchange_book_delta_bootstrap_events=1,
            exchange_book_queue_missing_count=2,
        ),
        expected_day="2026-01-01",
        max_queue_missing_ratio=0.001,
    )

    assert reasons == [
        "exchange_book_delta_bootstrap_events",
        "queue_missing_ratio",
    ]


def test_lifecycle_universe_freezes_only_integrity_complete_days(
    tmp_path,
) -> None:
    candidates = tmp_path / "days.csv"
    partial = tmp_path / "partial"
    audit = tmp_path / "audit.csv"
    strict = tmp_path / "strict.csv"
    candidates.write_text(
        "day\n2026-01-01\n2026-01-02\n",
        encoding="utf-8",
    )
    partial.mkdir()
    (partial / "2026-01-01.daily.json").write_text(
        json.dumps(_record("2026-01-01")),
        encoding="utf-8",
    )
    (partial / "2026-01-02.daily.json").write_text(
        json.dumps(
            _record(
                "2026-01-02",
                exchange_book_sequence_gaps=1,
            )
        ),
        encoding="utf-8",
    )

    payload = freeze_native_lifecycle_universe(
        candidate_days_path=candidates,
        partial_dir=partial,
        audit_output_path=audit,
        strict_days_output_path=strict,
    )

    assert payload["candidate_days_count"] == 2
    assert payload["strict_days_count"] == 1
    assert payload["excluded_days_count"] == 1
    assert strict.read_text(encoding="utf-8") == "day\n2026-01-01\n"
    assert audit.with_suffix(".manifest.json").is_file()
