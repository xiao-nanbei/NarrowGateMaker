import json

import pandas as pd

from models.audit.freeze_lifecycle_panel_subset import (
    freeze_lifecycle_panel_subset,
)
from models.audit.native_lifecycle_universe_v1 import STRICT_QUEUE_SCOPE


def _daily(day: str):
    return {
        "day": day,
        "lifecycle_rows": 2,
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
        "exchange_book_queue_lookup_count": 10,
        "exchange_book_queue_exact_count": 8,
        "exchange_book_queue_known_zero_count": 2,
        "exchange_book_queue_missing_count": 0,
    }


def test_freeze_development_lifecycle_subset(tmp_path) -> None:
    partial = tmp_path / "source.partial"
    partial.mkdir()
    days = ["2026-01-01", "2026-01-02"]
    (partial / "run_identity.json").write_text(
        json.dumps({"days": days, "workspace_sha256": "workspace"}),
        encoding="utf-8",
    )
    for day in days:
        frame = pd.DataFrame(
            {
                "day": [day, day],
                "order_id": [1, 1],
                "event": ["submit", "terminal"],
            }
        )
        if day == "2026-01-02":
            frame = frame[["event", "day", "order_id"]]
        frame.to_parquet(partial / f"{day}.lifecycle.parquet", index=False)
        (partial / f"{day}.daily.json").write_text(
            json.dumps(_daily(day)),
            encoding="utf-8",
        )
    split = tmp_path / "split.json"
    split.write_text(
        json.dumps(
            {
                "schema_version": "strict_native_evidence_split.v1",
                "panels": {
                    "development": {
                        "days": days,
                        "trainable": True,
                    },
                    "validation": {
                        "days": [],
                        "trainable": False,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    payload = freeze_lifecycle_panel_subset(
        partial_dir=partial,
        evidence_split_path=split,
        output_prefix=tmp_path / "development",
    )

    assert payload["day_count"] == 2
    assert payload["lifecycle_rows"] == 4
    frozen = pd.read_parquet(tmp_path / "development.lifecycle.parquet")
    assert len(frozen) == 4
    assert (tmp_path / "development.manifest.json").is_file()
