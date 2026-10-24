from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from models.audit.native_exchange_book_universe import (
    build_native_exchange_book_universe,
)


def _hour_path(
    root: Path,
    timestamp: datetime,
) -> Path:
    return (
        root
        / "binance_futures"
        / timestamp.strftime("%Y-%m-%d")
        / timestamp.strftime("%H")
        / "BTCUSDC_orderbook.parquet.zst"
    )


def test_source_universe_excludes_incomplete_warmup_and_hashes_inputs(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    complete_day = datetime(2026, 1, 2, tzinfo=timezone.utc)
    incomplete_day = datetime(2026, 1, 4, tzinfo=timezone.utc)
    for day, include_warmup in (
        (complete_day, True),
        (incomplete_day, False),
    ):
        start = day - timedelta(hours=1 if include_warmup else 0)
        end = day + timedelta(days=1)
        current = start
        while current < end:
            path = _hour_path(raw_root, current)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(current.isoformat().encode("ascii"))
            current += timedelta(hours=1)

    eligible = tmp_path / "eligible.csv"
    pd.DataFrame(
        {"day": ["2026-01-02", "2026-01-04"]}
    ).to_csv(eligible, index=False)
    files_manifest = tmp_path / "raw.sha256"
    sequence_audit = tmp_path / "sequence.json"
    sequence_audit.write_text(
        json.dumps(
            {
                "range_audits": [
                    {
                        "symbol": "BTCUSDC",
                        "range_start_utc": "2026-01-01T00:00:00+00:00",
                        "sequence_audit": {
                            "sequence_gaps": 0,
                            "invalid_sequence_messages": 0,
                            "message_time_reversals": 0,
                        },
                    },
                    {
                        "symbol": "BTCUSDC",
                        "range_start_utc": "2026-01-02T00:00:00+00:00",
                        "sequence_audit": {
                            "sequence_gaps": 0,
                            "invalid_sequence_messages": 0,
                            "message_time_reversals": 0,
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    payload = build_native_exchange_book_universe(
        raw_root=raw_root,
        eligible_days_path=eligible,
        raw_files_manifest_path=files_manifest,
        sequence_audit_path=sequence_audit,
        split={"train": ["2026-01-02"]},
        symbol="BTCUSDC",
        exchange="binance_futures",
        tick_size=0.1,
        warmup_hours=1,
    )

    assert payload["native_complete_days_count"] == 1
    assert payload["split"]["eligible_native_days"] == ["2026-01-02"]
    assert "2026-01-04" in payload["excluded_days"]
    assert payload["raw_unique_file_count"] == 25
    assert len(files_manifest.read_text(encoding="utf-8").splitlines()) == 25
