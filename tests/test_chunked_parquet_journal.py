from __future__ import annotations

import json
from pathlib import Path

import pytest

from execution.chunked_parquet_journal import (
    ChunkedParquetJournalWriter,
    iter_chunked_parquet_journal,
)


def test_chunked_parquet_journal_is_atomic_hash_verified_and_bounded(tmp_path) -> None:
    output = tmp_path / "mechanics"
    writer = ChunkedParquetJournalWriter(
        output,
        journal_id="test.mechanics.v1",
        chunk_rows=2,
    )
    for sequence in range(1, 6):
        writer.append(
            {
                "sequence": sequence,
                "event_type": "quote_decision",
                "event_ts_ns": sequence * 100,
                "side": "BUY",
                "decision_id": f"d-{sequence}",
                "prospective_campaign_side_id": "lineage-1",
                "nested": {"sequence": sequence},
            }
        )
    manifest = writer.close()

    assert manifest["closed"] is True
    assert manifest["row_count"] == 5
    assert manifest["part_count"] == 3
    rows = list(iter_chunked_parquet_journal(output / "manifest.json"))
    assert [row["sequence"] for row in rows] == [1, 2, 3, 4, 5]
    assert rows[-1]["nested"] == {"sequence": 5}

    payload = json.loads((output / "manifest.json").read_text())
    payload["parts"][0]["sha256"] = "0" * 64
    (output / "manifest.json").write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="hash mismatch"):
        list(iter_chunked_parquet_journal(output / "manifest.json"))


def test_chunked_parquet_journal_rejects_removable_volume() -> None:
    removable_root = Path("/", "Volumes", "NARROWGATE_TEST_REMOVABLE")
    with pytest.raises(ValueError, match="local cache disk"):
        ChunkedParquetJournalWriter(
            removable_root / "forbidden-replay-cache",
            journal_id="test.removable.v1",
        )
