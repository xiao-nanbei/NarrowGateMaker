"""Bounded-memory, atomic Parquet journal parts for replay mechanics."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

CHUNKED_PARQUET_JOURNAL_SCHEMA_VERSION = "chunked_parquet_journal.v1"

_BASE_FIELDS = (
    "sequence",
    "event_type",
    "event_ts_ns",
    "side",
    "decision_id",
    "prospective_campaign_side_id",
)

_ARROW_SCHEMA = pa.schema(
    [
        pa.field("sequence", pa.int64(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("event_ts_ns", pa.int64(), nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("decision_id", pa.string(), nullable=False),
        pa.field("prospective_campaign_side_id", pa.string(), nullable=False),
        pa.field("payload_json", pa.string(), nullable=False),
    ]
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_default(value: Any) -> Any:
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"unsupported journal payload value: {type(value).__name__}")


class ChunkedParquetJournalWriter:
    """Write mechanics rows in atomic, hash-addressed Parquet parts.

    The wide event payload is encoded as canonical JSON so event-specific
    fields cannot make schemas drift between chunks. The small fixed prefix is
    retained as native Parquet columns for sequential validation and pruning.
    """

    def __init__(
        self,
        output_dir: str | Path,
        *,
        journal_id: str,
        chunk_rows: int = 50_000,
        reject_removable_volume: bool = True,
    ) -> None:
        self.output_dir = Path(output_dir).expanduser().resolve()
        if reject_removable_volume and str(self.output_dir).startswith("/Volumes/"):
            raise ValueError("reusable replay journals must remain on the local cache disk")
        self.journal_id = str(journal_id).strip()
        if not self.journal_id:
            raise ValueError("journal_id is required")
        self.chunk_rows = int(chunk_rows)
        if self.chunk_rows < 1:
            raise ValueError("chunk_rows must be positive")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.output_dir / "manifest.json"
        if self.manifest_path.exists() or any(self.output_dir.glob("part-*.parquet")):
            raise FileExistsError(
                f"journal output must be empty for a new immutable run: {self.output_dir}"
            )
        self._buffer: list[dict[str, Any]] = []
        self._parts: list[dict[str, Any]] = []
        self._row_count = 0
        self._last_sequence = 0
        self._closed = False

    @property
    def row_count(self) -> int:
        return int(self._row_count)

    @property
    def part_count(self) -> int:
        return len(self._parts)

    @property
    def closed(self) -> bool:
        return bool(self._closed)

    def append(self, row: Mapping[str, Any]) -> None:
        if self._closed:
            raise RuntimeError("cannot append to a closed journal")
        sequence = int(row.get("sequence", 0) or 0)
        if sequence != self._last_sequence + 1:
            raise ValueError(
                f"journal sequence must be contiguous: expected {self._last_sequence + 1}, "
                f"found {sequence}"
            )
        event_type = str(row.get("event_type", "")).strip()
        if not event_type:
            raise ValueError("journal event_type is required")
        payload = {key: value for key, value in row.items() if key not in _BASE_FIELDS}
        encoded = {
            "sequence": sequence,
            "event_type": event_type,
            "event_ts_ns": int(row.get("event_ts_ns", 0) or 0),
            "side": str(row.get("side", "")),
            "decision_id": str(row.get("decision_id", "")),
            "prospective_campaign_side_id": str(
                row.get("prospective_campaign_side_id", "")
            ),
            "payload_json": json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
                default=_json_default,
            ),
        }
        self._buffer.append(encoded)
        self._row_count += 1
        self._last_sequence = sequence
        if len(self._buffer) >= self.chunk_rows:
            self.flush()

    def flush(self) -> None:
        if self._closed:
            raise RuntimeError("cannot flush a closed journal")
        if not self._buffer:
            return
        part_index = len(self._parts)
        final_path = self.output_dir / f"part-{part_index:06d}.parquet"
        partial_path = final_path.with_suffix(".parquet.partial")
        table = pa.Table.from_pylist(self._buffer, schema=_ARROW_SCHEMA)
        pq.write_table(table, partial_path, compression="zstd")
        os.replace(partial_path, final_path)
        self._parts.append(
            {
                "path": final_path.name,
                "rows": int(len(self._buffer)),
                "first_sequence": int(self._buffer[0]["sequence"]),
                "last_sequence": int(self._buffer[-1]["sequence"]),
                "bytes": int(final_path.stat().st_size),
                "sha256": _sha256_file(final_path),
            }
        )
        self._buffer.clear()
        self._write_manifest(closed=False)

    def _write_manifest(self, *, closed: bool) -> None:
        payload = {
            "schema_version": CHUNKED_PARQUET_JOURNAL_SCHEMA_VERSION,
            "journal_id": self.journal_id,
            "closed": bool(closed),
            "row_count": int(self._row_count),
            "last_sequence": int(self._last_sequence),
            "part_count": int(len(self._parts)),
            "parts": list(self._parts),
        }
        partial = self.manifest_path.with_suffix(".json.partial")
        partial.write_text(
            json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )
        os.replace(partial, self.manifest_path)

    def close(self) -> dict[str, Any]:
        if not self._closed:
            self.flush()
            self._closed = True
            self._write_manifest(closed=True)
        return json.loads(self.manifest_path.read_text(encoding="utf-8"))

    def __enter__(self) -> ChunkedParquetJournalWriter:
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            self.close()


def iter_chunked_parquet_journal(
    manifest_path: str | Path,
    *,
    require_closed: bool = True,
) -> Iterator[dict[str, Any]]:
    """Yield reconstructed journal rows after manifest and part validation."""

    manifest_file = Path(manifest_path).expanduser().resolve()
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))
    if payload.get("schema_version") != CHUNKED_PARQUET_JOURNAL_SCHEMA_VERSION:
        raise ValueError("unexpected chunked journal schema")
    if require_closed and not bool(payload.get("closed", False)):
        raise ValueError("journal manifest is not closed")
    expected_sequence = 1
    observed_rows = 0
    for part in payload.get("parts", []):
        path = manifest_file.parent / str(part["path"])
        if _sha256_file(path) != str(part["sha256"]):
            raise ValueError(f"journal part hash mismatch: {path}")
        table = pq.read_table(path, schema=_ARROW_SCHEMA)
        rows = table.to_pylist()
        if len(rows) != int(part["rows"]):
            raise ValueError(f"journal part row count mismatch: {path}")
        for encoded in rows:
            sequence = int(encoded["sequence"])
            if sequence != expected_sequence:
                raise ValueError(
                    f"journal sequence gap: expected {expected_sequence}, found {sequence}"
                )
            reconstructed = {
                "sequence": sequence,
                "event_type": str(encoded["event_type"]),
                "event_ts_ns": int(encoded["event_ts_ns"]),
                "side": str(encoded["side"]),
                "decision_id": str(encoded["decision_id"]),
                "prospective_campaign_side_id": str(
                    encoded["prospective_campaign_side_id"]
                ),
            }
            reconstructed.update(json.loads(str(encoded["payload_json"])))
            yield reconstructed
            expected_sequence += 1
            observed_rows += 1
    if observed_rows != int(payload.get("row_count", -1)):
        raise ValueError("journal manifest total row count mismatch")
