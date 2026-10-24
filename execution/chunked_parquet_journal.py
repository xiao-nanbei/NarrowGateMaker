"""Bounded-memory, atomic Parquet journal parts for replay mechanics."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Iterator, Mapping
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
    "prospective_inventory_lifecycle_side_id",
)

_ARROW_SCHEMA = pa.schema(
    [
        pa.field("sequence", pa.int64(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("event_ts_ns", pa.int64(), nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("decision_id", pa.string(), nullable=False),
        pa.field("prospective_inventory_lifecycle_side_id", pa.string(), nullable=False),
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


def _read_part_rows(
    path: Path,
    part: Mapping[str, Any],
    expected_sequence: int,
    *,
    verify_checksum: bool,
) -> list[dict[str, Any]]:
    """Validate the fixed journal envelope without rewriting its payload."""
    if path.stat().st_size != int(part["bytes"]):
        raise ValueError(f"journal Parquet part byte count mismatch: {path}")
    if verify_checksum and _sha256_file(path) != str(part["sha256"]):
        raise ValueError(f"journal part hash mismatch: {path}")
    rows = pq.read_table(path, schema=_ARROW_SCHEMA).to_pylist()
    if len(rows) != int(part["rows"]) or not rows:
        raise ValueError(f"journal part row count mismatch: {path}")
    if (int(part["first_sequence"]) != expected_sequence
            or int(part["last_sequence"]) != expected_sequence + len(rows) - 1):
        raise ValueError(f"journal part sequence bounds mismatch: {path}")
    for row in rows:
        if int(row["sequence"]) != expected_sequence:
            raise ValueError(
                f"journal sequence gap: expected {expected_sequence}, found {row['sequence']}"
            )
        expected_sequence += 1
    return rows


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
            "prospective_inventory_lifecycle_side_id": str(
                row.get("prospective_inventory_lifecycle_side_id", "")
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

    def copy_verified_prefix(
        self,
        manifest_path: str | Path,
        *,
        expected_manifest_sha256: str,
        validate_rows: Callable[[list[dict[str, Any]]], None],
        validate_complete: Callable[[], None],
    ) -> None:
        """Import immutable parts into a fresh writer without re-encoding rows.

        The payload owner validates every decoded part and final coverage.
        Only after all checks pass is one open manifest published. Each target
        part is an independent byte copy, checked both during source reading
        and from target read-back; neither links nor source files are modified.
        Failure leaves no accepted prefix and keeps this writer fresh.
        """
        if self._closed or self._row_count or self._parts or self._buffer:
            raise ValueError("journal prefix copy requires a fresh writer")
        source_manifest = Path(manifest_path).expanduser().resolve()
        if (source_manifest.parent == self.output_dir or self.manifest_path.exists()
                or self.manifest_path.with_suffix(".json.partial").exists()
                or any(self.output_dir.glob("part-*"))):
            raise ValueError("journal prefix copy requires an independent empty output")
        encoded = source_manifest.read_bytes()
        if hashlib.sha256(encoded).hexdigest() != expected_manifest_sha256:
            raise ValueError("continuous recovery manifest changed")
        payload = json.loads(encoded)
        if payload.get("schema_version") != CHUNKED_PARQUET_JOURNAL_SCHEMA_VERSION:
            raise ValueError("unexpected chunked journal schema")
        if payload.get("journal_id") != self.journal_id:
            raise ValueError("journal prefix identity differs")
        if not bool(payload.get("closed", False)):
            raise ValueError("journal manifest is not closed")
        source_parts = payload["parts"]
        if len(source_parts) != int(payload["part_count"]):
            raise ValueError("journal manifest part count mismatch")
        copied_parts = []
        owned_paths = []
        expected_sequence = 1
        try:
            for index, part in enumerate(source_parts):
                relative = Path(str(part["path"]))
                if relative.is_absolute() or len(relative.parts) != 1:
                    raise ValueError("journal prefix part must be a relative filename")
                source = source_manifest.parent / relative
                final = self.output_dir / f"part-{index:06d}.parquet"
                partial = final.with_suffix(".parquet.partial")
                if final.exists():
                    raise FileExistsError(f"journal prefix target already exists: {final}")
                digest = hashlib.sha256()
                copied_bytes = 0
                with partial.open("xb") as destination:
                    owned_paths.append(partial)
                    with source.open("rb") as origin:
                        for block in iter(lambda: origin.read(1024 * 1024), b""):
                            destination.write(block)
                            digest.update(block)
                            copied_bytes += len(block)
                    destination.flush()
                    os.fsync(destination.fileno())
                if (copied_bytes != int(part["bytes"])
                        or digest.hexdigest() != str(part["sha256"])):
                    raise ValueError("continuous recovery prefix changed")
                rows = _read_part_rows(
                    partial, part, expected_sequence, verify_checksum=True
                )
                validate_rows(rows)
                expected_sequence += len(rows)
                os.replace(partial, final)
                owned_paths.append(final)
                copied_parts.append({**part, "path": final.name})
            row_count = expected_sequence - 1
            if row_count != int(payload["row_count"]):
                raise ValueError("journal manifest total row count mismatch")
            if row_count != int(payload["last_sequence"]):
                raise ValueError("journal manifest last sequence mismatch")
            validate_complete()
            self._parts = copied_parts
            self._row_count = self._last_sequence = row_count
            self._write_manifest(closed=False)
        except BaseException:
            self._parts = []
            self._row_count = self._last_sequence = 0
            for path in owned_paths:
                path.unlink(missing_ok=True)
            self.manifest_path.unlink(missing_ok=True)
            self.manifest_path.with_suffix(".json.partial").unlink(missing_ok=True)
            raise

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
    verify_checksum: bool = True,
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
        rows = _read_part_rows(
            path, part, expected_sequence, verify_checksum=verify_checksum
        )
        for encoded in rows:
            sequence = int(encoded["sequence"])
            reconstructed = {
                "sequence": sequence,
                "event_type": str(encoded["event_type"]),
                "event_ts_ns": int(encoded["event_ts_ns"]),
                "side": str(encoded["side"]),
                "decision_id": str(encoded["decision_id"]),
                "prospective_inventory_lifecycle_side_id": str(
                    encoded["prospective_inventory_lifecycle_side_id"]
                ),
            }
            reconstructed.update(json.loads(str(encoded["payload_json"])))
            yield reconstructed
            expected_sequence += 1
            observed_rows += 1
    if observed_rows != int(payload.get("row_count", -1)):
        raise ValueError("journal manifest total row count mismatch")
