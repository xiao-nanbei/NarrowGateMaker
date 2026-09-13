#!/usr/bin/env python3
"""Freeze one evidence panel from immutable per-day lifecycle partitions."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from models.audit.native_lifecycle_universe_v1 import (
    lifecycle_integrity_reasons,
)

SCHEMA_VERSION = "lifecycle_panel_subset.v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _panel_days(split: dict[str, Any], panel: str) -> list[str]:
    if str(split.get("schema_version", "")) != (
        "strict_native_evidence_split.v1"
    ):
        raise ValueError("unsupported strict evidence split")
    panels = split.get("panels")
    if not isinstance(panels, dict) or panel not in panels:
        raise ValueError(f"panel is absent from strict split: {panel}")
    spec = panels[panel]
    if panel != "development" or not bool(spec.get("trainable", False)):
        raise ValueError("this freezer only permits the Development panel")
    days = [str(day) for day in spec.get("days", [])]
    if not days or days != sorted(set(days)):
        raise ValueError("Development days must be unique and chronological")
    return days


def _stream_concat_parquet(paths: list[Path], output: Path) -> int:
    writer: pq.ParquetWriter | None = None
    schema: pa.Schema | None = None
    rows = 0
    try:
        for path in paths:
            parquet = pq.ParquetFile(path)
            rows += int(parquet.metadata.num_rows)
            for batch in parquet.iter_batches(batch_size=100_000):
                table = pa.Table.from_batches([batch])
                if writer is None:
                    schema = table.schema
                    writer = pq.ParquetWriter(
                        output,
                        schema,
                        compression="zstd",
                    )
                elif table.schema != schema:
                    if set(table.schema.names) != set(schema.names):
                        missing = sorted(
                            set(schema.names) - set(table.schema.names)
                        )
                        extra = sorted(
                            set(table.schema.names) - set(schema.names)
                        )
                        raise ValueError(
                            "lifecycle parquet schema fields differ: "
                            f"missing={missing} extra={extra}"
                        )
                    table = table.select(schema.names)
                    table = table.cast(schema)
                writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise ValueError("no lifecycle rows to freeze")
    return rows


def freeze_lifecycle_panel_subset(
    *,
    partial_dir: Path,
    evidence_split_path: Path,
    output_prefix: Path,
    normalized_book_manifest_path: Path | None = None,
    panel: str = "development",
    max_queue_missing_ratio: float = 0.001,
) -> dict[str, Any]:
    partial = partial_dir.expanduser().resolve()
    split_path = evidence_split_path.expanduser().resolve()
    output = output_prefix.expanduser().resolve()
    split = json.loads(split_path.read_text(encoding="utf-8"))
    days = _panel_days(split, panel)
    run_identity_path = partial / "run_identity.json"
    run_identity = json.loads(
        run_identity_path.read_text(encoding="utf-8")
    )
    if not set(days).issubset(set(run_identity.get("days", []))):
        raise ValueError("Development days are absent from source run identity")

    lifecycle_output = output.with_suffix(".lifecycle.parquet")
    daily_output = output.with_suffix(".daily.csv")
    days_output = output.with_suffix(".days.csv")
    manifest_output = output.with_suffix(".manifest.json")
    for path in (
        lifecycle_output,
        daily_output,
        days_output,
        manifest_output,
    ):
        if path.exists():
            raise FileExistsError(f"refusing to overwrite panel artifact: {path}")

    partitions: list[Path] = []
    daily_rows: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    for day in days:
        lifecycle_path = partial / f"{day}.lifecycle.parquet"
        daily_path = partial / f"{day}.daily.json"
        if not lifecycle_path.is_file() or not daily_path.is_file():
            raise FileNotFoundError(f"incomplete Development partition: {day}")
        daily = json.loads(daily_path.read_text(encoding="utf-8"))
        reasons = lifecycle_integrity_reasons(
            daily,
            expected_day=day,
            max_queue_missing_ratio=max_queue_missing_ratio,
        )
        if reasons:
            raise ValueError(f"{day}: lifecycle integrity failed: {reasons}")
        partitions.append(lifecycle_path)
        daily_rows.append(daily)
        artifacts.append(
            {
                "day": day,
                "lifecycle_path": str(lifecycle_path),
                "lifecycle_sha256": _sha256(lifecycle_path),
                "daily_path": str(daily_path),
                "daily_sha256": _sha256(daily_path),
            }
        )

    output.parent.mkdir(parents=True, exist_ok=True)
    lifecycle_rows = _stream_concat_parquet(partitions, lifecycle_output)
    pd.DataFrame(daily_rows).to_csv(daily_output, index=False)
    pd.DataFrame({"day": days}).to_csv(days_output, index=False)
    normalized_manifest = (
        normalized_book_manifest_path.expanduser().resolve()
        if normalized_book_manifest_path is not None
        else None
    )
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "panel": panel,
        "day_count": len(days),
        "days": days,
        "lifecycle_rows": int(lifecycle_rows),
        "source_partial_dir": str(partial),
        "source_run_identity_path": str(run_identity_path),
        "source_run_identity_sha256": _sha256(run_identity_path),
        "source_workspace_sha256": str(
            run_identity.get("workspace_sha256", "")
        ),
        "evidence_split_path": str(split_path),
        "evidence_split_sha256": _sha256(split_path),
        "normalized_book_manifest_path": (
            str(normalized_manifest) if normalized_manifest is not None else ""
        ),
        "normalized_book_manifest_sha256": (
            _sha256(normalized_manifest)
            if normalized_manifest is not None
            else ""
        ),
        "max_queue_missing_ratio": float(max_queue_missing_ratio),
        "source_artifacts": artifacts,
        "source_artifacts_sha256": _canonical_sha256(artifacts),
        "lifecycle_output_path": str(lifecycle_output),
        "lifecycle_output_sha256": _sha256(lifecycle_output),
        "daily_output_path": str(daily_output),
        "daily_output_sha256": _sha256(daily_output),
        "days_output_path": str(days_output),
        "days_output_sha256": _sha256(days_output),
        "manifest_output_path": str(manifest_output),
    }
    payload["identity_sha256"] = _canonical_sha256(payload)
    manifest_output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partial-dir", type=Path, required=True)
    parser.add_argument("--evidence-split", type=Path, required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--normalized-book-manifest", type=Path)
    parser.add_argument("--panel", choices=("development",), default="development")
    parser.add_argument("--max-queue-missing-ratio", type=float, default=0.001)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = freeze_lifecycle_panel_subset(
        partial_dir=args.partial_dir,
        evidence_split_path=args.evidence_split,
        output_prefix=args.output_prefix,
        normalized_book_manifest_path=args.normalized_book_manifest,
        panel=args.panel,
        max_queue_missing_ratio=args.max_queue_missing_ratio,
    )
    print(
        json.dumps(
            {
                "panel": payload["panel"],
                "days": payload["day_count"],
                "lifecycle_rows": payload["lifecycle_rows"],
                "identity_sha256": payload["identity_sha256"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
