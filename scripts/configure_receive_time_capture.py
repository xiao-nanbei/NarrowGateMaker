#!/usr/bin/env python3
"""Toggle bounded receive-time recording without changing strategy fields."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


def _canonical_hash(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _file_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _section_bounds(lines: list[str], name: str) -> tuple[int, int]:
    start = next(
        (index for index, line in enumerate(lines) if line.startswith(f"{name}:")),
        None,
    )
    if start is None:
        raise ValueError(f"config has no {name} section")
    end = len(lines)
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line.strip() and not line.startswith((" ", "\t", "#")):
            end = index
            break
    return start, end


def _set_indented_field(
    block: list[str],
    *,
    indent: int,
    key: str,
    value: str,
) -> list[str]:
    pattern = re.compile(rf"^\s{{{indent}}}{re.escape(key)}\s*:")
    for index, line in enumerate(block):
        if pattern.match(line):
            ending = "\n" if line.endswith("\n") else ""
            block[index] = f"{' ' * indent}{key}: {value}{ending}"
            return block
    block.append(f"{' ' * indent}{key}: {value}\n")
    return block


def update_capture_config(
    text: str,
    *,
    enabled: bool,
    queue_size: int = 20_000,
) -> tuple[str, list[str]]:
    if queue_size <= 0:
        raise ValueError("queue_size must be positive")
    lines = text.splitlines(keepends=True)
    ext_start, ext_end = _section_bounds(lines, "external_venues")
    source_starts = [
        index
        for index in range(ext_start + 1, ext_end)
        if re.match(r"^\s{4}- venue\s*:", lines[index])
    ]
    if not source_starts:
        raise ValueError("external_venues has no sources")
    updated_sources: list[str] = []
    for reverse_index in range(len(source_starts) - 1, -1, -1):
        block_start = source_starts[reverse_index]
        block_end = (
            source_starts[reverse_index + 1]
            if reverse_index + 1 < len(source_starts)
            else ext_end
        )
        block = lines[block_start:block_end]
        venue = re.search(r"venue\s*:\s*[\"']?([^\"'\s]+)", block[0])
        instrument_line = next(
            (line for line in block if re.match(r"^\s{6}instrument_type\s*:", line)),
            "",
        )
        instrument = re.search(
            r"instrument_type\s*:\s*[\"']?([^\"'\s]+)", instrument_line
        )
        if venue is None or instrument is None:
            raise ValueError("external source missing venue/instrument_type")
        block = _set_indented_field(
            block,
            indent=6,
            key="record_enabled",
            value="true" if enabled else "false",
        )
        block = _set_indented_field(
            block,
            indent=6,
            key="record_queue_size",
            value=str(queue_size),
        )
        lines[block_start:block_end] = block
        ext_end += len(block) - (block_end - block_start)
        updated_sources.append(f"{venue.group(1).lower()}:{instrument.group(1).lower()}")

    log_start, log_end = _section_bounds(lines, "logging")
    logging_block = lines[log_start:log_end]
    logging_block = _set_indented_field(
        logging_block,
        indent=2,
        key="market_tape_enabled",
        value="true" if enabled else "false",
    )
    logging_block = _set_indented_field(
        logging_block,
        indent=2,
        key="market_tape_queue_size",
        value=str(queue_size),
    )
    lines[log_start:log_end] = logging_block
    return "".join(lines), sorted(updated_sources)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("live/config.yaml"))
    state = parser.add_mutually_exclusive_group(required=True)
    state.add_argument("--enable", action="store_true")
    state.add_argument("--disable", action="store_true")
    parser.add_argument("--queue-size", type=int, default=20_000)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-backup", action="store_true")
    parser.add_argument("--marker", type=Path)
    args = parser.parse_args()

    path = args.config.expanduser().resolve()
    original = path.read_text(encoding="utf-8")
    original_yaml = yaml.safe_load(original) or {}
    original_strategy_hash = _canonical_hash(original_yaml.get("strategy", {}))
    updated, sources = update_capture_config(
        original,
        enabled=bool(args.enable),
        queue_size=args.queue_size,
    )
    updated_yaml = yaml.safe_load(updated) or {}
    updated_strategy_hash = _canonical_hash(updated_yaml.get("strategy", {}))
    if updated_strategy_hash != original_strategy_hash:
        raise RuntimeError("capture toggle changed strategy configuration")
    if args.dry_run:
        print(
            "".join(
                difflib.unified_diff(
                    original.splitlines(keepends=True),
                    updated.splitlines(keepends=True),
                    fromfile=str(path),
                    tofile=str(path),
                )
            )
        )
        return 0

    if updated != original:
        if not args.no_backup:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            shutil.copy2(path, path.with_name(f"{path.name}.capture.{stamp}.bak"))
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(updated, encoding="utf-8")
        os.replace(temporary, path)

    marker = {
        "schema_version": "receive_time_capture_toggle.v1",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "config_path": str(path),
        "capture_enabled": bool(args.enable),
        "queue_size": args.queue_size,
        "sources": sources,
        "strategy_hash": updated_strategy_hash,
        "config_sha256_before": _file_hash(original),
        "config_sha256_after": _file_hash(updated),
        "changed": updated != original,
    }
    if args.marker:
        args.marker.parent.mkdir(parents=True, exist_ok=True)
        args.marker.write_text(
            json.dumps(marker, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    print(json.dumps(marker, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
