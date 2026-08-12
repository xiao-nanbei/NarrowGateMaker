#!/usr/bin/env python3
"""Generate the read-only non-window replay-cache audit."""

from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

from data_paths import cache_root, external_cache_root
from models.replay_cache_node_audit import (
    audit_json,
    audit_markdown,
    audit_replay_cache_nodes,
)

ROOT = Path(__file__).resolve().parents[1]


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cache-root",
        type=Path,
        default=cache_root(ROOT),
    )
    parser.add_argument(
        "--external-cache-root",
        type=Path,
        default=external_cache_root(ROOT),
    )
    parser.add_argument(
        "--reference-root",
        action="append",
        type=Path,
        default=[],
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=ROOT / "docs/replay_cache_non_window_audit_20260804.json",
    )
    parser.add_argument(
        "--output-markdown",
        type=Path,
        default=ROOT / "docs/replay_cache_non_window_audit_20260804.md",
    )
    parser.add_argument(
        "--skip-payload-hash-verification",
        action="store_true",
    )
    args = parser.parse_args()
    references = args.reference_root or [ROOT]
    payload = audit_replay_cache_nodes(
        cache_root=args.cache_root,
        external_cache_root=args.external_cache_root,
        repository_root=ROOT,
        reference_roots=references,
        verify_payload_hashes=not args.skip_payload_hash_verification,
        excluded_paths=(args.output_json, args.output_markdown),
    )
    _atomic_write(args.output_json, audit_json(payload))
    _atomic_write(args.output_markdown, audit_markdown(payload))
    print(args.output_json)
    print(args.output_markdown)


if __name__ == "__main__":
    main()
