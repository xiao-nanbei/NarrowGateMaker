#!/usr/bin/env python3
"""Audit legacy v10-v13 replay caches without touching their contents."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.replay_cache_audit import (  # noqa: E402
    audit_json,
    audit_legacy_window_caches,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cache-root",
        type=Path,
        default=Path.home() / "Library" / "Caches" / "NarrowGate_BTCUSDC" / "window_cache",
    )
    parser.add_argument(
        "--reference-root",
        action="append",
        type=Path,
        default=[],
        help="Text tree to scan for exact legacy cache basenames; repeatable.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional report path. Omit for a strictly zero-write run.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    roots = args.reference_root or [ROOT]
    payload = audit_legacy_window_caches(
        args.cache_root,
        reference_roots=roots,
    )
    rendered = audit_json(payload)
    if args.output is None:
        sys.stdout.write(rendered)
        return 0
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(output)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
