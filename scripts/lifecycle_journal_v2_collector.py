#!/usr/bin/env python3
"""Validate or manifest one journal-v2 remote spool; never transfer data."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.order_lifecycle_remote_spool_v2 import (  # noqa: E402
    inspect_bounded_remote_spool,
    publish_bounded_remote_spool_manifest,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("inspect", "manifest"))
    parser.add_argument("--session-root", required=True)
    parser.add_argument("--epoch-root", required=True)
    parser.add_argument(
        "--allowlisted-root",
        action="append",
        required=True,
        help="Explicit remote spool allowlist root; repeatable.",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    kwargs = {
        "session_root": Path(args.session_root),
        "epoch_root": Path(args.epoch_root),
        "allowlisted_roots": tuple(args.allowlisted_root),
    }
    if args.mode == "inspect":
        payload = inspect_bounded_remote_spool(**kwargs)
        print(json.dumps(payload, sort_keys=True, indent=2))
    else:
        output = publish_bounded_remote_spool_manifest(**kwargs)
        print(str(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
