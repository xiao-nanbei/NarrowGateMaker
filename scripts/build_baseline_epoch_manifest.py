#!/usr/bin/env python3
"""Build a fail-closed baseline epoch manifest from frozen identity files."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.replay.baseline_epoch_manifest import (  # noqa: E402
    build_manifest_from_baseline_identities,
    utc_timestamp_ns,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--identity", nargs="+", required=True, type=Path)
    parser.add_argument("--manifest-id", required=True)
    parser.add_argument("--scope-start-utc", required=True)
    parser.add_argument("--scope-end-utc", required=True)
    parser.add_argument("--overrides-json", type=Path)
    parser.add_argument("--first-decisions-json", type=Path)
    parser.add_argument("--boundary-events-json", type=Path)
    parser.add_argument("--restart-audit-complete", action="store_true")
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    overrides = None
    if args.overrides_json is not None:
        overrides = json.loads(
            args.overrides_json.expanduser().resolve().read_text(encoding="utf-8")
        )
    first_decisions = None
    if args.first_decisions_json is not None:
        first_decisions = json.loads(
            args.first_decisions_json.expanduser().resolve().read_text(encoding="utf-8")
        )
    boundary_events = []
    if args.boundary_events_json is not None:
        boundary_events = json.loads(
            args.boundary_events_json.expanduser().resolve().read_text(encoding="utf-8")
        )
    manifest = build_manifest_from_baseline_identities(
        args.identity,
        manifest_id=args.manifest_id,
        scope_start_ts_ns=utc_timestamp_ns(args.scope_start_utc),
        scope_end_ts_ns=utc_timestamp_ns(args.scope_end_utc),
        overrides_by_baseline_id=overrides,
        first_decision_ts_by_baseline_id=first_decisions,
        boundary_events=boundary_events,
        restart_audit_complete=bool(args.restart_audit_complete),
    )
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)
    print(output)
    print(manifest["canonical_manifest_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
