#!/usr/bin/env python3
"""Preflight, inspect, and admit F04 exact-opportunity v2.2 chunks."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import resolve_portable_path  # noqa: E402
from execution.exact_opportunity_tape_runtime import (  # noqa: E402
    build_exact_opportunity_runtime_identity,
    validate_exact_opportunity_runtime_config,
)
from live.config import load_config  # noqa: E402
from research.families.f04_external_market_alpha.audit.exact_opener_opportunity_tape_v2_2 import (  # noqa: E402
    admit_ready_chunk,
    scan_staging,
    validate_ready_chunk,
)


def _preflight(args: argparse.Namespace) -> dict[str, Any]:
    config_path = Path(args.config).expanduser().resolve()
    repo_root = Path(args.repo_root).expanduser().resolve()
    cfg = load_config(config_path)
    validation = validate_exact_opportunity_runtime_config(
        cfg,
        require_enabled=True,
    )
    staging = Path(cfg.logging.exact_opportunity_tape_staging_dir).expanduser()
    if not staging.is_absolute():
        staging = repo_root / staging
    staging = staging.resolve()
    storage_root = resolve_portable_path(
        "${NARROWGATE_STORAGE_ROOT}", root=repo_root
    ).resolve()
    if staging == storage_root or storage_root in staging.parents:
        raise ValueError("runtime staging must remain on local storage")
    destination = Path(args.destination_root).expanduser().resolve()
    if destination == storage_root or storage_root not in destination.parents:
        raise ValueError("formal destination must be under the configured storage root")
    if not storage_root.is_mount():
        raise ValueError("configured storage root is not mounted")
    identity = build_exact_opportunity_runtime_identity(cfg, repo_root=repo_root)
    return {
        "schema_version": "exact_opportunity_collection_preflight.v2.2",
        "valid": True,
        "prospective_collection_enabled_in_config": True,
        "shadow_only": True,
        "economic_outcomes_read": False,
        "config": validation,
        "runtime_identity_sha256": identity["runtime_identity_sha256"],
        "staging_root": str(staging),
        "destination_root": str(destination),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("--config", required=True)
    preflight.add_argument("--repo-root", default=".")
    preflight.add_argument("--destination-root", required=True)

    scan = subparsers.add_parser("scan")
    scan.add_argument("--staging-root", required=True)

    validate = subparsers.add_parser("validate")
    validate.add_argument("--manifest", required=True)

    admit = subparsers.add_parser("admit")
    admit.add_argument("--manifest", required=True)
    admit.add_argument("--destination-root", required=True)

    args = parser.parse_args()
    if args.command == "preflight":
        result = _preflight(args)
    elif args.command == "scan":
        result = scan_staging(args.staging_root)
    elif args.command == "validate":
        result = validate_ready_chunk(args.manifest)
    else:
        result = admit_ready_chunk(args.manifest, args.destination_root)
    print(json.dumps(result, sort_keys=True, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
