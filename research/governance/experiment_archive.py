"""Small, non-destructive experiment archive inventory and closure validator.

This describes retention, not economic acceptance or deletion authority. Existing
frozen manifests remain authoritative. No payload is moved, hashed or deleted.
"""

from __future__ import annotations

import argparse
import json
import os
import stat
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

SCHEMA = "narrowgate_experiment_archive_v1"
MANIFEST = "archive-manifest.json"
ROLES = ("contract", "provenance", "results", "acceptance", "conclusion")
GATES = (
    "payload_integrity", "dependency_resolution", "writers_exited",
    "no_active_or_recovery_consumers", "no_open_files", "scheduler_residue_review",
    "restore_read_check",
)


def inventory(root: Path) -> dict[str, Any]:
    """Count real files by top-level scope without opening large payloads.

    Symlinks/special files are reported, never followed. Counts are logical bytes,
    not physical allocation. The generated manifest is not counted recursively.
    """
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("archive root must be an existing real directory")
    groups: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    unexpected: list[str] = []
    stack = [root]
    while stack:
        current = stack.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                path = Path(entry.path)
                relative = path.relative_to(root)
                if relative.as_posix() == MANIFEST:
                    continue
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    stack.append(path)
                elif stat.S_ISREG(info.st_mode):
                    group = groups[relative.parts[0]]
                    group[0] += 1
                    group[1] += info.st_size
                else:
                    unexpected.append(relative.as_posix())
    scopes = [
        {"path": name, "files": values[0], "bytes": values[1]}
        for name, values in sorted(groups.items())
    ]
    return {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "method": "relative_names_and_sizes_only_not_content_verification",
        "files": sum(row["files"] for row in scopes),
        "logical_bytes": sum(row["bytes"] for row in scopes),
        "scopes": scopes,
        "non_regular_paths": sorted(unexpected),
    }


def draft(root: Path, experiment_id: str, owner: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA,
        "visibility": "local_only_do_not_publish",
        "experiment_id": experiment_id,
        "owner": owner,
        "economic_status": "unknown",
        "archive_status": "inventoried",
        "layout": "legacy_in_place",
        "identity_note": "Original identities and payload paths unchanged; inventory is not acceptance.",
        "roles": {role: {"status": "unreviewed", "refs": [], "note": "Needs owner contract review."} for role in ROLES},
        "dependencies": [],
        "inventory": inventory(root),
        "gates": {gate: {"status": "unknown", "refs": []} for gate in GATES},
        "exceptions": ["Legacy retention/dependency review is not complete; retain unclassified payloads."],
        "cleanup": {"status": "not_reviewed", "receipt_refs": []},
    }


def _relative_path(value: Any) -> bool:
    if not isinstance(value, str) or not value or "\\" in value:
        return False
    p = PurePosixPath(value)
    return not p.is_absolute() and ".." not in p.parts and str(p) not in (".", "")


def validate(payload: dict[str, Any], root: Path | None = None) -> list[str]:
    """Validate declared evidence structure; never infer PASS from file presence."""
    errors: list[str] = []
    if not isinstance(payload, dict):
        return ["manifest must be an object"]
    for key in ("roles", "gates", "inventory", "cleanup"):
        if not isinstance(payload.get(key), dict):
            errors.append(f"{key} must be an object")
    for key in ("dependencies", "exceptions"):
        if not isinstance(payload.get(key), list):
            errors.append(f"{key} must be a list")
    if errors:
        return errors
    if any(not isinstance(row, dict) for row in payload["dependencies"]):
        return ["dependencies must contain objects"]
    if any(not isinstance(row, dict) for row in payload["roles"].values()):
        return ["roles must contain objects"]
    if any(not isinstance(row, dict) for row in payload["gates"].values()):
        return ["gates must contain objects"]
    if not isinstance(payload["inventory"].get("scopes"), list) or any(
        not isinstance(row, dict) for row in payload["inventory"].get("scopes", [])
    ):
        return ["inventory.scopes must be a list of objects"]
    if payload.get("schema_version") != SCHEMA:
        errors.append("unsupported schema_version")
    if payload.get("visibility") != "local_only_do_not_publish":
        errors.append("private visibility is required")
    for key in ("experiment_id", "owner", "identity_note"):
        if not isinstance(payload.get(key), str) or not payload[key].strip():
            errors.append(f"{key} is required")
    if payload.get("layout") not in ("legacy_in_place", "standard_v1"):
        errors.append("invalid layout")
    if payload.get("economic_status") not in ("unknown", "active", "closed_complete", "closed_partial", "closed_failed"):
        errors.append("invalid economic_status")
    status = payload.get("archive_status")
    if status not in ("inventoried", "reviewed_with_exceptions", "verified_minimal"):
        errors.append("invalid archive_status")
    roles = payload.get("roles", {})
    for role in ROLES:
        row = roles.get(role, {})
        if row.get("status") not in ("unreviewed", "verified", "not_applicable"):
            errors.append(f"invalid role status: {role}")
        if not isinstance(row.get("refs"), list) or not all(isinstance(r, str) and r for r in row.get("refs", [])):
            errors.append(f"invalid role refs: {role}")
        if row.get("status") == "verified" and not row.get("refs"):
            errors.append(f"verified role needs evidence: {role}")
        if row.get("status") == "not_applicable" and not row.get("note"):
            errors.append(f"not_applicable role needs reason: {role}")
    for dep in payload.get("dependencies", []):
        if not all(dep.get(k) for k in ("role", "locator", "identity", "reason")):
            errors.append("dependency needs role, locator, identity and reason")
        if dep.get("status") not in ("verified", "unknown"):
            errors.append("invalid dependency status")
    inv = payload.get("inventory", {})
    paths: set[str] = set()
    scopes = inv.get("scopes", [])
    for row in scopes:
        name = row.get("path")
        if not _relative_path(name) or len(PurePosixPath(name).parts) != 1 or name == MANIFEST:
            errors.append(f"invalid top-level scope: {name}")
            continue
        if name in paths:
            errors.append(f"duplicate scope: {name}")
        paths.add(name)
        for key in ("files", "bytes"):
            if type(row.get(key)) is not int or row[key] < 0:
                errors.append(f"invalid {key}: {name}")
        if root is not None:
            p = root / name
            if p.is_symlink() or not p.exists():
                errors.append(f"missing or linked scope: {name}")
    if all(type(r.get("files")) is int and type(r.get("bytes")) is int for r in scopes):
        if inv.get("files") != sum(r["files"] for r in scopes) or inv.get("logical_bytes") != sum(r["bytes"] for r in scopes):
            errors.append("inventory totals differ from scopes")
    gates = payload.get("gates", {})
    for gate in GATES:
        row = gates.get(gate, {})
        if row.get("status") not in ("unknown", "pass", "blocked"):
            errors.append(f"invalid gate status: {gate}")
        if row.get("status") == "pass" and not row.get("refs"):
            errors.append(f"passed gate needs receipt: {gate}")
    if status == "verified_minimal":
        if payload.get("economic_status") not in ("closed_complete", "closed_partial", "closed_failed"):
            errors.append("minimal archive must be closed, not active or unknown")
        if payload.get("exceptions") or inv.get("non_regular_paths"):
            errors.append("minimal archive has unresolved exceptions")
        if any(roles.get(r, {}).get("status") == "unreviewed" for r in ROLES):
            errors.append("minimal archive has unreviewed roles")
        if any(gates.get(g, {}).get("status") != "pass" for g in GATES):
            errors.append("minimal archive requires every closure gate")
        if any(d.get("status") != "verified" for d in payload.get("dependencies", [])):
            errors.append("minimal archive has unresolved dependencies")
        if payload.get("cleanup", {}).get("status") != "complete" or not payload.get("cleanup", {}).get("receipt_refs"):
            errors.append("minimal archive requires cleanup receipt (including zero-delete review)")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("inventory", help="Read-only size inventory; no content hashing")
    p.add_argument("root", type=Path)
    p = commands.add_parser("init", help="Create a private inventoried draft; never overwrite")
    p.add_argument("root", type=Path)
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--owner", required=True)
    p.add_argument("--write", action="store_true", help="Explicitly create archive-manifest.json")
    p = commands.add_parser("validate", help="Check declarations; does not certify economics or permit deletion")
    p.add_argument("manifest", type=Path)
    p.add_argument("--check-paths", action="store_true")
    args = parser.parse_args()
    if args.command == "validate":
        payload = json.loads(args.manifest.read_text())
        errors = validate(payload, args.manifest.parent if args.check_paths else None)
        print(json.dumps({"valid": not errors, "errors": errors, "archive_status": payload.get("archive_status") if isinstance(payload, dict) else None}, indent=2))
        return int(bool(errors))
    if args.command == "inventory":
        payload = inventory(args.root)
    else:
        target = args.root / MANIFEST
        if args.write and target.exists():
            parser.error("manifest exists; review it rather than overwriting historical declarations")
        payload = draft(args.root, args.experiment_id, args.owner)
        if args.write:
            # O_EXCL prevents competing initializers from replacing an existing record.
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
            print(str(target))
            return 0
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
