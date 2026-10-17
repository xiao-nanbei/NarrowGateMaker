"""Bounded metadata-only observations of registered local daily files."""

import os
from pathlib import Path


def observe_inventory(inventory: dict, day: str, symbol: str, version: str) -> dict:
    """Only stat registered local files; no content reads, SHA, SSH or recursive scan."""
    if inventory["node"] != "local":
        return {"reason": "remote_replica_not_observed"}
    expected = None
    if "files_by_day" in inventory:
        entries = inventory["files_by_day"].get(day)
        if not entries:
            return {"reason": "inventory_snapshot_only"}
        if not isinstance(entries, list) or len(entries) > 32:
            raise ValueError("Register at most 32 exact files per dataset UTC day")
        paths = [Path(item["path"] if isinstance(item, dict) else item) for item in entries]
        if any(not p.is_absolute() for p in paths):
            raise ValueError("Registered inventory files must be absolute owner-local paths")
        if inventory.get("audit_version") == version and all(
            isinstance(item, dict)
            and type(item.get("size_bytes")) is int
            and item["size_bytes"] >= 0
            for item in entries
        ):
            expected = [item["size_bytes"] for item in entries]
        root = Path(inventory.get("directory", os.path.commonpath([str(p.parent) for p in paths])))
        if not root.is_dir():
            return {
                "status": "unknown",
                "reason": "local_inventory_root_unavailable",
                "snapshot": None,
            }
    elif "directory" in inventory:
        root = Path(inventory["directory"])
        if not root.is_dir():
            return {
                "status": "unknown",
                "reason": "local_inventory_root_unavailable",
                "snapshot": None,
            }
        pattern = inventory["pattern"].format(day=day, symbol=symbol)
        if (
            Path(pattern).is_absolute()
            or ".." in Path(pattern).parts
            or "**" in pattern
            or any(c in str(Path(pattern).parent) for c in "*?[")
        ):
            raise ValueError(
                "Inventory refresh only supports bounded non-recursive registered patterns"
            )
        paths = []
        for path in root.glob(pattern):
            if path.is_file():
                paths.append(path)
            if len(paths) > 32:
                raise ValueError("Inventory pattern exceeds the 32-file daily observation limit")
        paths.sort()
    else:
        return {"reason": "inventory_snapshot_only"}
    try:
        snapshot = []
        missing = 0
        for path in paths:
            if not path.is_file():
                missing += 1
                continue
            stat = path.stat()
            snapshot.append([str(path), stat.st_size, stat.st_mtime_ns, stat.st_ino, stat.st_dev])
    except OSError:
        return {"status": "unknown", "reason": "local_inventory_root_unavailable", "snapshot": None}
    return {
        "status": "present_unverified" if snapshot and not missing else "missing",
        "reason": "local_presence_only" if snapshot and not missing else "local_file_missing",
        "snapshot": snapshot,
        "file_counts": {
            "expected": len(paths) if "files_by_day" in inventory else None,
            "present": len(snapshot),
            "missing": missing if paths else None,
        },
        "size_bytes": sum(item[1] for item in snapshot),
        "audit_size_matched": bool(snapshot) and expected == [item[1] for item in snapshot],
        "binding_reason": (
            "recorded_audit_version_mismatch"
            if inventory.get("audit_version", version) != version
            else "recorded_output_size_mismatch"
            if expected is not None and expected != [item[1] for item in snapshot]
            else "historical_audit_not_bound_to_current_files"
        ),
    }
