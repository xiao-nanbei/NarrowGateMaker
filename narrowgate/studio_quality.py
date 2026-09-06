"""Read-only daily projections of existing audits and explicit node inventories.

This adapter does not rate data, run downloads, or grant replay admission. An
operator imports selected source/version audit CSVs and inventory patterns with
the CLI; HTTP consumers receive no filesystem locators. Missing audit rows are
unknown, never automatically failed. Sparse trades are not evidence of a gap.
"""

import argparse
import csv
import fcntl
import json
import math
import os
import re
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

NAME = "data-quality.json"
SOURCE_NAME = "data-quality-source.local.json"
META = ("id", "source", "exchange", "market", "symbol", "data_type", "version", "label")
TASKS = ("candles", "feature_input", "modeled_replay", "strict_replay", "funding_pnl")
LIMITATIONS = [
    "This inventory is not a replay dependency list; reference feeds need not be execution books.",
    "Audit status applies to the named source/version and recorded check scope, not every replay.",
    "File presence is not content verification; node reachability is not data quality.",
    "No interval is inferred from a second without trades, overall coverage, or forward filling.",
    "Unknown warmup, sequence, timing or fee/funding support is not silently admitted.",
    "This read-only catalog never changes frozen inputs or the dates of existing results.",
]


def _days(start: str, end: str, *, limit: int = 366) -> list[str]:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    count = (last - first).days + 1
    if not 1 <= count <= limit:
        raise ValueError(f"Select an inclusive UTC range of 1 to {limit} days")
    return [(first + timedelta(days=i)).isoformat() for i in range(count)]


def _text(value) -> str:
    # CSV reasons can contain obsolete private locators. Do not project them.
    text = str(value or "")[:1000]
    return re.sub(
        r"(?<!\w)(?:/[\w.~-]+){2,}[^\s|;]*|[A-Za-z]:\\[^\s|;]*", "[private locator]", text
    )


def _number(value):
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _status(value) -> str:
    value = str(value).strip().lower()
    if value in {"true", "1", "passed", "pass"}:
        return "passed"
    if value in {"false", "0", "failed", "fail"}:
        return "failed"
    return "unknown"


def _timestamp(value):
    if value is None:
        return None
    datetime.fromisoformat(value)
    return value


def _load(root: Path) -> dict:
    path = root / NAME
    if not path.exists():
        return {
            "datasets": [],
            "nodes": [{"id": "local", "status": "unknown", "last_seen": None}],
            "updated_at": None,
            "records": {},
        }
    return json.loads(path.read_text())


def _atomic_json(root: Path, name: str, payload: dict):
    descriptor, temporary = tempfile.mkstemp(prefix=".quality-", dir=root)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(payload, stream, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, root / name)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _observe_inventory(inventory: dict, day: str, symbol: str, version: str) -> dict:
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
        for path in paths:
            if not path.is_file():
                return {"status": "missing", "reason": "local_file_missing", "snapshot": []}
            stat = path.stat()
            snapshot.append([str(path), stat.st_size, stat.st_mtime_ns, stat.st_ino, stat.st_dev])
    except OSError:
        return {"status": "unknown", "reason": "local_inventory_root_unavailable", "snapshot": None}
    return {
        "status": "present_unverified" if snapshot else "missing",
        "reason": "local_presence_only" if snapshot else "local_file_missing",
        "snapshot": snapshot,
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


def _apply_observation(record, node, observation, now, baseline=None):
    replica = record["replicas"].setdefault(node, {"status": "unknown", "last_checked_at": None})
    replica["observation_reason"] = observation["reason"]
    if "status" not in observation:
        return
    replica.update(status=observation["status"], last_checked_at=now)
    replica["observed_at"] = now
    changed = baseline is not None and observation.get("snapshot") != baseline.get("snapshot")
    matched = observation.get("audit_size_matched", False) and not changed
    record.setdefault("current_file_checks", {})[node] = {
        "status": "changed_since_observation"
        if changed
        else ("recorded_content_audit_current_size_matched" if matched else "historical_unbound"),
        "reason": "local_files_changed_since_observation"
        if changed
        else (
            "recorded_content_audit_current_size_matched"
            if matched
            else observation.get("binding_reason", "historical_audit_not_bound_to_current_files")
        ),
    }


def _current_usability(record):
    # Source applicability and the selected machine's copy are separate facts.
    # An unobserved LAN/cloud replica cannot erase a bound canonical local audit.
    check = record.get("current_file_checks", {}).get("local")
    if not record.get("checked_at") and record.get("check_status", "unchecked") == "unchecked":
        check = {"status": "no_audit", "reason": "no_recorded_audit"}
    elif check is None:
        check = {"status": "inventory_snapshot_only", "reason": "inventory_snapshot_only"}
    applicable = check["status"] == "recorded_content_audit_current_size_matched"
    recorded = record.get("task_usability", dict.fromkeys(TASKS, "unknown"))
    current, reasons = {}, {}
    for task in TASKS:
        old = recorded.get(task, "unknown")
        current[task] = old if applicable or old == "not_applicable" else "unknown"
        reasons[task] = (
            "source_not_applicable"
            if old == "not_applicable"
            else "task_not_mapped_in_recorded_audit"
            if applicable and old == "unknown"
            else check["reason"]
        )
        if applicable and task in record.get("audit_task_reasons", {}):
            reasons[task] = record["audit_task_reasons"][task]
    return check, current, reasons


def quality_catalog(root: Path) -> dict:
    payload = _load(root)
    return {key: payload[key] for key in ("datasets", "nodes", "updated_at")}


def quality_days(
    root: Path, start_day: str, end_day: str, dataset_id: str = "", node: str = "local"
) -> dict:
    calendar = _days(start_day, end_day)
    payload = _load(root)
    datasets = [row for row in payload["datasets"] if not dataset_id or row["id"] == dataset_id]
    if dataset_id and not datasets:
        raise KeyError("Unknown registered dataset")
    nodes = {row["id"]: row for row in payload["nodes"]}
    if node not in nodes:
        raise KeyError("Unknown registered node")
    today = datetime.now(UTC).date().isoformat()
    items = []
    for day in calendar:
        sources = []
        for metadata in datasets:
            record = payload["records"].get(metadata["id"], {}).get(day, {})
            replica = record.get("replicas", {}).get(
                node,
                {
                    "status": "unknown",
                    "last_checked_at": None,
                },
            )
            applicability, current, task_reasons = _current_usability(record)
            sources.append(
                {
                    **{k: v for k, v in metadata.items() if k != "id"},
                    "dataset_id": metadata["id"],
                    "availability": record.get("availability", "unknown"),
                    "check_status": record.get("check_status", "unchecked"),
                    "check_scope": record.get("check_scope", "No source audit for this UTC day"),
                    "task_usability": record.get("task_usability", dict.fromkeys(TASKS, "unknown")),
                    "current_task_usability": current,
                    "task_reasons": task_reasons,
                    "audit_applicability": applicability,
                    "observed_at": record.get("replicas", {}).get("local", {}).get("observed_at"),
                    **{
                        key: record.get(key)
                        for key in (
                            "records",
                            "size_bytes",
                            "checked_at",
                            "coverage_ratio",
                            "max_gap_ms",
                        )
                    },
                    "reasons": record.get("reasons", []),
                    "intervals": record.get("intervals", []),
                    "replica": {
                        **replica,
                        "node_status": nodes[node]["status"],
                        "observation_reason": replica.get(
                            "observation_reason",
                            "remote_replica_not_observed"
                            if node != "local"
                            else "inventory_snapshot_only",
                        ),
                    },
                    "evidence_label": record.get("evidence_label", "No recorded audit"),
                }
            )
        items.append(
            {
                "day": day,
                "ongoing": day >= today,
                "sources": sources,
                "problem": not sources
                or any(
                    row["availability"] != "present"
                    or row["check_status"] != "passed"
                    or row["replica"]["status"] != "verified"
                    or any(r["status"] in {"gap", "invalid"} for r in row["intervals"])
                    for row in sources
                ),
            }
        )
    return {
        "start_day": start_day,
        "end_day": end_day,
        "node": node,
        "items": items,
        "limitations": LIMITATIONS,
    }


def quality_export(
    root: Path, start_day: str, end_day: str, dataset_id: str = "", node: str = "local"
) -> dict:
    report = quality_days(root, start_day, end_day, dataset_id, node)
    rows = []
    for day in report["items"]:
        for source in day["sources"]:
            state = source["replica"]["status"]
            gaps = [r for r in source["intervals"] if r["status"] in {"gap", "invalid"}]
            if source["availability"] == "missing":
                action = "Use existing provider download/resume tools, then normalize and audit"
            elif state in {"missing", "stale"}:
                action = "Synchronize this registered version from a verified canonical copy"
            elif gaps or source["check_status"] in {"failed", "partial"}:
                action = "Inspect existing source audit; repair or rebuild, then recheck"
            elif source["check_status"] != "passed":
                action = "Run existing source-specific audit; do not infer admission from presence"
            elif source["audit_applicability"]["status"] in {
                "changed_since_observation",
                "historical_unbound",
            }:
                action = (
                    "Recheck current files against the recorded source audit "
                    "before reusing its task decision"
                )
            elif state != "verified":
                action = (
                    "Verify node copy and its version; offline or unknown does not mean missing"
                )
            else:
                continue
            for gap in gaps or [{}]:
                rows.append(
                    {
                        "day": day["day"],
                        "node": node,
                        **{
                            k: source[k]
                            for k in (
                                "dataset_id",
                                "source",
                                "exchange",
                                "market",
                                "symbol",
                                "data_type",
                                "version",
                            )
                        },
                        "start_ms": gap.get("start_ms"),
                        "end_ms": gap.get("end_ms"),
                        "reason": gap.get("reason")
                        or "; ".join(source["reasons"])
                        or f"check={source['check_status']}; replica={state}",
                        "recommended_action": action,
                    }
                )
    return {"items": rows, "execution": "export_only_no_download_started"}


def _audit_rows(spec: dict) -> dict:
    audit = spec.get("audit")
    if not audit:
        return {}
    with Path(audit["path"]).open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    result = {}
    for row in rows:
        if audit.get("symbol_column") and row[audit["symbol_column"]] != spec["symbol"]:
            continue
        day = date.fromisoformat(row[audit.get("day_column", "day")]).isoformat()
        if day in result:
            raise ValueError("Duplicate date in a selected source/version audit")
        result[day] = row
    return result


def _import_quality(root: Path, manifest_path: Path) -> dict:
    """Import operator-selected CSV fields and metadata-only inventory, not raw tapes.

    Manifest audit mappings name existing columns; no new quality thresholds are
    applied. Optional explicit intervals must include their own source/version
    identity. Local inventory checks only presence/size and is not a SHA check.
    """
    manifest = json.loads(manifest_path.read_text())
    calendar = _days(manifest["start_day"], manifest["end_day"], limit=3660)
    now = datetime.now(UTC).isoformat()
    nodes = []
    for node in manifest["nodes"]:
        if not re.fullmatch(r"[a-zA-Z0-9_.-]+", node["id"]):
            raise ValueError("Invalid node ID")
        if node["status"] not in {"online", "offline", "unknown"}:
            raise ValueError("Invalid node status")
        nodes.append(
            {
                "id": node["id"],
                "status": node["status"],
                "last_seen": _timestamp(node.get("last_seen")),
            }
        )
    payload = {
        "datasets": [],
        "nodes": nodes,
        "updated_at": now,
        "records": {},
        "registered_at": now,
    }
    registration = {
        "registered_at": now,
        "start_day": manifest["start_day"],
        "end_day": manifest["end_day"],
        "datasets": [],
        "snapshots": {},
    }
    ids = set()
    node_ids = {row["id"] for row in payload["nodes"]}
    if len(node_ids) != len(payload["nodes"]):
        raise ValueError("Duplicate nodes")
    for spec in manifest["datasets"]:
        metadata = {key: _text(spec[key]) for key in META}
        metadata["stage"] = spec.get("stage", "registered")
        if metadata["stage"] not in {"raw", "processed", "registered"}:
            raise ValueError("Dataset stage must be raw, processed or registered")
        if not re.fullmatch(r"[a-zA-Z0-9_.-]+", metadata["id"]) or metadata["id"] in ids:
            raise ValueError("Invalid or duplicate dataset ID")
        ids.add(metadata["id"])
        payload["datasets"].append(metadata)
        registration["datasets"].append(
            {key: spec[key] for key in ("id", "symbol", "version", "inventories") if key in spec}
        )
        snapshots = registration["snapshots"][metadata["id"]] = {}
        by_day = payload["records"][metadata["id"]] = {}
        audit = spec.get("audit", {})
        if set(audit.get("not_applicable_tasks", [])) - set(TASKS):
            raise ValueError("Unknown not-applicable task")
        if set(audit.get("not_applicable_tasks", [])) & set(audit.get("task_columns", {})):
            raise ValueError("A task cannot have both an audit column and N/A scope")
        rows = _audit_rows(spec)
        intervals = spec.get("intervals", [])
        for item in intervals:
            if item["dataset_id"] != spec["id"] or item["version"] != spec["version"]:
                raise ValueError("Interval belongs to a different source/version")
            if item["status"] not in {"gap", "invalid", "valid", "unknown"}:
                raise ValueError("Invalid interval status")
            day_start = datetime.fromisoformat(item["day"]).replace(tzinfo=UTC).timestamp() * 1000
            if not day_start <= item["start_ms"] < item["end_ms"] <= day_start + 86_400_000:
                raise ValueError("Interval must be inside its UTC day")
        for day in calendar:
            row = rows.get(day)
            record = {
                "availability": "unknown",
                "check_status": "unchecked",
                "task_usability": {
                    task: "not_applicable"
                    if task in audit.get("not_applicable_tasks", [])
                    else "unknown"
                    for task in TASKS
                },
                "audit_task_reasons": {
                    task: _text(reason)
                    for task, reason in audit.get("task_reasons", {}).items()
                    if task in TASKS
                },
                "reasons": [],
                "intervals": [],
                "replicas": {},
            }
            if row is not None:
                state = _status(row.get(audit.get("check_column", "")))
                record.update(
                    {
                        "check_status": state if state != "unknown" else "unchecked",
                        "checked_at": _timestamp(audit.get("checked_at")),
                        "check_scope": _text(audit["scope"]),
                        "evidence_label": _text(audit["label"]),
                        "records": _number(row.get(audit.get("records_column", ""))),
                        "size_bytes": _number(row.get(audit.get("size_column", ""))),
                        "coverage_ratio": _number(row.get(audit.get("coverage_column", ""))),
                    }
                )
                gap = _number(row.get(audit.get("max_gap_seconds_column", "")))
                record["max_gap_ms"] = gap * 1000 if gap is not None else None
                for task in TASKS:
                    column = audit.get("task_columns", {}).get(task)
                    record["task_usability"][task] = (
                        _status(row.get(column)) if column else "unknown"
                    )
                    if task in audit.get("not_applicable_tasks", []):
                        if column:
                            raise ValueError(
                                "A task cannot have both an audit column and N/A scope"
                            )
                        record["task_usability"][task] = "not_applicable"
                for field in audit.get("reason_columns", []):
                    record["reasons"].extend(
                        _text(r) for r in re.split(r"[|;]", row.get(field, "")) if r
                    )
                present = _status(row.get(audit.get("availability_column", "")))
                if present != "unknown":
                    record["availability"] = "present" if present == "passed" else "missing"
            for item in intervals:
                if item["day"] == day:
                    record["intervals"].append(
                        {
                            k: _text(item[k]) if k in {"kind", "reason"} else item[k]
                            for k in ("start_ms", "end_ms", "status", "kind", "reason")
                        }
                    )
            for inventory in spec.get("inventories", []):
                node = inventory["node"]
                if node not in node_ids:
                    raise ValueError("Inventory names an unregistered node")
                observation = _observe_inventory(inventory, day, spec["symbol"], spec["version"])
                replica = inventory.get("days", {}).get(
                    day,
                    {
                        "status": "unknown",
                        "last_checked_at": inventory.get("checked_at"),
                    },
                )
                if replica.get("status") not in {
                    "unknown",
                    "verified",
                    "present_unverified",
                    "missing",
                    "stale",
                }:
                    raise ValueError("Unknown replica state")
                replica = {k: replica.get(k) for k in ("status", "last_checked_at")}
                replica["last_checked_at"] = _timestamp(replica["last_checked_at"])
                record["replicas"][node] = replica
                _apply_observation(record, node, observation, now)
                snapshots.setdefault(day, {})[node] = observation
                if inventory.get("canonical") and "status" in observation:
                    record["availability"] = (
                        "present"
                        if observation["status"] == "present_unverified"
                        else observation["status"]
                    )
                    record["size_bytes"] = observation.get("size_bytes")
            if any(r["status"] == "verified" for r in record["replicas"].values()):
                # A verified same-version remote copy can repair an absent
                # canonical local file. Do not recommend a new provider fetch.
                record["availability"] = "present"
            by_day[day] = record
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    _atomic_json(root, NAME, payload)
    _atomic_json(root, SOURCE_NAME, registration)
    return quality_catalog(root)


def import_quality(root: Path, manifest_path: Path) -> dict:
    """Register existing audit mappings and a private metadata-only refresh source.

    Exact local inventories may use ``files_by_day: {day: [{path, size_bytes}]}``
    plus ``audit_version`` matching the dataset version. The size comes from an
    existing content audit, not a new threshold. Matching current stat associates
    that recorded audit only; it is not a fresh content hash or strict admission.
    """
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (root / ".quality.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        return _import_quality(root, manifest_path)


def refresh_quality(root: Path, request: dict) -> dict:
    """Refresh only operator-registered local metadata; never run an audit/command."""
    if set(request) != {"start_day", "end_day", "dataset_id", "node"}:
        raise ValueError("Quality refresh accepts only calendar, registered dataset and node IDs")
    if any(not isinstance(value, str) for value in request.values()):
        raise ValueError("Quality refresh selectors must be text")
    calendar = _days(request["start_day"], request["end_day"])
    quality_days(root, **request)  # Validate registered IDs before any stat or write.
    source_path = root / SOURCE_NAME
    if not source_path.is_file():
        result = quality_days(root, **request)
        result["refresh"] = {
            "status": "not_registered",
            "observed_at": None,
            "scope": "registered_local_metadata_only",
            "reason": "quality_refresh_source_not_registered",
        }
        return result
    now = datetime.now(UTC).isoformat()
    with (root / ".quality.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if source_path.stat().st_mode & 0o077:
            raise ValueError("Quality refresh source must remain operator-private")
        registration = json.loads(source_path.read_text())
        payload = _load(root)
        if registration["registered_at"] != payload.get("registered_at"):
            raise ValueError(
                "Quality source registration changed; reimport selected owner manifest"
            )
        for spec in registration["datasets"]:
            if request["dataset_id"] and spec["id"] != request["dataset_id"]:
                continue
            for day in calendar:
                record = payload["records"][spec["id"]].get(day)
                if record is None:
                    continue  # A view range cannot expand the operator's registered calendar.
                for inventory in spec.get("inventories", []):
                    if inventory["node"] != request["node"]:
                        continue
                    observation = _observe_inventory(
                        inventory, day, spec["symbol"], spec["version"]
                    )
                    baseline = registration["snapshots"][spec["id"]][day].get(request["node"])
                    _apply_observation(record, request["node"], observation, now, baseline)
                    if inventory.get("canonical") and "status" in observation:
                        record["availability"] = (
                            "present"
                            if observation["status"] == "present_unverified"
                            else observation["status"]
                        )
                        record["size_bytes"] = observation.get("size_bytes")
                if any(r["status"] == "verified" for r in record["replicas"].values()):
                    record["availability"] = "present"
        payload["updated_at"] = now
        _atomic_json(root, NAME, payload)
    result = quality_days(root, **request)
    result["refresh"] = {
        "status": "refreshed",
        "observed_at": now,
        "scope": "registered_local_metadata_only",
        "reason": "local_presence_only"
        if request["node"] == "local"
        else "remote_replica_not_observed",
    }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(import_quality(args.state_dir, args.manifest)))


if __name__ == "__main__":
    main()
