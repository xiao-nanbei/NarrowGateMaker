"""Read-only daily projections of existing audits and explicit node inventories.

This adapter does not rate data, run downloads, or grant replay admission. An
operator imports selected source/version audit CSVs and inventory patterns with
the CLI; HTTP consumers receive no filesystem locators. Missing audit rows are
unknown, never automatically failed. Sparse trades are not evidence of a gap.
"""

import argparse
import csv
import json
import math
import os
import re
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

NAME = "data-quality.json"
META = ("id", "source", "exchange", "market", "symbol", "data_type", "version", "label")
TASKS = ("candles", "modeled_replay", "strict_replay", "funding_pnl")
LIMITATIONS = [
    "This inventory is not a replay dependency list; reference feeds need not be execution books.",
    "Audit status applies to the named source/version and recorded check scope, not every replay.",
    "File presence is not content verification; node reachability is not data quality.",
    "No interval is inferred from a second without trades, overall coverage, or forward filling.",
    "Unknown warmup, sequence, timing or fee/funding support is not silently admitted.",
    "This read-only catalog never changes frozen inputs or the dates of existing results.",
]


def _days(start: str, end: str) -> list[str]:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    count = (last - first).days + 1
    if not 1 <= count <= 366:
        raise ValueError("Select an inclusive UTC range of 1 to 366 days")
    return [(first + timedelta(days=i)).isoformat() for i in range(count)]


def _text(value) -> str:
    # CSV reasons can contain obsolete private locators. Do not project them.
    text = str(value or "")[:1000]
    return re.sub(r"(?:/[\w.~-]+){2,}[^\s|;]*|[A-Za-z]:\\[^\s|;]*", "[private locator]", text)


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
            sources.append(
                {
                    **{k: v for k, v in metadata.items() if k != "id"},
                    "dataset_id": metadata["id"],
                    "availability": record.get("availability", "unknown"),
                    "check_status": record.get("check_status", "unchecked"),
                    "check_scope": record.get("check_scope", "No source audit for this UTC day"),
                    "task_usability": record.get("task_usability", dict.fromkeys(TASKS, "unknown")),
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
                    "replica": {**replica, "node_status": nodes[node]["status"]},
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


def import_quality(root: Path, manifest_path: Path) -> dict:
    """Import operator-selected CSV fields and metadata-only inventory, not raw tapes.

    Manifest audit mappings name existing columns; no new quality thresholds are
    applied. Optional explicit intervals must include their own source/version
    identity. Local inventory checks only presence/size and is not a SHA check.
    """
    manifest = json.loads(manifest_path.read_text())
    calendar = _days(manifest["start_day"], manifest["end_day"])
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
    payload = {"datasets": [], "nodes": nodes, "updated_at": now, "records": {}}
    ids = set()
    node_ids = {row["id"] for row in payload["nodes"]}
    if len(node_ids) != len(payload["nodes"]):
        raise ValueError("Duplicate nodes")
    for spec in manifest["datasets"]:
        metadata = {key: _text(spec[key]) for key in META}
        if not re.fullmatch(r"[a-zA-Z0-9_.-]+", metadata["id"]) or metadata["id"] in ids:
            raise ValueError("Invalid or duplicate dataset ID")
        ids.add(metadata["id"])
        payload["datasets"].append(metadata)
        by_day = payload["records"][metadata["id"]] = {}
        audit = spec.get("audit", {})
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
                "task_usability": dict.fromkeys(TASKS, "unknown"),
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
                if "directory" in inventory:
                    directory = Path(inventory["directory"])
                    pattern = inventory["pattern"].format(day=day, symbol=spec["symbol"])
                    if Path(pattern).is_absolute() or ".." in Path(pattern).parts:
                        raise ValueError("Inventory pattern must stay under selected directory")
                    paths = sorted(directory.glob(pattern)) if directory.is_dir() else None
                    paths = [p for p in paths if p.is_file()] if paths is not None else None
                    # Missing/unmounted root is unknown, not an absent month.
                    status = (
                        "unknown" if paths is None else "present_unverified" if paths else "missing"
                    )
                    replica = {"status": status, "last_checked_at": now}
                    if inventory.get("canonical") and paths is not None:
                        record["availability"] = "present" if paths else "missing"
                        record["size_bytes"] = sum(p.stat().st_size for p in paths)
                else:
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
            if any(r["status"] == "verified" for r in record["replicas"].values()):
                # A verified same-version remote copy can repair an absent
                # canonical local file. Do not recommend a new provider fetch.
                record["availability"] = "present"
            by_day[day] = record
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, name = tempfile.mkstemp(prefix=".quality-", dir=root)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(payload, stream, allow_nan=False)
        os.replace(name, root / NAME)
    finally:
        Path(name).unlink(missing_ok=True)
    return quality_catalog(root)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(import_quality(args.state_dir, args.manifest)))


if __name__ == "__main__":
    main()
