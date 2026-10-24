"""One explicit local maintenance pass; scheduling stays with the caller."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import sys
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from . import file_index, locks


def write_json(path, value):
    """Publish one current receipt, never truncate the last valid receipt."""
    fd, name = tempfile.mkstemp(prefix=".maintenance-", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def configuration(path):
    raw = locks.read(Path(path))
    roots = []
    labels = {}
    for entry in raw["roots"]:
        root = Path(entry["path"])
        if not root.is_absolute() or root.parent == root:
            raise ValueError("maintenance roots must be explicit workspace directories")
        root = locks.physical(root)
        if not root.is_dir():
            raise ValueError(f"configured volume/workspace is unavailable: {root}")
        label = entry["label"]
        if not isinstance(label, str) or not re.fullmatch(r"[\w-]+", label):
            raise ValueError("root label must contain only letters, digits, underscores or hyphens")
        if label in labels.values():
            raise ValueError("root labels must be unique")
        if any(root.is_relative_to(r) or r.is_relative_to(root) for r in roots):
            raise ValueError("maintenance roots must not overlap")
        roots.append(root)
        labels[str(root)] = label
    if not roots:
        raise ValueError("at least one explicit workspace is required")
    output = Path(raw["index_directory"])
    pool = Path(raw["pool_state"])
    if not output.is_absolute() or not pool.is_absolute():
        raise ValueError("index and PoolRun paths must be absolute")
    output, pool = locks.physical(output), locks.physical(pool)
    if any(root == output or root.is_relative_to(output) for root in roots):
        raise ValueError("index output cannot contain a scan root")
    excluded = [locks.physical(Path(p)) for p in raw.get("excluded_directories", [])]
    if any(not Path(p).is_absolute() for p in raw.get("excluded_directories", [])):
        raise ValueError("excluded directories must be absolute")
    return roots, labels, pool, output, [output, *excluded]


@contextmanager
def maintenance_lock(output):
    output.mkdir(parents=True, exist_ok=True)
    with (output / ".maintenance.lock").open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def run(config, *, apply=False, index_only=False):
    roots, labels, pool, output, excluded = configuration(config)
    with maintenance_lock(output):
        latest = output / "maintenance-latest.json"
        previous = locks.read(latest) if latest.exists() else {}
        pending = bool(previous.get("index_refresh_pending"))
        started = datetime.now(UTC).isoformat()
        report = (
            {"mode": "index", "removed_files": 0, "records": [], "errors": []}
            if index_only
            else locks.clean(roots, pool, apply=apply, excluded=excluded)
        )
        report.update(
            visibility="local_only_do_not_publish",
            started_at=started,
            index_refresh_pending=pending or bool(report["removed_files"]),
            index_refreshed=False,
        )
        # Persist deletions before generating the optional display index. A failed
        # index refresh is retried next time without repeating successful removal.
        write_json(latest, report)
        if report["removed_files"]:
            write_json(output / "maintenance-last-change.json", report)
        if index_only or (apply and report["index_refresh_pending"]):
            try:
                summary = file_index.refresh(
                    output,
                    roots,
                    labels=labels,
                    lock_records={r["path"]: r for r in report["records"]},
                )
                report["index_refreshed"] = True
                report["indexed_files"] = summary["files"]
                report["index_refresh_pending"] = False
                if summary["errors"]:
                    report["errors"].append(
                        {
                            "stage": "index",
                            "error": "incomplete index scan",
                            "details": summary["errors"],
                        }
                    )
            except Exception as exc:
                report["errors"].append({"stage": "index", "error": str(exc)})
                report["index_refresh_pending"] = True
        report["finished_at"] = datetime.now(UTC).isoformat()
        report["status"] = (
            "ERROR" if report["errors"] else "CHANGED" if report["removed_files"] else "OK"
        )
        write_json(latest, report)
        if report["removed_files"]:
            write_json(output / "maintenance-last-change.json", report)
        return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("scan", "run", "index"))
    parser.add_argument(
        "--config",
        default=os.environ.get("NARROWGATE_MAINTENANCE_CONFIG"),
        help="explicit private roots, PoolRun state and existing index directory",
    )
    parser.add_argument(
        "--apply", action="store_true", help="delete verified terminal runtime locks (run only)"
    )
    args = parser.parse_args(argv)
    if not args.config:
        parser.error("provide --config or NARROWGATE_MAINTENANCE_CONFIG")
    if args.apply and args.action != "run":
        parser.error("--apply is valid only with run")
    try:
        report = run(args.config, apply=args.apply, index_only=args.action == "index")
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        print(
            json.dumps({"status": "ERROR", "error": str(exc)}, ensure_ascii=False), file=sys.stderr
        )
        return 1
    print(
        json.dumps(
            {key: value for key, value in report.items() if key not in {"records", "visibility"}},
            ensure_ascii=False,
        )
    )
    return 1 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
