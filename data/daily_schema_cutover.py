"""Recoverable, one-day publication of verified daily-format replacements.

This is an engineering cutover, not a research run. Only explicitly supplied
current catalog files are rebound; research-use records and historical results
are neither searched nor modified. Staging and targets must be real files.
"""
from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile

import pyarrow.parquet as pq

from data.daily_raw import _reject_output_symlinks, sha256_file
from data.trade_union_cutover import _read, _sync_directory, _write


def identity(path: Path) -> dict:
    _reject_output_symlinks(path)
    stat = path.stat()
    result = {"path": str(path), "sha256": sha256_file(path), "size_bytes": stat.st_size,
              "mtime_ns": stat.st_mtime_ns}
    if path.suffix == ".parquet":
        parquet = pq.ParquetFile(path)
        result.update(rows=parquet.metadata.num_rows, columns=parquet.schema_arrow.names)
    after = path.stat()
    if (after.st_ino, after.st_size, after.st_mtime_ns) != (stat.st_ino, stat.st_size, stat.st_mtime_ns):
        raise ValueError("file changed while identifying publication input")
    return result


def planned_files(pairs: list[tuple[Path, Path]], *, allow_create: bool = False) -> list[dict]:
    targets = [str(target) for _, target in pairs]
    if len(targets) != len(set(targets)):
        raise ValueError("duplicate publication target")
    files = []
    for staged, target in pairs:
        if staged == target or staged.stat().st_dev != target.parent.stat().st_dev:
            raise ValueError("publication needs distinct staging on the target filesystem")
        with staged.open("rb") as handle:
            os.fsync(handle.fileno())
        _reject_output_symlinks(target)
        before = ({"path": str(target), "sha256": None, "absent": True}
                  if allow_create and not target.exists() else identity(target))
        files.append({"staged": str(staged), "before": before, "after": identity(staged)})
        files[-1]["after"]["path"] = str(target)
    return files


def prepare_transaction(journal: Path, pairs: list[tuple[Path, Path]], *, day: str,
                        metadata: dict | None = None, allow_create: bool = False) -> dict:
    """All old/new identities are durable before the first target is replaced."""
    if journal.exists():
        raise FileExistsError(journal)
    files = planned_files(pairs, allow_create=allow_create)
    state = {"schema": "daily_schema_cutover.v1", "day": day,
             "status": "PREPARED", "files": files, "economic_admission": False}
    if metadata:
        if state.keys() & metadata.keys():
            raise ValueError("transaction metadata cannot replace publication fields")
        state.update(copy.deepcopy(metadata))
    _write(journal, state, exclusive=True)
    return state


def publish_transaction(journal: Path) -> dict:
    """Resume an interrupted atomic-file sequence without deleting unknown bytes."""
    state = _read(journal)
    if state.get("schema") != "daily_schema_cutover.v1":
        raise ValueError("unexpected publication journal")
    if state.get("catalog_prepared") is False:
        raise ValueError("daily current catalogs must be prepared before publication")
    # Check the whole transaction before mutating another target.
    remaining = []
    for record in state["files"]:
        target, staged = Path(record["after"]["path"]), Path(record["staged"])
        _reject_output_symlinks(target)
        if not target.exists():
            if not record["before"].get("absent"):
                raise ValueError("existing publication target disappeared")
            if identity(staged)["sha256"] != record["after"]["sha256"]:
                raise ValueError("verified staging changed before creation")
            remaining.append((staged, target))
            continue
        actual = identity(target)
        if actual["sha256"] == record["after"]["sha256"]:
            if staged.exists() and identity(staged)["sha256"] != record["after"]["sha256"]:
                raise ValueError("staging changed after publication")
        elif actual["sha256"] == record["before"]["sha256"]:
            if identity(staged)["sha256"] != record["after"]["sha256"]:
                raise ValueError("verified staging changed before publication")
            remaining.append((staged, target))
        else:
            raise ValueError("target is neither the original nor verified replacement")
    for staged, target in remaining:
        os.replace(staged, target)
        _sync_directory(target.parent)
    for record in state["files"]:
        if identity(Path(record["after"]["path"]))["sha256"] != record["after"]["sha256"]:
            raise ValueError("published file failed readback identity")
    state["status"] = "FILES_PUBLISHED"
    _write(journal, state)
    return state


_PRESERVE = {"research_use", "previous_use", "known_previous_use", "rights_scope",
             "historical_audit", "sealed", "holdout", "validation",
             "model_input_verification"}


def rebind_current(value, records: list[dict]):
    """Rebind current file identities, leaving usage/history subtrees intact.

    Callers explicitly choose current manifests. This function never discovers
    files or changes an eligibility flag. Value/schema changes require their
    own producer checks; a digest replacement alone is not such a check.
    """
    hashes = {r["before"]["sha256"]: r["after"]["sha256"] for r in records
              if r["before"]["sha256"] is not None}
    paths = {r["after"]["path"]: r["after"] for r in records}

    def walk(obj, key=""):
        if key in _PRESERVE or key.startswith(("previous_", "prior_", "historical_")):
            return copy.deepcopy(obj)
        if isinstance(obj, list):
            return [walk(item) for item in obj]
        if isinstance(obj, dict):
            out = {k: walk(v, k) for k, v in obj.items()}
            target = obj.get("path") or obj.get("final_path")
            if isinstance(target, str) and target in paths:
                after = paths[target]
                for field in ("sha256", "size_bytes", "rows", "columns"):
                    if field in obj and field in after:
                        out[field] = after[field]
                if "mtime_ns" in obj:
                    out["mtime_ns"] = after.get("mtime_ns", Path(target).stat().st_mtime_ns)
            return out
        if isinstance(obj, str):
            return hashes.get(obj, obj)
        return obj

    return walk(value)


def rebind_json(path: Path, records: list[dict], *, mutate=None, output: Path | None = None) -> dict:
    before = identity(path)
    original = _read(path)
    updated = rebind_current(original, records)
    if mutate is not None:
        mutate(updated)
    if identity(path)["sha256"] != before["sha256"]:
        raise ValueError("current catalog changed during rebind")
    _write(output or path, updated)
    after = identity(output or path)
    after["path"] = str(path)
    return {"before": before, "after": after, "staged": str(output or path)}


def rebind_csv(path: Path, records: list[dict], *, day: str | None = None,
               updates: dict | None = None, output: Path | None = None) -> dict:
    before = identity(path)
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        fields, rows = reader.fieldnames, list(reader)
    if fields is None:
        raise ValueError("current CSV lacks fields")
    updates = updates or {}
    for name in updates:
        if name not in fields:
            fields.append(name)
    for row in rows:
        if day is None or row.get("day", row.get("calendar_date")) == day:
            row.update(rebind_current(row, records))
            row.update(updates)
    content = io.StringIO(newline="")
    writer = csv.DictWriter(content, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    if identity(path)["sha256"] != before["sha256"]:
        raise ValueError("current CSV changed during rebind")
    target = output or path
    _reject_output_symlinks(target)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    try:
        with os.fdopen(descriptor, "w") as handle:
            handle.write(content.getvalue())
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        _sync_directory(target.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)
    after = identity(target)
    after["path"] = str(path)
    return {"before": before, "after": after, "staged": str(target)}


def usage_digest(readability: dict) -> str:
    payload = [row["research_use"] for row in readability["records"]]
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journal", type=Path)
    args = parser.parse_args()
    result = publish_transaction(args.journal)
    print(json.dumps({"day": result["day"], "status": result["status"], "files": len(result["files"])}))


if __name__ == "__main__":
    main()
