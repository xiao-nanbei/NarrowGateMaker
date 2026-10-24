"""Move retained funding bytes into accounting storage; no market-data rebuild."""

import hashlib
import json
import os
from pathlib import Path
import tempfile

import pyarrow.parquet as pq

from data_paths import daily_market_path


def _digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _save(path, payload):
    fd, name = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def migrate(root: Path, *, symbol="BTCUSDC"):
    root = root.resolve()
    raw = root / "raw"
    old = raw / "binance_futures" / symbol
    destination = daily_market_path("2000-01-01", symbol, "funding", root).parent
    sources = {p.parent.name: p for p in old.glob("*/funding.parquet")}
    for path in destination.glob("????-??-??.parquet"):
        sources.setdefault(path.stem, path)
    if not sources:
        raise ValueError("No retained funding files")
    index_path = raw / "daily-index.json"
    index = json.loads(index_path.read_text())
    records = []
    moves = []
    # Validate all inputs and collisions before moving any file.
    for day, source in sorted(sources.items()):
        target = daily_market_path(day, symbol, "funding", root)
        if source.is_symlink() or target.is_symlink():
            raise ValueError("Funding symlinks are not supported")
        digest = _digest(source)
        if target.exists() and _digest(target) != digest:
            raise ValueError(f"Conflicting funding destination: {day}")
        table = pq.read_table(source)
        if set(table.column("symbol").to_pylist()) != {symbol}:
            raise ValueError(f"Funding symbol mismatch: {day}")
        records.append({"day": day, "symbol": symbol, "channel": "funding",
                        "path": str(target), "sha256": digest,
                        "size_bytes": source.stat().st_size, "rows": table.num_rows})
        moves.append((source, target, digest))
    destination.mkdir(parents=True, exist_ok=True)
    for source, target, digest in moves:
        if source != target:
            if target.exists():
                source.unlink()  # Equal content already verified above.
            else:
                source.rename(target)
        if _digest(target) != digest:
            raise ValueError("Funding bytes changed during relocation")
    old_records = {(r.get("symbol"), r.get("day")): r for r in index["records"]
                   if r.get("channel") == "funding"}
    updated = [r for r in index["records"]
               if not (r.get("channel") == "funding" and r.get("symbol") == symbol)]
    for record in records:
        previous = old_records.get((symbol, record["day"]), {})
        updated.append({**previous, **record, "storage_role": "raw_accounting_input"})
    index["records"] = updated
    _save(index_path, index)
    receipt = {"schema": "funding_layout_migration.v1",
               "visibility": "local_only_do_not_publish", "symbol": symbol,
               "files": len(records), "rows": sum(r["rows"] for r in records),
               "bytes_unchanged": True, "records": records}
    _save(destination / "manifest.json", receipt)
    # Only empty directories and Finder metadata; never recursively remove data.
    if old.exists():
        for directory in sorted(old.iterdir()):
            if directory.is_dir():
                (directory / ".DS_Store").unlink(missing_ok=True)
                if not any(directory.iterdir()):
                    directory.rmdir()
        (old / ".DS_Store").unlink(missing_ok=True)
        if not any(old.iterdir()):
            old.rmdir()
    return receipt


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    result = migrate(args.root)
    print(json.dumps({k: v for k, v in result.items() if k != "records"}))
