"""Acquire hourly individual trades without changing canonical market data.

Source objects are receive-hour partitions. The optional next-day first hour is
boundary evidence only; consumers must partition by the original trade_time.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import io
import json
import os
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import zstandard as zstd

from data.downloaders import cryptohft_orderbook as transport


def hourly_plan(start: str, end: str, *, next_day_first_hour: bool = False):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last:
        raise ValueError("start must not exceed end")
    days = (first + timedelta(days=i) for i in range((last - first).days + 1))
    result = [(day.isoformat(), f"{hour:02}") for day in days for hour in range(24)]
    if next_day_first_hour:
        result.append(((last + timedelta(days=1)).isoformat(), "00"))
    return result


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def inspect_trade_file(path: Path, day: str, hour: str, symbol: str) -> dict:
    """Check source structure/clocks; retain invalid source rows as findings."""
    blob = path.read_bytes()
    digest = hashlib.sha256(blob).hexdigest()
    container = "parquet" if blob[:4] == b"PAR1" else "zstd_wrapped_parquet"
    if container != "parquet":
        with zstd.ZstdDecompressor().stream_reader(io.BytesIO(blob)) as reader:
            blob = reader.read()
    table = pq.read_table(io.BytesIO(blob))
    required = {
        "received_time", "event_time", "symbol", "trade_id", "price",
        "quantity", "trade_time", "is_buyer_maker", "order_type",
    }
    if not required.issubset(table.column_names):
        raise ValueError("source schema lacks individual-trade fields")
    for name in ("received_time", "event_time", "trade_time", "trade_id"):
        if not pa.types.is_integer(table[name].type):
            raise ValueError(f"source {name} is not integer")
    if not pa.types.is_boolean(table["is_buyer_maker"].type):
        raise ValueError("source maker side is not boolean")
    if table.num_rows and set(pc.unique(table["symbol"]).to_pylist()) != {symbol}:
        raise ValueError("source symbol differs")
    clocks = {name: pc.min_max(table[name]).as_py()
              for name in ("received_time", "event_time", "trade_time")}
    for name, low, high in (("received_time", 10**18, 3 * 10**18),
                            ("event_time", 10**12, 3 * 10**12),
                            ("trade_time", 10**12, 3 * 10**12)):
        span = clocks[name]
        if table.num_rows and (span["min"] is None
                               or not low <= span["min"] <= span["max"] <= high):
            raise ValueError(f"source {name} has unexpected clock units")
    findings = {f"null_{name}": table[name].null_count
                for name in required if table[name].null_count}
    for name in ("price", "quantity"):
        values = pc.cast(table[name], pa.float64())
        bad = pc.or_(pc.invert(pc.is_finite(values)), pc.less_equal(values, 0))
        count = pc.sum(pc.fill_null(bad, True)).as_py() or 0
        if count:
            findings[f"invalid_{name}"] = count
    duplicates = table.num_rows - len(pc.unique(table["trade_id"]))
    if duplicates:
        findings["duplicate_trade_id_rows"] = duplicates
    start_ns = int(datetime.fromisoformat(f"{day}T{hour}:00:00")
                   .replace(tzinfo=UTC).timestamp()) * 10**9
    outside = pc.or_(pc.less(table["received_time"], start_ns),
                     pc.greater_equal(table["received_time"], start_ns + 3600 * 10**9))
    count = pc.sum(pc.fill_null(outside, True)).as_py() or 0
    if count:
        findings["received_outside_object_hour"] = count
    return {
        "sha256": digest, "bytes": path.stat().st_size, "rows": table.num_rows,
        "container": container, "schema": str(table.schema), "columns": table.column_names,
        "clock_ranges": clocks, "trade_id_range": pc.min_max(table["trade_id"]).as_py(),
        "value_findings": findings,
        "status": ("EMPTY_SOURCE_UNKNOWN" if not table.num_rows else
                   "READABLE_WITH_FINDINGS" if findings else "READABLE"),
    }


def read_receipts(paths: list[Path]) -> dict:
    result = {}
    for path in paths:
        for line in path.read_text().splitlines():
            row = json.loads(line)
            if row.get("channel") != "trades" or not row.get("status", "").startswith("READABLE"):
                continue
            key = row["day"], f"{int(row['hour']):02}"
            if key in result and result[key]["sha256"] != row["sha256"]:
                raise ValueError(f"conflicting reusable source identity for {key}")
            result[key] = row
    return result


def _recover_receipt_journal(path: Path) -> list[dict]:
    """Read committed lines and discard only an unfinished tail under the lock."""
    if not path.exists():
        return []
    with path.open("r+b") as handle:
        content = handle.read()
        committed_end = content.rfind(b"\n") + 1
        # Validate every complete line before repairing anything. A corrupt
        # committed record is not an interrupted append and must remain visible.
        rows = [json.loads(line) for line in content[:committed_end].splitlines()]
        if any(not isinstance(row, dict) for row in rows):
            raise ValueError("receipt journal contains a non-object record")
        if committed_end != len(content):
            handle.truncate(committed_end)
            handle.flush()
            os.fsync(handle.fileno())
    return rows


def atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, allow_nan=False)
        handle.write("\n")
    temporary.chmod(0o600)
    temporary.replace(path)


class AccessBlocked(Exception):
    pass


class ObservedClient(transport.CryptoHFTClient):
    """Allow the existing JWT refresh; stop on repeat auth errors or quota."""

    def __init__(self, key: str, jwt: str, stop: threading.Event):
        self.status_counts = Counter()
        self.stop = stop
        super().__init__(key, jwt=jwt, transport="rest")
        self.session.hooks["response"] = [self.observe]

    def _reset_session(self):
        super()._reset_session()
        self.session.hooks["response"] = [self.observe]

    def observe(self, response, **kwargs):
        code = response.status_code
        self.status_counts[str(code)] += 1
        if code == 429 or (code in (401, 403)
                           and self.status_counts["401"] + self.status_counts["403"] > 1):
            self.stop.set()
            response.close()
            raise AccessBlocked("QUOTA_BLOCKED" if code == 429 else "AUTHORIZATION_BLOCKED")
        return response


def acquire(args) -> int:
    os.umask(0o077)
    args.state_dir.mkdir(parents=True, exist_ok=True)
    with (args.state_dir / "running.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return _acquire_locked(args)


def _acquire_locked(args) -> int:
    plan = hourly_plan(args.start, args.end, next_day_first_hour=args.next_day_first_hour)
    config = {"start": args.start, "end": args.end, "symbol": args.symbol,
              "next_day_first_hour": args.next_day_first_hour,
              "output_dir": str(args.output_dir.resolve()), "channel": "trades"}
    plan_path = args.state_dir / "plan.json"
    if plan_path.exists() and json.loads(plan_path.read_text())["configuration"] != config:
        raise ValueError("state directory belongs to a different acquisition plan")
    if not plan_path.exists():
        atomic_json(plan_path, {"configuration": config, "hours": plan,
                               "created_at_utc": datetime.now(UTC).isoformat()})
    receipts = args.state_dir / "objects.jsonl"
    completed = _recover_receipt_journal(receipts)
    # Resume only incomplete/transport-failed objects; do not retry permanent 404s.
    done = {(r["day"], r["hour"]) for r in completed
            if r["status"].startswith("READABLE") or r["status"] in {"NOT_FOUND", "EMPTY_SOURCE_UNKNOWN"}}
    reusable = read_receipts(args.reuse_manifest + ([receipts] if receipts.exists() else []))
    stop, local = threading.Event(), threading.local()
    transport.DEFAULT_DOWNLOAD_ATTEMPTS = args.attempts
    key = os.environ.get(transport.API_KEY_ENV)
    if not key:
        raise RuntimeError("CryptoHFT API credential is absent")
    jwt = transport.CryptoHFTClient(key, transport="rest").ensure_jwt()
    total = len(plan)

    def progress():
        latest = {(r["day"], r["hour"]): r for r in completed}
        return {"updated_at_utc": datetime.now(UTC).isoformat(), "total": total,
                "objects_recorded": len(latest), "status_counts": dict(Counter(r["status"] for r in latest.values())),
                "bytes": sum(r.get("bytes", 0) for r in latest.values()),
                "rows": sum(r.get("rows", 0) for r in latest.values()), "state": "RUNNING"}

    def work(pair):
        day, hour = pair
        source = Path(f"binance_futures/{day}/{hour}/{args.symbol}_trades.parquet")
        path = args.output_dir / source
        row = {"day": day, "hour": hour, "channel": "trades", "source_object": source.as_posix(),
               "path": str(path), "started_at_utc": datetime.now(UTC).isoformat()}
        if stop.is_set():
            return {**row, "status": "NOT_ATTEMPTED_ACCESS_BLOCKED"}
        try:
            previous = reusable.get(pair)
            if previous is not None:
                path = Path(previous["path"])
                row["path"] = str(path)
                if sha256(path) != previous["sha256"]:
                    raise ValueError("reusable source digest differs")
                row["download_status"] = "REUSED_VERIFIED_SOURCE"
            elif path.exists():
                # The transport publishes the file before its receipt is
                # appended. Recover that crash window by inspecting the exact
                # existing bytes below, never by overwriting or redownloading.
                row["download_status"] = "RECOVERED_EXISTING_SOURCE"
            else:
                if not hasattr(local, "client"):
                    local.client = ObservedClient(key, jwt, stop)
                client = local.client
                client.status_counts.clear()
                row["download_status"] = client.download_file(source, path)
                row["http_status_counts"] = dict(client.status_counts)
                if row["download_status"] == "404":
                    return {**row, "status": "NOT_FOUND"}
            row.update(inspect_trade_file(path, day, hour, args.symbol))
        except AccessBlocked as exc:
            row["status"] = str(exc)
        except Exception as exc:
            # Request text can contain signed URLs. The exception type is enough here.
            row.update(status="FAILED", error_type=type(exc).__name__)
        row["completed_at_utc"] = datetime.now(UTC).isoformat()
        return row

    with receipts.open("a") as handle, ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(work, pair) for pair in plan if pair not in done]
        atomic_json(args.state_dir / "progress.json", progress())
        for future in as_completed(futures):
            row = future.result()
            completed.append(row)
            handle.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
            handle.flush()
            atomic_json(args.state_dir / "progress.json", progress())
    summary = progress()
    counts = summary["status_counts"]
    summary["state"] = "COMPLETED" if all(k.startswith("READABLE") for k in counts) else "WITH_MISSING_OR_FINDINGS"
    summary["objects_sha256"] = sha256(receipts)
    atomic_json(args.state_dir / "summary.json", summary)
    atomic_json(args.state_dir / "progress.json", summary)
    return 0 if summary["state"] == "COMPLETED" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--next-day-first-hour", action="store_true")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--reuse-manifest", type=Path, action="append", default=[])
    parser.add_argument("--workers", type=int, choices=range(1, 5), default=4)
    parser.add_argument("--attempts", type=int, choices=range(1, 6), default=3)
    args = parser.parse_args()
    if not args.symbol.isalnum() or args.symbol.upper() != args.symbol:
        parser.error("symbol must be uppercase alphanumeric")
    return acquire(args)


if __name__ == "__main__":
    raise SystemExit(main())
