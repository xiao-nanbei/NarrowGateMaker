"""Owner-connected market context and immutable historical simulated fill queries.

Only the local CLI registers paths. HTTP queries use result IDs and bounded UTC
windows. This display index neither executes replay nor rewrites its evidence.
"""

import base64
import csv
import json
import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

INTERVALS = (1, 5, 60, 300)
MAX_CANDLES = 5000
MAX_WINDOW_MS = 86_400_000
CLASSIFICATION = "simulated_historical_fills"


def initialize(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS market_connections (
            result_id TEXT PRIMARY KEY, bars_dir TEXT);
        CREATE TABLE IF NOT EXISTS market_fills (
            result_id TEXT NOT NULL, id TEXT NOT NULL, ts INTEGER NOT NULL,
            ordinal INTEGER NOT NULL, order_id TEXT, payload TEXT NOT NULL,
            snapshot TEXT NOT NULL, PRIMARY KEY(result_id,id));
        CREATE INDEX IF NOT EXISTS market_fill_window
            ON market_fills(result_id,ts,ordinal);
        CREATE INDEX IF NOT EXISTS market_fill_order
            ON market_fills(result_id,order_id,ordinal);
    """)


def _number(row, key, *, integer=False, required=False):
    value = row.get(key)
    if value in (None, ""):
        if required:
            raise ValueError(f"fill trace is missing {key}")
        return None
    try:
        result = int(value) if integer else float(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"invalid fill trace {key}") from exc
    if not math.isfinite(result):
        raise ValueError(f"non-finite fill trace {key}")
    return result


def _clock(row, key):
    value = _number(row, key, integer=True)
    return value if value is not None and value >= 0 else None


def _source_id(row, key):
    value = _number(row, key, integer=True)
    return str(value) if value is not None and value >= 0 else None


def _days(segment):
    start, end = date.fromisoformat(segment["start_day"]), date.fromisoformat(segment["end_day"])
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


def connect_b0(store, result_id: str, summary_path: Path, bars_dir: Path | None = None):
    # Reuse the canonical small-summary projection, including its selected-file
    # boundary and summary-root identity. No extra leaf hashes or research gates.
    from narrowgate.studio import b0_projection, dumps

    report = store.result(result_id)
    projection = b0_projection(summary_path)
    if projection != {key: value for key, value in report.items() if key != "imported_at"}:
        raise ValueError("connection must reference the original imported B0 result")
    if store.root.stat().st_mode & 0o077:
        raise ValueError("market connections require an owner-only state directory (mode 0700)")
    if bars_dir is not None:
        bars_dir = bars_dir.resolve()
        if not bars_dir.is_dir():
            raise ValueError("market bars directory is unavailable")
    root = summary_path.resolve().parent
    summary = json.loads(summary_path.read_text())
    indexed = []
    for segment, selected in zip(report["segments"], summary["segments"], strict=True):
        path = (root / (selected["selected_output"] + ".fill_trace.csv")).resolve()
        if not path.is_relative_to(root):
            raise ValueError("selected fill trace escaped its summary root")
        days = set(_days(segment))
        seen = set()
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                sequence = _number(row, "fill_sequence", integer=True, required=True)
                ts = _number(row, "fill_ts", integer=True, required=True)
                side = row.get("side")
                if (
                    sequence < 0
                    or sequence in seen
                    or side not in {"BUY", "SELL"}
                    or row.get("arm") != "baseline"
                    or datetime.fromtimestamp(ts / 1000, UTC).date().isoformat() not in days
                ):
                    raise ValueError(
                        "fill trace has duplicate identity or mismatched segment coverage"
                    )
                seen.add(sequence)
                index = segment["index"]
                source_order_id = _source_id(row, "order_id")
                order_id = f"s{index}-o{source_order_id}" if source_order_id is not None else None
                fill = {
                    "id": f"s{index}-f{sequence}",
                    "segment_index": index,
                    "fill_sequence": sequence,
                    "fill_ts_ms": ts,
                    "visible_ts_ms": _clock(row, "last_private_fill_visible_ts_ms"),
                    "side": side,
                    "price": _number(row, "quote_px", required=True),
                    "fill_trade_price": _number(row, "fill_trade_px"),
                    "quantity": _number(row, "fill_qty", required=True),
                    "order_id": order_id,
                    "source_order_id": source_order_id,
                    "inventory_before": _number(row, "inventory_before_fill"),
                    "inventory_after": _number(row, "inventory_after_fill"),
                    "fee": _number(row, "fill_fee_usdc"),
                    "fee_asset": "USDC" if row.get("fill_fee_asset") == "USDC" else None,
                    "campaign_id": _source_id(row, "campaign_id"),
                    "campaign_id_at_submit": _source_id(row, "campaign_id_at_submit"),
                }
                if fill["price"] <= 0 or fill["quantity"] <= 0:
                    raise ValueError("fill execution price and quantity must be positive")
                snapshot = {
                    "source_order_id": source_order_id,
                    "segment_index": index,
                    "side": side,
                    "price": _number(row, "price"),
                    "quantity": _number(row, "quantity"),
                    **{
                        f"{field}_ms": _clock(row, field)
                        for field in (
                            "submit_ts",
                            "activate_ts",
                            "new_ack_ts",
                            "cancel_request_ts",
                            "cancel_effective_ts",
                            "cancel_ack_ts",
                        )
                    },
                }
                indexed.append(
                    (
                        result_id,
                        fill["id"],
                        ts,
                        len(indexed),
                        order_id,
                        dumps(fill),
                        dumps(snapshot),
                    )
                )
        if len(seen) != selected["fills"]:
            raise ValueError("selected fill trace count differs from the completed B0 summary")
    with store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("DELETE FROM market_fills WHERE result_id=?", (result_id,))
        db.executemany("INSERT INTO market_fills VALUES (?,?,?,?,?,?,?)", indexed)
        db.execute(
            "INSERT OR REPLACE INTO market_connections VALUES (?,?)",
            (result_id, str(bars_dir) if bars_dir is not None else None),
        )
    return market_info(store, result_id)


def _connection(store, result_id):
    with store.connect() as db:
        row = db.execute(
            "SELECT * FROM market_connections WHERE result_id=?", (result_id,)
        ).fetchone()
    return dict(row) if row else None


def _source(connection):
    return {
        "kind": "retained_market_1s_bars",
        "location": "local",
        "connected": bool(connection and connection["bars_dir"]),
        "identity": "context_only_not_exact_replay_binding",
    }


def market_info(store, result_id):
    report = store.result(result_id)
    connection = _connection(store, result_id)
    return {
        "result_id": result_id,
        "symbol": "BTCUSDC",
        "classification": CLASSIFICATION,
        "status": "available" if connection else "unavailable",
        "reason": None if connection else "owner_connection_not_registered",
        "source": _source(connection),
        "segments": [
            {key: segment[key] for key in ("index", "start_day", "end_day", "source")}
            | {"days": _days(segment)}
            for segment in report["segments"]
        ],
        "intervals_s": list(INTERVALS),
        "max_candles": MAX_CANDLES,
        "max_window_ms": MAX_WINDOW_MS,
        "order_lifecycle": "partial_fill_snapshots_only",
        "pnl": "unavailable",
        "inventory_clock": "local_fill_callback_order",
        "fee_semantics": "signed_cost_positive_rebate_negative_already_in_trading_pnl",
    }


def _window(start_ms, end_ms):
    if not 0 <= start_ms < end_ms < 253402300800000 or end_ms - start_ms > MAX_WINDOW_MS:
        raise ValueError("provide a positive [start_ms,end_ms) UTC window of at most 24 hours")


def candles(store, result_id, start_ms, end_ms, interval_s=60):
    _window(start_ms, end_ms)
    if interval_s not in INTERVALS:
        raise ValueError("interval_s must be 1, 5, 60 or 300")
    step = interval_s * 1000
    if start_ms % step or end_ms % step:
        raise ValueError("candle window boundaries must align to interval_s")
    if (end_ms - start_ms) // step > MAX_CANDLES:
        raise ValueError("candle window exceeds 5000 bars; shorten it or increase interval_s")
    info = market_info(store, result_id)
    connection = _connection(store, result_id)
    response = {
        "result_id": result_id,
        "status": "unavailable",
        "reason": None,
        "source": info["source"],
        "start_ms": start_ms,
        "end_ms": end_ms,
        "interval_s": interval_s,
        "items": [],
        "count": 0,
        "truncated": False,
        "gaps": 0,
        "gap_semantics": "seconds_without_retained_bar_unknown_cause",
    }
    if not connection or not connection["bars_dir"]:
        return response | {"reason": "market_source_not_connected"}
    allowed = {day for segment in info["segments"] for day in segment["days"]}
    first = datetime.fromtimestamp(start_ms / 1000, UTC).date()
    last = datetime.fromtimestamp((end_ms - 1) / 1000, UTC).date()
    days = [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]
    if not set(days).issubset(allowed):
        return response | {"reason": "window_outside_result_coverage"}
    try:
        import pyarrow.parquet as pq
        from pyarrow import ArrowException
    except ImportError:
        return response | {"reason": "parquet_reader_unavailable"}
    bars = []
    root = Path(connection["bars_dir"])
    for day in days:
        path = (root / f"BTCUSDC-1s-{day}.parquet").resolve()
        if not path.is_relative_to(root) or not path.is_file():
            return response | {"reason": "market_day_file_unavailable"}
        try:
            if pq.ParquetFile(path).metadata.num_rows > 86400:
                return response | {"reason": "market_day_exceeds_1s_row_bound"}
            table = pq.read_table(
                path,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
                filters=[("timestamp", ">=", start_ms), ("timestamp", "<", end_ms)],
            )
            bars.extend(table.to_pylist())
        except (OSError, ValueError, KeyError, ArrowException):
            return response | {"reason": "market_day_unreadable_or_schema_unavailable"}
    bars.sort(key=lambda row: row["timestamp"])
    previous = None
    aggregated = {}
    for row in bars:
        ts = row["timestamp"]
        values = [row[key] for key in ("open", "high", "low", "close", "volume")]
        if (
            not isinstance(ts, int)
            or ts % 1000
            or ts == previous
            or not all(isinstance(v, (int, float)) and math.isfinite(v) for v in values)
            or min(values[:4]) <= 0
            or values[4] < 0
            or row["low"] > min(row["open"], row["close"])
            or row["high"] < max(row["open"], row["close"])
        ):
            return response | {"reason": "market_bars_invalid"}
        previous = ts
        bucket = ts // step * step
        if bucket not in aggregated:
            aggregated[bucket] = {
                "time_ms": bucket,
                **{key: row[key] for key in ("open", "high", "low", "close", "volume")},
                "source_rows": 1,
            }
        else:
            target = aggregated[bucket]
            target.update(
                high=max(target["high"], row["high"]),
                low=min(target["low"], row["low"]),
                close=row["close"],
                volume=target["volume"] + row["volume"],
                source_rows=target["source_rows"] + 1,
            )
    items = list(aggregated.values())
    return response | {
        "status": "available",
        "items": items,
        "count": len(items),
        "gaps": (end_ms - start_ms) // 1000 - len(bars),
    }


def fills(store, result_id, start_ms, end_ms, limit=200, cursor=""):
    _window(start_ms, end_ms)
    store.result(result_id)
    if not 1 <= limit <= 1000:
        raise ValueError("fill page limit must be 1–1000")
    after_ts, after_ordinal = -1, -1
    if cursor:
        try:
            if len(cursor) > 512:
                raise ValueError
            decoded = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
            owner, start, end, after_ts, after_ordinal = decoded
            if (owner, start, end) != (result_id, start_ms, end_ms):
                raise ValueError
            if type(after_ts) is not int or type(after_ordinal) is not int:
                raise ValueError
        except (ValueError, TypeError, UnicodeError) as exc:
            raise ValueError("invalid cursor for this result and window") from exc
    connection = _connection(store, result_id)
    with store.connect() as db:
        rows = db.execute(
            "SELECT * FROM market_fills WHERE result_id=? AND ts>=? AND ts<? "
            "AND (ts>? OR (ts=? AND ordinal>?)) ORDER BY ts,ordinal LIMIT ?",
            (result_id, start_ms, end_ms, after_ts, after_ts, after_ordinal, limit + 1),
        ).fetchall()
    truncated = len(rows) > limit
    rows = rows[:limit]
    next_cursor = None
    if truncated:
        last = rows[-1]
        next_cursor = base64.urlsafe_b64encode(
            json.dumps([result_id, start_ms, end_ms, last["ts"], last["ordinal"]]).encode()
        ).decode()
    return {
        "result_id": result_id,
        "status": "available" if connection else "unavailable",
        "reason": None if connection else "owner_connection_not_registered",
        "classification": CLASSIFICATION,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "items": [json.loads(row["payload"]) for row in rows],
        "count": len(rows),
        "next_cursor": next_cursor,
        "truncated": truncated,
    }


def order(store, result_id, order_id):
    from narrowgate.studio import identifier

    store.result(result_id)
    identifier(order_id)
    with store.connect() as db:
        count = db.execute(
            "SELECT COUNT(*) FROM market_fills WHERE result_id=? AND order_id=?",
            (result_id, order_id),
        ).fetchone()[0]
        rows = db.execute(
            "SELECT * FROM market_fills WHERE result_id=? AND order_id=? "
            "ORDER BY ordinal LIMIT 1000",
            (result_id, order_id),
        ).fetchall()
    if not rows:
        raise KeyError(order_id)
    # Snapshot fields are those captured at actual fills, not target quotes or a
    # reconstructed complete lifecycle. Later cancel outcomes may be absent.
    snapshot = json.loads(rows[-1]["snapshot"])
    items = [json.loads(row["payload"]) for row in rows]
    return {
        "result_id": result_id,
        "id": order_id,
        "status": "available",
        "classification": CLASSIFICATION,
        "scope": "fill_trace_snapshots_only",
        **snapshot,
        "filled_quantity": sum(row["quantity"] for row in items) if count <= 1000 else None,
        "fill_count": count,
        "fills": items,
        "truncated": count > 1000,
        "lifecycle_complete": False,
        "pnl": None,
    }
