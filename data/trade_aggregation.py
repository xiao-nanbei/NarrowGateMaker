"""Individual-trade source union and explicitly synthetic 100 ms flow buckets.

This is offline ETL, not a reconstruction of exchange-native aggTrade messages.
Execution keeps the individual tape. Bucket completion is not network receipt.
Secondary inputs may be canonical trade Parquet, Tardis daily trade CSV
(plain/XZ/Zstandard), or the existing hourly Parquet boundary. A verified
preparation reports content_changed=False when no canonical trade is added;
callers can then keep current raw/derived files and their feature identities.
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta
from decimal import Decimal
import io
import json
import lzma
import os
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import zstandard as zstd

from data.daily_raw import (
    TRADE_SCHEMA, _day_bounds, _identity, _publish_atomic,
    _reject_output_symlinks, _write_verified_tables, sha256_file,
)

SCALE = 100_000_000
AGGREGATION_SCHEMA = "trade_aggregates_100ms_v1"
AGGREGATE_SCHEMA = pa.schema([
    ("exchange", pa.string()), ("symbol", pa.string()), ("group_id", pa.string()),
    ("bucket_start_ms", pa.int64()), ("bucket_end_ms", pa.int64()),
    ("feature_ready_ts_ms", pa.int64()), ("first_event_ts_ms", pa.int64()),
    ("last_event_ts_ms", pa.int64()), ("first_event_id", pa.int64()),
    ("last_event_id", pa.int64()), ("price", pa.string()),
    ("quantity", pa.string()), ("is_buyer_maker", pa.bool_()),
    ("trade_count", pa.int64()),
])
SECONDARY_SCHEMA = pa.schema([
    ("trade_id", pa.int64()), ("price", pa.string()), ("quantity", pa.string()),
    ("trade_time", pa.int64()), ("is_buyer_maker", pa.bool_()),
    ("symbol", pa.string()), ("exchange", pa.string()), ("local_timestamp", pa.int64()),
])


def _integer_column(table: pa.Table, name: str, *, nullable: bool = False) -> pa.ChunkedArray:
    value = table[name]
    if not pa.types.is_integer(value.type) or (not nullable and value.null_count):
        raise ValueError(f"{name} must contain {'nullable ' if nullable else ''}integers")
    return pc.cast(value, pa.int64(), safe=True)


def _secondary_table(table: pa.Table, *, symbol: str = "BTCUSDC") -> pa.Table:
    """Adapt canonical/Tardis individual trades or the existing hourly boundary.

    Tardis side is the aggressor side and its clocks are UTC microseconds.
    The maintained native trade contract is millisecond-granular: finer source
    event time is rejected, never rounded. Receive time keeps its own clock.
    """
    columns = set(table.column_names)
    daily = {"exchange", "symbol", "timestamp", "local_timestamp", "id", "side", "price", "amount"}
    if daily <= columns:
        ids = _integer_column(table, "id")
        timestamp = _integer_column(table, "timestamp")
        times = pc.divide(timestamp, 1000)
        if pc.any(pc.not_equal(timestamp, pc.multiply_checked(times, 1000))).as_py():
            raise ValueError("secondary exchange timestamp is not aligned to native milliseconds")
        if "time" in columns and not pc.all(pc.equal(times, _integer_column(table, "time"))).as_py():
            raise ValueError("secondary exchange clock aliases differ")
        side = table["side"]
        if side.null_count or not pc.all(pc.is_in(side, value_set=pa.array(["buy", "sell"]))).as_py():
            raise ValueError("secondary aggressor side must be buy or sell")
        maker = pc.equal(side, "sell")
        if "is_buyer_maker" in columns:
            native_maker = table["is_buyer_maker"]
            if (not pa.types.is_boolean(native_maker.type) or native_maker.null_count
                    or not pc.all(pc.equal(maker, native_maker)).as_py()):
                raise ValueError("secondary side and maker aliases differ")
        quantity = pc.cast(table["amount"], pa.string())
        if "qty" in columns:
            if not np.array_equal(_units(quantity.to_pandas()), _units(table["qty"].to_pandas())):
                raise ValueError("secondary quantity aliases differ")
        local = _integer_column(table, "local_timestamp", nullable=True)
        exchange = table["exchange"]
    else:
        required = {"trade_id", "price", "quantity", "trade_time", "is_buyer_maker", "symbol"}
        if not required <= columns:
            raise ValueError("missing individual trade schema fields")
        ids, times = _integer_column(table, "trade_id"), _integer_column(table, "trade_time")
        maker, quantity = table["is_buyer_maker"], pc.cast(table["quantity"], pa.string())
        if not pa.types.is_boolean(maker.type) or maker.null_count:
            raise ValueError("maker side must be boolean")
        if "local_timestamp" in columns:
            local = _integer_column(table, "local_timestamp", nullable=True)
        elif "received_time" in columns:
            local = pc.divide(_integer_column(table, "received_time", nullable=True), 1000)
        else:
            local = pa.nulls(table.num_rows, pa.int64())
        exchange = table["exchange"] if "exchange" in columns else pa.repeat("binance-futures", table.num_rows)
    if exchange.null_count or not pc.all(pc.equal(exchange, "binance-futures")).as_py():
        raise ValueError("individual trade venue mismatch")
    if table["symbol"].null_count or not pc.all(pc.equal(table["symbol"], symbol)).as_py():
        raise ValueError("individual trade symbol mismatch")
    return pa.Table.from_arrays([
        ids, pc.cast(table["price"], pa.string()), quantity, times, maker,
        table["symbol"], exchange, local,
    ], schema=SECONDARY_SCHEMA)


def _units(values: pd.Series) -> pd.Series:
    result = {}
    for value in values.unique():
        if value is None or pd.isna(value):
            raise ValueError("missing price or quantity")
        number = Decimal(str(value)) * SCALE
        if not number.is_finite() or number != number.to_integral_value():
            raise ValueError("unsupported non-finite or sub-1e-8 decimal")
        integer = int(number)
        if abs(integer) > np.iinfo(np.int64).max:
            raise ValueError("decimal exceeds exact integer range")
        result[value] = integer
    return values.map(result).astype("int64")


def _decimal(value: int) -> str:
    return format(Decimal(int(value)) / SCALE, "f")


def _core(table: pa.Table, *, secondary: bool, symbol: str) -> pd.DataFrame:
    names = dict(id="trade_id" if secondary else "id", p="price",
                 q="quantity" if secondary else "qty",
                 t="trade_time" if secondary else "time", maker="is_buyer_maker")
    required = [*names.values(), "symbol"]
    if any(name not in table.column_names for name in required):
        raise ValueError("missing individual trade schema fields")
    frame = table.select(required).to_pandas()
    if not frame.symbol.eq(symbol).all():
        raise ValueError("individual trade symbol mismatch")
    if "exchange" in table.column_names and not table["exchange"].to_pandas().eq("binance-futures").all():
        raise ValueError("individual trade venue mismatch")
    out = pd.DataFrame({dst: frame[src] for dst, src in names.items()})
    if out[["id", "t", "maker"]].isna().any().any():
        raise ValueError("missing trade identity, exchange time or maker side")
    if not pd.api.types.is_bool_dtype(out.maker):
        raise ValueError("maker side must be boolean")
    for name in ("id", "t"):
        if not pd.api.types.is_integer_dtype(out[name]):
            raise ValueError("trade identity and exchange millisecond time must be integers")
        out[name] = out[name].astype("int64")
    if (out.id < 0).any():
        raise ValueError("invalid native trade ID")
    out.p, out.q = _units(out.p), _units(out.q)
    return out


def union_trades(primary: pa.Table, secondary: pa.Table, day: str,
                 symbol: str = "BTCUSDC") -> tuple[pa.Table, dict]:
    """Union valid native IDs; current primary owns shared values and clock.

    Positive price/quantity/side conflicts fail, never average. Secondary-only
    zero records are not trades. All supplied secondary observations are retained
    by the batch writer as provenance, including alternate clocks and rejects.
    """
    start, end = (value // 1000 for value in _day_bounds(day))
    a = _core(primary, secondary=False, symbol=symbol)
    if a.empty or not a.t.between(start, end - 1).all():
        raise ValueError("primary day empty or outside its UTC date")
    if ((a.p <= 0) | (a.q <= 0)).any() or a.id.duplicated().any():
        raise ValueError("invalid or duplicate primary trade")
    if "timestamp" in primary.column_names:
        if not np.array_equal(primary["timestamp"].to_numpy(), a.t.to_numpy() * 1000):
            raise ValueError("primary exchange clock aliases differ")
    stats = dict(primary_rows=len(a), secondary_rows=secondary.num_rows,
                 invalid_secondary_rows=0, overlap_rows=0, added_rows=0,
                 outside_day_secondary_rows=0, exact_duplicates_removed=0,
                 time_conflicts=[], secondary_time_conflicts=[], content_changed=False)
    if secondary.num_rows == 0:
        return primary.cast(TRADE_SCHEMA), stats
    secondary = _secondary_table(secondary, symbol=symbol)
    b = _core(secondary, secondary=True, symbol=symbol)
    valid = (b.p > 0) & (b.q > 0)
    stats["invalid_secondary_rows"] = int((~valid).sum())
    b = b.loc[valid].copy()
    # Compare all supplied IDs before date filtering: a one-ms source difference
    # at midnight must not turn the same primary ID into a second day's trade.
    conflicts = b.groupby("id")[["p", "q", "maker"]].nunique().gt(1).any(axis=1)
    if conflicts.any():
        raise ValueError("conflicting secondary price/quantity/side for native ID")
    time_versions = b.groupby("id").t.nunique()
    if time_versions.gt(1).any():
        stats["secondary_time_conflicts"] = [
            {"id": int(k), "times": sorted(int(t) for t in g.t.unique())}
            for k, g in b[b.id.isin(time_versions[time_versions.gt(1)].index)].groupby("id")
        ]
    n = len(b)
    b = b.sort_values(["t", "id"], kind="stable").drop_duplicates("id", keep="first")
    stats["exact_duplicates_removed"] = n - len(b) - sum(
        len(row["times"]) - 1 for row in stats["secondary_time_conflicts"]
    )
    known = a.set_index("id")
    overlap = b.id.isin(known.index)
    shared = b.loc[overlap].set_index("id")
    official = known.loc[shared.index]
    if shared[["p", "q", "maker"]].ne(official[["p", "q", "maker"]]).any().any():
        raise ValueError("source price/quantity/side conflict for shared native ID")
    stats["overlap_rows"] = len(shared)
    changed = shared.t.ne(official.t)
    stats["time_conflicts"] = [dict(id=int(k), primary_time=int(official.at[k, "t"]),
                                    secondary_time=int(shared.at[k, "t"]))
                               for k in shared.index[changed]]
    if any(row["id"] not in known.index and any(start <= t < end for t in row["times"])
           for row in stats["secondary_time_conflicts"]):
        raise ValueError("secondary-only trade has conflicting exchange clocks")
    extra = b.loc[~overlap]
    in_day = extra.t.between(start, end - 1)
    stats["outside_day_secondary_rows"] = int((~in_day).sum())
    extra = extra.loc[in_day]
    # An ambiguous secondary-only clock cannot be silently chosen at a boundary.
    if set(extra.id) & {row["id"] for row in stats["secondary_time_conflicts"]}:
        raise ValueError("secondary-only trade has conflicting exchange clocks")
    stats["added_rows"] = len(extra)
    stats["content_changed"] = bool(len(extra))
    if extra.empty:
        return primary.cast(TRADE_SCHEMA), stats
    # Materialize only accepted rows, not a second full copy of every supplied
    # source column (often three receive-days plus the canonical day).
    accepted = secondary.select(["price", "quantity", "local_timestamp"]).take(
        pa.array(extra.index.to_numpy(), type=pa.int64())
    ).to_pylist()
    records = []
    for row, source in zip(extra.itertuples(index=False), accepted, strict=True):
        # Keep the exact accepted observation, not the first raw version of an
        # ID (which may have been a rejected zero-value source record).
        price, qty = source["price"], source["quantity"]
        records.append(dict(
            exchange="binance-futures", symbol=symbol, timestamp=int(row.t) * 1000,
            local_timestamp=source["local_timestamp"],
            id=int(row.id), side="sell" if row.maker else "buy", price=price,
            amount=qty, quote_qty=str(Decimal(price) * Decimal(qty)), time=int(row.t),
            qty=qty, is_buyer_maker=bool(row.maker),
        ))
    joined = pa.concat_tables([primary.cast(TRADE_SCHEMA), pa.Table.from_pylist(records, schema=TRADE_SCHEMA)])
    return joined.sort_by([("time", "ascending"), ("id", "ascending")]), stats


def build_aggregates(trades: pa.Table, day: str, window_ms: int = 100) -> pa.Table:
    """Exact-price/side fixed UTC buckets; IDs are synthetic, not f..l blocks."""
    if window_ms != 100:
        raise ValueError("the maintained derived aggregation uses 100 ms buckets")
    symbol_values = trades["symbol"].unique().to_pylist()
    if len(symbol_values) != 1:
        raise ValueError("one symbol is required")
    symbol = symbol_values[0]
    a = _core(trades, secondary=False, symbol=symbol)
    start, end = (value // 1000 for value in _day_bounds(day))
    if a.empty or not a.t.between(start, end - 1).all() or a.id.duplicated().any():
        raise ValueError("invalid individual day for aggregation")
    if ((a.p <= 0) | (a.q <= 0)).any():
        raise ValueError("invalid individual values")
    if sum(int(q) for q in a.q) > np.iinfo(np.int64).max:
        raise ValueError("exact daily quantity overflows int64")
    a = a.sort_values(["t", "id"], kind="stable")
    a["bucket"] = a.t // window_ms * window_ms
    grouped = a.groupby(["bucket", "p", "maker"], sort=True).agg(
        quantity=("q", "sum"), trade_count=("id", "size"),
        first_event_ts_ms=("t", "first"), last_event_ts_ms=("t", "last"),
        first_event_id=("id", "first"), last_event_id=("id", "last"),
    ).reset_index()
    grouped["exchange"], grouped["symbol"] = "binance-futures", symbol
    grouped["group_id"] = [f"ng100:{b}:{p}:{int(m)}" for b, p, m in
                           zip(grouped.bucket, grouped.p, grouped.maker, strict=True)]
    grouped["bucket_start_ms"] = grouped.bucket
    grouped["bucket_end_ms"] = grouped.bucket + window_ms
    grouped["feature_ready_ts_ms"] = grouped.bucket_end_ms
    grouped["price"] = grouped.p.map(_decimal)
    grouped["quantity"] = grouped.quantity.map(_decimal)
    grouped["is_buyer_maker"] = grouped.maker
    if int(grouped.trade_count.sum()) != len(a):
        raise ValueError("aggregate child-count mismatch")
    if sum(Decimal(q) for q in grouped.quantity) != Decimal(sum(int(q) for q in a.q)) / SCALE:
        raise ValueError("aggregate quantity mismatch")
    return pa.Table.from_pandas(grouped[AGGREGATE_SCHEMA.names], schema=AGGREGATE_SCHEMA,
                               preserve_index=False).replace_schema_metadata({
        b"narrowgate.schema": AGGREGATION_SCHEMA.encode(),
        b"native_aggtrade_identity": b"false", b"execution_event_authority": b"false",
        b"clock": b"UTC_exchange_bucket_right_not_receive_time",
    })


def _read_secondary(path: Path, *, symbol: str = "BTCUSDC") -> pa.Table:
    with path.open("rb") as stream:
        magic = stream.read(4)
    if magic == b"PAR1":
        return _secondary_table(pq.read_table(path), symbol=symbol)
    # This is only the ingestion boundary; it does not download or retire data.
    with path.open("rb") as raw:
        if magic == b"\xfd7zX":
            decoded = lzma.LZMAFile(raw)
        elif magic == b"\x28\xb5\x2f\xfd":
            decoded = zstd.ZstdDecompressor().stream_reader(raw)
        else:
            decoded = raw
        with io.BufferedReader(decoded) as stream:
            if stream.peek(4)[:4] == b"PAR1":
                # The legacy wrapped Parquet reader needs seekable storage.
                # Spill beyond 8 MiB instead of holding decompressed bytes twice.
                with tempfile.SpooledTemporaryFile(max_size=8 * 1024**2) as spool:
                    while chunk := stream.read(1024**2):
                        spool.write(chunk)
                    spool.seek(0)
                    return _secondary_table(pq.read_table(spool), symbol=symbol)
            if decoded is not raw:
                # Reuse the delivery validator rather than trusting permissive
                # streaming decoders at this independently callable boundary.
                from data.download_tardis_archive import _validate_archive

                _validate_archive(path, "xz" if magic == b"\xfd7zX" else "zstd")
            types = {name: TRADE_SCHEMA.field(name).type for name in (
                "exchange", "symbol", "timestamp", "local_timestamp", "id", "side", "price", "amount"
            )}
            table = pacsv.read_csv(stream, read_options=pacsv.ReadOptions(use_threads=False),
                                   convert_options=pacsv.ConvertOptions(column_types=types))
            return _secondary_table(table, symbol=symbol)


def _write_json(path: Path, payload: dict) -> None:
    _reject_output_symlinks(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(payload, stream, indent=2, allow_nan=False)
            stream.write("\n")
        _publish_atomic(Path(name), path)
    finally:
        Path(name).unlink(missing_ok=True)


def prepare_day(primary: Path, sources: list[dict], day: str, output_dir: Path,
                *, symbol: str = "BTCUSDC") -> dict:
    """Create verified staged outputs. Never delete or replace canonical inputs."""
    primary = Path(primary)
    _reject_output_symlinks(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    receipt = output_dir / "receipt.json"
    before = _identity(primary)
    primary_sha = sha256_file(primary)
    identity = [(str(Path(row["path"])), row["sha256"]) for row in sources]
    current = date.fromisoformat(day)
    neighbors = [primary.parent.parent / (current + timedelta(days=n)).isoformat() / "trades.parquet"
                 for n in (-1, 1)]
    context_before = {str(path): _identity(path) for path in neighbors if path.is_file()}
    context = {path: sha256_file(Path(path)) for path in context_before}
    if receipt.exists():
        previous = json.loads(receipt.read_text())
        if previous.get("symbol", "BTCUSDC") != symbol:
            raise ValueError("prepared day symbol changed")
        if previous["primary_sha256"] != primary_sha or previous["sources"] != [list(v) for v in identity]:
            raise ValueError("prepared day input identity changed")
        if previous.get("adjacent_primary_sha256") != context:
            raise ValueError("prepared adjacent primary identity changed")
        for path, digest in identity:
            if sha256_file(Path(path)) != digest:
                raise ValueError("prepared secondary source identity changed")
        for name, digest in previous["output_sha256"].items():
            if sha256_file(output_dir / name) != digest:
                raise ValueError("prepared output identity changed")
        if before != _identity(primary) or any(_identity(Path(path)) != value
                                               for path, value in context_before.items()):
            raise ValueError("prepared primary changed during resume verification")
        return previous
    frames = []
    for row in sources:
        if row.get("symbol", symbol) != symbol:
            raise ValueError("secondary source manifest symbol mismatch")
        source = Path(row["path"])
        observed = _identity(source)
        if sha256_file(source) != row["sha256"]:
            raise ValueError("secondary source digest changed")
        table = _read_secondary(source, symbol=symbol)
        if "rows" in row and table.num_rows != row["rows"]:
            raise ValueError("secondary source row count changed")
        if observed != _identity(source):
            raise ValueError("secondary source changed during read")
        frames.append(table)
    secondary = pa.concat_tables(frames, promote_options="default") if frames else pa.table({})
    original = pq.read_table(primary)
    adjacent_owned_count = 0
    if secondary.num_rows:
        # The primary archive owns a shared ID's UTC date as well as its clock.
        # In particular CryptoHFT +1 ms at midnight must not duplicate D's last
        # trade as a new secondary-only trade in D+1.
        local_ids = pd.Index(original["id"].to_numpy())
        secondary_ids = pd.Series(secondary["trade_id"].to_numpy())
        excluded = pd.Series(False, index=secondary_ids.index)
        for neighbor in neighbors:
            if neighbor.is_file():
                context_table = pq.read_table(neighbor, columns=["id", "symbol", "exchange"])
                if (not pc.all(pc.equal(context_table["symbol"], symbol)).as_py()
                        or not pc.all(pc.equal(context_table["exchange"], "binance-futures")).as_py()
                        or context_table["symbol"].null_count or context_table["exchange"].null_count):
                    raise ValueError("adjacent primary market identity mismatch")
                neighbor_ids = pd.Index(context_table["id"].to_numpy())
                if not local_ids.intersection(neighbor_ids).empty:
                    raise ValueError("primary native trade ID occurs on adjacent dates")
                excluded |= secondary_ids.isin(neighbor_ids)
        adjacent_owned_count = int(secondary_ids[excluded].nunique())
        secondary = secondary.filter(pa.array(~excluded))
    union, stats = union_trades(original, secondary, day, symbol=symbol)
    stats["adjacent_primary_ids_retained_on_owner_day"] = adjacent_owned_count
    aggregate = build_aggregates(union, day)
    future_fill = int(np.count_nonzero(aggregate["last_event_ts_ms"].to_numpy()
                                     >= aggregate["feature_ready_ts_ms"].to_numpy()))
    if future_fill:
        raise ValueError("aggregate feature readiness precedes completed input")
    outputs = {}
    for name, table, schema in [("trades.parquet", union, TRADE_SCHEMA),
                                 ("trade_aggregates_100ms.parquet", aggregate, AGGREGATE_SCHEMA)]:
        target = output_dir / name
        temporary = target.with_suffix(".partial")
        metadata = dict(table.schema.metadata or {})
        metadata.update({b"narrowgate.source_kind": b"individual_trade_union",
                         b"narrowgate.primary_sha256": primary_sha.encode(),
                         b"narrowgate.day": day.encode(),
                         b"narrowgate.symbol": symbol.encode()})
        batches = (pa.Table.from_batches([batch]) for batch in table.to_batches(max_chunksize=131072))
        _, digest = _write_verified_tables(batches, temporary, schema, metadata)
        _publish_atomic(temporary, target)
        outputs[name] = digest
    if before != _identity(primary):
        raise ValueError("primary changed during preparation")
    if any(_identity(Path(path)) != value for path, value in context_before.items()):
        raise ValueError("adjacent primary changed during preparation")
    result = dict(schema=AGGREGATION_SCHEMA, day=day, symbol=symbol, status="PREPARED_VERIFIED",
                  content_changed=stats["content_changed"],
                  canonical_replacement_required=stats["content_changed"],
                  primary_sha256=primary_sha, adjacent_primary_sha256=context,
                  sources=[list(v) for v in identity],
                  output_sha256=outputs, stats=stats, aggregate_rows=aggregate.num_rows,
                  future_fill_violations=future_fill, native_aggtrade_identity=False,
                  economic_admission=False)
    _write_json(receipt, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--symbol", choices=["BTCUSDC", "BTCUSDT"], default="BTCUSDC",
                        help="exact perpetual symbol; reference data never substitutes for execution data")
    parser.add_argument("--source-manifest", type=Path, required=True,
                        help="JSONL individual-trade sources with path, sha256, day, channel")
    parser.add_argument("--start-day", required=True)
    parser.add_argument("--end-day", required=True)
    parser.add_argument("--output-root", type=Path, required=True,
                        help="staging only; publication/deletion is a separate verified cutover")
    args = parser.parse_args()
    rows = [json.loads(line) for line in args.source_manifest.read_text().splitlines() if line]
    rows = [row for row in rows if row.get("channel") == "trades"
            and row.get("symbol", args.symbol) == args.symbol]
    current, final = date.fromisoformat(args.start_day), date.fromisoformat(args.end_day)
    while current <= final:
        day = current.isoformat()
        # Receive-day boundaries do not define exchange-day ownership. Read D-1
        # and D+1 context and let exchange clocks/native IDs perform the union.
        neighbors = {(current + timedelta(days=n)).isoformat() for n in (-1, 0, 1)}
        sources = [row for row in rows if row["day"] in neighbors]
        result = prepare_day(args.raw_root / "binance_futures" / args.symbol / day / "trades.parquet",
                             sources, day, args.output_root / day, symbol=args.symbol)
        print(json.dumps(dict(day=day, symbol=args.symbol, status=result["status"],
                              added_rows=result["stats"]["added_rows"],
                              aggregate_rows=result["aggregate_rows"])), flush=True)
        current += timedelta(days=1)


if __name__ == "__main__":
    main()
