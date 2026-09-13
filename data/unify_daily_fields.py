"""Daily feature-clock normalization and individual-trade reference bars.

Feature values remain unchanged. Reference bars are rebuilt from individuals,
so old native-packet counts are deliberately replaced with execution counts.
Publication is per file and atomic; callers update their selected-file indexes
only after the returned output identity has been recorded.
"""

from __future__ import annotations

import argparse
import fcntl
import gc
import hashlib
import json
import os
import tempfile
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

FEATURE_INDEX_UNIT = "us"
FEATURE_INDEX_TYPE = pa.timestamp(FEATURE_INDEX_UNIT, tz="UTC")
METRICS_INDEX_TYPE = FEATURE_INDEX_TYPE
METRICS_COLUMNS = (
    "sum_open_interest", "sum_open_interest_value", "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio", "count_long_short_ratio", "sum_taker_long_short_vol_ratio",
)
BAR_COLUMNS = (
    "open", "high", "low", "close", "volume", "buy_volume", "sell_volume",
    "trade_count", "buy_count", "sell_count", "last_event_ts_ms", "vwap", "timestamp",
)
DAY_MS = 86_400_000


def canonical_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Fix the storage clock without rounding, changing values or fitting models."""
    if not isinstance(frame.index, pd.DatetimeIndex) or str(frame.index.tz) != "UTC":
        raise ValueError("feature index must be a UTC DatetimeIndex")
    result = frame.copy(deep=False)
    result.index = frame.index.as_unit(FEATURE_INDEX_UNIT, round_ok=False)
    return result


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def _check_output(path: Path) -> None:
    for component in (path, *path.parents):
        # These system aliases are not project data links.
        if component in (Path("/var"), Path("/tmp")):
            continue
        if component.is_symlink():
            raise ValueError(f"output must not use a symbolic link: {component}")


def _same_values(left: pa.Table, right: pa.Table) -> bool:
    if left.schema.remove_metadata() != right.schema.remove_metadata():
        return False
    if left.num_rows != right.num_rows:
        return False
    for a, b in zip(left.columns, right.columns, strict=True):
        if a.equals(b):
            continue
        if not a.is_null().equals(b.is_null()):
            return False
        equal = pc.equal(a, b)
        if pa.types.is_floating(a.type):
            equal = pc.or_(equal, pc.and_(pc.is_nan(a), pc.is_nan(b)))
        if pc.all(pc.fill_null(equal, True)).as_py() is False:
            return False
    return True


def _publish(
    table: pa.Table, output: Path, inputs: dict[Path, str], *, dry_run: bool,
    reuse_verified_output: bool = False,
) -> dict:
    output = output.absolute()
    _check_output(output)
    result = {
        "output": str(output), "rows": table.num_rows,
        "columns": [[field.name, str(field.type)] for field in table.schema],
        "inputs": [{"path": str(path), "sha256": digest} for path, digest in inputs.items()],
    }
    for path, digest in inputs.items():
        if _digest(path) != digest:
            raise RuntimeError(f"input changed during normalization: {path.name}")
    if dry_run:
        return {**result, "status": "DRY_RUN", "output_sha256": None}
    in_place = any(output == path.absolute() for path in inputs)
    if output.exists() and not in_place:
        if reuse_verified_output:
            existing = pq.read_table(output)
            if _same_values(table, existing) and table.schema.metadata == existing.schema.metadata:
                if any(_digest(path) != digest for path, digest in inputs.items()):
                    raise RuntimeError("input changed during existing-output verification")
                return {**result, "status": "VERIFIED_EXISTING", "output_sha256": _digest(output),
                        "readback_equal": True}
        raise FileExistsError(f"refusing to overwrite an unrelated output: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{uuid.uuid4().hex}.partial")
    try:
        pq.write_table(table, temporary, compression="zstd")
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        actual = pq.read_table(temporary)
        if not _same_values(table, actual) or table.schema.metadata != actual.schema.metadata:
            raise RuntimeError("Parquet readback changed values or metadata")
        output_sha = _digest(temporary)
        for path, digest in inputs.items():
            if _digest(path) != digest:
                raise RuntimeError(f"input changed before publication: {path.name}")
        if output.exists() and not in_place:
            raise FileExistsError(f"output appeared before publication: {output}")
        os.replace(temporary, output)
        directory = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return {**result, "status": "VERIFIED", "output_sha256": output_sha,
                "readback_equal": True}
    finally:
        temporary.unlink(missing_ok=True)


def _unify_utc_index_day(
    source: Path, output: Path, *, dry_run: bool = False, metrics: bool = False,
) -> dict:
    source, output = Path(source), Path(output)
    source_sha = _digest(source)
    table = pq.read_table(source)
    metadata = dict(table.schema.metadata or {})
    pandas_metadata = json.loads(metadata.get(b"pandas", b"{}"))
    indexes = pandas_metadata.get("index_columns", [])
    if len(indexes) != 1 or not isinstance(indexes[0], str):
        raise ValueError("features require one physically stored timestamp index")
    name = indexes[0]
    if metrics and (name != "create_time" or table.column_names != [*METRICS_COLUMNS, name]
                    or any(table.schema.field(column).type != pa.float64() for column in METRICS_COLUMNS)):
        raise ValueError("metrics require six ordered double columns and the create_time index")
    position = table.schema.get_field_index(name)
    if position < 0:
        raise ValueError("feature timestamp index is not stored")
    before = table.column(position)
    if not pa.types.is_timestamp(before.type) or before.type.tz != "UTC":
        raise ValueError("feature index must have UTC timestamp type")
    after = before.cast(FEATURE_INDEX_TYPE, safe=True)
    if not after.cast(before.type, safe=True).equals(before):
        raise ValueError("feature clock conversion would lose precision")
    field = table.schema.field(position).with_type(FEATURE_INDEX_TYPE)
    target = table.set_column(position, field, after)
    for column in pandas_metadata.get("columns", []):
        if column.get("field_name") == name:
            column["numpy_type"] = "datetime64[us]"
            column["pandas_type"] = "datetimetz"
            column["metadata"] = {"timezone": "UTC"}
    metadata[b"pandas"] = json.dumps(pandas_metadata).encode()
    target = target.replace_schema_metadata(metadata)
    if not _same_values(table.drop([name]), target.drop([name])):
        raise RuntimeError("non-clock feature values changed")
    return {**_publish(target, output, {source: source_sha}, dry_run=dry_run),
            "operation": "metrics_index_utc_microseconds" if metrics else "feature_index_utc_microseconds",
            "previous_index_type": str(before.type),
            "new_index_type": str(FEATURE_INDEX_TYPE), "non_clock_values_equal": True}


def unify_feature_day(source: Path, output: Path, *, dry_run: bool = False) -> dict:
    """Normalize a daily feature index to exact UTC microseconds, preserving columns."""
    return _unify_utc_index_day(source, output, dry_run=dry_run)


def unify_metrics_day(source: Path, output: Path, *, dry_run: bool = False) -> dict:
    """Change storage precision only; never shift metric readiness or numerical values."""
    return _unify_utc_index_day(source, output, dry_run=dry_run, metrics=True)


def _individual_last_events(trades: Path, day: str, symbol: str) -> tuple[np.ndarray, np.ndarray]:
    start = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) * 1000
    reader = pq.ParquetFile(trades)
    names = set(reader.schema_arrow.names)
    if not {"timestamp", "id", "symbol", "exchange"} <= names or names & {"agg_trade_id", "group_id"}:
        raise ValueError("last-event repair requires canonical individual trades")
    columns = ["timestamp", "symbol", "exchange"] + (["time"] if "time" in names else [])
    last = np.full(86_400, -1, dtype=np.int64)
    count = np.zeros(86_400, dtype=np.int64)
    previous_us = None
    for batch in reader.iter_batches(batch_size=262_144, columns=columns):
        timestamps = batch.column(batch.schema.get_field_index("timestamp"))
        symbols = batch.column(batch.schema.get_field_index("symbol"))
        if timestamps.null_count or symbols.null_count or not pa.types.is_int64(timestamps.type):
            raise ValueError("unknown or non-integer individual exchange clock/symbol")
        if pc.any(pc.not_equal(symbols, symbol)).as_py():
            raise ValueError("individual-trade symbol mismatch")
        exchange = batch.column(batch.schema.get_field_index("exchange"))
        if exchange.null_count or pc.any(pc.invert(pc.is_in(exchange,
                value_set=pa.array(["binance-futures", "binance_futures"])))).as_py():
            raise ValueError("reference bars require the same Binance futures market")
        ts_us = timestamps.to_numpy(zero_copy_only=False)
        if len(ts_us) and ((previous_us is not None and ts_us[0] < previous_us)
                          or np.any(np.diff(ts_us) < 0)):
            raise ValueError("individual exchange timestamps must be ordered for OHLC")
        if len(ts_us):
            previous_us = ts_us[-1]
        ts_ms = ts_us // 1000
        if "time" in columns:
            alias = batch.column(batch.schema.get_field_index("time"))
            if alias.null_count or not pa.types.is_int64(alias.type):
                raise ValueError("unknown or non-integer individual time alias")
            if not np.array_equal(ts_ms, alias.to_numpy(zero_copy_only=False)):
                raise ValueError("individual exchange time aliases disagree")
        if np.any(ts_ms < start) or np.any(ts_ms >= start + DAY_MS):
            raise ValueError("individual exchange timestamps escape the requested UTC day")
        seconds = (ts_ms - start) // 1000
        np.maximum.at(last, seconds, ts_ms)
        np.add.at(count, seconds, 1)
    return last, count


def unify_reference_bars(
    source: Path, trades: Path, output: Path, *, day: str, symbol: str = "BTCUSDT",
    dry_run: bool = False,
    reuse_verified_output: bool = False,
) -> dict:
    """Rebuild reference OHLC/counts with the maintained individual-trade builder.

No old values are kept merely for numerical agreement. The returned differences
describe a reference-feature input change, not model fitting or economics.
"""
    source, trades, output = Path(source), Path(trades), Path(output)
    if output.resolve() == trades.resolve():
        raise ValueError("bar output must never replace individual-trade input")
    inputs = {source: _digest(source), trades: _digest(trades)}
    table = pq.read_table(source)
    if set(table.column_names) not in (set(BAR_COLUMNS), set(BAR_COLUMNS) - {"last_event_ts_ms"}):
        raise ValueError("unsupported bar columns; refusing to silently drop or invent fields")
    clock = table.column("timestamp")
    if clock.null_count or not pa.types.is_int64(clock.type):
        raise ValueError("bars require known integer UTC epoch-ms bucket timestamps")
    ts = clock.to_numpy()
    start = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) * 1000
    if np.any(ts < start) or np.any(ts >= start + DAY_MS) or np.any(ts % 1000):
        raise ValueError("bar bucket is outside day or not a whole exchange second")
    if len(ts) > 1 and np.any(np.diff(ts) <= 0):
        raise ValueError("bar timestamps must be unique and ordered")
    last, count = _individual_last_events(trades, day, symbol)
    if trades.parent.name != day:
        raise ValueError("canonical individual path must match the requested UTC day")
    if not count.any():
        raise ValueError("no observed individual trades; no empty bars may be invented")
    from features.preprocess import process_file

    # Scratch is bounded to one daily product and removed on success or failure.
    # Even dry-run uses the real builder, but never publishes a selected output.
    with tempfile.TemporaryDirectory(prefix="narrowgate-bar-fields-") as scratch:
        built, _, _, source_rows = process_file(trades, symbol, Path(scratch), data_type="trades")
        target = pq.read_table(built).select(BAR_COLUMNS)
        builder = json.loads(built.with_suffix(".parquet.meta.json").read_text())
    expected_seconds = np.flatnonzero(count)
    new_ts = target.column("timestamp").to_numpy()
    if not np.array_equal(new_ts, start + expected_seconds * 1000):
        raise RuntimeError("bar builder added or omitted observed seconds")
    for name, expected in (("last_event_ts_ms", last[expected_seconds]),
                           ("trade_count", count[expected_seconds])):
        if not np.array_equal(target.column(name).to_numpy(), expected):
            raise RuntimeError(f"bar builder violated individual {name} conservation")
    if source_rows != int(count.sum()) or builder["source_sha256"] != inputs[trades]:
        raise RuntimeError("bar builder source identity or count mismatch")
    metadata = dict(target.schema.metadata or {})
    metadata[b"narrowgate.bar_clock"] = b"exchange_epoch_ms; interval=[t,t+1s); visible_at=t+1s"
    metadata[b"narrowgate.trade_count_unit"] = b"individual_execution"
    metadata[b"narrowgate.individual_source_sha256"] = inputs[trades].encode()
    target = target.replace_schema_metadata(metadata)
    old_frame = table.to_pandas()
    new_frame = target.to_pandas()
    common = old_frame.index.intersection(new_frame.index)
    differences = {}
    for name in old_frame.columns.intersection(new_frame.columns):
        a, b = old_frame.loc[common, name], new_frame.loc[common, name]
        differences[name] = int((~(a.eq(b) | (a.isna() & b.isna()))).sum())
    published = _publish(target, output, inputs, dry_run=dry_run,
                         reuse_verified_output=reuse_verified_output)
    return {**published, "operation": "reference_bars_from_individuals", "day": day, "symbol": symbol,
            "previous_rows": table.num_rows, "individual_rows": int(count.sum()),
            "old_only_seconds": int(len(old_frame.index.difference(new_frame.index))),
            "new_only_seconds": int(len(new_frame.index.difference(old_frame.index))),
            "changed_common_rows_by_field": differences,
            "trade_count_sum": int(count.sum()), "counts_and_last_event_verified": True,
            "dependent_reference_features": "REFRESH_REQUIRED", "labels_generated": False,
            "builder_metadata": {**builder, "output_sha256": published["output_sha256"]},
            "last_event_min_ms": int(last[expected_seconds].min()),
            "last_event_max_ms": int(last[expected_seconds].max())}


def unify_reference_calendar(
    old_bars_dir: Path, raw_symbol_root: Path, output_dir: Path, *,
    start_day: str, end_day: str, symbol: str = "BTCUSDT", dry_run: bool = True,
) -> dict:
    """Sequential bounded-memory ETL with one resumable current progress record."""
    start, end = date.fromisoformat(start_day), date.fromisoformat(end_day)
    if end < start:
        raise ValueError("calendar end precedes start")
    days = [(start + timedelta(days=n)).isoformat() for n in range((end - start).days + 1)]
    old_bars_dir, raw_symbol_root, output_dir = map(Path, (old_bars_dir, raw_symbol_root, output_dir))
    config = {"start_day": start_day, "end_day": end_day, "symbol": symbol,
              "old_bars_dir": str(old_bars_dir.absolute()), "raw_symbol_root": str(raw_symbol_root.absolute()),
              "output_dir": str(output_dir.absolute())}
    if dry_run:
        missing = [day for day in days if not (old_bars_dir / f"{symbol}-1s-{day}.parquet").is_file()
                   or not (raw_symbol_root / day / "trades.parquet").is_file()]
        return {"status": "DRY_RUN", "config": config, "calendar_days": len(days), "missing_days": missing}
    from features import preprocess
    implementation = {"unify_daily_fields": _digest(Path(__file__)),
                      "preprocess": _digest(Path(preprocess.__file__))}
    _check_output(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    progress_path = output_dir / "current.json"
    lock_path = output_dir / ".running.lock"
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            if progress_path.exists():
                progress = json.loads(progress_path.read_text())
                if progress.get("config") != config:
                    raise ValueError("existing calendar progress has different inputs or date range")
                if progress.get("implementation") != implementation:
                    raise ValueError("existing calendar progress has a different implementation")
            else:
                progress = {"schema": "individual_reference_bars_calendar.v1", "config": config,
                            "calendar_days": len(days), "records": {}, "labels_generated": False,
                            "implementation": implementation}
            def save():
                progress["updated_at"] = datetime.now(timezone.utc).isoformat()
                temporary = progress_path.with_suffix(f".json.{uuid.uuid4().hex}.partial")
                try:
                    with temporary.open("x", encoding="utf-8") as handle:
                        json.dump(progress, handle, sort_keys=True)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(temporary, progress_path)
                finally:
                    temporary.unlink(missing_ok=True)
            progress["status"] = "RUNNING"
            save()
            for day in days:
                source = old_bars_dir / f"{symbol}-1s-{day}.parquet"
                trades = raw_symbol_root / day / "trades.parquet"
                output = output_dir / source.name
                previous = progress["records"].get(day)
                try:
                    if implementation != {"unify_daily_fields": _digest(Path(__file__)),
                                          "preprocess": _digest(Path(preprocess.__file__))}:
                        raise RuntimeError("implementation changed during calendar rebuild")
                    if previous and previous.get("status") in {"VERIFIED", "VERIFIED_EXISTING"}:
                        if (_digest(output) != previous["output_sha256"]
                                or any(_digest(Path(item["path"])) != item["sha256"] for item in previous["inputs"])):
                            raise RuntimeError(f"{day}: resume input or output identity changed")
                        continue
                    progress["records"][day] = unify_reference_bars(source, trades, output,
                        day=day, symbol=symbol, reuse_verified_output=True)
                    save()
                    print(f"[REFERENCE_BARS] {day} {len(progress['records'])}/{len(days)}", flush=True)
                    gc.collect()
                except Exception as error:
                    if not previous:
                        progress["records"][day] = {"status": "FAILED", "error": str(error)}
                    progress["failure"] = {"day": day, "error": str(error)}
                    progress["status"] = "FAILED"
                    save()
                    raise
            progress["status"] = "COMPLETED"
            progress.pop("failure", None)
            save()
            return {"status": "COMPLETED", "calendar_days": len(days), "progress_path": str(progress_path)}
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("features", "metrics", "bars", "reference-calendar"))
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--trades", type=Path)
    parser.add_argument("--day")
    parser.add_argument("--start-day")
    parser.add_argument("--end-day")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--apply", action="store_true", help="write verified output; default is dry-run")
    args = parser.parse_args(argv)
    if args.kind == "reference-calendar":
        if args.trades is None or args.start_day is None or args.end_day is None:
            parser.error("reference-calendar requires --trades RAW_SYMBOL_ROOT --start-day --end-day")
        result = unify_reference_calendar(args.input, args.trades, args.output,
            start_day=args.start_day, end_day=args.end_day, symbol=args.symbol, dry_run=not args.apply)
    elif args.kind == "features":
        result = unify_feature_day(args.input, args.output, dry_run=not args.apply)
    elif args.kind == "metrics":
        result = unify_metrics_day(args.input, args.output, dry_run=not args.apply)
    else:
        if args.trades is None or args.day is None:
            parser.error("bars requires --trades and --day")
        result = unify_reference_bars(args.input, args.trades, args.output,
            day=args.day, symbol=args.symbol, dry_run=not args.apply)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
