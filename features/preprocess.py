"""
Step 2a: 将原始日度 trades/aggTrades CSV 聚合为 1秒 K线 bar，存为 parquet。

输入: data/raw/BTCUSDT-aggTrades-*.csv 或 data/raw_spot/BTCUSDT-trades-*.csv
输出: data/bars_1s/BTCUSDT-1s-YYYY-MM-DD.parquet  (按 UTC 日)

每个1秒bar包含:
  - open, high, low, close (OHLC)
  - volume (总成交量)
  - buy_volume, sell_volume (买卖方向成交量)
  - trade_count, buy_count, sell_count (成交笔数)
  - vwap (成交量加权均价)

用法:
    python features/preprocess.py                # 处理全部
    python features/preprocess.py --file 2026-03-01 # 只处理匹配的文件
"""

import argparse
import hashlib
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_fusion import (  # noqa: E402
    PERP_MARKET,
    SPOT_MARKET,
    market_bars_dir,
    market_raw_dir,
    normalize_symbol,
)
from data_paths import daily_market_path, raw_data_root  # noqa: E402
from data.daily_raw import is_tardis_trade_archive, iter_tardis_trade_frames, market_day_from_path  # noqa: E402

DEFAULT_SYMBOL = normalize_symbol(os.environ.get("MM_SYMBOL"), "BTCUSDC")

AGG_TRADE_COLUMNS = [
    "agg_trade_id", "price", "quantity",
    "first_trade_id", "last_trade_id",
    "transact_time", "is_buyer_maker"
]
SPOT_AGG_TRADE_COLUMNS = AGG_TRADE_COLUMNS + ["is_best_match"]
AGG_TRADE_DTYPES = {
    "agg_trade_id": np.int64,
    "price": np.float64,
    "quantity": np.float64,
    "first_trade_id": np.int64,
    "last_trade_id": np.int64,
    "transact_time": np.int64,
    "is_buyer_maker": str,
}
SPOT_AGG_TRADE_DTYPES = {**AGG_TRADE_DTYPES, "is_best_match": str}
TRADE_COLUMNS = [
    "agg_trade_id", "price", "quantity",
    "quote_quantity", "transact_time",
    "is_buyer_maker",
]
TRADE_DTYPES = {
    "agg_trade_id": np.int64,
    "price": np.float64,
    "quantity": np.float64,
    "quote_quantity": np.float64,
    "transact_time": np.int64,
    "is_buyer_maker": str,
}
SPOT_TRADE_COLUMNS = TRADE_COLUMNS + ["is_best_match"]
SPOT_TRADE_DTYPES = {
    **TRADE_DTYPES,
    "is_best_match": str,
}
CHUNK_SIZE = int(os.environ.get("MM_PREPROCESS_CHUNK_SIZE", "3000000"))
BAR_METADATA_SCHEMA = "binance_individual_trade_bar_1s.v1"
BAR_BUILDER_VERSION = 4


def iter_public_input_bars(input_stream):
    """Current contract: consume ready Bars, never re-parse or forward-fill.

    The stream is shared with replay and live adapters. This entry deliberately
    does not call the historical sparse/dense Bar conversion functions below.
    """
    for tick in input_stream:
        yield from tick.bars


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_bar_metadata(
    out_path: Path,
    *,
    csv_path: Path,
    symbol: str,
    date_tag: str,
    data_type: str,
    rows: int,
    source_rows: int,
    source_sha256: str,
) -> None:
    metadata = {
        "schema_version": BAR_METADATA_SCHEMA,
        "complete": True,
        "utc_day": date_tag,
        "symbol": symbol,
        "source_data_type": data_type,
        "source_path": str(csv_path.resolve()),
        "source_size_bytes": csv_path.stat().st_size,
        "source_sha256": source_sha256,
        "builder_version": BAR_BUILDER_VERSION,
        "trade_count_unit": ("source_trade_event" if is_tardis_trade_archive(csv_path) else
                             "individual_execution" if data_type == "trades" else "native_aggregate_packet"),
        "market_clock": "source_timestamp_mapping_unverified" if is_tardis_trade_archive(csv_path) else "exchange",
        "provider_timestamp_used": False,
        "rows": int(rows),
        "source_rows": int(source_rows),
        "output_sha256": _sha256(out_path),
        "bar_interval": "[t,t+1s)",
        "causal_visible_at": None if is_tardis_trade_archive(csv_path) else "t+1s",
        "bar_role": "offline_event_time_audit_not_strategy_visible",
        "built_at": datetime.now(timezone.utc).isoformat(),
    }
    meta_path = out_path.with_suffix(out_path.suffix + ".meta.json")
    temp = meta_path.with_suffix(meta_path.suffix + ".tmp")
    temp.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temp, meta_path)


def _bar_source_matches(out_path: Path, source: Path, data_type: str,
                        source_sha256: str) -> bool:
    """A file existing is not proof it was built from today's selected tape."""
    metadata_path = out_path.with_suffix(out_path.suffix + ".meta.json")
    if not out_path.is_file() or not metadata_path.is_file():
        return False
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return (
        isinstance(metadata, dict)
        and metadata.get("complete") is True
        and metadata.get("builder_version") == BAR_BUILDER_VERSION
        and metadata.get("source_data_type") == data_type
        and metadata.get("source_path") == str(source.resolve())
        and metadata.get("source_sha256") == source_sha256
        and metadata.get("output_sha256") == _sha256(out_path)
    )


def _schema_for_data_type(data_type: str, csv_path: Optional[Path] = None):
    header = 0
    first_fields = []
    if csv_path is not None:
        first_line = csv_path.open("r").readline().strip()
        first_fields = first_line.split(",") if first_line else []
        if first_fields and first_fields[0].isdigit():
            header = None
        if data_type == "aggTrades" and len(first_fields) == 8:
            return SPOT_AGG_TRADE_COLUMNS, SPOT_AGG_TRADE_DTYPES, header
    if data_type == "trades" and len(first_fields) == 7:
        return SPOT_TRADE_COLUMNS, SPOT_TRADE_DTYPES, header
    if data_type == "trades":
        return TRADE_COLUMNS, TRADE_DTYPES, header
    return AGG_TRADE_COLUMNS, AGG_TRADE_DTYPES, header


def floor_trade_time_to_second_ms(transact_time: pd.Series) -> pd.Series:
    ts = transact_time.astype(np.int64)
    seconds = ts // 1_000
    seconds = seconds.where(ts < 100_000_000_000_000, ts // 1_000_000)
    seconds = seconds.where(ts < 100_000_000_000_000_000, ts // 1_000_000_000)
    return seconds * 1000


def aggregate_to_1s_bars(chunk: pd.DataFrame) -> pd.DataFrame:
    """将一个chunk的aggTrades聚合为1秒bar"""
    # Binance daily raw files通常按成交时间递增；这里不额外排序是为了避免大文件
    # 预处理成本翻倍。若 raw audit 发现非单调 trade id/time，应先修数据再进这里。
    # is_buyer_maker: "true" 表示买方是maker → 这笔是卖方主动成交(sell)
    # "false" 表示卖方是maker → 这笔是买方主动成交(buy)
    chunk["is_buyer_maker"] = chunk["is_buyer_maker"].str.strip().str.lower() == "true"
    chunk["is_buy"] = ~chunk["is_buyer_maker"]
    chunk["turnover"] = chunk["price"] * chunk["quantity"]
    chunk["buy_vol"] = chunk["quantity"].where(chunk["is_buy"], 0.0)
    chunk["sell_vol"] = chunk["quantity"].where(chunk["is_buyer_maker"], 0.0)

    chunk["ts_sec"] = floor_trade_time_to_second_ms(chunk["transact_time"])

    grouped = chunk.groupby("ts_sec", sort=True)
    bars = pd.DataFrame({
        "open": grouped["price"].first(),
        "high": grouped["price"].max(),
        "low": grouped["price"].min(),
        "close": grouped["price"].last(),
        "volume": grouped["quantity"].sum(),
        "turnover": grouped["turnover"].sum(),
        "buy_volume": grouped["buy_vol"].sum(),
        "sell_volume": grouped["sell_vol"].sum(),
        "trade_count": grouped["agg_trade_id"].count(),
        "buy_count": grouped["is_buy"].sum(),
        "sell_count": grouped["is_buyer_maker"].sum(),
        "last_event_ts_ms": grouped["transact_time"].max(),
    })
    bars["vwap"] = bars["turnover"] / bars["volume"]
    bars.index.name = "timestamp"
    return bars


def process_file(
    csv_path: Path,
    symbol: str,
    out_dir: Path,
    *,
    data_type: str = "aggTrades",
    verbose: bool = False,
) -> tuple[Path, str, int, int]:
    """Build bars from real ordered executions or an explicitly selected legacy tape."""
    if data_type not in {"trades", "aggTrades"}:
        raise ValueError("1s bars require individual trades or explicit native aggTrades")
    if csv_path.suffix == ".parquet":
        names = set(pq.ParquetFile(csv_path).schema_arrow.names)
        if names & {"group_id", "bucket_start_ms", "feature_ready_ts_ms"}:
            raise ValueError("self-aggregated flow features are not raw executions; build 1s bars from trades")
    date_tag = market_day_from_path(csv_path)
    purchased = is_tardis_trade_archive(csv_path)
    if purchased and data_type != "trades":
        raise ValueError("Tardis trades are source events, not native aggTrades")

    out_path = out_dir / f"{symbol}-1s-{date_tag}.parquet"
    source_stat = csv_path.stat()
    source_sha256 = _sha256(csv_path)
    if _bar_source_matches(out_path, csv_path, data_type, source_sha256):
        if verbose:
            print(f"[SKIP] {out_path.name} 已存在")
        return out_path, "skip", 0, 0

    if verbose:
        print(f"[...] 处理 {csv_path.name} ({csv_path.stat().st_size / 1e9:.1f} GB)")

    all_bars = []
    total_rows = 0

    if purchased:
        def chunks():
            for frame in iter_tardis_trade_frames(csv_path, symbol=symbol, chunk_size=CHUNK_SIZE):
                frame = frame.rename(columns={"id": "agg_trade_id", "qty": "quantity", "time": "transact_time"})
                frame["is_buyer_maker"] = frame["is_buyer_maker"].astype(str)
                yield frame
        reader = chunks()
    elif csv_path.suffix == ".parquet":
        def chunks():
            for batch in pq.ParquetFile(csv_path).iter_batches(batch_size=CHUNK_SIZE):
                frame = batch.to_pandas()
                if data_type == "trades":
                    frame = frame.rename(columns={"id": "agg_trade_id", "qty": "quantity",
                                                  "quote_qty": "quote_quantity"})
                if "timestamp" in frame:
                    frame["transact_time"] = frame["timestamp"].astype(np.int64) // 1000
                # Spot archive timestamps retain their original unit (often
                # microseconds); the same unit normalizer handles CSV/Parquet.
                elif "transact_time" not in frame:
                    raise ValueError(f"{csv_path.name}: missing exchange trade timestamp")
                _, dtypes, _ = _schema_for_data_type(data_type)
                yield frame.astype({key: value for key, value in dtypes.items() if key in frame})
        reader = chunks()
    else:
        columns, dtypes, header = _schema_for_data_type(data_type, csv_path)
        reader = pd.read_csv(csv_path, names=columns, dtype=dtypes, header=header, chunksize=CHUNK_SIZE)
    for i, chunk in enumerate(reader):
        total_rows += len(chunk)
        bars = aggregate_to_1s_bars(chunk)
        all_bars.append(bars)

        if verbose and (i + 1) % 10 == 0:
            print(f"  已处理 {total_rows / 1e6:.1f}M 行...")

    # 合并所有chunk的bars，同一秒可能跨chunk，需要再次聚合
    if not all_bars:
        raise ValueError("Empty trade archive is not a confirmed zero-trade day")
    combined = pd.concat(all_bars)

    # 处理跨chunk的同一秒
    if combined.index.duplicated().any():
        combined = combined.groupby(level=0).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "turnover": "sum",
            "buy_volume": "sum",
            "sell_volume": "sum",
            "trade_count": "sum",
            "buy_count": "sum",
            "sell_count": "sum",
            "last_event_ts_ms": "max",
            "vwap": "first",  # placeholder, recomputed below
        })
        # Recompute VWAP accurately from turnover
        combined["vwap"] = combined["turnover"] / combined["volume"]

    # Drop turnover (no longer needed, not in downstream features)
    combined.drop(columns=["turnover"], inplace=True)

    combined.sort_index(inplace=True)

    final_source_stat = csv_path.stat()
    if (source_stat.st_size, source_stat.st_mtime_ns, source_stat.st_ino) != (
        final_source_stat.st_size, final_source_stat.st_mtime_ns, final_source_stat.st_ino
    ):
        raise RuntimeError(f"{csv_path.name}: source changed while building bars")

    # parquet index 是 UTC epoch-ms floor 到秒后的整数；feature_engineer 会统一转 UTC。
    temp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    combined.to_parquet(temp_path, engine="pyarrow")
    os.replace(temp_path, out_path)
    n_bars = len(combined)
    _write_bar_metadata(
        out_path,
        csv_path=csv_path,
        symbol=symbol,
        date_tag=date_tag,
        data_type=data_type,
        rows=n_bars,
        source_rows=total_rows,
        source_sha256=source_sha256,
    )
    size_mb = out_path.stat().st_size / 1e6
    if verbose:
          print(f"[OK]  {out_path.name}: {n_bars:,} bars, {size_mb:.1f} MB "
              f"(从 {total_rows:,} 笔{data_type})")
    return out_path, "ok", n_bars, total_rows


def _is_daily_trade_file(path: Path, symbol: str) -> bool:
    if is_tardis_trade_archive(path):
        return path.name.startswith(f"{symbol}.csv")
    if path.suffix == ".parquet" and path.stem in {"trades", "aggTrades"}:
        return len(path.parent.name) == 10
    stem = path.stem
    for data_type in ("aggTrades", "trades"):
        prefix = f"{symbol}-{data_type}-"
        if stem.startswith(prefix):
            return len(stem.removeprefix(prefix)) == 10
    return False


def _select_daily_inputs(paths: list[Path], *, prefer_individual: bool) -> list[Path]:
    """One output per date: individual executions take priority in auto mode."""
    selected: dict[str, Path] = {}
    for path in sorted(paths):
        day = market_day_from_path(path)
        individual = is_tardis_trade_archive(path) or path.stem == "trades" or "-trades-" in path.name
        rank = (individual if prefer_individual else False, path.suffix == ".parquet")
        current = selected.get(day)
        if current is None:
            selected[day] = path
            continue
        current_individual = is_tardis_trade_archive(current) or current.stem == "trades" or "-trades-" in current.name
        current_rank = (current_individual if prefer_individual else False, current.suffix == ".parquet")
        if rank > current_rank:
            selected[day] = path
    return [selected[day] for day in sorted(selected)]


def main():
    parser = argparse.ArgumentParser(description="aggTrades → 1秒 bars")
    parser.add_argument("--data-plan", type=Path, help="Common-input observation plan; no legacy source fallback")
    parser.add_argument("--symbol", type=str, default=DEFAULT_SYMBOL,
                        help=f"交易对 (默认 {DEFAULT_SYMBOL}; 也可用 MM_SYMBOL 覆盖)")
    parser.add_argument("--market-type", choices=[PERP_MARKET, SPOT_MARKET],
                        default=PERP_MARKET,
                        help="数据来源类型: perp=永续, spot=现货")
    parser.add_argument("--data-type", choices=["auto", "aggTrades", "trades"], default="auto",
                        help="原始CSV类型: auto 根据文件名识别 (默认), aggTrades, trades")
    parser.add_argument("--file", type=str, default=None,
                        help="只处理文件名包含此 UTC 日度字符串的CSV (e.g. '2026-03-01')")
    parser.add_argument("--input-dir", type=Path, default=None,
                        help="可选 raw CSV 目录；默认使用项目 raw/raw_spot")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="可选 1s parquet 目录；默认使用项目 bars_1s/bars_1s_spot")
    parser.add_argument("--cleanup-input", action="store_true",
                        help="成功生成或确认 parquet 后删除对应 raw CSV")
    parser.add_argument("--workers", type=int, default=1,
                        help="按 UTC 日并行的进程数；默认 1")
    parser.add_argument("--verbose", action="store_true",
                        help="逐文件输出处理进度")
    args = parser.parse_args()
    if args.data_plan is not None:
        if args.output_dir is None or args.input_dir is not None or args.cleanup_input or args.file:
            parser.error("--data-plan requires --output-dir and cannot mix legacy inputs/cleanup")
        from data.runtime import derive_inputs
        import json
        result = derive_inputs(json.loads(args.data_plan.read_text()), args.output_dir)
        print(json.dumps({"rows": {k: v["rows"] for k, v in result["files"].items()}, "economic_replay": "not_run"}))
        return
    from data_paths import tardis_raw_root, tardis_market_path
    purchased_root = tardis_raw_root() if args.input_dir is None and args.market_type == PERP_MARKET else None
    if purchased_root is not None and (args.cleanup_input or args.data_type == "aggTrades"):
        raise SystemExit("Purchased Tardis originals are retained; use trades/auto without cleanup-input")
    if args.file and (len(args.file) != 10 or "/" in args.file):
        raise SystemExit(f"--file must be an explicit UTC daily tag YYYY-MM-DD: {args.file}")
    symbol = normalize_symbol(args.symbol)
    raw_dir = args.input_dir or market_raw_dir(ROOT, args.market_type)
    out_dir = args.output_dir or market_bars_dir(ROOT, args.market_type)

    out_dir.mkdir(parents=True, exist_ok=True)

    if args.data_type == "auto":
        csv_files = sorted(raw_dir.glob(f"{symbol}-aggTrades-*.csv")) + sorted(
            raw_dir.glob(f"{symbol}-trades-*.csv")
        )
    else:
        csv_files = sorted(raw_dir.glob(f"{symbol}-{args.data_type}-*.csv"))
    if purchased_root is not None:
        days = ({args.file} if args.file else {market_day_from_path(path) for path in
            (purchased_root / "binance-futures/trades").glob(f"*/*/*/{symbol}.csv*")})
        csv_files = [tardis_market_path(day, symbol, "trades", root=purchased_root) for day in sorted(days)]
    elif args.input_dir is None:
        canonical_root = (
            daily_market_path("2000-01-01", symbol, "trades").parent.parent
            if args.market_type == PERP_MARKET
            else raw_data_root() / "binance_spot" / symbol
        )
        kinds = ("aggTrades", "trades") if args.data_type == "auto" else (args.data_type,)
        canonical = [path for kind in kinds for path in canonical_root.glob(f"*/{kind}.parquet")]
        replaced = {(path.parent.name, path.stem) for path in canonical}
        csv_files = [path for path in csv_files if (path.stem[-10:],
                     "trades" if "-trades-" in path.name else "aggTrades") not in replaced] + sorted(canonical)
    if not csv_files:
        print(
            "错误：所选输入不存在；请按 data 输入合同提供已验证的真实文件，不自动下载或回退来源。"
        )
        sys.exit(1)

    if args.file:
        csv_files = [f for f in csv_files if market_day_from_path(f) == args.file]
    before = len(csv_files)
    csv_files = [f for f in csv_files if _is_daily_trade_file(f, symbol)]
    skipped_non_daily = before - len(csv_files)
    if skipped_non_daily:
        print(f"跳过 {skipped_non_daily} 个非日度 raw CSV；只生成日度 bars")
    csv_files = _select_daily_inputs(csv_files, prefer_individual=args.data_type == "auto")

    print(f"市场: {args.market_type}  交易对: {symbol}")
    print(f"输入目录: {raw_dir}")
    print(f"输出目录: {out_dir}")
    print(f"共 {len(csv_files)} 个CSV文件待处理\n")

    ok = skip = bars_written = rows_read = 0

    def accept_result(csv_path, result):
        nonlocal ok, skip, bars_written, rows_read
        out_path, status, n_bars, n_rows = result
        if (args.cleanup_input and csv_path.suffix != ".parquet" and not is_tardis_trade_archive(csv_path)
                and status in {"ok", "skip"} and out_path.exists()):
            csv_path.unlink()
        ok += status == "ok"
        skip += status == "skip"
        bars_written += n_bars
        rows_read += n_rows

    workers = max(1, int(args.workers))
    if workers == 1:
        for csv_path in csv_files:
            data_type = "trades" if is_tardis_trade_archive(csv_path) or "-trades-" in csv_path.name or csv_path.stem == "trades" else "aggTrades"
            accept_result(
                csv_path,
                process_file(
                    csv_path,
                    symbol,
                    out_dir,
                    data_type=data_type,
                    verbose=args.verbose,
                ),
            )
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            pending = {
                executor.submit(
                    process_file,
                    csv_path,
                    symbol,
                    out_dir,
                    data_type=("trades" if is_tardis_trade_archive(csv_path) or "-trades-" in csv_path.name or csv_path.stem == "trades" else "aggTrades"),
                    verbose=args.verbose,
                ): csv_path
                for csv_path in csv_files
            }
            for future in as_completed(pending):
                accept_result(pending[future], future.result())

    # 汇总
    parquet_files = sorted(out_dir.glob(f"{symbol}-1s-*.parquet"))
    total_size = sum(f.stat().st_size for f in parquet_files) / 1e9
    print(
        f"\n完成！新增 {ok} 个, 跳过 {skip} 个, "
        f"本次 {bars_written:,} bars / {rows_read:,} trades；"
        f"累计 {len(parquet_files)} 个parquet, {total_size:.2f} GB"
    )


if __name__ == "__main__":
    main()
