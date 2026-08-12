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
) -> None:
    metadata = {
        "schema_version": BAR_METADATA_SCHEMA,
        "complete": True,
        "utc_day": date_tag,
        "symbol": symbol,
        "source_data_type": data_type,
        "source_path": str(csv_path.resolve()),
        "source_size_bytes": csv_path.stat().st_size,
        "rows": int(rows),
        "source_rows": int(source_rows),
        "output_sha256": _sha256(out_path),
        "bar_interval": "[t,t+1s)",
        "causal_visible_at": "t+1s",
        "built_at": datetime.now(timezone.utc).isoformat(),
    }
    meta_path = out_path.with_suffix(out_path.suffix + ".meta.json")
    temp = meta_path.with_suffix(meta_path.suffix + ".tmp")
    temp.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temp, meta_path)


def _schema_for_data_type(data_type: str, csv_path: Optional[Path] = None):
    header = 0
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
    """处理单个CSV文件，输出parquet"""
    # 从文件名提取日期标识
    fname = csv_path.stem  # e.g. BTCUSDT-aggTrades-2026-03-01
    parts = fname.split("-")
    # 日度文件: BTCUSDT-aggTrades-YYYY-MM-DD
    if len(parts) != 5:
        raise ValueError(f"{csv_path.name}: expected daily raw CSV name YYYY-MM-DD")
    date_tag = f"{parts[2]}-{parts[3]}-{parts[4]}"  # YYYY-MM-DD

    out_path = out_dir / f"{symbol}-1s-{date_tag}.parquet"
    if out_path.exists():
        if verbose:
            print(f"[SKIP] {out_path.name} 已存在")
        return out_path, "skip", 0, 0

    if verbose:
        print(f"[...] 处理 {csv_path.name} ({csv_path.stat().st_size / 1e9:.1f} GB)")

    all_bars = []
    total_rows = 0

    columns, dtypes, header = _schema_for_data_type(data_type, csv_path)
    for i, chunk in enumerate(pd.read_csv(
        csv_path,
        names=columns,
        dtype=dtypes,
        header=header,
        chunksize=CHUNK_SIZE,
    )):
        total_rows += len(chunk)
        bars = aggregate_to_1s_bars(chunk)
        all_bars.append(bars)

        if verbose and (i + 1) % 10 == 0:
            print(f"  已处理 {total_rows / 1e6:.1f}M 行...")

    # 合并所有chunk的bars，同一秒可能跨chunk，需要再次聚合
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
    )
    size_mb = out_path.stat().st_size / 1e6
    if verbose:
          print(f"[OK]  {out_path.name}: {n_bars:,} bars, {size_mb:.1f} MB "
              f"(从 {total_rows:,} 笔{data_type})")
    return out_path, "ok", n_bars, total_rows


def _is_daily_trade_file(path: Path, symbol: str) -> bool:
    stem = path.stem
    for data_type in ("aggTrades", "trades"):
        prefix = f"{symbol}-{data_type}-"
        if stem.startswith(prefix):
            return len(stem.removeprefix(prefix)) == 10
    return False


def main():
    parser = argparse.ArgumentParser(description="aggTrades → 1秒 bars")
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
    if not csv_files:
        print(
            "错误：未找到CSV文件，请先运行 "
            f"python pipeline.py download-agg-trades --market-type {args.market_type}"
        )
        sys.exit(1)

    if args.file:
        csv_files = [f for f in csv_files if args.file in f.name]
    before = len(csv_files)
    csv_files = [f for f in csv_files if _is_daily_trade_file(f, symbol)]
    skipped_non_daily = before - len(csv_files)
    if skipped_non_daily:
        print(f"跳过 {skipped_non_daily} 个非日度 raw CSV；只生成日度 bars")

    print(f"市场: {args.market_type}  交易对: {symbol}")
    print(f"输入目录: {raw_dir}")
    print(f"输出目录: {out_dir}")
    print(f"共 {len(csv_files)} 个CSV文件待处理\n")

    ok = skip = bars_written = rows_read = 0

    def accept_result(csv_path, result):
        nonlocal ok, skip, bars_written, rows_read
        out_path, status, n_bars, n_rows = result
        if args.cleanup_input and status in {"ok", "skip"} and out_path.exists():
            csv_path.unlink()
        ok += status == "ok"
        skip += status == "skip"
        bars_written += n_bars
        rows_read += n_rows

    workers = max(1, int(args.workers))
    if workers == 1:
        for csv_path in csv_files:
            data_type = "trades" if "-trades-" in csv_path.name else "aggTrades"
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
                    data_type=("trades" if "-trades-" in csv_path.name else "aggTrades"),
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
