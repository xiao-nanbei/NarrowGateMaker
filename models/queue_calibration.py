#!/usr/bin/env python3
"""Daily queue calibration from live quote and order outcome logs.

这份脚本只负责把 live 日志转成 replay 可消费的“队列校准表”。
注意：observed_* 字段只是诊断统计，默认不要直接拿 live 的 1% fill
rate 去做二次伯努利门控，否则 replay fills 会被压低一个数量级。
真正会影响 replay 的是显式开启后的 queue/deplete multiplier。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root  # noqa: E402
from market_fusion import normalize_symbol  # noqa: E402

DEFAULT_QUOTE_LOG = ROOT / "logs" / "quote_decisions.csv"
DEFAULT_OUTCOME_LOG = ROOT / "logs" / "order_outcomes.csv"
DEFAULT_DISTANCE_EDGES = (20.0, 30.0, 45.0)
DEFAULT_RANK_EDGES = (0.25, 0.50, 0.75)
DEFAULT_MO_EDGES = (-2.0, -1.0, 0.0, 1.0, 2.0)
QUEUE_SCHEMA_VERSION = "narrowgate_queue_calibration.v3"

REPLAY_QUEUE_PARAM_DEFAULTS = {
    "queue_ahead_base_mult": 1.0,
    "queue_deplete_base_mult": 1.0,
    "queue_ahead_buy_exposure_mult": 1.0,
    "queue_ahead_buy_reducing_mult": 1.0,
    "queue_ahead_sell_exposure_mult": 1.0,
    "queue_ahead_sell_reducing_mult": 1.0,
}


def calibration_dir(root: Optional[Path] = None) -> Path:
    repo_root = Path(root).resolve() if root else ROOT
    path = data_root(repo_root) / "queue_calibration"
    path.mkdir(parents=True, exist_ok=True)
    return path


def calibration_path(symbol: Optional[str] = None, root: Optional[Path] = None) -> Path:
    override = os.getenv("MM_QUEUE_CALIBRATION_PATH", "").strip()
    if override and root is None:
        return Path(override).expanduser().resolve()
    sym = normalize_symbol(symbol)
    return calibration_dir(root) / f"{sym}-daily-queue-calibration.json"


def load_daily_queue_calibration(
    symbol: Optional[str] = None,
    path: Optional[str | Path] = None,
    root: Optional[Path] = None,
) -> dict:
    target = Path(path) if path else calibration_path(symbol=symbol, root=root)
    if not target.exists():
        return {}
    with open(target, "r") as f:
        return json.load(f)


def build_daily_queue_arrays(
    ts_ms: np.ndarray,
    calibration: dict,
    *,
    default_queue_base: float,
    default_queue_decay: float,
    default_buy_fill_prob: float,
    default_sell_fill_prob: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    # 日级 base 数组保持向后兼容；当前校准文件会把 queue_base/decay 写成默认值，
    # observed_queue_* 只用于审计，避免 live 日志反推出来的小队列直接污染默认 replay。
    n = len(ts_ms)
    queue_base = np.full(n, float(default_queue_base), dtype=np.float64)
    queue_decay = np.full(n, float(default_queue_decay), dtype=np.float64)
    buy_fill_prob = np.full(n, float(default_buy_fill_prob), dtype=np.float64)
    sell_fill_prob = np.full(n, float(default_sell_fill_prob), dtype=np.float64)
    days = calibration.get("days") if calibration else None
    if not days:
        return queue_base, queue_decay, buy_fill_prob, sell_fill_prob

    day_tags = pd.to_datetime(ts_ms, unit="ms", utc=True).strftime("%Y-%m-%d")
    for tag, payload in days.items():
        mask = day_tags == tag
        if not np.any(mask):
            continue
        queue_base[mask] = float(payload.get("queue_base", default_queue_base))
        queue_decay[mask] = float(payload.get("queue_decay", default_queue_decay))
        buy_fill_prob[mask] = float(payload.get("buy_fill_prob", default_buy_fill_prob))
        sell_fill_prob[mask] = float(payload.get("sell_fill_prob", default_sell_fill_prob))
    return queue_base, queue_decay, buy_fill_prob, sell_fill_prob


def reason_bucket_from_flags(
    *,
    adverse: bool = False,
    markout: bool = False,
    thin_depth: bool = False,
) -> str:
    if adverse:
        return "adverse"
    if markout and thin_depth:
        return "markout_thin"
    if markout:
        return "markout"
    if thin_depth:
        return "thin_depth"
    return "none"


def _reason_bucket_from_text(value: object) -> str:
    text = str(value or "").lower()
    return reason_bucket_from_flags(
        adverse="adverse" in text,
        markout="markout" in text,
        thin_depth="thin_depth" in text or "thin-depth" in text,
    )


def _quantile_edges(values: pd.Series, quantiles: tuple[float, ...], fallback: tuple[float, ...]) -> list[float]:
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if arr.size < 24:
        return [float(x) for x in fallback]
    raw = np.quantile(arr, quantiles)
    edges: list[float] = []
    for x in raw:
        if not np.isfinite(x):
            continue
        fx = float(x)
        if not edges or fx > edges[-1] + 1e-9:
            edges.append(fx)
    return edges or [float(x) for x in fallback]


def _bin_index(value: object, edges: list[float]) -> int:
    try:
        x = float(value)
    except Exception:
        return 0
    if not np.isfinite(x):
        return 0
    return int(np.searchsorted(np.asarray(edges, dtype=np.float64), x, side="right"))


def _regime_key(side: str, dist_bin: int, rank_bin: int, reason_bucket: str) -> str:
    return f"{str(side).upper()}|{int(dist_bin)}|{int(rank_bin)}|{str(reason_bucket)}"


def _read_trade_series(path: Optional[Path]) -> tuple[np.ndarray, np.ndarray]:
    if path is None:
        return np.asarray([], dtype=np.int64), np.asarray([], dtype=np.float64)
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() == ".parquet":
        trades = pd.read_parquet(path, columns=["transact_time", "price"])
    else:
        trades = pd.read_csv(path, usecols=["transact_time", "price"])
    ts = pd.to_numeric(trades["transact_time"], errors="coerce").to_numpy(dtype=np.float64)
    px = pd.to_numeric(trades["price"], errors="coerce").to_numpy(dtype=np.float64)
    valid = np.isfinite(ts) & np.isfinite(px)
    ts_i = ts[valid].astype(np.int64)
    px_f = px[valid].astype(np.float64)
    order = np.argsort(ts_i, kind="mergesort")
    return ts_i[order], px_f[order]


def _local_rank_at(ts_ms: int, trade_ts: np.ndarray, trade_px: np.ndarray, window_ms: int = 120_000) -> float:
    # 因果 local rank：只看 ts_ms 之前的价格窗口。OOS queue calibration 必须用这个口径，
    # 不能用 trace 里前后窗口的诊断 rank，否则会把未来价格信息混进 fill selection。
    if trade_ts.size == 0:
        return 0.5
    left = int(np.searchsorted(trade_ts, int(ts_ms) - int(window_ms), side="left"))
    right = int(np.searchsorted(trade_ts, int(ts_ms), side="right"))
    if right <= left:
        return 0.5
    window = trade_px[left:right]
    if window.size == 0:
        return 0.5
    lo = float(np.nanmin(window))
    hi = float(np.nanmax(window))
    if not np.isfinite(lo) or not np.isfinite(hi) or hi - lo <= 1e-9:
        return 0.5
    cur = float(window[-1])
    return float(max(0.0, min(1.0, (cur - lo) / (hi - lo))))


def _build_regime_payload(
    day_df: pd.DataFrame,
    *,
    buy_fill_rate: float,
    sell_fill_rate: float,
    smoothing: float,
    queue_mult_min: float,
    queue_mult_max: float,
) -> dict:
    # quote-time regime multiplier：在订单刚 open 时调整 fallback queue 厚薄。
    # 这是“哪些订单更容易排到队”的校准；它不应该被用来修正 fill 后 markout。
    dist_edges = _quantile_edges(day_df["dist_usd"], (0.25, 0.50, 0.75), DEFAULT_DISTANCE_EDGES)
    rank_edges = [float(x) for x in DEFAULT_RANK_EDGES]
    frame = day_df.copy()
    frame["dist_bin"] = [_bin_index(x, dist_edges) for x in frame["dist_usd"]]
    frame["rank_bin"] = [_bin_index(x, rank_edges) for x in frame["window120_rank"]]
    frame["reason_bucket"] = frame["reason_text"].map(_reason_bucket_from_text)

    side_base = {
        "BUY": max(float(buy_fill_rate), 1e-9),
        "SELL": max(float(sell_fill_rate), 1e-9),
    }
    groups: list[dict] = []

    def add_groups(group_cols: list[str], *, wildcard_reason: bool) -> None:
        for values, group in frame.groupby(group_cols, observed=True, sort=True):
            if not isinstance(values, tuple):
                values = (values,)
            row = dict(zip(group_cols, values))
            side = str(row.get("side", "")).upper()
            if side not in side_base:
                continue
            n_orders = int(len(group))
            n_fills = int(group["filled_any"].sum())
            base_rate = side_base[side]
            smoothed_rate = (n_fills + float(smoothing) * base_rate) / max(n_orders + float(smoothing), 1e-9)
            fill_mult = smoothed_rate / base_rate if base_rate > 0 else 1.0
            queue_mult = base_rate / max(smoothed_rate, 1e-9)
            queue_mult = float(np.clip(queue_mult, queue_mult_min, queue_mult_max))
            dist_bin = int(row.get("dist_bin", 0))
            rank_bin = int(row.get("rank_bin", 0))
            reason = "*" if wildcard_reason else str(row.get("reason_bucket", "none"))
            groups.append({
                "key": _regime_key(side, dist_bin, rank_bin, reason),
                "side": side,
                "dist_bin": dist_bin,
                "rank_bin": rank_bin,
                "reason_bucket": reason,
                "orders": n_orders,
                "fills": n_fills,
                "observed_fill_rate": float(n_fills / max(n_orders, 1)),
                "smoothed_fill_rate": float(smoothed_rate),
                "fill_mult": float(fill_mult),
                "queue_mult": queue_mult,
            })

    add_groups(["side", "dist_bin", "rank_bin"], wildcard_reason=True)
    add_groups(["side", "dist_bin", "rank_bin", "reason_bucket"], wildcard_reason=False)
    return {
        "distance_edges": dist_edges,
        "rank_edges": rank_edges,
        "smoothing": float(smoothing),
        "queue_mult_min": float(queue_mult_min),
        "queue_mult_max": float(queue_mult_max),
        "groups": groups,
    }


def build_queue_regime_lookup(calibration: dict) -> dict:
    days = calibration.get("days") if calibration else None
    if not days:
        return {}
    out = {}
    for day, payload in days.items():
        regime = payload.get("regime") or {}
        groups = {
            str(row.get("key")): float(row.get("queue_mult", 1.0))
            for row in regime.get("groups", [])
            if row.get("key") is not None
        }
        if groups:
            out[str(day)] = {
                "distance_edges": [float(x) for x in regime.get("distance_edges", DEFAULT_DISTANCE_EDGES)],
                "rank_edges": [float(x) for x in regime.get("rank_edges", DEFAULT_RANK_EDGES)],
                "groups": groups,
            }
    if out and "__default__" not in out:
        # OOS 验证需要“用 fit 窗口学到的表应用到后续日期”。
        # 因此目标 day 没有自己的 calibration 时，回退到最早的 fit day。
        out["__default__"] = out[sorted(k for k in out if k != "__default__")[0]]
    return out


def _build_deplete_rank_payload(
    day: str,
    outcome_df: pd.DataFrame,
    *,
    trade_ts: np.ndarray,
    trade_px: np.ndarray,
    replay_fill_trace_path: Optional[Path],
    smoothing: float,
    mult_min: float,
    mult_max: float,
) -> dict:
    # fill-time depletion multiplier：在 taker trade 消耗队列时缩放有效成交量。
    # 目标是让 BUY/SELL 在不同 causal local-rank bucket 的 fill 分布更像 live，
    # 而不是只用 maker_fill_prob 把总 fills 粗暴压到接近。
    if replay_fill_trace_path is None or not replay_fill_trace_path.exists() or trade_ts.size == 0:
        return {}
    live_fills = outcome_df.loc[
        outcome_df["event_type"].isin(("fill", "filled"))
        & (outcome_df["timestamp"].dt.strftime("%Y-%m-%d") == day)
    ].copy()
    if live_fills.empty:
        return {}
    replay = pd.read_csv(replay_fill_trace_path)
    if replay.empty or "side" not in replay.columns or "window120_rank" not in replay.columns:
        return {}

    live_fills["rank_bin"] = [
        _bin_index(
            _local_rank_at(int(ts.value // 1_000_000), trade_ts, trade_px),
            list(DEFAULT_RANK_EDGES),
        )
        for ts in live_fills["timestamp"]
    ]
    if "fill_ts" in replay.columns:
        replay_ts = pd.to_numeric(replay["fill_ts"], errors="coerce").fillna(0).to_numpy(dtype=np.int64)
        replay_ranks = [_local_rank_at(int(ts), trade_ts, trade_px) for ts in replay_ts]
    else:
        replay_ranks = pd.to_numeric(replay["window120_rank"], errors="coerce").fillna(0.5).to_list()
    replay["rank_bin"] = [
        _bin_index(x, list(DEFAULT_RANK_EDGES))
        for x in replay_ranks
    ]

    groups: list[dict] = []
    for side in ("BUY", "SELL"):
        live_side = live_fills.loc[live_fills["side"].astype(str).str.upper() == side]
        replay_side = replay.loc[replay["side"].astype(str).str.upper() == side]
        live_counts = live_side["rank_bin"].value_counts().to_dict()
        replay_counts = replay_side["rank_bin"].value_counts().to_dict()
        live_total = float(len(live_side))
        replay_total = float(len(replay_side))
        if live_total <= 0.0 or replay_total <= 0.0:
            continue
        n_bins = len(DEFAULT_RANK_EDGES) + 1
        for rank_bin in range(n_bins):
            live_share = (float(live_counts.get(rank_bin, 0)) + smoothing) / (live_total + smoothing * n_bins)
            replay_share = (float(replay_counts.get(rank_bin, 0)) + smoothing) / (replay_total + smoothing * n_bins)
            mult = float(np.clip(live_share / max(replay_share, 1e-9), mult_min, mult_max))
            groups.append({
                "key": f"{side}|{rank_bin}",
                "side": side,
                "rank_bin": int(rank_bin),
                "live_fills": int(live_counts.get(rank_bin, 0)),
                "replay_fills": int(replay_counts.get(rank_bin, 0)),
                "live_share": float(live_share),
                "replay_share": float(replay_share),
                "deplete_mult": mult,
            })
    return {
        "rank_edges": [float(x) for x in DEFAULT_RANK_EDGES],
        "smoothing": float(smoothing),
        "mult_min": float(mult_min),
        "mult_max": float(mult_max),
        "source_replay_fill_trace": str(replay_fill_trace_path),
        "groups": groups,
    }


def build_queue_deplete_lookup(calibration: dict) -> dict:
    days = calibration.get("days") if calibration else None
    if not days:
        return {}
    out = {}
    for day, payload in days.items():
        deplete = payload.get("deplete_rank") or {}
        groups = {
            str(row.get("key")): float(row.get("deplete_mult", 1.0))
            for row in deplete.get("groups", [])
            if row.get("key") is not None
        }
        if groups:
            out[str(day)] = {
                "rank_edges": [float(x) for x in deplete.get("rank_edges", DEFAULT_RANK_EDGES)],
                "groups": groups,
            }
    if out and "__default__" not in out:
        out["__default__"] = out[sorted(k for k in out if k != "__default__")[0]]
    return out


def _build_mo_queue_payload(
    day: str,
    outcome_df: pd.DataFrame,
    *,
    replay_fill_trace_path: Optional[Path],
    smoothing: float,
    mult_min: float,
    mult_max: float,
) -> dict:
    # side-specific markout EMA multiplier：实验性校准，用于检查某些 mo_ema bucket
    # 是否被 replay 过度填到。当前 2026-06-26 首 6h 里它会明显压低总 fills，
    # 所以默认生成文件可以把 min=max=1.0 作为 no-op，只保留代码能力。
    if replay_fill_trace_path is None or not replay_fill_trace_path.exists():
        return {}
    live_fills = outcome_df.loc[
        outcome_df["event_type"].isin(("fill", "filled"))
        & (outcome_df["timestamp"].dt.strftime("%Y-%m-%d") == day)
    ].copy()
    replay = pd.read_csv(replay_fill_trace_path)
    if live_fills.empty or replay.empty:
        return {}
    groups: list[dict] = []
    n_bins = len(DEFAULT_MO_EDGES) + 1
    for side in ("BUY", "SELL"):
        live_side = live_fills.loc[live_fills["side"].astype(str).str.upper() == side]
        replay_side = replay.loc[replay["side"].astype(str).str.upper() == side].copy()
        if live_side.empty or replay_side.empty:
            continue
        replay_mo_col = "mo_ema_bid" if side == "BUY" else "mo_ema_ask"
        if replay_mo_col not in replay_side.columns:
            continue
        live_bins = [
            _bin_index(x, list(DEFAULT_MO_EDGES))
            for x in pd.to_numeric(live_side["markout_ema"], errors="coerce").fillna(0.0)
        ]
        replay_bins = [
            _bin_index(x, list(DEFAULT_MO_EDGES))
            for x in pd.to_numeric(replay_side[replay_mo_col], errors="coerce").fillna(0.0)
        ]
        live_counts = pd.Series(live_bins).value_counts().to_dict()
        replay_counts = pd.Series(replay_bins).value_counts().to_dict()
        live_total = float(len(live_bins))
        replay_total = float(len(replay_bins))
        for mo_bin in range(n_bins):
            live_share = (float(live_counts.get(mo_bin, 0)) + smoothing) / (live_total + smoothing * n_bins)
            replay_share = (float(replay_counts.get(mo_bin, 0)) + smoothing) / (replay_total + smoothing * n_bins)
            queue_mult = replay_share / max(live_share, 1e-9)
            queue_mult = float(np.clip(queue_mult, mult_min, mult_max))
            groups.append({
                "key": f"{side}|{mo_bin}",
                "side": side,
                "mo_bin": int(mo_bin),
                "live_fills": int(live_counts.get(mo_bin, 0)),
                "replay_fills": int(replay_counts.get(mo_bin, 0)),
                "live_share": float(live_share),
                "replay_share": float(replay_share),
                "queue_mult": queue_mult,
            })
    return {
        "mo_edges": [float(x) for x in DEFAULT_MO_EDGES],
        "smoothing": float(smoothing),
        "mult_min": float(mult_min),
        "mult_max": float(mult_max),
        "source_replay_fill_trace": str(replay_fill_trace_path),
        "groups": groups,
    }


def build_queue_mo_lookup(calibration: dict) -> dict:
    days = calibration.get("days") if calibration else None
    if not days:
        return {}
    out = {}
    for day, payload in days.items():
        moq = payload.get("mo_queue") or {}
        groups = {
            str(row.get("key")): float(row.get("queue_mult", 1.0))
            for row in moq.get("groups", [])
            if row.get("key") is not None
        }
        if groups:
            out[str(day)] = {
                "mo_edges": [float(x) for x in moq.get("mo_edges", DEFAULT_MO_EDGES)],
                "groups": groups,
            }
    if out and "__default__" not in out:
        out["__default__"] = out[sorted(k for k in out if k != "__default__")[0]]
    return out


def lookup_queue_mo_multiplier(
    lookup: dict,
    *,
    day: str,
    side: str,
    markout_ema: float,
) -> float:
    payload = lookup.get(str(day)) or lookup.get("__default__")
    if not payload:
        return 1.0
    mo_bin = _bin_index(markout_ema, payload.get("mo_edges", list(DEFAULT_MO_EDGES)))
    return float((payload.get("groups") or {}).get(f"{str(side).upper()}|{mo_bin}", 1.0))


def lookup_queue_deplete_multiplier(
    lookup: dict,
    *,
    day: str,
    side: str,
    local_rank: float,
) -> float:
    payload = lookup.get(str(day)) or lookup.get("__default__")
    if not payload:
        return 1.0
    rank_bin = _bin_index(local_rank, payload.get("rank_edges", list(DEFAULT_RANK_EDGES)))
    return float((payload.get("groups") or {}).get(f"{str(side).upper()}|{rank_bin}", 1.0))


def lookup_queue_regime_multiplier(
    lookup: dict,
    *,
    day: str,
    side: str,
    dist_usd: float,
    near_depth_total: float = 0.0,
    local_rank: float = 0.5,
    reason_bucket: str = "none",
) -> float:
    payload = lookup.get(str(day)) or lookup.get("__default__")
    if not payload:
        return 1.0
    dist_bin = _bin_index(dist_usd, payload.get("distance_edges", list(DEFAULT_DISTANCE_EDGES)))
    rank_bin = _bin_index(local_rank, payload.get("rank_edges", list(DEFAULT_RANK_EDGES)))
    groups = payload.get("groups") or {}
    side = str(side).upper()
    reason_bucket = str(reason_bucket or "none")
    key = _regime_key(side, dist_bin, rank_bin, reason_bucket)
    if key in groups:
        return float(groups[key])
    key = _regime_key(side, dist_bin, rank_bin, "*")
    return float(groups.get(key, 1.0))


def _fit_queue_decay(frame: pd.DataFrame, default: float = 0.1) -> float:
    valid = frame.loc[
        frame["dist_usd"].notna()
        & np.isfinite(frame["dist_usd"])
        & (frame["dist_usd"] > 0)
    ].copy()
    if len(valid) < 24:
        return float(default)

    bins = min(6, max(3, valid["dist_usd"].nunique()))
    try:
        valid["dist_bin"] = pd.qcut(valid["dist_usd"], q=bins, duplicates="drop")
    except Exception:
        return float(default)

    grouped = (
        valid.groupby("dist_bin", observed=True)
        .agg(fill_rate=("filled_any", "mean"), dist_mean=("dist_usd", "mean"))
        .reset_index(drop=True)
    )
    grouped = grouped.loc[(grouped["fill_rate"] > 0.02) & (grouped["fill_rate"] < 0.98)]
    if len(grouped) < 2:
        return float(default)

    x = grouped["dist_mean"].to_numpy(dtype=np.float64)
    y = np.log(grouped["fill_rate"].to_numpy(dtype=np.float64))
    slope = np.polyfit(x, y, 1)[0]
    decay = max(0.0, -float(slope))
    if not np.isfinite(decay) or decay <= 0.0:
        return float(default)
    return float(min(decay, 5.0))


def _estimate_queue_base(frame: pd.DataFrame, fill_prob: float, default: float = 5.0) -> float:
    near_depth = frame["l2_near_depth_total"].replace(0, np.nan).dropna()
    if near_depth.empty:
        return float(default)
    median_depth = float(np.median(near_depth.to_numpy(dtype=np.float64)))
    scarcity = max(0.1, 1.0 - float(fill_prob))
    queue_base = median_depth * scarcity * 0.5
    queue_base = max(0.5, min(queue_base, max(50.0, median_depth * 2.0)))
    return float(queue_base)


def _read_log(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    if "timestamp" not in df.columns:
        raise ValueError(f"{path} missing timestamp column")
    df["timestamp"] = pd.to_datetime(pd.to_numeric(df["timestamp"], errors="coerce"), unit="s", utc=True)
    return df


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def calibrate_daily_queue(
    *,
    symbol: str,
    quote_log_path: Path,
    order_outcome_path: Path,
    output_path: Path,
    trade_data_path: Optional[Path] = None,
    replay_fill_trace_path: Optional[Path] = None,
    default_queue_base: float = 5.0,
    default_queue_decay: float = 0.1,
    default_fill_prob: float = 1.0,
    regime_smoothing: float = 50.0,
    queue_mult_min: float = 0.5,
    queue_mult_max: float = 2.0,
    deplete_smoothing: float = 1.0,
    deplete_mult_min: float = 0.5,
    deplete_mult_max: float = 2.0,
    mo_smoothing: float = 1.0,
    mo_queue_mult_min: float = 0.4,
    mo_queue_mult_max: float = 2.5,
    start_day: Optional[str] = None,
    end_day: Optional[str] = None,
    frozen_default_only: bool = False,
    replay_params: Optional[dict[str, float]] = None,
) -> dict:
    # 输出的是“日度 UTC 校准表”。后续 OOS 验证应使用 fit 窗口之外的 6h/日内窗口，
    # 观察 BUY/SELL fills、fills/hour、age、causal rank、SELL markout，不要先看 PnL。
    symbol = normalize_symbol(symbol)
    quote_df = _read_log(quote_log_path)
    outcome_df = _read_log(order_outcome_path)
    if start_day:
        start_ts = pd.Timestamp(start_day, tz="UTC")
        quote_df = quote_df.loc[quote_df["timestamp"] >= start_ts]
        outcome_df = outcome_df.loc[outcome_df["timestamp"] >= start_ts]
    if end_day:
        end_ts = pd.Timestamp(end_day, tz="UTC") + pd.Timedelta(days=1)
        quote_df = quote_df.loc[quote_df["timestamp"] < end_ts]
        outcome_df = outcome_df.loc[outcome_df["timestamp"] < end_ts]
    trade_ts, trade_px = _read_trade_series(trade_data_path)

    quote_df = quote_df.loc[quote_df.get("symbol", "").astype(str).str.upper() == symbol].copy()
    outcome_df = outcome_df.loc[outcome_df.get("symbol", "").astype(str).str.upper() == symbol].copy()
    if quote_df.empty or outcome_df.empty:
        raise ValueError(f"No log rows for {symbol} in {quote_log_path.name} / {order_outcome_path.name}")

    placements = outcome_df.loc[outcome_df["event_type"].isin(["placed", "placed_close"])].copy()
    if placements.empty:
        raise ValueError(f"No placed orders found for {symbol}")

    fill_event_types = ("fill", "filled")
    decision_counts = (
        quote_df.groupby([quote_df["timestamp"].dt.strftime("%Y-%m-%d"), "side", "action"])
        .size()
        .rename("count")
        .reset_index()
    )

    order_records = []
    for cid, group in outcome_df.groupby("client_order_id", sort=False):
        placed = group.loc[group["event_type"].isin(["placed", "placed_close"])].sort_values("timestamp")
        if placed.empty:
            continue
        placed_row = placed.iloc[0]
        fills_group = group.loc[group["event_type"].isin(fill_event_types)].sort_values("timestamp")
        terminal_row = group.sort_values("timestamp").iloc[-1]
        day = placed_row["timestamp"].strftime("%Y-%m-%d")
        ts_ms = int(placed_row["timestamp"].value // 1_000_000)
        order_records.append({
            "day": day,
            "timestamp_ms": ts_ms,
            "side": str(placed_row["side"]).upper(),
            "filled_any": float(len(fills_group) > 0),
            "first_fill_age_ms": float(fills_group["age_ms"].min()) if not fills_group.empty else np.nan,
            "terminal_age_ms": float(terminal_row.get("age_ms", np.nan)),
            "dist_usd": abs(float(placed_row.get("target_price", 0.0)) - float(placed_row.get("mid", 0.0))),
            "mid": float(placed_row.get("mid", 0.0)),
            "target_price": float(placed_row.get("target_price", 0.0)),
            "target_qty": float(placed_row.get("target_qty", 0.0)),
            "l2_near_depth_total": float(placed_row.get("l2_near_depth_total", 0.0)),
            "mode": str(placed_row.get("mode", "na")),
            "reason_text": str(placed_row.get("reason_text", "none")),
            "terminal_event": str(terminal_row.get("event_type", "")),
        })

    orders_df = pd.DataFrame(order_records)
    if orders_df.empty:
        raise ValueError(f"No order-level records produced for {symbol}")
    orders_df["window120_rank"] = [
        _local_rank_at(int(ts_ms), trade_ts, trade_px)
        for ts_ms in orders_df["timestamp_ms"].to_numpy(dtype=np.int64)
    ]

    days_out = {}
    for day, day_df in orders_df.groupby("day", sort=True):
        buy_df = day_df.loc[day_df["side"] == "BUY"]
        sell_df = day_df.loc[day_df["side"] == "SELL"]
        observed_buy_fill_prob = float(buy_df["filled_any"].mean()) if not buy_df.empty else float(default_fill_prob)
        observed_sell_fill_prob = float(sell_df["filled_any"].mean()) if not sell_df.empty else float(default_fill_prob)
        overall_fill_prob = float(day_df["filled_any"].mean()) if not day_df.empty else float(default_fill_prob)
        queue_decay = _fit_queue_decay(day_df, default=default_queue_decay)
        queue_base = _estimate_queue_base(day_df, overall_fill_prob, default=default_queue_base)
        regime = _build_regime_payload(
            day_df,
            buy_fill_rate=observed_buy_fill_prob,
            sell_fill_rate=observed_sell_fill_prob,
            smoothing=regime_smoothing,
            queue_mult_min=queue_mult_min,
            queue_mult_max=queue_mult_max,
        )
        deplete_rank = _build_deplete_rank_payload(
            day,
            outcome_df,
            trade_ts=trade_ts,
            trade_px=trade_px,
            replay_fill_trace_path=replay_fill_trace_path,
            smoothing=deplete_smoothing,
            mult_min=deplete_mult_min,
            mult_max=deplete_mult_max,
        )
        mo_queue = _build_mo_queue_payload(
            day,
            outcome_df,
            replay_fill_trace_path=replay_fill_trace_path,
            smoothing=mo_smoothing,
            mult_min=mo_queue_mult_min,
            mult_max=mo_queue_mult_max,
        )

        side_action_counts = {}
        day_decisions = decision_counts.loc[decision_counts["timestamp"] == day]
        for _, row in day_decisions.iterrows():
            side_action_counts.setdefault(str(row["side"]).upper(), {})[str(row["action"])] = int(row["count"])

        days_out[day] = {
            "queue_base": float(default_queue_base),
            "queue_decay": float(default_queue_decay),
            "observed_queue_base": queue_base,
            "observed_queue_decay": queue_decay,
            "buy_fill_prob": max(0.0, min(1.0, float(default_fill_prob))),
            "sell_fill_prob": max(0.0, min(1.0, float(default_fill_prob))),
            "observed_buy_fill_prob": max(0.0, min(1.0, observed_buy_fill_prob)),
            "observed_sell_fill_prob": max(0.0, min(1.0, observed_sell_fill_prob)),
            "placed_orders": int(len(day_df)),
            "filled_orders": int(day_df["filled_any"].sum()),
            "buy_orders": int(len(buy_df)),
            "sell_orders": int(len(sell_df)),
            "median_first_fill_age_ms": float(np.nanmedian(day_df["first_fill_age_ms"])) if day_df["first_fill_age_ms"].notna().any() else None,
            "median_terminal_age_ms": float(np.nanmedian(day_df["terminal_age_ms"])) if day_df["terminal_age_ms"].notna().any() else None,
            "median_dist_usd": float(np.nanmedian(day_df["dist_usd"])) if day_df["dist_usd"].notna().any() else None,
            "median_l2_near_depth_total": float(np.nanmedian(day_df["l2_near_depth_total"])) if day_df["l2_near_depth_total"].notna().any() else None,
            "decision_action_counts": side_action_counts,
            "regime": regime,
            "deplete_rank": deplete_rank,
            "mo_queue": mo_queue,
        }

    pooled_buy = orders_df.loc[orders_df["side"] == "BUY"]
    pooled_sell = orders_df.loc[orders_df["side"] == "SELL"]
    pooled_buy_rate = float(pooled_buy["filled_any"].mean()) if not pooled_buy.empty else float(default_fill_prob)
    pooled_sell_rate = float(pooled_sell["filled_any"].mean()) if not pooled_sell.empty else float(default_fill_prob)
    pooled_fill_rate = float(orders_df["filled_any"].mean())
    pooled_actions: dict[str, dict[str, int]] = {}
    for _, row in decision_counts.groupby(["side", "action"], as_index=False)["count"].sum().iterrows():
        pooled_actions.setdefault(str(row["side"]).upper(), {})[str(row["action"])] = int(row["count"])
    pooled_payload = {
        "queue_base": float(default_queue_base),
        "queue_decay": float(default_queue_decay),
        "observed_queue_base": _estimate_queue_base(orders_df, pooled_fill_rate, default=default_queue_base),
        "observed_queue_decay": _fit_queue_decay(orders_df, default=default_queue_decay),
        "buy_fill_prob": max(0.0, min(1.0, float(default_fill_prob))),
        "sell_fill_prob": max(0.0, min(1.0, float(default_fill_prob))),
        "observed_buy_fill_prob": max(0.0, min(1.0, pooled_buy_rate)),
        "observed_sell_fill_prob": max(0.0, min(1.0, pooled_sell_rate)),
        "placed_orders": int(len(orders_df)),
        "filled_orders": int(orders_df["filled_any"].sum()),
        "buy_orders": int(len(pooled_buy)),
        "sell_orders": int(len(pooled_sell)),
        "median_first_fill_age_ms": float(np.nanmedian(orders_df["first_fill_age_ms"])) if orders_df["first_fill_age_ms"].notna().any() else None,
        "median_terminal_age_ms": float(np.nanmedian(orders_df["terminal_age_ms"])) if orders_df["terminal_age_ms"].notna().any() else None,
        "median_dist_usd": float(np.nanmedian(orders_df["dist_usd"])),
        "median_l2_near_depth_total": float(np.nanmedian(orders_df["l2_near_depth_total"])),
        "decision_action_counts": pooled_actions,
        "regime": _build_regime_payload(
            orders_df,
            buy_fill_rate=pooled_buy_rate,
            sell_fill_rate=pooled_sell_rate,
            smoothing=regime_smoothing,
            queue_mult_min=queue_mult_min,
            queue_mult_max=queue_mult_max,
        ),
        "deplete_rank": {},
        "mo_queue": {},
    }
    fit_days = sorted(days_out)
    replay_days = {"__default__": pooled_payload}
    if not frozen_default_only:
        replay_days.update(days_out)

    resolved_replay_params = dict(REPLAY_QUEUE_PARAM_DEFAULTS)
    if replay_params:
        for key in resolved_replay_params:
            if key in replay_params:
                resolved_replay_params[key] = max(0.0, float(replay_params[key]))

    payload = {
        "schema_version": QUEUE_SCHEMA_VERSION,
        "symbol": symbol,
        "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "quote_log_path": str(quote_log_path),
        "order_outcome_log_path": str(order_outcome_path),
        "trade_data_path": str(trade_data_path) if trade_data_path else None,
        "input_sha256": {
            "quote_log": _sha256(quote_log_path),
            "order_outcome_log": _sha256(order_outcome_path),
            "trade_data": _sha256(trade_data_path) if trade_data_path else None,
        },
        "fit_days": fit_days,
        "apply_mode": "frozen_default" if frozen_default_only else "daily_with_frozen_default",
        "defaults": {
            "queue_base": float(default_queue_base),
            "queue_decay": float(default_queue_decay),
            "fill_prob": float(default_fill_prob),
        },
        # These replay-only scales are fitted against live placed/fill counts.
        # Keeping them in the versioned artifact prevents a runner flag from
        # silently changing the queue identity of the formal baseline.
        "replay_params": resolved_replay_params,
        "days": replay_days,
        "diagnostics_by_day": days_out,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2)
    return payload


def main() -> None:
    ap = argparse.ArgumentParser(description="Daily queue calibration from live logs")
    ap.add_argument("--symbol", required=True, help="Trading symbol, e.g. BTCUSDT or BTCUSDC")
    ap.add_argument("--quote-log", default=str(DEFAULT_QUOTE_LOG), help="Path to quote_decisions.csv")
    ap.add_argument("--order-log", default=str(DEFAULT_OUTCOME_LOG), help="Path to order_outcomes.csv")
    ap.add_argument("--trade-data", default=None,
                    help="Optional aggTrades CSV/parquet used to compute quote-time local-rank buckets")
    ap.add_argument("--replay-fill-trace", default=None,
                    help="Optional baseline replay fills.csv used to fit fill-time queue depletion multipliers")
    ap.add_argument("--output", default=None, help="Optional output path (JSON)")
    ap.add_argument("--default-queue-base", type=float, default=5.0)
    ap.add_argument("--default-queue-decay", type=float, default=0.1)
    ap.add_argument("--default-fill-prob", type=float, default=1.0)
    ap.add_argument("--regime-smoothing", type=float, default=50.0,
                    help="Pseudo-order count for side/regime queue multipliers")
    ap.add_argument("--queue-mult-min", type=float, default=0.5)
    ap.add_argument("--queue-mult-max", type=float, default=2.0)
    ap.add_argument("--deplete-smoothing", type=float, default=1.0)
    ap.add_argument("--deplete-mult-min", type=float, default=0.5)
    ap.add_argument("--deplete-mult-max", type=float, default=2.0)
    ap.add_argument("--mo-smoothing", type=float, default=1.0)
    ap.add_argument("--mo-queue-mult-min", type=float, default=0.4)
    ap.add_argument("--mo-queue-mult-max", type=float, default=2.5)
    ap.add_argument("--start-day", default=None)
    ap.add_argument("--end-day", default=None)
    ap.add_argument("--frozen-default-only", action="store_true")
    for key, default in REPLAY_QUEUE_PARAM_DEFAULTS.items():
        ap.add_argument(
            f"--{key.replace('_', '-')}",
            type=float,
            default=default,
            help=f"Frozen replay queue parameter stored in the artifact (default {default})",
        )
    args = ap.parse_args()

    out = Path(args.output) if args.output else calibration_path(args.symbol)
    payload = calibrate_daily_queue(
        symbol=args.symbol,
        quote_log_path=Path(args.quote_log),
        order_outcome_path=Path(args.order_log),
        output_path=out,
        trade_data_path=Path(args.trade_data) if args.trade_data else None,
        replay_fill_trace_path=Path(args.replay_fill_trace) if args.replay_fill_trace else None,
        default_queue_base=args.default_queue_base,
        default_queue_decay=args.default_queue_decay,
        default_fill_prob=args.default_fill_prob,
        regime_smoothing=args.regime_smoothing,
        queue_mult_min=args.queue_mult_min,
        queue_mult_max=args.queue_mult_max,
        deplete_smoothing=args.deplete_smoothing,
        deplete_mult_min=args.deplete_mult_min,
        deplete_mult_max=args.deplete_mult_max,
        mo_smoothing=args.mo_smoothing,
        mo_queue_mult_min=args.mo_queue_mult_min,
        mo_queue_mult_max=args.mo_queue_mult_max,
        start_day=args.start_day,
        end_day=args.end_day,
        frozen_default_only=args.frozen_default_only,
        replay_params={key: getattr(args, key) for key in REPLAY_QUEUE_PARAM_DEFAULTS},
    )
    print(f"Saved daily queue calibration -> {out}")
    print(f"Days: {', '.join(sorted(payload.get('days', {}).keys())) or 'none'}")


if __name__ == "__main__":
    main()
