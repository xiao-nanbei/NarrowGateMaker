"""Batch quote-context refresh helpers for offline audit and quote-EV labels."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import backtest_tick as bt  # noqa: E402
from models.data_windows import load_tick_window_dict  # noqa: E402
from models.tick_ab import base_params as _base_params  # noqa: E402
from strategy import quote_core as qc  # noqa: E402


COMMON_CPP_COLUMNS = [
    "raw_half_spread",
    "capped_half_spread",
    "raw_mid_shift",
    "raw_reservation_shift",
    "raw_asym_shift",
    "asym",
    "fair",
    "raw_quote_skew",
    "near_depth_total",
    "book_imb",
    "microprice_shift_bps",
    "kappa_before_depth",
    "kappa_used",
    "depth_tox_mult",
    "cap_bps",
    "max_spread",
    "cap_hit",
    "delta_cap",
    "final_compressed",
    "mid_guard",
    "post_only",
]

SIDE_CPP_COLUMNS = [
    "raw_price",
    "pre_guard_price",
    "final_price",
    "final_quote_delta_to_bbo",
    "final_distance_to_mid",
    "final_quote_skew",
    "spread_mult",
    "defense_spread_mult",
    "side_adverse",
    "side_adverse_pause",
    "adverse_toxicity",
    "adverse_markout",
    "adverse_direction",
    "adverse_ret",
    "adverse_microprice",
    "adverse_thin_depth",
    "defense_guard",
    "defense_pause",
    "defense_reducing",
    "defense_emergency",
    "mid_guard",
    "post_only",
]

CPP_QUOTE_CONTEXT_COLUMNS = [
    "cpp_quote_context_available",
    "cpp_quote_price",
    "cpp_price_diff",
    "cpp_final_price_diff",
    "cpp_final_quote_delta_to_bbo_diff",
    "cpp_raw_half_spread_diff",
    "cpp_near_depth_total_diff",
    "cpp_book_imb_diff",
    "cpp_microprice_shift_bps_diff",
    *[f"cpp_{name}" for name in COMMON_CPP_COLUMNS],
    *[f"cpp_side_{name}" for name in SIDE_CPP_COLUMNS],
]


def _to_epoch_ms(series: pd.Series) -> np.ndarray:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return numeric.to_numpy(dtype=np.float64, copy=False)
    parsed = pd.to_datetime(series, utc=True, errors="coerce")
    values = parsed.astype("int64").to_numpy(dtype=np.float64, copy=False) / 1_000_000.0
    values[parsed.isna().to_numpy()] = np.nan
    return values


def _numeric(frame: pd.DataFrame, column: str, default: float, n: int) -> np.ndarray:
    if column not in frame:
        return np.full(n, default, dtype=np.float64)
    values = pd.to_numeric(frame[column], errors="coerce").fillna(default).to_numpy(dtype=np.float64, copy=False)
    return np.ascontiguousarray(values, dtype=np.float64)


def _asof_values(
    ts_grid: Any,
    values: Any,
    ts: np.ndarray,
    *,
    default: float = 0.0,
) -> np.ndarray:
    out = np.full(len(ts), default, dtype=np.float64)
    if ts_grid is None or values is None:
        return out
    grid = np.asarray(ts_grid, dtype=np.int64)
    vals = np.asarray(values, dtype=np.float64)
    if grid.size == 0 or vals.shape[0] == 0:
        return out
    idx = np.searchsorted(grid, ts.astype(np.int64, copy=False), side="right") - 1
    valid = (idx >= 0) & np.isfinite(ts)
    if np.any(valid):
        out[valid] = vals[idx[valid]]
    return np.nan_to_num(out, nan=default, posinf=default, neginf=default)


def _asof_matrix(
    ts_grid: Any,
    values: Any,
    ts: np.ndarray,
) -> np.ndarray:
    if ts_grid is None or values is None:
        return np.zeros((len(ts), 0), dtype=np.float64)
    grid = np.asarray(ts_grid, dtype=np.int64)
    vals = np.asarray(values, dtype=np.float64)
    if grid.size == 0 or vals.ndim != 2 or vals.shape[0] == 0:
        return np.zeros((len(ts), 0), dtype=np.float64)
    out = np.zeros((len(ts), vals.shape[1]), dtype=np.float64)
    idx = np.searchsorted(grid, ts.astype(np.int64, copy=False), side="right") - 1
    valid = (idx >= 0) & np.isfinite(ts)
    if np.any(valid):
        out[valid] = vals[idx[valid]]
    return np.ascontiguousarray(np.nan_to_num(out, nan=0.0, posinf=0.0, neginf=0.0), dtype=np.float64)


def _window_params(symbol: str, params: dict[str, Any] | None) -> dict[str, Any]:
    if params is None:
        return _base_params(symbol)
    out = dict(params)
    # 中文说明：quote context batch 常被下游 label/audit 脚本复用。传入
    # live-style base params 时应继承 resolved_model_dir，而不是裸配置回默认
    # models/saved_btcusdc，否则同一个 day 的 quote context 会悄悄换 bundle。
    model_dir_override = out.get("resolved_model_dir") or out.get("model_dir_override") or out.get("model_dir")
    if model_dir_override:
        model_path = Path(str(model_dir_override)).expanduser()
        if not model_path.is_absolute():
            model_path = ROOT / model_path
        model_dir_override = str(model_path.resolve())
    bt.configure_symbol(symbol, model_dir_override=model_dir_override)
    return out


def _load_window(load_key: str, params: dict[str, Any], cache_dir: str | None, refresh_cache: bool) -> dict[str, Any]:
    return load_tick_window_dict(
        load_key,
        params,
        load_ml=True,
        require_ml=False,
        cross_market_enabled=True,
        require_historical_bbo=False,
        cache_dir=cache_dir or params.get("_window_cache_dir"),
        refresh_cache=refresh_cache or bool(params.get("_refresh_window_cache", False)),
    )


def _side_arrays(out: dict[str, np.ndarray], side: pd.Series, base_name: str) -> np.ndarray:
    side_upper = side.astype(str).str.upper().to_numpy(dtype=object)
    bid = np.asarray(out[f"bid_{base_name}"])
    ask = np.asarray(out[f"ask_{base_name}"])
    return np.where(side_upper == "BUY", bid, ask)


def _add_diffs(enriched: pd.DataFrame) -> None:
    comparisons = [
        ("price", "cpp_quote_price", "cpp_price_diff"),
        ("final_price", "cpp_side_final_price", "cpp_final_price_diff"),
        ("final_quote_delta_to_bbo", "cpp_side_final_quote_delta_to_bbo", "cpp_final_quote_delta_to_bbo_diff"),
        ("raw_half_spread", "cpp_raw_half_spread", "cpp_raw_half_spread_diff"),
        ("near_depth_total", "cpp_near_depth_total", "cpp_near_depth_total_diff"),
        ("book_imb", "cpp_book_imb", "cpp_book_imb_diff"),
        ("microprice_shift_bps", "cpp_microprice_shift_bps", "cpp_microprice_shift_bps_diff"),
    ]
    for left, right, diff in comparisons:
        if left in enriched and right in enriched:
            enriched[diff] = (
                pd.to_numeric(enriched[right], errors="coerce").fillna(0.0)
                - pd.to_numeric(enriched[left], errors="coerce").fillna(0.0)
            )


def _fill_missing_context_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "cpp_quote_context_available" not in out:
        out["cpp_quote_context_available"] = False
    for col in CPP_QUOTE_CONTEXT_COLUMNS:
        if col not in out:
            out[col] = False if col == "cpp_quote_context_available" else 0.0
    return out


def enrich_orders_with_cpp_quote_context(
    orders: pd.DataFrame,
    *,
    symbol: str,
    days: list[str] | None = None,
    params: dict[str, Any] | None = None,
    cache_dir: str | None = None,
    refresh_cache: bool = False,
    strict: bool = False,
    workers: int = 1,
) -> pd.DataFrame:
    """Attach C++ batch quote-core context to quote trace orders.

    Existing trace columns are preserved.  New columns use the `cpp_` prefix and
    include side-selected fields so downstream label files can audit the quote
    context that would be computed from the same ML/variance/L2 inputs.
    """

    if orders.empty:
        return _fill_missing_context_columns(orders)

    work = orders.copy()
    if "day" not in work:
        raise ValueError("quote-context batch requires day-labelled quote traces")
    if days is None:
        days = sorted(work["day"].astype(str).unique().tolist())
    params = _window_params(symbol, params)
    use_depth = not bool(params.get("use_bar_pricing", True))
    cfg = qc.quote_core_config_from_params(
        params,
        tick_size=bt.TICK,
        lot_size=bt.LOT_SIZE,
        use_ml=True,
        use_depth_microprice=use_depth,
        use_depth_kappa=use_depth,
    )
    work["cpp_quote_context_available"] = False

    for day in days:
        period = str(day)
        # quote context 必须加载同一个 UTC 日的窗口。历史版本这里用 period[:7]
        # 回退到月 key，会让 daily-only replay/labels 重新混入跨日状态或直接找错窗口。
        load_key = period
        day_mask = work["day"].astype(str).eq(period)
        if not day_mask.any():
            continue
        day_orders = work.loc[day_mask].copy()
        quote_ts = _to_epoch_ms(day_orders.get("quote_ts", day_orders.get("submit_ts")))
        try:
            window = _load_window(load_key, params, cache_dir, refresh_cache)
        except Exception:
            if strict:
                raise
            continue

        var_ts = window.get("var_ts_ms")
        sigma_sq = _asof_values(var_ts, window.get("var_ssq"), quote_ts, default=1e-6)
        trade_intensity = _asof_values(var_ts, window.get("var_ti"), quote_ts, default=0.0)

        pred_dir = _numeric(day_orders, "pred_dir", 0.5, len(day_orders))
        pred_vol = np.zeros(len(day_orders), dtype=np.float64)
        pred_ret = _numeric(day_orders, "pred_ret", 0.0, len(day_orders))
        tox_bid = _numeric(day_orders, "tox_bid", 0.5, len(day_orders))
        tox_ask = _numeric(day_orders, "tox_ask", 0.5, len(day_orders))
        ml_data = window.get("ml_data")
        if ml_data is not None and len(ml_data) >= 4:
            ml_ts, ml_dir, ml_vol, ml_ret = ml_data[:4]
            pred_dir = _asof_values(ml_ts, ml_dir, quote_ts, default=0.5)
            pred_vol = _asof_values(ml_ts, ml_vol, quote_ts, default=0.0)
            pred_ret = _asof_values(ml_ts, ml_ret, quote_ts, default=0.0)
            if len(ml_data) >= 6:
                tox_bid = _asof_values(ml_ts, ml_data[4], quote_ts, default=0.5)
                tox_ask = _asof_values(ml_ts, ml_data[5], quote_ts, default=0.5)

        l2 = window.get("l2_data")
        bbo = window.get("bbo_data")
        if l2 is not None:
            l2_bid_px = _asof_matrix(l2.ts_ms, l2.bid_px, quote_ts)
            l2_bid_qty = _asof_matrix(l2.ts_ms, l2.bid_qty, quote_ts)
            l2_ask_px = _asof_matrix(l2.ts_ms, l2.ask_px, quote_ts)
            l2_ask_qty = _asof_matrix(l2.ts_ms, l2.ask_qty, quote_ts)
            best_bid_from_book = l2_bid_px[:, 0] if l2_bid_px.shape[1] else np.zeros(len(day_orders))
            best_ask_from_book = l2_ask_px[:, 0] if l2_ask_px.shape[1] else np.zeros(len(day_orders))
        elif bbo is not None:
            best_bid_from_book = _asof_values(bbo.ts_ms, bbo.best_bid, quote_ts, default=0.0)
            best_ask_from_book = _asof_values(bbo.ts_ms, bbo.best_ask, quote_ts, default=0.0)
            l2_bid_px = best_bid_from_book.reshape(-1, 1)
            l2_ask_px = best_ask_from_book.reshape(-1, 1)
            l2_bid_qty = _asof_values(bbo.ts_ms, bbo.bid_qty, quote_ts, default=0.0).reshape(-1, 1)
            l2_ask_qty = _asof_values(bbo.ts_ms, bbo.ask_qty, quote_ts, default=0.0).reshape(-1, 1)
        else:
            best_bid_from_book = np.zeros(len(day_orders), dtype=np.float64)
            best_ask_from_book = np.zeros(len(day_orders), dtype=np.float64)
            l2_bid_px = l2_bid_qty = l2_ask_px = l2_ask_qty = np.zeros((len(day_orders), 0), dtype=np.float64)

        best_bid = _numeric(day_orders, "best_bid", 0.0, len(day_orders))
        best_ask = _numeric(day_orders, "best_ask", 0.0, len(day_orders))
        best_bid = np.where(best_bid > 0.0, best_bid, best_bid_from_book)
        best_ask = np.where(best_ask > best_bid, best_ask, best_ask_from_book)
        mid = _numeric(day_orders, "mid", 0.0, len(day_orders))
        book_mid = 0.5 * (best_bid + best_ask)
        mid = np.where(mid > 0.0, mid, book_mid)

        mo_bid = _numeric(day_orders, "mo_ema_bid", 0.0, len(day_orders))
        mo_ask = _numeric(day_orders, "mo_ema_ask", 0.0, len(day_orders))
        try:
            out = qc.compute_quote_core_batch_depth_cpp(
                mid=mid,
                inventory=_numeric(day_orders, "inventory", 0.0, len(day_orders)),
                sigma_sq=sigma_sq,
                trade_intensity=trade_intensity,
                best_bid=best_bid,
                best_ask=best_ask,
                dir_10s=pred_dir,
                vol_10s=pred_vol,
                ret_10s=pred_ret,
                tox_bid=tox_bid,
                tox_ask=tox_ask,
                mo_ema_bid=mo_bid,
                mo_ema_ask=mo_ask,
                mo_ema_all=0.5 * (mo_bid + mo_ask),
                mo_ref=np.full(len(day_orders), 50.0, dtype=np.float64),
                position_open=np.abs(_numeric(day_orders, "inventory", 0.0, len(day_orders))) > 0.0,
                l2_bid_px=l2_bid_px,
                l2_bid_qty=l2_bid_qty,
                l2_ask_px=l2_ask_px,
                l2_ask_qty=l2_ask_qty,
                cfg=cfg,
                strict=strict,
                workers=max(1, int(workers)),
            )
        except Exception:
            if strict:
                raise
            continue

        idx = day_orders.index
        side = day_orders["side"] if "side" in day_orders else pd.Series("BUY", index=day_orders.index)
        work.loc[idx, "cpp_quote_context_available"] = True
        work.loc[idx, "cpp_quote_price"] = _side_arrays(out, side, "price")
        for name in COMMON_CPP_COLUMNS:
            if name in out:
                work.loc[idx, f"cpp_{name}"] = out[name]
        for name in SIDE_CPP_COLUMNS:
            work.loc[idx, f"cpp_side_{name}"] = _side_arrays(out, side, name)

    work = _fill_missing_context_columns(work)
    _add_diffs(work)
    return work


def merge_cpp_order_context_to_fills(
    fills: pd.DataFrame,
    orders: pd.DataFrame,
) -> pd.DataFrame:
    if fills.empty or orders.empty:
        return fills
    cpp_cols = [col for col in orders.columns if col.startswith("cpp_")]
    if not cpp_cols:
        return fills
    key_cols = [col for col in ("day", "order_id", "side") if col in fills.columns and col in orders.columns]
    if "order_id" not in key_cols:
        return fills
    right = orders[key_cols + cpp_cols].drop_duplicates(key_cols, keep="last")
    merged = fills.merge(right, on=key_cols, how="left", suffixes=("", "_order"))
    if "cpp_quote_context_available" in merged:
        merged["cpp_quote_context_available"] = merged["cpp_quote_context_available"].fillna(False).astype(bool)
    return merged
