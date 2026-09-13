"""Causal cancel-request state features with a native acceleration path.

Market observations with the same millisecond as the request are excluded.
This makes the contract conservative when exchange-event ordering inside that
millisecond is unavailable.  The returned state is suitable for both the
pending-fill and conditional ACK-survival heads; neither head may reconstruct
request state from ACK-time information.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "request_state_features.v1"
DEFAULT_WINDOWS_MS = (25, 50, 100, 250, 500, 1_000)

_VECTOR_OUTPUTS = (
    "valid_book",
    "book_source_ts_ms",
    "book_age_ms",
    "best_bid",
    "best_ask",
    "bid_qty",
    "ask_qty",
    "mid",
    "bbo_spread_ticks",
    "book_imbalance",
    "microprice_shift_bps",
    "l2_near_depth_total",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "active_order_count",
    "pending_cancel_before_count",
    "request_batch_size",
)
_MATRIX_OUTPUTS = (
    "market_return_bps",
    "aggressive_buy_qty",
    "aggressive_sell_qty",
    "taker_imbalance",
    "trade_count",
    "book_update_count",
)


def _as_i64(values: Any) -> np.ndarray:
    return np.ascontiguousarray(values, dtype=np.int64)


def _as_f64(values: Any) -> np.ndarray:
    return np.ascontiguousarray(values, dtype=np.float64)


def _as_u8(values: Any) -> np.ndarray:
    return np.ascontiguousarray(values, dtype=np.uint8)


def _require_sorted(values: np.ndarray, name: str) -> None:
    if values.size > 1 and bool(np.any(values[1:] < values[:-1])):
        raise ValueError(f"{name} must be sorted ascending")


def _validate_inputs(inputs: Mapping[str, np.ndarray]) -> None:
    rows = len(inputs["request_ts_ms"])
    for name in (
        "book_cutoff_ts_ms",
        "trade_cutoff_ts_ms",
        "activation_ts_ms",
        "terminal_ts_ms",
        "cancel_ack_ts_ms",
    ):
        if len(inputs[name]) != rows:
            raise ValueError(f"{name} length does not match request_ts_ms")
    for prefix in ("bbo", "trade"):
        timestamp = inputs[f"{prefix}_ts_ms"]
        for name in {
            "bbo": ("bbo_best_bid", "bbo_best_ask", "bbo_bid_qty", "bbo_ask_qty"),
            "trade": ("trade_price", "trade_qty", "is_buyer_maker"),
        }[prefix]:
            if len(inputs[name]) != len(timestamp):
                raise ValueError(f"{name} length does not match {prefix}_ts_ms")
    l2_rows = len(inputs["l2_ts_ms"])
    shapes = {
        tuple(inputs[name].shape)
        for name in ("l2_bid_px", "l2_bid_qty", "l2_ask_px", "l2_ask_qty")
    }
    if len(shapes) != 1 or next(iter(shapes))[0] != l2_rows:
        raise ValueError("L2 matrices must share shape and match l2_ts_ms")
    for name in ("request_ts_ms", "bbo_ts_ms", "l2_ts_ms", "trade_ts_ms"):
        _require_sorted(inputs[name], name)
    windows = inputs["windows_ms"]
    _require_sorted(windows, "windows_ms")
    if windows.size == 0 or bool(np.any(windows <= 0)):
        raise ValueError("windows_ms must contain positive values")


def _normalized_inputs(
    *,
    request_ts_ms: Any,
    book_cutoff_ts_ms: Any,
    trade_cutoff_ts_ms: Any,
    activation_ts_ms: Any,
    terminal_ts_ms: Any,
    cancel_ack_ts_ms: Any,
    bbo_ts_ms: Any,
    bbo_best_bid: Any,
    bbo_best_ask: Any,
    bbo_bid_qty: Any,
    bbo_ask_qty: Any,
    l2_ts_ms: Any,
    l2_bid_px: Any,
    l2_bid_qty: Any,
    l2_ask_px: Any,
    l2_ask_qty: Any,
    trade_ts_ms: Any,
    trade_price: Any,
    trade_qty: Any,
    is_buyer_maker: Any,
    windows_ms: Sequence[int],
) -> dict[str, np.ndarray]:
    inputs = {
        "request_ts_ms": _as_i64(request_ts_ms),
        "book_cutoff_ts_ms": _as_i64(book_cutoff_ts_ms),
        "trade_cutoff_ts_ms": _as_i64(trade_cutoff_ts_ms),
        "activation_ts_ms": _as_i64(activation_ts_ms),
        "terminal_ts_ms": _as_i64(terminal_ts_ms),
        "cancel_ack_ts_ms": _as_i64(cancel_ack_ts_ms),
        "bbo_ts_ms": _as_i64(bbo_ts_ms),
        "bbo_best_bid": _as_f64(bbo_best_bid),
        "bbo_best_ask": _as_f64(bbo_best_ask),
        "bbo_bid_qty": _as_f64(bbo_bid_qty),
        "bbo_ask_qty": _as_f64(bbo_ask_qty),
        "l2_ts_ms": _as_i64(l2_ts_ms),
        "l2_bid_px": _as_f64(l2_bid_px),
        "l2_bid_qty": _as_f64(l2_bid_qty),
        "l2_ask_px": _as_f64(l2_ask_px),
        "l2_ask_qty": _as_f64(l2_ask_qty),
        "trade_ts_ms": _as_i64(trade_ts_ms),
        "trade_price": _as_f64(trade_price),
        "trade_qty": _as_f64(trade_qty),
        "is_buyer_maker": _as_u8(is_buyer_maker),
        "windows_ms": _as_i64(windows_ms),
    }
    _validate_inputs(inputs)
    return inputs


def compute_request_state_features_native(
    *,
    tick_size: float = 0.1,
    depth_levels: int = 5,
    l2_path_lookback_ms: int = 1_000,
    **values: Any,
) -> dict[str, np.ndarray]:
    """Compute request-time state through the C++ implementation."""

    inputs = _normalized_inputs(**values)
    try:
        import narrowgate_cpp  # type: ignore
    except Exception as exc:  # pragma: no cover - environment-specific failure
        raise RuntimeError("narrowgate_cpp is required for formal request-state builds") from exc
    if not hasattr(narrowgate_cpp, "compute_request_state_features"):
        raise RuntimeError("installed narrowgate_cpp lacks request_state_features.v1")
    result = narrowgate_cpp.compute_request_state_features(
        inputs["request_ts_ms"],
        inputs["book_cutoff_ts_ms"],
        inputs["trade_cutoff_ts_ms"],
        inputs["activation_ts_ms"],
        inputs["terminal_ts_ms"],
        inputs["cancel_ack_ts_ms"],
        inputs["bbo_ts_ms"],
        inputs["bbo_best_bid"],
        inputs["bbo_best_ask"],
        inputs["bbo_bid_qty"],
        inputs["bbo_ask_qty"],
        inputs["l2_ts_ms"],
        inputs["l2_bid_px"],
        inputs["l2_bid_qty"],
        inputs["l2_ask_px"],
        inputs["l2_ask_qty"],
        inputs["trade_ts_ms"],
        inputs["trade_price"],
        inputs["trade_qty"],
        inputs["is_buyer_maker"],
        inputs["windows_ms"],
        float(tick_size),
        int(depth_levels),
        int(l2_path_lookback_ms),
    )
    if str(result["schema_version"]) != SCHEMA_VERSION:
        raise RuntimeError("native request-state schema identity changed")
    return {name: np.asarray(value) for name, value in result.items() if name != "schema_version"}


def compute_request_state_features_python(
    *,
    tick_size: float = 0.1,
    depth_levels: int = 5,
    l2_path_lookback_ms: int = 1_000,
    **values: Any,
) -> dict[str, np.ndarray]:
    """Small authoritative reference used for C++ parity tests."""

    x = _normalized_inputs(**values)
    requests = x["request_ts_ms"]
    windows = x["windows_ms"]
    rows = len(requests)
    nan = np.nan
    out: dict[str, np.ndarray] = {
        "windows_ms": windows.copy(),
        "valid_book": np.zeros(rows, dtype=np.uint8),
        "book_source_ts_ms": np.zeros(rows, dtype=np.int64),
        "book_age_ms": np.full(rows, nan),
        "best_bid": np.full(rows, nan),
        "best_ask": np.full(rows, nan),
        "bid_qty": np.full(rows, nan),
        "ask_qty": np.full(rows, nan),
        "mid": np.full(rows, nan),
        "bbo_spread_ticks": np.full(rows, nan),
        "book_imbalance": np.full(rows, nan),
        "microprice_shift_bps": np.full(rows, nan),
        "l2_near_depth_total": np.full(rows, nan),
        "l2_quote_flip_rate": np.full(rows, nan),
        "l2_book_refresh_ratio": np.full(rows, nan),
        "l2_book_cancel_ratio": np.full(rows, nan),
        "active_order_count": np.zeros(rows, dtype=np.int64),
        "pending_cancel_before_count": np.zeros(rows, dtype=np.int64),
        "request_batch_size": np.zeros(rows, dtype=np.int64),
    }
    for name in _MATRIX_OUTPUTS:
        dtype = np.int64 if name in {"trade_count", "book_update_count"} else float
        fill = 0 if dtype is np.int64 or name != "market_return_bps" else nan
        out[name] = np.full((rows, len(windows)), fill, dtype=dtype)

    depth = min(int(depth_levels), x["l2_bid_qty"].shape[1])
    l2_total = (
        np.maximum(x["l2_bid_qty"][:, :depth], 0.0).sum(axis=1)
        + np.maximum(x["l2_ask_qty"][:, :depth], 0.0).sum(axis=1)
    )
    refresh = np.zeros(len(l2_total))
    cancel = np.zeros(len(l2_total))
    flip = np.zeros(len(l2_total))
    if len(l2_total) > 1:
        previous = l2_total[:-1]
        change = l2_total[1:] - previous
        valid = previous > 1e-12
        refresh[1:] = np.where(valid, np.maximum(change, 0.0) / np.where(valid, previous, 1.0), 0.0)
        cancel[1:] = np.where(valid, np.maximum(-change, 0.0) / np.where(valid, previous, 1.0), 0.0)
        flip[1:] = (
            (x["l2_bid_px"][1:, 0] != x["l2_bid_px"][:-1, 0])
            | (x["l2_ask_px"][1:, 0] != x["l2_ask_px"][:-1, 0])
        )
    refresh_prefix = np.r_[0.0, np.cumsum(refresh)]
    cancel_prefix = np.r_[0.0, np.cumsum(cancel)]
    flip_prefix = np.r_[0.0, np.cumsum(flip)]

    activations = np.sort(x["activation_ts_ms"][x["activation_ts_ms"] > 0])
    terminals = np.sort(x["terminal_ts_ms"][x["terminal_ts_ms"] > 0])
    request_events = np.sort(requests[requests > 0])
    pending_ends = np.sort(
        np.where(x["cancel_ack_ts_ms"] > 0, x["cancel_ack_ts_ms"], x["terminal_ts_ms"])[
            (requests > 0)
            & ((x["cancel_ack_ts_ms"] > 0) | (x["terminal_ts_ms"] > 0))
        ]
    )
    buy_prefix = np.r_[0.0, np.cumsum(np.where(x["is_buyer_maker"] == 0, np.maximum(x["trade_qty"], 0.0), 0.0))]
    sell_prefix = np.r_[0.0, np.cumsum(np.where(x["is_buyer_maker"] != 0, np.maximum(x["trade_qty"], 0.0), 0.0))]

    for row, request in enumerate(requests):
        if request <= 0:
            continue
        out["active_order_count"][row] = max(
            0,
            int(np.searchsorted(activations, request, side="right"))
            - int(np.searchsorted(terminals, request, side="right")),
        )
        out["pending_cancel_before_count"][row] = max(
            0,
            int(np.searchsorted(request_events, request, side="left"))
            - int(np.searchsorted(pending_ends, request, side="right")),
        )
        out["request_batch_size"][row] = int(
            np.searchsorted(request_events, request, side="right")
            - np.searchsorted(request_events, request, side="left")
        )
        book_cutoff = min(request, x["book_cutoff_ts_ms"][row])
        trade_cutoff = min(request, x["trade_cutoff_ts_ms"][row])
        bbo_end = int(np.searchsorted(x["bbo_ts_ms"], book_cutoff, side="left"))
        l2_end = int(np.searchsorted(x["l2_ts_ms"], book_cutoff, side="left"))
        if bbo_end:
            i = bbo_end - 1
            bid = x["bbo_best_bid"][i]
            ask = x["bbo_best_ask"][i]
            bid_qty = max(0.0, x["bbo_bid_qty"][i])
            ask_qty = max(0.0, x["bbo_ask_qty"][i])
            if bid > 0.0 and ask > bid:
                mid = 0.5 * (bid + ask)
                total = bid_qty + ask_qty
                microprice = (ask * bid_qty + bid * ask_qty) / total if total > 1e-12 else mid
                out["valid_book"][row] = 1
                out["book_source_ts_ms"][row] = x["bbo_ts_ms"][i]
                out["book_age_ms"][row] = request - x["bbo_ts_ms"][i]
                out["best_bid"][row] = bid
                out["best_ask"][row] = ask
                out["bid_qty"][row] = bid_qty
                out["ask_qty"][row] = ask_qty
                out["mid"][row] = mid
                out["bbo_spread_ticks"][row] = (ask - bid) / tick_size
                out["book_imbalance"][row] = (bid_qty - ask_qty) / total if total > 1e-12 else 0.0
                out["microprice_shift_bps"][row] = (microprice - mid) / mid * 10_000.0
        if l2_end:
            i = l2_end - 1
            out["l2_near_depth_total"][row] = l2_total[i]
            begin = int(np.searchsorted(x["l2_ts_ms"], request - l2_path_lookback_ms, side="left"))
            begin = min(begin, l2_end)
            count = l2_end - begin
            if count:
                out["l2_book_refresh_ratio"][row] = (refresh_prefix[l2_end] - refresh_prefix[begin]) / count
                out["l2_book_cancel_ratio"][row] = (cancel_prefix[l2_end] - cancel_prefix[begin]) / count
                out["l2_quote_flip_rate"][row] = (flip_prefix[l2_end] - flip_prefix[begin]) / count

        trade_end = int(np.searchsorted(x["trade_ts_ms"], trade_cutoff, side="left"))
        for column, window in enumerate(windows):
            trade_begin = int(np.searchsorted(x["trade_ts_ms"], request - window, side="left"))
            trade_begin = min(trade_begin, trade_end)
            buy = buy_prefix[trade_end] - buy_prefix[trade_begin]
            sell = sell_prefix[trade_end] - sell_prefix[trade_begin]
            out["aggressive_buy_qty"][row, column] = buy
            out["aggressive_sell_qty"][row, column] = sell
            out["taker_imbalance"][row, column] = (buy - sell) / (buy + sell) if buy + sell > 1e-12 else 0.0
            out["trade_count"][row, column] = trade_end - trade_begin
            book_begin = int(np.searchsorted(x["l2_ts_ms"], request - window, side="left"))
            book_begin = min(book_begin, l2_end)
            out["book_update_count"][row, column] = l2_end - book_begin
            if trade_end:
                current = trade_end - 1
                reference = trade_begin - 1 if trade_begin else (trade_begin if trade_begin < trade_end else current)
                if x["trade_price"][current] > 0.0 and x["trade_price"][reference] > 0.0:
                    out["market_return_bps"][row, column] = np.log(
                        x["trade_price"][current] / x["trade_price"][reference]
                    ) * 10_000.0
    return out


def flatten_request_state(result: Mapping[str, np.ndarray]) -> pd.DataFrame:
    """Flatten native vectors and window matrices into stable panel columns."""

    windows = np.asarray(result["windows_ms"], dtype=np.int64)
    rows = len(np.asarray(result["valid_book"]))
    columns: dict[str, Any] = {}
    for name in _VECTOR_OUTPUTS:
        values = np.asarray(result[name])
        if len(values) != rows:
            raise ValueError(f"request-state output length mismatch: {name}")
        columns[f"request_{name}"] = values
    for name in _MATRIX_OUTPUTS:
        matrix = np.asarray(result[name])
        if matrix.shape != (rows, len(windows)):
            raise ValueError(f"request-state matrix shape mismatch: {name}")
        for index, window in enumerate(windows):
            columns[f"request_{name}_{int(window)}ms"] = matrix[:, index]
    return pd.DataFrame(columns)
