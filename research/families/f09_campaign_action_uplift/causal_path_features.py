"""Causal fill-to-decision shock, refill, and price-recovery features."""

from __future__ import annotations

import math
from collections.abc import Mapping

import numpy as np

CAUSAL_PATH_FEATURE_VERSION = "shock_refill_recovery.v2"

CAUSAL_PATH_FEATURE_COLUMNS = (
    "path_feature_valid",
    "path_elapsed_ms",
    "path_log_elapsed_s",
    "path_book_age_ms",
    "path_log_book_age_ms",
    "path_l2_snapshot_count",
    "path_trade_count",
    "shock_adverse_flow_imbalance_1s",
    "shock_adverse_flow_imbalance_5s",
    "shock_adverse_flow_imbalance_since_fill",
    "shock_adverse_qty_to_depth_5s",
    "shock_log1p_adverse_qty_to_depth_5s",
    "shock_adverse_qty_to_depth_since_fill",
    "shock_log1p_adverse_qty_to_depth_since_fill",
    "shock_adverse_move_bps",
    "shock_time_to_extreme_ms",
    "shock_log1p_time_to_extreme_ms",
    "refill_depletion_ratio",
    "refill_recovery_ratio",
    "refill_current_vs_start_ratio",
    "refill_log1p_current_vs_start_ratio",
    "refill_half_life_ms",
    "refill_log1p_half_life_ms",
    "refill_half_life_observed",
    "recovery_current_adverse_bps",
    "recovery_from_extreme_bps",
    "recovery_price_ratio",
    "recovery_microprice_current_adverse_bps",
    "recovery_microprice_ratio",
)

# Compact subset used by action-Q. Counts/age/validity remain available for
# support audits, while the model consumes normalized path state.
CAUSAL_PATH_POLICY_FEATURES = (
    "path_feature_valid",
    "path_log_elapsed_s",
    "path_log_book_age_ms",
    "shock_adverse_flow_imbalance_1s",
    "shock_adverse_flow_imbalance_5s",
    "shock_adverse_flow_imbalance_since_fill",
    "shock_log1p_adverse_qty_to_depth_5s",
    "shock_log1p_adverse_qty_to_depth_since_fill",
    "shock_adverse_move_bps",
    "shock_log1p_time_to_extreme_ms",
    "refill_depletion_ratio",
    "refill_recovery_ratio",
    "refill_log1p_current_vs_start_ratio",
    "refill_log1p_half_life_ms",
    "refill_half_life_observed",
    "recovery_current_adverse_bps",
    "recovery_from_extreme_bps",
    "recovery_price_ratio",
    "recovery_microprice_current_adverse_bps",
    "recovery_microprice_ratio",
)


def empty_causal_path_features(
    *, start_ts_ms: int = 0, decision_ts_ms: int = 0
) -> dict[str, float]:
    output = {name: 0.0 for name in CAUSAL_PATH_FEATURE_COLUMNS}
    output["path_elapsed_ms"] = float(
        max(0, int(decision_ts_ms) - int(start_ts_ms))
    )
    output["path_log_elapsed_s"] = math.log1p(
        output["path_elapsed_ms"] / 1_000.0
    )
    return output


def _as_1d(values, *, dtype) -> np.ndarray:
    output = np.asarray(values, dtype=dtype)
    if output.ndim != 1:
        raise ValueError("causal path input must be one-dimensional")
    return output


def _flow_features(
    *,
    side: str,
    start_ts_ms: int,
    decision_ts_ms: int,
    trade_ts_ms: np.ndarray,
    trade_qty: np.ndarray,
    is_buyer_maker: np.ndarray,
    start_depth: float,
) -> dict[str, float]:
    adverse_is_seller = side == "BUY"

    def summarize(window_start_ms: int) -> tuple[float, float, int]:
        left = int(np.searchsorted(trade_ts_ms, window_start_ms, side="left"))
        right = int(np.searchsorted(trade_ts_ms, decision_ts_ms, side="right"))
        if right <= left:
            return 0.0, 0.0, 0
        qty = np.clip(trade_qty[left:right], 0.0, None)
        seller = is_buyer_maker[left:right]
        adverse = seller if adverse_is_seller else ~seller
        adverse_qty = float(qty[adverse].sum())
        favorable_qty = float(qty[~adverse].sum())
        total = adverse_qty + favorable_qty
        imbalance = (
            (adverse_qty - favorable_qty) / total if total > 1e-12 else 0.0
        )
        qty_to_depth = adverse_qty / start_depth if start_depth > 1e-12 else 0.0
        return float(imbalance), float(qty_to_depth), int(right - left)

    imbalance_1s, _, _ = summarize(max(start_ts_ms, decision_ts_ms - 1_000))
    imbalance_5s, qty_to_depth_5s, _ = summarize(
        max(start_ts_ms, decision_ts_ms - 5_000)
    )
    imbalance_path, qty_to_depth_path, trade_count = summarize(start_ts_ms)
    return {
        "path_trade_count": float(trade_count),
        "shock_adverse_flow_imbalance_1s": imbalance_1s,
        "shock_adverse_flow_imbalance_5s": imbalance_5s,
        "shock_adverse_flow_imbalance_since_fill": imbalance_path,
        "shock_adverse_qty_to_depth_5s": qty_to_depth_5s,
        "shock_adverse_qty_to_depth_since_fill": qty_to_depth_path,
    }


def _microprice(
    bid_px: np.ndarray,
    bid_qty: np.ndarray,
    ask_px: np.ndarray,
    ask_qty: np.ndarray,
) -> np.ndarray:
    bid_size = np.clip(bid_qty[:, 0], 0.0, None)
    ask_size = np.clip(ask_qty[:, 0], 0.0, None)
    denominator = bid_size + ask_size
    mid = 0.5 * (bid_px[:, 0] + ask_px[:, 0])
    return np.divide(
        ask_px[:, 0] * bid_size + bid_px[:, 0] * ask_size,
        denominator,
        out=mid.copy(),
        where=denominator > 1e-12,
    )


def _adverse_path(prices: np.ndarray, *, side: str) -> np.ndarray:
    start = float(prices[0])
    if not math.isfinite(start) or start <= 0.0:
        return np.zeros(len(prices), dtype=float)
    inventory_sign = 1.0 if side == "BUY" else -1.0
    returns_bps = (prices / start - 1.0) * 10_000.0
    return -inventory_sign * returns_bps


def _recovery_metrics(adverse_bps: np.ndarray) -> tuple[float, float, float, int]:
    if adverse_bps.size == 0:
        return 0.0, 0.0, 0.0, 0
    extreme_idx = int(np.argmax(adverse_bps))
    extreme = max(0.0, float(adverse_bps[extreme_idx]))
    current = float(adverse_bps[-1])
    recovered = max(0.0, extreme - current)
    ratio = min(2.0, recovered / extreme) if extreme > 1e-9 else 0.0
    return extreme, current, ratio, extreme_idx


def compute_causal_path_features(
    *,
    side: str,
    start_ts_ms: int,
    decision_ts_ms: int,
    trade_ts_ms,
    trade_qty,
    is_buyer_maker,
    l2_ts_ms,
    l2_bid_px,
    l2_bid_qty,
    l2_ask_px,
    l2_ask_qty,
    near_levels: int = 5,
) -> dict[str, float]:
    """Return side-normalized path state using events visible by decision time.

    ``start_ts_ms`` is the triggering same-side fill. BUY treats seller
    aggression and downward price movement as adverse; SELL uses the symmetric
    buyer-aggression/upward convention.
    """

    side = str(side).upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported side: {side}")
    start_ts_ms = int(start_ts_ms)
    decision_ts_ms = int(decision_ts_ms)
    output = empty_causal_path_features(
        start_ts_ms=start_ts_ms, decision_ts_ms=decision_ts_ms
    )
    if start_ts_ms <= 0 or decision_ts_ms < start_ts_ms:
        return output

    trade_ts = _as_1d(trade_ts_ms, dtype=np.int64)
    quantities = _as_1d(trade_qty, dtype=float)
    seller_flags = _as_1d(is_buyer_maker, dtype=bool)
    if not (len(trade_ts) == len(quantities) == len(seller_flags)):
        raise ValueError("trade path arrays have inconsistent lengths")

    book_ts = _as_1d(l2_ts_ms, dtype=np.int64)
    bid_px = np.asarray(l2_bid_px, dtype=float)
    bid_qty = np.asarray(l2_bid_qty, dtype=float)
    ask_px = np.asarray(l2_ask_px, dtype=float)
    ask_qty = np.asarray(l2_ask_qty, dtype=float)
    if not (
        bid_px.ndim == bid_qty.ndim == ask_px.ndim == ask_qty.ndim == 2
        and len(book_ts) == len(bid_px) == len(bid_qty) == len(ask_px) == len(ask_qty)
        and bid_px.shape == bid_qty.shape == ask_px.shape == ask_qty.shape
    ):
        raise ValueError("L2 path arrays have inconsistent shapes")
    if len(book_ts) == 0 or bid_px.shape[1] == 0:
        return output

    end_idx = int(np.searchsorted(book_ts, decision_ts_ms, side="right"))
    start_idx = int(np.searchsorted(book_ts, start_ts_ms, side="right")) - 1
    if end_idx <= 0 or start_idx < 0 or start_idx >= end_idx:
        return output
    path_slice = slice(start_idx, end_idx)
    path_ts = book_ts[path_slice]
    path_bid_px = bid_px[path_slice]
    path_bid_qty = bid_qty[path_slice]
    path_ask_px = ask_px[path_slice]
    path_ask_qty = ask_qty[path_slice]
    valid_book = (
        np.isfinite(path_bid_px[:, 0])
        & np.isfinite(path_ask_px[:, 0])
        & (path_bid_px[:, 0] > 0.0)
        & (path_ask_px[:, 0] > path_bid_px[:, 0])
    )
    if not valid_book.all():
        path_ts = path_ts[valid_book]
        path_bid_px = path_bid_px[valid_book]
        path_bid_qty = path_bid_qty[valid_book]
        path_ask_px = path_ask_px[valid_book]
        path_ask_qty = path_ask_qty[valid_book]
    if len(path_ts) == 0:
        return output

    levels = min(max(1, int(near_levels)), path_bid_qty.shape[1])
    side_depth = (
        np.clip(path_bid_qty[:, :levels], 0.0, None).sum(axis=1)
        if side == "BUY"
        else np.clip(path_ask_qty[:, :levels], 0.0, None).sum(axis=1)
    )
    start_depth = float(side_depth[0])
    current_depth = float(side_depth[-1])
    trough_idx = int(np.argmin(side_depth))
    trough_depth = float(side_depth[trough_idx])
    depletion = max(0.0, start_depth - trough_depth)
    depletion_ratio = depletion / start_depth if start_depth > 1e-12 else 0.0
    current_vs_start = (
        current_depth / start_depth if start_depth > 1e-12 else 0.0
    )
    refill_denominator = max(depletion, 0.05 * start_depth, 1e-12)
    refill_ratio = min(
        2.0, max(0.0, current_depth - trough_depth) / refill_denominator
    )
    half_life_ms = 0.0
    half_life_observed = 0.0
    if depletion > max(1e-12, 0.01 * start_depth):
        half_level = trough_depth + 0.5 * depletion
        recovered = np.flatnonzero(side_depth[trough_idx:] >= half_level)
        if recovered.size:
            recovery_idx = trough_idx + int(recovered[0])
            half_life_ms = float(path_ts[recovery_idx] - path_ts[trough_idx])
            half_life_observed = 1.0
        else:
            half_life_ms = float(decision_ts_ms - path_ts[trough_idx])

    mid = 0.5 * (path_bid_px[:, 0] + path_ask_px[:, 0])
    microprice = _microprice(
        path_bid_px, path_bid_qty, path_ask_px, path_ask_qty
    )
    adverse_mid = _adverse_path(mid, side=side)
    adverse_micro = _adverse_path(microprice, side=side)
    extreme, current_adverse, recovery_ratio, extreme_idx = _recovery_metrics(
        adverse_mid
    )
    micro_extreme, micro_current, micro_recovery_ratio, _ = _recovery_metrics(
        adverse_micro
    )

    output.update(
        _flow_features(
            side=side,
            start_ts_ms=start_ts_ms,
            decision_ts_ms=decision_ts_ms,
            trade_ts_ms=trade_ts,
            trade_qty=quantities,
            is_buyer_maker=seller_flags,
            start_depth=start_depth,
        )
    )
    output.update(
        {
            "path_feature_valid": 1.0,
            "path_book_age_ms": float(max(0, decision_ts_ms - int(path_ts[-1]))),
            "path_l2_snapshot_count": float(len(path_ts)),
            "shock_adverse_move_bps": extreme,
            "shock_time_to_extreme_ms": float(
                max(0, int(path_ts[extreme_idx]) - start_ts_ms)
            ),
            "refill_depletion_ratio": float(depletion_ratio),
            "refill_recovery_ratio": float(refill_ratio),
            "refill_current_vs_start_ratio": float(current_vs_start),
            "refill_half_life_ms": float(half_life_ms),
            "refill_half_life_observed": float(half_life_observed),
            "recovery_current_adverse_bps": float(current_adverse),
            "recovery_from_extreme_bps": float(max(0.0, extreme - current_adverse)),
            "recovery_price_ratio": float(recovery_ratio),
            "recovery_microprice_current_adverse_bps": float(micro_current),
            "recovery_microprice_ratio": float(micro_recovery_ratio),
        }
    )
    output.update(
        {
            "path_log_book_age_ms": math.log1p(output["path_book_age_ms"]),
            "shock_log1p_adverse_qty_to_depth_5s": math.log1p(
                output["shock_adverse_qty_to_depth_5s"]
            ),
            "shock_log1p_adverse_qty_to_depth_since_fill": math.log1p(
                output["shock_adverse_qty_to_depth_since_fill"]
            ),
            "shock_log1p_time_to_extreme_ms": math.log1p(
                output["shock_time_to_extreme_ms"]
            ),
            "refill_log1p_current_vs_start_ratio": math.log1p(
                output["refill_current_vs_start_ratio"]
            ),
            "refill_log1p_half_life_ms": math.log1p(
                output["refill_half_life_ms"]
            ),
        }
    )
    if not all(math.isfinite(float(value)) for value in output.values()):
        raise ValueError("causal path features must remain finite")
    return output


def validate_causal_path_mapping(row: Mapping[str, object]) -> None:
    missing = sorted(set(CAUSAL_PATH_FEATURE_COLUMNS) - set(row))
    if missing:
        raise ValueError(f"causal path mapping lacks features: {missing}")
    values = np.asarray([row[name] for name in CAUSAL_PATH_FEATURE_COLUMNS], dtype=float)
    if not np.isfinite(values).all():
        raise ValueError("causal path mapping contains non-finite values")
    if float(row["path_feature_valid"]) not in {0.0, 1.0}:
        raise ValueError("path_feature_valid must be binary")


__all__ = [
    "CAUSAL_PATH_FEATURE_COLUMNS",
    "CAUSAL_PATH_FEATURE_VERSION",
    "CAUSAL_PATH_POLICY_FEATURES",
    "compute_causal_path_features",
    "empty_causal_path_features",
    "validate_causal_path_mapping",
]
