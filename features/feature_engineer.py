"""
Step 2b: 从日度1秒bars构建10秒特征矩阵，用于ML模型训练。

输入: data/bars_1s/BTCUSDT-1s-YYYY-MM-DD.parquet
输出: data/features/features_YYYY-MM-DD.parquet  (按 UTC 日)
      data/features/dataset_train.parquet
      data/features/dataset_val.parquet
      data/features/dataset_test.parquet

特征分两大类：
  A. 微结构特征（滚动窗口计算）
  B. 时间特征（从timestamp派生）

用法:
    python features/feature_engineer.py              # 全部处理
    python features/feature_engineer.py --file 2026-05-15  # 只处理部分
"""

import argparse
import csv
import gc
import hashlib
import json
import math
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import yaml

try:
    from numba import njit as _njit
    HAS_NUMBA = True
    def njit_opt(*args, **kwargs):
        return _njit(*args, cache=True, **kwargs)
except ImportError:
    HAS_NUMBA = False
    def njit_opt(*args, **kwargs):
        def _wrap(func):
            return func
        return _wrap

ROOT = Path(__file__).parent.parent
LIVE_CONFIG_PATH = ROOT / "live" / "config.yaml"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from calendar_features import add_calendar_features  # noqa: E402
from data_paths import data_root, normalized_l2_root  # noqa: E402
from features.feature_dag import TEN_SECOND_CAUSAL_GRAPH  # noqa: E402
from data_quality import (  # noqa: E402
    continuous_segment_ids,
    filter_frame_for_orderbook_quality,
    filter_paths_for_orderbook_quality,
    mask_valid_horizon,
)
from market_fusion import (  # noqa: E402
    PERP_MARKET,
    SPOT_MARKET,
    default_reference_symbol,
    market_bars_dir,
    normalize_symbol,
)
from strategy.quote_core import finite_positive_quote_coefficient  # noqa: E402
from strategy.model_contract import (  # noqa: E402
    absolute_price_variance_unit_contract,
)

DATA_DIR = data_root(ROOT)
BARS_DIR = DATA_DIR / "bars_1s"
METRICS_DIR = DATA_DIR / "metrics_5m"
TRADE_FEATURE_DIR = Path(
    os.environ.get("MM_TRADE_FEATURE_DIR", DATA_DIR / "trade_features")
).expanduser().resolve()


def _book_dirs_for_symbol(
    symbol: str,
    *,
    legacy_root: Optional[Path] = None,
    normalized_root: Optional[Path] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> tuple[Path, Path]:
    """Route BTCUSDC to normalized L2 while BTCUSDT stays on legacy data."""

    env = os.environ if environ is None else environ
    legacy = DATA_DIR if legacy_root is None else Path(legacy_root)
    normalized = (
        normalized_l2_root(ROOT)
        if normalized_root is None
        else Path(normalized_root)
    )
    default_root = normalized if normalize_symbol(symbol) == "BTCUSDC" else legacy
    has_bbo_override = bool(str(env.get("MM_BBO_DIR", "")).strip())
    has_l2_override = bool(str(env.get("MM_L2_DIR", "")).strip())
    if has_bbo_override != has_l2_override:
        raise ValueError(
            "MM_BBO_DIR and MM_L2_DIR must be set together; "
            "mixed book roots are not a valid feature identity"
        )
    bbo_dir = Path(env.get("MM_BBO_DIR", default_root / "bbo"))
    l2_dir = Path(env.get("MM_L2_DIR", default_root / "l2"))
    return (
        bbo_dir.expanduser().resolve(),
        l2_dir.expanduser().resolve(),
    )


BBO_DIR, L2_DIR = _book_dirs_for_symbol("BTCUSDC")

DEFAULT_SYMBOL = normalize_symbol(os.environ.get("MM_SYMBOL"), "BTCUSDC")
VERBOSE = False


def qprint(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs)


def _is_day_tag(tag: str) -> bool:
    return len(tag) == 10 and tag[4] == "-" and tag[7] == "-"


def _read_days_file(path: Path) -> list[str]:
    with path.expanduser().resolve().open(
        newline="", encoding="utf-8-sig"
    ) as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or "day" not in reader.fieldnames:
            raise ValueError(f"days file must contain a day column: {path}")
        days = sorted(
            {
                str(row["day"]).strip()
                for row in reader
                if str(row.get("day", "")).strip()
            }
        )
    invalid = [day for day in days if not _is_day_tag(day)]
    if invalid:
        raise ValueError(f"invalid UTC day(s) in {path}: {invalid[:5]}")
    if not days:
        raise ValueError(f"days file is empty: {path}")
    return days


MIN_DATA_DAY = "2025-08-01"


def feature_out_dir(symbol: str) -> Path:
    return DATA_DIR / ("features" if symbol == "BTCUSDT" else f"features_{symbol.lower()}")


METRIC_FEATURE_COLS = [
    "oi_log", "oi_pct_change", "oi_zscore_1h", "oi_zscore_6h", "oi_momentum",
    "toptrader_ls_ratio", "crowd_ls_ratio", "taker_ls_ratio",
    "toptrader_ls_zscore", "crowd_ls_zscore", "taker_ls_zscore",
    "taker_ls_momentum", "oi_price_divergence",
]

# 10秒采样（匹配做市报撤频率）
RESAMPLE_SEC = 10
LABEL_GRID_MAX_GAP_S = float(os.environ.get("MM_LABEL_GRID_MAX_GAP_S", RESAMPLE_SEC * 1.5))
FEATURE_SOURCE_MAX_GAP_S = float(os.environ.get("MM_FEATURE_SOURCE_MAX_GAP_S", 3600.0))

# Rolling windows that can be represented exactly on completed 10s bars.
# Five-second microstructure features are computed separately from the causal
# 1s source; they must never be approximated by two 10s bars.
WINDOWS_10S = {
    "30s": 3,
    "60s": 6,
    "300s": 30,
}

# 标签：未来N秒价格变动
LABEL_HORIZONS = [10, 30, 60]  # 秒
TOXICITY_HORIZONS = [5, 10]  # side-specific adverse-selection probability

CROSS_FEATURE_SUFFIXES = [
    "basis_bps", "ret_10s", "ret_30s", "ret_60s",
    "volatility_60s", "volume_imbalance", "trade_intensity_60s", "vpin_60s",
    "basis_residual_bps", "age_s", "available",
]
CROSS_BOOK_FEATURE_SUFFIXES = [
    "basis_bps", "ret_10s", "ret_30s", "ret_60s", "volatility_60s",
    "basis_residual_bps", "age_s", "available",
]
CROSS_SOURCE_MAX_AGE_S = 30.0
CROSS_SOURCE_SEGMENT_MAX_GAP_S = 30.0
CROSS_BASIS_WINDOW_10S = 360
CROSS_BASIS_MIN_PERIODS = 30
EXECUTION_L2_STATE_COLS = [
    "l2_spread_bps",
    "l2_microprice_offset_bps",
    "l2_imbalance_l1",
    "l2_imbalance_l3",
    "l2_imbalance_l5",
    "l2_imbalance_l10",
    "l2_near_depth_total",
    "l2_depth_slope",
    "l2_depth_convexity",
    "l2_queue_concentration",
]
EXECUTION_L2_FLOW_COLS = [
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
]
EXECUTION_L2_FEATURE_COLS = EXECUTION_L2_STATE_COLS + EXECUTION_L2_FLOW_COLS

TAKER_TEMPO_WINDOWS_SEC = (5, 10, 30, 60)
TAKER_TEMPO_FEATURE_MAP = {
    **{
        f"quote_imbalance_{window}s": f"taker_quote_imbalance_{window}s"
        for window in TAKER_TEMPO_WINDOWS_SEC
    },
    **{
        f"signed_quote_sum_{window}s": f"taker_signed_quote_sum_{window}s"
        for window in TAKER_TEMPO_WINDOWS_SEC
    },
    **{
        f"trade_count_sum_{window}s": f"taker_trade_count_sum_{window}s"
        for window in TAKER_TEMPO_WINDOWS_SEC
    },
    **{
        f"max_same_side_run_max_{window}s": f"taker_max_same_side_run_{window}s"
        for window in TAKER_TEMPO_WINDOWS_SEC
    },
    **{
        f"buy_sweep_score_{window}s": f"taker_buy_sweep_score_{window}s"
        for window in TAKER_TEMPO_WINDOWS_SEC
    },
    **{
        f"sell_sweep_score_{window}s": f"taker_sell_sweep_score_{window}s"
        for window in TAKER_TEMPO_WINDOWS_SEC
    },
    **{
        f"buy_iceberg_pressure_sum_{window}s": f"taker_buy_iceberg_pressure_sum_{window}s"
        for window in TAKER_TEMPO_WINDOWS_SEC
    },
    **{
        f"sell_iceberg_pressure_sum_{window}s": f"taker_sell_iceberg_pressure_sum_{window}s"
        for window in TAKER_TEMPO_WINDOWS_SEC
    },
}
TAKER_TEMPO_FEATURE_COLS = list(TAKER_TEMPO_FEATURE_MAP.values())

_TS_NS = 1_000_000_000


@njit_opt()
def _compute_label_triplet(
    ts_1s_ns: np.ndarray,
    close_1s: np.ndarray,
    high_1s: np.ndarray,
    low_1s: np.ndarray,
    diff_1s: np.ndarray,
    quote_time_ns: np.ndarray,
    start_idx: np.ndarray,
    bid_quote: np.ndarray,
    ask_quote: np.ndarray,
    close_ref: np.ndarray,
    horizon_ns: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    n = len(quote_time_ns)
    ret_label = np.empty(n, dtype=np.float64)
    dir_label = np.empty(n, dtype=np.float64)
    vol_label = np.empty(n, dtype=np.float64)
    ret_label[:] = np.nan
    dir_label[:] = np.nan
    vol_label[:] = np.nan

    for i in range(n):
        start = int(start_idx[i])
        if start >= len(ts_1s_ns):
            continue

        # 1s bars are left-labelled.  A horizon [t, t+h) must exclude the bar
        # starting exactly at t+h.
        fill_stop = int(np.searchsorted(ts_1s_ns, quote_time_ns[i] + horizon_ns, side="left"))
        window_len = fill_stop - start
        if window_len >= 2:
            mean = 0.0
            for j in range(start, fill_stop):
                mean += diff_1s[j]
            mean /= window_len
            var = 0.0
            for j in range(start, fill_stop):
                d = diff_1s[j] - mean
                var += d * d
            vol_label[i] = var / (window_len - 1)

        if fill_stop <= start:
            continue

        bid_off = -1
        ask_off = -1
        for j in range(start, fill_stop):
            if bid_off == -1 and low_1s[j] <= bid_quote[i]:
                bid_off = j - start
            if ask_off == -1 and high_1s[j] >= ask_quote[i]:
                ask_off = j - start
            if bid_off != -1 and ask_off != -1:
                break

        if bid_off == -1 and ask_off == -1:
            continue
        if bid_off != -1 and ask_off != -1 and bid_off == ask_off:
            continue

        if ask_off == -1 or (bid_off != -1 and bid_off < ask_off):
            fill_idx = start + bid_off
            fill_px = bid_quote[i]
        else:
            fill_idx = start + ask_off
            fill_px = ask_quote[i]

        markout_idx = int(np.searchsorted(ts_1s_ns, ts_1s_ns[fill_idx] + horizon_ns, side="left"))
        if markout_idx >= len(close_1s):
            continue

        ref = close_ref[i] if close_ref[i] > 1e-9 else 1e-9
        ret_val = (close_1s[markout_idx] - fill_px) / ref
        ret_label[i] = ret_val
        dir_label[i] = 1.0 if ret_val > 0.0 else 0.0

    return ret_label, dir_label, vol_label


@njit_opt()
def _compute_toxicity_pair(
    ts_1s_ns: np.ndarray,
    close_1s: np.ndarray,
    high_1s: np.ndarray,
    low_1s: np.ndarray,
    quote_time_ns: np.ndarray,
    start_idx: np.ndarray,
    bid_quote: np.ndarray,
    ask_quote: np.ndarray,
    horizon_ns: int,
) -> tuple[np.ndarray, np.ndarray]:
    n = len(quote_time_ns)
    tox_bid = np.empty(n, dtype=np.float64)
    tox_ask = np.empty(n, dtype=np.float64)
    tox_bid[:] = np.nan
    tox_ask[:] = np.nan

    for i in range(n):
        start = int(start_idx[i])
        if start >= len(ts_1s_ns):
            continue

        fill_stop = int(np.searchsorted(ts_1s_ns, quote_time_ns[i] + horizon_ns, side="left"))
        if fill_stop <= start:
            continue

        bid_fill_idx = -1
        ask_fill_idx = -1
        for j in range(start, fill_stop):
            if bid_fill_idx == -1 and low_1s[j] <= bid_quote[i]:
                bid_fill_idx = j
            if ask_fill_idx == -1 and high_1s[j] >= ask_quote[i]:
                ask_fill_idx = j
            if bid_fill_idx != -1 and ask_fill_idx != -1:
                break

        if bid_fill_idx != -1:
            markout_idx = int(np.searchsorted(ts_1s_ns, ts_1s_ns[bid_fill_idx] + horizon_ns, side="left"))
            if markout_idx < len(close_1s):
                tox_bid[i] = 1.0 if close_1s[markout_idx] < bid_quote[i] else 0.0

        if ask_fill_idx != -1:
            markout_idx = int(np.searchsorted(ts_1s_ns, ts_1s_ns[ask_fill_idx] + horizon_ns, side="left"))
            if markout_idx < len(close_1s):
                tox_ask[i] = 1.0 if close_1s[markout_idx] > ask_quote[i] else 0.0

    return tox_bid, tox_ask


def _load_label_quote_params(symbol: str, config_path: Optional[Path] = None) -> dict:
    path = Path(config_path) if config_path else LIVE_CONFIG_PATH
    with open(path, "r") as f:
        raw = yaml.safe_load(f) or {}

    strat = raw.get("strategy", {})
    regime = raw.get("regime", {})
    fees = raw.get("fees", {})
    ml = raw.get("ml", {})
    tick_size = float(raw.get("tick_size", 0.1))
    maker_fee = float(fees.get("maker", 0.0))

    model_dir_raw = str(ml.get("model_dir", "") or "").strip()
    if not model_dir_raw:
        raise ValueError(
            f"{path}: ml.model_dir must explicitly identify the P3 artifact used for labels"
        )
    model_dir = Path(model_dir_raw).expanduser()
    if not model_dir.is_absolute():
        model_dir = ROOT / model_dir
    fill_prob_path = (model_dir / "fill_prob_params.json").resolve()

    from research.families.f02_empirical_p3_touch.fill_probability import FillProbabilityModel

    try:
        fill_model = FillProbabilityModel.load(fill_prob_path)
    except Exception as exc:
        raise RuntimeError(
            f"label P3 artifact unavailable: {fill_prob_path}: {exc}"
        ) from exc
    if (
        fill_model.schema_version != "narrowgate_p3_touch_calibration.v2"
        or fill_model.model_type != "empirical_survival"
    ):
        raise ValueError(
            "feature labels require empirical causal P3 v2, got "
            f"schema={fill_model.schema_version!r} type={fill_model.model_type!r} "
            f"from {fill_prob_path}"
        )
    p3_delta_star = float(fill_model.optimal_delta())
    p3_kappa_eff = float(fill_model.effective_kappa(p3_delta_star))
    if p3_delta_star <= 0.0 or p3_kappa_eff <= 0.0:
        raise ValueError(f"invalid P3 calibration values in {fill_prob_path}")

    gamma = finite_positive_quote_coefficient("strategy.gamma", strat.get("gamma", 0.01))
    raw_a_spread = strat.get("a_spread")
    a_spread = (
        gamma
        if raw_a_spread is None
        else finite_positive_quote_coefficient("strategy.a_spread", raw_a_spread)
    )
    raw_risk_per_order = strat.get("risk_per_order")
    risk_per_order = (
        a_spread
        if raw_risk_per_order is None
        else finite_positive_quote_coefficient(
            "strategy.risk_per_order", raw_risk_per_order
        )
    )
    raw_execution_slope = strat.get("execution_intensity_slope")
    execution_intensity_slope = (
        finite_positive_quote_coefficient(
            "strategy.kappa", strat.get("kappa", 0.073)
        )
        if raw_execution_slope is None
        else finite_positive_quote_coefficient(
            "strategy.execution_intensity_slope", raw_execution_slope
        )
    )
    historical_p3_adapter = bool(
        strat.get("historical_p3_scalar_adapter_enabled", True)
    )
    p3_side_bbo_floor_enabled = bool(
        strat.get("p3_side_bbo_floor_enabled", False)
    )
    if historical_p3_adapter and p3_side_bbo_floor_enabled:
        raise ValueError("P3 historical pair and side-BBO modes are mutually exclusive")
    if p3_side_bbo_floor_enabled:
        raise ValueError(
            "feature-label half-spread projection has no decision-time side BBO; "
            "P3 side-BBO mode must use the full quote consumer"
        )
    p3_identity = fill_model.semantic_identity(require_artifact_hash=True)
    return {
        "gamma": gamma,
        "a_spread": a_spread,
        "risk_per_order": risk_per_order,
        "execution_intensity_slope": execution_intensity_slope,
        "kappa": float(strat.get("kappa", 0.073)),
        "kappa_ratio": float(strat.get("kappa_ratio", 1.0)),
        "quote_horizon_s": float(strat.get("quote_horizon_s", 1.0)),
        "risk_horizon_s": float(
            strat.get("risk_horizon_s")
            if strat.get("risk_horizon_s") is not None
            else strat.get("quote_horizon_s", 1.0)
        ),
        "historical_p3_scalar_adapter_enabled": historical_p3_adapter,
        "p3_side_bbo_floor_enabled": p3_side_bbo_floor_enabled,
        "max_spread_bps": float(strat.get("max_spread_bps", 0.0)),
        "dynamic_cap_enabled": bool(strat.get("dynamic_cap_enabled", False)),
        "dynamic_cap_base_bps": float(
            strat.get("dynamic_cap_base_bps", strat.get("max_spread_bps", 0.0))
        ),
        "dynamic_cap_alpha": float(strat.get("dynamic_cap_alpha", 0.5)),
        "dynamic_cap_min_mult": float(strat.get("dynamic_cap_min_mult", 1.0)),
        "dynamic_cap_max_mult": float(strat.get("dynamic_cap_max_mult", 2.0)),
        "dynamic_cap_var_baseline": float(strat.get("dynamic_cap_var_baseline", 0.0)),
        "vol_power": float(strat.get("vol_power", 1.0)),
        "vol_baseline": float(regime.get("vol_baseline", 3.0)),
        "gamma_scale_min": float(regime.get("gamma_scale_min", 0.5)),
        "gamma_scale_max": float(regime.get("gamma_scale_max", 2.0)),
        "liq_baseline": float(regime.get("liq_baseline", 200.0)),
        "gamma_liq_scale_min": float(regime.get("gamma_liq_scale_min", 0.5)),
        "gamma_liq_scale_max": float(regime.get("gamma_liq_scale_max", 3.0)),
        "maker_fee": maker_fee,
        "tick_size": tick_size,
        "p3_delta_star": p3_delta_star,
        "p3_kappa_eff": p3_kappa_eff,
        "fill_probability_model_path": str(fill_prob_path),
        "fill_probability_sha256": _sha256_file(fill_prob_path),
        "fill_probability_schema_version": fill_model.schema_version,
        "fill_probability_model_type": fill_model.model_type,
        "fill_probability_event_type": str(p3_identity["event_type"]),
        "fill_probability_horizon_s": float(p3_identity["horizon_s"]),
        "fill_probability_distance_origin": str(p3_identity["distance_origin"]),
        "fill_probability_distance_unit": str(p3_identity["distance_unit"]),
        "fill_probability_side": str(p3_identity["side"]),
        "fill_probability_queue_included": bool(p3_identity["queue_included"]),
        "fill_probability_artifact_sha256": str(p3_identity["artifact_sha256"]),
    }


def _load_multi_market_defaults(config_path: Optional[Path]) -> tuple[str, Optional[str]]:
    stage = "single"
    reference_symbol = None
    path = Path(config_path) if config_path else LIVE_CONFIG_PATH
    try:
        with open(path, "r") as f:
            raw = yaml.safe_load(f) or {}
    except Exception:
        return stage, reference_symbol

    multi = raw.get("multi_market") or {}
    if multi:
        if not bool(multi.get("enabled", False)):
            stage = "single"
        else:
            stage = str(multi.get("market_stage", "minimal"))
        reference_symbol = multi.get("reference_symbol")
    return stage, reference_symbol


def _load_exec_l2_max_age_s(config_path: Optional[Path]) -> float:
    path = Path(config_path) if config_path else LIVE_CONFIG_PATH
    try:
        with open(path, "r") as f:
            raw = yaml.safe_load(f) or {}
    except Exception:
        return 5.0

    risk = raw.get("risk") or {}
    try:
        return float(risk.get("max_exec_book_age_s", 5.0))
    except (TypeError, ValueError):
        return 5.0


def _prepare_1s_label_context(bars_1s: pd.DataFrame) -> tuple[np.ndarray, ...]:
    if isinstance(bars_1s.index, pd.DatetimeIndex):
        idx = pd.to_datetime(bars_1s.index, utc=True)
    else:
        idx = pd.to_datetime(bars_1s.index, unit="ms", utc=True)

    closes = bars_1s["close"].to_numpy(dtype=np.float64)
    highs = bars_1s["high"].to_numpy(dtype=np.float64)
    lows = bars_1s["low"].to_numpy(dtype=np.float64)

    diffs = np.empty_like(closes)
    diffs[0] = 0.0
    diffs[1:] = closes[1:] - closes[:-1]
    sigma_sq = (pd.Series(diffs)
                .rolling(60, min_periods=20)
                .var()
                .ffill()
                .bfill()
                .to_numpy(dtype=np.float64))

    return idx.as_unit("ns").asi8, closes, highs, lows, diffs, sigma_sq


def _quote_half_spread(df: pd.DataFrame, close_ref: np.ndarray,
                       sigma_sq: np.ndarray, quote_params: dict) -> np.ndarray:
    raw_risk_per_order = quote_params.get("risk_per_order")
    if raw_risk_per_order is None:
        raw_risk_per_order = quote_params.get(
            "a_spread", quote_params["gamma"]
        )
    risk_per_order = max(
        finite_positive_quote_coefficient(
            "risk_per_order", raw_risk_per_order
        ),
        1e-12,
    )
    kappa_ratio = max(float(quote_params["kappa_ratio"]), 1e-6)
    historical_p3_adapter = bool(
        quote_params.get("historical_p3_scalar_adapter_enabled", True)
    )
    distance_slope = (
        float(quote_params["p3_kappa_eff"])
        if historical_p3_adapter
        else float(
            quote_params.get(
                "execution_intensity_slope", quote_params["kappa"]
            )
        )
    )
    kappa_spread = max(distance_slope * kappa_ratio, 1e-6)
    risk_horizon_s = max(
        float(
            quote_params.get(
                "risk_horizon_s", quote_params["quote_horizon_s"]
            )
        ),
        1e-6,
    )
    sigma_sq_horizon = sigma_sq * risk_horizon_s

    delta = risk_per_order * sigma_sq_horizon + (2.0 / risk_per_order) * np.log1p(
        risk_per_order / kappa_spread
    )

    liq_baseline = float(quote_params["liq_baseline"])
    if liq_baseline > 0 and "trade_intensity_60s" in df.columns:
        ti = df["trade_intensity_60s"].fillna(liq_baseline).to_numpy(dtype=np.float64)
        liq_ratio = np.maximum(ti / liq_baseline, 0.04)
        liq_scale = 1.0 / np.maximum(np.sqrt(liq_ratio), 0.2)
        liq_scale = np.clip(
            liq_scale,
            float(quote_params["gamma_liq_scale_min"]),
            float(quote_params["gamma_liq_scale_max"]),
        )
        delta *= liq_scale

    vol_baseline = float(quote_params["vol_baseline"])
    if vol_baseline > 0:
        vol_sq_ratio = np.maximum(sigma_sq / (vol_baseline * vol_baseline), 0.09)
        vol_scale = vol_sq_ratio ** (float(quote_params["vol_power"]) * 0.5)
        vol_scale = np.clip(
            vol_scale,
            float(quote_params["gamma_scale_min"]),
            float(quote_params["gamma_scale_max"]),
        )
        delta *= vol_scale

    p3_delta_star = float(quote_params["p3_delta_star"])
    if historical_p3_adapter and p3_delta_star > 0:
        delta = np.maximum(delta, 2.0 * p3_delta_star)

    tick_size = float(quote_params["tick_size"])
    fee_floor = 2.0 * abs(float(quote_params["maker_fee"])) * close_ref + tick_size
    delta = np.maximum(delta, fee_floor)

    cap_bps: float | np.ndarray = float(quote_params["max_spread_bps"])
    if (
        bool(quote_params["dynamic_cap_enabled"])
        and float(quote_params["dynamic_cap_base_bps"]) > 0.0
    ):
        var_baseline = float(quote_params["dynamic_cap_var_baseline"])
        if var_baseline <= 0.0:
            var_baseline = max(float(quote_params["vol_baseline"]) ** 2, 1e-12)
        cap_mult = (
            np.maximum(sigma_sq / var_baseline, 1.0)
            ** float(quote_params["dynamic_cap_alpha"])
        )
        cap_mult = np.clip(
            cap_mult,
            float(quote_params["dynamic_cap_min_mult"]),
            float(quote_params["dynamic_cap_max_mult"]),
        )
        cap_bps = float(quote_params["dynamic_cap_base_bps"]) * cap_mult
    if np.any(np.asarray(cap_bps) > 0.0):
        delta = np.minimum(delta, close_ref * np.asarray(cap_bps) / 10_000.0)

    return 0.5 * delta

def load_bars(date_filter: str = None, symbol: str = DEFAULT_SYMBOL,
              market_type: str = PERP_MARKET) -> pd.DataFrame:
    """加载1秒bars，可选按日期过滤"""
    symbol = normalize_symbol(symbol)
    bars_dir = market_bars_dir(ROOT, market_type)
    files = sorted(bars_dir.glob(f"{symbol}-1s-*.parquet"))
    if not files:
        print("错误：未找到1s bar文件，请先运行 features/preprocess.py")
        sys.exit(1)

    if date_filter:
        files = [f for f in files if date_filter in f.name]
    files = filter_paths_for_orderbook_quality(files, symbol, label="1s bar")

    dfs = []
    for f in files:
        df = pd.read_parquet(f)
        dfs.append(df)
        print(f"  加载 {f.name}: {len(df):,} bars")

    bars = pd.concat(dfs)
    bars.sort_index(inplace=True)
    # 去重（日文件重跑时可能重叠）
    bars = bars[~bars.index.duplicated(keep="first")]
    bars = filter_frame_for_orderbook_quality(bars, symbol, label="1s bar")
    print(f"  合计: {len(bars):,} 个1秒bars\n")
    return bars


def _as_utc_index(index_like) -> pd.DatetimeIndex:
    if isinstance(index_like, pd.DatetimeIndex):
        return pd.to_datetime(index_like, utc=True).tz_convert("UTC")
    return pd.to_datetime(index_like, unit="ms", utc=True)


def _calendar_bounds_for_tag(tag: Optional[str], index: pd.DatetimeIndex) -> tuple[pd.Timestamp, pd.Timestamp]:
    if tag:
        if len(tag) == 10:
            start = pd.Timestamp(tag, tz="UTC")
            end = start + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            return start, end
    start = index.min().floor("1s")
    end = index.max().floor("1s")
    return start, end


def densify_bars_1s(
    bars_1s: pd.DataFrame,
    calendar_tag: Optional[str] = None,
    ensure_through_day_tag: Optional[str] = None,
) -> pd.DataFrame:
    """Pad sparse traded-second bars to a dense UTC 1s axis."""
    if bars_1s is None or bars_1s.empty:
        return bars_1s.copy()

    bars = bars_1s.copy()
    bars.index = _as_utc_index(bars.index).floor("1s")
    bars.sort_index(inplace=True)
    bars = bars[~bars.index.duplicated(keep="last")]

    start, end = _calendar_bounds_for_tag(calendar_tag, bars.index)
    if ensure_through_day_tag:
        target_end = (
            pd.Timestamp(ensure_through_day_tag, tz="UTC")
            + pd.Timedelta(days=1)
            - pd.Timedelta(seconds=1)
        )
        end = max(end, target_end)
    bars = bars.reindex(pd.date_range(start, end, freq="1s", tz="UTC"))

    close_fill = bars["close"].ffill().bfill()
    missing_rows = bars["close"].isna()
    for col in ("open", "high", "low", "close", "vwap"):
        if col not in bars.columns:
            bars[col] = np.nan
        bars.loc[missing_rows, col] = close_fill[missing_rows]
        bars[col] = bars[col].fillna(close_fill)

    for col in ("volume", "buy_volume", "sell_volume", "trade_count", "buy_count", "sell_count"):
        if col not in bars.columns:
            bars[col] = 0.0
        bars[col] = bars[col].fillna(0.0)

    for col in ("trade_count", "buy_count", "sell_count"):
        bars[col] = bars[col].astype(np.int64)

    return bars


def _safe_divide(numerator, denominator) -> np.ndarray:
    num = np.asarray(numerator, dtype=np.float64)
    den = np.asarray(denominator, dtype=np.float64)
    out = np.zeros_like(num, dtype=np.float64)
    np.divide(num, den, out=out, where=den > 0)
    return out


def resample_to_10s(bars_1s: pd.DataFrame) -> pd.DataFrame:
    """将1s bars重采样为10s bars"""
    bars_1s = bars_1s.copy()
    bars_1s.index = _as_utc_index(bars_1s.index)
    turnover_1s = bars_1s["vwap"].fillna(bars_1s["close"]) * bars_1s["volume"].fillna(0.0)

    bars = bars_1s.resample("10s").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "buy_volume": "sum",
        "sell_volume": "sum",
        "trade_count": "sum",
        "buy_count": "sum",
        "sell_count": "sum",
    })
    bars["vwap"] = turnover_1s.resample("10s").sum() / bars["volume"].replace(0, np.nan)
    zero_volume = bars["volume"] <= 0
    bars.loc[zero_volume, "vwap"] = bars.loc[zero_volume, "close"]

    price_ref = bars["close"].ffill().bfill()
    for col in ("open", "high", "low", "close"):
        bars[col] = bars[col].fillna(price_ref)
    bars["vwap"] = bars["vwap"].fillna(price_ref)

    return bars


def _trade_tempo_paths(symbol: str, tag: str) -> list[Path]:
    root = TRADE_FEATURE_DIR / symbol
    if len(tag) == 10:
        path = root / f"{symbol}-trade-tempo-{tag}.parquet"
        return [path] if path.exists() else []
    return sorted(root.glob(f"{symbol}-trade-tempo-{tag}-*.parquet"))


def _load_taker_tempo_features(symbol: str, tag: str) -> Optional[pd.DataFrame]:
    paths = filter_paths_for_orderbook_quality(
        _trade_tempo_paths(symbol, tag), symbol, label="trade tempo"
    )
    if not paths:
        return None

    source_cols = list(TAKER_TEMPO_FEATURE_MAP.keys())
    chunks = []
    for path in paths:
        schema_cols = set(pq.ParquetFile(path).schema_arrow.names)
        read_cols = [col for col in source_cols if col in schema_cols]
        frame = pd.read_parquet(path, columns=read_cols)
        frame.index = _as_utc_index(frame.index)
        for col in source_cols:
            if col not in frame.columns:
                frame[col] = 0.0
        chunks.append(frame[source_cols])

    if not chunks:
        return None
    tempo = pd.concat(chunks).sort_index()
    tempo = tempo[~tempo.index.duplicated(keep="last")]
    tempo = tempo.rename(columns=TAKER_TEMPO_FEATURE_MAP)
    return tempo[TAKER_TEMPO_FEATURE_COLS]


def add_taker_tempo_features(df_10s: pd.DataFrame, symbol: str,
                             data_tag: str,
                             require_taker_tempo: bool = False) -> pd.DataFrame:
    tempo = _load_taker_tempo_features(symbol, data_tag)
    if tempo is None or tempo.empty:
        if require_taker_tempo:
            raise RuntimeError(
                "required taker-tempo sidecar is unavailable or invalid: "
                f"symbol={symbol} day={data_tag} root={TRADE_FEATURE_DIR}"
            )
        for col in TAKER_TEMPO_FEATURE_COLS:
            df_10s[col] = 0.0
        qprint(f"  taker tempo: no sidecar for {data_tag}; filled zeros")
        return df_10s

    tempo_10s = tempo.resample("10s").last()
    aligned = tempo_10s.reindex(df_10s.index)
    aligned = aligned.ffill(limit=1).fillna(0.0)
    return df_10s.join(aligned, how="left").fillna({col: 0.0 for col in aligned.columns})


def _empty_cross_features(index: pd.Index, prefix: str) -> pd.DataFrame:
    out = pd.DataFrame({f"{prefix}_{suffix}": 0.0 for suffix in CROSS_FEATURE_SUFFIXES}, index=index)
    out[f"{prefix}_age_s"] = CROSS_SOURCE_MAX_AGE_S + 10.0
    return out


def _source_freshness(
    source_index: pd.Index,
    target_index: pd.Index,
    source_event_ts_ms: Optional[pd.Series] = None,
) -> tuple[pd.Series, pd.Series]:
    """Return decision-time source age and availability on the 10s feature grid."""
    source_ts = _as_utc_index(source_index).floor("1ms")
    target_ts = _as_utc_index(target_index)
    if len(source_ts) == 0:
        age = pd.Series(CROSS_SOURCE_MAX_AGE_S + 10.0, index=target_ts)
        return age, pd.Series(0.0, index=target_ts)

    if source_event_ts_ms is None:
        event_ns = pd.Series(source_ts.as_unit("ns").asi8, index=source_ts)
        latest_by_bucket = event_ns.resample("10s").max()
    else:
        event_ms = pd.to_numeric(source_event_ts_ms, errors="coerce").to_numpy(dtype=np.float64)
        event_ns_values = event_ms * 1_000_000.0
        latest_by_bucket = pd.Series(event_ns_values, index=source_ts).groupby(level=0).max()
    aligned_ns = latest_by_bucket.reindex(
        target_ts,
        method="ffill",
        tolerance=pd.Timedelta(seconds=CROSS_SOURCE_MAX_AGE_S),
    )
    # Offline 10s bars are left-labelled and become observable at bucket end.
    decision_ns = (target_ts + pd.Timedelta(seconds=10)).as_unit("ns").asi8
    age_s = (pd.Series(decision_ns, index=target_ts) - aligned_ns) / 1_000_000_000.0
    age_s = age_s.where(age_s >= 0.0)
    available = age_s.notna() & age_s.le(CROSS_SOURCE_MAX_AGE_S)
    age_s = age_s.clip(lower=0.0, upper=CROSS_SOURCE_MAX_AGE_S + 10.0)
    age_s = age_s.fillna(CROSS_SOURCE_MAX_AGE_S + 10.0)
    return age_s.astype(float), available.astype(float)


def _basis_residual_bps(basis_bps: pd.Series) -> pd.Series:
    """Remove the slowly varying cross-market basis using history only."""
    anchor = basis_bps.rolling(
        CROSS_BASIS_WINDOW_10S,
        min_periods=CROSS_BASIS_MIN_PERIODS,
    ).median().shift(1)
    return (basis_bps - anchor).replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _first_existing_column(frame: pd.DataFrame, candidates: tuple[str, ...]) -> Optional[str]:
    for name in candidates:
        if name in frame.columns:
            return name
    return None


def _frame_ts_ms(frame: pd.DataFrame) -> np.ndarray:
    for col in ("timestamp", "ts_ms", "event_time", "transact_time", "time", "E", "T"):
        if col not in frame.columns:
            continue
        series = frame[col]
        if pd.api.types.is_datetime64_any_dtype(series):
            parsed = pd.DatetimeIndex(pd.to_datetime(series, utc=True, errors="coerce"))
            return parsed.as_unit("ns").asi8 // 1_000_000
        arr = series.to_numpy()
        if np.issubdtype(arr.dtype, np.integer):
            return arr.astype(np.int64, copy=False)
        parsed = pd.to_datetime(series, utc=True, errors="coerce")
        if parsed.notna().any():
            return pd.DatetimeIndex(parsed).as_unit("ns").asi8 // 1_000_000
    raise ValueError("No timestamp column found in historical BBO parquet")


def _load_bbo_10s(path: Path) -> Optional[pd.DataFrame]:
    try:
        parquet_file = pq.ParquetFile(path)
        bucket_frames = []

        for batch in parquet_file.iter_batches(batch_size=1_000_000):
            frame = batch.to_pandas()
            if frame.empty:
                continue

            bid_col = _first_existing_column(frame, ("best_bid", "bid_price", "bid", "b"))
            ask_col = _first_existing_column(frame, ("best_ask", "ask_price", "ask", "a"))
            if bid_col is None or ask_col is None:
                return None

            bid_qty_col = _first_existing_column(frame, ("best_bid_qty", "bid_qty", "bid_size", "B"))
            ask_qty_col = _first_existing_column(frame, ("best_ask_qty", "ask_qty", "ask_size", "A"))

            ts_ms = _frame_ts_ms(frame)
            out = pd.DataFrame({
                "timestamp": ts_ms,
                "best_bid": frame[bid_col].astype(np.float64, copy=False),
                "best_ask": frame[ask_col].astype(np.float64, copy=False),
                "best_bid_qty": frame[bid_qty_col].astype(np.float64, copy=False)
                if bid_qty_col else np.zeros(len(frame), dtype=np.float64),
                "best_ask_qty": frame[ask_qty_col].astype(np.float64, copy=False)
                if ask_qty_col else np.zeros(len(frame), dtype=np.float64),
            })
            valid = (
                (out["timestamp"] > 0)
                & (out["best_bid"] > 0.0)
                & (out["best_ask"] > out["best_bid"])
            )
            if not valid.any():
                continue

            out = out.loc[valid]
            out["bucket_ts"] = (out["timestamp"] // (_TS_NS // 100_000)) * (_TS_NS // 100_000)
            bucket_frames.append(out.groupby("bucket_ts", sort=False).last())

        if not bucket_frames:
            return None

        reduced = pd.concat(bucket_frames, axis=0)
        reduced = reduced.groupby(level=0, sort=True).last()
        reduced.index = pd.to_datetime(reduced.index.astype(np.int64), unit="ms", utc=True)
        reduced.index.name = None
        return reduced[["best_bid", "best_ask", "best_bid_qty", "best_ask_qty", "timestamp"]].rename(
            columns={"timestamp": "event_ts_ms"}
        )
    except Exception as exc:
        print(f"  [WARN] cross-market BBO 文件读取失败，已跳过: {path.name} ({exc})")
        return None


def _load_market_bbo_for_tag(symbol: str, market_type: str,
                             day_tag: Optional[str]) -> Optional[pd.DataFrame]:
    if market_type != PERP_MARKET:
        return None

    if not day_tag:
        return None
    bbo_dir, _ = _book_dirs_for_symbol(symbol)
    for stem in ("bookTicker", "bbo"):
        path = bbo_dir / f"{symbol}-{stem}-{day_tag}.parquet"
        if path.exists():
            frame = _load_bbo_10s(path)
            if frame is not None and not frame.empty:
                return filter_frame_for_orderbook_quality(frame, symbol, label="BBO")
    return None


def _load_market_bars_for_tag(symbol: str, market_type: str,
                              day_tag: Optional[str]) -> Optional[pd.DataFrame]:
    bars_dir = market_bars_dir(ROOT, market_type)
    if not day_tag:
        return None
    path = bars_dir / f"{symbol}-1s-{day_tag}.parquet"
    if path.exists():
        return filter_frame_for_orderbook_quality(pd.read_parquet(path), symbol, label=f"{market_type} 1s bar")
    print(f"  [WARN] cross-market bars missing: {symbol} {market_type} ({day_tag})")
    return None


def _cross_market_feature_segment(source_bars_1s: pd.DataFrame, target_index: pd.Index,
                                  target_close: pd.Series, prefix: str) -> pd.DataFrame:
    source_index = _as_utc_index(source_bars_1s.index)
    source_dense = densify_bars_1s(source_bars_1s, calendar_tag=None)
    source_10s = resample_to_10s(source_dense)
    aligned = source_10s.reindex(target_index, method="ffill", tolerance=pd.Timedelta(seconds=30))
    close = aligned["close"].astype(float)
    target_close = target_close.astype(float).replace(0, np.nan)
    log_ret = np.log(close / close.shift(1))

    out = pd.DataFrame(index=target_index)
    out[f"{prefix}_basis_bps"] = (close - target_close) / target_close * 10000.0
    out[f"{prefix}_ret_10s"] = log_ret
    out[f"{prefix}_ret_30s"] = np.log(close / close.shift(WINDOWS_10S["30s"]))
    out[f"{prefix}_ret_60s"] = np.log(close / close.shift(WINDOWS_10S["60s"]))
    out[f"{prefix}_volatility_60s"] = log_ret.rolling(WINDOWS_10S["60s"], min_periods=2).std()

    buy = aligned["buy_volume"].astype(float)
    sell = aligned["sell_volume"].astype(float)
    total = buy + sell
    out[f"{prefix}_volume_imbalance"] = (buy - sell) / total.replace(0, np.nan)
    out[f"{prefix}_trade_intensity_60s"] = (
        aligned["trade_count"].astype(float).rolling(WINDOWS_10S["60s"], min_periods=1).mean()
    )
    abs_imb = (buy - sell).abs().rolling(WINDOWS_10S["60s"], min_periods=1).sum()
    total_roll = total.rolling(WINDOWS_10S["60s"], min_periods=1).sum()
    out[f"{prefix}_vpin_60s"] = abs_imb / total_roll.replace(0, np.nan)
    out[f"{prefix}_basis_residual_bps"] = _basis_residual_bps(out[f"{prefix}_basis_bps"])
    age_s, available = _source_freshness(source_index, target_index)
    out[f"{prefix}_age_s"] = age_s.to_numpy(dtype=np.float64)
    out[f"{prefix}_available"] = available.to_numpy(dtype=np.float64)
    value_cols = [col for col in out if not col.endswith(("_age_s", "_available"))]
    out.loc[available.eq(0.0).to_numpy(), value_cols] = 0.0
    return out.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _source_segments(frame: pd.DataFrame, max_gap_s: float) -> list[pd.DataFrame]:
    # cross-market rolling feature 只能在连续 source segment 内计算；
    # 如果 reference/spot 中间缺数据，不允许用 ffill 把缺口两端粘在一起。
    ordered = frame.copy()
    ordered.index = _as_utc_index(ordered.index)
    ordered.sort_index(inplace=True)
    ordered = ordered[~ordered.index.duplicated(keep="last")]
    segment_ids = continuous_segment_ids(ordered.index, max_gap_s=max_gap_s)
    return [part for _, part in ordered.groupby(segment_ids, sort=False) if not part.empty]


def _target_segment(index: pd.DatetimeIndex, start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    start_bucket = start.floor("10s")
    end_bucket = end.floor("10s")
    return index[(index >= start_bucket) & (index <= end_bucket)]


def _cross_market_feature_frame(source_bars_1s: pd.DataFrame, target_index: pd.Index,
                                target_close: pd.Series, prefix: str,
                                calendar_tag: Optional[str] = None) -> pd.DataFrame:
    del calendar_tag  # Segments, not calendar padding, define rolling continuity.
    target_ts = _as_utc_index(target_index)
    target_close = target_close.copy()
    target_close.index = target_ts
    out = _empty_cross_features(target_ts, prefix)
    for segment in _source_segments(source_bars_1s, CROSS_SOURCE_SEGMENT_MAX_GAP_S):
        segment_target = _target_segment(target_ts, segment.index.min(), segment.index.max())
        if len(segment_target) == 0:
            continue
        part = _cross_market_feature_segment(
            segment,
            segment_target,
            target_close.reindex(segment_target),
            prefix,
        )
        out.loc[segment_target, part.columns] = part.to_numpy()
    return out


def _cross_market_book_feature_segment(source_bbo_10s: pd.DataFrame, target_index: pd.Index,
                                       target_close: pd.Series, prefix: str) -> pd.DataFrame:
    aligned = source_bbo_10s.reindex(target_index, method="ffill", tolerance=pd.Timedelta(seconds=30))
    mid = 0.5 * (aligned["best_bid"].astype(float) + aligned["best_ask"].astype(float))
    target_close = target_close.astype(float).replace(0, np.nan)
    log_ret = np.log(mid / mid.shift(1))

    out = pd.DataFrame(index=target_index)
    out[f"{prefix}_basis_bps"] = (mid - target_close) / target_close * 10000.0
    out[f"{prefix}_ret_10s"] = log_ret
    out[f"{prefix}_ret_30s"] = np.log(mid / mid.shift(WINDOWS_10S["30s"]))
    out[f"{prefix}_ret_60s"] = np.log(mid / mid.shift(WINDOWS_10S["60s"]))
    out[f"{prefix}_volatility_60s"] = log_ret.rolling(WINDOWS_10S["60s"], min_periods=2).std()
    out[f"{prefix}_basis_residual_bps"] = _basis_residual_bps(out[f"{prefix}_basis_bps"])
    age_s, available = _source_freshness(
        source_bbo_10s.index,
        target_index,
        source_event_ts_ms=source_bbo_10s.get("event_ts_ms"),
    )
    out[f"{prefix}_age_s"] = age_s.to_numpy(dtype=np.float64)
    out[f"{prefix}_available"] = available.to_numpy(dtype=np.float64)
    value_cols = [col for col in out if not col.endswith(("_age_s", "_available"))]
    out.loc[available.eq(0.0).to_numpy(), value_cols] = 0.0
    return out.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _cross_market_book_feature_frame(source_bbo_10s: pd.DataFrame, target_index: pd.Index,
                                     target_close: pd.Series, prefix: str) -> pd.DataFrame:
    target_ts = _as_utc_index(target_index)
    target_close = target_close.copy()
    target_close.index = target_ts
    out = _empty_cross_features(target_ts, prefix)
    for segment in _source_segments(source_bbo_10s, max_gap_s=15.0):
        segment_target = _target_segment(target_ts, segment.index.min(), segment.index.max())
        if len(segment_target) == 0:
            continue
        part = _cross_market_book_feature_segment(
            segment,
            segment_target,
            target_close.reindex(segment_target),
            prefix,
        )
        out.loc[segment_target, part.columns] = part.to_numpy()
    return out


def add_cross_market_features(df_10s: pd.DataFrame, symbol: str, day_tag: str,
                              market_stage: str = "minimal",
                              reference_symbol: Optional[str] = None,
                              data_tag: Optional[str] = None) -> pd.DataFrame:
    stage = (market_stage or "minimal").strip().lower()
    if stage in {"single", "none", "off", "disabled"}:
        return df_10s

    symbol = normalize_symbol(symbol)
    reference = normalize_symbol(reference_symbol, default_reference_symbol(symbol))
    target_close = df_10s["close"]

    markets = []
    if reference != symbol:
        markets.append((reference, PERP_MARKET, "cv_ref_perp"))
    if stage in {"enhanced", "full"}:
        markets.append((symbol, SPOT_MARKET, "cv_exec_spot"))
        if reference != symbol:
            markets.append((reference, SPOT_MARKET, "cv_ref_spot"))

    for market_symbol, market_type, prefix in markets:
        lookup_tag = data_tag or day_tag
        source = _load_market_bars_for_tag(market_symbol, market_type, lookup_tag)
        if source is None or source.empty:
            features = _empty_cross_features(df_10s.index, prefix)
        else:
            features = _cross_market_feature_frame(
                source, df_10s.index, target_close, prefix,
                calendar_tag=lookup_tag,
            )

        bbo_10s = _load_market_bbo_for_tag(market_symbol, market_type, lookup_tag)
        if bbo_10s is not None and not bbo_10s.empty:
            book_features = _cross_market_book_feature_frame(bbo_10s, df_10s.index, target_close, prefix)
            book_available = book_features[f"{prefix}_available"].gt(0.5)
            for suffix in CROSS_BOOK_FEATURE_SUFFIXES:
                col = f"{prefix}_{suffix}"
                features[col] = book_features[col].where(book_available, features[col])
        df_10s = df_10s.join(features, how="left")

    return df_10s.fillna({col: 0.0 for col in df_10s.columns if col.startswith("cv_")})


def load_metrics(data_tag: str, symbol: str) -> pd.DataFrame:
    """加载对应 UTC 日期的 metrics parquet (5分钟间隔)。"""
    path = METRICS_DIR / f"{symbol}-metrics-{data_tag}.parquet"
    if not path.exists():
        print(f"  [WARN] metrics 文件不存在: {path.name}")
        return None
    df = pd.read_parquet(path)
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    print(f"  加载 metrics: {len(df):,} 行")
    return df


def _l2_paths_for_tag(symbol: str, tag: str) -> list[Path]:
    _, l2_dir = _book_dirs_for_symbol(symbol)
    path = l2_dir / f"{symbol}-l2-{tag}.parquet"
    if path.exists():
        return filter_paths_for_orderbook_quality([path], symbol, label="L2")
    if len(tag) == 7:
        return filter_paths_for_orderbook_quality(
            sorted(l2_dir.glob(f"{symbol}-l2-{tag}-*.parquet")),
            symbol,
            label="L2",
        )
    return []


def _load_l2_summary_1s(path: Path) -> Optional[pd.DataFrame]:
    try:
        parquet_file = pq.ParquetFile(path)
        state_frames = []
        prev_best_bid = None
        prev_best_ask = None
        prev_total_depth = None

        bid_px_cols = [f"bid_px_{level}" for level in range(1, 11)]
        bid_qty_cols = [f"bid_qty_{level}" for level in range(1, 11)]
        ask_px_cols = [f"ask_px_{level}" for level in range(1, 11)]
        ask_qty_cols = [f"ask_qty_{level}" for level in range(1, 11)]

        for batch in parquet_file.iter_batches(batch_size=250_000):
            frame = batch.to_pandas()
            if frame.empty:
                continue
            required = ["timestamp", *bid_px_cols, *bid_qty_cols, *ask_px_cols, *ask_qty_cols]
            if any(col not in frame.columns for col in required):
                return None

            ts_ms = _frame_ts_ms(frame)
            bid_px = frame[bid_px_cols].to_numpy(dtype=np.float64, copy=False)
            bid_qty = np.nan_to_num(frame[bid_qty_cols].to_numpy(dtype=np.float64, copy=False), nan=0.0)
            ask_px = frame[ask_px_cols].to_numpy(dtype=np.float64, copy=False)
            ask_qty = np.nan_to_num(frame[ask_qty_cols].to_numpy(dtype=np.float64, copy=False), nan=0.0)

            best_bid = bid_px[:, 0]
            best_ask = ask_px[:, 0]
            mid = 0.5 * (best_bid + best_ask)
            valid = (ts_ms > 0) & (best_bid > 0.0) & (best_ask > best_bid) & (mid > 0.0)
            if not valid.any():
                continue

            ts_ms = ts_ms[valid]
            bid_px = bid_px[valid]
            bid_qty = bid_qty[valid]
            ask_px = ask_px[valid]
            ask_qty = ask_qty[valid]
            best_bid = bid_px[:, 0]
            best_ask = ask_px[:, 0]
            mid = mid[valid]

            bid_cum = np.cumsum(bid_qty, axis=1)
            ask_cum = np.cumsum(ask_qty, axis=1)
            level_qty = bid_qty + ask_qty
            near_depth_total = bid_cum[:, 2] + ask_cum[:, 2]
            total_depth_10 = bid_cum[:, 9] + ask_cum[:, 9]
            micro_den = bid_qty[:, 0] + ask_qty[:, 0]
            microprice = np.where(
                micro_den > 0,
                (best_ask * bid_qty[:, 0] + best_bid * ask_qty[:, 0]) / micro_den,
                mid,
            )

            prev_bid = np.empty_like(best_bid)
            prev_ask = np.empty_like(best_ask)
            prev_depth = np.empty_like(total_depth_10)
            prev_bid[0] = best_bid[0] if prev_best_bid is None else prev_best_bid
            prev_ask[0] = best_ask[0] if prev_best_ask is None else prev_best_ask
            prev_depth[0] = total_depth_10[0] if prev_total_depth is None else prev_total_depth
            if len(best_bid) > 1:
                prev_bid[1:] = best_bid[:-1]
                prev_ask[1:] = best_ask[:-1]
                prev_depth[1:] = total_depth_10[:-1]

            delta_depth = total_depth_10 - prev_depth
            front_mean = level_qty[:, :3].mean(axis=1)
            mid_mean = level_qty[:, 3:7].mean(axis=1)
            back_mean = level_qty[:, 7:10].mean(axis=1)

            out = pd.DataFrame({
                "bucket_ts": (ts_ms // 1000) * 1000,
                "l2_spread_bps": _safe_divide(best_ask - best_bid, mid) * 10000.0,
                "l2_microprice_offset_bps": _safe_divide(microprice - mid, mid) * 10000.0,
                "l2_imbalance_l1": _safe_divide(bid_cum[:, 0] - ask_cum[:, 0], bid_cum[:, 0] + ask_cum[:, 0]),
                "l2_imbalance_l3": _safe_divide(bid_cum[:, 2] - ask_cum[:, 2], bid_cum[:, 2] + ask_cum[:, 2]),
                "l2_imbalance_l5": _safe_divide(bid_cum[:, 4] - ask_cum[:, 4], bid_cum[:, 4] + ask_cum[:, 4]),
                "l2_imbalance_l10": _safe_divide(bid_cum[:, 9] - ask_cum[:, 9], bid_cum[:, 9] + ask_cum[:, 9]),
                "l2_near_depth_total": near_depth_total,
                "l2_depth_slope": _safe_divide(near_depth_total, total_depth_10),
                "l2_depth_convexity": _safe_divide(front_mean - 2.0 * mid_mean + back_mean, front_mean + mid_mean + back_mean),
                "l2_queue_concentration": _safe_divide(level_qty[:, 0], near_depth_total),
                "_quote_flip_sum": ((best_bid != prev_bid) | (best_ask != prev_ask)).astype(np.float64),
                "_refresh_sum": _safe_divide(np.maximum(delta_depth, 0.0), prev_depth),
                "_cancel_sum": _safe_divide(np.maximum(-delta_depth, 0.0), prev_depth),
                "_snapshot_count": 1.0,
            })
            if prev_best_bid is None:
                out.iloc[0, out.columns.get_loc("_quote_flip_sum")] = 0.0
                out.iloc[0, out.columns.get_loc("_refresh_sum")] = 0.0
                out.iloc[0, out.columns.get_loc("_cancel_sum")] = 0.0

            reduced = out.groupby("bucket_ts", sort=False).agg({
                **{col: "last" for col in EXECUTION_L2_STATE_COLS},
                "_quote_flip_sum": "sum",
                "_refresh_sum": "sum",
                "_cancel_sum": "sum",
                "_snapshot_count": "sum",
            })
            state_frames.append(reduced)
            prev_best_bid = float(best_bid[-1])
            prev_best_ask = float(best_ask[-1])
            prev_total_depth = float(total_depth_10[-1])

        if not state_frames:
            return None

        reduced = pd.concat(state_frames, axis=0)
        reduced = reduced.groupby(level=0, sort=True).agg({
            **{col: "last" for col in EXECUTION_L2_STATE_COLS},
            "_quote_flip_sum": "sum",
            "_refresh_sum": "sum",
            "_cancel_sum": "sum",
            "_snapshot_count": "sum",
        })
        reduced["l2_quote_flip_rate"] = _safe_divide(reduced["_quote_flip_sum"].to_numpy(), reduced["_snapshot_count"].to_numpy())
        reduced["l2_book_refresh_ratio"] = _safe_divide(reduced["_refresh_sum"].to_numpy(), reduced["_snapshot_count"].to_numpy())
        reduced["l2_book_cancel_ratio"] = _safe_divide(reduced["_cancel_sum"].to_numpy(), reduced["_snapshot_count"].to_numpy())
        reduced.index = pd.to_datetime(reduced.index.astype(np.int64), unit="ms", utc=True)
        reduced.index.name = None
        return reduced[EXECUTION_L2_FEATURE_COLS]
    except Exception as exc:
        print(f"  [WARN] execution L2 文件读取失败，已跳过: {path.name} ({exc})")
        return None


def load_l2_summary_1s(tag: str, symbol: str) -> Optional[pd.DataFrame]:
    symbol = normalize_symbol(symbol)
    paths = _l2_paths_for_tag(symbol, tag)
    if not paths:
        print(f"  [WARN] execution L2 文件不存在: {symbol} {tag}")
        return None

    frames = []
    for path in paths:
        frame = _load_l2_summary_1s(path)
        if frame is not None and not frame.empty:
            frames.append(frame)

    if not frames:
        return None

    combined = pd.concat(frames, axis=0)
    combined.sort_index(inplace=True)
    combined = combined[~combined.index.duplicated(keep="last")]
    combined = filter_frame_for_orderbook_quality(combined, symbol, label="L2 summary")
    print(f"  加载 execution L2: {len(combined):,} 秒级快照")
    return combined


def add_execution_l2_features(df_10s: pd.DataFrame, dense_index_1s: pd.Index,
                              day_tag: str, symbol: str,
                              config_path: Optional[Path] = None,
                              require_l2: bool = False) -> pd.DataFrame:
    l2_1s = load_l2_summary_1s(day_tag, symbol)
    if l2_1s is None or l2_1s.empty:
        if require_l2:
            _, l2_dir = _book_dirs_for_symbol(symbol)
            raise RuntimeError(
                "required execution L2 is unavailable or invalid: "
                f"symbol={symbol} day={day_tag} root={l2_dir}"
            )
        for col in EXECUTION_L2_FEATURE_COLS:
            df_10s[col] = 0.0
        return df_10s

    dense_index_1s = _as_utc_index(dense_index_1s)
    max_age_s = _load_exec_l2_max_age_s(config_path)
    state = l2_1s[EXECUTION_L2_STATE_COLS]
    if max_age_s > 0:
        aligned_state = state.reindex(
            dense_index_1s,
            method="ffill",
            tolerance=pd.Timedelta(seconds=max_age_s),
        )
    else:
        aligned_state = state.reindex(dense_index_1s, method="ffill")
    aligned = pd.concat([
        aligned_state,
        l2_1s[EXECUTION_L2_FLOW_COLS].reindex(dense_index_1s),
    ], axis=1)
    aligned[EXECUTION_L2_FLOW_COLS] = aligned[EXECUTION_L2_FLOW_COLS].fillna(0.0)

    l2_10s = pd.concat([
        aligned[EXECUTION_L2_STATE_COLS].resample("10s").last(),
        aligned[EXECUTION_L2_FLOW_COLS].resample("10s").mean(),
    ], axis=1)

    df_10s = df_10s.join(l2_10s.reindex(df_10s.index), how="left")
    return df_10s.fillna({col: 0.0 for col in EXECUTION_L2_FEATURE_COLS})


def compute_tick_momentum(bars_1s: pd.DataFrame) -> pd.DataFrame:
    """
    从1s bars计算 tick-by-tick momentum 特征，然后采样到10s。
    在 resample 之前调用（在1s精度上计算）。
    """
    bars_1s = bars_1s.copy()
    bars_1s.index = _as_utc_index(bars_1s.index)
    cl = bars_1s["close"]
    ret_1s = cl.diff()
    sign_1s = np.sign(ret_1s)

    # --- tick direction streak: 连续同方向1s bar数 ---
    streak = np.zeros(len(sign_1s), dtype=np.float64)
    for i in range(1, len(sign_1s)):
        if sign_1s.iloc[i] == sign_1s.iloc[i - 1] and sign_1s.iloc[i] != 0:
            streak[i] = streak[i - 1] + sign_1s.iloc[i]
        else:
            streak[i] = sign_1s.iloc[i]
    bars_1s["_tick_streak"] = streak

    # --- micro momentum: 滚动 sign 求和 ---
    bars_1s["_tick_mom_3s"] = sign_1s.rolling(3, min_periods=1).sum()
    bars_1s["_tick_mom_5s"] = sign_1s.rolling(5, min_periods=1).sum()
    bars_1s["_tick_mom_10s"] = sign_1s.rolling(10, min_periods=1).sum()

    # --- weighted tick momentum: EWM of 1s returns ---
    bars_1s["_tick_ewm_3s"] = ret_1s.ewm(span=3, min_periods=1).mean()
    bars_1s["_tick_ewm_10s"] = ret_1s.ewm(span=10, min_periods=1).mean()

    # --- return distribution moments (10s rolling window on 1s data) ---
    moments = ret_1s.rolling(10, min_periods=3)
    count = ret_1s.rolling(10, min_periods=1).count()
    population_std = moments.std(ddof=0).fillna(0.0)
    standardized = (count >= 5) & (population_std > 1e-12)
    # Live/native use population standardized moments. Undo pandas' sample
    # bias corrections using its vectorized rolling kernels, not rolling.apply.
    sample_moments = ret_1s.rolling(10, min_periods=5)
    population_skew = sample_moments.skew() * (count - 2) / np.sqrt(count * (count - 1))
    population_kurt = (
        sample_moments.kurt() * (count - 2) * (count - 3) / (count - 1) - 6
    ) / (count + 1)
    # pandas suppresses near-constant higher moments before live's 1e-12
    # standard-deviation cutoff. Recompute only those exceptional windows,
    # using the same two-pass centered population moments as live.
    fallback = standardized & (
        ~np.isfinite(population_skew) | ~np.isfinite(population_kurt)
    )
    returns = ret_1s.to_numpy()
    for end in np.flatnonzero(fallback.to_numpy()):
        sample = returns[max(0, end - 9):end + 1]
        sample = sample[np.isfinite(sample)]
        sd = float(np.std(sample))
        population_std.iloc[end] = sd
        standardized.iloc[end] = sd > 1e-12
        z = (sample - sample.mean()) / sd if sd > 1e-12 else np.zeros_like(sample)
        population_skew.iloc[end] = np.mean(z ** 3)
        population_kurt.iloc[end] = np.mean(z ** 4) - 3 if sd > 1e-12 else 0.0
    bars_1s["_micro_ret_std"] = population_std
    bars_1s["_micro_ret_skew"] = population_skew.where(standardized, 0.0).fillna(0.0)
    bars_1s["_micro_ret_kurt"] = population_kurt.where(standardized, 0.0).fillna(0.0)

    # --- tick reversal freq: sign change ratio in last 10 bars ---
    sign_change = (sign_1s.diff().abs() > 0).astype(float)
    bars_1s["_tick_reversal_freq"] = sign_change.rolling(10, min_periods=3).mean()

    # --- trade flow acceleration: 2nd derivative of cumulative signed volume ---
    signed_vol = bars_1s["buy_volume"] - bars_1s["sell_volume"]
    cum_sv = signed_vol.cumsum()
    bars_1s["_flow_velocity"] = cum_sv.diff(1)
    bars_1s["_flow_acceleration"] = cum_sv.diff(1).diff(1)

    # 采样到10s (取每10s窗口的最后值 + 统计量)
    tick_cols = [c for c in bars_1s.columns if c.startswith("_")]
    tick_df = bars_1s[tick_cols]

    # 对于 streak/momentum: 取窗口末尾值
    last_features = tick_df.resample("10s").last()
    # 对于 skew/kurt/std: 也取末尾 (已经是rolling计算的)
    # 去掉前缀 '_'
    last_features.columns = [c.lstrip("_") for c in last_features.columns]

    # 额外：10s窗口内的 max streak 和 mean reversal
    extra = pd.DataFrame(index=last_features.index)
    extra["tick_streak_max"] = bars_1s["_tick_streak"].abs().resample("10s").max()
    extra["tick_mom_range"] = (
        bars_1s["_tick_mom_5s"].resample("10s").max() -
        bars_1s["_tick_mom_5s"].resample("10s").min()
    )

    result = pd.concat([last_features, extra], axis=1)
    result.dropna(how="all", inplace=True)
    return result


def add_metrics_features(df_10s: pd.DataFrame, metrics: pd.DataFrame) -> pd.DataFrame:
    """
    将5分钟metrics forward-fill merge到10s bars，计算metrics衍生特征。
    原始字段: sum_open_interest, sum_open_interest_value,
              count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio,
              count_long_short_ratio, sum_taker_long_short_vol_ratio
    """
    if metrics is None or metrics.empty:
        for col in METRIC_FEATURE_COLS:
            df_10s[col] = 0.0
        return df_10s

    # Forward-fill 5min metrics to 10s grid
    m = metrics.reindex(df_10s.index, method="ffill")

    oi = m["sum_open_interest"]
    top_ls = m["sum_toptrader_long_short_ratio"]
    count_ls = m["count_long_short_ratio"]
    taker_ls = m["sum_taker_long_short_vol_ratio"]

    # --- 1. OI level (log-normalized for stationarity) ---
    df_10s["oi_log"] = np.log(oi.replace(0, np.nan))

    # --- 2. OI change rate (5min pct change, forward-filled) ---
    # Since data is 5min, diff(1) on forward-filled gives change at boundary
    df_10s["oi_pct_change"] = oi.pct_change(30)  # 30 x 10s = 5min

    # --- 3. OI z-score (rolling 1h, 6h) ---
    for w, label in [(360, "1h"), (2160, "6h")]:  # 360 x 10s = 1h
        oi_mean = oi.rolling(w, min_periods=30).mean()
        oi_std = oi.rolling(w, min_periods=30).std()
        df_10s[f"oi_zscore_{label}"] = (oi - oi_mean) / oi_std.replace(0, np.nan)

    # --- 4. OI momentum (short vs long rolling mean) ---
    oi_ma_short = oi.rolling(360, min_periods=30).mean()   # 1h
    oi_ma_long = oi.rolling(2160, min_periods=60).mean()   # 6h
    df_10s["oi_momentum"] = (oi_ma_short - oi_ma_long) / oi_ma_long.replace(0, np.nan)

    # --- 5. Long/short ratios (raw) ---
    df_10s["toptrader_ls_ratio"] = top_ls
    df_10s["crowd_ls_ratio"] = count_ls
    df_10s["taker_ls_ratio"] = taker_ls

    # --- 6. LS ratio deviations from rolling mean ---
    for col, name in [(top_ls, "toptrader_ls"), (count_ls, "crowd_ls"),
                      (taker_ls, "taker_ls")]:
        ma = col.rolling(2160, min_periods=60).mean()  # 6h
        std = col.rolling(2160, min_periods=60).std()
        df_10s[f"{name}_zscore"] = (col - ma) / std.replace(0, np.nan)

    # --- 7. Taker ratio momentum (short-term vs long-term) ---
    taker_ma_s = taker_ls.rolling(360, min_periods=30).mean()
    taker_ma_l = taker_ls.rolling(2160, min_periods=60).mean()
    df_10s["taker_ls_momentum"] = taker_ma_s - taker_ma_l

    # --- 8. OI-price divergence (OI up + price down = bearish signal) ---
    if "close" in df_10s.columns:
        price_ret = df_10s["close"].pct_change(30)  # 5min return
        oi_ret = oi.pct_change(30)
        df_10s["oi_price_divergence"] = oi_ret - price_ret

    return df_10s


# ============================================================
#  A. 微结构特征
# ============================================================

def _microstructure_5s_from_1s(bars_1s: pd.DataFrame) -> pd.DataFrame:
    """Compute true trailing-five-second state at each completed 10s bucket."""

    source = bars_1s.copy()
    source.index = _as_utc_index(source.index)
    close = source["close"].astype(float)
    log_ret_1s = np.log(close / close.shift(1))
    buy = source["buy_volume"].astype(float)
    sell = source["sell_volume"].astype(float)
    total = buy + sell

    five = pd.DataFrame(index=source.index)
    five["volatility_5s"] = log_ret_1s.rolling(5, min_periods=2).std() * np.sqrt(5.0)
    buy_sum = buy.rolling(5, min_periods=1).sum()
    sell_sum = sell.rolling(5, min_periods=1).sum()
    five["volume_imbalance_5s"] = (
        (buy_sum - sell_sum) / (buy_sum + sell_sum).replace(0, np.nan)
    )
    five["trade_intensity_5s"] = (
        source["trade_count"].astype(float).rolling(5, min_periods=1).mean()
    )
    five["vpin_5s"] = (
        (buy - sell).abs().rolling(5, min_periods=1).sum()
        / total.rolling(5, min_periods=1).sum().replace(0, np.nan)
    )
    five["price_change_5s"] = close.pct_change(5)
    return five.resample("10s").last()


def add_microstructure_features(
    df: pd.DataFrame,
    bars_1s: pd.DataFrame,
) -> pd.DataFrame:
    """Add causal 5s-from-1s and longer 10s-grid microstructure features."""

    close = df["close"]
    log_ret = np.log(close / close.shift(1))

    # --- 1. 已实现波动率 (多窗口) ---
    for label, w in WINDOWS_10S.items():
        df[f"volatility_{label}"] = log_ret.rolling(w, min_periods=2).std() * np.sqrt(w)

    # --- 2. 成交量不平衡 ---
    total_vol = df["buy_volume"] + df["sell_volume"]
    df["volume_imbalance"] = (df["buy_volume"] - df["sell_volume"]) / total_vol.replace(0, np.nan)

    # 多窗口滚动 volume imbalance
    for label, w in WINDOWS_10S.items():
        buy_sum = df["buy_volume"].rolling(w, min_periods=1).sum()
        sell_sum = df["sell_volume"].rolling(w, min_periods=1).sum()
        total = buy_sum + sell_sum
        df[f"volume_imbalance_{label}"] = (buy_sum - sell_sum) / total.replace(0, np.nan)

    # --- 3. 成交到达率 ---
    for label, w in WINDOWS_10S.items():
        df[f"trade_intensity_{label}"] = df["trade_count"].rolling(w, min_periods=1).mean()

    # --- 4. Clock-volume imbalance (frozen ``vpin_*`` feature ABI) ---
    # 固定墙钟窗口内 |buy_vol - sell_vol| / total_vol；没有等成交量 bucket，
    # 因此不是原始 VPIN estimator。字段名为冻结模型/feature schema 保持不变。
    for label, w in WINDOWS_10S.items():
        abs_imb = (df["buy_volume"] - df["sell_volume"]).abs().rolling(w, min_periods=1).sum()
        total = total_vol.rolling(w, min_periods=1).sum()
        df[f"vpin_{label}"] = abs_imb / total.replace(0, np.nan)

    # --- 5. 价格变动速度与加速度 ---
    df["price_velocity"] = close.diff(1)  # 一阶差分
    df["price_acceleration"] = df["price_velocity"].diff(1)  # 二阶差分

    # 多窗口变动
    for label, w in WINDOWS_10S.items():
        df[f"price_change_{label}"] = close.pct_change(w)

    exact_5s = _microstructure_5s_from_1s(bars_1s).reindex(df.index)
    for column in exact_5s.columns:
        df[column] = exact_5s[column]

    # --- 6. 大单比例 ---
    # 用 volume/trade_count 作为平均单笔量的代理
    avg_size = (df["volume"] / df["trade_count"].replace(0, np.nan)).fillna(0.0)
    df["avg_trade_size"] = avg_size

    # 滚动平均单笔量（用于判断当前是否偏大）
    df["avg_trade_size_60s"] = avg_size.rolling(
        WINDOWS_10S["60s"], min_periods=1
    ).mean()
    df["large_trade_ratio"] = (
        avg_size / df["avg_trade_size_60s"].replace(0, np.nan)
    ).fillna(1.0)

    # --- 7. 价格冲击 (高量bar前后的价格变动) ---
    volume_window = df["volume"].rolling(30, min_periods=3)
    vol_z = (df["volume"] - volume_window.mean()) / \
            volume_window.std(ddof=0).replace(0, np.nan)
    df["volume_zscore"] = vol_z.fillna(0.0)

    # --- 额外: spread代理 (high - low) ---
    df["bar_spread"] = df["high"] - df["low"]
    df["bar_spread_bps"] = df["bar_spread"] / df["close"] * 10000  # basis points

    # --- 额外: 收益率 ---
    df["return_1"] = log_ret
    df["return_abs"] = log_ret.abs()

    # --- 8. Vol regime (trailing realized vol percentile) ---
    # 24h rolling vol (2160 x 10s = 6h, 8640 = 24h)
    ret_abs = log_ret.abs()
    df["vol_regime_6h"] = ret_abs.rolling(2160, min_periods=360).mean()
    df["vol_regime_24h"] = ret_abs.rolling(8640, min_periods=2160).mean()
    # zscore of 6h vol vs 7-day history
    vol6h = df["vol_regime_6h"]
    vol6h_mean = vol6h.rolling(60480, min_periods=8640).mean()  # 7d
    vol6h_std = vol6h.rolling(60480, min_periods=8640).std()
    df["vol_regime_zscore"] = (vol6h - vol6h_mean) / vol6h_std.replace(0, np.nan)

    return df


# ============================================================
#  B. 时间特征
# ============================================================

def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """添加时间维度特征。

    日历/时区/session 口径统一走 calendar_features.py。这里保留旧字段名
    hour_sin/is_us_regular_hours 等，避免主模型历史 feature schema 断裂；
    同时额外写入 cal_* 字段，供后续 calendar-aware 训练显式选择。
    """
    df = add_calendar_features(df, prefix="cal_", include_legacy=True)
    ts = df.index  # DatetimeIndex (UTC)
    utc_hour = ts.hour

    # --- 4. 距下次资金费率结算的时间 ---
    # Binance USDT-M 资金费率结算: 00:00, 08:00, 16:00 UTC
    minutes_in_day = utc_hour * 60 + ts.minute
    # 结算时刻: 0, 480, 960 分钟
    funding_times = np.array([0, 480, 960, 1440])  # 加1440方便计算
    minutes_arr = minutes_in_day.values.astype(np.float64)

    # 距下次结算的分钟数
    time_to_funding = np.empty(len(minutes_arr))
    for i, m in enumerate(minutes_arr):
        diffs = funding_times - m
        diffs = diffs[diffs > 0]
        time_to_funding[i] = diffs[0] if len(diffs) > 0 else funding_times[0] + 1440 - m

    df["minutes_to_funding"] = time_to_funding
    # 归一化到 [0, 1]（最大480分钟）
    df["funding_phase"] = time_to_funding / 480.0
    # sin/cos编码（每8小时一个周期）
    df["funding_sin"] = np.sin(2 * math.pi * (1 - time_to_funding / 480.0))
    df["funding_cos"] = np.cos(2 * math.pi * (1 - time_to_funding / 480.0))

    # --- 5. 距最近整点/半点的分钟数 ---
    minute_of_hour = ts.minute + ts.second / 60.0
    dist_to_hour = np.minimum(minute_of_hour, 60 - minute_of_hour)
    dist_to_half = np.abs(minute_of_hour - 30)
    df["dist_to_hour"] = np.minimum(dist_to_hour, dist_to_half)
    df["near_candle_close"] = (df["dist_to_hour"] < 2).astype(np.int8)  # 2分钟内
    return df


# ============================================================
#  标签 & 权重
# ============================================================

def add_labels(df: pd.DataFrame, bars_1s: pd.DataFrame,
               symbol: str = DEFAULT_SYMBOL,
               config_path: Optional[Path] = None) -> pd.DataFrame:
    """Add fill-conditioned markout/toxicity labels and future dollar-variance labels."""
    ts_1s_ns, close_1s, high_1s, low_1s, diff_1s, sigma_sq_1s = _prepare_1s_label_context(bars_1s)
    quote_params = _load_label_quote_params(symbol, config_path=config_path)

    quote_time_ns = _as_utc_index(df.index).as_unit("ns").asi8 + RESAMPLE_SEC * _TS_NS
    start_idx = np.searchsorted(ts_1s_ns, quote_time_ns, side="left")
    last_hist_idx = np.clip(start_idx - 1, 0, len(sigma_sq_1s) - 1)
    sigma_sq_now = sigma_sq_1s[last_hist_idx]
    close_ref = df["close"].to_numpy(dtype=np.float64)
    half_spread = _quote_half_spread(df, close_ref, sigma_sq_now, quote_params)
    bid_quote = close_ref - half_spread
    ask_quote = close_ref + half_spread

    for h in LABEL_HORIZONS:
        ret_label, dir_label, vol_label = _compute_label_triplet(
            ts_1s_ns,
            close_1s,
            high_1s,
            low_1s,
            diff_1s,
            quote_time_ns,
            start_idx,
            bid_quote,
            ask_quote,
            close_ref,
            h * _TS_NS,
        )
        valid_horizon = mask_valid_horizon(
            quote_time_ns,
            horizon_s=2 * h,
            max_gap_s=LABEL_GRID_MAX_GAP_S,
        )
        # label 必须在连续 10s 网格内完整覆盖 horizon；坏日/长 gap 后的未来价格不参与训练。
        ret_label[~valid_horizon] = np.nan
        dir_label[~valid_horizon] = np.nan
        vol_label[~valid_horizon] = np.nan

        df[f"label_ret_{h}s"] = ret_label
        df[f"label_dir_{h}s"] = dir_label
        df[f"label_vol_{h}s"] = vol_label

        coverage = float(np.mean(~np.isnan(ret_label))) if len(ret_label) else 0.0
        print(
            f"  label_{h}s: fill-conditioned coverage={coverage:.1%}, "
            f"vol_median={np.nanmedian(vol_label):.4f}"
        )

    for h in TOXICITY_HORIZONS:
        tox_bid, tox_ask = _compute_toxicity_pair(
            ts_1s_ns,
            close_1s,
            high_1s,
            low_1s,
            quote_time_ns,
            start_idx,
            bid_quote,
            ask_quote,
            h * _TS_NS,
        )
        valid_horizon = mask_valid_horizon(
            quote_time_ns,
            horizon_s=2 * h,
            max_gap_s=LABEL_GRID_MAX_GAP_S,
        )
        tox_bid[~valid_horizon] = np.nan
        tox_ask[~valid_horizon] = np.nan

        df[f"label_tox_bid_{h}s"] = tox_bid
        df[f"label_tox_ask_{h}s"] = tox_ask

        bid_cov = float(np.mean(~np.isnan(tox_bid))) if len(tox_bid) else 0.0
        ask_cov = float(np.mean(~np.isnan(tox_ask))) if len(tox_ask) else 0.0
        print(
            f"  toxicity_{h}s: bid_coverage={bid_cov:.1%}, "
            f"ask_coverage={ask_cov:.1%}"
        )

    return df


def add_sample_weights(
    df: pd.DataFrame,
    reference_date: Optional[object] = None,
    lam: float = 0.1,
) -> pd.DataFrame:
    """添加时间衰减权重: w = exp(-λ * days_ago / 30.44)"""
    if lam < 0.0:
        raise ValueError("sample-weight lambda must be non-negative")
    index = pd.to_datetime(df.index, utc=True, errors="coerce")
    if index.isna().any():
        raise ValueError("sample weights require a finite UTC DatetimeIndex")
    if reference_date is None:
        if len(index) == 0:
            df["sample_weight"] = np.empty(0, dtype=np.float64)
            return df
        ref = index.max()
    else:
        ref = pd.Timestamp(reference_date)
        ref = ref.tz_localize("UTC") if ref.tzinfo is None else ref.tz_convert("UTC")
    days_ago = np.maximum(
        (ref - index).total_seconds().to_numpy(dtype=np.float64) / 86_400.0,
        0.0,
    )
    df["sample_weight"] = np.exp(-lam * days_ago / 30.44)
    return df


# ============================================================
#  主流程
# ============================================================

def process_day(bars_1s: pd.DataFrame, day_tag: str, symbol: str,
                config_path: Optional[Path] = None,
                market_stage: str = "minimal",
                reference_symbol: Optional[str] = None,
                calendar_tag: Optional[str] = None,
                output_day_tag: Optional[str] = None,
                sample_weight_reference_date: Optional[object] = None,
                sample_weight_lambda: float = 0.1,
                require_execution_l2: bool = False,
                require_taker_tempo: bool = False,
                include_labels: bool = True) -> pd.DataFrame:
    """处理单个 UTC 日数据：1s bars → 10s特征。"""
    data_lookup_tag = day_tag
    qprint("  补齐 dense 1s UTC 主时间轴...")
    sparse_rows = len(bars_1s)
    bars_1s = densify_bars_1s(
        bars_1s,
        calendar_tag=calendar_tag,
        ensure_through_day_tag=output_day_tag,
    )
    qprint(f"  dense 1s: {sparse_rows:,} → {len(bars_1s):,} 行")

    # --- tick-by-tick momentum (在1s精度上计算，然后采样到10s) ---
    qprint("  计算 tick momentum 特征 (1s → 10s)...")
    tick_feats = compute_tick_momentum(bars_1s)
    qprint(f"  tick momentum: {len(tick_feats):,} 行, {len(tick_feats.columns)} 列")

    qprint("  重采样 → 10s bars...")
    bars_10s = resample_to_10s(bars_1s)
    qprint(f"  {len(bars_10s):,} 个10s bars")

    # --- merge tick momentum ---
    bars_10s = bars_10s.join(tick_feats, how="left")

    qprint("  对齐 raw taker tempo 特征...")
    bars_10s = add_taker_tempo_features(
        bars_10s,
        symbol,
        data_lookup_tag,
        require_taker_tempo=require_taker_tempo,
    )
    qprint(f"  taker tempo 特征: {len(TAKER_TEMPO_FEATURE_COLS)} 列")

    qprint("  计算 execution-L2 特征...")
    bars_10s = add_execution_l2_features(
        bars_10s,
        bars_1s.index,
        data_lookup_tag,
        symbol,
        config_path=config_path,
        require_l2=require_execution_l2,
    )
    qprint(f"  execution-L2 特征: {len(EXECUTION_L2_FEATURE_COLS)} 列")

    # --- metrics features ---
    metrics = load_metrics(data_lookup_tag, symbol)
    if metrics is not None:
        qprint("  计算 metrics 特征...")
        bars_10s = add_metrics_features(bars_10s, metrics)
        n_metrics = sum(1 for c in bars_10s.columns
                        if any(x in c for x in ["oi_", "ls_ratio", "taker_ls", "crowd_ls", "toptrader_ls"]))
        qprint(f"  metrics 特征: {n_metrics} 列")

    qprint("  计算微结构特征...")
    bars_10s = add_microstructure_features(bars_10s, bars_1s)

    # Local rolling features may use a causal multi-day warmup, while all
    # target-day external/L2/label joins remain scoped to day_tag.
    if output_day_tag:
        target_start = pd.Timestamp(output_day_tag, tz="UTC")
        target_end = target_start + pd.Timedelta(days=1)
        bars_10s = bars_10s.loc[
            (bars_10s.index >= target_start) & (bars_10s.index < target_end)
        ].copy()

    qprint(f"  计算 cross-market 特征 ({market_stage})...")
    bars_10s = add_cross_market_features(
        bars_10s, symbol, day_tag,
        market_stage=market_stage,
        reference_symbol=reference_symbol,
        data_tag=data_lookup_tag,
    )

    qprint("  计算时间特征...")
    bars_10s = add_time_features(bars_10s)

    if include_labels:
        qprint("  计算标签...")
        bars_10s = add_labels(
            bars_10s,
            bars_1s,
            symbol=symbol,
            config_path=config_path,
        )

    qprint("  计算样本权重...")
    bars_10s = add_sample_weights(
        bars_10s,
        reference_date=sample_weight_reference_date,
        lam=sample_weight_lambda,
    )

    return bars_10s


def split_on_calendar_day_gaps(frame: pd.DataFrame, max_gap_s: Optional[float] = None) -> list[pd.DataFrame]:
    """Split sorted UTC data into independently processed chunks.

    max_gap_s=None 只按缺失 UTC 日切；传入 max_gap_s 时按连续性 segment 切，
    用于阻止 rolling feature 跨数据缺口。
    """
    if frame.empty:
        return []
    ordered = frame.copy()
    ordered.index = _as_utc_index(ordered.index)
    ordered.sort_index(inplace=True)
    if max_gap_s is None:
        day_ord = (
            ordered.index.normalize().as_unit("ns").asi8 // 86_400_000_000_000
        ).astype(np.int64)
        breaks = np.flatnonzero(np.diff(day_ord) > 1) + 1
    else:
        segments = continuous_segment_ids(ordered.index, max_gap_s=max_gap_s)
        breaks = np.flatnonzero(np.diff(segments) > 0) + 1
    starts = np.r_[0, breaks]
    ends = np.r_[breaks, len(ordered)]
    return [
        ordered.iloc[start:end]
        for start, end in zip(starts, ends, strict=True)
        if end > start
    ]


def _assert_unique_time_index(frame: pd.DataFrame, label: str) -> None:
    if frame.index.is_unique:
        return
    duplicate_rows = int(frame.index.duplicated(keep=False).sum())
    sample = pd.Index(frame.index[frame.index.duplicated(keep=False)]).unique()[:3].tolist()
    raise ValueError(
        f"{label}: duplicate timestamps are not valid feature data "
        f"({duplicate_rows:,} rows; sample={sample})"
    )


def _contiguous_warmup_paths(
    target_tag: str,
    paths_by_day: dict[str, Path],
    *,
    warmup_days: int,
) -> list[Path]:
    """Return target plus only its immediately contiguous causal history."""
    target = pd.Timestamp(target_tag)
    selected = [paths_by_day[target_tag]]
    for offset in range(1, max(0, int(warmup_days)) + 1):
        tag = (target - pd.Timedelta(days=offset)).strftime("%Y-%m-%d")
        path = paths_by_day.get(tag)
        if path is None:
            break
        selected.append(path)
    return list(reversed(selected))


def _write_feature_manifest(out_dir: Path, symbol: str) -> Path:
    manifest_dir = DATA_DIR / "reports" / "feature_manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    path = manifest_dir / f"features_before_force_{stamp}_{symbol.lower()}.json"
    files = []
    for feature_path in sorted(out_dir.glob("*.parquet")):
        stat = feature_path.stat()
        files.append({
            "path": str(feature_path),
            "size_bytes": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
        })
    path.write_text(json.dumps({
        "symbol": symbol,
        "created_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "files": files,
    }, indent=2), encoding="utf-8")
    return path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_causal_feature_manifest(
    out_dir: Path,
    *,
    symbol: str,
    feature_paths: list[tuple[str, Path]],
    warmup_days: int,
    market_stage: str,
    reference_symbol: str,
    config_path: Optional[Path],
    split: dict[str, list[str]],
    sample_weight_reference_date: str,
    sample_weight_lambda: float,
    require_execution_l2: bool,
    require_taker_tempo: bool,
    labels_materialized: bool = True,
) -> Path:
    """Bind a versioned feature panel to code, config, and daily content."""
    volatility_unit_contract = absolute_price_variance_unit_contract(symbol)
    quote_params = _load_label_quote_params(symbol, config_path=config_path)
    daily_files = []
    manifest_digest = hashlib.sha256()
    for day, feature_path in sorted(feature_paths):
        content_sha256 = _sha256_file(feature_path)
        stat = feature_path.stat()
        row = {
            "day": day,
            "file": feature_path.name,
            "size_bytes": int(stat.st_size),
            "sha256": content_sha256,
        }
        daily_files.append(row)
        manifest_digest.update(
            f"{day}\0{feature_path.name}\0{stat.st_size}\0{content_sha256}\n".encode()
        )
    derived_datasets = []
    for dataset_path in sorted(out_dir.glob("dataset_*.parquet")):
        stat = dataset_path.stat()
        derived_datasets.append(
            {"file": dataset_path.name, "size_bytes": int(stat.st_size)}
        )
    generator_path = Path(__file__).resolve()
    execution_l2_source: dict[str, object] = {
        "required": bool(require_execution_l2),
    }
    taker_tempo_source: dict[str, object] = {
        "required": bool(require_taker_tempo),
        "root": str(TRADE_FEATURE_DIR),
    }
    tempo_manifest_path = TRADE_FEATURE_DIR / "manifest.json"
    taker_tempo_source["manifest_path"] = (
        str(tempo_manifest_path) if tempo_manifest_path.is_file() else ""
    )
    taker_tempo_source["manifest_sha256"] = (
        _sha256_file(tempo_manifest_path)
        if tempo_manifest_path.is_file()
        else ""
    )
    tempo_digest = hashlib.sha256()
    tempo_files: list[dict[str, object]] = []
    for day, _ in sorted(feature_paths):
        path = TRADE_FEATURE_DIR / symbol / f"{symbol}-trade-tempo-{day}.parquet"
        if not path.is_file():
            if require_taker_tempo:
                raise RuntimeError(
                    f"required taker-tempo sidecar is missing: {path}"
                )
            continue
        content_sha256 = _sha256_file(path)
        size_bytes = int(path.stat().st_size)
        tempo_files.append(
            {
                "day": day,
                "file": path.name,
                "size_bytes": size_bytes,
                "sha256": content_sha256,
            }
        )
        tempo_digest.update(
            f"{day}\0{path.name}\0{size_bytes}\0{content_sha256}\n".encode()
        )
    taker_tempo_source.update(
        {
            "daily_file_count": len(tempo_files),
            "daily_manifest_sha256": tempo_digest.hexdigest(),
            "daily_files": tempo_files,
        }
    )
    if normalize_symbol(symbol) == "BTCUSDC":
        bbo_dir, l2_dir = _book_dirs_for_symbol(symbol)
        if bbo_dir.parent != l2_dir.parent:
            raise RuntimeError(
                "execution BBO/L2 must share one versioned dataset root: "
                f"bbo={bbo_dir} l2={l2_dir}"
            )
        dataset_root = bbo_dir.parent
        dataset_manifest = dataset_root / "manifest.json"
        daily_quality = dataset_root / "daily_quality.csv"
        if require_execution_l2 and (
            not dataset_manifest.is_file() or not daily_quality.is_file()
        ):
            raise RuntimeError(
                "required normalized L2 contract is incomplete: "
                f"{dataset_root}"
            )
        execution_l2_source.update(
            {
                "dataset_root": str(dataset_root),
                "bbo_dir": str(bbo_dir),
                "l2_dir": str(l2_dir),
                "manifest_path": str(dataset_manifest),
                "manifest_sha256": (
                    _sha256_file(dataset_manifest)
                    if dataset_manifest.is_file()
                    else ""
                ),
                "daily_quality_path": str(daily_quality),
                "daily_quality_sha256": (
                    _sha256_file(daily_quality)
                    if daily_quality.is_file()
                    else ""
                ),
            }
        )
        if daily_quality.is_file():
            quality = pd.read_csv(daily_quality, dtype={"day": str})
            quality_by_day = quality.set_index("day", drop=False)
            selected_days = [day for day, _ in sorted(feature_paths)]
            missing_days = sorted(set(selected_days) - set(quality_by_day.index))
            if missing_days:
                raise RuntimeError(
                    "normalized L2 quality contract is missing feature days: "
                    + ", ".join(missing_days[:5])
                )
            selected_quality = quality_by_day.loc[selected_days]
            if "source_authority" in selected_quality.columns:
                source_authority_counts = {
                    str(authority): int(count)
                    for authority, count in selected_quality[
                        "source_authority"
                    ].value_counts().items()
                }
            else:
                source_authority_counts = {
                    "legacy_unspecified": len(selected_quality)
                }
            invalid_days = selected_quality.loc[
                ~selected_quality["cadence_schema_valid"].astype(bool),
                "day",
            ].tolist()
            if require_execution_l2 and invalid_days:
                raise RuntimeError(
                    "required normalized L2 cadence/schema failed for: "
                    + ", ".join(invalid_days[:5])
                )
            execution_l2_source.update(
                {
                    "selected_day_count": len(selected_days),
                    "selected_days": selected_days,
                    "source_authority_counts": source_authority_counts,
                    "cadence_schema_valid_days": int(
                        selected_quality["cadence_schema_valid"].astype(bool).sum()
                    ),
                    "coverage_99_valid_days": int(
                        selected_quality["coverage_99_valid"].astype(bool).sum()
                    ),
                    "formal_eligible_days": int(
                        selected_quality["formal_eligible"].astype(bool).sum()
                    ),
                    "provider_sensitivity_replay_eligible_days": (
                        int(
                            selected_quality[
                                "provider_sensitivity_replay_eligible"
                            ]
                            .astype(bool)
                            .sum()
                        )
                        if "provider_sensitivity_replay_eligible"
                        in selected_quality.columns
                        else 0
                    ),
                    "exact_queue_policy_eligible_days": (
                        int(
                            selected_quality["exact_queue_policy_eligible"]
                            .astype(bool)
                            .sum()
                        )
                        if "exact_queue_policy_eligible"
                        in selected_quality.columns
                        else 0
                    ),
                }
            )
    payload = {
        "schema_version": 3,
        "symbol": symbol,
        "created_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "output_dir": str(out_dir),
        "generator": str(generator_path),
        "generator_sha256": _sha256_file(generator_path),
        "config_path": str(config_path) if config_path is not None else "",
        "config_sha256": (
            _sha256_file(config_path)
            if config_path is not None and config_path.exists()
            else ""
        ),
        "label_quote_calibration": {
            "path": quote_params["fill_probability_model_path"],
            "sha256": quote_params["fill_probability_sha256"],
            "schema_version": quote_params["fill_probability_schema_version"],
            "model_type": quote_params["fill_probability_model_type"],
            "p3_delta_star": quote_params["p3_delta_star"],
            "p3_kappa_eff": quote_params["p3_kappa_eff"],
        },
        "label_quote_policy": {
            "a_spread": quote_params["a_spread"],
            "quote_horizon_s": quote_params["quote_horizon_s"],
            "kappa_ratio": quote_params["kappa_ratio"],
            "dynamic_cap_enabled": quote_params["dynamic_cap_enabled"],
            "dynamic_cap_base_bps": quote_params["dynamic_cap_base_bps"],
            "dynamic_cap_alpha": quote_params["dynamic_cap_alpha"],
            "dynamic_cap_min_mult": quote_params["dynamic_cap_min_mult"],
            "dynamic_cap_max_mult": quote_params["dynamic_cap_max_mult"],
            "dynamic_cap_var_baseline": quote_params["dynamic_cap_var_baseline"],
        },
        "feature_timestamp_semantics": "left_label_bucket_end",
        "feature_bucket_ms": 10_000,
        "feature_ready_offset_ms": 10_000,
        "feature_semantics_version": 6,
        "feature_dag_id": TEN_SECOND_CAUSAL_GRAPH.graph_id,
        "feature_dag_sha256": TEN_SECOND_CAUSAL_GRAPH.sha256(),
        "feature_cutoff_semantics": "strict_exclusive_completed_bucket_end",
        "calendar_timestamp_semantics": (
            "preserve_datetime_physical_unit_ms_us_ns_before_epoch_conversion"
        ),
        "microstructure_5s_semantics": (
            "trailing_five_seconds_from_causal_left_labelled_1s_bars"
        ),
        "label_semantics_version": 3,
        "labels_materialized": bool(labels_materialized),
        "label_window_semantics": "left_closed_right_open_[t,t+h)",
        "label_return_direction_semantics": (
            "fill within h, then maker markout h after fill; decision outcome spans h..2h"
        ),
        "label_volatility_semantics": "fixed forward-h absolute-price variance",
        "label_volatility_units": volatility_unit_contract["variance_units"],
        "volatility_unit_contract": volatility_unit_contract,
        "warmup_days_requested": int(warmup_days),
        "warmup_policy": "immediately_contiguous_prior_utc_days_stop_at_first_gap",
        "market_stage": str(market_stage),
        "reference_symbol": str(reference_symbol),
        "split_mode": "chronological_good_day_with_embargo",
        "split": split,
        "sample_weight": {
            "formula": "exp(-lambda * days_ago / 30.44)",
            "lambda": float(sample_weight_lambda),
            "reference_date": str(sample_weight_reference_date),
        },
        "execution_l2_source": execution_l2_source,
        "taker_tempo_source": taker_tempo_source,
        "daily_file_count": len(daily_files),
        "daily_manifest_sha256": manifest_digest.hexdigest(),
        "daily_files": daily_files,
        "derived_datasets": derived_datasets,
    }
    path = out_dir / "causal_feature_manifest.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def chronological_good_day_split(
    tags: list[str],
    *,
    validation_days: Optional[int] = None,
    test_days: Optional[int] = None,
    embargo_good_days: int = 1,
) -> dict[str, list[str]]:
    """Split retained UTC days without allowing future days into training."""
    ordered = sorted({tag for tag in tags if _is_day_tag(tag)})
    n_days = len(ordered)
    embargo = max(0, int(embargo_good_days))
    default_eval_days = max(5, int(round(n_days * 0.20)))
    n_val = default_eval_days if validation_days is None else max(1, int(validation_days))
    n_test = default_eval_days if test_days is None else max(1, int(test_days))
    required = n_val + n_test + 2 * embargo + 1
    if n_days < required:
        raise ValueError(
            "not enough retained days for chronological split: "
            f"have={n_days}, need_at_least={required}"
        )
    test_start = n_days - n_test
    embargo_2_start = test_start - embargo
    val_start = embargo_2_start - n_val
    embargo_1_start = val_start - embargo
    return {
        "train": ordered[:embargo_1_start],
        "embargo_1": ordered[embargo_1_start:val_start],
        "validation": ordered[val_start:embargo_2_start],
        "embargo_2": ordered[embargo_2_start:test_start],
        "test": ordered[test_start:],
    }


def main():
    parser = argparse.ArgumentParser(description="特征工程: 1s bars → 10s 特征矩阵")
    parser.add_argument("--symbol", type=str, default=DEFAULT_SYMBOL,
                        help=f"交易对 (默认 {DEFAULT_SYMBOL}; 也可用 MM_SYMBOL 覆盖)")
    parser.add_argument("--file", type=str, default=None,
                        help="只处理名称包含此字符串的文件")
    parser.add_argument(
        "--days-file",
        type=Path,
        default=None,
        help=(
            "只处理 CSV day 列中的精确 UTC 日；用于绑定冻结 good-day "
            "universe，不能与 --file 同时使用"
        ),
    )
    parser.add_argument("--config", type=str, default=str(LIVE_CONFIG_PATH),
                        help=f"用于参考 quote 的 live config 路径 (默认 {LIVE_CONFIG_PATH})")
    parser.add_argument("--market-stage", choices=["single", "minimal", "enhanced", "full"],
                        default=None,
                        help="cross-market 特征阶段 (默认读取 config；未显式开启时为 single)")
    parser.add_argument("--reference-symbol", type=str, default=None,
                        help="参考永续交易对 (默认读取 config，否则自动 BTCUSDT/BTCUSDC 互为参考)")
    parser.add_argument("--lambda", type=float, default=0.1, dest="lam",
                        help="时间衰减系数 (默认0.1)")
    parser.add_argument(
        "--weight-reference-date",
        default=None,
        help=(
            "样本衰减参考 UTC 日期；默认使用本地可用输入中的最新日期。"
            "该值和 --lambda 会写入 causal manifest。"
        ),
    )
    parser.add_argument("--force", action="store_true",
                        help="强制重建已有日度特征parquet")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="版本化特征输出目录；默认使用 symbol data root 下的 canonical 目录",
    )
    parser.add_argument(
        "--warmup-days",
        type=int,
        default=7,
        help="目标日前连续因果 warmup 天数；遇到缺日立即截断 (默认7)",
    )
    parser.add_argument(
        "--validation-days",
        type=int,
        default=None,
        help="chronological validation good-day count (default: 20%% of retained days)",
    )
    parser.add_argument(
        "--test-days",
        type=int,
        default=None,
        help="chronological test good-day count (default: 20%% of retained days)",
    )
    parser.add_argument(
        "--embargo-good-days",
        type=int,
        default=1,
        help="retained good days excluded between train/validation/test (default: 1)",
    )
    parser.add_argument(
        "--require-execution-l2",
        action="store_true",
        help=(
            "fail instead of zero-filling when the versioned execution-L2 "
            "input is missing or invalid; bind its manifest and quality hashes"
        ),
    )
    parser.add_argument(
        "--require-taker-tempo",
        action="store_true",
        help=(
            "fail instead of zero-filling when the versioned taker-tempo "
            "sidecar is missing; bind every daily sidecar hash"
        ),
    )
    parser.add_argument(
        "--features-only",
        action="store_true",
        help=(
            "materialize causal inference features without computing any future "
            "direction, return, volatility, or toxicity labels"
        ),
    )
    parser.add_argument("--verbose", action="store_true",
                        help="逐日输出特征工程细节和完整特征列")
    args = parser.parse_args()
    if args.file and args.days_file is not None:
        raise SystemExit("--file and --days-file are mutually exclusive")
    selected_day_whitelist: set[str] | None = None
    if args.days_file is not None:
        try:
            selected_day_whitelist = set(_read_days_file(args.days_file))
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
    global VERBOSE
    VERBOSE = bool(args.verbose)
    symbol = normalize_symbol(args.symbol)
    out_dir = (
        args.output_dir.expanduser().resolve()
        if args.output_dir is not None
        else feature_out_dir(symbol)
    )
    config_path = Path(args.config).expanduser().resolve() if args.config else None
    cfg_stage, cfg_reference = _load_multi_market_defaults(config_path)
    market_stage = args.market_stage or cfg_stage
    reference_symbol = normalize_symbol(
        args.reference_symbol or cfg_reference,
        default_reference_symbol(symbol),
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    if args.force:
        manifest_path = _write_feature_manifest(out_dir, symbol)
        print(f"Feature manifest saved: {manifest_path}")

    # 加载1s bars
    files = sorted(BARS_DIR.glob(f"{symbol}-1s-*.parquet"))
    if not files:
        print("错误：未找到1s bar文件")
        sys.exit(1)

    files = [f for f in files if f.stem.replace(f"{symbol}-1s-", "") >= MIN_DATA_DAY]
    files = filter_paths_for_orderbook_quality(files, symbol, label="1s bar")
    paths_by_day = {
        f.stem.replace(f"{symbol}-1s-", ""): f
        for f in files
        if _is_day_tag(f.stem.replace(f"{symbol}-1s-", ""))
    }
    if not paths_by_day:
        print("错误：未找到带有效日期的1s bar文件")
        sys.exit(1)
    sample_weight_reference_date = (
        str(args.weight_reference_date)
        if args.weight_reference_date
        else max(paths_by_day)
    )
    if args.file:
        files = [f for f in files if args.file in f.name]
    elif selected_day_whitelist is not None:
        files = [
            f
            for f in files
            if f.stem.replace(f"{symbol}-1s-", "") in selected_day_whitelist
        ]
        observed = {
            f.stem.replace(f"{symbol}-1s-", "")
            for f in files
        }
        missing = sorted(selected_day_whitelist - observed)
        if missing:
            raise SystemExit(
                "frozen days file references missing 1s bars: "
                + ", ".join(missing[:10])
            )

    if not files:
        print(f"错误：未找到 {MIN_DATA_DAY} 及之后的1s bar文件")
        sys.exit(1)

    # 逐日处理，并只保留 parquet 路径，避免全量特征常驻内存。
    feature_paths = []
    sample_columns = None
    for f in files:
        tag = f.stem.replace(f"{symbol}-1s-", "")  # e.g. "2026-03-01"
        if not _is_day_tag(tag):
            qprint(f"[SKIP] {f.name}: non-daily bar container is not part of the daily feature pipeline")
            continue
        out_path = out_dir / f"features_{tag}.parquet"

        if out_path.exists() and not args.force:
            qprint(f"[SKIP] {out_path.name} 已存在")
            feature_paths.append((tag, out_path))
            continue

        qprint(f"\n[...] 处理 {f.name}")
        warmup_paths = _contiguous_warmup_paths(
            tag,
            paths_by_day,
            warmup_days=args.warmup_days,
        )
        bars_1s = pd.concat(
            [pd.read_parquet(path) for path in warmup_paths]
        ).sort_index()
        bars_1s = filter_frame_for_orderbook_quality(bars_1s, symbol, label="1s bar")
        qprint(
            f"  加载: {len(bars_1s):,} 个1s bars; "
            f"causal warmup={len(warmup_paths) - 1}d"
        )
        if bars_1s.empty:
            qprint("  [SKIP] 数据质量过滤后无可用1s bars")
            continue

        feature_chunks = []
        chunks = split_on_calendar_day_gaps(bars_1s, max_gap_s=FEATURE_SOURCE_MAX_GAP_S)
        if len(chunks) > 1:
            qprint(f"  数据质量过滤后拆分为 {len(chunks)} 个连续区间，避免 rolling 跨缺口")
        for chunk in chunks:
            feature_chunks.append(process_day(
                chunk, tag, symbol,
                config_path=config_path,
                market_stage=market_stage,
                reference_symbol=reference_symbol,
                calendar_tag=(
                    tag if len(chunks) == 1 and len(warmup_paths) == 1 else None
                ),
                output_day_tag=tag,
                sample_weight_reference_date=sample_weight_reference_date,
                sample_weight_lambda=args.lam,
                require_execution_l2=args.require_execution_l2,
                require_taker_tempo=args.require_taker_tempo,
                include_labels=not args.features_only,
            ))
        features = pd.concat(feature_chunks).sort_index()
        features = filter_frame_for_orderbook_quality(features, symbol, label="feature")
        _assert_unique_time_index(features, f"features_{tag}")

        # 删除全NaN行（开头的warmup期）
        features.dropna(subset=["volatility_60s"], inplace=True)

        features.to_parquet(out_path, engine="pyarrow")
        size_mb = out_path.stat().st_size / 1e6
        qprint(f"[OK]  {out_path.name}: {len(features):,} 行, {size_mb:.1f} MB")
        if sample_columns is None:
            sample_columns = list(features.columns)
        feature_paths.append((tag, out_path))

        # 释放内存
        del bars_1s, features
        gc.collect()

    # 按时间划分数据集
    print(f"特征工程: {len(files)} 个输入文件, 新建/覆盖 {sum(1 for _, p in feature_paths if p.exists())} 个可用特征文件")
    print("划分训练/验证/测试集...")

    daily_tags = [t for t, _ in feature_paths if _is_day_tag(t)]
    if args.features_only:
        split = {"inference": sorted(daily_tags)}
        causal_manifest_path = write_causal_feature_manifest(
            out_dir,
            symbol=symbol,
            feature_paths=feature_paths,
            warmup_days=args.warmup_days,
            market_stage=market_stage,
            reference_symbol=reference_symbol,
            config_path=config_path,
            split=split,
            sample_weight_reference_date=sample_weight_reference_date,
            sample_weight_lambda=args.lam,
            require_execution_l2=args.require_execution_l2,
            require_taker_tempo=args.require_taker_tempo,
            labels_materialized=False,
        )
        print(f"Causal feature manifest: {causal_manifest_path}")
        print(
            "features-only inference panel: "
            f"days={len(daily_tags)}, labels_materialized=false"
        )
        return
    # Incremental good-day extension commonly targets one date with --file.
    # The daily parquet is already complete at this point; a chronological
    # dataset split only makes sense when enough distinct days were requested.
    min_split_days = 1 + max(0, int(args.embargo_good_days)) * 2 + 5 + 5
    if len(daily_tags) < min_split_days:
        print(
            "  chronological split skipped: "
            f"have={len(daily_tags)} daily feature file(s), need_at_least={min_split_days}; "
            "incremental daily outputs are valid"
        )
        return
    split = chronological_good_day_split(
        daily_tags,
        validation_days=args.validation_days,
        test_days=args.test_days,
        embargo_good_days=args.embargo_good_days,
    )
    train_tags = split["train"]
    val_tags = split["validation"]
    test_tags = split["test"]
    print(
        "  chronological split: "
        f"train={len(train_tags)}, embargo1={len(split['embargo_1'])}, "
        f"validation={len(val_tags)}, embargo2={len(split['embargo_2'])}, "
        f"test={len(test_tags)}"
    )

    tag_to_path = dict(feature_paths)

    def concat_save(tags, name):
        if not tags:
            print(f"  {name}: 无数据")
            return
        out = out_dir / f"dataset_{name}.parquet"
        paths = [tag_to_path[t] for t in sorted(tags)]
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            writer = None
            total_rows = 0
            start_ts = None
            end_ts = None
            try:
                for path in paths:
                    df = pd.read_parquet(path)
                    df.sort_index(inplace=True)
                    df.index = pd.DatetimeIndex(df.index).as_unit("ns")
                    df = filter_frame_for_orderbook_quality(df, symbol, label=f"dataset_{name}")
                    _assert_unique_time_index(df, path.name)
                    df = add_sample_weights(
                        df,
                        reference_date=sample_weight_reference_date,
                        lam=args.lam,
                    )
                    if len(df) == 0:
                        continue
                    if start_ts is None:
                        start_ts = df.index[0]
                    end_ts = df.index[-1]
                    total_rows += len(df)
                    table = pa.Table.from_pandas(df, preserve_index=True)
                    if writer is None:
                        writer = pq.ParquetWriter(out, table.schema)
                    writer.write_table(table)
                    del df, table
                    gc.collect()
            finally:
                if writer is not None:
                    writer.close()
            if total_rows == 0:
                print(f"  {name}: 无数据")
                return
            print(f"  {name}: {total_rows:,} 行, "
                  f"{out.stat().st_size / 1e6:.1f} MB, "
                  f"时间范围 {start_ts} → {end_ts}")
        except Exception as exc:
            print(f"  [WARN] 流式写入失败，回退到 pandas concat: {exc}")
            dfs = [pd.read_parquet(path) for path in paths]
            combined = pd.concat(dfs)
            combined.sort_index(inplace=True)
            combined.index = pd.DatetimeIndex(combined.index).as_unit("ns")
            combined = filter_frame_for_orderbook_quality(combined, symbol, label=f"dataset_{name}")
            _assert_unique_time_index(combined, f"dataset_{name}")
            combined = add_sample_weights(
                combined,
                reference_date=sample_weight_reference_date,
                lam=args.lam,
            )
            combined.to_parquet(out, engine="pyarrow")
            print(f"  {name}: {len(combined):,} 行, "
                  f"{out.stat().st_size / 1e6:.1f} MB, "
                  f"时间范围 {combined.index[0]} → {combined.index[-1]}")
            del dfs, combined
            gc.collect()

    concat_save(train_tags, "train")
    concat_save(val_tags, "val")
    concat_save(test_tags, "test")
    if daily_tags:
        concat_save(daily_tags, "daily_latest")

    causal_manifest_path = write_causal_feature_manifest(
        out_dir,
        symbol=symbol,
        feature_paths=feature_paths,
        warmup_days=args.warmup_days,
        market_stage=market_stage,
        reference_symbol=reference_symbol,
        config_path=config_path,
        split=split,
        sample_weight_reference_date=sample_weight_reference_date,
        sample_weight_lambda=args.lam,
        require_execution_l2=args.require_execution_l2,
        require_taker_tempo=args.require_taker_tempo,
        labels_materialized=not args.features_only,
    )
    print(f"Causal feature manifest: {causal_manifest_path}")

    # 打印特征列表
    if feature_paths:
        if sample_columns is None:
            sample_df = pd.read_parquet(feature_paths[0][1])
            sample_columns = list(sample_df.columns)
            del sample_df
            gc.collect()
        feature_cols = [c for c in sample_columns
                        if not c.startswith("label_") and c != "sample_weight"]
        label_cols = [c for c in sample_columns if c.startswith("label_")]
        if args.verbose:
            print(f"\n特征列 ({len(feature_cols)}): {feature_cols}")
            print(f"标签列 ({len(label_cols)}): {label_cols}")
        else:
            print(f"特征列: {len(feature_cols)} 个, 标签列: {len(label_cols)} 个")

    print("\n完成！")


if __name__ == "__main__":
    main()
