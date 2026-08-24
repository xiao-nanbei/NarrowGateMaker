"""
Real-time Signal Engine — 在线计算特征 + LightGBM推理。

从 WebSocket 事件流 (aggTrade + partial_book_depth) 实时计算特征，
每10s产出一组 ML 预测，供 maker_engine 使用。

特征分类:
  A. 微结构特征 (from aggTrade → 1s bars → rolling windows)
  B. Depth特征 (from partial_book_depth snapshots)
  C. Tick momentum (from 1s close diffs)
  D. 时间特征 (from timestamp)
  E. Metrics特征 (from REST API polling: OI, long/short ratios)
"""

import json
import logging
import math
import os
import time
import threading
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

from calendar_features import calendar_scalar_features, is_relative_millisecond_clock
from features.feature_dag import TEN_SECOND_CAUSAL_GRAPH
from market_fusion import (
    BINANCE_VENUE,
    BITGET_VENUE,
    BYBIT_VENUE,
    OKX_VENUE,
    PERP_MARKET,
    SPOT_MARKET,
    default_reference_symbol,
    market_key,
    normalize_symbol,
    normalize_venue,
)
from strategy.global_reference import ReferenceObservation, build_global_reference_state
from strategy.global_flow import GlobalFlowEngine
from strategy.cross_venue_fair_price import (
    CrossVenueFairPriceEstimator,
    FairPriceSource,
)
from strategy.model_contract import REQUIRED_MODEL_HEADS, validate_model_bundle

logger = logging.getLogger("signal")

TEN_SECOND_FEATURE_DAG_SHA256 = TEN_SECOND_CAUSAL_GRAPH.sha256()
SIGNAL_FEATURE_CPP_ABI_VERSION = "signal_feature_cutoff.v1"

MODEL_DIR = Path(__file__).resolve().parent.parent / "models" / "saved"

_CPP_SIGNAL_MODULE = None
_CPP_SIGNAL_IMPORT_FAILED = False


def _cpp_signal_strict() -> bool:
    return os.environ.get("NARROWGATE_CPP_STRICT", "").strip().lower() in {"1", "true", "yes", "on"}


def _cpp_signal_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _load_cpp_signal_module():
    global _CPP_SIGNAL_MODULE, _CPP_SIGNAL_IMPORT_FAILED
    if _CPP_SIGNAL_MODULE is not None:
        return _CPP_SIGNAL_MODULE
    if _CPP_SIGNAL_IMPORT_FAILED and not _cpp_signal_strict():
        return None
    try:
        import narrowgate_cpp  # type: ignore
        required = (
            "TradeBarAggregator", "Bar1s", "FeatureHistoryRow",
            "SignalFeatureEngine", "compute_signal_feature_overlay",
        )
        missing = [name for name in required if not hasattr(narrowgate_cpp, name)]
        if missing:
            raise RuntimeError(f"narrowgate_cpp missing signal symbols: {missing}")
        if _cpp_signal_flag("NARROWGATE_CPP_SIGNAL_FEATURES"):
            actual_abi = str(
                getattr(narrowgate_cpp, "SIGNAL_FEATURE_ABI_VERSION", "")
            )
            if actual_abi != SIGNAL_FEATURE_CPP_ABI_VERSION:
                raise RuntimeError(
                    "narrowgate_cpp signal feature ABI mismatch: "
                    f"expected={SIGNAL_FEATURE_CPP_ABI_VERSION} actual={actual_abi!r}"
                )
        _CPP_SIGNAL_MODULE = narrowgate_cpp
        return _CPP_SIGNAL_MODULE
    except Exception:
        _CPP_SIGNAL_IMPORT_FAILED = True
        if _cpp_signal_strict():
            raise
        return None

# Match feature_engineer.py. Five-second microstructure state is computed from
# the live 1s bar buffer, not approximated on the 10s feature-history grid.
WINDOWS_10S = {"30s": 3, "60s": 6, "300s": 30}

# Feature order must match training
FEATURE_NAMES_BASE = [
    "close", "volume", "buy_volume", "sell_volume",
    "trade_count", "buy_count", "sell_count",
    # tick momentum (14)
    "tick_streak", "tick_mom_3s", "tick_mom_5s", "tick_mom_10s",
    "tick_ewm_3s", "tick_ewm_10s",
    "micro_ret_std", "micro_ret_skew", "micro_ret_kurt",
    "tick_reversal_freq", "flow_velocity", "flow_acceleration",
    "tick_streak_max", "tick_mom_range",
    # depth — REMOVED: live WS/REST cannot reproduce ±0.2-5% percentage buckets
    # metrics (13)
    "oi_log", "oi_pct_change",
    "oi_zscore_1h", "oi_zscore_6h", "oi_momentum",
    "toptrader_ls_ratio", "crowd_ls_ratio", "taker_ls_ratio",
    "toptrader_ls_zscore", "crowd_ls_zscore", "taker_ls_zscore",
    "taker_ls_momentum", "oi_price_divergence",
    # microstructure (rolling)
    "volatility_5s", "volatility_30s", "volatility_60s", "volatility_300s",
    "volume_imbalance", "volume_imbalance_5s", "volume_imbalance_30s",
    "volume_imbalance_60s", "volume_imbalance_300s",
    "trade_intensity_5s", "trade_intensity_30s",
    "trade_intensity_60s", "trade_intensity_300s",
    "vpin_5s", "vpin_30s", "vpin_60s", "vpin_300s",
    "price_velocity", "price_acceleration",
    "price_change_5s", "price_change_30s", "price_change_60s", "price_change_300s",
    "avg_trade_size", "avg_trade_size_60s", "large_trade_ratio",
    "volume_zscore",
    "bar_spread", "bar_spread_bps",
    "return_1", "return_abs",
    # vol regime (3)
    "vol_regime_6h", "vol_regime_24h", "vol_regime_zscore",
    # time (19)
    "hour_sin", "hour_cos",
    "session_asia", "session_europe", "session_america",
    "session_asia_europe_overlap", "session_europe_america_overlap",
    "dow_sin", "dow_cos",
    "minutes_to_funding", "funding_phase",
    "funding_sin", "funding_cos",
    "dist_to_hour", "near_candle_close",
    # US equity market (5)
    "is_us_trading_day", "is_us_regular_hours", "is_us_premarket",
    "minutes_to_us_open", "minutes_to_us_close",
]

CROSS_FEATURE_SUFFIXES = [
    "basis_bps", "ret_10s", "ret_30s", "ret_60s",
    "volatility_60s", "volume_imbalance", "trade_intensity_60s", "vpin_60s",
    "basis_residual_bps", "age_s", "available",
]
CROSS_SOURCE_MAX_AGE_S = 30.0
CROSS_BASIS_WINDOW_10S = 360
CROSS_BASIS_MIN_PERIODS = 30
GLOBAL_REFERENCE_SOURCE_MAX_AGE_MS = 2_000.0
# Stablecoin and spot conversion anchors are slow level inputs, not 1s
# price-discovery votes.  Requiring a book change every two seconds makes a
# valid USDCUSDT quote disappear during otherwise healthy quiet periods.
GLOBAL_REFERENCE_ANCHOR_MAX_AGE_MS = 30_000.0

# All model heads consume the same causal state. The retired return-stacking
# experiment had no active bundle consumers.
FEATURE_NAMES = FEATURE_NAMES_BASE

# Metadata-driven models can request these live-reproducible orderbook features
# without breaking older saved models that still use FEATURE_NAMES_BASE.
EXECUTION_L2_FEATURE_COLS = [
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
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
]

TAKER_TEMPO_WINDOWS_SEC = (5, 10, 30, 60)
TAKER_TEMPO_FEATURE_COLS = [
    *(f"taker_quote_imbalance_{window}s" for window in TAKER_TEMPO_WINDOWS_SEC),
    *(f"taker_signed_quote_sum_{window}s" for window in TAKER_TEMPO_WINDOWS_SEC),
    *(f"taker_trade_count_sum_{window}s" for window in TAKER_TEMPO_WINDOWS_SEC),
    *(f"taker_max_same_side_run_{window}s" for window in TAKER_TEMPO_WINDOWS_SEC),
    *(f"taker_buy_sweep_score_{window}s" for window in TAKER_TEMPO_WINDOWS_SEC),
    *(f"taker_sell_sweep_score_{window}s" for window in TAKER_TEMPO_WINDOWS_SEC),
    *(f"taker_buy_iceberg_pressure_sum_{window}s" for window in TAKER_TEMPO_WINDOWS_SEC),
    *(f"taker_sell_iceberg_pressure_sum_{window}s" for window in TAKER_TEMPO_WINDOWS_SEC),
]

HISTORY_FEATURE_KEYS = [
    "close", "volume", "buy_volume", "sell_volume", "trade_count",
    "flow_velocity", "avg_trade_size", "price_velocity", "return_abs",
    "return_1", "volume_imbalance", "trade_intensity_60s",
    "vol_regime_6h", "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "funding_phase", "bar_spread_bps",
]


@dataclass
class Bar1s:
    """1-second aggregated bar."""
    ts: int = 0        # ms timestamp (bucket start)
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    trade_count: int = 0
    buy_count: int = 0
    sell_count: int = 0
    quote_qty: float = 0.0
    buy_quote_qty: float = 0.0
    sell_quote_qty: float = 0.0
    max_same_side_run: int = 0
    max_buy_run: int = 0
    max_sell_run: int = 0
    buy_price_high: float = 0.0
    buy_price_low: float = 0.0
    sell_price_high: float = 0.0
    sell_price_low: float = 0.0


@dataclass(frozen=True)
class RollingVarianceSnapshot:
    """Causal 60-second absolute-price variance identity."""

    sigma_sq_price_per_s: float
    mid_price: float
    feature_ready_ts_ms: int
    sample_count: int
    valid: bool
    invalid_reason: str = ""


@dataclass(frozen=True)
class FeatureCutoff:
    """Causal visibility boundary for one completed feature bucket."""

    cutoff_exclusive_ms: int
    source_clock: str = "exchange_time_ms"
    availability_clock: str = "finalized_bar_time"

    def __post_init__(self) -> None:
        if int(self.cutoff_exclusive_ms) <= 0:
            raise ValueError("feature cutoff must be a positive millisecond timestamp")

    def visible_bars(self, bars: Sequence[Bar1s]) -> List[Bar1s]:
        return [bar for bar in bars if int(bar.ts) < int(self.cutoff_exclusive_ms)]


@dataclass
class DepthSnapshot:
    """Parsed orderbook depth snapshot."""
    ts: float = 0.0  # exchange timestamp in ms
    receive_ts_ns: int = 0
    bids: List[Tuple[float, float]] = field(default_factory=list)
    asks: List[Tuple[float, float]] = field(default_factory=list)


@dataclass(frozen=True)
class QuoteDepthObservation:
    exchange_ts_ms: int
    receive_ts_ns: int
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]

    @property
    def ts(self) -> float:
        return float(self.exchange_ts_ms)


@dataclass(frozen=True)
class QuotePostOnlyGuard:
    """Frozen BBO selected for post-only validation and order routing."""

    best_bid: float
    best_ask: float
    source: str
    fallback_reason: str
    visible_age_s: float
    source_lag_s: float


@dataclass(frozen=True)
class QuoteDecisionSnapshot:
    """Immutable execution-book view owned by one quote decision.

    Depth and bookTicker are independent exchange streams.  They do not need
    matching sequence numbers, but they must be copied under the same local
    lock so a quote never combines fields read on opposite sides of an update.
    Pricing mid and all depth-derived features use ``bids``/``asks`` from this
    exact depth generation.  The bookTicker fields are retained for the
    post-only guard and source-parity logging.
    """

    capture_ts_ns: int
    market_generation: int
    depth_generation: int
    book_ticker_generation: int
    depth_exchange_ts_ms: int
    depth_receive_ts_ns: int
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]
    best_bid: float
    best_ask: float
    mid: float
    bar_pricing_mid: float
    book_ticker_bid: float
    book_ticker_ask: float
    book_ticker_exchange_ts_ms: int
    book_ticker_receive_ts_ns: int
    book_ticker_sequence: Optional[int]
    depth_history: tuple[QuoteDepthObservation, ...]
    lock_wait_ns: int
    lock_hold_ns: int
    valid: bool
    invalid_reason: str = ""

    @property
    def depth_age_s(self) -> float:
        """Legacy end-to-end age, retained for feature compatibility only."""

        if self.depth_exchange_ts_ms <= 0 or self.capture_ts_ns <= 0:
            return float("inf")
        return self.capture_ts_ns / 1_000_000_000.0 - self.depth_exchange_ts_ms / 1000.0

    @property
    def depth_visible_age_s(self) -> float:
        if self.depth_receive_ts_ns <= 0 or self.capture_ts_ns <= 0:
            return float("inf")
        return (
            self.capture_ts_ns - self.depth_receive_ts_ns
        ) / 1_000_000_000.0

    @property
    def depth_source_lag_s(self) -> float:
        if self.depth_exchange_ts_ms <= 0 or self.depth_receive_ts_ns <= 0:
            return float("inf")
        return (
            self.depth_receive_ts_ns
            - self.depth_exchange_ts_ms * 1_000_000
        ) / 1_000_000_000.0

    @property
    def book_ticker_visible_age_s(self) -> float:
        if self.book_ticker_receive_ts_ns <= 0 or self.capture_ts_ns <= 0:
            return float("inf")
        return (
            self.capture_ts_ns - self.book_ticker_receive_ts_ns
        ) / 1_000_000_000.0

    @property
    def book_ticker_source_lag_s(self) -> float:
        if (
            self.book_ticker_exchange_ts_ms <= 0
            or self.book_ticker_receive_ts_ns <= 0
        ):
            return float("inf")
        return (
            self.book_ticker_receive_ts_ns
            - self.book_ticker_exchange_ts_ms * 1_000_000
        ) / 1_000_000_000.0

    def book_ticker_guard_invalid_reason(
        self,
        *,
        max_visible_age_s: float,
        max_source_lag_s: float,
    ) -> str:
        if not (
            math.isfinite(self.book_ticker_bid)
            and math.isfinite(self.book_ticker_ask)
            and 0.0 < self.book_ticker_bid < self.book_ticker_ask
        ):
            return "missing_or_crossed_book_ticker"
        if self.book_ticker_exchange_ts_ms <= 0:
            return "missing_book_ticker_exchange_timestamp"
        if self.book_ticker_receive_ts_ns <= 0:
            return "missing_book_ticker_receive_timestamp"
        visible_age_s = self.book_ticker_visible_age_s
        if visible_age_s < 0.0:
            return "book_ticker_receive_after_snapshot"
        if max_visible_age_s > 0.0 and visible_age_s > max_visible_age_s:
            return "stale_book_ticker_visible_age"
        source_lag_s = self.book_ticker_source_lag_s
        if source_lag_s < 0.0:
            return "book_ticker_exchange_after_receive"
        if max_source_lag_s > 0.0 and source_lag_s > max_source_lag_s:
            return "stale_book_ticker_source_lag"
        return ""

    def post_only_guard(
        self,
        *,
        max_visible_age_s: float,
        max_source_lag_s: float,
    ) -> QuotePostOnlyGuard:
        """Choose bookTicker only when its price and both clocks are valid."""

        fallback_reason = self.book_ticker_guard_invalid_reason(
            max_visible_age_s=max_visible_age_s,
            max_source_lag_s=max_source_lag_s,
        )
        if not fallback_reason:
            return QuotePostOnlyGuard(
                best_bid=float(self.book_ticker_bid),
                best_ask=float(self.book_ticker_ask),
                source="book_ticker",
                fallback_reason="",
                visible_age_s=float(self.book_ticker_visible_age_s),
                source_lag_s=float(self.book_ticker_source_lag_s),
            )
        return QuotePostOnlyGuard(
            best_bid=float(self.best_bid),
            best_ask=float(self.best_ask),
            source="depth",
            fallback_reason=fallback_reason,
            visible_age_s=float(self.depth_visible_age_s),
            source_lag_s=float(self.depth_source_lag_s),
        )


@dataclass
class Prediction:
    """ML prediction output."""
    ts: float = 0.0
    dir_10s: float = 0.5
    dir_30s: float = 0.5
    dir_60s: float = 0.5
    vol_10s: float = 0.0
    vol_30s: float = 0.0
    vol_60s: float = 0.0
    ret_10s: float = 0.0
    ret_30s: float = 0.0
    ret_60s: float = 0.0
    tox_bid_5s: float = 0.5
    tox_ask_5s: float = 0.5
    tox_bid_10s: float = 0.5
    tox_ask_10s: float = 0.5
    features: Optional[np.ndarray] = None
    feature_dict: Optional[Dict[str, float]] = None


class SignalEngine:
    """
    Streaming feature computation + ML inference.

    Flow:
        aggTrade events → aggregate into 1s bars
        → every 10s: compute 88 base features from a causal cutoff view
        → feed into LightGBM models
        → output Prediction

    Thread safety: on_agg_trade and on_depth can be called from
    different WS threads. compute_signal is called from engine thread.
    """

    def __init__(self, model_dir: Optional[Path] = None,
                 bar_buffer_size: int = 3700,
                 enable_ml: bool = True,
                 rest_client=None,
                 symbol: str = "BTCUSDC",
                 reference_symbol: Optional[str] = None,
                 stablecoin_anchor_symbol: Optional[str] = "USDCUSDT",
                 global_flow_shadow_enabled: bool = True,
                 global_reference_shadow_enabled: bool = True,
                 ret_demean_halflife: int = 360,
                 bad_trade_log_every: int = 100):
        self._lock = Lock()
        self._model_dir = model_dir or MODEL_DIR
        self._enable_ml = enable_ml
        self._rest = rest_client  # for metrics polling
        self._symbol = normalize_symbol(symbol, "BTCUSDC")
        self._reference_symbol = normalize_symbol(reference_symbol, default_reference_symbol(self._symbol))
        self._stablecoin_anchor_symbol = normalize_symbol(
            stablecoin_anchor_symbol, "USDCUSDT"
        )
        if type(global_flow_shadow_enabled) is not bool:
            raise TypeError("global_flow_shadow_enabled must be a boolean")
        if type(global_reference_shadow_enabled) is not bool:
            raise TypeError("global_reference_shadow_enabled must be a boolean")
        self._global_flow_shadow_enabled = global_flow_shadow_enabled
        self._global_reference_shadow_enabled = global_reference_shadow_enabled

        # 1s bar ring buffer (last N seconds)
        self._bar_buffer: deque = deque(maxlen=bar_buffer_size)
        self._current_bar: Optional[Bar1s] = None
        self._current_bucket: int = 0

        # depth snapshot buffer
        # depth@20@100ms needs >10s retention for bucket-aligned L2 flow features.
        self._depth_history: deque = deque(maxlen=300)
        self._last_depth: Optional[DepthSnapshot] = None
        self._quote_market_generation = 0
        self._depth_generation = 0
        self._book_ticker_generation = 0

        # 10s feature history (for rolling on 10s features)
        # Need 8640 entries (24h of 10s bars) for vol_regime_24h,
        # 2160 for vol_regime_6h, and 60480 for vol_regime_zscore (7d).
        self._feat_history: deque = deque(maxlen=60480)

        # Track the last completed 10s bucket we processed (ms timestamp)
        # Ensures features are only computed once per 10s boundary,
        # matching the fixed-10s cadence used during offline training.
        self._last_processed_bucket: Optional[int] = None

        # Transformer: sliding window of raw feature vectors (60 × 10s)

        # tick momentum state (on 1s close diffs)
        self._close_history: deque = deque(maxlen=320)
        self._sign_history: deque = deque(maxlen=320)
        self._signed_vol_cumsum: float = 0.0
        self._prev_flow_velocity: float = 0.0
        self._last_trade_side: int = 0
        self._last_trade_run_len: int = 0

        # Callback for bar completion (used by MakerEngine for dynamic RQ)
        self._on_bar_callbacks = []
        # Receive-time depth observers run after the atomic depth update and
        # outside the signal lock. They may never mutate quote-book state.
        self._on_depth_callbacks = []

        # metrics state (polled every 5 min via REST)
        self._metrics_history: deque = deque(maxlen=72)  # 6h of 5min data
        self._last_metrics: Optional[dict] = None
        self._book_tickers: Dict[str, list[float]] = {}
        self._book_ticker_history: Dict[str, deque] = {}
        self._cross_bar_buffers: Dict[str, deque] = {}
        self._cross_current_bars: Dict[str, Bar1s] = {}
        self._cross_current_buckets: Dict[str, int] = {}
        self._cross_basis_history: Dict[str, deque] = {}
        self._global_bridge_basis_history: deque = deque(maxlen=3600)
        # Venue-aware receive-time state is shadow evidence only.  Quote-time
        # policy must explicitly promote a derived feature before it can use
        # any external venue.
        self._market_source_state: Dict[str, dict] = {}
        self._global_flow = GlobalFlowEngine(
            execution_symbol=self._symbol,
            reference_symbol=self._reference_symbol,
        )
        self._cross_venue_fair_price = CrossVenueFairPriceEstimator()
        self._metrics_poll_interval = 300  # 5 minutes
        self._metrics_timer: Optional[threading.Timer] = None
        self._metrics_stop = threading.Event()  # graceful stop for timer chain
        self._metrics_started = False  # prevent duplicate timer chains

        # Full depth state — DISABLED: REST limit=1000 only covers ±0.18%, insufficient
        # Depth features removed from ML pipeline entirely
        # self._last_depth_full: Optional[DepthSnapshot] = None
        # self._depth_poll_interval = 5
        # self._depth_timer: Optional[threading.Timer] = None
        # self._depth_stop = threading.Event()
        # self._depth_started = False

        # models
        self._models: Dict[str, object] = {}
        self._model_feature_cols: Dict[str, List[str]] = {}
        if enable_ml:
            self._load_models()

        # latest prediction
        self._last_prediction = Prediction()
        self._warmup_count = 0
        self._bad_trade_events = 0
        self._bad_trade_parse_events = 0
        self._bad_trade_value_events = 0
        self._bad_trade_log_every = max(1, int(bad_trade_log_every))
        self._last_bad_trade_sample = ""
        self._live_feature_dump_path = os.environ.get("NARROWGATE_LIVE_FEATURE_DUMP", "").strip()
        self._live_feature_dump_every_n = max(
            1,
            int(os.environ.get("NARROWGATE_LIVE_FEATURE_DUMP_EVERY_N", "1") or "1"),
        )
        self._live_feature_dump_count = 0
        if self._live_feature_dump_path:
            logger.info("Live feature dump enabled: %s", self._live_feature_dump_path)

        # pred_ret EMA demeaning state
        self._ret_demean_halflife = ret_demean_halflife
        self._pred_ret_ema = [0.0, 0.0, 0.0]  # 10s, 30s, 60s

        self._cpp_signal = _load_cpp_signal_module() if (
            _cpp_signal_flag("NARROWGATE_CPP_SIGNAL_FEATURES")
            or _cpp_signal_flag("NARROWGATE_CPP_GLOBAL_FLOW")
        ) else None
        self._cpp_signal_feature_names = tuple(
            getattr(self._cpp_signal, "SIGNAL_FEATURE_NAMES", ())
        ) if self._cpp_signal is not None else ()
        self._cpp_signal_features_enabled = bool(
            self._cpp_signal is not None and _cpp_signal_flag("NARROWGATE_CPP_SIGNAL_FEATURES")
        )
        self._cpp_global_flow_requested = bool(
            self._cpp_signal is not None and _cpp_signal_flag("NARROWGATE_CPP_GLOBAL_FLOW")
        )
        self._cpp_global_flow_enabled = bool(
            self._global_flow_shadow_enabled and self._cpp_global_flow_requested
        )
        if self._cpp_global_flow_enabled:
            native_flow_cls = getattr(self._cpp_signal, "NativeGlobalFlowEngine", None)
            if native_flow_cls is None:
                if _cpp_signal_strict():
                    raise RuntimeError("narrowgate_cpp missing NativeGlobalFlowEngine")
                logger.warning("C++ global flow disabled: NativeGlobalFlowEngine missing")
                self._cpp_global_flow_enabled = False
            else:
                self._global_flow = GlobalFlowEngine(
                    execution_symbol=self._symbol,
                    reference_symbol=self._reference_symbol,
                    native_backend=native_flow_cls(2_000, 1_000.0, 1_000.0),
                )
        self._cpp_feature_engine = (
            self._cpp_signal.SignalFeatureEngine(bar_buffer_size, 60480)
            if self._cpp_signal_features_enabled else None
        )
        self._cpp_feature_engine_seeded = self._cpp_feature_engine is not None
        self._cpp_cross_aggregators: Dict[str, object] = {}
        self._cpp_cross_current_dirty = set()
        self._cpp_cross_batch_enabled = bool(
            self._cpp_signal is not None
            and self._cpp_global_flow_requested
            and hasattr(self._cpp_signal.TradeBarAggregator, "update_batch")
        )
        if (
            self._cpp_signal_features_enabled
            or self._cpp_global_flow_requested
        ):
            logger.info(
                "SignalEngine C++ path: features=%s global_flow_requested=%s "
                "global_flow_effective=%s "
                "cross_batch=%s module=%s",
                self._cpp_signal_features_enabled,
                self._cpp_global_flow_requested,
                self._cpp_global_flow_enabled,
                self._cpp_cross_batch_enabled,
                getattr(self._cpp_signal, "__file__", "<unknown>") if self._cpp_signal else None,
            )

        logger.info(f"SignalEngine init: buffer={bar_buffer_size}s, "
                     f"ml={enable_ml}, models={list(self._models.keys())}")

        # Start metrics polling if REST client available
        if self._rest:
            self._start_metrics_polling()

    def _start_metrics_polling(self):
        """Start the metrics polling chain (idempotent)."""
        if self._metrics_started:
            return
        self._metrics_started = True
        self._poll_metrics()

    def stop(self):
        """Stop metrics polling timer."""
        self._metrics_stop.set()
        if self._metrics_timer:
            self._metrics_timer.cancel()
            self._metrics_timer = None

    def set_model_dir(self, model_dir: Optional[Path]):
        """Update the model directory used by subsequent model loads."""
        self._model_dir = Path(model_dir).expanduser() if model_dir else MODEL_DIR

    def reload_models(self, model_dir: Optional[Path] = None):
        """Reload ML models, optionally switching to a new model directory."""
        if model_dir is not None:
            self.set_model_dir(model_dir)
        self._load_models()

    def _load_models(self):
        """Load saved LightGBM models and their explicit feature schemas."""
        try:
            metadata = validate_model_bundle(self._model_dir)
        except Exception as exc:
            raise RuntimeError(
                f"ML is enabled but its runtime bundle is invalid: {exc}"
            ) from exc
        try:
            import lightgbm as lgb
        except ImportError as exc:
            raise RuntimeError("ML is enabled but LightGBM is not installed") from exc

        loaded_models: Dict[str, object] = {}
        loaded_feature_cols: Dict[str, List[str]] = {}
        for name in REQUIRED_MODEL_HEADS:
            path = self._model_dir / f"{name}.txt"
            try:
                model = lgb.Booster(model_file=str(path))
            except Exception as exc:
                raise RuntimeError(f"failed to load required model {path}: {exc}") from exc
            cols = list(metadata[name]["feature_cols"])
            if model.num_feature() != len(cols):
                raise RuntimeError(
                    f"model/schema width mismatch for {name}: "
                    f"model={model.num_feature()} metadata={len(cols)}"
                )
            loaded_models[name] = model
            loaded_feature_cols[name] = cols
            logger.info(
                "Loaded model: %s (%d features, strict metadata)",
                name,
                model.num_feature(),
            )

        # Hot reload is atomic: a failed replacement leaves the old bundle in
        # memory, while startup has no old bundle and therefore fails closed.
        self._models = loaded_models
        self._model_feature_cols = loaded_feature_cols

    @staticmethod
    def _history_snapshot(features: dict) -> dict:
        return {key: features.get(key, 0.0) for key in HISTORY_FEATURE_KEYS}

    # ── warmup prefill ──

    def prefill_from_agg_trades(self, trades: list):
        """Pre-fill bar buffer from historical aggTrades (REST API).

        Builds bars offline and injects into buffer, avoiding
        interleaving issues with live WS data already flowing.
        """
        if not trades:
            return

        parsed_trades = []
        for t in trades:
            parsed = self._parse_trade_event(t)
            if parsed is not None:
                _, ts_ms, price, qty, is_buyer_maker = parsed
                parsed_trades.append((ts_ms, price, qty, is_buyer_maker))
        if not parsed_trades:
            return
        parsed_trades.sort(key=lambda row: row[0])

        bars = {}  # bucket_ms -> Bar1s
        last_side = 0
        last_run_len = 0
        for ts_ms, price, qty, is_buyer_maker in parsed_trades:
            bucket = (ts_ms // 1000) * 1000

            if bucket not in bars:
                bars[bucket] = Bar1s(
                    ts=bucket, open=price, high=price,
                    low=price, close=price,
                )
            side = -1 if is_buyer_maker else 1
            if side == last_side:
                last_run_len += 1
            else:
                last_side = side
                last_run_len = 1
            self._apply_trade_to_bar(
                bars[bucket],
                price,
                qty,
                is_buyer_maker,
                run_len=last_run_len,
            )

        # Sort by time and inject via _finalize_bar
        sorted_buckets = sorted(bars.keys())
        with self._lock:
            prev_bucket = None
            prev_close = None
            for bucket in sorted_buckets:
                if prev_bucket is not None and prev_close is not None:
                    self._emit_gap_bars(prev_bucket, bucket, prev_close, self._finalize_bar)
                self._finalize_bar(bars[bucket])
                prev_bucket = bucket
                prev_close = bars[bucket].close
            self._last_trade_side = last_side
            self._last_trade_run_len = last_run_len

        logger.info(
            f"Prefilled {len(sorted_buckets)} bars from "
            f"{len(parsed_trades)} aggTrades"
        )

        # Pre-compute 10s features from the prefilled 1s bars to populate
        # _feat_history.  Without this, vol_regime / metrics features are
        # empty or zero-filled until enough live compute_signal() calls
        # accumulate — the core train-live feature skew problem.
        self._prefill_10s_features()

    def _prefill_10s_features(self):
        """Replay 1s bars in 10s chunks to pre-fill _feat_history.

        This ensures that features like vol_regime_6h / vol_regime_24h
        already have history when the engine starts quoting, eliminating
        the cold-start zero-fill problem.
        """
        with self._lock:
            all_bars = list(self._bar_buffer)
            if len(all_bars) < 10:
                return
            count = len(self._process_completed_feature_buckets_locked(all_bars))

        logger.info(f"Pre-filled {count} 10s feature points from warmup bars")

    # ── event handlers ──

    def _record_bad_trade(self, reason: str, sample: str):
        """Aggregate bad trade diagnostics and log once every N events."""
        self._bad_trade_events += 1
        if reason == "parse":
            self._bad_trade_parse_events += 1
        else:
            self._bad_trade_value_events += 1

        s = str(sample)
        if len(s) > 200:
            s = s[:200] + "..."
        self._last_bad_trade_sample = s

        if self._bad_trade_events % self._bad_trade_log_every == 0:
            logger.warning(
                "Ignored invalid trades: total=%d (parse=%d, value=%d), last=%s",
                self._bad_trade_events,
                self._bad_trade_parse_events,
                self._bad_trade_value_events,
                self._last_bad_trade_sample,
            )

    def _parse_trade_event(self, event: dict):
        try:
            ts_ms = int(event.get("T", 0))
            price = float(event.get("p", 0))
            qty = float(event.get("q", 0))
        except (TypeError, ValueError):
            self._record_bad_trade("parse", event)
            return None

        if ts_ms <= 0 or price <= 0.0 or qty <= 0.0:
            self._record_bad_trade("value", f"T={ts_ms} p={price} q={qty}")
            return None

        m_raw = event.get("m", False)
        is_buyer_maker = (m_raw.lower() == "true") if isinstance(m_raw, str) else bool(m_raw)
        symbol = normalize_symbol(event.get("s"), self._symbol)
        return symbol, ts_ms, price, qty, is_buyer_maker

    @staticmethod
    def _apply_trade_to_bar(bar: Bar1s, price: float, qty: float,
                            is_buyer_maker: bool, run_len: int = 1):
        bar.close = price
        bar.high = max(bar.high, price)
        bar.low = min(bar.low, price)
        bar.volume += qty
        bar.trade_count += 1
        quote_qty = price * qty
        bar.quote_qty += quote_qty
        bar.max_same_side_run = max(bar.max_same_side_run, run_len)

        if is_buyer_maker:
            bar.sell_volume += qty
            bar.sell_count += 1
            bar.sell_quote_qty += quote_qty
            bar.max_sell_run = max(bar.max_sell_run, run_len)
            bar.sell_price_high = max(bar.sell_price_high, price)
            bar.sell_price_low = price if bar.sell_price_low <= 0 else min(bar.sell_price_low, price)
        else:
            bar.buy_volume += qty
            bar.buy_count += 1
            bar.buy_quote_qty += quote_qty
            bar.max_buy_run = max(bar.max_buy_run, run_len)
            bar.buy_price_high = max(bar.buy_price_high, price)
            bar.buy_price_low = price if bar.buy_price_low <= 0 else min(bar.buy_price_low, price)

    @staticmethod
    def _flat_bar(ts_ms: int, close: float) -> Bar1s:
        return Bar1s(
            ts=ts_ms,
            open=close,
            high=close,
            low=close,
            close=close,
        )

    @staticmethod
    def _bar_from_cpp(cpp_bar) -> Bar1s:
        return Bar1s(
            ts=int(getattr(cpp_bar, "ts_ms", 0)),
            open=float(getattr(cpp_bar, "open", 0.0)),
            high=float(getattr(cpp_bar, "high", 0.0)),
            low=float(getattr(cpp_bar, "low", 0.0)),
            close=float(getattr(cpp_bar, "close", 0.0)),
            volume=float(getattr(cpp_bar, "volume", 0.0)),
            buy_volume=float(getattr(cpp_bar, "buy_volume", 0.0)),
            sell_volume=float(getattr(cpp_bar, "sell_volume", 0.0)),
            trade_count=int(getattr(cpp_bar, "trade_count", 0.0)),
            buy_count=int(getattr(cpp_bar, "buy_count", 0.0)),
            sell_count=int(getattr(cpp_bar, "sell_count", 0.0)),
            quote_qty=float(getattr(cpp_bar, "quote_qty", 0.0)),
            buy_quote_qty=float(getattr(cpp_bar, "buy_quote_qty", 0.0)),
            sell_quote_qty=float(getattr(cpp_bar, "sell_quote_qty", 0.0)),
            max_same_side_run=int(getattr(cpp_bar, "max_same_side_run", 0.0)),
            max_buy_run=int(getattr(cpp_bar, "max_buy_run", 0.0)),
            max_sell_run=int(getattr(cpp_bar, "max_sell_run", 0.0)),
            buy_price_high=float(getattr(cpp_bar, "buy_price_high", 0.0)),
            buy_price_low=float(getattr(cpp_bar, "buy_price_low", 0.0)),
            sell_price_high=float(getattr(cpp_bar, "sell_price_high", 0.0)),
            sell_price_low=float(getattr(cpp_bar, "sell_price_low", 0.0)),
        )

    def _bar_to_cpp(self, bar: Bar1s):
        cpp = self._cpp_signal
        if cpp is None:
            return None
        out = cpp.Bar1s()
        out.ts_ms = int(bar.ts)
        out.open = float(bar.open)
        out.high = float(bar.high)
        out.low = float(bar.low)
        out.close = float(bar.close)
        out.volume = float(bar.volume)
        out.buy_volume = float(bar.buy_volume)
        out.sell_volume = float(bar.sell_volume)
        out.trade_count = float(bar.trade_count)
        out.buy_count = float(bar.buy_count)
        out.sell_count = float(bar.sell_count)
        out.quote_qty = float(bar.quote_qty)
        out.buy_quote_qty = float(bar.buy_quote_qty)
        out.sell_quote_qty = float(bar.sell_quote_qty)
        out.max_same_side_run = float(bar.max_same_side_run)
        out.max_buy_run = float(bar.max_buy_run)
        out.max_sell_run = float(bar.max_sell_run)
        out.buy_price_high = float(bar.buy_price_high)
        out.buy_price_low = float(bar.buy_price_low)
        out.sell_price_high = float(bar.sell_price_high)
        out.sell_price_low = float(bar.sell_price_low)
        return out

    def _history_to_cpp(self, row: dict):
        cpp = self._cpp_signal
        if cpp is None:
            return None
        out = cpp.FeatureHistoryRow()
        out.close = float(row.get("close", 0.0))
        out.volume = float(row.get("volume", 0.0))
        out.buy_volume = float(row.get("buy_volume", 0.0))
        out.sell_volume = float(row.get("sell_volume", 0.0))
        out.trade_count = float(row.get("trade_count", 0.0))
        out.flow_velocity = float(row.get("flow_velocity", 0.0))
        out.avg_trade_size = float(row.get("avg_trade_size", 0.0))
        out.price_velocity = float(row.get("price_velocity", 0.0))
        out.return_abs = float(row.get("return_abs", 0.0))
        out.vol_regime_6h = float(row.get("vol_regime_6h", 0.0))
        return out

    def _ensure_cpp_feature_engine(self, all_bars: Optional[List[Bar1s]] = None):
        if not self._cpp_signal_features_enabled or self._cpp_signal is None:
            return None
        if self._cpp_feature_engine is None:
            self._cpp_feature_engine = self._cpp_signal.SignalFeatureEngine(
                int(self._bar_buffer.maxlen or 320),
                60480,
            )
            self._cpp_feature_engine_seeded = False
        if not self._cpp_feature_engine_seeded:
            for bar in (all_bars if all_bars is not None else list(self._bar_buffer))[-320:]:
                self._cpp_feature_engine.push_bar(self._bar_to_cpp(bar))
            for row in self._feat_history:
                self._cpp_feature_engine.push_history(self._history_to_cpp(row))
            self._cpp_feature_engine_seeded = True
        return self._cpp_feature_engine

    def _sync_cpp_cross_current_bar_locked(self, key: str):
        if (
            not self._cpp_cross_batch_enabled
            or key not in self._cpp_cross_current_dirty
        ):
            return
        agg = self._cpp_cross_aggregators.get(key)
        if agg is None:
            self._cpp_cross_current_dirty.discard(key)
            return
        current = agg.current_bar()
        if current is not None:
            bar = self._bar_from_cpp(current)
            self._cross_current_bars[key] = bar
            self._cross_current_buckets[key] = int(bar.ts)
        self._cpp_cross_current_dirty.discard(key)

    def _emit_gap_bars(self, start_bucket: int, end_bucket: int, carry_close: float, emit):
        # live 流里短暂没有成交也要补平 1s bar，否则 rolling window 会把“无交易时间”
        # 压缩掉，和离线 daily feature 的时间轴不一致。
        gap_bucket = start_bucket + 1000
        while gap_bucket < end_bucket:
            emit(self._flat_bar(gap_bucket, carry_close))
            gap_bucket += 1000

    def _update_market_trade_state_locked(
        self,
        key: str,
        *,
        venue: str,
        market_type: str,
        symbol: str,
        ts_ms: int,
        receive_ns: int,
        price: float,
        qty: float,
        sequence_number: Optional[int],
    ) -> None:
        state = self._market_source_state.get(key)
        if state is None:
            state = {
                "market_id": key,
                "venue": venue,
                "market_type": market_type,
                "symbol": symbol,
            }
            self._market_source_state[key] = state
        state["last_trade_exchange_ts_ms"] = int(ts_ms)
        state["last_trade_receive_ts_ns"] = int(receive_ns)
        state["last_trade_price"] = float(price)
        state["last_trade_qty"] = float(qty)
        state["last_trade_sequence"] = sequence_number

    def _update_market_book_state_locked(
        self,
        key: str,
        *,
        venue: str,
        market_type: str,
        symbol: str,
        event_time_ms: int,
        receive_ns: int,
        bid: float,
        ask: float,
        sequence_number: Optional[int],
    ) -> None:
        state = self._market_source_state.get(key)
        if state is None:
            state = {
                "market_id": key,
                "venue": venue,
                "market_type": market_type,
                "symbol": symbol,
            }
            self._market_source_state[key] = state
        state["last_book_exchange_ts_ms"] = int(event_time_ms)
        state["last_book_receive_ts_ns"] = int(receive_ns)
        state["last_bid"] = float(bid)
        state["last_ask"] = float(ask)
        state["last_book_sequence"] = sequence_number

    def _cross_bar_buffer_locked(self, key: str) -> deque:
        buffer = self._cross_bar_buffers.get(key)
        if buffer is None:
            buffer = deque(maxlen=3700)
            self._cross_bar_buffers[key] = buffer
        return buffer

    def on_agg_trade(
        self,
        event: dict,
        *,
        receive_ts_ns: Optional[int] = None,
        sequence_number: Optional[int] = None,
    ):
        """
        Process aggTrade event from WebSocket.

        event = {
            "T": tradeTime (ms),
            "p": price (str),
            "q": quantity (str),
            "m": is_buyer_maker (bool),
        }
        """
        parsed = self._parse_trade_event(event)
        if parsed is None:
            return
        symbol, ts_ms, price, qty, is_buyer_maker = parsed
        receive_ns = int(receive_ts_ns or time.time_ns())

        bucket = (ts_ms // 1000) * 1000  # floor to second
        key = self._market_key(PERP_MARKET, symbol, venue=BINANCE_VENUE)
        flow_enabled = self._global_flow_shadow_enabled
        native_flow = flow_enabled and self._global_flow.native_enabled
        if native_flow:
            self._global_flow.on_trade(
                key,
                receive_ts_ns=receive_ns,
                price=price,
                size=qty,
                aggressor_side="sell" if is_buyer_maker else "buy",
                exchange_ts_ns=int(ts_ms) * 1_000_000,
            )

        with self._lock:
            self._update_market_trade_state_locked(
                key,
                venue=BINANCE_VENUE,
                market_type=PERP_MARKET,
                symbol=symbol,
                ts_ms=ts_ms,
                receive_ns=receive_ns,
                price=price,
                qty=qty,
                sequence_number=sequence_number,
            )
            if flow_enabled and not native_flow:
                self._global_flow.on_trade(
                    key,
                    receive_ts_ns=receive_ns,
                    price=price,
                    size=qty,
                    aggressor_side="sell" if is_buyer_maker else "buy",
                    exchange_ts_ns=int(ts_ms) * 1_000_000,
                )
            side = -1 if is_buyer_maker else 1
            if side == self._last_trade_side:
                self._last_trade_run_len += 1
            else:
                self._last_trade_side = side
                self._last_trade_run_len = 1
            run_len = self._last_trade_run_len

            if self._current_bar is None:
                # REST warmup commits completed bars without leaving a live
                # current bar. Bridge its final timestamp to the first newer
                # WS trade so the causal 1s grid does not start with a hole.
                if self._bar_buffer:
                    last_bar = self._bar_buffer[-1]
                    if bucket <= int(last_bar.ts):
                        return
                    self._emit_gap_bars(
                        int(last_bar.ts),
                        bucket,
                        last_bar.close,
                        self._finalize_bar,
                    )
                self._current_bar = Bar1s(
                    ts=bucket, open=price, high=price,
                    low=price, close=price,
                )
                self._current_bucket = bucket
            elif bucket != self._current_bucket:
                last_bar = self._current_bar
                self._finalize_bar(last_bar)
                self._emit_gap_bars(
                    self._current_bucket,
                    bucket,
                    last_bar.close,
                    self._finalize_bar,
                )
                self._current_bar = Bar1s(
                    ts=bucket, open=price, high=price,
                    low=price, close=price,
                )
                self._current_bucket = bucket

            bar = self._current_bar
            self._apply_trade_to_bar(bar, price, qty, is_buyer_maker, run_len=run_len)

    def on_cross_agg_trade(
        self,
        event: dict,
        market_type: str = PERP_MARKET,
        *,
        venue: str = BINANCE_VENUE,
        receive_ts_ns: Optional[int] = None,
        sequence_number: Optional[int] = None,
    ):
        """Aggregate non-execution market trades for cross-market live features."""
        parsed = self._parse_trade_event(event)
        if parsed is None:
            return
        symbol, ts_ms, price, qty, is_buyer_maker = parsed
        self.on_cross_trade_arrays(
            symbol,
            (ts_ms,),
            (price,),
            (qty,),
            (is_buyer_maker,),
            market_type=market_type,
            venue=venue,
            receive_ts_ns=receive_ts_ns,
            sequence_numbers=(sequence_number,),
        )

    def on_cross_agg_trade_batch(
        self,
        events: Sequence[dict],
        market_type: str = PERP_MARKET,
        *,
        venue: str = BINANCE_VENUE,
        receive_ts_ns: Optional[int] = None,
        sequence_numbers: Optional[Sequence[Optional[int]]] = None,
    ) -> None:
        """Consume one venue frame with a single shared-state lock acquisition."""
        parsed_rows: list[tuple] = []
        parsed_sequences: list[Optional[int]] = []
        sequences = sequence_numbers if sequence_numbers is not None else ()
        for index, event in enumerate(events):
            parsed = self._parse_trade_event(event)
            if parsed is None:
                continue
            parsed_rows.append(parsed)
            parsed_sequences.append(sequences[index] if index < len(sequences) else None)
        if not parsed_rows:
            return
        symbol = parsed_rows[0][0]
        if any(row[0] != symbol for row in parsed_rows[1:]):
            raise ValueError("cross-trade frame contains multiple symbols")
        self.on_cross_trade_arrays(
            symbol,
            [row[1] for row in parsed_rows],
            [row[2] for row in parsed_rows],
            [row[3] for row in parsed_rows],
            [row[4] for row in parsed_rows],
            market_type=market_type,
            venue=venue,
            receive_ts_ns=receive_ts_ns,
            sequence_numbers=parsed_sequences,
        )

    def on_cross_trade_arrays(
        self,
        symbol: str,
        ts_ms,
        prices,
        quantities,
        is_buyer_maker,
        market_type: str = PERP_MARKET,
        *,
        venue: str = BINANCE_VENUE,
        receive_ts_ns: Optional[int] = None,
        sequence_numbers: Optional[Sequence[Optional[int]]] = None,
    ) -> None:
        ts_values = np.ascontiguousarray(ts_ms, dtype=np.int64).reshape(-1)
        price_values = np.ascontiguousarray(prices, dtype=np.float64).reshape(-1)
        qty_values = np.ascontiguousarray(quantities, dtype=np.float64).reshape(-1)
        maker_values = np.ascontiguousarray(is_buyer_maker, dtype=np.uint8).reshape(-1)
        count = int(ts_values.size)
        if count == 0:
            return
        if not (
            price_values.size == count
            and qty_values.size == count
            and maker_values.size == count
        ):
            raise ValueError("cross-trade arrays must have equal length")

        normalized_symbol = normalize_symbol(symbol, self._reference_symbol)
        normalized_venue = normalize_venue(venue)
        key = self._market_key(
            market_type, normalized_symbol, venue=normalized_venue
        )
        receive_ns = int(receive_ts_ns or time.time_ns())
        exchange_ns = np.ascontiguousarray(ts_values * 1_000_000, dtype=np.int64)
        flow_enabled = self._global_flow_shadow_enabled
        native_flow = flow_enabled and self._global_flow.native_enabled
        if native_flow:
            self._global_flow.on_trade_batch(
                key,
                receive_ts_ns=receive_ns,
                exchange_ts_ns=exchange_ns,
                prices=price_values,
                sizes=qty_values,
                is_buyer_maker=maker_values,
            )

        sequences = sequence_numbers if sequence_numbers is not None else ()
        last_sequence = sequences[count - 1] if count <= len(sequences) else None
        with self._lock:
            last = count - 1
            self._update_market_trade_state_locked(
                key,
                venue=normalized_venue,
                market_type=market_type,
                symbol=normalized_symbol,
                ts_ms=int(ts_values[last]),
                receive_ns=receive_ns,
                price=float(price_values[last]),
                qty=float(qty_values[last]),
                sequence_number=last_sequence,
            )
            if flow_enabled and not native_flow:
                self._global_flow.on_trade_batch(
                    key,
                    receive_ts_ns=receive_ns,
                    exchange_ts_ns=exchange_ns,
                    prices=price_values,
                    sizes=qty_values,
                    is_buyer_maker=maker_values,
                )

            if self._cpp_cross_batch_enabled and self._cpp_signal is not None:
                try:
                    agg = self._cpp_cross_aggregators.get(key)
                    if agg is None:
                        agg = self._cpp_signal.TradeBarAggregator(False)
                        self._cpp_cross_aggregators[key] = agg
                    completed = agg.update_batch(
                        ts_values, price_values, qty_values, maker_values
                    )
                    if completed:
                        buffer = self._cross_bar_buffer_locked(key)
                        for cpp_bar in completed:
                            buffer.append(self._bar_from_cpp(cpp_bar))
                    self._cpp_cross_current_dirty.add(key)
                    return
                except Exception as exc:
                    if _cpp_signal_strict():
                        raise
                    logger.warning(
                        "C++ cross trade batch disabled after error: %s", exc
                    )
                    self._cpp_cross_batch_enabled = False
                    self._cpp_cross_aggregators.clear()
                    self._cpp_cross_current_dirty.clear()

            for index in range(count):
                event_ts_ms = int(ts_values[index])
                price = float(price_values[index])
                qty = float(qty_values[index])
                buyer_maker = bool(maker_values[index])
                bucket = (event_ts_ms // 1000) * 1000
                current = self._cross_current_bars.get(key)
                current_bucket = self._cross_current_buckets.get(key)
                if current is None or bucket != current_bucket:
                    if current is not None:
                        buffer = self._cross_bar_buffer_locked(key)
                        buffer.append(current)
                        self._emit_gap_bars(
                            current_bucket, bucket, current.close, buffer.append
                        )
                    current = Bar1s(
                        ts=bucket,
                        open=price,
                        high=price,
                        low=price,
                        close=price,
                    )
                    self._cross_current_bars[key] = current
                    self._cross_current_buckets[key] = bucket
                self._apply_trade_to_bar(current, price, qty, buyer_maker)

    def _finalize_bar(self, bar: Bar1s):
        """Called when a 1s bar is complete — update buffers."""
        self._bar_buffer.append(bar)
        if self._cpp_signal_features_enabled and self._cpp_signal is not None:
            try:
                already_seeded = bool(
                    self._cpp_feature_engine is not None and self._cpp_feature_engine_seeded
                )
                engine = self._ensure_cpp_feature_engine()
                if engine is not None and already_seeded:
                    engine.push_bar(self._bar_to_cpp(bar))
            except Exception as exc:
                if _cpp_signal_strict():
                    raise
                logger.warning("C++ persistent signal features disabled after bar update: %s", exc)
                self._cpp_signal_features_enabled = False
                self._cpp_feature_engine = None

        # Update close/sign history for tick momentum
        cl = bar.close
        self._close_history.append(cl)
        if len(self._close_history) >= 2:
            diff = cl - self._close_history[-2]
            sign = 1.0 if diff > 0 else (-1.0 if diff < 0 else 0.0)
        else:
            sign = 0.0
        self._sign_history.append(sign)

        # Cumulative signed volume for flow velocity
        signed_vol = bar.buy_volume - bar.sell_volume
        self._signed_vol_cumsum += signed_vol

        self._warmup_count += 1

        # Notify callbacks (e.g. MakerEngine dynamic RQ)
        for cb in self._on_bar_callbacks:
            cb(cl)

    def on_depth(
        self,
        event: dict,
        *,
        receive_ts_ns: Optional[int] = None,
    ):
        """
        Process partial_book_depth event from WebSocket.

        event = {
            "T": timestamp (ms),
            "b": [[price, qty], ...],  # bids
            "a": [[price, qty], ...],  # asks
        }
        """
        raw_exchange_ts = event.get("T", event.get("E"))
        try:
            exchange_ts_ms = float(raw_exchange_ts or 0.0)
        except (TypeError, ValueError):
            exchange_ts_ms = 0.0
        snap = DepthSnapshot(
            ts=exchange_ts_ms,
            receive_ts_ns=int(receive_ts_ns or time.time_ns()),
            bids=[(float(p), float(q)) for p, q in event.get("b", [])],
            asks=[(float(p), float(q)) for p, q in event.get("a", [])],
        )
        with self._lock:
            self._quote_market_generation += 1
            self._depth_generation += 1
            self._last_depth = snap
            self._depth_history.append(snap)
            market_generation = int(self._quote_market_generation)
            depth_generation = int(self._depth_generation)
        for callback in tuple(self._on_depth_callbacks):
            try:
                callback(
                    receive_ts_ns=int(snap.receive_ts_ns),
                    bids=tuple(snap.bids),
                    asks=tuple(snap.asks),
                    market_generation=market_generation,
                    depth_generation=depth_generation,
                )
            except Exception as exc:
                logger.error(
                    "DEPTH_OBSERVER_FAILED callback=%s error=%s",
                    getattr(callback, "__qualname__", type(callback).__name__),
                    exc,
                )

    def add_depth_observer(self, callback) -> None:
        """Attach one non-mutating receive-time observer before WS startup."""

        if not callable(callback):
            raise TypeError("depth observer must be callable")
        with self._lock:
            if callback not in self._on_depth_callbacks:
                self._on_depth_callbacks.append(callback)

    def quote_decision_snapshot(
        self,
        *,
        now_ns: Optional[int] = None,
    ) -> QuoteDecisionSnapshot:
        """Freeze all local execution-book inputs for one quote decision."""

        lock_request_perf_ns = time.perf_counter_ns()
        with self._lock:
            lock_acquired_perf_ns = time.perf_counter_ns()
            # Take the live clock only after acquiring the state lock.  A
            # websocket update that won the lock immediately before this call
            # must not be misclassified as future-received.
            capture_ns = int(
                now_ns if now_ns is not None else time.time_ns()
            )
            depth = self._last_depth
            bids = tuple(depth.bids) if depth is not None else ()
            asks = tuple(depth.asks) if depth is not None else ()
            depth_exchange_ts_ms = int(depth.ts) if depth is not None else 0
            depth_receive_ts_ns = (
                int(depth.receive_ts_ns) if depth is not None else 0
            )
            bar_pricing_mid = (
                float(self._close_history[-1]) if self._close_history else 0.0
            )

            key = self._market_key(
                PERP_MARKET,
                self._symbol,
                venue=BINANCE_VENUE,
            )
            book_state = self._market_source_state.get(key, {})
            book_bid = float(book_state.get("last_bid", 0.0) or 0.0)
            book_ask = float(book_state.get("last_ask", 0.0) or 0.0)
            book_exchange_ts_ms = int(
                book_state.get("last_book_exchange_ts_ms", 0) or 0
            )
            book_receive_ts_ns = int(
                book_state.get("last_book_receive_ts_ns", 0) or 0
            )
            book_sequence = book_state.get("last_book_sequence")
            market_generation = int(self._quote_market_generation)
            depth_generation = int(self._depth_generation)
            book_generation = int(self._book_ticker_generation)
            # DepthSnapshot instances are append-only. Copy their references
            # under the lock and freeze the nested arrays after releasing it.
            depth_history_source = tuple(
                item
                for item in self._depth_history
                if int(item.receive_ts_ns) <= capture_ns
            )
            lock_hold_ns = time.perf_counter_ns() - lock_acquired_perf_ns

        lock_wait_ns = lock_acquired_perf_ns - lock_request_perf_ns
        depth_history = tuple(
            QuoteDepthObservation(
                exchange_ts_ms=int(item.ts),
                receive_ts_ns=int(item.receive_ts_ns),
                bids=tuple(item.bids),
                asks=tuple(item.asks),
            )
            for item in depth_history_source
        )

        best_bid = float(bids[0][0]) if bids else 0.0
        best_ask = float(asks[0][0]) if asks else 0.0
        mid = 0.5 * (best_bid + best_ask) if best_ask > best_bid > 0.0 else 0.0
        invalid_reason = ""
        if capture_ns <= 0:
            invalid_reason = "missing_capture_timestamp"
        elif market_generation <= 0 or depth_generation <= 0:
            invalid_reason = "missing_depth_generation"
        elif not bids or not asks:
            invalid_reason = "missing_depth_levels"
        elif not all(
            math.isfinite(value)
            for value in (best_bid, best_ask, mid)
        ):
            invalid_reason = "nonfinite_depth_top"
        elif best_bid <= 0.0 or best_ask <= best_bid:
            invalid_reason = "crossed_or_invalid_depth_top"
        elif float(bids[0][1]) <= 0.0 or float(asks[0][1]) <= 0.0:
            invalid_reason = "nonpositive_depth_top_quantity"
        elif depth_exchange_ts_ms <= 0:
            invalid_reason = "missing_depth_exchange_timestamp"
        elif depth_receive_ts_ns <= 0:
            invalid_reason = "missing_depth_receive_timestamp"
        elif depth_receive_ts_ns > capture_ns:
            invalid_reason = "depth_receive_after_snapshot"
        elif depth_exchange_ts_ms * 1_000_000 > depth_receive_ts_ns:
            invalid_reason = "depth_exchange_after_receive"

        return QuoteDecisionSnapshot(
            capture_ts_ns=capture_ns,
            market_generation=market_generation,
            depth_generation=depth_generation,
            book_ticker_generation=book_generation,
            depth_exchange_ts_ms=depth_exchange_ts_ms,
            depth_receive_ts_ns=depth_receive_ts_ns,
            bids=bids,
            asks=asks,
            best_bid=best_bid,
            best_ask=best_ask,
            mid=mid,
            bar_pricing_mid=bar_pricing_mid,
            book_ticker_bid=book_bid,
            book_ticker_ask=book_ask,
            book_ticker_exchange_ts_ms=book_exchange_ts_ms,
            book_ticker_receive_ts_ns=book_receive_ts_ns,
            book_ticker_sequence=(
                int(book_sequence) if book_sequence is not None else None
            ),
            depth_history=depth_history,
            lock_wait_ns=int(lock_wait_ns),
            lock_hold_ns=int(lock_hold_ns),
            valid=not invalid_reason,
            invalid_reason=invalid_reason,
        )

    def last_depth_age_s(self, now: Optional[float] = None) -> float:
        """Age in seconds of the latest usable execution depth snapshot."""
        now_ts = time.time() if now is None else now
        with self._lock:
            snap = self._last_depth
            if not snap or not snap.bids or not snap.asks or snap.ts <= 0:
                return float("inf")
            return max(0.0, now_ts - snap.ts / 1000.0)

    def execution_depth_state(
        self,
        *,
        now_ns: Optional[int] = None,
    ) -> dict[str, Any]:
        """Return a copy of the top-20 strategy feed for shadow consumers.

        The existing feature and stale-data paths continue to use
        ``_last_depth`` exactly as before. This accessor only exposes the same
        snapshot with its local receive timestamp for active-order research.
        """

        clock_ns = int(now_ns if now_ns is not None else time.time_ns())
        with self._lock:
            snap = self._last_depth
            if snap is None or not snap.bids or not snap.asks:
                return {
                    "valid": 0,
                    "exchange_ts_ms": 0,
                    "receive_ts_ns": 0,
                    "receive_age_ms": float("inf"),
                    "bids": (),
                    "asks": (),
                }
            receive_ns = int(snap.receive_ts_ns)
            return {
                "valid": int(receive_ns > 0),
                "exchange_ts_ms": int(snap.ts),
                "receive_ts_ns": receive_ns,
                "receive_age_ms": (
                    max(0.0, (clock_ns - receive_ns) / 1_000_000.0)
                    if receive_ns > 0
                    else float("inf")
                ),
                "bids": tuple(snap.bids),
                "asks": tuple(snap.asks),
            }

    @staticmethod
    def _market_key(
        market_type: str,
        symbol: str,
        venue: str = BINANCE_VENUE,
    ) -> str:
        return market_key(venue, market_type, symbol)

    def _record_book_ticker(
        self,
        key: str,
        bid: float,
        ask: float,
        event_time: float,
        receive_time_ms: Optional[float] = None,
    ):
        history = self._book_ticker_history.get(key)
        if history is None:
            history = deque(maxlen=3600)
            self._book_ticker_history[key] = history
        bucket_ts = int(event_time // 1000) * 1000
        receive_ms = float(
            receive_time_ms if receive_time_ms is not None else event_time
        )
        if history and history[-1][0] == bucket_ts:
            snapshot = history[-1]
            if isinstance(snapshot, list):
                snapshot[1] = bid
                snapshot[2] = ask
                snapshot[3] = event_time
                snapshot[4] = receive_ms
            else:
                history[-1] = [bucket_ts, bid, ask, event_time, receive_ms]
        else:
            history.append([bucket_ts, bid, ask, event_time, receive_ms])

    def _update_global_bridge_basis_locked(self, receive_time_ms: float) -> None:
        """Sample the Binance bridge basis once per completed 10s state bucket."""

        def fresh_mid(
            market_type: str,
            symbol: str,
            *,
            max_age_ms: float = GLOBAL_REFERENCE_SOURCE_MAX_AGE_MS,
        ) -> float:
            key = self._market_key(market_type, symbol, venue=BINANCE_VENUE)
            record = self._book_tickers.get(key)
            state = self._market_source_state.get(key, {})
            receive_ns = int(state.get("last_book_receive_ts_ns", 0) or 0)
            if record is None or receive_ns <= 0:
                return math.nan
            age_ms = receive_time_ms - receive_ns / 1_000_000.0
            if age_ms < 0.0 or age_ms > max_age_ms:
                return math.nan
            bid, ask = float(record[0]), float(record[1])
            return 0.5 * (bid + ask) if bid > 0.0 and ask > bid else math.nan

        execution_mid = fresh_mid(PERP_MARKET, self._symbol)
        reference_mid = fresh_mid(PERP_MARKET, self._reference_symbol)
        stablecoin_mid = fresh_mid(
            SPOT_MARKET,
            self._stablecoin_anchor_symbol,
            max_age_ms=GLOBAL_REFERENCE_ANCHOR_MAX_AGE_MS,
        )
        execution_spot_mid = fresh_mid(
            SPOT_MARKET,
            self._symbol,
            max_age_ms=GLOBAL_REFERENCE_ANCHOR_MAX_AGE_MS,
        )
        converted_bridge = (
            reference_mid / stablecoin_mid
            if reference_mid > 0.0 and stablecoin_mid > 0.0
            else math.nan
        )
        bridge_mid = (
            converted_bridge if math.isfinite(converted_bridge) else execution_spot_mid
        )
        if not (execution_mid > 0.0 and bridge_mid > 0.0):
            return
        raw_basis = math.log(execution_mid / bridge_mid) * 10_000.0
        bucket = int(receive_time_ms // 10_000)
        if self._global_bridge_basis_history and self._global_bridge_basis_history[-1][0] == bucket:
            self._global_bridge_basis_history[-1] = (bucket, raw_basis)
        else:
            self._global_bridge_basis_history.append((bucket, raw_basis))

    def _book_ticker_at(
        self,
        market_type: str,
        symbol: str,
        target_ts_ms: float,
        max_age_ms: float = 30_000.0,
        *,
        venue: str = BINANCE_VENUE,
    ):
        record = self._book_ticker_record_at(
            market_type,
            symbol,
            target_ts_ms,
            max_age_ms=max_age_ms,
            venue=venue,
        )
        return record[:3] if record is not None else None

    def _book_ticker_record_at(
        self,
        market_type: str,
        symbol: str,
        target_ts_ms: float,
        max_age_ms: float = 30_000.0,
        *,
        venue: str = BINANCE_VENUE,
    ):
        """Return an event only if it was locally received by the target time."""
        key = self._market_key(market_type, symbol, venue=venue)
        history = self._book_ticker_history.get(key)
        # Old in-process state can survive a config hot reload.  Keep these
        # fallbacks during the venue-key migration, but never write new data
        # into the ambiguous keys.
        if not history and normalize_venue(venue) == BINANCE_VENUE:
            history = self._book_ticker_history.get(f"{market_type}:{symbol.upper()}")
        if not history and market_type == PERP_MARKET:
            history = self._book_ticker_history.get(symbol.upper())
        if not history:
            return None

        for snapshot in reversed(history):
            bucket_ts, bid, ask, event_time = snapshot[:4]
            receive_time_ms = snapshot[4] if len(snapshot) >= 5 else event_time
            if bucket_ts > target_ts_ms:
                continue
            if receive_time_ms > target_ts_ms:
                continue
            if target_ts_ms - event_time > max_age_ms or target_ts_ms - receive_time_ms > max_age_ms:
                continue
            if bid > 0 and ask > bid:
                return bid, ask, event_time, receive_time_ms
            return None
        return None

    @staticmethod
    def _extract_metric_ts_ms(*payloads) -> int:
        candidates = []
        for payload in payloads:
            if isinstance(payload, list) and payload:
                payload = payload[0]
            if not isinstance(payload, dict):
                continue
            for key in ("timestamp", "time", "updateTime"):
                value = payload.get(key)
                if value is None:
                    continue
                try:
                    ts = int(float(value))
                except (TypeError, ValueError):
                    continue
                if ts > 0:
                    if ts < 10**12:
                        ts *= 1000
                    candidates.append(ts)
        return max(candidates) if candidates else int(time.time() * 1000)

    @staticmethod
    def _zero_metrics_features(f: dict):
        for name in FEATURE_NAMES:
            if any(x in name for x in ["oi_", "ls_ratio", "ls_zscore",
                                        "ls_momentum", "price_divergence"]):
                f.setdefault(name, 0.0)

    def _metrics_state_at(self, target_ts_ms: float):
        history = list(self._metrics_history)
        if not history:
            return None, []
        eligible = [entry for entry in history if entry.get("ts_ms", 0) <= target_ts_ms]
        if not eligible:
            return None, []
        return eligible[-1], eligible

    def on_book_ticker(
        self,
        event: dict,
        market_type: str = PERP_MARKET,
        *,
        venue: str = BINANCE_VENUE,
        receive_ts_ns: Optional[int] = None,
        sequence_number: Optional[int] = None,
    ):
        """Store latest best bid/ask by symbol for cross-market live features."""
        try:
            symbol = str(event.get("s", self._symbol)).upper()
            bid = float(event.get("b", 0.0))
            ask = float(event.get("a", 0.0))
            bid_size = float(event.get("B", 0.0) or 0.0)
            ask_size = float(event.get("A", 0.0) or 0.0)
            event_time = float(event.get("E", 0.0) or 0.0)
            receive_ns = int(receive_ts_ns or time.time_ns())
            receive_time_ms = receive_ns / 1_000_000.0
            if bid > 0 and ask > 0:
                normalized_venue = normalize_venue(venue)
                key = self._market_key(
                    market_type, symbol, venue=normalized_venue
                )
                flow_enabled = self._global_flow_shadow_enabled
                native_flow = flow_enabled and self._global_flow.native_enabled
                if native_flow:
                    try:
                        self._global_flow.on_book(
                            key,
                            receive_ts_ns=receive_ns,
                            bid=bid,
                            bid_size=bid_size,
                            ask=ask,
                            ask_size=ask_size,
                        )
                    except Exception as exc:
                        if _cpp_signal_strict():
                            raise
                        logger.warning("C++ global-flow book update failed: %s", exc)
                        return
                with self._lock:
                    latest = self._book_tickers.get(key)
                    if isinstance(latest, list):
                        latest[0] = bid
                        latest[1] = ask
                        latest[2] = event_time
                    else:
                        self._book_tickers[key] = [bid, ask, event_time]
                    self._record_book_ticker(
                        key, bid, ask, event_time, receive_time_ms=receive_time_ms
                    )
                    self._update_market_book_state_locked(
                        key,
                        venue=normalized_venue,
                        market_type=market_type,
                        symbol=symbol,
                        event_time_ms=int(event_time),
                        receive_ns=receive_ns,
                        bid=bid,
                        ask=ask,
                        sequence_number=sequence_number,
                    )
                    if (
                        normalized_venue == BINANCE_VENUE
                        and market_type == PERP_MARKET
                        and symbol == self._symbol
                    ):
                        self._quote_market_generation += 1
                        self._book_ticker_generation += 1
                    if flow_enabled and not native_flow:
                        self._global_flow.on_book(
                            key,
                            receive_ts_ns=receive_ns,
                            bid=bid,
                            bid_size=bid_size,
                            ask=ask,
                            ask_size=ask_size,
                        )
                    self._market_source_state[key][
                        "last_book_feature_ready_ts_ns"
                    ] = time.time_ns()
                    if (
                        self._global_reference_shadow_enabled
                        and normalized_venue == BINANCE_VENUE
                    ):
                        self._update_global_bridge_basis_locked(receive_time_ms)
                    if market_type == PERP_MARKET and normalized_venue == BINANCE_VENUE:
                        symbol_latest = self._book_tickers.get(symbol)
                        if isinstance(symbol_latest, list):
                            symbol_latest[0] = bid
                            symbol_latest[1] = ask
                            symbol_latest[2] = event_time
                        else:
                            self._book_tickers[symbol] = [bid, ask, event_time]
                        self._record_book_ticker(
                            symbol, bid, ask, event_time, receive_time_ms=receive_time_ms
                        )
        except Exception as exc:
            logger.debug(f"Bad bookTicker event: {exc}")

    def market_source_snapshot(self, now_ns: Optional[int] = None) -> dict[str, dict]:
        """Return copy-safe receive-time freshness for HEALTH/shadow audits."""
        current_ns = int(now_ns or time.time_ns())
        with self._lock:
            states = {key: dict(value) for key, value in self._market_source_state.items()}
        for state in states.values():
            book_ns = int(state.get("last_book_receive_ts_ns", 0) or 0)
            trade_ns = int(state.get("last_trade_receive_ts_ns", 0) or 0)
            state["book_receive_age_ms"] = (
                max(0.0, (current_ns - book_ns) / 1_000_000.0)
                if book_ns > 0
                else float("inf")
            )
            state["trade_receive_age_ms"] = (
                max(0.0, (current_ns - trade_ns) / 1_000_000.0)
                if trade_ns > 0
                else float("inf")
            )
        return states

    def cross_venue_fair_price_state(
        self,
        *,
        local_mid: float,
        now_ns: Optional[int] = None,
    ):
        """Return a causal fair-price shadow state without changing quotes."""

        decision_ns = int(now_ns or time.time_ns())
        sources: list[FairPriceSource] = []
        anchor_mid = math.nan
        anchor_ready_ns = 0
        with self._lock:
            for venue in (BITGET_VENUE, BYBIT_VENUE, OKX_VENUE):
                for market_type in (SPOT_MARKET, PERP_MARKET):
                    key = self._market_key(
                        market_type,
                        self._reference_symbol,
                        venue=venue,
                    )
                    state = self._market_source_state.get(key, {})
                    sources.append(
                        FairPriceSource(
                            venue=venue,
                            market_type=market_type,
                            bid=float(state.get("last_bid", 0.0) or 0.0),
                            ask=float(state.get("last_ask", 0.0) or 0.0),
                            exchange_ts_ns=int(
                                state.get("last_book_exchange_ts_ms", 0) or 0
                            )
                            * 1_000_000,
                            local_receive_ts_ns=int(
                                state.get("last_book_receive_ts_ns", 0) or 0
                            ),
                            feature_ready_ts_ns=int(
                                state.get("last_book_feature_ready_ts_ns", 0) or 0
                            ),
                        )
                    )

            anchor_key = self._market_key(
                SPOT_MARKET,
                self._stablecoin_anchor_symbol,
                venue=BINANCE_VENUE,
            )
            anchor_state = self._market_source_state.get(anchor_key, {})
            anchor_bid = float(anchor_state.get("last_bid", 0.0) or 0.0)
            anchor_ask = float(anchor_state.get("last_ask", 0.0) or 0.0)
            if anchor_bid > 0.0 and anchor_ask > anchor_bid:
                anchor_mid = 0.5 * (anchor_bid + anchor_ask)
            anchor_ready_ns = int(
                anchor_state.get("last_book_feature_ready_ts_ns", 0) or 0
            )

        return self._cross_venue_fair_price.observe(
            decision_ts_ns=decision_ns,
            local_mid=float(local_mid),
            stablecoin_mid=anchor_mid,
            stablecoin_feature_ready_ts_ns=anchor_ready_ns,
            sources=sources,
        )

    def global_flow_state(self, *, now_ns: Optional[int] = None):
        """Return receive-time cross-venue flow state for shadow diagnostics."""
        if not self._global_flow_shadow_enabled:
            raise RuntimeError("global-flow shadow disabled by config")
        current_ns = int(now_ns or time.time_ns())
        with self._lock:
            self._global_flow.set_symbols(
                execution_symbol=self._symbol,
                reference_symbol=self._reference_symbol,
            )
            return self._global_flow.snapshot(now_ns=current_ns)

    def global_flow_backend_snapshot(self) -> dict:
        """Return native/fallback counters for live HEALTH and soak gates."""
        if self._global_flow.native_enabled:
            raw = self._global_flow.backend_stats()
        else:
            with self._lock:
                raw = self._global_flow.backend_stats()
        fields = (
            "market_count",
            "trade_batches",
            "trade_events_seen",
            "trade_events_accepted",
            "book_events_seen",
            "book_events_accepted",
            "out_of_order_events",
            "stale_trade_events",
            "trade_overflow_events",
            "book_overflow_events",
        )
        return {
            "native": int(raw.get("native", 0)),
            **{name: int(raw.get(name, 0)) for name in fields},
        }

    def shadow_runtime_snapshot(self) -> dict:
        """Return fail-closed diagnostic evaluator identity for attestation."""
        backend = self.global_flow_backend_snapshot()
        return {
            "schema_version": "narrowgate_shadow_runtime_identity.v1",
            "global_flow_shadow_enabled": self._global_flow_shadow_enabled,
            "global_reference_shadow_enabled": self._global_reference_shadow_enabled,
            "global_flow_native_requested": self._cpp_global_flow_requested,
            "global_flow_native_effective": self._cpp_global_flow_enabled,
            "global_flow_backend": backend,
            "global_reference_bridge_basis_sample_count": len(
                self._global_bridge_basis_history
            ),
            "state_restore_contract": "shadow_state_never_restored",
        }

    def global_reference_state(
        self,
        *,
        now_ms: Optional[float] = None,
        horizon_s: float = 1.0,
        max_source_age_ms: float = GLOBAL_REFERENCE_SOURCE_MAX_AGE_MS,
        anchor_max_age_ms: float = GLOBAL_REFERENCE_ANCHOR_MAX_AGE_MS,
        tick_size: float = 0.1,
    ):
        """Build the six-tape global reference for HEALTH/shadow only."""
        if not self._global_reference_shadow_enabled:
            raise RuntimeError("global-reference shadow disabled by config")
        target_ms = float(now_ms if now_ms is not None else time.time() * 1000.0)
        prior_ms = target_ms - max(0.1, float(horizon_s)) * 1000.0

        def observation(venue: str, market_type: str, symbol: str) -> ReferenceObservation:
            current = self._book_ticker_record_at(
                market_type, symbol, target_ms, max_age_ms=max_source_age_ms, venue=venue
            )
            previous = self._book_ticker_record_at(
                market_type, symbol, prior_ms, max_age_ms=max_source_age_ms, venue=venue
            )
            if current is None or previous is None:
                return ReferenceObservation(math.nan, math.nan, math.inf, False)
            current_mid = 0.5 * (current[0] + current[1])
            previous_mid = 0.5 * (previous[0] + previous[1])
            receive_age = max(0.0, target_ms - float(current[3]))
            event_age = max(0.0, target_ms - float(current[2]))
            return ReferenceObservation(
                current_mid,
                previous_mid,
                max(receive_age, event_age),
                True,
            )

        with self._lock:
            venues = (BITGET_VENUE, BYBIT_VENUE, OKX_VENUE)
            external_spot = {
                venue: observation(venue, SPOT_MARKET, self._reference_symbol) for venue in venues
            }
            external_perp = {
                venue: observation(venue, PERP_MARKET, self._reference_symbol) for venue in venues
            }
            binance_bridge = observation(
                BINANCE_VENUE, PERP_MARKET, self._reference_symbol
            )
            execution = self._book_ticker_record_at(
                PERP_MARKET,
                self._symbol,
                target_ms,
                max_age_ms=max_source_age_ms,
                venue=BINANCE_VENUE,
            )
            execution_spot = self._book_ticker_record_at(
                SPOT_MARKET,
                self._symbol,
                target_ms,
                max_age_ms=anchor_max_age_ms,
                venue=BINANCE_VENUE,
            )
            stablecoin_spot = self._book_ticker_record_at(
                SPOT_MARKET,
                self._stablecoin_anchor_symbol,
                target_ms,
                max_age_ms=anchor_max_age_ms,
                venue=BINANCE_VENUE,
            )
            execution_mid = (
                0.5 * (execution[0] + execution[1]) if execution is not None else math.nan
            )
            execution_spot_mid = (
                0.5 * (execution_spot[0] + execution_spot[1])
                if execution_spot is not None else math.nan
            )
            stablecoin_spot_mid = (
                0.5 * (stablecoin_spot[0] + stablecoin_spot[1])
                if stablecoin_spot is not None else math.nan
            )
            target_bucket = int(target_ms // 10_000)
            prior_basis = [
                value
                for bucket, value in self._global_bridge_basis_history
                if bucket < target_bucket
            ]
            slow_basis = float(np.median(prior_basis)) if len(prior_basis) >= 30 else math.nan

        correction_cap_bps = (
            abs(float(tick_size)) / execution_mid * 10_000.0
            if execution_mid > 0.0 else 0.0
        )
        return build_global_reference_state(
            external_spot=external_spot,
            external_perp=external_perp,
            binance_btcusdt_perp=binance_bridge,
            execution_btcusdc_perp_mid=execution_mid,
            usdcusdt_mid=stablecoin_spot_mid,
            binance_btcusdc_spot_mid=execution_spot_mid,
            slow_bridge_basis_bps=slow_basis,
            max_source_age_ms=max_source_age_ms,
            correction_cap_bps=correction_cap_bps,
            bridge_basis_sample_count=len(prior_basis),
        )

    # ── feature computation ──

    def _pending_completed_bucket_starts(self, all_bars: Sequence[Bar1s]) -> List[int]:
        """Return every unprocessed completed 10s bucket in chronological order."""
        if not all_bars:
            return []
        latest_ts = int(all_bars[-1].ts)
        # A bar becomes eligible only after _finalize_bar has committed it.
        # Once second 9 is finalized, that 10s bucket is complete; no bar from
        # the next bucket is needed to prove completion.
        completed_exclusive_ms = ((latest_ts + 1_000) // 10_000) * 10_000
        if self._last_processed_bucket is None:
            oldest_ts = int(all_bars[0].ts)
            start_ms = (oldest_ts // 10_000) * 10_000
            if oldest_ts > start_ms:
                start_ms += 10_000
        else:
            start_ms = int(self._last_processed_bucket) + 10_000
        if start_ms >= completed_exclusive_ms:
            return []
        return list(range(start_ms, completed_exclusive_ms, 10_000))

    @staticmethod
    def _complete_bucket_bars(all_bars: Sequence[Bar1s], bucket_start_ms: int) -> List[Bar1s]:
        bucket_end_ms = int(bucket_start_ms) + 10_000
        bars = [
            bar for bar in all_bars
            if int(bucket_start_ms) <= int(bar.ts) < bucket_end_ms
        ]
        expected = list(range(int(bucket_start_ms), bucket_end_ms, 1_000))
        actual = [int(bar.ts) for bar in bars]
        if actual != expected:
            raise RuntimeError(
                "completed 10s feature bucket lacks an exact causal 1s grid: "
                f"bucket={bucket_start_ms} expected={expected} actual={actual}"
            )
        return bars

    def _process_completed_feature_buckets_locked(
        self,
        all_bars: Sequence[Bar1s],
    ) -> List[dict]:
        """Materialize every pending bucket against its own immutable cutoff."""
        outputs: List[dict] = []
        for bucket_start_ms in self._pending_completed_bucket_starts(all_bars):
            cutoff = FeatureCutoff(bucket_start_ms + 10_000)
            bars_10 = self._complete_bucket_bars(all_bars, bucket_start_ms)
            context_bars = cutoff.visible_bars(all_bars)
            features = self._compute_features(
                self._aggregate_bars(bars_10),
                context_bars,
                cutoff=cutoff,
            )
            history_row = self._history_snapshot(features)
            self._feat_history.append(history_row)
            if self._cpp_signal_features_enabled and self._cpp_signal is not None:
                engine = self._ensure_cpp_feature_engine(list(all_bars))
                if engine is not None:
                    engine.push_history(self._history_to_cpp(history_row))
            self._last_processed_bucket = int(bucket_start_ms)
            outputs.append(features)
        return outputs

    def compute_signal(self) -> Prediction:
        """
        Compute features from current buffers and run ML inference.
        Only produces a NEW prediction when a complete 10s bucket has elapsed
        since the last computation, matching the fixed-10s cadence used
        during offline training.  Intermediate calls return the cached prediction.

        Returns Prediction with dir/vol/ret estimates.
        """
        with self._lock:
            if len(self._bar_buffer) < 30:
                return self._last_prediction
            feature_batches = self._process_completed_feature_buckets_locked(
                list(self._bar_buffer)
            )

        if not feature_batches:
            return self._last_prediction

        # Catch-up inference remains chronological so stateful demeaning and
        # feature dumps see the same one-point-per-bucket stream as live.
        prediction = self._last_prediction
        for features in feature_batches:
            prediction = self._predict(features)

        with self._lock:
            self._last_prediction = prediction
        return prediction

    def _aggregate_bars(self, bars: List[Bar1s]) -> dict:
        """Aggregate list of 1s bars into a single 10s bar dict."""
        return {
            "ts": bars[-1].ts,
            "open": bars[0].open,
            "high": max(b.high for b in bars),
            "low": min(b.low for b in bars),
            "close": bars[-1].close,
            "volume": sum(b.volume for b in bars),
            "buy_volume": sum(b.buy_volume for b in bars),
            "sell_volume": sum(b.sell_volume for b in bars),
            "trade_count": sum(b.trade_count for b in bars),
            "buy_count": sum(b.buy_count for b in bars),
            "sell_count": sum(b.sell_count for b in bars),
            "quote_qty": sum(b.quote_qty for b in bars),
            "buy_quote_qty": sum(b.buy_quote_qty for b in bars),
            "sell_quote_qty": sum(b.sell_quote_qty for b in bars),
            "max_same_side_run": max(b.max_same_side_run for b in bars),
        }

    def _fill_cross_market_book_features(self, f: dict, prefix: str, market_type: str,
                                         symbol: str, close: float, target_ts_ms: float):
        ticker = self._book_ticker_at(market_type, symbol, target_ts_ms)
        if not ticker or close <= 0:
            return

        bid, ask, event_time = ticker
        if bid <= 0 or ask <= bid:
            return

        mid = 0.5 * (bid + ask)
        age_s = max(0.0, (float(target_ts_ms) - float(event_time)) / 1000.0)
        if age_s > CROSS_SOURCE_MAX_AGE_S:
            return
        f[f"{prefix}_age_s"] = age_s
        f[f"{prefix}_available"] = 1.0
        f[f"{prefix}_basis_bps"] = (mid - close) / close * 10000.0
        for seconds, suffix in [(10_000, "10s"), (30_000, "30s"), (60_000, "60s")]:
            prev_ticker = self._book_ticker_at(market_type, symbol, target_ts_ms - seconds)
            if prev_ticker is not None:
                prev_mid = 0.5 * (prev_ticker[0] + prev_ticker[1])
                f[f"{prefix}_ret_{suffix}"] = self._safe_log_return(mid, prev_mid)

        mids = []
        for step in range(WINDOWS_10S["60s"], -1, -1):
            snap = self._book_ticker_at(market_type, symbol, target_ts_ms - step * 10_000)
            if snap is None:
                continue
            mids.append(0.5 * (snap[0] + snap[1]))
        if len(mids) >= 3:
            ret_window = [
                self._safe_log_return(mids[i], mids[i - 1])
                for i in range(1, len(mids))
            ]
            if len(ret_window) >= 2:
                f[f"{prefix}_volatility_60s"] = float(np.std(ret_window, ddof=1))

    def _cross_recent_bars(self, key: str, target_ts_ms: float, window_ms: int) -> list[Bar1s]:
        self._sync_cpp_cross_current_bar_locked(key)
        bars = list(self._cross_bar_buffers.get(key, ()))
        current = self._cross_current_bars.get(key)
        if current is not None:
            bars.append(current)
        start_ts = target_ts_ms - window_ms + 1
        return [bar for bar in bars if start_ts <= bar.ts <= target_ts_ms]

    @staticmethod
    def _bucket_cross_flow(bars: list[Bar1s]) -> dict[int, tuple[float, float, int]]:
        buckets: dict[int, list[float]] = {}
        for bar in bars:
            bucket = bar.ts // 10000
            values = buckets.setdefault(bucket, [0.0, 0.0, 0])
            values[0] += bar.buy_volume
            values[1] += bar.sell_volume
            values[2] += bar.trade_count
        return {bucket: (values[0], values[1], int(values[2])) for bucket, values in buckets.items()}

    def _fill_cross_market_trade_features(self, f: dict, prefix: str, market_type: str,
                                          symbol: str, target_ts_ms: float, target_close: float):
        key = self._market_key(market_type, symbol)
        bars_10s = self._cross_recent_bars(key, target_ts_ms, 10_000)
        if bars_10s:
            latest_ts = max(float(bar.ts) for bar in bars_10s)
            age_s = max(0.0, (float(target_ts_ms) - latest_ts) / 1000.0)
            if age_s <= CROSS_SOURCE_MAX_AGE_S and f.get(f"{prefix}_available", 0.0) <= 0.0:
                f[f"{prefix}_age_s"] = age_s
                f[f"{prefix}_available"] = 1.0
                source_close = float(bars_10s[-1].close)
                if source_close > 0.0 and target_close > 0.0:
                    f[f"{prefix}_basis_bps"] = (source_close - target_close) / target_close * 10000.0
            buy = sum(bar.buy_volume for bar in bars_10s)
            sell = sum(bar.sell_volume for bar in bars_10s)
            total = buy + sell
            if total > 0:
                f[f"{prefix}_volume_imbalance"] = (buy - sell) / total

        bars_60s = self._cross_recent_bars(key, target_ts_ms, 60_000)
        buckets = self._bucket_cross_flow(bars_60s)
        if not buckets:
            return

        trade_counts = [trade_count for _, _, trade_count in buckets.values()]
        f[f"{prefix}_trade_intensity_60s"] = float(np.mean(trade_counts)) if trade_counts else 0.0

        total_volume = 0.0
        abs_imbalance = 0.0
        for buy, sell, _ in buckets.values():
            total_volume += buy + sell
            abs_imbalance += abs(buy - sell)
        if total_volume > 0:
            f[f"{prefix}_vpin_60s"] = abs_imbalance / total_volume

    def _update_cross_basis_residual(self, f: dict, prefix: str, target_ts_ms: float):
        if f.get(f"{prefix}_available", 0.0) <= 0.0:
            return
        basis = float(f.get(f"{prefix}_basis_bps", 0.0))
        if not np.isfinite(basis):
            return
        history = self._cross_basis_history.setdefault(
            prefix,
            deque(maxlen=CROSS_BASIS_WINDOW_10S),
        )
        prior = [value for _, value in history]
        if len(prior) >= CROSS_BASIS_MIN_PERIODS:
            f[f"{prefix}_basis_residual_bps"] = basis - float(np.median(prior))
        bucket = int(float(target_ts_ms) // 10_000)
        if history and history[-1][0] == bucket:
            history[-1] = (bucket, basis)
        else:
            history.append((bucket, basis))

    def _compute_cross_market_features(self, f: dict, close: float, target_ts_ms: float):
        for prefix in ["cv_ref_perp", "cv_exec_spot", "cv_ref_spot"]:
            for suffix in CROSS_FEATURE_SUFFIXES:
                f[f"{prefix}_{suffix}"] = 0.0
            f[f"{prefix}_age_s"] = CROSS_SOURCE_MAX_AGE_S + 10.0

        self._fill_cross_market_book_features(
            f, "cv_ref_perp", PERP_MARKET, self._reference_symbol, close, target_ts_ms
        )
        self._fill_cross_market_trade_features(
            f, "cv_ref_perp", PERP_MARKET, self._reference_symbol, target_ts_ms, close
        )
        self._fill_cross_market_book_features(
            f, "cv_exec_spot", SPOT_MARKET, self._symbol, close, target_ts_ms
        )
        self._fill_cross_market_trade_features(
            f, "cv_exec_spot", SPOT_MARKET, self._symbol, target_ts_ms, close
        )
        if self._reference_symbol != self._symbol:
            self._fill_cross_market_book_features(
                f, "cv_ref_spot", SPOT_MARKET, self._reference_symbol, close, target_ts_ms
            )
            self._fill_cross_market_trade_features(
                f, "cv_ref_spot", SPOT_MARKET, self._reference_symbol, target_ts_ms, close
            )
        for prefix in ["cv_ref_perp", "cv_exec_spot", "cv_ref_spot"]:
            self._update_cross_basis_residual(f, prefix, target_ts_ms)

    def _compute_cpp_feature_overlay(
        self,
        bar_10s: dict,
        all_bars: List[Bar1s],
        cutoff: Optional[FeatureCutoff] = None,
    ) -> Optional[dict]:
        if not self._cpp_signal_features_enabled or self._cpp_signal is None:
            return None
        try:
            cutoff = cutoff or FeatureCutoff(int(bar_10s.get("ts", 0)) + 1_000)
            cpp_bar_10s = self._cpp_signal.Bar1s()
            cpp_bar_10s.ts_ms = int(bar_10s.get("ts", 0))
            cpp_bar_10s.open = float(bar_10s.get("open", 0.0))
            cpp_bar_10s.high = float(bar_10s.get("high", 0.0))
            cpp_bar_10s.low = float(bar_10s.get("low", 0.0))
            cpp_bar_10s.close = float(bar_10s.get("close", 0.0))
            cpp_bar_10s.volume = float(bar_10s.get("volume", 0.0))
            cpp_bar_10s.buy_volume = float(bar_10s.get("buy_volume", 0.0))
            cpp_bar_10s.sell_volume = float(bar_10s.get("sell_volume", 0.0))
            cpp_bar_10s.trade_count = float(bar_10s.get("trade_count", 0.0))
            cpp_bar_10s.buy_count = float(bar_10s.get("buy_count", 0.0))
            cpp_bar_10s.sell_count = float(bar_10s.get("sell_count", 0.0))
            cpp_bar_10s.quote_qty = float(bar_10s.get("quote_qty", 0.0))
            cpp_bar_10s.buy_quote_qty = float(bar_10s.get("buy_quote_qty", 0.0))
            cpp_bar_10s.sell_quote_qty = float(bar_10s.get("sell_quote_qty", 0.0))
            cpp_bar_10s.max_same_side_run = float(bar_10s.get("max_same_side_run", 0.0))

            # Python feature code only needs close/sign history up to 320 bars
            # and taker tempo up to 60 bars; sending the full 3700s buffer over
            # pybind is slower than the pure Python path.
            engine = self._ensure_cpp_feature_engine(all_bars)
            if engine is None:
                return None
            if self._cpp_signal_feature_names and hasattr(engine, "compute_values_at_cutoff"):
                values = np.asarray(
                    engine.compute_values_at_cutoff(
                        cpp_bar_10s,
                        int(cutoff.cutoff_exclusive_ms),
                    ),
                    dtype=np.float64,
                )
                return dict(zip(self._cpp_signal_feature_names, values.tolist()))
            if hasattr(engine, "compute_at_cutoff"):
                return dict(
                    engine.compute_at_cutoff(
                        cpp_bar_10s,
                        int(cutoff.cutoff_exclusive_ms),
                    )
                )
            raise RuntimeError("narrowgate_cpp lacks cutoff-aware signal feature ABI")
        except Exception as exc:
            if _cpp_signal_strict():
                raise
            logger.warning("C++ signal feature overlay disabled after error: %s", exc)
            self._cpp_signal_features_enabled = False
            return None

    def _compute_features(
        self,
        bar_10s: dict,
        all_bars: List[Bar1s],
        *,
        cutoff: Optional[FeatureCutoff] = None,
    ) -> dict:
        """Compute all 88 base features from one causal cutoff view."""
        f = {}
        close = bar_10s["close"]
        target_ts_ms = int(
            bar_10s.get("ts", all_bars[-1].ts if all_bars else time.time() * 1000)
        )
        cutoff = cutoff or FeatureCutoff(target_ts_ms + 1_000)
        all_bars = cutoff.visible_bars(all_bars)
        if not all_bars:
            raise RuntimeError("feature cutoff excludes every completed 1s bar")
        if target_ts_ms >= cutoff.cutoff_exclusive_ms:
            raise RuntimeError(
                "10s feature target must precede its exclusive causal cutoff: "
                f"target={target_ts_ms} cutoff={cutoff.cutoff_exclusive_ms}"
            )
        f["_feature_ts_ms"] = float(target_ts_ms)
        f["_feature_cutoff_exclusive_ms"] = float(cutoff.cutoff_exclusive_ms)

        # === A1. Basic OHLCV ===
        f["close"] = close
        f["volume"] = bar_10s["volume"]
        f["buy_volume"] = bar_10s["buy_volume"]
        f["sell_volume"] = bar_10s["sell_volume"]
        f["trade_count"] = bar_10s["trade_count"]
        f["buy_count"] = bar_10s["buy_count"]
        f["sell_count"] = bar_10s["sell_count"]

        cpp_overlay = self._compute_cpp_feature_overlay(bar_10s, all_bars, cutoff)
        if cpp_overlay is not None:
            f.update(cpp_overlay)
            self._compute_execution_l2_features(
                f,
                bucket_end_ms=int(cutoff.cutoff_exclusive_ms),
            )
            self._compute_cross_market_features(f, close, target_ts_ms)
            self._compute_time_features(f, bar_ts_ms=target_ts_ms)
            self._compute_metrics_features(f, target_ts_ms)
            return f

        # === A2. Tick momentum (from 1s history) ===
        causal_tail = all_bars[-320:]
        closes = [float(bar.close) for bar in causal_tail]
        signs = [0.0]
        signs.extend(
            1.0 if closes[index] > closes[index - 1]
            else (-1.0 if closes[index] < closes[index - 1] else 0.0)
            for index in range(1, len(closes))
        )
        n_s = len(signs)

        # streak — forward cumulative (matches offline feature_engineer.py)
        # Offline: streak[i] = streak[i-1] + sign[i] if same direction, else sign[i]
        # We compute the streak ending at the last 1s bar.
        streak = 0.0
        if n_s >= 2:
            streak = signs[-1]
            for i in range(n_s - 2, -1, -1):
                if signs[i + 1] == signs[i] and signs[i] != 0:
                    streak += signs[i]
                else:
                    break
        elif n_s == 1:
            streak = signs[-1]
        f["tick_streak"] = streak

        # momentum sums
        f["tick_mom_3s"] = sum(signs[-3:]) if n_s >= 3 else sum(signs)
        f["tick_mom_5s"] = sum(signs[-5:]) if n_s >= 5 else sum(signs)
        f["tick_mom_10s"] = sum(signs[-10:]) if n_s >= 10 else sum(signs)

        # EWM of 1s returns
        f["tick_ewm_3s"] = self._ewm(closes, 3)
        f["tick_ewm_10s"] = self._ewm(closes, 10)

        # micro return distribution
        rets_1s = self._diffs(closes, min(10, len(closes)))
        if len(rets_1s) >= 3:
            arr = np.array(rets_1s)
            f["micro_ret_std"] = float(np.std(arr))
            n = len(arr)
            if n >= 5:
                m = np.mean(arr)
                s = np.std(arr)
                if s > 1e-12:
                    f["micro_ret_skew"] = float(np.mean(((arr - m) / s) ** 3))
                    f["micro_ret_kurt"] = float(np.mean(((arr - m) / s) ** 4) - 3)
                else:
                    f["micro_ret_skew"] = 0.0
                    f["micro_ret_kurt"] = 0.0
            else:
                f["micro_ret_skew"] = 0.0
                f["micro_ret_kurt"] = 0.0
        else:
            f["micro_ret_std"] = 0.0
            f["micro_ret_skew"] = 0.0
            f["micro_ret_kurt"] = 0.0

        # tick reversal frequency
        if n_s >= 3:
            changes = sum(1 for i in range(1, min(10, n_s))
                          if signs[-(i)] != signs[-(i + 1)])
            f["tick_reversal_freq"] = changes / min(10, n_s - 1)
        else:
            f["tick_reversal_freq"] = 0.0

        # flow velocity & acceleration
        # Offline: flow_velocity = cum_signed_vol.diff(1) = per-1s-bar signed vol,
        # resampled to 10s via .last() → the last 1s bar's signed volume.
        # flow_acceleration = flow_velocity.diff(1).resample("10s").last()
        bars_10 = all_bars[-10:]
        last_1s = bars_10[-1] if bars_10 else None
        if last_1s:
            f["flow_velocity"] = last_1s.buy_volume - last_1s.sell_volume
        else:
            f["flow_velocity"] = 0.0
        # acceleration vs previous 10s bar's flow_velocity
        fh = list(self._feat_history)
        if fh:
            prev_vel = fh[-1].get("flow_velocity", 0)
            f["flow_acceleration"] = f["flow_velocity"] - prev_vel
        else:
            f["flow_acceleration"] = 0.0

        # streak max & mom range (over last 10 1s bars)
        if n_s >= 10:
            last10_signs = signs[-10:]
            streaks = self._compute_streaks(last10_signs)
            f["tick_streak_max"] = max(abs(s) for s in streaks) if streaks else 0.0

            mom5_vals = [sum(signs[max(0, i - 4):i + 1])
                         for i in range(max(0, n_s - 10), n_s)]
            f["tick_mom_range"] = max(mom5_vals) - min(mom5_vals) if mom5_vals else 0.0
        else:
            f["tick_streak_max"] = abs(streak)
            f["tick_mom_range"] = 0.0

        # === A3b. Execution-L2 summary features (live-parity with offline) ===
        self._compute_execution_l2_features(
            f,
            bucket_end_ms=int(cutoff.cutoff_exclusive_ms),
        )

        # === A4. Microstructure features (rolling on 10s bars) ===
        self._compute_micro_features(f, bar_10s, all_bars)

        # === A4b. Cross-market features (reference perp + optional spot placeholders) ===
        self._compute_cross_market_features(f, close, target_ts_ms)

        # === A5. Time features ===
        # Use the latest bar's timestamp to match offline behavior
        self._compute_time_features(f, bar_ts_ms=target_ts_ms)

        # === A6. Metrics features ===
        self._compute_metrics_features(f, target_ts_ms)

        return f

    @staticmethod
    def _snapshot_l2_state(snap: Optional[DepthSnapshot]):
        if snap is None or not snap.bids or not snap.asks:
            return None

        depth = min(len(snap.bids), len(snap.asks), 10)
        if depth <= 0:
            return None

        bid_px = np.array([snap.bids[i][0] for i in range(depth)], dtype=np.float64)
        bid_qty = np.array([max(snap.bids[i][1], 0.0) for i in range(depth)], dtype=np.float64)
        ask_px = np.array([snap.asks[i][0] for i in range(depth)], dtype=np.float64)
        ask_qty = np.array([max(snap.asks[i][1], 0.0) for i in range(depth)], dtype=np.float64)

        best_bid = bid_px[0]
        best_ask = ask_px[0]
        mid = 0.5 * (best_bid + best_ask)
        if best_bid <= 0.0 or best_ask <= best_bid or mid <= 0.0:
            return None

        bid_cum = np.cumsum(bid_qty)
        ask_cum = np.cumsum(ask_qty)
        level_qty = bid_qty + ask_qty

        def _imbalance(level: int) -> float:
            idx = min(level, depth) - 1
            bid_sum = bid_cum[idx]
            ask_sum = ask_cum[idx]
            total = bid_sum + ask_sum
            return (bid_sum - ask_sum) / total if total > 0 else 0.0

        near_depth = bid_cum[min(3, depth) - 1] + ask_cum[min(3, depth) - 1]
        total_depth = bid_cum[-1] + ask_cum[-1]
        micro_den = bid_qty[0] + ask_qty[0]
        microprice = ((best_ask * bid_qty[0] + best_bid * ask_qty[0]) / micro_den) if micro_den > 0 else mid

        front = level_qty[:min(3, depth)]
        middle = level_qty[3:min(7, depth)]
        back = level_qty[7:depth]
        front_mean = float(front.mean()) if len(front) else 0.0
        mid_mean = float(middle.mean()) if len(middle) else 0.0
        back_mean = float(back.mean()) if len(back) else 0.0
        convexity_den = front_mean + mid_mean + back_mean

        return ({
            "l2_spread_bps": (best_ask - best_bid) / mid * 10000.0,
            "l2_microprice_offset_bps": (microprice - mid) / mid * 10000.0,
            "l2_imbalance_l1": _imbalance(1),
            "l2_imbalance_l3": _imbalance(3),
            "l2_imbalance_l5": _imbalance(5),
            "l2_imbalance_l10": _imbalance(10),
            "l2_near_depth_total": float(near_depth),
            "l2_depth_slope": float(near_depth / total_depth) if total_depth > 0 else 0.0,
            "l2_depth_convexity": float((front_mean - 2.0 * mid_mean + back_mean) / convexity_den) if convexity_den > 0 else 0.0,
            "l2_queue_concentration": float(level_qty[0] / near_depth) if near_depth > 0 else 0.0,
        }, float(total_depth), float(best_bid), float(best_ask))

    @staticmethod
    def _set_execution_l2_defaults(f: dict):
        for name in EXECUTION_L2_FEATURE_COLS:
            f[name] = 0.0

    def _compute_execution_l2_features(self, f: dict, bucket_end_ms: int):
        bucket_start_ms = bucket_end_ms - 10_000
        snapshots = list(self._depth_history)
        if not snapshots:
            self._set_execution_l2_defaults(f)
            return

        state_snap = None
        for snap in reversed(snapshots):
            if snap.ts < bucket_end_ms:
                state_snap = snap
                break
        state_summary = self._snapshot_l2_state(state_snap)
        if state_summary is None:
            self._set_execution_l2_defaults(f)
            return

        state_features, _, _, _ = state_summary
        for key, value in state_features.items():
            f[key] = value

        prev_snap = None
        for snap in reversed(snapshots):
            if snap.ts < bucket_start_ms:
                prev_snap = snap
                break

        prev_summary = self._snapshot_l2_state(prev_snap)
        prev_depth = prev_summary[1] if prev_summary is not None else None
        prev_bid = prev_summary[2] if prev_summary is not None else None
        prev_ask = prev_summary[3] if prev_summary is not None else None

        flip_count = 0.0
        refresh_sum = 0.0
        cancel_sum = 0.0
        sample_count = 0
        for snap in snapshots:
            if snap.ts < bucket_start_ms or snap.ts >= bucket_end_ms:
                continue
            summary = self._snapshot_l2_state(snap)
            if summary is None:
                continue
            _, total_depth, best_bid, best_ask = summary
            if prev_bid is not None and prev_ask is not None:
                if best_bid != prev_bid or best_ask != prev_ask:
                    flip_count += 1.0
            if prev_depth is not None and prev_depth > 0:
                delta_depth = total_depth - prev_depth
                if delta_depth > 0:
                    refresh_sum += delta_depth / prev_depth
                elif delta_depth < 0:
                    cancel_sum += -delta_depth / prev_depth
            prev_depth = total_depth
            prev_bid = best_bid
            prev_ask = best_ask
            sample_count += 1

        if sample_count > 0:
            f["l2_quote_flip_rate"] = flip_count / sample_count
            f["l2_book_refresh_ratio"] = refresh_sum / sample_count
            f["l2_book_cancel_ratio"] = cancel_sum / sample_count
        else:
            f["l2_quote_flip_rate"] = 0.0
            f["l2_book_refresh_ratio"] = 0.0
            f["l2_book_cancel_ratio"] = 0.0

    def _compute_micro_features(self, f: dict, bar_10s: dict, all_bars: List[Bar1s]):
        """Compute microstructure features using rolling windows."""
        close = bar_10s["close"]

        # Collect 10s bar-equivalent data from feature history
        fh = list(self._feat_history)
        prev_closes = [h.get("close", close) for h in fh]
        prev_closes.append(close)

        # Log returns on 10s bars
        if len(prev_closes) >= 2:
            log_ret = self._safe_log_return(prev_closes[-1], prev_closes[-2])
        else:
            log_ret = 0.0

        recent_5s = list(all_bars[-5:])
        recent_6_closes = [bar.close for bar in all_bars[-6:]]
        returns_5s = [
            self._safe_log_return(recent_6_closes[i], recent_6_closes[i - 1])
            for i in range(1, len(recent_6_closes))
        ]
        if len(returns_5s) >= 2:
            f["volatility_5s"] = float(
                np.std(np.asarray(returns_5s), ddof=1) * math.sqrt(5.0)
            )
        else:
            f["volatility_5s"] = 0.0
        buy_5s = sum(bar.buy_volume for bar in recent_5s)
        sell_5s = sum(bar.sell_volume for bar in recent_5s)
        total_5s = buy_5s + sell_5s
        f["volume_imbalance_5s"] = (
            (buy_5s - sell_5s) / total_5s if total_5s > 0.0 else 0.0
        )
        f["trade_intensity_5s"] = (
            float(np.mean([bar.trade_count for bar in recent_5s]))
            if recent_5s else 0.0
        )
        f["vpin_5s"] = (
            sum(abs(bar.buy_volume - bar.sell_volume) for bar in recent_5s) / total_5s
            if total_5s > 0.0 else 0.0
        )
        f["price_change_5s"] = (
            recent_6_closes[-1] / recent_6_closes[0] - 1.0
            if len(recent_6_closes) >= 6 and recent_6_closes[0] > 0.0 else 0.0
        )

        # 1. Realized volatility (30s+ windows on completed 10s returns)
        all_log_rets = []
        for i in range(1, len(prev_closes)):
            all_log_rets.append(self._safe_log_return(prev_closes[i], prev_closes[i - 1]))

        for label, w in WINDOWS_10S.items():
            window_rets = all_log_rets[-w:] if len(all_log_rets) >= w else all_log_rets
            if len(window_rets) >= 2:
                arr = np.array(window_rets)
                f[f"volatility_{label}"] = float(np.std(arr, ddof=1) * math.sqrt(len(arr)))
            else:
                f[f"volatility_{label}"] = 0.0

        # 2. Volume imbalance
        total_vol = bar_10s["buy_volume"] + bar_10s["sell_volume"]
        f["volume_imbalance"] = (bar_10s["buy_volume"] - bar_10s["sell_volume"]) / total_vol if total_vol > 0 else 0.0

        # Rolling volume imbalance
        for label, w in WINDOWS_10S.items():
            buy_sum = sum(h.get("buy_volume", 0) for h in fh[-(w-1):]) + bar_10s["buy_volume"]
            sell_sum = sum(h.get("sell_volume", 0) for h in fh[-(w-1):]) + bar_10s["sell_volume"]
            total = buy_sum + sell_sum
            f[f"volume_imbalance_{label}"] = (buy_sum - sell_sum) / total if total > 0 else 0.0

        # 3. Trade intensity
        for label, w in WINDOWS_10S.items():
            trade_counts = [h.get("trade_count", 0) for h in fh[-(w-1):]]
            trade_counts.append(bar_10s["trade_count"])
            f[f"trade_intensity_{label}"] = np.mean(trade_counts)

        # 4. VPIN
        for label, w in WINDOWS_10S.items():
            abs_imbs = [abs(h.get("buy_volume", 0) - h.get("sell_volume", 0))
                        for h in fh[-(w-1):]]
            abs_imbs.append(abs(bar_10s["buy_volume"] - bar_10s["sell_volume"]))
            totals = [h.get("buy_volume", 0) + h.get("sell_volume", 0)
                      for h in fh[-(w-1):]]
            totals.append(total_vol)
            sum_abs = sum(abs_imbs)
            sum_total = sum(totals)
            f[f"vpin_{label}"] = sum_abs / sum_total if sum_total > 0 else 0.0

        f.update(self.compute_taker_tempo_features_from_bars(all_bars))

        # 5. Price velocity & acceleration
        f["price_velocity"] = close - prev_closes[-2] if len(prev_closes) >= 2 else 0.0
        prev_vel = fh[-1].get("price_velocity", 0) if fh else 0.0
        f["price_acceleration"] = f["price_velocity"] - prev_vel

        # Price change (multi-window pct)
        for label, w in WINDOWS_10S.items():
            if len(prev_closes) > w:
                old = prev_closes[-(w + 1)]
                f[f"price_change_{label}"] = (close - old) / old if old > 0 else 0.0
            else:
                f[f"price_change_{label}"] = 0.0

        # 6. Avg trade size
        tc = bar_10s["trade_count"]
        avg_size = bar_10s["volume"] / tc if tc > 0 else 0.0
        f["avg_trade_size"] = avg_size

        # Rolling avg trade size (60s)
        w60 = WINDOWS_10S["60s"]
        sizes = [h.get("avg_trade_size", 0) for h in fh[-(w60-1):]]
        sizes.append(avg_size)
        f["avg_trade_size_60s"] = np.mean(sizes) if sizes else avg_size
        f["large_trade_ratio"] = avg_size / f["avg_trade_size_60s"] if f["avg_trade_size_60s"] > 0 else 1.0

        # 7. Volume zscore
        vol_history = [h.get("volume", 0) for h in fh[-29:]]
        vol_history.append(bar_10s["volume"])
        if len(vol_history) >= 3:
            arr = np.array(vol_history)
            m, s = arr.mean(), arr.std()
            f["volume_zscore"] = (bar_10s["volume"] - m) / s if s > 0 else 0.0
        else:
            f["volume_zscore"] = 0.0

        # 8. Bar spread
        f["bar_spread"] = bar_10s["high"] - bar_10s["low"]
        f["bar_spread_bps"] = f["bar_spread"] / close * 10000 if close > 0 else 0.0

        # 9. Returns
        f["return_1"] = log_ret
        f["return_abs"] = abs(log_ret)

        # 10. Vol regime (from return_abs history)
        # Matches offline: vol_regime_6h = rolling(2160).mean of return_abs
        #                  vol_regime_24h = rolling(8640).mean of return_abs
        #                  vol_regime_zscore = (vol_6h - 7d_mean) / 7d_std
        abs_rets = [h.get("return_abs", 0) for h in fh]
        abs_rets.append(abs(log_ret))
        n_abs = len(abs_rets)
        arr_abs = np.array(abs_rets)

        # 6h window = 2160 10s bars
        if n_abs >= 360:  # min_periods=360 in offline
            f["vol_regime_6h"] = float(np.mean(arr_abs[-2160:])) if n_abs >= 2160 else float(np.mean(arr_abs))
        else:
            f["vol_regime_6h"] = float(np.mean(arr_abs)) if n_abs >= 3 else abs(log_ret)

        # 24h window = 8640 10s bars
        if n_abs >= 2160:  # min_periods=2160 in offline
            f["vol_regime_24h"] = float(np.mean(arr_abs[-8640:])) if n_abs >= 8640 else float(np.mean(arr_abs))
        else:
            f["vol_regime_24h"] = f["vol_regime_6h"]  # best approx until enough data

        # zscore: (vol_6h - 7d_mean) / 7d_std, 7d = 60480 10s bars
        # Compute using available vol_regime_6h history from feat_history
        vol6h_history = [h.get("vol_regime_6h", 0) for h in fh if "vol_regime_6h" in h]
        if len(vol6h_history) >= 8640:  # min_periods=8640 in offline
            arr_v6h = np.array(vol6h_history[-60480:])
            m_val, s_val = arr_v6h.mean(), arr_v6h.std()
            f["vol_regime_zscore"] = float((f["vol_regime_6h"] - m_val) / s_val) if s_val > 0 else 0.0
        elif n_abs >= 3:
            m_val = np.mean(arr_abs)
            s_val = np.std(arr_abs)
            f["vol_regime_zscore"] = float((arr_abs[-1] - m_val) / s_val) if s_val > 0 else 0.0
        else:
            f["vol_regime_zscore"] = 0.0

    @staticmethod
    def _side_sweep_bps(bar: Bar1s, side: str) -> float:
        if side == "buy":
            high = bar.buy_price_high
            low = bar.buy_price_low
            quote_qty = bar.buy_quote_qty
        else:
            high = bar.sell_price_high
            low = bar.sell_price_low
            quote_qty = bar.sell_quote_qty
        if high <= 0.0 or low <= 0.0 or quote_qty <= 0.0:
            return 0.0
        mid = (high + low) / 2.0
        return ((high - low) / mid * 10000.0) if mid > 0.0 else 0.0

    @classmethod
    def compute_taker_tempo_features_from_bars(cls, bars: List[Bar1s]) -> dict:
        if not bars:
            return {col: 0.0 for col in TAKER_TEMPO_FEATURE_COLS}

        out = {}
        for window in TAKER_TEMPO_WINDOWS_SEC:
            recent = bars[-window:]
            buy_quote = sum(bar.buy_quote_qty for bar in recent)
            sell_quote = sum(bar.sell_quote_qty for bar in recent)
            signed_quote = buy_quote - sell_quote
            total_quote = buy_quote + sell_quote
            trade_count = sum(bar.trade_count for bar in recent)
            max_run = max((bar.max_same_side_run for bar in recent), default=0)
            buy_sweep = max((cls._side_sweep_bps(bar, "buy") for bar in recent), default=0.0)
            sell_sweep = max((cls._side_sweep_bps(bar, "sell") for bar in recent), default=0.0)
            buy_iceberg = sum(
                bar.buy_count / (1.0 + cls._side_sweep_bps(bar, "buy"))
                for bar in recent
            )
            sell_iceberg = sum(
                bar.sell_count / (1.0 + cls._side_sweep_bps(bar, "sell"))
                for bar in recent
            )

            out[f"taker_quote_imbalance_{window}s"] = signed_quote / total_quote if total_quote > 0.0 else 0.0
            out[f"taker_signed_quote_sum_{window}s"] = signed_quote
            out[f"taker_trade_count_sum_{window}s"] = float(trade_count)
            out[f"taker_max_same_side_run_{window}s"] = float(max_run)
            out[f"taker_buy_sweep_score_{window}s"] = buy_sweep * math.log1p(max(buy_quote, 0.0))
            out[f"taker_sell_sweep_score_{window}s"] = sell_sweep * math.log1p(max(sell_quote, 0.0))
            out[f"taker_buy_iceberg_pressure_sum_{window}s"] = float(buy_iceberg)
            out[f"taker_sell_iceberg_pressure_sum_{window}s"] = float(sell_iceberg)

        return out

    def _compute_time_features(self, f: dict, bar_ts_ms: float = None):
        """Compute time features from bar timestamp (preferred) or current UTC time."""
        import datetime as dt
        if bar_ts_ms is not None:
            now = dt.datetime.fromtimestamp(bar_ts_ms / 1000.0, tz=dt.timezone.utc)
        else:
            now = dt.datetime.now(dt.timezone.utc)

        h = now.hour
        # Funding time (00:00, 08:00, 16:00 UTC)
        minutes_in_day = h * 60 + now.minute
        funding_times = [0, 480, 960, 1440]
        diffs = [ft - minutes_in_day for ft in funding_times if ft > minutes_in_day]
        time_to_funding = diffs[0] if diffs else funding_times[0] + 1440 - minutes_in_day

        f["minutes_to_funding"] = time_to_funding
        f["funding_phase"] = time_to_funding / 480.0
        f["funding_sin"] = math.sin(2 * math.pi * (1 - time_to_funding / 480.0))
        f["funding_cos"] = math.cos(2 * math.pi * (1 - time_to_funding / 480.0))

        minute_of_hour = now.minute + now.second / 60.0
        dist_to_hour = min(minute_of_hour, 60 - minute_of_hour)
        dist_to_half = abs(minute_of_hour - 30)
        f["dist_to_hour"] = min(dist_to_hour, dist_to_half)
        f["near_candle_close"] = 1 if f["dist_to_hour"] < 2 else 0

        # Calendar/session flags, including legacy names, come from the same
        # helper used by offline features and quote-level audits.
        calendar_now = (
            None
            if bar_ts_ms is not None and is_relative_millisecond_clock(bar_ts_ms)
            else now
        )
        f.update(
            calendar_scalar_features(
                calendar_now,
                prefix="cal_",
                include_legacy=True,
            )
        )

    # ── metrics polling & features ──

    def _poll_metrics(self):
        """Poll OI and long/short ratio data via REST API every 5 min."""
        try:
            if self._rest is None:
                return
            # Binance Futures REST endpoints
            oi_data = self._rest.open_interest(symbol=self._symbol)
            top_ls = self._rest.top_long_short_position_ratio(
                symbol=self._symbol, period="5m", limit=1)
            global_ls = self._rest.long_short_account_ratio(
                symbol=self._symbol, period="5m", limit=1)
            taker_ls = self._rest.taker_long_short_ratio(
                symbol=self._symbol, period="5m", limit=1)

            metrics = {
                "ts_ms": self._extract_metric_ts_ms(oi_data, top_ls, global_ls, taker_ls),
                "oi": float(oi_data.get("openInterest", 0)),
                "top_ls": float(top_ls[0]["longShortRatio"]) if top_ls else 1.0,
                "crowd_ls": float(global_ls[0]["longShortRatio"]) if global_ls else 1.0,
                "taker_ls": float(taker_ls[0]["buySellRatio"]) if taker_ls else 1.0,
            }

            with self._lock:
                self._metrics_history.append(metrics)
                self._last_metrics = metrics
            logger.debug(f"Metrics poll: OI={metrics['oi']:.0f} "
                         f"top_ls={metrics['top_ls']:.3f} "
                         f"taker_ls={metrics['taker_ls']:.3f}")
        except Exception as e:
            logger.warning(f"Metrics poll failed: {e}")
        finally:
            # Schedule next poll (unless stopped)
            if not self._metrics_stop.is_set():
                self._metrics_timer = threading.Timer(
                    self._metrics_poll_interval, self._poll_metrics)
                self._metrics_timer.daemon = True
                self._metrics_timer.start()

    def _compute_metrics_features(self, f: dict, target_ts_ms: float):
        """Compute metrics-derived features from polling history."""
        m, mh = self._metrics_state_at(target_ts_ms)
        if not m:
            self._zero_metrics_features(f)
            return

        oi = m["oi"]
        top_ls = m["top_ls"]
        crowd_ls = m["crowd_ls"]
        taker_ls = m["taker_ls"]

        # 1. OI log
        f["oi_log"] = math.log(oi) if oi > 0 else 0.0

        # 2. OI pct change (vs 5min ago)
        if len(mh) >= 2:
            prev_oi = mh[-2]["oi"]
            f["oi_pct_change"] = (oi - prev_oi) / prev_oi if prev_oi > 0 else 0.0
        else:
            f["oi_pct_change"] = 0.0

        # 3. OI z-score (1h = 12 x 5min, 6h = 72 x 5min)
        # Require min samples matching offline min_periods behavior
        oi_vals = [h["oi"] for h in mh]
        for n, label, min_req in [(12, "1h", 6), (72, "6h", 12)]:
            window = oi_vals[-n:] if len(oi_vals) >= n else oi_vals
            if len(window) >= min_req:
                arr = np.array(window)
                m_val, s_val = arr.mean(), arr.std()
                f[f"oi_zscore_{label}"] = (oi - m_val) / s_val if s_val > 0 else 0.0
            else:
                f[f"oi_zscore_{label}"] = 0.0

        # 4. OI momentum (short rolling vs long rolling)
        if len(oi_vals) >= 12:
            oi_ma_short = np.mean(oi_vals[-12:])
            oi_ma_long = np.mean(oi_vals[-min(72, len(oi_vals)):])
            f["oi_momentum"] = (oi_ma_short - oi_ma_long) / oi_ma_long if oi_ma_long > 0 else 0.0
        else:
            f["oi_momentum"] = 0.0

        # 5. Raw ratios
        f["toptrader_ls_ratio"] = top_ls
        f["crowd_ls_ratio"] = crowd_ls
        f["taker_ls_ratio"] = taker_ls

        # 6. LS ratio z-scores (vs 6h history)
        for key, name in [("top_ls", "toptrader_ls"), ("crowd_ls", "crowd_ls"),
                          ("taker_ls", "taker_ls")]:
            vals = [h[key] for h in mh]
            if len(vals) >= 12:
                arr = np.array(vals)
                m_val, s_val = arr.mean(), arr.std()
                f[f"{name}_zscore"] = (m[key] - m_val) / s_val if s_val > 0 else 0.0
            else:
                f[f"{name}_zscore"] = 0.0

        # 7. Taker LS momentum
        taker_vals = [h["taker_ls"] for h in mh]
        if len(taker_vals) >= 12:
            taker_ma_s = np.mean(taker_vals[-12:])
            taker_ma_l = np.mean(taker_vals[-min(72, len(taker_vals)):])
            f["taker_ls_momentum"] = taker_ma_s - taker_ma_l
        else:
            f["taker_ls_momentum"] = 0.0

        # 8. OI-price divergence
        close = f.get("close", 0)
        fh = list(self._feat_history)
        if len(mh) >= 2 and len(fh) >= 30 and close > 0:
            prev_oi = mh[-2]["oi"]
            oi_ret = (oi - prev_oi) / prev_oi if prev_oi > 0 else 0.0
            old_close = fh[-30].get("close", close)
            price_ret = (close - old_close) / old_close if old_close > 0 else 0.0
            f["oi_price_divergence"] = oi_ret - price_ret
        else:
            f["oi_price_divergence"] = 0.0

    # ── ML inference ──

    def _append_live_feature_dump(self, features: dict, pred: Prediction) -> None:
        """Write an optional JSONL row for live/offline feature parity audits.

        This is intentionally env-gated so normal live execution does no feature
        materialization beyond the existing model input path.
        """
        if not self._live_feature_dump_path:
            return
        self._live_feature_dump_count += 1
        if self._live_feature_dump_count % self._live_feature_dump_every_n != 0:
            return
        try:
            feature_ts_ms = int(float(features.get("_feature_ts_ms", float(pred.ts) * 1000.0)))
            wall_ts_ms = int(float(pred.ts) * 1000.0)
            row = {
                "timestamp": feature_ts_ms / 1000.0,
                "ts_ms": feature_ts_ms,
                "feature_ts_ms": feature_ts_ms,
                "dump_wall_ts_ms": wall_ts_ms,
                "symbol": self._symbol,
                "reference_symbol": self._reference_symbol,
                "model_dir": str(self._model_dir),
                "pred_dir_10s": float(pred.dir_10s),
                "pred_dir_30s": float(pred.dir_30s),
                "pred_dir_60s": float(pred.dir_60s),
                "pred_vol_10s": float(pred.vol_10s),
                "pred_vol_30s": float(pred.vol_30s),
                "pred_vol_60s": float(pred.vol_60s),
                "pred_ret_10s": float(pred.ret_10s),
                "pred_ret_30s": float(pred.ret_30s),
                "pred_ret_60s": float(pred.ret_60s),
                "pred_tox_bid_5s": float(pred.tox_bid_5s),
                "pred_tox_ask_5s": float(pred.tox_ask_5s),
                "pred_tox_bid_10s": float(pred.tox_bid_10s),
                "pred_tox_ask_10s": float(pred.tox_ask_10s),
            }
            for key, value in features.items():
                if isinstance(value, (int, float, np.integer, np.floating)):
                    val = float(value)
                    if math.isfinite(val):
                        row[str(key)] = val
            path = Path(self._live_feature_dump_path).expanduser()
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        except Exception as exc:
            logger.warning("Live feature dump write failed: %s", exc)

    def _predict(self, features: dict) -> Prediction:
        """Run each model head against its saved causal feature schema."""
        pred = Prediction(ts=time.time())

        if not self._enable_ml:
            # ML-OFF is explicitly neutral.  Never feed dimensionless realized
            # log-return volatility into the absolute-price-variance field.
            pred.features = self._features_to_array(features)
            pred.feature_dict = dict(features)
            return pred
        if set(self._models) != set(REQUIRED_MODEL_HEADS):
            raise RuntimeError("ML is enabled without a complete 13-head bundle")

        # FEATURE_NAMES_BASE is a legacy diagnostic vector.  The authoritative
        # runtime schema is the per-head metadata list validated below.
        X_base = self._feature_array(features, FEATURE_NAMES_BASE)

        for h in [10, 30, 60]:
            name = f"ret_{h}s"
            if name in self._models:
                try:
                    cols = self._model_feature_cols.get(name, FEATURE_NAMES_BASE)
                    X_ret = self._feature_array(features, cols, strict=True)
                    val = float(self._models[name].predict(X_ret)[0])
                    if h == 10:
                        pred.ret_10s = val
                    elif h == 30:
                        pred.ret_30s = val
                    elif h == 60:
                        pred.ret_60s = val
                except Exception as exc:
                    raise RuntimeError(f"prediction failed for {name}: {exc}") from exc

        def _model_input(name, model):
            cols = self._model_feature_cols.get(name)
            if cols:
                return self._feature_array(features, cols, strict=True)
            return X_base

        # Run dir/vol models
        for name, model in self._models.items():
            if name.startswith("ret_"):
                continue  # already processed in stage 1
            try:
                val = model.predict(_model_input(name, model))[0]
                if name == "dir_10s":
                    pred.dir_10s = float(val)
                elif name == "dir_30s":
                    pred.dir_30s = float(val)
                elif name == "dir_60s":
                    pred.dir_60s = float(val)
                elif name == "vol_10s":
                    pred.vol_10s = max(float(val), 0.0)
                elif name == "vol_30s":
                    pred.vol_30s = max(float(val), 0.0)
                elif name == "vol_60s":
                    pred.vol_60s = max(float(val), 0.0)
                elif name == "tox_bid_5s":
                    pred.tox_bid_5s = float(val)
                elif name == "tox_ask_5s":
                    pred.tox_ask_5s = float(val)
                elif name == "tox_bid_10s":
                    pred.tox_bid_10s = float(val)
                elif name == "tox_ask_10s":
                    pred.tox_ask_10s = float(val)
            except Exception as exc:
                raise RuntimeError(f"prediction failed for {name}: {exc}") from exc

        pred.tox_bid_5s = float(np.clip(pred.tox_bid_5s, 0.0, 1.0))
        pred.tox_ask_5s = float(np.clip(pred.tox_ask_5s, 0.0, 1.0))
        pred.tox_bid_10s = float(np.clip(pred.tox_bid_10s, 0.0, 1.0))
        pred.tox_ask_10s = float(np.clip(pred.tox_ask_10s, 0.0, 1.0))

        pred.features = X_base[0] if X_base.ndim == 2 else X_base
        pred.feature_dict = dict(features)

        # ── pred_ret demeaning: subtract running EMA to remove momentum bias ──
        if self._ret_demean_halflife > 0:
            alpha = 2.0 / (self._ret_demean_halflife + 1.0)
            raw_rets = [0.0, 0.0, 0.0]
            for idx, attr in enumerate(['ret_10s', 'ret_30s', 'ret_60s']):
                raw = getattr(pred, attr)
                raw_rets[idx] = raw
                self._pred_ret_ema[idx] = alpha * raw + (1.0 - alpha) * self._pred_ret_ema[idx]
                setattr(pred, attr, raw - self._pred_ret_ema[idx])
            # Diagnostic: log every 30 predictions (~5 min)
            self._demean_log_cnt = getattr(self, '_demean_log_cnt', 0) + 1
            if self._demean_log_cnt % 30 == 1:
                logger.info(
                    f"DEMEAN raw=[{raw_rets[0]:+.7f},{raw_rets[1]:+.7f},{raw_rets[2]:+.7f}] "
                    f"ema=[{self._pred_ret_ema[0]:+.7f},{self._pred_ret_ema[1]:+.7f},{self._pred_ret_ema[2]:+.7f}] "
                    f"out=[{pred.ret_10s:+.7f},{pred.ret_30s:+.7f},{pred.ret_60s:+.7f}] "
                    f"hl={self._ret_demean_halflife}"
                )

        self._append_live_feature_dump(pred.feature_dict or features, pred)
        return pred

    def _features_to_array(self, features: dict) -> np.ndarray:
        """Convert the canonical 88-feature base dictionary to model order."""
        return self._feature_array(features, FEATURE_NAMES)[0]

    @staticmethod
    def _feature_array(
        features: dict,
        feature_names: List[str],
        *,
        strict: bool = False,
    ) -> np.ndarray:
        if strict:
            missing = [name for name in feature_names if name not in features]
            if missing:
                preview = ", ".join(missing[:8])
                raise RuntimeError(
                    f"runtime model feature contract missing {len(missing)} columns: {preview}"
                )
        return np.array([features.get(name, 0.0) for name in feature_names],
                        dtype=np.float64).reshape(1, -1)

    # ── utility ──

    @staticmethod
    def _ewm(closes: list, span: int) -> float:
        """Exponentially weighted mean of 1s returns.

        Matches pandas: Series.diff().ewm(span=span, min_periods=1).mean()
        with adjust=True (default). Uses all available data, no truncation.
        Newest return gets weight 1, each older return gets ×(1-alpha).
        """
        if len(closes) < 2:
            return 0.0
        alpha = 2.0 / (span + 1)
        ret = 0.0
        w_sum = 0.0
        w = 1.0
        for i in range(len(closes) - 1, 0, -1):
            diff = closes[i] - closes[i - 1]
            ret += w * diff
            w_sum += w
            w *= (1 - alpha)
        return ret / w_sum if w_sum > 0 else 0.0

    @staticmethod
    def _diffs(values: list, n: int) -> list:
        """Last n diffs of a list."""
        if len(values) < 2:
            return []
        start = max(1, len(values) - n)
        return [values[i] - values[i - 1] for i in range(start, len(values))]

    @staticmethod
    def _safe_log_return(new_price: float, old_price: float) -> float:
        """Return log(new/old) only when both prices are strictly positive."""
        if new_price > 0.0 and old_price > 0.0:
            return math.log(new_price / old_price)
        return 0.0

    @staticmethod
    def _compute_streaks(signs: list) -> list:
        """Compute streak values from sign list."""
        if not signs:
            return []
        streaks = [signs[0]]
        for i in range(1, len(signs)):
            if signs[i] == signs[i - 1] and signs[i] != 0:
                streaks.append(streaks[-1] + signs[i])
            else:
                streaks.append(signs[i])
        return streaks

    # ── properties ──

    @property
    def is_warmed_up(self) -> bool:
        return self._warmup_count >= 300

    @property
    def mid_price(self) -> float:
        """Get current mid price from latest depth or last close."""
        with self._lock:
            if self._last_depth and self._last_depth.bids and self._last_depth.asks:
                return (self._last_depth.bids[0][0] + self._last_depth.asks[0][0]) / 2.0
            if self._close_history:
                return self._close_history[-1]
            return 0.0

    @property
    def rolling_variance(self) -> float:
        """Rolling 60s variance from 1s close diffs (for AS formula)."""
        closes = list(self._close_history)
        if len(closes) < 10:
            return 1.0
        diffs = [closes[i] - closes[i - 1] for i in range(max(1, len(closes) - 60), len(closes))]
        arr = np.array(diffs)
        return max(float(np.var(arr, ddof=1)), 1e-6)

    def causal_rolling_variance_snapshot(
        self,
        *,
        window_s: int = 60,
        minimum_diffs: int = 10,
        bad_tick_return_bps: float = 500.0,
    ) -> RollingVarianceSnapshot:
        """Return raw causal variance without the quote-core fallback floor.

        The newest completed bar becomes visible at ``bucket_start + 1s``.
        A clearly corrupt one-second move invalidates the sample rather than
        spending variance-time budget with an imputed value.
        """

        with self._lock:
            bars = list(self._bar_buffer)[-(max(2, int(window_s)) + 1) :]
        if not bars:
            return RollingVarianceSnapshot(0.0, 0.0, 0, 0, False, "no_completed_bar")
        latest = bars[-1]
        ready_ts_ms = int(latest.ts) + 1_000
        closes = np.asarray([float(bar.close) for bar in bars], dtype=np.float64)
        timestamps = np.asarray([int(bar.ts) for bar in bars], dtype=np.int64)
        if closes.size < int(minimum_diffs) + 1:
            return RollingVarianceSnapshot(
                0.0,
                float(latest.close),
                ready_ts_ms,
                max(0, int(closes.size - 1)),
                False,
                "insufficient_history",
            )
        if not np.all(np.isfinite(closes)) or np.any(closes <= 0.0):
            return RollingVarianceSnapshot(
                0.0, float(latest.close), ready_ts_ms, 0, False, "invalid_close"
            )
        if timestamps.size >= 2 and np.any(np.diff(timestamps) != 1_000):
            return RollingVarianceSnapshot(
                0.0, float(latest.close), ready_ts_ms, 0, False, "bar_gap"
            )
        diffs = np.diff(closes)
        prior = closes[:-1]
        return_bps = np.abs(diffs / prior) * 10_000.0
        if np.any(return_bps > float(bad_tick_return_bps)):
            return RollingVarianceSnapshot(
                0.0,
                float(latest.close),
                ready_ts_ms,
                int(diffs.size),
                False,
                "bad_tick_return",
            )
        sigma_sq = float(np.var(diffs, ddof=1))
        if not math.isfinite(sigma_sq) or sigma_sq < 0.0:
            return RollingVarianceSnapshot(
                0.0,
                float(latest.close),
                ready_ts_ms,
                int(diffs.size),
                False,
                "invalid_variance",
            )
        return RollingVarianceSnapshot(
            sigma_sq,
            float(latest.close),
            ready_ts_ms,
            int(diffs.size),
            True,
            "",
        )
