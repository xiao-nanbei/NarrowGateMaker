"""
Configuration loader — 读取 config.yaml 并暴露为 Python 对象。
支持 SIGHUP 热重载。
"""

import hashlib
import logging
import math
import os
import signal
import stat
import threading
from dataclasses import dataclass, field, is_dataclass
from pathlib import Path
from typing import Optional, get_args, get_origin

import yaml

from data_paths import data_root, storage_root
from execution.order_lifecycle_journal_storage_v2 import (
    LOCAL_ORICO_REPLAY_ADMISSION,
    validate_lifecycle_journal_storage,
)

logger = logging.getLogger("config")

CONFIG_PATH = Path(__file__).parent / "config.yaml"

_lock = threading.Lock()
_cfg: Optional["Config"] = None
_cfg_path: Path = CONFIG_PATH


@dataclass
class ApiConfig:
    key: str = ""
    secret: str = ""
    testnet: bool = True


@dataclass
class StrategyConfig:
    gamma: float = 0.010                  # inventory penalty coefficient
    kappa: float = 0.05                   # internal fallback only; live/tick path normally uses p3_kappa_eff
    p3_kappa_eff_override: float = 0.0    # 0=use fill-prob model; >0 explicit research/live-trial override
    order_size: float = 0.0026
    max_inventory: float = 0.026
    requote_interval: float = 10.0
    position_timeout: float = 0.0
    quote_horizon_s: float = 1.0           # AS variance integration horizon; sigma_sq input is per second
    max_spread_bps: float = 8.0           # v2.0: 8 bps safety cap
    replace_min_price_change_ticks: float = 0.0      # live order lifecycle: min price delta before cancel/new; 0=disabled
    replace_min_price_change_ticks_reducing: float = 0.0
    replace_min_interval_ms: float = 0.0             # min active order age before replace; 0=disabled
    replace_min_interval_ms_reducing: float = 0.0
    replace_pending_coalesce: bool = True             # do not stack replaces while local order state is pending
    replace_cancel_first_exposure_increasing: bool = False  # optional soak arm: cancel add-side quote before sending replacement
    dynamic_cap_enabled: bool = False     # volatility-scaled spread cap
    dynamic_cap_base_bps: float = 0.0     # base cap in bps when dynamic cap is enabled
    dynamic_cap_alpha: float = 0.5        # cap multiplier exponent on sigma^2 / sigma0^2
    dynamic_cap_max_mult: float = 2.0     # upper bound on dynamic cap multiplier
    dynamic_cap_var_baseline: float = 0.0 # sigma0^2; 0 uses regime.vol_baseline^2
    leverage: int = 10
    requote_threshold_bps: float = 3.0
    eta: float = 0.5
    book_imb_strength: float = 0.0
    rq_min: float = 5.0
    rq_max: float = 10.0
    kappa_ratio: float = 1.0              # v2.0: use calibrated κ directly
    inventory_skew_strength: float = 0.1  # Step 25C: was 0.3
    inventory_asym_strength: float = 0.0  # direct inventory half-spread asymmetry
    inventory_signal_fade_strength: float = 0.0  # fade exposure-increasing ML/markout asymmetry

    kappa_depth_baseline: float = 50.0
    depth_kappa_ratio: float = 0.3       # minimum depth κ multiplier floor for live/backtest parity
    thin_depth_threshold: float = 0.0     # side policy thin-depth threshold; 0 falls back to legacy kappa_depth_baseline * 0.5

    # ── v1.1: P1 BER Guard (Zhao & Linetsky 2021) ──
    ber_guard_thresh: float = 1.2         # ema_fast/ema_slow ratio threshold (0=disabled)
    ber_spread_mult: float = 2.0          # spread multiplier when BER active
    ber_exposure_add_only: bool = False   # owner successor: preserve BER only on pure add quotes

    # ── v1.2: volatility scaling; AMM LVR is analogy, not a CLOB optimum ──
    vol_power: float = 1.5                # empirical exponent; no paper proves 1.5 optimal for this LOB
    markout_horizon_s: float = 10.0       # exact fill-to-observation wall-clock horizon
    markout_ema_span_fills: int = 50       # EMA span N; alpha=2/(N+1)
    markout_spread_scale: float = 0.2     # markout → spread/asymmetry scale (0=disabled)
    markout_side_asymmetry_sign: float = 1.0  # maker-signed BUY/SELL EMA semantics require +1
    spread_cap_mode: str = "compress"     # compress | pause_exposure | observe
    adverse_guard_enabled: bool = False   # side-aware adverse-selection guard
    adverse_toxicity_threshold: float = 0.70
    adverse_markout_threshold: float = 5.0
    adverse_markout_pause_threshold: float = 0.0  # 0 = pause at adverse_markout_threshold
    adverse_markout_pause_hybrid: bool = False
    adverse_markout_pause_base_s: float = 120.0
    adverse_markout_pause_min_s: float = 120.0
    adverse_markout_pause_max_s: float = 900.0
    adverse_markout_decay_tau_s: float = 900.0
    adverse_dir_threshold: float = 0.0
    adverse_ret_bps_threshold: float = 0.0
    adverse_microprice_shift_bps: float = 0.0
    adverse_spread_mult: float = 1.10
    adverse_thin_depth_threshold: float = 0.0
    adverse_thin_depth_mult: float = 1.0
    adverse_pause: bool = True
    defense_guard_enabled: bool = False
    defense_markout_threshold: float = 2.0
    defense_dir_threshold: float = 0.05
    defense_ret_bps_threshold: float = 0.0
    defense_microprice_shift_bps: float = 0.0
    defense_spread_mult: float = 1.35
    defense_pause: bool = True
    defense_emergency_inventory_ratio: float = 0.50
    defense_emergency_loss: float = 5.0
    flat_unilateral_max_s: float = 120.0    # max time to remain one-sided while flat before restoring both sides
    local_extreme_guard_enabled: bool = False
    local_extreme_window_s: float = 120.0
    local_extreme_rank_threshold: float = 0.80
    local_extreme_require_thin_depth: bool = True
    local_extreme_thin_depth_threshold: float = 0.0
    local_extreme_spread_mult: float = 1.0
    local_extreme_pause: bool = False
    fragile_order_ttl_s: float = 0.0
    # Scoring/logging and quote action are independent permissions. The legacy
    # live flag remains the action switch for config compatibility.
    buy_fill_selection_shadow_enabled: bool = False
    buy_fill_selection_live_enabled: bool = False
    buy_fill_selection_live_model_path: str = ""
    buy_fill_selection_live_score_threshold: float = 0.50
    buy_fill_selection_live_spread_mult_cap: float = 1.00
    buy_fill_selection_live_apply_reducing: bool = False
    buy_fill_selection_live_max_missing_features: int = 99
    # Prediction-only active-order hazard shadow. This never authorizes
    # cancel/replace or changes quote geometry.
    dynamic_fill_hazard_shadow_enabled: bool = False
    dynamic_fill_hazard_shadow_model_path: str = ""
    dynamic_fill_hazard_shadow_model_sha256: str = ""
    dynamic_fill_hazard_shadow_sides: str = "BUY"
    dynamic_fill_hazard_shadow_exposure_ms: float = 100.0
    dynamic_fill_hazard_shadow_price_jump_ticks: float = 1.0
    # Optional action mapping is a separately hashed artifact. It may only
    # cancel BUY exposure-increasing orders and must preserve reducing quotes.
    dynamic_fill_hazard_action_enabled: bool = False
    dynamic_fill_hazard_action_policy_path: str = ""
    dynamic_fill_hazard_action_policy_sha256: str = ""
    # Next-generation bounded action policy. Live defaults to disabled; shadow
    # observes one add-side decision per campaign without changing orders.
    state_conditioned_policy_mode: str = "disabled"  # disabled | shadow | active
    state_conditioned_policy_model_path: str = ""
    # Evidence-only external fair-price projection. It writes candidate quote
    # coordinates but has no action mode and cannot mutate live orders.
    cross_venue_fair_price_shadow_enabled: bool = False
    fill_cooldown: float = 0.0               # Step 27: exposure-increasing same-side fill cooldown base (seconds, 0=disabled)
    fill_cooldown_consecutive_reset_policy: str = "opposite_fill_only"
    fill_cooldown_reducing: float = 0.0      # shorter same-side cooldown for inventory-reducing fills; 0=disabled
    fill_cooldown_reducing_campaign_only: bool = False  # if true, apply reducing cooldown only when campaign risk gates below are hit
    fill_cooldown_reducing_inv_threshold: float = 0.0   # abs inventory BTC threshold for campaign-only reducing cooldown
    fill_cooldown_reducing_inv_ratio: float = 0.0       # abs inventory / order_size threshold for campaign-only reducing cooldown
    fill_cooldown_reducing_age_s: float = 0.0           # campaign age threshold for campaign-only reducing cooldown
    fill_cooldown_reducing_vol_ref: float = 0.0       # if >0, scale reducing cooldown by vol_10s / ref
    fill_cooldown_reducing_vol_min_mult: float = 0.5
    fill_cooldown_reducing_vol_max_mult: float = 2.0
    # Owner-risk-accepted F05 policy. It replaces only the total SELL
    # exposure-increasing cooldown duration and is restart-only.
    boolean_cooldown_policy_enabled: bool = False
    boolean_cooldown_policy_path: str = ""
    boolean_cooldown_policy_sha256: str = ""
    boolean_cooldown_predicate_bundle_path: str = ""
    boolean_cooldown_predicate_bundle_sha256: str = ""
    boolean_cooldown_ema_warmup_s: float = 2048.0
    boolean_cooldown_evidence_route: str = "owner_risk_accepted_promotion"
    # Independent owner-risk-accepted BUY E3 artifact. It may replace only
    # the total exposure-increasing BUY cooldown selected on an executed fill.
    buy_e3_cooldown_policy_enabled: bool = False
    buy_e3_cooldown_artifact_manifest_path: str = ""
    buy_e3_cooldown_artifact_manifest_sha256: str = ""
    buy_e3_cooldown_artifact_sha256: str = ""
    buy_e3_cooldown_policy_path: str = ""
    buy_e3_cooldown_policy_sha256: str = ""
    buy_e3_cooldown_predicate_bundle_path: str = ""
    buy_e3_cooldown_predicate_bundle_sha256: str = ""
    buy_e3_cooldown_ema_warmup_s: float = 2048.0
    buy_e3_cooldown_evidence_route: str = "owner_risk_accepted_buy_e3_v1"
    post_fill_quote_response_enabled: bool = False
    post_fill_quote_response_mode: str = "noop"
    post_fill_inventory_ticks_per_order_unit: float = 0.25
    post_fill_inventory_max_ticks: float = 4.0
    post_fill_flow_ticks_per_excitation: float = 2.0
    post_fill_flow_max_ticks: float = 8.0
    post_fill_flow_excitation_per_order_unit: float = 1.0
    post_fill_flow_max_excitation: float = 4.0
    post_fill_flow_amplitude_mode: str = "excitation_ticks"
    post_fill_flow_expected_adverse_buy_ticks: float = 0.0
    post_fill_flow_expected_adverse_sell_ticks: float = 0.0
    post_fill_flow_add_distance_fraction_buy: float = 1.0
    post_fill_flow_add_distance_fraction_sell: float = 1.0
    post_fill_response_half_life_s: float = 20.0
    post_fill_response_half_life_min_s: float = 4.0
    post_fill_response_half_life_max_s: float = 120.0
    post_fill_response_volatility_ref_bps: float = 3.0
    post_fill_response_volatility_weight: float = 0.35
    post_fill_response_refill_edge_ref: float = 0.10
    post_fill_response_refill_weight: float = 0.75
    post_fill_response_repair_probability_anchor: float = 0.60
    post_fill_response_repair_probability_weight: float = 1.0
    adaptive_add_cooldown_enabled: bool = False       # exposure-increasing fill_cd multiplier; research/default off
    adaptive_add_cooldown_min_mult: float = 0.5
    adaptive_add_cooldown_max_mult: float = 2.5
    adaptive_add_cooldown_w_markout: float = 0.0
    adaptive_add_cooldown_w_flow: float = 0.0
    adaptive_add_cooldown_w_campaign: float = 0.0
    adaptive_add_cooldown_w_trend: float = 0.0
    adaptive_add_cooldown_w_refill_weak: float = 0.0
    adaptive_add_cooldown_w_refill_good: float = 0.0
    adaptive_add_cooldown_w_reversion: float = 0.0
    adaptive_add_cooldown_mo_ref: float = 50.0
    adaptive_add_cooldown_flow_ref: float = 2.0
    adaptive_add_cooldown_campaign_inv_ref: float = 0.006
    adaptive_add_cooldown_campaign_age_ref_s: float = 3600.0
    adaptive_add_cooldown_trend_ret_ref: float = 2e-5
    adaptive_add_cooldown_refill_ref: float = 0.10
    adaptive_add_cooldown_reversion_ref: float = 1.0
    adaptive_add_cooldown_gate_enabled: bool = False
    adaptive_add_cooldown_gate_mult: float = 1.75
    adaptive_add_cooldown_gate_campaign_score: float = 1.0
    adaptive_add_cooldown_gate_trend_score: float = 1.0
    adaptive_add_cooldown_gate_refill_edge_max: float = 0.0
    adaptive_add_cooldown_gate_reversion_max: float = 0.5
    adaptive_add_cooldown_gate_side: str = "BOTH"       # BOTH/BUY/SELL; research gate side filter
    symmetric_size: bool = False             # Step 28: mirror η-decayed bid/ask size to smaller side (keeps buy/sell qty balanced)
    use_bar_pricing: bool = True             # v2.0: use 1s bar close pricing (match backtest)


@dataclass
class MLConfig:
    enabled: bool = True
    vol_blend: float = 0.5            # Step 25C: was 0.7
    skew_strength: float = 0.0
    asym_strength: float = 0.1
    gamma_dir_bonus: float = 0.0      # Step 25A: disabled (was 0.3)
    dir_threshold: float = 0.05
    ret_skew: float = 200.0           # Step 25A: was 5000
    ret_shift_max_pct: float = 0.3    # Step 25C: was 0.5
    ret_demean_halflife: int = 0      # disabled — demeaning slightly hurts at RS=200
    toxicity_horizon_s: int = 10
    model_dir: str = "models/saved_btcusdc"  # relative to project root unless absolute


@dataclass
class RegimeConfig:
    enabled: bool = True
    vol_baseline: float = 3.0
    gamma_scale_min: float = 0.5
    gamma_scale_max: float = 2.0
    liq_baseline: float = 200.0
    gamma_liq_scale_min: float = 0.5
    gamma_liq_scale_max: float = 3.0


@dataclass
class FeeConfig:
    maker: float = 0.0                # BTCUSDC maker promotion
    taker: float = 0.00036            # BTCUSDC taker fee 0.036%


@dataclass
class WebSocketConfig:
    depth_levels: int = 20
    depth_speed: int = 100
    deep_book_enabled: bool = False
    deep_book_snapshot_levels: int = 1000
    deep_book_speed: int = 100
    deep_book_max_buffer_events: int = 20000
    deep_book_resync_backoff_s: float = 1.0
    deep_book_max_age_s: float = 2.0
    exec_stream_silence_timeout_s: float = 45.0
    anchor_stream_silence_timeout_s: float = 45.0


@dataclass
class RiskConfig:
    max_daily_loss: float = 50.0
    max_position_value: float = 3000.0
    emergency_close_dd: float = 150.0
    cooldown_after_loss: float = 30.0
    max_consecutive_losses: int = 3
    # Historical replay ABI: end-to-end capture minus exchange age.
    max_exec_book_age_s: float = 5.0
    # Live stale gating separates local visibility age from source transport.
    max_exec_book_visible_age_s: float = 5.0
    max_exec_book_source_lag_s: float = 5.0
    sync_adjust_degrade_enabled: bool = True
    sync_adjust_degrade_count: int = 1       # fallback default; BTCUSDC YAML currently raises this to reduce reconnect false positives
    sync_adjust_abs_qty_threshold: float = 0.0
    sync_adjust_degrade_window_s: float = 300.0
    sync_adjust_pause_s: float = 120.0
    sync_adjust_reconnect_user_stream: bool = True
    sync_adjust_cancel_orders: bool = True
    circuit_breaker_sigma: float = 8.0
    pnl_volatility_horizon_s: float = 300.0  # explicit lifecycle risk horizon; recalibrate per deployment
    exit_urgency_strength: float = 0.5
    urgency_time_weight: float = 0.3
    urgency_pnl_weight: float = 0.3
    urgency_signal_weight: float = 0.4


@dataclass
class LogConfig:
    level: str = "INFO"
    file: str = "logs/maker.log"          # relative to project root, resolved by main.py
    console: bool = True
    fill_cooldown_checkpoint: str = "logs/fill_cooldown_state.json"
    trade_log: str = "logs/trades.csv"    # relative to project root, resolved by main.py
    quote_log: str = "logs/quote_decisions.csv"
    order_outcome_log: str = "logs/order_outcomes.csv"
    buy_fill_selection_shadow_log: str = "logs/buy_fill_selection_shadow.csv"
    dynamic_fill_hazard_shadow_log: str = "logs/dynamic_fill_hazard_shadow.csv"
    dynamic_fill_hazard_action_log: str = "logs/dynamic_fill_hazard_action.csv"
    state_conditioned_policy_shadow_log: str = "logs/state_conditioned_policy_shadow.csv"
    cross_venue_fair_price_shadow_log: str = "logs/cross_venue_fair_price_shadow.csv"
    # Retired quote-time inventory threshold what-if stream. Campaign starts,
    # fills, and terminal outcomes remain in maker/trade/order event logs.
    inventory_campaign_shadow_enabled: bool = False
    inventory_campaign_shadow_log: str = "logs/inventory_campaign_shadow.csv"
    exact_opportunity_tape_enabled: bool = False
    exact_opportunity_tape: str = "logs/exact_opportunity_tape.csv"
    exact_opportunity_tape_staging_dir: str = "logs/exact_opportunity_tape_staging"
    exact_opportunity_tape_queue_size: int = 20_000
    exact_opportunity_tape_flush_rows: int = 1_000
    exact_opportunity_tape_flush_interval_s: float = 1.0
    exact_opportunity_tape_heartbeat_interval_s: float = 5.0
    order_lifecycle_journal: str = "logs/order_lifecycle_journal.csv"
    live_perf_telemetry_log: str = "logs/live_perf_telemetry.csv"
    quote_snapshot_integrity_log: str = "logs/quote_snapshot_integrity.csv"
    # Unified receive-time market tape.  This is an asynchronous shadow-only
    # recorder and is never read by quote policy.
    market_tape_enabled: bool = False
    market_tape_dir: str = "logs/market_tape"
    market_tape_record_books: bool = True
    market_tape_record_trades: bool = True
    market_tape_record_depth: bool = False
    market_tape_book_interval_ms: float = 0.0
    market_tape_queue_size: int = 20_000
    bad_trade_log_every: int = 100         # aggregate invalid trade warnings every N events


@dataclass
class LifecycleJournalV2Config:
    """Prospective mechanics-only collection; restart-only and disabled by default."""

    enabled: bool = False
    storage_profile: str = LOCAL_ORICO_REPLAY_ADMISSION
    required_mount: str = field(default_factory=lambda: str(storage_root()))
    root: str = field(
        default_factory=lambda: str(
            data_root() / "formal_collection/order_lifecycle_journal_v2"
        )
    )
    prospective_epoch_root: str = field(
        default_factory=lambda: str(
            data_root() / "formal_collection/prospective_baseline_epochs"
        )
    )
    remote_spool_allowlisted_roots: list[str] = field(
        default_factory=lambda: [
            str(Path.home() / "NarrowGate_BTCUSDC/formal_collection")
        ]
    )
    remote_session_max_duration_s: float = 3600.0
    remote_session_max_bytes: int = 4 * 1024 * 1024 * 1024
    baseline_identity_path: str = ""
    baseline_identity_sha256: str = ""
    storage_format: str = "parquet"
    queue_size: int = 8192
    heartbeat_interval_s: float = 5.0
    shutdown_drain_timeout_s: float = 5.0


@dataclass
class PerfConfig:
    listen_key_renew: int = 1800


@dataclass
class MultiMarketConfig:
    enabled: bool = False
    market_stage: str = "minimal"
    reference_symbol: str = "BTCUSDT"
    # Binance lists the conversion market as USDCUSDT: USDT paid per USDC.
    # Therefore BTCUSDT / USDCUSDT converts the local bridge into BTCUSDC.
    stablecoin_anchor_symbol: str = "USDCUSDT"
    # Optional diagnostic evaluators are fail-closed in the live config.  They
    # do not provide active quote inputs and must be enabled explicitly.
    global_flow_shadow_enabled: bool = False
    global_reference_shadow_enabled: bool = False
    # multi_market.enabled only controls feature/source wiring for ML, quote EV
    # features, shadow labels, and risk buckets. It is not a direct expected-PnL
    # or widen/retreat/TTL/size switch; policy promotion still needs daily OOS
    # and live/shadow gates.


@dataclass
class ExternalVenueSourceConfig:
    """One read-only external market source.

    Public reference streams do not use credentials and expose no account or
    order methods. Private venue APIs belong in a separate adapter boundary.
    """

    venue: str = "bitget"
    enabled: bool = False
    transport: str = "websocket"
    role: str = "reference"
    symbol: str = "BTCUSDT"
    instrument_type: str = "perp"
    product_type: str = "USDT-FUTURES"
    quote_currency: str = "USDT"
    settlement_currency: str = "USDT"
    instrument_id: str = ""
    contract_multiplier: float = 1.0
    websocket_url: str = "wss://ws.bitget.com/v3/ws/public"
    rest_url: str = "https://api.bybit.com"
    book_channel: str = "books1"
    trade_channel: str = "trade"
    poll_interval_ms: float = 250.0
    trade_poll_interval_ms: float = 500.0
    request_timeout_s: float = 2.0
    max_source_age_s: float = 2.0
    record_enabled: bool = False
    record_interval_ms: float = 100.0
    record_trades: bool = True
    record_queue_size: int = 20_000
    record_dir: str = "logs/external_venues"


@dataclass
class ExternalVenuesConfig:
    enabled: bool = False
    shadow_only: bool = True
    sources: list[ExternalVenueSourceConfig] = field(default_factory=list)


@dataclass
class DepthMicropriceKappaConfig:
    enabled: bool = False
    microprice_levels: int = 3
    kappa_levels: int = 5
    kappa_depth_baseline: float = 50.0


@dataclass
class DepthImbalanceAsymConfig:
    enabled: bool = False
    levels: int = 20
    strength: float = 0.0


@dataclass
class DepthToxSpreadConfig:
    enabled: bool = False
    levels: int = 20
    imbalance_threshold: float = 0.65
    microprice_shift_bps: float = 1.0
    spread_mult: float = 1.25


@dataclass
class DepthExecutionConfig:
    shadow_enabled: bool = False
    log_interval_requotes: int = 6
    microprice_kappa: DepthMicropriceKappaConfig = field(default_factory=DepthMicropriceKappaConfig)
    imbalance_asym: DepthImbalanceAsymConfig = field(default_factory=DepthImbalanceAsymConfig)
    depth_tox_spread: DepthToxSpreadConfig = field(default_factory=DepthToxSpreadConfig)


@dataclass
class Config:
    project_name: str = "NarrowGate"
    symbol: str = "BTCUSDC"
    tick_size: float = 0.1
    lot_size: float = 0.001
    min_notional: float = 5.0

    api: ApiConfig = field(default_factory=ApiConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    ml: MLConfig = field(default_factory=MLConfig)
    regime: RegimeConfig = field(default_factory=RegimeConfig)
    fees: FeeConfig = field(default_factory=FeeConfig)
    websocket: WebSocketConfig = field(default_factory=WebSocketConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    logging: LogConfig = field(default_factory=LogConfig)
    lifecycle_journal_v2: LifecycleJournalV2Config = field(
        default_factory=LifecycleJournalV2Config
    )
    performance: PerfConfig = field(default_factory=PerfConfig)
    multi_market: MultiMarketConfig = field(default_factory=MultiMarketConfig)
    external_venues: ExternalVenuesConfig = field(default_factory=ExternalVenuesConfig)
    depth_execution: DepthExecutionConfig = field(default_factory=DepthExecutionConfig)


BACKTEST_PARAM_SOURCES = (
    ("symbol", ("symbol",)),
    ("gamma", ("strategy", "gamma")),
    ("kappa", ("strategy", "kappa")),
    ("p3_kappa_eff_override", ("strategy", "p3_kappa_eff_override")),
    ("order_size", ("strategy", "order_size")),
    ("max_inventory", ("strategy", "max_inventory")),
    ("requote_interval", ("strategy", "requote_interval")),
    ("requote_threshold_bps", ("strategy", "requote_threshold_bps")),
    ("replace_min_price_change_ticks", ("strategy", "replace_min_price_change_ticks")),
    ("replace_min_price_change_ticks_reducing", ("strategy", "replace_min_price_change_ticks_reducing")),
    ("replace_min_interval_ms", ("strategy", "replace_min_interval_ms")),
    ("replace_min_interval_ms_reducing", ("strategy", "replace_min_interval_ms_reducing")),
    ("replace_pending_coalesce", ("strategy", "replace_pending_coalesce")),
    ("replace_cancel_first_exposure_increasing", ("strategy", "replace_cancel_first_exposure_increasing")),
    ("rq_min", ("strategy", "rq_min")),
    ("rq_max", ("strategy", "rq_max")),
    ("eta", ("strategy", "eta")),
    ("kappa_ratio", ("strategy", "kappa_ratio")),
    ("lot_size", ("lot_size",)),
    ("book_imb_strength", ("strategy", "book_imb_strength")),
    ("inventory_skew_strength", ("strategy", "inventory_skew_strength")),
    ("inventory_asym_strength", ("strategy", "inventory_asym_strength")),
    ("inventory_signal_fade_strength", ("strategy", "inventory_signal_fade_strength")),
    ("max_spread_bps", ("strategy", "max_spread_bps")),
    ("dynamic_cap_enabled", ("strategy", "dynamic_cap_enabled")),
    ("dynamic_cap_base_bps", ("strategy", "dynamic_cap_base_bps")),
    ("dynamic_cap_alpha", ("strategy", "dynamic_cap_alpha")),
    ("dynamic_cap_max_mult", ("strategy", "dynamic_cap_max_mult")),
    ("dynamic_cap_var_baseline", ("strategy", "dynamic_cap_var_baseline")),
    ("position_timeout", ("strategy", "position_timeout")),
    ("quote_horizon_s", ("strategy", "quote_horizon_s")),
    ("kappa_depth_baseline", ("strategy", "kappa_depth_baseline")),
    ("depth_kappa_ratio", ("strategy", "depth_kappa_ratio")),
    ("thin_depth_threshold", ("strategy", "thin_depth_threshold")),
    ("ml_enabled", ("ml", "enabled")),
    ("model_dir", ("ml", "model_dir")),
    ("vol_blend", ("ml", "vol_blend")),
    ("skew_strength", ("ml", "skew_strength")),
    ("asym_strength", ("ml", "asym_strength")),
    ("gamma_dir_bonus", ("ml", "gamma_dir_bonus")),
    ("dir_threshold", ("ml", "dir_threshold")),
    ("ret_skew", ("ml", "ret_skew")),
    ("ret_shift_max_pct", ("ml", "ret_shift_max_pct")),
    ("ret_demean_halflife", ("ml", "ret_demean_halflife")),
    ("regime_enabled", ("regime", "enabled")),
    ("vol_baseline", ("regime", "vol_baseline")),
    ("gamma_scale_min", ("regime", "gamma_scale_min")),
    ("gamma_scale_max", ("regime", "gamma_scale_max")),
    ("liq_baseline", ("regime", "liq_baseline")),
    ("gamma_liq_scale_min", ("regime", "gamma_liq_scale_min")),
    ("gamma_liq_scale_max", ("regime", "gamma_liq_scale_max")),
    ("maker_fee", ("fees", "maker")),
    ("taker_fee", ("fees", "taker")),
    ("exit_urgency_strength", ("risk", "exit_urgency_strength")),
    ("circuit_breaker_sigma", ("risk", "circuit_breaker_sigma")),
    ("pnl_volatility_horizon_s", ("risk", "pnl_volatility_horizon_s")),
    ("urgency_time_weight", ("risk", "urgency_time_weight")),
    ("urgency_pnl_weight", ("risk", "urgency_pnl_weight")),
    ("urgency_signal_weight", ("risk", "urgency_signal_weight")),
    ("max_exec_book_age_s", ("risk", "max_exec_book_age_s")),
    ("ber_guard_thresh", ("strategy", "ber_guard_thresh")),
    ("ber_spread_mult", ("strategy", "ber_spread_mult")),
    ("ber_exposure_add_only", ("strategy", "ber_exposure_add_only")),
    ("vol_power", ("strategy", "vol_power")),
    ("markout_ema_span_fills", ("strategy", "markout_ema_span_fills")),
    ("markout_horizon_s", ("strategy", "markout_horizon_s")),
    ("markout_spread_scale", ("strategy", "markout_spread_scale")),
    ("markout_side_asymmetry_sign", ("strategy", "markout_side_asymmetry_sign")),
    ("spread_cap_mode", ("strategy", "spread_cap_mode")),
    ("adverse_guard_enabled", ("strategy", "adverse_guard_enabled")),
    ("adverse_toxicity_threshold", ("strategy", "adverse_toxicity_threshold")),
    ("adverse_markout_threshold", ("strategy", "adverse_markout_threshold")),
    ("adverse_markout_pause_threshold", ("strategy", "adverse_markout_pause_threshold")),
    ("adverse_markout_pause_hybrid", ("strategy", "adverse_markout_pause_hybrid")),
    ("adverse_markout_pause_base_s", ("strategy", "adverse_markout_pause_base_s")),
    ("adverse_markout_pause_min_s", ("strategy", "adverse_markout_pause_min_s")),
    ("adverse_markout_pause_max_s", ("strategy", "adverse_markout_pause_max_s")),
    ("adverse_markout_decay_tau_s", ("strategy", "adverse_markout_decay_tau_s")),
    ("adverse_dir_threshold", ("strategy", "adverse_dir_threshold")),
    ("adverse_ret_bps_threshold", ("strategy", "adverse_ret_bps_threshold")),
    ("adverse_microprice_shift_bps", ("strategy", "adverse_microprice_shift_bps")),
    ("adverse_spread_mult", ("strategy", "adverse_spread_mult")),
    ("adverse_thin_depth_threshold", ("strategy", "adverse_thin_depth_threshold")),
    ("adverse_thin_depth_mult", ("strategy", "adverse_thin_depth_mult")),
    ("adverse_pause", ("strategy", "adverse_pause")),
    ("defense_guard_enabled", ("strategy", "defense_guard_enabled")),
    ("defense_markout_threshold", ("strategy", "defense_markout_threshold")),
    ("defense_dir_threshold", ("strategy", "defense_dir_threshold")),
    ("defense_ret_bps_threshold", ("strategy", "defense_ret_bps_threshold")),
    ("defense_microprice_shift_bps", ("strategy", "defense_microprice_shift_bps")),
    ("defense_spread_mult", ("strategy", "defense_spread_mult")),
    ("defense_pause", ("strategy", "defense_pause")),
    ("defense_emergency_inventory_ratio", ("strategy", "defense_emergency_inventory_ratio")),
    ("defense_emergency_loss", ("strategy", "defense_emergency_loss")),
    ("flat_unilateral_max_s", ("strategy", "flat_unilateral_max_s")),
    ("local_extreme_guard_enabled", ("strategy", "local_extreme_guard_enabled")),
    ("local_extreme_window_s", ("strategy", "local_extreme_window_s")),
    ("local_extreme_rank_threshold", ("strategy", "local_extreme_rank_threshold")),
    ("local_extreme_require_thin_depth", ("strategy", "local_extreme_require_thin_depth")),
    ("local_extreme_thin_depth_threshold", ("strategy", "local_extreme_thin_depth_threshold")),
    ("local_extreme_spread_mult", ("strategy", "local_extreme_spread_mult")),
    ("local_extreme_pause", ("strategy", "local_extreme_pause")),
    ("fragile_order_ttl_s", ("strategy", "fragile_order_ttl_s")),
    ("buy_fill_selection_shadow_enabled", ("strategy", "buy_fill_selection_shadow_enabled")),
    ("buy_fill_selection_live_enabled", ("strategy", "buy_fill_selection_live_enabled")),
    ("buy_fill_selection_live_model_path", ("strategy", "buy_fill_selection_live_model_path")),
    ("buy_fill_selection_live_score_threshold", ("strategy", "buy_fill_selection_live_score_threshold")),
    ("buy_fill_selection_live_spread_mult_cap", ("strategy", "buy_fill_selection_live_spread_mult_cap")),
    ("buy_fill_selection_live_apply_reducing", ("strategy", "buy_fill_selection_live_apply_reducing")),
    ("buy_fill_selection_live_max_missing_features", ("strategy", "buy_fill_selection_live_max_missing_features")),
    ("dynamic_fill_hazard_shadow_enabled", ("strategy", "dynamic_fill_hazard_shadow_enabled")),
    ("dynamic_fill_hazard_shadow_model_path", ("strategy", "dynamic_fill_hazard_shadow_model_path")),
    ("dynamic_fill_hazard_shadow_model_sha256", ("strategy", "dynamic_fill_hazard_shadow_model_sha256")),
    ("dynamic_fill_hazard_shadow_sides", ("strategy", "dynamic_fill_hazard_shadow_sides")),
    ("dynamic_fill_hazard_shadow_exposure_ms", ("strategy", "dynamic_fill_hazard_shadow_exposure_ms")),
    ("dynamic_fill_hazard_shadow_price_jump_ticks", ("strategy", "dynamic_fill_hazard_shadow_price_jump_ticks")),
    ("dynamic_fill_hazard_action_enabled", ("strategy", "dynamic_fill_hazard_action_enabled")),
    ("dynamic_fill_hazard_action_policy_path", ("strategy", "dynamic_fill_hazard_action_policy_path")),
    ("dynamic_fill_hazard_action_policy_sha256", ("strategy", "dynamic_fill_hazard_action_policy_sha256")),
    ("state_conditioned_policy_mode", ("strategy", "state_conditioned_policy_mode")),
    ("state_conditioned_policy_model_path", ("strategy", "state_conditioned_policy_model_path")),
    ("fill_cooldown", ("strategy", "fill_cooldown")),
    ("fill_cooldown_consecutive_reset_policy", ("strategy", "fill_cooldown_consecutive_reset_policy")),
    ("fill_cooldown_reducing", ("strategy", "fill_cooldown_reducing")),
    ("fill_cooldown_reducing_campaign_only", ("strategy", "fill_cooldown_reducing_campaign_only")),
    ("fill_cooldown_reducing_inv_threshold", ("strategy", "fill_cooldown_reducing_inv_threshold")),
    ("fill_cooldown_reducing_inv_ratio", ("strategy", "fill_cooldown_reducing_inv_ratio")),
    ("fill_cooldown_reducing_age_s", ("strategy", "fill_cooldown_reducing_age_s")),
    ("fill_cooldown_reducing_vol_ref", ("strategy", "fill_cooldown_reducing_vol_ref")),
    ("fill_cooldown_reducing_vol_min_mult", ("strategy", "fill_cooldown_reducing_vol_min_mult")),
    ("fill_cooldown_reducing_vol_max_mult", ("strategy", "fill_cooldown_reducing_vol_max_mult")),
    ("boolean_cooldown_policy_enabled", ("strategy", "boolean_cooldown_policy_enabled")),
    ("boolean_cooldown_policy_path", ("strategy", "boolean_cooldown_policy_path")),
    ("boolean_cooldown_policy_sha256", ("strategy", "boolean_cooldown_policy_sha256")),
    ("boolean_cooldown_predicate_bundle_path", ("strategy", "boolean_cooldown_predicate_bundle_path")),
    ("boolean_cooldown_predicate_bundle_sha256", ("strategy", "boolean_cooldown_predicate_bundle_sha256")),
    ("boolean_cooldown_ema_warmup_s", ("strategy", "boolean_cooldown_ema_warmup_s")),
    ("boolean_cooldown_evidence_route", ("strategy", "boolean_cooldown_evidence_route")),
    ("buy_e3_cooldown_policy_enabled", ("strategy", "buy_e3_cooldown_policy_enabled")),
    ("buy_e3_cooldown_artifact_manifest_path", ("strategy", "buy_e3_cooldown_artifact_manifest_path")),
    ("buy_e3_cooldown_artifact_manifest_sha256", ("strategy", "buy_e3_cooldown_artifact_manifest_sha256")),
    ("buy_e3_cooldown_artifact_sha256", ("strategy", "buy_e3_cooldown_artifact_sha256")),
    ("buy_e3_cooldown_policy_path", ("strategy", "buy_e3_cooldown_policy_path")),
    ("buy_e3_cooldown_policy_sha256", ("strategy", "buy_e3_cooldown_policy_sha256")),
    ("buy_e3_cooldown_predicate_bundle_path", ("strategy", "buy_e3_cooldown_predicate_bundle_path")),
    ("buy_e3_cooldown_predicate_bundle_sha256", ("strategy", "buy_e3_cooldown_predicate_bundle_sha256")),
    ("buy_e3_cooldown_ema_warmup_s", ("strategy", "buy_e3_cooldown_ema_warmup_s")),
    ("buy_e3_cooldown_evidence_route", ("strategy", "buy_e3_cooldown_evidence_route")),
    ("post_fill_quote_response_enabled", ("strategy", "post_fill_quote_response_enabled")),
    ("post_fill_quote_response_mode", ("strategy", "post_fill_quote_response_mode")),
    ("post_fill_inventory_ticks_per_order_unit", ("strategy", "post_fill_inventory_ticks_per_order_unit")),
    ("post_fill_inventory_max_ticks", ("strategy", "post_fill_inventory_max_ticks")),
    ("post_fill_flow_ticks_per_excitation", ("strategy", "post_fill_flow_ticks_per_excitation")),
    ("post_fill_flow_max_ticks", ("strategy", "post_fill_flow_max_ticks")),
    ("post_fill_flow_excitation_per_order_unit", ("strategy", "post_fill_flow_excitation_per_order_unit")),
    ("post_fill_flow_max_excitation", ("strategy", "post_fill_flow_max_excitation")),
    ("post_fill_flow_amplitude_mode", ("strategy", "post_fill_flow_amplitude_mode")),
    ("post_fill_flow_expected_adverse_buy_ticks", ("strategy", "post_fill_flow_expected_adverse_buy_ticks")),
    ("post_fill_flow_expected_adverse_sell_ticks", ("strategy", "post_fill_flow_expected_adverse_sell_ticks")),
    ("post_fill_flow_add_distance_fraction_buy", ("strategy", "post_fill_flow_add_distance_fraction_buy")),
    ("post_fill_flow_add_distance_fraction_sell", ("strategy", "post_fill_flow_add_distance_fraction_sell")),
    ("post_fill_response_half_life_s", ("strategy", "post_fill_response_half_life_s")),
    ("post_fill_response_half_life_min_s", ("strategy", "post_fill_response_half_life_min_s")),
    ("post_fill_response_half_life_max_s", ("strategy", "post_fill_response_half_life_max_s")),
    ("post_fill_response_volatility_ref_bps", ("strategy", "post_fill_response_volatility_ref_bps")),
    ("post_fill_response_volatility_weight", ("strategy", "post_fill_response_volatility_weight")),
    ("post_fill_response_refill_edge_ref", ("strategy", "post_fill_response_refill_edge_ref")),
    ("post_fill_response_refill_weight", ("strategy", "post_fill_response_refill_weight")),
    ("post_fill_response_repair_probability_anchor", ("strategy", "post_fill_response_repair_probability_anchor")),
    ("post_fill_response_repair_probability_weight", ("strategy", "post_fill_response_repair_probability_weight")),
    ("adaptive_add_cooldown_enabled", ("strategy", "adaptive_add_cooldown_enabled")),
    ("adaptive_add_cooldown_min_mult", ("strategy", "adaptive_add_cooldown_min_mult")),
    ("adaptive_add_cooldown_max_mult", ("strategy", "adaptive_add_cooldown_max_mult")),
    ("adaptive_add_cooldown_w_markout", ("strategy", "adaptive_add_cooldown_w_markout")),
    ("adaptive_add_cooldown_w_flow", ("strategy", "adaptive_add_cooldown_w_flow")),
    ("adaptive_add_cooldown_w_campaign", ("strategy", "adaptive_add_cooldown_w_campaign")),
    ("adaptive_add_cooldown_w_trend", ("strategy", "adaptive_add_cooldown_w_trend")),
    ("adaptive_add_cooldown_w_refill_weak", ("strategy", "adaptive_add_cooldown_w_refill_weak")),
    ("adaptive_add_cooldown_w_refill_good", ("strategy", "adaptive_add_cooldown_w_refill_good")),
    ("adaptive_add_cooldown_w_reversion", ("strategy", "adaptive_add_cooldown_w_reversion")),
    ("adaptive_add_cooldown_mo_ref", ("strategy", "adaptive_add_cooldown_mo_ref")),
    ("adaptive_add_cooldown_flow_ref", ("strategy", "adaptive_add_cooldown_flow_ref")),
    ("adaptive_add_cooldown_campaign_inv_ref", ("strategy", "adaptive_add_cooldown_campaign_inv_ref")),
    ("adaptive_add_cooldown_campaign_age_ref_s", ("strategy", "adaptive_add_cooldown_campaign_age_ref_s")),
    ("adaptive_add_cooldown_trend_ret_ref", ("strategy", "adaptive_add_cooldown_trend_ret_ref")),
    ("adaptive_add_cooldown_refill_ref", ("strategy", "adaptive_add_cooldown_refill_ref")),
    ("adaptive_add_cooldown_reversion_ref", ("strategy", "adaptive_add_cooldown_reversion_ref")),
    ("adaptive_add_cooldown_gate_enabled", ("strategy", "adaptive_add_cooldown_gate_enabled")),
    ("adaptive_add_cooldown_gate_mult", ("strategy", "adaptive_add_cooldown_gate_mult")),
    ("adaptive_add_cooldown_gate_campaign_score", ("strategy", "adaptive_add_cooldown_gate_campaign_score")),
    ("adaptive_add_cooldown_gate_trend_score", ("strategy", "adaptive_add_cooldown_gate_trend_score")),
    ("adaptive_add_cooldown_gate_refill_edge_max", ("strategy", "adaptive_add_cooldown_gate_refill_edge_max")),
    ("adaptive_add_cooldown_gate_reversion_max", ("strategy", "adaptive_add_cooldown_gate_reversion_max")),
    ("adaptive_add_cooldown_gate_side", ("strategy", "adaptive_add_cooldown_gate_side")),
    ("use_bar_pricing", ("strategy", "use_bar_pricing")),
    ("symmetric_size", ("strategy", "symmetric_size")),
    ("toxicity_horizon_s", ("ml", "toxicity_horizon_s")),
    ("depth_tox_enabled", ("depth_execution", "depth_tox_spread", "enabled")),
    ("depth_tox_levels", ("depth_execution", "depth_tox_spread", "levels")),
    ("depth_tox_imbalance_threshold", ("depth_execution", "depth_tox_spread", "imbalance_threshold")),
    ("depth_tox_microprice_shift_bps", ("depth_execution", "depth_tox_spread", "microprice_shift_bps")),
    ("depth_tox_spread_mult", ("depth_execution", "depth_tox_spread", "spread_mult")),
    ("sync_adjust_degrade_enabled", ("risk", "sync_adjust_degrade_enabled")),
    ("sync_adjust_degrade_count", ("risk", "sync_adjust_degrade_count")),
    ("sync_adjust_abs_qty_threshold", ("risk", "sync_adjust_abs_qty_threshold")),
    ("sync_adjust_degrade_window_s", ("risk", "sync_adjust_degrade_window_s")),
    ("sync_adjust_pause_s", ("risk", "sync_adjust_pause_s")),
    ("sync_adjust_reconnect_user_stream", ("risk", "sync_adjust_reconnect_user_stream")),
    ("sync_adjust_cancel_orders", ("risk", "sync_adjust_cancel_orders")),
    ("cooldown_after_loss", ("risk", "cooldown_after_loss")),
    ("max_consecutive_losses", ("risk", "max_consecutive_losses")),
)


def _nested_attr(obj, path):
    for name in path:
        obj = getattr(obj, name)
    return obj


def to_backtest_params(cfg: Config) -> dict:
    # This is the explicit live→backtest ABI. Add new strategy fields here first,
    # then keep Python/C++ replay parity tests in step with the added key.
    params = {key: _nested_attr(cfg, path) for key, path in BACKTEST_PARAM_SOURCES}
    params["markout_ema_span_fills"] = max(
        0,
        int(params.get("markout_ema_span_fills", 0) or 0),
    )
    imb_cfg = getattr(cfg.depth_execution, "imbalance_asym", None)
    if imb_cfg and getattr(imb_cfg, "enabled", False):
        levels = int(getattr(imb_cfg, "levels", 20))
        params["book_imb_strength"] = float(getattr(imb_cfg, "strength", 0.0))
        params["book_imb_levels"] = levels
        params["trace_book_imb_levels"] = levels
    return params


def _dataclass_from_dict(cls, data: dict, *, path: str):
    if not isinstance(data, dict):
        raise ValueError(f"{path} must be a mapping")
    known = set(cls.__dataclass_fields__)
    unknown = sorted(set(data) - known)
    if unknown:
        keys = ", ".join(f"{path}.{name}" for name in unknown)
        raise ValueError(f"unknown config key(s): {keys}")
    kwargs = {}
    for name, field_info in cls.__dataclass_fields__.items():
        if name not in data:
            continue
        value = data[name]
        field_type = field_info.type
        origin = get_origin(field_type)
        args = get_args(field_type)
        if isinstance(value, dict) and is_dataclass(field_type):
            kwargs[name] = _dataclass_from_dict(
                field_type,
                value,
                path=f"{path}.{name}",
            )
        elif origin is list and isinstance(value, list) and args and is_dataclass(args[0]):
            kwargs[name] = [
                _dataclass_from_dict(
                    args[0],
                    item,
                    path=f"{path}.{name}[{index}]",
                )
                if isinstance(item, dict)
                else item
                for index, item in enumerate(value)
            ]
        else:
            kwargs[name] = value
    return cls(**kwargs)


def _parse(raw: dict) -> Config:
    if not isinstance(raw, dict):
        raise ValueError("config root must be a mapping")
    scalar_keys = {
        "project_name",
        "symbol",
        "tick_size",
        "lot_size",
        "min_notional",
    }
    section_keys = {
        "api",
        "strategy",
        "ml",
        "regime",
        "fees",
        "websocket",
        "risk",
        "logging",
        "lifecycle_journal_v2",
        "performance",
        "multi_market",
        "external_venues",
        "depth_execution",
    }
    unknown = sorted(set(raw) - scalar_keys - section_keys)
    if unknown:
        raise ValueError(
            "unknown config key(s): " + ", ".join(unknown)
        )
    cfg = Config()
    cfg.project_name = raw.get("project_name", cfg.project_name)
    cfg.symbol = raw.get("symbol", cfg.symbol)
    cfg.tick_size = raw.get("tick_size", cfg.tick_size)
    cfg.lot_size = raw.get("lot_size", cfg.lot_size)
    cfg.min_notional = raw.get("min_notional", cfg.min_notional)

    for section, cls in [
        ("api", ApiConfig), ("strategy", StrategyConfig),
        ("ml", MLConfig), ("regime", RegimeConfig),
        ("fees", FeeConfig),
        ("websocket", WebSocketConfig), ("risk", RiskConfig),
        ("logging", LogConfig), ("performance", PerfConfig),
        ("lifecycle_journal_v2", LifecycleJournalV2Config),
        ("multi_market", MultiMarketConfig),
        ("external_venues", ExternalVenuesConfig),
        ("depth_execution", DepthExecutionConfig),
    ]:
        sub = raw.get(section, {})
        if sub:
            obj = _dataclass_from_dict(cls, sub, path=section)
            setattr(cfg, section, obj)
    multi_raw = raw.get("multi_market", {})
    if not isinstance(multi_raw, dict):
        raise ValueError("multi_market must be a mapping")
    # Runtime attestation distinguishes an explicit fail-closed setting from a
    # dataclass default.  These private markers are not configurable fields.
    cfg.multi_market._global_flow_shadow_enabled_explicit = (
        "global_flow_shadow_enabled" in multi_raw
    )
    cfg.multi_market._global_reference_shadow_enabled_explicit = (
        "global_reference_shadow_enabled" in multi_raw
    )
    return cfg


def _validate_config(cfg: Config) -> None:
    """Validate loaded config before it can drive live/reload behavior."""
    from live.runtime_policy import (
        f05_boolean_cooldown_runtime_policy,
        f05_buy_e3_runtime_policy,
        q90_action_runtime_policy,
    )
    from strategy.fill_cooldown import normalize_consecutive_reset_policy

    for name in (
        "global_flow_shadow_enabled",
        "global_reference_shadow_enabled",
    ):
        value = getattr(cfg.multi_market, name, None)
        if type(value) is not bool:
            raise ValueError(f"multi_market.{name} must be a boolean")

    cfg.strategy.fill_cooldown_consecutive_reset_policy = (
        normalize_consecutive_reset_policy(
            cfg.strategy.fill_cooldown_consecutive_reset_policy,
            require_explicit=True,
        )
    )
    checkpoint_path = str(cfg.logging.fill_cooldown_checkpoint or "").strip()
    cooldown_stateful = any(
        (
            float(cfg.strategy.fill_cooldown) > 0.0,
            float(cfg.strategy.fill_cooldown_reducing) > 0.0,
            bool(cfg.strategy.boolean_cooldown_policy_enabled),
            bool(cfg.strategy.buy_e3_cooldown_policy_enabled),
        )
    )
    if cooldown_stateful and not checkpoint_path:
        raise ValueError(
            "stateful fill cooldown requires logging.fill_cooldown_checkpoint"
        )
    if "\x00" in checkpoint_path:
        raise ValueError("logging.fill_cooldown_checkpoint contains a NUL byte")
    lifecycle_v2 = cfg.lifecycle_journal_v2
    if int(lifecycle_v2.queue_size) <= 0:
        raise ValueError("lifecycle_journal_v2.queue_size must be positive")
    if float(lifecycle_v2.heartbeat_interval_s) <= 0.0:
        raise ValueError(
            "lifecycle_journal_v2.heartbeat_interval_s must be positive"
        )
    if float(lifecycle_v2.shutdown_drain_timeout_s) < 0.0:
        raise ValueError(
            "lifecycle_journal_v2.shutdown_drain_timeout_s cannot be negative"
        )
    if str(lifecycle_v2.storage_format).strip().lower() not in {"parquet", "jsonl"}:
        raise ValueError(
            "lifecycle_journal_v2.storage_format must be parquet or jsonl"
        )
    if not math.isfinite(float(lifecycle_v2.remote_session_max_duration_s)) or not (
        60.0 <= float(lifecycle_v2.remote_session_max_duration_s) <= 86_400.0
    ):
        raise ValueError(
            "lifecycle_journal_v2.remote_session_max_duration_s must be in [60, 86400]"
        )
    if not (
        1024 * 1024
        <= int(lifecycle_v2.remote_session_max_bytes)
        <= 100 * 1024 * 1024 * 1024
    ):
        raise ValueError(
            "lifecycle_journal_v2.remote_session_max_bytes must be in [1 MiB, 100 GiB]"
        )
    validate_lifecycle_journal_storage(
        profile=lifecycle_v2.storage_profile,
        journal_root=lifecycle_v2.root,
        prospective_epoch_root=lifecycle_v2.prospective_epoch_root,
        required_mount=lifecycle_v2.required_mount,
        remote_spool_allowlisted_roots=lifecycle_v2.remote_spool_allowlisted_roots,
        enabled=bool(lifecycle_v2.enabled),
    )
    if bool(lifecycle_v2.enabled):
        if not str(lifecycle_v2.baseline_identity_path).strip():
            raise ValueError(
                "enabled lifecycle_journal_v2 requires baseline_identity_path"
            )
        baseline_sha = str(lifecycle_v2.baseline_identity_sha256).strip().lower()
        if len(baseline_sha) != 64 or any(
            char not in "0123456789abcdef" for char in baseline_sha
        ):
            raise ValueError(
                "enabled lifecycle_journal_v2 requires baseline_identity_sha256"
            )
    if int(cfg.websocket.depth_levels) not in {5, 10, 20}:
        raise ValueError("websocket.depth_levels must be 5, 10, or 20")
    if int(cfg.websocket.depth_speed) not in {100, 250, 500}:
        raise ValueError("websocket.depth_speed must be 100, 250, or 500")
    if int(cfg.websocket.deep_book_snapshot_levels) not in {
        5,
        10,
        20,
        50,
        100,
        500,
        1000,
    }:
        raise ValueError(
            "websocket.deep_book_snapshot_levels must be a Binance-supported "
            "USD-M depth limit"
        )
    if int(cfg.websocket.deep_book_speed) not in {100, 250, 500}:
        raise ValueError("websocket.deep_book_speed must be 100, 250, or 500")
    if int(cfg.websocket.deep_book_max_buffer_events) <= 0:
        raise ValueError("websocket.deep_book_max_buffer_events must be positive")
    if float(cfg.websocket.deep_book_resync_backoff_s) <= 0.0:
        raise ValueError("websocket.deep_book_resync_backoff_s must be positive")
    if float(cfg.websocket.deep_book_max_age_s) <= 0.0:
        raise ValueError("websocket.deep_book_max_age_s must be positive")
    if float(cfg.risk.max_exec_book_visible_age_s) <= 0.0:
        raise ValueError("risk.max_exec_book_visible_age_s must be positive")
    if float(cfg.risk.max_exec_book_source_lag_s) <= 0.0:
        raise ValueError("risk.max_exec_book_source_lag_s must be positive")
    if bool(cfg.strategy.dynamic_fill_hazard_shadow_enabled):
        if not bool(cfg.websocket.deep_book_enabled):
            raise ValueError(
                "dynamic fill-hazard shadow requires websocket.deep_book_enabled"
            )
        if not str(cfg.strategy.dynamic_fill_hazard_shadow_model_path).strip():
            raise ValueError(
                "dynamic fill-hazard shadow requires a model artifact path"
            )
        sha = str(
            cfg.strategy.dynamic_fill_hazard_shadow_model_sha256
        ).strip().lower()
        if len(sha) != 64 or any(char not in "0123456789abcdef" for char in sha):
            raise ValueError(
                "dynamic fill-hazard shadow requires a 64-character SHA256"
            )
        sides = {
            side.strip().upper()
            for side in str(
                cfg.strategy.dynamic_fill_hazard_shadow_sides
            ).split(",")
            if side.strip()
        }
        if not sides or not sides.issubset({"BUY", "SELL"}):
            raise ValueError("dynamic fill-hazard shadow sides are invalid")
        if float(cfg.strategy.dynamic_fill_hazard_shadow_exposure_ms) <= 0.0:
            raise ValueError(
                "dynamic fill-hazard shadow exposure must be positive"
            )
        if float(
            cfg.strategy.dynamic_fill_hazard_shadow_price_jump_ticks
        ) <= 0.0:
            raise ValueError(
                "dynamic fill-hazard shadow jump threshold must be positive"
            )
    if bool(cfg.strategy.dynamic_fill_hazard_action_enabled):
        if not bool(cfg.strategy.dynamic_fill_hazard_shadow_enabled):
            raise ValueError(
                "dynamic fill-hazard action requires the hazard model"
            )
        if not bool(cfg.websocket.deep_book_enabled):
            raise ValueError(
                "dynamic fill-hazard action requires full-depth live state"
            )
        if str(cfg.strategy.dynamic_fill_hazard_shadow_sides).strip().upper() != "BUY":
            raise ValueError(
                "dynamic fill-hazard action requires BUY-only model scoring"
            )
        if not str(
            cfg.strategy.dynamic_fill_hazard_action_policy_path
        ).strip():
            raise ValueError(
                "dynamic fill-hazard action requires a policy artifact"
            )
        policy_sha = str(
            cfg.strategy.dynamic_fill_hazard_action_policy_sha256
        ).strip().lower()
        if len(policy_sha) != 64 or any(
            char not in "0123456789abcdef" for char in policy_sha
        ):
            raise ValueError(
                "dynamic fill-hazard action requires a 64-character SHA256"
            )
    q90_action_runtime_policy(
        bool(cfg.strategy.dynamic_fill_hazard_action_enabled)
    )
    f05_boolean_cooldown_runtime_policy(
        bool(cfg.strategy.boolean_cooldown_policy_enabled),
        evidence_route=cfg.strategy.boolean_cooldown_evidence_route,
    )
    f05_buy_e3_runtime_policy(
        bool(cfg.strategy.buy_e3_cooldown_policy_enabled),
        evidence_route=cfg.strategy.buy_e3_cooldown_evidence_route,
    )
    if bool(cfg.strategy.boolean_cooldown_policy_enabled):
        if not math.isclose(
            float(cfg.strategy.fill_cooldown),
            85.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError(
                "enabled F05 Boolean cooldown requires fill_cooldown=85"
            )
        if bool(cfg.strategy.adaptive_add_cooldown_enabled):
            raise ValueError(
                "enabled F05 Boolean cooldown requires adaptive add cooldown OFF"
            )
        if cfg.strategy.fill_cooldown_consecutive_reset_policy != "opposite_fill_only":
            raise ValueError(
                "enabled F05 Boolean cooldown requires opposite_fill_only reset"
            )
        if float(cfg.strategy.boolean_cooldown_ema_warmup_s) <= 0.0:
            raise ValueError(
                "boolean_cooldown_ema_warmup_s must be positive"
            )
        for field_name in (
            "boolean_cooldown_policy_path",
            "boolean_cooldown_predicate_bundle_path",
        ):
            if not str(getattr(cfg.strategy, field_name)).strip():
                raise ValueError(f"enabled F05 Boolean cooldown requires {field_name}")
        for field_name in (
            "boolean_cooldown_policy_sha256",
            "boolean_cooldown_predicate_bundle_sha256",
        ):
            value = str(getattr(cfg.strategy, field_name)).strip().lower()
            if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
                raise ValueError(
                    f"enabled F05 Boolean cooldown requires SHA256 field {field_name}"
                )
    if bool(cfg.strategy.buy_e3_cooldown_policy_enabled):
        if not math.isclose(
            float(cfg.strategy.fill_cooldown),
            85.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError("enabled F05 BUY E3 requires fill_cooldown=85")
        if bool(cfg.strategy.adaptive_add_cooldown_enabled):
            raise ValueError(
                "enabled F05 BUY E3 requires adaptive add cooldown OFF"
            )
        if cfg.strategy.fill_cooldown_consecutive_reset_policy != "opposite_fill_only":
            raise ValueError(
                "enabled F05 BUY E3 requires opposite_fill_only reset"
            )
        if float(cfg.strategy.buy_e3_cooldown_ema_warmup_s) < 2048.0:
            raise ValueError(
                "buy_e3_cooldown_ema_warmup_s must be at least 2048 seconds"
            )
        for field_name in (
            "buy_e3_cooldown_artifact_manifest_path",
            "buy_e3_cooldown_policy_path",
            "buy_e3_cooldown_predicate_bundle_path",
        ):
            if not str(getattr(cfg.strategy, field_name)).strip():
                raise ValueError(f"enabled F05 BUY E3 requires {field_name}")
        for field_name in (
            "buy_e3_cooldown_artifact_manifest_sha256",
            "buy_e3_cooldown_artifact_sha256",
            "buy_e3_cooldown_policy_sha256",
            "buy_e3_cooldown_predicate_bundle_sha256",
        ):
            value = str(getattr(cfg.strategy, field_name)).strip().lower()
            if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
                raise ValueError(
                    f"enabled F05 BUY E3 requires SHA256 field {field_name}"
                )
    cap_mode = str(getattr(cfg.strategy, "spread_cap_mode", "compress") or "compress").strip().lower()
    if cap_mode not in {"compress", "pause_exposure", "observe"}:
        raise ValueError(
            "strategy.spread_cap_mode must be compress, pause_exposure, or observe"
        )
    sign = float(getattr(cfg.strategy, "markout_side_asymmetry_sign", 1.0))
    if sign not in {-1.0, 1.0}:
        raise ValueError("strategy.markout_side_asymmetry_sign must be -1 or +1")
    if float(getattr(cfg.strategy, "quote_horizon_s", 0.0)) <= 0.0:
        raise ValueError("strategy.quote_horizon_s must be > 0")
    if float(getattr(cfg.strategy, "markout_horizon_s", 0.0)) <= 0.0:
        raise ValueError("strategy.markout_horizon_s must be > 0")
    if float(getattr(cfg.risk, "pnl_volatility_horizon_s", 0.0)) <= 0.0:
        raise ValueError("risk.pnl_volatility_horizon_s must be > 0")
    response_mode = str(
        getattr(cfg.strategy, "post_fill_quote_response_mode", "noop") or "noop"
    ).strip().lower()
    if response_mode not in {"noop", "inventory_shift", "flow_add_widen", "hybrid"}:
        raise ValueError(
            "strategy.post_fill_quote_response_mode must be noop, inventory_shift, "
            "flow_add_widen, or hybrid"
        )
    if bool(getattr(cfg.strategy, "post_fill_quote_response_enabled", False)):
        if response_mode == "noop":
            raise ValueError(
                "strategy.post_fill_quote_response_enabled requires a non-noop mode"
            )
        if response_mode in {"flow_add_widen", "hybrid"}:
            raise ValueError(
                "live A_t requires a causal campaign-repair model bundle; only "
                "inventory_shift is currently live-wired"
            )
    state_policy_mode = str(
        getattr(cfg.strategy, "state_conditioned_policy_mode", "disabled")
        or "disabled"
    ).strip().lower()
    if state_policy_mode not in {"disabled", "shadow", "active"}:
        raise ValueError(
            "strategy.state_conditioned_policy_mode must be disabled, shadow, or active"
        )
    state_policy_path = str(
        getattr(cfg.strategy, "state_conditioned_policy_model_path", "") or ""
    ).strip()
    if state_policy_mode != "disabled" and not state_policy_path:
        raise ValueError(
            "state-conditioned policy shadow/active mode requires a model artifact"
        )
    if (
        state_policy_mode == "active"
        and os.environ.get("NARROWGATE_ALLOW_STATE_CONDITIONED_POLICY_LIVE") != "1"
    ):
        raise ValueError(
            "State-conditioned live actions require an explicit promotion unlock. "
            "Use shadow mode until chronological OPE and later holdout pass; set "
            "NARROWGATE_ALLOW_STATE_CONDITIONED_POLICY_LIVE=1 only for a promoted trial."
        )

    external = getattr(cfg, "external_venues", None)
    fair_shadow_enabled = bool(
        getattr(cfg.strategy, "cross_venue_fair_price_shadow_enabled", False)
    )
    if fair_shadow_enabled:
        if external is None or not bool(getattr(external, "enabled", False)):
            raise ValueError(
                "cross-venue fair-price shadow requires external_venues.enabled"
            )
        if not bool(getattr(external, "shadow_only", True)):
            raise ValueError(
                "cross-venue fair-price inputs must remain external_venues.shadow_only"
            )
        enabled_sources = [
            source
            for source in getattr(external, "sources", [])
            if bool(getattr(source, "enabled", False))
        ]
        enabled_venues = {
            str(getattr(source, "venue", "")).strip().lower()
            for source in enabled_sources
        }
        if len(enabled_venues.intersection({"bitget", "bybit", "okx"})) < 2:
            raise ValueError(
                "cross-venue fair-price shadow requires at least two enabled venues"
            )
        multi_market = getattr(cfg, "multi_market", None)
        if multi_market is None or not bool(getattr(multi_market, "enabled", False)):
            raise ValueError(
                "cross-venue fair-price shadow requires the Binance stablecoin anchor"
            )
    if external and getattr(external, "enabled", False):
        if not bool(getattr(external, "shadow_only", True)):
            raise ValueError(
                "external_venues is currently shadow-only; it cannot route or place orders"
            )
        market_ids: set[tuple[str, str, str]] = set()
        for source in getattr(external, "sources", []):
            venue = str(getattr(source, "venue", "")).strip().lower()
            if venue not in {"bitget", "bybit", "okx"}:
                raise ValueError(f"unsupported external venue adapter: {venue or '<empty>'}")
            if str(getattr(source, "role", "reference")).lower() != "reference":
                raise ValueError("external venue sources must use role=reference")
            instrument_type = str(
                getattr(source, "instrument_type", "perp") or "perp"
            ).strip().lower()
            if instrument_type not in {"perp", "spot"}:
                raise ValueError(
                    "external venue instrument_type must be perp or spot"
                )
            symbol = str(getattr(source, "symbol", "BTCUSDT") or "BTCUSDT").upper()
            identity = (venue, instrument_type, symbol)
            if identity in market_ids:
                raise ValueError(
                    f"duplicate external venue market source: {venue}:{instrument_type}:{symbol}"
                )
            market_ids.add(identity)
            transport = str(getattr(source, "transport", "websocket")).strip().lower()
            allowed_transports = (
                {"websocket"} if venue == "bitget" else
                {"rest", "websocket"} if venue == "bybit" else
                {"rest", "websocket"}
            )
            if transport not in allowed_transports:
                raise ValueError(
                    f"external venue {venue} requires transport in "
                    f"{sorted(allowed_transports)}, got {transport or '<empty>'}"
                )
            if int(getattr(source, "record_queue_size", 20_000)) <= 0:
                raise ValueError("external venue record_queue_size must be positive")
            product_type = str(getattr(source, "product_type", "") or "").strip().lower()
            expected_product = (
                "spot" if instrument_type == "spot" else
                "usdt-futures" if venue == "bitget" else
                "linear" if venue == "bybit" else
                "swap"
            )
            if product_type != expected_product:
                raise ValueError(
                    f"external venue {venue}:{instrument_type} requires "
                    f"product_type={expected_product}, got {product_type or '<empty>'}"
                )
            if venue == "okx":
                instrument_id = str(getattr(source, "instrument_id", "") or "").strip().upper()
                expected_id = "BTC-USDT" if instrument_type == "spot" else "BTC-USDT-SWAP"
                if instrument_id != expected_id:
                    raise ValueError(
                        f"external venue okx:{instrument_type} requires "
                        f"instrument_id={expected_id}, got {instrument_id or '<empty>'}"
                    )
                if float(getattr(source, "contract_multiplier", 1.0)) <= 0.0:
                    raise ValueError("external venue contract_multiplier must be positive")
                if instrument_type == "spot" and not math.isclose(
                    float(getattr(source, "contract_multiplier", 1.0)), 1.0
                ):
                    raise ValueError("external venue okx:spot requires contract_multiplier=1.0")


def _stable_config_bytes(path: Path) -> tuple[bytes, tuple[int, int, int, int]]:
    """Read one regular config while binding its exact filesystem identity."""
    candidate = path.expanduser().resolve(strict=True)
    with candidate.open("rb") as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("config source must be a regular file")
        raw = handle.read()
        after = os.fstat(handle.fileno())
    lexical_after = candidate.stat()
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    path_identity = (
        lexical_after.st_dev,
        lexical_after.st_ino,
        lexical_after.st_size,
        lexical_after.st_mtime_ns,
    )
    if before_identity != after_identity or before_identity != path_identity:
        raise ValueError("config source changed during stable read")
    if len(raw) != before.st_size:
        raise ValueError("config source size changed during stable read")
    return raw, before_identity


def revalidate_loaded_config_source(cfg: Config, path: Path) -> str:
    """Prove that ``cfg`` still names the exact bytes parsed at load time."""
    resolved = path.expanduser().resolve(strict=True)
    expected_path = getattr(cfg, "_source_file_path", None)
    expected_sha256 = getattr(cfg, "_source_file_sha256", None)
    expected_identity = getattr(cfg, "_source_file_identity", None)
    if (
        expected_path != str(resolved)
        or not isinstance(expected_sha256, str)
        or len(expected_sha256) != 64
        or not isinstance(expected_identity, tuple)
        or len(expected_identity) != 4
    ):
        raise ValueError("loaded config source identity is unavailable")
    raw, observed_identity = _stable_config_bytes(resolved)
    if observed_identity != expected_identity:
        raise ValueError("loaded config source file identity drifted")
    observed_sha256 = hashlib.sha256(raw).hexdigest()
    if observed_sha256 != expected_sha256:
        raise ValueError("loaded config source SHA256 drifted")
    return expected_sha256


def _load_config_candidate(path: Path) -> Config:
    """Parse and validate one config without changing the active config."""
    p = path.expanduser().resolve(strict=True)
    source, source_identity = _stable_config_bytes(p)
    raw = yaml.safe_load(source.decode("utf-8")) or {}

    cfg = _parse(raw)
    cfg._source_file_path = str(p)
    cfg._source_file_sha256 = hashlib.sha256(source).hexdigest()
    cfg._source_file_identity = source_identity

    # Allow env-var override for secrets (never commit keys)
    if os.environ.get("BINANCE_API_KEY"):
        cfg.api.key = os.environ["BINANCE_API_KEY"]
    if os.environ.get("BINANCE_API_SECRET"):
        cfg.api.secret = os.environ["BINANCE_API_SECRET"]

    _validate_config(cfg)
    return cfg


def require_multi_market_shadow_restart(previous: Config, candidate: Config) -> None:
    """Reject hot changes to diagnostic evaluators before runtime mutation."""
    previous_multi = getattr(previous, "multi_market", None)
    candidate_multi = getattr(candidate, "multi_market", None)
    for name in (
        "global_flow_shadow_enabled",
        "global_reference_shadow_enabled",
    ):
        if bool(getattr(previous_multi, name, False)) != bool(
            getattr(candidate_multi, name, False)
        ):
            raise ValueError(
                f"multi_market.{name} is restart-only and cannot be hot-reloaded"
            )


def load_config(path: Optional[Path] = None) -> Config:
    """Load config from YAML. Supports env var override for API keys."""
    global _cfg, _cfg_path
    p = (Path(path) if path else _cfg_path).expanduser().resolve()
    cfg = _load_config_candidate(p)

    with _lock:
        _cfg = cfg
        _cfg_path = p
    return cfg


_engine_ref = None  # Will be set by main.py after engine creation


def set_engine_ref(engine):
    """Register the engine so reload_config can update its cfg."""
    global _engine_ref
    _engine_ref = engine


def reload_config(*_args):
    """SIGHUP handler — reload config from disk and propagate to engine."""
    global _cfg
    with _lock:
        previous_cfg = _cfg
        active_path = _cfg_path
    try:
        cfg = _load_config_candidate(active_path)
        if previous_cfg is not None:
            from live.runtime_policy import (
                require_f05_boolean_cooldown_restart,
                require_f05_buy_e3_restart,
                require_q90_action_restart,
            )

            require_q90_action_restart(
                previous_cfg.strategy.dynamic_fill_hazard_action_enabled,
                cfg.strategy.dynamic_fill_hazard_action_enabled,
            )
            require_f05_boolean_cooldown_restart(
                vars(previous_cfg.strategy),
                vars(cfg.strategy),
            )
            require_f05_buy_e3_restart(
                vars(previous_cfg.strategy),
                vars(cfg.strategy),
            )
            require_multi_market_shadow_restart(previous_cfg, cfg)
        revalidate_loaded_config_source(cfg, active_path)
        if _engine_ref is not None:
            _engine_ref.on_config_reload(cfg)
        with _lock:
            _cfg = cfg
        logger.info(f"Reloaded {active_path}: γ={cfg.strategy.gamma}, "
                    f"fallback_κ={cfg.strategy.kappa}, vol_blend={cfg.ml.vol_blend}")
        if _engine_ref is not None:
            logger.info("Config propagated to running engine via on_config_reload")
    except Exception as e:
        if previous_cfg is not None:
            with _lock:
                _cfg = previous_cfg
        logger.error(f"Reload failed: {e}")


def install_reload_handler():
    """Install SIGHUP handler for hot-reload."""
    signal.signal(signal.SIGHUP, reload_config)
