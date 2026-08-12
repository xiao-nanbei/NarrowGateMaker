"""
NarrowGate Maker Engine Core — 做市引擎主循环。

整合 SignalEngine → AS公式 → OrderManager → InventoryManager。

核心流程 (每~10s):
  1. signal.compute_signal()  →  ML prediction
  2. AS公式 (with ML override) →  bid/ask quotes
  3. 风控检查 →  是否允许报单
  4. cancel stale orders + place new orders
  5. position timeout check
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import os
import threading
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import asdict, dataclass, is_dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from execution.order_lifecycle import (
    OrderLifecyclePhase,
    TerminalPolicyRoute,
    terminal_policy_route,
)
from execution.order_lifecycle_journal import (
    OrderLifecycleJournalRow,
    order_lifecycle_journal_payload,
)
from execution.exact_opportunity_tape import (
    ExactQuoteOpportunityTapeRow,
    empty_exact_opportunity_row,
    exact_quote_role,
)
from execution.exact_opportunity_tape_runtime import (
    ExactOpportunityDailyWriter,
    build_exact_opportunity_runtime_identity,
    validate_exact_opportunity_runtime_config,
)
from strategy.inventory_manager import InventoryManager, PositionState
from strategy.order_manager import OrderManager, OrderState, Side
from strategy.post_fill_quote_response import (
    PostFillQuoteResponse,
    PostFillQuoteResponseConfig,
)
from strategy.quote_core import (
    QuotePrediction,
    QuoteState,
    SPREAD_CAP_COMPRESS,
    SPREAD_CAP_PAUSE_EXPOSURE,
    apply_final_spread_cap,
    apply_final_spread_cap_preserve_side,
    ber_inventory_role_for_target,
    compose_ber_exposure_add_only_quote,
    circuit_breaker_loss_threshold,
    circuit_breaker_triggered,
    compute_quote_core_live,
    microprice_from_book,
    quote_core_config_from_live_config,
    quote_depth_from_book,
    spread_cap_mode_code,
)
from strategy.policy_guards import (
    CommonSidePolicyInput,
    LocalExtremeGuardConfig,
    POLICY_REASON_ADVERSE,
    POLICY_REASON_BURST,
    POLICY_REASON_BUY_FILL_SELECTION,
    POLICY_REASON_BUY_HAZARD_CANCEL,
    POLICY_REASON_DEFENSE,
    POLICY_REASON_EXPOSURE_ONLY,
    POLICY_REASON_FILL_COOLDOWN,
    POLICY_REASON_FLAT_TTL,
    POLICY_REASON_INV_LIMIT,
    POLICY_REASON_MARKOUT,
    POLICY_REASON_SPREAD_CAP,
    POLICY_REASON_STALE_HARD,
    POLICY_REASON_STALE_WARN,
    POLICY_REASON_SYNC_DEGRADED,
    POLICY_REASON_THIN_DEPTH,
    apply_local_extreme_guard_context,
    evaluate_common_side_policy,
    local_extreme_rank,
)
from strategy.policy_guards import (
    AdaptiveAddCooldownConfig,
    adaptive_add_cooldown_multiplier,
)
from strategy.fill_selection_model import (
    FillSelectionScoreEnsemble,
    build_fill_selection_feature_row,
    fill_selection_actionable,
)
from strategy.fill_cooldown import (
    RESET_OPPOSITE_FILL_OR_EXPIRY,
    normalize_consecutive_reset_policy,
    update_same_side_fill_units,
)
from strategy.boolean_cooldown_live import LiveBooleanCooldownPolicy
from strategy.dynamic_fill_hazard_model import (
    DynamicFillHazardActionPolicy,
    DynamicFillHazardBundle,
    DynamicFillHazardShadowRuntime,
)
from strategy.cross_venue_fair_price import (
    CrossVenueFairPriceState,
    FAIR_PRICE_SCHEMA_VERSION,
    project_fair_center_shadow,
)
from strategy.external_adverse_quote_edge_guard import (
    ExternalAdverseQuoteEdgeProjection,
    project_external_adverse_quote_edge,
)
from features.feature_dag import CROSS_VENUE_FAIR_PRICE_GRAPH, TEN_SECOND_CAUSAL_GRAPH
from strategy.signal import (
    Prediction,
    QuoteDecisionSnapshot,
    QuotePostOnlyGuard,
    SignalEngine,
)
from strategy.state_conditioned_quote_policy import (
    StateConditionedQuotePolicy,
    apply_local_add_action,
    inventory_role_for_quote,
)
from market_fusion import default_reference_symbol, normalize_symbol
from models.replay.prospective_baseline_epoch import (
    CPP_FEATURE_RECONSTRUCTION_CONTRACT,
    PROSPECTIVE_INITIAL_STATE_COMPLETENESS_SCHEMA_VERSION,
    PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS,
    PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS,
    PYTHON_FEATURE_STATE_CONTRACT,
)

try:
    from research.families.f05_fill_quality_quote_ev.quote_ev import materialize_quote_ev_feature_values
except Exception:  # pragma: no cover - live can run without research models on PYTHONPATH.
    materialize_quote_ev_feature_values = None

logger = logging.getLogger("maker_engine")


_QUOTE_ASSET_SUFFIXES = ("USDC", "USDT", "BUSD", "FDUSD", "USD")


def _prospective_state_plain(
    value: Any,
    *,
    path: str,
    unsupported: list[str],
) -> Any:
    """Normalize runtime state without silently stringifying unknown objects."""

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return {"nonfinite_float": "nan"}
        if math.isinf(value):
            return {"nonfinite_float": "+inf" if value > 0 else "-inf"}
        return value
    if isinstance(value, Enum):
        return _prospective_state_plain(
            value.value,
            path=path,
            unsupported=unsupported,
        )
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return _prospective_state_plain(
            asdict(value),
            path=path,
            unsupported=unsupported,
        )
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, nested in value.items():
            normalized_key = (
                str(key)
                if isinstance(key, (str, int, float, bool))
                else json.dumps(
                    _prospective_state_plain(
                        key,
                        path=f"{path}.<key>",
                        unsupported=unsupported,
                    ),
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
            if normalized_key in normalized:
                unsupported.append(f"{path}.duplicate_normalized_key:{normalized_key}")
                continue
            normalized[normalized_key] = _prospective_state_plain(
                nested,
                path=f"{path}.{normalized_key}",
                unsupported=unsupported,
            )
        return {key: normalized[key] for key in sorted(normalized)}
    if isinstance(value, (list, tuple, deque)):
        return [
            _prospective_state_plain(
                nested,
                path=f"{path}[{index}]",
                unsupported=unsupported,
            )
            for index, nested in enumerate(value)
        ]
    if isinstance(value, (set, frozenset)):
        rows = [
            _prospective_state_plain(
                nested,
                path=f"{path}[]",
                unsupported=unsupported,
            )
            for nested in value
        ]
        return sorted(
            rows,
            key=lambda nested: json.dumps(
                nested,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ),
        )
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _prospective_state_plain(
                item(),
                path=path,
                unsupported=unsupported,
            )
        except Exception:
            pass
    unsupported.append(f"{path}:{type(value).__module__}.{type(value).__qualname__}")
    return {"unsupported_type": f"{type(value).__module__}.{type(value).__qualname__}"}


def _prospective_state_fingerprint(
    value: Any,
    *,
    path: str,
    unsupported: list[str],
) -> tuple[Any, str]:
    normalized = _prospective_state_plain(
        value,
        path=path,
        unsupported=unsupported,
    )
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()
    return normalized, hashlib.sha256(encoded).hexdigest()


def _infer_symbol_assets(symbol: str) -> tuple[str, str]:
    normalized = str(symbol or "").upper()
    for quote_asset in _QUOTE_ASSET_SUFFIXES:
        if normalized.endswith(quote_asset) and len(normalized) > len(quote_asset):
            return normalized[:-len(quote_asset)], quote_asset
    return "", ""


def _commission_in_quote_asset(
    amount: float,
    asset: str,
    *,
    fill_price: float,
    base_asset: str,
    quote_asset: str,
    settlement_asset: str,
) -> float:
    """Convert a fill commission to quote currency without mixing units."""

    value = float(amount)
    if abs(value) <= 1e-18:
        return 0.0
    normalized = str(asset or "").upper()
    base = str(base_asset or "").upper()
    quote = str(quote_asset or "").upper()
    settlement = str(settlement_asset or "").upper()
    if not normalized:
        raise ValueError("nonzero commission is missing its asset")
    if normalized in {quote, settlement} - {""}:
        return value
    if normalized == base and fill_price > 0.0:
        return value * float(fill_price)
    raise ValueError(
        f"unsupported commission asset {normalized!r}; "
        f"base={base!r} quote={quote!r} settlement={settlement!r}"
    )

@dataclass
class SidePolicyDecision:
    side: str
    mode: str = "normal"
    allow_post: bool = True
    allow_exposure_increase: bool = True
    spread_mult: float = 1.0
    size_mult: float = 1.0
    reason_mask: int = 0
    reason_text: str = "none"
    inventory_ratio: float = 0.0
    toxicity: float = 0.5
    markout_ema: float = 0.0
    depth_age_s: float = 0.0
    microprice_shift_bps: float = 0.0
    l2_quote_flip_rate: float = 0.0
    l2_book_refresh_ratio: float = 0.0
    l2_book_cancel_ratio: float = 0.0
    l2_near_depth_total: float = 0.0
    # 中文说明：字段名保留 bid_quote_* 是为了兼容历史 CSV schema；
    # 实际上 SELL/ASK policy 也会把逐侧 quote EV 结果写到这些列里。
    bid_quote_ev_30s: float = 0.0
    bid_quote_toxic_30s: float = 0.0
    bid_quote_fill_prob: float = 0.0
    bid_quote_fill_markout_30s: float = 0.0
    order_ttl_ms: int = 0


@dataclass
class QuoteDecisionLogRow:
    timestamp: str
    symbol: str
    side: str
    mode: str
    allow_post: int
    allow_exposure_increase: int
    reason_mask: int
    reason_text: str
    spread_mult: float
    size_mult: float
    inventory_ratio: float
    toxicity: float
    markout_ema: float
    depth_age_s: float
    microprice_shift_bps: float
    l2_quote_flip_rate: float
    l2_book_refresh_ratio: float
    l2_book_cancel_ratio: float
    l2_near_depth_total: float
    bid_quote_ev_30s: float
    bid_quote_toxic_30s: float
    bid_quote_fill_prob: float
    bid_quote_fill_markout_30s: float
    mid: float
    base_price: float
    final_price: float
    base_size: float
    final_size: float
    can_post_after_inventory: int
    order_active_before: int
    needs_update: int
    action: str


@dataclass
class CrossVenueFairPriceShadowLogRow:
    timestamp: str
    decision_ts_ns: int
    symbol: str
    schema_version: str
    feature_graph_sha256: str
    valid: int
    reason: str
    action_authorized: int
    executed_action: str
    local_mid: float
    external_fair: float
    raw_lead_bps: float
    gain: float
    center_shift_price: float
    center_shift_bps: float
    confidence: float
    dispersion_bps: float
    valid_venues: int
    venue_ids: str
    minimum_basis_samples: int
    lead_variance_bps2: float
    noise_variance_bps2: float
    max_source_age_ms: float
    max_feed_latency_ms: float
    max_feature_latency_ms: float
    source_kinds: str
    transport_supported: int
    baseline_bid: float
    baseline_ask: float
    candidate_bid: float
    candidate_ask: float
    requested_shift_ticks: int
    effective_shift_ticks: int
    gtx_clamped: int
    pair_spread_preserved: int


@dataclass
class BuyFillSelectionShadowLogRow:
    timestamp: str
    symbol: str
    enabled: int
    hit: int
    actionable_hit: int
    q: float
    mid: float
    score: float
    threshold: float
    missing_features: int
    used_features: int
    model_count: int
    base_spread_mult: float
    final_spread_mult: float
    quote_distance_bps: float
    near_depth: float
    queue_local_rank: float
    trend_inventory_risk_score: float
    micro_reversion_score: float
    allow_post: int
    allow_exposure_increase: int
    exposure_increasing: int
    reason_mask_before: int
    reason_mask_after: int
    hard_blocked: int
    order_ttl_ms: int


@dataclass
class DynamicFillHazardShadowLogRow:
    timestamp: str
    symbol: str
    model_family_id: str
    model_file_sha256: str
    client_order_id: str
    side: str
    inventory_role: str
    valid: int
    reason: str
    edge_ms: int
    elapsed_ms: float
    missed_edges: int
    feature_source_ts_ns: int
    feature_ready_ts_ns: int
    deep_generation: int
    deep_age_ms: float
    order_price: float
    mid: float
    microprice: float
    queue_initial: float
    queue_remaining: float
    cancel_events: int
    cancel_qty: float
    refill_events: int
    refill_qty: float
    favorable_probability: float
    adverse_probability: float
    favorable_raw_probability: float
    adverse_raw_probability: float
    action_authorized: int
    executed_action: str


@dataclass
class DynamicFillHazardActionLogRow:
    timestamp: str
    symbol: str
    policy_id: str
    policy_file_sha256: str
    model_file_sha256: str
    client_order_id: str
    inventory_role: str
    event: str
    adverse_value: float
    entry_threshold: float
    favorable_probability: float
    adverse_probability: float
    order_state: str
    cancel_succeeded: int
    hold_age_ms: float
    deep_generation: int
    deep_age_ms: float


@dataclass
class _BuyHazardCancelHold:
    client_order_id: str
    order_price: float
    entered_ts_ns: int
    entry_score: float
    phase: OrderLifecyclePhase = OrderLifecyclePhase.CANCEL_PENDING
    recovered: bool = False
    terminal_seen: bool = False
    cancel_succeeded: bool = False
    exchange_terminal_ts_ns: int = 0


@dataclass
class StateConditionedPolicyShadowLogRow:
    timestamp: str
    symbol: str
    policy_id: str
    policy_mode: str
    side: str
    inventory_role: str
    campaign_id: int
    q: float
    mid: float
    eligible: int
    reason: str
    candidate_action: str
    executed_action: str
    baseline_value: float
    candidate_value: float
    estimated_advantage: float
    feature_age_ms: float
    baseline_price: float
    candidate_price: float
    executed_price: float
    action_delta_ticks: float
    action_effective: int
    clamp_reason: str
    allow_post: int
    allow_exposure_increase: int


@dataclass
class InventoryCampaignShadowLogRow:
    timestamp: str
    symbol: str
    q: float
    mid: float
    active: int
    campaign_id: int
    side: str
    age_s: float
    max_abs_qty: float
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    adverse_excursion: float
    fills: int
    buy_fills: int
    sell_fills: int
    exposure_increasing_fills: int
    reducing_fills: int
    bid_exposure_increasing: int
    ask_exposure_increasing: int
    bid_block_if_inv_006: int
    ask_block_if_inv_006: int
    bid_block_if_inv_008: int
    ask_block_if_inv_008: int
    bid_block_if_inv_010: int
    ask_block_if_inv_010: int
    bid_block_if_age_20m: int
    ask_block_if_age_20m: int
    bid_block_if_age_40m: int
    ask_block_if_age_40m: int
    bid_block_if_age_60m: int
    ask_block_if_age_60m: int
    bid_block_if_reducing_only: int
    ask_block_if_reducing_only: int


@dataclass
class LivePerfTelemetryLogRow:
    timestamp: str
    symbol: str
    event: str
    status: str
    requote_id: int
    mid: float
    q: float
    requote_total_us: float
    sync_check_us: float
    stale_check_us: float
    signal_compute_us: float
    risk_check_us: float
    compute_quotes_us: float
    update_orders_us: float
    rest_new_count: int
    rest_new_sum_us: float
    rest_new_max_us: float
    rest_cancel_count: int
    rest_cancel_sum_us: float
    rest_cancel_max_us: float
    rest_cancel_all_count: int
    rest_cancel_all_sum_us: float
    rest_cancel_all_max_us: float
    exec_trade_age_s: float
    exec_book_age_s: float
    exec_depth_age_s: float
    anchor_trade_max_age_s: float
    anchor_book_max_age_s: float
    spot_trade_max_age_s: float
    spot_book_max_age_s: float
    active_orders: int
    bid_action: str
    ask_action: str
    cpp_routing_used: int


@dataclass
class QuoteSnapshotIntegrityLogRow:
    timestamp: str
    symbol: str
    requote_id: int
    status: str
    use_bar_pricing: int
    snapshot_valid: int
    invalid_reason: str
    capture_ts_ns: int
    market_generation: int
    depth_generation: int
    book_ticker_generation: int
    depth_bid: float
    depth_ask: float
    book_ticker_bid: float
    book_ticker_ask: float
    guard_bid: float
    guard_ask: float
    guard_source: str
    guard_fallback_reason: str
    depth_total_age_s: float
    depth_visible_age_s: float
    depth_source_lag_s: float
    book_ticker_visible_age_s: float
    book_ticker_source_lag_s: float
    snapshot_lock_wait_us: float
    snapshot_lock_hold_us: float
    bar_pricing_mid: float
    pricing_mid: float
    final_bid: float
    final_ask: float
    quote_identity_error_ticks: float
    post_only_violation_count: int
    consecutive_snapshot_blocks: int
    rest_cancel_count: int
    active_orders: int
    bid_action: str
    ask_action: str


@dataclass
class OrderOutcomeLogRow:
    timestamp: str
    symbol: str
    event_type: str
    client_order_id: str
    side: str
    price: float
    quantity: float
    filled_qty: float
    avg_fill_price: float
    age_ms: int
    mode: str
    reason_mask: int
    reason_text: str
    spread_mult: float
    size_mult: float
    inventory_ratio: float
    toxicity: float
    markout_ema: float
    depth_age_s: float
    microprice_shift_bps: float
    l2_quote_flip_rate: float
    l2_book_refresh_ratio: float
    l2_book_cancel_ratio: float
    l2_near_depth_total: float
    bid_quote_ev_30s: float
    bid_quote_toxic_30s: float
    bid_quote_fill_prob: float
    bid_quote_fill_markout_30s: float
    mid: float
    target_price: float
    target_qty: float

# ── P3 fill probability model (lazy load) ──
_fill_model = None
_fill_model_path = None
_fill_model_loaded = False
_buy_fill_selection_model = None
_buy_fill_selection_model_path = None
_live_routing_cpp = None
_live_routing_cpp_failed = False


def _cpp_strict() -> bool:
    return os.environ.get("NARROWGATE_CPP_STRICT", "").strip().lower() in {"1", "true", "yes", "on"}


def _live_routing_cpp_enabled() -> bool:
    return os.environ.get("NARROWGATE_CPP_LIVE_ROUTING", "").strip().lower() in {"1", "true", "yes", "on"}


def _get_live_routing_cpp():
    global _live_routing_cpp, _live_routing_cpp_failed
    if not _live_routing_cpp_enabled():
        return None
    if _live_routing_cpp is not None:
        return _live_routing_cpp
    if _live_routing_cpp_failed and not _cpp_strict():
        return None
    try:
        import narrowgate_cpp  # type: ignore
        if not hasattr(narrowgate_cpp, "compute_live_routing_decision"):
            raise RuntimeError("narrowgate_cpp missing compute_live_routing_decision")
        _live_routing_cpp = narrowgate_cpp
        return _live_routing_cpp
    except Exception:
        _live_routing_cpp_failed = True
        if _cpp_strict():
            raise
        return None


def _resolve_model_dir(cfg) -> Optional[Path]:
    model_dir = getattr(cfg.ml, 'model_dir', '')
    if not model_dir:
        return None
    model_path = Path(model_dir).expanduser()
    if not model_path.is_absolute():
        model_path = Path(__file__).resolve().parent.parent / model_path
    return model_path


def _get_fill_model(model_dir: Optional[Path] = None):
    """Lazy-load FillProbabilityModel from saved params."""
    global _fill_model, _fill_model_path, _fill_model_loaded
    model_path = model_dir / "fill_prob_params.json" if model_dir else None
    if not _fill_model_loaded or model_path != _fill_model_path:
        try:
            from research.families.f02_empirical_p3_touch.fill_probability import FillProbabilityModel
            _fill_model = FillProbabilityModel.load(model_path)
            _fill_model_path = model_path
            _fill_model_loaded = True
            logger.info(f"Loaded fill probability model: {_fill_model} ({model_path})")
        except Exception as e:
            _fill_model = None
            _fill_model_path = model_path
            _fill_model_loaded = True
            logger.warning(f"Fill probability model not available: {e}")
    return _fill_model


def _resolve_buy_fill_selection_model_path(cfg) -> Optional[Path]:
    raw = getattr(cfg.strategy, "buy_fill_selection_live_model_path", "") or ""
    if not raw:
        return None
    model_path = Path(raw).expanduser()
    if not model_path.is_absolute():
        model_path = Path(__file__).resolve().parent.parent / model_path
    return model_path


def _resolve_state_conditioned_policy_path(cfg) -> Optional[Path]:
    raw = getattr(cfg.strategy, "state_conditioned_policy_model_path", "") or ""
    if not raw:
        return None
    model_path = Path(raw).expanduser()
    if not model_path.is_absolute():
        model_path = Path(__file__).resolve().parent.parent / model_path
    return model_path


def _resolve_dynamic_fill_hazard_shadow_path(cfg) -> Optional[Path]:
    raw = (
        getattr(
            cfg.strategy,
            "dynamic_fill_hazard_shadow_model_path",
            "",
        )
        or ""
    )
    if not raw:
        return None
    model_path = Path(raw).expanduser()
    if not model_path.is_absolute():
        model_path = Path(__file__).resolve().parent.parent / model_path
    return model_path


def _resolve_dynamic_fill_hazard_action_policy_path(
    cfg,
) -> Optional[Path]:
    raw = (
        getattr(
            cfg.strategy,
            "dynamic_fill_hazard_action_policy_path",
            "",
        )
        or ""
    )
    if not raw:
        return None
    policy_path = Path(raw).expanduser()
    if not policy_path.is_absolute():
        policy_path = Path(__file__).resolve().parent.parent / policy_path
    return policy_path


def _load_dynamic_fill_hazard_shadow(
    cfg,
) -> tuple[
    Optional[DynamicFillHazardBundle],
    Optional[DynamicFillHazardShadowRuntime],
    Optional[DynamicFillHazardActionPolicy],
]:
    if not bool(
        getattr(
            cfg.strategy,
            "dynamic_fill_hazard_shadow_enabled",
            False,
        )
    ):
        return None, None, None
    model_path = _resolve_dynamic_fill_hazard_shadow_path(cfg)
    if model_path is None:
        raise ValueError("dynamic fill-hazard shadow model path is empty")
    sides = tuple(
        side.strip().upper()
        for side in str(
            getattr(
                cfg.strategy,
                "dynamic_fill_hazard_shadow_sides",
                "BUY",
            )
        ).split(",")
        if side.strip()
    )
    bundle = DynamicFillHazardBundle.load(
        model_path,
        expected_file_sha256=str(
            getattr(
                cfg.strategy,
                "dynamic_fill_hazard_shadow_model_sha256",
                "",
            )
            or ""
        ).strip().lower(),
        shadow_sides=sides,
    )
    action_policy = None
    if bool(
        getattr(
            cfg.strategy,
            "dynamic_fill_hazard_action_enabled",
            False,
        )
    ):
        policy_path = _resolve_dynamic_fill_hazard_action_policy_path(cfg)
        if policy_path is None:
            raise ValueError(
                "dynamic fill-hazard action policy path is empty"
            )
        action_policy = DynamicFillHazardActionPolicy.load(
            policy_path,
            expected_file_sha256=str(
                getattr(
                    cfg.strategy,
                    "dynamic_fill_hazard_action_policy_sha256",
                    "",
                )
                or ""
            ).strip().lower(),
            model_bundle=bundle,
        )
    runtime = DynamicFillHazardShadowRuntime(
        bundle,
        tick_size=float(cfg.tick_size),
        lot_size=float(cfg.lot_size),
        exposure_ms=float(
            getattr(
                cfg.strategy,
                "dynamic_fill_hazard_shadow_exposure_ms",
                100.0,
            )
        ),
        price_jump_ticks=float(
            getattr(
                cfg.strategy,
                "dynamic_fill_hazard_shadow_price_jump_ticks",
                1.0,
            )
        ),
        evaluation_interval_ms=(
            action_policy.evaluation_interval_ms
            if action_policy is not None
            else 0.0
        ),
    )
    logger.warning(
        "Loaded dynamic fill-hazard model: family=%s sides=%s "
        "action_authorized=%d path=%s sha256=%s",
        bundle.family_id,
        ",".join(bundle.shadow_sides),
        int(action_policy is not None),
        model_path,
        bundle.file_sha256,
    )
    if action_policy is not None:
        logger.warning(
            "Loaded BUY adverse-value action policy: policy=%s "
            "threshold=%.12f interval_ms=%.1f validation_rate=%.4f "
            "path=%s sha256=%s",
            action_policy.policy_id,
            action_policy.entry_threshold,
            action_policy.evaluation_interval_ms,
            action_policy.validation_activation_rate,
            action_policy.path,
            action_policy.file_sha256,
        )
    return bundle, runtime, action_policy


def _load_state_conditioned_policy(cfg) -> Optional[StateConditionedQuotePolicy]:
    mode = str(
        getattr(cfg.strategy, "state_conditioned_policy_mode", "disabled")
        or "disabled"
    ).strip().lower()
    if mode == "disabled":
        return None
    model_path = _resolve_state_conditioned_policy_path(cfg)
    if model_path is None:
        raise ValueError(
            "state-conditioned policy mode requires a model artifact path"
        )
    policy = StateConditionedQuotePolicy.load(model_path, mode=mode)
    logger.info(
        "Loaded state-conditioned quote policy: id=%s mode=%s path=%s",
        policy.artifact.policy_id,
        mode,
        model_path,
    )
    return policy


def _get_buy_fill_selection_model(model_path: Optional[Path]):
    """Lazy-load the BUY fill-selection fold ensemble."""
    global _buy_fill_selection_model, _buy_fill_selection_model_path
    if model_path is None:
        return None
    if _buy_fill_selection_model is not None and model_path == _buy_fill_selection_model_path:
        return _buy_fill_selection_model
    try:
        _buy_fill_selection_model = FillSelectionScoreEnsemble.load(model_path)
        _buy_fill_selection_model_path = model_path
        logger.info("Loaded BUY fill-selection model: %s", model_path)
    except Exception as exc:
        _buy_fill_selection_model = None
        _buy_fill_selection_model_path = model_path
        logger.warning("BUY fill-selection model not available: %s", exc)
    return _buy_fill_selection_model


def _reset_lazy_model_caches(reset_buy_fill_selection: bool = False):
    """Reset lazy global caches that do not key themselves by path."""
    global _buy_fill_selection_model, _buy_fill_selection_model_path
    if reset_buy_fill_selection:
        _buy_fill_selection_model = None
        _buy_fill_selection_model_path = None


def _resolve_repo_runtime_path(value: str) -> Path:
    path = Path(str(value).strip()).expanduser()
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[1] / path
    return path.resolve()


def _load_boolean_cooldown_live_policy(cfg) -> Optional[LiveBooleanCooldownPolicy]:
    if not bool(
        getattr(cfg.strategy, "boolean_cooldown_policy_enabled", False)
    ):
        return None
    return LiveBooleanCooldownPolicy.from_files(
        policy_path=_resolve_repo_runtime_path(
            cfg.strategy.boolean_cooldown_policy_path
        ),
        policy_sha256=str(
            cfg.strategy.boolean_cooldown_policy_sha256
        ).strip().lower(),
        predicate_bundle_path=_resolve_repo_runtime_path(
            cfg.strategy.boolean_cooldown_predicate_bundle_path
        ),
        predicate_bundle_sha256=str(
            cfg.strategy.boolean_cooldown_predicate_bundle_sha256
        ).strip().lower(),
        warmup_s=float(cfg.strategy.boolean_cooldown_ema_warmup_s),
        max_feature_age_s=float(cfg.risk.max_exec_book_visible_age_s),
    )


class MakerEngine:
    """
    Event-driven market making engine.

    Usage:
        engine = MakerEngine(cfg, rest_client)
        engine.start()       # starts requote loop
        engine.stop()        # graceful shutdown
    """

    def __init__(self, cfg, rest_client):
        """
        cfg: live.config.Config
        rest_client: binance.um_futures.UMFutures instance
        """
        self.cfg = cfg
        self.rest = rest_client
        self._base_asset, self._quote_asset = _infer_symbol_assets(cfg.symbol)
        self._settlement_asset = self._quote_asset
        self._commission_unit_error: Optional[str] = None
        self._csv_log_lock = threading.Lock()
        self._order_ref_lock = threading.RLock()
        self._order_context_lock = threading.RLock()
        self._order_lifecycle_journal_lock = threading.Lock()
        self._journaled_lifecycle_sequence: dict[str, int] = {}
        self._order_lifecycle_live_writer_v2 = None
        self._order_lifecycle_live_writer_v2_shutdown_timeout_s = 5.0

        model_path = _resolve_model_dir(cfg)
        self._model_dir = model_path
        self._fill_model_quote_cache = None

        # Components
        multi = getattr(cfg, "multi_market", None)
        self.signal = SignalEngine(model_dir=model_path,
                                   enable_ml=cfg.ml.enabled,
                                   rest_client=rest_client,
                                   symbol=cfg.symbol,
                                   reference_symbol=getattr(multi, "reference_symbol", None),
                                   stablecoin_anchor_symbol=getattr(
                                       multi, "stablecoin_anchor_symbol", "USDCUSDT"
                                   ),
                                   ret_demean_halflife=cfg.ml.ret_demean_halflife,
                                   bad_trade_log_every=cfg.logging.bad_trade_log_every)
        self._boolean_cooldown_policy = _load_boolean_cooldown_live_policy(cfg)
        if self._boolean_cooldown_policy is not None:
            self.signal.add_depth_observer(
                self._boolean_cooldown_policy.observe_depth
            )
        self.orders = OrderManager(
            on_fill=self._on_fill,
            on_cancel=self._on_cancel,
            on_terminal=self._on_order_terminal,
            on_lifecycle_event=self._on_order_lifecycle_event,
        )
        self._exact_opportunity_tape_runtime: Optional[
            ExactOpportunityDailyWriter
        ] = None
        self.inventory = InventoryManager(
            max_inventory=cfg.strategy.max_inventory,
            position_timeout=cfg.strategy.position_timeout,
            trade_log_path=cfg.logging.trade_log,
        )
        self._post_fill_quote_response = PostFillQuoteResponse(
            PostFillQuoteResponseConfig.from_params(vars(cfg.strategy))
        )
        self._state_conditioned_policy = _load_state_conditioned_policy(cfg)
        (
            self._dynamic_fill_hazard_shadow_bundle,
            self._dynamic_fill_hazard_shadow_runtime,
            self._dynamic_fill_hazard_action_policy,
        ) = _load_dynamic_fill_hazard_shadow(cfg)
        self._state_conditioned_policy_campaigns: set[int] = set()
        self._ws_handler = None
        self._order_policy_context: Dict[str, dict] = {}
        self._last_quote_context: Dict[str, dict[str, Any]] = {}
        self._last_quote_diagnostics: Dict[str, Any] = {}
        self._last_quote_decision_snapshot: Optional[QuoteDecisionSnapshot] = None
        self._last_post_only_guard: Optional[QuotePostOnlyGuard] = None
        self._consecutive_quote_snapshot_blocks = 0

        # State
        self._running = False
        self._last_requote_time = 0.0
        self._cooldown_until = 0.0
        self._requote_count = 0

        # Dynamic RQ state
        self._ema_var_fast = 0.0   # EMA of squared 1s returns (10s half-life)
        self._ema_var_slow = 0.0   # EMA of squared 1s returns (60s half-life)
        self._prev_close = 0.0
        self._dynamic_rq_ready = False

        # P1: BER (Book Exhaustion Rate) state
        self._ber_ema_fast = 0.0   # fast EMA of trade intensity (~8s half-life)
        self._ber_ema_slow = 0.0   # slow EMA of trade intensity (~60s half-life)
        self._ber_ready = False
        self._ber_active = False

        # v1.2: Markout tracking state
        self._mo_ema_bid = 0.0
        self._mo_ema_ask = 0.0
        self._mo_ema_all = 0.0
        self._mo_pending = []      # list of (fill_time_s, fill_price, side)
        self._mo_ref = 50.0        # reference markout for tanh normalization
        self._mo_last_decay_time = time.time()
        self._mo_pause_until = {"BUY": 0.0, "SELL": 0.0}
        self._mo_last_pause_log = {"BUY": 0.0, "SELL": 0.0}

        # Register bar callback for dynamic RQ
        self.signal._on_bar_callbacks.append(self.update_dynamic_rq)
        # Register bar callback for P1 BER tracking
        self.signal._on_bar_callbacks.append(self._update_ber)

        # Current quotes
        self._bid_cid: Optional[str] = None
        self._ask_cid: Optional[str] = None

        # GTX rejection counter for closing orders
        self._close_gtx_rejects: int = 0
        # Track when closing state started (for stale-time escalation)
        self._close_start_time: float = 0.0

        # Best bid/ask from bookTicker (for Post Only guard)
        self._best_bid: float = 0.0
        self._best_ask: float = 0.0

        # Step 27: Fill cooldown — prevent same-side consecutive accumulation
        self._fill_cooldown_until = {"BUY": 0.0, "SELL": 0.0}
        self._consec_buy: float = 0.0
        self._consec_sell: float = 0.0
        self._last_same_side_fill_epoch_ms = {"BUY": 0, "SELL": 0}
        self._last_fill_side: str = ""
        self._last_cooldown_cancel_time: float = 0.0
        self._last_stale_data_block_log: float = 0.0
        self._last_quote_snapshot_block_log: float = 0.0
        self._last_prediction: Optional[Prediction] = None
        self._flat_unilateral_started = {"BUY": 0.0, "SELL": 0.0, "BOTH": 0.0}
        self._last_flat_unilateral_release_log = {"BUY": 0.0, "SELL": 0.0, "BOTH": 0.0}
        self._last_seen_sync_adjust_seq: int = 0
        self._sync_adjust_degrade_until: float = 0.0
        self._last_sync_adjust_degrade_log: float = 0.0
        self._last_sync_adjust_user_reconnect: float = 0.0
        self._buy_fill_selection_eval_count: int = 0
        self._buy_fill_selection_hit_count: int = 0
        self._buy_fill_selection_action_count: int = 0
        self._buy_fill_selection_last_hit_time: float = 0.0
        self._buy_fill_selection_last_eval_time: float = 0.0
        self._buy_fill_selection_last_score: float = 0.0
        self._buy_fill_selection_last_missing: int = 0
        self._dynamic_fill_hazard_shadow_rows: int = 0
        self._dynamic_fill_hazard_shadow_valid_rows: int = 0
        self._dynamic_fill_hazard_shadow_invalid_rows: int = 0
        self._dynamic_fill_hazard_shadow_last_time: float = 0.0
        self._dynamic_fill_hazard_shadow_last_favorable: float = math.nan
        self._dynamic_fill_hazard_shadow_last_adverse: float = math.nan
        self._cross_venue_fair_price_shadow_rows: int = 0
        self._cross_venue_fair_price_shadow_valid_rows: int = 0
        self._cross_venue_fair_price_shadow_last_time: float = 0.0
        self._cross_venue_fair_price_shadow_last_warning: float = 0.0
        self._dynamic_fill_hazard_action_hold: Optional[
            _BuyHazardCancelHold
        ] = None
        self._dynamic_fill_hazard_action_lock = threading.RLock()
        self._dynamic_fill_hazard_action_cancel_count: int = 0
        self._dynamic_fill_hazard_action_reentry_count: int = 0
        self._dynamic_fill_hazard_action_keep_count: int = 0
        self._dynamic_fill_hazard_action_invalid_hold_count: int = 0
        self._dynamic_fill_hazard_action_last_score: float = math.nan
        self._replace_throttle_counts = {"BUY": 0, "SELL": 0}
        self._last_replace_throttle_log = {"BUY": 0.0, "SELL": 0.0}
        self._replace_pending_coalesce_counts = {"BUY": 0, "SELL": 0}
        self._last_replace_pending_coalesce_log = {"BUY": 0.0, "SELL": 0.0}
        self._replace_cancel_first_counts = {"BUY": 0, "SELL": 0}
        self._last_replace_cancel_first_log = {"BUY": 0.0, "SELL": 0.0}

        # Exchange filter (updated in start() via exchange_info)
        self._min_qty: float = cfg.lot_size
        self._qty_precision: int = self._precision_from_step(cfg.lot_size)
        self._price_precision: int = self._precision_from_step(cfg.tick_size)

        self._quote_log_path = self._resolve_quote_log_path(cfg)
        self._order_outcome_log_path = self._resolve_order_outcome_log_path(cfg)
        self._buy_fill_selection_shadow_log_path = self._resolve_buy_fill_selection_shadow_log_path(cfg)
        self._dynamic_fill_hazard_shadow_log_path = (
            self._resolve_dynamic_fill_hazard_shadow_log_path(cfg)
        )
        self._dynamic_fill_hazard_action_log_path = (
            self._resolve_dynamic_fill_hazard_action_log_path(cfg)
        )
        self._state_conditioned_policy_shadow_log_path = (
            self._resolve_state_conditioned_policy_shadow_log_path(cfg)
        )
        self._cross_venue_fair_price_shadow_log_path = (
            self._resolve_cross_venue_fair_price_shadow_log_path(cfg)
        )
        self._exact_opportunity_tape_path = (
            self._resolve_exact_opportunity_tape_path(cfg)
        )
        self._configure_exact_opportunity_tape_runtime(cfg)
        self._order_lifecycle_journal_path = (
            self._resolve_order_lifecycle_journal_path(cfg)
        )
        self._inventory_campaign_shadow_log_path = (
            self._resolve_inventory_campaign_shadow_log_path(cfg)
        )
        self._live_perf_telemetry_log_path = self._resolve_live_perf_telemetry_log_path(cfg)
        self._quote_snapshot_integrity_log_path = (
            self._resolve_quote_snapshot_integrity_log_path(cfg)
        )
        self._perf_rest_new_count = 0
        self._perf_rest_new_sum_us = 0.0
        self._perf_rest_new_max_us = 0.0
        self._perf_rest_cancel_count = 0
        self._perf_rest_cancel_sum_us = 0.0
        self._perf_rest_cancel_max_us = 0.0
        self._perf_rest_cancel_all_count = 0
        self._perf_rest_cancel_all_sum_us = 0.0
        self._perf_rest_cancel_all_max_us = 0.0
        self._last_bid_action = "none"
        self._last_ask_action = "none"
        self._last_cpp_routing_used = 0
        self._init_csv_log(
            self._quote_log_path,
            list(QuoteDecisionLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._order_outcome_log_path,
            list(OrderOutcomeLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._buy_fill_selection_shadow_log_path,
            list(BuyFillSelectionShadowLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._dynamic_fill_hazard_shadow_log_path,
            list(DynamicFillHazardShadowLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._dynamic_fill_hazard_action_log_path,
            list(DynamicFillHazardActionLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._state_conditioned_policy_shadow_log_path,
            list(StateConditionedPolicyShadowLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._cross_venue_fair_price_shadow_log_path,
            list(CrossVenueFairPriceShadowLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            "" if self._exact_opportunity_tape_runtime is not None else self._exact_opportunity_tape_path,
            list(ExactQuoteOpportunityTapeRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._order_lifecycle_journal_path,
            list(OrderLifecycleJournalRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._inventory_campaign_shadow_log_path,
            list(InventoryCampaignShadowLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._live_perf_telemetry_log_path,
            list(LivePerfTelemetryLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._quote_snapshot_integrity_log_path,
            list(QuoteSnapshotIntegrityLogRow.__dataclass_fields__.keys()),
        )
        logger.info(
            "Engine config: "
            f"symbol={cfg.symbol}, fill_cooldown={cfg.strategy.fill_cooldown}, "
            f"model_dir={model_path}, ml={cfg.ml.enabled}, "
            f"multi_market={getattr(multi, 'enabled', False)}, "
            f"stage={getattr(multi, 'market_stage', 'minimal')}, "
            f"reference={self.signal._reference_symbol}, "
            f"quote_log={self._quote_log_path}, "
            f"order_outcome_log={self._order_outcome_log_path}, "
            f"buy_fill_selection_shadow_log={self._buy_fill_selection_shadow_log_path}, "
            f"dynamic_fill_hazard_shadow_log={self._dynamic_fill_hazard_shadow_log_path}, "
            f"dynamic_fill_hazard_action_log={self._dynamic_fill_hazard_action_log_path}, "
            f"state_conditioned_policy_shadow_log={self._state_conditioned_policy_shadow_log_path}, "
            f"cross_venue_fair_price_shadow_log={self._cross_venue_fair_price_shadow_log_path}, "
            f"exact_opportunity_tape={self._exact_opportunity_tape_path or '<disabled>'}, "
            f"order_lifecycle_journal={self._order_lifecycle_journal_path}, "
            f"inventory_campaign_shadow_log={self._inventory_campaign_shadow_log_path}, "
            f"live_perf_telemetry_log={self._live_perf_telemetry_log_path}, "
            f"quote_snapshot_integrity_log={self._quote_snapshot_integrity_log_path}"
        )

    def set_ws_handler(self, ws_handler):
        """Register WS handler so config reload can update stream settings."""
        self._ws_handler = ws_handler

    def set_order_lifecycle_live_writer_v2(
        self,
        writer,
        *,
        shutdown_drain_timeout_s: float,
    ) -> None:
        """Attach the restart-only async journal after its startup preflight."""

        if self._order_lifecycle_live_writer_v2 is not None:
            raise RuntimeError("order lifecycle live writer v2 is already attached")
        self._order_lifecycle_live_writer_v2 = writer
        self._order_lifecycle_live_writer_v2_shutdown_timeout_s = max(
            0.0,
            float(shutdown_drain_timeout_s),
        )

    def order_lifecycle_live_writer_v2_health_snapshot(self) -> dict[str, Any]:
        runtime = self._order_lifecycle_live_writer_v2
        if runtime is None:
            return {"enabled": False, "state": "disabled"}
        return {"enabled": True, **runtime.health_snapshot()}

    def prospective_epoch_initial_runtime_state(
        self,
        *,
        account_snapshot: Optional[dict[str, Any]] = None,
        exchange_open_orders: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        """Capture every supported state that can affect the next decision."""

        capture_started_ns = time.time_ns()
        unsupported: list[str] = []
        active_orders = []
        for order in self.orders.get_active_orders():
            lifecycle = getattr(order, "lifecycle", None)
            active_orders.append(
                {
                    "client_order_id": str(order.client_order_id),
                    "exchange_order_id": int(getattr(order, "order_id", 0) or 0),
                    "side": str(order.side.value),
                    "price": float(order.price),
                    "quantity": float(order.quantity),
                    "remaining_quantity": float(order.remaining_qty),
                    "state": str(order.state.name),
                    "lifecycle": lifecycle.snapshot() if lifecycle is not None else None,
                }
            )
        with self.inventory._lock:
            inventory_accounting = {
                "quantity_btc": float(self.inventory._qty),
                "average_entry_price": float(self.inventory._avg_entry),
                "cost_basis": float(self.inventory._cost_basis),
                "realized_pnl": float(self.inventory._realized_pnl),
                "unrealized_pnl": float(self.inventory._unrealized_pnl),
                "position_state": str(self.inventory._state.name),
                "open_time": float(self.inventory._open_time),
                "mark_price": float(self.inventory._mark_price),
                "total_traded_volume": float(self.inventory._total_volume),
                "total_commission": float(self.inventory._total_commission),
                "open_commission": float(self.inventory._open_commission),
                "round_trip_realized_pnl": float(self.inventory._round_trip_rpnl),
                "last_trade_pnl": float(self.inventory._last_trade_pnl),
                "peak_pnl": float(self.inventory._peak_pnl),
                "peak_unrealized_pnl": float(self.inventory._peak_unrealized_pnl),
                "daily_utc_day": int(self.inventory._daily_utc_day),
                "day_start_total_pnl": float(self.inventory._day_start_total_pnl),
                "day_buy_fill_qty": float(self.inventory._day_buy_fill_qty),
                "day_sell_fill_qty": float(self.inventory._day_sell_fill_qty),
                "day_buy_fill_notional": float(self.inventory._day_buy_fill_notional),
                "day_sell_fill_notional": float(self.inventory._day_sell_fill_notional),
                "last_sync_request_time": float(self.inventory._last_sync_request_time),
                "sync_adjust_seq": int(self.inventory._sync_adjust_seq),
                "last_sync_adjust_time": float(self.inventory._last_sync_adjust_time),
                "last_sync_adjust_delta": float(self.inventory._last_sync_adjust_delta),
                "sync_adjust_events": [
                    [float(ts), float(delta)]
                    for ts, delta in self.inventory._sync_adjust_events
                ],
                "inventory_time_start_ts": float(self.inventory._inventory_time_start_ts),
                "inventory_time_last_ts": float(self.inventory._inventory_time_last_ts),
                "signed_inventory_time_s": float(self.inventory._signed_inventory_time_s),
                "abs_inventory_time_s": float(self.inventory._abs_inventory_time_s),
                "sq_inventory_time_s": float(self.inventory._sq_inventory_time_s),
                "signed_notional_inventory_time_s": float(
                    self.inventory._signed_notional_inventory_time_s
                ),
                "notional_inventory_time_s": float(
                    self.inventory._notional_inventory_time_s
                ),
            }
            campaign = {
                "active": bool(self.inventory._campaign_active),
                "campaign_id": int(self.inventory._campaign_id),
                "start_time": float(self.inventory._campaign_start_time),
                "start_realized_pnl": float(
                    self.inventory._campaign_start_realized_pnl
                ),
                "start_side": str(self.inventory._campaign_start_side),
                "max_abs_qty": float(self.inventory._campaign_max_abs_qty),
                "min_total_pnl": float(self.inventory._campaign_min_total_pnl),
                "total_pnl": float(self.inventory._campaign_total_pnl),
                "realized_pnl": float(self.inventory._campaign_realized_pnl),
                "unrealized_pnl": float(self.inventory._campaign_unrealized_pnl),
                "fills": int(self.inventory._campaign_fills),
                "buy_fills": int(self.inventory._campaign_buy_fills),
                "sell_fills": int(self.inventory._campaign_sell_fills),
                "exposure_increasing_fills": int(
                    self.inventory._campaign_exposure_increasing_fills
                ),
                "reducing_fills": int(self.inventory._campaign_reducing_fills),
                "volume": float(self.inventory._campaign_volume),
            }
            consecutive_losses = int(self.inventory._consecutive_losses)
        hold = getattr(self, "_dynamic_fill_hazard_action_hold", None)
        hold_state, _ = _prospective_state_fingerprint(
            hold,
            path="q90_runtime.action_hold",
            unsupported=unsupported,
        )
        hazard_orders, hazard_orders_sha256 = _prospective_state_fingerprint(
            getattr(self._dynamic_fill_hazard_shadow_runtime, "_orders", {}),
            path="q90_runtime.shadow_orders",
            unsupported=unsupported,
        )
        with self.signal._lock:
            bar_rows = list(self.signal._bar_buffer)
            feature_rows = list(self.signal._feat_history)
            last_emitted_bucket_ms = int(self.signal._last_processed_bucket or 0)
            cpp_feature_engine_seeded = bool(
                self.signal._cpp_feature_engine_seeded
            )
            cpp_feature_engine_present = bool(
                self.signal._cpp_feature_engine is not None
            )
            global_flow_native_enabled = bool(
                self.signal._global_flow.native_enabled
            )
            cpp_cross_aggregator_count = len(
                self.signal._cpp_cross_aggregators
            )
            expected_cpp_bar_count = len(bar_rows)
            expected_cpp_history_count = len(feature_rows)
            actual_cpp_bar_count = 0
            actual_cpp_history_count = 0
            if self.signal._cpp_feature_engine is not None:
                if not all(
                    hasattr(self.signal._cpp_feature_engine, name)
                    for name in ("bar_count", "history_count")
                ):
                    unsupported.append(
                        "signal.cpp_feature_engine_count_introspection"
                    )
                else:
                    actual_cpp_bar_count = int(
                        self.signal._cpp_feature_engine.bar_count()
                    )
                    actual_cpp_history_count = int(
                        self.signal._cpp_feature_engine.history_count()
                    )
                    if not cpp_feature_engine_seeded:
                        unsupported.append(
                            "signal.cpp_feature_engine_not_seeded"
                        )
                    if actual_cpp_bar_count != expected_cpp_bar_count:
                        unsupported.append(
                            "signal.cpp_feature_engine_bar_count_mismatch"
                        )
                    if actual_cpp_history_count != expected_cpp_history_count:
                        unsupported.append(
                            "signal.cpp_feature_engine_history_count_mismatch"
                        )
            global_flow_stats = self.signal._global_flow.backend_stats()
            native_boundary_event_fields = (
                "market_count",
                "book_events_seen",
                "book_events_accepted",
                "trade_batches",
                "trade_events_seen",
                "trade_events_accepted",
                "out_of_order_events",
                "stale_trade_events",
                "book_overflow_events",
                "trade_overflow_events",
            )
            native_boundary_event_count = sum(
                max(0, int(global_flow_stats.get(field, 0) or 0))
                for field in native_boundary_event_fields
            )
            if native_boundary_event_count != 0:
                unsupported.append("signal.global_flow_nonzero_at_epoch_boundary")
            signal_state = {
                "bar_buffer": bar_rows,
                "current_bar": self.signal._current_bar,
                "current_bucket": self.signal._current_bucket,
                "depth_history": list(self.signal._depth_history),
                "last_depth": self.signal._last_depth,
                "quote_market_generation": self.signal._quote_market_generation,
                "depth_generation": self.signal._depth_generation,
                "book_ticker_generation": self.signal._book_ticker_generation,
                "feature_history": feature_rows,
                "last_processed_bucket": self.signal._last_processed_bucket,
                "close_history": list(self.signal._close_history),
                "sign_history": list(self.signal._sign_history),
                "signed_volume_cumsum": self.signal._signed_vol_cumsum,
                "previous_flow_velocity": self.signal._prev_flow_velocity,
                "last_trade_side": self.signal._last_trade_side,
                "last_trade_run_length": self.signal._last_trade_run_len,
                "metrics_history": list(self.signal._metrics_history),
                "last_metrics": self.signal._last_metrics,
                "book_tickers": self.signal._book_tickers,
                "book_ticker_history": self.signal._book_ticker_history,
                "cross_bar_buffers": self.signal._cross_bar_buffers,
                "cross_current_bars": self.signal._cross_current_bars,
                "cross_current_buckets": self.signal._cross_current_buckets,
                "cross_basis_history": self.signal._cross_basis_history,
                "global_bridge_basis_history": self.signal._global_bridge_basis_history,
                "market_source_state": self.signal._market_source_state,
                "last_prediction": self.signal._last_prediction,
                "warmup_count": self.signal._warmup_count,
                "prediction_return_ema": self.signal._pred_ret_ema,
                "cpp_feature_engine_seeded": self.signal._cpp_feature_engine_seeded,
                "cpp_cross_current_dirty": self.signal._cpp_cross_current_dirty,
                "cross_venue_fair_price_state": {
                    "basis": self.signal._cross_venue_fair_price._basis,
                    "last_source_identity": (
                        self.signal._cross_venue_fair_price._last_source_identity
                    ),
                    "lead": self.signal._cross_venue_fair_price._lead,
                    "noise": self.signal._cross_venue_fair_price._noise,
                    "last_consensus_identity": (
                        self.signal._cross_venue_fair_price._last_consensus_identity
                    ),
                    "last_decision_ts_ns": (
                        self.signal._cross_venue_fair_price._last_decision_ts_ns
                    ),
                },
            }
            if self.signal._global_flow.native_enabled:
                global_flow_state: Any = {
                    "native_enabled": True,
                    "backend_stats": global_flow_stats,
                    "boundary_event_count": native_boundary_event_count,
                }
            else:
                global_flow_state = {
                    "native_enabled": False,
                    "markets": self.signal._global_flow._markets,
                    "backend_stats": global_flow_stats,
                    "boundary_event_count": native_boundary_event_count,
                }
            signal_state["global_flow"] = global_flow_state
            if self.signal._cpp_cross_aggregators:
                unsupported.append("signal.cpp_cross_aggregator_nonzero_at_epoch_boundary")
                signal_state["cpp_cross_aggregator_keys"] = sorted(
                    self.signal._cpp_cross_aggregators
                )
            normalized_signal_state, signal_state_sha256 = (
                _prospective_state_fingerprint(
                    signal_state,
                    path="signal_feature_dag_warmup",
                    unsupported=unsupported,
                )
            )
        quote_context, quote_context_sha256 = _prospective_state_fingerprint(
            {
                "last_quote_context": self._last_quote_context,
                "last_quote_diagnostics": self._last_quote_diagnostics,
                "last_quote_decision_snapshot": self._last_quote_decision_snapshot,
                "last_post_only_guard": self._last_post_only_guard,
                "last_prediction": self._last_prediction,
            },
            path="defense_and_stale_guards.quote_context",
            unsupported=unsupported,
        )
        post_fill_state, _ = _prospective_state_fingerprint(
            {
                "add_side": self._post_fill_quote_response._add_side,
                "excitation": self._post_fill_quote_response._excitation,
                "last_update_ms": self._post_fill_quote_response._last_update_ms,
                "last_half_life_s": self._post_fill_quote_response._last_half_life_s,
            },
            path="post_fill_response",
            unsupported=unsupported,
        )
        unsupported = sorted(set(unsupported))
        captured_domains = list(PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS)
        binding_status = "fully_bound" if not unsupported else "unsupported"
        first_feature_bucket_ms = (
            last_emitted_bucket_ms - (len(feature_rows) - 1) * 10_000
            if feature_rows and last_emitted_bucket_ms > 0
            else 0
        )
        first_bar_ts_ms = int(bar_rows[0].ts) if bar_rows else 0
        last_bar_ts_ms = int(bar_rows[-1].ts) if bar_rows else 0
        return {
            "schema_version": "narrowgate_live_initial_runtime_state.v2",
            "symbol": str(self.cfg.symbol),
            "capture_started_ts_ns": capture_started_ns,
            "capture_completed_ts_ns": time.time_ns(),
            "account_and_exchange": {
                "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[
                    "account_and_exchange"
                ],
                "account": dict(account_snapshot or {}),
                "exchange_open_orders": list(exchange_open_orders or []),
            },
            "inventory_accounting": {
                "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[
                    "inventory_accounting"
                ],
                **inventory_accounting,
            },
            "campaign": {
                "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[
                    "campaign"
                ],
                **campaign,
            },
            "reward_path_loss_cooldown": {
                "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[
                    "reward_path_loss_cooldown"
                ],
                "consecutive_losses": consecutive_losses,
                "cooldown_until_wall_s": float(self._cooldown_until),
                "last_cooldown_cancel_time_wall_s": float(
                    self._last_cooldown_cancel_time
                ),
            },
            "adverse_markout_pause": {
                "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[
                    "adverse_markout_pause"
                ],
                "ema_bid": float(self._mo_ema_bid),
                "ema_ask": float(self._mo_ema_ask),
                "ema_all": float(self._mo_ema_all),
                "pending": [list(item) for item in self._mo_pending],
                "reference": float(self._mo_ref),
                "last_decay_time_wall_s": float(self._mo_last_decay_time),
                "pause_until_wall_s": {
                    side: float(value) for side, value in self._mo_pause_until.items()
                },
            },
            "sync_degrade": {
                "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[
                    "sync_degrade"
                ],
                "last_seen_sync_adjust_seq": int(self._last_seen_sync_adjust_seq),
                "degrade_until_wall_s": float(self._sync_adjust_degrade_until),
                "last_user_reconnect_wall_s": float(
                    self._last_sync_adjust_user_reconnect
                ),
            },
            "defense_and_stale_guards": {
                "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[
                    "defense_and_stale_guards"
                ],
                "consecutive_quote_snapshot_blocks": int(
                    self._consecutive_quote_snapshot_blocks
                ),
                "flat_unilateral_started_wall_s": dict(
                    self._flat_unilateral_started
                ),
                "best_bid": float(self._best_bid),
                "best_ask": float(self._best_ask),
                "quote_context_sha256": quote_context_sha256,
                "quote_context": quote_context,
            },
            "fill_cooldown_lineage": {
                "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[
                    "fill_cooldown_lineage"
                ],
                "same_side_fill_units": {
                    "BUY": float(self._consec_buy),
                    "SELL": float(self._consec_sell),
                },
                "fill_cooldown_until_wall_s": {
                    "BUY": float(self._fill_cooldown_until.get("BUY", 0.0)),
                    "SELL": float(self._fill_cooldown_until.get("SELL", 0.0)),
                },
                "last_same_side_fill_epoch_ms": dict(
                    self._last_same_side_fill_epoch_ms
                ),
                "last_fill_side": str(self._last_fill_side or ""),
            },
            "order_lifecycle": {
                "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[
                    "order_lifecycle"
                ],
                "active_local_orders": active_orders,
                "bid_client_order_id": self._bid_cid,
                "ask_client_order_id": self._ask_cid,
                "order_policy_context_count": len(self._order_policy_context),
            },
            "q90_runtime": {
                "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[
                    "q90_runtime"
                ],
                "action_hold": hold_state,
                "shadow_order_count": len(hazard_orders),
                "shadow_orders_sha256": hazard_orders_sha256,
                "shadow_orders": hazard_orders,
                "last_action_score": _prospective_state_plain(
                    float(self._dynamic_fill_hazard_action_last_score),
                    path="q90_runtime.last_action_score",
                    unsupported=unsupported,
                ),
            },
            "post_fill_response": {
                "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[
                    "post_fill_response"
                ],
                **post_fill_state,
            },
            "quote_policy_clocks": {
                "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[
                    "quote_policy_clocks"
                ],
                "running": bool(self._running),
                "last_requote_time_wall_s": float(self._last_requote_time),
                "requote_count": int(self._requote_count),
                "dynamic_rq": {
                    "ema_var_fast": float(self._ema_var_fast),
                    "ema_var_slow": float(self._ema_var_slow),
                    "previous_close": float(self._prev_close),
                    "ready": bool(self._dynamic_rq_ready),
                },
                "book_exhaustion": {
                    "ema_fast": float(self._ber_ema_fast),
                    "ema_slow": float(self._ber_ema_slow),
                    "ready": bool(self._ber_ready),
                    "active": bool(self._ber_active),
                },
                "closing": {
                    "gtx_rejects": int(self._close_gtx_rejects),
                    "start_time_wall_s": float(self._close_start_time),
                },
                "state_conditioned_policy_campaigns": sorted(
                    self._state_conditioned_policy_campaigns
                ),
                "buy_fill_selection": {
                    "last_eval_time_wall_s": float(
                        self._buy_fill_selection_last_eval_time
                    ),
                    "last_hit_time_wall_s": float(
                        self._buy_fill_selection_last_hit_time
                    ),
                    "last_score": float(self._buy_fill_selection_last_score),
                    "last_missing": int(self._buy_fill_selection_last_missing),
                },
            },
            "signal_feature_dag_warmup": {
                "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[
                    "signal_feature_dag_warmup"
                ],
                "feature_dag_sha256": str(TEN_SECOND_CAUSAL_GRAPH.sha256()),
                "causal_cutoff_exclusive_ms": (
                    last_emitted_bucket_ms + 10_000
                    if last_emitted_bucket_ms > 0
                    else 0
                ),
                "last_emitted_bucket_ms": last_emitted_bucket_ms,
                "bar_history_coverage": {
                    "row_count": len(bar_rows),
                    "first_ts_ms": first_bar_ts_ms,
                    "last_ts_ms": last_bar_ts_ms,
                },
                "feature_history_coverage": {
                    "row_count": len(feature_rows),
                    "first_bucket_ms": first_feature_bucket_ms,
                    "last_bucket_ms": last_emitted_bucket_ms,
                },
                "state_sha256": signal_state_sha256,
                "cpp_engine_seeded": cpp_feature_engine_seeded,
                "cpp_backend_state": {
                    "feature_engine_present": cpp_feature_engine_present,
                    "reconstruction_contract": (
                        CPP_FEATURE_RECONSTRUCTION_CONTRACT
                        if cpp_feature_engine_present
                        else PYTHON_FEATURE_STATE_CONTRACT
                    ),
                    "expected_bar_count": expected_cpp_bar_count,
                    "actual_bar_count": actual_cpp_bar_count,
                    "expected_history_count": expected_cpp_history_count,
                    "actual_history_count": actual_cpp_history_count,
                    "global_flow_native_enabled": global_flow_native_enabled,
                    "global_flow_boundary_event_count": (
                        native_boundary_event_count
                    ),
                    "cross_aggregator_count": cpp_cross_aggregator_count,
                },
                "state": normalized_signal_state,
            },
            "completeness": {
                "schema_version": (
                    PROSPECTIVE_INITIAL_STATE_COMPLETENESS_SCHEMA_VERSION
                ),
                "required_domains": list(
                    PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS
                ),
                "captured_domains": captured_domains,
                "unsupported_initial_state_fields": unsupported,
                "binding_status": binding_status,
            },
        }

    def on_config_reload(self, cfg):
        """Apply runtime config and propagate changes to signal + ws handler."""
        if cfg.lifecycle_journal_v2 != self.cfg.lifecycle_journal_v2:
            raise ValueError(
                "lifecycle_journal_v2 configuration is restart-only and cannot be hot-reloaded"
            )
        from live.runtime_policy import require_f05_boolean_cooldown_restart

        require_f05_boolean_cooldown_restart(
            vars(self.cfg.strategy),
            vars(cfg.strategy),
        )
        exact_validation = validate_exact_opportunity_runtime_config(cfg)
        if bool(exact_validation["enabled"]):
            build_exact_opportunity_runtime_identity(
                cfg,
                repo_root=Path(__file__).resolve().parents[1],
            )
        old_cfg = self.cfg
        old_model_dir = self._model_dir
        self.cfg = cfg
        self._quote_core_config_cache = None
        self._post_fill_quote_response = PostFillQuoteResponse(
            PostFillQuoteResponseConfig.from_params(vars(cfg.strategy))
        )
        previous_state_policy = self._state_conditioned_policy
        self._state_conditioned_policy = _load_state_conditioned_policy(cfg)
        previous_hazard_action_policy = (
            self._dynamic_fill_hazard_action_policy
        )
        (
            self._dynamic_fill_hazard_shadow_bundle,
            self._dynamic_fill_hazard_shadow_runtime,
            self._dynamic_fill_hazard_action_policy,
        ) = _load_dynamic_fill_hazard_shadow(cfg)
        previous_hazard_identity = (
            previous_hazard_action_policy.policy_id,
            previous_hazard_action_policy.file_sha256,
        ) if previous_hazard_action_policy is not None else None
        current_hazard_identity = (
            self._dynamic_fill_hazard_action_policy.policy_id,
            self._dynamic_fill_hazard_action_policy.file_sha256,
        ) if self._dynamic_fill_hazard_action_policy is not None else None
        if current_hazard_identity != previous_hazard_identity:
            self._release_dynamic_fill_hazard_action_hold(
                event="config_reload",
                force_requote=False,
            )
        previous_identity = (
            previous_state_policy.artifact.policy_id,
            previous_state_policy.mode,
        ) if previous_state_policy is not None else None
        current_identity = (
            self._state_conditioned_policy.artifact.policy_id,
            self._state_conditioned_policy.mode,
        ) if self._state_conditioned_policy is not None else None
        if current_identity != previous_identity:
            self._state_conditioned_policy_campaigns.clear()
        self._model_dir = _resolve_model_dir(cfg)
        model_dir_changed = self._model_dir != old_model_dir
        if model_dir_changed:
            _reset_lazy_model_caches(reset_buy_fill_selection=True)
            self._fill_model_quote_cache = None

        # InventoryManager keeps these as local fields.
        self.inventory.max_inventory = cfg.strategy.max_inventory
        self.inventory.position_timeout = cfg.strategy.position_timeout

        # Keep local precision/filters in sync with reloaded config.
        self._min_qty = cfg.lot_size
        self._qty_precision = self._precision_from_step(cfg.lot_size)
        self._price_precision = self._precision_from_step(cfg.tick_size)

        # SignalEngine caches ML enable + ret demeaning settings.
        prev_ml_enabled = self.signal._enable_ml
        multi = getattr(cfg, "multi_market", None)
        self.signal._symbol = normalize_symbol(cfg.symbol)
        self.signal._reference_symbol = normalize_symbol(
            getattr(multi, "reference_symbol", None),
            default_reference_symbol(cfg.symbol),
        )
        self.signal._ret_demean_halflife = cfg.ml.ret_demean_halflife
        self.signal._bad_trade_log_every = max(1, int(cfg.logging.bad_trade_log_every))
        self.signal.set_model_dir(self._model_dir)

        if cfg.ml.enabled:
            self.signal._enable_ml = True
            if (not prev_ml_enabled) or model_dir_changed or not self.signal._models:
                self.signal.reload_models()
                if not prev_ml_enabled:
                    reason = "enabled"
                elif model_dir_changed:
                    reason = "model_dir changed"
                else:
                    reason = "empty model cache"
                logger.info(f"Config reload: ML models loaded ({reason})")
        elif not cfg.ml.enabled and prev_ml_enabled:
            self.signal._enable_ml = False
            logger.info("Config reload: ML disabled")

        if self._ws_handler is not None and hasattr(self._ws_handler, "on_config_reload"):
            self._ws_handler.on_config_reload(old_cfg, cfg)

        logger.info(
            "Config applied: "
            f"max_inv={cfg.strategy.max_inventory}, "
            f"rq={cfg.strategy.requote_interval}, "
            f"fill_cooldown={cfg.strategy.fill_cooldown}, "
            f"model_dir={self._model_dir}, "
            f"ml={cfg.ml.enabled}, "
            f"multi_market={getattr(multi, 'enabled', False)}, "
            f"stage={getattr(multi, 'market_stage', 'minimal')}, "
            f"reference={self.signal._reference_symbol}, "
            f"bad_trade_log_every={self.signal._bad_trade_log_every}"
        )

        self._quote_log_path = self._resolve_quote_log_path(cfg)
        self._order_outcome_log_path = self._resolve_order_outcome_log_path(cfg)
        self._buy_fill_selection_shadow_log_path = self._resolve_buy_fill_selection_shadow_log_path(cfg)
        self._dynamic_fill_hazard_shadow_log_path = (
            self._resolve_dynamic_fill_hazard_shadow_log_path(cfg)
        )
        self._dynamic_fill_hazard_action_log_path = (
            self._resolve_dynamic_fill_hazard_action_log_path(cfg)
        )
        self._state_conditioned_policy_shadow_log_path = (
            self._resolve_state_conditioned_policy_shadow_log_path(cfg)
        )
        self._cross_venue_fair_price_shadow_log_path = (
            self._resolve_cross_venue_fair_price_shadow_log_path(cfg)
        )
        self._exact_opportunity_tape_path = (
            self._resolve_exact_opportunity_tape_path(cfg)
        )
        self._configure_exact_opportunity_tape_runtime(cfg)
        self._order_lifecycle_journal_path = (
            self._resolve_order_lifecycle_journal_path(cfg)
        )
        self._inventory_campaign_shadow_log_path = (
            self._resolve_inventory_campaign_shadow_log_path(cfg)
        )
        self._live_perf_telemetry_log_path = self._resolve_live_perf_telemetry_log_path(cfg)
        self._quote_snapshot_integrity_log_path = (
            self._resolve_quote_snapshot_integrity_log_path(cfg)
        )
        self._init_csv_log(
            self._quote_log_path,
            list(QuoteDecisionLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._order_outcome_log_path,
            list(OrderOutcomeLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._buy_fill_selection_shadow_log_path,
            list(BuyFillSelectionShadowLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._dynamic_fill_hazard_shadow_log_path,
            list(DynamicFillHazardShadowLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._dynamic_fill_hazard_action_log_path,
            list(DynamicFillHazardActionLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._state_conditioned_policy_shadow_log_path,
            list(StateConditionedPolicyShadowLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._cross_venue_fair_price_shadow_log_path,
            list(CrossVenueFairPriceShadowLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            "" if self._exact_opportunity_tape_runtime is not None else self._exact_opportunity_tape_path,
            list(ExactQuoteOpportunityTapeRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._order_lifecycle_journal_path,
            list(OrderLifecycleJournalRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._inventory_campaign_shadow_log_path,
            list(InventoryCampaignShadowLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._live_perf_telemetry_log_path,
            list(LivePerfTelemetryLogRow.__dataclass_fields__.keys()),
        )
        self._init_csv_log(
            self._quote_snapshot_integrity_log_path,
            list(QuoteSnapshotIntegrityLogRow.__dataclass_fields__.keys()),
        )

    @staticmethod
    def _resolve_quote_log_path(cfg) -> str:
        path = getattr(cfg.logging, "quote_log", "") or ""
        if path:
            return path
        trade_log = getattr(cfg.logging, "trade_log", "") or ""
        if trade_log:
            return str(Path(trade_log).with_name("quote_decisions.csv"))
        return "quote_decisions.csv"

    @staticmethod
    def _resolve_order_outcome_log_path(cfg) -> str:
        path = getattr(cfg.logging, "order_outcome_log", "") or ""
        if path:
            return path
        trade_log = getattr(cfg.logging, "trade_log", "") or ""
        if trade_log:
            return str(Path(trade_log).with_name("order_outcomes.csv"))
        return "order_outcomes.csv"

    @staticmethod
    def _resolve_buy_fill_selection_shadow_log_path(cfg) -> str:
        path = getattr(cfg.logging, "buy_fill_selection_shadow_log", "") or ""
        if path:
            return path
        trade_log = getattr(cfg.logging, "trade_log", "") or ""
        if trade_log:
            return str(Path(trade_log).with_name("buy_fill_selection_shadow.csv"))
        return "buy_fill_selection_shadow.csv"

    @staticmethod
    def _resolve_dynamic_fill_hazard_shadow_log_path(cfg) -> str:
        path = (
            getattr(
                cfg.logging,
                "dynamic_fill_hazard_shadow_log",
                "",
            )
            or ""
        )
        if path:
            return path
        trade_log = getattr(cfg.logging, "trade_log", "") or ""
        if trade_log:
            return str(
                Path(trade_log).with_name(
                    "dynamic_fill_hazard_shadow.csv"
                )
            )
        return "dynamic_fill_hazard_shadow.csv"

    @staticmethod
    def _resolve_dynamic_fill_hazard_action_log_path(cfg) -> str:
        path = (
            getattr(
                cfg.logging,
                "dynamic_fill_hazard_action_log",
                "",
            )
            or ""
        )
        if path:
            return path
        trade_log = getattr(cfg.logging, "trade_log", "") or ""
        if trade_log:
            return str(
                Path(trade_log).with_name(
                    "dynamic_fill_hazard_action.csv"
                )
            )
        return "dynamic_fill_hazard_action.csv"

    @staticmethod
    def _resolve_state_conditioned_policy_shadow_log_path(cfg) -> str:
        path = getattr(cfg.logging, "state_conditioned_policy_shadow_log", "") or ""
        if path:
            return path
        trade_log = getattr(cfg.logging, "trade_log", "") or ""
        if trade_log:
            return str(
                Path(trade_log).with_name("state_conditioned_policy_shadow.csv")
            )
        return "state_conditioned_policy_shadow.csv"

    @staticmethod
    def _resolve_cross_venue_fair_price_shadow_log_path(cfg) -> str:
        path = getattr(
            cfg.logging, "cross_venue_fair_price_shadow_log", ""
        ) or ""
        if path:
            return path
        trade_log = getattr(cfg.logging, "trade_log", "") or ""
        if trade_log:
            return str(
                Path(trade_log).with_name("cross_venue_fair_price_shadow.csv")
            )
        return "cross_venue_fair_price_shadow.csv"

    @staticmethod
    def _resolve_exact_opportunity_tape_path(cfg) -> str:
        if not bool(
            getattr(cfg.logging, "exact_opportunity_tape_enabled", False)
        ):
            return ""
        path = getattr(cfg.logging, "exact_opportunity_tape", "") or ""
        if path:
            return path
        trade_log = getattr(cfg.logging, "trade_log", "") or ""
        if trade_log:
            return str(
                Path(trade_log).with_name("exact_opportunity_tape.csv")
            )
        return "exact_opportunity_tape.csv"

    def _configure_exact_opportunity_tape_runtime(self, cfg) -> None:
        previous = getattr(self, "_exact_opportunity_tape_runtime", None)
        if previous is not None:
            previous.close()
            self._exact_opportunity_tape_runtime = None
        validation = validate_exact_opportunity_runtime_config(cfg)
        if not bool(validation["enabled"]):
            return

        repo_root = Path(__file__).resolve().parents[1]
        staging = Path(
            str(
                getattr(
                    cfg.logging,
                    "exact_opportunity_tape_staging_dir",
                    "logs/exact_opportunity_tape_staging",
                )
            )
        ).expanduser()
        if not staging.is_absolute():
            staging = repo_root / staging
        active_ids = {
            str(order.client_order_id)
            for order in self.orders.get_active_orders()
            if not order.is_terminal
        }
        identity = build_exact_opportunity_runtime_identity(
            cfg,
            repo_root=repo_root,
        )
        self._exact_opportunity_tape_runtime = ExactOpportunityDailyWriter(
            staging,
            runtime_identity=identity,
            initial_active_order_ids=active_ids,
            queue_size=int(
                getattr(cfg.logging, "exact_opportunity_tape_queue_size", 20_000)
            ),
            flush_rows=int(
                getattr(cfg.logging, "exact_opportunity_tape_flush_rows", 1_000)
            ),
            flush_interval_s=float(
                getattr(
                    cfg.logging,
                    "exact_opportunity_tape_flush_interval_s",
                    1.0,
                )
            ),
            heartbeat_interval_s=float(
                getattr(
                    cfg.logging,
                    "exact_opportunity_tape_heartbeat_interval_s",
                    5.0,
                )
            ),
        )
        logger.info(
            "EXACT_OPPORTUNITY_TAPE_V2_2 state=%s staging=%s "
            "runtime_identity_sha256=%s quarantined_orders=%d shadow_only=1",
            "quarantine" if active_ids else "collecting",
            staging,
            identity["runtime_identity_sha256"],
            len(active_ids),
        )

    def _exact_opportunity_tape_enabled(self) -> bool:
        return bool(
            getattr(self, "_exact_opportunity_tape_runtime", None) is not None
            or getattr(self, "_exact_opportunity_tape_path", "")
        )

    def exact_opportunity_tape_health_snapshot(self) -> dict[str, Any]:
        runtime = getattr(self, "_exact_opportunity_tape_runtime", None)
        if runtime is None:
            return {"enabled": False, "state": "disabled"}
        return {"enabled": True, **runtime.health_snapshot()}

    @staticmethod
    def _resolve_order_lifecycle_journal_path(cfg) -> str:
        path = getattr(cfg.logging, "order_lifecycle_journal", "") or ""
        if path:
            return str(path)
        trade_log = getattr(cfg.logging, "trade_log", "") or ""
        if trade_log:
            return str(Path(trade_log).with_name("order_lifecycle_journal.csv"))
        return "order_lifecycle_journal.csv"

    @staticmethod
    def _resolve_inventory_campaign_shadow_log_path(cfg) -> str:
        if not bool(
            getattr(cfg.logging, "inventory_campaign_shadow_enabled", False)
        ):
            return ""
        path = getattr(cfg.logging, "inventory_campaign_shadow_log", "") or ""
        if path:
            return path
        trade_log = getattr(cfg.logging, "trade_log", "") or ""
        if trade_log:
            return str(Path(trade_log).with_name("inventory_campaign_shadow.csv"))
        return "inventory_campaign_shadow.csv"

    @staticmethod
    def _resolve_live_perf_telemetry_log_path(cfg) -> str:
        path = getattr(cfg.logging, "live_perf_telemetry_log", "") or ""
        if path:
            return path
        trade_log = getattr(cfg.logging, "trade_log", "") or ""
        if trade_log:
            return str(Path(trade_log).with_name("live_perf_telemetry.csv"))
        return "live_perf_telemetry.csv"

    @staticmethod
    def _resolve_quote_snapshot_integrity_log_path(cfg) -> str:
        path = getattr(cfg.logging, "quote_snapshot_integrity_log", "") or ""
        if path:
            return str(path)
        trade_log = getattr(cfg.logging, "trade_log", "") or ""
        if trade_log:
            return str(Path(trade_log).with_name("quote_snapshot_integrity.csv"))
        return "quote_snapshot_integrity.csv"

    @staticmethod
    def _init_csv_log(path: str, headers: list[str]):
        if not path:
            return
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        if not os.path.exists(path):
            with open(path, "w", newline="") as f:
                csv.writer(f).writerow(headers)

    @staticmethod
    def _policy_reason_text(mask: int) -> str:
        mapping = [
            (POLICY_REASON_FILL_COOLDOWN, "fill_cd"),
            (POLICY_REASON_MARKOUT, "markout"),
            (POLICY_REASON_STALE_WARN, "stale_warn"),
            (POLICY_REASON_STALE_HARD, "stale_hard"),
            (POLICY_REASON_BURST, "burst"),
            (POLICY_REASON_THIN_DEPTH, "thin_depth"),
            (POLICY_REASON_INV_LIMIT, "inv_limit"),
            (POLICY_REASON_EXPOSURE_ONLY, "exposure_only"),
            (POLICY_REASON_ADVERSE, "adverse"),
            (POLICY_REASON_FLAT_TTL, "flat_ttl"),
            (POLICY_REASON_DEFENSE, "defense"),
            (POLICY_REASON_SYNC_DEGRADED, "sync_degraded"),
            (POLICY_REASON_BUY_FILL_SELECTION, "buy_fill_sel"),
            (POLICY_REASON_SPREAD_CAP, "spread_cap"),
            (POLICY_REASON_BUY_HAZARD_CANCEL, "buy_hazard_cancel"),
        ]
        labels = [name for bit, name in mapping if mask & bit]
        return "|".join(labels) if labels else "none"

    def _sync_adjust_degrade_active(self, now: Optional[float] = None) -> bool:
        if not bool(getattr(self.cfg.risk, "sync_adjust_degrade_enabled", True)):
            return False
        now = time.time() if now is None else now
        return now < self._sync_adjust_degrade_until

    def _check_sync_adjust_degrade(self, now: float) -> None:
        risk_cfg = self.cfg.risk
        if not bool(getattr(risk_cfg, "sync_adjust_degrade_enabled", True)):
            return
        window_s = float(getattr(risk_cfg, "sync_adjust_degrade_window_s", 300.0) or 0.0)
        threshold = max(1, int(getattr(risk_cfg, "sync_adjust_degrade_count", 1) or 1))
        abs_qty_threshold = max(0.0, float(getattr(risk_cfg, "sync_adjust_abs_qty_threshold", 0.0) or 0.0))
        snap = self.inventory.sync_adjust_snapshot(window_s)
        seq = int(snap.get("seq", 0) or 0)
        if seq <= self._last_seen_sync_adjust_seq:
            return

        # 中文说明：REST sync 发现 position delta 不等于一定要立刻停机。
        # 这里先按“连续次数”或“单次绝对偏差”触发 hard degrade，
        # 避免 user stream 重连后的第一笔审计差异误挡双边报价。
        self._last_seen_sync_adjust_seq = seq
        recent_count = int(snap.get("recent_count", 0) or 0)
        last_delta = float(snap.get("last_delta", 0.0) or 0.0)
        recent_abs_qty = float(snap.get("recent_abs_qty", 0.0) or 0.0)
        count_triggered = recent_count >= threshold
        size_triggered = abs_qty_threshold > 0.0 and abs(last_delta) >= abs_qty_threshold
        if not count_triggered and not size_triggered:
            logger.warning(
                "SYNC_ADJUST_OBSERVED: count=%d/%d window=%.0fs "
                "last_delta=%+.6f abs_threshold=%.6f recent_abs_qty=%.6f",
                recent_count,
                threshold,
                window_s,
                last_delta,
                abs_qty_threshold,
                recent_abs_qty,
            )
            return

        pause_s = max(0.0, float(getattr(risk_cfg, "sync_adjust_pause_s", 120.0) or 0.0))
        self._sync_adjust_degrade_until = max(self._sync_adjust_degrade_until, now + pause_s)
        if bool(getattr(risk_cfg, "sync_adjust_cancel_orders", True)):
            self._cancel_all_orders()
        logger.error(
            "SYNC_ADJUST_DEGRADE: pausing exposure-increasing quotes for %.0fs "
            "count=%d/%d size_triggered=%s window=%.0fs last_delta=%+.6f "
            "abs_threshold=%.6f recent_abs_qty=%.6f",
            pause_s,
            recent_count,
            threshold,
            size_triggered,
            window_s,
            last_delta,
            abs_qty_threshold,
            recent_abs_qty,
        )
        if (
            bool(getattr(risk_cfg, "sync_adjust_reconnect_user_stream", True))
            and self._ws_handler is not None
            and now - self._last_sync_adjust_user_reconnect >= 30.0
        ):
            restart = getattr(self._ws_handler, "restart_user_stream", None)
            if callable(restart):
                restart("SYNC_ADJUST_DEGRADE")
                self._last_sync_adjust_user_reconnect = now

    def _append_row(self, path: str, row):
        if not path:
            return
        try:
            payload = asdict(row)
            with self._csv_log_lock:
                with open(path, "a", newline="") as f:
                    csv.DictWriter(f, fieldnames=list(payload.keys())).writerow(payload)
        except Exception as exc:
            logger.error(f"CSV log write failed ({path}): {exc}")

    def _append_exact_opportunity_payload(
        self,
        payload: dict[str, object],
    ) -> None:
        runtime = getattr(self, "_exact_opportunity_tape_runtime", None)
        if runtime is not None:
            try:
                runtime.append(payload)
            except Exception as exc:
                runtime.report_error(f"producer_append:{type(exc).__name__}:{exc}")
                logger.error("Exact-opportunity v2.2 append failed: %s", exc)
            return
        path = getattr(self, "_exact_opportunity_tape_path", "")
        if not path:
            return
        self._append_row(
            path,
            ExactQuoteOpportunityTapeRow(**payload),
        )

    def _record_order_lifecycle_journal(
        self,
        order: Any,
        source_event_type: str,
        event: Optional[dict[str, Any]] = None,
    ) -> None:
        lifecycle = getattr(order, "lifecycle", None) if order is not None else None
        if lifecycle is None:
            return
        runtime = getattr(self, "_order_lifecycle_live_writer_v2", None)
        if runtime is not None:
            runtime.enqueue_order_event(
                order,
                str(source_event_type),
                dict(event or {}),
            )
            return
        path = getattr(self, "_order_lifecycle_journal_path", "")
        if not path:
            return
        events = lifecycle.events()
        if not events:
            return
        sequence = int(events[-1]["sequence"])
        client_order_id = str(order.client_order_id)
        with self._order_lifecycle_journal_lock:
            if sequence <= self._journaled_lifecycle_sequence.get(
                client_order_id,
                0,
            ):
                return
            payload = order_lifecycle_journal_payload(
                lifecycle=lifecycle,
                runtime_source="live",
                source_event_type=str(source_event_type),
                client_order_id=client_order_id,
                exchange_order_id=int(getattr(order, "order_id", 0) or 0),
                symbol=str(getattr(order, "symbol", self.cfg.symbol)),
                side=str(getattr(getattr(order, "side", None), "value", "")),
                order_state=str(getattr(order, "state", "").name),
            )
            self._append_row(path, OrderLifecycleJournalRow(**payload))
            self._journaled_lifecycle_sequence[client_order_id] = sequence
            if len(self._journaled_lifecycle_sequence) > 10_000:
                oldest = next(iter(self._journaled_lifecycle_sequence))
                self._journaled_lifecycle_sequence.pop(oldest, None)

    def _record_exact_order_event(
        self,
        order: Any,
        event_type: str,
        event: Optional[dict[str, Any]] = None,
        *,
        trigger_decision_id: str = "",
    ) -> None:
        """Journal a native order event against its originating quote decision."""

        if order is None:
            return
        self._record_order_lifecycle_journal(order, event_type, event)
        if not self._exact_opportunity_tape_enabled():
            return
        context = self._get_order_context(order.client_order_id)
        origin_decision_id = str(context.get("exact_decision_id", ""))
        if not origin_decision_id:
            return
        raw_event = dict(event or {})
        lifecycle_events = (
            order.lifecycle.events()
            if getattr(order, "lifecycle", None) is not None
            else ()
        )
        latest = lifecycle_events[-1] if lifecycle_events else {}
        visibility_ts_ns = int(
            raw_event.get("_local_receive_ts_ns", 0)
            or latest.get("visibility_ts_ns", 0)
            or time.time_ns()
        )
        exchange_ts_ns = int(raw_event.get("_exchange_ts_ns", 0) or 0)
        if exchange_ts_ns <= 0:
            exchange_ts_ns = int(raw_event.get("T", 0) or 0) * 1_000_000
        if exchange_ts_ns <= 0:
            exchange_ts_ns = int(latest.get("exchange_ts_ns", 0) or 0)
        payload = empty_exact_opportunity_row(
            event_type=event_type,
            event_ts_ns=visibility_ts_ns,
            symbol=getattr(order, "symbol", self.cfg.symbol),
            side=getattr(getattr(order, "side", None), "value", ""),
        )
        payload.update(
            exchange_ts_ns=exchange_ts_ns,
            visibility_ts_ns=visibility_ts_ns,
            decision_group_id=str(
                context.get("exact_decision_group_id", "")
            ),
            decision_id=origin_decision_id,
            origin_decision_id=origin_decision_id,
            trigger_decision_id=str(
                trigger_decision_id
                or context.get("exact_trigger_decision_id", "")
            ),
            decision_start_ts_ns=int(
                context.get("exact_decision_start_ts_ns", 0)
            ),
            feature_ready_ts_ns=int(
                context.get("exact_feature_ready_ts_ns", 0)
            ),
            role=str(context.get("exact_role", "unknown")),
            signed_inventory_before=float(
                context.get("exact_signed_inventory_before", math.nan)
            ),
            exposure_increasing=int(
                bool(context.get("exact_exposure_increasing", False))
            ),
            baseline_eligible=int(
                bool(context.get("exact_baseline_eligible", False))
            ),
            baseline_quote_price=float(
                context.get("exact_baseline_quote_price", 0.0)
            ),
            candidate_quote_price=float(
                context.get("exact_candidate_quote_price", 0.0)
            ),
            guard_valid=int(bool(context.get("exact_guard_valid", False))),
            guard_reason=str(
                context.get("exact_guard_reason", "not_evaluated")
            ),
            guard_adverse_side=str(
                context.get("exact_guard_adverse_side", "")
            ),
            requested_outward_ticks=int(
                context.get("exact_requested_outward_ticks", 0)
            ),
            effective_outward_ticks=int(
                context.get("exact_effective_outward_ticks", 0)
            ),
            client_order_id=str(order.client_order_id),
            replaced_client_order_id=str(
                context.get("exact_replaced_client_order_id", "")
            ),
            final_executed_action=str(
                context.get("exact_final_executed_action", "none")
            ),
            queue_reset=int(bool(context.get("exact_queue_reset", False))),
            lifecycle_sequence=int(latest.get("sequence", 0) or 0),
            order_state=str(getattr(order, "state", "").name),
            terminal_reason=str(
                raw_event.get("_reason", "")
                or getattr(getattr(order, "lifecycle", None), "terminal_reason", "")
            ),
            order_quantity=float(getattr(order, "quantity", 0.0)),
            remaining_quantity=float(getattr(order, "remaining_qty", 0.0)),
            fill_quantity=float(raw_event.get("_fill_qty", 0.0) or 0.0),
            fill_price=float(raw_event.get("_fill_price", 0.0) or 0.0),
        )
        self._append_exact_opportunity_payload(payload)

    def _on_order_lifecycle_event(
        self,
        order: Any,
        event_type: str,
        event: dict[str, Any],
    ) -> None:
        self._record_exact_order_event(order, event_type, event)

    def record_reconciled_order_lifecycle(
        self,
        client_order_id: str,
        event_type: str,
    ) -> None:
        """Publish a REST-reconciled lifecycle transition to async journals."""

        order = self.orders.get_order(str(client_order_id))
        lifecycle = getattr(order, "lifecycle", None) if order is not None else None
        events = lifecycle.events() if lifecycle is not None else ()
        if not events:
            return
        self._record_exact_order_event(
            order,
            str(event_type),
            {"_local_receive_ts_ns": int(events[-1]["visibility_ts_ns"])},
        )

    def _set_order_context(self, cid: str, context: dict) -> None:
        with self._order_context_lock:
            self._order_policy_context[cid] = dict(context)

    def _get_order_context(self, cid: str) -> dict:
        with self._order_context_lock:
            return dict(self._order_policy_context.get(cid, {}))

    def _pop_order_context(self, cid: str) -> None:
        with self._order_context_lock:
            self._order_policy_context.pop(cid, None)

    def _reset_perf_rest_counters(self) -> None:
        self._perf_rest_new_count = 0
        self._perf_rest_new_sum_us = 0.0
        self._perf_rest_new_max_us = 0.0
        self._perf_rest_cancel_count = 0
        self._perf_rest_cancel_sum_us = 0.0
        self._perf_rest_cancel_max_us = 0.0
        self._perf_rest_cancel_all_count = 0
        self._perf_rest_cancel_all_sum_us = 0.0
        self._perf_rest_cancel_all_max_us = 0.0

    def _record_perf_rest_latency(self, kind: str, elapsed_us: float) -> None:
        elapsed_us = max(0.0, float(elapsed_us))
        if kind == "new":
            self._perf_rest_new_count += 1
            self._perf_rest_new_sum_us += elapsed_us
            self._perf_rest_new_max_us = max(self._perf_rest_new_max_us, elapsed_us)
        elif kind == "cancel":
            self._perf_rest_cancel_count += 1
            self._perf_rest_cancel_sum_us += elapsed_us
            self._perf_rest_cancel_max_us = max(self._perf_rest_cancel_max_us, elapsed_us)
        elif kind == "cancel_all":
            self._perf_rest_cancel_all_count += 1
            self._perf_rest_cancel_all_sum_us += elapsed_us
            self._perf_rest_cancel_all_max_us = max(self._perf_rest_cancel_all_max_us, elapsed_us)

    @staticmethod
    def _age_from_seen(store: Any, key: str, now: float) -> float:
        if not isinstance(store, dict) or not key:
            return float("inf")
        last = store.get(key) or store.get(key.lower()) or store.get(key.upper())
        if not last:
            return float("inf")
        return max(0.0, now - float(last))

    def _ws_age_snapshot(self, now: float) -> dict[str, float]:
        """Best-effort WebSocket freshness telemetry.

        中文说明：这里故意只读 WSHandler 的已见时间戳，不参与风控；
        真实风控仍由 ws_handler 的 silence watchdog 和 depth stale guard 负责。
        """
        ws = self._ws_handler
        symbol = str(getattr(self.cfg, "symbol", "") or "").lower()
        if ws is None or not symbol:
            return {
                "exec_trade_age_s": float("inf"),
                "exec_book_age_s": float("inf"),
                "anchor_trade_max_age_s": float("inf"),
                "anchor_book_max_age_s": float("inf"),
                "spot_trade_max_age_s": float("inf"),
                "spot_book_max_age_s": float("inf"),
            }

        market_trade = getattr(ws, "_market_trade_seen", {})
        market_book = getattr(ws, "_market_book_seen", {})
        spot_trade = getattr(ws, "_spot_trade_seen", {})
        spot_book = getattr(ws, "_spot_book_seen", {})
        exec_trade_age = self._age_from_seen(market_trade, symbol, now)
        exec_book_age = self._age_from_seen(market_book, symbol, now)

        anchor_trade_ages = [
            self._age_from_seen(market_trade, key, now)
            for key in market_trade.keys()
            if str(key).lower() != symbol
        ]
        anchor_book_ages = [
            self._age_from_seen(market_book, key, now)
            for key in market_book.keys()
            if str(key).lower() != symbol
        ]
        spot_trade_ages = [self._age_from_seen(spot_trade, key, now) for key in spot_trade.keys()]
        spot_book_ages = [self._age_from_seen(spot_book, key, now) for key in spot_book.keys()]
        return {
            "exec_trade_age_s": exec_trade_age,
            "exec_book_age_s": exec_book_age,
            "anchor_trade_max_age_s": max(anchor_trade_ages) if anchor_trade_ages else float("inf"),
            "anchor_book_max_age_s": max(anchor_book_ages) if anchor_book_ages else float("inf"),
            "spot_trade_max_age_s": max(spot_trade_ages) if spot_trade_ages else float("inf"),
            "spot_book_max_age_s": max(spot_book_ages) if spot_book_ages else float("inf"),
        }

    def _log_live_perf_telemetry(
        self,
        *,
        requote_start_perf: float,
        status: str,
        mid: float = 0.0,
        q: float = 0.0,
        timings: Optional[dict[str, float]] = None,
    ) -> None:
        timings = timings or {}
        now = time.time()
        ws_ages = self._ws_age_snapshot(now)
        try:
            depth_age = self.signal.last_depth_age_s(now)
        except Exception:
            depth_age = float("inf")
        self._append_row(
            self._live_perf_telemetry_log_path,
            LivePerfTelemetryLogRow(
                timestamp=f"{now:.3f}",
                symbol=self.cfg.symbol,
                event="requote",
                status=status,
                requote_id=int(self._requote_count),
                mid=float(mid or 0.0),
                q=float(q or 0.0),
                requote_total_us=(time.perf_counter() - requote_start_perf) * 1_000_000.0,
                sync_check_us=float(timings.get("sync_check_us", 0.0)),
                stale_check_us=float(timings.get("stale_check_us", 0.0)),
                signal_compute_us=float(timings.get("signal_compute_us", 0.0)),
                risk_check_us=float(timings.get("risk_check_us", 0.0)),
                compute_quotes_us=float(timings.get("compute_quotes_us", 0.0)),
                update_orders_us=float(timings.get("update_orders_us", 0.0)),
                rest_new_count=int(self._perf_rest_new_count),
                rest_new_sum_us=float(self._perf_rest_new_sum_us),
                rest_new_max_us=float(self._perf_rest_new_max_us),
                rest_cancel_count=int(self._perf_rest_cancel_count),
                rest_cancel_sum_us=float(self._perf_rest_cancel_sum_us),
                rest_cancel_max_us=float(self._perf_rest_cancel_max_us),
                rest_cancel_all_count=int(self._perf_rest_cancel_all_count),
                rest_cancel_all_sum_us=float(self._perf_rest_cancel_all_sum_us),
                rest_cancel_all_max_us=float(self._perf_rest_cancel_all_max_us),
                exec_trade_age_s=float(ws_ages["exec_trade_age_s"]),
                exec_book_age_s=float(ws_ages["exec_book_age_s"]),
                exec_depth_age_s=float(depth_age),
                anchor_trade_max_age_s=float(ws_ages["anchor_trade_max_age_s"]),
                anchor_book_max_age_s=float(ws_ages["anchor_book_max_age_s"]),
                spot_trade_max_age_s=float(ws_ages["spot_trade_max_age_s"]),
                spot_book_max_age_s=float(ws_ages["spot_book_max_age_s"]),
                active_orders=int(self.orders.active_count()),
                bid_action=str(self._last_bid_action),
                ask_action=str(self._last_ask_action),
                cpp_routing_used=int(self._last_cpp_routing_used),
            ),
        )

    def _log_inventory_campaign_shadow(self, now: float, mid: float, q: float) -> None:
        """Log shadow-only inventory campaign gates at quote time.

        中文说明：这里不改变报价，只记录“如果库存/持仓时间阈值生效，
        哪一侧会因为继续加仓而被挡住”。后续用这个文件评估 size cap、
        campaign age cap 和 reducing-only 三类库存风险控制。
        """
        if not self._inventory_campaign_shadow_log_path:
            return
        camp = self.inventory.campaign_snapshot()
        active = bool(camp.active and abs(q) > 1e-10)
        bid_inc = bool(active and q >= 0.0)
        ask_inc = bool(active and q <= 0.0)
        abs_q = abs(q)
        age_s = float(camp.age_s)

        def inv_block(threshold: float, is_inc: bool) -> int:
            return int(bool(active and is_inc and abs_q >= threshold))

        def age_block(threshold_s: float, is_inc: bool) -> int:
            return int(bool(active and is_inc and age_s >= threshold_s))

        self._append_row(
            self._inventory_campaign_shadow_log_path,
            InventoryCampaignShadowLogRow(
                timestamp=f"{now:.3f}",
                symbol=self.cfg.symbol,
                q=q,
                mid=mid,
                active=int(active),
                campaign_id=int(camp.campaign_id),
                side=camp.side,
                age_s=age_s,
                max_abs_qty=float(camp.max_abs_qty),
                realized_pnl=float(camp.realized_pnl),
                unrealized_pnl=float(camp.unrealized_pnl),
                total_pnl=float(camp.total_pnl),
                adverse_excursion=float(camp.adverse_excursion),
                fills=int(camp.fills),
                buy_fills=int(camp.buy_fills),
                sell_fills=int(camp.sell_fills),
                exposure_increasing_fills=int(camp.exposure_increasing_fills),
                reducing_fills=int(camp.reducing_fills),
                bid_exposure_increasing=int(bid_inc),
                ask_exposure_increasing=int(ask_inc),
                bid_block_if_inv_006=inv_block(0.006, bid_inc),
                ask_block_if_inv_006=inv_block(0.006, ask_inc),
                bid_block_if_inv_008=inv_block(0.008, bid_inc),
                ask_block_if_inv_008=inv_block(0.008, ask_inc),
                bid_block_if_inv_010=inv_block(0.010, bid_inc),
                ask_block_if_inv_010=inv_block(0.010, ask_inc),
                bid_block_if_age_20m=age_block(20.0 * 60.0, bid_inc),
                ask_block_if_age_20m=age_block(20.0 * 60.0, ask_inc),
                bid_block_if_age_40m=age_block(40.0 * 60.0, bid_inc),
                ask_block_if_age_40m=age_block(40.0 * 60.0, ask_inc),
                bid_block_if_age_60m=age_block(60.0 * 60.0, bid_inc),
                ask_block_if_age_60m=age_block(60.0 * 60.0, ask_inc),
                bid_block_if_reducing_only=int(bid_inc),
                ask_block_if_reducing_only=int(ask_inc),
            ),
        )

    def _current_l2_policy_metrics(
        self,
        mid: float,
        quote_snapshot: Optional[QuoteDecisionSnapshot] = None,
    ) -> dict:
        snapshot = quote_snapshot or self.signal.quote_decision_snapshot()
        depth = snapshot
        metrics = {
            # Policy freshness is local visibility age. End-to-end source age
            # remains available on the immutable snapshot for diagnostics.
            "depth_age_s": snapshot.depth_visible_age_s,
            "microprice_shift_bps": self._depth_micro_shift_bps(mid, depth, levels=3),
            "l2_quote_flip_rate": 0.0,
            "l2_book_refresh_ratio": 0.0,
            "l2_book_cancel_ratio": 0.0,
            "l2_near_depth_total": 0.0,
        }
        snapshots = snapshot.depth_history
        if not snapshots:
            return metrics

        end_exchange_ms = float(snapshot.depth_exchange_ts_ms)
        start_exchange_ms = end_exchange_ms - 10_000.0
        prev_summary = None
        sample_count = 0
        flip_count = 0.0
        refresh_sum = 0.0
        cancel_sum = 0.0
        for snap in snapshots:
            if (
                snap.exchange_ts_ms < start_exchange_ms
                or snap.exchange_ts_ms > end_exchange_ms
            ):
                continue
            summary = self.signal._snapshot_l2_state(snap)
            if summary is None:
                continue
            state, total_depth, best_bid, best_ask = summary
            metrics["l2_near_depth_total"] = state.get("l2_near_depth_total", metrics["l2_near_depth_total"])
            if prev_summary is not None:
                prev_total, prev_bid, prev_ask = prev_summary
                if best_bid != prev_bid or best_ask != prev_ask:
                    flip_count += 1.0
                if prev_total > 0:
                    delta = total_depth - prev_total
                    if delta > 0:
                        refresh_sum += delta / prev_total
                    elif delta < 0:
                        cancel_sum += -delta / prev_total
            prev_summary = (total_depth, best_bid, best_ask)
            sample_count += 1

        if sample_count > 0:
            metrics["l2_quote_flip_rate"] = flip_count / sample_count
            metrics["l2_book_refresh_ratio"] = refresh_sum / sample_count
            metrics["l2_book_cancel_ratio"] = cancel_sum / sample_count
        return metrics

    @staticmethod
    def _finite_float(value: Any, default: float = 0.0) -> float:
        try:
            out = float(value)
        except (TypeError, ValueError):
            return default
        return out if math.isfinite(out) else default

    def buy_fill_selection_live_snapshot(self) -> dict[str, float]:
        eval_count = int(self._buy_fill_selection_eval_count)
        hit_count = int(self._buy_fill_selection_hit_count)
        action_count = int(self._buy_fill_selection_action_count)
        now = time.time()
        return {
            "eval_count": eval_count,
            "hit_count": hit_count,
            "hit_rate": (hit_count / eval_count) if eval_count > 0 else 0.0,
            "action_count": action_count,
            "action_rate": (action_count / eval_count) if eval_count > 0 else 0.0,
            "last_hit_age_s": (now - self._buy_fill_selection_last_hit_time) if self._buy_fill_selection_last_hit_time > 0.0 else -1.0,
            "last_eval_age_s": (now - self._buy_fill_selection_last_eval_time) if self._buy_fill_selection_last_eval_time > 0.0 else -1.0,
            "last_score": self._buy_fill_selection_last_score,
            "last_missing_features": float(self._buy_fill_selection_last_missing),
        }

    def boolean_cooldown_policy_snapshot(self) -> dict[str, Any]:
        policy = self._boolean_cooldown_policy
        if policy is None:
            return {
                "enabled": 0,
                "evaluations": 0,
                "supported": 0,
                "nonbaseline": 0,
                "fallback": 0,
                "last_action": "CONTROL_85N",
                "last_fallback": "disabled",
                "last_decision_age_s": -1.0,
                "windows": {
                    "updates": 0,
                    "completed_windows": 0,
                    "gap_windows": 0,
                    "resets": 0,
                    "invalid_updates": 0,
                    "out_of_order_updates": 0,
                    "warmup_admitted": 0,
                    "feature_ready_ts_ns": 0,
                    "last_error": "",
                },
            }
        return policy.audit()

    def dynamic_fill_hazard_shadow_snapshot(self) -> dict[str, Any]:
        rows = int(self._dynamic_fill_hazard_shadow_rows)
        valid = int(self._dynamic_fill_hazard_shadow_valid_rows)
        now = time.time()
        policy = self._dynamic_fill_hazard_action_policy
        with self._dynamic_fill_hazard_action_lock:
            hold = self._dynamic_fill_hazard_action_hold
        return {
            "enabled": int(
                self._dynamic_fill_hazard_shadow_runtime is not None
            ),
            "rows": rows,
            "valid_rows": valid,
            "invalid_rows": int(
                self._dynamic_fill_hazard_shadow_invalid_rows
            ),
            "valid_rate": valid / rows if rows > 0 else 0.0,
            "last_age_s": (
                now - self._dynamic_fill_hazard_shadow_last_time
                if self._dynamic_fill_hazard_shadow_last_time > 0.0
                else -1.0
            ),
            "last_favorable_probability": float(
                self._dynamic_fill_hazard_shadow_last_favorable
            ),
            "last_adverse_probability": float(
                self._dynamic_fill_hazard_shadow_last_adverse
            ),
            "action_authorized": int(policy is not None),
            "action_threshold": (
                float(policy.entry_threshold)
                if policy is not None
                else math.nan
            ),
            "action_last_score": float(
                self._dynamic_fill_hazard_action_last_score
            ),
            "action_hold": int(hold is not None),
            "action_hold_phase": (
                hold.phase.value if hold is not None else "NONE"
            ),
            "action_hold_age_s": (
                max(0.0, now - hold.entered_ts_ns / 1_000_000_000.0)
                if hold is not None
                else 0.0
            ),
            "action_cancel_count": int(
                self._dynamic_fill_hazard_action_cancel_count
            ),
            "action_reentry_count": int(
                self._dynamic_fill_hazard_action_reentry_count
            ),
            "action_keep_count": int(
                self._dynamic_fill_hazard_action_keep_count
            ),
            "action_invalid_hold_count": int(
                self._dynamic_fill_hazard_action_invalid_hold_count
            ),
        }

    @staticmethod
    def _order_state_name(order: Any) -> str:
        if order is None:
            return "MISSING"
        state = getattr(order, "state", "")
        return str(getattr(state, "name", state)).upper()

    def _log_dynamic_fill_hazard_action(
        self,
        *,
        observation: Any,
        event: str,
        adverse_value: float,
        cancel_succeeded: bool = False,
    ) -> None:
        policy = self._dynamic_fill_hazard_action_policy
        if policy is None:
            return
        order = self.orders.get_order(observation.client_order_id)
        with self._dynamic_fill_hazard_action_lock:
            hold = self._dynamic_fill_hazard_action_hold
        hold_age_ms = (
            max(
                0.0,
                (
                    int(observation.feature_ready_ts_ns)
                    - hold.entered_ts_ns
                )
                / 1_000_000.0,
            )
            if hold is not None
            else 0.0
        )
        self._append_row(
            self._dynamic_fill_hazard_action_log_path,
            DynamicFillHazardActionLogRow(
                timestamp=(
                    f"{float(observation.feature_ready_ts_ns) / 1_000_000_000.0:.6f}"
                ),
                symbol=self.cfg.symbol,
                policy_id=policy.policy_id,
                policy_file_sha256=policy.file_sha256,
                model_file_sha256=policy.model_file_sha256,
                client_order_id=observation.client_order_id,
                inventory_role=observation.inventory_role,
                event=str(event),
                adverse_value=float(adverse_value),
                entry_threshold=float(policy.entry_threshold),
                favorable_probability=float(
                    observation.favorable_probability
                ),
                adverse_probability=float(
                    observation.adverse_probability
                ),
                order_state=self._order_state_name(order),
                cancel_succeeded=int(bool(cancel_succeeded)),
                hold_age_ms=float(hold_age_ms),
                deep_generation=int(observation.deep_generation),
                deep_age_ms=float(observation.deep_age_ms),
            ),
        )

    def _release_dynamic_fill_hazard_action_hold(
        self,
        *,
        event: str,
        force_requote: bool,
    ) -> Optional[_BuyHazardCancelHold]:
        with self._dynamic_fill_hazard_action_lock:
            hold = self._dynamic_fill_hazard_action_hold
            if hold is None:
                return None
            self._dynamic_fill_hazard_action_hold = None
        if force_requote:
            self._last_requote_time = 0.0
        logger.warning(
            "BUY_HAZARD_POLICY_RELEASE event=%s cid=%s "
            "entry_score=%.8f force_requote=%d",
            event,
            hold.client_order_id,
            hold.entry_score,
            int(bool(force_requote)),
        )
        return hold

    def _dynamic_fill_hazard_buy_blocked(self, q: float) -> bool:
        """Block only BUY quotes that would increase absolute exposure."""

        if self._dynamic_fill_hazard_action_policy is None:
            return False
        with self._dynamic_fill_hazard_action_lock:
            hold = self._dynamic_fill_hazard_action_hold
        if hold is None:
            return False
        return float(q) >= -max(
            float(self.cfg.lot_size) * 0.5,
            1e-12,
        )

    def _dynamic_fill_hazard_prospective_state(
        self,
        *,
        candidate_price: float,
        now_ns: int,
    ) -> tuple[dict[str, Any], Any]:
        """Read one atomic current-book view for a fresh BUY candidate."""

        ws = self._ws_handler
        if ws is None:
            return {}, {}
        provider = getattr(
            ws,
            "dynamic_fill_hazard_prospective_state",
            None,
        )
        if callable(provider):
            deep_book, candidate_level = provider(
                side="BUY",
                price=float(candidate_price),
                now_ns=int(now_ns),
            )
            return dict(deep_book or {}), candidate_level

        # The current WS handler owns this thread-safe book object. Keeping the
        # read here avoids expanding the public live ABI while q90 action is OFF.
        book = getattr(ws, "_deep_book", None)
        if book is not None and hasattr(book, "atomic_read"):
            max_age_ms = (
                float(self.cfg.websocket.deep_book_max_age_s) * 1_000.0
            )
            with book.atomic_read():
                deep_book = book.snapshot(
                    now_ns=int(now_ns),
                    max_age_ms=max_age_ms,
                )
                candidate_level = book.level_state(
                    "BUY",
                    float(candidate_price),
                    now_ns=int(now_ns),
                    max_age_ms=max_age_ms,
                )
            return dict(deep_book), candidate_level
        return {}, {}

    def _evaluate_dynamic_fill_hazard_prospective_recovery(
        self,
        *,
        candidate_price: float,
        inventory: float,
        now_ns: int,
    ) -> str:
        runtime = getattr(self, "_dynamic_fill_hazard_shadow_runtime", None)
        policy = getattr(self, "_dynamic_fill_hazard_action_policy", None)
        if runtime is None or policy is None:
            return "baseline"
        with self._dynamic_fill_hazard_action_lock:
            hold = self._dynamic_fill_hazard_action_hold
        if hold is None or hold.phase != OrderLifecyclePhase.POST_CANCEL_RECOVERY:
            return "not_in_post_cancel_recovery"
        order = self.orders.get_order(hold.client_order_id)
        lifecycle = getattr(order, "lifecycle", None)
        if lifecycle is None:
            raise RuntimeError("q90 prospective recovery is missing lifecycle")
        snapshot = lifecycle.snapshot(now_ns=int(now_ns))
        if snapshot["terminal_policy_route"] != (
            TerminalPolicyRoute.PROSPECTIVE_CANCEL_REENTRY.value
        ):
            raise RuntimeError(
                "q90 prospective recovery received a non-cancel terminal route"
            )
        deep_book, candidate_level = self._dynamic_fill_hazard_prospective_state(
            candidate_price=float(candidate_price),
            now_ns=int(now_ns),
        )
        result = runtime.evaluate_prospective_cancel_reentry(
            terminal_policy_route=str(snapshot["terminal_policy_route"]),
            terminal_reason=str(snapshot["terminal_reason"]),
            remaining_quantity=float(snapshot["remaining_quantity"]),
            candidate_price=float(candidate_price),
            inventory=float(inventory),
            deep_book=deep_book,
            candidate_level=candidate_level,
            now_ns=int(now_ns),
            prospective_id=hold.client_order_id,
        )
        observation = result.observation
        if not result.activation_supported:
            self._dynamic_fill_hazard_action_invalid_hold_count += 1
            return "prospective_hold_invalid"
        if not policy.recovered(observation):
            return "prospective_hold"
        lifecycle.mark_reentry_eligible(int(now_ns))
        with self._dynamic_fill_hazard_action_lock:
            current = self._dynamic_fill_hazard_action_hold
            if current is not hold:
                raise RuntimeError("q90 recovery hold changed during evaluation")
            current.recovered = True
            current.phase = OrderLifecyclePhase.REENTRY_ELIGIBLE
        self._log_dynamic_fill_hazard_action(
            observation=observation,
            event="prospective_placement_recovered",
            adverse_value=policy.score(observation),
            cancel_succeeded=hold.cancel_succeeded,
        )
        self._release_dynamic_fill_hazard_action_hold(
            event="prospective_placement_recovered",
            force_requote=False,
        )
        self._dynamic_fill_hazard_action_reentry_count += 1
        return "baseline_reenter"

    def _apply_dynamic_fill_hazard_action(
        self,
        observation: Any,
    ) -> str:
        policy = self._dynamic_fill_hazard_action_policy
        ws = self._ws_handler
        if policy is None or ws is None:
            return "baseline"
        score = (
            policy.score(observation)
            if observation.valid
            else math.nan
        )
        if math.isfinite(score):
            self._dynamic_fill_hazard_action_last_score = score

        with self._dynamic_fill_hazard_action_lock:
            hold = self._dynamic_fill_hazard_action_hold

        if hold is None:
            if not policy.eligible(observation):
                return "invalid_keep" if not observation.valid else "baseline"
            if not policy.cancel_required(observation):
                self._dynamic_fill_hazard_action_keep_count += 1
                return "keep"
            hold = _BuyHazardCancelHold(
                client_order_id=observation.client_order_id,
                order_price=float(observation.order_price),
                entered_ts_ns=int(observation.feature_ready_ts_ns),
                entry_score=float(score),
            )
            with self._dynamic_fill_hazard_action_lock:
                self._dynamic_fill_hazard_action_hold = hold
            cancel_succeeded = self._cancel_order(
                observation.client_order_id,
                record_requote_perf=False,
            )
            with self._dynamic_fill_hazard_action_lock:
                current = self._dynamic_fill_hazard_action_hold
                if current is hold:
                    current.cancel_succeeded = bool(cancel_succeeded)
            self._dynamic_fill_hazard_action_cancel_count += 1
            self._log_dynamic_fill_hazard_action(
                observation=observation,
                event="cancel_request",
                adverse_value=score,
                cancel_succeeded=cancel_succeeded,
            )
            logger.warning(
                "BUY_HAZARD_POLICY_CANCEL cid=%s role=%s "
                "score=%.8f threshold=%.8f cancel_succeeded=%d",
                observation.client_order_id,
                observation.inventory_role,
                score,
                policy.entry_threshold,
                int(bool(cancel_succeeded)),
            )
            return "cancel"

        if observation.client_order_id != hold.client_order_id:
            return "blocked_other"
        if hold.phase == OrderLifecyclePhase.EXCHANGE_TERMINAL:
            return "terminal_reason_requires_explicit_route"
        if hold.phase == OrderLifecyclePhase.POST_CANCEL_RECOVERY:
            return "post_cancel_recovery_requires_prospective_placement"
        if hold.phase == OrderLifecyclePhase.REENTRY_ELIGIBLE:
            self._release_dynamic_fill_hazard_action_hold(
                event="prospective_placement_recovered",
                force_requote=True,
            )
            self._dynamic_fill_hazard_action_reentry_count += 1
            return "baseline_reenter"
        if not observation.valid:
            self._dynamic_fill_hazard_action_invalid_hold_count += 1
            return "hold_invalid"

        if observation.inventory_role == "reducing":
            recovered = True
            recovery_event = "reducing_role_release"
        else:
            recovered = policy.recovered(observation)
            recovery_event = "score_recovered"
        if recovered:
            with self._dynamic_fill_hazard_action_lock:
                current = self._dynamic_fill_hazard_action_hold
                if current is hold:
                    current.recovered = True

        order = self.orders.get_order(hold.client_order_id)
        state_name = self._order_state_name(order)
        cancel_pending = state_name == "PENDING_CANCEL"
        if recovered and not cancel_pending:
            self._log_dynamic_fill_hazard_action(
                observation=observation,
                event=recovery_event,
                adverse_value=score,
                cancel_succeeded=hold.cancel_succeeded,
            )
            self._release_dynamic_fill_hazard_action_hold(
                event=recovery_event,
                force_requote=False,
            )
            return "cancel_not_effective_keep"
        if recovered:
            return "recovery_wait_cancel_ack"
        return "hold"

    def _evaluate_dynamic_fill_hazard_shadow(self, now_ns: int) -> None:
        """Score active/retained orders and apply the frozen BUY action map."""

        runtime = getattr(self, "_dynamic_fill_hazard_shadow_runtime", None)
        bundle = self._dynamic_fill_hazard_shadow_bundle
        ws = self._ws_handler
        if runtime is None or bundle is None or ws is None:
            return
        visible_snapshot_fn = getattr(
            ws,
            "dynamic_fill_hazard_visible_snapshot",
            None,
        )
        if callable(visible_snapshot_fn):
            visible_snapshot = visible_snapshot_fn()
            snapshot_ready_ns = int(
                getattr(visible_snapshot, "feature_ready_ts_ns", 0) or 0
            )
            if snapshot_ready_ns <= 0 or snapshot_ready_ns > int(now_ns):
                return
            snapshot_paths = tuple(getattr(visible_snapshot, "paths", ()))
            deep_book = dict(getattr(visible_snapshot, "deep_book", {}) or {})
        else:
            snapshot_paths = tuple(ws.active_order_depth_states())
            deep_book = ws.deep_book_snapshot(now_ns=now_ns)
        paths = {state.client_order_id: state for state in snapshot_paths}
        active_orders = self.orders.get_active_orders()
        candidates = [
            (
                order.client_order_id,
                order.side.value,
                float(order.price),
            )
            for order in active_orders
        ]
        active_ids = [client_order_id for client_order_id, _, _ in candidates]
        with self._dynamic_fill_hazard_action_lock:
            hold = self._dynamic_fill_hazard_action_hold
        inventory = float(self.inventory.net_position)
        if (
            hold is not None
            and inventory < -max(float(self.cfg.lot_size) * 0.5, 1e-12)
        ):
            if hold.phase == OrderLifecyclePhase.POST_CANCEL_RECOVERY:
                hold.phase = OrderLifecyclePhase.REENTRY_ELIGIBLE
            self._release_dynamic_fill_hazard_action_hold(
                event="reducing_inventory_release",
                force_requote=True,
            )
            hold = None
        runtime.drop_inactive(active_ids)
        if not paths:
            return
        for client_order_id, side, order_price in candidates:
            path = paths.get(client_order_id)
            if path is None:
                continue
            observation = runtime.evaluate(
                client_order_id=client_order_id,
                side=side,
                order_price=order_price,
                inventory=inventory,
                path=path,
                deep_book=deep_book,
                now_ns=int(now_ns),
            )
            if observation is None:
                continue
            self._dynamic_fill_hazard_shadow_rows += 1
            self._dynamic_fill_hazard_shadow_last_time = (
                float(now_ns) / 1_000_000_000.0
            )
            if observation.valid:
                self._dynamic_fill_hazard_shadow_valid_rows += 1
                self._dynamic_fill_hazard_shadow_last_favorable = (
                    observation.favorable_probability
                )
                self._dynamic_fill_hazard_shadow_last_adverse = (
                    observation.adverse_probability
                )
            else:
                self._dynamic_fill_hazard_shadow_invalid_rows += 1
            executed_action = self._apply_dynamic_fill_hazard_action(
                observation
            )
            action_authorized = int(
                self._dynamic_fill_hazard_action_policy is not None
                and (
                    self._dynamic_fill_hazard_action_policy.eligible(
                        observation
                    )
                    or (
                        self._dynamic_fill_hazard_action_hold is not None
                        and observation.client_order_id
                        == self._dynamic_fill_hazard_action_hold.client_order_id
                    )
                )
            )
            self._append_row(
                self._dynamic_fill_hazard_shadow_log_path,
                DynamicFillHazardShadowLogRow(
                    timestamp=f"{float(now_ns) / 1_000_000_000.0:.6f}",
                    symbol=self.cfg.symbol,
                    model_family_id=observation.model_family_id,
                    model_file_sha256=bundle.file_sha256,
                    client_order_id=observation.client_order_id,
                    side=observation.side,
                    inventory_role=observation.inventory_role,
                    valid=int(observation.valid),
                    reason=observation.reason,
                    edge_ms=observation.edge_ms,
                    elapsed_ms=observation.elapsed_ms,
                    missed_edges=observation.missed_edges,
                    feature_source_ts_ns=observation.feature_source_ts_ns,
                    feature_ready_ts_ns=observation.feature_ready_ts_ns,
                    deep_generation=observation.deep_generation,
                    deep_age_ms=observation.deep_age_ms,
                    order_price=observation.order_price,
                    mid=observation.mid,
                    microprice=observation.microprice,
                    queue_initial=observation.queue_initial,
                    queue_remaining=observation.queue_remaining,
                    cancel_events=observation.cancel_events,
                    cancel_qty=observation.cancel_qty,
                    refill_events=observation.refill_events,
                    refill_qty=observation.refill_qty,
                    favorable_probability=(
                        observation.favorable_probability
                    ),
                    adverse_probability=observation.adverse_probability,
                    favorable_raw_probability=(
                        observation.favorable_raw_probability
                    ),
                    adverse_raw_probability=(
                        observation.adverse_raw_probability
                    ),
                    action_authorized=action_authorized,
                    executed_action=executed_action,
                ),
            )

    def _fill_selection_live_features(
        self,
        side: Side,
        q: float,
        pred: Prediction,
        quote_ctx: dict,
        *,
        mid: float,
        decision: SidePolicyDecision,
        exposure_increasing: bool,
    ) -> dict[str, Any]:
        base_px = self._finite_float(
            quote_ctx.get("pre_guard_price", quote_ctx.get("base_price", quote_ctx.get("price"))),
            0.0,
        )
        near_depth = max(
            decision.l2_near_depth_total,
            self._finite_float(quote_ctx.get("near_depth_total"), 0.0),
            self._finite_float(quote_ctx.get("l2_near_depth_total"), 0.0),
        )
        prediction_features = dict(getattr(pred, "feature_dict", None) or {})
        merged = dict(prediction_features)
        merged.update(dict(quote_ctx or {}))
        queue_local_rank = self._finite_float(merged.get("queue_local_rank"), 0.5)
        features = build_fill_selection_feature_row(
            prediction_features=prediction_features,
            quote_context=quote_ctx,
            side=side.value,
            inventory=q,
            max_inventory=float(getattr(self.cfg.strategy, "max_inventory", 0.0) or 0.0),
            mid=mid,
            base_price=base_px,
            allow_post=decision.allow_post,
            allow_exposure_increase=decision.allow_exposure_increase,
            exposure_increasing=exposure_increasing,
            near_depth_total=near_depth,
            toxicity=decision.toxicity,
            markout_ema=decision.markout_ema,
            queue_local_rank=queue_local_rank,
            materialize=materialize_quote_ev_feature_values,
        )
        features.setdefault("l2_book_refresh_ratio", decision.l2_book_refresh_ratio)
        features.setdefault("l2_book_cancel_ratio", decision.l2_book_cancel_ratio)
        features.setdefault("microprice_shift_bps", decision.microprice_shift_bps)
        return features

    def _apply_buy_fill_selection_live_arm(
        self,
        *,
        side: Side,
        mid: float,
        q: float,
        decision: SidePolicyDecision,
        quote_ctx: dict,
        pred: Prediction,
    ) -> None:
        cfg = self.cfg.strategy
        shadow_enabled = bool(
            getattr(cfg, "buy_fill_selection_shadow_enabled", False)
        )
        action_enabled = bool(
            getattr(cfg, "buy_fill_selection_live_enabled", False)
        )
        if side != Side.BUY or not (shadow_enabled or action_enabled):
            return

        exposure_increasing = q >= 0.0
        if not exposure_increasing and not bool(getattr(cfg, "buy_fill_selection_live_apply_reducing", False)):
            return

        model = _get_buy_fill_selection_model(_resolve_buy_fill_selection_model_path(self.cfg))
        if model is None:
            return

        features = self._fill_selection_live_features(
            side,
            q,
            pred,
            quote_ctx,
            mid=mid,
            decision=decision,
            exposure_increasing=exposure_increasing,
        )
        score_result = model.score(features)
        threshold = float(getattr(cfg, "buy_fill_selection_live_score_threshold", 0.50) or 0.50)
        max_missing = int(getattr(cfg, "buy_fill_selection_live_max_missing_features", 99) or 0)
        reason_mask_before = int(decision.reason_mask)
        spread_mult_before = float(decision.spread_mult)
        hard_mask = (
            POLICY_REASON_STALE_HARD
            | POLICY_REASON_BURST
            | POLICY_REASON_INV_LIMIT
            | POLICY_REASON_DEFENSE
            | POLICY_REASON_SYNC_DEGRADED
            | POLICY_REASON_FILL_COOLDOWN
            | POLICY_REASON_SPREAD_CAP
        )
        hard_blocked = bool(reason_mask_before & hard_mask) or (not decision.allow_post) or (not decision.allow_exposure_increase)
        hit = bool(score_result.score >= threshold and score_result.missing_features <= max_missing)
        actionable_hit = fill_selection_actionable(
            threshold_hit=hit,
            allow_post=decision.allow_post,
            allow_exposure_increase=decision.allow_exposure_increase,
            hard_reason_active=hard_blocked,
        )
        action_applied = bool(action_enabled and actionable_hit)

        self._buy_fill_selection_eval_count += 1
        self._buy_fill_selection_last_eval_time = time.time()
        self._buy_fill_selection_last_score = float(score_result.score)
        self._buy_fill_selection_last_missing = int(score_result.missing_features)

        if actionable_hit:
            self._buy_fill_selection_hit_count += 1
            self._buy_fill_selection_last_hit_time = self._buy_fill_selection_last_eval_time

        if action_applied:
            self._buy_fill_selection_action_count += 1
            decision.reason_mask |= POLICY_REASON_BUY_FILL_SELECTION
            cap = max(1.0, float(getattr(cfg, "buy_fill_selection_live_spread_mult_cap", 1.0) or 1.0))
            decision.spread_mult = min(decision.spread_mult, cap)
            quote_ctx["buy_fill_selection_live_hit"] = True
            quote_ctx["buy_fill_selection_live_score"] = float(score_result.score)

        self._append_row(
            self._buy_fill_selection_shadow_log_path,
            BuyFillSelectionShadowLogRow(
                timestamp=f"{self._buy_fill_selection_last_eval_time:.3f}",
                symbol=self.cfg.symbol,
                enabled=int(action_enabled),
                hit=int(hit),
                actionable_hit=int(actionable_hit),
                q=q,
                mid=mid,
                score=float(score_result.score),
                threshold=threshold,
                missing_features=int(score_result.missing_features),
                used_features=int(score_result.used_features),
                model_count=int(score_result.model_count),
                base_spread_mult=spread_mult_before,
                final_spread_mult=float(decision.spread_mult),
                quote_distance_bps=self._finite_float(features.get("quote_distance_bps"), 0.0),
                near_depth=self._finite_float(features.get("near_depth_total"), 0.0),
                queue_local_rank=self._finite_float(features.get("queue_local_rank"), 0.0),
                trend_inventory_risk_score=self._finite_float(features.get("trend_inventory_risk_score"), 0.0),
                micro_reversion_score=self._finite_float(features.get("micro_reversion_score"), 0.0),
                allow_post=int(decision.allow_post),
                allow_exposure_increase=int(decision.allow_exposure_increase),
                exposure_increasing=int(exposure_increasing),
                reason_mask_before=reason_mask_before,
                reason_mask_after=int(decision.reason_mask),
                hard_blocked=int(hard_blocked),
                order_ttl_ms=int(decision.order_ttl_ms),
            ),
        )

    def _maybe_apply_state_conditioned_quote_policy(
        self,
        *,
        side: Side,
        mid: float,
        q: float,
        baseline_price: float,
        pre_guard_price: float,
        other_side_price: float,
        max_pair_spread: float,
        can_post: bool,
        order_active: bool,
        order_pending: bool,
        decision: SidePolicyDecision,
        best_bid: float,
        best_ask: float,
    ) -> tuple[float, bool]:
        """Evaluate the shared policy on the frozen one-add/campaign surface."""

        policy = self._state_conditioned_policy
        if policy is None or not can_post or order_active or order_pending:
            return float(baseline_price), False
        campaign = self.inventory.campaign_snapshot()
        inventory_role = inventory_role_for_quote(
            side.value, float(q), float(self.cfg.lot_size)
        )
        if (
            not campaign.active
            or inventory_role != "add"
            or campaign.campaign_id <= 0
            or campaign.campaign_id in self._state_conditioned_policy_campaigns
        ):
            return float(baseline_price), False

        now = time.time()
        decision_ts_ns = time.time_ns()
        features = {
            "inventory": float(q),
            "inventory_ratio": float(
                q / max(float(self.cfg.strategy.max_inventory), 1e-9)
            ),
            "campaign_age_s": float(campaign.age_s),
            "campaign_max_abs_qty_so_far": float(campaign.max_abs_qty),
            "campaign_pnl_so_far": float(campaign.total_pnl),
            "campaign_adverse_excursion_so_far": float(
                campaign.adverse_excursion
            ),
            "campaign_exposure_increasing_fills_so_far": int(
                campaign.exposure_increasing_fills
            ),
            "campaign_reducing_fills_so_far": int(campaign.reducing_fills),
            "toxicity": float(decision.toxicity),
            "markout_ema": float(decision.markout_ema),
            "microprice_shift_bps": float(decision.microprice_shift_bps),
            "l2_quote_flip_rate": float(decision.l2_quote_flip_rate),
            "l2_book_refresh_ratio": float(decision.l2_book_refresh_ratio),
            "l2_book_cancel_ratio": float(decision.l2_book_cancel_ratio),
            "l2_near_depth_total": float(decision.l2_near_depth_total),
        }
        policy_decision = policy.decide(
            side=side.value,
            inventory_role=inventory_role,
            features=features,
            decision_ts_ns=decision_ts_ns,
            feature_ready_ts_ns=decision_ts_ns,
        )
        candidate_quote = apply_local_add_action(
            side=side.value,
            action=policy_decision.candidate_action,
            baseline_price=float(baseline_price),
            pre_guard_price=float(pre_guard_price),
            other_side_price=float(other_side_price),
            mid=float(mid),
            best_bid=float(best_bid),
            best_ask=float(best_ask),
            microprice_shift_bps=float(decision.microprice_shift_bps),
            tick=float(self.cfg.tick_size),
            max_pair_spread=float(max_pair_spread),
        )
        executed_quote = apply_local_add_action(
            side=side.value,
            action=policy_decision.action,
            baseline_price=float(baseline_price),
            pre_guard_price=float(pre_guard_price),
            other_side_price=float(other_side_price),
            mid=float(mid),
            best_bid=float(best_bid),
            best_ask=float(best_ask),
            microprice_shift_bps=float(decision.microprice_shift_bps),
            tick=float(self.cfg.tick_size),
            max_pair_spread=float(max_pair_spread),
        )
        self._state_conditioned_policy_campaigns.add(campaign.campaign_id)
        self._append_row(
            self._state_conditioned_policy_shadow_log_path,
            StateConditionedPolicyShadowLogRow(
                timestamp=f"{now:.3f}",
                symbol=self.cfg.symbol,
                policy_id=policy.artifact.policy_id,
                policy_mode=policy.mode,
                side=side.value,
                inventory_role=inventory_role,
                campaign_id=int(campaign.campaign_id),
                q=float(q),
                mid=float(mid),
                eligible=int(policy_decision.eligible),
                reason=str(policy_decision.reason),
                candidate_action=str(policy_decision.candidate_action),
                executed_action=str(policy_decision.action),
                baseline_value=float(policy_decision.baseline_value),
                candidate_value=float(policy_decision.candidate_value),
                estimated_advantage=float(policy_decision.advantage),
                feature_age_ms=float(policy_decision.feature_age_ms),
                baseline_price=float(baseline_price),
                candidate_price=float(candidate_quote.selected_price),
                executed_price=float(executed_quote.selected_price),
                action_delta_ticks=float(executed_quote.delta_ticks),
                action_effective=int(executed_quote.effective),
                clamp_reason=str(executed_quote.clamp_reason),
                allow_post=int(decision.allow_post),
                allow_exposure_increase=int(decision.allow_exposure_increase),
            ),
        )
        return float(executed_quote.selected_price), bool(executed_quote.effective)

    def _build_side_policy(
        self,
        side: Side,
        mid: float,
        q: float,
        pred: Prediction,
        quote_snapshot: Optional[QuoteDecisionSnapshot] = None,
    ) -> SidePolicyDecision:
        cfg = self.cfg
        side_name = side.value
        max_inv = max(cfg.strategy.max_inventory, 1e-9)
        inventory_ratio = min(abs(q) / max_inv, 1.0)
        tox_bid, tox_ask = self._toxicity_probs(pred)
        toxicity = tox_bid if side == Side.BUY else tox_ask
        markout_ema = self._mo_ema_bid if side == Side.BUY else self._mo_ema_ask
        metrics = (
            self._current_l2_policy_metrics(mid, quote_snapshot)
            if quote_snapshot is not None
            else self._current_l2_policy_metrics(mid)
        )
        decision = SidePolicyDecision(
            side=side_name,
            inventory_ratio=inventory_ratio,
            toxicity=toxicity,
            markout_ema=markout_ema,
            depth_age_s=metrics["depth_age_s"],
            microprice_shift_bps=metrics["microprice_shift_bps"],
            l2_quote_flip_rate=metrics["l2_quote_flip_rate"],
            l2_book_refresh_ratio=metrics["l2_book_refresh_ratio"],
            l2_book_cancel_ratio=metrics["l2_book_cancel_ratio"],
            l2_near_depth_total=metrics["l2_near_depth_total"],
        )

        exposure_increasing = (side == Side.BUY and q >= 0.0) or (side == Side.SELL and q <= 0.0)
        reducing_cooldown_enabled = float(getattr(cfg.strategy, "fill_cooldown_reducing", 0.0) or 0.0) > 0.0
        now = time.time()
        self._expire_fill_cooldown_state(side_name, now)
        cooldown_until = self._fill_cooldown_until.get(side_name, 0.0)
        quote_ctx = self._last_quote_context.get(side_name, {})
        if not isinstance(quote_ctx, dict):
            quote_ctx = {}
        decision.order_ttl_ms = int(max(0, int(quote_ctx.get("order_ttl_ms", 0) or 0)))
        common = evaluate_common_side_policy(
            CommonSidePolicyInput(
                exposure_increasing=exposure_increasing,
                fill_cooldown_active=bool(
                    cooldown_until > now
                    and (exposure_increasing or reducing_cooldown_enabled)
                ),
                inventory_ratio=inventory_ratio,
                depth_age_s=decision.depth_age_s,
                max_book_age_s=float(
                    getattr(cfg.risk, "max_exec_book_visible_age_s", 0.0)
                ),
                toxicity=toxicity,
                markout_ema=markout_ema,
                markout_spread_scale=float(getattr(cfg.strategy, "markout_spread_scale", 0.0) or 0.0),
                markout_reference=self._mo_ref,
                microprice_shift_bps=decision.microprice_shift_bps,
                l2_quote_flip_rate=decision.l2_quote_flip_rate,
                l2_book_cancel_ratio=decision.l2_book_cancel_ratio,
                l2_near_depth_total=decision.l2_near_depth_total,
                thin_depth_threshold=float(getattr(cfg.strategy, "thin_depth_threshold", 0.0) or 0.0),
                kappa_depth_baseline=float(getattr(cfg.strategy, "kappa_depth_baseline", 50.0)),
                side_adverse=bool(quote_ctx.get("side_adverse", False) or quote_ctx.get("bid_adverse", False)),
                side_adverse_pause=bool(quote_ctx.get("side_adverse_pause", False)),
                local_extreme_guard=bool(quote_ctx.get("local_extreme_guard", False)),
                local_extreme_spread_mult=float(quote_ctx.get("local_extreme_spread_mult", 1.0) or 1.0),
                local_extreme_pause=bool(quote_ctx.get("local_extreme_pause", False)),
                defense_guard=bool(quote_ctx.get("defense_guard", False)),
                defense_spread_mult=float(quote_ctx.get("defense_spread_mult", 1.0) or 1.0),
                defense_pause=bool(quote_ctx.get("defense_pause", False)),
            )
        )
        decision.allow_post = common.allow_post
        decision.allow_exposure_increase = common.allow_exposure_increase
        decision.spread_mult = common.spread_mult
        decision.size_mult = common.size_mult
        decision.reason_mask = common.reason_mask

        self._apply_buy_fill_selection_live_arm(
            side=side,
            mid=mid,
            q=q,
            decision=decision,
            quote_ctx=quote_ctx,
            pred=pred,
        )

        if not decision.allow_post:
            decision.mode = "pause"
        elif not decision.allow_exposure_increase:
            decision.mode = "defend"
            decision.reason_mask |= POLICY_REASON_EXPOSURE_ONLY
        elif decision.reason_mask != 0 or decision.spread_mult > 1.0 or decision.size_mult < 1.0:
            decision.mode = "defend"

        decision.spread_mult = max(1.0, decision.spread_mult)
        decision.size_mult = max(0.0, min(1.0, decision.size_mult))
        decision.reason_text = self._policy_reason_text(decision.reason_mask)
        return decision

    def _fill_cooldown_reset_policy(self) -> str:
        return normalize_consecutive_reset_policy(
            getattr(
                self.cfg.strategy,
                "fill_cooldown_consecutive_reset_policy",
                "opposite_fill_only",
            ),
            require_explicit=True,
        )

    def _expire_fill_cooldown_state(self, side: str, now_s: float) -> None:
        normalized = str(side).upper()
        until = float(self._fill_cooldown_until.get(normalized, 0.0) or 0.0)
        if until <= 0.0 or now_s < until:
            return
        self._fill_cooldown_until[normalized] = 0.0
        if self._fill_cooldown_reset_policy() == RESET_OPPOSITE_FILL_OR_EXPIRY:
            if normalized == "BUY":
                self._consec_buy = 0.0
            elif normalized == "SELL":
                self._consec_sell = 0.0

    def fill_cooldown_state_snapshot(self, *, now_ms: Optional[int] = None) -> dict[str, Any]:
        """Return the restart-safe wall-clock cooldown identity."""

        current_ms = int(now_ms if now_ms is not None else time.time() * 1000.0)
        return {
            "schema_version": "narrowgate_fill_cooldown_state.v1",
            "reset_policy": self._fill_cooldown_reset_policy(),
            "consec_buy": float(self._consec_buy),
            "consec_sell": float(self._consec_sell),
            "buy_remaining_ms": max(
                0, int(round(self._fill_cooldown_until["BUY"] * 1000.0)) - current_ms
            ),
            "sell_remaining_ms": max(
                0, int(round(self._fill_cooldown_until["SELL"] * 1000.0)) - current_ms
            ),
            "last_buy_fill_ts_ms": int(self._last_same_side_fill_epoch_ms["BUY"]),
            "last_sell_fill_ts_ms": int(self._last_same_side_fill_epoch_ms["SELL"]),
            "last_fill_side": str(self._last_fill_side),
            "snapshot_ts_ms": current_ms,
        }

    def _select_boolean_cooldown_duration(
        self,
        *,
        side: str,
        exposure_increasing_fill: bool,
        baseline_duration_s: float,
        campaign_age_s: float,
        fill_visible_ts_ns: int,
        snapshot_id: str,
    ) -> tuple[float, Any | None]:
        """Apply the frozen SELL policy while preserving exact control fallback."""

        policy = self._boolean_cooldown_policy
        if (
            policy is None
            or not exposure_increasing_fill
            or str(side).upper() != "SELL"
        ):
            return float(baseline_duration_s), None
        decision = policy.evaluate(
            side="SELL",
            baseline_duration_ms=int(round(float(baseline_duration_s) * 1_000.0)),
            campaign_age_s=float(campaign_age_s),
            decision_ts_ns=int(fill_visible_ts_ns),
            snapshot_id=str(snapshot_id),
        )
        if decision.action_id == "CONTROL_85N":
            return float(baseline_duration_s), decision
        return decision.duration_ms / 1_000.0, decision

    def restore_fill_cooldown_state(
        self,
        payload: dict[str, Any],
        *,
        now_ms: Optional[int] = None,
    ) -> None:
        """Restore a state captured by :meth:`fill_cooldown_state_snapshot`."""

        if str(payload.get("schema_version", "")) != "narrowgate_fill_cooldown_state.v1":
            raise ValueError("unsupported fill cooldown state schema")
        policy = normalize_consecutive_reset_policy(
            payload.get("reset_policy"), require_explicit=True
        )
        if policy != self._fill_cooldown_reset_policy():
            raise ValueError("fill cooldown state reset policy differs from runtime config")
        current_ms = int(now_ms if now_ms is not None else time.time() * 1000.0)
        self._consec_buy = max(0.0, float(payload.get("consec_buy", 0.0) or 0.0))
        self._consec_sell = max(0.0, float(payload.get("consec_sell", 0.0) or 0.0))
        for side in ("BUY", "SELL"):
            remaining = max(0, int(payload.get(f"{side.lower()}_remaining_ms", 0) or 0))
            self._fill_cooldown_until[side] = (current_ms + remaining) / 1000.0
            self._last_same_side_fill_epoch_ms[side] = int(
                payload.get(f"last_{side.lower()}_fill_ts_ms", 0) or 0
            )
            if remaining == 0 and policy == RESET_OPPOSITE_FILL_OR_EXPIRY:
                if side == "BUY":
                    self._consec_buy = 0.0
                else:
                    self._consec_sell = 0.0
        last_side = str(payload.get("last_fill_side", "") or "").upper()
        self._last_fill_side = last_side if last_side in {"BUY", "SELL"} else ""

    def _apply_sync_adjust_degrade_policy(
        self,
        q: float,
        bid_policy: SidePolicyDecision,
        ask_policy: SidePolicyDecision,
    ) -> None:
        now = time.time()
        if not self._sync_adjust_degrade_active(now):
            return

        lot_band = max(float(getattr(self.cfg, "lot_size", 0.0)) * 0.5, 1e-12)
        # 中文说明：sync degrade 只阻止扩大当前风险，不阻止减库存。
        # q>0 时 BUY 被挡但 SELL 仍可减仓；q<0 时 SELL 被挡但 BUY 仍可减仓。
        block_bid = q >= -lot_band
        block_ask = q <= lot_band
        for policy, block in ((bid_policy, block_bid), (ask_policy, block_ask)):
            if not block:
                continue
            policy.allow_exposure_increase = False
            policy.reason_mask |= POLICY_REASON_SYNC_DEGRADED | POLICY_REASON_EXPOSURE_ONLY
            policy.reason_text = self._policy_reason_text(policy.reason_mask)
            if policy.allow_post:
                policy.mode = "defend"
            else:
                policy.mode = "pause"

        if now - self._last_sync_adjust_degrade_log >= 30.0:
            logger.warning(
                "SYNC_ADJUST_DEGRADE_ACTIVE: remaining=%.0fs q=%+.6f "
                "block_bid=%s block_ask=%s",
                max(0.0, self._sync_adjust_degrade_until - now),
                q,
                block_bid,
                block_ask,
            )
            self._last_sync_adjust_degrade_log = now

    def _apply_live_local_extreme_guard_context(self, mid: float) -> None:
        strategy_cfg = self.cfg.strategy
        enabled = bool(getattr(strategy_cfg, "local_extreme_guard_enabled", False))
        fragile_ttl_s = max(0.0, float(getattr(strategy_cfg, "fragile_order_ttl_s", 0.0) or 0.0))
        if (not enabled and fragile_ttl_s <= 0.0) or not self._last_quote_context:
            return
        window_s = max(5.0, float(getattr(strategy_cfg, "local_extreme_window_s", 120.0) or 120.0))
        closes = list(getattr(self.signal, "_close_history", ()))
        n = max(1, min(len(closes), int(round(window_s))))
        rank, local_low, local_high = local_extreme_rank(
            closes[-n:],
            mid,
            tick_size=float(getattr(self.cfg, "tick_size", 0.0) or 0.0),
        )
        result = apply_local_extreme_guard_context(
            self._last_quote_context,
            mid_px=mid,
            rank=rank,
            local_low=local_low,
            local_high=local_high,
            cfg=LocalExtremeGuardConfig(
                enabled=enabled,
                window_s=window_s,
                rank_threshold=min(0.999, max(0.501, float(getattr(strategy_cfg, "local_extreme_rank_threshold", 0.80)))),
                require_thin_depth=bool(getattr(strategy_cfg, "local_extreme_require_thin_depth", True)),
                thin_depth_threshold=max(0.0, float(getattr(strategy_cfg, "local_extreme_thin_depth_threshold", 0.0) or 0.0)),
                spread_mult=max(1.0, float(getattr(strategy_cfg, "local_extreme_spread_mult", 1.0) or 1.0)),
                pause=bool(getattr(strategy_cfg, "local_extreme_pause", False)),
                fragile_order_ttl_s=fragile_ttl_s,
                kappa_depth_baseline=float(getattr(strategy_cfg, "kappa_depth_baseline", 50.0) or 50.0),
                tick_size=float(getattr(self.cfg, "tick_size", 0.0) or 0.0),
            ),
        )
        if result.bid_active or result.ask_active:
            self._last_quote_diagnostics["local_extreme_guard"] = True
            self._last_quote_diagnostics["local_extreme_rank"] = result.rank

    def _apply_flat_unilateral_ttl(
        self,
        q: float,
        bid_policy: SidePolicyDecision,
        ask_policy: SidePolicyDecision,
    ) -> None:
        ttl_s = float(getattr(self.cfg.strategy, "flat_unilateral_max_s", 0.0) or 0.0)
        flat_band = max(float(getattr(self.cfg, "lot_size", 0.0)) * 0.5, 1e-12)
        if ttl_s <= 0.0 or abs(q) > flat_band:
            self._flat_unilateral_started["BUY"] = 0.0
            self._flat_unilateral_started["SELL"] = 0.0
            self._flat_unilateral_started["BOTH"] = 0.0
            return

        bid_blocked = (
            bid_policy.allow_post
            and not bid_policy.allow_exposure_increase
            and q >= 0.0
        )
        ask_blocked = (
            ask_policy.allow_post
            and not ask_policy.allow_exposure_increase
            and q <= 0.0
        )
        bid_available = bid_policy.allow_post and bid_policy.allow_exposure_increase
        ask_available = ask_policy.allow_post and ask_policy.allow_exposure_increase

        if bid_blocked and ask_blocked:
            self._flat_unilateral_started["BUY"] = 0.0
            self._flat_unilateral_started["SELL"] = 0.0
            now = time.time()
            started = self._flat_unilateral_started.get("BOTH", 0.0)
            if started <= 0.0:
                self._flat_unilateral_started["BOTH"] = now
                return
            blocked_for = now - started
            if blocked_for < ttl_s:
                return

            # 中文说明：flat 状态下如果两侧都因 exposure_only 长时间被挡，
            # TTL 到期后释放双侧 exposure-increase，但仍保留 widen/size decay、
            # stale hard、交易所过滤等后续门控。
            for policy in (bid_policy, ask_policy):
                policy.allow_exposure_increase = True
                policy.reason_mask &= ~POLICY_REASON_EXPOSURE_ONLY
                policy.reason_mask |= POLICY_REASON_FLAT_TTL
                policy.reason_text = self._policy_reason_text(policy.reason_mask)
                policy.mode = "defend" if policy.reason_mask != 0 else "normal"

            last_log = self._last_flat_unilateral_release_log.get("BOTH", 0.0)
            if now - last_log >= max(ttl_s, 30.0):
                logger.info(
                    f"FLAT_UNILATERAL_TTL_RELEASE side=BOTH "
                    f"blocked_for={blocked_for:.1f}s ttl={ttl_s:.1f}s "
                    f"bid_reason={bid_policy.reason_text} ask_reason={ask_policy.reason_text}"
                )
                self._last_flat_unilateral_release_log["BOTH"] = now
            return

        side = None
        policy = None
        if bid_blocked and ask_available and not ask_blocked:
            side = "BUY"
            policy = bid_policy
            self._flat_unilateral_started["SELL"] = 0.0
            self._flat_unilateral_started["BOTH"] = 0.0
        elif ask_blocked and bid_available and not bid_blocked:
            side = "SELL"
            policy = ask_policy
            self._flat_unilateral_started["BUY"] = 0.0
            self._flat_unilateral_started["BOTH"] = 0.0
        else:
            self._flat_unilateral_started["BOTH"] = 0.0
            if not bid_blocked:
                self._flat_unilateral_started["BUY"] = 0.0
            if not ask_blocked:
                self._flat_unilateral_started["SELL"] = 0.0
            return

        now = time.time()
        started = self._flat_unilateral_started.get(side, 0.0)
        if started <= 0.0:
            self._flat_unilateral_started[side] = now
            return
        blocked_for = now - started
        if blocked_for < ttl_s:
            return

        # 中文说明：flat 单边锁死释放只处理“另一侧仍可报”的情况，
        # 目的是防止旧 markout/adverse 状态把系统长期变成单边 maker。
        policy.allow_exposure_increase = True
        policy.reason_mask &= ~POLICY_REASON_EXPOSURE_ONLY
        policy.reason_mask |= POLICY_REASON_FLAT_TTL
        policy.reason_text = self._policy_reason_text(policy.reason_mask)
        policy.mode = "defend" if policy.reason_mask != 0 else "normal"

        last_log = self._last_flat_unilateral_release_log.get(side, 0.0)
        if now - last_log >= max(ttl_s, 30.0):
            logger.info(
                f"FLAT_UNILATERAL_TTL_RELEASE side={side} "
                f"blocked_for={blocked_for:.1f}s ttl={ttl_s:.1f}s "
                f"reason={policy.reason_text}"
            )
            self._last_flat_unilateral_release_log[side] = now

    @staticmethod
    def _precision_from_step(step: float) -> int:
        """Derive decimal precision from step size (e.g. 0.0001 → 4)."""
        s = f"{step:.10f}".rstrip("0")
        if "." in s:
            return len(s.split(".")[1])
        return 0

    @staticmethod
    def _rest_exchange_timestamp_ns(response: Any) -> int:
        """Read an exchange event clock without substituting local receipt time."""

        if not isinstance(response, dict):
            return 0
        for field in ("transactTime", "updateTime", "workingTime"):
            value_ms = int(response.get(field, 0) or 0)
            if value_ms > 0:
                return value_ms * 1_000_000
        return 0

    def _fmt_qty(self, qty: float) -> str:
        # Floor to lot_size to match exchange rules (never round up)
        lot = self.cfg.lot_size
        floored = int(qty / lot) * lot
        return f"{floored:.{self._qty_precision}f}"

    @staticmethod
    def _floor_to_lot(qty: float, lot: float) -> float:
        if lot <= 0:
            return max(0.0, qty)
        return math.floor(max(0.0, qty) / lot + 1e-12) * lot

    @staticmethod
    def _ceil_to_lot(qty: float, lot: float) -> float:
        if lot <= 0:
            return max(0.0, qty)
        return math.ceil(max(0.0, qty) / lot - 1e-12) * lot

    def _policy_sized_qty(self, raw_size: float, base_size: float,
                          price: float, min_qty: float,
                          min_notional: float, lot: float) -> float:
        """Apply lot flooring without letting size decay erase a valid min order."""
        floored = self._floor_to_lot(raw_size, lot)
        if raw_size <= 0.0 or price <= 0.0 or lot <= 0.0:
            return floored

        min_filter_qty = max(
            self._ceil_to_lot(min_qty, lot),
            self._ceil_to_lot(min_notional / price, lot),
            lot,
        )
        base_is_valid = (
            base_size + 1e-12 >= min_filter_qty
            and base_size * price + 1e-8 >= min_notional
        )
        if base_is_valid and floored < min_filter_qty:
            return min_filter_qty
        return floored

    def _fmt_price(self, price: float) -> str:
        return f"{price:.{self._price_precision}f}"

    # ── dynamic requote interval ──

    def update_dynamic_rq(self, close: float):
        """Called once per 1s bar from SignalEngine to update fast/slow vol EMAs.
        Uses the same EMA approach as the backtest _simulate_ml_core."""
        if self._prev_close <= 0.0:
            self._prev_close = close
            return
        ret_sq = (close - self._prev_close) ** 2
        self._prev_close = close
        alpha_fast = 0.067   # 10s half-life
        alpha_slow = 0.011   # 60s half-life
        if not self._dynamic_rq_ready:
            self._ema_var_fast = ret_sq
            self._ema_var_slow = ret_sq
            self._dynamic_rq_ready = True
        else:
            self._ema_var_fast = alpha_fast * ret_sq + (1 - alpha_fast) * self._ema_var_fast
            self._ema_var_slow = alpha_slow * ret_sq + (1 - alpha_slow) * self._ema_var_slow

    def _update_ber(self, close: float):
        """Update BER (Book Exhaustion Rate) EMAs from trade intensity per 1s bar."""
        last_feat = self.signal._feat_history[-1] if self.signal._feat_history else {}
        ti = last_feat.get("trade_intensity_60s", 50.0)
        alpha_fast = 0.13   # ~5s half-life
        alpha_slow = 0.011  # ~60s half-life
        if not self._ber_ready:
            self._ber_ema_fast = ti
            self._ber_ema_slow = ti
            self._ber_ready = True
        else:
            self._ber_ema_fast = alpha_fast * ti + (1 - alpha_fast) * self._ber_ema_fast
            self._ber_ema_slow = alpha_slow * ti + (1 - alpha_slow) * self._ber_ema_slow
        ber_thresh = getattr(self.cfg.strategy, 'ber_guard_thresh', 0.0)
        if ber_thresh > 0 and self._ber_ema_slow > 1e-6:
            self._ber_active = (self._ber_ema_fast / self._ber_ema_slow) > ber_thresh
        else:
            self._ber_active = False

    def _effective_rq_interval(self) -> float:
        """Return current requote interval (seconds).
        Static if rq_min/rq_max not configured; dynamic otherwise.
        Maps fast/slow volatility ratio to [rq_min, rq_max] exponentially."""
        cfg = self.cfg.strategy
        rq_min = getattr(cfg, 'rq_min', 0.0)
        rq_max = getattr(cfg, 'rq_max', 0.0)
        if rq_min <= 0 or rq_max <= 0 or rq_min >= rq_max:
            return cfg.requote_interval
        if not self._dynamic_rq_ready or self._ema_var_slow < 1e-12:
            return cfg.requote_interval
        vol_ratio = self._ema_var_fast / self._ema_var_slow
        vol_ratio = max(0.0, min(vol_ratio, 2.0))
        log_ratio = math.log(rq_min / rq_max)
        rq = rq_max * math.exp(log_ratio * vol_ratio)
        return max(rq_min, min(rq_max, rq))

    def _quote_snapshot_contract_error(
        self,
        snapshot: QuoteDecisionSnapshot,
        *,
        use_bar_pricing: bool,
        post_only_guard: QuotePostOnlyGuard,
    ) -> str:
        """Return the first decision-snapshot invariant violation."""

        if snapshot.capture_ts_ns <= 0:
            return "missing_capture_timestamp"
        if not snapshot.valid:
            return snapshot.invalid_reason or "invalid_depth_snapshot"
        if (
            post_only_guard.best_bid <= 0.0
            or post_only_guard.best_ask <= post_only_guard.best_bid
        ):
            return "missing_or_crossed_post_only_bbo"
        if snapshot.depth_visible_age_s < 0.0:
            return "depth_receive_after_snapshot"
        if snapshot.depth_source_lag_s < 0.0:
            return "depth_exchange_after_receive"
        max_source_lag_s = float(
            getattr(self.cfg.risk, "max_exec_book_source_lag_s", 0.0) or 0.0
        )
        if (
            max_source_lag_s > 0.0
            and snapshot.depth_source_lag_s > max_source_lag_s
        ):
            return "stale_depth_source_lag"

        expected_mid = 0.5 * (snapshot.best_bid + snapshot.best_ask)
        price_tol = max(float(self.cfg.tick_size) * 1e-9, abs(expected_mid) * 1e-12)
        if abs(snapshot.mid - expected_mid) > price_tol:
            return "depth_mid_identity_mismatch"

        depth = quote_depth_from_book(snapshot)
        if not depth.has_book:
            return "normalized_depth_empty"
        microprice = microprice_from_book(depth.bids, depth.asks, levels=3)
        if not math.isfinite(microprice):
            return "nonfinite_microprice"
        if microprice < snapshot.best_bid - price_tol or microprice > snapshot.best_ask + price_tol:
            return "microprice_outside_depth_top"
        if use_bar_pricing and (
            not math.isfinite(snapshot.bar_pricing_mid)
            or snapshot.bar_pricing_mid <= 0.0
        ):
            return "missing_or_invalid_frozen_bar_pricing_mid"
        return ""

    def _post_only_guard_for_snapshot(
        self,
        snapshot: QuoteDecisionSnapshot,
    ) -> QuotePostOnlyGuard:
        risk = self.cfg.risk
        return snapshot.post_only_guard(
            max_visible_age_s=float(
                getattr(risk, "max_exec_book_visible_age_s", 0.0) or 0.0
            ),
            max_source_lag_s=float(
                getattr(risk, "max_exec_book_source_lag_s", 0.0) or 0.0
            ),
        )

    def _quote_routing_contract_error(
        self,
        *,
        bid_price: float,
        ask_price: float,
        can_bid: bool,
        can_ask: bool,
        post_only_guard: QuotePostOnlyGuard,
    ) -> str:
        tick = float(self.cfg.tick_size)
        bid_tick_error = abs(bid_price / tick - round(bid_price / tick))
        ask_tick_error = abs(ask_price / tick - round(ask_price / tick))
        if bid_tick_error > 1e-7 or ask_tick_error > 1e-7:
            return (
                "non_executable_tick_price:"
                f"bid_error={bid_tick_error}:ask_error={ask_tick_error}"
            )
        price_tol = tick * 1e-9
        if can_bid and bid_price >= post_only_guard.best_ask - price_tol:
            return "post_only_buy_crosses_frozen_guard"
        if can_ask and ask_price <= post_only_guard.best_bid + price_tol:
            return "post_only_sell_crosses_frozen_guard"
        return ""

    def _log_quote_snapshot_integrity(
        self,
        snapshot: QuoteDecisionSnapshot,
        post_only_guard: QuotePostOnlyGuard,
        *,
        status: str,
        pricing_mid: float = 0.0,
        final_bid: float = 0.0,
        final_ask: float = 0.0,
    ) -> None:
        path = str(getattr(self, "_quote_snapshot_integrity_log_path", "") or "")
        if not path:
            return
        tick = max(float(getattr(self.cfg, "tick_size", 0.0) or 0.0), 1e-12)
        identity_error_ticks = 0.0
        if final_bid > 0.0 and final_ask > 0.0 and pricing_mid > 0.0:
            identity_error_ticks = (
                (final_ask - pricing_mid)
                + (pricing_mid - final_bid)
                - (final_ask - final_bid)
            ) / tick
        bid_action = str(getattr(self, "_last_bid_action", "none"))
        ask_action = str(getattr(self, "_last_ask_action", "none"))
        price_tol = tick * 1e-9
        post_only_violations = 0
        if bid_action in {"place", "replace"} and final_bid >= post_only_guard.best_ask - price_tol:
            post_only_violations += 1
        if ask_action in {"place", "replace"} and final_ask <= post_only_guard.best_bid + price_tol:
            post_only_violations += 1
        orders = getattr(self, "orders", None)
        active_orders = int(orders.active_count()) if orders is not None else 0
        self._append_row(
            path,
            QuoteSnapshotIntegrityLogRow(
                timestamp=f"{time.time():.6f}",
                symbol=str(self.cfg.symbol),
                requote_id=int(getattr(self, "_requote_count", 0)),
                status=str(status),
                use_bar_pricing=int(
                    bool(getattr(self.cfg.strategy, "use_bar_pricing", False))
                ),
                snapshot_valid=int(bool(snapshot.valid)),
                invalid_reason=str(snapshot.invalid_reason),
                capture_ts_ns=int(snapshot.capture_ts_ns),
                market_generation=int(snapshot.market_generation),
                depth_generation=int(snapshot.depth_generation),
                book_ticker_generation=int(snapshot.book_ticker_generation),
                depth_bid=float(snapshot.best_bid),
                depth_ask=float(snapshot.best_ask),
                book_ticker_bid=float(snapshot.book_ticker_bid),
                book_ticker_ask=float(snapshot.book_ticker_ask),
                guard_bid=float(post_only_guard.best_bid),
                guard_ask=float(post_only_guard.best_ask),
                guard_source=str(post_only_guard.source),
                guard_fallback_reason=str(post_only_guard.fallback_reason),
                depth_total_age_s=float(snapshot.depth_age_s),
                depth_visible_age_s=float(snapshot.depth_visible_age_s),
                depth_source_lag_s=float(snapshot.depth_source_lag_s),
                book_ticker_visible_age_s=float(
                    snapshot.book_ticker_visible_age_s
                ),
                book_ticker_source_lag_s=float(
                    snapshot.book_ticker_source_lag_s
                ),
                snapshot_lock_wait_us=float(snapshot.lock_wait_ns) / 1_000.0,
                snapshot_lock_hold_us=float(snapshot.lock_hold_ns) / 1_000.0,
                bar_pricing_mid=float(snapshot.bar_pricing_mid),
                pricing_mid=float(pricing_mid),
                final_bid=float(final_bid),
                final_ask=float(final_ask),
                quote_identity_error_ticks=float(identity_error_ticks),
                post_only_violation_count=int(post_only_violations),
                consecutive_snapshot_blocks=int(
                    getattr(self, "_consecutive_quote_snapshot_blocks", 0)
                ),
                rest_cancel_count=int(
                    getattr(self, "_perf_rest_cancel_count", 0)
                    + getattr(self, "_perf_rest_cancel_all_count", 0)
                ),
                active_orders=active_orders,
                bid_action=bid_action,
                ask_action=ask_action,
            ),
        )

    def _block_invalid_quote_snapshot(
        self,
        snapshot: QuoteDecisionSnapshot,
        reason: str,
        post_only_guard: Optional[QuotePostOnlyGuard] = None,
    ) -> None:
        """Fail closed without turning a feed race into an executable quote."""

        self._consecutive_quote_snapshot_blocks = int(
            getattr(self, "_consecutive_quote_snapshot_blocks", 0)
        ) + 1
        active_count = self.orders.active_count()
        if active_count > 0:
            self._cancel_all_orders()
        now = time.time()
        if now - self._last_quote_snapshot_block_log < 10.0:
            return
        logger.error(
            "QUOTE_SNAPSHOT_BLOCK reason=%s active_orders=%d market_gen=%d "
            "depth_gen=%d book_gen=%d depth_bid=%.8f depth_ask=%.8f "
            "depth_exchange_ts_ms=%d depth_receive_ts_ns=%d "
            "book_bid=%.8f book_ask=%.8f book_exchange_ts_ms=%d "
            "book_receive_ts_ns=%d capture_ts_ns=%d "
            "depth_visible_age_s=%.6f depth_source_lag_s=%.6f "
            "book_visible_age_s=%.6f book_source_lag_s=%.6f "
            "guard_source=%s guard_fallback=%s lock_wait_us=%.3f lock_hold_us=%.3f "
            "consecutive_blocks=%d",
            reason,
            active_count,
            snapshot.market_generation,
            snapshot.depth_generation,
            snapshot.book_ticker_generation,
            snapshot.best_bid,
            snapshot.best_ask,
            snapshot.depth_exchange_ts_ms,
            snapshot.depth_receive_ts_ns,
            snapshot.book_ticker_bid,
            snapshot.book_ticker_ask,
            snapshot.book_ticker_exchange_ts_ms,
            snapshot.book_ticker_receive_ts_ns,
            snapshot.capture_ts_ns,
            snapshot.depth_visible_age_s,
            snapshot.depth_source_lag_s,
            snapshot.book_ticker_visible_age_s,
            snapshot.book_ticker_source_lag_s,
            post_only_guard.source if post_only_guard is not None else "unavailable",
            post_only_guard.fallback_reason if post_only_guard is not None else "",
            snapshot.lock_wait_ns / 1_000.0,
            snapshot.lock_hold_ns / 1_000.0,
            self._consecutive_quote_snapshot_blocks,
        )
        self._last_quote_snapshot_block_log = now

    # ── main loop ──

    def tick(self):
        """
        Called every iteration of the main event loop.
        Checks if it's time to requote and executes if so.
        """
        now = time.time()

        # Markout is a wall-clock observation, not a requote-side effect.  The
        # main loop calls tick much more frequently than the 5-10s quote clock,
        # so resolving here keeps live close to the configured horizon.
        markout_mid = self.signal.mid_price
        if markout_mid > 0.0:
            self._resolve_pending_markouts(now, markout_mid)
        self._evaluate_dynamic_fill_hazard_shadow(time.time_ns())

        # Cooldown after loss
        if now < self._cooldown_until:
            if self.orders.has_active_orders() and now - self._last_cooldown_cancel_time >= 5.0:
                self._cancel_all_orders()
                self._last_cooldown_cancel_time = now
            return
        # Reset consecutive losses after cooldown expires so we don't
        # immediately re-enter cooldown (was causing infinite cooldown loop)
        if self._cooldown_until > 0:
            self.inventory.reset_consecutive_losses()
            self._cooldown_until = 0.0
            self._last_cooldown_cancel_time = 0.0

        # Requote interval check (dynamic or static)
        rq_interval = self._effective_rq_interval()
        if now - self._last_requote_time < rq_interval:
            return

        # Warmup check
        if not self.signal.is_warmed_up:
            wc = self.signal._warmup_count
            if wc == 0 or wc % 60 == 0:
                logger.info(f"Warming up: {wc}/300 bars")
            self._last_requote_time = now  # avoid spamming warmup check
            return

        self._requote()

    def _requote(self):
        """Core requote logic — compute quotes, manage orders, risk check."""
        requote_start_perf = time.perf_counter()
        timings: dict[str, float] = {}
        self._reset_perf_rest_counters()
        self._last_bid_action = "none"
        self._last_ask_action = "none"
        self._last_cpp_routing_used = 0
        requote_start_ts_ns = time.time_ns()
        now = requote_start_ts_ns / 1_000_000_000.0
        self._last_requote_time = now
        self._requote_count += 1
        cfg = self.cfg
        step_start = time.perf_counter()
        self._check_sync_adjust_degrade(now)
        timings["sync_check_us"] = (time.perf_counter() - step_start) * 1_000_000.0

        # 1. Get ML prediction
        step_start = time.perf_counter()
        pred = self.signal.compute_signal()
        timings["signal_compute_us"] = (time.perf_counter() - step_start) * 1_000_000.0
        use_bar_pricing = getattr(cfg.strategy, 'use_bar_pricing', False)
        quote_snapshot = self.signal.quote_decision_snapshot()
        self._last_quote_decision_snapshot = quote_snapshot
        post_only_guard = self._post_only_guard_for_snapshot(quote_snapshot)
        self._last_post_only_guard = post_only_guard
        # The actionable decision clock begins only after prediction and the
        # immutable execution-book view have both been assembled.
        decision_start_ts_ns = quote_snapshot.capture_ts_ns
        snapshot_error = self._quote_snapshot_contract_error(
            quote_snapshot,
            use_bar_pricing=bool(use_bar_pricing),
            post_only_guard=post_only_guard,
        )
        if snapshot_error:
            self._block_invalid_quote_snapshot(
                quote_snapshot,
                snapshot_error,
                post_only_guard,
            )
            self._log_quote_snapshot_integrity(
                quote_snapshot,
                post_only_guard,
                status=f"blocked:{snapshot_error}",
            )
            self._log_live_perf_telemetry(
                requote_start_perf=requote_start_perf,
                status="invalid_quote_snapshot",
                q=self.inventory.net_position,
                timings=timings,
            )
            return

        max_book_visible_age = float(
            getattr(cfg.risk, "max_exec_book_visible_age_s", 0.0) or 0.0
        )
        if max_book_visible_age > 0.0:
            step_start = time.perf_counter()
            book_visible_age = quote_snapshot.depth_visible_age_s
            timings["stale_check_us"] = (
                time.perf_counter() - step_start
            ) * 1_000_000.0
            if book_visible_age > max_book_visible_age:
                self._block_stale_quote_data(
                    book_visible_age,
                    max_book_visible_age,
                )
                self._log_quote_snapshot_integrity(
                    quote_snapshot,
                    post_only_guard,
                    status="blocked:stale_depth_visible_age",
                )
                self._log_live_perf_telemetry(
                    requote_start_perf=requote_start_perf,
                    status="stale_book",
                    q=self.inventory.net_position,
                    timings=timings,
                )
                return
        self._consecutive_quote_snapshot_blocks = 0

        # v2.0: use_bar_pricing → 用1s bar close作mid (和回测一致)
        if use_bar_pricing:
            mid = quote_snapshot.bar_pricing_mid
        else:
            mid = quote_snapshot.mid
        if mid <= 0:
            logger.warning("No mid price available, skipping requote")
            self._log_live_perf_telemetry(
                requote_start_perf=requote_start_perf,
                status="no_mid",
                q=self.inventory.net_position,
                timings=timings,
            )
            return

        # 2. Get current position
        q = self.inventory.net_position
        snap = self.inventory.snapshot
        self._log_inventory_campaign_shadow(now, mid, q)

        # 3. Catastrophic circuit breaker (σ-scaled safety net)
        # Normal exits happen through urgency-based quote asymmetry,
        # not hard TP/SL (which would be CTA, not maker behavior).
        if snap.state == PositionState.OPEN and abs(q) > 1e-8:
            cb_sigma = cfg.risk.circuit_breaker_sigma
            if cb_sigma > 0:
                vol_roll = self.signal.rolling_variance
                if vol_roll > 1e-10:
                    loss_threshold = circuit_breaker_loss_threshold(
                        vol_roll,
                        getattr(cfg.risk, "pnl_volatility_horizon_s", 1.0),
                        q,
                        cb_sigma,
                    )
                    if circuit_breaker_triggered(
                        snap.unrealized_pnl,
                        vol_roll,
                        getattr(cfg.risk, "pnl_volatility_horizon_s", 1.0),
                        q,
                        cb_sigma,
                    ):
                        logger.warning(
                            f"CIRCUIT_BREAKER: upnl={snap.unrealized_pnl:.2f} "
                            f"< -{cb_sigma}σ (loss_threshold={loss_threshold:.2f})"
                        )
                        self._handle_position_timeout(q, mid)
                        self._log_live_perf_telemetry(
                            requote_start_perf=requote_start_perf,
                            status="circuit_breaker",
                            mid=mid,
                            q=q,
                            timings=timings,
                        )
                        return

        # 4. Position timeout check — also handle ongoing TIMEOUT_CLOSING
        if snap.state == PositionState.OPEN and self.inventory.is_timeout:
            self._handle_position_timeout(q, mid)
            self._log_live_perf_telemetry(
                requote_start_perf=requote_start_perf,
                status="position_timeout",
                mid=mid,
                q=q,
                timings=timings,
            )
            return

        if snap.state == PositionState.TIMEOUT_CLOSING:
            # Still closing — only allow orders that reduce position
            if abs(q) < 1e-8:
                # Fully closed, reset state
                self.inventory._update_state()
                self._close_start_time = 0.0
                self._log_live_perf_telemetry(
                    requote_start_perf=requote_start_perf,
                    status="closing_flat",
                    mid=mid,
                    q=q,
                    timings=timings,
                )
                return
            # Ensure close timer is set after a timeout transition.
            if self._close_start_time <= 0:
                self._close_start_time = time.time()
            self._handle_closing_requote(
                q,
                mid,
                pred,
                quote_snapshot=quote_snapshot,
                post_only_guard=post_only_guard,
            )
            self._log_live_perf_telemetry(
                requote_start_perf=requote_start_perf,
                status="closing_requote",
                mid=mid,
                q=q,
                timings=timings,
            )
            return

        # 5. Risk checks
        step_start = time.perf_counter()
        risk_ok = self._risk_check(snap, mid)
        timings["risk_check_us"] = (time.perf_counter() - step_start) * 1_000_000.0
        if not risk_ok:
            self._log_live_perf_telemetry(
                requote_start_perf=requote_start_perf,
                status="risk_block",
                mid=mid,
                q=q,
                timings=timings,
            )
            return

        # 6. Compute AS quotes with ML enhancement
        step_start = time.perf_counter()
        bid_price, ask_price, spread = self._compute_quotes(
            quote_snapshot,
            q,
            pred,
            pricing_mid=mid,
            post_only_guard=post_only_guard,
        )
        timings["compute_quotes_us"] = (time.perf_counter() - step_start) * 1_000_000.0

        # 7. Cancel and replace orders
        step_start = time.perf_counter()
        decision_group_id = (
            f"{cfg.symbol}:{os.getpid()}:{decision_start_ts_ns}:"
            f"{self._requote_count}"
        )
        bid_updated, ask_updated = self._update_orders(
            mid,
            bid_price,
            ask_price,
            q,
            pred,
            quote_snapshot=quote_snapshot,
            post_only_guard=post_only_guard,
            decision_group_id=decision_group_id,
            decision_start_ts_ns=decision_start_ts_ns,
        )
        timings["update_orders_us"] = (time.perf_counter() - step_start) * 1_000_000.0
        self._log_quote_snapshot_integrity(
            quote_snapshot,
            post_only_guard,
            status="ok",
            pricing_mid=mid,
            final_bid=float(getattr(self, "_last_routed_bid_price", bid_price)),
            final_ask=float(getattr(self, "_last_routed_ask_price", ask_price)),
        )
        self._log_live_perf_telemetry(
            requote_start_perf=requote_start_perf,
            status="ok",
            mid=mid,
            q=q,
            timings=timings,
        )

        if self._requote_count % 30 == 0:  # log every 5 minutes
            bid_status = "NEW" if bid_updated else "KEEP"
            ask_status = "NEW" if ask_updated else "KEEP"
            logger.info(
                f"REQUOTE #{self._requote_count} mid={mid:.1f} "
                f"bid={bid_price:.1f}[{bid_status}] ask={ask_price:.1f}[{ask_status}] "
                f"spread={spread:.1f}({spread/mid*10000:.1f}bps) pos={q:+.4f} "
                f"dir={pred.dir_10s:.3f} vol={pred.vol_10s:.6f}"
            )
        else:
            logger.debug(
                f"REQUOTE #{self._requote_count} mid={mid:.1f} "
                f"bid={bid_price:.1f} ask={ask_price:.1f} "
                f"spread={spread:.1f}({spread/mid*10000:.1f}bps)"
            )

    def _adverse_markout_pause_threshold(self) -> float:
        strategy = self.cfg.strategy
        threshold = abs(float(getattr(strategy, "adverse_markout_pause_threshold", 0.0)))
        if threshold <= 0.0:
            threshold = abs(float(getattr(strategy, "adverse_markout_threshold", 5.0)))
        return max(threshold, 1e-9)

    def _side_markout_ema(self, side: str) -> float:
        return self._mo_ema_bid if side == "BUY" else self._mo_ema_ask

    def _apply_markout_wallclock_decay(self, now: float) -> None:
        tau_s = float(getattr(self.cfg.strategy, "adverse_markout_decay_tau_s", 0.0))
        last = float(getattr(self, "_mo_last_decay_time", 0.0))
        if tau_s > 0.0 and last > 0.0 and now > last:
            dt = now - last
            decay = math.exp(-dt / tau_s)
            self._mo_ema_bid *= decay
            self._mo_ema_ask *= decay
            self._mo_ema_all *= decay
        self._mo_last_decay_time = now

    def _resolve_pending_markouts(self, now: float, mid: float) -> None:
        self._apply_markout_wallclock_decay(now)
        strategy = self.cfg.strategy
        mo_span = int(getattr(strategy, "markout_ema_span_fills", 0) or 0)
        mo_ss = float(getattr(strategy, "markout_spread_scale", 0.0))
        horizon_s = max(1e-6, float(getattr(strategy, "markout_horizon_s", 10.0)))
        if mo_span <= 0 or mo_ss <= 0.0 or not self._mo_pending:
            return

        mo_alpha = 2.0 / (mo_span + 1.0)
        pending = []
        for fill_time, fill_price, fill_side in self._mo_pending:
            if now - fill_time < horizon_s:
                pending.append((fill_time, fill_price, fill_side))
                continue
            markout = mid - fill_price if fill_side == "BUY" else fill_price - mid
            self._mo_ema_all = mo_alpha * markout + (1.0 - mo_alpha) * self._mo_ema_all
            if fill_side == "BUY":
                self._mo_ema_bid = mo_alpha * markout + (1.0 - mo_alpha) * self._mo_ema_bid
                self._extend_markout_pause_until("BUY", now)
            else:
                self._mo_ema_ask = mo_alpha * markout + (1.0 - mo_alpha) * self._mo_ema_ask
                self._extend_markout_pause_until("SELL", now)
        self._mo_pending = pending

    def _extend_markout_pause_until(self, side: str, now: float) -> None:
        strategy = self.cfg.strategy
        if not bool(getattr(strategy, "adverse_markout_pause_hybrid", False)):
            return
        if not bool(getattr(strategy, "adverse_pause", True)):
            return
        threshold = self._adverse_markout_pause_threshold()
        ema = self._side_markout_ema(side)
        if ema >= -threshold:
            return
        base_s = max(0.0, float(getattr(strategy, "adverse_markout_pause_base_s", 120.0)))
        min_s = max(0.0, float(getattr(strategy, "adverse_markout_pause_min_s", 120.0)))
        max_s = max(min_s, float(getattr(strategy, "adverse_markout_pause_max_s", 900.0)))
        ttl_s = base_s * abs(ema) / threshold if threshold > 0.0 else base_s
        ttl_s = max(min_s, min(max_s, ttl_s))
        until = now + ttl_s
        if until > self._mo_pause_until.get(side, 0.0):
            self._mo_pause_until[side] = until
            last_log = self._mo_last_pause_log.get(side, 0.0)
            if now - last_log >= 30.0:
                logger.info(
                    "ADVERSE_MARKOUT_PAUSE side=%s ema=%.3f threshold=%.3f ttl=%.0fs until=%.0f",
                    side, ema, threshold, ttl_s, until,
                )
                self._mo_last_pause_log[side] = now

    def _markout_pause_latch_active(self, side: str, now: float) -> bool:
        strategy = self.cfg.strategy
        if not bool(getattr(strategy, "adverse_markout_pause_hybrid", False)):
            return False
        threshold = self._adverse_markout_pause_threshold()
        return self._side_markout_ema(side) < -threshold and now < self._mo_pause_until.get(side, 0.0)

    def _compute_quotes(
        self,
        quote_snapshot: QuoteDecisionSnapshot,
        q: float,
        pred: Prediction,
        *,
        pricing_mid: Optional[float] = None,
        post_only_guard: Optional[QuotePostOnlyGuard] = None,
    ) -> tuple:
        """
        Shared quote core wrapper.

        Live owns data freshness, model lookup, and logging; strategy.quote_core
        owns the pure quote math used by both live and tick replay.
        """
        cfg = self.cfg
        use_bar_pricing = bool(getattr(cfg.strategy, "use_bar_pricing", False))
        guard = post_only_guard or self._post_only_guard_for_snapshot(
            quote_snapshot
        )
        snapshot_error = self._quote_snapshot_contract_error(
            quote_snapshot,
            use_bar_pricing=use_bar_pricing,
            post_only_guard=guard,
        )
        if snapshot_error:
            raise RuntimeError(f"invalid quote decision snapshot: {snapshot_error}")
        mid = float(
            pricing_mid
            if pricing_mid is not None
            else quote_snapshot.mid
        )
        if not use_bar_pricing:
            price_tol = max(float(cfg.tick_size) * 1e-9, abs(quote_snapshot.mid) * 1e-12)
            if abs(mid - quote_snapshot.mid) > price_tol:
                raise RuntimeError(
                    "depth-pricing mid does not belong to quote snapshot: "
                    f"mid={mid} snapshot_mid={quote_snapshot.mid}"
                )

        now = time.time()

        fill_model = _get_fill_model(self._model_dir)
        snap = self.inventory.snapshot
        tox_bid, tox_ask = self._toxicity_probs(pred)
        p3_delta_star = 0.0
        p3_kappa_eff = 0.0
        if fill_model is not None:
            cache_key = (self._model_dir, id(fill_model))
            cache = getattr(self, "_fill_model_quote_cache", None)
            if cache is None or cache[0] != cache_key:
                delta_star = fill_model.optimal_delta()
                cache = (cache_key, delta_star, fill_model.effective_kappa(delta_star))
                self._fill_model_quote_cache = cache
            p3_delta_star = float(cache[1])
            p3_kappa_eff = float(cache[2])
        p3_kappa_eff_override = max(0.0, float(getattr(cfg.strategy, "p3_kappa_eff_override", 0.0) or 0.0))
        if p3_kappa_eff_override > 0.0:
            p3_kappa_eff = p3_kappa_eff_override
        trade_intensity = getattr(getattr(cfg, "regime", None), "liq_baseline", 200.0)
        if self.signal._feat_history:
            trade_intensity = self.signal._feat_history[-1].get("trade_intensity_60s", trade_intensity)
        depth_raw = quote_snapshot
        hold_time = max(0.0, now - snap.open_time) if getattr(snap, "open_time", 0.0) > 0 else 0.0
        quote_state = QuoteState(
            mid=mid,
            inventory=q,
            sigma_sq=self.signal.rolling_variance,
            trade_intensity=trade_intensity,
            best_bid=guard.best_bid,
            best_ask=guard.best_ask,
            ber_active=self._ber_active,
            mo_ema_all=self._mo_ema_all,
            mo_ema_bid=self._mo_ema_bid,
            mo_ema_ask=self._mo_ema_ask,
            bid_adverse_markout_pause_latch=self._markout_pause_latch_active("BUY", now),
            ask_adverse_markout_pause_latch=self._markout_pause_latch_active("SELL", now),
            mo_ref=self._mo_ref,
            position_open=snap.state == PositionState.OPEN,
            hold_time_s=hold_time,
            unrealized_pnl=float(getattr(snap, "unrealized_pnl", 0.0)),
        )
        quote_cfg_key = (id(cfg), p3_delta_star, p3_kappa_eff)
        quote_cfg_cache = getattr(self, "_quote_core_config_cache", None)
        if quote_cfg_cache is None or quote_cfg_cache[0] != quote_cfg_key:
            quote_cfg_cache = (
                quote_cfg_key,
                quote_core_config_from_live_config(
                    cfg,
                    p3_delta_star=p3_delta_star,
                    p3_kappa_eff=p3_kappa_eff,
                ),
            )
            self._quote_core_config_cache = quote_cfg_cache
        quote_cfg = quote_cfg_cache[1]
        quote_pred = QuotePrediction(
            dir_10s=pred.dir_10s,
            vol_10s=pred.vol_10s,
            ret_10s=getattr(pred, "ret_10s", 0.0),
            tox_bid=tox_bid,
            tox_ask=tox_ask,
        )
        require_full_quote_context = bool(
            getattr(cfg.strategy, "buy_fill_selection_shadow_enabled", False)
            or getattr(cfg.strategy, "buy_fill_selection_live_enabled", False)
        )
        quote_depth = quote_depth_from_book(depth_raw)
        result = compute_quote_core_live(
            quote_state,
            quote_cfg,
            quote_pred,
            quote_depth,
            require_full_context=require_full_quote_context,
        )
        if (
            bool(getattr(cfg.strategy, "ber_exposure_add_only", False))
            and bool(quote_state.ber_active)
        ):
            bypass_result = compute_quote_core_live(
                replace(quote_state, ber_active=False),
                quote_cfg,
                quote_pred,
                quote_depth,
                require_full_context=require_full_quote_context,
            )
            result = compose_ber_exposure_add_only_quote(
                ber_quote=result,
                bypass_quote=bypass_result,
                inventory=q,
                target_buy_quantity=float(cfg.strategy.order_size),
                target_sell_quantity=float(cfg.strategy.order_size),
            )
        bid_price = result.bid_price
        ask_price = result.ask_price
        self._last_quote_context = result.quote_context
        quote_ts_ms = int(now * 1000.0)
        for _side_ctx in self._last_quote_context.values():
            if isinstance(_side_ctx, dict):
                _side_ctx.setdefault("quote_ts_ms", quote_ts_ms)
                _side_ctx.setdefault(
                    "quote_snapshot_market_generation",
                    quote_snapshot.market_generation,
                )
                _side_ctx.setdefault(
                    "quote_snapshot_depth_generation",
                    quote_snapshot.depth_generation,
                )
                _side_ctx.setdefault(
                    "quote_snapshot_book_ticker_generation",
                    quote_snapshot.book_ticker_generation,
                )
                _side_ctx.setdefault(
                    "quote_snapshot_depth_exchange_ts_ms",
                    quote_snapshot.depth_exchange_ts_ms,
                )
                _side_ctx.setdefault(
                    "quote_snapshot_depth_receive_ts_ns",
                    quote_snapshot.depth_receive_ts_ns,
                )
        diag = result.diagnostics
        self._last_quote_diagnostics = dict(diag)
        self._last_quote_diagnostics.update(
            {
                "quote_snapshot_market_generation": quote_snapshot.market_generation,
                "quote_snapshot_depth_generation": quote_snapshot.depth_generation,
                "quote_snapshot_book_ticker_generation": quote_snapshot.book_ticker_generation,
                "quote_snapshot_depth_bid": quote_snapshot.best_bid,
                "quote_snapshot_depth_ask": quote_snapshot.best_ask,
                "quote_snapshot_book_ticker_bid": quote_snapshot.book_ticker_bid,
                "quote_snapshot_book_ticker_ask": quote_snapshot.book_ticker_ask,
                "quote_snapshot_depth_exchange_ts_ms": quote_snapshot.depth_exchange_ts_ms,
                "quote_snapshot_depth_receive_ts_ns": quote_snapshot.depth_receive_ts_ns,
                "quote_snapshot_book_ticker_exchange_ts_ms": quote_snapshot.book_ticker_exchange_ts_ms,
                "quote_snapshot_book_ticker_receive_ts_ns": quote_snapshot.book_ticker_receive_ts_ns,
                "quote_snapshot_capture_ts_ns": quote_snapshot.capture_ts_ns,
                "quote_snapshot_depth_visible_age_s": quote_snapshot.depth_visible_age_s,
                "quote_snapshot_depth_source_lag_s": quote_snapshot.depth_source_lag_s,
                "quote_snapshot_book_ticker_visible_age_s": quote_snapshot.book_ticker_visible_age_s,
                "quote_snapshot_book_ticker_source_lag_s": quote_snapshot.book_ticker_source_lag_s,
                "quote_snapshot_guard_source": guard.source,
                "quote_snapshot_guard_fallback_reason": guard.fallback_reason,
                "quote_snapshot_lock_wait_us": quote_snapshot.lock_wait_ns / 1_000.0,
                "quote_snapshot_lock_hold_us": quote_snapshot.lock_hold_ns / 1_000.0,
            }
        )
        self._apply_live_local_extreme_guard_context(mid)

        # ── Final quote diagnostic (every 6 requotes ≈ 1 min) ──
        if self._requote_count % 6 == 0:
            _max_spread = f"{diag.get('max_spread', 0.0):.2f}" if diag.get("max_spread", 0.0) > 0 else "off"
            bid_dist = mid - bid_price
            ask_dist = ask_price - mid
            total_sp = ask_price - bid_price
            skew_pct = (ask_dist - bid_dist) / total_sp * 100 if total_sp > 0.1 else 0.0
            logger.info(
                f"QUOTE_DBG mid={mid:.1f} fair={diag.get('fair', mid):.1f} "
                f"r={diag.get('reservation_price', mid):.1f} "
                f"dir={diag.get('dir_signal', 0.0):+.3f} ret10={getattr(pred, 'ret_10s', 0.0):+.7f} "
                f"r_shift={diag.get('r_shift', 0.0):+.2f} clamp=±{diag.get('rs_clamp', 0.0):.2f} "
                f"sigma_sq_raw={diag.get('sigma_sq_raw', 0.0):.4f} "
                f"sigma_sq_blended={diag.get('sigma_sq_blended', 0.0):.4f} "
                f"delta_raw={diag.get('delta_raw', 0.0):.2f} "
                f"delta_after_regime={diag.get('delta_after_regime', 0.0):.2f} "
                f"delta_pre_cap={diag.get('delta_pre_cap', 0.0):.2f} "
                f"delta_after_cap={diag.get('delta_after_cap', 0.0):.2f} "
                f"cap_hit={diag.get('cap_hit', False)} cap_reason={diag.get('cap_reason', 'none')} "
                f"max_spread={_max_spread} half_d={diag.get('half_d', 0.0):.2f} q={q:+.4f}"
            )
            logger.info(
                f"QUOTE_FINAL bid={bid_price:.1f}(−{bid_dist:.1f}) "
                f"ask={ask_price:.1f}(+{ask_dist:.1f}) "
                f"spread={total_sp:.1f} skew={skew_pct:+.1f}% "
                f"asym={diag.get('asym', 0.0):+.3f}"
            )
            logger.info(
                "QUOTE_SNAPSHOT market_gen=%d depth_gen=%d book_gen=%d "
                "depth_bid=%.8f depth_ask=%.8f book_bid=%.8f book_ask=%.8f "
                "depth_exchange_ts_ms=%d depth_receive_ts_ns=%d "
                "book_exchange_ts_ms=%d book_receive_ts_ns=%d capture_ts_ns=%d "
                "depth_visible_age_s=%.6f depth_source_lag_s=%.6f "
                "book_visible_age_s=%.6f book_source_lag_s=%.6f "
                "guard_source=%s guard_fallback=%s lock_wait_us=%.3f lock_hold_us=%.3f",
                quote_snapshot.market_generation,
                quote_snapshot.depth_generation,
                quote_snapshot.book_ticker_generation,
                quote_snapshot.best_bid,
                quote_snapshot.best_ask,
                quote_snapshot.book_ticker_bid,
                quote_snapshot.book_ticker_ask,
                quote_snapshot.depth_exchange_ts_ms,
                quote_snapshot.depth_receive_ts_ns,
                quote_snapshot.book_ticker_exchange_ts_ms,
                quote_snapshot.book_ticker_receive_ts_ns,
                quote_snapshot.capture_ts_ns,
                quote_snapshot.depth_visible_age_s,
                quote_snapshot.depth_source_lag_s,
                quote_snapshot.book_ticker_visible_age_s,
                quote_snapshot.book_ticker_source_lag_s,
                guard.source,
                guard.fallback_reason,
                quote_snapshot.lock_wait_ns / 1_000.0,
                quote_snapshot.lock_hold_ns / 1_000.0,
            )

        self._log_depth_execution_shadow(
            mid=mid, depth=depth_raw, pred=pred,
            kappa_base=diag.get("kappa_before_depth", cfg.strategy.kappa),
            kappa_used=diag.get("kappa_used", cfg.strategy.kappa),
            bid_price=bid_price, ask_price=ask_price,
            asym=diag.get("asym", 0.0),
        )

        return bid_price, ask_price, ask_price - bid_price

    @staticmethod
    def _depth_imbalance(depth, levels: int) -> Tuple[float, float, float]:
        if not depth or not depth.bids or not depth.asks:
            return 0.0, 0.0, 0.0
        n = max(1, int(levels))
        bid_qty = sum(q for _, q in depth.bids[:n])
        ask_qty = sum(q for _, q in depth.asks[:n])
        total_qty = bid_qty + ask_qty
        if total_qty <= 1e-8:
            return 0.0, bid_qty, ask_qty
        return (bid_qty - ask_qty) / total_qty, bid_qty, ask_qty

    @staticmethod
    def _estimate_depth_kappa(depth, kappa_base: float, depth_baseline: float,
                              levels: int, min_ratio: float = 0.3) -> float:
        if not depth or not depth.bids or not depth.asks or depth_baseline <= 0:
            return kappa_base
        n = max(1, min(int(levels), len(depth.bids), len(depth.asks)))
        bid_depth = sum(q for _, q in depth.bids[:n])
        ask_depth = sum(q for _, q in depth.asks[:n])
        avg_depth = (bid_depth + ask_depth) * 0.5
        if avg_depth <= 0:
            return kappa_base
        ratio_floor = max(0.05, min(3.0, float(min_ratio)))
        ratio = max(ratio_floor, min(3.0, avg_depth / depth_baseline))
        return kappa_base * ratio

    def _depth_tox_spread_mult(self, mid: float, depth, depth_exec_cfg,
                               force: bool = False) -> float:
        tox_cfg = getattr(depth_exec_cfg, 'depth_tox_spread', None) if depth_exec_cfg else None
        if not tox_cfg or (not force and not getattr(tox_cfg, 'enabled', False)):
            return 1.0
        imb, _, _ = self._depth_imbalance(depth, getattr(tox_cfg, 'levels', 20))
        micro_shift_bps = self._depth_micro_shift_bps(mid, depth, levels=3)
        imb_thr = abs(getattr(tox_cfg, 'imbalance_threshold', 0.65))
        shift_thr = abs(getattr(tox_cfg, 'microprice_shift_bps', 1.0))
        if abs(imb) >= imb_thr or abs(micro_shift_bps) >= shift_thr:
            return max(1.0, float(getattr(tox_cfg, 'spread_mult', 1.25)))
        return 1.0

    @staticmethod
    def _depth_micro_shift_bps(mid: float, depth, levels: int) -> float:
        if mid <= 0 or not depth or not depth.bids or not depth.asks:
            return 0.0
        from strategy.quote_core import microprice_from_book
        fair = microprice_from_book(depth.bids, depth.asks, levels=max(1, int(levels)))
        return (fair - mid) / mid * 10000.0

    def _log_depth_execution_shadow(self, mid: float, depth, pred: Prediction,
                                    kappa_base: float, kappa_used: float,
                                    bid_price: float, ask_price: float,
                                    asym: float):
        depth_cfg = getattr(self.cfg, 'depth_execution', None)
        if not depth_cfg or not getattr(depth_cfg, 'shadow_enabled', False):
            return
        interval = max(1, int(getattr(depth_cfg, 'log_interval_requotes', 6)))
        if self._requote_count % interval != 0:
            return
        if not depth or not depth.bids or not depth.asks:
            logger.info("DEPTH_SHADOW status=no_depth")
            return

        mk_cfg = getattr(depth_cfg, 'microprice_kappa', None)
        imb_cfg = getattr(depth_cfg, 'imbalance_asym', None)
        tox_cfg = getattr(depth_cfg, 'depth_tox_spread', None)
        mp_levels = getattr(mk_cfg, 'microprice_levels', 3) if mk_cfg else 3
        kappa_levels = getattr(mk_cfg, 'kappa_levels', 5) if mk_cfg else 5
        imb_levels = getattr(imb_cfg, 'levels', 20) if imb_cfg else 20
        tox_levels = getattr(tox_cfg, 'levels', 20) if tox_cfg else 20

        micro_shift_bps = self._depth_micro_shift_bps(mid, depth, mp_levels)
        imb, bid_qty, ask_qty = self._depth_imbalance(depth, imb_levels)
        tox_imb, _, _ = self._depth_imbalance(depth, tox_levels)
        kappa_baseline = (
            getattr(mk_cfg, 'kappa_depth_baseline', getattr(self.cfg.strategy, 'kappa_depth_baseline', 50.0))
            if mk_cfg else getattr(self.cfg.strategy, 'kappa_depth_baseline', 50.0)
        )
        depth_kappa_min_ratio = float(getattr(self.cfg.strategy, 'depth_kappa_ratio', 0.3))
        kappa_candidate = self._estimate_depth_kappa(
            depth, kappa_base=kappa_base,
            depth_baseline=kappa_baseline,
            levels=kappa_levels,
            min_ratio=depth_kappa_min_ratio,
        )
        kappa_ratio = kappa_candidate / kappa_base if kappa_base > 0 else 1.0
        spread = ask_price - bid_price
        tox_bid, tox_ask = self._toxicity_probs(pred)
        dtox_mult = self._depth_tox_spread_mult(mid, depth, depth_cfg, force=True)
        tox_imb_thr = abs(getattr(tox_cfg, 'imbalance_threshold', 0.65)) if tox_cfg else 0.65
        tox_shift_thr = abs(getattr(tox_cfg, 'microprice_shift_bps', 1.0)) if tox_cfg else 1.0
        dtox_bid = tox_imb <= -tox_imb_thr or micro_shift_bps <= -tox_shift_thr
        dtox_ask = tox_imb >= tox_imb_thr or micro_shift_bps >= tox_shift_thr
        logger.info(
            f"DEPTH_SHADOW mp_en={getattr(mk_cfg, 'enabled', False)} "
            f"imb_en={getattr(imb_cfg, 'enabled', False)} "
            f"dtox_en={getattr(tox_cfg, 'enabled', False)} "
            f"micro_shift_bps={micro_shift_bps:+.3f} "
            f"kappa_ratio={kappa_ratio:.3f} kappa_used={kappa_used:.6f} "
            f"imb={imb:+.3f} tox_imb={tox_imb:+.3f} "
            f"bid_qty={bid_qty:.3f} ask_qty={ask_qty:.3f} "
            f"tox_bid={tox_bid:.3f} tox_ask={tox_ask:.3f} "
            f"dtox_bid={dtox_bid} dtox_ask={dtox_ask} "
            f"dtox_mult={dtox_mult:.2f} spread={spread:.2f} asym={asym:+.3f}"
        )

    def _toxicity_probs(self, pred: Prediction) -> tuple[float, float]:
        horizon = int(getattr(self.cfg.ml, 'toxicity_horizon_s', 10))
        horizon = 5 if horizon == 5 else 10
        bid_attr = f"tox_bid_{horizon}s"
        ask_attr = f"tox_ask_{horizon}s"
        tox_bid = getattr(pred, bid_attr, 1.0 - pred.dir_10s)
        tox_ask = getattr(pred, ask_attr, pred.dir_10s)
        tox_bid = max(0.0, min(1.0, float(tox_bid)))
        tox_ask = max(0.0, min(1.0, float(tox_ask)))
        return tox_bid, tox_ask

    def _reducing_cooldown_vol_mult(self) -> float:
        """Optional volatility multiplier for the shorter reducing-side cooldown."""
        ref = float(getattr(self.cfg.strategy, "fill_cooldown_reducing_vol_ref", 0.0) or 0.0)
        if ref <= 0.0:
            return 1.0
        pred = getattr(self, "_last_prediction", None)
        vol = float(getattr(pred, "vol_10s", 0.0) or 0.0) if pred is not None else 0.0
        if not math.isfinite(vol) or vol <= 0.0:
            return 1.0
        lo = float(getattr(self.cfg.strategy, "fill_cooldown_reducing_vol_min_mult", 0.5) or 0.5)
        hi = float(getattr(self.cfg.strategy, "fill_cooldown_reducing_vol_max_mult", 2.0) or 2.0)
        if hi < lo:
            lo, hi = hi, lo
        return max(lo, min(hi, vol / max(ref, 1e-12)))

    def _reducing_cooldown_campaign_gate_active(self, prev_q: float) -> bool:
        """Return whether a reducing fill should start the short cooldown.

        中文说明：减仓方向 cooldown 不应该全局常开。这里把它限制在大库存
        或老 campaign 这类真实风险状态里；普通自然减仓仍然不节流。
        """
        cfg = self.cfg.strategy
        if not bool(getattr(cfg, "fill_cooldown_reducing_campaign_only", False)):
            return True

        abs_q = abs(float(prev_q or 0.0))
        inv_thr = float(getattr(cfg, "fill_cooldown_reducing_inv_threshold", 0.0) or 0.0)
        ratio_thr = float(getattr(cfg, "fill_cooldown_reducing_inv_ratio", 0.0) or 0.0)
        age_thr = float(getattr(cfg, "fill_cooldown_reducing_age_s", 0.0) or 0.0)

        inv_hit = inv_thr > 0.0 and abs_q >= inv_thr
        order_size = max(float(getattr(cfg, "order_size", 0.0) or 0.0), 1e-12)
        ratio_hit = ratio_thr > 0.0 and (abs_q / order_size) >= ratio_thr

        age_hit = False
        inv = getattr(self, "inventory", None)
        if inv is not None and age_thr > 0.0:
            try:
                camp = inv.campaign_snapshot()
                age_hit = bool(camp.active and float(camp.age_s) >= age_thr)
            except Exception:
                age_hit = False

        return bool(inv_hit or ratio_hit or age_hit)

    def _adaptive_add_cooldown_multiplier(self, side: str, prev_q: float, consec_units: float) -> float:
        """State-dependent multiplier for exposure-increasing fill cooldown.

        中文说明：这是固定 add-side `fill_cooldown` 的可选 multiplier，默认
        关闭。输入只来自 fill/quote 当时可见状态：同侧连续成交、campaign
        年龄/库存、短窗趋势和 L2 refill/cancel。不要接入 fill 后结果。
        """
        strat = self.cfg.strategy
        if not bool(getattr(strat, "adaptive_add_cooldown_enabled", False)):
            return 1.0

        def f(name: str, default: float) -> float:
            try:
                return float(getattr(strat, name, default) or default)
            except Exception:
                return float(default)

        cfg = AdaptiveAddCooldownConfig(
            enabled=True,
            min_mult=f("adaptive_add_cooldown_min_mult", 0.5),
            max_mult=f("adaptive_add_cooldown_max_mult", 2.5),
            w_markout=f("adaptive_add_cooldown_w_markout", 0.0),
            w_flow=f("adaptive_add_cooldown_w_flow", 0.0),
            w_campaign=f("adaptive_add_cooldown_w_campaign", 0.0),
            w_trend=f("adaptive_add_cooldown_w_trend", 0.0),
            w_refill_weak=f("adaptive_add_cooldown_w_refill_weak", 0.0),
            w_refill_good=f("adaptive_add_cooldown_w_refill_good", 0.0),
            w_reversion=f("adaptive_add_cooldown_w_reversion", 0.0),
            mo_ref=f("adaptive_add_cooldown_mo_ref", 50.0),
            flow_ref=f("adaptive_add_cooldown_flow_ref", 2.0),
            campaign_inv_ref=f("adaptive_add_cooldown_campaign_inv_ref", 0.006),
            campaign_age_ref_s=f("adaptive_add_cooldown_campaign_age_ref_s", 3600.0),
            trend_ret_ref=f("adaptive_add_cooldown_trend_ret_ref", 2e-5),
            refill_ref=f("adaptive_add_cooldown_refill_ref", 0.10),
            reversion_ref=f("adaptive_add_cooldown_reversion_ref", 1.0),
            gate_enabled=bool(getattr(strat, "adaptive_add_cooldown_gate_enabled", False)),
            gate_mult=f("adaptive_add_cooldown_gate_mult", 1.75),
            gate_campaign_score=f("adaptive_add_cooldown_gate_campaign_score", 1.0),
            gate_trend_score=f("adaptive_add_cooldown_gate_trend_score", 1.0),
            gate_refill_edge_max=f("adaptive_add_cooldown_gate_refill_edge_max", 0.0),
            gate_reversion_max=f("adaptive_add_cooldown_gate_reversion_max", 0.5),
            gate_side=str(getattr(strat, "adaptive_add_cooldown_gate_side", "BOTH") or "BOTH").upper(),
        )

        pred = getattr(self, "_last_prediction", None)
        ret_10s = float(getattr(pred, "ret_10s", 0.0) or 0.0) if pred is not None else 0.0
        side_adverse_ret = max(0.0, -ret_10s) if side == "BUY" else max(0.0, ret_10s)

        markout_ema = self._mo_ema_bid if side == "BUY" else self._mo_ema_ask
        campaign_age_s = 0.0
        try:
            camp = self.inventory.campaign_snapshot()
            campaign_age_s = float(camp.age_s if camp.active else 0.0)
        except Exception:
            campaign_age_s = 0.0

        refill_edge = 0.0
        try:
            metrics = self._current_l2_policy_metrics(self._last_mid)
            refill_edge = float(metrics.get("l2_book_refresh_ratio", 0.0)) - float(
                metrics.get("l2_book_cancel_ratio", 0.0)
            )
        except Exception:
            refill_edge = 0.0

        trend_norm = min(1.0, side_adverse_ret / max(cfg.trend_ret_ref, 1e-12))
        good_refill_norm = min(1.0, max(0.0, refill_edge) / max(cfg.refill_ref, 1e-12))
        micro_reversion_score = good_refill_norm * (1.0 - trend_norm)

        return adaptive_add_cooldown_multiplier(
            side=side,
            side_markout_ema=markout_ema,
            consec_units=consec_units,
            prev_inventory=prev_q,
            max_inventory=float(getattr(strat, "max_inventory", 0.0) or 0.0),
            campaign_age_s=campaign_age_s,
            side_adverse_ret=side_adverse_ret,
            refill_edge=refill_edge,
            micro_reversion_score=micro_reversion_score,
            cfg=cfg,
        )

    def _update_orders(
        self,
        mid: float,
        bid_price: float,
        ask_price: float,
        q: float,
        pred: Prediction,
        *,
        quote_snapshot: QuoteDecisionSnapshot,
        post_only_guard: QuotePostOnlyGuard,
        decision_group_id: str = "",
        decision_start_ts_ns: int = 0,
    ) -> tuple:
        """
        Apply per-side quote policy, then lazy cancel/replace orders.

        Returns (bid_updated, ask_updated) booleans.
        """
        cfg = self.cfg
        symbol = cfg.symbol
        snapshot_error = self._quote_snapshot_contract_error(
            quote_snapshot,
            use_bar_pricing=bool(getattr(cfg.strategy, "use_bar_pricing", False)),
            post_only_guard=post_only_guard,
        )
        if snapshot_error:
            raise RuntimeError(
                f"order routing received invalid quote snapshot: {snapshot_error}"
            )
        best_bid = float(post_only_guard.best_bid)
        best_ask = float(post_only_guard.best_ask)
        decision_start_ts_ns = int(
            decision_start_ts_ns or time.time_ns()
        )
        decision_group_id = str(
            decision_group_id
            or f"{symbol}:{os.getpid()}:{decision_start_ts_ns}:adhoc"
        )
        bid_decision_id = f"{decision_group_id}:BUY"
        ask_decision_id = f"{decision_group_id}:SELL"
        threshold = cfg.strategy.requote_threshold_bps / 10000.0
        base_bid_price = bid_price
        base_ask_price = ask_price
        self._last_prediction = pred

        bid_policy = self._build_side_policy(
            Side.BUY, mid, q, pred, quote_snapshot
        )
        ask_policy = self._build_side_policy(
            Side.SELL, mid, q, pred, quote_snapshot
        )
        self._apply_flat_unilateral_ttl(q, bid_policy, ask_policy)
        self._apply_sync_adjust_degrade_policy(q, bid_policy, ask_policy)

        bid_price = self._apply_side_policy_price(Side.BUY, mid, bid_price, bid_policy.spread_mult)
        ask_price = self._apply_side_policy_price(Side.SELL, mid, ask_price, ask_policy.spread_mult)
        bid_price, ask_price = self._apply_post_fill_quote_response(
            q=q,
            bid_price=bid_price,
            ask_price=ask_price,
            pred=pred,
            bid_policy=bid_policy,
            ask_policy=ask_policy,
            best_bid=best_bid,
            best_ask=best_ask,
        )
        bid_price, ask_price, post_policy_cap_hit = self._apply_post_policy_spread_cap(
            mid,
            bid_price,
            ask_price,
            inventory=q,
            best_bid=best_bid,
            best_ask=best_ask,
        )
        if post_policy_cap_hit and self._requote_count % 6 == 0:
            logger.info(
                f"QUOTE_POST_POLICY_CAP bid={bid_price:.1f} ask={ask_price:.1f} "
                f"spread={ask_price - bid_price:.1f}"
            )

        cap_mode = spread_cap_mode_code(getattr(cfg.strategy, "spread_cap_mode", "compress"))
        if cap_mode == SPREAD_CAP_PAUSE_EXPOSURE:
            bid_cap_block = bool(self._last_quote_context.get("BUY", {}).get("cap_exposure_block", False))
            ask_cap_block = bool(self._last_quote_context.get("SELL", {}).get("cap_exposure_block", False))
            # Policy multipliers can create a second cap hit after quote-core.
            # In pause mode that hit blocks only the side that would add risk.
            if post_policy_cap_hit:
                bid_cap_block = bid_cap_block or q >= 0.0
                ask_cap_block = ask_cap_block or q <= 0.0
            if bid_cap_block and q >= 0.0:
                bid_policy.allow_exposure_increase = False
                bid_policy.reason_mask |= POLICY_REASON_SPREAD_CAP | POLICY_REASON_EXPOSURE_ONLY
                bid_policy.mode = "defend"
                bid_policy.reason_text = self._policy_reason_text(bid_policy.reason_mask)
            if ask_cap_block and q <= 0.0:
                ask_policy.allow_exposure_increase = False
                ask_policy.reason_mask |= POLICY_REASON_SPREAD_CAP | POLICY_REASON_EXPOSURE_ONLY
                ask_policy.mode = "defend"
                ask_policy.reason_text = self._policy_reason_text(ask_policy.reason_mask)

        can_bid_after_inventory = q < cfg.strategy.max_inventory
        can_ask_after_inventory = q > -cfg.strategy.max_inventory
        bid_exposure_increasing = q >= 0.0
        ask_exposure_increasing = q <= 0.0

        can_bid = (
            can_bid_after_inventory
            and bid_policy.allow_post
            and (bid_policy.allow_exposure_increase or not bid_exposure_increasing)
        )
        can_ask = (
            can_ask_after_inventory
            and ask_policy.allow_post
            and (ask_policy.allow_exposure_increase or not ask_exposure_increasing)
        )
        now_ts = time.time()

        # Check existing bid order
        bid_order = self.orders.get_order(self._bid_cid) if self._bid_cid else None
        bid_pending_lifecycle = self._order_lifecycle_pending(bid_order)
        bid_alive = bid_order and bid_order.is_active
        if not bid_alive and not bid_pending_lifecycle:
            self._bid_cid = None  # clear stale reference (e.g. GTX expired)
        bid_needs_update = True
        bid_force_update = False
        if bid_alive and bid_order.price > 0:
            drift = abs(bid_price - bid_order.price) / bid_order.price
            if drift <= threshold:
                bid_needs_update = False  # keep existing order
            if bid_policy.order_ttl_ms > 0 and (time.time() - bid_order.create_time) * 1000.0 >= bid_policy.order_ttl_ms:
                bid_needs_update = True
                bid_force_update = True
                bid_policy.reason_mask |= POLICY_REASON_THIN_DEPTH
                bid_policy.reason_text = self._policy_reason_text(bid_policy.reason_mask)

        # Check existing ask order
        ask_order = self.orders.get_order(self._ask_cid) if self._ask_cid else None
        ask_pending_lifecycle = self._order_lifecycle_pending(ask_order)
        ask_alive = ask_order and ask_order.is_active
        if not ask_alive and not ask_pending_lifecycle:
            self._ask_cid = None  # clear stale reference (e.g. GTX expired)
        ask_needs_update = True
        ask_force_update = False
        if ask_alive and ask_order.price > 0:
            drift = abs(ask_price - ask_order.price) / ask_order.price
            if drift <= threshold:
                ask_needs_update = False  # keep existing order
            if ask_policy.order_ttl_ms > 0 and (time.time() - ask_order.create_time) * 1000.0 >= ask_policy.order_ttl_ms:
                ask_needs_update = True
                ask_force_update = True
                ask_policy.reason_mask |= POLICY_REASON_THIN_DEPTH
                ask_policy.reason_text = self._policy_reason_text(ask_policy.reason_mask)

        # Base size controls first (legacy eta / symmetric sizing), then per-side policy.
        eta = getattr(cfg.strategy, 'eta', 0.0)
        base_size = cfg.strategy.order_size
        lot = cfg.lot_size
        min_notional = cfg.min_notional
        min_qty = getattr(self, '_min_qty', lot)
        symmetric_size = getattr(cfg.strategy, 'symmetric_size', False)

        cpp_route = _get_live_routing_cpp()
        cpp_routing_used = False
        if cpp_route is not None:
            try:
                max_spread = float(self._last_quote_diagnostics.get("max_spread", 0.0) or 0.0)
                if max_spread <= 0.0:
                    cap_bps = float(getattr(cfg.strategy, "max_spread_bps", 0.0) or 0.0)
                    max_spread = mid * cap_bps / 10000.0 if cap_bps > 0.0 and mid > 0.0 else 0.0
                # Python already applies the configured cap action above. The
                # compact C++ router only understands inward compression, so a
                # non-compress A/B must pass zero here to avoid reintroducing it.
                if cap_mode != SPREAD_CAP_COMPRESS:
                    max_spread = 0.0
                pre_cpp_post_policy_cap_hit = post_policy_cap_hit
                now_ts = time.time()
                routed = cpp_route.compute_live_routing_decision(
                    (
                        mid,
                        q,
                        base_bid_price,
                        base_ask_price,
                        best_bid,
                        best_ask,
                        cfg.tick_size,
                        lot,
                        min_qty,
                        min_notional,
                        base_size,
                        cfg.strategy.max_inventory,
                        eta,
                        bool(symmetric_size),
                        cfg.strategy.requote_threshold_bps,
                        max_spread,
                        bool(bid_alive),
                        float(bid_order.price if bid_alive and bid_order else 0.0),
                        max(0.0, (now_ts - bid_order.create_time) * 1000.0) if bid_alive and bid_order else 0.0,
                        bool(ask_alive),
                        float(ask_order.price if ask_alive and ask_order else 0.0),
                        max(0.0, (now_ts - ask_order.create_time) * 1000.0) if ask_alive and ask_order else 0.0,
                    ),
                    (
                        bid_policy.allow_post,
                        bid_policy.allow_exposure_increase,
                        bid_policy.spread_mult,
                        bid_policy.size_mult,
                        bid_policy.order_ttl_ms,
                    ),
                    (
                        ask_policy.allow_post,
                        ask_policy.allow_exposure_increase,
                        ask_policy.spread_mult,
                        ask_policy.size_mult,
                        ask_policy.order_ttl_ms,
                    ),
                )
                (
                    bid_price,
                    ask_price,
                    post_policy_cap_hit,
                    can_bid_after_inventory,
                    can_ask_after_inventory,
                    can_bid,
                    can_ask,
                    bid_needs_update,
                    ask_needs_update,
                    bid_size_pre,
                    ask_size_pre,
                ) = routed
                post_policy_cap_hit = bool(post_policy_cap_hit or pre_cpp_post_policy_cap_hit)
                bid_force_update = bid_force_update or self._policy_order_ttl_expired(bid_policy, bid_order)
                ask_force_update = ask_force_update or self._policy_order_ttl_expired(ask_policy, ask_order)
                cpp_routing_used = True
                self._last_cpp_routing_used = 1
            except Exception as exc:
                if _cpp_strict():
                    raise
                logger.warning("C++ live routing disabled after error: %s", exc)
                global _live_routing_cpp_failed
                _live_routing_cpp_failed = True
                cpp_route = None

        if not cpp_routing_used:
            bid_size_pre = base_size
            ask_size_pre = base_size
            if eta > 0 and cfg.strategy.max_inventory > 1e-10:
                q_norm = q / cfg.strategy.max_inventory
                if q_norm > 0:
                    bid_size_pre = base_size * math.exp(-eta * q_norm)
                    bid_size_pre = max(lot, math.floor(bid_size_pre / lot) * lot)
                elif q_norm < 0:
                    ask_size_pre = base_size * math.exp(eta * q_norm)
                    ask_size_pre = max(lot, math.floor(ask_size_pre / lot) * lot)
            if symmetric_size:
                mirrored = min(bid_size_pre, ask_size_pre)
                bid_size_pre = mirrored
                ask_size_pre = mirrored

            bid_size_pre = self._policy_sized_qty(
                bid_size_pre * bid_policy.size_mult,
                base_size,
                bid_price,
                min_qty,
                min_notional,
                lot,
            )
            ask_size_pre = self._policy_sized_qty(
                ask_size_pre * ask_policy.size_mult,
                base_size,
                ask_price,
                min_qty,
                min_notional,
                lot,
            )

        state_policy_max_spread = float(
            self._last_quote_diagnostics.get("max_spread", 0.0) or 0.0
        )
        if state_policy_max_spread <= 0.0:
            cap_bps = float(
                getattr(cfg.strategy, "max_spread_bps", 0.0) or 0.0
            )
            state_policy_max_spread = (
                mid * cap_bps / 10000.0
                if cap_bps > 0.0 and mid > 0.0
                else 0.0
            )
        bid_price, bid_state_policy_applied = (
            self._maybe_apply_state_conditioned_quote_policy(
                side=Side.BUY,
                mid=mid,
                q=q,
                baseline_price=bid_price,
                pre_guard_price=base_bid_price,
                other_side_price=ask_price,
                max_pair_spread=state_policy_max_spread,
                can_post=bool(can_bid),
                order_active=bool(bid_alive),
                order_pending=bool(bid_pending_lifecycle),
                decision=bid_policy,
                best_bid=best_bid,
                best_ask=best_ask,
            )
        )
        ask_price, ask_state_policy_applied = (
            self._maybe_apply_state_conditioned_quote_policy(
                side=Side.SELL,
                mid=mid,
                q=q,
                baseline_price=ask_price,
                pre_guard_price=base_ask_price,
                other_side_price=bid_price,
                max_pair_spread=state_policy_max_spread,
                can_post=bool(can_ask),
                order_active=bool(ask_alive),
                order_pending=bool(ask_pending_lifecycle),
                decision=ask_policy,
                best_bid=best_bid,
                best_ask=best_ask,
            )
        )
        if bid_state_policy_applied:
            bid_force_update = True
        if ask_state_policy_applied:
            ask_force_update = True

        self._evaluate_dynamic_fill_hazard_prospective_recovery(
            candidate_price=float(bid_price),
            inventory=float(q),
            now_ns=int(decision_start_ts_ns),
        )
        if self._dynamic_fill_hazard_buy_blocked(q):
            bid_policy.allow_exposure_increase = False
            bid_policy.mode = "hazard_hold"
            bid_policy.reason_mask |= (
                POLICY_REASON_BUY_HAZARD_CANCEL
                | POLICY_REASON_EXPOSURE_ONLY
            )
            bid_policy.reason_text = self._policy_reason_text(
                bid_policy.reason_mask
            )
            can_bid = False

        # Both evidence-only external projections share one causal state read.
        # Neither candidate is allowed to flow into live order routing.
        edge_projection: Optional[ExternalAdverseQuoteEdgeProjection] = None
        edge_projection_error = "not_evaluated"
        feature_ready_ts_ns = time.time_ns()
        exact_tape_enabled = self._exact_opportunity_tape_enabled()
        fair_shadow_enabled = bool(
            getattr(
                cfg.strategy,
                "cross_venue_fair_price_shadow_enabled",
                False,
            )
        )
        if exact_tape_enabled:
            fair_state: Optional[CrossVenueFairPriceState] = None
            state_query_ts_ns = time.time_ns()
            try:
                fair_state = self.signal.cross_venue_fair_price_state(
                    local_mid=float(mid),
                    now_ns=state_query_ts_ns,
                )
                edge_projection = project_external_adverse_quote_edge(
                    fair_state,
                    baseline_bid=float(bid_price),
                    baseline_ask=float(ask_price),
                    tick_size=float(cfg.tick_size),
                    max_pair_spread_bps=float(
                        getattr(cfg.strategy, "max_spread_bps", 0.0) or 0.0
                    ),
                )
                edge_projection_error = str(edge_projection.reason)
            except Exception as exc:
                edge_projection_error = f"projection_error:{type(exc).__name__}"
                logger.warning(
                    "External adverse-edge exact tape projection failed closed: %s",
                    exc,
                )
            feature_ready_ts_ns = time.time_ns()
            if fair_shadow_enabled and fair_state is not None:
                self._record_cross_venue_fair_price_shadow(
                    symbol=symbol,
                    mid=mid,
                    baseline_bid=bid_price,
                    baseline_ask=ask_price,
                    decision_ts_ns=state_query_ts_ns,
                    state=fair_state,
                    best_bid=best_bid,
                    best_ask=best_ask,
                )
        else:
            self._record_cross_venue_fair_price_shadow(
                symbol=symbol,
                mid=mid,
                baseline_bid=bid_price,
                baseline_ask=ask_price,
                best_bid=best_bid,
                best_ask=best_ask,
            )

        routing_error = self._quote_routing_contract_error(
            bid_price=float(bid_price),
            ask_price=float(ask_price),
            can_bid=bool(can_bid),
            can_ask=bool(can_ask),
            post_only_guard=post_only_guard,
        )
        if routing_error:
            raise RuntimeError(
                "order routing violates frozen quote snapshot: "
                f"{routing_error} bid={bid_price} ask={ask_price} "
                f"guard_bid={best_bid} guard_ask={best_ask} "
                f"guard_source={post_only_guard.source}"
            )

        def exact_side_context(
            *,
            side: str,
            baseline_price: float,
            exposure_increasing: bool,
            baseline_eligible: bool,
            final_action: str,
            replaced_client_order_id: str = "",
            queue_reset: bool = False,
        ) -> dict[str, object]:
            candidate_price = float(baseline_price)
            requested_ticks = 0
            effective_ticks = 0
            if (
                exposure_increasing
                and edge_projection is not None
                and edge_projection.valid
                and edge_projection.adverse_side == side
            ):
                candidate_price = float(
                    edge_projection.candidate_bid
                    if side == "BUY"
                    else edge_projection.candidate_ask
                )
                requested_ticks = int(edge_projection.requested_ticks)
                effective_ticks = int(edge_projection.effective_ticks)
            return {
                "exact_decision_group_id": decision_group_id,
                "exact_decision_id": (
                    bid_decision_id if side == "BUY" else ask_decision_id
                ),
                "exact_decision_start_ts_ns": decision_start_ts_ns,
                "exact_feature_ready_ts_ns": feature_ready_ts_ns,
                "exact_role": exact_quote_role(side, q),
                "exact_signed_inventory_before": float(q),
                "exact_exposure_increasing": bool(exposure_increasing),
                "exact_baseline_eligible": bool(baseline_eligible),
                "exact_baseline_quote_price": float(baseline_price),
                "exact_candidate_quote_price": candidate_price,
                "exact_guard_valid": bool(
                    edge_projection is not None and edge_projection.valid
                ),
                "exact_guard_reason": (
                    str(edge_projection.reason)
                    if edge_projection is not None
                    else edge_projection_error
                ),
                "exact_guard_adverse_side": (
                    str(edge_projection.adverse_side)
                    if edge_projection is not None
                    else ""
                ),
                "exact_requested_outward_ticks": requested_ticks,
                "exact_effective_outward_ticks": effective_ticks,
                "exact_replaced_client_order_id": str(
                    replaced_client_order_id
                ),
                "exact_final_executed_action": str(final_action),
                "exact_queue_reset": bool(queue_reset),
            }

        bid_needs_update = self._apply_replace_throttle(
            side=Side.BUY,
            now_ts=now_ts,
            q=q,
            target_price=bid_price,
            order=bid_order,
            needs_update=bool(bid_needs_update),
            force_update=bool(bid_force_update),
        )
        ask_needs_update = self._apply_replace_throttle(
            side=Side.SELL,
            now_ts=now_ts,
            q=q,
            target_price=ask_price,
            order=ask_order,
            needs_update=bool(ask_needs_update),
            force_update=bool(ask_force_update),
        )
        bid_pending_coalesce = self._apply_pending_replace_coalesce(
            side=Side.BUY,
            now_ts=now_ts,
            q=q,
            target_price=bid_price,
            order=bid_order,
            needs_update=bool(bid_needs_update),
            can_post=bool(can_bid),
        )
        ask_pending_coalesce = self._apply_pending_replace_coalesce(
            side=Side.SELL,
            now_ts=now_ts,
            q=q,
            target_price=ask_price,
            order=ask_order,
            needs_update=bool(ask_needs_update),
            can_post=bool(can_ask),
        )
        if bid_pending_coalesce:
            bid_needs_update = False
        if ask_pending_coalesce:
            ask_needs_update = False

        bid_cancel_first = self._should_cancel_first_replace(
            side=Side.BUY,
            q=q,
            order=bid_order,
            needs_update=bool(bid_needs_update),
            force_update=bool(bid_force_update),
            can_post=bool(can_bid),
        )
        ask_cancel_first = self._should_cancel_first_replace(
            side=Side.SELL,
            q=q,
            order=ask_order,
            needs_update=bool(ask_needs_update),
            force_update=bool(ask_force_update),
            can_post=bool(can_ask),
        )

        bid_replaced_cid = (
            str(bid_order.client_order_id)
            if bid_alive and (bid_cancel_first or bid_needs_update)
            else ""
        )
        ask_replaced_cid = (
            str(ask_order.client_order_id)
            if ask_alive and (ask_cancel_first or ask_needs_update)
            else ""
        )
        bid_cancel_requested = False
        ask_cancel_requested = False

        # Cancel only the orders that need updating
        if bid_cancel_first and bid_alive:
            bid_cancel_requested = self._cancel_order(
                self._bid_cid,
                trigger_decision_id=bid_decision_id,
            )
            bid_needs_update = False
        elif bid_needs_update and bid_alive:
            bid_cancel_requested = self._cancel_order(
                self._bid_cid,
                trigger_decision_id=bid_decision_id,
            )
            self._bid_cid = None
        if ask_cancel_first and ask_alive:
            ask_cancel_requested = self._cancel_order(
                self._ask_cid,
                trigger_decision_id=ask_decision_id,
            )
            ask_needs_update = False
        elif ask_needs_update and ask_alive:
            ask_cancel_requested = self._cancel_order(
                self._ask_cid,
                trigger_decision_id=ask_decision_id,
            )
            self._ask_cid = None

        bid_action = "none"
        ask_action = "none"
        bid_submitted_cid = ""
        ask_submitted_cid = ""
        if bid_pending_coalesce:
            bid_action = "pending_coalesce"
        elif bid_cancel_first:
            bid_action = "cancel_first"
        elif bid_needs_update and can_bid:
            bid_size = bid_size_pre
            # Cap: don't let position exceed max_inventory
            if q > 0:
                room = max(0.0, cfg.strategy.max_inventory - q)
                room = math.floor(room / lot) * lot
                if room >= lot:
                    bid_size = min(bid_size, room)
                else:
                    bid_size = 0.0
            # Cap: prevent flip when closing short position
            elif q < -lot:
                close_cap = math.floor(abs(q) / lot) * lot
                if close_cap >= min_qty and close_cap * bid_price >= min_notional:
                    bid_size = min(bid_size, close_cap)
            # Exchange filter guard after eta decay
            if bid_size < min_qty or bid_size * bid_price < min_notional:
                logger.debug(
                    f"Bid skipped: qty={bid_size:.4f}(min={min_qty}) "
                    f"notional={bid_size * bid_price:.1f}(min={min_notional})"
                )
                bid_action = "skip_filter"
            else:
                bid_action = "place" if not bid_alive else "replace"
                decision_context = {
                    "decision_ts": time.time(),
                    "mid": mid,
                    "target_price": bid_price,
                    "target_qty": bid_size,
                    **asdict(bid_policy),
                    **exact_side_context(
                        side="BUY",
                        baseline_price=bid_price,
                        exposure_increasing=bid_exposure_increasing,
                        baseline_eligible=True,
                        final_action=bid_action,
                        replaced_client_order_id=bid_replaced_cid,
                        queue_reset=bid_cancel_requested,
                    ),
                }
                bid_submitted_cid = self._place_order(
                    symbol,
                    Side.BUY,
                    bid_price,
                    bid_size,
                    decision_context=decision_context,
                ) or ""
        elif not can_bid:
            bid_action = "pause"
        elif not bid_needs_update and bid_alive:
            bid_action = "keep"

        if ask_pending_coalesce:
            ask_action = "pending_coalesce"
        elif ask_cancel_first:
            ask_action = "cancel_first"
        elif ask_needs_update and can_ask:
            ask_size = ask_size_pre
            # Cap: don't let position exceed max_inventory
            if q < 0:
                room = max(0.0, cfg.strategy.max_inventory - abs(q))
                room = math.floor(room / lot) * lot
                if room >= lot:
                    ask_size = min(ask_size, room)
                else:
                    ask_size = 0.0
            # Cap: prevent flip when closing long position
            elif q > lot:
                close_cap = math.floor(q / lot) * lot
                if close_cap >= min_qty and close_cap * ask_price >= min_notional:
                    ask_size = min(ask_size, close_cap)
            # Exchange filter guard after eta decay
            if ask_size < min_qty or ask_size * ask_price < min_notional:
                logger.debug(
                    f"Ask skipped: qty={ask_size:.4f}(min={min_qty}) "
                    f"notional={ask_size * ask_price:.1f}(min={min_notional})"
                )
                ask_action = "skip_filter"
            else:
                ask_action = "place" if not ask_alive else "replace"
                decision_context = {
                    "decision_ts": time.time(),
                    "mid": mid,
                    "target_price": ask_price,
                    "target_qty": ask_size,
                    **asdict(ask_policy),
                    **exact_side_context(
                        side="SELL",
                        baseline_price=ask_price,
                        exposure_increasing=ask_exposure_increasing,
                        baseline_eligible=True,
                        final_action=ask_action,
                        replaced_client_order_id=ask_replaced_cid,
                        queue_reset=ask_cancel_requested,
                    ),
                }
                ask_submitted_cid = self._place_order(
                    symbol,
                    Side.SELL,
                    ask_price,
                    ask_size,
                    decision_context=decision_context,
                ) or ""
        elif not can_ask:
            ask_action = "pause"
        elif not ask_needs_update and ask_alive:
            ask_action = "keep"

        # Handle inventory limits: cancel sides we shouldn't be quoting
        if not can_bid:
            self._cancel_active_side_orders("BUY", "QUOTE_BLOCK_CANCEL")
        if not can_ask:
            self._cancel_active_side_orders("SELL", "QUOTE_BLOCK_CANCEL")

        def record_exact_decision(
            *,
            side: str,
            baseline_price: float,
            exposure_increasing: bool,
            can_post: bool,
            action: str,
            submitted_cid: str,
            existing_order: Any,
            replaced_cid: str,
            cancel_requested: bool,
            target_quantity: float,
        ) -> None:
            if not exact_tape_enabled:
                return
            executed_action = str(action)
            if action in {"place", "replace"} and not submitted_cid:
                executed_action = f"{action}_rejected"
            baseline_eligible = bool(
                exposure_increasing
                and can_post
                and action in {"place", "replace", "keep"}
            )
            fields = exact_side_context(
                side=side,
                baseline_price=baseline_price,
                exposure_increasing=exposure_increasing,
                baseline_eligible=baseline_eligible,
                final_action=executed_action,
                replaced_client_order_id=replaced_cid,
                queue_reset=cancel_requested,
            )
            client_order_id = str(submitted_cid)
            if not client_order_id and existing_order is not None:
                client_order_id = str(existing_order.client_order_id)
            event_ts_ns = time.time_ns()
            payload = empty_exact_opportunity_row(
                event_type="decision",
                event_ts_ns=event_ts_ns,
                symbol=symbol,
                side=side,
            )
            payload.update(
                decision_group_id=decision_group_id,
                decision_id=str(fields["exact_decision_id"]),
                origin_decision_id=str(fields["exact_decision_id"]),
                decision_start_ts_ns=decision_start_ts_ns,
                feature_ready_ts_ns=feature_ready_ts_ns,
                role=str(fields["exact_role"]),
                signed_inventory_before=float(q),
                exposure_increasing=int(bool(exposure_increasing)),
                baseline_eligible=int(baseline_eligible),
                baseline_quote_price=float(baseline_price),
                candidate_quote_price=float(
                    fields["exact_candidate_quote_price"]
                ),
                guard_valid=int(bool(fields["exact_guard_valid"])),
                guard_reason=str(fields["exact_guard_reason"]),
                guard_adverse_side=str(
                    fields["exact_guard_adverse_side"]
                ),
                requested_outward_ticks=int(
                    fields["exact_requested_outward_ticks"]
                ),
                effective_outward_ticks=int(
                    fields["exact_effective_outward_ticks"]
                ),
                client_order_id=client_order_id,
                replaced_client_order_id=str(replaced_cid),
                final_executed_action=executed_action,
                queue_reset=int(bool(cancel_requested)),
                order_state=(
                    str(getattr(existing_order, "state", "").name)
                    if existing_order is not None
                    else ""
                ),
                order_quantity=float(target_quantity),
            )
            self._append_exact_opportunity_payload(payload)

        record_exact_decision(
            side="BUY",
            baseline_price=bid_price,
            exposure_increasing=bid_exposure_increasing,
            can_post=can_bid,
            action=bid_action,
            submitted_cid=bid_submitted_cid,
            existing_order=bid_order,
            replaced_cid=bid_replaced_cid,
            cancel_requested=bid_cancel_requested,
            target_quantity=bid_size_pre,
        )
        record_exact_decision(
            side="SELL",
            baseline_price=ask_price,
            exposure_increasing=ask_exposure_increasing,
            can_post=can_ask,
            action=ask_action,
            submitted_cid=ask_submitted_cid,
            existing_order=ask_order,
            replaced_cid=ask_replaced_cid,
            cancel_requested=ask_cancel_requested,
            target_quantity=ask_size_pre,
        )

        self._append_row(
            self._quote_log_path,
            QuoteDecisionLogRow(
                timestamp=f"{time.time():.3f}",
                symbol=symbol,
                side="BUY",
                mode=bid_policy.mode,
                allow_post=int(bid_policy.allow_post),
                allow_exposure_increase=int(bid_policy.allow_exposure_increase),
                reason_mask=bid_policy.reason_mask,
                reason_text=bid_policy.reason_text,
                spread_mult=bid_policy.spread_mult,
                size_mult=bid_policy.size_mult,
                inventory_ratio=bid_policy.inventory_ratio,
                toxicity=bid_policy.toxicity,
                markout_ema=bid_policy.markout_ema,
                depth_age_s=bid_policy.depth_age_s,
                microprice_shift_bps=bid_policy.microprice_shift_bps,
                l2_quote_flip_rate=bid_policy.l2_quote_flip_rate,
                l2_book_refresh_ratio=bid_policy.l2_book_refresh_ratio,
                l2_book_cancel_ratio=bid_policy.l2_book_cancel_ratio,
                l2_near_depth_total=bid_policy.l2_near_depth_total,
                bid_quote_ev_30s=bid_policy.bid_quote_ev_30s,
                bid_quote_toxic_30s=bid_policy.bid_quote_toxic_30s,
                bid_quote_fill_prob=bid_policy.bid_quote_fill_prob,
                bid_quote_fill_markout_30s=bid_policy.bid_quote_fill_markout_30s,
                mid=mid,
                base_price=base_bid_price,
                final_price=bid_price,
                base_size=base_size,
                final_size=bid_size_pre,
                can_post_after_inventory=int(can_bid_after_inventory),
                order_active_before=int(bool(bid_alive)),
                needs_update=int(bool(bid_needs_update)),
                action=bid_action,
            ),
        )
        self._append_row(
            self._quote_log_path,
            QuoteDecisionLogRow(
                timestamp=f"{time.time():.3f}",
                symbol=symbol,
                side="SELL",
                mode=ask_policy.mode,
                allow_post=int(ask_policy.allow_post),
                allow_exposure_increase=int(ask_policy.allow_exposure_increase),
                reason_mask=ask_policy.reason_mask,
                reason_text=ask_policy.reason_text,
                spread_mult=ask_policy.spread_mult,
                size_mult=ask_policy.size_mult,
                inventory_ratio=ask_policy.inventory_ratio,
                toxicity=ask_policy.toxicity,
                markout_ema=ask_policy.markout_ema,
                depth_age_s=ask_policy.depth_age_s,
                microprice_shift_bps=ask_policy.microprice_shift_bps,
                l2_quote_flip_rate=ask_policy.l2_quote_flip_rate,
                l2_book_refresh_ratio=ask_policy.l2_book_refresh_ratio,
                l2_book_cancel_ratio=ask_policy.l2_book_cancel_ratio,
                l2_near_depth_total=ask_policy.l2_near_depth_total,
                bid_quote_ev_30s=ask_policy.bid_quote_ev_30s,
                bid_quote_toxic_30s=ask_policy.bid_quote_toxic_30s,
                bid_quote_fill_prob=ask_policy.bid_quote_fill_prob,
                bid_quote_fill_markout_30s=ask_policy.bid_quote_fill_markout_30s,
                mid=mid,
                base_price=base_ask_price,
                final_price=ask_price,
                base_size=base_size,
                final_size=ask_size_pre,
                can_post_after_inventory=int(can_ask_after_inventory),
                order_active_before=int(bool(ask_alive)),
                needs_update=int(bool(ask_needs_update)),
                action=ask_action,
            ),
        )

        self._last_bid_action = bid_action
        self._last_ask_action = ask_action
        self._last_cpp_routing_used = int(bool(cpp_routing_used))
        self._last_routed_bid_price = float(bid_price)
        self._last_routed_ask_price = float(ask_price)
        return bid_needs_update, ask_needs_update

    def _record_cross_venue_fair_price_shadow(
        self,
        *,
        symbol: str,
        mid: float,
        baseline_bid: float,
        baseline_ask: float,
        best_bid: float,
        best_ask: float,
        decision_ts_ns: Optional[int] = None,
        state: Optional[CrossVenueFairPriceState] = None,
    ):
        """Write a candidate pair without mutating live quote variables."""

        if not bool(
            getattr(
                self.cfg.strategy,
                "cross_venue_fair_price_shadow_enabled",
                False,
            )
        ):
            return None
        decision_ns = int(decision_ts_ns or time.time_ns())
        try:
            if state is None:
                state = self.signal.cross_venue_fair_price_state(
                    local_mid=float(mid),
                    now_ns=decision_ns,
                )
            shadow = project_fair_center_shadow(
                state,
                baseline_bid=float(baseline_bid),
                baseline_ask=float(baseline_ask),
                best_bid=float(best_bid),
                best_ask=float(best_ask),
                tick_size=float(self.cfg.tick_size),
            )
            self._append_row(
                self._cross_venue_fair_price_shadow_log_path,
                CrossVenueFairPriceShadowLogRow(
                    timestamp=f"{time.time():.3f}",
                    decision_ts_ns=decision_ns,
                    symbol=str(symbol),
                    schema_version=FAIR_PRICE_SCHEMA_VERSION,
                    feature_graph_sha256=CROSS_VENUE_FAIR_PRICE_GRAPH.sha256(),
                    valid=int(bool(shadow.valid)),
                    reason=str(shadow.reason),
                    action_authorized=0,
                    executed_action="shadow_only",
                    local_mid=float(state.local_mid),
                    external_fair=float(state.fair_price),
                    raw_lead_bps=float(state.raw_lead_bps),
                    gain=float(state.gain),
                    center_shift_price=float(state.center_shift_price),
                    center_shift_bps=float(state.center_shift_bps),
                    confidence=float(state.confidence),
                    dispersion_bps=float(state.dispersion_bps),
                    valid_venues=int(state.valid_venues),
                    venue_ids="|".join(state.venue_ids),
                    minimum_basis_samples=int(state.minimum_basis_samples),
                    lead_variance_bps2=float(state.lead_variance_bps2),
                    noise_variance_bps2=float(state.noise_variance_bps2),
                    max_source_age_ms=float(state.max_source_age_ms),
                    max_feed_latency_ms=float(state.max_feed_latency_ms),
                    max_feature_latency_ms=float(state.max_feature_latency_ms),
                    source_kinds="|".join(state.source_kinds),
                    transport_supported=int(bool(state.transport_supported)),
                    baseline_bid=float(shadow.baseline_bid),
                    baseline_ask=float(shadow.baseline_ask),
                    candidate_bid=float(shadow.candidate_bid),
                    candidate_ask=float(shadow.candidate_ask),
                    requested_shift_ticks=int(shadow.requested_shift_ticks),
                    effective_shift_ticks=int(shadow.effective_shift_ticks),
                    gtx_clamped=int(bool(shadow.gtx_clamped)),
                    pair_spread_preserved=int(bool(shadow.pair_spread_preserved)),
                ),
            )
            self._cross_venue_fair_price_shadow_rows += 1
            self._cross_venue_fair_price_shadow_valid_rows += int(shadow.valid)
            self._cross_venue_fair_price_shadow_last_time = time.time()
            return shadow
        except Exception as exc:
            now = time.time()
            if now - self._cross_venue_fair_price_shadow_last_warning >= 60.0:
                logger.warning("Cross-venue fair-price shadow failed closed: %s", exc)
                self._cross_venue_fair_price_shadow_last_warning = now
            return None

    def _policy_order_ttl_expired(self, policy: SidePolicyDecision, order) -> bool:
        """Return true when a policy TTL requires a real replace/cancel cycle."""
        if order is None or not getattr(order, "is_active", False):
            return False
        ttl_ms = float(getattr(policy, "order_ttl_ms", 0.0) or 0.0)
        if ttl_ms <= 0.0:
            return False
        return (time.time() - float(getattr(order, "create_time", time.time()))) * 1000.0 >= ttl_ms

    def _replace_throttle_params(self, side: Side, q: float) -> tuple[float, float, bool]:
        """Return price/age replace throttle for a side.

        中文说明：这是 live order lifecycle 节流，不是 alpha。加仓侧用更保守
        的阈值，减仓侧可用更短/更小阈值，避免库存退出被过度延迟。
        """
        exposure_increasing = (side == Side.BUY and q >= 0.0) or (side == Side.SELL and q <= 0.0)
        strat = self.cfg.strategy
        if exposure_increasing:
            ticks = float(getattr(strat, "replace_min_price_change_ticks", 0.0) or 0.0)
            interval_ms = float(getattr(strat, "replace_min_interval_ms", 0.0) or 0.0)
        else:
            ticks = float(
                getattr(
                    strat,
                    "replace_min_price_change_ticks_reducing",
                    getattr(strat, "replace_min_price_change_ticks", 0.0),
                )
                or 0.0
            )
            interval_ms = float(
                getattr(
                    strat,
                    "replace_min_interval_ms_reducing",
                    getattr(strat, "replace_min_interval_ms", 0.0),
                )
                or 0.0
            )
        return max(0.0, ticks), max(0.0, interval_ms), exposure_increasing

    def _apply_replace_throttle(
        self,
        *,
        side: Side,
        now_ts: float,
        q: float,
        target_price: float,
        order,
        needs_update: bool,
        force_update: bool,
    ) -> bool:
        """Coalesce small/too-frequent live replaces to reduce REST tail latency."""
        if not needs_update or force_update:
            return needs_update
        if order is None or not getattr(order, "is_active", False) or getattr(order, "price", 0.0) <= 0.0:
            return needs_update
        tick = max(float(getattr(self.cfg, "tick_size", 0.0) or 0.0), 1e-12)
        price_ticks, interval_ms, exposure_increasing = self._replace_throttle_params(side, q)
        price_delta_ticks = abs(float(target_price) - float(order.price)) / tick
        age_ms = max(0.0, (now_ts - float(getattr(order, "create_time", now_ts))) * 1000.0)
        throttle_by_price = price_ticks > 0.0 and price_delta_ticks + 1e-9 < price_ticks
        throttle_by_age = interval_ms > 0.0 and age_ms < interval_ms
        if not (throttle_by_price or throttle_by_age):
            return needs_update

        side_name = side.value
        self._replace_throttle_counts[side_name] = self._replace_throttle_counts.get(side_name, 0) + 1
        last_log = self._last_replace_throttle_log.get(side_name, 0.0)
        if now_ts - last_log >= 60.0:
            self._last_replace_throttle_log[side_name] = now_ts
            logger.info(
                "REPLACE_THROTTLE side=%s exposure_increasing=%s "
                "price_delta_ticks=%.2f min_ticks=%.2f age_ms=%.0f min_interval_ms=%.0f "
                "count=%d",
                side_name,
                int(exposure_increasing),
                price_delta_ticks,
                price_ticks,
                age_ms,
                interval_ms,
                self._replace_throttle_counts[side_name],
            )
        return False

    @staticmethod
    def _order_lifecycle_pending(order) -> bool:
        """Return true for local order states that already have REST work in flight."""
        if order is None:
            return False
        return getattr(order, "state", None) in (OrderState.PENDING_NEW, OrderState.PENDING_CANCEL)

    def _apply_pending_replace_coalesce(
        self,
        *,
        side: Side,
        now_ts: float,
        q: float,
        target_price: float,
        order,
        needs_update: bool,
        can_post: bool,
    ) -> bool:
        """Avoid stacking a new replace while the previous order lifecycle is pending.

        中文说明：这是 live 系统层的 REST 尾延迟控制，不是策略 alpha。
        如果同侧订单仍处于 PENDING_NEW/PENDING_CANCEL，就等交易所状态收敛，
        避免一边撤单未确认、一边继续发新单造成 replace 风暴。
        """
        if not needs_update or not can_post:
            return False
        if not bool(getattr(self.cfg.strategy, "replace_pending_coalesce", True)):
            return False
        if not self._order_lifecycle_pending(order):
            return False

        side_name = side.value
        self._replace_pending_coalesce_counts[side_name] = (
            self._replace_pending_coalesce_counts.get(side_name, 0) + 1
        )
        last_log = self._last_replace_pending_coalesce_log.get(side_name, 0.0)
        if now_ts - last_log >= 60.0:
            self._last_replace_pending_coalesce_log[side_name] = now_ts
            state_name = getattr(getattr(order, "state", None), "name", "UNKNOWN")
            tick = max(float(getattr(self.cfg, "tick_size", 0.0) or 0.0), 1e-12)
            price_delta_ticks = 0.0
            if order is not None and getattr(order, "price", 0.0) > 0.0:
                price_delta_ticks = abs(float(target_price) - float(order.price)) / tick
            _, _, exposure_increasing = self._replace_throttle_params(side, q)
            logger.info(
                "REPLACE_PENDING_COALESCE side=%s state=%s exposure_increasing=%s "
                "price_delta_ticks=%.2f count=%d",
                side_name,
                state_name,
                int(exposure_increasing),
                price_delta_ticks,
                self._replace_pending_coalesce_counts[side_name],
            )
        return True

    def _should_cancel_first_replace(
        self,
        *,
        side: Side,
        q: float,
        order,
        needs_update: bool,
        force_update: bool,
        can_post: bool,
    ) -> bool:
        """Optional soak arm: cancel exposure-increasing quote before replacement.

        This is a stepping stone toward an async order manager. It only applies to
        exposure-increasing replaces, and it deliberately does not apply to forced
        TTL/stale/pause cancels or inventory-reducing quotes.
        """
        if not bool(getattr(self.cfg.strategy, "replace_cancel_first_exposure_increasing", False)):
            return False
        if not needs_update or force_update or not can_post:
            return False
        if order is None or not getattr(order, "is_active", False):
            return False
        _, _, exposure_increasing = self._replace_throttle_params(side, q)
        if not exposure_increasing:
            return False

        side_name = side.value
        now_ts = time.time()
        self._replace_cancel_first_counts[side_name] = (
            self._replace_cancel_first_counts.get(side_name, 0) + 1
        )
        last_log = self._last_replace_cancel_first_log.get(side_name, 0.0)
        if now_ts - last_log >= 60.0:
            self._last_replace_cancel_first_log[side_name] = now_ts
            logger.info(
                "REPLACE_CANCEL_FIRST side=%s exposure_increasing=1 count=%d",
                side_name,
                self._replace_cancel_first_counts[side_name],
            )
        return True

    def _apply_side_policy_price(self, side: Side, mid: float, price: float, spread_mult: float) -> float:
        tick = self.cfg.tick_size
        if mid <= 0 or abs(spread_mult - 1.0) <= 1e-12:
            return price
        spread_mult = max(0.05, float(spread_mult))
        if side == Side.BUY:
            dist = max(mid - price, tick)
            adj = math.floor((mid - dist * spread_mult) / tick) * tick
            return min(adj, mid - tick)
        dist = max(price - mid, tick)
        adj = math.ceil((mid + dist * spread_mult) / tick) * tick
        return max(adj, mid + tick)

    def _apply_post_fill_quote_response(
        self,
        *,
        q: float,
        bid_price: float,
        ask_price: float,
        pred: Prediction,
        bid_policy: SidePolicyDecision,
        ask_policy: SidePolicyDecision,
        best_bid: float,
        best_ask: float,
        now_ms: int | None = None,
    ) -> tuple[float, float]:
        """Apply the same discrete I/A quote transform used by Python replay."""

        cfg = self.cfg
        refill_edge = (
            float(bid_policy.l2_book_refresh_ratio)
            - float(bid_policy.l2_book_cancel_ratio)
            if q > 0.0
            else float(ask_policy.l2_book_refresh_ratio)
            - float(ask_policy.l2_book_cancel_ratio)
            if q < 0.0
            else 0.0
        )
        response = self._post_fill_quote_response.quote(
            now_ms=int(now_ms if now_ms is not None else time.time() * 1000.0),
            inventory=float(q),
            order_size=float(cfg.strategy.order_size),
            baseline_bid=float(bid_price),
            baseline_ask=float(ask_price),
            tick_size=float(cfg.tick_size),
            max_pair_spread=float(
                self._last_quote_diagnostics.get("max_spread", 0.0) or 0.0
            ),
            best_bid=float(best_bid),
            best_ask=float(best_ask),
            volatility_bps=(
                math.sqrt(max(float(getattr(pred, "vol_10s", 0.0) or 0.0), 0.0))
                / max(0.5 * (float(bid_price) + float(ask_price)), 1e-12)
                * 10_000.0
            ),
            refill_edge=refill_edge,
            repair_probability=math.nan,
        )
        if response.active and self._requote_count % 6 == 0:
            logger.info(
                "POST_FILL_QUOTE_RESPONSE mode=%s q=%+.6f add_side=%s "
                "inventory_ticks=%d add_ticks=%d half_life_s=%.3f",
                response.mode,
                q,
                response.add_side,
                response.inventory_shift_ticks,
                response.add_widen_ticks,
                response.effective_half_life_s,
            )
        return response.bid_price, response.ask_price

    def _apply_post_policy_spread_cap(
        self,
        mid: float,
        bid_price: float,
        ask_price: float,
        *,
        inventory: float,
        best_bid: float,
        best_ask: float,
    ) -> tuple[float, float, bool]:
        max_spread = float(self._last_quote_diagnostics.get("max_spread", 0.0) or 0.0)
        if max_spread <= 0.0:
            cap_bps = float(getattr(self.cfg.strategy, "max_spread_bps", 0.0) or 0.0)
            if cap_bps > 0.0 and mid > 0.0:
                max_spread = mid * cap_bps / 10000.0
        role_safe_ber = bool(
            getattr(self.cfg.strategy, "ber_exposure_add_only", False)
            and self._ber_active
        )
        cap_role_safe = False
        cap_role_safe_feasible = True
        if role_safe_ber:
            quantity = float(self.cfg.strategy.order_size)
            buy_role = ber_inventory_role_for_target(
                "BUY", inventory, quantity
            )
            sell_role = ber_inventory_role_for_target(
                "SELL", inventory, quantity
            )
            preserve_side = (
                "SELL" if buy_role == "add" and sell_role == "reducing"
                else (
                    "BUY" if sell_role == "add" and buy_role == "reducing"
                    else ""
                )
            )
            if preserve_side:
                (
                    bid_new,
                    ask_new,
                    cap_hit,
                    _,
                    cap_role_safe_feasible,
                ) = apply_final_spread_cap_preserve_side(
                    mid,
                    bid_price,
                    ask_price,
                    max_spread,
                    self.cfg.tick_size,
                    preserve_side=preserve_side,
                )
                cap_role_safe = cap_hit
            else:
                bid_new, ask_new, cap_hit, _ = apply_final_spread_cap(
                    mid, bid_price, ask_price, max_spread, self.cfg.tick_size
                )
        else:
            bid_new, ask_new, cap_hit, _ = apply_final_spread_cap(
                mid, bid_price, ask_price, max_spread, self.cfg.tick_size
            )
        if not cap_hit:
            return bid_price, ask_price, False

        cap_mode = spread_cap_mode_code(getattr(self.cfg.strategy, "spread_cap_mode", "compress"))
        if cap_mode != SPREAD_CAP_COMPRESS:
            return bid_price, ask_price, True

        if cap_role_safe and not cap_role_safe_feasible:
            logger.error(
                "BER_ROLE_SAFE_CAP_FAIL_CLOSED q=%+.8f bid=%.8f ask=%.8f cap=%.8f",
                inventory,
                bid_price,
                ask_price,
                max_spread,
            )
            bid_new, ask_new, _, _ = apply_final_spread_cap(
                mid, bid_price, ask_price, max_spread, self.cfg.tick_size
            )

        tick = self.cfg.tick_size
        if best_ask > 0.0 and bid_new >= best_ask:
            bid_new = best_ask - tick
        if best_bid > 0.0 and ask_new <= best_bid:
            ask_new = best_bid + tick
        return bid_new, ask_new, True

    def _log_order_outcome(self, event_type: str, order, **extra):
        context = self._get_order_context(order.client_order_id) if order is not None else {}
        decision_ts = float(context.get("decision_ts", getattr(order, "create_time", time.time())))
        age_ms = max(0, int((time.time() - decision_ts) * 1000))
        self._append_row(
            self._order_outcome_log_path,
            OrderOutcomeLogRow(
                timestamp=f"{time.time():.3f}",
                symbol=getattr(order, "symbol", self.cfg.symbol),
                event_type=event_type,
                client_order_id=getattr(order, "client_order_id", ""),
                side=getattr(getattr(order, "side", None), "value", ""),
                price=float(getattr(order, "price", 0.0)),
                quantity=float(getattr(order, "quantity", 0.0)),
                filled_qty=float(extra.get("filled_qty", getattr(order, "filled_qty", 0.0))),
                avg_fill_price=float(extra.get("avg_fill_price", getattr(order, "avg_fill_price", 0.0))),
                age_ms=age_ms,
                mode=str(context.get("mode", "na")),
                reason_mask=int(context.get("reason_mask", 0)),
                reason_text=str(context.get("reason_text", "none")),
                spread_mult=float(context.get("spread_mult", 1.0)),
                size_mult=float(context.get("size_mult", 1.0)),
                inventory_ratio=float(context.get("inventory_ratio", 0.0)),
                toxicity=float(context.get("toxicity", 0.5)),
                markout_ema=float(context.get("markout_ema", 0.0)),
                depth_age_s=float(context.get("depth_age_s", 0.0)),
                microprice_shift_bps=float(context.get("microprice_shift_bps", 0.0)),
                l2_quote_flip_rate=float(context.get("l2_quote_flip_rate", 0.0)),
                l2_book_refresh_ratio=float(context.get("l2_book_refresh_ratio", 0.0)),
                l2_book_cancel_ratio=float(context.get("l2_book_cancel_ratio", 0.0)),
                l2_near_depth_total=float(context.get("l2_near_depth_total", 0.0)),
                bid_quote_ev_30s=float(context.get("bid_quote_ev_30s", 0.0)),
                bid_quote_toxic_30s=float(context.get("bid_quote_toxic_30s", 0.0)),
                bid_quote_fill_prob=float(context.get("bid_quote_fill_prob", 0.0)),
                bid_quote_fill_markout_30s=float(context.get("bid_quote_fill_markout_30s", 0.0)),
                mid=float(context.get("mid", 0.0)),
                target_price=float(context.get("target_price", getattr(order, "price", 0.0))),
                target_qty=float(context.get("target_qty", getattr(order, "quantity", 0.0))),
            ),
        )

    def _place_order(self, symbol: str, side: Side,
                     price: float, quantity: float,
                     reduce_only: bool = False,
                     decision_context: Optional[dict] = None,
                     record_requote_perf: bool = True) -> Optional[str]:
        """Send limit order to exchange."""
        # Floor quantity to lot_size so local state matches exchange-accepted qty
        qty_str = self._fmt_qty(quantity)
        quantity = float(qty_str)

        cid = self.orders.create_order(symbol, side, price, quantity)
        if decision_context is not None:
            self._set_order_context(cid, decision_context)
        self._record_exact_order_event(
            self.orders.get_order(cid),
            "submit",
        )

        try:
            params = dict(
                symbol=symbol,
                side=side.value,
                type="LIMIT",
                timeInForce="GTX",
                quantity=qty_str,
                price=self._fmt_price(price),
                newClientOrderId=cid,
                newOrderRespType="RESULT",
            )
            if reduce_only:
                params["reduceOnly"] = "true"
            rest_start = time.perf_counter()
            try:
                resp = self.rest.new_order(**params)
            finally:
                if record_requote_perf:
                    self._record_perf_rest_latency(
                        "new", (time.perf_counter() - rest_start) * 1_000_000.0
                    )
            oid = resp.get("orderId", 0)
            status = resp.get("status", "NEW")

            if status == "EXPIRED":
                # GTX rejected: order would cross the book
                self.orders.confirm_rejected(cid, "GTX_CROSSED")
                order = self.orders.get_order(cid)
                if order is not None:
                    self._log_order_outcome("reject_gtx", order)
                self._pop_order_context(cid)
                logger.debug(f"GTX expired (would cross): {side.value} {quantity}@{price}")
                return None

            self.orders.confirm_new(
                cid,
                oid,
                exchange_ts_ns=self._rest_exchange_timestamp_ns(resp),
            )
            order = self.orders.get_order(cid)
            if order is not None:
                self._log_order_outcome("placed", order)

            with self._order_ref_lock:
                if side == Side.BUY:
                    self._bid_cid = cid
                else:
                    self._ask_cid = cid
            return cid

        except Exception as e:
            self.orders.confirm_rejected(cid, str(e))
            order = self.orders.get_order(cid)
            if order is not None:
                self._log_order_outcome("reject_error", order)
            self._pop_order_context(cid)
            err_str = str(e)
            if "-5022" in err_str:
                # Post Only would cross book — treat as GTX rejection, not error
                logger.debug(f"GTX rejected (would cross): {side.value} {quantity}@{price}")
            else:
                logger.error(f"Order place failed: {side.value} {quantity}@{price}: {e}")
            return None

    def _place_close_order(self, symbol: str, side: Side,
                          price: float, quantity: float,
                          decision_context: Optional[dict] = None,
                          *, use_ioc: bool = False):
        """Send one reduce-only close order using the caller-selected TIF."""
        MAX_GTX_REJECTS = 3

        # Floor quantity to lot_size so local state matches exchange-accepted qty
        qty_str = self._fmt_qty(quantity)
        quantity = float(qty_str)

        cid = self.orders.create_order(symbol, side, price, quantity)
        if decision_context is not None:
            self._set_order_context(cid, decision_context)
        self._record_exact_order_event(
            self.orders.get_order(cid),
            "submit",
        )
        try:
            tif = "IOC" if use_ioc else "GTX"
            params = dict(
                symbol=symbol,
                side=side.value,
                type="LIMIT",
                timeInForce=tif,
                quantity=qty_str,
                price=self._fmt_price(price),
                newClientOrderId=cid,
                newOrderRespType="RESULT",
                reduceOnly="true",
            )
            rest_start = time.perf_counter()
            try:
                resp = self.rest.new_order(**params)
            finally:
                self._record_perf_rest_latency(
                    "new", (time.perf_counter() - rest_start) * 1_000_000.0
                )
            oid = resp.get("orderId", 0)
            status = resp.get("status", "NEW")

            if status == "EXPIRED":
                if use_ioc:
                    # Keep IOC escalation latched. A marketable limit can still
                    # expire if the touch moves before exchange arrival; retry
                    # from the next fresh BBO instead of falling back to GTX.
                    self.orders.confirm_rejected(cid, "IOC_EXPIRED")
                    order = self.orders.get_order(cid)
                    if order is not None:
                        self._log_order_outcome("reject_ioc_expired", order)
                    self._pop_order_context(cid)
                    self._close_gtx_rejects = max(
                        self._close_gtx_rejects,
                        MAX_GTX_REJECTS,
                    )
                    logger.warning(f"IOC close expired: {side.value} {quantity}@{price}")
                else:
                    self.orders.confirm_rejected(cid, "GTX_CROSSED")
                    order = self.orders.get_order(cid)
                    if order is not None:
                        self._log_order_outcome("reject_gtx_close", order)
                    self._pop_order_context(cid)
                    self._close_gtx_rejects += 1
                    logger.warning(
                        f"GTX close rejected ({self._close_gtx_rejects}/{MAX_GTX_REJECTS}): "
                        f"{side.value} {quantity}@{price}"
                    )
                return

            # A passive close accepted by the exchange clears the rejection
            # streak. IOC remains latched until inventory is actually flat.
            if use_ioc:
                self._close_gtx_rejects = max(
                    self._close_gtx_rejects,
                    MAX_GTX_REJECTS,
                )
            else:
                self._close_gtx_rejects = 0
            self.orders.confirm_new(
                cid,
                oid,
                exchange_ts_ns=self._rest_exchange_timestamp_ns(resp),
            )
            order = self.orders.get_order(cid)
            if order is not None:
                self._log_order_outcome("placed_close", order)

            if side == Side.BUY:
                self._bid_cid = cid
            else:
                self._ask_cid = cid

            if use_ioc:
                logger.info(f"IOC close placed: {side.value} {quantity}@{price}")

        except Exception as e:
            self.orders.confirm_rejected(cid, str(e))
            order = self.orders.get_order(cid)
            err_str = str(e)
            if "-5022" in err_str and not use_ioc:
                if order is not None:
                    self._log_order_outcome("reject_gtx_close", order)
                self._close_gtx_rejects += 1
                logger.warning(
                    f"GTX close rejected ({self._close_gtx_rejects}/{MAX_GTX_REJECTS}): "
                    f"{side.value} {quantity}@{price}"
                )
            else:
                if order is not None:
                    self._log_order_outcome("reject_close_error", order)
                if use_ioc:
                    self._close_gtx_rejects = max(
                        self._close_gtx_rejects,
                        MAX_GTX_REJECTS,
                    )
                logger.error(
                    f"Close order failed tif={'IOC' if use_ioc else 'GTX'}: "
                    f"{side.value} {quantity}@{price}: {e}"
                )
            self._pop_order_context(cid)

    def _cancel_all_orders(self):
        """Cancel all active orders."""
        active = self.orders.get_active_orders()
        if not active:
            return

        marked_ids: list[str] = []
        for order in active:
            if order.state == OrderState.PENDING_CANCEL:
                continue
            self.orders.mark_pending_cancel(order.client_order_id)
            marked_ids.append(order.client_order_id)
            self._record_exact_order_event(order, "cancel_request")
        try:
            rest_start = time.perf_counter()
            try:
                self.rest.cancel_open_orders(symbol=self.cfg.symbol)
            finally:
                self._record_perf_rest_latency(
                    "cancel_all", (time.perf_counter() - rest_start) * 1_000_000.0
                )
            logger.debug(f"Canceled {len(active)} orders")
        except Exception as e:
            for cid in marked_ids:
                self.orders.cancel_rejected(cid, str(e))
            logger.error(f"Cancel all orders failed: {e}")

    def _cancel_order(
        self,
        cid: str,
        *,
        record_requote_perf: bool = True,
        trigger_decision_id: str = "",
    ) -> bool:
        """Cancel a single order by client order id."""
        order = self.orders.get_order(cid)
        if not order or order.is_terminal:
            return True

        already_pending = order.state == OrderState.PENDING_CANCEL
        self.orders.mark_pending_cancel(cid)
        if not already_pending:
            self._record_exact_order_event(
                order,
                "cancel_request",
                trigger_decision_id=trigger_decision_id,
            )
        try:
            rest_start = time.perf_counter()
            try:
                self.rest.cancel_order(
                    symbol=self.cfg.symbol,
                    origClientOrderId=cid,
                )
            finally:
                if record_requote_perf:
                    self._record_perf_rest_latency(
                        "cancel", (time.perf_counter() - rest_start) * 1_000_000.0
                    )
            return True
        except Exception as e:
            if not already_pending:
                self.orders.cancel_rejected(cid, str(e))
            logger.error(f"Cancel order {cid} failed: {e}")
            return False

    def _cancel_cooldown_side_order(self, side: str):
        """Cancel the same-side quote immediately after a fill starts cooldown."""
        self._cancel_active_side_orders(side, "FILL_CD_CANCEL")

    def _cancel_active_side_orders(self, side: str, reason: str):
        """Cancel every active order on one side and clear the tracked quote cid."""
        if side == "BUY":
            active_orders = self.orders.get_bid_orders()
            self._bid_cid = None
        else:
            active_orders = self.orders.get_ask_orders()
            self._ask_cid = None

        canceled = 0
        seen = set()
        for order in active_orders:
            cid = order.client_order_id
            if cid in seen or not order.is_active:
                continue
            seen.add(cid)
            self._cancel_order(cid)
            canceled += 1

        if canceled:
            logger.info(f"{reason}: {side} active_orders_canceled={canceled}")

    def _block_stale_quote_data(self, book_age: float, max_age: float):
        """Cancel live quotes and skip requote when execution book data is stale."""
        active_count = self.orders.active_count()
        if active_count > 0:
            self._cancel_all_orders()

        now = time.time()
        if now - self._last_stale_data_block_log < 10.0:
            return

        age_text = "missing" if not math.isfinite(book_age) else f"{book_age:.2f}s"
        logger.warning(
            f"QUOTE_STALE_DATA_BLOCK: exec_depth_age={age_text} "
            f"max_age={max_age:.2f}s active_orders={active_count}"
        )
        self._last_stale_data_block_log = now

    # ── risk management ──

    def _risk_check(self, snap, mid: float) -> bool:
        """Return False if trading should be paused."""
        cfg = self.cfg.risk

        if self._commission_unit_error:
            logger.critical(
                "RISK: commission accounting is not unit-safe: %s",
                self._commission_unit_error,
            )
            self._cancel_all_orders()
            return False

        # Daily loss limit
        daily_pnl = self.inventory.daily_pnl
        if daily_pnl < -cfg.max_daily_loss:
            logger.warning(f"RISK: Daily loss limit hit: {daily_pnl:.2f}")
            self._cancel_all_orders()
            return False

        # Position value limit
        pos_value = abs(self.inventory.net_position) * mid
        if pos_value > cfg.max_position_value:
            logger.warning(f"RISK: Position value limit: {pos_value:.0f}")
            self._cancel_all_orders()
            return False

        # Drawdown emergency
        dd = self.inventory.drawdown
        if dd > cfg.emergency_close_dd:
            logger.critical(f"RISK: Emergency drawdown: {dd:.2f}")
            self._emergency_close(mid)
            return False

        # Consecutive losses
        if self.inventory.consecutive_losses >= cfg.max_consecutive_losses:
            logger.warning(
                f"RISK: {self.inventory.consecutive_losses} consecutive losses, cooling down"
            )
            self._cooldown_until = time.time() + cfg.cooldown_after_loss
            self._cancel_all_orders()
            return False

        return True

    def _handle_closing_requote(
        self,
        q: float,
        mid: float,
        pred,
        *,
        quote_snapshot: Optional[QuoteDecisionSnapshot] = None,
        post_only_guard: Optional[QuotePostOnlyGuard] = None,
    ):
        """
        During TIMEOUT_CLOSING: only place a limit order on the
        closing side (aggressive, inside the spread) — never open new positions.

        Stale-time escalation tiers:
          0-30s: GTX at edge of spread (passive maker close)
          30-60s: GTX 1 tick into the spread (more aggressive)
          60s+: marketable reduce-only LIMIT IOC, retried until flat
        """
        cfg = self.cfg
        tick = cfg.tick_size
        lot = cfg.lot_size
        guard = post_only_guard
        if guard is None and quote_snapshot is not None:
            guard = self._post_only_guard_for_snapshot(quote_snapshot)
        best_bid = (
            float(guard.best_bid)
            if guard is not None
            else float(self._best_bid)
        )
        best_ask = (
            float(guard.best_ask)
            if guard is not None
            else float(self._best_ask)
        )
        close_side = Side.SELL if q > 0 else Side.BUY
        qty = abs(q)

        # Dust position below lot_size — can't close, treat as flat
        if qty < lot:
            logger.info(f"CLOSING dust position {q:+.4f} < lot_size {lot}, resetting to FLAT")
            self.inventory.force_flat()
            return

        # Cancel any order on the opening side (should not exist, but safety)
        if q > 0 and self._ask_cid is None and self._bid_cid:
            self._cancel_order(self._bid_cid)
            self._bid_cid = None
        elif q < 0 and self._bid_cid is None and self._ask_cid:
            self._cancel_order(self._ask_cid)
            self._ask_cid = None

        # Round close quantity to lot_size
        close_qty = min(qty, cfg.strategy.order_size)
        close_qty = math.floor(close_qty / lot) * lot
        if close_qty < lot:
            close_qty = lot

        # Stale-time escalation
        stale_seconds = time.time() - self._close_start_time if self._close_start_time > 0 else 0.0
        # Also check reject-based escalation (original logic)
        use_ioc = self._close_gtx_rejects >= 3 or stale_seconds >= 60.0
        aggressive_passive = not use_ioc and stale_seconds >= 30.0

        if use_ioc:
            # Tier 3: IOC taker — cancel existing order and send IOC
            if close_side == Side.SELL:
                if self._ask_cid:
                    self._cancel_order(self._ask_cid)
                    self._ask_cid = None
                touch = best_bid if best_bid > 0.0 else mid
                close_price = math.floor((touch - 2.0 * tick) / tick) * tick
            else:
                if self._bid_cid:
                    self._cancel_order(self._bid_cid)
                    self._bid_cid = None
                touch = best_ask if best_ask > 0.0 else mid
                close_price = math.ceil((touch + 2.0 * tick) / tick) * tick
            close_price = round(close_price, self._price_precision)
            logger.warning(
                f"CLOSING_IOC stale={stale_seconds:.0f}s rejects={self._close_gtx_rejects}: "
                f"{close_side.value} {close_qty:.3f} @ {close_price:.1f}"
            )
            self._place_close_order(
                cfg.symbol,
                close_side,
                close_price,
                close_qty,
                use_ioc=True,
            )
            return

        # Tier 1-2: GTX passive (possibly with 1-tick aggression)
        if close_side == Side.SELL:
            close_price = round(math.ceil(mid / tick) * tick, 1)
            if aggressive_passive:
                close_price -= tick  # slide 1 tick into spread
                close_price = round(close_price, 1)
            if self._ask_cid:
                ask_order = self.orders.get_order(self._ask_cid)
                if ask_order and ask_order.is_active:
                    drift = abs(close_price - ask_order.price) / ask_order.price
                    if drift <= cfg.strategy.requote_threshold_bps / 10000.0:
                        return  # keep existing close order
                    self._cancel_order(self._ask_cid)
                    self._ask_cid = None
            self._place_close_order(
                cfg.symbol,
                Side.SELL,
                close_price,
                close_qty,
                use_ioc=False,
            )
        else:
            close_price = round(math.floor(mid / tick) * tick, 1)
            if aggressive_passive:
                close_price += tick  # slide 1 tick into spread
                close_price = round(close_price, 1)
            if self._bid_cid:
                bid_order = self.orders.get_order(self._bid_cid)
                if bid_order and bid_order.is_active:
                    drift = abs(close_price - bid_order.price) / bid_order.price
                    if drift <= cfg.strategy.requote_threshold_bps / 10000.0:
                        return
                    self._cancel_order(self._bid_cid)
                    self._bid_cid = None
            self._place_close_order(
                cfg.symbol,
                Side.BUY,
                close_price,
                close_qty,
                use_ioc=False,
            )

        tier = "AGGRESSIVE" if aggressive_passive else "PASSIVE"
        logger.debug(
            f"CLOSING_REQUOTE[{tier}] {close_side.value} {close_qty:.3f} @ {close_price:.1f} "
            f"(mid={mid:.1f}, stale={stale_seconds:.0f}s, remaining={qty:.4f})"
        )

    def _handle_position_timeout(self, q: float, mid: float):
        """Close position that has been held too long.
        Sets TIMEOUT_CLOSING state and cancels all orders.
        Subsequent ticks will use _handle_closing_requote() to place
        aggressive limit close orders (avoiding taker fees).
        """
        self.inventory.set_timeout_closing()
        self._cancel_all_orders()
        self._bid_cid = None
        self._ask_cid = None
        self._close_gtx_rejects = 0  # reset for new closing sequence
        self._close_start_time = time.time()  # track start for stale-time escalation

        side_str = "SELL" if q > 0 else "BUY"
        qty = abs(q)

        logger.warning(
            f"POSITION_TIMEOUT: will close {qty:.4f} {side_str} via limit orders"
        )

    def _emergency_close(self, mid: float):
        """Emergency: cancel all orders and close position at market."""
        self._cancel_all_orders()
        q = self.inventory.net_position

        if abs(q) > 1e-8:
            side_str = "SELL" if q > 0 else "BUY"
            side = Side.SELL if q > 0 else Side.BUY
            qty = abs(q)
            logger.critical(f"EMERGENCY_CLOSE: {side_str} {qty:.4f}")
            cid = self.orders.create_order(self.cfg.symbol, side, 0.0, qty)
            self._record_exact_order_event(
                self.orders.get_order(cid),
                "submit",
            )
            try:
                rest_start = time.perf_counter()
                try:
                    resp = self.rest.new_order(
                        symbol=self.cfg.symbol,
                        side=side_str,
                        type="MARKET",
                        quantity=self._fmt_qty(qty),
                        newClientOrderId=cid,
                        reduceOnly=True,
                    )
                finally:
                    self._record_perf_rest_latency(
                        "new", (time.perf_counter() - rest_start) * 1_000_000.0
                    )
                self.orders.confirm_new(
                    cid,
                    resp.get("orderId", 0),
                    exchange_ts_ns=self._rest_exchange_timestamp_ns(resp),
                )
            except Exception as e:
                self.orders.confirm_rejected(cid, str(e))
                logger.error(f"Emergency close failed: {e}")

        self._running = False

    # ── fill callbacks ──

    def _on_dynamic_fill_hazard_order_terminal(
        self,
        order: Any,
        *,
        terminal_reason: str,
    ) -> None:
        ws = self._ws_handler
        if ws is not None and hasattr(ws, "terminal_active_order_depth_path"):
            ws.terminal_active_order_depth_path(order.client_order_id)
        runtime = self._dynamic_fill_hazard_shadow_runtime
        if runtime is not None:
            runtime.drop_order(order.client_order_id)
        remaining_quantity = float(getattr(order, "remaining_qty", 0.0) or 0.0)
        route = terminal_policy_route(terminal_reason, remaining_quantity)
        if route == TerminalPolicyRoute.UNSUPPORTED:
            raise RuntimeError(
                f"unsupported q90 terminal reason: {terminal_reason}"
            )
        release_event = ""
        force_requote = False
        with self._dynamic_fill_hazard_action_lock:
            hold = self._dynamic_fill_hazard_action_hold
            if (
                hold is None
                or hold.client_order_id != order.client_order_id
            ):
                return
            terminal_ts_ns = int(
                getattr(getattr(order, "lifecycle", None), "terminal_ts_ns", 0)
                or time.time_ns()
            )
            hold.phase = OrderLifecyclePhase.EXCHANGE_TERMINAL
            hold.terminal_seen = True
            hold.exchange_terminal_ts_ns = terminal_ts_ns
            lifecycle = getattr(order, "lifecycle", None)
            if route == TerminalPolicyRoute.PROSPECTIVE_CANCEL_REENTRY:
                if lifecycle is None:
                    logger.error(
                        "BUY_HAZARD_POLICY_TERMINAL missing lifecycle cid=%s",
                        order.client_order_id,
                    )
                    return
                lifecycle.enter_post_cancel_recovery(terminal_ts_ns)
                hold.phase = OrderLifecyclePhase.POST_CANCEL_RECOVERY
            elif route == TerminalPolicyRoute.TERMINAL_COMPLETE:
                release_event = "terminal_complete_no_reentry"
            elif route == TerminalPolicyRoute.BASELINE_RESUBMIT:
                release_event = f"{terminal_reason}_baseline_resubmit"
                force_requote = True
            elif route == TerminalPolicyRoute.SHUTDOWN_NO_REENTRY:
                release_event = "shutdown_no_reentry"
            pre_ack_recovered = bool(hold.recovered)
        if release_event:
            self._release_dynamic_fill_hazard_action_hold(
                event=release_event,
                force_requote=force_requote,
            )
        logger.warning(
            "BUY_HAZARD_POLICY_TERMINAL reason=%s route=%s cid=%s "
            "phase=%s pre_ack_recovered=%d released=%d",
            terminal_reason,
            route.value,
            order.client_order_id,
            hold.phase.value,
            int(pre_ack_recovered),
            int(bool(release_event)),
        )

    def _on_order_terminal(self, order: Any, reason: str) -> None:
        """Route every exchange terminal outcome through one cleanup path."""

        runtime = getattr(self, "_exact_opportunity_tape_runtime", None)
        if runtime is not None:
            runtime.observe_order_terminal(order.client_order_id)
        self._on_dynamic_fill_hazard_order_terminal(
            order,
            terminal_reason=str(reason),
        )

    def _on_fill(self, order, event):
        """Called when an order is filled (from OrderManager)."""
        side = order.side.value
        qty = float(event.get("_fill_qty", event.get("l", 0)))
        price = float(event.get("_fill_price", event.get("L") or event.get("ap") or order.price))
        commission = float(event.get("_fill_commission", event.get("n", 0)))
        commission_asset = str(
            event.get("_fill_commission_asset", event.get("N", "")) or ""
        ).upper()
        trade_time_ms = int(event.get("T", 0))

        if qty <= 1e-10:
            logger.warning(f"ORDER_FILL ignored non-positive qty: {side} qty={qty} event={event}")
            return

        try:
            commission_quote = _commission_in_quote_asset(
                commission,
                commission_asset,
                fill_price=price,
                base_asset=self._base_asset,
                quote_asset=self._quote_asset,
                settlement_asset=self._settlement_asset,
            )
        except ValueError as exc:
            self._commission_unit_error = str(exc)
            commission_quote = 0.0
            logger.critical(
                "COMMISSION_UNIT_ERROR amount=%s asset=%s fill=%s@%s: %s; "
                "inventory will update without mixing currencies and quoting is blocked",
                commission,
                commission_asset or "<missing>",
                qty,
                price,
                exc,
            )

        self._log_order_outcome("filled", order, filled_qty=qty, avg_fill_price=price)
        prev_q = float(self.inventory.snapshot.qty)
        self.inventory.on_fill(side, qty, price, commission_quote, trade_time_ms)
        self._post_fill_quote_response.record_fill(
            side=side,
            inventory_before=prev_q,
            inventory_after=float(self.inventory.snapshot.qty),
            fill_qty=qty,
            order_size=max(self.cfg.strategy.order_size, self.cfg.lot_size),
            ts_ms=int(trade_time_ms or time.time() * 1000.0),
        )

        # Step 27: Track consecutive same-side fills for cooldown
        now = time.time()
        self._consec_buy, self._consec_sell, fill_units = update_same_side_fill_units(
            side=side,
            fill_qty=qty,
            order_size=self.cfg.strategy.order_size,
            lot_size=self.cfg.lot_size,
            buy_units=self._consec_buy,
            sell_units=self._consec_sell,
        )
        opposite = "SELL" if side == "BUY" else "BUY"
        self._fill_cooldown_until[opposite] = 0.0
        self._last_same_side_fill_epoch_ms[side] = int(
            trade_time_ms or time.time() * 1000.0
        )
        self._last_fill_side = side

        exposure_increasing_fill = (
            (side == "BUY" and prev_q >= 0.0)
            or (side == "SELL" and prev_q <= 0.0)
        )
        fc_add = float(getattr(self.cfg.strategy, 'fill_cooldown', 0.0) or 0.0)
        fc_reduce = float(getattr(self.cfg.strategy, 'fill_cooldown_reducing', 0.0) or 0.0)
        raw_fc = fc_add if exposure_increasing_fill else fc_reduce
        effective_fc = raw_fc
        cd_kind = "add" if exposure_increasing_fill else "reduce"
        vol_mult = 1.0
        if not exposure_increasing_fill and effective_fc > 0.0:
            if self._reducing_cooldown_campaign_gate_active(prev_q):
                vol_mult = self._reducing_cooldown_vol_mult()
                effective_fc *= vol_mult
            else:
                effective_fc = 0.0
        add_mult = 1.0
        if exposure_increasing_fill and effective_fc > 0.0:
            add_mult = self._adaptive_add_cooldown_multiplier(side, prev_q, self._consec_buy if side == "BUY" else self._consec_sell)
            effective_fc *= add_mult
        if effective_fc > 0:
            consec = self._consec_buy if side == "BUY" else self._consec_sell
            cd = effective_fc * max(1.0, consec)
            boolean_decision = None
            if (
                exposure_increasing_fill
                and side == "SELL"
                and self._boolean_cooldown_policy is not None
            ):
                campaign = self.inventory.campaign_snapshot()
                fill_visible_ts_ns = int(
                    event.get("_local_receive_ts_ns", 0) or time.time_ns()
                )
                event_id = str(
                    event.get("t")
                    or event.get("tradeId")
                    or event.get("T")
                    or fill_visible_ts_ns
                )
                cd, boolean_decision = self._select_boolean_cooldown_duration(
                    side=side,
                    exposure_increasing_fill=exposure_increasing_fill,
                    baseline_duration_s=cd,
                    campaign_age_s=float(campaign.age_s),
                    fill_visible_ts_ns=fill_visible_ts_ns,
                    snapshot_id=(
                        f"live-fill-{order.client_order_id}-{event_id}"
                    ),
                )
                logger.warning(
                    "F05_BOOLEAN_COOLDOWN side=%s action=%s baseline_ms=%d "
                    "chosen_ms=%d supported=%d rule=%s fallback=%s "
                    "feature_age_ms=%.3f policy_sha=%s bundle_sha=%s",
                    side,
                    boolean_decision.action_id,
                    int(round(effective_fc * max(1.0, consec) * 1_000.0)),
                    int(round(cd * 1_000.0)),
                    int(boolean_decision.support_valid),
                    (
                        boolean_decision.matched_rule_index
                        if boolean_decision.matched_rule_index is not None
                        else "none"
                    ),
                    boolean_decision.fallback_reason or "none",
                    float(boolean_decision.feature_age_ms),
                    boolean_decision.policy_sha256[:12],
                    boolean_decision.predicate_bundle_sha256[:12],
                )
            self._fill_cooldown_until[side] = now + cd
            logger.info(
                f"FILL_CD: {side} kind={cd_kind} qty={qty:.4f} consec={consec:.2f} "
                f"base={raw_fc:.1f}s effective_base={effective_fc:.1f}s "
                f"vol_mult={vol_mult:.2f} add_mult={add_mult:.2f} cooldown={cd:.0f}s "
                f"until={self._fill_cooldown_until[side]:.0f}")
            self._cancel_cooldown_side_order(side)

        # v1.2: Enqueue fill for delayed markout computation
        mo_span = int(getattr(self.cfg.strategy, "markout_ema_span_fills", 0) or 0)
        mo_ss = getattr(self.cfg.strategy, 'markout_spread_scale', 0.0)
        if mo_span > 0 and mo_ss > 0:
            fill_time = trade_time_ms / 1000.0 if trade_time_ms > 0 else time.time()
            self._mo_pending.append((fill_time, price, side))

        # Immediately cancel accumulating-side order if at max inventory
        # to prevent position growing beyond limit between requote cycles
        q = self.inventory.net_position
        max_inv = self.cfg.strategy.max_inventory
        if q >= max_inv and self._bid_cid:
            self._cancel_order(self._bid_cid)
            self._bid_cid = None
        elif q <= -max_inv and self._ask_cid:
            self._cancel_order(self._ask_cid)
            self._ask_cid = None

        if order.is_terminal:
            self._pop_order_context(order.client_order_id)

    def _on_cancel(self, order):
        """Called when an order is canceled."""
        self._log_order_outcome("canceled", order)
        self._pop_order_context(order.client_order_id)
        logger.debug(f"ORDER_CANCELED: {order.client_order_id}")

    # ── lifecycle ──

    def _sync_exchange_filters(self):
        """Fetch exchange filters and override config with actual values."""
        try:
            info = self.rest.exchange_info()
            for s in info.get("symbols", []):
                if s["symbol"] == self.cfg.symbol:
                    self._base_asset = str(s.get("baseAsset") or self._base_asset).upper()
                    self._quote_asset = str(s.get("quoteAsset") or self._quote_asset).upper()
                    self._settlement_asset = str(
                        s.get("marginAsset") or self._quote_asset or self._settlement_asset
                    ).upper()
                    for f in s.get("filters", []):
                        if f["filterType"] == "LOT_SIZE":
                            self.cfg.lot_size = float(f["stepSize"])
                            self._min_qty = float(f["minQty"])
                        elif f["filterType"] == "PRICE_FILTER":
                            self.cfg.tick_size = float(f["tickSize"])
                        elif f["filterType"] == "MIN_NOTIONAL":
                            self.cfg.min_notional = float(f["notional"])
                    logger.info(
                        f"Exchange filters: tick={self.cfg.tick_size} "
                        f"lot={self.cfg.lot_size} "
                        f"min_qty={self._min_qty} "
                        f"min_notional={self.cfg.min_notional} "
                        f"assets={self._base_asset}/{self._quote_asset} "
                        f"settlement={self._settlement_asset}"
                    )
                    self._qty_precision = self._precision_from_step(self.cfg.lot_size)
                    self._price_precision = self._precision_from_step(self.cfg.tick_size)
                    return
            logger.warning(f"Symbol {self.cfg.symbol} not found in exchange_info")
        except Exception as e:
            logger.warning(f"exchange_info failed, using config defaults: {e}")

    def _prefill_warmup(self):
        """Fetch a small recent aggTrades sample to seed warmup buffers."""
        try:
            end_ms = int(time.time() * 1000)
            start_ms = end_ms - 360_000
            trades = []
            next_start = start_ms
            for _ in range(8):
                batch = self.rest.agg_trades(
                    symbol=self.cfg.symbol,
                    startTime=next_start,
                    endTime=end_ms,
                    limit=1000,
                )
                if not batch:
                    break
                trades.extend(batch)
                last_ts = max(int(t.get("T", 0) or 0) for t in batch)
                if len(batch) < 1000 or last_ts >= end_ms - 1000:
                    break
                next_start = max(next_start + 1, last_ts + 1)

            deduped = {}
            for trade in trades:
                key = trade.get("a")
                if key is None:
                    key = (trade.get("T"), trade.get("p"), trade.get("q"), trade.get("m"))
                deduped[key] = trade
            trades = sorted(deduped.values(), key=lambda t: int(t.get("T", 0) or 0))

            if trades:
                self.signal.prefill_from_agg_trades(trades)
                first_ts = int(trades[0].get("T", 0) or 0)
                last_ts = int(trades[-1].get("T", 0) or 0)
                coverage_s = max(0.0, (last_ts - first_ts) / 1000.0)
                logger.info(
                    f"Warmup prefill: injected {len(trades)} recent aggTrades "
                    f"covering {coverage_s:.0f}s"
                )
            else:
                logger.warning("Warmup prefill returned no aggTrades")
        except Exception as e:
            logger.warning(f"Warmup prefill failed, falling back to live: {e}")

        # Ensure metrics are available before first requote (avoid zero-fill).
        # _poll_metrics runs async on a timer; do one blocking fetch now
        # (idempotent — won't start a second timer chain).
        try:
            self.signal._start_metrics_polling()
            if self.signal._last_metrics:
                logger.info("Warmup: initial metrics poll OK")
            else:
                logger.warning("Warmup: metrics poll returned no data")
        except Exception as e:
            logger.warning(f"Warmup: metrics poll failed: {e}")

    def start(self):
        self._running = True
        self._min_qty = self.cfg.lot_size  # default before exchange sync
        self._sync_exchange_filters()
        self._prefill_warmup()

        # Cancel any stale orders left from crashed/killed previous session
        try:
            self.rest.cancel_open_orders(symbol=self.cfg.symbol)
            logger.info("Startup: canceled all existing exchange orders")
        except Exception as e:
            # -2011 = "Unknown order" means no orders to cancel (OK)
            if "-2011" not in str(e):
                logger.warning(f"Startup cancel failed: {e}")

        # Set leverage
        try:
            self.rest.change_leverage(
                symbol=self.cfg.symbol,
                leverage=self.cfg.strategy.leverage,
            )
            logger.info(f"Leverage set to {self.cfg.strategy.leverage}x")
        except Exception as e:
            logger.warning(f"Set leverage failed: {e}")
        logger.info("MakerEngine started")

    def stop(self):
        """Graceful shutdown: stop quoting and cancel all orders.

        Shutdown is an operational stop, not a flatten command. Remaining
        inventory is left for explicit operator/risk handling.
        """
        self._running = False
        logger.info("MakerEngine stopping...")
        self.signal.stop()
        self._cancel_all_orders()

        self.orders.cancel_all_local()
        lifecycle_runtime = getattr(self, "_order_lifecycle_live_writer_v2", None)
        if lifecycle_runtime is not None:
            health = lifecycle_runtime.close(
                drain_timeout_s=self._order_lifecycle_live_writer_v2_shutdown_timeout_s
            )
            logger.info(
                "ORDER_LIFECYCLE_JOURNAL_V2_CLOSED rows=%d drops=%d errors=%d valid=%d",
                int(health.get("rows_committed", 0)),
                int(health.get("drop_count", 0)),
                int(health.get("error_count", 0)),
                int(bool(health.get("formal_collection_valid", False))),
            )
            self._order_lifecycle_live_writer_v2 = None
        runtime = getattr(self, "_exact_opportunity_tape_runtime", None)
        if runtime is not None:
            health = runtime.close()
            logger.info(
                "EXACT_OPPORTUNITY_TAPE_V2_2_CLOSED rows=%d drops=%d errors=%d",
                int(health.get("rows_written", 0)),
                int(health.get("rows_dropped", 0)),
                int(health.get("error_count", 0)),
            )
            self._exact_opportunity_tape_runtime = None
        logger.info("MakerEngine stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    # ── exchange sync ──

    def sync_position(self):
        """Sync local position with exchange (call periodically)."""
        try:
            sync_start = time.time()  # before REST call
            positions = self.rest.get_position_risk(symbol=self.cfg.symbol)
            for pos in positions:
                if pos.get("symbol") == self.cfg.symbol:
                    qty = float(pos.get("positionAmt", 0))
                    entry = float(pos.get("entryPrice", 0))
                    self.inventory.sync_from_exchange(qty, entry, sync_start)
                    break
        except Exception as e:
            logger.error(f"Position sync failed: {e}")
