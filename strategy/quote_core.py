"""
Shared quote-core calculation for live maker and tick replay.

The function in this module is intentionally pure: callers assemble
QuoteState + QuoteCoreConfig + QuotePrediction + QuoteDepthSnapshot, and the
core returns raw quote decomposition plus the constrained final quote context.

中文说明：这里是 live 与 replay 的共同报价数学层。不要在本文件里加入
REST/WS、订单生命周期、fill cooldown 或 live-only routing 逻辑；那些属于
MakerEngine / tick replay policy executor。
"""

from __future__ import annotations

import math
import os
import weakref
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

SPREAD_CAP_COMPRESS = 0
SPREAD_CAP_PAUSE_EXPOSURE = 1
SPREAD_CAP_OBSERVE = 2
QUOTE_CORE_UNIT_ABI_FIELDS = (
    "inventory_reference_qty",
    "eta_inventory",
    "a_spread",
    "risk_per_order",
    "execution_intensity_slope",
    "risk_horizon_s",
    "historical_p3_scalar_adapter_enabled",
    "p3_side_bbo_floor_enabled",
    "p3_identity_required",
    "p3_event_type",
    "p3_horizon_s",
    "p3_distance_origin",
    "p3_distance_unit",
    "p3_side",
    "p3_queue_included",
    "p3_artifact_sha256",
    "trade_intensity_acceleration_spread_mult",
    "f03_ret_action_horizon_s",
    "f03_ret_action_compatible",
)

P3_TOUCH_EVENT_TYPE = "touch"
P3_TOUCH_HORIZON_S = 10.0
P3_TOUCH_DISTANCE_UNIT = "USDC_per_BTC"
P3_TOUCH_DISTANCE_ORIGIN = "same_side_best_bid_or_ask_at_window_start"
P3_TOUCH_SIDE_IDENTITY = "pooled_buy_sell"


def validate_p3_touch_identity(
    identity: Mapping[str, Any],
    *,
    require_artifact_hash: bool = True,
) -> dict[str, Any]:
    """Validate the estimand before projecting it into the legacy quote ABI.

    P3 is a pooled, ten-second *touch* curve measured outward from the
    same-side BBO.  It is not a one-second fill curve and it contains no queue
    model.  Keeping these fields together prevents a naked slope/distance from
    acquiring a different meaning at the quote consumer.
    """
    normalized = {
        "event_type": str(identity.get("event_type") or "").strip().lower(),
        "horizon_s": float(identity.get("horizon_s", 0.0) or 0.0),
        "distance_origin": str(identity.get("distance_origin") or "").strip(),
        "distance_unit": str(identity.get("distance_unit") or "").strip(),
        "side": str(identity.get("side") or "").strip().lower(),
        "queue_included": identity.get("queue_included"),
        "artifact_sha256": str(identity.get("artifact_sha256") or "").strip().lower(),
    }
    expected = {
        "event_type": P3_TOUCH_EVENT_TYPE,
        "horizon_s": P3_TOUCH_HORIZON_S,
        "distance_origin": P3_TOUCH_DISTANCE_ORIGIN,
        "distance_unit": P3_TOUCH_DISTANCE_UNIT,
        "side": P3_TOUCH_SIDE_IDENTITY,
        "queue_included": False,
    }
    for name, value in expected.items():
        actual = normalized[name]
        matches = (
            actual is False
            if name == "queue_included"
            else math.isclose(actual, value, rel_tol=0.0, abs_tol=1e-12)
            if name == "horizon_s"
            else actual == value
        )
        if not matches:
            raise ValueError(
                f"P3 touch identity {name}={actual!r} does not match {value!r}"
            )
    artifact_sha256 = normalized["artifact_sha256"]
    if require_artifact_hash and (
        len(artifact_sha256) != 64
        or any(ch not in "0123456789abcdef" for ch in artifact_sha256)
    ):
        raise ValueError("P3 touch identity requires an exact artifact_sha256")
    return normalized


def finite_positive_quote_coefficient(name: str, value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite positive coefficient")
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite positive coefficient") from exc
    if not math.isfinite(resolved) or resolved <= 0.0:
        raise ValueError(f"{name} must be a finite positive coefficient")
    return resolved


def reservation_price(
    mid: float,
    q: float,
    gamma: float,
    sigma_sq: float,
    horizon_s: float = 1.0,
) -> float:
    """Inventory fair value for an explicit per-second variance integral.

    ``q`` is measured in base currency, ``sigma_sq`` in
    ``(quote/base)^2 / second``, and legacy ``gamma`` in ``1 / quote``.
    """
    return mid - q * gamma * sigma_sq * max(float(horizon_s), 0.0)


def weighted_mid_proxy_from_book(
    bids: Sequence[tuple[float, float]],
    asks: Sequence[tuple[float, float]],
    levels: int = 1,
) -> float:
    """Return the legacy top-N weighted-mid proxy used by quote control.

    Quantities are summed over the selected levels while prices remain the
    best bid and ask.  This is not Stoikov's state-transition/conditional-
    expectation micro-price estimator.
    """
    if not bids or not asks:
        return 0.0
    n = max(1, min(int(levels), len(bids), len(asks)))
    bid_qty = sum(qty for _, qty in bids[:n])
    ask_qty = sum(qty for _, qty in asks[:n])
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    total = bid_qty + ask_qty
    if total <= 0.0:
        return 0.5 * (best_bid + best_ask)
    imbalance = (bid_qty - ask_qty) / total
    mid = 0.5 * (best_bid + best_ask)
    return mid + imbalance * 0.5 * (best_ask - best_bid)


# Frozen public/runtime ABI.  New code should use the semantically accurate
# name above; old callers remain behavior-identical.
microprice_from_book = weighted_mid_proxy_from_book

def spread_cap_mode_code(value: Any) -> int:
    """Normalize the public string mode to the compact Python/C++ ABI value."""
    if isinstance(value, str):
        normalized = value.strip().lower().replace("-", "_")
        mapping = {
            "compress": SPREAD_CAP_COMPRESS,
            "pause_exposure": SPREAD_CAP_PAUSE_EXPOSURE,
            "observe": SPREAD_CAP_OBSERVE,
        }
        if normalized not in mapping:
            raise ValueError(
                f"Unknown spread_cap_mode={value!r}; expected compress, pause_exposure, or observe"
            )
        return mapping[normalized]
    code = int(value)
    if code not in {SPREAD_CAP_COMPRESS, SPREAD_CAP_PAUSE_EXPOSURE, SPREAD_CAP_OBSERVE}:
        raise ValueError(f"Unknown spread_cap_mode code: {code}")
    return code


def spread_cap_mode_name(value: Any) -> str:
    return {
        SPREAD_CAP_COMPRESS: "compress",
        SPREAD_CAP_PAUSE_EXPOSURE: "pause_exposure",
        SPREAD_CAP_OBSERVE: "observe",
    }[spread_cap_mode_code(value)]


@dataclass(frozen=True)
class QuotePrediction:
    dir_10s: float = 0.5
    vol_10s: float = 0.0
    ret_10s: float = 0.0
    tox_bid: float = 0.5
    tox_ask: float = 0.5


@dataclass(frozen=True)
class DepthSnapshot:
    bids: tuple[tuple[float, float], ...] = ()
    asks: tuple[tuple[float, float], ...] = ()

    @property
    def has_book(self) -> bool:
        return bool(self.bids and self.asks)


@dataclass(frozen=True)
class QuoteState:
    mid: float
    inventory: float
    sigma_sq: float
    trade_intensity: float = 0.0
    best_bid: float = 0.0
    best_ask: float = 0.0
    ber_active: bool = False
    mo_ema_all: float = 0.0
    mo_ema_bid: float = 0.0
    mo_ema_ask: float = 0.0
    bid_adverse_markout_pause_latch: bool = False
    ask_adverse_markout_pause_latch: bool = False
    mo_ref: float = 50.0
    position_open: bool = False
    hold_time_s: float = 0.0
    unrealized_pnl: float = 0.0

@dataclass(frozen=True)
class QuoteCoreConfig:
    # Compatibility input only.  When the two dimensioned coefficients below
    # are omitted, both inherit this historical numeric value exactly.
    gamma: float
    kappa: float
    tick_size: float
    lot_size: float
    maker_fee: float
    order_size: float
    max_inventory: float
    position_timeout_s: float = 0.0
    # sigma_sq is the variance of one-second absolute price changes in
    # (quote/base)^2 / second.  The AS inventory term must integrate it over
    # an explicit action horizon instead of silently assuming one sample.
    quote_horizon_s: float = 1.0
    pnl_volatility_horizon_s: float = 1.0

    ml_enabled: bool = True
    vol_blend: float = 0.0
    dir_threshold: float = 0.05
    gamma_dir_bonus: float = 0.0
    skew_strength: float = 0.0
    asym_strength: float = 0.0
    ret_skew: float = 0.0
    ret_shift_max_pct: float = 0.3

    regime_enabled: bool = False
    vol_baseline: float = 3.0
    gamma_scale_min: float = 0.5
    gamma_scale_max: float = 2.0
    liq_baseline: float = 200.0
    gamma_liq_scale_min: float = 0.5
    gamma_liq_scale_max: float = 3.0
    vol_power: float = 1.0

    kappa_ratio: float = 0.3
    p3_delta_star: float = 0.0
    p3_kappa_eff: float = 0.0

    use_bar_pricing: bool = True
    use_depth_microprice: bool = False
    use_depth_kappa: bool = False
    microprice_levels: int = 3
    kappa_levels: int = 5
    kappa_depth_baseline: float = 50.0
    depth_kappa_ratio: float = 0.3

    ber_spread_mult: float = 2.0
    markout_spread_scale: float = 0.0
    # -1 preserves the historical implementation; +1 favors the side with the
    # better maker-signed markout.  Keep explicit so the sign can be A/B tested.
    markout_side_asymmetry_sign: float = 1.0
    inventory_skew_strength: float = 0.0
    inventory_asym_strength: float = 0.0
    inventory_signal_fade_strength: float = 0.0

    book_imb_strength: float = 0.0
    book_imb_levels: int = 20
    trace_book_imb_levels: int = 10

    depth_tox_enabled: bool = False
    depth_tox_levels: int = 20
    depth_tox_imbalance_threshold: float = 0.65
    depth_tox_microprice_shift_bps: float = 1.0
    depth_tox_spread_mult: float = 1.25

    dynamic_cap_enabled: bool = False
    max_spread_bps: float = 0.0
    dynamic_cap_base_bps: float = 0.0
    dynamic_cap_alpha: float = 0.5
    dynamic_cap_max_mult: float = 2.0
    dynamic_cap_var_baseline: float = 0.0
    # Liquidity-aware cap term: cap_mult *= (liq_baseline / near_depth) ** liq_beta.
    # Thin near-book depth widens the cap; thick depth tightens it. Disabled when
    # liq_beta<=0 or liq_baseline<=0. min_mult<1.0 lets a thick/calm book tighten
    # below the base cap; default 1.0 preserves the original widen-only behavior.
    dynamic_cap_liq_beta: float = 0.0
    dynamic_cap_liq_baseline: float = 0.0
    dynamic_cap_min_mult: float = 1.0
    # 0=compress (explicit research arm), 1=pause exposure-increasing side,
    # 2=observe only. Runtime/replay adapters default to the fail-closed mode.
    spread_cap_mode: int = SPREAD_CAP_PAUSE_EXPOSURE

    exit_urgency_strength: float = 0.0
    urgency_time_weight: float = 0.3
    urgency_pnl_weight: float = 0.3
    urgency_signal_weight: float = 0.4

    adverse_guard_enabled: bool = False
    adverse_toxicity_threshold: float = 0.70
    adverse_markout_threshold: float = 5.0
    adverse_markout_pause_threshold: float = 0.0
    adverse_markout_pause_hybrid: bool = False
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

    # Appended after every legacy field to preserve positional construction.
    # ``inventory_reference_qty`` is q_ref in base-asset units and
    # ``order_size`` is z in the same units.  The reservation term consumes
    # n=q/q_ref (dimensionless).  eta_inventory and a_spread have inverse-price
    # units (base/quote) and are independent empirical controller coefficients;
    # the historical fallback below exists
    # only to preserve B0 numerically.  In particular z is not present in the
    # logarithmic spread term, so legacy gamma/a_spread must not be interpreted
    # as a portable CARA coefficient derived for arbitrary order quantity.
    # Changing q_ref with eta omitted rescales eta so the old q*gamma inventory
    # shift remains unchanged.
    inventory_reference_qty: float = 1.0
    eta_inventory: float | None = None
    a_spread: float | None = None

    # Complete P3 identity is validated here and copied into the native ABI,
    # where it is validated again.  A stale native extension missing any of
    # these fields is rejected before it can consume the scalar projection.
    p3_identity_required: bool = False
    p3_event_type: str = ""
    p3_horizon_s: float = 0.0
    p3_distance_origin: str = ""
    p3_distance_unit: str = ""
    p3_side: str = ""
    p3_queue_included: bool | None = None
    p3_artifact_sha256: str = ""

    # F03 ret labels historically span 10--20 seconds.  A nonzero ret_skew is
    # admitted only when an explicitly bound action-horizon head matches the
    # quote consumer horizon.  Zero leaves the frozen ret action disabled.
    f03_ret_action_horizon_s: float = 0.0
    f03_ret_action_compatible: bool = False

    # ``risk_per_order`` is the inverse-price (base/quote) coefficient consumed by the
    # spread expression.  In a quantity-aware AS derivation it is gamma*z;
    # legacy B0 instead inherits the empirical ``a_spread`` value exactly.
    # ``execution_intensity_slope`` is a distance-decay coefficient.  It must
    # not silently inherit the fixed-horizon P3 touch slope.
    risk_per_order: float | None = None
    execution_intensity_slope: float | None = None
    risk_horizon_s: float | None = None

    # P3 scalars may enter one explicitly named projection only.  The legacy
    # switch reproduces B0's pooled pair-floor/touch-slope adapter.  The side
    # switch applies the distance in its true same-side-BBO coordinates and is
    # a behavior-changing research candidate.  They are mutually exclusive.
    historical_p3_scalar_adapter_enabled: bool = False
    p3_side_bbo_floor_enabled: bool = False

    # Canonical name for the old BER multiplier.  The underlying state is a
    # trade-intensity acceleration proxy, not book-exhaustion BER.
    trade_intensity_acceleration_spread_mult: float | None = None

    def __post_init__(self) -> None:
        legacy = finite_positive_quote_coefficient("gamma", self.gamma)
        legacy_effective = max(legacy, 1e-12)
        inventory_reference_qty = finite_positive_quote_coefficient(
            "inventory_reference_qty", self.inventory_reference_qty
        )
        eta_inventory = (
            legacy_effective * inventory_reference_qty
            if self.eta_inventory is None
            else finite_positive_quote_coefficient("eta_inventory", self.eta_inventory)
        )
        a_spread = (
            legacy_effective
            if self.a_spread is None
            else finite_positive_quote_coefficient("a_spread", self.a_spread)
        )
        risk_per_order = (
            a_spread
            if self.risk_per_order is None
            else finite_positive_quote_coefficient(
                "risk_per_order", self.risk_per_order
            )
        )
        execution_intensity_slope = (
            finite_positive_quote_coefficient("kappa", self.kappa)
            if self.execution_intensity_slope is None
            else finite_positive_quote_coefficient(
                "execution_intensity_slope", self.execution_intensity_slope
            )
        )
        risk_horizon_s = (
            finite_positive_quote_coefficient(
                "quote_horizon_s", self.quote_horizon_s
            )
            if self.risk_horizon_s is None
            else finite_positive_quote_coefficient(
                "risk_horizon_s", self.risk_horizon_s
            )
        )
        acceleration_spread_mult = (
            finite_positive_quote_coefficient("ber_spread_mult", self.ber_spread_mult)
            if self.trade_intensity_acceleration_spread_mult is None
            else finite_positive_quote_coefficient(
                "trade_intensity_acceleration_spread_mult",
                self.trade_intensity_acceleration_spread_mult,
            )
        )
        object.__setattr__(self, "gamma", legacy)
        object.__setattr__(self, "inventory_reference_qty", inventory_reference_qty)
        object.__setattr__(self, "eta_inventory", eta_inventory)
        object.__setattr__(self, "a_spread", a_spread)
        object.__setattr__(self, "risk_per_order", risk_per_order)
        object.__setattr__(
            self, "execution_intensity_slope", execution_intensity_slope
        )
        object.__setattr__(self, "risk_horizon_s", risk_horizon_s)
        object.__setattr__(
            self,
            "trade_intensity_acceleration_spread_mult",
            acceleration_spread_mult,
        )
        if (
            self.historical_p3_scalar_adapter_enabled
            and self.p3_side_bbo_floor_enabled
        ):
            raise ValueError(
                "historical P3 pair projection and side-BBO floor are mutually exclusive"
            )
        has_p3_identity = any(
            (
                self.p3_event_type,
                self.p3_horizon_s,
                self.p3_distance_origin,
                self.p3_distance_unit,
                self.p3_side,
                self.p3_queue_included is not None,
                self.p3_artifact_sha256,
            )
        )
        p3_projection_active = (
            self.historical_p3_scalar_adapter_enabled
            and (self.p3_delta_star > 0.0 or self.p3_kappa_eff > 0.0)
        ) or (
            self.p3_side_bbo_floor_enabled and self.p3_delta_star > 0.0
        )
        if self.p3_identity_required or has_p3_identity:
            validate_p3_touch_identity(
                {
                    "event_type": self.p3_event_type,
                    "horizon_s": self.p3_horizon_s,
                    "distance_origin": self.p3_distance_origin,
                    "distance_unit": self.p3_distance_unit,
                    "side": self.p3_side,
                    "queue_included": self.p3_queue_included,
                    "artifact_sha256": self.p3_artifact_sha256,
                },
                require_artifact_hash=bool(
                    self.p3_identity_required or p3_projection_active
                ),
            )
        if p3_projection_active and not has_p3_identity:
            raise ValueError("an active P3 projection requires the complete touch identity")
        if self.ml_enabled and self.ret_skew > 0.0:
            producer_horizon_s = float(self.f03_ret_action_horizon_s)
            consumer_horizon_s = float(self.quote_horizon_s)
            if (
                not self.f03_ret_action_compatible
                or
                not math.isfinite(producer_horizon_s)
                or producer_horizon_s <= 0.0
                or not math.isclose(
                    producer_horizon_s,
                    consumer_horizon_s,
                    rel_tol=0.0,
                    abs_tol=1e-12,
                )
            ):
                raise ValueError(
                    "F03 ret action horizon is not compatible with the quote "
                    f"consumer: producer={producer_horizon_s!r}s "
                    f"consumer={consumer_horizon_s!r}s"
                )


@dataclass(frozen=True)
class QuoteCoreResult:
    bid_price: float
    ask_price: float
    spread: float
    raw_half_spread: float
    raw_mid_shift: float
    final_quote_delta: dict[str, float]
    quote_context: dict[str, dict[str, Any]]
    quote_flags: dict[str, bool]
    diagnostics: dict[str, Any] = field(default_factory=dict)


class DeferredNativeQuoteCoreResult:
    """Own one native quote result until a legacy mapping is requested.

    MakerEngine only needs a small subset of the native POD on each decision.
    Keeping that POD alive avoids eagerly recreating the much larger Python
    ``quote_context`` and ``diagnostics`` mappings.  ``materialize`` deliberately
    delegates to the existing compact converter so the public/evidence shape
    has one implementation and remains behavior-identical.
    """

    __slots__ = (
        "_buy_context",
        "_cfg",
        "_materialized",
        "_native_result",
        "_pred_values",
        "_sell_context",
        "_state",
    )

    def __init__(
        self,
        *,
        native_result: Any,
        state: QuoteState,
        cfg: QuoteCoreConfig,
        pred_values: tuple[float, float, float, float, float],
    ) -> None:
        self._native_result = native_result
        # Cache the pybind child views once.  The owning result remains alive
        # for their complete lifetime.
        self._buy_context = native_result.buy
        self._sell_context = native_result.sell
        self._state = state
        self._cfg = cfg
        self._pred_values = pred_values
        self._materialized: QuoteCoreResult | None = None

    @property
    def bid_price(self) -> float:
        return float(self._native_result.bid_price)

    @property
    def ask_price(self) -> float:
        return float(self._native_result.ask_price)

    @property
    def spread(self) -> float:
        return float(self._native_result.spread)

    @property
    def max_spread(self) -> float:
        return float(self._native_result.max_spread)

    @property
    def config(self) -> QuoteCoreConfig:
        return self._cfg

    @property
    def is_materialized(self) -> bool:
        return self._materialized is not None

    def side_context(self, side: str) -> Any:
        if side == "BUY":
            return self._buy_context
        if side == "SELL":
            return self._sell_context
        raise ValueError(f"unsupported quote side: {side!r}")

    def side_value(self, side: str, key: str, default: Any = None) -> Any:
        """Read a compact live-policy field without creating a mapping."""

        context = self.side_context(side)
        if key == "bid_adverse":
            return bool(context.side_adverse) if side == "BUY" else False
        if key == "ask_adverse":
            return bool(context.side_adverse) if side == "SELL" else False
        if key == "order_ttl_ms":
            return 0
        if key == "local_extreme_guard" or key == "local_extreme_pause":
            return False
        if key == "local_extreme_spread_mult":
            return 1.0
        if key == "side_adverse":
            return bool(context.side_adverse)
        if key == "side_adverse_pause":
            return bool(context.side_adverse_pause)
        if key == "defense_guard":
            return bool(context.defense_guard)
        if key == "defense_pause":
            return bool(context.defense_pause)
        if key == "defense_spread_mult":
            return float(context.defense_spread_mult)
        if key == "cap_exposure_block":
            return bool(context.cap_exposure_block)
        return self.materialize().quote_context.get(side, {}).get(key, default)

    def diagnostic_value(self, key: str, default: Any = None) -> Any:
        """Read diagnostics used on every decision directly from native POD."""

        if key in {"max_spread", "kappa_before_depth", "kappa_used", "asym"}:
            return getattr(self._native_result, key)
        if key == "p3_side_bbo_floor_enabled":
            return self._cfg.p3_side_bbo_floor_enabled
        if key == "p3_touch_delta_star":
            return self._cfg.p3_delta_star
        return self.materialize().diagnostics.get(key, default)

    def materialize(self) -> QuoteCoreResult:
        materialized = self._materialized
        if materialized is None:
            materialized = _compute_quote_core_cpp_compact(
                self._state,
                self._cfg,
                self._pred_values,
                None,
                _native_result=self._native_result,
                _native_pred_values=self._pred_values,
            )
            self._materialized = materialized
        return materialized


def ber_inventory_role_for_target(
    side: str,
    inventory: float,
    target_quantity: float,
    *,
    epsilon_btc: float = 1e-10,
) -> str:
    """Classify a quote without hiding partial-inventory cross-zero risk."""
    normalized = str(side).strip().upper()
    if normalized not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported BER quote side: {side!r}")
    quantity = float(target_quantity)
    if not math.isfinite(quantity) or quantity <= 0.0:
        raise ValueError(
            "BER target_quantity must be finite and positive"
        )
    q = float(inventory)
    epsilon = max(float(epsilon_btc), 0.0)
    if abs(q) <= epsilon:
        return "opener"
    signed_quantity = quantity if normalized == "BUY" else -quantity
    if q * signed_quantity > 0.0:
        return "add"
    q_after = q + signed_quantity
    if abs(q_after) <= epsilon or q * q_after > 0.0:
        return "reducing"
    return "mixed_cross_zero"


def compose_ber_exposure_add_only_quote(
    *,
    ber_quote: QuoteCoreResult,
    bypass_quote: QuoteCoreResult,
    inventory: float,
    target_buy_quantity: float,
    target_sell_quantity: float,
    epsilon_btc: float = 1e-10,
) -> QuoteCoreResult:
    """Keep BER widening only when that quote is a pure inventory add.

    Both source quotes must come from the same decision snapshot.  The helper
    does not invent a new spread formula: each side is copied byte-for-byte
    from either the legacy global-BER quote or the BER-bypass quote.
    """
    buy_role = ber_inventory_role_for_target(
        "BUY",
        inventory,
        target_buy_quantity,
        epsilon_btc=epsilon_btc,
    )
    sell_role = ber_inventory_role_for_target(
        "SELL",
        inventory,
        target_sell_quantity,
        epsilon_btc=epsilon_btc,
    )
    buy_source = ber_quote if buy_role in {"add", "mixed_cross_zero"} else bypass_quote
    sell_source = ber_quote if sell_role in {"add", "mixed_cross_zero"} else bypass_quote

    bid_price = float(buy_source.bid_price)
    ask_price = float(sell_source.ask_price)
    buy_context = dict(buy_source.quote_context.get("BUY", {}))
    sell_context = dict(sell_source.quote_context.get("SELL", {}))
    mid = _float(
        buy_context.get("mid", sell_context.get("mid", 0.0)),
        0.0,
    )
    fair = _float(
        buy_context.get("fair", sell_context.get("fair", mid)),
        mid,
    )
    best_bid = _float(buy_context.get("best_bid", 0.0), 0.0)
    best_ask = _float(sell_context.get("best_ask", 0.0), 0.0)
    spread = ask_price - bid_price
    if not math.isfinite(spread) or spread <= 0.0:
        raise ValueError(
            "role-safe BER composition produced a non-positive quote spread"
        )
    final_quote_skew = (
        ((ask_price - mid) - (mid - bid_price)) / spread
        if spread > 1e-12 else 0.0
    )
    final_mid_shift = 0.5 * (bid_price + ask_price) - fair
    final_bias_side = (
        "BUY" if final_mid_shift > 0.0 else ("SELL" if final_mid_shift < 0.0 else "balanced")
    )

    for side, role, context, price in (
        ("BUY", buy_role, buy_context, bid_price),
        ("SELL", sell_role, sell_context, ask_price),
    ):
        context["final_price"] = price
        context["final_pair_spread"] = spread
        context["final_quote_skew"] = final_quote_skew
        context["final_bias_side"] = final_bias_side
        context["final_distance_to_mid"] = (
            mid - price if side == "BUY" else price - mid
        )
        context["final_quote_delta_to_bbo"] = (
            best_bid - price if side == "BUY" and best_bid > 0.0
            else (
                price - best_ask
                if side == "SELL" and best_ask > 0.0
                else 0.0
            )
        )
        context["ber_application_scope"] = "inventory_add_side_only"
        context["ber_inventory_role"] = role
        context["ber_role_eligible"] = role == "add"
        context["ber_bypassed"] = role in {"opener", "reducing"}
        context["ber_mixed_cross_zero_fail_closed"] = role == "mixed_cross_zero"

    selected_flags = {
        "final_compressed": bool(
            buy_source.quote_flags.get("final_compressed", False)
            or sell_source.quote_flags.get("final_compressed", False)
        ),
        "delta_cap": bool(
            buy_source.quote_flags.get("delta_cap", False)
            or sell_source.quote_flags.get("delta_cap", False)
        ),
        "mid_guard": bool(
            buy_context.get("mid_guard", False)
            or sell_context.get("mid_guard", False)
        ),
        "post_only": bool(
            buy_context.get("post_only", False)
            or sell_context.get("post_only", False)
        ),
        "bid_adverse": bool(buy_context.get("side_adverse", False)),
        "ask_adverse": bool(sell_context.get("side_adverse", False)),
        "side_adverse": bool(
            buy_context.get("side_adverse", False)
            or sell_context.get("side_adverse", False)
        ),
        "defense_guard": bool(
            buy_context.get("defense_guard", False)
            or sell_context.get("defense_guard", False)
        ),
        "cap_exposure_block": bool(
            buy_context.get("cap_exposure_block", False)
            or sell_context.get("cap_exposure_block", False)
        ),
        "ber_role_safe": True,
    }
    diagnostics = dict(
        ber_quote.diagnostics
        if buy_source is ber_quote or sell_source is ber_quote
        else bypass_quote.diagnostics
    )
    diagnostics.update(
        {
            "ber_application_scope": "inventory_add_side_only",
            "ber_buy_inventory_role": buy_role,
            "ber_sell_inventory_role": sell_role,
            "ber_global_bid_price": float(ber_quote.bid_price),
            "ber_global_ask_price": float(ber_quote.ask_price),
            "ber_bypass_bid_price": float(bypass_quote.bid_price),
            "ber_bypass_ask_price": float(bypass_quote.ask_price),
            "pre_guard_bid": _float(buy_context.get("pre_guard_price", bid_price), bid_price),
            "pre_guard_ask": _float(sell_context.get("pre_guard_price", ask_price), ask_price),
            "delta_after_cap": spread,
            "capped_pair_spread": max(
                _float(sell_context.get("pre_guard_price", ask_price), ask_price)
                - _float(buy_context.get("pre_guard_price", bid_price), bid_price),
                0.0,
            ),
            "final_compressed": selected_flags["final_compressed"],
        }
    )
    raw_bid = _float(buy_context.get("raw_price", bid_price), bid_price)
    raw_ask = _float(sell_context.get("raw_price", ask_price), ask_price)
    return QuoteCoreResult(
        bid_price=bid_price,
        ask_price=ask_price,
        spread=spread,
        raw_half_spread=max(raw_ask - raw_bid, 0.0) * 0.5,
        raw_mid_shift=0.5 * (raw_bid + raw_ask) - fair,
        final_quote_delta={
            "BUY": _float(buy_context.get("final_quote_delta_to_bbo", 0.0), 0.0),
            "SELL": _float(sell_context.get("final_quote_delta_to_bbo", 0.0), 0.0),
        },
        quote_context={"BUY": buy_context, "SELL": sell_context},
        quote_flags=selected_flags,
        diagnostics=diagnostics,
    )


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def price_variance_pnl_sigma(
    sigma_sq_price_per_s: float,
    horizon_s: float,
    qty_base: float,
) -> float:
    """Convert absolute-price variance to quote-currency inventory sigma.

    Units: sqrt((quote/base)^2/s * s) * base = quote.  There is deliberately
    no mid-price multiplier; that belongs only to a return-variance input.
    """
    variance = max(float(sigma_sq_price_per_s), 0.0)
    horizon = max(float(horizon_s), 0.0)
    quantity = abs(float(qty_base))
    return math.sqrt(variance * horizon) * quantity


def circuit_breaker_loss_threshold(
    sigma_sq_price_per_s: float,
    horizon_s: float,
    qty_base: float,
    sigma_multiple: float,
) -> float:
    """Return the positive quote-currency loss threshold for the safety stop."""
    multiple = max(float(sigma_multiple), 0.0)
    return multiple * price_variance_pnl_sigma(
        sigma_sq_price_per_s,
        horizon_s,
        qty_base,
    )


def circuit_breaker_triggered(
    unrealized_pnl: float,
    sigma_sq_price_per_s: float,
    horizon_s: float,
    qty_base: float,
    sigma_multiple: float,
) -> bool:
    """Shared live/replay predicate under the absolute-price variance ABI."""
    threshold = circuit_breaker_loss_threshold(
        sigma_sq_price_per_s,
        horizon_s,
        qty_base,
        sigma_multiple,
    )
    return threshold > 0.0 and float(unrealized_pnl) < -threshold


def _get(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _floor_tick(price: float, tick: float) -> float:
    units = price / tick
    nearest = round(units)
    if abs(units - nearest) <= 1e-9:
        units = float(nearest)
    return math.floor(units) * tick


def _ceil_tick(price: float, tick: float) -> float:
    units = price / tick
    nearest = round(units)
    if abs(units - nearest) <= 1e-9:
        units = float(nearest)
    return math.ceil(units) * tick


def apply_p3_side_bbo_floor(
    bid_price: float,
    ask_price: float,
    *,
    enabled: bool,
    delta_star: float,
    best_bid: float,
    best_ask: float,
    tick_size: float,
) -> tuple[float, float, float, float, bool, bool]:
    """Apply the P3 same-side-BBO distance contract to final quote prices.

    The operation only moves quotes outward.  Calling it again after downstream
    policy transforms is therefore idempotent and cannot make a quote more
    aggressive.  Invalid active inputs fail closed instead of silently turning
    the floor into a naked scalar projection.
    """

    bid = float(bid_price)
    ask = float(ask_price)
    if not enabled or float(delta_star) <= 0.0:
        return bid, ask, 0.0, 0.0, False, False
    delta = float(delta_star)
    bbo_bid = float(best_bid)
    bbo_ask = float(best_ask)
    tick = float(tick_size)
    if not all(math.isfinite(value) and value > 0.0 for value in (delta, bbo_bid, bbo_ask, tick)):
        raise ValueError("P3 side-BBO floor requires finite positive BBO, delta, and tick")
    if not math.isfinite(bid) or not math.isfinite(ask):
        raise ValueError("P3 side-BBO floor requires finite quote prices")
    buy_floor = _floor_tick(bbo_bid - delta, tick)
    sell_floor = _ceil_tick(bbo_ask + delta, tick)
    final_bid = min(bid, buy_floor)
    final_ask = max(ask, sell_floor)
    tolerance = max(tick * 1e-9, 1e-12)
    return (
        final_bid,
        final_ask,
        buy_floor,
        sell_floor,
        final_bid < bid - tolerance,
        final_ask > ask + tolerance,
    )


def _normalize_levels(
    rows: Sequence[Sequence[float]] | None,
) -> tuple[tuple[float, float], ...]:
    if not rows:
        return ()
    out: list[tuple[float, float]] = []
    for price, qty in rows:
        p = _float(price)
        q = _float(qty)
        if p > 0.0 and q > 0.0:
            out.append((p, q))
    return tuple(out)


def quote_depth_from_book(depth: Any) -> DepthSnapshot:
    if depth is None:
        return DepthSnapshot()
    return DepthSnapshot(
        bids=_normalize_levels(getattr(depth, "bids", None)),
        asks=_normalize_levels(getattr(depth, "asks", None)),
    )


def quote_depth_from_l2_rows(
    bid_px_row: Sequence[float] | None,
    bid_qty_row: Sequence[float] | None,
    ask_px_row: Sequence[float] | None,
    ask_qty_row: Sequence[float] | None,
) -> DepthSnapshot:
    if bid_px_row is None or bid_qty_row is None or ask_px_row is None or ask_qty_row is None:
        return DepthSnapshot()
    bids: list[tuple[float, float]] = []
    asks: list[tuple[float, float]] = []
    seen_bids: set[float] = set()
    seen_asks: set[float] = set()
    for px, qty in zip(bid_px_row, bid_qty_row, strict=True):
        p = _float(px)
        q = _float(qty)
        # Historical L2 files can contain repeated price columns when the exact
        # book source degrades to touch/top-level snapshots. Live orderbooks
        # expose each price level once, so replay must keep the first visible
        # level and ignore later duplicates instead of multiplying depth.
        if p > 0.0 and q > 0.0 and p not in seen_bids:
            bids.append((p, q))
            seen_bids.add(p)
    for px, qty in zip(ask_px_row, ask_qty_row, strict=True):
        p = _float(px)
        q = _float(qty)
        if p > 0.0 and q > 0.0 and p not in seen_asks:
            asks.append((p, q))
            seen_asks.add(p)
    return DepthSnapshot(bids=tuple(bids), asks=tuple(asks))


def _weighted_mid_proxy(
    depth: DepthSnapshot, levels: int, fallback_mid: float
) -> float:
    if not depth.has_book:
        return fallback_mid
    n = max(1, min(int(levels), len(depth.bids), len(depth.asks)))
    bid_qty = sum(q for _, q in depth.bids[:n])
    ask_qty = sum(q for _, q in depth.asks[:n])
    total = bid_qty + ask_qty
    best_bid = depth.bids[0][0]
    best_ask = depth.asks[0][0]
    if total <= 0.0 or best_ask <= best_bid:
        return 0.5 * (best_bid + best_ask)
    mid = 0.5 * (best_bid + best_ask)
    half = 0.5 * (best_ask - best_bid)
    imb = (bid_qty - ask_qty) / total
    return mid + imb * half


def _depth_imbalance(depth: DepthSnapshot, levels: int) -> tuple[float, float, float]:
    if not depth.has_book:
        return 0.0, 0.0, 0.0
    n = max(1, min(int(levels), len(depth.bids), len(depth.asks)))
    bid_qty = sum(q for _, q in depth.bids[:n])
    ask_qty = sum(q for _, q in depth.asks[:n])
    total = bid_qty + ask_qty
    if total <= 1e-12:
        return 0.0, bid_qty, ask_qty
    return (bid_qty - ask_qty) / total, bid_qty, ask_qty


def _estimate_depth_kappa(
    depth: DepthSnapshot,
    kappa_base: float,
    depth_baseline: float,
    levels: int,
    min_ratio: float,
) -> float:
    if not depth.has_book or depth_baseline <= 0.0:
        return kappa_base
    n = max(1, min(int(levels), len(depth.bids), len(depth.asks)))
    bid_depth = sum(q for _, q in depth.bids[:n])
    ask_depth = sum(q for _, q in depth.asks[:n])
    avg_depth = 0.5 * (bid_depth + ask_depth)
    if avg_depth <= 0.0:
        return kappa_base
    ratio_floor = max(0.05, min(3.0, float(min_ratio)))
    ratio = max(ratio_floor, min(3.0, avg_depth / depth_baseline))
    return kappa_base * ratio


def _depth_tox_mult(mid: float, depth: DepthSnapshot, cfg: QuoteCoreConfig) -> float:
    if not cfg.depth_tox_enabled or not depth.has_book:
        return 1.0
    imb, _, _ = _depth_imbalance(depth, cfg.depth_tox_levels)
    micro_shift_bps = 0.0
    if mid > 0.0:
        fair = _weighted_mid_proxy(depth, 3, mid)
        micro_shift_bps = (fair - mid) / mid * 10000.0
    if (
        abs(imb) >= abs(cfg.depth_tox_imbalance_threshold)
        or abs(micro_shift_bps) >= abs(cfg.depth_tox_microprice_shift_bps)
    ):
        return max(1.0, cfg.depth_tox_spread_mult)
    return 1.0


def apply_final_spread_cap(
    mid: float,
    bid_price: float,
    ask_price: float,
    max_spread: float,
    tick_size: float,
) -> tuple[float, float, bool, float]:
    """Compress an already-built quote pair so the final spread respects cap.

    中文说明：这是“价格对”压缩函数，不判断是否允许报单。调用方仍需
    再做 post-only、inventory、policy pause 等门控。
    """
    tick = max(float(tick_size), 1e-12)
    if mid <= 0.0 or max_spread <= 0.0 or ask_price <= bid_price:
        return bid_price, ask_price, False, 0.0
    spread = ask_price - bid_price
    if spread <= max_spread + 1e-12:
        return bid_price, ask_price, False, 0.0

    excess = spread - max_spread
    bid_dist = max(mid - bid_price, tick)
    ask_dist = max(ask_price - mid, tick)
    dist_sum = bid_dist + ask_dist
    if max_spread > 2.0 * tick and dist_sum > 1e-9:
        usable = max_spread - 2.0 * tick
        bid_dist = tick + usable * bid_dist / dist_sum
        ask_dist = tick + usable * ask_dist / dist_sum
        bid_price = _ceil_tick(mid - bid_dist, tick)
        if bid_price >= mid:
            bid_price = _floor_tick(mid, tick)
            if bid_price >= mid:
                bid_price -= tick
        ask_price = _floor_tick(mid + ask_dist, tick)
        if ask_price <= mid:
            ask_price = _ceil_tick(mid, tick)
            if ask_price <= mid:
                ask_price += tick
    else:
        bid_price = _floor_tick(mid - tick, tick)
        ask_price = _ceil_tick(mid + tick, tick)

    return bid_price, ask_price, True, excess


def apply_final_spread_cap_preserve_side(
    mid: float,
    bid_price: float,
    ask_price: float,
    max_spread: float,
    tick_size: float,
    *,
    preserve_side: str,
) -> tuple[float, float, bool, float, bool]:
    """Cap a role-safe pair without moving its opener/reducing anchor side."""
    normalized = str(preserve_side).strip().upper()
    if normalized not in {"BUY", "SELL"}:
        raise ValueError("preserve_side must be BUY or SELL")
    tick = max(float(tick_size), 1e-12)
    spread = float(ask_price) - float(bid_price)
    if mid <= 0.0 or max_spread <= 0.0 or spread <= max_spread + 1e-12:
        return bid_price, ask_price, False, 0.0, True
    if ask_price <= bid_price:
        return bid_price, ask_price, True, 0.0, False
    excess = spread - max_spread
    if normalized == "SELL":
        # SELL is the reducing anchor; move only the BUY add inward.
        candidate_bid = _ceil_tick(ask_price - max_spread, tick)
        if candidate_bid >= mid or candidate_bid >= ask_price:
            return bid_price, ask_price, True, excess, False
        return candidate_bid, ask_price, True, excess, True
    # BUY is the reducing anchor; move only the SELL add inward.
    candidate_ask = _floor_tick(bid_price + max_spread, tick)
    if candidate_ask <= mid or candidate_ask <= bid_price:
        return bid_price, ask_price, True, excess, False
    return bid_price, candidate_ask, True, excess, True


def _near_depth_total(depth: DepthSnapshot, levels: int) -> float:
    if not depth.has_book:
        return 0.0
    n = max(1, min(int(levels), len(depth.bids), len(depth.asks)))
    return sum(q for _, q in depth.bids[:n]) + sum(q for _, q in depth.asks[:n])


def _exposure_increasing(
    side: str,
    inventory: float,
    quantity: float,
    lot_size: float,
) -> bool:
    """Classify the full posted quantity, failing closed on invalid or flip risk.

    A partial or exact close is reducing.  A quote that crosses through flat has
    both a reducing and an opening component, so the whole quote is treated as
    exposure-increasing until execution is split into explicit reduce/open legs.
    """

    try:
        inventory = float(inventory)
        quantity = float(quantity)
        lot = abs(float(lot_size))
    except (TypeError, ValueError):
        return True
    if (
        not math.isfinite(inventory)
        or not math.isfinite(quantity)
        or not math.isfinite(lot)
        or quantity <= 0.0
        or lot <= 0.0
    ):
        return True
    tolerance = max(lot * 1e-9, 1e-12)
    normalized_side = str(side).strip().upper()
    if normalized_side == "BUY":
        if inventory >= -tolerance:
            return True
        return inventory + quantity > tolerance
    if normalized_side == "SELL":
        if inventory <= tolerance:
            return True
        return inventory - quantity < -tolerance
    return True


def _side_adverse_state(
    side: str,
    *,
    inventory: float,
    quantity: float,
    lot_size: float,
    dir_signal: float,
    pred_ret: float,
    toxicity: float,
    markout_ema: float,
    markout_pause_latch: bool,
    microprice_shift_bps: float,
    near_depth: float,
    cfg: QuoteCoreConfig,
) -> dict[str, Any]:
    """Evaluate adverse state for the side that would increase exposure.

    中文说明：adverse guard 只针对加仓方向。已有 long 时 SELL 是减库存，
    已有 short 时 BUY 是减库存，这些方向不能被这里的 adverse pause 锁死。
    hybrid pause 的 wall-clock TTL/latch 由 MakerEngine 维护，这里只消费 latch。
    """
    exposure_inc = _exposure_increasing(side, inventory, quantity, lot_size)
    if not cfg.adverse_guard_enabled:
        return {
            "active": False,
            "pause": False,
            "exposure_increasing": exposure_inc,
            "toxicity": False,
            "markout": False,
            "direction": False,
            "ret": False,
            "microprice": False,
            "thin_depth": False,
            "markout_pause_raw": False,
            "markout_pause_latch": False,
            "spread_mult": 1.0,
        }

    sign = 1.0 if side == "SELL" else -1.0
    tox_active = toxicity >= cfg.adverse_toxicity_threshold
    markout_active = markout_ema < -abs(cfg.adverse_markout_threshold)
    markout_pause_threshold = abs(cfg.adverse_markout_pause_threshold)
    if markout_pause_threshold <= 0.0:
        markout_pause_threshold = abs(cfg.adverse_markout_threshold)
    markout_pause_raw = markout_ema < -markout_pause_threshold
    markout_pause_active = bool(markout_pause_latch) if cfg.adverse_markout_pause_hybrid else markout_pause_raw

    dir_active = False
    if cfg.adverse_dir_threshold > 0.0:
        dir_active = sign * dir_signal >= abs(cfg.adverse_dir_threshold)

    ret_active = False
    if cfg.adverse_ret_bps_threshold > 0.0:
        ret_active = sign * pred_ret * 10000.0 >= abs(cfg.adverse_ret_bps_threshold)

    micro_active = False
    if cfg.adverse_microprice_shift_bps > 0.0:
        micro_active = sign * microprice_shift_bps >= abs(cfg.adverse_microprice_shift_bps)

    thin_active = (
        cfg.adverse_thin_depth_threshold > 0.0
        and 0.0 < near_depth < cfg.adverse_thin_depth_threshold
    )
    active = exposure_inc and (
        tox_active or markout_active or dir_active or ret_active or micro_active
    )
    spread_mult = max(1.0, cfg.adverse_spread_mult)
    if active and thin_active:
        spread_mult *= max(1.0, cfg.adverse_thin_depth_mult)
    pause_active = active and cfg.adverse_pause and (
        tox_active or markout_pause_active or dir_active or ret_active or micro_active
    )
    return {
        "active": active,
        "pause": pause_active,
        "exposure_increasing": exposure_inc,
        "toxicity": tox_active,
        "markout": markout_active,
        "direction": dir_active,
        "ret": ret_active,
        "microprice": micro_active,
        "thin_depth": thin_active,
        "markout_pause_raw": markout_pause_raw,
        "markout_pause_latch": bool(markout_pause_latch),
        "spread_mult": spread_mult,
    }


def _side_defense_state(
    side: str,
    *,
    inventory: float,
    max_inventory: float,
    dir_signal: float,
    pred_ret: float,
    markout_ema: float,
    microprice_shift_bps: float,
    unrealized_pnl: float,
    cfg: QuoteCoreConfig,
) -> dict[str, Any]:
    """Evaluate reducing-side defense state.

    中文说明：defense 是“减库存但环境不利”的保护层，不是 alpha gate。
    emergency inventory/loss 时不触发 defense pause，避免极端风险下阻止减仓。
    """
    reducing = (side == "BUY" and inventory < 0.0) or (side == "SELL" and inventory > 0.0)
    inv_ratio = abs(inventory) / max(max_inventory, 1e-12)
    emergency_inventory = (
        cfg.defense_emergency_inventory_ratio > 0.0
        and inv_ratio >= cfg.defense_emergency_inventory_ratio
    )
    emergency_loss = (
        cfg.defense_emergency_loss > 0.0
        and unrealized_pnl <= -abs(cfg.defense_emergency_loss)
    )
    emergency = emergency_inventory or emergency_loss

    if not cfg.defense_guard_enabled:
        return {
            "active": False,
            "pause": False,
            "reducing": reducing,
            "emergency": emergency,
            "markout": False,
            "direction": False,
            "ret": False,
            "microprice": False,
            "spread_mult": 1.0,
        }

    sign = 1.0 if side == "BUY" else -1.0
    markout_active = markout_ema < -abs(cfg.defense_markout_threshold)

    dir_active = False
    if cfg.defense_dir_threshold > 0.0:
        dir_active = sign * dir_signal >= abs(cfg.defense_dir_threshold)

    ret_active = False
    if cfg.defense_ret_bps_threshold > 0.0:
        ret_active = sign * pred_ret * 10000.0 >= abs(cfg.defense_ret_bps_threshold)

    micro_active = False
    if cfg.defense_microprice_shift_bps > 0.0:
        micro_active = sign * microprice_shift_bps >= abs(cfg.defense_microprice_shift_bps)

    needs_extreme = (
        cfg.defense_dir_threshold > 0.0
        or cfg.defense_ret_bps_threshold > 0.0
        or cfg.defense_microprice_shift_bps > 0.0
    )
    extreme_active = (dir_active or ret_active or micro_active) if needs_extreme else True
    active = reducing and not emergency and markout_active and extreme_active
    return {
        "active": active,
        "pause": active and cfg.defense_pause,
        "reducing": reducing,
        "emergency": emergency,
        "markout": markout_active,
        "direction": dir_active,
        "ret": ret_active,
        "microprice": micro_active,
        "spread_mult": max(1.0, cfg.defense_spread_mult),
    }


def quote_core_config_from_live_config(
    cfg: Any,
    *,
    p3_delta_star: float = 0.0,
    p3_kappa_eff: float = 0.0,
    p3_identity: Mapping[str, Any] | None = None,
    f03_ret_action_horizon_s: float = 0.0,
    f03_ret_action_compatible: bool = False,
) -> QuoteCoreConfig:
    strategy = cfg.strategy
    ml = cfg.ml
    regime = getattr(cfg, "regime", None)
    fees = cfg.fees
    risk = cfg.risk
    depth_exec = getattr(cfg, "depth_execution", None)
    mk_cfg = getattr(depth_exec, "microprice_kappa", None) if depth_exec else None
    mk_enabled = bool(mk_cfg and getattr(mk_cfg, "enabled", False))
    use_depth_pricing = (not bool(getattr(strategy, "use_bar_pricing", False))) or mk_enabled

    imb_cfg = getattr(depth_exec, "imbalance_asym", None) if depth_exec else None
    if imb_cfg and getattr(imb_cfg, "enabled", False):
        book_imb_strength = float(getattr(imb_cfg, "strength", 0.0))
        book_imb_levels = int(getattr(imb_cfg, "levels", 20))
    else:
        book_imb_strength = float(getattr(strategy, "book_imb_strength", 0.0))
        book_imb_levels = 20

    tox_cfg = getattr(depth_exec, "depth_tox_spread", None) if depth_exec else None
    dyn_base = float(getattr(strategy, "dynamic_cap_base_bps", getattr(strategy, "max_spread_bps", 0.0)))
    if dyn_base <= 0.0 and float(getattr(strategy, "max_spread_bps", 0.0)) > 0.0:
        dyn_base = float(getattr(strategy, "max_spread_bps", 0.0))
    dyn_var_base = float(getattr(strategy, "dynamic_cap_var_baseline", 0.0))
    if dyn_var_base <= 0.0 and regime is not None:
        vol_base = float(getattr(regime, "vol_baseline", 0.0))
        if vol_base > 0.0:
            dyn_var_base = vol_base * vol_base

    normalized_p3_identity = (
        validate_p3_touch_identity(p3_identity, require_artifact_hash=True)
        if p3_identity is not None
        else {}
    )
    order_size = finite_positive_quote_coefficient(
        "strategy.order_size", strategy.order_size
    )
    inventory_reference_qty = finite_positive_quote_coefficient(
        "strategy.inventory_reference_qty",
        getattr(strategy, "inventory_reference_qty", 1.0),
    )
    eta_inventory = getattr(strategy, "eta_inventory", None)
    a_spread = getattr(strategy, "a_spread", None)
    risk_per_order = getattr(strategy, "risk_per_order", None)
    execution_intensity_slope = getattr(
        strategy, "execution_intensity_slope", None
    )
    risk_horizon_s = getattr(strategy, "risk_horizon_s", None)
    historical_p3_scalar_adapter_enabled = bool(
        getattr(strategy, "historical_p3_scalar_adapter_enabled", True)
    )
    p3_side_bbo_floor_enabled = bool(
        getattr(strategy, "p3_side_bbo_floor_enabled", False)
    )
    acceleration_spread_mult = getattr(
        strategy, "trade_intensity_acceleration_spread_mult", None
    )
    return QuoteCoreConfig(
        gamma=float(strategy.gamma),
        kappa=float(strategy.kappa),
        tick_size=float(cfg.tick_size),
        lot_size=float(cfg.lot_size),
        maker_fee=float(fees.maker),
        order_size=order_size,
        max_inventory=float(strategy.max_inventory),
        position_timeout_s=float(getattr(strategy, "position_timeout", 0.0)),
        quote_horizon_s=max(1e-6, float(getattr(strategy, "quote_horizon_s", 1.0))),
        pnl_volatility_horizon_s=max(
            1e-6, float(getattr(risk, "pnl_volatility_horizon_s", 1.0))
        ),
        ml_enabled=bool(getattr(ml, "enabled", False)),
        vol_blend=float(getattr(ml, "vol_blend", 0.0)),
        dir_threshold=float(getattr(ml, "dir_threshold", 0.05)),
        gamma_dir_bonus=float(getattr(ml, "gamma_dir_bonus", 0.0)),
        skew_strength=float(getattr(ml, "skew_strength", 0.0)),
        asym_strength=float(getattr(ml, "asym_strength", 0.0)),
        ret_skew=float(getattr(ml, "ret_skew", 0.0)),
        ret_shift_max_pct=float(getattr(ml, "ret_shift_max_pct", 0.3)),
        regime_enabled=bool(regime and getattr(regime, "enabled", False)),
        vol_baseline=float(getattr(regime, "vol_baseline", 3.0)) if regime else 3.0,
        gamma_scale_min=float(getattr(regime, "gamma_scale_min", 0.5)) if regime else 0.5,
        gamma_scale_max=float(getattr(regime, "gamma_scale_max", 2.0)) if regime else 2.0,
        liq_baseline=float(getattr(regime, "liq_baseline", 200.0)) if regime else 200.0,
        gamma_liq_scale_min=float(getattr(regime, "gamma_liq_scale_min", 0.5)) if regime else 0.5,
        gamma_liq_scale_max=float(getattr(regime, "gamma_liq_scale_max", 3.0)) if regime else 3.0,
        vol_power=float(getattr(strategy, "vol_power", 1.0)),
        kappa_ratio=float(getattr(strategy, "kappa_ratio", 0.3)),
        p3_delta_star=float(p3_delta_star),
        p3_kappa_eff=float(p3_kappa_eff),
        p3_identity_required=p3_identity is not None,
        p3_event_type=str(normalized_p3_identity.get("event_type", "")),
        p3_horizon_s=float(normalized_p3_identity.get("horizon_s", 0.0)),
        p3_distance_origin=str(normalized_p3_identity.get("distance_origin", "")),
        p3_distance_unit=str(normalized_p3_identity.get("distance_unit", "")),
        p3_side=str(normalized_p3_identity.get("side", "")),
        p3_queue_included=normalized_p3_identity.get("queue_included"),
        p3_artifact_sha256=str(normalized_p3_identity.get("artifact_sha256", "")),
        f03_ret_action_horizon_s=float(f03_ret_action_horizon_s),
        f03_ret_action_compatible=bool(f03_ret_action_compatible),
        use_bar_pricing=bool(getattr(strategy, "use_bar_pricing", True)),
        use_depth_microprice=use_depth_pricing,
        use_depth_kappa=use_depth_pricing,
        microprice_levels=int(getattr(mk_cfg, "microprice_levels", 3)) if mk_enabled else 3,
        kappa_levels=int(getattr(mk_cfg, "kappa_levels", 5)) if mk_enabled else 5,
        kappa_depth_baseline=(
            float(getattr(mk_cfg, "kappa_depth_baseline", 50.0))
            if mk_enabled else float(getattr(strategy, "kappa_depth_baseline", 50.0))
        ),
        depth_kappa_ratio=max(0.05, min(3.0, float(getattr(strategy, "depth_kappa_ratio", 0.3)))),
        ber_spread_mult=float(getattr(strategy, "ber_spread_mult", 2.0)),
        markout_spread_scale=float(getattr(strategy, "markout_spread_scale", 0.0)),
        markout_side_asymmetry_sign=float(
            getattr(strategy, "markout_side_asymmetry_sign", 1.0)
        ),
        inventory_skew_strength=float(getattr(strategy, "inventory_skew_strength", 0.0)),
        inventory_asym_strength=float(getattr(strategy, "inventory_asym_strength", 0.0)),
        inventory_signal_fade_strength=float(getattr(strategy, "inventory_signal_fade_strength", 0.0)),
        book_imb_strength=book_imb_strength,
        book_imb_levels=book_imb_levels,
        trace_book_imb_levels=book_imb_levels,
        depth_tox_enabled=bool(tox_cfg and getattr(tox_cfg, "enabled", False)),
        depth_tox_levels=int(getattr(tox_cfg, "levels", 20)) if tox_cfg else 20,
        depth_tox_imbalance_threshold=float(getattr(tox_cfg, "imbalance_threshold", 0.65)) if tox_cfg else 0.65,
        depth_tox_microprice_shift_bps=float(getattr(tox_cfg, "microprice_shift_bps", 1.0)) if tox_cfg else 1.0,
        depth_tox_spread_mult=float(getattr(tox_cfg, "spread_mult", 1.25)) if tox_cfg else 1.25,
        dynamic_cap_enabled=bool(getattr(strategy, "dynamic_cap_enabled", False)),
        max_spread_bps=float(getattr(strategy, "max_spread_bps", 0.0)),
        dynamic_cap_base_bps=dyn_base,
        dynamic_cap_alpha=float(getattr(strategy, "dynamic_cap_alpha", 0.5)),
        dynamic_cap_max_mult=max(1.0, float(getattr(strategy, "dynamic_cap_max_mult", 2.0))),
        dynamic_cap_var_baseline=dyn_var_base,
        dynamic_cap_liq_beta=max(0.0, float(getattr(strategy, "dynamic_cap_liq_beta", 0.0))),
        dynamic_cap_liq_baseline=max(0.0, float(getattr(strategy, "dynamic_cap_liq_baseline", 0.0))),
        dynamic_cap_min_mult=max(0.0, min(1.0, float(getattr(strategy, "dynamic_cap_min_mult", 1.0)))),
        spread_cap_mode=spread_cap_mode_code(
            getattr(strategy, "spread_cap_mode", "pause_exposure")
        ),
        exit_urgency_strength=float(getattr(risk, "exit_urgency_strength", 0.0)),
        urgency_time_weight=float(getattr(risk, "urgency_time_weight", 0.3)),
        urgency_pnl_weight=float(getattr(risk, "urgency_pnl_weight", 0.3)),
        urgency_signal_weight=float(getattr(risk, "urgency_signal_weight", 0.4)),
        adverse_guard_enabled=bool(getattr(strategy, "adverse_guard_enabled", False)),
        adverse_toxicity_threshold=float(getattr(strategy, "adverse_toxicity_threshold", 0.70)),
        adverse_markout_threshold=abs(float(getattr(strategy, "adverse_markout_threshold", 5.0))),
        adverse_markout_pause_threshold=abs(float(getattr(strategy, "adverse_markout_pause_threshold", 0.0))),
        adverse_markout_pause_hybrid=bool(getattr(strategy, "adverse_markout_pause_hybrid", False)),
        adverse_dir_threshold=abs(float(getattr(strategy, "adverse_dir_threshold", 0.0))),
        adverse_ret_bps_threshold=abs(float(getattr(strategy, "adverse_ret_bps_threshold", 0.0))),
        adverse_microprice_shift_bps=abs(float(getattr(strategy, "adverse_microprice_shift_bps", 0.0))),
        adverse_spread_mult=max(1.0, float(getattr(strategy, "adverse_spread_mult", 1.10))),
        adverse_thin_depth_threshold=max(0.0, float(getattr(strategy, "adverse_thin_depth_threshold", 0.0))),
        adverse_thin_depth_mult=max(1.0, float(getattr(strategy, "adverse_thin_depth_mult", 1.0))),
        adverse_pause=bool(getattr(strategy, "adverse_pause", True)),
        defense_guard_enabled=bool(getattr(strategy, "defense_guard_enabled", False)),
        defense_markout_threshold=abs(float(getattr(strategy, "defense_markout_threshold", 2.0))),
        defense_dir_threshold=abs(float(getattr(strategy, "defense_dir_threshold", 0.05))),
        defense_ret_bps_threshold=abs(float(getattr(strategy, "defense_ret_bps_threshold", 0.0))),
        defense_microprice_shift_bps=abs(float(getattr(strategy, "defense_microprice_shift_bps", 0.0))),
        defense_spread_mult=max(1.0, float(getattr(strategy, "defense_spread_mult", 1.35))),
        defense_pause=bool(getattr(strategy, "defense_pause", True)),
        defense_emergency_inventory_ratio=max(0.0, float(getattr(strategy, "defense_emergency_inventory_ratio", 0.50))),
        defense_emergency_loss=max(0.0, float(getattr(strategy, "defense_emergency_loss", 5.0))),
        inventory_reference_qty=inventory_reference_qty,
        eta_inventory=eta_inventory,
        a_spread=a_spread,
        risk_per_order=risk_per_order,
        execution_intensity_slope=execution_intensity_slope,
        risk_horizon_s=risk_horizon_s,
        historical_p3_scalar_adapter_enabled=(
            historical_p3_scalar_adapter_enabled
        ),
        p3_side_bbo_floor_enabled=p3_side_bbo_floor_enabled,
        trade_intensity_acceleration_spread_mult=acceleration_spread_mult,
    )


def quote_core_config_from_params(
    params: Mapping[str, Any],
    *,
    tick_size: float,
    lot_size: float,
    use_ml: bool,
    use_depth_microprice: bool,
    use_depth_kappa: bool,
) -> QuoteCoreConfig:
    max_spread_bps = float(params.get("max_spread_bps", 0.0))
    dyn_base = float(params.get("dynamic_cap_base_bps", max_spread_bps))
    if dyn_base <= 0.0 and max_spread_bps > 0.0:
        dyn_base = max_spread_bps
    dyn_var_base = float(params.get("dynamic_cap_var_baseline", 0.0))
    if dyn_var_base <= 0.0:
        vol_base = float(params.get("vol_baseline", 0.0))
        if vol_base > 0.0:
            dyn_var_base = vol_base * vol_base
    formal_replay = bool(params.get("strict_calibration", False)) or (
        str(params.get("replay_purpose", "")).strip().lower() == "formal"
    )
    f03_ret_action_compatible = bool(
        params.get("f03_ret_action_compatible", False)
    ) and (
        not formal_replay
        or bool(params.get("f03_ret_action_contract_bound", False))
    )
    quote_math_mode = str(
        params.get("quote_math_mode", "legacy_v0") or "legacy_v0"
    ).strip().lower()
    if quote_math_mode not in {"legacy_v0", "quantity_aware_v1"}:
        raise ValueError("quote_math_mode must be legacy_v0 or quantity_aware_v1")
    order_size = finite_positive_quote_coefficient("order_size", params["order_size"])
    legacy_gamma = finite_positive_quote_coefficient("gamma", params["gamma"])
    if quote_math_mode == "quantity_aware_v1":
        cara_risk_aversion = finite_positive_quote_coefficient(
            "cara_risk_aversion",
            params.get("cara_risk_aversion", legacy_gamma),
        )
        default_risk_per_order = cara_risk_aversion * order_size
        inventory_reference_qty = finite_positive_quote_coefficient(
            "inventory_reference_qty",
            params.get("inventory_reference_qty", order_size),
        )
        eta_inventory = params.get("eta_inventory")
        if eta_inventory is None:
            eta_inventory = default_risk_per_order
        a_spread = params.get("a_spread")
        if a_spread is None:
            a_spread = default_risk_per_order
    else:
        default_risk_per_order = params.get("a_spread", legacy_gamma)
        inventory_reference_qty = finite_positive_quote_coefficient(
            "inventory_reference_qty",
            params.get("inventory_reference_qty", 1.0),
        )
        eta_inventory = params.get("eta_inventory")
        a_spread = params.get("a_spread")
    return QuoteCoreConfig(
        gamma=legacy_gamma,
        inventory_reference_qty=inventory_reference_qty,
        eta_inventory=eta_inventory,
        a_spread=a_spread,
        risk_per_order=params.get("risk_per_order", default_risk_per_order),
        execution_intensity_slope=params.get(
            "execution_intensity_slope", params["kappa"]
        ),
        risk_horizon_s=params.get(
            "risk_horizon_s", params.get("quote_horizon_s", 1.0)
        ),
        historical_p3_scalar_adapter_enabled=bool(
            # Pre-unit-split replay bundles predate this explicit identity
            # field, but their B0 behavior always consumed the historical P3
            # pair-spread projection.  Defaulting a missing field to False
            # silently narrows those frozen quotes and breaks live/replay
            # behavior identity.
            params.get("historical_p3_scalar_adapter_enabled", True)
        ),
        p3_side_bbo_floor_enabled=bool(
            params.get("p3_side_bbo_floor_enabled", False)
        ),
        trade_intensity_acceleration_spread_mult=params.get(
            "trade_intensity_acceleration_spread_mult",
            params.get("ber_spread_mult", 2.0),
        ),
        kappa=float(params["kappa"]),
        tick_size=float(tick_size),
        lot_size=float(lot_size),
        maker_fee=float(params["maker_fee"]),
        order_size=order_size,
        max_inventory=float(params["max_inventory"]),
        position_timeout_s=float(params.get("position_timeout", 0.0)),
        quote_horizon_s=max(1e-6, float(params.get("quote_horizon_s", 1.0))),
        pnl_volatility_horizon_s=max(
            1e-6, float(params.get("pnl_volatility_horizon_s", 1.0))
        ),
        ml_enabled=bool(use_ml),
        vol_blend=float(params.get("vol_blend", 0.0)),
        dir_threshold=float(params.get("dir_threshold", 0.05)),
        gamma_dir_bonus=float(params.get("gamma_dir_bonus", 0.0)),
        skew_strength=float(params.get("skew_strength", 0.0)),
        asym_strength=float(params.get("asym_strength", 0.0)),
        ret_skew=float(params.get("ret_skew", 0.0)),
        ret_shift_max_pct=float(params.get("ret_shift_max_pct", 0.3)),
        regime_enabled=bool(params.get("regime_enabled", False)),
        vol_baseline=float(params.get("vol_baseline", 3.0)),
        gamma_scale_min=float(params.get("gamma_scale_min", 0.5)),
        gamma_scale_max=float(params.get("gamma_scale_max", 2.0)),
        liq_baseline=float(params.get("liq_baseline", 200.0)),
        gamma_liq_scale_min=float(params.get("gamma_liq_scale_min", 0.5)),
        gamma_liq_scale_max=float(params.get("gamma_liq_scale_max", 3.0)),
        vol_power=float(params.get("vol_power", 1.5)),
        kappa_ratio=float(params.get("kappa_ratio", 0.3)),
        p3_delta_star=float(params.get("p3_delta_star", 0.0)),
        p3_kappa_eff=float(params.get("p3_kappa_eff", 0.0)),
        p3_identity_required=bool(params.get("p3_identity_required", False)),
        p3_event_type=str(params.get("fill_probability_event_type", "")),
        p3_horizon_s=float(params.get("fill_probability_horizon_s", 0.0)),
        p3_distance_origin=str(params.get("fill_probability_distance_origin", "")),
        p3_distance_unit=str(params.get("fill_probability_distance_unit", "")),
        p3_side=str(params.get("fill_probability_side", "")),
        p3_queue_included=params.get("fill_probability_queue_included"),
        p3_artifact_sha256=str(params.get("fill_probability_artifact_sha256", "")),
        f03_ret_action_horizon_s=float(
            params.get("f03_ret_action_horizon_s", 0.0)
        ),
        f03_ret_action_compatible=f03_ret_action_compatible,
        use_bar_pricing=bool(params.get("use_bar_pricing", True)),
        use_depth_microprice=bool(use_depth_microprice),
        use_depth_kappa=bool(use_depth_kappa),
        microprice_levels=int(params.get("microprice_levels", 3)),
        kappa_levels=int(params.get("kappa_levels", 5)),
        kappa_depth_baseline=float(params.get("kappa_depth_baseline", 50.0)),
        depth_kappa_ratio=max(0.05, min(3.0, float(params.get("depth_kappa_ratio", 0.3)))),
        ber_spread_mult=float(params.get("ber_spread_mult", 2.0)),
        markout_spread_scale=float(params.get("markout_spread_scale", 0.0)),
        markout_side_asymmetry_sign=float(
            params.get("markout_side_asymmetry_sign", 1.0)
        ),
        inventory_skew_strength=float(params.get("inventory_skew_strength", 0.0)),
        inventory_asym_strength=float(params.get("inventory_asym_strength", 0.0)),
        inventory_signal_fade_strength=float(params.get("inventory_signal_fade_strength", 0.0)),
        book_imb_strength=float(params.get("book_imb_strength", 0.0)),
        book_imb_levels=max(1, int(params.get("book_imb_levels", 20))),
        trace_book_imb_levels=max(1, int(params.get("trace_book_imb_levels", 10))),
        depth_tox_enabled=bool(params.get("depth_tox_enabled", False)),
        depth_tox_levels=max(1, int(params.get("depth_tox_levels", 20))),
        depth_tox_imbalance_threshold=float(params.get("depth_tox_imbalance_threshold", 0.65)),
        depth_tox_microprice_shift_bps=float(params.get("depth_tox_microprice_shift_bps", 1.0)),
        depth_tox_spread_mult=max(1.0, float(params.get("depth_tox_spread_mult", 1.25))),
        dynamic_cap_enabled=bool(params.get("dynamic_cap_enabled", False)),
        max_spread_bps=max_spread_bps,
        dynamic_cap_base_bps=dyn_base,
        dynamic_cap_alpha=float(params.get("dynamic_cap_alpha", 0.5)),
        dynamic_cap_max_mult=max(1.0, float(params.get("dynamic_cap_max_mult", 2.0))),
        dynamic_cap_var_baseline=dyn_var_base,
        dynamic_cap_liq_beta=max(0.0, float(params.get("dynamic_cap_liq_beta", 0.0))),
        dynamic_cap_liq_baseline=max(0.0, float(params.get("dynamic_cap_liq_baseline", 0.0))),
        dynamic_cap_min_mult=max(0.0, min(1.0, float(params.get("dynamic_cap_min_mult", 1.0)))),
        spread_cap_mode=spread_cap_mode_code(
            params.get("spread_cap_mode", "pause_exposure")
        ),
        exit_urgency_strength=float(params.get("exit_urgency_strength", 0.0)),
        urgency_time_weight=float(params.get("urgency_time_weight", 0.3)),
        urgency_pnl_weight=float(params.get("urgency_pnl_weight", 0.3)),
        urgency_signal_weight=float(params.get("urgency_signal_weight", 0.4)),
        adverse_guard_enabled=bool(params.get("adverse_guard_enabled", False)),
        adverse_toxicity_threshold=float(params.get("adverse_toxicity_threshold", 0.70)),
        adverse_markout_threshold=abs(float(params.get("adverse_markout_threshold", 5.0))),
        adverse_markout_pause_threshold=abs(float(params.get("adverse_markout_pause_threshold", 0.0))),
        adverse_markout_pause_hybrid=bool(params.get("adverse_markout_pause_hybrid", False)),
        adverse_dir_threshold=abs(float(params.get("adverse_dir_threshold", 0.0))),
        adverse_ret_bps_threshold=abs(float(params.get("adverse_ret_bps_threshold", 0.0))),
        adverse_microprice_shift_bps=abs(float(params.get("adverse_microprice_shift_bps", 0.0))),
        adverse_spread_mult=max(1.0, float(params.get("adverse_spread_mult", 1.10))),
        adverse_thin_depth_threshold=max(0.0, float(params.get("adverse_thin_depth_threshold", 0.0))),
        adverse_thin_depth_mult=max(1.0, float(params.get("adverse_thin_depth_mult", 1.0))),
        adverse_pause=bool(params.get("adverse_pause", True)),
        defense_guard_enabled=bool(params.get("defense_guard_enabled", False)),
        defense_markout_threshold=abs(float(params.get("defense_markout_threshold", 2.0))),
        defense_dir_threshold=abs(float(params.get("defense_dir_threshold", 0.05))),
        defense_ret_bps_threshold=abs(float(params.get("defense_ret_bps_threshold", 0.0))),
        defense_microprice_shift_bps=abs(float(params.get("defense_microprice_shift_bps", 0.0))),
        defense_spread_mult=max(1.0, float(params.get("defense_spread_mult", 1.35))),
        defense_pause=bool(params.get("defense_pause", True)),
        defense_emergency_inventory_ratio=max(0.0, float(params.get("defense_emergency_inventory_ratio", 0.50))),
        defense_emergency_loss=max(0.0, float(params.get("defense_emergency_loss", 5.0))),
    )


def _compute_quote_core_py(
    state: QuoteState,
    cfg: QuoteCoreConfig,
    pred: QuotePrediction | Any,
    depth: DepthSnapshot | None = None,
) -> QuoteCoreResult:
    depth = depth or DepthSnapshot()
    tick = max(float(cfg.tick_size), 1e-12)
    mid = float(state.mid)
    q = float(state.inventory)
    inventory_reference_qty = float(cfg.inventory_reference_qty)
    inventory_units = q / inventory_reference_qty
    eta_inventory = float(cfg.eta_inventory)
    risk_per_order = float(cfg.risk_per_order)
    sigma_sq_raw = max(float(state.sigma_sq), 0.0)
    pred_dir = _float(_get(pred, "dir_10s", 0.5), 0.5)
    pred_vol = _float(_get(pred, "vol_10s", 0.0), 0.0)
    pred_ret = _float(_get(pred, "ret_10s", 0.0), 0.0)
    tox_bid = _float(_get(pred, "tox_bid", _get(pred, "tox_bid_10s", 0.5)), 0.5)
    tox_ask = _float(_get(pred, "tox_ask", _get(pred, "tox_ask_10s", 0.5)), 0.5)

    distance_decay_source = "execution_intensity_slope"
    distance_decay_before_depth = float(cfg.execution_intensity_slope)
    if (
        cfg.historical_p3_scalar_adapter_enabled
        and cfg.p3_kappa_eff > 0.0
    ):
        distance_decay_source = "legacy_p3_touch_slope_projection"
        distance_decay_before_depth = float(cfg.p3_kappa_eff)
    kappa_used = max(distance_decay_before_depth, 1e-12)
    kappa_before_depth = kappa_used

    fair = mid
    if depth.has_book and cfg.use_depth_microprice:
        fair = _weighted_mid_proxy(depth, cfg.microprice_levels, mid)
    if depth.has_book and cfg.use_depth_kappa:
        kappa_used = _estimate_depth_kappa(
            depth,
            kappa_used,
            cfg.kappa_depth_baseline,
            cfg.kappa_levels,
            cfg.depth_kappa_ratio,
        )

    sigma_sq = sigma_sq_raw
    if cfg.ml_enabled and cfg.vol_blend > 0.0 and pred_vol > 1e-8:
        sigma_sq = (1.0 - cfg.vol_blend) * sigma_sq_raw + cfg.vol_blend * max(pred_vol, 0.0)
    sigma_sq = max(sigma_sq, 1e-6)
    quote_horizon_s = max(float(cfg.quote_horizon_s), 1e-6)
    risk_horizon_s = max(float(cfg.risk_horizon_s), 1e-6)
    sigma_sq_horizon = sigma_sq * risk_horizon_s

    regime_spread_scale = 1.0
    g_base = eta_inventory
    if cfg.regime_enabled:
        if cfg.liq_baseline > 0.0 and state.trade_intensity > 0.0:
            liq_ratio = state.trade_intensity / cfg.liq_baseline
            liq_scale = 1.0 / max(math.sqrt(liq_ratio), 0.2)
            regime_spread_scale *= max(cfg.gamma_liq_scale_min, min(cfg.gamma_liq_scale_max, liq_scale))
        if cfg.vol_baseline > 0.0:
            vol_sq_ratio = sigma_sq / (cfg.vol_baseline * cfg.vol_baseline)
            vol_sq_ratio = max(vol_sq_ratio, 0.09)
            vol_scale = vol_sq_ratio ** (cfg.vol_power * 0.5)
            regime_spread_scale *= max(cfg.gamma_scale_min, min(cfg.gamma_scale_max, vol_scale))

    if cfg.max_inventory > 0.0 and abs(q) > 0.0:
        inv_ratio_g = abs(q) / cfg.max_inventory
        g_base *= 1.0 + inv_ratio_g * inv_ratio_g

    dir_signal = pred_dir - 0.5 if cfg.ml_enabled else 0.0
    active_dir = abs(dir_signal) > cfg.dir_threshold
    g_eff = g_base
    if active_dir and cfg.gamma_dir_bonus > 0.0:
        align = 0.0
        if q > 0.0:
            align = dir_signal
        elif q < 0.0:
            align = -dir_signal
        g_eff = g_base * (1.0 - cfg.gamma_dir_bonus * align * 2.0)
        g_eff = max(g_base * 0.2, min(g_base * 3.0, g_eff))

    r = fair - inventory_units * g_eff * sigma_sq_horizon
    kappa_spread = max(kappa_used * cfg.kappa_ratio, 1e-12)
    delta = risk_per_order * sigma_sq_horizon + (
        (2.0 / risk_per_order)
        * math.log(1.0 + risk_per_order / kappa_spread)
    )

    delta_raw = delta
    delta *= regime_spread_scale
    delta_after_regime = delta

    if (
        state.ber_active
        and cfg.trade_intensity_acceleration_spread_mult > 1.0
    ):
        delta *= cfg.trade_intensity_acceleration_spread_mult

    if cfg.markout_spread_scale > 0.0 and state.mo_ema_all != 0.0:
        mo_ratio = state.mo_ema_all / max(state.mo_ref, 1e-6)
        mo_adj = 1.0 - cfg.markout_spread_scale * math.tanh(mo_ratio)
        delta *= max(0.5, min(2.0, mo_adj))

    depth_tox_mult = _depth_tox_mult(mid, depth, cfg)
    if depth_tox_mult > 1.0:
        delta *= depth_tox_mult

    p3_pair_floor = 0.0
    p3_floor_mode = "inactive"
    if (
        cfg.regime_enabled
        and cfg.historical_p3_scalar_adapter_enabled
        and cfg.p3_delta_star > 0.0
    ):
        # The frozen B0 mechanism used one pooled same-side-BBO touch distance
        # as a symmetric pair-spread floor.  Keep that exact projection for
        # behavior identity while making the coordinate conversion explicit;
        # it must not be misread as a side-specific fill-probability optimum.
        p3_pair_floor = 2.0 * float(cfg.p3_delta_star)
        p3_floor_mode = (
            "legacy_pair_projection_from_same_side_bbo"
            if cfg.p3_event_type
            else "legacy_naked_pair_floor"
        )
        delta = max(delta, p3_pair_floor)

    min_spread = 2.0 * abs(cfg.maker_fee) * mid + tick
    delta = max(delta, min_spread)

    # Near-book depth (liquidity proxy) is needed for the liquidity-aware cap
    # below; computed once here and reused for book-imbalance asym further down.
    near_depth_total = (
        _near_depth_total(depth, cfg.trace_book_imb_levels) if depth.has_book else 0.0
    )

    delta_pre_cap = delta
    cap_hit = False
    delta_cap_hit = False
    cap_exposure_block = False
    cap_reason = "none"
    cap_mode = spread_cap_mode_code(cfg.spread_cap_mode)
    cap_bps = float(cfg.max_spread_bps)
    if cfg.dynamic_cap_enabled and cfg.dynamic_cap_base_bps > 0.0:
        cap_mult = 1.0
        if cfg.dynamic_cap_var_baseline > 1e-12:
            cap_ratio = max(1.0, sigma_sq / cfg.dynamic_cap_var_baseline)
            cap_mult = cap_ratio ** cfg.dynamic_cap_alpha
        # Liquidity term: thin near-book depth widens the cap, thick depth
        # tightens it (cap_t = cap_0 * clamp(vol^alpha * (liq_ref/liq)^beta, lo, hi)).
        if (
            cfg.dynamic_cap_liq_beta > 0.0
            and cfg.dynamic_cap_liq_baseline > 1e-12
            and near_depth_total > 1e-12
        ):
            liq_ratio = cfg.dynamic_cap_liq_baseline / near_depth_total
            cap_mult *= liq_ratio ** cfg.dynamic_cap_liq_beta
        cap_mult = max(cfg.dynamic_cap_min_mult, min(cfg.dynamic_cap_max_mult, cap_mult))
        cap_bps = cfg.dynamic_cap_base_bps * cap_mult

    max_spread = 0.0
    if cap_bps > 0.0:
        max_spread = mid * cap_bps / 10000.0
        if delta > max_spread:
            cap_hit = True
            delta_cap_hit = True
            cap_reason = "delta"
            if cap_mode == SPREAD_CAP_COMPRESS:
                delta = max_spread
            elif cap_mode == SPREAD_CAP_PAUSE_EXPOSURE:
                cap_exposure_block = True

    half_d = delta * 0.5
    r_shift = 0.0
    rs_clamp = 0.0

    if cfg.inventory_skew_strength > 0.0 and cfg.max_inventory > 1e-10:
        r -= cfg.inventory_skew_strength * (q / cfg.max_inventory) * delta

    if active_dir and cfg.skew_strength > 0.0:
        dir_r_shift = cfg.skew_strength * dir_signal * delta
        if cfg.max_inventory > 1e-10 and abs(q) > 1e-10:
            inv_r = min(abs(q) / cfg.max_inventory, 1.0)
            if (q > 0.0 and dir_r_shift > 0.0) or (q < 0.0 and dir_r_shift < 0.0):
                dir_r_shift *= 1.0 - inv_r
        r += dir_r_shift

    if cfg.ml_enabled and cfg.ret_skew > 0.0:
        r_shift = pred_ret * cfg.ret_skew * mid
        rs_clamp = cfg.ret_shift_max_pct * half_d
        r_shift = max(-rs_clamp, min(rs_clamp, r_shift))
        if cfg.max_inventory > 1e-10 and abs(q) > 1e-10:
            inv_r = min(abs(q) / cfg.max_inventory, 1.0)
            adds_exp = (q > 0.0 and r_shift > 0.0) or (q < 0.0 and r_shift < 0.0)
            if adds_exp:
                r_shift *= 1.0 - inv_r
        r += r_shift

    asym = 0.0
    if active_dir and cfg.asym_strength > 0.0:
        asym = cfg.asym_strength * dir_signal * 2.0

    if cfg.exit_urgency_strength > 0.0 and abs(q) > 1e-8 and state.position_open:
        hold_ratio = (
            min(state.hold_time_s / cfg.position_timeout_s, 1.0)
            if cfg.position_timeout_s > 0.0 else 0.0
        )
        time_urg = hold_ratio * hold_ratio
        pnl_urg = 0.0
        if sigma_sq > 1e-10 and state.unrealized_pnl < 0.0:
            dollar_vol = price_variance_pnl_sigma(
                sigma_sq,
                cfg.pnl_volatility_horizon_s,
                q,
            )
            if dollar_vol > 1e-8:
                pnl_urg = min(-state.unrealized_pnl / dollar_vol, 3.0)
        signal_urg = 0.0
        if q > 0.0 and dir_signal < 0.0:
            signal_urg = min(abs(dir_signal) * 2.0, 1.0)
        elif q < 0.0 and dir_signal > 0.0:
            signal_urg = min(abs(dir_signal) * 2.0, 1.0)
        urgency = (
            cfg.urgency_time_weight * time_urg
            + cfg.urgency_pnl_weight * pnl_urg
            + cfg.urgency_signal_weight * signal_urg
        )
        inv_sign = 1.0 if q > 0.0 else -1.0
        asym -= inv_sign * min(urgency, 1.0) * cfg.exit_urgency_strength

    trace_book_imb = 0.0
    microprice_shift_bps = (fair - mid) / mid * 10000.0 if mid > 0.0 else 0.0
    if depth.has_book:
        asym_imb, _, _ = _depth_imbalance(depth, cfg.book_imb_levels)
        trace_book_imb, _, _ = _depth_imbalance(depth, cfg.trace_book_imb_levels)
        if cfg.book_imb_strength > 0.0:
            asym += asym_imb * cfg.book_imb_strength

    if cfg.markout_spread_scale > 0.0 and (state.mo_ema_bid != 0.0 or state.mo_ema_ask != 0.0):
        mo_diff = state.mo_ema_bid - state.mo_ema_ask
        mo_asym = (
            cfg.markout_side_asymmetry_sign
            * cfg.markout_spread_scale
            * math.tanh(mo_diff / max(state.mo_ref, 1e-6))
            * 0.5
        )
        asym += mo_asym

    inv_ratio_ctrl = 0.0
    if cfg.max_inventory > 1e-10 and abs(q) > 1e-10:
        inv_ratio_ctrl = min(abs(q) / cfg.max_inventory, 1.0)

    if cfg.inventory_signal_fade_strength > 0.0 and inv_ratio_ctrl > 0.0:
        adds_exp = (q > 0.0 and asym > 0.0) or (q < 0.0 and asym < 0.0)
        if adds_exp:
            asym *= max(0.0, 1.0 - cfg.inventory_signal_fade_strength * inv_ratio_ctrl)

    if cfg.inventory_asym_strength > 0.0 and inv_ratio_ctrl > 0.0:
        inv_sign = 1.0 if q > 0.0 else -1.0
        asym -= inv_sign * cfg.inventory_asym_strength * inv_ratio_ctrl

    asym = max(-0.9, min(0.9, asym))
    raw_hd = delta_pre_cap * 0.5
    raw_hd_bid = raw_hd * (1.0 - asym)
    raw_hd_ask = raw_hd * (1.0 + asym)
    hd_bid = half_d * (1.0 - asym)
    hd_ask = half_d * (1.0 + asym)

    raw_bid_px = r - raw_hd_bid
    raw_ask_px = r + raw_hd_ask
    raw_pair_spread = max(raw_ask_px - raw_bid_px, tick)
    raw_mid_shift = 0.5 * (raw_bid_px + raw_ask_px) - fair
    raw_reservation_shift = r - fair
    raw_asym_shift = raw_hd * asym
    raw_quote_skew = (
        ((raw_ask_px - mid) - (mid - raw_bid_px)) / raw_pair_spread
        if raw_pair_spread > 1e-12 else 0.0
    )

    bid_side_adverse = _side_adverse_state(
        "BUY",
        inventory=q,
        quantity=cfg.order_size,
        lot_size=cfg.lot_size,
        dir_signal=dir_signal,
        pred_ret=pred_ret,
        toxicity=tox_bid,
        markout_ema=state.mo_ema_bid,
        markout_pause_latch=state.bid_adverse_markout_pause_latch,
        microprice_shift_bps=microprice_shift_bps,
        near_depth=near_depth_total,
        cfg=cfg,
    )
    ask_side_adverse = _side_adverse_state(
        "SELL",
        inventory=q,
        quantity=cfg.order_size,
        lot_size=cfg.lot_size,
        dir_signal=dir_signal,
        pred_ret=pred_ret,
        toxicity=tox_ask,
        markout_ema=state.mo_ema_ask,
        markout_pause_latch=state.ask_adverse_markout_pause_latch,
        microprice_shift_bps=microprice_shift_bps,
        near_depth=near_depth_total,
        cfg=cfg,
    )
    bid_defense = _side_defense_state(
        "BUY",
        inventory=q,
        max_inventory=cfg.max_inventory,
        dir_signal=dir_signal,
        pred_ret=pred_ret,
        markout_ema=state.mo_ema_bid,
        microprice_shift_bps=microprice_shift_bps,
        unrealized_pnl=float(state.unrealized_pnl),
        cfg=cfg,
    )
    ask_defense = _side_defense_state(
        "SELL",
        inventory=q,
        max_inventory=cfg.max_inventory,
        dir_signal=dir_signal,
        pred_ret=pred_ret,
        markout_ema=state.mo_ema_ask,
        microprice_shift_bps=microprice_shift_bps,
        unrealized_pnl=float(state.unrealized_pnl),
        cfg=cfg,
    )

    bid_adverse_active = bool(bid_side_adverse["active"])
    bid_adverse_pause_active = bool(bid_side_adverse["pause"])
    bid_adverse_toxicity_active = bool(bid_side_adverse["toxicity"])
    bid_adverse_markout_active = bool(bid_side_adverse["markout"])
    ask_adverse_active = bool(ask_side_adverse["active"])
    ask_adverse_pause_active = bool(ask_side_adverse["pause"])

    pre_guard_bid = _floor_tick(r - hd_bid, tick)
    pre_guard_ask = _ceil_tick(r + hd_ask, tick)
    bid_price = pre_guard_bid
    ask_price = pre_guard_ask

    mid_guard_bid = False
    mid_guard_ask = False
    if bid_price >= mid:
        mid_guard_bid = True
        bid_price = _floor_tick(mid, tick)
        if bid_price >= mid:
            bid_price -= tick
    if ask_price <= mid:
        mid_guard_ask = True
        ask_price = _ceil_tick(mid, tick)
        if ask_price <= mid:
            ask_price += tick

    post_only_ask = False
    post_only_bid = False
    if state.best_bid > 0.0 and ask_price <= state.best_bid:
        ask_price = state.best_bid + tick
        post_only_ask = True
    if state.best_ask > 0.0 and bid_price >= state.best_ask:
        bid_price = state.best_ask - tick
        post_only_bid = True

    final_compressed = False
    final_cap_excess = 0.0
    final_cap_rounding = False
    final_cap_mid_guard = False
    final_cap_post_only = False
    final_cap_delta = False
    if max_spread > 0.0 and ask_price - bid_price > max_spread:
        pre_final_spread = ask_price - bid_price
        cap_hit = True
        final_cap_excess = pre_final_spread - max_spread
        if cap_mode == SPREAD_CAP_COMPRESS:
            bid_price, ask_price, _, final_cap_excess = apply_final_spread_cap(
                mid, bid_price, ask_price, max_spread, tick
            )
            final_cap_rounding = pre_final_spread <= max_spread + 2.0 * tick + 1e-12
            final_cap_mid_guard = mid_guard_bid or mid_guard_ask
            final_cap_post_only = post_only_bid or post_only_ask
            final_cap_delta = delta_cap_hit
            final_compressed = True
        elif cap_mode == SPREAD_CAP_PAUSE_EXPOSURE:
            cap_exposure_block = True
        if cap_reason == "none":
            cap_reason = "post_only"

    if bid_adverse_active and not bid_adverse_pause_active:
        # 中文说明：adverse widening 在 quote-core 的第一轮 cap 后执行；
        # live/replay policy executor 还必须再做 post-policy cap，不能假设
        # 这里返回的 pair 一定已经满足最终 max_spread。
        bid_dist_policy = max(mid - bid_price, tick)
        bid_spread_mult = float(bid_side_adverse["spread_mult"])
        bid_price = _floor_tick(mid - bid_dist_policy * bid_spread_mult, tick)
        if bid_price >= mid:
            bid_price = _floor_tick(mid, tick)
            if bid_price >= mid:
                bid_price -= tick
        if cap_reason == "none":
            cap_reason = "bid_adverse"
    if ask_adverse_active and not ask_adverse_pause_active:
        # See BUY-side note above: this is protective widening, not a final
        # exchange-ready spread guarantee.
        ask_dist_policy = max(ask_price - mid, tick)
        ask_price = _ceil_tick(mid + ask_dist_policy * float(ask_side_adverse["spread_mult"]), tick)
        if ask_price <= mid:
            ask_price = _ceil_tick(mid, tick)
            if ask_price <= mid:
                ask_price += tick
        if cap_reason == "none":
            cap_reason = "ask_adverse"

    p3_buy_side_floor_changed = False
    p3_sell_side_floor_changed = False
    p3_buy_floor_price = 0.0
    p3_sell_floor_price = 0.0
    if cfg.p3_side_bbo_floor_enabled and cfg.p3_delta_star > 0.0:
        p3_floor_mode = "same_side_bbo_floor"
        (
            bid_price,
            ask_price,
            p3_buy_floor_price,
            p3_sell_floor_price,
            p3_buy_side_floor_changed,
            p3_sell_side_floor_changed,
        ) = apply_p3_side_bbo_floor(
            bid_price,
            ask_price,
            enabled=True,
            delta_star=cfg.p3_delta_star,
            best_bid=state.best_bid,
            best_ask=state.best_ask,
            tick_size=tick,
        )
        if max_spread > 0.0 and ask_price - bid_price > max_spread:
            cap_hit = True
            final_cap_excess = max(
                final_cap_excess,
                ask_price - bid_price - max_spread,
            )
            if cap_mode != SPREAD_CAP_OBSERVE:
                # A max-spread control cannot pull a side back through a
                # declared same-side-BBO safety floor.  NarrowGate resolves
                # the conflict by blocking exposure-increasing routing.
                cap_exposure_block = True
            cap_reason = "p3_side_floor"

    final_pair_spread = max(ask_price - bid_price, tick)
    bid_final_guard_changed = abs(bid_price - pre_guard_bid) > (tick * 0.5)
    ask_final_guard_changed = abs(ask_price - pre_guard_ask) > (tick * 0.5)
    final_mid_shift = 0.5 * (bid_price + ask_price) - fair
    final_quote_skew = (
        ((ask_price - mid) - (mid - bid_price)) / final_pair_spread
        if final_pair_spread > 1e-12 else 0.0
    )

    raw_bias_side = "balanced"
    if raw_mid_shift > tick:
        raw_bias_side = "BUY"
    elif raw_mid_shift < -tick:
        raw_bias_side = "SELL"
    final_bias_side = "balanced"
    if final_mid_shift > tick:
        final_bias_side = "BUY"
    elif final_mid_shift < -tick:
        final_bias_side = "SELL"

    capped_pair_spread = max(pre_guard_ask - pre_guard_bid, tick)
    common_ctx: dict[str, Any] = {
        "raw_half_spread": raw_hd,
        "capped_half_spread": half_d,
        "raw_mid_shift": raw_mid_shift,
        "raw_reservation_shift": raw_reservation_shift,
        "raw_asym_shift": raw_asym_shift,
        "asym": asym,
        "inventory": q,
        "inventory_reference_qty": inventory_reference_qty,
        "inventory_units": inventory_units,
        "order_size": float(cfg.order_size),
        "order_units": float(cfg.order_size) / inventory_reference_qty,
        "dir_signal": dir_signal,
        "pred_dir": pred_dir,
        "pred_ret": pred_ret,
        "tox_bid": tox_bid,
        "tox_ask": tox_ask,
        "book_imb": trace_book_imb,
        "weighted_mid_proxy_shift_bps": microprice_shift_bps,
        "microprice_shift_bps": microprice_shift_bps,
        "near_depth_total": near_depth_total,
        "mo_ema_bid": state.mo_ema_bid,
        "mo_ema_ask": state.mo_ema_ask,
        "fair": fair,
        "mid": mid,
        "best_bid": state.best_bid,
        "best_ask": state.best_ask,
        "raw_pair_spread": raw_pair_spread,
        "capped_pair_spread": capped_pair_spread,
        "final_pair_spread": final_pair_spread,
        "raw_quote_skew": raw_quote_skew,
        "final_quote_skew": final_quote_skew,
        "raw_bias_side": raw_bias_side,
        "final_bias_side": final_bias_side,
        "delta_cap": delta_cap_hit,
        "final_compressed": final_compressed,
        "side_adverse_enabled": cfg.adverse_guard_enabled,
        "adverse_toxicity": False,
        "adverse_markout": False,
        "adverse_direction": False,
        "adverse_ret": False,
        "adverse_microprice": False,
        "adverse_thin_depth": False,
        "side_adverse": False,
        "side_adverse_pause": False,
        "side_adverse_spread_mult": 1.0,
        "defense_guard_enabled": cfg.defense_guard_enabled,
        "defense_guard": False,
        "defense_pause": False,
        "defense_reducing": False,
        "defense_emergency": False,
        "defense_markout": False,
        "defense_direction": False,
        "defense_ret": False,
        "defense_microprice": False,
        "defense_spread_mult": 1.0,
        "bid_adverse": bid_adverse_active,
        "ask_adverse": ask_adverse_active,
        "p3_floor_mode": p3_floor_mode,
        "p3_touch_delta_star": float(cfg.p3_delta_star),
        "p3_pair_floor": p3_pair_floor,
        "p3_distance_origin": str(cfg.p3_distance_origin),
        "p3_buy_floor_price": p3_buy_floor_price,
        "p3_sell_floor_price": p3_sell_floor_price,
        "p3_side_floor_changed": (
            p3_buy_side_floor_changed or p3_sell_side_floor_changed
        ),
    }
    quote_context = {
        "BUY": {
            **common_ctx,
            "adverse_toxicity": bool(bid_side_adverse["toxicity"]),
            "adverse_markout": bool(bid_side_adverse["markout"]),
            "adverse_direction": bool(bid_side_adverse["direction"]),
            "adverse_ret": bool(bid_side_adverse["ret"]),
            "adverse_microprice": bool(bid_side_adverse["microprice"]),
            "adverse_thin_depth": bool(bid_side_adverse["thin_depth"]),
            "side_adverse": bool(bid_adverse_active),
            "side_adverse_pause": bool(bid_adverse_pause_active),
            "side_adverse_spread_mult": float(bid_side_adverse["spread_mult"]),
            "defense_guard": bool(bid_defense["active"]),
            "defense_pause": bool(bid_defense["pause"]),
            "defense_reducing": bool(bid_defense["reducing"]),
            "defense_emergency": bool(bid_defense["emergency"]),
            "defense_markout": bool(bid_defense["markout"]),
            "defense_direction": bool(bid_defense["direction"]),
            "defense_ret": bool(bid_defense["ret"]),
            "defense_microprice": bool(bid_defense["microprice"]),
            "defense_spread_mult": float(bid_defense["spread_mult"]),
            "raw_price": raw_bid_px,
            "pre_guard_price": pre_guard_bid,
            "final_price": bid_price,
            "raw_quote_delta_to_bbo": state.best_bid - raw_bid_px if state.best_bid > 0.0 else 0.0,
            "pre_guard_delta_to_bbo": state.best_bid - pre_guard_bid if state.best_bid > 0.0 else 0.0,
            "final_quote_delta_to_bbo": state.best_bid - bid_price if state.best_bid > 0.0 else 0.0,
            "raw_distance_to_mid": mid - raw_bid_px,
            "final_distance_to_mid": mid - bid_price,
            "favored_by_raw_shift": raw_mid_shift > tick,
            "mid_guard": mid_guard_bid,
            "post_only": post_only_bid,
            "ask_adverse": False,
            "final_guard_changed": bid_final_guard_changed,
            "any_constraint_changed": (
                delta_cap_hit or final_compressed or bid_final_guard_changed
                or cap_exposure_block or p3_buy_side_floor_changed
                or bid_adverse_active or bool(bid_defense["active"])
            ),
            "cap_exposure_block": bool(
                cap_exposure_block
                and bid_side_adverse["exposure_increasing"]
            ),
        },
        "SELL": {
            **common_ctx,
            "adverse_toxicity": bool(ask_side_adverse["toxicity"]),
            "adverse_markout": bool(ask_side_adverse["markout"]),
            "adverse_direction": bool(ask_side_adverse["direction"]),
            "adverse_ret": bool(ask_side_adverse["ret"]),
            "adverse_microprice": bool(ask_side_adverse["microprice"]),
            "adverse_thin_depth": bool(ask_side_adverse["thin_depth"]),
            "side_adverse": bool(ask_side_adverse["active"]),
            "side_adverse_pause": bool(ask_side_adverse["pause"]),
            "side_adverse_spread_mult": float(ask_side_adverse["spread_mult"]),
            "defense_guard": bool(ask_defense["active"]),
            "defense_pause": bool(ask_defense["pause"]),
            "defense_reducing": bool(ask_defense["reducing"]),
            "defense_emergency": bool(ask_defense["emergency"]),
            "defense_markout": bool(ask_defense["markout"]),
            "defense_direction": bool(ask_defense["direction"]),
            "defense_ret": bool(ask_defense["ret"]),
            "defense_microprice": bool(ask_defense["microprice"]),
            "defense_spread_mult": float(ask_defense["spread_mult"]),
            "raw_price": raw_ask_px,
            "pre_guard_price": pre_guard_ask,
            "final_price": ask_price,
            "raw_quote_delta_to_bbo": raw_ask_px - state.best_ask if state.best_ask > 0.0 else 0.0,
            "pre_guard_delta_to_bbo": pre_guard_ask - state.best_ask if state.best_ask > 0.0 else 0.0,
            "final_quote_delta_to_bbo": ask_price - state.best_ask if state.best_ask > 0.0 else 0.0,
            "raw_distance_to_mid": raw_ask_px - mid,
            "final_distance_to_mid": ask_price - mid,
            "favored_by_raw_shift": raw_mid_shift < -tick,
            "mid_guard": mid_guard_ask,
            "post_only": post_only_ask,
            "bid_adverse": False,
            "ask_adverse": ask_adverse_active,
            "final_guard_changed": ask_final_guard_changed,
            "any_constraint_changed": (
                delta_cap_hit or final_compressed or ask_final_guard_changed
                or cap_exposure_block or p3_sell_side_floor_changed
                or ask_adverse_active or bool(ask_defense["active"])
            ),
            "cap_exposure_block": bool(
                cap_exposure_block
                and ask_side_adverse["exposure_increasing"]
            ),
        },
    }
    quote_flags = {
        "final_compressed": final_compressed,
        "delta_cap": delta_cap_hit,
        "mid_guard": mid_guard_bid or mid_guard_ask,
        "post_only": post_only_bid or post_only_ask,
        "bid_adverse": bid_adverse_active,
        "ask_adverse": ask_adverse_active,
        "side_adverse": bid_adverse_active or ask_adverse_active,
        "defense_guard": bool(bid_defense["active"] or ask_defense["active"]),
        "cap_exposure_block": cap_exposure_block,
    }
    diagnostics = {
        "fair": fair,
        "reservation_price": r,
        "sigma_sq_raw": sigma_sq_raw,
        "sigma_sq_blended": sigma_sq,
        "quote_horizon_s": quote_horizon_s,
        "risk_horizon_s": risk_horizon_s,
        "sigma_sq_horizon": sigma_sq_horizon,
        "inventory_reference_qty": inventory_reference_qty,
        "inventory_units": inventory_units,
        "order_size": float(cfg.order_size),
        "order_units": float(cfg.order_size) / inventory_reference_qty,
        "eta_inventory": eta_inventory,
        "a_spread": float(cfg.a_spread),
        "risk_per_order": risk_per_order,
        "execution_intensity_slope": float(cfg.execution_intensity_slope),
        "distance_decay_source": distance_decay_source,
        "p3_floor_mode": p3_floor_mode,
        "p3_touch_delta_star": float(cfg.p3_delta_star),
        "p3_pair_floor": p3_pair_floor,
        "p3_buy_floor_price": p3_buy_floor_price,
        "p3_sell_floor_price": p3_sell_floor_price,
        "p3_side_floor_changed": (
            p3_buy_side_floor_changed or p3_sell_side_floor_changed
        ),
        "historical_p3_scalar_adapter_enabled": bool(
            cfg.historical_p3_scalar_adapter_enabled
        ),
        "p3_side_bbo_floor_enabled": bool(cfg.p3_side_bbo_floor_enabled),
        "p3_event_type": str(cfg.p3_event_type),
        "p3_horizon_s": float(cfg.p3_horizon_s),
        "p3_distance_origin": str(cfg.p3_distance_origin),
        "p3_distance_unit": str(cfg.p3_distance_unit),
        "p3_side": str(cfg.p3_side),
        "p3_queue_included": cfg.p3_queue_included,
        "p3_artifact_sha256": str(cfg.p3_artifact_sha256),
        "f03_ret_action_horizon_s": float(cfg.f03_ret_action_horizon_s),
        "f03_ret_action_compatible": bool(cfg.f03_ret_action_compatible),
        "delta_raw": delta_raw,
        "delta_after_regime": delta_after_regime,
        "delta_pre_cap": delta_pre_cap,
        "delta_after_cap": delta,
        "capped_pair_spread": capped_pair_spread,
        "cap_hit": cap_hit,
        "delta_cap_hit": delta_cap_hit,
        "cap_reason": cap_reason,
        "spread_cap_mode": spread_cap_mode_name(cap_mode),
        "cap_exposure_block": cap_exposure_block,
        "cap_bps": cap_bps,
        "max_spread": max_spread,
        "half_d": half_d,
        "asym": asym,
        "r_shift": r_shift,
        "rs_clamp": rs_clamp,
        "dir_signal": dir_signal,
        "kappa_before_depth": kappa_before_depth,
        "kappa_used": kappa_used,
        "trade_intensity_acceleration_guard_active": bool(state.ber_active),
        "trade_intensity_acceleration_spread_mult": float(
            cfg.trade_intensity_acceleration_spread_mult
        ),
        "depth_tox_mult": depth_tox_mult,
        "bid_adverse_active": bid_adverse_active,
        "bid_adverse_toxicity_active": bid_adverse_toxicity_active,
        "bid_adverse_markout_active": bid_adverse_markout_active,
        "bid_adverse_pause_active": bid_adverse_pause_active,
        "ask_adverse_active": ask_adverse_active,
        "ask_adverse_pause_active": ask_adverse_pause_active,
        "bid_defense_active": bool(bid_defense["active"]),
        "bid_defense_pause_active": bool(bid_defense["pause"]),
        "bid_defense_reducing": bool(bid_defense["reducing"]),
        "bid_defense_emergency": bool(bid_defense["emergency"]),
        "bid_defense_markout_active": bool(bid_defense["markout"]),
        "bid_defense_direction_active": bool(bid_defense["direction"]),
        "bid_defense_ret_active": bool(bid_defense["ret"]),
        "bid_defense_microprice_active": bool(bid_defense["microprice"]),
        "ask_defense_active": bool(ask_defense["active"]),
        "ask_defense_pause_active": bool(ask_defense["pause"]),
        "ask_defense_reducing": bool(ask_defense["reducing"]),
        "ask_defense_emergency": bool(ask_defense["emergency"]),
        "ask_defense_markout_active": bool(ask_defense["markout"]),
        "ask_defense_direction_active": bool(ask_defense["direction"]),
        "ask_defense_ret_active": bool(ask_defense["ret"]),
        "ask_defense_microprice_active": bool(ask_defense["microprice"]),
        "adverse_guard_enabled": cfg.adverse_guard_enabled,
        "bid_adverse_direction_active": bool(bid_side_adverse["direction"]),
        "bid_adverse_ret_active": bool(bid_side_adverse["ret"]),
        "bid_adverse_microprice_active": bool(bid_side_adverse["microprice"]),
        "bid_adverse_thin_depth_active": bool(bid_side_adverse["thin_depth"]),
        "ask_adverse_toxicity_active": bool(ask_side_adverse["toxicity"]),
        "ask_adverse_markout_active": bool(ask_side_adverse["markout"]),
        "ask_adverse_direction_active": bool(ask_side_adverse["direction"]),
        "ask_adverse_ret_active": bool(ask_side_adverse["ret"]),
        "ask_adverse_microprice_active": bool(ask_side_adverse["microprice"]),
        "ask_adverse_thin_depth_active": bool(ask_side_adverse["thin_depth"]),
        "mid_guard_bid": mid_guard_bid,
        "mid_guard_ask": mid_guard_ask,
        "post_only_bid": post_only_bid,
        "post_only_ask": post_only_ask,
        "final_compressed": final_compressed,
        "final_cap_excess": final_cap_excess,
        "final_cap_rounding": final_cap_rounding,
        "final_cap_mid_guard": final_cap_mid_guard,
        "final_cap_post_only": final_cap_post_only,
        "final_cap_delta": final_cap_delta,
        "pre_guard_bid": pre_guard_bid,
        "pre_guard_ask": pre_guard_ask,
        "raw_bid_px": raw_bid_px,
        "raw_ask_px": raw_ask_px,
    }
    final_quote_delta = {
        "BUY": quote_context["BUY"]["final_quote_delta_to_bbo"],
        "SELL": quote_context["SELL"]["final_quote_delta_to_bbo"],
    }
    return QuoteCoreResult(
        bid_price=bid_price,
        ask_price=ask_price,
        spread=ask_price - bid_price,
        raw_half_spread=raw_hd,
        raw_mid_shift=raw_mid_shift,
        final_quote_delta=final_quote_delta,
        quote_context=quote_context,
        quote_flags=quote_flags,
        diagnostics=diagnostics,
    )


_CPP_QUOTE_CORE = None
_CPP_QUOTE_CORE_IMPORT_FAILED = False
_CPP_CFG_CACHE_KEY = None
_CPP_CFG_CACHE_REF = None
_CPP_CFG_CACHE_VALUE = None
_DEFERRED_NATIVE_QUOTE_RESULT_TYPES: set[type] = set()


def _cpp_strict_enabled() -> bool:
    return os.environ.get("NARROWGATE_CPP_STRICT", "0").lower() in {"1", "true", "yes", "on"}


def _cpp_quote_core_enabled(cfg: QuoteCoreConfig) -> bool:
    flag = os.environ.get("NARROWGATE_CPP_QUOTE_CORE", "").strip().lower()
    if flag not in {"1", "true", "yes", "on"}:
        return False
    return True


def _cpp_module_points_to_repo(repo_root: Path) -> bool:
    try:
        import json
        from importlib import metadata as importlib_metadata
        from urllib.parse import unquote, urlparse
    except Exception:
        return False

    # The GitHub repo name is intentionally public-facing (`NarrowGateMaker`),
    # while the private research directory and C++ package historically used
    # `NarrowGate_BTCUSDC` / `narrowgate-btcusdc-cpp`.  Strict mode should prove
    # that the editable C++ extension points at this checkout, not that the repo
    # folder name matches the Python distribution name.
    dist_names = {
        f"{repo_root.name.lower().replace('_', '-')}-cpp",
        "narrowgate-btcusdc-cpp",
        "narrowgate-cpp",
    }
    repo_root = repo_root.resolve()
    for dist_name in dist_names:
        try:
            direct_url = importlib_metadata.distribution(dist_name).read_text("direct_url.json")
            if not direct_url:
                continue
            url = json.loads(direct_url).get("url", "")
            if not url:
                continue
            parsed = urlparse(url)
            raw_path = unquote(parsed.path if parsed.scheme == "file" else url)
            project_path = Path(raw_path).resolve()
        except Exception:
            continue
        if project_path == repo_root or project_path == (repo_root / "cpp"):
            return True
    return False


def _load_cpp_quote_core():
    global _CPP_QUOTE_CORE, _CPP_QUOTE_CORE_IMPORT_FAILED
    if _CPP_QUOTE_CORE is not None:
        return _CPP_QUOTE_CORE
    if _CPP_QUOTE_CORE_IMPORT_FAILED:
        return None
    try:
        import narrowgate_cpp  # type: ignore
    except Exception:
        _CPP_QUOTE_CORE_IMPORT_FAILED = True
        return None
    expected_token = os.environ.get("NARROWGATE_CPP_EXPECT_MODULE_TOKEN")
    strict = _cpp_strict_enabled()
    module_path = str(Path(getattr(narrowgate_cpp, "__file__", "")).resolve()).lower()
    repo_root = Path(__file__).resolve().parents[1]
    repo_token = repo_root.name.lower()
    token_ok = True
    if expected_token:
        token_ok = expected_token.lower() in module_path
    elif strict:
        token_ok = (
            repo_token in module_path
            or repo_token.replace("narrowgate_", "") in module_path
            or _cpp_module_points_to_repo(repo_root)
        )
    if not token_ok:
        msg = (
            "Imported narrowgate_cpp appears to belong to a different build: "
            f"{getattr(narrowgate_cpp, '__file__', '<unknown>')}. "
            "Set PYTHONPATH to the current repo build directory or set "
            "NARROWGATE_CPP_EXPECT_MODULE_TOKEN deliberately."
        )
        if strict or expected_token:
            raise RuntimeError(msg)
        _CPP_QUOTE_CORE_IMPORT_FAILED = True
        return None
    _CPP_QUOTE_CORE = narrowgate_cpp
    return _CPP_QUOTE_CORE


def _copy_attrs(
    src: Any,
    dst: Any,
    names: Sequence[str],
    *,
    required: Sequence[str] = (),
) -> Any:
    missing = [name for name in required if not hasattr(dst, name)]
    if missing:
        raise RuntimeError(
            "narrowgate_cpp QuoteCoreConfig ABI missing fields: "
            + ", ".join(missing)
        )
    for name in names:
        if hasattr(dst, name) and hasattr(src, name):
            setattr(dst, name, getattr(src, name))
    return dst


def _cached_cpp_config(cpp: Any, cfg: QuoteCoreConfig) -> Any:
    """Cache the immutable native config used by the scalar live path."""
    global _CPP_CFG_CACHE_KEY, _CPP_CFG_CACHE_REF, _CPP_CFG_CACHE_VALUE
    key = id(cfg)
    cached = _CPP_CFG_CACHE_REF() if _CPP_CFG_CACHE_REF is not None else None
    if key == _CPP_CFG_CACHE_KEY and cached is cfg and _CPP_CFG_CACHE_VALUE is not None:
        return _CPP_CFG_CACHE_VALUE
    value = _copy_attrs(
        cfg,
        cpp.QuoteCoreConfig(),
        _CPP_CFG_FIELDS,
        required=QUOTE_CORE_UNIT_ABI_FIELDS,
    )
    _CPP_CFG_CACHE_KEY = key
    _CPP_CFG_CACHE_REF = weakref.ref(cfg)
    _CPP_CFG_CACHE_VALUE = value
    return value


_CPP_CFG_FIELDS = (
    "gamma", "kappa", "tick_size", "lot_size",
    "maker_fee", "order_size",
    "max_inventory", "position_timeout_s", "quote_horizon_s",
    "pnl_volatility_horizon_s", "ml_enabled", "vol_blend",
    "dir_threshold", "gamma_dir_bonus", "skew_strength", "asym_strength",
    "ret_skew", "ret_shift_max_pct", "regime_enabled", "vol_baseline",
    "gamma_scale_min", "gamma_scale_max", "liq_baseline",
    "gamma_liq_scale_min", "gamma_liq_scale_max", "vol_power",
    "kappa_ratio", "p3_delta_star", "p3_kappa_eff", "use_bar_pricing",
    "use_depth_microprice", "use_depth_kappa", "microprice_levels",
    "kappa_levels", "kappa_depth_baseline", "depth_kappa_ratio",
    "ber_spread_mult", "markout_spread_scale", "markout_side_asymmetry_sign",
    "inventory_skew_strength",
    "inventory_asym_strength", "inventory_signal_fade_strength",
    "book_imb_strength", "book_imb_levels", "trace_book_imb_levels",
    "depth_tox_enabled", "depth_tox_levels", "depth_tox_imbalance_threshold",
    "depth_tox_microprice_shift_bps", "depth_tox_spread_mult",
    "dynamic_cap_enabled", "max_spread_bps", "dynamic_cap_base_bps",
    "dynamic_cap_alpha", "dynamic_cap_max_mult", "dynamic_cap_var_baseline",
    "dynamic_cap_liq_beta", "dynamic_cap_liq_baseline",
    "dynamic_cap_min_mult", "spread_cap_mode", "exit_urgency_strength", "urgency_time_weight",
    "urgency_pnl_weight", "urgency_signal_weight", "adverse_guard_enabled",
    "adverse_toxicity_threshold", "adverse_markout_threshold",
    "adverse_markout_pause_threshold", "adverse_markout_pause_hybrid",
    "adverse_dir_threshold",
    "adverse_ret_bps_threshold", "adverse_microprice_shift_bps",
    "adverse_spread_mult", "adverse_thin_depth_threshold",
    "adverse_thin_depth_mult", "adverse_pause", "defense_guard_enabled",
    "defense_markout_threshold", "defense_dir_threshold",
    "defense_ret_bps_threshold", "defense_microprice_shift_bps",
    "defense_spread_mult", "defense_pause",
    "defense_emergency_inventory_ratio", "defense_emergency_loss",
    "inventory_reference_qty", "eta_inventory", "a_spread",
    "f03_ret_action_horizon_s", "f03_ret_action_compatible",
    "risk_per_order", "execution_intensity_slope", "risk_horizon_s",
    "historical_p3_scalar_adapter_enabled", "p3_side_bbo_floor_enabled",
    "p3_identity_required", "p3_event_type", "p3_horizon_s",
    "p3_distance_origin", "p3_distance_unit", "p3_side",
    "p3_queue_included", "p3_artifact_sha256",
    "trade_intensity_acceleration_spread_mult",
)

_CPP_STATE_FIELDS = (
    "mid", "inventory", "sigma_sq", "trade_intensity", "best_bid",
    "best_ask", "ber_active", "mo_ema_all", "mo_ema_bid", "mo_ema_ask",
    "bid_adverse_markout_pause_latch", "ask_adverse_markout_pause_latch",
    "mo_ref", "position_open", "hold_time_s", "unrealized_pnl",
)

_CPP_PRED_FIELDS = ("dir_10s", "vol_10s", "ret_10s", "tox_bid", "tox_ask")

_DEFERRED_NATIVE_RESULT_FLOAT_FIELDS = (
    "ask_price",
    "asym",
    "bid_price",
    "delta_after_cap",
    "delta_after_regime",
    "delta_pre_cap",
    "delta_raw",
    "fair",
    "half_d",
    "kappa_before_depth",
    "kappa_used",
    "max_spread",
    "near_depth_total",
    "raw_half_spread",
    "raw_mid_shift",
    "raw_reservation_shift",
    "reservation_price",
    "sigma_sq_blended",
    "sigma_sq_raw",
    "spread",
)
_DEFERRED_NATIVE_SIDE_FLOAT_FIELDS = (
    "defense_spread_mult",
    "final_price",
    "final_quote_delta_to_bbo",
    "pre_guard_price",
    "spread_mult",
)
_DEFERRED_NATIVE_SIDE_BOOL_FIELDS = (
    "cap_exposure_block",
    "defense_emergency",
    "defense_guard",
    "defense_pause",
    "defense_reducing",
    "mid_guard",
    "post_only",
    "side_adverse",
    "side_adverse_pause",
)
_DEFERRED_NATIVE_FLAGS_BOOL_FIELDS = (
    "ask_adverse",
    "bid_adverse",
    "cap_exposure_block",
    "cap_hit",
    "defense_guard",
    "delta_cap",
    "final_compressed",
    "mid_guard",
    "post_only",
)


def _validate_deferred_native_quote_result_once(result: Any) -> None:
    """Prove the complete compact-result ABI before deferred routing begins.

    A loaded extension type is immutable for the process lifetime.  Touching
    every field consumed by the compact converter once preserves the old
    fail-before-routing ABI boundary without rebuilding Python dictionaries on
    every quote.
    """

    result_type = type(result)
    if result_type in _DEFERRED_NATIVE_QUOTE_RESULT_TYPES:
        return
    try:
        for name in _DEFERRED_NATIVE_RESULT_FLOAT_FIELDS:
            float(getattr(result, name))
        for side_name in ("buy", "sell"):
            side = getattr(result, side_name)
            for name in _DEFERRED_NATIVE_SIDE_FLOAT_FIELDS:
                float(getattr(side, name))
            for name in _DEFERRED_NATIVE_SIDE_BOOL_FIELDS:
                bool(getattr(side, name))
        flags = result.flags
        for name in _DEFERRED_NATIVE_FLAGS_BOOL_FIELDS:
            bool(getattr(flags, name))
    except (AttributeError, TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError(
            "narrowgate_cpp deferred quote result ABI is incomplete"
        ) from exc
    _DEFERRED_NATIVE_QUOTE_RESULT_TYPES.add(result_type)


def _to_cpp_depth(cpp: Any, depth: DepthSnapshot | None) -> Any:
    cpp_depth = cpp.DepthSnapshot()
    if depth is None:
        return cpp_depth

    def levels(rows: Sequence[Sequence[float]]) -> list[Any]:
        out = []
        for price, qty in rows:
            level = cpp.DepthLevel()
            level.price = float(price)
            level.qty = float(qty)
            out.append(level)
        return out

    cpp_depth.bids = levels(depth.bids)
    cpp_depth.asks = levels(depth.asks)
    return cpp_depth


def _cpp_side_context_to_dict(
    side: str,
    ctx: Any,
    *,
    common: dict[str, Any],
    tick: float,
) -> dict[str, Any]:
    final_guard_changed = abs(
        float(getattr(ctx, "final_price", 0.0)) - float(getattr(ctx, "pre_guard_price", 0.0))
    ) > tick * 0.5
    side_adverse = bool(getattr(ctx, "side_adverse", False))
    defense_guard = bool(getattr(ctx, "defense_guard", False))
    cap_exposure_block = bool(getattr(ctx, "cap_exposure_block", False))
    out = {
        **common,
        "adverse_toxicity": bool(getattr(ctx, "adverse_toxicity", False)),
        "adverse_markout": bool(getattr(ctx, "adverse_markout", False)),
        "adverse_direction": bool(getattr(ctx, "adverse_direction", False)),
        "adverse_ret": bool(getattr(ctx, "adverse_ret", False)),
        "adverse_microprice": bool(getattr(ctx, "adverse_microprice", False)),
        "adverse_thin_depth": bool(getattr(ctx, "adverse_thin_depth", False)),
        "side_adverse": side_adverse,
        "side_adverse_pause": bool(getattr(ctx, "side_adverse_pause", False)),
        "side_adverse_spread_mult": float(getattr(ctx, "spread_mult", 1.0)),
        "defense_guard": defense_guard,
        "defense_pause": bool(getattr(ctx, "defense_pause", False)),
        "defense_reducing": bool(getattr(ctx, "defense_reducing", False)),
        "defense_emergency": bool(getattr(ctx, "defense_emergency", False)),
        "defense_markout": bool(getattr(ctx, "defense_markout", False)),
        "defense_direction": bool(getattr(ctx, "defense_direction", False)),
        "defense_ret": bool(getattr(ctx, "defense_ret", False)),
        "defense_microprice": bool(getattr(ctx, "defense_microprice", False)),
        "defense_spread_mult": float(getattr(ctx, "defense_spread_mult", 1.0)),
        "raw_price": float(getattr(ctx, "raw_price", 0.0)),
        "pre_guard_price": float(getattr(ctx, "pre_guard_price", 0.0)),
        "final_price": float(getattr(ctx, "final_price", 0.0)),
        "raw_quote_delta_to_bbo": float(getattr(ctx, "raw_quote_delta_to_bbo", 0.0)),
        "pre_guard_delta_to_bbo": float(getattr(ctx, "pre_guard_delta_to_bbo", 0.0)),
        "final_quote_delta_to_bbo": float(getattr(ctx, "final_quote_delta_to_bbo", 0.0)),
        "raw_distance_to_mid": float(getattr(ctx, "raw_distance_to_mid", 0.0)),
        "final_distance_to_mid": float(getattr(ctx, "final_distance_to_mid", 0.0)),
        "favored_by_raw_shift": False,
        "mid_guard": bool(getattr(ctx, "mid_guard", False)),
        "post_only": bool(getattr(ctx, "post_only", False)),
        "cap_exposure_block": cap_exposure_block,
        "bid_adverse": side == "BUY" and side_adverse,
        "ask_adverse": side == "SELL" and side_adverse,
        "final_guard_changed": final_guard_changed,
        "any_constraint_changed": bool(
            common["delta_cap"] or common["final_compressed"] or final_guard_changed
            or cap_exposure_block or side_adverse or defense_guard
        ),
    }
    return out


def _call_cpp_quote_core(
    state: QuoteState,
    cfg: QuoteCoreConfig,
    pred: QuotePrediction | Any,
    depth: DepthSnapshot | None,
) -> tuple[Any, tuple[float, float, float, float, float]]:
    cpp = _load_cpp_quote_core()
    if cpp is None:
        raise RuntimeError("narrowgate_cpp is not available")

    pred_dir = _float(_get(pred, "dir_10s", 0.5), 0.5)
    pred_vol = _float(_get(pred, "vol_10s", 0.0), 0.0)
    pred_ret = _float(_get(pred, "ret_10s", 0.0), 0.0)
    pred_tox_bid = _float(_get(pred, "tox_bid", _get(pred, "tox_bid_10s", 0.5)), 0.5)
    pred_tox_ask = _float(_get(pred, "tox_ask", _get(pred, "tox_ask_10s", 0.5)), 0.5)
    cpp_cfg = _cached_cpp_config(cpp, cfg)
    if hasattr(cpp, "compute_quote_core_live"):
        result = cpp.compute_quote_core_live(
            tuple(getattr(state, name) for name in _CPP_STATE_FIELDS),
            cpp_cfg,
            (pred_dir, pred_vol, pred_ret, pred_tox_bid, pred_tox_ask),
            depth.bids if depth is not None else (),
            depth.asks if depth is not None else (),
        )
    else:
        cpp_state = _copy_attrs(state, cpp.QuoteState(), _CPP_STATE_FIELDS)
        cpp_pred = cpp.QuotePrediction()
        cpp_pred.dir_10s = pred_dir
        cpp_pred.vol_10s = pred_vol
        cpp_pred.ret_10s = pred_ret
        cpp_pred.tox_bid = pred_tox_bid
        cpp_pred.tox_ask = pred_tox_ask
        result = cpp.compute_quote_core(cpp_state, cpp_cfg, cpp_pred, _to_cpp_depth(cpp, depth))
    return result, (pred_dir, pred_vol, pred_ret, pred_tox_bid, pred_tox_ask)


def _compute_quote_core_cpp(
    state: QuoteState,
    cfg: QuoteCoreConfig,
    pred: QuotePrediction | Any,
    depth: DepthSnapshot | None,
    *,
    _native_result: Any | None = None,
    _native_pred_values: tuple[float, float, float, float, float] | None = None,
) -> QuoteCoreResult:
    if _native_result is None or _native_pred_values is None:
        result, pred_values = _call_cpp_quote_core(state, cfg, pred, depth)
    else:
        result, pred_values = _native_result, _native_pred_values
    pred_dir, pred_vol, pred_ret, pred_tox_bid, pred_tox_ask = pred_values
    tick = max(float(cfg.tick_size), 1e-12)
    quote_horizon_s = max(float(cfg.quote_horizon_s), 1e-6)
    risk_horizon_s = max(float(cfg.risk_horizon_s), 1e-6)
    sigma_sq_horizon = float(result.sigma_sq_blended) * risk_horizon_s
    inventory_reference_qty = float(cfg.inventory_reference_qty)
    inventory_units = float(state.inventory) / inventory_reference_qty
    p3_pair_floor = (
        2.0 * float(cfg.p3_delta_star)
        if cfg.regime_enabled
        and cfg.historical_p3_scalar_adapter_enabled
        and cfg.p3_delta_star > 0.0
        else 0.0
    )
    p3_floor_mode = (
        "legacy_pair_projection_from_same_side_bbo"
        if p3_pair_floor > 0.0 and cfg.p3_event_type
        else "legacy_naked_pair_floor"
        if p3_pair_floor > 0.0
        else "same_side_bbo_floor"
        if cfg.p3_side_bbo_floor_enabled and cfg.p3_delta_star > 0.0
        else "inactive"
    )
    p3_buy_floor_price = (
        _floor_tick(float(state.best_bid) - float(cfg.p3_delta_star), tick)
        if cfg.p3_side_bbo_floor_enabled
        and cfg.p3_delta_star > 0.0
        and state.best_bid > 0.0
        else 0.0
    )
    p3_sell_floor_price = (
        _ceil_tick(float(state.best_ask) + float(cfg.p3_delta_star), tick)
        if cfg.p3_side_bbo_floor_enabled
        and cfg.p3_delta_star > 0.0
        and state.best_ask > 0.0
        else 0.0
    )
    quote_flags = {
        "final_compressed": bool(result.flags.final_compressed),
        "delta_cap": bool(result.flags.delta_cap),
        "mid_guard": bool(result.flags.mid_guard),
        "post_only": bool(result.flags.post_only),
        "bid_adverse": bool(result.flags.bid_adverse),
        "ask_adverse": bool(result.flags.ask_adverse),
        "side_adverse": bool(result.flags.bid_adverse or result.flags.ask_adverse),
        "defense_guard": bool(result.flags.defense_guard),
        "cap_exposure_block": bool(result.flags.cap_exposure_block),
    }
    final_pair_spread = max(float(result.spread), tick)
    common = {
        "raw_half_spread": float(result.raw_half_spread),
        "capped_half_spread": float(result.capped_half_spread),
        "raw_mid_shift": float(result.raw_mid_shift),
        "raw_reservation_shift": float(result.raw_reservation_shift),
        "raw_asym_shift": float(result.raw_asym_shift),
        "asym": float(result.asym),
        "inventory": float(state.inventory),
        "inventory_reference_qty": inventory_reference_qty,
        "inventory_units": inventory_units,
        "order_size": float(cfg.order_size),
        "order_units": float(cfg.order_size) / inventory_reference_qty,
        "dir_signal": pred_dir - 0.5 if cfg.ml_enabled else 0.0,
        "pred_dir": pred_dir,
        "pred_ret": pred_ret,
        "tox_bid": pred_tox_bid,
        "tox_ask": pred_tox_ask,
        "book_imb": float(result.book_imb),
        "weighted_mid_proxy_shift_bps": float(result.microprice_shift_bps),
        "microprice_shift_bps": float(result.microprice_shift_bps),
        "near_depth_total": float(result.near_depth_total),
        "mo_ema_bid": float(state.mo_ema_bid),
        "mo_ema_ask": float(state.mo_ema_ask),
        "fair": float(result.fair),
        "mid": float(state.mid),
        "best_bid": float(state.best_bid),
        "best_ask": float(state.best_ask),
        "raw_pair_spread": max(float(result.sell.raw_price - result.buy.raw_price), tick),
        "capped_pair_spread": max(float(result.sell.pre_guard_price - result.buy.pre_guard_price), tick),
        "final_pair_spread": final_pair_spread,
        "raw_quote_skew": float(result.raw_quote_skew),
        "final_quote_skew": float(result.buy.final_quote_skew),
        "raw_bias_side": "BUY" if result.raw_mid_shift > tick else "SELL" if result.raw_mid_shift < -tick else "balanced",
        "final_bias_side": "balanced",
        "delta_cap": quote_flags["delta_cap"],
        "final_compressed": quote_flags["final_compressed"],
        "cap_exposure_block": quote_flags["cap_exposure_block"],
        "side_adverse_enabled": bool(cfg.adverse_guard_enabled),
        "adverse_toxicity": False,
        "adverse_markout": False,
        "adverse_direction": False,
        "adverse_ret": False,
        "adverse_microprice": False,
        "adverse_thin_depth": False,
        "side_adverse": False,
        "side_adverse_pause": False,
        "side_adverse_spread_mult": 1.0,
        "defense_guard_enabled": bool(cfg.defense_guard_enabled),
        "defense_guard": False,
        "defense_pause": False,
        "defense_reducing": False,
        "defense_emergency": False,
        "defense_markout": False,
        "defense_direction": False,
        "defense_ret": False,
        "defense_microprice": False,
        "defense_spread_mult": 1.0,
        "bid_adverse": quote_flags["bid_adverse"],
        "ask_adverse": quote_flags["ask_adverse"],
        "p3_floor_mode": p3_floor_mode,
        "p3_touch_delta_star": float(cfg.p3_delta_star),
        "p3_pair_floor": p3_pair_floor,
        "p3_distance_origin": str(cfg.p3_distance_origin),
        "p3_buy_floor_price": p3_buy_floor_price,
        "p3_sell_floor_price": p3_sell_floor_price,
        "historical_p3_scalar_adapter_enabled": bool(
            cfg.historical_p3_scalar_adapter_enabled
        ),
        "p3_side_bbo_floor_enabled": bool(cfg.p3_side_bbo_floor_enabled),
    }
    quote_context = {
        "BUY": _cpp_side_context_to_dict("BUY", result.buy, common=common, tick=tick),
        "SELL": _cpp_side_context_to_dict("SELL", result.sell, common=common, tick=tick),
    }
    diagnostics = {
        "fair": float(result.fair),
        "reservation_price": float(result.reservation_price),
        "sigma_sq_raw": float(result.sigma_sq_raw),
        "sigma_sq_blended": float(result.sigma_sq_blended),
        "quote_horizon_s": quote_horizon_s,
        "risk_horizon_s": risk_horizon_s,
        "sigma_sq_horizon": sigma_sq_horizon,
        "inventory_reference_qty": inventory_reference_qty,
        "inventory_units": inventory_units,
        "order_size": float(cfg.order_size),
        "order_units": float(cfg.order_size) / inventory_reference_qty,
        "eta_inventory": float(cfg.eta_inventory),
        "a_spread": float(cfg.a_spread),
        "risk_per_order": float(cfg.risk_per_order),
        "execution_intensity_slope": float(cfg.execution_intensity_slope),
        "distance_decay_source": (
            "legacy_p3_touch_slope_projection"
            if cfg.historical_p3_scalar_adapter_enabled
            and cfg.p3_kappa_eff > 0.0
            else "execution_intensity_slope"
        ),
        "p3_floor_mode": p3_floor_mode,
        "p3_touch_delta_star": float(cfg.p3_delta_star),
        "p3_pair_floor": p3_pair_floor,
        "p3_buy_floor_price": p3_buy_floor_price,
        "p3_sell_floor_price": p3_sell_floor_price,
        "historical_p3_scalar_adapter_enabled": bool(
            cfg.historical_p3_scalar_adapter_enabled
        ),
        "p3_side_bbo_floor_enabled": bool(cfg.p3_side_bbo_floor_enabled),
        "p3_event_type": str(cfg.p3_event_type),
        "p3_horizon_s": float(cfg.p3_horizon_s),
        "p3_distance_origin": str(cfg.p3_distance_origin),
        "p3_distance_unit": str(cfg.p3_distance_unit),
        "p3_side": str(cfg.p3_side),
        "p3_queue_included": cfg.p3_queue_included,
        "p3_artifact_sha256": str(cfg.p3_artifact_sha256),
        "f03_ret_action_horizon_s": float(cfg.f03_ret_action_horizon_s),
        "f03_ret_action_compatible": bool(cfg.f03_ret_action_compatible),
        "delta_raw": float(result.delta_raw),
        "delta_after_regime": float(result.delta_after_regime),
        "delta_pre_cap": float(result.delta_pre_cap),
        "delta_after_cap": float(result.delta_after_cap),
        "capped_pair_spread": common["capped_pair_spread"],
        "cap_hit": bool(result.flags.cap_hit),
        "delta_cap_hit": bool(result.flags.delta_cap),
        "cap_reason": "delta" if result.flags.delta_cap else "none",
        "spread_cap_mode": spread_cap_mode_name(cfg.spread_cap_mode),
        "cap_exposure_block": quote_flags["cap_exposure_block"],
        "cap_bps": float(result.cap_bps),
        "max_spread": float(result.max_spread),
        "half_d": float(result.half_d),
        "asym": float(result.asym),
        "r_shift": float(result.raw_reservation_shift),
        "rs_clamp": 0.0,
        "dir_signal": common["dir_signal"],
        "kappa_before_depth": float(result.kappa_before_depth),
        "kappa_used": float(result.kappa_used),
        "depth_tox_mult": float(result.depth_tox_mult),
        "bid_adverse_active": quote_flags["bid_adverse"],
        "bid_adverse_toxicity_active": bool(result.buy.adverse_toxicity),
        "bid_adverse_markout_active": bool(result.buy.adverse_markout),
        "bid_adverse_pause_active": bool(result.buy.side_adverse_pause),
        "ask_adverse_active": quote_flags["ask_adverse"],
        "ask_adverse_pause_active": bool(result.sell.side_adverse_pause),
        "bid_defense_active": bool(result.buy.defense_guard),
        "bid_defense_pause_active": bool(result.buy.defense_pause),
        "bid_defense_reducing": bool(result.buy.defense_reducing),
        "bid_defense_emergency": bool(result.buy.defense_emergency),
        "bid_defense_markout_active": bool(result.buy.defense_markout),
        "bid_defense_direction_active": bool(result.buy.defense_direction),
        "bid_defense_ret_active": bool(result.buy.defense_ret),
        "bid_defense_microprice_active": bool(result.buy.defense_microprice),
        "ask_defense_active": bool(result.sell.defense_guard),
        "ask_defense_pause_active": bool(result.sell.defense_pause),
        "ask_defense_reducing": bool(result.sell.defense_reducing),
        "ask_defense_emergency": bool(result.sell.defense_emergency),
        "ask_defense_markout_active": bool(result.sell.defense_markout),
        "ask_defense_direction_active": bool(result.sell.defense_direction),
        "ask_defense_ret_active": bool(result.sell.defense_ret),
        "ask_defense_microprice_active": bool(result.sell.defense_microprice),
        "adverse_guard_enabled": bool(cfg.adverse_guard_enabled),
        "bid_adverse_direction_active": bool(result.buy.adverse_direction),
        "bid_adverse_ret_active": bool(result.buy.adverse_ret),
        "bid_adverse_microprice_active": bool(result.buy.adverse_microprice),
        "bid_adverse_thin_depth_active": bool(result.buy.adverse_thin_depth),
        "ask_adverse_toxicity_active": bool(result.sell.adverse_toxicity),
        "ask_adverse_markout_active": bool(result.sell.adverse_markout),
        "ask_adverse_direction_active": bool(result.sell.adverse_direction),
        "ask_adverse_ret_active": bool(result.sell.adverse_ret),
        "ask_adverse_microprice_active": bool(result.sell.adverse_microprice),
        "ask_adverse_thin_depth_active": bool(result.sell.adverse_thin_depth),
        "mid_guard_bid": bool(result.mid_guard_bid),
        "mid_guard_ask": bool(result.mid_guard_ask),
        "post_only_bid": bool(result.post_only_bid),
        "post_only_ask": bool(result.post_only_ask),
        "final_compressed": quote_flags["final_compressed"],
        "final_cap_excess": float(result.final_cap_excess),
        "final_cap_rounding": bool(result.final_cap_rounding),
        "final_cap_mid_guard": bool(result.final_cap_mid_guard),
        "final_cap_post_only": bool(result.final_cap_post_only),
        "final_cap_delta": bool(result.final_cap_delta),
        "pre_guard_bid": float(result.buy.pre_guard_price),
        "pre_guard_ask": float(result.sell.pre_guard_price),
        "raw_bid_px": float(result.buy.raw_price),
        "raw_ask_px": float(result.sell.raw_price),
    }
    final_quote_delta = {
        "BUY": quote_context["BUY"]["final_quote_delta_to_bbo"],
        "SELL": quote_context["SELL"]["final_quote_delta_to_bbo"],
    }
    return QuoteCoreResult(
        bid_price=float(result.bid_price),
        ask_price=float(result.ask_price),
        spread=float(result.spread),
        raw_half_spread=float(result.raw_half_spread),
        raw_mid_shift=float(result.raw_mid_shift),
        final_quote_delta=final_quote_delta,
        quote_context=quote_context,
        quote_flags=quote_flags,
        diagnostics=diagnostics,
    )


def _compute_quote_core_cpp_compact(
    state: QuoteState,
    cfg: QuoteCoreConfig,
    pred: QuotePrediction | Any,
    depth: DepthSnapshot | None,
    *,
    _native_result: Any | None = None,
    _native_pred_values: tuple[float, float, float, float, float] | None = None,
) -> QuoteCoreResult:
    """Build only the context consumed by the per-tick maker policy.

    Full quote-EV feature dictionaries remain available through
    `_compute_quote_core_cpp`; this compact path avoids recreating roughly one
    hundred Python dict entries when the live loop only needs guard flags,
    quote distances, and a small diagnostics set.
    """
    if _native_result is None or _native_pred_values is None:
        result, pred_values = _call_cpp_quote_core(state, cfg, pred, depth)
    else:
        result, pred_values = _native_result, _native_pred_values
    pred_dir, _pred_vol, _pred_ret, _pred_tox_bid, _pred_tox_ask = pred_values
    tick = max(float(cfg.tick_size), 1e-12)
    quote_horizon_s = max(float(cfg.quote_horizon_s), 1e-6)
    risk_horizon_s = max(float(cfg.risk_horizon_s), 1e-6)
    sigma_sq_horizon = float(result.sigma_sq_blended) * risk_horizon_s
    inventory_reference_qty = float(cfg.inventory_reference_qty)
    inventory_units = float(state.inventory) / inventory_reference_qty
    p3_pair_floor = (
        2.0 * float(cfg.p3_delta_star)
        if cfg.regime_enabled
        and cfg.historical_p3_scalar_adapter_enabled
        and cfg.p3_delta_star > 0.0
        else 0.0
    )
    delta_cap = bool(result.flags.delta_cap)
    final_compressed = bool(result.flags.final_compressed)
    cap_exposure_block = bool(result.flags.cap_exposure_block)

    def side_context(side: str, ctx: Any) -> dict[str, Any]:
        side_adverse = bool(ctx.side_adverse)
        defense_guard = bool(ctx.defense_guard)
        return {
            "near_depth_total": float(result.near_depth_total),
            "final_quote_delta_to_bbo": float(ctx.final_quote_delta_to_bbo),
            "side_adverse": side_adverse,
            "side_adverse_pause": bool(ctx.side_adverse_pause),
            "side_adverse_spread_mult": float(ctx.spread_mult),
            "defense_guard": defense_guard,
            "defense_pause": bool(ctx.defense_pause),
            "defense_reducing": bool(ctx.defense_reducing),
            "defense_emergency": bool(ctx.defense_emergency),
            "defense_spread_mult": float(ctx.defense_spread_mult),
            "mid_guard": bool(ctx.mid_guard),
            "post_only": bool(ctx.post_only),
            "cap_exposure_block": bool(ctx.cap_exposure_block),
            "bid_adverse": side == "BUY" and side_adverse,
            "ask_adverse": side == "SELL" and side_adverse,
            "order_ttl_ms": 0,
            "local_extreme_guard": False,
            "local_extreme_pause": False,
            "local_extreme_spread_mult": 1.0,
            "delta_cap": delta_cap,
            "final_compressed": final_compressed,
            "any_constraint_changed": bool(
                delta_cap or final_compressed or bool(ctx.cap_exposure_block)
                or side_adverse or defense_guard
                or abs(float(ctx.final_price) - float(ctx.pre_guard_price)) > tick * 0.5
            ),
        }

    quote_context = {
        "BUY": side_context("BUY", result.buy),
        "SELL": side_context("SELL", result.sell),
    }
    diagnostics = {
        "fair": float(result.fair),
        "reservation_price": float(result.reservation_price),
        "sigma_sq_raw": float(result.sigma_sq_raw),
        "sigma_sq_blended": float(result.sigma_sq_blended),
        "quote_horizon_s": quote_horizon_s,
        "risk_horizon_s": risk_horizon_s,
        "sigma_sq_horizon": sigma_sq_horizon,
        "inventory_reference_qty": inventory_reference_qty,
        "inventory_units": inventory_units,
        "order_size": float(cfg.order_size),
        "order_units": float(cfg.order_size) / inventory_reference_qty,
        "eta_inventory": float(cfg.eta_inventory),
        "a_spread": float(cfg.a_spread),
        "risk_per_order": float(cfg.risk_per_order),
        "execution_intensity_slope": float(cfg.execution_intensity_slope),
        "p3_floor_mode": (
            "legacy_pair_projection_from_same_side_bbo"
            if p3_pair_floor > 0.0 and cfg.p3_event_type
            else "legacy_naked_pair_floor"
            if p3_pair_floor > 0.0
            else "same_side_bbo_floor"
            if cfg.p3_side_bbo_floor_enabled and cfg.p3_delta_star > 0.0
            else "inactive"
        ),
        "p3_touch_delta_star": float(cfg.p3_delta_star),
        "p3_pair_floor": p3_pair_floor,
        "historical_p3_scalar_adapter_enabled": bool(
            cfg.historical_p3_scalar_adapter_enabled
        ),
        "p3_side_bbo_floor_enabled": bool(cfg.p3_side_bbo_floor_enabled),
        "p3_event_type": str(cfg.p3_event_type),
        "p3_horizon_s": float(cfg.p3_horizon_s),
        "p3_distance_origin": str(cfg.p3_distance_origin),
        "p3_distance_unit": str(cfg.p3_distance_unit),
        "p3_side": str(cfg.p3_side),
        "p3_queue_included": cfg.p3_queue_included,
        "p3_artifact_sha256": str(cfg.p3_artifact_sha256),
        "f03_ret_action_horizon_s": float(cfg.f03_ret_action_horizon_s),
        "f03_ret_action_compatible": bool(cfg.f03_ret_action_compatible),
        "delta_raw": float(result.delta_raw),
        "delta_after_regime": float(result.delta_after_regime),
        "delta_pre_cap": float(result.delta_pre_cap),
        "delta_after_cap": float(result.delta_after_cap),
        "cap_hit": bool(result.flags.cap_hit),
        "cap_reason": "delta" if delta_cap else "none",
        "spread_cap_mode": spread_cap_mode_name(cfg.spread_cap_mode),
        "cap_exposure_block": cap_exposure_block,
        "max_spread": float(result.max_spread),
        "half_d": float(result.half_d),
        "asym": float(result.asym),
        "r_shift": float(result.raw_reservation_shift),
        "rs_clamp": 0.0,
        "dir_signal": pred_dir - 0.5 if cfg.ml_enabled else 0.0,
        "kappa_before_depth": float(result.kappa_before_depth),
        "kappa_used": float(result.kappa_used),
    }
    return QuoteCoreResult(
        bid_price=float(result.bid_price),
        ask_price=float(result.ask_price),
        spread=float(result.spread),
        raw_half_spread=float(result.raw_half_spread),
        raw_mid_shift=float(result.raw_mid_shift),
        final_quote_delta={
            "BUY": quote_context["BUY"]["final_quote_delta_to_bbo"],
            "SELL": quote_context["SELL"]["final_quote_delta_to_bbo"],
        },
        quote_context=quote_context,
        quote_flags={
            "final_compressed": final_compressed,
            "delta_cap": delta_cap,
            "mid_guard": bool(result.flags.mid_guard),
            "post_only": bool(result.flags.post_only),
            "bid_adverse": bool(result.flags.bid_adverse),
            "ask_adverse": bool(result.flags.ask_adverse),
            "side_adverse": bool(result.flags.bid_adverse or result.flags.ask_adverse),
            "defense_guard": bool(result.flags.defense_guard),
            "cap_exposure_block": cap_exposure_block,
        },
        diagnostics=diagnostics,
    )


_CPP_COMMON_POLICY_FIELDS = (
    "exposure_increasing", "fill_cooldown_active", "side_adverse",
    "side_adverse_pause", "local_extreme_guard", "local_extreme_pause",
    "defense_guard", "defense_pause", "inventory_ratio", "depth_age_s",
    "max_book_age_s", "toxicity", "markout_ema", "markout_spread_scale",
    "markout_reference", "microprice_shift_bps", "l2_quote_flip_rate",
    "l2_book_cancel_ratio", "l2_near_depth_total", "thin_depth_threshold",
    "kappa_depth_baseline", "local_extreme_spread_mult",
    "defense_spread_mult",
)


def make_native_quote_policy_stage(cfg: QuoteCoreConfig) -> Any:
    """Freeze the explicitly selected native quote/common-policy stage."""
    cpp = _load_cpp_quote_core()
    if cpp is None or not bool(
        getattr(cpp, "NATIVE_QUOTE_POLICY_STAGE_AVAILABLE", False)
    ):
        raise RuntimeError("native quote-policy stage is unavailable")
    return cpp.NativeQuotePolicyStage(_cached_cpp_config(cpp, cfg))


def compute_native_quote_policy_stage_live(
    stage: Any,
    state: QuoteState,
    cfg: QuoteCoreConfig,
    pred: QuotePrediction | Any,
    depth: DepthSnapshot | None,
    buy_policy: Any,
    sell_policy: Any,
    *,
    require_full_context: bool = False,
) -> tuple[DeferredNativeQuoteCoreResult | QuoteCoreResult, Any, Any]:
    """Cross the native boundary once; never compute a Python reference."""
    cpp = _load_cpp_quote_core()
    if cpp is None:
        raise RuntimeError("narrowgate_cpp is not available")
    pred_values = tuple(
        _float(_get(pred, name, 0.5 if "tox" in name or name == "dir_10s" else 0.0))
        for name in _CPP_PRED_FIELDS
    )

    def policy_values(source: Any) -> tuple[Any, ...]:
        return tuple(getattr(source, name) for name in _CPP_COMMON_POLICY_FIELDS)

    native = stage.compute(
        tuple(getattr(state, name) for name in _CPP_STATE_FIELDS),
        pred_values,
        depth.bids if depth is not None else (),
        depth.asks if depth is not None else (),
        policy_values(buy_policy),
        policy_values(sell_policy),
    )
    if _cpp_strict_enabled() and not require_full_context:
        _validate_deferred_native_quote_result_once(native.quote)
        quote = DeferredNativeQuoteCoreResult(
            native_result=native.quote,
            state=state,
            cfg=cfg,
            pred_values=pred_values,
        )
    else:
        converter = (
            _compute_quote_core_cpp
            if require_full_context
            else _compute_quote_core_cpp_compact
        )
        quote = converter(
            state,
            cfg,
            pred,
            depth,
            _native_result=native.quote,
            _native_pred_values=pred_values,
        )
    return quote, native.buy_policy, native.sell_policy


def compute_quote_core_batch_depth_cpp(
    *,
    mid: Any,
    inventory: Any,
    sigma_sq: Any,
    trade_intensity: Any,
    best_bid: Any,
    best_ask: Any,
    dir_10s: Any,
    vol_10s: Any,
    ret_10s: Any,
    tox_bid: Any,
    tox_ask: Any,
    cfg: QuoteCoreConfig,
    mo_ema_bid: Any | None = None,
    mo_ema_ask: Any | None = None,
    mo_ema_all: Any | None = None,
    mo_ref: Any | None = None,
    ber_active: Any | None = None,
    position_open: Any | None = None,
    hold_time_s: Any | None = None,
    unrealized_pnl: Any | None = None,
    l2_bid_px: Any | None = None,
    l2_bid_qty: Any | None = None,
    l2_ask_px: Any | None = None,
    l2_ask_qty: Any | None = None,
    strict: bool = False,
    workers: int = 1,
) -> dict[str, np.ndarray]:
    """Compute quote-core context for aligned arrays using C++ and L2 depth rows.

    This is an offline/research batch helper.  It intentionally raises when
    the extension is unavailable so callers do not silently mix Python trace
    context with C++ refreshed context.
    """

    cpp = _load_cpp_quote_core()
    if cpp is None:
        raise RuntimeError("narrowgate_cpp is not available")
    if not hasattr(cpp, "compute_quote_core_batch_depth"):
        raise RuntimeError("narrowgate_cpp.compute_quote_core_batch_depth is not available")

    def arr(values: Any, name: str, default: float | None = None) -> np.ndarray:
        if values is None:
            if default is None:
                raise ValueError(f"{name} is required")
            out = np.full(n, float(default), dtype=np.float64)
        else:
            out = np.asarray(values, dtype=np.float64).reshape(-1)
        if out.shape[0] != n:
            raise ValueError(f"{name} length mismatch: {out.shape[0]} != {n}")
        return np.ascontiguousarray(np.nan_to_num(out, nan=0.0, posinf=0.0, neginf=0.0), dtype=np.float64)

    def mat(values: Any, name: str) -> np.ndarray:
        if values is None:
            return np.zeros((n, 0), dtype=np.float64)
        out = np.asarray(values, dtype=np.float64)
        if out.ndim == 1:
            out = out.reshape(-1, 1)
        if out.ndim != 2:
            raise ValueError(f"{name} must be a 2D array")
        if out.shape[0] != n:
            raise ValueError(f"{name} row mismatch: {out.shape[0]} != {n}")
        return np.ascontiguousarray(np.nan_to_num(out, nan=0.0, posinf=0.0, neginf=0.0), dtype=np.float64)

    mid_arr = np.asarray(mid, dtype=np.float64).reshape(-1)
    n = int(mid_arr.shape[0])
    mid_arr = np.ascontiguousarray(np.nan_to_num(mid_arr, nan=0.0, posinf=0.0, neginf=0.0), dtype=np.float64)
    cpp_cfg = _copy_attrs(
        cfg,
        cpp.QuoteCoreConfig(),
        _CPP_CFG_FIELDS,
        required=QUOTE_CORE_UNIT_ABI_FIELDS,
    )

    try:
        out = cpp.compute_quote_core_batch_depth(
            mid_arr,
            arr(inventory, "inventory"),
            arr(sigma_sq, "sigma_sq"),
            arr(trade_intensity, "trade_intensity"),
            arr(best_bid, "best_bid"),
            arr(best_ask, "best_ask"),
            arr(dir_10s, "dir_10s", 0.5),
            arr(vol_10s, "vol_10s"),
            arr(ret_10s, "ret_10s"),
            arr(tox_bid, "tox_bid", 0.5),
            arr(tox_ask, "tox_ask", 0.5),
            arr(mo_ema_bid, "mo_ema_bid", 0.0),
            arr(mo_ema_ask, "mo_ema_ask", 0.0),
            arr(mo_ema_all, "mo_ema_all", 0.0),
            arr(mo_ref, "mo_ref", 50.0),
            arr(ber_active, "ber_active", 0.0),
            arr(position_open, "position_open", 0.0),
            arr(hold_time_s, "hold_time_s", 0.0),
            arr(unrealized_pnl, "unrealized_pnl", 0.0),
            mat(l2_bid_px, "l2_bid_px"),
            mat(l2_bid_qty, "l2_bid_qty"),
            mat(l2_ask_px, "l2_ask_px"),
            mat(l2_ask_qty, "l2_ask_qty"),
            cpp_cfg,
            max(1, int(workers)),
        )
    except Exception:
        if strict or _cpp_strict_enabled():
            raise
        raise

    return {str(key): np.asarray(value) for key, value in dict(out).items()}


def compute_quote_core(
    state: QuoteState,
    cfg: QuoteCoreConfig,
    pred: QuotePrediction | Any,
    depth: DepthSnapshot | None = None,
) -> QuoteCoreResult:
    """Compute quote core, optionally delegated to the C++ extension.

    The default path remains the exact Python implementation.  Set
    `NARROWGATE_CPP_QUOTE_CORE=1` when running targeted benchmarks or controlled
    experiments that should use the pybind11 engine.
    """
    if _cpp_quote_core_enabled(cfg):
        try:
            return _compute_quote_core_cpp(state, cfg, pred, depth)
        except Exception:
            if os.environ.get("NARROWGATE_CPP_STRICT", "").strip().lower() in {"1", "true", "yes", "on"}:
                raise
    return _compute_quote_core_py(state, cfg, pred, depth)


def compute_quote_core_live(
    state: QuoteState,
    cfg: QuoteCoreConfig,
    pred: QuotePrediction | Any,
    depth: DepthSnapshot | None = None,
    *,
    require_full_context: bool = False,
) -> QuoteCoreResult:
    """Live wrapper with compact C++ context unless a model needs all fields."""
    if _cpp_quote_core_enabled(cfg) and not require_full_context:
        try:
            return _compute_quote_core_cpp_compact(state, cfg, pred, depth)
        except Exception:
            if _cpp_strict_enabled():
                raise
    return compute_quote_core(state, cfg, pred, depth)


def compute_quote_core_live_deferred(
    state: QuoteState,
    cfg: QuoteCoreConfig,
    pred: QuotePrediction | Any,
    depth: DepthSnapshot | None = None,
    *,
    require_full_context: bool = False,
) -> DeferredNativeQuoteCoreResult | QuoteCoreResult:
    """Live-only native call that defers legacy Python mapping construction.

    Public callers continue to use :func:`compute_quote_core_live`, which keeps
    returning an eagerly materialized ``QuoteCoreResult``.  The deferred form
    is useful only to a live owner that can consume the native POD directly and
    retain it until evidence or logging requests the legacy mappings.
    """

    # Validate the complete compact ABI once per native result type before the
    # deferred object can reach order routing.  Use this only for the fail-fast
    # native profile; non-strict mode retains the eager Python-fallback boundary.
    if (
        _cpp_quote_core_enabled(cfg)
        and _cpp_strict_enabled()
        and not require_full_context
    ):
        result, pred_values = _call_cpp_quote_core(state, cfg, pred, depth)
        _validate_deferred_native_quote_result_once(result)
        return DeferredNativeQuoteCoreResult(
            native_result=result,
            state=state,
            cfg=cfg,
            pred_values=pred_values,
        )
    return compute_quote_core_live(
        state,
        cfg,
        pred,
        depth,
        require_full_context=require_full_context,
    )
