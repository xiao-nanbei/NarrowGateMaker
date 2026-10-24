"""State-dependent quote response after exposure-increasing maker fills.

The policy decomposes inventory control from persistent-flow defense:

    center = base_center - z * (I + A / 2)
    half_spread = base_half_spread + A / 2

where ``z`` is the inventory sign. ``I`` shifts the whole pair without
changing pair spread. ``A`` moves only the exposure-increasing quote farther
from the market; the reducing quote is invariant to ``A``.

This module is intentionally policy-only. Order cancellation, replacement,
latency, queue loss, and replay traces remain owned by the replay engine.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

NOOP_MODE = "noop"
INVENTORY_SHIFT_MODE = "inventory_shift"
FLOW_ADD_WIDEN_MODE = "flow_add_widen"
HYBRID_MODE = "hybrid"
LEGACY_FLOW_AMPLITUDE_MODE = "excitation_ticks"
EXPECTED_ADVERSE_FLOW_AMPLITUDE_MODE = "expected_adverse"
VALID_MODES = {
    NOOP_MODE,
    INVENTORY_SHIFT_MODE,
    FLOW_ADD_WIDEN_MODE,
    HYBRID_MODE,
}


def _clip(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def _nearest_nonnegative_int(value: float) -> int:
    return max(0, int(math.floor(max(0.0, value) + 0.5)))


def _floor_tick(price: float, tick_size: float) -> float:
    return math.floor((price + tick_size * 1e-9) / tick_size) * tick_size


def _ceil_tick(price: float, tick_size: float) -> float:
    return math.ceil((price - tick_size * 1e-9) / tick_size) * tick_size


@dataclass(frozen=True)
class PostFillQuoteResponseConfig:
    enabled: bool = False
    mode: str = NOOP_MODE

    # I_t: pair-center inventory shift. The first order unit is deliberately
    # allowed to round to zero in the small research arm.
    inventory_ticks_per_order_unit: float = 0.25
    inventory_max_ticks: float = 4.0

    # A_t: Hawkes/response-kernel excitation after add-side fills.
    flow_ticks_per_excitation: float = 2.0
    flow_max_ticks: float = 8.0
    flow_excitation_per_order_unit: float = 1.0
    flow_max_excitation: float = 4.0
    flow_amplitude_mode: str = LEGACY_FLOW_AMPLITUDE_MODE
    flow_expected_adverse_buy_ticks: float = 0.0
    flow_expected_adverse_sell_ticks: float = 0.0
    flow_add_distance_fraction_buy: float = 1.0
    flow_add_distance_fraction_sell: float = 1.0
    response_half_life_s: float = 20.0
    response_half_life_min_s: float = 4.0
    response_half_life_max_s: float = 120.0

    # State-dependent half-life multipliers. Higher volatility and lower repair
    # probability lengthen defense; positive refill edge shortens it.
    volatility_ref_bps: float = 3.0
    volatility_weight: float = 0.35
    refill_edge_ref: float = 0.10
    refill_weight: float = 0.75
    repair_probability_anchor: float = 0.60
    repair_probability_weight: float = 1.00

    @classmethod
    def from_params(cls, params: dict[str, Any]) -> PostFillQuoteResponseConfig:
        enabled = bool(params.get("post_fill_quote_response_enabled", False))
        mode = str(
            params.get("post_fill_quote_response_mode", NOOP_MODE) or NOOP_MODE
        ).lower()
        if not enabled:
            mode = NOOP_MODE
        if mode not in VALID_MODES:
            raise ValueError(
                f"unsupported post_fill_quote_response_mode={mode!r}; "
                f"expected one of {sorted(VALID_MODES)}"
            )
        amplitude_mode = str(
            params.get(
                "post_fill_flow_amplitude_mode", LEGACY_FLOW_AMPLITUDE_MODE
            )
            or LEGACY_FLOW_AMPLITUDE_MODE
        ).lower()
        if amplitude_mode not in {
            LEGACY_FLOW_AMPLITUDE_MODE,
            EXPECTED_ADVERSE_FLOW_AMPLITUDE_MODE,
        }:
            raise ValueError(
                f"unsupported post_fill_flow_amplitude_mode={amplitude_mode!r}"
            )
        half_life_min = max(
            1e-3,
            float(params.get("post_fill_response_half_life_min_s", 4.0) or 4.0),
        )
        half_life_max = max(
            half_life_min,
            float(params.get("post_fill_response_half_life_max_s", 120.0) or 120.0),
        )
        return cls(
            enabled=enabled,
            mode=mode,
            inventory_ticks_per_order_unit=max(
                0.0,
                float(
                    params.get(
                        "post_fill_inventory_ticks_per_order_unit", 0.25
                    )
                    or 0.0
                ),
            ),
            inventory_max_ticks=max(
                0.0,
                float(params.get("post_fill_inventory_max_ticks", 4.0) or 0.0),
            ),
            flow_ticks_per_excitation=max(
                0.0,
                float(params.get("post_fill_flow_ticks_per_excitation", 2.0) or 0.0),
            ),
            flow_max_ticks=max(
                0.0,
                float(params.get("post_fill_flow_max_ticks", 8.0) or 0.0),
            ),
            flow_excitation_per_order_unit=max(
                0.0,
                float(
                    params.get(
                        "post_fill_flow_excitation_per_order_unit", 1.0
                    )
                    or 0.0
                ),
            ),
            flow_max_excitation=max(
                0.0,
                float(params.get("post_fill_flow_max_excitation", 4.0) or 0.0),
            ),
            flow_amplitude_mode=amplitude_mode,
            flow_expected_adverse_buy_ticks=max(
                0.0,
                float(
                    params.get("post_fill_flow_expected_adverse_buy_ticks", 0.0)
                    or 0.0
                ),
            ),
            flow_expected_adverse_sell_ticks=max(
                0.0,
                float(
                    params.get("post_fill_flow_expected_adverse_sell_ticks", 0.0)
                    or 0.0
                ),
            ),
            flow_add_distance_fraction_buy=_clip(
                float(
                    params.get("post_fill_flow_add_distance_fraction_buy", 1.0)
                    or 0.0
                ),
                0.0,
                1.0,
            ),
            flow_add_distance_fraction_sell=_clip(
                float(
                    params.get("post_fill_flow_add_distance_fraction_sell", 1.0)
                    or 0.0
                ),
                0.0,
                1.0,
            ),
            response_half_life_s=_clip(
                float(params.get("post_fill_response_half_life_s", 20.0) or 20.0),
                half_life_min,
                half_life_max,
            ),
            response_half_life_min_s=half_life_min,
            response_half_life_max_s=half_life_max,
            volatility_ref_bps=max(
                1e-9,
                float(params.get("post_fill_response_volatility_ref_bps", 3.0) or 3.0),
            ),
            volatility_weight=max(
                0.0,
                float(params.get("post_fill_response_volatility_weight", 0.35) or 0.0),
            ),
            refill_edge_ref=max(
                1e-9,
                float(params.get("post_fill_response_refill_edge_ref", 0.10) or 0.10),
            ),
            refill_weight=max(
                0.0,
                float(params.get("post_fill_response_refill_weight", 0.75) or 0.0),
            ),
            repair_probability_anchor=_clip(
                float(
                    params.get(
                        "post_fill_response_repair_probability_anchor", 0.60
                    )
                    or 0.60
                ),
                0.0,
                1.0,
            ),
            repair_probability_weight=max(
                0.0,
                float(
                    params.get(
                        "post_fill_response_repair_probability_weight", 1.0
                    )
                    or 0.0
                ),
            ),
        )

    @property
    def inventory_shift_enabled(self) -> bool:
        return self.enabled and self.mode in {INVENTORY_SHIFT_MODE, HYBRID_MODE}

    @property
    def flow_add_widen_enabled(self) -> bool:
        return self.enabled and self.mode in {FLOW_ADD_WIDEN_MODE, HYBRID_MODE}

    @property
    def requires_repair_model(self) -> bool:
        return self.flow_add_widen_enabled and self.repair_probability_weight > 0.0


@dataclass(frozen=True)
class PostFillQuoteResponseDecision:
    active: bool
    mode: str
    add_side: str
    reducing_side: str
    baseline_bid: float
    baseline_ask: float
    bid_price: float
    ask_price: float
    inventory_shift_ticks: int
    raw_add_widen_ticks: int
    add_widen_ticks: int
    flow_amplitude_mode: str
    baseline_add_distance_ticks: int
    excitation: float
    effective_half_life_s: float
    refill_edge: float
    repair_probability: float
    cap_headroom_ticks: int
    cap_limited: bool
    post_only_clamped: bool

    @property
    def pair_spread_delta(self) -> float:
        return (self.ask_price - self.bid_price) - (
            self.baseline_ask - self.baseline_bid
        )

class PostFillQuoteResponse:
    """Maintain causal add-fill excitation and produce discrete quote targets."""

    def __init__(self, config: PostFillQuoteResponseConfig | None = None) -> None:
        self.config = config or PostFillQuoteResponseConfig()
        self._add_side = ""
        self._excitation = 0.0
        self._last_update_ms: int | None = None
        self._last_half_life_s = self.config.response_half_life_s

    @property
    def excitation(self) -> float:
        return float(self._excitation)

    @property
    def add_side(self) -> str:
        return self._add_side

    def reset(self) -> None:
        self._add_side = ""
        self._excitation = 0.0
        self._last_update_ms = None
        self._last_half_life_s = self.config.response_half_life_s

    def _decay_to(self, now_ms: int, half_life_s: float) -> None:
        now_ms = int(now_ms)
        if self._last_update_ms is None:
            self._last_update_ms = now_ms
            self._last_half_life_s = half_life_s
            return
        elapsed_s = max(0.0, float(now_ms - self._last_update_ms) / 1000.0)
        if elapsed_s > 0.0 and self._excitation > 0.0:
            self._excitation *= math.exp(
                -math.log(2.0) * elapsed_s / max(half_life_s, 1e-9)
            )
            if self._excitation < 1e-9:
                self._excitation = 0.0
        self._last_update_ms = now_ms
        self._last_half_life_s = half_life_s

    def record_fill(
        self,
        *,
        side: str,
        inventory_before: float,
        inventory_after: float,
        fill_qty: float,
        order_size: float,
        ts_ms: int,
    ) -> bool:
        """Add Hawkes-style excitation only for exposure-increasing fills."""

        if not self.config.enabled:
            return False
        side = str(side).upper()
        exposure_increasing = (
            abs(float(inventory_after))
            > abs(float(inventory_before)) + 1e-12
        )
        expected_add_side = "BUY" if inventory_after > 0.0 else "SELL" if inventory_after < 0.0 else ""
        if not exposure_increasing or side != expected_add_side:
            if abs(float(inventory_after)) <= 1e-12:
                self.reset()
            return False
        if self._add_side and self._add_side != side:
            self.reset()
        self._add_side = side
        self._decay_to(int(ts_ms), self._last_half_life_s)
        units = abs(float(fill_qty)) / max(float(order_size), 1e-12)
        self._excitation = min(
            self.config.flow_max_excitation,
            self._excitation
            + units * self.config.flow_excitation_per_order_unit,
        )
        return True

    def _effective_half_life(
        self,
        *,
        volatility_bps: float,
        refill_edge: float,
        repair_probability: float,
    ) -> float:
        cfg = self.config
        vol_norm = _clip(
            float(volatility_bps) / cfg.volatility_ref_bps - 1.0,
            -1.0,
            2.0,
        )
        refill_norm = _clip(
            float(refill_edge) / cfg.refill_edge_ref,
            -2.0,
            2.0,
        )
        repair_term = 0.0
        if math.isfinite(float(repair_probability)):
            repair_term = cfg.repair_probability_anchor - _clip(
                float(repair_probability), 0.0, 1.0
            )
        multiplier = math.exp(
            cfg.volatility_weight * vol_norm
            - cfg.refill_weight * refill_norm
            + cfg.repair_probability_weight * repair_term
        )
        return _clip(
            cfg.response_half_life_s * multiplier,
            cfg.response_half_life_min_s,
            cfg.response_half_life_max_s,
        )

    def quote(
        self,
        *,
        now_ms: int,
        inventory: float,
        order_size: float,
        baseline_bid: float,
        baseline_ask: float,
        tick_size: float,
        max_pair_spread: float,
        best_bid: float,
        best_ask: float,
        volatility_bps: float,
        refill_edge: float,
        repair_probability: float,
    ) -> PostFillQuoteResponseDecision:
        cfg = self.config
        inventory = float(inventory)
        tick_size = max(float(tick_size), 1e-12)
        add_side = "BUY" if inventory > 1e-12 else "SELL" if inventory < -1e-12 else ""
        reducing_side = "SELL" if add_side == "BUY" else "BUY" if add_side else ""
        if not cfg.enabled or not add_side or baseline_ask <= baseline_bid:
            if not add_side:
                self.reset()
            return PostFillQuoteResponseDecision(
                active=False,
                mode=cfg.mode,
                add_side=add_side,
                reducing_side=reducing_side,
                baseline_bid=float(baseline_bid),
                baseline_ask=float(baseline_ask),
                bid_price=float(baseline_bid),
                ask_price=float(baseline_ask),
                inventory_shift_ticks=0,
                raw_add_widen_ticks=0,
                add_widen_ticks=0,
                flow_amplitude_mode=cfg.flow_amplitude_mode,
                baseline_add_distance_ticks=0,
                excitation=float(self._excitation),
                effective_half_life_s=cfg.response_half_life_s,
                refill_edge=float(refill_edge),
                repair_probability=float(repair_probability),
                cap_headroom_ticks=0,
                cap_limited=False,
                post_only_clamped=False,
            )

        inventory_ticks = 0
        if cfg.inventory_shift_enabled:
            inventory_units = abs(inventory) / max(float(order_size), 1e-12)
            inventory_ticks = min(
                _nearest_nonnegative_int(cfg.inventory_max_ticks),
                _nearest_nonnegative_int(
                    inventory_units * cfg.inventory_ticks_per_order_unit
                ),
            )

        half_life_s = self._effective_half_life(
            volatility_bps=volatility_bps,
            refill_edge=refill_edge,
            repair_probability=repair_probability,
        )
        self._decay_to(int(now_ms), half_life_s)
        raw_add_ticks = 0
        if cfg.flow_add_widen_enabled and self._add_side == add_side:
            if cfg.flow_amplitude_mode == EXPECTED_ADVERSE_FLOW_AMPLITUDE_MODE:
                expected_ticks = (
                    cfg.flow_expected_adverse_buy_ticks
                    if add_side == "BUY"
                    else cfg.flow_expected_adverse_sell_ticks
                )
                raw_add_ticks = _nearest_nonnegative_int(
                    self._excitation * expected_ticks
                )
            else:
                raw_add_ticks = _nearest_nonnegative_int(
                    self._excitation * cfg.flow_ticks_per_excitation
                )
            raw_add_ticks = min(
                _nearest_nonnegative_int(cfg.flow_max_ticks),
                raw_add_ticks,
            )

        baseline_spread = max(0.0, float(baseline_ask) - float(baseline_bid))
        baseline_add_distance_ticks = _nearest_nonnegative_int(
            baseline_spread / (2.0 * tick_size)
        )
        if cfg.flow_amplitude_mode == EXPECTED_ADVERSE_FLOW_AMPLITUDE_MODE:
            distance_fraction = (
                cfg.flow_add_distance_fraction_buy
                if add_side == "BUY"
                else cfg.flow_add_distance_fraction_sell
            )
            raw_add_ticks = min(
                raw_add_ticks,
                int(
                    math.floor(
                        baseline_add_distance_ticks * distance_fraction + 1e-9
                    )
                ),
            )
        if max_pair_spread > 0.0:
            cap_headroom_ticks = max(
                0,
                int(
                    math.floor(
                        (float(max_pair_spread) - baseline_spread) / tick_size
                        + 1e-9
                    )
                ),
            )
        else:
            cap_headroom_ticks = raw_add_ticks
        add_ticks = min(raw_add_ticks, cap_headroom_ticks)
        cap_limited = add_ticks < raw_add_ticks

        shift_i = inventory_ticks * tick_size
        shift_a = add_ticks * tick_size
        if inventory > 0.0:
            bid_price = _floor_tick(
                float(baseline_bid) - shift_i - shift_a,
                tick_size,
            )
            ask_price = _floor_tick(float(baseline_ask) - shift_i, tick_size)
        else:
            bid_price = _ceil_tick(float(baseline_bid) + shift_i, tick_size)
            ask_price = _ceil_tick(
                float(baseline_ask) + shift_i + shift_a,
                tick_size,
            )

        unclamped_bid = bid_price
        unclamped_ask = ask_price
        if best_ask > 0.0:
            bid_price = min(bid_price, _floor_tick(best_ask - tick_size, tick_size))
        if best_bid > 0.0:
            ask_price = max(ask_price, _ceil_tick(best_bid + tick_size, tick_size))
        if ask_price <= bid_price:
            if inventory > 0.0:
                ask_price = bid_price + tick_size
            else:
                bid_price = ask_price - tick_size
        post_only_clamped = (
            abs(bid_price - unclamped_bid) > tick_size * 1e-6
            or abs(ask_price - unclamped_ask) > tick_size * 1e-6
        )

        return PostFillQuoteResponseDecision(
            active=bool(inventory_ticks or add_ticks),
            mode=cfg.mode,
            add_side=add_side,
            reducing_side=reducing_side,
            baseline_bid=float(baseline_bid),
            baseline_ask=float(baseline_ask),
            bid_price=float(bid_price),
            ask_price=float(ask_price),
            inventory_shift_ticks=inventory_ticks,
            raw_add_widen_ticks=raw_add_ticks,
            add_widen_ticks=add_ticks,
            flow_amplitude_mode=cfg.flow_amplitude_mode,
            baseline_add_distance_ticks=baseline_add_distance_ticks,
            excitation=float(self._excitation),
            effective_half_life_s=float(half_life_s),
            refill_edge=float(refill_edge),
            repair_probability=float(repair_probability),
            cap_headroom_ticks=cap_headroom_ticks,
            cap_limited=cap_limited,
            post_only_clamped=post_only_clamped,
        )


__all__ = [
    "EXPECTED_ADVERSE_FLOW_AMPLITUDE_MODE",
    "FLOW_ADD_WIDEN_MODE",
    "HYBRID_MODE",
    "INVENTORY_SHIFT_MODE",
    "LEGACY_FLOW_AMPLITUDE_MODE",
    "NOOP_MODE",
    "PostFillQuoteResponse",
    "PostFillQuoteResponseConfig",
    "PostFillQuoteResponseDecision",
]
