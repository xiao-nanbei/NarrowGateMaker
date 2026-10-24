"""Shared live/replay replacement decisions, independent of clocks and transport.

Adapters supply a strategy-visible order state, its elapsed age in milliseconds
and its price movement in ticks.  This owner decides the role, throttle and
pending/cancel-first gates; it never turns a cancel request into a terminal ACK.
"""

from dataclasses import dataclass
import math
from strategy.target_variance import dynamic_outward_ticks


def replace_throttle_parameters(
    *, side: str, inventory: float, ordinary_ticks: float,
    ordinary_interval_ms: float, reducing_ticks: float,
    reducing_interval_ms: float,
) -> tuple[float, float, bool]:
    """Select the original sign-based role, including both sides when flat.

    This is deliberately not the quantity-aware exposure classification used
    by submission risk checks: replacing a quote has the original side role.
    Values have already been resolved/clamped by each configuration adapter.
    """
    exposure_increasing = (side == "BUY" and inventory >= 0.0) or (
        side == "SELL" and inventory <= 0.0
    )
    if exposure_increasing:
        return ordinary_ticks, ordinary_interval_ms, True
    return reducing_ticks, reducing_interval_ms, False


def replace_throttle_blocks(
    *, price_delta_ticks: float, age_ms: float, min_ticks: float,
    min_interval_ms: float,
) -> tuple[bool, bool]:
    """Independent price and elapsed-age gates, with original boundaries.

    Keep these two causes separate: a response policy may replace the price
    decision, but must not remove the age gate or its future wake deadline.
    Price representation and clock conversion remain explicit in the adapters.
    """
    return (
        min_ticks > 0.0 and price_delta_ticks + 1e-9 < min_ticks,
        min_interval_ms > 0.0 and age_ms < min_interval_ms,
    )


def coalesce_pending_replace(
    *, enabled: bool, needs_update: bool, can_post: bool, pending: bool,
) -> bool:
    """A pending NEW/CANCEL owns the side until its state converges."""
    return bool(enabled and needs_update and can_post and pending)


def cancel_first_replace(
    *, enabled: bool, needs_update: bool, can_post: bool, active: bool,
    exposure_increasing: bool, force_update: bool = False,
) -> bool:
    """Select configured cancel-first routing, not successful cancellation.

    Forced safety/TTL updates bypass this optional ordinary replacement arm.
    A caller without a force-update input explicitly retains its legacy false
    default; no transport ACK or ownership release is inferred here.
    """
    return bool(
        enabled and needs_update and not force_update and can_post
        and active and exposure_increasing
    )


@dataclass(frozen=True)
class ReplacePriceThreshold:
    mode: str = "fixed"
    outward: float | None = None
    inward: float | None = None
    outward_r: float | None = None
    static_ticks: int | None = None

    def __post_init__(self):
        if self.mode not in ("fixed", "directional", "dynamic_outward", "static_outward"):
            raise ValueError("invalid replace_price_threshold_mode")
        if self.mode == "directional":
            for value in (self.outward, self.inward):
                if value is None or not math.isfinite(value) or value < 0:
                    raise ValueError("directional replace thresholds must be finite and nonnegative")
        if self.mode == "dynamic_outward" and (
            self.outward_r is None or not math.isfinite(self.outward_r) or self.outward_r <= 0
        ):
            raise ValueError("dynamic outward R must be finite and positive")
        if self.mode == "static_outward" and (
            type(self.static_ticks) is not int or self.static_ticks < 1
        ):
            raise ValueError("static outward ticks must be a positive integer")

    @classmethod
    def from_getter(cls, get):
        return cls(get("replace_price_threshold_mode", "fixed"),
                   get("replace_min_price_change_ticks_outward", None),
                   get("replace_min_price_change_ticks_inward", None),
                   get("replace_outward_r", None),
                   get("replace_outward_static_ticks", None))

    def select(self, *, ordinary, side, old_price, target_price, fixed,
               tick_size=None, observation=None):
        if self.mode == "fixed" or not ordinary:
            return fixed
        signed_move = (1 if side == "BUY" else -1) * (target_price - old_price)
        # No price change retains the existing quantity-update branch semantics.
        if signed_move == 0:
            return fixed
        if self.mode in ("dynamic_outward", "static_outward"):
            if signed_move > 0:
                return fixed
            if observation is None:
                raise ValueError("outward estimator state is required")
            # Read only the applied watermark. Never advance a pending/future
            # publication here, including the target being judged now.
            state = observation.sides[side]
            if not state.ready:
                return fixed
            if self.mode == "static_outward":
                return self.static_ticks
            return dynamic_outward_ticks(variance_rate=state.variance_rate,
                                         r=self.outward_r, tick_size=tick_size)
        return self.inward if signed_move > 0 else self.outward
