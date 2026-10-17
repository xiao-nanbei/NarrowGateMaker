"""Price-direction selection only; callers retain their original throttle rules."""

from dataclasses import dataclass
import math
from strategy.target_variance import dynamic_outward_ticks


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
