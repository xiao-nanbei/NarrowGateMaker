"""Causal target-price innovation rate; no market-feed or order side effects.

Prices have quote/base units P, time is seconds, and variance_rate is P²/s.
The fourth-root controller uses R in P²*s. It is an empirical controller,
not an optimum for discrete asynchronous order execution. Callers must publish
pre-throttle targets, not accepted order prices, under one explicit contract.
"""

from dataclasses import asdict, dataclass, field
import math


HALFLIFE_SECONDS = 60.0
READY_SPAN_SECONDS = 300.0


def _finite(value, name, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number")
    if not math.isfinite(value) or (positive and value <= 0):
        raise ValueError(f"invalid {name}")
    return float(value)


@dataclass
class TargetVarianceState:
    last_target_price: float | None = None
    last_target_ready_time: int | None = None  # integer nanoseconds, not wall runtime
    last_target_version: int | None = None  # monotonically increasing per side
    mean_rate: float = 0.0
    variance_rate: float = 0.0
    valid_span: float = 0.0
    positive_dt_count: int = 0
    initialized: bool = False
    last_invalid_reason: str | None = "uninitialized"

    @property
    def ready(self):
        return (self.initialized and self.valid_span >= READY_SPAN_SECONDS
                and self.positive_dt_count >= 2)

    def invalidate(self, reason):
        """Explicit disconnect/missing-data boundary; never inject a zero price.

        Keep the version/time watermark so a repeated consumer cannot turn an
        old publication into a new initial observation after invalidation.
        """
        if not isinstance(reason, str) or not reason:
            raise ValueError("invalidation reason required")
        self.mean_rate = self.variance_rate = self.valid_span = 0.0
        self.positive_dt_count = 0
        self.initialized = False
        self.last_invalid_reason = reason

    def observe(self, *, price, ready_ns, version):
        """Return publication disposition; identical versions are idempotent.

        At equal time a new version replaces the previous price in publication
        order, without a rate update. The next positive-dt increment therefore
        starts at that last price. No epsilon time or duplicate zero is invented.
        Invalid numbers/clock regressions raise before changing any state.
        """
        price = _finite(price, "target_price", positive=True)
        if type(ready_ns) is not int or ready_ns < 0:
            raise ValueError("ready_ns must be a nonnegative integer")
        if type(version) is not int or version < 0:
            raise ValueError("version must be a nonnegative integer")
        if self.last_target_version is not None:
            if version < self.last_target_version:
                raise ValueError("target version regression")
            if version == self.last_target_version:
                if price != self.last_target_price or ready_ns != self.last_target_ready_time:
                    raise ValueError("target version content conflict")
                return "duplicate"
            if ready_ns < self.last_target_ready_time:
                raise ValueError("target ready clock regression")
        disposition = "initialized"
        mean, variance, span, count = 0.0, 0.0, 0.0, 0
        if self.initialized:
            dt = (ready_ns - self.last_target_ready_time) / 1_000_000_000
            mean, variance = self.mean_rate, self.variance_rate
            span, count = self.valid_span, self.positive_dt_count
            if dt > HALFLIFE_SECONDS:
                mean, variance, span, count = 0.0, 0.0, 0.0, 0
                disposition = "sample_gap"
            elif dt == 0:
                disposition = "same_time_replaced"
            else:
                dp = price - self.last_target_price
                weight = -math.expm1(-math.log(2.0) * dt / HALFLIFE_SECONDS)
                innovation = dp - mean * dt  # previous mean, before this update
                variance = (1.0 - weight) * variance + weight * (innovation * innovation / dt)
                mean = (1.0 - weight) * mean + weight * dp / dt
                span += dt
                count += 1
                if not all(math.isfinite(x) for x in (mean, variance, span)):
                    raise ValueError("target rate numerical range exceeded")
                disposition = "updated"
        self.last_target_price = price
        self.last_target_ready_time = ready_ns
        self.last_target_version = version
        self.mean_rate, self.variance_rate = mean, variance
        self.valid_span, self.positive_dt_count = span, count
        self.initialized = True
        self.last_invalid_reason = None if self.ready else (
            "sample_gap" if disposition == "sample_gap" else "warmup")
        return disposition

    def snapshot(self):
        return asdict(self)

    @classmethod
    def restore(cls, value):
        if set(value) != set(cls.__dataclass_fields__):
            raise ValueError("target estimator state fields mismatch")
        result = cls(**value)
        for name in ("mean_rate", "variance_rate", "valid_span"):
            _finite(getattr(result, name), name)
        if result.variance_rate < 0 or result.valid_span < 0:
            raise ValueError("negative estimator state")
        if type(result.positive_dt_count) is not int or result.positive_dt_count < 0:
            raise ValueError("invalid increment count")
        if type(result.initialized) is not bool:
            raise ValueError("invalid initialization state")
        watermark = (result.last_target_price, result.last_target_ready_time,
                     result.last_target_version)
        if any(x is not None for x in watermark):
            _finite(result.last_target_price, "target_price", positive=True)
            for x in watermark[1:]:
                if type(x) is not int or x < 0:
                    raise ValueError("invalid target watermark")
        elif result.initialized:
            raise ValueError("initialized estimator has no target")
        if not result.initialized and (result.mean_rate or result.variance_rate
                                       or result.valid_span or result.positive_dt_count):
            raise ValueError("uninitialized estimator has statistics")
        return result


def dynamic_outward_ticks(*, variance_rate, r, tick_size):
    """Dimensionally valid fourth-root scale with adjacent-integer cost choice.

    C/K=r has P²*s units. No 10/15 floor or ceiling exists. R is validated even
    for zero variance; missing/unready variance is handled outside this function.
    """
    v = _finite(variance_rate, "variance_rate")
    r = _finite(r, "R", positive=True)
    tick = _finite(tick_size, "tick_size", positive=True)
    if v < 0:
        raise ValueError("negative variance_rate")
    if v == 0:
        return 1
    log_n = (math.log(12.0) + math.log(r) + math.log(v)) / 4.0 - math.log(tick)
    if log_n <= 0:
        return 1
    # Beyond this range a float cannot distinguish adjacent integer choices.
    if log_n >= math.log(2**53):
        raise ValueError("dynamic tick threshold exceeds adjacent-integer precision")
    n = math.exp(log_n)
    lo, hi = math.floor(n), math.ceil(n)
    cost_lo = (lo / n)**2 + (n / lo)**2
    cost_hi = (hi / n)**2 + (n / hi)**2
    return lo if cost_lo <= cost_hi else hi


@dataclass
class TargetVariancePair:
    """Independent per-path, per-side state with bound restore metadata.

    This object does not read a clock or a market feed, and does not submit an
    order. Runtime publishers must provide version and ready time explicitly.
    """
    r: float
    observation_contract: str
    sides: dict = field(default_factory=lambda: {
        "BUY": TargetVarianceState(), "SELL": TargetVarianceState()})

    def __post_init__(self):
        _finite(self.r, "R", positive=True)
        if not isinstance(self.observation_contract, str) or not self.observation_contract:
            raise ValueError("observation contract required")
        if set(self.sides) != {"BUY", "SELL"}:
            raise ValueError("both target sides required")
        if self.sides['BUY'] is self.sides['SELL']:
            raise ValueError("target sides cannot share mutable state")

    def outward_ticks(self, side, tick_size):
        _finite(tick_size, "tick_size", positive=True)
        state = self.sides[side]
        if not state.ready:
            return 15, state.last_invalid_reason or "warmup"
        return dynamic_outward_ticks(variance_rate=state.variance_rate,
                                     r=self.r, tick_size=tick_size), None

    def snapshot(self):
        return dict(schema='target_variance_pair.v1', r=self.r,
                    observation_contract=self.observation_contract,
                    half_life_seconds=HALFLIFE_SECONDS,
                    ready_span_seconds=READY_SPAN_SECONDS,
                    sides={k: v.snapshot() for k, v in self.sides.items()})

    @classmethod
    def restore(cls, value, *, r, observation_contract):
        expected = cls(r, observation_contract).snapshot()
        if set(value) != set(expected) or any(value[k] != expected[k]
                for k in expected if k != 'sides'):
            raise ValueError("target estimator restore binding mismatch")
        return cls(r, observation_contract,
                   {k: TargetVarianceState.restore(v) for k, v in value['sides'].items()})
