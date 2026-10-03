"""Causal EMA state for the Boolean cooldown-duration study.

This module is deliberately separate from the closed ADD-vs-WAIT identities.
It exposes every legal EMA pair and preserves the state needed by a bounded
Boolean rule learner.  It does not select a cooldown, read economic outcomes,
or authorize an action.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import combinations

import numpy as np

IDENTITY = "multiscale_ema_boolean_cooldown_duration_policy_v1"
SCHEMA_VERSION = f"{IDENTITY}.ema_state.v1"
EMA_HALF_LIVES_S = (0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0)
CROSS_AGE_MISSING_SENTINEL_S = 1.0e12


def duration_seconds_to_milliseconds(duration_s: float) -> int:
    """Convert a frozen action duration without accepting unit ambiguity."""

    value = float(duration_s)
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError("cooldown duration seconds must be positive and finite")
    milliseconds = int(round(value * 1_000.0))
    if not math.isclose(
        float(milliseconds), value * 1_000.0, rel_tol=0.0, abs_tol=1e-9
    ):
        raise ValueError("cooldown duration is not exactly representable in ms")
    return milliseconds


def epoch_milliseconds_to_nanoseconds(timestamp_ms: int) -> int:
    """Convert the replay visibility clock to the EMA clock exactly once."""

    value = int(timestamp_ms)
    if value < 0:
        raise ValueError("epoch millisecond timestamp must be non-negative")
    return value * 1_000_000


def _label(value: float) -> str:
    return f"h{float(value):g}s".replace(".", "p")


def _side_sign(side: str) -> float:
    normalized = str(side).upper()
    if normalized == "BUY":
        return 1.0
    if normalized == "SELL":
        return -1.0
    raise ValueError(f"unsupported side: {side!r}")


def ema_pairs(
    half_lives_s: Sequence[float] = EMA_HALF_LIVES_S,
) -> tuple[tuple[float, float], ...]:
    values = tuple(float(value) for value in half_lives_s)
    if not values or values != tuple(sorted(set(values))):
        raise ValueError("EMA half-lives must be strictly increasing and unique")
    if any(not math.isfinite(value) or value <= 0.0 for value in values):
        raise ValueError("EMA half-lives must be positive and finite")
    return tuple(combinations(values, 2))


def pair_prefix(fast_half_life_s: float, slow_half_life_s: float) -> str:
    if float(fast_half_life_s) >= float(slow_half_life_s):
        raise ValueError("EMA pair requires fast < slow")
    return f"ema_pair_{_label(fast_half_life_s)}_{_label(slow_half_life_s)}"


@dataclass
class _PairState:
    effective_sign: int = 0
    arrangement_start_ts_ns: int | None = None
    last_cross_ts_ns: int | None = None
    last_cross_direction: int = 0
    prior_abs_distance_bps: float = 0.0


class BooleanEmaSurface:
    """Irregular-clock, all-pair EMA surface on one canonical visible price."""

    def __init__(self, half_lives_s: Sequence[float] = EMA_HALF_LIVES_S) -> None:
        self.half_lives_s = tuple(float(value) for value in half_lives_s)
        self.pairs = ema_pairs(self.half_lives_s)
        self._index = {
            half_life: index
            for index, half_life in enumerate(self.half_lives_s)
        }
        self._ema: list[float] = []
        self._velocity: list[float] = []
        self._pair_state = {pair: _PairState() for pair in self.pairs}
        self._last_ts_ns: int | None = None
        self._first_ts_ns: int | None = None
        self._last_price: float | None = None
        self._ordering_signature: tuple[int, ...] | None = None
        self._ordering_start_ts_ns: int | None = None

    @property
    def initialized(self) -> bool:
        return self._last_ts_ns is not None

    @property
    def feature_ready_ts_ns(self) -> int:
        if self._last_ts_ns is None:
            raise RuntimeError("EMA surface is not initialized")
        return int(self._last_ts_ns)

    def update(self, *, ts_ns: int, price: float) -> None:
        timestamp = int(ts_ns)
        value = float(price)
        if timestamp < 0 or not math.isfinite(value) or value <= 0.0:
            raise ValueError("EMA observation requires a valid clock and price")
        if self._last_ts_ns is None:
            self._last_ts_ns = timestamp
            self._first_ts_ns = timestamp
            self._last_price = value
            self._ema = [value] * len(self.half_lives_s)
            self._velocity = [0.0] * len(self.half_lives_s)
            return
        if timestamp < self._last_ts_ns:
            raise ValueError("EMA observation clock regressed")
        if timestamp == self._last_ts_ns:
            if not math.isclose(value, float(self._last_price), abs_tol=1e-12):
                raise ValueError("duplicate EMA timestamp changed canonical price")
            return

        delta_s = float(timestamp - self._last_ts_ns) / 1_000_000_000.0
        prior = tuple(self._ema)
        for index, half_life_s in enumerate(self.half_lives_s):
            decay = math.exp(-math.log(2.0) * delta_s / half_life_s)
            current = decay * prior[index] + (1.0 - decay) * value
            self._ema[index] = current
            self._velocity[index] = (current - prior[index]) / delta_s

        pair_signatures: list[int] = []
        for fast, slow in self.pairs:
            fast_index = self._index[fast]
            slow_index = self._index[slow]
            distance_bps = (
                10_000.0
                * (self._ema[fast_index] - self._ema[slow_index])
                / value
            )
            raw_sign = 1 if distance_bps > 0.0 else -1 if distance_bps < 0.0 else 0
            state = self._pair_state[(fast, slow)]
            if raw_sign:
                if state.effective_sign == 0:
                    state.effective_sign = raw_sign
                    state.arrangement_start_ts_ns = timestamp
                elif raw_sign != state.effective_sign:
                    state.effective_sign = raw_sign
                    state.arrangement_start_ts_ns = timestamp
                    state.last_cross_ts_ns = timestamp
                    state.last_cross_direction = raw_sign
            state.prior_abs_distance_bps = abs(distance_bps)
            pair_signatures.append(state.effective_sign)

        signature = tuple(pair_signatures)
        if all(value != 0 for value in signature) and signature != self._ordering_signature:
            self._ordering_signature = signature
            self._ordering_start_ts_ns = timestamp
        self._last_ts_ns = timestamp
        self._last_price = value

    def feature_row(
        self,
        *,
        side: str,
        causal_volatility_bps: float,
        decision_ts_ns: int,
        volatility_ready_ts_ns: int,
        snapshot_market_generation: int,
        snapshot_depth_generation: int,
        history_state_complete: bool,
    ) -> dict[str, float | int]:
        if not self.initialized:
            raise RuntimeError("EMA surface is not initialized")
        decision_timestamp = int(decision_ts_ns)
        volatility_ready_timestamp = int(volatility_ready_ts_ns)
        market_generation = int(snapshot_market_generation)
        depth_generation = int(snapshot_depth_generation)
        if not history_state_complete:
            raise ValueError("EMA history state is incomplete")
        if (
            decision_timestamp < self.feature_ready_ts_ns
            or volatility_ready_timestamp > decision_timestamp
            or volatility_ready_timestamp < 0
            or market_generation < 0
            or depth_generation < 0
        ):
            raise ValueError("EMA feature clocks or snapshot generation are invalid")
        volatility = float(causal_volatility_bps)
        if not math.isfinite(volatility) or volatility <= 0.0:
            raise ValueError("causal volatility scale must be positive and finite")
        sign = _side_sign(side)
        mid = float(self._last_price)
        now = self.feature_ready_ts_ns
        output: dict[str, float | int] = {
            "ema_surface_feature_ready_ts_ns": now,
            "ema_surface_first_history_ts_ns": int(self._first_ts_ns),
            "ema_surface_decision_ts_ns": decision_timestamp,
            "ema_surface_volatility_ready_ts_ns": volatility_ready_timestamp,
            "ema_surface_snapshot_market_generation": market_generation,
            "ema_surface_snapshot_depth_generation": depth_generation,
            "ema_surface_history_state_complete": 1,
            "ema_surface_canonical_mid": mid,
            "ema_pair_count": len(self.pairs),
        }
        for half_life, ema, velocity in zip(
            self.half_lives_s, self._ema, self._velocity, strict=True
        ):
            label = _label(half_life)
            output[f"ema_rel_mid_bps_{label}"] = sign * 10_000.0 * (ema - mid) / mid
            output[f"ema_slope_bps_per_s_{label}"] = sign * 10_000.0 * velocity / mid

        favorable_count = 0
        for fast, slow in self.pairs:
            fast_index = self._index[fast]
            slow_index = self._index[slow]
            prefix = pair_prefix(fast, slow)
            raw_distance = (
                10_000.0
                * (self._ema[fast_index] - self._ema[slow_index])
                / mid
            )
            favorable_distance = sign * raw_distance
            distance_velocity = (
                sign
                * 10_000.0
                * (self._velocity[fast_index] - self._velocity[slow_index])
                / mid
            )
            state = self._pair_state[(fast, slow)]
            favorable_ordering = int(sign * state.effective_sign > 0)
            favorable_count += favorable_ordering
            cross_age_s = (
                float(now - state.last_cross_ts_ns) / 1_000_000_000.0
                if state.last_cross_ts_ns is not None
                else CROSS_AGE_MISSING_SENTINEL_S
            )
            persistence_s = (
                float(now - state.arrangement_start_ts_ns) / 1_000_000_000.0
                if state.arrangement_start_ts_ns is not None
                else 0.0
            )
            output.update(
                {
                    f"{prefix}_favorable_ordering": favorable_ordering,
                    f"{prefix}_adverse_ordering": int(
                        sign * state.effective_sign < 0
                    ),
                    f"{prefix}_last_cross_favorable": int(
                        sign * state.last_cross_direction > 0
                    ),
                    f"{prefix}_last_cross_adverse": int(
                        sign * state.last_cross_direction < 0
                    ),
                    f"{prefix}_cross_missing": int(
                        state.last_cross_ts_ns is None
                    ),
                    f"{prefix}_cross_age_s": cross_age_s,
                    f"{prefix}_arrangement_persistence_s": persistence_s,
                    f"{prefix}_favorable_distance_bps": favorable_distance,
                    f"{prefix}_abs_distance_bps": abs(raw_distance),
                    f"{prefix}_volatility_normalized": (
                        favorable_distance / volatility
                    ),
                    f"{prefix}_favorable_distance_velocity_bps_per_s": (
                        distance_velocity
                    ),
                    f"{prefix}_distance_expanding": int(
                        raw_distance * (
                            self._velocity[fast_index]
                            - self._velocity[slow_index]
                        )
                        > 0.0
                    ),
                    f"{prefix}_distance_converging": int(
                        raw_distance * (
                            self._velocity[fast_index]
                            - self._velocity[slow_index]
                        )
                        < 0.0
                    ),
                }
            )

        output["ema_pair_favorable_fraction"] = favorable_count / len(self.pairs)
        ordering_age_s = (
            float(now - self._ordering_start_ts_ns) / 1_000_000_000.0
            if self._ordering_start_ts_ns is not None
            else 0.0
        )
        output["ema_full_ordering_persistence_s"] = ordering_age_s
        output["ema_full_ordering_missing"] = int(
            self._ordering_start_ts_ns is None
        )
        return output


@dataclass(frozen=True, slots=True)
class AtomicPredicate:
    name: str
    feature: str
    operator: str
    threshold: float
    provenance: str

    def evaluate(self, row: Mapping[str, object]) -> int:
        value = float(row[self.feature])
        if not math.isfinite(value):
            raise ValueError(f"predicate feature is nonfinite: {self.feature}")
        if self.operator == "gt":
            return int(value > self.threshold)
        if self.operator == "ge":
            return int(value >= self.threshold)
        if self.operator == "lt":
            return int(value < self.threshold)
        if self.operator == "le":
            return int(value <= self.threshold)
        if self.operator == "eq":
            return int(value == self.threshold)
        raise ValueError(f"unsupported predicate operator: {self.operator}")


def atomic_predicate_dictionary(
    *,
    pair_distance_scale_bps: Mapping[str, float],
    half_lives_s: Sequence[float] = EMA_HALF_LIVES_S,
) -> tuple[AtomicPredicate, ...]:
    """Build an outcome-blind Boolean basis.

    Pair-distance scales must come from the admitted 2025 provider source
    population.  Age cutoffs are tied to the two EMA half-lives themselves;
    they are feature-basis semantics, not selected cooldown durations.
    """

    predicates: list[AtomicPredicate] = []
    for fast, slow in ema_pairs(half_lives_s):
        prefix = pair_prefix(fast, slow)
        scale = float(pair_distance_scale_bps[prefix])
        if not math.isfinite(scale) or scale <= 0.0:
            raise ValueError(f"invalid provider pair scale: {prefix}")
        predicates.extend(
            (
                AtomicPredicate(
                    f"{prefix}:favorable",
                    f"{prefix}_favorable_ordering",
                    "eq",
                    1.0,
                    "ordering",
                ),
                AtomicPredicate(
                    f"{prefix}:last_cross_favorable",
                    f"{prefix}_last_cross_favorable",
                    "eq",
                    1.0,
                    "cross_direction",
                ),
                AtomicPredicate(
                    f"{prefix}:cross_age_le_fast",
                    f"{prefix}_cross_age_s",
                    "le",
                    fast,
                    "ema_half_life_basis",
                ),
                AtomicPredicate(
                    f"{prefix}:cross_age_le_slow",
                    f"{prefix}_cross_age_s",
                    "le",
                    slow,
                    "ema_half_life_basis",
                ),
                AtomicPredicate(
                    f"{prefix}:persistence_ge_fast",
                    f"{prefix}_arrangement_persistence_s",
                    "ge",
                    fast,
                    "ema_half_life_basis",
                ),
                AtomicPredicate(
                    f"{prefix}:persistence_ge_slow",
                    f"{prefix}_arrangement_persistence_s",
                    "ge",
                    slow,
                    "ema_half_life_basis",
                ),
                AtomicPredicate(
                    f"{prefix}:distance_ge_provider_sigma",
                    f"{prefix}_abs_distance_bps",
                    "ge",
                    scale,
                    "2025_provider_full_source_grid_standard_deviation",
                ),
                AtomicPredicate(
                    f"{prefix}:expanding",
                    f"{prefix}_distance_expanding",
                    "eq",
                    1.0,
                    "distance_dynamics",
                ),
            )
        )
    return tuple(predicates)


def apply_predicates(
    row: Mapping[str, object],
    predicates: Sequence[AtomicPredicate],
) -> dict[str, int]:
    names = [predicate.name for predicate in predicates]
    if len(names) != len(set(names)):
        raise ValueError("atomic predicate names are not unique")
    return {predicate.name: predicate.evaluate(row) for predicate in predicates}


def provider_pair_distance_scales(
    *,
    feature_names: Sequence[str],
    scale: np.ndarray,
    components: np.ndarray,
    eigenvalues: np.ndarray,
    half_lives_s: Sequence[float] = EMA_HALF_LIVES_S,
) -> dict[str, float]:
    """Recover all-pair distance scales from the 2025 full-rank encoder."""

    names = tuple(str(name) for name in feature_names)
    raw_scale = np.asarray(scale, dtype=np.float64)
    vectors = np.asarray(components, dtype=np.float64)
    values = np.asarray(eigenvalues, dtype=np.float64)
    width = len(names)
    if (
        raw_scale.shape != (width,)
        or vectors.shape != (width, width)
        or values.shape != (width,)
    ):
        raise ValueError("provider encoder shape drifted")
    correlation = vectors.T @ np.diag(values) @ vectors
    covariance = correlation * np.outer(raw_scale, raw_scale)
    output: dict[str, float] = {}
    for fast, slow in ema_pairs(half_lives_s):
        fast_name = f"ema_rel_mid_bps_{_label(fast)}"
        slow_name = f"ema_rel_mid_bps_{_label(slow)}"
        try:
            fast_index = names.index(fast_name)
            slow_index = names.index(slow_name)
        except ValueError as exc:
            raise ValueError("provider encoder lacks an EMA level") from exc
        variance = (
            covariance[fast_index, fast_index]
            + covariance[slow_index, slow_index]
            - 2.0 * covariance[fast_index, slow_index]
        )
        output[pair_prefix(fast, slow)] = math.sqrt(max(float(variance), 1e-24))
    return output


__all__ = [
    "AtomicPredicate",
    "BooleanEmaSurface",
    "CROSS_AGE_MISSING_SENTINEL_S",
    "EMA_HALF_LIVES_S",
    "IDENTITY",
    "SCHEMA_VERSION",
    "apply_predicates",
    "atomic_predicate_dictionary",
    "ema_pairs",
    "duration_seconds_to_milliseconds",
    "epoch_milliseconds_to_nanoseconds",
    "pair_prefix",
    "provider_pair_distance_scales",
]
