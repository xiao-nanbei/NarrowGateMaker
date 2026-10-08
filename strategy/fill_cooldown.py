"""Shared fill-cooldown and variance-time contracts.

The live baseline counts same-side filled quantity in order-size units.  The
counter includes both exposure-increasing and reducing fills; only an opposite
side fill necessarily clears it.  Historical replay may additionally clear it
when the wall-clock cooldown expires, but that behavior must be explicit.
"""

from __future__ import annotations

import hashlib
import hmac
import math
from bisect import bisect_right
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass

LINEAGE_CONTROL_ACTION = "fixed_wall_time_85n"
LINEAGE_CANDIDATE_ACTION = "variance_time_rearm"
LINEAGE_RANDOMIZED_ACTIONS = (
    LINEAGE_CONTROL_ACTION,
    LINEAGE_CANDIDATE_ACTION,
)

RESET_OPPOSITE_FILL_ONLY = "opposite_fill_only"
RESET_OPPOSITE_FILL_OR_EXPIRY = "opposite_fill_or_expiry"
VALID_CONSECUTIVE_RESET_POLICIES = frozenset(
    {RESET_OPPOSITE_FILL_ONLY, RESET_OPPOSITE_FILL_OR_EXPIRY}
)


def normalize_lineage_randomization_probabilities(
    value: Mapping[str, object] | None,
) -> dict[str, float]:
    """Validate the two-arm cooldown-lineage behavior policy."""

    raw = value or {
        LINEAGE_CONTROL_ACTION: 0.5,
        LINEAGE_CANDIDATE_ACTION: 0.5,
    }
    if set(raw) != set(LINEAGE_RANDOMIZED_ACTIONS):
        raise ValueError(
            "cooldown-lineage probabilities must contain exactly "
            f"{list(LINEAGE_RANDOMIZED_ACTIONS)}"
        )
    probabilities = {
        action: float(raw[action]) for action in LINEAGE_RANDOMIZED_ACTIONS
    }
    if any(
        not math.isfinite(probability) or probability <= 0.0
        for probability in probabilities.values()
    ):
        raise ValueError("cooldown-lineage probabilities must be positive")
    if not math.isclose(sum(probabilities.values()), 1.0, abs_tol=1e-12):
        raise ValueError("cooldown-lineage probabilities must sum to one")
    return probabilities


def choose_lineage_randomized_action(
    rng: object,
    probabilities: Mapping[str, float],
) -> tuple[str, float]:
    """Draw one assignment before the lineage's downstream path exists."""

    normalized = normalize_lineage_randomization_probabilities(probabilities)
    draw = float(rng.random())
    if not 0.0 <= draw < 1.0:
        raise ValueError("random generator returned a value outside [0, 1)")
    control_probability = normalized[LINEAGE_CONTROL_ACTION]
    action = (
        LINEAGE_CONTROL_ACTION
        if draw < control_probability
        else LINEAGE_CANDIDATE_ACTION
    )
    return action, draw


@dataclass(frozen=True)
class StratifiedBernoulliLineageRandomizer:
    """Independent causal 0.5 assignment keyed by a pre-assignment lineage id.

    Day and side are part of the PRF domain, so analysis is stratified without
    making one path-dependent lineage's action depend on an earlier action.
    """

    seed: int
    family_id: str

    def _uniform(self, *parts: object) -> float:
        key = str(int(self.seed)).encode("ascii")
        raw = "|".join((self.family_id, *(str(part) for part in parts))).encode(
            "utf-8"
        )
        value = int.from_bytes(
            hmac.new(key, raw, hashlib.sha256).digest()[:8],
            "big",
        )
        return value / float(1 << 64)

    def assign(
        self,
        *,
        utc_day: str,
        side: str,
        pre_assignment_lineage_uid: str,
    ) -> tuple[str, float, str]:
        normalized_side = str(side).upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError("lineage randomization side must be BUY or SELL")
        day = str(utc_day)
        if len(day) != 10:
            raise ValueError("lineage randomization requires YYYY-MM-DD UTC day")
        uid = str(pre_assignment_lineage_uid).strip()
        if not uid:
            raise ValueError("lineage randomization requires a pre-assignment uid")
        if not str(self.family_id).strip():
            raise ValueError("lineage randomization requires a frozen family id")
        stratum = f"{day}|{normalized_side}"
        assignment_u = self._uniform(
            stratum,
            uid,
            "assignment_u",
        )
        action = (
            LINEAGE_CONTROL_ACTION
            if assignment_u < 0.5
            else LINEAGE_CANDIDATE_ACTION
        )
        return action, assignment_u, stratum


def normalize_consecutive_reset_policy(
    value: object,
    *,
    legacy_reset_on_expiry: object | None = None,
    require_explicit: bool = False,
) -> str:
    """Resolve the reset policy while preserving frozen historical replay.

    New formal replay must provide ``value``.  The legacy boolean is accepted
    only by non-formal or explicitly historical callers.
    """

    raw = str(value or "").strip().lower()
    if raw:
        if raw not in VALID_CONSECUTIVE_RESET_POLICIES:
            raise ValueError(
                "fill cooldown reset policy must be one of "
                f"{sorted(VALID_CONSECUTIVE_RESET_POLICIES)}, got {raw!r}"
            )
        if legacy_reset_on_expiry is not None:
            legacy = (
                RESET_OPPOSITE_FILL_OR_EXPIRY
                if bool(legacy_reset_on_expiry)
                else RESET_OPPOSITE_FILL_ONLY
            )
            if legacy != raw:
                raise ValueError(
                    "fill cooldown reset policy conflicts with the legacy "
                    "fill_cooldown_reset_consec_on_expiry value"
                )
        return raw
    if require_explicit:
        raise ValueError(
            "formal replay requires fill_cooldown_consecutive_reset_policy"
        )
    if legacy_reset_on_expiry is not None:
        return (
            RESET_OPPOSITE_FILL_OR_EXPIRY
            if bool(legacy_reset_on_expiry)
            else RESET_OPPOSITE_FILL_ONLY
        )
    # Preserve the historical exploratory replay default. Live configuration
    # always carries an explicit policy and is therefore unaffected.
    return RESET_OPPOSITE_FILL_OR_EXPIRY


def reset_policy_clears_on_expiry(policy: str) -> bool:
    return (
        normalize_consecutive_reset_policy(policy)
        == RESET_OPPOSITE_FILL_OR_EXPIRY
    )


def update_same_side_fill_units(
    *,
    side: str,
    fill_qty: float,
    order_size: float,
    lot_size: float,
    buy_units: float,
    sell_units: float,
) -> tuple[float, float, float]:
    """Apply one fill using the exact live quantity-unit semantics."""

    normalized_side = str(side).upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise ValueError(f"invalid fill side: {side!r}")
    denominator = max(float(order_size), float(lot_size))
    if not math.isfinite(denominator) or denominator <= 0.0:
        raise ValueError("order_size/lot_size denominator must be positive")
    qty = float(fill_qty)
    if not math.isfinite(qty) or qty <= 0.0:
        raise ValueError("fill_qty must be positive and finite")
    units = qty / denominator
    if normalized_side == "BUY":
        return max(0.0, float(buy_units)) + units, 0.0, units
    return 0.0, max(0.0, float(sell_units)) + units, units


def cooldown_duration_after_same_side_fill(
    *,
    previous_fill_ts_ms: int,
    previous_cooldown_ms: float,
    current_fill_ts_ms: int,
    new_cooldown_ms: float,
) -> float:
    """Return the duration to store relative to the current same-side fill.

    Live stores an absolute cooldown deadline. Replay stores a fill timestamp
    plus a duration. A reducing fill with a zero configured cooldown must not
    erase or restart an already-active add cooldown, so its remaining duration
    is rebased onto the current fill timestamp.
    """

    previous_ts = int(previous_fill_ts_ms)
    current_ts = int(current_fill_ts_ms)
    previous_duration = float(previous_cooldown_ms)
    new_duration = float(new_cooldown_ms)
    if current_ts < previous_ts:
        raise ValueError("same-side fill timestamps must be non-decreasing")
    if not math.isfinite(previous_duration) or previous_duration < 0.0:
        raise ValueError("previous_cooldown_ms must be finite and non-negative")
    if not math.isfinite(new_duration) or new_duration < 0.0:
        raise ValueError("new_cooldown_ms must be finite and non-negative")
    if new_duration > 0.0:
        return new_duration
    previous_deadline = float(previous_ts) + previous_duration
    return max(0.0, previous_deadline - float(current_ts))


def price_variance_to_bps2_rate(
    sigma_sq_price_per_s: float,
    mid_price: float,
) -> float:
    """Convert absolute price variance rate to squared-bps per second."""

    sigma_sq = float(sigma_sq_price_per_s)
    mid = float(mid_price)
    if not math.isfinite(sigma_sq) or sigma_sq < 0.0:
        raise ValueError("sigma_sq_price_per_s must be finite and non-negative")
    if not math.isfinite(mid) or mid <= 0.0:
        raise ValueError("mid_price must be positive and finite")
    return 1.0e8 * sigma_sq / (mid * mid)


@dataclass(frozen=True)
class CausalVarianceSample:
    feature_ready_ts_ms: int
    mid_price: float
    sigma_sq_price_per_s: float
    valid: bool = True


@dataclass
class VarianceTimeEpisodeState:
    side: str
    episode_start_ts_ms: int
    consecutive_same_side_fill_units: float
    budget_bps2: float
    accumulated_qv_bps2: float = 0.0
    last_feature_ready_ts_ms: int = 0
    stale_frozen_ms: float = 0.0

    SCHEMA_VERSION = "narrowgate_variance_time_episode_state.v1"

    def snapshot(self) -> dict[str, object]:
        return {"schema_version": self.SCHEMA_VERSION, **asdict(self)}

    @classmethod
    def restore(cls, payload: Mapping[str, object]) -> VarianceTimeEpisodeState:
        if str(payload.get("schema_version", "")) != cls.SCHEMA_VERSION:
            raise ValueError("unsupported variance-time episode state schema")
        side = str(payload.get("side", "")).upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError("variance-time episode side must be BUY or SELL")
        return cls(
            side=side,
            episode_start_ts_ms=int(payload["episode_start_ts_ms"]),
            consecutive_same_side_fill_units=float(
                payload["consecutive_same_side_fill_units"]
            ),
            budget_bps2=float(payload["budget_bps2"]),
            accumulated_qv_bps2=float(payload.get("accumulated_qv_bps2", 0.0)),
            last_feature_ready_ts_ms=int(
                payload.get("last_feature_ready_ts_ms", 0)
            ),
            stale_frozen_ms=float(payload.get("stale_frozen_ms", 0.0)),
        )


@dataclass(frozen=True)
class VarianceTimeRearmResult:
    rearm_ts_ms: int | None
    rearm_elapsed_ms: float | None
    reason: str
    accumulated_qv_bps2: float
    stale_frozen_ms: float
    valid_interval_ms: float
    budget_reached_ts_ms: float | None


@dataclass
class OnlineVarianceTimeEpisode:
    """Path-dependent variance clock used by authoritative replay.

    The state is intentionally independent of PnL and future fills. A new
    exposure-increasing fill restarts it with a frozen budget; reducing fills
    leave the active clock unchanged, while an opposite-side fill discards it.
    """

    side: str
    episode_start_ts_ms: int
    baseline_ready_ts_ms: int
    consecutive_same_side_fill_units: float
    budget_bps2: float
    minimum_wall_time_ms: int
    maximum_wall_time_ms: int
    max_feature_age_ms: int
    accumulated_qv_bps2: float = 0.0
    valid_interval_ms: float = 0.0
    stale_frozen_ms: float = 0.0
    last_update_ts_ms: int = 0
    budget_reached_ts_ms: float | None = None
    ready_ts_ms: int | None = None
    release_reason: str = ""

    SCHEMA_VERSION = "narrowgate_online_variance_time_episode.v1"

    @property
    def active(self) -> bool:
        return self.ready_ts_ms is None

    def snapshot(self) -> dict[str, object]:
        return {"schema_version": self.SCHEMA_VERSION, **asdict(self)}

    @classmethod
    def restore(cls, payload: Mapping[str, object]) -> OnlineVarianceTimeEpisode:
        if str(payload.get("schema_version", "")) != cls.SCHEMA_VERSION:
            raise ValueError("unsupported online variance-time state schema")
        side = str(payload.get("side", "")).upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError("online variance-time side must be BUY or SELL")
        return cls(
            side=side,
            episode_start_ts_ms=int(payload["episode_start_ts_ms"]),
            baseline_ready_ts_ms=int(payload["baseline_ready_ts_ms"]),
            consecutive_same_side_fill_units=float(
                payload["consecutive_same_side_fill_units"]
            ),
            budget_bps2=float(payload["budget_bps2"]),
            minimum_wall_time_ms=int(payload["minimum_wall_time_ms"]),
            maximum_wall_time_ms=int(payload["maximum_wall_time_ms"]),
            max_feature_age_ms=int(payload["max_feature_age_ms"]),
            accumulated_qv_bps2=float(
                payload.get("accumulated_qv_bps2", 0.0)
            ),
            valid_interval_ms=float(payload.get("valid_interval_ms", 0.0)),
            stale_frozen_ms=float(payload.get("stale_frozen_ms", 0.0)),
            last_update_ts_ms=int(payload.get("last_update_ts_ms", 0)),
            budget_reached_ts_ms=(
                float(payload["budget_reached_ts_ms"])
                if payload.get("budget_reached_ts_ms") is not None
                else None
            ),
            ready_ts_ms=(
                int(payload["ready_ts_ms"])
                if payload.get("ready_ts_ms") is not None
                else None
            ),
            release_reason=str(payload.get("release_reason", "")),
        )


class CausalVarianceRateStream:
    """Completed-bucket bps-squared rate stream for online replay.

    Each sample becomes visible only at ``feature_ready_ts_ms``. Between
    samples the latest valid observation is piecewise constant until its
    explicit freshness limit; uncovered time freezes the variance clock.
    """

    def __init__(
        self,
        feature_ready_ts_ms: Sequence[int],
        rate_bps2_per_s: Sequence[float],
        valid: Sequence[bool],
    ) -> None:
        ready = tuple(int(value) for value in feature_ready_ts_ms)
        rates = tuple(float(value) for value in rate_bps2_per_s)
        flags = tuple(bool(value) for value in valid)
        if not ready or len(ready) != len(rates) or len(ready) != len(flags):
            raise ValueError("variance-rate stream arrays must be non-empty and aligned")
        if any(
            right <= left
            for left, right in zip(ready, ready[1:], strict=False)
        ):
            raise ValueError("variance-rate feature-ready timestamps must increase")
        for rate, flag in zip(rates, flags, strict=True):
            if flag and (not math.isfinite(rate) or rate < 0.0):
                raise ValueError("valid variance rates must be finite and non-negative")
        self.feature_ready_ts_ms = ready
        self.rate_bps2_per_s = rates
        self.valid = flags

    def advance(
        self,
        episode: OnlineVarianceTimeEpisode,
        now_ts_ms: int,
    ) -> OnlineVarianceTimeEpisode:
        """Advance one episode without consulting future samples or outcomes."""

        if not episode.active:
            return episode
        now = int(now_ts_ms)
        start = int(episode.episode_start_ts_ms)
        if now < start:
            raise ValueError("variance-time replay clock regressed before episode start")
        deadline = start + int(episode.maximum_wall_time_ms)
        target = min(now, deadline)
        cursor = max(start, int(episode.last_update_ts_ms or start))
        if target < cursor:
            raise ValueError("variance-time replay clock regressed")

        ready = self.feature_ready_ts_ms
        rates = self.rate_bps2_per_s
        flags = self.valid
        while cursor < target:
            index = bisect_right(ready, cursor) - 1
            next_ready = (
                ready[index + 1]
                if index + 1 < len(ready)
                else target
            )
            interval_end = min(target, max(cursor + 1, next_ready))
            if index < 0:
                interval_end = min(target, ready[0])
                if interval_end <= cursor:
                    interval_end = target
                episode.stale_frozen_ms += float(interval_end - cursor)
                cursor = interval_end
                continue

            freshness_end = ready[index] + int(episode.max_feature_age_ms)
            valid_end = min(interval_end, freshness_end)
            if flags[index] and valid_end > cursor:
                duration_ms = float(valid_end - cursor)
                rate = rates[index]
                before = float(episode.accumulated_qv_bps2)
                increment = rate * duration_ms / 1000.0
                crossing_tolerance = max(
                    1e-12,
                    abs(float(episode.budget_bps2)) * 1e-12,
                )
                if (
                    episode.budget_reached_ts_ms is None
                    and rate > 0.0
                    and before < episode.budget_bps2 <= before + increment
                    + crossing_tolerance
                ):
                    crossing = (
                        float(cursor)
                        + max(0.0, episode.budget_bps2 - before)
                        / rate
                        * 1000.0
                    )
                    episode.budget_reached_ts_ms = min(
                        float(valid_end),
                        max(float(cursor), crossing),
                    )
                episode.accumulated_qv_bps2 = before + increment
                episode.valid_interval_ms += duration_ms
            if interval_end > valid_end:
                episode.stale_frozen_ms += float(interval_end - max(cursor, valid_end))
            cursor = interval_end

        episode.last_update_ts_ms = target
        if episode.budget_reached_ts_ms is not None:
            release = max(
                float(start + int(episode.minimum_wall_time_ms)),
                float(episode.budget_reached_ts_ms),
            )
            if now >= release:
                episode.ready_ts_ms = int(math.ceil(release))
                episode.release_reason = "variance_budget"
                return episode
        if now >= deadline:
            episode.ready_ts_ms = deadline
            episode.release_reason = "maximum_wall_time"
        return episode


def start_online_variance_time_episode(
    *,
    side: str,
    episode_start_ts_ms: int,
    baseline_cooldown_ms: float,
    consecutive_same_side_fill_units: float,
    reference_rate_bps2_per_s: float,
    minimum_wall_time_ms: int,
    maximum_wall_time_ms: int,
    max_feature_age_ms: int,
) -> OnlineVarianceTimeEpisode:
    """Create a frozen-budget episode equivalent to the 85n baseline clock."""

    normalized_side = str(side).upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise ValueError("variance-time episode side must be BUY or SELL")
    cooldown_ms = float(baseline_cooldown_ms)
    reference_rate = float(reference_rate_bps2_per_s)
    units = float(consecutive_same_side_fill_units)
    if not math.isfinite(cooldown_ms) or cooldown_ms <= 0.0:
        raise ValueError("baseline cooldown must be positive")
    if not math.isfinite(reference_rate) or reference_rate <= 0.0:
        raise ValueError("reference variance rate must be positive")
    if not math.isfinite(units) or units <= 0.0:
        raise ValueError("consecutive fill units must be positive")
    if maximum_wall_time_ms < minimum_wall_time_ms or maximum_wall_time_ms <= 0:
        raise ValueError("variance-time wall-time bounds are invalid")
    start = int(episode_start_ts_ms)
    return OnlineVarianceTimeEpisode(
        side=normalized_side,
        episode_start_ts_ms=start,
        baseline_ready_ts_ms=int(math.ceil(start + cooldown_ms)),
        consecutive_same_side_fill_units=units,
        budget_bps2=reference_rate * cooldown_ms / 1000.0,
        minimum_wall_time_ms=int(minimum_wall_time_ms),
        maximum_wall_time_ms=int(maximum_wall_time_ms),
        max_feature_age_ms=int(max_feature_age_ms),
        last_update_ts_ms=start,
    )


def integrate_variance_time_episode(
    samples: Sequence[CausalVarianceSample] | Iterable[CausalVarianceSample],
    *,
    episode_start_ts_ms: int,
    budget_bps2: float,
    minimum_wall_time_ms: int,
    maximum_wall_time_ms: int,
    max_feature_age_ms: int,
    censor_ts_ms: int | None = None,
) -> VarianceTimeRearmResult:
    """Integrate a causal piecewise-constant variance clock.

    Invalid or stale intervals freeze the clock. The maximum wall-time bound is
    a liveness release, not an imputed variance observation.
    """

    start = int(episode_start_ts_ms)
    min_ms = max(0, int(minimum_wall_time_ms))
    max_ms = int(maximum_wall_time_ms)
    max_age = max(0, int(max_feature_age_ms))
    budget = float(budget_bps2)
    if max_ms < min_ms or max_ms <= 0:
        raise ValueError("maximum wall time must be positive and >= minimum")
    if not math.isfinite(budget) or budget <= 0.0:
        raise ValueError("variance-time budget must be positive and finite")
    deadline = start + max_ms
    censor = int(censor_ts_ms) if censor_ts_ms is not None else deadline
    stop = min(deadline, censor)

    ordered = sorted(samples, key=lambda row: int(row.feature_ready_ts_ms))
    qv = 0.0
    stale_ms = 0.0
    valid_ms = 0.0
    budget_hit: float | None = None
    covered_until = start
    for index, sample in enumerate(ordered):
        ready = int(sample.feature_ready_ts_ms)
        if ready >= stop:
            break
        interval_start = max(start, ready)
        next_ready = (
            int(ordered[index + 1].feature_ready_ts_ms)
            if index + 1 < len(ordered)
            else stop
        )
        interval_end = min(stop, next_ready)
        if interval_end <= interval_start:
            continue
        if interval_start > covered_until:
            stale_ms += float(interval_start - covered_until)
        fresh_end = min(interval_end, ready + max_age)
        valid_end = max(interval_start, fresh_end)
        interval_valid = (
            bool(sample.valid)
            and ready <= interval_start
            and math.isfinite(float(sample.mid_price))
            and float(sample.mid_price) > 0.0
            and math.isfinite(float(sample.sigma_sq_price_per_s))
            and float(sample.sigma_sq_price_per_s) >= 0.0
            and valid_end > interval_start
        )
        if interval_valid:
            duration_ms = float(valid_end - interval_start)
            rate = price_variance_to_bps2_rate(
                sample.sigma_sq_price_per_s, sample.mid_price
            )
            increment = rate * duration_ms / 1000.0
            if rate > 0.0 and qv + increment >= budget and budget_hit is None:
                budget_hit = interval_start + (budget - qv) / rate * 1000.0
            qv += increment
            valid_ms += duration_ms
            if interval_end > valid_end:
                stale_ms += float(interval_end - valid_end)
        else:
            stale_ms += float(interval_end - interval_start)
        covered_until = max(covered_until, interval_end)
        if budget_hit is not None:
            release = max(float(start + min_ms), budget_hit)
            if release <= stop:
                return VarianceTimeRearmResult(
                    rearm_ts_ms=int(math.ceil(release)),
                    rearm_elapsed_ms=float(release - start),
                    reason="variance_budget",
                    accumulated_qv_bps2=float(qv),
                    stale_frozen_ms=stale_ms,
                    valid_interval_ms=valid_ms,
                    budget_reached_ts_ms=float(budget_hit),
                )

    if covered_until < stop:
        stale_ms += float(stop - covered_until)

    if deadline <= censor:
        return VarianceTimeRearmResult(
            rearm_ts_ms=deadline,
            rearm_elapsed_ms=float(max_ms),
            reason="maximum_wall_time",
            accumulated_qv_bps2=float(qv),
            stale_frozen_ms=stale_ms,
            valid_interval_ms=valid_ms,
            budget_reached_ts_ms=budget_hit,
        )
    return VarianceTimeRearmResult(
        rearm_ts_ms=None,
        rearm_elapsed_ms=None,
        reason="censored",
        accumulated_qv_bps2=float(qv),
        stale_frozen_ms=stale_ms,
        valid_interval_ms=valid_ms,
        budget_reached_ts_ms=budget_hit,
    )
