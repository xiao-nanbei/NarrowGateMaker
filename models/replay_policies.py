"""Replay-only local action randomization for offline policy evaluation.

The module has no live wiring.  It defines a small, pre-registered action set
whose prices can be executed by the authoritative Python tick replay while
leaving order size, inventory limits, and the reducing quote unchanged.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass

from strategy.state_conditioned_quote_policy import (
    LOCAL_QUOTE_ACTIONS,
    LocalActionQuote,
    apply_local_add_action,
)

LOCAL_ACTIONS = LOCAL_QUOTE_ACTIONS

SELL_ADD_SKIP_ACTIONS = (
    "baseline",
    "skip_one_add_cycle",
)

CAMPAIGN_STOP_ADD_ACTIONS = (
    "baseline",
    "stop_add_until_flat",
)

STATE_CONDITIONED_REARM_ACTIONS = (
    "baseline_rearm",
    "continue_block_until_recovery",
)


@dataclass(frozen=True)
class StateConditionedRearmSpec:
    """Frozen causal entry/exit thresholds for post-cooldown add rearm.

    Entry is deliberately stricter than exit.  This hysteresis prevents a
    single noisy 1-second flow observation from turning a state episode into
    the already-closed one-cycle skip family.
    """

    entry_min_current_adverse_bps: float = 2.0
    entry_min_adverse_flow_1s: float = 0.0
    entry_min_adverse_flow_5s: float = 0.0
    entry_min_adverse_flow_path: float = 0.0
    entry_max_refill_recovery_ratio: float = 0.75
    entry_max_refill_current_vs_start_ratio: float = 1.0
    entry_max_price_recovery_ratio: float = 0.50
    entry_max_microprice_recovery_ratio: float = 0.50
    max_book_age_ms: float = 2_000.0
    exit_max_current_adverse_bps: float = 0.50
    exit_max_adverse_flow_5s: float = 0.0
    exit_max_adverse_flow_path: float = 0.0
    exit_min_refill_recovery_ratio: float = 1.0
    exit_min_refill_current_vs_start_ratio: float = 1.0
    exit_min_price_recovery_ratio: float = 0.75
    exit_min_microprice_recovery_ratio: float = 0.75


@dataclass(frozen=True)
class StateConditionedRearmDecision:
    entry_active: bool
    exit_active: bool
    data_valid: bool
    adverse_move_active: bool
    persistent_flow_active: bool
    weak_refill_active: bool
    weak_recovery_active: bool
    exit_reason: str


@dataclass(frozen=True)
class RecoveryEventSpec:
    """Frozen local recovery event for a post-cooldown add decision.

    ``refill_recovery_ratio`` measures replenishment from the post-fill depth
    trough, while ``refill_current_vs_start_ratio`` measures whether visible
    same-side queue/depth has recovered to its pre-shock level.  Keeping these
    as separate components avoids treating one burst of refill as full queue
    repair.
    """

    score_threshold: float
    max_book_age_ms: float = 2_000.0
    adverse_flow_reference_floor: float = 0.05
    component_epsilon: float = 1e-6

    def __post_init__(self) -> None:
        if not 0.0 < float(self.score_threshold) <= 1.0:
            raise ValueError("recovery score threshold must be in (0, 1]")
        if float(self.max_book_age_ms) <= 0.0:
            raise ValueError("max book age must be positive")
        if float(self.adverse_flow_reference_floor) <= 0.0:
            raise ValueError("adverse flow reference floor must be positive")
        if not 0.0 < float(self.component_epsilon) < 1.0:
            raise ValueError("component epsilon must be in (0, 1)")


@dataclass(frozen=True)
class RecoveryEventDecision:
    data_valid: bool
    shock_decay_score: float
    refill_score: float
    microprice_recovery_score: float
    queue_recovery_score: float
    recovery_score: float
    recovery_event: bool
    hold_add_active: bool


def evaluate_recovery_event(
    features: Mapping[str, float],
    spec: RecoveryEventSpec,
) -> RecoveryEventDecision:
    """Evaluate a causal four-path recovery event without an elapsed-time exit.

    The geometric mean makes every path consequential while remaining
    continuous enough for a support-only quantile preflight.  Invalid or stale
    state always falls back to the baseline at entry; an already-active replay
    episode separately refuses to exit until a valid recovery event appears.
    """

    def value(name: str) -> float:
        try:
            result = float(features.get(name, 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return result if math.isfinite(result) else 0.0

    data_valid = bool(
        value("path_feature_valid") >= 0.5
        and value("path_l2_snapshot_count") >= 2.0
        and value("path_book_age_ms") <= float(spec.max_book_age_ms)
    )

    adverse_flow_1s = max(
        0.0, value("shock_adverse_flow_imbalance_1s")
    )
    adverse_flow_reference = max(
        max(0.0, value("shock_adverse_flow_imbalance_5s")),
        max(0.0, value("shock_adverse_flow_imbalance_since_fill")),
        float(spec.adverse_flow_reference_floor),
    )
    shock_decay_score = min(
        1.0,
        max(0.0, 1.0 - adverse_flow_1s / adverse_flow_reference),
    )
    refill_score = min(
        1.0, max(0.0, value("refill_recovery_ratio"))
    )
    microprice_recovery_score = min(
        1.0, max(0.0, value("recovery_microprice_ratio"))
    )
    queue_recovery_score = min(
        1.0, max(0.0, value("refill_current_vs_start_ratio"))
    )
    epsilon = float(spec.component_epsilon)
    recovery_score = math.exp(
        0.25
        * sum(
            math.log(max(epsilon, component))
            for component in (
                shock_decay_score,
                refill_score,
                microprice_recovery_score,
                queue_recovery_score,
            )
        )
    )
    recovery_event = bool(
        data_valid and recovery_score >= float(spec.score_threshold)
    )
    return RecoveryEventDecision(
        data_valid=data_valid,
        shock_decay_score=float(shock_decay_score),
        refill_score=float(refill_score),
        microprice_recovery_score=float(microprice_recovery_score),
        queue_recovery_score=float(queue_recovery_score),
        recovery_score=float(recovery_score),
        recovery_event=recovery_event,
        hold_add_active=bool(data_valid and not recovery_event),
    )


def normalize_state_conditioned_rearm_probabilities(
    raw: Mapping[str, float] | str | None = None,
) -> dict[str, float]:
    """Return the frozen 50/50 rearm behavior policy with exact overlap."""

    if isinstance(raw, str):
        raw = json.loads(raw)
    values = (
        {
            "baseline_rearm": 0.5,
            "continue_block_until_recovery": 0.5,
        }
        if raw is None
        else {
            str(key).strip().lower(): float(value)
            for key, value in raw.items()
        }
    )
    unknown = sorted(set(values) - set(STATE_CONDITIONED_REARM_ACTIONS))
    missing = sorted(set(STATE_CONDITIONED_REARM_ACTIONS) - set(values))
    if unknown:
        raise ValueError(f"unknown state-conditioned rearm actions: {unknown}")
    if missing:
        raise ValueError(
            f"state-conditioned rearm probability vector is missing: {missing}"
        )
    if any(
        not math.isfinite(value) or value <= 0.0
        for value in values.values()
    ):
        raise ValueError(
            "every state-conditioned rearm action must have finite positive support"
        )
    if not math.isclose(
        sum(values.values()), 1.0, abs_tol=1e-10, rel_tol=0.0
    ):
        raise ValueError("state-conditioned rearm probabilities must sum to one")
    return {
        action: float(values[action])
        for action in STATE_CONDITIONED_REARM_ACTIONS
    }


def choose_state_conditioned_rearm_action(
    rng,
    probabilities: Mapping[str, float],
) -> tuple[str, float]:
    """Sample one post-cooldown action and return its logged random draw."""

    draw = float(rng.random())
    cumulative = 0.0
    for action in STATE_CONDITIONED_REARM_ACTIONS:
        cumulative += float(probabilities[action])
        if draw < cumulative:
            return action, draw
    return STATE_CONDITIONED_REARM_ACTIONS[-1], draw


def evaluate_state_conditioned_rearm(
    features: Mapping[str, float],
    spec: StateConditionedRearmSpec | None = None,
) -> StateConditionedRearmDecision:
    """Evaluate only decision-time-visible adverse/refill/recovery state."""

    cfg = spec or StateConditionedRearmSpec()

    def value(name: str) -> float:
        try:
            result = float(features.get(name, 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return result if math.isfinite(result) else 0.0

    data_valid = bool(
        value("path_feature_valid") >= 0.5
        and value("path_l2_snapshot_count") >= 2.0
        and value("path_book_age_ms") <= cfg.max_book_age_ms
    )
    adverse_move_active = bool(
        value("recovery_current_adverse_bps")
        >= cfg.entry_min_current_adverse_bps
    )
    persistent_flow_active = bool(
        value("shock_adverse_flow_imbalance_1s")
        > cfg.entry_min_adverse_flow_1s
        and value("shock_adverse_flow_imbalance_5s")
        > cfg.entry_min_adverse_flow_5s
        and value("shock_adverse_flow_imbalance_since_fill")
        > cfg.entry_min_adverse_flow_path
    )
    weak_refill_active = bool(
        value("refill_recovery_ratio")
        < cfg.entry_max_refill_recovery_ratio
        and value("refill_current_vs_start_ratio")
        < cfg.entry_max_refill_current_vs_start_ratio
    )
    weak_recovery_active = bool(
        value("recovery_price_ratio")
        < cfg.entry_max_price_recovery_ratio
        and value("recovery_microprice_ratio")
        < cfg.entry_max_microprice_recovery_ratio
    )
    entry_active = bool(
        data_valid
        and adverse_move_active
        and persistent_flow_active
        and weak_refill_active
        and weak_recovery_active
    )

    exit_reason = ""
    if data_valid:
        if (
            value("recovery_current_adverse_bps")
            <= cfg.exit_max_current_adverse_bps
        ):
            exit_reason = "price_recovered"
        elif (
            value("shock_adverse_flow_imbalance_5s")
            <= cfg.exit_max_adverse_flow_5s
            and value("shock_adverse_flow_imbalance_since_fill")
            <= cfg.exit_max_adverse_flow_path
        ):
            exit_reason = "adverse_flow_dissipated"
        elif (
            value("refill_recovery_ratio")
            >= cfg.exit_min_refill_recovery_ratio
            and value("refill_current_vs_start_ratio")
            >= cfg.exit_min_refill_current_vs_start_ratio
        ):
            exit_reason = "book_refilled"
        elif (
            value("recovery_price_ratio")
            >= cfg.exit_min_price_recovery_ratio
            and value("recovery_microprice_ratio")
            >= cfg.exit_min_microprice_recovery_ratio
        ):
            exit_reason = "price_and_microprice_recovered"

    return StateConditionedRearmDecision(
        entry_active=entry_active,
        exit_active=bool(exit_reason),
        data_valid=data_valid,
        adverse_move_active=adverse_move_active,
        persistent_flow_active=persistent_flow_active,
        weak_refill_active=weak_refill_active,
        weak_recovery_active=weak_recovery_active,
        exit_reason=exit_reason,
    )

QUEUE_VALUE_KEEP_CANCEL_ACTIONS = (
    "keep",
    "cancel_until_state_exit",
)

QUEUE_VALUE_CANCEL_REENTER_ACTIONS = (
    "keep",
    "cancel_then_baseline_reenter",
)


def normalize_action_probabilities(
    raw: Mapping[str, float] | str | None = None,
    *,
    baseline_probability: float = 0.90,
    allow_zero_support: bool = False,
) -> dict[str, float]:
    """Return a complete probability vector in canonical action order.

    Zero support is only allowed for a frozen sub-family. Inactive actions stay
    explicit in the trace schema, while every action that can be logged retains
    positive propensity.
    """

    if isinstance(raw, str):
        raw = json.loads(raw)
    if raw is None:
        candidate_probability = (1.0 - float(baseline_probability)) / 3.0
        values = {
            "baseline": float(baseline_probability),
            "prevent_over_widen": candidate_probability,
            "widen_1tick": candidate_probability,
            "recenter_1tick": candidate_probability,
        }
    else:
        values = {str(key).strip().lower(): float(value) for key, value in raw.items()}
        unknown = sorted(set(values) - set(LOCAL_ACTIONS))
        missing = sorted(set(LOCAL_ACTIONS) - set(values))
        if unknown:
            raise ValueError(f"unknown local replay actions: {unknown}")
        if missing:
            raise ValueError(f"action probability vector is missing: {missing}")
    if any(
        not math.isfinite(value)
        or value < 0.0
        or (not allow_zero_support and value == 0.0)
        for value in values.values()
    ):
        qualifier = "non-negative" if allow_zero_support else "positive"
        raise ValueError(
            f"every local replay action must have finite {qualifier} support"
        )
    if allow_zero_support and not any(value > 0.0 for value in values.values()):
        raise ValueError("at least one local replay action needs positive support")
    if not math.isclose(sum(values.values()), 1.0, abs_tol=1e-10, rel_tol=0.0):
        raise ValueError("local replay action probabilities must sum to one")
    return {action: float(values[action]) for action in LOCAL_ACTIONS}


def choose_action(rng, probabilities: Mapping[str, float]) -> tuple[str, float]:
    """Sample one action and return both the action and the random draw."""

    draw = float(rng.random())
    cumulative = 0.0
    for action in LOCAL_ACTIONS:
        cumulative += float(probabilities[action])
        if draw < cumulative:
            return action, draw
    return LOCAL_ACTIONS[-1], draw


def normalize_sell_add_skip_probabilities(
    raw: Mapping[str, float] | str | None = None,
) -> dict[str, float]:
    """Return the frozen SELL baseline/skip behavior vector."""

    if isinstance(raw, str):
        raw = json.loads(raw)
    values = (
        {"baseline": 0.5, "skip_one_add_cycle": 0.5}
        if raw is None
        else {
            str(key).strip().lower(): float(value)
            for key, value in raw.items()
        }
    )
    unknown = sorted(set(values) - set(SELL_ADD_SKIP_ACTIONS))
    missing = sorted(set(SELL_ADD_SKIP_ACTIONS) - set(values))
    if unknown:
        raise ValueError(f"unknown SELL add-skip actions: {unknown}")
    if missing:
        raise ValueError(f"SELL add-skip probability vector is missing: {missing}")
    if any(
        not math.isfinite(value) or value <= 0.0
        for value in values.values()
    ):
        raise ValueError(
            "every SELL add-skip action must have finite positive support"
        )
    if not math.isclose(
        sum(values.values()), 1.0, abs_tol=1e-10, rel_tol=0.0
    ):
        raise ValueError("SELL add-skip probabilities must sum to one")
    return {
        action: float(values[action])
        for action in SELL_ADD_SKIP_ACTIONS
    }


def choose_sell_add_skip_action(
    rng,
    probabilities: Mapping[str, float],
) -> tuple[str, float]:
    """Sample baseline or one-cycle skip with the logged random draw."""

    draw = float(rng.random())
    cumulative = 0.0
    for action in SELL_ADD_SKIP_ACTIONS:
        cumulative += float(probabilities[action])
        if draw < cumulative:
            return action, draw
    return SELL_ADD_SKIP_ACTIONS[-1], draw


def normalize_campaign_stop_add_probabilities(
    raw: Mapping[str, float] | str | None = None,
) -> dict[str, float]:
    """Return the frozen campaign-level add-permission behavior vector."""

    if isinstance(raw, str):
        raw = json.loads(raw)
    values = (
        {"baseline": 0.5, "stop_add_until_flat": 0.5}
        if raw is None
        else {
            str(key).strip().lower(): float(value)
            for key, value in raw.items()
        }
    )
    unknown = sorted(set(values) - set(CAMPAIGN_STOP_ADD_ACTIONS))
    missing = sorted(set(CAMPAIGN_STOP_ADD_ACTIONS) - set(values))
    if unknown:
        raise ValueError(f"unknown campaign stop-add actions: {unknown}")
    if missing:
        raise ValueError(
            f"campaign stop-add probability vector is missing: {missing}"
        )
    if any(
        not math.isfinite(value) or value <= 0.0
        for value in values.values()
    ):
        raise ValueError(
            "every campaign stop-add action must have finite positive support"
        )
    if not math.isclose(
        sum(values.values()), 1.0, abs_tol=1e-10, rel_tol=0.0
    ):
        raise ValueError("campaign stop-add probabilities must sum to one")
    return {
        action: float(values[action])
        for action in CAMPAIGN_STOP_ADD_ACTIONS
    }


def choose_campaign_stop_add_action(
    rng,
    probabilities: Mapping[str, float],
) -> tuple[str, float]:
    """Sample campaign-level add permission with the logged random draw."""

    draw = float(rng.random())
    cumulative = 0.0
    for action in CAMPAIGN_STOP_ADD_ACTIONS:
        cumulative += float(probabilities[action])
        if draw < cumulative:
            return action, draw
    return CAMPAIGN_STOP_ADD_ACTIONS[-1], draw


def normalize_queue_value_probabilities(
    raw: Mapping[str, float] | str | None = None,
) -> dict[str, float]:
    """Return the frozen keep/cancel behavior vector with exact overlap."""

    if isinstance(raw, str):
        raw = json.loads(raw)
    values = (
        {"keep": 0.5, "cancel_until_state_exit": 0.5}
        if raw is None
        else {
            str(key).strip().lower(): float(value)
            for key, value in raw.items()
        }
    )
    unknown = sorted(set(values) - set(QUEUE_VALUE_KEEP_CANCEL_ACTIONS))
    missing = sorted(set(QUEUE_VALUE_KEEP_CANCEL_ACTIONS) - set(values))
    if unknown:
        raise ValueError(f"unknown queue-value actions: {unknown}")
    if missing:
        raise ValueError(f"queue-value probability vector is missing: {missing}")
    if any(
        not math.isfinite(value) or value <= 0.0
        for value in values.values()
    ):
        raise ValueError(
            "every queue-value action must have finite positive support"
        )
    if not math.isclose(
        sum(values.values()), 1.0, abs_tol=1e-10, rel_tol=0.0
    ):
        raise ValueError("queue-value action probabilities must sum to one")
    return {
        action: float(values[action])
        for action in QUEUE_VALUE_KEEP_CANCEL_ACTIONS
    }


def choose_queue_value_action(
    rng,
    probabilities: Mapping[str, float],
) -> tuple[str, float]:
    """Sample keep/cancel and return the logged random draw."""

    draw = float(rng.random())
    cumulative = 0.0
    for action in QUEUE_VALUE_KEEP_CANCEL_ACTIONS:
        cumulative += float(probabilities[action])
        if draw < cumulative:
            return action, draw
    return QUEUE_VALUE_KEEP_CANCEL_ACTIONS[-1], draw


def normalize_queue_value_cancel_reenter_probabilities(
    raw: Mapping[str, float] | str | None = None,
) -> dict[str, float]:
    """Return the frozen keep/cancel-reenter vector with exact overlap."""

    if isinstance(raw, str):
        raw = json.loads(raw)
    values = (
        {"keep": 0.5, "cancel_then_baseline_reenter": 0.5}
        if raw is None
        else {
            str(key).strip().lower(): float(value)
            for key, value in raw.items()
        }
    )
    unknown = sorted(set(values) - set(QUEUE_VALUE_CANCEL_REENTER_ACTIONS))
    missing = sorted(set(QUEUE_VALUE_CANCEL_REENTER_ACTIONS) - set(values))
    if unknown:
        raise ValueError(f"unknown queue-value cancel-reenter actions: {unknown}")
    if missing:
        raise ValueError(
            f"queue-value cancel-reenter probability vector is missing: {missing}"
        )
    if any(
        not math.isfinite(value) or value <= 0.0
        for value in values.values()
    ):
        raise ValueError(
            "every queue-value cancel-reenter action must have finite positive support"
        )
    if not math.isclose(
        sum(values.values()), 1.0, abs_tol=1e-10, rel_tol=0.0
    ):
        raise ValueError(
            "queue-value cancel-reenter probabilities must sum to one"
        )
    return {
        action: float(values[action])
        for action in QUEUE_VALUE_CANCEL_REENTER_ACTIONS
    }


def choose_queue_value_cancel_reenter_action(
    rng,
    probabilities: Mapping[str, float],
) -> tuple[str, float]:
    """Sample keep/cancel-reenter and return the logged random draw."""

    draw = float(rng.random())
    cumulative = 0.0
    for action in QUEUE_VALUE_CANCEL_REENTER_ACTIONS:
        cumulative += float(probabilities[action])
        if draw < cumulative:
            return action, draw
    return QUEUE_VALUE_CANCEL_REENTER_ACTIONS[-1], draw


SAFE_ADD_REARM_ACTIONS = (
    "r0_block",
    "r1_rearm",
    "r2_rearm_widen_1tick",
)


@dataclass(frozen=True)
class SafeAddRearmQuote:
    action: str
    baseline_price: float
    selected_price: float
    delta_ticks: float
    allow_post: bool
    effective: bool
    clamp_reason: str


def normalize_safe_add_rearm_probabilities(
    raw: Mapping[str, float] | str | None = None,
    *,
    baseline_probability: float = 0.80,
) -> dict[str, float]:
    """Return a complete probability vector with overlap for all actions."""
    if isinstance(raw, str):
        raw = json.loads(raw)
    if raw is None:
        candidate_probability = (1.0 - float(baseline_probability)) / 2.0
        values = {
            "r0_block": float(baseline_probability),
            "r1_rearm": candidate_probability,
            "r2_rearm_widen_1tick": candidate_probability,
        }
    else:
        values = {str(key).strip().lower(): float(value) for key, value in raw.items()}
        unknown = sorted(set(values) - set(SAFE_ADD_REARM_ACTIONS))
        missing = sorted(set(SAFE_ADD_REARM_ACTIONS) - set(values))
        if unknown:
            raise ValueError(f"unknown safe-add rearm actions: {unknown}")
        if missing:
            raise ValueError(f"safe-add rearm probability vector is missing: {missing}")
    if any(not math.isfinite(value) or value <= 0.0 for value in values.values()):
        raise ValueError("every safe-add rearm action must have finite positive support")
    if not math.isclose(sum(values.values()), 1.0, abs_tol=1e-10, rel_tol=0.0):
        raise ValueError("safe-add rearm action probabilities must sum to one")
    return {action: float(values[action]) for action in SAFE_ADD_REARM_ACTIONS}


def choose_safe_add_rearm_action(
    rng,
    probabilities: Mapping[str, float],
) -> tuple[str, float]:
    draw = float(rng.random())
    cumulative = 0.0
    for action in SAFE_ADD_REARM_ACTIONS:
        cumulative += float(probabilities[action])
        if draw < cumulative:
            return action, draw
    return SAFE_ADD_REARM_ACTIONS[-1], draw


def apply_safe_add_rearm_action(
    *,
    side: str,
    action: str,
    baseline_price: float,
    other_side_price: float,
    tick: float,
    max_pair_spread: float,
) -> SafeAddRearmQuote:
    """Apply R0/R1/R2 without changing size, reducing side, or risk limits."""
    side = str(side).upper()
    action = str(action).strip().lower()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported side: {side}")
    if action not in SAFE_ADD_REARM_ACTIONS:
        raise ValueError(f"unsupported safe-add rearm action: {action}")
    if tick <= 0.0 or baseline_price <= 0.0:
        raise ValueError("tick and baseline price must be positive")
    if action == "r0_block":
        return SafeAddRearmQuote(
            action=action,
            baseline_price=float(baseline_price),
            selected_price=float(baseline_price),
            delta_ticks=0.0,
            allow_post=False,
            effective=False,
            clamp_reason="fill_cooldown",
        )

    selected = float(baseline_price)
    if action == "r2_rearm_widen_1tick":
        selected += -tick if side == "BUY" else tick
    reasons: list[str] = []
    if max_pair_spread > 0.0 and other_side_price > 0.0:
        bounded = (
            max(selected, float(other_side_price) - max_pair_spread)
            if side == "BUY"
            else min(selected, float(other_side_price) + max_pair_spread)
        )
        if not math.isclose(bounded, selected, abs_tol=tick * 0.01, rel_tol=0.0):
            reasons.append("pair_spread_cap")
        selected = bounded
    selected = round(selected / tick) * tick
    return SafeAddRearmQuote(
        action=action,
        baseline_price=float(baseline_price),
        selected_price=float(selected),
        delta_ticks=float((selected - baseline_price) / tick),
        allow_post=True,
        effective=True,
        clamp_reason="|".join(reasons) or "none",
    )


__all__ = [
    "CAMPAIGN_STOP_ADD_ACTIONS",
    "LOCAL_ACTIONS",
    "LocalActionQuote",
    "QUEUE_VALUE_CANCEL_REENTER_ACTIONS",
    "QUEUE_VALUE_KEEP_CANCEL_ACTIONS",
    "RecoveryEventDecision",
    "RecoveryEventSpec",
    "SAFE_ADD_REARM_ACTIONS",
    "SELL_ADD_SKIP_ACTIONS",
    "STATE_CONDITIONED_REARM_ACTIONS",
    "SafeAddRearmQuote",
    "StateConditionedRearmDecision",
    "StateConditionedRearmSpec",
    "apply_local_add_action",
    "apply_safe_add_rearm_action",
    "choose_action",
    "choose_campaign_stop_add_action",
    "choose_queue_value_cancel_reenter_action",
    "choose_queue_value_action",
    "choose_safe_add_rearm_action",
    "choose_sell_add_skip_action",
    "choose_state_conditioned_rearm_action",
    "evaluate_recovery_event",
    "evaluate_state_conditioned_rearm",
    "normalize_action_probabilities",
    "normalize_campaign_stop_add_probabilities",
    "normalize_queue_value_cancel_reenter_probabilities",
    "normalize_queue_value_probabilities",
    "normalize_safe_add_rearm_probabilities",
    "normalize_sell_add_skip_probabilities",
    "normalize_state_conditioned_rearm_probabilities",
]
