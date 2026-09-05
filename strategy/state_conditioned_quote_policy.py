"""Bounded state-conditioned quote action selection.

The policy chooses among the pre-registered local quote actions.  It does not
price orders, alter size, change inventory limits, or bypass strategy safety
gates.  A caller must apply the selected action through the authoritative
live/replay quote path.

The JSON artifact is intentionally strict: unsupported actions are rejected,
while weak behavior overlap, non-positive uplift lower bounds, and stale
features fall back to the rolling baseline. Live permission comes from the
verified deployment envelope, never from the artifact's research annotations.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "state_conditioned_quote_policy.v1"
LOCAL_QUOTE_ACTIONS = (
    "baseline",
    "prevent_over_widen",
    "widen_1tick",
    "recenter_1tick",
)
POLICY_MODES = ("disabled", "shadow", "active")
PROMOTION_ELIGIBLE = "promotion_eligible"


@dataclass(frozen=True)
class FeatureTransform:
    name: str
    mean: float = 0.0
    scale: float = 1.0

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> FeatureTransform:
        name = str(raw.get("name", "")).strip()
        mean = float(raw.get("mean", 0.0) or 0.0)
        scale = float(raw.get("scale", 1.0) or 1.0)
        if not name:
            raise ValueError("policy feature name cannot be empty")
        if not math.isfinite(mean) or not math.isfinite(scale) or scale <= 0.0:
            raise ValueError(f"invalid transform for policy feature {name}")
        return cls(name=name, mean=mean, scale=scale)


@dataclass(frozen=True)
class ActionValueModel:
    intercept: float
    coefficients: dict[str, float]
    support_rows: int
    behavior_probability_floor: float
    uplift_lcb: float

    @classmethod
    def from_dict(
        cls,
        raw: Mapping[str, Any],
        *,
        feature_names: set[str],
    ) -> ActionValueModel:
        coefficients = {
            str(name): float(value)
            for name, value in dict(raw.get("coefficients", {})).items()
        }
        unknown = sorted(set(coefficients) - feature_names)
        if unknown:
            raise ValueError(f"action model uses unknown features: {unknown}")
        values = [float(raw.get("intercept", 0.0)), *coefficients.values()]
        if not all(math.isfinite(value) for value in values):
            raise ValueError("action model contains non-finite coefficients")
        probability = float(raw.get("behavior_probability_floor", 0.0) or 0.0)
        uplift_lcb = float(raw.get("uplift_lcb", 0.0) or 0.0)
        if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
            raise ValueError("behavior_probability_floor must be in [0, 1]")
        if not math.isfinite(uplift_lcb):
            raise ValueError("uplift_lcb must be finite")
        return cls(
            intercept=float(raw.get("intercept", 0.0) or 0.0),
            coefficients=coefficients,
            support_rows=max(0, int(raw.get("support_rows", 0) or 0)),
            behavior_probability_floor=probability,
            uplift_lcb=uplift_lcb,
        )

    def predict(self, transformed: Mapping[str, float]) -> float:
        return float(
            self.intercept
            + sum(
                coefficient * float(transformed.get(name, 0.0))
                for name, coefficient in self.coefficients.items()
            )
        )


@dataclass(frozen=True)
class PolicyGates:
    min_support_rows: int = 100
    min_behavior_probability: float = 0.05
    min_advantage: float = 0.0
    max_feature_age_ms: float = 1_000.0

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> PolicyGates:
        gates = cls(
            min_support_rows=max(1, int(raw.get("min_support_rows", 100) or 100)),
            min_behavior_probability=float(
                raw.get("min_behavior_probability", 0.05) or 0.05
            ),
            min_advantage=float(raw.get("min_advantage", 0.0) or 0.0),
            max_feature_age_ms=float(raw.get("max_feature_age_ms", 1_000.0) or 1_000.0),
        )
        if not 0.0 < gates.min_behavior_probability <= 1.0:
            raise ValueError("min_behavior_probability must be in (0, 1]")
        if not math.isfinite(gates.min_advantage) or gates.min_advantage < 0.0:
            raise ValueError("min_advantage must be finite and non-negative")
        if not math.isfinite(gates.max_feature_age_ms) or gates.max_feature_age_ms < 0.0:
            raise ValueError("max_feature_age_ms must be finite and non-negative")
        return gates


@dataclass(frozen=True)
class PolicyArtifact:
    policy_id: str
    promotion_status: str
    trained_through_day: str
    input_scope: str
    features: tuple[FeatureTransform, ...]
    gates: PolicyGates
    models: dict[str, dict[str, ActionValueModel]]

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> PolicyArtifact:
        if str(raw.get("schema_version", "")) != SCHEMA_VERSION:
            raise ValueError("state-conditioned policy schema mismatch")
        policy_id = str(raw.get("policy_id", "")).strip()
        if not policy_id:
            raise ValueError("policy_id cannot be empty")
        actions = tuple(str(value) for value in raw.get("actions", ()))
        if actions != LOCAL_QUOTE_ACTIONS:
            raise ValueError("policy action registry differs from the code registry")
        features = tuple(
            FeatureTransform.from_dict(value) for value in raw.get("features", ())
        )
        if not features or len({feature.name for feature in features}) != len(features):
            raise ValueError("policy features must be non-empty and unique")
        feature_names = {feature.name for feature in features}
        input_scope = str(raw.get("input_scope", "local_only")).strip().lower()
        if input_scope not in {"local_only", "local_plus_external"}:
            raise ValueError("input_scope must be local_only or local_plus_external")
        if input_scope == "local_only" and any(
            name.startswith(("external_", "global_", "venue_"))
            for name in feature_names
        ):
            raise ValueError("local_only policy cannot declare external features")

        parsed_models: dict[str, dict[str, ActionValueModel]] = {}
        for surface, raw_actions in dict(raw.get("models", {})).items():
            surface_name = str(surface).upper().replace(":ADD", ":add")
            if surface_name not in {"BUY:add", "SELL:add"}:
                raise ValueError(f"unsupported policy surface: {surface}")
            action_models = {
                str(action): ActionValueModel.from_dict(
                    payload,
                    feature_names=feature_names,
                )
                for action, payload in dict(raw_actions).items()
            }
            unknown_actions = sorted(set(action_models) - set(LOCAL_QUOTE_ACTIONS))
            if unknown_actions:
                raise ValueError(f"unknown policy actions: {unknown_actions}")
            if "baseline" not in action_models:
                raise ValueError(f"policy surface {surface_name} lacks baseline model")
            parsed_models[surface_name] = action_models
        if not parsed_models:
            raise ValueError("policy artifact contains no side/role models")
        return cls(
            policy_id=policy_id,
            promotion_status=str(raw.get("promotion_status", "shadow_only")).strip().lower(),
            trained_through_day=str(raw.get("trained_through_day", "")).strip(),
            input_scope=input_scope,
            features=features,
            gates=PolicyGates.from_dict(dict(raw.get("gates", {}))),
            models=parsed_models,
        )

    @classmethod
    def load(
        cls, path: str | Path, *, expected_sha256: str | None = None
    ) -> PolicyArtifact:
        raw = Path(path).expanduser().read_bytes()
        if expected_sha256 is not None and hashlib.sha256(raw).hexdigest() != expected_sha256:
            raise ValueError("state_conditioned_policy_file_sha256_mismatch")
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("policy artifact must be a JSON object")
        return cls.from_dict(payload)


@dataclass(frozen=True)
class StateConditionedDecision:
    policy_id: str
    side: str
    inventory_role: str
    mode: str
    action: str
    candidate_action: str
    baseline_value: float
    candidate_value: float
    advantage: float
    eligible: bool
    reason: str
    feature_age_ms: float
    scores: dict[str, float]


@dataclass(frozen=True)
class LocalActionQuote:
    action: str
    baseline_price: float
    selected_price: float
    delta_ticks: float
    effective: bool
    clamp_reason: str


def _round_passive(side: str, price: float, tick: float) -> float:
    tick_units = price / tick
    nearest = round(tick_units)
    if math.isclose(tick_units, nearest, abs_tol=1e-8, rel_tol=0.0):
        return float(nearest * tick)
    if side == "BUY":
        return math.floor(tick_units) * tick
    return math.ceil(tick_units) * tick


def apply_local_add_action(
    *,
    side: str,
    action: str,
    baseline_price: float,
    pre_guard_price: float,
    other_side_price: float,
    mid: float,
    best_bid: float,
    best_ask: float,
    microprice_shift_bps: float,
    tick: float,
    max_pair_spread: float,
) -> LocalActionQuote:
    """Apply one bounded action without changing size or the other quote."""

    side = str(side).upper()
    action = str(action).strip().lower()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported side: {side}")
    if action not in LOCAL_QUOTE_ACTIONS:
        raise ValueError(f"unsupported local quote action: {action}")
    if tick <= 0.0 or baseline_price <= 0.0:
        raise ValueError("tick and baseline price must be positive")
    if action == "baseline":
        return LocalActionQuote(
            action=action,
            baseline_price=float(baseline_price),
            selected_price=float(baseline_price),
            delta_ticks=0.0,
            effective=False,
            clamp_reason="none",
        )

    selected = float(baseline_price)
    if action == "prevent_over_widen" and pre_guard_price > 0.0:
        if side == "BUY":
            selected = min(
                max(selected, float(pre_guard_price)),
                float(baseline_price) + tick,
            )
        else:
            selected = max(
                min(selected, float(pre_guard_price)),
                float(baseline_price) - tick,
            )
    elif action == "widen_1tick":
        selected += -tick if side == "BUY" else tick
    elif action == "recenter_1tick":
        if microprice_shift_bps > 0.0:
            selected += tick
        elif microprice_shift_bps < 0.0:
            selected -= tick

    reasons: list[str] = []
    if max_pair_spread > 0.0 and other_side_price > 0.0:
        if side == "BUY":
            bounded = max(selected, other_side_price - max_pair_spread)
        else:
            bounded = min(selected, other_side_price + max_pair_spread)
        if not math.isclose(bounded, selected, abs_tol=tick * 0.01):
            reasons.append("pair_spread_cap")
        selected = bounded

    if side == "BUY":
        passive_ceiling = mid - tick if mid > 0.0 else math.inf
        if best_ask > 0.0:
            passive_ceiling = min(passive_ceiling, best_ask - tick)
        bounded = min(selected, passive_ceiling)
    else:
        passive_floor = mid + tick if mid > 0.0 else -math.inf
        if best_bid > 0.0:
            passive_floor = max(passive_floor, best_bid + tick)
        bounded = max(selected, passive_floor)
    if not math.isclose(bounded, selected, abs_tol=tick * 0.01):
        reasons.append("post_only")
    selected = _round_passive(side, bounded, tick)

    delta_ticks = (selected - baseline_price) / tick
    return LocalActionQuote(
        action=action,
        baseline_price=float(baseline_price),
        selected_price=float(selected),
        delta_ticks=float(delta_ticks),
        effective=not math.isclose(selected, baseline_price, abs_tol=tick * 0.01),
        clamp_reason="|".join(reasons) or "none",
    )


def inventory_role_for_quote(side: str, inventory: float, lot_size: float) -> str:
    side = str(side).upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported side: {side}")
    flat_tolerance = max(abs(float(lot_size)) * 0.5, 1e-12)
    if abs(float(inventory)) < flat_tolerance:
        return "opener"
    if (side == "BUY" and inventory > 0.0) or (side == "SELL" and inventory < 0.0):
        return "add"
    return "reducing"


class StateConditionedQuotePolicy:
    """Evaluate a frozen action-value artifact with conservative fallback."""

    def __init__(self, artifact: PolicyArtifact, *, mode: str = "shadow") -> None:
        normalized_mode = str(mode).strip().lower()
        if normalized_mode not in POLICY_MODES:
            raise ValueError(f"unsupported state-conditioned policy mode: {mode}")
        self.artifact = artifact
        self.mode = normalized_mode

    @classmethod
    def load(
        cls, path: str | Path, *, mode: str = "shadow", expected_sha256: str | None = None
    ) -> StateConditionedQuotePolicy:
        return cls(PolicyArtifact.load(path, expected_sha256=expected_sha256), mode=mode)

    def decide(
        self,
        *,
        side: str,
        inventory_role: str,
        features: Mapping[str, Any],
        decision_ts_ns: int,
        feature_ready_ts_ns: int,
    ) -> StateConditionedDecision:
        side = str(side).upper()
        role = str(inventory_role).strip().lower()
        surface = f"{side}:{role}"
        age_ms = (int(decision_ts_ns) - int(feature_ready_ts_ns)) / 1_000_000.0

        def fallback(reason: str) -> StateConditionedDecision:
            return StateConditionedDecision(
                policy_id=self.artifact.policy_id,
                side=side,
                inventory_role=role,
                mode=self.mode,
                action="baseline",
                candidate_action="baseline",
                baseline_value=0.0,
                candidate_value=0.0,
                advantage=0.0,
                eligible=False,
                reason=reason,
                feature_age_ms=float(age_ms),
                scores={"baseline": 0.0},
            )

        if self.mode == "disabled":
            return fallback("disabled")
        if feature_ready_ts_ns > decision_ts_ns:
            return fallback("future_feature")
        if age_ms < 0.0 or age_ms > self.artifact.gates.max_feature_age_ms:
            return fallback("stale_feature")
        action_models = self.artifact.models.get(surface)
        if action_models is None or role != "add":
            return fallback("unsupported_surface")

        transformed: dict[str, float] = {}
        for feature in self.artifact.features:
            try:
                value = float(features[feature.name])
            except (KeyError, TypeError, ValueError):
                return fallback(f"missing_feature:{feature.name}")
            if not math.isfinite(value):
                return fallback(f"nonfinite_feature:{feature.name}")
            transformed[feature.name] = (value - feature.mean) / feature.scale

        baseline_model = action_models["baseline"]
        baseline_value = baseline_model.predict(transformed)
        scores = {"baseline": baseline_value}
        supported: list[tuple[float, str]] = []
        for action in LOCAL_QUOTE_ACTIONS[1:]:
            model = action_models.get(action)
            if model is None:
                continue
            scores[action] = model.predict(transformed)
            if (
                model.support_rows >= self.artifact.gates.min_support_rows
                and model.behavior_probability_floor
                >= self.artifact.gates.min_behavior_probability
                and model.uplift_lcb > 0.0
            ):
                supported.append((scores[action], action))
        if not supported:
            return StateConditionedDecision(
                policy_id=self.artifact.policy_id,
                side=side,
                inventory_role=role,
                mode=self.mode,
                action="baseline",
                candidate_action="baseline",
                baseline_value=baseline_value,
                candidate_value=baseline_value,
                advantage=0.0,
                eligible=False,
                reason="no_supported_candidate",
                feature_age_ms=float(age_ms),
                scores=scores,
            )

        candidate_value, candidate_action = max(
            supported,
            key=lambda item: (item[0], -LOCAL_QUOTE_ACTIONS.index(item[1])),
        )
        advantage = float(candidate_value - baseline_value)
        eligible = advantage > self.artifact.gates.min_advantage
        selected_action = candidate_action if eligible and self.mode == "active" else "baseline"
        reason = "selected" if eligible else "nonpositive_state_advantage"
        if eligible and self.mode == "shadow":
            reason = "shadow_candidate"
        return StateConditionedDecision(
            policy_id=self.artifact.policy_id,
            side=side,
            inventory_role=role,
            mode=self.mode,
            action=selected_action,
            candidate_action=candidate_action if eligible else "baseline",
            baseline_value=float(baseline_value),
            candidate_value=float(candidate_value if eligible else baseline_value),
            advantage=float(advantage if eligible else 0.0),
            eligible=bool(eligible),
            reason=reason,
            feature_age_ms=float(age_ms),
            scores=scores,
        )


__all__ = [
    "LocalActionQuote",
    "LOCAL_QUOTE_ACTIONS",
    "POLICY_MODES",
    "PROMOTION_ELIGIBLE",
    "SCHEMA_VERSION",
    "PolicyArtifact",
    "StateConditionedDecision",
    "StateConditionedQuotePolicy",
    "apply_local_add_action",
    "inventory_role_for_quote",
]
