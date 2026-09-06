"""Pure E/C selection of already-permitted baseline order intentions.

No network, order mutation, evidence writer, or runtime file access belongs in
``evaluate_risk_selection``. Values are POST-minus-WAIT or KEEP-minus-CANCEL in
USDC per current candidate, not probabilities or per-BTC returns. These types
provide mechanics only; no model or economic benefit is supplied by default.

Adapters batch both sides from the same visible account/order/market snapshot.
``features`` supplies shared scalar inputs; candidate features override only
the corresponding side's fields. Both mappings are detached on construction,
and unavailable/nonfinite values remain unavailable, never zero-filled. Policy
feature transforms declare units plus training-only means/scales; adapters do
unit conversion before construction, not again while scoring.

The opportunity collector and scorer use ``candidate_role`` identically: E
requires ``opener``; C requires ``opener`` or ``add`` for its current remaining
quantity. Pass every other nonterminal order's positive remaining exposure,
including pending cancel/new; only the matching C target is excluded. Selection
does not reserve budget, cancel orders, release ownership, or change cadence.
The existing execution layer rechecks current authority before dispatch. A
WAIT skips one eligible submission, and a CANCEL uses the ordinary lifecycle;
neither establishes a new cooldown nor forces immediate replacement.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any

SCHEMA_VERSION = "risk_selection_policy.v1"
VALUE_UNIT = "USDC_per_action"
_ACTIONS = {"E": ("POST", "WAIT"), "C": ("KEEP", "CANCEL")}


def _finite(value: Any, name: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite")
    return number


def _feature_snapshot(values: Mapping[str, Any]) -> Mapping[str, float | None]:
    frozen = {}
    for name, value in values.items():
        try:
            frozen[name] = _finite(value, name)
        except (TypeError, ValueError, OverflowError):
            frozen[name] = None
    return MappingProxyType(frozen)


def inventory_role_for_target(
    side: str, inventory: float, target_quantity: float, *, epsilon_btc: float = 1e-10
) -> str:
    """Classify the entire quantity, including a partial close crossing zero."""
    side = str(side).strip().upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported quote side: {side!r}")
    quantity = _finite(target_quantity, "target_quantity")
    if quantity <= 0:
        raise ValueError("target_quantity must be positive")
    q = _finite(inventory, "inventory")
    epsilon = max(_finite(epsilon_btc, "epsilon_btc"), 0.0)
    if abs(q) <= epsilon:
        return "opener"
    signed_quantity = quantity if side == "BUY" else -quantity
    if q * signed_quantity > 0:
        return "add"
    q_after = q + signed_quantity
    if abs(q_after) <= epsilon or q * q_after > 0:
        return "reducing"
    return "mixed_cross_zero"


@dataclass(frozen=True, slots=True)
class PendingExposure:
    """Other nonterminal remaining exposure, including pending cancel/new.

    Include each order once. A cancel request does not remove its exposure.
    The caller supplies one coherent order/account snapshot; these immutable
    values neither reconcile nor independently infer exchange state.
    """

    order_id: str
    side: str
    remaining_qty_btc: float

    def __post_init__(self) -> None:
        if not self.order_id or self.side not in {"BUY", "SELL"}:
            raise ValueError("pending exposure requires an order ID and BUY/SELL side")
        quantity = _finite(self.remaining_qty_btc, "remaining_qty_btc")
        if quantity < 0:
            raise ValueError("remaining_qty_btc cannot be negative")
        object.__setattr__(self, "remaining_qty_btc", quantity)


@dataclass(frozen=True, slots=True)
class RiskSelectionObservation:
    decision_ts_ns: int
    feature_ready_ts_ns: int
    inventory_btc: float
    pending_orders: tuple[PendingExposure, ...] = ()
    features: Mapping[str, Any] = field(default_factory=dict)
    environment_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "inventory_btc", _finite(self.inventory_btc, "inventory_btc"))
        if self.decision_ts_ns < 0 or self.feature_ready_ts_ns < 0:
            raise ValueError("observation clocks cannot be negative")
        orders = tuple(self.pending_orders)
        if len({order.order_id for order in orders}) != len(orders):
            raise ValueError("pending order IDs must be unique")
        object.__setattr__(self, "pending_orders", orders)
        object.__setattr__(self, "features", _feature_snapshot(self.features))


@dataclass(frozen=True, slots=True)
class RiskSelectionCandidate:
    opportunity_id: str
    kind: str
    side: str
    quantity_btc: float
    baseline_action: str
    baseline_allowed: bool = True
    order_id: str = ""
    features: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.opportunity_id or self.kind not in _ACTIONS:
            raise ValueError("candidate requires an opportunity ID and E/C kind")
        if self.side not in {"BUY", "SELL"}:
            raise ValueError("candidate side must be BUY or SELL")
        if self.baseline_action not in _ACTIONS[self.kind]:
            raise ValueError("baseline action does not match E/C kind")
        if not self.baseline_allowed and self.baseline_action == _ACTIONS[self.kind][0]:
            raise ValueError("H0-blocked candidate cannot declare baseline POST/KEEP")
        quantity = _finite(self.quantity_btc, "quantity_btc")
        if quantity <= 0:
            raise ValueError("quantity_btc must be positive")
        object.__setattr__(self, "quantity_btc", quantity)
        if self.kind == "C" and not self.order_id:
            raise ValueError("C candidate requires its nonterminal order ID")
        object.__setattr__(self, "features", _feature_snapshot(self.features))


def candidate_role(
    observation: RiskSelectionObservation, candidate: RiskSelectionCandidate
) -> str:
    """Require one role across every possible fill of other pending exposure.

    Remaining orders may partially fill, so their reachable inventory is an
    interval, not their net signed total. Exclude only the C target itself.
    """
    low = high = float(observation.inventory_btc)
    for order in observation.pending_orders:
        if candidate.kind == "C" and order.order_id == candidate.order_id:
            if order.side != candidate.side or not math.isclose(
                order.remaining_qty_btc, candidate.quantity_btc, rel_tol=0, abs_tol=1e-10
            ):
                raise ValueError("C target differs from the order snapshot")
            continue
        if order.side == "BUY":
            high += order.remaining_qty_btc
        else:
            low -= order.remaining_qty_btc
    low_role = inventory_role_for_target(candidate.side, low, candidate.quantity_btc)
    high_role = inventory_role_for_target(candidate.side, high, candidate.quantity_btc)
    if low_role == high_role:
        return low_role
    if candidate.kind == "C" and {low_role, high_role} == {"opener", "add"}:
        # Every reachable inventory in this one-sided interval still makes the
        # remaining order purely exposure-increasing. Flat versus already-held
        # inventory is not an ambiguity about whether C can cancel new risk.
        return "opener_or_add"
    return "ambiguous_pending"


@dataclass(frozen=True, slots=True)
class LinearValueModel:
    intercept_usdc: float
    coefficients: Mapping[str, float]

    def __post_init__(self) -> None:
        object.__setattr__(self, "intercept_usdc", _finite(self.intercept_usdc, "intercept_usdc"))
        coefficients = {name: _finite(value, name) for name, value in self.coefficients.items()}
        if any(not isinstance(name, str) or not name for name in coefficients):
            raise ValueError("coefficient names must be nonempty strings")
        object.__setattr__(self, "coefficients", MappingProxyType(coefficients))


@dataclass(frozen=True, slots=True)
class RiskSelectionPolicy:
    policy_id: str
    # Each feature maps to (documented unit, training mean, training scale).
    features: Mapping[str, tuple[str, float, float]]
    models: Mapping[str, LinearValueModel]

    def __post_init__(self) -> None:
        if not isinstance(self.policy_id, str) or not self.policy_id:
            raise ValueError("policy_id must be a nonempty string")
        features = {}
        for name, (unit, mean, scale) in self.features.items():
            if (
                not isinstance(name, str) or not name
                or not isinstance(unit, str) or not unit
                or _finite(scale, "feature scale") <= 0
            ):
                raise ValueError("features need a name, unit, and positive scale")
            features[name] = (unit, _finite(mean, "feature mean"), float(scale))
        models = dict(self.models)
        for surface, model in models.items():
            if surface not in {f"{kind}:{side}" for kind in _ACTIONS for side in ("BUY", "SELL")}:
                raise ValueError(f"unsupported policy surface: {surface}")
            if not isinstance(model, LinearValueModel):
                raise ValueError("policy models must be parsed LinearValueModel instances")
            if set(model.coefficients) - features.keys():
                raise ValueError("model uses undeclared features")
        object.__setattr__(self, "features", MappingProxyType(features))
        object.__setattr__(self, "models", MappingProxyType(models))

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> RiskSelectionPolicy:
        try:
            if payload["schema_version"] != SCHEMA_VERSION or payload["value_unit"] != VALUE_UNIT:
                raise ValueError("risk selection schema or value unit mismatch")
            features = {
                name: (row["unit"], row["mean"], row["scale"])
                for name, row in payload["features"].items()
            }
            models = {
                surface: LinearValueModel(row["intercept_usdc"], row["coefficients"])
                for surface, row in payload["models"].items()
            }
            return cls(payload["policy_id"], features, models)
        except (KeyError, TypeError, AttributeError) as exc:
            raise ValueError("malformed risk selection policy") from exc

    @classmethod
    def load(cls, path: str | Path) -> RiskSelectionPolicy:
        """Load once at the caller's existing artifact-loading boundary."""
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("risk selection policy must be an object")
        return cls.from_dict(payload)


@dataclass(frozen=True, slots=True)
class RiskSelectionDecision:
    opportunity_id: str
    kind: str
    side: str
    order_id: str
    action: str
    value_delta_usdc: float | None
    role: str
    reason: str
    out_of_scope: bool
    policy_id: str
    decision_ts_ns: int
    feature_ready_ts_ns: int
    environment_id: str


def evaluate_risk_selection(
    observation: RiskSelectionObservation,
    candidates: Sequence[RiskSelectionCandidate],
    policy: RiskSelectionPolicy | None = None,
) -> tuple[RiskSelectionDecision, ...]:
    """Batch evaluate one snapshot; never mutate it after the first side.

    E requires strictly positive POST-minus-WAIT; C cancels only a negative
    KEEP-minus-CANCEL value. Thus the zero-value tie waits for E and keeps for C.
    Missing models/features abstain; invalid account/order inputs raise instead
    of pretending that an unknown trading state is safe.
    """
    if len({candidate.opportunity_id for candidate in candidates}) != len(candidates):
        raise ValueError("candidate opportunity IDs must be unique within one decision")
    decisions = []
    for candidate in candidates:
        role = candidate_role(observation, candidate)
        action, value, reason = candidate.baseline_action, None, "no_model"
        active_action, veto_action = _ACTIONS[candidate.kind]
        model = policy.models.get(f"{candidate.kind}:{candidate.side}") if policy else None
        allowed_roles = ({"opener"} if candidate.kind == "E"
                         else {"opener", "add", "opener_or_add"})
        if not candidate.baseline_allowed or action != active_action:
            reason = "baseline_blocked"
        elif role not in allowed_roles:
            reason = role
        elif observation.feature_ready_ts_ns > observation.decision_ts_ns:
            reason = "future_feature"
        elif model is not None:
            values = dict(observation.features)
            values.update(candidate.features)
            try:
                value = float(model.intercept_usdc)
                for name, coefficient in model.coefficients.items():
                    _, mean, scale = policy.features[name]
                    value += coefficient * (_finite(values[name], name) - mean) / scale
                value = _finite(value, "value_delta_usdc")
            except (KeyError, ValueError, TypeError, OverflowError):
                value, reason = None, "unavailable_feature"
            else:
                veto = value <= 0 if candidate.kind == "E" else value < 0
                action = veto_action if veto else active_action
                reason = ("zero_entry_value" if candidate.kind == "E" and value == 0
                          else "negative_value" if value < 0 else "nonnegative_value")
        decisions.append(RiskSelectionDecision(
            candidate.opportunity_id, candidate.kind, candidate.side, candidate.order_id,
            action, value, role, reason, value is None,
            policy.policy_id if policy else "", observation.decision_ts_ns,
            observation.feature_ready_ts_ns, observation.environment_id,
        ))
    return tuple(decisions)
