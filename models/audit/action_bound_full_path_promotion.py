"""Governance validator for action-bound full-path promotion.

Observation-only shadowing is optional engineering evidence.  A strategy
promotion must bind one concrete action and earn authority from full-path
economic, execution, safety, and rollback evidence.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from typing import Any


SCHEMA_VERSION = "narrowgate.action_bound_full_path_direct_promotion.v1"
IDENTITY = "action_bound_full_path_direct_promotion_contract_v1"
STANDARD_PROMOTION = "research_supported_promotion"
OWNER_PROMOTION = "owner_risk_accepted_promotion"
P3_LEVERAGE_INTERFACE_SCHEMA = "conditional_p3_action_leverage.interface.v1"

P3_LEVERAGE_FIELDS = (
    "interface_schema_version",
    "parent_action_identity_hash",
    "baseline_identity_hash",
    "candidate_policy_hash",
    "candidate_universe_sha256",
    "p3_artifact_hash",
    "p3_cadence",
    "p3_horizon_s",
    "quote_snapshot_id",
    "quote_snapshot_market_generation",
    "quote_snapshot_depth_generation",
    "side",
    "role",
    "candidate_source",
    "candidate_action",
    "baseline_effective_price",
    "candidate_effective_price",
    "tick_size",
    "effective_tick_delta",
    "price_action_noop",
    "support_valid",
    "unsupported_reason",
    "baseline_reach_probability",
    "candidate_reach_probability",
    "delta_reach_probability",
    "relative_reach_ratio",
    "relative_reach_ratio_valid",
    "probability_denominator_epsilon",
    "delta_reach_lcb_simultaneous",
    "delta_reach_ucb_simultaneous",
    "reach_near_noop",
    "reach_near_noop_abs_delta",
    "reach_collapse_risk",
    "reach_retention_floor",
    "simultaneous_band_family_id",
    "simultaneous_band_artifact_sha256",
    "simultaneous_band_method",
    "p3_generates_quote",
    "p3_grants_action_authority",
)

REQUIRED_FULL_PATH_COMPONENTS = (
    "quote_decision_snapshot",
    "tick_rounding_gtx_and_spread_cap",
    "activation",
    "queue",
    "partial_fill",
    "cancel_request_ack_race",
    "cooldown",
    "inventory",
    "campaign",
)

REQUIRED_ECONOMIC_GATES = (
    "assignment_to_terminal_pnl_lcb_positive",
    "campaign_q10_noninferior",
    "campaign_cvar_noninferior",
    "mae_noninferior",
    "maximum_inventory_noninferior",
    "inventory_time_noninferior",
    "fill_and_activity_within_frozen_bounds",
)

REQUIRED_PRODUCTION_GATES = (
    "python_cpp_parity",
    "config_model_and_artifact_hash_match",
    "live_preflight",
    "automatic_rollback",
)


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _string_tuple(value: object, *, name: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{name} must be a sequence")
    values = tuple(str(item).strip() for item in value)
    if not values or any(not item for item in values):
        raise ValueError(f"{name} must contain non-empty strings")
    return values


def _require_sha256(value: object, *, name: str) -> str:
    text = str(value).strip().lower()
    if (
        len(text) != 64
        or any(character not in "0123456789abcdef" for character in text)
        or len(set(text)) == 1
    ):
        raise ValueError(f"{name} must be a non-degenerate SHA256")
    return text


def _finite_float(value: object, *, name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _probability(value: object, *, name: str) -> float:
    result = _finite_float(value, name=name)
    if not 0.0 <= result <= 1.0:
        raise ValueError(f"{name} must lie in [0, 1]")
    return result


def _optional_probability(value: object, *, name: str) -> float | None:
    if value is None:
        return None
    return _probability(value, name=name)


def _price_tick(value: object, *, tick_size: float, name: str) -> int:
    price = _finite_float(value, name=name)
    if price <= 0.0:
        raise ValueError(f"{name} must be positive")
    ratio = price / tick_size
    tick = round(ratio)
    if abs(ratio - tick) > 1e-9:
        raise ValueError(f"{name} must lie on the executable tick grid")
    return int(tick)


def _require_exact_members(
    value: object,
    *,
    name: str,
    required: tuple[str, ...],
) -> None:
    values = _string_tuple(value, name=name)
    if set(values) != set(required) or len(values) != len(required):
        raise ValueError(f"{name} must exactly match the frozen contract")


def validate_governance_contract(contract: Mapping[str, Any]) -> None:
    """Validate the reusable promotion rule without granting an action."""

    if contract.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported direct-promotion schema")
    if contract.get("identity") != IDENTITY:
        raise ValueError("direct-promotion identity mismatch")

    flow = contract.get("stage_order")
    expected_flow = (
        "freeze_concrete_action",
        "authoritative_full_path_replay",
        "promotion_controller",
        "direct_active_live_with_rollback",
    )
    if tuple(_string_tuple(flow, name="stage_order")) != expected_flow:
        raise ValueError("stage order must bind action before replay and promotion")

    shadow = contract.get("shadow_stage")
    if not isinstance(shadow, Mapping):
        raise ValueError("shadow_stage must be present")
    if bool(shadow.get("mandatory_for_promotion", True)):
        raise ValueError("observation-only shadow cannot be a mandatory promotion stage")
    if bool(shadow.get("grants_action_authority", True)):
        raise ValueError("shadow evidence cannot grant action authority")
    if not bool(shadow.get("engineering_exception_requires_budget_and_expiry", False)):
        raise ValueError("engineering shadow exceptions require a budget and expiry")

    action = contract.get("action_binding")
    if not isinstance(action, Mapping):
        raise ValueError("action_binding must be present")
    for field in (
        "identity_hash_required",
        "baseline_hash_required",
        "candidate_policy_hash_required",
        "side_scope_frozen_before_replay",
        "candidate_rate_frozen_before_replay",
        "rate_limit_frozen_before_replay",
        "deployed_parameters_must_equal_replay",
    ):
        if action.get(field) is not True:
            raise ValueError(f"action binding must require {field}")

    leverage = contract.get("conditional_p3_leverage")
    if not isinstance(leverage, Mapping):
        raise ValueError("conditional_p3_leverage must be present")
    if leverage.get("role") != "action_embedded_mechanics_input_only":
        raise ValueError("P3 leverage must remain an action-embedded mechanics input")
    if leverage.get("standalone_research_identity_allowed") is not False:
        raise ValueError("standalone P3 leverage identity is forbidden")
    if leverage.get("generates_quote") is not False:
        raise ValueError("P3 leverage cannot generate a quote")
    if leverage.get("grants_action_authority") is not False:
        raise ValueError("P3 leverage cannot grant action authority")
    _require_exact_members(
        leverage.get("fields"),
        name="conditional_p3_leverage.fields",
        required=P3_LEVERAGE_FIELDS,
    )
    if leverage.get("interval_scope") != (
        "paired_simultaneous_band_over_frozen_action_candidate_family"
    ):
        raise ValueError("P3 leverage requires a paired candidate-family band")

    replay = contract.get("authoritative_full_path_replay")
    if not isinstance(replay, Mapping):
        raise ValueError("authoritative_full_path_replay must be present")
    if replay.get("shared_quote_snapshot_between_arms") is not True:
        raise ValueError("baseline and candidate must share the decision snapshot")
    _require_exact_members(
        replay.get("required_components"),
        name="authoritative_full_path_replay.required_components",
        required=REQUIRED_FULL_PATH_COMPONENTS,
    )
    _require_exact_members(
        replay.get("economic_gates"),
        name="authoritative_full_path_replay.economic_gates",
        required=REQUIRED_ECONOMIC_GATES,
    )

    production = contract.get("production_promotion")
    if not isinstance(production, Mapping):
        raise ValueError("production_promotion must be present")
    _require_exact_members(
        production.get("required_gates"),
        name="production_promotion.required_gates",
        required=REQUIRED_PRODUCTION_GATES,
    )
    routes = production.get("routes")
    if not isinstance(routes, Mapping):
        raise ValueError("production promotion routes must be present")
    if routes.get("hard_gate_path") != STANDARD_PROMOTION:
        raise ValueError("hard-gate route label mismatch")
    if routes.get("owner_progression_path") != OWNER_PROMOTION:
        raise ValueError("owner route label mismatch")
    if production.get("owner_label_is_permanent") is not True:
        raise ValueError("owner-risk promotion label must remain permanent")
    if production.get("shadow_can_substitute_for_full_path") is not False:
        raise ValueError("shadow cannot substitute for full-path evidence")

    permissions = contract.get("contract_permissions")
    if not isinstance(permissions, Mapping):
        raise ValueError("contract_permissions must be present")
    if any(bool(value) for value in permissions.values()):
        raise ValueError("a governance contract cannot itself grant permissions")


def validate_action_embedded_p3_leverage(payload: Mapping[str, Any]) -> None:
    """Validate P3 leverage nested inside one concrete action path record."""

    expected_fields = set(P3_LEVERAGE_FIELDS)
    actual_fields = set(payload)
    if actual_fields != expected_fields:
        raise ValueError(
            "embedded P3 leverage schema mismatch; "
            f"missing={sorted(expected_fields - actual_fields)}, "
            f"extra={sorted(actual_fields - expected_fields)}"
        )
    if payload["interface_schema_version"] != P3_LEVERAGE_INTERFACE_SCHEMA:
        raise ValueError("embedded P3 leverage interface version mismatch")
    for field in (
        "parent_action_identity_hash",
        "baseline_identity_hash",
        "candidate_policy_hash",
        "candidate_universe_sha256",
        "p3_artifact_hash",
        "simultaneous_band_artifact_sha256",
    ):
        _require_sha256(payload[field], name=field)
    if payload["parent_action_identity_hash"] != payload["candidate_policy_hash"]:
        raise ValueError("P3 leverage must be owned by the candidate action identity")
    if payload["p3_cadence"] != "canonical_10s_only":
        raise ValueError("current P3 leverage interface supports canonical 10s only")
    if _finite_float(payload["p3_horizon_s"], name="p3_horizon_s") != 10.0:
        raise ValueError("current P3 leverage interface supports the 10s horizon only")
    if not str(payload["quote_snapshot_id"]).strip():
        raise ValueError("quote_snapshot_id must be non-empty")
    for field in (
        "quote_snapshot_market_generation",
        "quote_snapshot_depth_generation",
    ):
        value = payload[field]
        if isinstance(value, bool) or int(value) != value or int(value) < 0:
            raise ValueError(f"{field} must be a nonnegative integer")
    if payload["side"] not in ("BUY", "SELL"):
        raise ValueError("side must be BUY or SELL")
    for field in ("role", "candidate_source", "candidate_action"):
        if not str(payload[field]).strip():
            raise ValueError(f"{field} must be non-empty")

    tick_size = _finite_float(payload["tick_size"], name="tick_size")
    if tick_size <= 0.0:
        raise ValueError("tick_size must be positive")
    baseline_tick = _price_tick(
        payload["baseline_effective_price"],
        tick_size=tick_size,
        name="baseline_effective_price",
    )
    candidate_tick = _price_tick(
        payload["candidate_effective_price"],
        tick_size=tick_size,
        name="candidate_effective_price",
    )
    tick_delta = candidate_tick - baseline_tick
    if int(payload["effective_tick_delta"]) != tick_delta:
        raise ValueError("effective_tick_delta mismatch")
    price_action_noop = tick_delta == 0
    if payload["price_action_noop"] is not price_action_noop:
        raise ValueError("price_action_noop mismatch")

    epsilon = _finite_float(
        payload["probability_denominator_epsilon"],
        name="probability_denominator_epsilon",
    )
    if not 0.0 < epsilon < 1.0:
        raise ValueError("probability denominator epsilon must lie in (0, 1)")
    near_noop_threshold = _finite_float(
        payload["reach_near_noop_abs_delta"],
        name="reach_near_noop_abs_delta",
    )
    if not 0.0 <= near_noop_threshold < 1.0:
        raise ValueError("reach near-noop threshold must lie in [0, 1)")
    retention_floor = _finite_float(
        payload["reach_retention_floor"],
        name="reach_retention_floor",
    )
    if not 0.0 < retention_floor < 1.0:
        raise ValueError("reach retention floor must lie in (0, 1)")
    if not str(payload["simultaneous_band_family_id"]).strip():
        raise ValueError("simultaneous_band_family_id must be non-empty")
    if payload["simultaneous_band_method"] not in (
        "paired_day_cluster_bootstrap",
        "joint_surface_refit_band",
    ):
        raise ValueError("unsupported simultaneous band method")
    if payload["p3_generates_quote"] is not False:
        raise ValueError("P3 leverage cannot generate a quote")
    if payload["p3_grants_action_authority"] is not False:
        raise ValueError("P3 leverage cannot grant action authority")

    support_valid = payload["support_valid"]
    if not isinstance(support_valid, bool):
        raise ValueError("support_valid must be boolean")
    p0 = _optional_probability(
        payload["baseline_reach_probability"],
        name="baseline_reach_probability",
    )
    p1 = _optional_probability(
        payload["candidate_reach_probability"],
        name="candidate_reach_probability",
    )
    if support_valid:
        if str(payload["unsupported_reason"]):
            raise ValueError("supported P3 leverage cannot carry unsupported_reason")
        if p0 is None or p1 is None:
            raise ValueError("supported P3 leverage requires paired probabilities")
        delta = p1 - p0
        if not math.isclose(
            _finite_float(
                payload["delta_reach_probability"],
                name="delta_reach_probability",
            ),
            delta,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError("delta_reach_probability mismatch")
        lcb = _finite_float(
            payload["delta_reach_lcb_simultaneous"],
            name="delta_reach_lcb_simultaneous",
        )
        ucb = _finite_float(
            payload["delta_reach_ucb_simultaneous"],
            name="delta_reach_ucb_simultaneous",
        )
        if lcb > ucb or lcb > delta + 1e-12 or ucb < delta - 1e-12:
            raise ValueError("paired simultaneous band must contain delta reach")
        ratio_valid = p0 >= epsilon
        if payload["relative_reach_ratio_valid"] is not ratio_valid:
            raise ValueError("relative_reach_ratio_valid mismatch")
        if ratio_valid:
            expected_ratio = p1 / p0
            ratio = _finite_float(
                payload["relative_reach_ratio"],
                name="relative_reach_ratio",
            )
            if not math.isclose(ratio, expected_ratio, rel_tol=0.0, abs_tol=1e-12):
                raise ValueError("relative_reach_ratio mismatch")
        elif payload["relative_reach_ratio"] is not None:
            raise ValueError("relative_reach_ratio must be null below epsilon")
        expected_near_noop = (
            not price_action_noop
            and max(abs(lcb), abs(ucb)) <= near_noop_threshold
        )
        if payload["reach_near_noop"] is not expected_near_noop:
            raise ValueError("reach_near_noop mismatch")
        expected_collapse = None if not ratio_valid else (p1 / p0) < retention_floor
        if payload["reach_collapse_risk"] is not expected_collapse:
            raise ValueError("reach_collapse_risk mismatch")
    else:
        if not str(payload["unsupported_reason"]).strip():
            raise ValueError("unsupported P3 leverage requires a reason")
        nullable = (
            "baseline_reach_probability",
            "candidate_reach_probability",
            "delta_reach_probability",
            "relative_reach_ratio",
            "delta_reach_lcb_simultaneous",
            "delta_reach_ucb_simultaneous",
            "reach_near_noop",
            "reach_collapse_risk",
        )
        if any(payload[field] is not None for field in nullable):
            raise ValueError("unsupported P3 leverage cannot report reach estimates")
        if payload["relative_reach_ratio_valid"] is not False:
            raise ValueError("unsupported P3 leverage cannot validate a ratio")


def build_action_embedded_p3_leverage(
    *,
    parent_action_identity_hash: str,
    baseline_identity_hash: str,
    candidate_policy_hash: str,
    candidate_universe_sha256: str,
    p3_artifact_hash: str,
    quote_snapshot_id: str,
    quote_snapshot_market_generation: int,
    quote_snapshot_depth_generation: int,
    side: str,
    role: str,
    candidate_source: str,
    candidate_action: str,
    baseline_effective_price: float,
    candidate_effective_price: float,
    tick_size: float,
    support_valid: bool,
    unsupported_reason: str = "",
    baseline_reach_probability: float | None = None,
    candidate_reach_probability: float | None = None,
    probability_denominator_epsilon: float,
    delta_reach_lcb_simultaneous: float | None = None,
    delta_reach_ucb_simultaneous: float | None = None,
    reach_near_noop_abs_delta: float,
    reach_retention_floor: float,
    simultaneous_band_family_id: str,
    simultaneous_band_artifact_sha256: str,
    simultaneous_band_method: str,
) -> dict[str, Any]:
    """Build the P3 mechanics block embedded in a concrete full-path action."""

    tick = _finite_float(tick_size, name="tick_size")
    if tick <= 0.0:
        raise ValueError("tick_size must be positive")
    baseline_tick = _price_tick(
        baseline_effective_price,
        tick_size=tick,
        name="baseline_effective_price",
    )
    candidate_tick = _price_tick(
        candidate_effective_price,
        tick_size=tick,
        name="candidate_effective_price",
    )
    epsilon = _finite_float(
        probability_denominator_epsilon,
        name="probability_denominator_epsilon",
    )
    if not 0.0 < epsilon < 1.0:
        raise ValueError("probability denominator epsilon must lie in (0, 1)")
    near_noop_threshold = _finite_float(
        reach_near_noop_abs_delta,
        name="reach_near_noop_abs_delta",
    )
    if not 0.0 <= near_noop_threshold < 1.0:
        raise ValueError("reach near-noop threshold must lie in [0, 1)")
    retention_floor = _finite_float(
        reach_retention_floor,
        name="reach_retention_floor",
    )
    if not 0.0 < retention_floor < 1.0:
        raise ValueError("reach retention floor must lie in (0, 1)")
    price_action_noop = baseline_tick == candidate_tick
    if support_valid:
        if unsupported_reason:
            raise ValueError("supported P3 leverage cannot carry unsupported_reason")
        if (
            baseline_reach_probability is None
            or candidate_reach_probability is None
            or delta_reach_lcb_simultaneous is None
            or delta_reach_ucb_simultaneous is None
        ):
            raise ValueError("supported P3 leverage requires probabilities and band")
        p0 = _probability(
            baseline_reach_probability,
            name="baseline_reach_probability",
        )
        p1 = _probability(
            candidate_reach_probability,
            name="candidate_reach_probability",
        )
        ratio_valid = p0 >= epsilon
        ratio = p1 / p0 if ratio_valid else None
        lcb = _finite_float(
            delta_reach_lcb_simultaneous,
            name="delta_reach_lcb_simultaneous",
        )
        ucb = _finite_float(
            delta_reach_ucb_simultaneous,
            name="delta_reach_ucb_simultaneous",
        )
        near_noop = (
            not price_action_noop
            and max(abs(lcb), abs(ucb)) <= near_noop_threshold
        )
        collapse = None if not ratio_valid else ratio < retention_floor
        delta = p1 - p0
    else:
        if not unsupported_reason.strip():
            raise ValueError("unsupported P3 leverage requires a reason")
        if any(
            value is not None
            for value in (
                baseline_reach_probability,
                candidate_reach_probability,
                delta_reach_lcb_simultaneous,
                delta_reach_ucb_simultaneous,
            )
        ):
            raise ValueError("unsupported P3 leverage cannot report reach estimates")
        p0 = None
        p1 = None
        delta = None
        ratio_valid = False
        ratio = None
        lcb = None
        ucb = None
        near_noop = None
        collapse = None
    payload = {
        "interface_schema_version": P3_LEVERAGE_INTERFACE_SCHEMA,
        "parent_action_identity_hash": parent_action_identity_hash,
        "baseline_identity_hash": baseline_identity_hash,
        "candidate_policy_hash": candidate_policy_hash,
        "candidate_universe_sha256": candidate_universe_sha256,
        "p3_artifact_hash": p3_artifact_hash,
        "p3_cadence": "canonical_10s_only",
        "p3_horizon_s": 10.0,
        "quote_snapshot_id": quote_snapshot_id,
        "quote_snapshot_market_generation": quote_snapshot_market_generation,
        "quote_snapshot_depth_generation": quote_snapshot_depth_generation,
        "side": side,
        "role": role,
        "candidate_source": candidate_source,
        "candidate_action": candidate_action,
        "baseline_effective_price": baseline_effective_price,
        "candidate_effective_price": candidate_effective_price,
        "tick_size": tick,
        "effective_tick_delta": candidate_tick - baseline_tick,
        "price_action_noop": price_action_noop,
        "support_valid": support_valid,
        "unsupported_reason": unsupported_reason,
        "baseline_reach_probability": p0,
        "candidate_reach_probability": p1,
        "delta_reach_probability": delta,
        "relative_reach_ratio": ratio,
        "relative_reach_ratio_valid": ratio_valid,
        "probability_denominator_epsilon": epsilon,
        "delta_reach_lcb_simultaneous": lcb,
        "delta_reach_ucb_simultaneous": ucb,
        "reach_near_noop": near_noop,
        "reach_near_noop_abs_delta": near_noop_threshold,
        "reach_collapse_risk": collapse,
        "reach_retention_floor": retention_floor,
        "simultaneous_band_family_id": simultaneous_band_family_id,
        "simultaneous_band_artifact_sha256": simultaneous_band_artifact_sha256,
        "simultaneous_band_method": simultaneous_band_method,
        "p3_generates_quote": False,
        "p3_grants_action_authority": False,
    }
    validate_action_embedded_p3_leverage(payload)
    return payload


def governance_contract_sha256(contract: Mapping[str, Any]) -> str:
    validate_governance_contract(contract)
    return canonical_sha256(contract)


__all__ = [
    "IDENTITY",
    "OWNER_PROMOTION",
    "P3_LEVERAGE_INTERFACE_SCHEMA",
    "P3_LEVERAGE_FIELDS",
    "REQUIRED_ECONOMIC_GATES",
    "REQUIRED_FULL_PATH_COMPONENTS",
    "REQUIRED_PRODUCTION_GATES",
    "SCHEMA_VERSION",
    "STANDARD_PROMOTION",
    "canonical_sha256",
    "build_action_embedded_p3_leverage",
    "governance_contract_sha256",
    "validate_action_embedded_p3_leverage",
    "validate_governance_contract",
]
