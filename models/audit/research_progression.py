"""Shared contract for immutable hard gates and explicit owner progression.

The two paths may both eventually reach live, but they preserve different
evidence labels.  An owner continuation never rewrites a failed hard gate.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any


SCHEMA_VERSION = "narrowgate.research_progression.v1"
STANDARD_PROMOTION = "research_supported_promotion"
OWNER_PROMOTION = "owner_risk_accepted_promotion"
NO_PROMOTION = "not_authorized"


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _require_sha256(value: object, *, name: str) -> str:
    text = str(value).lower()
    if (
        len(text) != 64
        or any(character not in "0123456789abcdef" for character in text)
        or len(set(text)) == 1
    ):
        raise ValueError(f"{name} must be a non-degenerate SHA256")
    return text


def _require_nonempty_strings(values: object, *, name: str) -> tuple[str, ...]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise ValueError(f"{name} must be a sequence")
    normalized = tuple(str(value).strip() for value in values)
    if not normalized or any(not value for value in normalized):
        raise ValueError(f"{name} must contain non-empty strings")
    return normalized


def validate_progression_contract(contract: Mapping[str, Any]) -> None:
    """Fail closed when either research path can silently rewrite the other."""

    if contract.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported research progression schema")
    if not str(contract.get("identity", "")).strip():
        raise ValueError("progression identity must be non-empty")

    hard = contract.get("hard_gate_path")
    owner = contract.get("owner_progression_path")
    if not isinstance(hard, Mapping) or not isinstance(owner, Mapping):
        raise ValueError("both hard-gate and owner-progression paths are required")

    _require_sha256(hard.get("predecessor_spec_sha256"), name="predecessor Spec")
    _require_sha256(hard.get("predecessor_report_sha256"), name="predecessor report")
    if not bool(hard.get("immutable", False)):
        raise ValueError("hard-gate predecessor must remain immutable")
    if not isinstance(hard.get("passed"), bool):
        raise ValueError("hard-gate result must be boolean")
    _require_nonempty_strings(hard.get("failed_gates", ()), name="failed_gates")

    if not bool(owner.get("owner_requested", False)):
        raise ValueError("owner progression requires an explicit owner request")
    if not bool(owner.get("risk_accepted", False)):
        raise ValueError("owner progression must record accepted residual risk")
    if not bool(owner.get("outcome_informed", False)):
        raise ValueError("post-result owner progression must remain outcome-informed")
    _require_nonempty_strings(owner.get("accepted_risks", ()), name="accepted_risks")
    if bool(owner.get("rewrites_hard_gate", True)):
        raise ValueError("owner progression cannot rewrite the hard-gate result")

    current = contract.get("current_permissions")
    if not isinstance(current, Mapping):
        raise ValueError("current_permissions must be present")
    for field in (
        "validation_read",
        "sealed_holdout_read",
        "action_authorized",
        "live_authorized",
    ):
        if bool(current.get(field, False)):
            raise ValueError(f"progression registration cannot grant {field}")
    if not bool(current.get("development_continuation_authorized", False)):
        raise ValueError("owner successor must explicitly authorize Development continuation")

    promotion = contract.get("promotion_routes")
    if not isinstance(promotion, Mapping):
        raise ValueError("promotion_routes must be present")
    if promotion.get("hard_gate_path") != STANDARD_PROMOTION:
        raise ValueError("hard-gate path must retain standard promotion semantics")
    if promotion.get("owner_progression_path") != OWNER_PROMOTION:
        raise ValueError("owner path must retain owner-risk promotion semantics")
    requirements = _require_nonempty_strings(
        promotion.get("shared_downstream_requirements", ()),
        name="shared_downstream_requirements",
    )
    required = {
        "positive_full_path_economic_evidence",
        "execution_and_shadow_parity",
        "tail_and_safety_gates",
        "promotion_controller_decision",
    }
    if not required.issubset(requirements):
        raise ValueError("promotion routes omit mandatory downstream safeguards")


def progression_contract_sha256(contract: Mapping[str, Any]) -> str:
    validate_progression_contract(contract)
    return canonical_sha256(contract)
