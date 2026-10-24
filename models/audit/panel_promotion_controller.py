"""Panel-state controller kept separate from evidence scoring.

Scorecards classify and rank evidence. This controller alone decides whether a
frozen family may read the next panel. It never authorizes live deployment.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

SCHEMA_VERSION = "narrowgate_panel_promotion_controller.v1"

_REQUIRED_PRIOR_DECISION = {
    "validation": "development_validation_unlocked",
    "sealed_holdout": "validation_holdout_unlocked",
}


def _sha256(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def control_panel_promotion(
    scorecard: Mapping[str, Any],
    *,
    panel_identity_frozen: bool,
    family_status: str = "active",
    prior_panel_decision: str | None = None,
) -> dict[str, Any]:
    """Return a deterministic panel-unlock decision from a frozen scorecard."""

    panel_role = str(scorecard.get("panel_role", ""))
    profile = dict(scorecard.get("profile") or {})
    profile_id = str(profile.get("profile_id", ""))
    gates_passed = bool(
        (scorecard.get("validity") or {}).get("passed")
        and (scorecard.get("support") or {}).get("passed")
        and (scorecard.get("hard_gates") or {}).get("passed")
    )
    failures: list[str] = []
    if not str(scorecard.get("scorecard_sha256", "")):
        failures.append("scorecard_identity_missing")
    if not profile_id:
        failures.append("score_profile_identity_missing")
    if not bool(profile.get("frozen_before_outcome", False)):
        failures.append("score_profile_not_frozen_before_outcome")
    if not panel_identity_frozen:
        failures.append("panel_identity_not_frozen_before_outcome")
    if family_status != "active":
        failures.append(f"family_status_not_active:{family_status}")

    required_prior = _REQUIRED_PRIOR_DECISION.get(panel_role)
    if required_prior and prior_panel_decision != required_prior:
        failures.append(f"prior_panel_decision_required:{required_prior}")

    screening_only = bool(profile.get("screening_only", False))
    close_family = False
    unlock_allowed = False
    next_stage: str | None = None
    if failures:
        decision = "panel_unlock_blocked"
    elif screening_only:
        decision = "screening_only_no_panel_unlock"
    elif not gates_passed:
        decision = f"close_family_on_{panel_role or 'unknown_panel'}"
        close_family = True
    elif not bool(scorecard.get("ranking_eligible", False)):
        decision = "panel_unlock_blocked_unrankable_evidence"
    elif panel_role == "development":
        decision = "development_validation_unlocked"
        unlock_allowed = True
        next_stage = "validation"
    elif panel_role == "validation":
        decision = "validation_holdout_unlocked"
        unlock_allowed = True
        next_stage = "sealed_holdout"
    elif panel_role == "sealed_holdout":
        decision = "sealed_holdout_shadow_candidate"
        unlock_allowed = True
        next_stage = "shadow"
    else:
        decision = "panel_unlock_blocked_unknown_panel"

    output = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(scorecard.get("family_id", "")),
        "panel_role": panel_role,
        "profile_id": profile_id,
        "scorecard_sha256": str(scorecard.get("scorecard_sha256", "")),
        "family_status_before": family_status,
        "prior_panel_decision": prior_panel_decision,
        "panel_identity_frozen": bool(panel_identity_frozen),
        "gates_passed": gates_passed,
        "decision": decision,
        "unlock_allowed": unlock_allowed,
        "next_stage": next_stage,
        "close_family": close_family,
        "live_promotion_allowed": False,
        "failures": failures,
        "decision_sha256": "",
    }
    output["decision_sha256"] = _sha256(
        {key: value for key, value in output.items() if key != "decision_sha256"}
    )
    return output
