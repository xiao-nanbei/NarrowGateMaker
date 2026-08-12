"""Fail-closed live policy authorities shared by every startup path."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

Q90_ACTION_OWNER_OVERRIDE_ENV = (
    "NARROWGATE_ALLOW_UNREPAIRED_Q90_ACTION_DEPLOY"
)
Q90_POST_CANCEL_RECOVERY_CONTRACT_SUPPORTED = False
F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV = (
    "NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_OWNER_DEPLOY"
)
F05_BOOLEAN_COOLDOWN_EVIDENCE_ROUTE = "owner_risk_accepted_promotion"
F05_BOOLEAN_COOLDOWN_HARD_GATES_PASSED = False
RUNTIME_POLICY_SCHEMA_VERSION = "narrowgate_runtime_policy.v1"


def q90_action_runtime_policy(
    action_enabled: bool,
    *,
    environ: Mapping[str, str] | None = None,
    post_cancel_recovery_supported: bool = (
        Q90_POST_CANCEL_RECOVERY_CONTRACT_SUPPORTED
    ),
) -> dict[str, Any]:
    """Return q90 authority or reject an unsupported action at runtime."""
    environment = os.environ if environ is None else environ
    override_requested = environment.get(Q90_ACTION_OWNER_OVERRIDE_ENV) == "1"
    enabled = bool(action_enabled)
    contract_supported = bool(post_cancel_recovery_supported)

    if enabled and not contract_supported and not override_requested:
        raise ValueError(
            "strategy.dynamic_fill_hazard_action_enabled cannot run while "
            "the cancel-ACK terminal active-order risk-set and "
            "POST_CANCEL_RECOVERY contract is unresolved; keep shadow enabled "
            "with action disabled, or set "
            f"{Q90_ACTION_OWNER_OVERRIDE_ENV}=1 only for an explicit owner "
            "risk-accepted override"
        )

    if not enabled:
        authority = "action_suspended_shadow_only"
    elif contract_supported:
        authority = "post_cancel_recovery_contract_supported"
    else:
        authority = "owner_risk_accepted_override"

    return {
        "schema_version": RUNTIME_POLICY_SCHEMA_VERSION,
        "dynamic_fill_hazard_action_enabled": enabled,
        "q90_post_cancel_recovery_contract_supported": contract_supported,
        "q90_action_runtime_authority": authority,
        "q90_owner_override_env": Q90_ACTION_OWNER_OVERRIDE_ENV,
        "q90_owner_override_requested": override_requested,
        "q90_owner_override_effective": bool(
            enabled and not contract_supported and override_requested
        ),
    }


def require_q90_action_restart(
    previous_action_enabled: bool,
    candidate_action_enabled: bool,
) -> None:
    """Keep the startup identity authoritative across SIGHUP reloads."""
    if bool(previous_action_enabled) != bool(candidate_action_enabled):
        raise ValueError(
            "strategy.dynamic_fill_hazard_action_enabled cannot change via "
            "SIGHUP; restart through live/run.sh so deploy preflight and the "
            "runtime identity are regenerated"
        )


def f05_boolean_cooldown_runtime_policy(
    enabled: bool,
    *,
    evidence_route: str,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Require an explicit owner grant for the positive-estimate F05 policy."""

    environment = os.environ if environ is None else environ
    override_requested = (
        environment.get(F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV) == "1"
    )
    normalized_route = str(evidence_route).strip()
    if enabled and normalized_route != F05_BOOLEAN_COOLDOWN_EVIDENCE_ROUTE:
        raise ValueError(
            "enabled F05 Boolean cooldown policy must retain the permanent "
            f"{F05_BOOLEAN_COOLDOWN_EVIDENCE_ROUTE} evidence label"
        )
    if enabled and not override_requested:
        raise ValueError(
            "F05 Boolean cooldown hard gates did not pass; set "
            f"{F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV}=1 only for the "
            "owner-authorized risk-accepted deployment"
        )
    return {
        "schema_version": RUNTIME_POLICY_SCHEMA_VERSION,
        "f05_boolean_cooldown_enabled": bool(enabled),
        "f05_boolean_cooldown_hard_gates_passed": (
            F05_BOOLEAN_COOLDOWN_HARD_GATES_PASSED
        ),
        "f05_boolean_cooldown_evidence_route": normalized_route,
        "f05_boolean_cooldown_owner_override_env": (
            F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV
        ),
        "f05_boolean_cooldown_owner_override_requested": override_requested,
        "f05_boolean_cooldown_owner_override_effective": bool(
            enabled and override_requested
        ),
        "f05_boolean_cooldown_runtime_authority": (
            "owner_risk_accepted_active"
            if enabled and override_requested
            else "disabled"
        ),
    }


def require_f05_boolean_cooldown_restart(
    previous: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> None:
    """Prevent SIGHUP from changing an active policy or any artifact binding."""

    fields = (
        "boolean_cooldown_policy_enabled",
        "boolean_cooldown_policy_path",
        "boolean_cooldown_policy_sha256",
        "boolean_cooldown_predicate_bundle_path",
        "boolean_cooldown_predicate_bundle_sha256",
        "boolean_cooldown_ema_warmup_s",
        "boolean_cooldown_evidence_route",
    )
    changed = [name for name in fields if previous.get(name) != candidate.get(name)]
    if changed:
        raise ValueError(
            "F05 Boolean cooldown policy is restart-only; changed field(s): "
            + ", ".join(changed)
        )


def write_runtime_identity(path: Path, identity: Mapping[str, Any]) -> None:
    """Atomically persist the machine-readable startup identity."""
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        temporary.write_text(
            json.dumps(identity, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
