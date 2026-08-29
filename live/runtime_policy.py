"""Fail-closed live policy authorities shared by every startup path."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

Q90_ACTION_OWNER_OVERRIDE_ENV = "NARROWGATE_ALLOW_Q90_PRIVATE_DEPLOY"
Q90_POST_CANCEL_RECOVERY_CONTRACT_SUPPORTED = False
F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV = "NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_PRIVATE_DEPLOY"
F05_BOOLEAN_COOLDOWN_EVIDENCE_ROUTE = "private_deployment_approval"
F05_BOOLEAN_COOLDOWN_HARD_GATES_PASSED = False
F05_BUY_E3_OWNER_OVERRIDE_ENV = "NARROWGATE_ALLOW_F05_BUY_E3_PRIVATE_DEPLOY"
F05_BUY_E3_EVIDENCE_ROUTE = "private_deployment_buy_e3"
F05_BUY_E3_HARD_GATES_PASSED = False
DEPLOYMENT_ENVELOPE_PATH_ENV = "NARROWGATE_DEPLOYMENT_ENVELOPE_PATH"
DEPLOYMENT_ENVELOPE_CANONICAL_SHA256_ENV = "NARROWGATE_DEPLOYMENT_ENVELOPE_CANONICAL_SHA256"
RUNTIME_POLICY_SCHEMA_VERSION = "narrowgate_runtime_policy.v1"


def q90_action_runtime_policy(
    action_enabled: bool,
    *,
    environ: Mapping[str, str] | None = None,
    post_cancel_recovery_supported: bool = (Q90_POST_CANCEL_RECOVERY_CONTRACT_SUPPORTED),
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
            f"{Q90_ACTION_OWNER_OVERRIDE_ENV}=1 only with an explicit private "
            "deployment approval"
        )

    if not enabled:
        authority = "action_suspended_shadow_only"
    elif contract_supported:
        authority = "post_cancel_recovery_contract_supported"
    else:
        authority = "private_deployment_approved"

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
    """Require explicit private approval for the historical F05 policy."""

    environment = os.environ if environ is None else environ
    override_requested = environment.get(F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV) == "1"
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
            "approved private deployment"
        )
    return {
        "schema_version": RUNTIME_POLICY_SCHEMA_VERSION,
        "f05_boolean_cooldown_enabled": bool(enabled),
        "f05_boolean_cooldown_hard_gates_passed": (F05_BOOLEAN_COOLDOWN_HARD_GATES_PASSED),
        "f05_boolean_cooldown_evidence_route": normalized_route,
        "f05_boolean_cooldown_owner_override_env": (F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV),
        "f05_boolean_cooldown_owner_override_requested": override_requested,
        "f05_boolean_cooldown_owner_override_effective": bool(enabled and override_requested),
        "f05_boolean_cooldown_runtime_authority": (
            "private_deployment_approved" if enabled and override_requested else "disabled"
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
            "F05 Boolean cooldown policy is restart-only; changed field(s): " + ", ".join(changed)
        )


def f05_buy_e3_runtime_policy(
    enabled: bool,
    *,
    evidence_route: str,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Require separate private approval for the historical BUY-only E3 artifact."""

    environment = os.environ if environ is None else environ
    override_requested = environment.get(F05_BUY_E3_OWNER_OVERRIDE_ENV) == "1"
    normalized_route = str(evidence_route).strip()
    if enabled and normalized_route != F05_BUY_E3_EVIDENCE_ROUTE:
        raise ValueError(
            "enabled F05 BUY E3 policy must retain the permanent "
            f"{F05_BUY_E3_EVIDENCE_ROUTE} evidence label"
        )
    if enabled and not override_requested:
        raise ValueError(
            "F05 BUY E3 formal hierarchy and hard gates did not pass; set "
            f"{F05_BUY_E3_OWNER_OVERRIDE_ENV}=1 only for the explicit "
            "approved private deployment"
        )
    return {
        "schema_version": RUNTIME_POLICY_SCHEMA_VERSION,
        "f05_buy_e3_enabled": bool(enabled),
        "f05_buy_e3_research_supported": False,
        "f05_buy_e3_hard_gates_passed": F05_BUY_E3_HARD_GATES_PASSED,
        "f05_buy_e3_evidence_route": normalized_route,
        "f05_buy_e3_owner_override_env": F05_BUY_E3_OWNER_OVERRIDE_ENV,
        "f05_buy_e3_owner_override_requested": override_requested,
        "f05_buy_e3_owner_override_effective": bool(enabled and override_requested),
        "f05_buy_e3_runtime_authority": (
            "private_deployment_approved" if enabled and override_requested else "disabled"
        ),
    }


def deployment_envelope_runtime_authority(
    *,
    buy_e3_enabled: bool,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Load a private, content-addressed deployment envelope.

    The caller supplies one release-root digest.  Nested build/config/policy
    manifests verify their own leaves; this policy layer does not repeat them.
    """

    environment = os.environ if environ is None else environ
    path_text = str(environment.get(DEPLOYMENT_ENVELOPE_PATH_ENV, "")).strip()
    expected_root = (
        str(environment.get(DEPLOYMENT_ENVELOPE_CANONICAL_SHA256_ENV, "")).strip().lower()
    )
    candidate = Path(path_text).expanduser()
    if not path_text or "\x00" in path_text or not candidate.is_absolute():
        raise ValueError("private deployment envelope is missing or malformed")
    from live import deployment_runtime

    try:
        return deployment_runtime.load_deployment_envelope(
            candidate,
            expected_root_sha256=expected_root,
            buy_e3_enabled=buy_e3_enabled,
        )
    except (OSError, deployment_runtime.LockedRuntimeError) as exc:
        raise ValueError(f"private deployment envelope rejected: {exc}") from exc


def require_f05_buy_e3_restart(
    previous: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> None:
    """Keep the BUY E3 artifact and all bindings restart-only."""

    fields = (
        "buy_e3_cooldown_policy_enabled",
        "buy_e3_cooldown_artifact_manifest_path",
        "buy_e3_cooldown_artifact_manifest_sha256",
        "buy_e3_cooldown_artifact_sha256",
        "buy_e3_cooldown_policy_path",
        "buy_e3_cooldown_policy_sha256",
        "buy_e3_cooldown_predicate_bundle_path",
        "buy_e3_cooldown_predicate_bundle_sha256",
        "buy_e3_cooldown_ema_warmup_s",
        "buy_e3_cooldown_evidence_route",
    )
    changed = [name for name in fields if previous.get(name) != candidate.get(name)]
    if changed:
        raise ValueError(
            "F05 BUY E3 cooldown policy is restart-only; changed field(s): " + ", ".join(changed)
        )


def write_runtime_identity(path: Path, identity: Mapping[str, Any]) -> None:
    """Atomically and durably persist the observed startup identity."""
    candidate = path.expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    parent = candidate.parent.resolve()
    parent.mkdir(parents=True, exist_ok=True)
    destination = parent / candidate.name
    if destination.is_symlink():
        raise ValueError("runtime identity destination must not be a symlink")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.tmp.",
        dir=parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(
                dict(identity),
                handle,
                allow_nan=False,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        destination.chmod(0o600)
        directory_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
