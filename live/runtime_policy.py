"""Fail-closed live policy authorities shared by every startup path."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

Q90_ACTION_OWNER_OVERRIDE_ENV = "NARROWGATE_ALLOW_UNREPAIRED_Q90_ACTION_DEPLOY"
Q90_POST_CANCEL_RECOVERY_CONTRACT_SUPPORTED = False
F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV = "NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_OWNER_DEPLOY"
F05_BOOLEAN_COOLDOWN_EVIDENCE_ROUTE = "owner_risk_accepted_promotion"
F05_BOOLEAN_COOLDOWN_HARD_GATES_PASSED = False
F05_BUY_E3_OWNER_OVERRIDE_ENV = "NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY"
F05_BUY_E3_EVIDENCE_ROUTE = "owner_risk_accepted_buy_e3_v1"
F05_BUY_E3_HARD_GATES_PASSED = False
F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV = "NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_PATH"
F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV = (
    "NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256"
)
F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV = (
    "NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256"
)
F05_BUY_E3_ACTIVE_RELEASE_AUTHORITY_SCHEMA = (
    "narrowgate_f05_buy_e3_active_release_runtime_authority.v1"
)
LIVE_SAFETY_SUCCESSOR_PATH_ENV = "NARROWGATE_LIVE_SAFETY_SUCCESSOR_PATH"
LIVE_SAFETY_SUCCESSOR_FILE_SHA256_ENV = (
    "NARROWGATE_LIVE_SAFETY_SUCCESSOR_FILE_SHA256"
)
LIVE_SAFETY_SUCCESSOR_CANONICAL_SHA256_ENV = (
    "NARROWGATE_LIVE_SAFETY_SUCCESSOR_CANONICAL_SHA256"
)
RUNTIME_POLICY_SCHEMA_VERSION = "narrowgate_runtime_policy.v1"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


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
            "owner-authorized risk-accepted deployment"
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
            "owner_risk_accepted_active" if enabled and override_requested else "disabled"
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
    """Require a separate owner grant for the BUY-only E3 artifact."""

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
            "owner risk-accepted deployment"
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
            "owner_risk_accepted_active" if enabled and override_requested else "disabled"
        ),
    }


def f05_buy_e3_active_release_runtime_authority(
    enabled: bool,
    *,
    environ: Mapping[str, str] | None = None,
    require_present: bool = True,
) -> dict[str, Any]:
    """Resolve the post-envelope active-release grant for one process start."""

    environment = os.environ if environ is None else environ
    active = bool(enabled)
    path = str(environment.get(F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV, "")).strip()
    file_sha256 = str(
        environment.get(F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV, "")
    ).strip().lower()
    canonical_sha256 = str(
        environment.get(F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV, "")
    ).strip().lower()
    if active and require_present:
        if environment.get(F05_BUY_E3_OWNER_OVERRIDE_ENV) != "1":
            raise ValueError("enabled F05 BUY E3 requires the owner override grant")
        candidate = Path(path).expanduser()
        if not path or "\x00" in path or not candidate.is_absolute():
            raise ValueError(
                f"enabled F05 BUY E3 requires an absolute {F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV}"
            )
        for label, value in (
            (F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV, file_sha256),
            (F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV, canonical_sha256),
        ):
            if _SHA256_RE.fullmatch(value) is None:
                raise ValueError(f"enabled F05 BUY E3 requires a valid {label}")
    elif not active:
        path = ""
        file_sha256 = ""
        canonical_sha256 = ""
    return {
        "schema_version": F05_BUY_E3_ACTIVE_RELEASE_AUTHORITY_SCHEMA,
        "required": active,
        "active_release_path": path,
        "active_release_file_sha256": file_sha256,
        "active_release_canonical_sha256": canonical_sha256,
    }


def live_safety_successor_runtime_authority(
    *,
    expected_manifest_file_sha256: str,
    expected_policy_file_sha256: str,
    expected_predicate_bundle_file_sha256: str,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Load the always-required shared execution-safety authority."""

    environment = os.environ if environ is None else environ
    path_text = str(environment.get(LIVE_SAFETY_SUCCESSOR_PATH_ENV, "")).strip()
    expected_file = str(
        environment.get(LIVE_SAFETY_SUCCESSOR_FILE_SHA256_ENV, "")
    ).strip().lower()
    expected_canonical = str(
        environment.get(LIVE_SAFETY_SUCCESSOR_CANONICAL_SHA256_ENV, "")
    ).strip().lower()
    candidate = Path(path_text).expanduser()
    if (
        not path_text
        or "\x00" in path_text
        or not candidate.is_absolute()
        or _SHA256_RE.fullmatch(expected_file) is None
        or _SHA256_RE.fullmatch(expected_canonical) is None
        or candidate.is_symlink()
        or not candidate.is_file()
    ):
        raise ValueError("live safety successor authority is missing or malformed")
    resolved = candidate.resolve(strict=True)
    metadata = resolved.stat()
    if metadata.st_nlink != 1 or metadata.st_mode & 0o777 != 0o600:
        raise ValueError("live safety successor permissions/link count drifted")
    before = resolved.read_bytes()
    if hashlib.sha256(before).hexdigest() != expected_file:
        raise ValueError("live safety successor file hash drifted")
    try:
        payload = json.loads(before)
    except json.JSONDecodeError as exc:
        raise ValueError("live safety successor is not JSON") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("live safety successor is not a mapping")
    from strategy import boolean_cooldown_buy_e3 as buy_e3

    identity = buy_e3._validate_active_release(  # noqa: SLF001
        payload,
        expected_canonical_sha256=expected_canonical,
        expected_artifact_sha256=buy_e3.DIRECT_OWNER_EXACT_ARTIFACT_SHA256,
        expected_manifest_file_sha256=str(expected_manifest_file_sha256).lower(),
        expected_policy_file_sha256=str(expected_policy_file_sha256).lower(),
        expected_predicate_bundle_file_sha256=str(
            expected_predicate_bundle_file_sha256
        ).lower(),
    )
    if payload.get("schema_version") != buy_e3.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA:
        raise ValueError("runtime requires the live safety successor schema")
    after = resolved.read_bytes()
    if before != after:
        raise ValueError("live safety successor changed while loading")
    return {
        "path": str(resolved),
        "file_sha256": expected_file,
        "canonical_sha256": expected_canonical,
        "execution_commit": identity["execution_commit"],
        "execution_tree": identity["execution_tree"],
        "active_config_file_sha256": identity["active_config_file_sha256"],
        "disabled_config_file_sha256": identity["disabled_config_file_sha256"],
        "native_module_sha256": str(payload["native_build"]["module_sha256"]),
        "native_wheel_sha256": str(payload["native_build"]["wheel_sha256"]),
        "native_soabi": str(payload["native_build"]["soabi"]),
        "native_build_receipt_sha256": str(
            payload["native_build"]["file_sha256"]
        ),
        "native_build_receipt_canonical_sha256": str(
            payload["native_build"]["canonical_sha256"]
        ),
        "runtime_lock_file_sha256": str(
            payload["native_build"]["runtime_lock_file_sha256"]
        ),
        "runtime_lock_path": str(payload["native_build"]["runtime_lock_path"]),
        "runtime_lock_canonical_sha256": str(
            payload["native_build"]["runtime_lock_canonical_sha256"]
        ),
        "wheelhouse_manifest_file_sha256": str(
            payload["native_build"]["wheelhouse_manifest_file_sha256"]
        ),
        "wheelhouse_path": str(payload["native_build"]["wheelhouse_path"]),
        "wheelhouse_canonical_sha256": str(
            payload["native_build"]["wheelhouse_canonical_sha256"]
        ),
        "install_receipt_path": str(
            payload["native_build"]["install_receipt_path"]
        ),
        "install_receipt_file_sha256": str(
            payload["native_build"]["install_receipt_file_sha256"]
        ),
        "install_receipt_canonical_sha256": str(
            payload["native_build"]["install_receipt_canonical_sha256"]
        ),
        "root_wheel_sha256": str(
            payload["native_build"]["root_wheel_sha256"]
        ),
        "root_wheel_path": str(payload["native_build"]["root_wheel_path"]),
        "native_wheel_path": str(payload["native_build"]["native_wheel_path"]),
        "installed_record_aggregate_sha256": str(
            payload["native_build"]["installed_record_aggregate_sha256"]
        ),
        "locked_runtime_interpreter": dict(
            payload["native_build"]["interpreter"]
        ),
    }


def require_f05_buy_e3_active_release_restart(
    previous: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> None:
    """Keep the post-envelope active-release grant immutable for a process."""

    fields = (
        F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV,
        F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV,
        F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV,
    )
    changed = [name for name in fields if previous.get(name) != candidate.get(name)]
    if changed:
        raise ValueError(
            "F05 BUY E3 active-release authority is restart-only; changed field(s): "
            + ", ".join(changed)
        )


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
