"""Release-bound policy admission and restart-only configuration rules."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

DEPLOYMENT_ENVELOPE_PATH_ENV = "NARROWGATE_DEPLOYMENT_ENVELOPE_PATH"
DEPLOYMENT_ENVELOPE_CANONICAL_SHA256_ENV = "NARROWGATE_DEPLOYMENT_ENVELOPE_CANONICAL_SHA256"


def admit_runtime_policies(
    strategy: Mapping[str, Any],
    *,
    deployment_authority: Mapping[str, Any],
) -> dict[str, Any]:
    """Admit once, after verifying the envelope and its exact config/artifacts.

    This consumes the already verified release, not environment switches or
    historical research verdicts. Callers must not pass an unverified JSON
    object as deployment authority. The result is in-memory startup state;
    recording/logging it must not re-evaluate permission.
    """
    from live.deployment_runtime import DEPLOYMENT_POLICY_CONFIG_FIELDS

    enabled = sorted(
        policy
        for policy, field in DEPLOYMENT_POLICY_CONFIG_FIELDS.items()
        if strategy.get(field, False)
    )
    # The envelope loader owns schema, allowlist and byte verification.
    # This boundary only compares requested actions with that verified grant.
    approved = deployment_authority["policy_approvals"]
    missing = set(enabled) - set(approved)
    if missing:
        raise ValueError("release does not approve enabled policy: " + ", ".join(sorted(missing)))

    return {
        "approved_policies": enabled,
        "authorization_source": "deployment_envelope",
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


def require_f05_boolean_cooldown_restart(
    previous: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> None:
    """Protect executable policy bindings, not descriptive research annotations."""

    fields = (
        "boolean_cooldown_policy_enabled",
        "boolean_cooldown_policy_path",
        "boolean_cooldown_predicate_bundle_path",
        "boolean_cooldown_ema_warmup_s",
    )
    changed = [name for name in fields if previous.get(name) != candidate.get(name)]
    if changed:
        raise ValueError(
            "F05 Boolean cooldown policy is restart-only; changed field(s): " + ", ".join(changed)
        )


def deployment_envelope_runtime_authority(
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Load a private, content-addressed deployment envelope.

    The caller supplies one release-root digest. Nested build/config/policy
    manifests verify their own leaves; this policy layer does not repeat them.
    The selected config is one immutable member of the envelope; rollback is a
    separate activation rather than a second config role in the same release.
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
        )
    except (OSError, deployment_runtime.LockedRuntimeError) as exc:
        raise ValueError(f"private deployment envelope rejected: {exc}") from exc


def require_f05_buy_e3_restart(
    previous: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> None:
    """Keep executable BUY E3 bindings restart-only, excluding annotations."""

    fields = (
        "buy_e3_cooldown_policy_enabled",
        "buy_e3_cooldown_artifact_manifest_path",
        "buy_e3_cooldown_policy_path",
        "buy_e3_cooldown_predicate_bundle_path",
        "buy_e3_cooldown_ema_warmup_s",
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
