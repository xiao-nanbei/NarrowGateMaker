#!/usr/bin/env python3
"""Freeze the explicit owner override that permits exact BUY E3 to run early.

This release identity is intentionally separate from the normal v1 active
release.  It does not claim that the Attempt4/V5 evidence chain is complete
and it cannot be interpreted as research support.  The exact frozen artifact,
clean operational checkout, rollback contract, and evidence omissions remain
machine bound.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

try:
    from scripts import f05_buy_e3_active_release as legacy_release
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    import f05_buy_e3_active_release as legacy_release

SCHEMA_VERSION: Final = (
    "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
    "direct_owner_active_release.v1"
)
IDENTITY: Final = SCHEMA_VERSION
STATUS: Final = "owner_authorized_direct_live_with_incomplete_evidence"
EXACT_ARTIFACT_SHA256: Final = (
    "17e99df737157c6587602e6b496eadbecbed0a98d025da1d1db4cc8ef670786d"
)

AUTHORIZATION_BASIS: Final = {
    "authority": "explicit_owner_directive",
    "directive_id": "deploy_exact_buy_e3_while_v5_rebuild_continues_20260824",
    "owner_accepts_unfinished_evidence_risk": True,
    "live_timing_override_only": True,
    "does_not_relabel_research_evidence": True,
}
INCOMPLETE_EVIDENCE: Final = {
    "legacy_v1_evidence_chain_complete": False,
    "attempt4_final_receipt_complete": False,
    "v5_exact_panel_rebuild_complete": False,
    "final_composition_complete": False,
    "concurrent_resource_window_receipt_complete": False,
    "post_disabled_activation_envelope_complete": False,
    "panel_rebuild_continues": True,
    "legacy_v1_evidence_roles_bound": False,
    "release_must_not_be_cited_as_an_evidence_gate_pass": True,
    "unresolved_or_unbound_gates": [
        "exact_v5_panel_hash_reproduction",
        "compatible_execution_attempt_final_receipt",
        "final_composition",
        "concurrent_resource_window_receipt",
        "post_disabled_activation_envelope",
    ],
}
SCOPE: Final = {
    "side": "BUY",
    "trigger": "exposure_increasing_executed_fill",
    "output": "total_cooldown",
    "reducing_buy_unchanged": True,
    "sell_owner_policy_unchanged": True,
}
ROLLBACK: Final = {
    "buy_e3_enabled": False,
    "buy_deadline_identity": "B0",
    "e3_deadline_imported": False,
    "b0_seconds": 85,
    "b0_multiplier": "consecutive_fill_units",
    "b0_contract": "85s_x_consecutive_fill_units",
}
EVIDENCE_BOUNDARY: Final = {
    "old_oof_applies_to_learning_algorithm_only": True,
    "exact_artifact_oof_available": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "shadow_created": False,
    "companion_created": False,
    "new_economic_arm_run": False,
}
TOP_LEVEL_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "research_supported",
        "formal_hierarchy_passed",
        "formal_hard_gates_passed",
        "owner_risk_accepted",
        "outcome_informed_owner_override",
        "action_authorized",
        "live_authorized",
        "authorization_basis",
        "scope",
        "execution",
        "exact_artifact",
        "incomplete_evidence",
        "rollback",
        "evidence_boundary",
        "canonical_active_release_sha256",
    }
)


class DirectOwnerReleaseError(RuntimeError):
    """Raised when the direct owner release fails closed."""


def _artifact_release(
    artifact_paths: Mapping[str, Path],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    documents = legacy_release._read_role_documents(  # noqa: SLF001
        artifact_paths,
        legacy_release.ARTIFACT_ROLES,
    )
    legacy_release._assert_inode_uniqueness(list(documents.values()))  # noqa: SLF001
    artifact = legacy_release._validate_artifact_documents(documents)  # noqa: SLF001
    if artifact["artifact_sha256"] != EXACT_ARTIFACT_SHA256:
        raise DirectOwnerReleaseError("direct release exact BUY E3 artifact drifted")
    roles = legacy_release._release_binding_map(  # noqa: SLF001
        documents,
        legacy_release.ARTIFACT_ROLES,
    )
    return artifact, roles


def build_direct_owner_release(
    *,
    repository_root: Path,
    annotated_operational_tag: str,
    artifact_paths: Mapping[str, Path],
    generated_utc: str | None = None,
) -> dict[str, Any]:
    """Build an explicit, non-research direct owner release in memory."""

    execution = legacy_release._operational_git_identity(  # noqa: SLF001
        repository_root,
        annotated_operational_tag,
    )
    artifact, roles = _artifact_release(artifact_paths)
    timestamp = generated_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    legacy_release._timestamp(timestamp, "direct owner release timestamp")  # noqa: SLF001
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": STATUS,
        "generated_utc": timestamp,
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "outcome_informed_owner_override": True,
        "action_authorized": True,
        "live_authorized": True,
        "authorization_basis": dict(AUTHORIZATION_BASIS),
        "scope": dict(SCOPE),
        "execution": execution,
        "exact_artifact": {
            "artifact_sha256": artifact["artifact_sha256"],
            "roles": roles,
        },
        "incomplete_evidence": dict(INCOMPLETE_EVIDENCE),
        "rollback": dict(ROLLBACK),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload["canonical_active_release_sha256"] = legacy_release.document_sha256(
        payload,
        "canonical_active_release_sha256",
    )
    return payload


def validate_direct_owner_release(
    path: Path,
    *,
    repository_root: Path,
) -> dict[str, Any]:
    """Independently validate an immutable direct owner release."""

    document = legacy_release._open_document(path, "direct owner active release")  # noqa: SLF001
    payload = document.payload
    if set(payload) != TOP_LEVEL_FIELDS:
        raise DirectOwnerReleaseError("direct owner release fields drifted")
    if (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("identity") != IDENTITY
        or payload.get("status") != STATUS
        or payload.get("research_supported") is not False
        or payload.get("formal_hierarchy_passed") is not False
        or payload.get("formal_hard_gates_passed") is not False
        or payload.get("owner_risk_accepted") is not True
        or payload.get("outcome_informed_owner_override") is not True
        or payload.get("action_authorized") is not True
        or payload.get("live_authorized") is not True
        or payload.get("authorization_basis") != AUTHORIZATION_BASIS
        or payload.get("scope") != SCOPE
        or payload.get("incomplete_evidence") != INCOMPLETE_EVIDENCE
        or payload.get("rollback") != ROLLBACK
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
    ):
        raise DirectOwnerReleaseError("direct owner release authority drifted")
    legacy_release._timestamp(  # noqa: SLF001
        payload.get("generated_utc"),
        "direct owner release timestamp",
    )
    canonical = legacy_release._require_sha256(  # noqa: SLF001
        payload.get("canonical_active_release_sha256"),
        "direct owner release canonical SHA256",
    )
    if canonical != legacy_release.document_sha256(
        payload,
        "canonical_active_release_sha256",
    ):
        raise DirectOwnerReleaseError("direct owner release canonical SHA256 drifted")
    execution = payload.get("execution")
    if not isinstance(execution, Mapping):
        raise DirectOwnerReleaseError("direct owner release execution is missing")
    observed_execution = legacy_release._operational_git_identity(  # noqa: SLF001
        repository_root,
        str(execution.get("annotated_operational_tag", "")),
    )
    if dict(execution) != observed_execution:
        raise DirectOwnerReleaseError("direct owner release execution identity drifted")
    exact_artifact = payload.get("exact_artifact")
    if not isinstance(exact_artifact, Mapping):
        raise DirectOwnerReleaseError("direct owner release artifact is missing")
    if exact_artifact.get("artifact_sha256") != EXACT_ARTIFACT_SHA256:
        raise DirectOwnerReleaseError("direct owner release artifact SHA256 drifted")
    roles = legacy_release._validate_portable_release_bindings(  # noqa: SLF001
        exact_artifact.get("roles"),
        legacy_release.ARTIFACT_ROLES,
        "direct owner artifact roles",
    )
    if roles["manifest"].get("canonical_sha256") != EXACT_ARTIFACT_SHA256:
        raise DirectOwnerReleaseError("direct owner manifest canonical identity drifted")
    return dict(payload)


def finalize_direct_owner_release(
    *,
    repository_root: Path,
    annotated_operational_tag: str,
    artifact_paths: Mapping[str, Path],
    output_path: Path,
    generated_utc: str | None = None,
) -> tuple[dict[str, Any], str]:
    payload = build_direct_owner_release(
        repository_root=repository_root,
        annotated_operational_tag=annotated_operational_tag,
        artifact_paths=artifact_paths,
        generated_utc=generated_utc,
    )
    file_hash = legacy_release._write_exclusive(output_path, payload)  # noqa: SLF001
    validated = validate_direct_owner_release(output_path, repository_root=repository_root)
    if validated != payload:
        raise DirectOwnerReleaseError("written direct owner release changed")
    return payload, file_hash


def _artifact_paths(args: argparse.Namespace) -> dict[str, Path]:
    return {
        "manifest": args.artifact_manifest,
        "policy": args.policy,
        "predicate_bundle": args.predicate_bundle,
    }


def _add_build_inputs(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--annotated-operational-tag", required=True)
    parser.add_argument("--artifact-manifest", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--predicate-bundle", type=Path, required=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    _add_build_inputs(build)
    finalize = subparsers.add_parser("finalize")
    _add_build_inputs(finalize)
    finalize.add_argument("--output", type=Path, required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--repository-root", type=Path, required=True)
    validate.add_argument("--receipt", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "validate":
        payload = validate_direct_owner_release(
            args.receipt,
            repository_root=args.repository_root,
        )
        print(payload["canonical_active_release_sha256"])
        return 0
    if args.command == "build":
        payload = build_direct_owner_release(
            repository_root=args.repository_root,
            annotated_operational_tag=args.annotated_operational_tag,
            artifact_paths=_artifact_paths(args),
        )
        print(json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True))
        return 0
    payload, file_hash = finalize_direct_owner_release(
        repository_root=args.repository_root,
        annotated_operational_tag=args.annotated_operational_tag,
        artifact_paths=_artifact_paths(args),
        output_path=args.output,
    )
    print(file_hash)
    print(payload["canonical_active_release_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
