"""Freeze the lifecycle-repair successor to the BUY E3 owner release.

This release keeps the exact BUY E3 artifact and owner-risk authority from the
direct-v3 runtime, while truthfully separating completed historical mechanics
evidence from evidence that must be collected for the repaired runtime.
"""

from __future__ import annotations

import argparse
import hashlib
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

try:
    from scripts import f05_buy_e3_active_release as legacy_release
    from scripts import f05_buy_e3_direct_owner_release as parent_release
    from scripts import f05_buy_e3_lifecycle_reject_fix_supplement as supplement
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    import f05_buy_e3_active_release as legacy_release
    import f05_buy_e3_direct_owner_release as parent_release
    import f05_buy_e3_lifecycle_reject_fix_supplement as supplement

SCHEMA_VERSION: Final = (
    "causal_multichannel_window_boolean_cooldown_owner_buy_e3_direct_owner_active_release.v2"
)
IDENTITY: Final = SCHEMA_VERSION
STATUS: Final = "owner_authorized_direct_live_lifecycle_repair_pending_evidence"
EXACT_ARTIFACT_SHA256: Final = parent_release.EXACT_ARTIFACT_SHA256

PARENT_RELEASE_FILE_SHA256: Final = (
    "aacf30f0abc978b9a14570cb0082c3858b0f022c2f0cc9daa8a687d71932f396"
)
PARENT_RELEASE_CANONICAL_SHA256: Final = (
    "b5baea19a925b8fe8b1a8a8f1d387bfcc0c1aa0124b51108556e3df46ab59384"
)
PARENT_EXECUTION: Final = {
    "execution_commit": "1be0e062fe2c8ac12a34d5fc2193ca166898105a",
    "execution_tree": "ec54a9fbe5a4e476af4d6e58cc323804f0a2f275",
    "annotated_operational_tag": "f05-owner-buy-e3-direct-live-v3-20260824",
    "annotated_operational_tag_object": "00b5d8bb9078a04dee7e2ae2b3ecdec332698106",
    "tag_peeled_commit": "1be0e062fe2c8ac12a34d5fc2193ca166898105a",
}
PARENT_DIRECT_OWNER_RELEASE: Final = {
    "schema_version": parent_release.SCHEMA_VERSION,
    "status": parent_release.STATUS,
    "file_sha256": PARENT_RELEASE_FILE_SHA256,
    "canonical_field": "canonical_active_release_sha256",
    "canonical_sha256": PARENT_RELEASE_CANONICAL_SHA256,
    "execution": PARENT_EXECUTION,
}

AUTHORIZATION_BASIS: Final = {
    "authority": "explicit_owner_directive",
    "directive_id": "continue_exact_buy_e3_with_lifecycle_writer_repair_20260824",
    "owner_accepts_unfinished_current_runtime_evidence_risk": True,
    "lifecycle_repair_only": True,
    "does_not_relabel_research_evidence": True,
}
HISTORICAL_EVIDENCE_STATE: Final = {
    "attempt4_mechanics_and_stability_complete": True,
    "exact_v5_mechanics_recovery_complete": True,
    "attempt4_resource_or_activation_claimed": False,
    "research_supported": False,
}
EXACT_V5_RECOVERY: Final = {
    "schema_version": "f05_v5_exact_isolated_verify.v1",
    "status": "historical_v5_exact_bytes_recovered",
    "file_sha256": "0c6729a248a7e80c19d376a77cc08b6ad56849b2a42d782bbb5b3624f5cc4346",
    "canonical_field": "canonical_receipt_sha256",
    "canonical_sha256": "2efe1922f7c3d3156b8736d53127dcc81f3a4639ec13b6dc93ab788750bcc517",
    "size_bytes": 6975,
    "mode": "0600",
}
HISTORICAL_ATTEMPT4_ANCHOR: Final = {
    "schema_version": (
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_v1."
        "compatible_execution_attempt_interpreter_equivalence_successor.v2"
    ),
    "status": "compatible_runtime_frozen_not_activated_interpreter_equivalence",
    "file_sha256": "9cec4434cdfbb1070d6f783449f4b37f6f153488edf8f17d7d5293cba05ca1df",
    "canonical_field": "canonical_execution_attempt_sha256",
    "canonical_sha256": "1d43d67162b25f4a74318a2fb0edb7d945e6b56c304a5e213c41564ac495907f",
    "size_bytes": 15729,
    "mode": "0600",
}
PENDING_CURRENT_RUNTIME_EVIDENCE: Final = {
    "v4_resource_gate_complete": False,
    "v4_active_capture_complete": False,
    "cross_host_admission_complete": False,
    "lifecycle_orico_admission_complete": False,
    "final_evidence_composition_complete": False,
}
LIFECYCLE_FIX_CONTRACT: Final = {
    "e3_artifact_unchanged": True,
    "e3_decision_semantics_unchanged": True,
    "quote_price_and_size_semantics_unchanged": True,
    "buy_action_vocabulary_unchanged": True,
    "sell_runtime_unchanged": True,
    "new_strategy_arm_created": False,
    "allowed_change_classes": [
        "preactivation_reject_exchange_exposure_zero_consistency",
        "preactivation_reject_without_exchange_order_id_strict_acceptance",
        "direct_owner_release_v2_validation",
    ],
}
SCOPE: Final = dict(parent_release.SCOPE)
ROLLBACK: Final = dict(parent_release.ROLLBACK)
EVIDENCE_BOUNDARY: Final = dict(parent_release.EVIDENCE_BOUNDARY)

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
        "parent_direct_owner_release",
        "historical_evidence_state",
        "historical_attempt4_anchor",
        "exact_v5_recovery",
        "pending_current_runtime_evidence",
        "lifecycle_fix_contract",
        "lifecycle_fix_supplement",
        "rollback",
        "evidence_boundary",
        "canonical_active_release_sha256",
    }
)


class DirectOwnerReleaseV2Error(RuntimeError):
    """Raised when the lifecycle-repair release fails closed."""


def _parent_binding(path: Path) -> dict[str, Any]:
    document = legacy_release._open_document(path, "parent direct owner release")  # noqa: SLF001
    file_sha256 = hashlib.sha256(document.raw).hexdigest()
    payload = document.payload
    observed = {
        "schema_version": payload.get("schema_version"),
        "status": payload.get("status"),
        "file_sha256": file_sha256,
        "canonical_field": "canonical_active_release_sha256",
        "canonical_sha256": payload.get("canonical_active_release_sha256"),
        "execution": payload.get("execution"),
    }
    if observed != PARENT_DIRECT_OWNER_RELEASE:
        raise DirectOwnerReleaseV2Error("parent direct owner release identity drifted")
    if (
        legacy_release.document_sha256(
            payload,
            "canonical_active_release_sha256",
        )
        != PARENT_RELEASE_CANONICAL_SHA256
    ):
        raise DirectOwnerReleaseV2Error("parent direct owner release canonical drifted")
    return observed


def _historical_binding(
    path: Path,
    expected: Mapping[str, Any],
    label: str,
) -> dict[str, Any]:
    document = legacy_release._open_document(path, label)  # noqa: SLF001
    payload = document.payload
    observed: dict[str, Any] = {
        "schema_version": payload.get("schema_version"),
        "status": payload.get("status"),
        "file_sha256": hashlib.sha256(document.raw).hexdigest(),
        "canonical_field": expected["canonical_field"],
        "canonical_sha256": payload.get(str(expected["canonical_field"])),
        "size_bytes": len(document.raw),
        "mode": "0600",
    }
    if observed != expected:
        raise DirectOwnerReleaseV2Error(f"{label} identity drifted")
    if (
        legacy_release.document_sha256(
            payload,
            str(expected["canonical_field"]),
        )
        != expected["canonical_sha256"]
    ):
        raise DirectOwnerReleaseV2Error(f"{label} canonical drifted")
    return observed


def build_direct_owner_release_v2(
    *,
    repository_root: Path,
    annotated_operational_tag: str,
    artifact_paths: Mapping[str, Path],
    parent_direct_release_path: Path,
    exact_v5_verify_path: Path,
    attempt4_successor_path: Path,
    lifecycle_fix_supplement_path: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    execution = legacy_release._operational_git_identity(  # noqa: SLF001
        repository_root,
        annotated_operational_tag,
    )
    artifact, roles = parent_release._artifact_release(artifact_paths)  # noqa: SLF001
    parent = _parent_binding(parent_direct_release_path)
    exact_v5_recovery = _historical_binding(
        exact_v5_verify_path,
        EXACT_V5_RECOVERY,
        "exact V5 recovery",
    )
    historical_attempt4_anchor = _historical_binding(
        attempt4_successor_path,
        HISTORICAL_ATTEMPT4_ANCHOR,
        "historical Attempt4 anchor",
    )
    lifecycle_fix_supplement = supplement.supplement_binding(
        lifecycle_fix_supplement_path,
        repository_root=repository_root,
    )
    timestamp = generated_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    legacy_release._timestamp(timestamp, "direct owner v2 release timestamp")  # noqa: SLF001
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
        "parent_direct_owner_release": parent,
        "historical_evidence_state": dict(HISTORICAL_EVIDENCE_STATE),
        "historical_attempt4_anchor": historical_attempt4_anchor,
        "exact_v5_recovery": exact_v5_recovery,
        "pending_current_runtime_evidence": dict(PENDING_CURRENT_RUNTIME_EVIDENCE),
        "lifecycle_fix_contract": dict(LIFECYCLE_FIX_CONTRACT),
        "lifecycle_fix_supplement": dict(lifecycle_fix_supplement),
        "rollback": dict(ROLLBACK),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload["canonical_active_release_sha256"] = legacy_release.document_sha256(
        payload,
        "canonical_active_release_sha256",
    )
    return payload


def validate_direct_owner_release_v2(
    path: Path,
    *,
    repository_root: Path,
    parent_direct_release_path: Path,
    exact_v5_verify_path: Path,
    attempt4_successor_path: Path,
    lifecycle_fix_supplement_path: Path,
) -> dict[str, Any]:
    document = legacy_release._open_document(path, "direct owner v2 active release")  # noqa: SLF001
    payload = document.payload
    if set(payload) != TOP_LEVEL_FIELDS:
        raise DirectOwnerReleaseV2Error("direct owner v2 release fields drifted")
    fixed = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": STATUS,
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "outcome_informed_owner_override": True,
        "action_authorized": True,
        "live_authorized": True,
        "authorization_basis": AUTHORIZATION_BASIS,
        "scope": SCOPE,
        "parent_direct_owner_release": PARENT_DIRECT_OWNER_RELEASE,
        "historical_evidence_state": HISTORICAL_EVIDENCE_STATE,
        "historical_attempt4_anchor": HISTORICAL_ATTEMPT4_ANCHOR,
        "exact_v5_recovery": EXACT_V5_RECOVERY,
        "pending_current_runtime_evidence": PENDING_CURRENT_RUNTIME_EVIDENCE,
        "lifecycle_fix_contract": LIFECYCLE_FIX_CONTRACT,
        "lifecycle_fix_supplement": supplement.supplement_binding(
            lifecycle_fix_supplement_path,
            repository_root=repository_root,
        ),
        "rollback": ROLLBACK,
        "evidence_boundary": EVIDENCE_BOUNDARY,
    }
    if any(payload.get(field) != value for field, value in fixed.items()):
        raise DirectOwnerReleaseV2Error("direct owner v2 release authority drifted")
    legacy_release._timestamp(  # noqa: SLF001
        payload.get("generated_utc"),
        "direct owner v2 release timestamp",
    )
    canonical = legacy_release._require_sha256(  # noqa: SLF001
        payload.get("canonical_active_release_sha256"),
        "direct owner v2 release canonical SHA256",
    )
    if canonical != legacy_release.document_sha256(
        payload,
        "canonical_active_release_sha256",
    ):
        raise DirectOwnerReleaseV2Error("direct owner v2 release canonical drifted")
    execution = payload.get("execution")
    if not isinstance(execution, Mapping):
        raise DirectOwnerReleaseV2Error("direct owner v2 execution is missing")
    if dict(execution) != legacy_release._operational_git_identity(  # noqa: SLF001
        repository_root,
        str(execution.get("annotated_operational_tag", "")),
    ):
        raise DirectOwnerReleaseV2Error("direct owner v2 execution identity drifted")
    if payload.get("parent_direct_owner_release") != _parent_binding(parent_direct_release_path):
        raise DirectOwnerReleaseV2Error("direct owner v2 parent binding drifted")
    for field, source, expected, label in (
        (
            "exact_v5_recovery",
            exact_v5_verify_path,
            EXACT_V5_RECOVERY,
            "exact V5 recovery",
        ),
        (
            "historical_attempt4_anchor",
            attempt4_successor_path,
            HISTORICAL_ATTEMPT4_ANCHOR,
            "historical Attempt4 anchor",
        ),
    ):
        if payload.get(field) != _historical_binding(
            source,
            expected,
            label,
        ):
            raise DirectOwnerReleaseV2Error(f"direct owner v2 {field} binding drifted")
    exact_artifact = payload.get("exact_artifact")
    if not isinstance(exact_artifact, Mapping):
        raise DirectOwnerReleaseV2Error("direct owner v2 artifact is missing")
    if exact_artifact.get("artifact_sha256") != EXACT_ARTIFACT_SHA256:
        raise DirectOwnerReleaseV2Error("direct owner v2 artifact SHA256 drifted")
    legacy_release._validate_portable_release_bindings(  # noqa: SLF001
        exact_artifact.get("roles"),
        legacy_release.ARTIFACT_ROLES,
        "direct owner v2 artifact roles",
    )
    return dict(payload)


def finalize_direct_owner_release_v2(
    *,
    repository_root: Path,
    annotated_operational_tag: str,
    artifact_paths: Mapping[str, Path],
    parent_direct_release_path: Path,
    exact_v5_verify_path: Path,
    attempt4_successor_path: Path,
    lifecycle_fix_supplement_path: Path,
    output_path: Path,
) -> tuple[dict[str, Any], str]:
    payload = build_direct_owner_release_v2(
        repository_root=repository_root,
        annotated_operational_tag=annotated_operational_tag,
        artifact_paths=artifact_paths,
        parent_direct_release_path=parent_direct_release_path,
        exact_v5_verify_path=exact_v5_verify_path,
        attempt4_successor_path=attempt4_successor_path,
        lifecycle_fix_supplement_path=lifecycle_fix_supplement_path,
    )
    file_hash = legacy_release._write_exclusive(output_path, payload)  # noqa: SLF001
    validated = validate_direct_owner_release_v2(
        output_path,
        repository_root=repository_root,
        parent_direct_release_path=parent_direct_release_path,
        exact_v5_verify_path=exact_v5_verify_path,
        attempt4_successor_path=attempt4_successor_path,
        lifecycle_fix_supplement_path=lifecycle_fix_supplement_path,
    )
    if validated != payload:
        raise DirectOwnerReleaseV2Error("written direct owner v2 release changed")
    return payload, file_hash


def _artifact_paths(args: argparse.Namespace) -> dict[str, Path]:
    return {
        "manifest": args.artifact_manifest,
        "policy": args.policy,
        "predicate_bundle": args.predicate_bundle,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("finalize", "validate"):
        command = commands.add_parser(name)
        command.add_argument("--repository-root", type=Path, required=True)
        command.add_argument("--parent-direct-release", type=Path, required=True)
        command.add_argument("--exact-v5-verify", type=Path, required=True)
        command.add_argument("--attempt4-successor", type=Path, required=True)
        command.add_argument("--lifecycle-fix-supplement", type=Path, required=True)
        if name == "finalize":
            command.add_argument("--annotated-operational-tag", required=True)
            command.add_argument("--artifact-manifest", type=Path, required=True)
            command.add_argument("--policy", type=Path, required=True)
            command.add_argument("--predicate-bundle", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "validate":
        payload = validate_direct_owner_release_v2(
            args.output,
            repository_root=args.repository_root,
            parent_direct_release_path=args.parent_direct_release,
            exact_v5_verify_path=args.exact_v5_verify,
            attempt4_successor_path=args.attempt4_successor,
            lifecycle_fix_supplement_path=args.lifecycle_fix_supplement,
        )
        print(payload["canonical_active_release_sha256"])
        return 0
    payload, file_hash = finalize_direct_owner_release_v2(
        repository_root=args.repository_root,
        annotated_operational_tag=args.annotated_operational_tag,
        artifact_paths=_artifact_paths(args),
        parent_direct_release_path=args.parent_direct_release,
        exact_v5_verify_path=args.exact_v5_verify,
        attempt4_successor_path=args.attempt4_successor,
        lifecycle_fix_supplement_path=args.lifecycle_fix_supplement,
        output_path=args.output,
    )
    print(f"file_sha256={file_hash}")
    print(f"canonical_sha256={payload['canonical_active_release_sha256']}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
