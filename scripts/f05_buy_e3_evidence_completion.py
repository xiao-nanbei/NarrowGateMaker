#!/usr/bin/env python3
"""Complete the BUY E3 evidence record without replacing live authority.

The immutable direct-v3 owner release remains the only runtime authority.  This
module creates additive, create-only receipts for evidence that completed after
that release.  It never reads economic outcomes, Validation, or sealed holdout
data and it never deploys, scores, or changes a strategy arm.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_amendment_v2 as gate_v2,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_v1 as gate_v1,
)
from scripts import f05_buy_e3_active_release as release_io
from scripts import f05_buy_e3_direct_owner_release as direct_release
from scripts import f05_buy_e3_execution_attempt as legacy_attempt

OWNER: Final = "causal_multichannel_window_boolean_cooldown_owner_buy_e3_v1"
ARTIFACT_SHA256: Final = direct_release.EXACT_ARTIFACT_SHA256

DIRECT_COMMIT: Final = "1be0e062fe2c8ac12a34d5fc2193ca166898105a"
DIRECT_TREE: Final = "ec54a9fbe5a4e476af4d6e58cc323804f0a2f275"
DIRECT_TAG: Final = "f05-owner-buy-e3-direct-live-v3-20260824"
DIRECT_TAG_OBJECT: Final = "00b5d8bb9078a04dee7e2ae2b3ecdec332698106"
DIRECT_RELEASE_FILE_SHA256: Final = (
    "aacf30f0abc978b9a14570cb0082c3858b0f022c2f0cc9daa8a687d71932f396"
)
DIRECT_RELEASE_CANONICAL_SHA256: Final = (
    "b5baea19a925b8fe8b1a8a8f1d387bfcc0c1aa0124b51108556e3df46ab59384"
)
FLAT_POSITION_FIX_COMMIT: Final = "6007bfabba9995d4d9a32e075a718eb7b50f4136"
SPARSE_WINDOW_FIX_COMMIT: Final = DIRECT_COMMIT

ATTEMPT4_COMMIT: Final = "bba4396a9fdc4dff397795c5501cab5bb78ea9b0"
ATTEMPT4_TAG: Final = "f05-owner-buy-e3-live-attempt4-20260823"

V5_SCHEMA: Final = "f05_v5_exact_isolated_verify.v1"
V5_STATUS: Final = "historical_v5_exact_bytes_recovered"
V5_STUDY_SHA256: Final = "5a293b1c895f91693424489acdd993540b2f6c2ba0d3a6d26553635d044c8486"
V5_MODEL_CENSUS_SHA256: Final = "74a0e698fe286e3b5efa25d4ebc5c94677a4a2a3ec8014683a5695e1ea563ed6"
V5_INPUT_SHA256: Final = "717d955aa194b1de463615d5263f241d73ae431f245f8a0787174ba4678b3e3f"
V5_DAY_COUNT: Final = 30
V5_OPPORTUNITY_COUNT: Final = 3516
V5_RAW_SHA256: Final = {
    "metadata": "84cd849e21210027254d8a364efc553ee8d65d236364d246bdc3b88e669c287c",
    "boolean_features": "1fd11ddef21e244a3e4e770c9fb52d11194b9275f2334c4bbc21ac54979cf7a1",
    "continuous_features": "2e373a46863190131cf3decfd2f95b8ac5b1da0fea1c0e244e17180fb9e9d8e2",
    "exact_owner_actions": "d0e5e72232f19688f30010b01b22941eea253245a8b848b71e51c31b5178365f",
    "replay_inputs": "e8062e6354e6db9501d0d19746b6e64e2bbe32284aade34d9a2e46f35405d41d",
}
V5_FRAME_SHA256: Final = {
    "metadata": "b2997df96d21d22991ddb8ea6adad81bec1cea8263fe42c40cee8e333664ad00",
    "boolean_features": "7c4c62f7f6eb30a8f726440e3e6782dd53ac9c10d956dd6f2c64774586e23fd3",
    "primitive_boolean_features": "a38e031e190bee6dce24e23b138f23961b306075f548938b560a25a9407367af",
    "continuous_features": "5fd7df17c9114530abbd4c9e5cf6a9bcda446b8b860a667abbb4fcae764f4b0e",
    "exact_owner_actions": "9e6138032e4ccf048b66093786518be1a0c484bbb910ffe24a1556c77c2a8790",
    "replay_inputs": "6cff57a4d1cf8139d9d29a9933e2d2e119ba12a17be1438cd8a837bc44f1016a",
}
V5_ENVELOPE_SHA256: Final = {
    "builder_manifest_file": "ca0e0b02dad04e97120aa09a374c781dec99d97087cfa7695ddd920ab5d83156",
    "builder_manifest_canonical": "7438d19c9081fc56d1b35af78fb06f6f2a2b489fe95e209c8f38f496eb3034e0",
    "merged_panel_manifest_file": "a814e0e3edba1279b8d21058e880795be5b0fa9ccac845e1e6faa914d3698fb7",
    "merged_panel_manifest_canonical": "9920a13c7f0e6748b52a88ff818fa973f8816d57e51fbfcff1f77701c9ee8f20",
    "portable_replay_binding_file": "b5bbeb01f01fabef8c99df55ecd6b649960d3e9b86e0576e7e01d77ea8e46c63",
    "mechanics_manifest_file": "85e8bbcf30396a6353289f5e69b87fe9913ccb20d55a6d61edf35ff0dc1ec4a2",
    "mechanics_manifest_canonical": "c90930f9bdb51996af33efaeb9ef4d5386716f3ba911fcf2e1e935a65a0c8cab",
}
V5_ROW_KEY_SHA256: Final = "e481d8a61eecb71a36e6e3c8f2be1630c483a466a5418adc583d6398c29330ec"
V5_MECHANICS_MANIFEST: Final = (
    "/Volumes/ORICO/MarketData/NarrowGate_BTCUSDC/reports/"
    "f05_full_multiscale_offline_mechanics_v1/canonical_offline_v1/"
    "mechanics_panel_manifest.json"
)

FOCUSED_SCHEMA: Final = f"{OWNER}.direct_v3_runtime_successor_regression.v1"
FOCUSED_STATUS: Final = "direct_v3_runtime_successor_regression_passed"
ACTIVE_CAPTURE_SCHEMA: Final = f"{OWNER}.fresh_direct_v3_active_process_capture.v1"
ACTIVE_CAPTURE_STATUS: Final = "fresh_active_process_captured"
OPERATIONAL_ATTEMPT_SCHEMA: Final = f"{OWNER}.operational_evidence_attempt.v1"
OPERATIONAL_ATTEMPT_STATUS: Final = "operational_evidence_attempt_frozen_no_authority"
ACTIVATION_ENVELOPE_SCHEMA: Final = "f05_buy_e3_fresh_direct_v3_activation_envelope.v2"
ACTIVATION_ENVELOPE_STATUS: Final = "fresh_disabled_to_active_restart_evidence_complete"
COMPLETION_SCHEMA: Final = f"{OWNER}.operational_evidence_completion.v1"
COMPLETION_STATUS: Final = "operational_evidence_complete_authority_unchanged"
COMPOSITION_SCHEMA: Final = f"{OWNER}.final_composition_receipt.v4"
COMPOSITION_STATUS: Final = "owner_buy_e3_operational_evidence_composed"
ATTEMPT_FINAL_SCHEMA: Final = f"{OWNER}.operational_evidence_attempt_final_receipt.v3"
ATTEMPT_FINAL_STATUS: Final = "operational_attempt_results_bound_authority_unchanged"
EVIDENCE_RELEASE_SCHEMA: Final = f"{OWNER}.evidence_complete_active_release.v2"
EVIDENCE_RELEASE_STATUS: Final = (
    "owner_authorized_live_evidence_complete_runtime_authority_unchanged"
)

RESOURCE_SCHEMA: Final = f"{OWNER}.current_host_concurrent_resource_gate.v3"
RESOURCE_STATUS: Final = "fresh_disabled_same_pid_concurrent_gate_passed"
RESOURCE_CANONICAL_FIELD: Final = "canonical_resource_receipt_sha256"

LIFECYCLE_SCHEMA: Final = "prospective_lifecycle_remote_session_admission.v1"
RUNTIME_IDENTITY_SCHEMA: Final = "narrowgate_live_runtime_identity.v1"
STARTUP_ATTESTATION_SCHEMA: Final = "narrowgate_buy_e3_startup_attestation.v4"
ACTIVE_RUNTIME_AUTHORITY_SCHEMA: Final = "narrowgate_f05_buy_e3_active_release_runtime_authority.v1"

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
_UTC_RE: Final = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")

NO_AUTHORITY: Final = {"research": False, "action": False, "live": False}
EVIDENCE_BOUNDARY: Final = {
    "economic_outcomes_read": False,
    "economic_values_persisted": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "new_economic_arm_run": False,
    "shadow_created": False,
    "companion_created": False,
    "hypothetical_live_actions_scored": False,
}
AUTHORITY_DESIGN: Final = {
    "runtime_authority": "immutable_direct_v3_owner_release",
    "runtime_authority_replaced": False,
    "runtime_consumed": False,
    "does_not_replace_runtime_active_release": True,
    "retrospective_authority_created": False,
    "evidence_is_additive_only": True,
}

FOCUSED_NODEIDS: Final = (
    "tests/test_maker_position_sync.py::test_sync_position_accepts_signed_empty_response_as_flat",
    "tests/test_boolean_cooldown_buy_e3.py::test_short_gap_and_out_of_order_preserve_receive_time_state",
    "tests/test_boolean_cooldown_buy_e3.py::test_sparse_receive_time_windows_match_offline_projector",
)
FOCUSED_SOURCES: Final = (
    "strategy/maker_engine.py",
    "strategy/boolean_cooldown_buy_e3.py",
    "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_features.py",
    "tests/test_maker_position_sync.py",
    "tests/test_boolean_cooldown_buy_e3.py",
)
FOCUSED_COVERAGE: Final = {
    "signed_empty_position_response_is_explicit_flat": True,
    "sparse_100ms_windows_emit_source_gap": True,
    "short_gap_preserves_ema_state": True,
    "gap_over_one_second_resets": True,
    "out_of_order_update_rejected_without_state_mutation": True,
    "sparse_streaming_matches_offline_projector": True,
}


class EvidenceCompletionError(RuntimeError):
    """Raised when additive BUY E3 evidence fails closed."""


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _document_sha256(payload: Mapping[str, Any], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return _canonical_sha256(body)


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise EvidenceCompletionError(f"{label} is not a lowercase SHA256")
    return normalized


def _require_git_sha(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if _GIT_SHA_RE.fullmatch(normalized) is None:
        raise EvidenceCompletionError(f"{label} is not a lowercase git SHA")
    return normalized


def _timestamp(value: Any, label: str) -> str:
    normalized = str(value)
    if _UTC_RE.fullmatch(normalized) is None:
        raise EvidenceCompletionError(f"{label} is not canonical UTC")
    try:
        datetime.fromisoformat(normalized.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise EvidenceCompletionError(f"{label} is invalid") from exc
    return normalized


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_admitted_json(
    path: Path, *, admission_root: Path, label: str
) -> tuple[dict[str, Any], bytes]:
    root = admission_root.resolve(strict=True)
    candidate = path.absolute()
    if not candidate.is_relative_to(root):
        raise EvidenceCompletionError(f"{label} escapes its admitted session")
    before = os.lstat(candidate)
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise EvidenceCompletionError(f"{label} is not a single-link regular file")
    descriptor = os.open(
        candidate,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino, opened.st_size) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
        ):
            raise EvidenceCompletionError(f"{label} changed during open")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1 << 20)
            if not chunk:
                break
            chunks.append(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        path_after = os.lstat(candidate)
        if (
            len(raw) != opened.st_size
            or (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
            or (path_after.st_dev, path_after.st_ino, path_after.st_size, path_after.st_mtime_ns)
            != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
        ):
            raise EvidenceCompletionError(f"{label} changed during read")
    finally:
        os.close(descriptor)

    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise EvidenceCompletionError(f"{label} has a duplicate JSON key")
            result[key] = value
        return result

    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda value: (_ for _ in ()).throw(
                EvidenceCompletionError(f"{label} has non-finite JSON: {value}")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceCompletionError(f"{label} is not JSON") from exc
    if not isinstance(payload, dict):
        raise EvidenceCompletionError(f"{label} is not a JSON object")
    return payload, raw


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ("git", *args),
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _is_ancestor(root: Path, ancestor: str, descendant: str) -> bool:
    completed = subprocess.run(
        ("git", "merge-base", "--is-ancestor", ancestor, descendant),
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.returncode == 0


def _direct_execution() -> dict[str, str]:
    return {
        "execution_commit": DIRECT_COMMIT,
        "execution_tree": DIRECT_TREE,
        "annotated_operational_tag": DIRECT_TAG,
        "annotated_operational_tag_object": DIRECT_TAG_OBJECT,
        "tag_peeled_commit": DIRECT_COMMIT,
    }


def _validate_direct_repository(root: Path) -> Path:
    repository = root.expanduser().resolve(strict=True)
    observed = release_io._operational_git_identity(repository, DIRECT_TAG)  # noqa: SLF001
    if observed != _direct_execution():
        raise EvidenceCompletionError("direct-v3 repository identity drifted")
    if not _is_ancestor(repository, FLAT_POSITION_FIX_COMMIT, DIRECT_COMMIT):
        raise EvidenceCompletionError("flat-position fix is not in direct-v3 ancestry")
    return repository


def _collector_execution(repository_root: Path, annotated_tag: str) -> dict[str, Any]:
    repository = repository_root.expanduser().resolve(strict=True)
    execution = release_io._operational_git_identity(repository, annotated_tag)  # noqa: SLF001
    if not _is_ancestor(repository, DIRECT_COMMIT, execution["execution_commit"]):
        raise EvidenceCompletionError("evidence collector is not a direct-v3 descendant")
    return execution


def _binding(
    path: Path,
    *,
    label: str,
    canonical_field: str,
    expected_schema: str | None = None,
    expected_status: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    opened = release_io._open_document(path, label)  # noqa: SLF001
    payload = dict(opened.payload)
    canonical = _require_sha256(payload.get(canonical_field), f"{label} canonical identity")
    if canonical != _document_sha256(payload, canonical_field):
        raise EvidenceCompletionError(f"{label} canonical identity drifted")
    if expected_schema is not None and payload.get("schema_version") != expected_schema:
        raise EvidenceCompletionError(f"{label} schema drifted")
    if expected_status is not None and payload.get("status") != expected_status:
        raise EvidenceCompletionError(f"{label} status drifted")
    return payload, {
        "path": str(opened.path),
        "file_sha256": hashlib.sha256(opened.raw).hexdigest(),
        "size_bytes": len(opened.raw),
        "mode": "0600",
        "device": opened.metadata.st_dev,
        "inode": opened.metadata.st_ino,
        "schema_version": payload.get("schema_version"),
        "status": payload.get("status"),
        "canonical_field": canonical_field,
        "canonical_sha256": canonical,
    }


def _write(path: Path, payload: Mapping[str, Any]) -> str:
    try:
        return release_io._write_exclusive(path, payload)  # noqa: SLF001
    except Exception as exc:
        raise EvidenceCompletionError(f"receipt creation failed: {path}") from exc


def _finalize(
    output_path: Path,
    payload: dict[str, Any],
    *,
    canonical_field: str,
    validator: Any,
    validator_kwargs: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    payload[canonical_field] = _document_sha256(payload, canonical_field)
    file_sha = _write(output_path, payload)
    observed = validator(output_path, **dict(validator_kwargs))
    if observed != payload:
        raise EvidenceCompletionError("written receipt differs after validation")
    return payload, file_sha


def _direct_authority(
    path: Path, *, direct_repository_root: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    repository = _validate_direct_repository(direct_repository_root)
    try:
        payload = direct_release.validate_direct_owner_release(path, repository_root=repository)
    except Exception as exc:
        raise EvidenceCompletionError("immutable direct-v3 release is invalid") from exc
    if payload["execution"] != _direct_execution():
        raise EvidenceCompletionError("direct-v3 release execution drifted")
    rebound, binding = _binding(
        path,
        label="immutable direct-v3 release",
        canonical_field="canonical_active_release_sha256",
        expected_schema=direct_release.SCHEMA_VERSION,
        expected_status=direct_release.STATUS,
    )
    if rebound != payload:
        raise EvidenceCompletionError("direct-v3 release changed during validation")
    if (
        binding["file_sha256"] != DIRECT_RELEASE_FILE_SHA256
        or binding["canonical_sha256"] != DIRECT_RELEASE_CANONICAL_SHA256
    ):
        raise EvidenceCompletionError("direct-v3 release byte identity drifted")
    binding["runtime_authority"] = True
    return payload, binding


def _artifact_projection(release: Mapping[str, Any]) -> dict[str, Any]:
    artifact = release.get("exact_artifact")
    if not isinstance(artifact, Mapping) or artifact.get("artifact_sha256") != ARTIFACT_SHA256:
        raise EvidenceCompletionError("exact artifact projection drifted")
    roles = artifact.get("roles")
    if not isinstance(roles, Mapping) or set(roles) != {"manifest", "policy", "predicate_bundle"}:
        raise EvidenceCompletionError("exact artifact role projection drifted")
    return {"artifact_sha256": ARTIFACT_SHA256, "roles": dict(roles)}


def _validate_v5_exact(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload, binding = _binding(
        path,
        label="isolated exact V5 verification",
        canonical_field="canonical_receipt_sha256",
        expected_schema=V5_SCHEMA,
        expected_status=V5_STATUS,
    )
    required = {
        "schema_version",
        "status",
        "python",
        "isolated",
        "safe_path",
        "cwd",
        "sys_path",
        "git_commit",
        "git_tree",
        "tracked_worktree_clean",
        "imports",
        "study_sha256",
        "model_bundle_census_sha256",
        "input_binding_sha256",
        "selected_day_count",
        "output_root",
        "economic_outcomes_read",
        "labels_read",
        "candidate_actions_generated",
        "validation_read",
        "sealed_holdout_read",
        "raw_sha256",
        "frame_sha256",
        "envelope_sha256",
        "row_key_sha256",
        "opportunity_count",
        "mechanics_manifest",
        "canonical_receipt_sha256",
    }
    if set(payload) != required:
        raise EvidenceCompletionError("exact V5 verification fields drifted")
    for name in (
        "economic_outcomes_read",
        "labels_read",
        "candidate_actions_generated",
        "validation_read",
        "sealed_holdout_read",
    ):
        if payload.get(name) is not False:
            raise EvidenceCompletionError(f"exact V5 exceeded evidence boundary at {name}")
    study = _require_sha256(payload.get("study_sha256"), "exact V5 study SHA256")
    model = _require_sha256(
        payload.get("model_bundle_census_sha256"), "exact V5 model census SHA256"
    )
    source = _require_sha256(payload.get("input_binding_sha256"), "exact V5 input SHA256")
    if study != V5_STUDY_SHA256:
        raise EvidenceCompletionError("exact V5 study identity is not the isolated 5a293 bundle")
    if model != V5_MODEL_CENSUS_SHA256:
        raise EvidenceCompletionError("exact V5 model bundle is not the frozen 74a0 census")
    if source != V5_INPUT_SHA256:
        raise EvidenceCompletionError("exact V5 input is not the frozen 717d binding")
    if (
        payload.get("isolated") is not True
        or payload.get("safe_path") is not True
        or payload.get("tracked_worktree_clean") is not True
        or payload.get("selected_day_count") != V5_DAY_COUNT
        or payload.get("opportunity_count") != V5_OPPORTUNITY_COUNT
    ):
        raise EvidenceCompletionError("exact V5 isolated reconstruction contract drifted")
    aggregate_maps = {
        "raw_sha256": V5_RAW_SHA256,
        "frame_sha256": V5_FRAME_SHA256,
        "envelope_sha256": V5_ENVELOPE_SHA256,
    }
    for name, expected in aggregate_maps.items():
        value = payload.get(name)
        if not isinstance(value, Mapping) or dict(value) != expected:
            raise EvidenceCompletionError(f"exact V5 aggregate {name} drifted")
    row_key = _require_sha256(payload.get("row_key_sha256"), "exact V5 row-key SHA256")
    if row_key != V5_ROW_KEY_SHA256:
        raise EvidenceCompletionError("exact V5 row-key identity drifted")
    mechanics = payload.get("mechanics_manifest")
    if mechanics != V5_MECHANICS_MANIFEST:
        raise EvidenceCompletionError("exact V5 mechanics manifest path drifted")
    binding["study_sha256"] = study
    binding["model_bundle_census_sha256"] = model
    binding["input_binding_sha256"] = source
    binding["selected_day_count"] = V5_DAY_COUNT
    binding["opportunity_count"] = V5_OPPORTUNITY_COUNT
    binding["raw_sha256"] = dict(V5_RAW_SHA256)
    binding["frame_sha256"] = dict(V5_FRAME_SHA256)
    binding["envelope_sha256"] = dict(V5_ENVELOPE_SHA256)
    binding["row_key_sha256"] = row_key
    binding["mechanics_manifest"] = mechanics
    return payload, binding


def _validate_lifecycle_admission(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    admission_path = path.expanduser().absolute()
    admission_root = admission_path.parent.resolve(strict=True)
    payload, admission_raw = _read_admitted_json(
        admission_path,
        admission_root=admission_root,
        label="ORICO lifecycle admission",
    )
    admission_metadata = os.lstat(admission_path)
    required = {
        "schema_version",
        "admitted_ts_ns",
        "remote",
        "remote_repo_root",
        "remote_allowlisted_root",
        "remote_session_root",
        "remote_epoch_root",
        "remote_seal_path",
        "remote_seal_sha256",
        "remote_seal_identity_sha256",
        "single_rsync_files_from_session",
        "atomic_rename_admission",
        "remote_payload_deleted",
        "economic_outcomes_read",
        "action_authorized",
        "live_policy_authorized",
        "validation",
        "admission_identity_sha256",
    }
    if set(payload) != required or payload.get("schema_version") != LIFECYCLE_SCHEMA:
        raise EvidenceCompletionError("ORICO lifecycle admission fields drifted")
    identity = _require_sha256(
        payload.get("admission_identity_sha256"), "ORICO lifecycle admission identity"
    )
    if identity != _document_sha256(payload, "admission_identity_sha256"):
        raise EvidenceCompletionError("ORICO lifecycle admission identity drifted")
    if (
        payload.get("single_rsync_files_from_session") is not True
        or payload.get("atomic_rename_admission") is not True
        or payload.get("remote_payload_deleted") is not False
        or payload.get("economic_outcomes_read") is not False
        or payload.get("action_authorized") is not False
        or payload.get("live_policy_authorized") is not False
    ):
        raise EvidenceCompletionError("ORICO lifecycle admission exceeded authority")
    validation = payload.get("validation")
    if not isinstance(validation, Mapping):
        raise EvidenceCompletionError("ORICO lifecycle validation is missing")
    event_count = int(validation.get("event_id_count", -1))
    row_count = int(validation.get("row_count", -1))
    part_count = int(validation.get("part_count", -1))
    lifecycle_count = int(validation.get("lifecycle_count", -1))
    cursor_count = int(validation.get("cursor_count", -1))
    if (
        validation.get("epoch_fully_bound") is not True
        or validation.get("stable_double_read_passed") is not True
        or validation.get("storage_format") != "parquet"
        or event_count <= 0
        or event_count != row_count
        or part_count <= 0
        or lifecycle_count <= 0
        or lifecycle_count != cursor_count
        or int(validation.get("health_drop_count", -1)) != 0
        or int(validation.get("health_error_count", -1)) != 0
        or int(validation.get("file_count", -1)) <= 0
        or int(validation.get("payload_bytes", -1)) <= 0
    ):
        raise EvidenceCompletionError("ORICO lifecycle admission validation failed")
    runtime_sha = _require_sha256(
        validation.get("runtime_identity_sha256"), "ORICO lifecycle runtime identity"
    )
    session_id = str(validation.get("session_id", ""))
    baseline_epoch_id = str(validation.get("baseline_epoch_id", ""))
    if not session_id or not baseline_epoch_id:
        raise EvidenceCompletionError("ORICO lifecycle session identity is missing")
    writer_path = (
        admission_root
        / "source"
        / "order_lifecycle_journal_v2"
        / f"session-{session_id}"
        / "runtime_identity.json"
    )
    epoch_root = admission_root / "source" / "prospective_baseline_epochs" / baseline_epoch_id
    epoch_path = epoch_root / "epoch_manifest.json"
    evidence_path = epoch_root / "identity_evidence.json"
    writer, writer_raw = _read_admitted_json(
        writer_path,
        admission_root=admission_root,
        label="admitted lifecycle writer identity",
    )
    epoch, epoch_raw = _read_admitted_json(
        epoch_path,
        admission_root=admission_root,
        label="admitted lifecycle epoch manifest",
    )
    identity_evidence, evidence_raw = _read_admitted_json(
        evidence_path,
        admission_root=admission_root,
        label="admitted lifecycle identity evidence",
    )
    writer_runtime = writer.get("runtime_identity")
    epoch_identity = epoch.get("identity")
    if not isinstance(writer_runtime, Mapping) or not isinstance(epoch_identity, Mapping):
        raise EvidenceCompletionError("admitted lifecycle runtime identity is malformed")
    if (
        writer.get("runtime_identity_sha256") != runtime_sha
        or _canonical_sha256(writer_runtime) != runtime_sha
        or writer_runtime.get("baseline_epoch_id") != baseline_epoch_id
        or writer_runtime.get("baseline_epoch_identity_sha256")
        != validation.get("epoch_identity_sha256")
        or epoch.get("epoch_id") != baseline_epoch_id
        or epoch.get("identity_sha256") != validation.get("epoch_identity_sha256")
        or epoch_identity.get("config_sha256") != writer_runtime.get("config_sha256")
        or epoch_identity.get("runtime_code_sha256") != writer_runtime.get("runtime_code_sha256")
        or epoch_identity.get("action_enablement_sha256")
        != writer_runtime.get("action_enablement_sha256")
    ):
        raise EvidenceCompletionError("admitted lifecycle epoch/runtime identity drifted")
    evidence_binding = epoch.get("identity_evidence")
    if (
        not isinstance(evidence_binding, Mapping)
        or evidence_binding.get("path") != "identity_evidence.json"
        or evidence_binding.get("canonical_sha256") != _canonical_sha256(identity_evidence)
    ):
        raise EvidenceCompletionError("admitted lifecycle identity evidence drifted")
    runtime_code = identity_evidence.get("runtime_code")
    action_enablement = identity_evidence.get("action_enablement")
    config = identity_evidence.get("config")
    if (
        not isinstance(runtime_code, Mapping)
        or runtime_code.get("sha256") != epoch_identity.get("runtime_code_sha256")
        or not isinstance(runtime_code.get("files"), Mapping)
        or _canonical_sha256(
            {
                "schema_version": runtime_code.get("schema_version"),
                "files": runtime_code.get("files"),
            }
        )
        != runtime_code.get("sha256")
        or not isinstance(action_enablement, Mapping)
        or not isinstance(action_enablement.get("fields"), Mapping)
        or _canonical_sha256(action_enablement) != epoch_identity.get("action_enablement_sha256")
        or not isinstance(config, Mapping)
        or config.get("sha256") != epoch_identity.get("config_sha256")
    ):
        raise EvidenceCompletionError("admitted lifecycle source evidence drifted")
    action_fields = action_enablement["fields"]
    required_action_state = {
        "strategy.buy_e3_cooldown_policy_enabled": True,
        "strategy.buy_fill_selection_live_enabled": False,
        "strategy.buy_fill_selection_shadow_enabled": False,
        "strategy.dynamic_fill_hazard_action_enabled": False,
        "strategy.dynamic_fill_hazard_shadow_enabled": False,
        "strategy.state_conditioned_policy_mode": "disabled",
        "logging.exact_opportunity_tape_enabled": False,
        "logging.inventory_campaign_shadow_enabled": False,
    }
    if any(action_fields.get(name) != value for name, value in required_action_state.items()):
        raise EvidenceCompletionError("admitted lifecycle action/shadow state drifted")
    runtime_code_files = {
        str(name): _require_sha256(value, f"admitted lifecycle runtime source {name}")
        for name, value in runtime_code["files"].items()
    }
    return payload, {
        "path": str(admission_path),
        "file_sha256": hashlib.sha256(admission_raw).hexdigest(),
        "size_bytes": len(admission_raw),
        "mode": f"{stat.S_IMODE(admission_metadata.st_mode):04o}",
        "device": admission_metadata.st_dev,
        "inode": admission_metadata.st_ino,
        "schema_version": LIFECYCLE_SCHEMA,
        "canonical_field": "admission_identity_sha256",
        "canonical_sha256": identity,
        "writer_runtime_identity_sha256": runtime_sha,
        "session_id": session_id,
        "baseline_epoch_id": baseline_epoch_id,
        "config_sha256": epoch_identity["config_sha256"],
        "runtime_code_sha256": epoch_identity["runtime_code_sha256"],
        "runtime_code_files": runtime_code_files,
        "action_enablement_sha256": epoch_identity["action_enablement_sha256"],
        "required_action_state": required_action_state,
        "epoch_start_ts_ns": int(epoch.get("start_ts_ns", -1)),
        "writer_identity_file_sha256": hashlib.sha256(writer_raw).hexdigest(),
        "epoch_manifest_file_sha256": hashlib.sha256(epoch_raw).hexdigest(),
        "identity_evidence_file_sha256": hashlib.sha256(evidence_raw).hexdigest(),
        "event_count": event_count,
        "part_count": part_count,
        "lifecycle_count": lifecycle_count,
    }


def _resource_module() -> Any:
    try:
        from research.families.f05_fill_quality_quote_ev.audit import (
            causal_multichannel_window_boolean_cooldown_owner_buy_e3_current_host_resource_gate_v3 as resource,
        )
    except ImportError as exc:
        raise EvidenceCompletionError(
            "current-host resource-gate v3 module is unavailable"
        ) from exc
    return resource


def _validate_resource(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    resource = _resource_module()
    try:
        payload = resource.validate_resource_receipt(path)
    except Exception as exc:
        raise EvidenceCompletionError("current-host resource receipt is invalid") from exc
    rebound, binding = _binding(
        path,
        label="current-host resource receipt",
        canonical_field=RESOURCE_CANONICAL_FIELD,
        expected_schema=RESOURCE_SCHEMA,
        expected_status=RESOURCE_STATUS,
    )
    if rebound != payload:
        raise EvidenceCompletionError("resource receipt changed during validation")
    execution = payload.get("runtime_execution")
    authority = payload.get("authority_design")
    deployed = payload.get("exact_deployed_files")
    if (
        not isinstance(execution, Mapping)
        or execution.get("execution_commit") != DIRECT_COMMIT
        or execution.get("execution_tree") != DIRECT_TREE
        or execution.get("annotated_tag") != DIRECT_TAG
        or execution.get("annotated_tag_object") != DIRECT_TAG_OBJECT
        or not isinstance(authority, Mapping)
        or authority.get("runtime_authority_release_file_sha256") != DIRECT_RELEASE_FILE_SHA256
    ):
        raise EvidenceCompletionError("resource receipt direct-v3 authority drifted")
    if (
        authority.get("runtime_authority_release_canonical_sha256")
        != DIRECT_RELEASE_CANONICAL_SHA256
        or authority.get("direct_v3_release_does_not_depend_on_resource_receipt") is not True
        or not isinstance(deployed, Mapping)
        or deployed.get("artifact_sha256") != ARTIFACT_SHA256
    ):
        raise EvidenceCompletionError("resource receipt authority/deployed binding drifted")
    binding["runtime_authority_replaced"] = False
    binding["runtime_execution_commit"] = DIRECT_COMMIT
    binding["runtime_execution_tree"] = DIRECT_TREE
    binding["collector_execution"] = dict(payload["collector_execution"])
    return payload, binding


def _disabled_process(resource: Mapping[str, Any]) -> dict[str, Any]:
    process = resource.get("fresh_disabled_process")
    if not isinstance(process, Mapping):
        raise EvidenceCompletionError("resource receipt lacks its fresh disabled process")
    if "disabled_pid" in process:
        process = {
            "pid": process.get("disabled_pid"),
            "pid_start_ticks": process.get("disabled_pid_start_ticks"),
            "canonical_process_identity_sha256": process.get("disabled_process_identity_sha256"),
            "config_path": process.get("disabled_config_path"),
            "config_sha256": process.get("disabled_config_sha256"),
            "fresh_pid": process.get("fresh_pid"),
            "fresh_start_ticks": process.get("fresh_start_ticks"),
            "same_pid_pre_post": process.get("same_pid_pre_post"),
        }
    else:
        nested = process.get("process_identity")
        if isinstance(nested, Mapping):
            process = nested
    pid = process.get("pid")
    start = process.get("pid_start_ticks")
    if (
        not isinstance(pid, int)
        or isinstance(pid, bool)
        or pid <= 0
        or not isinstance(start, int)
        or isinstance(start, bool)
        or start <= 0
    ):
        raise EvidenceCompletionError("resource disabled process identity is malformed")
    if any(
        process.get(name) is not True
        for name in ("fresh_pid", "fresh_start_ticks", "same_pid_pre_post")
    ):
        raise EvidenceCompletionError("resource process is not a fresh same-PID disabled window")
    _require_sha256(
        process.get("canonical_process_identity_sha256"),
        "resource disabled process identity",
    )
    return dict(process)


def build_focused_runtime_regression(
    *,
    direct_repository_root: Path,
    direct_release_path: Path,
    python_executable: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    repository = _validate_direct_repository(direct_repository_root)
    release, release_binding = _direct_authority(
        direct_release_path, direct_repository_root=repository
    )
    executable = python_executable.expanduser().resolve(strict=True)
    if not executable.is_file():
        raise EvidenceCompletionError("focused regression Python executable is not a file")
    for relative in FOCUSED_SOURCES:
        if not (repository / relative).is_file():
            raise EvidenceCompletionError(f"focused regression source is missing: {relative}")
    command = [str(executable), "-m", "pytest", "-q", *FOCUSED_NODEIDS]
    with tempfile.TemporaryDirectory(prefix="f05-buy-e3-focused-") as temporary:
        junit = Path(temporary) / "focused.xml"
        run_command = [
            str(executable),
            "-m",
            "pytest",
            "-q",
            f"--junitxml={junit}",
            *FOCUSED_NODEIDS,
        ]
        completed = subprocess.run(
            run_command,
            cwd=repository,
            check=False,
            capture_output=True,
            text=True,
        )
        if not junit.is_file():
            raise EvidenceCompletionError("focused regression did not produce JUnit evidence")
        root = ET.parse(junit).getroot()
        suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
        counts = {
            name: sum(int(suite.attrib.get(name, "0")) for suite in suites)
            for name in ("tests", "failures", "errors", "skipped")
        }
    passed = counts["tests"] - counts["failures"] - counts["errors"] - counts["skipped"]
    if (
        completed.returncode != 0
        or counts["tests"] != len(FOCUSED_NODEIDS)
        or passed != len(FOCUSED_NODEIDS)
        or any(counts[name] != 0 for name in ("failures", "errors", "skipped"))
    ):
        raise EvidenceCompletionError("focused direct-v3 runtime regression failed")
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "focused regression timestamp")
    sources = {relative: _file_sha256(repository / relative) for relative in FOCUSED_SOURCES}
    payload = {
        "schema_version": FOCUSED_SCHEMA,
        "identity": OWNER,
        "status": FOCUSED_STATUS,
        "generated_utc": timestamp,
        "runtime_authority": release_binding,
        "runtime_execution": _direct_execution(),
        "fix_commits": {
            "explicit_flat_position_response": FLAT_POSITION_FIX_COMMIT,
            "sparse_receive_time_windows": SPARSE_WINDOW_FIX_COMMIT,
        },
        "artifact_sha256": _artifact_projection(release)["artifact_sha256"],
        "python_executable": str(executable),
        "python_file_sha256": _file_sha256(executable),
        "command": command,
        "nodeids": list(FOCUSED_NODEIDS),
        "nodeid_manifest_sha256": _canonical_sha256(list(FOCUSED_NODEIDS)),
        "counts": {**counts, "passed": passed, "return_code": completed.returncode},
        "stdout_sha256": hashlib.sha256(completed.stdout.encode()).hexdigest(),
        "stderr_sha256": hashlib.sha256(completed.stderr.encode()).hexdigest(),
        "source_files": sources,
        "source_manifest_sha256": _canonical_sha256(sources),
        "coverage": dict(FOCUSED_COVERAGE),
        "permissions": dict(NO_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload["canonical_receipt_sha256"] = _document_sha256(payload, "canonical_receipt_sha256")
    return payload


def validate_focused_runtime_regression(
    path: Path,
    *,
    direct_repository_root: Path,
    direct_release_path: Path,
) -> dict[str, Any]:
    repository = _validate_direct_repository(direct_repository_root)
    release, release_binding = _direct_authority(
        direct_release_path, direct_repository_root=repository
    )
    payload, _binding_row = _binding(
        path,
        label="focused direct-v3 runtime regression",
        canonical_field="canonical_receipt_sha256",
        expected_schema=FOCUSED_SCHEMA,
        expected_status=FOCUSED_STATUS,
    )
    expected_fields = {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "runtime_authority",
        "runtime_execution",
        "fix_commits",
        "artifact_sha256",
        "python_executable",
        "python_file_sha256",
        "command",
        "nodeids",
        "nodeid_manifest_sha256",
        "counts",
        "stdout_sha256",
        "stderr_sha256",
        "source_files",
        "source_manifest_sha256",
        "coverage",
        "permissions",
        "evidence_boundary",
        "canonical_receipt_sha256",
    }
    executable = Path(str(payload.get("python_executable", ""))).resolve(strict=True)
    sources = {relative: _file_sha256(repository / relative) for relative in FOCUSED_SOURCES}
    counts = payload.get("counts")
    if (
        set(payload) != expected_fields
        or payload.get("identity") != OWNER
        or payload.get("runtime_authority") != release_binding
        or payload.get("runtime_execution") != _direct_execution()
        or payload.get("fix_commits")
        != {
            "explicit_flat_position_response": FLAT_POSITION_FIX_COMMIT,
            "sparse_receive_time_windows": SPARSE_WINDOW_FIX_COMMIT,
        }
        or payload.get("artifact_sha256") != _artifact_projection(release)["artifact_sha256"]
        or payload.get("python_file_sha256") != _file_sha256(executable)
        or payload.get("command") != [str(executable), "-m", "pytest", "-q", *FOCUSED_NODEIDS]
        or payload.get("nodeids") != list(FOCUSED_NODEIDS)
        or payload.get("nodeid_manifest_sha256") != _canonical_sha256(list(FOCUSED_NODEIDS))
        or not isinstance(counts, Mapping)
        or counts
        != {
            "tests": len(FOCUSED_NODEIDS),
            "failures": 0,
            "errors": 0,
            "skipped": 0,
            "passed": len(FOCUSED_NODEIDS),
            "return_code": 0,
        }
        or payload.get("source_files") != sources
        or payload.get("source_manifest_sha256") != _canonical_sha256(sources)
        or payload.get("coverage") != FOCUSED_COVERAGE
        or payload.get("permissions") != NO_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
    ):
        raise EvidenceCompletionError("focused direct-v3 regression identity drifted")
    _timestamp(payload.get("generated_utc"), "focused regression timestamp")
    for name in ("stdout_sha256", "stderr_sha256"):
        _require_sha256(payload.get(name), f"focused regression {name}")
    return payload


def finalize_focused_runtime_regression(
    *,
    direct_repository_root: Path,
    direct_release_path: Path,
    python_executable: Path,
    output_path: Path,
    generated_utc: str | None = None,
) -> tuple[dict[str, Any], str]:
    payload = build_focused_runtime_regression(
        direct_repository_root=direct_repository_root,
        direct_release_path=direct_release_path,
        python_executable=python_executable,
        generated_utc=generated_utc,
    )
    file_sha = _write(output_path, payload)
    observed = validate_focused_runtime_regression(
        output_path,
        direct_repository_root=direct_repository_root,
        direct_release_path=direct_release_path,
    )
    if observed != payload:
        raise EvidenceCompletionError("focused regression changed after write")
    return payload, file_sha


def _active_runtime_semantics(
    runtime: Mapping[str, Any],
    *,
    active_pid: int,
    direct_release_payload: Mapping[str, Any],
    direct_release_binding: Mapping[str, Any],
    expected_config_path: str | Path,
    expected_config_sha256: str,
) -> dict[str, Any]:
    if isinstance(expected_config_path, Path):
        expected_config = str(expected_config_path.expanduser().resolve(strict=True))
    else:
        expected_config = str(expected_config_path)
        if not expected_config.startswith("/"):
            raise EvidenceCompletionError("active runtime config path is not absolute")
    if (
        runtime.get("schema_version") != RUNTIME_IDENTITY_SCHEMA
        or runtime.get("pid") != active_pid
        or runtime.get("config_path") != expected_config
        or runtime.get("config_sha256") != expected_config_sha256
        or runtime.get("f05_buy_e3_enabled") is not True
        or runtime.get("f05_buy_e3_owner_override_effective") is not True
        or runtime.get("f05_buy_e3_artifact_sha256") != ARTIFACT_SHA256
        or runtime.get("f05_buy_e3_artifact_manifest_sha256")
        != direct_release_payload["exact_artifact"]["roles"]["manifest"]["file_sha256"]
        or runtime.get("f05_buy_e3_policy_sha256")
        != direct_release_payload["exact_artifact"]["roles"]["policy"]["file_sha256"]
        or runtime.get("f05_buy_e3_predicate_bundle_sha256")
        != direct_release_payload["exact_artifact"]["roles"]["predicate_bundle"]["file_sha256"]
        or runtime.get("f05_buy_e3_active_release_authority_schema_version")
        != ACTIVE_RUNTIME_AUTHORITY_SCHEMA
        or runtime.get("f05_buy_e3_required") is not True
        or runtime.get("f05_buy_e3_active_release_file_sha256")
        != direct_release_binding["file_sha256"]
        or runtime.get("f05_buy_e3_active_release_canonical_sha256")
        != direct_release_binding["canonical_sha256"]
    ):
        raise EvidenceCompletionError("active runtime authority or artifact identity drifted")
    startup = runtime.get("startup_attestation")
    if not isinstance(startup, Mapping):
        raise EvidenceCompletionError("active runtime startup attestation is missing")
    gates = startup.get("gates")
    checkout = startup.get("running_checkout")
    active_release_identity = startup.get("buy_e3_active_release")
    if (
        startup.get("schema_version") != STARTUP_ATTESTATION_SCHEMA
        or startup.get("status") != "accepted"
        or startup.get("errors") != []
        or not isinstance(gates, Mapping)
        or not gates
        or any(value is not True for value in gates.values())
        or not isinstance(checkout, Mapping)
        or checkout.get("git_commit") != DIRECT_COMMIT
        or checkout.get("git_tree") != DIRECT_TREE
        or checkout.get("git_worktree_clean") is not True
        or not isinstance(active_release_identity, Mapping)
        or active_release_identity.get("file_sha256") != direct_release_binding["file_sha256"]
        or active_release_identity.get("file_canonical_sha256")
        != direct_release_binding["canonical_sha256"]
        or active_release_identity.get("execution_commit") != DIRECT_COMMIT
        or active_release_identity.get("execution_tree") != DIRECT_TREE
        or active_release_identity.get("annotated_operational_tag") != DIRECT_TAG
        or active_release_identity.get("annotated_operational_tag_object") != DIRECT_TAG_OBJECT
    ):
        raise EvidenceCompletionError("active runtime startup attestation failed closed")
    state = startup.get("fill_cooldown_state")
    if not isinstance(state, Mapping):
        raise EvidenceCompletionError("active runtime fill-cooldown state is missing")
    buy_identity = str(state.get("buy_deadline_identity", ""))
    if buy_identity != "B0" and buy_identity != f"BUY_E3:{ARTIFACT_SHA256}":
        raise EvidenceCompletionError("active startup imported an unknown BUY deadline identity")
    if state.get("e3_deadline_imported") is True and buy_identity == "B0":
        raise EvidenceCompletionError("active startup relabeled an E3 deadline as B0")
    return {
        "startup_attestation_sha256": _canonical_sha256(startup),
        "startup_status": "accepted",
        "running_checkout_commit": DIRECT_COMMIT,
        "running_checkout_tree": DIRECT_TREE,
        "buy_deadline_identity": buy_identity,
        "fill_cooldown_restore_mode": state.get("restore_mode"),
        "buy_remaining_ms": state.get("buy_remaining_ms"),
        "e3_deadline_imported": state.get("e3_deadline_imported"),
    }


def _predecessor_is_quiescent(pid: int, *, proc_root: Path = Path("/proc")) -> bool:
    if not proc_root.is_dir():
        raise EvidenceCompletionError("active capture requires Linux /proc")
    return not (proc_root / str(pid)).exists()


def _startup_runtime_sources(runtime: Mapping[str, Any]) -> tuple[str, dict[str, str]]:
    startup = runtime.get("startup_attestation")
    checkout = startup.get("running_checkout") if isinstance(startup, Mapping) else None
    rows = checkout.get("runtime_source_files") if isinstance(checkout, Mapping) else None
    manifest_sha = (
        checkout.get("runtime_source_manifest_sha256") if isinstance(checkout, Mapping) else None
    )
    if not isinstance(rows, list) or not rows:
        raise EvidenceCompletionError("active startup runtime-source rows are missing")
    normalized: dict[str, str] = {}
    for row in rows:
        if (
            not isinstance(row, Mapping)
            or not isinstance(row.get("path"), str)
            or row.get("matches_head_blob") is not True
        ):
            raise EvidenceCompletionError("active startup runtime-source row is malformed")
        relative = str(row["path"])
        if relative in normalized:
            raise EvidenceCompletionError("active startup runtime-source path is duplicated")
        normalized[relative] = _require_sha256(
            row.get("working_file_sha256"), f"active startup runtime source {relative}"
        )
    return _require_sha256(manifest_sha, "active startup runtime-source manifest"), normalized


def build_active_process_capture(
    *,
    direct_repository_root: Path,
    direct_release_path: Path,
    resource_receipt_path: Path,
    pid_file: Path,
    config_path: Path,
    config_sha256: str,
    python_executable: Path,
    venv_root: Path,
    runtime_identity_path: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    repository = _validate_direct_repository(direct_repository_root)
    release, release_binding = _direct_authority(
        direct_release_path, direct_repository_root=repository
    )
    resource, resource_binding = _validate_resource(resource_receipt_path)
    disabled = _disabled_process(resource)
    if not _predecessor_is_quiescent(int(disabled["pid"])):
        raise EvidenceCompletionError("disabled predecessor PID is still running")
    expected_config_sha = _require_sha256(config_sha256, "active config SHA256")
    pid_path = pid_file.expanduser().resolve(strict=True)
    try:
        active_pid = int(pid_path.read_text(encoding="ascii").strip())
    except (OSError, ValueError) as exc:
        raise EvidenceCompletionError("active PID file is invalid") from exc
    if active_pid <= 0 or active_pid == int(disabled["pid"]):
        raise EvidenceCompletionError("activation did not create a fresh PID")
    try:
        process = gate_v2.capture_actual_process_identity(
            pid=active_pid,
            expected_repository_root=repository,
            expected_config_path=config_path,
            expected_config_sha256=expected_config_sha,
            expected_python_executable=python_executable,
            expected_venv_root=venv_root,
            runtime_identity_path=runtime_identity_path,
        )
    except Exception as exc:
        raise EvidenceCompletionError("active process identity capture failed") from exc
    active_start = process.get("pid_start_ticks")
    if (
        not isinstance(active_start, int)
        or isinstance(active_start, bool)
        or active_start <= int(disabled["pid_start_ticks"])
    ):
        raise EvidenceCompletionError("active PID start ticks do not follow the disabled process")
    opened = release_io._open_document(  # noqa: SLF001
        runtime_identity_path, "active runtime identity"
    )
    runtime = dict(opened.payload)
    runtime_file_sha = hashlib.sha256(opened.raw).hexdigest()
    process_runtime = process.get("runtime_identity")
    if (
        not isinstance(process_runtime, Mapping)
        or process_runtime.get("file_sha256") != runtime_file_sha
    ):
        raise EvidenceCompletionError("active process and runtime identity bytes differ")
    semantics = _active_runtime_semantics(
        runtime,
        active_pid=active_pid,
        direct_release_payload=release,
        direct_release_binding=release_binding,
        expected_config_path=config_path,
        expected_config_sha256=expected_config_sha,
    )
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "active process capture timestamp")
    process_row = dict(process)
    process_row["execution_commit"] = DIRECT_COMMIT
    process_row["execution_tree"] = DIRECT_TREE
    process_row["artifact_sha256"] = ARTIFACT_SHA256
    process_row["buy_e3_enabled"] = True
    process_row["owner_override_effective"] = True
    process_row["runtime_identity_file_sha256"] = runtime_file_sha
    process_row["startup_attestation_sha256"] = semantics["startup_attestation_sha256"]
    process_row["canonical_process_identity_sha256"] = _document_sha256(
        process_row, "canonical_process_identity_sha256"
    )
    payload = {
        "schema_version": ACTIVE_CAPTURE_SCHEMA,
        "identity": OWNER,
        "status": ACTIVE_CAPTURE_STATUS,
        "generated_utc": timestamp,
        "runtime_authority": release_binding,
        "resource_receipt": resource_binding,
        "host": dict(resource.get("host", {})),
        "disabled_predecessor": {
            "pid": int(disabled["pid"]),
            "pid_start_ticks": int(disabled["pid_start_ticks"]),
            "process_identity_sha256": disabled.get("canonical_process_identity_sha256"),
            "quiescent_before_active_capture": True,
        },
        "active_process": process_row,
        "runtime_identity": runtime,
        "runtime_identity_file_sha256": runtime_file_sha,
        "startup_semantics": semantics,
        "checks": {
            "fresh_pid": True,
            "fresh_start_ticks": True,
            "disabled_predecessor_quiescent": True,
            "direct_v3_checkout_exact": True,
            "direct_v3_release_exact": True,
            "startup_attestation_accepted": True,
            "artifact_exact": True,
            "restart_only_activation": True,
            "retroactive_signature": False,
        },
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(NO_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload["canonical_active_capture_sha256"] = _document_sha256(
        payload, "canonical_active_capture_sha256"
    )
    return payload


def validate_active_process_capture(
    path: Path,
    *,
    direct_repository_root: Path,
    direct_release_path: Path,
    resource_receipt_path: Path,
) -> dict[str, Any]:
    repository = _validate_direct_repository(direct_repository_root)
    release, release_binding = _direct_authority(
        direct_release_path, direct_repository_root=repository
    )
    resource, resource_binding = _validate_resource(resource_receipt_path)
    disabled = _disabled_process(resource)
    payload, _capture_binding = _binding(
        path,
        label="fresh active process capture",
        canonical_field="canonical_active_capture_sha256",
        expected_schema=ACTIVE_CAPTURE_SCHEMA,
        expected_status=ACTIVE_CAPTURE_STATUS,
    )
    fields = {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "runtime_authority",
        "resource_receipt",
        "host",
        "disabled_predecessor",
        "active_process",
        "runtime_identity",
        "runtime_identity_file_sha256",
        "startup_semantics",
        "checks",
        "authority_design",
        "permissions",
        "evidence_boundary",
        "canonical_active_capture_sha256",
    }
    process = payload.get("active_process")
    predecessor = payload.get("disabled_predecessor")
    runtime = payload.get("runtime_identity")
    if not isinstance(process, Mapping) or not isinstance(predecessor, Mapping):
        raise EvidenceCompletionError("fresh active process transition is malformed")
    if not isinstance(runtime, Mapping):
        raise EvidenceCompletionError("fresh active runtime identity is malformed")
    runtime_file_sha = _require_sha256(
        payload.get("runtime_identity_file_sha256"), "active runtime identity file SHA256"
    )
    if process.get("runtime_identity_file_sha256") != runtime_file_sha:
        raise EvidenceCompletionError("active process lost its runtime identity binding")
    expected_config = str(runtime.get("config_path", ""))
    semantics = _active_runtime_semantics(
        runtime,
        active_pid=int(process.get("pid", -1)),
        direct_release_payload=release,
        direct_release_binding=release_binding,
        expected_config_path=expected_config,
        expected_config_sha256=_require_sha256(
            runtime.get("config_sha256"), "active runtime config SHA256"
        ),
    )
    process_body = dict(process)
    process_canonical = _require_sha256(
        process_body.pop("canonical_process_identity_sha256", None),
        "active process canonical identity",
    )
    if process_canonical != _canonical_sha256(process_body):
        raise EvidenceCompletionError("active process canonical identity drifted")
    if (
        set(payload) != fields
        or payload.get("identity") != OWNER
        or payload.get("runtime_authority") != release_binding
        or payload.get("resource_receipt") != resource_binding
        or payload.get("host") != resource.get("host", {})
        or predecessor
        != {
            "pid": int(disabled["pid"]),
            "pid_start_ticks": int(disabled["pid_start_ticks"]),
            "process_identity_sha256": disabled.get("canonical_process_identity_sha256"),
            "quiescent_before_active_capture": True,
        }
        or int(process.get("pid", -1)) == int(disabled["pid"])
        or int(process.get("pid_start_ticks", -1)) <= int(disabled["pid_start_ticks"])
        or process.get("execution_commit") != DIRECT_COMMIT
        or process.get("execution_tree") != DIRECT_TREE
        or process.get("artifact_sha256") != ARTIFACT_SHA256
        or process.get("buy_e3_enabled") is not True
        or process.get("owner_override_effective") is not True
        or process.get("startup_attestation_sha256") != semantics["startup_attestation_sha256"]
        or payload.get("startup_semantics") != semantics
        or payload.get("checks")
        != {
            "fresh_pid": True,
            "fresh_start_ticks": True,
            "disabled_predecessor_quiescent": True,
            "direct_v3_checkout_exact": True,
            "direct_v3_release_exact": True,
            "startup_attestation_accepted": True,
            "artifact_exact": True,
            "restart_only_activation": True,
            "retroactive_signature": False,
        }
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or payload.get("permissions") != NO_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
    ):
        raise EvidenceCompletionError("fresh active process capture identity drifted")
    _timestamp(payload.get("generated_utc"), "active process capture timestamp")
    return payload


def finalize_active_process_capture(
    *,
    direct_repository_root: Path,
    direct_release_path: Path,
    resource_receipt_path: Path,
    pid_file: Path,
    config_path: Path,
    config_sha256: str,
    python_executable: Path,
    venv_root: Path,
    runtime_identity_path: Path,
    output_path: Path,
    generated_utc: str | None = None,
) -> tuple[dict[str, Any], str]:
    payload = build_active_process_capture(
        direct_repository_root=direct_repository_root,
        direct_release_path=direct_release_path,
        resource_receipt_path=resource_receipt_path,
        pid_file=pid_file,
        config_path=config_path,
        config_sha256=config_sha256,
        python_executable=python_executable,
        venv_root=venv_root,
        runtime_identity_path=runtime_identity_path,
        generated_utc=generated_utc,
    )
    file_sha = _write(output_path, payload)
    observed = validate_active_process_capture(
        output_path,
        direct_repository_root=direct_repository_root,
        direct_release_path=direct_release_path,
        resource_receipt_path=resource_receipt_path,
    )
    if observed != payload:
        raise EvidenceCompletionError("active process capture changed after write")
    return payload, file_sha


def _validate_attempt4_manifest(
    path: Path, *, repository_root: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    repository = repository_root.expanduser().resolve(strict=True)
    try:
        payload = legacy_attempt.validate_manifest(
            path,
            repository_root=repository,
            require_current_checkout=True,
        )
    except Exception as exc:
        raise EvidenceCompletionError("historical Attempt4 manifest is invalid") from exc
    runtime = payload.get("runtime_execution")
    evidence = payload.get("pre_admission_evidence")
    if (
        not isinstance(runtime, Mapping)
        or runtime.get("execution_commit") != ATTEMPT4_COMMIT
        or runtime.get("annotated_tag") != ATTEMPT4_TAG
        or not isinstance(evidence, Mapping)
        or set(evidence) != set(legacy_attempt.PRE_ADMISSION_RECEIPT_ROLES)
        or "parity_layer2" not in evidence
        or "parity_layer4" not in evidence
    ):
        raise EvidenceCompletionError("historical Attempt4 mechanics anchor drifted")
    rebound, binding = _binding(
        path,
        label="historical Attempt4 manifest",
        canonical_field="canonical_execution_attempt_sha256",
        expected_schema=legacy_attempt.SCHEMA_VERSION,
        expected_status="compatible_runtime_frozen_not_activated",
    )
    if rebound != payload:
        raise EvidenceCompletionError("historical Attempt4 manifest changed during validation")
    wrappers: dict[str, str] = {}
    for role in legacy_attempt.PRE_ADMISSION_RECEIPT_ROLES:
        row = evidence.get(role)
        if not isinstance(row, Mapping):
            raise EvidenceCompletionError(f"historical Attempt4 wrapper is missing: {role}")
        wrappers[role] = _require_sha256(
            row.get("canonical_sha256"), f"historical Attempt4 {role} wrapper"
        )
    binding.update(
        {
            "historical_only": True,
            "resource_or_activation_claimed": False,
            "execution_commit": ATTEMPT4_COMMIT,
            "execution_tag": ATTEMPT4_TAG,
            "wrapper_canonical_sha256": wrappers,
            "layer2_canonical_sha256": wrappers["parity_layer2"],
            "layer4_canonical_sha256": wrappers["parity_layer4"],
        }
    )
    return payload, binding


def _validate_full_regression(
    path: Path,
    *,
    direct_repository_root: Path,
    direct_release_payload: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    artifact = _artifact_projection(direct_release_payload)
    try:
        payload = gate_v1.validate_runtime_regression_receipt(
            path,
            repository_root=direct_repository_root,
            expected_artifact_sha256=ARTIFACT_SHA256,
            expected_execution_commit=DIRECT_COMMIT,
            expected_execution_tag=DIRECT_TAG,
        )
    except Exception as exc:
        raise EvidenceCompletionError(
            "current direct-v3 full runtime regression is invalid"
        ) from exc
    rebound, binding = _binding(
        path,
        label="current direct-v3 full runtime regression",
        canonical_field="canonical_receipt_sha256",
        expected_schema=gate_v1.COMPATIBLE_REGRESSION_SCHEMA,
        expected_status="passed",
    )
    if rebound != payload or payload.get("artifact_sha256") != artifact["artifact_sha256"]:
        raise EvidenceCompletionError("current full runtime regression changed during validation")
    return payload, binding


def _validate_sell54(
    path: Path,
    *,
    direct_repository_root: Path,
    direct_release_payload: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    artifact = _artifact_projection(direct_release_payload)
    expected_files = {
        role: artifact["roles"][role]["file_sha256"]
        for role in ("manifest", "policy", "predicate_bundle")
    }
    try:
        payload = gate_v1.validate_sell_owner_54_case_receipt(
            path,
            repository_root=direct_repository_root,
            expected_artifact_sha256=ARTIFACT_SHA256,
            expected_artifact_files=expected_files,
        )
    except Exception as exc:
        raise EvidenceCompletionError("current SELL 54-case parity receipt is invalid") from exc
    rebound, binding = _binding(
        path,
        label="current SELL 54-case parity",
        canonical_field="canonical_receipt_sha256",
        expected_schema=gate_v1.SELL_PARITY_SCHEMA,
        expected_status="parity_complete",
    )
    if rebound != payload:
        raise EvidenceCompletionError("SELL 54-case parity changed during validation")
    return payload, binding


def _focused_binding(
    path: Path,
    *,
    direct_repository_root: Path,
    direct_release_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = validate_focused_runtime_regression(
        path,
        direct_repository_root=direct_repository_root,
        direct_release_path=direct_release_path,
    )
    rebound, binding = _binding(
        path,
        label="focused direct-v3 runtime regression",
        canonical_field="canonical_receipt_sha256",
        expected_schema=FOCUSED_SCHEMA,
        expected_status=FOCUSED_STATUS,
    )
    if rebound != payload:
        raise EvidenceCompletionError("focused runtime receipt changed during validation")
    return payload, binding


def build_operational_attempt(
    *,
    collector_repository_root: Path,
    collector_annotated_tag: str,
    direct_repository_root: Path,
    direct_release_path: Path,
    attempt4_repository_root: Path,
    attempt4_manifest_path: Path,
    v5_exact_verify_path: Path,
    full_runtime_regression_path: Path,
    focused_runtime_regression_path: Path,
    sell54_path: Path,
    attempt_id: str,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    if re.fullmatch(r"operational-attempt-[a-z0-9][a-z0-9._-]*", attempt_id) is None:
        raise EvidenceCompletionError("operational attempt id is malformed")
    direct_repository = _validate_direct_repository(direct_repository_root)
    collector_execution = _collector_execution(collector_repository_root, collector_annotated_tag)
    release, release_binding = _direct_authority(
        direct_release_path, direct_repository_root=direct_repository
    )
    _attempt4, attempt4_binding = _validate_attempt4_manifest(
        attempt4_manifest_path, repository_root=attempt4_repository_root
    )
    _v5, v5_binding = _validate_v5_exact(v5_exact_verify_path)
    _full, full_binding = _validate_full_regression(
        full_runtime_regression_path,
        direct_repository_root=direct_repository,
        direct_release_payload=release,
    )
    _focused, focused_binding = _focused_binding(
        focused_runtime_regression_path,
        direct_repository_root=direct_repository,
        direct_release_path=direct_release_path,
    )
    _sell, sell_binding = _validate_sell54(
        sell54_path,
        direct_repository_root=direct_repository,
        direct_release_payload=release,
    )
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "operational attempt timestamp")
    payload = {
        "schema_version": OPERATIONAL_ATTEMPT_SCHEMA,
        "identity": OWNER,
        "attempt_id": attempt_id,
        "status": OPERATIONAL_ATTEMPT_STATUS,
        "generated_utc": timestamp,
        "collector_execution": collector_execution,
        "runtime_execution": _direct_execution(),
        "runtime_authority": release_binding,
        "exact_artifact": _artifact_projection(release),
        "historical_attempt4_anchor": attempt4_binding,
        "exact_v5_recovery": v5_binding,
        "current_runtime_evidence": {
            "full_regression": full_binding,
            "focused_successor_regression": focused_binding,
            "sell54_parity": sell_binding,
        },
        "historical_truth": {
            "attempt4_mechanics_and_stability_only": True,
            "attempt4_resource_gate_claimed": False,
            "attempt4_activation_claimed": False,
            "attempt4_final_receipt_claimed": False,
            "direct_v3_is_operational_attempt": True,
        },
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(NO_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload["canonical_operational_attempt_sha256"] = _document_sha256(
        payload, "canonical_operational_attempt_sha256"
    )
    return payload


def validate_operational_attempt(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> dict[str, Any]:
    payload, _row = _binding(
        path,
        label="operational evidence attempt",
        canonical_field="canonical_operational_attempt_sha256",
        expected_schema=OPERATIONAL_ATTEMPT_SCHEMA,
        expected_status=OPERATIONAL_ATTEMPT_STATUS,
    )
    fields = {
        "schema_version",
        "identity",
        "attempt_id",
        "status",
        "generated_utc",
        "collector_execution",
        "runtime_execution",
        "runtime_authority",
        "exact_artifact",
        "historical_attempt4_anchor",
        "exact_v5_recovery",
        "current_runtime_evidence",
        "historical_truth",
        "authority_design",
        "permissions",
        "evidence_boundary",
        "canonical_operational_attempt_sha256",
    }
    collector = payload.get("collector_execution")
    if not isinstance(collector, Mapping):
        raise EvidenceCompletionError("operational attempt collector identity is missing")
    expected_collector = _collector_execution(
        collector_repository_root,
        str(collector.get("annotated_operational_tag", "")),
    )
    release_path = Path(str(payload.get("runtime_authority", {}).get("path", "")))
    release, release_binding = _direct_authority(
        release_path, direct_repository_root=direct_repository_root
    )
    attempt4_path = Path(str(payload.get("historical_attempt4_anchor", {}).get("path", "")))
    _attempt4, attempt4_binding = _validate_attempt4_manifest(
        attempt4_path, repository_root=attempt4_repository_root
    )
    v5_path = Path(str(payload.get("exact_v5_recovery", {}).get("path", "")))
    _v5, v5_binding = _validate_v5_exact(v5_path)
    current = payload.get("current_runtime_evidence")
    if not isinstance(current, Mapping):
        raise EvidenceCompletionError("operational attempt runtime evidence is missing")
    full_path = Path(str(current.get("full_regression", {}).get("path", "")))
    focused_path = Path(str(current.get("focused_successor_regression", {}).get("path", "")))
    sell_path = Path(str(current.get("sell54_parity", {}).get("path", "")))
    _full, full_binding = _validate_full_regression(
        full_path,
        direct_repository_root=direct_repository_root,
        direct_release_payload=release,
    )
    _focused, focused_binding = _focused_binding(
        focused_path,
        direct_repository_root=direct_repository_root,
        direct_release_path=release_path,
    )
    _sell, sell_binding = _validate_sell54(
        sell_path,
        direct_repository_root=direct_repository_root,
        direct_release_payload=release,
    )
    if (
        set(payload) != fields
        or payload.get("identity") != OWNER
        or re.fullmatch(
            r"operational-attempt-[a-z0-9][a-z0-9._-]*", str(payload.get("attempt_id", ""))
        )
        is None
        or payload.get("collector_execution") != expected_collector
        or payload.get("runtime_execution") != _direct_execution()
        or payload.get("runtime_authority") != release_binding
        or payload.get("exact_artifact") != _artifact_projection(release)
        or payload.get("historical_attempt4_anchor") != attempt4_binding
        or payload.get("exact_v5_recovery") != v5_binding
        or current
        != {
            "full_regression": full_binding,
            "focused_successor_regression": focused_binding,
            "sell54_parity": sell_binding,
        }
        or payload.get("historical_truth")
        != {
            "attempt4_mechanics_and_stability_only": True,
            "attempt4_resource_gate_claimed": False,
            "attempt4_activation_claimed": False,
            "attempt4_final_receipt_claimed": False,
            "direct_v3_is_operational_attempt": True,
        }
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or payload.get("permissions") != NO_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
    ):
        raise EvidenceCompletionError("operational evidence attempt identity drifted")
    _timestamp(payload.get("generated_utc"), "operational attempt timestamp")
    return payload


def finalize_operational_attempt(
    *,
    output_path: Path,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
    payload = build_operational_attempt(**kwargs)
    file_sha = _write(output_path, payload)
    observed = validate_operational_attempt(
        output_path,
        collector_repository_root=kwargs["collector_repository_root"],
        direct_repository_root=kwargs["direct_repository_root"],
        attempt4_repository_root=kwargs["attempt4_repository_root"],
    )
    if observed != payload:
        raise EvidenceCompletionError("operational attempt changed after write")
    return payload, file_sha


def _receipt_binding(
    path: Path,
    *,
    label: str,
    canonical_field: str,
    schema: str,
    status: str,
) -> dict[str, Any]:
    _payload, binding = _binding(
        path,
        label=label,
        canonical_field=canonical_field,
        expected_schema=schema,
        expected_status=status,
    )
    return binding


def build_activation_envelope(
    *,
    operational_attempt_path: Path,
    active_capture_path: Path,
    resource_receipt_path: Path,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    attempt = validate_operational_attempt(
        operational_attempt_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    release_path = Path(str(attempt["runtime_authority"]["path"]))
    release, release_binding = _direct_authority(
        release_path, direct_repository_root=direct_repository_root
    )
    resource, resource_binding = _validate_resource(resource_receipt_path)
    active = validate_active_process_capture(
        active_capture_path,
        direct_repository_root=direct_repository_root,
        direct_release_path=release_path,
        resource_receipt_path=resource_receipt_path,
    )
    active_binding = _receipt_binding(
        active_capture_path,
        label="fresh active process capture",
        canonical_field="canonical_active_capture_sha256",
        schema=ACTIVE_CAPTURE_SCHEMA,
        status=ACTIVE_CAPTURE_STATUS,
    )
    attempt_binding = _receipt_binding(
        operational_attempt_path,
        label="operational evidence attempt",
        canonical_field="canonical_operational_attempt_sha256",
        schema=OPERATIONAL_ATTEMPT_SCHEMA,
        status=OPERATIONAL_ATTEMPT_STATUS,
    )
    disabled = _disabled_process(resource)
    predecessor = active["disabled_predecessor"]
    active_process = active["active_process"]
    active_runtime_identity = active.get("runtime_identity")
    if not isinstance(active_runtime_identity, Mapping):
        raise EvidenceCompletionError("active capture lacks runtime identity")
    runtime_source_manifest, runtime_source_files = _startup_runtime_sources(
        active_runtime_identity
    )
    if (
        predecessor.get("pid") != disabled["pid"]
        or predecessor.get("pid_start_ticks") != disabled["pid_start_ticks"]
        or active_process.get("pid") == disabled["pid"]
        or active_process.get("pid_start_ticks") <= disabled["pid_start_ticks"]
        or active["runtime_authority"] != release_binding
        or attempt["runtime_authority"] != release_binding
    ):
        raise EvidenceCompletionError("disabled-to-active transition cross-binding failed")
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "activation envelope timestamp")
    payload = {
        "schema_version": ACTIVATION_ENVELOPE_SCHEMA,
        "identity": OWNER,
        "status": ACTIVATION_ENVELOPE_STATUS,
        "generated_utc": timestamp,
        "operational_attempt": attempt_binding,
        "runtime_authority": release_binding,
        "resource_receipt": resource_binding,
        "active_process_capture": active_binding,
        "transition": {
            "disabled_pid": int(disabled["pid"]),
            "disabled_pid_start_ticks": int(disabled["pid_start_ticks"]),
            "active_pid": int(active_process["pid"]),
            "active_pid_start_ticks": int(active_process["pid_start_ticks"]),
            "disabled_same_pid_resource_gate": True,
            "disabled_predecessor_quiescent": True,
            "fresh_active_restart": True,
            "activation_via_sighup": False,
            "runtime_checkout_changed": False,
        },
        "active_runtime": {
            "execution": _direct_execution(),
            "artifact_sha256": ARTIFACT_SHA256,
            "runtime_identity_file_sha256": active["runtime_identity_file_sha256"],
            "startup_attestation_sha256": active["startup_semantics"]["startup_attestation_sha256"],
            "startup_status": "accepted",
            "config_sha256": active_runtime_identity["config_sha256"],
            "runtime_source_manifest_sha256": runtime_source_manifest,
            "runtime_source_files": runtime_source_files,
        },
        "exact_artifact": _artifact_projection(release),
        "checks": {
            "resource_gate_preceded_activation": True,
            "fresh_disabled_process": True,
            "same_disabled_pid_during_resource_window": True,
            "fresh_active_pid_after_restart": True,
            "direct_v3_runtime_authority_unchanged": True,
            "startup_attestation_accepted": True,
            "retroactive_activation_claim": False,
        },
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(NO_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload["canonical_activation_envelope_sha256"] = _document_sha256(
        payload, "canonical_activation_envelope_sha256"
    )
    return payload


def validate_activation_envelope(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> dict[str, Any]:
    payload, _row = _binding(
        path,
        label="fresh direct-v3 activation envelope",
        canonical_field="canonical_activation_envelope_sha256",
        expected_schema=ACTIVATION_ENVELOPE_SCHEMA,
        expected_status=ACTIVATION_ENVELOPE_STATUS,
    )
    fields = {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "operational_attempt",
        "runtime_authority",
        "resource_receipt",
        "active_process_capture",
        "transition",
        "active_runtime",
        "exact_artifact",
        "checks",
        "authority_design",
        "permissions",
        "evidence_boundary",
        "canonical_activation_envelope_sha256",
    }
    attempt_path = Path(str(payload.get("operational_attempt", {}).get("path", "")))
    attempt = validate_operational_attempt(
        attempt_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    attempt_binding = _receipt_binding(
        attempt_path,
        label="operational evidence attempt",
        canonical_field="canonical_operational_attempt_sha256",
        schema=OPERATIONAL_ATTEMPT_SCHEMA,
        status=OPERATIONAL_ATTEMPT_STATUS,
    )
    release_path = Path(str(attempt["runtime_authority"]["path"]))
    release, release_binding = _direct_authority(
        release_path, direct_repository_root=direct_repository_root
    )
    resource_path = Path(str(payload.get("resource_receipt", {}).get("path", "")))
    resource, resource_binding = _validate_resource(resource_path)
    active_path = Path(str(payload.get("active_process_capture", {}).get("path", "")))
    active = validate_active_process_capture(
        active_path,
        direct_repository_root=direct_repository_root,
        direct_release_path=release_path,
        resource_receipt_path=resource_path,
    )
    active_binding = _receipt_binding(
        active_path,
        label="fresh active process capture",
        canonical_field="canonical_active_capture_sha256",
        schema=ACTIVE_CAPTURE_SCHEMA,
        status=ACTIVE_CAPTURE_STATUS,
    )
    disabled = _disabled_process(resource)
    active_process = active["active_process"]
    active_runtime_identity = active.get("runtime_identity")
    if not isinstance(active_runtime_identity, Mapping):
        raise EvidenceCompletionError("active capture lacks runtime identity")
    runtime_source_manifest, runtime_source_files = _startup_runtime_sources(
        active_runtime_identity
    )
    expected_transition = {
        "disabled_pid": int(disabled["pid"]),
        "disabled_pid_start_ticks": int(disabled["pid_start_ticks"]),
        "active_pid": int(active_process["pid"]),
        "active_pid_start_ticks": int(active_process["pid_start_ticks"]),
        "disabled_same_pid_resource_gate": True,
        "disabled_predecessor_quiescent": True,
        "fresh_active_restart": True,
        "activation_via_sighup": False,
        "runtime_checkout_changed": False,
    }
    expected_active_runtime = {
        "execution": _direct_execution(),
        "artifact_sha256": ARTIFACT_SHA256,
        "runtime_identity_file_sha256": active["runtime_identity_file_sha256"],
        "startup_attestation_sha256": active["startup_semantics"]["startup_attestation_sha256"],
        "startup_status": "accepted",
        "config_sha256": active_runtime_identity["config_sha256"],
        "runtime_source_manifest_sha256": runtime_source_manifest,
        "runtime_source_files": runtime_source_files,
    }
    if (
        set(payload) != fields
        or payload.get("identity") != OWNER
        or payload.get("operational_attempt") != attempt_binding
        or payload.get("runtime_authority") != release_binding
        or payload.get("resource_receipt") != resource_binding
        or payload.get("active_process_capture") != active_binding
        or payload.get("transition") != expected_transition
        or payload.get("active_runtime") != expected_active_runtime
        or payload.get("exact_artifact") != _artifact_projection(release)
        or payload.get("checks")
        != {
            "resource_gate_preceded_activation": True,
            "fresh_disabled_process": True,
            "same_disabled_pid_during_resource_window": True,
            "fresh_active_pid_after_restart": True,
            "direct_v3_runtime_authority_unchanged": True,
            "startup_attestation_accepted": True,
            "retroactive_activation_claim": False,
        }
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or payload.get("permissions") != NO_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
    ):
        raise EvidenceCompletionError("activation envelope identity drifted")
    _timestamp(payload.get("generated_utc"), "activation envelope timestamp")
    return payload


def finalize_activation_envelope(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_activation_envelope(**kwargs)
    file_sha = _write(output_path, payload)
    observed = validate_activation_envelope(
        output_path,
        collector_repository_root=kwargs["collector_repository_root"],
        direct_repository_root=kwargs["direct_repository_root"],
        attempt4_repository_root=kwargs["attempt4_repository_root"],
    )
    if observed != payload:
        raise EvidenceCompletionError("activation envelope changed after write")
    return payload, file_sha


def build_operational_completion(
    *,
    operational_attempt_path: Path,
    activation_envelope_path: Path,
    lifecycle_admission_path: Path,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    attempt = validate_operational_attempt(
        operational_attempt_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    envelope = validate_activation_envelope(
        activation_envelope_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    if (
        envelope["operational_attempt"]["canonical_sha256"]
        != attempt["canonical_operational_attempt_sha256"]
    ):
        raise EvidenceCompletionError("activation envelope belongs to another attempt")
    lifecycle, lifecycle_binding = _validate_lifecycle_admission(lifecycle_admission_path)
    _require_lifecycle_active_match(envelope, lifecycle_binding)
    attempt_binding = _receipt_binding(
        operational_attempt_path,
        label="operational evidence attempt",
        canonical_field="canonical_operational_attempt_sha256",
        schema=OPERATIONAL_ATTEMPT_SCHEMA,
        status=OPERATIONAL_ATTEMPT_STATUS,
    )
    envelope_binding = _receipt_binding(
        activation_envelope_path,
        label="fresh direct-v3 activation envelope",
        canonical_field="canonical_activation_envelope_sha256",
        schema=ACTIVATION_ENVELOPE_SCHEMA,
        status=ACTIVATION_ENVELOPE_STATUS,
    )
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "operational completion timestamp")
    payload = {
        "schema_version": COMPLETION_SCHEMA,
        "identity": OWNER,
        "attempt_id": attempt["attempt_id"],
        "status": COMPLETION_STATUS,
        "generated_utc": timestamp,
        "operational_attempt": attempt_binding,
        "activation_envelope": envelope_binding,
        "runtime_authority": dict(attempt["runtime_authority"]),
        "exact_artifact": dict(attempt["exact_artifact"]),
        "historical_attempt4_anchor": dict(attempt["historical_attempt4_anchor"]),
        "exact_v5_recovery": dict(attempt["exact_v5_recovery"]),
        "current_runtime_evidence": dict(attempt["current_runtime_evidence"]),
        "current_host_resource": dict(envelope["resource_receipt"]),
        "fresh_activation": dict(envelope_binding),
        "lifecycle_orico_admission": lifecycle_binding,
        "gate_results": {
            "attempt4_wrappers_and_manifest": "passed_historical_mechanics_anchor",
            "attempt4_resource_or_activation": "not_claimed",
            "exact_v5_mechanics_recovery": "passed",
            "current_full_runtime_regression": "passed",
            "direct_v3_focused_successor_regression": "passed",
            "sparse_streaming_offline_parity": "passed",
            "sell_54_case_unchanged": "passed",
            "fresh_disabled_same_pid_resource_gate": "passed",
            "fresh_disabled_to_active_restart": "passed",
            "active_startup_attestation": "passed",
            "lifecycle_orico_admission": "passed",
        },
        "formal_research_state": {
            "research_supported": False,
            "formal_hierarchy_passed": False,
            "formal_hard_gates_passed": False,
            "owner_risk_accepted": True,
            "old_oof_applies_to_learning_algorithm_only": True,
            "exact_artifact_oof_available": False,
        },
        "direct_release_immutable_incomplete_record_preserved": True,
        "post_release_evidence_completed": True,
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(NO_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
        "lifecycle_admission_observed_payload_sha256": _canonical_sha256(lifecycle),
    }
    payload["canonical_operational_completion_sha256"] = _document_sha256(
        payload, "canonical_operational_completion_sha256"
    )
    return payload


def _require_lifecycle_active_match(
    envelope: Mapping[str, Any], lifecycle_binding: Mapping[str, Any]
) -> None:
    active = envelope.get("active_runtime")
    if not isinstance(active, Mapping):
        raise EvidenceCompletionError("activation envelope lacks active runtime identity")
    active_files = active.get("runtime_source_files")
    admitted_files = lifecycle_binding.get("runtime_code_files")
    if not isinstance(active_files, Mapping) or not isinstance(admitted_files, Mapping):
        raise EvidenceCompletionError("lifecycle/runtime source cross-binding is missing")
    mandatory = {"strategy/maker_engine.py", "strategy/boolean_cooldown_buy_e3.py"}
    if (
        active.get("config_sha256") != lifecycle_binding.get("config_sha256")
        or not mandatory.issubset(active_files)
        or any(admitted_files.get(path) != digest for path, digest in active_files.items())
    ):
        raise EvidenceCompletionError("lifecycle admission belongs to another active runtime")


def validate_operational_completion(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> dict[str, Any]:
    payload, _row = _binding(
        path,
        label="operational evidence completion",
        canonical_field="canonical_operational_completion_sha256",
        expected_schema=COMPLETION_SCHEMA,
        expected_status=COMPLETION_STATUS,
    )
    fields = {
        "schema_version",
        "identity",
        "attempt_id",
        "status",
        "generated_utc",
        "operational_attempt",
        "activation_envelope",
        "runtime_authority",
        "exact_artifact",
        "historical_attempt4_anchor",
        "exact_v5_recovery",
        "current_runtime_evidence",
        "current_host_resource",
        "fresh_activation",
        "lifecycle_orico_admission",
        "gate_results",
        "formal_research_state",
        "direct_release_immutable_incomplete_record_preserved",
        "post_release_evidence_completed",
        "authority_design",
        "permissions",
        "evidence_boundary",
        "lifecycle_admission_observed_payload_sha256",
        "canonical_operational_completion_sha256",
    }
    attempt_path = Path(str(payload.get("operational_attempt", {}).get("path", "")))
    envelope_path = Path(str(payload.get("activation_envelope", {}).get("path", "")))
    lifecycle_path = Path(str(payload.get("lifecycle_orico_admission", {}).get("path", "")))
    attempt = validate_operational_attempt(
        attempt_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    envelope = validate_activation_envelope(
        envelope_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    lifecycle, lifecycle_binding = _validate_lifecycle_admission(lifecycle_path)
    attempt_binding = _receipt_binding(
        attempt_path,
        label="operational evidence attempt",
        canonical_field="canonical_operational_attempt_sha256",
        schema=OPERATIONAL_ATTEMPT_SCHEMA,
        status=OPERATIONAL_ATTEMPT_STATUS,
    )
    envelope_binding = _receipt_binding(
        envelope_path,
        label="fresh direct-v3 activation envelope",
        canonical_field="canonical_activation_envelope_sha256",
        schema=ACTIVATION_ENVELOPE_SCHEMA,
        status=ACTIVATION_ENVELOPE_STATUS,
    )
    expected_gates = {
        "attempt4_wrappers_and_manifest": "passed_historical_mechanics_anchor",
        "attempt4_resource_or_activation": "not_claimed",
        "exact_v5_mechanics_recovery": "passed",
        "current_full_runtime_regression": "passed",
        "direct_v3_focused_successor_regression": "passed",
        "sparse_streaming_offline_parity": "passed",
        "sell_54_case_unchanged": "passed",
        "fresh_disabled_same_pid_resource_gate": "passed",
        "fresh_disabled_to_active_restart": "passed",
        "active_startup_attestation": "passed",
        "lifecycle_orico_admission": "passed",
    }
    expected_research = {
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "old_oof_applies_to_learning_algorithm_only": True,
        "exact_artifact_oof_available": False,
    }
    _require_lifecycle_active_match(envelope, lifecycle_binding)
    if (
        set(payload) != fields
        or payload.get("identity") != OWNER
        or payload.get("attempt_id") != attempt["attempt_id"]
        or payload.get("operational_attempt") != attempt_binding
        or payload.get("activation_envelope") != envelope_binding
        or payload.get("runtime_authority") != attempt["runtime_authority"]
        or payload.get("exact_artifact") != attempt["exact_artifact"]
        or payload.get("historical_attempt4_anchor") != attempt["historical_attempt4_anchor"]
        or payload.get("exact_v5_recovery") != attempt["exact_v5_recovery"]
        or payload.get("current_runtime_evidence") != attempt["current_runtime_evidence"]
        or payload.get("current_host_resource") != envelope["resource_receipt"]
        or payload.get("fresh_activation") != envelope_binding
        or payload.get("lifecycle_orico_admission") != lifecycle_binding
        or payload.get("gate_results") != expected_gates
        or payload.get("formal_research_state") != expected_research
        or payload.get("direct_release_immutable_incomplete_record_preserved") is not True
        or payload.get("post_release_evidence_completed") is not True
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or payload.get("permissions") != NO_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get("lifecycle_admission_observed_payload_sha256")
        != _canonical_sha256(lifecycle)
    ):
        raise EvidenceCompletionError("operational completion identity drifted")
    _timestamp(payload.get("generated_utc"), "operational completion timestamp")
    return payload


def finalize_operational_completion(
    *, output_path: Path, **kwargs: Any
) -> tuple[dict[str, Any], str]:
    payload = build_operational_completion(**kwargs)
    file_sha = _write(output_path, payload)
    observed = validate_operational_completion(
        output_path,
        collector_repository_root=kwargs["collector_repository_root"],
        direct_repository_root=kwargs["direct_repository_root"],
        attempt4_repository_root=kwargs["attempt4_repository_root"],
    )
    if observed != payload:
        raise EvidenceCompletionError("operational completion changed after write")
    return payload, file_sha


def _chain_context(
    *,
    operational_attempt_path: Path,
    activation_envelope_path: Path,
    operational_completion_path: Path,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    attempt = validate_operational_attempt(
        operational_attempt_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    envelope = validate_activation_envelope(
        activation_envelope_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    completion = validate_operational_completion(
        operational_completion_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    bindings = {
        "operational_attempt": _receipt_binding(
            operational_attempt_path,
            label="operational evidence attempt",
            canonical_field="canonical_operational_attempt_sha256",
            schema=OPERATIONAL_ATTEMPT_SCHEMA,
            status=OPERATIONAL_ATTEMPT_STATUS,
        ),
        "activation_envelope": _receipt_binding(
            activation_envelope_path,
            label="fresh direct-v3 activation envelope",
            canonical_field="canonical_activation_envelope_sha256",
            schema=ACTIVATION_ENVELOPE_SCHEMA,
            status=ACTIVATION_ENVELOPE_STATUS,
        ),
        "operational_completion": _receipt_binding(
            operational_completion_path,
            label="operational evidence completion",
            canonical_field="canonical_operational_completion_sha256",
            schema=COMPLETION_SCHEMA,
            status=COMPLETION_STATUS,
        ),
    }
    if (
        completion["attempt_id"] != attempt["attempt_id"]
        or completion["activation_envelope"]["canonical_sha256"]
        != envelope["canonical_activation_envelope_sha256"]
    ):
        raise EvidenceCompletionError("operational evidence chain is not one attempt")
    return attempt, envelope, completion, bindings


def build_final_composition(
    *,
    operational_attempt_path: Path,
    activation_envelope_path: Path,
    operational_completion_path: Path,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    attempt, _envelope, completion, bindings = _chain_context(
        operational_attempt_path=operational_attempt_path,
        activation_envelope_path=activation_envelope_path,
        operational_completion_path=operational_completion_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "final composition timestamp")
    evidence = {
        "operational_attempt": bindings["operational_attempt"],
        "historical_attempt4_anchor": completion["historical_attempt4_anchor"],
        "exact_v5_recovery": completion["exact_v5_recovery"],
        "current_runtime_evidence": completion["current_runtime_evidence"],
        "current_host_resource": completion["current_host_resource"],
        "fresh_activation": bindings["activation_envelope"],
        "lifecycle_orico_admission": completion["lifecycle_orico_admission"],
        "operational_completion": bindings["operational_completion"],
    }
    payload = {
        "schema_version": COMPOSITION_SCHEMA,
        "identity": OWNER,
        "attempt_id": attempt["attempt_id"],
        "status": COMPOSITION_STATUS,
        "generated_utc": timestamp,
        "collector_execution": dict(attempt["collector_execution"]),
        "runtime_execution": _direct_execution(),
        "runtime_authority": dict(attempt["runtime_authority"]),
        "exact_artifact": dict(attempt["exact_artifact"]),
        "ordered_evidence_roles": list(evidence),
        "evidence": evidence,
        "composition_root_sha256": _canonical_sha256(evidence),
        "composition_truth": {
            "historical_attempt4_resource_or_activation_invented": False,
            "direct_release_rewritten": False,
            "direct_release_renamed": False,
            "post_release_evidence_additive": True,
            "single_operational_attempt": True,
        },
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(NO_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload["canonical_final_composition_sha256"] = _document_sha256(
        payload, "canonical_final_composition_sha256"
    )
    return payload


def validate_final_composition(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> dict[str, Any]:
    payload, _row = _binding(
        path,
        label="final operational evidence composition",
        canonical_field="canonical_final_composition_sha256",
        expected_schema=COMPOSITION_SCHEMA,
        expected_status=COMPOSITION_STATUS,
    )
    evidence = payload.get("evidence")
    if not isinstance(evidence, Mapping):
        raise EvidenceCompletionError("final composition evidence is missing")
    attempt_path = Path(str(evidence.get("operational_attempt", {}).get("path", "")))
    envelope_path = Path(str(evidence.get("fresh_activation", {}).get("path", "")))
    completion_path = Path(str(evidence.get("operational_completion", {}).get("path", "")))
    attempt, _envelope, completion, bindings = _chain_context(
        operational_attempt_path=attempt_path,
        activation_envelope_path=envelope_path,
        operational_completion_path=completion_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    expected_evidence = {
        "operational_attempt": bindings["operational_attempt"],
        "historical_attempt4_anchor": completion["historical_attempt4_anchor"],
        "exact_v5_recovery": completion["exact_v5_recovery"],
        "current_runtime_evidence": completion["current_runtime_evidence"],
        "current_host_resource": completion["current_host_resource"],
        "fresh_activation": bindings["activation_envelope"],
        "lifecycle_orico_admission": completion["lifecycle_orico_admission"],
        "operational_completion": bindings["operational_completion"],
    }
    fields = {
        "schema_version",
        "identity",
        "attempt_id",
        "status",
        "generated_utc",
        "collector_execution",
        "runtime_execution",
        "runtime_authority",
        "exact_artifact",
        "ordered_evidence_roles",
        "evidence",
        "composition_root_sha256",
        "composition_truth",
        "authority_design",
        "permissions",
        "evidence_boundary",
        "canonical_final_composition_sha256",
    }
    if (
        set(payload) != fields
        or payload.get("identity") != OWNER
        or payload.get("attempt_id") != attempt["attempt_id"]
        or payload.get("collector_execution") != attempt["collector_execution"]
        or payload.get("runtime_execution") != _direct_execution()
        or payload.get("runtime_authority") != attempt["runtime_authority"]
        or payload.get("exact_artifact") != attempt["exact_artifact"]
        or payload.get("ordered_evidence_roles") != list(expected_evidence)
        or evidence != expected_evidence
        or payload.get("composition_root_sha256") != _canonical_sha256(expected_evidence)
        or payload.get("composition_truth")
        != {
            "historical_attempt4_resource_or_activation_invented": False,
            "direct_release_rewritten": False,
            "direct_release_renamed": False,
            "post_release_evidence_additive": True,
            "single_operational_attempt": True,
        }
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or payload.get("permissions") != NO_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
    ):
        raise EvidenceCompletionError("final composition identity drifted")
    _timestamp(payload.get("generated_utc"), "final composition timestamp")
    return payload


def finalize_final_composition(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_final_composition(**kwargs)
    file_sha = _write(output_path, payload)
    observed = validate_final_composition(
        output_path,
        collector_repository_root=kwargs["collector_repository_root"],
        direct_repository_root=kwargs["direct_repository_root"],
        attempt4_repository_root=kwargs["attempt4_repository_root"],
    )
    if observed != payload:
        raise EvidenceCompletionError("final composition changed after write")
    return payload, file_sha


def build_attempt_final(
    *,
    final_composition_path: Path,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    composition = validate_final_composition(
        final_composition_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    composition_binding = _receipt_binding(
        final_composition_path,
        label="final operational evidence composition",
        canonical_field="canonical_final_composition_sha256",
        schema=COMPOSITION_SCHEMA,
        status=COMPOSITION_STATUS,
    )
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "attempt-final timestamp")
    payload = {
        "schema_version": ATTEMPT_FINAL_SCHEMA,
        "identity": OWNER,
        "attempt_id": composition["attempt_id"],
        "status": ATTEMPT_FINAL_STATUS,
        "generated_utc": timestamp,
        "runtime_execution": _direct_execution(),
        "runtime_authority": dict(composition["runtime_authority"]),
        "exact_artifact": dict(composition["exact_artifact"]),
        "final_composition": composition_binding,
        "composition_root_sha256": composition["composition_root_sha256"],
        "result": {
            "operational_evidence_complete": True,
            "runtime_authority_unchanged": True,
            "research_supported": False,
            "owner_risk_accepted": True,
            "new_authority_granted": False,
        },
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(NO_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload["canonical_attempt_final_sha256"] = _document_sha256(
        payload, "canonical_attempt_final_sha256"
    )
    return payload


def validate_attempt_final(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> dict[str, Any]:
    payload, _row = _binding(
        path,
        label="operational attempt-final receipt",
        canonical_field="canonical_attempt_final_sha256",
        expected_schema=ATTEMPT_FINAL_SCHEMA,
        expected_status=ATTEMPT_FINAL_STATUS,
    )
    composition_path = Path(str(payload.get("final_composition", {}).get("path", "")))
    composition = validate_final_composition(
        composition_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    composition_binding = _receipt_binding(
        composition_path,
        label="final operational evidence composition",
        canonical_field="canonical_final_composition_sha256",
        schema=COMPOSITION_SCHEMA,
        status=COMPOSITION_STATUS,
    )
    fields = {
        "schema_version",
        "identity",
        "attempt_id",
        "status",
        "generated_utc",
        "runtime_execution",
        "runtime_authority",
        "exact_artifact",
        "final_composition",
        "composition_root_sha256",
        "result",
        "authority_design",
        "permissions",
        "evidence_boundary",
        "canonical_attempt_final_sha256",
    }
    if (
        set(payload) != fields
        or payload.get("identity") != OWNER
        or payload.get("attempt_id") != composition["attempt_id"]
        or payload.get("runtime_execution") != _direct_execution()
        or payload.get("runtime_authority") != composition["runtime_authority"]
        or payload.get("exact_artifact") != composition["exact_artifact"]
        or payload.get("final_composition") != composition_binding
        or payload.get("composition_root_sha256") != composition["composition_root_sha256"]
        or payload.get("result")
        != {
            "operational_evidence_complete": True,
            "runtime_authority_unchanged": True,
            "research_supported": False,
            "owner_risk_accepted": True,
            "new_authority_granted": False,
        }
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or payload.get("permissions") != NO_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
    ):
        raise EvidenceCompletionError("operational attempt-final identity drifted")
    _timestamp(payload.get("generated_utc"), "attempt-final timestamp")
    return payload


def finalize_attempt_final(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_attempt_final(**kwargs)
    file_sha = _write(output_path, payload)
    observed = validate_attempt_final(
        output_path,
        collector_repository_root=kwargs["collector_repository_root"],
        direct_repository_root=kwargs["direct_repository_root"],
        attempt4_repository_root=kwargs["attempt4_repository_root"],
    )
    if observed != payload:
        raise EvidenceCompletionError("attempt-final receipt changed after write")
    return payload, file_sha


def build_evidence_complete_release(
    *,
    attempt_final_path: Path,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    attempt_final = validate_attempt_final(
        attempt_final_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    attempt_final_binding = _receipt_binding(
        attempt_final_path,
        label="operational attempt-final receipt",
        canonical_field="canonical_attempt_final_sha256",
        schema=ATTEMPT_FINAL_SCHEMA,
        status=ATTEMPT_FINAL_STATUS,
    )
    direct_path = Path(str(attempt_final["runtime_authority"]["path"]))
    direct, direct_binding = _direct_authority(
        direct_path, direct_repository_root=direct_repository_root
    )
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "evidence-complete release timestamp")
    payload = {
        "schema_version": EVIDENCE_RELEASE_SCHEMA,
        "identity": OWNER,
        "attempt_id": attempt_final["attempt_id"],
        "status": EVIDENCE_RELEASE_STATUS,
        "generated_utc": timestamp,
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "outcome_informed_owner_override": True,
        "action_authorized": True,
        "live_authorized": True,
        "authority_provenance": {
            "source": "immutable_direct_v3_owner_release",
            "new_authority_granted": False,
            "direct_release_canonical_sha256": direct_binding["canonical_sha256"],
            "direct_release_incomplete_record_preserved": True,
        },
        "runtime_execution": _direct_execution(),
        "runtime_authority": direct_binding,
        "exact_artifact": _artifact_projection(direct),
        "scope": dict(direct["scope"]),
        "rollback": dict(direct["rollback"]),
        "operational_attempt_final": attempt_final_binding,
        "composition_root_sha256": attempt_final["composition_root_sha256"],
        "evidence_state": {
            "post_release_evidence_complete": True,
            "direct_release_was_pre_evidence_owner_override": True,
            "exact_artifact_oof_available": False,
            "old_oof_applies_to_learning_algorithm_only": True,
            "runtime_authority_replaced": False,
            "runtime_consumed": False,
            "does_not_replace_runtime_active_release": True,
        },
        "authority_design": dict(AUTHORITY_DESIGN),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload["canonical_evidence_release_sha256"] = _document_sha256(
        payload, "canonical_evidence_release_sha256"
    )
    return payload


def validate_evidence_complete_release(
    path: Path,
    *,
    collector_repository_root: Path,
    direct_repository_root: Path,
    attempt4_repository_root: Path,
) -> dict[str, Any]:
    payload, _row = _binding(
        path,
        label="evidence-complete proof-layer release",
        canonical_field="canonical_evidence_release_sha256",
        expected_schema=EVIDENCE_RELEASE_SCHEMA,
        expected_status=EVIDENCE_RELEASE_STATUS,
    )
    attempt_final_path = Path(str(payload.get("operational_attempt_final", {}).get("path", "")))
    attempt_final = validate_attempt_final(
        attempt_final_path,
        collector_repository_root=collector_repository_root,
        direct_repository_root=direct_repository_root,
        attempt4_repository_root=attempt4_repository_root,
    )
    attempt_final_binding = _receipt_binding(
        attempt_final_path,
        label="operational attempt-final receipt",
        canonical_field="canonical_attempt_final_sha256",
        schema=ATTEMPT_FINAL_SCHEMA,
        status=ATTEMPT_FINAL_STATUS,
    )
    direct_path = Path(str(payload.get("runtime_authority", {}).get("path", "")))
    direct, direct_binding = _direct_authority(
        direct_path, direct_repository_root=direct_repository_root
    )
    fields = {
        "schema_version",
        "identity",
        "attempt_id",
        "status",
        "generated_utc",
        "research_supported",
        "formal_hierarchy_passed",
        "formal_hard_gates_passed",
        "owner_risk_accepted",
        "outcome_informed_owner_override",
        "action_authorized",
        "live_authorized",
        "authority_provenance",
        "runtime_execution",
        "runtime_authority",
        "exact_artifact",
        "scope",
        "rollback",
        "operational_attempt_final",
        "composition_root_sha256",
        "evidence_state",
        "authority_design",
        "evidence_boundary",
        "canonical_evidence_release_sha256",
    }
    if (
        set(payload) != fields
        or payload.get("identity") != OWNER
        or payload.get("attempt_id") != attempt_final["attempt_id"]
        or payload.get("research_supported") is not False
        or payload.get("formal_hierarchy_passed") is not False
        or payload.get("formal_hard_gates_passed") is not False
        or payload.get("owner_risk_accepted") is not True
        or payload.get("outcome_informed_owner_override") is not True
        or payload.get("action_authorized") is not True
        or payload.get("live_authorized") is not True
        or payload.get("authority_provenance")
        != {
            "source": "immutable_direct_v3_owner_release",
            "new_authority_granted": False,
            "direct_release_canonical_sha256": direct_binding["canonical_sha256"],
            "direct_release_incomplete_record_preserved": True,
        }
        or payload.get("runtime_execution") != _direct_execution()
        or payload.get("runtime_authority") != direct_binding
        or payload.get("exact_artifact") != _artifact_projection(direct)
        or payload.get("scope") != direct["scope"]
        or payload.get("rollback") != direct["rollback"]
        or payload.get("operational_attempt_final") != attempt_final_binding
        or payload.get("composition_root_sha256") != attempt_final["composition_root_sha256"]
        or payload.get("evidence_state")
        != {
            "post_release_evidence_complete": True,
            "direct_release_was_pre_evidence_owner_override": True,
            "exact_artifact_oof_available": False,
            "old_oof_applies_to_learning_algorithm_only": True,
            "runtime_authority_replaced": False,
            "runtime_consumed": False,
            "does_not_replace_runtime_active_release": True,
        }
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
    ):
        raise EvidenceCompletionError("evidence-complete proof-layer release drifted")
    _timestamp(payload.get("generated_utc"), "evidence-complete release timestamp")
    return payload


def finalize_evidence_complete_release(
    *, output_path: Path, **kwargs: Any
) -> tuple[dict[str, Any], str]:
    payload = build_evidence_complete_release(**kwargs)
    file_sha = _write(output_path, payload)
    observed = validate_evidence_complete_release(
        output_path,
        collector_repository_root=kwargs["collector_repository_root"],
        direct_repository_root=kwargs["direct_repository_root"],
        attempt4_repository_root=kwargs["attempt4_repository_root"],
    )
    if observed != payload:
        raise EvidenceCompletionError("evidence-complete release changed after write")
    return payload, file_sha


def _add_chain_roots(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--collector-repository-root", type=Path, required=True)
    parser.add_argument("--direct-repository-root", type=Path, required=True)
    parser.add_argument("--attempt4-repository-root", type=Path, required=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    focused = subparsers.add_parser("focused-regression")
    focused.add_argument("--direct-repository-root", type=Path, required=True)
    focused.add_argument("--direct-release", type=Path, required=True)
    focused.add_argument("--python", type=Path, required=True)
    focused.add_argument("--output", type=Path, required=True)

    active = subparsers.add_parser("active-capture")
    active.add_argument("--direct-repository-root", type=Path, required=True)
    active.add_argument("--direct-release", type=Path, required=True)
    active.add_argument("--resource-receipt", type=Path, required=True)
    active.add_argument("--pid-file", type=Path, required=True)
    active.add_argument("--config", type=Path, required=True)
    active.add_argument("--config-sha256", required=True)
    active.add_argument("--python", type=Path, required=True)
    active.add_argument("--venv-root", type=Path, required=True)
    active.add_argument("--runtime-identity", type=Path, required=True)
    active.add_argument("--output", type=Path, required=True)

    attempt = subparsers.add_parser("attempt")
    _add_chain_roots(attempt)
    attempt.add_argument("--collector-annotated-tag", required=True)
    attempt.add_argument("--direct-release", type=Path, required=True)
    attempt.add_argument("--attempt4-manifest", type=Path, required=True)
    attempt.add_argument("--v5-exact-verify", type=Path, required=True)
    attempt.add_argument("--full-runtime-regression", type=Path, required=True)
    attempt.add_argument("--focused-runtime-regression", type=Path, required=True)
    attempt.add_argument("--sell54", type=Path, required=True)
    attempt.add_argument("--attempt-id", required=True)
    attempt.add_argument("--output", type=Path, required=True)

    envelope = subparsers.add_parser("activation-envelope")
    _add_chain_roots(envelope)
    envelope.add_argument("--operational-attempt", type=Path, required=True)
    envelope.add_argument("--active-capture", type=Path, required=True)
    envelope.add_argument("--resource-receipt", type=Path, required=True)
    envelope.add_argument("--output", type=Path, required=True)

    completion = subparsers.add_parser("completion")
    _add_chain_roots(completion)
    completion.add_argument("--operational-attempt", type=Path, required=True)
    completion.add_argument("--activation-envelope", type=Path, required=True)
    completion.add_argument("--lifecycle-admission", type=Path, required=True)
    completion.add_argument("--output", type=Path, required=True)

    composition = subparsers.add_parser("composition")
    _add_chain_roots(composition)
    composition.add_argument("--operational-attempt", type=Path, required=True)
    composition.add_argument("--activation-envelope", type=Path, required=True)
    composition.add_argument("--operational-completion", type=Path, required=True)
    composition.add_argument("--output", type=Path, required=True)

    attempt_final = subparsers.add_parser("attempt-final")
    _add_chain_roots(attempt_final)
    attempt_final.add_argument("--final-composition", type=Path, required=True)
    attempt_final.add_argument("--output", type=Path, required=True)

    evidence_release = subparsers.add_parser("evidence-release")
    _add_chain_roots(evidence_release)
    evidence_release.add_argument("--attempt-final", type=Path, required=True)
    evidence_release.add_argument("--output", type=Path, required=True)

    validate = subparsers.add_parser("validate")
    _add_chain_roots(validate)
    validate.add_argument(
        "--kind",
        choices=(
            "attempt",
            "activation-envelope",
            "completion",
            "composition",
            "attempt-final",
            "evidence-release",
        ),
        required=True,
    )
    validate.add_argument("--receipt", type=Path, required=True)
    return parser


def _print_result(payload: Mapping[str, Any], file_sha: str | None = None) -> None:
    canonical = next(
        (
            value
            for name, value in payload.items()
            if name.startswith("canonical_") and name.endswith("sha256")
        ),
        None,
    )
    result = {
        "status": payload["status"],
        "schema_version": payload["schema_version"],
        "canonical_sha256": canonical,
    }
    if file_sha is not None:
        result["file_sha256"] = file_sha
    print(json.dumps(result, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    command = args.command
    if command == "focused-regression":
        payload, file_sha = finalize_focused_runtime_regression(
            direct_repository_root=args.direct_repository_root,
            direct_release_path=args.direct_release,
            python_executable=args.python,
            output_path=args.output,
        )
    elif command == "active-capture":
        payload, file_sha = finalize_active_process_capture(
            direct_repository_root=args.direct_repository_root,
            direct_release_path=args.direct_release,
            resource_receipt_path=args.resource_receipt,
            pid_file=args.pid_file,
            config_path=args.config,
            config_sha256=args.config_sha256,
            python_executable=args.python,
            venv_root=args.venv_root,
            runtime_identity_path=args.runtime_identity,
            output_path=args.output,
        )
    elif command == "attempt":
        payload, file_sha = finalize_operational_attempt(
            collector_repository_root=args.collector_repository_root,
            collector_annotated_tag=args.collector_annotated_tag,
            direct_repository_root=args.direct_repository_root,
            direct_release_path=args.direct_release,
            attempt4_repository_root=args.attempt4_repository_root,
            attempt4_manifest_path=args.attempt4_manifest,
            v5_exact_verify_path=args.v5_exact_verify,
            full_runtime_regression_path=args.full_runtime_regression,
            focused_runtime_regression_path=args.focused_runtime_regression,
            sell54_path=args.sell54,
            attempt_id=args.attempt_id,
            output_path=args.output,
        )
    elif command == "activation-envelope":
        payload, file_sha = finalize_activation_envelope(
            operational_attempt_path=args.operational_attempt,
            active_capture_path=args.active_capture,
            resource_receipt_path=args.resource_receipt,
            collector_repository_root=args.collector_repository_root,
            direct_repository_root=args.direct_repository_root,
            attempt4_repository_root=args.attempt4_repository_root,
            output_path=args.output,
        )
    elif command == "completion":
        payload, file_sha = finalize_operational_completion(
            operational_attempt_path=args.operational_attempt,
            activation_envelope_path=args.activation_envelope,
            lifecycle_admission_path=args.lifecycle_admission,
            collector_repository_root=args.collector_repository_root,
            direct_repository_root=args.direct_repository_root,
            attempt4_repository_root=args.attempt4_repository_root,
            output_path=args.output,
        )
    elif command == "composition":
        payload, file_sha = finalize_final_composition(
            operational_attempt_path=args.operational_attempt,
            activation_envelope_path=args.activation_envelope,
            operational_completion_path=args.operational_completion,
            collector_repository_root=args.collector_repository_root,
            direct_repository_root=args.direct_repository_root,
            attempt4_repository_root=args.attempt4_repository_root,
            output_path=args.output,
        )
    elif command == "attempt-final":
        payload, file_sha = finalize_attempt_final(
            final_composition_path=args.final_composition,
            collector_repository_root=args.collector_repository_root,
            direct_repository_root=args.direct_repository_root,
            attempt4_repository_root=args.attempt4_repository_root,
            output_path=args.output,
        )
    elif command == "evidence-release":
        payload, file_sha = finalize_evidence_complete_release(
            attempt_final_path=args.attempt_final,
            collector_repository_root=args.collector_repository_root,
            direct_repository_root=args.direct_repository_root,
            attempt4_repository_root=args.attempt4_repository_root,
            output_path=args.output,
        )
    else:
        validators = {
            "attempt": validate_operational_attempt,
            "activation-envelope": validate_activation_envelope,
            "completion": validate_operational_completion,
            "composition": validate_final_composition,
            "attempt-final": validate_attempt_final,
            "evidence-release": validate_evidence_complete_release,
        }
        payload = validators[args.kind](
            args.receipt,
            collector_repository_root=args.collector_repository_root,
            direct_repository_root=args.direct_repository_root,
            attempt4_repository_root=args.attempt4_repository_root,
        )
        file_sha = None
    _print_result(payload, file_sha)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
