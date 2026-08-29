from __future__ import annotations

import copy
import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from scripts import deploy_f05_buy_e3_owner_v1 as deploy
from scripts import f05_buy_e3_operational_safety_successor_release as builder
from strategy import boolean_cooldown_buy_e3 as runtime

SHA = "a" * 64
GIT = "b" * 40


def _payload() -> dict:
    roles = {
        role: {
            "role": role,
            "path": f"/private/{role}.json",
            "file_sha256": SHA,
            "device": 1,
            "inode": 1,
            "schema_version": "fixture.v1",
            "identity": "fixture",
            "status": "frozen",
            "canonical_field": "artifact_sha256" if role == "manifest" else None,
            "canonical_sha256": (
                runtime.DIRECT_OWNER_EXACT_ARTIFACT_SHA256 if role == "manifest" else None
            ),
            "size_bytes": 1,
            "mode": "0600",
        }
        for role in ("manifest", "policy", "predicate_bundle")
    }
    config_binding = {
        "file_sha256": SHA,
        "semantic_sha256": SHA,
        "size_bytes": 1,
        "mode": "0600",
    }
    required = runtime._LIVE_SAFETY_SUCCESSOR_RUNTIME_SOURCE_CONTRACT[  # noqa: SLF001
        "required_repository_paths"
    ]
    interpreter = {
        "implementation": "cpython",
        "version": "3.12.13",
        "version_info": [3, 12, 13, "final", 0],
        "cache_tag": "cpython-312",
        "soabi": "cpython-312-x86_64-linux-gnu",
        "abiflags": "",
        "sysconfig_platform": "linux-x86_64",
        "system": "Linux",
        "machine": "x86_64",
        "compiler": "GCC 12.2.0",
        "openssl_runtime": "OpenSSL 3.0.0",
        "openssl_version_number": 805306368,
        "executable_sha256": SHA,
        "executable_size_bytes": 1,
        "base_executable_sha256": SHA,
        "base_executable_size_bytes": 1,
        "is_virtual_environment": True,
    }
    payload = {
        "schema_version": runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_SCHEMA,
        "identity": runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_IDENTITY,
        "status": runtime.DIRECT_OWNER_LIVE_SAFETY_SUCCESSOR_STATUS,
        "generated_utc": "2026-08-25T00:00:00Z",
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "outcome_informed_owner_override": True,
        "action_authorized": True,
        "live_authorized": True,
        "authorization_basis": dict(builder.AUTHORIZATION_BASIS),
        "scope": copy.deepcopy(builder.SCOPE),
        "execution": {
            "execution_commit": GIT,
            "execution_tree": GIT,
            "annotated_operational_tag": "safety-successor-v1",
            "annotated_operational_tag_object": GIT,
            "tag_peeled_commit": GIT,
        },
        "predecessor_runtime_authority": {
            "release": dict(builder.PREDECESSOR_RELEASE),
            "execution": dict(builder.PREDECESSOR_EXECUTION),
        },
        "exact_artifact": {
            "artifact_sha256": runtime.DIRECT_OWNER_EXACT_ARTIFACT_SHA256,
            "roles": roles,
        },
        "candidate_semantics": copy.deepcopy(builder.CANDIDATE_SEMANTICS),
        "protected_semantics": {
            "e3_decision_ast_sha256": SHA,
            "sell_owner_policy_file_sha256": SHA,
            "e1_e2_definition_file_sha256": SHA,
            "b0_fallback_file_sha256": SHA,
            "f03_sources_unchanged_after_e080": True,
            "frozen_mechanics_evidence_bytes_referenced_not_modified": True,
            "current_head_not_mechanics_authority": True,
        },
        "config_pair": {
            "schema_version": "f05_buy_e3_live_safety_successor_config_pair.v1",
            "status": "explicit_timeout_pause_exposure_no_shadow_pair",
            "predecessor": {
                "disabled_file_sha256": "d92fdec7ce89586f56fb1a6c80a6bc6fbe96b50023bd8c481cae730606c75204",
                "active_file_sha256": "3d8463c47c1cc2ff2017c9f6e7a963c77a8edb0cc692c48d89b03ee09bff772e",
            },
            "disabled": dict(config_binding),
            "active": dict(config_binding),
            "predecessor_to_successor_semantic_changes": [
                "api.timeout_s",
                "strategy.spread_cap_mode",
            ],
            "explicit_safety_values": {
                "api.timeout_s": 5.0,
                "strategy.spread_cap_mode": "pause_exposure",
            },
            "active_disabled_only_difference": "strategy.buy_e3_cooldown_policy_enabled",
            "required_false_paths": list(runtime._DIRECT_OWNER_V3_CONFIG_FALSE_PATHS),  # noqa: SLF001
            "external_shadow_only_marker_inert": True,
            "release_fields_present_in_yaml": False,
        },
        "operational_safety_contract": copy.deepcopy(builder.OPERATIONAL_SAFETY_CONTRACT),
        "runtime_source_contract": copy.deepcopy(builder.RUNTIME_SOURCE_CONTRACT),
        "changed_repository_files": {
            path: {"git_blob_sha1": GIT, "file_sha256": SHA} for path in required
        },
        "native_abi_contract": copy.deepcopy(builder.NATIVE_ABI_CONTRACT),
        "native_build": {
            "schema_version": "narrowgate_linux_x86_64_native_build_receipt.v2",
            "status": "exact_tag_native_build_dependency_lock_and_parity_passed",
            "file_sha256": SHA,
            "canonical_sha256": SHA,
            "module_sha256": SHA,
            "wheel_sha256": SHA,
            "soabi": "cpython-312-x86_64-linux-gnu",
            "python_minor": "3.12",
            "platform": "linux_x86_64",
            "runtime_lock_file_sha256": SHA,
            "runtime_lock_path": "/stage/runtime-lock.json",
            "runtime_lock_canonical_sha256": SHA,
            "wheelhouse_manifest_file_sha256": SHA,
            "wheelhouse_path": "/stage/wheelhouse",
            "wheelhouse_canonical_sha256": SHA,
            "install_receipt_path": "/stage/install-receipt.json",
            "install_receipt_file_sha256": SHA,
            "install_receipt_canonical_sha256": SHA,
            "root_wheel_sha256": SHA,
            "root_wheel_path": "/stage/root.whl",
            "native_wheel_path": "/stage/native.whl",
            "installed_record_aggregate_sha256": SHA,
            "interpreter": interpreter,
        },
        "no_shadow_runtime_contract": copy.deepcopy(builder.NO_SHADOW_RUNTIME_CONTRACT),
        "pending_current_runtime_evidence": copy.deepcopy(
            builder.PENDING_CURRENT_RUNTIME_EVIDENCE
        ),
        "rollback": copy.deepcopy(builder.ROLLBACK),
        "evidence_boundary": copy.deepcopy(builder.EVIDENCE_BOUNDARY),
    }
    payload[builder.CANONICAL_FIELD] = builder.release_io.document_sha256(
        payload,
        builder.CANONICAL_FIELD,
    )
    return payload


def _validate(payload: dict) -> dict[str, str]:
    return runtime._validate_active_release(  # noqa: SLF001
        payload,
        expected_canonical_sha256=payload[builder.CANONICAL_FIELD],
        expected_artifact_sha256=runtime.DIRECT_OWNER_EXACT_ARTIFACT_SHA256,
        expected_manifest_file_sha256=SHA,
        expected_policy_file_sha256=SHA,
        expected_predicate_bundle_file_sha256=SHA,
    )


def test_successor_schema_roundtrips_and_v3_remains_separate() -> None:
    payload = _payload()
    assert _validate(payload)["execution_commit"] == GIT
    assert payload["schema_version"] != runtime.DIRECT_OWNER_ACTIVE_RELEASE_V3_SCHEMA


def test_b0_fallback_guard_allows_only_named_reconciliation_method() -> None:
    baseline = b"""
class MakerEngine:
    def sync_position(self, *, required=False):
        return required

    def quote(self, mid):
        return mid + 1
"""
    allowed_body_change = b"""
class MakerEngine:
    def sync_position(self, *, required=False):
        for attempt in range(3):
            if required:
                return attempt
        return None

    def quote(self, mid):
        return mid + 1
"""
    protected_signature_change = b"""
class MakerEngine:
    def sync_position(self, retries=3):
        return retries > 0

    def quote(self, mid):
        return mid + 1
"""
    protected_change = b"""
class MakerEngine:
    def sync_position(self, *, required=False):
        return required

    def quote(self, mid, *, skew=0):
        return mid + skew
"""
    allowed = builder.B0_FALLBACK_ALLOWED_METHODS
    baseline_hash = builder._semantic_ast_sha256_redacting_method_bodies(  # noqa: SLF001
        baseline,
        allowed_methods=allowed,
    )
    assert baseline_hash == builder._semantic_ast_sha256_redacting_method_bodies(  # noqa: SLF001
        allowed_body_change,
        allowed_methods=allowed,
    )
    assert baseline_hash != builder._semantic_ast_sha256_redacting_method_bodies(  # noqa: SLF001
        protected_signature_change,
        allowed_methods=allowed,
    )
    assert baseline_hash != builder._semantic_ast_sha256_redacting_method_bodies(  # noqa: SLF001
        protected_change,
        allowed_methods=allowed,
    )


def test_b0_fallback_receipt_binds_real_successor_file() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    execution_commit = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    current = subprocess.run(
        ("git", "show", f"{execution_commit}:{builder.B0_FALLBACK_PATH}"),
        cwd=repository_root,
        check=True,
        capture_output=True,
    ).stdout
    mechanics = subprocess.run(
        (
            "git",
            "show",
            "e0804e1dd8b199e2dc04d36c0dcd5f27e9fc83d5:"
            f"{builder.B0_FALLBACK_PATH}",
        ),
        cwd=repository_root,
        check=True,
        capture_output=True,
    ).stdout

    protected = builder._protected_semantics(  # noqa: SLF001
        repository_root,
        execution_commit,
    )

    assert protected["b0_fallback_file_sha256"] == hashlib.sha256(current).hexdigest()
    assert protected["b0_fallback_file_sha256"] != hashlib.sha256(mechanics).hexdigest()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["authorization_basis"].update(
            {"shared_execution_safety_authority_from_current_owner_directive": False}
        ),
        lambda value: value["protected_semantics"].update(
            {"frozen_mechanics_evidence_bytes_referenced_not_modified": False}
        ),
        lambda value: value["native_build"].update({"soabi": "cpython-311-linux"}),
        lambda value: value["rollback"]["deep_predecessor"].update(
            {"automatic_historical_runtime_start_authorized": True}
        ),
    ],
)
def test_successor_rejects_authority_safety_native_and_rollback_drift(mutate) -> None:
    payload = _payload()
    mutate(payload)
    payload[builder.CANONICAL_FIELD] = runtime._canonical_sha256(payload)  # noqa: SLF001
    with pytest.raises(ValueError):
        _validate(payload)


def _deployable_release(tmp_path: Path) -> tuple[Path, dict, dict]:
    payload = _payload()
    commit = deploy.SUCCESSOR_EXECUTION_COMMIT
    payload["execution"] = {
        "execution_commit": commit,
        "execution_tree": deploy.SUCCESSOR_EXECUTION_TREE,
        "annotated_operational_tag": deploy.SUCCESSOR_ANNOTATED_TAG,
        "annotated_operational_tag_object": deploy.SUCCESSOR_ANNOTATED_TAG_OBJECT,
        "tag_peeled_commit": commit,
    }
    native = payload["native_build"]
    native.update(
        {
            "runtime_lock_path": f"/stage/runtime-lock-{commit}.json",
            "wheelhouse_path": f"/stage/wheelhouse-{SHA}",
            "install_receipt_path": f"/stage/locked-runtime-install-{commit}.json",
            "root_wheel_path": f"/stage/root-wheel-{commit}/root.whl",
            "native_wheel_path": f"/stage/native-wheel-{commit}/native.whl",
        }
    )
    payload[builder.CANONICAL_FIELD] = builder.release_io.document_sha256(
        payload,
        builder.CANONICAL_FIELD,
    )
    path = tmp_path / "successor-release.json"
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)
    file_sha256 = deploy.gate_v2.file_sha256(path)
    repo_root = "/srv/narrowgate"
    plan = {
        "planner_repository_root": str(tmp_path),
        "execution": {
            "execution_commit": commit,
            "execution_tree": deploy.SUCCESSOR_EXECUTION_TREE,
            "annotated_tag": deploy.SUCCESSOR_ANNOTATED_TAG,
            "annotated_tag_object": deploy.SUCCESSOR_ANNOTATED_TAG_OBJECT,
            "tag_peeled_commit": commit,
        },
        "artifact": {
            "artifact_sha256": runtime.DIRECT_OWNER_EXACT_ARTIFACT_SHA256,
            "manifest_path": "/private/manifest.json",
            "manifest_file_sha256": SHA,
            "policy_path": "/private/policy.json",
            "policy_file_sha256": SHA,
            "predicate_bundle_path": "/private/predicates.json",
            "predicate_bundle_file_sha256": SHA,
        },
        "configs": {
            "active": {"config_sha256": SHA},
            "disabled": {"config_sha256": SHA},
        },
        "active_pointer": {"repo_root": repo_root},
        "host": {"trusted_static_python_sha256": SHA},
        "remote": {
            "stage_root": "/stage",
            "safety_release_path": deploy._remote_active_release_path(  # noqa: SLF001
                repo_root, file_sha256
            ),
            "safety_release_file_sha256": file_sha256,
            "safety_release_canonical_sha256": payload[builder.CANONICAL_FIELD],
        },
    }
    return path, payload, plan


def test_deploy_release_binding_rejects_forged_execution_and_native_receipt(
    tmp_path: Path,
) -> None:
    path, payload, plan = _deployable_release(tmp_path)
    binding = deploy._validate_active_release_for_activation(  # noqa: SLF001
        path,
        plan=plan,
        activation_envelope_binding=None,
    )
    deploy._validate_active_release_phase_binding(binding, plan=plan)  # noqa: SLF001

    forged_execution = copy.deepcopy(payload)
    forged_execution["execution"]["execution_commit"] = "0" * 40
    forged_execution[builder.CANONICAL_FIELD] = builder.release_io.document_sha256(
        forged_execution,
        builder.CANONICAL_FIELD,
    )
    path.write_text(
        json.dumps(forged_execution, sort_keys=True) + "\n",
        encoding="ascii",
    )
    with pytest.raises(
        deploy.BuyE3TransactionalDeployError,
        match="active release validation failed.*execution_identity_drifted",
    ):
        deploy._validate_active_release_for_activation(  # noqa: SLF001
            path,
            plan=plan,
            activation_envelope_binding=None,
        )

    forged_receipt = copy.deepcopy(payload)
    forged_receipt["native_build"]["file_sha256"] = "0" * 64
    forged_receipt[builder.CANONICAL_FIELD] = builder.release_io.document_sha256(
        forged_receipt,
        builder.CANONICAL_FIELD,
    )
    path.write_text(
        json.dumps(forged_receipt, sort_keys=True) + "\n",
        encoding="ascii",
    )
    forged_binding = deploy._validate_active_release_for_activation(  # noqa: SLF001
        path,
        plan=plan,
        activation_envelope_binding=None,
    )
    with pytest.raises(
        deploy.BuyE3TransactionalDeployError,
        match="release differs from the plan-time safety authority",
    ):
        deploy._validate_active_release_phase_binding(  # noqa: SLF001
            forged_binding,
            plan=plan,
        )
