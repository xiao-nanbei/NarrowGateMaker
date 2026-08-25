from __future__ import annotations

import copy

import pytest

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
    payload[builder.CANONICAL_FIELD] = runtime._canonical_sha256(payload)  # noqa: SLF001
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
