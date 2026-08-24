from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_buy_e3_cross_host_transport_v6 as subject

SOURCE_FROZEN_FINAL = {
    "execution_commit": subject.FROZEN_FINAL_EXECUTION_COMMIT,
    "execution_tree": subject.FROZEN_FINAL_EXECUTION_TREE,
    "annotated_tag": subject.FROZEN_FINAL_ANNOTATED_TAG,
    "tag_object": subject.FROZEN_FINAL_TAG_OBJECT,
    "release_schema": subject.FROZEN_FINAL_RELEASE_SCHEMA,
    "release_status": subject.FROZEN_FINAL_RELEASE_STATUS,
    "release_file_sha256": subject.FROZEN_FINAL_RELEASE_FILE_SHA256,
    "release_canonical_sha256": subject.FROZEN_FINAL_RELEASE_CANONICAL_SHA256,
    "resource_schema": subject.FROZEN_FINAL_RESOURCE_SCHEMA,
    "resource_status": subject.FROZEN_FINAL_RESOURCE_STATUS,
    "resource_file_sha256": subject.FROZEN_FINAL_RESOURCE_FILE_SHA256,
    "resource_canonical_sha256": subject.FROZEN_FINAL_RESOURCE_CANONICAL_SHA256,
    "active_schema": subject.FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA,
    "active_status": subject.FROZEN_FINAL_ACTIVE_CAPTURE_STATUS,
    "active_file_sha256": subject.FROZEN_FINAL_ACTIVE_CAPTURE_FILE_SHA256,
    "active_canonical_sha256": subject.FROZEN_FINAL_ACTIVE_CAPTURE_CANONICAL_SHA256,
    "config_correction_file_sha256": (subject.FROZEN_FINAL_CONFIG_CORRECTION_FILE_SHA256),
    "config_correction_canonical_sha256": (subject.FROZEN_FINAL_CONFIG_CORRECTION_CANONICAL_SHA256),
    "disabled_config_sha256": subject.FROZEN_FINAL_DISABLED_CONFIG_SHA256,
    "active_config_sha256": subject.FROZEN_FINAL_ACTIVE_CONFIG_SHA256,
    "resource_path": subject.FROZEN_FINAL_RESOURCE_PATH_PROVENANCE,
    "active_path": subject.FROZEN_FINAL_ACTIVE_CAPTURE_PATH_PROVENANCE,
    "config_correction_path": subject.FROZEN_CONFIG_CORRECTION_PATH_PROVENANCE,
}


def _write(path: Path, payload: dict[str, Any], *, mode: int = 0o600) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(mode)
    return path


def _document(schema: str, status: str, canonical_field: str, marker: str) -> dict[str, Any]:
    payload = {
        "schema_version": schema,
        "status": status,
        "marker": marker,
    }
    payload[canonical_field] = subject._document_sha256(payload, canonical_field)  # noqa: SLF001
    return payload


def _freeze_final(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    values = {
        "FROZEN_FINAL_EXECUTION_COMMIT": "1" * 40,
        "FROZEN_FINAL_EXECUTION_TREE": "2" * 40,
        "FROZEN_FINAL_ANNOTATED_TAG": "f05-owner-buy-e3-no-shadow-live-fixture",
        "FROZEN_FINAL_TAG_OBJECT": "3" * 40,
        "FROZEN_FINAL_RELEASE_SCHEMA": "fixture.release.v3",
        "FROZEN_FINAL_RELEASE_STATUS": "fixture_release_active",
        "FROZEN_FINAL_RELEASE_FILE_SHA256": "4" * 64,
        "FROZEN_FINAL_RELEASE_CANONICAL_SHA256": "5" * 64,
        "FROZEN_FINAL_RESOURCE_SCHEMA": "fixture.resource.v4",
        "FROZEN_FINAL_RESOURCE_STATUS": "fixture_resource_passed",
        "FROZEN_FINAL_RESOURCE_FILE_SHA256": "6" * 64,
        "FROZEN_FINAL_RESOURCE_CANONICAL_SHA256": "7" * 64,
        "FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA": "fixture.active.v2",
        "FROZEN_FINAL_ACTIVE_CAPTURE_STATUS": "fixture_active_captured",
        "FROZEN_FINAL_ACTIVE_CAPTURE_FILE_SHA256": "8" * 64,
        "FROZEN_FINAL_ACTIVE_CAPTURE_CANONICAL_SHA256": "9" * 64,
        "FROZEN_FINAL_CONFIG_CORRECTION_FILE_SHA256": "6" * 64,
        "FROZEN_FINAL_CONFIG_CORRECTION_CANONICAL_SHA256": "7" * 64,
        "FROZEN_FINAL_DISABLED_CONFIG_SHA256": "a" * 64,
        "FROZEN_FINAL_ACTIVE_CONFIG_SHA256": "b" * 64,
        "FROZEN_FINAL_ARTIFACT_SHA256": "c" * 64,
        "FROZEN_FINAL_RESOURCE_PATH_PROVENANCE": str(tmp_path / subject.RESOURCE_FILENAME),
        "FROZEN_FINAL_ACTIVE_CAPTURE_PATH_PROVENANCE": str(
            tmp_path / subject.ACTIVE_CAPTURE_FILENAME
        ),
        "FROZEN_CONFIG_CORRECTION_PATH_PROVENANCE": str(
            tmp_path / subject.CONFIG_CORRECTION_FILENAME
        ),
        "FROZEN_FINAL_NO_SHADOW_RUNTIME_SUPPLEMENT": {
            "schema_version": "fixture.no_shadow_runtime_supplement.v1",
            "status": "fixture_no_shadow_runtime_verified",
            "file_sha256": "d" * 64,
            "canonical_field": "canonical_no_shadow_runtime_supplement_sha256",
            "canonical_sha256": "e" * 64,
            "size_bytes": 456,
            "mode": "0600",
        },
    }
    for name, value in values.items():
        monkeypatch.setattr(subject, name, value)


@pytest.fixture(autouse=True)
def frozen_final(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    tmp_path.chmod(0o700)
    _freeze_final(monkeypatch, tmp_path)


def _incoming(root: Path) -> dict[str, Path]:
    root.mkdir(mode=0o700)
    fields = {
        "config_correction": ("fixture.config", "config", "config"),
        "current_host_resource_gate": ("fixture.resource", "resource", "resource"),
        "active_process_capture": ("fixture.active", "active", "active"),
        "remote_active_attestation": ("fixture.attestation", "attestation", "attestation"),
    }
    output: dict[str, Path] = {}
    for role, filename in subject.SOURCE_FILENAMES.items():
        schema, status, marker = fields[role]
        output[role] = _write(
            root / filename,
            _document(schema, status, "canonical_sha256", marker),
        )
    return output


def _content(path: Path) -> dict[str, Any]:
    opened = subject._open_private_json(path, path.name)  # noqa: SLF001
    payload = opened.payload
    return subject._content_binding(  # noqa: SLF001
        opened,
        canonical_field="canonical_sha256",
        expected_schema=payload["schema_version"],
        expected_status=payload["status"],
    )


def _portable_components(direct_binding: dict[str, Any]) -> dict[str, Any]:
    execution = subject._frozen_final_execution()  # noqa: SLF001
    return {
        "host": {
            "provider": "aws",
            "region": "ap-northeast-1",
            "instance_id": subject.CURRENT_INSTANCE_ID,
            "instance_type": subject.CURRENT_INSTANCE_TYPE,
            "public_ipv4": subject.CURRENT_PUBLIC_IPV4_PROVENANCE,
            "public_ipv4_role": "network_locator_provenance_only_not_host_authority",
            "resource_host_identity": {
                "instance_id": subject.CURRENT_INSTANCE_ID,
                "instance_type": subject.CURRENT_INSTANCE_TYPE,
            },
        },
        "runtime_execution": execution,
        "runtime_authority": {
            **direct_binding,
            "execution": execution,
            "runtime_authority": True,
        },
        "exact_artifact": {"artifact_sha256": subject.FROZEN_FINAL_ARTIFACT_SHA256},
        "resource_disabled_process": {
            "pid": 10,
            "pid_start_ticks": 100,
            "process_identity_sha256": "d" * 64,
            "config_sha256": subject.FROZEN_FINAL_DISABLED_CONFIG_SHA256,
            "fresh_pid": True,
            "fresh_start_ticks": True,
            "same_pid_pre_post": True,
            "runtime_source_manifest_sha256": "5" * 64,
            "runtime_source_files": {
                row["path"]: row["sha256"]
                for row in subject.resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.values()
            },
        },
        "transition": {
            "disabled_pid": 10,
            "disabled_pid_start_ticks": 100,
            "active_pid": 20,
            "active_pid_start_ticks": 200,
            "active_process_identity_sha256": "e" * 64,
            "fresh_disabled_to_active_restart": True,
        },
        "active_runtime": {
            "config_sha256": subject.FROZEN_FINAL_ACTIVE_CONFIG_SHA256,
            "runtime_identity": {
                "schema_version": "runtime.v1",
                "file_sha256": "f" * 64,
                "canonical_sha256": "0" * 64,
            },
            "startup_attestation": {
                "schema_version": "startup.v1",
                "status": "accepted",
                "canonical_sha256": "1" * 64,
            },
            "runtime_source_manifest_sha256": "2" * 64,
            "runtime_source_files": {"live/main.py": "3" * 64},
            "artifact_sha256": subject.FROZEN_FINAL_ARTIFACT_SHA256,
            "buy_e3_enabled": True,
            "owner_override_effective": True,
            "startup_semantics": {"startup_status": "accepted"},
            "active_health_window": {
                "schema_version": subject.active_capture_v8.HEALTH_WINDOW_SCHEMA,
                "status": subject.active_capture_v8.HEALTH_WINDOW_STATUS,
                "boundary_offset_bytes": 100,
                "active_pid": 20,
                "active_pid_start_ticks": 200,
                "active_process_stable_identity_sha256": "4" * 64,
                "rows": [],
                "checks": dict(subject._ACTIVE_HEALTH_WINDOW_CHECKS),  # noqa: SLF001
            },
        },
    }


def _patch_source_validator(monkeypatch: pytest.MonkeyPatch) -> None:
    direct_binding = {
        "schema_version": subject.FROZEN_FINAL_RELEASE_SCHEMA,
        "status": subject.FROZEN_FINAL_RELEASE_STATUS,
        "file_sha256": subject.FROZEN_FINAL_RELEASE_FILE_SHA256,
        "canonical_field": "canonical_active_release_sha256",
        "canonical_sha256": subject.FROZEN_FINAL_RELEASE_CANONICAL_SHA256,
        "size_bytes": 123,
        "mode": "0600",
    }
    components = _portable_components(direct_binding)

    def validate(
        *,
        correction_path: Path,
        resource_path: Path,
        active_path: Path,
        attestation_path: Path,
        **_kwargs: Any,
    ) -> subject._SourceSet:  # noqa: SLF001
        bindings = {
            "config_correction": _content(correction_path),
            "current_host_resource_gate": _content(resource_path),
            "active_process_capture": _content(active_path),
            "remote_active_attestation": _content(attestation_path),
            "direct_active_release": direct_binding,
        }
        return subject._SourceSet(  # noqa: SLF001
            correction={"fixture": "config"},
            resource={"fixture": "resource"},
            active={"fixture": "active"},
            attestation={**components},
            release={"fixture": "release"},
            bindings=bindings,
        )

    monkeypatch.setattr(subject, "_validate_source_set", validate)


def test_final_authority_fails_closed_until_new_epoch_is_source_frozen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subject, "FROZEN_FINAL_RELEASE_FILE_SHA256", "")
    with pytest.raises(subject.CrossHostTransportError, match="not a lowercase SHA256"):
        subject._frozen_final_execution()  # noqa: SLF001


def test_formal_transport_route_is_module_only_and_requires_remote_live_log() -> None:
    assert subject.FORMAL_MODULE_ROUTE == "scripts.f05_buy_e3_cross_host_transport_v6"
    repository = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [sys.executable, "-m", subject.FORMAL_MODULE_ROUTE, "--help"],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    with pytest.raises(SystemExit):
        subject._parser().parse_args(  # noqa: SLF001
            [
                "remote-attest",
                "--direct-repository-root",
                "/runtime",
                "--direct-release",
                "/runtime/release.json",
                "--config-correction",
                "/remote/config.json",
                "--resource-receipt",
                "/remote/resource.json",
                "--active-capture",
                "/remote/active.json",
                "--output",
                "/remote/attestation.json",
            ]
        )


def test_new_epoch_source_constants_are_exact_and_receipts_fail_closed_pending() -> None:
    assert SOURCE_FROZEN_FINAL["release_schema"] == subject.direct_release_v3.SCHEMA_VERSION
    assert SOURCE_FROZEN_FINAL["release_status"] == subject.direct_release_v3.STATUS
    assert SOURCE_FROZEN_FINAL["resource_schema"] == subject.resource_v8.RESOURCE_SCHEMA
    assert SOURCE_FROZEN_FINAL["resource_status"] == subject.resource_v8.RESOURCE_STATUS
    assert SOURCE_FROZEN_FINAL["active_schema"] == subject.active_capture_v8.SCHEMA_VERSION
    assert SOURCE_FROZEN_FINAL["active_status"] == subject.active_capture_v8.STATUS
    assert SOURCE_FROZEN_FINAL["disabled_config_sha256"] == (
        "d92fdec7ce89586f56fb1a6c80a6bc6fbe96b50023bd8c481cae730606c75204"
    )
    assert SOURCE_FROZEN_FINAL["active_config_sha256"] == (
        "3d8463c47c1cc2ff2017c9f6e7a963c77a8edb0cc692c48d89b03ee09bff772e"
    )
    assert SOURCE_FROZEN_FINAL["execution_commit"] == ("eacb6ccb1f4437d99d8385ba3f46ba6012f5c1de")
    assert SOURCE_FROZEN_FINAL["execution_tree"] == ("0343bd5586b337385cf2aa0d7a643f5c32b0da77")
    assert SOURCE_FROZEN_FINAL["annotated_tag"] == (
        "f05-owner-buy-e3-no-shadow-runtime-v3-20260824"
    )
    assert SOURCE_FROZEN_FINAL["tag_object"] == ("3878ea05252ef8f274b6f74ee7a984431c53b892")
    assert SOURCE_FROZEN_FINAL["release_file_sha256"] == (
        "310d86d30bd875a7029b3e2f784877c6802ab7b05b0f639383e68bb81a458f49"
    )
    assert SOURCE_FROZEN_FINAL["release_canonical_sha256"] == (
        "81d4449301d29828162a7fb57f52c855803682a697639b6d9cfa2d38a2846b8f"
    )
    assert SOURCE_FROZEN_FINAL["resource_file_sha256"] == (
        "158c9e8345645174b441ca1ec3bc907b2a9eb05a4ddfc149a7373ac123f514db"
    )
    assert SOURCE_FROZEN_FINAL["resource_canonical_sha256"] == (
        "f84320d0658e0d30b99f79e4f60707ce85bb2fd1dbaf2d29a04dec2fc2dbfee6"
    )
    assert SOURCE_FROZEN_FINAL["active_file_sha256"] == (
        "6647d2667747e988c28108a3a95e2ced512a017236b8d2a909f6d9b4729be3d7"
    )
    assert SOURCE_FROZEN_FINAL["active_canonical_sha256"] == (
        "7d31830797e071912265ac2f1803b7812757f8b5c8bd64a9209a0d6541dd3861"
    )
    assert SOURCE_FROZEN_FINAL["config_correction_file_sha256"] == (
        "486fe6d6d8b8d5667488b3916f1917a93ebead65284151f81ee3829330964c24"
    )
    assert SOURCE_FROZEN_FINAL["config_correction_canonical_sha256"] == (
        "55f4e703849b1852aabafa00e5a08d5ba15da06dabc42bcdd8bc76d12aa6dd35"
    )
    assert SOURCE_FROZEN_FINAL["resource_path"].endswith(
        "/attempt1/current_host_resource_gate.json"
    )
    assert SOURCE_FROZEN_FINAL["active_path"].endswith("/active_process_capture_v7.json")
    assert SOURCE_FROZEN_FINAL["config_correction_path"].endswith(
        "/receipts/config_correction.json"
    )


def test_final_authority_rejects_stale_direct_owner_v1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        subject,
        "FROZEN_FINAL_RELEASE_SCHEMA",
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_direct_owner_active_release.v1",
    )
    with pytest.raises(subject.CrossHostTransportError, match="stale direct-owner release v1"):
        subject._frozen_final_execution()  # noqa: SLF001


def test_final_authority_requires_exact_content_only_supplement_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subject, "FROZEN_FINAL_NO_SHADOW_RUNTIME_SUPPLEMENT", None)
    with pytest.raises(subject.CrossHostTransportError, match="supplement is not source-frozen"):
        subject._frozen_final_execution()  # noqa: SLF001
    monkeypatch.setattr(
        subject,
        "FROZEN_FINAL_NO_SHADOW_RUNTIME_SUPPLEMENT",
        {
            "schema_version": "fixture.supplement.v1",
            "status": "verified",
            "file_sha256": "a" * 64,
            "canonical_field": "canonical_sha256",
            "canonical_sha256": "b" * 64,
            "size_bytes": 10,
            "mode": "0600",
            "path": "/remote/not-content-authority",
        },
    )
    with pytest.raises(subject.CrossHostTransportError, match="supplement is not source-frozen"):
        subject._frozen_final_execution()  # noqa: SLF001


def test_validate_runtime_authority_binds_release_v3_and_no_shadow_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = subject.direct_release_v3.SCHEMA_VERSION
    status = subject.direct_release_v3.STATUS
    monkeypatch.setattr(subject, "FROZEN_FINAL_RELEASE_SCHEMA", schema)
    monkeypatch.setattr(subject, "FROZEN_FINAL_RELEASE_STATUS", status)
    roles = {
        role: {
            "schema_version": f"fixture.{role}.v1",
            "status": None if role == "predicate_bundle" else "frozen",
            "file_sha256": f"{index + 1:064x}",
            "canonical_field": "canonical_sha256",
            "canonical_sha256": f"{index + 4:064x}",
            "size_bytes": 100 + index,
            "mode": "0600",
        }
        for index, role in enumerate(("manifest", "policy", "predicate_bundle"))
    }
    config_pair = {
        "schema_version": "f05_buy_e3_no_shadow_config_pair.v1",
        "status": "exact_no_shadow_config_pair_frozen",
        "predecessor": {
            "disabled_file_sha256": subject.direct_release_v3.OLD_DISABLED_CONFIG_SHA256,
            "active_file_sha256": subject.direct_release_v3.OLD_ACTIVE_CONFIG_SHA256,
        },
        "disabled": {
            "file_sha256": subject.FROZEN_FINAL_DISABLED_CONFIG_SHA256,
            "semantic_sha256": subject.direct_release_v3.NEW_DISABLED_CONFIG_SEMANTIC_SHA256,
            "size_bytes": subject.direct_release_v3.NEW_DISABLED_CONFIG_SIZE,
            "mode": "0600",
        },
        "active": {
            "file_sha256": subject.FROZEN_FINAL_ACTIVE_CONFIG_SHA256,
            "semantic_sha256": subject.direct_release_v3.NEW_ACTIVE_CONFIG_SEMANTIC_SHA256,
            "size_bytes": subject.direct_release_v3.NEW_ACTIVE_CONFIG_SIZE,
            "mode": "0600",
        },
        "old_to_new_semantic_additions": list(subject.direct_release_v3.NEW_CONFIG_ADDITIONS),
        "active_disabled_only_difference": subject.direct_release_v3.CONFIG_PAIR_DIFFERENCE,
        "required_false_paths": list(subject.direct_release_v3.REQUIRED_FALSE_CONFIG_PATHS),
        "external_shadow_only_marker_inert": True,
        "release_fields_present_in_yaml": False,
    }
    payload = {
        "schema_version": schema,
        "identity": schema,
        "status": status,
        "generated_utc": "2026-08-24T00:00:00Z",
        "execution": subject._frozen_final_execution(),  # noqa: SLF001
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "outcome_informed_owner_override": True,
        "action_authorized": True,
        "live_authorized": True,
        "authorization_basis": dict(subject.direct_release_v3.AUTHORIZATION_BASIS),
        "scope": dict(subject.direct_release_v3.SCOPE),
        "rollback": dict(subject.direct_release_v3.ROLLBACK),
        "exact_artifact": {
            "artifact_sha256": subject.FROZEN_FINAL_ARTIFACT_SHA256,
            "roles": roles,
        },
        "parent_runtime_authority": {
            "release": dict(subject.direct_release_v3.PARENT_RELEASE_V2_BINDING),
            "execution": dict(subject.direct_release_v3.PARENT_EXECUTION),
        },
        "historical_evidence": dict(subject.direct_release_v3.HISTORICAL_EVIDENCE),
        "config_pair": config_pair,
        "runtime_fix_contract": dict(subject.direct_release_v3.RUNTIME_FIX_CONTRACT),
        "runtime_fix_supplement": subject.FROZEN_FINAL_NO_SHADOW_RUNTIME_SUPPLEMENT,
        "changed_repository_files": {
            "live/main.py": {"git_blob_sha1": "1" * 40, "file_sha256": "2" * 64}
        },
        "no_shadow_runtime_contract": dict(subject.direct_release_v3.NO_SHADOW_RUNTIME_CONTRACT),
        "pending_current_runtime_evidence": dict(
            subject.direct_release_v3.PENDING_CURRENT_RUNTIME_EVIDENCE
        ),
        "evidence_boundary": dict(subject.direct_release_v3.EVIDENCE_BOUNDARY),
    }
    payload["canonical_active_release_sha256"] = subject._document_sha256(  # noqa: SLF001
        payload, "canonical_active_release_sha256"
    )
    path = _write(tmp_path / "release-v3.json", payload)
    monkeypatch.setattr(
        subject,
        "FROZEN_FINAL_RELEASE_FILE_SHA256",
        hashlib.sha256(path.read_bytes()).hexdigest(),
    )
    monkeypatch.setattr(
        subject,
        "FROZEN_FINAL_RELEASE_CANONICAL_SHA256",
        payload["canonical_active_release_sha256"],
    )
    monkeypatch.setattr(
        subject.release_io,
        "_operational_git_identity",
        lambda *_a, **_k: subject._frozen_final_execution(),  # noqa: SLF001
    )

    observed, binding = subject.validate_runtime_authority(tmp_path, path)

    assert observed == payload
    assert binding["file_sha256"] == subject.FROZEN_FINAL_RELEASE_FILE_SHA256
    monkeypatch.setattr(subject, "FROZEN_FINAL_RELEASE_FILE_SHA256", "f" * 64)
    with pytest.raises(subject.CrossHostTransportError, match="semantic authority drifted"):
        subject.validate_runtime_authority(tmp_path, path)

    stale = json.loads(json.dumps(payload))
    stale["historical_evidence"]["panel_rebuild_continues"] = True
    stale["canonical_active_release_sha256"] = subject._document_sha256(  # noqa: SLF001
        stale, "canonical_active_release_sha256"
    )
    _write(path, stale)
    monkeypatch.setattr(
        subject,
        "FROZEN_FINAL_RELEASE_FILE_SHA256",
        hashlib.sha256(path.read_bytes()).hexdigest(),
    )
    monkeypatch.setattr(
        subject,
        "FROZEN_FINAL_RELEASE_CANONICAL_SHA256",
        stale["canonical_active_release_sha256"],
    )
    with pytest.raises(subject.CrossHostTransportError, match="semantic authority drifted"):
        subject.validate_runtime_authority(tmp_path, path)

    shadow_drift = json.loads(json.dumps(payload))
    shadow_drift["no_shadow_runtime_contract"]["global_flow_shadow_enabled"] = True
    shadow_drift["canonical_active_release_sha256"] = subject._document_sha256(  # noqa: SLF001
        shadow_drift, "canonical_active_release_sha256"
    )
    _write(path, shadow_drift)
    monkeypatch.setattr(
        subject,
        "FROZEN_FINAL_RELEASE_FILE_SHA256",
        hashlib.sha256(path.read_bytes()).hexdigest(),
    )
    monkeypatch.setattr(
        subject,
        "FROZEN_FINAL_RELEASE_CANONICAL_SHA256",
        shadow_drift["canonical_active_release_sha256"],
    )
    with pytest.raises(subject.CrossHostTransportError, match="semantic authority drifted"):
        subject.validate_runtime_authority(tmp_path, path)


def test_incoming_accepts_exact_same_directory_allowlist(tmp_path: Path) -> None:
    paths = _incoming(tmp_path / "incoming")
    assert subject._incoming_paths(tmp_path / "incoming") == paths  # noqa: SLF001


def test_incoming_rejects_missing_and_unallowlisted_file(tmp_path: Path) -> None:
    paths = _incoming(tmp_path / "incoming")
    paths["active_process_capture"].unlink()
    with pytest.raises(subject.CrossHostTransportError, match="exactly allowlisted"):
        subject._incoming_paths(tmp_path / "incoming")  # noqa: SLF001
    _write(tmp_path / "incoming" / "extra.json", {"extra": True})
    with pytest.raises(subject.CrossHostTransportError, match="exactly allowlisted"):
        subject._incoming_paths(tmp_path / "incoming")  # noqa: SLF001


def test_incoming_rejects_wrong_mode(tmp_path: Path) -> None:
    paths = _incoming(tmp_path / "incoming")
    paths["current_host_resource_gate"].chmod(0o644)
    with pytest.raises(subject.CrossHostTransportError, match="0600"):
        subject._incoming_paths(tmp_path / "incoming")  # noqa: SLF001


def test_incoming_rejects_symlink(tmp_path: Path) -> None:
    paths = _incoming(tmp_path / "incoming")
    target = paths["active_process_capture"]
    target.unlink()
    target.symlink_to(paths["current_host_resource_gate"])
    with pytest.raises(subject.CrossHostTransportError, match="single-link"):
        subject._incoming_paths(tmp_path / "incoming")  # noqa: SLF001


def test_incoming_rejects_hardlink_and_duplicate_file_identity(tmp_path: Path) -> None:
    paths = _incoming(tmp_path / "incoming")
    target = paths["active_process_capture"]
    target.unlink()
    os.link(paths["current_host_resource_gate"], target)
    with pytest.raises(subject.CrossHostTransportError, match="single-link"):
        subject._incoming_paths(tmp_path / "incoming")  # noqa: SLF001


def test_incoming_rejects_duplicate_json_key(tmp_path: Path) -> None:
    paths = _incoming(tmp_path / "incoming")
    paths["remote_active_attestation"].write_text('{"a":1,"a":2}\n', encoding="ascii")
    paths["remote_active_attestation"].chmod(0o600)
    with pytest.raises(subject.CrossHostTransportError, match="single-link JSON"):
        subject._incoming_paths(tmp_path / "incoming")  # noqa: SLF001


def test_admission_root_rejects_path_escape(tmp_path: Path) -> None:
    parent = tmp_path / "parent"
    parent.mkdir(mode=0o700)
    with pytest.raises(subject.CrossHostTransportError, match="path-escape"):
        subject._create_private_root(parent / ".." / "escape")  # noqa: SLF001


def test_copy_rejects_non_allowlisted_path_component(tmp_path: Path) -> None:
    root = tmp_path / "copy"
    root.mkdir(mode=0o700)
    descriptor = subject._open_directory_descriptor(root)  # noqa: SLF001
    try:
        with pytest.raises(subject.CrossHostTransportError, match="not allowlisted"):
            subject._copy_exclusive(descriptor, "../escape.json", b"{}\n")  # noqa: SLF001
    finally:
        os.close(descriptor)


def test_cross_host_admission_round_trip_and_portable_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_source_validator(monkeypatch)
    _incoming(tmp_path / "incoming")
    payload, file_sha = subject.finalize_cross_host_admission(
        incoming_root=tmp_path / "incoming",
        admission_root=tmp_path / "admitted",
        direct_repository_root=tmp_path,
        direct_release_path=tmp_path / "release.json",
        admitted_utc="2026-08-24T00:00:00Z",
    )
    path = tmp_path / "admitted" / subject.ADMISSION_FILENAME
    assert hashlib.sha256(path.read_bytes()).hexdigest() == file_sha
    assert (
        subject.validate_cross_host_admission(
            path,
            direct_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.json",
        )
        == payload
    )
    portable = payload["portable_evidence"]
    assert set(portable) == set(subject.PORTABLE_EVIDENCE_FIELDS)
    assert portable["runtime_authority"]["runtime_authority"] is True
    assert portable["host"]["instance_id"] == subject.CURRENT_INSTANCE_ID
    assert portable["host"]["public_ipv4_role"].endswith("not_host_authority")
    assert payload["checks"]["active_capture_v7_content_and_health_projection_exact"] is True
    assert payload["checks"]["remote_log_not_required_for_local_admission"] is True
    for role, row in portable["source_receipts"].items():
        assert set(row) == {*subject.CONTENT_BINDING_FIELDS, "local_filename"}
        assert row["local_filename"] == subject.SOURCE_FILENAMES[role]
    assert all(
        (tmp_path / "admitted" / name).stat().st_mode & 0o777 == 0o600
        for name in subject.SOURCE_FILENAMES.values()
    )


def test_admission_is_create_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_source_validator(monkeypatch)
    _incoming(tmp_path / "incoming")
    (tmp_path / "admitted").mkdir(mode=0o700)
    with pytest.raises(subject.CrossHostTransportError, match="already exists"):
        subject.finalize_cross_host_admission(
            incoming_root=tmp_path / "incoming",
            admission_root=tmp_path / "admitted",
            direct_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.json",
        )


def _admitted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    _patch_source_validator(monkeypatch)
    _incoming(tmp_path / "incoming")
    subject.finalize_cross_host_admission(
        incoming_root=tmp_path / "incoming",
        admission_root=tmp_path / "admitted",
        direct_repository_root=tmp_path,
        direct_release_path=tmp_path / "release.json",
        admitted_utc="2026-08-24T00:00:00Z",
    )
    return tmp_path / "admitted" / subject.ADMISSION_FILENAME


def test_admission_rejects_transferred_file_tamper_and_wrong_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    receipt = _admitted(tmp_path, monkeypatch)
    source = receipt.parent / subject.ACTIVE_CAPTURE_FILENAME
    payload = json.loads(source.read_text(encoding="ascii"))
    payload["marker"] = "tampered"
    payload["canonical_sha256"] = subject._document_sha256(  # noqa: SLF001
        payload, "canonical_sha256"
    )
    _write(source, payload)
    with pytest.raises(subject.CrossHostTransportError, match="identity drifted"):
        subject.validate_cross_host_admission(
            receipt,
            direct_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.json",
        )


def test_admission_rejects_wrong_host_even_when_recanonicalized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    receipt = _admitted(tmp_path, monkeypatch)
    payload = json.loads(receipt.read_text(encoding="ascii"))
    payload["portable_evidence"]["host"]["instance_id"] = "i-wrong"
    payload[subject.ADMISSION_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload, subject.ADMISSION_CANONICAL_FIELD
    )
    _write(receipt, payload)
    with pytest.raises(subject.CrossHostTransportError, match="identity drifted"):
        subject.validate_cross_host_admission(
            receipt,
            direct_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.json",
        )


def test_admission_rejects_mode_symlink_and_hardlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    receipt = _admitted(tmp_path, monkeypatch)
    source = receipt.parent / subject.RESOURCE_FILENAME
    source.chmod(0o644)
    with pytest.raises(subject.CrossHostTransportError, match="0600"):
        subject.validate_cross_host_admission(
            receipt,
            direct_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.json",
        )

    source.chmod(0o600)
    duplicate = tmp_path / "hardlink.json"
    os.link(source, duplicate)
    with pytest.raises(subject.CrossHostTransportError, match="0600"):
        subject.validate_cross_host_admission(
            receipt,
            direct_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.json",
        )
    duplicate.unlink()

    target = receipt.parent / subject.ACTIVE_CAPTURE_FILENAME
    replacement = tmp_path / "replacement.json"
    target.rename(replacement)
    target.symlink_to(replacement)
    with pytest.raises(subject.CrossHostTransportError, match="0600"):
        subject.validate_cross_host_admission(
            receipt,
            direct_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.json",
        )


def test_active_v2_content_only_binding_is_reopened_at_frozen_path(tmp_path: Path) -> None:
    canonical_field = "canonical_active_capture_sha256"
    path = _write(
        tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
        _document(
            subject.FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA,
            subject.FROZEN_FINAL_ACTIVE_CAPTURE_STATUS,
            canonical_field,
            "active-v2",
        ),
    )
    binding = subject._content_binding(  # noqa: SLF001
        subject._open_private_json(path, "active capture"),  # noqa: SLF001
        canonical_field=canonical_field,
        expected_schema=subject.FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA,
        expected_status=subject.FROZEN_FINAL_ACTIVE_CAPTURE_STATUS,
    )

    subject._validate_remote_content_at_path(binding, path, "active capture")  # noqa: SLF001

    with pytest.raises(subject.CrossHostTransportError, match="fields drifted"):
        subject._validate_remote_content_at_path(  # noqa: SLF001
            {**binding, "path": str(path)}, path, "active capture"
        )

    _write(
        path,
        _document(
            subject.FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA,
            subject.FROZEN_FINAL_ACTIVE_CAPTURE_STATUS,
            canonical_field,
            "tampered-active-v2",
        ),
    )
    with pytest.raises(subject.CrossHostTransportError, match="content binding drifted"):
        subject._validate_remote_content_at_path(binding, path, "active capture")  # noqa: SLF001


def _active_health_projection(*, updates: int) -> dict[str, Any]:
    shadow = {
        name: 0
        for name in {
            "externalSources",
            *subject.resource_v8.GLOBAL_FLOW_STATE_ZERO_FIELDS,
            *subject.resource_v8.GLOBAL_FLOW_VALUE_ZERO_FIELDS,
            *subject.resource_v8.GLOBAL_REFERENCE_ZERO_FIELDS,
            *subject.resource_v8.GLOBAL_REFERENCE_VALUE_ZERO_FIELDS,
            *subject.resource_v8.GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS,
        }
    }
    shadow.update(
        {
            "globalFlowReason": subject.resource_v8.SHADOW_DISABLED_REASON,
            "globalRefReason": subject.resource_v8.SHADOW_DISABLED_REASON,
        }
    )
    return {
        "boolean_cooldown_enabled": 1,
        "boolean_cooldown_updates": updates,
        "buy_e3_enabled": 1,
        "deep_book_buffer": 0,
        "shadow_disabled_state": shadow,
        "counter_values": {name: 0 for name in subject.resource_v8.WINDOW_ZERO_COUNTERS[:-2]},
    }


def _active_health_window(process: dict[str, Any], log_path: Path) -> dict[str, Any]:
    stable_sha = subject._canonical_sha256(  # noqa: SLF001
        subject._stable_process_projection(process)  # noqa: SLF001
    )
    return {
        "schema_version": subject.active_capture_v8.HEALTH_WINDOW_SCHEMA,
        "status": subject.active_capture_v8.HEALTH_WINDOW_STATUS,
        "log_path_provenance": str(log_path),
        "boundary_offset_bytes": 100,
        "active_pid": process["pid"],
        "active_pid_start_ticks": process["pid_start_ticks"],
        "active_process_stable_identity_sha256": stable_sha,
        "rows": [
            {
                "fresh_generation": 1,
                "line_offset_bytes": 110,
                "line_size_bytes": 20,
                "line_sha256": "5" * 64,
                "main_wall_timestamp_s": 1_000.0,
                "projection": _active_health_projection(updates=2_000),
            },
            {
                "fresh_generation": 2,
                "line_offset_bytes": 140,
                "line_size_bytes": 20,
                "line_sha256": "6" * 64,
                "main_wall_timestamp_s": 1_060.0,
                "projection": _active_health_projection(updates=2_500),
            },
        ],
        "checks": dict(subject._ACTIVE_HEALTH_WINDOW_CHECKS),  # noqa: SLF001
    }


def test_active_health_content_projection_is_exact_and_path_free(tmp_path: Path) -> None:
    process = {
        "schema_version": "process.v1",
        "pid": 20,
        "pid_start_ticks": 200,
        "cmdline": ["python", "live/main.py"],
        "cmdline_sha256": "1" * 64,
        "cwd": "/runtime",
        "config_path": "/runtime/active.yaml",
        "config_sha256": subject.FROZEN_FINAL_ACTIVE_CONFIG_SHA256,
        "python_executable": "/runtime/.venv/bin/python",
        "python_binary_resolved": "/usr/bin/python",
        "venv_root": "/runtime/.venv",
        "runtime_identity": {"present": True},
    }
    window = _active_health_window(process, Path("/remote/live.log"))
    portable = subject._validate_active_health_window_content(  # noqa: SLF001
        window, process=process
    )
    assert set(portable) == subject._PORTABLE_ACTIVE_HEALTH_WINDOW_FIELDS  # noqa: SLF001
    assert "log_path_provenance" not in portable
    assert portable["rows"][1]["projection"]["boolean_cooldown_updates"] == 2_500

    tampered = json.loads(json.dumps(window))
    tampered["rows"][1]["projection"]["counter_values"]["externalErrors"] = 1
    with pytest.raises(subject.CrossHostTransportError, match="projection drifted"):
        subject._validate_active_health_window_content(tampered, process=process)  # noqa: SLF001


def test_active_payload_requires_exact_v7_health_and_control_contracts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    binding = {
        "schema_version": subject.FROZEN_FINAL_RELEASE_SCHEMA,
        "status": subject.FROZEN_FINAL_RELEASE_STATUS,
        "file_sha256": subject.FROZEN_FINAL_RELEASE_FILE_SHA256,
        "canonical_field": "canonical_active_release_sha256",
        "canonical_sha256": subject.FROZEN_FINAL_RELEASE_CANONICAL_SHA256,
        "size_bytes": 100,
        "mode": "0600",
    }
    resource_binding = {
        **binding,
        "schema_version": subject.FROZEN_FINAL_RESOURCE_SCHEMA,
        "status": subject.FROZEN_FINAL_RESOURCE_STATUS,
        "file_sha256": subject.FROZEN_FINAL_RESOURCE_FILE_SHA256,
        "canonical_field": "canonical_resource_receipt_sha256",
        "canonical_sha256": subject.FROZEN_FINAL_RESOURCE_CANONICAL_SHA256,
    }
    correction_binding = {
        **binding,
        "schema_version": subject.resource_v8.config_successor.SCHEMA_VERSION,
        "status": subject.resource_v8.config_successor.STATUS,
        "file_sha256": subject.FROZEN_FINAL_CONFIG_CORRECTION_FILE_SHA256,
        "canonical_field": subject.resource_v8.config_successor.CANONICAL_FIELD,
        "canonical_sha256": subject.FROZEN_FINAL_CONFIG_CORRECTION_CANONICAL_SHA256,
    }
    semantics = {"startup_attestation_sha256": "7" * 64, "startup_status": "accepted"}
    monkeypatch.setattr(subject, "_active_runtime_semantics", lambda *_a, **_k: semantics)
    process = {
        "schema_version": "process.v1",
        "pid": 20,
        "pid_start_ticks": 200,
        "cmdline": ["python", "live/main.py"],
        "cmdline_sha256": "1" * 64,
        "cwd": "/runtime",
        "config_path": "/runtime/active.yaml",
        "config_sha256": subject.FROZEN_FINAL_ACTIVE_CONFIG_SHA256,
        "python_executable": "/runtime/.venv/bin/python",
        "python_binary_resolved": "/usr/bin/python",
        "venv_root": "/runtime/.venv",
        "runtime_identity": {
            "present": True,
            "path": "/runtime/runtime_identity.json",
            "file_sha256": "2" * 64,
        },
        "captured_utc": "2026-08-24T00:00:00Z",
        "runtime_identity_file_sha256": "2" * 64,
        "execution_commit": subject.FROZEN_FINAL_EXECUTION_COMMIT,
        "execution_tree": subject.FROZEN_FINAL_EXECUTION_TREE,
        "artifact_sha256": subject.FROZEN_FINAL_ARTIFACT_SHA256,
        "buy_e3_enabled": True,
        "owner_override_effective": True,
        "startup_attestation_sha256": semantics["startup_attestation_sha256"],
    }
    process["canonical_process_identity_sha256"] = subject._canonical_sha256(  # noqa: SLF001
        process
    )
    resource = {
        "host": {"instance_id": "fixture"},
        "config_correction": correction_binding,
        "fresh_disabled_process": {
            "pid": 10,
            "pid_start_ticks": 100,
            "canonical_process_identity_sha256": "3" * 64,
            "config_sha256": subject.FROZEN_FINAL_DISABLED_CONFIG_SHA256,
            "fresh_pid": True,
            "fresh_start_ticks": True,
            "same_pid_pre_post": True,
        },
    }
    payload = {
        "schema_version": subject.FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA,
        "identity": subject.OWNER,
        "status": subject.FROZEN_FINAL_ACTIVE_CAPTURE_STATUS,
        "generated_utc": "2026-08-24T00:01:00Z",
        "runtime_authority": binding,
        "resource_receipt": resource_binding,
        "config_correction": correction_binding,
        "host": resource["host"],
        "disabled_predecessor": {
            "pid": 10,
            "pid_start_ticks": 100,
            "process_identity_sha256": "3" * 64,
            "quiescent_before_active_capture": True,
        },
        "active_process": process,
        "runtime_identity": {},
        "runtime_identity_file_sha256": "2" * 64,
        "startup_semantics": semantics,
        "active_health_window": _active_health_window(process, Path("/remote/live.log")),
        "checks": dict(subject.active_capture_v8.CHECKS),
        "authority_design": dict(subject.active_capture_v8.AUTHORITY_DESIGN),
        "permissions": dict(subject.active_capture_v8.NO_AUTHORITY),
        "evidence_boundary": dict(subject.active_capture_v8.EVIDENCE_BOUNDARY),
        "canonical_active_capture_sha256": "4" * 64,
    }
    assert (
        subject._validate_active_payload(  # noqa: SLF001
            payload,
            release={},
            release_binding=binding,
            resource=resource,
            resource_binding=resource_binding,
        )
        == semantics
    )

    tampered = json.loads(json.dumps(payload))
    del tampered["checks"]["active_health_line_bytes_revalidated"]
    with pytest.raises(subject.CrossHostTransportError, match="semantic identity drifted"):
        subject._validate_active_payload(  # noqa: SLF001
            tampered,
            release={},
            release_binding=binding,
            resource=resource,
            resource_binding=resource_binding,
        )


def _remote_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    correction_path = _write(tmp_path / subject.CONFIG_CORRECTION_FILENAME, {"fixture": True})
    resource_path = _write(tmp_path / subject.RESOURCE_FILENAME, {"fixture": True})
    active_path = _write(tmp_path / subject.ACTIVE_CAPTURE_FILENAME, {"fixture": True})
    _write(tmp_path / "release.json", {"fixture": True})
    monkeypatch.setattr(subject, "FROZEN_FINAL_RESOURCE_PATH_PROVENANCE", str(resource_path))
    monkeypatch.setattr(subject, "FROZEN_FINAL_ACTIVE_CAPTURE_PATH_PROVENANCE", str(active_path))
    monkeypatch.setattr(
        subject,
        "FROZEN_CONFIG_CORRECTION_PATH_PROVENANCE",
        str(correction_path),
    )
    direct_binding = {
        "schema_version": subject.FROZEN_FINAL_RELEASE_SCHEMA,
        "status": subject.FROZEN_FINAL_RELEASE_STATUS,
        "file_sha256": subject.FROZEN_FINAL_RELEASE_FILE_SHA256,
        "canonical_field": "canonical_active_release_sha256",
        "canonical_sha256": subject.FROZEN_FINAL_RELEASE_CANONICAL_SHA256,
        "size_bytes": 100,
        "mode": "0600",
    }
    resource_binding = {
        **direct_binding,
        "schema_version": subject.FROZEN_FINAL_RESOURCE_SCHEMA,
        "status": subject.FROZEN_FINAL_RESOURCE_STATUS,
        "file_sha256": subject.FROZEN_FINAL_RESOURCE_FILE_SHA256,
        "canonical_field": "canonical_resource_receipt_sha256",
        "canonical_sha256": subject.FROZEN_FINAL_RESOURCE_CANONICAL_SHA256,
    }
    active_binding = {
        **direct_binding,
        "schema_version": subject.FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA,
        "status": subject.FROZEN_FINAL_ACTIVE_CAPTURE_STATUS,
        "file_sha256": subject.FROZEN_FINAL_ACTIVE_CAPTURE_FILE_SHA256,
        "canonical_field": "canonical_active_capture_sha256",
        "canonical_sha256": subject.FROZEN_FINAL_ACTIVE_CAPTURE_CANONICAL_SHA256,
    }
    correction_binding = {
        **direct_binding,
        "schema_version": subject.resource_v8.config_successor.SCHEMA_VERSION,
        "status": subject.resource_v8.config_successor.STATUS,
        "file_sha256": "6" * 64,
        "canonical_field": subject.resource_v8.config_successor.CANONICAL_FIELD,
        "canonical_sha256": "7" * 64,
    }
    host = {
        "instance_id": subject.CURRENT_INSTANCE_ID,
        "instance_type": subject.CURRENT_INSTANCE_TYPE,
    }
    stable = {
        "schema_version": "process.v1",
        "pid": 20,
        "pid_start_ticks": 200,
        "cmdline": ["python", "live/main.py"],
        "cmdline_sha256": "1" * 64,
        "cwd": "/runtime",
        "config_path": "/runtime/active.yaml",
        "config_sha256": subject.FROZEN_FINAL_ACTIVE_CONFIG_SHA256,
        "python_executable": "/runtime/.venv/bin/python",
        "python_binary_resolved": "/usr/bin/python",
        "venv_root": "/runtime/.venv",
        "runtime_identity": {
            "present": True,
            "path": "/runtime/runtime_identity.json",
            "file_sha256": "2" * 64,
            "schema_version": "runtime.v1",
        },
    }
    process = {
        **stable,
        "captured_utc": "2026-08-24T00:00:00Z",
        "canonical_process_identity_sha256": "3" * 64,
    }
    resource = {"host": host, "config_correction": correction_binding}
    active = {
        "generated_utc": "2026-08-24T00:00:00Z",
        "active_process": process,
        "runtime_identity": {
            "config_path": stable["config_path"],
            "config_sha256": stable["config_sha256"],
            "f05_buy_e3_active_release_path": str(tmp_path / "release.json"),
        },
        "runtime_identity_file_sha256": "2" * 64,
        "resource_receipt": resource_binding,
        "config_correction": correction_binding,
        "runtime_authority": direct_binding,
        "active_health_window": _active_health_window(process, tmp_path / "live.log"),
    }
    (tmp_path / "live.log").write_text("fixture\n", encoding="ascii")
    components = _portable_components(direct_binding)
    monkeypatch.setattr(subject, "_direct_authority", lambda *_a, **_k: ({}, direct_binding))
    monkeypatch.setattr(
        subject,
        "_validate_config_correction",
        lambda *_a, **_k: ({}, correction_binding, b"config"),
    )
    monkeypatch.setattr(
        subject,
        "_validate_resource",
        lambda *_a, **_k: (resource, resource_binding, b"resource"),
    )
    monkeypatch.setattr(
        subject,
        "_validate_active",
        lambda *_a, **_k: (active, active_binding, b"active", {}),
    )
    monkeypatch.setattr(
        subject,
        "_validate_active_against_remote_log",
        lambda *_a, **_k: active,
    )
    monkeypatch.setattr(subject, "_validate_remote_content_at_path", lambda *_a, **_k: None)
    monkeypatch.setattr(
        subject,
        "_portable_components",
        lambda **_k: json.loads(json.dumps(components)),
    )
    monkeypatch.setattr(subject.resource_v8, "host_identity", lambda **_k: host)
    recaptured = {
        **stable,
        "captured_utc": "2026-08-24T00:00:00Z",
        "canonical_process_identity_sha256": "4" * 64,
    }
    monkeypatch.setattr(
        subject.gate_v2,
        "capture_actual_process_identity",
        lambda **_k: recaptured,
    )
    return (
        active,
        components,
        {
            "resource": resource_binding,
            "config": correction_binding,
            "active": active_binding,
            "direct": direct_binding,
        },
    )


def test_remote_attestation_captures_live_not_retroactive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    active, _components, _bindings = _remote_fixture(tmp_path, monkeypatch)
    payload = subject.build_remote_active_attestation(
        direct_repository_root=tmp_path,
        direct_release_path=tmp_path / "release.json",
        config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
        resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
        active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
        live_log_path=tmp_path / "live.log",
        generated_utc="2026-08-24T00:00:01Z",
    )
    assert payload["live_process_attestation"]["pid"] == active["active_process"]["pid"]
    assert (
        payload["live_process_attestation"]["active_capture_stable_identity_sha256"]
        == payload["live_process_attestation"]["recaptured_stable_identity_sha256"]
    )
    assert payload["checks"]["captured_live_not_retroactive"] is True
    assert payload["checks"]["frozen_final_runtime_authority_exact"] is True
    assert payload["checks"]["active_health_window_revalidated_against_remote_log"] is True


def test_remote_attestation_calls_full_active_validator_with_live_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    active, _components, _bindings = _remote_fixture(tmp_path, monkeypatch)
    observed: dict[str, Any] = {}

    def validate(path: Path, **kwargs: Any) -> dict[str, Any]:
        observed["path"] = path
        observed.update(kwargs)
        return active

    monkeypatch.setattr(subject, "_validate_active_against_remote_log", validate)
    subject.build_remote_active_attestation(
        direct_repository_root=tmp_path,
        direct_release_path=tmp_path / "release.json",
        config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
        resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
        active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
        live_log_path=tmp_path / "live.log",
        generated_utc="2026-08-24T00:00:01Z",
    )
    assert observed["path"] == tmp_path / subject.ACTIVE_CAPTURE_FILENAME
    assert observed["live_log_path"] == tmp_path / "live.log"


def test_local_source_validation_does_not_reopen_remote_live_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _remote_fixture(tmp_path, monkeypatch)
    payload = subject.build_remote_active_attestation(
        direct_repository_root=tmp_path,
        direct_release_path=tmp_path / "release.json",
        config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
        resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
        active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
        live_log_path=tmp_path / "live.log",
        generated_utc="2026-08-24T00:00:01Z",
    )
    attestation = _write(tmp_path / subject.REMOTE_ATTESTATION_FILENAME, payload)
    monkeypatch.setattr(
        subject,
        "_validate_active_against_remote_log",
        lambda *_a, **_k: pytest.fail("local admission reopened the remote log"),
    )
    sources = subject._validate_source_set(  # noqa: SLF001
        correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
        resource_path=tmp_path / subject.RESOURCE_FILENAME,
        active_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
        attestation_path=attestation,
        direct_repository_root=tmp_path,
        direct_release_path=tmp_path / "release.json",
    )
    assert sources.attestation == payload


def test_remote_attestation_rejects_resource_path_provenance_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _remote_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(
        subject,
        "FROZEN_FINAL_RESOURCE_PATH_PROVENANCE",
        str(tmp_path / "wrong-resource-receipt.json"),
    )
    with pytest.raises(subject.CrossHostTransportError, match="path provenance drifted"):
        subject.build_remote_active_attestation(
            direct_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.json",
            config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
            resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
            active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
            live_log_path=tmp_path / "live.log",
        )


def test_remote_attestation_release_provenance_is_portable_and_cross_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    active, _components, _bindings = _remote_fixture(tmp_path, monkeypatch)
    remote_release = tmp_path / "release.json"
    payload = subject.build_remote_active_attestation(
        direct_repository_root=tmp_path,
        direct_release_path=remote_release,
        config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
        resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
        active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
        live_log_path=tmp_path / "live.log",
        generated_utc="2026-08-24T00:00:01Z",
    )
    attestation = _write(tmp_path / subject.REMOTE_ATTESTATION_FILENAME, payload)
    local_release = _write(tmp_path / "local-release-copy.json", {"fixture": True})

    validated = subject.validate_remote_active_attestation(
        attestation,
        direct_repository_root=tmp_path,
        direct_release_path=local_release,
        config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
        resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
        active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
        live_log_path=tmp_path / "live.log",
    )
    assert validated["project_references"]["direct_active_release"][
        "remote_path_provenance"
    ] == str(remote_release)

    payload["project_references"]["direct_active_release"]["remote_path_provenance"] = (
        "/tampered/remote-release.json"
    )
    payload[subject.REMOTE_ATTESTATION_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload, subject.REMOTE_ATTESTATION_CANONICAL_FIELD
    )
    _write(attestation, payload)
    with pytest.raises(subject.CrossHostTransportError, match="path provenance drifted"):
        subject.validate_remote_active_attestation(
            attestation,
            direct_repository_root=tmp_path,
            direct_release_path=local_release,
            config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
            resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
            active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
            live_log_path=tmp_path / "live.log",
        )

    payload["project_references"]["direct_active_release"]["remote_path_provenance"] = str(
        remote_release
    )
    payload[subject.REMOTE_ATTESTATION_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload, subject.REMOTE_ATTESTATION_CANONICAL_FIELD
    )
    _write(attestation, payload)
    active["runtime_identity"]["f05_buy_e3_active_release_path"] = (
        "/tampered/active-runtime-release.json"
    )
    with pytest.raises(subject.CrossHostTransportError, match="path provenance drifted"):
        subject.validate_remote_active_attestation(
            attestation,
            direct_repository_root=tmp_path,
            direct_release_path=local_release,
            config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
            resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
            active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
            live_log_path=tmp_path / "live.log",
        )


def test_remote_attestation_rejects_wrong_host(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _remote_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(
        subject.resource_v8,
        "host_identity",
        lambda **_k: {"instance_id": "i-wrong", "instance_type": subject.CURRENT_INSTANCE_TYPE},
    )
    with pytest.raises(subject.CrossHostTransportError, match="not the resource-gate host"):
        subject.build_remote_active_attestation(
            direct_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.json",
            config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
            resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
            active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
            live_log_path=tmp_path / "live.log",
        )


def test_remote_attestation_rejects_process_identity_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _remote_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(
        subject.gate_v2,
        "capture_actual_process_identity",
        lambda **_k: {
            **{
                field: None
                for field in subject._PROCESS_STABLE_FIELDS  # noqa: SLF001
            },
            "pid": 999,
        },
    )
    with pytest.raises(subject.CrossHostTransportError, match="changed after its capture"):
        subject.build_remote_active_attestation(
            direct_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.json",
            config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
            resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
            active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
            live_log_path=tmp_path / "live.log",
        )


def test_remote_attestation_validator_rejects_stable_identity_or_chronology_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _remote_fixture(tmp_path, monkeypatch)
    payload = subject.build_remote_active_attestation(
        direct_repository_root=tmp_path,
        direct_release_path=tmp_path / "release.json",
        config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
        resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
        active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
        live_log_path=tmp_path / "live.log",
        generated_utc="2026-08-24T00:00:01Z",
    )
    path = _write(tmp_path / subject.REMOTE_ATTESTATION_FILENAME, payload)

    payload["live_process_attestation"]["recaptured_stable_identity_sha256"] = "f" * 64
    payload[subject.REMOTE_ATTESTATION_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload, subject.REMOTE_ATTESTATION_CANONICAL_FIELD
    )
    _write(path, payload)
    with pytest.raises(subject.CrossHostTransportError, match="identity drifted"):
        subject.validate_remote_active_attestation(
            path,
            direct_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.json",
            config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
            resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
            active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
        )

    payload["live_process_attestation"]["recaptured_stable_identity_sha256"] = payload[
        "live_process_attestation"
    ]["active_capture_stable_identity_sha256"]
    payload["live_process_attestation"]["recaptured_utc"] = "2026-08-24T00:00:02Z"
    payload[subject.REMOTE_ATTESTATION_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload, subject.REMOTE_ATTESTATION_CANONICAL_FIELD
    )
    _write(path, payload)
    with pytest.raises(subject.CrossHostTransportError, match="identity drifted"):
        subject.validate_remote_active_attestation(
            path,
            direct_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.json",
            config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
            resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
            active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
        )


def test_remote_attestation_rejects_recanonicalized_portable_health_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _remote_fixture(tmp_path, monkeypatch)
    payload = subject.build_remote_active_attestation(
        direct_repository_root=tmp_path,
        direct_release_path=tmp_path / "release.json",
        config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
        resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
        active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
        live_log_path=tmp_path / "live.log",
        generated_utc="2026-08-24T00:00:01Z",
    )
    path = _write(tmp_path / subject.REMOTE_ATTESTATION_FILENAME, payload)
    payload["active_runtime"]["active_health_window"]["status"] = "tampered"
    payload[subject.REMOTE_ATTESTATION_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload, subject.REMOTE_ATTESTATION_CANONICAL_FIELD
    )
    _write(path, payload)
    with pytest.raises(subject.CrossHostTransportError, match="identity drifted"):
        subject.validate_remote_active_attestation(
            path,
            direct_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.json",
            config_correction_path=tmp_path / subject.CONFIG_CORRECTION_FILENAME,
            resource_receipt_path=tmp_path / subject.RESOURCE_FILENAME,
            active_capture_path=tmp_path / subject.ACTIVE_CAPTURE_FILENAME,
        )
