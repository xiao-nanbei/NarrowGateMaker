from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_buy_e3_cross_host_transport_v5 as subject

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
        "FROZEN_FINAL_ANNOTATED_TAG": "f05-owner-buy-e3-direct-live-v4-fixture",
        "FROZEN_FINAL_TAG_OBJECT": "3" * 40,
        "FROZEN_FINAL_RELEASE_SCHEMA": "fixture.release.v4",
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
        "FROZEN_FINAL_LIFECYCLE_FIX_SUPPLEMENT": {
            "schema_version": "fixture.lifecycle_fix_supplement.v1",
            "status": "fixture_lifecycle_fix_verified",
            "file_sha256": "d" * 64,
            "canonical_field": "canonical_lifecycle_fix_supplement_sha256",
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


def test_new_epoch_source_constants_are_exact_and_receipts_fail_closed_pending() -> None:
    assert SOURCE_FROZEN_FINAL == {
        "execution_commit": "07ef93733a3a685caba945c7761a48473e403072",
        "execution_tree": "ff505cd81a8eb11f2087d2ae27e7986fd99b0444",
        "annotated_tag": "f05-owner-buy-e3-direct-live-v4-20260824",
        "tag_object": "da83fa0b4aed00e4d04ea3faa212b2fb27a81f0d",
        "release_schema": (
            "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
            "direct_owner_active_release.v2"
        ),
        "release_status": "owner_authorized_direct_live_lifecycle_repair_pending_evidence",
        "release_file_sha256": ("ff888f4b5973563275c2b97e1554d45c9d686ef15d686440bf096521aab17fc2"),
        "release_canonical_sha256": (
            "823ca1e4d53e968eb0afc53c4d2cad99cc17aac696548baa1700e800a4579702"
        ),
        "resource_schema": (
            "causal_multichannel_window_boolean_cooldown_owner_buy_e3_v1."
            "current_host_concurrent_resource_gate.v7"
        ),
        "resource_status": (
            "fresh_external_venues_disabled_correct_benchmark_route_concurrent_gate_passed"
        ),
        "resource_file_sha256": "",
        "resource_canonical_sha256": "",
        "active_schema": (
            "causal_multichannel_window_boolean_cooldown_owner_buy_e3_v1."
            "fresh_external_venues_disabled_active_process_capture.v5"
        ),
        "active_status": (
            "fresh_external_venues_disabled_correct_benchmark_active_process_captured"
        ),
        "active_file_sha256": "",
        "active_canonical_sha256": "",
        "config_correction_file_sha256": "",
        "config_correction_canonical_sha256": "",
        "disabled_config_sha256": (
            "10158a92177cd87b77fdb24a2a477dcab4b41cfb29208cf96c19953edafe166f"
        ),
        "active_config_sha256": (
            "ad153012b14e725a3ac24f0ddbe02bc353168a13ec827b777cc94761020524ec"
        ),
        "resource_path": SOURCE_FROZEN_FINAL["resource_path"],
        "active_path": SOURCE_FROZEN_FINAL["active_path"],
        "config_correction_path": SOURCE_FROZEN_FINAL["config_correction_path"],
    }
    assert str(SOURCE_FROZEN_FINAL["resource_path"]).endswith(
        "/f05-buy-e3-resource-gate-v7-no-external-20260824/attempt2/current_host_resource_gate.json"
    )
    assert str(SOURCE_FROZEN_FINAL["active_path"]).endswith(
        "/f05-buy-e3-active-capture-v7-no-external-20260824/active_process_capture_v5.json"
    )
    assert str(SOURCE_FROZEN_FINAL["config_correction_path"]).endswith(
        "/f05-buy-e3-no-external-shadow-phase1-v4-20260824/receipts/config_correction_v4.json"
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
    monkeypatch.setattr(subject, "FROZEN_FINAL_LIFECYCLE_FIX_SUPPLEMENT", None)
    with pytest.raises(subject.CrossHostTransportError, match="supplement is not source-frozen"):
        subject._frozen_final_execution()  # noqa: SLF001
    monkeypatch.setattr(
        subject,
        "FROZEN_FINAL_LIFECYCLE_FIX_SUPPLEMENT",
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


def test_validate_runtime_authority_binds_release_v2_and_clean_tag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = "fixture.direct_owner_active_release.v2"
    status = "fixture_final_v4_owner_authority"
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
        "authorization_basis": dict(subject.AUTHORIZATION_BASIS),
        "scope": {
            "side": "BUY",
            "trigger": "exposure_increasing_executed_fill",
            "output": "total_cooldown",
            "reducing_buy_unchanged": True,
            "sell_owner_policy_unchanged": True,
        },
        "rollback": {
            "buy_e3_enabled": False,
            "buy_deadline_identity": "B0",
            "e3_deadline_imported": False,
            "b0_seconds": 85,
            "b0_multiplier": "consecutive_fill_units",
            "b0_contract": "85s_x_consecutive_fill_units",
        },
        "exact_artifact": {
            "artifact_sha256": subject.FROZEN_FINAL_ARTIFACT_SHA256,
            "roles": roles,
        },
        "parent_direct_owner_release": subject.PARENT_DIRECT_OWNER_RELEASE,
        "historical_evidence_state": subject.HISTORICAL_EVIDENCE_STATE,
        "historical_attempt4_anchor": subject.HISTORICAL_ATTEMPT4_ANCHOR,
        "exact_v5_recovery": subject.EXACT_V5_RECOVERY,
        "pending_current_runtime_evidence": subject.PENDING_CURRENT_RUNTIME_EVIDENCE,
        "lifecycle_fix_contract": subject.LIFECYCLE_FIX_CONTRACT,
        "lifecycle_fix_supplement": subject.FROZEN_FINAL_LIFECYCLE_FIX_SUPPLEMENT,
        "evidence_boundary": subject.RELEASE_V2_EVIDENCE_BOUNDARY,
    }
    payload["canonical_active_release_sha256"] = subject._document_sha256(  # noqa: SLF001
        payload, "canonical_active_release_sha256"
    )
    path = _write(tmp_path / "release-v2.json", payload)
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
    stale["historical_evidence_state"]["panel_rebuild_continues"] = True
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

    lifecycle_drift = json.loads(json.dumps(payload))
    lifecycle_drift["lifecycle_fix_contract"]["e3_decision_semantics_unchanged"] = False
    lifecycle_drift["canonical_active_release_sha256"] = subject._document_sha256(  # noqa: SLF001
        lifecycle_drift, "canonical_active_release_sha256"
    )
    _write(path, lifecycle_drift)
    monkeypatch.setattr(
        subject,
        "FROZEN_FINAL_RELEASE_FILE_SHA256",
        hashlib.sha256(path.read_bytes()).hexdigest(),
    )
    monkeypatch.setattr(
        subject,
        "FROZEN_FINAL_RELEASE_CANONICAL_SHA256",
        lifecycle_drift["canonical_active_release_sha256"],
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
    assert payload["checks"]["active_capture_v3_content_exact"] is True
    assert "active_capture_v1_content_exact" not in payload["checks"]
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
        "schema_version": subject.resource_v7.config_successor.SCHEMA_VERSION,
        "status": subject.resource_v7.config_successor.STATUS,
        "file_sha256": "6" * 64,
        "canonical_field": subject.resource_v7.config_successor.CANONICAL_FIELD,
        "canonical_sha256": "7" * 64,
    }
    host = {
        "instance_id": subject.CURRENT_INSTANCE_ID,
        "instance_type": subject.CURRENT_INSTANCE_TYPE,
    }
    stable = {
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
    }
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
    monkeypatch.setattr(subject, "_validate_remote_content_at_path", lambda *_a, **_k: None)
    monkeypatch.setattr(subject, "_portable_components", lambda **_k: components)
    monkeypatch.setattr(subject.resource_v7, "host_identity", lambda **_k: host)
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
        generated_utc="2026-08-24T00:00:01Z",
    )
    assert payload["live_process_attestation"]["pid"] == active["active_process"]["pid"]
    assert (
        payload["live_process_attestation"]["active_capture_stable_identity_sha256"]
        == payload["live_process_attestation"]["recaptured_stable_identity_sha256"]
    )
    assert payload["checks"]["captured_live_not_retroactive"] is True
    assert payload["checks"]["frozen_final_runtime_authority_exact"] is True


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
        )


def test_remote_attestation_rejects_wrong_host(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _remote_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(
        subject.resource_v7,
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
