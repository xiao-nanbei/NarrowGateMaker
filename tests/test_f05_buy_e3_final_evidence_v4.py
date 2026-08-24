from __future__ import annotations

import json
import stat
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_buy_e3_final_evidence_v4 as subject


def _write(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


def _content(
    schema: str,
    status: str | None,
    file_marker: str,
    canonical_marker: str,
    *,
    canonical_field: str = "canonical_fixture_sha256",
    size_bytes: int = 100,
) -> dict[str, Any]:
    return {
        "schema_version": schema,
        "status": status,
        "file_sha256": file_marker * 64,
        "canonical_field": canonical_field,
        "canonical_sha256": canonical_marker * 64,
        "size_bytes": size_bytes,
        "mode": "0600",
    }


@pytest.fixture(autouse=True)
def _freeze_transport(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    values: dict[str, Any] = {
        "FROZEN_FINAL_EXECUTION_COMMIT": "1" * 40,
        "FROZEN_FINAL_EXECUTION_TREE": "2" * 40,
        "FROZEN_FINAL_ANNOTATED_TAG": "f05-owner-buy-e3-direct-live-v4-fixture",
        "FROZEN_FINAL_TAG_OBJECT": "3" * 40,
        "FROZEN_FINAL_RELEASE_SCHEMA": "fixture.direct_owner_active_release.v2",
        "FROZEN_FINAL_RELEASE_STATUS": "fixture_direct_v4_active",
        "FROZEN_FINAL_RELEASE_FILE_SHA256": "4" * 64,
        "FROZEN_FINAL_RELEASE_CANONICAL_SHA256": "5" * 64,
        "FROZEN_FINAL_RESOURCE_SCHEMA": "fixture.resource.v4",
        "FROZEN_FINAL_RESOURCE_STATUS": "fixture_resource_v4_passed",
        "FROZEN_FINAL_RESOURCE_FILE_SHA256": "6" * 64,
        "FROZEN_FINAL_RESOURCE_CANONICAL_SHA256": "7" * 64,
        "FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA": "fixture.active_capture.v2",
        "FROZEN_FINAL_ACTIVE_CAPTURE_STATUS": "fixture_active_v4_captured",
        "FROZEN_FINAL_ACTIVE_CAPTURE_FILE_SHA256": "8" * 64,
        "FROZEN_FINAL_ACTIVE_CAPTURE_CANONICAL_SHA256": "9" * 64,
        "FROZEN_FINAL_DISABLED_CONFIG_SHA256": "a" * 64,
        "FROZEN_FINAL_ACTIVE_CONFIG_SHA256": "b" * 64,
        "FROZEN_FINAL_ARTIFACT_SHA256": "c" * 64,
        "FROZEN_FINAL_RESOURCE_PATH_PROVENANCE": str(tmp_path / "resource.json"),
        "FROZEN_FINAL_ACTIVE_CAPTURE_PATH_PROVENANCE": str(tmp_path / "active.json"),
        "FROZEN_FINAL_LIFECYCLE_FIX_SUPPLEMENT": dict(subject.LIFECYCLE_FIX_SUPPLEMENT_CONTENT),
    }
    for name, value in values.items():
        monkeypatch.setattr(subject.transport, name, value)


def _release_bundle() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    execution = subject.transport._frozen_final_execution()  # noqa: SLF001
    roles = {
        role: _content(
            f"fixture.{role}.v1",
            None if role == "predicate_bundle" else "frozen",
            str(index + 1),
            str(index + 4),
            canonical_field=f"canonical_{role}_sha256",
            size_bytes=200 + index,
        )
        for index, role in enumerate(("manifest", "policy", "predicate_bundle"))
    }
    release = {
        "action_authorized": True,
        "live_authorized": True,
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
            "artifact_sha256": subject.transport.FROZEN_FINAL_ARTIFACT_SHA256,
            "roles": roles,
        },
    }
    binding = {
        "schema_version": subject.transport.FROZEN_FINAL_RELEASE_SCHEMA,
        "status": subject.transport.FROZEN_FINAL_RELEASE_STATUS,
        "file_sha256": subject.transport.FROZEN_FINAL_RELEASE_FILE_SHA256,
        "canonical_field": "canonical_active_release_sha256",
        "canonical_sha256": subject.transport.FROZEN_FINAL_RELEASE_CANONICAL_SHA256,
        "size_bytes": 777,
        "mode": "0600",
    }
    artifact = subject.transport._artifact_projection(release)  # noqa: SLF001
    return release, binding, execution, artifact


def _portable() -> dict[str, Any]:
    release, binding, execution, artifact = _release_bundle()
    del release
    runtime_files = {
        name: f"{index + 1:x}" * 64
        for index, name in enumerate(sorted(subject.REQUIRED_V4_RUNTIME_SOURCES))
    }
    receipts = {
        "current_host_resource_gate": {
            **_content(
                subject.transport.FROZEN_FINAL_RESOURCE_SCHEMA,
                subject.transport.FROZEN_FINAL_RESOURCE_STATUS,
                "6",
                "7",
                canonical_field="canonical_resource_receipt_sha256",
            ),
            "local_filename": subject.transport.RESOURCE_FILENAME,
        },
        "active_process_capture": {
            **_content(
                subject.transport.FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA,
                subject.transport.FROZEN_FINAL_ACTIVE_CAPTURE_STATUS,
                "8",
                "9",
                canonical_field="canonical_active_capture_sha256",
            ),
            "local_filename": subject.transport.ACTIVE_CAPTURE_FILENAME,
        },
        "remote_active_attestation": {
            **_content(
                subject.transport.REMOTE_ATTESTATION_SCHEMA,
                subject.transport.REMOTE_ATTESTATION_STATUS,
                "d",
                "e",
                canonical_field=subject.transport.REMOTE_ATTESTATION_CANONICAL_FIELD,
            ),
            "local_filename": subject.transport.REMOTE_ATTESTATION_FILENAME,
        },
    }
    return {
        "host": {
            "provider": subject.transport.CURRENT_PROVIDER,
            "region": subject.transport.CURRENT_REGION,
            "instance_id": subject.transport.CURRENT_INSTANCE_ID,
            "instance_type": subject.transport.CURRENT_INSTANCE_TYPE,
            "public_ipv4": subject.transport.CURRENT_PUBLIC_IPV4_PROVENANCE,
            "public_ipv4_role": "network_locator_provenance_only_not_host_authority",
            "resource_host_identity": {"fixture": "host"},
        },
        "runtime_execution": execution,
        "runtime_authority": {
            **binding,
            "execution": execution,
            "runtime_authority": True,
        },
        "exact_artifact": artifact,
        "resource_disabled_process": {
            "pid": 10,
            "pid_start_ticks": 100,
            "process_identity_sha256": "f" * 64,
            "config_sha256": subject.transport.FROZEN_FINAL_DISABLED_CONFIG_SHA256,
            "fresh_pid": True,
            "fresh_start_ticks": True,
            "same_pid_pre_post": True,
        },
        "transition": {
            "disabled_pid": 10,
            "disabled_pid_start_ticks": 100,
            "active_pid": 20,
            "active_pid_start_ticks": 200,
            "active_process_identity_sha256": "0" * 64,
            "fresh_disabled_to_active_restart": True,
        },
        "active_runtime": {
            "config_sha256": subject.transport.FROZEN_FINAL_ACTIVE_CONFIG_SHA256,
            "runtime_identity": {
                "schema_version": "runtime.v1",
                "file_sha256": "1" * 64,
                "canonical_sha256": "2" * 64,
            },
            "startup_attestation": {
                "schema_version": "startup.v4",
                "status": "accepted",
                "canonical_sha256": "3" * 64,
            },
            "runtime_source_manifest_sha256": "4" * 64,
            "runtime_source_files": runtime_files,
            "artifact_sha256": subject.transport.FROZEN_FINAL_ARTIFACT_SHA256,
            "buy_e3_enabled": True,
            "owner_override_effective": True,
            "startup_semantics": {
                "startup_status": "accepted",
                "running_checkout_commit": execution["execution_commit"],
                "running_checkout_tree": execution["execution_tree"],
            },
        },
        "source_receipts": receipts,
    }


def _history() -> dict[str, Any]:
    _release, _binding, _execution, artifact = _release_bundle()
    return {
        "role": "historical_mechanics_and_regression_anchor_only",
        "attempt_id": subject.HISTORICAL_OPERATIONAL_ATTEMPT_V10_ID,
        "operational_attempt": {"canonical_sha256": "1" * 64},
        "attempt4_mechanics_anchor": _content("attempt4.v2", "historical", "1", "2"),
        "exact_v5_mechanics_recovery": _content("v5.v1", "historical", "2", "3"),
        "direct_v3_runtime_execution": {
            "execution_commit": "9" * 40,
            "execution_tree": "8" * 40,
            "annotated_operational_tag": "direct-v3",
            "annotated_operational_tag_object": "7" * 40,
            "tag_peeled_commit": "9" * 40,
        },
        "direct_v3_runtime_authority": _content("release.v1", "historical", "8", "9"),
        "direct_v3_exact_artifact": artifact,
        "direct_v3_regressions": {
            "full_regression": _content("full.v1", "passed", "1", "2"),
            "focused_successor_regression": _content("focused.v1", "passed", "2", "3"),
            "sell54_parity": _content("sell.v1", "passed", "3", "4"),
        },
        "attempt4_resource_or_activation_claimed": False,
        "final_runtime_authority": False,
        "final_exact_artifact_authority": False,
    }


def _predecessor_payload() -> dict[str, Any]:
    return {
        "schema_version": subject.REJECTED_PREDECESSOR_CONTENT["schema_version"],
        "status": "rejected_not_admitted",
        "epoch": {"baseline_epoch_id": subject.REJECTED_PREDECESSOR_EPOCH_ID},
        "rejection": {
            "error_count": 1,
            "drop_count": 0,
            "exchange_error_code": -5022,
            "formal_collection_valid": False,
            "formal_admission_allowed": False,
        },
        "authority_boundary": {
            "final_active_capture": False,
            "lifecycle_admission": False,
            "successor_runtime_authority": False,
            "research_supported": False,
            "shadow_created": False,
            "companion_created": False,
        },
    }


def _supplement_payload() -> dict[str, Any]:
    return {
        "schema_version": subject.LIFECYCLE_FIX_SUPPLEMENT_CONTENT["schema_version"],
        "status": subject.LIFECYCLE_FIX_SUPPLEMENT_CONTENT["status"],
        "v4_execution": subject.transport._frozen_final_execution(),  # noqa: SLF001
        "e3_unchanged": {
            "verified": True,
            "artifact_sha256": subject.transport.FROZEN_FINAL_ARTIFACT_SHA256,
            "action_vocabulary_seconds": [79, 173, 223, 356, 640, 709, 2048],
        },
        "permissions": {"research": False, "action": False, "live": False},
        "focused_regression": {"passed": 12, "failed": 0},
        "full_regression": {"passed": 157, "failed": 0},
    }


def _patch_authority(monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, Any], dict[str, Any]]:
    release, binding, _execution, _artifact = _release_bundle()
    monkeypatch.setattr(
        subject.transport,
        "validate_runtime_authority",
        lambda *_args, **_kwargs: (release, binding),
    )
    return release, binding


def test_all_five_receipt_schemas_are_new_v4_identities() -> None:
    schemas = {
        subject.ENVELOPE_SCHEMA,
        subject.COMPLETION_SCHEMA,
        subject.COMPOSITION_SCHEMA,
        subject.ATTEMPT_FINAL_SCHEMA,
        subject.EVIDENCE_RELEASE_SCHEMA,
    }
    assert len(schemas) == 5
    assert all(schema.endswith(".v4") for schema in schemas)


def test_frozen_lifecycle_and_rejected_epoch_exact7_identities() -> None:
    assert set(subject.LIFECYCLE_FIX_SUPPLEMENT_CONTENT) == set(subject.CONTENT_BINDING_FIELDS)
    assert (
        subject.LIFECYCLE_FIX_SUPPLEMENT_CONTENT["file_sha256"]
        == "c7a83f37f679ab94f7c0c670d53a43d894295d94cc74927e3a83fd3313336e87"
    )
    assert (
        subject.LIFECYCLE_FIX_SUPPLEMENT_CONTENT["canonical_sha256"]
        == "e69c4edb2025937a8569cbedd3163f3ec3b953a17fc904218e4df332dc1f221d"
    )
    assert set(subject.REJECTED_PREDECESSOR_CONTENT) == set(subject.CONTENT_BINDING_FIELDS)
    assert (
        subject.REJECTED_PREDECESSOR_CONTENT["file_sha256"]
        == "c44f3f32ae61635ce683e5711f19fd59863e4235996a9401f48d62bc1af4d80b"
    )
    assert (
        subject.REJECTED_PREDECESSOR_CONTENT["canonical_sha256"]
        == "4a3c01f7f178fa2d3f573a1696c637074fd74b51e846bd785886689ba44613d1"
    )


def test_portable_v4_accepts_release_v2_and_rejects_v3_execution() -> None:
    release, binding, execution, artifact = _release_bundle()
    portable = _portable()
    observed = subject._validate_portable_v4(  # noqa: SLF001
        portable,
        release=release,
        release_binding=binding,
        execution=execution,
        artifact=artifact,
    )
    assert observed["runtime_execution"] == execution

    portable["runtime_execution"] = _history()["direct_v3_runtime_execution"]
    with pytest.raises(subject.FinalEvidenceV4Error, match="not direct-v4"):
        subject._validate_portable_v4(  # noqa: SLF001
            portable,
            release=release,
            release_binding=binding,
            execution=execution,
            artifact=artifact,
        )


def test_activation_keeps_v3_attempt_as_history_while_v4_is_final(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    history = _history()
    portable = _portable()
    attempt_binding = {"canonical_sha256": "a" * 64}
    admission_binding = {"canonical_sha256": "b" * 64}
    monkeypatch.setattr(
        subject,
        "_historical_attempt_context",
        lambda *_args, **_kwargs: (
            {"attempt_id": subject.HISTORICAL_OPERATIONAL_ATTEMPT_V10_ID},
            attempt_binding,
            history,
        ),
    )
    monkeypatch.setattr(
        subject,
        "_admission_context",
        lambda *_args, **_kwargs: ({}, admission_binding, portable),
    )
    monkeypatch.setattr(
        subject,
        "_supplement_context",
        lambda *_args, **_kwargs: (
            _supplement_payload(),
            dict(subject.LIFECYCLE_FIX_SUPPLEMENT_CONTENT),
        ),
    )
    monkeypatch.setattr(
        subject,
        "_rejected_predecessor_context",
        lambda *_args, **_kwargs: (
            _predecessor_payload(),
            dict(subject.REJECTED_PREDECESSOR_CONTENT),
        ),
    )

    observed = subject.build_activation_envelope(
        historical_operational_attempt_v10_path=Path("attempt.json"),
        cross_host_admission_path=Path("admission.json"),
        lifecycle_fix_supplement_path=Path("supplement.json"),
        rejected_predecessor_path=Path("rejected.json"),
        historical_collector_v10_root=Path("collector-v10"),
        historical_direct_v3_root=Path("direct-v3"),
        attempt4_root=Path("attempt4"),
        final_v4_root=Path("direct-v4"),
        final_v4_release=Path("release-v2.json"),
        generated_utc="2026-08-24T00:00:00Z",
    )

    assert (
        observed["historical_evidence"]["direct_v3_runtime_execution"]
        != observed["runtime_execution"]
    )
    assert observed["runtime_authority"] == portable["runtime_authority"]
    assert observed["exact_artifact"] == portable["exact_artifact"]
    assert observed["checks"]["failed_predecessor_reused"] is False
    assert observed["authority_design"]["historical_direct_v3_is_runtime_authority"] is False


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (
            lambda row: row["e3_unchanged"].__setitem__("verified", False),
            "supplement semantics",
        ),
        (
            lambda row: row["focused_regression"].__setitem__("passed", 11),
            "supplement semantics",
        ),
    ),
)
def test_lifecycle_supplement_semantic_tamper_rejected(mutate: Any, message: str) -> None:
    payload = _supplement_payload()
    mutate(payload)
    with pytest.raises(subject.FinalEvidenceV4Error, match=message):
        subject._validate_supplement_payload(payload)  # noqa: SLF001


@pytest.mark.parametrize(
    "field_value",
    (("exchange_error_code", -2013), ("error_count", 0), ("formal_admission_allowed", True)),
)
def test_rejected_predecessor_semantic_tamper_rejected(field_value: tuple[str, Any]) -> None:
    payload = _predecessor_payload()
    field, value = field_value
    payload["rejection"][field] = value
    with pytest.raises(subject.FinalEvidenceV4Error, match="predecessor lifecycle semantics"):
        subject._validate_rejected_predecessor_payload(payload)  # noqa: SLF001


def test_lifecycle_admission_requires_new_epoch_and_exact_active_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active = _portable()["active_runtime"]
    binding = {
        "baseline_epoch_id": "prospective-new-v4-epoch",
        "config_sha256": active["config_sha256"],
        "runtime_code_sha256": "d" * 64,
        "runtime_code_files": dict(active["runtime_source_files"]),
    }
    monkeypatch.setattr(
        subject.base,
        "_validate_lifecycle_admission",
        lambda _path: ({"admitted": True}, dict(binding)),
    )
    _payload, observed = subject._lifecycle_context(  # noqa: SLF001
        Path("lifecycle.json"), active_runtime=active
    )
    assert observed["baseline_epoch_id"] != subject.REJECTED_PREDECESSOR_EPOCH_ID

    binding["baseline_epoch_id"] = subject.REJECTED_PREDECESSOR_EPOCH_ID
    with pytest.raises(subject.FinalEvidenceV4Error, match="epoch/config/runtime files"):
        subject._lifecycle_context(Path("lifecycle.json"), active_runtime=active)  # noqa: SLF001

    binding["baseline_epoch_id"] = "prospective-new-v4-epoch"
    first_source = next(iter(binding["runtime_code_files"]))
    binding["runtime_code_files"][first_source] = "0" * 64
    with pytest.raises(subject.FinalEvidenceV4Error, match="epoch/config/runtime files"):
        subject._lifecycle_context(Path("lifecycle.json"), active_runtime=active)  # noqa: SLF001


def _write_attempt_final_fixture(
    tmp_path: Path,
) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]]:
    _release, release_binding, execution, artifact = _release_bundle()
    runtime_authority = {
        **release_binding,
        "execution": execution,
        "runtime_authority": True,
    }
    composition = {
        "schema_version": subject.COMPOSITION_SCHEMA,
        "identity": subject.OWNER,
        "attempt_id": subject.HISTORICAL_OPERATIONAL_ATTEMPT_V10_ID,
        "status": subject.COMPOSITION_STATUS,
        "generated_utc": "2026-08-24T00:00:00Z",
        "runtime_execution": execution,
        "runtime_authority": runtime_authority,
        "exact_artifact": artifact,
        "composition_root_sha256": "d" * 64,
        "authority_design": dict(subject.AUTHORITY_DESIGN),
    }
    composition[subject.COMPOSITION_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        composition, subject.COMPOSITION_CANONICAL_FIELD
    )
    composition_path = _write(tmp_path / "composition.json", composition)
    composition_binding = subject._receipt_binding(  # noqa: SLF001
        composition_path,
        label="fixture composition",
        canonical_field=subject.COMPOSITION_CANONICAL_FIELD,
        schema=subject.COMPOSITION_SCHEMA,
        status=subject.COMPOSITION_STATUS,
    )
    attempt = {
        "schema_version": subject.ATTEMPT_FINAL_SCHEMA,
        "identity": subject.OWNER,
        "attempt_id": subject.HISTORICAL_OPERATIONAL_ATTEMPT_V10_ID,
        "status": subject.ATTEMPT_FINAL_STATUS,
        "generated_utc": "2026-08-24T00:01:00Z",
        "runtime_execution": execution,
        "runtime_authority": runtime_authority,
        "exact_artifact": artifact,
        "final_composition": composition_binding,
        "composition_root_sha256": composition["composition_root_sha256"],
        "lifecycle_fix_supplement": dict(subject.LIFECYCLE_FIX_SUPPLEMENT_CONTENT),
        "rejected_predecessor_epoch": {
            "epoch_id": subject.REJECTED_PREDECESSOR_EPOCH_ID,
            "evidence": dict(subject.REJECTED_PREDECESSOR_CONTENT),
            "rejected": True,
            "admitted": False,
            "reused": False,
        },
        "result": {
            "operational_evidence_complete": True,
            "cross_host_evidence_admitted": True,
            "lifecycle_evidence_admitted": True,
            "direct_v4_runtime_authority_unchanged": True,
            "historical_v3_is_history_only": True,
            "research_supported": False,
            "owner_risk_accepted": True,
            "new_authority_granted": False,
        },
        "formal_research_state": dict(subject.FORMAL_RESEARCH_STATE),
        "authority_design": dict(subject.AUTHORITY_DESIGN),
        "permissions": dict(subject.NO_NEW_AUTHORITY),
        "evidence_boundary": dict(subject.EVIDENCE_BOUNDARY),
    }
    attempt[subject.ATTEMPT_FINAL_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        attempt, subject.ATTEMPT_FINAL_CANONICAL_FIELD
    )
    return _write(tmp_path / "attempt-final.json", attempt), attempt, execution, artifact


def test_proof_release_revalidates_v4_and_never_calls_base_direct_helpers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempt_path, _attempt, execution, artifact = _write_attempt_final_fixture(tmp_path)
    release, _binding = _patch_authority(monkeypatch)

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("historical direct-v3 helper must not be called for final proof")

    monkeypatch.setattr(subject.base, "_direct_authority", forbidden)
    monkeypatch.setattr(subject.base, "_direct_execution", forbidden)
    output = tmp_path / "proof-release.json"
    payload, file_sha = subject.finalize_evidence_release(
        attempt_final_path=attempt_path,
        final_v4_root=tmp_path,
        final_v4_release=tmp_path / "release-v2.json",
        output_path=output,
        generated_utc="2026-08-24T00:02:00Z",
    )

    assert payload["runtime_execution"] == execution
    assert payload["exact_artifact"] == artifact
    assert payload["action_authorized"] == release["action_authorized"]
    assert payload["research_supported"] is False
    assert payload["owner_risk_accepted"] is True
    assert (
        payload["authority_provenance"]["proof_release_replaces_direct_v4_runtime_authority"]
        is False
    )
    assert payload["evidence_state"]["failed_predecessor_reused"] is False
    assert payload["evidence_state"]["runtime_consumed"] is True
    assert (
        payload["evidence_state"]["runtime_consumed_authority"]
        == "direct_v4_owner_release_v2"
    )
    assert payload["authority_design"]["runtime_consumed"] is True
    assert len(file_sha) == 64
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert (
        subject.validate_evidence_release(
            output,
            final_v4_root=tmp_path,
            final_v4_release=tmp_path / "release-v2.json",
        )
        == payload
    )
    with pytest.raises(subject.FinalEvidenceV4Error, match="receipt creation failed"):
        subject.finalize_evidence_release(
            attempt_final_path=attempt_path,
            final_v4_root=tmp_path,
            final_v4_release=tmp_path / "release-v2.json",
            output_path=output,
            generated_utc="2026-08-24T00:02:00Z",
        )


@pytest.mark.parametrize(
    "mutation",
    (
        lambda attempt: attempt["rejected_predecessor_epoch"].__setitem__("reused", True),
        lambda attempt: attempt["rejected_predecessor_epoch"]["evidence"].__setitem__(
            "canonical_sha256", "0" * 64
        ),
        lambda attempt: attempt["lifecycle_fix_supplement"].__setitem__(
            "canonical_sha256", "0" * 64
        ),
    ),
)
def test_proof_release_rejects_reused_or_tampered_source_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: Any,
) -> None:
    attempt_path, attempt, _execution, _artifact = _write_attempt_final_fixture(tmp_path)
    mutation(attempt)
    attempt[subject.ATTEMPT_FINAL_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        attempt, subject.ATTEMPT_FINAL_CANONICAL_FIELD
    )
    attempt_path.unlink()
    _write(attempt_path, attempt)
    _patch_authority(monkeypatch)
    with pytest.raises(subject.FinalEvidenceV4Error):
        subject.build_evidence_release(
            attempt_final_path=attempt_path,
            final_v4_root=tmp_path,
            final_v4_release=tmp_path / "release-v2.json",
            generated_utc="2026-08-24T00:02:00Z",
        )


def test_cli_requires_separate_historical_and_final_roots() -> None:
    parser = subject._parser()  # noqa: SLF001
    help_text = parser.format_help()
    assert "activation-envelope" in help_text
    activation = parser._subparsers._group_actions[0].choices["activation-envelope"]  # noqa: SLF001
    option_strings = {
        option
        for action in activation._actions  # noqa: SLF001
        for option in action.option_strings
    }
    assert {
        "--historical-collector-v10-root",
        "--historical-direct-v3-root",
        "--attempt4-root",
        "--final-v4-root",
        "--final-v4-release",
    }.issubset(option_strings)
