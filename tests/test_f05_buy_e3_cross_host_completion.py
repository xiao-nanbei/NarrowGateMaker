from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from scripts import f05_buy_e3_cross_host_completion as subject


def _attempt() -> dict:
    authority = {
        "path": "/local/direct-release.json",
        "file_sha256": subject.base.DIRECT_RELEASE_FILE_SHA256,
        "size_bytes": 100,
        "mode": "0600",
        "device": 7,
        "inode": 9,
        "schema_version": "direct.owner.release.v1",
        "status": "active",
        "canonical_field": "canonical_active_release_sha256",
        "canonical_sha256": subject.base.DIRECT_RELEASE_CANONICAL_SHA256,
        "runtime_authority": True,
    }
    return {
        "attempt_id": "operational-attempt-v10",
        "runtime_authority": authority,
        "exact_artifact": {
            "artifact_sha256": subject.base.ARTIFACT_SHA256,
            "roles": {"manifest": {}, "policy": {}, "predicate_bundle": {}},
        },
    }


def _content(
    *, schema: str, status: str, canonical_field: str, name: str
) -> dict:
    return {
        "schema_version": schema,
        "status": status,
        "file_sha256": "1" * 64,
        "canonical_field": canonical_field,
        "canonical_sha256": "2" * 64,
        "size_bytes": 123,
        "mode": "0600",
        "local_filename": name,
    }


def _transport() -> SimpleNamespace:
    return SimpleNamespace(
        REMOTE_ATTESTATION_SCHEMA=f"{subject.OWNER}.remote_active_attestation.v2",
        REMOTE_ATTESTATION_STATUS="remote_active_process_attested_for_cross_host_transport",
        REMOTE_ATTESTATION_CANONICAL_FIELD="canonical_remote_active_attestation_sha256",
    )


def _portable() -> dict:
    attempt = _attempt()
    disabled_pid = 41
    disabled_start = 100
    return {
        "host": dict(subject.EXPECTED_HOST),
        "runtime_execution": subject.base._direct_execution(),  # noqa: SLF001
        "runtime_authority": subject._portable_authority_projection(attempt),  # noqa: SLF001
        "exact_artifact": deepcopy(attempt["exact_artifact"]),
        "resource_disabled_process": {
            "pid": disabled_pid,
            "pid_start_ticks": disabled_start,
        },
        "transition": {
            "disabled_pid": disabled_pid,
            "disabled_pid_start_ticks": disabled_start,
            "active_pid": 52,
            "active_pid_start_ticks": 200,
            "disabled_same_pid_resource_gate": True,
            "disabled_predecessor_quiescent": True,
            "fresh_active_restart": True,
            "activation_via_sighup": False,
            "runtime_checkout_changed": False,
        },
        "active_runtime": {
            "execution": subject.base._direct_execution(),  # noqa: SLF001
            "artifact_sha256": subject.base.ARTIFACT_SHA256,
            "runtime_identity_file_sha256": "3" * 64,
            "startup_attestation_sha256": "4" * 64,
            "startup_status": "accepted",
            "config_sha256": "5" * 64,
            "runtime_source_manifest_sha256": "6" * 64,
            "runtime_source_files": {
                "strategy/maker_engine.py": "7" * 64,
                "strategy/boolean_cooldown_buy_e3.py": "8" * 64,
            },
        },
        "source_receipts": {
            "current_host_resource_gate": _content(
                schema=subject.base.RESOURCE_SCHEMA,
                status=subject.base.RESOURCE_STATUS,
                canonical_field=subject.base.RESOURCE_CANONICAL_FIELD,
                name="current_host_resource_gate.json",
            ),
            "active_process_capture": _content(
                schema=subject.base.ACTIVE_CAPTURE_SCHEMA,
                status=subject.base.ACTIVE_CAPTURE_STATUS,
                canonical_field="canonical_active_capture_sha256",
                name="active_process_capture.json",
            ),
            "remote_active_attestation": _content(
                schema=_transport().REMOTE_ATTESTATION_SCHEMA,
                status=_transport().REMOTE_ATTESTATION_STATUS,
                canonical_field=_transport().REMOTE_ATTESTATION_CANONICAL_FIELD,
                name="remote_active_attestation.json",
            ),
        },
    }


@pytest.fixture(autouse=True)
def _stub_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subject, "_transport_module", _transport)


def test_portable_projection_accepts_content_only_without_remote_inode() -> None:
    observed = subject._validate_portable_evidence(_portable(), attempt=_attempt())  # noqa: SLF001

    for row in observed["source_receipts"].values():
        assert "path" not in row
        assert "device" not in row
        assert "inode" not in row


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (
            lambda row: row["host"].__setitem__("instance_id", "i-wrong"),
            "host identity",
        ),
        (
            lambda row: row["runtime_authority"].__setitem__("runtime_authority", False),
            "runtime authority",
        ),
        (
            lambda row: row["transition"].__setitem__("active_pid", 41),
            "transition",
        ),
        (
            lambda row: row["active_runtime"].__setitem__("startup_status", "rejected"),
            "active runtime",
        ),
        (
            lambda row: row["source_receipts"]["active_process_capture"].__setitem__(
                "inode", 3
            ),
            "content binding fields",
        ),
    ),
)
def test_portable_projection_rejects_drift(mutate: object, message: str) -> None:
    portable = _portable()
    mutate(portable)  # type: ignore[operator]

    with pytest.raises(subject.CrossHostCompletionError, match=message):
        subject._validate_portable_evidence(portable, attempt=_attempt())  # noqa: SLF001


def test_activation_envelope_declares_nonretroactive_content_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = _attempt()
    attempt_binding = {"canonical_sha256": "9" * 64}
    admission_binding = {"canonical_sha256": "a" * 64}
    monkeypatch.setattr(
        subject,
        "_attempt_context",
        lambda *args, **kwargs: (attempt, attempt_binding),
    )
    monkeypatch.setattr(
        subject,
        "_admission_context",
        lambda *args, **kwargs: ({}, admission_binding, _portable()),
    )

    observed = subject.build_activation_envelope(
        operational_attempt_path=subject.Path("attempt.json"),
        cross_host_admission_path=subject.Path("admission.json"),
        collector_repository_root=subject.Path("collector"),
        direct_repository_root=subject.Path("direct"),
        attempt4_repository_root=subject.Path("attempt4"),
        generated_utc="2026-08-24T00:00:00Z",
    )

    assert observed["checks"]["captured_live_not_retroactive"] is True
    assert observed["checks"]["remote_inode_reinterpreted_locally"] is False
    assert observed["authority_design"]["runtime_authority_replaced"] is False
    assert observed["permissions"] == {"research": False, "action": False, "live": False}


def test_proof_release_explicitly_does_not_replace_runtime_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt_final = {
        "attempt_id": "operational-attempt-v10",
        "runtime_authority": {"path": "/local/direct-release.json"},
        "composition_root_sha256": "a" * 64,
    }
    binding = {"canonical_sha256": "b" * 64}
    direct_binding = {
        "canonical_sha256": subject.base.DIRECT_RELEASE_CANONICAL_SHA256,
        "runtime_authority": True,
    }
    direct = {
        "scope": {"buy_only": True},
        "rollback": {"identity": "B0"},
    }
    monkeypatch.setattr(subject, "validate_attempt_final", lambda *args, **kwargs: attempt_final)
    monkeypatch.setattr(subject, "_receipt_binding", lambda *args, **kwargs: binding)
    monkeypatch.setattr(
        subject.base,
        "_direct_authority",
        lambda *args, **kwargs: (direct, direct_binding),
    )
    monkeypatch.setattr(
        subject.base,
        "_artifact_projection",
        lambda _release: {"artifact_sha256": subject.base.ARTIFACT_SHA256},
    )

    observed = subject.build_evidence_release(
        attempt_final_path=subject.Path("attempt-final.json"),
        collector_repository_root=subject.Path("collector"),
        direct_repository_root=subject.Path("direct"),
        attempt4_repository_root=subject.Path("attempt4"),
        generated_utc="2026-08-24T00:00:00Z",
    )

    assert observed["authority_provenance"]["proof_release_replaces_runtime_authority"] is False
    assert observed["evidence_state"]["runtime_authority_replaced"] is False
    assert observed["research_supported"] is False
    assert observed["owner_risk_accepted"] is True
