from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_closeout_operational_metadata_v5 as subject
from scripts import live_remote_pointer


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _write_private(path: Path, payload: dict[str, Any]) -> bytes:
    raw = subject._render(payload)  # noqa: SLF001
    path.write_bytes(raw)
    path.chmod(0o600)
    return raw


def _canonical_payload(
    *, schema: str, status: str | None, canonical_field: str, extra: dict[str, Any] | None = None
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": schema,
        "status": status,
        **(extra or {}),
    }
    payload[canonical_field] = subject._document_sha(payload, canonical_field)  # noqa: SLF001
    return payload


def _binding(
    path: Path,
    payload: dict[str, Any],
    raw: bytes,
    canonical_field: str,
) -> dict[str, Any]:
    return {
        "path": str(path),
        "schema_version": payload["schema_version"],
        "status": payload.get("status"),
        "file_sha256": hashlib.sha256(raw).hexdigest(),
        "canonical_field": canonical_field,
        "canonical_sha256": payload[canonical_field],
        "size_bytes": len(raw),
        "mode": "0600",
    }


def _audit(*findings: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "narrowgate_private_evidence_audit_v2",
        "mode": "metadata-only",
        "findings": list(findings),
        "passed": not findings,
    }


def _fixture(tmp_path: Path, *, secret_in_catalog: bool = False) -> dict[str, Any]:
    root = tmp_path / "repo"
    private = root / "docs" / "private"
    private.mkdir(parents=True)
    pointer_path = private / "live_remote.current.local.json"
    catalog_path = private / "catalog.current.local.json"
    old_receipt_path = private / "activation_v4.json"
    new_receipt_path = private / "activation_v5.json"

    old_receipt = _canonical_payload(
        schema="narrowgate.live_replacement_activation_receipt.v1",
        status="completed_active_direct_v4_evidence_closed",
        canonical_field=subject.RECEIPT_CANONICAL_FIELD,
        extra={"receipt_id": "old-v4"},
    )
    old_receipt_raw = _write_private(old_receipt_path, old_receipt)
    old_receipt_binding = _binding(
        old_receipt_path,
        old_receipt,
        old_receipt_raw,
        subject.RECEIPT_CANONICAL_FIELD,
    )

    host = {
        "provider": "AWS",
        "region": "ap-northeast-1",
        "city": "Tokyo",
        "ssh_target": "fixture-current-host",
        "public_ipv4": "192.0.2.10",
        "instance_id": "i-fixture-current",
        "instance_type": "c7i-flex.large",
        "repo_root": "/srv/NarrowGate_BTCUSDC",
    }
    old_epoch = "prospective-1000000000000000000-oldfixture"
    pointer = {
        "schema_version": subject.POINTER_SCHEMA,
        "status": "current_active",
        **host,
        "activated_utc": "2026-08-24T03:31:01Z",
        "maker_started_utc": "2026-08-24T03:31:01Z",
        "runtime_identity_recorded_utc": "2026-08-24T03:31:00Z",
        "prospective_epoch_id": old_epoch,
        "prospective_epoch_started_ts_ns": 1_000_000_000_000_000_000,
        "prospective_epoch_identity_sha256": _digest("old-epoch"),
        "runtime_code_sha256": _digest("old-runtime"),
        "config_sha256": _digest("old-config"),
        "pointer_publication_status": "completed_active_direct_v4_evidence_closed",
        "current_process_id": 100,
        "current_process_start_ticks": 200,
        "current_activation_receipt": {
            "path": str(old_receipt_path),
            "sha256": old_receipt_binding["file_sha256"],
            "canonical_sha256": old_receipt_binding["canonical_sha256"],
            "bytes": old_receipt_binding["size_bytes"],
        },
        "current_buy_e3_release": {"status": "historical-v4"},
        "current_operational_evidence": {
            "proof_evidence_release": {"status": "historical-v4-proof"}
        },
        "current_evidence_health": {"snapshot_utc": "2026-08-24T05:02:05Z"},
        "host_epochs": [
            {
                "status": "current_active",
                "prospective_epoch_id": old_epoch,
                "config_sha256": _digest("old-config"),
            }
        ],
        "evidence_coverage_gaps": [],
        "current_query_policy": {
            "fill_trade_query_order": [
                "partition_request_by_instance_id_and_prospective_epoch_id_before_reading_rows",
                f"query_current_{old_epoch}",
                "preserve_historical_epochs",
            ]
        },
    }
    pointer_raw = _write_private(pointer_path, pointer)

    old_artifact_id = "repository-live-replacement-buy-e3-v4-fixture"
    new_artifact_id = "repository-live-replacement-buy-e3-v5-fixture"
    extra_entry: dict[str, Any] = {
        "artifact_id": "unrelated-private-artifact",
        "role": "fixture",
        "local_path": str(private / "unrelated.json"),
    }
    if secret_in_catalog:
        extra_entry["api_secret"] = "must-never-appear"
    catalog = {
        "schema_version": subject.CATALOG_SCHEMA,
        "generated_at_utc": "2026-08-24T05:13:53Z",
        "entries": [
            {
                "artifact_id": "repository-live-remote-current",
                "role": "current_live_remote_pointer",
                "local_path": str(pointer_path),
                "sha256": hashlib.sha256(pointer_raw).hexdigest(),
                "bytes": len(pointer_raw),
                "last_verified_utc": "2026-08-24T05:13:53Z",
                "notes": "historical fixture pointer",
            },
            {
                "artifact_id": old_artifact_id,
                "role": "historical_v4_activation",
                "local_path": str(old_receipt_path),
                "sha256": old_receipt_binding["file_sha256"],
                "bytes": old_receipt_binding["size_bytes"],
                "last_verified_utc": "2026-08-24T05:13:53Z",
                "notes": "historical v4 fixture",
            },
            extra_entry,
        ],
    }
    catalog_raw = _write_private(catalog_path, catalog)

    sources: dict[str, dict[str, Any]] = {}
    for role, (schema, status, canonical_field) in subject.SOURCE_IDENTITIES.items():
        schema_value = schema or "f05_buy_e3_external_venues_disabled_post_health.v1"
        status_value = (
            status
            if status is not None
            else (None if role == "lifecycle_admission" else "fresh_no_external_post_health_passed")
        )
        canonical_value = canonical_field or "canonical_post_health_sha256"
        sources[role] = {
            "path": str(tmp_path / "sources" / f"{role}.json"),
            "schema_version": schema_value,
            "status": status_value,
            "file_sha256": _digest(f"{role}-file"),
            "canonical_field": canonical_value,
            "canonical_sha256": _digest(f"{role}-canonical"),
            "size_bytes": 100 + len(role),
            "mode": "0600",
        }

    new_epoch = "prospective-2000000000000000000-newfixture"
    runtime = {
        "host": host,
        "process": {
            "pid": 300,
            "pid_start_ticks": 400,
            "process_identity_sha256": _digest("new-process"),
            "runtime_identity_recorded_utc": "2026-08-24T06:00:00Z",
            "active_capture_utc": "2026-08-24T06:01:00Z",
        },
        "epoch": {
            "epoch_id": new_epoch,
            "started_ts_ns": 2_000_000_000_000_000_000,
            "started_utc": "2026-08-24T06:00:01Z",
            "identity_sha256": _digest("new-epoch"),
            "predecessor_authority_end_utc": "2026-08-24T05:59:59Z",
        },
        "execution": dict(subject.EXECUTION),
        "release_remote_path": "/srv/NarrowGate_BTCUSDC/live/private/active_release.v2.json",
        "active_config_sha256": subject.ACTIVE_CONFIG_SHA256,
        "disabled_config_sha256": subject.DISABLED_CONFIG_SHA256,
        "runtime_code_sha256": _digest("new-runtime"),
        "artifact_sha256": subject.ARTIFACT_SHA256,
    }
    lifecycle = {
        "requested_duration_s": 3600,
        "observed_duration_s": 3600.25,
        "health_error_count": 0,
        "health_drop_count": 0,
        "stable_double_read_passed": True,
        "formal_collection_valid": True,
        "runtime_identity_sha256": _digest("lifecycle-runtime"),
        "runtime_code_identity_sha256": _digest("new-runtime"),
        "part_count": 100,
        "row_count": 100,
        "event_id_count": 100,
        "lifecycle_count": 25,
        "cursor_count": 25,
        "remote_payload_deleted": True,
    }
    post_health = {
        "observed_utc": "2026-08-24T07:02:00Z",
        "process_alive": True,
        "health_error_count": 0,
        "health_drop_count": 0,
        "external_venues_enabled": False,
        "buy_e3_shadow_enabled": False,
        "buy_e3_companion_enabled": False,
        "buy_e3_policy_enabled": True,
        "tracked_dirty_count": 0,
    }
    assertions = {
        "lifecycle_admission": {name: f"/fixture/{name}" for name in subject.LIFECYCLE_ASSERTIONS},
        "post_health": {name: f"/fixture/{name}" for name in subject.POST_HEALTH_ASSERTIONS},
    }
    manifest = {
        "schema_version": subject.MANIFEST_SCHEMA,
        "status": subject.MANIFEST_STATUS,
        "generated_utc": "2026-08-24T07:03:00Z",
        "receipt_id": "buy-e3-no-external-metadata-v5-fixture",
        "repository_root": str(root),
        "transaction": {
            "pointer_path": str(pointer_path),
            "catalog_path": str(catalog_path),
            "replacement_receipt_path": str(new_receipt_path),
            "predecessor_pointer": {
                "file_sha256": hashlib.sha256(pointer_raw).hexdigest(),
                "size_bytes": len(pointer_raw),
            },
            "predecessor_catalog": {
                "file_sha256": hashlib.sha256(catalog_raw).hexdigest(),
                "size_bytes": len(catalog_raw),
            },
            "predecessor_activation": old_receipt_binding,
            "predecessor_activation_artifact_id": old_artifact_id,
            "replacement_activation_artifact_id": new_artifact_id,
        },
        "runtime": runtime,
        "lifecycle": lifecycle,
        "post_health": post_health,
        "source_assertions": assertions,
        "sources": sources,
        "permissions": dict(subject.NO_NEW_AUTHORITY),
        "evidence_boundary": dict(subject.EVIDENCE_BOUNDARY),
    }
    manifest[subject.MANIFEST_CANONICAL_FIELD] = subject._document_sha(  # noqa: SLF001
        manifest, subject.MANIFEST_CANONICAL_FIELD
    )
    manifest_path = private / "operational_metadata_v5_manifest.json"
    _write_private(manifest_path, manifest)
    return {
        "root": root,
        "private": private,
        "manifest": manifest_path,
        "manifest_payload": manifest,
        "pointer": pointer_path,
        "pointer_before": pointer_raw,
        "catalog": catalog_path,
        "catalog_before": catalog_raw,
        "receipt": new_receipt_path,
        "bindings": sources,
        "old_artifact_id": old_artifact_id,
        "new_artifact_id": new_artifact_id,
    }


def _install_source_stub(monkeypatch: pytest.MonkeyPatch, fixture: dict[str, Any]) -> None:
    monkeypatch.setattr(
        subject,
        "_validate_sources",
        lambda _manifest: ({}, fixture["bindings"]),
    )


def test_dry_run_is_write_free_and_current_roles_are_exact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)

    result = subject.execute(
        fixture["manifest"],
        audit_fn=lambda _root: _audit({"kind": "preexisting", "path": "old"}),
    )

    assert result["mode"] == "dry_run"
    assert result["writes_performed"] is False
    assert fixture["pointer"].read_bytes() == fixture["pointer_before"]
    assert fixture["catalog"].read_bytes() == fixture["catalog_before"]
    assert not fixture["receipt"].exists()
    assert result["resolver_exact"] is True


def test_apply_is_resumable_after_receipt_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)

    def fail_after_receipt(step: str) -> None:
        if step == "receipt":
            raise RuntimeError("simulated crash")

    with pytest.raises(RuntimeError, match="simulated crash"):
        subject.execute(
            fixture["manifest"],
            apply=True,
            audit_fn=lambda _root: _audit(),
            failure_hook=fail_after_receipt,
        )
    assert fixture["receipt"].exists()
    assert fixture["receipt"].stat().st_nlink == 1
    assert fixture["pointer"].read_bytes() == fixture["pointer_before"]

    result = subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    assert result["post_write_verified"] is True
    assert result["state_after"]["catalog"] == "successor"

    repeated = subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    assert repeated["post_write_verified"] is True
    assert repeated["state_before"] == {
        "receipt": "published",
        "pointer": "successor",
        "catalog": "successor",
    }


def test_nlink2_receipt_crash_is_repaired_on_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)

    def stop(step: str) -> None:
        if step == "receipt":
            raise RuntimeError("stop")

    with pytest.raises(RuntimeError, match="stop"):
        subject.execute(
            fixture["manifest"],
            apply=True,
            audit_fn=lambda _root: _audit(),
            failure_hook=stop,
        )
    pending = subject._pending(fixture["receipt"], "create")  # noqa: SLF001
    os.link(fixture["receipt"], pending)
    assert fixture["receipt"].stat().st_nlink == 2

    result = subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    assert result["post_write_verified"] is True
    assert fixture["receipt"].stat().st_nlink == 1
    assert not pending.exists()


def test_pending_only_receipt_is_resumed_without_rebaselining_audit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)

    def stop(step: str) -> None:
        if step == "receipt":
            raise RuntimeError("stop")

    baseline = {"kind": "preexisting", "path": "before"}
    with pytest.raises(RuntimeError, match="stop"):
        subject.execute(
            fixture["manifest"],
            apply=True,
            audit_fn=lambda _root: _audit(baseline),
            failure_hook=stop,
        )
    pending = subject._pending(fixture["receipt"], "create")  # noqa: SLF001
    os.link(fixture["receipt"], pending)
    fixture["receipt"].unlink()
    assert pending.stat().st_nlink == 1

    calls = 0

    def audit_after(_root: Path) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return _audit(baseline)

    result = subject.execute(fixture["manifest"], apply=True, audit_fn=audit_after)
    assert result["state_before"]["receipt"] == "pending_create_only"
    assert result["metadata_audit"]["new_finding_count"] == 0
    assert calls == 1  # post-write only; immutable baseline came from the pending receipt
    assert fixture["receipt"].stat().st_nlink == 1
    assert not pending.exists()


def test_catalog_before_pointer_publication_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)
    subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    fixture["pointer"].write_bytes(fixture["pointer_before"])
    fixture["pointer"].chmod(0o600)

    with pytest.raises(subject.OperationalMetadataV5Error, match="catalog advanced before pointer"):
        subject.execute(fixture["manifest"], audit_fn=lambda _root: _audit())


def test_apply_resumes_after_pointer_before_catalog_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)

    def stop(step: str) -> None:
        if step == "pointer":
            raise RuntimeError("pointer published")

    with pytest.raises(RuntimeError, match="pointer published"):
        subject.execute(
            fixture["manifest"],
            apply=True,
            audit_fn=lambda _root: _audit(),
            failure_hook=stop,
        )
    assert fixture["receipt"].exists()
    assert fixture["pointer"].read_bytes() != fixture["pointer_before"]
    assert fixture["catalog"].read_bytes() == fixture["catalog_before"]

    result = subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    assert result["state_before"]["pointer"] == "successor"
    assert result["state_before"]["catalog"] == "predecessor"
    assert result["post_write_verified"] is True


def test_catalog_secret_scan_covers_unrelated_existing_entries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, secret_in_catalog=True)
    _install_source_stub(monkeypatch, fixture)

    with pytest.raises(subject.OperationalMetadataV5Error, match="secret-shaped catalog field"):
        subject.execute(fixture["manifest"], audit_fn=lambda _root: _audit())


def test_metadata_audit_allows_old_findings_but_rejects_new_ones() -> None:
    old = {"kind": "preexisting", "path": "old"}
    baseline = subject._audit_baseline(_audit(old))  # noqa: SLF001
    assert subject._assert_no_new_findings(baseline, _audit(old))["passed"] is True  # noqa: SLF001
    with pytest.raises(subject.OperationalMetadataV5Error, match="introduced 1 new"):
        subject._assert_no_new_findings(  # noqa: SLF001
            baseline,
            _audit(old, {"kind": "new", "path": "successor"}),
        )


def test_successor_preserves_v4_only_as_historical_and_binds_no_external_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)
    subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())

    pointer = json.loads(fixture["pointer"].read_text())
    assert set(pointer["current_operational_evidence"]) == {
        "config_correction",
        "resource_gate",
        "active_process_capture",
        "cross_host_admission",
        "lifecycle_admission",
        "post_health",
        "final_activation_envelope",
        "final_operational_completion",
        "final_composition",
        "final_attempt",
        "final_proof",
    }
    assert pointer["current_buy_e3_release"]["external_venues_enabled"] is False
    assert pointer["config_sha256"] == subject.ACTIVE_CONFIG_SHA256
    assert (
        pointer["historical_superseded_operational_evidence"][-1]["classification"]
        == subject.SUPERSEDED_REASON
    )
    old_epoch = pointer["host_epochs"][-2]
    assert old_epoch["superseded_reason"] == subject.SUPERSEDED_REASON

    monkeypatch.setenv("NARROWGATE_LIVE_REMOTE_POINTER", str(fixture["pointer"]))
    resolved = live_remote_pointer.active_live_remote_fields(fixture["root"])
    assert resolved == {
        key: fixture["manifest_payload"]["runtime"]["host"][key]
        for key in ("ssh_target", "provider", "region", "city", "public_ipv4", "repo_root")
    }


def test_manifest_refuses_short_lifecycle_before_any_source_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    manifest = json.loads(fixture["manifest"].read_text())
    manifest["lifecycle"]["observed_duration_s"] = 3499.9
    manifest[subject.MANIFEST_CANONICAL_FIELD] = subject._document_sha(  # noqa: SLF001
        manifest, subject.MANIFEST_CANONICAL_FIELD
    )
    _write_private(fixture["manifest"], manifest)
    monkeypatch.setattr(
        subject,
        "_validate_sources",
        lambda _manifest: pytest.fail("sources must not be read for an invalid manifest"),
    )

    with pytest.raises(subject.OperationalMetadataV5Error, match="valid bounded 3600s"):
        subject.execute(fixture["manifest"], audit_fn=lambda _root: _audit())


def test_real_source_validator_checks_recursive_no_external_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    manifest = json.loads(fixture["manifest"].read_text())
    source_root = tmp_path / "real-sources"
    source_root.mkdir()

    def publish(
        role: str,
        *,
        extra: dict[str, Any],
        schema: str | None = None,
        status: str | None | object = ...,
        canonical_field: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        default_schema, default_status, default_canonical = subject.SOURCE_IDENTITIES[role]
        chosen_schema = (
            schema or default_schema or "f05_buy_e3_external_venues_disabled_post_health.v1"
        )
        chosen_status = default_status if status is ... else status
        if role == "post_health" and status is ...:
            chosen_status = "fresh_no_external_post_health_passed"
        field = canonical_field or default_canonical or "canonical_post_health_sha256"
        payload = _canonical_payload(
            schema=chosen_schema,
            status=chosen_status,  # type: ignore[arg-type]
            canonical_field=field,
            extra=extra,
        )
        path = source_root / f"{role}.json"
        raw = _write_private(path, payload)
        return payload, _binding(path, payload, raw, field)

    release_payload, release_binding = publish(
        "direct_release", extra={"identity": "direct-v4-release-v2"}
    )
    del release_payload
    monkeypatch.setitem(subject.RELEASE, "file_sha256", release_binding["file_sha256"])
    monkeypatch.setitem(subject.RELEASE, "canonical_sha256", release_binding["canonical_sha256"])

    correction_payload, correction_binding = publish(
        "config_correction",
        extra={
            "corrected_config_pair": {
                "active_sha256": subject.ACTIVE_CONFIG_SHA256,
                "disabled_sha256": subject.DISABLED_CONFIG_SHA256,
                "external_venues_enabled": False,
            },
            "semantic_diff": {"external_network_shadow_disabled": True},
        },
    )
    del correction_payload
    resource_payload, resource_binding = publish(
        "resource_gate",
        extra={
            "runtime_execution": dict(subject.EXECUTION),
            "config_correction": subject._without_path(correction_binding),  # noqa: SLF001
            "checks": {"external_venues_disabled_throughout": True},
        },
    )
    del resource_payload
    runtime = manifest["runtime"]
    active_payload, active_binding = publish(
        "active_process_capture",
        extra={
            "runtime_authority": subject._without_path(release_binding),  # noqa: SLF001
            "resource_receipt": subject._without_path(resource_binding),  # noqa: SLF001
            "config_correction": subject._without_path(correction_binding),  # noqa: SLF001
            "active_process": {
                "pid": runtime["process"]["pid"],
                "pid_start_ticks": runtime["process"]["pid_start_ticks"],
                "config_sha256": subject.ACTIVE_CONFIG_SHA256,
                "artifact_sha256": subject.ARTIFACT_SHA256,
            },
            "checks": {
                "external_venues_disabled": True,
                "shadow_flags_disabled": True,
            },
        },
    )
    del active_payload
    cross_payload, cross_binding = publish(
        "cross_host_admission",
        extra={
            "portable_evidence": {
                "runtime_execution": dict(subject.EXECUTION),
                "runtime_authority": {
                    **subject._without_path(release_binding),  # noqa: SLF001
                    "execution": dict(subject.EXECUTION),
                    "runtime_authority": True,
                },
                "exact_artifact": {"artifact_sha256": subject.ARTIFACT_SHA256},
                "source_receipts": {
                    "config_correction": subject._without_path(correction_binding),  # noqa: SLF001
                    "current_host_resource_gate": subject._without_path(resource_binding),  # noqa: SLF001
                    "active_process_capture": subject._without_path(active_binding),  # noqa: SLF001
                },
            }
        },
    )
    del cross_payload

    lifecycle = manifest["lifecycle"]
    lifecycle_fixture = {
        "epoch_id": runtime["epoch"]["epoch_id"],
        "epoch_identity_sha256": runtime["epoch"]["identity_sha256"],
        "config_sha256": subject.ACTIVE_CONFIG_SHA256,
        "requested_duration_s": 3600,
        "observed_duration_s": lifecycle["observed_duration_s"],
        "health_error_count": 0,
        "health_drop_count": 0,
        "stable_double_read_passed": True,
        "formal_collection_valid": True,
    }
    lifecycle_payload, lifecycle_binding = publish(
        "lifecycle_admission",
        status=None,
        extra={"fixture": lifecycle_fixture},
    )
    del lifecycle_payload
    health_fixture = {
        "epoch_id": runtime["epoch"]["epoch_id"],
        "pid": runtime["process"]["pid"],
        "pid_start_ticks": runtime["process"]["pid_start_ticks"],
        "config_sha256": subject.ACTIVE_CONFIG_SHA256,
        "process_alive": True,
        "health_error_count": 0,
        "health_drop_count": 0,
        "external_venues_enabled": False,
        "buy_e3_shadow_enabled": False,
        "buy_e3_companion_enabled": False,
        "buy_e3_policy_enabled": True,
        "observed_utc": manifest["post_health"]["observed_utc"],
    }
    health_payload, health_binding = publish("post_health", extra={"fixture": health_fixture})
    del health_payload

    envelope_payload, envelope_binding = publish(
        "final_activation_envelope",
        extra={"cross_host": subject._without_path(cross_binding)},  # noqa: SLF001
    )
    del envelope_payload
    completion_payload, completion_binding = publish(
        "final_operational_completion",
        extra={
            "envelope": subject._without_path(envelope_binding),  # noqa: SLF001
            "lifecycle": subject._without_path(lifecycle_binding),  # noqa: SLF001
        },
    )
    del completion_payload
    composition_payload, composition_binding = publish(
        "final_composition",
        extra={
            "envelope": subject._without_path(envelope_binding),  # noqa: SLF001
            "completion": subject._without_path(completion_binding),  # noqa: SLF001
        },
    )
    del composition_payload
    attempt_payload, attempt_binding = publish(
        "final_attempt",
        extra={"composition": subject._without_path(composition_binding)},  # noqa: SLF001
    )
    del attempt_payload
    proof_payload, proof_binding = publish(
        "final_proof",
        extra={
            "runtime_execution": dict(subject.EXECUTION),
            "runtime_authority": {
                **subject._without_path(release_binding),  # noqa: SLF001
                "execution": dict(subject.EXECUTION),
                "runtime_authority": True,
            },
            "exact_artifact": {"artifact_sha256": subject.ARTIFACT_SHA256},
            "config_correction": subject._without_path(correction_binding),  # noqa: SLF001
            "operational_attempt_final": subject._without_path(attempt_binding),  # noqa: SLF001
            "research_supported": False,
            "owner_risk_accepted": True,
            "authority_provenance": {"new_authority_granted": False},
            "evidence_state": {"external_venues_disabled_active_config_exact": True},
        },
    )
    del proof_payload
    historical_payload, historical_binding = publish(
        "historical_v4_proof", extra={"historical_only": True}
    )
    del historical_payload

    real_bindings = {
        "direct_release": release_binding,
        "config_correction": correction_binding,
        "resource_gate": resource_binding,
        "active_process_capture": active_binding,
        "cross_host_admission": cross_binding,
        "lifecycle_admission": lifecycle_binding,
        "post_health": health_binding,
        "final_activation_envelope": envelope_binding,
        "final_operational_completion": completion_binding,
        "final_composition": composition_binding,
        "final_attempt": attempt_binding,
        "final_proof": proof_binding,
        "historical_v4_proof": historical_binding,
    }
    manifest["sources"] = real_bindings
    manifest[subject.MANIFEST_CANONICAL_FIELD] = subject._document_sha(  # noqa: SLF001
        manifest, subject.MANIFEST_CANONICAL_FIELD
    )
    _write_private(fixture["manifest"], manifest)

    validated_manifest, _manifest_binding = subject._validate_manifest(  # noqa: SLF001
        fixture["manifest"]
    )
    _payloads, observed_bindings = subject._validate_sources(validated_manifest)  # noqa: SLF001
    assert observed_bindings == real_bindings

    tampered = json.loads((source_root / "post_health.json").read_text())
    tampered["fixture"]["external_venues_enabled"] = True
    field = real_bindings["post_health"]["canonical_field"]
    tampered[field] = subject._document_sha(tampered, field)  # noqa: SLF001
    raw = _write_private(source_root / "post_health.json", tampered)
    validated_manifest["sources"]["post_health"] = _binding(
        source_root / "post_health.json", tampered, raw, field
    )
    with pytest.raises(subject.OperationalMetadataV5Error, match="post-health external_venues"):
        subject._validate_sources(validated_manifest)  # noqa: SLF001
