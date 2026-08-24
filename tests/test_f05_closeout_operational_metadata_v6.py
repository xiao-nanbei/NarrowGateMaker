from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_closeout_operational_metadata_v6 as subject


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _write_private(path: Path, payload: dict[str, Any]) -> bytes:
    raw = subject._render(payload)  # noqa: SLF001
    path.parent.mkdir(parents=True, exist_ok=True)
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
    path: Path, payload: dict[str, Any], raw: bytes, canonical_field: str
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
        "schema_version": subject.audit_private_evidence.AUDIT_SCHEMA,
        "mode": subject.audit_private_evidence.METADATA_ONLY,
        "deny_locked": True,
        "validation_read": False,
        "sealed_holdout_read": False,
        "findings": list(findings),
        "passed": not findings,
    }


def _publisher() -> dict[str, Any]:
    return {
        "module_route": subject.PUBLISHER_MODULE_ROUTE,
        "annotated_tag": subject.PUBLISHER_TAG,
        "annotated_tag_object": "1" * 40,
        "commit": "2" * 40,
        "tree": "3" * 40,
        "script_sha256": "4" * 64,
    }


def _source_context(publisher: dict[str, Any]) -> dict[str, Any]:
    rows = [
        {
            "row_index": 0,
            "fresh_generation": 1,
            "line_sha256": _digest("post-row-1"),
            "wall_timestamp_s": 1787578302.0,
            "boolean_cooldown_updates": 88_257,
            "completed_windows": 97_276,
            "runtime_loaded": True,
            "warmup_time_admitted": True,
            "gap_resets": 0,
            "resets": 0,
            "invalid_updates": 0,
            "external_sources": 0,
            "external_errors": 0,
            "global_flow_shadow_enabled": 0,
            "global_flow_state_error": 0,
            "global_reference_shadow_enabled": 0,
            "global_reference_state_error": 0,
        },
        {
            "row_index": 1,
            "fresh_generation": 2,
            "line_sha256": _digest("post-row-2"),
            "wall_timestamp_s": 1787578362.0,
            "boolean_cooldown_updates": 88_832,
            "completed_windows": 97_876,
            "runtime_loaded": True,
            "warmup_time_admitted": True,
            "gap_resets": 0,
            "resets": 0,
            "invalid_updates": 0,
            "external_sources": 0,
            "external_errors": 0,
            "global_flow_shadow_enabled": 0,
            "global_flow_state_error": 0,
            "global_reference_shadow_enabled": 0,
            "global_reference_state_error": 0,
        },
    ]
    health = {
        "snapshot_utc": subject.POST_LIFECYCLE_RECEIPT_GENERATED_UTC,
        "source_semantics": "durable_post_lifecycle_health_not_latest_heartbeat",
        "source_receipt": deepcopy(subject.FROZEN_SOURCES["post_lifecycle_health"]),
        "pid": subject.CURRENT_PID,
        "pid_start_ticks": subject.CURRENT_PID_START_TICKS,
        "live_maker_running_at_post_lifecycle_capture": True,
        "main_health_rows": rows,
        "completed_windows_and_updates_strictly_increase": True,
        "buy_e3_and_sell_owner_enabled_both_rows": True,
        "runtime_loaded_and_warmup_time_admitted_both_rows": True,
        "gap_resets_resets_invalid_absolute_zero": True,
        "config_sha256": subject.ACTIVE_CONFIG_SHA256,
        "execution_commit": subject.EXECUTION["execution_commit"],
        "execution_tree": subject.EXECUTION["execution_tree"],
        "external_sources_and_errors_absolute_zero": True,
        "global_flow_explicit_disabled_error_state_value_backend_absolute_zero": True,
        "global_reference_explicit_disabled_error_state_value_absolute_zero": True,
        "lifecycle_health_observed_utc": "2026-08-24T13:31:42Z",
        "lifecycle_error_count": 0,
        "lifecycle_drop_count": 0,
        "economic_outcomes_read": False,
        "economic_values_persisted": False,
        "latest_live_status_claimed": False,
    }
    return {
        "host_core": dict(subject.CURRENT_HOST_CORE),
        "process": {
            "pid": subject.CURRENT_PID,
            "pid_start_ticks": subject.CURRENT_PID_START_TICKS,
            "process_identity_sha256": _digest("process"),
            "stable_process_identity_sha256": _digest("stable-process"),
            "runtime_identity_recorded_utc": subject.ACTIVE_RUNTIME_IDENTITY_RECORDED_UTC,
            "active_capture_utc": subject.ACTIVE_CAPTURED_UTC,
            "post_lifecycle_health_utc": subject.POST_LIFECYCLE_RECEIPT_GENERATED_UTC,
            "active_release_remote_path": (
                "${NARROWGATE_REMOTE_ROOT}/live/private/"
                "f05_boolean_cooldown_owner_buy_e3_v1/active_release.direct_owner.v3.json"
            ),
        },
        "epoch": {
            "epoch_id": subject.CURRENT_EPOCH_ID,
            "started_ts_ns": 1_787_568_574_639_266_387,
            "started_utc": subject.CURRENT_EPOCH_STARTED_UTC,
            "identity_sha256": subject.FROZEN_SOURCES["lifecycle_admission"]["canonical_sha256"],
        },
        "lifecycle": {
            "admitted_ts_ns": 1_787_572_367_622_148_000,
            "runtime_source_file_count": 65,
            "runtime_source_files_canonical_sha256": (
                subject.lifecycle_context_v1.RUNTIME_SOURCE_FILES_CANONICAL_SHA256
            ),
            "runtime_code_sha256": subject.lifecycle_context_v1.RUNTIME_CODE_SHA256,
            "external_effective_stream_and_recording_disabled": True,
        },
        "post_lifecycle_health": {
            "generated_utc": subject.POST_LIFECYCLE_RECEIPT_GENERATED_UTC,
            "checks": deepcopy(subject.post_lifecycle_v1.CHECKS),
            "economic_values_persisted": False,
        },
        "pointer_health_snapshot": health,
        "publisher_source": dict(publisher),
    }


def _fixture(tmp_path: Path, *, secret_in_catalog: bool = False) -> dict[str, Any]:
    manifest_generated_utc = subject._nanosecond_utc(  # noqa: SLF001
        subject._now_utc_ns(),
        "fixture generation clock",  # noqa: SLF001
    )
    root = tmp_path / "repo"
    publisher_root = tmp_path / "publisher-checkout"
    publisher_root.mkdir(parents=True)
    private = root / "docs" / "private"
    private.mkdir(parents=True)
    private.chmod(0o700)
    pointer_path = private / "live_remote.current.local.json"
    catalog_path = private / "catalog.current.local.json"
    old_receipt_path = private / subject.PREDECESSOR_RECEIPT_FILENAME
    receipt_path = private / subject.RECEIPT_FILENAME

    old_receipt = _canonical_payload(
        schema=subject.PREDECESSOR_RECEIPT_SCHEMA,
        status=subject.PREDECESSOR_RECEIPT_STATUS,
        canonical_field=subject.RECEIPT_CANONICAL_FIELD,
        extra={"receipt_id": "old-v4"},
    )
    old_raw = _write_private(old_receipt_path, old_receipt)
    old_binding = _binding(old_receipt_path, old_receipt, old_raw, subject.RECEIPT_CANONICAL_FIELD)
    host = {
        "provider": "AWS",
        "region": subject.CURRENT_HOST_CORE["region"],
        "city": "Tokyo",
        "ssh_target": "<current-live-ssh-target>",
        "public_ipv4": subject.CURRENT_HOST_CORE["public_ipv4"],
        "instance_id": subject.CURRENT_HOST_CORE["instance_id"],
        "instance_type": subject.CURRENT_HOST_CORE["instance_type"],
        "repo_root": "${NARROWGATE_REMOTE_ROOT}",
    }
    old_epoch = "prospective-1787540000000000000-old-v4"
    pointer = {
        "schema_version": subject.POINTER_SCHEMA,
        "status": "current_active",
        **host,
        "activated_utc": "2026-08-24T03:31:01Z",
        "maker_started_utc": "2026-08-24T03:31:01Z",
        "runtime_identity_recorded_utc": "2026-08-24T03:31:00Z",
        "prospective_epoch_id": old_epoch,
        "prospective_epoch_started_ts_ns": 1_787_540_000_000_000_000,
        "prospective_epoch_identity_sha256": _digest("old-epoch"),
        "runtime_code_sha256": _digest("old-runtime"),
        "config_sha256": _digest("old-config"),
        "pointer_publication_status": subject.PREDECESSOR_RECEIPT_STATUS,
        "current_process_id": 100,
        "current_process_start_ticks": 200,
        "current_activation_receipt": {
            "path": str(old_receipt_path),
            "sha256": old_binding["file_sha256"],
            "canonical_sha256": old_binding["canonical_sha256"],
            "bytes": old_binding["size_bytes"],
        },
        "current_buy_e3_release": {"status": "historical-v4"},
        "current_operational_evidence": {"proof_evidence_release": {"historical": True}},
        "current_evidence_health": {"snapshot_utc": subject.PREDECESSOR_V4_LAST_EVIDENCED_UTC},
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
                f"query_current_{old_epoch.replace('-', '_')}",
                "preserve_older_historical_epochs",
            ],
            "same_instance_epoch_rule": "legacy_v3_rejected_and_v4_are_distinct",
        },
    }
    pointer_raw = _write_private(pointer_path, pointer)

    old_artifact_id = subject.PREDECESSOR_ARTIFACT_ID
    new_artifact_id = subject.REPLACEMENT_ARTIFACT_ID
    extra: dict[str, Any] = {
        "artifact_id": "unrelated-private-artifact",
        "role": "fixture",
        "local_path": str(private / "unrelated.json"),
    }
    if secret_in_catalog:
        extra["api_secret"] = "must-never-appear"
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
                "sha256": old_binding["file_sha256"],
                "bytes": old_binding["size_bytes"],
                "last_verified_utc": "2026-08-24T05:13:53Z",
                "notes": "historical v4 fixture",
            },
            extra,
        ],
    }
    catalog_raw = _write_private(catalog_path, catalog)
    publisher = _publisher()
    manifest = {
        "schema_version": subject.MANIFEST_SCHEMA,
        "status": subject.MANIFEST_STATUS,
        "generated_utc": manifest_generated_utc,
        "receipt_id": subject.FORMAL_RECEIPT_ID,
        "publisher_root": str(publisher_root),
        "metadata_repository_root": str(root),
        "publisher_source": publisher,
        "transaction": {
            "pointer_path": str(pointer_path),
            "catalog_path": str(catalog_path),
            "replacement_receipt_path": str(receipt_path),
            "predecessor_pointer": {
                "file_sha256": hashlib.sha256(pointer_raw).hexdigest(),
                "size_bytes": len(pointer_raw),
            },
            "predecessor_catalog": {
                "file_sha256": hashlib.sha256(catalog_raw).hexdigest(),
                "size_bytes": len(catalog_raw),
            },
            "predecessor_activation": old_binding,
            "predecessor_activation_artifact_id": old_artifact_id,
            "replacement_activation_artifact_id": new_artifact_id,
        },
        "validation_roots": {
            "current_runtime_root": str(tmp_path / "current-runtime"),
            "current_release_v3": subject.FROZEN_SOURCES["direct_release"]["path"],
            "historical_v4_root": str(tmp_path / "historical-runtime"),
            "historical_v4_release_v2": subject.FROZEN_SOURCES["historical_v4_release"]["path"],
        },
        "sources": deepcopy(subject.FROZEN_SOURCES),
        "permissions": dict(subject.NO_NEW_AUTHORITY),
        "evidence_boundary": dict(subject.EVIDENCE_BOUNDARY),
    }
    manifest[subject.MANIFEST_CANONICAL_FIELD] = subject._document_sha(  # noqa: SLF001
        manifest, subject.MANIFEST_CANONICAL_FIELD
    )
    manifest_path = tmp_path / "manifest.json"
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
        "old_receipt": old_receipt_path,
        "receipt": receipt_path,
        "bindings": deepcopy(subject.FROZEN_SOURCES),
        "source_context": _source_context(publisher),
        "old_epoch": old_epoch,
        "predecessor_contract": {
            "pointer": {
                "file_sha256": hashlib.sha256(pointer_raw).hexdigest(),
                "size_bytes": len(pointer_raw),
            },
            "catalog": {
                "file_sha256": hashlib.sha256(catalog_raw).hexdigest(),
                "size_bytes": len(catalog_raw),
            },
            "activation": {key: value for key, value in old_binding.items() if key != "path"},
        },
    }


def _install_source_stub(monkeypatch: pytest.MonkeyPatch, fixture: dict[str, Any]) -> None:
    monkeypatch.setattr(subject, "FROZEN_PREDECESSOR", fixture["predecessor_contract"])
    monkeypatch.setattr(subject, "FORMAL_MANIFEST_PATH", str(fixture["manifest"]))
    monkeypatch.setattr(
        subject,
        "_validate_sources",
        lambda _manifest: (deepcopy(fixture["source_context"]), deepcopy(fixture["bindings"])),
    )


def test_dry_run_is_write_free_and_roles_are_exact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)
    before_names = sorted(path.name for path in fixture["private"].iterdir())

    result = subject.execute(fixture["manifest"], audit_fn=lambda _root: _audit())

    assert result["mode"] == "dry_run"
    assert result["writes_performed"] is False
    assert fixture["pointer"].read_bytes() == fixture["pointer_before"]
    assert fixture["catalog"].read_bytes() == fixture["catalog_before"]
    assert sorted(path.name for path in fixture["private"].iterdir()) == before_names
    assert result["source_count"] == len(subject.FROZEN_SOURCES)


def test_apply_is_ordered_resumable_and_resolver_compatible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)

    def stop_after_receipt(step: str) -> None:
        if step == "receipt":
            raise RuntimeError("simulated crash")

    with pytest.raises(RuntimeError, match="simulated crash"):
        subject.execute(
            fixture["manifest"],
            apply=True,
            audit_fn=lambda _root: _audit(),
            failure_hook=stop_after_receipt,
        )
    assert fixture["receipt"].stat().st_nlink == 1
    assert fixture["pointer"].read_bytes() == fixture["pointer_before"]

    result = subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    assert result["post_write_verified"] is True
    assert result["transaction_committed"] is True
    assert result["post_audit_diagnostic_error_detected"] is False
    assert result["post_audit_drift_detected"] is False
    assert result["status"] == "completed_exact_transaction"
    assert result["active_live_remote_fields_compatible"] is True
    repeated = subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    assert repeated["state_before"] == {
        "receipt": "published",
        "pointer": "successor",
        "catalog": "successor",
    }


def test_pointer_is_honest_about_post_capture_and_transition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)
    subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    pointer = json.loads(fixture["pointer"].read_text())

    assert set(pointer["current_operational_evidence"]) == {
        "direct_release",
        "cross_host_admission",
        "config_correction",
        "resource_gate",
        "active_process_capture",
        "remote_active_attestation",
        "lifecycle_admission",
        "lifecycle_context",
        "post_lifecycle_health",
        "final_activation_envelope",
        "final_operational_completion",
        "final_composition",
        "final_attempt",
        "final_proof",
    }
    health = pointer["current_evidence_health"]
    assert health["snapshot_utc"] == subject.POST_LIFECYCLE_RECEIPT_GENERATED_UTC
    assert health["live_maker_running_at_post_lifecycle_capture"] is True
    assert health["latest_live_status_claimed"] is False
    assert health["economic_values_persisted"] is False
    assert "live_maker_running" not in health
    assert pointer["maker_started_utc"] is None
    assert pointer["runtime_identity_recorded_utc"] == subject.ACTIVE_RUNTIME_IDENTITY_RECORDED_UTC
    gap = pointer["evidence_coverage_gaps"][-1]
    assert gap["interval_semantics"] == "open_start_open_end_utc"
    assert gap["start_exclusive_utc"] == subject.PREDECESSOR_V4_LAST_EVIDENCED_UTC
    assert gap["end_exclusive_utc"] == subject.CURRENT_EPOCH_STARTED_UTC
    assert gap["downtime_claimed"] is False
    assert gap["v4_stopped_at_interval_start_claimed"] is False
    assert gap["v4_process_stop_claimed"] is False


def test_pointer_preserves_host_presentation_and_historical_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)
    subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    pointer = json.loads(fixture["pointer"].read_text())

    assert pointer["provider"] == "AWS"
    assert pointer["ssh_target"] == "<current-live-ssh-target>"
    old_row = next(
        row for row in pointer["host_epochs"] if row["prospective_epoch_id"] == fixture["old_epoch"]
    )
    assert old_row["verified_evidence_end_utc"] == subject.PREDECESSOR_V4_LAST_EVIDENCED_UTC
    assert old_row["exact_process_stop_claimed"] is False
    order = pointer["current_query_policy"]["fill_trade_query_order"]
    assert not any(fixture["old_epoch"].replace("-", "_") in row for row in order)
    assert any(
        f"only_through_last_verified_evidence_{subject.PREDECESSOR_V4_LAST_EVIDENCED_UTC}" in row
        for row in order
    )
    assert any(
        f"strictly_after_{subject.PREDECESSOR_V4_LAST_EVIDENCED_UTC}_and_strictly_before_"
        f"{subject.CURRENT_EPOCH_STARTED_UTC}" in row
        for row in order
    )
    assert pointer["current_query_policy"]["historical_same_instance_epoch_rule"] == (
        "legacy_v3_rejected_and_v4_are_distinct"
    )


def test_nlink2_receipt_is_repaired(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)

    def stop(step: str) -> None:
        if step == "receipt":
            raise RuntimeError("stop")

    with pytest.raises(RuntimeError, match="stop"):
        subject.execute(
            fixture["manifest"], apply=True, audit_fn=lambda _root: _audit(), failure_hook=stop
        )
    pending = subject._pending(fixture["receipt"], "create")  # noqa: SLF001
    os.link(fixture["receipt"], pending)
    result = subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    assert result["post_write_verified"] is True
    assert fixture["receipt"].stat().st_nlink == 1
    assert not pending.exists()


def test_pointer_before_catalog_resume(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)

    def stop(step: str) -> None:
        if step == "pointer":
            raise RuntimeError("pointer published")

    with pytest.raises(RuntimeError, match="pointer published"):
        subject.execute(
            fixture["manifest"], apply=True, audit_fn=lambda _root: _audit(), failure_hook=stop
        )
    result = subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    assert result["state_before"]["pointer"] == "successor"
    assert result["state_before"]["catalog"] == "predecessor"


def test_catalog_before_pointer_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)
    subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    fixture["pointer"].write_bytes(fixture["pointer_before"])
    fixture["pointer"].chmod(0o600)
    with pytest.raises(subject.OperationalMetadataV6Error, match="catalog advanced before pointer"):
        subject.execute(fixture["manifest"], audit_fn=lambda _root: _audit())


def test_secret_and_economic_fields_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret_fixture = _fixture(tmp_path / "secret", secret_in_catalog=True)
    _install_source_stub(monkeypatch, secret_fixture)
    with pytest.raises(subject.OperationalMetadataV6Error, match="secret-shaped"):
        subject.execute(secret_fixture["manifest"], audit_fn=lambda _root: _audit())
    for field in (
        "positionAmt",
        "clientOrderId",
        "avgPrice",
        "tradeId",
        "notional",
        "fee",
        "inventory",
        "markout",
    ):
        with pytest.raises(subject.OperationalMetadataV6Error, match="economic/raw field"):
            subject._reject_economic_fields({field: "forbidden"})  # noqa: SLF001
    for payload in (
        {"accessToken": "forbidden"},
        {"clientSecret": "forbidden"},
        {"awsSecretAccessKey": "forbidden"},
        {"secretKey": "forbidden"},
        {"consumerSecret": "forbidden"},
        {"apiToken": "forbidden"},
        {"authToken": "forbidden"},
        {"awsSessionToken": "forbidden"},
        {"privateKeyPem": "forbidden"},
        {"passphrase": "forbidden"},
        {"header": "Bearer abcdefghijklmnopqrstuvwxyz"},
        {"header": "eyJabcdefgh.abcdefghij.abcdefghij"},
        {"header": "ghp_abcdefghijklmnopqrstuvwxyz123456"},
        {"header": "xoxb-1234567890-abcdefghijklmnopqrst"},
    ):
        with pytest.raises(
            subject.OperationalMetadataV6Error,
            match="secret-shaped|credential-shaped",
        ):
            subject._reject_secrets(payload)  # noqa: SLF001


def test_audit_envelope_and_new_findings_fail_closed() -> None:
    old = {"kind": "preexisting", "path": "old"}
    baseline = subject._audit_baseline(_audit(old))  # noqa: SLF001
    assert subject._assert_no_new_findings(baseline, _audit(old))["passed"] is True  # noqa: SLF001
    with pytest.raises(subject.OperationalMetadataV6Error, match="introduced 1 new"):
        subject._assert_no_new_findings(  # noqa: SLF001
            baseline, _audit(old, {"kind": "new", "path": "successor"})
        )
    malformed = _audit(old)
    malformed["deny_locked"] = False
    with pytest.raises(subject.OperationalMetadataV6Error, match="audit envelope"):
        subject._audit_baseline(malformed)  # noqa: SLF001


@pytest.mark.parametrize(
    ("target", "value", "message"),
    [
        ("publisher_source.commit", "f" * 39, "Git SHA-1"),
        ("sources.active_process_capture.file_sha256", "0" * 64, "frozen exact7"),
        ("transaction.replacement_receipt_path", "/tmp/wrong.json", "exact private targets"),
    ],
)
def test_manifest_or_transaction_tamper_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    value: Any,
    message: str,
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)
    manifest = json.loads(fixture["manifest"].read_text())
    parent, leaf = target.split(".", 1)
    if "." in leaf:
        middle, final = leaf.split(".", 1)
        manifest[parent][middle][final] = value
    else:
        manifest[parent][leaf] = value
    manifest[subject.MANIFEST_CANONICAL_FIELD] = subject._document_sha(  # noqa: SLF001
        manifest, subject.MANIFEST_CANONICAL_FIELD
    )
    _write_private(fixture["manifest"], manifest)
    with pytest.raises(subject.OperationalMetadataV6Error, match=message):
        subject.execute(fixture["manifest"], audit_fn=lambda _root: _audit())


def test_catalog_predecessor_bindings_are_exact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)
    catalog = json.loads(fixture["catalog"].read_text())
    catalog["entries"][0]["sha256"] = _digest("wrong-pointer")
    raw = _write_private(fixture["catalog"], catalog)
    manifest = json.loads(fixture["manifest"].read_text())
    manifest["transaction"]["predecessor_catalog"] = {
        "file_sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
    }
    manifest[subject.MANIFEST_CANONICAL_FIELD] = subject._document_sha(  # noqa: SLF001
        manifest, subject.MANIFEST_CANONICAL_FIELD
    )
    _write_private(fixture["manifest"], manifest)
    with pytest.raises(
        subject.OperationalMetadataV6Error,
        match="predecessor pointer/catalog identity",
    ):
        subject.execute(fixture["manifest"], audit_fn=lambda _root: _audit())


def test_nanosecond_epoch_formatter_does_not_round() -> None:
    assert (
        subject._nanosecond_utc(1_787_568_574_639_266_387, "fixture")  # noqa: SLF001
        == "2026-08-24T10:49:34.639266387Z"
    )


def test_stat_identity_detects_mode_or_link_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "private.json"
    _write_private(path, {"safe": True})
    real_fstat = os.fstat
    calls = 0

    def changed_fstat(fd: int) -> os.stat_result:
        nonlocal calls
        calls += 1
        observed = real_fstat(fd)
        if calls == 2:
            path.chmod(0o644)
        return observed

    monkeypatch.setattr(subject.os, "fstat", changed_fstat)
    with pytest.raises(subject.OperationalMetadataV6Error, match="changed while reading"):
        subject._read_regular(path, mode=0o600)  # noqa: SLF001


def test_manifest_producer_is_create_only_recursive_and_exact_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    monkeypatch.setattr(subject, "FROZEN_PREDECESSOR", fixture["predecessor_contract"])
    monkeypatch.setattr(
        subject,
        "_observe_publisher_checkout",
        lambda _root: deepcopy(fixture["manifest_payload"]["publisher_source"]),
    )
    recursive_calls = 0

    def validate_sources(_manifest: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        nonlocal recursive_calls
        recursive_calls += 1
        return deepcopy(fixture["source_context"]), deepcopy(fixture["bindings"])

    monkeypatch.setattr(subject, "_validate_sources", validate_sources)
    evidence = tmp_path / "durable-evidence"
    evidence.mkdir(mode=0o700)
    formal = evidence / "operational_metadata" / "activation_manifest_v6.json"
    monkeypatch.setattr(subject, "FORMAL_MANIFEST_PATH", str(formal))
    generated_ns = subject._timestamp_ns(  # noqa: SLF001
        fixture["manifest_payload"]["generated_utc"], "fixture generation clock"
    )
    monkeypatch.setattr(subject, "_now_utc_ns", lambda: generated_ns)
    first = subject.prepare_activation_manifest(
        publisher_root=Path(fixture["manifest_payload"]["publisher_root"]),
        metadata_repository_root=fixture["root"],
        current_runtime_root=Path(
            fixture["manifest_payload"]["validation_roots"]["current_runtime_root"]
        ),
        historical_v4_root=Path(
            fixture["manifest_payload"]["validation_roots"]["historical_v4_root"]
        ),
        receipt_id=fixture["manifest_payload"]["receipt_id"],
        output_path=formal,
    )
    assert first["recursive_validation_passed"] is True
    assert formal.stat().st_mode & 0o777 == 0o600
    assert formal.stat().st_nlink == 1
    exact = formal.read_bytes()
    second = subject.prepare_activation_manifest(
        publisher_root=Path(fixture["manifest_payload"]["publisher_root"]),
        metadata_repository_root=fixture["root"],
        current_runtime_root=Path(
            fixture["manifest_payload"]["validation_roots"]["current_runtime_root"]
        ),
        historical_v4_root=Path(
            fixture["manifest_payload"]["validation_roots"]["historical_v4_root"]
        ),
        receipt_id=fixture["manifest_payload"]["receipt_id"],
        output_path=formal,
    )
    assert second["manifest"] == first["manifest"]
    assert formal.read_bytes() == exact
    assert recursive_calls == 4  # create pre/post validation, then existing pre/post validation

    payload = json.loads(formal.read_text())
    conflict = deepcopy(payload)
    conflict["generated_utc"] = subject._nanosecond_utc(  # noqa: SLF001
        generated_ns + 1, "conflicting generation clock"
    )
    conflict[subject.MANIFEST_CANONICAL_FIELD] = subject._document_sha(  # noqa: SLF001
        conflict, subject.MANIFEST_CANONICAL_FIELD
    )
    with pytest.raises(subject.OperationalMetadataV6Error, match="differ from plan"):
        subject.finalize_activation_manifest(conflict, output_path=formal)
    copied = tmp_path / "copied-manifest.json"
    copied.write_bytes(exact)
    copied.chmod(0o600)
    with pytest.raises(subject.OperationalMetadataV6Error, match="formal manifest path"):
        subject.validate_activation_manifest(copied, recursive=False)


def test_manifest_receipt_id_and_time_bounds_are_frozen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kwargs = {
        "publisher_root": tmp_path / "publisher",
        "metadata_repository_root": tmp_path / "metadata",
        "current_runtime_root": tmp_path / "runtime",
        "historical_v4_root": tmp_path / "historical",
    }
    now_ns = subject._timestamp_ns("2026-08-24T14:30:00Z", "test now")  # noqa: SLF001
    monkeypatch.setattr(subject, "_now_utc_ns", lambda: now_ns)
    with pytest.raises(subject.OperationalMetadataV6Error, match="frozen formal id"):
        subject.build_activation_manifest(
            **kwargs,
            generated_utc="2026-08-24T14:30:00Z",
            receipt_id="arbitrary-first-writer-id",
        )
    with pytest.raises(subject.OperationalMetadataV6Error, match="precedes"):
        subject.build_activation_manifest(
            **kwargs,
            generated_utc="2026-08-24T13:32:42.644562999Z",
            receipt_id=subject.FORMAL_RECEIPT_ID,
        )
    with pytest.raises(subject.OperationalMetadataV6Error, match="contemporaneous"):
        subject.build_activation_manifest(
            **kwargs,
            generated_utc="2026-08-24T14:29:54.999999999Z",
            receipt_id=subject.FORMAL_RECEIPT_ID,
        )
    with pytest.raises(subject.OperationalMetadataV6Error, match="future"):
        subject.build_activation_manifest(
            **kwargs,
            generated_utc="2026-08-24T14:30:05.000000001Z",
            receipt_id=subject.FORMAL_RECEIPT_ID,
        )


def test_prepare_manifest_cli_does_not_accept_operator_generated_time(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        subject._parser().parse_args(  # noqa: SLF001
            [
                "prepare-manifest",
                "--publisher-root",
                str(tmp_path / "publisher"),
                "--metadata-repository-root",
                str(tmp_path / "metadata"),
                "--current-runtime-root",
                str(tmp_path / "runtime"),
                "--historical-v4-root",
                str(tmp_path / "historical"),
                "--receipt-id",
                subject.FORMAL_RECEIPT_ID,
                "--generated-utc",
                "2026-08-24T14:30:00Z",
            ]
        )


def test_manifest_mtime_must_match_generated_utc(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)
    generated_ns = subject._timestamp_ns(  # noqa: SLF001
        fixture["manifest_payload"]["generated_utc"], "fixture generated_utc"
    )
    os.utime(fixture["manifest"], ns=(generated_ns + 6_000_000_000,) * 2)

    with pytest.raises(subject.OperationalMetadataV6Error, match="mtime"):
        subject.validate_activation_manifest(fixture["manifest"], recursive=False)


def test_manifest_slow_recursive_validation_refreshes_time_before_first_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    monkeypatch.setattr(subject, "FROZEN_PREDECESSOR", fixture["predecessor_contract"])
    monkeypatch.setattr(
        subject,
        "_observe_publisher_checkout",
        lambda _root: deepcopy(fixture["manifest_payload"]["publisher_source"]),
    )
    evidence = tmp_path / "durable-evidence"
    evidence.mkdir(mode=0o700)
    formal = evidence / "operational_metadata" / "activation_manifest_v6.json"
    monkeypatch.setattr(subject, "FORMAL_MANIFEST_PATH", str(formal))
    real_now_ns = subject._now_utc_ns()  # noqa: SLF001
    simulated = {"now_ns": real_now_ns - 10_000_000_000}
    monkeypatch.setattr(subject, "_now_utc_ns", lambda: simulated["now_ns"])
    calls = 0

    def slow_sources(_manifest: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        nonlocal calls
        calls += 1
        simulated["now_ns"] = real_now_ns
        return deepcopy(fixture["source_context"]), deepcopy(fixture["bindings"])

    monkeypatch.setattr(subject, "_validate_sources", slow_sources)
    result = subject.prepare_activation_manifest(
        publisher_root=Path(fixture["manifest_payload"]["publisher_root"]),
        metadata_repository_root=fixture["root"],
        current_runtime_root=Path(
            fixture["manifest_payload"]["validation_roots"]["current_runtime_root"]
        ),
        historical_v4_root=Path(
            fixture["manifest_payload"]["validation_roots"]["historical_v4_root"]
        ),
        receipt_id=subject.FORMAL_RECEIPT_ID,
        output_path=formal,
    )

    payload = json.loads(formal.read_text())
    assert calls == 2
    assert result["recursive_validation_passed"] is True
    assert payload["generated_utc"] == subject._nanosecond_utc(  # noqa: SLF001
        real_now_ns, "expected refreshed clock"
    )
    assert abs(formal.stat().st_mtime_ns - real_now_ns) <= 5_000_000_000


def test_manifest_delayed_directory_setup_precedes_final_time_freeze(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    monkeypatch.setattr(subject, "FROZEN_PREDECESSOR", fixture["predecessor_contract"])
    monkeypatch.setattr(
        subject,
        "_observe_publisher_checkout",
        lambda _root: deepcopy(fixture["manifest_payload"]["publisher_source"]),
    )
    monkeypatch.setattr(
        subject,
        "_validate_sources",
        lambda _manifest: (
            deepcopy(fixture["source_context"]),
            deepcopy(fixture["bindings"]),
        ),
    )
    evidence = tmp_path / "durable-evidence"
    evidence.mkdir(mode=0o700)
    formal = evidence / "operational_metadata" / "activation_manifest_v6.json"
    monkeypatch.setattr(subject, "FORMAL_MANIFEST_PATH", str(formal))
    real_now_ns = subject._now_utc_ns()  # noqa: SLF001
    simulated = {"now_ns": real_now_ns - 10_000_000_000}
    monkeypatch.setattr(subject, "_now_utc_ns", lambda: simulated["now_ns"])
    real_ensure = subject._ensure_private_directory  # noqa: SLF001

    def delayed_ensure(path: Path) -> None:
        real_ensure(path)
        simulated["now_ns"] = real_now_ns

    monkeypatch.setattr(subject, "_ensure_private_directory", delayed_ensure)
    result = subject.prepare_activation_manifest(
        publisher_root=Path(fixture["manifest_payload"]["publisher_root"]),
        metadata_repository_root=fixture["root"],
        current_runtime_root=Path(
            fixture["manifest_payload"]["validation_roots"]["current_runtime_root"]
        ),
        historical_v4_root=Path(
            fixture["manifest_payload"]["validation_roots"]["historical_v4_root"]
        ),
        receipt_id=subject.FORMAL_RECEIPT_ID,
        output_path=formal,
    )

    payload = json.loads(formal.read_text())
    assert result["recursive_validation_passed"] is True
    assert payload["generated_utc"] == subject._nanosecond_utc(  # noqa: SLF001
        real_now_ns, "expected post-directory-setup clock"
    )
    assert abs(formal.stat().st_mtime_ns - real_now_ns) <= 5_000_000_000


def test_manifest_recursive_failure_leaves_no_final_or_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    monkeypatch.setattr(subject, "FROZEN_PREDECESSOR", fixture["predecessor_contract"])
    monkeypatch.setattr(
        subject,
        "_observe_publisher_checkout",
        lambda _root: deepcopy(fixture["manifest_payload"]["publisher_source"]),
    )
    evidence = tmp_path / "durable-evidence"
    evidence.mkdir(mode=0o700)
    formal = evidence / "operational_metadata" / "activation_manifest_v6.json"
    monkeypatch.setattr(subject, "FORMAL_MANIFEST_PATH", str(formal))

    def fail_sources(_manifest: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        raise subject.OperationalMetadataV6Error("recursive source validation failed")

    monkeypatch.setattr(subject, "_validate_sources", fail_sources)
    with pytest.raises(subject.OperationalMetadataV6Error, match="recursive source"):
        subject.prepare_activation_manifest(
            publisher_root=Path(fixture["manifest_payload"]["publisher_root"]),
            metadata_repository_root=fixture["root"],
            current_runtime_root=Path(
                fixture["manifest_payload"]["validation_roots"]["current_runtime_root"]
            ),
            historical_v4_root=Path(
                fixture["manifest_payload"]["validation_roots"]["historical_v4_root"]
            ),
            receipt_id=subject.FORMAL_RECEIPT_ID,
            output_path=formal,
        )
    assert not formal.exists()
    assert not subject._pending(formal, "create").exists()  # noqa: SLF001


def test_manifest_publication_apis_reject_nonrecursive_bypass_before_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    evidence = tmp_path / "durable-evidence"
    evidence.mkdir(mode=0o700)
    formal = evidence / "operational_metadata" / "activation_manifest_v6.json"
    monkeypatch.setattr(subject, "FORMAL_MANIFEST_PATH", str(formal))
    with pytest.raises(subject.OperationalMetadataV6Error, match="requires recursive"):
        subject.prepare_activation_manifest(
            publisher_root=Path(fixture["manifest_payload"]["publisher_root"]),
            metadata_repository_root=fixture["root"],
            current_runtime_root=Path(
                fixture["manifest_payload"]["validation_roots"]["current_runtime_root"]
            ),
            historical_v4_root=Path(
                fixture["manifest_payload"]["validation_roots"]["historical_v4_root"]
            ),
            receipt_id=subject.FORMAL_RECEIPT_ID,
            output_path=formal,
            recursive=False,
        )
    with pytest.raises(subject.OperationalMetadataV6Error, match="requires recursive"):
        subject.finalize_activation_manifest(
            fixture["manifest_payload"],
            output_path=formal,
            recursive=False,
        )
    assert not formal.exists()
    assert not formal.parent.exists()
    assert not subject._pending(formal, "create").exists()  # noqa: SLF001


def test_manifest_predecessor_drift_during_slow_validation_blocks_first_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    monkeypatch.setattr(subject, "FROZEN_PREDECESSOR", fixture["predecessor_contract"])
    monkeypatch.setattr(
        subject,
        "_observe_publisher_checkout",
        lambda _root: deepcopy(fixture["manifest_payload"]["publisher_source"]),
    )
    evidence = tmp_path / "durable-evidence"
    evidence.mkdir(mode=0o700)
    formal = evidence / "operational_metadata" / "activation_manifest_v6.json"
    monkeypatch.setattr(subject, "FORMAL_MANIFEST_PATH", str(formal))

    def mutate_predecessor(_manifest: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        pointer = json.loads(fixture["pointer"].read_text())
        pointer["concurrent_drift"] = True
        _write_private(fixture["pointer"], pointer)
        return deepcopy(fixture["source_context"]), deepcopy(fixture["bindings"])

    monkeypatch.setattr(subject, "_validate_sources", mutate_predecessor)
    with pytest.raises(subject.OperationalMetadataV6Error, match="predecessor state drifted"):
        subject.prepare_activation_manifest(
            publisher_root=Path(fixture["manifest_payload"]["publisher_root"]),
            metadata_repository_root=fixture["root"],
            current_runtime_root=Path(
                fixture["manifest_payload"]["validation_roots"]["current_runtime_root"]
            ),
            historical_v4_root=Path(
                fixture["manifest_payload"]["validation_roots"]["historical_v4_root"]
            ),
            receipt_id=subject.FORMAL_RECEIPT_ID,
            output_path=formal,
        )
    assert not formal.exists()
    assert not formal.parent.exists()
    assert not subject._pending(formal, "create").exists()  # noqa: SLF001


def test_manifest_prepare_recovers_pending_and_hardlink_crash_points(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    monkeypatch.setattr(subject, "FROZEN_PREDECESSOR", fixture["predecessor_contract"])
    monkeypatch.setattr(
        subject,
        "_observe_publisher_checkout",
        lambda _root: deepcopy(fixture["manifest_payload"]["publisher_source"]),
    )
    monkeypatch.setattr(
        subject,
        "_validate_sources",
        lambda _manifest: (
            deepcopy(fixture["source_context"]),
            deepcopy(fixture["bindings"]),
        ),
    )
    evidence = tmp_path / "durable-evidence"
    evidence.mkdir(mode=0o700)
    formal = evidence / "operational_metadata" / "activation_manifest_v6.json"
    monkeypatch.setattr(subject, "FORMAL_MANIFEST_PATH", str(formal))
    kwargs = {
        "publisher_root": Path(fixture["manifest_payload"]["publisher_root"]),
        "metadata_repository_root": fixture["root"],
        "current_runtime_root": Path(
            fixture["manifest_payload"]["validation_roots"]["current_runtime_root"]
        ),
        "historical_v4_root": Path(
            fixture["manifest_payload"]["validation_roots"]["historical_v4_root"]
        ),
        "receipt_id": subject.FORMAL_RECEIPT_ID,
        "output_path": formal,
    }
    subject.prepare_activation_manifest(**kwargs)
    exact = formal.read_bytes()
    pending = subject._pending(formal, "create")  # noqa: SLF001

    os.link(formal, pending)
    formal.unlink()
    pending_recovery = subject.prepare_activation_manifest(**kwargs)
    assert pending_recovery["write_semantics"] == "create_only_pending_crash_recovered"
    assert formal.read_bytes() == exact
    assert formal.stat().st_nlink == 1
    assert not pending.exists()

    os.link(formal, pending)
    hardlink_recovery = subject.prepare_activation_manifest(**kwargs)
    assert hardlink_recovery["write_semantics"] == "create_only_hardlink_crash_recovered"
    assert formal.read_bytes() == exact
    assert formal.stat().st_nlink == 1
    assert not pending.exists()

    os.link(formal, pending)
    manifest_payload = json.loads(formal.read_text())
    pointer_exact = fixture["pointer"].read_bytes()
    pointer = json.loads(pointer_exact)
    pointer["concurrent_drift_before_finalize_repair"] = True
    _write_private(fixture["pointer"], pointer)
    with pytest.raises(subject.OperationalMetadataV6Error, match="predecessor state drifted"):
        subject.finalize_activation_manifest(manifest_payload, output_path=formal)
    assert formal.stat().st_nlink == 2
    assert pending.stat().st_nlink == 2
    pending.unlink()
    fixture["pointer"].write_bytes(pointer_exact)
    fixture["pointer"].chmod(0o600)

    pending.write_bytes(exact)
    pending.chmod(0o600)
    with pytest.raises(subject.OperationalMetadataV6Error, match="orphan formal manifest"):
        subject.prepare_activation_manifest(**kwargs)
    assert formal.read_bytes() == exact
    pending.unlink()

    formal.unlink()
    pending.write_text("{}\n")
    pending.chmod(0o600)
    with pytest.raises(subject.OperationalMetadataV6Error, match="identity|canonical|fields"):
        subject.prepare_activation_manifest(**kwargs)
    assert not formal.exists()
    assert pending.read_text() == "{}\n"


def test_fresh_prewrite_audit_blocks_every_official_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)
    calls = 0

    def changed_audit(_root: Path) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return _audit()
        return _audit({"kind": "new_before_write", "path": "unrelated"})

    with pytest.raises(subject.OperationalMetadataV6Error, match="introduced 1 new"):
        subject.execute(fixture["manifest"], apply=True, audit_fn=changed_audit)
    assert calls == 2
    assert not fixture["receipt"].exists()
    assert not subject._pending(fixture["receipt"], "create").exists()  # noqa: SLF001
    assert fixture["pointer"].read_bytes() == fixture["pointer_before"]
    assert fixture["catalog"].read_bytes() == fixture["catalog_before"]


def test_post_commit_audit_drift_is_reported_without_misreporting_rollback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)
    calls = 0
    drift = {"kind": "unattributed_after_commit", "path": "unrelated"}

    def changing_audit(_root: Path) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return _audit() if calls < 3 else _audit(drift)

    result = subject.execute(fixture["manifest"], apply=True, audit_fn=changing_audit)

    assert calls == 3
    assert result["transaction_committed"] is True
    assert result["post_write_verified"] is True
    assert result["post_audit_drift_detected"] is True
    assert result["post_audit_drift_attribution"] == "unattributed_after_commit"
    assert result["status"] == "committed_exact_transaction_with_unattributed_post_audit_drift"
    assert result["metadata_audit"]["new_findings"] == [drift]
    assert fixture["receipt"].exists()
    assert fixture["pointer"].read_bytes() != fixture["pointer_before"]
    assert fixture["catalog"].read_bytes() != fixture["catalog_before"]


@pytest.mark.parametrize("failure_kind", ["raise", "malformed"])
def test_post_commit_audit_error_is_nonrollback_diagnostic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_kind: str
) -> None:
    fixture = _fixture(tmp_path / failure_kind)
    _install_source_stub(monkeypatch, fixture)
    calls = 0

    def failing_after_commit(_root: Path) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if calls < 3:
            return _audit()
        if failure_kind == "raise":
            raise RuntimeError("post audit unavailable")
        malformed = _audit()
        malformed["deny_locked"] = False
        return malformed

    result = subject.execute(fixture["manifest"], apply=True, audit_fn=failing_after_commit)

    assert calls == 3
    assert result["transaction_committed"] is True
    assert result["post_write_verified"] is True
    assert result["post_audit_diagnostic_error_detected"] is True
    assert result["post_audit_drift_detected"] is None
    assert result["post_audit_drift_attribution"] == "unattributed_after_commit"
    assert result["status"] == "committed_exact_transaction_with_post_audit_diagnostic_error"
    assert result["metadata_audit"]["passed"] is None
    assert result["metadata_audit"]["diagnostic_error_type"] in {
        "RuntimeError",
        "OperationalMetadataV6Error",
    }
    assert fixture["receipt"].exists()


def _crash_after_receipt(
    fixture: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> tuple[dict[str, Any], bytes, dict[str, Any]]:
    _install_source_stub(monkeypatch, fixture)

    def stop(step: str) -> None:
        if step == "receipt":
            raise RuntimeError("receipt published")

    with pytest.raises(RuntimeError, match="receipt published"):
        subject.execute(
            fixture["manifest"], apply=True, audit_fn=lambda _root: _audit(), failure_hook=stop
        )
    receipt, receipt_raw, _metadata = subject._load_json(  # noqa: SLF001
        fixture["receipt"], mode=0o600
    )
    binding = subject._binding_from_bytes(  # noqa: SLF001
        fixture["receipt"], receipt, receipt_raw, subject.RECEIPT_CANONICAL_FIELD
    )
    return receipt, receipt_raw, binding


def test_receipt_pending_before_link_resumes_and_wrong_bytes_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path / "resume")
    _receipt, _raw, _binding_row = _crash_after_receipt(fixture, monkeypatch)
    pending = subject._pending(fixture["receipt"], "create")  # noqa: SLF001
    os.link(fixture["receipt"], pending)
    fixture["receipt"].unlink()
    result = subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    assert result["state_before"]["receipt"] == "pending_create_only"
    assert fixture["receipt"].stat().st_nlink == 1

    wrong = _fixture(tmp_path / "wrong")
    _install_source_stub(monkeypatch, wrong)
    wrong_pending = subject._pending(wrong["receipt"], "create")  # noqa: SLF001
    wrong_pending.write_text("{}\n")
    wrong_pending.chmod(0o600)
    with pytest.raises(subject.OperationalMetadataV6Error, match="published receipt|canonical"):
        subject.execute(wrong["manifest"], apply=True, audit_fn=lambda _root: _audit())
    assert not wrong["receipt"].exists()
    assert wrong["pointer"].read_bytes() == wrong["pointer_before"]


@pytest.mark.parametrize("apply", [False, True])
def test_published_receipt_with_independent_orphan_pending_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, apply: bool
) -> None:
    fixture = _fixture(tmp_path / str(apply))
    _install_source_stub(monkeypatch, fixture)
    subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    pending = subject._pending(fixture["receipt"], "create")  # noqa: SLF001
    pending.write_bytes(fixture["receipt"].read_bytes())
    pending.chmod(0o600)
    pointer_before = fixture["pointer"].read_bytes()
    catalog_before = fixture["catalog"].read_bytes()

    with pytest.raises(subject.OperationalMetadataV6Error, match="orphan receipt pending"):
        subject.execute(
            fixture["manifest"],
            apply=apply,
            audit_fn=lambda _root: _audit(),
        )
    assert fixture["pointer"].read_bytes() == pointer_before
    assert fixture["catalog"].read_bytes() == catalog_before
    assert pending.exists()


def test_pointer_and_catalog_pending_before_replace_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pointer_fixture = _fixture(tmp_path / "pointer")
    receipt, _receipt_raw, binding = _crash_after_receipt(pointer_fixture, monkeypatch)
    pointer_payload = subject._pointer_payload(receipt, binding)  # noqa: SLF001
    pointer_data = subject._render(pointer_payload)  # noqa: SLF001
    pointer_pending = subject._pending(pointer_fixture["pointer"], "pointer")  # noqa: SLF001
    subject._write_new(pointer_pending, pointer_data)  # noqa: SLF001
    result = subject.execute(
        pointer_fixture["manifest"], apply=True, audit_fn=lambda _root: _audit()
    )
    assert result["post_write_verified"] is True
    assert not pointer_pending.exists()

    catalog_fixture = _fixture(tmp_path / "catalog")
    receipt, _receipt_raw, binding = _crash_after_receipt(catalog_fixture, monkeypatch)
    pointer_payload = subject._pointer_payload(receipt, binding)  # noqa: SLF001
    pointer_data = subject._render(pointer_payload)  # noqa: SLF001
    catalog_payload = json.loads(catalog_fixture["catalog"].read_text())
    planned_catalog = subject._catalog_payload(  # noqa: SLF001
        catalog_payload, receipt, binding, pointer_data
    )
    catalog_data = subject._render(planned_catalog)  # noqa: SLF001

    def stop_pointer(step: str) -> None:
        if step == "pointer":
            raise RuntimeError("pointer published")

    with pytest.raises(RuntimeError, match="pointer published"):
        subject.execute(
            catalog_fixture["manifest"],
            apply=True,
            audit_fn=lambda _root: _audit(),
            failure_hook=stop_pointer,
        )
    catalog_pending = subject._pending(catalog_fixture["catalog"], "catalog")  # noqa: SLF001
    subject._write_new(catalog_pending, catalog_data)  # noqa: SLF001
    resumed = subject.execute(
        catalog_fixture["manifest"], apply=True, audit_fn=lambda _root: _audit()
    )
    assert resumed["post_write_verified"] is True
    assert not catalog_pending.exists()


def test_wrong_pointer_or_catalog_pending_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for kind in ("pointer", "catalog"):
        fixture = _fixture(tmp_path / kind)
        _receipt, _raw, _binding_row = _crash_after_receipt(fixture, monkeypatch)
        target = fixture[kind]
        pending = subject._pending(target, kind)  # noqa: SLF001
        pending.write_text("{}\n")
        pending.chmod(0o600)
        with pytest.raises(subject.OperationalMetadataV6Error, match="pending/published bytes"):
            subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
        assert target.read_bytes() == fixture[f"{kind}_before"]


def test_mixed_direct_and_transitive_module_origins_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(subject.__file__).resolve().parents[1]
    original_final_origin = subject.final_v6.__file__

    def fake_git(_root: Path, *args: str) -> str:
        joined = " ".join(args)
        if args[:2] == ("cat-file", "-t"):
            return "tag"
        if "status" in args:
            return ""
        if "^{tree}" in joined:
            return "3" * 40
        if "^{commit}" in joined or args == ("rev-parse", "HEAD"):
            return "2" * 40
        return "1" * 40

    monkeypatch.setattr(subject, "_git", fake_git)
    monkeypatch.setattr(subject.final_v6, "__file__", str(tmp_path / "mixed/final_v6.py"))
    with pytest.raises(subject.OperationalMetadataV6Error, match="module origin drifted"):
        subject._observe_publisher_checkout(root)  # noqa: SLF001
    monkeypatch.setattr(subject.final_v6, "__file__", original_final_origin)
    monkeypatch.setattr(
        subject.transport_v6.resource_v8,
        "__file__",
        str(tmp_path / "mixed/resource_v8.py"),
    )
    with pytest.raises(
        subject.OperationalMetadataV6Error,
        match="transitive validator module origin drifted",
    ):
        subject._observe_publisher_checkout(root)  # noqa: SLF001


def test_same_root_or_symlink_alias_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)
    alias = tmp_path / "metadata-alias"
    alias.symlink_to(fixture["root"], target_is_directory=True)
    manifest = json.loads(fixture["manifest"].read_text())
    manifest["publisher_root"] = str(alias)
    manifest[subject.MANIFEST_CANONICAL_FIELD] = subject._document_sha(  # noqa: SLF001
        manifest, subject.MANIFEST_CANONICAL_FIELD
    )
    _write_private(fixture["manifest"], manifest)
    with pytest.raises(subject.OperationalMetadataV6Error, match="roots must be distinct"):
        subject.execute(fixture["manifest"], audit_fn=lambda _root: _audit())


def test_two_root_sandbox_dry_run_and_apply_never_mutate_publisher(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    _install_source_stub(monkeypatch, fixture)
    publisher = Path(fixture["manifest_payload"]["publisher_root"])
    marker = publisher / "frozen-source-marker"
    marker.write_text("unchanged\n")
    before = (marker.read_bytes(), marker.stat().st_mtime_ns)

    dry = subject.execute(fixture["manifest"], audit_fn=lambda _root: _audit())
    assert dry["mode"] == "dry_run"
    assert (marker.read_bytes(), marker.stat().st_mtime_ns) == before
    applied = subject.execute(fixture["manifest"], apply=True, audit_fn=lambda _root: _audit())
    assert applied["post_write_verified"] is True
    assert (marker.read_bytes(), marker.stat().st_mtime_ns) == before
    assert fixture["receipt"].parent == fixture["root"] / "docs" / "private"


def test_actual_durable_sources_validate_with_only_unfrozen_publisher_stub(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata_root_value = os.environ.get("NARROWGATE_METADATA_REPOSITORY_ROOT")
    if not metadata_root_value:
        pytest.skip("NARROWGATE_METADATA_REPOSITORY_ROOT is not configured")
    checkout_root = Path(subject.EVIDENCE_ROOT) / "authority_sources" / "checkouts"
    runtime_root = checkout_root / "runtime_v3"
    historical_root = checkout_root / "historical_v4"
    metadata_root = Path(metadata_root_value)
    if not all(path.exists() for path in (runtime_root, historical_root, metadata_root)):
        pytest.skip("durable current/historical validation roots are unavailable")
    publisher_root = Path(subject.__file__).resolve().parents[1]
    publisher = _publisher()
    monkeypatch.setattr(
        subject,
        "_observe_publisher_checkout",
        lambda _root: deepcopy(publisher),
    )
    manifest = subject.build_activation_manifest(
        publisher_root=publisher_root,
        metadata_repository_root=metadata_root,
        current_runtime_root=runtime_root,
        historical_v4_root=historical_root,
        generated_utc=subject._nanosecond_utc(  # noqa: SLF001
            subject._now_utc_ns(),
            "actual durable source validation clock",  # noqa: SLF001
        ),
        receipt_id=subject.FORMAL_RECEIPT_ID,
    )
    monkeypatch.setattr(
        subject,
        "_publisher_checkout",
        lambda _root, expected: dict(expected),
    )

    context, bindings = subject._validate_sources(manifest)  # noqa: SLF001

    assert len(bindings) == 16
    assert context["epoch"]["started_utc"] == subject.CURRENT_EPOCH_STARTED_UTC
    assert (
        context["pointer_health_snapshot"]["snapshot_utc"]
        == subject.POST_LIFECYCLE_RECEIPT_GENERATED_UTC
    )
    assert context["pointer_health_snapshot"]["latest_live_status_claimed"] is False
