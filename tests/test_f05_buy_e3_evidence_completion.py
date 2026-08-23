from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_current_host_resource_gate_v3 as resource_v3,
)
from scripts import f05_buy_e3_evidence_completion as subject


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


def _self_hash(payload: dict, field: str) -> dict:
    result = dict(payload)
    result[field] = subject._document_sha256(result, field)  # noqa: SLF001
    return result


def _binding(path: Path, canonical: str = "a" * 64) -> dict:
    return {
        "path": str(path),
        "file_sha256": "b" * 64,
        "size_bytes": 1,
        "mode": "0600",
        "device": 1,
        "inode": 2,
        "schema_version": "fixture.v1",
        "status": "passed",
        "canonical_field": "canonical_receipt_sha256",
        "canonical_sha256": canonical,
    }


def _direct_release_payload() -> dict:
    role_files = {
        "manifest": "c64f8551268d0aaabab1a17bfc2f184cc576a2570cad3d0efb63fdcbc33c9929",
        "policy": "ba041dac4f082829f72e9f6838bc50b0c5dce61b24fcb5e1897ef2ac6c2c754b",
        "predicate_bundle": "4e127745fcc7987fb2eddc3bbf3ceaa19d64251c20ec156bb6d9b5d57edef915",
    }
    roles = {role: {"file_sha256": digest, "role": role} for role, digest in role_files.items()}
    return {
        "execution": subject._direct_execution(),  # noqa: SLF001
        "exact_artifact": {
            "artifact_sha256": subject.ARTIFACT_SHA256,
            "roles": roles,
        },
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
    }


def _direct_binding(path: Path) -> dict:
    row = _binding(path)
    row["runtime_authority"] = True
    return row


def _v5_payload() -> dict:
    payload = {
        "schema_version": subject.V5_SCHEMA,
        "status": subject.V5_STATUS,
        "python": "/venv/bin/python",
        "isolated": True,
        "safe_path": True,
        "cwd": "/private/tmp/isolated",
        "sys_path": ["/private/tmp/isolated"],
        "git_commit": "1" * 40,
        "git_tree": "2" * 40,
        "tracked_worktree_clean": True,
        "imports": {},
        "study_sha256": subject.V5_STUDY_SHA256,
        "model_bundle_census_sha256": subject.V5_MODEL_CENSUS_SHA256,
        "input_binding_sha256": subject.V5_INPUT_SHA256,
        "selected_day_count": 30,
        "output_root": "/private/tmp/exact",
        "economic_outcomes_read": False,
        "labels_read": False,
        "candidate_actions_generated": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "raw_sha256": dict(subject.V5_RAW_SHA256),
        "frame_sha256": dict(subject.V5_FRAME_SHA256),
        "envelope_sha256": dict(subject.V5_ENVELOPE_SHA256),
        "row_key_sha256": subject.V5_ROW_KEY_SHA256,
        "opportunity_count": 3516,
        "mechanics_manifest": subject.V5_MECHANICS_MANIFEST,
    }
    return _self_hash(payload, "canonical_receipt_sha256")


def test_exact_v5_accepts_only_isolated_outcome_blind_recovery(tmp_path: Path) -> None:
    path = _write(tmp_path / "v5.json", _v5_payload())
    payload, binding = subject._validate_v5_exact(path)  # noqa: SLF001
    assert payload["opportunity_count"] == 3516
    assert binding["selected_day_count"] == 30

    bad = _v5_payload()
    bad["labels_read"] = True
    bad = _self_hash(bad, "canonical_receipt_sha256")
    with pytest.raises(subject.EvidenceCompletionError, match="evidence boundary"):
        subject._validate_v5_exact(_write(tmp_path / "bad-v5.json", bad))  # noqa: SLF001

    wrong_bytes = _v5_payload()
    wrong_bytes["raw_sha256"] = dict(subject.V5_RAW_SHA256)
    wrong_bytes["raw_sha256"]["metadata"] = "0" * 64
    wrong_bytes = _self_hash(wrong_bytes, "canonical_receipt_sha256")
    with pytest.raises(subject.EvidenceCompletionError, match="aggregate raw_sha256"):
        subject._validate_v5_exact(  # noqa: SLF001
            _write(tmp_path / "wrong-v5-bytes.json", wrong_bytes)
        )


def _lifecycle_payload(runtime_sha: str = "4" * 64) -> dict:
    validation = {
        "session_id": "session-prospective-fixture",
        "baseline_epoch_id": "prospective-fixture",
        "epoch_fully_bound": True,
        "event_id_count": 1884,
        "row_count": 1884,
        "part_count": 1884,
        "lifecycle_count": 475,
        "cursor_count": 475,
        "file_count": 4248,
        "payload_bytes": 29396491,
        "health_drop_count": 0,
        "health_error_count": 0,
        "stable_double_read_passed": True,
        "storage_format": "parquet",
        "runtime_identity_sha256": runtime_sha,
        "epoch_identity_sha256": "e" * 64,
    }
    payload = {
        "schema_version": subject.LIFECYCLE_SCHEMA,
        "admitted_ts_ns": 1,
        "remote": "ec2-user@13.158.101.253",
        "remote_repo_root": "/home/ec2-user/NarrowGate_BTCUSDC",
        "remote_allowlisted_root": "/home/ec2-user/NarrowGate_BTCUSDC/live/private",
        "remote_session_root": "/remote/session",
        "remote_epoch_root": "/remote/epoch",
        "remote_seal_path": "/remote/seal.json",
        "remote_seal_sha256": "5" * 64,
        "remote_seal_identity_sha256": "6" * 64,
        "single_rsync_files_from_session": True,
        "atomic_rename_admission": True,
        "remote_payload_deleted": False,
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_policy_authorized": False,
        "validation": validation,
    }
    return _self_hash(payload, "admission_identity_sha256")


def _write_lifecycle_tree(root: Path, payload: dict) -> Path:
    session_id = payload["validation"]["session_id"]
    epoch_id = payload["validation"]["baseline_epoch_id"]
    runtime_files = {"live/main.py": "d" * 64, "strategy/maker_engine.py": "c" * 64}
    runtime_code = {
        "schema_version": "narrowgate_prospective_runtime_code_identity.v1",
        "files": runtime_files,
    }
    runtime_code["sha256"] = subject._canonical_sha256(runtime_code)  # noqa: SLF001
    action_enablement = {
        "schema_version": "narrowgate_action_enablement_identity.v1",
        "fields": {
            "strategy.buy_e3_cooldown_policy_enabled": True,
            "strategy.buy_fill_selection_live_enabled": False,
            "strategy.buy_fill_selection_shadow_enabled": False,
            "strategy.dynamic_fill_hazard_action_enabled": False,
            "strategy.dynamic_fill_hazard_shadow_enabled": False,
            "strategy.state_conditioned_policy_mode": "disabled",
            "logging.exact_opportunity_tape_enabled": False,
            "logging.inventory_campaign_shadow_enabled": False,
        },
    }
    action_sha = subject._canonical_sha256(action_enablement)  # noqa: SLF001
    identity = {
        "config_sha256": "7" * 64,
        "runtime_code_sha256": runtime_code["sha256"],
        "action_enablement_sha256": action_sha,
    }
    writer_runtime = {
        "baseline_epoch_id": epoch_id,
        "baseline_epoch_identity_sha256": "e" * 64,
        **identity,
    }
    runtime_sha = subject._canonical_sha256(writer_runtime)  # noqa: SLF001
    payload["validation"]["runtime_identity_sha256"] = runtime_sha
    payload = _self_hash(payload, "admission_identity_sha256")
    evidence = {
        "runtime_code": runtime_code,
        "action_enablement": action_enablement,
        "config": {"path": "/remote/config.yaml", "sha256": "7" * 64},
    }
    epoch = {
        "epoch_id": epoch_id,
        "identity_sha256": "e" * 64,
        "identity": identity,
        "identity_evidence": {
            "path": "identity_evidence.json",
            "canonical_sha256": subject._canonical_sha256(evidence),  # noqa: SLF001
        },
        "start_ts_ns": 123,
    }
    writer = {
        "runtime_identity": writer_runtime,
        "runtime_identity_sha256": runtime_sha,
    }
    _write(
        root
        / "source"
        / "order_lifecycle_journal_v2"
        / f"session-{session_id}"
        / "runtime_identity.json",
        writer,
    )
    epoch_root = root / "source" / "prospective_baseline_epochs" / epoch_id
    _write(epoch_root / "epoch_manifest.json", epoch)
    _write(epoch_root / "identity_evidence.json", evidence)
    return _write(root / "admission_manifest.json", payload)


def test_lifecycle_admission_rejects_drops_and_count_drift(tmp_path: Path) -> None:
    payload, binding = subject._validate_lifecycle_admission(  # noqa: SLF001
        _write_lifecycle_tree(tmp_path / "good", _lifecycle_payload())
    )
    assert payload["economic_outcomes_read"] is False
    assert binding["config_sha256"] == "7" * 64

    for field, value in (("health_drop_count", 1), ("row_count", 1883)):
        bad = _lifecycle_payload()
        bad["validation"][field] = value
        bad = _self_hash(bad, "admission_identity_sha256")
        with pytest.raises(subject.EvidenceCompletionError, match="validation failed"):
            subject._validate_lifecycle_admission(  # noqa: SLF001
                _write_lifecycle_tree(tmp_path / f"bad-{field}", bad)
            )


def _runtime_identity(release_binding: dict, release: dict, *, pid: int = 22) -> dict:
    startup_release = {
        "file_sha256": release_binding["file_sha256"],
        "file_canonical_sha256": release_binding["canonical_sha256"],
        "execution_commit": subject.DIRECT_COMMIT,
        "execution_tree": subject.DIRECT_TREE,
        "annotated_operational_tag": subject.DIRECT_TAG,
        "annotated_operational_tag_object": subject.DIRECT_TAG_OBJECT,
    }
    return {
        "schema_version": subject.RUNTIME_IDENTITY_SCHEMA,
        "pid": pid,
        "config_path": "/home/ec2-user/NarrowGate_BTCUSDC/live/config.yaml",
        "config_sha256": "7" * 64,
        "f05_buy_e3_enabled": True,
        "f05_buy_e3_owner_override_effective": True,
        "f05_buy_e3_artifact_sha256": subject.ARTIFACT_SHA256,
        "f05_buy_e3_artifact_manifest_sha256": release["exact_artifact"]["roles"]["manifest"][
            "file_sha256"
        ],
        "f05_buy_e3_policy_sha256": release["exact_artifact"]["roles"]["policy"]["file_sha256"],
        "f05_buy_e3_predicate_bundle_sha256": release["exact_artifact"]["roles"][
            "predicate_bundle"
        ]["file_sha256"],
        "f05_buy_e3_active_release_authority_schema_version": subject.ACTIVE_RUNTIME_AUTHORITY_SCHEMA,
        "f05_buy_e3_required": True,
        "f05_buy_e3_active_release_file_sha256": release_binding["file_sha256"],
        "f05_buy_e3_active_release_canonical_sha256": release_binding["canonical_sha256"],
        "startup_attestation": {
            "schema_version": subject.STARTUP_ATTESTATION_SCHEMA,
            "status": "accepted",
            "errors": [],
            "gates": {"safe_to_start_live_loops": True, "runtime_files_match_head": True},
            "running_checkout": {
                "git_commit": subject.DIRECT_COMMIT,
                "git_tree": subject.DIRECT_TREE,
                "git_worktree_clean": True,
            },
            "buy_e3_active_release": startup_release,
            "fill_cooldown_state": {
                "buy_deadline_identity": "B0",
                "restore_mode": "fresh_b0_no_checkpoint",
                "buy_remaining_ms": 0,
                "e3_deadline_imported": False,
            },
        },
    }


def test_active_runtime_semantics_are_exact_direct_v3() -> None:
    release = _direct_release_payload()
    release_binding = _direct_binding(Path("/private/direct.json"))
    runtime = _runtime_identity(release_binding, release)
    semantics = subject._active_runtime_semantics(  # noqa: SLF001
        runtime,
        active_pid=22,
        direct_release_payload=release,
        direct_release_binding=release_binding,
        expected_config_path=runtime["config_path"],
        expected_config_sha256=runtime["config_sha256"],
    )
    assert semantics["startup_status"] == "accepted"

    runtime["startup_attestation"]["gates"]["runtime_files_match_head"] = False
    with pytest.raises(subject.EvidenceCompletionError, match="attestation"):
        subject._active_runtime_semantics(  # noqa: SLF001
            runtime,
            active_pid=22,
            direct_release_payload=release,
            direct_release_binding=release_binding,
            expected_config_path=runtime["config_path"],
            expected_config_sha256=runtime["config_sha256"],
        )


def test_activation_envelope_requires_fresh_pid(monkeypatch, tmp_path: Path) -> None:
    release = _direct_release_payload()
    release_binding = _direct_binding(tmp_path / "direct.json")
    attempt = {
        "runtime_authority": release_binding,
        "exact_artifact": release["exact_artifact"],
        "canonical_operational_attempt_sha256": "1" * 64,
    }
    resource = {
        "fresh_disabled_process": {
            "pid": 10,
            "pid_start_ticks": 100,
            "canonical_process_identity_sha256": "f" * 64,
            "fresh_pid": True,
            "fresh_start_ticks": True,
            "same_pid_pre_post": True,
        },
        "host": {"instance_id": "i-fixture"},
    }
    active = {
        "disabled_predecessor": {"pid": 10, "pid_start_ticks": 100},
        "active_process": {"pid": 11, "pid_start_ticks": 200},
        "runtime_authority": release_binding,
        "runtime_identity_file_sha256": "8" * 64,
        "startup_semantics": {"startup_attestation_sha256": "9" * 64},
        "runtime_identity": {
            "config_sha256": "7" * 64,
            "startup_attestation": {
                "running_checkout": {
                    "runtime_source_manifest_sha256": "6" * 64,
                    "runtime_source_files": [
                        {
                            "path": "strategy/maker_engine.py",
                            "working_file_sha256": "5" * 64,
                            "matches_head_blob": True,
                        },
                        {
                            "path": "strategy/boolean_cooldown_buy_e3.py",
                            "working_file_sha256": "4" * 64,
                            "matches_head_blob": True,
                        },
                    ],
                }
            },
        },
    }
    monkeypatch.setattr(subject, "validate_operational_attempt", lambda *a, **k: attempt)
    monkeypatch.setattr(subject, "_direct_authority", lambda *a, **k: (release, release_binding))
    monkeypatch.setattr(
        subject, "_validate_resource", lambda *a, **k: (resource, _binding(tmp_path / "resource"))
    )
    monkeypatch.setattr(subject, "validate_active_process_capture", lambda *a, **k: active)
    monkeypatch.setattr(subject, "_receipt_binding", lambda path, **k: _binding(Path(path)))

    payload = subject.build_activation_envelope(
        operational_attempt_path=tmp_path / "attempt",
        active_capture_path=tmp_path / "active",
        resource_receipt_path=tmp_path / "resource",
        collector_repository_root=tmp_path,
        direct_repository_root=tmp_path,
        attempt4_repository_root=tmp_path,
    )
    assert payload["transition"]["fresh_active_restart"] is True
    active["active_process"]["pid"] = 10
    with pytest.raises(subject.EvidenceCompletionError, match="transition"):
        subject.build_activation_envelope(
            operational_attempt_path=tmp_path / "attempt",
            active_capture_path=tmp_path / "active",
            resource_receipt_path=tmp_path / "resource",
            collector_repository_root=tmp_path,
            direct_repository_root=tmp_path,
            attempt4_repository_root=tmp_path,
        )


def test_completion_cross_binds_lifecycle_to_active_runtime(monkeypatch, tmp_path: Path) -> None:
    runtime_sha = "a" * 64
    attempt = {
        "attempt_id": "operational-attempt-fixture",
        "canonical_operational_attempt_sha256": "1" * 64,
        "runtime_authority": _direct_binding(tmp_path / "direct"),
        "exact_artifact": _direct_release_payload()["exact_artifact"],
        "historical_attempt4_anchor": {},
        "exact_v5_recovery": {},
        "current_runtime_evidence": {},
    }
    envelope = {
        "operational_attempt": {"canonical_sha256": "1" * 64},
        "canonical_activation_envelope_sha256": "2" * 64,
        "active_runtime": {
            "runtime_identity_file_sha256": runtime_sha,
            "config_sha256": "7" * 64,
            "runtime_source_files": {
                "strategy/maker_engine.py": "5" * 64,
                "strategy/boolean_cooldown_buy_e3.py": "4" * 64,
            },
        },
        "resource_receipt": {},
    }
    lifecycle_binding = {
        **_binding(tmp_path / "lifecycle"),
        "config_sha256": "7" * 64,
        "runtime_code_files": {
            "strategy/maker_engine.py": "5" * 64,
            "strategy/boolean_cooldown_buy_e3.py": "4" * 64,
        },
    }
    monkeypatch.setattr(subject, "validate_operational_attempt", lambda *a, **k: attempt)
    monkeypatch.setattr(subject, "validate_activation_envelope", lambda *a, **k: envelope)
    monkeypatch.setattr(
        subject,
        "_validate_lifecycle_admission",
        lambda *a, **k: ({"fixture": True}, lifecycle_binding),
    )
    monkeypatch.setattr(subject, "_receipt_binding", lambda path, **k: _binding(Path(path)))
    payload = subject.build_operational_completion(
        operational_attempt_path=tmp_path / "attempt",
        activation_envelope_path=tmp_path / "envelope",
        lifecycle_admission_path=tmp_path / "lifecycle",
        collector_repository_root=tmp_path,
        direct_repository_root=tmp_path,
        attempt4_repository_root=tmp_path,
    )
    assert payload["post_release_evidence_completed"] is True
    lifecycle_binding["config_sha256"] = "b" * 64
    with pytest.raises(subject.EvidenceCompletionError, match="another active runtime"):
        subject.build_operational_completion(
            operational_attempt_path=tmp_path / "attempt",
            activation_envelope_path=tmp_path / "envelope",
            lifecycle_admission_path=tmp_path / "lifecycle",
            collector_repository_root=tmp_path,
            direct_repository_root=tmp_path,
            attempt4_repository_root=tmp_path,
        )


def test_evidence_release_is_never_runtime_consumed(monkeypatch, tmp_path: Path) -> None:
    direct = _direct_release_payload()
    direct_binding = _direct_binding(tmp_path / "direct")
    attempt_final = {
        "attempt_id": "operational-attempt-fixture",
        "runtime_authority": direct_binding,
        "composition_root_sha256": "c" * 64,
    }
    monkeypatch.setattr(subject, "validate_attempt_final", lambda *a, **k: attempt_final)
    monkeypatch.setattr(subject, "_receipt_binding", lambda path, **k: _binding(Path(path)))
    monkeypatch.setattr(subject, "_direct_authority", lambda *a, **k: (direct, direct_binding))
    payload = subject.build_evidence_complete_release(
        attempt_final_path=tmp_path / "attempt-final",
        collector_repository_root=tmp_path,
        direct_repository_root=tmp_path,
        attempt4_repository_root=tmp_path,
    )
    assert payload["research_supported"] is False
    assert payload["action_authorized"] is True
    assert payload["evidence_state"]["runtime_consumed"] is False
    assert payload["evidence_state"]["does_not_replace_runtime_active_release"] is True
    assert payload["authority_provenance"]["new_authority_granted"] is False


def test_boundary_has_no_shadow_companion_or_economic_reads() -> None:
    assert subject.EVIDENCE_BOUNDARY == {
        "economic_outcomes_read": False,
        "economic_values_persisted": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "new_economic_arm_run": False,
        "shadow_created": False,
        "companion_created": False,
        "hypothetical_live_actions_scored": False,
    }
    assert hashlib.sha256(json.dumps(subject.FOCUSED_NODEIDS).encode()).hexdigest()


def test_resource_v3_interface_and_disabled_process_shape_are_locked() -> None:
    assert resource_v3.RESOURCE_SCHEMA == subject.RESOURCE_SCHEMA
    assert resource_v3.RESOURCE_STATUS == subject.RESOURCE_STATUS
    assert resource_v3.RESOURCE_CANONICAL_FIELD == subject.RESOURCE_CANONICAL_FIELD
    process = subject._disabled_process(  # noqa: SLF001
        {
            "fresh_disabled_process": {
                "prior_process_identity_sha256": "1" * 64,
                "prior_pid": 8,
                "prior_pid_start_ticks": 80,
                "disabled_process_identity_sha256": "2" * 64,
                "disabled_pid": 9,
                "disabled_pid_start_ticks": 90,
                "disabled_config_path": "/remote/config.disabled.yaml",
                "disabled_config_sha256": "3" * 64,
                "fresh_pid": True,
                "fresh_start_ticks": True,
                "same_pid_pre_post": True,
            }
        }
    )
    assert process["pid"] == 9
    assert process["pid_start_ticks"] == 90
    assert process["canonical_process_identity_sha256"] == "2" * 64
