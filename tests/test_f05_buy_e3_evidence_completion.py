from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_current_host_resource_gate_v3 as resource_v3,
)
from scripts import f05_buy_e3_evidence_completion as subject


def test_lexical_python_entrypoint_preserves_venv_symlink(tmp_path: Path) -> None:
    entrypoint = tmp_path / ".venv/bin/python"
    entrypoint.parent.mkdir(parents=True)
    entrypoint.symlink_to(Path("/bin/sh"))

    observed = subject._lexical_python_executable(entrypoint)  # noqa: SLF001

    assert observed == entrypoint.absolute()
    assert observed != entrypoint.resolve()


def test_v5_mechanics_manifest_path_uses_frozen_v5_authority() -> None:
    assert (
        "f05_full_multiscale_offline_mechanics_v5/canonical_offline_v5"
        in subject.V5_MECHANICS_MANIFEST
    )


def test_sell54_validator_accepts_legacy_projection_shape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "sell54.json"
    path.write_text("{}\n", encoding="ascii")
    raw = {"status": "parity_complete"}
    binding = _binding(path, canonical="c" * 64)
    roles = {
        role: {"file_sha256": digest}
        for role, digest in {
            "manifest": "1" * 64,
            "policy": "2" * 64,
            "predicate_bundle": "3" * 64,
        }.items()
    }
    projection = {
        "path": binding["path"],
        "file_sha256": binding["file_sha256"],
        "canonical_receipt_sha256": binding["canonical_sha256"],
        "source_files": {"strategy/example.py": "4" * 64},
    }
    monkeypatch.setattr(subject, "_artifact_projection", lambda _payload: {"roles": roles})
    monkeypatch.setattr(
        subject.gate_v1,
        "validate_sell_owner_54_case_receipt",
        lambda *args, **kwargs: projection,
    )
    monkeypatch.setattr(subject, "_binding", lambda *args, **kwargs: (raw, binding))

    observed_raw, observed_binding = subject._validate_sell54(  # noqa: SLF001
        path,
        direct_repository_root=tmp_path,
        direct_release_payload={},
    )

    assert observed_raw == raw
    assert observed_binding == binding


def test_attempt4_anchor_requires_interpreter_equivalence_successor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_path = tmp_path / "attempt4-successor.json"
    manifest_path.write_text("{}\n", encoding="ascii")
    roles = subject.attempt4_successor.stability.REQUIRED_ROLES
    payload = {
        "runtime_execution": {
            "execution_commit": subject.ATTEMPT4_COMMIT,
            "annotated_tag": subject.ATTEMPT4_TAG,
        },
        "pre_admission_evidence": {
            role: {"canonical_sha256": f"{index + 1:064x}"} for index, role in enumerate(roles)
        },
        "interpreter_equivalence": {"canonical_sha256": "f" * 64},
    }
    binding = _binding(manifest_path)
    prior_portable_root = subject.data_paths.ROOT
    prior_collector_execution = subject.attempt4_successor._collector_execution  # noqa: SLF001
    monkeypatch.setattr(
        subject.attempt4_successor,
        "validate_manifest",
        lambda *args, **kwargs: payload,
    )
    portable_root = {
        "lexical_path": str(tmp_path),
        "realpath": str(tmp_path.resolve()),
    }
    monkeypatch.setattr(
        subject,
        "_historical_portable_root",
        lambda _path: (payload, tmp_path.resolve(), portable_root),
    )
    monkeypatch.setattr(subject, "_binding", lambda *args, **kwargs: (payload, binding))

    observed, observed_binding = subject._validate_attempt4_manifest(  # noqa: SLF001
        manifest_path, repository_root=tmp_path
    )

    assert observed == payload
    assert observed_binding["interpreter_equivalence_canonical_sha256"] == "f" * 64
    assert observed_binding["historical_portable_root"] == portable_root
    assert subject.data_paths.ROOT == prior_portable_root
    assert (
        subject.attempt4_successor._collector_execution  # noqa: SLF001
        is prior_collector_execution
    )


def test_attempt4_anchor_restores_scoped_globals_after_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_path = tmp_path / "attempt4-successor.json"
    manifest_path.write_text("{}\n", encoding="ascii")
    payload = {"fixture": True}
    portable_root = {
        "lexical_path": str(tmp_path),
        "realpath": str(tmp_path.resolve()),
    }
    prior_portable_root = subject.data_paths.ROOT
    prior_collector_execution = subject.attempt4_successor._collector_execution  # noqa: SLF001
    monkeypatch.setattr(
        subject,
        "_historical_portable_root",
        lambda _path: (payload, tmp_path.resolve(), portable_root),
    )

    def reject(*args: object, **kwargs: object) -> dict:
        assert subject.data_paths.ROOT == tmp_path.resolve()
        assert (
            subject.attempt4_successor._collector_execution  # noqa: SLF001
            is subject._historical_attempt4_collector_execution  # noqa: SLF001
        )
        raise RuntimeError("fixture rejection")

    monkeypatch.setattr(subject.attempt4_successor, "validate_manifest", reject)

    with pytest.raises(subject.EvidenceCompletionError, match="manifest is invalid"):
        subject._validate_attempt4_manifest(  # noqa: SLF001
            manifest_path, repository_root=tmp_path
        )
    assert subject.data_paths.ROOT == prior_portable_root
    assert (
        subject.attempt4_successor._collector_execution  # noqa: SLF001
        is prior_collector_execution
    )


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


def _file_binding(path: Path) -> dict:
    metadata = path.stat()
    return {
        "path": str(path),
        "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "size_bytes": metadata.st_size,
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
    }


def _portable_root_fixture(
    tmp_path: Path, *, interpreter_root: Path | None = None
) -> tuple[Path, Path, dict[str, Path]]:
    root = tmp_path / "historical-main"
    artifact_root = root / subject.HISTORICAL_ARTIFACT_ROOT
    artifacts = {
        "manifest": _write(artifact_root / "artifact_manifest.json", {"role": "manifest"}),
        "policy": _write(artifact_root / "policy.json", {"role": "policy"}),
        "predicate_bundle": _write(
            artifact_root / "predicate_bundle.json", {"role": "predicate_bundle"}
        ),
    }
    venv_owner = interpreter_root or root
    venv_root = venv_owner / ".venv"
    python = venv_root / "bin/python"
    equivalence = {
        "schema_version": subject.attempt4_successor.INTERPRETER_SCHEMA,
        "status": subject.attempt4_successor.INTERPRETER_STATUS,
        "venv_identity": {
            "venv_root": str(venv_root),
            "pyvenv_cfg_path": str(venv_root / "pyvenv.cfg"),
            "pyvenv_cfg_file_sha256": "1" * 64,
            "creation_command": ["python", "-m", "venv", str(venv_root)],
        },
        "lexical_provenance": {
            role: {
                "receipt_python_executable": str(python),
                "run_command_argv0": str(python),
                "probe": {
                    "sys_executable": str(python),
                    "sys_prefix": str(venv_root),
                    "exec_prefix": str(venv_root),
                },
            }
            for role in ("runtime_regression", "durability_regression_supplement")
        },
    }
    equivalence = _self_hash(equivalence, subject.attempt4_successor.INTERPRETER_CANONICAL_FIELD)
    equivalence_path = _write(tmp_path / "interpreter-equivalence.json", equivalence)
    _equivalence, equivalence_binding = subject._binding(  # noqa: SLF001
        equivalence_path,
        label="fixture equivalence",
        canonical_field=subject.attempt4_successor.INTERPRETER_CANONICAL_FIELD,
        expected_schema=subject.attempt4_successor.INTERPRETER_SCHEMA,
        expected_status=subject.attempt4_successor.INTERPRETER_STATUS,
    )
    manifest = {
        "schema_version": subject.attempt4_successor.MANIFEST_SCHEMA,
        "status": subject.attempt4_successor.MANIFEST_STATUS,
        "artifact": {"files": {role: _file_binding(path) for role, path in artifacts.items()}},
        "interpreter_equivalence": equivalence_binding,
    }
    manifest = _self_hash(manifest, "canonical_execution_attempt_sha256")
    return _write(tmp_path / "attempt4-successor.json", manifest), root, artifacts


def test_historical_portable_root_is_jointly_bound(tmp_path: Path) -> None:
    manifest, root, _artifacts = _portable_root_fixture(tmp_path)

    _payload, observed, binding = subject._historical_portable_root(manifest)  # noqa: SLF001

    assert observed == root.resolve()
    assert binding["lexical_path"] == str(root)
    assert binding["realpath"] == str(root.resolve())
    assert set(binding["artifact_files"]) == {"manifest", "policy", "predicate_bundle"}


def test_historical_portable_root_rejects_wrong_interpreter_root(tmp_path: Path) -> None:
    wrong = tmp_path / "wrong-main"
    wrong.mkdir()
    manifest, _root, _artifacts = _portable_root_fixture(tmp_path, interpreter_root=wrong)

    with pytest.raises(subject.EvidenceCompletionError, match="roots disagree"):
        subject._historical_portable_root(manifest)  # noqa: SLF001


def test_historical_portable_root_rejects_moved_or_tampered_artifact(
    tmp_path: Path,
) -> None:
    manifest, root, artifacts = _portable_root_fixture(tmp_path)
    artifacts["policy"].write_text('{"tampered":true}\n', encoding="ascii")
    artifacts["policy"].chmod(0o600)
    with pytest.raises(subject.EvidenceCompletionError, match="artifact binding drifted"):
        subject._historical_portable_root(manifest)  # noqa: SLF001

    manifest, root, _artifacts = _portable_root_fixture(tmp_path / "moved-case")
    root.rename(root.with_name("historical-main-moved"))
    with pytest.raises(subject.EvidenceCompletionError, match="not safely readable"):
        subject._historical_portable_root(manifest)  # noqa: SLF001


def _patch_historical_collector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    ancestor: bool = True,
    blob: str | None = None,
) -> None:
    source = _write(tmp_path / subject.ATTEMPT4_SUPPLEMENT_SUCCESSOR_PATH, {"fixture": True})
    legacy = subject.attempt4_successor.legacy_attempt
    expected = {
        "execution_commit": subject.ATTEMPT4_SUPPLEMENT_COLLECTOR_COMMIT,
        "execution_tree": subject.ATTEMPT4_SUPPLEMENT_COLLECTOR_TREE,
        "annotated_tag": subject.ATTEMPT4_SUPPLEMENT_COLLECTOR_TAG,
        "annotated_tag_object": subject.ATTEMPT4_SUPPLEMENT_COLLECTOR_TAG_OBJECT,
        "tag_peeled_commit": subject.ATTEMPT4_SUPPLEMENT_COLLECTOR_COMMIT,
    }
    monkeypatch.setattr(subject.attempt4_successor, "_COLLECTOR_ROOT", tmp_path)
    monkeypatch.setattr(subject.attempt4_successor, "__file__", str(source))
    monkeypatch.setattr(legacy, "_require_clean_worktree", lambda _root: None)
    monkeypatch.setattr(legacy, "_annotated_tag_identity", lambda *args, **kwargs: expected)
    monkeypatch.setattr(legacy, "_git_is_ancestor", lambda *args: ancestor)
    monkeypatch.setattr(
        legacy,
        "_git",
        lambda _root, *args: (
            "f" * 40
            if args == ("rev-parse", "HEAD")
            else blob or subject.ATTEMPT4_SUPPLEMENT_SUCCESSOR_BLOB
        ),
    )
    monkeypatch.setattr(
        subject,
        "_file_sha256",
        lambda _path: subject.ATTEMPT4_SUPPLEMENT_SUCCESSOR_FILE_SHA256,
    )


def test_historical_supplement_collector_rejects_tampered_tag() -> None:
    with pytest.raises(subject.EvidenceCompletionError, match="frozen v6 identity"):
        subject._historical_attempt4_collector_execution("tampered-tag")  # noqa: SLF001


def test_historical_supplement_collector_rejects_non_ancestor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_historical_collector(tmp_path, monkeypatch, ancestor=False)
    with pytest.raises(subject.EvidenceCompletionError, match="not a current collector ancestor"):
        subject._historical_attempt4_collector_execution(  # noqa: SLF001
            subject.ATTEMPT4_SUPPLEMENT_COLLECTOR_TAG
        )


def test_historical_supplement_collector_rejects_source_blob(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_historical_collector(tmp_path, monkeypatch, blob="0" * 40)
    with pytest.raises(subject.EvidenceCompletionError, match="successor source drifted"):
        subject._historical_attempt4_collector_execution(  # noqa: SLF001
            subject.ATTEMPT4_SUPPLEMENT_COLLECTOR_TAG
        )


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
        "cwd": "${NARROWGATE_EPHEMERAL_ROOT}/isolated",
        "sys_path": ["${NARROWGATE_EPHEMERAL_ROOT}/isolated"],
        "git_commit": "1" * 40,
        "git_tree": "2" * 40,
        "tracked_worktree_clean": True,
        "imports": {},
        "study_sha256": subject.V5_STUDY_SHA256,
        "model_bundle_census_sha256": subject.V5_MODEL_CENSUS_SHA256,
        "input_binding_sha256": subject.V5_INPUT_SHA256,
        "selected_day_count": 30,
        "output_root": "${NARROWGATE_EPHEMERAL_ROOT}/exact",
        "economic_outcomes_read": False,
        "labels_read": False,
        "candidate_actions_generated": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "raw_sha256": dict(subject.V5_RAW_SHA256),
        "frame_sha256": dict(subject.V5_FRAME_SHA256),
        "envelope_sha256": dict(subject.V5_ENVELOPE_SHA256),
        "row_key_sha256": subject.V5_ROW_KEY_SHA256,
        "replay_frame_binding": dict(subject.V5_REPLAY_FRAME_BINDING),
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

    wrong_replay = _v5_payload()
    wrong_replay["replay_frame_binding"] = dict(subject.V5_REPLAY_FRAME_BINDING)
    wrong_replay["replay_frame_binding"]["file_sha256"] = "0" * 64
    wrong_replay = _self_hash(wrong_replay, "canonical_receipt_sha256")
    with pytest.raises(subject.EvidenceCompletionError, match="replay-frame binding"):
        subject._validate_v5_exact(  # noqa: SLF001
            _write(tmp_path / "wrong-v5-replay-frame.json", wrong_replay)
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
        "remote": "<current-live-ssh-target>",
        "remote_repo_root": "${NARROWGATE_REMOTE_ROOT}",
        "remote_allowlisted_root": "${NARROWGATE_REMOTE_ROOT}/live/private",
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
        "config_path": "/srv/narrowgate-test/live/config.yaml",
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
