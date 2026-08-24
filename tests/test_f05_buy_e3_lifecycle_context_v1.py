from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_buy_e3_lifecycle_context_v1 as subject


@pytest.fixture(scope="module")
def exact_runtime_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    target = tmp_path_factory.mktemp("exact-eacb-runtime") / "repository"
    subprocess.run(
        ("git", "clone", "--shared", "--no-checkout", str(Path.cwd()), str(target)),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ("git", "checkout", "--detach", subject.EXECUTION_COMMIT),
        cwd=target,
        check=True,
        capture_output=True,
    )
    return target


def _formal_bundle() -> tuple[dict[str, Any], dict[str, Any]]:
    payload = {"admitted_ts_ns": 2_000_000_000}
    binding = {
        "schema_version": subject.lifecycle_io.LIFECYCLE_SCHEMA,
        "file_sha256": "1" * 64,
        "canonical_field": "admission_identity_sha256",
        "canonical_sha256": "2" * 64,
        "size_bytes": 1234,
        "mode": "0644",
        "session_id": "fixture-session",
        "baseline_epoch_id": "prospective-fixture-context",
        "config_sha256": subject.ACTIVE_CONFIG_SHA256,
        "runtime_code_sha256": subject.RUNTIME_CODE_SHA256,
        "runtime_code_files": dict(subject.EXPECTED_RUNTIME_SOURCE_SHA256),
        "action_enablement_sha256": "3" * 64,
        "epoch_start_ts_ns": 1_000_000_000,
        "writer_runtime_identity_sha256": "4" * 64,
        "writer_identity_file_sha256": "5" * 64,
        "epoch_manifest_file_sha256": "6" * 64,
        "identity_evidence_file_sha256": "7" * 64,
    }
    return payload, binding


def _build(monkeypatch: pytest.MonkeyPatch, runtime_root: Path) -> dict[str, Any]:
    payload, binding = _formal_bundle()
    monkeypatch.setattr(
        subject,
        "_formal_lifecycle_context",
        lambda _path: (
            payload,
            binding,
            dict(subject.SAFE_ACTION_STATE),
            "d" * 64,
            deepcopy(subject.SAFE_EXTERNAL_SOURCE_RECORDING_STATE),
        ),
    )
    return subject.build_lifecycle_context(
        lifecycle_admission_path=Path("/fixture/admission_manifest.json"),
        runtime_repository_root=runtime_root,
        generated_utc="2026-08-24T11:00:00Z",
    )


def _recanonicalize(payload: dict[str, Any]) -> None:
    payload[subject.CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload,
        subject.CANONICAL_FIELD,
    )


def _write_json(path: Path, payload: dict[str, Any], *, mode: int = 0o600) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(mode)
    return path


def _actual_formal_tree(root: Path) -> Path:
    session_id = "session-prospective-context-cli"
    epoch_id = "prospective-context-cli"
    admitted_ts_ns = time.time_ns() - 1_000_000_000
    start_ts_ns = admitted_ts_ns - 1_000_000_000
    runtime_code = {
        "schema_version": subject.RUNTIME_CODE_SCHEMA,
        "files": dict(subject.EXPECTED_RUNTIME_SOURCE_SHA256),
        "sha256": subject.RUNTIME_CODE_SHA256,
    }
    action_enablement = {
        "schema_version": "narrowgate_action_enablement_identity.v1",
        "fields": {
            **dict(subject.SAFE_ACTION_STATE),
        },
    }
    action_sha = subject._canonical_sha256(action_enablement)  # noqa: SLF001
    data_source_identity = {
        "schema_version": "narrowgate_live_data_source_identity.v1",
        "symbol": "BTCUSDC",
        "external_venues": {
            "enabled": False,
            "shadow_only": True,
            "sources": [
                {
                    "venue": row["venue"],
                    "instrument_type": row["instrument_type"],
                    "symbol": row["symbol"],
                    "role": row["role"],
                    "enabled": row["source_enabled"],
                    "record_enabled": row["record_enabled"],
                    "record_trades": row["record_trades"],
                }
                for row in subject.SAFE_EXTERNAL_SOURCE_RECORDING_STATE
            ],
        },
        "multi_market": {
            "enabled": True,
            "global_flow_shadow_enabled": False,
            "global_reference_shadow_enabled": False,
        },
        "websocket": {"deep_book_enabled": True},
    }
    data_source_sha = subject._canonical_sha256(data_source_identity)  # noqa: SLF001
    epoch_identity_sha = "e" * 64
    epoch_identity = {
        "config_sha256": subject.ACTIVE_CONFIG_SHA256,
        "runtime_code_sha256": subject.RUNTIME_CODE_SHA256,
        "action_enablement_sha256": action_sha,
        "data_source_identity_sha256": data_source_sha,
    }
    writer_runtime = {
        "baseline_epoch_id": epoch_id,
        "baseline_epoch_identity_sha256": epoch_identity_sha,
        **epoch_identity,
    }
    writer_runtime_sha = subject._canonical_sha256(writer_runtime)  # noqa: SLF001
    writer = {
        "runtime_identity": writer_runtime,
        "runtime_identity_sha256": writer_runtime_sha,
    }
    evidence = {
        "runtime_code": runtime_code,
        "action_enablement": action_enablement,
        "data_source_identity": data_source_identity,
        "config": {"path": "live/config.yaml", "sha256": subject.ACTIVE_CONFIG_SHA256},
    }
    epoch = {
        "epoch_id": epoch_id,
        "identity_sha256": epoch_identity_sha,
        "identity": epoch_identity,
        "identity_evidence": {
            "path": "identity_evidence.json",
            "canonical_sha256": subject._canonical_sha256(evidence),  # noqa: SLF001
        },
        "start_ts_ns": start_ts_ns,
    }
    validation = {
        "session_id": session_id,
        "baseline_epoch_id": epoch_id,
        "epoch_fully_bound": True,
        "event_id_count": 2,
        "row_count": 2,
        "part_count": 1,
        "lifecycle_count": 1,
        "cursor_count": 1,
        "file_count": 4,
        "payload_bytes": 1024,
        "health_drop_count": 0,
        "health_error_count": 0,
        "stable_double_read_passed": True,
        "storage_format": "parquet",
        "runtime_identity_sha256": writer_runtime_sha,
        "epoch_identity_sha256": epoch_identity_sha,
    }
    admission: dict[str, Any] = {
        "schema_version": subject.lifecycle_io.LIFECYCLE_SCHEMA,
        "admitted_ts_ns": admitted_ts_ns,
        "remote": "fixture-current-host",
        "remote_repo_root": "/fixture/runtime",
        "remote_allowlisted_root": "/fixture/runtime/live/private",
        "remote_session_root": "/fixture/session",
        "remote_epoch_root": "/fixture/epoch",
        "remote_seal_path": "/fixture/seal.json",
        "remote_seal_sha256": "1" * 64,
        "remote_seal_identity_sha256": "2" * 64,
        "single_rsync_files_from_session": True,
        "atomic_rename_admission": True,
        "remote_payload_deleted": False,
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_policy_authorized": False,
        "validation": validation,
    }
    admission["admission_identity_sha256"] = subject.lifecycle_io._document_sha256(  # noqa: SLF001
        admission,
        "admission_identity_sha256",
    )
    _write_json(
        root
        / "source"
        / "order_lifecycle_journal_v2"
        / f"session-{session_id}"
        / "runtime_identity.json",
        writer,
    )
    epoch_root = root / "source" / "prospective_baseline_epochs" / epoch_id
    _write_json(epoch_root / "epoch_manifest.json", epoch)
    _write_json(epoch_root / "identity_evidence.json", evidence)
    return _write_json(root / "admission_manifest.json", admission, mode=0o644)


def test_frozen_exact65_matches_working_and_eacb_bytes(exact_runtime_root: Path) -> None:
    observed = subject.validate_runtime_source_checkout(exact_runtime_root)
    assert observed == subject.EXPECTED_RUNTIME_SOURCE_SHA256
    assert len(observed) == 65
    assert subject._canonical_sha256(observed) == (  # noqa: SLF001
        subject.RUNTIME_SOURCE_FILES_CANONICAL_SHA256
    )
    assert (
        subject._canonical_sha256(  # noqa: SLF001
            {"schema_version": subject.RUNTIME_CODE_SCHEMA, "files": observed}
        )
        == subject.RUNTIME_CODE_SHA256
    )


def test_context_is_portable_nonauthoritative_exact65(
    monkeypatch: pytest.MonkeyPatch,
    exact_runtime_root: Path,
) -> None:
    payload = _build(monkeypatch, exact_runtime_root)
    assert payload["lifecycle_projection"]["runtime_source_files"] == (
        subject.EXPECTED_RUNTIME_SOURCE_SHA256
    )
    assert payload["lifecycle_projection"]["runtime_source_file_count"] == 65
    assert payload["lifecycle_projection"]["safe_action_state"] == subject.SAFE_ACTION_STATE
    assert payload["lifecycle_projection"]["external_shadow_only_inert"] is True
    assert (
        payload["lifecycle_projection"]["source_settings_inert_because_external_master_false"]
        is True
    )
    assert (
        payload["lifecycle_projection"][
            "record_trades_inert_because_master_false_and_record_enabled_false"
        ]
        is True
    )
    assert (
        payload["lifecycle_projection"]["external_effective_stream_and_recording_disabled"] is True
    )
    assert payload["lifecycle_projection"]["external_source_recording_state"] == (
        subject.SAFE_EXTERNAL_SOURCE_RECORDING_STATE
    )
    assert payload["lifecycle_admission"]["mode"] == "0644"
    assert payload["evidence_boundary"]["orico_admission_replaced"] is False
    assert payload["evidence_boundary"]["active_process_identity_read"] is False
    assert (
        payload["evidence_boundary"]["direct_lifecycle_admission_to_active_process_binding_claimed"]
        is False
    )
    assert "path" not in payload["lifecycle_admission"]


def test_context_rejects_generated_time_before_admission(
    monkeypatch: pytest.MonkeyPatch,
    exact_runtime_root: Path,
) -> None:
    payload = _build(monkeypatch, exact_runtime_root)
    payload["lifecycle_projection"]["admitted_ts_ns"] = 2_000_000_000_000_000_000
    _recanonicalize(payload)
    with pytest.raises(subject.LifecycleContextError, match="identity drifted"):
        subject.validate_content_projection(payload)


def test_runtime_checkout_rejects_dirty_and_symlink_components(
    exact_runtime_root: Path,
    tmp_path: Path,
) -> None:
    untracked = exact_runtime_root / "untracked-context-fixture"
    untracked.write_text("dirty\n", encoding="ascii")
    try:
        with pytest.raises(subject.LifecycleContextError, match="clean exact"):
            subject.validate_runtime_source_checkout(exact_runtime_root)
    finally:
        untracked.unlink()
    real = tmp_path / "real"
    real.mkdir()
    source = real / "source.py"
    source.write_text("pass\n", encoding="ascii")
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)
    with pytest.raises(subject.LifecycleContextError, match="symlink/non-directory ancestor"):
        subject._regular_file_bytes(linked / "source.py", "fixture source")  # noqa: SLF001


def test_runtime_checkout_rejects_wrong_head_or_tag_contract(
    monkeypatch: pytest.MonkeyPatch,
    exact_runtime_root: Path,
) -> None:
    real_git = subject._git  # noqa: SLF001

    def wrong_head(repository: Path, *args: str) -> bytes:
        if args == ("rev-parse", "HEAD"):
            return b"0" * 40 + b"\n"
        return real_git(repository, *args)

    monkeypatch.setattr(subject, "_git", wrong_head)
    with pytest.raises(subject.LifecycleContextError, match="clean exact"):
        subject.validate_runtime_source_checkout(exact_runtime_root)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("drop_source", "identity drifted"),
        ("source_tamper", "identity drifted"),
        ("map_hash", "identity drifted"),
        ("stored_aggregate", "identity drifted"),
        ("record_trades", "identity drifted"),
        ("inert_marker", "identity drifted"),
        ("authority", "identity drifted"),
        ("mode", "identity drifted"),
    ),
)
def test_context_rejects_subset_hash_tamper_and_authority(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
    exact_runtime_root: Path,
) -> None:
    payload = _build(monkeypatch, exact_runtime_root)
    projection = payload["lifecycle_projection"]
    if mutation == "drop_source":
        projection["runtime_source_files"].pop(next(iter(projection["runtime_source_files"])))
    elif mutation == "source_tamper":
        projection["runtime_source_files"][next(iter(projection["runtime_source_files"]))] = (
            "0" * 64
        )
    elif mutation == "map_hash":
        projection["runtime_source_files_canonical_sha256"] = "0" * 64
    elif mutation == "stored_aggregate":
        projection["runtime_code_sha256"] = "0" * 64
    elif mutation == "record_trades":
        projection["external_source_recording_state"][0]["record_trades"] = False
    elif mutation == "inert_marker":
        projection["record_trades_inert_because_master_false_and_record_enabled_false"] = False
    elif mutation == "authority":
        payload["evidence_boundary"]["lifecycle_authority_created"] = True
    else:
        payload["lifecycle_admission"]["mode"] = "0600"
    _recanonicalize(payload)
    with pytest.raises(subject.LifecycleContextError, match=message):
        subject.validate_content_projection(payload)


def test_finalize_is_create_only_private_and_validator_reopens_receipt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    exact_runtime_root: Path,
) -> None:
    formal_payload, formal_binding = _formal_bundle()
    monkeypatch.setattr(
        subject,
        "_formal_lifecycle_context",
        lambda _path: (
            formal_payload,
            formal_binding,
            dict(subject.SAFE_ACTION_STATE),
            "d" * 64,
            deepcopy(subject.SAFE_EXTERNAL_SOURCE_RECORDING_STATE),
        ),
    )
    output = tmp_path / "lifecycle_context.json"
    payload, file_sha = subject.finalize_lifecycle_context(
        output_path=output,
        lifecycle_admission_path=tmp_path / "admission_manifest.json",
        runtime_repository_root=exact_runtime_root,
        generated_utc="2026-08-24T11:00:00Z",
    )
    metadata = os.lstat(output)
    assert metadata.st_nlink == 1
    assert metadata.st_mode & 0o777 == 0o600
    assert len(file_sha) == 64
    assert (
        subject.validate_lifecycle_context(
            output,
            runtime_repository_root=exact_runtime_root,
        )
        == payload
    )
    with pytest.raises(subject.LifecycleContextError, match="create-only"):
        subject.finalize_lifecycle_context(
            output_path=output,
            lifecycle_admission_path=tmp_path / "admission_manifest.json",
            runtime_repository_root=exact_runtime_root,
            generated_utc="2026-08-24T11:00:00Z",
        )


def test_against_admission_requires_exact_projection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    exact_runtime_root: Path,
) -> None:
    formal_payload, formal_binding = _formal_bundle()
    monkeypatch.setattr(
        subject,
        "_formal_lifecycle_context",
        lambda _path: (
            formal_payload,
            formal_binding,
            dict(subject.SAFE_ACTION_STATE),
            "d" * 64,
            deepcopy(subject.SAFE_EXTERNAL_SOURCE_RECORDING_STATE),
        ),
    )
    output = tmp_path / "lifecycle_context.json"
    payload, _sha = subject.finalize_lifecycle_context(
        output_path=output,
        lifecycle_admission_path=tmp_path / "admission_manifest.json",
        runtime_repository_root=exact_runtime_root,
        generated_utc="2026-08-24T11:00:00Z",
    )
    assert (
        subject.validate_lifecycle_context_against_admission(
            output,
            lifecycle_admission_path=tmp_path / "admission_manifest.json",
            runtime_repository_root=exact_runtime_root,
        )
        == payload
    )
    drifted = deepcopy(formal_binding)
    drifted["identity_evidence_file_sha256"] = "0" * 64
    monkeypatch.setattr(
        subject,
        "_formal_lifecycle_context",
        lambda _path: (
            formal_payload,
            drifted,
            dict(subject.SAFE_ACTION_STATE),
            "d" * 64,
            deepcopy(subject.SAFE_EXTERNAL_SOURCE_RECORDING_STATE),
        ),
    )
    with pytest.raises(subject.LifecycleContextError, match="differs from ORICO"):
        subject.validate_lifecycle_context_against_admission(
            output,
            lifecycle_admission_path=tmp_path / "admission_manifest.json",
            runtime_repository_root=exact_runtime_root,
        )


def test_actual_cli_prepare_validate_and_compare_roundtrip(
    tmp_path: Path,
    exact_runtime_root: Path,
) -> None:
    admission = _actual_formal_tree(tmp_path / "formal")
    output = tmp_path / "portable-context.json"
    module = "scripts.f05_buy_e3_lifecycle_context_v1"
    prepare = subprocess.run(
        (
            sys.executable,
            "-m",
            module,
            "prepare-context",
            "--lifecycle-admission",
            str(admission),
            "--runtime-repository-root",
            str(exact_runtime_root),
            "--output",
            str(output),
        ),
        cwd=Path.cwd(),
        check=True,
        capture_output=True,
        text=True,
    )
    prepared = json.loads(prepare.stdout)
    metadata = os.lstat(output)
    assert metadata.st_nlink == 1
    assert metadata.st_mode & 0o777 == 0o600
    assert prepared["schema_version"] == subject.SCHEMA_VERSION
    common = (
        "--context",
        str(output),
        "--runtime-repository-root",
        str(exact_runtime_root),
    )
    validated = subprocess.run(
        (sys.executable, "-m", module, "validate-context", *common),
        cwd=Path.cwd(),
        check=True,
        capture_output=True,
        text=True,
    )
    compared = subprocess.run(
        (
            sys.executable,
            "-m",
            module,
            "validate-against-admission",
            *common,
            "--lifecycle-admission",
            str(admission),
        ),
        cwd=Path.cwd(),
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(validated.stdout)["canonical_sha256"] == prepared["canonical_sha256"]
    assert json.loads(compared.stdout)["canonical_sha256"] == prepared["canonical_sha256"]
    duplicate = subprocess.run(
        (
            sys.executable,
            "-m",
            module,
            "prepare-context",
            "--lifecycle-admission",
            str(admission),
            "--runtime-repository-root",
            str(exact_runtime_root),
            "--output",
            str(output),
        ),
        cwd=Path.cwd(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert duplicate.returncode != 0
    assert "create-only" in duplicate.stderr
