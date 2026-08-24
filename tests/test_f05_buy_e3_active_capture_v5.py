from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_buy_e3_active_capture_v5 as subject


def _content_binding(schema: str, status: str, canonical_field: str) -> dict[str, Any]:
    return {
        "schema_version": schema,
        "status": status,
        "file_sha256": "a" * 64,
        "canonical_field": canonical_field,
        "canonical_sha256": "b" * 64,
        "size_bytes": 123,
        "mode": "0600",
    }


def _release() -> dict[str, Any]:
    deployed = subject.resource_v5.EXACT_DEPLOYED_FILE_SHA256
    return {
        "exact_artifact": {
            "artifact_sha256": subject.EXACT_ARTIFACT_SHA256,
            "roles": {
                role: {"file_sha256": deployed[role]}
                for role in ("manifest", "policy", "predicate_bundle")
            },
        }
    }


def _release_binding() -> dict[str, Any]:
    binding = _content_binding(
        subject.DIRECT_V4_RELEASE_SCHEMA,
        subject.DIRECT_V4_RELEASE_STATUS,
        "canonical_active_release_sha256",
    )
    binding["file_sha256"] = subject.DIRECT_V4_RELEASE_FILE_SHA256
    binding["canonical_sha256"] = subject.DIRECT_V4_RELEASE_CANONICAL_SHA256
    return binding


def _resource_binding() -> dict[str, Any]:
    return _content_binding(
        subject.resource_v5.RESOURCE_SCHEMA,
        subject.resource_v5.RESOURCE_STATUS,
        subject.resource_v5.RESOURCE_CANONICAL_FIELD,
    )


def _config_correction_binding() -> dict[str, Any]:
    return _content_binding(
        subject.resource_v5.config_successor.SCHEMA_VERSION,
        subject.resource_v5.config_successor.STATUS,
        subject.resource_v5.config_successor.CANONICAL_FIELD,
    )


def _resource() -> dict[str, Any]:
    files = {
        role: {
            "role": role,
            "repository_relative_path": frozen["path"],
            "sha256": frozen["sha256"],
        }
        for role, frozen in subject.resource_v5.CURRENT_V4_RUNTIME_SOURCE_SHA256.items()
    }
    return {
        "config_correction": _config_correction_binding(),
        "host": {
            "instance_id": subject.resource_v5.CURRENT_INSTANCE_ID,
            "instance_type": "c7i-flex.large",
        },
        "fresh_disabled_process": {
            "disabled_pid": 101,
            "disabled_pid_start_ticks": 1_000,
            "disabled_process_identity_sha256": "c" * 64,
            "disabled_config_path": "/runtime/config.disabled.yaml",
            "disabled_config_sha256": subject.resource_v5.EXPECTED_DISABLED_CONFIG_SHA256,
            "fresh_pid": True,
            "fresh_start_ticks": True,
            "same_pid_pre_post": True,
        },
        "runtime_sources": {
            "direct_v4_execution_commit": subject.DIRECT_V4_EXECUTION_COMMIT,
            "files": files,
        },
    }


def _active_sources() -> dict[str, Any]:
    files = {
        role: {
            "role": role,
            "repository_relative_path": frozen["path"],
            "sha256": frozen["sha256"],
            "active_working_matches_direct_v4": True,
            "direct_v4_commit_blob_matches": True,
            "resource_v5_binding_matches": True,
        }
        for role, frozen in subject.resource_v5.CURRENT_V4_RUNTIME_SOURCE_SHA256.items()
    }
    return {
        "execution_commit": subject.DIRECT_V4_EXECUTION_COMMIT,
        "files": files,
        "runtime_source_manifest_sha256": subject.resource_v5.canonical_sha256(files),
        "buy_and_four_lifecycle_sources_exact": True,
    }


def _process(*, pid: int = 202, start_ticks: int = 2_000) -> dict[str, Any]:
    raw = {
        "schema_version": "owner.process.v1",
        "captured_utc": "2026-08-24T00:00:01Z",
        "pid": pid,
        "pid_start_ticks": start_ticks,
        "cmdline": [
            "/runtime/.venv/bin/python",
            "live/main.py",
            "--config",
            "/runtime/config.yaml",
        ],
        "cmdline_sha256": "d" * 64,
        "cwd": "/runtime",
        "config_path": "/runtime/config.yaml",
        "config_sha256": subject.ACTIVE_CONFIG_SHA256,
        "python_executable": "/runtime/.venv/bin/python",
        "python_binary_resolved": "/usr/bin/python3.12",
        "venv_root": "/runtime/.venv",
        "runtime_identity": {
            "present": True,
            "path": "/runtime/logs/runtime_identity.json",
            "file_sha256": "e" * 64,
            "schema_version": subject.RUNTIME_IDENTITY_SCHEMA,
        },
    }
    raw["canonical_process_identity_sha256"] = subject.resource_v5.document_sha256(
        raw, "canonical_process_identity_sha256"
    )
    return raw


def _runtime() -> dict[str, Any]:
    roles = _release()["exact_artifact"]["roles"]
    return {
        "schema_version": subject.RUNTIME_IDENTITY_SCHEMA,
        "pid": 202,
        "config_path": "/runtime/config.yaml",
        "config_sha256": subject.ACTIVE_CONFIG_SHA256,
        "buy_fill_selection_shadow_enabled": False,
        "dynamic_fill_hazard_shadow_enabled": False,
        "f05_buy_e3_enabled": True,
        "f05_buy_e3_owner_override_effective": True,
        "f05_buy_e3_required": True,
        "f05_buy_e3_artifact_sha256": subject.EXACT_ARTIFACT_SHA256,
        "f05_buy_e3_artifact_manifest_sha256": roles["manifest"]["file_sha256"],
        "f05_buy_e3_policy_sha256": roles["policy"]["file_sha256"],
        "f05_buy_e3_predicate_bundle_sha256": roles["predicate_bundle"]["file_sha256"],
        "f05_buy_e3_active_release_authority_schema_version": (
            subject.ACTIVE_RUNTIME_AUTHORITY_SCHEMA
        ),
        "f05_buy_e3_active_release_path": "/runtime/release.v2.json",
        "f05_buy_e3_active_release_file_sha256": subject.DIRECT_V4_RELEASE_FILE_SHA256,
        "f05_buy_e3_active_release_canonical_sha256": (subject.DIRECT_V4_RELEASE_CANONICAL_SHA256),
    }


def _startup() -> dict[str, Any]:
    return {
        "schema_version": subject.STARTUP_ATTESTATION_SCHEMA,
        "status": "accepted",
        "errors": [],
        "gates": {"safe_to_start_live_loops": True},
        "buy_e3_active_release": {
            "execution_commit": subject.DIRECT_V4_EXECUTION_COMMIT,
            "execution_tree": subject.DIRECT_V4_EXECUTION_TREE,
            "annotated_operational_tag": subject.DIRECT_V4_ANNOTATED_TAG,
            "annotated_operational_tag_object": subject.DIRECT_V4_TAG_OBJECT,
        },
        "fill_cooldown_state": {
            "restore_mode": "fresh_b0_no_checkpoint",
            "buy_deadline_identity": "B0",
            "buy_remaining_ms": 0,
            "e3_deadline_imported": False,
        },
    }


def _semantics() -> dict[str, Any]:
    startup = _startup()
    return {
        "startup_attestation_sha256": subject.resource_v5.canonical_sha256(startup),
        "startup_status": "accepted",
        "running_checkout_commit": subject.DIRECT_V4_EXECUTION_COMMIT,
        "running_checkout_tree": subject.DIRECT_V4_EXECUTION_TREE,
        "buy_deadline_identity": "B0",
        "fill_cooldown_restore_mode": "fresh_b0_no_checkpoint",
        "buy_remaining_ms": 0,
        "e3_deadline_imported": False,
    }


def _patch_build_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    pid: int = 202,
    start_ticks: int = 2_000,
    predecessor_quiescent: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], Path, Path]:
    runtime = _runtime()
    raw = json.dumps(runtime, sort_keys=True).encode("ascii")
    process = _process(pid=pid, start_ticks=start_ticks)
    process["runtime_identity"]["file_sha256"] = __import__("hashlib").sha256(raw).hexdigest()
    repository = tmp_path / "runtime"
    repository.mkdir()
    config = tmp_path / "config.yaml"
    config.write_text(
        "external_venues:\n  enabled: false\n  shadow_only: true\n",
        encoding="ascii",
    )
    release_path = tmp_path / "release.v2.json"
    release_path.write_text("{}\n", encoding="ascii")
    runtime_path = tmp_path / "runtime_identity.json"
    runtime_path.write_bytes(raw)
    os.chmod(runtime_path, 0o600)
    monkeypatch.setattr(subject, "_validate_runtime_repository", lambda _root: (repository, {}))
    monkeypatch.setattr(
        subject, "_validate_release", lambda _path: (_release(), _release_binding())
    )
    monkeypatch.setattr(
        subject,
        "_validate_resource",
        lambda _path, **_kwargs: (_resource(), _resource_binding()),
    )
    monkeypatch.setattr(
        subject,
        "_predecessor_is_quiescent",
        lambda _pid, proc_root: predecessor_quiescent,
    )
    monkeypatch.setattr(
        subject.resource_v5, "file_sha256", lambda _path: subject.ACTIVE_CONFIG_SHA256
    )
    monkeypatch.setattr(subject, "_read_pid", lambda _path: pid)
    monkeypatch.setattr(subject, "_capture_process", lambda **_kwargs: deepcopy(process))
    monkeypatch.setattr(
        subject,
        "_open_private_json",
        lambda path, _label: subject.OpenedJson(
            Path(path), deepcopy(runtime), raw, runtime_path.stat()
        ),
    )
    monkeypatch.setattr(
        subject, "_capture_active_runtime_sources", lambda *_args: _active_sources()
    )
    monkeypatch.setattr(subject, "_runtime_semantics", lambda *_args, **_kwargs: _semantics())
    return runtime, process, config, release_path


def _build(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, **kwargs: Any) -> dict[str, Any]:
    _runtime, _process_row, config, release_path = _patch_build_dependencies(
        monkeypatch, tmp_path, **kwargs
    )
    return subject.build_active_capture(
        runtime_repository_root=tmp_path,
        direct_release_path=release_path,
        resource_receipt_path=tmp_path / "resource.json",
        config_correction_path=tmp_path / "config-correction.json",
        pid_file=tmp_path / "maker.pid",
        config_path=config,
        python_executable=tmp_path / ".venv/bin/python",
        venv_root=tmp_path / ".venv",
        runtime_identity_path=tmp_path / "runtime_identity.json",
        proc_root=tmp_path,
        generated_utc="2026-08-24T00:00:02Z",
    )


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="ascii")
    os.chmod(path, 0o600)


def test_build_binds_content_only_v4_authority_and_all_runtime_sources(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payload = _build(monkeypatch, tmp_path)

    assert payload["schema_version"].endswith(".v3")
    assert set(payload["runtime_authority"]) == subject.CONTENT_BINDING_FIELDS
    assert set(payload["resource_receipt"]) == subject.CONTENT_BINDING_FIELDS
    assert payload["checks"] == subject.CHECKS
    assert payload["checks"]["external_venues_disabled"] is True
    assert {name for name, value in payload["checks"].items() if value is False} == {
        "retroactive_signature"
    }
    files = payload["active_process"]["runtime_source_files"]
    assert subject.REQUIRED_ACTIVE_SOURCE_ROLES.issubset(files)
    assert (
        files["buy_e3_runtime"]["sha256"]
        == (subject.resource_v5.CURRENT_V4_RUNTIME_SOURCE_SHA256["buy_e3_runtime"]["sha256"])
    )


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"pid": 101}, "reused"),
        ({"start_ticks": 999}, "did not start after"),
        ({"predecessor_quiescent": False}, "still running"),
    ],
)
def test_build_rejects_nonfresh_or_nonquiescent_transition(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    kwargs: dict[str, Any],
    match: str,
) -> None:
    with pytest.raises(subject.ActiveCaptureV5Error, match=match):
        _build(monkeypatch, tmp_path, **kwargs)


def test_runtime_semantics_accepts_only_exact_v4_active_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    startup = _startup()
    monkeypatch.setattr(
        subject.deploy,
        "_validate_runtime_identity_authority",
        lambda *_args, **_kwargs: deepcopy(startup),
    )

    observed = subject._runtime_semantics(
        _runtime(),
        process=_process(),
        release=_release(),
        release_binding=_release_binding(),
        expected_release_path="/runtime/release.v2.json",
    )

    assert observed["running_checkout_commit"] == subject.DIRECT_V4_EXECUTION_COMMIT
    assert observed["startup_status"] == "accepted"


@pytest.mark.parametrize(
    ("mutate_runtime", "mutate_startup", "match"),
    [
        (lambda row: row.__setitem__("config_sha256", "0" * 64), None, "drifted"),
        (
            lambda row: row.__setitem__("f05_buy_e3_active_release_file_sha256", "0" * 64),
            None,
            "drifted",
        ),
        (
            lambda row: row.__setitem__("buy_fill_selection_shadow_enabled", True),
            None,
            "drifted",
        ),
        (
            None,
            lambda row: row["buy_e3_active_release"].__setitem__(
                "annotated_operational_tag", "f05-owner-buy-e3-direct-live-v3-20260824"
            ),
            "drifted",
        ),
    ],
)
def test_runtime_semantics_rejects_config_release_shadow_or_v3(
    monkeypatch: pytest.MonkeyPatch,
    mutate_runtime: Any,
    mutate_startup: Any,
    match: str,
) -> None:
    runtime = _runtime()
    startup = _startup()
    if mutate_runtime is not None:
        mutate_runtime(runtime)
    if mutate_startup is not None:
        mutate_startup(startup)
    monkeypatch.setattr(
        subject.deploy,
        "_validate_runtime_identity_authority",
        lambda *_args, **_kwargs: startup,
    )

    with pytest.raises(subject.ActiveCaptureV5Error, match=match):
        subject._runtime_semantics(
            runtime,
            process=_process(),
            release=_release(),
            release_binding=_release_binding(),
            expected_release_path="/runtime/release.v2.json",
        )


def test_active_source_capture_rejects_lifecycle_source_tamper(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    role = "order_lifecycle_live_writer_v2"
    for frozen in subject.resource_v5.CURRENT_V4_RUNTIME_SOURCE_SHA256.values():
        path = tmp_path / frozen["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture\n", encoding="ascii")
    monkeypatch.setattr(
        subject.resource_v5,
        "file_sha256",
        lambda path: (
            "0" * 64
            if str(path).endswith("order_lifecycle_live_writer_v2.py")
            else next(
                frozen["sha256"]
                for frozen in subject.resource_v5.CURRENT_V4_RUNTIME_SOURCE_SHA256.values()
                if str(path).endswith(frozen["path"])
            )
        ),
    )
    monkeypatch.setattr(
        subject,
        "_git_blob_sha256",
        lambda _root, relative: next(
            frozen["sha256"]
            for frozen in subject.resource_v5.CURRENT_V4_RUNTIME_SOURCE_SHA256.values()
            if relative == frozen["path"]
        ),
    )

    with pytest.raises(subject.ActiveCaptureV5Error, match=role):
        subject._capture_active_runtime_sources(tmp_path, _resource())


def test_validate_rejects_tamper_even_with_recomputed_document_hash(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payload = _build(monkeypatch, tmp_path)
    receipt = tmp_path / "active.json"
    payload["active_process"]["config_sha256"] = "0" * 64
    process_body = dict(payload["active_process"])
    process_body.pop("canonical_process_identity_sha256")
    payload["active_process"]["canonical_process_identity_sha256"] = (
        subject.resource_v5.canonical_sha256(process_body)
    )
    payload[subject.CANONICAL_FIELD] = subject.resource_v5.document_sha256(
        payload, subject.CANONICAL_FIELD
    )
    _write(receipt, payload)
    monkeypatch.setattr(
        subject,
        "_open_private_json",
        lambda path, _label: subject.OpenedJson(
            Path(path),
            json.loads(Path(path).read_text(encoding="ascii")),
            Path(path).read_bytes(),
            Path(path).stat(),
        ),
    )

    with pytest.raises(subject.ActiveCaptureV5Error, match="transition or v4 source"):
        subject.validate_active_capture(
            receipt,
            runtime_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.v2.json",
            resource_receipt_path=tmp_path / "resource.json",
            config_correction_path=tmp_path / "config-correction.json",
        )


def test_create_only_writer_refuses_overwrite(tmp_path: Path) -> None:
    target = tmp_path / "receipt.json"
    target.write_text("reserved\n", encoding="ascii")

    with pytest.raises(subject.resource_v5.BuyE3CurrentHostResourceGateError):
        subject.resource_v5.atomic_write_receipt(target, {"status": "must_not_replace"})
