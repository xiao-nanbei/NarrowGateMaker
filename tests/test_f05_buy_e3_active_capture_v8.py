from __future__ import annotations

import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_buy_e3_active_capture_v8 as subject


def test_startup_source_plan_binds_every_no_shadow_evaluator_module() -> None:
    plan = subject._startup_source_plan()  # noqa: SLF001
    assert set(plan["files"]) == set(subject.deploy._CURRENT_RUNTIME_SOURCE_PATHS)  # noqa: SLF001
    assert {
        "signal_engine",
        "global_flow",
        "global_reference",
        "sell_owner_runtime",
        "live_buy_runtime",
    }.issubset(plan["files"])
    for startup_role, resource_role in subject.STARTUP_SOURCE_ROLE_MAP.items():
        row = plan["files"][startup_role]
        assert set(row) == {
            "repository_relative_path",
            "execution_commit_blob_sha256",
            "working_file_sha256",
            "authority_basis",
        }
        assert (
            row["repository_relative_path"]
            == (subject.resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256[resource_role]["path"])
        )
        assert row["authority_basis"] == (
            subject.deploy._CURRENT_RUNTIME_SOURCE_AUTHORITY_BASIS  # noqa: SLF001
        )
    assert subject.deploy._validated_expected_runtime_source_hashes(plan)  # noqa: SLF001


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
    deployed = subject.resource_v8.EXACT_DEPLOYED_FILE_SHA256
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
        subject.DIRECT_SUCCESSOR_RELEASE_SCHEMA,
        subject.DIRECT_SUCCESSOR_RELEASE_STATUS,
        "canonical_active_release_sha256",
    )
    binding["file_sha256"] = subject.DIRECT_SUCCESSOR_RELEASE_FILE_SHA256
    binding["canonical_sha256"] = subject.DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
    return binding


def _resource_binding() -> dict[str, Any]:
    return _content_binding(
        subject.resource_v8.RESOURCE_SCHEMA,
        subject.resource_v8.RESOURCE_STATUS,
        subject.resource_v8.RESOURCE_CANONICAL_FIELD,
    )


def _config_correction_binding() -> dict[str, Any]:
    return _content_binding(
        subject.resource_v8.config_successor.SCHEMA_VERSION,
        subject.resource_v8.config_successor.STATUS,
        subject.resource_v8.config_successor.CANONICAL_FIELD,
    )


def _resource() -> dict[str, Any]:
    files = {
        role: {
            "role": role,
            "repository_relative_path": frozen["path"],
            "sha256": frozen["sha256"],
        }
        for role, frozen in subject.resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.items()
    }
    return {
        "config_correction": _config_correction_binding(),
        "host": {
            "instance_id": subject.resource_v8.CURRENT_INSTANCE_ID,
            "instance_type": "c7i-flex.large",
        },
        "fresh_disabled_process": {
            "disabled_pid": 101,
            "disabled_pid_start_ticks": 1_000,
            "disabled_process_identity_sha256": "c" * 64,
            "disabled_config_path": "/runtime/config.disabled.yaml",
            "disabled_config_sha256": subject.resource_v8.EXPECTED_DISABLED_CONFIG_SHA256,
            "fresh_pid": True,
            "fresh_start_ticks": True,
            "same_pid_pre_post": True,
        },
        "runtime_sources": {
            "direct_successor_execution_commit": subject.DIRECT_SUCCESSOR_EXECUTION_COMMIT,
            "files": files,
        },
    }


def _active_sources() -> dict[str, Any]:
    files = {
        role: {
            "role": role,
            "repository_relative_path": frozen["path"],
            "sha256": frozen["sha256"],
            "active_working_matches_direct_successor": True,
            "direct_successor_commit_blob_matches": True,
            "resource_v8_binding_matches": True,
        }
        for role, frozen in subject.resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.items()
    }
    return {
        "execution_commit": subject.DIRECT_SUCCESSOR_EXECUTION_COMMIT,
        "files": files,
        "runtime_source_manifest_sha256": subject.resource_v8.canonical_sha256(files),
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
    raw["canonical_process_identity_sha256"] = subject.resource_v8.document_sha256(
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
        "global_flow_shadow_enabled": False,
        "global_flow_shadow_config_explicit": True,
        "global_reference_shadow_enabled": False,
        "global_reference_shadow_config_explicit": True,
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
        "f05_buy_e3_active_release_file_sha256": subject.DIRECT_SUCCESSOR_RELEASE_FILE_SHA256,
        "f05_buy_e3_active_release_canonical_sha256": (
            subject.DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
        ),
    }


def _startup() -> dict[str, Any]:
    backend = {
        name: 0
        for name in (
            "native",
            "market_count",
            "trade_batches",
            "trade_events_seen",
            "trade_events_accepted",
            "book_events_seen",
            "book_events_accepted",
            "out_of_order_events",
            "stale_trade_events",
            "trade_overflow_events",
            "book_overflow_events",
        )
    }
    return {
        "schema_version": subject.STARTUP_ATTESTATION_SCHEMA,
        "status": "accepted",
        "errors": [],
        "gates": {
            "safe_to_start_live_loops": True,
            "shadow_config_explicit": True,
            "global_flow_shadow_backend_contract_valid": True,
            "global_reference_shadow_state_contract_valid": True,
            "buy_e3_active_release_matches_running_config": True,
        },
        "shadow_runtime_identity": {
            "schema_version": "narrowgate_shadow_runtime_identity.v1",
            "global_flow_shadow_enabled": False,
            "global_reference_shadow_enabled": False,
            "global_flow_native_requested": True,
            "global_flow_native_effective": False,
            "global_flow_backend": backend,
            "global_reference_bridge_basis_sample_count": 0,
            "state_restore_contract": "shadow_state_never_restored",
            "global_flow_shadow_config_explicit": True,
            "global_reference_shadow_config_explicit": True,
        },
        "buy_e3_active_release": {
            "execution_commit": subject.DIRECT_SUCCESSOR_EXECUTION_COMMIT,
            "execution_tree": subject.DIRECT_SUCCESSOR_EXECUTION_TREE,
            "annotated_operational_tag": subject.DIRECT_SUCCESSOR_ANNOTATED_TAG,
            "annotated_operational_tag_object": subject.DIRECT_SUCCESSOR_TAG_OBJECT,
            "active_config_file_sha256": subject.ACTIVE_CONFIG_SHA256,
            "disabled_config_file_sha256": subject.resource_v8.EXPECTED_DISABLED_CONFIG_SHA256,
        },
        "fill_cooldown_state": {
            "restore_mode": "fresh_b0_no_checkpoint",
            "buy_deadline_identity": "B0",
            "buy_remaining_ms": 0,
            "e3_deadline_imported": False,
        },
    }


def _runtime_with_real_startup_attestation() -> dict[str, Any]:
    runtime = _runtime()
    runtime.update(
        {
            "recorded_at_utc": "2026-08-24T00:00:00Z",
            "python_executable": "/runtime/.venv/bin/python",
            "native_runtime": {
                "profile": "fixture-native",
                "module": "/runtime/native.so",
                "NARROWGATE_CPP_QUOTE_CORE": False,
                "NARROWGATE_CPP_SIGNAL_FEATURES": False,
                "NARROWGATE_CPP_GLOBAL_FLOW": True,
                "NARROWGATE_CPP_LIVE_ROUTING": False,
                "NARROWGATE_CPP_STRICT": False,
                "NARROWGATE_CPP_GLOBAL_FLOW_REQUESTED": True,
                "NARROWGATE_CPP_GLOBAL_FLOW_EFFECTIVE": False,
            },
        }
    )
    source_plan = subject._startup_source_plan()  # noqa: SLF001
    expected_sources = subject.deploy._validated_expected_runtime_source_hashes(  # noqa: SLF001
        source_plan
    )
    source_rows = [
        {
            "path": path,
            "working_file_sha256": sha,
            "head_blob_sha256": sha,
            "working_size_bytes": index + 1,
            "head_blob_size_bytes": index + 1,
            "matches_head_blob": True,
        }
        for index, (path, sha) in enumerate(sorted(expected_sources.items()))
    ]
    snapshot = {
        "commit": subject.DIRECT_SUCCESSOR_EXECUTION_COMMIT,
        "tree": subject.DIRECT_SUCCESSOR_EXECUTION_TREE,
        "status_porcelain_sha256": __import__("hashlib").sha256(b"").hexdigest(),
        "status_entry_count": 0,
        "worktree_clean": True,
        "snapshot_internally_stable": True,
    }
    startup = _startup()
    startup.update(
        {
            "attested_at_utc": "2026-08-24T00:00:01Z",
            "fill_cooldown_state": {
                "schema_version": subject.deploy.FILL_COOLDOWN_STATE_SCHEMA,
                "reset_policy": "fresh_process_b0",
                "restore_mode": "fresh_b0_no_checkpoint",
                "checkpoint_loaded": False,
                "checkpoint_sequence": 0,
                "consec_buy": 0.0,
                "consec_sell": 0.0,
                "buy_remaining_ms": 0,
                "sell_remaining_ms": 0,
                "last_buy_fill_ts_ms": 0,
                "last_sell_fill_ts_ms": 0,
                "last_fill_side": "",
                "buy_deadline_identity": "B0",
                "sell_deadline_identity": "B0",
                "snapshot_ts_ms": 1,
            },
            "buy_e3_active_release": {
                "path": "/runtime/release.v2.json",
                "file_sha256": subject.DIRECT_SUCCESSOR_RELEASE_FILE_SHA256,
                "file_canonical_sha256": (subject.DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256),
                "execution_commit": subject.DIRECT_SUCCESSOR_EXECUTION_COMMIT,
                "execution_tree": subject.DIRECT_SUCCESSOR_EXECUTION_TREE,
                "annotated_operational_tag": subject.DIRECT_SUCCESSOR_ANNOTATED_TAG,
                "annotated_operational_tag_object": subject.DIRECT_SUCCESSOR_TAG_OBJECT,
                "active_config_file_sha256": subject.ACTIVE_CONFIG_SHA256,
                "disabled_config_file_sha256": (
                    subject.resource_v8.EXPECTED_DISABLED_CONFIG_SHA256
                ),
            },
            "gates": {
                name: True
                for name in subject.deploy._STARTUP_GATE_FIELDS  # noqa: SLF001
            },
            "running_checkout": {
                "schema_version": subject.deploy.RUNNING_CHECKOUT_SCHEMA,
                "git_commit": subject.DIRECT_SUCCESSOR_EXECUTION_COMMIT,
                "git_tree": subject.DIRECT_SUCCESSOR_EXECUTION_TREE,
                "git_worktree_clean": True,
                "pre_snapshot": deepcopy(snapshot),
                "post_snapshot": deepcopy(snapshot),
                "stable_snapshot": {
                    "pre_snapshot_internally_stable": True,
                    "post_snapshot_internally_stable": True,
                    "commit_identical": True,
                    "tree_identical": True,
                    "status_identical": True,
                    "runtime_files_match_head": True,
                    "stable": True,
                },
                "runtime_source_file_count": len(source_rows),
                "runtime_source_manifest_sha256": (
                    subject.deploy._runtime_source_manifest_sha256(source_rows)  # noqa: SLF001
                ),
                "runtime_source_files": source_rows,
            },
            "loaded_module_origins": {
                role: {
                    "module_name": module_name,
                    "origin_path": f"/runtime/{relative}",
                    "repository_relative_path": relative,
                    "source_sha256": expected_sources[relative],
                }
                for role, (
                    module_name,
                    relative,
                ) in subject.deploy._LOADED_RUNTIME_MODULE_IDENTITIES.items()  # noqa: SLF001
            },
            "interpreter_identity": {
                "schema_version": subject.deploy.INTERPRETER_IDENTITY_SCHEMA,
                "version": "3.12.13",
                "before": {
                    "reported_path": "/runtime/.venv/bin/python",
                    "resolved_path": "/usr/bin/python3.12",
                    "sha256": "9" * 64,
                    "size_bytes": 123,
                },
                "after": {
                    "reported_path": "/runtime/.venv/bin/python",
                    "resolved_path": "/usr/bin/python3.12",
                    "sha256": "9" * 64,
                    "size_bytes": 123,
                },
                "stable": True,
            },
            "native_runtime_identity": {
                "schema_version": subject.deploy.NATIVE_RUNTIME_IDENTITY_SCHEMA,
                "profile": "fixture-native",
                "platform": "linux",
                "enabled": True,
                "reported_module_path": "/runtime/native.so",
                "loaded_module_origin_path": "/runtime/native.so",
                "before": {
                    "reported_path": "/runtime/native.so",
                    "resolved_path": "/runtime/native.so",
                    "sha256": "8" * 64,
                    "size_bytes": 456,
                },
                "after": {
                    "reported_path": "/runtime/native.so",
                    "resolved_path": "/runtime/native.so",
                    "sha256": "8" * 64,
                    "size_bytes": 456,
                },
                "stable": True,
            },
        }
    )
    runtime["startup_attestation"] = startup
    return runtime


def _semantics() -> dict[str, Any]:
    startup = _startup()
    return {
        "startup_attestation_sha256": subject.resource_v8.canonical_sha256(startup),
        "startup_status": "accepted",
        "running_checkout_commit": subject.DIRECT_SUCCESSOR_EXECUTION_COMMIT,
        "running_checkout_tree": subject.DIRECT_SUCCESSOR_EXECUTION_TREE,
        "buy_deadline_identity": "B0",
        "fill_cooldown_restore_mode": "fresh_b0_no_checkpoint",
        "buy_remaining_ms": 0,
        "e3_deadline_imported": False,
        "shadow_runtime": subject._shadow_runtime_semantics(startup, _runtime()),  # noqa: SLF001
    }


def _health_projection(*, updates: int = 2_000, **overrides: Any) -> dict[str, Any]:
    shadow: dict[str, Any] = {
        name: 0
        for name in (
            "externalSources",
            *subject.resource_v8.GLOBAL_FLOW_STATE_ZERO_FIELDS,
            *subject.resource_v8.GLOBAL_FLOW_VALUE_ZERO_FIELDS,
            *subject.resource_v8.GLOBAL_REFERENCE_ZERO_FIELDS,
            *subject.resource_v8.GLOBAL_REFERENCE_VALUE_ZERO_FIELDS,
            *subject.resource_v8.GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS,
        )
    }
    shadow.update(
        {
            "globalFlowReason": subject.resource_v8.SHADOW_DISABLED_REASON,
            "globalRefReason": subject.resource_v8.SHADOW_DISABLED_REASON,
        }
    )
    counters = {name: 0 for name in subject.resource_v8.WINDOW_ZERO_COUNTERS[:-2]}
    projection: dict[str, Any] = {
        "boolean_cooldown_enabled": 1,
        "boolean_cooldown_updates": updates,
        "buy_e3_enabled": 1,
        "deep_book_buffer": 0,
        "shadow_disabled_state": shadow,
        "counter_values": counters,
    }
    projection.update(overrides)
    return projection


def _health_line(timestamp: str, *, updates: int = 2_000, **overrides: Any) -> str:
    values: dict[str, Any] = {
        "booleanCooldownEnabled": 1,
        "booleanCooldownUpdates": updates,
        "buyE3CooldownEnabled": 1,
        "deepBookBuffer": 0,
        **_health_projection()["shadow_disabled_state"],
        **_health_projection()["counter_values"],
    }
    values.update(overrides)
    fields = " ".join(f"{name}={value}" for name, value in values.items())
    return f"{timestamp} [main] INFO HEALTH {fields}\n"


def _health_window(log_path: Path, *, pid: int = 202, start_ticks: int = 2_000) -> dict[str, Any]:
    rows = []
    for generation, (timestamp, updates) in enumerate(
        (("2026-08-24 00:00:00", 2_000), ("2026-08-24 00:00:01", 2_001)),
        start=1,
    ):
        line = _health_line(timestamp, updates=updates).encode("ascii")
        rows.append(
            {
                "fresh_generation": generation,
                "line_offset_bytes": (generation - 1) * len(line),
                "line_size_bytes": len(line),
                "line_sha256": __import__("hashlib").sha256(line).hexdigest(),
                "main_wall_timestamp_s": 1_777_161_600.0 + generation - 1,
                "projection": _health_projection(updates=updates),
            }
        )
    return {
        "schema_version": subject.HEALTH_WINDOW_SCHEMA,
        "status": subject.HEALTH_WINDOW_STATUS,
        "log_path_provenance": str(log_path.resolve()),
        "boundary_offset_bytes": 0,
        "active_pid": pid,
        "active_pid_start_ticks": start_ticks,
        "active_process_stable_identity_sha256": "f" * 64,
        "rows": rows,
        "checks": {
            "constructor_boundary_only": True,
            "two_consecutive_fresh_main_health_rows": True,
            "same_pid_and_start_ticks_before_between_after": True,
            "sell_owner_enabled_both_rows": True,
            "buy_e3_enabled_both_rows": True,
            "external_sources_absolute_zero_both_rows": True,
            "global_flow_explicit_disabled_error_and_backend_zero_both_rows": True,
            "global_reference_explicit_disabled_error_and_state_zero_both_rows": True,
        },
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
        "external_venues:\n"
        "  enabled: false\n"
        "  shadow_only: true\n"
        "multi_market:\n"
        "  global_flow_shadow_enabled: false\n"
        "  global_reference_shadow_enabled: false\n",
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
        subject.resource_v8, "file_sha256", lambda _path: subject.ACTIVE_CONFIG_SHA256
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
    live_log = tmp_path / "maker.log"
    live_log.write_text("", encoding="ascii")
    health = _health_window(live_log, pid=pid, start_ticks=start_ticks)
    monkeypatch.setattr(
        subject,
        "_capture_fresh_active_health_window",
        lambda **_kwargs: deepcopy(health),
    )
    monkeypatch.setattr(
        subject,
        "_validate_active_health_window",
        lambda raw, **_kwargs: deepcopy(dict(raw)),
    )
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
        live_log_path=tmp_path / "maker.log",
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

    assert payload["schema_version"].endswith(".v7")
    assert set(payload["runtime_authority"]) == subject.CONTENT_BINDING_FIELDS
    assert set(payload["resource_receipt"]) == subject.CONTENT_BINDING_FIELDS
    assert payload["checks"] == subject.CHECKS
    assert payload["checks"]["external_venues_disabled"] is True
    assert payload["startup_semantics"]["shadow_runtime"]["all_shadow_evaluators_disabled"] is True
    assert {name for name, value in payload["checks"].items() if value is False} == {
        "retroactive_signature"
    }
    files = payload["active_process"]["runtime_source_files"]
    assert subject.REQUIRED_ACTIVE_SOURCE_ROLES.issubset(files)
    assert (
        files["buy_e3_runtime"]["sha256"]
        == (subject.resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256["buy_e3_runtime"]["sha256"])
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
    with pytest.raises(subject.ActiveCaptureV6Error, match=match):
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
        expected_repository_root=Path("/runtime"),
        expected_release_path="/runtime/release.v2.json",
    )

    assert observed["running_checkout_commit"] == subject.DIRECT_SUCCESSOR_EXECUTION_COMMIT
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
        (
            None,
            lambda row: row["shadow_runtime_identity"].__setitem__(
                "global_flow_shadow_enabled", True
            ),
            "shadow runtime authority drifted",
        ),
        (
            None,
            lambda row: row["shadow_runtime_identity"]["global_flow_backend"].__setitem__(
                "out_of_order_events", 1
            ),
            "not exact absolute zero",
        ),
        (
            lambda row: row.__setitem__("global_reference_shadow_config_explicit", False),
            None,
            "shadow runtime authority drifted",
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

    with pytest.raises(subject.ActiveCaptureV6Error, match=match):
        subject._runtime_semantics(
            runtime,
            process=_process(),
            release=_release(),
            release_binding=_release_binding(),
            expected_repository_root=Path("/runtime"),
            expected_release_path="/runtime/release.v2.json",
        )


def test_active_source_capture_rejects_lifecycle_source_tamper(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    role = "order_lifecycle_live_writer_v2"
    for frozen in subject.resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.values():
        path = tmp_path / frozen["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture\n", encoding="ascii")
    monkeypatch.setattr(
        subject.resource_v8,
        "file_sha256",
        lambda path: (
            "0" * 64
            if str(path).endswith("order_lifecycle_live_writer_v2.py")
            else next(
                frozen["sha256"]
                for frozen in subject.resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.values()
                if str(path).endswith(frozen["path"])
            )
        ),
    )
    monkeypatch.setattr(
        subject,
        "_git_blob_sha256",
        lambda _root, relative: next(
            frozen["sha256"]
            for frozen in subject.resource_v8.CURRENT_SUCCESSOR_RUNTIME_SOURCE_SHA256.values()
            if relative == frozen["path"]
        ),
    )

    with pytest.raises(subject.ActiveCaptureV6Error, match=role):
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
        subject.resource_v8.canonical_sha256(process_body)
    )
    payload[subject.CANONICAL_FIELD] = subject.resource_v8.document_sha256(
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

    with pytest.raises(subject.ActiveCaptureV6Error, match="transition or successor source"):
        subject.validate_active_capture(
            receipt,
            runtime_repository_root=tmp_path,
            direct_release_path=tmp_path / "release.v2.json",
            resource_receipt_path=tmp_path / "resource.json",
            config_correction_path=tmp_path / "config-correction.json",
            live_log_path=tmp_path / "maker.log",
        )


def test_create_only_writer_refuses_overwrite(tmp_path: Path) -> None:
    target = tmp_path / "receipt.json"
    target.write_text("reserved\n", encoding="ascii")

    with pytest.raises(subject.resource_v8.BuyE3CurrentHostResourceGateError):
        subject.resource_v8.atomic_write_receipt(target, {"status": "must_not_replace"})


def _append(path: Path, *lines: str) -> None:
    with path.open("a", encoding="ascii") as handle:
        handle.writelines(lines)


def test_real_tail_ignores_preboundary_health_and_revalidates_first_two_fresh_rows(
    tmp_path: Path,
) -> None:
    log = tmp_path / "maker.log"
    old_line = _health_line("2026-08-23 23:59:59", updates=1_999)
    log.write_text(old_line, encoding="ascii")
    tail = subject.ActiveMainHealthTail(log)
    _append(
        log,
        "2026-08-24 00:00:00 [main] INFO unrelated=1\n",
        _health_line("2026-08-24 00:00:01", updates=2_000),
        _health_line("2026-08-24 00:00:02", updates=2_001),
    )

    window = subject._capture_fresh_active_health_window(  # noqa: SLF001
        tail=tail,
        expected_pid=202,
        expected_start_ticks=2_000,
        process_stable_identity_sha256="f" * 64,
        identity_supplier=lambda: (202, 2_000),
        timeout_s=1.0,
        poll_interval_s=0.01,
    )
    observed = subject._validate_active_health_window(  # noqa: SLF001
        window,
        live_log_path=log,
        expected_pid=202,
        expected_start_ticks=2_000,
        expected_process_stable_identity_sha256="f" * 64,
    )

    assert observed == window
    assert window["boundary_offset_bytes"] == len(old_line.encode("ascii"))
    assert [row["fresh_generation"] for row in window["rows"]] == [1, 2]
    assert window["rows"][0]["line_offset_bytes"] > window["boundary_offset_bytes"]
    assert window["rows"][1]["projection"]["boolean_cooldown_updates"] == 2_001


def test_health_capture_rejects_missing_second_fresh_row(tmp_path: Path) -> None:
    log = tmp_path / "maker.log"
    log.write_text("", encoding="ascii")
    tail = subject.ActiveMainHealthTail(log)
    _append(log, _health_line("2026-08-24 00:00:01", updates=2_000))
    clock = {"now": 0.0}

    def monotonic() -> float:
        clock["now"] += 0.1
        return clock["now"]

    with pytest.raises(subject.ActiveCaptureV7Error, match="two fresh"):
        subject._capture_fresh_active_health_window(  # noqa: SLF001
            tail=tail,
            expected_pid=202,
            expected_start_ticks=2_000,
            process_stable_identity_sha256="f" * 64,
            identity_supplier=lambda: (202, 2_000),
            timeout_s=0.15,
            poll_interval_s=0.01,
            sleep=lambda _seconds: None,
            monotonic=monotonic,
        )


@pytest.mark.parametrize(("second_updates"), [2_000, 1_999])
def test_health_capture_requires_strict_active_callback_progress(
    tmp_path: Path, second_updates: int
) -> None:
    log = tmp_path / "maker.log"
    log.write_text("", encoding="ascii")
    tail = subject.ActiveMainHealthTail(log)
    _append(
        log,
        _health_line("2026-08-24 00:00:01", updates=2_000),
        _health_line("2026-08-24 00:00:02", updates=second_updates),
    )
    with pytest.raises(subject.ActiveCaptureV7Error, match="consecutive and monotonic"):
        subject._capture_fresh_active_health_window(  # noqa: SLF001
            tail=tail,
            expected_pid=202,
            expected_start_ticks=2_000,
            process_stable_identity_sha256="f" * 64,
            identity_supplier=lambda: (202, 2_000),
            timeout_s=1.0,
            poll_interval_s=0.01,
        )


def test_health_capture_rejects_pid_drift_between_fresh_rows(tmp_path: Path) -> None:
    log = tmp_path / "maker.log"
    log.write_text("", encoding="ascii")
    tail = subject.ActiveMainHealthTail(log)
    _append(
        log,
        _health_line("2026-08-24 00:00:01", updates=2_000),
        _health_line("2026-08-24 00:00:02", updates=2_001),
    )
    calls = {"count": 0}

    def identity() -> tuple[int, int]:
        calls["count"] += 1
        return (202, 2_000) if calls["count"] < 4 else (203, 2_001)

    with pytest.raises(subject.ActiveCaptureV7Error, match="PID/start ticks changed"):
        subject._capture_fresh_active_health_window(  # noqa: SLF001
            tail=tail,
            expected_pid=202,
            expected_start_ticks=2_000,
            process_stable_identity_sha256="f" * 64,
            identity_supplier=identity,
            timeout_s=1.0,
            poll_interval_s=0.01,
        )


@pytest.mark.parametrize(
    ("override", "value"),
    [
        ("buyE3CooldownEnabled", 0),
        ("booleanCooldownEnabled", 0),
        ("externalSources", 1),
        ("externalErrors", 1),
        ("externalRecordDropped", 1),
        ("globalFlowShadowEnabled", 1),
        ("globalFlowStateError", 1),
        ("globalFlowBookAccepted", 1),
        ("globalRefShadowEnabled", 1),
        ("globalRefStateError", 1),
        ("globalRefBasisSamples", 1),
    ],
)
def test_real_tail_rejects_any_active_shadow_or_owner_drift(
    tmp_path: Path, override: str, value: int
) -> None:
    log = tmp_path / "maker.log"
    log.write_text("", encoding="ascii")
    tail = subject.ActiveMainHealthTail(log)
    _append(log, _health_line("2026-08-24 00:00:01", **{override: value}))

    with pytest.raises(subject.ActiveCaptureV7Error, match="HEALTH"):
        tail.poll()


def test_health_validator_rejects_skipped_first_row_and_byte_tamper(tmp_path: Path) -> None:
    log = tmp_path / "maker.log"
    log.write_text("", encoding="ascii")
    tail = subject.ActiveMainHealthTail(log)
    first = _health_line("2026-08-24 00:00:01", updates=2_000)
    second = _health_line("2026-08-24 00:00:02", updates=2_001)
    third = _health_line("2026-08-24 00:00:03", updates=2_002)
    _append(log, first, second, third)
    window = subject._capture_fresh_active_health_window(  # noqa: SLF001
        tail=tail,
        expected_pid=202,
        expected_start_ticks=2_000,
        process_stable_identity_sha256="f" * 64,
        identity_supplier=lambda: (202, 2_000),
        timeout_s=1.0,
        poll_interval_s=0.01,
    )

    skipped = deepcopy(window)
    third_bytes = third.encode("ascii")
    skipped["rows"][0] = deepcopy(skipped["rows"][1])
    skipped["rows"][0]["fresh_generation"] = 1
    skipped["rows"][1] = {
        **deepcopy(skipped["rows"][1]),
        "fresh_generation": 2,
        "line_offset_bytes": len(first.encode("ascii")) + len(second.encode("ascii")),
        "line_size_bytes": len(third_bytes),
        "line_sha256": __import__("hashlib").sha256(third_bytes).hexdigest(),
        "main_wall_timestamp_s": skipped["rows"][1]["main_wall_timestamp_s"] + 1,
        "projection": _health_projection(updates=2_002),
    }
    with pytest.raises(subject.ActiveCaptureV7Error, match="rows or intervening bytes"):
        subject._validate_active_health_window(  # noqa: SLF001
            skipped,
            live_log_path=log,
            expected_pid=202,
            expected_start_ticks=2_000,
            expected_process_stable_identity_sha256="f" * 64,
        )

    raw = bytearray(log.read_bytes())
    raw[window["rows"][0]["line_offset_bytes"]] ^= 1
    log.write_bytes(raw)
    with pytest.raises(subject.ActiveCaptureV7Error, match="could not be recomputed|drifted"):
        subject._validate_active_health_window(  # noqa: SLF001
            window,
            live_log_path=log,
            expected_pid=202,
            expected_start_ticks=2_000,
            expected_process_stable_identity_sha256="f" * 64,
        )


def test_runtime_semantics_passes_real_current_deploy_validator() -> None:
    release_binding = _release_binding()
    phase_binding = subject._release_phase_binding(  # noqa: SLF001
        release_binding,
        runtime_release_path="/runtime/release.v2.json",
    )
    assert set(phase_binding) == subject.deploy._ACTIVE_RELEASE_PHASE_BINDING_FIELDS  # noqa: SLF001
    assert (
        subject.deploy._expected_active_release_identity(  # noqa: SLF001
            phase_binding,
            expected_execution_commit=subject.DIRECT_SUCCESSOR_EXECUTION_COMMIT,
            expected_execution_tree=subject.DIRECT_SUCCESSOR_EXECUTION_TREE,
        )["active_config_file_sha256"]
        == subject.ACTIVE_CONFIG_SHA256
    )

    observed = subject._runtime_semantics(  # noqa: SLF001
        _runtime_with_real_startup_attestation(),
        process=_process(),
        release=_release(),
        release_binding=release_binding,
        expected_repository_root=Path("/runtime"),
        expected_release_path="/runtime/release.v2.json",
    )
    assert observed["startup_status"] == "accepted"
    assert observed["running_checkout_commit"] == subject.DIRECT_SUCCESSOR_EXECUTION_COMMIT


def test_active_capture_module_cli_help_uses_collector_module_route() -> None:
    repository = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [sys.executable, "-m", "scripts.f05_buy_e3_active_capture_v8", "--help"],
        cwd=repository,
        env={**os.environ, "PYTHONPATH": str(repository)},
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert "capture" in completed.stdout
