from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from live import main as live_main
from live import runtime_policy
from live.config import Config
from scripts import f05_buy_e3_active_release as release_signer
from strategy import boolean_cooldown_buy_e3 as buy_runtime

ARTIFACT_SHA256 = "4" * 64
MANIFEST_FILE_SHA256 = "1" * 64
POLICY_FILE_SHA256 = "2" * 64
PREDICATE_FILE_SHA256 = "3" * 64


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _binding(role: str, file_sha256: str, index: int) -> dict[str, Any]:
    return {
        "role": role,
        "path": f"/private/evidence/{role}.json",
        "file_sha256": file_sha256,
        "size_bytes": 100 + index,
        "mode": "0600",
        "device": 1,
        "inode": 1000 + index,
        "schema_version": f"fixture.{role}.v1",
        "identity": f"fixture_{role}",
        "status": "passed",
        "canonical_field": "canonical_sha256",
        "canonical_sha256": format(index + 8, "x") * 64,
    }


def _release_payload() -> dict[str, Any]:
    artifact_roles = {
        "manifest": _binding("manifest", MANIFEST_FILE_SHA256, 1),
        "policy": _binding("policy", POLICY_FILE_SHA256, 2),
        "predicate_bundle": _binding("predicate_bundle", PREDICATE_FILE_SHA256, 3),
    }
    evidence_roles = {
        role: _binding(role, format(index + 9, "x") * 64, index + 10)
        for index, role in enumerate(
            (
                "final_composition",
                "compatible_attempt_final",
                "concurrent_resource",
                "runtime_regression",
                "sell54",
                "activation_envelope",
            )
        )
    }
    payload: dict[str, Any] = {
        "schema_version": release_signer.ACTIVE_RELEASE_SCHEMA,
        "identity": release_signer.ACTIVE_RELEASE_IDENTITY,
        "status": release_signer.ACTIVE_RELEASE_STATUS,
        "generated_utc": "2026-08-23T00:00:00Z",
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "action_authorized": True,
        "live_authorized": True,
        "scope": {
            "side": "BUY",
            "trigger": "exposure_increasing_executed_fill",
            "output": "total_cooldown",
            "reducing_buy_unchanged": True,
            "sell_owner_policy_unchanged": True,
        },
        "execution": {
            "execution_commit": "a" * 40,
            "execution_tree": "b" * 40,
            "annotated_operational_tag": "f05-buy-e3-owner-v1",
            "annotated_operational_tag_object": "c" * 40,
            "tag_peeled_commit": "a" * 40,
        },
        "exact_artifact": {
            "artifact_sha256": ARTIFACT_SHA256,
            "roles": artifact_roles,
        },
        "evidence": evidence_roles,
        "rollback": {
            "buy_e3_enabled": False,
            "buy_deadline_identity": "B0",
            "e3_deadline_imported": False,
            "b0_seconds": 85,
            "b0_multiplier": "consecutive_fill_units",
            "b0_contract": "85s_x_consecutive_fill_units",
        },
        "evidence_boundary": {
            "old_oof_applies_to_learning_algorithm_only": True,
            "exact_artifact_oof_available": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "shadow_created": False,
            "companion_created": False,
            "new_economic_arm_run": False,
        },
    }
    payload["canonical_active_release_sha256"] = release_signer.document_sha256(
        payload,
        "canonical_active_release_sha256",
    )
    return payload


def _validate(payload: dict[str, Any]) -> dict[str, str]:
    return buy_runtime._validate_active_release(  # noqa: SLF001
        payload,
        expected_canonical_sha256=payload["canonical_active_release_sha256"],
        expected_artifact_sha256=ARTIFACT_SHA256,
        expected_manifest_file_sha256=MANIFEST_FILE_SHA256,
        expected_policy_file_sha256=POLICY_FILE_SHA256,
        expected_predicate_bundle_file_sha256=PREDICATE_FILE_SHA256,
    )


def _rewrite_canonical(payload: dict[str, Any]) -> None:
    payload["canonical_active_release_sha256"] = release_signer.document_sha256(
        payload,
        "canonical_active_release_sha256",
    )


def _release_environment(path: str = "/private/release.json") -> dict[str, str]:
    return {
        runtime_policy.F05_BUY_E3_OWNER_OVERRIDE_ENV: "1",
        runtime_policy.F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV: path,
        runtime_policy.F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV: "d" * 64,
        runtime_policy.F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV: "e" * 64,
    }


def test_live_loader_uses_the_standalone_signer_schema() -> None:
    assert buy_runtime.ACTIVE_RELEASE_SCHEMA == release_signer.ACTIVE_RELEASE_SCHEMA
    assert buy_runtime.ACTIVE_RELEASE_IDENTITY == release_signer.ACTIVE_RELEASE_IDENTITY
    assert buy_runtime.ACTIVE_RELEASE_STATUS == release_signer.ACTIVE_RELEASE_STATUS


def test_portable_loader_accepts_exact_signed_release_contract() -> None:
    payload = _release_payload()

    identity = _validate(payload)

    assert identity == {
        "file_canonical_sha256": payload["canonical_active_release_sha256"],
        "execution_commit": "a" * 40,
        "execution_tree": "b" * 40,
        "annotated_operational_tag": "f05-buy-e3-owner-v1",
        "annotated_operational_tag_object": "c" * 40,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", "wrong.schema"),
        ("identity", "wrong.identity"),
        ("status", "not_authorized"),
        ("research_supported", True),
        ("formal_hierarchy_passed", True),
        ("formal_hard_gates_passed", True),
        ("owner_risk_accepted", False),
        ("action_authorized", False),
        ("live_authorized", False),
    ],
)
def test_portable_loader_rejects_authority_drift(field: str, value: Any) -> None:
    payload = _release_payload()
    payload[field] = value
    _rewrite_canonical(payload)

    with pytest.raises(ValueError, match="authority"):
        _validate(payload)


@pytest.mark.parametrize(
    ("role", "argument"),
    [
        ("manifest", "expected_manifest_file_sha256"),
        ("policy", "expected_policy_file_sha256"),
        ("predicate_bundle", "expected_predicate_bundle_file_sha256"),
    ],
)
def test_portable_loader_rejects_artifact_role_hash_drift(
    role: str,
    argument: str,
) -> None:
    payload = _release_payload()
    arguments = {
        "expected_canonical_sha256": payload["canonical_active_release_sha256"],
        "expected_artifact_sha256": ARTIFACT_SHA256,
        "expected_manifest_file_sha256": MANIFEST_FILE_SHA256,
        "expected_policy_file_sha256": POLICY_FILE_SHA256,
        "expected_predicate_bundle_file_sha256": PREDICATE_FILE_SHA256,
    }
    arguments[argument] = "f" * 64

    with pytest.raises(ValueError, match=f"{role}_binding"):
        buy_runtime._validate_active_release(payload, **arguments)  # noqa: SLF001


def test_portable_loader_rejects_evidence_binding_drift() -> None:
    payload = _release_payload()
    payload["evidence"]["runtime_regression"]["mode"] = "0644"
    _rewrite_canonical(payload)

    with pytest.raises(ValueError, match="runtime_regression_binding"):
        _validate(payload)


@pytest.mark.parametrize(
    "missing",
    [
        runtime_policy.F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV,
        runtime_policy.F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV,
        runtime_policy.F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV,
    ],
)
def test_enabled_process_requires_all_release_environment_bindings(missing: str) -> None:
    environment = _release_environment()
    environment.pop(missing)

    with pytest.raises(ValueError, match=missing):
        runtime_policy.f05_buy_e3_active_release_runtime_authority(
            True,
            environ=environment,
        )


def test_enabled_process_requires_owner_override() -> None:
    environment = _release_environment()
    environment.pop(runtime_policy.F05_BUY_E3_OWNER_OVERRIDE_ENV)

    with pytest.raises(ValueError, match="owner override"):
        runtime_policy.f05_buy_e3_active_release_runtime_authority(
            True,
            environ=environment,
        )


def test_environment_authority_is_portable_and_does_not_open_release() -> None:
    environment = _release_environment("/path/that/does/not/exist/release.json")

    authority = runtime_policy.f05_buy_e3_active_release_runtime_authority(
        True,
        environ=environment,
    )

    assert authority == {
        "schema_version": runtime_policy.F05_BUY_E3_ACTIVE_RELEASE_AUTHORITY_SCHEMA,
        "required": True,
        "active_release_path": "/path/that/does/not/exist/release.json",
        "active_release_file_sha256": "d" * 64,
        "active_release_canonical_sha256": "e" * 64,
    }


def test_disabled_process_clears_release_environment_bindings() -> None:
    authority = runtime_policy.f05_buy_e3_active_release_runtime_authority(
        False,
        environ=_release_environment(),
    )

    assert authority == {
        "schema_version": runtime_policy.F05_BUY_E3_ACTIVE_RELEASE_AUTHORITY_SCHEMA,
        "required": False,
        "active_release_path": "",
        "active_release_file_sha256": "",
        "active_release_canonical_sha256": "",
    }


@pytest.mark.parametrize(
    "field",
    [
        runtime_policy.F05_BUY_E3_ACTIVE_RELEASE_PATH_ENV,
        runtime_policy.F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256_ENV,
        runtime_policy.F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256_ENV,
    ],
)
def test_active_release_environment_identity_is_restart_only(field: str) -> None:
    previous = _release_environment()
    candidate = dict(previous)
    candidate[field] = "/private/other.json" if field.endswith("_PATH") else "f" * 64

    runtime_policy.require_f05_buy_e3_active_release_restart(previous, dict(previous))
    with pytest.raises(ValueError, match="restart-only"):
        runtime_policy.require_f05_buy_e3_active_release_restart(previous, candidate)


def _write_private_release(path: Path) -> dict[str, Any]:
    payload = _release_payload()
    path.write_text(
        json.dumps(payload, sort_keys=True, indent=2, allow_nan=False) + "\n",
        encoding="ascii",
    )
    path.chmod(0o600)
    return payload


def test_bound_release_reader_accepts_private_hash_bound_file(tmp_path: Path) -> None:
    path = tmp_path / "release.json"
    payload = _write_private_release(path)

    opened = buy_runtime._open_bound_json(  # noqa: SLF001
        path,
        _file_sha256(path),
        "buy_e3_active_release",
    )
    try:
        assert opened.payload == payload
    finally:
        buy_runtime._close_opened_files([opened])  # noqa: SLF001


@pytest.mark.parametrize("mode", [0o640, 0o644, 0o660])
def test_bound_release_reader_rejects_permission_drift(
    tmp_path: Path,
    mode: int,
) -> None:
    path = tmp_path / "release.json"
    _write_private_release(path)
    expected = _file_sha256(path)
    path.chmod(mode)

    with pytest.raises(ValueError, match="mode_not_private"):
        buy_runtime._open_bound_json(path, expected, "buy_e3_active_release")  # noqa: SLF001


def test_bound_release_reader_rejects_hardlink(tmp_path: Path) -> None:
    path = tmp_path / "release.json"
    _write_private_release(path)
    expected = _file_sha256(path)
    os.link(path, tmp_path / "second-link.json")

    with pytest.raises(ValueError, match="link_count"):
        buy_runtime._open_bound_json(path, expected, "buy_e3_active_release")  # noqa: SLF001


def test_bound_release_reader_rejects_symlink(tmp_path: Path) -> None:
    path = tmp_path / "release.json"
    _write_private_release(path)
    link = tmp_path / "release-link.json"
    link.symlink_to(path)

    with pytest.raises(ValueError, match="symlink"):
        buy_runtime._open_bound_json(  # noqa: SLF001
            link,
            _file_sha256(path),
            "buy_e3_active_release",
        )


def test_bound_release_reader_rejects_file_tamper(tmp_path: Path) -> None:
    path = tmp_path / "release.json"
    _write_private_release(path)
    expected = _file_sha256(path)
    path.write_bytes(path.read_bytes() + b" \n")
    path.chmod(0o600)

    with pytest.raises(ValueError, match="file_sha256_mismatch"):
        buy_runtime._open_bound_json(path, expected, "buy_e3_active_release")  # noqa: SLF001


def test_startup_runtime_identity_emits_release_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    expected = {
        "schema_version": runtime_policy.F05_BUY_E3_ACTIVE_RELEASE_AUTHORITY_SCHEMA,
        "required": True,
        "active_release_path": "/private/release.json",
        "active_release_file_sha256": "d" * 64,
        "active_release_canonical_sha256": "e" * 64,
    }
    monkeypatch.setattr(
        live_main,
        "f05_buy_e3_active_release_runtime_authority",
        lambda *_args, **_kwargs: deepcopy(expected),
    )
    monkeypatch.setenv(runtime_policy.F05_BUY_E3_OWNER_OVERRIDE_ENV, "1")
    cfg = Config()
    cfg.strategy.buy_e3_cooldown_policy_enabled = True
    cfg.logging.file = str(tmp_path / "logs" / "maker.log")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("project_name: NarrowGate\n", encoding="ascii")

    _path, identity = live_main.record_startup_runtime_identity(
        cfg=cfg,
        config_path=config_path,
        native_runtime={"profile": "test"},
        dry_run=True,
    )

    assert (
        identity["f05_buy_e3_active_release_authority_schema_version"] == expected["schema_version"]
    )
    assert identity["f05_buy_e3_required"] is True
    assert identity["f05_buy_e3_active_release_path"] == expected["active_release_path"]
    assert (
        identity["f05_buy_e3_active_release_file_sha256"] == expected["active_release_file_sha256"]
    )
    assert (
        identity["f05_buy_e3_active_release_canonical_sha256"]
        == expected["active_release_canonical_sha256"]
    )


def test_release_authority_is_not_part_of_reloadable_config() -> None:
    strategy = Config().strategy
    assert not hasattr(strategy, "buy_e3_active_release_path")
    assert not hasattr(strategy, "buy_e3_active_release_file_sha256")
    assert not hasattr(strategy, "buy_e3_active_release_canonical_sha256")
