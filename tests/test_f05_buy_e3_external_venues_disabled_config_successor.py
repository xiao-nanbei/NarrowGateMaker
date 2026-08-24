from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts import f05_buy_e3_external_venues_disabled_config_successor as subject


def _config(*, buy_e3: bool, external_enabled: bool) -> dict[str, Any]:
    return {
        "strategy": {
            "buy_e3_cooldown_policy_enabled": buy_e3,
            "buy_fill_selection_shadow_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "cross_venue_fair_price_shadow_enabled": False,
            "artifact": "unchanged",
        },
        "logging": {
            "inventory_campaign_shadow_enabled": False,
            "market_tape_enabled": False,
        },
        "external_venues": {
            "enabled": external_enabled,
            "shadow_only": True,
            "sources": [{"venue": "bitget", "enabled": True}],
        },
        "execution": {"unchanged": True},
    }


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")
    path.chmod(0o600)


def _inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    rows = {
        "old_disabled_config": _config(buy_e3=False, external_enabled=True),
        "old_active_config": _config(buy_e3=True, external_enabled=True),
        "new_disabled_config": _config(buy_e3=False, external_enabled=False),
        "new_active_config": _config(buy_e3=True, external_enabled=False),
    }
    paths: dict[str, Path] = {}
    constant_names = {
        "old_disabled_config": "OLD_DISABLED_CONFIG_SHA256",
        "old_active_config": "OLD_ACTIVE_CONFIG_SHA256",
        "new_disabled_config": "NEW_DISABLED_CONFIG_SHA256",
        "new_active_config": "NEW_ACTIVE_CONFIG_SHA256",
    }
    for role, payload in rows.items():
        path = tmp_path / f"{role}.yaml"
        _write_yaml(path, payload)
        paths[role] = path
        monkeypatch.setattr(subject, constant_names[role], subject.file_sha256(path))
    release = tmp_path / "release.json"
    release.write_text("{}\n", encoding="ascii")
    release.chmod(0o600)
    paths["direct_release"] = release
    monkeypatch.setattr(subject, "_release_binding", lambda _path: dict(subject.RELEASE_V2_BINDING))
    monkeypatch.setattr(
        subject,
        "_git_execution",
        lambda _root, tag: {
            "repository_root": str(tmp_path),
            "execution_commit": "a" * 40,
            "execution_tree": "b" * 40,
            "annotated_tag_object": "c" * 40,
            "tag_peeled_commit": "a" * 40,
            "annotated_tag": tag,
            "direct_v4_commit_is_ancestor": False,
            "runtime_authority_checkout": False,
        },
    )
    return paths


def _build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, Any], dict[str, Path]]:
    paths = _inputs(tmp_path, monkeypatch)
    payload = subject.build_receipt(
        repository_root=tmp_path,
        annotated_tag="config-successor-test",
        generated_utc="2026-08-24T06:00:00Z",
        **paths,
    )
    return payload, paths


def test_frozen_config_hashes_and_successor_boundary() -> None:
    assert subject.NEW_DISABLED_CONFIG_SHA256 == (
        "10158a92177cd87b77fdb24a2a477dcab4b41cfb29208cf96c19953edafe166f"
    )
    assert subject.NEW_ACTIVE_CONFIG_SHA256 == (
        "ad153012b14e725a3ac24f0ddbe02bc353168a13ec827b777cc94761020524ec"
    )
    assert subject.AUTHORITY_DESIGN["release_v2_reissued"] is False
    assert subject.AUTHORITY_DESIGN["fresh_resource_active_transport_lifecycle_and_final_evidence_required"]
    assert all(value is False for value in subject.PERMISSIONS.values())
    assert all(value is False for value in subject.EVIDENCE_BOUNDARY.values())


def test_build_proves_only_external_enable_changed_and_pair_remains_buy_e3_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _paths = _build(tmp_path, monkeypatch)
    assert payload["semantic_diff"]["changed_paths"] == ["external_venues.enabled"]
    assert payload["corrected_config_pair"]["external_venues_enabled"] is False
    assert payload["corrected_config_pair"]["active_disabled_only_difference"] == (
        "strategy.buy_e3_cooldown_policy_enabled"
    )
    assert payload[subject.CANONICAL_FIELD] == subject.document_sha256(
        payload, subject.CANONICAL_FIELD
    )


def test_pair_rejects_any_additional_change() -> None:
    old_disabled = _config(buy_e3=False, external_enabled=True)
    old_active = _config(buy_e3=True, external_enabled=True)
    new_disabled = _config(buy_e3=False, external_enabled=False)
    new_active = _config(buy_e3=True, external_enabled=False)
    new_active["execution"]["unchanged"] = False
    with pytest.raises(subject.ConfigSuccessorError, match="outside external_venues.enabled"):
        subject._validate_pair(old_disabled, old_active, new_disabled, new_active)  # noqa: SLF001


def test_pair_rejects_external_venue_left_enabled() -> None:
    old_disabled = _config(buy_e3=False, external_enabled=True)
    old_active = _config(buy_e3=True, external_enabled=True)
    with pytest.raises(subject.ConfigSuccessorError, match="changed outside"):
        subject._validate_pair(  # noqa: SLF001
            old_disabled,
            old_active,
            deepcopy(old_disabled),
            _config(buy_e3=True, external_enabled=False),
        )


def test_validate_rejects_tamper_and_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, paths = _build(tmp_path, monkeypatch)
    receipt = tmp_path / "receipt.json"
    subject._write_exclusive(receipt, payload)  # noqa: SLF001
    validated = subject.validate_receipt(
        receipt,
        repository_root=tmp_path,
        annotated_tag="config-successor-test",
        **paths,
    )
    assert validated == payload
    tampered = deepcopy(payload)
    tampered["semantic_diff"]["new_value"] = True
    tampered[subject.CANONICAL_FIELD] = subject.document_sha256(
        tampered, subject.CANONICAL_FIELD
    )
    receipt.write_text(json.dumps(tampered), encoding="utf-8")
    receipt.chmod(0o600)
    with pytest.raises(subject.ConfigSuccessorError, match="drifted"):
        subject.validate_receipt(
            receipt,
            repository_root=tmp_path,
            annotated_tag="config-successor-test",
            **paths,
        )
    receipt.chmod(0o644)
    with pytest.raises(subject.ConfigSuccessorError, match="mode"):
        subject.validate_receipt(
            receipt,
            repository_root=tmp_path,
            annotated_tag="config-successor-test",
            **paths,
        )


def test_create_only_writer_refuses_overwrite(tmp_path: Path) -> None:
    target = tmp_path / "receipt.json"
    target.write_text("reserved\n", encoding="ascii")
    with pytest.raises(FileExistsError):
        subject._write_exclusive(target, {"status": "must-not-replace"})  # noqa: SLF001
    assert target.read_text(encoding="ascii") == "reserved\n"


def test_config_files_require_single_link_0600(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    _write_yaml(path, _config(buy_e3=False, external_enabled=False))
    path.chmod(0o644)
    with pytest.raises(subject.ConfigSuccessorError, match="mode"):
        subject._load_yaml(path, subject.file_sha256(path), "config")  # noqa: SLF001
