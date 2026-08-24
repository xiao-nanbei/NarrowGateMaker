from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
import yaml

from scripts import f05_buy_e3_no_shadow_config_successor as subject


def _pair(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    common = {
        "strategy": {"buy_e3_cooldown_policy_enabled": False, "fill_cooldown": 85.0},
        "multi_market": {"enabled": True, "market_stage": "enhanced"},
        "external_venues": {"enabled": False, "shadow_only": True},
    }
    disabled = tmp_path / "parent-disabled.yaml"
    active = tmp_path / "parent-active.yaml"
    disabled.write_text(yaml.safe_dump(common, sort_keys=False), encoding="utf-8")
    common["strategy"]["buy_e3_cooldown_policy_enabled"] = True
    active.write_text(yaml.safe_dump(common, sort_keys=False), encoding="utf-8")
    monkeypatch.setattr(
        subject,
        "PARENT_DISABLED_SHA256",
        hashlib.sha256(disabled.read_bytes()).hexdigest(),
    )
    monkeypatch.setattr(
        subject,
        "PARENT_ACTIVE_SHA256",
        hashlib.sha256(active.read_bytes()).hexdigest(),
    )
    return disabled, active


def test_successor_adds_only_two_false_flags_and_preserves_pair_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disabled, active = _pair(tmp_path, monkeypatch)
    out_disabled = tmp_path / "successor-disabled.yaml"
    out_active = tmp_path / "successor-active.yaml"
    receipt = subject.build_successor_pair(
        parent_disabled=disabled,
        parent_active=active,
        output_disabled=out_disabled,
        output_active=out_active,
    )

    disabled_cfg = yaml.safe_load(out_disabled.read_text(encoding="utf-8"))
    active_cfg = yaml.safe_load(out_active.read_text(encoding="utf-8"))
    for cfg in (disabled_cfg, active_cfg):
        assert cfg["multi_market"]["global_flow_shadow_enabled"] is False
        assert cfg["multi_market"]["global_reference_shadow_enabled"] is False
        assert cfg["external_venues"]["enabled"] is False
        assert "active_release" not in cfg
    assert disabled_cfg["strategy"]["buy_e3_cooldown_policy_enabled"] is False
    assert active_cfg["strategy"]["buy_e3_cooldown_policy_enabled"] is True
    active_cfg["strategy"].pop("buy_e3_cooldown_policy_enabled")
    disabled_cfg["strategy"].pop("buy_e3_cooldown_policy_enabled")
    assert active_cfg == disabled_cfg
    assert receipt["status"] == subject.STATUS
    assert receipt["canonical_config_successor_sha256"] == subject.canonical_sha256(
        receipt, field="canonical_config_successor_sha256"
    )


def test_successor_is_create_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disabled, active = _pair(tmp_path, monkeypatch)
    out_disabled = tmp_path / "successor-disabled.yaml"
    out_active = tmp_path / "successor-active.yaml"
    out_disabled.write_text("do-not-replace\n", encoding="ascii")
    with pytest.raises(FileExistsError, match="refusing to replace"):
        subject.build_successor_pair(
            parent_disabled=disabled,
            parent_active=active,
            output_disabled=out_disabled,
            output_active=out_active,
        )
    assert out_disabled.read_text(encoding="ascii") == "do-not-replace\n"
    assert not out_active.exists()


def test_successor_rejects_parent_semantic_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disabled, active = _pair(tmp_path, monkeypatch)
    active_cfg = yaml.safe_load(active.read_text(encoding="utf-8"))
    active_cfg["strategy"]["fill_cooldown"] = 86.0
    active.write_text(yaml.safe_dump(active_cfg, sort_keys=False), encoding="utf-8")
    monkeypatch.setattr(
        subject,
        "PARENT_ACTIVE_SHA256",
        hashlib.sha256(active.read_bytes()).hexdigest(),
    )
    with pytest.raises(ValueError, match="differ outside BUY E3"):
        subject.build_successor_pair(
            parent_disabled=disabled,
            parent_active=active,
            output_disabled=tmp_path / "out-disabled.yaml",
            output_active=tmp_path / "out-active.yaml",
        )
