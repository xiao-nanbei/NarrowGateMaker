from __future__ import annotations

import json
from pathlib import Path

import pytest

from live.config import Config, _validate_config
from live.main import record_startup_runtime_identity
from live.runtime_policy import (
    F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV,
    F05_BUY_E3_OWNER_OVERRIDE_ENV,
    Q90_ACTION_OWNER_OVERRIDE_ENV,
    f05_boolean_cooldown_runtime_policy,
    f05_buy_e3_runtime_policy,
    q90_action_runtime_policy,
    require_f05_boolean_cooldown_restart,
    require_f05_buy_e3_restart,
    require_q90_action_restart,
    write_runtime_identity,
)

ROOT = Path(__file__).resolve().parents[1]


def _q90_action_config() -> Config:
    cfg = Config()
    cfg.websocket.deep_book_enabled = True
    cfg.strategy.dynamic_fill_hazard_shadow_enabled = True
    cfg.strategy.dynamic_fill_hazard_shadow_model_path = "model.json"
    cfg.strategy.dynamic_fill_hazard_shadow_model_sha256 = "a" * 64
    cfg.strategy.dynamic_fill_hazard_shadow_sides = "BUY"
    cfg.strategy.dynamic_fill_hazard_action_enabled = True
    cfg.strategy.dynamic_fill_hazard_action_policy_path = "policy.json"
    cfg.strategy.dynamic_fill_hazard_action_policy_sha256 = "b" * 64
    return cfg


def test_q90_action_is_runtime_fail_closed_without_owner_override() -> None:
    with pytest.raises(ValueError, match="POST_CANCEL_RECOVERY"):
        q90_action_runtime_policy(True, environ={})


def test_direct_runtime_config_validation_uses_the_same_q90_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(Q90_ACTION_OWNER_OVERRIDE_ENV, raising=False)

    with pytest.raises(ValueError, match="POST_CANCEL_RECOVERY"):
        _validate_config(_q90_action_config())


def test_direct_runtime_config_records_an_explicit_owner_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(Q90_ACTION_OWNER_OVERRIDE_ENV, "1")

    _validate_config(_q90_action_config())


def test_q90_owner_override_is_explicit_in_runtime_identity(tmp_path: Path) -> None:
    policy = q90_action_runtime_policy(
        True,
        environ={Q90_ACTION_OWNER_OVERRIDE_ENV: "1"},
    )
    path = tmp_path / "runtime_identity.json"
    write_runtime_identity(path, policy)

    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert persisted["q90_action_runtime_authority"] == (
        "owner_risk_accepted_override"
    )
    assert persisted["q90_owner_override_requested"] is True
    assert persisted["q90_owner_override_effective"] is True


def test_run_sh_preflights_before_background_launch() -> None:
    script = (ROOT / "live/run.sh").read_text(encoding="utf-8")
    start_body = script.split("start() {", 1)[1].split("\nstop() {", 1)[0]
    restart_body = script.split("restart() {", 1)[1].split(
        "\nstatus() {", 1
    )[0]

    assert "_run_deploy_preflight" in start_body
    assert "scripts/preflight_live_deploy.py" in script
    assert start_body.index("_run_deploy_preflight") < start_body.index("nohup ")
    assert restart_body.index("_run_deploy_preflight") < restart_body.index(
        "stop 2>/dev/null"
    )


def test_q90_action_state_cannot_change_via_sighup() -> None:
    require_q90_action_restart(False, False)
    require_q90_action_restart(True, True)
    with pytest.raises(ValueError, match="restart through live/run.sh"):
        require_q90_action_restart(False, True)


def test_startup_identity_preserves_its_own_schema(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("project_name: NarrowGate\n", encoding="utf-8")
    cfg = Config()
    cfg.logging.file = str(tmp_path / "maker.log")

    path, identity = record_startup_runtime_identity(
        cfg=cfg,
        config_path=config_path,
        native_runtime={"profile": "test"},
        dry_run=True,
    )

    assert path == tmp_path / "runtime_identity.json"
    assert identity["schema_version"] == "narrowgate_live_runtime_identity.v1"
    assert identity["q90_runtime_policy_schema_version"] == (
        "narrowgate_runtime_policy.v1"
    )
    assert identity["q90_action_runtime_authority"] == (
        "action_suspended_shadow_only"
    )


def test_f05_boolean_cooldown_requires_permanent_owner_label_and_override() -> None:
    with pytest.raises(ValueError, match="owner-authorized"):
        f05_boolean_cooldown_runtime_policy(
            True,
            evidence_route="owner_risk_accepted_promotion",
            environ={},
        )
    with pytest.raises(ValueError, match="permanent"):
        f05_boolean_cooldown_runtime_policy(
            True,
            evidence_route="research_supported_promotion",
            environ={F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV: "1"},
        )

    policy = f05_boolean_cooldown_runtime_policy(
        True,
        evidence_route="owner_risk_accepted_promotion",
        environ={F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV: "1"},
    )
    assert policy["f05_boolean_cooldown_hard_gates_passed"] is False
    assert policy["f05_boolean_cooldown_owner_override_effective"] is True
    assert policy["f05_boolean_cooldown_runtime_authority"] == (
        "owner_risk_accepted_active"
    )


def test_f05_boolean_cooldown_identity_is_restart_only() -> None:
    base = {
        "boolean_cooldown_policy_enabled": False,
        "boolean_cooldown_policy_path": "",
        "boolean_cooldown_policy_sha256": "",
        "boolean_cooldown_predicate_bundle_path": "",
        "boolean_cooldown_predicate_bundle_sha256": "",
        "boolean_cooldown_ema_warmup_s": 2048.0,
        "boolean_cooldown_evidence_route": "owner_risk_accepted_promotion",
    }
    require_f05_boolean_cooldown_restart(base, dict(base))
    changed = {**base, "boolean_cooldown_policy_enabled": True}
    with pytest.raises(ValueError, match="restart-only"):
        require_f05_boolean_cooldown_restart(base, changed)


def test_f05_buy_e3_requires_separate_owner_override_and_label() -> None:
    with pytest.raises(ValueError, match="owner risk-accepted"):
        f05_buy_e3_runtime_policy(
            True,
            evidence_route="owner_risk_accepted_buy_e3_v1",
            environ={},
        )
    with pytest.raises(ValueError, match="permanent"):
        f05_buy_e3_runtime_policy(
            True,
            evidence_route="research_supported",
            environ={F05_BUY_E3_OWNER_OVERRIDE_ENV: "1"},
        )
    policy = f05_buy_e3_runtime_policy(
        True,
        evidence_route="owner_risk_accepted_buy_e3_v1",
        environ={F05_BUY_E3_OWNER_OVERRIDE_ENV: "1"},
    )
    assert policy["f05_buy_e3_research_supported"] is False
    assert policy["f05_buy_e3_hard_gates_passed"] is False
    assert policy["f05_buy_e3_owner_override_effective"] is True


def test_f05_buy_e3_identity_is_restart_only() -> None:
    base = {
        "buy_e3_cooldown_policy_enabled": False,
        "buy_e3_cooldown_artifact_manifest_path": "",
        "buy_e3_cooldown_artifact_manifest_sha256": "",
        "buy_e3_cooldown_artifact_sha256": "",
        "buy_e3_cooldown_policy_path": "",
        "buy_e3_cooldown_policy_sha256": "",
        "buy_e3_cooldown_predicate_bundle_path": "",
        "buy_e3_cooldown_predicate_bundle_sha256": "",
        "buy_e3_cooldown_ema_warmup_s": 2048.0,
        "buy_e3_cooldown_evidence_route": "owner_risk_accepted_buy_e3_v1",
    }
    require_f05_buy_e3_restart(base, dict(base))
    with pytest.raises(ValueError, match="restart-only"):
        require_f05_buy_e3_restart(
            base,
            {**base, "buy_e3_cooldown_policy_enabled": True},
        )
