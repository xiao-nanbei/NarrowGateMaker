from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import data_paths
from models.backtest_config import load_live_config_as_params, load_tick_base_params
from research.families.f05_fill_quality_quote_ev.audit import (
    freeze_multiscale_ema_boolean_cooldown_duration_policy as duration_freeze,
)
from research.families.f06_placement_fill_cif.audit import placement_fill_panel
from research.families.f09_campaign_action_uplift.audit import (
    causal_v12_toxicity_conditional_p3_reach_gate as toxicity_gate,
)


@pytest.mark.parametrize("entrypoint", ("backtest_tick", "tick_ab", "quote_decomposition_tick"))
def test_offline_help_accepts_a_symlinked_checkout(tmp_path: Path, entrypoint: str) -> None:
    root = Path(__file__).resolve().parents[1]
    checkout = tmp_path / "checkout-alias"
    checkout.symlink_to(root, target_is_directory=True)
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    result = subprocess.run(
        [sys.executable, "-I", str(checkout / "models" / f"{entrypoint}.py"), "--help"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout.lower()


def _normalized_v12_binding(
    *,
    archive: Path,
    identity: Path,
) -> dict[str, object]:
    pointer = {
        "schema_version": "narrowgate_operational_baseline_pointer.v2",
        "baseline_id": (
            "btc_usdc_causal_v12_f05_boolean_cooldown_owner_risk_accepted_baseline_20260812"
        ),
        "identity_sha256": "1" * 64,
        "live_config_sha256": data_paths.IMMUTABLE_BACKTEST_V12_CONFIG_SHA256,
        "bundle_meta_sha256": "2" * 64,
        "backtest_control_arm": ("causal_multichannel_window_boolean_cooldown_owner_policy_v1"),
        "ml_enabled": True,
        "dynamic_fill_hazard_shadow_enabled": True,
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_shadow_enabled": False,
        "buy_fill_selection_live_enabled": False,
    }
    return {
        "pointer": pointer,
        "governance_pointer": {"schema_version": "narrowgate_operational_baseline_pointer.v2"},
        "identity": {"baseline_id": pointer["baseline_id"]},
        "identity_path": identity,
        "identity_sha256": pointer["identity_sha256"],
        "config_path": archive,
        "config_exists": True,
        "config_scope": "immutable_v12_backtest_default",
    }


def test_v12_locator_is_distinct_from_mutable_live_alias(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private_root = tmp_path / "private-configs"
    live_alias = tmp_path / "mutable-live-alias.yaml"
    monkeypatch.setenv("NARROWGATE_PRIVATE_CONFIG_ROOT", str(private_root))
    monkeypatch.setenv("NARROWGATE_LIVE_CONFIG", str(live_alias))

    expected_archive = (private_root / data_paths.IMMUTABLE_BACKTEST_V12_CONFIG_FILENAME).resolve()
    assert data_paths.immutable_backtest_v12_config_path(root=tmp_path) == expected_archive
    assert (
        data_paths.resolve_portable_path(
            data_paths.IMMUTABLE_BACKTEST_V12_CONFIG_LOCATOR,
            root=tmp_path,
        )
        == expected_archive
    )
    assert (
        data_paths.resolve_portable_path(
            "${NARROWGATE_LIVE_CONFIG}",
            root=tmp_path,
        )
        == live_alias.resolve()
    )
    assert expected_archive != live_alias.resolve()


def test_frozen_f05_and_f06_require_explicit_hash_closed_configs(
    tmp_path: Path,
) -> None:
    assert duration_freeze.FROZEN_CONFIG_SHA256 == (
        "62a6add8d46c2695205e278ecb41bcaa16dc8199e683ef9114c21f6118b04e18"
    )
    with pytest.raises(SystemExit):
        duration_freeze._parse_args([])
    with pytest.raises(SystemExit):
        placement_fill_panel.parse_args([])

    config = tmp_path / "historical-config.yaml"
    config.write_bytes(b"strategy:\n  fill_cooldown: 85\n")
    observed = hashlib.sha256(config.read_bytes()).hexdigest()
    assert (
        duration_freeze._require_frozen_config(
            config,
            expected_sha256=observed,
        )
        == config.resolve()
    )
    placement_fill_panel._require_identity(config, observed, "config")

    with pytest.raises(duration_freeze.FreezeError, match="identity drifted"):
        duration_freeze._require_frozen_config(config, expected_sha256="0" * 64)
    with pytest.raises(RuntimeError, match="identity changed"):
        placement_fill_panel._require_identity(config, "0" * 64, "config")


def test_flat_pointer_consumers_accept_only_normalized_v12_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private_root = tmp_path / "private-configs"
    private_root.mkdir()
    archive = private_root / data_paths.IMMUTABLE_BACKTEST_V12_CONFIG_FILENAME
    archive.write_text("frozen-v12\n", encoding="utf-8")
    identity = tmp_path / "identity.json"
    identity.write_text("{}\n", encoding="utf-8")
    live_alias = tmp_path / "mutable-live-alias.yaml"
    live_alias.write_text("current-live-e3\n", encoding="utf-8")
    monkeypatch.setenv("NARROWGATE_PRIVATE_CONFIG_ROOT", str(private_root))
    monkeypatch.setenv("NARROWGATE_LIVE_CONFIG", str(live_alias))
    binding = _normalized_v12_binding(archive=archive.resolve(), identity=identity)

    monkeypatch.setattr(
        toxicity_gate,
        "load_operational_baseline_binding",
        lambda **_: binding,
    )
    monkeypatch.setattr(
        toxicity_gate,
        "require_file",
        lambda path, expected_sha256=None: Path(path).expanduser().resolve(),
    )
    toxicity = toxicity_gate.validate_current_baseline()
    assert toxicity["config_path"] == archive.resolve()
    assert toxicity["config_path"] != live_alias.resolve()


def _local_replay_projection(tmp_path):
    config = tmp_path / "original.yaml"
    config.write_text("strategy:\n  gamma: 0.023\nml:\n  model_dir: /remote/model\n")
    model = tmp_path / "model"
    model.mkdir()
    policy = tmp_path / "policy.json"
    policy.write_text("{}")
    payload = {
        "schema_version": "narrowgate_local_replay_locator_projection.v1",
        "visibility": "local_only_do_not_publish",
        "authority": "none_locator_only",
        "source_config": {
            "path": str(config), "sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
        },
        "locator_overrides": {
            "ml.model_dir": str(model),
            "strategy.boolean_cooldown_policy_path": str(policy),
        },
    }
    projection = tmp_path / "locators.json"
    projection.write_text(json.dumps(payload))
    return config, projection, payload


def test_replay_locators_preserve_original_config_and_strategy(tmp_path, monkeypatch):
    config, projection, payload = _local_replay_projection(tmp_path)
    before = config.read_bytes()
    monkeypatch.delenv("MM_MODEL_DIR", raising=False)
    params = load_tick_base_params(
        config_path=config, locator_projection_path=projection,
        include_fill_probability=False, include_queue_calibration=False,
    )
    assert config.read_bytes() == before
    assert params["gamma"] == 0.023
    assert params["model_dir"] == payload["locator_overrides"]["ml.model_dir"]
    assert params["boolean_cooldown_policy_path"] == str(tmp_path / "policy.json")
    assert params["_config_path"] == str(config)
    assert params["_config_source_sha256"] == hashlib.sha256(before).hexdigest()
    assert params["_replay_locator_projection"]["source_config"] == payload["source_config"]


@pytest.mark.parametrize("key", ["strategy.gamma", "api.key", "risk.max_daily_loss"])
def test_replay_locators_reject_non_locator_overrides(tmp_path, key):
    config, projection, payload = _local_replay_projection(tmp_path)
    payload["locator_overrides"][key] = 0
    projection.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="only the six"):
        load_live_config_as_params(config, locator_projection_path=projection)


@pytest.mark.parametrize("invalid", ["missing", "relative", "wrong_type", "sha", "source"])
def test_replay_locators_reject_invalid_paths_or_source(tmp_path, invalid):
    config, projection, payload = _local_replay_projection(tmp_path)
    if invalid == "missing":
        payload["locator_overrides"]["ml.model_dir"] = str(tmp_path / "absent")
    elif invalid == "relative":
        payload["locator_overrides"]["ml.model_dir"] = "model"
    elif invalid == "wrong_type":
        payload["locator_overrides"]["ml.model_dir"] = str(tmp_path / "policy.json")
    elif invalid == "sha":
        payload["source_config"]["sha256"] = "0" * 64
    else:
        payload["source_config"]["path"] = str(tmp_path / "different.yaml")
    projection.write_text(json.dumps(payload))
    with pytest.raises((ValueError, FileNotFoundError)):
        load_live_config_as_params(config, locator_projection_path=projection)


def test_replay_locators_reject_environment_model_replacement(tmp_path, monkeypatch):
    config, projection, _ = _local_replay_projection(tmp_path)
    monkeypatch.setenv("MM_MODEL_DIR", str(tmp_path / "another-model"))
    with pytest.raises(ValueError, match="MM_MODEL_DIR conflicts"):
        load_tick_base_params(
            config_path=config, locator_projection_path=projection,
            include_fill_probability=False, include_queue_calibration=False,
        )
