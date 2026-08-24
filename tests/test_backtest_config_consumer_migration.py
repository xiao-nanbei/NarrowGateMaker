from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

import data_paths
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_panel_builder_v1 as panel_builder,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    freeze_multiscale_ema_boolean_cooldown_duration_policy as duration_freeze,
)
from research.families.f06_placement_fill_cif.audit import placement_fill_panel
from research.families.f09_campaign_action_uplift.audit import (
    causal_v12_toxicity_conditional_p3_reach_gate as toxicity_gate,
)
from scripts import run_restart_aware_continuous_baseline as restart_baseline


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


def test_current_f05_panel_defaults_to_versioned_v12_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert data_paths.IMMUTABLE_BACKTEST_V12_CONFIG_SHA256 == (
        panel_builder.offline.ACTIVE_PRIVATE_CONFIG_SHA256
    )
    private_root = tmp_path / "private-configs"
    live_alias = tmp_path / "mutable-live-alias.yaml"
    monkeypatch.setenv("NARROWGATE_PRIVATE_CONFIG_ROOT", str(private_root))
    monkeypatch.setenv("NARROWGATE_LIVE_CONFIG", str(live_alias))
    monkeypatch.setenv("NARROWGATE_DATA_ROOT", str(tmp_path / "data"))

    archive = private_root / data_paths.IMMUTABLE_BACKTEST_V12_CONFIG_FILENAME
    defaults = panel_builder._default_cli_paths()
    assert defaults["owner_config"] == archive.resolve()
    inputs = SimpleNamespace(
        owner_artifacts=SimpleNamespace(private_config=archive),
        project_data_root=tmp_path / "data",
        marketdata_root=tmp_path / "marketdata",
    )
    assert panel_builder._portable_bound_path(archive, inputs=inputs) == (
        data_paths.IMMUTABLE_BACKTEST_V12_CONFIG_LOCATOR
    )


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
        restart_baseline,
        "load_operational_baseline_binding",
        lambda **_: binding,
    )
    monkeypatch.setattr(restart_baseline, "require_sha256", lambda *args, **kwargs: None)
    restart = restart_baseline.validate_identities()
    assert restart["config_path"] == archive.resolve()

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

    wrong = dict(binding)
    wrong["config_path"] = live_alias.resolve()
    monkeypatch.setattr(
        restart_baseline,
        "load_operational_baseline_binding",
        lambda **_: wrong,
    )
    with pytest.raises(RuntimeError, match="did not resolve the immutable v12"):
        restart_baseline.validate_identities()
