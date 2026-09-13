from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from models.replay_cache_components import canonical_sha256, write_model_overlay
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_dual_overlay_ml_ab_replay as dual,
)

DAY = "2026-04-17"
BUNDLE_ID = "a" * 64


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _control_binding(
    tmp_path: Path,
    *,
    remove_index: int | None = None,
    include_prior_carry: bool = False,
) -> dict:
    start = int(np.datetime64(DAY, "ms").astype(np.int64))
    timestamps = np.arange(start, start + 86_400_000, 10_000, dtype=np.int64)
    if include_prior_carry:
        timestamps = np.concatenate((np.asarray([start - 10_000]), timestamps))
    if remove_index is not None:
        timestamps = np.delete(timestamps, remove_index)
    values: list[object] = [timestamps]
    values.extend(np.full(len(timestamps), 0.5, dtype=np.float64) for _ in range(20))
    values.append({"feature": np.ones(len(timestamps), dtype=np.float64)})
    identity = {
        "schema_version": "narrowgate.model_overlay_day.v1.1",
        "dag_node": "model_overlay_day",
        "symbol": "BTCUSDC",
        "day": DAY,
        "run_ml_inference": True,
        "model_bundle_identity_sha256": BUNDLE_ID,
        "market_context_identity_sha256": "b" * 64,
        "feature_source_identity_sha256": "c" * 64,
        "cross_market_enabled": True,
        "toxicity_horizon_s": 10,
    }
    artifact = write_model_overlay(cache_root=tmp_path, identity=identity, ml_data=tuple(values))
    manifest_path = artifact.manifest_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    data_path = artifact.directory / "model_overlay.npz"
    return {
        "cache_root": str(tmp_path),
        "identity": identity,
        "identity_sha256": canonical_sha256(identity),
        "manifest": {
            "path": str(manifest_path),
            "sha256": _sha(manifest_path),
            "size_bytes": manifest_path.stat().st_size,
        },
        "data": {
            "path": str(data_path),
            "sha256": _sha(data_path),
            "size_bytes": data_path.stat().st_size,
        },
        "manifest_payload": manifest,
    }


def _baseline_binding(tmp_path: Path) -> dict:
    config_path = tmp_path / "config.yaml"
    identity_path = tmp_path / "identity.json"
    config_path.write_text("ml: true\n", encoding="utf-8")
    identity_payload = {
        "schema_version": dual.candidate_abi.EXPECTED_BASELINE_SCHEMA,
        "baseline_id": dual.candidate_abi.EXPECTED_BASELINE_ID,
        "config": {
            "ml_enabled": True,
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_live_enabled": False,
        },
    }
    identity_path.write_text(json.dumps(identity_payload), encoding="utf-8")
    return {
        "pointer": {
            "baseline_id": dual.candidate_abi.EXPECTED_BASELINE_ID,
            "ml_enabled": True,
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "live_config_sha256": _sha(config_path),
        },
        "identity": identity_payload,
        "identity_path": identity_path,
        "identity_sha256": _sha(identity_path),
        "config_path": config_path,
    }


def test_control_overlay_is_complete_hash_bound_v9_schedule(tmp_path: Path) -> None:
    schedule = dual.load_bound_v9_control_overlay(
        _control_binding(tmp_path),
        expected_day=DAY,
        expected_model_bundle_identity_sha256=BUNDLE_ID,
    )
    assert len(schedule.ready_ts_ms) == 8_640
    assert schedule.target_grid_row_count == 8_640
    assert schedule.ml_data is not None
    assert schedule.model_bundle_identity_sha256 == BUNDLE_ID


def test_control_overlay_gap_fails_before_outcomes(tmp_path: Path) -> None:
    with pytest.raises(dual.DualOverlayReplayError, match="complete causal 10s"):
        dual.load_bound_v9_control_overlay(
            _control_binding(tmp_path, remove_index=100),
            expected_day=DAY,
            expected_model_bundle_identity_sha256=BUNDLE_ID,
        )


def test_control_overlay_rejects_noncanonical_prior_carry(tmp_path: Path) -> None:
    with pytest.raises(dual.DualOverlayReplayError, match="complete causal 10s"):
        dual.load_bound_v9_control_overlay(
            _control_binding(tmp_path, include_prior_carry=True),
            expected_day=DAY,
            expected_model_bundle_identity_sha256=BUNDLE_ID,
        )


def test_dual_replay_keeps_both_arms_ml_enabled_and_control_nonempty(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        dual,
        "load_operational_baseline_binding",
        lambda **_: _baseline_binding(tmp_path),
    )
    control = dual.load_bound_v9_control_overlay(
        _control_binding(tmp_path / "overlay"),
        expected_day=DAY,
        expected_model_bundle_identity_sha256=BUNDLE_ID,
    )
    candidate_ml = (
        np.asarray([1], dtype=np.int64),
        *[np.asarray([0.5], dtype=np.float64) for _ in range(5)],
    )
    candidate = SimpleNamespace(
        utc_day=DAY,
        ml_data=candidate_ml,
        overlay_identity_sha256="d" * 64,
        research_bundle_sha256="e" * 64,
    )
    window = SimpleNamespace(
        ml_data=None,
        book_source_authority="native_formal_lifecycle",
        trades="trades",
        var_ts_ms="var-ts",
        var_ssq="var-ssq",
        bbo_data="bbo",
        l2_data="l2",
        var_ti="var-ti",
        var_retsq="var-retsq",
    )
    calls: list[dict] = []

    def simulate(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return {"terminal_mtm_pnl": 0.0}

    result = dual.run_dual_overlay_tick_replay(
        window=window,
        base_params={
            "ml_enabled": False,
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_live_enabled": False,
        },
        control_schedule=control,
        candidate_schedule=candidate,
        simulate=simulate,
    )
    assert len(calls) == 2
    assert calls[0]["kwargs"]["ml_data"] is control.ml_data
    assert calls[0]["kwargs"]["ml_data"] is not None
    assert calls[1]["kwargs"]["ml_data"] is candidate_ml
    assert calls[0]["args"][4]["ml_enabled"] is True
    assert calls[1]["args"][4]["ml_enabled"] is True
    assert calls[0]["args"][4] == calls[1]["args"][4]
    assert result["identity"]["predecessor_ml_off_control_forbidden"] is True


def test_dual_arm_binding_never_calls_disable_ml_params(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        dual,
        "load_operational_baseline_binding",
        lambda **_: _baseline_binding(tmp_path),
    )
    arms = dual.bind_current_v9_dual_ml_on_arms(
        {
            "ml_enabled": False,
            "vol_blend": 0.7,
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_live_enabled": False,
        }
    )
    assert arms.control == arms.candidate
    assert arms.control["ml_enabled"] is True
    assert arms.control["vol_blend"] == pytest.approx(0.7)
