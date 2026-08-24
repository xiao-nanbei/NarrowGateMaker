from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_native_40day_full_path_ml_ab as panel,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_native_40day_full_path_ml_ab_execution_amendment as amendment,
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_frozen_precommit_confirmation_schema_is_accepted() -> None:
    payload, _ = panel._validate_precommit(panel.DEFAULT_PRECOMMIT)

    assert payload["confirmation_panels"]["validation"]["days"] == []
    assert payload["confirmation_panels"]["family_specific_sealed_holdout"]["days"] == []


def test_precommit_rejects_a_registered_confirmation_day(tmp_path: Path) -> None:
    payload = json.loads(panel.DEFAULT_PRECOMMIT.read_text(encoding="utf-8"))
    payload["confirmation_panels"]["validation"]["days"] = ["2026-07-01"]
    path = tmp_path / "precommit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(panel.NativeFullPathABError, match="forbids Validation/holdout"):
        panel._validate_precommit(path)


def _stub_amendment_inputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> dict[str, object]:
    days = [f"2026-05-{index + 1:02d}" for index in range(31)] + [
        f"2026-06-{index + 1:02d}" for index in range(9)
    ]
    precommit_path = tmp_path / "precommit.json"
    candidate_path = tmp_path / "candidate-panel.json"
    control_path = tmp_path / "control-panel.json"
    config_path = tmp_path / "config.yaml"
    precommit_path.write_text("{}\n", encoding="utf-8")
    candidate_path.write_text("{}\n", encoding="utf-8")
    control_path.write_text("{}\n", encoding="utf-8")
    config_path.write_text("ml:\n  enabled: true\n", encoding="utf-8")

    def config_binding() -> dict[str, object]:
        return {
            "path": str(config_path.resolve()),
            "sha256": _sha(config_path),
            "size_bytes": config_path.stat().st_size,
        }

    frozen_config_sha = _sha(config_path)
    precommit = {
        "native_development_panel": {"days": days},
        "baseline": {"config_sha256": frozen_config_sha},
        "comparison": {"bootstrap_draws": 100, "bootstrap_seed": 7},
    }
    precommit_binding = {
        "path": str(precommit_path.resolve()),
        "sha256": _sha(precommit_path),
        "score_profile": {"path": str(precommit_path.resolve()), "sha256": _sha(precommit_path)},
    }
    candidate_days = {
        day: {
            "overlay_dir": str(tmp_path / "candidate" / day),
            "overlay_path": str(candidate_path.resolve()),
            "overlay_sha256": _sha(candidate_path),
            "overlay_manifest_path": str(candidate_path.resolve()),
            "overlay_manifest_sha256": _sha(candidate_path),
        }
        for day in days
    }
    control_days = {
        day: {
            "window": {"path": str(control_path.resolve()), "sha256": _sha(control_path)},
            "control_component": {},
            "native_book_artifacts": [],
            "daily_source_identity_sha256": "4" * 64,
        }
        for day in days
    }

    monkeypatch.setattr(panel, "_validate_precommit", lambda _: (precommit, precommit_binding))
    monkeypatch.setattr(
        panel,
        "_validate_candidate_panel",
        lambda *_args, **_kwargs: {
            "path": str(candidate_path.resolve()),
            "sha256": _sha(candidate_path),
            "panel_identity_sha256": "1" * 64,
            "bundle_meta_sha256": "2" * 64,
            "days": candidate_days,
        },
    )
    monkeypatch.setattr(
        panel,
        "_validate_control_sources",
        lambda *_args, **_kwargs: {
            "path": str(control_path.resolve()),
            "sha256": _sha(control_path),
            "panel_identity_sha256": "3" * 64,
            "v9_model_bundle_identity_sha256": "5" * 64,
            "operational_config": config_binding(),
            "days": control_days,
        },
    )
    return {
        "days": days,
        "precommit_path": precommit_path,
        "candidate_path": candidate_path,
        "control_path": control_path,
        "config_path": config_path,
    }


def test_successor_amendment_build_validate_and_reject_config_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    inputs = _stub_amendment_inputs(monkeypatch, tmp_path)
    output = tmp_path / "amendment.json"
    payload = amendment.build_execution_amendment(
        candidate_overlay_panel_manifest=inputs["candidate_path"],
        control_overlay_panel_manifest=inputs["control_path"],
        precommit_path=inputs["precommit_path"],
        output_path=output,
    )
    assert payload["campaign_mae_contract"]["trace_campaign_repair_max"] == (
        panel.CAMPAIGN_MAE_TRACE_MAX
    )
    assert payload["campaign_mae_contract"]["source_trace_field"] == (
        "campaign_adverse_excursion_so_far"
    )
    assert payload["governance"]["route"] == "owner_only"
    assert payload["permissions"]["live_authorized"] is False
    assert (
        amendment.validate_execution_amendment(
            output,
            candidate_overlay_panel_manifest=inputs["candidate_path"],
            control_overlay_panel_manifest=inputs["control_path"],
            precommit_path=inputs["precommit_path"],
        )
        == payload
    )

    inputs["config_path"].write_text("ml:\n  enabled: false\n", encoding="utf-8")
    with pytest.raises(amendment.ExecutionAmendmentError, match="config differs"):
        amendment.validate_execution_amendment(
            output,
            candidate_overlay_panel_manifest=inputs["candidate_path"],
            control_overlay_panel_manifest=inputs["control_path"],
            precommit_path=inputs["precommit_path"],
        )


def test_prepare_requires_and_binds_exact_successor_amendment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    inputs = _stub_amendment_inputs(monkeypatch, tmp_path)
    output = tmp_path / "amendment.json"
    amendment.build_execution_amendment(
        candidate_overlay_panel_manifest=inputs["candidate_path"],
        control_overlay_panel_manifest=inputs["control_path"],
        precommit_path=inputs["precommit_path"],
        output_path=output,
    )
    monkeypatch.setattr(panel, "_storage_gate", lambda _: {"passed": True})
    monkeypatch.setattr(panel, "_runtime_artifacts", lambda: {})
    plan = panel.prepare_execution_plan(
        candidate_overlay_panel_manifest=inputs["candidate_path"],
        control_overlay_panel_manifest=inputs["control_path"],
        execution_amendment_path=output,
        output_root=tmp_path / "plan-output",
        precommit_path=inputs["precommit_path"],
    )
    binding = plan["identity_payload"]["execution_amendment"]
    assert binding["path"] == str(output.resolve())
    assert binding["sha256"] == _sha(output)
    assert binding["trace_campaign_repair_max"] == panel.CAMPAIGN_MAE_TRACE_MAX


def test_prepare_requires_both_formal_overlays_before_writing(tmp_path: Path) -> None:
    output = tmp_path / "output"
    with pytest.raises(panel.NativeFullPathABError, match="formal candidate"):
        panel.prepare_execution_plan(
            candidate_overlay_panel_manifest=None,
            control_overlay_panel_manifest=None,
            execution_amendment_path=None,
            output_root=output,
        )
    assert not output.exists()


def test_prepare_rejects_missing_successor_amendment_before_writing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    inputs = _stub_amendment_inputs(monkeypatch, tmp_path)
    output = tmp_path / "output"
    with pytest.raises(panel.NativeFullPathABError, match="exact successor"):
        panel.prepare_execution_plan(
            candidate_overlay_panel_manifest=inputs["candidate_path"],
            control_overlay_panel_manifest=inputs["control_path"],
            execution_amendment_path=None,
            output_root=output,
            precommit_path=inputs["precommit_path"],
        )
    assert not output.exists()


def test_trace_campaign_repair_capacity_must_be_positive_and_exact() -> None:
    with pytest.raises(panel.NativeFullPathABError, match="must be positive"):
        panel._validate_campaign_mae_trace_capacity(
            {"trace_campaign_repair_max": 0},
            expected=panel.CAMPAIGN_MAE_TRACE_MAX,
        )
    with pytest.raises(panel.NativeFullPathABError, match="equal to the amendment"):
        panel._validate_campaign_mae_trace_capacity(
            {"trace_campaign_repair_max": panel.CAMPAIGN_MAE_TRACE_MAX - 1},
            expected=panel.CAMPAIGN_MAE_TRACE_MAX,
        )
    assert (
        panel._validate_campaign_mae_trace_capacity(
            {"trace_campaign_repair_max": panel.CAMPAIGN_MAE_TRACE_MAX},
            expected=panel.CAMPAIGN_MAE_TRACE_MAX,
        )
        == panel.CAMPAIGN_MAE_TRACE_MAX
    )


def _empty_result() -> dict:
    return {
        "pnl": 0.0,
        "terminal_mtm_pnl": 0.0,
        "terminal_mark_price": 65_000.0,
        "fills_bid": 0,
        "fills_ask": 0,
        "fills_total": 0,
        "abs_inventory_time_s": 0.0,
        "max_inventory": 0.0,
        "final_inventory": 0.0,
        "_fill_trace": [],
    }


def _one_fill_result() -> dict:
    result = _empty_result()
    result.update(
        {
            "fills_bid": 1,
            "fills_total": 1,
            "final_inventory": 0.001,
            "_fill_trace": [
                {
                    "side": "BUY",
                    "fill_ts": 1_000,
                    "quote_px": 65_000.0,
                    "fill_qty": 0.001,
                    "inventory_before_fill": 0.0,
                    "inventory_after_fill": 0.001,
                    "markout_30s": 0.0,
                    "ev_30s": 0.0,
                }
            ],
        }
    )
    return result


def test_projection_fails_closed_when_campaign_mae_is_not_emitted() -> None:
    summary, campaigns, fills = panel._project_arm(
        day="2026-04-17",
        arm=panel.ARMS[0],
        result=_one_fill_result(),
        order_size=0.001,
        campaign_mae_trace_max=panel.CAMPAIGN_MAE_TRACE_MAX,
    )
    assert summary["campaign_mae_usdc"] is None
    assert summary["metric_blockers"] == ["campaign_mae_not_emitted_by_authoritative_replay"]
    assert len(campaigns) == 1
    assert len(fills) == 1


def test_projection_reads_worst_campaign_mae_from_a_complete_trace() -> None:
    result = _one_fill_result()
    result["_campaign_repair_trace"] = [
        {"campaign_id": 1, "ts_ns": 1, "campaign_adverse_excursion_so_far": -0.04},
        {"campaign_id": 1, "ts_ns": 2, "campaign_adverse_excursion_so_far": -0.12},
        {"campaign_id": 2, "ts_ns": 3, "campaign_adverse_excursion_so_far": -0.07},
    ]
    result["_campaign_mae_trace_audit"] = {
        "source": "python_probe_locked_to_cpp_fill_path",
        "cpp_python_fill_path_mismatch_count": 0,
    }
    summary, _, _ = panel._project_arm(
        day="2026-04-17",
        arm=panel.ARMS[0],
        result=result,
        order_size=0.001,
        campaign_mae_trace_max=panel.CAMPAIGN_MAE_TRACE_MAX,
    )
    assert summary["campaign_mae_usdc"] == pytest.approx(-0.12)
    assert summary["campaign_mae_trace_rows"] == 3
    assert summary["campaign_mae_trace_field"] == "campaign_adverse_excursion_so_far"
    assert summary["metric_blockers"] == []


def test_projection_fails_closed_when_campaign_mae_trace_hits_capacity() -> None:
    result = _one_fill_result()
    result["_campaign_repair_trace"] = [
        {"campaign_id": 1, "ts_ns": 1, "campaign_adverse_excursion_so_far": -0.1}
    ]
    summary, _, _ = panel._project_arm(
        day="2026-04-17",
        arm=panel.ARMS[0],
        result=result,
        order_size=0.001,
        campaign_mae_trace_max=1,
    )
    assert summary["campaign_mae_usdc"] is None
    assert summary["metric_blockers"] == ["campaign_mae_trace_capacity_reached"]


def test_projection_rejects_zero_campaign_mae_trace_capacity() -> None:
    with pytest.raises(panel.NativeFullPathABError, match="trace_campaign_repair_max=0"):
        panel._project_arm(
            day="2026-04-17",
            arm=panel.ARMS[0],
            result=_empty_result(),
            order_size=0.001,
            campaign_mae_trace_max=0,
        )


def test_projection_rejects_legacy_campaign_mae_alias() -> None:
    result = _one_fill_result()
    result["_campaign_repair_trace"] = [
        {"campaign_id": 1, "ts_ns": 1, "campaign_mae": -0.1}
    ]
    with pytest.raises(panel.NativeFullPathABError, match="trace row is invalid"):
        panel._project_arm(
            day="2026-04-17",
            arm=panel.ARMS[0],
            result=result,
            order_size=0.001,
            campaign_mae_trace_max=panel.CAMPAIGN_MAE_TRACE_MAX,
        )


def test_projection_rejects_nonmonotone_campaign_adverse_excursion() -> None:
    result = _one_fill_result()
    result["_campaign_repair_trace"] = [
        {"campaign_id": 1, "ts_ns": 1, "campaign_adverse_excursion_so_far": -0.2},
        {"campaign_id": 1, "ts_ns": 2, "campaign_adverse_excursion_so_far": -0.1},
    ]
    with pytest.raises(panel.NativeFullPathABError, match="not a running minimum"):
        panel._project_arm(
            day="2026-04-17",
            arm=panel.ARMS[0],
            result=result,
            order_size=0.001,
            campaign_mae_trace_max=panel.CAMPAIGN_MAE_TRACE_MAX,
        )


def test_execute_day_is_atomic_resume_safe_and_dual_ml_on(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    day = "2026-04-17"
    window_path = tmp_path / "window.pkl"
    window_path.write_bytes(b"window")
    config_path = tmp_path / "frozen-control-config.yaml"
    config_path.write_text("execution:\n  order_size: 0.001\n", encoding="utf-8")
    mutable_alias = tmp_path / "mutable-live-alias.yaml"
    mutable_alias.write_text("execution:\n  order_size: 9.999\n", encoding="utf-8")
    precommit_path = tmp_path / "precommit.json"
    precommit_path.write_text(
        json.dumps(
            {
                "baseline": {
                    "config_path": str(mutable_alias),
                    "config_sha256": _sha(config_path),
                }
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "output"
    plan_payload = {
        "output_root": str(output),
        "ordered_utc_days": [day],
        "precommit": {"path": str(precommit_path)},
        "execution_amendment": {
            "sha256": "9" * 64,
            "trace_campaign_repair_max": panel.CAMPAIGN_MAE_TRACE_MAX,
        },
        "runtime_artifacts": {},
        "control_sources": {
            "path": str(tmp_path / "control-panel.json"),
            "sha256": "a" * 64,
            "panel_identity_sha256": "f" * 64,
            "v9_model_bundle_identity_sha256": "a" * 64,
            "operational_config": {
                "path": str(config_path),
                "sha256": _sha(config_path),
                "size_bytes": config_path.stat().st_size,
            },
        },
        "days": [
            {
                "utc_day": day,
                "window": {
                    "path": str(window_path),
                    "sha256": _sha(window_path),
                },
                "control_component": {},
                "candidate_overlay": {"overlay_dir": str(tmp_path / "candidate")},
                "daily_source_identity_sha256": "b" * 64,
            }
        ],
    }
    fake_plan = {
        "plan_identity_sha256": "c" * 64,
        "identity_payload": plan_payload,
    }
    monkeypatch.setattr(panel, "validate_execution_plan", lambda _, **__: fake_plan)
    control = SimpleNamespace(
        utc_day=day,
        ml_data=(np.asarray([1], dtype=np.int64),),
        identity_sha256="d" * 64,
    )
    candidate = SimpleNamespace(
        utc_day=day,
        ml_data=(np.asarray([1], dtype=np.int64),),
        overlay_identity_sha256="e" * 64,
    )
    monkeypatch.setattr(
        panel.control_repair,
        "load_admitted_control_schedule",
        lambda *_, **__: control,
    )
    monkeypatch.setattr(
        panel.candidate_abi,
        "load_admitted_one_second_overlay",
        lambda *_, **__: candidate,
    )
    window = SimpleNamespace(ml_data=None, book_source_authority="native_formal_lifecycle")
    calls: list[tuple[object, object]] = []

    def dual_run(**kwargs):
        calls.append((kwargs["control_schedule"].ml_data, kwargs["candidate_schedule"].ml_data))
        return {
            "identity": {"both_arms_ml_enabled": True},
            "arms": {arm: _empty_result() for arm in panel.ARMS},
        }

    monkeypatch.setattr(panel.dual_abi, "run_dual_overlay_tick_replay", dual_run)
    loaded_configs: list[Path] = []

    def load_params(path: Path) -> dict[str, object]:
        loaded_configs.append(path)
        return {
            "order_size": 0.001,
            "ml_enabled": True,
            "trace_campaign_repair_max": panel.CAMPAIGN_MAE_TRACE_MAX,
        }

    first = panel.execute_day(
        tmp_path / "plan.json",
        day=day,
        execution_amendment_path=tmp_path / "amendment.json",
        base_params_loader=load_params,
        window_loader=lambda _: window,
        simulate=lambda *_, **__: _empty_result(),
        allow_test_only_candidate=True,
    )
    second = panel.execute_day(
        tmp_path / "plan.json",
        day=day,
        execution_amendment_path=tmp_path / "amendment.json",
        base_params_loader=lambda _: {
            "order_size": 0.001,
            "ml_enabled": True,
            "trace_campaign_repair_max": panel.CAMPAIGN_MAE_TRACE_MAX,
        },
        window_loader=lambda _: window,
        allow_test_only_candidate=True,
    )
    assert first["reused"] is False
    assert second["reused"] is True
    assert len(calls) == 1
    assert loaded_configs == [config_path.resolve()]
    assert mutable_alias.resolve() not in loaded_configs
    assert (output / "days" / day / panel.DAY_SUCCESS).is_file()
    assert not any((output / ".staging").glob("*"))


def _synthetic_daily() -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    campaigns = []
    for index in range(panel.EXPECTED_DAY_COUNT):
        day = f"2026-05-{index + 1:02d}" if index < 31 else f"2026-06-{index - 30:02d}"
        for arm, scale in ((panel.ARMS[0], 1.0), (panel.ARMS[1], 1.01)):
            rows.append(
                {
                    "day": day,
                    "arm": arm,
                    "terminal_mtm_pnl_usdc": scale,
                    "closed_campaign_value_usdc": scale,
                    "negative_campaign_terminal_value_usdc": -1.0 / scale,
                    "fills_total": 100 if arm == panel.ARMS[0] else 82,
                    "abs_inventory_time_btc_s": 10.0 / scale,
                    "max_inventory_btc": 0.003 / scale,
                    "final_inventory_btc": 0.0,
                    "buy_maker_value_30s_bps": 0.1,
                    "sell_maker_value_30s_bps": 0.1,
                    "campaign_mae_usdc": None,
                    "campaign_mae_cpp_python_fill_path_mismatch_count": 0,
                    "repair_event_rate": 0.9,
                    "mean_closed_repair_time_s": 10.0,
                    "campaign_accounting_error_usdc": 0.0,
                    "campaign_q10_usdc": scale,
                    "campaign_cvar10_usdc": scale,
                    "multi_level_long_negative_value_usdc": -1.0 / scale,
                    "multi_level_short_negative_value_usdc": -1.0 / scale,
                    "metric_blockers": ["campaign_mae_not_emitted_by_authoritative_replay"],
                }
            )
            for campaign_index in range(5):
                campaigns.append(
                    {
                        "day": day,
                        "arm": arm,
                        "campaign_index": campaign_index,
                        "closed": True,
                        "terminal_value_usdc": scale,
                    }
                )
    return pd.DataFrame(rows), pd.DataFrame(campaigns)


def test_owner_route_preserves_raw_failures_and_only_relaxes_fill_band() -> None:
    daily, campaigns = _synthetic_daily()
    precommit = panel.DEFAULT_PRECOMMIT.resolve()
    fake_plan = {
        "plan_identity_sha256": "1" * 64,
        "identity_payload": {
            "execution_amendment": {"sha256": "9" * 64},
            "precommit": {"path": str(precommit), "sha256": _sha(precommit)},
            "candidate_panel": {"panel_identity_sha256": "2" * 64},
            "control_sources": {"sha256": "3" * 64},
        },
    }
    raw, owner, metrics = panel._score_panel(daily, campaigns, plan=fake_plan)
    assert raw["ranking_score"] is None
    assert "campaign_mae_not_emitted_by_authoritative_replay" in raw["validity"]["failures"]
    assert owner["fill_retention"] == pytest.approx(0.82)
    assert owner["only_allowed_override"] == "fills_retention_0.80_to_1.20"
    assert owner["raw_action_alpha_v2_scorecard_preserved"] is True
    assert owner["owner_progression_eligible"] is False
    assert "continuous_71_day_confirmation_not_part_of_this_runner" in owner["failures"]
    assert metrics["metric_blockers"] == ["campaign_mae_not_emitted_by_authoritative_replay"]


def test_owner_risk_gate_reads_campaign_mae_lcb() -> None:
    daily, campaigns = _synthetic_daily()
    daily["metric_blockers"] = [[] for _ in range(len(daily))]
    daily["campaign_mae_usdc"] = np.where(daily["arm"].eq(panel.ARMS[0]), -1.0, -0.5)
    precommit = panel.DEFAULT_PRECOMMIT.resolve()
    fake_plan = {
        "plan_identity_sha256": "1" * 64,
        "identity_payload": {
            "execution_amendment": {"sha256": "9" * 64},
            "precommit": {"path": str(precommit), "sha256": _sha(precommit)},
            "candidate_panel": {"panel_identity_sha256": "2" * 64},
            "control_sources": {"sha256": "3" * 64},
        },
    }
    raw, owner, metrics = panel._score_panel(daily, campaigns, plan=fake_plan)
    mae = metrics["paired_metrics"]["campaign_mae_avoidance"]
    assert mae["estimate"] == pytest.approx(0.5)
    assert mae["lower_bound"] == pytest.approx(0.5)
    assert owner["additional_gates"]["campaign_mae_avoidance_lcb_nonnegative"] is True
    assert not any(
        failure == "missing_score_metric:campaign_mae_avoidance"
        for failure in raw["validity"]["failures"]
    )


def test_control_input_rejects_predecessor_source_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        panel.control_repair,
        "validate_panel",
        lambda _: {"identity": "predecessor_source_plan"},
    )
    with pytest.raises(panel.NativeFullPathABError, match="successor panel identity drift"):
        panel._validate_control_sources(Path("old-source-plan.json"), expected_days=["2026-04-17"])


def test_read_panel_requires_and_validates_atomic_panel_admission(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    day = "2026-04-17"
    output_root = tmp_path / "panel"
    day_dir = output_root / "days" / day
    day_dir.mkdir(parents=True)
    day_manifest = day_dir / "manifest.json"
    day_manifest.write_text('{"day":"2026-04-17"}\n', encoding="utf-8")
    artifacts: dict[str, dict[str, object]] = {}
    for name in ("daily", "campaigns", "fills", "raw_scorecard", "owner_route"):
        path = output_root / f"{name}.bin"
        path.write_bytes(name.encode("ascii"))
        artifacts[name] = {
            "path": str(path),
            "sha256": _sha(path),
            "size_bytes": path.stat().st_size,
        }
    report_path = output_root / "report.json"
    report = {
        "schema_version": panel.PANEL_SCHEMA_VERSION,
        "identity": panel.IDENTITY,
        "execution_amendment_sha256": "2" * 64,
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")
    artifacts["report"] = {
        "path": str(report_path),
        "sha256": _sha(report_path),
        "size_bytes": report_path.stat().st_size,
    }
    manifest = {
        "schema_version": panel.PANEL_SCHEMA_VERSION,
        "identity": panel.IDENTITY,
        "plan_identity_sha256": "1" * 64,
        "execution_amendment_sha256": "2" * 64,
        "days": [{"utc_day": day, "manifest_sha256": _sha(day_manifest)}],
        **artifacts,
    }
    manifest_path = output_root / panel.PANEL_MANIFEST
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (output_root / panel.PANEL_SUCCESS).write_text(_sha(manifest_path) + "\n", encoding="ascii")
    monkeypatch.setattr(
        panel,
        "validate_execution_plan",
        lambda *_args, **_kwargs: {
            "plan_identity_sha256": "1" * 64,
            "identity_payload": {
                "output_root": str(output_root),
                "ordered_utc_days": [day],
                "execution_amendment": {"sha256": "2" * 64},
            },
        },
    )
    monkeypatch.setattr(panel, "_validate_day_admission", lambda *_args, **_kwargs: {"ok": True})

    loaded = panel.read_panel(
        tmp_path / "plan.json",
        execution_amendment_path=tmp_path / "amendment.json",
    )
    assert loaded["panel_admission_validated"] is True
    assert loaded["identity"] == panel.IDENTITY

    report_path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(panel.NativeFullPathABError, match="panel report SHA256 drift"):
        panel.read_panel(
            tmp_path / "plan.json",
            execution_amendment_path=tmp_path / "amendment.json",
        )


def test_finalize_reuses_an_admitted_panel_without_rewriting(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "panel"
    output_root.mkdir()
    (output_root / panel.PANEL_SUCCESS).write_text("admitted\n", encoding="ascii")
    monkeypatch.setattr(
        panel,
        "validate_execution_plan",
        lambda *_args, **_kwargs: {"identity_payload": {"output_root": str(output_root)}},
    )
    expected = {"panel_admission_validated": True}
    monkeypatch.setattr(panel, "read_panel", lambda *_args, **_kwargs: expected)

    assert (
        panel.finalize_panel(
            tmp_path / "plan.json",
            execution_amendment_path=tmp_path / "amendment.json",
        )
        == expected
    )
