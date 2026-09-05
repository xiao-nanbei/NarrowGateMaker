import gzip
import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f01_fixed_parameter_racing import (
    campaign_outcome_replay_audit as campaign_audit,
)
from research.families.f01_fixed_parameter_racing.audit.paired_screening import (
    RANKING_AUTHORITY,
    screen_paired_daily_arms,
)
from research.families.f01_fixed_parameter_racing.campaign_outcome_replay_audit import (
    _load_arm_spec_json,
    _load_initial_live_states_json,
    _load_initial_states_from_trades_csv,
)
from research.families.f01_fixed_parameter_racing.campaign_outcome_replay_audit import (
    main as campaign_audit_main,
)
from research.families.f01_fixed_parameter_racing.parameter_selection import (
    build_paired_daily_evidence,
    constraint_score_rollup,
    coverage_rows,
    paired_daily_selection,
)


def test_load_external_arm_spec_json(tmp_path: Path):
    path = tmp_path / "arms.json"
    path.write_text(
        json.dumps(
            {
                "arms": [
                    {
                        "name": "kr_probe",
                        "group": "spread",
                        "overrides": {"kappa_ratio": 1.25},
                        "note": "probe",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    arms = _load_arm_spec_json(path)
    assert len(arms) == 1
    assert arms[0].name == "kr_probe"
    assert arms[0].overrides["kappa_ratio"] == 1.25


def test_campaign_audit_rejects_disabled_fill_trace():
    with pytest.raises(SystemExit, match="requires --trace-fills-max > 0"):
        campaign_audit_main(
            [
                "--days",
                "2026-01-01",
                "--arms",
                "baseline",
                "--trace-fills-max",
                "0",
            ]
        )


def _runtime_fifo_params():
    return {
        "order_transport": "rest",
        "async_order_lanes_enabled": True,
        "cross_side_order_lanes_enabled": False,
        "async_order_lane_capacity": 3,
        "rest_gateway_timing_mode": "sampled_async_fifo",
    }


def _runtime_calibration_stub(monkeypatch):
    params = {
        **_runtime_fifo_params(),
        "replay_purpose": "diagnostic",
        "replay_event_clock": "merged",
        "replay_main_loop_sleep_ms": 100,
        "_serial_rest_return_samples_by_operation": {
            "new": [[1.0, 4.0, 6.0], [2.0, 15.0, 1200.0]],
            "cancel": [[1.0, 5.0, 7.0]],
        },
        "_serial_rest_return_sample_semantics": "observed paired samples; explicit proxy",
    }
    del params["async_order_lane_capacity"]
    calibration = {
        "source": {"path": "synthetic.json", "sha256": "synthetic-test-only"},
        "sample_counts": {"new": 2, "cancel": 1},
        "compute": {"consumed_by_replay": False},
        "limitations": ["Gateway only; compute and source clocks remain uncalibrated."],
    }
    calls = []

    def load(path, *, effective_time_assumption, bulk_cancel_model="unmodeled"):
        calls.append((path, effective_time_assumption, bulk_cancel_model))
        return {"params": params, "calibration": calibration}

    monkeypatch.setattr(campaign_audit, "load_runtime_timing_samples", load)
    return calls, calibration


@pytest.mark.parametrize(("key", "value"), [
    ("order_transport", "websocket_api_ab"),
    ("async_order_lanes_enabled", False),
    ("cross_side_order_lanes_enabled", True),
    ("cross_side_order_lanes_enabled", None),
    ("async_order_lane_capacity", 0),
    ("async_order_lane_capacity", True),
    ("async_order_lane_capacity", 3.0),
    ("async_order_lane_capacity", None),
])
def test_campaign_runtime_timing_does_not_override_transport(monkeypatch, key, value):
    calls, _ = _runtime_calibration_stub(monkeypatch)
    base = _runtime_fifo_params()
    base[key] = value
    before = dict(base)
    with pytest.raises(ValueError, match="configured"):
        campaign_audit._apply_runtime_timing_samples(
            base, Path("samples.json"), effective_time_assumption="dispatch"
        )
    assert calls == []
    assert base == before


@pytest.mark.parametrize("key", [
    "replay_main_loop_sleep_ms", "replay_event_clock", "replay_promotion_eligible",
    "replay_evidence_scope", "latency_seed", "latency_baseline_clip_quantile", "rng_seed",
    "async_order_lane_capacity", "rest_gateway_timing_mode", "order_transport",
    "_serial_rest_return_samples_by_operation", "_serial_rest_http_result_status_by_operation",
    "_serial_rest_return_sample_semantics", "_decision_to_gateway_latency_samples_ms",
    "_exec_book_visibility_paired_delay_ms", "exec_depth_visibility_source_offset_ms",
    "_pre_snapshot_compute_latency_samples_ms", "_main_loop_work_samples_ms",
    "_requote_tail_work_samples_ms", "_empirical_requote_ts_ms",
    "_bulk_cancel_timing_samples_ms", "_bulk_cancel_timing_sample_semantics",
])
def test_campaign_runtime_timing_arms_cannot_replace_environment(monkeypatch, key):
    _runtime_calibration_stub(monkeypatch)
    base = _runtime_fifo_params()
    before = dict(base)
    arm = campaign_audit.smoke.SmokeArm(
        name="candidate", group="test", note="synthetic", overrides={key: 1},
    )
    with pytest.raises(ValueError, match="changes bound environment fields"):
        campaign_audit._apply_runtime_timing_samples(
            base, Path("samples.json"), effective_time_assumption="dispatch", arms=[arm],
        )
    assert base == before


def test_campaign_runtime_timing_cli_checks_arm_environment_before_replay(monkeypatch):
    _runtime_calibration_stub(monkeypatch)
    monkeypatch.setattr(campaign_audit.bt, "configure_symbol", lambda *_a, **_k: None)
    monkeypatch.setattr(
        campaign_audit, "load_tick_base_params", lambda **_k: _runtime_fifo_params()
    )
    arm = campaign_audit.smoke.SmokeArm(
        name="baseline", group="test", note="synthetic", overrides={
            "replay_main_loop_sleep_ms": 500,
            "_serial_rest_return_samples_by_operation": {
                "new": [[0.0, 1.0, 2.0]], "cancel": [[0.0, 1.0, 2.0]],
            },
            "_serial_rest_http_result_status_by_operation": {},
        },
    )
    monkeypatch.setattr(campaign_audit, "_arm_map", lambda: {"baseline": arm})
    monkeypatch.setattr(
        campaign_audit, "_run_day_campaign_audit", lambda **_k: pytest.fail("replay started")
    )
    with pytest.raises(ValueError, match="changes bound environment fields"):
        campaign_audit_main([
            "--days", "2026-01-01", "--arms", "baseline",
            "--replay-purpose", "diagnostic", "--config", "explicit.yaml",
            "--runtime-timing-samples", "samples.json",
            "--runtime-effective-time-assumption", "dispatch",
        ])


@pytest.mark.parametrize(("extra", "message"), [
    (["--replay-purpose", "formal"], "requires --replay-purpose diagnostic"),
    (["--replay-purpose", "live_alignment"], "requires --replay-purpose diagnostic"),
    (["--engine", "cpp"], "requires --replay-purpose diagnostic"),
    (["--live-perf-telemetry", "old.csv"], "cannot be combined"),
    (["--exec-book-visibility-profile", "snapshot.csv"], "cannot be combined"),
    (["--latency-baseline-clip-quantile", "0.99"], "without clipping"),
    (["--latency-scenario", "stress"], "unchanged empirical rows"),
])
def test_campaign_runtime_timing_cli_rejects_mixed_models(extra, message):
    with pytest.raises(SystemExit, match=message):
        campaign_audit_main([
            "--days", "2026-01-01", "--arms", "baseline",
            "--replay-purpose", "diagnostic", "--config", "explicit.yaml",
            "--runtime-timing-samples", "samples.json",
            "--runtime-effective-time-assumption", "dispatch", *extra,
        ])


@pytest.mark.parametrize("missing", ["config", "assumption", "samples"])
def test_campaign_runtime_timing_cli_requires_explicit_inputs(missing):
    args = ["--days", "2026-01-01", "--replay-purpose", "diagnostic"]
    if missing != "config":
        args.extend(["--config", "explicit.yaml"])
    if missing != "assumption":
        args.extend(["--runtime-effective-time-assumption", "dispatch"])
    if missing != "samples":
        args.extend(["--runtime-timing-samples", "samples.json"])
    with pytest.raises(SystemExit, match="requires --"):
        campaign_audit_main(args)


def test_campaign_runtime_bulk_cli_requires_runtime_samples():
    with pytest.raises(SystemExit, match="bulk-cancel-model requires --runtime-timing-samples"):
        campaign_audit_main([
            "--days", "2026-01-01", "--replay-purpose", "diagnostic",
            "--runtime-bulk-cancel-model", "matched_risk_case",
        ])


@pytest.mark.parametrize("args", [[], ["--config", "original.yaml"],
                                 ["--replay-purpose", "diagnostic"]])
def test_replay_locator_cli_requires_diagnostic_and_explicit_config(args):
    with pytest.raises(SystemExit, match="requires diagnostic and explicit --config"):
        campaign_audit_main([
            "--days", "2026-01-01", "--replay-locator-projection", "locators.json", *args,
        ])


def test_replay_locator_cli_forwards_projection_without_starting_replay(monkeypatch):
    monkeypatch.setattr(campaign_audit.bt, "configure_symbol", lambda *_a, **_k: None)
    captured = {}

    class ConfigNotRead(Exception):
        pass

    def load(**kwargs):
        captured.update(kwargs)
        raise ConfigNotRead

    monkeypatch.setattr(campaign_audit, "load_tick_base_params", load)
    with pytest.raises(ConfigNotRead):
        campaign_audit_main([
            "--days", "2026-01-01", "--arms", "baseline",
            "--replay-purpose", "diagnostic", "--config", "original.yaml",
            "--replay-locator-projection", "locators.json",
        ])
    assert captured["config_path"] == Path("original.yaml")
    assert captured["locator_projection_path"] == Path("locators.json")


@pytest.mark.parametrize("key", ["model_dir", "resolved_model_dir",
                                 "buy_e3_cooldown_policy_path"])
def test_replay_locator_cli_rejects_arm_path_overrides(monkeypatch, key):
    monkeypatch.setattr(campaign_audit.bt, "configure_symbol", lambda *_a, **_k: None)
    arm = campaign_audit.smoke.SmokeArm(
        name="baseline", group="test", note="synthetic", overrides={key: "/other"},
    )
    monkeypatch.setattr(campaign_audit, "_arm_map", lambda: {"baseline": arm})
    with pytest.raises(ValueError, match="changes model/policy locations"):
        campaign_audit_main([
            "--days", "2026-01-01", "--arms", "baseline",
            "--replay-purpose", "diagnostic", "--config", "original.yaml",
            "--replay-locator-projection", "locators.json",
        ])


@pytest.mark.parametrize("overrides", [
    {}, {"fill_cooldown": 85.0, "replace_terminal_continuation": False, "order_size": 0.002},
])
@pytest.mark.parametrize("bulk_model", ["unmodeled", "matched_risk_case"])
def test_campaign_runtime_timing_cli_reaches_runner_with_pairs_and_limits(
    monkeypatch, tmp_path, overrides, bulk_model,
):
    calls, calibration = _runtime_calibration_stub(monkeypatch)
    monkeypatch.setattr(campaign_audit.bt, "configure_symbol", lambda *_a, **_k: None)
    monkeypatch.setattr(
        campaign_audit, "load_tick_base_params", lambda **_k: _runtime_fifo_params()
    )
    arm = campaign_audit.smoke.SmokeArm(
        name="baseline", group="test", note="synthetic", overrides=overrides,
    )
    monkeypatch.setattr(campaign_audit, "_arm_map", lambda: {"baseline": arm})

    class ReplayNotStarted(Exception):
        pass

    captured = {}

    def stop_before_replay(**kwargs):
        captured.update(kwargs)
        raise ReplayNotStarted

    monkeypatch.setattr(campaign_audit, "_run_day_campaign_audit", stop_before_replay)
    monkeypatch.setenv("MM_RESULTS_DIR", str(tmp_path))
    with pytest.raises(ReplayNotStarted):
        campaign_audit_main([
            "--days", "2026-01-01", "--arms", "baseline",
            "--replay-purpose", "diagnostic", "--config", "explicit.yaml",
            "--runtime-timing-samples", "samples.json",
            "--runtime-effective-time-assumption", "observable_upper_bound",
            "--runtime-bulk-cancel-model", bulk_model,
        ])
    base = captured["base"]
    assert calls == [(Path("samples.json"), "observable_upper_bound", bulk_model)]
    assert base["async_order_lane_capacity"] == 3
    assert base["replay_evidence_scope"] == "runtime_gateway_only_diagnostic"
    assert base["replay_promotion_eligible"] is False
    assert base["replay_event_clock"] == "merged"
    assert base["latency_baseline_clip_quantile"] == 1.0
    assert base["_serial_rest_return_samples_by_operation"]["new"][1] == [2, 15, 1200]
    assert captured["arms"][0].overrides == overrides
    assert not any(key.startswith("_decision_to_gateway") for key in base)
    assert not any(key.startswith("_exec_book_visibility_delay_samples") for key in base)
    output = tmp_path / "diagnostic.md"
    campaign_audit._write_markdown(output, pd.DataFrame(), pd.DataFrame(), {
        "tag": "synthetic", "symbol": "BTCUSDC", "days": [], "arms": [],
        "runtime_timing_calibration": calibration,
    })
    rendered = output.read_text()
    assert "not a complete current-live baseline" in rendered
    assert calibration["limitations"][0] in rendered


def test_load_initial_states_supports_gzip_live_ledger(tmp_path: Path):
    path = tmp_path / "trades.csv.gz"
    with gzip.open(path, mode="wt", newline="") as handle:
        handle.write(
            "timestamp,side,trade_type,qty,price,commission,position,avg_entry,"
            "realized_pnl,unrealized_pnl,state\n"
            "1767225599.0,SELL,OPEN,0.001,100.0,0,-0.004,99.5,0,0,OPEN\n"
        )
    states = _load_initial_states_from_trades_csv(path, ["2026-01-01"])
    assert states["2026-01-01"] == {
        "initial_inventory": -0.004,
        "initial_entry_price": 99.5,
    }


def test_load_initial_live_state_requires_every_requested_day(tmp_path: Path):
    path = tmp_path / "initial-live-state.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "narrowgate.live_replay_initial_state.v1",
                "days": {
                    "2026-01-01": {
                        "initial_inventory": -0.004,
                        "active_orders": [{"side": "BUY", "price": 99.0}],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    states = _load_initial_live_states_json(path, ["2026-01-01"])
    assert states["2026-01-01"]["initial_inventory"] == -0.004
    with pytest.raises(ValueError, match="2026-01-02"):
        _load_initial_live_states_json(path, ["2026-01-01", "2026-01-02"])


def test_constraint_score_penalizes_non_live_like_fill_cut():
    rollup = pd.DataFrame(
        [
            {
                "arm": "baseline",
                "fills_total": 1000,
                "replay_abs_inventory_time_s_sum": 100.0,
                "decision_pause_rate": 0.08,
                "decision_keep_rate": 0.15,
                "loss_tail": 10,
                "bad_campaign_rate": 0.35,
                "terminal_pnl_sum": -100.0,
                "replay_pnl_sum": -120.0,
                "replay_inv_adj_sum": -20.0,
                "buy_fill_share": 0.50,
                "sell_fill_share": 0.50,
            },
            {
                "arm": "looks_good_but_kills_fills",
                "fills_total": 600,
                "replay_abs_inventory_time_s_sum": 90.0,
                "decision_pause_rate": 0.09,
                "decision_keep_rate": 0.16,
                "loss_tail": 8,
                "bad_campaign_rate": 0.34,
                "terminal_pnl_sum": -50.0,
                "replay_pnl_sum": -60.0,
                "replay_inv_adj_sum": -10.0,
                "buy_fill_share": 0.50,
                "sell_fill_share": 0.50,
            },
        ]
    )
    scored = constraint_score_rollup(rollup)
    row = scored[scored["arm"] == "looks_good_but_kills_fills"].iloc[0]
    assert not bool(row["hard_gate_pass"])
    assert "fills_retention_lt_85pct" in row["constraint_notes"]


def test_config_coverage_has_no_unknown_for_current_live_config():
    rows = coverage_rows(Path(__file__).resolve().parents[1] / "live" / "config.yaml")
    assert rows
    assert not [row for row in rows if row["category"] == "unclassified"]


def test_paired_daily_selection_rebases_to_current_live_and_rejects_fill_kill():
    rows = []
    for day_idx in range(24):
        day = f"2026-01-{day_idx + 1:02d}"
        common = {
            "day": day,
            "group": "test",
            "campaigns": 10,
            "bad_campaigns": 4,
            "repaired_campaigns": 6,
            "loss_tail": 1 if day_idx % 8 == 0 else 0,
            "fills_bid_buy": 50,
            "fills_ask_sell": 50,
            "decision_total": 1000,
            "decision_place_count": 20,
            "decision_replace_count": 700,
            "decision_keep_count": 160,
            "decision_pause_count": 120,
            "replay_abs_inventory_time_s": 100.0,
            "replay_campaign_max_adverse_excursion": -1.0,
            "early_20m_drawdown_mean": 1.0,
            "duration_mean_s": 100.0,
            "replay_avg_final_spread": 60.0,
            "replay_n_final_spread": 1000,
        }
        rows.append(
            {
                **common,
                "arm": "current_live",
                "replay_pnl": -1.0,
                "terminal_pnl_sum": -0.8,
                "replay_inv_adj": -0.2,
                "fills_total": 100,
            }
        )
        rows.append(
            {
                **common,
                "arm": "strict_better",
                "replay_pnl": -0.5,
                "terminal_pnl_sum": -0.4,
                "replay_inv_adj": -0.1,
                "replay_abs_inventory_time_s": 95.0,
                "fills_total": 100,
            }
        )
        rows.append(
            {
                **common,
                "arm": "looks_good_but_kills_fills",
                "replay_pnl": 1.0,
                "terminal_pnl_sum": 1.0,
                "replay_inv_adj": 0.0,
                "fills_total": 50,
                "fills_bid_buy": 25,
                "fills_ask_sell": 25,
            }
        )

    daily = pd.DataFrame(rows)
    evidence = build_paired_daily_evidence(daily, baseline_arm="current_live")
    assert "selection_tier" not in evidence
    assert "candidate_for_blocked_oos" not in evidence
    assert "scorecard_total_score" not in evidence
    assert "promotion_status" not in evidence

    canonical = screen_paired_daily_arms(daily, baseline_arm="current_live")
    assert "selection_tier" not in canonical
    assert "scorecard_promotion_status" not in canonical
    assert canonical["scorecard_screening_status"].eq("screening_rank_only").any()
    assert canonical["scorecard_profile_id"].eq("paired_screen_v2").all()
    assert canonical["ranking_authority"].eq(RANKING_AUTHORITY).all()
    assert not canonical["promotion_authority"].any()
    eligible_scores = canonical.loc[
        canonical["scorecard_ranking_eligible"], "scorecard_ranking_score"
    ].tolist()
    assert eligible_scores == sorted(eligible_scores, reverse=True)

    with pytest.warns(DeprecationWarning, match="compatibility-only"):
        selected = paired_daily_selection(daily, baseline_arm="current_live")
    baseline = selected.loc[selected["arm"] == "current_live"].iloc[0]
    better = selected.loc[selected["arm"] == "strict_better"].iloc[0]
    fill_kill = selected.loc[selected["arm"] == "looks_good_but_kills_fills"].iloc[0]

    assert baseline["selection_tier"] == "baseline"
    assert better["selection_tier"] == "strict_candidate"
    assert bool(better["candidate_for_blocked_oos"])
    assert better["activity_adjusted_raw_delta"] > 0.0
    assert better["campaign_adjusted_terminal_delta"] > 0.0
    assert bool(better["unit_quality_candidate"])
    assert better["unit_quality_notes"] == "pass"
    assert not bool(fill_kill["mechanism_pass"])
    assert not bool(fill_kill["unit_quality_candidate"])
    assert "fills_retention_lt_85pct" in fill_kill["unit_quality_notes"]
    assert "fills_outside_direction_budget" in fill_kill["mechanism_notes"]
    assert better["scorecard_profile_id"] == "paired_screen_v2"
    assert bool(better["scorecard_gate_pass"])
    assert better["scorecard_promotion_status"] == "screening_rank_only"
    assert better["scorecard_total_score"] > baseline["scorecard_total_score"]
    assert not bool(fill_kill["scorecard_gate_pass"])
    assert len(str(better["scorecard_sha256"])) == 64
    assert bool(better["selection_tier_compatibility_only"])
    assert not bool(better["candidate_for_blocked_oos_promotion_authority"])
