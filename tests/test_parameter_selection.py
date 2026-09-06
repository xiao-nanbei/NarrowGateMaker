import gzip
import json
from pathlib import Path

import numpy as np
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


def test_campaign_funding_uses_exchange_fill_time_and_preserves_trade_count():
    start_ms = 1_780_000_000_000
    trace = [
        {"fill_sequence": 0, "fill_ts": start_ms + 10, "side": "BUY",
         "fill_qty": 1, "quote_px": 100, "fill_fee_usdc": 0,
         "private_fill_visible_ts_ms": start_ms + 500},
        {"fill_sequence": 1, "fill_ts": start_ms + 30, "side": "SELL",
         "fill_qty": 1, "quote_px": 110, "fill_fee_usdc": 0},
    ]
    funding = [{"fundingTime": start_ms + 20, "markPrice": "105", "fundingRate": ".01"}]
    payments = []
    rows = campaign_audit._fills_to_trade_rows(
        trace, day_start_ts=start_ms / 1000, terminal_ts=(start_ms + 1000) / 1000,
        terminal_mark_price=110, funding_events=funding, funding_trace=payments,
    )
    assert payments[0]["position_btc"] == 1
    assert payments[0]["funding_cashflow_usdc"] == pytest.approx(-1.05)
    assert sum(row.trade_type == "FILL" for row in rows) == 2
    assert rows[-1].realized_pnl + rows[-1].unrealized_pnl == pytest.approx(8.95)
    labels = campaign_audit.campaign_label_rows(campaign_audit.build_campaigns(rows))
    assert sum(float(row["final_total_pnl_delta"]) for row in labels) == pytest.approx(8.95)


def test_campaign_funding_equal_ms_is_explicit_and_empty_fills_still_settle():
    ms = 1_780_000_000_000
    payment = {"fundingTime": ms, "markPrice": "100", "fundingRate": ".01"}
    trace = []
    rows = campaign_audit._fills_to_trade_rows(
        [{"fill_sequence": 0, "fill_ts": ms, "side": "BUY", "fill_qty": 1, "quote_px": 100}],
        day_start_ts=ms / 1000, terminal_ts=ms / 1000 + 1,
        terminal_mark_price=100, funding_events=[payment], funding_trace=trace,
    )
    assert trace[0]["funding_cashflow_usdc"] == 0
    assert trace[0]["same_ms_fill_count"] == 1
    assert rows[-1].position == 1
    rows = campaign_audit._fills_to_trade_rows(
        [], initial_inventory=-1, initial_entry_price=100,
        day_start_ts=ms / 1000, terminal_ts=ms / 1000 + 1,
        terminal_mark_price=100, funding_events=[payment],
    )
    assert rows[-1].realized_pnl + rows[-1].unrealized_pnl == pytest.approx(1)


@pytest.mark.parametrize("change", ["duplicate", "symbol", "mark"])
def test_funding_history_rejects_invalid_records(tmp_path, change):
    row = {"symbol": "BTCUSDC", "fundingTime": 100, "markPrice": 100, "fundingRate": .01}
    rows = [row, dict(row)] if change == "duplicate" else [row]
    if change == "symbol":
        row["symbol"] = "BTCUSDT"
    if change == "mark":
        row["markPrice"] = 0
    path = tmp_path / "funding.json"
    path.write_text(json.dumps(rows))
    with pytest.raises(ValueError):
        campaign_audit._load_funding_history(path, symbol="BTCUSDC")


def test_continuous_campaign_runner_invokes_one_state_machine_per_arm(monkeypatch):
    days = ["2026-01-01", "2026-01-02"]
    monkeypatch.setattr(campaign_audit.bt, "configure_symbol", lambda *_a, **_kw: None)
    windows = []

    def load(day, params):
        ms = int(campaign_audit._day_start_ts(day) * 1000)
        window = {"trades": pd.DataFrame({"transact_time": [ms], "price": [100.]}),
                  "var_ts_ms": np.array([ms]), "var_ssq": np.array([1.]),
                  "var_ti": None, "var_retsq": None, "bbo_data": None,
                  "l2_data": None, "ml_data": None}
        windows.append(window)
        return window

    calls = []

    def simulate(engine, trades, var_ts, var_ssq, params, **kwargs):
        calls.append((trades, params))
        return {"pnl": 3., "_fill_trace": []}

    monkeypatch.setattr(campaign_audit.smoke, "_load_window", load)
    monkeypatch.setattr(campaign_audit.bt, "_simulate_tick_with_engine", simulate)
    monkeypatch.setattr(campaign_audit, "build_configured_cooldown_policy_adapter",
                        lambda **_kw: None)
    arms = [campaign_audit.smoke.SmokeArm(name, "test", {}, "") for name in ("B", "E")]
    result = campaign_audit._run_day_campaign_audit(
        day=days[0], continuous_days=days, symbol="BTCUSDC", base={}, arms=arms,
        engine="python", day_initial={}, day_live_state=None, use_initial_state=False,
    )
    assert len(windows) == 2  # shared market data; no per-arm reload
    assert len(calls) == 2  # not four independent daily simulations
    assert calls[0][0] is calls[1][0]
    assert calls[0][1] is not calls[1][1]
    assert calls[0][1]["replay_event_clock_end_ts_ms"] == int(
        (campaign_audit._day_start_ts(days[-1]) + 86400) * 1000 - 1
    )
    assert all(row["window_day_count"] == 2 for row in result["daily_rows"])
    assert all(row["accounting_window"] == "continuous_segment" for row in result["daily_rows"])


@pytest.mark.parametrize("opening_fee", [0.1, -0.1, 0.0])
def test_campaign_ledger_uses_execution_price_and_both_signed_fees(opening_fee):
    fills = [
        {"fill_sequence": 0, "fill_ts": 1000, "order_id": 9, "side": "BUY",
         "fill_qty": 1.0, "quote_px": 100.0, "fill_trade_px": 99.0,
         "fill_fee_usdc": opening_fee},
        {"fill_sequence": 1, "fill_ts": 1001, "order_id": 1, "side": "SELL",
         "fill_qty": 1.0, "quote_px": 102.0, "fill_trade_px": 103.0,
         "fill_fee_usdc": 0.2},
    ]
    rows = campaign_audit._fills_to_trade_rows(fills)
    expected = 2.0 - opening_fee - 0.2
    assert [row.price for row in rows] == [100.0, 102.0]
    assert rows[-1].realized_pnl == pytest.approx(expected)
    campaign, = campaign_audit.build_campaigns(rows)
    assert campaign.closed
    assert campaign.final_total_pnl - campaign.start_total_pnl == pytest.approx(expected)


def test_campaign_ledger_splits_flip_and_preserves_execution_sequence():
    fills = [
        {"fill_sequence": 2, "fill_ts": 1001, "order_id": 1, "side": "BUY",
         "fill_qty": 1.0, "quote_px": 101.0, "fill_fee_usdc": 0.1},
        {"fill_sequence": 0, "fill_ts": 1000, "order_id": 8, "side": "BUY",
         "fill_qty": 1.0, "quote_px": 100.0, "fill_fee_usdc": 0.1},
        {"fill_sequence": 1, "fill_ts": 1001, "order_id": 9, "side": "SELL",
         "fill_qty": 2.0, "quote_px": 102.0, "fill_fee_usdc": 0.4},
    ]
    rows = campaign_audit._fills_to_trade_rows(fills)
    assert [row.position for row in rows] == [1.0, 0.0, -1.0, 0.0]
    assert sum(row.fee_usdc for row in rows) == pytest.approx(0.6)
    campaigns = campaign_audit.build_campaigns(rows)
    values = [row.final_total_pnl - row.start_total_pnl for row in campaigns]
    assert values == pytest.approx([1.7, 0.7])
    assert sum(values) == pytest.approx(rows[-1].total_pnl)
    # Fill counts remain physical executions, not the expanded economic legs.
    split = campaign_audit._fill_split(fills)
    assert split["buy_exposure_fills"] == 1
    assert split["buy_reducing_fills"] == 1


def test_campaign_legacy_same_timestamp_order_is_not_sorted_by_order_id():
    fills = [
        {"fill_ts": 1000, "order_id": 8, "side": "BUY", "fill_qty": 1.0, "price": 100},
        {"fill_ts": 1000, "order_id": 1, "side": "SELL", "fill_qty": 1.0, "price": 101},
    ]
    assert [row.side for row in campaign_audit._fills_to_trade_rows(fills)] == ["BUY", "SELL"]


def test_campaign_residual_inventory_is_marked_without_terminal_liquidation():
    fills = [{"fill_sequence": 0, "fill_ts": 1000, "side": "BUY",
              "fill_qty": 1.0, "quote_px": 100.0, "fill_fee_usdc": 0.1}]
    rows = campaign_audit._fills_to_trade_rows(
        fills, terminal_ts=2000.0, terminal_mark_price=103.0,
    )
    assert rows[-1].total_pnl == pytest.approx(2.9)
    assert rows[-1].position == 1.0
    assert not rows[-1].is_real_fill
    campaign, = campaign_audit.build_campaigns(rows)
    assert not campaign.closed
    assert campaign.fills == 1
    assert campaign.end_ts == 2000.0
    assert campaign.final_total_pnl - campaign.start_total_pnl == pytest.approx(2.9)


@pytest.mark.parametrize("fee", [float("nan"), float("inf"), -float("inf")])
def test_campaign_ledger_does_not_turn_invalid_commission_into_zero(fee):
    with pytest.raises(ValueError, match="fill_fee_usdc"):
        campaign_audit._fills_to_trade_rows([
            {"fill_ts": 1000, "side": "BUY", "fill_qty": 1.0,
             "quote_px": 100.0, "fill_fee_usdc": fee},
        ])


@pytest.mark.parametrize("sequences", [[0, 0], [0, None]])
def test_campaign_ledger_rejects_ambiguous_partial_sequence(sequences):
    fills = [{"fill_sequence": value} if value is not None else {} for value in sequences]
    with pytest.raises(ValueError, match="fill_sequence"):
        campaign_audit._ordered_fill_trace(fills)


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
        "compute": {
            "consumed_by_replay": False,
            "columns": [
                "sync_check_ms", "signal_compute_ms", "compute_quotes_ms", "requote_total_ms",
            ],
            "by_signal_path": {
                "cached_no_new_bucket": [[1.0, 2.0, 3.0, 7.0]],
                "new_bucket": [[2.0, 3.0, 4.0, 11.0]],
                "catch_up": [[3.0, 4.0, 5.0, 15.0]],
            },
        },
        "limitations": ["Gateway only; compute and source clocks remain uncalibrated."],
    }
    calls = []

    def load(
        path, *, effective_time_assumption, bulk_cancel_model="unmodeled",
        private_fill_model="unmodeled",
    ):
        calls.append((path, effective_time_assumption, bulk_cancel_model, private_fill_model))
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
    "runtime_compute_clock", "runtime_compute_initial_bucket_end_ms",
    "_runtime_compute_samples_by_path", "exec_message_delivery_input_semantics",
    "_exec_message_delivery", "exec_source_stratified_profile_id",
    "_private_fill_visibility_latency_samples_ms",
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


@pytest.mark.parametrize(("flag", "value"), [
    ("--runtime-compute-clock", "source_time_assumption"),
    ("--runtime-private-fill-model", "observed_callback"),
])
def test_campaign_runtime_optional_models_require_samples(flag, value):
    with pytest.raises(SystemExit, match="requires --runtime-timing-samples"):
        campaign_audit_main([
            "--days", "2026-01-01", "--replay-purpose", "diagnostic", flag, value,
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
@pytest.mark.parametrize("compute_clock", [None, "source_time_assumption"])
@pytest.mark.parametrize("execution_profile", [False, True])
def test_campaign_runtime_timing_cli_reaches_runner_with_pairs_and_limits(
    monkeypatch, tmp_path, overrides, bulk_model, compute_clock, execution_profile,
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
    if execution_profile and compute_clock:
        compute_clock = "prediction_delivery"
    extra = ["--runtime-compute-clock", compute_clock] if compute_clock else []
    if execution_profile:
        profile_path = tmp_path / "message-profile.json"
        profile_path.write_text(json.dumps({"schema": "market_data_latency_profile.v1",
            "profile_id": "synthetic", "groups": [
                {"market_id": "binance:perp:BTCUSDC", "event_type": event_type,
                 "transport": "websocket", "rows": 1,
                 "simulation_clock_pair_columns": ["transport_lag_ms", "feature_latency_ms"],
                 "simulation_clock_pair_samples_ms": [[5.0, 1.0]],
                 "simulation_clock_pair_semantics": "all_observed_same_message_pairs"}
                for event_type in ("book", "depth", "trade")
            ]}))
        extra += ["--market-data-latency-profile", str(profile_path),
                  "--market-data-latency-mode", "profile_empirical"]
    with pytest.raises(ReplayNotStarted):
        campaign_audit_main([
            "--days", "2026-01-01", "--arms", "baseline",
            "--replay-purpose", "diagnostic", "--config", "explicit.yaml",
            "--runtime-timing-samples", "samples.json",
            "--runtime-effective-time-assumption", "observable_upper_bound",
            "--runtime-bulk-cancel-model", bulk_model,
            "--runtime-private-fill-model", "observed_callback", *extra,
        ])
    base = captured["base"]
    assert calls == [(
        Path("samples.json"), "observable_upper_bound", bulk_model, "observed_callback",
    )]
    assert base["async_order_lane_capacity"] == 3
    assert base["replay_evidence_scope"] == "runtime_gateway_diagnostic"
    assert base["replay_promotion_eligible"] is False
    assert base["replay_event_clock"] == "merged"
    assert base["latency_baseline_clip_quantile"] == 1.0
    assert base["_serial_rest_return_samples_by_operation"]["new"][1] == [2, 15, 1200]
    assert captured["arms"][0].overrides == overrides
    assert captured["runtime_compute_clock"] == compute_clock
    assert captured["runtime_compute_calibration"] is (calibration if compute_clock else None)
    assert not any(key.startswith("_decision_to_gateway") for key in base)
    assert not any(key.startswith("_exec_book_visibility_delay_samples") for key in base)
    if execution_profile:
        assert base["exec_book_visibility_mode"] == "message_schedule"
        assert base["exec_message_delivery_profile_path"] == str(profile_path)
        assert captured["market_data_latency_mode"] == "profile_empirical"
    output = tmp_path / "diagnostic.md"
    campaign_audit._write_markdown(output, pd.DataFrame(), pd.DataFrame(), {
        "tag": "synthetic", "symbol": "BTCUSDC", "days": [], "arms": [],
        "runtime_timing_calibration": calibration,
        "runtime_compute_clock": compute_clock,
    })
    rendered = output.read_text()
    assert "not a complete current-live baseline" in rendered
    assert calibration["limitations"][0] in rendered


@pytest.mark.parametrize("clock", ["prediction_delivery", "source_time_assumption"])
def test_campaign_runtime_compute_uses_actual_pre_roll(monkeypatch, clock):
    _, calibration = _runtime_calibration_stub(monkeypatch)
    source_ms = np.array([10_000, 20_000, 30_000], dtype=np.int64)
    params = {"exec_book_visibility_mode": "message_schedule", "_exec_message_delivery": {
        "prediction": {
            "exchange_ts_ns": source_ms * 1_000_000,
            # The 20s source bucket was not delivered before the 30s start.
            "feature_ready_ts_ns": np.array([11_000, 30_000, 40_000]) * 1_000_000,
        },
    }}
    adapted = campaign_audit._runtime_compute_for_window(
        {"ml_data": (source_ms,)}, params, calibration, clock=clock, start_ms=30_000,
    )
    assert adapted["runtime_compute_initial_bucket_end_ms"] == (
        10_000 if clock == "prediction_delivery" else 20_000
    )
    assert adapted["runtime_compute_clock"] == clock
    np.testing.assert_array_equal(
        adapted["_runtime_compute_samples_by_path"]["new_bucket"], [[5, 9, 2]],
    )
    assert not adapted["_runtime_compute_samples_by_path"]["new_bucket"].flags.writeable
    assert calibration["compute"]["consumed_by_replay"] is False


@pytest.mark.parametrize(("source_ms", "clock", "message"), [
    ([30_000, 40_000], "source_time_assumption", "completed prediction before"),
    ([20_001, 30_000], "source_time_assumption", "align with the bucket grid"),
    ([20_000, 10_000], "source_time_assumption", "ordered integer"),
    ([10_000, 20_000], "prediction_delivery", "prediction message schedule"),
])
def test_campaign_runtime_compute_rejects_invented_state(monkeypatch, source_ms, clock, message):
    _, calibration = _runtime_calibration_stub(monkeypatch)
    with pytest.raises(ValueError, match=message):
        campaign_audit._runtime_compute_for_window(
            {"ml_data": (np.array(source_ms),)}, {}, calibration, clock=clock, start_ms=30_000,
        )


@pytest.mark.parametrize("clock", ["source_time_assumption", "prediction_delivery"])
def test_campaign_day_runtime_compute_shared_across_arms_and_resets_per_day(monkeypatch, clock):
    _, calibration = _runtime_calibration_stub(monkeypatch)
    monkeypatch.setattr(campaign_audit.bt, "configure_symbol", lambda *_a, **_k: None)
    windows = {}

    def load(day, params):
        start = int(campaign_audit._day_start_ts(day) * 1000)
        assert params["runtime_compute_clock"] == clock
        assert params["replay_event_clock_start_ts_ms"] == start
        windows[day] = {
            "ml_data": (np.array([start - 20_000, start - 10_000, start]),),
            "trades": object(), "var_ts_ms": object(), "var_ssq": object(),
            "bbo_data": None, "l2_data": None, "var_ti": None, "var_retsq": None,
        }
        return windows[day]

    monkeypatch.setattr(campaign_audit.smoke, "_load_window", load)
    parent_loads, message_loads = [], []

    def load_parents(day, params):
        parent_loads.append(day)
        return object(), []

    def message_params(window, **kwargs):
        message_loads.append(window)
        source_ns = window["ml_data"][0] * 1_000_000
        return {"exec_book_visibility_mode": "message_schedule", "_exec_message_delivery": {
            "prediction": {"exchange_ts_ns": source_ns,
                           "feature_ready_ts_ns": source_ns + 1_000_000},
        }}

    monkeypatch.setattr(campaign_audit.data_windows, "load_replay_aggregate_parents", load_parents)
    monkeypatch.setattr(campaign_audit.data_windows, "execution_message_delivery_params", message_params)
    policy_loads = []

    def policy_adapter(*, window, params):
        if clock != "prediction_delivery":
            return None
        adapter = object()
        policy_loads.append((window, params["_exec_message_delivery"], adapter))
        return adapter

    monkeypatch.setattr(campaign_audit, "build_configured_cooldown_policy_adapter", policy_adapter)
    captures = []

    def simulate(_engine, _trades, _var_ts, _var_ssq, params, **kwargs):
        captures.append(params)
        return {
            "runtime_compute_clock": params["runtime_compute_clock"],
            "runtime_compute_path_counts": {"cached_no_new_bucket": 1, "new_bucket": 2},
            "exec_message_delivery_sources": {"depth": {"messages": 3}},
            "exec_message_missing_source_skip_count": 1,
        }

    monkeypatch.setattr(campaign_audit.bt, "_simulate_tick_with_engine", simulate)
    arms = [
        campaign_audit.smoke.SmokeArm(name=name, group="synthetic", overrides={"gamma": gamma})
        for name, gamma in (("baseline", 0.01), ("candidate", 0.02))
    ]
    base = _runtime_fifo_params()
    if clock == "prediction_delivery":
        base["exec_message_delivery_profile_path"] = "synthetic-profile.json"
    for day in ("2026-01-01", "2026-01-02"):
        result = campaign_audit._run_day_campaign_audit(
            day=day, symbol="BTCUSDC", base=base, arms=arms, engine="python",
            day_initial={}, day_live_state=None, use_initial_state=False,
            runtime_compute_calibration=calibration, runtime_compute_clock=clock,
            market_data_latency_profile_payload={} if clock == "prediction_delivery" else None,
            market_data_latency_mode="profile_empirical",
        )
        start = int(campaign_audit._day_start_ts(day) * 1000)
        for row in result["daily_rows"]:
            assert row["runtime_compute_initial_bucket_end_ms"] == start - 10_000
            assert row["runtime_compute_path_counts"]["new_bucket"] == 2
            assert row["exec_message_delivery_sources"] == {"depth": {"messages": 3}}
            assert row["exec_message_missing_source_skip_count"] == 1
            assert row["replay_evidence_scope"] == "runtime_gateway_diagnostic"
            assert "economic_pnl_complete" not in row
        left, right = captures[-2:]
        assert left is not right
        assert left["_runtime_compute_samples_by_path"] is right["_runtime_compute_samples_by_path"]
        if clock == "prediction_delivery":
            assert left["_exec_message_delivery"] is right["_exec_message_delivery"]
            assert (left["cooldown_duration_policy_evaluator"]
                    is not right["cooldown_duration_policy_evaluator"])
            assert left["cooldown_v2_snapshot_emitter"] is left["cooldown_duration_policy_evaluator"]
            assert policy_loads[-2][0] is policy_loads[-1][0]
            assert policy_loads[-2][1] is policy_loads[-1][1]
        assert left["replay_event_clock_start_ts_ms"] == start
        assert left["replay_event_clock_end_ts_ms"] == start + 86_400_000 - 1
    assert "runtime_compute_initial_bucket_end_ms" not in base
    assert len(message_loads) == len(parent_loads) == (2 if clock == "prediction_delivery" else 0)
    assert len(policy_loads) == (4 if clock == "prediction_delivery" else 0)


def test_campaign_configured_policy_runs_real_simulator_with_atomic_emitter(monkeypatch):
    from strategy.boolean_cooldown_live import LiveBooleanCooldownPolicy
    from tests.test_exec_book_visibility_delay import (
        _ReceiveTimeTestPolicy,
        _profile_execution_message_fixture,
    )

    inputs, _, _, window = _profile_execution_message_fixture()
    window.update(var_ssq=inputs["var_ssq"], var_ti=None, var_retsq=None)
    monkeypatch.setattr(campaign_audit.bt, "configure_symbol", lambda *_a, **_k: None)
    monkeypatch.setattr(campaign_audit.smoke, "_load_window", lambda *_a: window)
    monkeypatch.setattr(LiveBooleanCooldownPolicy, "from_files", lambda **_k: _ReceiveTimeTestPolicy())
    params = {**inputs["params"],
        "boolean_cooldown_policy_enabled": True,
        "boolean_cooldown_policy_path": "synthetic-policy.json",
        "boolean_cooldown_predicate_bundle_path": "synthetic-predicates.json",
        "max_exec_book_visible_age_s": 5.0,
    }
    result = campaign_audit._run_day_campaign_audit(
        day="2026-01-01", symbol="BTCUSDC", base=params,
        arms=[campaign_audit.smoke.SmokeArm(name="baseline", group="synthetic")],
        engine="python", day_initial={}, day_live_state=None, use_initial_state=False,
    )
    assert len(result["daily_rows"]) == 1


@pytest.mark.parametrize("clock", [None, "source_time_assumption", "prediction_delivery"])
@pytest.mark.parametrize("private_fill_mode", ["unmodeled", "observed_callback"])
def test_campaign_runtime_report_reflects_consumed_components(
    monkeypatch, tmp_path, clock, private_fill_mode,
):
    _, calibration = _runtime_calibration_stub(monkeypatch)
    warning = "Compute paths remain metadata; no measured compute samples are injected."
    calibration["limitations"] = [warning, "Source messages remain modeled."]
    calibration["private_fill_model"] = {"mode": private_fill_mode}
    phase = "synthetic paired sync+signal before snapshot; quote before enqueue; residual after"
    rows = [{"runtime_compute_path_counts": {"new_bucket": 2},
             "runtime_compute_phase_placement": phase}]
    report = campaign_audit._runtime_timing_report(calibration, rows, compute_clock=clock)
    assert calibration["limitations"][0] == warning
    assert calibration["compute"]["consumed_by_replay"] is False
    if clock:
        assert report["compute"]["consumed_by_replay"] is True
        assert report["compute"]["phase_placement"] == [phase]
        assert warning not in report["limitations"]
        assert any(clock in line for line in report["limitations"])
        assert any(phase in line for line in report["limitations"])
    else:
        assert report is calibration
    path = tmp_path / "runtime.md"
    campaign_audit._write_markdown(path, pd.DataFrame(), pd.DataFrame(), {
        "tag": "synthetic", "symbol": "BTCUSDC", "days": [], "arms": [],
        "runtime_timing_calibration": report, "runtime_compute_clock": clock,
    })
    text = path.read_text()
    assert ("phase-conditioned compute" in text) is bool(clock)
    assert ("observed private-fill callback visibility" in text) is (
        private_fill_mode == "observed_callback"
    )
    assert "Gateway-only" not in text


def test_campaign_runtime_report_does_not_claim_compute_without_counters(monkeypatch):
    _, calibration = _runtime_calibration_stub(monkeypatch)
    report = campaign_audit._runtime_timing_report(
        calibration, [{}], compute_clock="source_time_assumption",
    )
    assert report["compute"]["consumed_by_replay"] is False
    assert report["compute"]["phase_placement"] == []


@pytest.mark.parametrize("status", [
    "incomplete_pending_private_fills", "incomplete_unmodeled_emergency_fatal_recovery",
])
def test_campaign_preserves_incomplete_day_but_not_full_window_reward(tmp_path, status):
    arm = campaign_audit.smoke.SmokeArm(name="baseline", group="synthetic")
    rows = []
    for day, complete, pnl in (("2026-01-01", True, 2.0), ("2026-01-02", False, 3.0)):
        result = {
            "pnl": pnl, "economic_pnl_complete": complete,
            "economic_pnl_status": "complete_local_fill_ledger" if complete else status,
            "risk_emergency_ownership_conflict_count": 0 if complete else 1,
        }
        if not complete:
            result["risk_emergency_stop_reason"] = "stop_reconciliation_required"
        rows.append(campaign_audit._campaign_daily_row(
            day=day, arm=arm, result=result, label_rows=[], runtime_s=0.0,
            fill_split=campaign_audit._fill_split([], initial_inventory=0.0),
        ))
    daily = pd.DataFrame(rows)
    rollup = campaign_audit._rollup(daily)
    assert rows[-1]["economic_pnl_status"] == status
    assert rows[-1]["replay_pnl"] == 3.0
    assert rows[-1]["risk_emergency_stop_reason"] == "stop_reconciliation_required"
    row = rollup.iloc[0]
    assert not row["economic_pnl_complete"]
    assert row["economic_pnl_incomplete_days"] == 1
    assert row["economic_pnl_status"] == status
    assert row["risk_emergency_ownership_conflict_count"] == 1
    assert row["risk_emergency_stop_reason"] == "stop_reconciliation_required"
    assert not row["replay_promotion_eligible"]
    assert pd.isna(row["replay_pnl_sum"])
    assert pd.isna(row["terminal_pnl_sum"])
    path = tmp_path / "incomplete.md"
    campaign_audit._write_markdown(path, daily, rollup, {
        "tag": "synthetic", "symbol": "BTCUSDC", "days": [], "arms": [],
    })
    assert "Full-window PnL aggregates are unset" in path.read_text()


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
