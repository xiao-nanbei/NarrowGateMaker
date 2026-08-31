import hashlib
import json

import numpy as np
import pytest

from live.config import Config, to_backtest_params
from models.backtest_config import apply_tick_defaults, build_backtest_base_params
from models.replay_contract import (
    INDIVIDUAL_TRADES_REPAIR_ID,
    INDIVIDUAL_TRADES_REPAIRED_DAYS,
    KEYED_LATENCY_SAMPLER_VERSION,
    STANDARD_INITIAL_STATE_SCHEMA,
    configure_fixed_latency_distribution,
    freeze_replay_contract,
    validate_frozen_replay_contract,
)
from research.families.f01_fixed_parameter_racing.campaign_outcome_replay_audit import (
    main as replay_audit_main,
)
from strategy.replay_controls import (
    LOSS_COOLDOWN_SEMANTICS,
    SYNC_DEGRADE_SEMANTICS,
    SYNC_DEGRADE_TAPE_SCHEMA,
)


def _write_individual_trades_identity(tmp_path):
    repaired_rows = [
        {
            "day": day,
            "raw_file": f"BTCUSDC-trades-{day}.csv",
            "raw_sha256": f"{index:064x}",
            "raw_size_bytes": index * 100,
        }
        for index, day in enumerate(INDIVIDUAL_TRADES_REPAIRED_DAYS, start=1)
    ]
    manifest = tmp_path / "individual_trades_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "schema": "narrowgate.taker_tempo_manifest.v1",
                "symbol": "BTCUSDC",
                "raw_root": "/marketdata/raw_trades/BTCUSDC",
                "daily_manifest_sha256": "daily-files-identity",
                "daily_files": repaired_rows,
            }
        ),
        encoding="utf-8",
    )
    integrity_report = tmp_path / "individual_trades_integrity.csv"
    integrity_report.write_text(
        "day,side_flags_valid\n"
        + "".join(f"{day},true\n" for day in INDIVIDUAL_TRADES_REPAIRED_DAYS),
        encoding="utf-8",
    )
    return manifest, integrity_report


def _contract_params(tmp_path):
    config = tmp_path / "live.yaml"
    config.write_text("project_name: NarrowGate\n", encoding="utf-8")
    model_dir = tmp_path / "model"
    model_dir.mkdir(exist_ok=True)
    (model_dir / "dir_10s_meta.json").write_text(
        json.dumps({"feature_cols": ["return_1"]}), encoding="utf-8"
    )
    p3 = tmp_path / "fill_prob_params.json"
    p3.write_text('{"schema_version":"narrowgate_p3_touch_calibration.v2"}\n')
    p3_sha256 = hashlib.sha256(p3.read_bytes()).hexdigest()
    queue = tmp_path / "queue.json"
    queue.write_text('{"schema_version":"narrowgate_queue_calibration.v3"}\n')
    trades_manifest, trades_integrity = _write_individual_trades_identity(tmp_path)
    params = {
        "_config_path": str(config),
        "resolved_model_dir": str(model_dir),
        "fill_probability_model_path": str(p3),
        "queue_calibration_path": str(queue),
        "fill_probability_schema_version": "narrowgate_p3_touch_calibration.v2",
        "fill_probability_model_type": "empirical_survival",
        "fill_probability_event_type": "touch",
        "fill_probability_horizon_s": 10.0,
        "fill_probability_distance_origin": (
            "same_side_best_bid_or_ask_at_window_start"
        ),
        "fill_probability_distance_unit": "USDC_per_BTC",
        "fill_probability_side": "pooled_buy_sell",
        "fill_probability_queue_included": False,
        "fill_probability_artifact_sha256": p3_sha256,
        "p3_delta_star": 14.0,
        "p3_kappa_eff": 0.061,
        "historical_p3_scalar_adapter_enabled": True,
        "p3_side_bbo_floor_enabled": False,
        "quote_math_mode": "legacy_v0",
        "gamma": 0.046,
        "kappa": 0.05,
        "inventory_reference_qty": 1.0,
        "order_size": 0.001,
        "quote_horizon_s": 1.0,
        "queue_calibration_schema_version": "narrowgate_queue_calibration.v3",
        "queue_calibration_apply_mode": "frozen_default",
        "queue_calibration_fit_days": ["2026-07-01"],
        "queue_ahead_base_mult": 0.7,
        "queue_deplete_base_mult": 1.0,
        "queue_ahead_buy_exposure_mult": 0.5,
        "queue_ahead_buy_reducing_mult": 1.15,
        "queue_ahead_sell_exposure_mult": 1.45,
        "queue_ahead_sell_reducing_mult": 0.7,
        "execution_trade_source": "individual_trades",
        "individual_trades_manifest_path": str(trades_manifest),
        "individual_trades_integrity_report_path": str(trades_integrity),
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "require_historical_bbo": True,
        "exec_book_visibility_mode": "sampled",
        "exec_depth_visibility_source_offset_ms": 0,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        "rng_seed": 42,
        "latency_seed": 59,
        "_new_order_latency_samples_ms": np.asarray([1.0, 2.0, 200.0]),
        "_new_order_exchange_effective_latency_samples_ms": np.asarray(
            [0.5, 1.5, 150.0]
        ),
        "_cancel_order_latency_samples_ms": np.asarray([3.0, 4.0, 300.0]),
        "_cancel_exchange_effective_latency_samples_ms": np.asarray(
            [1.0, 2.0, 100.0]
        ),
        "_cancel_ack_visibility_latency_samples_ms": np.asarray(
            [2.0, 3.0, 80.0]
        ),
        "_exec_book_visibility_delay_samples_ms": np.asarray([5.0, 6.0, 400.0]),
    }
    configure_fixed_latency_distribution(
        params,
        scenario="baseline",
        profile_id="provider_neutral_test_profile_v1",
        environment="provider_neutral_test_environment",
        baseline_clip_quantile=0.9,
    )
    return params


def _write_sync_contract_tape(tmp_path):
    path = tmp_path / "sync_adjust_events.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": SYNC_DEGRADE_TAPE_SCHEMA,
                "environment": "provider_neutral_test_environment",
                "start_ts_ms": 1_700_000_000_000,
                "end_ts_ms": 1_700_086_400_000,
                "events": [
                    {
                        "ts_ms": 1_700_000_001_000,
                        "reason": "position_sync_adjust",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def test_formal_contract_is_stable_and_fresh_start(tmp_path):
    params = _contract_params(tmp_path)
    params["initial_inventory"] = 0.005
    params["initial_entry_price"] = 80_000.0

    first = freeze_replay_contract(params, root=tmp_path)
    second = validate_frozen_replay_contract(params)

    assert first["contract_sha256"] == second["contract_sha256"]
    assert params["initial_inventory"] == 0.0
    assert params["initial_entry_price"] == 0.0
    assert first["latency"]["sampler_version"] == KEYED_LATENCY_SAMPLER_VERSION
    assert first["promotion_eligible"] is True
    assert first["latency"]["new_order_samples"]["max_ms"] < 200.0
    assert (
        first["latency"]["new_order_exchange_effective_samples"]["count"]
        == 3
    )
    assert first["latency"]["cancel_exchange_effective_samples"]["count"] == 3
    assert first["latency"]["cancel_ack_visibility_samples"]["count"] == 3
    assert (
        "exec_book_visibility_identity"
        not in first["causal_event_semantics"]
    )
    assert first["p3"]["event_type"] == "touch"
    assert first["p3"]["horizon_s"] == 10.0
    assert first["p3"]["distance_unit"] == "USDC_per_BTC"
    assert first["p3"]["artifact_sha256"] == first["artifacts"]["p3"]["sha256"]
    assert first["p3"]["consumer_mode"] == "historical_pair_projection"
    assert first["quote_unit_contract"] == {
        "formula_identity": "as_shaped_empirical_controller",
        "quote_math_mode": "legacy_v0",
        "inventory_unit": "base_asset",
        "normalized_inventory_unit": "dimensionless",
        "price_unit": "quote_asset_per_base_asset",
        "variance_rate_unit": "price_squared_per_second",
        "duration_unit": "second",
        "eta_inventory_unit": "inverse_price",
        "a_spread_unit": "inverse_price",
        "risk_per_order_unit": "inverse_price",
        "execution_intensity_slope_unit": "inverse_price",
        "inventory_reference_qty": 1.0,
        "order_size": 0.001,
        "eta_inventory": 0.046,
        "a_spread": 0.046,
        "risk_per_order": 0.046,
        "execution_intensity_slope": 0.05,
        "quote_horizon_s": 1.0,
        "risk_horizon_s": 1.0,
    }
    trades_identity = first["artifacts"]["individual_trades"]
    assert trades_identity["status"] == "verified"
    assert trades_identity["manifest_sha256"]
    assert trades_identity["integrity_report_sha256"]
    assert trades_identity["repair_identity"]["id"] == INDIVIDUAL_TRADES_REPAIR_ID
    assert [
        row["day"] for row in trades_identity["repair_identity"]["days"]
    ] == list(INDIVIDUAL_TRADES_REPAIRED_DAYS)


def test_legacy_contract_omits_unselected_split_lifecycle_identities(tmp_path):
    params = _contract_params(tmp_path)
    for name in (
        "_new_order_exchange_effective_latency_samples_ms",
        "_cancel_exchange_effective_latency_samples_ms",
        "_cancel_ack_visibility_latency_samples_ms",
    ):
        params.pop(name)

    contract = freeze_replay_contract(params, root=tmp_path)

    assert "new_order_exchange_effective_samples" not in contract["latency"]
    assert "cancel_exchange_effective_samples" not in contract["latency"]
    assert "cancel_ack_visibility_samples" not in contract["latency"]
    assert "private_fill_visibility" not in contract["latency"]
    assert "serial_rest_gateway" not in contract["latency"]


def test_zero_private_fill_visibility_preserves_formal_b0_contract(tmp_path):
    baseline = freeze_replay_contract(_contract_params(tmp_path), root=tmp_path)
    zero = _contract_params(tmp_path)
    zero["_private_fill_visibility_latency_samples_ms"] = np.asarray([0.0])

    contract = freeze_replay_contract(zero, root=tmp_path)

    assert contract["contract_sha256"] == baseline["contract_sha256"]
    assert "private_fill_visibility" not in contract["latency"]


def test_private_fill_visibility_identity_is_diagnostic_only(tmp_path):
    formal = _contract_params(tmp_path)
    formal["_private_fill_visibility_latency_samples_ms"] = np.asarray(
        [1.0, 3.0, 7.0]
    )
    with pytest.raises(
        RuntimeError,
        match="private-fill visibility latency is diagnostic-only",
    ):
        freeze_replay_contract(formal, root=tmp_path)

    diagnostic = _contract_params(tmp_path)
    diagnostic["_private_fill_visibility_latency_samples_ms"] = np.asarray(
        [1.0, 3.0, 7.0]
    )
    contract = freeze_replay_contract(
        diagnostic,
        purpose="diagnostic",
        root=tmp_path,
    )
    identity = contract["latency"]["private_fill_visibility"]

    assert contract["promotion_eligible"] is False
    assert identity["evidence_scope"] == "diagnostic_only"
    assert identity["clock"] == (
        "exchange_fill_to_local_private_callback_visibility"
    )
    assert identity["samples"]["count"] == 3
    validate_frozen_replay_contract(diagnostic)


def test_decision_to_gateway_latency_identity_is_diagnostic_only(tmp_path):
    formal = _contract_params(tmp_path)
    formal["_decision_to_gateway_latency_samples_ms"] = np.asarray(
        [2.0, 5.0, 11.0]
    )
    with pytest.raises(
        RuntimeError,
        match="decision-to-gateway compute latency is diagnostic-only",
    ):
        freeze_replay_contract(formal, root=tmp_path)

    diagnostic = _contract_params(tmp_path)
    diagnostic["_decision_to_gateway_latency_samples_ms"] = np.asarray(
        [2.0, 5.0, 11.0]
    )
    diagnostic["decision_to_gateway_latency_seed"] = 73
    contract = freeze_replay_contract(
        diagnostic,
        purpose="diagnostic",
        root=tmp_path,
    )
    identity = contract["latency"]["decision_to_gateway"]

    assert contract["promotion_eligible"] is False
    assert identity["evidence_scope"] == "diagnostic_only"
    assert identity["clock"] == "decision_to_first_gateway_request"
    assert identity["sampling_unit"] == "one_keyed_draw_per_decision"
    assert identity["market_snapshot"] == "frozen_at_decision_time"
    assert identity["serial_row_origin"] == "after_decision_compute_delay"
    assert identity["samples"]["count"] == 3
    assert identity["seed"] == 73
    validate_frozen_replay_contract(diagnostic)


def test_zero_decision_to_gateway_samples_do_not_change_contract_identity(
    tmp_path,
):
    params = _contract_params(tmp_path)
    params["_decision_to_gateway_latency_samples_ms"] = np.asarray([0.0])
    contract = freeze_replay_contract(params, root=tmp_path)

    assert contract["promotion_eligible"] is True
    assert "decision_to_gateway" not in contract["latency"]


@pytest.mark.parametrize("total", [[], [2.0, 5.0, 11.0]])
def test_zero_pre_snapshot_compute_preserves_contract_bytes(tmp_path, total):
    params = _contract_params(tmp_path)
    params["_decision_to_gateway_latency_samples_ms"] = np.asarray(total)
    before = freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)
    params["_pre_snapshot_compute_latency_samples_ms"] = np.zeros(3)
    after = freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)
    assert json.dumps(before, sort_keys=True) == json.dumps(after, sort_keys=True)


def test_pre_snapshot_compute_records_paired_split(tmp_path):
    params = _contract_params(tmp_path)
    params.update(
        _decision_to_gateway_latency_samples_ms=np.asarray([2.0, 5.0, 11.0]),
        _pre_snapshot_compute_latency_samples_ms=np.asarray([1.0, 4.0, 9.0]),
        decision_to_gateway_latency_seed=73,
        replay_main_loop_sleep_ms=100.0,
        rest_gateway_timing_mode="sampled_serial",
    )
    contract = freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)
    identity = contract["latency"]["decision_to_gateway"]
    split = identity["computation_split"]
    assert identity["seed"] == 73
    assert identity["clock"] == "requote_entry_to_first_gateway_request"
    assert identity["market_snapshot"] == "captured_after_pre_snapshot_compute"
    assert split["prediction_cutoff"] == "requote_entry_before_pre_snapshot_compute"
    assert split["snapshot_capture"] == "requote_entry_plus_pre_snapshot_compute"
    assert split["pre_snapshot_samples"]["median_ms"] == 4.0
    assert split["post_snapshot_compute"] == (
        "round_total_minus_round_pre_not_independently_sampled"
    )
    assert "post_snapshot_samples" not in split
    assert split["total_accounting"] == "pre_plus_post_equals_total_not_added_again"
    validate_frozen_replay_contract(params)


@pytest.mark.parametrize(
    "pre,total,mode,sleep",
    [
        ([1.0], [2.0, 5.0], "sampled_serial", 100),
        ([3.0], [2.0], "sampled_serial", 100),
        ([float("nan")], [2.0], "sampled_serial", 100),
        ([-1.0], [2.0], "sampled_serial", 100),
        ([[1.0]], [[2.0]], "sampled_serial", 100),
        ([1.0], [float("nan")], "sampled_serial", 100),
        ([1.0], [2.0], "disabled", 100),
        ([1.0], [2.0], "sampled_serial", 0),
    ],
)
def test_pre_snapshot_compute_rejects_unpaired_or_unsupported_input(
    tmp_path, pre, total, mode, sleep,
):
    params = _contract_params(tmp_path)
    params.update(
        _decision_to_gateway_latency_samples_ms=np.asarray(total),
        _pre_snapshot_compute_latency_samples_ms=np.asarray(pre),
        rest_gateway_timing_mode=mode,
        replay_main_loop_sleep_ms=sleep,
    )
    with pytest.raises(ValueError, match="pre-snapshot compute"):
        freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)


def test_serial_rest_gateway_profile_identity_is_diagnostic_only(tmp_path):
    profile = tmp_path / "serial_rest_gateway.npz"
    present = np.ones((1, 4), dtype=np.bool_)
    np.savez_compressed(
        profile,
        slot_names=np.asarray(
            ["cancel_buy", "cancel_sell", "new_buy", "new_sell"]
        ),
        request_present_mask=present,
        request_start_offset_ms=np.asarray([[0.0, 2.0, 4.0, 6.0]]),
        exchange_effective_observed_mask=present,
        exchange_effective_latency_ms=np.asarray([[1.0, 1.5, 2.0, 2.5]]),
        local_visibility_observed_mask=present,
        local_visibility_latency_ms=np.asarray([[3.0, 3.5, 4.0, 4.5]]),
    )
    overrides = {
        "rest_gateway_timing_mode": "paired_npz",
        "rest_gateway_timing_profile_path": str(profile),
        "rest_gateway_timing_seed": 71,
    }
    formal = _contract_params(tmp_path)
    formal.update(overrides)
    with pytest.raises(
        RuntimeError,
        match="serial REST gateway timing is diagnostic-only",
    ):
        freeze_replay_contract(formal, root=tmp_path)

    diagnostic = _contract_params(tmp_path)
    diagnostic.update(overrides)
    contract = freeze_replay_contract(
        diagnostic,
        purpose="diagnostic",
        root=tmp_path,
    )
    identity = contract["latency"]["serial_rest_gateway"]

    assert contract["promotion_eligible"] is False
    assert identity["evidence_scope"] == "diagnostic_only"
    assert identity["sampling_unit"] == "whole_observed_request_row"
    assert identity["exact_request_mask_required"] is True
    assert identity["request_slot_order"] == [
        "cancel_buy",
        "cancel_sell",
        "new_buy",
        "new_sell",
    ]
    assert identity["profile"]["sha256"] == hashlib.sha256(
        profile.read_bytes()
    ).hexdigest()
    assert identity["seed"] == 71
    validate_frozen_replay_contract(diagnostic)


def test_serial_rest_gateway_requires_a_bound_profile(tmp_path):
    params = _contract_params(tmp_path)
    params.update(
        {
            "rest_gateway_timing_mode": "paired_npz",
            "rest_gateway_timing_profile_path": str(tmp_path / "missing.npz"),
        }
    )

    with pytest.raises(
        RuntimeError,
        match="serial REST gateway timing profile identity is missing",
    ):
        freeze_replay_contract(
            params,
            purpose="diagnostic",
            root=tmp_path,
        )


def test_sampled_serial_gateway_binds_existing_lifecycle_samples(tmp_path):
    params = _contract_params(tmp_path)
    params.update(
        {
            "rest_gateway_timing_mode": "sampled_serial",
            "_new_order_latency_samples_ms": [5.0, 8.0],
            "_new_order_exchange_effective_latency_samples_ms": [2.0, 4.0],
            "_cancel_exchange_effective_latency_samples_ms": [3.0, 5.0],
            "_cancel_ack_visibility_latency_samples_ms": [6.0, 9.0],
        }
    )
    with pytest.raises(RuntimeError, match="serial REST gateway timing is diagnostic-only"):
        freeze_replay_contract(params, root=tmp_path)
    contract = freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)
    identity = contract["latency"]["serial_rest_gateway"]
    assert identity["mode"] == "sampled_serial"
    assert identity["joint_request_replay"] is False
    assert identity["sampling_unit"] == "independent_request_with_paired_effective_ack"
    assert "profile" not in identity
    assert contract["promotion_eligible"] is False
    validate_frozen_replay_contract(params)


def test_sampled_serial_gateway_records_return_clock_separately(tmp_path):
    profile = tmp_path / "request_times.npz"
    np.savez(profile, response_upper_bound_ms=np.asarray([3.0, 7.0]))
    params = _contract_params(tmp_path)
    params.update(
        rest_gateway_timing_mode="sampled_serial",
        rest_gateway_timing_profile_path=str(profile),
    )
    contract = freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)
    timing = contract["latency"]["serial_rest_gateway"]
    assert timing["request_start"] == "max_decision_ready_previous_rest_return"
    assert timing["response_clock_semantics"] == "paired_observed_upper_bound"
    assert timing["cancel_continuation"] == "skip_new_if_not_terminal_at_rest_return"
    assert timing["profile"]["sha256"] == hashlib.sha256(profile.read_bytes()).hexdigest()
    assert timing["joint_request_replay"] is False
    validate_frozen_replay_contract(params)


def test_serial_direct_samples_record_proxy_semantics_and_paired_rows(tmp_path):
    params = _contract_params(tmp_path)
    params.update(
        rest_gateway_timing_mode="sampled_serial",
        _serial_rest_return_sample_semantics="HTTP_return_upper_bound_proxy",
        _serial_rest_return_samples_by_operation={
            "new": [[4.0, 4.0, 4.0], [7.0, 7.0, 7.0]],
            "cancel": [[3.0, 3.0, 3.0]],
        },
    )
    with pytest.raises(RuntimeError, match="serial REST gateway timing is diagnostic-only"):
        freeze_replay_contract(params, root=tmp_path)
    contract = freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)
    identity = contract["latency"]["serial_rest_gateway"]
    assert identity["response_clock_semantics"] == "HTTP_return_upper_bound_proxy"
    assert identity["sample_identity_source"] == "operation_pooled_direct_samples"
    assert identity["sample_columns"] == ["exchange_effective_ms", "local_ack_ms", "http_return_ms"]
    assert "profile" not in identity
    validate_frozen_replay_contract(params)
    before = identity["operation_samples"]["new"]
    params["_serial_rest_return_samples_by_operation"]["new"][0] = [5.0, 5.0, 5.0]
    updated = freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)
    assert updated["latency"]["serial_rest_gateway"]["operation_samples"]["new"] != before


def test_full_chain_execution_controls_survive_shared_parameter_mapping():
    config = Config()
    config.risk.max_exec_book_visible_age_s = 0.4
    config.risk.max_exec_book_source_lag_s = 1.25
    live_params = to_backtest_params(config)
    assert "replay_main_loop_sleep_ms" not in live_params
    defaults = apply_tick_defaults(build_backtest_base_params(live_params))
    assert defaults["max_exec_book_visible_age_s"] == 0.4
    assert defaults["max_exec_book_source_lag_s"] == 1.25
    assert defaults["replay_main_loop_sleep_ms"] == 0

    explicit = dict(live_params, replay_main_loop_sleep_ms=100)
    params = apply_tick_defaults(build_backtest_base_params(explicit))
    assert params["replay_main_loop_sleep_ms"] == 100
    assert params["max_exec_book_visible_age_s"] == 0.4
    assert params["max_exec_book_source_lag_s"] == 1.25


def test_main_loop_timing_is_bound_without_changing_disabled_b0(tmp_path):
    params = _contract_params(tmp_path)
    b0 = freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)
    params["replay_main_loop_sleep_ms"] = 0
    assert validate_frozen_replay_contract(params)["contract_sha256"] == b0["contract_sha256"]
    assert "main_loop" not in b0["causal_event_semantics"]

    profile = tmp_path / "request_times.npz"
    np.savez(profile, response_upper_bound_ms=np.asarray([3.0, 7.0]))
    params.update(
        rest_gateway_timing_mode="sampled_serial",
        rest_gateway_timing_profile_path=str(profile),
        replay_main_loop_sleep_ms=100,
    )
    contract = freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)
    loop = contract["causal_event_semantics"]["main_loop"]
    assert loop["replay_main_loop_sleep_ms"] == 100
    assert loop["requote_anchor"] == "actual_requote_start"
    assert loop["wake_clock"] == "actual_tick_and_rest_return_then_sleep"
    assert loop["dynamic_requote_clock"] == "source_1s_bars_before_due_check"
    validate_frozen_replay_contract(params)
    params["replay_main_loop_sleep_ms"] = 200
    with pytest.raises(RuntimeError, match="identity differs"):
        validate_frozen_replay_contract(params)
    params.update(replay_main_loop_sleep_ms=100, exec_book_visibility_mode="message_schedule")
    message_contract = freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)
    assert message_contract["causal_event_semantics"]["main_loop"]["dynamic_requote_clock"] == (
        "delivered_1s_bars_before_due_check"
    )


def test_contract_rejects_artifact_or_queue_drift(tmp_path):
    params = _contract_params(tmp_path)
    freeze_replay_contract(params, root=tmp_path)

    params["queue_ahead_base_mult"] = 0.15
    with pytest.raises(RuntimeError, match="differs from the frozen contract"):
        validate_frozen_replay_contract(params)

    params = _contract_params(tmp_path)
    freeze_replay_contract(params, root=tmp_path)
    model_meta = tmp_path / "model" / "dir_10s_meta.json"
    model_meta.write_text('{"feature_cols":["changed"]}\n', encoding="utf-8")
    with pytest.raises(RuntimeError, match="differs from the frozen contract"):
        validate_frozen_replay_contract(params)


def test_formal_individual_trades_requires_manifest(tmp_path):
    params = _contract_params(tmp_path)
    params.pop("individual_trades_manifest_path")

    with pytest.raises(
        RuntimeError,
        match="formal individual-trades replay requires a frozen manifest",
    ):
        freeze_replay_contract(params, root=tmp_path)


def test_exploratory_individual_trades_without_manifest_is_diagnostic(tmp_path):
    params = _contract_params(tmp_path)
    params.pop("individual_trades_manifest_path")

    contract = freeze_replay_contract(
        params,
        purpose="exploratory",
        root=tmp_path,
    )

    assert contract["promotion_eligible"] is False
    assert contract["artifacts"]["individual_trades"]["status"] == "missing_manifest"
    assert (
        contract["artifacts"]["individual_trades"]["evidence_scope"]
        == "diagnostic_only"
    )


def test_formal_individual_trades_requires_complete_repair_identity(tmp_path):
    params = _contract_params(tmp_path)
    manifest_path = tmp_path / "individual_trades_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["daily_files"] = payload["daily_files"][:-1]
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(
        RuntimeError,
        match="formal individual-trades identity is not verified: "
        "repair_identity_incomplete",
    ):
        freeze_replay_contract(params, root=tmp_path)


def test_formal_individual_trades_rejects_declared_manifest_sha_drift(tmp_path):
    params = _contract_params(tmp_path)
    params["individual_trades_manifest_sha256"] = "f" * 64

    with pytest.raises(
        RuntimeError,
        match="formal individual-trades identity is not verified: "
        "manifest_hash_mismatch",
    ):
        freeze_replay_contract(params, root=tmp_path)


def test_aggregate_trade_formal_replay_does_not_require_individual_manifest(tmp_path):
    params = _contract_params(tmp_path)
    params["execution_trade_source"] = "aggTrades"
    params.pop("individual_trades_manifest_path")
    params.pop("individual_trades_integrity_report_path")

    contract = freeze_replay_contract(params, root=tmp_path)

    assert contract["artifacts"]["individual_trades"]["status"] == "not_applicable"
    assert contract["promotion_eligible"] is True


def test_frozen_standard_initial_state_is_hashed_and_engine_neutral(tmp_path):
    params = _contract_params(tmp_path)
    state = tmp_path / "standard_initial_state.json"
    state.write_text(
        json.dumps(
            {
                "schema_version": STANDARD_INITIAL_STATE_SCHEMA,
                "initial_inventory": 0.002,
                "initial_entry_price": 85_000.0,
            }
        ),
        encoding="utf-8",
    )
    contract = freeze_replay_contract(
        params,
        initial_state_mode="frozen_standard",
        initial_state_artifact=state,
        root=tmp_path,
    )

    assert params["initial_inventory"] == pytest.approx(0.002)
    assert params["initial_entry_price"] == pytest.approx(85_000.0)
    assert contract["initial_state"]["artifact_sha256"]
    validate_frozen_replay_contract(params)


def test_formal_contract_rejects_live_window_identity(tmp_path):
    paired = _contract_params(tmp_path)
    paired["exec_book_visibility_mode"] = "paired"
    with pytest.raises(RuntimeError, match="live_alignment-only"):
        freeze_replay_contract(paired, root=tmp_path)

    warm = _contract_params(tmp_path)
    warm["initial_live_state"] = {"active_orders": [{"side": "BUY"}]}
    with pytest.raises(RuntimeError, match="live_alignment-only"):
        freeze_replay_contract(warm, root=tmp_path)


def test_formal_sampled_joint_visibility_binds_source_and_input_arrays(tmp_path):
    source = tmp_path / "visibility.csv"
    source.write_text("exec_book_age_s,exec_depth_age_s,exec_trade_age_s\n", encoding="utf-8")
    params = _contract_params(tmp_path)
    params.update(
        {
            "exec_book_visibility_mode": "sampled_joint",
            "exec_book_visibility_delay_profile_path": str(source),
            "exec_book_visibility_delay_profile_id": "test_joint_visibility",
            "_exec_book_visibility_paired_delay_ms": np.asarray([1.0, 2.0]),
            "_exec_depth_visibility_paired_delay_ms": np.asarray([3.0, 4.0]),
            "_exec_trade_visibility_paired_delay_ms": np.asarray([5.0, 6.0]),
        }
    )

    contract = freeze_replay_contract(params, root=tmp_path)
    identity = contract["causal_event_semantics"][
        "exec_book_visibility_identity"
    ]

    assert contract["promotion_eligible"] is True
    assert identity["evidence_scope"] == "formal_eligible"
    assert identity["source_profile"]["sha256"] == hashlib.sha256(
        source.read_bytes()
    ).hexdigest()
    assert identity["inputs"]["book"]["count"] == 2
    assert identity["inputs"]["depth"]["count"] == 2
    assert identity["inputs"]["trade"]["count"] == 2

    params["_exec_trade_visibility_paired_delay_ms"] = np.asarray([5.0, 7.0])
    with pytest.raises(RuntimeError, match="differs from the frozen contract"):
        validate_frozen_replay_contract(params)


def test_formal_sampled_joint_visibility_rejects_unbound_inputs(tmp_path):
    params = _contract_params(tmp_path)
    params["exec_book_visibility_mode"] = "sampled_joint"
    params["_exec_book_visibility_paired_delay_ms"] = np.asarray([1.0])
    params["_exec_depth_visibility_paired_delay_ms"] = np.asarray([2.0])
    params["_exec_trade_visibility_paired_delay_ms"] = np.asarray([3.0])

    with pytest.raises(
        RuntimeError,
        match="formal sampled_joint visibility requires a complete frozen source/input identity",
    ):
        freeze_replay_contract(params, root=tmp_path)


def test_source_stratified_visibility_profile_is_diagnostic_only(tmp_path):
    source = tmp_path / "source_stratified.json"
    source.write_text(
        json.dumps(
            {
                "schema": "market_data_latency_profile.v1",
                "profile_id": "diagnostic_source_profile",
                "source_stratified_sampling": {
                    "authority": "diagnostic_non_authoritative",
                    "promotion_eligible": False,
                },
            }
        ),
        encoding="utf-8",
    )
    source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    params = _contract_params(tmp_path)
    params.update(
        {
            "exec_book_visibility_mode": "profile_source_stratified",
            "exec_source_stratified_profile_path": str(source),
            "exec_source_stratified_profile_sha256": source_sha256,
            "exec_source_stratified_profile_id": "diagnostic_source_profile",
            "exec_source_stratified_profile_market_id": "binance:perp:BTCUSDC",
            "exec_source_stratified_profile_transport": "websocket",
        }
    )

    with pytest.raises(
        RuntimeError,
        match="profile_source_stratified visibility is diagnostic-only",
    ):
        freeze_replay_contract(params, root=tmp_path)

    contract = freeze_replay_contract(
        params,
        purpose="exploratory",
        root=tmp_path,
    )
    identity = contract["causal_event_semantics"][
        "exec_book_visibility_identity"
    ]
    assert contract["promotion_eligible"] is False
    assert identity["evidence_scope"] == "diagnostic_only"
    assert identity["source_profile"]["sha256"] == source_sha256


def test_message_delivery_schedule_is_not_claimed_as_formal_evidence(tmp_path):
    params = _contract_params(tmp_path)
    params.update(
        exec_book_visibility_mode="message_schedule",
        exec_message_delivery_input_semantics={
            "depth": "bookTicker_latency_proxy",
            "features": "frozen_source_time_values_delayed_consumption",
        },
    )
    with pytest.raises(RuntimeError, match="message_schedule visibility is diagnostic-only"):
        freeze_replay_contract(params, root=tmp_path)
    contract = freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)
    identity = contract["causal_event_semantics"]["exec_book_visibility_identity"]
    assert identity["sampling_unit"] == "assigned_once_per_source_message"
    assert identity["visibility_boundary"] == "feature_ready_ns_strictly_before_local_now"
    assert contract["causal_event_semantics"]["feature_visibility"] == (
        identity["visibility_boundary"]
    )
    assert identity["input_semantics"]["depth"] == "bookTicker_latency_proxy"
    assert identity["max_exec_book_visible_age_s"] == 5.0
    assert identity["max_exec_book_source_lag_s"] == 5.0
    assert contract["promotion_eligible"] is False
    validate_frozen_replay_contract(params)


@pytest.mark.parametrize(
    "field", ["max_exec_book_visible_age_s", "max_exec_book_source_lag_s"]
)
def test_message_schedule_binds_independent_freshness_limits(tmp_path, field):
    params = _contract_params(tmp_path)
    params.update(
        exec_book_visibility_mode="message_schedule",
        max_exec_book_visible_age_s=0.4,
        max_exec_book_source_lag_s=1.25,
    )
    contract = freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)
    identity = contract["causal_event_semantics"]["exec_book_visibility_identity"]
    assert identity["max_exec_book_visible_age_s"] == 0.4
    assert identity["max_exec_book_source_lag_s"] == 1.25
    params[field] += 0.1
    with pytest.raises(RuntimeError, match="identity differs"):
        validate_frozen_replay_contract(params)


@pytest.mark.parametrize("field", ["callback_fifo", "parent_mapping", "policy_producer"])
def test_message_schedule_reuses_existing_execution_recipe_identity(tmp_path, field):
    params = _contract_params(tmp_path)
    params.update(
        exec_book_visibility_mode="message_schedule",
        exec_message_delivery_input_semantics={
            "callback_fifo": "per_feed_serial_measured_service",
            "parent_mapping": "last_child_before_parent_delivery",
            "policy_producer": "receive_time_depth_EMA",
        },
    )
    freeze_replay_contract(params, purpose="diagnostic", root=tmp_path)
    validate_frozen_replay_contract(params)
    params["exec_message_delivery_input_semantics"] = {
        **params["exec_message_delivery_input_semantics"], field: "different_recipe"
    }
    with pytest.raises(RuntimeError, match="identity differs"):
        validate_frozen_replay_contract(params)


def test_latency_stress_is_reproducible_but_not_promotion_evidence(tmp_path):
    params = _contract_params(tmp_path)
    configure_fixed_latency_distribution(
        params,
        scenario="stress",
        profile_id="provider_neutral_test_profile_v1",
        environment="provider_neutral_test_environment",
        stress_spike_probability=0.01,
        stress_spike_multiplier=8.0,
    )
    contract = freeze_replay_contract(params, root=tmp_path)

    assert contract["latency"]["scenario"] == "stress"
    assert contract["promotion_eligible"] is False
    assert contract["latency"]["rare_spike_policy"] == "stress_only"


def test_diagnostic_queue_artifact_is_not_promotion_evidence(tmp_path):
    params = _contract_params(tmp_path)
    params["queue_calibration_diagnostic_only"] = True
    params["queue_calibration_diagnostic_parent_sha256"] = "parent"
    params["queue_calibration_diagnostic_note"] = "sensitivity only"

    contract = freeze_replay_contract(params, root=tmp_path)

    assert contract["promotion_eligible"] is False
    assert contract["queue"]["diagnostic_only"] is True
    assert contract["queue"]["diagnostic_parent_sha256"] == "parent"


def test_formal_cli_requires_strict_calibration():
    with pytest.raises(SystemExit, match="formal requires --strict-calibration"):
        replay_audit_main(
            ["--days", "2026-07-01", "--replay-purpose", "formal"]
        )


def test_formal_contract_requires_path_dependent_loss_pair_and_semantics(tmp_path):
    missing_duration = _contract_params(tmp_path)
    missing_duration["max_consecutive_losses"] = 3
    with pytest.raises(RuntimeError, match="requires both a positive threshold"):
        freeze_replay_contract(missing_duration, root=tmp_path)

    wrong_semantics = _contract_params(tmp_path)
    wrong_semantics.update(
        {
            "max_consecutive_losses": 3,
            "cooldown_after_loss": 30.0,
            "consecutive_loss_cooldown_semantics": "legacy_fill_count_v0",
        }
    )
    with pytest.raises(RuntimeError, match="semantics identity is invalid"):
        freeze_replay_contract(wrong_semantics, root=tmp_path)

    valid = _contract_params(tmp_path)
    valid.update(
        {
            "max_consecutive_losses": 3,
            "cooldown_after_loss": 30.0,
            "consecutive_loss_cooldown_semantics": LOSS_COOLDOWN_SEMANTICS,
        }
    )
    contract = freeze_replay_contract(valid, root=tmp_path)
    assert contract["path_dependent_controls"]["consecutive_loss_cooldown"][
        "round_trip_clock"
    ] == "full_close_or_flip_then_next_policy_clock"


def test_enabled_sync_adjust_requires_frozen_tape_for_promotion(tmp_path):
    omitted = _contract_params(tmp_path)
    omitted["sync_adjust_degrade_enabled"] = True
    with pytest.raises(RuntimeError, match="cannot omit the enabled live"):
        freeze_replay_contract(omitted, root=tmp_path)

    tape, tape_sha = _write_sync_contract_tape(tmp_path)
    frozen = _contract_params(tmp_path)
    frozen.update(
        {
            "sync_adjust_degrade_enabled": True,
            "sync_adjust_replay_mode": "frozen_tape",
            "sync_adjust_event_tape_path": str(tape),
            "sync_adjust_event_tape_sha256": tape_sha,
            "sync_adjust_event_environment": (
                "provider_neutral_test_environment"
            ),
            "sync_adjust_semantics": SYNC_DEGRADE_SEMANTICS,
            "sync_adjust_pause_s": 120.0,
            "sync_adjust_cancel_orders": True,
        }
    )
    contract = freeze_replay_contract(frozen, root=tmp_path)
    sync = contract["path_dependent_controls"]["sync_adjust_degrade"]
    assert contract["promotion_eligible"] is True
    assert sync["promotion_eligible"] is True
    assert sync["event_tape"]["event_count"] == 1
    assert sync["event_tape"]["start_ts_ms"] < sync["event_tape"]["end_ts_ms"]


def test_sync_censor_and_stress_are_diagnostic_only(tmp_path):
    tape, tape_sha = _write_sync_contract_tape(tmp_path)
    censor = _contract_params(tmp_path)
    censor.update(
        {
            "sync_adjust_degrade_enabled": True,
            "sync_adjust_replay_mode": "censor",
            "sync_adjust_event_tape_path": str(tape),
            "sync_adjust_event_tape_sha256": tape_sha,
            "sync_adjust_event_environment": (
                "provider_neutral_test_environment"
            ),
            "sync_adjust_semantics": SYNC_DEGRADE_SEMANTICS,
        }
    )
    censor_contract = freeze_replay_contract(censor, root=tmp_path)
    assert censor_contract["promotion_eligible"] is False

    stress = _contract_params(tmp_path)
    stress.update(
        {
            "sync_adjust_degrade_enabled": True,
            "sync_adjust_replay_mode": "stress",
            "sync_adjust_semantics": SYNC_DEGRADE_SEMANTICS,
        }
    )
    stress_contract = freeze_replay_contract(stress, root=tmp_path)
    assert stress_contract["promotion_eligible"] is False


def test_sync_tape_environment_identity_must_match_payload(tmp_path):
    tape, tape_sha = _write_sync_contract_tape(tmp_path)
    params = _contract_params(tmp_path)
    params.update(
        {
            "sync_adjust_degrade_enabled": True,
            "sync_adjust_replay_mode": "frozen_tape",
            "sync_adjust_event_tape_path": str(tape),
            "sync_adjust_event_tape_sha256": tape_sha,
            "sync_adjust_event_environment": "local_macos",
            "sync_adjust_semantics": SYNC_DEGRADE_SEMANTICS,
        }
    )
    with pytest.raises(RuntimeError, match="environment identity is mismatched"):
        freeze_replay_contract(params, root=tmp_path)


def test_buy_q90_formal_contract_binds_native_l2_and_artifact_hashes(tmp_path):
    model = tmp_path / "hazard_model.json"
    model.write_text('{"model":"q90"}\n', encoding="utf-8")
    policy = tmp_path / "hazard_policy.json"
    policy.write_text('{"policy":"cancel_reenter"}\n', encoding="utf-8")
    l2_manifest = tmp_path / "formal_l2_manifest.json"
    l2_manifest.write_text('{"schema":"formal_l2.v1"}\n', encoding="utf-8")
    native_root = tmp_path / "cryptohftdata"
    native_root.mkdir()
    params = _contract_params(tmp_path)
    params.update(
        {
            "dynamic_fill_hazard_shadow_enabled": True,
            "dynamic_fill_hazard_action_enabled": True,
            "dynamic_fill_hazard_shadow_model_path": str(model),
            "dynamic_fill_hazard_shadow_model_sha256": hashlib.sha256(
                model.read_bytes()
            ).hexdigest(),
            "dynamic_fill_hazard_action_policy_path": str(policy),
            "dynamic_fill_hazard_action_policy_sha256": hashlib.sha256(
                policy.read_bytes()
            ).hexdigest(),
            "exchange_book_queue_mode": "strict",
            "require_formal_l2": True,
            "formal_l2_manifest_path": str(l2_manifest),
            "native_exchange_book_root": str(native_root),
            "native_exchange_book_warmup_hours": 24,
        }
    )
    contract = freeze_replay_contract(params, root=tmp_path)
    q90 = contract["path_dependent_controls"]["dynamic_fill_hazard_q90"]
    assert q90["replay_authority"] == "python_native_exchange_book"
    assert q90["pending_cancel_fillable"] is True

    params["dynamic_fill_hazard_action_policy_sha256"] = "f" * 64
    with pytest.raises(RuntimeError, match="artifact hash is missing or mismatched"):
        freeze_replay_contract(params, root=tmp_path)
