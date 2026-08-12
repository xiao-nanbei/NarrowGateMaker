import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models.backtest_config import (
    add_queue_calibration_params,
    validate_formal_replay_calibration,
)
from models.backtest_tick import (
    _read_individual_trade_csv,
    build_replay_event_clock,
    ensure_model_feature_columns,
    terminal_pnl_decomposition,
)
from models.tick_data_types import HistoricalBBOData, HistoricalL2Data
from research.families.f01_fixed_parameter_racing.campaign_outcome_replay_audit import (
    _integrity_diagnostic_arms,
    _random_passive_arms,
    _random_passive_null_table,
    _resolve_cpp_parity_days,
)


def _strict_params(config_path: Path) -> dict:
    p3_path = config_path.parent / "fill_prob_params.json"
    p3_path.write_text("frozen-p3-fixture\n", encoding="utf-8")
    return {
        "_config_explicit": True,
        "_config_path": str(config_path),
        "fill_probability_calibrated": True,
        "fill_probability_schema_version": "narrowgate_p3_touch_calibration.v2",
        "fill_probability_model_type": "empirical_survival",
        "fill_probability_event_type": "touch",
        "fill_probability_horizon_s": 10.0,
        "fill_probability_distance_unit": "USDC_per_BTC",
        "fill_probability_artifact_sha256": hashlib.sha256(
            p3_path.read_bytes()
        ).hexdigest(),
        "fill_probability_model_path": str(p3_path),
        "p3_delta_star": 20.0,
        "p3_kappa_eff": 0.05,
        "queue_calibration_loaded": True,
        "queue_calibration_schema_version": "narrowgate_queue_calibration.v3",
        "queue_calibration_apply_mode": "frozen_default",
        "queue_calibration_fit_days": ["2026-07-10"],
        "queue_calibration_replay_params": {
            "queue_ahead_base_mult": 1.0,
            "queue_deplete_base_mult": 1.0,
            "queue_ahead_buy_exposure_mult": 1.0,
            "queue_ahead_buy_reducing_mult": 1.0,
            "queue_ahead_sell_exposure_mult": 1.0,
            "queue_ahead_sell_reducing_mult": 1.0,
        },
        "queue_ahead_base_mult": 1.0,
        "queue_deplete_base_mult": 1.0,
        "queue_ahead_buy_exposure_mult": 1.0,
        "queue_ahead_buy_reducing_mult": 1.0,
        "queue_ahead_sell_exposure_mult": 1.0,
        "queue_ahead_sell_reducing_mult": 1.0,
        "require_historical_bbo": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "_new_order_latency_samples_ms": np.array([10.0]),
        "_cancel_order_latency_samples_ms": np.array([12.0]),
    }


def test_terminal_pnl_decomposition_is_side_symmetric():
    long = terminal_pnl_decomposition(-100.0, 1.0, 110.0, 0.001)
    short = terminal_pnl_decomposition(100.0, -1.0, 90.0, 0.001)
    assert long == pytest.approx((10.0, 0.0, 10.0))
    assert short == pytest.approx((10.0, 0.0, 10.0))
    for mtm, fee, final in (long, short):
        assert final == pytest.approx(mtm - fee)


def test_individual_trade_loader_preserves_matching_events(tmp_path):
    path = tmp_path / "BTCUSDC-trades-2026-01-01.csv"
    path.write_text(
        "id,price,qty,quote_qty,time,is_buyer_maker\n"
        "1,100.0,0.1,10.0,1767225600000,false\n"
        "2,100.0,0.2,20.0,1767225600000,false\n",
        encoding="utf-8",
    )

    trades = _read_individual_trade_csv(path)

    assert trades["trade_id"].tolist() == [1, 2]
    assert trades["quantity"].tolist() == pytest.approx([0.1, 0.2])
    assert trades["transact_time"].tolist() == [
        1767225600000,
        1767225600000,
    ]
    assert trades["is_buyer_maker"].tolist() == [False, False]


def test_individual_trade_loader_orders_equal_ms_by_trade_id(tmp_path):
    path = tmp_path / "BTCUSDC-trades-2026-01-01.csv"
    path.write_text(
        "id,price,qty,quote_qty,time,is_buyer_maker\n"
        "2,100.1,0.2,20.02,1767225600000,false\n"
        "1,100.0,0.1,10.0,1767225600000,true\n"
        "3,100.2,0.3,30.06,1767225600001,false\n",
        encoding="utf-8",
    )

    trades = _read_individual_trade_csv(path)

    assert trades["transact_time"].tolist() == [
        1767225600000,
        1767225600000,
        1767225600001,
    ]
    assert trades["trade_id"].tolist() == [1, 2, 3]


def test_empirical_requote_clock_preserves_ok_and_block_actions():
    trades = pd.DataFrame(
        {
            "transact_time": [1_000, 2_000],
            "price": [100.0, 101.0],
            "quantity": [1.0, 1.0],
            "is_buyer_maker": [True, False],
        }
    )
    events, execution_count = build_replay_event_clock(
        trades,
        mode="empirical",
        interval_ms=100,
        empirical_ts_ms=np.array([1_250, 1_500], dtype=np.int64),
        empirical_action=np.array([2, 3], dtype=np.uint8),
    )

    assert execution_count == 2
    synthetic = events[events["_is_execution_trade"] == 0]
    assert synthetic["transact_time"].tolist() == [1_250, 1_500]
    assert synthetic["is_buyer_maker"].tolist() == [2, 3]
    assert synthetic["quantity"].tolist() == [0.0, 0.0]


def test_merged_clock_includes_book_events_without_creating_fills():
    trades = pd.DataFrame(
        {
            "transact_time": [1_000, 1_400],
            "price": [100.0, 101.0],
            "quantity": [0.1, 0.2],
            "is_buyer_maker": [True, False],
        }
    )
    bbo = HistoricalBBOData(
        ts_ms=np.array([1_050, 1_150], dtype=np.int64),
        best_bid=np.array([99.9, 100.0]),
        best_ask=np.array([100.1, 100.2]),
        bid_qty=np.ones(2),
        ask_qty=np.ones(2),
    )
    l2 = HistoricalL2Data(
        ts_ms=np.array([1_075, 1_150], dtype=np.int64),
        bid_px=np.array([[99.9], [100.0]]),
        bid_qty=np.ones((2, 1)),
        ask_px=np.array([[100.1], [100.2]]),
        ask_qty=np.ones((2, 1)),
    )

    events, execution_count = build_replay_event_clock(
        trades,
        mode="merged",
        interval_ms=100,
        bbo_data=bbo,
        l2_data=l2,
    )

    assert execution_count == 2
    synthetic = events.loc[~events["_is_execution_trade"]]
    assert synthetic["transact_time"].tolist() == [
        1_050,
        1_075,
        1_100,
        1_150,
        1_200,
        1_300,
    ]
    assert synthetic["quantity"].eq(0.0).all()


def test_strict_calibration_accepts_complete_private_inputs(tmp_path):
    config = tmp_path / "live.current.yaml"
    config.write_text("project_name: NarrowGate\n", encoding="utf-8")
    params = _strict_params(config)
    validate_formal_replay_calibration(params)
    assert params["strict_calibration_validated"] is True


def test_strict_calibration_rejects_queue_parameter_override(tmp_path):
    config = tmp_path / "live.current.yaml"
    config.write_text("project_name: NarrowGate\n", encoding="utf-8")
    params = _strict_params(config)
    params.update(params["queue_calibration_replay_params"])
    params["queue_ahead_base_mult"] = 0.15

    with pytest.raises(RuntimeError, match="does not match queue artifact"):
        validate_formal_replay_calibration(params)


def test_strict_calibration_rejects_p3_identity_or_byte_drift(tmp_path):
    config = tmp_path / "live.current.yaml"
    config.write_text("project_name: NarrowGate\n", encoding="utf-8")
    params = _strict_params(config)
    params["fill_probability_event_type"] = "fill"
    with pytest.raises(RuntimeError, match="event_type=touch"):
        validate_formal_replay_calibration(params)

    params = _strict_params(config)
    Path(params["fill_probability_model_path"]).write_text(
        "mutated-p3-fixture\n", encoding="utf-8"
    )
    with pytest.raises(RuntimeError, match="SHA256 does not match"):
        validate_formal_replay_calibration(params)


def test_queue_calibration_can_be_bound_to_an_explicit_artifact(tmp_path):
    artifact = tmp_path / "queue-v3.json"
    replay_params = {
        "queue_ahead_base_mult": 0.7,
        "queue_deplete_base_mult": 1.0,
        "queue_ahead_buy_exposure_mult": 0.5,
        "queue_ahead_buy_reducing_mult": 1.15,
        "queue_ahead_sell_exposure_mult": 1.45,
        "queue_ahead_sell_reducing_mult": 0.7,
    }
    artifact.write_text(
        json.dumps(
            {
                "schema_version": "narrowgate_queue_calibration.v3",
                "apply_mode": "frozen_default",
                "fit_days": ["2026-07-10", "2026-07-11"],
                "diagnostic_only": True,
                "diagnostic_parent_sha256": "parent-sha",
                "diagnostic_note": "sensitivity only",
                "replay_params": replay_params,
                "days": {"2026-07-10": {}},
            }
        ),
        encoding="utf-8",
    )

    params: dict = {}
    add_queue_calibration_params(
        params,
        symbol="BTCUSDC",
        strict=True,
        path=artifact,
    )

    assert params["queue_calibration_path"] == str(artifact.resolve())
    assert params["queue_calibration_replay_params"] == replay_params
    assert params["queue_calibration_diagnostic_only"] is True
    assert params["queue_calibration_diagnostic_parent_sha256"] == "parent-sha"
    assert params["queue_calibration_diagnostic_note"] == "sensitivity only"
    assert {
        key: params[key] for key in replay_params
    } == replay_params


def test_missing_model_features_fail_unless_exploratory_opt_in(tmp_path):
    features = pd.DataFrame({"present": [1.0]})

    with pytest.raises(RuntimeError, match="formal replay must not synthesize"):
        ensure_model_feature_columns(
            features,
            ["present", "missing"],
            model_name="dir_10s",
            feature_dir=tmp_path,
            allow_missing_features=False,
        )

    exploratory = ensure_model_feature_columns(
        features,
        ["present", "missing"],
        model_name="dir_10s",
        feature_dir=tmp_path,
        allow_missing_features=True,
    )
    assert exploratory["missing"].tolist() == [0.0]


def test_strict_calibration_rejects_public_template_and_zero_latency(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("# PUBLIC TEMPLATE\n", encoding="utf-8")
    params = _strict_params(config)
    params["_new_order_latency_samples_ms"] = [0.0]
    params["_cancel_order_latency_samples_ms"] = [0.0]
    with pytest.raises(RuntimeError, match="public template.*latency calibration"):
        validate_formal_replay_calibration(params)


def test_strict_calibration_rejects_trade_only_event_clock(tmp_path):
    config = tmp_path / "live.current.yaml"
    config.write_text("project_name: NarrowGate\n", encoding="utf-8")
    params = _strict_params(config)
    params["replay_event_clock"] = "trade"
    with pytest.raises(RuntimeError, match="replay_event_clock=merged"):
        validate_formal_replay_calibration(params)


def test_strict_calibration_rejects_legacy_ml_timestamp_contract(tmp_path):
    config = tmp_path / "live.current.yaml"
    config.write_text("project_name: NarrowGate\n", encoding="utf-8")
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "dir_10s_meta.json").write_text(
        json.dumps({"feature_cols": ["vol_regime_zscore"]}),
        encoding="utf-8",
    )
    params = _strict_params(config)
    params["model_dir"] = str(model_dir)
    with pytest.raises(RuntimeError, match="bucket-end feature visibility contract"):
        validate_formal_replay_calibration(params)


def test_strict_calibration_accepts_causal_ml_contract(tmp_path):
    config = tmp_path / "live.current.yaml"
    config.write_text("project_name: NarrowGate\n", encoding="utf-8")
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "dir_10s_meta.json").write_text(
        json.dumps(
            {
                "feature_cols": ["return_1"],
                "feature_timestamp_semantics": "left_label_bucket_end",
                "feature_bucket_ms": 10_000,
                "label_semantics_version": 2,
                "feature_manifest_sha256": "manifest-sha",
                "feature_daily_manifest_sha256": "daily-sha",
                "feature_availability_train": {"return_1": 0.99},
            }
        ),
        encoding="utf-8",
    )
    params = _strict_params(config)
    params["model_dir"] = str(model_dir)
    validate_formal_replay_calibration(params)
    assert params["strict_calibration_validated"] is True


def test_integrity_diagnostics_define_two_three_arm_tests_with_shared_baseline():
    arms = {arm.name: arm for arm in _integrity_diagnostic_arms()}
    assert arms["markout_asym_off"].overrides["markout_spread_scale"] == 0.0
    assert arms["markout_asym_sign_corrected"].overrides["markout_side_asymmetry_sign"] == 1.0
    assert arms["cap_pause_exposure"].overrides["spread_cap_mode"] == "pause_exposure"
    assert arms["cap_observe_only"].overrides["spread_cap_mode"] == "observe"


def test_random_passive_arms_are_reproducible_and_executable_specs():
    arms = _random_passive_arms(
        3,
        seed=100,
        side_mirror_prob=0.5,
        timing_jitter_fraction=0.35,
    )
    assert [arm.name for arm in arms] == [
        "random_passive_100",
        "random_passive_101",
        "random_passive_102",
    ]
    assert all(arm.overrides["random_passive_enabled"] for arm in arms)
    assert all(arm.overrides["random_passive_preserve_inventory_skew"] for arm in arms)


def test_cpp_parity_days_default_explicit_and_all():
    replay_days = ["2026-04-18", "2026-05-30", "2026-06-10"]
    assert _resolve_cpp_parity_days(replay_days, []) == ["2026-04-18"]
    assert _resolve_cpp_parity_days(replay_days, ["all"]) == replay_days
    assert _resolve_cpp_parity_days(
        replay_days,
        ["2026-06-10", "2026-04-18"],
    ) == ["2026-04-18", "2026-06-10"]
    with pytest.raises(ValueError, match="must be included"):
        _resolve_cpp_parity_days(replay_days, ["2026-07-04"])


def test_random_passive_null_table_reports_activity_and_pooled_baseline_gap():
    rows = []
    for day, baseline_pnl in (("2026-07-01", -2.0), ("2026-07-02", 1.0)):
        common = {
            "day": day,
            "replay_inv_adj": -0.5,
            "mtm_before_terminal_fee": baseline_pnl + 0.1,
            "terminal_fee_drag": 0.0,
            "terminal_liquidation_fee_estimate": 0.1,
            "fills_total": 100,
            "replay_abs_inventory_time_s": 50.0,
            "terminal_pnl_sum": baseline_pnl + 0.5,
            "loss_tail": 1,
            "avg_markout": -2.0,
            "replay_avg_final_spread": 60.0,
            "replay_n_final_spread": 1000,
            "decision_place_rate": 0.38,
            "decision_replace_rate": 0.28,
            "decision_pause_rate": 0.08,
            "decision_keep_rate": 0.12,
            "decision_pending_coalesce_rate": 0.01,
            "decision_cancel_first_rate": 0.0,
            "decision_total": 1000,
            "buy_fill_share": 0.5,
        }
        rows.append({**common, "arm": "baseline", "group": "baseline", "replay_pnl": baseline_pnl})
        for idx, random_pnl in enumerate((baseline_pnl - 1.0, baseline_pnl + 1.0)):
            rows.append(
                {
                    **common,
                    "arm": f"random_passive_{idx}",
                    "group": "executable_random_passive",
                    "replay_pnl": random_pnl,
                    "fills_total": 110,
                    "replay_abs_inventory_time_s": 40.0,
                    "loss_tail": 0,
                }
            )

    table = _random_passive_null_table(pd.DataFrame(rows))
    pooled = table.loc[table["scope"] == "pooled"].iloc[0]
    assert pooled["trials"] == 2
    assert pooled["baseline_replay_pnl"] == pytest.approx(-1.0)
    assert pooled["random_replay_pnl_mean"] == pytest.approx(-1.0)
    assert pooled["fills_retention_baseline_over_random_mean"] == pytest.approx(200.0 / 220.0)
    assert pooled["inventory_time_ratio_baseline_over_random_mean"] == pytest.approx(100.0 / 80.0)
    assert pooled["baseline_minus_random_loss_tail_mean"] == pytest.approx(2.0)
