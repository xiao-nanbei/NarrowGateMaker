import hashlib
import json

import numpy as np
import pytest

from models.replay_contract import (
    INDIVIDUAL_TRADES_REPAIR_ID,
    INDIVIDUAL_TRADES_REPAIRED_DAYS,
    KEYED_LATENCY_SAMPLER_VERSION,
    STANDARD_INITIAL_STATE_SCHEMA,
    configure_fixed_latency_distribution,
    freeze_replay_contract,
    validate_frozen_replay_contract,
)
from strategy.replay_controls import (
    LOSS_COOLDOWN_SEMANTICS,
    SYNC_DEGRADE_SEMANTICS,
    SYNC_DEGRADE_TAPE_SCHEMA,
)
from research.families.f01_fixed_parameter_racing.campaign_outcome_replay_audit import (
    main as replay_audit_main,
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
        "_cancel_order_latency_samples_ms": np.asarray([3.0, 4.0, 300.0]),
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
