import argparse
import hashlib
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.families.f07_active_order_continuation.audit.dynamic_fill_hazard import (
    CALIBRATOR_CONTRACT,
    MODEL_FEATURES,
    OPTIMIZER_CONTRACT,
    ChronologicalFold,
    _canonical_sha256,
    _cause_gate,
    _fit_cloglog_hazard,
    _fit_nested_calibrated_hazard,
    _predict_nested_model,
    _read_split_development_folds,
    build_delayed_entry_repair_risk_set,
    build_dynamic_fill_risk_set,
    evaluate_validation,
    fit_development,
    freeze_spec,
)
from models.audit.order_lifecycle import OrderLifecycleRecorder


def _order(order_id: int, side: str, *, campaign_id: int = 0) -> dict:
    return {
        "trace_id": order_id,
        "side": side,
        "price": 100.0 if side == "BUY" else 100.2,
        "quantity": 0.001,
        "remaining": 0.001,
        "inventory_at_submit": 0.0,
        "inventory_role_at_submit": "add" if campaign_id else "opener",
        "campaign_id_at_submit": campaign_id,
        "state": "PENDING_NEW",
        "fill_eligible": True,
        "exchange_book_queue_ambiguous": False,
    }


def _features() -> dict:
    return {
        "exact_queue_path_valid": 1,
        "queue_path_ambiguous": 0,
        "visible_state_age_ms": 5.0,
        "spread_ticks": 2.0,
        "quote_distance_ticks": 1.0,
        "best_bid": 100.0,
        "best_ask": 100.2,
        "top_bid_size": 2.0,
        "top_ask_size": 1.0,
        "book_imbalance": 1.0 / 3.0,
        "microprice_shift_ticks": 0.25,
        "side_microprice_adverse_ticks": 0.0,
        "policy_queue_initial": 1.0,
        "policy_queue_remaining": 0.8,
        "policy_queue_fraction_left": 0.8,
        "policy_queue_progress": 0.2,
        "visible_cancel_events": 1.0,
        "visible_cancel_size": 0.2,
        "visible_refill_events": 2.0,
        "visible_refill_size": 0.4,
        "visible_refill_event_share": 2.0 / 3.0,
        "clock_hour_sin": 0.0,
        "clock_hour_cos": 1.0,
    }


def _snapshot(
    recorder: OrderLifecycleRecorder,
    order: dict,
    ts_ms: int,
    *,
    role: str,
    inventory: float,
    campaign_id: int,
) -> None:
    recorder.risk_snapshot(
        order,
        ts_ms,
        feature_source_ts_ns=(ts_ms - 5) * 1_000_000,
        feature_ready_ts_ns=ts_ms * 1_000_000,
        inventory_role=role,
        inventory=inventory,
        campaign_id=campaign_id,
        mid=100.1,
        microprice=100.12,
        top_size=2.0,
        features=_features(),
    )


def _synthetic_risk_rows(
    days: list[str],
    *,
    side: str,
    rows_per_day: int = 200,
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for day_index, day in enumerate(days):
        row_index = np.arange(rows_per_day)
        favorable = (row_index % 50 == 0).astype(int)
        adverse = (row_index % 50 == 1).astype(int)
        frame = pd.DataFrame(
            {
                name: np.zeros(rows_per_day, dtype=float)
                for name in MODEL_FEATURES
            }
        )
        frame["day"] = day
        frame["side"] = side
        frame["risk_row_id"] = [
            f"risk:{day}:{side}:{index}" for index in row_index
        ]
        frame["order_id"] = day_index * rows_per_day + row_index
        frame["campaign_id"] = day_index * rows_per_day + row_index
        frame["current_inventory_role"] = "add"
        frame["risk_interval_ms"] = 100.0
        frame["favorable_fill"] = favorable
        frame["adverse_fill"] = adverse
        frame["fill_event"] = np.where(
            favorable > 0,
            "favorable_fill",
            np.where(adverse > 0, "adverse_fill", "none"),
        )
        frame["fill_value_markout_bps"] = favorable - adverse
        frame["side_microprice_adverse_ticks"] = (
            adverse - favorable + 0.01 * day_index
        )
        parts.append(frame)
    return pd.concat(parts, ignore_index=True)


def test_dynamic_fill_risk_treats_cancel_as_censor_and_jump_as_transition() -> None:
    recorder = OrderLifecycleRecorder(
        symbol="BTCUSDC",
        lot_size=0.001,
        tick_size=0.1,
        price_jump_ticks=1.0,
        max_orders=10,
        risk_snapshot_edges_ms=(0, 100),
    )
    filled = _order(1, "BUY", campaign_id=1)
    recorder.submit(filled, 1_000)
    filled["state"] = "OPEN"
    recorder.activate(filled, 1_010, mid=100.1)
    _snapshot(
        recorder,
        filled,
        1_020,
        role="add",
        inventory=0.001,
        campaign_id=1,
    )
    recorder.native_mid(
        1_050_000_000,
        99.9,
        segment_id=1,
        same_ms_ordering_resolved=True,
    )
    _snapshot(
        recorder,
        filled,
        1_120,
        role="add",
        inventory=0.001,
        campaign_id=1,
    )
    filled["remaining"] = 0.0
    recorder.fill(
        filled,
        1_150,
        fill_qty=0.001,
        remaining_before=0.001,
        remaining_after=0.0,
        fill_price=100.0,
        inventory_before=0.001,
        inventory_after=0.002,
        campaign_id=1,
    )
    recorder.annotate_fill_value(
        filled,
        markout_bps=-1.0,
        horizon_ms=1_000,
        horizon_censored=False,
        observation_ts_ns=2_150_000_000,
        observation_mid=99.99,
    )

    cancelled = _order(2, "SELL", campaign_id=2)
    recorder.submit(cancelled, 2_000)
    cancelled["state"] = "OPEN"
    recorder.activate(cancelled, 2_010, mid=100.1)
    _snapshot(
        recorder,
        cancelled,
        2_020,
        role="add",
        inventory=-0.001,
        campaign_id=2,
    )
    cancelled["state"] = "PENDING_CANCEL"
    recorder.request_cancel(cancelled, 2_080, reason="requote")

    risk, identity = build_dynamic_fill_risk_set(
        pd.DataFrame(recorder.events())
    )

    buy = risk[risk["side"].eq("BUY")]
    assert len(buy) == 2
    assert buy.iloc[0]["next_event_type"] == "risk_snapshot"
    assert buy.iloc[0]["native_jump_transitions_in_interval"] == 1
    assert buy.iloc[1]["fill_event"] == "adverse_fill"
    sell = risk[risk["side"].eq("SELL")].iloc[0]
    assert sell["fill_event"] == "censored"
    assert sell["cancel_action_censor"] == 1
    assert identity["jump_is_nonabsorbing_transition"] is True
    assert identity["cancel_is_action_or_censor"] is True

    second_day = pd.DataFrame(recorder.events())
    second_day["day"] = "1970-01-02"
    cross_day, _ = build_dynamic_fill_risk_set(
        pd.concat(
            [pd.DataFrame(recorder.events()), second_day],
            ignore_index=True,
        )
    )
    assert len(cross_day) == 2 * len(risk)
    assert cross_day["risk_row_id"].is_unique


def test_repair_risk_starts_only_after_reducing_quote_is_eligible() -> None:
    recorder = OrderLifecycleRecorder(
        symbol="BTCUSDC",
        lot_size=0.001,
        tick_size=0.1,
        price_jump_ticks=1.0,
        max_orders=10,
        risk_snapshot_edges_ms=(0,),
    )
    opener = _order(3, "BUY", campaign_id=3)
    reducing = _order(4, "SELL", campaign_id=3)
    for order in (opener, reducing):
        recorder.submit(order, 3_000)
        order["state"] = "OPEN"
        recorder.activate(order, 3_010, mid=100.1)
        recorder.bind_campaign(order, 3)

    recorder.sync_repair_state(
        3_015,
        campaign_id=3,
        campaign_active=True,
        inventory=0.001,
        active_orders=[opener],
    )
    _snapshot(
        recorder,
        opener,
        3_020,
        role="add",
        inventory=0.001,
        campaign_id=3,
    )
    recorder.sync_repair_state(
        3_025,
        campaign_id=3,
        campaign_active=True,
        inventory=0.001,
        active_orders=[opener, reducing],
    )
    _snapshot(
        recorder,
        reducing,
        3_030,
        role="reducing",
        inventory=0.001,
        campaign_id=3,
    )
    recorder.campaign_repair(3, 3_080)

    repair, identity = build_delayed_entry_repair_risk_set(
        pd.DataFrame(recorder.events())
    )
    assert len(repair) == 1
    assert repair.iloc[0]["risk_interval_start_ts_ns"] == 3_030_000_000
    assert repair.iloc[0]["repair_event"] == 1
    assert repair.iloc[0]["campaign_side"] == "BUY"
    assert identity["pre_entry_rows"] == 0
    assert identity["delayed_entry_identity_passed"] is True

    lifecycle = pd.DataFrame(recorder.events())
    second_day = lifecycle.copy()
    second_day["day"] = "1970-01-02"
    cross_day, cross_identity = build_delayed_entry_repair_risk_set(
        pd.concat([lifecycle, second_day], ignore_index=True)
    )
    assert len(cross_day) == 2 * len(repair)
    assert cross_day["repair_risk_row_id"].is_unique
    assert cross_identity["campaigns"] == 2


def test_cloglog_fit_bounds_extreme_standardized_features() -> None:
    rows = 400
    frame = pd.DataFrame(
        {
            name: np.zeros(rows, dtype=float)
            for name in MODEL_FEATURES
        }
    )
    frame["risk_interval_ms"] = 100.0
    frame["favorable_fill"] = 0
    frame.loc[::40, "favorable_fill"] = 1
    frame["side_microprice_adverse_ticks"] = np.linspace(-1.0, 1.0, rows)
    frame.loc[rows - 1, "side_microprice_adverse_ticks"] = 1e6
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        model = _fit_cloglog_hazard(
            frame,
            target="favorable_fill",
            l2_penalty=1.0,
            side="BUY",
            cause="favorable_fill",
            train_days=["2026-01-01"],
        )
    coefficients = np.asarray(model["coefficients"], dtype=float)
    assert np.isfinite(coefficients).all()
    assert np.abs(coefficients).max() <= 8.0


def test_probability_gate_distinguishes_positive_tendency_from_strict_ci() -> None:
    metrics = {
        "events": 100,
        "days": 10,
        "average_precision_lift": 2.0,
        "mean_top20_lift": 1.5,
        "daily_high_low_positive_rate": 0.7,
        "observed_to_expected": 1.0,
        "brier_skill": 0.001,
        "brier_improvement_ci_lower": -1e-6,
        "brier_improvement_bootstrap_probability_positive": 0.77,
    }
    shared = {
        "minimum_events_per_side_cause": 20,
        "minimum_oof_days": 4,
        "minimum_average_precision_lift": 1.1,
        "minimum_within_day_top20_lift": 1.15,
        "minimum_daily_high_low_positive_rate": 0.55,
        "observed_to_expected_min": 0.7,
        "observed_to_expected_max": 1.3,
        "minimum_brier_skill": 0.0,
    }

    strict_passed, strict_failures = _cause_gate(
        metrics,
        {
            **shared,
            "minimum_day_cluster_brier_improvement_lower": 0.0,
        },
    )
    screen_passed, screen_failures = _cause_gate(
        metrics,
        {
            **shared,
            "minimum_day_cluster_brier_improvement_positive_probability": 0.75,
        },
    )

    assert strict_passed is False
    assert strict_failures == ["day_cluster_brier_improvement_lower"]
    assert screen_passed is True
    assert screen_failures == []


def test_nested_calibrator_uses_inner_oof_from_outer_train_only() -> None:
    days = pd.date_range("2026-01-01", periods=12, freq="D").strftime(
        "%Y-%m-%d"
    ).tolist()
    frame = _synthetic_risk_rows(days, side="BUY")
    contract = dict(CALIBRATOR_CONTRACT)
    contract.update(
        {
            "inner_min_train_days": 5,
            "inner_embargo_days": 1,
            "inner_test_days": 3,
            "minimum_rows": 100,
            "minimum_events": 4,
            "minimum_nonevents": 20,
            "minimum_inner_oof_days": 3,
            "day_cluster_bootstrap_samples": 100,
        }
    )

    model = _fit_nested_calibrated_hazard(
        frame,
        target="favorable_fill",
        l2_penalty=1.0,
        side="BUY",
        cause="favorable_fill",
        train_days=days,
        calibration_contract=contract,
    )

    calibrator = model["nested_calibrator"]
    assert calibrator["train_days"] == days[6:]
    assert calibrator["train_day_count"] == 6
    assert all(
        fold["train_days"][0] == days[0]
        for fold in calibrator["inner_folds"]
    )
    assert all(
        set(fold["train_days"]).isdisjoint(fold["test_days"])
        for fold in calibrator["inner_folds"]
    )
    raw, calibrated = _predict_nested_model(model, frame.iloc[:100])
    assert np.isfinite(raw).all()
    assert np.isfinite(calibrated).all()
    assert np.all((calibrated >= 0.0) & (calibrated <= 1.0))


def test_frozen_outer_folds_are_read_without_regeneration(
    tmp_path: Path,
) -> None:
    days = pd.date_range("2026-01-01", periods=9, freq="D").strftime(
        "%Y-%m-%d"
    ).tolist()
    split = {
        "development_folds": [
            {
                "fold": 7,
                "train_days": days[:5],
                "embargo_days": days[5:6],
                "test_days": days[6:9],
            }
        ],
        "development_oof_days": days[6:9],
    }
    path = tmp_path / "split.json"
    path.write_text(json.dumps(split), encoding="utf-8")

    folds = _read_split_development_folds(path, days)

    assert folds == (
        ChronologicalFold(
            fold=7,
            train_days=tuple(days[:5]),
            embargo_days=tuple(days[5:6]),
            test_days=tuple(days[6:9]),
        ),
    )


def test_fill_and_repair_use_same_nested_outer_contract() -> None:
    days = pd.date_range("2026-01-01", periods=16, freq="D").strftime(
        "%Y-%m-%d"
    ).tolist()
    fill = pd.concat(
        [
            _synthetic_risk_rows(days, side="BUY", rows_per_day=100),
            _synthetic_risk_rows(days, side="SELL", rows_per_day=100),
        ],
        ignore_index=True,
    )
    repair_parts: list[pd.DataFrame] = []
    for side in ("BUY", "SELL"):
        part = _synthetic_risk_rows(
            days,
            side=side,
            rows_per_day=100,
        ).copy()
        part["campaign_side"] = side
        part["repair_risk_row_id"] = (
            "repair:" + part["risk_row_id"].astype(str)
        )
        part["repair_event"] = part["favorable_fill"]
        part["repair_next_event_type"] = np.where(
            part["repair_event"].gt(0),
            "campaign_repair",
            "risk_snapshot",
        )
        repair_parts.append(part)
    repair = pd.concat(repair_parts, ignore_index=True)
    contract = dict(CALIBRATOR_CONTRACT)
    contract.update(
        {
            "inner_min_train_days": 5,
            "inner_embargo_days": 1,
            "inner_test_days": 3,
            "minimum_rows": 100,
            "minimum_events": 4,
            "minimum_nonevents": 20,
            "minimum_inner_oof_days": 3,
            "day_cluster_bootstrap_samples": 100,
        }
    )
    gates = {
        "minimum_oof_days": 1,
        "minimum_events_per_side_cause": 1,
        "minimum_average_precision_lift": 0.0,
        "minimum_within_day_top20_lift": 0.0,
        "minimum_daily_high_low_positive_rate": 0.0,
        "observed_to_expected_min": 0.0,
        "observed_to_expected_max": 10.0,
        "minimum_brier_skill": -10.0,
        "minimum_day_cluster_brier_improvement_lower": -10.0,
        "minimum_repair_events_per_campaign_side": 1,
    }
    outer_fold = ChronologicalFold(
        fold=1,
        train_days=tuple(days[:12]),
        embargo_days=tuple(days[12:13]),
        test_days=tuple(days[13:16]),
    )

    fill_predictions, repair_predictions, summary, bundle = fit_development(
        fill,
        repair,
        gates=gates,
        l2_penalty=1.0,
        family_id="nested-test",
        action_experiment_id="nested-action-test",
        outer_folds=(outer_fold,),
        calibration_contract=contract,
        require_repair_prediction_gate=True,
    )

    assert sorted(fill_predictions["day"].unique()) == days[13:16]
    assert sorted(repair_predictions["day"].unique()) == days[13:16]
    audits = summary["nested_calibration"]
    assert len(audits["fill_outer_model_audits"]) == 4
    assert len(audits["repair_outer_model_audits"]) == 2
    for audit in (
        audits["fill_outer_model_audits"]
        + audits["repair_outer_model_audits"]
    ):
        assert audit["outer_train_days"] == days[:12]
        assert audit["outer_test_days"] == days[13:16]
        assert not (
            set(audit["calibration_train_days"]) & set(days[13:16])
        )
    assert bundle["nested_calibration"]["enabled"] is True
    assert all(
        "nested_calibrator" in bundle["repair_models"][side]
        for side in ("BUY", "SELL")
    )

    validation_days = ["2026-01-17", "2026-01-18"]
    validation_fill = pd.concat(
        [
            _synthetic_risk_rows(
                validation_days,
                side="BUY",
                rows_per_day=100,
            ),
            _synthetic_risk_rows(
                validation_days,
                side="SELL",
                rows_per_day=100,
            ),
        ],
        ignore_index=True,
    )
    validation_repair = _synthetic_risk_rows(
        validation_days,
        side="BUY",
        rows_per_day=100,
    ).copy()
    validation_repair["campaign_side"] = "BUY"
    validation_repair["repair_risk_row_id"] = (
        "validation-repair:" + validation_repair["risk_row_id"].astype(str)
    )
    validation_repair["repair_event"] = validation_repair["favorable_fill"]
    validation_repair["repair_next_event_type"] = np.where(
        validation_repair["repair_event"].gt(0),
        "campaign_repair",
        "risk_snapshot",
    )

    validation_fill_predictions, validation_repair_predictions, result = (
        evaluate_validation(
            validation_fill,
            validation_repair,
            bundle=bundle,
            strict_gates=gates,
            admitted_sides=["BUY"],
            minimum_favorable_probability_positive=0.0,
            calibration_contract=contract,
        )
    )

    assert set(validation_fill_predictions["side"]) == {"BUY"}
    assert set(validation_repair_predictions["campaign_side"]) == {"BUY"}
    assert result["admitted_sides"] == ["BUY"]
    assert result["model_refit_on_validation"] is False
    assert result["calibrator_refit_on_validation"] is False
    assert result["sealed_holdout_access_allowed"] is False


def test_freeze_spec_preserves_and_validates_source_manifest_identity(
    tmp_path: Path,
) -> None:
    source_manifest = tmp_path / "features.json"
    source_manifest.write_text('{"schema_version":"test"}\n', encoding="utf-8")
    source_hash = hashlib.sha256(source_manifest.read_bytes()).hexdigest()
    source_action = {
        "family_id": "source",
        "surface": "test",
        "actions": ["baseline", "candidate"],
        "behavior_probabilities": {"baseline": 0.5, "candidate": 0.5},
    }
    source_split = {
        "schema_version": "narrowgate_evidence_split.v1",
        "family_id": "source",
        "split_mode": "explicit_chronological_existing_good_days",
        "panels": {
            "development": {"days": ["2026-01-01"], "sealed": False},
            "embargo_1": {"days": ["2026-01-02"], "sealed": False},
            "validation": {"days": ["2026-01-03"], "sealed": False},
            "embargo_2": {"days": ["2026-01-04"], "sealed": False},
            "sealed_holdout": {"days": ["2026-01-05"], "sealed": True},
        },
        "source_manifest_path": str(source_manifest),
        "source_manifest_sha256": source_hash,
        "source_daily_manifest_sha256": "daily-test",
        "action_family": source_action,
        "action_family_sha256": _canonical_sha256(source_action),
    }
    source_split_path = tmp_path / "source_split.json"
    source_split_path.write_text(json.dumps(source_split), encoding="utf-8")
    artifacts: list[Path] = []
    for name in ("config", "p3", "queue", "latency", "visibility"):
        path = tmp_path / name
        path.write_text(name, encoding="utf-8")
        artifacts.append(path)
    args = argparse.Namespace(
        source_split=source_split_path,
        output_split=tmp_path / "frozen_split.json",
        output_spec=tmp_path / "family_spec.json",
        config=artifacts[0],
        p3_artifact=artifacts[1],
        queue_artifact=artifacts[2],
        latency_artifact=artifacts[3],
        visibility_artifact=artifacts[4],
        family_id="dynamic_fill_hazard_m0_native_strict_test",
        action_experiment_id="queue_value_keep_cancel_native_strict_test",
    )

    freeze_spec(args)
    frozen = json.loads(args.output_split.read_text(encoding="utf-8"))
    assert frozen["source_manifest_path"] == str(source_manifest)
    assert frozen["source_manifest_sha256"] == source_hash
    assert frozen["source_daily_manifest_sha256"] == "daily-test"
    assert frozen["family_id"] == "dynamic_fill_hazard_m0_native_strict_test"
    assert (
        frozen["action_family"]["family_id"]
        == "queue_value_keep_cancel_native_strict_test"
    )
    assert frozen["action_family"]["sides"] == ["BUY", "SELL"]
    assert frozen["action_family"]["inventory_role"] == "add"
    spec = json.loads(args.output_spec.read_text(encoding="utf-8"))
    assert spec["family_id"] == "dynamic_fill_hazard_m0_native_strict_test"
    assert spec["model"]["optimizer"] == OPTIMIZER_CONTRACT
    assert spec["model"]["nested_calibration"]["enabled"] is True
    assert (
        spec["model"]["nested_calibration"]["contract"]
        == CALIBRATOR_CONTRACT
    )
    assert spec["model"]["repair_outer_oof"] is True
    assert spec["model"]["require_repair_prediction_gate"] is True

    source_manifest.write_text('{"changed":true}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="source feature manifest"):
        freeze_spec(args)


def test_freeze_spec_accepts_strict_native_split_and_preserves_fold_contract(
    tmp_path: Path,
) -> None:
    days = pd.date_range("2026-01-01", periods=5, freq="D").strftime(
        "%Y-%m-%d"
    )
    strict_days = tmp_path / "strict_days.csv"
    pd.DataFrame({"day": days}).to_csv(strict_days, index=False)
    strict_hash = hashlib.sha256(strict_days.read_bytes()).hexdigest()
    panel_names = (
        "development",
        "embargo_1",
        "validation",
        "embargo_2",
        "sealed_holdout",
    )
    source_split = {
        "schema_version": "strict_native_evidence_split.v1",
        "family_id": "strict_source",
        "strict_days_path": str(strict_days),
        "strict_days_sha256": strict_hash,
        "strict_days_count": 5,
        "panels": {
            name: {
                "days": [days[index]],
                "day_count": 1,
                "sealed": name == "sealed_holdout",
                "trainable": name == "development",
            }
            for index, name in enumerate(panel_names)
        },
        "fold_contract": {
            "min_train_days": 20,
            "embargo_days": 1,
            "test_days": 5,
        },
        "development_folds": [],
        "development_oof_days": [],
        "development_oof_day_count": 0,
    }
    source_split_path = tmp_path / "strict_source_split.json"
    source_split_path.write_text(json.dumps(source_split), encoding="utf-8")
    artifacts: list[Path] = []
    for name in ("config", "p3", "queue", "latency", "visibility"):
        path = tmp_path / name
        path.write_text(name, encoding="utf-8")
        artifacts.append(path)
    args = argparse.Namespace(
        source_split=source_split_path,
        output_split=tmp_path / "frozen_split.json",
        output_spec=tmp_path / "family_spec.json",
        config=artifacts[0],
        p3_artifact=artifacts[1],
        queue_artifact=artifacts[2],
        latency_artifact=artifacts[3],
        visibility_artifact=artifacts[4],
        family_id="native_strict_test",
        action_experiment_id="native_strict_action_test",
    )

    freeze_spec(args)

    frozen = json.loads(args.output_split.read_text(encoding="utf-8"))
    spec = json.loads(args.output_spec.read_text(encoding="utf-8"))
    assert frozen["source_manifest_path"] == str(strict_days)
    assert frozen["source_manifest_sha256"] == strict_hash
    assert spec["model"]["minimum_train_days"] == 20
    assert spec["model"]["embargo_days"] == 1
    assert spec["model"]["test_days"] == 5
