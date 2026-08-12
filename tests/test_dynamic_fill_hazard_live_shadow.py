import hashlib
import json
import math
from types import SimpleNamespace

import pytest

from strategy.dynamic_fill_hazard_model import (
    MODEL_FEATURES,
    DynamicFillHazardActionPolicy,
    DynamicFillHazardBundle,
    DynamicFillHazardShadowRuntime,
    build_dynamic_fill_hazard_features,
)


def _canonical_sha256(value):
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _head(cause):
    return {
        "schema_version": "cause_specific_discrete_cloglog.v1",
        "side": "BUY",
        "cause": cause,
        "feature_names": list(MODEL_FEATURES),
        "feature_mean": [0.0] * len(MODEL_FEATURES),
        "feature_scale": [1.0] * len(MODEL_FEATURES),
        "intercept": math.log(0.1 if cause == "favorable_fill" else 0.2),
        "coefficients": [0.0] * len(MODEL_FEATURES),
        "baseline_rate_per_second": 0.1,
        "nested_calibrator": {
            "schema_version": "nested_affine_cloglog_calibrator.v1",
            "contract": {
                "probability_clip": [1e-9, 1.0 - 1e-9],
            },
            "intercept": 0.0,
            "slope": 1.0,
        },
    }


def _write_bundle(tmp_path):
    payload = {
        "schema_version": "dynamic_fill_hazard_bundle.v2",
        "family_id": "test_dynamic_fill_hazard",
        "feature_names": list(MODEL_FEATURES),
        "gates": {},
        "development_days": ["2026-01-01"],
        "models": {
            "BUY": {
                "favorable_fill": _head("favorable_fill"),
                "adverse_fill": _head("adverse_fill"),
            }
        },
        "repair_models": {},
        "nested_calibration": {"enabled": True},
        "prediction_gate_passed_sides": [],
        "action_experiment_id": "none",
        "action_family_allowed": False,
    }
    payload["bundle_sha256"] = _canonical_sha256(payload)
    path = tmp_path / "bundle.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_policy(tmp_path, bundle_sha):
    payload = {
        "schema_version": "dynamic_fill_hazard_live_policy.v1",
        "policy_id": "test_buy_q90",
        "model_family_id": "test_dynamic_fill_hazard",
        "model_file_sha256": bundle_sha,
        "side": "BUY",
        "eligible_roles": ["opener", "add"],
        "score_formula": (
            "probability_adverse_fill-probability_favorable_fill"
        ),
        "entry_threshold": 0.005,
        "entry_action": "cancel",
        "recovery_rule": "score_below_entry_threshold",
        "reentry_action": "baseline_reenter",
        "evaluation_interval_ms": 100.0,
        "reducing_side_unchanged": True,
        "validation_activation_rate": 0.09,
    }
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_live_feature_transform_matches_frozen_names():
    raw = {
        "risk_snapshot_elapsed_ms": 100.0,
        "visible_state_age_ms": 25.0,
        "policy_queue_initial": 2.0,
        "policy_queue_remaining": 1.0,
        "visible_cancel_events": 3.0,
        "visible_cancel_size": 0.4,
        "visible_refill_events": 2.0,
        "visible_refill_size": 0.3,
        "time_since_native_adverse_jump_ms": -1.0,
        "current_inventory_role": "add",
    }
    features = build_dynamic_fill_hazard_features(raw)
    assert tuple(features) == MODEL_FEATURES
    assert features["elapsed_log1p"] == pytest.approx(math.log1p(100.0))
    assert features["queue_initial_log1p"] == pytest.approx(math.log1p(2.0))
    assert features["role_add"] == 1.0
    assert features["role_opener"] == 0.0


def test_prediction_only_bundle_verifies_hash_and_scores(tmp_path):
    path = _write_bundle(tmp_path)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    bundle = DynamicFillHazardBundle.load(
        path,
        expected_file_sha256=sha,
        shadow_sides=("BUY",),
    )
    raw = {
        "risk_snapshot_elapsed_ms": 0.0,
        "visible_state_age_ms": 0.0,
        "policy_queue_initial": 0.0,
        "policy_queue_remaining": 0.0,
        "visible_cancel_events": 0.0,
        "visible_cancel_size": 0.0,
        "visible_refill_events": 0.0,
        "visible_refill_size": 0.0,
        "time_since_native_adverse_jump_ms": -1.0,
        "current_inventory_role": "opener",
    }
    prediction = bundle.predict(
        side="BUY",
        raw_features=raw,
        exposure_ms=100.0,
    )
    assert prediction.favorable_probability == pytest.approx(
        -math.expm1(-0.01)
    )
    assert prediction.adverse_probability == pytest.approx(
        -math.expm1(-0.02)
    )
    assert not bundle.action_family_allowed


def test_shadow_runtime_uses_deep_book_and_emits_registered_edges(tmp_path):
    path = _write_bundle(tmp_path)
    bundle = DynamicFillHazardBundle.load(path, shadow_sides=("BUY",))
    runtime = DynamicFillHazardShadowRuntime(
        bundle,
        tick_size=0.1,
        lot_size=0.001,
        exposure_ms=100.0,
        price_jump_ticks=1.0,
        edges_ms=(0, 100, 200),
    )
    path_state = SimpleNamespace(
        valid=True,
        invalid_reason="",
        generation=3,
        receive_ts_ns=1_000_000_000,
        feature_ready_ts_ns=1_000_000_000,
        activation_ts_ns=900_000_000,
        age_ms=5.0,
        initial_visible_qty=2.0,
        queue_ahead_estimate=1.2,
        inferred_cancel_events=1,
        inferred_cancel_qty=0.3,
        refill_events=1,
        refill_qty=0.5,
    )
    deep = {
        "valid": 1,
        "best_bid": 99.9,
        "best_bid_qty": 2.0,
        "best_ask": 100.1,
        "best_ask_qty": 1.0,
        "age_ms": 5.0,
        "last_trade_receive_ts_ns": 999_000_000,
    }
    first = runtime.evaluate(
        client_order_id="mm_B_1",
        side="BUY",
        order_price=98.0,
        inventory=0.001,
        path=path_state,
        deep_book=deep,
        now_ns=1_000_000_000,
    )
    assert first is not None
    assert first.valid
    assert first.edge_ms == 100
    assert first.inventory_role == "add"
    assert runtime.evaluate(
        client_order_id="mm_B_1",
        side="BUY",
        order_price=98.0,
        inventory=0.001,
        path=path_state,
        deep_book=deep,
        now_ns=1_050_000_000,
    ) is None
    second = runtime.evaluate(
        client_order_id="mm_B_1",
        side="BUY",
        order_price=98.0,
        inventory=0.001,
        path=path_state,
        deep_book=deep,
        now_ns=1_100_000_000,
    )
    assert second is not None
    assert second.edge_ms == 200
    assert second.feature_source_ts_ns <= second.feature_ready_ts_ns
    assert second.feature_ready_ts_ns == 1_000_000_000


def test_action_policy_is_separately_hashed_and_buy_exposure_only(tmp_path):
    bundle_path = _write_bundle(tmp_path)
    bundle_sha = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
    bundle = DynamicFillHazardBundle.load(
        bundle_path,
        expected_file_sha256=bundle_sha,
        shadow_sides=("BUY",),
    )
    policy_path = _write_policy(tmp_path, bundle_sha)
    policy_sha = hashlib.sha256(policy_path.read_bytes()).hexdigest()
    policy = DynamicFillHazardActionPolicy.load(
        policy_path,
        expected_file_sha256=policy_sha,
        model_bundle=bundle,
    )
    base = dict(
        client_order_id="mm_B_1",
        side="BUY",
        inventory_role="add",
        valid=True,
        reason="ok",
        edge_ms=100,
        elapsed_ms=100.0,
        missed_edges=0,
        feature_source_ts_ns=1,
        feature_ready_ts_ns=2,
        deep_generation=1,
        deep_age_ms=1.0,
        order_price=99.0,
        mid=100.0,
        microprice=100.0,
        queue_initial=1.0,
        queue_remaining=1.0,
        cancel_events=0,
        cancel_qty=0.0,
        refill_events=0,
        refill_qty=0.0,
        favorable_probability=0.001,
        adverse_probability=0.007,
        favorable_raw_probability=0.001,
        adverse_raw_probability=0.007,
        model_family_id=bundle.family_id,
    )
    from strategy.dynamic_fill_hazard_model import (
        DynamicFillHazardShadowObservation,
    )

    adverse = DynamicFillHazardShadowObservation(**base)
    assert policy.cancel_required(adverse)
    recovered = DynamicFillHazardShadowObservation(
        **{
            **base,
            "adverse_probability": 0.004,
        }
    )
    assert policy.recovered(recovered)
    reducing = DynamicFillHazardShadowObservation(
        **{
            **base,
            "inventory_role": "reducing",
        }
    )
    assert not policy.cancel_required(reducing)


def test_action_runtime_scores_every_100ms_beyond_shadow_edges(tmp_path):
    bundle_path = _write_bundle(tmp_path)
    bundle = DynamicFillHazardBundle.load(
        bundle_path,
        shadow_sides=("BUY",),
    )
    runtime = DynamicFillHazardShadowRuntime(
        bundle,
        tick_size=0.1,
        lot_size=0.001,
        exposure_ms=100.0,
        price_jump_ticks=1.0,
        edges_ms=(0, 100),
        evaluation_interval_ms=100.0,
    )
    path_state = SimpleNamespace(
        valid=True,
        invalid_reason="",
        generation=3,
        receive_ts_ns=1_000_000_000,
        age_ms=5.0,
        initial_visible_qty=2.0,
        queue_ahead_estimate=1.2,
        inferred_cancel_events=0,
        inferred_cancel_qty=0.0,
        refill_events=0,
        refill_qty=0.0,
    )
    deep = {
        "valid": 1,
        "best_bid": 99.9,
        "best_bid_qty": 2.0,
        "best_ask": 100.1,
        "best_ask_qty": 1.0,
        "age_ms": 5.0,
        "last_trade_receive_ts_ns": 999_000_000,
    }
    assert runtime.evaluate(
        client_order_id="mm_B_1",
        side="BUY",
        order_price=98.0,
        inventory=0.001,
        path=path_state,
        deep_book=deep,
        now_ns=1_000_000_000,
    ).edge_ms == 0
    assert runtime.evaluate(
        client_order_id="mm_B_1",
        side="BUY",
        order_price=98.0,
        inventory=0.001,
        path=path_state,
        deep_book=deep,
        now_ns=1_250_000_000,
    ).edge_ms == 200
