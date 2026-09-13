from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pytest

from models.exchange_book_replay import HistoricalExchangeBookScheduler
from models.tick_data_types import HistoricalExchangeBookEvent
from strategy.dynamic_fill_hazard_model import (
    CPP_DYNAMIC_FILL_HAZARD_ABI_VERSION,
    MODEL_FEATURES,
    DynamicFillHazardBundle,
    DynamicFillHazardShadowRuntime,
    build_dynamic_fill_hazard_features,
    load_cpp_dynamic_fill_hazard_runtime,
)
from strategy.replay_controls import (
    ReplayOrderDepthPath,
    synchronize_visibility_batch_ambiguity_to_cpp,
)

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")
if (
    getattr(narrowgate_cpp, "DYNAMIC_FILL_HAZARD_ABI_VERSION", "")
    != CPP_DYNAMIC_FILL_HAZARD_ABI_VERSION
):
    pytest.skip("native dynamic fill-hazard ABI is not built", allow_module_level=True)


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_ROOT = ROOT / "models" / "saved_btcusdc_dynamic_fill_hazard_20260724"
MODEL_PATH = ARTIFACT_ROOT / "fill_hazard_bundle.json"
POLICY_PATH = ARTIFACT_ROOT / "buy_exposure_adverse_q90_policy.json"
MODEL_SHA256 = hashlib.sha256(MODEL_PATH.read_bytes()).hexdigest()
POLICY_SHA256 = hashlib.sha256(POLICY_PATH.read_bytes()).hexdigest()
BASE_MS = 1_700_000_000_000


def _event(
    offset_ms: int,
    *,
    event_type: str,
    levels: tuple[tuple[str, int, float], ...],
    first_update_id: int | None = None,
    final_update_id: int | None = None,
    previous_final_update_id: int | None = None,
    last_update_id: int | None = None,
) -> HistoricalExchangeBookEvent:
    timestamp_ms = BASE_MS + int(offset_ms)
    return HistoricalExchangeBookEvent(
        market_id="binance_futures:perpetual:BTCUSDC",
        event_type=event_type,
        exchange_ts_ns=timestamp_ms * 1_000_000,
        exchange_ts_source="transaction",
        local_receive_ts_ns=(timestamp_ms + 1) * 1_000_000,
        event_time_ns=timestamp_ms * 1_000_000,
        transaction_time_ns=timestamp_ms * 1_000_000,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        previous_final_update_id=previous_final_update_id,
        last_update_id=last_update_id,
        levels=levels,
    )


def _apply_cpp_event(target, event: HistoricalExchangeBookEvent):
    return target.apply_message(
        event_type=event.event_type,
        exchange_ts_ns=event.exchange_ts_ns,
        receive_ts_ns=event.local_receive_ts_ns,
        event_time_ms=event.event_time_ns // 1_000_000,
        transaction_time_ms=event.transaction_time_ns // 1_000_000,
        first_update_id=event.first_update_id,
        final_update_id=event.final_update_id,
        previous_final_update_id=event.previous_final_update_id,
        last_update_id=event.last_update_id,
        levels=event.levels,
    )


def _head(*, intercept: float, price_adverse_coefficient: float = 0.0):
    coefficients = [0.0] * len(MODEL_FEATURES)
    coefficients[MODEL_FEATURES.index("price_adverse_ticks")] = (
        price_adverse_coefficient
    )
    return {
        "feature_names": list(MODEL_FEATURES),
        "feature_mean": [0.0] * len(MODEL_FEATURES),
        "feature_scale": [1.0] * len(MODEL_FEATURES),
        "coefficients": coefficients,
        "intercept": intercept,
    }


def _visible_age_model():
    favorable = _head(intercept=math.log(0.001))
    favorable["coefficients"][MODEL_FEATURES.index("visible_state_age_log1p")] = 1.0
    return {
        "favorable_fill": favorable,
        "adverse_fill": _head(intercept=math.log(0.001)),
    }


def _runtime_model():
    return {
        "favorable_fill": _head(intercept=math.log(0.01)),
        "adverse_fill": _head(
            intercept=math.log(0.001),
            price_adverse_coefficient=4.0,
        ),
    }


def _runtime():
    return narrowgate_cpp.DynamicFillHazardRuntime(
        _runtime_model(),
        {
            "tick_size": 0.1,
            "lot_size": 0.001,
            "exposure_ms": 100.0,
            "price_jump_ticks": 1.0,
            "evaluation_interval_ms": 100.0,
            "entry_threshold": 0.002,
            "strict_sequence": True,
            "strict_after_ns": 0,
        },
    )


def _runtime_apply(target, event: HistoricalExchangeBookEvent):
    return target.apply_book_message(
        event_type=event.event_type,
        exchange_ts_ns=event.exchange_ts_ns,
        receive_ts_ns=event.local_receive_ts_ns,
        event_time_ms=event.event_time_ns // 1_000_000,
        transaction_time_ms=event.transaction_time_ns // 1_000_000,
        first_update_id=event.first_update_id,
        final_update_id=event.final_update_id,
        previous_final_update_id=event.previous_final_update_id,
        last_update_id=event.last_update_id,
        levels=event.levels,
    )


def _runtime_with_cancel_hold():
    runtime = _runtime()
    snapshot = _event(
        100,
        event_type="snapshot",
        levels=(("bid", 995, 3.0), ("bid", 1000, 2.0), ("ask", 1002, 2.0)),
        last_update_id=100,
    )
    _runtime_apply(runtime, snapshot)
    runtime.activate_order("buy-1", 99.5, snapshot.local_receive_ts_ns)
    runtime.evaluate("buy-1", 0.0, snapshot.local_receive_ts_ns)
    adverse = _event(
        300,
        event_type="delta",
        levels=(
            ("bid", 1000, 0.0),
            ("bid", 999, 2.0),
            ("ask", 1002, 0.0),
            ("ask", 1001, 2.0),
        ),
        first_update_id=101,
        final_update_id=101,
        previous_final_update_id=100,
    )
    _runtime_apply(runtime, adverse)
    assert runtime.evaluate(
        "buy-1", 0.0, adverse.local_receive_ts_ns
    )["action"] == "cancel"
    return runtime, adverse.local_receive_ts_ns


def test_exact_ready_batch_ambiguity_is_persistent_in_python_and_cpp() -> None:
    runtime = _runtime()
    snapshot = _event(
        100,
        event_type="snapshot",
        levels=(("bid", 995, 3.0), ("bid", 1000, 2.0), ("ask", 1002, 2.0)),
        last_update_id=100,
    )
    _runtime_apply(runtime, snapshot)
    activation_ns = snapshot.local_receive_ts_ns
    runtime.activate_order("513", 99.5, activation_ns)
    path = ReplayOrderDepthPath(
        client_order_id="513",
        side="BUY",
        price=99.5,
        generation=1,
        initial_visible_qty=3.0,
        current_visible_qty=3.0,
        receive_ts_ns=snapshot.exchange_ts_ns,
        activation_ts_ns=activation_ns,
        feature_ready_ts_ns=activation_ns,
    )

    tied_delta = _event(
        200,
        event_type="delta",
        levels=(("bid", 995, 2.5),),
        first_update_id=101,
        final_update_id=101,
        previous_final_update_id=100,
    )
    # The native message alone cannot know that an execution trade shares its
    # exact feature-ready boundary.
    _runtime_apply(runtime, tied_delta)
    path.observe_level_change(
        quantity_before=3.0,
        quantity_after=2.5,
        generation=1,
        receive_ts_ns=tied_delta.exchange_ts_ns,
        feature_ready_ts_ns=tied_delta.local_receive_ts_ns,
        ambiguous=True,
    )

    synchronized_ids: set[str] = set()
    assert synchronize_visibility_batch_ambiguity_to_cpp(
        {"513": path}, runtime, synchronized_ids
    ) == 1
    assert synchronized_ids == {"513"}
    assert synchronize_visibility_batch_ambiguity_to_cpp(
        {"513": path}, runtime, synchronized_ids
    ) == 0

    later_delta = _event(
        300,
        event_type="delta",
        levels=(("bid", 995, 2.75),),
        first_update_id=102,
        final_update_id=102,
        previous_final_update_id=101,
    )
    _runtime_apply(runtime, later_delta)
    cpp = runtime.evaluate("513", 0.0, (BASE_MS + 400) * 1_000_000)
    assert path.valid is False
    assert path.invalid_reason == "same_ms_exchange_book_ambiguity"
    assert cpp["valid"] is False
    assert cpp["reason"] == "same_ms_exchange_book_ambiguity"


def test_distinct_ready_boundaries_do_not_create_visibility_ambiguity() -> None:
    class FakeRuntime:
        def has_tracked_path(self, client_order_id: str) -> bool:
            return True

        def invalidate_order(self, client_order_id: str, reason: str) -> None:
            raise AssertionError("distinct ready boundaries must not invalidate")

    path = ReplayOrderDepthPath(
        client_order_id="control",
        side="BUY",
        price=99.5,
        generation=1,
        initial_visible_qty=1.0,
        current_visible_qty=1.0,
        receive_ts_ns=1,
        activation_ts_ns=1,
        feature_ready_ts_ns=1,
    )
    path.observe_level_change(
        quantity_before=1.0,
        quantity_after=0.5,
        generation=1,
        receive_ts_ns=2,
        feature_ready_ts_ns=2,
        ambiguous=False,
    )
    assert synchronize_visibility_batch_ambiguity_to_cpp(
        {"control": path}, FakeRuntime(), set()
    ) == 0


def test_real_artifact_python_cpp_probabilities_match() -> None:
    bundle = DynamicFillHazardBundle.load(
        MODEL_PATH,
        expected_file_sha256=MODEL_SHA256,
        shadow_sides=("BUY",),
    )
    rng = np.random.default_rng(20260729)
    for _ in range(100):
        raw = {
            "risk_snapshot_elapsed_ms": rng.uniform(0.0, 85_000.0),
            "visible_state_age_ms": rng.uniform(0.0, 500.0),
            "spread_ticks": rng.uniform(1.0, 8.0),
            "quote_distance_ticks": rng.uniform(0.0, 250.0),
            "top_bid_size": rng.uniform(0.0, 20.0),
            "top_ask_size": rng.uniform(0.0, 20.0),
            "book_imbalance": rng.uniform(-1.0, 1.0),
            "side_microprice_adverse_ticks": rng.uniform(-1.0, 1.0),
            "policy_queue_initial": rng.uniform(0.0, 10.0),
            "policy_queue_remaining": rng.uniform(0.0, 10.0),
            "policy_queue_fraction_left": rng.uniform(0.0, 1.0),
            "policy_queue_progress": rng.uniform(0.0, 1.0),
            "visible_cancel_events": rng.integers(0, 20),
            "visible_cancel_size": rng.uniform(0.0, 10.0),
            "visible_refill_events": rng.integers(0, 20),
            "visible_refill_size": rng.uniform(0.0, 10.0),
            "visible_refill_event_share": rng.uniform(0.0, 1.0),
            "price_adverse_ticks": rng.uniform(0.0, 20.0),
            "price_worst_adverse_ticks": rng.uniform(0.0, 30.0),
            "price_recovery_ratio": rng.uniform(0.0, 1.0),
            "microprice_adverse_ticks": rng.uniform(0.0, 20.0),
            "microprice_worst_adverse_ticks": rng.uniform(0.0, 30.0),
            "microprice_recovery_ratio": rng.uniform(0.0, 1.0),
            "visible_depth_recovery_ratio": rng.uniform(0.0, 2.0),
            "native_adverse_jump_seen": rng.integers(0, 2),
            "time_since_native_adverse_jump_ms": rng.uniform(-1.0, 20_000.0),
            "clock_hour_sin": rng.uniform(-1.0, 1.0),
            "clock_hour_cos": rng.uniform(-1.0, 1.0),
            "current_inventory_role": rng.choice(
                ["opener", "add", "reducing"]
            ),
        }
        exposure_ms = float(rng.choice([25.0, 100.0, 500.0]))
        python = bundle.predict(
            side="BUY",
            raw_features=raw,
            exposure_ms=exposure_ms,
        )
        features = build_dynamic_fill_hazard_features(raw)
        cpp = narrowgate_cpp.dynamic_fill_hazard_predict(
            bundle.native_model_payload("BUY"),
            [features[name] for name in MODEL_FEATURES],
            exposure_ms,
        )
        assert cpp["favorable_raw_probability"] == pytest.approx(
            python.favorable_raw_probability, rel=2e-14, abs=1e-15
        )
        assert cpp["favorable_probability"] == pytest.approx(
            python.favorable_probability, rel=2e-14, abs=1e-15
        )
        assert cpp["adverse_raw_probability"] == pytest.approx(
            python.adverse_raw_probability, rel=2e-14, abs=1e-15
        )
        assert cpp["adverse_probability"] == pytest.approx(
            python.adverse_probability, rel=2e-14, abs=1e-15
        )


def test_native_runtime_visible_age_uses_feature_ready_not_source_time() -> None:
    runtime = narrowgate_cpp.DynamicFillHazardRuntime(
        _visible_age_model(),
        {
            "tick_size": 0.1,
            "lot_size": 0.001,
            "exposure_ms": 100.0,
            "price_jump_ticks": 1.0,
            "evaluation_interval_ms": 100.0,
            "entry_threshold": 0.002,
            "strict_sequence": True,
            "strict_after_ns": 0,
        },
    )
    snapshot = _event(
        100,
        event_type="snapshot",
        levels=(("bid", 995, 3.0), ("ask", 1002, 2.0)),
        last_update_id=100,
    )
    feature_ready_ns = snapshot.local_receive_ts_ns + 2_000_000_000
    runtime.apply_book_message(
        event_type=snapshot.event_type,
        exchange_ts_ns=snapshot.exchange_ts_ns,
        receive_ts_ns=snapshot.local_receive_ts_ns,
        feature_ready_ts_ns=feature_ready_ns,
        event_time_ms=snapshot.event_time_ns // 1_000_000,
        transaction_time_ms=snapshot.transaction_time_ns // 1_000_000,
        first_update_id=snapshot.first_update_id,
        final_update_id=snapshot.final_update_id,
        previous_final_update_id=snapshot.previous_final_update_id,
        last_update_id=snapshot.last_update_id,
        levels=snapshot.levels,
        execution_trade_same_ms=False,
    )
    activation_ns = feature_ready_ns
    runtime.activate_order("buy-ready-age", 99.5, activation_ns)
    evaluation = runtime.evaluate(
        "buy-ready-age",
        0.0,
        activation_ns + 100_000_000,
    )

    expected_eta = math.log(0.001) + math.log1p(100.0)
    expected_raw = -math.expm1(-math.exp(expected_eta) * 0.1)
    assert evaluation["deep_age_ms"] == pytest.approx(100.0)
    assert evaluation["prediction"]["favorable_raw_probability"] == pytest.approx(
        expected_raw,
        rel=2e-14,
        abs=1e-15,
    )


def test_native_snapshot_delta_sequence_and_exact_lookup_match_python() -> None:
    events = [
        _event(
            100,
            event_type="snapshot",
            levels=(
                ("bid", 995, 0.0),
                ("bid", 999, 3.0),
                ("bid", 1000, 2.0),
                ("ask", 1002, 2.0),
                ("ask", 1005, 4.0),
            ),
            last_update_id=100,
        ),
        _event(
            200,
            event_type="delta",
            levels=(("bid", 999, 2.25), ("ask", 1005, 0.0)),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
        ),
        _event(
            300,
            event_type="delta",
            levels=(("bid", 998, 1.5),),
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
        ),
    ]
    python = HistoricalExchangeBookScheduler(events)
    cpp = narrowgate_cpp.NativeExchangeBookScheduler(True, 0, False)
    for event in events:
        py_advance = python.advance_to(event.exchange_ts_ns, inclusive=True)
        cpp_advance = _apply_cpp_event(cpp, event)
        assert cpp_advance["accepted"]
        assert cpp_advance["snapshot_reset"] == py_advance.snapshot_reset
        for side, tick in (
            ("BUY", 995),
            ("BUY", 998),
            ("BUY", 999),
            ("BUY", 1000),
            ("SELL", 1002),
            ("SELL", 1005),
        ):
            py_lookup = python.lookup(side, tick)
            cpp_lookup = cpp.lookup(side, tick)
            assert cpp_lookup["status"] == py_lookup.status
            assert cpp_lookup["reason"] == py_lookup.reason
            assert cpp_lookup["quantity"] == pytest.approx(py_lookup.quantity)
            assert cpp_lookup["segment_id"] == py_lookup.segment_id
        bids, asks = python.top_levels(1)
        top = cpp.top()
        assert top["best_bid_tick"] == bids[0][0]
        assert top["best_ask_tick"] == asks[0][0]
        assert top["best_bid_qty"] == pytest.approx(bids[0][1])
        assert top["best_ask_qty"] == pytest.approx(asks[0][1])

    py_stats = python.stats()
    cpp_stats = cpp.stats()
    assert cpp_stats["snapshot_messages"] == py_stats.snapshot_events
    assert cpp_stats["update_messages"] == py_stats.delta_events
    assert cpp_stats["accepted_updates"] == 2
    assert cpp_stats["sequence_gaps"] == py_stats.sequence_gaps


def test_q90_cancel_pending_fill_recovery_ack_and_queue_reset() -> None:
    runtime = _runtime()
    snapshot = _event(
        100,
        event_type="snapshot",
        levels=(
            ("bid", 995, 3.0),
            ("bid", 1000, 2.0),
            ("ask", 1002, 2.0),
            ("ask", 1006, 3.0),
        ),
        last_update_id=100,
    )
    _runtime_apply(runtime, snapshot)
    activation_ns = snapshot.local_receive_ts_ns
    seed = runtime.activate_order("buy-1", 99.5, activation_ns)
    assert seed["status"] == "exact"
    assert seed["quantity"] == pytest.approx(3.0)

    first = runtime.evaluate("buy-1", 0.0, activation_ns)
    assert first["action"] == "keep"

    adverse = _event(
        300,
        event_type="delta",
        levels=(
            ("bid", 1000, 0.0),
            ("bid", 999, 2.0),
            ("ask", 1002, 0.0),
            ("ask", 1001, 2.0),
            ("bid", 995, 2.5),
        ),
        first_update_id=101,
        final_update_id=101,
        previous_final_update_id=100,
    )
    _runtime_apply(runtime, adverse)
    runtime.observe_trade(True, 99.5, 0.25, adverse.local_receive_ts_ns)
    cancel = runtime.evaluate("buy-1", 0.0, adverse.local_receive_ts_ns)
    assert cancel["action"] == "cancel"
    assert runtime.hold_active
    assert runtime.on_fill("buy-1", 0.001, adverse.local_receive_ts_ns) == "none"

    recovery = _event(
        500,
        event_type="delta",
        levels=(
            ("bid", 999, 0.0),
            ("bid", 1000, 2.0),
            ("ask", 1001, 0.0),
            ("ask", 1002, 2.0),
            ("bid", 995, 4.0),
        ),
        first_update_id=102,
        final_update_id=102,
        previous_final_update_id=101,
    )
    _runtime_apply(runtime, recovery)
    recovered = runtime.evaluate("buy-1", 0.0, recovery.local_receive_ts_ns)
    assert recovered["action"] == "recover_wait_ack"
    assert runtime.on_cancel_ack(
        "buy-1", recovery.local_receive_ts_ns + 1, 0.001
    ) == (
        "post_cancel_recovery"
    )
    assert runtime.hold_active
    assert runtime.hold_phase == "POST_CANCEL_RECOVERY"
    assert runtime.tracked_path_count == 0
    assert runtime.evaluation_state_count == 0
    assert not runtime.has_tracked_path("buy-1")
    post_ack = runtime.evaluate_prospective_cancel_reentry(
        99.5,
        0.0,
        recovery.local_receive_ts_ns + 2,
    )
    assert post_ack["action"] == "reenter"
    assert post_ack["elapsed_ms"] == 0.0
    assert post_ack["queue_initial"] == pytest.approx(4.0)
    assert post_ack["queue_remaining"] == pytest.approx(4.0)
    assert not runtime.hold_active

    reset = runtime.activate_order(
        "buy-2", 99.5, recovery.local_receive_ts_ns + 4
    )
    assert reset["quantity"] == pytest.approx(4.0)
    new_path = runtime.evaluate(
        "buy-2", 0.0, recovery.local_receive_ts_ns + 4
    )
    assert new_path["queue_initial"] == pytest.approx(4.0)
    assert new_path["queue_remaining"] == pytest.approx(4.0)

    counters = runtime.counters()
    assert counters["cancel_request_count"] == 1
    assert counters["pre_ack_fill_count"] == 1
    assert counters["recovery_count"] == 1
    assert counters["cancel_ack_count"] == 1
    assert counters["reentry_count"] == 1


def test_ack_before_recovery_ends_old_path_and_requires_new_placement_state() -> None:
    runtime = _runtime()
    snapshot = _event(
        100,
        event_type="snapshot",
        levels=(
            ("bid", 995, 3.0),
            ("bid", 1000, 2.0),
            ("ask", 1002, 2.0),
        ),
        last_update_id=100,
    )
    _runtime_apply(runtime, snapshot)
    runtime.activate_order("buy-1", 99.5, snapshot.local_receive_ts_ns)
    runtime.evaluate("buy-1", 0.0, snapshot.local_receive_ts_ns)
    adverse = _event(
        300,
        event_type="delta",
        levels=(
            ("bid", 1000, 0.0),
            ("bid", 999, 2.0),
            ("ask", 1002, 0.0),
            ("ask", 1001, 2.0),
        ),
        first_update_id=101,
        final_update_id=101,
        previous_final_update_id=100,
    )
    _runtime_apply(runtime, adverse)
    assert runtime.evaluate(
        "buy-1", 0.0, adverse.local_receive_ts_ns
    )["action"] == "cancel"
    assert runtime.on_cancel_ack(
        "buy-1", adverse.local_receive_ts_ns + 1, 0.001
    ) == (
        "post_cancel_recovery"
    )
    assert runtime.hold_active
    assert runtime.hold_phase == "POST_CANCEL_RECOVERY"

    recovery = _event(
        500,
        event_type="delta",
        levels=(
            ("bid", 999, 0.0),
            ("bid", 1000, 2.0),
            ("ask", 1001, 0.0),
            ("ask", 1002, 2.0),
        ),
        first_update_id=102,
        final_update_id=102,
        previous_final_update_id=101,
    )
    _runtime_apply(runtime, recovery)
    assert runtime.tracked_path_count == 0
    assert runtime.evaluation_state_count == 0
    post_ack = runtime.evaluate_prospective_cancel_reentry(
        100.0,
        0.0,
        recovery.local_receive_ts_ns,
    )
    assert post_ack["valid"] is True
    assert post_ack["action"] == "reenter"
    assert post_ack["queue_initial"] == pytest.approx(2.0)
    assert not runtime.hold_active


def test_cpp_terminal_reason_routes_do_not_reenter_after_full_fill() -> None:
    runtime, now_ns = _runtime_with_cancel_hold()
    assert runtime.on_fill("buy-1", 0.0, now_ns + 1) == (
        "terminal_complete_no_reentry"
    )
    assert not runtime.hold_active


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        ("expired", "baseline_resubmit"),
        ("rejected", "baseline_resubmit"),
        ("local_shutdown_cancel", "shutdown_no_reentry"),
    ],
)
def test_cpp_terminal_reason_routing(reason: str, expected: str) -> None:
    runtime, now_ns = _runtime_with_cancel_hold()
    assert runtime.on_order_terminal(
        "buy-1",
        now_ns + 1,
        reason,
        0.001,
    ) == expected
    assert not runtime.hold_active


def test_cpp_unknown_terminal_reason_fails_fast_without_path_reuse() -> None:
    runtime, now_ns = _runtime_with_cancel_hold()
    with pytest.raises(ValueError, match="unsupported q90 terminal reason"):
        runtime.on_order_terminal("buy-1", now_ns + 1, "unknown", 0.001)
    assert runtime.tracked_path_count == 1


def test_python_cpp_fresh_prospective_prediction_parity() -> None:
    payload = {
        "family_id": "test_dynamic_fill_hazard",
        "action_family_allowed": False,
        "feature_names": list(MODEL_FEATURES),
        "models": {"BUY": _runtime_model()},
    }
    bundle = DynamicFillHazardBundle(
        payload,
        path=Path("synthetic_fill_hazard_bundle.json"),
        file_sha256="0" * 64,
        shadow_sides=("BUY",),
    )
    python_runtime = DynamicFillHazardShadowRuntime(
        bundle,
        tick_size=0.1,
        lot_size=0.001,
        exposure_ms=100.0,
        price_jump_ticks=1.0,
        evaluation_interval_ms=100.0,
    )
    cpp_runtime, cancel_ns = _runtime_with_cancel_hold()
    assert cpp_runtime.on_cancel_ack("buy-1", cancel_ns + 1, 0.001) == (
        "post_cancel_recovery"
    )
    now_ns = cancel_ns + 2
    top = dict(cpp_runtime.top())
    candidate_price = top["best_bid_tick"] * 0.1
    level = dict(cpp_runtime.lookup("BUY", top["best_bid_tick"]))
    deep_book = {
        "valid": int(top["valid"]),
        "generation": int(top["segment_id"]),
        "best_bid": candidate_price,
        "best_bid_qty": float(top["best_bid_qty"]),
        "best_ask": top["best_ask_tick"] * 0.1,
        "best_ask_qty": float(top["best_ask_qty"]),
        "last_receive_ts_ns": int(top["last_receive_ts_ns"]),
        "feature_ready_ts_ns": int(top["last_receive_ts_ns"]),
        "age_ms": (now_ns - int(top["last_receive_ts_ns"])) / 1_000_000.0,
    }
    level_state = {
        "valid": bool(level["strict_usable"]),
        "covered": level["snapshot_min_tick"] is not None,
        "price": candidate_price,
        "quantity": float(level["quantity"]),
        "receive_ts_ns": int(top["last_receive_ts_ns"]),
        "feature_ready_ts_ns": int(top["last_receive_ts_ns"]),
        "age_ms": deep_book["age_ms"],
    }
    python_result = python_runtime.evaluate_prospective_cancel_reentry(
        terminal_policy_route="PROSPECTIVE_CANCEL_REENTRY",
        terminal_reason="cancel_ack",
        remaining_quantity=0.001,
        candidate_price=candidate_price,
        inventory=0.0,
        deep_book=deep_book,
        candidate_level=level_state,
        now_ns=now_ns,
    )
    cpp_result = cpp_runtime.evaluate_prospective_cancel_reentry(
        candidate_price,
        0.0,
        now_ns,
    )
    py_observation = python_result.observation
    assert python_result.old_path_reused is False
    assert py_observation.valid is cpp_result["valid"] is True
    assert py_observation.elapsed_ms == cpp_result["elapsed_ms"] == 0.0
    assert py_observation.queue_initial == pytest.approx(
        cpp_result["queue_initial"]
    )
    assert py_observation.favorable_probability == pytest.approx(
        cpp_result["prediction"]["favorable_probability"],
        abs=1e-12,
    )
    assert py_observation.adverse_probability == pytest.approx(
        cpp_result["prediction"]["adverse_probability"],
        abs=1e-12,
    )


def test_hash_bound_adapter_refuses_to_claim_full_cpp_replay_authority() -> None:
    adapter = load_cpp_dynamic_fill_hazard_runtime(
        model_path=MODEL_PATH,
        expected_model_sha256=MODEL_SHA256,
        policy_path=POLICY_PATH,
        expected_policy_sha256=POLICY_SHA256,
        tick_size=0.1,
        lot_size=0.001,
        exposure_ms=100.0,
        price_jump_ticks=1.0,
    )
    identity = adapter.identity()
    assert identity["model_file_sha256"] == MODEL_SHA256
    assert identity["policy_file_sha256"] == POLICY_SHA256
    assert identity["full_cpp_tick_replay_authority"] is False
    assert identity["action_or_live_authorization"] is False
    assert json.loads(json.dumps(identity))["abi_version"] == (
        CPP_DYNAMIC_FILL_HAZARD_ABI_VERSION
    )
