import platform
from concurrent.futures import ThreadPoolExecutor

import pytest

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")


BUY = narrowgate_cpp.Side.Buy
SELL = narrowgate_cpp.Side.Sell
ARM = narrowgate_cpp.ReplaceContinuationEventKind.Arm
PUBLISH = narrowgate_cpp.ReplaceContinuationEventKind.Publish
DECISION = narrowgate_cpp.ReplaceContinuationEventKind.Decision
DROP = narrowgate_cpp.ReplaceContinuationEventKind.Drop


def _kinds(transition):
    return [event.kind for event in transition.events]


def _assert_conservation(state) -> None:
    telemetry = state.telemetry()
    assert telemetry.arm_count == (
        telemetry.decision_count
        + telemetry.drop_count
        + telemetry.pending_count
        + telemetry.in_flight_count
    )
    assert telemetry.event_sequence == (
        telemetry.arm_count
        + telemetry.publish_count
        + telemetry.decision_count
        + telemetry.drop_count
    )


def test_native_replace_continuation_exact_one_shot_path() -> None:
    state = narrowgate_cpp.NativeReplaceContinuationState()
    armed = state.arm(BUY, "buy-1", 1_000)

    assert armed.accepted
    assert armed.generation == 1
    assert _kinds(armed) == [ARM]
    assert armed.events[0].sequence == 1

    assert not state.publish(BUY, "wrong", 1, 2_000).accepted
    assert not state.publish(BUY, "buy-1", 2, 2_000).accepted
    assert not state.publish(BUY, "buy-1", 1, 999).accepted

    published = state.publish(BUY, "buy-1", 1, 2_000)
    assert published.accepted
    assert _kinds(published) == [PUBLISH]
    assert published.events[0].sequence == 2
    assert not state.publish(BUY, "buy-1", 1, 2_001).accepted

    ready = state.take_ready()
    assert [(item.side, item.client_order_id, item.generation) for item in ready] == [
        (BUY, "buy-1", 1)
    ]
    assert state.take_ready() == []

    decided = state.finalize_decision(BUY, 1, 2_750)
    assert decided.accepted
    assert _kinds(decided) == [DECISION]
    assert decided.events[0].decision_latency_ns == 750
    assert decided.events[0].sequence == 3
    assert not state.finalize_decision(BUY, 1, 3_000).accepted

    telemetry = state.telemetry()
    assert telemetry.arm_count == 1
    assert telemetry.publish_count == 1
    assert telemetry.decision_count == 1
    assert telemetry.drop_count == 0
    assert telemetry.buy_decision_count == 1
    assert telemetry.sell_decision_count == 0
    assert telemetry.decision_latency_sum_ns == 750
    assert telemetry.decision_latency_max_ns == 750
    _assert_conservation(state)


def test_native_replace_continuation_supersede_is_cid_and_generation_bound() -> None:
    state = narrowgate_cpp.NativeReplaceContinuationState()
    first = state.arm(BUY, "stale-buy", 10_000)
    second = state.arm(BUY, "current-buy", 11_000)

    assert second.generation == first.generation + 1
    assert _kinds(second) == [DROP, ARM]
    assert [event.sequence for event in second.events] == [2, 3]
    assert second.events[0].client_order_id == "stale-buy"
    assert second.events[0].reason == "superseded_by_new_arm"
    assert not state.publish(BUY, "stale-buy", first.generation, 12_000).accepted
    assert not state.publish(BUY, "current-buy", first.generation, 12_000).accepted
    assert state.publish(BUY, "current-buy", second.generation, 12_000).accepted

    snapshot = state.side_snapshot(BUY)
    assert snapshot.pending_phase == narrowgate_cpp.ReplaceContinuationPhase.Ready
    assert snapshot.pending.client_order_id == "current-buy"
    assert snapshot.pending.generation == second.generation
    _assert_conservation(state)


def test_native_replace_continuation_consumes_two_sides_as_one_batch() -> None:
    state = narrowgate_cpp.NativeReplaceContinuationState()
    buy = state.arm(BUY, "buy-1", 1_000)
    sell = state.arm(SELL, "sell-1", 1_100)
    assert state.publish(BUY, "buy-1", buy.generation, 2_000).accepted
    assert state.publish(SELL, "sell-1", sell.generation, 2_200).accepted

    ready = state.take_ready()
    assert [item.side for item in ready] == [BUY, SELL]
    assert state.telemetry().pending_count == 0
    assert state.telemetry().in_flight_count == 2

    buy_decision = state.finalize_decision(BUY, buy.generation, 3_000)
    sell_decision = state.finalize_decision(SELL, sell.generation, 3_000)
    assert buy_decision.events[0].decision_latency_ns == 1_000
    assert sell_decision.events[0].decision_latency_ns == 800

    telemetry = state.telemetry()
    assert telemetry.decision_count == 2
    assert telemetry.buy_decision_count == 1
    assert telemetry.sell_decision_count == 1
    assert telemetry.decision_latency_sum_ns == 1_800
    assert telemetry.decision_latency_max_ns == 1_000
    _assert_conservation(state)


def test_native_replace_continuation_clear_semantics_match_live_contract() -> None:
    state = narrowgate_cpp.NativeReplaceContinuationState()
    armed = state.arm(SELL, "sell-1", 10_000)

    assert not state.clear_exact(SELL, "sell-1", armed.generation, 9_999).accepted
    assert not state.clear_unready(
        SELL,
        "sell-1",
        0,
        "zero_is_not_a_generation_wildcard",
    ).accepted
    assert not state.clear_unready(
        SELL,
        "sell-1",
        armed.generation + 1,
        "stale_generation",
    ).accepted
    assert state.side_snapshot(SELL).pending_phase == (
        narrowgate_cpp.ReplaceContinuationPhase.Armed
    )
    cleared = state.clear_unready(
        SELL,
        "sell-1",
        armed.generation,
        "terminal_before_callback",
    )
    assert cleared.accepted
    assert _kinds(cleared) == [DROP]
    assert cleared.events[0].reason == "terminal_before_callback"

    armed = state.arm(SELL, "sell-2", 20_000)
    assert state.publish(SELL, "sell-2", armed.generation, 21_000).accepted
    assert not state.clear_unready(
        SELL,
        "sell-2",
        armed.generation,
        "must_not_drop_ready",
    ).accepted
    cleared = state.clear_side(SELL, "risk_cancel")
    assert cleared.accepted
    assert cleared.events[0].reason == "risk_cancel"
    assert state.take_ready() == []
    _assert_conservation(state)


def test_native_replace_continuation_clear_all_leaves_in_flight_for_finalization() -> None:
    state = narrowgate_cpp.NativeReplaceContinuationState()
    buy = state.arm(BUY, "buy-ready", 1_000)
    assert state.publish(BUY, "buy-ready", buy.generation, 2_000).accepted
    assert state.take_ready()[0].generation == buy.generation
    state.arm(BUY, "buy-pending", 3_000)
    state.arm(SELL, "sell-pending", 3_100)

    events = state.clear_all("shutdown")
    assert [event.side for event in events] == [BUY, SELL]
    assert all(event.kind == DROP and event.reason == "shutdown" for event in events)
    assert state.telemetry().pending_count == 0
    assert state.telemetry().in_flight_count == 1

    dropped = state.drop_in_flight(BUY, buy.generation, "tick_exception")
    assert dropped.accepted
    assert dropped.events[0].reason == "tick_exception"
    _assert_conservation(state)


def test_native_replace_continuation_disabled_and_fixed_cid_contract() -> None:
    disabled = narrowgate_cpp.NativeReplaceContinuationState(False)
    assert not disabled.arm(BUY, "disabled", 1_000).accepted
    assert not disabled.arm(BUY, "disabled", 1_000, False).accepted
    assert disabled.telemetry().event_sequence == 0

    state = narrowgate_cpp.NativeReplaceContinuationState()
    expected_isolation = (
        128
        if platform.system() == "Darwin" and platform.machine() == "arm64"
        else 64
    )
    assert state.cache_line_bytes == expected_isolation
    assert state.max_client_order_id_bytes == 64
    with pytest.raises(ValueError, match="cannot be empty"):
        state.arm(BUY, "", 1_000)
    with pytest.raises(ValueError, match="fixed native capacity"):
        state.arm(BUY, "x" * 65, 1_000)


def test_native_replace_continuation_side_cells_remain_independent_under_threads() -> None:
    state = narrowgate_cpp.NativeReplaceContinuationState()

    def arm_side(side, prefix):
        return [
            state.arm(side, f"{prefix}-{index}", 10_000 + index)
            for index in range(200)
        ]

    with ThreadPoolExecutor(max_workers=2) as pool:
        buy_future = pool.submit(arm_side, BUY, "buy")
        sell_future = pool.submit(arm_side, SELL, "sell")
        buy_results = buy_future.result()
        sell_results = sell_future.result()

    assert all(result.accepted for result in buy_results + sell_results)
    assert state.side_snapshot(BUY).generation_counter == 200
    assert state.side_snapshot(SELL).generation_counter == 200
    telemetry = state.telemetry()
    assert telemetry.arm_count == 400
    assert telemetry.drop_count == 398
    assert telemetry.pending_count == 2
    _assert_conservation(state)
