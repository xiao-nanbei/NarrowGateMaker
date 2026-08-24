from __future__ import annotations

import pytest

from execution.order_lifecycle import (
    OrderLifecyclePhase,
    QuantityWeightedOrderLifecycle,
    TerminalPolicyRoute,
    terminal_policy_route,
)
from strategy.order_manager import OrderManager, OrderState, Side


def test_journal_snapshot_detaches_event_history_from_live_lifecycle() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    snapshot = lifecycle.journal_snapshot()

    assert snapshot.latest_event().event == "submit"
    lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)

    assert len(snapshot.events()) == 1
    assert snapshot.phase == OrderLifecyclePhase.SUBMITTED
    assert len(lifecycle.events()) == 2
    assert lifecycle.phase == OrderLifecyclePhase.ACTIVE


def test_preactivation_rejection_records_complete_zero_exchange_exposure() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)

    lifecycle.exchange_terminal(2_000_000_000, reason="rejected")

    terminal_event = lifecycle.events()[-1]
    snapshot = lifecycle.snapshot()
    assert lifecycle.activation_exchange_ts_ns == 0
    assert lifecycle.exchange_exposure_btc_s() == 0.0
    assert terminal_event["quantity_time_exposure_exchange_btc_s"] == 0.0
    assert terminal_event["exchange_exposure_valid"] is True
    assert terminal_event["exchange_exposure_complete"] is True
    assert snapshot["quantity_time_exposure_exchange_btc_s"] == 0.0
    assert snapshot["exchange_exposure_valid"] is True
    assert snapshot["exchange_exposure_complete"] is True
    assert snapshot["terminal_policy_route"] == "BASELINE_RESUBMIT"


def test_quantity_weighted_exposure_tracks_partial_fill_and_terminal() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(
        initial_quantity=0.001,
        submitted_ts_ns=1_000_000_000,
    )
    lifecycle.activate(2_000_000_000)
    lifecycle.observe_fill(
        remaining_after=0.0004,
        visibility_ts_ns=4_000_000_000,
    )
    lifecycle.request_cancel(5_000_000_000)
    lifecycle.exchange_terminal(
        9_000_000_000,
        reason="cancel_ack",
    )

    assert lifecycle.phase == OrderLifecyclePhase.EXCHANGE_TERMINAL
    assert lifecycle.first_fill_latency_s == pytest.approx(2.0)
    assert lifecycle.exposure_btc_s() == pytest.approx(0.004)
    assert lifecycle.exposure_btc_s(now_ns=20_000_000_000) == pytest.approx(
        0.004
    )

    lifecycle.enter_post_cancel_recovery(9_000_000_000)
    assert lifecycle.phase == OrderLifecyclePhase.POST_CANCEL_RECOVERY
    assert not lifecycle.fill_risk_active
    lifecycle.mark_reentry_eligible(10_000_000_000)
    assert lifecycle.phase == OrderLifecyclePhase.REENTRY_ELIGIBLE
    with pytest.raises(ValueError, match="outside the exchange fill-risk set"):
        lifecycle.observe_fill(
            remaining_after=0.0,
            visibility_ts_ns=11_000_000_000,
            full_fill=True,
        )


def test_partial_fill_during_cancel_pending_stays_in_fill_risk_set() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(
        initial_quantity=0.001,
        submitted_ts_ns=1_000_000_000,
    )
    lifecycle.activate(2_000_000_000)
    lifecycle.request_cancel(3_000_000_000)
    lifecycle.observe_fill(
        remaining_after=0.0004,
        visibility_ts_ns=4_000_000_000,
    )

    assert lifecycle.phase == OrderLifecyclePhase.CANCEL_PENDING
    assert lifecycle.fill_risk_active
    assert lifecycle.remaining_quantity == pytest.approx(0.0004)


def test_exchange_and_visibility_exposure_are_distinct_estimands() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(
        initial_quantity=0.001,
        submitted_ts_ns=1_000_000_000,
    )
    lifecycle.activate(
        2_200_000_000,
        exchange_ts_ns=2_000_000_000,
    )
    lifecycle.observe_fill(
        remaining_after=0.0004,
        visibility_ts_ns=4_500_000_000,
        exchange_ts_ns=4_000_000_000,
    )
    lifecycle.request_cancel(5_000_000_000)
    lifecycle.exchange_terminal(
        9_000_000_000,
        reason="cancel_ack",
        exchange_ts_ns=8_000_000_000,
    )

    snapshot = lifecycle.snapshot()
    assert snapshot["quantity_time_exposure_visible_btc_s"] == pytest.approx(
        0.0041
    )
    assert snapshot["quantity_time_exposure_exchange_btc_s"] == pytest.approx(
        0.0036
    )
    assert snapshot[
        "quantity_time_exposure_visibility_minus_exchange_btc_s"
    ] == pytest.approx(0.0005)
    assert snapshot["exchange_exposure_valid"] is True
    assert snapshot["exchange_exposure_complete"] is True
    assert snapshot["first_fill_latency_visible_s"] == pytest.approx(2.3)
    assert snapshot["first_fill_latency_exchange_s"] == pytest.approx(2.0)


def test_missing_exchange_activation_invalidates_only_physical_exposure() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(
        initial_quantity=0.001,
        submitted_ts_ns=1_000_000_000,
    )
    lifecycle.activate(2_000_000_000)

    activation_snapshot = lifecycle.snapshot()
    assert activation_snapshot["exchange_exposure_valid"] is False
    assert activation_snapshot["exchange_exposure_complete"] is False
    assert activation_snapshot["quantity_time_exposure_exchange_btc_s"] is None
    assert (
        activation_snapshot["exchange_exposure_invalid_reason"]
        == "missing_exchange_timestamp:activate"
    )

    lifecycle.observe_fill(
        remaining_after=0.0004,
        visibility_ts_ns=4_000_000_000,
        exchange_ts_ns=3_500_000_000,
    )

    snapshot = lifecycle.snapshot(now_ns=5_000_000_000)
    assert snapshot["quantity_time_exposure_visible_btc_s"] == pytest.approx(
        0.0024
    )
    assert snapshot["quantity_time_exposure_exchange_btc_s"] is None
    assert snapshot["exchange_exposure_valid"] is False
    assert (
        snapshot["exchange_exposure_invalid_reason"]
        == "missing_exchange_timestamp:activate"
    )


@pytest.mark.parametrize(
    ("reason", "remaining", "expected"),
    [
        ("cancel_ack", 0.0004, TerminalPolicyRoute.PROSPECTIVE_CANCEL_REENTRY),
        ("cancel_ack", 0.0, TerminalPolicyRoute.TERMINAL_COMPLETE),
        ("full_fill", 0.0, TerminalPolicyRoute.TERMINAL_COMPLETE),
        ("rejected", 0.001, TerminalPolicyRoute.BASELINE_RESUBMIT),
        ("expired", 0.001, TerminalPolicyRoute.BASELINE_RESUBMIT),
        ("local_shutdown_cancel", 0.001, TerminalPolicyRoute.SHUTDOWN_NO_REENTRY),
        ("unknown", 0.001, TerminalPolicyRoute.UNSUPPORTED),
    ],
)
def test_terminal_policy_route_is_reason_and_quantity_specific(
    reason: str,
    remaining: float,
    expected: TerminalPolicyRoute,
) -> None:
    assert terminal_policy_route(reason, remaining) == expected


def test_full_fill_cannot_enter_post_cancel_recovery() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(
        initial_quantity=0.001,
        submitted_ts_ns=1_000_000_000,
    )
    lifecycle.activate(
        2_000_000_000,
        exchange_ts_ns=1_900_000_000,
    )
    lifecycle.observe_fill(
        remaining_after=0.0,
        visibility_ts_ns=3_000_000_000,
        exchange_ts_ns=2_900_000_000,
        full_fill=True,
    )

    with pytest.raises(ValueError, match="cancel ACK with remaining quantity"):
        lifecycle.enter_post_cancel_recovery(3_000_000_000)


def test_unknown_terminal_reason_fails_before_terminal_transition() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(
        initial_quantity=0.001,
        submitted_ts_ns=1_000_000_000,
    )
    lifecycle.activate(
        2_000_000_000,
        exchange_ts_ns=1_900_000_000,
    )
    with pytest.raises(ValueError, match="unsupported order terminal reason"):
        lifecycle.exchange_terminal(
            3_000_000_000,
            exchange_ts_ns=2_900_000_000,
            reason="unknown",
        )
    assert lifecycle.phase == OrderLifecyclePhase.ACTIVE
    assert lifecycle.fill_risk_active is True


def test_submit_ack_unknown_remains_submitted_until_authoritative_activation() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(
        initial_quantity=0.001,
        submitted_ts_ns=1_000_000_000,
    )
    lifecycle.mark_submit_ack_unknown(
        2_000_000_000,
        reason="transport_timeout",
    )

    assert lifecycle.phase == OrderLifecyclePhase.SUBMITTED
    assert lifecycle.exchange_exposure_btc_s() is None
    assert lifecycle.events()[-1]["event"] == "submit_ack_unknown"

    lifecycle.activate(3_000_000_000, exchange_ts_ns=2_500_000_000)
    assert lifecycle.phase == OrderLifecyclePhase.ACTIVE
    assert lifecycle.visible_exposure_valid is False
    assert lifecycle.exchange_exposure_valid is False
    assert lifecycle.activation_ts_ns == 3_000_000_000


def test_reconciled_unknown_submit_can_be_censored_but_not_zero_encoded() -> None:
    manager = OrderManager()
    cid = manager.create_order("BTCUSDC", Side.SELL, 100.0, 0.001)
    assert manager.mark_submit_ack_unknown(cid, "response_lost") is True
    assert manager.censor_submit_ack_unknown(cid, "terminal_without_private_callback")

    snapshot = manager.lifecycle_snapshot(cid)
    assert snapshot is not None
    assert snapshot["phase"] == OrderLifecyclePhase.SUBMITTED.value
    assert snapshot["locally_censored"] is True
    assert snapshot["terminal_policy_route"] == ""
    assert snapshot["visible_exposure_valid"] is False
    assert snapshot["exchange_exposure_complete"] is False
    assert snapshot["quantity_time_exposure_exchange_btc_s"] is None
    assert manager.lifecycle_events(cid)[-1]["event"] == "submit_ack_unknown_censored"
    order = manager.get_order(cid)
    assert order is not None
    assert order.state == OrderState.PENDING_NEW
    assert manager.get_active_by_side(Side.SELL) == [order]


def test_shutdown_censor_keeps_unknown_submit_in_active_ownership() -> None:
    terminal = []
    manager = OrderManager(on_terminal=lambda order, reason: terminal.append((order, reason)))
    cid = manager.create_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    assert manager.mark_submit_ack_unknown(cid, "response_lost") is True

    manager.cancel_all_local()

    order = manager.get_order(cid)
    assert order is not None
    assert order.state == OrderState.PENDING_NEW
    assert manager.get_active_by_side(Side.BUY) == [order]
    assert terminal == []
    snapshot = manager.lifecycle_snapshot(cid)
    assert snapshot is not None
    assert snapshot["locally_censored"] is True
    assert snapshot["exchange_exposure_complete"] is False
    assert manager.lifecycle_events(cid)[-1]["event"] == "submit_ack_unknown_censored"


def test_orphan_adoption_is_left_truncated_without_fabricated_activation_clock() -> None:
    lifecycle_callbacks = []
    manager = OrderManager(
        on_lifecycle_event=lambda order, event, payload: lifecycle_callbacks.append(
            (order, event, payload)
        )
    )

    manager.on_order_update(
        {
            "s": "BTCUSDC",
            "c": "mm_B_restart_orphan",
            "S": "BUY",
            "X": "NEW",
            "i": 17,
            "p": "99.9",
            "q": "0.001",
            "T": 1_900_000_000_000,
            "_local_receive_ts_ns": 1_900_000_100_000_000_000,
        }
    )

    order = manager.get_order("mm_B_restart_orphan")
    assert order is not None
    assert order.orphan_adoption is True
    assert order.left_truncation_reason == "exchange_callback_without_local_submit"
    snapshot = manager.lifecycle_snapshot(order.client_order_id)
    assert snapshot is not None
    assert snapshot["phase"] == OrderLifecyclePhase.ACTIVE.value
    assert snapshot["activation_ts_ns"] == 0
    assert snapshot["activation_exchange_ts_ns"] == 0
    assert snapshot["visible_exposure_valid"] is False
    assert snapshot["exchange_exposure_valid"] is False
    assert lifecycle_callbacks[0][1] == "activate_unknown_prefix"


@pytest.mark.parametrize(
    ("status", "executed_qty", "expected_state"),
    [
        ("PARTIALLY_FILLED", "0.0004", OrderState.PARTIALLY_FILLED),
        ("FILLED", "0.001", OrderState.FILLED),
        ("EXPIRED", "0.0004", OrderState.EXPIRED),
    ],
)
def test_private_fill_after_unknown_submit_preserves_unknown_activation_prefix(
    status: str,
    executed_qty: str,
    expected_state: OrderState,
) -> None:
    manager = OrderManager()
    cid = manager.create_order("BTCUSDC", Side.BUY, 99.9, 0.001)
    assert manager.mark_submit_ack_unknown(cid, "response_lost")

    manager.on_order_update(
        {
            "s": "BTCUSDC",
            "c": cid,
            "S": "BUY",
            "X": status,
            "i": 77,
            "p": "99.9",
            "q": "0.001",
            "z": executed_qty,
            "L": "99.8",
            "ap": "99.8",
            "T": 1_900_000_000_000,
            "_local_receive_ts_ns": 1_900_000_100_000_000_000,
        }
    )

    order = manager.get_order(cid)
    assert order is not None
    assert order.state == expected_state
    assert order.filled_qty == pytest.approx(float(executed_qty))
    snapshot = manager.lifecycle_snapshot(cid)
    assert snapshot is not None
    assert snapshot["activation_ts_ns"] == 0
    assert snapshot["activation_exchange_ts_ns"] == 0
    assert snapshot["visible_exposure_valid"] is False
    assert snapshot["exchange_exposure_valid"] is False


def test_orphan_expiry_preserves_cumulative_fill_before_terminal() -> None:
    fills = []
    manager = OrderManager(on_fill=lambda order, event: fills.append((order, event)))

    manager.on_order_update(
        {
            "s": "BTCUSDC",
            "c": "mm_S_restart_partial_expiry",
            "S": "SELL",
            "X": "EXPIRED",
            "i": 91,
            "p": "100.1",
            "q": "0.001",
            "z": "0.0004",
            "L": "100.1",
            "ap": "100.1",
            "T": 1_900_000_000_000,
            "_local_receive_ts_ns": 1_900_000_100_000_000_000,
        }
    )

    order = manager.get_order("mm_S_restart_partial_expiry")
    assert order is not None
    assert order.state == OrderState.EXPIRED
    assert order.filled_qty == pytest.approx(0.0004)
    assert order.remaining_qty == pytest.approx(0.0006)
    assert len(fills) == 1
    snapshot = manager.lifecycle_snapshot(order.client_order_id)
    assert snapshot is not None
    assert snapshot["terminal_reason"] == "expired"
    assert snapshot["activation_ts_ns"] == 0
    assert snapshot["visible_exposure_valid"] is False


def test_cancel_reject_restores_partial_fill_phase_and_exposure() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(
        initial_quantity=0.001,
        submitted_ts_ns=1_000_000_000,
    )
    lifecycle.activate(2_000_000_000)
    lifecycle.observe_fill(
        remaining_after=0.0004,
        visibility_ts_ns=3_000_000_000,
    )
    lifecycle.request_cancel(4_000_000_000)
    lifecycle.cancel_rejected(5_000_000_000)

    assert lifecycle.phase == OrderLifecyclePhase.PARTIALLY_FILLED
    assert lifecycle.fill_risk_active
    assert lifecycle.exposure_btc_s(now_ns=6_000_000_000) == pytest.approx(
        0.0022
    )


def test_order_manager_reconcile_preserves_partial_fill_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timestamps = iter(
        [
            1_000_000_000,
            2_000_000_000,
            4_000_000_000,
            6_000_000_000,
        ]
    )
    monkeypatch.setattr(
        "strategy.order_manager.time.time_ns",
        lambda: next(timestamps),
    )
    manager = OrderManager()
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    manager.confirm_new(cid, 42, exchange_ts_ns=2_000_000_000)
    manager.on_order_update(
        {
            "c": cid,
            "X": "PARTIALLY_FILLED",
            "i": 42,
            "z": "0.0006",
            "L": "100.0",
            "ap": "100.0",
            "T": 3_500,
            "_local_receive_ts_ns": 3_000_000_000,
        }
    )
    manager.mark_pending_cancel(cid)

    assert manager.reconcile_pending_cancel(
        cid,
        exchange_open=True,
        exchange_oid=42,
    )
    order = manager.get_order(cid)
    assert order is not None
    assert order.state == OrderState.PARTIALLY_FILLED
    assert order.lifecycle_phase == OrderLifecyclePhase.PARTIALLY_FILLED.value
    assert order.remaining_qty == pytest.approx(0.0004)


def test_order_manager_exposes_quantity_weighted_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timestamps = iter(
        [
            1_000_000_000,
            2_000_000_000,
            3_000_000_000,
        ]
    )
    monkeypatch.setattr(
        "strategy.order_manager.time.time_ns",
        lambda: next(timestamps),
    )
    terminal: list[tuple[str, str]] = []
    lifecycle_callbacks: list[str] = []
    manager = OrderManager(
        on_terminal=lambda order, reason: terminal.append(
            (order.client_order_id, reason)
        ),
        on_lifecycle_event=lambda _order, event_type, _event: lifecycle_callbacks.append(
            event_type
        ),
    )
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    manager.confirm_new(cid, 42, exchange_ts_ns=2_000_000_000)
    manager.on_order_update(
        {
            "c": cid,
            "X": "NEW",
            "i": 42,
            "T": 2_500,
            "_local_receive_ts_ns": 2_600_000_000,
        }
    )
    order_after_new = manager.get_order(cid)
    assert order_after_new is not None
    assert [event["event"] for event in order_after_new.lifecycle.events()] == [
        "submit",
        "activate",
    ]
    assert lifecycle_callbacks == ["rest_ack"]
    manager.mark_pending_cancel(cid)

    manager.on_order_update(
        {
            "c": cid,
            "X": "PARTIALLY_FILLED",
            "i": 42,
            "z": "0.0006",
            "L": "100.0",
            "ap": "100.0",
            "T": 3_500,
            "_local_receive_ts_ns": 4_000_000_000,
        }
    )
    order = manager.get_order(cid)
    assert order is not None
    assert order.state == OrderState.PENDING_CANCEL
    assert order.lifecycle_phase == OrderLifecyclePhase.CANCEL_PENDING.value

    manager.on_order_update(
        {
            "c": cid,
            "X": "CANCELED",
            "i": 42,
            "T": 5_500,
            "_local_receive_ts_ns": 6_000_000_000,
        }
    )
    snapshot = manager.lifecycle_snapshot(cid)
    assert snapshot is not None
    assert snapshot["phase"] == OrderLifecyclePhase.EXCHANGE_TERMINAL.value
    assert snapshot["remaining_quantity"] == pytest.approx(0.0004)
    assert snapshot["first_fill_latency_s"] == pytest.approx(2.0)
    assert snapshot["quantity_time_exposure_btc_s"] == pytest.approx(0.0028)
    assert snapshot["quantity_time_exposure_visible_btc_s"] == pytest.approx(
        0.0028
    )
    assert snapshot["quantity_time_exposure_exchange_btc_s"] == pytest.approx(
        0.0023
    )
    assert snapshot["exchange_exposure_complete"] is True
    assert terminal == [(cid, "cancel_ack")]
