import pytest

from models.replay.replay_state_checkpoint import ContinuousReplayState
from models.replay.restart_boundary import (
    OrderPhase,
    PlannedRestartInterval,
    RestartBoundaryMachine,
    RestartPhase,
)


def _state() -> ContinuousReplayState:
    return ContinuousReplayState(
        arm_id="control",
        checkpoint_ts_ms=0,
        cash_usdc=0.0,
        position_btc=0.0,
        average_entry_price=0.0,
        cumulative_realized_pnl_usdc=0.0,
        cumulative_fees_usdc=0.0,
        equity_anchor_usdc=0.0,
        last_mark_price=100.0,
        cumulative_pnl_usdc=0.0,
    )


def _interval() -> PlannedRestartInterval:
    return PlannedRestartInterval(
        gap_id="G1",
        quote_stop_ts_ms=100,
        cancel_deadline_ts_ms=150,
        offline_start_ts_ms=151,
        resume_snapshot_ts_ms=1_000,
    )


def test_cancel_ack_restart_and_causal_warmup() -> None:
    machine = RestartBoundaryMachine()
    machine.register_active_order(client_order_id="O1", remaining_quantity_btc=0.001)
    machine.begin_maintenance(_interval(), now_ts_ms=100)
    machine.request_cancel("O1", ts_ms=110)
    machine.terminal("O1", ts_ms=120, reason="CANCEL_ACK")
    state = machine.enter_offline(ts_ms=151, state=_state())

    assert state.restart_safe
    assert machine.phase == RestartPhase.OFFLINE

    state = machine.begin_restart(
        ts_ms=1_000,
        snapshot_identity="snapshot-sha256",
        state=state,
    )
    with pytest.raises(RuntimeError, match="later than"):
        machine.complete_warmup(
            feature_ready_ts_ms=1_101,
            decision_ts_ms=1_100,
            state=state,
        )
    state = machine.complete_warmup(
        feature_ready_ts_ms=1_100,
        decision_ts_ms=1_100,
        state=state,
    )
    assert state.quoting_enabled
    assert machine.phase == RestartPhase.READY


def test_cancel_reject_and_partial_fill_do_not_end_risk_set() -> None:
    machine = RestartBoundaryMachine()
    machine.register_active_order(client_order_id="O1", remaining_quantity_btc=0.001)
    machine.begin_maintenance(_interval(), now_ts_ms=100)
    machine.request_cancel("O1", ts_ms=110)
    machine.partial_fill("O1", ts_ms=115, filled_quantity_btc=0.0004)
    machine.cancel_reject("O1", ts_ms=120)

    assert machine.orders["O1"].phase == OrderPhase.PARTIALLY_FILLED
    with pytest.raises(RuntimeError, match="failed to terminate"):
        machine.enter_offline(ts_ms=151, state=_state())

    machine.request_cancel("O1", ts_ms=130)
    machine.terminal("O1", ts_ms=140, reason="CANCEL_ACK")
    assert machine.enter_offline(ts_ms=151, state=_state()).restart_safe


def test_full_fill_before_cancel_ack_is_terminal() -> None:
    machine = RestartBoundaryMachine()
    machine.register_active_order(client_order_id="O1", remaining_quantity_btc=0.001)
    machine.begin_maintenance(_interval(), now_ts_ms=100)
    machine.request_cancel("O1", ts_ms=110)
    machine.terminal("O1", ts_ms=120, reason="FULL_FILL")
    assert machine.enter_offline(ts_ms=151, state=_state()).restart_safe
