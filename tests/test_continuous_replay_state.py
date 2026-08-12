from dataclasses import replace

import pytest

from models.replay.replay_state_checkpoint import (
    RESTART_RESET_FIELDS,
    ContinuousReplayState,
    EconomicCampaignState,
    read_checkpoint,
    write_checkpoint,
)


def _flat_state() -> ContinuousReplayState:
    return ContinuousReplayState(
        arm_id="control",
        checkpoint_ts_ms=1_000,
        cash_usdc=0.0,
        position_btc=0.0,
        average_entry_price=0.0,
        cumulative_realized_pnl_usdc=0.0,
        cumulative_fees_usdc=0.0,
        equity_anchor_usdc=0.0,
        last_mark_price=100.0,
        cumulative_pnl_usdc=0.0,
    )


def test_checkpoint_roundtrip_and_tamper_detection(tmp_path) -> None:
    path = tmp_path / "state.json"
    state = _flat_state().for_planned_restart(2_000)
    identity = write_checkpoint(path, state)

    loaded = read_checkpoint(path)
    assert loaded == state
    assert len(identity) == 64

    path.write_text(path.read_text().replace('"cash_usdc": 0.0', '"cash_usdc": 1.0'))
    with pytest.raises(ValueError, match="hash mismatch"):
        read_checkpoint(path)


def test_restart_preserves_economics_and_clears_transient_state() -> None:
    campaign = EconomicCampaignState(
        campaign_id="SHORT-1",
        side="SHORT",
        start_ts_ms=1_000,
        start_equity_usdc=0.0,
        peak_abs_inventory_btc=0.002,
    )
    state = ContinuousReplayState(
        arm_id="candidate",
        checkpoint_ts_ms=2_000,
        cash_usdc=200.0,
        position_btc=-0.002,
        average_entry_price=100_000.0,
        cumulative_realized_pnl_usdc=0.0,
        cumulative_fees_usdc=0.0,
        equity_anchor_usdc=0.0,
        last_mark_price=99_000.0,
        cumulative_pnl_usdc=2.0,
        economic_campaign=campaign,
        orders_terminal=False,
        active_order_count=1,
        pending_cancel_count=1,
        queue_cursor_count=1,
        q90_cursor_count=1,
    )

    restarted = state.for_planned_restart(3_000)

    assert restarted.cash_usdc == state.cash_usdc
    assert restarted.position_btc == state.position_btc
    assert restarted.average_entry_price == state.average_entry_price
    assert restarted.economic_campaign == campaign
    assert restarted.restart_generation == 1
    assert restarted.restart_safe
    assert restarted.runtime_reset_fields == RESTART_RESET_FIELDS
    assert not restarted.feature_warmup_ready
    assert not restarted.quoting_enabled


def test_nonflat_state_requires_economic_campaign() -> None:
    state = replace(
        _flat_state(),
        cash_usdc=-100.0,
        position_btc=1.0,
        average_entry_price=100.0,
    )
    with pytest.raises(ValueError, match="economic campaign"):
        state.validate()
