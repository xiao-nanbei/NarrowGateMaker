from datetime import UTC, datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from models.replay import narrowgate_continuous_tick_adapter as tick_adapter
from models.replay.continuous_accounting import (
    CAMPAIGN_ACCOUNTING_SEMANTICS,
    FEE_ACCOUNTING_SEMANTICS,
    ContinuousAccountingLedger,
)
from models.replay.replay_state_checkpoint import ContinuousReplayState


def _ts(day: int) -> int:
    return int(datetime(2026, 1, day, tzinfo=UTC).timestamp() * 1_000)


def _ledger() -> ContinuousAccountingLedger:
    return ContinuousAccountingLedger(
        ContinuousReplayState(
            arm_id="control",
            checkpoint_ts_ms=_ts(1),
            cash_usdc=0.0,
            position_btc=0.0,
            average_entry_price=0.0,
            cumulative_realized_pnl_usdc=0.0,
            cumulative_fees_usdc=0.0,
            equity_anchor_usdc=0.0,
            last_mark_price=100.0,
            cumulative_pnl_usdc=0.0,
        )
    )


def test_daily_slices_add_to_continuous_pnl_without_midnight_flatten() -> None:
    ledger = _ledger()
    ledger.fill(
        ts_ms=_ts(1) + 1_000,
        side="BUY",
        quantity_btc=1.0,
        price=100.0,
        new_campaign_id="LONG-1",
    )
    first = ledger.close_utc_day(day_end_ts_ms=_ts(2), mark_price=105.0)

    assert first.pnl_usdc == pytest.approx(5.0)
    assert ledger.state.position_btc == 1.0
    assert ledger.state.economic_campaign is not None

    ledger.fill(
        ts_ms=_ts(2) + 1_000,
        side="SELL",
        quantity_btc=1.0,
        price=110.0,
    )
    second = ledger.close_utc_day(day_end_ts_ms=_ts(3), mark_price=110.0)
    audit = ledger.accounting_audit()

    assert second.pnl_usdc == pytest.approx(5.0)
    assert sum(row.pnl_usdc for row in ledger.daily_slices) == pytest.approx(10.0)
    assert ledger.state.cumulative_pnl_usdc == pytest.approx(10.0)
    assert audit["closed_daily_additivity_error_usdc"] == pytest.approx(0.0)
    assert ledger.closed_campaigns[0].value_usdc == pytest.approx(10.0)


@pytest.mark.parametrize("side, signed_qty", [("BUY", 1.0), ("SELL", -1.0)])
def test_tick_adapter_midnight_fill_belongs_to_new_day_once(
    monkeypatch, side: str, signed_qty: float
) -> None:
    ledger = _ledger()
    midnight = _ts(2)
    end = midnight + 1_000
    epoch = tick_adapter.AuthoritativeReplayEpoch(
        epoch_id="synthetic-midnight",
        start_ts_ms=_ts(1),
        quote_stop_ts_ms=midnight + 500,
        end_ts_ms=end,
        warmup_lookback_start_ts_ms=_ts(1),
        gap_id="",
        gap_end_ts_ms=end,
        utc_boundaries_ts_ms=(midnight,),
        source_days=("2026-01-01", "2026-01-02"),
        random_seed=0,
        random_path_sha256="0" * 64,
        terminal=True,
    )
    window = SimpleNamespace(
        trades=pd.DataFrame(
            {"transact_time": [midnight - 1, midnight, end], "price": [100.0, 110.0, 120.0]}
        ),
        var_ts_ms=None,
        var_ssq=None,
        bbo_data=None,
        l2_data=None,
        var_ti=None,
        var_retsq=None,
    )
    monkeypatch.setattr(
        tick_adapter, "assemble_epoch_input", lambda *_args, **_kwargs: (window, (), ())
    )
    adapter = object.__new__(tick_adapter.NarrowGateContinuousTickReplayAdapter)
    adapter.input_provider = SimpleNamespace(load_day=lambda **_kwargs: None)
    adapter.arm_bindings = {
        "control": tick_adapter.AdapterArmBinding("control", {}, "0" * 64, 1_000)
    }
    adapter._simulate = lambda *_args, **_kwargs: {
        "planned_quote_stop_triggered": True,
        "final_inventory": signed_qty,
        "_fill_trace": [
            {
                "fill_ts": midnight,
                "side": side,
                "fill_qty": 1.0,
                "quote_px": 105.0,
                "fill_fee_usdc": 0.25,
            }
        ],
    }

    adapter._simulate_epoch(arm="control", epoch=epoch, ledger=ledger)

    first = ledger.daily_slices[0]
    assert first.day == "2026-01-01"
    assert first.end_inventory_btc == 0.0
    assert first.pnl_usdc == 0.0
    terminal_pnl = signed_qty * (120.0 - 105.0) - 0.25
    audit = ledger.accounting_audit()
    assert audit["open_day_pnl_usdc"] == pytest.approx(terminal_pnl)
    assert audit["continuous_pnl_usdc"] == pytest.approx(terminal_pnl)
    assert ledger.state.position_btc == signed_qty
    assert ledger.state.economic_campaign is not None
    assert ledger.state.economic_campaign.start_ts_ms == midnight
    second = ledger.close_utc_day(day_end_ts_ms=_ts(3), mark_price=120.0)
    assert second.day == "2026-01-02"
    assert second.pnl_usdc == pytest.approx(terminal_pnl)
    assert sum(row.pnl_usdc for row in ledger.daily_slices) == pytest.approx(terminal_pnl)
    assert ledger.accounting_audit()["open_day_pnl_usdc"] == 0.0


def test_gap_inventory_is_marked_while_strategy_is_offline() -> None:
    ledger = _ledger()
    ledger.fill(
        ts_ms=_ts(1) + 1_000,
        side="SELL",
        quantity_btc=0.002,
        price=100_000.0,
        new_campaign_id="SHORT-1",
    )
    gap = ledger.record_gap(
        gap_id="maintenance-1",
        start_ts_ms=_ts(1) + 2_000,
        end_ts_ms=_ts(1) + 3_000,
        start_mark_price=100_000.0,
        end_mark_price=101_000.0,
    )

    assert gap.pnl_usdc == pytest.approx(-2.0)
    assert ledger.state.position_btc == pytest.approx(-0.002)
    assert ledger.state.cumulative_pnl_usdc == pytest.approx(-2.0)


def test_restart_transition_preserves_economics_and_waits_for_warmup() -> None:
    ledger = _ledger()
    ledger.fill(
        ts_ms=_ts(1) + 1_000,
        side="BUY",
        quantity_btc=0.001,
        price=100.0,
        new_campaign_id="LONG-1",
    )
    cash = ledger.state.cash_usdc
    position = ledger.state.position_btc

    stopped = ledger.enter_planned_restart(_ts(1) + 2_000)
    assert stopped.restart_generation == 1
    assert not stopped.quoting_enabled
    assert stopped.cash_usdc == cash
    assert stopped.position_btc == position
    with pytest.raises(ValueError, match="future"):
        ledger.resume_after_warmup(
            decision_ts_ms=_ts(1) + 3_000,
            feature_ready_ts_ms=_ts(1) + 3_001,
        )

    ready = ledger.resume_after_warmup(
        decision_ts_ms=_ts(1) + 3_000,
        feature_ready_ts_ms=_ts(1) + 3_000,
    )
    assert ready.quoting_enabled
    assert ready.restart_generation == 1
    assert ready.cash_usdc == cash
    assert ready.position_btc == position


def test_flip_closes_at_zero_and_carries_opening_fee_to_new_campaign() -> None:
    ledger = _ledger()
    ledger.fill(
        ts_ms=_ts(1) + 1_000,
        side="BUY",
        quantity_btc=0.001,
        price=100.0,
        fee_usdc=0.005,
        new_campaign_id="LONG-1",
    )
    ledger.fill(
        ts_ms=_ts(1) + 2_000,
        side="SELL",
        quantity_btc=0.002,
        price=120.0,
        fee_usdc=0.02,
        new_campaign_id="SHORT-2",
    )

    assert ledger.state.position_btc == pytest.approx(-0.001)
    assert len(ledger.closed_campaigns) == 1
    closed = ledger.closed_campaigns[0]
    assert closed.terminal_reason == "flip"
    assert closed.end_equity_usdc == pytest.approx(0.005)
    assert closed.value_usdc == pytest.approx(0.005)
    assert ledger.state.economic_campaign is not None
    assert ledger.state.economic_campaign.campaign_id == "SHORT-2"
    assert ledger.state.economic_campaign.start_ts_ms == _ts(1) + 2_000
    assert ledger.state.economic_campaign.start_equity_usdc == pytest.approx(0.005)

    ledger.mark(_ts(1) + 2_001, 120.0)
    assert ledger.state.equity_usdc == pytest.approx(-0.005)
    assert (
        ledger.state.equity_usdc
        - ledger.state.economic_campaign.start_equity_usdc
    ) == pytest.approx(-0.01)
    assert ledger.accounting_audit()["campaign_accounting_semantics"] == (
        CAMPAIGN_ACCOUNTING_SEMANTICS
    )
    assert ledger.state.cumulative_fees_usdc == pytest.approx(0.025)


def test_flip_preserves_signed_rebate_and_campaign_additivity() -> None:
    ledger = _ledger()
    ledger.fill(
        ts_ms=_ts(1) + 1_000,
        side="BUY",
        quantity_btc=0.001,
        price=100.0,
        fee_usdc=-0.005,
        new_campaign_id="LONG-REBATE",
    )
    ledger.fill(
        ts_ms=_ts(1) + 2_000,
        side="SELL",
        quantity_btc=0.002,
        price=120.0,
        fee_usdc=-0.02,
        new_campaign_id="SHORT-REBATE",
    )

    closed = ledger.closed_campaigns[0]
    assert closed.terminal_reason == "flip"
    assert closed.value_usdc == pytest.approx(0.035)
    assert ledger.state.cumulative_fees_usdc == pytest.approx(-0.025)
    ledger.mark(_ts(1) + 2_001, 120.0)
    assert ledger.state.equity_usdc == pytest.approx(0.045)
    assert ledger.state.economic_campaign is not None
    assert (
        ledger.state.equity_usdc
        - ledger.state.economic_campaign.start_equity_usdc
    ) == pytest.approx(0.01)
    assert closed.value_usdc + 0.01 == pytest.approx(
        ledger.state.equity_usdc
    )
    assert ledger.accounting_audit()["fee_accounting_semantics"] == (
        FEE_ACCOUNTING_SEMANTICS
    )
