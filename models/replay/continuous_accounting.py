"""Continuous cash, inventory, campaign, and UTC-slice accounting."""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

from .replay_state_checkpoint import (
    ContinuousReplayState,
    EconomicCampaignState,
)

SCHEMA_VERSION = "continuous_accounting_contract.v1"
_EPS = 1e-10


@dataclass(frozen=True)
class DailyPnlSlice:
    day: str
    start_equity_usdc: float
    end_equity_usdc: float
    pnl_usdc: float
    start_inventory_btc: float
    end_inventory_btc: float
    end_mark_price: float


@dataclass(frozen=True)
class ClosedCampaign:
    campaign_id: str
    side: str
    start_ts_ms: int
    end_ts_ms: int
    start_equity_usdc: float
    end_equity_usdc: float
    value_usdc: float
    peak_abs_inventory_btc: float


@dataclass(frozen=True)
class GapCarry:
    gap_id: str
    start_ts_ms: int
    end_ts_ms: int
    position_btc: float
    start_mark_price: float
    end_mark_price: float
    pnl_usdc: float


def _day(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1_000.0, tz=UTC).date().isoformat()


class ContinuousAccountingLedger:
    """Use marked equity as the only authoritative PnL process."""

    def __init__(self, state: ContinuousReplayState) -> None:
        state.validate()
        self._state = state
        self._day_start_equity = state.equity_usdc
        self._day_start_inventory = state.position_btc
        self._day_start = _day(state.checkpoint_ts_ms)
        self.daily_slices: list[DailyPnlSlice] = []
        self.closed_campaigns: list[ClosedCampaign] = []
        self.gap_carries: list[GapCarry] = []

    @property
    def state(self) -> ContinuousReplayState:
        return self._state

    @property
    def equity_usdc(self) -> float:
        return self._state.equity_usdc

    def mark(self, ts_ms: int, price: float) -> ContinuousReplayState:
        if int(ts_ms) < self._state.checkpoint_ts_ms:
            raise ValueError("accounting mark timestamp moved backward")
        self._state = self._state.with_mark(int(ts_ms), float(price))
        return self._state

    def enter_planned_restart(self, ts_ms: int) -> ContinuousReplayState:
        if int(ts_ms) < self._state.checkpoint_ts_ms:
            raise ValueError("planned restart timestamp moved backward")
        self._state = self._state.for_planned_restart(int(ts_ms))
        return self._state

    def resume_after_warmup(
        self,
        *,
        decision_ts_ms: int,
        feature_ready_ts_ms: int,
    ) -> ContinuousReplayState:
        if int(decision_ts_ms) < self._state.checkpoint_ts_ms:
            raise ValueError("restart decision timestamp moved backward")
        if int(feature_ready_ts_ms) > int(decision_ts_ms):
            raise ValueError("restart feature-ready timestamp is in the future")
        state = replace(
            self._state,
            checkpoint_ts_ms=int(decision_ts_ms),
            feature_warmup_ready=True,
            quoting_enabled=True,
        )
        state.validate(require_restart_safe=True)
        self._state = state
        return self._state

    def fill(
        self,
        *,
        ts_ms: int,
        side: str,
        quantity_btc: float,
        price: float,
        fee_usdc: float = 0.0,
        new_campaign_id: str | None = None,
    ) -> ContinuousReplayState:
        if int(ts_ms) < self._state.checkpoint_ts_ms:
            raise ValueError("fill timestamp moved backward")
        side = str(side).upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError("fill side must be BUY or SELL")
        qty = float(quantity_btc)
        px = float(price)
        fee = float(fee_usdc)
        if not math.isfinite(qty) or qty <= 0 or not math.isfinite(px) or px <= 0:
            raise ValueError("fill quantity and price must be positive and finite")
        if not math.isfinite(fee) or fee < 0:
            raise ValueError("fill fee must be finite and non-negative")

        before = self._state
        signed_qty = qty if side == "BUY" else -qty
        q0 = before.position_btc
        q1 = q0 + signed_qty
        if abs(q1) <= _EPS:
            q1 = 0.0
        cash = before.cash_usdc - signed_qty * px - fee
        realized = before.cumulative_realized_pnl_usdc
        entry = before.average_entry_price
        campaign = before.economic_campaign
        closed_campaign: ClosedCampaign | None = None

        same_direction = abs(q0) <= _EPS or q0 * signed_qty > 0
        if same_direction:
            if abs(q0) <= _EPS:
                if not new_campaign_id or not str(new_campaign_id).strip():
                    raise ValueError("opening fill requires a stable economic campaign id")
                entry = px
                start_equity = before.equity_usdc
                campaign = EconomicCampaignState(
                    campaign_id=str(new_campaign_id),
                    side="LONG" if q1 > 0 else "SHORT",
                    start_ts_ms=int(ts_ms),
                    start_equity_usdc=float(start_equity),
                    peak_abs_inventory_btc=abs(q1),
                )
            else:
                entry = (abs(q0) * entry + qty * px) / abs(q1)
                if campaign is None:
                    raise ValueError("non-flat inventory lost its economic campaign")
                campaign = replace(
                    campaign,
                    peak_abs_inventory_btc=max(
                        campaign.peak_abs_inventory_btc,
                        abs(q1),
                    ),
                )
        else:
            closed_qty = min(abs(q0), qty)
            realized += closed_qty * (px - entry) * (1.0 if q0 > 0 else -1.0)
            if abs(q1) <= _EPS:
                if campaign is None:
                    raise ValueError("closing fill lost its economic campaign")
                end_equity = cash
                closed_campaign = ClosedCampaign(
                    campaign_id=campaign.campaign_id,
                    side=campaign.side,
                    start_ts_ms=campaign.start_ts_ms,
                    end_ts_ms=int(ts_ms),
                    start_equity_usdc=campaign.start_equity_usdc,
                    end_equity_usdc=end_equity,
                    value_usdc=end_equity - campaign.start_equity_usdc,
                    peak_abs_inventory_btc=campaign.peak_abs_inventory_btc,
                )
                entry = 0.0
                campaign = None
            elif q0 * q1 > 0:
                if campaign is None:
                    raise ValueError("partial reduction lost its economic campaign")
            else:
                if campaign is None:
                    raise ValueError("inventory flip lost its closing campaign")
                close_equity = cash + q1 * px
                closed_campaign = ClosedCampaign(
                    campaign_id=campaign.campaign_id,
                    side=campaign.side,
                    start_ts_ms=campaign.start_ts_ms,
                    end_ts_ms=int(ts_ms),
                    start_equity_usdc=campaign.start_equity_usdc,
                    end_equity_usdc=close_equity,
                    value_usdc=close_equity - campaign.start_equity_usdc,
                    peak_abs_inventory_btc=campaign.peak_abs_inventory_btc,
                )
                if not new_campaign_id or not str(new_campaign_id).strip():
                    raise ValueError("inventory flip requires a new economic campaign id")
                entry = px
                campaign = EconomicCampaignState(
                    campaign_id=str(new_campaign_id),
                    side="LONG" if q1 > 0 else "SHORT",
                    start_ts_ms=int(ts_ms),
                    start_equity_usdc=close_equity,
                    peak_abs_inventory_btc=abs(q1),
                )

        mark_price = before.last_mark_price
        equity = cash + q1 * mark_price
        self._state = replace(
            before,
            checkpoint_ts_ms=int(ts_ms),
            cash_usdc=cash,
            position_btc=q1,
            average_entry_price=entry,
            cumulative_realized_pnl_usdc=realized,
            cumulative_fees_usdc=before.cumulative_fees_usdc + fee,
            cumulative_pnl_usdc=equity - before.equity_anchor_usdc,
            economic_campaign=campaign,
        )
        self._state.validate()
        if closed_campaign is not None:
            self.closed_campaigns.append(closed_campaign)
        return self._state

    def record_gap(
        self,
        *,
        gap_id: str,
        start_ts_ms: int,
        end_ts_ms: int,
        start_mark_price: float,
        end_mark_price: float,
    ) -> GapCarry:
        if end_ts_ms <= start_ts_ms:
            raise ValueError("gap end must follow gap start")
        self.mark(start_ts_ms, start_mark_price)
        position = self._state.position_btc
        self.mark(end_ts_ms, end_mark_price)
        row = GapCarry(
            gap_id=str(gap_id),
            start_ts_ms=int(start_ts_ms),
            end_ts_ms=int(end_ts_ms),
            position_btc=position,
            start_mark_price=float(start_mark_price),
            end_mark_price=float(end_mark_price),
            pnl_usdc=position * (float(end_mark_price) - float(start_mark_price)),
        )
        self.gap_carries.append(row)
        return row

    def close_utc_day(self, *, day_end_ts_ms: int, mark_price: float) -> DailyPnlSlice:
        self.mark(day_end_ts_ms, mark_price)
        current_day = _day(max(0, int(day_end_ts_ms) - 1))
        if current_day != self._day_start:
            raise ValueError(
                f"UTC accounting boundary mismatch: expected={self._day_start} got={current_day}"
            )
        row = DailyPnlSlice(
            day=current_day,
            start_equity_usdc=self._day_start_equity,
            end_equity_usdc=self._state.equity_usdc,
            pnl_usdc=self._state.equity_usdc - self._day_start_equity,
            start_inventory_btc=self._day_start_inventory,
            end_inventory_btc=self._state.position_btc,
            end_mark_price=float(mark_price),
        )
        self.daily_slices.append(row)
        self._day_start_equity = self._state.equity_usdc
        self._day_start_inventory = self._state.position_btc
        self._day_start = _day(int(day_end_ts_ms))
        return row

    def accounting_audit(self) -> dict[str, Any]:
        daily_sum = sum(row.pnl_usdc for row in self.daily_slices)
        closed_days_pnl = self._day_start_equity - self._state.equity_anchor_usdc
        return {
            "schema_version": f"{SCHEMA_VERSION}.audit",
            "daily_slice_count": len(self.daily_slices),
            "closed_daily_pnl_sum_usdc": daily_sum,
            "closed_daily_equity_change_usdc": closed_days_pnl,
            "closed_daily_additivity_error_usdc": daily_sum - closed_days_pnl,
            "continuous_pnl_usdc": self._state.cumulative_pnl_usdc,
            "open_day_pnl_usdc": self._state.equity_usdc - self._day_start_equity,
            "campaigns_closed": len(self.closed_campaigns),
            "gap_count": len(self.gap_carries),
            "gap_inventory_pnl_usdc": sum(row.pnl_usdc for row in self.gap_carries),
        }
