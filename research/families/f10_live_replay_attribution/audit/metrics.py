"""Shared metric builders for NarrowGate audit reports."""

from __future__ import annotations

import bisect
import math
import random
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

from models.audit.support import (
    AGE_THRESHOLDS_S,
    EARLY_WINDOWS_S,
    EPS,
    INV_THRESHOLDS,
    norm_side,
    safe_float,
    safe_int,
    session_stack,
    utc_day,
    utc_text,
)


def inventory_role(side: Any, q_before: float) -> str:
    """Classify an order/fill from inventory known immediately beforehand.

    ``opener`` is intentionally separate from ``add``.  Both increase absolute
    exposure, but only ``add`` compounds an already-open campaign.  A crossing
    order is classified from its initial intent as reducing; the current maker
    uses one-lot orders, so crossing through flat is not a normal path.
    """
    normalized = norm_side(side)
    if normalized not in {"BUY", "SELL"} or not math.isfinite(q_before):
        return "unknown"
    if abs(q_before) < EPS:
        return "opener"
    if (normalized == "BUY" and q_before > 0.0) or (normalized == "SELL" and q_before < 0.0):
        return "add"
    return "reducing"


def _first_finite(row: dict[str, Any], *keys: str) -> float:
    for key in keys:
        value = safe_float(row, key, math.nan)
        if math.isfinite(value):
            return value
    return math.nan


def _norm_ms_ts(row: dict[str, Any], *keys: str) -> float:
    """Return the first finite timestamp in seconds, accepting epoch ms."""
    value = math.nan
    for key in keys:
        value = safe_float(row, key, math.nan)
        if math.isfinite(value) and value > 0.0:
            break
    if not math.isfinite(value):
        return 0.0
    return value / 1000.0 if value > 10_000_000_000 else value


@dataclass
class TradeRow:
    ts: float
    side: str
    trade_type: str
    qty: float
    price: float
    position: float
    realized_pnl: float
    unrealized_pnl: float

    @property
    def total_pnl(self) -> float:
        return self.realized_pnl + self.unrealized_pnl

    @property
    def is_real_fill(self) -> bool:
        return self.trade_type != "SYNC_ADJUST"


@dataclass
class Campaign:
    campaign_id: int
    start_ts: float
    start_position: float
    start_realized_pnl: float
    start_total_pnl: float
    end_ts: float = 0.0
    final_position: float = 0.0
    closed: bool = False
    final_realized_pnl: float = 0.0
    final_total_pnl: float = 0.0
    max_abs_inventory: float = 0.0
    min_total_pnl_delta: float = 0.0
    max_total_pnl_delta: float = 0.0
    fills: int = 0
    buy_fills: int = 0
    sell_fills: int = 0
    exposure_increasing_fills: int = 0
    reducing_fills: int = 0
    fill_sequence: list[str] = field(default_factory=list)
    early_min_pnl_delta: dict[int, float] = field(default_factory=dict)
    shadow_inv_blocks: dict[float, int] = field(
        default_factory=lambda: {x: 0 for x in INV_THRESHOLDS}
    )
    shadow_age_blocks: dict[int, int] = field(
        default_factory=lambda: {x: 0 for x in AGE_THRESHOLDS_S}
    )
    shadow_reducing_only_blocks: int = 0

    def update_path(self, row: TradeRow) -> None:
        pnl_delta = row.total_pnl - self.start_total_pnl
        self.final_total_pnl = row.total_pnl
        self.max_abs_inventory = max(self.max_abs_inventory, abs(row.position))
        self.min_total_pnl_delta = min(self.min_total_pnl_delta, pnl_delta)
        self.max_total_pnl_delta = max(self.max_total_pnl_delta, pnl_delta)
        elapsed = row.ts - self.start_ts
        for window_s in EARLY_WINDOWS_S:
            if elapsed <= window_s:
                self.early_min_pnl_delta[window_s] = min(
                    self.early_min_pnl_delta.get(window_s, 0.0),
                    pnl_delta,
                )

    def update_fill(self, prev_position: float, row: TradeRow) -> None:
        if not row.is_real_fill:
            return
        self.fills += 1
        if row.side == "BUY":
            self.buy_fills += 1
        elif row.side == "SELL":
            self.sell_fills += 1
        self.fill_sequence.append(row.side[:1])
        exposure_increasing = abs(row.position) > abs(prev_position) + EPS
        reducing = abs(row.position) < abs(prev_position) - EPS
        if exposure_increasing:
            self.exposure_increasing_fills += 1
        if reducing:
            self.reducing_fills += 1
        age_s = row.ts - self.start_ts
        if exposure_increasing and abs(prev_position) > EPS:
            self.shadow_reducing_only_blocks += 1
            for threshold in INV_THRESHOLDS:
                if abs(prev_position) >= threshold:
                    self.shadow_inv_blocks[threshold] += 1
            for threshold_s in AGE_THRESHOLDS_S:
                if age_s >= threshold_s:
                    self.shadow_age_blocks[threshold_s] += 1


@dataclass(frozen=True)
class CampaignPolicy:
    name: str
    inv_threshold: float | None = None
    age_threshold_s: float | None = None
    reducing_only: bool = False


@dataclass
class ShadowCampaignState:
    campaign_id: int
    start_ts: float
    start_position: float
    start_equity: float
    end_ts: float = 0.0
    closed: bool = False
    max_abs_inventory: float = 0.0
    min_equity_delta: float = 0.0
    max_equity_delta: float = 0.0
    fills: int = 0
    buy_fills: int = 0
    sell_fills: int = 0
    exposure_increasing_fills: int = 0
    reducing_fills: int = 0
    blocked_fills: int = 0

    def mark(self, ts: float, position: float, equity: float) -> None:
        delta = equity - self.start_equity
        self.end_ts = ts
        self.max_abs_inventory = max(self.max_abs_inventory, abs(position))
        self.min_equity_delta = min(self.min_equity_delta, delta)
        self.max_equity_delta = max(self.max_equity_delta, delta)


@dataclass
class CampaignPolicyReplayState:
    policy: CampaignPolicy
    position: float
    cash: float
    last_price: float
    accepted_fills: int = 0
    blocked_fills: int = 0
    blocked_buy_fills: int = 0
    blocked_sell_fills: int = 0
    blocked_qty: float = 0.0
    exposure_increasing_fills: int = 0
    reducing_fills: int = 0
    campaigns: list[ShadowCampaignState] = field(default_factory=list)
    active: ShadowCampaignState | None = None
    campaign_id: int = 0

    @property
    def equity(self) -> float:
        return self.cash + self.position * self.last_price

    def ensure_active_for_initial_inventory(self, ts: float) -> None:
        if self.active is None and abs(self.position) >= EPS:
            self.campaign_id += 1
            self.active = ShadowCampaignState(
                campaign_id=self.campaign_id,
                start_ts=ts,
                start_position=self.position,
                start_equity=self.equity,
                max_abs_inventory=abs(self.position),
            )

    def mark_active(self, ts: float) -> None:
        if self.active is not None:
            self.active.mark(ts, self.position, self.equity)

    def start_campaign_after_fill(self, ts: float) -> None:
        if self.active is None and abs(self.position) >= EPS:
            self.campaign_id += 1
            self.active = ShadowCampaignState(
                campaign_id=self.campaign_id,
                start_ts=ts,
                start_position=self.position,
                start_equity=self.equity,
                max_abs_inventory=abs(self.position),
            )

    def close_if_flat(self, ts: float) -> None:
        if self.active is not None and abs(self.position) < EPS:
            self.active.mark(ts, self.position, self.equity)
            self.active.closed = True
            self.campaigns.append(self.active)
            self.active = None

    def finish(self, ts: float) -> None:
        if self.active is not None:
            self.active.mark(ts, self.position, self.equity)
            self.campaigns.append(self.active)
            self.active = None


@dataclass(frozen=True)
class ReducingCooldownPolicy:
    name: str
    cooldown_s: float
    high_campaign_only: bool = False
    vol_scaled: bool = False


@dataclass
class ReducingCooldownReplayState:
    """成交序列级 reducing cooldown shadow 状态。

    中文说明：这不是完整盘口反事实，只回答一个很窄的问题：如果减仓方向
    也有短 cooldown，真实成交序列里哪些 reducing fill 会被挡住，最终库存
    campaign proxy 是变好还是变坏。
    """

    policy: ReducingCooldownPolicy
    position: float
    cash: float
    last_price: float
    accepted_fills: int = 0
    blocked_fills: int = 0
    blocked_buy_fills: int = 0
    blocked_sell_fills: int = 0
    blocked_qty: float = 0.0
    blocked_reducing_fills: int = 0
    exposure_increasing_fills: int = 0
    reducing_fills: int = 0
    last_reducing_ts_by_side: dict[str, float] = field(default_factory=dict)
    campaigns: list[ShadowCampaignState] = field(default_factory=list)
    active: ShadowCampaignState | None = None
    campaign_id: int = 0

    @property
    def equity(self) -> float:
        return self.cash + self.position * self.last_price

    def ensure_active_for_initial_inventory(self, ts: float) -> None:
        if self.active is None and abs(self.position) >= EPS:
            self.campaign_id += 1
            self.active = ShadowCampaignState(
                campaign_id=self.campaign_id,
                start_ts=ts,
                start_position=self.position,
                start_equity=self.equity,
                max_abs_inventory=abs(self.position),
            )

    def mark_active(self, ts: float) -> None:
        if self.active is not None:
            self.active.mark(ts, self.position, self.equity)

    def start_campaign_after_fill(self, ts: float) -> None:
        if self.active is None and abs(self.position) >= EPS:
            self.campaign_id += 1
            self.active = ShadowCampaignState(
                campaign_id=self.campaign_id,
                start_ts=ts,
                start_position=self.position,
                start_equity=self.equity,
                max_abs_inventory=abs(self.position),
            )

    def close_if_flat(self, ts: float) -> None:
        if self.active is not None and abs(self.position) < EPS:
            self.active.mark(ts, self.position, self.equity)
            self.active.closed = True
            self.campaigns.append(self.active)
            self.active = None

    def finish(self, ts: float) -> None:
        if self.active is not None:
            self.active.mark(ts, self.position, self.equity)
            self.campaigns.append(self.active)
            self.active = None


@dataclass
class ReplayCampaignSnapshot:
    """Quote-time campaign state reconstructed from replay fills.

    中文说明：replay trace 里每笔 order 有当时库存，但没有完整 campaign
    shadow log。这里用已经发生的 replay fills 按时间重建库存 campaign，再在
    order submit 时刻取快照。这个状态只能使用 submit 前的 fills 和当前 mid，
    不能看当前 order 是否最终成交。
    """

    active: bool = False
    campaign_id: int = 0
    q: float = 0.0
    side: str = "FLAT"
    age_s: float = 0.0
    duration_s: float = 0.0
    max_abs_qty: float = 0.0
    total_pnl: float = 0.0
    adverse_excursion: float = 0.0
    exposure_increasing_fills: int = 0
    reducing_fills: int = 0


@dataclass
class ReplayCampaignTracker:
    """Minimal campaign state machine for replay order-level evidence."""

    campaign_id: int = 0
    active: bool = False
    start_ts: float = 0.0
    start_equity: float = 0.0
    q: float = 0.0
    cash: float = 0.0
    max_abs_qty: float = 0.0
    min_equity_delta: float = 0.0
    max_equity_delta: float = 0.0
    exposure_increasing_fills: int = 0
    reducing_fills: int = 0

    def _equity(self, mark_px: float) -> float:
        return self.cash + self.q * mark_px

    def _start(self, ts: float, mark_px: float) -> None:
        self.campaign_id += 1
        self.active = True
        self.start_ts = ts
        self.start_equity = self._equity(mark_px)
        self.max_abs_qty = abs(self.q)
        self.min_equity_delta = 0.0
        self.max_equity_delta = 0.0
        self.exposure_increasing_fills = 0
        self.reducing_fills = 0

    def _resync_position(self, q: float, mark_px: float) -> None:
        """Align with replay's authoritative inventory without inventing PnL."""
        if not math.isfinite(q):
            return
        if abs(q - self.q) <= 1e-9:
            return
        equity = self._equity(mark_px) if mark_px > 0.0 else self.cash
        self.q = q
        if mark_px > 0.0:
            self.cash = equity - self.q * mark_px

    def ensure_from_order_inventory(self, *, ts: float, q_before: float, mid: float) -> None:
        """Fallback when an order has nonzero inventory before any parsed fill."""
        if not math.isfinite(q_before):
            return
        mark_px = mid if mid > 0.0 else 1.0
        if self.active:
            self._resync_position(q_before, mark_px)
            if abs(self.q) < EPS:
                self.active = False
                self.cash = 0.0
            return
        if abs(q_before) < EPS:
            return
        if not self.active and abs(self.q) < EPS:
            # 中文说明：这通常只会出现在单元测试或截断 trace 中；用当前
            # order mid 建一个合成 campaign，age 从当前时刻开始，避免
            # campaign score 继续全为 0。
            self.q = q_before
            self.cash = -self.q * mark_px
            self._start(ts, mark_px)

    def mark(self, ts: float, mid: float) -> None:
        if not self.active or mid <= 0.0:
            return
        delta = self._equity(mid) - self.start_equity
        self.max_abs_qty = max(self.max_abs_qty, abs(self.q))
        self.min_equity_delta = min(self.min_equity_delta, delta)
        self.max_equity_delta = max(self.max_equity_delta, delta)

    def apply_fill(self, row: dict[str, Any]) -> None:
        side = norm_side(row.get("side", ""))
        qty = safe_float(row, "fill_qty")
        px = safe_float(row, "fill_trade_px", safe_float(row, "price"))
        fill_ts_raw = safe_float(row, "fill_ts")
        ts = fill_ts_raw / 1000.0 if fill_ts_raw > 10_000_000_000 else fill_ts_raw
        if side not in {"BUY", "SELL"} or qty <= 0.0 or px <= 0.0 or ts <= 0.0:
            return

        # New traces carry the exact pre-fill inventory.  Older traces only
        # carry quote-time inventory, which can be stale after a resting order
        # survives other fills; in that case the sequential tracker is safer.
        prev_q = _first_finite(row, "inventory_before_fill", "fill_q_before")
        if not math.isfinite(prev_q):
            prev_q = self.q
        if abs(prev_q - self.q) > 1e-9:
            self._resync_position(prev_q, px)

        signed_qty = qty if side == "BUY" else -qty
        after_q = prev_q + signed_qty
        exposure_increasing = abs(after_q) > abs(prev_q) + EPS
        reducing = abs(after_q) < abs(prev_q) - EPS

        self.cash += -qty * px if side == "BUY" else qty * px
        self.q = after_q
        if not self.active and abs(self.q) >= EPS:
            self._start(ts, px)
        if self.active:
            if exposure_increasing:
                self.exposure_increasing_fills += 1
            if reducing:
                self.reducing_fills += 1
            self.mark(ts, px)
            if abs(self.q) < EPS:
                # 中文说明：campaign 结束后保留 campaign_id 递增状态；
                # 下一次非零库存会开启新的 campaign。
                self.active = False
                self.q = 0.0
                self.cash = 0.0

    def snapshot(self, ts: float, mid: float) -> ReplayCampaignSnapshot:
        self.mark(ts, mid)
        if not self.active:
            return ReplayCampaignSnapshot(q=self.q)
        age = max(0.0, ts - self.start_ts)
        delta = self._equity(mid) - self.start_equity if mid > 0.0 else 0.0
        side = "LONG" if self.q > EPS else "SHORT" if self.q < -EPS else "FLAT"
        return ReplayCampaignSnapshot(
            active=True,
            campaign_id=self.campaign_id,
            q=self.q,
            side=side,
            age_s=age,
            duration_s=age,
            max_abs_qty=max(self.max_abs_qty, abs(self.q)),
            total_pnl=delta,
            adverse_excursion=self.min_equity_delta,
            exposure_increasing_fills=self.exposure_increasing_fills,
            reducing_fills=self.reducing_fills,
        )


def _trade_signed_qty(row: TradeRow) -> float:
    return row.qty if row.side == "BUY" else -row.qty if row.side == "SELL" else 0.0


def _campaign_policy_blocks(
    policy: CampaignPolicy,
    *,
    ts: float,
    position_before: float,
    signed_qty: float,
    active: ShadowCampaignState | None,
) -> bool:
    if active is None or abs(position_before) < EPS:
        return False
    position_after = position_before + signed_qty
    exposure_increasing = abs(position_after) > abs(position_before) + EPS
    if not exposure_increasing:
        return False
    if policy.reducing_only:
        return True
    if policy.inv_threshold is not None and abs(position_before) >= policy.inv_threshold:
        return True
    if policy.age_threshold_s is not None and ts - active.start_ts >= policy.age_threshold_s:
        return True
    return False


def _campaign_policy_list() -> list[CampaignPolicy]:
    return [
        CampaignPolicy("observed_sequence"),
        CampaignPolicy("stop_add_inv_006", inv_threshold=0.006),
        CampaignPolicy("stop_add_inv_008", inv_threshold=0.008),
        CampaignPolicy("stop_add_inv_010", inv_threshold=0.010),
        CampaignPolicy("stop_add_age_20m", age_threshold_s=20 * 60),
        CampaignPolicy("stop_add_age_40m", age_threshold_s=40 * 60),
        CampaignPolicy("stop_add_age_60m", age_threshold_s=60 * 60),
        CampaignPolicy("reducing_only_after_campaign_start", reducing_only=True),
        CampaignPolicy("stop_add_inv_006_or_age_20m", inv_threshold=0.006, age_threshold_s=20 * 60),
        CampaignPolicy("stop_add_inv_006_or_age_40m", inv_threshold=0.006, age_threshold_s=40 * 60),
        CampaignPolicy("stop_add_inv_006_or_age_60m", inv_threshold=0.006, age_threshold_s=60 * 60),
    ]


def _campaign_state_row(state: CampaignPolicyReplayState) -> dict[str, Any]:
    closed = sum(1 for c in state.campaigns if c.closed)
    open_count = len(state.campaigns) - closed
    max_inv = max((c.max_abs_inventory for c in state.campaigns), default=0.0)
    max_age = max(((c.end_ts - c.start_ts) for c in state.campaigns), default=0.0)
    worst_mae = min((c.min_equity_delta for c in state.campaigns), default=0.0)
    long_campaigns = sum(1 for c in state.campaigns if c.start_position > 0)
    short_campaigns = sum(1 for c in state.campaigns if c.start_position < 0)
    return {
        "accepted_fills": state.accepted_fills,
        "blocked_fills": state.blocked_fills,
        "blocked_buy_fills": state.blocked_buy_fills,
        "blocked_sell_fills": state.blocked_sell_fills,
        "blocked_qty": f"{state.blocked_qty:.6f}",
        "exposure_increasing_fills": state.exposure_increasing_fills,
        "reducing_fills": state.reducing_fills,
        "campaigns": len(state.campaigns),
        "closed_campaigns": closed,
        "open_campaigns": open_count,
        "long_campaigns": long_campaigns,
        "short_campaigns": short_campaigns,
        "max_abs_inventory": f"{max_inv:.6f}",
        "max_campaign_age_s": f"{max_age:.1f}",
        "worst_campaign_mae_proxy": f"{worst_mae:.6f}",
        "final_position": f"{state.position:+.6f}",
        "pnl_proxy": f"{state.equity:.6f}",
    }


def _initial_shadow_position(real_fills: list[TradeRow]) -> tuple[float, float]:
    first = real_fills[0]
    initial_position = first.position - _trade_signed_qty(first)
    return initial_position, first.price


def _resync_shadow_state_to_trade(
    state: CampaignPolicyReplayState | ReducingCooldownReplayState,
    row: TradeRow,
) -> None:
    """Resync shadow path to an authoritative live position row.

    中文说明：live trades.csv 里的 SYNC_ADJUST 不是可阻断成交，但它会修正
    position/PnL。shadow replay 必须吸收这个账本状态，否则 observed baseline
    会和真实 campaign 路径漂移，后续 PnL/MAE proxy 会失真。
    """
    if row.price <= 0.0:
        return
    prev_position = state.position
    state.last_price = row.price
    state.cash = row.total_pnl - row.position * row.price
    state.position = row.position
    if state.active is not None:
        state.active.mark(row.ts, state.position, state.equity)
        if abs(state.position) < EPS:
            state.active.closed = True
            state.campaigns.append(state.active)
            state.active = None
        elif abs(prev_position) >= EPS and prev_position * state.position < 0.0:
            state.active.closed = True
            state.campaigns.append(state.active)
            state.active = None
    if state.active is None and abs(state.position) >= EPS:
        state.campaign_id += 1
        state.active = ShadowCampaignState(
            campaign_id=state.campaign_id,
            start_ts=row.ts,
            start_position=state.position,
            start_equity=state.equity,
            max_abs_inventory=abs(state.position),
        )


def campaign_policy_replay_rows(trades: list[TradeRow]) -> list[dict[str, Any]]:
    """Replay observed fills through simple campaign-level shadow policies.

    中文说明：这是成交序列级 shadow replay，不是完整盘口/队列级反事实。
    它按 shadow 自己的库存路径判断一笔真实成交是否会继续加仓；若策略
    命中，则跳过该 fill，并继续用后续真实 fill 价格 mark-to-market。
    """
    events = [t for t in trades if t.side in {"BUY", "SELL"} and t.qty > 0 and t.price > 0]
    real_fills = [t for t in events if t.is_real_fill]
    if not real_fills:
        return []
    real_fills.sort(key=lambda x: x.ts)
    events.sort(key=lambda x: x.ts)
    initial_position, first_price = _initial_shadow_position(real_fills)
    states = [
        CampaignPolicyReplayState(
            policy=policy,
            position=initial_position,
            cash=-initial_position * first_price,
            last_price=first_price,
        )
        for policy in _campaign_policy_list()
    ]

    for fill in events:
        if not fill.is_real_fill:
            for state in states:
                _resync_shadow_state_to_trade(state, fill)
            continue
        signed_qty = _trade_signed_qty(fill)
        for state in states:
            state.last_price = fill.price
            state.ensure_active_for_initial_inventory(fill.ts)
            state.mark_active(fill.ts)
            blocked = _campaign_policy_blocks(
                state.policy,
                ts=fill.ts,
                position_before=state.position,
                signed_qty=signed_qty,
                active=state.active,
            )
            if blocked:
                state.blocked_fills += 1
                state.blocked_qty += fill.qty
                if fill.side == "BUY":
                    state.blocked_buy_fills += 1
                else:
                    state.blocked_sell_fills += 1
                if state.active is not None:
                    state.active.blocked_fills += 1
                    state.active.mark(fill.ts, state.position, state.equity)
                continue

            before = state.position
            after = before + signed_qty
            exposure_increasing = abs(after) > abs(before) + EPS
            reducing = abs(after) < abs(before) - EPS
            state.cash -= signed_qty * fill.price
            state.position = after
            state.accepted_fills += 1
            if exposure_increasing:
                state.exposure_increasing_fills += 1
            if reducing:
                state.reducing_fills += 1
            state.start_campaign_after_fill(fill.ts)
            if state.active is not None:
                state.active.fills += 1
                if fill.side == "BUY":
                    state.active.buy_fills += 1
                else:
                    state.active.sell_fills += 1
                if exposure_increasing:
                    state.active.exposure_increasing_fills += 1
                if reducing:
                    state.active.reducing_fills += 1
                state.active.mark(fill.ts, state.position, state.equity)
            state.close_if_flat(fill.ts)

    end_ts = real_fills[-1].ts
    for state in states:
        state.finish(end_ts)

    rows: list[dict[str, Any]] = []
    baseline = next(s for s in states if s.policy.name == "observed_sequence")
    baseline_equity = baseline.equity
    baseline_campaigns = len(baseline.campaigns)
    baseline_row = _campaign_state_row(baseline)
    baseline_max_inv = safe_float(baseline_row, "max_abs_inventory")
    baseline_worst_mae = safe_float(baseline_row, "worst_campaign_mae_proxy")
    for state in states:
        state_row = _campaign_state_row(state)
        max_inv = safe_float(state_row, "max_abs_inventory")
        worst_mae = safe_float(state_row, "worst_campaign_mae_proxy")
        rows.append(
            {
                "policy": state.policy.name,
                **state_row,
                "delta_pnl_proxy_vs_observed": f"{state.equity - baseline_equity:.6f}",
                "delta_campaigns_vs_observed": len(state.campaigns) - baseline_campaigns,
                "delta_max_inventory_vs_observed": f"{max_inv - baseline_max_inv:.6f}",
                "delta_worst_mae_vs_observed": f"{worst_mae - baseline_worst_mae:.6f}",
            }
        )
    rows.sort(
        key=lambda r: (
            0 if r["policy"] == "observed_sequence" else 1,
            -safe_float(r, "delta_pnl_proxy_vs_observed"),
        )
    )
    return rows


def campaign_policy_blocked_fill_rows(trades: list[TradeRow]) -> list[dict[str, Any]]:
    """Per-fill detail for campaign stop-add shadow policies.

    中文说明：这张表回答“规则会挡掉哪些后续成交”。它用 shadow policy
    自己的 counterfactual inventory 判断是否加仓，输出的 PnL/MAE 仍只是
    成交序列 proxy，不是完整盘口重放。
    """
    events = [t for t in trades if t.side in {"BUY", "SELL"} and t.qty > 0 and t.price > 0]
    real_fills = [t for t in events if t.is_real_fill]
    if not real_fills:
        return []
    real_fills.sort(key=lambda x: x.ts)
    events.sort(key=lambda x: x.ts)
    initial_position, first_price = _initial_shadow_position(real_fills)
    states = [
        CampaignPolicyReplayState(
            policy=policy,
            position=initial_position,
            cash=-initial_position * first_price,
            last_price=first_price,
        )
        for policy in _campaign_policy_list()
        if policy.name != "observed_sequence"
    ]
    rows: list[dict[str, Any]] = []
    observed_prev_position = initial_position
    real_fill_idx = 0
    for fill in events:
        if not fill.is_real_fill:
            observed_prev_position = fill.position
            for state in states:
                _resync_shadow_state_to_trade(state, fill)
            continue
        idx = real_fill_idx
        real_fill_idx += 1
        signed_qty = _trade_signed_qty(fill)
        observed_position_before = observed_prev_position
        observed_position_after = fill.position
        observed_exposure_increasing = (
            abs(observed_position_after) > abs(observed_position_before) + EPS
        )
        observed_reducing = abs(observed_position_after) < abs(observed_position_before) - EPS
        for state in states:
            state.last_price = fill.price
            state.ensure_active_for_initial_inventory(fill.ts)
            state.mark_active(fill.ts)
            position_before = state.position
            position_after = position_before + signed_qty
            exposure_increasing = abs(position_after) > abs(position_before) + EPS
            active_age_s = fill.ts - state.active.start_ts if state.active is not None else 0.0
            active_max_inv = state.active.max_abs_inventory if state.active is not None else 0.0
            active_mae = state.active.min_equity_delta if state.active is not None else 0.0
            blocked = _campaign_policy_blocks(
                state.policy,
                ts=fill.ts,
                position_before=position_before,
                signed_qty=signed_qty,
                active=state.active,
            )
            if blocked:
                state.blocked_fills += 1
                state.blocked_qty += fill.qty
                if fill.side == "BUY":
                    state.blocked_buy_fills += 1
                else:
                    state.blocked_sell_fills += 1
                if state.active is not None:
                    state.active.blocked_fills += 1
                    state.active.mark(fill.ts, state.position, state.equity)
                rows.append(
                    {
                        "policy": state.policy.name,
                        "fill_index": idx,
                        "timestamp": f"{fill.ts:.3f}",
                        "utc": utc_text(fill.ts),
                        "day": utc_day(fill.ts),
                        "session_stack": session_stack(fill.ts),
                        "side": fill.side,
                        "qty": f"{fill.qty:.6f}",
                        "price": f"{fill.price:.4f}",
                        "observed_position_before": f"{observed_position_before:+.6f}",
                        "observed_position_after": f"{observed_position_after:+.6f}",
                        "observed_exposure_increasing": int(observed_exposure_increasing),
                        "observed_reducing": int(observed_reducing),
                        "shadow_position_before": f"{position_before:+.6f}",
                        "shadow_position_if_accepted": f"{position_after:+.6f}",
                        "shadow_exposure_increasing": int(exposure_increasing),
                        "shadow_campaign_id": state.active.campaign_id
                        if state.active is not None
                        else "",
                        "shadow_campaign_age_s": f"{max(0.0, active_age_s):.3f}",
                        "shadow_campaign_max_abs_inventory": f"{active_max_inv:.6f}",
                        "shadow_campaign_mae_proxy": f"{active_mae:.6f}",
                        "blocked_fills_so_far": state.blocked_fills,
                        "blocked_qty_so_far": f"{state.blocked_qty:.6f}",
                    }
                )
                continue

            before = state.position
            after = before + signed_qty
            accepted_exposure_increasing = abs(after) > abs(before) + EPS
            accepted_reducing = abs(after) < abs(before) - EPS
            state.cash -= signed_qty * fill.price
            state.position = after
            state.accepted_fills += 1
            if accepted_exposure_increasing:
                state.exposure_increasing_fills += 1
            if accepted_reducing:
                state.reducing_fills += 1
            state.start_campaign_after_fill(fill.ts)
            if state.active is not None:
                state.active.fills += 1
                if fill.side == "BUY":
                    state.active.buy_fills += 1
                else:
                    state.active.sell_fills += 1
                if accepted_exposure_increasing:
                    state.active.exposure_increasing_fills += 1
                if accepted_reducing:
                    state.active.reducing_fills += 1
                state.active.mark(fill.ts, state.position, state.equity)
            state.close_if_flat(fill.ts)
        observed_prev_position = observed_position_after
    return rows


def campaign_policy_blocked_fill_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    by_policy = Counter(str(r.get("policy", "")) for r in rows)
    return {
        "blocked_fill_rows": len(rows),
        "policies": len(by_policy),
        "top_policy_block_counts": dict(by_policy.most_common(8)),
        "blocked_buy_fills": sum(1 for r in rows if r.get("side") == "BUY"),
        "blocked_sell_fills": sum(1 for r in rows if r.get("side") == "SELL"),
        "avg_shadow_campaign_age_s": _safe_mean(
            [safe_float(r, "shadow_campaign_age_s", math.nan) for r in rows]
        ),
        "avg_shadow_campaign_max_abs_inventory": _safe_mean(
            [safe_float(r, "shadow_campaign_max_abs_inventory", math.nan) for r in rows]
        ),
        "avg_shadow_campaign_mae_proxy": _safe_mean(
            [safe_float(r, "shadow_campaign_mae_proxy", math.nan) for r in rows]
        ),
    }


def _reducing_cooldown_policies() -> list[ReducingCooldownPolicy]:
    policies = [ReducingCooldownPolicy("observed_sequence", cooldown_s=0.0)]
    for seconds in (5.0, 8.0, 12.0, 20.0):
        policies.append(ReducingCooldownPolicy(f"reducing_cd_{int(seconds)}s", cooldown_s=seconds))
        policies.append(
            ReducingCooldownPolicy(
                f"reducing_cd_{int(seconds)}s_high_campaign_only",
                cooldown_s=seconds,
                high_campaign_only=True,
            )
        )
    for seconds in (8.0, 12.0):
        policies.append(
            ReducingCooldownPolicy(
                f"reducing_cd_{int(seconds)}s_vol_scaled_proxy", cooldown_s=seconds, vol_scaled=True
            )
        )
    return policies


def _reducing_cooldown_active(
    state: ReducingCooldownReplayState,
    *,
    fill: TradeRow,
    reducing: bool,
) -> bool:
    if state.policy.cooldown_s <= 0.0 or not reducing:
        return False
    if state.policy.high_campaign_only:
        if state.active is None:
            return False
        age_s = fill.ts - state.active.start_ts
        high_campaign = (
            abs(state.position) >= 0.006
            or age_s >= 60.0 * 60.0
            or state.active.min_equity_delta <= -1.0
        )
        if not high_campaign:
            return False
    cooldown_s = state.policy.cooldown_s
    if state.policy.vol_scaled:
        # 成交序列 audit 没有 quote-time vol；用连续同向 reducing fill 的
        # 最保守 proxy：基础值的 0.5-2.0 区间中值。真正 vol-scaled 必须
        # 在 replay quote/order table 上验证。
        cooldown_s *= 1.0
    last_ts = state.last_reducing_ts_by_side.get(fill.side, -1e30)
    return fill.ts - last_ts < cooldown_s


def reducing_cooldown_replay_rows(trades: list[TradeRow]) -> list[dict[str, Any]]:
    """Replay observed fills with reducing-side cooldown shadow policies."""
    events = [t for t in trades if t.side in {"BUY", "SELL"} and t.qty > 0 and t.price > 0]
    real_fills = [t for t in events if t.is_real_fill]
    if not real_fills:
        return []
    real_fills.sort(key=lambda x: x.ts)
    events.sort(key=lambda x: x.ts)
    initial_position, first_price = _initial_shadow_position(real_fills)
    states = [
        ReducingCooldownReplayState(
            policy=policy,
            position=initial_position,
            cash=-initial_position * first_price,
            last_price=first_price,
        )
        for policy in _reducing_cooldown_policies()
    ]
    for fill in events:
        if not fill.is_real_fill:
            for state in states:
                _resync_shadow_state_to_trade(state, fill)
            continue
        signed_qty = _trade_signed_qty(fill)
        for state in states:
            state.last_price = fill.price
            state.ensure_active_for_initial_inventory(fill.ts)
            state.mark_active(fill.ts)
            before = state.position
            after = before + signed_qty
            exposure_increasing = abs(after) > abs(before) + EPS
            reducing = abs(after) < abs(before) - EPS
            if _reducing_cooldown_active(state, fill=fill, reducing=reducing):
                state.blocked_fills += 1
                state.blocked_reducing_fills += int(reducing)
                state.blocked_qty += fill.qty
                if fill.side == "BUY":
                    state.blocked_buy_fills += 1
                else:
                    state.blocked_sell_fills += 1
                if state.active is not None:
                    state.active.blocked_fills += 1
                    state.active.mark(fill.ts, state.position, state.equity)
                continue
            state.cash -= signed_qty * fill.price
            state.position = after
            state.accepted_fills += 1
            if exposure_increasing:
                state.exposure_increasing_fills += 1
            if reducing:
                state.reducing_fills += 1
                state.last_reducing_ts_by_side[fill.side] = fill.ts
            state.start_campaign_after_fill(fill.ts)
            if state.active is not None:
                state.active.fills += 1
                if fill.side == "BUY":
                    state.active.buy_fills += 1
                else:
                    state.active.sell_fills += 1
                if exposure_increasing:
                    state.active.exposure_increasing_fills += 1
                if reducing:
                    state.active.reducing_fills += 1
                state.active.mark(fill.ts, state.position, state.equity)
            state.close_if_flat(fill.ts)
    end_ts = real_fills[-1].ts
    for state in states:
        state.finish(end_ts)

    baseline = next(s for s in states if s.policy.name == "observed_sequence")
    baseline_equity = baseline.equity
    baseline_max_inv = max((c.max_abs_inventory for c in baseline.campaigns), default=0.0)
    baseline_worst_mae = min((c.min_equity_delta for c in baseline.campaigns), default=0.0)
    rows: list[dict[str, Any]] = []
    for state in states:
        max_inv = max((c.max_abs_inventory for c in state.campaigns), default=0.0)
        worst_mae = min((c.min_equity_delta for c in state.campaigns), default=0.0)
        max_age = max(((c.end_ts - c.start_ts) for c in state.campaigns), default=0.0)
        rows.append(
            {
                "policy": state.policy.name,
                "cooldown_s": f"{state.policy.cooldown_s:.3f}",
                "high_campaign_only": int(state.policy.high_campaign_only),
                "vol_scaled_proxy": int(state.policy.vol_scaled),
                "accepted_fills": state.accepted_fills,
                "blocked_fills": state.blocked_fills,
                "blocked_reducing_fills": state.blocked_reducing_fills,
                "blocked_buy_fills": state.blocked_buy_fills,
                "blocked_sell_fills": state.blocked_sell_fills,
                "blocked_qty": f"{state.blocked_qty:.6f}",
                "reducing_fills": state.reducing_fills,
                "exposure_increasing_fills": state.exposure_increasing_fills,
                "campaigns": len(state.campaigns),
                "closed_campaigns": sum(1 for c in state.campaigns if c.closed),
                "max_abs_inventory": f"{max_inv:.6f}",
                "max_campaign_age_s": f"{max_age:.1f}",
                "worst_campaign_mae_proxy": f"{worst_mae:.6f}",
                "final_position": f"{state.position:+.6f}",
                "pnl_proxy": f"{state.equity:.6f}",
                "delta_pnl_proxy_vs_observed": f"{state.equity - baseline_equity:.6f}",
                "delta_max_inventory_vs_observed": f"{max_inv - baseline_max_inv:.6f}",
                "delta_worst_mae_vs_observed": f"{worst_mae - baseline_worst_mae:.6f}",
            }
        )
    rows.sort(
        key=lambda r: (
            0 if r["policy"] == "observed_sequence" else 1,
            -safe_float(r, "delta_pnl_proxy_vs_observed"),
        )
    )
    return rows


def reducing_cooldown_replay_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    baseline = next((r for r in rows if r.get("policy") == "observed_sequence"), rows[0])
    candidates = [r for r in rows if r.get("policy") != "observed_sequence"]
    best_pnl = max(
        candidates, key=lambda r: safe_float(r, "delta_pnl_proxy_vs_observed"), default={}
    )
    best_mae = max(
        candidates, key=lambda r: safe_float(r, "delta_worst_mae_vs_observed"), default={}
    )
    return {
        "policies": len(rows),
        "observed_pnl_proxy": baseline.get("pnl_proxy", ""),
        "observed_worst_campaign_mae_proxy": baseline.get("worst_campaign_mae_proxy", ""),
        "best_pnl_policy": best_pnl.get("policy", ""),
        "best_pnl_delta_proxy": best_pnl.get("delta_pnl_proxy_vs_observed", ""),
        "best_mae_policy": best_mae.get("policy", ""),
        "best_mae_delta_proxy": best_mae.get("delta_worst_mae_vs_observed", ""),
    }


def campaign_policy_replay_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    baseline = next((r for r in rows if r.get("policy") == "observed_sequence"), rows[0])
    non_baseline = [r for r in rows if r.get("policy") != "observed_sequence"]
    best_pnl = max(
        non_baseline, key=lambda r: safe_float(r, "delta_pnl_proxy_vs_observed"), default={}
    )
    best_mae = max(
        non_baseline, key=lambda r: safe_float(r, "delta_worst_mae_vs_observed"), default={}
    )
    return {
        "policies": len(rows),
        "observed_pnl_proxy": baseline.get("pnl_proxy", ""),
        "observed_max_inventory": baseline.get("max_abs_inventory", ""),
        "observed_worst_campaign_mae_proxy": baseline.get("worst_campaign_mae_proxy", ""),
        "best_pnl_policy": best_pnl.get("policy", ""),
        "best_pnl_delta_proxy": best_pnl.get("delta_pnl_proxy_vs_observed", ""),
        "best_mae_policy": best_mae.get("policy", ""),
        "best_mae_delta_proxy": best_mae.get("delta_worst_mae_vs_observed", ""),
    }


def trades_from_rows(rows: list[dict[str, Any]]) -> list[TradeRow]:
    trades: list[TradeRow] = []
    for row in rows:
        trades.append(
            TradeRow(
                ts=float(row.get("_ts", 0.0) or 0.0),
                side=norm_side(row.get("side", "")),
                trade_type=str(row.get("trade_type", "")),
                qty=safe_float(row, "qty"),
                price=safe_float(row, "price"),
                position=safe_float(row, "position"),
                realized_pnl=safe_float(row, "realized_pnl"),
                unrealized_pnl=safe_float(row, "unrealized_pnl"),
            )
        )
    trades.sort(key=lambda x: x.ts)
    return trades


def build_campaigns(trades: list[TradeRow]) -> list[Campaign]:
    campaigns: list[Campaign] = []
    active: Campaign | None = None
    prev_position = 0.0
    campaign_id = 0
    last_realized = 0.0
    last_total = 0.0
    for row in trades:
        if active is None and abs(prev_position) < EPS and abs(row.position) >= EPS:
            campaign_id += 1
            active = Campaign(
                campaign_id=campaign_id,
                start_ts=row.ts,
                start_position=row.position,
                start_realized_pnl=row.realized_pnl,
                start_total_pnl=row.total_pnl,
                final_total_pnl=row.total_pnl,
                max_abs_inventory=abs(row.position),
            )
        if active is not None:
            active.update_fill(prev_position, row)
            active.update_path(row)
            active.final_realized_pnl = row.realized_pnl
            active.final_total_pnl = row.total_pnl
            if abs(row.position) < EPS:
                active.end_ts = row.ts
                active.final_position = row.position
                active.closed = True
                campaigns.append(active)
                active = None
        prev_position = row.position
        last_realized = row.realized_pnl
        last_total = row.total_pnl
    if active is not None:
        active.end_ts = trades[-1].ts if trades else 0.0
        active.final_position = prev_position
        active.final_realized_pnl = last_realized
        active.final_total_pnl = last_total
        campaigns.append(active)
    return campaigns


def campaign_rows(campaigns: list[Campaign]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for c in campaigns:
        duration_s = max(0.0, c.end_ts - c.start_ts)
        row: dict[str, Any] = {
            "campaign_id": c.campaign_id,
            "closed": int(c.closed),
            "start_utc": utc_text(c.start_ts),
            "end_utc": utc_text(c.end_ts),
            "duration_s": f"{duration_s:.1f}",
            "start_side": "LONG" if c.start_position > 0 else "SHORT",
            "final_position": f"{c.final_position:+.6f}",
            "start_session": session_stack(c.start_ts),
            "max_abs_inventory": f"{c.max_abs_inventory:.6f}",
            "realized_pnl_delta": f"{c.final_realized_pnl - c.start_realized_pnl:.4f}",
            "min_total_pnl_delta": f"{c.min_total_pnl_delta:.4f}",
            "max_total_pnl_delta": f"{c.max_total_pnl_delta:.4f}",
            "fills": c.fills,
            "buy_fills": c.buy_fills,
            "sell_fills": c.sell_fills,
            "exposure_increasing_fills": c.exposure_increasing_fills,
            "reducing_fills": c.reducing_fills,
            "sequence_head": "".join(c.fill_sequence[:80]),
            "shadow_reducing_only_blocks": c.shadow_reducing_only_blocks,
        }
        for window_s in EARLY_WINDOWS_S:
            row[f"early_{window_s // 60}m_min_pnl_delta"] = (
                f"{c.early_min_pnl_delta.get(window_s, 0.0):.4f}"
            )
        for threshold in INV_THRESHOLDS:
            key = str(threshold).replace("0.", "0p")
            row[f"shadow_inv_{key}_blocks"] = c.shadow_inv_blocks[threshold]
        for threshold_s in AGE_THRESHOLDS_S:
            row[f"shadow_age_{threshold_s // 60}m_blocks"] = c.shadow_age_blocks[threshold_s]
        rows.append(row)
    return rows


def _campaign_label(c: Campaign) -> str:
    final_pnl = c.final_total_pnl - c.start_total_pnl
    if not c.closed:
        return "open_risk"
    if final_pnl >= 0.0 and c.min_total_pnl_delta < -0.25:
        return "repaired_after_drawdown"
    if final_pnl >= 0.0:
        return "positive_flat"
    if c.min_total_pnl_delta <= -1.0 or c.max_abs_inventory >= 0.010:
        return "loss_tail"
    return "negative_flat"


def _campaign_label_bad(label: str) -> int:
    return int(label in {"negative_flat", "loss_tail", "open_risk"})


def _campaign_label_target(label: str) -> float:
    """Continuous terminal campaign risk target for score calibration."""
    return {
        "positive_flat": 0.0,
        "repaired_after_drawdown": 0.25,
        "negative_flat": 0.70,
        "loss_tail": 1.0,
        "open_risk": 1.0,
    }.get(label, math.nan)


def campaign_label_rows(campaigns: list[Campaign]) -> list[dict[str, Any]]:
    """Campaign-level alpha/risk labels for downstream order evidence.

    中文说明：这张表把 flat->nonzero->flat 的库存周期变成可训练/可审计
    label。它关注 campaign 终局和早期风险，而不是单笔 fill 的 20/30s
    markout，因此可用来检验“BUY-open 后自然修复”这类 order-level label
    看不到的现象。
    """
    rows: list[dict[str, Any]] = []
    for c in campaigns:
        duration_s = max(0.0, c.end_ts - c.start_ts)
        final_total_delta = c.final_total_pnl - c.start_total_pnl
        realized_delta = c.final_realized_pnl - c.start_realized_pnl
        early_5m = c.early_min_pnl_delta.get(5 * 60, 0.0)
        early_10m = c.early_min_pnl_delta.get(10 * 60, 0.0)
        early_20m = c.early_min_pnl_delta.get(20 * 60, 0.0)
        label = _campaign_label(c)
        rows.append(
            {
                "campaign_id": c.campaign_id,
                "start_day": utc_day(c.start_ts),
                "end_day": utc_day(c.end_ts),
                "closed": int(c.closed),
                "start_utc": utc_text(c.start_ts),
                "end_utc": utc_text(c.end_ts),
                "duration_s": f"{duration_s:.3f}",
                "start_side": "LONG" if c.start_position > 0 else "SHORT",
                "start_position": f"{c.start_position:+.6f}",
                "final_position": f"{c.final_position:+.6f}",
                "start_session": session_stack(c.start_ts),
                "max_abs_inventory": f"{c.max_abs_inventory:.6f}",
                "final_total_pnl_delta": f"{final_total_delta:.6f}",
                "realized_pnl_delta": f"{realized_delta:.6f}",
                "min_total_pnl_delta": f"{c.min_total_pnl_delta:.6f}",
                "max_total_pnl_delta": f"{c.max_total_pnl_delta:.6f}",
                "early_5m_min_pnl_delta": f"{early_5m:.6f}",
                "early_10m_min_pnl_delta": f"{early_10m:.6f}",
                "early_20m_min_pnl_delta": f"{early_20m:.6f}",
                "early_drawdown_20m": f"{abs(min(0.0, early_20m)):.6f}",
                "fills": c.fills,
                "buy_fills": c.buy_fills,
                "sell_fills": c.sell_fills,
                "exposure_increasing_fills": c.exposure_increasing_fills,
                "reducing_fills": c.reducing_fills,
                "campaign_repaired": int(label in {"repaired_after_drawdown", "positive_flat"}),
                "campaign_tail_loss": int(label == "loss_tail"),
                "campaign_bad": _campaign_label_bad(label),
                "campaign_outcome_risk_target": f"{_campaign_label_target(label):.6f}",
                "campaign_label": label,
            }
        )
    return rows


def campaign_label_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    closed = [r for r in rows if safe_int(r, "closed") == 1]
    labels = Counter(str(r.get("campaign_label", "")) for r in rows)
    return {
        "campaign_labels": len(rows),
        "closed_campaigns": len(closed),
        "open_campaigns": len(rows) - len(closed),
        "label_counts": dict(labels.most_common()),
        "total_final_pnl_delta": _safe_mean(
            [safe_float(r, "final_total_pnl_delta", math.nan) for r in rows]
        )
        * len(rows),
        "avg_final_pnl_delta": _safe_mean(
            [safe_float(r, "final_total_pnl_delta", math.nan) for r in rows]
        ),
        "avg_early_20m_drawdown": _safe_mean(
            [safe_float(r, "early_drawdown_20m", math.nan) for r in rows]
        ),
        "max_inventory": max(safe_float(r, "max_abs_inventory") for r in rows),
        "tail_loss_campaigns": sum(safe_int(r, "campaign_tail_loss") for r in rows),
        "repaired_campaigns": sum(safe_int(r, "campaign_repaired") for r in rows),
    }


def attach_campaign_labels_to_orders(
    rows: list[dict[str, Any]],
    labels: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attach campaign terminal labels to order-level rows.

    中文说明：order row 的 campaign state 是 quote-time 已知；这里附加的是
    事后 label，只能用于训练/校准/复盘，不能回流到 quote-time score。
    live campaign_id 通常全局递增，replay campaign_id 可能按日重置，所以
    先按 (day, campaign_id) 匹配，再用唯一 campaign_id 兜底。
    """
    by_day_id: dict[tuple[str, str], dict[str, Any]] = {}
    id_counts: Counter[str] = Counter()
    by_id: dict[str, dict[str, Any]] = {}
    for label in labels:
        cid = str(label.get("campaign_id", ""))
        if not cid:
            continue
        for day_key in ("start_day", "end_day"):
            day = str(label.get(day_key, ""))
            if day:
                by_day_id[(day, cid)] = label
        id_counts[cid] += 1
        by_id[cid] = label

    out: list[dict[str, Any]] = []
    for row in rows:
        enriched = dict(row)
        cid = str(row.get("campaign_id", ""))
        day = str(row.get("day", ""))
        label = by_day_id.get((day, cid))
        if label is None and cid and id_counts.get(cid, 0) == 1:
            label = by_id.get(cid)
        if label is not None:
            for key in (
                "campaign_label",
                "campaign_repaired",
                "campaign_tail_loss",
                "campaign_bad",
                "campaign_outcome_risk_target",
                "final_total_pnl_delta",
                "realized_pnl_delta",
                "min_total_pnl_delta",
                "max_total_pnl_delta",
                "early_5m_min_pnl_delta",
                "early_10m_min_pnl_delta",
                "early_20m_min_pnl_delta",
                "early_drawdown_20m",
            ):
                enriched[f"terminal_{key}"] = label.get(key, "")
            enriched["terminal_campaign_duration_s"] = label.get("duration_s", "")
            enriched["terminal_campaign_max_abs_inventory"] = label.get("max_abs_inventory", "")
        else:
            enriched["terminal_campaign_label"] = ""
        out.append(enriched)
    return out


def campaign_summary(campaigns: list[Campaign]) -> dict[str, Any]:
    closed = [c for c in campaigns if c.closed]
    return {
        "campaigns": len(campaigns),
        "closed_campaigns": len(closed),
        "open_campaigns": len(campaigns) - len(closed),
        "long_starts": sum(1 for c in campaigns if c.start_position > 0),
        "short_starts": sum(1 for c in campaigns if c.start_position < 0),
        "max_inventory": max((c.max_abs_inventory for c in campaigns), default=0.0),
        "closed_realized_pnl_delta": sum(
            c.final_realized_pnl - c.start_realized_pnl for c in closed
        ),
        "exposure_increasing_fills": sum(c.exposure_increasing_fills for c in campaigns),
        "reducing_fills": sum(c.reducing_fills for c in campaigns),
        "shadow_inv_006_blocks": sum(c.shadow_inv_blocks[0.006] for c in campaigns),
        "shadow_inv_008_blocks": sum(c.shadow_inv_blocks[0.008] for c in campaigns),
        "shadow_inv_010_blocks": sum(c.shadow_inv_blocks[0.010] for c in campaigns),
        "shadow_age_20m_blocks": sum(c.shadow_age_blocks[20 * 60] for c in campaigns),
        "shadow_age_40m_blocks": sum(c.shadow_age_blocks[40 * 60] for c in campaigns),
        "shadow_age_60m_blocks": sum(c.shadow_age_blocks[60 * 60] for c in campaigns),
        "shadow_reducing_only_blocks": sum(c.shadow_reducing_only_blocks for c in campaigns),
    }


def fill_summary(trades: list[TradeRow], order_rows: list[dict[str, Any]]) -> dict[str, Any]:
    real_fills = [r for r in trades if r.is_real_fill]
    if real_fills:
        start_ts = real_fills[0].ts
        end_ts = real_fills[-1].ts
    else:
        ts_values = [float(r.get("_ts", 0.0) or 0.0) for r in order_rows]
        start_ts = min(ts_values) if ts_values else 0.0
        end_ts = max(ts_values) if ts_values else start_ts
    hours = max((end_ts - start_ts) / 3600.0, 1e-9)
    event_counts = Counter(str(r.get("event_type", "")).lower() for r in order_rows)
    placed = sum(v for k, v in event_counts.items() if "place" in k or "new" in k)
    filled_events = sum(v for k, v in event_counts.items() if "fill" in k)
    age_values = [
        safe_float(r, "age_ms")
        for r in order_rows
        if "fill" in str(r.get("event_type", "")).lower()
    ]
    age_values = [x for x in age_values if x > 0]
    buy_qty = sum(r.qty for r in real_fills if r.side == "BUY")
    sell_qty = sum(r.qty for r in real_fills if r.side == "SELL")
    buy_notional = sum(r.qty * r.price for r in real_fills if r.side == "BUY")
    sell_notional = sum(r.qty * r.price for r in real_fills if r.side == "SELL")
    return {
        "fills": len(real_fills),
        "buy_fills": sum(1 for r in real_fills if r.side == "BUY"),
        "sell_fills": sum(1 for r in real_fills if r.side == "SELL"),
        "buy_fill_qty": buy_qty,
        "sell_fill_qty": sell_qty,
        "buy_fill_notional": buy_notional,
        "sell_fill_notional": sell_notional,
        "buy_avg_fill_price": buy_notional / buy_qty if buy_qty > 1e-12 else 0.0,
        "sell_avg_fill_price": sell_notional / sell_qty if sell_qty > 1e-12 else 0.0,
        "sync_adjust_rows": sum(1 for r in trades if not r.is_real_fill),
        "filled_events": filled_events,
        "placed_events": placed,
        "hours": hours,
        "fills_per_hour": len(real_fills) / hours,
        "placed_per_hour": placed / hours if placed else 0.0,
        "fill_per_placed": len(real_fills) / placed if placed else 0.0,
        "avg_fill_age_ms": sum(age_values) / len(age_values) if age_values else 0.0,
        "max_fill_age_ms": max(age_values) if age_values else 0.0,
        "event_counts": dict(event_counts.most_common(12)),
    }


def _day_from_row_ts(row: dict[str, Any]) -> str:
    ts = safe_float(row, "timestamp", safe_float(row, "_ts", math.nan))
    return utc_day(ts) if math.isfinite(ts) and ts > 0 else str(row.get("day", ""))


def live_order_daily_rows(
    *,
    order_rows: list[dict[str, Any]],
    quote_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Summarize live maker orders by UTC day.

    中文说明：这里用 order_outcomes.csv 的 `filled` 行作为 maker-only 成交源。
    `trades.csv` 可能包含人工 taker/干预成交，不适合直接做 live/replay
    fill-selection 和 VWAP 对齐。
    """
    by_day: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "placed": 0,
            "filled": 0,
            "canceled": 0,
            "reject_error": 0,
            "buy_fills": 0,
            "sell_fills": 0,
            "buy_qty": 0.0,
            "sell_qty": 0.0,
            "buy_notional": 0.0,
            "sell_notional": 0.0,
        }
    )
    for row in order_rows:
        day = _day_from_row_ts(row)
        if not day:
            continue
        event = str(row.get("event_type", "")).strip().lower()
        side = norm_side(row.get("side", ""))
        bucket = by_day[day]
        if event in {"placed", "filled", "canceled", "reject_error"}:
            bucket[event] += 1
        if event != "filled" or side not in {"BUY", "SELL"}:
            continue
        qty = safe_float(row, "filled_qty", safe_float(row, "quantity", 0.0))
        px = safe_float(row, "avg_fill_price", safe_float(row, "price", 0.0))
        if qty <= 0.0 or px <= 0.0:
            continue
        if side == "BUY":
            bucket["buy_fills"] += 1
            bucket["buy_qty"] += qty
            bucket["buy_notional"] += qty * px
        else:
            bucket["sell_fills"] += 1
            bucket["sell_qty"] += qty
            bucket["sell_notional"] += qty * px

    quote_by_day: dict[str, Counter[str]] = defaultdict(Counter)
    for row in quote_rows:
        day = _day_from_row_ts(row)
        if not day:
            continue
        action = str(row.get("action", "")).strip().lower() or "unknown"
        quote_by_day[day][action] += 1

    rows: list[dict[str, Any]] = []
    for day in sorted(set(by_day) | set(quote_by_day)):
        b = by_day.get(day, {})
        q = quote_by_day.get(day, Counter())
        quote_total = sum(q.values())
        buy_qty = float(b.get("buy_qty", 0.0))
        sell_qty = float(b.get("sell_qty", 0.0))
        buy_vwap = float(b.get("buy_notional", 0.0)) / buy_qty if buy_qty > 1e-12 else 0.0
        sell_vwap = float(b.get("sell_notional", 0.0)) / sell_qty if sell_qty > 1e-12 else 0.0
        rows.append(
            {
                "day": day,
                "live_placed_orders": int(b.get("placed", 0)),
                "live_filled_orders": int(b.get("filled", 0)),
                "live_canceled_orders": int(b.get("canceled", 0)),
                "live_reject_error_orders": int(b.get("reject_error", 0)),
                "live_buy_fills": int(b.get("buy_fills", 0)),
                "live_sell_fills": int(b.get("sell_fills", 0)),
                "live_buy_qty": f"{buy_qty:.6f}",
                "live_sell_qty": f"{sell_qty:.6f}",
                "live_buy_avg_fill_price": f"{buy_vwap:.6f}" if buy_vwap else "",
                "live_sell_avg_fill_price": f"{sell_vwap:.6f}" if sell_vwap else "",
                "live_side_vwap_edge": f"{sell_vwap - buy_vwap:.6f}"
                if buy_vwap and sell_vwap
                else "",
                "live_quote_decisions": quote_total,
                "live_action_place": q.get("place", 0),
                "live_action_replace": q.get("replace", 0),
                "live_action_keep": q.get("keep", 0),
                "live_action_pause": q.get("pause", 0),
                "live_action_pending_coalesce": q.get("pending_coalesce", 0),
                "live_action_cancel_first": q.get("cancel_first", 0),
                "live_action_place_replace_rate": (
                    f"{(q.get('place', 0) + q.get('replace', 0)) / quote_total:.6f}"
                    if quote_total
                    else ""
                ),
                "live_action_keep_rate": f"{q.get('keep', 0) / quote_total:.6f}"
                if quote_total
                else "",
                "live_action_pause_rate": f"{q.get('pause', 0) / quote_total:.6f}"
                if quote_total
                else "",
            }
        )
    return rows


def live_replay_baseline_compare_rows(
    *,
    live_daily_rows: list[dict[str, Any]],
    replay_daily_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Join live maker-order daily summary with replay daily baseline rows."""
    live_by_day = {str(r.get("day", "")): r for r in live_daily_rows}
    replay_by_day = {str(r.get("day", "")): r for r in replay_daily_rows}
    rows: list[dict[str, Any]] = []
    for day in sorted(set(live_by_day) | set(replay_by_day)):
        live = live_by_day.get(day, {})
        replay = replay_by_day.get(day, {})
        replay_place_replace = safe_int(replay, "decision_place_count") + safe_int(
            replay, "decision_replace_count"
        )
        live_placed = safe_int(live, "live_placed_orders")
        live_fills = safe_int(live, "live_filled_orders")
        replay_fills = safe_int(replay, "fills_total")
        live_buy_px = safe_float(live, "live_buy_avg_fill_price", math.nan)
        live_sell_px = safe_float(live, "live_sell_avg_fill_price", math.nan)
        replay_buy_px = safe_float(replay, "buy_avg_fill_price", math.nan)
        replay_sell_px = safe_float(replay, "sell_avg_fill_price", math.nan)
        live_edge = (
            live_sell_px - live_buy_px
            if math.isfinite(live_buy_px) and math.isfinite(live_sell_px)
            else math.nan
        )
        replay_edge = (
            replay_sell_px - replay_buy_px
            if math.isfinite(replay_buy_px) and math.isfinite(replay_sell_px)
            else math.nan
        )
        mid_px = (
            (live_buy_px + live_sell_px) / 2.0
            if math.isfinite(live_buy_px) and math.isfinite(live_sell_px)
            else math.nan
        )

        def _bps(diff: float) -> str:
            return (
                f"{diff / mid_px * 10000.0:.6f}"
                if math.isfinite(diff) and math.isfinite(mid_px) and mid_px > 0
                else ""
            )

        rows.append(
            {
                "day": day,
                "live_placed_orders": live_placed,
                "replay_place_replace_decisions": replay_place_replace,
                "placed_diff": live_placed - replay_place_replace,
                "placed_ratio_live_over_replay": f"{live_placed / replay_place_replace:.6f}"
                if replay_place_replace
                else "",
                "live_fills": live_fills,
                "replay_fills": replay_fills,
                "fills_diff": live_fills - replay_fills,
                "fills_ratio_live_over_replay": f"{live_fills / replay_fills:.6f}"
                if replay_fills
                else "",
                "live_buy_fills": safe_int(live, "live_buy_fills"),
                "replay_buy_fills": safe_int(replay, "fills_bid_buy"),
                "live_sell_fills": safe_int(live, "live_sell_fills"),
                "replay_sell_fills": safe_int(replay, "fills_ask_sell"),
                "live_buy_avg_fill_price": live.get("live_buy_avg_fill_price", ""),
                "replay_buy_avg_fill_price": f"{replay_buy_px:.6f}"
                if math.isfinite(replay_buy_px) and replay_buy_px > 0
                else "",
                "buy_vwap_diff_bps": _bps(live_buy_px - replay_buy_px),
                "live_sell_avg_fill_price": live.get("live_sell_avg_fill_price", ""),
                "replay_sell_avg_fill_price": f"{replay_sell_px:.6f}"
                if math.isfinite(replay_sell_px) and replay_sell_px > 0
                else "",
                "sell_vwap_diff_bps": _bps(live_sell_px - replay_sell_px),
                "live_side_vwap_edge": f"{live_edge:.6f}" if math.isfinite(live_edge) else "",
                "replay_side_vwap_edge": f"{replay_edge:.6f}" if math.isfinite(replay_edge) else "",
                "side_edge_diff_bps": _bps(live_edge - replay_edge),
                "live_action_place_replace_rate": live.get("live_action_place_replace_rate", ""),
                "replay_action_place_replace_rate": (
                    f"{(safe_int(replay, 'decision_place_count') + safe_int(replay, 'decision_replace_count')) / safe_int(replay, 'decision_total'):.6f}"
                    if safe_int(replay, "decision_total")
                    else ""
                ),
                "live_action_keep_rate": live.get("live_action_keep_rate", ""),
                "replay_action_keep_rate": (
                    f"{safe_float(replay, 'decision_keep_rate', math.nan):.6f}"
                    if math.isfinite(safe_float(replay, "decision_keep_rate", math.nan))
                    else ""
                ),
                "live_action_pause_rate": live.get("live_action_pause_rate", ""),
                "replay_action_pause_rate": (
                    f"{safe_float(replay, 'decision_pause_rate', math.nan):.6f}"
                    if math.isfinite(safe_float(replay, "decision_pause_rate", math.nan))
                    else ""
                ),
                "replay_initial_inventory": replay.get("initial_inventory", ""),
                "replay_initial_entry_price": replay.get("initial_entry_price", ""),
            }
        )
    return rows


def live_replay_baseline_compare_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    return {
        "days": len(rows),
        "live_fills_total": sum(safe_int(r, "live_fills") for r in rows),
        "replay_fills_total": sum(safe_int(r, "replay_fills") for r in rows),
        "live_placed_total": sum(safe_int(r, "live_placed_orders") for r in rows),
        "replay_place_replace_total": sum(
            safe_int(r, "replay_place_replace_decisions") for r in rows
        ),
        "avg_abs_buy_vwap_diff_bps": _safe_mean(
            [abs(safe_float(r, "buy_vwap_diff_bps", math.nan)) for r in rows]
        ),
        "avg_abs_sell_vwap_diff_bps": _safe_mean(
            [abs(safe_float(r, "sell_vwap_diff_bps", math.nan)) for r in rows]
        ),
        "avg_abs_side_edge_diff_bps": _safe_mean(
            [abs(safe_float(r, "side_edge_diff_bps", math.nan)) for r in rows]
        ),
    }


def quote_decision_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    allow = sum(1 for r in rows if safe_int(r, "allow_post") == 1)
    actions = Counter(str(r.get("action", "")) for r in rows)
    reasons = Counter(
        str(r.get("reason_text", "none")) for r in rows if safe_int(r, "allow_post") == 0
    )
    by_ts: dict[str, dict[str, float]] = defaultdict(dict)
    for row in rows:
        side = norm_side(row.get("side", ""))
        if side in {"BUY", "SELL"}:
            by_ts[str(row.get("timestamp", ""))][side] = safe_float(row, "final_price")
    spreads = [
        v["SELL"] - v["BUY"]
        for v in by_ts.values()
        if v.get("SELL", 0.0) > 0 and v.get("BUY", 0.0) > 0
    ]
    spreads.sort()

    def pct(p: float) -> float:
        if not spreads:
            return 0.0
        idx = min(len(spreads) - 1, max(0, int(round((len(spreads) - 1) * p))))
        return spreads[idx]

    return {
        "decision_rows": len(rows),
        "allow_post_rate": allow / len(rows),
        "blocked_rate": 1.0 - allow / len(rows),
        "actions": dict(actions.most_common(12)),
        "top_block_reasons": dict(reasons.most_common(12)),
        "pair_spread_count": len(spreads),
        "pair_spread_p50": pct(0.50),
        "pair_spread_p90": pct(0.90),
        "pair_spread_lt_100_rate": sum(1 for x in spreads if x < 100.0) / max(len(spreads), 1),
    }


def inventory_shadow_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    last = rows[-1]
    keys = [
        "bid_block_if_inv_006",
        "ask_block_if_inv_006",
        "bid_block_if_inv_008",
        "ask_block_if_inv_008",
        "bid_block_if_inv_010",
        "ask_block_if_inv_010",
        "bid_block_if_age_20m",
        "ask_block_if_age_20m",
        "bid_block_if_age_40m",
        "ask_block_if_age_40m",
        "bid_block_if_age_60m",
        "ask_block_if_age_60m",
        "bid_block_if_reducing_only",
        "ask_block_if_reducing_only",
    ]
    out: dict[str, Any] = {
        "shadow_rows": len(rows),
        "last_q": safe_float(last, "q"),
        "last_campaign_active": safe_int(last, "active"),
        "last_campaign_age_s": safe_float(last, "age_s"),
        "last_campaign_max_abs_qty": safe_float(last, "max_abs_qty"),
        "last_campaign_pnl": safe_float(last, "total_pnl"),
        "last_campaign_mae": safe_float(last, "adverse_excursion"),
        "last_exposure_increasing_fills": safe_int(last, "exposure_increasing_fills"),
        "last_reducing_fills": safe_int(last, "reducing_fills"),
    }
    for key in keys:
        out[key] = sum(safe_int(r, key) for r in rows)
    return out


def _clip01(value: float) -> float:
    if not math.isfinite(value):
        return 0.0
    return max(0.0, min(1.0, value))


def _row_ts(row: dict[str, Any]) -> float:
    return float(row.get("_ts", 0.0) or 0.0)


def _event_type(row: dict[str, Any]) -> str:
    return str(row.get("event_type", "")).lower()


def _is_placed_event(row: dict[str, Any]) -> bool:
    event = _event_type(row)
    return event == "placed" or event == "new" or event.endswith("_new")


def _is_fill_event(row: dict[str, Any]) -> bool:
    event = _event_type(row)
    return "fill" in event and not _is_placed_event(row)


def _nearest_row(
    rows: list[dict[str, Any]],
    ts_values: list[float],
    ts: float,
    *,
    max_abs_lag_s: float,
    side: str = "",
) -> dict[str, Any] | None:
    if not rows or not ts_values or ts <= 0.0:
        return None
    idx = bisect.bisect_left(ts_values, ts)
    candidates: list[dict[str, Any]] = []
    if idx < len(rows):
        candidates.append(rows[idx])
    if idx > 0:
        candidates.append(rows[idx - 1])
    if side:
        candidates = [r for r in candidates if norm_side(r.get("side", "")) == side]
    if not candidates:
        return None
    best = min(candidates, key=lambda r: abs(_row_ts(r) - ts))
    if abs(_row_ts(best) - ts) <= max_abs_lag_s:
        return best
    return None


def _previous_row(
    rows: list[dict[str, Any]],
    ts_values: list[float],
    ts: float,
    *,
    max_lag_s: float,
) -> dict[str, Any] | None:
    if not rows or not ts_values or ts <= 0.0:
        return None
    idx = bisect.bisect_right(ts_values, ts) - 1
    if idx < 0:
        return None
    row = rows[idx]
    if ts - _row_ts(row) <= max_lag_s:
        return row
    return None


def _future_mid(mid_series: list[tuple[float, float]], mid_ts: list[float], ts: float) -> float:
    if not mid_series:
        return math.nan
    idx = bisect.bisect_left(mid_ts, ts)
    if idx >= len(mid_series):
        return math.nan
    return mid_series[idx][1]


def _past_mid_window_stats(
    mid_series: list[tuple[float, float]],
    mid_ts: list[float],
    *,
    ts: float,
    mid: float,
    window_s: int,
) -> dict[str, float]:
    """Quote-time path stats using only mid observations at or before ``ts``."""
    if not mid_series or not mid_ts or ts <= 0.0 or mid <= 0.0:
        return {"range_bps": math.nan, "rv_bps": math.nan, "ret_bps": math.nan, "count": 0.0}
    right = bisect.bisect_right(mid_ts, ts)
    left = bisect.bisect_left(mid_ts, ts - float(window_s), 0, right)
    values = [m for _, m in mid_series[left:right] if m > 0.0 and math.isfinite(m)]
    if not values or abs(values[-1] - mid) > 1e-12:
        values.append(mid)
    if not values:
        return {"range_bps": math.nan, "rv_bps": math.nan, "ret_bps": math.nan, "count": 0.0}
    hi = max(values)
    lo = min(values)
    range_bps = (hi - lo) / mid * 10_000.0
    ret_bps = math.log(mid / values[0]) * 10_000.0 if values[0] > 0.0 else math.nan
    ssq = 0.0
    n_ret = 0
    for prev, cur in zip(values, values[1:]):
        if prev > 0.0 and cur > 0.0:
            r = math.log(cur / prev)
            ssq += r * r
            n_ret += 1
    rv_bps = math.sqrt(ssq) * 10_000.0 if n_ret > 0 else 0.0
    return {
        "range_bps": range_bps,
        "rv_bps": rv_bps,
        "ret_bps": ret_bps,
        "count": float(len(values)),
    }


def _micro_macro_regime(micro_macro_ratio: float, trend_efficiency: float) -> str:
    if not (math.isfinite(micro_macro_ratio) and math.isfinite(trend_efficiency)):
        return "missing"
    micro_high = micro_macro_ratio >= 0.30
    micro_low = micro_macro_ratio < 0.20
    trend_high = trend_efficiency >= 0.55
    trend_low = trend_efficiency < 0.35
    if micro_high and trend_low:
        return "local_noise_macro_flat"
    if micro_low and trend_high:
        return "macro_trend_dominant"
    if micro_high and trend_high:
        return "shock_transition"
    if micro_low and trend_low:
        return "dead_water"
    return "mixed"


def _order_path_features(
    *,
    ts: float,
    side: str,
    mid: float,
    quote_distance_bps: float,
    mid_series: list[tuple[float, float]],
    mid_ts: list[float],
) -> dict[str, Any]:
    """Build micro/macro path features for a quote/placed order.

    中文说明：这些字段回答“短窗波动是否足以触达报价”和“大窗趋势是否
    会把库存带走”。它们只使用 quote-time 可见 mid path，不能用 fill 后
    markout 或 campaign outcome。
    """
    stats = {
        window_s: _past_mid_window_stats(mid_series, mid_ts, ts=ts, mid=mid, window_s=window_s)
        for window_s in MICRO_MACRO_WINDOWS_S
    }
    out: dict[str, Any] = {}
    for window_s in MICRO_MACRO_WINDOWS_S:
        s = stats[window_s]
        out[f"range_{window_s}s_bps"] = s["range_bps"]
        out[f"rv_{window_s}s_bps"] = s["rv_bps"]
        out[f"ret_{window_s}s_bps"] = s["ret_bps"]
        out[f"path_count_{window_s}s"] = s["count"]

    def ratio(num: float, den: float) -> float:
        if not (math.isfinite(num) and math.isfinite(den)):
            return math.nan
        return num / max(abs(den), PATH_RATIO_EPS_BPS)

    range_5 = stats[5]["range_bps"]
    range_10 = stats[10]["range_bps"]
    range_20 = stats[20]["range_bps"]
    range_60 = stats[60]["range_bps"]
    range_300 = stats[300]["range_bps"]
    rv_10 = stats[10]["rv_bps"]
    rv_300 = stats[300]["rv_bps"]
    ret_60 = stats[60]["ret_bps"]
    ret_300 = stats[300]["ret_bps"]
    trend_eff_60 = ratio(abs(ret_60), range_60)
    trend_eff_300 = ratio(abs(ret_300), range_300)
    micro_macro_range = ratio(range_10, range_300)
    micro_macro_vol = ratio(rv_10, rv_300)

    out["quote_distance_micro_5s"] = ratio(quote_distance_bps, range_5)
    out["quote_distance_micro_10s"] = ratio(quote_distance_bps, range_10)
    out["quote_distance_micro"] = out["quote_distance_micro_10s"]
    out["micro_macro_range_ratio"] = micro_macro_range
    out["micro_macro_vol_ratio"] = micro_macro_vol
    out["inventory_horizon_range_ratio"] = ratio(range_20, range_300)
    out["trend_efficiency_60s"] = trend_eff_60
    out["trend_efficiency_300s"] = trend_eff_300
    if side == "BUY":
        out["side_trend_adverse_60s_bps"] = max(0.0, -ret_60) if math.isfinite(ret_60) else math.nan
        out["side_trend_adverse_300s_bps"] = (
            max(0.0, -ret_300) if math.isfinite(ret_300) else math.nan
        )
    elif side == "SELL":
        out["side_trend_adverse_60s_bps"] = max(0.0, ret_60) if math.isfinite(ret_60) else math.nan
        out["side_trend_adverse_300s_bps"] = (
            max(0.0, ret_300) if math.isfinite(ret_300) else math.nan
        )
    else:
        out["side_trend_adverse_60s_bps"] = math.nan
        out["side_trend_adverse_300s_bps"] = math.nan
    out["micro_macro_regime"] = _micro_macro_regime(micro_macro_range, trend_eff_300)
    return out


def _fmt_path_feature(value: Any) -> Any:
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return f"{float(value):.6f}"
    return ""


def _maker_signed_markout_bps(side: str, fill_px: float, future_mid: float) -> float:
    if not (fill_px > 0.0 and future_mid > 0.0):
        return math.nan
    if side == "BUY":
        return (future_mid - fill_px) / fill_px * 10_000.0
    if side == "SELL":
        return (fill_px - future_mid) / fill_px * 10_000.0
    return math.nan


def _reason_has(text: str, *needles: str) -> int:
    lower = text.lower()
    return int(any(n in lower for n in needles))


def _truthy_score(value: Any, *, missing: float = 0.5) -> float:
    text = str(value if value is not None else "").strip().lower()
    if not text:
        return missing
    if text in {"1", "true", "yes", "y"}:
        return 1.0
    if text in {"0", "false", "no", "n"}:
        return 0.0
    return missing


def _score_bucket(value: float) -> str:
    if not math.isfinite(value):
        return "missing"
    if value < 0.33:
        return "low"
    if value < 0.66:
        return "mid"
    return "high"


ORDER_LEVEL_SCORE_COLS = (
    "micro_fill_reach_score",
    "fill_probability_score",
    "fill_quality_score",
    "toxic_risk_score",
    "campaign_risk_score",
    "campaign_outcome_risk_score",
    "resiliency_score",
    "micro_reversion_score",
    "trend_inventory_risk_score",
    "reducing_burst_risk_score",
    "lifecycle_risk_score",
    "post_fill_spot_pending_risk_score",
    "post_fill_campaign_outcome_risk_score",
)

RANK_QUANTILE_SCORE_COLS = {
    "toxic_risk_score",
    "post_fill_spot_pending_risk_score",
    "post_fill_campaign_outcome_risk_score",
}
MICRO_MACRO_WINDOWS_S = (5, 10, 20, 60, 300)
PATH_RATIO_EPS_BPS = 0.05


def _toxic_quantile_bucket_for_rank(rank: int, n: int) -> str:
    if n <= 0:
        return "missing"
    frac = rank / n
    if frac < 0.70:
        return "q000_070"
    if frac < 0.85:
        return "q070_085"
    if frac < 0.95:
        return "q085_095"
    return "q095_100"


def _rank_quantile_bucket_map(
    rows: list[dict[str, Any]],
    *,
    score: str,
    group_key,
) -> dict[int, str]:
    """Assign rank buckets within each group, robust to tied score values."""
    grouped: dict[tuple[Any, ...], list[tuple[float, int, int]]] = defaultdict(list)
    out: dict[int, str] = {}
    for idx, row in enumerate(rows):
        value = safe_float(row, score, math.nan)
        rid = id(row)
        if not math.isfinite(value):
            out[rid] = "missing"
            continue
        grouped[group_key(row)].append((value, idx, rid))
    for items in grouped.values():
        items.sort(key=lambda x: (x[0], x[1]))
        n = len(items)
        for rank, (_, _, rid) in enumerate(items):
            out[rid] = _toxic_quantile_bucket_for_rank(rank, n)
    return out


def _score_bucket_value(
    row: dict[str, Any],
    score: str,
    *,
    toxic_bucket_by_row: dict[int, str] | None = None,
) -> str:
    if score in RANK_QUANTILE_SCORE_COLS and toxic_bucket_by_row is not None:
        return toxic_bucket_by_row.get(id(row), "missing")
    return _score_bucket(safe_float(row, score, math.nan))


def _safe_mean(values: list[float]) -> float:
    values = [v for v in values if math.isfinite(v)]
    return sum(values) / len(values) if values else 0.0


def _filled_qty(row: dict[str, Any]) -> float:
    """Return executed base quantity without treating order size as a partial fill."""
    for key in ("filled_qty", "fill_qty"):
        value = safe_float(row, key, math.nan)
        if math.isfinite(value) and value > 0.0:
            return value
    if safe_int(row, "filled") == 1:
        value = safe_float(row, "quantity", math.nan)
        if math.isfinite(value) and value > 0.0:
            return value
        # Legacy audit tables omitted quantity because every fill was one lot.
        return 1.0
    return 0.0


def _fill_qty_weighted_mean(rows: list[dict[str, Any]], key: str) -> float:
    """Quantity-weight a per-base-unit fill metric such as maker markout bps."""
    weighted_sum = 0.0
    total_qty = 0.0
    for row in rows:
        value = safe_float(row, key, math.nan)
        qty = _filled_qty(row)
        if math.isfinite(value) and qty > 0.0:
            weighted_sum += value * qty
            total_qty += qty
    return weighted_sum / total_qty if total_qty > 0.0 else 0.0


def _unique_terminal_campaign_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return one terminal-label row per stable campaign identity.

    Terminal outcomes are copied onto every order in a campaign. Directly
    averaging those rows gives long or frequently requoted campaigns extra
    weight. Rows without a campaign id cannot be deduplicated safely and remain
    individual observations.
    """
    unique: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        if not str(row.get("terminal_campaign_label", "")):
            continue
        campaign_id = str(row.get("campaign_id", "")).strip()
        if not campaign_id:
            unique.append(row)
            continue
        day = str(row.get("day", "")).strip()
        if not day:
            day = str(row.get("utc", ""))[:10]
        key = (
            day,
            str(row.get("arm", "")).strip(),
            campaign_id,
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def _order_inventory_reducing_score_context(row: dict[str, Any], side: str) -> bool:
    """Return whether the quote would reduce current inventory.

    中文说明：order-level 表有些来自 live log，有些来自 replay trace。
    新的 reducing-burst / lifecycle 分数只应该作用在减仓报价上，所以这里
    优先读统一字段，缺失时再用 submit 前库存推断。
    """
    explicit = str(row.get("order_inventory_reducing", "")).strip().lower()
    if explicit in {"1", "true", "yes", "y"}:
        return True
    if explicit in {"0", "false", "no", "n"}:
        return False
    q_before = safe_float(
        row,
        "q_before",
        safe_float(row, "inventory_at_submit", safe_float(row, "inventory", 0.0)),
    )
    if side == "BUY":
        return q_before < -EPS
    if side == "SELL":
        return q_before > EPS
    return False


def _order_scores(row: dict[str, Any]) -> dict[str, float | str]:
    """Small explainable quote-time scores, not a trained alpha model.

    中文说明：这些分数只是把连续状态压成便于排序/画图的轴。它们不作为
    live 参数，也不替代后续校准；真正用途是把 bucket evidence 重新组织成
    “报价决策函数”的输入。
    """
    quote_dist_bps = safe_float(row, "quote_distance_bps")
    near_depth = safe_float(row, "near_depth_total")
    refresh = safe_float(row, "l2_book_refresh_ratio")
    cancel = safe_float(row, "l2_book_cancel_ratio")
    flip = safe_float(row, "l2_quote_flip_rate")
    toxicity = safe_float(row, "toxicity", 0.5)
    markout_ema = safe_float(row, "markout_ema")
    side_quote_fill_prob = safe_float(row, "side_quote_fill_prob", math.nan)
    side_quote_markout = safe_float(row, "side_quote_fill_markout_30s", math.nan)
    queue_init = safe_float(row, "queue_init", math.nan)
    queue_rank = safe_float(row, "queue_local_rank", safe_float(row, "sell_resil_rank", math.nan))
    queue_mo_mult = safe_float(row, "queue_mo_mult", math.nan)
    queue_deplete_mult = safe_float(row, "queue_deplete_mult", math.nan)
    queue_regime_mult = safe_float(row, "queue_regime_mult", math.nan)
    fill_eligible_score = _truthy_score(row.get("fill_eligible", ""), missing=0.5)
    ttl_budget_ms = safe_float(row, "ttl_budget_ms", math.nan)
    exact_l2_spread_bps = safe_float(row, "exact_l2_spread_bps", math.nan)
    flow_decel = safe_float(row, "sell_resil_flow_decel", math.nan)
    refill_edge = safe_float(row, "sell_resil_refill_edge", math.nan)
    ref_adv = safe_float(row, "sell_resil_ref_adv", math.nan)
    spot_adv = safe_float(row, "sell_resil_spot_adv", math.nan)
    exposure_increasing = safe_int(row, "order_exposure_increasing")
    shadow_inv006 = safe_int(row, "shadow_block_inv006")
    shadow_age60m = safe_int(row, "shadow_block_age60m")
    campaign_age_s = safe_float(row, "campaign_age_s")
    campaign_max_abs = safe_float(row, "campaign_max_abs_qty")
    campaign_mae = abs(min(0.0, safe_float(row, "campaign_adverse_excursion")))
    reason = str(row.get("reason_text", ""))
    side = norm_side(row.get("side", ""))
    quote_distance_micro = safe_float(row, "quote_distance_micro", math.nan)
    micro_macro_range = safe_float(row, "micro_macro_range_ratio", math.nan)
    micro_macro_vol = safe_float(row, "micro_macro_vol_ratio", math.nan)
    trend_eff_60 = safe_float(row, "trend_efficiency_60s", math.nan)
    trend_eff_300 = safe_float(row, "trend_efficiency_300s", math.nan)
    side_trend_adv_60 = safe_float(row, "side_trend_adverse_60s_bps", math.nan)
    side_trend_adv_300 = safe_float(row, "side_trend_adverse_300s_bps", math.nan)
    reducing_burst_count_8s = safe_float(row, "reducing_burst_count_8s", 0.0)
    reducing_burst_qty_8s = safe_float(row, "reducing_burst_qty_8s", 0.0)
    filled = safe_int(row, "filled") == 1

    def _fill_spot_pending_scores() -> tuple[float, float, int]:
        """Return fill-time spot residual risk/favorable scores.

        中文说明：这些字段只在成交后可见，所以只能用于 Stage 1
        campaign risk / post-fill calibration，不能用于 quote-time re-center。
        side_favorable_bps > 0 表示对该 side 有利；< 0 表示成交时的
        spot residual 对该 side 不利。
        """
        weights = ((1000, 0.50), (3000, 0.30), (5000, 0.20))
        risk_sum = 0.0
        fav_sum = 0.0
        weight_sum = 0.0
        support = 0
        for prefix in ("fill_exec_spot_pending", "fill_ref_spot_pending"):
            for horizon_ms, weight in weights:
                value = safe_float(row, f"{prefix}_{horizon_ms}ms_side_favorable_bps", math.nan)
                if not math.isfinite(value):
                    continue
                risk_sum += weight * _clip01(max(0.0, -value) / 2.0)
                fav_sum += weight * _clip01(max(0.0, value) / 2.0)
                weight_sum += weight
                support += 1
        if weight_sum <= 0.0:
            return math.nan, math.nan, support
        return _clip01(risk_sum / weight_sum), _clip01(fav_sum / weight_sum), support

    depth_score = _clip01(near_depth / 8.0)
    quote_nearness_abs = _clip01(1.0 - quote_dist_bps / 14.0)
    # 中文说明：quote_distance_micro = quote distance / 10s range。
    # 线性 1 - x/1.5 会把 high 桶压得过窄，retained-all 日度支持不足。
    # 这里用平滑 reach score：1 表示落在短窗内，>1.5-2 倍短窗仍有
    # 可比较 support；它只用于成交概率 evidence，不代表“好成交”。
    quote_nearness_micro = (
        1.0 / (1.0 + max(0.0, quote_distance_micro) / 3.0)
        if math.isfinite(quote_distance_micro)
        else math.nan
    )
    micro_fill_reach = (
        quote_nearness_micro if math.isfinite(quote_nearness_micro) else quote_nearness_abs
    )
    quote_nearness = (
        _clip01(0.55 * quote_nearness_abs + 0.45 * quote_nearness_micro)
        if math.isfinite(quote_nearness_micro)
        else quote_nearness_abs
    )
    refresh_edge = _clip01((refresh - cancel + 0.25) / 0.75)
    queue_front_score = _clip01(1.0 - queue_rank) if math.isfinite(queue_rank) else 0.50
    queue_capacity = (
        max(0.50, near_depth * 0.35) if math.isfinite(near_depth) and near_depth > 0 else 1.0
    )
    queue_ahead_score = (
        _clip01(1.0 - queue_init / queue_capacity) if math.isfinite(queue_init) else 0.50
    )
    queue_flow_score = _clip01(
        (max(queue_mo_mult if math.isfinite(queue_mo_mult) else 1.0, 0.0) - 0.50) / 1.50
    )
    queue_deplete_score = _clip01(
        (max(queue_deplete_mult if math.isfinite(queue_deplete_mult) else 1.0, 0.0) - 0.50) / 1.50
    )
    queue_regime_score = _clip01(
        (max(queue_regime_mult if math.isfinite(queue_regime_mult) else 1.0, 0.0) - 0.50) / 1.50
    )
    ttl_budget_score = (
        _clip01(ttl_budget_ms / 120_000.0)
        if math.isfinite(ttl_budget_ms) and ttl_budget_ms > 0
        else 1.0
    )
    l2_spread_score = (
        _clip01(1.0 - max(0.0, exact_l2_spread_bps - 1.5) / 8.0)
        if math.isfinite(exact_l2_spread_bps)
        else 0.50
    )
    flow_decel_score = _clip01(flow_decel / 0.50) if math.isfinite(flow_decel) else 0.50
    refill_score = (
        _clip01((refill_edge + 0.02) / 0.10) if math.isfinite(refill_edge) else refresh_edge
    )
    xmarket_adverse_score = max(
        _clip01(abs(ref_adv) / 1.0) if math.isfinite(ref_adv) else 0.0,
        _clip01(abs(spot_adv) / 1.0) if math.isfinite(spot_adv) else 0.0,
    )
    trend_adverse_score = max(
        _clip01(side_trend_adv_60 / 4.0) if math.isfinite(side_trend_adv_60) else 0.0,
        _clip01(side_trend_adv_300 / 12.0) if math.isfinite(side_trend_adv_300) else 0.0,
    )
    trend_eff_score = max(
        _clip01((trend_eff_60 - 0.35) / 0.45) if math.isfinite(trend_eff_60) else 0.0,
        _clip01((trend_eff_300 - 0.35) / 0.45) if math.isfinite(trend_eff_300) else 0.0,
    )
    macro_trend_dominance = (
        _clip01((0.30 - micro_macro_range) / 0.30) * _clip01((trend_eff_300 - 0.35) / 0.45)
        if math.isfinite(micro_macro_range) and math.isfinite(trend_eff_300)
        else 0.0
    )
    micro_noise_score = (
        _clip01((micro_macro_range - 0.15) / 0.35) * _clip01(1.0 - trend_eff_300 / 0.55)
        if math.isfinite(micro_macro_range) and math.isfinite(trend_eff_300)
        else 0.0
    )
    micro_vol_score = (
        _clip01((micro_macro_vol - 0.15) / 0.35) if math.isfinite(micro_macro_vol) else 0.0
    )
    trend_inventory_risk = _clip01(
        0.45 * trend_adverse_score
        + 0.25 * trend_eff_score
        + 0.15 * macro_trend_dominance
        + 0.15 * xmarket_adverse_score
    )
    micro_reversion_raw = _clip01(
        0.35 * micro_noise_score
        + 0.20 * micro_vol_score
        + 0.20 * refill_score
        + 0.15 * flow_decel_score
        + 0.10 * (1.0 - _clip01(xmarket_adverse_score))
    )

    # The producer already emits maker-signed markout for both sides:
    # positive is favorable and negative is adverse.  Do not flip SELL again.
    side_markout_risk = -markout_ema if side in {"BUY", "SELL"} else 0.0
    ema_risk = _clip01(side_markout_risk / 2.0)
    toxicity_excess = _clip01((toxicity - 0.55) / 0.15)
    # 中文说明：fill-probability 只使用 quote-time 可见状态。实际
    # lifetime_ms / filled 与 outcome 相关，只能在 calibration 表里做 target，
    # 不能进入这里；否则会把“事后活了多久”泄漏进成交概率分数。
    heuristic_fill_probability = _clip01(
        0.22 * quote_nearness
        + 0.18 * queue_ahead_score
        + 0.12 * queue_front_score
        + 0.12 * fill_eligible_score
        + 0.10 * queue_flow_score
        + 0.08 * queue_deplete_score
        + 0.06 * queue_regime_score
        + 0.05 * ttl_budget_score
        + 0.04 * depth_score
        + 0.03 * l2_spread_score
    )
    thin_depth = _reason_has(reason, "thin")
    adverse_guard = _reason_has(reason, "adverse")
    stale_or_burst = _reason_has(reason, "stale", "burst")
    markout_guard = _reason_has(reason, "markout") if ema_risk > 0.10 else 0
    low_depth_risk = _clip01(1.0 - near_depth / 1.0)
    deep_depth_risk = _clip01((near_depth - 3.0) / 2.0)
    depth_risk = max(low_depth_risk, deep_depth_risk) if side == "SELL" else low_depth_risk
    toxic_risk = _clip01(
        0.45 * ema_risk
        + 0.15 * toxicity_excess
        + 0.20 * thin_depth
        + 0.10 * adverse_guard
        + 0.05 * markout_guard
        + 0.05 * depth_risk
        + 0.05 * stale_or_burst
        + 0.10 * trend_inventory_risk
    )

    campaign_risk = _clip01(
        0.45 * _clip01(campaign_max_abs / 0.006)
        + 0.35 * _clip01(campaign_age_s / 3600.0)
        + 0.20 * _clip01(campaign_mae / 1.0)
    )

    # 中文说明：fill_probability 只回答“是否更可能成交”。如果 quote EV
    # fill-prob head 可用，就和启发式分数混合；否则完全退回 quote-time
    # 启发式。它不能再被解释为“好成交”。
    model_fill_probability = (
        _clip01(side_quote_fill_prob / 0.05)
        if math.isfinite(side_quote_fill_prob) and side_quote_fill_prob > 0.0
        else math.nan
    )
    fill_probability = (
        _clip01(0.60 * model_fill_probability + 0.40 * heuristic_fill_probability)
        if math.isfinite(model_fill_probability)
        else heuristic_fill_probability
    )

    # 中文说明：fill_quality 只回答“成交后是否更不容易 toxic”。若 quote EV
    # markout head 可用，优先让它表达 30s 质量；否则用低 toxic、非 adverse
    # EMA、适中深度、安静 book、较低 campaign risk 组合。
    side_adverse_ema = -markout_ema if side in {"BUY", "SELL"} else 0.0
    non_adverse_ema_score = _clip01(1.0 - max(0.0, side_adverse_ema) / 1.0)
    mild_favorable_score = _clip01((-side_adverse_ema + 0.25) / 1.25)
    medium_depth_score = _clip01(1.0 - abs(near_depth - 2.0) / 2.5)
    quiet_book_score = _clip01(1.0 - flip / 0.25)
    predicted_markout_quality = (
        _clip01((side_quote_markout + 15.0) / 35.0)
        if math.isfinite(side_quote_markout) and abs(side_quote_markout) > 1e-9
        else math.nan
    )
    risk_gate = 1.0 - _clip01(toxic_risk / 0.50)
    no_guard_score = 1.0 - _clip01(
        0.90 * thin_depth + 0.70 * adverse_guard + 0.70 * markout_guard + 0.40 * stale_or_burst
    )
    distance_buffer_score = _clip01((quote_dist_bps - 2.0) / 8.0)
    low_campaign_score = 1.0 - campaign_risk
    low_toxic_score = 1.0 - toxic_risk
    heuristic_fill_quality = _clip01(
        0.30 * low_toxic_score
        + 0.20 * non_adverse_ema_score
        + 0.15 * low_campaign_score
        + 0.10 * medium_depth_score
        + 0.10 * quiet_book_score
        + 0.05 * distance_buffer_score
        + 0.05 * micro_reversion_raw
        + 0.05 * (1.0 - trend_inventory_risk)
    )
    fill_quality = (
        _clip01(0.55 * predicted_markout_quality + 0.45 * heuristic_fill_quality)
        if math.isfinite(predicted_markout_quality)
        else heuristic_fill_quality
    )

    # 中文说明：campaign_outcome_risk_score 是 terminal campaign 的监督轴，
    # 但分数本身只能用 quote-time 可见状态。它不是“BTC*s 越小越好”的
    # 库存惩罚，而是估计“这笔报价所在 campaign 是否更容易走向
    # negative/loss/open risk”。因此大库存/长 campaign 只是背景风险；
    # 只有同时出现 trend adverse、fill quality 低、local repair 弱、xmarket
    # adverse，尤其还在继续加仓时，才应强烈升高分数。
    repair_weak_score = _clip01(
        0.45 * (1.0 - refill_score)
        + 0.25 * _clip01((cancel - refresh + 0.02) / 0.12)
        + 0.20 * (1.0 - micro_reversion_raw)
        + 0.10 * _clip01(flip / 0.25)
    )
    quality_gap_score = 1.0 - fill_quality
    campaign_pressure = _clip01(0.60 * campaign_risk + 0.20 * shadow_inv006 + 0.20 * shadow_age60m)
    lifecycle_intervention_pressure = (
        _clip01(
            0.35 * campaign_pressure
            + 0.25 * trend_inventory_risk
            + 0.20 * repair_weak_score
            + 0.20 * quality_gap_score
        )
        if exposure_increasing
        else 0.0
    )
    raw_campaign_outcome_risk = _clip01(
        0.22 * campaign_risk
        + 0.16 * toxic_risk
        + 0.16 * trend_inventory_risk
        + 0.16 * repair_weak_score
        + 0.12 * quality_gap_score
        + 0.10 * lifecycle_intervention_pressure
        + 0.05 * xmarket_adverse_score
        + 0.03 * stale_or_burst
    )
    # 中文说明：上面的 raw score 保留排序逻辑，但 retained/smoke panel 中
    # 会集中在 0.1-0.55，导致 high bucket 没有支持。这里做固定仿射校准，
    # 只展开 quote-time risk range，不使用 terminal label。
    campaign_outcome_risk = _clip01((raw_campaign_outcome_risk - 0.10) / 0.48)
    fill_spot_pending_risk, fill_spot_pending_favorable, fill_spot_pending_support = (
        _fill_spot_pending_scores()
    )
    # 中文说明：post-fill campaign risk 是 Stage 1 标签。它允许使用成交
    # 时刻 spot residual，因为用途是“这笔已成交库存是否要进入更保守的
    # campaign 管理”，不是在 submit 时刻决定是否报价。
    post_fill_scores_available = (
        filled and fill_spot_pending_support > 0 and math.isfinite(fill_spot_pending_risk)
    )
    if post_fill_scores_available:
        post_fill_campaign_outcome_risk = _clip01(
            0.72 * campaign_outcome_risk
            + 0.28 * fill_spot_pending_risk
            - 0.08
            * (fill_spot_pending_favorable if math.isfinite(fill_spot_pending_favorable) else 0.0)
        )
    else:
        post_fill_campaign_outcome_risk = math.nan

    order_reducing = _order_inventory_reducing_score_context(row, side)
    # 中文说明：reducing_burst_risk_score 只表达“同侧减仓成交刚发生过”的
    # quote-time 聚集程度，不直接代表一定有害。真正的生命周期风险还要看
    # trend/refill/campaign outcome。
    reducing_burst_risk = (
        _clip01(
            0.85 * (1.0 if reducing_burst_count_8s >= 1.0 else 0.0)
            + 0.15 * min(max(reducing_burst_qty_8s, 0.0) / 0.003, 1.0)
        )
        if order_reducing
        else 0.0
    )
    refill_edge_for_lifecycle = (
        refill_edge
        if math.isfinite(refill_edge)
        else (refresh - cancel if math.isfinite(refresh) and math.isfinite(cancel) else math.nan)
    )
    refill_weak_score = (
        _clip01((0.02 - refill_edge_for_lifecycle) / 0.12)
        if math.isfinite(refill_edge_for_lifecycle)
        else 0.35
    )
    # 中文说明：lifecycle_risk_score 是更窄的 reducing-side 节流候选：
    # burst_1+、趋势库存风险高、refill/cancel 变差、campaign 修复前景差。
    # 它是 shadow calibration 输入，不是 live hard stop。
    lifecycle_risk = (
        _clip01(
            0.30 * reducing_burst_risk
            + 0.25 * trend_inventory_risk
            + 0.20 * refill_weak_score
            + 0.25 * campaign_outcome_risk
        )
        if order_reducing and reducing_burst_count_8s >= 1.0
        else 0.0
    )

    # 中文说明：resiliency 降维为“局部吸收能力 diagnostic”。它不再混入
    # campaign 和泛化 moderator；只要求低 toxic、无 guard、refill/flow
    # 衰减、适中 depth、xmarket 不 adverse。高分仍只能先看 shadow evidence。
    resiliency = _clip01(
        (
            0.25 * non_adverse_ema_score
            + 0.20 * mild_favorable_score
            + 0.15 * medium_depth_score
            + 0.15 * quiet_book_score
            + 0.10 * refill_score
            + 0.10 * flow_decel_score
            + 0.05 * micro_reversion_raw
        )
        * risk_gate
        * no_guard_score
        * (1.0 - _clip01(xmarket_adverse_score))
        * (1.0 - 0.50 * trend_inventory_risk)
    )
    if lifecycle_risk >= 0.66:
        hint = "lifecycle_pacing_shadow"
    elif campaign_outcome_risk >= 0.66:
        hint = "stop_add_or_widen"
    elif (
        fill_quality >= 0.66
        and fill_probability >= 0.50
        and toxic_risk < 0.33
        and campaign_risk < 0.50
    ):
        hint = "quote_eligible"
    elif resiliency >= 0.66 and toxic_risk < 0.66:
        hint = "resilient_watch"
    else:
        hint = "neutral"
    return {
        "micro_fill_reach_score": micro_fill_reach,
        "fill_probability_score": fill_probability,
        "fill_quality_score": fill_quality,
        "toxic_risk_score": toxic_risk,
        "campaign_risk_score": campaign_risk,
        "campaign_outcome_risk_score": campaign_outcome_risk,
        "campaign_repair_weak_score": repair_weak_score,
        "campaign_lifecycle_intervention_score": lifecycle_intervention_pressure,
        "resiliency_score": resiliency,
        "micro_reversion_score": micro_reversion_raw,
        "trend_inventory_risk_score": trend_inventory_risk,
        "reducing_burst_risk_score": reducing_burst_risk,
        "lifecycle_risk_score": lifecycle_risk,
        "post_fill_spot_pending_risk_score": (
            fill_spot_pending_risk
            if post_fill_scores_available and math.isfinite(fill_spot_pending_risk)
            else ""
        ),
        "post_fill_spot_pending_favorable_score": (
            fill_spot_pending_favorable
            if post_fill_scores_available and math.isfinite(fill_spot_pending_favorable)
            else ""
        ),
        "post_fill_spot_pending_support": float(fill_spot_pending_support),
        "post_fill_campaign_outcome_risk_score": (
            post_fill_campaign_outcome_risk
            if math.isfinite(post_fill_campaign_outcome_risk)
            else ""
        ),
        "score_hint": hint,
    }


def order_level_rows(
    *,
    order_rows: list[dict[str, Any]],
    quote_rows: list[dict[str, Any]],
    inventory_shadow_rows: list[dict[str, Any]],
    sell_resiliency_rows: list[dict[str, Any]] | None = None,
    markout_horizons_s: tuple[int, ...] = (1, 5, 20, 30),
) -> list[dict[str, Any]]:
    """Build one row per placed order with quote-time state and outcomes.

    中文说明：这是把 bucket evidence 收回到 order denominator 的核心表。
    每一行是一笔真实 placed order；是否成交、成交后 maker-signed markout、
    campaign 风险和可解释 score 都在同一行。它适合训练/校准前的人工审阅，
    不是 live policy。
    """
    placed = [r for r in order_rows if _is_placed_event(r)]
    events_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in order_rows:
        oid = str(row.get("client_order_id", ""))
        if oid:
            events_by_id[oid].append(row)
    for rows in events_by_id.values():
        rows.sort(key=_row_ts)

    quote_by_side: dict[str, list[dict[str, Any]]] = {"BUY": [], "SELL": []}
    for row in quote_rows:
        side = norm_side(row.get("side", ""))
        if side in quote_by_side:
            quote_by_side[side].append(row)
    quote_ts_by_side: dict[str, list[float]] = {}
    for side, rows in quote_by_side.items():
        rows.sort(key=_row_ts)
        quote_ts_by_side[side] = [_row_ts(r) for r in rows]

    mid_seen: set[tuple[float, float]] = set()
    mid_series: list[tuple[float, float]] = []
    for row in quote_rows:
        ts = _row_ts(row)
        mid = safe_float(row, "mid", math.nan)
        if ts > 0.0 and mid > 0.0 and (ts, mid) not in mid_seen:
            mid_seen.add((ts, mid))
            mid_series.append((ts, mid))
    mid_series.sort(key=lambda x: x[0])
    mid_ts = [x[0] for x in mid_series]

    inv_rows = sorted(inventory_shadow_rows, key=_row_ts)
    inv_ts = [_row_ts(r) for r in inv_rows]
    resil_rows = sorted(sell_resiliency_rows or [], key=_row_ts)
    resil_ts = [_row_ts(r) for r in resil_rows]

    out: list[dict[str, Any]] = []
    for row in placed:
        oid = str(row.get("client_order_id", ""))
        side = norm_side(row.get("side", ""))
        ts = _row_ts(row)
        if side not in {"BUY", "SELL"} or not oid:
            continue
        events = events_by_id.get(oid, [])
        fill_event = next(
            (e for e in events if _is_fill_event(e) and safe_float(e, "filled_qty") > 0.0), None
        )
        terminal = events[-1] if events else row
        quote = _nearest_row(
            quote_by_side.get(side, []),
            quote_ts_by_side.get(side, []),
            ts,
            max_abs_lag_s=2.0,
            side=side,
        )
        inv = _previous_row(inv_rows, inv_ts, ts, max_lag_s=30.0)
        resil = (
            _nearest_row(resil_rows, resil_ts, ts, max_abs_lag_s=2.0, side="SELL")
            if side == "SELL"
            else None
        )

        mid = safe_float(row, "mid", safe_float(quote or {}, "mid", math.nan))
        price = safe_float(row, "price", safe_float(quote or {}, "final_price", math.nan))
        qty = safe_float(row, "quantity", safe_float(row, "target_qty", 0.0))
        signed_qty = qty if side == "BUY" else -qty
        q_before = safe_float(inv or {}, "q", 0.0)
        quote_inventory_role = inventory_role(side, q_before)
        exposure_increasing = int(abs(q_before + signed_qty) > abs(q_before) + EPS)
        quote_distance = (mid - price) if side == "BUY" else (price - mid)
        quote_distance_bps = quote_distance / mid * 10_000.0 if mid > 0.0 else math.nan
        best_bid = safe_float(row, "best_bid", safe_float(quote or {}, "best_bid", math.nan))
        best_ask = safe_float(row, "best_ask", safe_float(quote or {}, "best_ask", math.nan))
        exact_l2_spread_bps = (
            (best_ask - best_bid) / mid * 10_000.0
            if mid > 0.0 and best_ask > 0.0 and best_bid > 0.0 and best_ask >= best_bid
            else math.nan
        )
        ttl_budget_ms = safe_float(
            row,
            "xmarket_retreat_ttl_ms",
            safe_float(quote or {}, "xmarket_retreat_ttl_ms", math.nan),
        )

        filled = int(fill_event is not None)
        fill_ts = _row_ts(fill_event) if fill_event else 0.0
        fill_px = safe_float(fill_event or {}, "avg_fill_price", 0.0)
        if fill_px <= 0.0:
            fill_px = safe_float(fill_event or {}, "price", 0.0)
        fill_age_ms = safe_float(fill_event or {}, "age_ms", 0.0)
        fill_q_before = _first_finite(
            fill_event or {},
            "inventory_before_fill",
            "fill_q_before",
            "q_before_fill",
        )
        fill_inventory_role = inventory_role(side, fill_q_before) if filled else ""
        fill_role_source = (
            "exact_trace"
            if filled and math.isfinite(fill_q_before)
            else ("unknown" if filled else "")
        )
        result: dict[str, Any] = {
            "timestamp": f"{ts:.3f}",
            "utc": utc_text(ts),
            "day": utc_day(ts),
            "session_stack": session_stack(ts),
            "client_order_id": oid,
            "side": side,
            "event_type": row.get("event_type", ""),
            "outcome_event": terminal.get("event_type", ""),
            "filled": filled,
            "fill_ts": f"{fill_ts:.3f}" if fill_ts else "",
            "fill_utc": utc_text(fill_ts) if fill_ts else "",
            "fill_age_ms": f"{fill_age_ms:.3f}",
            "filled_qty": f"{safe_float(fill_event or {}, 'filled_qty'):.6f}"
            if fill_event
            else "0.000000",
            "avg_fill_price": f"{fill_px:.4f}" if fill_px > 0.0 else "",
            "price": f"{price:.4f}" if price > 0.0 else "",
            "quantity": f"{qty:.6f}",
            "mid": f"{mid:.4f}" if mid > 0.0 else "",
            "quote_distance": f"{quote_distance:.6f}" if math.isfinite(quote_distance) else "",
            "quote_distance_bps": f"{quote_distance_bps:.6f}"
            if math.isfinite(quote_distance_bps)
            else "",
            "mode": _pick(row, "mode", default=_pick(quote or {}, "mode")),
            "reason_mask": _pick(row, "reason_mask", default=_pick(quote or {}, "reason_mask")),
            "reason_text": _pick(
                row, "reason_text", default=_pick(quote or {}, "reason_text", default="none")
            ),
            "spread_mult": f"{safe_float(row, 'spread_mult', safe_float(quote or {}, 'spread_mult')):.6f}",
            "size_mult": f"{safe_float(row, 'size_mult', safe_float(quote or {}, 'size_mult')):.6f}",
            "toxicity": f"{safe_float(row, 'toxicity'):.6f}",
            "markout_ema": f"{safe_float(row, 'markout_ema'):.6f}",
            "depth_age_s": f"{safe_float(row, 'depth_age_s'):.6f}",
            "microprice_shift_bps": f"{safe_float(row, 'microprice_shift_bps'):.6f}",
            "l2_quote_flip_rate": f"{safe_float(row, 'l2_quote_flip_rate'):.6f}",
            "l2_book_refresh_ratio": f"{safe_float(row, 'l2_book_refresh_ratio'):.6f}",
            "l2_book_cancel_ratio": f"{safe_float(row, 'l2_book_cancel_ratio'):.6f}",
            "near_depth_total": f"{safe_float(row, 'l2_near_depth_total'):.6f}",
            "exact_l2_spread_bps": f"{exact_l2_spread_bps:.6f}"
            if math.isfinite(exact_l2_spread_bps)
            else "",
            "queue_init": f"{safe_float(row, 'queue_init', math.nan):.6f}"
            if math.isfinite(safe_float(row, "queue_init", math.nan))
            else "",
            "queue_left": f"{safe_float(row, 'queue_left', math.nan):.6f}"
            if math.isfinite(safe_float(row, "queue_left", math.nan))
            else "",
            "queue_local_rank": f"{safe_float(row, 'queue_local_rank', math.nan):.6f}"
            if math.isfinite(safe_float(row, "queue_local_rank", math.nan))
            else "",
            "queue_regime_mult": f"{safe_float(row, 'queue_regime_mult', math.nan):.6f}"
            if math.isfinite(safe_float(row, "queue_regime_mult", math.nan))
            else "",
            "queue_mo_mult": f"{safe_float(row, 'queue_mo_mult', math.nan):.6f}"
            if math.isfinite(safe_float(row, "queue_mo_mult", math.nan))
            else "",
            "queue_deplete_mult": f"{safe_float(row, 'queue_deplete_mult', math.nan):.6f}"
            if math.isfinite(safe_float(row, "queue_deplete_mult", math.nan))
            else "",
            "fill_eligible": _pick(row, "fill_eligible"),
            "ttl_budget_ms": f"{ttl_budget_ms:.3f}" if math.isfinite(ttl_budget_ms) else "",
            "observed_lifetime_ms": f"{safe_float(terminal, 'lifetime_ms', safe_float(terminal, 'age_ms', safe_float(fill_event or {}, 'age_ms', math.nan))):.3f}"
            if math.isfinite(
                safe_float(
                    terminal,
                    "lifetime_ms",
                    safe_float(
                        terminal, "age_ms", safe_float(fill_event or {}, "age_ms", math.nan)
                    ),
                )
            )
            else "",
            "bid_quote_fill_prob": f"{safe_float(row, 'bid_quote_fill_prob'):.6f}",
            "bid_quote_fill_markout_30s": f"{safe_float(row, 'bid_quote_fill_markout_30s'):.6f}",
            "ask_quote_fill_prob": f"{safe_float(row, 'ask_quote_fill_prob'):.6f}",
            "ask_quote_fill_markout_30s": f"{safe_float(row, 'ask_quote_fill_markout_30s'):.6f}",
            "side_quote_fill_prob": f"{safe_float(row, 'bid_quote_fill_prob' if side == 'BUY' else 'ask_quote_fill_prob'):.6f}",
            "side_quote_fill_markout_30s": f"{safe_float(row, 'bid_quote_fill_markout_30s' if side == 'BUY' else 'ask_quote_fill_markout_30s'):.6f}",
            "quote_action": _pick(quote or {}, "action"),
            "quote_allow_post": _pick(quote or {}, "allow_post"),
            "quote_allow_exposure_increase": _pick(quote or {}, "allow_exposure_increase"),
            "base_price": _pick(quote or {}, "base_price"),
            "final_price": _pick(quote or {}, "final_price"),
            "campaign_active": _pick(inv or {}, "active"),
            "campaign_id": _pick(inv or {}, "campaign_id"),
            "q_before": f"{q_before:.6f}",
            "inventory_role": quote_inventory_role,
            "inventory_role_quote": quote_inventory_role,
            "order_add_on": int(quote_inventory_role == "add"),
            "fill_q_before": f"{fill_q_before:.6f}" if math.isfinite(fill_q_before) else "",
            "fill_inventory_role": fill_inventory_role,
            "fill_role_source": fill_role_source,
            "inventory_role_drift": int(
                filled
                and fill_inventory_role not in {"", "unknown"}
                and fill_inventory_role != quote_inventory_role
            ),
            "campaign_side": _pick(inv or {}, "side"),
            "campaign_age_s": f"{safe_float(inv or {}, 'age_s'):.6f}",
            "campaign_max_abs_qty": f"{safe_float(inv or {}, 'max_abs_qty'):.6f}",
            "campaign_total_pnl": f"{safe_float(inv or {}, 'total_pnl'):.6f}",
            "campaign_adverse_excursion": f"{safe_float(inv or {}, 'adverse_excursion'):.6f}",
            "campaign_exposure_increasing_fills": _pick(inv or {}, "exposure_increasing_fills"),
            "campaign_reducing_fills": _pick(inv or {}, "reducing_fills"),
            "order_exposure_increasing": exposure_increasing,
            "shadow_block_inv006": safe_int(
                inv or {}, "bid_block_if_inv_006" if side == "BUY" else "ask_block_if_inv_006"
            ),
            "shadow_block_age60m": safe_int(
                inv or {}, "bid_block_if_age_60m" if side == "BUY" else "ask_block_if_age_60m"
            ),
            "shadow_block_reducing_only": safe_int(
                inv or {},
                "bid_block_if_reducing_only" if side == "BUY" else "ask_block_if_reducing_only",
            ),
            "sell_resil_hit": safe_int(resil or {}, "hit"),
            "sell_resil_flow_decel": f"{safe_float(resil or {}, 'flow_decel', math.nan):.6f}"
            if resil
            else "",
            "sell_resil_rank": f"{safe_float(resil or {}, 'rank', math.nan):.6f}" if resil else "",
            "sell_resil_refill_edge": f"{safe_float(resil or {}, 'refill_edge', math.nan):.6f}"
            if resil
            else "",
            "sell_resil_ref_adv": f"{safe_float(resil or {}, 'ref_adv', math.nan):.6f}"
            if resil
            else "",
            "sell_resil_spot_adv": f"{safe_float(resil or {}, 'spot_adv', math.nan):.6f}"
            if resil
            else "",
            "sell_resil_spot_available": _pick(resil or {}, "spot_available"),
        }
        path_features = _order_path_features(
            ts=ts,
            side=side,
            mid=mid,
            quote_distance_bps=quote_distance_bps,
            mid_series=mid_series,
            mid_ts=mid_ts,
        )
        for key, value in path_features.items():
            result[key] = _fmt_path_feature(value)
        for horizon_s in markout_horizons_s:
            markout = math.nan
            opportunity_markout = math.nan
            if ts > 0.0 and price > 0.0:
                opportunity_markout = _maker_signed_markout_bps(
                    side,
                    price,
                    _future_mid(mid_series, mid_ts, ts + horizon_s),
                )
            if filled and fill_ts > 0.0 and fill_px > 0.0:
                markout = _maker_signed_markout_bps(
                    side, fill_px, _future_mid(mid_series, mid_ts, fill_ts + horizon_s)
                )
            result[f"opportunity_markout_{horizon_s}s_bps"] = (
                f"{opportunity_markout:.6f}" if math.isfinite(opportunity_markout) else ""
            )
            result[f"markout_{horizon_s}s_bps"] = f"{markout:.6f}" if math.isfinite(markout) else ""
        scores = _order_scores(result)
        for key, value in scores.items():
            result[key] = f"{value:.6f}" if isinstance(value, float) else value
        out.append(result)
    return out


def order_level_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    filled = [r for r in rows if safe_int(r, "filled") == 1]
    out: dict[str, Any] = {
        "order_rows": len(rows),
        "filled_orders": len(filled),
        "fill_rate": len(filled) / len(rows),
        "buy_orders": sum(1 for r in rows if r.get("side") == "BUY"),
        "sell_orders": sum(1 for r in rows if r.get("side") == "SELL"),
        "exposure_increasing_orders": sum(safe_int(r, "order_exposure_increasing") for r in rows),
        "opener_orders": sum(1 for r in rows if r.get("inventory_role") == "opener"),
        "add_orders": sum(1 for r in rows if r.get("inventory_role") == "add"),
        "reducing_orders": sum(1 for r in rows if r.get("inventory_role") == "reducing"),
        "filled_role_drift_orders": sum(safe_int(r, "inventory_role_drift") for r in rows),
        "shadow_inv006_orders": sum(safe_int(r, "shadow_block_inv006") for r in rows),
        "shadow_age60m_orders": sum(safe_int(r, "shadow_block_age60m") for r in rows),
        "shadow_reducing_only_orders": sum(safe_int(r, "shadow_block_reducing_only") for r in rows),
    }
    for side in ("BUY", "SELL"):
        side_rows = [r for r in rows if r.get("side") == side]
        side_fills = [r for r in side_rows if safe_int(r, "filled") == 1]
        out[f"{side.lower()}_orders"] = len(side_rows)
        out[f"{side.lower()}_fill_rate"] = len(side_fills) / len(side_rows) if side_rows else 0.0
        out[f"{side.lower()}_avg_markout_30s_bps"] = _fill_qty_weighted_mean(
            side_fills, "markout_30s_bps"
        )
    for score in ORDER_LEVEL_SCORE_COLS:
        out[f"avg_{score}"] = _safe_mean([safe_float(r, score, math.nan) for r in rows])
    return out


def order_level_score_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    quantile_bucket_by_score = {
        score: _rank_quantile_bucket_map(
            rows, score=score, group_key=lambda r: (str(r.get("side", "")),)
        )
        for score in RANK_QUANTILE_SCORE_COLS
    }
    for row in rows:
        for score in ORDER_LEVEL_SCORE_COLS:
            bucket = _score_bucket_value(
                row, score, toxic_bucket_by_row=quantile_bucket_by_score.get(score)
            )
            groups[(score, bucket, str(row.get("side", "")))].append(row)
    out: list[dict[str, Any]] = []
    for (score, bucket, side), group in sorted(groups.items()):
        fills = [r for r in group if safe_int(r, "filled") == 1]
        out.append(
            {
                "score": score,
                "bucket": bucket,
                "bucket_mode": "side_rank_quantile"
                if score in RANK_QUANTILE_SCORE_COLS
                else "fixed_0p33_0p66",
                "side": side,
                "orders": len(group),
                "filled_orders": len(fills),
                "fill_rate": f"{len(fills) / len(group) if group else 0.0:.6f}",
                "avg_markout_5s_bps": f"{_fill_qty_weighted_mean(fills, 'markout_5s_bps'):.6f}",
                "avg_markout_20s_bps": f"{_fill_qty_weighted_mean(fills, 'markout_20s_bps'):.6f}",
                "avg_markout_30s_bps": f"{_fill_qty_weighted_mean(fills, 'markout_30s_bps'):.6f}",
                "tail_rate_m50_30s": f"{sum(1 for r in fills if safe_float(r, 'markout_30s_bps', math.nan) <= -50.0) / len(fills) if fills else 0.0:.6f}",
                "avg_campaign_risk_score": f"{_safe_mean([safe_float(r, 'campaign_risk_score', math.nan) for r in group]):.6f}",
                "avg_toxic_risk_score": f"{_safe_mean([safe_float(r, 'toxic_risk_score', math.nan) for r in group]):.6f}",
            }
        )
    return out


def _order_group_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    fills = [r for r in rows if safe_int(r, "filled") == 1]
    terminal_campaigns = _unique_terminal_campaign_rows(rows)
    orders = len(rows)
    filled = len(fills)
    return {
        "orders": orders,
        "filled_orders": filled,
        "fill_rate": filled / orders if orders else 0.0,
        "avg_markout_1s_bps": _fill_qty_weighted_mean(fills, "markout_1s_bps"),
        "avg_markout_5s_bps": _fill_qty_weighted_mean(fills, "markout_5s_bps"),
        "avg_markout_20s_bps": _fill_qty_weighted_mean(fills, "markout_20s_bps"),
        "avg_markout_30s_bps": _fill_qty_weighted_mean(fills, "markout_30s_bps"),
        "tail_rate_m50_30s": (
            sum(1 for r in fills if safe_float(r, "markout_30s_bps", math.nan) <= -50.0) / filled
            if filled
            else 0.0
        ),
        "positive_rate_30s": (
            sum(1 for r in fills if safe_float(r, "markout_30s_bps", math.nan) > 0.0) / filled
            if filled
            else 0.0
        ),
        "avg_quote_distance_bps": _safe_mean(
            [safe_float(r, "quote_distance_bps", math.nan) for r in rows]
        ),
        "avg_quote_distance_micro": _safe_mean(
            [safe_float(r, "quote_distance_micro", math.nan) for r in rows]
        ),
        "avg_micro_macro_range_ratio": _safe_mean(
            [safe_float(r, "micro_macro_range_ratio", math.nan) for r in rows]
        ),
        "avg_micro_macro_vol_ratio": _safe_mean(
            [safe_float(r, "micro_macro_vol_ratio", math.nan) for r in rows]
        ),
        "avg_trend_efficiency_300s": _safe_mean(
            [safe_float(r, "trend_efficiency_300s", math.nan) for r in rows]
        ),
        "avg_side_trend_adverse_300s_bps": _safe_mean(
            [safe_float(r, "side_trend_adverse_300s_bps", math.nan) for r in rows]
        ),
        "avg_campaign_age_s": _safe_mean([safe_float(r, "campaign_age_s", math.nan) for r in rows]),
        "avg_campaign_max_abs_qty": _safe_mean(
            [safe_float(r, "campaign_max_abs_qty", math.nan) for r in rows]
        ),
        "avg_campaign_mae": _safe_mean(
            [safe_float(r, "campaign_adverse_excursion", math.nan) for r in rows]
        ),
        "terminal_labeled_orders": sum(
            1 for r in rows if str(r.get("terminal_campaign_label", ""))
        ),
        "terminal_labeled_campaigns": len(terminal_campaigns),
        "avg_terminal_campaign_pnl": _safe_mean(
            [safe_float(r, "terminal_final_total_pnl_delta", math.nan) for r in terminal_campaigns]
        ),
        "terminal_repair_rate": (
            _safe_mean(
                [safe_float(r, "terminal_campaign_repaired", math.nan) for r in terminal_campaigns]
            )
        ),
        "terminal_tail_loss_rate": (
            _safe_mean(
                [safe_float(r, "terminal_campaign_tail_loss", math.nan) for r in terminal_campaigns]
            )
        ),
        "terminal_bad_rate": (
            _safe_mean(
                [safe_float(r, "terminal_campaign_bad", math.nan) for r in terminal_campaigns]
            )
        ),
        "avg_terminal_outcome_risk_target": _safe_mean(
            [
                safe_float(
                    r,
                    "terminal_campaign_outcome_risk_target",
                    math.nan,
                )
                for r in terminal_campaigns
            ]
        ),
        "avg_terminal_early_20m_drawdown": _safe_mean(
            [safe_float(r, "terminal_early_drawdown_20m", math.nan) for r in terminal_campaigns]
        ),
        "exposure_increasing_orders": sum(safe_int(r, "order_exposure_increasing") for r in rows),
        "shadow_inv006_orders": sum(safe_int(r, "shadow_block_inv006") for r in rows),
        "shadow_age60m_orders": sum(safe_int(r, "shadow_block_age60m") for r in rows),
        "shadow_reducing_only_orders": sum(safe_int(r, "shadow_block_reducing_only") for r in rows),
    }


def _fmt_stats(prefix: str, stats: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in stats.items():
        if isinstance(value, float):
            out[f"{prefix}{key}"] = f"{value:.6f}"
        else:
            out[f"{prefix}{key}"] = value
    return out


def order_level_score_daily_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Daily sanity table for score buckets.

    中文说明：这张表回答“score 是否稳定解释 outcome”，不是找最优参数。
    toxic / post-fill risk 用 day+side 内部 rank bucket，保证每天都有
    p70/p85/p95 层级可看；其余 score 继续用固定 low/mid/high。
    """
    daily_quantile_bucket_by_score = {
        score: _rank_quantile_bucket_map(
            rows,
            score=score,
            group_key=lambda r: (str(r.get("day", "")), str(r.get("side", ""))),
        )
        for score in RANK_QUANTILE_SCORE_COLS
    }
    score_cols = ORDER_LEVEL_SCORE_COLS
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        day = str(row.get("day", ""))
        side = str(row.get("side", ""))
        if not day or side not in {"BUY", "SELL"}:
            continue
        for score in score_cols:
            groups[
                (
                    day,
                    side,
                    score,
                    _score_bucket_value(
                        row,
                        score,
                        toxic_bucket_by_row=daily_quantile_bucket_by_score.get(score),
                    ),
                )
            ].append(row)
    out: list[dict[str, Any]] = []
    for (day, side, score, bucket), group in sorted(groups.items()):
        stats = _order_group_stats(group)
        out.append(
            {
                "day": day,
                "side": side,
                "score": score,
                "bucket": bucket,
                "bucket_mode": "day_side_rank_quantile"
                if score in RANK_QUANTILE_SCORE_COLS
                else "fixed_0p33_0p66",
                **_fmt_stats("", stats),
            }
        )
    return out


def order_level_score_sanity_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate score sanity checks.

    Expectations:
    - fill_probability high should raise fill rate.
    - fill_quality high should improve maker-signed markout.
    - toxic_risk p95 should worsen markout/tail relative to p70.
    - campaign_risk high should correspond to higher inventory exposure/MAE.
    - resiliency high should not be systematically more toxic.
    """
    specs = {
        "micro_fill_reach_score": "high_fill_rate_above_low",
        "fill_probability_score": "high_fill_rate_above_low",
        "fill_quality_score": "high_markout_above_low",
        "toxic_risk_score": "p95_markout_or_tail_worse_than_p70",
        "campaign_risk_score": "high_inventory_exposure_above_low",
        "campaign_outcome_risk_score": "high_terminal_campaign_risk_above_low",
        "resiliency_score": "high_not_more_toxic_than_low",
        "micro_reversion_score": "high_not_more_toxic_and_terminal_not_worse",
        "trend_inventory_risk_score": "high_markout_or_terminal_worse_than_low",
        "reducing_burst_risk_score": "high_reducing_burst_fill_rate_above_low",
        "lifecycle_risk_score": "high_terminal_or_early_campaign_risk_above_low",
        "post_fill_spot_pending_risk_score": "high_markout_or_terminal_worse_than_low",
        "post_fill_campaign_outcome_risk_score": "high_terminal_campaign_risk_above_low",
    }
    quantile_bucket_by_score = {
        score: _rank_quantile_bucket_map(
            rows, score=score, group_key=lambda r: (str(r.get("side", "")),)
        )
        for score in RANK_QUANTILE_SCORE_COLS
    }
    out: list[dict[str, Any]] = []
    for side in ("BUY", "SELL"):
        side_rows = [r for r in rows if r.get("side") == side]
        for score, expectation in specs.items():
            if score in RANK_QUANTILE_SCORE_COLS:
                score_bucket_map = quantile_bucket_by_score.get(score, {})
                buckets = {
                    "low": [
                        r
                        for r in side_rows
                        if _score_bucket_value(r, score, toxic_bucket_by_row=score_bucket_map)
                        == "q000_070"
                    ],
                    "mid": [
                        r
                        for r in side_rows
                        if _score_bucket_value(r, score, toxic_bucket_by_row=score_bucket_map)
                        in {"q070_085", "q085_095"}
                    ],
                    "high": [
                        r
                        for r in side_rows
                        if _score_bucket_value(r, score, toxic_bucket_by_row=score_bucket_map)
                        == "q095_100"
                    ],
                }
                bucket_mode = "side_rank_quantile"
                low_bucket = "q000_070"
                mid_bucket = "q070_095"
                high_bucket = "q095_100"
            else:
                buckets = {
                    bucket: [r for r in side_rows if _score_bucket_value(r, score) == bucket]
                    for bucket in ("low", "mid", "high")
                }
                bucket_mode = "fixed_0p33_0p66"
                low_bucket = "low"
                mid_bucket = "mid"
                high_bucket = "high"
            low = _order_group_stats(buckets["low"])
            high = _order_group_stats(buckets["high"])
            mid = _order_group_stats(buckets["mid"])
            verdict = "insufficient_high_support"
            if high["orders"] >= 100 and high["filled_orders"] >= 5 and low["orders"] > 0:
                if score in {"micro_fill_reach_score", "fill_probability_score"}:
                    verdict = "pass" if high["fill_rate"] >= low["fill_rate"] else "fail"
                elif score == "fill_quality_score":
                    verdict = (
                        "pass"
                        if high["avg_markout_30s_bps"] >= low["avg_markout_30s_bps"]
                        else "fail"
                    )
                elif score == "toxic_risk_score":
                    worse_markout = high["avg_markout_30s_bps"] <= low["avg_markout_30s_bps"]
                    worse_tail = high["tail_rate_m50_30s"] >= low["tail_rate_m50_30s"]
                    verdict = "pass" if (worse_markout or worse_tail) else "fail"
                elif score == "campaign_risk_score":
                    worse_inventory = (
                        high["avg_campaign_age_s"] >= low["avg_campaign_age_s"]
                        and high["avg_campaign_max_abs_qty"] >= low["avg_campaign_max_abs_qty"]
                    )
                    verdict = "pass" if worse_inventory else "fail"
                elif score == "campaign_outcome_risk_score":
                    if high["terminal_labeled_orders"] < 20 or low["terminal_labeled_orders"] < 20:
                        verdict = "insufficient_terminal_support"
                    else:
                        worse_terminal = (
                            high["avg_terminal_outcome_risk_target"]
                            >= low["avg_terminal_outcome_risk_target"]
                            or high["terminal_tail_loss_rate"] >= low["terminal_tail_loss_rate"]
                            or high["avg_terminal_campaign_pnl"] <= low["avg_terminal_campaign_pnl"]
                        )
                        verdict = "pass" if worse_terminal else "fail"
                elif score == "resiliency_score":
                    not_more_toxic = (
                        high["avg_markout_30s_bps"] >= low["avg_markout_30s_bps"] - 0.25
                    )
                    verdict = "pass" if not_more_toxic else "fail"
                elif score == "micro_reversion_score":
                    not_more_toxic = (
                        high["avg_markout_30s_bps"] >= low["avg_markout_30s_bps"] - 0.25
                    )
                    terminal_not_worse = (
                        high["terminal_labeled_orders"] < 20
                        or low["terminal_labeled_orders"] < 20
                        or high["terminal_tail_loss_rate"] <= low["terminal_tail_loss_rate"] + 0.02
                    )
                    verdict = "pass" if (not_more_toxic and terminal_not_worse) else "fail"
                elif score == "trend_inventory_risk_score":
                    worse = (
                        high["avg_markout_30s_bps"] <= low["avg_markout_30s_bps"]
                        or high["tail_rate_m50_30s"] >= low["tail_rate_m50_30s"]
                        or high["avg_terminal_outcome_risk_target"]
                        >= low["avg_terminal_outcome_risk_target"]
                        or high["avg_terminal_campaign_pnl"] <= low["avg_terminal_campaign_pnl"]
                    )
                    verdict = "pass" if worse else "fail"
                elif score == "reducing_burst_risk_score":
                    # Burst itself is only a clustering/fill-rate feature. It is
                    # not expected to be toxic unless lifecycle_risk also fires.
                    verdict = "pass" if high["fill_rate"] >= low["fill_rate"] else "fail"
                elif score == "lifecycle_risk_score":
                    worse = (
                        high["avg_terminal_campaign_pnl"] <= low["avg_terminal_campaign_pnl"]
                        or high["terminal_tail_loss_rate"] >= low["terminal_tail_loss_rate"]
                        or high["avg_terminal_early_20m_drawdown"]
                        >= low["avg_terminal_early_20m_drawdown"]
                    )
                    verdict = "pass" if worse else "fail"
                elif score == "post_fill_spot_pending_risk_score":
                    worse = (
                        high["avg_markout_1s_bps"] <= low["avg_markout_1s_bps"]
                        or high["avg_markout_5s_bps"] <= low["avg_markout_5s_bps"]
                        or high["avg_markout_30s_bps"] <= low["avg_markout_30s_bps"]
                        or high["avg_terminal_campaign_pnl"] <= low["avg_terminal_campaign_pnl"]
                        or high["terminal_tail_loss_rate"] >= low["terminal_tail_loss_rate"]
                    )
                    verdict = "pass" if worse else "fail"
                elif score == "post_fill_campaign_outcome_risk_score":
                    if high["terminal_labeled_orders"] < 20 or low["terminal_labeled_orders"] < 20:
                        verdict = "insufficient_terminal_support"
                    else:
                        worse_terminal = (
                            high["avg_terminal_outcome_risk_target"]
                            >= low["avg_terminal_outcome_risk_target"]
                            or high["terminal_tail_loss_rate"] >= low["terminal_tail_loss_rate"]
                            or high["avg_terminal_campaign_pnl"] <= low["avg_terminal_campaign_pnl"]
                        )
                        verdict = "pass" if worse_terminal else "fail"
            out.append(
                {
                    "side": side,
                    "score": score,
                    "expectation": expectation,
                    "verdict": verdict,
                    "bucket_mode": bucket_mode,
                    "low_bucket": low_bucket,
                    "mid_bucket": mid_bucket,
                    "high_bucket": high_bucket,
                    **_fmt_stats("low_", low),
                    **_fmt_stats("mid_", mid),
                    **_fmt_stats("high_", high),
                    "delta_high_minus_low_fill_rate": f"{high['fill_rate'] - low['fill_rate']:.6f}",
                    "delta_high_minus_low_markout_30s_bps": f"{high['avg_markout_30s_bps'] - low['avg_markout_30s_bps']:.6f}",
                    "delta_high_minus_low_campaign_age_s": f"{high['avg_campaign_age_s'] - low['avg_campaign_age_s']:.6f}",
                    "delta_high_minus_low_campaign_max_abs_qty": f"{high['avg_campaign_max_abs_qty'] - low['avg_campaign_max_abs_qty']:.6f}",
                    "delta_high_minus_low_terminal_campaign_pnl": f"{high['avg_terminal_campaign_pnl'] - low['avg_terminal_campaign_pnl']:.6f}",
                    "delta_high_minus_low_terminal_repair_rate": f"{high['terminal_repair_rate'] - low['terminal_repair_rate']:.6f}",
                    "delta_high_minus_low_terminal_tail_loss_rate": f"{high['terminal_tail_loss_rate'] - low['terminal_tail_loss_rate']:.6f}",
                    "delta_high_minus_low_terminal_bad_rate": f"{high['terminal_bad_rate'] - low['terminal_bad_rate']:.6f}",
                    "delta_high_minus_low_terminal_outcome_risk_target": f"{high['avg_terminal_outcome_risk_target'] - low['avg_terminal_outcome_risk_target']:.6f}",
                    "delta_high_minus_low_terminal_early_20m_drawdown": f"{high['avg_terminal_early_20m_drawdown'] - low['avg_terminal_early_20m_drawdown']:.6f}",
                }
            )
    return out


def _knob_rule(row: dict[str, Any]) -> tuple[str, str, str] | None:
    side = str(row.get("side", "")).upper()
    fill_probability = safe_float(row, "fill_probability_score", math.nan)
    fill_quality = safe_float(row, "fill_quality_score", math.nan)
    toxic_score = safe_float(row, "toxic_risk_score", math.nan)
    campaign_score = safe_float(row, "campaign_risk_score", math.nan)
    campaign_outcome_score = safe_float(row, "campaign_outcome_risk_score", math.nan)
    campaign_repair_weak = safe_float(row, "campaign_repair_weak_score", math.nan)
    resil_score = safe_float(row, "resiliency_score", math.nan)
    micro_reversion = safe_float(row, "micro_reversion_score", math.nan)
    trend_inventory_risk = safe_float(row, "trend_inventory_risk_score", math.nan)
    lifecycle_risk = safe_float(row, "lifecycle_risk_score", math.nan)
    burst_count_8s = safe_float(row, "reducing_burst_count_8s", 0.0)
    refill_edge = safe_float(
        row,
        "sell_resil_refill_edge",
        safe_float(row, "l2_book_refresh_ratio", 0.0)
        - safe_float(row, "l2_book_cancel_ratio", 0.0),
    )
    exposure_increasing = safe_int(row, "order_exposure_increasing") == 1
    order_reducing = _order_inventory_reducing_score_context(row, side)
    # 中文说明：SELL + exposure-increasing 是 add/open short，不是卖出减多。
    # retained evidence 显示它的 campaign sorter 可用于生命周期风控校准，
    # 但 20s/30s markout 仍未转正，所以只能输出 shadow stop-add/skew/TTL，
    # 不能把它解释成 tighten 或 size-up alpha。
    sell_add_short = side == "SELL" and exposure_increasing and not order_reducing
    local_repair_weak = (
        (math.isfinite(refill_edge) and refill_edge <= 0.02)
        or (math.isfinite(campaign_repair_weak) and campaign_repair_weak >= 0.50)
        or (math.isfinite(micro_reversion) and micro_reversion < 0.40)
    )
    if (
        sell_add_short
        and campaign_outcome_score >= 0.66
        and (trend_inventory_risk >= 0.50 or local_repair_weak)
    ):
        return (
            "sell_addshort_campaign_lifecycle_shadow",
            "stop_add_skew_away_or_shorter_add_ttl_shadow",
            "SELL add/open-short campaign sorter high-risk; shadow only: stop adding short, skew away from adding short, or shorten add-side TTL; do not tighten or increase size",
        )
    if (
        sell_add_short
        and campaign_outcome_score <= 0.40
        and toxic_score < 0.66
        and trend_inventory_risk < 0.66
    ):
        return (
            "sell_addshort_campaign_low_risk_keep_shadow",
            "normal_quote_keep_no_tighten",
            "SELL add/open-short campaign sorter low-risk; keep as normal-quote evidence only because fill markout is not positive enough for tighten/size",
        )
    if (
        order_reducing
        and burst_count_8s >= 1.0
        and trend_inventory_risk >= 0.66
        and refill_edge <= 0.02
        and campaign_outcome_score >= 0.66
        and lifecycle_risk >= 0.66
    ):
        return (
            "reducing_burst_lifecycle_narrow",
            "shorter_ttl_or_pacing_shadow",
            "narrow reducing-side lifecycle shadow: burst_1+, high trend inventory risk, weak/negative refill, and poor campaign repair score; not a fixed cooldown",
        )
    if trend_inventory_risk >= 0.66 and exposure_increasing:
        return (
            "trend_inventory_risk_high_exposure_increasing",
            "soft_spread_widen_or_reducing_skew",
            "quote side is adverse to the 1m/5m trend; treat as inventory-risk shadow input, not an alpha trigger",
        )
    if campaign_outcome_score >= 0.66 and exposure_increasing:
        return (
            "campaign_outcome_high_exposure_increasing",
            "soft_spread_widen_or_reducing_skew",
            "campaign terminal risk high; treat 0.006 BTC / 60m style triggers as score inputs, then only lightly widen or skew the exposure-increasing side in shadow",
        )
    if campaign_score >= 0.66 and exposure_increasing:
        return (
            "campaign_state_high_exposure_increasing",
            "soft_spread_widen_or_reducing_skew",
            "campaign state risk high; use as risk-control shadow input, not a hard stop or alpha signal",
        )
    if toxic_score >= 0.66 and resil_score < 0.33:
        return (
            "toxic_high_resil_low",
            "ttl_shorter_or_widen",
            "widen spread and shorten lifecycle; do not increase size",
        )
    if fill_probability >= 0.66 and fill_quality < 0.33:
        return (
            "fill_prob_high_quality_low",
            "spread_widen_or_skip",
            "high fill probability but poor fill quality; do not tighten from fill-rate alone",
        )
    if (
        fill_probability >= 0.50
        and fill_quality >= 0.66
        and toxic_score < 0.33
        and campaign_score < 0.50
    ):
        return (
            "fill_prob_quality_ok_toxic_low_campaign_ok",
            "normal_quote_keep",
            "normal spread/lifecycle; eligible for future keep/tighten study",
        )
    if micro_reversion >= 0.66 and fill_quality >= 0.50 and trend_inventory_risk < 0.50:
        return (
            "micro_reversion_watch",
            "normal_or_slight_keep",
            "short-window noise is high while macro trend is weak; keep as shadow evidence before any tighten/size study",
        )
    if resil_score >= 0.66 and toxic_score < 0.66 and campaign_score < 0.66:
        return (
            "resilient_non_toxic",
            "normal_or_slight_keep",
            "do not widen only because of isolated bucket evidence",
        )
    return None


def order_level_knob_shadow_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Map continuous scores to spread/skew/lifecycle shadow actions."""
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    rule_desc: dict[tuple[str, str], str] = {}
    for row in rows:
        rule = _knob_rule(row)
        if rule is None:
            continue
        name, knob, desc = rule
        side = str(row.get("side", ""))
        day = str(row.get("day", ""))
        if side not in {"BUY", "SELL"} or not day:
            continue
        groups[(day, side, name, knob)].append(row)
        rule_desc[(name, knob)] = desc
    out: list[dict[str, Any]] = []
    for (day, side, name, knob), group in sorted(groups.items()):
        stats = _order_group_stats(group)
        out.append(
            {
                "day": day,
                "side": side,
                "shadow_rule": name,
                "knob": knob,
                "description": rule_desc.get((name, knob), ""),
                **_fmt_stats("", stats),
            }
        )
    return out


def order_level_score_audit_summary(
    sanity_rows: list[dict[str, Any]],
    daily_rows: list[dict[str, Any]],
    knob_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "sanity_checks": len(sanity_rows),
        "sanity_pass": sum(1 for r in sanity_rows if r.get("verdict") == "pass"),
        "sanity_fail": sum(1 for r in sanity_rows if r.get("verdict") == "fail"),
        "sanity_insufficient": sum(
            1 for r in sanity_rows if r.get("verdict") == "insufficient_high_support"
        ),
        "daily_score_rows": len(daily_rows),
        "knob_shadow_rows": len(knob_rows),
        "knob_shadow_orders": sum(safe_int(r, "orders") for r in knob_rows),
    }


NULL_BASELINE_HORIZONS_S = (20, 30)


def _finite_values(rows: list[dict[str, Any]], column: str) -> list[float]:
    return [
        safe_float(r, column, math.nan)
        for r in rows
        if math.isfinite(safe_float(r, column, math.nan))
    ]


def _positive_rate(values: list[float]) -> float:
    return sum(1 for v in values if v > 0.0) / len(values) if values else 0.0


def _tail_rate(values: list[float], threshold_bps: float = -50.0) -> float:
    return sum(1 for v in values if v <= threshold_bps) / len(values) if values else 0.0


def _avg_terminal_pnl(rows: list[dict[str, Any]]) -> float:
    return _safe_mean(
        [
            safe_float(r, "terminal_final_total_pnl_delta", math.nan)
            for r in _unique_terminal_campaign_rows(rows)
        ]
    )


def _avg_terminal_target(rows: list[dict[str, Any]]) -> float:
    return _safe_mean(
        [
            safe_float(r, "terminal_campaign_outcome_risk_target", math.nan)
            for r in _unique_terminal_campaign_rows(rows)
        ]
    )


def _repair_rate(rows: list[dict[str, Any]]) -> float:
    return _safe_mean(
        [
            safe_float(r, "terminal_campaign_repaired", math.nan)
            for r in _unique_terminal_campaign_rows(rows)
        ]
    )


def _xmarket_not_adverse_for_null(row: dict[str, Any]) -> tuple[bool, str]:
    """Return xmarket veto state when the order-level table has ref tags.

    中文说明：null-baseline report 可以在没有 xmarket shadow 字段的
    order-level 表上运行。此时返回 unknown_as_pass，避免把缺字段误解成
    adverse；最终表会暴露这个状态，防止把 positive intersection 误读成
    已经过 BTCUSDT/spot moderator。
    """
    known = False
    for key, value in row.items():
        if "pending_ref" in key and "side_bucket" in key:
            text = str(value or "").lower()
            if text:
                known = True
                if "adverse" in text:
                    return False, "adverse_pending_ref"
        if key in {"xmarket_state", "ref_state", "spot_state"}:
            text = str(value or "").lower()
            if text:
                known = True
                if "adverse" in text:
                    return False, f"adverse_{key}"
    ref_adv = safe_float(row, "sell_resil_ref_adv", math.nan)
    spot_adv = safe_float(row, "sell_resil_spot_adv", math.nan)
    if math.isfinite(ref_adv) or math.isfinite(spot_adv):
        known = True
        if max(ref_adv if math.isfinite(ref_adv) else 0.0, 0.0) > 0.0:
            return False, "adverse_ref_shadow"
        if max(spot_adv if math.isfinite(spot_adv) else 0.0, 0.0) > 0.0:
            return False, "adverse_spot_shadow"
    return True, "not_adverse" if known else "unknown_as_pass"


def _positive_intersection_flags(row: dict[str, Any]) -> dict[str, Any]:
    fill_quality = safe_float(row, "fill_quality_score", math.nan)
    campaign_outcome = safe_float(row, "campaign_outcome_risk_score", math.nan)
    toxic = safe_float(row, "toxic_risk_score", math.nan)
    fill_probability = safe_float(row, "fill_probability_score", math.nan)
    local_ctx = _local_liquidity_context(row)
    xmarket_ok, xmarket_state = _xmarket_not_adverse_for_null(row)
    flags = {
        "fill_quality_high": fill_quality >= 0.66,
        "campaign_outcome_low": campaign_outcome <= 0.40,
        "toxic_low": toxic <= 0.33,
        "fill_probability_not_low": fill_probability >= 0.33,
        "local_absorption_ok": safe_float(local_ctx, "capacity_score", 0.0) >= 0.50,
        "xmarket_not_adverse": xmarket_ok,
        "xmarket_state_for_null": xmarket_state,
        "local_capacity_score_for_null": safe_float(local_ctx, "capacity_score", math.nan),
    }
    flags["positive_intersection"] = int(
        all(
            bool(v)
            for k, v in flags.items()
            if k
            not in {
                "xmarket_state_for_null",
                "local_capacity_score_for_null",
            }
        )
    )
    return flags


def _current_group_row(day: str, side: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    fills = [r for r in rows if safe_int(r, "filled") == 1]
    out: dict[str, Any] = {
        "day": day,
        "side": side,
        "orders": len(rows),
        "filled_orders": len(fills),
        "fill_rate": f"{len(fills) / len(rows) if rows else 0.0:.6f}",
        "avg_quote_distance_bps": f"{_safe_mean([safe_float(r, 'quote_distance_bps', math.nan) for r in rows]):.6f}",
        "avg_opportunity_markout_20s_bps_all_orders": f"{_safe_mean(_finite_values(rows, 'opportunity_markout_20s_bps')):.6f}",
        "avg_opportunity_markout_30s_bps_all_orders": f"{_safe_mean(_finite_values(rows, 'opportunity_markout_30s_bps')):.6f}",
        "avg_terminal_campaign_pnl": f"{_avg_terminal_pnl(rows):.6f}",
        "avg_terminal_outcome_risk_target": f"{_avg_terminal_target(rows):.6f}",
        "terminal_repair_rate": f"{_repair_rate(rows):.6f}",
    }
    for horizon_s in NULL_BASELINE_HORIZONS_S:
        values = _finite_values(fills, f"markout_{horizon_s}s_bps")
        out[f"actual_fill_avg_markout_{horizon_s}s_bps"] = (
            f"{_fill_qty_weighted_mean(fills, f'markout_{horizon_s}s_bps'):.6f}"
        )
        out[f"actual_fill_positive_rate_{horizon_s}s"] = f"{_positive_rate(values):.6f}"
        out[f"actual_fill_tail_rate_m50_{horizon_s}s"] = f"{_tail_rate(values):.6f}"
    return out


def _random_null_rows_for_group(
    *,
    day: str,
    side: str,
    rows: list[dict[str, Any]],
    trials: int,
    seed: int,
) -> list[dict[str, Any]]:
    fills = [r for r in rows if safe_int(r, "filled") == 1]
    k = len(fills)
    out: list[dict[str, Any]] = []
    if not rows or k <= 0:
        return out
    for horizon_s in NULL_BASELINE_HORIZONS_S:
        current_values = _finite_values(fills, f"markout_{horizon_s}s_bps")
        trial_means: list[float] = []
        trial_pos: list[float] = []
        trial_tail: list[float] = []
        eligible = [
            r
            for r in rows
            if math.isfinite(safe_float(r, f"opportunity_markout_{horizon_s}s_bps", math.nan))
        ]
        if not eligible:
            continue
        sample_n = min(k, len(eligible))
        for trial in range(max(1, trials)):
            rng = random.Random(f"{seed}:{day}:{side}:{horizon_s}:{trial}")
            sample = rng.sample(eligible, sample_n) if sample_n < len(eligible) else list(eligible)
            values = _finite_values(sample, f"opportunity_markout_{horizon_s}s_bps")
            trial_means.append(_safe_mean(values))
            trial_pos.append(_positive_rate(values))
            trial_tail.append(_tail_rate(values))
        current_mean = _safe_mean(current_values)
        out.append(
            {
                "day": day,
                "side": side,
                "horizon_s": horizon_s,
                "orders": len(rows),
                "actual_filled_orders": k,
                "random_sample_size": sample_n,
                "trials": max(1, trials),
                "actual_fill_avg_markout_bps": f"{current_mean:.6f}",
                "random_opportunity_avg_markout_mean_bps": f"{_safe_mean(trial_means):.6f}",
                "random_opportunity_avg_markout_p05_bps": f"{_safe_quantile(trial_means, 0.05):.6f}",
                "random_opportunity_avg_markout_p50_bps": f"{_safe_quantile(trial_means, 0.50):.6f}",
                "random_opportunity_avg_markout_p95_bps": f"{_safe_quantile(trial_means, 0.95):.6f}",
                "delta_actual_minus_random_mean_bps": f"{current_mean - _safe_mean(trial_means):.6f}",
                "actual_fill_positive_rate": f"{_positive_rate(current_values):.6f}",
                "random_positive_rate_mean": f"{_safe_mean(trial_pos):.6f}",
                "actual_fill_tail_rate_m50": f"{_tail_rate(current_values):.6f}",
                "random_tail_rate_m50_mean": f"{_safe_mean(trial_tail):.6f}",
                "note": "random samples same day/side placed orders and evaluates submit-time opportunity_markout, not a full fill counterfactual",
            }
        )
    return out


def _oracle_rows_for_group(day: str, side: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fills = [r for r in rows if safe_int(r, "filled") == 1]
    k = len(fills)
    out: list[dict[str, Any]] = []
    if not rows or k <= 0:
        return out
    for horizon_s in NULL_BASELINE_HORIZONS_S:
        actual_values = _finite_values(fills, f"markout_{horizon_s}s_bps")
        opportunities = [
            (safe_float(r, f"opportunity_markout_{horizon_s}s_bps", math.nan), r) for r in rows
        ]
        opportunities = [(v, r) for v, r in opportunities if math.isfinite(v)]
        opportunities.sort(key=lambda x: x[0], reverse=True)
        top = [r for _, r in opportunities[: min(k, len(opportunities))]]
        top_values = _finite_values(top, f"opportunity_markout_{horizon_s}s_bps")
        realized_positive = [
            r for r in fills if safe_float(r, f"markout_{horizon_s}s_bps", math.nan) > 0.0
        ]
        realized_negative = [
            r for r in fills if safe_float(r, f"markout_{horizon_s}s_bps", math.nan) <= 0.0
        ]
        actual_mean = _safe_mean(actual_values)
        out.append(
            {
                "day": day,
                "side": side,
                "horizon_s": horizon_s,
                "orders": len(rows),
                "actual_filled_orders": k,
                "actual_fill_avg_markout_bps": f"{actual_mean:.6f}",
                "oracle_topk_opportunity_avg_markout_bps": f"{_safe_mean(top_values):.6f}",
                "oracle_topk_edge_vs_actual_bps": f"{_safe_mean(top_values) - actual_mean:.6f}",
                "oracle_topk_positive_rate": f"{_positive_rate(top_values):.6f}",
                "realized_positive_fills": len(realized_positive),
                "realized_positive_fill_rate": f"{len(realized_positive) / k if k else 0.0:.6f}",
                "realized_positive_avg_markout_bps": f"{_safe_mean(_finite_values(realized_positive, f'markout_{horizon_s}s_bps')):.6f}",
                "realized_negative_or_zero_fills": len(realized_negative),
                "realized_negative_or_zero_avg_markout_bps": f"{_safe_mean(_finite_values(realized_negative, f'markout_{horizon_s}s_bps')):.6f}",
                "note": "oracle_topk uses submit-time opportunity_markout for all placed orders; realized_positive is a fill-level hindsight upper bound",
            }
        )
    return out


def _positive_intersection_row(day: str, side: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    enriched: list[dict[str, Any]] = []
    xmarket_counter: Counter[str] = Counter()
    for row in rows:
        flags = _positive_intersection_flags(row)
        if flags["positive_intersection"]:
            merged = dict(row)
            merged.update(flags)
            enriched.append(merged)
        xmarket_counter[str(flags["xmarket_state_for_null"])] += 1
    stats = _order_group_stats(enriched)
    return {
        "day": day,
        "side": side,
        "candidate": "fill_quality_high+campaign_outcome_low+toxic_low+fill_probability_not_low+local_absorption+xmarket_not_adverse",
        "orders": stats["orders"],
        "filled_orders": stats["filled_orders"],
        "fill_rate": f"{stats['fill_rate']:.6f}",
        "avg_markout_20s_bps": f"{stats['avg_markout_20s_bps']:.6f}",
        "avg_markout_30s_bps": f"{stats['avg_markout_30s_bps']:.6f}",
        "positive_rate_30s": f"{stats['positive_rate_30s']:.6f}",
        "tail_rate_m50_30s": f"{stats['tail_rate_m50_30s']:.6f}",
        "avg_opportunity_markout_20s_bps": f"{_safe_mean(_finite_values(enriched, 'opportunity_markout_20s_bps')):.6f}",
        "avg_opportunity_markout_30s_bps": f"{_safe_mean(_finite_values(enriched, 'opportunity_markout_30s_bps')):.6f}",
        "avg_terminal_campaign_pnl": f"{stats['avg_terminal_campaign_pnl']:.6f}",
        "terminal_repair_rate": f"{stats['terminal_repair_rate']:.6f}",
        "terminal_tail_loss_rate": f"{stats['terminal_tail_loss_rate']:.6f}",
        "avg_terminal_outcome_risk_target": f"{stats['avg_terminal_outcome_risk_target']:.6f}",
        "avg_local_capacity_score": f"{_safe_mean([safe_float(r, 'local_capacity_score_for_null', math.nan) for r in enriched]):.6f}",
        "xmarket_unknown_orders_in_parent": xmarket_counter.get("unknown_as_pass", 0),
        "xmarket_known_not_adverse_orders_in_parent": xmarket_counter.get("not_adverse", 0),
        "xmarket_adverse_orders_in_parent": sum(
            v for k, v in xmarket_counter.items() if k.startswith("adverse")
        ),
        "note": "candidate is evidence only; xmarket_unknown means no ref shadow fields were present in the source order-level table",
    }


def _null_condition_memberships(row: dict[str, Any]) -> list[tuple[str, str]]:
    """Quote-time condition set for null-baseline alpha search.

    中文说明：这里的 condition 只允许使用 quote-time 可见字段。它不是
    policy，也不代表要 tighten/widen；它只是问这个状态里的 actual fills
    是否比全体 fills 更接近 random opportunity / oracle positive subset。
    """
    memberships: list[tuple[str, str]] = [("all", "baseline")]
    fill_probability = safe_float(row, "fill_probability_score", math.nan)
    fill_quality = safe_float(row, "fill_quality_score", math.nan)
    campaign_outcome = safe_float(row, "campaign_outcome_risk_score", math.nan)
    toxic = safe_float(row, "toxic_risk_score", math.nan)
    micro_reversion = safe_float(row, "micro_reversion_score", math.nan)
    trend_risk = safe_float(row, "trend_inventory_risk_score", math.nan)
    quote_distance_micro = safe_float(row, "quote_distance_micro", math.nan)
    micro_macro = safe_float(row, "micro_macro_range_ratio", math.nan)
    trend_eff_300 = safe_float(row, "trend_efficiency_300s", math.nan)
    near_depth = safe_float(row, "near_depth_total", math.nan)
    queue_rank = safe_float(row, "queue_local_rank", math.nan)
    refill_edge = safe_float(
        row,
        "sell_resil_refill_edge",
        safe_float(row, "l2_book_refresh_ratio", 0.0)
        - safe_float(row, "l2_book_cancel_ratio", 0.0),
    )
    local_ctx = _local_liquidity_context(row)
    local_capacity = safe_float(local_ctx, "capacity_score", math.nan)
    xmarket_ok, xmarket_state = _xmarket_not_adverse_for_null(row)

    if fill_quality >= 0.66:
        memberships.append(("fill_quality_high", "score"))
    if fill_quality >= 0.55:
        memberships.append(("fill_quality_mid_high", "score"))
    if campaign_outcome <= 0.40:
        memberships.append(("campaign_outcome_low", "score"))
    if campaign_outcome <= 0.50:
        memberships.append(("campaign_outcome_not_high", "score"))
    if toxic <= 0.33:
        memberships.append(("toxic_low", "score"))
    if fill_probability >= 0.33:
        memberships.append(("fill_probability_not_low", "score"))
    if fill_probability >= 0.50:
        memberships.append(("fill_probability_mid_high", "score"))
    if micro_reversion >= 0.66:
        memberships.append(("micro_reversion_high", "micro_macro"))
    if trend_risk <= 0.33:
        memberships.append(("trend_inventory_risk_low", "micro_macro"))
    if trend_risk <= 0.50:
        memberships.append(("trend_inventory_risk_not_high", "micro_macro"))
    if math.isfinite(quote_distance_micro):
        if quote_distance_micro <= 1.0:
            memberships.append(("quote_distance_micro_le1", "micro_macro"))
        elif quote_distance_micro <= 3.0:
            memberships.append(("quote_distance_micro_1_3", "micro_macro"))
    if math.isfinite(micro_macro):
        if micro_macro >= 0.75:
            memberships.append(("micro_macro_range_high", "micro_macro"))
        elif micro_macro <= 0.35:
            memberships.append(("micro_macro_range_low", "micro_macro"))
    if math.isfinite(trend_eff_300):
        if trend_eff_300 <= 0.30:
            memberships.append(("trend_efficiency_300s_low", "micro_macro"))
        elif trend_eff_300 >= 0.65:
            memberships.append(("trend_efficiency_300s_high", "micro_macro"))
    if math.isfinite(local_capacity):
        if local_capacity >= 0.66:
            memberships.append(("local_absorption_high", "local_liquidity"))
        if local_capacity >= 0.50:
            memberships.append(("local_absorption_mid_high", "local_liquidity"))
    if math.isfinite(near_depth):
        if near_depth >= 1.0:
            memberships.append(("near_depth_ge1", "local_liquidity"))
        if near_depth >= 2.0:
            memberships.append(("near_depth_ge2", "local_liquidity"))
    if math.isfinite(refill_edge):
        if refill_edge >= 0.0:
            memberships.append(("refill_edge_nonnegative", "local_liquidity"))
        if refill_edge >= 0.08:
            memberships.append(("refill_edge_gt8pct", "local_liquidity"))
        if refill_edge < 0.0:
            memberships.append(("refill_edge_negative", "local_liquidity"))
    if math.isfinite(queue_rank):
        if queue_rank <= 0.25:
            memberships.append(("queue_front_quartile", "queue"))
        elif queue_rank <= 0.50:
            memberships.append(("queue_front_half", "queue"))
        elif queue_rank >= 0.75:
            memberships.append(("queue_back_quartile", "queue"))
    if xmarket_ok:
        memberships.append(("xmarket_not_adverse", "xmarket"))
        if xmarket_state != "unknown_as_pass":
            memberships.append(("xmarket_known_not_adverse", "xmarket"))

    combos: list[tuple[str, str, bool]] = [
        (
            "quality_high+risk_low",
            "interaction",
            fill_quality >= 0.66 and campaign_outcome <= 0.40,
        ),
        (
            "quality_high+risk_low+toxic_low",
            "interaction",
            fill_quality >= 0.66 and campaign_outcome <= 0.40 and toxic <= 0.33,
        ),
        (
            "quality_high+risk_low+toxic_low+fill_prob_not_low",
            "interaction",
            fill_quality >= 0.66
            and campaign_outcome <= 0.40
            and toxic <= 0.33
            and fill_probability >= 0.33,
        ),
        (
            "quality_high+risk_low+toxic_low+local_absorption",
            "interaction",
            fill_quality >= 0.66
            and campaign_outcome <= 0.40
            and toxic <= 0.33
            and local_capacity >= 0.50,
        ),
        (
            "quality_high+risk_low+trend_low+local_absorption",
            "interaction",
            fill_quality >= 0.66
            and campaign_outcome <= 0.40
            and trend_risk <= 0.50
            and local_capacity >= 0.50,
        ),
        (
            "micro_reversion+trend_low+local_absorption",
            "interaction",
            micro_reversion >= 0.66 and trend_risk <= 0.50 and local_capacity >= 0.50,
        ),
        (
            "quality_high+risk_low+toxic_low+local_absorption+xmarket_ok",
            "interaction",
            fill_quality >= 0.66
            and campaign_outcome <= 0.40
            and toxic <= 0.33
            and local_capacity >= 0.50
            and xmarket_ok,
        ),
        (
            "positive_intersection_strict",
            "interaction",
            bool(_positive_intersection_flags(row)["positive_intersection"]),
        ),
    ]
    for name, family, ok in combos:
        if ok:
            memberships.append((name, family))
    # Preserve insertion order while removing duplicate names.
    seen: set[str] = set()
    unique: list[tuple[str, str]] = []
    for name, family in memberships:
        if name not in seen:
            seen.add(name)
            unique.append((name, family))
    return unique


def _condition_daily_rows_for_group(
    *,
    day: str,
    side: str,
    parent_rows: list[dict[str, Any]],
    random_trials: int,
    random_seed: int,
) -> list[dict[str, Any]]:
    condition_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in parent_rows:
        for name, family in _null_condition_memberships(row):
            condition_groups[(name, family)].append(row)

    parent_random = _random_null_rows_for_group(
        day=day,
        side=side,
        rows=parent_rows,
        trials=random_trials,
        seed=random_seed,
    )
    parent_by_horizon = {safe_int(r, "horizon_s"): r for r in parent_random}
    out: list[dict[str, Any]] = []
    for (condition, family), rows in sorted(condition_groups.items()):
        fills = [r for r in rows if safe_int(r, "filled") == 1]
        if len(rows) < 20:
            continue
        random_rows = _random_null_rows_for_group(
            day=day,
            side=side,
            rows=rows,
            trials=random_trials,
            seed=random_seed + 17,
        )
        oracle_rows = _oracle_rows_for_group(day, side, rows)
        random_by_horizon = {safe_int(r, "horizon_s"): r for r in random_rows}
        oracle_by_horizon = {safe_int(r, "horizon_s"): r for r in oracle_rows}
        for horizon_s in NULL_BASELINE_HORIZONS_S:
            rnd = random_by_horizon.get(horizon_s)
            parent = parent_by_horizon.get(horizon_s, {})
            oracle = oracle_by_horizon.get(horizon_s, {})
            actual_values = _finite_values(fills, f"markout_{horizon_s}s_bps")
            opp_values = _finite_values(rows, f"opportunity_markout_{horizon_s}s_bps")
            actual_avg = _safe_mean(actual_values)
            random_avg = safe_float(rnd or {}, "random_opportunity_avg_markout_mean_bps", math.nan)
            current_minus_random = (
                actual_avg - random_avg
                if math.isfinite(actual_avg) and math.isfinite(random_avg)
                else math.nan
            )
            parent_gap = safe_float(parent, "delta_actual_minus_random_mean_bps", math.nan)
            gap_improvement = (
                current_minus_random - parent_gap
                if math.isfinite(current_minus_random) and math.isfinite(parent_gap)
                else math.nan
            )
            stats = _order_group_stats(rows)
            out.append(
                {
                    "day": day,
                    "side": side,
                    "condition": condition,
                    "family": family,
                    "horizon_s": horizon_s,
                    "orders": len(rows),
                    "filled_orders": len(fills),
                    "fill_rate": f"{len(fills) / len(rows) if rows else 0.0:.6f}",
                    "actual_avg_markout_bps": f"{actual_avg:.6f}",
                    "random_opportunity_avg_markout_bps": f"{random_avg:.6f}"
                    if math.isfinite(random_avg)
                    else "",
                    "current_minus_random_bps": f"{current_minus_random:.6f}"
                    if math.isfinite(current_minus_random)
                    else "",
                    "parent_current_minus_random_bps": f"{parent_gap:.6f}"
                    if math.isfinite(parent_gap)
                    else "",
                    "gap_improvement_vs_parent_bps": f"{gap_improvement:.6f}"
                    if math.isfinite(gap_improvement)
                    else "",
                    "positive_rate": f"{_positive_rate(actual_values):.6f}",
                    "tail_rate_m50": f"{_tail_rate(actual_values):.6f}",
                    "opportunity_positive_rate": f"{_positive_rate(opp_values):.6f}",
                    "oracle_topk_edge_vs_actual_bps": _pick(
                        oracle, "oracle_topk_edge_vs_actual_bps"
                    ),
                    "oracle_topk_opportunity_avg_markout_bps": _pick(
                        oracle, "oracle_topk_opportunity_avg_markout_bps"
                    ),
                    "terminal_labeled_orders": stats["terminal_labeled_orders"],
                    "avg_terminal_campaign_pnl": f"{stats['avg_terminal_campaign_pnl']:.6f}",
                    "terminal_repair_rate": f"{stats['terminal_repair_rate']:.6f}",
                    "terminal_tail_loss_rate": f"{stats['terminal_tail_loss_rate']:.6f}",
                    "avg_terminal_outcome_risk_target": f"{stats['avg_terminal_outcome_risk_target']:.6f}",
                    "avg_campaign_max_abs_qty": f"{stats['avg_campaign_max_abs_qty']:.6f}",
                    "avg_campaign_mae": f"{stats['avg_campaign_mae']:.6f}",
                    "avg_quote_distance_micro": f"{stats['avg_quote_distance_micro']:.6f}",
                    "avg_micro_macro_range_ratio": f"{stats['avg_micro_macro_range_ratio']:.6f}",
                    "avg_trend_efficiency_300s": f"{stats['avg_trend_efficiency_300s']:.6f}",
                }
            )
    return out


def null_baseline_condition_summary_rows(
    daily_rows: list[dict[str, Any]],
    *,
    min_support_days: int = 3,
    min_total_fills: int = 20,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in daily_rows:
        side = norm_side(row.get("side", ""))
        condition = str(row.get("condition", ""))
        family = str(row.get("family", ""))
        horizon_s = safe_int(row, "horizon_s")
        if side in {"BUY", "SELL"} and condition and horizon_s:
            groups[(side, condition, family, horizon_s)].append(row)
    out: list[dict[str, Any]] = []
    for (side, condition, family, horizon_s), rows in sorted(groups.items()):
        support = [r for r in rows if safe_int(r, "filled_orders") > 0]
        days = len({str(r.get("day", "")) for r in rows if str(r.get("day", ""))})
        support_days = len({str(r.get("day", "")) for r in support if str(r.get("day", ""))})
        total_orders = sum(safe_int(r, "orders") for r in rows)
        total_fills = sum(safe_int(r, "filled_orders") for r in rows)
        gap_values = _finite_values(rows, "current_minus_random_bps")
        improvement_values = _finite_values(rows, "gap_improvement_vs_parent_bps")
        actual_values = _finite_values(rows, "actual_avg_markout_bps")
        random_values = _finite_values(rows, "random_opportunity_avg_markout_bps")
        oracle_values = _finite_values(rows, "oracle_topk_edge_vs_actual_bps")
        pass_like_days = sum(
            1 for r in rows if safe_float(r, "gap_improvement_vs_parent_bps", math.nan) > 0.0
        )
        close_to_random_days = sum(
            1 for r in rows if safe_float(r, "current_minus_random_bps", math.nan) >= -1.0
        )
        positive_markout_days = sum(
            1 for r in rows if safe_float(r, "actual_avg_markout_bps", math.nan) > 0.0
        )
        candidate_grade = "insufficient"
        if support_days >= min_support_days and total_fills >= min_total_fills:
            if _safe_mean(gap_values) >= -1.0 and _safe_mean(actual_values) > 0.0:
                candidate_grade = "near_random_positive"
            elif _safe_mean(improvement_values) > 1.0 and positive_markout_days >= max(
                1, support_days // 2
            ):
                candidate_grade = "improves_selection"
            elif _safe_mean(improvement_values) > 0.0:
                candidate_grade = "weak_improvement"
            else:
                candidate_grade = "no_improvement"
        out.append(
            {
                "side": side,
                "condition": condition,
                "family": family,
                "horizon_s": horizon_s,
                "days": days,
                "support_days": support_days,
                "total_orders": total_orders,
                "total_fills": total_fills,
                "fill_rate": f"{total_fills / total_orders if total_orders else 0.0:.6f}",
                "avg_actual_markout_bps": f"{_safe_mean(actual_values):.6f}",
                "avg_random_opportunity_markout_bps": f"{_safe_mean(random_values):.6f}",
                "avg_current_minus_random_bps": f"{_safe_mean(gap_values):.6f}",
                "median_current_minus_random_bps": f"{_safe_quantile(gap_values, 0.5):.6f}",
                "avg_gap_improvement_vs_parent_bps": f"{_safe_mean(improvement_values):.6f}",
                "median_gap_improvement_vs_parent_bps": f"{_safe_quantile(improvement_values, 0.5):.6f}",
                "pass_like_days": pass_like_days,
                "close_to_random_days": close_to_random_days,
                "positive_markout_days": positive_markout_days,
                "avg_positive_rate": f"{_safe_mean(_finite_values(rows, 'positive_rate')):.6f}",
                "avg_tail_rate_m50": f"{_safe_mean(_finite_values(rows, 'tail_rate_m50')):.6f}",
                "avg_oracle_topk_edge_vs_actual_bps": f"{_safe_mean(oracle_values):.6f}",
                "avg_terminal_campaign_pnl": f"{_safe_mean(_finite_values(rows, 'avg_terminal_campaign_pnl')):.6f}",
                "avg_terminal_repair_rate": f"{_safe_mean(_finite_values(rows, 'terminal_repair_rate')):.6f}",
                "avg_terminal_tail_loss_rate": f"{_safe_mean(_finite_values(rows, 'terminal_tail_loss_rate')):.6f}",
                "avg_terminal_outcome_risk_target": f"{_safe_mean(_finite_values(rows, 'avg_terminal_outcome_risk_target')):.6f}",
                "candidate_grade": candidate_grade,
            }
        )
    out.sort(
        key=lambda r: (
            {
                "near_random_positive": 0,
                "improves_selection": 1,
                "weak_improvement": 2,
                "no_improvement": 3,
                "insufficient": 4,
            }.get(str(r["candidate_grade"]), 5),
            -safe_float(r, "avg_gap_improvement_vs_parent_bps", -1e9),
            -safe_float(r, "total_fills", 0.0),
        )
    )
    return out


def null_baseline_tables(
    order_rows: list[dict[str, Any]],
    *,
    random_trials: int = 64,
    random_seed: int = 20260706,
) -> dict[str, list[dict[str, Any]]]:
    """Build null/random/oracle evidence from a shared order-level table.

    中文说明：这不是一个新策略，也不是完整随机报价 replay。它在同一批
    placed orders 上建立三层参照：
    1. current actual fills: 当前 baseline 实际被打到的 fill selection；
    2. random opportunity null: 同 day/side、同 fill 数量，随机抽 placed
       orders，用 submit-time opportunity_markout 衡量无选择机会质量；
    3. oracle upper bound: 事后选择 opportunity/top-k 或 realized positive
       fills，估计这个分母里到底有没有足够正 edge。
    """
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in order_rows:
        day = str(row.get("day", ""))
        side = norm_side(row.get("side", ""))
        if day and side in {"BUY", "SELL"}:
            groups[(day, side)].append(row)

    current: list[dict[str, Any]] = []
    random_rows: list[dict[str, Any]] = []
    oracle: list[dict[str, Any]] = []
    positive: list[dict[str, Any]] = []
    condition_daily: list[dict[str, Any]] = []
    for (day, side), rows in sorted(groups.items()):
        current.append(_current_group_row(day, side, rows))
        random_rows.extend(
            _random_null_rows_for_group(
                day=day,
                side=side,
                rows=rows,
                trials=random_trials,
                seed=random_seed,
            )
        )
        oracle.extend(_oracle_rows_for_group(day, side, rows))
        positive.append(_positive_intersection_row(day, side, rows))
        condition_daily.extend(
            _condition_daily_rows_for_group(
                day=day,
                side=side,
                parent_rows=rows,
                random_trials=random_trials,
                random_seed=random_seed,
            )
        )

    def _aggregate_by_side(table: list[dict[str, Any]], table_name: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for side in ("BUY", "SELL"):
            side_rows = [r for r in table if r.get("side") == side]
            if not side_rows:
                continue
            horizons = sorted(
                {safe_int(r, "horizon_s") for r in side_rows if str(r.get("horizon_s", "")).strip()}
            )
            row_groups = (
                [(0, side_rows)]
                if not horizons
                else [
                    (h, [r for r in side_rows if safe_int(r, "horizon_s") == h]) for h in horizons
                ]
            )
            for horizon_s, group_rows in row_groups:
                row: dict[str, Any] = {
                    "table": table_name,
                    "side": side,
                    "horizon_s": "" if horizon_s == 0 else horizon_s,
                    "days": len(
                        {str(r.get("day", "")) for r in group_rows if str(r.get("day", ""))}
                    ),
                }
                for key in (
                    "orders",
                    "filled_orders",
                    "actual_filled_orders",
                    "random_sample_size",
                    "realized_positive_fills",
                    "realized_negative_or_zero_fills",
                ):
                    values = [
                        safe_float(r, key, math.nan)
                        for r in group_rows
                        if math.isfinite(safe_float(r, key, math.nan))
                    ]
                    if values:
                        row[f"sum_{key}"] = f"{sum(values):.6f}"
                for key in (
                    "fill_rate",
                    "actual_fill_avg_markout_20s_bps",
                    "actual_fill_avg_markout_30s_bps",
                    "delta_actual_minus_random_mean_bps",
                    "oracle_topk_edge_vs_actual_bps",
                    "avg_markout_30s_bps",
                    "avg_terminal_campaign_pnl",
                    "terminal_repair_rate",
                    "terminal_tail_loss_rate",
                    "avg_terminal_outcome_risk_target",
                ):
                    values = [
                        safe_float(r, key, math.nan)
                        for r in group_rows
                        if math.isfinite(safe_float(r, key, math.nan))
                    ]
                    if values:
                        row[f"avg_{key}"] = f"{_safe_mean(values):.6f}"
                out.append(row)
        return out

    aggregate = []
    aggregate.extend(_aggregate_by_side(current, "current_actual"))
    aggregate.extend(_aggregate_by_side(random_rows, "random_opportunity_null"))
    aggregate.extend(_aggregate_by_side(oracle, "oracle"))
    aggregate.extend(_aggregate_by_side(positive, "positive_intersection"))
    return {
        "current_daily": current,
        "random_daily": random_rows,
        "oracle_daily": oracle,
        "positive_intersection_daily": positive,
        "condition_daily": condition_daily,
        "condition_summary": null_baseline_condition_summary_rows(condition_daily),
        "aggregate": aggregate,
    }


def null_baseline_summary(tables: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    random_rows = tables.get("random_daily", [])
    oracle_rows = tables.get("oracle_daily", [])
    positive_rows = tables.get("positive_intersection_daily", [])
    out: dict[str, Any] = {
        "current_daily_rows": len(tables.get("current_daily", [])),
        "random_daily_rows": len(random_rows),
        "oracle_daily_rows": len(oracle_rows),
        "positive_intersection_daily_rows": len(positive_rows),
        "interpretation": (
            "current-vs-random compares actual fill markout with submit-time random opportunity markout; "
            "oracle is an upper-bound diagnostic, not a tradable policy"
        ),
    }
    for side in ("BUY", "SELL"):
        side_random = [
            r for r in random_rows if r.get("side") == side and safe_int(r, "horizon_s") == 30
        ]
        side_oracle = [
            r for r in oracle_rows if r.get("side") == side and safe_int(r, "horizon_s") == 30
        ]
        side_pos = [r for r in positive_rows if r.get("side") == side]
        out[f"{side.lower()}_avg_current_minus_random_30s_bps"] = _safe_mean(
            [safe_float(r, "delta_actual_minus_random_mean_bps", math.nan) for r in side_random]
        )
        out[f"{side.lower()}_avg_oracle_topk_edge_30s_bps"] = _safe_mean(
            [safe_float(r, "oracle_topk_edge_vs_actual_bps", math.nan) for r in side_oracle]
        )
        out[f"{side.lower()}_positive_intersection_orders"] = sum(
            safe_int(r, "orders") for r in side_pos
        )
        out[f"{side.lower()}_positive_intersection_fills"] = sum(
            safe_int(r, "filled_orders") for r in side_pos
        )
        out[f"{side.lower()}_positive_intersection_avg_markout_30s_bps"] = _safe_mean(
            [
                safe_float(r, "avg_markout_30s_bps", math.nan)
                for r in side_pos
                if safe_int(r, "filled_orders") > 0
            ]
        )
    return out


def _bool_text(row: dict[str, Any], key: str) -> bool:
    text = str(row.get(key, "")).strip().lower()
    return text in {"1", "true", "yes"}


def _replay_reason_text(row: dict[str, Any]) -> str:
    reasons: list[str] = []
    if _bool_text(row, "side_adverse") or _bool_text(row, "side_adverse_pause"):
        reasons.append("adverse")
    if _bool_text(row, "adverse_markout"):
        reasons.append("markout")
    if _bool_text(row, "adverse_thin_depth"):
        reasons.append("thin_depth")
    if _bool_text(row, "defense_guard") or _bool_text(row, "defense_pause"):
        reasons.append("defense")
    if _bool_text(row, "local_extreme_guard") or _bool_text(row, "local_extreme_pause"):
        reasons.append("local_extreme")
    return "|".join(reasons) if reasons else "none"


def replay_order_level_rows(
    *,
    replay_order_rows: list[dict[str, Any]],
    replay_fill_rows: list[dict[str, Any]] | None = None,
    attach_terminal_campaign_labels: bool = True,
) -> list[dict[str, Any]]:
    """Convert tick replay trace orders into the shared order-level schema.

    中文说明：这里会从 replay fills 重建 quote-time campaign 状态。每一
    行 order 只看到 submit 前已经发生的 fills 和当前 mid，因此可用于
    campaign risk 的 historical OOS，而不是用成交后的信息作弊。
    """
    fills_by_order: dict[str, dict[str, Any]] = {}
    for row in replay_fill_rows or []:
        oid = str(row.get("order_id", ""))
        if oid:
            fills_by_order[oid] = row

    orders_by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    order_side_by_id: dict[str, str] = {}
    order_qty_by_id: dict[str, float] = {}
    for row in replay_order_rows:
        side = norm_side(row.get("side", ""))
        oid = str(row.get("order_id", ""))
        if side not in {"BUY", "SELL"} or not oid:
            continue
        ts = _norm_ms_ts(row, "submit_ts", "quote_ts")
        day = str(row.get("day") or utc_day(ts))
        orders_by_day[day].append(row)
        order_side_by_id[oid] = side
        order_qty_by_id[oid] = safe_float(row, "fill_qty", safe_float(row, "quantity", 0.0))

    fills_by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in replay_fill_rows or []:
        ts = _norm_ms_ts(row, "fill_ts")
        day = str(row.get("day") or utc_day(ts))
        fills_by_day[day].append(row)

    # Fill-time inventory can differ from quote-time inventory while an order
    # rests.  Build an exact/fallback role map before constructing order rows.
    # Daily replay is fresh-start, so old traces without the new exact field can
    # still be reconstructed causally from all fills in that UTC day.
    fill_role_by_order: dict[str, tuple[float, str, str]] = {}
    for _day_value, day_fills in fills_by_day.items():
        running_q = 0.0
        for fill_row in sorted(day_fills, key=lambda r: _norm_ms_ts(r, "fill_ts")):
            oid = str(fill_row.get("order_id", ""))
            side = norm_side(fill_row.get("side", "")) or order_side_by_id.get(oid, "")
            qty = safe_float(
                fill_row,
                "fill_qty",
                safe_float(fill_row, "quantity", order_qty_by_id.get(oid, 0.0)),
            )
            if side not in {"BUY", "SELL"} or qty <= 0.0:
                continue
            exact_q = _first_finite(
                fill_row, "inventory_before_fill", "fill_q_before", "q_before_fill"
            )
            if math.isfinite(exact_q):
                q_before_fill = exact_q
                running_q = exact_q
                source = "exact_trace"
            else:
                q_before_fill = running_q
                source = "reconstructed_daily"
            if oid and oid not in fill_role_by_order:
                fill_role_by_order[oid] = (
                    q_before_fill,
                    inventory_role(side, q_before_fill),
                    source,
                )
            running_q += qty if side == "BUY" else -qty

    out: list[dict[str, Any]] = []
    for day in sorted(orders_by_day):
        day_orders = sorted(
            orders_by_day[day],
            key=lambda r: (_norm_ms_ts(r, "submit_ts", "quote_ts"), str(r.get("order_id", ""))),
        )
        day_fills = sorted(fills_by_day.get(day, []), key=lambda r: _norm_ms_ts(r, "fill_ts"))
        day_mid_seen: set[tuple[float, float]] = set()
        day_mid_series: list[tuple[float, float]] = []
        for mid_row in day_orders:
            mts = _norm_ms_ts(mid_row, "submit_ts", "quote_ts")
            mmid = safe_float(mid_row, "mid", math.nan)
            key = (mts, mmid)
            if mts > 0.0 and mmid > 0.0 and key not in day_mid_seen:
                day_mid_seen.add(key)
                day_mid_series.append((mts, mmid))
        day_mid_series.sort(key=lambda x: x[0])
        day_mid_ts = [x[0] for x in day_mid_series]
        fill_idx = 0
        tracker = ReplayCampaignTracker()

        for row in day_orders:
            oid = str(row.get("order_id", ""))
            side = norm_side(row.get("side", ""))
            ts = _norm_ms_ts(row, "submit_ts", "quote_ts")
            while fill_idx < len(day_fills) and _norm_ms_ts(day_fills[fill_idx], "fill_ts") < ts:
                tracker.apply_fill(day_fills[fill_idx])
                fill_idx += 1

            fill = fills_by_order.get(oid, {})
            filled = int(
                str(row.get("outcome", "")).lower() == "fill" or safe_float(row, "fill_qty") > 0.0
            )
            fill_ts = _norm_ms_ts(fill, "fill_ts") or _norm_ms_ts(row, "outcome_ts")
            mid = safe_float(row, "mid", safe_float(fill, "quote_mid", math.nan))
            price = safe_float(
                row, "final_price", safe_float(row, "price", safe_float(fill, "quote_px", math.nan))
            )
            qty = safe_float(row, "quantity", safe_float(row, "fill_qty", 0.0))
            distance = safe_float(
                row, "final_distance_to_mid", safe_float(fill, "quote_dist", math.nan)
            )
            distance_bps = (
                distance / mid * 10_000.0 if mid > 0.0 and math.isfinite(distance) else math.nan
            )
            best_bid = safe_float(row, "best_bid", math.nan)
            best_ask = safe_float(row, "best_ask", math.nan)
            exact_l2_spread_bps = (
                (best_ask - best_bid) / mid * 10_000.0
                if mid > 0.0 and best_ask > 0.0 and best_bid > 0.0 and best_ask >= best_bid
                else math.nan
            )
            ttl_budget_ms = safe_float(row, "xmarket_retreat_ttl_ms", math.nan)
            inv_raw = safe_float(row, "inventory", math.nan)
            tracker.ensure_from_order_inventory(ts=ts, q_before=inv_raw, mid=mid)
            snap = tracker.snapshot(ts, mid)
            q_before = inv_raw if math.isfinite(inv_raw) else snap.q
            signed_qty = qty if side == "BUY" else -qty
            quote_inventory_role = inventory_role(side, q_before)
            exposure_increasing = int(abs(q_before + signed_qty) > abs(q_before) + EPS)
            fill_role_tuple = fill_role_by_order.get(oid)
            fill_q_before = fill_role_tuple[0] if fill_role_tuple else math.nan
            fill_inventory_role = (
                fill_role_tuple[1] if fill_role_tuple else ("unknown" if filled else "")
            )
            fill_role_source = (
                fill_role_tuple[2] if fill_role_tuple else ("unknown" if filled else "")
            )
            campaign_active = int(snap.active or abs(q_before) > EPS)
            campaign_age_s = snap.age_s if snap.active else 0.0
            result: dict[str, Any] = {
                "timestamp": f"{ts:.3f}",
                "utc": utc_text(ts),
                "day": str(row.get("day") or utc_day(ts)),
                "session_stack": session_stack(ts),
                "client_order_id": oid,
                "side": side,
                "event_type": "replay_order",
                "outcome_event": row.get("outcome", ""),
                "filled": filled,
                "fill_ts": f"{fill_ts:.3f}" if fill_ts else "",
                "fill_utc": utc_text(fill_ts) if fill_ts else "",
                "fill_age_ms": f"{safe_float(row, 'lifetime_ms', safe_float(fill, 'age_ms')):.3f}",
                "filled_qty": f"{safe_float(row, 'fill_qty', safe_float(fill, 'fill_qty')):.6f}",
                "avg_fill_price": _pick(fill, "fill_trade_px"),
                "price": f"{price:.4f}" if price > 0.0 else "",
                "quantity": f"{qty:.6f}",
                "mid": f"{mid:.4f}" if mid > 0.0 else "",
                "quote_distance": f"{distance:.6f}" if math.isfinite(distance) else "",
                "quote_distance_bps": f"{distance_bps:.6f}" if math.isfinite(distance_bps) else "",
                "mode": "replay",
                "reason_mask": "",
                "reason_text": _replay_reason_text(row),
                "spread_mult": _pick(row, "defense_spread_mult", default="1.0"),
                "size_mult": "1.0",
                "toxicity": f"{safe_float(row, 'tox_bid' if side == 'BUY' else 'tox_ask'):.6f}",
                "markout_ema": f"{safe_float(row, 'mo_ema_bid' if side == 'BUY' else 'mo_ema_ask'):.6f}",
                "depth_age_s": "0.000000",
                "microprice_shift_bps": f"{safe_float(row, 'microprice_shift_bps'):.6f}",
                "l2_quote_flip_rate": f"{safe_float(row, 'l2_quote_flip_rate'):.6f}",
                "l2_book_refresh_ratio": f"{safe_float(row, 'l2_book_refresh_ratio'):.6f}",
                "l2_book_cancel_ratio": f"{safe_float(row, 'l2_book_cancel_ratio'):.6f}",
                "near_depth_total": f"{safe_float(row, 'near_depth_total'):.6f}",
                "exact_l2_spread_bps": f"{exact_l2_spread_bps:.6f}"
                if math.isfinite(exact_l2_spread_bps)
                else "",
                "queue_init": f"{safe_float(row, 'queue_init', math.nan):.6f}"
                if math.isfinite(safe_float(row, "queue_init", math.nan))
                else "",
                "queue_left": f"{safe_float(row, 'queue_left', math.nan):.6f}"
                if math.isfinite(safe_float(row, "queue_left", math.nan))
                else "",
                "queue_regime_mult": f"{safe_float(row, 'queue_regime_mult'):.6f}",
                "queue_mo_mult": f"{safe_float(row, 'queue_mo_mult'):.6f}",
                "queue_deplete_mult": f"{safe_float(row, 'queue_deplete_mult'):.6f}",
                "fill_eligible": _pick(row, "fill_eligible"),
                "ttl_budget_ms": f"{ttl_budget_ms:.3f}" if math.isfinite(ttl_budget_ms) else "",
                "observed_lifetime_ms": f"{safe_float(row, 'lifetime_ms', safe_float(fill, 'age_ms', math.nan)):.3f}"
                if math.isfinite(
                    safe_float(row, "lifetime_ms", safe_float(fill, "age_ms", math.nan))
                )
                else "",
                "bid_quote_fill_prob": f"{safe_float(row, 'bid_quote_fill_prob'):.6f}",
                "bid_quote_fill_markout_30s": f"{safe_float(row, 'bid_quote_fill_markout_30s'):.6f}",
                "ask_quote_fill_prob": f"{safe_float(row, 'ask_quote_fill_prob'):.6f}",
                "ask_quote_fill_markout_30s": f"{safe_float(row, 'ask_quote_fill_markout_30s'):.6f}",
                "side_quote_fill_prob": f"{safe_float(row, 'bid_quote_fill_prob' if side == 'BUY' else 'ask_quote_fill_prob'):.6f}",
                "side_quote_fill_markout_30s": f"{safe_float(row, 'bid_quote_fill_markout_30s' if side == 'BUY' else 'ask_quote_fill_markout_30s'):.6f}",
                "queue_local_rank": f"{safe_float(row, 'queue_local_rank'):.6f}",
                "quote_action": "place" if row.get("outcome") else "",
                "quote_allow_post": "1",
                "quote_allow_exposure_increase": "1",
                "base_price": _pick(row, "raw_price"),
                "final_price": _pick(row, "final_price", "price"),
                "campaign_active": campaign_active,
                "campaign_id": snap.campaign_id if snap.active else "",
                "q_before": f"{q_before:.6f}" if math.isfinite(q_before) else "",
                "inventory_role": quote_inventory_role,
                "inventory_role_quote": quote_inventory_role,
                "order_add_on": int(quote_inventory_role == "add"),
                "fill_q_before": f"{fill_q_before:.6f}" if math.isfinite(fill_q_before) else "",
                "fill_inventory_role": fill_inventory_role,
                "fill_role_source": fill_role_source,
                "inventory_role_drift": int(
                    filled
                    and fill_inventory_role not in {"", "unknown"}
                    and fill_inventory_role != quote_inventory_role
                ),
                "campaign_side": snap.side
                if snap.active
                else "LONG"
                if q_before > EPS
                else "SHORT"
                if q_before < -EPS
                else "FLAT",
                "campaign_age_s": f"{campaign_age_s:.6f}",
                "campaign_duration_s": f"{snap.duration_s:.6f}" if snap.active else "0.000000",
                "campaign_max_abs_qty": f"{max(snap.max_abs_qty, abs(q_before) if math.isfinite(q_before) else 0.0):.6f}",
                "campaign_total_pnl": f"{snap.total_pnl:.6f}" if snap.active else "0.000000",
                "campaign_adverse_excursion": f"{snap.adverse_excursion:.6f}"
                if snap.active
                else "0.000000",
                "campaign_exposure_increasing_fills": snap.exposure_increasing_fills
                if snap.active
                else "",
                "campaign_reducing_fills": snap.reducing_fills if snap.active else "",
                "order_exposure_increasing": exposure_increasing,
                "shadow_block_inv006": int(
                    math.isfinite(q_before) and abs(q_before) >= 0.006 and exposure_increasing
                ),
                "shadow_block_age60m": int(
                    campaign_active and campaign_age_s >= 3600.0 and exposure_increasing
                ),
                "shadow_block_reducing_only": int(
                    math.isfinite(q_before) and abs(q_before) > EPS and exposure_increasing
                ),
                "sell_resil_hit": 0,
                "sell_resil_flow_decel": "",
                "sell_resil_rank": _pick(row, "local_extreme_rank", "queue_local_rank"),
                "sell_resil_refill_edge": "",
                "sell_resil_ref_adv": "",
                "sell_resil_spot_adv": "",
                "sell_resil_spot_available": "",
                "opportunity_markout_1s_bps": "",
                "opportunity_markout_5s_bps": "",
                "opportunity_markout_20s_bps": "",
                "opportunity_markout_30s_bps": "",
                "markout_1s_bps": "",
                "markout_5s_bps": "",
                "markout_20s_bps": "",
                "markout_30s_bps": "",
            }
            path_features = _order_path_features(
                ts=ts,
                side=side,
                mid=mid,
                quote_distance_bps=distance_bps,
                mid_series=day_mid_series,
                mid_ts=day_mid_ts,
            )
            for key, value in path_features.items():
                result[key] = _fmt_path_feature(value)
            # Existing replay decomposition emits markout in quote currency.
            # Convert to bps so live/replay score summaries share a unit.
            fill_px = safe_float(fill, "fill_trade_px", price)
            for horizon_s in (1, 5, 20, 30):
                if ts > 0.0 and price > 0.0:
                    opportunity_markout = _maker_signed_markout_bps(
                        side,
                        price,
                        _future_mid(day_mid_series, day_mid_ts, ts + horizon_s),
                    )
                    if math.isfinite(opportunity_markout):
                        result[f"opportunity_markout_{horizon_s}s_bps"] = (
                            f"{opportunity_markout:.6f}"
                        )
                raw = safe_float(fill, f"markout_{horizon_s}s", math.nan)
                if math.isfinite(raw) and fill_px > 0.0:
                    result[f"markout_{horizon_s}s_bps"] = f"{raw / fill_px * 10_000.0:.6f}"
            scores = _order_scores(result)
            for key, value in scores.items():
                result[key] = f"{value:.6f}" if isinstance(value, float) else value
            out.append(result)
    if not attach_terminal_campaign_labels or not replay_fill_rows:
        return out

    labels: list[dict[str, Any]] = []
    for day, day_fills in fills_by_day.items():
        # 中文说明：xmarket/order-level evidence 需要 terminal campaign PnL /
        # repair rate 做目标；这些 label 只能用于事后校准，不能回流到
        # quote-time 条件。这里按 UTC day fresh-start 复用 replay fill trace，
        # 与当前日度回测边界一致。
        ledger: list[TradeRow] = []
        q = 0.0
        cash = 0.0
        for raw in sorted(day_fills, key=lambda r: _norm_ms_ts(r, "fill_ts")):
            side = norm_side(raw.get("side", ""))
            qty = safe_float(raw, "fill_qty", safe_float(raw, "quantity", 0.0))
            px = safe_float(
                raw, "fill_trade_px", safe_float(raw, "quote_px", safe_float(raw, "price", 0.0))
            )
            ts = _norm_ms_ts(raw, "fill_ts")
            if side not in {"BUY", "SELL"} or qty <= 0.0 or px <= 0.0 or ts <= 0.0:
                continue
            signed_qty = qty if side == "BUY" else -qty
            cash -= signed_qty * px
            q += signed_qty
            ledger.append(
                TradeRow(
                    ts=ts,
                    side=side,
                    trade_type="FILL",
                    qty=qty,
                    price=px,
                    position=q,
                    realized_pnl=cash,
                    unrealized_pnl=q * px,
                )
            )
        if not ledger:
            continue
        labels.extend(campaign_label_rows(build_campaigns(ledger)))
    return attach_campaign_labels_to_orders(out, labels)


def _safe_quantile(values: list[float], q: float) -> float:
    clean = sorted(v for v in values if math.isfinite(v))
    if not clean:
        return math.nan
    q = max(0.0, min(1.0, q))
    pos = (len(clean) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return clean[lo]
    frac = pos - lo
    return clean[lo] * (1.0 - frac) + clean[hi] * frac


def _bucket_value(
    value: float, edges: tuple[float, ...], labels: tuple[str, ...], *, missing: str = "missing"
) -> str:
    if not math.isfinite(value):
        return missing
    for idx in range(len(labels)):
        if edges[idx] <= value < edges[idx + 1]:
            return labels[idx]
    return labels[-1]


def _local_liquidity_bucket(row: dict[str, Any]) -> str:
    side = norm_side(row.get("side", ""))
    distance = abs(safe_float(row, "quote_distance_bps", math.nan))
    depth = safe_float(row, "near_depth_total", math.nan)
    queue_rank = safe_float(row, "queue_local_rank", math.nan)
    taker_flow = safe_float(row, "taker_flow_adverse", math.nan)
    distance_bucket = _bucket_value(
        distance,
        (float("-inf"), 10.0, 20.0, 30.0, 40.0, 60.0, float("inf")),
        ("dist_lt10", "dist_10_20", "dist_20_30", "dist_30_40", "dist_40_60", "dist_ge60"),
    )
    depth_bucket = _bucket_value(
        depth,
        (float("-inf"), 0.5, 1.0, 2.0, 5.0, float("inf")),
        ("depth_lt0p5", "depth_0p5_1", "depth_1_2", "depth_2_5", "depth_ge5"),
    )
    rank_bucket = _bucket_value(
        queue_rank,
        (float("-inf"), 0.25, 0.50, 0.75, 1.01, float("inf")),
        ("rank_front_0_25", "rank_0_25_0_50", "rank_0_50_0_75", "rank_back_0_75_1", "rank_gt1"),
    )
    if math.isfinite(taker_flow):
        flow_bucket = _bucket_value(
            taker_flow,
            (float("-inf"), 0.05, 0.15, 0.35, float("inf")),
            ("flow_absent", "flow_mild", "flow_adverse", "flow_strong_adverse"),
        )
    else:
        reason = str(row.get("reason_text", "")).lower()
        flow_bucket = "flow_adverse_unknown" if "adverse" in reason else "flow_unknown"
    return "|".join(
        [side or "side_unknown", distance_bucket, depth_bucket, rank_bucket, flow_bucket]
    )


def _local_liquidity_context(row: dict[str, Any]) -> dict[str, float | str | int]:
    depth = safe_float(row, "near_depth_total", math.nan)
    refresh = safe_float(row, "l2_book_refresh_ratio", 0.0)
    cancel = safe_float(row, "l2_book_cancel_ratio", 0.0)
    refill_edge = refresh - cancel
    queue_deplete = safe_float(row, "queue_deplete_mult", 1.0)
    queue_mo = safe_float(row, "queue_mo_mult", 1.0)
    taker_flow = safe_float(row, "taker_flow_adverse", 0.0)
    ref_adv = max(
        0.0, safe_float(row, "sell_resil_ref_adv", safe_float(row, "quote_ref_adverse_ret", 0.0))
    )
    spot_adv = max(
        0.0, safe_float(row, "sell_resil_spot_adv", safe_float(row, "quote_spot_adverse_ret", 0.0))
    )

    depth_score = (
        _clip01(math.log1p(max(depth, 0.0)) / math.log1p(5.0)) if math.isfinite(depth) else 0.0
    )
    refill_score = _clip01(0.5 + refill_edge)
    queue_score = _clip01((0.5 * max(queue_deplete, 0.0) + 0.5 * max(queue_mo, 0.0)) / 1.5)
    flow_absorption_score = _clip01(1.0 - max(taker_flow, 0.0) / 0.35)
    xmarket_penalty = _clip01((ref_adv + spot_adv) / max(2e-5 * 2.5, 1e-12))
    capacity = _clip01(
        0.35 * depth_score
        + 0.25 * refill_score
        + 0.20 * queue_score
        + 0.20 * flow_absorption_score
        - 0.25 * xmarket_penalty
    )
    return {
        "local_bucket": _local_liquidity_bucket(row),
        "capacity_score": capacity,
        "refill_edge": refill_edge,
        "refill_or_stable": int(refill_edge >= -0.05 and queue_deplete >= 0.85),
        "depth_vanish": int(refill_edge < -0.15 or queue_deplete < 0.75),
        "xmarket_adverse": int((ref_adv + spot_adv) > 0.0),
        "quote_time_depth": depth,
        "queue_deplete_mult": queue_deplete,
        "flow_absorption_score": flow_absorption_score,
    }


def _response_half_life_from_path(path: list[tuple[float, float]]) -> float:
    if not path:
        return math.inf
    path = sorted(path, key=lambda x: x[0])
    first_t, first_v = path[0]
    if first_v >= 0.0:
        return 0.0
    target = first_v * 0.5
    last_t, last_v = first_t, first_v
    for t, v in path[1:]:
        if v >= target:
            denom = v - last_v
            if abs(denom) <= 1e-12:
                return max(0.0, t - first_t)
            ratio = (target - last_v) / denom
            return max(0.0, (last_t + ratio * (t - last_t)) - first_t)
        last_t, last_v = t, v
    return math.inf


def _avg_markout_path(rows: list[dict[str, Any]]) -> list[tuple[float, float]]:
    path: list[tuple[float, float]] = []
    for horizon_s in (1, 5, 20, 30):
        key = f"markout_{horizon_s}s_bps"
        values = [safe_float(r, key, math.nan) for r in rows]
        mean = _fill_qty_weighted_mean(rows, key)
        if any(math.isfinite(v) for v in values):
            path.append((float(horizon_s), mean))
    return path


def local_liquidity_mechanism_tables(
    order_rows: list[dict[str, Any]],
    *,
    min_fills: int = 30,
    min_daily_fills: int = 5,
    holding_budget_s: float = 20.0,
    max_xmarket_adverse_rate: float = 0.25,
) -> dict[str, list[dict[str, Any]]]:
    """Build unified local-liquidity response/reversion/capacity evidence.

    中文说明：这是旧 local-liquidity 独立脚本的共享输出版本。输入必须是
    统一 order-level schema，所以 response、OU-style recovery、
    absorptive capacity 与 xmarket moderator 共用同一套 quote-time 字段。
    """
    enriched: list[dict[str, Any]] = []
    for row in order_rows:
        side = norm_side(row.get("side", ""))
        if side not in {"BUY", "SELL"}:
            continue
        ctx = _local_liquidity_context(row)
        merged = dict(row)
        merged.update(ctx)
        enriched.append(merged)

    capacity_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    fill_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    daily_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in enriched:
        bucket = str(row["local_bucket"])
        capacity_groups[bucket].append(row)
        filled = safe_int(row, "filled")
        markout30 = safe_float(row, "markout_30s_bps", math.nan)
        if filled and math.isfinite(markout30):
            fill_groups[bucket].append(row)
            daily_groups[(str(row.get("day", "")), bucket)].append(row)

    order_capacity_rows: list[dict[str, Any]] = []
    for bucket, rows in capacity_groups.items():
        capacity_values = [safe_float(r, "capacity_score", math.nan) for r in rows]
        order_capacity_rows.append(
            {
                "local_bucket": bucket,
                "placed_orders": len(rows),
                "placed_days": len({str(r.get("day", "")) for r in rows}),
                "placed_avg_capacity_score": f"{_safe_mean(capacity_values):.6f}",
                "placed_median_capacity_score": f"{_safe_quantile(capacity_values, 0.5):.6f}",
                "placed_refill_or_stable_rate": f"{_safe_mean([safe_float(r, 'refill_or_stable') for r in rows]):.6f}",
                "placed_depth_vanish_rate": f"{_safe_mean([safe_float(r, 'depth_vanish') for r in rows]):.6f}",
                "placed_xmarket_adverse_rate": f"{_safe_mean([safe_float(r, 'xmarket_adverse') for r in rows]):.6f}",
                "placed_avg_quote_time_depth": f"{_safe_mean([safe_float(r, 'quote_time_depth', math.nan) for r in rows]):.6f}",
                "placed_avg_refill_edge": f"{_safe_mean([safe_float(r, 'refill_edge', math.nan) for r in rows]):.6f}",
                "placed_avg_queue_deplete_mult": f"{_safe_mean([safe_float(r, 'queue_deplete_mult', math.nan) for r in rows]):.6f}",
                "placed_avg_flow_absorption_score": f"{_safe_mean([safe_float(r, 'flow_absorption_score', math.nan) for r in rows]):.6f}",
            }
        )

    daily_rows: list[dict[str, Any]] = []
    for (day, bucket), rows in daily_groups.items():
        path = _avg_markout_path(rows)
        daily_rows.append(
            {
                "day": day,
                "local_bucket": bucket,
                "fills": len(rows),
                "half_life_s": f"{_response_half_life_from_path(path):.6f}",
                "avg_capacity_score": f"{_safe_mean([safe_float(r, 'capacity_score', math.nan) for r in rows]):.6f}",
                "xmarket_adverse_rate": f"{_safe_mean([safe_float(r, 'xmarket_adverse') for r in rows]):.6f}",
                "depth_vanish_rate": f"{_safe_mean([safe_float(r, 'depth_vanish') for r in rows]):.6f}",
                "refill_or_stable_rate": f"{_safe_mean([safe_float(r, 'refill_or_stable') for r in rows]):.6f}",
                "median_fill_age_s": f"{_safe_quantile([safe_float(r, 'fill_age_ms', math.nan) / 1000.0 for r in rows], 0.5):.6f}",
                "p75_fill_age_s": f"{_safe_quantile([safe_float(r, 'fill_age_ms', math.nan) / 1000.0 for r in rows], 0.75):.6f}",
                "avg_markout_1s_bps": f"{_fill_qty_weighted_mean(rows, 'markout_1s_bps'):.6f}",
                "avg_markout_5s_bps": f"{_fill_qty_weighted_mean(rows, 'markout_5s_bps'):.6f}",
                "avg_markout_20s_bps": f"{_fill_qty_weighted_mean(rows, 'markout_20s_bps'):.6f}",
                "avg_markout_30s_bps": f"{_fill_qty_weighted_mean(rows, 'markout_30s_bps'):.6f}",
                "positive_30s_rate": f"{_safe_mean([1.0 if safe_float(r, 'markout_30s_bps', math.nan) > 0.0 else 0.0 for r in rows]):.6f}",
            }
        )

    capacity_by_bucket = {row["local_bucket"]: row for row in order_capacity_rows}
    rollup_rows: list[dict[str, Any]] = []
    for bucket, rows in fill_groups.items():
        path = _avg_markout_path(rows)
        half_life = _response_half_life_from_path(path)
        support = [
            r
            for r in daily_rows
            if r["local_bucket"] == bucket and safe_int(r, "fills") >= min_daily_fills
        ]
        support_days = len(support)
        positive_support_days = sum(
            1 for r in support if safe_float(r, "avg_markout_30s_bps", math.nan) > 0.0
        )
        positive_support_ratio = positive_support_days / support_days if support_days else 0.0
        p75_fill_age_s = _safe_quantile(
            [safe_float(r, "fill_age_ms", math.nan) / 1000.0 for r in rows], 0.75
        )
        lifecycle_budget = min(
            holding_budget_s,
            p75_fill_age_s
            if math.isfinite(p75_fill_age_s) and p75_fill_age_s > 0.0
            else holding_budget_s,
        )
        capacity_row = capacity_by_bucket.get(bucket, {})
        avg_markout_30 = _fill_qty_weighted_mean(rows, "markout_30s_bps")
        response_pass = (
            len(rows) >= min_fills
            and support_days >= 3
            and positive_support_ratio >= (2.0 / 3.0)
            and avg_markout_30 > 0.0
        )
        absorptive_pass = (
            safe_float(
                capacity_row,
                "placed_avg_capacity_score",
                _safe_mean([safe_float(r, "capacity_score", math.nan) for r in rows]),
            )
            >= 0.55
            and safe_float(
                capacity_row,
                "placed_refill_or_stable_rate",
                _safe_mean([safe_float(r, "refill_or_stable") for r in rows]),
            )
            >= 0.50
            and safe_float(
                capacity_row,
                "placed_depth_vanish_rate",
                _safe_mean([safe_float(r, "depth_vanish") for r in rows]),
            )
            <= 0.35
        )
        xmarket_pass = (
            safe_float(
                capacity_row,
                "placed_xmarket_adverse_rate",
                _safe_mean([safe_float(r, "xmarket_adverse") for r in rows]),
            )
            <= max_xmarket_adverse_rate
        )
        half_life_pass = math.isfinite(half_life) and half_life <= lifecycle_budget
        candidate = response_pass and half_life_pass and absorptive_pass and xmarket_pass
        row = {
            "local_bucket": bucket,
            "fills": len(rows),
            "days": len({str(r.get("day", "")) for r in rows}),
            "support_days": support_days,
            "positive_support_days": positive_support_days,
            "positive_support_ratio": f"{positive_support_ratio:.6f}",
            "half_life_s": f"{half_life:.6f}",
            "holding_budget_s": f"{holding_budget_s:.6f}",
            "p75_fill_age_s": f"{p75_fill_age_s:.6f}",
            "effective_lifecycle_budget_s": f"{lifecycle_budget:.6f}",
            "half_life_lt_budget": int(half_life_pass),
            "avg_capacity_score": f"{_safe_mean([safe_float(r, 'capacity_score', math.nan) for r in rows]):.6f}",
            "refill_or_stable_rate": f"{_safe_mean([safe_float(r, 'refill_or_stable') for r in rows]):.6f}",
            "depth_vanish_rate": f"{_safe_mean([safe_float(r, 'depth_vanish') for r in rows]):.6f}",
            "xmarket_adverse_rate": f"{_safe_mean([safe_float(r, 'xmarket_adverse') for r in rows]):.6f}",
            "avg_markout_1s_bps": f"{_fill_qty_weighted_mean(rows, 'markout_1s_bps'):.6f}",
            "avg_markout_5s_bps": f"{_fill_qty_weighted_mean(rows, 'markout_5s_bps'):.6f}",
            "avg_markout_20s_bps": f"{_fill_qty_weighted_mean(rows, 'markout_20s_bps'):.6f}",
            "avg_markout_30s_bps": f"{avg_markout_30:.6f}",
            "response_pass": int(response_pass),
            "absorptive_pass": int(absorptive_pass),
            "xmarket_not_adverse_pass": int(xmarket_pass),
            "mechanism_candidate": int(candidate),
            "mechanism_verdict": "candidate"
            if candidate
            else (
                "response_only"
                if response_pass
                else ("capacity_without_response" if absorptive_pass else "mixed_or_failed")
            ),
        }
        for key, value in capacity_row.items():
            row.setdefault(key, value)
        rollup_rows.append(row)

    rollup_rows.sort(
        key=lambda r: (
            -safe_int(r, "mechanism_candidate"),
            -safe_int(r, "response_pass"),
            -safe_int(r, "absorptive_pass"),
            -safe_float(r, "avg_markout_30s_bps", -1e9),
            -safe_float(r, "fills"),
        )
    )
    candidates = [r for r in rollup_rows if safe_int(r, "mechanism_candidate")]
    return {
        "daily": sorted(daily_rows, key=lambda r: (str(r["day"]), str(r["local_bucket"]))),
        "order_capacity": sorted(
            order_capacity_rows, key=lambda r: -safe_float(r, "placed_orders")
        ),
        "rollup": rollup_rows,
        "candidates": candidates,
    }


def local_liquidity_mechanism_summary(tables: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    rollup = tables.get("rollup", [])
    candidates = tables.get("candidates", [])
    return {
        "rollup_rows": len(rollup),
        "candidate_rows": len(candidates),
        "response_pass_rows": sum(safe_int(r, "response_pass") for r in rollup),
        "absorptive_pass_rows": sum(safe_int(r, "absorptive_pass") for r in rollup),
        "placed_buckets": len(tables.get("order_capacity", [])),
        "daily_rows": len(tables.get("daily", [])),
    }


@dataclass(frozen=True)
class BboMidSeries:
    """Monotonic BBO-mid series used by xmarket shadow audits.

    中文说明：这里故意只保存 (timestamp, mid)。第一阶段只做 quote-time
    reference risk evidence 和 event-cancel 反事实，不把完整 orderbook
    状态塞进 audit 层，避免和 replay 引擎语义混在一起。
    """

    ts: tuple[float, ...]
    mid: tuple[float, ...]
    resolution: str = "unknown"

    @property
    def empty(self) -> bool:
        return not self.ts or not self.mid


XMARKET_REF_HORIZONS_MS = (100, 250, 500, 1000, 3000, 5000, 10000)
XMARKET_PENDING_HORIZONS_MS = (1000, 3000, 5000)
XMARKET_CANCEL_LATENCIES_MS = (50, 100, 250, 500)


def _bbo_mid_at_or_before(
    series: BboMidSeries | None, ts: float, *, max_lag_s: float = 5.0
) -> float:
    if series is None or series.empty or ts <= 0.0:
        return math.nan
    idx = bisect.bisect_right(series.ts, ts) - 1
    if idx < 0:
        return math.nan
    if max_lag_s > 0.0 and ts - series.ts[idx] > max_lag_s:
        return math.nan
    return series.mid[idx]


def _bbo_move_bps(
    series: BboMidSeries | None,
    *,
    ts: float,
    horizon_ms: int,
    max_lag_s: float,
) -> float:
    now_mid = _bbo_mid_at_or_before(series, ts, max_lag_s=max_lag_s)
    past_mid = _bbo_mid_at_or_before(series, ts - horizon_ms / 1000.0, max_lag_s=max_lag_s)
    if not (now_mid > 0.0 and past_mid > 0.0):
        return math.nan
    return (now_mid - past_mid) / past_mid * 10_000.0


def _side_adverse_move_bps(side: str, move_bps: float) -> float:
    if not math.isfinite(move_bps):
        return math.nan
    if side == "BUY":
        return max(0.0, -move_bps)
    if side == "SELL":
        return max(0.0, move_bps)
    return math.nan


def _side_favorable_move_bps(side: str, move_bps: float) -> float:
    if not math.isfinite(move_bps):
        return math.nan
    if side == "BUY":
        return max(0.0, move_bps)
    if side == "SELL":
        return max(0.0, -move_bps)
    return math.nan


def _pending_ref_bucket(value_bps: float, *, threshold_bps: float) -> str:
    """Bucket a raw BTCUSDT residual in bps.

    中文说明：raw pending 保留方向，不按 side 翻转。它回答
    “BTCUSDT 相对 BTCUSDC 是否仍有未被吸收的上/下行残差”。
    """
    if not math.isfinite(value_bps):
        return "missing"
    t = max(float(threshold_bps), 1e-9)
    if value_bps <= -2.0 * t:
        return "raw_down_gt2t"
    if value_bps <= -1.0 * t:
        return "raw_down_1t_2t"
    if value_bps < -0.5 * t:
        return "raw_down_0p5t_1t"
    if value_bps < 0.5 * t:
        return "raw_neutral"
    if value_bps < 1.0 * t:
        return "raw_up_0p5t_1t"
    if value_bps < 2.0 * t:
        return "raw_up_1t_2t"
    return "raw_up_gt2t"


def _side_pending_bucket(side: str, pending_bps: float, *, threshold_bps: float) -> str:
    """Bucket pending residual after converting it into side-favorable sign.

    For BUY, positive pending is favorable because fair value is moving up
    before/around our bid fill.  For SELL, positive pending is adverse because
    fair value is moving up while we may sell too cheaply.
    """
    if not math.isfinite(pending_bps):
        return "missing"
    if side == "BUY":
        side_fav = pending_bps
    elif side == "SELL":
        side_fav = -pending_bps
    else:
        return "missing"
    t = max(float(threshold_bps), 1e-9)
    if side_fav <= -2.0 * t:
        return "adverse_gt2t"
    if side_fav <= -1.0 * t:
        return "adverse_1t_2t"
    if side_fav < -0.5 * t:
        return "adverse_0p5t_1t"
    if side_fav < 0.5 * t:
        return "neutral"
    if side_fav < 1.0 * t:
        return "favorable_0p5t_1t"
    if side_fav < 2.0 * t:
        return "favorable_1t_2t"
    return "favorable_gt2t"


def _side_pending_favorable_bps(side: str, pending_bps: float) -> float:
    if not math.isfinite(pending_bps):
        return math.nan
    if side == "BUY":
        return pending_bps
    if side == "SELL":
        return -pending_bps
    return math.nan


def _pending_sorting_row(
    *,
    side: str,
    horizon_ms: int,
    rows_by_bucket: dict[str, list[dict[str, Any]]],
    min_fills: int = 20,
) -> dict[str, Any]:
    adverse_rows: list[dict[str, Any]] = []
    favorable_rows: list[dict[str, Any]] = []
    neutral_rows: list[dict[str, Any]] = []
    for bucket, rows in rows_by_bucket.items():
        if bucket.startswith("adverse_"):
            adverse_rows.extend(rows)
        elif bucket.startswith("favorable_"):
            favorable_rows.extend(rows)
        elif bucket == "neutral":
            neutral_rows.extend(rows)

    adverse_fills = [r for r in adverse_rows if safe_int(r, "filled")]
    favorable_fills = [r for r in favorable_rows if safe_int(r, "filled")]
    neutral_fills = [r for r in neutral_rows if safe_int(r, "filled")]
    adv_campaign = _avg_terminal_pnl(adverse_rows)
    fav_campaign = _avg_terminal_pnl(favorable_rows)
    out = {
        "side": side,
        "horizon_ms": horizon_ms,
        "adverse_orders": len(adverse_rows),
        "neutral_orders": len(neutral_rows),
        "favorable_orders": len(favorable_rows),
        "adverse_fills": len(adverse_fills),
        "neutral_fills": len(neutral_fills),
        "favorable_fills": len(favorable_fills),
        "adverse_avg_campaign_terminal_pnl": f"{adv_campaign:.6f}",
        "favorable_avg_campaign_terminal_pnl": f"{fav_campaign:.6f}",
        "support_pass": int(len(adverse_fills) >= min_fills and len(favorable_fills) >= min_fills),
    }
    for horizon_s in (1, 5, 20, 30):
        col = f"markout_{horizon_s}s_bps"
        adv_m = _fill_qty_weighted_mean(adverse_fills, col)
        fav_m = _fill_qty_weighted_mean(favorable_fills, col)
        neu_m = _fill_qty_weighted_mean(neutral_fills, col)
        out[f"adverse_avg_markout_{horizon_s}s_bps"] = f"{adv_m:.6f}"
        out[f"neutral_avg_markout_{horizon_s}s_bps"] = f"{neu_m:.6f}"
        out[f"favorable_avg_markout_{horizon_s}s_bps"] = f"{fav_m:.6f}"
        out[f"fav_minus_adv_markout_{horizon_s}s_bps"] = (
            f"{fav_m - adv_m:.6f}" if math.isfinite(fav_m) and math.isfinite(adv_m) else ""
        )
        out[f"fav_minus_neutral_markout_{horizon_s}s_bps"] = (
            f"{fav_m - neu_m:.6f}" if math.isfinite(fav_m) and math.isfinite(neu_m) else ""
        )
        out[f"sorting_pass_{horizon_s}s"] = int(
            len(adverse_fills) >= min_fills
            and len(favorable_fills) >= min_fills
            and math.isfinite(fav_m)
            and math.isfinite(adv_m)
            and fav_m > adv_m
        )
    # Backward-compatible default: historical reports used 30s as the primary
    # sorting horizon.  New xref work must inspect 1s/5s as well.
    return out


def _xmarket_state(
    *,
    side: str,
    ref_move_bps: float,
    local_move_bps: float,
    spot_move_bps: float,
    threshold_bps: float,
) -> tuple[str, int, int, int, int]:
    ref_adv = _side_adverse_move_bps(side, ref_move_bps)
    ref_fav = _side_favorable_move_bps(side, ref_move_bps)
    local_adv = _side_adverse_move_bps(side, local_move_bps)
    spot_adv = _side_adverse_move_bps(side, spot_move_bps)
    spot_fav = _side_favorable_move_bps(side, spot_move_bps)

    ref_adverse = int(math.isfinite(ref_adv) and ref_adv >= threshold_bps)
    ref_favorable = int(math.isfinite(ref_fav) and ref_fav >= threshold_bps)
    local_already_moved = int(
        ref_adverse
        and math.isfinite(local_adv)
        and local_adv >= max(threshold_bps * 0.5, ref_adv * 0.5)
    )
    spot_confirmed = int(
        (ref_adverse and math.isfinite(spot_adv) and spot_adv >= threshold_bps * 0.5)
        or (ref_favorable and math.isfinite(spot_fav) and spot_fav >= threshold_bps * 0.5)
    )
    ref_leads_local = int(ref_adverse and not local_already_moved)

    if ref_adverse and ref_leads_local:
        state = "adverse_leading"
    elif ref_adverse and local_already_moved:
        state = "adverse_confirmed"
    elif ref_favorable and spot_confirmed:
        state = "favorable_confirmed"
    elif ref_favorable:
        state = "favorable_leading"
    else:
        state = "neutral"
    return state, ref_adverse, local_already_moved, ref_leads_local, spot_confirmed


def _group_outcome_row(name: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    fills = [r for r in rows if safe_int(r, "filled")]
    labeled = [r for r in rows if str(r.get("terminal_campaign_label", ""))]
    terminal_campaigns = _unique_terminal_campaign_rows(rows)
    return {
        "bucket": name,
        "placed_orders": len(rows),
        "filled_orders": len(fills),
        "fill_rate": f"{len(fills) / len(rows) if rows else 0.0:.6f}",
        "avg_markout_1s_bps": f"{_fill_qty_weighted_mean(fills, 'markout_1s_bps'):.6f}",
        "avg_markout_5s_bps": f"{_fill_qty_weighted_mean(fills, 'markout_5s_bps'):.6f}",
        "avg_markout_20s_bps": f"{_fill_qty_weighted_mean(fills, 'markout_20s_bps'):.6f}",
        "avg_markout_30s_bps": f"{_fill_qty_weighted_mean(fills, 'markout_30s_bps'):.6f}",
        "tail_m50_30s": sum(
            1 for r in fills if safe_float(r, "markout_30s_bps", math.nan) <= -50.0
        ),
        "tail_m50_30s_rate": f"{sum(1 for r in fills if safe_float(r, 'markout_30s_bps', math.nan) <= -50.0) / len(fills) if fills else 0.0:.6f}",
        "positive_30s_rate": f"{sum(1 for r in fills if safe_float(r, 'markout_30s_bps', math.nan) > 0.0) / len(fills) if fills else 0.0:.6f}",
        "terminal_labeled_orders": len(labeled),
        "terminal_labeled_campaigns": len(terminal_campaigns),
        "avg_campaign_terminal_pnl": f"{_safe_mean([safe_float(r, 'terminal_final_total_pnl_delta', math.nan) for r in terminal_campaigns]):.6f}",
        "campaign_repair_rate": f"{_safe_mean([safe_float(r, 'terminal_campaign_repaired', math.nan) for r in terminal_campaigns]):.6f}",
        "campaign_bad_rate": f"{_safe_mean([safe_float(r, 'terminal_campaign_bad', math.nan) for r in terminal_campaigns]):.6f}",
        "campaign_tail_rate": f"{_safe_mean([safe_float(r, 'terminal_campaign_tail_loss', math.nan) for r in terminal_campaigns]):.6f}",
        "avg_campaign_adverse_excursion": f"{_safe_mean([safe_float(r, 'campaign_adverse_excursion', math.nan) for r in rows]):.6f}",
    }


def _event_cancel_signal_before_fill(
    *,
    side: str,
    submit_ts: float,
    outcome_ts: float,
    ref_series: BboMidSeries,
    threshold_bps: float,
    latency_ms: int,
    lookback_ms: int,
    max_lag_s: float,
) -> tuple[int, float, float]:
    """Return whether a reference shock would cancel before the order outcome.

    中文说明：第一版用 ref BBO snapshot 扫描 [submit, outcome-latency]。
    如果 BBO 是 1s 快照，输出只能代表 coarse shadow，不是 50ms/100ms
    精确撮合模拟。
    """
    if ref_series.empty or side not in {"BUY", "SELL"} or submit_ts <= 0.0:
        return 0, 0.0, math.nan
    end_ts = outcome_ts - latency_ms / 1000.0 if outcome_ts > 0.0 else submit_ts
    if end_ts < submit_ts:
        return 0, 0.0, math.nan
    start_idx = max(0, bisect.bisect_left(ref_series.ts, submit_ts))
    end_idx = min(len(ref_series.ts), bisect.bisect_right(ref_series.ts, end_ts))
    max_adverse = 0.0
    first_signal_ts = 0.0
    for idx in range(start_idx, end_idx):
        ts = ref_series.ts[idx]
        move = _bbo_move_bps(ref_series, ts=ts, horizon_ms=lookback_ms, max_lag_s=max_lag_s)
        adverse = _side_adverse_move_bps(side, move)
        if math.isfinite(adverse):
            max_adverse = max(max_adverse, adverse)
            if adverse >= threshold_bps and first_signal_ts <= 0.0:
                first_signal_ts = ts
                break
    return int(first_signal_ts > 0.0), first_signal_ts, max_adverse


def _attach_pending_fields(
    out: dict[str, Any],
    *,
    side: str,
    sample_ts: float,
    prefix_root: str,
    ref_bbo: BboMidSeries,
    local_bbo: BboMidSeries,
    threshold_bps: float,
    max_lag_s: float,
) -> dict[int, str]:
    """Attach pending-ref fields sampled at one timestamp.

    ``prefix_root`` is either ``pending_ref`` for submit-time evidence or
    ``fill_pending_ref`` for fill-time/C1 campaign evidence.
    """
    buckets: dict[int, str] = {}
    local_mid_now = _bbo_mid_at_or_before(local_bbo, sample_ts, max_lag_s=max_lag_s)
    for horizon_ms in XMARKET_PENDING_HORIZONS_MS:
        ref_move = _bbo_move_bps(ref_bbo, ts=sample_ts, horizon_ms=horizon_ms, max_lag_s=max_lag_s)
        local_move = _bbo_move_bps(
            local_bbo, ts=sample_ts, horizon_ms=horizon_ms, max_lag_s=max_lag_s
        )
        pending = (
            ref_move - local_move
            if math.isfinite(ref_move) and math.isfinite(local_move)
            else math.nan
        )
        pending_ticks = (
            pending / 10_000.0 * local_mid_now / 0.1
            if math.isfinite(pending) and math.isfinite(local_mid_now) and local_mid_now > 0.0
            else math.nan
        )
        side_pending = _side_pending_favorable_bps(side, pending)
        raw_bucket = _pending_ref_bucket(pending, threshold_bps=threshold_bps)
        side_bucket = _side_pending_bucket(side, pending, threshold_bps=threshold_bps)
        out[f"{prefix_root}_{horizon_ms}ms_bps"] = (
            f"{pending:.6f}" if math.isfinite(pending) else ""
        )
        out[f"{prefix_root}_{horizon_ms}ms_ticks"] = (
            f"{pending_ticks:.6f}" if math.isfinite(pending_ticks) else ""
        )
        out[f"{prefix_root}_{horizon_ms}ms_raw_bucket"] = raw_bucket
        out[f"{prefix_root}_{horizon_ms}ms_side_favorable_bps"] = (
            f"{side_pending:.6f}" if math.isfinite(side_pending) else ""
        )
        out[f"{prefix_root}_{horizon_ms}ms_side_bucket"] = side_bucket
        buckets[horizon_ms] = side_bucket
    return buckets


def xmarket_ref_shadow_tables(
    order_rows: list[dict[str, Any]],
    *,
    ref_bbo: BboMidSeries | None,
    local_bbo: BboMidSeries | None = None,
    spot_bbo: BboMidSeries | None = None,
    threshold_bps: float = 1.0,
    max_lag_s: float = 5.0,
    horizons_ms: tuple[int, ...] = XMARKET_REF_HORIZONS_MS,
    cancel_latencies_ms: tuple[int, ...] = XMARKET_CANCEL_LATENCIES_MS,
    cancel_lookback_ms: int = 1000,
    cancel_threshold_bps: float = 1.0,
    enable_event_cancel: bool = True,
    include_orders: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    """Build BTCUSDT reference shadow evidence for order-level rows.

    中文说明：这个 report 只回答“reference 在 quote-time 是否有风险解释力”
    和“event-cancel 反事实是否可能救 toxic fill”。它不改变 replay outcome，
    也不复活 archived xmarket direct policy。
    """
    if ref_bbo is None or ref_bbo.empty:
        return {
            "summary": [{"status": "missing_ref_bbo", "orders": len(order_rows)}],
            "state_rollup": [],
            "daily": [],
            "pending_rollup": [],
            "pending_daily": [],
            "pending_sorting": [],
            "fill_pending_rollup": [],
            "fill_pending_daily": [],
            "fill_pending_sorting": [],
            "event_cancel": [],
            "orders": [],
        }

    enriched: list[dict[str, Any]] = []
    groups: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    daily_groups: dict[tuple[str, str, str, int], list[dict[str, Any]]] = defaultdict(list)
    pending_groups: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    pending_daily_groups: dict[tuple[str, str, str, int], list[dict[str, Any]]] = defaultdict(list)
    fill_pending_groups: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    fill_pending_daily_groups: dict[tuple[str, str, str, int], list[dict[str, Any]]] = defaultdict(
        list
    )
    cancel_groups: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)

    ref_resolution = ref_bbo.resolution
    local = local_bbo or ref_bbo
    for row in order_rows:
        side = norm_side(row.get("side", ""))
        if side not in {"BUY", "SELL"}:
            continue
        ts = safe_float(row, "timestamp", 0.0)
        if ts > 10_000_000_000:
            ts /= 1000.0
        if ts <= 0.0:
            ts = _norm_ms_ts(row, "submit_ts", "quote_ts")
        if ts <= 0.0:
            continue
        day = str(row.get("day") or utc_day(ts))
        out = (
            dict(row)
            if include_orders
            else {
                "timestamp": f"{ts:.3f}",
                "utc": utc_text(ts),
                "day": day,
                "arm": row.get("arm", ""),
                "campaign_id": row.get("campaign_id", ""),
                "client_order_id": row.get("client_order_id", row.get("order_id", "")),
                "side": side,
                "filled": row.get("filled", ""),
                "fill_ts": row.get("fill_ts", ""),
                "markout_1s_bps": row.get("markout_1s_bps", ""),
                "markout_5s_bps": row.get("markout_5s_bps", ""),
                "markout_20s_bps": row.get("markout_20s_bps", ""),
                "markout_30s_bps": row.get("markout_30s_bps", ""),
                "campaign_adverse_excursion": row.get("campaign_adverse_excursion", ""),
                "terminal_campaign_label": row.get("terminal_campaign_label", ""),
                "terminal_final_total_pnl_delta": row.get("terminal_final_total_pnl_delta", ""),
                "terminal_campaign_repaired": row.get("terminal_campaign_repaired", ""),
                "terminal_campaign_bad": row.get("terminal_campaign_bad", ""),
                "terminal_campaign_tail_loss": row.get("terminal_campaign_tail_loss", ""),
            }
        )
        out["ref_bbo_resolution"] = ref_resolution
        out["xmarket_threshold_bps"] = f"{threshold_bps:.6f}"
        for horizon_ms in horizons_ms:
            ref_move = _bbo_move_bps(ref_bbo, ts=ts, horizon_ms=horizon_ms, max_lag_s=max_lag_s)
            local_move = _bbo_move_bps(local, ts=ts, horizon_ms=horizon_ms, max_lag_s=max_lag_s)
            spot_move = (
                _bbo_move_bps(spot_bbo, ts=ts, horizon_ms=horizon_ms, max_lag_s=max_lag_s)
                if spot_bbo
                else math.nan
            )
            state, ref_adverse, local_moved, ref_leads, spot_confirmed = _xmarket_state(
                side=side,
                ref_move_bps=ref_move,
                local_move_bps=local_move,
                spot_move_bps=spot_move,
                threshold_bps=threshold_bps,
            )
            prefix = f"ref_{horizon_ms}ms"
            out[f"{prefix}_move_bps"] = f"{ref_move:.6f}" if math.isfinite(ref_move) else ""
            out[f"{prefix}_side_adverse_bps"] = (
                f"{_side_adverse_move_bps(side, ref_move):.6f}" if math.isfinite(ref_move) else ""
            )
            out[f"{prefix}_state"] = state
            if horizon_ms in (1000, 3000, 5000, 10000):
                groups[(side, state, horizon_ms)].append(out)
                daily_groups[(day, side, state, horizon_ms)].append(out)
            if horizon_ms == 1000:
                out["xmarket_state"] = state
                out["ref_adverse_for_side"] = ref_adverse
                out["btc_usdc_local_already_moved"] = local_moved
                out["ref_leads_local"] = ref_leads
                out["ref_confirmed_by_spot"] = spot_confirmed
        submit_pending_buckets = _attach_pending_fields(
            out,
            side=side,
            sample_ts=ts,
            prefix_root="pending_ref",
            ref_bbo=ref_bbo,
            local_bbo=local,
            threshold_bps=threshold_bps,
            max_lag_s=max_lag_s,
        )
        for horizon_ms, side_bucket in submit_pending_buckets.items():
            pending_groups[(side, side_bucket, horizon_ms)].append(out)
            pending_daily_groups[(day, side, side_bucket, horizon_ms)].append(out)
        filled = safe_int(row, "filled")
        outcome_ts = safe_float(row, "fill_ts", 0.0)
        if outcome_ts > 10_000_000_000:
            outcome_ts /= 1000.0
        if outcome_ts <= 0.0:
            lifetime_ms = safe_float(
                row,
                "observed_lifetime_ms",
                safe_float(row, "fill_age_ms", safe_float(row, "lifetime_ms", 0.0)),
            )
            outcome_ts = ts + lifetime_ms / 1000.0 if lifetime_ms > 0.0 else ts
        if filled and outcome_ts > 0.0:
            fill_pending_buckets = _attach_pending_fields(
                out,
                side=side,
                sample_ts=outcome_ts,
                prefix_root="fill_pending_ref",
                ref_bbo=ref_bbo,
                local_bbo=local,
                threshold_bps=threshold_bps,
                max_lag_s=max_lag_s,
            )
            for horizon_ms, side_bucket in fill_pending_buckets.items():
                fill_pending_groups[(side, side_bucket, horizon_ms)].append(out)
                fill_pending_daily_groups[(day, side, side_bucket, horizon_ms)].append(out)
        for latency_ms in cancel_latencies_ms if enable_event_cancel else ():
            signaled, signal_ts, max_adverse = _event_cancel_signal_before_fill(
                side=side,
                submit_ts=ts,
                outcome_ts=outcome_ts,
                ref_series=ref_bbo,
                threshold_bps=cancel_threshold_bps,
                latency_ms=latency_ms,
                lookback_ms=cancel_lookback_ms,
                max_lag_s=max_lag_s,
            )
            markout30 = safe_float(row, "markout_30s_bps", math.nan)
            cancel_row = {
                "day": day,
                "side": side,
                "latency_ms": latency_ms,
                "resolution": ref_resolution,
                "signaled": signaled,
                "filled": filled,
                "saved_toxic_fill": int(
                    signaled and filled and math.isfinite(markout30) and markout30 <= -50.0
                ),
                "false_cancel_positive_fill": int(
                    signaled and filled and math.isfinite(markout30) and markout30 > 0.0
                ),
                "blocked_any_fill": int(signaled and filled),
                "signal_ts": f"{signal_ts:.3f}" if signal_ts > 0.0 else "",
                "max_ref_adverse_bps": f"{max_adverse:.6f}" if math.isfinite(max_adverse) else "",
                "markout_30s_bps": f"{markout30:.6f}" if math.isfinite(markout30) else "",
            }
            cancel_groups[(side, ref_resolution, latency_ms)].append(cancel_row)
        enriched.append(out)

    state_rollup = []
    for (side, state, horizon_ms), rows in groups.items():
        row = _group_outcome_row(f"{side}|{state}|{horizon_ms}ms", rows)
        row.update(
            {
                "side": side,
                "xmarket_state": state,
                "horizon_ms": horizon_ms,
                "resolution": ref_resolution,
            }
        )
        state_rollup.append(row)
    state_rollup.sort(key=lambda r: (str(r["side"]), int(r["horizon_ms"]), str(r["xmarket_state"])))

    daily = []
    for (day, side, state, horizon_ms), rows in daily_groups.items():
        row = _group_outcome_row(f"{day}|{side}|{state}|{horizon_ms}ms", rows)
        row.update(
            {
                "day": day,
                "side": side,
                "xmarket_state": state,
                "horizon_ms": horizon_ms,
                "resolution": ref_resolution,
            }
        )
        daily.append(row)
    daily.sort(
        key=lambda r: (str(r["day"]), str(r["side"]), int(r["horizon_ms"]), str(r["xmarket_state"]))
    )

    pending_rollup = []
    pending_by_side_horizon: dict[tuple[str, int], dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for (side, bucket, horizon_ms), rows in pending_groups.items():
        row = _group_outcome_row(f"{side}|{bucket}|{horizon_ms}ms", rows)
        side_fav_values = [
            safe_float(r, f"pending_ref_{horizon_ms}ms_side_favorable_bps", math.nan) for r in rows
        ]
        raw_values = [safe_float(r, f"pending_ref_{horizon_ms}ms_bps", math.nan) for r in rows]
        tick_values = [safe_float(r, f"pending_ref_{horizon_ms}ms_ticks", math.nan) for r in rows]
        row.update(
            {
                "side": side,
                "pending_side_bucket": bucket,
                "horizon_ms": horizon_ms,
                "resolution": ref_resolution,
                "avg_pending_ref_bps": f"{_safe_mean(raw_values):.6f}",
                "avg_side_favorable_pending_bps": f"{_safe_mean(side_fav_values):.6f}",
                "avg_pending_ref_ticks": f"{_safe_mean(tick_values):.6f}",
            }
        )
        pending_rollup.append(row)
        pending_by_side_horizon[(side, horizon_ms)][bucket].extend(rows)
    pending_rollup.sort(
        key=lambda r: (str(r["side"]), int(r["horizon_ms"]), str(r["pending_side_bucket"]))
    )

    pending_daily = []
    daily_sorting_work: dict[tuple[str, str, int], dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for (day, side, bucket, horizon_ms), rows in pending_daily_groups.items():
        row = _group_outcome_row(f"{day}|{side}|{bucket}|{horizon_ms}ms", rows)
        row.update(
            {
                "day": day,
                "side": side,
                "pending_side_bucket": bucket,
                "horizon_ms": horizon_ms,
                "resolution": ref_resolution,
            }
        )
        pending_daily.append(row)
        daily_sorting_work[(day, side, horizon_ms)][bucket].extend(rows)
    pending_daily.sort(
        key=lambda r: (
            str(r["day"]),
            str(r["side"]),
            int(r["horizon_ms"]),
            str(r["pending_side_bucket"]),
        )
    )

    pending_sorting = [
        _pending_sorting_row(side=side, horizon_ms=horizon_ms, rows_by_bucket=buckets)
        for (side, horizon_ms), buckets in pending_by_side_horizon.items()
    ]
    for (day, side, horizon_ms), buckets in daily_sorting_work.items():
        row = _pending_sorting_row(
            side=side, horizon_ms=horizon_ms, rows_by_bucket=buckets, min_fills=5
        )
        row["day"] = day
        row["scope"] = "daily"
        pending_sorting.append(row)
    for row in pending_sorting:
        row.setdefault("scope", "aggregate")
        row.setdefault("day", "")
    pending_sorting.sort(
        key=lambda r: (
            str(r.get("scope", "")),
            str(r.get("day", "")),
            str(r["side"]),
            int(r["horizon_ms"]),
        )
    )

    fill_pending_rollup = []
    fill_pending_by_side_horizon: dict[tuple[str, int], dict[str, list[dict[str, Any]]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    for (side, bucket, horizon_ms), rows in fill_pending_groups.items():
        row = _group_outcome_row(f"{side}|{bucket}|{horizon_ms}ms|fill_time", rows)
        side_fav_values = [
            safe_float(r, f"fill_pending_ref_{horizon_ms}ms_side_favorable_bps", math.nan)
            for r in rows
        ]
        raw_values = [safe_float(r, f"fill_pending_ref_{horizon_ms}ms_bps", math.nan) for r in rows]
        tick_values = [
            safe_float(r, f"fill_pending_ref_{horizon_ms}ms_ticks", math.nan) for r in rows
        ]
        row.update(
            {
                "sample_time": "fill",
                "side": side,
                "pending_side_bucket": bucket,
                "horizon_ms": horizon_ms,
                "resolution": ref_resolution,
                "avg_pending_ref_bps": f"{_safe_mean(raw_values):.6f}",
                "avg_side_favorable_pending_bps": f"{_safe_mean(side_fav_values):.6f}",
                "avg_pending_ref_ticks": f"{_safe_mean(tick_values):.6f}",
            }
        )
        fill_pending_rollup.append(row)
        fill_pending_by_side_horizon[(side, horizon_ms)][bucket].extend(rows)
    fill_pending_rollup.sort(
        key=lambda r: (str(r["side"]), int(r["horizon_ms"]), str(r["pending_side_bucket"]))
    )

    fill_pending_daily = []
    fill_daily_sorting_work: dict[tuple[str, str, int], dict[str, list[dict[str, Any]]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    for (day, side, bucket, horizon_ms), rows in fill_pending_daily_groups.items():
        row = _group_outcome_row(f"{day}|{side}|{bucket}|{horizon_ms}ms|fill_time", rows)
        row.update(
            {
                "sample_time": "fill",
                "day": day,
                "side": side,
                "pending_side_bucket": bucket,
                "horizon_ms": horizon_ms,
                "resolution": ref_resolution,
            }
        )
        fill_pending_daily.append(row)
        fill_daily_sorting_work[(day, side, horizon_ms)][bucket].extend(rows)
    fill_pending_daily.sort(
        key=lambda r: (
            str(r["day"]),
            str(r["side"]),
            int(r["horizon_ms"]),
            str(r["pending_side_bucket"]),
        )
    )

    fill_pending_sorting = [
        _pending_sorting_row(side=side, horizon_ms=horizon_ms, rows_by_bucket=buckets)
        for (side, horizon_ms), buckets in fill_pending_by_side_horizon.items()
    ]
    for (day, side, horizon_ms), buckets in fill_daily_sorting_work.items():
        row = _pending_sorting_row(
            side=side, horizon_ms=horizon_ms, rows_by_bucket=buckets, min_fills=5
        )
        row["day"] = day
        row["scope"] = "daily"
        fill_pending_sorting.append(row)
    for row in fill_pending_sorting:
        row.setdefault("scope", "aggregate")
        row.setdefault("day", "")
        row["sample_time"] = "fill"
    fill_pending_sorting.sort(
        key=lambda r: (
            str(r.get("scope", "")),
            str(r.get("day", "")),
            str(r["side"]),
            int(r["horizon_ms"]),
        )
    )

    event_cancel = []
    for (side, resolution, latency_ms), rows in cancel_groups.items():
        orders = len(rows)
        signaled = sum(safe_int(r, "signaled") for r in rows)
        filled = sum(safe_int(r, "filled") for r in rows)
        saved = sum(safe_int(r, "saved_toxic_fill") for r in rows)
        false_pos = sum(safe_int(r, "false_cancel_positive_fill") for r in rows)
        blocked = sum(safe_int(r, "blocked_any_fill") for r in rows)
        event_cancel.append(
            {
                "side": side,
                "resolution": resolution,
                "latency_ms": latency_ms,
                "orders": orders,
                "fills": filled,
                "cancel_signals": signaled,
                "cancel_pressure_rate": f"{signaled / orders if orders else 0.0:.6f}",
                "blocked_fills": blocked,
                "blocked_fill_rate": f"{blocked / filled if filled else 0.0:.6f}",
                "saved_toxic_fills_m50_30s": saved,
                "saved_toxic_rate_of_fills": f"{saved / filled if filled else 0.0:.6f}",
                "false_cancel_positive_fills": false_pos,
                "false_positive_rate_of_blocked_fills": f"{false_pos / blocked if blocked else 0.0:.6f}",
                "net_tail_minus_false_positive": saved - false_pos,
            }
        )
    event_cancel.sort(key=lambda r: (str(r["side"]), int(r["latency_ms"])))

    note = (
        "event_cancel is coarse if resolution is 1s_snapshot; use raw bookTicker before live-policy conclusions."
        if enable_event_cancel
        else "event_cancel disabled: trade-time history is shadow evidence, not receive-time cancel evidence."
    )
    summary = [
        {
            "status": "ok",
            "orders": len(enriched),
            "ref_resolution": ref_resolution,
            "threshold_bps": f"{threshold_bps:.6f}",
            "cancel_threshold_bps": f"{cancel_threshold_bps:.6f}",
            "event_cancel_enabled": int(enable_event_cancel),
            "max_lag_s": f"{max_lag_s:.6f}",
            "note": note,
        }
    ]
    return {
        "summary": summary,
        "state_rollup": state_rollup,
        "daily": daily,
        "pending_rollup": pending_rollup,
        "pending_daily": pending_daily,
        "pending_sorting": pending_sorting,
        "fill_pending_rollup": fill_pending_rollup,
        "fill_pending_daily": fill_pending_daily,
        "fill_pending_sorting": fill_pending_sorting,
        "event_cancel": event_cancel,
        "orders": enriched if include_orders else [],
    }


def xmarket_ref_shadow_summary(tables: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    summary = tables.get("summary", [{}])[0] if tables.get("summary") else {}
    return {
        "status": summary.get("status", "missing"),
        "orders": safe_int(summary, "orders"),
        "state_rows": len(tables.get("state_rollup", [])),
        "daily_rows": len(tables.get("daily", [])),
        "pending_rows": len(tables.get("pending_rollup", [])),
        "pending_daily_rows": len(tables.get("pending_daily", [])),
        "pending_sorting_rows": len(tables.get("pending_sorting", [])),
        "fill_pending_rows": len(tables.get("fill_pending_rollup", [])),
        "fill_pending_daily_rows": len(tables.get("fill_pending_daily", [])),
        "fill_pending_sorting_rows": len(tables.get("fill_pending_sorting", [])),
        "event_cancel_rows": len(tables.get("event_cancel", [])),
        "candidate_note": summary.get("note", ""),
    }


def _pending_rollup_tables(
    *,
    groups: dict[tuple[str, str, str, int], list[dict[str, Any]]],
    daily_groups: dict[tuple[str, str, str, str, int], list[dict[str, Any]]],
    sample_time: str,
    value_prefix_template: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Build reusable pending residual rollups for spot/ref shadow sources.

    中文说明：Stage 0 只验证 quote-time 可见的 residual 是否能排序成交后
    markout/campaign outcome。这里保留 order denominator，不只看 fills。
    """
    rollup: list[dict[str, Any]] = []
    by_source_side_horizon: dict[tuple[str, str, int], dict[str, list[dict[str, Any]]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    for (source, side, bucket, horizon_ms), rows in groups.items():
        value_prefix = value_prefix_template.format(source=source, horizon_ms=horizon_ms)
        row = _group_outcome_row(f"{source}|{sample_time}|{side}|{bucket}|{horizon_ms}ms", rows)
        raw_values = [safe_float(r, f"{value_prefix}_bps", math.nan) for r in rows]
        tick_values = [safe_float(r, f"{value_prefix}_ticks", math.nan) for r in rows]
        side_values = [safe_float(r, f"{value_prefix}_side_favorable_bps", math.nan) for r in rows]
        row.update(
            {
                "source": source,
                "sample_time": sample_time,
                "side": side,
                "pending_side_bucket": bucket,
                "horizon_ms": horizon_ms,
                "avg_pending_bps": f"{_safe_mean(raw_values):.6f}",
                "avg_side_favorable_pending_bps": f"{_safe_mean(side_values):.6f}",
                "avg_pending_ticks": f"{_safe_mean(tick_values):.6f}",
            }
        )
        rollup.append(row)
        by_source_side_horizon[(source, side, horizon_ms)][bucket].extend(rows)
    rollup.sort(
        key=lambda r: (
            str(r["source"]),
            str(r["sample_time"]),
            str(r["side"]),
            int(r["horizon_ms"]),
            str(r["pending_side_bucket"]),
        )
    )

    daily: list[dict[str, Any]] = []
    daily_sorting_work: dict[tuple[str, str, str, int], dict[str, list[dict[str, Any]]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    for (day, source, side, bucket, horizon_ms), rows in daily_groups.items():
        row = _group_outcome_row(
            f"{day}|{source}|{sample_time}|{side}|{bucket}|{horizon_ms}ms", rows
        )
        row.update(
            {
                "day": day,
                "source": source,
                "sample_time": sample_time,
                "side": side,
                "pending_side_bucket": bucket,
                "horizon_ms": horizon_ms,
            }
        )
        daily.append(row)
        daily_sorting_work[(day, source, side, horizon_ms)][bucket].extend(rows)
    daily.sort(
        key=lambda r: (
            str(r["day"]),
            str(r["source"]),
            str(r["sample_time"]),
            str(r["side"]),
            int(r["horizon_ms"]),
            str(r["pending_side_bucket"]),
        )
    )

    sorting: list[dict[str, Any]] = []
    for (source, side, horizon_ms), buckets in by_source_side_horizon.items():
        row = _pending_sorting_row(side=side, horizon_ms=horizon_ms, rows_by_bucket=buckets)
        row.update({"source": source, "sample_time": sample_time, "scope": "aggregate", "day": ""})
        sorting.append(row)
    for (day, source, side, horizon_ms), buckets in daily_sorting_work.items():
        row = _pending_sorting_row(
            side=side, horizon_ms=horizon_ms, rows_by_bucket=buckets, min_fills=5
        )
        row.update({"source": source, "sample_time": sample_time, "scope": "daily", "day": day})
        sorting.append(row)
    sorting.sort(
        key=lambda r: (
            str(r.get("scope", "")),
            str(r.get("day", "")),
            str(r.get("source", "")),
            str(r.get("sample_time", "")),
            str(r["side"]),
            int(r["horizon_ms"]),
        )
    )
    return rollup, daily, sorting


def spot_pending_shadow_tables(
    order_rows: list[dict[str, Any]],
    *,
    local_bbo: BboMidSeries | None,
    exec_spot_bbo: BboMidSeries | None = None,
    ref_spot_bbo: BboMidSeries | None = None,
    threshold_bps: float = 1.0,
    max_lag_s: float = 5.0,
    include_orders: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    """Build Stage-0 spot pending residual sorting evidence.

    ``exec_spot_pending`` compares BTCUSDC spot movement with BTCUSDC futures
    local movement. ``ref_spot_pending`` compares BTCUSDT spot movement with
    BTCUSDC futures local movement.  Both are shadow-only: no replay outcome
    is changed and no live policy is implied.
    """
    sources = {
        "exec_spot": exec_spot_bbo,
        "ref_spot": ref_spot_bbo,
    }
    active_sources = {
        name: series for name, series in sources.items() if series is not None and not series.empty
    }
    if local_bbo is None or local_bbo.empty or not active_sources:
        return {
            "summary": [
                {
                    "status": "missing_spot_or_local_series",
                    "orders": len(order_rows),
                    "active_sources": ",".join(sorted(active_sources)),
                }
            ],
            "pending_rollup": [],
            "pending_daily": [],
            "pending_sorting": [],
            "fill_pending_rollup": [],
            "fill_pending_daily": [],
            "fill_pending_sorting": [],
            "orders": [],
        }

    enriched: list[dict[str, Any]] = []
    pending_groups: dict[tuple[str, str, str, int], list[dict[str, Any]]] = defaultdict(list)
    pending_daily_groups: dict[tuple[str, str, str, str, int], list[dict[str, Any]]] = defaultdict(
        list
    )
    fill_pending_groups: dict[tuple[str, str, str, int], list[dict[str, Any]]] = defaultdict(list)
    fill_pending_daily_groups: dict[tuple[str, str, str, str, int], list[dict[str, Any]]] = (
        defaultdict(list)
    )

    for row in order_rows:
        side = norm_side(row.get("side", ""))
        if side not in {"BUY", "SELL"}:
            continue
        ts = safe_float(row, "timestamp", 0.0)
        if ts > 10_000_000_000:
            ts /= 1000.0
        if ts <= 0.0:
            ts = _norm_ms_ts(row, "submit_ts", "quote_ts")
        if ts <= 0.0:
            continue
        day = str(row.get("day") or utc_day(ts))
        out = (
            dict(row)
            if include_orders
            else {
                "timestamp": f"{ts:.3f}",
                "utc": utc_text(ts),
                "day": day,
                "arm": row.get("arm", ""),
                "campaign_id": row.get("campaign_id", ""),
                "client_order_id": row.get("client_order_id", row.get("order_id", "")),
                "side": side,
                "filled": row.get("filled", ""),
                "fill_ts": row.get("fill_ts", ""),
                "markout_1s_bps": row.get("markout_1s_bps", ""),
                "markout_5s_bps": row.get("markout_5s_bps", ""),
                "markout_20s_bps": row.get("markout_20s_bps", ""),
                "markout_30s_bps": row.get("markout_30s_bps", ""),
                "campaign_adverse_excursion": row.get("campaign_adverse_excursion", ""),
                "terminal_campaign_label": row.get("terminal_campaign_label", ""),
                "terminal_final_total_pnl_delta": row.get("terminal_final_total_pnl_delta", ""),
                "terminal_campaign_repaired": row.get("terminal_campaign_repaired", ""),
                "terminal_campaign_bad": row.get("terminal_campaign_bad", ""),
                "terminal_campaign_tail_loss": row.get("terminal_campaign_tail_loss", ""),
            }
        )
        out["spot_pending_threshold_bps"] = f"{threshold_bps:.6f}"
        out["local_resolution"] = local_bbo.resolution

        for source, spot_series in active_sources.items():
            out[f"{source}_resolution"] = spot_series.resolution
            submit_buckets = _attach_pending_fields(
                out,
                side=side,
                sample_ts=ts,
                prefix_root=f"{source}_pending",
                ref_bbo=spot_series,
                local_bbo=local_bbo,
                threshold_bps=threshold_bps,
                max_lag_s=max_lag_s,
            )
            for horizon_ms, side_bucket in submit_buckets.items():
                pending_groups[(source, side, side_bucket, horizon_ms)].append(out)
                pending_daily_groups[(day, source, side, side_bucket, horizon_ms)].append(out)

        filled = safe_int(row, "filled")
        fill_ts = safe_float(row, "fill_ts", 0.0)
        if fill_ts > 10_000_000_000:
            fill_ts /= 1000.0
        if fill_ts <= 0.0:
            lifetime_ms = safe_float(
                row,
                "observed_lifetime_ms",
                safe_float(row, "fill_age_ms", safe_float(row, "lifetime_ms", 0.0)),
            )
            fill_ts = ts + lifetime_ms / 1000.0 if lifetime_ms > 0.0 else ts
        if filled and fill_ts > 0.0:
            for source, spot_series in active_sources.items():
                fill_buckets = _attach_pending_fields(
                    out,
                    side=side,
                    sample_ts=fill_ts,
                    prefix_root=f"fill_{source}_pending",
                    ref_bbo=spot_series,
                    local_bbo=local_bbo,
                    threshold_bps=threshold_bps,
                    max_lag_s=max_lag_s,
                )
                for horizon_ms, side_bucket in fill_buckets.items():
                    fill_pending_groups[(source, side, side_bucket, horizon_ms)].append(out)
                    fill_pending_daily_groups[(day, source, side, side_bucket, horizon_ms)].append(
                        out
                    )
        scores = _order_scores(out)
        for key, value in scores.items():
            out[key] = f"{value:.6f}" if isinstance(value, float) else value
        enriched.append(out)

    pending_rollup, pending_daily, pending_sorting = _pending_rollup_tables(
        groups=pending_groups,
        daily_groups=pending_daily_groups,
        sample_time="quote",
        value_prefix_template="{source}_pending_{horizon_ms}ms",
    )
    fill_pending_rollup, fill_pending_daily, fill_pending_sorting = _pending_rollup_tables(
        groups=fill_pending_groups,
        daily_groups=fill_pending_daily_groups,
        sample_time="fill",
        value_prefix_template="fill_{source}_pending_{horizon_ms}ms",
    )

    summary = [
        {
            "status": "ok",
            "orders": len(enriched),
            "active_sources": ",".join(sorted(active_sources)),
            "local_resolution": local_bbo.resolution,
            "exec_spot_resolution": exec_spot_bbo.resolution if exec_spot_bbo else "",
            "ref_spot_resolution": ref_spot_bbo.resolution if ref_spot_bbo else "",
            "threshold_bps": f"{threshold_bps:.6f}",
            "max_lag_s": f"{max_lag_s:.6f}",
            "note": "Stage-0 shadow sorting only; do not infer live policy from this report alone.",
        }
    ]
    score_sanity = order_level_score_sanity_rows(enriched)
    score_daily = order_level_score_daily_rows(enriched)
    return {
        "summary": summary,
        "pending_rollup": pending_rollup,
        "pending_daily": pending_daily,
        "pending_sorting": pending_sorting,
        "fill_pending_rollup": fill_pending_rollup,
        "fill_pending_daily": fill_pending_daily,
        "fill_pending_sorting": fill_pending_sorting,
        "score_sanity": score_sanity,
        "score_daily": score_daily,
        "orders": enriched if include_orders else [],
    }


def spot_pending_shadow_summary(tables: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    summary = tables.get("summary", [{}])[0] if tables.get("summary") else {}
    return {
        "status": summary.get("status", "missing"),
        "orders": safe_int(summary, "orders"),
        "active_sources": summary.get("active_sources", ""),
        "pending_rows": len(tables.get("pending_rollup", [])),
        "pending_daily_rows": len(tables.get("pending_daily", [])),
        "pending_sorting_rows": len(tables.get("pending_sorting", [])),
        "fill_pending_rows": len(tables.get("fill_pending_rollup", [])),
        "fill_pending_daily_rows": len(tables.get("fill_pending_daily", [])),
        "fill_pending_sorting_rows": len(tables.get("fill_pending_sorting", [])),
        "score_sanity_rows": len(tables.get("score_sanity", [])),
        "score_daily_rows": len(tables.get("score_daily", [])),
        "candidate_note": summary.get("note", ""),
    }


SMOKE_GATES = {
    "pair_spread_p50_min": 45.0,
    "pair_spread_p50_max": 75.0,
    "final_spread_lt_100_rate_min": 0.75,
    "allow_post_rate_min": 0.75,
    "pause_rate_max": 0.25,
    "bad_guard_block_rate_max": 0.12,
    "top_bad_reason_rate_max": 0.08,
    "fill_cd_block_rate_max": 0.18,
}


def daily_gate_rows(summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in summary_rows:
        failures: list[str] = []
        spread_p50 = safe_float(row, "pair_spread_p50", safe_float(row, "avg_final_spread", 0.0))
        spread_lt100 = safe_float(
            row, "final_spread_lt_100_rate", safe_float(row, "quote_spread_lt_100_rate", 0.0)
        )
        allow_post = safe_float(row, "allow_post_rate", 1.0 - safe_float(row, "blocked_rate", 0.0))
        pause_rate = safe_float(row, "pause_rate", 0.0)
        bad_guard = safe_float(row, "bad_guard_block_rate", 0.0)
        top_bad = safe_float(row, "top_bad_reason_rate", 0.0)
        fill_cd = safe_float(row, "fill_cd_block_rate", 0.0)
        if spread_p50 and not (
            SMOKE_GATES["pair_spread_p50_min"] <= spread_p50 <= SMOKE_GATES["pair_spread_p50_max"]
        ):
            failures.append("spread_p50")
        if spread_lt100 < SMOKE_GATES["final_spread_lt_100_rate_min"]:
            failures.append("spread_lt100")
        if allow_post < SMOKE_GATES["allow_post_rate_min"]:
            failures.append("allow_post")
        if pause_rate > SMOKE_GATES["pause_rate_max"]:
            failures.append("pause_rate")
        if bad_guard > SMOKE_GATES["bad_guard_block_rate_max"]:
            failures.append("bad_guard")
        if top_bad > SMOKE_GATES["top_bad_reason_rate_max"]:
            failures.append("top_bad_reason")
        if fill_cd > SMOKE_GATES["fill_cd_block_rate_max"]:
            failures.append("fill_cd")
        out.append(
            {
                "day": row.get("day") or row.get("date") or utc_day(safe_float(row, "start_ts")),
                "arm": row.get("arm", row.get("name", "")),
                "gate_pass": int(not failures),
                "gate_failures": "|".join(failures) if failures else "none",
                "pnl": row.get("pnl", ""),
                "inventory_adjusted_pnl": row.get("inventory_adjusted_pnl", ""),
                "abs_inventory_time_s": row.get("abs_inventory_time_s", ""),
                "fills_total": row.get("fills_total", row.get("fills", "")),
                "spread_p50": f"{spread_p50:.4f}",
                "spread_lt100": f"{spread_lt100:.4f}",
                "allow_post_rate": f"{allow_post:.4f}",
                "pause_rate": f"{pause_rate:.4f}",
                "fill_cd_block_rate": f"{fill_cd:.4f}",
            }
        )
    return out


def _pick(row: dict[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return default


VERDICT_ORDER = {
    "strict_pass_review_required": 0,
    "watch_positive_proxy": 1,
    "tail_or_false_block_high": 2,
    "sparse_watch": 3,
    "diagnostic_only": 4,
    "no_denominator": 5,
    "model_metric_only": 6,
    "research_clue_watch": 1,
    "research_clue_too_sparse": 3,
}


def _evidence_sort_key(row: dict[str, Any]) -> tuple[float, float, float, float, str, str]:
    verdict = str(row.get("verdict", ""))
    return (
        float(VERDICT_ORDER.get(verdict, 9)),
        -safe_float(row, "shadow_improvement_proxy"),
        -safe_float(row, "filled_orders"),
        -safe_float(row, "placed_orders"),
        str(row.get("bucket_family", "")),
        str(row.get("bucket", "")),
    )


def _verdict_from_rates(
    *,
    placed: float,
    fills: float,
    daily_support: float,
    tail_rate: float,
    false_block_rate: float,
    improvement: float,
    strict_pass: bool = False,
) -> str:
    """Common evidence verdict for normalized research tables.

    中文说明：这里不是 promotion gate，只是把不同 research script 的输出
    压成相同状态，方便人工比较。严格 live gate 仍在 plan 文档里独立执行。
    """
    if strict_pass:
        return "strict_pass_review_required"
    if placed <= 0 or fills <= 0:
        return "no_denominator"
    if fills < 30 or daily_support < 5:
        return "sparse_watch"
    if tail_rate >= 0.35 or false_block_rate >= 0.35:
        return "tail_or_false_block_high"
    if improvement > 0:
        return "watch_positive_proxy"
    return "diagnostic_only"


def toxic_risk_evidence_rows(
    *,
    model_compare_rows: list[dict[str, Any]],
    order_aggregate_rows: list[dict[str, Any]],
    shadow_avoidance_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in shadow_avoidance_rows:
        placed = safe_float(row, "placed_orders")
        fills = safe_float(row, "filled_orders")
        tail_rate = safe_float(row, "tail_rate_filled_m50")
        false_block = safe_float(row, "positive_false_block_rate")
        improvement = safe_float(row, "shadow_improvement_qty_bps_proxy_if_avoided")
        rows.append(
            {
                "evidence_type": "toxic_shadow_avoidance",
                "candidate": _pick(row, "candidate"),
                "bucket_family": "",
                "bucket": "",
                "placed_orders": f"{placed:.0f}",
                "filled_orders": f"{fills:.0f}",
                "fill_rate": f"{safe_float(row, 'fill_rate'):.6f}",
                "tail_rate": f"{tail_rate:.6f}",
                "positive_false_block_rate": f"{false_block:.6f}",
                "shadow_improvement_proxy": f"{improvement:.6f}",
                "inventory_time_proxy": f"{safe_float(row, 'inventory_time_btc_s_proxy'):.6f}",
                "post_fill_20s_proxy": f"{safe_float(row, 'post_fill_20s_btc_s_proxy'):.6f}",
                "daily_support": "",
                "verdict": _verdict_from_rates(
                    placed=placed,
                    fills=fills,
                    daily_support=999.0,
                    tail_rate=tail_rate,
                    false_block_rate=false_block,
                    improvement=improvement,
                ),
            }
        )
    for row in order_aggregate_rows:
        placed = safe_float(row, "placed_orders")
        fills = safe_float(row, "filled_orders")
        tail_rate = safe_float(row, "tail_rate_filled_m50")
        false_block = 1.0 - safe_float(row, "positive_rate_filled")
        improvement = safe_float(row, "shadow_improvement_qty_bps_proxy_if_avoided")
        rows.append(
            {
                "evidence_type": "toxic_order_denominator",
                "candidate": "",
                "bucket_family": _pick(row, "bucket_family"),
                "bucket": _pick(row, "bucket"),
                "placed_orders": f"{placed:.0f}",
                "filled_orders": f"{fills:.0f}",
                "fill_rate": f"{safe_float(row, 'fill_rate'):.6f}",
                "tail_rate": f"{tail_rate:.6f}",
                "positive_false_block_rate": f"{false_block:.6f}",
                "shadow_improvement_proxy": f"{improvement:.6f}",
                "inventory_time_proxy": f"{safe_float(row, 'inventory_time_btc_s_proxy'):.6f}",
                "post_fill_20s_proxy": f"{safe_float(row, 'post_fill_20s_btc_s_proxy'):.6f}",
                "daily_support": f"{safe_float(row, 'order_days'):.0f}",
                "verdict": _verdict_from_rates(
                    placed=placed,
                    fills=fills,
                    daily_support=safe_float(row, "order_days"),
                    tail_rate=tail_rate,
                    false_block_rate=false_block,
                    improvement=improvement,
                ),
            }
        )
    for row in model_compare_rows:
        rows.append(
            {
                "evidence_type": "toxic_model_metric",
                "candidate": f"{_pick(row, 'side')}:{_pick(row, 'metric')}",
                "bucket_family": "",
                "bucket": "",
                "placed_orders": "",
                "filled_orders": _pick(row, "new_valid_rows"),
                "fill_rate": "",
                "tail_rate": "",
                "positive_false_block_rate": "",
                "shadow_improvement_proxy": f"{safe_float(row, 'delta_new_minus_old'):.6f}",
                "inventory_time_proxy": "",
                "post_fill_20s_proxy": "",
                "daily_support": "",
                "verdict": "model_metric_only",
            }
        )
    rows.sort(key=_evidence_sort_key)
    return rows


def toxic_risk_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    avoidance = [r for r in rows if r["evidence_type"] == "toxic_shadow_avoidance"]
    denom = [r for r in rows if r["evidence_type"] == "toxic_order_denominator"]
    return {
        "normalized_rows": len(rows),
        "shadow_avoidance_candidates": len(avoidance),
        "order_denominator_buckets": len(denom),
        "watch_positive_proxy": sum(1 for r in rows if r.get("verdict") == "watch_positive_proxy"),
        "sparse_watch": sum(1 for r in rows if r.get("verdict") == "sparse_watch"),
        "tail_or_false_block_high": sum(
            1 for r in rows if r.get("verdict") == "tail_or_false_block_high"
        ),
    }


def shadow_avoidance_evidence_rows(
    *,
    candidates_rows: list[dict[str, Any]],
    daily_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    daily_by_bucket: dict[tuple[str, str], dict[str, float]] = {}
    for row in daily_rows:
        key = (_pick(row, "bucket_family"), _pick(row, "bucket"))
        stats = daily_by_bucket.setdefault(
            key, {"days": 0.0, "positive_days": 0.0, "negative_days": 0.0}
        )
        stats["days"] += 1.0
        markout = safe_float(row, "avg_markout_30s", safe_float(row, "jl_avg_markout_30s"))
        if markout > 0:
            stats["positive_days"] += 1.0
        elif markout < 0:
            stats["negative_days"] += 1.0
    rows: list[dict[str, Any]] = []
    for row in candidates_rows:
        family = _pick(row, "bucket_family")
        bucket = _pick(row, "bucket")
        stats = daily_by_bucket.get((family, bucket), {})
        placed = safe_float(row, "placed_orders", safe_float(row, "jl_placed_orders"))
        fills = safe_float(row, "filled_orders", safe_float(row, "jl_filled_orders"))
        tail_rate = safe_float(row, "jl_tail_rate", safe_float(row, "tail_rate"))
        false_block = safe_float(
            row, "jl_positive_false_block_rate", safe_float(row, "positive_false_block_rate")
        )
        improvement = safe_float(
            row, "jl_improvement_if_avoided", safe_float(row, "shadow_improvement_if_avoided")
        )
        strict_pass = str(row.get("candidate_pass", "0")).lower() in {"1", "true", "yes"}
        support = safe_float(row, "jl_support_days", stats.get("days", 0.0))
        rows.append(
            {
                "evidence_type": "shadow_avoidance_bucket",
                "candidate": "",
                "bucket_family": family,
                "bucket": bucket,
                "placed_orders": f"{placed:.0f}",
                "filled_orders": f"{fills:.0f}",
                "fill_rate": f"{safe_float(row, 'jl_fill_rate', safe_float(row, 'fill_rate')):.6f}",
                "tail_rate": f"{tail_rate:.6f}",
                "positive_false_block_rate": f"{false_block:.6f}",
                "shadow_improvement_proxy": f"{improvement:.6f}",
                "inventory_time_proxy": f"{safe_float(row, 'jl_inventory_time_btc_s_proxy', safe_float(row, 'inventory_time_btc_s_proxy')):.6f}",
                "post_fill_20s_proxy": f"{safe_float(row, 'jl_post_fill_20s_btc_s_proxy', safe_float(row, 'post_fill_20s_btc_s_proxy')):.6f}",
                "daily_support": f"{support:.0f}",
                "positive_days": f"{stats.get('positive_days', safe_float(row, 'jl_positive_markout_days')):.0f}",
                "negative_days": f"{stats.get('negative_days', safe_float(row, 'jl_negative_markout_days')):.0f}",
                "verdict": _verdict_from_rates(
                    placed=placed,
                    fills=fills,
                    daily_support=support,
                    tail_rate=tail_rate,
                    false_block_rate=false_block,
                    improvement=improvement,
                    strict_pass=strict_pass,
                ),
            }
        )
    rows.sort(key=_evidence_sort_key)
    return rows


def shadow_avoidance_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "normalized_rows": len(rows),
        "strict_pass_review_required": sum(
            1 for r in rows if r.get("verdict") == "strict_pass_review_required"
        ),
        "watch_positive_proxy": sum(1 for r in rows if r.get("verdict") == "watch_positive_proxy"),
        "sparse_watch": sum(1 for r in rows if r.get("verdict") == "sparse_watch"),
        "tail_or_false_block_high": sum(
            1 for r in rows if r.get("verdict") == "tail_or_false_block_high"
        ),
    }


def bucket_evidence_rows(
    *,
    research_clue_rows: list[dict[str, Any]],
    daily_support_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    daily_map: dict[tuple[str, str], dict[str, Any]] = {}
    for row in daily_support_rows:
        daily_map[(_pick(row, "bucket_family"), _pick(row, "bucket"))] = row
    rows: list[dict[str, Any]] = []
    for row in research_clue_rows:
        family = _pick(row, "bucket_family")
        bucket = _pick(row, "bucket")
        daily = daily_map.get((family, bucket), {})
        placed = safe_float(row, "total_placed_orders")
        fills = safe_float(row, "total_fills")
        support = safe_float(row, "total_support_days", safe_float(daily, "days"))
        tail_rate = safe_float(
            row, "worst_tail_loss_rate", safe_float(daily, "avg_tail_loss_30s_rate")
        )
        positive_daily = safe_float(row, "positive_daily_ratio")
        verdict = _pick(row, "clue_status", "promotion_status", default="")
        if not verdict:
            verdict = _verdict_from_rates(
                placed=placed,
                fills=fills,
                daily_support=support,
                tail_rate=tail_rate,
                false_block_rate=1.0 - positive_daily if positive_daily else 0.0,
                improvement=safe_float(row, "min_positive_cohort_markout_30s"),
            )
        rows.append(
            {
                "evidence_type": "local_bucket_research_clue",
                "candidate": verdict,
                "bucket_family": family,
                "bucket": bucket,
                "placed_orders": f"{placed:.0f}",
                "filled_orders": f"{fills:.0f}",
                "fill_rate": f"{fills / placed if placed > 0 else 0.0:.6f}",
                "tail_rate": f"{tail_rate:.6f}",
                "positive_false_block_rate": f"{1.0 - positive_daily if positive_daily else 0.0:.6f}",
                "shadow_improvement_proxy": f"{safe_float(row, 'min_positive_cohort_markout_30s'):.6f}",
                "inventory_time_proxy": f"{safe_float(row, 'sum_order_inventory_time_s_jan_apr_train') + safe_float(row, 'sum_order_inventory_time_s_may_oos') + safe_float(row, 'sum_order_inventory_time_s_june_selection') + safe_float(row, 'sum_order_inventory_time_s_later_oos'):.6f}",
                "post_fill_20s_proxy": "",
                "daily_support": f"{support:.0f}",
                "positive_days": f"{safe_float(daily, 'positive_days'):.0f}",
                "tail_days": f"{safe_float(daily, 'tail_days'):.0f}",
                "avg_daily_markout_30s": f"{safe_float(daily, 'avg_daily_markout_30s'):.6f}",
                "worst_daily_markout_30s": f"{safe_float(row, 'worst_daily_markout_30s', safe_float(daily, 'worst_daily_markout_30s')):.6f}",
                "verdict": verdict,
            }
        )
    rows.sort(key=_evidence_sort_key)
    return rows


def bucket_evidence_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    verdicts = Counter(str(r.get("verdict", "")) for r in rows)
    return {
        "normalized_rows": len(rows),
        "verdict_counts": dict(verdicts.most_common(12)),
        "total_filled_orders": sum(safe_float(r, "filled_orders") for r in rows),
        "total_placed_orders": sum(safe_float(r, "placed_orders") for r in rows),
    }
