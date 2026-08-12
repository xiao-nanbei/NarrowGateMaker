"""
Inventory / Position Manager — 持仓状态机 + 本地缓存。

交易状态机:
  FLAT → OPEN → TIMEOUT_CLOSING → FLAT

持仓缓存:
  - 本地跟踪净持仓、均价、未实现PnL
  - 每次成交事件更新
  - 定期与交易所同步校验
"""

import csv
import logging
import os
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from threading import Lock
from typing import Optional

logger = logging.getLogger("inventory_manager")


class PositionState(Enum):
    FLAT = auto()              # 无持仓
    OPEN = auto()              # 持仓中
    TIMEOUT_CLOSING = auto()   # 超时强制平仓中


@dataclass
class PositionSnapshot:
    """当前持仓快照"""
    qty: float = 0.0               # 净持仓 (>0 long, <0 short)
    avg_entry_price: float = 0.0   # 加权平均入场价
    unrealized_pnl: float = 0.0    # 未实现PnL
    realized_pnl: float = 0.0      # 累计已实现PnL (当日)
    state: PositionState = PositionState.FLAT
    open_time: float = 0.0         # 建仓时间
    total_traded_volume: float = 0.0  # 累计成交量
    peak_unrealized_pnl: float = 0.0  # 本仓位峰值未实现PnL


@dataclass
class CampaignSnapshot:
    """flat -> nonzero -> flat 的库存 campaign 快照。

    中文说明：单笔 fill markout 容易低估库存风险。campaign 把一段从
    建仓到完全回到 flat 的连续库存周期视为一个风险单元，用来观察大库存
    是否在自然减仓过程中持续亏损。
    """
    active: bool = False
    campaign_id: int = 0
    side: str = "FLAT"
    age_s: float = 0.0
    start_time: float = 0.0
    max_abs_qty: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_pnl: float = 0.0
    adverse_excursion: float = 0.0
    fills: int = 0
    buy_fills: int = 0
    sell_fills: int = 0
    exposure_increasing_fills: int = 0
    reducing_fills: int = 0
    volume: float = 0.0


class InventoryManager:
    """
    Thread-safe position tracker with state machine.

    Usage:
        im = InventoryManager(max_inventory=0.1, position_timeout=100.0)
        im.on_fill(side="BUY", qty=0.005, price=85000.0, commission=0.04)
        im.update_mark_price(84990.0)
        snap = im.snapshot
    """

    def __init__(self, max_inventory: float = 0.1,
                 position_timeout: float = 100.0,
                 trade_log_path: Optional[str] = None):
        self._lock = Lock()
        self.max_inventory = max_inventory
        self.position_timeout = position_timeout

        # position state
        self._qty: float = 0.0
        self._avg_entry: float = 0.0
        self._cost_basis: float = 0.0      # total cost for avg calc
        self._realized_pnl: float = 0.0
        self._unrealized_pnl: float = 0.0
        self._state = PositionState.FLAT
        self._open_time: float = 0.0
        self._mark_price: float = 0.0
        self._total_volume: float = 0.0
        self._total_commission: float = 0.0

        # UTC-day tracking.  The baseline is total marked PnL, so carrying an
        # open position across midnight does not re-count its prior-day PnL.
        now = time.time()
        self._daily_utc_day: int = int(now // 86_400)
        self._day_start_total_pnl: float = 0.0
        self._consecutive_losses: int = 0
        self._last_trade_pnl: float = 0.0
        self._day_buy_fill_qty: float = 0.0
        self._day_sell_fill_qty: float = 0.0
        self._day_buy_fill_notional: float = 0.0
        self._day_sell_fill_notional: float = 0.0

        # open-side commission pool (accumulated from OPEN/ADD fills)
        self._open_commission: float = 0.0
        # round-trip PnL accumulator (summed across partial closes)
        self._round_trip_rpnl: float = 0.0

        # peak PnL for drawdown calculation
        self._peak_pnl: float = 0.0

        # sync guard: skip fills that occurred before the last exchange sync
        self._last_sync_request_time: float = 0.0
        self._sync_adjust_seq: int = 0
        self._last_sync_adjust_time: float = 0.0
        self._last_sync_adjust_delta: float = 0.0
        self._sync_adjust_events: deque[tuple[float, float]] = deque(maxlen=256)

        # per-position peak unrealized PnL for trailing stop
        self._peak_unrealized_pnl: float = 0.0

        # Inventory-time exposure, accumulated since process start.
        # 中文说明：库存风险不是只看瞬时持仓。abs/squared/notional
        # inventory-time 用于比较不同机制是否只是把风险拖得更久。
        self._inventory_time_start_ts: float = now
        self._inventory_time_last_ts: float = now
        self._signed_inventory_time_s: float = 0.0
        self._abs_inventory_time_s: float = 0.0
        self._sq_inventory_time_s: float = 0.0
        self._signed_notional_inventory_time_s: float = 0.0
        self._notional_inventory_time_s: float = 0.0

        # Campaign-level inventory risk state.  A campaign starts when position
        # moves from flat to nonzero and ends when it returns to flat.
        # 中文说明：这些字段只做风险审计，不直接改变报价。
        self._campaign_id: int = 0
        self._campaign_active: bool = False
        self._campaign_start_time: float = 0.0
        self._campaign_start_realized_pnl: float = 0.0
        self._campaign_start_side: str = "FLAT"
        self._campaign_max_abs_qty: float = 0.0
        self._campaign_min_total_pnl: float = 0.0
        self._campaign_total_pnl: float = 0.0
        self._campaign_realized_pnl: float = 0.0
        self._campaign_unrealized_pnl: float = 0.0
        self._campaign_fills: int = 0
        self._campaign_buy_fills: int = 0
        self._campaign_sell_fills: int = 0
        self._campaign_exposure_increasing_fills: int = 0
        self._campaign_reducing_fills: int = 0
        self._campaign_volume: float = 0.0

        # trade log
        self._trade_log_path = trade_log_path
        if trade_log_path:
            self._init_trade_log(trade_log_path)

    def _init_trade_log(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        if not os.path.exists(path):
            with open(path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["timestamp", "side", "trade_type", "qty", "price",
                            "commission", "position", "avg_entry",
                            "realized_pnl", "unrealized_pnl", "state"])

    # ── fill processing ──

    def _inventory_time_mark_locked(self) -> float:
        if self._mark_price > 0.0:
            return self._mark_price
        if self._avg_entry > 0.0:
            return self._avg_entry
        return 0.0

    def _total_pnl_locked(self) -> float:
        return self._realized_pnl + self._unrealized_pnl

    def _reset_daily_locked(self, utc_day: int) -> None:
        self._daily_utc_day = int(utc_day)
        self._day_start_total_pnl = self._total_pnl_locked()
        self._consecutive_losses = 0
        self._peak_pnl = self._realized_pnl
        self._day_buy_fill_qty = 0.0
        self._day_sell_fill_qty = 0.0
        self._day_buy_fill_notional = 0.0
        self._day_sell_fill_notional = 0.0
        logger.info(
            "DAILY_RESET utc_day=%d start_total_pnl=%.6f",
            self._daily_utc_day,
            self._day_start_total_pnl,
        )

    def _roll_daily_if_needed_locked(self, timestamp_s: float) -> bool:
        utc_day = int(max(0.0, float(timestamp_s)) // 86_400)
        # Never rewind the accounting day for a delayed exchange event.
        if utc_day <= self._daily_utc_day:
            return False
        self._reset_daily_locked(utc_day)
        return True

    def _accrue_inventory_time_locked(self, now: float):
        """Accrue inventory exposure before mutating qty or mark price.

        中文说明：所有会改变 qty/mark 的入口都应先调用这里，否则库存时间积分
        会把新状态错误地套用到过去时间段。
        """
        dt_s = max(0.0, now - self._inventory_time_last_ts)
        if dt_s <= 0.0:
            return
        q = self._qty
        mark = self._inventory_time_mark_locked()
        self._signed_inventory_time_s += q * dt_s
        self._abs_inventory_time_s += abs(q) * dt_s
        self._sq_inventory_time_s += q * q * dt_s
        if mark > 0.0:
            self._signed_notional_inventory_time_s += q * mark * dt_s
            self._notional_inventory_time_s += abs(q) * mark * dt_s
        self._inventory_time_last_ts = now

    def _campaign_update_pnl_locked(self) -> None:
        if not self._campaign_active:
            return
        self._campaign_realized_pnl = self._realized_pnl - self._campaign_start_realized_pnl
        self._campaign_unrealized_pnl = self._unrealized_pnl
        self._campaign_total_pnl = self._campaign_realized_pnl + self._campaign_unrealized_pnl
        self._campaign_min_total_pnl = min(self._campaign_min_total_pnl, self._campaign_total_pnl)
        self._campaign_max_abs_qty = max(self._campaign_max_abs_qty, abs(self._qty))

    def _campaign_start_locked(self, now: float, qty: float) -> None:
        self._campaign_id += 1
        self._campaign_active = True
        self._campaign_start_time = now
        self._campaign_start_realized_pnl = self._realized_pnl
        self._campaign_start_side = "LONG" if qty > 0 else "SHORT"
        self._campaign_max_abs_qty = abs(qty)
        self._campaign_min_total_pnl = 0.0
        self._campaign_total_pnl = 0.0
        self._campaign_realized_pnl = 0.0
        self._campaign_unrealized_pnl = self._unrealized_pnl
        self._campaign_fills = 0
        self._campaign_buy_fills = 0
        self._campaign_sell_fills = 0
        self._campaign_exposure_increasing_fills = 0
        self._campaign_reducing_fills = 0
        self._campaign_volume = 0.0
        logger.info(
            "CAMPAIGN_START id=%d side=%s qty=%+.6f",
            self._campaign_id,
            self._campaign_start_side,
            qty,
        )

    def _campaign_finish_locked(self, now: float) -> None:
        if not self._campaign_active:
            return
        self._campaign_update_pnl_locked()
        logger.info(
            "CAMPAIGN_END id=%d side=%s duration=%.1fs max_abs_qty=%.6f "
            "pnl=%.4f mae=%.4f fills=%d inc=%d red=%d",
            self._campaign_id,
            self._campaign_start_side,
            max(0.0, now - self._campaign_start_time),
            self._campaign_max_abs_qty,
            self._campaign_total_pnl,
            min(0.0, self._campaign_min_total_pnl),
            self._campaign_fills,
            self._campaign_exposure_increasing_fills,
            self._campaign_reducing_fills,
        )
        self._campaign_active = False
        self._campaign_start_side = "FLAT"

    def _campaign_on_position_change_locked(
        self,
        prev_qty: float,
        new_qty: float,
        side: str,
        qty: float,
        now: float,
        count_fill: bool,
    ) -> None:
        """Update campaign state after a fill or sync correction.

        中文说明：SYNC_ADJUST 可能改变 flat/nonzero 边界，但不是可交易
        fill，所以默认不计入 exposure-increasing/reducing fill 计数。
        """
        eps = 1e-10
        if not self._campaign_active and abs(prev_qty) < eps and abs(new_qty) >= eps:
            self._campaign_start_locked(now, new_qty)

        if self._campaign_active and count_fill:
            self._campaign_fills += 1
            self._campaign_volume += qty
            if side == "BUY":
                self._campaign_buy_fills += 1
            elif side == "SELL":
                self._campaign_sell_fills += 1
            if abs(new_qty) > abs(prev_qty) + eps:
                self._campaign_exposure_increasing_fills += 1
            elif abs(new_qty) < abs(prev_qty) - eps:
                self._campaign_reducing_fills += 1

        if self._campaign_active:
            self._campaign_update_pnl_locked()
            if abs(new_qty) < eps:
                self._campaign_finish_locked(now)

    def on_fill(self, side: str, qty: float, price: float,
                commission: float = 0.0, trade_time_ms: int = 0):
        """
        Process a fill event. Updates position cache and state machine.

        side: "BUY" or "SELL"
        qty: filled quantity (always positive)
        price: fill price
        commission: commission converted to the contract quote/settlement asset
        trade_time_ms: exchange trade time in epoch milliseconds (from WS event)
        """
        now = time.time()
        signed_qty = qty if side == "BUY" else -qty

        with self._lock:
            event_time_s = trade_time_ms / 1000.0 if trade_time_ms > 0 else now
            self._roll_daily_if_needed_locked(event_time_s)
            # Guard against double-counting: if sync_from_exchange recently
            # corrected our position, this fill may already be reflected.
            # Skip fills whose exchange trade time predates our sync REST call.
            if (trade_time_ms > 0 and self._last_sync_request_time > 0
                    and trade_time_ms / 1000.0 < self._last_sync_request_time + 1.0):
                logger.info(
                    f"FILL_SKIP_POST_SYNC {side} {qty:.4f}@{price:.1f} "
                    f"trade_t={trade_time_ms} sync_t={self._last_sync_request_time:.3f}"
                )
                return

            prev_qty = self._qty
            prev_state = self._state
            self._accrue_inventory_time_locked(now)

            # --- Determine trade type based on current position ---
            if abs(self._qty) < 1e-10:
                trade_type = "OPEN"
            elif (self._qty > 0 and signed_qty > 0) or \
                 (self._qty < 0 and signed_qty < 0):
                trade_type = "OPEN"  # adding to position
            else:
                # opposite direction: closing or flipping
                if abs(signed_qty) > abs(self._qty) + 1e-10:
                    trade_type = "FLIP"  # close + reverse
                else:
                    trade_type = "CLOSE"

            self._total_commission += commission
            self._total_volume += qty
            if side == "BUY":
                self._day_buy_fill_qty += qty
                self._day_buy_fill_notional += qty * price
            elif side == "SELL":
                self._day_sell_fill_qty += qty
                self._day_sell_fill_notional += qty * price
            # --- PnL calculation ---
            if self._qty == 0.0:
                # opening new position
                self._qty = signed_qty
                self._avg_entry = price
                self._cost_basis = price * abs(signed_qty)
                self._open_time = now
                self._open_commission = commission
                self._round_trip_rpnl = 0.0

            elif (self._qty > 0 and signed_qty > 0) or \
                 (self._qty < 0 and signed_qty < 0):
                # adding to position (same direction)
                total_qty = abs(self._qty) + qty
                self._cost_basis += price * qty
                self._avg_entry = self._cost_basis / total_qty
                self._qty += signed_qty
                self._open_commission += commission

            else:
                # reducing or flipping position
                close_qty = min(qty, abs(self._qty))
                # Proportional share of accumulated open-side commission
                if abs(self._qty) > 1e-10:
                    open_comm_share = self._open_commission * (close_qty / abs(self._qty))
                else:
                    open_comm_share = self._open_commission
                # realized PnL for closed portion (includes both open + close commission)
                if self._qty > 0:
                    rpnl = (price - self._avg_entry) * close_qty - commission - open_comm_share
                else:
                    rpnl = (self._avg_entry - price) * close_qty - commission - open_comm_share

                self._open_commission -= open_comm_share
                self._realized_pnl += rpnl
                self._last_trade_pnl = rpnl
                self._round_trip_rpnl += rpnl

                # track peak PnL
                if self._realized_pnl > self._peak_pnl:
                    self._peak_pnl = self._realized_pnl

                remaining = abs(self._qty) - close_qty
                if remaining < 1e-10:
                    # fully closed (possibly flipped)
                    # ── consecutive losses: count per round-trip, not per partial fill ──
                    if self._round_trip_rpnl < 0:
                        self._consecutive_losses += 1
                    else:
                        self._consecutive_losses = 0

                    flip_qty = qty - close_qty
                    if flip_qty > 1e-10:
                        # flipped direction
                        self._qty = signed_qty + (self._qty)  # net
                        self._avg_entry = price
                        self._cost_basis = price * abs(self._qty)
                        self._open_time = now
                        # Split remaining commission for new position entry
                        self._open_commission = 0.0
                        self._round_trip_rpnl = 0.0
                    else:
                        self._qty = 0.0
                        self._avg_entry = 0.0
                        self._cost_basis = 0.0
                        self._open_commission = 0.0
                        self._round_trip_rpnl = 0.0
                else:
                    self._qty += signed_qty
                    self._cost_basis = self._avg_entry * abs(self._qty)

            # --- State machine transitions ---
            self._update_state()

            # --- Update unrealized PnL ---
            self._recalc_unrealized()
            self._campaign_on_position_change_locked(
                prev_qty,
                self._qty,
                side,
                qty,
                now,
                count_fill=True,
            )

            logger.info(
                f"FILL {side} {qty:.4f}@{price:.1f} [{trade_type}] "
                f"pos={self._qty:+.4f}@{self._avg_entry:.1f} "
                f"rpnl={self._realized_pnl:.2f} "
                f"{prev_state.name}→{self._state.name}"
            )

            # --- Log trade ---
            if self._trade_log_path:
                self._log_trade(now, side, trade_type, qty, price, commission)

    def _update_state(self):
        """Transition position state based on current qty."""
        if abs(self._qty) < 1e-10:
            self._state = PositionState.FLAT
            self._qty = 0.0
            self._peak_unrealized_pnl = 0.0
        elif self._state == PositionState.FLAT:
            self._state = PositionState.OPEN
        elif self._state == PositionState.TIMEOUT_CLOSING:
            if abs(self._qty) < 1e-10:
                self._state = PositionState.FLAT
            # else still closing (partial)

    def set_timeout_closing(self):
        """Transition to TIMEOUT_CLOSING (position held too long)."""
        with self._lock:
            if self._state == PositionState.OPEN:
                self._state = PositionState.TIMEOUT_CLOSING
                logger.warning(
                    f"POSITION_TIMEOUT pos={self._qty:+.4f} "
                    f"held={time.time() - self._open_time:.0f}s"
                )

    # ── mark price & unrealized PnL ──

    def update_mark_price(self, price: float):
        now = time.time()
        with self._lock:
            self._roll_daily_if_needed_locked(now)
            self._accrue_inventory_time_locked(now)
            self._mark_price = price
            self._recalc_unrealized()

    def _recalc_unrealized(self):
        if self._qty != 0.0 and self._mark_price > 0:
            if self._qty > 0:
                self._unrealized_pnl = (self._mark_price - self._avg_entry) * self._qty
            else:
                self._unrealized_pnl = (self._avg_entry - self._mark_price) * abs(self._qty)
            # track peak unrealized for trailing stop
            if self._unrealized_pnl > self._peak_unrealized_pnl:
                self._peak_unrealized_pnl = self._unrealized_pnl
        else:
            self._unrealized_pnl = 0.0
        self._campaign_update_pnl_locked()

    # ── queries ──

    @property
    def snapshot(self) -> PositionSnapshot:
        with self._lock:
            return PositionSnapshot(
                qty=self._qty,
                avg_entry_price=self._avg_entry,
                unrealized_pnl=self._unrealized_pnl,
                realized_pnl=self._realized_pnl,
                state=self._state,
                open_time=self._open_time,
                total_traded_volume=self._total_volume,
                peak_unrealized_pnl=self._peak_unrealized_pnl,
            )

    def inventory_exposure_snapshot(self) -> dict:
        """Return inventory-time exposure accumulated since process start.

        Inventory-time units are BTC*seconds.  Notional inventory-time units are
        quote-currency*seconds, using the latest mark or entry price.
        """
        now = time.time()
        with self._lock:
            self._roll_daily_if_needed_locked(now)
            dt_s = max(0.0, now - self._inventory_time_last_ts)
            q = self._qty
            mark = self._inventory_time_mark_locked()
            signed_inv_time = self._signed_inventory_time_s + q * dt_s
            abs_inv_time = self._abs_inventory_time_s + abs(q) * dt_s
            sq_inv_time = self._sq_inventory_time_s + q * q * dt_s
            signed_notional_time = self._signed_notional_inventory_time_s
            notional_time = self._notional_inventory_time_s
            if mark > 0.0:
                signed_notional_time += q * mark * dt_s
                notional_time += abs(q) * mark * dt_s
            elapsed_s = max(0.0, now - self._inventory_time_start_ts)
            inv_time_hours = abs_inv_time / 3600.0
            daily_pnl = self._total_pnl_locked() - self._day_start_total_pnl
            return {
                "elapsed_s": elapsed_s,
                "signed_inventory_time_s": signed_inv_time,
                "abs_inventory_time_s": abs_inv_time,
                "sq_inventory_time_s": sq_inv_time,
                "signed_notional_inventory_time_s": signed_notional_time,
                "notional_inventory_time_s": notional_time,
                "time_avg_signed_inventory": signed_inv_time / max(elapsed_s, 1e-9),
                "time_avg_abs_inventory": abs_inv_time / max(elapsed_s, 1e-9),
                "time_avg_sq_inventory": sq_inv_time / max(elapsed_s, 1e-9),
                "time_avg_notional_inventory": notional_time / max(elapsed_s, 1e-9),
                "daily_pnl_per_abs_inventory_hour": (
                    daily_pnl / inv_time_hours if inv_time_hours > 1e-12 else 0.0
                ),
                "daily_buy_fill_qty": self._day_buy_fill_qty,
                "daily_sell_fill_qty": self._day_sell_fill_qty,
                "daily_buy_avg_fill_price": (
                    self._day_buy_fill_notional / self._day_buy_fill_qty
                    if self._day_buy_fill_qty > 1e-12 else 0.0
                ),
                "daily_sell_avg_fill_price": (
                    self._day_sell_fill_notional / self._day_sell_fill_qty
                    if self._day_sell_fill_qty > 1e-12 else 0.0
                ),
            }

    def campaign_snapshot(self) -> CampaignSnapshot:
        """Return current campaign risk state for HEALTH/shadow logging."""
        now = time.time()
        with self._lock:
            self._campaign_update_pnl_locked()
            age_s = (
                max(0.0, now - self._campaign_start_time)
                if self._campaign_active else 0.0
            )
            return CampaignSnapshot(
                active=self._campaign_active,
                campaign_id=self._campaign_id,
                side=self._campaign_start_side if self._campaign_active else "FLAT",
                age_s=age_s,
                start_time=self._campaign_start_time if self._campaign_active else 0.0,
                max_abs_qty=self._campaign_max_abs_qty if self._campaign_active else 0.0,
                realized_pnl=self._campaign_realized_pnl if self._campaign_active else 0.0,
                unrealized_pnl=self._campaign_unrealized_pnl if self._campaign_active else 0.0,
                total_pnl=self._campaign_total_pnl if self._campaign_active else 0.0,
                adverse_excursion=(
                    min(0.0, self._campaign_min_total_pnl)
                    if self._campaign_active else 0.0
                ),
                fills=self._campaign_fills if self._campaign_active else 0,
                buy_fills=self._campaign_buy_fills if self._campaign_active else 0,
                sell_fills=self._campaign_sell_fills if self._campaign_active else 0,
                exposure_increasing_fills=(
                    self._campaign_exposure_increasing_fills
                    if self._campaign_active else 0
                ),
                reducing_fills=(
                    self._campaign_reducing_fills
                    if self._campaign_active else 0
                ),
                volume=self._campaign_volume if self._campaign_active else 0.0,
            )

    @property
    def net_position(self) -> float:
        with self._lock:
            return self._qty

    @property
    def position_age(self) -> float:
        """Seconds since position was opened."""
        with self._lock:
            if self._state == PositionState.FLAT:
                return 0.0
            return time.time() - self._open_time

    @property
    def is_timeout(self) -> bool:
        """True if position held longer than timeout. 0 = disabled."""
        if self.position_timeout <= 0:
            return False
        return self.position_age > self.position_timeout

    @property
    def daily_pnl(self) -> float:
        with self._lock:
            self._roll_daily_if_needed_locked(time.time())
            return self._total_pnl_locked() - self._day_start_total_pnl

    @property
    def drawdown(self) -> float:
        with self._lock:
            return max(0, self._peak_pnl - self._realized_pnl)

    @property
    def consecutive_losses(self) -> int:
        with self._lock:
            return self._consecutive_losses

    def reset_consecutive_losses(self):
        with self._lock:
            self._consecutive_losses = 0

    def force_flat(self):
        """Force position to FLAT (for dust cleanup below lot_size)."""
        with self._lock:
            logger.info(f"FORCE_FLAT dust={self._qty:+.6f}")
            self._accrue_inventory_time_locked(time.time())
            prev_qty = self._qty
            self._qty = 0.0
            self._avg_entry = 0.0
            self._cost_basis = 0.0
            self._unrealized_pnl = 0.0
            self._peak_unrealized_pnl = 0.0
            self._state = PositionState.FLAT
            self._campaign_on_position_change_locked(
                prev_qty,
                self._qty,
                "FLAT",
                abs(prev_qty),
                time.time(),
                count_fill=False,
            )

    # ── sync with exchange ──

    def sync_from_exchange(self, exchange_qty: float, exchange_entry: float,
                           sync_request_time: float = 0.0):
        """Reconcile local state with exchange position.

        sync_request_time: time.time() captured BEFORE the REST call,
            used to discard stale WS fill events that arrive after sync.

        中文说明：这里不是常规成交路径，而是“用户流可能漏单”的审计/修正路径。
        一旦产生 SYNC_ADJUST，MakerEngine 会按连续次数或绝对偏差进入
        exposure-increasing degrade，但仍允许减库存方向报价。
        """
        with self._lock:
            # Skip sync during active closing — on_fill from WS is more
            # current than REST/ACCOUNT_UPDATE position data
            if self._state == PositionState.TIMEOUT_CLOSING:
                if abs(exchange_qty - self._qty) > 1e-6:
                    logger.debug(
                        f"SYNC_SKIP_CLOSING local={self._qty:+.4f} "
                        f"exchange={exchange_qty:+.4f}"
                    )
                return

            if abs(exchange_qty - self._qty) > 1e-6:
                old_qty = self._qty
                self._accrue_inventory_time_locked(time.time())
                logger.warning(
                    f"POSITION_SYNC local={self._qty:+.4f} "
                    f"exchange={exchange_qty:+.4f}"
                )
                was_flat = abs(self._qty) < 1e-10
                self._qty = exchange_qty
                self._avg_entry = exchange_entry
                self._cost_basis = exchange_entry * abs(exchange_qty)
                # Record sync time so on_fill can skip stale WS events
                if sync_request_time > 0:
                    self._last_sync_request_time = sync_request_time
                # Set open_time when transitioning from FLAT to OPEN
                # to avoid immediate POSITION_TIMEOUT
                if was_flat and abs(exchange_qty) > 1e-10:
                    self._open_time = time.time()
                self._update_state()
                self._recalc_unrealized()

                delta = exchange_qty - old_qty
                if abs(delta) > 1e-10:
                    side = "BUY" if delta > 0 else "SELL"
                    self._campaign_on_position_change_locked(
                        old_qty,
                        self._qty,
                        side,
                        abs(delta),
                        time.time(),
                        count_fill=False,
                    )
                    # If WS ORDER_TRADE_UPDATE was missed, keep an audit trail in trades.csv.
                    if self._trade_log_path:
                        px = exchange_entry if exchange_entry > 0 else self._mark_price
                        self._log_trade(time.time(), side, "SYNC_ADJUST",
                                        abs(delta), px, 0.0)
                    event_ts = time.time()
                    self._sync_adjust_seq += 1
                    self._last_sync_adjust_time = event_ts
                    self._last_sync_adjust_delta = delta
                    self._sync_adjust_events.append((event_ts, delta))

    def sync_adjust_snapshot(self, window_s: float = 300.0) -> dict:
        """Return recent exchange-sync corrections for live risk gating."""
        now = time.time()
        window_s = max(0.0, float(window_s or 0.0))
        with self._lock:
            if window_s > 0.0:
                cutoff = now - window_s
                recent = [(ts, delta) for ts, delta in self._sync_adjust_events if ts >= cutoff]
            else:
                recent = list(self._sync_adjust_events)
            return {
                "seq": self._sync_adjust_seq,
                "last_time": self._last_sync_adjust_time,
                "last_delta": self._last_sync_adjust_delta,
                "last_age_s": (
                    now - self._last_sync_adjust_time
                    if self._last_sync_adjust_time > 0.0 else float("inf")
                ),
                "recent_count": len(recent),
                "recent_abs_qty": sum(abs(delta) for _, delta in recent),
            }

    # ── daily reset ──

    def reset_daily(self):
        with self._lock:
            self._reset_daily_locked(int(time.time() // 86_400))

    # ── trade log ──

    def _log_trade(self, ts, side, trade_type, qty, price, commission):
        try:
            with open(self._trade_log_path, "a", newline="") as f:
                w = csv.writer(f)
                w.writerow([
                    f"{ts:.3f}", side, trade_type, f"{qty:.4f}",
                    f"{price:.1f}", f"{commission:.4f}",
                    f"{self._qty:+.4f}", f"{self._avg_entry:.1f}",
                    f"{self._realized_pnl:.2f}",
                    f"{self._unrealized_pnl:.2f}", self._state.name,
                ])
        except Exception as e:
            logger.error(f"Trade log write failed: {e}")
