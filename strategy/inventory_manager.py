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
import math
import os
import time
from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum, auto
from threading import Lock

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


@dataclass(frozen=True)
class _TradeIdentity:
    """Immutable exchange identity for one trade ID for the process lifetime."""

    side: str
    order_id: str
    quantity: float
    price: float
    commission: float
    trade_time_ms: int
    cumulative_filled_qty: float


@dataclass(frozen=True)
class _AppliedFill:
    """A locally applied fill fragment that may be newer than a REST snapshot."""

    sequence: int
    side: str
    quantity: float
    price: float
    commission: float
    trade_time_ms: int
    order_id: str | None = None
    trade_id: str | None = None
    cumulative_start: float | None = None
    cumulative_end: float | None = None
    trade_identity: _TradeIdentity | None = None


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
                 trade_log_path: str | None = None):
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

        # Process-session marked-equity high-water mark.  Unlike daily
        # accounting, this safety state must survive UTC midnight.
        self._peak_pnl: float = 0.0

        # Exchange reconciliation identity.  A local request clock cannot say
        # whether a delayed WS fill is already represented by a position REST
        # snapshot.  Keep exact order cumulative-fill cursors instead.
        self._last_reconciliation_snapshot_update_time_ms: int = 0
        self._snapshot_order_cumulative_filled_qty: dict[str, float] = {}
        self._order_cumulative_filled_qty: dict[str, float] = {}
        self._applied_fills_since_barrier: list[_AppliedFill] = []
        self._fill_identity_sequence: int = 0
        # Never evict a trade identity during the process lifetime.  Reusing an
        # old exchange trade ID must remain a duplicate (or an identity drift)
        # even after millions of later fills.
        self._trade_identity_by_id: dict[str, _TradeIdentity] = {}
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
        self._campaign_last_terminal_reason: str = ""

        # trade log
        self._trade_log_path = trade_log_path
        self._runtime_evidence_writer = None
        self._runtime_evidence_error: BaseException | None = None
        if trade_log_path:
            self._init_trade_log(trade_log_path)

    def set_runtime_evidence_writer(self, writer) -> None:
        """Route canonical trade rows through the process-wide FIFO writer."""

        if self._runtime_evidence_writer is not None:
            raise RuntimeError("runtime evidence writer is already attached")
        self._runtime_evidence_writer = writer

    def pop_runtime_evidence_error(self) -> BaseException | None:
        """Return a deferred trade-evidence failure after fill safety actions."""

        with self._lock:
            error = self._runtime_evidence_error
            self._runtime_evidence_error = None
            return error

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
        # Marked equity includes the still-open commission pool immediately.
        # Positive commission is a cost and a negative commission is a rebate.
        # Deferring this pool until close would move opening economics across a
        # UTC-day boundary and make the daily risk baseline discontinuous.
        return (
            self._realized_pnl
            + self._unrealized_pnl
            - self._open_commission
        )

    def _update_session_high_water_locked(self) -> None:
        self._peak_pnl = max(self._peak_pnl, self._total_pnl_locked())

    def _reset_daily_locked(self, utc_day: int) -> None:
        # UTC accounting reset only.  Inventory, open campaign, consecutive-loss
        # state, process-session marked-equity high-water and inventory-time
        # state continue across midnight.  Market risk does not reset with the
        # daily reporting denominator.
        self._daily_utc_day = int(utc_day)
        self._day_start_total_pnl = self._total_pnl_locked()
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
        # Opening commission belongs to the active campaign immediately, even
        # though position accounting realizes its proportional share only when
        # inventory is reduced.  Keeping the still-open fee pool here avoids
        # both delaying the fee until close and double-counting it at close.
        self._campaign_realized_pnl = (
            self._realized_pnl
            - self._campaign_start_realized_pnl
            - self._open_commission
        )
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

    def _campaign_finish_locked(self, now: float, terminal_reason: str = "flat") -> None:
        if not self._campaign_active:
            return
        self._campaign_update_pnl_locked()
        logger.info(
            "CAMPAIGN_END id=%d side=%s duration=%.1fs max_abs_qty=%.6f "
            "pnl=%.4f mae=%.4f fills=%d inc=%d red=%d reason=%s",
            self._campaign_id,
            self._campaign_start_side,
            max(0.0, now - self._campaign_start_time),
            self._campaign_max_abs_qty,
            self._campaign_total_pnl,
            min(0.0, self._campaign_min_total_pnl),
            self._campaign_fills,
            self._campaign_exposure_increasing_fills,
            self._campaign_reducing_fills,
            terminal_reason,
        )
        self._campaign_last_terminal_reason = str(terminal_reason)
        self._campaign_active = False
        self._campaign_start_side = "FLAT"

    def _campaign_count_fill_locked(self, side: str, qty: float, *, reducing: bool) -> None:
        if not self._campaign_active:
            return
        self._campaign_fills += 1
        self._campaign_volume += qty
        if side == "BUY":
            self._campaign_buy_fills += 1
        elif side == "SELL":
            self._campaign_sell_fills += 1
        if reducing:
            self._campaign_reducing_fills += 1
        else:
            self._campaign_exposure_increasing_fills += 1

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
        crossed_side = prev_qty * new_qty < -(eps * eps)
        if crossed_side:
            # A physical flip fill is two economic legs at the zero boundary.
            # A sync correction rotates the campaign at the same boundary but
            # deliberately contributes no fill or volume counters.
            if self._campaign_active:
                if count_fill:
                    self._campaign_count_fill_locked(
                        side,
                        abs(prev_qty),
                        reducing=True,
                    )
                final_qty = self._qty
                final_unrealized = self._unrealized_pnl
                final_open_commission = self._open_commission
                self._qty = 0.0
                self._unrealized_pnl = 0.0
                # The fee pool now belongs to the opening leg on the new side.
                # Finish the old campaign at the zero boundary before restoring
                # it, otherwise the old campaign would absorb the new-side fee.
                self._open_commission = 0.0
                self._campaign_finish_locked(now, terminal_reason="flip")
                self._qty = final_qty
                self._unrealized_pnl = final_unrealized
                self._open_commission = final_open_commission
            self._campaign_start_locked(now, new_qty)
            if count_fill:
                self._campaign_count_fill_locked(
                    side,
                    abs(new_qty),
                    reducing=False,
                )
            self._campaign_update_pnl_locked()
            return

        if not self._campaign_active and abs(prev_qty) < eps and abs(new_qty) >= eps:
            self._campaign_start_locked(now, new_qty)

        if self._campaign_active and count_fill:
            if abs(new_qty) > abs(prev_qty) + eps:
                self._campaign_count_fill_locked(side, qty, reducing=False)
            elif abs(new_qty) < abs(prev_qty) - eps:
                self._campaign_count_fill_locked(side, qty, reducing=True)
            else:
                self._campaign_fills += 1
                self._campaign_volume += qty
                if side == "BUY":
                    self._campaign_buy_fills += 1
                elif side == "SELL":
                    self._campaign_sell_fills += 1

        if self._campaign_active:
            self._campaign_update_pnl_locked()
            if abs(new_qty) < eps:
                self._campaign_finish_locked(now, terminal_reason="flat")

    @staticmethod
    def _identity_value(value: object) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        if not normalized or normalized == "0":
            return None
        return normalized

    def _remember_trade_id_locked(
        self,
        trade_id: str,
        identity: _TradeIdentity,
    ) -> None:
        previous = self._trade_identity_by_id.get(trade_id)
        if previous is None:
            self._trade_identity_by_id[trade_id] = identity
            return
        if previous != identity:
            raise RuntimeError(
                "duplicate exchange trade_id changed exact fill identity"
            )

    @classmethod
    def _trade_identity_from_mapping(
        cls,
        raw_trade_id: object,
        payload: Mapping[str, object],
    ) -> tuple[str, _TradeIdentity]:
        trade_id = cls._identity_value(raw_trade_id)
        if trade_id is None or not isinstance(payload, Mapping):
            raise ValueError("included exchange trade identity is invalid")
        side = str(payload.get("side", "")).upper()
        order_id = cls._identity_value(payload.get("order_id"))
        try:
            quantity = float(payload["quantity"])
            price = float(payload["price"])
            commission = float(payload["commission"])
            trade_time_ms = int(payload["trade_time_ms"])
            cumulative = float(payload["cumulative_filled_qty"])
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "included exchange trade identity fields are invalid"
            ) from exc
        if (
            side not in {"BUY", "SELL"}
            or order_id is None
            or not math.isfinite(quantity)
            or not math.isfinite(price)
            or not math.isfinite(commission)
            or not math.isfinite(cumulative)
            or quantity <= 0.0
            or price <= 0.0
            or trade_time_ms < 0
            or cumulative <= 0.0
            or cumulative + 1e-10 < quantity
        ):
            raise ValueError("included exchange trade identity is invalid")
        return trade_id, _TradeIdentity(
            side=side,
            order_id=order_id,
            quantity=quantity,
            price=price,
            commission=commission,
            trade_time_ms=trade_time_ms,
            cumulative_filled_qty=cumulative,
        )

    def _prove_snapshot_cursor_advances_locked(
        self,
        *,
        new_cursors: Mapping[str, float],
        included_identities: Mapping[str, _TradeIdentity],
    ) -> None:
        """Prove every positive REST cursor delta was already applied locally."""

        for trade_id, identity in included_identities.items():
            committed = self._trade_identity_by_id.get(trade_id)
            if committed is None:
                raise RuntimeError(
                    "exchange snapshot included an unknown local trade identity"
                )
            if committed != identity:
                raise RuntimeError(
                    "exchange snapshot trade identity drifted from the local ledger"
                )

        for order_id, new_cursor in new_cursors.items():
            old_cursor = self._snapshot_order_cumulative_filled_qty.get(
                order_id,
                0.0,
            )
            if new_cursor <= old_cursor + 1e-10:
                continue
            candidates = sorted(
                (
                    fill
                    for fill in self._applied_fills_since_barrier
                    if fill.order_id == order_id
                    and fill.cumulative_start is not None
                    and fill.cumulative_end is not None
                    and fill.cumulative_end > old_cursor + 1e-10
                    and fill.cumulative_start < new_cursor - 1e-10
                ),
                key=lambda fill: (
                    float(fill.cumulative_start),
                    float(fill.cumulative_end),
                    fill.sequence,
                ),
            )
            proven_cursor = old_cursor
            for fill in candidates:
                if fill.cumulative_start is None or fill.cumulative_end is None:
                    raise RuntimeError("local fill lost its cumulative interval")
                if abs(fill.cumulative_start - proven_cursor) > 1e-10:
                    raise RuntimeError(
                        "exchange snapshot cursor advance has a local trade gap "
                        "or overlap"
                    )
                if fill.cumulative_end > new_cursor + 1e-10:
                    raise RuntimeError(
                        "exchange snapshot cursor bisects a locally applied trade"
                    )
                if fill.trade_id is None or fill.trade_identity is None:
                    raise RuntimeError(
                        "exchange snapshot cursor advance lacks an exact local "
                        "trade identity"
                    )
                included = included_identities.get(fill.trade_id)
                committed = self._trade_identity_by_id.get(fill.trade_id)
                if included is None or included != fill.trade_identity:
                    raise RuntimeError(
                        "exchange snapshot cursor advance is not covered by its "
                        "exact included trade identity"
                    )
                if committed != fill.trade_identity:
                    raise RuntimeError(
                        "local trade identity drifted before barrier installation"
                    )
                proven_cursor = fill.cumulative_end
                if abs(proven_cursor - new_cursor) <= 1e-10:
                    break
            if abs(proven_cursor - new_cursor) > 1e-10:
                raise RuntimeError(
                    "exchange snapshot cursor advanced without a complete local "
                    "exact-trade covering chain"
                )

    def _prepare_fill_identity_locked(
        self,
        *,
        side: str,
        quantity: float,
        price: float,
        commission: float,
        trade_time_ms: int,
        order_id: object,
        trade_id: object,
        cumulative_filled_qty: float | None,
    ) -> tuple[float, float, _AppliedFill] | None:
        normalized_order_id = self._identity_value(order_id)
        normalized_trade_id = self._identity_value(trade_id)
        has_order_cursor = normalized_order_id is not None or cumulative_filled_qty is not None
        if has_order_cursor and (
            normalized_order_id is None or cumulative_filled_qty is None
        ):
            raise ValueError(
                "fill reconciliation identity requires both order_id and "
                "cumulative_filled_qty"
            )
        if normalized_trade_id is not None and normalized_order_id is None:
            raise ValueError(
                "trade_id reconciliation requires order_id and "
                "cumulative_filled_qty"
            )
        if (
            self._last_reconciliation_snapshot_update_time_ms > 0
            and normalized_order_id is not None
            and normalized_trade_id is None
        ):
            raise RuntimeError(
                "fill lacks exact trade_id after an exchange reconciliation "
                "barrier was installed"
            )

        cumulative_start: float | None = None
        cumulative_end: float | None = None
        trade_identity: _TradeIdentity | None = None
        effective_qty = quantity
        effective_commission = commission
        if normalized_order_id is not None:
            cumulative_end = float(cumulative_filled_qty)
            if not math.isfinite(cumulative_end) or cumulative_end <= 0.0:
                raise ValueError("cumulative_filled_qty must be positive and finite")
            if cumulative_end + 1e-10 < quantity:
                raise ValueError("fill quantity exceeds its cumulative_filled_qty")
            if (
                self._last_reconciliation_snapshot_update_time_ms > 0
                and normalized_order_id
                not in self._snapshot_order_cumulative_filled_qty
                and trade_time_ms
                <= self._last_reconciliation_snapshot_update_time_ms
            ):
                raise RuntimeError(
                    "fill at or before the reconciliation snapshot lacks its "
                    "snapshot order cursor"
                )
            if normalized_trade_id is not None:
                trade_identity = _TradeIdentity(
                    side=side,
                    order_id=normalized_order_id,
                    quantity=quantity,
                    price=price,
                    commission=commission,
                    trade_time_ms=trade_time_ms,
                    cumulative_filled_qty=cumulative_end,
                )
                previous_identity = self._trade_identity_by_id.get(
                    normalized_trade_id
                )
                if previous_identity is not None:
                    if previous_identity != trade_identity:
                        raise RuntimeError(
                            "duplicate exchange trade_id changed exact fill identity"
                        )
                    logger.info(
                        "FILL_SKIP_DUPLICATE_TRADE trade_id=%s order_id=%s",
                        normalized_trade_id,
                        normalized_order_id,
                    )
                    return None
            event_start = max(0.0, cumulative_end - quantity)
            applied_cursor = self._order_cumulative_filled_qty.get(
                normalized_order_id,
                0.0,
            )
            if cumulative_end <= applied_cursor + 1e-10:
                self._order_cumulative_filled_qty[normalized_order_id] = max(
                    applied_cursor,
                    cumulative_end,
                )
                if normalized_trade_id is not None and trade_identity is not None:
                    self._remember_trade_id_locked(
                        normalized_trade_id,
                        trade_identity,
                    )
                logger.info(
                    "FILL_SKIP_RECONCILED order_id=%s trade_id=%s cumulative=%.8f cursor=%.8f",
                    normalized_order_id,
                    normalized_trade_id or "<missing>",
                    cumulative_end,
                    applied_cursor,
                )
                return None
            if abs(event_start - applied_cursor) > 1e-10:
                raise RuntimeError(
                    "exchange fill cumulative interval is not contiguous with "
                    "the locally proven order cursor"
                )
            cumulative_start = event_start
            effective_qty = quantity
            effective_commission = commission
        elif self._last_reconciliation_snapshot_update_time_ms > 0:
            raise RuntimeError(
                "fill lacks order cumulative identity after an exchange "
                "reconciliation barrier was installed"
            )

        record = _AppliedFill(
            sequence=self._fill_identity_sequence + 1,
            side=side,
            quantity=effective_qty,
            price=price,
            commission=effective_commission,
            trade_time_ms=trade_time_ms,
            order_id=normalized_order_id,
            trade_id=normalized_trade_id,
            cumulative_start=cumulative_start,
            cumulative_end=cumulative_end,
            trade_identity=trade_identity,
        )
        return effective_qty, effective_commission, record

    def _commit_fill_identity_locked(self, record: _AppliedFill) -> None:
        self._fill_identity_sequence = record.sequence
        self._applied_fills_since_barrier.append(record)
        if record.order_id is not None and record.cumulative_end is not None:
            self._order_cumulative_filled_qty[record.order_id] = max(
                self._order_cumulative_filled_qty.get(record.order_id, 0.0),
                record.cumulative_end,
            )
        if record.trade_id is not None and record.trade_identity is not None:
            self._remember_trade_id_locked(
                record.trade_id,
                record.trade_identity,
            )

    def on_fill(self, side: str, qty: float, price: float,
                commission: float = 0.0, trade_time_ms: int = 0, *,
                order_id: int | str | None = None,
                trade_id: int | str | None = None,
                cumulative_filled_qty: float | None = None) -> float:
        """
        Process a fill event. Updates position cache and state machine.

        side: "BUY" or "SELL"
        qty: filled quantity (always positive)
        price: fill price
        commission: commission converted to the contract quote/settlement asset
        trade_time_ms: exchange trade time in epoch milliseconds (from WS event)

        Returns the quantity actually applied after cumulative-cursor
        reconciliation.  A duplicate/already-snapshotted event returns zero so
        downstream fill-driven controls can remain idempotent too.
        """
        side = str(side).upper()
        qty = float(qty)
        price = float(price)
        commission = float(commission)
        trade_time_ms = int(trade_time_ms)
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported fill side {side!r}")
        if (
            not math.isfinite(qty)
            or not math.isfinite(price)
            or not math.isfinite(commission)
            or qty <= 0.0
            or price <= 0.0
            or trade_time_ms < 0
        ):
            raise ValueError("fill values are invalid")
        now = time.time()

        with self._lock:
            event_time_s = trade_time_ms / 1000.0 if trade_time_ms > 0 else now
            self._roll_daily_if_needed_locked(event_time_s)
            prepared = self._prepare_fill_identity_locked(
                side=side,
                quantity=qty,
                price=price,
                commission=commission,
                trade_time_ms=trade_time_ms,
                order_id=order_id,
                trade_id=trade_id,
                cumulative_filled_qty=cumulative_filled_qty,
            )
            if prepared is None:
                return 0.0
            qty, commission, fill_identity = prepared
            signed_qty = qty if side == "BUY" else -qty

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
                closing_commission = commission * close_qty / qty
                opening_commission = commission - closing_commission
                # Proportional share of accumulated open-side commission
                if abs(self._qty) > 1e-10:
                    open_comm_share = self._open_commission * (close_qty / abs(self._qty))
                else:
                    open_comm_share = self._open_commission
                # realized PnL for closed portion (includes both open + close commission)
                if self._qty > 0:
                    rpnl = (
                        (price - self._avg_entry) * close_qty
                        - closing_commission
                        - open_comm_share
                    )
                else:
                    rpnl = (
                        (self._avg_entry - price) * close_qty
                        - closing_commission
                        - open_comm_share
                    )

                self._open_commission -= open_comm_share
                self._realized_pnl += rpnl
                self._last_trade_pnl = rpnl
                self._round_trip_rpnl += rpnl

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
                        self._open_commission = opening_commission
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
            self._recalc_unrealized(update_campaign=False)
            self._update_session_high_water_locked()
            self._campaign_on_position_change_locked(
                prev_qty,
                self._qty,
                side,
                qty,
                now,
                count_fill=True,
            )
            self._commit_fill_identity_locked(fill_identity)

            logger.info(
                f"FILL {side} {qty:.4f}@{price:.1f} [{trade_type}] "
                f"pos={self._qty:+.4f}@{self._avg_entry:.1f} "
                f"rpnl={self._realized_pnl:.2f} "
                f"{prev_state.name}→{self._state.name}"
            )

            # --- Log trade ---
            if self._trade_log_path:
                self._log_trade(now, side, trade_type, qty, price, commission)
            return qty

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
            self._update_session_high_water_locked()

    def _recalc_unrealized(self, *, update_campaign: bool = True):
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
        if update_campaign:
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
            session_marked_pnl = self._total_pnl_locked()
            session_marked_drawdown = max(
                0.0,
                self._peak_pnl - session_marked_pnl,
            )
            daily_pnl = session_marked_pnl - self._day_start_total_pnl
            return {
                "elapsed_s": elapsed_s,
                "session_marked_pnl": session_marked_pnl,
                "session_marked_high_water": self._peak_pnl,
                "session_marked_drawdown": session_marked_drawdown,
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
            return max(0.0, self._peak_pnl - self._total_pnl_locked())

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

    @staticmethod
    def _position_after_fill_fragment(
        position: float,
        entry_price: float,
        fill: _AppliedFill,
    ) -> tuple[float, float]:
        signed_qty = fill.quantity if fill.side == "BUY" else -fill.quantity
        if abs(position) <= 1e-10:
            return signed_qty, fill.price
        if position * signed_qty > 0.0:
            new_position = position + signed_qty
            new_entry = (
                abs(position) * entry_price + fill.quantity * fill.price
            ) / abs(new_position)
            return new_position, new_entry
        if fill.quantity < abs(position) - 1e-10:
            return position + signed_qty, entry_price
        if abs(fill.quantity - abs(position)) <= 1e-10:
            return 0.0, 0.0
        return position + signed_qty, fill.price

    @staticmethod
    def _fragment_after_order_cursor(
        fill: _AppliedFill,
        cursor: float,
    ) -> _AppliedFill | None:
        if fill.cumulative_start is None or fill.cumulative_end is None:
            raise RuntimeError("identified fill lost its cumulative interval")
        if cursor >= fill.cumulative_end - 1e-10:
            return None
        if cursor <= fill.cumulative_start + 1e-10:
            return fill
        raise RuntimeError(
            "exchange snapshot order cursor bisects an atomic trade interval"
        )

    def sync_from_exchange(
        self,
        exchange_qty: float,
        exchange_entry: float,
        *,
        snapshot_update_time_ms: int,
        order_cumulative_filled_qty: Mapping[int | str, float],
        included_trade_ids: Iterable[int | str] = (),
        included_trade_identities: Mapping[
            int | str,
            Mapping[str, object],
        ] | None = None,
    ) -> dict[str, object]:
        """Reconcile local state with exchange position.

        ``snapshot_update_time_ms`` is the exchange position snapshot clock.
        ``order_cumulative_filled_qty`` and ``included_trade_identities`` bind
        the exact fills already represented by that snapshot.  IDs alone are
        never cursor coverage.  Before a non-initial call, every missing REST
        account trade must already have traversed the normal OrderManager ->
        fill callback pipeline.  This method proves each positive cursor delta
        is tiled by locally committed exact trade intervals, then verifies that
        the local ledger equals the stable exchange snapshot plus any locally
        applied post-snapshot fills before installing the barrier.

        The first barrier is the only bootstrap path allowed to seed position
        quantity/entry from REST.  Later mismatches fail closed; they are never
        converted into a synthetic ``SYNC_ADJUST`` because that would lose the
        missing trade's fee, realized PnL, campaign, and cooldown side effects.

        中文说明：这里不是常规成交路径。首个 barrier 只负责 bootstrap；
        之后用户流漏单必须先按 accountTrade 身份走真实 fill 管线，本方法
        只做强校验，绝不以无手续费/无成交身份的 SYNC_ADJUST 代替真实成交。
        """
        exchange_qty = float(exchange_qty)
        exchange_entry = float(exchange_entry)
        snapshot_update_time_ms = int(snapshot_update_time_ms)
        if (
            not math.isfinite(exchange_qty)
            or not math.isfinite(exchange_entry)
            or snapshot_update_time_ms <= 0
            or (abs(exchange_qty) > 1e-10 and exchange_entry <= 0.0)
            or exchange_entry < 0.0
        ):
            raise ValueError("exchange reconciliation snapshot is invalid")
        normalized_cursors: dict[str, float] = {}
        for raw_order_id, raw_cumulative in order_cumulative_filled_qty.items():
            order_key = self._identity_value(raw_order_id)
            cumulative = float(raw_cumulative)
            if order_key is None or not math.isfinite(cumulative) or cumulative < 0.0:
                raise ValueError("exchange order cumulative cursor is invalid")
            normalized_cursors[order_key] = max(
                normalized_cursors.get(order_key, 0.0),
                cumulative,
            )
        normalized_trade_ids = {
            trade_key
            for raw_trade_id in included_trade_ids
            if (trade_key := self._identity_value(raw_trade_id)) is not None
        }
        raw_identities = included_trade_identities or {}
        if not isinstance(raw_identities, Mapping):
            raise ValueError("included_trade_identities must be a mapping")
        normalized_trade_identities: dict[str, _TradeIdentity] = {}
        for raw_trade_id, raw_identity in raw_identities.items():
            trade_key, identity = self._trade_identity_from_mapping(
                raw_trade_id,
                raw_identity,
            )
            previous = normalized_trade_identities.get(trade_key)
            if previous is not None and previous != identity:
                raise RuntimeError(
                    "exchange snapshot repeated a trade ID with drifted identity"
                )
            normalized_trade_identities[trade_key] = identity
        if normalized_trade_ids != set(normalized_trade_identities):
            raise RuntimeError(
                "exchange snapshot trade IDs and exact identities disagree"
            )

        with self._lock:
            is_initial_seed = (
                self._last_reconciliation_snapshot_update_time_ms <= 0
            )
            if snapshot_update_time_ms < self._last_reconciliation_snapshot_update_time_ms:
                raise RuntimeError("exchange reconciliation snapshot clock regressed")
            for order_key, old_cursor in self._snapshot_order_cumulative_filled_qty.items():
                new_cursor = normalized_cursors.get(order_key)
                if new_cursor is None:
                    raise RuntimeError(
                        "exchange reconciliation omitted a previously bound "
                        "order cumulative cursor"
                    )
                if new_cursor + 1e-10 < old_cursor:
                    raise RuntimeError("exchange order cumulative cursor regressed")

            if is_initial_seed:
                for identity in normalized_trade_identities.values():
                    snapshot_cursor = normalized_cursors.get(identity.order_id)
                    if (
                        snapshot_cursor is None
                        or snapshot_cursor
                        < identity.cumulative_filled_qty - 1e-10
                    ):
                        raise RuntimeError(
                            "initial exchange trade identity is not covered by "
                            "its snapshot order cursor"
                        )
            else:
                self._prove_snapshot_cursor_advances_locked(
                    new_cursors=normalized_cursors,
                    included_identities=normalized_trade_identities,
                )

            retained_fills: list[_AppliedFill] = []
            for fill in self._applied_fills_since_barrier:
                if fill.order_id is not None and fill.order_id in normalized_cursors:
                    snapshot_cursor = normalized_cursors[fill.order_id]
                    if (
                        fill.trade_id is not None
                        and fill.trade_id in normalized_trade_ids
                        and fill.cumulative_end is not None
                        and snapshot_cursor < fill.cumulative_end - 1e-10
                    ):
                        raise RuntimeError(
                            "exchange snapshot included trade_id without an "
                            "order cumulative cursor covering that trade"
                        )
                    retained = self._fragment_after_order_cursor(
                        fill,
                        snapshot_cursor,
                    )
                    if retained is not None:
                        retained_fills.append(retained)
                    continue
                if fill.trade_id is not None and fill.trade_id in normalized_trade_ids:
                    raise RuntimeError(
                        "exchange snapshot included trade_id without its order "
                        "cumulative cursor"
                    )
                if fill.trade_time_ms > snapshot_update_time_ms:
                    retained_fills.append(fill)
                    continue
                raise RuntimeError(
                    "exchange snapshot omitted the identity cursor for a locally "
                    "applied fill at or before its update time"
                )

            target_qty = exchange_qty
            target_entry = exchange_entry if abs(exchange_qty) > 1e-10 else 0.0
            for fill in sorted(retained_fills, key=lambda item: item.sequence):
                target_qty, target_entry = self._position_after_fill_fragment(
                    target_qty,
                    target_entry,
                    fill,
                )
            if abs(target_qty) <= 1e-10:
                target_qty = 0.0
                target_entry = 0.0

            now = time.time()
            old_qty = self._qty
            old_entry = self._avg_entry
            quantity_changed = abs(target_qty - old_qty) > 1e-6
            entry_changed = abs(target_entry - old_entry) > 1e-8
            if (quantity_changed or entry_changed) and not is_initial_seed:
                raise RuntimeError(
                    "exchange reconciliation ledger mismatch after authoritative "
                    "account trades: "
                    f"local={old_qty:+.8f}@{old_entry:.8f} "
                    f"expected={target_qty:+.8f}@{target_entry:.8f} "
                    f"snapshot={exchange_qty:+.8f}@{exchange_entry:.8f} "
                    f"retained_post_snapshot={len(retained_fills)}"
                )

            if (quantity_changed or entry_changed) and is_initial_seed:
                self._accrue_inventory_time_locked(now)
                logger.warning(
                    "POSITION_RECONCILIATION_SEED local=%+.8f@%.8f "
                    "exchange=%+.8f@%.8f "
                    "target=%+.8f@%.8f retained_post_snapshot=%d",
                    old_qty,
                    old_entry,
                    exchange_qty,
                    exchange_entry,
                    target_qty,
                    target_entry,
                    len(retained_fills),
                )
                crossed_side = old_qty * target_qty < -1e-20
                was_flat = abs(old_qty) < 1e-10
                self._qty = target_qty
                self._avg_entry = target_entry
                self._cost_basis = target_entry * abs(target_qty)
                # A position snapshot contains no cost/fee history.  Bootstrap
                # therefore starts a new locally auditable economic boundary.
                self._open_commission = 0.0
                self._round_trip_rpnl = 0.0
                if (was_flat or crossed_side) and abs(target_qty) > 1e-10:
                    self._open_time = now
                self._state = (
                    PositionState.FLAT
                    if abs(target_qty) < 1e-10
                    else PositionState.OPEN
                )
                self._recalc_unrealized()
                self._update_session_high_water_locked()

                delta = target_qty - old_qty
                if abs(delta) > 1e-10:
                    side = "BUY" if delta > 0 else "SELL"
                    self._campaign_on_position_change_locked(
                        old_qty,
                        self._qty,
                        side,
                        abs(delta),
                        now,
                        count_fill=False,
                    )
                    if self._trade_log_path:
                        px = target_entry if target_entry > 0 else self._mark_price
                        self._log_trade(
                            now,
                            side,
                            "RECONCILIATION_SEED",
                            abs(delta),
                            px,
                            0.0,
                        )

            # Install the barrier even when position quantity was already equal.
            self._last_reconciliation_snapshot_update_time_ms = snapshot_update_time_ms
            self._snapshot_order_cumulative_filled_qty = dict(normalized_cursors)
            self._applied_fills_since_barrier = retained_fills
            if is_initial_seed:
                for trade_id, identity in normalized_trade_identities.items():
                    self._remember_trade_id_locked(trade_id, identity)
            for order_key, cumulative in normalized_cursors.items():
                self._order_cumulative_filled_qty[order_key] = max(
                    self._order_cumulative_filled_qty.get(order_key, 0.0),
                    cumulative,
                )
            logger.info(
                "RECONCILIATION_BARRIER update_time_ms=%d seed=%d orders=%d "
                "trades=%d retained_post_snapshot=%d",
                snapshot_update_time_ms,
                int(is_initial_seed),
                len(normalized_cursors),
                len(normalized_trade_ids),
                len(retained_fills),
            )
            return {
                "snapshot_update_time_ms": snapshot_update_time_ms,
                "seeded": is_initial_seed,
                "position_changed": bool(quantity_changed or entry_changed),
                "retained_post_snapshot_fill_count": len(retained_fills),
            }

    def reconciliation_snapshot(self) -> dict:
        """Return the exchange-identity barrier for checkpoint/health binding."""
        with self._lock:
            return {
                "snapshot_update_time_ms": (
                    self._last_reconciliation_snapshot_update_time_ms
                ),
                "order_cumulative_filled_qty": dict(
                    self._snapshot_order_cumulative_filled_qty
                ),
                "local_order_cumulative_filled_qty": dict(
                    self._order_cumulative_filled_qty
                ),
                "retained_post_snapshot_fill_count": len(
                    self._applied_fills_since_barrier
                ),
                "tracked_trade_identity_count": len(
                    self._trade_identity_by_id
                ),
            }

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
        row = {
            "timestamp": f"{ts:.3f}",
            "side": side,
            "trade_type": trade_type,
            "qty": f"{qty:.4f}",
            "price": f"{price:.1f}",
            "commission": f"{commission:.4f}",
            "position": f"{self._qty:+.4f}",
            "avg_entry": f"{self._avg_entry:.1f}",
            "realized_pnl": f"{self._realized_pnl:.2f}",
            "unrealized_pnl": f"{self._unrealized_pnl:.2f}",
            "state": self._state.name,
        }
        runtime = getattr(self, "_runtime_evidence_writer", None)
        if runtime is not None:
            try:
                runtime.enqueue_csv(self._trade_log_path, row)
            except Exception as exc:
                if self._runtime_evidence_error is None:
                    self._runtime_evidence_error = exc
                logger.critical(
                    "Trade evidence admission failed; deferring fatal until "
                    "post-fill risk actions complete: %s",
                    exc,
                    exc_info=True,
                )
            return
        try:
            with open(self._trade_log_path, "a", newline="") as f:
                w = csv.writer(f)
                w.writerow(list(row.values()))
        except Exception as e:
            logger.error(f"Trade log write failed: {e}")
