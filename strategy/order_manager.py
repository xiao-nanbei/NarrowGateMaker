"""
Order State Machine — 管理每个订单的完整生命周期。

状态转换:
  PENDING_NEW → OPEN → (PARTIALLY_FILLED →) FILLED
              → CANCELED
              → EXPIRED
              → REJECTED
  PENDING_CANCEL → CANCELED / OPEN (cancel rejected)

每个订单通过 ORDER_TRADE_UPDATE 事件驱动状态变迁。
"""

import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum, auto
from threading import Lock
from typing import Callable, Dict, List, Optional

from execution.order_lifecycle import (
    OrderLifecyclePhase,
    QuantityWeightedOrderLifecycle,
)

logger = logging.getLogger("order_manager")


class OrderState(Enum):
    PENDING_NEW = auto()       # 已发出REST请求，未收到确认
    OPEN = auto()              # NEW / PARTIALLY_FILLED 挂单中
    PARTIALLY_FILLED = auto()  # 部分成交
    FILLED = auto()            # 全部成交 (终态)
    CANCELED = auto()          # 已撤单 (终态)
    EXPIRED = auto()           # 过期 (终态)
    REJECTED = auto()          # 被拒 (终态)
    PENDING_CANCEL = auto()    # 撤单请求已发出


class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"


TERMINAL_STATES = {OrderState.FILLED, OrderState.CANCELED,
                   OrderState.EXPIRED, OrderState.REJECTED}


@dataclass
class Order:
    client_order_id: str
    symbol: str
    side: Side
    price: float
    quantity: float
    state: OrderState = OrderState.PENDING_NEW

    # filled tracking
    filled_qty: float = 0.0
    avg_fill_price: float = 0.0

    # server info
    order_id: int = 0              # exchange order id
    create_time: float = 0.0      # local timestamp
    update_time: float = 0.0
    lifecycle: Optional[QuantityWeightedOrderLifecycle] = field(
        default=None,
        repr=False,
    )
    orphan_adoption: bool = False
    left_truncation_reason: str = ""

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def is_active(self) -> bool:
        return self.state in (OrderState.OPEN, OrderState.PARTIALLY_FILLED,
                              OrderState.PENDING_NEW)

    @property
    def is_fill_risk_active(self) -> bool:
        return bool(self.lifecycle and self.lifecycle.fill_risk_active)

    @property
    def remaining_qty(self) -> float:
        return max(0.0, float(self.quantity) - float(self.filled_qty))

    @property
    def lifecycle_phase(self) -> str:
        if self.lifecycle is None:
            return "UNKNOWN"
        return self.lifecycle.phase.value


class OrderManager:
    """
    Thread-safe order state machine with event-driven updates.

    Usage:
        om = OrderManager(on_fill=my_callback)
        cid = om.create_order("BTCUSDT", Side.BUY, 85000.0, 0.005)
        om.confirm_new(cid, exchange_order_id)
        om.on_order_update(event_dict)   # from user_data stream
    """

    def __init__(self, on_fill: Optional[Callable] = None,
                 on_cancel: Optional[Callable] = None,
                 on_terminal: Optional[Callable] = None,
                 on_lifecycle_event: Optional[Callable] = None,
                 max_history: int = 500):
        self._lock = Lock()
        self._orders: Dict[str, Order] = {}          # cid → Order (active)
        self._history: OrderedDict = OrderedDict()    # cid → Order (terminal)
        self._oid_map: Dict[int, str] = {}            # exchange_oid → cid
        self._max_history = max_history
        self._generation = 0

        # callbacks
        self._on_fill = on_fill
        self._on_cancel = on_cancel
        self._on_terminal = on_terminal
        self._on_lifecycle_event = on_lifecycle_event

    # ── order creation ──

    def create_order(self, symbol: str, side: Side,
                     price: float, quantity: float) -> str:
        """Register a new order (before sending REST request)."""
        self._generation += 1
        now_ns = time.time_ns()
        cid = f"mm_{side.value[0]}_{now_ns // 1_000_000}_{self._generation}"

        order = Order(
            client_order_id=cid, symbol=symbol, side=side,
            price=price, quantity=quantity,
            state=OrderState.PENDING_NEW,
            create_time=now_ns / 1_000_000_000.0,
            lifecycle=QuantityWeightedOrderLifecycle(
                initial_quantity=float(quantity),
                submitted_ts_ns=now_ns,
            ),
        )
        with self._lock:
            self._orders[cid] = order
        logger.debug(f"ORDER_CREATE {cid} {side.value} {quantity}@{price}")
        return cid

    def confirm_new(
        self,
        cid: str,
        exchange_oid: int,
        *,
        exchange_ts_ns: int = 0,
    ):
        """Confirm order accepted by exchange (REST response)."""
        now_ns = time.time_ns()
        accepted = None
        with self._lock:
            o = self._orders.get(cid)
            if o and o.state == OrderState.PENDING_NEW:
                o.state = OrderState.OPEN
                o.order_id = exchange_oid
                o.update_time = now_ns / 1_000_000_000.0
                if o.lifecycle is not None:
                    o.lifecycle.activate(
                        now_ns,
                        exchange_ts_ns=int(exchange_ts_ns),
                    )
                self._oid_map[exchange_oid] = cid
                logger.debug(f"ORDER_CONFIRMED {cid} oid={exchange_oid}")
                accepted = o
        if accepted is not None and self._on_lifecycle_event:
            self._on_lifecycle_event(
                accepted,
                "rest_ack",
                {
                    "_local_receive_ts_ns": now_ns,
                    "_exchange_ts_ns": int(exchange_ts_ns),
                },
            )

    def confirm_rejected(self, cid: str, reason: str = ""):
        """Order was rejected by exchange."""
        now_ns = time.time_ns()
        rejected = None
        with self._lock:
            o = self._orders.get(cid)
            if o:
                o.state = OrderState.REJECTED
                o.update_time = now_ns / 1_000_000_000.0
                if o.lifecycle is not None:
                    o.lifecycle.exchange_terminal(
                        now_ns,
                        reason="rejected",
                    )
                self._move_to_history(cid)
                logger.warning(f"ORDER_REJECTED {cid}: {reason}")
                rejected = o
        if rejected is not None and self._on_lifecycle_event:
            self._on_lifecycle_event(
                rejected,
                "rejected",
                {
                    "_local_receive_ts_ns": now_ns,
                    "_reason": str(reason),
                },
            )
        if rejected is not None and self._on_terminal:
            self._on_terminal(rejected, "rejected")

    def mark_submit_ack_unknown(self, cid: str, reason: str) -> bool:
        """Keep a submit in-flight until exchange/user-stream reconciliation."""

        now_ns = time.time_ns()
        pending = None
        with self._lock:
            order = self._orders.get(cid)
            if order is None or order.state != OrderState.PENDING_NEW:
                return False
            if order.lifecycle is not None:
                order.lifecycle.mark_submit_ack_unknown(
                    now_ns,
                    reason=str(reason),
                )
            order.update_time = now_ns / 1_000_000_000.0
            pending = order
            logger.error("ORDER_SUBMIT_ACK_UNKNOWN %s: %s", cid, reason)
        if pending is not None and self._on_lifecycle_event:
            self._on_lifecycle_event(
                pending,
                "submit_ack_unknown",
                {
                    "_local_receive_ts_ns": now_ns,
                    "_reason": str(reason),
                    "_point_identified": False,
                },
            )
        return pending is not None

    def censor_submit_ack_unknown(self, cid: str, reason: str) -> bool:
        """Censor local observation during shutdown without releasing live ownership."""

        now_ns = time.time_ns()
        censored = None
        with self._lock:
            order = self._orders.get(cid)
            if order is None or order.state != OrderState.PENDING_NEW:
                return False
            if order.lifecycle is not None:
                order.lifecycle.censor_submit_ack_unknown(
                    now_ns,
                    reason="local_shutdown_unknown_ack",
                )
            order.update_time = now_ns / 1_000_000_000.0
            censored = order
            logger.error("ORDER_SUBMIT_ACK_CENSORED %s: %s", cid, reason)
        if censored is not None and self._on_lifecycle_event:
            self._on_lifecycle_event(
                censored,
                "submit_ack_unknown_censored",
                {
                    "_local_receive_ts_ns": now_ns,
                    "_reason": str(reason),
                    "_point_identified": False,
                },
            )
        return censored is not None

    def mark_pending_cancel(self, cid: str):
        """Mark order as awaiting cancel confirmation."""
        now_ns = time.time_ns()
        with self._lock:
            o = self._orders.get(cid)
            if o and o.state in (OrderState.OPEN, OrderState.PARTIALLY_FILLED):
                o.state = OrderState.PENDING_CANCEL
                o.update_time = now_ns / 1_000_000_000.0
                if o.lifecycle is not None:
                    o.lifecycle.request_cancel(now_ns)

    def cancel_rejected(
        self,
        cid: str,
        reason: str = "",
        *,
        exchange_ts_ns: int = 0,
    ) -> bool:
        """Restore the active risk set after an explicit cancel rejection."""

        now_ns = time.time_ns()
        restored = None
        with self._lock:
            o = self._orders.get(cid)
            if o is None or o.state != OrderState.PENDING_CANCEL:
                return False
            if o.lifecycle is not None:
                o.lifecycle.cancel_rejected(
                    now_ns,
                    exchange_ts_ns=int(exchange_ts_ns),
                )
                o.state = (
                    OrderState.PARTIALLY_FILLED
                    if o.lifecycle.phase == OrderLifecyclePhase.PARTIALLY_FILLED
                    else OrderState.OPEN
                )
            else:
                o.state = (
                    OrderState.PARTIALLY_FILLED
                    if o.filled_qty > 0.0
                    else OrderState.OPEN
                )
            o.update_time = now_ns / 1_000_000_000.0
            restored = o
            logger.warning("ORDER_CANCEL_REJECTED %s: %s", cid, reason)
        if restored is not None and self._on_lifecycle_event:
            self._on_lifecycle_event(
                restored,
                "cancel_rejected",
                {
                    "_local_receive_ts_ns": now_ns,
                    "_exchange_ts_ns": int(exchange_ts_ns),
                    "_reason": str(reason),
                },
            )
        return restored is not None

    # ── event-driven updates (from user_data stream) ──

    @staticmethod
    def _visibility_ts_ns(event: dict) -> int:
        value = int(event.get("_local_receive_ts_ns", 0) or 0)
        return value if value > 0 else time.time_ns()

    @staticmethod
    def _exchange_ts_ns(event: dict) -> int:
        value_ms = int(event.get("T", 0) or 0)
        return max(0, value_ms) * 1_000_000

    def _apply_terminal_status_fill(
        self,
        order: Order,
        event: dict,
        *,
        visibility_ts_ns: int,
        exchange_ts_ns: int,
        reconciled_unknown_prefix: bool,
    ) -> tuple[bool, bool]:
        """Apply quantity reported by a terminal ACK before ending its lifecycle."""

        is_new_fill = self._apply_fill(order, event)
        if not is_new_fill:
            return False, False
        full_fill = order.remaining_qty <= 1e-10
        lifecycle = order.lifecycle
        if lifecycle is None:
            return True, full_fill
        if lifecycle.phase == OrderLifecyclePhase.SUBMITTED:
            if reconciled_unknown_prefix:
                lifecycle.observe_fill_with_unknown_activation(
                    remaining_after=order.remaining_qty,
                    visibility_ts_ns=visibility_ts_ns,
                    full_fill=full_fill,
                )
            else:
                lifecycle.activate(
                    visibility_ts_ns,
                    exchange_ts_ns=exchange_ts_ns,
                )
                lifecycle.observe_fill(
                    remaining_after=order.remaining_qty,
                    visibility_ts_ns=visibility_ts_ns,
                    exchange_ts_ns=exchange_ts_ns,
                    full_fill=full_fill,
                )
        else:
            lifecycle.observe_fill(
                remaining_after=order.remaining_qty,
                visibility_ts_ns=visibility_ts_ns,
                exchange_ts_ns=exchange_ts_ns,
                full_fill=full_fill,
            )
        return True, full_fill

    def on_order_update(self, event: dict):
        """
        Process ORDER_TRADE_UPDATE from WebSocket user_data stream.

        event = {
            "s": symbol,
            "c": clientOrderId,
            "S": side,
            "o": orderType,
            "X": orderStatus,   # NEW, PARTIALLY_FILLED, FILLED, CANCELED, EXPIRED
            "i": orderId,
            "l": lastFilledQty,
            "L": lastFilledPrice,
            "z": cumFilledQty,
            "ap": avgPrice,
            "n": commission,
            "N": commissionAsset,
            "p": originalPrice,
            "q": originalQty,
            "T": tradeTime,
        }
        """
        cid = event.get("c", "")
        status = event.get("X", "")
        oid = int(event.get("i", 0))
        visibility_ts_ns = self._visibility_ts_ns(event)
        exchange_ts_ns = self._exchange_ts_ns(event)
        reconciled_unknown_prefix = bool(event.get("_submit_ack_reconciled", False))
        terminal_reason = ""
        lifecycle_event_type = ""

        with self._lock:
            # lookup by cid first, then by exchange oid
            order = self._orders.get(cid)
            if not order and oid in self._oid_map:
                order = self._orders.get(self._oid_map[oid])
            if not order:
                # Check history — duplicate WS event for already-terminal order
                if cid in self._history:
                    logger.debug(f"ORDER_UPDATE duplicate for terminal order: cid={cid}")
                    return
                logger.warning(f"ORDER_UPDATE for unknown order: cid={cid} oid={oid}")
                # Auto-adopt orphan orders from previous session
                # 只收养 mm_ 前缀订单，避免把手工/其他策略订单并进本地状态机。
                if cid.startswith("mm_"):
                    self._adopted_fill = None
                    self._adopted_cancel = None
                    self._adopted_terminal = None
                    self._adopted_lifecycle_event = None
                    self._adopt_orphan(cid, oid, event)
                    adopted_fill = self._adopted_fill
                    adopted_cancel = self._adopted_cancel
                    adopted_terminal = self._adopted_terminal
                    adopted_lifecycle_event = self._adopted_lifecycle_event
                # The lifecycle callback only snapshots into a bounded queue;
                # fill/inventory callbacks retain their historical behavior.
                if cid.startswith("mm_"):
                    if adopted_lifecycle_event and self._on_lifecycle_event:
                        self._on_lifecycle_event(*adopted_lifecycle_event)
                    if adopted_fill and self._on_fill:
                        self._on_fill(adopted_fill[0], adopted_fill[1])
                    if adopted_cancel and self._on_cancel:
                        self._on_cancel(adopted_cancel)
                    if adopted_terminal and self._on_terminal:
                        self._on_terminal(
                            adopted_terminal[0],
                            adopted_terminal[1],
                        )
                return

            # A private user-stream callback does not carry the internal REST
            # reconciliation marker.  Preserve the unknown activation prefix
            # whenever the original submit response was indeterminate.
            reconciled_unknown_prefix = bool(
                reconciled_unknown_prefix
                or (
                    order.lifecycle is not None
                    and order.lifecycle.phase == OrderLifecyclePhase.SUBMITTED
                    and order.lifecycle.submit_ack_unknown_observed
                )
            )

            prev_state = order.state
            is_new_fill = False

            # update oid mapping if not yet set
            if oid and not order.order_id:
                order.order_id = oid
                self._oid_map[oid] = order.client_order_id

            if status == "NEW":
                order.state = OrderState.OPEN
                if order.lifecycle is not None:
                    if (
                        order.lifecycle.phase
                        == OrderLifecyclePhase.CANCEL_PENDING
                    ):
                        order.lifecycle.cancel_rejected(
                            visibility_ts_ns,
                            exchange_ts_ns=exchange_ts_ns,
                        )
                        if (
                            order.lifecycle.phase
                            == OrderLifecyclePhase.PARTIALLY_FILLED
                        ):
                            order.state = OrderState.PARTIALLY_FILLED
                        lifecycle_event_type = "cancel_rejected"
                    elif order.lifecycle.phase == OrderLifecyclePhase.SUBMITTED:
                        if reconciled_unknown_prefix:
                            order.lifecycle.activate_with_unknown_prefix(
                                visibility_ts_ns
                            )
                            lifecycle_event_type = "activate_unknown_prefix"
                        else:
                            order.lifecycle.activate(
                                visibility_ts_ns,
                                exchange_ts_ns=exchange_ts_ns,
                            )
                            lifecycle_event_type = "activate"

            elif status == "PARTIALLY_FILLED":
                order.state = (
                    OrderState.PENDING_CANCEL
                    if prev_state == OrderState.PENDING_CANCEL
                    else OrderState.PARTIALLY_FILLED
                )
                is_new_fill = self._apply_fill(order, event)
                if is_new_fill and order.lifecycle is not None:
                    if order.lifecycle.phase == OrderLifecyclePhase.SUBMITTED:
                        if reconciled_unknown_prefix:
                            order.lifecycle.observe_fill_with_unknown_activation(
                                remaining_after=order.remaining_qty,
                                visibility_ts_ns=visibility_ts_ns,
                                full_fill=False,
                            )
                        else:
                            order.lifecycle.activate(
                                visibility_ts_ns,
                                exchange_ts_ns=exchange_ts_ns,
                            )
                            order.lifecycle.observe_fill(
                                remaining_after=order.remaining_qty,
                                visibility_ts_ns=visibility_ts_ns,
                                exchange_ts_ns=exchange_ts_ns,
                            )
                    else:
                        order.lifecycle.observe_fill(
                            remaining_after=order.remaining_qty,
                            visibility_ts_ns=visibility_ts_ns,
                            exchange_ts_ns=exchange_ts_ns,
                        )
                    lifecycle_event_type = "partial_fill"

            elif status == "FILLED":
                order.state = OrderState.FILLED
                is_new_fill = self._apply_fill(order, event)
                if order.lifecycle is not None:
                    if is_new_fill:
                        if (
                            order.lifecycle.phase
                            == OrderLifecyclePhase.SUBMITTED
                        ):
                            if reconciled_unknown_prefix:
                                order.lifecycle.observe_fill_with_unknown_activation(
                                    remaining_after=0.0,
                                    visibility_ts_ns=visibility_ts_ns,
                                    full_fill=True,
                                )
                            else:
                                order.lifecycle.activate(
                                    visibility_ts_ns,
                                    exchange_ts_ns=exchange_ts_ns,
                                )
                                order.lifecycle.observe_fill(
                                    remaining_after=0.0,
                                    visibility_ts_ns=visibility_ts_ns,
                                    exchange_ts_ns=exchange_ts_ns,
                                    full_fill=True,
                                )
                        else:
                            order.lifecycle.observe_fill(
                                remaining_after=0.0,
                                visibility_ts_ns=visibility_ts_ns,
                                exchange_ts_ns=exchange_ts_ns,
                                full_fill=True,
                            )
                    else:
                        order.lifecycle.exchange_terminal(
                            visibility_ts_ns,
                            exchange_ts_ns=exchange_ts_ns,
                            reason="full_fill",
                        )
                self._move_to_history(order.client_order_id)
                terminal_reason = "full_fill"
                lifecycle_event_type = "full_fill"

            elif status == "CANCELED":
                is_new_fill, fill_completed = self._apply_terminal_status_fill(
                    order,
                    event,
                    visibility_ts_ns=visibility_ts_ns,
                    exchange_ts_ns=exchange_ts_ns,
                    reconciled_unknown_prefix=reconciled_unknown_prefix,
                )
                order.state = OrderState.FILLED if fill_completed else OrderState.CANCELED
                if order.lifecycle is not None and not fill_completed:
                    if (
                        reconciled_unknown_prefix
                        and order.lifecycle.phase == OrderLifecyclePhase.SUBMITTED
                    ):
                        order.lifecycle.mark_activation_unknown(
                            reason="rest_reconcile_activation_unknown"
                        )
                    order.lifecycle.exchange_terminal(
                        visibility_ts_ns,
                        exchange_ts_ns=(0 if reconciled_unknown_prefix else exchange_ts_ns),
                        reason="cancel_ack",
                    )
                self._move_to_history(order.client_order_id)
                terminal_reason = "full_fill" if fill_completed else "cancel_ack"
                lifecycle_event_type = "full_fill" if fill_completed else "cancel_ack"

            elif status == "EXPIRED":
                is_new_fill, fill_completed = self._apply_terminal_status_fill(
                    order,
                    event,
                    visibility_ts_ns=visibility_ts_ns,
                    exchange_ts_ns=exchange_ts_ns,
                    reconciled_unknown_prefix=reconciled_unknown_prefix,
                )
                order.state = OrderState.FILLED if fill_completed else OrderState.EXPIRED
                if order.lifecycle is not None and not fill_completed:
                    if (
                        reconciled_unknown_prefix
                        and order.lifecycle.phase == OrderLifecyclePhase.SUBMITTED
                    ):
                        order.lifecycle.mark_activation_unknown(
                            reason="rest_reconcile_activation_unknown"
                        )
                    order.lifecycle.exchange_terminal(
                        visibility_ts_ns,
                        exchange_ts_ns=(0 if reconciled_unknown_prefix else exchange_ts_ns),
                        reason="expired",
                    )
                self._move_to_history(order.client_order_id)
                terminal_reason = "full_fill" if fill_completed else "expired"
                lifecycle_event_type = "full_fill" if fill_completed else "expired"

            elif status == "REJECTED":
                is_new_fill, fill_completed = self._apply_terminal_status_fill(
                    order,
                    event,
                    visibility_ts_ns=visibility_ts_ns,
                    exchange_ts_ns=exchange_ts_ns,
                    reconciled_unknown_prefix=reconciled_unknown_prefix,
                )
                order.state = OrderState.FILLED if fill_completed else OrderState.REJECTED
                if order.lifecycle is not None and not fill_completed:
                    if (
                        reconciled_unknown_prefix
                        and order.lifecycle.phase == OrderLifecyclePhase.SUBMITTED
                    ):
                        order.lifecycle.mark_activation_unknown(
                            reason="rest_reconcile_activation_unknown"
                        )
                    order.lifecycle.exchange_terminal(
                        visibility_ts_ns,
                        exchange_ts_ns=(0 if reconciled_unknown_prefix else exchange_ts_ns),
                        reason="rejected",
                    )
                self._move_to_history(order.client_order_id)
                terminal_reason = "full_fill" if fill_completed else "rejected"
                lifecycle_event_type = "full_fill" if fill_completed else "rejected"

            order.update_time = visibility_ts_ns / 1_000_000_000.0
            logger.info(
                f"ORDER_UPDATE {order.client_order_id} "
                f"{prev_state.name}→{order.state.name} "
                f"filled={order.filled_qty}/{order.quantity} "
                f"avg_px={order.avg_fill_price:.1f}"
            )

        # callbacks outside lock，避免库存/下单逻辑回调时反向调用 OrderManager 造成死锁。
        if lifecycle_event_type and self._on_lifecycle_event:
            self._on_lifecycle_event(
                order,
                lifecycle_event_type,
                dict(event),
            )
        if is_new_fill and self._on_fill:
            self._on_fill(order, event)
        if status == "CANCELED" and self._on_cancel:
            self._on_cancel(order)
        if terminal_reason and self._on_terminal:
            self._on_terminal(order, terminal_reason)

    def _apply_fill(self, order: Order, event: dict):
        """Update fill quantities from event. Returns True if new fill detected."""
        prev_filled_qty = order.filled_qty
        new_cum_qty = float(event.get("z", order.filled_qty))
        fill_qty = new_cum_qty - prev_filled_qty
        if fill_qty <= 1e-10:
            # Duplicate WS event with same cumulative qty — skip
            return False
        order.filled_qty = new_cum_qty
        order.avg_fill_price = float(event.get("ap", order.avg_fill_price))
        event["_fill_qty"] = fill_qty
        event["_fill_price"] = float(event.get("L") or order.avg_fill_price or order.price)
        event["_fill_commission"] = float(event.get("n", 0))
        event["_fill_commission_asset"] = str(event.get("N") or "").upper()
        return True

    def _move_to_history(self, cid: str):
        """Move terminal order from active to history."""
        order = self._orders.pop(cid, None)
        if order:
            self._history[cid] = order
            if order.order_id in self._oid_map:
                del self._oid_map[order.order_id]
            # trim history
            while len(self._history) > self._max_history:
                self._history.popitem(last=False)

    def _adopt_orphan(self, cid: str, oid: int, event: dict):
        """Adopt an orphan order from a previous session so fills are tracked.

        IMPORTANT: Caller must already hold self._lock.
        We register the order inline (no nested lock) and process the
        event directly (no recursive on_order_update call) to avoid
        deadlocking on the non-reentrant threading.Lock.
        """
        status = event.get("X", "")
        side_str = event.get("S", "BUY")
        side = Side.BUY if side_str == "BUY" else Side.SELL
        price = float(event.get("p", 0))
        qty = float(event.get("q", 0))
        visibility_ts_ns = self._visibility_ts_ns(event)
        exchange_ts_ns = self._exchange_ts_ns(event)

        order = Order(
            client_order_id=cid, symbol=event.get("s", ""),
            side=side, price=price, quantity=qty,
            state=OrderState.OPEN, order_id=oid,
            create_time=visibility_ts_ns / 1_000_000_000.0,
            lifecycle=QuantityWeightedOrderLifecycle(
                initial_quantity=qty,
                submitted_ts_ns=visibility_ts_ns,
            ),
            orphan_adoption=True,
            left_truncation_reason="exchange_callback_without_local_submit",
        )
        order.lifecycle.activate_with_unknown_prefix(
            visibility_ts_ns,
            reason="orphan_adoption",
        )
        # Already inside self._lock — mutate directly, no nested lock
        self._orders[cid] = order
        self._oid_map[oid] = cid
        logger.info(f"ADOPTED orphan order {cid} {side_str} {qty}@{price}")

        # Process the event inline instead of calling on_order_update
        # (which would try to acquire self._lock again → deadlock)
        is_new_fill = False
        if status == "NEW":
            order.state = OrderState.OPEN
        elif status == "PARTIALLY_FILLED":
            order.state = OrderState.PARTIALLY_FILLED
            is_new_fill = self._apply_fill(order, event)
            if is_new_fill:
                order.lifecycle.observe_fill(
                    remaining_after=order.remaining_qty,
                    visibility_ts_ns=visibility_ts_ns,
                    exchange_ts_ns=exchange_ts_ns,
                )
        elif status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
            is_new_fill, fill_completed = self._apply_terminal_status_fill(
                order,
                event,
                visibility_ts_ns=visibility_ts_ns,
                exchange_ts_ns=exchange_ts_ns,
                reconciled_unknown_prefix=False,
            )
            terminal_reason = {
                "FILLED": "full_fill",
                "CANCELED": "cancel_ack",
                "EXPIRED": "expired",
                "REJECTED": "rejected",
            }[status]
            if status == "FILLED" and not fill_completed:
                # FILLED without a complete cumulative quantity is an invalid
                # exchange claim.  Keep the adopted order in the active risk
                # set instead of manufacturing a terminal quantity.
                order.state = (
                    OrderState.PARTIALLY_FILLED
                    if order.filled_qty > 0.0
                    else OrderState.OPEN
                )
                logger.error(
                    "ORPHAN_FILLED_QUANTITY_INCOMPLETE cid=%s filled=%s qty=%s",
                    cid,
                    order.filled_qty,
                    order.quantity,
                )
            else:
                order.state = (
                    OrderState.FILLED
                    if fill_completed
                    else {
                        "CANCELED": OrderState.CANCELED,
                        "EXPIRED": OrderState.EXPIRED,
                        "REJECTED": OrderState.REJECTED,
                    }.get(status, OrderState.FILLED)
                )
                if not fill_completed:
                    order.lifecycle.exchange_terminal(
                        visibility_ts_ns,
                        exchange_ts_ns=exchange_ts_ns,
                        reason=terminal_reason,
                    )
                self._move_to_history(cid)
        order.update_time = visibility_ts_ns / 1_000_000_000.0

        # Store fill/cancel info so caller can trigger callbacks outside lock
        self._adopted_fill = (order, event) if is_new_fill else None
        self._adopted_cancel = order if status == "CANCELED" else None
        terminal_reason = {
            "FILLED": "full_fill" if order.is_terminal else "",
            "CANCELED": "full_fill" if order.state == OrderState.FILLED else "cancel_ack",
            "EXPIRED": "full_fill" if order.state == OrderState.FILLED else "expired",
            "REJECTED": "full_fill" if order.state == OrderState.FILLED else "rejected",
        }.get(status, "")
        self._adopted_terminal = (
            (order, terminal_reason) if terminal_reason else None
        )
        lifecycle_event_type = {
            "NEW": "activate_unknown_prefix",
            "PARTIALLY_FILLED": "partial_fill",
            "FILLED": (
                "full_fill"
                if order.is_terminal
                else ("partial_fill" if is_new_fill else "activate_unknown_prefix")
            ),
            "CANCELED": "full_fill" if order.state == OrderState.FILLED else "cancel_ack",
            "EXPIRED": "full_fill" if order.state == OrderState.FILLED else "expired",
            "REJECTED": "full_fill" if order.state == OrderState.FILLED else "rejected",
        }.get(status, "")
        self._adopted_lifecycle_event = (
            (order, lifecycle_event_type, dict(event))
            if lifecycle_event_type
            else None
        )

    # ── queries ──

    def get_active_orders(self) -> List[Order]:
        with self._lock:
            return list(self._orders.values())

    def get_active_by_side(self, side: Side) -> List[Order]:
        with self._lock:
            return [o for o in self._orders.values() if o.side == side]

    def get_bid_orders(self) -> List[Order]:
        return self.get_active_by_side(Side.BUY)

    def get_ask_orders(self) -> List[Order]:
        return self.get_active_by_side(Side.SELL)

    def get_order(self, cid: str) -> Optional[Order]:
        with self._lock:
            return self._orders.get(cid) or self._history.get(cid)

    def lifecycle_snapshot(
        self,
        cid: str,
        *,
        now_ns: int | None = None,
    ) -> Optional[dict[str, object]]:
        with self._lock:
            order = self._orders.get(cid) or self._history.get(cid)
            if order is None or order.lifecycle is None:
                return None
            return order.lifecycle.snapshot(now_ns=now_ns)

    def lifecycle_events(self, cid: str) -> tuple[dict[str, object], ...]:
        with self._lock:
            order = self._orders.get(cid) or self._history.get(cid)
            if order is None or order.lifecycle is None:
                return ()
            return order.lifecycle.events()

    def has_active_orders(self) -> bool:
        with self._lock:
            return len(self._orders) > 0

    def active_count(self) -> int:
        with self._lock:
            return len(self._orders)

    def get_stale_pending_cancel_orders(self, max_age: float) -> List[Order]:
        """Find cancel requests that did not converge through user stream."""
        now = time.time()
        with self._lock:
            return [
                o for o in self._orders.values()
                if o.state == OrderState.PENDING_CANCEL
                and now - (o.update_time or o.create_time) > max_age
            ]

    def get_stale_orders(self, max_age: float) -> List[Order]:
        """Find orders older than max_age seconds in PENDING_NEW state."""
        now = time.time()
        with self._lock:
            return [o for o in self._orders.values()
                    if o.state == OrderState.PENDING_NEW
                    and now - o.create_time > max_age]

    def reconcile_pending_cancel(
        self,
        cid: str,
        *,
        exchange_open: bool,
        exchange_oid: int = 0,
    ) -> bool:
        """Restore a stale cancel only from an affirmatively open REST row.

        Absence from the open-order endpoint cannot distinguish cancellation,
        fill, expiry, response loss, or temporary visibility gaps.  It must not
        release local ownership; the caller must query the individual order.
        """
        now_ns = time.time_ns()
        with self._lock:
            o = self._orders.get(cid)
            if not o or o.state != OrderState.PENDING_CANCEL:
                return False
            prev_state = o.state
            if exchange_open:
                if exchange_oid:
                    o.order_id = exchange_oid
                    self._oid_map[exchange_oid] = cid
                o.update_time = now_ns / 1_000_000_000.0
                if o.lifecycle is not None:
                    o.lifecycle.cancel_rejected(now_ns)
                    o.state = (
                        OrderState.PARTIALLY_FILLED
                        if o.lifecycle.phase
                        == OrderLifecyclePhase.PARTIALLY_FILLED
                        else OrderState.OPEN
                    )
                else:
                    o.state = OrderState.OPEN
                logger.warning(
                    "STALE_PENDING_CANCEL_REOPENED %s %s→%s oid=%s",
                    cid,
                    prev_state.name,
                    o.state.name,
                    o.order_id,
                )
            else:
                logger.warning(
                    "STALE_PENDING_CANCEL_OPEN_ORDER_ABSENCE_UNRESOLVED %s",
                    cid,
                )
                return False
        return True

    def cancel_all_local(self):
        """Censor local shutdown without releasing unresolved submit ownership."""
        now_ns = time.time_ns()
        terminal_orders = []
        unknown_submit_orders = []
        with self._lock:
            for cid in list(self._orders.keys()):
                order = self._orders[cid]
                if (
                    order.lifecycle is not None
                    and order.lifecycle.phase == OrderLifecyclePhase.SUBMITTED
                ):
                    if not order.lifecycle.locally_censored:
                        order.lifecycle.censor_submit_ack_unknown(
                            now_ns,
                            reason="local_shutdown_unknown_ack",
                        )
                    order.update_time = now_ns / 1_000_000_000.0
                    unknown_submit_orders.append(order)
                    continue
                order.state = OrderState.CANCELED
                if order.lifecycle is not None:
                    if not order.lifecycle.locally_censored:
                        order.lifecycle.local_shutdown_censor(
                            now_ns,
                            reason="local_shutdown_cancel",
                        )
                terminal_orders.append(order)
                self._move_to_history(cid)
        for order in unknown_submit_orders:
            if self._on_lifecycle_event:
                self._on_lifecycle_event(
                    order,
                    "submit_ack_unknown_censored",
                    {
                        "_local_receive_ts_ns": now_ns,
                        "_reason": "local_shutdown_unknown_ack",
                        "_point_identified": False,
                    },
                )
        for order in terminal_orders:
            if self._on_lifecycle_event:
                self._on_lifecycle_event(
                    order,
                    "local_shutdown_cancel",
                    {"_local_receive_ts_ns": now_ns},
                )
            if self._on_terminal:
                self._on_terminal(order, "local_shutdown_cancel")
