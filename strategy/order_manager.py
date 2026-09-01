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
import math
import time
from collections import OrderedDict, deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from enum import Enum, auto
from threading import Condition, Lock, get_ident

from execution.order_lifecycle import (
    OrderLifecyclePhase,
    QuantityWeightedOrderLifecycle,
)
from execution.order_lifecycle_quantity_contract import (
    PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC,
    QUANTITY_INCREASE_ABS_TOLERANCE_BTC,
    TERMINAL_REMAINDER_ABS_TOLERANCE_BTC,
)

logger = logging.getLogger("order_manager")


class OrderState(Enum):
    PENDING_NEW = auto()  # 已发出REST请求，未收到确认
    OPEN = auto()  # NEW / PARTIALLY_FILLED 挂单中
    PARTIALLY_FILLED = auto()  # 部分成交
    FILLED = auto()  # 全部成交 (终态)
    CANCELED = auto()  # 已撤单 (终态)
    EXPIRED = auto()  # 过期 (终态)
    REJECTED = auto()  # 被拒 (终态)
    PENDING_CANCEL = auto()  # 撤单请求已发出


class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderManagerFatalError(RuntimeError):
    """Raised after the manager latches an unrecoverable delivery/state fault."""


class OrderReconciliationRequired(OrderManagerFatalError):
    """Raised when exact exchange reconciliation must precede further events."""


class _FillEvidenceGap(ValueError):
    """A cumulative jump cannot be priced/commissioned from the last trade."""


TERMINAL_STATES = {OrderState.FILLED, OrderState.CANCELED, OrderState.EXPIRED, OrderState.REJECTED}

_KNOWN_EXCHANGE_STATUSES = frozenset(
    {"NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED"}
)
_STATUSES_WITH_CUMULATIVE_FILL = frozenset(
    {"PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED"}
)
_AVERAGE_FILL_PRICE_REL_TOLERANCE = 1e-10
_AVERAGE_FILL_PRICE_ABS_TOLERANCE = 1e-8
_MARKET_ORDER_TYPES = frozenset(
    {
        "MARKET",
        "STOP_MARKET",
        "TAKE_PROFIT_MARKET",
        "TRAILING_STOP_MARKET",
    }
)


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
    order_id: int = 0  # exchange order id
    create_time: float = 0.0  # local timestamp
    update_time: float = 0.0
    lifecycle: QuantityWeightedOrderLifecycle | None = field(
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
        return self.state in (OrderState.OPEN, OrderState.PARTIALLY_FILLED, OrderState.PENDING_NEW)

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


@dataclass(frozen=True)
class _PreparedFill:
    """Validated cumulative-fill delta, not yet committed to the order ledger."""

    new_cum_qty: float
    fill_qty: float
    avg_fill_price: float
    fill_price: float
    commission: float
    commission_asset: str
    complete: bool
    trade_id: int = 0
    trade_time_ms: int = 0
    stale: bool = False

    @property
    def is_new_fill(self) -> bool:
        return self.fill_qty > PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC


@dataclass
class _CallbackBatch:
    """External callbacks accumulated while holding the state-machine lock."""

    order: Order
    event: dict
    emit_fill: bool = False
    emit_cancel: bool = False
    terminal_reason: str = ""
    lifecycle_event_type: str = ""
    fatal_after_dispatch_reason: str = ""
    dispatch_sequence: int = 0
    dispatch_done: bool = False
    dispatch_error: BaseException | None = field(default=None, repr=False)
    dispatch_wait_deferred: bool = False


@dataclass
class _OrderTombstone:
    """Non-evicting process-lifetime identity and cumulative-fill cursor."""

    client_order_id: str
    order_id: int
    symbol: str
    side: Side
    price: float
    quantity: float
    filled_qty: float
    avg_fill_price: float
    terminal_state: OrderState
    terminal_reason: str
    max_trade_id: int


@dataclass(frozen=True)
class _TradeIdentity:
    """Non-evicting exact economics bound to one exchange trade ID."""

    exchange_order_id: int
    symbol: str
    side: Side
    quantity: float
    price: float
    commission: float
    commission_asset: str
    trade_time_ms: int
    cumulative_fill: float


class OrderManager:
    """
    Thread-safe order state machine with event-driven updates.

    Usage:
        om = OrderManager(on_fill=my_callback)
        cid = om.create_order("BTCUSDT", Side.BUY, 85000.0, 0.005)
        om.confirm_new(cid, exchange_order_id)
        om.on_order_update(event_dict)   # from user_data stream
    """

    def __init__(
        self,
        on_fill: Callable | None = None,
        on_cancel: Callable | None = None,
        on_terminal: Callable | None = None,
        on_lifecycle_event: Callable | None = None,
        max_history: int = 500,
        allowed_symbols: Iterable[str] | None = None,
    ):
        self._lock = Lock()
        self._orders: dict[str, Order] = {}  # cid → Order (active)
        self._history: OrderedDict = OrderedDict()  # cid → Order (terminal)
        self._oid_map: dict[int, str] = {}  # exchange_oid → cid
        self._tombstones: dict[str, _OrderTombstone] = {}
        self._tombstone_oid_map: dict[int, str] = {}
        self._trade_ids_by_oid: dict[int, set[int]] = {}
        self._trade_identity_by_symbol_id: dict[tuple[str, int], _TradeIdentity] = {}
        self._max_history = max_history
        self._generation = 0
        self._fatal_latched = False
        self._fatal_reason = ""
        self._fatal_client_order_id = ""
        self._fatal_callback = ""
        self._fatal_ts_ns = 0
        self._reconciliation_required = False
        self._allowed_symbols_explicit = allowed_symbols is not None
        self._allowed_symbols = {
            self._canonical_symbol(symbol, label="allowed symbol")
            for symbol in (allowed_symbols or ())
        }

        # Callback delivery is a second, strictly ordered phase after ledger
        # commit.  State transitions append batches while holding ``_lock``;
        # exactly one caller drains them after releasing it.  This prevents a
        # later REST reconciliation callback from overtaking an earlier WS
        # fill callback while preserving callback re-entry into this manager.
        self._callback_commit_sequence = 0
        self._callback_dispatched_sequence = 0
        self._callback_dispatch_condition = Condition(Lock())
        self._callback_dispatch_queue: deque[_CallbackBatch] = deque()
        self._callback_dispatch_drainer_thread_id: int | None = None
        self._callback_dispatch_failure: BaseException | None = None
        self._callback_dispatch_failure_sequence = 0

        # callbacks
        self._on_fill = on_fill
        self._on_cancel = on_cancel
        self._on_terminal = on_terminal
        self._on_lifecycle_event = on_lifecycle_event

    def _latch_fatal_locked(
        self,
        reason: str,
        *,
        cid: str = "",
        callback: str = "",
        reconciliation_required: bool,
    ) -> None:
        normalized_reason = str(reason).strip() or "unspecified_order_manager_fatal"
        if not self._fatal_latched:
            self._fatal_latched = True
            self._fatal_reason = normalized_reason
            self._fatal_client_order_id = str(cid)
            self._fatal_callback = str(callback)
            self._fatal_ts_ns = int(time.time() * 1_000_000_000.0)
        elif callback and not self._fatal_callback:
            self._fatal_callback = str(callback)
        self._reconciliation_required = bool(
            self._reconciliation_required or reconciliation_required
        )

    def _latch_fatal(
        self,
        reason: str,
        *,
        cid: str = "",
        callback: str = "",
        reconciliation_required: bool,
    ) -> None:
        with self._lock:
            self._latch_fatal_locked(
                reason,
                cid=cid,
                callback=callback,
                reconciliation_required=reconciliation_required,
            )

    def _raise_if_fatal_locked(self) -> None:
        if self._fatal_latched:
            raise OrderManagerFatalError("order manager fatal latch is set: " + self._fatal_reason)

    def _mark_reconciliation_required_locked(self, cid: str, reason: str) -> str:
        normalized = f"reconciliation_required:{str(reason).strip()}"
        self._latch_fatal_locked(
            normalized,
            cid=cid,
            reconciliation_required=True,
        )
        logger.critical(
            "ORDER_MANAGER_RECONCILIATION_REQUIRED cid=%s reason=%s",
            cid,
            reason,
        )
        return normalized

    @property
    def fatal_latched(self) -> bool:
        with self._lock:
            return self._fatal_latched

    @property
    def reconciliation_required(self) -> bool:
        with self._lock:
            return self._reconciliation_required

    def fatal_status(self) -> dict[str, object]:
        """Return the non-clearable process-lifetime fatal/reconcile latch."""

        with self._lock:
            return {
                "latched": self._fatal_latched,
                "reason": self._fatal_reason,
                "client_order_id": self._fatal_client_order_id,
                "callback": self._fatal_callback,
                "latched_ts_ns": self._fatal_ts_ns,
                "reconciliation_required": self._reconciliation_required,
            }

    def in_callback_dispatch(self) -> bool:
        """Return whether the caller is inside this manager's callback drainer.

        Consumers use this narrow signal to defer blocking reconciliation until
        the external callback has unwound.  It does not expose queue ownership
        to other threads and therefore cannot be used to bypass FIFO delivery.
        """

        with self._callback_dispatch_condition:
            return self._callback_dispatch_drainer_thread_id == get_ident()

    def callback_dispatch_active(self) -> bool:
        """Return whether committed callback work is not fully quiescent.

        A committed batch briefly exists before a thread claims the drainer.
        Treat that interval as active too: consumers must not act on callback-
        produced state until the queue is empty and every committed sequence
        has been dispatched.
        """

        with self._callback_dispatch_condition:
            return bool(
                self._callback_dispatch_drainer_thread_id is not None
                or self._callback_dispatch_queue
                or self._callback_commit_sequence
                != self._callback_dispatched_sequence
                or self._callback_dispatch_failure is not None
            )

    def _invoke_callback(
        self,
        callback_name: str,
        callback: Callable,
        *args,
        cid: str,
    ) -> None:
        try:
            callback(*args)
        except Exception as exc:
            reason = f"callback_delivery_failed:{callback_name}:{type(exc).__name__}:{exc}"
            self._latch_fatal(
                reason,
                cid=cid,
                callback=callback_name,
                reconciliation_required=True,
            )
            logger.critical(
                "ORDER_MANAGER_CALLBACK_FATAL cid=%s callback=%s",
                cid,
                callback_name,
                exc_info=True,
            )
            raise

    # ── order creation ──

    def create_order(self, symbol: str, side: Side, price: float, quantity: float) -> str:
        """Register a new order (before sending REST request)."""
        symbol = self._canonical_symbol(symbol, label="order symbol")
        # A zero submit price is the existing MARKET-order sentinel. Actual
        # execution prices are validated as strictly positive before a fill.
        price = self._finite_float(
            price,
            label="order price",
            nonnegative=True,
        )
        quantity = self._positive_float(
            quantity,
            label="order quantity",
        )
        with self._lock:
            self._raise_if_fatal_locked()
            if self._allowed_symbols_explicit and symbol not in self._allowed_symbols:
                raise ValueError(f"order symbol is not configured: {symbol}")
            self._allowed_symbols.add(symbol)
            self._generation += 1
            now_ns = time.time_ns()
            cid = f"mm_{side.value[0]}_{now_ns // 1_000_000}_{self._generation}"
            order = Order(
                client_order_id=cid,
                symbol=symbol,
                side=side,
                price=price,
                quantity=quantity,
                state=OrderState.PENDING_NEW,
                create_time=now_ns / 1_000_000_000.0,
                lifecycle=QuantityWeightedOrderLifecycle(
                    initial_quantity=float(quantity),
                    submitted_ts_ns=now_ns,
                ),
            )
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
        exchange_oid = int(exchange_oid)
        if exchange_oid <= 0:
            raise ValueError("exchange order ID must be positive")
        now_ns = time.time_ns()
        batch = None
        with self._lock:
            self._raise_if_fatal_locked()
            o = self._orders.get(cid)
            if o is not None and o.state != OrderState.PENDING_NEW:
                if o.order_id != exchange_oid:
                    fatal_reason = self._mark_reconciliation_required_locked(
                        cid,
                        "duplicate REST ACK exchange order ID mismatch: "
                        f"event={exchange_oid} ledger={o.order_id}",
                    )
                    raise OrderReconciliationRequired(fatal_reason)
                return
            if o is not None:
                active_cid = self._oid_map.get(exchange_oid)
                terminal_cid = self._tombstone_oid_map.get(exchange_oid)
                if active_cid not in (None, cid) or terminal_cid not in (None, cid):
                    fatal_reason = self._mark_reconciliation_required_locked(
                        cid,
                        f"REST ACK exchange order ID collision: oid={exchange_oid} "
                        f"active_cid={active_cid!r} terminal_cid={terminal_cid!r}",
                    )
                    raise OrderReconciliationRequired(fatal_reason)
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
                if self._on_lifecycle_event:
                    batch = self._queue_callback_batch_locked(
                        _CallbackBatch(
                            order=o,
                            event={
                                "_local_receive_ts_ns": now_ns,
                                "_exchange_ts_ns": int(exchange_ts_ns),
                            },
                            lifecycle_event_type="rest_ack",
                        )
                    )
            else:
                terminal = self._history.get(cid)
                tombstone = self._tombstones.get(cid)
                expected_oid = (
                    terminal.order_id
                    if terminal is not None
                    else (tombstone.order_id if tombstone is not None else None)
                )
                if expected_oid is not None and expected_oid != exchange_oid:
                    fatal_reason = self._mark_reconciliation_required_locked(
                        cid,
                        "late REST ACK exchange order ID mismatch: "
                        f"event={exchange_oid} ledger={expected_oid}",
                    )
                    raise OrderReconciliationRequired(fatal_reason)
        if batch is not None:
            self._dispatch_callbacks(batch)

    def bind_exchange_order_identity(
        self,
        cid: str,
        exchange_oid: int,
        *,
        activation_unknown: bool = True,
    ) -> bool:
        """Bind a REST-observed OID without fabricating fill economics.

        This is the identity-only precursor to ``reconcile_exchange_trade``
        for query/RESULT rows whose cumulative status has no per-trade price
        and commission evidence.  ``activation_unknown`` preserves the
        unobserved exchange-exposure prefix instead of inventing an ACK clock.
        """

        exchange_oid = int(exchange_oid)
        if exchange_oid <= 0:
            raise ValueError("exchange order ID must be positive")
        now_ns = time.time_ns()
        bound = None
        lifecycle_event_type = ""
        batch = None
        with self._lock:
            self._raise_if_fatal_locked()
            order = self._orders.get(cid)
            if order is None:
                terminal = self._history.get(cid)
                tombstone = self._tombstones.get(cid)
                expected_oid = (
                    terminal.order_id
                    if terminal is not None
                    else (tombstone.order_id if tombstone is not None else None)
                )
                if expected_oid is None:
                    raise ValueError(f"unknown client order ID: {cid}")
                if expected_oid != exchange_oid:
                    reason = self._mark_reconciliation_required_locked(
                        cid,
                        "identity-only REST bind changed terminal exchange order ID: "
                        f"event={exchange_oid} ledger={expected_oid}",
                    )
                    raise OrderReconciliationRequired(reason)
                return False
            if order.order_id:
                if order.order_id != exchange_oid:
                    reason = self._mark_reconciliation_required_locked(
                        cid,
                        "identity-only REST bind changed active exchange order ID: "
                        f"event={exchange_oid} ledger={order.order_id}",
                    )
                    raise OrderReconciliationRequired(reason)
                return False
            if order.state != OrderState.PENDING_NEW:
                reason = self._mark_reconciliation_required_locked(
                    cid,
                    f"identity-only REST bind requires PENDING_NEW: state={order.state.name}",
                )
                raise OrderReconciliationRequired(reason)
            active_cid = self._oid_map.get(exchange_oid)
            terminal_cid = self._tombstone_oid_map.get(exchange_oid)
            if active_cid not in (None, cid) or terminal_cid not in (None, cid):
                reason = self._mark_reconciliation_required_locked(
                    cid,
                    f"identity-only REST bind collided: oid={exchange_oid} "
                    f"active_cid={active_cid!r} terminal_cid={terminal_cid!r}",
                )
                raise OrderReconciliationRequired(reason)
            order.order_id = exchange_oid
            order.state = OrderState.OPEN
            order.update_time = now_ns / 1_000_000_000.0
            self._oid_map[exchange_oid] = cid
            if order.lifecycle is not None:
                if activation_unknown:
                    order.lifecycle.activate_with_unknown_prefix(
                        now_ns,
                        reason="rest_identity_bind_without_exact_fill",
                    )
                    lifecycle_event_type = "activate_unknown_prefix"
                else:
                    order.lifecycle.activate(now_ns)
                    lifecycle_event_type = "activate"
            bound = order
            if lifecycle_event_type and self._on_lifecycle_event:
                batch = self._queue_callback_batch_locked(
                    _CallbackBatch(
                        order=order,
                        event={
                            "_local_receive_ts_ns": now_ns,
                            "_identity_bind_only": True,
                            "_activation_unknown": bool(activation_unknown),
                        },
                        lifecycle_event_type=lifecycle_event_type,
                    )
                )

        if batch is not None:
            self._dispatch_callbacks(batch)
        return bound is not None

    def confirm_rejected(self, cid: str, reason: str = ""):
        """Order was rejected by exchange."""
        now_ns = time.time_ns()
        batch = None
        with self._lock:
            self._raise_if_fatal_locked()
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
                if self._on_lifecycle_event or self._on_terminal:
                    batch = self._queue_callback_batch_locked(
                        _CallbackBatch(
                            order=o,
                            event={
                                "_local_receive_ts_ns": now_ns,
                                "_reason": str(reason),
                            },
                            lifecycle_event_type="rejected",
                            terminal_reason="rejected",
                        )
                    )
        if batch is not None:
            self._dispatch_callbacks(batch)

    def mark_submit_ack_unknown(self, cid: str, reason: str) -> bool:
        """Keep a submit in-flight until exchange/user-stream reconciliation."""

        now_ns = time.time_ns()
        pending = None
        batch = None
        with self._lock:
            self._raise_if_fatal_locked()
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
            if self._on_lifecycle_event:
                batch = self._queue_callback_batch_locked(
                    _CallbackBatch(
                        order=order,
                        event={
                            "_local_receive_ts_ns": now_ns,
                            "_reason": str(reason),
                            "_point_identified": False,
                        },
                        lifecycle_event_type="submit_ack_unknown",
                    )
                )
        if batch is not None:
            self._dispatch_callbacks(batch)
        return pending is not None

    def censor_submit_ack_unknown(self, cid: str, reason: str) -> bool:
        """Censor local observation during shutdown without releasing live ownership."""

        now_ns = time.time_ns()
        censored = None
        batch = None
        with self._lock:
            self._raise_if_fatal_locked()
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
            if self._on_lifecycle_event:
                batch = self._queue_callback_batch_locked(
                    _CallbackBatch(
                        order=order,
                        event={
                            "_local_receive_ts_ns": now_ns,
                            "_reason": str(reason),
                            "_point_identified": False,
                        },
                        lifecycle_event_type="submit_ack_unknown_censored",
                    )
                )
        if batch is not None:
            self._dispatch_callbacks(batch)
        return censored is not None

    def mark_pending_cancel(self, cid: str):
        """Mark order as awaiting cancel confirmation."""
        now_ns = time.time_ns()
        with self._lock:
            self._raise_if_fatal_locked()
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
        batch = None
        with self._lock:
            self._raise_if_fatal_locked()
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
                o.state = OrderState.PARTIALLY_FILLED if o.filled_qty > 0.0 else OrderState.OPEN
            o.update_time = now_ns / 1_000_000_000.0
            restored = o
            logger.warning("ORDER_CANCEL_REJECTED %s: %s", cid, reason)
            if self._on_lifecycle_event:
                batch = self._queue_callback_batch_locked(
                    _CallbackBatch(
                        order=o,
                        event={
                            "_local_receive_ts_ns": now_ns,
                            "_exchange_ts_ns": int(exchange_ts_ns),
                            "_reason": str(reason),
                        },
                        lifecycle_event_type="cancel_rejected",
                    )
                )
        if batch is not None:
            self._dispatch_callbacks(batch)
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

    @staticmethod
    def _canonical_symbol(value: object, *, label: str) -> str:
        if not isinstance(value, str):
            raise ValueError(f"{label} must be a string")
        symbol = value.strip()
        if not symbol or symbol != value or symbol != symbol.upper():
            raise ValueError(f"{label} must be a non-empty canonical exchange symbol")
        return symbol

    @staticmethod
    def _finite_float(value: object, *, label: str, nonnegative: bool) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{label} must be numeric") from exc
        if not math.isfinite(parsed):
            raise ValueError(f"{label} must be finite")
        if nonnegative and parsed < 0.0:
            raise ValueError(f"{label} must be non-negative")
        return parsed

    @classmethod
    def _positive_float(cls, value: object, *, label: str) -> float:
        parsed = cls._finite_float(value, label=label, nonnegative=True)
        if parsed <= 0.0:
            raise ValueError(f"{label} must be positive")
        return parsed

    @staticmethod
    def _event_side(value: object) -> Side:
        if isinstance(value, Side):
            return value
        if (
            not isinstance(value, str)
            or value not in {Side.BUY.value, Side.SELL.value}
        ):
            raise ValueError("event side must be BUY or SELL")
        return Side(value)

    @staticmethod
    def _event_order_type(value: object) -> str:
        if value in (None, ""):
            return ""
        if not isinstance(value, str):
            raise ValueError("event order type must be a string")
        if not value or value != value.strip() or value != value.upper():
            raise ValueError("event order type must be canonical")
        return value

    @staticmethod
    def _event_commission_asset(value: object) -> str:
        if value in (None, ""):
            return ""
        if not isinstance(value, str):
            raise ValueError("commission asset must be a string")
        if value != value.strip() or value != value.upper():
            raise ValueError("commission asset must be canonical")
        return value

    def _validate_ws_order_economics_locked(
        self,
        order: Order | _OrderTombstone,
        event: dict,
        *,
        context: str,
    ) -> None:
        """Validate immutable WS order identity before any ledger mutation."""

        cid = order.client_order_id
        try:
            event_symbol = self._canonical_symbol(
                event.get("s"),
                label="event symbol",
            )
            if event_symbol != order.symbol:
                raise ValueError(
                    f"event symbol mismatch: event={event_symbol!r} ledger={order.symbol!r}"
                )

            event_side = self._event_side(event.get("S"))
            if event_side is not order.side:
                raise ValueError(
                    f"event side mismatch: event={event_side.value} ledger={order.side.value}"
                )

            event_quantity = self._positive_float(
                event.get("q"),
                label="event original quantity",
            )
            ledger_quantity = self._positive_float(
                order.quantity,
                label="ledger original quantity",
            )
            if (
                abs(event_quantity - ledger_quantity)
                > QUANTITY_INCREASE_ABS_TOLERANCE_BTC
            ):
                raise ValueError(
                    "event original quantity mismatch: "
                    f"event={event_quantity:.17g} ledger={ledger_quantity:.17g}"
                )

            order_type = self._event_order_type(event.get("o"))
            ledger_price = self._finite_float(
                order.price,
                label="ledger order price",
                nonnegative=True,
            )
            market_order = ledger_price == 0.0
            if market_order and order_type and order_type not in _MARKET_ORDER_TYPES:
                raise ValueError(
                    "event order type disagrees with ledger market-order sentinel"
                )
            if not market_order and order_type in _MARKET_ORDER_TYPES:
                raise ValueError(
                    "event order type disagrees with ledger limit order price"
                )
            event_price_raw = event.get("p")
            if market_order:
                if event_price_raw not in (None, ""):
                    self._finite_float(
                        event_price_raw,
                        label="event order price",
                        nonnegative=True,
                    )
            else:
                if event_price_raw in (None, ""):
                    raise ValueError("event limit order price is missing")
                event_price = self._finite_float(
                    event_price_raw,
                    label="event order price",
                    nonnegative=True,
                )
                if not math.isclose(
                    event_price,
                    ledger_price,
                    rel_tol=_AVERAGE_FILL_PRICE_REL_TOLERANCE,
                    abs_tol=_AVERAGE_FILL_PRICE_ABS_TOLERANCE,
                ):
                    raise ValueError(
                        "event limit order price mismatch: "
                        f"event={event_price:.17g} ledger={ledger_price:.17g}"
                    )
        except ValueError as exc:
            reason = self._mark_reconciliation_required_locked(
                cid,
                f"invalid {context} identity: {exc}",
            )
            raise OrderReconciliationRequired(reason) from exc

    def _prepare_fill(self, order: Order, event: dict) -> _PreparedFill:
        """Validate a cumulative fill without mutating the order or event."""

        quantity = self._finite_float(
            order.quantity,
            label="order quantity",
            nonnegative=True,
        )
        previous = self._finite_float(
            order.filled_qty,
            label="ledger cumulative fill",
            nonnegative=True,
        )
        if previous > quantity + QUANTITY_INCREASE_ABS_TOLERANCE_BTC:
            raise ValueError(
                "ledger cumulative fill exceeds order quantity: "
                f"filled={previous:.17g} quantity={quantity:.17g}"
            )

        new_cum_qty = self._finite_float(
            event.get("z", previous),
            label="event cumulative fill",
            nonnegative=True,
        )
        event_cum_qty = new_cum_qty
        stale_cumulative = (
            new_cum_qty < previous - QUANTITY_INCREASE_ABS_TOLERANCE_BTC
        )
        if new_cum_qty > quantity + QUANTITY_INCREASE_ABS_TOLERANCE_BTC:
            raise ValueError(
                "event cumulative fill exceeds order quantity: "
                f"filled={new_cum_qty:.17g} quantity={quantity:.17g}"
            )

        # Absorb only numerical noise.  The callback delta is always bounded
        # by the exact unfilled quantity, never by the exchange's overshoot.
        if new_cum_qty < previous:
            new_cum_qty = previous
        if new_cum_qty > quantity or quantity - new_cum_qty <= TERMINAL_REMAINDER_ABS_TOLERANCE_BTC:
            new_cum_qty = quantity
        fill_qty = new_cum_qty - previous
        if fill_qty <= PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC:
            fill_qty = 0.0
            new_cum_qty = previous

        complete = quantity - new_cum_qty <= TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
        trade_id = 0
        trade_time_ms = 0
        trade_id_raw = event.get("t")
        if trade_id_raw not in (None, ""):
            try:
                trade_id = int(trade_id_raw)
            except (TypeError, ValueError) as exc:
                raise ValueError("trade ID must be an integer") from exc
            if trade_id < 0:
                raise ValueError("trade ID must be non-negative")
        try:
            trade_time_ms = int(event.get("T", 0) or 0)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("exchange trade timestamp must be an integer") from exc
        if trade_time_ms < 0:
            raise ValueError("exchange trade timestamp must be non-negative")

        known_identity = (
            self._trade_identity_by_symbol_id.get((order.symbol, trade_id))
            if trade_id > 0
            else None
        )
        last_fill_raw = event.get("l")
        if known_identity is not None:
            if last_fill_raw in (None, ""):
                raise _FillEvidenceGap(
                    "known trade ID duplicate is missing last fill quantity"
                )
            duplicate_last_fill = self._finite_float(
                last_fill_raw,
                label="last fill quantity",
                nonnegative=True,
            )
            duplicate_fill_price = self._positive_float(
                event.get("L"),
                label="last fill price",
            )
            duplicate_commission_raw = event.get("n")
            if duplicate_commission_raw in (None, ""):
                duplicate_commission_raw = 0.0
            duplicate_identity = _TradeIdentity(
                exchange_order_id=order.order_id,
                symbol=order.symbol,
                side=order.side,
                quantity=duplicate_last_fill,
                price=duplicate_fill_price,
                commission=self._finite_float(
                    duplicate_commission_raw,
                    label="fill commission",
                    nonnegative=False,
                ),
                commission_asset=self._event_commission_asset(event.get("N")),
                trade_time_ms=trade_time_ms,
                cumulative_fill=(event_cum_qty if stale_cumulative else new_cum_qty),
            )
            if duplicate_identity != known_identity:
                raise _FillEvidenceGap(
                    "previously applied trade ID changed exact WS identity: "
                    f"trade_id={trade_id} event={duplicate_identity!r} "
                    f"ledger={known_identity!r}"
                )
            if fill_qty > 0.0:
                raise _FillEvidenceGap(
                    "previously applied trade ID carries a new cumulative delta: "
                    f"trade_id={trade_id}"
                )
            return _PreparedFill(
                new_cum_qty=new_cum_qty,
                fill_qty=0.0,
                avg_fill_price=float(order.avg_fill_price),
                fill_price=known_identity.price,
                commission=0.0,
                commission_asset=known_identity.commission_asset,
                complete=complete,
                trade_id=trade_id,
                trade_time_ms=trade_time_ms,
            )
        if trade_id > 0 and trade_id in self._trade_ids_by_oid.get(order.order_id, set()):
            raise _FillEvidenceGap(
                "seen trade ID has no exact process-lifetime identity: "
                f"trade_id={trade_id}"
            )
        if stale_cumulative:
            return _PreparedFill(
                new_cum_qty=previous,
                fill_qty=0.0,
                avg_fill_price=float(order.avg_fill_price),
                fill_price=float(order.price),
                commission=0.0,
                commission_asset="",
                complete=complete,
                stale=True,
            )
        if fill_qty <= 0.0:
            return _PreparedFill(
                new_cum_qty=new_cum_qty,
                fill_qty=0.0,
                avg_fill_price=float(order.avg_fill_price),
                fill_price=float(order.price),
                commission=0.0,
                commission_asset="",
                complete=complete,
            )
        if trade_id_raw not in (None, "") and trade_id <= 0:
            raise ValueError("trade ID must be positive for a fill")

        if last_fill_raw in (None, ""):
            raise _FillEvidenceGap("last fill quantity is missing for a positive cumulative delta")
        last_fill_qty = self._finite_float(
            last_fill_raw,
            label="last fill quantity",
            nonnegative=True,
        )
        if abs(last_fill_qty - fill_qty) > QUANTITY_INCREASE_ABS_TOLERANCE_BTC:
            raise _FillEvidenceGap(
                "cumulative fill delta does not equal last fill quantity: "
                f"delta={fill_qty:.17g} last_fill={last_fill_qty:.17g}"
            )

        fill_price_raw = event.get("L")
        if fill_price_raw in (None, ""):
            raise _FillEvidenceGap("last fill price is missing for a positive cumulative delta")
        fill_price = self._positive_float(
            fill_price_raw,
            label="last fill price",
        )

        if previous > PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC:
            prior_avg_fill_price = self._positive_float(
                order.avg_fill_price,
                label="ledger average fill price",
            )
        else:
            prior_avg_fill_price = 0.0
        expected_avg_fill_price = (
            previous * prior_avg_fill_price + fill_qty * fill_price
        ) / new_cum_qty
        avg_raw = event.get("ap")
        if avg_raw not in (None, ""):
            exchange_avg_fill_price = self._positive_float(
                avg_raw,
                label="average fill price",
            )
            if not math.isclose(
                exchange_avg_fill_price,
                expected_avg_fill_price,
                rel_tol=_AVERAGE_FILL_PRICE_REL_TOLERANCE,
                abs_tol=_AVERAGE_FILL_PRICE_ABS_TOLERANCE,
            ):
                raise _FillEvidenceGap(
                    "exchange average fill price is inconsistent with the "
                    "previous notional and exact last fill: "
                    f"event={exchange_avg_fill_price:.17g} "
                    f"expected={expected_avg_fill_price:.17g}"
                )
        # Store the locally derived cumulative notional.  The exchange's
        # cumulative average is a cross-check, never an independent source
        # that can rewrite already-proven fill economics.
        avg_fill_price = expected_avg_fill_price
        commission_raw = event.get("n")
        if commission_raw in (None, ""):
            commission_raw = 0.0
        commission = self._finite_float(
            commission_raw,
            label="fill commission",
            nonnegative=False,
        )
        return _PreparedFill(
            new_cum_qty=new_cum_qty,
            fill_qty=fill_qty,
            avg_fill_price=avg_fill_price,
            fill_price=fill_price,
            commission=commission,
            commission_asset=self._event_commission_asset(event.get("N")),
            complete=complete,
            trade_id=trade_id,
            trade_time_ms=trade_time_ms,
        )

    def _commit_fill(self, order: Order, event: dict, prepared: _PreparedFill) -> None:
        """Commit a previously validated delta and its callback payload."""

        if not prepared.is_new_fill:
            return
        order.filled_qty = prepared.new_cum_qty
        order.avg_fill_price = prepared.avg_fill_price
        event["_fill_qty"] = prepared.fill_qty
        event["_fill_price"] = prepared.fill_price
        event["_fill_commission"] = prepared.commission
        event["_fill_commission_asset"] = prepared.commission_asset
        if prepared.trade_id and order.order_id:
            self._trade_ids_by_oid.setdefault(order.order_id, set()).add(prepared.trade_id)
            identity = _TradeIdentity(
                exchange_order_id=order.order_id,
                symbol=order.symbol,
                side=order.side,
                quantity=prepared.fill_qty,
                price=prepared.fill_price,
                commission=prepared.commission,
                commission_asset=prepared.commission_asset,
                trade_time_ms=prepared.trade_time_ms,
                cumulative_fill=prepared.new_cum_qty,
            )
            trade_key = (order.symbol, prepared.trade_id)
            existing = self._trade_identity_by_symbol_id.get(trade_key)
            if existing is not None and existing != identity:
                raise AssertionError("validated trade identity changed before commit")
            self._trade_identity_by_symbol_id[trade_key] = identity

    @staticmethod
    def _observe_prepared_fill(
        order: Order,
        prepared: _PreparedFill,
        *,
        visibility_ts_ns: int,
        exchange_ts_ns: int,
        reconciled_unknown_prefix: bool,
    ) -> None:
        """Advance the lifecycle before committing the validated ledger delta."""

        lifecycle = order.lifecycle
        if lifecycle is None or not prepared.is_new_fill:
            return
        remaining_after = max(0.0, float(order.quantity) - prepared.new_cum_qty)
        if lifecycle.phase == OrderLifecyclePhase.SUBMITTED:
            if reconciled_unknown_prefix:
                lifecycle.observe_fill_with_unknown_activation(
                    remaining_after=remaining_after,
                    visibility_ts_ns=visibility_ts_ns,
                    full_fill=prepared.complete,
                )
            else:
                lifecycle.activate(
                    visibility_ts_ns,
                    exchange_ts_ns=exchange_ts_ns,
                )
                lifecycle.observe_fill(
                    remaining_after=remaining_after,
                    visibility_ts_ns=visibility_ts_ns,
                    exchange_ts_ns=exchange_ts_ns,
                    full_fill=prepared.complete,
                )
        else:
            lifecycle.observe_fill(
                remaining_after=remaining_after,
                visibility_ts_ns=visibility_ts_ns,
                exchange_ts_ns=exchange_ts_ns,
                full_fill=prepared.complete,
            )

    def _queue_callback_batch_locked(self, batch: _CallbackBatch) -> _CallbackBatch:
        """Commit one callback batch to the global delivery sequence.

        The caller must hold ``self._lock``.  Acquiring the separate dispatch
        condition in this direction is safe because the drainer never holds
        that condition while acquiring or invoking anything that can acquire
        the state lock.
        """

        self._callback_commit_sequence += 1
        batch.dispatch_sequence = self._callback_commit_sequence
        with self._callback_dispatch_condition:
            if self._callback_dispatch_failure is None:
                self._callback_dispatch_queue.append(batch)
            else:
                batch.dispatch_error = OrderManagerFatalError(
                    "callback delivery blocked by prior dispatch failure at "
                    f"sequence {self._callback_dispatch_failure_sequence}: "
                    f"{self._callback_dispatch_failure}"
                )
                batch.dispatch_done = True
            self._callback_dispatch_condition.notify_all()
        return batch

    def _invoke_callback_batch(self, batch: _CallbackBatch) -> None:
        """Invoke one already-committed batch without holding either lock."""

        if batch.lifecycle_event_type and self._on_lifecycle_event:
            self._invoke_callback(
                "on_lifecycle_event",
                self._on_lifecycle_event,
                batch.order,
                batch.lifecycle_event_type,
                dict(batch.event),
                cid=batch.order.client_order_id,
            )
        if batch.emit_fill and self._on_fill:
            self._invoke_callback(
                "on_fill",
                self._on_fill,
                batch.order,
                batch.event,
                cid=batch.order.client_order_id,
            )
        if batch.emit_cancel and self._on_cancel:
            self._invoke_callback(
                "on_cancel",
                self._on_cancel,
                batch.order,
                cid=batch.order.client_order_id,
            )
        if batch.terminal_reason and self._on_terminal:
            self._invoke_callback(
                "on_terminal",
                self._on_terminal,
                batch.order,
                batch.terminal_reason,
                cid=batch.order.client_order_id,
            )
        if batch.fatal_after_dispatch_reason:
            raise OrderReconciliationRequired(batch.fatal_after_dispatch_reason)

    def _fail_callback_dispatch(
        self,
        batch: _CallbackBatch,
        error: BaseException,
    ) -> BaseException | None:
        """Fail the current batch and prevent every later batch overtaking it."""

        deferred_error = error if batch.dispatch_wait_deferred else None
        with self._callback_dispatch_condition:
            batch.dispatch_error = error
            batch.dispatch_done = True
            self._callback_dispatched_sequence = batch.dispatch_sequence
            if self._callback_dispatch_failure is None:
                self._callback_dispatch_failure = error
                self._callback_dispatch_failure_sequence = batch.dispatch_sequence
            while self._callback_dispatch_queue:
                pending = self._callback_dispatch_queue.popleft()
                pending_error = OrderManagerFatalError(
                    "callback delivery blocked by prior dispatch failure at "
                    f"sequence {batch.dispatch_sequence}: {error}"
                )
                pending.dispatch_error = pending_error
                pending.dispatch_done = True
                if deferred_error is None and pending.dispatch_wait_deferred:
                    deferred_error = pending_error
            self._callback_dispatch_drainer_thread_id = None
            self._callback_dispatch_condition.notify_all()
        return deferred_error

    def _drain_callback_batches(self) -> BaseException | None:
        """Drain committed callback batches in sequence as the sole consumer."""

        deferred_error = None
        while True:
            with self._callback_dispatch_condition:
                if not self._callback_dispatch_queue:
                    self._callback_dispatch_drainer_thread_id = None
                    self._callback_dispatch_condition.notify_all()
                    return deferred_error
                batch = self._callback_dispatch_queue.popleft()
                expected_sequence = self._callback_dispatched_sequence + 1

            if batch.dispatch_sequence != expected_sequence:
                error = OrderManagerFatalError(
                    "callback commit sequence is discontinuous: "
                    f"expected={expected_sequence} actual={batch.dispatch_sequence}"
                )
                self._latch_fatal(
                    str(error),
                    cid=batch.order.client_order_id,
                    callback="callback_dispatch_sequence",
                    reconciliation_required=True,
                )
                return self._fail_callback_dispatch(batch, error) or deferred_error

            try:
                self._invoke_callback_batch(batch)
            except BaseException as error:
                # Normal callback exceptions are already latched by
                # ``_invoke_callback``.  BaseException subclasses still need
                # a durable latch so waiters cannot remain stranded.
                if not isinstance(error, Exception):
                    self._latch_fatal(
                        "callback_dispatch_aborted:"
                        f"{type(error).__name__}:{error}",
                        cid=batch.order.client_order_id,
                        callback="callback_dispatch",
                        reconciliation_required=True,
                    )
                return self._fail_callback_dispatch(batch, error) or deferred_error

            with self._callback_dispatch_condition:
                batch.dispatch_done = True
                self._callback_dispatched_sequence = batch.dispatch_sequence
                self._callback_dispatch_condition.notify_all()

    def _dispatch_callbacks(self, batch: _CallbackBatch) -> None:
        """Wait for an enqueued batch, draining the queue when elected.

        A callback may re-enter OrderManager and commit another callback batch.
        The re-entrant call cannot synchronously wait for itself without either
        nesting callbacks or deadlocking the sole consumer, so it returns after
        enqueue; the outer drainer delivers that batch immediately after the
        current callback returns.  All non-drainer callers wait for their own
        batch's success or failure.
        """

        if batch.dispatch_sequence <= 0:
            raise AssertionError("callback batch was not committed under the state lock")

        thread_id = get_ident()
        should_drain = False
        with self._callback_dispatch_condition:
            if batch.dispatch_done:
                error = batch.dispatch_error
            elif self._callback_dispatch_drainer_thread_id == thread_id:
                batch.dispatch_wait_deferred = True
                return
            else:
                if self._callback_dispatch_drainer_thread_id is None:
                    self._callback_dispatch_drainer_thread_id = thread_id
                    should_drain = True
                if not should_drain:
                    while not batch.dispatch_done:
                        self._callback_dispatch_condition.wait()
                    error = batch.dispatch_error

        deferred_error = self._drain_callback_batches() if should_drain else None
        if should_drain:
            with self._callback_dispatch_condition:
                error = batch.dispatch_error
        if error is not None:
            raise error
        if deferred_error is not None:
            raise deferred_error

    def _validate_or_bind_active_identity_locked(
        self,
        order: Order,
        *,
        cid: str,
        oid: int,
    ) -> None:
        if cid != order.client_order_id:
            reason = self._mark_reconciliation_required_locked(
                order.client_order_id,
                f"active client order ID mismatch: event={cid!r} ledger={order.client_order_id!r}",
            )
            raise OrderReconciliationRequired(reason)
        if order.order_id:
            if oid != order.order_id:
                reason = self._mark_reconciliation_required_locked(
                    cid,
                    f"active exchange order ID mismatch: event={oid} ledger={order.order_id}",
                )
                raise OrderReconciliationRequired(reason)
            return
        if oid <= 0:
            reason = self._mark_reconciliation_required_locked(
                cid,
                "active order has no positive exchange order ID in update",
            )
            raise OrderReconciliationRequired(reason)
        if order.state != OrderState.PENDING_NEW:
            reason = self._mark_reconciliation_required_locked(
                cid,
                "only a pending-new order may bind its first positive "
                f"exchange order ID; state={order.state.name}",
            )
            raise OrderReconciliationRequired(reason)
        active_cid = self._oid_map.get(oid)
        terminal_cid = self._tombstone_oid_map.get(oid)
        if active_cid not in (None, cid) or terminal_cid not in (None, cid):
            reason = self._mark_reconciliation_required_locked(
                cid,
                f"exchange order ID collision while binding: oid={oid} "
                f"active_cid={active_cid!r} terminal_cid={terminal_cid!r}",
            )
            raise OrderReconciliationRequired(reason)
        order.order_id = oid
        self._oid_map[oid] = cid

    def _validate_terminal_identity_locked(
        self,
        *,
        cid: str,
        oid: int,
        expected_cid: str,
        expected_oid: int,
    ) -> None:
        if cid != expected_cid or oid != expected_oid:
            reason = self._mark_reconciliation_required_locked(
                expected_cid,
                "terminal order identity mismatch: "
                f"event_cid={cid!r} ledger_cid={expected_cid!r} "
                f"event_oid={oid} ledger_oid={expected_oid}",
            )
            raise OrderReconciliationRequired(reason)

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
        cid = str(event.get("c", "") or "")
        status = str(event.get("X", "") or "").strip().upper()
        if status not in _KNOWN_EXCHANGE_STATUSES:
            reason = (
                "reconciliation_required:invalid_exchange_order_update:"
                f"unsupported status {status!r}"
            )
            self._latch_fatal(
                reason,
                cid=cid,
                reconciliation_required=True,
            )
            logger.critical(
                "ORDER_UPDATE_FATAL cid=%s reason=unsupported_status status=%r",
                cid,
                status,
            )
            raise OrderReconciliationRequired(reason)
        try:
            oid = int(event.get("i", 0) or 0)
            visibility_ts_ns = self._visibility_ts_ns(event)
            exchange_ts_ns = self._exchange_ts_ns(event)
        except (TypeError, ValueError, OverflowError) as exc:
            reason = (
                "reconciliation_required:invalid_exchange_order_update:"
                f"identity/timestamp parse failed: {exc}"
            )
            self._latch_fatal(
                reason,
                cid=cid,
                reconciliation_required=True,
            )
            logger.critical(
                "ORDER_UPDATE_FATAL cid=%s status=%s reason=%s",
                cid,
                status,
                exc,
            )
            raise OrderReconciliationRequired(reason) from exc
        reconciled_unknown_prefix = bool(event.get("_submit_ack_reconciled", False))
        batch = None

        with self._lock:
            self._raise_if_fatal_locked()
            # Resolve both identity axes, then require exact agreement.
            order = self._orders.get(cid)
            oid_cid = self._oid_map.get(oid) if oid else None
            if order is None and oid_cid is not None:
                order = self._orders.get(oid_cid)
            if not order:
                history_order = self._history.get(cid)
                if history_order is not None:
                    self._validate_terminal_identity_locked(
                        cid=cid,
                        oid=oid,
                        expected_cid=history_order.client_order_id,
                        expected_oid=history_order.order_id,
                    )
                    self._validate_ws_order_economics_locked(
                        history_order,
                        event,
                        context="terminal order",
                    )
                    batch = self._process_history_order_update(
                        history_order,
                        event,
                        status=status,
                        visibility_ts_ns=visibility_ts_ns,
                    )
                else:
                    tombstone = self._tombstones.get(cid)
                    tombstone_oid_cid = self._tombstone_oid_map.get(oid) if oid else None
                    if tombstone is not None:
                        self._validate_terminal_identity_locked(
                            cid=cid,
                            oid=oid,
                            expected_cid=tombstone.client_order_id,
                            expected_oid=tombstone.order_id,
                        )
                        self._validate_ws_order_economics_locked(
                            tombstone,
                            event,
                            context="evicted terminal order",
                        )
                        batch = self._process_tombstone_order_update(
                            tombstone,
                            event,
                            status=status,
                            oid=oid,
                            visibility_ts_ns=visibility_ts_ns,
                        )
                    elif tombstone_oid_cid is not None:
                        reason = self._mark_reconciliation_required_locked(
                            cid or tombstone_oid_cid,
                            "event exchange order ID belongs to a different "
                            f"terminal CID: event_cid={cid!r} "
                            f"terminal_cid={tombstone_oid_cid!r} oid={oid}",
                        )
                        raise OrderReconciliationRequired(reason)
                    else:
                        logger.warning(
                            "ORDER_UPDATE for unknown order: cid=%s oid=%s",
                            cid,
                            oid,
                        )
                        # Only adopt strategy-owned IDs.  The returned callback
                        # batch is dispatched after this ``with`` block.
                        if cid.startswith("mm_"):
                            batch = self._adopt_orphan(
                                cid,
                                oid,
                                event,
                                status=status,
                                visibility_ts_ns=visibility_ts_ns,
                                exchange_ts_ns=exchange_ts_ns,
                            )
            else:
                self._validate_ws_order_economics_locked(
                    order,
                    event,
                    context="active order",
                )
                self._validate_or_bind_active_identity_locked(
                    order,
                    cid=cid,
                    oid=oid,
                )
                batch = self._process_active_order_update(
                    order,
                    event,
                    status=status,
                    visibility_ts_ns=visibility_ts_ns,
                    exchange_ts_ns=exchange_ts_ns,
                    reconciled_unknown_prefix=reconciled_unknown_prefix,
                )
            if batch is not None:
                self._queue_callback_batch_locked(batch)

        if batch is not None:
            self._dispatch_callbacks(batch)

    def _process_active_order_update(
        self,
        order: Order,
        event: dict,
        *,
        status: str,
        visibility_ts_ns: int,
        exchange_ts_ns: int,
        reconciled_unknown_prefix: bool,
    ) -> _CallbackBatch | None:
        """Process one active order. Caller must hold ``self._lock``."""

        # A private user-stream callback does not carry the internal REST
        # reconciliation marker. Preserve an already-observed unknown prefix.
        reconciled_unknown_prefix = bool(
            reconciled_unknown_prefix
            or (
                order.lifecycle is not None
                and order.lifecycle.phase == OrderLifecyclePhase.SUBMITTED
                and order.lifecycle.submit_ack_unknown_observed
            )
        )

        prev_state = order.state
        lifecycle_event_type = ""
        terminal_reason = ""
        fatal_after_dispatch_reason = ""
        prepared = None

        if status in _STATUSES_WITH_CUMULATIVE_FILL:
            try:
                prepared = self._prepare_fill(order, event)
            except _FillEvidenceGap as exc:
                reason = self._mark_reconciliation_required_locked(
                    order.client_order_id,
                    str(exc),
                )
                raise OrderReconciliationRequired(reason) from exc
            except ValueError as exc:
                reason = self._mark_reconciliation_required_locked(
                    order.client_order_id,
                    f"invalid exchange order update ({status}): {exc}",
                )
                raise OrderReconciliationRequired(reason) from exc
            if prepared.stale:
                logger.warning(
                    "ORDER_UPDATE_STALE_CUMULATIVE_FILL cid=%s status=%s event_z=%r ledger_z=%.17g",
                    order.client_order_id,
                    status,
                    event.get("z"),
                    order.filled_qty,
                )
                return None

        if status == "NEW":
            order.state = (
                OrderState.PARTIALLY_FILLED
                if order.filled_qty > PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC
                else OrderState.OPEN
            )
            if order.lifecycle is not None:
                if order.lifecycle.phase == OrderLifecyclePhase.CANCEL_PENDING:
                    order.lifecycle.cancel_rejected(
                        visibility_ts_ns,
                        exchange_ts_ns=exchange_ts_ns,
                    )
                    order.state = (
                        OrderState.PARTIALLY_FILLED
                        if order.lifecycle.phase == OrderLifecyclePhase.PARTIALLY_FILLED
                        else OrderState.OPEN
                    )
                    lifecycle_event_type = "cancel_rejected"
                elif order.lifecycle.phase == OrderLifecyclePhase.SUBMITTED:
                    if reconciled_unknown_prefix:
                        order.lifecycle.activate_with_unknown_prefix(visibility_ts_ns)
                        lifecycle_event_type = "activate_unknown_prefix"
                    else:
                        order.lifecycle.activate(
                            visibility_ts_ns,
                            exchange_ts_ns=exchange_ts_ns,
                        )
                        lifecycle_event_type = "activate"

        elif status == "PARTIALLY_FILLED":
            assert prepared is not None
            if prepared.is_new_fill:
                self._observe_prepared_fill(
                    order,
                    prepared,
                    visibility_ts_ns=visibility_ts_ns,
                    exchange_ts_ns=exchange_ts_ns,
                    reconciled_unknown_prefix=reconciled_unknown_prefix,
                )
                self._commit_fill(order, event, prepared)
                lifecycle_event_type = "full_fill" if prepared.complete else "partial_fill"
            if prepared.complete:
                # Cumulative quantity is authoritative even if the status is
                # lagging. Keeping a zero-remainder order active is unsafe.
                order.state = OrderState.FILLED
                if order.lifecycle is not None and not prepared.is_new_fill:
                    order.lifecycle.exchange_terminal(
                        visibility_ts_ns,
                        exchange_ts_ns=exchange_ts_ns,
                        reason="full_fill",
                    )
                self._move_to_history(order.client_order_id)
                terminal_reason = "full_fill"
                lifecycle_event_type = lifecycle_event_type or "full_fill"
                logger.warning(
                    "ORDER_STATUS_LAGGED_FULL_CUMULATIVE_FILL cid=%s",
                    order.client_order_id,
                )
            else:
                order.state = (
                    OrderState.PENDING_CANCEL
                    if prev_state == OrderState.PENDING_CANCEL
                    else OrderState.PARTIALLY_FILLED
                )

        elif status == "FILLED":
            assert prepared is not None
            if not prepared.complete:
                # Preserve the valid delta, but reject the terminal claim. The
                # order stays in the active risk set until quantity reaches q.
                if prepared.is_new_fill:
                    self._observe_prepared_fill(
                        order,
                        prepared,
                        visibility_ts_ns=visibility_ts_ns,
                        exchange_ts_ns=exchange_ts_ns,
                        reconciled_unknown_prefix=reconciled_unknown_prefix,
                    )
                    self._commit_fill(order, event, prepared)
                    lifecycle_event_type = "partial_fill"
                order.state = (
                    OrderState.PENDING_CANCEL
                    if prev_state == OrderState.PENDING_CANCEL
                    else (
                        OrderState.PARTIALLY_FILLED
                        if order.filled_qty > PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC
                        else OrderState.OPEN
                    )
                )
                logger.error(
                    "ORDER_FILLED_QUANTITY_INCOMPLETE cid=%s filled=%.17g qty=%.17g",
                    order.client_order_id,
                    order.filled_qty,
                    order.quantity,
                )
                fatal_after_dispatch_reason = self._mark_reconciliation_required_locked(
                    order.client_order_id,
                    "exchange FILLED status has incomplete cumulative quantity",
                )
            else:
                if prepared.is_new_fill:
                    self._observe_prepared_fill(
                        order,
                        prepared,
                        visibility_ts_ns=visibility_ts_ns,
                        exchange_ts_ns=exchange_ts_ns,
                        reconciled_unknown_prefix=reconciled_unknown_prefix,
                    )
                    self._commit_fill(order, event, prepared)
                elif order.lifecycle is not None:
                    order.lifecycle.exchange_terminal(
                        visibility_ts_ns,
                        exchange_ts_ns=exchange_ts_ns,
                        reason="full_fill",
                    )
                order.state = OrderState.FILLED
                self._move_to_history(order.client_order_id)
                terminal_reason = "full_fill"
                lifecycle_event_type = "full_fill"

        else:
            assert prepared is not None
            if prepared.is_new_fill:
                self._observe_prepared_fill(
                    order,
                    prepared,
                    visibility_ts_ns=visibility_ts_ns,
                    exchange_ts_ns=exchange_ts_ns,
                    reconciled_unknown_prefix=reconciled_unknown_prefix,
                )
                self._commit_fill(order, event, prepared)
            fill_completed = prepared.complete
            reason = {
                "CANCELED": "cancel_ack",
                "EXPIRED": "expired",
                "REJECTED": "rejected",
            }[status]
            order.state = (
                OrderState.FILLED
                if fill_completed
                else {
                    "CANCELED": OrderState.CANCELED,
                    "EXPIRED": OrderState.EXPIRED,
                    "REJECTED": OrderState.REJECTED,
                }[status]
            )
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
                    reason=reason,
                )
            elif order.lifecycle is not None and fill_completed and not prepared.is_new_fill:
                order.lifecycle.exchange_terminal(
                    visibility_ts_ns,
                    exchange_ts_ns=exchange_ts_ns,
                    reason="full_fill",
                )
            self._move_to_history(order.client_order_id)
            terminal_reason = "full_fill" if fill_completed else reason
            lifecycle_event_type = terminal_reason

        order.update_time = visibility_ts_ns / 1_000_000_000.0
        logger.info(
            "ORDER_UPDATE %s %s→%s filled=%s/%s avg_px=%.1f",
            order.client_order_id,
            prev_state.name,
            order.state.name,
            order.filled_qty,
            order.quantity,
            order.avg_fill_price,
        )
        return _CallbackBatch(
            order=order,
            event=event,
            emit_fill=bool(prepared and prepared.is_new_fill),
            emit_cancel=status == "CANCELED",
            terminal_reason=terminal_reason,
            lifecycle_event_type=lifecycle_event_type,
            fatal_after_dispatch_reason=fatal_after_dispatch_reason,
        )

    def _process_history_order_update(
        self,
        order: Order,
        event: dict,
        *,
        status: str,
        visibility_ts_ns: int,
    ) -> _CallbackBatch | None:
        """Apply a missing post-terminal fill delta exactly once."""

        if status not in _STATUSES_WITH_CUMULATIVE_FILL:
            logger.debug(
                "ORDER_UPDATE duplicate for terminal order: cid=%s status=%s",
                order.client_order_id,
                status,
            )
            return None
        try:
            prepared = self._prepare_fill(order, event)
        except _FillEvidenceGap as exc:
            reason = self._mark_reconciliation_required_locked(
                order.client_order_id,
                str(exc),
            )
            raise OrderReconciliationRequired(reason) from exc
        except ValueError as exc:
            reason = self._mark_reconciliation_required_locked(
                order.client_order_id,
                f"invalid terminal order update ({status}): {exc}",
            )
            raise OrderReconciliationRequired(reason) from exc
        if prepared.stale or not prepared.is_new_fill:
            logger.debug(
                "ORDER_UPDATE duplicate/stale for terminal order: cid=%s status=%s",
                order.client_order_id,
                status,
            )
            return None

        previous_state = order.state
        self._commit_fill(order, event, prepared)
        if prepared.complete:
            order.state = OrderState.FILLED
        order.update_time = max(
            order.update_time,
            visibility_ts_ns / 1_000_000_000.0,
        )
        self._history.move_to_end(order.client_order_id)
        self._record_tombstone_locked(order)
        fatal_reason = self._mark_reconciliation_required_locked(
            order.client_order_id,
            "positive cumulative fill arrived after exchange terminal",
        )
        logger.warning(
            "ORDER_HISTORY_FILL_CORRECTION cid=%s status=%s state=%s→%s "
            "delta=%.17g cumulative=%.17g quantity=%.17g",
            order.client_order_id,
            status,
            previous_state.name,
            order.state.name,
            prepared.fill_qty,
            order.filled_qty,
            order.quantity,
        )
        if status == "FILLED" and not prepared.complete:
            logger.error(
                "ORDER_HISTORY_FILLED_QUANTITY_INCOMPLETE cid=%s filled=%.17g qty=%.17g",
                order.client_order_id,
                order.filled_qty,
                order.quantity,
            )
        # Do not emit a second terminal/cancel lifecycle callback.  The only
        # new economic fact is the bounded fill delta.
        return _CallbackBatch(
            order=order,
            event=event,
            emit_fill=True,
            fatal_after_dispatch_reason=fatal_reason,
        )

    def _move_to_history(self, cid: str):
        """Move terminal order from active to history."""
        order = self._orders.pop(cid, None)
        if order:
            self._history[cid] = order
            if self._oid_map.get(order.order_id) == cid:
                del self._oid_map[order.order_id]
            self._record_tombstone_locked(order)
            # trim history
            while len(self._history) > self._max_history:
                self._history.popitem(last=False)

    @staticmethod
    def _terminal_identity(order: Order) -> str:
        return {
            OrderState.FILLED: "full_fill",
            OrderState.CANCELED: "canceled",
            OrderState.EXPIRED: "expired",
            OrderState.REJECTED: "rejected",
        }.get(order.state, order.state.name.lower())

    def _record_tombstone_locked(self, order: Order) -> None:
        """Persist terminal identity/cursor independently of rich history eviction."""

        existing = self._tombstones.get(order.client_order_id)
        if existing is not None and (
            existing.order_id != order.order_id
            or existing.symbol != order.symbol
            or existing.side != order.side
            or existing.quantity != order.quantity
        ):
            reason = self._mark_reconciliation_required_locked(
                order.client_order_id,
                "terminal tombstone identity changed",
            )
            raise OrderReconciliationRequired(reason)
        if order.order_id:
            mapped_cid = self._tombstone_oid_map.get(order.order_id)
            if mapped_cid not in (None, order.client_order_id):
                reason = self._mark_reconciliation_required_locked(
                    order.client_order_id,
                    "terminal tombstone exchange order ID collision",
                )
                raise OrderReconciliationRequired(reason)
            self._tombstone_oid_map[order.order_id] = order.client_order_id
        self._tombstones[order.client_order_id] = _OrderTombstone(
            client_order_id=order.client_order_id,
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            price=order.price,
            quantity=order.quantity,
            filled_qty=order.filled_qty,
            avg_fill_price=order.avg_fill_price,
            terminal_state=order.state,
            terminal_reason=self._terminal_identity(order),
            max_trade_id=max(self._trade_ids_by_oid.get(order.order_id, {0})),
        )

    @staticmethod
    def _order_from_tombstone(tombstone: _OrderTombstone) -> Order:
        return Order(
            client_order_id=tombstone.client_order_id,
            order_id=tombstone.order_id,
            symbol=tombstone.symbol,
            side=tombstone.side,
            price=tombstone.price,
            quantity=tombstone.quantity,
            filled_qty=tombstone.filled_qty,
            avg_fill_price=tombstone.avg_fill_price,
            state=tombstone.terminal_state,
        )

    def _process_tombstone_order_update(
        self,
        tombstone: _OrderTombstone,
        event: dict,
        *,
        status: str,
        oid: int,
        visibility_ts_ns: int,
    ) -> _CallbackBatch | None:
        """Deduplicate an evicted terminal order without orphan re-adoption."""

        if oid != tombstone.order_id:
            reason = self._mark_reconciliation_required_locked(
                tombstone.client_order_id,
                "evicted terminal exchange order ID mismatch: "
                f"event={oid} tombstone={tombstone.order_id}",
            )
            raise OrderReconciliationRequired(reason)
        if status not in _STATUSES_WITH_CUMULATIVE_FILL:
            logger.debug(
                "ORDER_UPDATE duplicate for evicted terminal order: cid=%s status=%s",
                tombstone.client_order_id,
                status,
            )
            return None
        order = self._order_from_tombstone(tombstone)
        try:
            prepared = self._prepare_fill(order, event)
        except _FillEvidenceGap as exc:
            reason = self._mark_reconciliation_required_locked(
                tombstone.client_order_id,
                str(exc),
            )
            raise OrderReconciliationRequired(reason) from exc
        except ValueError as exc:
            reason = self._mark_reconciliation_required_locked(
                tombstone.client_order_id,
                f"invalid evicted terminal update ({status}): {exc}",
            )
            raise OrderReconciliationRequired(reason) from exc
        if prepared.stale or not prepared.is_new_fill:
            logger.debug(
                "ORDER_UPDATE duplicate/stale for evicted terminal order: cid=%s status=%s",
                tombstone.client_order_id,
                status,
            )
            return None

        self._commit_fill(order, event, prepared)
        if prepared.complete:
            order.state = OrderState.FILLED
        tombstone.filled_qty = order.filled_qty
        tombstone.avg_fill_price = order.avg_fill_price
        tombstone.terminal_state = order.state
        tombstone.terminal_reason = self._terminal_identity(order)
        tombstone.max_trade_id = max(
            tombstone.max_trade_id,
            prepared.trade_id,
        )
        reason = self._mark_reconciliation_required_locked(
            tombstone.client_order_id,
            "positive cumulative fill arrived after rich terminal history eviction",
        )
        logger.warning(
            "ORDER_TOMBSTONE_FILL_CORRECTION cid=%s delta=%.17g cumulative=%.17g",
            tombstone.client_order_id,
            prepared.fill_qty,
            prepared.new_cum_qty,
        )
        return _CallbackBatch(
            order=order,
            event=event,
            emit_fill=True,
            fatal_after_dispatch_reason=reason,
        )

    def _adopt_orphan(
        self,
        cid: str,
        oid: int,
        event: dict,
        *,
        status: str,
        visibility_ts_ns: int,
        exchange_ts_ns: int,
    ) -> _CallbackBatch | None:
        """Adopt an orphan order from a previous session so fills are tracked.

        IMPORTANT: Caller must already hold self._lock.
        No external callback may run from this method.
        """
        if oid <= 0:
            reason = self._mark_reconciliation_required_locked(
                cid,
                "orphan update has no positive exchange order ID",
            )
            raise OrderReconciliationRequired(reason)
        try:
            symbol = self._canonical_symbol(
                event.get("s"),
                label="orphan event symbol",
            )
        except ValueError as exc:
            reason = self._mark_reconciliation_required_locked(
                cid,
                f"invalid orphan identity: {exc}",
            )
            raise OrderReconciliationRequired(reason) from exc
        if symbol not in self._allowed_symbols:
            reason = self._mark_reconciliation_required_locked(
                cid,
                "orphan event symbol is outside the configured order-manager scope: "
                f"event={symbol!r} allowed={sorted(self._allowed_symbols)!r}",
            )
            raise OrderReconciliationRequired(reason)
        active_cid = self._oid_map.get(oid)
        terminal_cid = self._tombstone_oid_map.get(oid)
        if active_cid is not None or terminal_cid is not None:
            reason = self._mark_reconciliation_required_locked(
                cid,
                f"orphan exchange order ID collision: oid={oid} "
                f"active_cid={active_cid!r} terminal_cid={terminal_cid!r}",
            )
            raise OrderReconciliationRequired(reason)
        try:
            side = self._event_side(event.get("S"))
            order_type = self._event_order_type(event.get("o"))
            price_raw = event.get("p")
            if order_type in _MARKET_ORDER_TYPES:
                price = (
                    0.0
                    if price_raw in (None, "")
                    else self._finite_float(
                        price_raw,
                        label="orphan market order price",
                        nonnegative=True,
                    )
                )
            else:
                price = self._positive_float(
                    price_raw,
                    label="orphan limit order price",
                )
            qty = self._positive_float(
                event.get("q"),
                label="orphan order quantity",
            )
        except ValueError as exc:
            reason = self._mark_reconciliation_required_locked(
                cid,
                f"invalid orphan identity: {exc}",
            )
            raise OrderReconciliationRequired(reason) from exc
        order = Order(
            client_order_id=cid,
            symbol=symbol,
            side=side,
            price=price,
            quantity=qty,
            state=OrderState.OPEN,
            order_id=oid,
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
        if oid:
            self._oid_map[oid] = cid
        logger.info(f"ADOPTED orphan order {cid} {side.value} {qty}@{price}")
        batch = self._process_active_order_update(
            order,
            event,
            status=status,
            visibility_ts_ns=visibility_ts_ns,
            exchange_ts_ns=exchange_ts_ns,
            reconciled_unknown_prefix=False,
        )
        if batch is None:
            # The adoption itself is still authoritative risk information even
            # when the attached fill claim is malformed.
            return _CallbackBatch(
                order=order,
                event=event,
                lifecycle_event_type="activate_unknown_prefix",
            )
        if not batch.lifecycle_event_type:
            batch.lifecycle_event_type = "activate_unknown_prefix"
        return batch

    def reconcile_exchange_trade(
        self,
        *,
        exchange_order_id: int,
        trade_id: int,
        symbol: str,
        side: Side | str,
        quantity: float,
        price: float,
        commission: float,
        commission_asset: str,
        cumulative_fill: float,
        trade_time_ms: int = 0,
        local_receive_ts_ns: int = 0,
    ) -> bool:
        """Apply one exact account-trade row by OID/trade-ID, idempotently.

        This API intentionally has no client-order-ID fallback: accountTrades
        rows without a locally known exchange order ID require an explicit
        order query and may never be guessed into the orphan path.
        """

        try:
            oid = int(exchange_order_id)
            parsed_trade_id = int(trade_id)
            if oid <= 0:
                raise ValueError("exchange order ID must be positive")
            if parsed_trade_id <= 0:
                raise ValueError("trade ID must be positive")
            parsed_side = side if isinstance(side, Side) else Side(str(side).upper())
            parsed_quantity = self._positive_float(
                quantity,
                label="exchange trade quantity",
            )
            parsed_price = self._positive_float(
                price,
                label="exchange trade price",
            )
            parsed_commission = self._finite_float(
                commission,
                label="exchange trade commission",
                nonnegative=False,
            )
            parsed_commission_asset = str(commission_asset or "").upper()
            parsed_cumulative = self._finite_float(
                cumulative_fill,
                label="exchange trade cumulative fill",
                nonnegative=True,
            )
            visibility_ts_ns = int(local_receive_ts_ns or time.time_ns())
            if visibility_ts_ns <= 0:
                raise ValueError("local receive timestamp must be positive")
            parsed_trade_time_ms = int(trade_time_ms or 0)
            if parsed_trade_time_ms < 0:
                raise ValueError("exchange trade timestamp must be non-negative")
        except (TypeError, ValueError, OverflowError) as exc:
            reason = f"reconciliation_required:invalid_exchange_trade:{exc}"
            self._latch_fatal(
                reason,
                reconciliation_required=True,
            )
            raise OrderReconciliationRequired(reason) from exc

        batch = None
        with self._lock:
            self._raise_if_fatal_locked()
            active_cid = self._oid_map.get(oid)
            terminal_cid = self._tombstone_oid_map.get(oid)
            if active_cid is not None and terminal_cid is not None:
                reason = self._mark_reconciliation_required_locked(
                    active_cid,
                    f"exchange order ID is both active and terminal: oid={oid}",
                )
                raise OrderReconciliationRequired(reason)
            cid = active_cid or terminal_cid
            if cid is None:
                reason = self._mark_reconciliation_required_locked(
                    "",
                    f"account trade references unknown exchange order ID; "
                    f"individual order query required: oid={oid}",
                )
                raise OrderReconciliationRequired(reason)

            order = self._orders.get(cid) or self._history.get(cid)
            tombstone = self._tombstones.get(cid)
            identity = order or (
                self._order_from_tombstone(tombstone) if tombstone is not None else None
            )
            if identity is None:
                reason = self._mark_reconciliation_required_locked(
                    cid,
                    f"exchange order ID resolved without order identity: oid={oid}",
                )
                raise OrderReconciliationRequired(reason)
            if identity.symbol != str(symbol) or identity.side != parsed_side:
                reason = self._mark_reconciliation_required_locked(
                    cid,
                    "account trade identity mismatch: "
                    f"symbol={symbol!r}/{identity.symbol!r} "
                    f"side={parsed_side.value}/{identity.side.value}",
                )
                raise OrderReconciliationRequired(reason)

            seen_trade_ids = self._trade_ids_by_oid.setdefault(oid, set())
            current_cumulative = float(identity.filled_qty)
            candidate_trade_identity = _TradeIdentity(
                exchange_order_id=oid,
                symbol=identity.symbol,
                side=parsed_side,
                quantity=parsed_quantity,
                price=parsed_price,
                commission=parsed_commission,
                commission_asset=parsed_commission_asset,
                trade_time_ms=parsed_trade_time_ms,
                cumulative_fill=parsed_cumulative,
            )
            trade_key = (identity.symbol, parsed_trade_id)
            known_trade_identity = self._trade_identity_by_symbol_id.get(trade_key)
            if (
                known_trade_identity is not None
                and known_trade_identity != candidate_trade_identity
            ):
                reason = self._mark_reconciliation_required_locked(
                    cid,
                    "previously applied trade ID changed exact economic identity: "
                    f"trade_id={parsed_trade_id} "
                    f"event={candidate_trade_identity!r} "
                    f"ledger={known_trade_identity!r}",
                )
                raise OrderReconciliationRequired(reason)
            if parsed_trade_id in seen_trade_ids:
                if known_trade_identity is None:
                    reason = self._mark_reconciliation_required_locked(
                        cid,
                        "seen trade ID has no exact process-lifetime identity: "
                        f"trade_id={parsed_trade_id}",
                    )
                    raise OrderReconciliationRequired(reason)
                return False
            if parsed_cumulative <= current_cumulative + QUANTITY_INCREASE_ABS_TOLERANCE_BTC:
                # The trade is already represented by a newer cumulative
                # cursor. Remember its ID without delivering inventory twice.
                seen_trade_ids.add(parsed_trade_id)
                self._trade_identity_by_symbol_id[trade_key] = candidate_trade_identity
                if tombstone is not None:
                    tombstone.max_trade_id = max(
                        tombstone.max_trade_id,
                        parsed_trade_id,
                    )
                return False
            delta = parsed_cumulative - current_cumulative
            if abs(delta - parsed_quantity) > QUANTITY_INCREASE_ABS_TOLERANCE_BTC:
                reason = self._mark_reconciliation_required_locked(
                    cid,
                    "account trade does not bridge cumulative cursor exactly: "
                    f"delta={delta:.17g} trade_qty={parsed_quantity:.17g}",
                )
                raise OrderReconciliationRequired(reason)

            event = {
                "s": str(symbol),
                "c": cid,
                "S": parsed_side.value,
                "i": oid,
                "z": parsed_cumulative,
                "l": delta,
                "L": parsed_price,
                "n": parsed_commission,
                "N": parsed_commission_asset,
                "t": parsed_trade_id,
                "T": parsed_trade_time_ms,
                "_local_receive_ts_ns": visibility_ts_ns,
                "_exchange_trade_reconciled": True,
            }
            complete = (
                float(identity.quantity) - parsed_cumulative <= TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
            )
            status = "FILLED" if complete else "PARTIALLY_FILLED"
            event["X"] = status
            if active_cid is not None:
                batch = self._process_active_order_update(
                    identity,
                    event,
                    status=status,
                    visibility_ts_ns=visibility_ts_ns,
                    exchange_ts_ns=max(0, parsed_trade_time_ms) * 1_000_000,
                    reconciled_unknown_prefix=True,
                )
            elif order is not None:
                batch = self._process_history_order_update(
                    order,
                    event,
                    status=status,
                    visibility_ts_ns=visibility_ts_ns,
                )
            else:
                assert tombstone is not None
                batch = self._process_tombstone_order_update(
                    tombstone,
                    event,
                    status=status,
                    oid=oid,
                    visibility_ts_ns=visibility_ts_ns,
                )
            if batch is not None:
                self._queue_callback_batch_locked(batch)

        if batch is None:
            return False
        self._dispatch_callbacks(batch)
        return batch.emit_fill

    # ── queries ──

    def get_active_orders(self) -> list[Order]:
        with self._lock:
            return list(self._orders.values())

    def get_active_by_side(self, side: Side) -> list[Order]:
        with self._lock:
            return [o for o in self._orders.values() if o.side == side]

    def get_bid_orders(self) -> list[Order]:
        return self.get_active_by_side(Side.BUY)

    def get_ask_orders(self) -> list[Order]:
        return self.get_active_by_side(Side.SELL)

    def get_order(self, cid: str) -> Order | None:
        with self._lock:
            order = self._orders.get(cid) or self._history.get(cid)
            if order is not None:
                return order
            tombstone = self._tombstones.get(cid)
            if tombstone is None:
                return None
            # Detached proof object: rich lifecycle history may be evicted,
            # but terminal identity must never look like an unknown CID.
            return self._order_from_tombstone(tombstone)

    def terminal_identity(self, cid: str) -> dict[str, object] | None:
        """Return non-evicting terminal identity/cursor for ownership pruning."""

        with self._lock:
            tombstone = self._tombstones.get(cid)
            if tombstone is None:
                return None
            return {
                "client_order_id": tombstone.client_order_id,
                "exchange_order_id": tombstone.order_id,
                "symbol": tombstone.symbol,
                "side": tombstone.side.value,
                "price": tombstone.price,
                "quantity": tombstone.quantity,
                "cumulative_fill": tombstone.filled_qty,
                "average_fill_price": tombstone.avg_fill_price,
                "terminal_state": tombstone.terminal_state.name,
                "terminal_reason": tombstone.terminal_reason,
                "max_trade_id": tombstone.max_trade_id,
            }

    def tombstone_count(self) -> int:
        with self._lock:
            return len(self._tombstones)

    def lifecycle_snapshot(
        self,
        cid: str,
        *,
        now_ns: int | None = None,
    ) -> dict[str, object] | None:
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

    def get_stale_pending_cancel_orders(self, max_age: float) -> list[Order]:
        """Find cancel requests that did not converge through user stream."""
        now = time.time()
        with self._lock:
            return [
                o
                for o in self._orders.values()
                if o.state == OrderState.PENDING_CANCEL
                and now - (o.update_time or o.create_time) > max_age
            ]

    def get_stale_orders(self, max_age: float) -> list[Order]:
        """Find orders older than max_age seconds in PENDING_NEW state."""
        now = time.time()
        with self._lock:
            return [
                o
                for o in self._orders.values()
                if o.state == OrderState.PENDING_NEW and now - o.create_time > max_age
            ]

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
            self._raise_if_fatal_locked()
            o = self._orders.get(cid)
            if not o or o.state != OrderState.PENDING_CANCEL:
                return False
            prev_state = o.state
            if exchange_open:
                if exchange_oid:
                    self._validate_or_bind_active_identity_locked(
                        o,
                        cid=cid,
                        oid=int(exchange_oid),
                    )
                o.update_time = now_ns / 1_000_000_000.0
                if o.lifecycle is not None:
                    o.lifecycle.cancel_rejected(now_ns)
                    o.state = (
                        OrderState.PARTIALLY_FILLED
                        if o.lifecycle.phase == OrderLifecyclePhase.PARTIALLY_FILLED
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
        callback_batches: list[_CallbackBatch] = []
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
                    if self._on_lifecycle_event:
                        callback_batches.append(
                            self._queue_callback_batch_locked(
                                _CallbackBatch(
                                    order=order,
                                    event={
                                        "_local_receive_ts_ns": now_ns,
                                        "_reason": "local_shutdown_unknown_ack",
                                        "_point_identified": False,
                                    },
                                    lifecycle_event_type="submit_ack_unknown_censored",
                                )
                            )
                        )
                    continue
                order.state = OrderState.CANCELED
                if order.lifecycle is not None:
                    if not order.lifecycle.locally_censored:
                        order.lifecycle.local_shutdown_censor(
                            now_ns,
                            reason="local_shutdown_cancel",
                        )
                self._move_to_history(cid)
                if self._on_lifecycle_event or self._on_terminal:
                    callback_batches.append(
                        self._queue_callback_batch_locked(
                            _CallbackBatch(
                                order=order,
                                event={"_local_receive_ts_ns": now_ns},
                                lifecycle_event_type="local_shutdown_cancel",
                                terminal_reason="local_shutdown_cancel",
                            )
                        )
                    )
        for batch in callback_batches:
            self._dispatch_callbacks(batch)
