#!/usr/bin/env python3
"""Run the hash-bound, synthetic public NarrowGate replay demonstration."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

if sys.version_info < (3, 11):  # noqa: UP036 - explicit runtime-contract preflight
    raise RuntimeError("narrowgate replay-demo requires Python 3.11 or newer")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.replay import continuous_accounting as accounting  # noqa: E402
from models.replay import replay_state_checkpoint as checkpoint  # noqa: E402
from narrowgate import __version__ as PACKAGE_VERSION  # noqa: E402

ContinuousAccountingLedger = accounting.ContinuousAccountingLedger
ContinuousReplayState = checkpoint.ContinuousReplayState
ACCOUNTING_CONTRACT_VERSION = accounting.SCHEMA_VERSION
STATE_CONTRACT_VERSION = checkpoint.SCHEMA_VERSION

ENGINE_VERSION = "narrowgate_replay_demo_engine.v1"
CONTRACT_VERSION = "narrowgate_replay_demo_contract.v1"
TAPE_VERSION = "narrowgate.synthetic_replay_tape.v1"
TRACE_VERSION = "narrowgate_replay_demo_trace.v1"
SUMMARY_VERSION = "narrowgate_replay_demo_summary.v1"
GATE_VERSION = "narrowgate_replay_demo_gate.v1"
RECEIPT_VERSION = "narrowgate_replay_demo_receipt.v1"

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "replay_demo"
DEFAULT_CONTRACT = FIXTURE_ROOT / "contract.json"
DEFAULT_REFERENCE_DIR = FIXTURE_ROOT / "reference"
DEFAULT_OUTPUT_DIR = ROOT / "results" / "replay_demo"

NETWORK_ACCESS_ALLOWED = False
EXTERNAL_ORDER_SUBMISSION_ALLOWED = False
PRIVATE_EVIDENCE_READ_ALLOWED = False
RUNTIME_CLOCK_READ_ALLOWED = False

ZERO = Decimal("0")
DISPLAY_QUANTUM = Decimal("0.00000001")
CODE_SOURCE_PATHS = (
    "narrowgate/replay_demo.py",
    "models/__init__.py",
    "models/replay/__init__.py",
    "models/replay/continuous_accounting.py",
    "models/replay/continuous_calendar.py",
    "models/replay/replay_state_checkpoint.py",
    "data/__init__.py",
    "data/quality/__init__.py",
    "data/quality/calendar_gap_manifest.py",
    "data_paths.py",
)


class DemoError(RuntimeError):
    """Base error for fail-closed demo admission and replay."""


class DemoAdmissionError(DemoError):
    """Raised before replay when a public fixture identity is invalid."""


class DemoReplayError(DemoError):
    """Raised when the tiny reference engine sees unsupported mechanics."""


@dataclass(frozen=True)
class TopBook:
    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal

    @property
    def midpoint(self) -> Decimal:
        return (self.bid_price + self.ask_price) / Decimal("2")

    def queue_quantity(self, side: str, price: Decimal) -> Decimal:
        if side == "BUY" and price == self.bid_price:
            return self.bid_quantity
        if side == "SELL" and price == self.ask_price:
            return self.ask_quantity
        raise DemoReplayError("demo orders must join the displayed best price")


@dataclass
class SimulatedOrder:
    order_id: str
    side: str
    price: Decimal
    quantity: Decimal
    remaining_quantity: Decimal
    queue_ahead_quantity: Decimal
    submit_seq: int
    status: str = "active"
    filled_quantity: Decimal = ZERO
    matching_trade_opportunities: int = 0
    fill_events: int = 0


@dataclass(frozen=True)
class DemoRun:
    summary: dict[str, Any]
    receipt: dict[str, Any]
    summary_path: Path
    trace_path: Path
    receipt_path: Path
    reference_verified: bool


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_bytes(payload: object) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(payload: object) -> str:
    return sha256_bytes(canonical_bytes(payload))


def canonical_document_sha256(payload: Mapping[str, Any], hash_field: str) -> str:
    return canonical_sha256({key: value for key, value in payload.items() if key != hash_field})


def pretty_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def trace_jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(canonical_bytes(row) + b"\n" for row in rows)


def _write_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.partial")
    partial.write_bytes(payload)
    partial.replace(path)


def _decimal(
    value: object,
    *,
    field: str,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise DemoReplayError(f"{field} is not a decimal") from exc
    if not parsed.is_finite():
        raise DemoReplayError(f"{field} must be finite")
    if positive and parsed <= ZERO:
        raise DemoReplayError(f"{field} must be positive")
    if nonnegative and parsed < ZERO:
        raise DemoReplayError(f"{field} must be non-negative")
    return parsed


def _display(value: Decimal | float | int) -> str:
    parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    if abs(parsed) < DISPLAY_QUANTUM / Decimal("2"):
        parsed = ZERO
    return format(parsed.quantize(DISPLAY_QUANTUM), "f")


def _read_contract(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DemoAdmissionError(f"cannot read replay-demo contract: {path}") from exc
    if not isinstance(payload, dict):
        raise DemoAdmissionError("replay-demo contract must be a JSON object")
    if payload.get("schema_version") != CONTRACT_VERSION:
        raise DemoAdmissionError("unsupported replay-demo contract version")
    if payload.get("engine_version") != ENGINE_VERSION:
        raise DemoAdmissionError("contract requires a different replay-demo engine")
    if payload.get("fixture", {}).get("classification") != "synthetic_non_economic":
        raise DemoAdmissionError("contract must remain explicitly synthetic and non-economic")
    required_permissions = {
        "network_access": NETWORK_ACCESS_ALLOWED,
        "external_order_submission": EXTERNAL_ORDER_SUBMISSION_ALLOWED,
        "private_evidence_read": PRIVATE_EVIDENCE_READ_ALLOWED,
        "live_runtime_import": False,
        "runtime_clock_read": RUNTIME_CLOCK_READ_ALLOWED,
    }
    if payload.get("permissions") != required_permissions:
        raise DemoAdmissionError("contract permissions must lock network, live, and private access")
    gate_contract = payload.get("gate_contract", {})
    if any(
        gate_contract.get(field) is not False
        for field in (
            "economic_evidence_eligible",
            "live_action_eligible",
            "promotion_eligible",
        )
    ):
        raise DemoAdmissionError(
            "demo contract cannot grant economic, action, or promotion authority"
        )
    return payload


def _resolve_tape(contract_path: Path, contract: Mapping[str, Any]) -> Path:
    tape_name = contract.get("tape", {}).get("path")
    if not isinstance(tape_name, str) or not tape_name or Path(tape_name).is_absolute():
        raise DemoAdmissionError("contract tape path must be a non-empty relative path")
    fixture_root = contract_path.resolve().parent
    tape_path = (fixture_root / tape_name).resolve()
    try:
        tape_path.relative_to(fixture_root)
    except ValueError as exc:
        raise DemoAdmissionError("contract tape path escapes its public fixture directory") from exc
    if not tape_path.is_file():
        raise DemoAdmissionError("contract tape is missing")
    observed = sha256_file(tape_path)
    expected = contract.get("tape", {}).get("sha256")
    if observed != expected:
        raise DemoAdmissionError(
            f"synthetic tape SHA256 mismatch: expected={expected} observed={observed}"
        )
    return tape_path


def _read_tape(path: Path, contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            raise DemoAdmissionError(f"blank tape record at line {line_number}")
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise DemoAdmissionError(f"invalid JSON tape record at line {line_number}") from exc
        if not isinstance(event, dict):
            raise DemoAdmissionError(f"tape record {line_number} must be an object")
        events.append(event)
    if not events or events[0].get("event") != "metadata":
        raise DemoAdmissionError("synthetic tape must begin with metadata")
    metadata = events[0]
    if metadata.get("schema_version") != TAPE_VERSION:
        raise DemoAdmissionError("unsupported synthetic tape schema")
    if metadata.get("schema_version") != contract.get("tape", {}).get("schema_version"):
        raise DemoAdmissionError("tape schema does not match the contract")
    if metadata.get("classification") != "synthetic_non_economic":
        raise DemoAdmissionError("tape lost its synthetic non-economic classification")
    if metadata.get("symbol") != contract.get("market", {}).get("symbol"):
        raise DemoAdmissionError("tape symbol does not match the contract")

    previous_ts = -1
    supported = {"metadata", "book", "submit", "trade", "cancel", "mark"}
    for expected_seq, event in enumerate(events):
        if event.get("event") not in supported:
            raise DemoAdmissionError(f"unsupported tape event: {event.get('event')!r}")
        if event.get("event") == "metadata" and expected_seq != 0:
            raise DemoAdmissionError("metadata may appear only as the first tape record")
        if event.get("seq") != expected_seq:
            raise DemoAdmissionError("tape sequence must be contiguous from zero")
        ts_ms = event.get("ts_ms")
        if not isinstance(ts_ms, int) or isinstance(ts_ms, bool) or ts_ms < previous_ts:
            raise DemoAdmissionError("tape timestamps must be non-decreasing integer milliseconds")
        previous_ts = ts_ms
    return events


def _source_identity() -> dict[str, Any]:
    return {
        "identity_kind": "package_distribution",
        "package_version": PACKAGE_VERSION,
        "exact_bytes": "external_wheel_digest_or_git_commit_tree",
        "version": ENGINE_VERSION,
    }


def _identity(
    *,
    contract_path: Path,
    contract: Mapping[str, Any],
    tape_path: Path,
) -> dict[str, Any]:
    return {
        "accounting_contract_version": ACCOUNTING_CONTRACT_VERSION,
        "code": _source_identity(),
        "contract": {
            "artifact_id": "public_replay_demo_contract",
            "sha256": sha256_file(contract_path),
            "version": CONTRACT_VERSION,
        },
        "state_contract_version": STATE_CONTRACT_VERSION,
        "tape": {
            "artifact_id": contract["tape"]["artifact_id"],
            "schema_version": TAPE_VERSION,
            "sha256": sha256_file(tape_path),
        },
    }


class ReferenceReplayEngine:
    """Small FIFO top-book model; mechanics only, never an exchange simulator."""

    def __init__(self, contract: Mapping[str, Any], events: Sequence[Mapping[str, Any]]) -> None:
        self.contract = contract
        self.events = events
        self.tick_size = _decimal(contract["market"]["tick_size"], field="tick_size", positive=True)
        self.lot_size = _decimal(
            contract["market"]["lot_size_btc"], field="lot_size_btc", positive=True
        )
        self.maker_fee_rate = _decimal(
            contract["market"]["maker_fee_rate"],
            field="maker_fee_rate",
            nonnegative=True,
        )
        self.book: TopBook | None = None
        self.orders: dict[str, SimulatedOrder] = {}
        self.trace: list[dict[str, Any]] = []
        self.position_btc = ZERO
        self.campaigns_opened = 0
        self.orders_with_opportunity: set[str] = set()
        self.counters = {
            "book": 0,
            "trade": 0,
            "mark": 0,
            "market": 0,
            "order_lifecycle": 0,
            "fill_opportunities": 0,
            "fill_opportunities_with_fill": 0,
            "fill_events": 0,
        }

        first_book = next((event for event in events if event.get("event") == "book"), None)
        if first_book is None:
            raise DemoReplayError("synthetic tape requires at least one book event")
        initial_bid = _decimal(first_book.get("bid_price"), field="bid_price", positive=True)
        initial_ask = _decimal(first_book.get("ask_price"), field="ask_price", positive=True)
        initial_mark = (initial_bid + initial_ask) / Decimal("2")
        self.ledger = ContinuousAccountingLedger(
            ContinuousReplayState(
                arm_id="public_synthetic_demo",
                checkpoint_ts_ms=int(events[0]["ts_ms"]),
                cash_usdc=0.0,
                position_btc=0.0,
                average_entry_price=0.0,
                cumulative_realized_pnl_usdc=0.0,
                cumulative_fees_usdc=0.0,
                equity_anchor_usdc=0.0,
                last_mark_price=float(initial_mark),
                cumulative_pnl_usdc=0.0,
            )
        )

    def run(self) -> None:
        for event in self.events:
            handler = getattr(self, f"_on_{event['event']}")
            handler(event)

    def _on_metadata(self, event: Mapping[str, Any]) -> None:
        self.trace.append(
            {
                "classification": event["classification"],
                "event": "metadata",
                "schema_version": TRACE_VERSION,
                "seq": event["seq"],
                "symbol": event["symbol"],
                "ts_ms": event["ts_ms"],
            }
        )

    def _on_book(self, event: Mapping[str, Any]) -> None:
        book = TopBook(
            bid_price=_decimal(event.get("bid_price"), field="bid_price", positive=True),
            bid_quantity=_decimal(
                event.get("bid_quantity"), field="bid_quantity", nonnegative=True
            ),
            ask_price=_decimal(event.get("ask_price"), field="ask_price", positive=True),
            ask_quantity=_decimal(
                event.get("ask_quantity"), field="ask_quantity", nonnegative=True
            ),
        )
        if book.bid_price >= book.ask_price:
            raise DemoReplayError("synthetic top book must remain uncrossed")
        if book.bid_price % self.tick_size or book.ask_price % self.tick_size:
            raise DemoReplayError("book price is not tick aligned")
        self.book = book
        self.ledger.mark(int(event["ts_ms"]), float(book.midpoint))
        self.counters["book"] += 1
        self.counters["market"] += 1
        self.trace.append(
            {
                "ask_price": _display(book.ask_price),
                "ask_quantity": _display(book.ask_quantity),
                "bid_price": _display(book.bid_price),
                "bid_quantity": _display(book.bid_quantity),
                "event": "book",
                "mark_price": _display(book.midpoint),
                "seq": event["seq"],
                "ts_ms": event["ts_ms"],
            }
        )

    def _on_submit(self, event: Mapping[str, Any]) -> None:
        if self.book is None:
            raise DemoReplayError("order submitted before a top book exists")
        order_id = str(event.get("order_id", "")).strip()
        side = str(event.get("side", "")).upper()
        price = _decimal(event.get("price"), field="order_price", positive=True)
        quantity = _decimal(event.get("quantity"), field="order_quantity", positive=True)
        if not order_id or order_id in self.orders:
            raise DemoReplayError("order ids must be unique and non-empty")
        if side not in {"BUY", "SELL"}:
            raise DemoReplayError("order side must be BUY or SELL")
        if price % self.tick_size or quantity % self.lot_size:
            raise DemoReplayError("order price or quantity is not contract aligned")
        if (side == "BUY" and price >= self.book.ask_price) or (
            side == "SELL" and price <= self.book.bid_price
        ):
            raise DemoReplayError("synthetic maker order would cross the book")
        if any(
            order.status == "active" and order.side == side and order.price == price
            for order in self.orders.values()
        ):
            raise DemoReplayError("reference engine supports one active order per side and price")
        queue_ahead = self.book.queue_quantity(side, price)
        self.orders[order_id] = SimulatedOrder(
            order_id=order_id,
            side=side,
            price=price,
            quantity=quantity,
            remaining_quantity=quantity,
            queue_ahead_quantity=queue_ahead,
            submit_seq=int(event["seq"]),
        )
        self.counters["order_lifecycle"] += 1
        self.trace.append(
            {
                "event": "submit",
                "order_id": order_id,
                "price": _display(price),
                "quantity": _display(quantity),
                "queue_ahead_quantity": _display(queue_ahead),
                "seq": event["seq"],
                "side": side,
                "status": "active",
                "ts_ms": event["ts_ms"],
            }
        )

    def _apply_fill(
        self,
        *,
        order: SimulatedOrder,
        quantity: Decimal,
        ts_ms: int,
    ) -> dict[str, Any]:
        before_position = self.position_btc
        signed_quantity = quantity if order.side == "BUY" else -quantity
        after_position = before_position + signed_quantity
        campaign_id: str | None = None
        if before_position == ZERO and after_position != ZERO:
            self.campaigns_opened += 1
            campaign_id = f"synthetic-campaign-{self.campaigns_opened:03d}"
        elif before_position * after_position < ZERO:
            self.campaigns_opened += 1
            campaign_id = f"synthetic-campaign-{self.campaigns_opened:03d}"
        fee = quantity * order.price * self.maker_fee_rate
        self.ledger.fill(
            ts_ms=ts_ms,
            side=order.side,
            quantity_btc=float(quantity),
            price=float(order.price),
            fee_usdc=float(fee),
            new_campaign_id=campaign_id,
        )
        self.position_btc = after_position
        if _display(self.ledger.state.position_btc) != _display(self.position_btc):
            raise DemoReplayError("reference queue inventory diverged from accounting inventory")
        return {
            "campaign_id_opened": campaign_id,
            "fee_usdc": _display(fee),
            "inventory_after_btc": _display(after_position),
            "inventory_before_btc": _display(before_position),
            "order_id": order.order_id,
            "price": _display(order.price),
            "quantity_btc": _display(quantity),
            "side": order.side,
        }

    def _on_trade(self, event: Mapping[str, Any]) -> None:
        aggressor_side = str(event.get("aggressor_side", "")).upper()
        if aggressor_side not in {"BUY", "SELL"}:
            raise DemoReplayError("trade aggressor side must be BUY or SELL")
        passive_side = "SELL" if aggressor_side == "BUY" else "BUY"
        price = _decimal(event.get("price"), field="trade_price", positive=True)
        quantity = _decimal(event.get("quantity"), field="trade_quantity", positive=True)
        if price % self.tick_size or quantity % self.lot_size:
            raise DemoReplayError("trade price or quantity is not contract aligned")
        candidates = sorted(
            (
                order
                for order in self.orders.values()
                if order.status == "active" and order.side == passive_side and order.price == price
            ),
            key=lambda order: (order.submit_seq, order.order_id),
        )
        if len(candidates) > 1:
            raise DemoReplayError("ambiguous same-level queue is outside the reference contract")

        details: list[dict[str, Any]] = []
        fills: list[dict[str, Any]] = []
        for order in candidates:
            self.counters["fill_opportunities"] += 1
            order.matching_trade_opportunities += 1
            self.orders_with_opportunity.add(order.order_id)
            available = quantity
            queue_before = order.queue_ahead_quantity
            queue_consumed = min(queue_before, available)
            order.queue_ahead_quantity -= queue_consumed
            available -= queue_consumed
            fill_quantity = min(order.remaining_quantity, available)
            if fill_quantity > ZERO:
                order.remaining_quantity -= fill_quantity
                order.filled_quantity += fill_quantity
                order.fill_events += 1
                self.counters["fill_events"] += 1
                self.counters["fill_opportunities_with_fill"] += 1
                fills.append(
                    self._apply_fill(
                        order=order,
                        quantity=fill_quantity,
                        ts_ms=int(event["ts_ms"]),
                    )
                )
                if order.remaining_quantity == ZERO:
                    order.status = "filled"
            details.append(
                {
                    "fill_quantity_btc": _display(fill_quantity),
                    "order_id": order.order_id,
                    "order_status": order.status,
                    "queue_ahead_after_btc": _display(order.queue_ahead_quantity),
                    "queue_ahead_before_btc": _display(queue_before),
                    "queue_consumed_btc": _display(queue_consumed),
                    "remaining_quantity_btc": _display(order.remaining_quantity),
                }
            )

        self.counters["trade"] += 1
        self.counters["market"] += 1
        self.trace.append(
            {
                "aggressor_side": aggressor_side,
                "event": "trade",
                "fills": fills,
                "opportunities": details,
                "passive_side": passive_side,
                "price": _display(price),
                "quantity": _display(quantity),
                "seq": event["seq"],
                "ts_ms": event["ts_ms"],
            }
        )

    def _on_cancel(self, event: Mapping[str, Any]) -> None:
        order_id = str(event.get("order_id", ""))
        order = self.orders.get(order_id)
        if order is None or order.status != "active":
            raise DemoReplayError("cancel must name an active synthetic order")
        order.status = "canceled"
        self.counters["order_lifecycle"] += 1
        self.trace.append(
            {
                "event": "cancel",
                "filled_quantity_btc": _display(order.filled_quantity),
                "order_id": order_id,
                "reason": str(event.get("reason", "")),
                "remaining_quantity_btc": _display(order.remaining_quantity),
                "seq": event["seq"],
                "status": "canceled",
                "ts_ms": event["ts_ms"],
            }
        )

    def _on_mark(self, event: Mapping[str, Any]) -> None:
        price = _decimal(event.get("price"), field="mark_price", positive=True)
        if price % (self.tick_size / Decimal("2")):
            raise DemoReplayError("terminal mark is not half-tick aligned")
        self.ledger.mark(int(event["ts_ms"]), float(price))
        self.counters["mark"] += 1
        self.counters["market"] += 1
        self.trace.append(
            {
                "event": "mark",
                "price": _display(price),
                "seq": event["seq"],
                "terminal_equity_usdc": _display(self.ledger.equity_usdc),
                "ts_ms": event["ts_ms"],
            }
        )

    def denominators(self) -> dict[str, Any]:
        orders = list(self.orders.values())
        filled_quantity = sum((order.filled_quantity for order in orders), ZERO)
        terminal_orders = [order for order in orders if order.status in {"filled", "canceled"}]
        canceled = [order for order in orders if order.status == "canceled"]
        return {
            "campaigns": {
                "closed": len(self.ledger.closed_campaigns),
                "open_at_terminal": 1 if self.ledger.state.economic_campaign is not None else 0,
                "opened": self.campaigns_opened,
            },
            "events": {
                "book": self.counters["book"],
                "mark": self.counters["mark"],
                "market": self.counters["market"],
                "order_lifecycle": self.counters["order_lifecycle"],
                "tape_records": len(self.events),
                "trade": self.counters["trade"],
            },
            "fill_opportunities": {
                "eligible": self.counters["fill_opportunities"],
                "fill_events": self.counters["fill_events"],
                "filled_quantity_btc": _display(filled_quantity),
                "with_fill": self.counters["fill_opportunities_with_fill"],
                "without_fill": (
                    self.counters["fill_opportunities"]
                    - self.counters["fill_opportunities_with_fill"]
                ),
            },
            "orders": {
                "active_at_terminal": sum(order.status == "active" for order in orders),
                "canceled": len(canceled),
                "canceled_unfilled": sum(order.filled_quantity == ZERO for order in canceled),
                "fill_eligible": len(orders),
                "filled": sum(order.status == "filled" for order in orders),
                "submitted": len(orders),
                "terminal": len(terminal_orders),
                "with_trade_opportunity": len(self.orders_with_opportunity),
            },
        }

    def campaign_payload(self) -> dict[str, Any]:
        closed = [
            {
                "campaign_id": row.campaign_id,
                "end_ts_ms": row.end_ts_ms,
                "peak_abs_inventory_btc": _display(row.peak_abs_inventory_btc),
                "side": row.side,
                "start_ts_ms": row.start_ts_ms,
                "terminal_value_usdc": _display(row.value_usdc),
            }
            for row in self.ledger.closed_campaigns
        ]
        value = sum((Decimal(str(row.value_usdc)) for row in self.ledger.closed_campaigns), ZERO)
        return {
            "closed": closed,
            "closed_count": len(closed),
            "open_at_terminal": self.ledger.state.economic_campaign is not None,
            "terminal_value_usdc": _display(value),
        }

    def terminal_payload(self) -> dict[str, Any]:
        state = self.ledger.state
        return {
            "cash_usdc": _display(state.cash_usdc),
            "cumulative_fees_usdc": _display(state.cumulative_fees_usdc),
            "inventory_btc": _display(state.position_btc),
            "mark_price_usdc_per_btc": _display(state.last_mark_price),
            "terminal_pnl_usdc": _display(state.cumulative_pnl_usdc),
        }

    def orders_payload(self) -> list[dict[str, Any]]:
        return [
            {
                "fill_events": order.fill_events,
                "filled_quantity_btc": _display(order.filled_quantity),
                "matching_trade_opportunities": order.matching_trade_opportunities,
                "order_id": order.order_id,
                "price": _display(order.price),
                "quantity_btc": _display(order.quantity),
                "queue_ahead_terminal_btc": _display(order.queue_ahead_quantity),
                "side": order.side,
                "status": order.status,
            }
            for order in sorted(self.orders.values(), key=lambda value: value.submit_seq)
        ]


def _gate(
    *,
    contract: Mapping[str, Any],
    identity: Mapping[str, Any],
    engine: ReferenceReplayEngine,
    denominators: Mapping[str, Any],
    campaign: Mapping[str, Any],
    terminal: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, *, observed: object = None, expected: object = None) -> None:
        row: dict[str, Any] = {"check": name, "passed": bool(passed)}
        if observed is not None:
            row["observed"] = observed
        if expected is not None:
            row["expected"] = expected
        checks.append(row)

    add(
        "tape_sha256_matches_contract",
        identity["tape"]["sha256"] == contract["tape"]["sha256"],
        observed=identity["tape"]["sha256"],
        expected=contract["tape"]["sha256"],
    )
    add(
        "denominators_match_contract",
        denominators == contract["expected_denominators"],
        observed=denominators,
        expected=contract["expected_denominators"],
    )
    expected_terminal = contract["expected_terminal"]
    observed_terminal = {
        **terminal,
        "campaign_terminal_value_usdc": campaign["terminal_value_usdc"],
    }
    add(
        "terminal_values_match_contract",
        observed_terminal == expected_terminal,
        observed=observed_terminal,
        expected=expected_terminal,
    )
    add(
        "all_orders_terminal",
        denominators["orders"]["active_at_terminal"] == 0,
        observed=denominators["orders"]["active_at_terminal"],
        expected=0,
    )
    add(
        "campaign_closed_flat",
        not campaign["open_at_terminal"] and terminal["inventory_btc"] == _display(ZERO),
    )
    add(
        "campaign_value_equals_terminal_pnl",
        campaign["terminal_value_usdc"] == terminal["terminal_pnl_usdc"],
        observed=campaign["terminal_value_usdc"],
        expected=terminal["terminal_pnl_usdc"],
    )
    add(
        "trace_covers_every_tape_record",
        len(engine.trace) == denominators["events"]["tape_records"],
        observed=len(engine.trace),
        expected=denominators["events"]["tape_records"],
    )
    required_permissions = {
        "external_order_submission": False,
        "live_runtime_import": False,
        "network_access": False,
        "private_evidence_read": False,
        "runtime_clock_read": False,
    }
    add(
        "offline_permissions_locked",
        contract["permissions"] == required_permissions,
        observed=contract["permissions"],
        expected=required_permissions,
    )
    add(
        "synthetic_non_economic_boundary",
        contract["fixture"]["classification"] == "synthetic_non_economic"
        and contract["fixture"]["economic_authority"] == "none",
    )

    failures = [row["check"] for row in checks if not row["passed"]]
    passed = not failures
    return {
        "checks": checks,
        "economic_evidence_eligible": False,
        "failures": failures,
        "live_action_eligible": False,
        "passed": passed,
        "promotion_eligible": False,
        "schema_version": GATE_VERSION,
        "status": contract["gate_contract"]["status_on_pass"] if passed else "failed_closed",
    }


def _summary(
    *,
    contract: Mapping[str, Any],
    identity: Mapping[str, Any],
    engine: ReferenceReplayEngine,
) -> dict[str, Any]:
    denominators = engine.denominators()
    campaign = engine.campaign_payload()
    terminal = engine.terminal_payload()
    gate = _gate(
        contract=contract,
        identity=identity,
        engine=engine,
        denominators=denominators,
        campaign=campaign,
        terminal=terminal,
    )
    accounting_audit = engine.ledger.accounting_audit()
    summary: dict[str, Any] = {
        "accounting": {
            "campaign_value_additivity_error_usdc": _display(
                Decimal(campaign["terminal_value_usdc"])
                - Decimal(terminal["terminal_pnl_usdc"])
            ),
            "campaigns_closed": accounting_audit["campaigns_closed"],
            "continuous_pnl_usdc": _display(accounting_audit["continuous_pnl_usdc"]),
            "contract_version": ACCOUNTING_CONTRACT_VERSION,
            "state_contract_version": STATE_CONTRACT_VERSION,
        },
        "campaign": campaign,
        "campaign_terminal_value_usdc": campaign["terminal_value_usdc"],
        "classification": contract["fixture"],
        "denominators": denominators,
        "frozen_generated_at_utc": contract["frozen_generated_at_utc"],
        "gate": gate,
        "identity": identity,
        "orders": engine.orders_payload(),
        "permissions": contract["permissions"],
        "schema_version": SUMMARY_VERSION,
        "terminal": terminal,
        "canonical_summary_sha256": "",
    }
    summary["canonical_summary_sha256"] = canonical_document_sha256(
        summary, "canonical_summary_sha256"
    )
    return summary


def _receipt(
    *,
    contract: Mapping[str, Any],
    summary: Mapping[str, Any],
    summary_bytes: bytes,
    trace_bytes: bytes,
) -> dict[str, Any]:
    receipt: dict[str, Any] = {
        "artifacts": {
            "summary": {
                "canonical_sha256": summary["canonical_summary_sha256"],
                "logical_path": "summary.json",
                "sha256": sha256_bytes(summary_bytes),
            },
            "trace": {
                "logical_path": "trace.jsonl",
                "schema_version": TRACE_VERSION,
                "sha256": sha256_bytes(trace_bytes),
            },
        },
        "campaign_terminal_value_usdc": summary["campaign_terminal_value_usdc"],
        "denominators": summary["denominators"],
        "frozen_generated_at_utc": contract["frozen_generated_at_utc"],
        "gate": {
            "economic_evidence_eligible": False,
            "live_action_eligible": False,
            "passed": summary["gate"]["passed"],
            "promotion_eligible": False,
            "status": summary["gate"]["status"],
        },
        "identity": summary["identity"],
        "permissions": summary["permissions"],
        "schema_version": RECEIPT_VERSION,
        "canonical_receipt_sha256": "",
    }
    receipt["canonical_receipt_sha256"] = canonical_document_sha256(
        receipt, "canonical_receipt_sha256"
    )
    return receipt


def verify_reference_outputs(
    artifacts: Mapping[str, bytes],
    reference_dir: Path,
) -> None:
    for filename, observed in artifacts.items():
        reference_path = reference_dir / filename
        if not reference_path.is_file():
            raise DemoAdmissionError(f"reference output is missing: {filename}")
        expected = reference_path.read_bytes()
        if observed != expected:
            raise DemoAdmissionError(f"reference byte mismatch: {filename}")


def run_demo(
    *,
    output_dir: Path,
    contract_path: Path = DEFAULT_CONTRACT,
    verify_reference: bool = False,
    reference_dir: Path = DEFAULT_REFERENCE_DIR,
) -> DemoRun:
    contract_path = contract_path.resolve()
    contract = _read_contract(contract_path)
    tape_path = _resolve_tape(contract_path, contract)
    events = _read_tape(tape_path, contract)
    identity = _identity(contract_path=contract_path, contract=contract, tape_path=tape_path)

    engine = ReferenceReplayEngine(contract, events)
    engine.run()
    summary = _summary(contract=contract, identity=identity, engine=engine)
    trace_bytes = trace_jsonl_bytes(engine.trace)
    summary_bytes = pretty_json_bytes(summary)
    receipt = _receipt(
        contract=contract,
        summary=summary,
        summary_bytes=summary_bytes,
        trace_bytes=trace_bytes,
    )
    receipt_bytes = pretty_json_bytes(receipt)

    artifacts = {
        "summary.json": summary_bytes,
        "trace.jsonl": trace_bytes,
        "receipt.json": receipt_bytes,
    }
    if verify_reference:
        verify_reference_outputs(artifacts, reference_dir.resolve())

    output_dir = output_dir.resolve()
    summary_path = output_dir / "summary.json"
    trace_path = output_dir / "trace.jsonl"
    receipt_path = output_dir / "receipt.json"
    for filename, payload in artifacts.items():
        _write_atomic(output_dir / filename, payload)
    return DemoRun(
        summary=summary,
        receipt=receipt,
        summary_path=summary_path,
        trace_path=trace_path,
        receipt_path=receipt_path,
        reference_verified=verify_reference,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the public synthetic queue-to-campaign replay. "
            "It has no network or external-order capability."
        )
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="directory for deterministic summary, trace, and receipt artifacts",
    )
    parser.add_argument(
        "--contract",
        type=Path,
        default=DEFAULT_CONTRACT,
        help="hash-bound synthetic fixture contract",
    )
    parser.add_argument(
        "--verify-reference",
        action="store_true",
        help="require byte equality with the distributed reference output",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_demo(
            output_dir=args.output_dir,
            contract_path=args.contract,
            verify_reference=args.verify_reference,
        )
    except (DemoError, OSError, KeyError, TypeError, ValueError) as exc:
        print(f"replay-demo failed closed: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "campaign_terminal_value_usdc": result.summary[
                    "campaign_terminal_value_usdc"
                ],
                "gate_status": result.summary["gate"]["status"],
                "receipt": str(result.receipt_path),
                "reference_verified": result.reference_verified,
                "summary": str(result.summary_path),
                "tape_sha256": result.summary["identity"]["tape"]["sha256"],
                "trace": str(result.trace_path),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if result.summary["gate"]["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
