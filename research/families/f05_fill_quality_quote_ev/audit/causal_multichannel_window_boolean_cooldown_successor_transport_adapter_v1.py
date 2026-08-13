"""Three-clock transport adapter for the F05 full-multiscale successor.

The adapter keeps three states separate:

* exchange-time public book truth and arm-local fill truth;
* receive/feature-ready public market visibility; and
* private fill visibility, which alone may update strategy inventory.

It is mechanics infrastructure, not an economic simulator.  It deliberately
refuses to invent cross-stream ordering or a zero-delay counterfactual private
fill callback.  A caller must run an actual repeated-policy simulator on top
of this state machine before any terminal-value result can be admitted.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any

from models.exchange_book_replay import (
    HistoricalExchangeBookScheduler,
    HistoricalExchangeBookVisibilityScheduler,
)
from models.tick_data_types import HistoricalExchangeBookEvent

TRANSPORT_IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_successor_transport_adapter_v1"
)
TRANSPORT_RECEIPT_SCHEMA_VERSION = f"{TRANSPORT_IDENTITY}.receipt.v1"

_SHA256_LENGTH = 64


class TransportContractError(ValueError):
    """Raised when recorded or modeled clocks cannot support causal replay."""


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if (
        len(normalized) != _SHA256_LENGTH
        or any(character not in "0123456789abcdef" for character in normalized)
    ):
        raise TransportContractError(f"{label} must be a lowercase SHA256")
    return normalized


def _positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise TransportContractError(f"{label} must be a positive integer")
    result = int(value)
    if result <= 0:
        raise TransportContractError(f"{label} must be a positive integer")
    return result


def _nonnegative_int(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise TransportContractError(f"{label} must be a nonnegative integer")
    result = int(value)
    if result < 0:
        raise TransportContractError(f"{label} must be a nonnegative integer")
    return result


def _positive_float(value: Any, label: str) -> float:
    if isinstance(value, bool):
        raise TransportContractError(f"{label} must be finite and positive")
    result = float(value)
    if not math.isfinite(result) or result <= 0.0:
        raise TransportContractError(f"{label} must be finite and positive")
    return result


@dataclass(frozen=True, slots=True)
class RecordedBookVisibility:
    """One native book message plus the clock when its features are usable."""

    event: HistoricalExchangeBookEvent
    feature_ready_ts_ns: int
    total_order_ordinal: int

    def __post_init__(self) -> None:
        ready = _positive_int(self.feature_ready_ts_ns, "book feature-ready clock")
        receive = _positive_int(
            self.event.local_receive_ts_ns,
            "book receive clock",
        )
        _positive_int(self.total_order_ordinal, "book total-order ordinal")
        if int(self.event.exchange_ts_ns) > receive or receive > ready:
            raise TransportContractError("book exchange/receive/ready clocks inverted")


@dataclass(frozen=True, slots=True)
class RecordedTradeVisibility:
    """One public trade with exchange, receive, and feature-ready clocks."""

    event_id: str
    exchange_ts_ns: int
    receive_ts_ns: int
    feature_ready_ts_ns: int
    total_order_ordinal: int
    side: str
    price: float
    quantity: float
    source_sequence: int

    def __post_init__(self) -> None:
        if not str(self.event_id).strip():
            raise TransportContractError("trade event id is required")
        exchange = _positive_int(self.exchange_ts_ns, "trade exchange clock")
        receive = _positive_int(self.receive_ts_ns, "trade receive clock")
        ready = _positive_int(self.feature_ready_ts_ns, "trade feature-ready clock")
        _positive_int(self.total_order_ordinal, "trade total-order ordinal")
        _nonnegative_int(self.source_sequence, "trade source sequence")
        if exchange > receive or receive > ready:
            raise TransportContractError("trade exchange/receive/ready clocks inverted")
        if str(self.side).upper() not in {"BUY", "SELL"}:
            raise TransportContractError("trade side must be BUY or SELL")
        _positive_float(self.price, "trade price")
        _positive_float(self.quantity, "trade quantity")


@dataclass(frozen=True, slots=True)
class ArmFillTruthEvent:
    """One arm-local exchange fill and its optional recorded private callback."""

    fill_event_id: str
    lifecycle_id: str
    side: str
    role: str
    exchange_ts_ns: int
    price: float
    quantity: float
    partial_fill_ordinal: int
    lifecycle_sequence: int
    total_order_ordinal: int
    recorded_receive_ts_ns: int | None = None
    recorded_feature_ready_ts_ns: int | None = None

    def __post_init__(self) -> None:
        if not str(self.fill_event_id).strip() or not str(self.lifecycle_id).strip():
            raise TransportContractError("fill and lifecycle ids are required")
        if str(self.side).upper() not in {"BUY", "SELL"}:
            raise TransportContractError("fill side must be BUY or SELL")
        if str(self.role) not in {"opener", "add", "reducing"}:
            raise TransportContractError("fill role is invalid")
        exchange = _positive_int(self.exchange_ts_ns, "fill exchange clock")
        _positive_float(self.price, "fill price")
        _positive_float(self.quantity, "fill quantity")
        _positive_int(self.partial_fill_ordinal, "partial-fill ordinal")
        _positive_int(self.lifecycle_sequence, "fill lifecycle sequence")
        _positive_int(self.total_order_ordinal, "fill total-order ordinal")
        clocks = (self.recorded_receive_ts_ns, self.recorded_feature_ready_ts_ns)
        if (clocks[0] is None) != (clocks[1] is None):
            raise TransportContractError(
                "recorded private fill requires both receive and feature-ready clocks"
            )
        if clocks[0] is not None:
            receive = _positive_int(clocks[0], "fill receive clock")
            ready = _positive_int(clocks[1], "fill feature-ready clock")
            if exchange > receive or receive > ready:
                raise TransportContractError(
                    "fill exchange/receive/feature-ready clocks inverted"
                )


@dataclass(frozen=True, slots=True)
class FillDelayCohort:
    """Past-only paired callback delays for one side/role cohort."""

    side: str
    role: str
    receive_delay_ns: tuple[int, ...]
    feature_after_receive_ns: tuple[int, ...]

    def __post_init__(self) -> None:
        if str(self.side).upper() not in {"BUY", "SELL"}:
            raise TransportContractError("delay cohort side is invalid")
        if str(self.role) not in {"opener", "add", "reducing"}:
            raise TransportContractError("delay cohort role is invalid")
        if not self.receive_delay_ns or (
            len(self.receive_delay_ns) != len(self.feature_after_receive_ns)
        ):
            raise TransportContractError("delay cohort requires paired observations")
        for delay in self.receive_delay_ns:
            if _positive_int(delay, "modeled receive delay") <= 0:
                raise TransportContractError("modeled receive delay cannot be zero")
        for delay in self.feature_after_receive_ns:
            _nonnegative_int(delay, "modeled feature-ready delay")

    @property
    def key(self) -> str:
        return f"{str(self.side).upper()}:{self.role}"


@dataclass(frozen=True, slots=True)
class ResolvedPrivateFillVisibility:
    fill_event_id: str
    receive_ts_ns: int
    feature_ready_ts_ns: int
    authority: str
    delay_artifact_sha256: str | None


@dataclass(frozen=True, slots=True)
class PastOnlyPrivateFillDelayArtifact:
    """Hash-bound sensitivity model for counterfactual private callbacks."""

    identity: str
    fitted_through_ts_ns: int
    minimum_support: int
    cohorts: tuple[FillDelayCohort, ...]

    def __post_init__(self) -> None:
        if not str(self.identity).strip():
            raise TransportContractError("delay artifact identity is required")
        _positive_int(self.fitted_through_ts_ns, "delay artifact cutoff")
        _positive_int(self.minimum_support, "delay artifact minimum support")
        keys = [cohort.key for cohort in self.cohorts]
        if len(keys) != len(set(keys)):
            raise TransportContractError("delay artifact repeats a side/role cohort")

    @property
    def artifact_sha256(self) -> str:
        return _canonical_sha256(
            {
                "identity": self.identity,
                "fitted_through_ts_ns": self.fitted_through_ts_ns,
                "minimum_support": self.minimum_support,
                "cohorts": [asdict(cohort) for cohort in self.cohorts],
            }
        )

    def resolve(
        self,
        fill: ArmFillTruthEvent,
    ) -> ResolvedPrivateFillVisibility | None:
        if int(fill.exchange_ts_ns) <= int(self.fitted_through_ts_ns):
            raise TransportContractError(
                "counterfactual fill delay artifact is not past-only"
            )
        cohort = next(
            (
                row
                for row in self.cohorts
                if row.key == f"{str(fill.side).upper()}:{fill.role}"
            ),
            None,
        )
        if cohort is None or len(cohort.receive_delay_ns) < self.minimum_support:
            return None
        selector = hashlib.sha256(
            f"{self.artifact_sha256}:{fill.fill_event_id}".encode("ascii")
        ).digest()
        index = int.from_bytes(selector[:8], "big") % len(cohort.receive_delay_ns)
        receive_delay = int(cohort.receive_delay_ns[index])
        feature_delay = int(cohort.feature_after_receive_ns[index])
        if receive_delay <= 0:
            raise TransportContractError(
                "counterfactual private visibility cannot assume zero delay"
            )
        receive = int(fill.exchange_ts_ns) + receive_delay
        return ResolvedPrivateFillVisibility(
            fill_event_id=fill.fill_event_id,
            receive_ts_ns=receive,
            feature_ready_ts_ns=receive + feature_delay,
            authority="modeled_sensitivity",
            delay_artifact_sha256=self.artifact_sha256,
        )


@dataclass(frozen=True, slots=True)
class VisibleStrategyState:
    asof_feature_ready_ts_ns: int
    inventory_btc: float
    cash_usdc_before_fees: float
    public_trade_count: int
    private_fill_callback_count: int
    last_public_trade_id: str | None
    last_private_fill_event_id: str | None


@dataclass(frozen=True, slots=True)
class TransportReceipt:
    schema_version: str
    identity: str
    arm: str
    common_market_source_sha256: str
    arm_fill_source_sha256: str
    delay_artifact_sha256: str | None
    private_fill_visibility_authority: str
    book_event_count: int
    book_visible_count: int
    trade_event_count: int
    trade_visible_count: int
    fill_truth_count: int
    private_fill_visible_count: int
    counterfactual_fill_censored_count: int
    source_gap_count: int
    pre_exchange_clamp_count: int
    head_of_line_clamp_count: int
    clock_inversion_count: int
    future_visibility_violation_count: int
    ambiguous_same_timestamp_count: int
    pending_private_fill_count: int
    formal_replay_support_valid: bool
    live_equivalent: bool
    thread_interleaving_replayed: bool
    rest_user_stream_reconnect_replayed: bool
    action_authorized: bool
    live_policy_authorized: bool
    exclusion_reasons: tuple[str, ...]
    transport_receipt_sha256: str

    def __post_init__(self) -> None:
        if self.schema_version != TRANSPORT_RECEIPT_SCHEMA_VERSION:
            raise TransportContractError("transport receipt schema drifted")
        if self.identity != TRANSPORT_IDENTITY:
            raise TransportContractError("transport receipt identity drifted")
        for label, value in (
            ("common market source", self.common_market_source_sha256),
            ("arm fill source", self.arm_fill_source_sha256),
            ("transport receipt", self.transport_receipt_sha256),
        ):
            _require_sha256(value, label)
        if self.delay_artifact_sha256 is not None:
            _require_sha256(self.delay_artifact_sha256, "delay artifact")
        if any(
            (
                self.live_equivalent,
                self.thread_interleaving_replayed,
                self.rest_user_stream_reconnect_replayed,
                self.action_authorized,
                self.live_policy_authorized,
            )
        ):
            raise TransportContractError(
                "research transport receipt cannot claim live or action authority"
            )
        body = asdict(self)
        supplied = body.pop("transport_receipt_sha256")
        if _canonical_sha256(body) != supplied:
            raise TransportContractError("transport receipt hash mismatch")
        if self.formal_replay_support_valid and self.exclusion_reasons:
            raise TransportContractError(
                "supported transport receipt carries exclusion reasons"
            )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ProspectiveReplayTransportBundle:
    """Immutable public source bundle shared byte-for-byte by paired arms."""

    market_source_manifest_sha256: str
    book_events: tuple[RecordedBookVisibility, ...]
    trade_events: tuple[RecordedTradeVisibility, ...]
    common_market_source_sha256: str

    @classmethod
    def build(
        cls,
        *,
        market_source_manifest_sha256: str,
        book_events: Sequence[RecordedBookVisibility],
        trade_events: Sequence[RecordedTradeVisibility],
    ) -> ProspectiveReplayTransportBundle:
        manifest_sha = _require_sha256(
            market_source_manifest_sha256,
            "market source manifest",
        )
        books = tuple(book_events)
        trades = tuple(trade_events)
        if not books or not trades:
            raise TransportContractError(
                "formal transport requires both native book and individual trades"
            )
        _validate_unique_ids(
            [row.event.source_ordinal for row in books],
            "book source ordinal",
            allow_zero=False,
        )
        _validate_unique_ids(
            [row.event_id for row in trades],
            "trade event id",
            allow_zero=True,
        )
        _validate_stream_ordering(books, trades, ())
        body = {
            "identity": TRANSPORT_IDENTITY,
            "market_source_manifest_sha256": manifest_sha,
            "book_events": [_book_payload(row) for row in books],
            "trade_events": [asdict(row) for row in trades],
        }
        return cls(
            market_source_manifest_sha256=manifest_sha,
            book_events=books,
            trade_events=trades,
            common_market_source_sha256=_canonical_sha256(body),
        )

    def spawn_arm(
        self,
        *,
        arm: str,
        fill_events: Sequence[ArmFillTruthEvent],
        counterfactual_delay_artifact: PastOnlyPrivateFillDelayArtifact | None = None,
    ) -> ArmReplayTransport:
        return ArmReplayTransport(
            bundle=self,
            arm=arm,
            fill_events=tuple(fill_events),
            counterfactual_delay_artifact=counterfactual_delay_artifact,
        )


@dataclass(frozen=True, slots=True)
class _TruthEnvelope:
    ts_ns: int
    total_order_ordinal: int
    kind: str
    payload: Any


@dataclass(frozen=True, slots=True)
class _VisibleEnvelope:
    ready_ts_ns: int
    total_order_ordinal: int
    kind: str
    payload: Any


def _book_payload(row: RecordedBookVisibility) -> dict[str, Any]:
    return {
        "event": asdict(row.event),
        "feature_ready_ts_ns": row.feature_ready_ts_ns,
        "total_order_ordinal": row.total_order_ordinal,
    }


def _validate_unique_ids(
    values: Sequence[Any],
    label: str,
    *,
    allow_zero: bool,
) -> None:
    normalized = tuple(values)
    if len(normalized) != len(set(normalized)):
        raise TransportContractError(f"{label} is not unique")
    if not allow_zero and any(int(value) <= 0 for value in normalized):
        raise TransportContractError(f"{label} must be positive")


def _validate_stream_ordering(
    books: Sequence[RecordedBookVisibility],
    trades: Sequence[RecordedTradeVisibility],
    fills: Sequence[ArmFillTruthEvent],
) -> None:
    exchange_rows = [
        (int(row.event.exchange_ts_ns), int(row.total_order_ordinal), "book")
        for row in books
    ]
    exchange_rows.extend(
        (int(row.exchange_ts_ns), int(row.total_order_ordinal), "trade")
        for row in trades
    )
    exchange_rows.extend(
        (int(row.exchange_ts_ns), int(row.total_order_ordinal), "fill")
        for row in fills
    )
    _validate_tied_total_order(exchange_rows, clock="exchange")


def _validate_tied_total_order(
    rows: Sequence[tuple[int, int, str]],
    *,
    clock: str,
) -> None:
    grouped: dict[int, list[tuple[int, str]]] = {}
    for timestamp, ordinal, kind in rows:
        grouped.setdefault(int(timestamp), []).append((int(ordinal), kind))
    for timestamp, tied in grouped.items():
        if len(tied) <= 1:
            continue
        ordinals = [ordinal for ordinal, _ in tied]
        if any(ordinal <= 0 for ordinal in ordinals) or len(ordinals) != len(
            set(ordinals)
        ):
            raise TransportContractError(
                f"ambiguous_same_timestamp_order:{clock}:{timestamp}"
            )


class ArmReplayTransport:
    """Arm-local mutable replay state backed by one immutable source bundle."""

    def __init__(
        self,
        *,
        bundle: ProspectiveReplayTransportBundle,
        arm: str,
        fill_events: tuple[ArmFillTruthEvent, ...],
        counterfactual_delay_artifact: PastOnlyPrivateFillDelayArtifact | None,
    ) -> None:
        self.arm = str(arm).strip()
        if self.arm not in {"control", "candidate"}:
            raise TransportContractError("transport arm must be control or candidate")
        self.bundle = bundle
        self._fills = tuple(fill_events)
        _validate_unique_ids(
            [fill.fill_event_id for fill in self._fills],
            "fill event id",
            allow_zero=True,
        )
        self._validate_partial_fill_sequences()
        _validate_stream_ordering(bundle.book_events, bundle.trade_events, self._fills)

        self._truth_book = HistoricalExchangeBookScheduler((), strict_sequence=True)
        self._visible_book = HistoricalExchangeBookVisibilityScheduler(
            strict_sequence=True
        )
        assigned_book_ready: dict[int, int] = {}
        for row in sorted(
            bundle.book_events,
            key=lambda value: (
                int(value.event.exchange_ts_ns),
                int(value.total_order_ordinal),
            ),
        ):
            assigned_book_ready[id(row)] = self._visible_book.enqueue(
                row.event,
                feature_ready_ts_ns=int(row.feature_ready_ts_ns),
            )

        self._resolved_fills: dict[str, ResolvedPrivateFillVisibility] = {}
        self._censored_fills: dict[str, str] = {}
        for fill in self._fills:
            resolved = self._resolve_private_fill(
                fill,
                counterfactual_delay_artifact,
            )
            if resolved is None:
                self._censored_fills[fill.fill_event_id] = (
                    "counterfactual_private_fill_delay_unsupported"
                )
            else:
                self._resolved_fills[fill.fill_event_id] = resolved

        self._truth_events = self._build_truth_events()
        self._visible_events = self._build_visible_events(assigned_book_ready)
        _validate_tied_total_order(
            [
                (event.ready_ts_ns, event.total_order_ordinal, event.kind)
                for event in self._visible_events
            ],
            clock="feature_ready",
        )
        self._validate_visible_book_ties()

        self._truth_cursor = 0
        self._visible_cursor = 0
        self._truth_boundary_ns = 0
        self._visible_boundary_ns = 0
        self._truth_fill_ids: set[str] = set()
        self._visible_fill_ids: set[str] = set()
        self._visible_trades: list[RecordedTradeVisibility] = []
        self._visible_inventory_btc = 0.0
        self._visible_cash_usdc = 0.0
        self._last_private_fill_id: str | None = None
        self._source_gap_count = 0
        self._callback: Callable[[ArmFillTruthEvent], None] | None = None
        self._delay_artifact_sha256 = (
            counterfactual_delay_artifact.artifact_sha256
            if counterfactual_delay_artifact is not None
            else None
        )

    def _validate_partial_fill_sequences(self) -> None:
        prior: dict[str, tuple[int, int]] = {}
        for fill in sorted(
            self._fills,
            key=lambda row: (row.exchange_ts_ns, row.total_order_ordinal),
        ):
            previous = prior.get(fill.lifecycle_id)
            current = (fill.partial_fill_ordinal, fill.lifecycle_sequence)
            if previous is not None and (
                current[0] <= previous[0] or current[1] <= previous[1]
            ):
                raise TransportContractError(
                    "partial fills must preserve lifecycle and ordinal order"
                )
            prior[fill.lifecycle_id] = current

    @staticmethod
    def _resolve_private_fill(
        fill: ArmFillTruthEvent,
        artifact: PastOnlyPrivateFillDelayArtifact | None,
    ) -> ResolvedPrivateFillVisibility | None:
        if fill.recorded_receive_ts_ns is not None:
            return ResolvedPrivateFillVisibility(
                fill_event_id=fill.fill_event_id,
                receive_ts_ns=int(fill.recorded_receive_ts_ns),
                feature_ready_ts_ns=int(fill.recorded_feature_ready_ts_ns or 0),
                authority="recorded_exact",
                delay_artifact_sha256=None,
            )
        if artifact is None:
            return None
        return artifact.resolve(fill)

    def _build_truth_events(self) -> tuple[_TruthEnvelope, ...]:
        rows = [
            _TruthEnvelope(
                ts_ns=int(row.event.exchange_ts_ns),
                total_order_ordinal=int(row.total_order_ordinal),
                kind="book",
                payload=row,
            )
            for row in self.bundle.book_events
        ]
        rows.extend(
            _TruthEnvelope(
                ts_ns=int(row.exchange_ts_ns),
                total_order_ordinal=int(row.total_order_ordinal),
                kind="trade",
                payload=row,
            )
            for row in self.bundle.trade_events
        )
        rows.extend(
            _TruthEnvelope(
                ts_ns=int(row.exchange_ts_ns),
                total_order_ordinal=int(row.total_order_ordinal),
                kind="fill",
                payload=row,
            )
            for row in self._fills
        )
        return tuple(sorted(rows, key=lambda row: (row.ts_ns, row.total_order_ordinal)))

    def _build_visible_events(
        self,
        assigned_book_ready: Mapping[int, int],
    ) -> tuple[_VisibleEnvelope, ...]:
        rows = [
            _VisibleEnvelope(
                ready_ts_ns=int(assigned_book_ready[id(row)]),
                total_order_ordinal=int(row.total_order_ordinal),
                kind="book",
                payload=row,
            )
            for row in self.bundle.book_events
        ]
        rows.extend(
            _VisibleEnvelope(
                ready_ts_ns=int(row.feature_ready_ts_ns),
                total_order_ordinal=int(row.total_order_ordinal),
                kind="trade",
                payload=row,
            )
            for row in self.bundle.trade_events
        )
        for fill in self._fills:
            resolved = self._resolved_fills.get(fill.fill_event_id)
            if resolved is None:
                continue
            rows.append(
                _VisibleEnvelope(
                    ready_ts_ns=int(resolved.feature_ready_ts_ns),
                    total_order_ordinal=int(fill.total_order_ordinal),
                    kind="fill",
                    payload=fill,
                )
            )
        return tuple(
            sorted(rows, key=lambda row: (row.ready_ts_ns, row.total_order_ordinal))
        )

    def _validate_visible_book_ties(self) -> None:
        grouped: dict[int, list[_VisibleEnvelope]] = {}
        for event in self._visible_events:
            grouped.setdefault(event.ready_ts_ns, []).append(event)
        for timestamp, rows in grouped.items():
            kinds = {row.kind for row in rows}
            if "book" in kinds and len(kinds) > 1:
                raise TransportContractError(
                    "cross-stream feature-ready tie cannot be represented without "
                    f"inventing order:{timestamp}"
                )

    def set_private_fill_callback(
        self,
        callback: Callable[[ArmFillTruthEvent], None] | None,
    ) -> None:
        self._callback = callback

    def advance_exchange_to(self, exchange_ts_ns: int) -> None:
        target = _positive_int(exchange_ts_ns, "exchange replay boundary")
        if target < self._truth_boundary_ns:
            raise TransportContractError("exchange replay clock regressed")
        while self._truth_cursor < len(self._truth_events):
            envelope = self._truth_events[self._truth_cursor]
            if envelope.ts_ns > target:
                break
            if envelope.kind == "book":
                row = envelope.payload
                assert isinstance(row, RecordedBookVisibility)
                try:
                    self._truth_book.apply_scheduled_events(
                        (row.event,),
                        boundary_ts_ns=int(row.event.exchange_ts_ns),
                        inclusive=True,
                    )
                except ValueError as exc:
                    if row.event.event_type == "source_gap":
                        self._source_gap_count += 1
                    raise TransportContractError(
                        "exchange-truth book source is not formally usable"
                    ) from exc
            elif envelope.kind == "fill":
                fill = envelope.payload
                assert isinstance(fill, ArmFillTruthEvent)
                self._truth_fill_ids.add(fill.fill_event_id)
            self._truth_cursor += 1
        self._truth_book.apply_scheduled_events(
            (),
            boundary_ts_ns=target,
            inclusive=True,
        )
        self._truth_boundary_ns = target

    def advance_strategy_to(self, feature_ready_ts_ns: int) -> VisibleStrategyState:
        target = _positive_int(feature_ready_ts_ns, "strategy-visible boundary")
        if target < self._visible_boundary_ns:
            raise TransportContractError("strategy-visible replay clock regressed")
        self.advance_exchange_to(target)
        delivered_book_ready: set[int] = set()
        while self._visible_cursor < len(self._visible_events):
            envelope = self._visible_events[self._visible_cursor]
            if envelope.ready_ts_ns > target:
                break
            if envelope.kind == "book":
                if envelope.ready_ts_ns not in delivered_book_ready:
                    self._visible_book.advance_to(
                        envelope.ready_ts_ns,
                        inclusive=True,
                    )
                    delivered_book_ready.add(envelope.ready_ts_ns)
            elif envelope.kind == "trade":
                trade = envelope.payload
                assert isinstance(trade, RecordedTradeVisibility)
                self._visible_trades.append(trade)
            else:
                fill = envelope.payload
                assert isinstance(fill, ArmFillTruthEvent)
                if fill.fill_event_id not in self._truth_fill_ids:
                    raise TransportContractError(
                        "private fill became visible before exchange fill truth"
                    )
                signed = fill.quantity if fill.side.upper() == "BUY" else -fill.quantity
                self._visible_inventory_btc += signed
                self._visible_cash_usdc -= signed * fill.price
                self._visible_fill_ids.add(fill.fill_event_id)
                self._last_private_fill_id = fill.fill_event_id
                if self._callback is not None:
                    self._callback(fill)
            self._visible_cursor += 1
        self._visible_book.advance_to(target, inclusive=True)
        self._visible_boundary_ns = target
        return self.visible_state()

    def visible_state(self) -> VisibleStrategyState:
        return VisibleStrategyState(
            asof_feature_ready_ts_ns=int(self._visible_boundary_ns),
            inventory_btc=float(self._visible_inventory_btc),
            cash_usdc_before_fees=float(self._visible_cash_usdc),
            public_trade_count=len(self._visible_trades),
            private_fill_callback_count=len(self._visible_fill_ids),
            last_public_trade_id=(
                self._visible_trades[-1].event_id if self._visible_trades else None
            ),
            last_private_fill_event_id=self._last_private_fill_id,
        )

    def truth_top_levels(
        self,
        count: int = 1,
    ) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        return self._truth_book.top_levels(count)

    def visible_top_levels(
        self,
        count: int = 1,
    ) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        return self._visible_book.top_levels(count)

    def transport_receipt(self) -> TransportReceipt:
        visibility_stats = self._visible_book.stats()
        book_stats = self._truth_book.stats()
        pending = len(self._truth_fill_ids - self._visible_fill_ids)
        authorities = {
            resolved.authority for resolved in self._resolved_fills.values()
        }
        if self._censored_fills:
            authority = "incomplete_censored"
        elif not authorities:
            authority = "none"
        elif len(authorities) == 1:
            authority = next(iter(authorities))
        else:
            authority = "mixed"
        exclusions: list[str] = []
        if self._censored_fills:
            exclusions.append("counterfactual_private_fill_delay_unsupported")
        if self._source_gap_count or int(book_stats.source_gap_events):
            exclusions.append("source_gap")
        if int(visibility_stats.pre_exchange_clamped_events):
            exclusions.append("pre_exchange_visibility_clamp")
        if pending:
            exclusions.append("pending_private_fill_callbacks")
        if self._truth_cursor != len(self._truth_events):
            exclusions.append("exchange_truth_not_fully_consumed")
        if self._visible_cursor != len(self._visible_events):
            exclusions.append("strategy_visibility_not_fully_consumed")
        body: dict[str, Any] = {
            "schema_version": TRANSPORT_RECEIPT_SCHEMA_VERSION,
            "identity": TRANSPORT_IDENTITY,
            "arm": self.arm,
            "common_market_source_sha256": self.bundle.common_market_source_sha256,
            "arm_fill_source_sha256": _canonical_sha256(
                [asdict(fill) for fill in self._fills]
            ),
            "delay_artifact_sha256": self._delay_artifact_sha256,
            "private_fill_visibility_authority": authority,
            "book_event_count": len(self.bundle.book_events),
            "book_visible_count": int(visibility_stats.delivered_events),
            "trade_event_count": len(self.bundle.trade_events),
            "trade_visible_count": len(self._visible_trades),
            "fill_truth_count": len(self._truth_fill_ids),
            "private_fill_visible_count": len(self._visible_fill_ids),
            "counterfactual_fill_censored_count": len(self._censored_fills),
            "source_gap_count": max(
                self._source_gap_count,
                int(book_stats.source_gap_events),
            ),
            "pre_exchange_clamp_count": int(
                visibility_stats.pre_exchange_clamped_events
            ),
            "head_of_line_clamp_count": int(
                visibility_stats.head_of_line_clamped_events
            ),
            "clock_inversion_count": 0,
            "future_visibility_violation_count": 0,
            "ambiguous_same_timestamp_count": 0,
            "pending_private_fill_count": pending,
            "formal_replay_support_valid": not exclusions,
            "live_equivalent": False,
            "thread_interleaving_replayed": False,
            "rest_user_stream_reconnect_replayed": False,
            "action_authorized": False,
            "live_policy_authorized": False,
            "exclusion_reasons": tuple(dict.fromkeys(exclusions)),
        }
        return TransportReceipt(
            **body,
            transport_receipt_sha256=_canonical_sha256(body),
        )


def validate_transport_receipt(
    payload: Mapping[str, Any],
    *,
    expected_arm: str,
    expected_common_market_source_sha256: str,
) -> TransportReceipt:
    """Validate a simulator receipt before its economics enter a formal row."""

    expected = {field.name for field in TransportReceipt.__dataclass_fields__.values()}
    if set(payload) != expected:
        raise TransportContractError("transport receipt field set drifted")
    receipt = TransportReceipt(**{name: payload[name] for name in expected})
    if receipt.arm != expected_arm:
        raise TransportContractError("transport receipt arm identity drifted")
    if receipt.common_market_source_sha256 != _require_sha256(
        expected_common_market_source_sha256,
        "expected common market source",
    ):
        raise TransportContractError("paired transport source identity drifted")
    return receipt


def latest_transport_timestamp(
    *,
    bundle: ProspectiveReplayTransportBundle,
    fills: Sequence[ArmFillTruthEvent],
    delay_artifact: PastOnlyPrivateFillDelayArtifact | None,
) -> int:
    """Return a mechanics-only drain boundary without inventing an expiry."""

    clocks = [
        *(row.feature_ready_ts_ns for row in bundle.book_events),
        *(row.feature_ready_ts_ns for row in bundle.trade_events),
    ]
    for fill in fills:
        if fill.recorded_feature_ready_ts_ns is not None:
            clocks.append(fill.recorded_feature_ready_ts_ns)
            continue
        if delay_artifact is not None:
            resolved = delay_artifact.resolve(fill)
            if resolved is not None:
                clocks.append(resolved.feature_ready_ts_ns)
    if not clocks:
        raise TransportContractError("transport bundle has no visible clock")
    return max(int(value) for value in clocks)
