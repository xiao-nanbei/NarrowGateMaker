"""Shared tick-replay data container types."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass(frozen=True)
class HistoricalBBOData:
    """Time-sorted BBO arrays aligned by millisecond timestamps.

    The containers are intentionally dumb/immutable: slicing and monotonic
    cursor logic live in `models.data_windows` and C++ replay.
    """

    ts_ms: np.ndarray
    best_bid: np.ndarray
    best_ask: np.ndarray
    bid_qty: np.ndarray
    ask_qty: np.ndarray
    source: str = "bbo"


@dataclass(frozen=True)
class HistoricalL2Data:
    """Top-N L2 snapshot matrix for exact-level queue/fill diagnostics."""

    ts_ms: np.ndarray
    bid_px: np.ndarray
    bid_qty: np.ndarray
    ask_px: np.ndarray
    ask_qty: np.ndarray
    source: str = "l2"


@dataclass(frozen=True)
class HistoricalExchangeBookEvent:
    """One native exchange-time order-book snapshot or delta message.

    This stream is deliberately independent of strategy orders.  It carries
    the exchange sequence and every price-level update in the source message;
    the replay scheduler owns reconstruction and orders may only query its
    state after the event has been scheduled.
    """

    market_id: str
    event_type: str
    exchange_ts_ns: int
    exchange_ts_source: str = "unknown"
    local_receive_ts_ns: int = 0
    event_time_ns: int = 0
    transaction_time_ns: int = 0
    first_update_id: int | None = None
    final_update_id: int | None = None
    previous_final_update_id: int | None = None
    last_update_id: int | None = None
    levels: tuple[tuple[str, int, float], ...] = ()
    source: str = ""
    source_ordinal: int = 0

    def __post_init__(self) -> None:
        market_id = str(self.market_id).strip()
        event_type = str(self.event_type).strip().lower()
        exchange_ts_source = str(self.exchange_ts_source).strip().lower()
        if event_type == "update":
            event_type = "delta"
        if not market_id:
            raise ValueError("historical exchange-book event requires market_id")
        if event_type not in {"snapshot", "delta", "source_gap"}:
            raise ValueError(
                "historical exchange-book event_type must be snapshot, "
                f"delta, or source_gap; got {self.event_type!r}"
            )
        if int(self.exchange_ts_ns) <= 0:
            raise ValueError(
                "historical exchange-book event requires exchange_ts_ns > 0"
            )
        if exchange_ts_source not in {
            "transaction",
            "event",
            "receive",
            "source_gap",
            "unknown",
        }:
            raise ValueError(
                "unsupported exchange-book timestamp source: "
                f"{self.exchange_ts_source!r}"
            )

        normalized_levels: list[tuple[str, int, float]] = []
        for raw_side, raw_tick, raw_quantity in self.levels:
            side = str(raw_side).strip().lower()
            if side in {"buy", "bid"}:
                side = "bid"
            elif side in {"sell", "ask"}:
                side = "ask"
            else:
                raise ValueError(
                    f"unsupported exchange-book level side={raw_side!r}"
                )
            tick = int(raw_tick)
            quantity = float(raw_quantity)
            if tick <= 0 or not np.isfinite(quantity) or quantity < 0.0:
                raise ValueError(
                    "exchange-book levels require positive integer ticks and "
                    "finite non-negative quantities"
                )
            normalized_levels.append((side, tick, quantity))
        if event_type != "source_gap" and not normalized_levels:
            raise ValueError(
                "snapshot/delta exchange-book events require at least one level"
            )
        if event_type == "source_gap" and normalized_levels:
            raise ValueError("source_gap exchange-book events cannot contain levels")

        object.__setattr__(self, "market_id", market_id)
        object.__setattr__(self, "event_type", event_type)
        object.__setattr__(
            self,
            "exchange_ts_source",
            exchange_ts_source,
        )
        object.__setattr__(self, "levels", tuple(normalized_levels))
        object.__setattr__(self, "source", str(self.source))

    @property
    def exchange_ts_ms(self) -> int:
        return int(self.exchange_ts_ns) // 1_000_000

    @property
    def sort_key(self) -> tuple[int, int, int]:
        sequence = next(
            (
                int(value)
                for value in (
                    self.final_update_id,
                    self.last_update_id,
                    self.first_update_id,
                )
                if value is not None
            ),
            -1,
        )
        return (
            int(self.exchange_ts_ns),
            sequence,
            int(self.source_ordinal),
        )


@dataclass(frozen=True)
class HistoricalReferenceEvent:
    """One causally visible cross-venue market-data event.

    ``feature_ready_ts_ns`` is the replay visibility boundary.  Exchange and
    local receive timestamps remain attached for latency diagnostics, but an
    event must never reach policy state before its feature-ready timestamp.
    """

    market_id: str
    event_type: str
    exchange_event_ts_ns: int
    local_receive_ts_ns: int
    feature_ready_ts_ns: int
    sequence_number: int = 0
    previous_sequence_number: int = 0
    gap_flag: bool | None = None
    transport: str = "unknown"
    bid: float = 0.0
    bid_size: float = 0.0
    ask: float = 0.0
    ask_size: float = 0.0
    price: float = 0.0
    size: float = 0.0
    aggressor_side: str = ""

    def __post_init__(self) -> None:
        market_id = str(self.market_id).strip()
        event_type = str(self.event_type).strip().lower()
        if not market_id:
            raise ValueError("historical reference event requires market_id")
        if event_type not in {"book", "trade"}:
            raise ValueError(
                f"unsupported historical reference event_type={self.event_type!r}"
            )
        if int(self.feature_ready_ts_ns) <= 0:
            raise ValueError("historical reference event requires feature_ready_ts_ns > 0")
        object.__setattr__(self, "market_id", market_id)
        object.__setattr__(self, "event_type", event_type)
        object.__setattr__(self, "transport", str(self.transport or "unknown"))
        object.__setattr__(self, "aggressor_side", str(self.aggressor_side).lower())

    @property
    def sort_key(self) -> tuple[int, str, int]:
        return (
            int(self.feature_ready_ts_ns),
            self.market_id,
            int(self.sequence_number),
        )

    @classmethod
    def from_mapping(
        cls,
        row: Mapping[str, Any],
        *,
        ready_key: str = "feature_ready_ts_ns",
    ) -> HistoricalReferenceEvent:
        """Normalize one recorder/audit row without changing its visibility time."""

        def _int(name: str, default: int = 0) -> int:
            try:
                return int(row.get(name, default) or default)
            except (TypeError, ValueError):
                return default

        def _float(name: str, default: float = 0.0) -> float:
            try:
                return float(row.get(name, default) or default)
            except (TypeError, ValueError):
                return default

        gap_value = row.get("gap_flag")
        if gap_value is None or gap_value == "":
            gap_flag = None
        elif isinstance(gap_value, str):
            gap_flag = gap_value.strip().lower() in {"1", "true", "yes", "y"}
        else:
            gap_flag = bool(gap_value)
        return cls(
            market_id=str(row.get("market_id", "")),
            event_type=str(row.get("event_type", "")),
            exchange_event_ts_ns=_int("exchange_event_ts_ns"),
            local_receive_ts_ns=_int("local_receive_ts_ns"),
            feature_ready_ts_ns=_int(ready_key),
            sequence_number=_int("sequence_number", _int("sequence")),
            previous_sequence_number=_int("previous_sequence_number"),
            gap_flag=gap_flag,
            transport=str(row.get("transport", "unknown")),
            bid=_float("bid"),
            bid_size=_float("bid_size"),
            ask=_float("ask"),
            ask_size=_float("ask_size"),
            price=_float("price"),
            size=_float("size", _float("qty")),
            aggressor_side=str(row.get("aggressor_side", row.get("side", ""))),
        )


@dataclass(frozen=True)
class HistoricalCampaignRepairData:
    """Causal post-fill repair probabilities available to replay policy."""

    ts_ns: np.ndarray
    probability: np.ndarray
    source: str = "campaign_repair_probability"

    def __post_init__(self) -> None:
        timestamps = np.asarray(self.ts_ns, dtype=np.int64)
        probabilities = np.asarray(self.probability, dtype=np.float64)
        if timestamps.ndim != 1 or probabilities.ndim != 1:
            raise ValueError("campaign repair arrays must be one-dimensional")
        if timestamps.size != probabilities.size:
            raise ValueError("campaign repair timestamps/probabilities length mismatch")
        if timestamps.size and np.any(np.diff(timestamps) < 0):
            raise ValueError("campaign repair timestamps must be sorted")
        if probabilities.size and (
            np.any(~np.isfinite(probabilities))
            or np.any(probabilities < 0.0)
            or np.any(probabilities > 1.0)
        ):
            raise ValueError("campaign repair probabilities must be finite in [0, 1]")
        object.__setattr__(self, "ts_ns", timestamps)
        object.__setattr__(self, "probability", probabilities)


@dataclass(frozen=True)
class HistoricalGlobalFlowData:
    """Causal right-edge global-flow states for second-scale replay.

    This is intentionally distinct from receive-time event tapes.  Each row is
    a state built from events in ``[t, t+1s)`` and becomes visible at ``t+1s``.
    It can test a post-fill campaign moderator, but it cannot establish a
    sub-second cancel/re-center edge.
    """

    ts_ns: np.ndarray
    spot_move_bps: np.ndarray
    perp_move_bps: np.ndarray
    spot_flow_pressure: np.ndarray
    perp_flow_pressure: np.ndarray
    spot_venue_agreement: np.ndarray
    perp_venue_agreement: np.ndarray
    fresh_spot_venues: np.ndarray
    fresh_perp_venues: np.ndarray
    local_bridge_move_bps: np.ndarray
    spot_source_age_ms: np.ndarray
    perp_source_age_ms: np.ndarray
    spot_valid: np.ndarray
    perp_valid: np.ndarray
    source_age_ms: np.ndarray
    source_horizon_ms: int = 1_000
    source: str = "causal_external_1s_right_edge"

    def __post_init__(self) -> None:
        timestamps = np.asarray(self.ts_ns, dtype=np.int64)
        if timestamps.ndim != 1:
            raise ValueError("historical global-flow timestamps must be one-dimensional")
        if timestamps.size and np.any(np.diff(timestamps) < 0):
            raise ValueError("historical global-flow timestamps must be sorted")
        object.__setattr__(self, "ts_ns", timestamps)
        float_fields = (
            "spot_move_bps",
            "perp_move_bps",
            "spot_flow_pressure",
            "perp_flow_pressure",
            "spot_venue_agreement",
            "perp_venue_agreement",
            "local_bridge_move_bps",
            "spot_source_age_ms",
            "perp_source_age_ms",
            "source_age_ms",
        )
        int_fields = (
            "fresh_spot_venues",
            "fresh_perp_venues",
            "spot_valid",
            "perp_valid",
        )
        for name in float_fields:
            values = np.asarray(getattr(self, name), dtype=np.float64)
            if values.ndim != 1 or values.size != timestamps.size:
                raise ValueError(f"historical global-flow field {name} length mismatch")
            object.__setattr__(self, name, values)
        for name in int_fields:
            values = np.asarray(getattr(self, name), dtype=np.int8)
            if values.ndim != 1 or values.size != timestamps.size:
                raise ValueError(f"historical global-flow field {name} length mismatch")
            object.__setattr__(self, name, values)
        if int(self.source_horizon_ms) <= 0:
            raise ValueError("historical global-flow source_horizon_ms must be positive")
