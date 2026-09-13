"""Causal receive-time scheduling for cross-venue tick replay."""

from __future__ import annotations

import heapq
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path

import numpy as np
import pandas as pd

from models.tick_data_types import (
    HistoricalCampaignRepairData,
    HistoricalGlobalFlowData,
    HistoricalReferenceEvent,
)
from strategy.global_flow import GlobalFlowEngine, GlobalFlowState


@dataclass(frozen=True)
class ReferenceSchedulerStats:
    consumed_events: int
    accepted_events: int
    rejected_events: int
    gap_events: int
    pending_tapes: int
    last_decision_ts_ns: int
    last_ready_ts_ns: int


class HistoricalReferenceScheduler:
    """Merge per-source tapes and expose only feature-ready events.

    Each input tape must already be monotonic by ``feature_ready_ts_ns``.  The
    scheduler keeps one event per tape in a heap, so policy visibility follows
    a deterministic k-way merge without loading a combined event list.
    """

    def __init__(
        self,
        tapes: Mapping[str, Iterable[HistoricalReferenceEvent]],
        *,
        engine: GlobalFlowEngine | None = None,
        allow_one_shot: bool = False,
    ) -> None:
        self.engine = engine or GlobalFlowEngine()
        ordered_tapes = sorted(tapes.items())
        self._iterators = []
        self._tape_names = []
        for name, events in ordered_tapes:
            iterator = iter(events)
            if iterator is events and not allow_one_shot:
                raise TypeError(
                    f"reference tape {name!r} is a one-shot iterator; formal paired "
                    "replay requires a re-iterable sequence/source"
                )
            self._tape_names.append(name)
            self._iterators.append(iterator)
        self._last_tape_ready_ns = [0] * len(self._iterators)
        self._ordinals = [0] * len(self._iterators)
        self._heap: list[tuple[int, int, int, HistoricalReferenceEvent]] = []
        self._consumed = 0
        self._accepted = 0
        self._rejected = 0
        self._gap_events = 0
        self._last_decision_ns = 0
        self._last_ready_ns = 0
        for tape_index in range(len(self._iterators)):
            self._push_next(tape_index)

    def _push_next(self, tape_index: int) -> None:
        try:
            event = next(self._iterators[tape_index])
        except StopIteration:
            return
        if not isinstance(event, HistoricalReferenceEvent):
            raise TypeError(
                f"reference tape {self._tape_names[tape_index]!r} yielded "
                f"{type(event).__name__}, expected HistoricalReferenceEvent"
            )
        ready_ns = int(event.feature_ready_ts_ns)
        if ready_ns < self._last_tape_ready_ns[tape_index]:
            raise ValueError(
                f"reference tape {self._tape_names[tape_index]!r} is not sorted: "
                f"{ready_ns} < {self._last_tape_ready_ns[tape_index]}"
            )
        self._last_tape_ready_ns[tape_index] = ready_ns
        ordinal = self._ordinals[tape_index]
        self._ordinals[tape_index] += 1
        heapq.heappush(self._heap, (ready_ns, tape_index, ordinal, event))

    def advance_to(self, decision_ts_ns: int) -> int:
        """Consume all events visible at or before a replay decision timestamp."""

        decision_ns = int(decision_ts_ns)
        if decision_ns < self._last_decision_ns:
            raise ValueError(
                f"reference replay decision time regressed: "
                f"{decision_ns} < {self._last_decision_ns}"
            )
        self._last_decision_ns = decision_ns
        consumed_now = 0
        while self._heap and self._heap[0][0] <= decision_ns:
            ready_ns, tape_index, _, event = heapq.heappop(self._heap)
            accepted = self._ingest(event)
            self._consumed += 1
            self._accepted += int(accepted)
            self._rejected += int(not accepted)
            self._gap_events += int(event.gap_flag is True)
            self._last_ready_ns = max(self._last_ready_ns, ready_ns)
            consumed_now += 1
            self._push_next(tape_index)
        return consumed_now

    def _ingest(self, event: HistoricalReferenceEvent) -> bool:
        # Policy becomes aware of the event at feature-ready time.  Using an
        # earlier exchange/local timestamp inside GlobalFlowEngine would make
        # window membership depend on information that was not yet available.
        ready_ns = int(event.feature_ready_ts_ns)
        if event.event_type == "book":
            return bool(
                self.engine.on_book(
                    event.market_id,
                    receive_ts_ns=ready_ns,
                    bid=event.bid,
                    bid_size=event.bid_size,
                    ask=event.ask,
                    ask_size=event.ask_size,
                    gap_flag=event.gap_flag,
                )
            )
        return bool(
            self.engine.on_trade(
                event.market_id,
                receive_ts_ns=ready_ns,
                exchange_ts_ns=event.exchange_event_ts_ns,
                price=event.price,
                size=event.size,
                aggressor_side=event.aggressor_side,
            )
        )

    def snapshot(self, *, now_ns: int | None = None) -> GlobalFlowState:
        query_ns = self._last_decision_ns if now_ns is None else int(now_ns)
        if query_ns < self._last_decision_ns:
            raise ValueError("reference snapshot cannot look behind replay decision time")
        return self.engine.snapshot(now_ns=query_ns)

    def stats(self) -> ReferenceSchedulerStats:
        return ReferenceSchedulerStats(
            consumed_events=self._consumed,
            accepted_events=self._accepted,
            rejected_events=self._rejected,
            gap_events=self._gap_events,
            pending_tapes=len({item[1] for item in self._heap}),
            last_decision_ts_ns=self._last_decision_ns,
            last_ready_ts_ns=self._last_ready_ns,
        )


class CampaignRepairCursor:
    """As-of cursor for a causal campaign-repair probability series."""

    def __init__(self, data: HistoricalCampaignRepairData) -> None:
        self._ts = data.ts_ns
        self._probability = data.probability

    def asof(
        self,
        decision_ts_ns: int,
        *,
        lookback_ms: int,
        max_age_ms: int,
    ) -> tuple[float, float, float]:
        """Return current probability, lookback change, and source age in ms."""

        now_ns = int(decision_ts_ns)
        index = int(np.searchsorted(self._ts, now_ns, side="right") - 1)
        if index < 0:
            return math.nan, math.nan, math.inf
        age_ms = (now_ns - int(self._ts[index])) / 1_000_000.0
        if age_ms < 0.0 or age_ms > max(0, int(max_age_ms)):
            return math.nan, math.nan, age_ms
        current = float(self._probability[index])
        prior_ns = now_ns - max(0, int(lookback_ms)) * 1_000_000
        prior_index = int(np.searchsorted(self._ts, prior_ns, side="right") - 1)
        change = (
            current - float(self._probability[prior_index])
            if prior_index >= 0
            else math.nan
        )
        return current, change, age_ms


class HistoricalGlobalFlowCursor:
    """As-of cursor for causal right-edge external-market states."""

    def __init__(self, data: HistoricalGlobalFlowData, *, max_age_ms: int = 1_500) -> None:
        self.data = data
        self.max_age_ms = max(1, int(max_age_ms))

    def asof(self, decision_ts_ns: int, *, horizon_ms: int) -> GlobalFlowState:
        now_ns = int(decision_ts_ns)
        index = int(np.searchsorted(self.data.ts_ns, now_ns, side="right") - 1)
        if index < 0:
            return GlobalFlowState("global_flow.v1", now_ns, {})
        state_age_ms = (now_ns - int(self.data.ts_ns[index])) / 1_000_000.0
        stale = state_age_ms < 0.0 or state_age_ms > self.max_age_ms

        def _finite_median(values: tuple[float, ...]) -> float:
            clean = [value for value in values if math.isfinite(value)]
            return float(np.median(clean)) if clean else math.nan

        spot_move = float(self.data.spot_move_bps[index])
        perp_move = float(self.data.perp_move_bps[index])
        spot_pressure = float(self.data.spot_flow_pressure[index])
        perp_pressure = float(self.data.perp_flow_pressure[index])
        bridge_move = float(self.data.local_bridge_move_bps[index])
        spot_age_ms = float(self.data.spot_source_age_ms[index]) + max(
            0.0, state_age_ms
        )
        perp_age_ms = float(self.data.perp_source_age_ms[index]) + max(
            0.0, state_age_ms
        )
        spot_valid = (
            bool(self.data.spot_valid[index])
            and not stale
            and math.isfinite(spot_age_ms)
            and spot_age_ms <= self.max_age_ms
        )
        perp_valid = (
            bool(self.data.perp_valid[index])
            and not stale
            and math.isfinite(perp_age_ms)
            and perp_age_ms <= self.max_age_ms
        )

        def _factor(
            market_type: str,
            *,
            valid: bool,
            move: float,
            pressure: float,
            agreement: float,
            fresh: int,
            source_age_ms: float,
        ) -> dict[str, object]:
            return {
                "market_type": market_type,
                "valid": int(valid),
                "fresh_venues": int(fresh),
                "venue_agreement": float(agreement),
                "mid_move_bps": move,
                "dispersion_bps": math.nan,
                "flow_pressure": pressure,
                "trade_imbalance": pressure,
                "source_horizon_ms": int(self.data.source_horizon_ms),
                "state_age_ms": max(0.0, state_age_ms),
                "source_age_ms": source_age_ms,
            }

        spot = _factor(
            "spot",
            valid=spot_valid,
            move=spot_move,
            pressure=spot_pressure,
            agreement=float(self.data.spot_venue_agreement[index]),
            fresh=int(self.data.fresh_spot_venues[index]),
            source_age_ms=spot_age_ms,
        )
        perp = _factor(
            "perp",
            valid=perp_valid,
            move=perp_move,
            pressure=perp_pressure,
            agreement=float(self.data.perp_venue_agreement[index]),
            fresh=int(self.data.fresh_perp_venues[index]),
            source_age_ms=perp_age_ms,
        )
        global_move = _finite_median(
            tuple(
                value
                for value, valid in ((spot_move, spot_valid), (perp_move, perp_valid))
                if valid
            )
        )
        global_pressure = _finite_median(
            tuple(
                value
                for value, valid in (
                    (spot_pressure, spot_valid),
                    (perp_pressure, perp_valid),
                )
                if valid
            )
        )
        window = {
            "horizon_ms": int(horizon_ms),
            "source_horizon_ms": int(self.data.source_horizon_ms),
            "state_age_ms": max(0.0, state_age_ms),
            "spot": spot,
            "perp": perp,
            "global_mid_move_bps": global_move,
            "global_flow_pressure": global_pressure,
            "perp_minus_spot_move_bps": (
                perp_move - spot_move
                if math.isfinite(perp_move) and math.isfinite(spot_move)
                else math.nan
            ),
            "local_bridge_move_bps": bridge_move,
            "execution_move_bps": math.nan,
            "global_minus_bridge_bps": (
                global_move - bridge_move
                if math.isfinite(global_move) and math.isfinite(bridge_move)
                else math.nan
            ),
            "global_minus_execution_bps": math.nan,
            "valid": int(spot_valid or perp_valid),
        }
        return GlobalFlowState("global_flow.v1", now_ns, {int(horizon_ms): window})


def apply_global_flow_visibility_delay(
    data: HistoricalGlobalFlowData,
    delays_ms: float | np.ndarray,
    *,
    profile_id: str,
    mode: str,
) -> HistoricalGlobalFlowData:
    """Delay causal right-edge states under one labeled host profile.

    A scalar models a fixed profile quantile. An array supports a seeded
    empirical/stable-spike path. ``maximum.accumulate`` models a single
    serialized feature path: a callback stall can delay later states, but a
    later state cannot become visible by overtaking an earlier queued state.
    """

    raw_delays = np.asarray(delays_ms, dtype=np.float64)
    if raw_delays.ndim == 0:
        raw_delays = np.full(data.ts_ns.size, float(raw_delays), dtype=np.float64)
    if raw_delays.ndim != 1 or raw_delays.size != data.ts_ns.size:
        raise ValueError("global-flow visibility delays must be scalar or match rows")
    if np.any(~np.isfinite(raw_delays)) or np.any(raw_delays < 0.0):
        raise ValueError("global-flow visibility delays must be finite and non-negative")
    added_ns = np.rint(raw_delays * 1_000_000.0).astype(np.int64)
    visible_ns = np.maximum.accumulate(data.ts_ns + added_ns)
    suffix = (
        f"latency_profile={str(profile_id).strip() or 'unknown'};"
        f"mode={str(mode).strip() or 'unknown'}"
    )
    return replace(
        data,
        ts_ns=visible_ns,
        source=f"{data.source}|{suffix}",
    )


def load_causal_1s_global_flow(
    day: str,
    root: str | Path,
    *,
    max_source_age_ms: float = 1_500.0,
    global_reference_identity: str = (
        "global_reference_3venue_trade_bridge_v1"
    ),
) -> HistoricalGlobalFlowData:
    """Load one UTC day of three-venue spot/perp right-edge states.

    The input feature timestamps are the right edge of ``[t, t+1s)``.  The
    merge is backward-only with a bounded tolerance, so no future bar can enter
    the state used by a replay decision.
    """

    base = Path(root).expanduser()
    consensus = base / "consensus"
    spot_path = (
        consensus
        / "spot_3venue/BTCUSDT/features_1s"
        / f"BTCUSDT-bitget-bybit-okx-consensus-1s-{day}.parquet"
    )
    perp_path = (
        consensus
        / "perp_3venue/BTCUSDT/features_1s"
        / f"BTCUSDT-bitget-bybit-okx-consensus-1s-{day}.parquet"
    )
    global_path = (
        consensus
        / global_reference_identity
        / "BTCUSDC/features_1s"
        / f"BTCUSDC-global-reference-1s-{day}.parquet"
    )
    missing = [str(path) for path in (spot_path, perp_path, global_path) if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"missing causal 1s global-flow input for {day}: {missing}"
        )

    spot = pd.read_parquet(
        spot_path,
        columns=[
            "timestamp",
            "flow_imbalance",
            "agreement_score",
            "available_venues",
            "available_return_venues",
            "source_age_ms",
        ],
    ).rename(
        columns={
            "flow_imbalance": "spot_flow_pressure",
            "agreement_score": "spot_venue_agreement",
            "available_venues": "spot_available_venues",
            "available_return_venues": "spot_available_return_venues",
            "source_age_ms": "spot_source_age_ms",
        }
    )
    perp = pd.read_parquet(
        perp_path,
        columns=[
            "timestamp",
            "flow_imbalance",
            "agreement_score",
            "available_venues",
            "available_return_venues",
            "source_age_ms",
        ],
    ).rename(
        columns={
            "flow_imbalance": "perp_flow_pressure",
            "agreement_score": "perp_venue_agreement",
            "available_venues": "perp_available_venues",
            "available_return_venues": "perp_available_return_venues",
            "source_age_ms": "perp_source_age_ms",
        }
    )
    global_state = pd.read_parquet(
        global_path,
        columns=[
            "timestamp",
            "global_spot_move_bps",
            "fresh_spot_venues",
            "global_perp_move_bps",
            "fresh_perp_venues",
            "binance_local_move_bps",
        ],
    ).sort_values("timestamp")
    tolerance_ms = int(max(1_000.0, float(max_source_age_ms)))
    merged = pd.merge_asof(
        global_state,
        spot.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
        tolerance=tolerance_ms,
    )
    merged = pd.merge_asof(
        merged,
        perp.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
        tolerance=tolerance_ms,
    )

    def _numeric(name: str, default: float = math.nan) -> np.ndarray:
        if name not in merged:
            return np.full(len(merged), default, dtype=np.float64)
        return pd.to_numeric(merged[name], errors="coerce").to_numpy(dtype=np.float64)

    spot_move = _numeric("global_spot_move_bps")
    perp_move = _numeric("global_perp_move_bps")
    spot_pressure = _numeric("spot_flow_pressure")
    perp_pressure = _numeric("perp_flow_pressure")
    spot_agreement = _numeric("spot_venue_agreement", 0.0)
    perp_agreement = _numeric("perp_venue_agreement", 0.0)
    fresh_spot = _numeric("fresh_spot_venues", 0.0)
    fresh_perp = _numeric("fresh_perp_venues", 0.0)
    spot_age = _numeric("spot_source_age_ms", math.inf)
    perp_age = _numeric("perp_source_age_ms", math.inf)
    source_age = np.maximum(spot_age, perp_age)
    spot_valid = (
        np.isfinite(spot_move)
        & np.isfinite(spot_pressure)
        & (fresh_spot >= 2.0)
        & (spot_agreement >= 2.0 / 3.0)
        & (spot_age <= float(max_source_age_ms))
    )
    perp_valid = (
        np.isfinite(perp_move)
        & np.isfinite(perp_pressure)
        & (fresh_perp >= 2.0)
        & (perp_agreement >= 2.0 / 3.0)
        & (perp_age <= float(max_source_age_ms))
    )
    return HistoricalGlobalFlowData(
        ts_ns=_numeric("timestamp", 0.0).astype(np.int64) * 1_000_000,
        spot_move_bps=spot_move,
        perp_move_bps=perp_move,
        spot_flow_pressure=spot_pressure,
        perp_flow_pressure=perp_pressure,
        spot_venue_agreement=spot_agreement,
        perp_venue_agreement=perp_agreement,
        fresh_spot_venues=fresh_spot.astype(np.int8),
        fresh_perp_venues=fresh_perp.astype(np.int8),
        local_bridge_move_bps=_numeric("binance_local_move_bps"),
        spot_source_age_ms=spot_age,
        perp_source_age_ms=perp_age,
        spot_valid=spot_valid.astype(np.int8),
        perp_valid=perp_valid.astype(np.int8),
        source_age_ms=source_age,
        source_horizon_ms=1_000,
        source=(
            "three_venue_spot_perp_1s_right_edge|"
            f"{global_reference_identity}"
        ),
    )
