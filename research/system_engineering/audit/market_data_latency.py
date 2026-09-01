#!/usr/bin/env python3
"""Build and consume environment-labeled market-data latency profiles.

Profiles measure when public market events became feature-visible on one live
host. They are suitable for exchange-time historical replay, but they are not
pure one-way network latency because exchange clock offsets are embedded in
``local_receive_ts - exchange_event_ts``.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.system_engineering.audit.receive_time_tape import (  # noqa: E402
    LatencyGroup,
    expand_inputs,
    iter_rows,
)

PROFILE_SCHEMA_VERSION = "market_data_latency_profile.v1"
SIMULATION_MODES = (
    "captured",
    "exchange_zero",
    "profile_p50",
    "profile_p95",
    "profile_p99",
    "profile_p999",
    "profile_max",
    "profile_empirical",
    "profile_stable_spike",
    "profile_source_stratified",
)
STABLE_CORE_QUANTILE = 0.95
STABLE_SPIKE_PROBABILITY = 0.005
STABLE_SPIKE_HIGH_QUANTILE = 0.99
DEFAULT_QUANTILE_PROBABILITIES = tuple(
    sorted({*(index / 100.0 for index in range(101)), 0.995, 0.999})
)
SOURCE_STRATIFIED_SCHEMA_VERSION = "market_data_latency_source_stratified.v1"
SOURCE_STRATIFIED_EVENT_TYPES = ("book", "trade")


def _environment_pairs(values: Iterable[str]) -> dict[str, Any]:
    environment: dict[str, Any] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"environment field must be key=value, got {value!r}")
        key, raw = value.split("=", 1)
        key = key.strip()
        raw = raw.strip()
        if not key:
            raise ValueError("environment key cannot be empty")
        try:
            parsed: Any = json.loads(raw)
        except json.JSONDecodeError:
            parsed = raw
        environment[key] = parsed
    return environment


def build_latency_profile(
    paths: Iterable[Path],
    *,
    profile_id: str,
    environment: dict[str, Any],
    window_seconds: int = 3_600,
    end_receive_ns: int | None = None,
    max_samples: int = 200_000,
    transports: set[str] | None = None,
    market_ids: set[str] | None = None,
    quantile_probabilities: Iterable[float] = DEFAULT_QUANTILE_PROBABILITIES,
    source_windows: Iterable[tuple[str, Iterable[Path]]] | None = None,
    joint_bucket_ms: int = 1_000,
) -> dict[str, Any]:
    """Measure one wall-clock receive-time window and build replay CDFs."""
    paths = list(paths)
    normalized_windows = [
        (str(window_id), [Path(path) for path in window_paths])
        for window_id, window_paths in (source_windows or [])
    ]
    end_ns = int(end_receive_ns or time.time_ns())
    window_seconds = max(1, int(window_seconds))
    start_ns = end_ns - window_seconds * 1_000_000_000
    allowed_transports = {
        str(value).strip().lower() for value in (transports or set()) if str(value).strip()
    }
    allowed_markets = {
        str(value).strip() for value in (market_ids or set()) if str(value).strip()
    }
    groups: dict[tuple[str, str, str], LatencyGroup] = {}
    row_count = 0
    for row in iter_rows(
        paths,
        start_receive_ns=None if normalized_windows else start_ns,
        end_receive_ns=None if normalized_windows else end_ns,
    ):
        transport = str(row.get("transport", "unknown")).strip().lower()
        if allowed_transports and transport not in allowed_transports:
            continue
        market_id = str(row.get("market_id", "unknown"))
        if allowed_markets and market_id not in allowed_markets:
            continue
        if str(row.get("event_timestamp_source", "exchange")) != "exchange":
            continue
        key = (
            market_id,
            str(row.get("event_type", "unknown")),
            transport,
        )
        groups.setdefault(key, LatencyGroup(max_samples=max_samples)).add(row)
        row_count += 1

    probabilities = tuple(
        sorted({min(1.0, max(0.0, float(value))) for value in quantile_probabilities})
    )
    profile_groups: list[dict[str, Any]] = []
    for key in sorted(groups):
        group = groups[key]
        summary = group.summary(*key)
        raw_quantiles = group.visibility_ms.quantiles(probabilities)
        profile_groups.append(
            {
                **summary,
                "simulation_quantile_probabilities": list(probabilities),
                # Negative apparent latency is possible when exchange clocks
                # lead the host clock. It is reported above but cannot make a
                # replay event visible before its exchange timestamp.
                "simulation_visibility_lag_ms_quantiles": [
                    max(0.0, float(value)) for value in raw_quantiles
                ],
                "simulation_negative_lag_clamped": True,
            }
        )

    profile = {
        "schema": PROFILE_SCHEMA_VERSION,
        "profile_id": str(profile_id),
        "environment": dict(environment),
        "measurement": {
            "start_receive_ts_ns": start_ns,
            "end_receive_ts_ns": end_ns,
            "window_seconds": window_seconds,
            "row_count": row_count,
            "group_count": len(profile_groups),
            "source_file_count": len(paths),
            "transports": sorted(allowed_transports),
            "market_ids": sorted(allowed_markets),
        },
        "semantics": {
            "transport_lag_ms": (
                "local_receive_ts_ns - exchange_event_ts_ns; includes exchange-clock "
                "offset, public-network transport, websocket delivery, and callback scheduling"
            ),
            "feature_latency_us": "feature_ready_ts_ns - local_receive_ts_ns",
            "visibility_lag_ms": "feature_ready_ts_ns - exchange_event_ts_ns",
            "captured_mode": "uses recorded feature_ready_ts_ns and injects no extra delay",
            "profile_modes": (
                "start from exchange_event_ts_ns and inject the selected environment profile"
            ),
        },
        "groups": profile_groups,
    }
    if normalized_windows:
        source_stratified = _build_source_stratified_sampling(
            normalized_windows,
            transports=allowed_transports,
            market_ids=allowed_markets,
            joint_bucket_ms=joint_bucket_ms,
        )
        profile["source_stratified_sampling"] = source_stratified
        profile["measurement"].update(
            {
                "start_receive_ts_ns": source_stratified[
                    "source_start_receive_ts_ns"
                ],
                "end_receive_ts_ns": source_stratified[
                    "source_end_receive_ts_ns"
                ],
                "independent_window_count": len(normalized_windows),
                "source_stratified_source_count": len(source_stratified["sources"]),
                "source_stratified_joint_sample_count": sum(
                    len(stratum["book_visibility_lag_ms_samples"])
                    for source in source_stratified["sources"]
                    for stratum in source["strata"]
                ),
            }
        )
    return profile


def _build_source_stratified_sampling(
    source_windows: Iterable[tuple[str, Iterable[Path]]],
    *,
    transports: set[str],
    market_ids: set[str],
    joint_bucket_ms: int,
) -> dict[str, Any]:
    """Build paired source states without event-count or busy-day weighting.

    Each input is one independently captured window. Events are first reduced
    to one median book/trade visibility pair per receive-time bucket. Replay
    then samples UTC day, window within day, and bucket within window uniformly.
    This retains within-source book/trade congestion while preventing a busy
    feed, a longer capture, or a day with more captures from dominating.
    """
    bucket_ms = max(1, int(joint_bucket_ms))
    bucket_ns = bucket_ms * 1_000_000
    buckets: dict[
        tuple[str, str, str, str, int],
        dict[str, list[float]],
    ] = {}
    source_start_receive_ns: int | None = None
    source_end_receive_ns: int | None = None
    for window_id, window_paths in source_windows:
        for row in iter_rows(list(window_paths)):
            transport = str(row.get("transport", "unknown")).strip().lower()
            if transports and transport not in transports:
                continue
            market_id = str(row.get("market_id", "unknown"))
            if market_ids and market_id not in market_ids:
                continue
            if str(row.get("event_timestamp_source", "exchange")) != "exchange":
                continue
            event_type = str(row.get("event_type", "unknown")).strip().lower()
            if event_type not in SOURCE_STRATIFIED_EVENT_TYPES:
                continue
            exchange_ns = int(row.get("exchange_event_ts_ns", 0) or 0)
            receive_ns = int(row.get("local_receive_ts_ns", 0) or 0)
            ready_ns = int(row.get("feature_ready_ts_ns", 0) or 0)
            if exchange_ns <= 0 or receive_ns <= 0 or ready_ns <= 0:
                continue
            source_start_receive_ns = (
                receive_ns
                if source_start_receive_ns is None
                else min(source_start_receive_ns, receive_ns)
            )
            source_end_receive_ns = (
                receive_ns
                if source_end_receive_ns is None
                else max(source_end_receive_ns, receive_ns)
            )
            visibility_lag_ms = max(0.0, (ready_ns - exchange_ns) / 1_000_000.0)
            if not math.isfinite(visibility_lag_ms):
                continue
            utc_day = datetime.fromtimestamp(
                receive_ns / 1_000_000_000.0,
                tz=UTC,
            ).date().isoformat()
            key = (
                market_id,
                transport,
                utc_day,
                str(window_id),
                receive_ns // bucket_ns,
            )
            bucket = buckets.setdefault(
                key,
                {event: [] for event in SOURCE_STRATIFIED_EVENT_TYPES},
            )
            bucket[event_type].append(float(visibility_lag_ms))

    strata: dict[
        tuple[str, str, str, str],
        dict[str, list[float]],
    ] = {}
    for key in sorted(buckets):
        market_id, transport, utc_day, window_id, _ = key
        bucket = buckets[key]
        if any(not bucket[event] for event in SOURCE_STRATIFIED_EVENT_TYPES):
            continue
        stratum = strata.setdefault(
            (market_id, transport, utc_day, window_id),
            {event: [] for event in SOURCE_STRATIFIED_EVENT_TYPES},
        )
        for event_type in SOURCE_STRATIFIED_EVENT_TYPES:
            stratum[event_type].append(float(np.median(bucket[event_type])))

    sources: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for (market_id, transport, utc_day, window_id), samples in sorted(strata.items()):
        sources.setdefault((market_id, transport), []).append(
            {
                "utc_day": utc_day,
                "window_id": window_id,
                "book_visibility_lag_ms_samples": samples["book"],
                "trade_visibility_lag_ms_samples": samples["trade"],
            }
        )
    return {
        "schema": SOURCE_STRATIFIED_SCHEMA_VERSION,
        "authority": "diagnostic_non_authoritative",
        "promotion_eligible": False,
        "joint_bucket_ms": bucket_ms,
        "weighting": "utc_day_equal_then_window_equal_then_bucket_equal",
        "joint_semantics": (
            "book and trade are medians from the same source receive-time bucket"
        ),
        "source_start_receive_ts_ns": int(source_start_receive_ns or 0),
        "source_end_receive_ts_ns": int(source_end_receive_ns or 0),
        "sources": [
            {
                "market_id": market_id,
                "transport": transport,
                "event_types": list(SOURCE_STRATIFIED_EVENT_TYPES),
                "strata": source_strata,
            }
            for (market_id, transport), source_strata in sorted(sources.items())
        ],
    }


def snapshot_conditioned_message_clocks(
    event_ts_ns: np.ndarray,
    capture_ts_ns: np.ndarray,
    observed_exchange_ts_ns: np.ndarray,
    observed_receive_ts_ns: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    """Interpolate one source's receive clock subject to observed visibility.

    This is a snapshot-conditioned diagnostic, not a recovered message tape.
    Receive time is interpolated between observed exchange/receive pairs. A
    later source message becomes ready only after the last capture still seeing
    its predecessor. Ready time is therefore an earliest-release bound; unknown
    callback work is neither measured nor borrowed from another feed. Long gaps
    between distinct observations are interpolated, not declared stream stalls.
    Call separately per source, including blocked snapshots, and supply boundary
    observations: timestamps outside observed exchange support are never held.
    """
    arrays = []
    for name, value in (
        ("event", event_ts_ns), ("capture", capture_ts_ns),
        ("observed exchange", observed_exchange_ts_ns),
        ("observed receive", observed_receive_ts_ns),
    ):
        vector = np.asarray(value)
        if vector.ndim != 1 or vector.dtype.kind not in "iu":
            raise ValueError(f"{name} timestamps must be one-dimensional integer vectors")
        if np.any(vector < 0) or np.any(vector > np.iinfo(np.int64).max):
            raise ValueError(f"{name} timestamps must fit non-negative int64 nanoseconds")
        vector = vector.astype(np.int64, copy=False)
        if np.any(vector[1:] < vector[:-1]):
            raise ValueError(f"{name} chronology regressed")
        arrays.append(vector)
    events, captures, exchange, observed_receive = arrays
    if not len(captures) or not (len(captures) == len(exchange) == len(observed_receive)):
        raise ValueError("snapshot timestamp vectors must be nonempty and aligned")
    if np.any(observed_receive < exchange) or np.any(observed_receive >= captures):
        raise ValueError("observations require exchange <= receive < capture")
    same_exchange = exchange[1:] == exchange[:-1]
    if np.any(same_exchange & (observed_receive[1:] != observed_receive[:-1])):
        raise ValueError("repeated exchange timestamp must retain its observed receive time")
    first = np.r_[0, np.flatnonzero(~same_exchange) + 1]
    last = np.r_[first[1:] - 1, len(exchange) - 1]
    source, received = exchange[first], observed_receive[first]
    first_capture, last_capture = captures[first], captures[last]
    if np.any(last_capture[:-1] >= first_capture[1:]):
        raise ValueError("successive source observations have contradictory capture bounds")
    if len(events) and (events[0] < source[0] or events[-1] > source[-1]):
        raise ValueError("target event time exceeds snapshot exchange support")

    right = np.searchsorted(source, events, side="left")
    left = np.maximum(right - 1, 0)
    between = source[right] != events
    receive = received[right].copy()
    spans = source[right[between]] - source[left[between]]
    # Subtract each bracket's origin before floating-point interpolation. Never
    # convert epoch nanoseconds to float or reinterpret visible age as transport.
    offsets = events[between] - source[left[between]]
    receive[between] = received[left[between]] + np.rint(
        offsets.astype(np.float64) / spans
        * (received[right[between]] - received[left[between]])
    ).astype(np.int64)
    receive = np.maximum(receive, events)
    ready = receive.copy()
    has_predecessor = right > 0
    ready[has_predecessor] = np.maximum(
        ready[has_predecessor], last_capture[right[has_predecessor] - 1] + 1,
    )
    if np.any(ready >= first_capture[right]):
        raise ValueError("interpolated readiness must precede its next observed capture")
    if np.any(receive[1:] < receive[:-1]) or np.any(ready[1:] < ready[:-1]):
        raise ValueError("snapshot-conditioned clocks must preserve source FIFO")
    hold = ready - receive
    stats = {
        "mode": "snapshot_conditioned_diagnostic",
        "callback_semantics": "earliest_release_bound_not_measured_callback_completion",
        "event_count": int(len(events)),
        "observation_count": int(len(captures)),
        "unique_exchange_count": int(len(source)),
        "repeated_observation_count": int(len(captures) - len(source)),
        "max_capture_gap_ns": int(np.diff(captures).max(initial=0)),
        "max_source_bracket_gap_ns": int(np.diff(source).max(initial=0)),
        "max_interpolation_gap_ns": int(spans.max(initial=0)),
        "barrier_adjusted_count": int(np.count_nonzero(hold)),
        "max_barrier_hold_ns": int(hold.max(initial=0)),
    }
    return receive, ready, stats


class RawWindowMarketDataLatencySimulator:
    """Replay historical lag chronology, not an exact target-host transport trace.

    A UTC-hour-keyed draw selects a day uniformly, then one of its windows
    uniformly. Sources share that window and phase zero. Within each source,
    previous-observation lag pairs form a step function on exchange-relative
    time; no rescaling, cycling, smoothing, independent message draws or HOL
    adjustment is performed. The caller owns causal delivery and any explicit
    BBO-as-depth approximation (``book`` here means bookTicker, never depth).
    """

    BLOCK_NS = 3_600_000_000_000

    def __init__(self, manifest_path: Path, *, seed: int, edge_policy: str = "error"):
        if edge_policy not in {"error", "hold"}:
            raise ValueError("edge_policy must be error or hold")
        path = Path(manifest_path)
        self.manifest = json.loads(path.read_text(encoding="utf-8"))
        if self.manifest.get("schema") != "ng_source_visibility_messages.v1":
            raise ValueError("unsupported raw-window latency schema")
        self.samples_path = Path(self.manifest["samples_path"])
        if not self.samples_path.is_absolute():
            self.samples_path = path.parent / self.samples_path
        self.seed = int(seed)
        self.edge_policy = edge_policy
        self._days: dict[str, list[dict[str, Any]]] = {}
        for window in self.manifest["windows"]:
            self._days.setdefault(str(window["utc_day"]), []).append(window)
        if not self._days:
            raise ValueError("raw-window latency manifest has no windows")
        self._day_keys = sorted(self._days)
        for windows in self._days.values():
            windows.sort(key=lambda item: str(item["array_prefix"]))
        self._cached_prefix = ""
        self._cached_sources: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
        self.diagnostics: dict[str, Any] = {
            "edge_policy": edge_policy,
            "phase_ns": 0,
            "blocks": {},
            "sources": {},
        }

    def _window(self, block: int) -> dict[str, Any]:
        rng = random.Random(f"{self.seed}:{block}")
        day = self._day_keys[rng.randrange(len(self._day_keys))]
        windows = self._days[day]
        window = windows[rng.randrange(len(windows))]
        self.diagnostics["blocks"][str(block * self.BLOCK_NS)] = {
            "utc_day": day,
            "array_prefix": window["array_prefix"],
            "source_epoch": window.get("source_epoch", "unspecified"),
        }
        return window

    def _source(self, window: dict[str, Any], source: str) -> tuple[np.ndarray, ...]:
        prefix = str(window["array_prefix"])
        if prefix != self._cached_prefix:
            self._cached_sources.clear()
            self._cached_prefix = prefix
        if source not in self._cached_sources:
            with np.load(self.samples_path, allow_pickle=False) as samples:
                select = samples[f"{prefix}_event_type"] == {"book": 1, "trade": 2}[source]
                values = [samples[f"{prefix}_{field}"][select] for field in (
                    "exchange_ts_ns", "receive_ts_ns", "ready_ts_ns", "timestamp_mask"
                )]
            exchange, receive, ready, mask = values
            if not exchange.size:
                raise ValueError(f"raw window {prefix} has no {source} samples")
            if any(value.ndim != 1 or value.dtype.kind not in "iu" for value in values):
                raise ValueError("raw timestamps and masks must be integer vectors")
            if np.any(mask != 15) or np.any(exchange <= 0):
                raise ValueError("raw window has missing or unaligned exchange timestamps")
            if np.any(receive < exchange) or np.any(ready < receive):
                raise ValueError("raw window has negative aligned-clock lag")
            if np.any(exchange[1:] < exchange[:-1]):
                raise ValueError("raw source exchange chronology regressed")
            axis = exchange.astype(np.int64) - int(window["relative_origin_receive_ns"])
            self._cached_sources[source] = (axis, receive - exchange, ready - receive)
        return self._cached_sources[source]

    def align(self, event_ts_ns: np.ndarray, *, source: str) -> tuple[np.ndarray, np.ndarray]:
        """Return receive/ready vectors; diagnostics count explicit edge holds.

        Calls may be chunked or sources interleaved without changing selection.
        Equal exchange timestamps use the last recorded row at that timestamp.
        Raw observations remain untouched, including unsupported negative lags.
        """
        if source not in {"book", "trade"}:
            raise ValueError("source must be book (bookTicker) or trade; depth is not measured")
        events = np.asarray(event_ts_ns)
        if events.ndim != 1 or events.dtype.kind not in "iu":
            raise ValueError("target events must be an integer timestamp vector")
        if np.any(events <= 0) or np.any(events > np.iinfo(np.int64).max):
            raise ValueError("target timestamps must be positive int64 values")
        events = events.astype(np.int64, copy=False)
        if np.any(events[1:] < events[:-1]):
            raise ValueError("target event chronology regressed")
        receive = np.empty_like(events)
        ready = np.empty_like(events)
        stats = self.diagnostics["sources"].setdefault(source, {
            "events": 0, "edge_held_events": 0, "max_extrapolation_ns": 0,
        })
        blocks = events // self.BLOCK_NS
        for block in np.unique(blocks):
            start, end = np.searchsorted(blocks, [block, block + 1])
            target = events[start:end]
            offset = target - int(block) * self.BLOCK_NS
            axis, transport, callback = self._source(self._window(int(block)), source)
            outside = (offset < axis[0]) | (offset > axis[-1])
            if outside.any() and self.edge_policy == "error":
                raise ValueError(f"target {source} time exceeds raw window support")
            indices = np.clip(np.searchsorted(axis, offset, side="right") - 1, 0, len(axis) - 1)
            lag = transport[indices] + callback[indices]
            if np.any(target > np.iinfo(np.int64).max - lag):
                raise ValueError("aligned timestamp overflow")
            receive[start:end] = target + transport[indices]
            ready[start:end] = target + lag
            stats["events"] += len(target)
            stats["edge_held_events"] += int(outside.sum())
            stats["max_extrapolation_ns"] = max(stats["max_extrapolation_ns"], int(max(
                0, axis[0] - offset[0], offset[-1] - axis[-1]
            )))
        return receive, ready


class MarketDataLatencySimulator:
    """Resolve feature visibility from a saved environment profile."""

    def __init__(self, profile: dict[str, Any]):
        if str(profile.get("schema", "")) != PROFILE_SCHEMA_VERSION:
            raise ValueError("unsupported market-data latency profile schema")
        self.profile = profile
        self.profile_id = str(profile.get("profile_id", "unknown"))
        self._exact: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._market_event: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._source_stratified: dict[
            tuple[str, str],
            dict[str, list[dict[str, Any]]],
        ] = {}
        for group in profile.get("groups", []):
            key = (
                str(group.get("market_id", "")),
                str(group.get("event_type", "")),
                str(group.get("transport", "unknown")).lower(),
            )
            self._exact[key] = group
            self._market_event.setdefault(key[:2], []).append(group)
        source_sampling = profile.get("source_stratified_sampling", {})
        if source_sampling:
            if str(source_sampling.get("schema", "")) != SOURCE_STRATIFIED_SCHEMA_VERSION:
                raise ValueError("unsupported source-stratified latency schema")
            for source in source_sampling.get("sources", []):
                by_day: dict[str, list[dict[str, Any]]] = {}
                for stratum in source.get("strata", []):
                    book = list(stratum.get("book_visibility_lag_ms_samples", []))
                    trade = list(stratum.get("trade_visibility_lag_ms_samples", []))
                    if not book or len(book) != len(trade):
                        raise ValueError(
                            "source-stratified book/trade samples must be paired"
                        )
                    by_day.setdefault(str(stratum.get("utc_day", "")), []).append(
                        {**stratum, "_book": book, "_trade": trade}
                    )
                if by_day:
                    key = (
                        str(source.get("market_id", "")),
                        str(source.get("transport", "unknown")).lower(),
                    )
                    self._source_stratified[key] = by_day

    @property
    def source_stratified_bucket_ms(self) -> int:
        sampling = self.profile.get("source_stratified_sampling", {})
        return max(1, int(sampling.get("joint_bucket_ms", 1_000) or 1_000))

    @staticmethod
    def _uniform_index(rng: random.Random, size: int) -> int:
        if size <= 0:
            raise ValueError("cannot sample an empty latency stratum")
        return min(size - 1, int(rng.random() * size))

    def _source_stratified_delay_ms(
        self,
        row: dict[str, Any],
        *,
        rng: random.Random,
    ) -> float:
        key = (
            str(row.get("market_id", "")),
            str(row.get("transport", "unknown")).lower(),
        )
        by_day = self._source_stratified.get(key)
        if not by_day:
            raise KeyError(
                "source-stratified latency profile has no source for "
                f"{key[0]} / {key[1]}"
            )
        days = sorted(by_day)
        day = days[self._uniform_index(rng, len(days))]
        windows = sorted(by_day[day], key=lambda item: str(item.get("window_id", "")))
        window = windows[self._uniform_index(rng, len(windows))]
        event_type = str(row.get("event_type", "")).strip().lower()
        if event_type not in SOURCE_STRATIFIED_EVENT_TYPES:
            raise KeyError(
                f"source-stratified latency has no event type {event_type!r}"
            )
        values = window[f"_{event_type}"]
        return max(0.0, float(values[self._uniform_index(rng, len(values))]))

    @classmethod
    def load(cls, path: Path) -> MarketDataLatencySimulator:
        return cls(json.loads(Path(path).read_text(encoding="utf-8")))

    def _group(self, row: dict[str, Any]) -> dict[str, Any] | None:
        key = (
            str(row.get("market_id", "")),
            str(row.get("event_type", "")),
            str(row.get("transport", "unknown")).lower(),
        )
        exact = self._exact.get(key)
        if exact is not None:
            return exact
        candidates = self._market_event.get(key[:2], [])
        return candidates[0] if len(candidates) == 1 else None

    def delay_ms(
        self,
        row: dict[str, Any],
        *,
        mode: str,
        rng: random.Random,
    ) -> float:
        normalized_mode = str(mode).strip().lower()
        if normalized_mode == "exchange_zero":
            return 0.0
        if normalized_mode not in SIMULATION_MODES or normalized_mode == "captured":
            raise ValueError(f"profile delay requires one of {SIMULATION_MODES[1:]}")
        if normalized_mode == "profile_source_stratified":
            return self._source_stratified_delay_ms(row, rng=rng)
        group = self._group(row)
        if group is None:
            raise KeyError(
                "latency profile has no group for "
                f"{row.get('market_id')} / {row.get('event_type')} / "
                f"{row.get('transport', 'unknown')}"
            )
        if normalized_mode in {"profile_empirical", "profile_stable_spike"}:
            probabilities = np.asarray(
                group.get("simulation_quantile_probabilities", []), dtype=np.float64
            )
            values = np.asarray(
                group.get("simulation_visibility_lag_ms_quantiles", []),
                dtype=np.float64,
            )
            if probabilities.size == 0 or probabilities.size != values.size:
                raise ValueError("profile group has no empirical visibility CDF")
            if normalized_mode == "profile_stable_spike":
                # Primary draws renormalize the observed CDF below p95. A
                # separately seeded 0.5% branch samples p95-p99 so occasional
                # callback/VM stalls remain testable without letting p99/p99.9
                # choose the strategy. This policy is intentionally explicit;
                # profile_empirical continues to replay the full observed CDF.
                spike = rng.random() < STABLE_SPIKE_PROBABILITY
                if spike:
                    quantile = rng.uniform(
                        STABLE_CORE_QUANTILE,
                        STABLE_SPIKE_HIGH_QUANTILE,
                    )
                else:
                    quantile = rng.uniform(0.0, STABLE_CORE_QUANTILE)
            else:
                quantile = rng.random()
            return max(0.0, float(np.interp(quantile, probabilities, values)))
        suffix = normalized_mode.removeprefix("profile_")
        key = f"visibility_lag_ms_{suffix}"
        try:
            value = float(group[key])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"profile group is missing {key}") from exc
        if not math.isfinite(value):
            raise ValueError(f"profile group has non-finite {key}")
        return max(0.0, value)

    def visible_ts_ns(
        self,
        row: dict[str, Any],
        *,
        mode: str,
        rng: random.Random,
    ) -> tuple[int, float]:
        normalized_mode = str(mode).strip().lower()
        if normalized_mode == "captured":
            ready_ns = int(row.get("feature_ready_ts_ns", 0) or 0)
            return ready_ns, 0.0
        if normalized_mode == "exchange_zero":
            exchange_ns = int(row.get("exchange_event_ts_ns", 0) or 0)
            ready_ns = exchange_ns or int(row.get("feature_ready_ts_ns", 0) or 0)
            return ready_ns, 0.0
        source_key = (
            str(row.get("market_id", "")),
            str(row.get("transport", "unknown")).lower(),
        )
        calibrated = (
            source_key in self._source_stratified
            if normalized_mode == "profile_source_stratified"
            else self._group(row) is not None
        )
        if not calibrated:
            # Sources intentionally excluded from the profile (for example a
            # slow anchor with no exchange timestamp) retain captured
            # visibility. Never guess a latency for an uncalibrated source.
            ready_ns = int(row.get("feature_ready_ts_ns", 0) or 0)
            return ready_ns, math.nan
        exchange_ns = int(row.get("exchange_event_ts_ns", 0) or 0)
        if exchange_ns <= 0:
            ready_ns = int(row.get("feature_ready_ts_ns", 0) or 0)
            return ready_ns, math.nan
        delay_ms = self.delay_ms(row, mode=normalized_mode, rng=rng)
        return exchange_ns + int(round(delay_ms * 1_000_000.0)), delay_ms


def write_profile_markdown(profile: dict[str, Any], path: Path) -> None:
    environment = profile.get("environment", {})
    measurement = profile.get("measurement", {})
    lines = [
        f"# Market-data latency profile: `{profile.get('profile_id', 'unknown')}`",
        "",
        "## Environment",
        "",
    ]
    for key, value in environment.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## Measurement",
            "",
            f"- Window: `{measurement.get('window_seconds', 0)}s`",
            f"- Rows: `{measurement.get('row_count', 0)}`",
            "- Transport lag is exchange-clock-sensitive; it is not pure one-way network latency.",
            "- `captured` uses recorded visibility; `exchange_zero` is idealized; "
            "`profile_*` injects this host profile.",
            "",
            "## Groups",
            "",
            "| market | event | transport | n | p50 | p95 | p99 | p99.9 | max | feature p50 us |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for group in profile.get("groups", []):
        lines.append(
            "| {market_id} | {event_type} | {transport} | {rows} | {p50:.3f} | "
            "{p95:.3f} | {p99:.3f} | {p999:.3f} | {maximum:.3f} | {feature:.1f} |".format(
                market_id=group.get("market_id", ""),
                event_type=group.get("event_type", ""),
                transport=group.get("transport", ""),
                rows=int(group.get("rows", 0)),
                p50=float(group.get("transport_lag_ms_p50", math.nan)),
                p95=float(group.get("transport_lag_ms_p95", math.nan)),
                p99=float(group.get("transport_lag_ms_p99", math.nan)),
                p999=float(group.get("transport_lag_ms_p999", math.nan)),
                maximum=float(group.get("transport_lag_ms_max", math.nan)),
                feature=float(group.get("feature_latency_us_p50", math.nan)),
            )
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", action="append", required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path)
    parser.add_argument("--profile-id", required=True)
    parser.add_argument("--window-seconds", type=int, default=3_600)
    parser.add_argument("--end-receive-ns", type=int, default=0)
    parser.add_argument("--max-samples", type=int, default=200_000)
    parser.add_argument("--transport", action="append", default=[])
    parser.add_argument("--market-id", action="append", default=[])
    parser.add_argument("--environment", action="append", default=[])
    parser.add_argument(
        "--source-stratified",
        action="store_true",
        help=(
            "Treat each --input argument as one independent capture window and add "
            "diagnostic day/window-equal paired book/trade samples"
        ),
    )
    parser.add_argument("--joint-bucket-ms", type=int, default=1_000)
    args = parser.parse_args()

    source_windows = None
    if args.source_stratified:
        if args.end_receive_ns:
            raise ValueError(
                "--source-stratified consumes already bounded capture windows; "
                "do not set --end-receive-ns"
            )
        source_windows = []
        for index, raw_input in enumerate(args.input, 1):
            window_paths = expand_inputs([raw_input])
            if not window_paths:
                raise FileNotFoundError(f"no receive-time files matched {raw_input!r}")
            source_windows.append((f"window_{index:04d}", window_paths))
        paths = list(dict.fromkeys(path for _, window in source_windows for path in window))
    else:
        paths = expand_inputs(args.input)
    if not paths:
        raise FileNotFoundError("no receive-time JSONL files matched")
    profile = build_latency_profile(
        paths,
        profile_id=args.profile_id,
        environment=_environment_pairs(args.environment),
        window_seconds=args.window_seconds,
        end_receive_ns=args.end_receive_ns or None,
        max_samples=max(1, args.max_samples),
        transports=set(args.transport),
        market_ids=set(args.market_id),
        source_windows=source_windows,
        joint_bucket_ms=max(1, args.joint_bucket_ms),
    )
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(
        json.dumps(profile, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if args.output_md:
        write_profile_markdown(profile, args.output_md)
    print(
        json.dumps(
            {
                "status": "ok",
                "profile_id": profile["profile_id"],
                **profile["measurement"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
