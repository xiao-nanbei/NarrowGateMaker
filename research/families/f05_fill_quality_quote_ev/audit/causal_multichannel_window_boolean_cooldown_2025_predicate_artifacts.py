#!/usr/bin/env python3
"""Materialize clock-separated, outcome-blind 2025 predicate artifacts.

The materializer consumes the frozen provider-normalized BBO/L2 and official
Binance individual-trade identities.  Provider book state and exchange-time
trade state are deliberately processed into different reference frames and
different predicate artifacts.  They are never joined into a synthetic
historical clock.

Every recursive EMA is updated from each admitted 100ms window.  A
timestamp-hash sample controls only which completed states are persisted for
quantile estimation; it is not a policy or feature-update cadence.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import gzip
import hashlib
import json
import math
import os
import re
import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Literal

import numpy as np
import pandas as pd

from models.tick_data_types import HistoricalBBOData, HistoricalL2Data
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_source_manifest as source_manifest,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
    CHANNELS_BY_BLOCK,
    IDENTITY,
    CausalMultichannelEmaState,
    CausalWindowObservation,
    FeatureContractError,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_predicates import (
    PredicateArtifact,
    fit_predicate_artifact,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_windows import (
    BBO_CHANNELS,
    DEPTH_CHANNELS,
    PROVIDER_BOOK_PROFILE,
    TRADE_CHANNELS,
    WindowExtractionAccumulator,
    WindowExtractionContract,
    stream_causal_windows,
)

SCHEMA_VERSION = f"{IDENTITY}.outcome_blind_2025_predicate_materialization.v1"
REFERENCE_PART_SCHEMA = f"{SCHEMA_VERSION}.reference_part.v1"
AUDIT_SCHEMA = f"{SCHEMA_VERSION}.audit.v1"
SAMPLING_SCHEMA = f"{SCHEMA_VERSION}.timestamp_hash_sampling.v1"
PREDICATE_BUNDLE_SCHEMA = f"{SCHEMA_VERSION}.predicate_bundle.v1"
STUDY_PREDICATE_BUNDLE_SCHEMA = f"{IDENTITY}.multiday_label_panel_nested_oof.v1.predicate_bundle.v1"

BOOK_CLOCK_GROUP = "book"
TRADE_CLOCK_GROUP = "trade"
CLOCK_GROUPS = (BOOK_CLOCK_GROUP, TRADE_CLOCK_GROUP)
SIDES = ("BUY", "SELL")
DAY_NS = 86_400_000_000_000
DEFAULT_SAMPLE_NUMERATOR = 100
DEFAULT_SAMPLE_DENOMINATOR = 1_000_000
DEFAULT_SAMPLE_SALT = f"{IDENTITY}:2025-predicate-reference:v1"
DEFAULT_QUANTILES = (0.25, 0.5, 0.75)

DEFAULT_SOURCE_MANIFEST = source_manifest.DEFAULT_OUTPUT
DEFAULT_OUTPUT_ROOT = (
    source_manifest.DEFAULT_DATA_ROOT
    / "reports"
    / "causal_multichannel_window_boolean_cooldown_duration_v2_20260810"
    / "outcome_blind_2025_predicate_artifacts"
)

PROVIDER_BOOK_CHANNELS = frozenset(BBO_CHANNELS | DEPTH_CHANNELS)
OFFICIAL_TRADE_CHANNELS = frozenset(TRADE_CHANNELS)
RAW_EXACT_LEVEL_CHANNELS = frozenset(
    spec.name for spec in CHANNELS_BY_BLOCK["M2"] if "exact_level" in spec.name
)
ALL_M2_CHANNELS = tuple(spec.name for spec in CHANNELS_BY_BLOCK["M2"])

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DAY_RE = re.compile(r"^2025-\d{2}-\d{2}$")
_BBO_COLUMNS = (
    "timestamp",
    "best_bid",
    "best_bid_qty",
    "best_ask",
    "best_ask_qty",
)
_L2_COLUMNS = (
    "timestamp",
    *tuple(
        name
        for level in range(1, 21)
        for name in (
            f"bid_px_{level}",
            f"bid_qty_{level}",
            f"ask_px_{level}",
            f"ask_qty_{level}",
        )
    ),
)
_INDIVIDUAL_TRADE_COLUMNS = (
    "id",
    "price",
    "qty",
    "quote_qty",
    "time",
    "is_buyer_maker",
)
_NON_FEATURE_REFERENCE_COLUMNS = frozenset({"utc_day", "side", "sample_ts_ns"})


class PredicateMaterializationError(RuntimeError):
    """Raised when a source, clock, resume, or admission contract drifts."""


@dataclass(frozen=True, slots=True)
class TimestampHashSamplingContract:
    """Outcome-blind persistence sample; never an EMA or policy cadence."""

    numerator: int = DEFAULT_SAMPLE_NUMERATOR
    denominator: int = DEFAULT_SAMPLE_DENOMINATOR
    salt: str = DEFAULT_SAMPLE_SALT
    hash_algorithm: str = "sha256_timestamp_identity_v1"
    base_window_width_ns: int = BASE_WINDOW_WIDTH_NS

    def __post_init__(self) -> None:
        if self.denominator <= 1:
            raise PredicateMaterializationError("sample denominator must exceed one")
        if not 0 < self.numerator <= self.denominator:
            raise PredicateMaterializationError("sample numerator is outside its denominator")
        if not self.salt.strip():
            raise PredicateMaterializationError("sample salt is empty")
        if self.hash_algorithm != "sha256_timestamp_identity_v1":
            raise PredicateMaterializationError("timestamp sample algorithm drifted")
        if self.base_window_width_ns != BASE_WINDOW_WIDTH_NS:
            raise PredicateMaterializationError("sample grid is not the admitted 100ms grid")

    @property
    def probability(self) -> float:
        return self.numerator / self.denominator

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256(asdict(self))

    def selected(self, *, clock_group: str, target_day: str, right_ts_ns: int) -> bool:
        return timestamp_hash_selected(
            clock_group=clock_group,
            target_day=target_day,
            right_ts_ns=right_ts_ns,
            contract=self,
        )


@dataclass(slots=True)
class OfficialTradeWindowState:
    previous_trade_side: int | None = None
    terminal_run: int = 0
    last_buy_ts_ns: int | None = None
    last_sell_ts_ns: int | None = None


@dataclass(frozen=True, slots=True)
class ReferenceRowsAudit:
    clock_group: str
    target_day: str
    warmup_window_count: int
    target_window_count: int
    selected_window_count: int
    output_row_count: int
    first_sample_ts_ns: int | None
    last_sample_ts_ns: int | None
    distinct_sample_interval_count: int
    warmup_last_right_ts_ns: int
    target_first_left_ts_ns: int
    target_last_right_ts_ns: int
    feature_ready_cutoff_violation_count: int
    all_windows_updated_before_sampling: bool
    sampling_is_policy_or_feature_cadence: bool
    economic_outcomes_read: bool


@dataclass(frozen=True, slots=True)
class MaterializationResult:
    admission_dir: Path
    manifest: Mapping[str, Any]
    resumed: bool


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("ascii")).hexdigest()


def canonical_document_sha256(value: Mapping[str, Any], field: str) -> str:
    payload = dict(value)
    payload.pop(field, None)
    return canonical_sha256(payload)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def timestamp_hash_selected(
    *,
    clock_group: str,
    target_day: str,
    right_ts_ns: int,
    contract: TimestampHashSamplingContract,
) -> bool:
    """Select a persistence sample without creating a regular time cadence."""

    if clock_group not in CLOCK_GROUPS:
        raise PredicateMaterializationError("sample clock group is invalid")
    _canonical_2025_day(target_day)
    timestamp = int(right_ts_ns)
    if timestamp <= 0 or timestamp % contract.base_window_width_ns:
        raise PredicateMaterializationError("sample timestamp is off the 100ms grid")
    identity = f"{contract.salt}|{clock_group}|{target_day}|{timestamp}"
    draw = int.from_bytes(hashlib.sha256(identity.encode("ascii")).digest()[:8], "big")
    return draw % contract.denominator < contract.numerator


def _canonical_2025_day(value: Any) -> str:
    text = str(value)
    if not _DAY_RE.fullmatch(text):
        raise PredicateMaterializationError(f"reference day is not in 2025: {value!r}")
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise PredicateMaterializationError(f"invalid reference day: {value!r}") from exc
    if parsed.isoformat() != text:
        raise PredicateMaterializationError(f"noncanonical reference day: {value!r}")
    return text


def _day_bounds_ns(day: str) -> tuple[int, int]:
    parsed = datetime.combine(
        date.fromisoformat(_canonical_2025_day(day)),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    left = int(parsed.timestamp() * 1_000_000_000)
    return left, left + DAY_NS


def _previous_day(day: str) -> str:
    return (date.fromisoformat(day) - timedelta(days=1)).isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PredicateMaterializationError(f"cannot read JSON: {path}") from exc
    if not isinstance(raw, dict):
        raise PredicateMaterializationError(f"JSON root is not an object: {path}")
    return raw


def load_and_validate_source_manifest(
    path: Path,
    *,
    rehash_sources: bool = True,
) -> dict[str, Any]:
    """Strictly validate the frozen source identity and its permission boundary."""

    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise PredicateMaterializationError(f"source manifest is missing: {resolved}")
    payload = _load_json(resolved)
    try:
        source_manifest.validate_manifest(payload, rehash_sources=rehash_sources)
    except source_manifest.SourceManifestError as exc:
        raise PredicateMaterializationError(str(exc)) from exc
    targets = tuple(str(day) for day in payload.get("target_days") or ())
    if not targets or any(_canonical_2025_day(day) != day for day in targets):
        raise PredicateMaterializationError("source manifest target-day universe drifted")
    windows = payload.get("target_windows")
    if not isinstance(windows, list) or len(windows) != len(targets):
        raise PredicateMaterializationError("source manifest target windows drifted")
    by_target = {str(row.get("target_day")): row for row in windows if isinstance(row, Mapping)}
    if set(by_target) != set(targets):
        raise PredicateMaterializationError("source target-window identity is incomplete")
    for target in targets:
        row = by_target[target]
        if row.get("warmup_day") != _previous_day(target):
            raise PredicateMaterializationError(f"D-1 warmup day drifted for {target}")
        if row.get("warmup_duration_hours") != 24:
            raise PredicateMaterializationError(f"D-1 warmup duration drifted for {target}")
    clocks = payload.get("clock_contract") or {}
    if clocks.get("book_trade_joint_visibility_authority") is not False:
        raise PredicateMaterializationError("book/trade joint clock authority is prohibited")
    permissions = payload.get("permission_boundary") or {}
    for key in (
        "economic_outcomes_read",
        "queue_or_lifecycle_authority",
        "exact_queue_policy_eligible",
        "action_authorized",
        "live_authorized",
    ):
        if permissions.get(key) is not False:
            raise PredicateMaterializationError(f"source permission drifted: {key}")
    return payload


def _strict_columns(frame: pd.DataFrame, expected: Sequence[str], *, label: str) -> None:
    actual = tuple(str(column) for column in frame.columns)
    if actual != tuple(expected):
        raise PredicateMaterializationError(
            f"{label} schema drifted: expected={tuple(expected)}, actual={actual}"
        )


def _strict_day_timestamps_ms(values: np.ndarray, *, day: str, label: str) -> None:
    timestamps = np.asarray(values, dtype=np.int64)
    if timestamps.ndim != 1 or (timestamps.size and np.any(np.diff(timestamps) <= 0)):
        raise PredicateMaterializationError(f"{label} timestamps are not unique/increasing")
    left_ns, right_ns = _day_bounds_ns(day)
    if timestamps.size and (
        int(timestamps[0]) * 1_000_000 < left_ns or int(timestamps[-1]) * 1_000_000 >= right_ns
    ):
        raise PredicateMaterializationError(f"{label} rows escaped UTC day {day}")


def load_provider_book_day(
    *,
    bbo_path: Path,
    l2_path: Path,
    day: str,
) -> tuple[HistoricalBBOData, HistoricalL2Data]:
    """Load only the exact provider-normalized BBO/L2 schemas."""

    canonical_day = _canonical_2025_day(day)
    bbo_frame = pd.read_parquet(bbo_path)
    l2_frame = pd.read_parquet(l2_path)
    _strict_columns(bbo_frame, _BBO_COLUMNS, label="provider BBO")
    _strict_columns(l2_frame, _L2_COLUMNS, label="provider L2")
    bbo_ts = bbo_frame["timestamp"].to_numpy(dtype=np.int64, copy=True)
    l2_ts = l2_frame["timestamp"].to_numpy(dtype=np.int64, copy=True)
    _strict_day_timestamps_ms(bbo_ts, day=canonical_day, label="provider BBO")
    _strict_day_timestamps_ms(l2_ts, day=canonical_day, label="provider L2")
    if bbo_frame.empty or l2_frame.empty:
        raise PredicateMaterializationError(f"provider book source is empty for {day}")
    bbo = HistoricalBBOData(
        ts_ms=bbo_ts,
        best_bid=bbo_frame["best_bid"].to_numpy(dtype=float, copy=True),
        best_ask=bbo_frame["best_ask"].to_numpy(dtype=float, copy=True),
        bid_qty=bbo_frame["best_bid_qty"].to_numpy(dtype=float, copy=True),
        ask_qty=bbo_frame["best_ask_qty"].to_numpy(dtype=float, copy=True),
        source="provider_normalized_causal_2025_book_only",
    )
    l2 = HistoricalL2Data(
        ts_ms=l2_ts,
        bid_px=np.column_stack(
            [
                l2_frame[f"bid_px_{level}"].to_numpy(dtype=float, copy=False)
                for level in range(1, 21)
            ]
        ),
        bid_qty=np.column_stack(
            [
                l2_frame[f"bid_qty_{level}"].to_numpy(dtype=float, copy=False)
                for level in range(1, 21)
            ]
        ),
        ask_px=np.column_stack(
            [
                l2_frame[f"ask_px_{level}"].to_numpy(dtype=float, copy=False)
                for level in range(1, 21)
            ]
        ),
        ask_qty=np.column_stack(
            [
                l2_frame[f"ask_qty_{level}"].to_numpy(dtype=float, copy=False)
                for level in range(1, 21)
            ]
        ),
        source="provider_normalized_causal_2025_book_only",
    )
    return bbo, l2


def _parse_buyer_maker(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1"}:
        return True
    if text in {"false", "0"}:
        return False
    raise PredicateMaterializationError(f"invalid is_buyer_maker value: {value!r}")


def load_official_individual_trades(*, path: Path, day: str) -> pd.DataFrame:
    """Load only official individual trades; no economic columns are accepted."""

    opener = gzip.open if path.suffix == ".gz" else Path.open
    with opener(path, "rt", encoding="utf-8", newline="") as handle:
        header = tuple(next(csv.reader(handle), ()))
    if header != _INDIVIDUAL_TRADE_COLUMNS:
        raise PredicateMaterializationError(f"official individual-trade schema drifted: {header}")
    frame = pd.read_csv(path, usecols=list(_INDIVIDUAL_TRADE_COLUMNS))
    _strict_columns(frame, _INDIVIDUAL_TRADE_COLUMNS, label="official individual trades")
    frame["time"] = pd.to_numeric(frame["time"], errors="raise").astype(np.int64)
    frame["qty"] = pd.to_numeric(frame["qty"], errors="raise").astype(float)
    frame["is_buyer_maker"] = frame["is_buyer_maker"].map(_parse_buyer_maker)
    timestamps = frame["time"].to_numpy(dtype=np.int64, copy=False)
    if timestamps.size and np.any(np.diff(timestamps) < 0):
        raise PredicateMaterializationError("official trade timestamps regressed")
    left_ns, right_ns = _day_bounds_ns(_canonical_2025_day(day))
    if timestamps.size and (
        int(timestamps[0]) * 1_000_000 < left_ns or int(timestamps[-1]) * 1_000_000 >= right_ns
    ):
        raise PredicateMaterializationError(f"official trades escaped UTC day {day}")
    quantities = frame["qty"].to_numpy(dtype=float, copy=False)
    if np.any(~np.isfinite(quantities)) or np.any(quantities <= 0.0):
        raise PredicateMaterializationError("official trade quantities are invalid")
    return frame


def stream_official_trade_windows(
    *,
    trades: pd.DataFrame,
    left_ts_ns: int,
    right_ts_ns: int,
    state: OfficialTradeWindowState | None = None,
    generation_start: int = 0,
) -> Iterator[CausalWindowObservation]:
    """Yield exchange-time trade-only observations on every admitted 100ms grid."""

    if left_ts_ns <= 0 or right_ts_ns <= left_ts_ns:
        raise PredicateMaterializationError("trade window interval is invalid")
    if left_ts_ns % BASE_WINDOW_WIDTH_NS or right_ts_ns % BASE_WINDOW_WIDTH_NS:
        raise PredicateMaterializationError("trade interval is off the 100ms grid")
    required = {"time", "qty", "is_buyer_maker"}
    if not required <= set(trades):
        raise PredicateMaterializationError("official trade frame is incomplete")
    cursor_state = state if state is not None else OfficialTradeWindowState()
    ts_ns = trades["time"].to_numpy(dtype=np.int64, copy=True) * 1_000_000
    qty = trades["qty"].to_numpy(dtype=float, copy=True)
    buyer_maker = trades["is_buyer_maker"].map(_parse_buyer_maker).to_numpy(dtype=bool)
    if ts_ns.size and np.any(np.diff(ts_ns) < 0):
        raise PredicateMaterializationError("official trade timestamps regressed")
    cursor = int(np.searchsorted(ts_ns, left_ts_ns, side="left"))
    generation = int(generation_start)
    right = int(left_ts_ns) + BASE_WINDOW_WIDTH_NS
    while right <= int(right_ts_ns):
        stop = int(np.searchsorted(ts_ns, right, side="left"))
        buy_qty = 0.0
        sell_qty = 0.0
        for index in range(cursor, stop):
            side = 0 if bool(buyer_maker[index]) else 1
            if side:
                buy_qty += float(qty[index])
                cursor_state.last_buy_ts_ns = int(ts_ns[index])
            else:
                sell_qty += float(qty[index])
                cursor_state.last_sell_ts_ns = int(ts_ns[index])
            cursor_state.terminal_run = (
                cursor_state.terminal_run + 1 if cursor_state.previous_trade_side == side else 1
            )
            cursor_state.previous_trade_side = side
        total_qty = buy_qty + sell_qty
        values: dict[str, float | None] = {name: None for name in ALL_M2_CHANNELS}
        values.update(
            {
                "aggressive_buy_qty_btc_per_s": buy_qty * 10.0,
                "aggressive_sell_qty_btc_per_s": sell_qty * 10.0,
                "signed_flow_imbalance": (
                    (buy_qty - sell_qty) / total_qty if total_qty > 0.0 else 0.0
                ),
                "trade_count_per_s": float(stop - cursor) * 10.0,
                "buy_run_length": float(
                    cursor_state.terminal_run if cursor_state.previous_trade_side == 1 else 0
                ),
                "sell_run_length": float(
                    cursor_state.terminal_run if cursor_state.previous_trade_side == 0 else 0
                ),
                "last_aggressive_buy_age_s": (
                    None
                    if cursor_state.last_buy_ts_ns is None
                    else (right - cursor_state.last_buy_ts_ns) / 1_000_000_000.0
                ),
                "last_aggressive_sell_age_s": (
                    None
                    if cursor_state.last_sell_ts_ns is None
                    else (right - cursor_state.last_sell_ts_ns) / 1_000_000_000.0
                ),
            }
        )
        generation += 1
        yield CausalWindowObservation(
            left_ts_ns=right - BASE_WINDOW_WIDTH_NS,
            right_ts_ns=right,
            feature_ready_ts_ns=right,
            market_generation=generation,
            depth_generation=generation,
            values=values,
            source_gap=False,
            source_stale=False,
        )
        cursor = stop
        right += BASE_WINDOW_WIDTH_NS


def _reference_m0_context(*, side: str, ts_ns: int) -> dict[str, Any]:
    inventory_after = 0.001 if side == "BUY" else -0.001
    return {
        "assignment_ts_ns": int(ts_ns),
        "fill_visible_ts_ns": int(ts_ns),
        "side": side,
        "role_at_fill": "opener",
        "inventory_before_fill_btc": 0.0,
        "inventory_after_fill_btc": inventory_after,
        "fill_qty_btc": 0.001,
        "order_qty_btc": 0.001,
        "cumulative_filled_qty_before_btc": 0.0,
        "cumulative_filled_qty_after_btc": 0.001,
        "remaining_order_qty_after_btc": 0.0,
        "partial_fill_ordinal": 1,
        "fill_is_partial": False,
        "order_age_s": 0.0,
        "queue_ahead_before_fill_btc": 0.0,
        "queue_state_before_fill": "exact",
        "target_price_tick": 1,
        "target_price_displayed_qty_btc": 0.001,
        "target_price_displayed_qty_status": "exact",
        "target_price_displayed_qty_known": True,
        "target_price_displayed_qty_is_queue_ahead": False,
        "consecutive_units_after": 1.0,
        "baseline_duration_ms": 85_000.0,
        "campaign_age_s": 0.0,
        "campaign_add_count": 0,
        "campaign_mae_to_date_usdc": 0.0,
        "campaign_inventory_time_to_date_btc_s": 0.0,
        "last_same_side_fill_age_s": None,
        "last_opposite_side_fill_age_s": None,
        "cooldown_remaining_ms": 0.0,
        "cooldown_blocker_active": False,
        "cooldown_lineage_revision_before": 0,
        "cooldown_deadline_owner": "none",
    }


def _field_channel(field: str, allowed_channels: frozenset[str]) -> str | None:
    for channel in sorted(allowed_channels, key=len, reverse=True):
        if field.startswith(f"tri::{channel}__"):
            return channel
        if field.startswith(f"value::{channel}::") or field.startswith(f"value::{channel}__"):
            return channel
    return None


def _project_reference_row(
    feature_row: Mapping[str, Any],
    *,
    allowed_channels: frozenset[str],
    target_day: str,
    side: str,
    sample_ts_ns: int,
) -> dict[str, Any]:
    output: dict[str, Any] = {
        "utc_day": target_day,
        "side": side,
        "sample_ts_ns": int(sample_ts_ns),
    }
    for field, value in feature_row.items():
        if not str(field).startswith(("tri::", "value::")):
            continue
        channel = _field_channel(str(field), allowed_channels)
        if channel is not None:
            output[str(field)] = value
    if not any(field.startswith("tri::") for field in output):
        raise PredicateMaterializationError("reference projection has no tri-state features")
    foreign = [
        field
        for field in output
        if field.startswith(("tri::", "value::"))
        and _field_channel(field, allowed_channels) is None
    ]
    if foreign:
        raise PredicateMaterializationError(f"reference projection mixed channels: {foreign}")
    return output


def build_clock_reference_rows(
    *,
    warmup_observations: Iterable[CausalWindowObservation],
    target_observations: Iterable[CausalWindowObservation],
    target_day: str,
    clock_group: Literal["book", "trade"],
    allowed_channels: frozenset[str],
    warmup_identity: str,
    sampling: TimestampHashSamplingContract,
) -> tuple[pd.DataFrame, ReferenceRowsAudit]:
    """Update every observation, but persist only timestamp-hash samples."""

    day = _canonical_2025_day(target_day)
    expected = (
        PROVIDER_BOOK_CHANNELS if clock_group == BOOK_CLOCK_GROUP else OFFICIAL_TRADE_CHANNELS
    )
    if allowed_channels != expected:
        raise PredicateMaterializationError("clock-specific channel universe drifted")
    if allowed_channels & RAW_EXACT_LEVEL_CHANNELS:
        raise PredicateMaterializationError(
            "provider reference cannot claim raw exact-level channels"
        )
    if not _SHA256_RE.fullmatch(str(warmup_identity)):
        raise PredicateMaterializationError("warmup identity is not a SHA256")
    state = CausalMultichannelEmaState(
        block="M2",
        warmup_admitted=True,
        warmup_identity=warmup_identity,
    )
    warmup_count = 0
    warmup_last_right: int | None = None
    for observation in warmup_observations:
        state.update(observation)
        warmup_count += 1
        warmup_last_right = int(observation.right_ts_ns)
    if warmup_count == 0 or warmup_last_right is None:
        raise PredicateMaterializationError("D-1 warmup stream is empty")

    rows: list[dict[str, Any]] = []
    selected_timestamps: list[int] = []
    target_count = 0
    target_first_left: int | None = None
    target_last_right: int | None = None
    for observation in target_observations:
        if target_first_left is None:
            target_first_left = int(observation.left_ts_ns)
            if target_first_left != warmup_last_right:
                raise PredicateMaterializationError("D-1 warmup is not contiguous with target")
        state.update(observation)
        target_count += 1
        target_last_right = int(observation.right_ts_ns)
        if not sampling.selected(
            clock_group=clock_group,
            target_day=day,
            right_ts_ns=observation.right_ts_ns,
        ):
            continue
        selected_timestamps.append(int(observation.right_ts_ns))
        for side in SIDES:
            try:
                feature_row = state.feature_row(
                    side=side,
                    decision_ts_ns=int(observation.right_ts_ns),
                    m0_context=_reference_m0_context(
                        side=side,
                        ts_ns=int(observation.right_ts_ns),
                    ),
                )
            except FeatureContractError as exc:
                raise PredicateMaterializationError(
                    "feature-ready state crossed the sample cutoff"
                ) from exc
            rows.append(
                _project_reference_row(
                    feature_row,
                    allowed_channels=allowed_channels,
                    target_day=day,
                    side=side,
                    sample_ts_ns=int(observation.right_ts_ns),
                )
            )
    if target_count == 0 or target_first_left is None or target_last_right is None:
        raise PredicateMaterializationError("target observation stream is empty")
    if not selected_timestamps:
        raise PredicateMaterializationError("timestamp hash selected no target windows")
    frame = pd.DataFrame(rows)
    frame = frame.reindex(columns=sorted(frame.columns))
    intervals = np.diff(np.asarray(selected_timestamps, dtype=np.int64))
    audit = ReferenceRowsAudit(
        clock_group=clock_group,
        target_day=day,
        warmup_window_count=warmup_count,
        target_window_count=target_count,
        selected_window_count=len(selected_timestamps),
        output_row_count=len(frame),
        first_sample_ts_ns=selected_timestamps[0],
        last_sample_ts_ns=selected_timestamps[-1],
        distinct_sample_interval_count=len(set(intervals.tolist())),
        warmup_last_right_ts_ns=warmup_last_right,
        target_first_left_ts_ns=target_first_left,
        target_last_right_ts_ns=target_last_right,
        feature_ready_cutoff_violation_count=0,
        all_windows_updated_before_sampling=True,
        sampling_is_policy_or_feature_cadence=False,
        economic_outcomes_read=False,
    )
    return frame, audit


def _source_rows(manifest: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    rows = manifest.get("source_days")
    if not isinstance(rows, list):
        raise PredicateMaterializationError("source-day records are missing")
    result = {str(row.get("day")): row for row in rows if isinstance(row, Mapping)}
    if len(result) != len(rows):
        raise PredicateMaterializationError("source-day records are malformed")
    return result


def _rebase_observations(
    observations: Iterable[CausalWindowObservation],
    *,
    generation_offset: int,
) -> Iterator[CausalWindowObservation]:
    for observation in observations:
        yield replace(
            observation,
            market_generation=int(observation.market_generation) + generation_offset,
            depth_generation=int(observation.depth_generation) + generation_offset,
        )


def _provider_observations_for_day(
    row: Mapping[str, Any],
    *,
    day: str,
    generation_offset: int,
) -> tuple[Iterator[CausalWindowObservation], WindowExtractionAccumulator]:
    bbo, l2 = load_provider_book_day(
        bbo_path=Path(str(row["bbo"]["path"])),
        l2_path=Path(str(row["l2"]["path"])),
        day=day,
    )
    left, right = _day_bounds_ns(day)
    audit = WindowExtractionAccumulator()
    observations = stream_causal_windows(
        contract=WindowExtractionContract(
            block="M2",
            source_clock_profile=PROVIDER_BOOK_PROFILE,
            left_ts_ns=left,
            right_ts_ns=right,
        ),
        bbo=bbo,
        l2=l2,
        trades=None,
        audit=audit,
    )
    return _rebase_observations(observations, generation_offset=generation_offset), audit


def _trade_observations_for_day(
    row: Mapping[str, Any],
    *,
    day: str,
    state: OfficialTradeWindowState,
    generation_offset: int,
) -> Iterator[CausalWindowObservation]:
    frame = load_official_individual_trades(
        path=Path(str(row["individual_trades"]["path"])),
        day=day,
    )
    left, right = _day_bounds_ns(day)
    return stream_official_trade_windows(
        trades=frame,
        left_ts_ns=left,
        right_ts_ns=right,
        state=state,
        generation_start=generation_offset,
    )


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_write_bytes(path, (canonical_json(payload) + "\n").encode("ascii"))


def _atomic_write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        frame.to_parquet(temporary, index=False, compression="zstd")
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _file_record(path: Path, *, root: Path) -> dict[str, Any]:
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise PredicateMaterializationError("admission file escaped its root") from exc
    return {
        "path": relative.as_posix(),
        "sha256": sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _schema_identity(frame: pd.DataFrame) -> str:
    return canonical_sha256([(str(column), str(frame[column].dtype)) for column in frame.columns])


def _frame_content_sha256(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    digest.update(_schema_identity(frame).encode("ascii"))
    digest.update(
        pd.util.hash_pandas_object(
            frame,
            index=False,
            categorize=True,
        )
        .to_numpy(dtype=np.uint64, copy=False)
        .tobytes()
    )
    return digest.hexdigest()


def _dtype_family(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series.dtype):
        return "bool"
    if pd.api.types.is_numeric_dtype(series.dtype):
        return "numeric"
    return "text"


def _fit_market_only_artifact(
    frame: pd.DataFrame,
    *,
    side: str,
    reference_identity_sha256: str,
    reference_days: Sequence[str],
    source_clock_identity: str,
    quantiles: Sequence[float],
) -> PredicateArtifact:
    """Use the public fitter without admitting synthetic M0 definitions.

    The shared fitter currently treats its mandatory ``side`` metadata as a
    partial M0 block.  Temporary categorical sentinels satisfy that public
    API, then the returned immutable artifact is projected back to the exact
    market-only input schema.  No sentinel is used for a threshold or retained
    as a predicate.
    """

    fit_frame = frame.assign(
        role_at_fill="opener",
        queue_state_before_fill="unknown",
        target_price_displayed_qty_status="unknown",
        target_price_displayed_qty_known=False,
        fill_is_partial=False,
        cooldown_blocker_active=False,
        cooldown_deadline_owner="none",
    )
    fitted = fit_predicate_artifact(
        fit_frame,
        side=side,
        source_role="outcome_blind_2025_single_channel",
        reference_identity_sha256=reference_identity_sha256,
        reference_days=reference_days,
        source_clock_identity=source_clock_identity,
        quantiles=quantiles,
    )
    original_schema = tuple(sorted((str(name), _dtype_family(frame[name])) for name in frame))
    market_definitions = tuple(
        definition
        for definition in fitted.definitions
        if definition.clock_group != "context" and definition.block != "M0"
    )
    if not market_definitions:
        raise PredicateMaterializationError("market-only predicate artifact is empty")
    return PredicateArtifact(
        schema_version=fitted.schema_version,
        identity=fitted.identity,
        side=fitted.side,
        source_role=fitted.source_role,
        reference_identity_sha256=fitted.reference_identity_sha256,
        reference_days=fitted.reference_days,
        source_clock_identity=fitted.source_clock_identity,
        clock_separated_2025=fitted.clock_separated_2025,
        quantiles=fitted.quantiles,
        input_schema=original_schema,
        definitions=market_definitions,
    )


def _receipt_payload(
    *,
    clock_group: str,
    target_day: str,
    frame_path: Path,
    audit: ReferenceRowsAudit,
    source_manifest_sha256: str,
    sampling: TimestampHashSamplingContract,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": REFERENCE_PART_SCHEMA,
        "identity": IDENTITY,
        "clock_group": clock_group,
        "target_day": target_day,
        "reference_frame": {
            "path": frame_path.name,
            "sha256": sha256_file(frame_path),
            "size_bytes": frame_path.stat().st_size,
            "row_count": audit.output_row_count,
            "schema_sha256": _schema_identity(pd.read_parquet(frame_path)),
        },
        "source_manifest_canonical_sha256": source_manifest_sha256,
        "sampling_contract": asdict(sampling),
        "sampling_contract_sha256": sampling.identity_sha256,
        "audit": asdict(audit),
        "clock_separated": True,
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    payload["canonical_receipt_sha256"] = canonical_document_sha256(
        payload, "canonical_receipt_sha256"
    )
    return payload


def _validate_part(frame_path: Path, receipt_path: Path) -> dict[str, Any]:
    if not frame_path.is_file() or not receipt_path.is_file():
        raise PredicateMaterializationError("reference part is incomplete")
    receipt = _load_json(receipt_path)
    if receipt.get("canonical_receipt_sha256") != canonical_document_sha256(
        receipt, "canonical_receipt_sha256"
    ):
        raise PredicateMaterializationError("reference receipt hash drifted")
    record = receipt.get("reference_frame") or {}
    if record.get("path") != frame_path.name:
        raise PredicateMaterializationError("reference part path drifted")
    if record.get("sha256") != sha256_file(frame_path):
        raise PredicateMaterializationError("reference part SHA256 drifted")
    frame = pd.read_parquet(frame_path)
    if int(record.get("row_count", -1)) != len(frame):
        raise PredicateMaterializationError("reference part row count drifted")
    if record.get("schema_sha256") != _schema_identity(frame):
        raise PredicateMaterializationError("reference part schema drifted")
    return receipt


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _run_identity_payload(
    *,
    source_manifest_path: Path,
    source_payload: Mapping[str, Any],
    sampling: TimestampHashSamplingContract,
    quantiles: Sequence[float],
    reference_frame_identities: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    normalized_quantiles = tuple(float(value) for value in quantiles)
    if tuple(sorted(set(normalized_quantiles))) != normalized_quantiles:
        raise PredicateMaterializationError("quantiles must be increasing and unique")
    module_root = Path(__file__).resolve().parent
    implementation_files = {
        "materializer": Path(__file__).resolve(),
        "features": module_root / "causal_multichannel_window_boolean_cooldown_features.py",
        "predicates": module_root / "causal_multichannel_window_boolean_cooldown_predicates.py",
        "source_manifest": module_root
        / "causal_multichannel_window_boolean_cooldown_source_manifest.py",
        "windows": module_root / "causal_multichannel_window_boolean_cooldown_windows.py",
    }
    if any(not path.is_file() for path in implementation_files.values()):
        raise PredicateMaterializationError("implementation binding is incomplete")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "source_manifest_file_sha256": sha256_file(source_manifest_path),
        "source_manifest_canonical_sha256": source_payload["canonical_manifest_sha256"],
        "target_days": list(source_payload["target_days"]),
        "implementation_sha256": {
            name: sha256_file(path) for name, path in sorted(implementation_files.items())
        },
        "sampling_contract": asdict(sampling),
        "quantiles": list(normalized_quantiles),
        "book_clock_identity": source_payload["clock_contract"]["bbo_l2_clock"],
        "trade_clock_identity": source_payload["clock_contract"]["trade_clock"],
        "book_channels": sorted(PROVIDER_BOOK_CHANNELS),
        "trade_channels": sorted(OFFICIAL_TRADE_CHANNELS),
        "provider_raw_exact_level_channels_present": False,
        "official_trade_role": "diagnostic_and_threshold_support_only",
        "ema_update_grid_ns": BASE_WINDOW_WIDTH_NS,
        "sampling_is_policy_or_feature_cadence": False,
        "d_minus_one_warmup_hours": 24,
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    if reference_frame_identities is not None:
        if set(reference_frame_identities) != set(CLOCK_GROUPS):
            raise PredicateMaterializationError("reference-frame identity universe drifted")
        if any(
            not _SHA256_RE.fullmatch(str(value)) for value in reference_frame_identities.values()
        ):
            raise PredicateMaterializationError("reference-frame SHA256 drifted")
        payload["reference_frame_identities"] = dict(sorted(reference_frame_identities.items()))
    return payload


def _build_target_part(
    *,
    manifest: Mapping[str, Any],
    target_day: str,
    clock_group: str,
    sampling: TimestampHashSamplingContract,
) -> tuple[pd.DataFrame, ReferenceRowsAudit]:
    rows = _source_rows(manifest)
    warmup_day = _previous_day(target_day)
    if warmup_day not in rows or target_day not in rows:
        raise PredicateMaterializationError(f"source intersection is incomplete for {target_day}")
    warmup_identity = canonical_sha256(
        {
            "clock_group": clock_group,
            "warmup_day": warmup_day,
            "source_manifest": manifest["canonical_manifest_sha256"],
        }
    )
    windows_per_day = DAY_NS // BASE_WINDOW_WIDTH_NS
    if clock_group == BOOK_CLOCK_GROUP:
        warmup, _ = _provider_observations_for_day(
            rows[warmup_day], day=warmup_day, generation_offset=0
        )
        target, _ = _provider_observations_for_day(
            rows[target_day],
            day=target_day,
            generation_offset=int(windows_per_day),
        )
        allowed = PROVIDER_BOOK_CHANNELS
    else:
        trade_state = OfficialTradeWindowState()
        warmup = _trade_observations_for_day(
            rows[warmup_day],
            day=warmup_day,
            state=trade_state,
            generation_offset=0,
        )
        target = _trade_observations_for_day(
            rows[target_day],
            day=target_day,
            state=trade_state,
            generation_offset=int(windows_per_day),
        )
        allowed = OFFICIAL_TRADE_CHANNELS
    return build_clock_reference_rows(
        warmup_observations=warmup,
        target_observations=target,
        target_day=target_day,
        clock_group=clock_group,  # type: ignore[arg-type]
        allowed_channels=allowed,
        warmup_identity=warmup_identity,
        sampling=sampling,
    )


def _fit_artifacts(
    *,
    reference_frames: Mapping[str, pd.DataFrame],
    source_payload: Mapping[str, Any],
    sampling: TimestampHashSamplingContract,
    quantiles: Sequence[float],
) -> dict[str, PredicateArtifact]:
    targets = tuple(str(day) for day in source_payload["target_days"])
    output: dict[str, PredicateArtifact] = {}
    for clock_group in CLOCK_GROUPS:
        frame = reference_frames[clock_group]
        allowed = (
            PROVIDER_BOOK_CHANNELS if clock_group == BOOK_CLOCK_GROUP else OFFICIAL_TRADE_CHANNELS
        )
        feature_columns = [
            column
            for column in frame.columns
            if column in {"utc_day", "side"} or _field_channel(str(column), allowed) is not None
        ]
        fit_frame = frame.loc[:, sorted(feature_columns)].copy()
        foreign = [
            column
            for column in fit_frame
            if column.startswith(("tri::", "value::")) and _field_channel(column, allowed) is None
        ]
        if foreign:
            raise PredicateMaterializationError(
                f"{clock_group} reference frame mixed clocks: {foreign}"
            )
        clock_identity = source_payload["clock_contract"][
            "bbo_l2_clock" if clock_group == BOOK_CLOCK_GROUP else "trade_clock"
        ]
        for side in SIDES:
            side_frame = fit_frame.loc[fit_frame["side"] == side].reset_index(drop=True)
            if tuple(sorted(side_frame["utc_day"].unique())) != targets:
                raise PredicateMaterializationError(f"{clock_group}/{side} reference days drifted")
            reference_identity = canonical_sha256(
                {
                    "source_manifest": source_payload["canonical_manifest_sha256"],
                    "clock_group": clock_group,
                    "side": side,
                    "sampling": sampling.identity_sha256,
                    "frame_schema": _schema_identity(side_frame),
                    "row_count": len(side_frame),
                }
            )
            output[f"{clock_group}_{side.lower()}"] = _fit_market_only_artifact(
                side_frame,
                side=side,
                reference_identity_sha256=reference_identity,
                reference_days=targets,
                source_clock_identity=str(clock_identity),
                quantiles=quantiles,
            )
    return output


def _write_final_payloads(
    *,
    work_dir: Path,
    source_manifest_path: Path,
    source_payload: Mapping[str, Any],
    run_identity: str,
    sampling: TimestampHashSamplingContract,
    quantiles: Sequence[float],
    part_receipts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    by_clock: dict[str, list[pd.DataFrame]] = {clock: [] for clock in CLOCK_GROUPS}
    for receipt in part_receipts:
        clock = str(receipt["clock_group"])
        day = str(receipt["target_day"])
        frame_path = work_dir / "reference_parts" / clock / f"{day}.parquet"
        by_clock[clock].append(pd.read_parquet(frame_path))
    reference_frames = {
        clock: pd.concat(parts, ignore_index=True)
        .sort_values(["utc_day", "sample_ts_ns", "side"], kind="stable")
        .reset_index(drop=True)
        for clock, parts in by_clock.items()
    }
    artifacts = _fit_artifacts(
        reference_frames=reference_frames,
        source_payload=source_payload,
        sampling=sampling,
        quantiles=quantiles,
    )
    artifact_records: dict[str, Any] = {}
    for key, artifact in sorted(artifacts.items()):
        path = work_dir / "artifacts" / f"{key}.json"
        _atomic_write_bytes(path, (artifact.to_json() + "\n").encode("ascii"))
        artifact_records[key] = {
            **_file_record(path, root=work_dir),
            "canonical_artifact_sha256": artifact.canonical_sha256,
            "side": artifact.side,
            "clock_group": key.split("_", 1)[0],
        }

    audit_records: dict[str, Any] = {}
    for clock, frame in reference_frames.items():
        intervals = []
        for _, side_frame in frame.loc[frame["side"] == "BUY"].groupby("utc_day"):
            intervals.extend(np.diff(side_frame["sample_ts_ns"].to_numpy(dtype=np.int64)).tolist())
        payload: dict[str, Any] = {
            "schema_version": AUDIT_SCHEMA,
            "identity": IDENTITY,
            "clock_group": clock,
            "source_clock_identity": source_payload["clock_contract"][
                "bbo_l2_clock" if clock == BOOK_CLOCK_GROUP else "trade_clock"
            ],
            "target_days": list(source_payload["target_days"]),
            "row_count": len(frame),
            "selected_window_count": len(frame) // 2,
            "side_row_counts": {side: int((frame["side"] == side).sum()) for side in SIDES},
            "distinct_sample_interval_count": len(set(intervals)),
            "sampling_contract": asdict(sampling),
            "ema_updated_on_every_admitted_100ms_window": True,
            "sampling_controls_materialization_only": True,
            "clock_frames_joined": False,
            "market_only_fit_adapter": ("temporary_categorical_sentinels_removed_before_artifact"),
            "m0_context_predicates_present": False,
            "provider_raw_exact_level_channels_present": False,
            "provider_raw_exact_level_channels_excluded": sorted(RAW_EXACT_LEVEL_CHANNELS),
            "official_trade_role": (
                "not_applicable"
                if clock == BOOK_CLOCK_GROUP
                else "diagnostic_and_threshold_support_only"
            ),
            "economic_outcomes_read": False,
            "action_authorized": False,
            "live_authorized": False,
        }
        payload["canonical_audit_sha256"] = canonical_document_sha256(
            payload, "canonical_audit_sha256"
        )
        path = work_dir / "audit" / f"{clock}.json"
        _atomic_write_json(path, payload)
        audit_records[clock] = {
            **_file_record(path, root=work_dir),
            "canonical_audit_sha256": payload["canonical_audit_sha256"],
        }

    part_records: list[dict[str, Any]] = []
    for receipt in sorted(
        part_receipts,
        key=lambda item: (str(item["clock_group"]), str(item["target_day"])),
    ):
        clock = str(receipt["clock_group"])
        day = str(receipt["target_day"])
        frame_path = work_dir / "reference_parts" / clock / f"{day}.parquet"
        receipt_path = work_dir / "reference_parts" / clock / f"{day}.receipt.json"
        part_records.append(
            {
                "clock_group": clock,
                "target_day": day,
                "reference_frame": _file_record(frame_path, root=work_dir),
                "receipt": {
                    **_file_record(receipt_path, root=work_dir),
                    "canonical_receipt_sha256": receipt["canonical_receipt_sha256"],
                },
            }
        )

    study_predicate_bundle: dict[str, Any] = {
        "schema_version": STUDY_PREDICATE_BUNDLE_SCHEMA,
        "identity": IDENTITY,
        "book": {
            side: {
                "path": artifact_records[f"book_{side.lower()}"]["path"],
                "sha256": artifact_records[f"book_{side.lower()}"]["sha256"],
            }
            for side in SIDES
        },
        "trade": {
            side: {
                "path": artifact_records[f"trade_{side.lower()}"]["path"],
                "sha256": artifact_records[f"trade_{side.lower()}"]["sha256"],
            }
            for side in SIDES
        },
        "m0_artifacts": [],
        "cross_clock_clause_authorized": False,
        "cross_clock_clause_scope": "2025_reference_rows_only",
        "strict_2026_target_snapshot": {
            "book_trade_predicates_may_be_combined_by_study": True,
            "required_condition": (
                "book and trade predicates are evaluated on the same admitted "
                "strict target snapshot and causal feature-ready cutoff"
            ),
            "authority_owner": "2026_strict_denominator_study",
        },
    }
    study_predicate_bundle["canonical_sha256"] = canonical_document_sha256(
        study_predicate_bundle,
        "canonical_sha256",
    )
    study_bundle_path = work_dir / "predicate_bundle.json"
    _atomic_write_json(study_bundle_path, study_predicate_bundle)
    study_bundle_record = {
        **_file_record(study_bundle_path, root=work_dir),
        "canonical_sha256": study_predicate_bundle["canonical_sha256"],
    }

    predicate_bundle = {
        "schema_version": PREDICATE_BUNDLE_SCHEMA,
        "book": {
            side: {
                "artifact_key": f"book_{side.lower()}",
                **artifact_records[f"book_{side.lower()}"],
            }
            for side in SIDES
        },
        "trade": {
            side: {
                "artifact_key": f"trade_{side.lower()}",
                **artifact_records[f"trade_{side.lower()}"],
            }
            for side in SIDES
        },
        "m0": {
            "materialized": False,
            "owner": "inner_chronological_development_builder",
            "required_partition_keys": [
                "panel_scope",
                "side",
                "outer_fold_id",
                "inner_fold_id",
            ],
            "required_source_role": "inner_chronological_development",
            "required_api": "fit_predicate_artifact",
            "reason": (
                "2025 market sources contain no decision-visible order, "
                "inventory, campaign, or cooldown action context"
            ),
        },
        "book_trade_reference_frames_joined": False,
        "cross_clock_clause_authorized": False,
        "cross_clock_clause_scope": "2025_reference_rows_only",
        "cross_clock_clause_authorized_on_2025_reference_rows": False,
        "strict_2026_target_snapshot": {
            "book_trade_predicates_may_be_combined_by_study": True,
            "required_condition": (
                "book and trade features share the same admitted strict target "
                "snapshot and causal feature-ready cutoff"
            ),
            "authority_owner": "2026_strict_denominator_study",
        },
    }
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "run_identity_sha256": run_identity,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_request": _file_record(work_dir / "run_request.json", root=work_dir),
        "source_manifest": {
            "path": str(source_manifest_path),
            "file_sha256": sha256_file(source_manifest_path),
            "canonical_manifest_sha256": source_payload["canonical_manifest_sha256"],
        },
        "target_days": list(source_payload["target_days"]),
        "sampling_contract": asdict(sampling),
        "sampling_contract_sha256": sampling.identity_sha256,
        "quantiles": [float(value) for value in quantiles],
        "reference_parts": part_records,
        "artifacts": artifact_records,
        "predicate_bundle": predicate_bundle,
        "study_predicate_bundle": study_bundle_record,
        "audits": audit_records,
        "clock_contract": {
            "book": source_payload["clock_contract"]["bbo_l2_clock"],
            "trade": source_payload["clock_contract"]["trade_clock"],
            "book_trade_reference_frames_joined": False,
            "book_trade_joint_clauses_authorized_on_2025_reference_rows": False,
            "ema_update_grid_ns": BASE_WINDOW_WIDTH_NS,
            "sampling_is_policy_or_feature_cadence": False,
        },
        "support_boundary": {
            "provider_book_raw_exact_level_support": False,
            "official_trade_action_grade_support": False,
            "official_trade_role": "diagnostic_and_threshold_support_only",
            "economic_outcomes_read": False,
            "queue_or_lifecycle_authority": False,
            "exact_queue_policy_eligible": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    manifest["canonical_manifest_sha256"] = canonical_document_sha256(
        manifest, "canonical_manifest_sha256"
    )
    _atomic_write_json(work_dir / "manifest.json", manifest)
    return manifest


def validate_admission(
    admission_dir: Path,
    *,
    source_manifest_path: Path | None = None,
    rehash_sources: bool = False,
) -> dict[str, Any]:
    """Re-hash every admitted output and optionally revalidate all raw sources."""

    root = admission_dir.expanduser().resolve()
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        raise PredicateMaterializationError("admission manifest is missing")
    manifest = _load_json(manifest_path)
    if manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("identity") != IDENTITY:
        raise PredicateMaterializationError("admission identity drifted")
    if manifest.get("canonical_manifest_sha256") != canonical_document_sha256(
        manifest, "canonical_manifest_sha256"
    ):
        raise PredicateMaterializationError("admission manifest hash drifted")
    if root.name != manifest.get("run_identity_sha256"):
        raise PredicateMaterializationError("admission directory identity drifted")
    support = manifest.get("support_boundary") or {}
    for key in (
        "provider_book_raw_exact_level_support",
        "official_trade_action_grade_support",
        "economic_outcomes_read",
        "queue_or_lifecycle_authority",
        "exact_queue_policy_eligible",
        "action_authorized",
        "live_authorized",
    ):
        if support.get(key) is not False:
            raise PredicateMaterializationError(f"admission permission drifted: {key}")
    clocks = manifest.get("clock_contract") or {}
    if clocks.get("book_trade_reference_frames_joined") is not False:
        raise PredicateMaterializationError("book/trade reference frames were joined")
    if clocks.get("book_trade_joint_clauses_authorized_on_2025_reference_rows") is not False:
        raise PredicateMaterializationError("2025 cross-clock reference clauses were authorized")

    def validate_record(record: Mapping[str, Any]) -> Path:
        relative = Path(str(record.get("path", "")))
        if relative.is_absolute() or ".." in relative.parts:
            raise PredicateMaterializationError("admission record escaped root")
        path = root / relative
        if not path.is_file():
            raise PredicateMaterializationError(f"admission file is missing: {relative}")
        if record.get("sha256") != sha256_file(path):
            raise PredicateMaterializationError(f"admission file hash drifted: {relative}")
        if int(record.get("size_bytes", -1)) != path.stat().st_size:
            raise PredicateMaterializationError(f"admission file size drifted: {relative}")
        return path

    run_request_path = validate_record(manifest.get("run_request") or {})
    run_request = _load_json(run_request_path)
    if canonical_sha256(run_request) != manifest["run_identity_sha256"]:
        raise PredicateMaterializationError("run request identity drifted")
    if (
        run_request.get("source_manifest_canonical_sha256")
        != manifest["source_manifest"]["canonical_manifest_sha256"]
    ):
        raise PredicateMaterializationError("run request source binding drifted")
    if run_request.get("sampling_contract") != manifest["sampling_contract"]:
        raise PredicateMaterializationError("run request sampling binding drifted")

    parts = manifest.get("reference_parts")
    if not isinstance(parts, list) or not parts:
        raise PredicateMaterializationError("reference parts are missing")
    observed_keys: set[tuple[str, str]] = set()
    for record in parts:
        if not isinstance(record, Mapping):
            raise PredicateMaterializationError("reference part record is malformed")
        key = (str(record.get("clock_group")), str(record.get("target_day")))
        if key in observed_keys:
            raise PredicateMaterializationError("reference part is duplicated")
        observed_keys.add(key)
        frame_path = validate_record(record["reference_frame"])
        receipt_path = validate_record(record["receipt"])
        receipt = _validate_part(frame_path, receipt_path)
        if receipt["canonical_receipt_sha256"] != record["receipt"]["canonical_receipt_sha256"]:
            raise PredicateMaterializationError("reference receipt binding drifted")
    expected_keys = {(clock, day) for clock in CLOCK_GROUPS for day in manifest["target_days"]}
    if observed_keys != expected_keys:
        raise PredicateMaterializationError("reference part denominator drifted")

    artifact_records = manifest.get("artifacts")
    if set(artifact_records or ()) != {
        "book_buy",
        "book_sell",
        "trade_buy",
        "trade_sell",
    }:
        raise PredicateMaterializationError("artifact universe drifted")
    for key, record in artifact_records.items():
        path = validate_record(record)
        artifact = PredicateArtifact.from_json(path.read_text(encoding="ascii"))
        if artifact.canonical_sha256 != record.get("canonical_artifact_sha256"):
            raise PredicateMaterializationError(f"artifact binding drifted: {key}")
        expected_side = key.rsplit("_", 1)[1].upper()
        if artifact.side != expected_side:
            raise PredicateMaterializationError(f"artifact side drifted: {key}")
        if artifact.source_role != "outcome_blind_2025_single_channel":
            raise PredicateMaterializationError(f"artifact source role drifted: {key}")
        if artifact.clock_separated_2025 is not True:
            raise PredicateMaterializationError(f"artifact clock separation drifted: {key}")
        expected_clock = key.split("_", 1)[0]
        definition_groups = {
            definition.clock_group
            for definition in artifact.definitions
            if definition.clock_group in CLOCK_GROUPS
        }
        if definition_groups - {expected_clock}:
            raise PredicateMaterializationError(f"artifact mixed clocks: {key}")
        if expected_clock == BOOK_CLOCK_GROUP and any(
            "exact_level" in definition.source_field for definition in artifact.definitions
        ):
            raise PredicateMaterializationError("provider artifact claimed raw exact levels")
    bundle = manifest.get("predicate_bundle") or {}
    if bundle.get("schema_version") != PREDICATE_BUNDLE_SCHEMA:
        raise PredicateMaterializationError("predicate bundle identity drifted")
    for clock in CLOCK_GROUPS:
        if set(bundle.get(clock) or ()) != set(SIDES):
            raise PredicateMaterializationError(f"predicate bundle {clock} side universe drifted")
        for side in SIDES:
            key = f"{clock}_{side.lower()}"
            record = bundle[clock][side]
            if record.get("artifact_key") != key:
                raise PredicateMaterializationError("predicate bundle key drifted")
            for field in (
                "path",
                "sha256",
                "size_bytes",
                "canonical_artifact_sha256",
                "side",
                "clock_group",
            ):
                if record.get(field) != artifact_records[key].get(field):
                    raise PredicateMaterializationError(
                        f"predicate bundle artifact binding drifted: {key}/{field}"
                    )
    m0 = bundle.get("m0") or {}
    if m0.get("materialized") is not False:
        raise PredicateMaterializationError("2025 bundle must not materialize M0")
    if m0.get("required_partition_keys") != [
        "panel_scope",
        "side",
        "outer_fold_id",
        "inner_fold_id",
    ]:
        raise PredicateMaterializationError("M0 external-builder scope drifted")
    if bundle.get("book_trade_reference_frames_joined") is not False:
        raise PredicateMaterializationError("predicate bundle joined source clocks")
    if bundle.get("cross_clock_clause_authorized") is not False:
        raise PredicateMaterializationError(
            "predicate bundle authorized mixed 2025 reference clauses"
        )
    if bundle.get("cross_clock_clause_scope") != "2025_reference_rows_only":
        raise PredicateMaterializationError("predicate bundle cross-clock scope drifted")
    if bundle.get("cross_clock_clause_authorized_on_2025_reference_rows") is not False:
        raise PredicateMaterializationError(
            "predicate bundle authorized mixed 2025 reference clauses"
        )
    strict_target = bundle.get("strict_2026_target_snapshot") or {}
    if strict_target.get("book_trade_predicates_may_be_combined_by_study") is not True:
        raise PredicateMaterializationError(
            "predicate bundle incorrectly restricted the 2026 strict study"
        )
    if strict_target.get("authority_owner") != "2026_strict_denominator_study":
        raise PredicateMaterializationError("strict-target authority owner drifted")

    study_bundle_path = validate_record(manifest.get("study_predicate_bundle") or {})
    study_bundle = _load_json(study_bundle_path)
    if study_bundle.get("canonical_sha256") != canonical_document_sha256(
        study_bundle,
        "canonical_sha256",
    ):
        raise PredicateMaterializationError("study predicate bundle canonical hash drifted")
    if study_bundle.get("canonical_sha256") != manifest["study_predicate_bundle"].get(
        "canonical_sha256"
    ):
        raise PredicateMaterializationError("study predicate bundle binding drifted")
    if study_bundle.get("schema_version") != STUDY_PREDICATE_BUNDLE_SCHEMA:
        raise PredicateMaterializationError("study predicate bundle schema drifted")
    if study_bundle.get("identity") != IDENTITY:
        raise PredicateMaterializationError("study predicate bundle identity drifted")
    if study_bundle.get("m0_artifacts") != []:
        raise PredicateMaterializationError("study predicate bundle fabricated M0 artifacts")
    if study_bundle.get("cross_clock_clause_authorized") is not False:
        raise PredicateMaterializationError(
            "study predicate bundle authorized mixed 2025 reference clauses"
        )
    if study_bundle.get("cross_clock_clause_scope") != "2025_reference_rows_only":
        raise PredicateMaterializationError("study predicate bundle cross-clock scope drifted")
    study_target = study_bundle.get("strict_2026_target_snapshot") or {}
    if study_target.get("book_trade_predicates_may_be_combined_by_study") is not True:
        raise PredicateMaterializationError(
            "study predicate bundle prohibited strict-target clock combination"
        )
    for clock in CLOCK_GROUPS:
        if set(study_bundle.get(clock) or ()) != set(SIDES):
            raise PredicateMaterializationError(
                f"study predicate bundle {clock} side universe drifted"
            )
        for side in SIDES:
            key = f"{clock}_{side.lower()}"
            entry = study_bundle[clock][side]
            if entry.get("path") != artifact_records[key].get("path"):
                raise PredicateMaterializationError(
                    f"study predicate bundle artifact path drifted: {key}"
                )
            if entry.get("sha256") != artifact_records[key].get("sha256"):
                raise PredicateMaterializationError(
                    f"study predicate bundle artifact hash drifted: {key}"
                )
    for record in (manifest.get("audits") or {}).values():
        path = validate_record(record)
        audit = _load_json(path)
        if audit.get("canonical_audit_sha256") != canonical_document_sha256(
            audit, "canonical_audit_sha256"
        ):
            raise PredicateMaterializationError("audit canonical hash drifted")
        if audit["canonical_audit_sha256"] != record["canonical_audit_sha256"]:
            raise PredicateMaterializationError("audit binding drifted")

    if source_manifest_path is not None:
        source = load_and_validate_source_manifest(
            source_manifest_path,
            rehash_sources=rehash_sources,
        )
        source_record = manifest["source_manifest"]
        if source_record["file_sha256"] != sha256_file(source_manifest_path):
            raise PredicateMaterializationError("source manifest file hash drifted")
        if source_record["canonical_manifest_sha256"] != source["canonical_manifest_sha256"]:
            raise PredicateMaterializationError("source manifest identity drifted")
    return manifest


def load_2025_predicate_bundle(
    admission_dir: Path,
    *,
    expected_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    """Load the stable 2025 market bundle without fabricating fold-local M0."""

    manifest = validate_admission(admission_dir)
    observed_sha = str(manifest["canonical_manifest_sha256"])
    if expected_manifest_sha256 is not None and expected_manifest_sha256 != observed_sha:
        raise PredicateMaterializationError("expected predicate bundle manifest drifted")
    bundle = manifest["predicate_bundle"]
    loaded: dict[str, Any] = {
        "schema_version": bundle["schema_version"],
        "book": {},
        "trade": {},
        "m0": dict(bundle["m0"]),
        "compatibility": {
            "book_trade_reference_frames_joined": bundle["book_trade_reference_frames_joined"],
            "cross_clock_clause_authorized": bundle["cross_clock_clause_authorized"],
            "cross_clock_clause_scope": bundle["cross_clock_clause_scope"],
            "cross_clock_clause_authorized_on_2025_reference_rows": bundle[
                "cross_clock_clause_authorized_on_2025_reference_rows"
            ],
            "strict_2026_target_snapshot": dict(bundle["strict_2026_target_snapshot"]),
        },
        "study_predicate_bundle_path": (
            admission_dir.resolve() / manifest["study_predicate_bundle"]["path"]
        ),
        "study_predicate_bundle_sha256": manifest["study_predicate_bundle"]["sha256"],
        "canonical_manifest_sha256": observed_sha,
    }
    for clock in CLOCK_GROUPS:
        for side in SIDES:
            record = bundle[clock][side]
            path = admission_dir.resolve() / str(record["path"])
            loaded[clock][side] = PredicateArtifact.from_json(path.read_text(encoding="ascii"))
    return loaded


def admit_reference_frames(
    *,
    source_manifest_path: Path,
    output_root: Path,
    reference_frames: Mapping[str, pd.DataFrame],
    sampling: TimestampHashSamplingContract,
    audits: Mapping[str, Mapping[str, ReferenceRowsAudit]],
    quantiles: Sequence[float] = DEFAULT_QUANTILES,
    rehash_sources: bool = True,
) -> MaterializationResult:
    """Atomically admit already-built clock-separated frames.

    This public entry point is useful for bounded builders and synthetic
    contract tests.  It applies the same source validation, artifact fitting,
    hash binding, and final directory admission as the full materializer.
    """

    source_path = source_manifest_path.expanduser().resolve()
    source_payload = load_and_validate_source_manifest(source_path, rehash_sources=rehash_sources)
    if set(reference_frames) != set(CLOCK_GROUPS):
        raise PredicateMaterializationError("reference frames must be exactly book and trade")
    targets = tuple(str(day) for day in source_payload["target_days"])
    for clock, frame in reference_frames.items():
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            raise PredicateMaterializationError(f"{clock} reference frame is empty")
        if set(_NON_FEATURE_REFERENCE_COLUMNS) - set(frame):
            raise PredicateMaterializationError(f"{clock} reference metadata is incomplete")
        if tuple(sorted(frame["utc_day"].astype(str).unique())) != targets:
            raise PredicateMaterializationError(f"{clock} reference days drifted")
        if set(frame["side"].astype(str).unique()) != set(SIDES):
            raise PredicateMaterializationError(f"{clock} reference sides drifted")
        allowed = PROVIDER_BOOK_CHANNELS if clock == BOOK_CLOCK_GROUP else OFFICIAL_TRADE_CHANNELS
        for column in frame:
            if column in _NON_FEATURE_REFERENCE_COLUMNS:
                continue
            if _field_channel(str(column), allowed) is None:
                raise PredicateMaterializationError(
                    f"{clock} frame mixed a foreign channel: {column}"
                )
    if set(audits) != set(CLOCK_GROUPS) or any(
        set(audits[clock]) != set(targets) for clock in CLOCK_GROUPS
    ):
        raise PredicateMaterializationError("reference audit denominator drifted")
    run_payload = _run_identity_payload(
        source_manifest_path=source_path,
        source_payload=source_payload,
        sampling=sampling,
        quantiles=quantiles,
        reference_frame_identities={
            clock: _frame_content_sha256(reference_frames[clock]) for clock in CLOCK_GROUPS
        },
    )
    run_identity = canonical_sha256(run_payload)
    root = output_root.expanduser().resolve()
    final_dir = root / run_identity
    work_dir = root / f".{run_identity}.work"
    lock_path = root / f".{run_identity}.lock"
    with _exclusive_lock(lock_path):
        if final_dir.exists():
            manifest = validate_admission(
                final_dir,
                source_manifest_path=source_path,
                rehash_sources=False,
            )
            return MaterializationResult(final_dir, manifest, True)
        work_dir.mkdir(parents=True, exist_ok=True)
        request_path = work_dir / "run_request.json"
        if request_path.exists():
            if _load_json(request_path) != run_payload:
                raise PredicateMaterializationError("resume request identity drifted")
        else:
            _atomic_write_json(request_path, run_payload)
        part_receipts: list[dict[str, Any]] = []
        try:
            for clock in CLOCK_GROUPS:
                frame = reference_frames[clock]
                for day in targets:
                    day_frame = frame.loc[frame["utc_day"] == day].reset_index(drop=True)
                    frame_path = work_dir / "reference_parts" / clock / f"{day}.parquet"
                    receipt_path = work_dir / "reference_parts" / clock / f"{day}.receipt.json"
                    if frame_path.exists() or receipt_path.exists():
                        receipt = _validate_part(frame_path, receipt_path)
                        if receipt.get("sampling_contract_sha256") != sampling.identity_sha256:
                            raise PredicateMaterializationError("resume sampling identity drifted")
                        part_receipts.append(receipt)
                        continue
                    supplied_audit = audits[clock][day]
                    if supplied_audit.clock_group != clock or supplied_audit.target_day != day:
                        raise PredicateMaterializationError(
                            "reference audit clock/day identity drifted"
                        )
                    if supplied_audit.output_row_count != len(day_frame):
                        raise PredicateMaterializationError("reference audit row count drifted")
                    _atomic_write_parquet(frame_path, day_frame)
                    receipt = _receipt_payload(
                        clock_group=clock,
                        target_day=day,
                        frame_path=frame_path,
                        audit=supplied_audit,
                        source_manifest_sha256=source_payload["canonical_manifest_sha256"],
                        sampling=sampling,
                    )
                    _atomic_write_json(receipt_path, receipt)
                    part_receipts.append(receipt)
            manifest = _write_final_payloads(
                work_dir=work_dir,
                source_manifest_path=source_path,
                source_payload=source_payload,
                run_identity=run_identity,
                sampling=sampling,
                quantiles=quantiles,
                part_receipts=part_receipts,
            )
            os.replace(work_dir, final_dir)
            _fsync_directory(root)
        except Exception:
            raise
        validated = validate_admission(
            final_dir,
            source_manifest_path=source_path,
            rehash_sources=False,
        )
        return MaterializationResult(final_dir, validated, False)


def _build_and_write_part(
    *,
    source_payload: Mapping[str, Any],
    target_day: str,
    clock_group: str,
    sampling: TimestampHashSamplingContract,
    frame_path: Path,
    receipt_path: Path,
) -> dict[str, Any]:
    """Build one independent day/channel part for bounded process parallelism."""

    frame, audit = _build_target_part(
        manifest=source_payload,
        target_day=target_day,
        clock_group=clock_group,
        sampling=sampling,
    )
    _atomic_write_parquet(frame_path, frame)
    receipt = _receipt_payload(
        clock_group=clock_group,
        target_day=target_day,
        frame_path=frame_path,
        audit=audit,
        source_manifest_sha256=str(source_payload["canonical_manifest_sha256"]),
        sampling=sampling,
    )
    _atomic_write_json(receipt_path, receipt)
    return receipt


def materialize_predicate_artifacts(
    *,
    source_manifest_path: Path = DEFAULT_SOURCE_MANIFEST,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    sampling: TimestampHashSamplingContract | None = None,
    quantiles: Sequence[float] = DEFAULT_QUANTILES,
    workers: int = 1,
) -> MaterializationResult:
    """Build all 2025 parts with durable day-level resume and atomic admission."""

    sample_contract = sampling or TimestampHashSamplingContract()
    if workers < 1 or workers > 4:
        raise PredicateMaterializationError("workers must be in [1, 4]")
    if sample_contract.numerator == sample_contract.denominator:
        raise PredicateMaterializationError(
            "authoritative materialization requires sparse timestamp-hash sampling"
        )
    source_path = source_manifest_path.expanduser().resolve()
    source_payload = load_and_validate_source_manifest(source_path, rehash_sources=True)
    run_payload = _run_identity_payload(
        source_manifest_path=source_path,
        source_payload=source_payload,
        sampling=sample_contract,
        quantiles=quantiles,
    )
    run_identity = canonical_sha256(run_payload)
    root = output_root.expanduser().resolve()
    final_dir = root / run_identity
    work_dir = root / f".{run_identity}.work"
    lock_path = root / f".{run_identity}.lock"
    with _exclusive_lock(lock_path):
        if final_dir.exists():
            manifest = validate_admission(
                final_dir,
                source_manifest_path=source_path,
                rehash_sources=False,
            )
            return MaterializationResult(final_dir, manifest, True)
        work_dir.mkdir(parents=True, exist_ok=True)
        request_path = work_dir / "run_request.json"
        if request_path.exists():
            if _load_json(request_path) != run_payload:
                raise PredicateMaterializationError("resume request identity drifted")
        else:
            _atomic_write_json(request_path, run_payload)
        receipts: list[dict[str, Any]] = []
        pending: list[tuple[str, str, Path, Path]] = []
        for target_day in source_payload["target_days"]:
            for clock_group in CLOCK_GROUPS:
                frame_path = work_dir / "reference_parts" / clock_group / f"{target_day}.parquet"
                receipt_path = (
                    work_dir / "reference_parts" / clock_group / f"{target_day}.receipt.json"
                )
                if frame_path.exists() or receipt_path.exists():
                    receipt = _validate_part(frame_path, receipt_path)
                    if receipt.get("sampling_contract_sha256") != sample_contract.identity_sha256:
                        raise PredicateMaterializationError("resume sampling identity drifted")
                    receipts.append(receipt)
                    continue
                pending.append((str(target_day), clock_group, frame_path, receipt_path))
        total = len(source_payload["target_days"]) * len(CLOCK_GROUPS)
        completed = len(receipts)
        started = monotonic()
        print(
            f"[2025-predicates] parts={completed}/{total} resumed={completed} "
            f"pending={len(pending)} workers={workers}",
            flush=True,
        )

        def record(receipt: dict[str, Any]) -> None:
            nonlocal completed
            receipts.append(receipt)
            completed += 1
            elapsed = monotonic() - started
            rate = (completed - (total - len(pending))) / elapsed if elapsed > 0.0 else 0.0
            remaining = total - completed
            eta = remaining / rate if rate > 0.0 else math.inf
            print(
                f"[2025-predicates] parts={completed}/{total} "
                f"clock={receipt['clock_group']} day={receipt['target_day']} "
                f"elapsed_s={elapsed:.1f} eta_s={eta:.1f}",
                flush=True,
            )

        if workers == 1:
            for target_day, clock_group, frame_path, receipt_path in pending:
                record(
                    _build_and_write_part(
                        source_payload=source_payload,
                        target_day=target_day,
                        clock_group=clock_group,
                        sampling=sample_contract,
                        frame_path=frame_path,
                        receipt_path=receipt_path,
                    )
                )
        else:
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(
                        _build_and_write_part,
                        source_payload=source_payload,
                        target_day=target_day,
                        clock_group=clock_group,
                        sampling=sample_contract,
                        frame_path=frame_path,
                        receipt_path=receipt_path,
                    ): (target_day, clock_group)
                    for target_day, clock_group, frame_path, receipt_path in pending
                }
                for future in as_completed(futures):
                    record(future.result())
        manifest = _write_final_payloads(
            work_dir=work_dir,
            source_manifest_path=source_path,
            source_payload=source_payload,
            run_identity=run_identity,
            sampling=sample_contract,
            quantiles=quantiles,
            part_receipts=receipts,
        )
        os.replace(work_dir, final_dir)
        _fsync_directory(root)
        validated = validate_admission(
            final_dir,
            source_manifest_path=source_path,
            rehash_sources=False,
        )
        return MaterializationResult(final_dir, validated, False)


def _parse_quantiles(value: str) -> tuple[float, ...]:
    try:
        parsed = tuple(float(item) for item in value.split(",") if item.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("quantiles must be comma-separated floats") from exc
    if not parsed:
        raise argparse.ArgumentTypeError("quantiles are empty")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--source-manifest", type=Path, default=DEFAULT_SOURCE_MANIFEST)
    build.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    build.add_argument("--sample-numerator", type=int, default=DEFAULT_SAMPLE_NUMERATOR)
    build.add_argument("--sample-denominator", type=int, default=DEFAULT_SAMPLE_DENOMINATOR)
    build.add_argument("--sample-salt", default=DEFAULT_SAMPLE_SALT)
    build.add_argument("--quantiles", type=_parse_quantiles, default=DEFAULT_QUANTILES)
    build.add_argument("--workers", type=int, default=2)
    validate = subparsers.add_parser("validate")
    validate.add_argument("admission_dir", type=Path)
    validate.add_argument("--source-manifest", type=Path, default=DEFAULT_SOURCE_MANIFEST)
    validate.add_argument("--rehash-sources", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "build":
        result = materialize_predicate_artifacts(
            source_manifest_path=args.source_manifest,
            output_root=args.output_root,
            sampling=TimestampHashSamplingContract(
                numerator=args.sample_numerator,
                denominator=args.sample_denominator,
                salt=args.sample_salt,
            ),
            quantiles=args.quantiles,
            workers=args.workers,
        )
        summary = {
            "admission_dir": str(result.admission_dir),
            "canonical_manifest_sha256": result.manifest["canonical_manifest_sha256"],
            "resumed": result.resumed,
            "action_authorized": False,
            "live_authorized": False,
        }
    else:
        manifest = validate_admission(
            args.admission_dir,
            source_manifest_path=args.source_manifest,
            rehash_sources=args.rehash_sources,
        )
        summary = {
            "admission_dir": str(args.admission_dir.resolve()),
            "canonical_manifest_sha256": manifest["canonical_manifest_sha256"],
            "valid": True,
            "action_authorized": False,
            "live_authorized": False,
        }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AUDIT_SCHEMA",
    "BOOK_CLOCK_GROUP",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_SOURCE_MANIFEST",
    "MaterializationResult",
    "OFFICIAL_TRADE_CHANNELS",
    "OfficialTradeWindowState",
    "PREDICATE_BUNDLE_SCHEMA",
    "PROVIDER_BOOK_CHANNELS",
    "PredicateMaterializationError",
    "RAW_EXACT_LEVEL_CHANNELS",
    "ReferenceRowsAudit",
    "SCHEMA_VERSION",
    "TRADE_CLOCK_GROUP",
    "TimestampHashSamplingContract",
    "admit_reference_frames",
    "build_clock_reference_rows",
    "canonical_sha256",
    "load_and_validate_source_manifest",
    "load_2025_predicate_bundle",
    "load_official_individual_trades",
    "load_provider_book_day",
    "materialize_predicate_artifacts",
    "stream_official_trade_windows",
    "timestamp_hash_selected",
    "validate_admission",
]
