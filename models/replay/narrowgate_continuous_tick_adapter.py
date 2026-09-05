"""Concrete restart-aware adapter for the authoritative NarrowGate tick replay.

The adapter deliberately checkpoints only at drained maintenance boundaries.
An active epoch may cross UTC midnights; the underlying tick replay therefore
owns orders, queues, cooldowns, held model values, and RNG state continuously
for the whole epoch.  UTC boundaries are accounting/cluster cuts only.

No economic result field from the replay summary is consumed here.  Cash,
inventory, entry price, and campaign carry are advanced from the authoritative
fill journal because those values are required inputs to the following epoch.
"""

from __future__ import annotations

import json
import math
import os
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Protocol

import numpy as np
import pandas as pd

from models.replay.continuous_accounting import ContinuousAccountingLedger
from models.replay.replay_state_checkpoint import ContinuousReplayState
from models.replay.restart_aware_continuous_ab import canonical_sha256, sha256_file
from models.replay.restart_aware_continuous_execution import ContinuousOperation
from models.tick_data_types import HistoricalBBOData, HistoricalL2Data

SCHEMA_VERSION = "narrowgate_authoritative_continuous_tick_adapter.v1"
CHECKPOINT_SCHEMA_VERSION = f"{SCHEMA_VERSION}.checkpoint"
RECEIPT_SCHEMA_VERSION = f"{SCHEMA_VERSION}.mechanics_receipt"
DAY_MS = 86_400_000


class NarrowGateContinuousAdapterError(RuntimeError):
    """Raised before an inexact or drifted continuous replay can advance."""


def _day_start_ms(day: str) -> int:
    return int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp() * 1_000)


def _utc_day(ts_ms: int) -> str:
    return datetime.fromtimestamp(int(ts_ms) / 1_000.0, tz=UTC).date().isoformat()


def _calendar_days(start_ts_ms: int, end_ts_ms: int) -> tuple[str, ...]:
    if end_ts_ms < start_ts_ms:
        raise NarrowGateContinuousAdapterError("calendar interval moved backward")
    start = _day_start_ms(_utc_day(start_ts_ms))
    terminal = _day_start_ms(_utc_day(max(start_ts_ms, end_ts_ms - 1)))
    return tuple(_utc_day(ts) for ts in range(start, terminal + DAY_MS, DAY_MS))


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="ascii") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class ReplayDayInput:
    """One hash-bound day of actual replay inputs for one arm."""

    day: str
    window: Any
    ml_data: tuple[Any, ...]
    market_window_sha256: str
    overlay_identity_sha256: str
    source_identity_sha256: str
    source_profile: str
    exact_queue_authority: bool
    exact_lifecycle_authority: bool

    def validate(self) -> None:
        if self.day != _utc_day(_day_start_ms(self.day)):
            raise NarrowGateContinuousAdapterError("replay input day is invalid")
        if self.source_profile not in {"native", "provider_normalized"}:
            raise NarrowGateContinuousAdapterError("replay source profile is invalid")
        for value in (
            self.market_window_sha256,
            self.overlay_identity_sha256,
            self.source_identity_sha256,
        ):
            if len(value) != 64:
                raise NarrowGateContinuousAdapterError("replay input identity is incomplete")
        if self.source_profile == "provider_normalized" and (
            self.exact_queue_authority or self.exact_lifecycle_authority
        ):
            raise NarrowGateContinuousAdapterError(
                "provider replay cannot claim exact queue or lifecycle authority"
            )
        if getattr(self.window, "ml_data", None) is not None:
            raise NarrowGateContinuousAdapterError(
                "continuous replay requires a model-free market window"
            )
        if not isinstance(self.ml_data, tuple) or not self.ml_data:
            raise NarrowGateContinuousAdapterError("ML-ON replay lacks its overlay")


class ReplayDayInputProvider(Protocol):
    def load_day(self, *, arm_id: str, day: str) -> ReplayDayInput: ...


@dataclass(frozen=True, slots=True)
class AuthoritativeReplayEpoch:
    epoch_id: str
    start_ts_ms: int
    quote_stop_ts_ms: int
    end_ts_ms: int
    warmup_lookback_start_ts_ms: int
    gap_id: str
    gap_end_ts_ms: int
    utc_boundaries_ts_ms: tuple[int, ...]
    source_days: tuple[str, ...]
    random_seed: int
    random_path_sha256: str
    terminal: bool

    def validate(self) -> None:
        if not self.epoch_id or not (
            self.warmup_lookback_start_ts_ms <= self.start_ts_ms
            < self.quote_stop_ts_ms
            < self.end_ts_ms
            <= self.gap_end_ts_ms
        ):
            raise NarrowGateContinuousAdapterError("authoritative epoch clock is invalid")
        if not self.source_days or tuple(sorted(set(self.source_days))) != self.source_days:
            raise NarrowGateContinuousAdapterError("authoritative epoch days are invalid")
        if any(
            not (self.start_ts_ms < boundary <= self.gap_end_ts_ms)
            for boundary in self.utc_boundaries_ts_ms
        ):
            raise NarrowGateContinuousAdapterError("UTC boundary escaped its epoch")
        if len(self.random_path_sha256) != 64:
            raise NarrowGateContinuousAdapterError("epoch random-path identity is invalid")


def compile_authoritative_epochs(
    operations: Sequence[ContinuousOperation],
    *,
    panel_cancel_drain_ms: int,
) -> tuple[AuthoritativeReplayEpoch, ...]:
    """Collapse the framework tape into restart-bounded real replay calls.

    In particular, ``utc_accounting`` never ends an engine epoch.  The final
    panel receives an actual shutdown drain using the already-frozen drain
    duration; this terminates exchange order state without flattening inventory.
    """

    if panel_cancel_drain_ms <= 0:
        raise NarrowGateContinuousAdapterError("panel cancel drain must be positive")
    rows = tuple(operations)
    if not rows:
        raise NarrowGateContinuousAdapterError("continuous operation tape is empty")
    for row in rows:
        row.validate()
    epochs: list[AuthoritativeReplayEpoch] = []
    online_start: int | None = None
    warmup_start: int | None = None
    days: set[str] = set()
    boundaries: list[int] = []
    seed: int | None = None
    random_hash: str | None = None
    pending_gap: tuple[str, int, int] | None = None

    def finish(*, quote_stop: int, end: int, gap_id: str, gap_end: int, terminal: bool) -> None:
        nonlocal online_start, warmup_start, days, boundaries, seed, random_hash
        if online_start is None:
            return
        effective_warmup = online_start if warmup_start is None else warmup_start
        payload = {
            "member_operation_random_path": random_hash,
            "seed": int(seed if seed is not None else 0),
            "epoch_ordinal": len(epochs) + 1,
            "shared_between_arms": True,
        }
        epoch = AuthoritativeReplayEpoch(
            epoch_id=f"epoch-{len(epochs) + 1:04d}",
            start_ts_ms=int(online_start),
            quote_stop_ts_ms=int(quote_stop),
            end_ts_ms=int(end),
            warmup_lookback_start_ts_ms=int(effective_warmup),
            gap_id=str(gap_id),
            gap_end_ts_ms=int(gap_end),
            utc_boundaries_ts_ms=tuple(sorted(set(boundaries))),
            source_days=tuple(sorted(days)),
            random_seed=int(seed if seed is not None else 0),
            random_path_sha256=canonical_sha256(payload),
            terminal=bool(terminal),
        )
        epoch.validate()
        epochs.append(epoch)
        online_start = None
        warmup_start = None
        days = set()
        boundaries = []
        seed = None
        random_hash = None

    for index, row in enumerate(rows):
        if row.kind == "warmup_resume":
            warmup_start = int(row.warmup_lookback_start_ts_ms or row.start_ts_ms)
            continue
        if row.kind == "online":
            if online_start is None:
                online_start = int(row.start_ts_ms)
                seed = int(row.random_seed)
                random_hash = row.random_path_sha256
            days.add(row.source_day)
            continue
        if row.kind == "utc_accounting":
            if online_start is not None:
                boundaries.append(int(row.start_ts_ms))
            continue
        if row.kind == "cancel_drain":
            if online_start is None:
                continue
            days.add(row.source_day)
            gap_end = int(row.end_ts_ms)
            gap_id = row.gap_id
            for future in rows[index + 1 :]:
                if future.kind == "offline_gap" and future.gap_id == gap_id:
                    gap_end = max(gap_end, int(future.end_ts_ms))
                elif future.kind == "warmup_resume" and future.gap_id == gap_id:
                    gap_end = max(gap_end, int(future.start_ts_ms))
                    break
                elif future.kind in {"online", "cancel_drain"}:
                    break
            finish(
                quote_stop=int(row.start_ts_ms),
                end=int(row.end_ts_ms),
                gap_id=gap_id,
                gap_end=gap_end,
                terminal=False,
            )
            pending_gap = (gap_id, int(row.end_ts_ms), gap_end)
            continue
        if row.kind == "panel_terminal":
            if online_start is None:
                continue
            end = int(row.start_ts_ms)
            quote_stop = end - int(panel_cancel_drain_ms)
            if quote_stop <= online_start:
                raise NarrowGateContinuousAdapterError("terminal epoch is shorter than cancel drain")
            finish(
                quote_stop=quote_stop,
                end=end,
                gap_id="panel_terminal_shutdown",
                gap_end=end,
                terminal=True,
            )
        if row.kind == "offline_gap" and pending_gap is not None:
            pending_gap = (
                pending_gap[0],
                min(pending_gap[1], int(row.start_ts_ms)),
                max(pending_gap[2], int(row.end_ts_ms)),
            )
    if online_start is not None:
        raise NarrowGateContinuousAdapterError("operation tape ended without panel terminal")
    if not epochs:
        raise NarrowGateContinuousAdapterError("operation tape produced no replay epochs")
    bridged_epochs: list[AuthoritativeReplayEpoch] = []
    for index, epoch in enumerate(epochs):
        if index + 1 >= len(epochs):
            bridged_epochs.append(epoch)
            continue
        next_start = epochs[index + 1].start_ts_ms
        if epoch.gap_end_ts_ms > next_start:
            raise NarrowGateContinuousAdapterError(
                "authoritative epochs overlap after their restart gap"
            )
        if epoch.gap_end_ts_ms == next_start:
            bridged_epochs.append(epoch)
            continue

        # A maintenance interval can contain a warmup followed immediately by
        # another drain/gap without an intervening online segment.  Such a
        # restart has no orders or fills, but inventory MTM must still carry to
        # the next real online epoch.  Prove that the operation tape covers the
        # entire bridge with non-trading operations before folding it into the
        # preceding checkpoint.
        cursor = epoch.gap_end_ts_ms
        bridge_gap_ids: list[str] = []
        for row in rows:
            if row.end_ts_ms <= cursor or row.start_ts_ms >= next_start:
                continue
            if row.start_ts_ms > cursor:
                break
            if row.kind not in {"cancel_drain", "offline_gap"}:
                raise NarrowGateContinuousAdapterError(
                    "uncompiled epoch bridge contains a trading operation"
                )
            cursor = max(cursor, min(row.end_ts_ms, next_start))
            if row.gap_id and row.gap_id not in bridge_gap_ids:
                bridge_gap_ids.append(row.gap_id)
            if cursor == next_start:
                break
        if cursor != next_start:
            raise NarrowGateContinuousAdapterError(
                "operation tape does not cover the inter-epoch maintenance bridge"
            )
        combined_gap_ids = list(dict.fromkeys([epoch.gap_id, *bridge_gap_ids]))
        bridged_epochs.append(
            replace(
                epoch,
                gap_id="+".join(value for value in combined_gap_ids if value),
                gap_end_ts_ms=next_start,
            )
        )

    all_boundaries = tuple(
        int(row.start_ts_ms) for row in rows if row.kind == "utc_accounting"
    )
    compiled = tuple(
        replace(
            epoch,
            utc_boundaries_ts_ms=tuple(
                boundary
                for boundary in all_boundaries
                if epoch.start_ts_ms < boundary <= epoch.gap_end_ts_ms
            ),
        )
        for epoch in bridged_epochs
    )
    for previous, current in zip(compiled, compiled[1:], strict=False):
        if previous.gap_end_ts_ms != current.start_ts_ms:
            raise NarrowGateContinuousAdapterError(
                "authoritative epoch chain is not calendar-continuous"
            )
    for epoch in compiled:
        epoch.validate()
    return compiled


def _slice_frame(frame: pd.DataFrame, start_ms: int, end_ms: int) -> pd.DataFrame:
    ts = frame["transact_time"].to_numpy(dtype=np.int64, copy=False)
    left = int(np.searchsorted(ts, start_ms, side="left"))
    right = int(np.searchsorted(ts, end_ms, side="left"))
    return frame.iloc[left:right].copy()


def _slice_vector_pair(
    ts: np.ndarray,
    values: np.ndarray,
    start_ms: int,
    end_ms: int,
    *,
    keep_previous: bool,
) -> tuple[np.ndarray, np.ndarray]:
    clock = np.asarray(ts, dtype=np.int64)
    left = int(np.searchsorted(clock, start_ms, side="left"))
    if keep_previous and left > 0:
        left -= 1
    right = int(np.searchsorted(clock, end_ms, side="left"))
    return clock[left:right].copy(), np.asarray(values)[left:right].copy()


def _slice_timed_payload(payload: Any, start_ms: int, end_ms: int) -> Any:
    if payload is None:
        return None
    if isinstance(payload, HistoricalBBOData):
        ts = np.asarray(payload.ts_ms, dtype=np.int64)
        left = int(np.searchsorted(ts, start_ms, side="left"))
        if left > 0:
            left -= 1
        right = int(np.searchsorted(ts, end_ms, side="left"))
        return HistoricalBBOData(
            ts[left:right].copy(),
            np.asarray(payload.best_bid)[left:right].copy(),
            np.asarray(payload.best_ask)[left:right].copy(),
            np.asarray(payload.bid_qty)[left:right].copy(),
            np.asarray(payload.ask_qty)[left:right].copy(),
            source=payload.source,
        )
    if isinstance(payload, HistoricalL2Data):
        ts = np.asarray(payload.ts_ms, dtype=np.int64)
        left = int(np.searchsorted(ts, start_ms, side="left"))
        if left > 0:
            left -= 1
        right = int(np.searchsorted(ts, end_ms, side="left"))
        return HistoricalL2Data(
            ts[left:right].copy(),
            np.asarray(payload.bid_px)[left:right].copy(),
            np.asarray(payload.bid_qty)[left:right].copy(),
            np.asarray(payload.ask_px)[left:right].copy(),
            np.asarray(payload.ask_qty)[left:right].copy(),
            source=payload.source,
        )
    if not isinstance(payload, (tuple, list)) or not payload:
        raise NarrowGateContinuousAdapterError("timed replay payload has unsupported shape")
    ts = np.asarray(payload[0], dtype=np.int64)
    left = int(np.searchsorted(ts, start_ms, side="left"))
    if left > 0:
        left -= 1
    right = int(np.searchsorted(ts, end_ms, side="left"))
    out: list[Any] = []
    for value in payload:
        array = np.asarray(value)
        if array.ndim == 0 or len(array) != len(ts):
            raise NarrowGateContinuousAdapterError("timed replay payload is misaligned")
        out.append(array[left:right].copy())
    return tuple(out)


def _slice_ml_data(payload: tuple[Any, ...], start_ms: int, end_ms: int) -> tuple[Any, ...]:
    ts = np.asarray(payload[0], dtype=np.int64)
    left = int(np.searchsorted(ts, start_ms, side="left"))
    if left > 0:
        left -= 1
    right = int(np.searchsorted(ts, end_ms, side="left"))
    out: list[Any] = []
    for value in payload:
        if isinstance(value, Mapping):
            mapped: dict[str, np.ndarray] = {}
            for name, raw in value.items():
                array = np.asarray(raw)
                if array.ndim != 1 or len(array) != len(ts):
                    raise NarrowGateContinuousAdapterError(
                        f"ML feature {name} is not aligned to its visibility clock"
                    )
                mapped[str(name)] = array[left:right].copy()
            out.append(mapped)
        else:
            array = np.asarray(value)
            if array.ndim != 1 or len(array) != len(ts):
                raise NarrowGateContinuousAdapterError("ML overlay array is misaligned")
            out.append(array[left:right].copy())
    return tuple(out)


def _concat_timed_payloads(payloads: Sequence[Any]) -> Any:
    present = [payload for payload in payloads if payload is not None]
    if not present:
        return None
    if isinstance(present[0], HistoricalBBOData):
        if any(not isinstance(payload, HistoricalBBOData) for payload in present):
            raise NarrowGateContinuousAdapterError("BBO payload type differs across days")
        ts = np.concatenate([payload.ts_ms for payload in present])
        order = np.argsort(ts, kind="stable")
        ordered_ts = ts[order]
        keep = np.r_[ordered_ts[1:] != ordered_ts[:-1], True]
        return HistoricalBBOData(
            ordered_ts[keep],
            np.concatenate([payload.best_bid for payload in present])[order][keep],
            np.concatenate([payload.best_ask for payload in present])[order][keep],
            np.concatenate([payload.bid_qty for payload in present])[order][keep],
            np.concatenate([payload.ask_qty for payload in present])[order][keep],
            source="continuous_bound_bbo",
        )
    if isinstance(present[0], HistoricalL2Data):
        if any(not isinstance(payload, HistoricalL2Data) for payload in present):
            raise NarrowGateContinuousAdapterError("L2 payload type differs across days")
        ts = np.concatenate([payload.ts_ms for payload in present])
        order = np.argsort(ts, kind="stable")
        ordered_ts = ts[order]
        keep = np.r_[ordered_ts[1:] != ordered_ts[:-1], True]
        return HistoricalL2Data(
            ordered_ts[keep],
            np.concatenate([payload.bid_px for payload in present], axis=0)[order][keep],
            np.concatenate([payload.bid_qty for payload in present], axis=0)[order][keep],
            np.concatenate([payload.ask_px for payload in present], axis=0)[order][keep],
            np.concatenate([payload.ask_qty for payload in present], axis=0)[order][keep],
            source="continuous_bound_l2",
        )
    width = len(present[0])
    if any(len(payload) != width for payload in present):
        raise NarrowGateContinuousAdapterError("timed payload ABI differs across days")
    return tuple(np.concatenate([np.asarray(payload[i]) for payload in present]) for i in range(width))


def _concat_ml_payloads(payloads: Sequence[tuple[Any, ...]]) -> tuple[Any, ...]:
    if not payloads:
        raise NarrowGateContinuousAdapterError("active epoch lacks ML overlays")
    width = len(payloads[0])
    if any(len(payload) != width for payload in payloads):
        raise NarrowGateContinuousAdapterError("ML overlay ABI differs across days")
    out: list[Any] = []
    for index in range(width):
        values = [payload[index] for payload in payloads]
        if isinstance(values[0], Mapping):
            keys = tuple(values[0])
            if any(tuple(value) != keys for value in values):
                raise NarrowGateContinuousAdapterError("ML feature mapping differs across days")
            out.append(
                {
                    key: np.concatenate([np.asarray(value[key]) for value in values])
                    for key in keys
                }
            )
        else:
            out.append(np.concatenate([np.asarray(value) for value in values]))
    ready = np.asarray(out[0], dtype=np.int64)
    order = np.argsort(ready, kind="stable")
    if len(np.unique(ready)) != len(ready):
        raise NarrowGateContinuousAdapterError("ML visibility clock contains duplicate rows")
    for index, value in enumerate(out):
        if isinstance(value, Mapping):
            out[index] = {name: np.asarray(array)[order] for name, array in value.items()}
        else:
            out[index] = np.asarray(value)[order]
    return tuple(out)


def assemble_epoch_input(
    rows: Sequence[ReplayDayInput],
    *,
    start_ts_ms: int,
    end_ts_ms: int,
) -> tuple[Any, tuple[Any, ...], tuple[dict[str, Any], ...]]:
    """Assemble one bounded in-memory epoch and immediately release day inputs."""

    if not rows:
        raise NarrowGateContinuousAdapterError("epoch has no day inputs")
    sliced_windows: list[Any] = []
    overlays: list[tuple[Any, ...]] = []
    authority: list[dict[str, Any]] = []
    for row in rows:
        row.validate()
        window = row.window
        trades = _slice_frame(window.trades, start_ts_ms, end_ts_ms)
        if trades.empty:
            continue
        var_ts, var_ssq = _slice_vector_pair(
            window.var_ts_ms,
            window.var_ssq,
            start_ts_ms,
            end_ts_ms,
            keep_previous=True,
        )
        var_ti = None
        if window.var_ti is not None:
            _, var_ti = _slice_vector_pair(
                window.var_ts_ms,
                window.var_ti,
                start_ts_ms,
                end_ts_ms,
                keep_previous=True,
            )
        var_retsq = None
        if window.var_retsq is not None:
            _, var_retsq = _slice_vector_pair(
                window.var_ts_ms,
                window.var_retsq,
                start_ts_ms,
                end_ts_ms,
                keep_previous=True,
            )
        sliced_windows.append(
            SimpleNamespace(
                trades=trades,
                var_ts_ms=var_ts,
                var_ssq=var_ssq,
                var_ti=var_ti,
                var_retsq=var_retsq,
                bbo_data=_slice_timed_payload(window.bbo_data, start_ts_ms, end_ts_ms),
                l2_data=_slice_timed_payload(window.l2_data, start_ts_ms, end_ts_ms),
            )
        )
        overlays.append(_slice_ml_data(row.ml_data, start_ts_ms, end_ts_ms))
        authority.append(
            {
                "day": row.day,
                "source_profile": row.source_profile,
                "source_identity_sha256": row.source_identity_sha256,
                "market_window_sha256": row.market_window_sha256,
                "overlay_identity_sha256": row.overlay_identity_sha256,
                "exact_queue_authority": row.exact_queue_authority,
                "exact_lifecycle_authority": row.exact_lifecycle_authority,
                "continuous_pnl_inventory_campaign_sensitivity_authority": True,
                "q90_authority": False,
            }
        )
    if not sliced_windows:
        raise NarrowGateContinuousAdapterError("epoch has no executable market events")
    trades = pd.concat([window.trades for window in sliced_windows], ignore_index=True)
    trades = trades.sort_values("transact_time", kind="stable").reset_index(drop=True)
    var_ts = np.concatenate([window.var_ts_ms for window in sliced_windows])
    var_order = np.argsort(var_ts, kind="stable")
    ordered_var_ts = var_ts[var_order]
    var_keep = np.r_[ordered_var_ts[1:] != ordered_var_ts[:-1], True]
    window = SimpleNamespace(
        trades=trades,
        var_ts_ms=ordered_var_ts[var_keep],
        var_ssq=np.concatenate([window.var_ssq for window in sliced_windows])[var_order][var_keep],
        var_ti=(
            np.concatenate([window.var_ti for window in sliced_windows])[var_order][var_keep]
            if all(window.var_ti is not None for window in sliced_windows)
            else None
        ),
        var_retsq=(
            np.concatenate([window.var_retsq for window in sliced_windows])[var_order][var_keep]
            if all(window.var_retsq is not None for window in sliced_windows)
            else None
        ),
        bbo_data=_concat_timed_payloads([window.bbo_data for window in sliced_windows]),
        l2_data=_concat_timed_payloads([window.l2_data for window in sliced_windows]),
        ml_data=None,
    )
    return window, _concat_ml_payloads(overlays), tuple(authority)


@dataclass(frozen=True, slots=True)
class AdapterArmBinding:
    arm_id: str
    params: Mapping[str, Any]
    policy_identity_sha256: str
    cadence_ms: int

    def validate(self) -> None:
        if self.cadence_ms not in {1_000, 10_000}:
            raise NarrowGateContinuousAdapterError("F03 arm cadence is invalid")
        if len(self.policy_identity_sha256) != 64:
            raise NarrowGateContinuousAdapterError("F03 policy identity is incomplete")
        if self.params.get("ml_enabled") is not True:
            raise NarrowGateContinuousAdapterError("both F03 arms must be ML-ON")
        if bool(self.params.get("dynamic_fill_hazard_action_enabled", False)):
            raise NarrowGateContinuousAdapterError("q90 action must be OFF")
        if bool(self.params.get("buy_fill_selection_live_enabled", False)):
            raise NarrowGateContinuousAdapterError("BUY fill selector must be OFF")


class NarrowGateContinuousTickReplayAdapter:
    """Run restart-bounded epochs through the real NarrowGate C++ tick engine."""

    def __init__(
        self,
        *,
        plan_identity_sha256: str,
        operations: Sequence[ContinuousOperation],
        arm_bindings: Mapping[str, AdapterArmBinding],
        input_provider: ReplayDayInputProvider,
        initial_states: Mapping[str, ContinuousReplayState],
        output_root: Path,
        panel_cancel_drain_ms: int,
        simulate: Callable[..., Mapping[str, Any]] | None = None,
    ) -> None:
        if len(plan_identity_sha256) != 64:
            raise NarrowGateContinuousAdapterError("plan identity is invalid")
        if set(arm_bindings) != set(initial_states) or len(arm_bindings) != 2:
            raise NarrowGateContinuousAdapterError("paired replay requires exactly two arms")
        self.plan_identity_sha256 = plan_identity_sha256
        self.operations = tuple(operations)
        self.epochs = compile_authoritative_epochs(
            self.operations, panel_cancel_drain_ms=panel_cancel_drain_ms
        )
        self.arm_bindings = dict(arm_bindings)
        self.input_provider = input_provider
        self.initial_states = dict(initial_states)
        self.output_root = output_root.expanduser().resolve()
        self._simulate = simulate
        for arm, binding in self.arm_bindings.items():
            binding.validate()
            state = self.initial_states[arm]
            state.validate(require_restart_safe=True)
            if state.arm_id != arm:
                raise NarrowGateContinuousAdapterError("initial state crossed arm identity")
        control, candidate = tuple(self.arm_bindings.values())
        ignored = {"rng_seed", "initial_inventory", "initial_entry_price", "planned_quote_stop_ts_ms"}
        control_params = {k: v for k, v in control.params.items() if k not in ignored}
        candidate_params = {k: v for k, v in candidate.params.items() if k not in ignored}
        if control_params != candidate_params:
            raise NarrowGateContinuousAdapterError(
                "paired F03 arms may differ only by bound ML cadence/overlay"
            )

    def _warmup_receipt(
        self,
        *,
        arm: str,
        epoch: AuthoritativeReplayEpoch,
    ) -> dict[str, Any]:
        if epoch.warmup_lookback_start_ts_ms >= epoch.start_ts_ms:
            return {
                "required": False,
                "market_event_count": 0,
                "prediction_row_count": 0,
                "latest_prediction_ready_ts_ms": None,
                "quoting_enabled": False,
                "source_identity_sha256": canonical_sha256([]),
            }
        event_count = 0
        prediction_count = 0
        latest_ready: int | None = None
        identities: list[dict[str, str]] = []
        for day in _calendar_days(
            epoch.warmup_lookback_start_ts_ms, epoch.start_ts_ms
        ):
            row = self.input_provider.load_day(arm_id=arm, day=day)
            row.validate()
            trades = _slice_frame(
                row.window.trades,
                epoch.warmup_lookback_start_ts_ms,
                epoch.start_ts_ms,
            )
            ready = np.asarray(row.ml_data[0], dtype=np.int64)
            left = int(
                np.searchsorted(ready, epoch.warmup_lookback_start_ts_ms, side="left")
            )
            right = int(np.searchsorted(ready, epoch.start_ts_ms, side="right"))
            visible = ready[left:right]
            if visible.size and int(visible[-1]) > epoch.start_ts_ms:
                raise NarrowGateContinuousAdapterError(
                    "warmup consumed a future-ready model row"
                )
            event_count += len(trades)
            prediction_count += len(visible)
            if visible.size:
                latest_ready = max(
                    latest_ready if latest_ready is not None else int(visible[-1]),
                    int(visible[-1]),
                )
            identities.append(
                {
                    "day": day,
                    "market_window_sha256": row.market_window_sha256,
                    "overlay_identity_sha256": row.overlay_identity_sha256,
                }
            )
        if event_count <= 0 or prediction_count <= 0:
            raise NarrowGateContinuousAdapterError(
                "restart warmup lacks official past-only market/model events"
            )
        return {
            "required": True,
            "market_event_count": event_count,
            "prediction_row_count": prediction_count,
            "latest_prediction_ready_ts_ms": latest_ready,
            "feature_ready_not_after_resume": bool(
                latest_ready is not None and latest_ready <= epoch.start_ts_ms
            ),
            "quoting_enabled": False,
            "source_identity_sha256": canonical_sha256(identities),
        }

    def _mark_price(self, *, arm: str, ts_ms: int) -> float:
        candidates: list[tuple[int, float]] = []
        for day in _calendar_days(max(0, ts_ms - DAY_MS), ts_ms + 1):
            try:
                row = self.input_provider.load_day(arm_id=arm, day=day)
            except (KeyError, FileNotFoundError, NarrowGateContinuousAdapterError):
                continue
            row.validate()
            frame = row.window.trades
            clock = frame["transact_time"].to_numpy(dtype=np.int64, copy=False)
            prices = frame["price"].to_numpy(dtype=np.float64, copy=False)
            if not len(clock):
                continue
            before = int(np.searchsorted(clock, ts_ms, side="right") - 1)
            if before >= 0:
                candidates.append((int(clock[before]), float(prices[before])))
            after = int(np.searchsorted(clock, ts_ms, side="left"))
            if after < len(clock):
                candidates.append((int(clock[after]), float(prices[after])))
        if not candidates:
            raise NarrowGateContinuousAdapterError("gap/accounting mark has no market price")
        past = [row for row in candidates if row[0] <= ts_ms]
        selected = max(past) if past else min(candidates)
        if not math.isfinite(selected[1]) or selected[1] <= 0.0:
            raise NarrowGateContinuousAdapterError("gap/accounting mark is invalid")
        return selected[1]

    def _advance_offline_gap(
        self,
        *,
        arm: str,
        epoch: AuthoritativeReplayEpoch,
        ledger: ContinuousAccountingLedger,
    ) -> dict[str, Any]:
        if epoch.gap_end_ts_ms <= epoch.end_ts_ms:
            return {
                "gap_id": epoch.gap_id,
                "start_ts_ms": epoch.end_ts_ms,
                "end_ts_ms": epoch.gap_end_ts_ms,
                "market_event_trading_enabled": False,
                "fill_count": 0,
                "utc_boundary_count": 0,
            }
        start_price = self._mark_price(arm=arm, ts_ms=epoch.end_ts_ms)
        boundaries = tuple(
            boundary
            for boundary in epoch.utc_boundaries_ts_ms
            if epoch.end_ts_ms < boundary <= epoch.gap_end_ts_ms
        )
        cursor = epoch.end_ts_ms
        cursor_price = start_price
        for boundary in boundaries:
            boundary_price = self._mark_price(arm=arm, ts_ms=boundary - 1)
            ledger.mark(boundary, boundary_price)
            ledger.close_utc_day(day_end_ts_ms=boundary, mark_price=boundary_price)
            cursor = boundary
            cursor_price = boundary_price
        end_price = self._mark_price(arm=arm, ts_ms=epoch.gap_end_ts_ms)
        ledger.record_gap(
            gap_id=epoch.gap_id,
            start_ts_ms=cursor,
            end_ts_ms=epoch.gap_end_ts_ms,
            start_mark_price=cursor_price,
            end_mark_price=end_price,
        )
        return {
            "gap_id": epoch.gap_id,
            "start_ts_ms": epoch.end_ts_ms,
            "end_ts_ms": epoch.gap_end_ts_ms,
            "market_event_trading_enabled": False,
            "fill_count": 0,
            "utc_boundary_count": len(boundaries),
            "inventory_unchanged": True,
            "cash_unchanged": True,
        }

    def _simulate_epoch(
        self,
        *,
        arm: str,
        epoch: AuthoritativeReplayEpoch,
        ledger: ContinuousAccountingLedger,
    ) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
        warmup = self._warmup_receipt(arm=arm, epoch=epoch)
        rows = [self.input_provider.load_day(arm_id=arm, day=day) for day in epoch.source_days]
        window, ml_data, authority = assemble_epoch_input(
            rows,
            start_ts_ms=epoch.start_ts_ms,
            end_ts_ms=epoch.end_ts_ms,
        )
        binding = self.arm_bindings[arm]
        params = dict(binding.params)
        params.update(
            {
                "ml_enabled": True,
                "dynamic_fill_hazard_action_enabled": False,
                "buy_fill_selection_live_enabled": False,
                "initial_inventory": ledger.state.position_btc,
                "initial_entry_price": ledger.state.average_entry_price,
                "rng_seed": epoch.random_seed,
                "planned_quote_stop_ts_ms": epoch.quote_stop_ts_ms,
                "replay_event_clock_end_ts_ms": epoch.end_ts_ms,
                "collect_curves": False,
                "trace_fills_max": max(int(params.get("trace_fills_max", 0) or 0), 1_000_000),
                "trace_quotes_max": max(int(params.get("trace_quotes_max", 0) or 0), 1_000_000),
            }
        )
        simulate = self._simulate
        if simulate is None:
            from models import backtest_tick as bt

            simulate = bt._simulate_tick_with_engine
        result = dict(
            simulate(
                "cpp",
                window.trades,
                window.var_ts_ms,
                window.var_ssq,
                params,
                ml_data=ml_data,
                bbo_data=window.bbo_data,
                l2_data=window.l2_data,
                var_ti=window.var_ti,
                var_retsq=window.var_retsq,
            )
        )
        if result.get("planned_quote_stop_triggered") is not True:
            raise NarrowGateContinuousAdapterError("actual replay did not reach quote stop")
        remaining = sum(
            int(result.get(name, 0) or 0)
            for name in (
                "planned_shutdown_open_order_count",
                "planned_shutdown_pending_new_order_count",
                "planned_shutdown_pending_cancel_order_count",
            )
        )
        if remaining:
            raise NarrowGateContinuousAdapterError(
                f"maintenance boundary retained {remaining} exchange orders"
            )
        fills = list(result.get("_fill_trace") or ())
        if len(fills) != int(result.get("fills_total", len(fills)) or 0):
            raise NarrowGateContinuousAdapterError("authoritative fill journal was truncated")
        timeline: list[tuple[int, int, Any]] = []
        for row in fills:
            # UTC days are half-open: close the old day before a midnight fill
            # changes cash or inventory for the new day.
            timeline.append((int(row["fill_ts"]), 1, row))
        for boundary in epoch.utc_boundaries_ts_ms:
            if boundary <= epoch.end_ts_ms:
                timeline.append((int(boundary), 0, None))
        trade_ts = window.trades["transact_time"].to_numpy(dtype=np.int64, copy=False)
        trade_px = window.trades["price"].to_numpy(dtype=np.float64, copy=False)

        def mark_at(ts_ms: int) -> float:
            index = int(np.searchsorted(trade_ts, ts_ms, side="right") - 1)
            if index < 0:
                raise NarrowGateContinuousAdapterError("accounting mark lacks past market price")
            return float(trade_px[index])

        campaign_ordinal = len(ledger.closed_campaigns)
        for ts_ms, kind, row in sorted(timeline, key=lambda value: (value[0], value[1])):
            if kind == 1:
                assert row is not None
                before = ledger.state.position_btc
                side = str(row["side"]).upper()
                qty = float(row["fill_qty"])
                signed = qty if side == "BUY" else -qty
                after = before + signed
                new_id = None
                if abs(before) <= 1e-10 or before * after < -1e-20:
                    campaign_ordinal += 1
                    new_id = f"{arm}:{epoch.epoch_id}:campaign-{campaign_ordinal:06d}"
                ledger.mark(ts_ms, mark_at(ts_ms))
                ledger.fill(
                    ts_ms=ts_ms,
                    side=side,
                    quantity_btc=qty,
                    price=float(row.get("quote_px", row.get("fill_trade_px"))),
                    fee_usdc=float(row.get("fill_fee_usdc", 0.0) or 0.0),
                    new_campaign_id=new_id,
                )
            else:
                ledger.close_utc_day(day_end_ts_ms=ts_ms, mark_price=mark_at(ts_ms - 1))
        observed_inventory = float(result.get("final_inventory", ledger.state.position_btc))
        if not math.isclose(
            observed_inventory,
            ledger.state.position_btc,
            rel_tol=0.0,
            abs_tol=1e-10,
        ):
            raise NarrowGateContinuousAdapterError("fill journal inventory differs from replay")
        ledger.mark(epoch.end_ts_ms, mark_at(epoch.end_ts_ms))
        quote_trace = list(result.get("_quote_trace") or ())
        terminal_fills = sum(int(row["fill_ts"]) >= epoch.quote_stop_ts_ms for row in fills)
        mechanics = {
            "epoch_id": epoch.epoch_id,
            "arm": arm,
            "actual_tick_replay_used": True,
            "quote_count": len(quote_trace),
            "fill_count": len(fills),
            "terminal_fill_count": terminal_fills,
            "cancel_request_count": int(
                result.get("planned_shutdown_orders_at_trigger", 0) or 0
            ),
            "cancel_ack_count": sum(
                str(row.get("outcome", "")).lower() == "cancel" for row in quote_trace
            ),
            "planned_quote_stop_triggered": True,
            "planned_shutdown_remaining_orders": remaining,
            "warmup_lookback_start_ts_ms": epoch.warmup_lookback_start_ts_ms,
            "quote_resume_ts_ms": epoch.start_ts_ms,
            "warmup": warmup,
            "random_seed": epoch.random_seed,
            "random_path_sha256": epoch.random_path_sha256,
            "policy_identity_sha256": binding.policy_identity_sha256,
            "cadence_ms": binding.cadence_ms,
            "source_authority": list(authority),
            "provider_exact_queue_claim_count": sum(
                row["source_profile"] == "provider_normalized" and row["exact_queue_authority"]
                for row in authority
            ),
            "provider_exact_lifecycle_claim_count": sum(
                row["source_profile"] == "provider_normalized"
                and row["exact_lifecycle_authority"]
                for row in authority
            ),
            "economic_outputs_read": False,
        }
        return mechanics, authority

    def _checkpoint_payload(
        self,
        *,
        arm: str,
        epoch: AuthoritativeReplayEpoch,
        ledger: ContinuousAccountingLedger,
        previous_sha256: str,
        mechanics: Mapping[str, Any],
    ) -> dict[str, Any]:
        restarted = ledger.enter_planned_restart(epoch.gap_end_ts_ms)
        ledger_snapshot = {
            "state": restarted.to_dict(),
            "day_start_equity_usdc": float(ledger._day_start_equity),
            "day_start_inventory_btc": float(ledger._day_start_inventory),
            "day_start": str(ledger._day_start),
            "daily_slices": [asdict(row) for row in ledger.daily_slices],
            "closed_campaigns": [asdict(row) for row in ledger.closed_campaigns],
            "gap_carries": [asdict(row) for row in ledger.gap_carries],
        }
        engine_state = {
            "schema_version": SCHEMA_VERSION,
            "checkpoint_boundary": "post_cancel_ack_drain",
            "active_orders": [],
            "pending_new_orders": [],
            "pending_cancel_orders": [],
            "queue_positions": [],
            "queue_cursors": [],
            "q90_cursors": [],
            "cooldown_lineages": {},
            "campaign_reward_path": (
                asdict(restarted.economic_campaign)
                if restarted.economic_campaign is not None
                else None
            ),
            "feature_model_held_state": {
                "cleared_by_production_restart": True,
                "warmup_required_before_next_quote": not epoch.terminal,
            },
            "rng_state": {
                "algorithm": "operation_keyed_seed_v1",
                "completed_epoch_random_path_sha256": epoch.random_path_sha256,
            },
            "accounting_state_sha256": canonical_sha256(ledger_snapshot),
            "runtime_reset_fields": list(restarted.runtime_reset_fields),
        }
        payload = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "plan_identity_sha256": self.plan_identity_sha256,
            "arm_id": arm,
            "epoch_id": epoch.epoch_id,
            "state": restarted.to_dict(),
            "state_sha256": canonical_sha256(restarted.to_dict()),
            "ledger_state": ledger_snapshot,
            "ledger_state_sha256": canonical_sha256(ledger_snapshot),
            "engine_state": engine_state,
            "engine_state_sha256": canonical_sha256(engine_state),
            "mechanics_sha256": canonical_sha256(dict(mechanics)),
            "previous_checkpoint_sha256": previous_sha256,
            "economic_outcomes_read": False,
            "promotion_authorized": False,
        }
        payload["checkpoint_sha256"] = canonical_sha256(payload)
        return payload

    def run(self, *, max_epochs: int | None = None) -> dict[str, Any]:
        """Execute mechanics and checkpoint atomically; never aggregate PnL."""

        self.output_root.mkdir(parents=True, exist_ok=True)
        arm_ledgers: dict[str, ContinuousAccountingLedger] = {}
        previous: dict[str, str] = {arm: "" for arm in self.arm_bindings}
        start_epoch = 0
        receipt_dir = self.output_root / "receipts"
        admitted_receipts = (
            sorted(receipt_dir.glob("epoch-*.json")) if receipt_dir.exists() else []
        )
        admitted: dict[str, Any] | None = None
        if admitted_receipts:
            receipt_path = admitted_receipts[-1]
            marker = receipt_path.with_suffix(".success")
            if (
                not marker.is_file()
                or marker.read_text(encoding="ascii").strip() != sha256_file(receipt_path)
            ):
                raise NarrowGateContinuousAdapterError("paired receipt admission drifted")
            admitted = json.loads(receipt_path.read_text(encoding="utf-8"))
            receipt_sha = str(admitted.pop("receipt_sha256", ""))
            if canonical_sha256(admitted) != receipt_sha:
                raise NarrowGateContinuousAdapterError("paired receipt canonical hash drifted")
            admitted["receipt_sha256"] = receipt_sha
            if admitted.get("plan_identity_sha256") != self.plan_identity_sha256:
                raise NarrowGateContinuousAdapterError("paired receipt belongs to another plan")
            checkpoint_refs = admitted.get("checkpoints")
            if set(checkpoint_refs or {}) != set(self.arm_bindings):
                raise NarrowGateContinuousAdapterError(
                    "paired receipt does not bind both arm checkpoints"
                )
            start_epoch = int(str(admitted["epoch"]["epoch_id"]).split("-")[-1])

        for arm in self.arm_bindings:
            if admitted is None:
                arm_ledgers[arm] = ContinuousAccountingLedger(self.initial_states[arm])
                continue
            checkpoint_ref = admitted["checkpoints"][arm]
            checkpoint_path = Path(str(checkpoint_ref["path"])).expanduser().resolve()
            marker = checkpoint_path.with_suffix(".success")
            if (
                not checkpoint_path.is_file()
                or not marker.is_file()
                or marker.read_text(encoding="ascii").strip()
                != sha256_file(checkpoint_path)
                or checkpoint_ref.get("file_sha256") != sha256_file(checkpoint_path)
            ):
                raise NarrowGateContinuousAdapterError("admitted checkpoint file drifted")
            latest = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            expected = str(latest.pop("checkpoint_sha256", ""))
            if canonical_sha256(latest) != expected:
                raise NarrowGateContinuousAdapterError("checkpoint canonical hash drifted")
            latest["checkpoint_sha256"] = expected
            if (
                expected != checkpoint_ref.get("checkpoint_sha256")
                or latest.get("plan_identity_sha256") != self.plan_identity_sha256
                or latest.get("epoch_id") != admitted["epoch"]["epoch_id"]
            ):
                raise NarrowGateContinuousAdapterError("checkpoint admission identity drifted")
            state = ContinuousReplayState.from_dict(latest["state"])
            state.validate(require_restart_safe=True)
            ledger_payload = latest.get("ledger_state")
            if not isinstance(ledger_payload, Mapping) or canonical_sha256(
                dict(ledger_payload)
            ) != latest.get("ledger_state_sha256"):
                raise NarrowGateContinuousAdapterError("checkpoint ledger state drifted")
            ledger = ContinuousAccountingLedger(state)
            from models.replay.continuous_accounting import (
                ClosedCampaign,
                DailyPnlSlice,
                GapCarry,
            )

            ledger._day_start_equity = float(ledger_payload["day_start_equity_usdc"])
            ledger._day_start_inventory = float(ledger_payload["day_start_inventory_btc"])
            ledger._day_start = str(ledger_payload["day_start"])
            ledger.daily_slices = [
                DailyPnlSlice(**row) for row in ledger_payload.get("daily_slices", ())
            ]
            ledger.closed_campaigns = [
                ClosedCampaign(**row) for row in ledger_payload.get("closed_campaigns", ())
            ]
            ledger.gap_carries = [
                GapCarry(**row) for row in ledger_payload.get("gap_carries", ())
            ]
            arm_ledgers[arm] = ledger
            previous[arm] = expected
        completed = 0
        receipts: list[dict[str, Any]] = []
        for epoch_index, epoch in enumerate(self.epochs, start=1):
            if epoch_index <= start_epoch:
                continue
            if max_epochs is not None and completed >= max_epochs:
                break
            paired: dict[str, Any] = {}
            paired_checkpoints: dict[str, dict[str, str]] = {}
            for arm in self.arm_bindings:
                ledger = arm_ledgers[arm]
                mechanics, _ = self._simulate_epoch(arm=arm, epoch=epoch, ledger=ledger)
                mechanics = {
                    **mechanics,
                    "offline_gap": self._advance_offline_gap(
                        arm=arm,
                        epoch=epoch,
                        ledger=ledger,
                    ),
                }
                checkpoint = self._checkpoint_payload(
                    arm=arm,
                    epoch=epoch,
                    ledger=ledger,
                    previous_sha256=previous[arm],
                    mechanics=mechanics,
                )
                checkpoint_path = self.output_root / "checkpoints" / arm / f"{epoch.epoch_id}.json"
                _atomic_json(checkpoint_path, checkpoint)
                _atomic_text(
                    checkpoint_path.with_suffix(".success"),
                    sha256_file(checkpoint_path) + "\n",
                )
                previous[arm] = checkpoint["checkpoint_sha256"]
                paired[arm] = mechanics
                paired_checkpoints[arm] = {
                    "path": str(checkpoint_path),
                    "file_sha256": sha256_file(checkpoint_path),
                    "checkpoint_sha256": checkpoint["checkpoint_sha256"],
                }
            random_paths = {row["random_path_sha256"] for row in paired.values()}
            if len(random_paths) != 1:
                raise NarrowGateContinuousAdapterError("paired arms consumed different random paths")
            receipt = {
                "schema_version": RECEIPT_SCHEMA_VERSION,
                "plan_identity_sha256": self.plan_identity_sha256,
                "epoch": asdict(epoch),
                "arms": paired,
                "checkpoints": paired_checkpoints,
                "same_random_path": True,
                "economic_outcomes_read": False,
                "promotion_authorized": False,
            }
            receipt["receipt_sha256"] = canonical_sha256(receipt)
            receipt_path = self.output_root / "receipts" / f"{epoch.epoch_id}.json"
            _atomic_json(receipt_path, receipt)
            _atomic_text(receipt_path.with_suffix(".success"), sha256_file(receipt_path) + "\n")
            receipts.append(receipt)
            completed += 1
        return {
            "schema_version": f"{SCHEMA_VERSION}.run",
            "plan_identity_sha256": self.plan_identity_sha256,
            "epoch_count": len(self.epochs),
            "epochs_completed_this_call": completed,
            "last_completed_epoch": start_epoch + completed,
            "checkpoint_sha256": previous,
            "receipt_sha256": [row["receipt_sha256"] for row in receipts],
            "economic_outcomes_read": False,
            "economic_results_aggregated": False,
            "promotion_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
        }
