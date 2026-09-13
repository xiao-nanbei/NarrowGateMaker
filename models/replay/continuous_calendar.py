"""Static calendar plans for restart-aware continuous replay."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from data.quality.calendar_gap_manifest import (
    DAY_MS,
    validate_calendar_continuity_manifest,
)

SCHEMA_VERSION = "versioned_continuous_replay_substrate.v1"


class ReplayMode(str, Enum):
    ANCHOR_PANEL_CONTINUOUS = "anchor_panel_continuous"
    NATIVE_STRICT_CONTINUOUS = "native_strict_continuous"
    RESTART_AWARE_CALENDAR = "restart_aware_calendar"
    PROVIDER_NORMALIZED_CONTINUOUS = "provider_normalized_continuous"


@dataclass(frozen=True)
class RestartScheduleInterval:
    gap_id: str
    offline_start_ts_ms: int
    resume_snapshot_ts_ms: int
    quote_stop_ts_ms: int
    cancel_deadline_ts_ms: int
    warmup_lookback_start_ts_ms: int

    def validate(self) -> None:
        if not self.gap_id.strip():
            raise ValueError("restart schedule gap id must be non-empty")
        if not (
            self.quote_stop_ts_ms
            <= self.cancel_deadline_ts_ms
            < self.offline_start_ts_ms
            < self.resume_snapshot_ts_ms
        ):
            raise ValueError("restart schedule timestamps are not ordered")


@dataclass(frozen=True)
class ReplayAdapterCapabilities:
    arm_specific_economic_state: bool
    carries_cash_inventory_entry: bool
    carries_economic_campaign: bool
    utc_midnight_accounting_only: bool
    observable_cancel_drain: bool
    no_strategy_fills_while_offline: bool
    clears_transient_runtime_state: bool
    fresh_snapshot_on_restart: bool
    past_only_feature_warmup: bool
    continuous_inventory_mtm: bool
    versioned_checkpoint_roundtrip: bool


REQUIRED_ADAPTER_CAPABILITIES = tuple(ReplayAdapterCapabilities.__dataclass_fields__)


def validate_adapter_capabilities(capabilities: ReplayAdapterCapabilities) -> None:
    missing = [
        name for name in REQUIRED_ADAPTER_CAPABILITIES if not getattr(capabilities, name)
    ]
    if missing:
        raise RuntimeError(
            "continuous replay adapter is missing required capabilities: "
            + ",".join(missing)
        )


def _day_start_ms(day: str) -> int:
    return int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp() * 1_000)


def _merge_intervals(intervals: Sequence[tuple[int, int]]) -> list[tuple[int, int]]:
    ordered = sorted((int(start), int(end)) for start, end in intervals if start < end)
    merged: list[list[int]] = []
    for start, end in ordered:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return [(start, end) for start, end in merged]


def _active_for_mode(row: Mapping[str, Any], mode: ReplayMode) -> bool:
    usable = bool(row.get("strategy_tape_usable"))
    if mode == ReplayMode.ANCHOR_PANEL_CONTINUOUS:
        return usable and bool(row.get("anchor_target_day"))
    if mode == ReplayMode.NATIVE_STRICT_CONTINUOUS:
        return usable and str(row.get("quality_grade")) == "A"
    if mode == ReplayMode.RESTART_AWARE_CALENDAR:
        return usable and str(row.get("quality_grade")) in {"A", "B", "C"}
    if mode == ReplayMode.PROVIDER_NORMALIZED_CONTINUOUS:
        return bool(row.get("provider_normalized_tape_usable"))
    raise ValueError(f"unsupported replay mode: {mode}")


def _resume_at_first_visible(
    resume_ts_ms: int,
    rows_by_day: Mapping[str, Mapping[str, Any]],
) -> int:
    day = datetime.fromtimestamp(resume_ts_ms / 1_000.0, tz=UTC).date().isoformat()
    row = rows_by_day.get(day)
    if row is None:
        return int(resume_ts_ms)
    first = row.get("first_timestamp_ms")
    if first is None:
        return int(resume_ts_ms)
    return max(int(resume_ts_ms), int(first))


@dataclass(frozen=True)
class CalendarReplayPlan:
    mode: ReplayMode
    manifest_path: str
    manifest_sha256: str
    manifest_canonical_sha256: str
    source_manifest_calendar_day_count: int
    calendar_start_day: str
    calendar_end_day: str
    calendar_day_count: int
    anchor_target_days: tuple[str, ...]
    active_days: tuple[str, ...]
    restart_intervals: tuple[RestartScheduleInterval, ...]
    initial_offline_until_ts_ms: int | None
    final_offline_from_ts_ms: int | None
    utc_accounting_boundaries_ts_ms: tuple[int, ...]
    economic_mark_bridge_complete: bool
    full_tick_runner_binding: bool
    exact_queue_lifecycle_authority: bool
    action_or_live_authority: bool
    timeline_sha256: str
    schema_version: str = SCHEMA_VERSION

    @classmethod
    def from_manifest(
        cls,
        path: Path,
        *,
        mode: ReplayMode | str,
    ) -> CalendarReplayPlan:
        path = path.expanduser().resolve()
        raw = path.read_bytes()
        manifest = json.loads(raw)
        validate_calendar_continuity_manifest(manifest)
        mode = ReplayMode(mode)
        all_rows = list(manifest["day_sources"])
        anchor_days = tuple(str(day) for day in manifest["anchor_target_days"])
        if mode == ReplayMode.ANCHOR_PANEL_CONTINUOUS:
            effective_start_day = anchor_days[0]
            effective_end_day = anchor_days[-1]
            rows = [
                row
                for row in all_rows
                if effective_start_day <= str(row["day"]) <= effective_end_day
            ]
        else:
            effective_start_day = str(manifest["calendar_start_day"])
            effective_end_day = str(manifest["calendar_end_day"])
            rows = all_rows
        rows_by_day = {str(row["day"]): row for row in rows}
        active_days = tuple(
            str(row["day"]) for row in rows if _active_for_mode(row, mode)
        )
        if not active_days:
            raise RuntimeError(f"replay mode has no active days: {mode.value}")

        calendar_start_ms = _day_start_ms(effective_start_day)
        calendar_end_ms = _day_start_ms(effective_end_day) + DAY_MS
        offline: list[tuple[int, int]] = []
        for row in rows:
            day_start = _day_start_ms(str(row["day"]))
            if str(row["day"]) not in active_days:
                offline.append((day_start, day_start + DAY_MS))
        active_set = set(active_days)
        for gap in manifest.get("observed_data_gaps", ()):
            if str(gap["day"]) in active_set and str(gap["day"]) in rows_by_day:
                offline.append(
                    (int(gap["offline_start_ts_ms"]), int(gap["resume_ts_ms"]))
                )
        merged = _merge_intervals(offline)

        initial_offline_until: int | None = None
        final_offline_from: int | None = None
        restart_rows: list[RestartScheduleInterval] = []
        cancel_drain_ms = int(manifest["cancel_drain_ms"])
        warmup_ms = int(manifest["feature_warmup_lookback_s"]) * 1_000
        for index, (offline_start, resume) in enumerate(merged, start=1):
            if offline_start <= calendar_start_ms:
                if resume >= calendar_end_ms:
                    raise RuntimeError("calendar replay has no observable active interval")
                initial_offline_until = _resume_at_first_visible(resume, rows_by_day)
                continue
            if resume >= calendar_end_ms:
                final_offline_from = offline_start
                continue
            resume = _resume_at_first_visible(resume, rows_by_day)
            interval = RestartScheduleInterval(
                gap_id=f"{mode.value}-G{index:03d}",
                quote_stop_ts_ms=max(calendar_start_ms, offline_start - cancel_drain_ms),
                cancel_deadline_ts_ms=offline_start - 1,
                offline_start_ts_ms=offline_start,
                resume_snapshot_ts_ms=resume,
                warmup_lookback_start_ts_ms=resume - warmup_ms,
            )
            interval.validate()
            restart_rows.append(interval)

        boundaries = tuple(
            _day_start_ms(str(row["day"])) + DAY_MS for row in rows
        )
        timeline_payload = {
            "mode": mode.value,
            "calendar_start_day": effective_start_day,
            "calendar_end_day": effective_end_day,
            "active_days": list(active_days),
            "restart_intervals": [asdict(row) for row in restart_rows],
            "initial_offline_until_ts_ms": initial_offline_until,
            "final_offline_from_ts_ms": final_offline_from,
            "utc_accounting_boundaries_ts_ms": list(boundaries),
        }
        timeline_sha256 = hashlib.sha256(
            json.dumps(
                timeline_payload,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        return cls(
            mode=mode,
            manifest_path=str(path),
            manifest_sha256=hashlib.sha256(raw).hexdigest(),
            manifest_canonical_sha256=str(manifest["canonical_manifest_sha256"]),
            source_manifest_calendar_day_count=int(manifest["calendar_day_count"]),
            calendar_start_day=effective_start_day,
            calendar_end_day=effective_end_day,
            calendar_day_count=len(rows),
            anchor_target_days=anchor_days,
            active_days=active_days,
            restart_intervals=tuple(restart_rows),
            initial_offline_until_ts_ms=initial_offline_until,
            final_offline_from_ts_ms=final_offline_from,
            utc_accounting_boundaries_ts_ms=boundaries,
            economic_mark_bridge_complete=all(
                bool(row.get("daily_mark_available")) for row in rows
            ),
            full_tick_runner_binding=False,
            exact_queue_lifecycle_authority=(
                mode == ReplayMode.NATIVE_STRICT_CONTINUOUS
            ),
            action_or_live_authority=False,
            timeline_sha256=timeline_sha256,
        )

    def validate_for_execution(
        self,
        capabilities: ReplayAdapterCapabilities,
        *,
        require_daily_accounting: bool = True,
    ) -> None:
        validate_adapter_capabilities(capabilities)
        if not self.full_tick_runner_binding:
            raise RuntimeError("continuous calendar is not bound to the authoritative tick runner")
        if require_daily_accounting and not self.economic_mark_bridge_complete:
            raise RuntimeError("continuous calendar is missing official daily mark inputs")
        if self.action_or_live_authority:
            raise RuntimeError("shared calendar substrate cannot grant action authority")


def assert_shared_market_timeline(*plans: CalendarReplayPlan) -> None:
    if len(plans) < 2:
        raise ValueError("at least two arm plans are required")
    first = plans[0]
    for plan in plans[1:]:
        if plan.timeline_sha256 != first.timeline_sha256:
            raise RuntimeError("control and candidate do not share the same market timeline")


def identify_continuity_and_governance(
    *,
    daily_fresh_governance_on_pnl_usdc: float,
    continuous_governance_on_pnl_usdc: float,
    continuous_governance_off_pnl_usdc: float | None = None,
) -> dict[str, Any]:
    continuity_effect = (
        float(continuous_governance_on_pnl_usdc)
        - float(daily_fresh_governance_on_pnl_usdc)
    )
    governance_effect = (
        None
        if continuous_governance_off_pnl_usdc is None
        else float(continuous_governance_on_pnl_usdc)
        - float(continuous_governance_off_pnl_usdc)
    )
    return {
        "schema_version": f"{SCHEMA_VERSION}.identification",
        "continuity_effect_usdc": continuity_effect,
        "continuity_effect_estimand": (
            "continuous_state_restart_accounting_minus_daily_fresh_start_under_same_governance"
        ),
        "tail_governance_effect_usdc": governance_effect,
        "tail_governance_effect_estimand": (
            "continuous_governance_on_minus_continuous_governance_off_on_shared_timeline"
        ),
        "continuity_improvement_proves_tail_governance": False,
        "tail_governance_point_identified": governance_effect is not None,
        "statistical_support_requires_clustered_lower_bound_and_tail_gates": True,
    }
