"""Execution helpers for restart-aware continuous tick replay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from .continuous_calendar import CalendarReplayPlan

DAY_MS = 86_400_000
SCHEMA_VERSION = "continuous_tick_runner_binding.v1"


def _day_start_ms(day: str) -> int:
    return int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp() * 1_000)


def utc_day(ts_ms: int) -> str:
    return datetime.fromtimestamp(int(ts_ms) / 1_000.0, tz=UTC).date().isoformat()


@dataclass(frozen=True)
class ActiveReplaySegment:
    segment_id: str
    day: str
    start_ts_ms: int
    end_ts_ms: int
    planned_quote_stop_ts_ms: int
    gap_after_id: str
    terminal_censor: bool = False

    def validate(self) -> None:
        if not self.segment_id.strip() or not self.gap_after_id.strip():
            raise ValueError("continuous active segment ids must be non-empty")
        if self.terminal_censor and self.planned_quote_stop_ts_ms != 0:
            raise ValueError("terminal-censored segment cannot schedule maintenance")
        if not self.terminal_censor and not (
            self.start_ts_ms < self.planned_quote_stop_ts_ms < self.end_ts_ms
        ):
            raise ValueError("active segment timestamps are not ordered")
        if utc_day(self.start_ts_ms) != self.day:
            raise ValueError("active segment day does not match its start timestamp")
        if utc_day(self.end_ts_ms - 1) != self.day:
            raise ValueError("continuous tick runner does not split an online interval at UTC midnight")


def build_active_segments(
    plan: CalendarReplayPlan,
    *,
    cancel_drain_ms: int,
) -> tuple[ActiveReplaySegment, ...]:
    """Return complements of frozen gaps, preserving each pre-gap cancel drain."""

    drain_ms = int(cancel_drain_ms)
    if drain_ms <= 0:
        raise ValueError("cancel_drain_ms must be positive")
    calendar_start = _day_start_ms(plan.calendar_start_day)
    calendar_end = _day_start_ms(plan.calendar_end_day) + DAY_MS
    offline: list[tuple[int, int, int, str]] = []
    if plan.initial_offline_until_ts_ms is not None:
        offline.append(
            (
                calendar_start,
                int(plan.initial_offline_until_ts_ms),
                calendar_start,
                "initial_offline",
            )
        )
    for row in plan.restart_intervals:
        offline.append(
            (
                int(row.offline_start_ts_ms),
                int(row.resume_snapshot_ts_ms),
                int(row.quote_stop_ts_ms),
                str(row.gap_id),
            )
        )
    if plan.final_offline_from_ts_ms is not None:
        start = int(plan.final_offline_from_ts_ms)
        offline.append((start, calendar_end, start - drain_ms, "final_offline"))
    offline.sort()

    cursor = calendar_start
    segments: list[ActiveReplaySegment] = []
    for offline_start, resume, quote_stop, gap_id in offline:
        if offline_start < cursor:
            raise ValueError("calendar plan contains overlapping offline intervals")
        if cursor < offline_start:
            if quote_stop <= cursor:
                # A visible island shorter than the frozen cancel-drain cannot
                # safely admit a quote. Treat it as continued maintenance.
                cursor = max(cursor, resume)
                continue
            segment = ActiveReplaySegment(
                segment_id=f"S{len(segments) + 1:03d}",
                day=utc_day(cursor),
                start_ts_ms=cursor,
                end_ts_ms=offline_start,
                planned_quote_stop_ts_ms=quote_stop,
                gap_after_id=gap_id,
            )
            segment.validate()
            segments.append(segment)
        cursor = max(cursor, resume)
    if cursor < calendar_end:
        segment = ActiveReplaySegment(
            segment_id=f"S{len(segments) + 1:03d}",
            day=utc_day(cursor),
            start_ts_ms=cursor,
            end_ts_ms=calendar_end,
            planned_quote_stop_ts_ms=0,
            gap_after_id="panel_end_censor",
            terminal_censor=True,
        )
        segment.validate()
        segments.append(segment)
    if not segments:
        raise RuntimeError("calendar plan produced no active replay segments")
    if {row.day for row in segments} != set(plan.active_days):
        raise RuntimeError("active segment days do not match the frozen calendar plan")
    return tuple(segments)


def requires_new_campaign_id(
    inventory_before_btc: float,
    *,
    side: str,
    quantity_btc: float,
    epsilon: float = 1e-10,
) -> bool:
    signed = float(quantity_btc) if str(side).upper() == "BUY" else -float(quantity_btc)
    before = float(inventory_before_btc)
    after = before + signed
    return abs(before) <= epsilon or before * after < -(epsilon * epsilon)


def assert_planned_shutdown_drained(result: dict[str, object]) -> None:
    if not bool(result.get("planned_quote_stop_triggered")):
        raise RuntimeError("planned quote stop was not reached by the replay event clock")
    remaining = sum(
        int(result.get(name, 0) or 0)
        for name in (
            "planned_shutdown_open_order_count",
            "planned_shutdown_pending_new_order_count",
            "planned_shutdown_pending_cancel_order_count",
        )
    )
    if remaining:
        raise RuntimeError(f"planned maintenance entered the gap with {remaining} live orders")


def expected_segment_local_pnl_delta(
    *,
    terminal_mtm_pnl_usdc: float,
    initial_inventory_btc: float,
    initial_entry_price: float,
    first_mark_price: float,
) -> float:
    starting_local_equity = float(initial_inventory_btc) * (
        float(first_mark_price) - float(initial_entry_price)
    )
    return float(terminal_mtm_pnl_usdc) - starting_local_equity
