"""Python contract for the native discrete competing-risk expander."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "competing_risk_intervals.v1"
EVENT_CENSOR = 0
EVENT_FILL = 1
EVENT_ACK = 2


def expand_competing_risk_intervals_native(
    duration_ms: Any,
    event_kind: Any,
    bin_edges_ms: Any,
) -> pd.DataFrame:
    import narrowgate_cpp  # type: ignore

    if not hasattr(narrowgate_cpp, "expand_competing_risk_intervals"):
        raise RuntimeError("installed narrowgate_cpp lacks competing_risk_intervals.v1")
    output = narrowgate_cpp.expand_competing_risk_intervals(
        np.ascontiguousarray(duration_ms, dtype=np.float64),
        np.ascontiguousarray(event_kind, dtype=np.uint8),
        np.ascontiguousarray(bin_edges_ms, dtype=np.float64),
    )
    if str(output["schema_version"]) != SCHEMA_VERSION:
        raise RuntimeError("native risk-set schema identity changed")
    return pd.DataFrame(
        {name: np.asarray(values) for name, values in output.items() if name != "schema_version"}
    )


def expand_competing_risk_intervals_python(
    duration_ms: Any,
    event_kind: Any,
    bin_edges_ms: Any,
) -> pd.DataFrame:
    durations = np.asarray(duration_ms, dtype=float)
    events = np.asarray(event_kind, dtype=np.uint8)
    edges = np.asarray(bin_edges_ms, dtype=float)
    if len(durations) != len(events):
        raise ValueError("duration_ms and event_kind must align")
    if len(edges) < 2 or edges[0] != 0.0 or np.any(edges[1:] <= edges[:-1]):
        raise ValueError("bin edges must start at zero and increase")
    rows: list[dict[str, Any]] = []
    for row, (raw_duration, event) in enumerate(zip(durations, events, strict=True)):
        if not np.isfinite(raw_duration) or raw_duration < 0.0 or event > EVENT_ACK:
            raise ValueError("invalid duration/event")
        observed = min(float(raw_duration), float(edges[-1]))
        event_observed = event != EVENT_CENSOR and raw_duration <= edges[-1]
        event_bin = (
            int(np.searchsorted(edges[1:], raw_duration, side="left"))
            if event_observed
            else len(edges) - 1
        )
        for index, (start, end) in enumerate(zip(edges[:-1], edges[1:], strict=True)):
            exposure = max(0.0, min(observed, end) - start)
            is_event = event_observed and index == event_bin
            if exposure <= 0.0 and not is_event:
                break
            rows.append(
                {
                    "row_index": row,
                    "bin_index": index,
                    "interval_start_ms": start,
                    "interval_end_ms": end,
                    "exposure_fraction": np.clip(exposure / (end - start), 0.0, 1.0),
                    "fill_target": int(is_event and event == EVENT_FILL),
                    "ack_target": int(is_event and event == EVENT_ACK),
                }
            )
            if is_event or observed <= end:
                break
    return pd.DataFrame(rows)
