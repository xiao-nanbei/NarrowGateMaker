"""Complete offline E/C opportunities and one deterministic intervention.

This collector owns no simulator state and does not score or authorize live
actions. Re-running the same prefix locates one opportunity before its request
is changed; the existing replay then simulates the branch's complete future.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from typing import Any


def opportunity_id(symbol: str, ts_ms: int, decision_sequence: int,
                   side: str, kind: str, order_id: str = "") -> str:
    return f"{symbol}:{ts_ms}:{decision_sequence}:{side}:{kind}:{order_id or '-'}"


def visible_feature_snapshot(values: Mapping[str, Any]) -> dict[str, float | None]:
    """Preserve unavailable model inputs as unknown, never fabricated zeros."""
    snapshot = {}
    for name, value in values.items():
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            number = math.nan
        snapshot[name] = number if math.isfinite(number) else None
    return snapshot


def feature_ready_time(source_ready_ns: Mapping[str, int], prediction_ready_ns: int,
                       decision_ts_ns: int) -> int:
    """Include the prediction fallback alongside individually scheduled sources."""
    return max(prediction_ready_ns, max(source_ready_ns.values(), default=decision_ts_ns))


class ReplayRiskSelection:
    """An uncapped collector, or an explicit streaming sink that must succeed."""

    def __init__(self, *, intervention: Mapping[str, Any] | None = None,
                 sink: Callable[[dict[str, Any]], None] | None = None,
                 max_rows: int = 0) -> None:
        self.target = dict(intervention or {})
        if self.target and (
            set(self.target) != {"opportunity_id", "action"}
            or not str(self.target["opportunity_id"]).strip()
            or self.target["action"] not in {"WAIT", "CANCEL"}
        ):
            raise ValueError("risk_selection_intervention requires opportunity_id and WAIT/CANCEL")
        if sink is not None and not callable(sink):
            raise ValueError("risk-selection opportunity sink must be callable")
        if max_rows < 0:
            raise ValueError("risk-selection opportunity limit cannot be negative")
        self.sink = sink
        self.max_rows = max_rows
        self.rows: list[dict[str, Any]] = []
        self.counts = {"E": 0, "C": 0}
        self.intervention_count = 0

    def targets(self, identity: str, action: str) -> bool:
        return self.target == {"opportunity_id": identity, "action": action}

    def observe(self, row: dict[str, Any]) -> str:
        if self.max_rows and sum(self.counts.values()) >= self.max_rows:
            raise RuntimeError("complete risk-selection opportunity collector exceeded max_rows")
        action = str(row["baseline_action"])
        if self.target.get("opportunity_id") == row["opportunity_id"]:
            expected = "WAIT" if row["kind"] == "E" else "CANCEL"
            if self.target["action"] != expected:
                raise ValueError(
                    "risk-selection intervention action does not match opportunity kind"
                )
            if self.intervention_count:
                raise RuntimeError("risk-selection target opportunity occurred more than once")
            action = expected
            self.intervention_count += 1
        row["action"] = action
        self.counts[row["kind"]] += 1
        if self.sink is None:
            self.rows.append(row)
        else:
            self.sink(row)
        return action

    def finish(self) -> dict[str, Any]:
        if self.target and self.intervention_count != 1:
            raise RuntimeError("risk-selection target opportunity was not reached")
        return {
            "_risk_selection_opportunities": self.rows,
            "risk_selection_opportunity_counts": dict(self.counts),
            "risk_selection_intervention_count": self.intervention_count,
            "risk_selection_opportunities_streamed": self.sink is not None,
        }
