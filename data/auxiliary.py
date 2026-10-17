"""Explicit auxiliary observations, separate from purchased book/trade facts.

Archived metrics timestamps describe periods, not historical host receipt.
The caller must declare an availability delay; it remains a modeled clock.
"""

from dataclasses import dataclass
import math

import pandas as pd

from features.preprocess_metrics import normalize_feature_ready_time


METRIC_FIELDS = {
    "oi": "sum_open_interest",
    "top_ls": "sum_toptrader_long_short_ratio",
    "crowd_ls": "count_long_short_ratio",
    "taker_ls": "sum_taker_long_short_vol_ratio",
}


@dataclass(frozen=True)
class MetricObservation:
    symbol: str
    source_id: str
    period_end_ns: int
    ready_ns: int
    values: tuple[tuple[str, float], ...]
    availability_origin: str = "modeled_period_end_plus_delay"

    def __post_init__(self):
        if not self.source_id or self.symbol != "BTCUSDC":
            raise ValueError("bound BTCUSDC auxiliary identity required")
        if self.ready_ns < self.period_end_ns:
            raise ValueError("metrics cannot be ready before period end")
        values = dict(self.values)
        if len(self.values) != len(METRIC_FIELDS) or set(values) != set(METRIC_FIELDS):
            raise ValueError("complete, nonduplicated metric fields required")
        if any(not math.isfinite(v) or v < 0 for v in values.values()):
            raise ValueError("invalid metric value; missing is not zero")

    def signal_values(self, *, decision_ns):
        if self.ready_ns > decision_ns:
            raise ValueError("metrics not yet visible at decision")
        return {"ts_ms": (self.ready_ns + 999_999) // 1_000_000,
                "period_end_ns": self.period_end_ns, "source_id": self.source_id,
                **dict(self.values)}


def metrics_observations(frame, *, day, symbol, source_id, availability_delay_ns):
    """Validate archived periods without inventing missing rows or receipts.

    Use original metrics records, not a previously shifted feature table.
    Five-minute timestamp normalization reuses the maintained period contract.
    Source identity verification is the caller's manifest responsibility.
    """
    if type(availability_delay_ns) is not int or availability_delay_ns < 0:
        raise ValueError("explicit nonnegative modeled availability delay required")
    if symbol != "BTCUSDC" or "symbol" not in frame:
        raise ValueError("raw metrics require explicit BTCUSDC symbol identity")
    if not frame["symbol"].eq(symbol).all():
        raise ValueError("mixed or mismatched metric market")
    missing = set(METRIC_FIELDS.values()) - set(frame.columns)
    if missing:
        raise ValueError(f"missing metric fields: {sorted(missing)}")
    normalized = normalize_feature_ready_time(frame, day=day, allow_missing_observations=True)
    if len(normalized) != len(frame):
        raise ValueError("invalid metric timestamps must not silently disappear")
    normalized = normalized.sort_values("create_time")
    rows = []
    for _, row in normalized.iterrows():
        period_end = pd.Timestamp(row["create_time"]).value
        rows.append(MetricObservation(symbol, source_id, period_end,
            period_end + availability_delay_ns,
            tuple((name, float(row[column])) for name, column in METRIC_FIELDS.items())))
    return tuple(rows)


def reconcile_metrics(observations):
    """Equal period duplicates count once; conflicting values fail closed."""
    periods = {}
    for observation in observations:
        key = observation.symbol, observation.period_end_ns
        previous = periods.get(key)
        if previous is not None:
            if (dict(previous.values) != dict(observation.values)
                    or previous.ready_ns != observation.ready_ns):
                raise ValueError("conflicting metric period")
            continue
        periods[key] = observation
    return tuple(sorted(periods.values(), key=lambda row: (row.ready_ns, row.period_end_ns)))
