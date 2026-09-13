#!/usr/bin/env python3
"""Audit causal cross-venue receive-time tapes.

The runner reports local transport/feature latency separately and builds a
single-source pending-follow diagnostic against a Binance local bridge.  It is
an infrastructure/diagnostic report, not an alpha or event-cancel backtest.
"""

from __future__ import annotations

import argparse
import csv
import glob
import gzip
import json
import math
import random
import time
import zlib
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

DEFAULT_HORIZONS_MS = (10, 25, 50, 100, 250, 500, 1_000)


@dataclass
class RunningDistribution:
    """Bounded deterministic reservoir for operational latency quantiles."""

    max_samples: int = 200_000
    seed: int = 7
    count: int = 0
    total: float = 0.0
    minimum: float = math.inf
    maximum: float = -math.inf
    negative_count: int = 0
    samples: list[float] = field(default_factory=list)
    _rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)

    def add(self, value: object) -> None:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return
        if not math.isfinite(numeric):
            return
        self.count += 1
        self.total += numeric
        self.minimum = min(self.minimum, numeric)
        self.maximum = max(self.maximum, numeric)
        self.negative_count += int(numeric < 0.0)
        if len(self.samples) < self.max_samples:
            self.samples.append(numeric)
            return
        index = self._rng.randrange(self.count)
        if index < self.max_samples:
            self.samples[index] = numeric

    def summary(self, prefix: str) -> dict[str, float | int]:
        values = np.asarray(self.samples, dtype=np.float64)
        out: dict[str, float | int] = {
            f"{prefix}_count": self.count,
            f"{prefix}_sample_count": len(self.samples),
            f"{prefix}_negative_count": self.negative_count,
            f"{prefix}_mean": self.total / self.count if self.count else math.nan,
            f"{prefix}_min": self.minimum if self.count else math.nan,
            f"{prefix}_max": self.maximum if self.count else math.nan,
        }
        for label, quantile in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99), ("p999", 0.999)):
            out[f"{prefix}_{label}"] = (
                float(np.quantile(values, quantile)) if values.size else math.nan
            )
        return out

    def quantiles(self, probabilities: Iterable[float]) -> list[float]:
        values = np.asarray(self.samples, dtype=np.float64)
        if values.size == 0:
            return []
        return [
            float(np.quantile(values, min(1.0, max(0.0, float(probability)))))
            for probability in probabilities
        ]


@dataclass
class LatencyGroup:
    max_samples: int
    transport_ms: RunningDistribution = field(init=False)
    feature_us: RunningDistribution = field(init=False)
    visibility_ms: RunningDistribution = field(init=False)
    cadence_ms: RunningDistribution = field(init=False)
    rows: int = 0
    gap_known: int = 0
    gap_count: int = 0
    last_receive_ns: int = 0

    def __post_init__(self) -> None:
        self.transport_ms = RunningDistribution(self.max_samples, seed=11)
        self.feature_us = RunningDistribution(self.max_samples, seed=13)
        self.visibility_ms = RunningDistribution(self.max_samples, seed=15)
        self.cadence_ms = RunningDistribution(self.max_samples, seed=17)

    def add(self, row: dict) -> None:
        self.rows += 1
        self.transport_ms.add(row.get("transport_lag_ms"))
        self.feature_us.add(row.get("feature_latency_us"))
        exchange_ns = int(row.get("exchange_event_ts_ns", 0) or 0)
        ready_ns = int(row.get("feature_ready_ts_ns", 0) or 0)
        if exchange_ns > 0 and ready_ns > 0:
            self.visibility_ms.add((ready_ns - exchange_ns) / 1_000_000.0)
        receive_ns = int(row.get("local_receive_ts_ns", 0) or 0)
        if receive_ns > 0 and self.last_receive_ns > 0 and receive_ns >= self.last_receive_ns:
            self.cadence_ms.add((receive_ns - self.last_receive_ns) / 1_000_000.0)
        if receive_ns > self.last_receive_ns:
            self.last_receive_ns = receive_ns
        gap = row.get("gap_flag")
        if gap is not None:
            self.gap_known += 1
            self.gap_count += int(bool(gap))

    def summary(self, market_id: str, event_type: str, transport: str) -> dict:
        out = {
            "market_id": market_id,
            "event_type": event_type,
            "transport": transport,
            "rows": self.rows,
            "gap_known": self.gap_known,
            "gap_count": self.gap_count,
            "gap_rate": self.gap_count / self.gap_known if self.gap_known else math.nan,
        }
        out.update(self.transport_ms.summary("transport_lag_ms"))
        out.update(self.feature_us.summary("feature_latency_us"))
        out.update(self.visibility_ms.summary("visibility_lag_ms"))
        out.update(self.cadence_ms.summary("cadence_ms"))
        cadence_p50 = float(out["cadence_ms_p50"])
        out["empirical_min_horizon_ms"] = (
            max(1.0, cadence_p50) if math.isfinite(cadence_p50) else math.nan
        )
        return out


def expand_inputs(values: Iterable[str]) -> list[Path]:
    paths: set[Path] = set()
    for value in values:
        candidate = Path(value).expanduser()
        if candidate.is_dir():
            paths.update(candidate.glob("*.jsonl"))
            paths.update(candidate.glob("*.jsonl.gz"))
            continue
        matches = [Path(path) for path in glob.glob(str(candidate))]
        if matches:
            paths.update(matches)
        elif candidate.is_file():
            paths.add(candidate)
    return sorted(path.resolve() for path in paths if path.is_file())


def iter_rows(
    paths: Iterable[Path],
    *,
    start_receive_ns: int = 0,
    end_receive_ns: int = 0,
):
    for path in paths:
        opener = gzip.open if path.suffix == ".gz" else Path.open
        kwargs = {"mode": "rt", "encoding": "utf-8"} if path.suffix == ".gz" else {"encoding": "utf-8"}
        with opener(path, **kwargs) as handle:
            line_number = 0
            while True:
                try:
                    line = handle.readline()
                except EOFError:
                    # A live gzip stream has no footer until the recorder
                    # closes it. Flushed complete lines remain valid input.
                    break
                except (gzip.BadGzipFile, zlib.error):
                    # Appending a new gzip member can expose an incomplete
                    # compressed block to an operational profiler. Tolerate
                    # only a file that is demonstrably still being written;
                    # stale/history corruption must remain fail-fast.
                    if time.time() - path.stat().st_mtime <= 30.0:
                        break
                    raise
                if not line:
                    break
                line_number += 1
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    # Ignore only a truncated final line from an actively
                    # written file. Corruption in the middle still fails.
                    try:
                        next_byte = handle.read(1)
                    except EOFError:
                        next_byte = ""
                    if not next_byte:
                        break
                    raise ValueError(f"{path}:{line_number}: invalid JSON") from exc
                if isinstance(row, dict):
                    receive_ns = int(row.get("local_receive_ts_ns", 0) or 0)
                    if start_receive_ns and receive_ns < int(start_receive_ns):
                        continue
                    if end_receive_ns and receive_ns > int(end_receive_ns):
                        continue
                    yield row


def latency_distribution(
    paths: Iterable[Path],
    *,
    max_samples: int = 200_000,
    start_receive_ns: int = 0,
    end_receive_ns: int = 0,
) -> list[dict]:
    groups: dict[tuple[str, str, str], LatencyGroup] = {}
    for row in iter_rows(
        paths,
        start_receive_ns=start_receive_ns,
        end_receive_ns=end_receive_ns,
    ):
        key = (
            str(row.get("market_id", "unknown")),
            str(row.get("event_type", "unknown")),
            str(row.get("transport", "unknown")),
        )
        group = groups.setdefault(key, LatencyGroup(max_samples=max_samples))
        group.add(row)
    return [
        groups[key].summary(*key)
        for key in sorted(groups)
    ]


def load_book_series(
    paths: Iterable[Path], market_ids: set[str]
) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    rows: dict[str, list[tuple[int, float]]] = {market_id: [] for market_id in market_ids}
    for row in iter_rows(paths):
        market_id = str(row.get("market_id", ""))
        if market_id not in rows or str(row.get("event_type", "")) != "book":
            continue
        ready_ns = int(row.get("feature_ready_ts_ns", 0) or 0)
        try:
            bid = float(row.get("bid", 0.0))
            ask = float(row.get("ask", 0.0))
        except (TypeError, ValueError):
            continue
        if ready_ns > 0 and bid > 0.0 and ask > bid:
            rows[market_id].append((ready_ns, 0.5 * (bid + ask)))

    out: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for market_id, values in rows.items():
        if not values:
            continue
        values.sort(key=lambda item: item[0])
        deduped: list[tuple[int, float]] = []
        for timestamp_ns, mid in values:
            if deduped and deduped[-1][0] == timestamp_ns:
                deduped[-1] = (timestamp_ns, mid)
            else:
                deduped.append((timestamp_ns, mid))
        out[market_id] = (
            np.asarray([item[0] for item in deduped], dtype=np.int64),
            np.asarray([item[1] for item in deduped], dtype=np.float64),
        )
    return out


def _asof(
    timestamps: np.ndarray,
    values: np.ndarray,
    query: np.ndarray,
    *,
    max_age_ns: int,
) -> tuple[np.ndarray, np.ndarray]:
    indices = np.searchsorted(timestamps, query, side="right") - 1
    valid = indices >= 0
    safe = np.clip(indices, 0, max(0, timestamps.size - 1))
    age = query - timestamps[safe]
    valid &= age >= 0
    valid &= age <= max_age_ns
    result = np.full(query.shape, np.nan, dtype=np.float64)
    result[valid] = values[safe[valid]]
    return result, valid


def leader_survival(
    series: dict[str, tuple[np.ndarray, np.ndarray]],
    *,
    local_market_id: str,
    external_market_ids: Iterable[str],
    horizons_ms: Iterable[int] = DEFAULT_HORIZONS_MS,
    lookback_ms: int = 100,
    shock_threshold_bps: float = 0.25,
    max_book_age_ms: int = 2_000,
) -> list[dict]:
    """Measure whether an external-local pending move survives each horizon.

    This is a single-source diagnostic.  It does not establish 2-of-3 global
    consensus and must not be read as a cancel/re-center counterfactual.
    """
    if local_market_id not in series:
        raise ValueError(f"missing local market series: {local_market_id}")
    local_ts, local_mid = series[local_market_id]
    max_age_ns = int(max(1, max_book_age_ms) * 1_000_000)
    lookback_ns = int(max(1, lookback_ms) * 1_000_000)
    rows: list[dict] = []

    for market_id in external_market_ids:
        if market_id not in series:
            continue
        external_ts, external_mid = series[market_id]
        if external_ts.size < 2:
            continue
        # Repeated BBO events carry no price innovation and only inflate the
        # denominator, so evaluate points where the external mid changed.
        changed = np.concatenate(([True], np.diff(external_mid) != 0.0))
        event_ts = external_ts[changed]
        event_mid = external_mid[changed]
        prior_query = event_ts - lookback_ns
        external_prior, external_prior_ok = _asof(
            external_ts, external_mid, prior_query, max_age_ns=max_age_ns
        )
        local_prior, local_prior_ok = _asof(
            local_ts, local_mid, prior_query, max_age_ns=max_age_ns
        )
        local_now, local_now_ok = _asof(
            local_ts, local_mid, event_ts, max_age_ns=max_age_ns
        )
        with np.errstate(divide="ignore", invalid="ignore"):
            external_move = np.log(event_mid / external_prior) * 10_000.0
            local_move = np.log(local_now / local_prior) * 10_000.0
        pending = external_move - local_move
        base_valid = (
            external_prior_ok
            & local_prior_ok
            & local_now_ok
            & np.isfinite(pending)
            & (np.abs(pending) >= float(shock_threshold_bps))
        )

        for horizon_ms in horizons_ms:
            horizon_ns = int(max(1, int(horizon_ms)) * 1_000_000)
            local_future, future_ok = _asof(
                local_ts,
                local_mid,
                event_ts + horizon_ns,
                max_age_ns=max_age_ns,
            )
            valid = base_valid & future_ok & np.isfinite(local_future)
            if not np.any(valid):
                rows.append(
                    {
                        "external_market_id": market_id,
                        "local_market_id": local_market_id,
                        "lookback_ms": lookback_ms,
                        "horizon_ms": int(horizon_ms),
                        "events": 0,
                    }
                )
                continue
            selected_pending = pending[valid]
            direction = np.sign(selected_pending)
            with np.errstate(divide="ignore", invalid="ignore"):
                local_follow = (
                    np.log(local_future[valid] / local_now[valid]) * 10_000.0 * direction
                )
            magnitude = np.abs(selected_pending)
            follow_ratio = local_follow / magnitude
            rows.append(
                {
                    "external_market_id": market_id,
                    "local_market_id": local_market_id,
                    "lookback_ms": lookback_ms,
                    "horizon_ms": int(horizon_ms),
                    "events": int(valid.sum()),
                    "avg_abs_pending_bps": float(np.mean(magnitude)),
                    "median_abs_pending_bps": float(np.median(magnitude)),
                    "avg_signed_local_follow_bps": float(np.mean(local_follow)),
                    "follow_direction_rate": float(np.mean(local_follow > 0.0)),
                    "absorbed_50pct_rate": float(np.mean(follow_ratio >= 0.5)),
                    "pending_survival_50pct_rate": float(np.mean(follow_ratio < 0.5)),
                }
            )
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", action="append", required=True, help="JSONL file, glob, or directory"
    )
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--local-market-id", default="binance:perp:BTCUSDT")
    parser.add_argument("--external-market-id", action="append", default=[])
    parser.add_argument("--lookback-ms", type=int, default=100)
    parser.add_argument("--horizons-ms", default=",".join(map(str, DEFAULT_HORIZONS_MS)))
    parser.add_argument("--shock-threshold-bps", type=float, default=0.25)
    parser.add_argument("--max-book-age-ms", type=int, default=2_000)
    parser.add_argument("--max-quantile-samples", type=int, default=200_000)
    args = parser.parse_args()

    paths = expand_inputs(args.input)
    if not paths:
        raise FileNotFoundError("no receive-time JSONL files matched")
    horizons = tuple(int(value) for value in args.horizons_ms.split(",") if value.strip())
    latency_rows = latency_distribution(paths, max_samples=max(1, args.max_quantile_samples))
    write_csv(args.output_prefix.with_suffix(".latency.csv"), latency_rows)

    leader_rows: list[dict] = []
    if args.external_market_id:
        market_ids = {args.local_market_id, *args.external_market_id}
        series = load_book_series(paths, market_ids)
        leader_rows = leader_survival(
            series,
            local_market_id=args.local_market_id,
            external_market_ids=args.external_market_id,
            horizons_ms=horizons,
            lookback_ms=args.lookback_ms,
            shock_threshold_bps=args.shock_threshold_bps,
            max_book_age_ms=args.max_book_age_ms,
        )
        write_csv(args.output_prefix.with_suffix(".leader_survival.csv"), leader_rows)

    summary = {
        "status": "ok",
        "schema": "market_tape.v1",
        "input_files": [str(path) for path in paths],
        "latency_groups": len(latency_rows),
        "leader_rows": len(leader_rows),
        "horizons_ms": horizons,
        "interpretation": (
            "infrastructure diagnostic only; transport lag is exchange-clock sensitive and "
            "leader survival is not a policy counterfactual"
        ),
    }
    args.output_prefix.with_suffix(".json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
