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
)
STABLE_CORE_QUANTILE = 0.95
STABLE_SPIKE_PROBABILITY = 0.005
STABLE_SPIKE_HIGH_QUANTILE = 0.99
DEFAULT_QUANTILE_PROBABILITIES = tuple(
    sorted({*(index / 100.0 for index in range(101)), 0.995, 0.999})
)


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
) -> dict[str, Any]:
    """Measure one wall-clock receive-time window and build replay CDFs."""
    paths = list(paths)
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
        start_receive_ns=start_ns,
        end_receive_ns=end_ns,
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

    return {
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


class MarketDataLatencySimulator:
    """Resolve feature visibility from a saved environment profile."""

    def __init__(self, profile: dict[str, Any]):
        if str(profile.get("schema", "")) != PROFILE_SCHEMA_VERSION:
            raise ValueError("unsupported market-data latency profile schema")
        self.profile = profile
        self.profile_id = str(profile.get("profile_id", "unknown"))
        self._exact: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._market_event: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for group in profile.get("groups", []):
            key = (
                str(group.get("market_id", "")),
                str(group.get("event_type", "")),
                str(group.get("transport", "unknown")).lower(),
            )
            self._exact[key] = group
            self._market_event.setdefault(key[:2], []).append(group)

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
        if self._group(row) is None:
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
            "- `captured` uses recorded visibility; `exchange_zero` is idealized; `profile_*` injects this host profile.",
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
    args = parser.parse_args()

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
