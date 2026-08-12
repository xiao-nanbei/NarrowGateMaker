#!/usr/bin/env python3
"""Physical daily source readers for the causal-v12 1s cadence successor.

The readers accept only raw/derived 1-second source artifacts. They never
accept an existing 10-second feature panel, labels, predictions, or economic
outcomes. Every input path is explicit so callers can bind source authority
and clock semantics in the materialized panel identity.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from data_paths import resolve_portable_path
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_feature_generator as base,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_full_schema as full
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema

SCHEMA_VERSION = "causal_v12_1s_daily_physical_sources.v2"
MAX_SYNTHETIC_GAP_SECONDS = 30
OBSERVED_BAR_LAG_STATE = "observed_completed_trade_1s"
SYNTHETIC_BAR_LAG_STATE = "synthetic_flat_no_trade_1s_from_prior_observed_close"
DAY_PATTERN = re.compile(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)")
LOCAL_MANIFEST_SCHEMA = "narrowgate.taker_tempo_manifest.v1"
PROVIDER_L2_QUALITY_SCHEMA = "narrowgate.normalized_tardis_l2_day.v1"
NATIVE_L2_QUALITY_SCHEMA = "narrowgate.native_l2_day_quality.v1"
L2_QUALITY_SCHEMA = PROVIDER_L2_QUALITY_SCHEMA
L2_QUALITY_SCHEMAS = (PROVIDER_L2_QUALITY_SCHEMA, NATIVE_L2_QUALITY_SCHEMA)
REFERENCE_MANIFEST_SCHEMA = "binance_individual_trade_bar_1s.v1"
L2_CLOCK_ALIASES = {
    "tardis_provider_local": "tardis_provider_local",
    "tardis_provider_local_visibility_ms": "tardis_provider_local",
    "cryptohft_transaction_time": "cryptohft_transaction_time_100ms_grid",
    "cryptohft_transaction_time_ms": "cryptohft_transaction_time_100ms_grid",
    "cryptohft_transaction_time_100ms_grid": "cryptohft_transaction_time_100ms_grid",
}

LOCAL_COLUMNS = (
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "buy_qty",
    "sell_qty",
    "trade_count",
    "buy_trade_count",
    "sell_trade_count",
    "buy_quote_qty",
    "sell_quote_qty",
    "max_same_side_run",
    "buy_price_high",
    "buy_price_low",
    "sell_price_high",
    "sell_price_low",
)

REFERENCE_COLUMNS = (
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "buy_volume",
    "sell_volume",
    "trade_count",
    "buy_count",
    "sell_count",
)

METRIC_COLUMNS = (
    "create_time",
    "sum_open_interest",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
)

RAW_METRIC_CSV_COLUMNS = (
    "create_time",
    "symbol",
    "sum_open_interest",
    "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
)

L2_COLUMNS = (
    "timestamp",
    *(f"bid_px_{level}" for level in range(1, 11)),
    *(f"bid_qty_{level}" for level in range(1, 11)),
    *(f"ask_px_{level}" for level in range(1, 11)),
    *(f"ask_qty_{level}" for level in range(1, 11)),
)

FORBIDDEN_COLUMN_PREFIXES = (
    "label_",
    "target_",
    "prediction_",
    "pred_",
    "reward_",
    "pnl_",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _as_paths(values: Iterable[str | Path]) -> tuple[Path, ...]:
    return tuple(resolve_portable_path(value).resolve() for value in values)


def _path_day(path: Path) -> str | None:
    match = DAY_PATTERN.search(path.name)
    if match is None:
        return None
    try:
        parsed = datetime.strptime(match.group(1), "%Y-%m-%d")
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%d")


def _required_days(utc_day: str) -> tuple[str, str]:
    target = datetime.strptime(utc_day, "%Y-%m-%d").replace(tzinfo=UTC)
    return ((target - timedelta(days=1)).strftime("%Y-%m-%d"), utc_day)


def _day_bounds_ms(day: str) -> tuple[int, int]:
    start = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=UTC).timestamp() * 1_000)
    return start, start + 86_400_000


@dataclass(frozen=True, slots=True)
class BarReadAudit:
    """Dense causal 1s bars plus explicit observed/synthetic provenance."""

    bars: tuple[base.OneSecondBar, ...]
    observed_rows: int
    synthesized_start_ts_ms: tuple[int, ...]
    maximum_missing_run_seconds: int
    first_observed_start_ts_ms: int
    last_observed_start_ts_ms: int

    @property
    def synthesized_seconds(self) -> int:
        return len(self.synthesized_start_ts_ms)

    def audit_payload(self) -> dict[str, Any]:
        return {
            "observed_rows": self.observed_rows,
            "synthesized_seconds": self.synthesized_seconds,
            "maximum_missing_run_seconds": self.maximum_missing_run_seconds,
            "first_observed_start_ts_ms": self.first_observed_start_ts_ms,
            "last_observed_start_ts_ms": self.last_observed_start_ts_ms,
            "observed_lag_state": OBSERVED_BAR_LAG_STATE,
            "synthetic_lag_state": SYNTHETIC_BAR_LAG_STATE,
            "maximum_supported_missing_run_seconds": MAX_SYNTHETIC_GAP_SECONDS,
            "requires_prior_observed_close": True,
        }


@dataclass(frozen=True, slots=True)
class DailySourceBundle:
    """Explicit physical inputs for one UTC target-day panel."""

    utc_day: str
    local_trade_tempo_paths: tuple[Path, ...]
    local_source_manifest_paths: tuple[Path, ...] = ()
    execution_l2_paths: tuple[Path, ...] = ()
    execution_l2_quality_paths: tuple[Path, ...] = ()
    metric_paths: tuple[Path, ...] = ()
    reference_bar_paths: tuple[Path, ...] = ()
    reference_bar_manifest_paths: tuple[Path, ...] = ()
    local_source_identity: str = "binance_futures_individual_trades_1s_tempo.v1"
    execution_l2_clock_identity: str = "unspecified"
    metric_source_identity: str = "binance_futures_metrics_interval_end_5m.v1"
    reference_source_identity: str = "binance_futures_reference_trades_1s.v1"

    def __post_init__(self) -> None:
        try:
            parsed = datetime.strptime(self.utc_day, "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError as exc:
            raise base.FeatureContractError("utc_day must be YYYY-MM-DD") from exc
        if parsed.strftime("%Y-%m-%d") != self.utc_day:
            raise base.FeatureContractError("utc_day is not canonical YYYY-MM-DD")
        if not self.local_trade_tempo_paths:
            raise base.FeatureContractError("at least one local 1s trade-tempo path is required")
        if not self.execution_l2_clock_identity.strip():
            raise base.FeatureContractError("execution L2 clock identity must be explicit")
        for path in self.all_paths():
            if not path.is_file():
                raise FileNotFoundError(path)

    @classmethod
    def from_json(cls, path: Path) -> DailySourceBundle:
        payload = json.loads(path.read_text(encoding="utf-8"))
        allowed = {
            "utc_day",
            "local_trade_tempo_paths",
            "local_source_manifest_paths",
            "execution_l2_paths",
            "execution_l2_quality_paths",
            "metric_paths",
            "reference_bar_paths",
            "reference_bar_manifest_paths",
            "local_source_identity",
            "execution_l2_clock_identity",
            "metric_source_identity",
            "reference_source_identity",
        }
        unknown = sorted(set(payload) - allowed)
        if unknown:
            raise base.FeatureContractError(f"unknown daily source fields: {unknown}")
        return cls(
            utc_day=str(payload["utc_day"]),
            local_trade_tempo_paths=_as_paths(payload["local_trade_tempo_paths"]),
            local_source_manifest_paths=_as_paths(payload.get("local_source_manifest_paths", ())),
            execution_l2_paths=_as_paths(payload.get("execution_l2_paths", ())),
            execution_l2_quality_paths=_as_paths(payload.get("execution_l2_quality_paths", ())),
            metric_paths=_as_paths(payload.get("metric_paths", ())),
            reference_bar_paths=_as_paths(payload.get("reference_bar_paths", ())),
            reference_bar_manifest_paths=_as_paths(payload.get("reference_bar_manifest_paths", ())),
            local_source_identity=str(
                payload.get(
                    "local_source_identity",
                    "binance_futures_individual_trades_1s_tempo.v1",
                )
            ),
            execution_l2_clock_identity=str(
                payload.get("execution_l2_clock_identity", "unspecified")
            ),
            metric_source_identity=str(
                payload.get(
                    "metric_source_identity",
                    "binance_futures_metrics_interval_end_5m.v1",
                )
            ),
            reference_source_identity=str(
                payload.get(
                    "reference_source_identity",
                    "binance_futures_reference_trades_1s.v1",
                )
            ),
        )

    def all_paths(self) -> tuple[Path, ...]:
        return (
            *self.local_trade_tempo_paths,
            *self.local_source_manifest_paths,
            *self.execution_l2_paths,
            *self.execution_l2_quality_paths,
            *self.metric_paths,
            *self.reference_bar_paths,
            *self.reference_bar_manifest_paths,
        )

    def identity_payload(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "utc_day": self.utc_day,
            "source_identities": {
                "local": self.local_source_identity,
                "execution_l2_clock": self.execution_l2_clock_identity,
                "metrics": self.metric_source_identity,
                "reference": self.reference_source_identity,
            },
            "gap_fill_contract": {
                "lag_state": SYNTHETIC_BAR_LAG_STATE,
                "maximum_consecutive_seconds": MAX_SYNTHETIC_GAP_SECONDS,
                "requires_prior_observed_close": True,
                "old_10s_rows_are_never_forward_filled": True,
            },
            "inputs": [
                {
                    "path": str(path),
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
                for path in self.all_paths()
            ],
            "ten_second_feature_rows_accepted": False,
        }

    def identity_sha256(self) -> str:
        return _canonical_sha256(self.identity_payload())


def _schema_names(path: Path) -> tuple[str, ...]:
    return tuple(pq.ParquetFile(path).schema_arrow.names)


def _require_columns(path: Path, required: Sequence[str]) -> tuple[str, ...]:
    observed = _schema_names(path)
    forbidden = sorted(
        name for name in observed if name.lower().startswith(FORBIDDEN_COLUMN_PREFIXES)
    )
    if forbidden:
        raise base.FeatureContractError(
            f"{path.name}: label/outcome columns are forbidden physical inputs: {forbidden}"
        )
    if set(schema.TRAINABLE_FEATURE_ORDER).issubset(observed):
        raise base.FeatureContractError(
            f"{path.name}: an existing feature panel cannot be used as a 1s source"
        )
    missing = sorted(set(required) - set(observed))
    if missing:
        raise base.FeatureContractError(f"{path.name}: missing physical columns {missing}")
    return observed


def _read_parquet_columns(path: Path, columns: Sequence[str]) -> pd.DataFrame:
    _require_columns(path, columns)
    # The physical tempo/reference files persist ``timestamp`` as a pandas
    # index. Ignoring pandas metadata keeps every requested Arrow field as a
    # normal column and avoids silently dropping the causal clock.
    frame = pq.read_table(path, columns=list(columns)).to_pandas(ignore_metadata=True)
    missing_from_columns = [name for name in columns if name not in frame.columns]
    if len(missing_from_columns) == 1 and frame.index.name == missing_from_columns[0]:
        frame = frame.reset_index()
    remaining = sorted(set(columns) - set(frame.columns))
    if remaining:
        raise base.FeatureContractError(
            f"{path.name}: physical columns were not recoverable after index restoration: "
            f"{remaining}"
        )
    return frame


def _timestamp_ms(values: pd.Series) -> np.ndarray:
    if isinstance(values.dtype, pd.DatetimeTZDtype) or np.issubdtype(values.dtype, np.datetime64):
        nanoseconds = pd.to_datetime(values, utc=True).astype("datetime64[ns, UTC]")
        return nanoseconds.astype("int64").to_numpy() // 1_000_000
    numeric = pd.to_numeric(values, errors="raise").to_numpy(dtype=np.int64)
    if len(numeric) and np.nanmax(numeric) >= 100_000_000_000_000:
        numeric = numeric // 1_000
    return numeric


def _validate_observed_1s_clock(starts: np.ndarray, *, source_name: str) -> None:
    if len(starts) == 0:
        raise base.FeatureContractError(f"{source_name}: source is empty")
    if np.any(starts <= 0) or np.any(starts % schema.CADENCE_MS):
        raise base.FeatureContractError(f"{source_name}: timestamps are not canonical 1s")
    deltas = np.diff(starts)
    if np.any(deltas <= 0):
        raise base.FeatureContractError(f"{source_name}: duplicate or reversed 1s source clock")
    if np.any(deltas % schema.CADENCE_MS):
        raise base.FeatureContractError(f"{source_name}: non-integral 1s source gap")


def _flat_no_trade_bar(previous: base.OneSecondBar, start_ts_ms: int) -> base.OneSecondBar:
    close = float(previous.close)
    return base.OneSecondBar(
        start_ts_ms=start_ts_ms,
        finalized_ts_ms=start_ts_ms + schema.CADENCE_MS,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=0.0,
        buy_volume=0.0,
        sell_volume=0.0,
        trade_count=0,
        buy_count=0,
        sell_count=0,
        buy_quote_qty=0.0,
        sell_quote_qty=0.0,
        max_same_side_run=0,
        buy_price_high=0.0,
        buy_price_low=0.0,
        sell_price_high=0.0,
        sell_price_low=0.0,
    )


def _dense_bar_audit(
    observed: Sequence[base.OneSecondBar],
    *,
    source_name: str,
    fill_through_exclusive_ms: int,
) -> BarReadAudit:
    if not observed:
        raise base.FeatureContractError(f"{source_name}: source is empty")
    ordered = tuple(sorted(observed, key=lambda item: int(item.start_ts_ms)))
    starts = np.asarray([int(item.start_ts_ms) for item in ordered], dtype=np.int64)
    _validate_observed_1s_clock(starts, source_name=source_name)
    if fill_through_exclusive_ms <= int(ordered[-1].start_ts_ms):
        raise base.FeatureContractError(
            f"{source_name}: fill-through boundary must follow the last observed bar"
        )

    dense: list[base.OneSecondBar] = [ordered[0]]
    synthesized: list[int] = []
    maximum_missing_run = 0
    for current in ordered[1:]:
        prior = dense[-1]
        missing = (int(current.start_ts_ms) - int(prior.start_ts_ms)) // schema.CADENCE_MS - 1
        if missing > MAX_SYNTHETIC_GAP_SECONDS:
            raise base.FeatureContractError(
                f"{source_name}: missing run {missing}s exceeds frozen "
                f"{MAX_SYNTHETIC_GAP_SECONDS}s support"
            )
        maximum_missing_run = max(maximum_missing_run, missing)
        for _ in range(missing):
            start = int(dense[-1].start_ts_ms) + schema.CADENCE_MS
            if not dense:
                raise base.FeatureContractError(
                    f"{source_name}: cannot synthesize a no-trade bar without a prior close"
                )
            dense.append(_flat_no_trade_bar(dense[-1], start))
            synthesized.append(start)
        dense.append(current)

    trailing_missing = (
        fill_through_exclusive_ms - (int(dense[-1].start_ts_ms) + schema.CADENCE_MS)
    ) // schema.CADENCE_MS
    if trailing_missing < 0:
        raise base.FeatureContractError(f"{source_name}: invalid trailing source boundary")
    if trailing_missing > MAX_SYNTHETIC_GAP_SECONDS:
        raise base.FeatureContractError(
            f"{source_name}: trailing missing run {trailing_missing}s exceeds frozen "
            f"{MAX_SYNTHETIC_GAP_SECONDS}s support"
        )
    maximum_missing_run = max(maximum_missing_run, trailing_missing)
    for _ in range(trailing_missing):
        start = int(dense[-1].start_ts_ms) + schema.CADENCE_MS
        dense.append(_flat_no_trade_bar(dense[-1], start))
        synthesized.append(start)

    return BarReadAudit(
        bars=tuple(dense),
        observed_rows=len(ordered),
        synthesized_start_ts_ms=tuple(synthesized),
        maximum_missing_run_seconds=maximum_missing_run,
        first_observed_start_ts_ms=int(ordered[0].start_ts_ms),
        last_observed_start_ts_ms=int(ordered[-1].start_ts_ms),
    )


def _fill_through_from_paths(paths: Sequence[Path], *, source_name: str) -> int:
    days = [_path_day(path) for path in paths]
    if any(day is None for day in days):
        raise base.FeatureContractError(
            f"{source_name}: every physical path must include a UTC day"
        )
    return _day_bounds_ms(max(str(day) for day in days))[1]


def read_local_trade_bars_with_audit(paths: Sequence[Path]) -> BarReadAudit:
    frames = [_read_parquet_columns(path, LOCAL_COLUMNS) for path in paths]
    frame = pd.concat(frames, ignore_index=True)
    frame["start_ts_ms"] = _timestamp_ms(frame["timestamp"])
    frame.sort_values("start_ts_ms", inplace=True, kind="stable")
    observed = tuple(
        base.OneSecondBar(
            start_ts_ms=int(row.start_ts_ms),
            finalized_ts_ms=int(row.start_ts_ms) + schema.CADENCE_MS,
            open=float(row.open),
            high=float(row.high),
            low=float(row.low),
            close=float(row.close),
            volume=float(row.volume),
            buy_volume=float(row.buy_qty),
            sell_volume=float(row.sell_qty),
            trade_count=int(row.trade_count),
            buy_count=int(row.buy_trade_count),
            sell_count=int(row.sell_trade_count),
            buy_quote_qty=float(row.buy_quote_qty),
            sell_quote_qty=float(row.sell_quote_qty),
            max_same_side_run=int(row.max_same_side_run),
            buy_price_high=float(row.buy_price_high),
            buy_price_low=float(row.buy_price_low),
            sell_price_high=float(row.sell_price_high),
            sell_price_low=float(row.sell_price_low),
        )
        for row in frame.itertuples(index=False)
    )
    return _dense_bar_audit(
        observed,
        source_name="local trade-tempo",
        fill_through_exclusive_ms=_fill_through_from_paths(paths, source_name="local trade-tempo"),
    )


def read_local_trade_bars(paths: Sequence[Path]) -> tuple[base.OneSecondBar, ...]:
    return read_local_trade_bars_with_audit(paths).bars


def read_reference_bars_with_audit(paths: Sequence[Path]) -> BarReadAudit | None:
    if not paths:
        return None
    frames = [_read_parquet_columns(path, REFERENCE_COLUMNS) for path in paths]
    frame = pd.concat(frames, ignore_index=True)
    frame["start_ts_ms"] = _timestamp_ms(frame["timestamp"])
    frame.sort_values("start_ts_ms", inplace=True, kind="stable")
    observed = tuple(
        base.OneSecondBar(
            start_ts_ms=int(row.start_ts_ms),
            finalized_ts_ms=int(row.start_ts_ms) + schema.CADENCE_MS,
            open=float(row.open),
            high=float(row.high),
            low=float(row.low),
            close=float(row.close),
            volume=float(row.volume),
            buy_volume=float(row.buy_volume),
            sell_volume=float(row.sell_volume),
            trade_count=int(row.trade_count),
            buy_count=int(row.buy_count),
            sell_count=int(row.sell_count),
        )
        for row in frame.itertuples(index=False)
    )
    return _dense_bar_audit(
        observed,
        source_name="reference trade bars",
        fill_through_exclusive_ms=_fill_through_from_paths(
            paths, source_name="reference trade bars"
        ),
    )


def read_reference_bars(paths: Sequence[Path]) -> tuple[base.OneSecondBar, ...]:
    audit = read_reference_bars_with_audit(paths)
    return () if audit is None else audit.bars


@dataclass(frozen=True, slots=True)
class MetricFileAudit:
    path: Path
    utc_day: str
    input_timestamp_semantics: str
    rows: int
    first_feature_ready_ts_ms: int
    last_feature_ready_ts_ms: int
    maximum_source_ready_delay_ms: int
    input_rows_reordered: bool
    input_clock_inversions: int

    def audit_payload(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "utc_day": self.utc_day,
            "input_timestamp_semantics": self.input_timestamp_semantics,
            "rows": self.rows,
            "feature_ready_rule": "completed_5m_interval_end",
            "first_feature_ready_ts_ms": self.first_feature_ready_ts_ms,
            "last_feature_ready_ts_ms": self.last_feature_ready_ts_ms,
            "maximum_source_ready_delay_ms": self.maximum_source_ready_delay_ms,
            "input_rows_reordered": self.input_rows_reordered,
            "input_clock_inversions": self.input_clock_inversions,
        }


@dataclass(frozen=True, slots=True)
class MetricReadAudit:
    observations: tuple[full.MetricObservation, ...]
    files: tuple[MetricFileAudit, ...]


# The complete retained raw-metrics corpus available on 2026-08-05 contains
# seven 2s and two 1s post-boundary publication delays among 89,853 rows. Keep
# the actual source-ready timestamp; never snap a delayed observation earlier.
MAX_METRIC_SOURCE_READY_DELAY_MS = 2_000


def _read_metric_csv(path: Path) -> tuple[pd.DataFrame, MetricFileAudit]:
    if path.suffix.lower() != ".csv":
        raise base.FeatureContractError(f"{path.name}: metrics authority must be raw CSV")
    day = _path_day(path)
    if day is None:
        raise base.FeatureContractError(f"{path.name}: metrics path lacks a UTC day")
    frame = pd.read_csv(path)
    if tuple(frame.columns) != RAW_METRIC_CSV_COLUMNS:
        raise base.FeatureContractError(
            f"{path.name}: metrics CSV schema mismatch; observed={tuple(frame.columns)!r}"
        )
    if len(frame) != 288:
        raise base.FeatureContractError(f"{path.name}: metrics CSV must contain exactly 288 rows")
    if frame["symbol"].isna().any() or set(frame["symbol"].astype(str)) != {"BTCUSDC"}:
        raise base.FeatureContractError(f"{path.name}: metrics symbol must be BTCUSDC")

    raw_times = pd.to_datetime(frame["create_time"], utc=True, errors="raise").astype(
        "datetime64[ns, UTC]"
    )
    raw_ms = raw_times.astype("int64").to_numpy(dtype=np.int64) // 1_000_000
    input_clock_inversions = int(np.count_nonzero(np.diff(raw_ms) <= 0))
    if len(np.unique(raw_ms)) != len(raw_ms):
        raise base.FeatureContractError(f"{path.name}: duplicate metrics timestamps")
    input_rows_reordered = input_clock_inversions > 0
    if input_rows_reordered:
        order = np.argsort(raw_ms, kind="stable")
        frame = frame.iloc[order].reset_index(drop=True)
        raw_ms = raw_ms[order]
    day_start, day_end = _day_bounds_ms(day)
    expected_start_stamped = np.arange(day_start, day_end, 300_000, dtype=np.int64)
    expected_end_stamped = np.arange(day_start + 300_000, day_end + 1, 300_000, dtype=np.int64)
    start_delays_ms = raw_ms - expected_start_stamped
    end_delays_ms = raw_ms - expected_end_stamped
    bounded_start_stamps = bool(
        np.all((start_delays_ms >= 0) & (start_delays_ms <= MAX_METRIC_SOURCE_READY_DELAY_MS))
    )
    bounded_end_stamps = bool(
        np.all((end_delays_ms >= 0) & (end_delays_ms <= MAX_METRIC_SOURCE_READY_DELAY_MS))
    )
    if bounded_start_stamps:
        maximum_source_ready_delay_ms = int(start_delays_ms.max(initial=0))
        semantics = (
            "interval_start_shifted_to_causal_end"
            if maximum_source_ready_delay_ms == 0
            else "interval_start_with_bounded_source_ready_delay"
        )
        ready_ms = raw_ms + 300_000
    elif bounded_end_stamps:
        maximum_source_ready_delay_ms = int(end_delays_ms.max(initial=0))
        semantics = (
            "interval_end_already_causal_ready"
            if maximum_source_ready_delay_ms == 0
            else "interval_end_with_bounded_source_ready_delay"
        )
        ready_ms = raw_ms
    else:
        raise base.FeatureContractError(
            f"{path.name}: metrics timestamps are neither complete 5m start/end stamps "
            f"nor bounded source-ready delays <= {MAX_METRIC_SOURCE_READY_DELAY_MS}ms"
        )

    numeric_columns = tuple(
        name for name in RAW_METRIC_CSV_COLUMNS if name not in {"create_time", "symbol"}
    )
    for name in numeric_columns:
        frame[name] = pd.to_numeric(frame[name], errors="raise")
        if not np.isfinite(frame[name].to_numpy(dtype=np.float64)).all():
            raise base.FeatureContractError(f"{path.name}: non-finite metrics value in {name}")
    frame["source_ts_ms"] = ready_ms
    return frame, MetricFileAudit(
        path=path,
        utc_day=day,
        input_timestamp_semantics=semantics,
        rows=len(frame),
        first_feature_ready_ts_ms=int(ready_ms[0]),
        last_feature_ready_ts_ms=int(ready_ms[-1]),
        maximum_source_ready_delay_ms=maximum_source_ready_delay_ms,
        input_rows_reordered=input_rows_reordered,
        input_clock_inversions=input_clock_inversions,
    )


def read_metrics_with_audit(paths: Sequence[Path]) -> MetricReadAudit:
    if not paths:
        return MetricReadAudit((), ())
    parsed = [_read_metric_csv(path) for path in paths]
    frames = [item[0] for item in parsed]
    frame = pd.concat(frames, ignore_index=True)
    frame.sort_values("source_ts_ms", inplace=True, kind="stable")
    source_times = frame["source_ts_ms"].to_numpy(dtype=np.int64)
    if len(source_times) == 0 or np.any(np.diff(source_times) <= 0):
        raise base.FeatureContractError("metrics source clock is empty, duplicate, or reversed")
    observations = tuple(
        full.MetricObservation(
            source_ts_ms=int(row.source_ts_ms),
            feature_ready_ts_ms=int(row.source_ts_ms),
            sum_open_interest=float(row.sum_open_interest),
            toptrader_ls_ratio=float(row.sum_toptrader_long_short_ratio),
            crowd_ls_ratio=float(row.count_long_short_ratio),
            taker_ls_ratio=float(row.sum_taker_long_short_vol_ratio),
        )
        for row in frame.itertuples(index=False)
    )
    return MetricReadAudit(observations, tuple(item[1] for item in parsed))


def read_metrics(paths: Sequence[Path]) -> tuple[full.MetricObservation, ...]:
    return read_metrics_with_audit(paths).observations


def _safe_divide(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    result = np.zeros_like(numerator, dtype=np.float64)
    np.divide(numerator, denominator, out=result, where=denominator != 0.0)
    return result


def read_execution_l2(paths: Sequence[Path]) -> tuple[full.ExecutionL2Observation, ...]:
    """Reduce causally timestamped 100ms L2 snapshots into completed 1s nodes."""

    if not paths:
        return ()
    bid_px_cols = [f"bid_px_{level}" for level in range(1, 11)]
    bid_qty_cols = [f"bid_qty_{level}" for level in range(1, 11)]
    ask_px_cols = [f"ask_px_{level}" for level in range(1, 11)]
    ask_qty_cols = [f"ask_qty_{level}" for level in range(1, 11)]
    state_frames: list[pd.DataFrame] = []
    previous_timestamp: int | None = None
    previous_best_bid: float | None = None
    previous_best_ask: float | None = None
    previous_total_depth: float | None = None

    for path in paths:
        _require_columns(path, L2_COLUMNS)
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(columns=list(L2_COLUMNS), batch_size=250_000):
            frame = batch.to_pandas(ignore_metadata=True)
            if frame.empty:
                continue
            ts_ms = _timestamp_ms(frame["timestamp"])
            if (
                len(ts_ms)
                and previous_timestamp is not None
                and int(ts_ms[0]) <= previous_timestamp
            ):
                raise base.FeatureContractError("execution L2 clock is duplicate or reversed")
            if len(ts_ms) > 1 and np.any(np.diff(ts_ms) <= 0):
                raise base.FeatureContractError("execution L2 clock is duplicate or reversed")

            bid_px = frame[bid_px_cols].to_numpy(dtype=np.float64, copy=False)
            bid_qty = np.nan_to_num(
                frame[bid_qty_cols].to_numpy(dtype=np.float64, copy=False), nan=0.0
            )
            ask_px = frame[ask_px_cols].to_numpy(dtype=np.float64, copy=False)
            ask_qty = np.nan_to_num(
                frame[ask_qty_cols].to_numpy(dtype=np.float64, copy=False), nan=0.0
            )
            best_bid = bid_px[:, 0]
            best_ask = ask_px[:, 0]
            mid = 0.5 * (best_bid + best_ask)
            valid = (ts_ms > 0) & (best_bid > 0.0) & (best_ask > best_bid) & (mid > 0.0)
            if not valid.any():
                continue
            ts_ms = ts_ms[valid]
            bid_qty = bid_qty[valid]
            ask_qty = ask_qty[valid]
            best_bid = best_bid[valid]
            best_ask = best_ask[valid]
            mid = mid[valid]

            bid_cum = np.cumsum(bid_qty, axis=1)
            ask_cum = np.cumsum(ask_qty, axis=1)
            level_qty = bid_qty + ask_qty
            near_depth = bid_cum[:, 2] + ask_cum[:, 2]
            total_depth = bid_cum[:, 9] + ask_cum[:, 9]
            micro_den = bid_qty[:, 0] + ask_qty[:, 0]
            microprice = np.where(
                micro_den > 0.0,
                (best_ask * bid_qty[:, 0] + best_bid * ask_qty[:, 0]) / micro_den,
                mid,
            )

            prior_bid = np.empty_like(best_bid)
            prior_ask = np.empty_like(best_ask)
            prior_depth = np.empty_like(total_depth)
            prior_bid[0] = best_bid[0] if previous_best_bid is None else previous_best_bid
            prior_ask[0] = best_ask[0] if previous_best_ask is None else previous_best_ask
            prior_depth[0] = (
                total_depth[0] if previous_total_depth is None else previous_total_depth
            )
            if len(best_bid) > 1:
                prior_bid[1:] = best_bid[:-1]
                prior_ask[1:] = best_ask[:-1]
                prior_depth[1:] = total_depth[:-1]

            delta_depth = total_depth - prior_depth
            front_mean = level_qty[:, :3].mean(axis=1)
            middle_mean = level_qty[:, 3:7].mean(axis=1)
            back_mean = level_qty[:, 7:10].mean(axis=1)
            output = pd.DataFrame(
                {
                    "bucket_start_ts_ms": (ts_ms // 1_000) * 1_000,
                    "l2_spread_bps": _safe_divide(best_ask - best_bid, mid) * 10_000.0,
                    "l2_microprice_offset_bps": _safe_divide(microprice - mid, mid) * 10_000.0,
                    "l2_imbalance_l1": _safe_divide(
                        bid_cum[:, 0] - ask_cum[:, 0],
                        bid_cum[:, 0] + ask_cum[:, 0],
                    ),
                    "l2_imbalance_l3": _safe_divide(
                        bid_cum[:, 2] - ask_cum[:, 2],
                        bid_cum[:, 2] + ask_cum[:, 2],
                    ),
                    "l2_imbalance_l5": _safe_divide(
                        bid_cum[:, 4] - ask_cum[:, 4],
                        bid_cum[:, 4] + ask_cum[:, 4],
                    ),
                    "l2_imbalance_l10": _safe_divide(
                        bid_cum[:, 9] - ask_cum[:, 9],
                        bid_cum[:, 9] + ask_cum[:, 9],
                    ),
                    "l2_near_depth_total": near_depth,
                    "l2_depth_slope": _safe_divide(near_depth, total_depth),
                    "l2_depth_convexity": _safe_divide(
                        front_mean - 2.0 * middle_mean + back_mean,
                        front_mean + middle_mean + back_mean,
                    ),
                    "l2_queue_concentration": _safe_divide(level_qty[:, 0], near_depth),
                    "_quote_flip_sum": ((best_bid != prior_bid) | (best_ask != prior_ask)).astype(
                        np.float64
                    ),
                    "_refresh_sum": _safe_divide(np.maximum(delta_depth, 0.0), prior_depth),
                    "_cancel_sum": _safe_divide(np.maximum(-delta_depth, 0.0), prior_depth),
                    "_snapshot_count": np.ones(len(ts_ms), dtype=np.float64),
                }
            )
            if previous_best_bid is None:
                output.loc[output.index[0], ["_quote_flip_sum", "_refresh_sum", "_cancel_sum"]] = (
                    0.0
                )
            reduced = output.groupby("bucket_start_ts_ms", sort=False).agg(
                {
                    **{
                        name: "last"
                        for name in schema.EXECUTION_L2_FEATURES
                        if name
                        not in {
                            "l2_quote_flip_rate",
                            "l2_book_refresh_ratio",
                            "l2_book_cancel_ratio",
                        }
                    },
                    "_quote_flip_sum": "sum",
                    "_refresh_sum": "sum",
                    "_cancel_sum": "sum",
                    "_snapshot_count": "sum",
                }
            )
            state_frames.append(reduced)
            previous_timestamp = int(ts_ms[-1])
            previous_best_bid = float(best_bid[-1])
            previous_best_ask = float(best_ask[-1])
            previous_total_depth = float(total_depth[-1])

    if not state_frames:
        return ()
    combined = pd.concat(state_frames)
    aggregate = {
        **{
            name: "last"
            for name in schema.EXECUTION_L2_FEATURES
            if name
            not in {
                "l2_quote_flip_rate",
                "l2_book_refresh_ratio",
                "l2_book_cancel_ratio",
            }
        },
        "_quote_flip_sum": "sum",
        "_refresh_sum": "sum",
        "_cancel_sum": "sum",
        "_snapshot_count": "sum",
    }
    combined = combined.groupby(level=0, sort=True).agg(aggregate)
    combined["l2_quote_flip_rate"] = combined["_quote_flip_sum"] / combined["_snapshot_count"]
    combined["l2_book_refresh_ratio"] = combined["_refresh_sum"] / combined["_snapshot_count"]
    combined["l2_book_cancel_ratio"] = combined["_cancel_sum"] / combined["_snapshot_count"]
    return tuple(
        full.ExecutionL2Observation(
            bucket_start_ts_ms=int(index),
            feature_ready_ts_ms=int(index) + schema.CADENCE_MS,
            values={name: float(row[name]) for name in schema.EXECUTION_L2_FEATURES},
        )
        for index, row in combined.iterrows()
        if all(math.isfinite(float(row[name])) for name in schema.EXECUTION_L2_FEATURES)
    )


def _path_day_coverage(
    paths: Sequence[Path], required_days: Sequence[str], *, group: str
) -> dict[str, Any]:
    observed: dict[str, list[str]] = {day: [] for day in required_days}
    unparseable: list[str] = []
    extras: list[str] = []
    for path in paths:
        day = _path_day(path)
        if day is None:
            unparseable.append(str(path))
        elif day in observed:
            observed[day].append(str(path))
        else:
            extras.append(str(path))
    missing = [day for day, matches in observed.items() if not matches]
    duplicates = {day: matches for day, matches in observed.items() if len(matches) != 1}
    valid = not missing and not duplicates and not unparseable and not extras
    return {
        "group": group,
        "required_days": list(required_days),
        "paths_by_day": observed,
        "missing_days": missing,
        "duplicate_days": duplicates,
        "unparseable_paths": unparseable,
        "extra_paths": extras,
        "valid": valid,
    }


def _validate_local_manifests(
    bundle: DailySourceBundle, required_days: Sequence[str]
) -> dict[str, Any]:
    errors: list[str] = []
    entries: dict[str, list[Mapping[str, Any]]] = {day: [] for day in required_days}
    manifest_rows: list[dict[str, Any]] = []
    for path in bundle.local_source_manifest_paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("schema") != LOCAL_MANIFEST_SCHEMA:
                errors.append(f"{path.name}: unsupported local manifest schema")
            if payload.get("symbol") != "BTCUSDC":
                errors.append(f"{path.name}: local manifest symbol is not BTCUSDC")
            daily_files = payload.get("daily_files")
            if not isinstance(daily_files, list):
                errors.append(f"{path.name}: daily_files is not a list")
                daily_files = []
            if payload.get("daily_file_count") != len(daily_files):
                errors.append(f"{path.name}: daily_file_count mismatch")
            for item in daily_files:
                if isinstance(item, Mapping) and item.get("day") in entries:
                    entries[str(item["day"])].append(item)
            manifest_rows.append(
                {
                    "path": str(path),
                    "sha256": sha256_file(path),
                    "schema": payload.get("schema"),
                    "daily_manifest_sha256": payload.get("daily_manifest_sha256"),
                }
            )
        except (OSError, ValueError, TypeError) as exc:
            errors.append(f"{path.name}: {exc}")

    bound: list[dict[str, Any]] = []
    local_by_day = {_path_day(path): path for path in bundle.local_trade_tempo_paths}
    for day in required_days:
        data_path = local_by_day.get(day)
        day_entries = entries[day]
        matches: list[Mapping[str, Any]] = []
        if data_path is not None:
            actual_sha = sha256_file(data_path)
            actual_size = data_path.stat().st_size
            actual_rows = pq.ParquetFile(data_path).metadata.num_rows
            matches = [
                item
                for item in day_entries
                if item.get("sidecar_sha256") == actual_sha
                and item.get("sidecar_size_bytes") == actual_size
                and item.get("sidecar_rows") == actual_rows
            ]
        if len(matches) != 1:
            errors.append(f"{day}: local tempo is not uniquely hash-bound by a manifest")
        bound.append(
            {
                "day": day,
                "data_path": None if data_path is None else str(data_path),
                "manifest_entry_count": len(day_entries),
                "matching_entry_count": len(matches),
            }
        )
    return {
        "schema": LOCAL_MANIFEST_SCHEMA,
        "manifests": manifest_rows,
        "bound_days": bound,
        "errors": errors,
        "valid": not errors,
    }


def _validate_reference_manifests(
    bundle: DailySourceBundle, required_days: Sequence[str]
) -> dict[str, Any]:
    errors: list[str] = []
    bars_by_day = {_path_day(path): path for path in bundle.reference_bar_paths}
    meta_by_day: dict[str | None, list[Path]] = {}
    for path in bundle.reference_bar_manifest_paths:
        meta_by_day.setdefault(_path_day(path), []).append(path)
    bound: list[dict[str, Any]] = []
    for day in required_days:
        bar_path = bars_by_day.get(day)
        meta_paths = meta_by_day.get(day, [])
        day_errors: list[str] = []
        if bar_path is None:
            day_errors.append("BTCUSDT reference bar path missing")
        if len(meta_paths) != 1:
            day_errors.append("reference sidecar count is not exactly one")
        payload: Mapping[str, Any] = {}
        if not day_errors:
            try:
                payload = json.loads(meta_paths[0].read_text(encoding="utf-8"))
                if payload.get("schema_version") != REFERENCE_MANIFEST_SCHEMA:
                    day_errors.append("unsupported reference sidecar schema")
                if payload.get("symbol") != "BTCUSDT" or payload.get("utc_day") != day:
                    day_errors.append("reference symbol/day mismatch")
                if payload.get("complete") is not True:
                    day_errors.append("reference bar day is incomplete")
                if payload.get("bar_interval") != "[t,t+1s)":
                    day_errors.append("reference bar interval mismatch")
                if payload.get("causal_visible_at") != "t+1s":
                    day_errors.append("reference causal-ready rule mismatch")
                if payload.get("output_sha256") != sha256_file(bar_path):
                    day_errors.append("reference output SHA256 mismatch")
                if payload.get("rows") != pq.ParquetFile(bar_path).metadata.num_rows:
                    day_errors.append("reference row-count mismatch")
            except (OSError, ValueError, TypeError) as exc:
                day_errors.append(str(exc))
        errors.extend(f"{day}: {message}" for message in day_errors)
        bound.append(
            {
                "day": day,
                "bar_path": None if bar_path is None else str(bar_path),
                "sidecar_path": None if len(meta_paths) != 1 else str(meta_paths[0]),
                "source_data_type": payload.get("source_data_type"),
                "valid": not day_errors,
                "errors": day_errors,
            }
        )
    return {
        "schema": REFERENCE_MANIFEST_SCHEMA,
        "bound_days": bound,
        "errors": errors,
        "valid": not errors,
    }


def _normalize_l2_clock_identity(identity: str) -> str:
    return L2_CLOCK_ALIASES.get(identity, identity)


def _validate_l2_quality(bundle: DailySourceBundle, required_days: Sequence[str]) -> dict[str, Any]:
    errors: list[str] = []
    l2_by_day = {_path_day(path): path for path in bundle.execution_l2_paths}
    quality_by_day: dict[str | None, list[Path]] = {}
    for path in bundle.execution_l2_quality_paths:
        quality_by_day.setdefault(_path_day(path), []).append(path)
    expected_clock = _normalize_l2_clock_identity(bundle.execution_l2_clock_identity)
    bound: list[dict[str, Any]] = []
    for day in required_days:
        l2_path = l2_by_day.get(day)
        quality_paths = quality_by_day.get(day, [])
        day_errors: list[str] = []
        payload: Mapping[str, Any] = {}
        if l2_path is None:
            day_errors.append("L2 path missing")
        if len(quality_paths) != 1:
            day_errors.append("L2 quality JSON count is not exactly one")
        if not day_errors:
            try:
                payload = json.loads(quality_paths[0].read_text(encoding="utf-8"))
                schema_version = payload.get("schema_version")
                if schema_version not in L2_QUALITY_SCHEMAS:
                    day_errors.append("unsupported L2 quality schema")
                if payload.get("symbol") != "BTCUSDC" or payload.get("day") != day:
                    day_errors.append("L2 quality symbol/day mismatch")
                if payload.get("cadence_ms") != 100:
                    day_errors.append("L2 quality cadence is not 100ms")
                levels = int(payload.get("levels", 0))
                if levels < 10:
                    day_errors.append("L2 quality declares fewer than 10 levels")
                else:
                    names = set(_schema_names(l2_path))
                    level_columns = {
                        f"{side}_{kind}_{level}"
                        for side in ("bid", "ask")
                        for kind in ("px", "qty")
                        for level in range(1, levels + 1)
                    }
                    if not level_columns.issubset(names):
                        day_errors.append("L2 Parquet lacks quality-declared depth levels")
                if schema_version == PROVIDER_L2_QUALITY_SCHEMA:
                    if payload.get("complete_day") is not True:
                        day_errors.append("L2 quality does not declare a complete day")
                    if payload.get("causal_violations") != 0:
                        day_errors.append("L2 quality reports causal violations")
                    if payload.get("observed_internal_gap_valid") is not True:
                        day_errors.append("L2 internal gap contract is not valid")
                    if payload.get("cross_channel_contract_valid") is not True:
                        day_errors.append("L2 cross-channel contract is not valid")
                    if payload.get("provider_normalized_replay_candidate") is not True:
                        day_errors.append("L2 is not provider-normalized replay eligible")
                elif schema_version == NATIVE_L2_QUALITY_SCHEMA:
                    declared_identity = payload.get("identity_sha256")
                    identity_payload = dict(payload)
                    identity_payload.pop("identity_sha256", None)
                    if (
                        not isinstance(declared_identity, str)
                        or _canonical_sha256(identity_payload) != declared_identity
                    ):
                        day_errors.append("native L2 quality identity SHA256 mismatch")
                    if payload.get("source_kind") != "cryptohft_native_snapshot_delta":
                        day_errors.append("native L2 source kind is invalid")
                    if payload.get("provider_normalized_replay_candidate") is not False:
                        day_errors.append("native L2 is mislabeled as provider-normalized")
                    if payload.get("native_sequence_valid") is not True:
                        day_errors.append("native L2 sequence contract is invalid")
                    if payload.get("normalized_structural_valid") is not True:
                        day_errors.append("native L2 structural contract is invalid")
                    cross_channel = payload.get("cross_channel_quality")
                    if (
                        not isinstance(cross_channel, Mapping)
                        or cross_channel.get("valid") is not True
                    ):
                        day_errors.append("native L2 cross-channel contract is invalid")
                    role_field = (
                        "target_replay_candidate"
                        if day == bundle.utc_day
                        else "midnight_warmup_candidate"
                    )
                    if payload.get(role_field) is not True:
                        day_errors.append(f"native L2 {role_field} is false")
                declared_clock = payload.get("clock_source")
                if declared_clock != expected_clock:
                    day_errors.append(
                        f"L2 clock mismatch: quality={declared_clock!r}, "
                        f"bundle={bundle.execution_l2_clock_identity!r}"
                    )
                output = payload.get("l2_output")
                if not isinstance(output, Mapping):
                    day_errors.append("L2 quality lacks l2_output identity")
                else:
                    if output.get("sha256") != sha256_file(l2_path):
                        day_errors.append("L2 output SHA256 mismatch")
                    if output.get("size_bytes") != l2_path.stat().st_size:
                        day_errors.append("L2 output size mismatch")
            except (OSError, ValueError, TypeError) as exc:
                day_errors.append(str(exc))
        errors.extend(f"{day}: {message}" for message in day_errors)
        bound.append(
            {
                "day": day,
                "l2_path": None if l2_path is None else str(l2_path),
                "quality_path": None if len(quality_paths) != 1 else str(quality_paths[0]),
                "quality_schema": payload.get("schema_version"),
                "declared_clock_source": payload.get("clock_source"),
                "levels": payload.get("levels"),
                "target_replay_candidate": payload.get("target_replay_candidate"),
                "midnight_warmup_candidate": payload.get("midnight_warmup_candidate"),
                "valid": not day_errors,
                "errors": day_errors,
            }
        )
    return {
        "accepted_schemas": list(L2_QUALITY_SCHEMAS),
        "expected_clock_source": expected_clock,
        "bound_days": bound,
        "errors": errors,
        "valid": not errors,
    }


def probe_source_bundle(bundle: DailySourceBundle) -> dict[str, Any]:
    """Read schemas/quality metadata without materializing feature values."""

    required_days = _required_days(bundle.utc_day)
    groups: Mapping[str, Sequence[Path]] = {
        "local_trade_tempo": bundle.local_trade_tempo_paths,
        "local_source_manifest": bundle.local_source_manifest_paths,
        "execution_l2": bundle.execution_l2_paths,
        "execution_l2_quality": bundle.execution_l2_quality_paths,
        "metrics": bundle.metric_paths,
        "reference_bars": bundle.reference_bar_paths,
        "reference_bar_manifest": bundle.reference_bar_manifest_paths,
    }
    required_by_group: Mapping[str, Sequence[str]] = {
        "local_trade_tempo": LOCAL_COLUMNS,
        "execution_l2": L2_COLUMNS,
        "metrics": METRIC_COLUMNS,
        "reference_bars": REFERENCE_COLUMNS,
    }
    rows: list[dict[str, Any]] = []
    for group, paths in groups.items():
        for path in paths:
            row: dict[str, Any] = {
                "group": group,
                "path": str(path),
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            try:
                if path.suffix == ".parquet":
                    names = _schema_names(path)
                    row["columns"] = list(names)
                    required = required_by_group.get(group)
                    row["schema_supported"] = (
                        True if required is None else set(required).issubset(names)
                    )
                    if group == "execution_l2":
                        row["available_depth_levels"] = max(
                            (
                                int(name.rsplit("_", 1)[1])
                                for name in names
                                if name.startswith("bid_px_")
                            ),
                            default=0,
                        )
                elif path.suffix.lower() == ".csv":
                    header = tuple(pd.read_csv(path, nrows=0).columns)
                    row["columns"] = list(header)
                    row["schema_supported"] = (
                        group == "metrics" and header == RAW_METRIC_CSV_COLUMNS
                    )
                elif path.suffix.lower() == ".json":
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    row["schema_supported"] = isinstance(payload, Mapping)
                    row["declared_schema"] = payload.get("schema_version", payload.get("schema"))
                    row["declared_clock_source"] = payload.get("clock_source")
                    row["provider_normalized_replay_candidate"] = payload.get(
                        "provider_normalized_replay_candidate"
                    )
                    row["live_transport_eligible"] = payload.get("live_transport_eligible")
                else:
                    row["schema_supported"] = False
                    row["schema_error"] = "unsupported physical source suffix"
            except (OSError, ValueError, TypeError) as exc:
                row["schema_supported"] = False
                row["schema_error"] = str(exc)
            rows.append(row)

    coverage = {
        "local_trade_tempo": _path_day_coverage(
            bundle.local_trade_tempo_paths, required_days, group="local_trade_tempo"
        ),
        "execution_l2": _path_day_coverage(
            bundle.execution_l2_paths, required_days, group="execution_l2"
        ),
        "execution_l2_quality": _path_day_coverage(
            bundle.execution_l2_quality_paths,
            required_days,
            group="execution_l2_quality",
        ),
        "metrics": _path_day_coverage(bundle.metric_paths, required_days, group="metrics"),
        "reference_bars": _path_day_coverage(
            bundle.reference_bar_paths, required_days, group="reference_bars"
        ),
        "reference_bar_manifest": _path_day_coverage(
            bundle.reference_bar_manifest_paths,
            required_days,
            group="reference_bar_manifest",
        ),
    }
    local_authority = _validate_local_manifests(bundle, required_days)
    l2_authority = _validate_l2_quality(bundle, required_days)
    reference_authority = _validate_reference_manifests(bundle, required_days)
    metric_errors: list[str] = []
    metric_files: list[dict[str, Any]] = []
    try:
        metric_audit = read_metrics_with_audit(bundle.metric_paths)
        metric_files = [item.audit_payload() for item in metric_audit.files]
    except (OSError, ValueError, TypeError) as exc:
        metric_errors.append(str(exc))
    metrics_authority = {
        "schema": "binance_futures_raw_metrics_5m_csv.v1",
        "feature_ready_rule": "completed_5m_interval_end",
        "files": metric_files,
        "errors": metric_errors,
        "valid": not metric_errors and len(metric_files) == len(required_days),
    }
    bar_clock_errors: list[str] = []
    local_bar_audit_payload: dict[str, Any] | None = None
    reference_bar_audit_payload: dict[str, Any] | None = None
    try:
        local_bar_audit_payload = read_local_trade_bars_with_audit(
            bundle.local_trade_tempo_paths
        ).audit_payload()
    except (OSError, ValueError, TypeError) as exc:
        bar_clock_errors.append(f"local trade-tempo: {exc}")
    try:
        reference_audit = read_reference_bars_with_audit(bundle.reference_bar_paths)
        if reference_audit is None:
            bar_clock_errors.append("BTCUSDT reference bars are absent")
        else:
            reference_bar_audit_payload = reference_audit.audit_payload()
    except (OSError, ValueError, TypeError) as exc:
        bar_clock_errors.append(f"BTCUSDT reference bars: {exc}")
    bar_clock_authority = {
        "lag_state_rule": SYNTHETIC_BAR_LAG_STATE,
        "maximum_supported_missing_run_seconds": MAX_SYNTHETIC_GAP_SECONDS,
        "local_trade_tempo": local_bar_audit_payload,
        "reference_bars": reference_bar_audit_payload,
        "errors": bar_clock_errors,
        "valid": not bar_clock_errors,
    }

    failure_reasons: list[str] = []
    for group, result in coverage.items():
        if not result["valid"]:
            failure_reasons.append(f"{group}: D-1/target path coverage is incomplete")
    for name, result in (
        ("local_manifest", local_authority),
        ("execution_l2_quality", l2_authority),
        ("metrics", metrics_authority),
        ("reference_manifest", reference_authority),
        ("bar_clock", bar_clock_authority),
    ):
        if not result["valid"]:
            failure_reasons.append(f"{name}: authority binding failed")
    if not all(bool(row.get("schema_supported")) for row in rows):
        failure_reasons.append("one or more physical file schemas are unsupported")
    physical_materialization_eligible = not failure_reasons
    return {
        "schema_version": "causal_v12_1s_daily_source_probe.v2",
        "utc_day": bundle.utc_day,
        "bundle_identity_sha256": bundle.identity_sha256(),
        "execution_l2_clock_identity": bundle.execution_l2_clock_identity,
        "warmup_contract": {
            "required_days": list(required_days),
            "previous_natural_utc_day_required": True,
            "reason": "6h/24h local and metrics warmup plus reference/L2 boundary state",
            "search_back_to_older_day_allowed": False,
        },
        "path_day_coverage": coverage,
        "local_source_authority": local_authority,
        "execution_l2_quality_authority": l2_authority,
        "metrics_authority": metrics_authority,
        "reference_btcusdt_authority": reference_authority,
        "bar_clock_authority": bar_clock_authority,
        "files": rows,
        "physical_materialization_eligible": physical_materialization_eligible,
        "failure_reasons": failure_reasons,
        "ten_second_feature_rows_accepted": False,
        "economic_outcomes_read": False,
    }
