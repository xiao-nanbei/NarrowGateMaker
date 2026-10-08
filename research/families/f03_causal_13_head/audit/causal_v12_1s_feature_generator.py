#!/usr/bin/env python3
"""Causal 1-second feature-generator prototype for the F03 cadence successor.

This module is deliberately isolated from the live signal path.  It proves the
clock, cutoff, namespace, lag-state, and canonical-fingerprint contracts needed
by a future 1s cadence-specific feature artifact.  It does not train a model,
read labels or economic outcomes, mutate orders, or grant live authority.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION = "causal_v12_1s_feature_generator.v1"
IDENTITY = "causal_v12_cadence_1s_source_aware_semantics_successor_v1"
FEATURE_DAG_ID = "live_1s_signal_cutoff.v1"
FEATURE_SEMANTICS_IDENTITY = "causal_13_head_features_canonical_1s.v1"
CADENCE_MS = 1_000
SOURCE_CLOCK = "exchange_bucket_start_ms"
AVAILABILITY_CLOCK = "finalized_1s_bar_time_ms"
LOCAL_SOURCE = "binance_futures_btcusdc_completed_1s_bar"
REFERENCE_SOURCE = "cv_ref_perp_completed_1s_bar"
FORBIDDEN_FEATURE_PREFIXES = ("label_", "target_", "future_", "reward_", "pnl_")


class FeatureContractError(ValueError):
    """Raised when the causal 1s feature contract cannot be satisfied."""


@dataclass(frozen=True, slots=True)
class OneSecondBar:
    """One completed raw 1s bar with an explicit availability timestamp."""

    start_ts_ms: int
    finalized_ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    buy_volume: float
    sell_volume: float
    trade_count: int
    buy_count: int = 0
    sell_count: int = 0
    buy_quote_qty: float = 0.0
    sell_quote_qty: float = 0.0
    max_same_side_run: int = 0
    buy_price_high: float = 0.0
    buy_price_low: float = 0.0
    sell_price_high: float = 0.0
    sell_price_low: float = 0.0

    def __post_init__(self) -> None:
        if int(self.start_ts_ms) <= 0 or int(self.start_ts_ms) % CADENCE_MS:
            raise FeatureContractError("1s bar start timestamp must be positive and aligned")
        if int(self.finalized_ts_ms) < int(self.start_ts_ms) + CADENCE_MS:
            raise FeatureContractError("1s bar cannot be available before its interval ends")

        prices = (self.open, self.high, self.low, self.close)
        if any(not math.isfinite(float(value)) or float(value) <= 0.0 for value in prices):
            raise FeatureContractError("1s OHLC prices must be finite and positive")
        if float(self.high) < max(float(self.open), float(self.close)):
            raise FeatureContractError("1s bar high is below open/close")
        if float(self.low) > min(float(self.open), float(self.close)):
            raise FeatureContractError("1s bar low is above open/close")

        quantities = (
            self.volume,
            self.buy_volume,
            self.sell_volume,
            self.buy_quote_qty,
            self.sell_quote_qty,
        )
        if any(not math.isfinite(float(value)) or float(value) < 0.0 for value in quantities):
            raise FeatureContractError("1s quantities must be finite and non-negative")
        if any(
            int(value) < 0
            for value in (
                self.trade_count,
                self.buy_count,
                self.sell_count,
                self.max_same_side_run,
            )
        ):
            raise FeatureContractError("1s trade counts must be non-negative")
        side_prices = (
            self.buy_price_high,
            self.buy_price_low,
            self.sell_price_high,
            self.sell_price_low,
        )
        if any(not math.isfinite(float(value)) or float(value) < 0.0 for value in side_prices):
            raise FeatureContractError("1s side trade prices must be finite and non-negative")


@dataclass(frozen=True, slots=True)
class FeatureNodeSpec:
    """Static semantics for one feature-DAG node."""

    name: str
    dependencies: tuple[str, ...]
    unit: str
    cadence_ms: int
    lookback_ms: int
    source: str
    source_clock: str
    availability_clock: str
    lag_state_rule: str
    minimum_observations: int
    stateful: bool
    namespace: str = "feature"

    def contract_dict(self) -> dict[str, Any]:
        return {
            "availability_clock": self.availability_clock,
            "cadence_ms": self.cadence_ms,
            "dependencies": list(self.dependencies),
            "lag_state_rule": self.lag_state_rule,
            "lookback_ms": self.lookback_ms,
            "minimum_observations": self.minimum_observations,
            "name": self.name,
            "namespace": self.namespace,
            "source": self.source,
            "source_clock": self.source_clock,
            "stateful": self.stateful,
            "unit": self.unit,
        }


@dataclass(frozen=True, slots=True)
class FeatureValue:
    """One node value plus its dynamic causal-availability state."""

    value: float | None
    source_latest_ts_ms: int | None
    feature_ready_ts_ms: int | None
    observation_count: int
    lag_state: str


@dataclass(frozen=True, slots=True)
class FeatureRow:
    """Canonical feature row for one cutoff-exclusive 1s decision."""

    cutoff_exclusive_ms: int
    decision_ts_ms: int
    feature_ready_ts_ms: int
    values: Mapping[str, FeatureValue]
    fingerprint_sha256: str
    feature_dag_id: str = FEATURE_DAG_ID
    feature_semantics_identity: str = FEATURE_SEMANTICS_IDENTITY
    feature_namespace: str = "feature"


def _node(
    name: str,
    dependencies: tuple[str, ...],
    unit: str,
    lookback_s: int,
    minimum_observations: int,
    *,
    source: str = LOCAL_SOURCE,
    lag_state_rule: str = "strict_contiguous_raw_1s_no_forward_fill",
    stateful: bool = False,
) -> FeatureNodeSpec:
    return FeatureNodeSpec(
        name=name,
        dependencies=dependencies,
        unit=unit,
        cadence_ms=CADENCE_MS,
        lookback_ms=int(lookback_s) * CADENCE_MS,
        source=source,
        source_clock=SOURCE_CLOCK,
        availability_clock=AVAILABILITY_CLOCK,
        lag_state_rule=lag_state_rule,
        minimum_observations=minimum_observations,
        stateful=stateful,
    )


def _build_feature_specs() -> tuple[FeatureNodeSpec, ...]:
    specs: list[FeatureNodeSpec] = [
        _node("close", ("raw.close",), "USDC_per_BTC", 1, 1),
        _node("volume", ("raw.volume",), "BTC", 1, 1),
        _node("buy_volume", ("raw.buy_volume",), "BTC", 1, 1),
        _node("sell_volume", ("raw.sell_volume",), "BTC", 1, 1),
        _node("trade_count", ("raw.trade_count",), "count", 1, 1),
        _node("buy_count", ("raw.buy_count",), "count", 1, 1),
        _node("sell_count", ("raw.sell_count",), "count", 1, 1),
        _node("bar_spread", ("raw.high", "raw.low"), "USDC_per_BTC", 1, 1),
        _node(
            "bar_spread_bps",
            ("bar_spread", "close"),
            "bps",
            1,
            1,
        ),
        _node(
            "avg_trade_size",
            ("volume", "trade_count"),
            "BTC_per_trade",
            1,
            1,
        ),
        _node("return_1", ("raw.close",), "log_return", 2, 2),
        _node("return_abs", ("return_1",), "absolute_log_return", 2, 2),
    ]

    for seconds in (3, 5, 10):
        specs.append(
            _node(
                f"tick_mom_{seconds}s",
                ("raw.close",),
                "signed_tick_count",
                seconds + 1,
                seconds + 1,
            )
        )

    for seconds in (5, 10, 30, 60):
        specs.append(
            _node(
                f"taker_quote_imbalance_{seconds}s",
                ("raw.buy_quote_qty", "raw.sell_quote_qty"),
                "ratio",
                seconds,
                seconds,
            )
        )

    for seconds in (5, 30, 60, 300):
        specs.extend(
            (
                _node(
                    f"volatility_{seconds}s",
                    ("raw.close",),
                    "realized_log_return_scale",
                    seconds + 1,
                    seconds + 1,
                ),
                _node(
                    f"volume_imbalance_{seconds}s",
                    ("raw.buy_volume", "raw.sell_volume"),
                    "ratio",
                    seconds,
                    seconds,
                ),
                _node(
                    f"trade_intensity_{seconds}s",
                    ("raw.trade_count",),
                    "trades_per_second",
                    seconds,
                    seconds,
                ),
                _node(
                    f"vpin_{seconds}s",
                    ("raw.buy_volume", "raw.sell_volume"),
                    "ratio",
                    seconds,
                    seconds,
                ),
                _node(
                    f"price_change_{seconds}s",
                    ("raw.close",),
                    "log_return",
                    seconds + 1,
                    seconds + 1,
                ),
            )
        )

    specs.extend(
        (
            _node(
                "vol_regime_6h",
                ("return_abs",),
                "mean_absolute_log_return",
                21_601,
                21_601,
                stateful=True,
            ),
            _node(
                "vol_regime_24h",
                ("return_abs",),
                "mean_absolute_log_return",
                86_401,
                86_401,
                stateful=True,
            ),
        )
    )

    for seconds in (10, 30, 60):
        specs.append(
            _node(
                f"cv_ref_perp_ret_{seconds}s",
                ("raw.cv_ref_perp_close",),
                "log_return",
                seconds + 1,
                seconds + 1,
                source=REFERENCE_SOURCE,
                lag_state_rule="strict_reference_1s_or_missing_no_forward_fill",
            )
        )
    return tuple(specs)


FEATURE_SPECS = _build_feature_specs()
FEATURE_SPEC_BY_NAME = {spec.name: spec for spec in FEATURE_SPECS}
RAW_DEPENDENCY_PREFIX = "raw."


def _canonical_json_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def validate_feature_dag(specs: Sequence[FeatureNodeSpec] = FEATURE_SPECS) -> None:
    """Validate namespace separation, metadata completeness, and acyclicity."""

    names = [spec.name for spec in specs]
    if len(names) != len(set(names)):
        raise FeatureContractError("feature node names must be unique")
    by_name = {spec.name: spec for spec in specs}

    for spec in specs:
        if spec.namespace != "feature":
            raise FeatureContractError(f"non-feature node in feature DAG: {spec.name}")
        if spec.name.startswith(FORBIDDEN_FEATURE_PREFIXES):
            raise FeatureContractError(f"label/outcome namespace is forbidden: {spec.name}")
        if spec.cadence_ms != CADENCE_MS:
            raise FeatureContractError(f"node cadence drifted from 1s: {spec.name}")
        if spec.lookback_ms <= 0 or spec.minimum_observations <= 0:
            raise FeatureContractError(f"invalid lookback/support for node: {spec.name}")
        required_text = (
            spec.unit,
            spec.source,
            spec.source_clock,
            spec.availability_clock,
            spec.lag_state_rule,
        )
        if any(not value for value in required_text):
            raise FeatureContractError(f"incomplete node semantics: {spec.name}")
        for dependency in spec.dependencies:
            if dependency.startswith(("label.", "target.", "future.", "reward.", "pnl.")):
                raise FeatureContractError(
                    f"feature node depends on forbidden outcome namespace: {spec.name}"
                )
            if not dependency.startswith(RAW_DEPENDENCY_PREFIX) and dependency not in by_name:
                raise FeatureContractError(
                    f"unknown feature dependency {dependency!r} for {spec.name}"
                )

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visiting:
            raise FeatureContractError(f"feature DAG cycle detected at {name}")
        if name in visited:
            return
        visiting.add(name)
        for dependency in by_name[name].dependencies:
            if dependency in by_name:
                visit(dependency)
        visiting.remove(name)
        visited.add(name)

    for name in names:
        visit(name)


def feature_contract_payload() -> dict[str, Any]:
    validate_feature_dag()
    return {
        "availability_clock": AVAILABILITY_CLOCK,
        "cadence_ms": CADENCE_MS,
        "feature_dag_id": FEATURE_DAG_ID,
        "feature_namespace": "feature",
        "feature_semantics_identity": FEATURE_SEMANTICS_IDENTITY,
        "identity": IDENTITY,
        "nodes": [spec.contract_dict() for spec in FEATURE_SPECS],
        "schema_version": SCHEMA_VERSION,
        "source_clock": SOURCE_CLOCK,
    }


def feature_contract_fingerprint() -> str:
    return _canonical_json_sha256(feature_contract_payload())


def _validate_cutoff(cutoff_exclusive_ms: int, decision_ts_ms: int) -> None:
    if int(cutoff_exclusive_ms) <= 0 or int(cutoff_exclusive_ms) % CADENCE_MS:
        raise FeatureContractError("cutoff_exclusive_ms must be a positive canonical 1s edge")
    if int(decision_ts_ms) < int(cutoff_exclusive_ms):
        raise FeatureContractError("decision precedes the feature cutoff")


def _cutoff_view(
    bars: Sequence[OneSecondBar],
    cutoff_exclusive_ms: int,
    *,
    source_name: str,
) -> tuple[OneSecondBar, ...]:
    candidates: list[OneSecondBar] = []
    for bar in bars:
        if not isinstance(bar, OneSecondBar):
            raise FeatureContractError(f"{source_name} accepts raw OneSecondBar inputs only")
        if int(bar.start_ts_ms) < int(cutoff_exclusive_ms):
            candidates.append(bar)

    starts = [int(bar.start_ts_ms) for bar in candidates]
    if len(starts) != len(set(starts)):
        raise FeatureContractError(f"duplicate 1s source clock in {source_name}")

    visible = sorted(
        (
            bar
            for bar in candidates
            if int(bar.start_ts_ms) + CADENCE_MS <= int(cutoff_exclusive_ms)
            and int(bar.finalized_ts_ms) <= int(cutoff_exclusive_ms)
        ),
        key=lambda bar: int(bar.start_ts_ms),
    )
    if not visible:
        raise FeatureContractError(f"no causal 1s bars are visible for {source_name}")
    if int(visible[-1].start_ts_ms) != int(cutoff_exclusive_ms) - CADENCE_MS:
        raise FeatureContractError(f"latest completed 1s bar is missing or late for {source_name}")
    for previous, current in zip(visible, visible[1:], strict=False):
        if int(current.start_ts_ms) != int(previous.start_ts_ms) + CADENCE_MS:
            raise FeatureContractError(f"1s gap detected in {source_name}")
    return tuple(visible)


def _tail(view: Sequence[OneSecondBar], count: int) -> tuple[OneSecondBar, ...]:
    if count <= 0:
        raise FeatureContractError("tail count must be positive")
    return tuple(view[-count:])


def _log_returns(view: Sequence[OneSecondBar]) -> list[float]:
    result: list[float] = []
    for previous, current in zip(view, view[1:], strict=False):
        result.append(math.log(float(current.close) / float(previous.close)))
    return result


def _sample_std(values: Sequence[float]) -> float:
    if len(values) < 2:
        raise FeatureContractError("sample standard deviation requires two values")
    mean = math.fsum(values) / len(values)
    variance = math.fsum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(max(0.0, variance))


def _ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0.0:
        return None
    return numerator / denominator


def _feature_value(
    *,
    value: float | None,
    contributing: Sequence[OneSecondBar],
    required_observations: int,
    missing_state: str = "warmup_insufficient",
) -> FeatureValue:
    count = len(contributing)
    ready_ts = max((int(bar.finalized_ts_ms) for bar in contributing), default=None)
    source_ts = max((int(bar.start_ts_ms) for bar in contributing), default=None)
    if count < required_observations:
        return FeatureValue(
            value=None,
            source_latest_ts_ms=source_ts,
            feature_ready_ts_ms=ready_ts,
            observation_count=count,
            lag_state=missing_state,
        )
    if value is None or not math.isfinite(float(value)):
        return FeatureValue(
            value=None,
            source_latest_ts_ms=source_ts,
            feature_ready_ts_ms=ready_ts,
            observation_count=count,
            lag_state="undefined_zero_denominator",
        )
    return FeatureValue(
        value=float(value),
        source_latest_ts_ms=source_ts,
        feature_ready_ts_ms=ready_ts,
        observation_count=count,
        lag_state="ready_contiguous_full_window",
    )


def _current_bar_values(view: Sequence[OneSecondBar]) -> dict[str, FeatureValue]:
    bar = view[-1]
    spread = float(bar.high) - float(bar.low)
    avg_size = _ratio(float(bar.volume), float(bar.trade_count))
    raw = {
        "close": float(bar.close),
        "volume": float(bar.volume),
        "buy_volume": float(bar.buy_volume),
        "sell_volume": float(bar.sell_volume),
        "trade_count": float(bar.trade_count),
        "buy_count": float(bar.buy_count),
        "sell_count": float(bar.sell_count),
        "bar_spread": spread,
        "bar_spread_bps": 10_000.0 * spread / float(bar.close),
        "avg_trade_size": avg_size,
    }
    return {
        name: _feature_value(
            value=value,
            contributing=(bar,),
            required_observations=1,
        )
        for name, value in raw.items()
    }


def _compute_local_features(view: Sequence[OneSecondBar]) -> dict[str, FeatureValue]:
    values = _current_bar_values(view)

    two = _tail(view, 2)
    returns = _log_returns(two)
    return_1 = returns[-1] if len(two) == 2 else None
    values["return_1"] = _feature_value(
        value=return_1,
        contributing=two,
        required_observations=2,
    )
    values["return_abs"] = _feature_value(
        value=abs(return_1) if return_1 is not None else None,
        contributing=two,
        required_observations=2,
    )

    for seconds in (3, 5, 10):
        bars = _tail(view, seconds + 1)
        signs = [1.0 if item > 0 else -1.0 if item < 0 else 0.0 for item in _log_returns(bars)]
        values[f"tick_mom_{seconds}s"] = _feature_value(
            value=math.fsum(signs) if len(bars) == seconds + 1 else None,
            contributing=bars,
            required_observations=seconds + 1,
        )

    for seconds in (5, 10, 30, 60):
        bars = _tail(view, seconds)
        buy_quote = math.fsum(float(bar.buy_quote_qty) for bar in bars)
        sell_quote = math.fsum(float(bar.sell_quote_qty) for bar in bars)
        values[f"taker_quote_imbalance_{seconds}s"] = _feature_value(
            value=_ratio(buy_quote - sell_quote, buy_quote + sell_quote),
            contributing=bars,
            required_observations=seconds,
        )

    for seconds in (5, 30, 60, 300):
        bars = _tail(view, seconds)
        return_bars = _tail(view, seconds + 1)
        log_returns = _log_returns(return_bars)
        volatility = (
            _sample_std(log_returns) * math.sqrt(float(seconds))
            if len(log_returns) == seconds and len(log_returns) >= 2
            else None
        )
        buy = math.fsum(float(bar.buy_volume) for bar in bars)
        sell = math.fsum(float(bar.sell_volume) for bar in bars)
        absolute_imbalance = math.fsum(
            abs(float(bar.buy_volume) - float(bar.sell_volume)) for bar in bars
        )
        total = buy + sell
        values[f"volatility_{seconds}s"] = _feature_value(
            value=volatility,
            contributing=return_bars,
            required_observations=seconds + 1,
        )
        values[f"volume_imbalance_{seconds}s"] = _feature_value(
            value=_ratio(buy - sell, total),
            contributing=bars,
            required_observations=seconds,
        )
        values[f"trade_intensity_{seconds}s"] = _feature_value(
            value=math.fsum(float(bar.trade_count) for bar in bars) / seconds,
            contributing=bars,
            required_observations=seconds,
        )
        values[f"vpin_{seconds}s"] = _feature_value(
            value=_ratio(absolute_imbalance, total),
            contributing=bars,
            required_observations=seconds,
        )
        values[f"price_change_{seconds}s"] = _feature_value(
            value=math.log(float(return_bars[-1].close) / float(return_bars[0].close))
            if len(return_bars) == seconds + 1
            else None,
            contributing=return_bars,
            required_observations=seconds + 1,
        )

    for name, seconds in (("vol_regime_6h", 21_600), ("vol_regime_24h", 86_400)):
        bars = _tail(view, seconds + 1)
        absolute_returns = [abs(value) for value in _log_returns(bars)]
        values[name] = _feature_value(
            value=math.fsum(absolute_returns) / seconds
            if len(absolute_returns) == seconds
            else None,
            contributing=bars,
            required_observations=seconds + 1,
        )
    return values


def _missing_reference_value() -> FeatureValue:
    return FeatureValue(
        value=None,
        source_latest_ts_ms=None,
        feature_ready_ts_ms=None,
        observation_count=0,
        lag_state="source_unavailable_no_forward_fill",
    )


def _compute_reference_features(
    reference_view: Sequence[OneSecondBar] | None,
) -> dict[str, FeatureValue]:
    result: dict[str, FeatureValue] = {}
    for seconds in (10, 30, 60):
        name = f"cv_ref_perp_ret_{seconds}s"
        if reference_view is None:
            result[name] = _missing_reference_value()
            continue
        bars = _tail(reference_view, seconds + 1)
        result[name] = _feature_value(
            value=math.log(float(bars[-1].close) / float(bars[0].close))
            if len(bars) == seconds + 1
            else None,
            contributing=bars,
            required_observations=seconds + 1,
        )
    return result


def _canonical_value(value: float | None) -> str | None:
    return None if value is None else float(value).hex()


def _row_fingerprint_payload(
    cutoff_exclusive_ms: int,
    values: Mapping[str, FeatureValue],
) -> dict[str, Any]:
    return {
        "contract_sha256": feature_contract_fingerprint(),
        "cutoff_exclusive_ms": int(cutoff_exclusive_ms),
        "feature_dag_id": FEATURE_DAG_ID,
        "feature_semantics_identity": FEATURE_SEMANTICS_IDENTITY,
        "values": {
            name: {
                "feature_ready_ts_ms": item.feature_ready_ts_ms,
                "lag_state": item.lag_state,
                "observation_count": item.observation_count,
                "source_latest_ts_ms": item.source_latest_ts_ms,
                "value_hex": _canonical_value(item.value),
            }
            for name, item in sorted(values.items())
        },
    }


def generate_feature_row(
    local_bars: Sequence[OneSecondBar],
    *,
    cutoff_exclusive_ms: int,
    decision_ts_ms: int | None = None,
    reference_bars: Sequence[OneSecondBar] | None = None,
) -> FeatureRow:
    """Generate one row from raw bars visible strictly before the cutoff.

    A bar beginning at ``cutoff_exclusive_ms`` is never visible to this row,
    even when it is already present in the input sequence.  Delayed execution
    also cannot retroactively admit a bar finalized after the canonical cutoff.
    """

    decision = int(cutoff_exclusive_ms if decision_ts_ms is None else decision_ts_ms)
    _validate_cutoff(int(cutoff_exclusive_ms), decision)
    validate_feature_dag()
    local_view = _cutoff_view(
        local_bars,
        int(cutoff_exclusive_ms),
        source_name=LOCAL_SOURCE,
    )
    reference_view = (
        None
        if reference_bars is None
        else _cutoff_view(
            reference_bars,
            int(cutoff_exclusive_ms),
            source_name=REFERENCE_SOURCE,
        )
    )

    values = _compute_local_features(local_view)
    values.update(_compute_reference_features(reference_view))
    if set(values) != set(FEATURE_SPEC_BY_NAME):
        missing = sorted(set(FEATURE_SPEC_BY_NAME) - set(values))
        extra = sorted(set(values) - set(FEATURE_SPEC_BY_NAME))
        raise FeatureContractError(
            f"generated feature schema mismatch: missing={missing} extra={extra}"
        )

    ready_times = [
        int(value.feature_ready_ts_ms)
        for value in values.values()
        if value.feature_ready_ts_ms is not None
    ]
    feature_ready_ts_ms = max(ready_times)
    if feature_ready_ts_ms > int(cutoff_exclusive_ms):
        raise FeatureContractError("feature row used data available after the canonical cutoff")
    if feature_ready_ts_ms > decision:
        raise FeatureContractError("feature_ready_ts_ms exceeds decision_ts_ms")

    fingerprint = _canonical_json_sha256(_row_fingerprint_payload(int(cutoff_exclusive_ms), values))
    return FeatureRow(
        cutoff_exclusive_ms=int(cutoff_exclusive_ms),
        decision_ts_ms=decision,
        feature_ready_ts_ms=feature_ready_ts_ms,
        values=dict(values),
        fingerprint_sha256=fingerprint,
    )


class Causal1sFeatureGenerator:
    """Stateful catch-up cursor with atomic advancement on successful rows."""

    def __init__(self, *, last_emitted_cutoff_ms: int) -> None:
        if int(last_emitted_cutoff_ms) <= 0 or int(last_emitted_cutoff_ms) % CADENCE_MS:
            raise FeatureContractError("last emitted cutoff must be a positive canonical edge")
        self._last_emitted_cutoff_ms = int(last_emitted_cutoff_ms)

    @property
    def last_emitted_cutoff_ms(self) -> int:
        return self._last_emitted_cutoff_ms

    def emit_through(
        self,
        local_bars: Sequence[OneSecondBar],
        *,
        completed_exclusive_ms: int,
        reference_bars: Sequence[OneSecondBar] | None = None,
    ) -> tuple[FeatureRow, ...]:
        completed = int(completed_exclusive_ms)
        if completed <= 0 or completed % CADENCE_MS:
            raise FeatureContractError("completed_exclusive_ms must be a canonical 1s edge")
        if completed < self._last_emitted_cutoff_ms:
            raise FeatureContractError("completed feature clock moved backwards")
        if completed == self._last_emitted_cutoff_ms:
            return ()

        cutoffs = range(self._last_emitted_cutoff_ms + CADENCE_MS, completed + 1, CADENCE_MS)
        rows = tuple(
            generate_feature_row(
                local_bars,
                cutoff_exclusive_ms=cutoff,
                decision_ts_ms=cutoff,
                reference_bars=reference_bars,
            )
            for cutoff in cutoffs
        )
        self._last_emitted_cutoff_ms = completed
        return rows


def canonical_timestamp_iso(cutoff_exclusive_ms: int) -> str:
    """Human-readable UTC timestamp used only by engineering reports."""

    return datetime.fromtimestamp(int(cutoff_exclusive_ms) / 1000.0, tz=UTC).isoformat()


validate_feature_dag()
