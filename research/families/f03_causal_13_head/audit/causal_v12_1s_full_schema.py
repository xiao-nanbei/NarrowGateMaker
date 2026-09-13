#!/usr/bin/env python3
"""Full Python-side 1s Feature DAG for the causal-v12 cadence successor.

The module consumes raw, causally visible source observations.  It never
accepts completed 10s feature rows and never reads labels, predictions, or
economic outcomes.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from calendar_features import calendar_scalar_features
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_feature_generator as base,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema

SCHEMA_VERSION = "causal_v12_1s_full_feature_generator.v1"
METRICS_MAX_AGE_MS = 300_000
CROSS_SOURCE_MAX_AGE_MS = 30_000


@dataclass(frozen=True, slots=True)
class ExecutionL2Observation:
    """One completed 1s execution-book summary."""

    bucket_start_ts_ms: int
    feature_ready_ts_ms: int
    values: Mapping[str, float]

    def __post_init__(self) -> None:
        if self.bucket_start_ts_ms <= 0 or self.bucket_start_ts_ms % schema.CADENCE_MS:
            raise base.FeatureContractError("execution L2 bucket must be a canonical 1s edge")
        if self.feature_ready_ts_ms < self.bucket_start_ts_ms + schema.CADENCE_MS:
            raise base.FeatureContractError("execution L2 cannot be ready before bucket end")
        if tuple(self.values) != schema.EXECUTION_L2_FEATURES:
            raise base.FeatureContractError("execution L2 schema/order mismatch")
        if any(not math.isfinite(float(value)) for value in self.values.values()):
            raise base.FeatureContractError("execution L2 values must be finite")


@dataclass(frozen=True, slots=True)
class MetricObservation:
    """One causally visible Binance 5m metrics observation."""

    source_ts_ms: int
    feature_ready_ts_ms: int
    sum_open_interest: float
    toptrader_ls_ratio: float
    crowd_ls_ratio: float
    taker_ls_ratio: float

    def __post_init__(self) -> None:
        if self.source_ts_ms <= 0:
            raise base.FeatureContractError("metric source timestamp must be positive")
        if self.feature_ready_ts_ms < self.source_ts_ms:
            raise base.FeatureContractError("metric feature-ready time precedes source time")
        values = (
            self.sum_open_interest,
            self.toptrader_ls_ratio,
            self.crowd_ls_ratio,
            self.taker_ls_ratio,
        )
        if any(not math.isfinite(float(value)) or float(value) < 0.0 for value in values):
            raise base.FeatureContractError("metric values must be finite and non-negative")


@dataclass(frozen=True, slots=True)
class FullFeatureRow:
    cutoff_exclusive_ms: int
    decision_ts_ms: int
    feature_ready_ts_ms: int
    values: Mapping[str, base.FeatureValue]
    feature_order: tuple[str, ...]
    fingerprint_sha256: str
    feature_contract_sha256: str
    source_manifest_sha256: str
    head_linkage_sha256: str
    feature_dag_id: str = schema.FEATURE_DAG_ID
    feature_semantics_identity: str = schema.FEATURE_SEMANTICS_IDENTITY


def _source_for_feature(name: str) -> schema.SourceContract:
    for contract in schema.SOURCE_CONTRACTS:
        if name in contract.feature_names:
            return contract
    raise base.FeatureContractError(f"feature has no source contract: {name}")


def _lookback_ms(name: str) -> int:
    if name == "vol_regime_6h":
        return 21_600_000
    if name == "vol_regime_24h":
        return 86_400_000
    if name == "vol_regime_zscore":
        return 604_800_000
    if name.startswith(("oi_", "toptrader_", "crowd_", "taker_ls_")):
        return 21_600_000
    if name == "cv_ref_perp_basis_residual_bps":
        return 3_600_000
    for suffix in ("300s", "60s", "30s", "10s", "5s", "3s"):
        if suffix in name:
            return int(suffix[:-1]) * 1_000
    if name in {
        "tick_streak_max",
        "tick_mom_range",
        "micro_ret_std",
        "micro_ret_skew",
        "micro_ret_kurt",
        "tick_reversal_freq",
    }:
        return 10_000
    return schema.CADENCE_MS


def _unit(name: str) -> str:
    if name == "close" or name in {"bar_spread", "price_velocity", "price_acceleration"}:
        return "USDC_per_BTC"
    if name in {"volume", "buy_volume", "sell_volume", "l2_near_depth_total"}:
        return "BTC"
    if name.endswith("_bps"):
        return "bps"
    if "count" in name or name in {"tick_streak", "tick_streak_max", "near_candle_close"}:
        return "count"
    if name.startswith("cal_") or name.endswith("_available"):
        return "calendar_or_indicator"
    if name.endswith("_age_s"):
        return "seconds"
    if name.startswith("minutes_") or name == "dist_to_hour":
        return "minutes"
    if name.startswith("taker_signed_quote_sum"):
        return "USDC"
    return "dimensionless_or_rate"


def _build_full_specs() -> tuple[base.FeatureNodeSpec, ...]:
    result: list[base.FeatureNodeSpec] = []
    for name in schema.TRAINABLE_FEATURE_ORDER:
        source = _source_for_feature(name)
        dependencies: tuple[str, ...] = (f"raw.{source.name}",)
        if name == "return_abs":
            dependencies = ("return_1",)
        elif name == "large_trade_ratio":
            dependencies = ("avg_trade_size", "avg_trade_size_60s")
        elif name == "bar_spread_bps":
            dependencies = ("bar_spread", "close")
        result.append(
            base.FeatureNodeSpec(
                name=name,
                dependencies=dependencies,
                unit=_unit(name),
                cadence_ms=schema.CADENCE_MS,
                lookback_ms=_lookback_ms(name),
                source=source.name,
                source_clock=source.source_clock,
                availability_clock=source.feature_ready_rule,
                lag_state_rule=source.freshness_rule,
                minimum_observations=1,
                stateful=_lookback_ms(name) > schema.CADENCE_MS,
            )
        )
    return tuple(result)


FULL_FEATURE_SPECS = _build_full_specs()


def validate_full_feature_dag(
    specs: Sequence[base.FeatureNodeSpec] = FULL_FEATURE_SPECS,
) -> None:
    schema.validate_trainable_schema()
    base.validate_feature_dag(specs)
    order = tuple(spec.name for spec in specs)
    if order != schema.TRAINABLE_FEATURE_ORDER:
        raise base.FeatureContractError("full 1s DAG order differs from the v12 trainable ABI")


def full_feature_contract_payload() -> dict[str, Any]:
    validate_full_feature_dag()
    return {
        "cadence_ms": schema.CADENCE_MS,
        "feature_dag_id": schema.FEATURE_DAG_ID,
        "feature_order_sha256": schema.feature_order_sha256(),
        "feature_semantics_identity": schema.FEATURE_SEMANTICS_IDENTITY,
        "head_linkage_sha256": schema.canonical_sha256(schema.head_linkage_payload()),
        "identity": schema.IDENTITY,
        "nodes": [spec.contract_dict() for spec in FULL_FEATURE_SPECS],
        "schema_version": SCHEMA_VERSION,
        "source_manifest_sha256": schema.canonical_sha256(schema.source_manifest_payload()),
    }


def full_feature_contract_fingerprint() -> str:
    return schema.canonical_sha256(full_feature_contract_payload())


def _fv(
    value: float | int | None,
    *,
    source_ts_ms: int | None,
    ready_ts_ms: int | None,
    observation_count: int,
    required_observations: int = 1,
    missing_state: str = "warmup_insufficient",
) -> base.FeatureValue:
    if observation_count < required_observations:
        return base.FeatureValue(None, source_ts_ms, ready_ts_ms, observation_count, missing_state)
    if value is None or not math.isfinite(float(value)):
        return base.FeatureValue(
            None,
            source_ts_ms,
            ready_ts_ms,
            observation_count,
            "undefined_or_zero_denominator",
        )
    return base.FeatureValue(float(value), source_ts_ms, ready_ts_ms, observation_count, "ready")


def _missing(reason: str) -> base.FeatureValue:
    return base.FeatureValue(None, None, None, 0, reason)


def _tail(view: Sequence[base.OneSecondBar], count: int) -> tuple[base.OneSecondBar, ...]:
    return tuple(view[-count:])


def _diffs(view: Sequence[base.OneSecondBar]) -> list[float]:
    return [
        float(current.close) - float(previous.close)
        for previous, current in zip(view, view[1:], strict=False)
    ]


def _log_returns(view: Sequence[base.OneSecondBar]) -> list[float]:
    return [
        math.log(float(current.close) / float(previous.close))
        for previous, current in zip(view, view[1:], strict=False)
    ]


def _mean(values: Sequence[float]) -> float | None:
    return math.fsum(values) / len(values) if values else None


def _sample_std(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = math.fsum(values) / len(values)
    return math.sqrt(
        max(0.0, math.fsum((value - mean) ** 2 for value in values) / (len(values) - 1))
    )


def _skew(values: Sequence[float]) -> float | None:
    if len(values) < 5:
        return None
    mean = math.fsum(values) / len(values)
    second = math.fsum((value - mean) ** 2 for value in values) / len(values)
    if second <= 0.0:
        return 0.0
    third = math.fsum((value - mean) ** 3 for value in values) / len(values)
    return third / (second**1.5)


def _kurtosis(values: Sequence[float]) -> float | None:
    if len(values) < 5:
        return None
    mean = math.fsum(values) / len(values)
    second = math.fsum((value - mean) ** 2 for value in values) / len(values)
    if second <= 0.0:
        return 0.0
    fourth = math.fsum((value - mean) ** 4 for value in values) / len(values)
    return fourth / (second * second) - 3.0


def _ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator != 0.0 else None


def _signs(view: Sequence[base.OneSecondBar]) -> list[float]:
    return [1.0 if value > 0.0 else -1.0 if value < 0.0 else 0.0 for value in _diffs(view)]


def _ewm_last(values: Sequence[float], span: int) -> float | None:
    if not values:
        return None
    alpha = 2.0 / (span + 1.0)
    result = float(values[0])
    for value in values[1:]:
        result = alpha * float(value) + (1.0 - alpha) * result
    return result


def _bar_ready(view: Sequence[base.OneSecondBar]) -> tuple[int, int]:
    return int(view[-1].start_ts_ms), max(int(bar.finalized_ts_ms) for bar in view)


def _sweep_bps(bar: base.OneSecondBar, side: str) -> float:
    high = float(getattr(bar, f"{side}_price_high"))
    low = float(getattr(bar, f"{side}_price_low"))
    quote = float(getattr(bar, f"{side}_quote_qty"))
    if high <= 0.0 or low <= 0.0 or quote <= 0.0:
        return 0.0
    mid = 0.5 * (high + low)
    return (high - low) / mid * 10_000.0 if mid > 0.0 else 0.0


def _compute_trade_features(
    view: Sequence[base.OneSecondBar],
) -> dict[str, base.FeatureValue]:
    result: dict[str, base.FeatureValue] = {}
    current = view[-1]
    source_ts, ready_ts = _bar_ready((current,))
    for name in schema.BASE_FEATURES:
        result[name] = _fv(
            getattr(current, name),
            source_ts_ms=source_ts,
            ready_ts_ms=ready_ts,
            observation_count=1,
        )

    all_diffs = _diffs(view)
    all_signs = _signs(view)
    streak = 0.0
    streak_history: list[float] = []
    previous = 0.0
    for sign in all_signs:
        streak = streak + sign if sign != 0.0 and sign == previous else sign
        streak_history.append(streak)
        previous = sign
    result["tick_streak"] = _fv(
        streak_history[-1] if streak_history else None,
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(view),
        required_observations=2,
    )
    for seconds in (3, 5, 10):
        bars = _tail(view, seconds + 1)
        result[f"tick_mom_{seconds}s"] = _fv(
            math.fsum(_signs(bars)) if len(bars) == seconds + 1 else None,
            source_ts_ms=source_ts,
            ready_ts_ms=ready_ts,
            observation_count=len(bars),
            required_observations=seconds + 1,
        )
    for span in (3, 10):
        result[f"tick_ewm_{span}s"] = _fv(
            _ewm_last(all_diffs, span),
            source_ts_ms=source_ts,
            ready_ts_ms=ready_ts,
            observation_count=len(view),
            required_observations=2,
        )
    micro = all_diffs[-10:]
    for name, value, required in (
        ("micro_ret_std", _sample_std(micro), 3),
        ("micro_ret_skew", _skew(micro), 5),
        ("micro_ret_kurt", _kurtosis(micro), 5),
    ):
        result[name] = _fv(
            value,
            source_ts_ms=source_ts,
            ready_ts_ms=ready_ts,
            observation_count=len(micro),
            required_observations=required,
        )
    recent_signs = all_signs[-10:]
    reversals = [
        1.0 if current_sign != prior else 0.0
        for prior, current_sign in zip(recent_signs, recent_signs[1:], strict=False)
    ]
    result["tick_reversal_freq"] = _fv(
        _mean(reversals),
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(recent_signs),
        required_observations=3,
    )
    signed = [float(bar.buy_volume) - float(bar.sell_volume) for bar in view]
    result["flow_velocity"] = _fv(
        signed[-1], source_ts_ms=source_ts, ready_ts_ms=ready_ts, observation_count=1
    )
    result["flow_acceleration"] = _fv(
        signed[-1] - signed[-2] if len(signed) >= 2 else None,
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(signed),
        required_observations=2,
    )
    result["tick_streak_max"] = _fv(
        max((abs(value) for value in streak_history[-10:]), default=None),
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=min(10, len(streak_history)),
    )
    momentum5: list[float] = []
    for end in range(max(0, len(all_signs) - 10), len(all_signs)):
        momentum5.append(math.fsum(all_signs[max(0, end - 4) : end + 1]))
    result["tick_mom_range"] = _fv(
        max(momentum5) - min(momentum5) if momentum5 else None,
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(momentum5),
    )

    for window in (5, 10, 30, 60):
        bars = _tail(view, window)
        buy_quote = math.fsum(float(bar.buy_quote_qty) for bar in bars)
        sell_quote = math.fsum(float(bar.sell_quote_qty) for bar in bars)
        total_quote = buy_quote + sell_quote
        values = {
            f"taker_quote_imbalance_{window}s": _ratio(buy_quote - sell_quote, total_quote),
            f"taker_signed_quote_sum_{window}s": buy_quote - sell_quote,
            f"taker_trade_count_sum_{window}s": math.fsum(float(bar.trade_count) for bar in bars),
            f"taker_max_same_side_run_{window}s": max(
                (float(bar.max_same_side_run) for bar in bars), default=0.0
            ),
            f"taker_buy_sweep_score_{window}s": max(
                (_sweep_bps(bar, "buy") for bar in bars), default=0.0
            )
            * math.log1p(max(buy_quote, 0.0)),
            f"taker_sell_sweep_score_{window}s": max(
                (_sweep_bps(bar, "sell") for bar in bars), default=0.0
            )
            * math.log1p(max(sell_quote, 0.0)),
            f"taker_buy_iceberg_pressure_sum_{window}s": math.fsum(
                float(bar.buy_count) / (1.0 + _sweep_bps(bar, "buy")) for bar in bars
            ),
            f"taker_sell_iceberg_pressure_sum_{window}s": math.fsum(
                float(bar.sell_count) / (1.0 + _sweep_bps(bar, "sell")) for bar in bars
            ),
        }
        for name, value in values.items():
            result[name] = _fv(
                value,
                source_ts_ms=source_ts,
                ready_ts_ms=ready_ts,
                observation_count=len(bars),
                required_observations=window,
            )
    return result


def _compute_local_microstructure(
    view: Sequence[base.OneSecondBar],
) -> dict[str, base.FeatureValue]:
    result: dict[str, base.FeatureValue] = {}
    source_ts, ready_ts = _bar_ready((view[-1],))
    current = view[-1]
    current_total = float(current.buy_volume) + float(current.sell_volume)
    result["volume_imbalance"] = _fv(
        _ratio(float(current.buy_volume) - float(current.sell_volume), current_total),
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=1,
    )
    for seconds in (5, 30, 60, 300):
        bars = _tail(view, seconds)
        return_bars = _tail(view, seconds + 1)
        returns = _log_returns(return_bars)
        buy = math.fsum(float(bar.buy_volume) for bar in bars)
        sell = math.fsum(float(bar.sell_volume) for bar in bars)
        total = buy + sell
        absolute = math.fsum(abs(float(bar.buy_volume) - float(bar.sell_volume)) for bar in bars)
        result[f"volatility_{seconds}s"] = _fv(
            (_sample_std(returns) or 0.0) * math.sqrt(float(seconds))
            if len(returns) >= 2
            else None,
            source_ts_ms=source_ts,
            ready_ts_ms=ready_ts,
            observation_count=len(return_bars),
            required_observations=3,
        )
        result[f"volume_imbalance_{seconds}s"] = _fv(
            _ratio(buy - sell, total),
            source_ts_ms=source_ts,
            ready_ts_ms=ready_ts,
            observation_count=len(bars),
        )
        result[f"trade_intensity_{seconds}s"] = _fv(
            _mean([float(bar.trade_count) for bar in bars]),
            source_ts_ms=source_ts,
            ready_ts_ms=ready_ts,
            observation_count=len(bars),
        )
        result[f"vpin_{seconds}s"] = _fv(
            _ratio(absolute, total),
            source_ts_ms=source_ts,
            ready_ts_ms=ready_ts,
            observation_count=len(bars),
        )
        result[f"price_change_{seconds}s"] = _fv(
            float(return_bars[-1].close) / float(return_bars[0].close) - 1.0
            if len(return_bars) == seconds + 1
            else None,
            source_ts_ms=source_ts,
            ready_ts_ms=ready_ts,
            observation_count=len(return_bars),
            required_observations=seconds + 1,
        )
    diffs = _diffs(view)
    result["price_velocity"] = _fv(
        diffs[-1] if diffs else None,
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(view),
        required_observations=2,
    )
    result["price_acceleration"] = _fv(
        diffs[-1] - diffs[-2] if len(diffs) >= 2 else None,
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(view),
        required_observations=3,
    )
    current_avg = _ratio(float(current.volume), float(current.trade_count))
    size_bars = _tail(view, 60)
    sizes = [
        value
        for bar in size_bars
        if (value := _ratio(float(bar.volume), float(bar.trade_count))) is not None
    ]
    average_size = _mean(sizes)
    result["avg_trade_size"] = _fv(
        current_avg, source_ts_ms=source_ts, ready_ts_ms=ready_ts, observation_count=1
    )
    result["avg_trade_size_60s"] = _fv(
        average_size,
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(size_bars),
    )
    result["large_trade_ratio"] = _fv(
        _ratio(float(current_avg or 0.0), float(average_size or 0.0)),
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(size_bars),
    )
    volume_bars = _tail(view, 300)
    volumes = [float(bar.volume) for bar in volume_bars]
    volume_mean = _mean(volumes)
    volume_std = _sample_std(volumes)
    result["volume_zscore"] = _fv(
        (float(current.volume) - float(volume_mean)) / float(volume_std)
        if volume_mean is not None and volume_std not in (None, 0.0)
        else None,
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(volume_bars),
        required_observations=2,
    )
    spread = float(current.high) - float(current.low)
    result["bar_spread"] = _fv(
        spread, source_ts_ms=source_ts, ready_ts_ms=ready_ts, observation_count=1
    )
    result["bar_spread_bps"] = _fv(
        spread / float(current.close) * 10_000.0,
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=1,
    )
    one_return = _log_returns(_tail(view, 2))
    result["return_1"] = _fv(
        one_return[-1] if one_return else None,
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(view),
        required_observations=2,
    )
    result["return_abs"] = _fv(
        abs(one_return[-1]) if one_return else None,
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(view),
        required_observations=2,
    )
    absolute_returns = [abs(value) for value in _log_returns(view)]
    vol6_values = absolute_returns[-21_600:]
    vol24_values = absolute_returns[-86_400:]
    vol6 = _mean(vol6_values)
    vol24 = _mean(vol24_values)
    result["vol_regime_6h"] = _fv(
        vol6,
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(vol6_values),
        required_observations=3_600,
    )
    result["vol_regime_24h"] = _fv(
        vol24,
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(vol24_values),
        required_observations=21_600,
    )
    daily_blocks = [
        _mean(absolute_returns[max(0, end - 21_600) : end])
        for end in range(
            max(21_600, len(absolute_returns) - 604_800), len(absolute_returns) + 1, 3_600
        )
    ]
    supported_blocks = [float(value) for value in daily_blocks if value is not None]
    block_std = _sample_std(supported_blocks)
    result["vol_regime_zscore"] = _fv(
        (float(vol6) - float(_mean(supported_blocks))) / float(block_std)
        if vol6 is not None and _mean(supported_blocks) is not None and block_std not in (None, 0.0)
        else None,
        source_ts_ms=source_ts,
        ready_ts_ms=ready_ts,
        observation_count=len(supported_blocks),
        required_observations=24,
    )
    return result


def _compute_execution_l2(
    observations: Sequence[ExecutionL2Observation], cutoff: int
) -> dict[str, base.FeatureValue]:
    starts = [int(item.bucket_start_ts_ms) for item in observations]
    if len(starts) != len(set(starts)):
        raise base.FeatureContractError("duplicate execution L2 source clock")
    target = cutoff - schema.CADENCE_MS
    matching = [item for item in observations if int(item.bucket_start_ts_ms) == target]
    if not matching:
        return {
            name: _missing("execution_l2_exact_bucket_missing_no_carry")
            for name in schema.EXECUTION_L2_FEATURES
        }
    item = matching[0]
    if int(item.feature_ready_ts_ms) > cutoff:
        return {
            name: _missing("execution_l2_late_at_cutoff") for name in schema.EXECUTION_L2_FEATURES
        }
    return {
        name: _fv(
            item.values[name],
            source_ts_ms=item.bucket_start_ts_ms,
            ready_ts_ms=item.feature_ready_ts_ms,
            observation_count=1,
        )
        for name in schema.EXECUTION_L2_FEATURES
    }


def _zscore(values: Sequence[float]) -> float | None:
    std = _sample_std(values)
    mean = _mean(values)
    if not values or std in (None, 0.0) or mean is None:
        return None
    return (float(values[-1]) - mean) / std


def _close_at_or_before(view: Sequence[base.OneSecondBar], timestamp_ms: int) -> float | None:
    matches = [bar for bar in view if int(bar.start_ts_ms) <= int(timestamp_ms)]
    return float(matches[-1].close) if matches else None


def _compute_metrics(
    observations: Sequence[MetricObservation],
    local_view: Sequence[base.OneSecondBar],
    cutoff: int,
) -> dict[str, base.FeatureValue]:
    source_times = [int(item.source_ts_ms) for item in observations]
    if len(source_times) != len(set(source_times)):
        raise base.FeatureContractError("duplicate metrics source clock")
    visible = sorted(
        (
            item
            for item in observations
            if int(item.source_ts_ms) < cutoff and int(item.feature_ready_ts_ms) <= cutoff
        ),
        key=lambda item: int(item.source_ts_ms),
    )
    if not visible or cutoff - int(visible[-1].source_ts_ms) > METRICS_MAX_AGE_MS:
        return {
            name: _missing("metrics_missing_or_stale_no_default") for name in schema.METRIC_FEATURES
        }
    current = visible[-1]
    source_ts = int(current.source_ts_ms)
    ready_ts = int(current.feature_ready_ts_ms)
    one_hour = [item for item in visible if source_ts - int(item.source_ts_ms) <= 3_600_000]
    six_hour = [item for item in visible if source_ts - int(item.source_ts_ms) <= 21_600_000]
    prior = visible[-2] if len(visible) >= 2 else None
    oi = float(current.sum_open_interest)
    previous_oi = float(prior.sum_open_interest) if prior is not None else None
    oi_pct = _ratio(oi - float(previous_oi or 0.0), float(previous_oi or 0.0))
    oi_1h = [float(item.sum_open_interest) for item in one_hour]
    oi_6h = [float(item.sum_open_interest) for item in six_hour]
    oi_short = _mean(oi_1h)
    oi_long = _mean(oi_6h)
    result_values: dict[str, float | None] = {
        "oi_log": math.log(oi) if oi > 0.0 else None,
        "oi_pct_change": oi_pct,
        "oi_zscore_1h": _zscore(oi_1h),
        "oi_zscore_6h": _zscore(oi_6h),
        "oi_momentum": _ratio(
            float(oi_short or 0.0) - float(oi_long or 0.0), float(oi_long or 0.0)
        ),
        "toptrader_ls_ratio": float(current.toptrader_ls_ratio),
        "crowd_ls_ratio": float(current.crowd_ls_ratio),
        "taker_ls_ratio": float(current.taker_ls_ratio),
        "toptrader_ls_zscore": _zscore([float(item.toptrader_ls_ratio) for item in six_hour]),
        "crowd_ls_zscore": _zscore([float(item.crowd_ls_ratio) for item in six_hour]),
        "taker_ls_zscore": _zscore([float(item.taker_ls_ratio) for item in six_hour]),
        "taker_ls_momentum": (
            float(_mean([float(item.taker_ls_ratio) for item in one_hour]) or 0.0)
            - float(_mean([float(item.taker_ls_ratio) for item in six_hour]) or 0.0)
        ),
        "oi_price_divergence": None,
    }
    if prior is not None and previous_oi not in (None, 0.0):
        previous_close = _close_at_or_before(local_view, int(prior.source_ts_ms))
        current_close = float(local_view[-1].close)
        price_change = (
            current_close / previous_close - 1.0 if previous_close not in (None, 0.0) else None
        )
        if price_change is not None and oi_pct is not None:
            result_values["oi_price_divergence"] = oi_pct - price_change
    return {
        name: _fv(
            result_values[name],
            source_ts_ms=source_ts,
            ready_ts_ms=ready_ts,
            observation_count=len(visible),
        )
        for name in schema.METRIC_FEATURES
    }


def _optional_reference_tail(
    bars: Sequence[base.OneSecondBar], cutoff: int
) -> tuple[base.OneSecondBar, ...] | None:
    candidates = sorted(
        (
            bar
            for bar in bars
            if int(bar.start_ts_ms) < cutoff and int(bar.finalized_ts_ms) <= cutoff
        ),
        key=lambda bar: int(bar.start_ts_ms),
    )
    starts = [int(bar.start_ts_ms) for bar in candidates]
    if len(starts) != len(set(starts)):
        raise base.FeatureContractError("duplicate reference source clock")
    if not candidates:
        return None
    latest_end = int(candidates[-1].start_ts_ms) + schema.CADENCE_MS
    if cutoff - latest_end > CROSS_SOURCE_MAX_AGE_MS:
        return None
    start = len(candidates) - 1
    while (
        start > 0
        and int(candidates[start].start_ts_ms)
        == int(candidates[start - 1].start_ts_ms) + schema.CADENCE_MS
    ):
        start -= 1
    return tuple(candidates[start:])


def _compute_cross_market(
    reference_bars: Sequence[base.OneSecondBar],
    local_view: Sequence[base.OneSecondBar],
    cutoff: int,
) -> dict[str, base.FeatureValue]:
    view = _optional_reference_tail(reference_bars, cutoff)
    if view is None:
        return {
            name: _missing("cross_market_missing_or_stale_no_forward_fill")
            for name in schema.CROSS_MARKET_FEATURES
        }
    source_ts, ready_ts = _bar_ready((view[-1],))
    local_close = float(local_view[-1].close)
    reference_close = float(view[-1].close)
    basis = (reference_close - local_close) / local_close * 10_000.0
    values: dict[str, float | None] = {
        "cv_ref_perp_basis_bps": basis,
        "cv_ref_perp_age_s": max(0.0, (cutoff - ready_ts) / 1_000.0),
        "cv_ref_perp_available": 1.0,
    }
    for seconds in (10, 30, 60):
        bars = _tail(view, seconds + 1)
        values[f"cv_ref_perp_ret_{seconds}s"] = (
            float(bars[-1].close) / float(bars[0].close) - 1.0 if len(bars) == seconds + 1 else None
        )
    bars60 = _tail(view, 61)
    returns60 = _log_returns(bars60)
    values["cv_ref_perp_volatility_60s"] = (
        (_sample_std(returns60) or 0.0) * math.sqrt(60.0) if len(returns60) >= 2 else None
    )
    volume_bars = _tail(view, 60)
    buy = math.fsum(float(bar.buy_volume) for bar in volume_bars)
    sell = math.fsum(float(bar.sell_volume) for bar in volume_bars)
    total = buy + sell
    values["cv_ref_perp_volume_imbalance"] = _ratio(buy - sell, total)
    values["cv_ref_perp_trade_intensity_60s"] = _mean(
        [float(bar.trade_count) for bar in volume_bars]
    )
    values["cv_ref_perp_vpin_60s"] = _ratio(
        math.fsum(abs(float(bar.buy_volume) - float(bar.sell_volume)) for bar in volume_bars),
        total,
    )
    local_by_ts = {int(bar.start_ts_ms): float(bar.close) for bar in local_view}
    historical_basis = [
        (float(bar.close) - local_by_ts[int(bar.start_ts_ms)])
        / local_by_ts[int(bar.start_ts_ms)]
        * 10_000.0
        for bar in view[-3_601:-1]
        if int(bar.start_ts_ms) in local_by_ts
    ]
    values["cv_ref_perp_basis_residual_bps"] = (
        basis - sorted(historical_basis)[len(historical_basis) // 2]
        if len(historical_basis) >= 30
        else None
    )
    return {
        name: _fv(
            values.get(name),
            source_ts_ms=source_ts,
            ready_ts_ms=ready_ts,
            observation_count=len(view),
        )
        for name in schema.CROSS_MARKET_FEATURES
    }


def _compute_calendar(cutoff: int) -> dict[str, base.FeatureValue]:
    instant = datetime.fromtimestamp(cutoff / 1_000.0, tz=UTC)
    values = calendar_scalar_features(instant, prefix="cal_", include_legacy=False)
    minutes_in_day = instant.hour * 60 + instant.minute
    funding = next(
        (value - minutes_in_day for value in (480, 960, 1_440) if value > minutes_in_day), 480.0
    )
    minute_of_hour = instant.minute + instant.second / 60.0
    dist = min(minute_of_hour, 60.0 - minute_of_hour, abs(minute_of_hour - 30.0))
    values.update(
        {
            "minutes_to_funding": float(funding),
            "funding_phase": float(funding) / 480.0,
            "funding_sin": math.sin(2.0 * math.pi * (1.0 - float(funding) / 480.0)),
            "funding_cos": math.cos(2.0 * math.pi * (1.0 - float(funding) / 480.0)),
            "dist_to_hour": dist,
            "near_candle_close": 1.0 if dist < 2.0 else 0.0,
        }
    )
    expected = set((*schema.CALENDAR_FEATURES, *schema.TIME_FEATURES))
    selected = {name: values[name] for name in expected}
    return {
        name: _fv(
            selected[name],
            source_ts_ms=cutoff,
            ready_ts_ms=cutoff,
            observation_count=1,
        )
        for name in expected
    }


def _fingerprint_payload(cutoff: int, values: Mapping[str, base.FeatureValue]) -> dict[str, Any]:
    return {
        "cutoff_exclusive_ms": cutoff,
        "feature_contract_sha256": full_feature_contract_fingerprint(),
        "feature_order_sha256": schema.feature_order_sha256(),
        "source_manifest_sha256": schema.canonical_sha256(schema.source_manifest_payload()),
        "values": [
            {
                "name": name,
                "value_hex": None
                if values[name].value is None
                else float(values[name].value).hex(),
                "source_latest_ts_ms": values[name].source_latest_ts_ms,
                "feature_ready_ts_ms": values[name].feature_ready_ts_ms,
                "observation_count": values[name].observation_count,
                "lag_state": values[name].lag_state,
            }
            for name in schema.TRAINABLE_FEATURE_ORDER
        ],
    }


def generate_full_feature_row(
    local_bars: Sequence[base.OneSecondBar],
    *,
    cutoff_exclusive_ms: int,
    decision_ts_ms: int | None = None,
    execution_l2: Sequence[ExecutionL2Observation] = (),
    metrics: Sequence[MetricObservation] = (),
    reference_bars: Sequence[base.OneSecondBar] = (),
) -> FullFeatureRow:
    """Generate the exact 173-column training ABI from raw source observations."""

    cutoff = int(cutoff_exclusive_ms)
    decision = cutoff if decision_ts_ms is None else int(decision_ts_ms)
    base._validate_cutoff(cutoff, decision)
    validate_full_feature_dag()
    local_view = base._cutoff_view(local_bars, cutoff, source_name=base.LOCAL_SOURCE)

    values: dict[str, base.FeatureValue] = {}
    values.update(_compute_trade_features(local_view))
    values.update(_compute_execution_l2(execution_l2, cutoff))
    values.update(_compute_metrics(metrics, local_view, cutoff))
    values.update(_compute_local_microstructure(local_view))
    values.update(_compute_cross_market(reference_bars, local_view, cutoff))
    values.update(_compute_calendar(cutoff))
    if (
        tuple(name for name in schema.TRAINABLE_FEATURE_ORDER if name in values)
        != schema.TRAINABLE_FEATURE_ORDER
    ):
        missing = [name for name in schema.TRAINABLE_FEATURE_ORDER if name not in values]
        extra = sorted(set(values) - set(schema.TRAINABLE_FEATURE_ORDER))
        raise base.FeatureContractError(
            f"full generated schema mismatch: missing={missing} extra={extra}"
        )
    ordered = {name: values[name] for name in schema.TRAINABLE_FEATURE_ORDER}
    ready_times = [
        int(value.feature_ready_ts_ms)
        for value in ordered.values()
        if value.feature_ready_ts_ms is not None
    ]
    feature_ready = max(ready_times)
    if feature_ready > cutoff or feature_ready > decision:
        raise base.FeatureContractError("full feature row violates feature-ready causality")
    fingerprint = schema.canonical_sha256(_fingerprint_payload(cutoff, ordered))
    return FullFeatureRow(
        cutoff_exclusive_ms=cutoff,
        decision_ts_ms=decision,
        feature_ready_ts_ms=feature_ready,
        values=ordered,
        feature_order=schema.TRAINABLE_FEATURE_ORDER,
        fingerprint_sha256=fingerprint,
        feature_contract_sha256=full_feature_contract_fingerprint(),
        source_manifest_sha256=schema.canonical_sha256(schema.source_manifest_payload()),
        head_linkage_sha256=schema.canonical_sha256(schema.head_linkage_payload()),
    )


class CausalV12FullSchema1sGenerator:
    """Atomic 1s catch-up cursor for full-schema rows."""

    def __init__(self, *, last_emitted_cutoff_ms: int) -> None:
        if last_emitted_cutoff_ms <= 0 or last_emitted_cutoff_ms % schema.CADENCE_MS:
            raise base.FeatureContractError("last emitted cutoff must be canonical 1s")
        self._last_emitted_cutoff_ms = int(last_emitted_cutoff_ms)

    @property
    def last_emitted_cutoff_ms(self) -> int:
        return self._last_emitted_cutoff_ms

    def emit_through(
        self,
        local_bars: Sequence[base.OneSecondBar],
        *,
        completed_exclusive_ms: int,
        execution_l2: Sequence[ExecutionL2Observation] = (),
        metrics: Sequence[MetricObservation] = (),
        reference_bars: Sequence[base.OneSecondBar] = (),
    ) -> tuple[FullFeatureRow, ...]:
        completed = int(completed_exclusive_ms)
        if completed < self._last_emitted_cutoff_ms:
            raise base.FeatureContractError("completed full-schema clock moved backwards")
        if completed % schema.CADENCE_MS:
            raise base.FeatureContractError("completed cutoff must be canonical 1s")
        rows = tuple(
            generate_full_feature_row(
                local_bars,
                cutoff_exclusive_ms=cutoff,
                execution_l2=execution_l2,
                metrics=metrics,
                reference_bars=reference_bars,
            )
            for cutoff in range(
                self._last_emitted_cutoff_ms + schema.CADENCE_MS,
                completed + 1,
                schema.CADENCE_MS,
            )
        )
        self._last_emitted_cutoff_ms = completed
        return rows
