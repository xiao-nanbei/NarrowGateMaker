"""Causal semantics for the F05 multiscale EMA ADD-vs-WAIT study.

This module deliberately contains no strategy authority.  It defines the
continuous-time EMA basis, the external market-generation decision clock, the
baseline-aware action fallback, and the joint economic washout checks used to
build paired F05 labels.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

import pandas as pd

IDENTITY = "multiscale_ema_add_wait_incremental_value_v1"
SCHEMA_VERSION = "narrowgate_multiscale_ema_add_wait_incremental_value.v1"
EMA_HALF_LIVES_S = (0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0)
ADD_NOW = "ADD_NOW"
WAIT_ONE_EPOCH = "WAIT_ONE_EPOCH"


def _half_life_label(value: float) -> str:
    text = f"{float(value):g}".replace(".", "p")
    return f"h{text}s"


def _side_sign(side: str) -> float:
    normalized = str(side).upper()
    if normalized == "BUY":
        return 1.0
    if normalized == "SELL":
        return -1.0
    raise ValueError(f"unsupported side: {side}")


@dataclass(frozen=True)
class MarketGeneration:
    """Decision-visible market state, excluding all order/campaign state."""

    bbo_index: int
    l2_index: int
    trade_index: int
    feature_ready_index: int
    prediction_index: int
    snapshot_mid_tick_x2: int

    def validate(self) -> None:
        if self.bbo_index < 0 or self.l2_index < 0:
            raise ValueError("market generation requires valid BBO and L2 indexes")
        if self.trade_index < 0:
            raise ValueError("market generation requires a visible trade index")
        if self.feature_ready_index < 0 or self.prediction_index < 0:
            raise ValueError("market generation requires ready feature/model indexes")
        if self.snapshot_mid_tick_x2 <= 0:
            raise ValueError("market generation requires a positive snapshot mid tick")

    @property
    def identity(self) -> str:
        payload = {
            "bbo_index": int(self.bbo_index),
            "l2_index": int(self.l2_index),
            "trade_index": int(self.trade_index),
            "feature_ready_index": int(self.feature_ready_index),
            "prediction_index": int(self.prediction_index),
            "snapshot_mid_tick_x2": int(self.snapshot_mid_tick_x2),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "ascii"
        )
        return hashlib.sha256(encoded).hexdigest()

    def is_strictly_after(self, previous: MarketGeneration) -> bool:
        current_values = (
            self.bbo_index,
            self.l2_index,
            self.trade_index,
            self.feature_ready_index,
            self.prediction_index,
            self.snapshot_mid_tick_x2,
        )
        previous_values = (
            previous.bbo_index,
            previous.l2_index,
            previous.trade_index,
            previous.feature_ready_index,
            previous.prediction_index,
            previous.snapshot_mid_tick_x2,
        )
        # Source cursors are monotone. The paired snapshot mid is content, not
        # a cursor, and may move in either direction without implying a clock
        # regression.
        if any(
            current < prior
            for current, prior in zip(
                current_values[:-1], previous_values[:-1], strict=True
            )
        ):
            raise ValueError("market generation regressed")
        return current_values != previous_values


@dataclass(frozen=True)
class ExternalDecision:
    decision_ts_ms: int
    generation: MarketGeneration
    scheduled_requote: bool
    readiness: bool


def next_external_decision(
    decisions: Iterable[ExternalDecision],
    *,
    after_ts_ms: int,
    previous_generation: MarketGeneration,
) -> ExternalDecision | None:
    """Return the first candidate-independent ready scheduled decision."""

    for decision in decisions:
        if int(decision.decision_ts_ms) <= int(after_ts_ms):
            continue
        if not decision.scheduled_requote or not decision.readiness:
            continue
        if decision.generation.is_strictly_after(previous_generation):
            return decision
    return None


def baseline_add_action(*, cooldown_active: bool, baseline_can_add: bool) -> str:
    """Preserve the current cooldown behavior when model evidence is absent."""

    return ADD_NOW if baseline_can_add and not cooldown_active else WAIT_ONE_EPOCH


def choose_against_baseline(
    *,
    baseline_action: str,
    add_minus_wait_lcb_usdc: float,
    add_minus_wait_ucb_usdc: float,
    economic_threshold_usdc: float,
) -> str:
    """Depart from baseline only when the appropriate value bound is positive."""

    if baseline_action not in {ADD_NOW, WAIT_ONE_EPOCH}:
        raise ValueError(f"unsupported baseline action: {baseline_action}")
    if add_minus_wait_lcb_usdc > add_minus_wait_ucb_usdc:
        raise ValueError("incremental-value interval is inverted")
    if economic_threshold_usdc < 0.0:
        raise ValueError("economic threshold must be non-negative")

    if baseline_action == WAIT_ONE_EPOCH:
        return (
            ADD_NOW
            if add_minus_wait_lcb_usdc > economic_threshold_usdc
            else WAIT_ONE_EPOCH
        )
    return (
        WAIT_ONE_EPOCH
        if add_minus_wait_ucb_usdc < -economic_threshold_usdc
        else ADD_NOW
    )


class ContinuousTimeEmaSurface:
    """Irregular-clock EMA surface using one canonical decision-visible price."""

    def __init__(self, half_lives_s: Sequence[float] = EMA_HALF_LIVES_S) -> None:
        values = tuple(float(value) for value in half_lives_s)
        if not values or any(value <= 0.0 for value in values):
            raise ValueError("EMA half-lives must be positive")
        if values != tuple(sorted(set(values))):
            raise ValueError("EMA half-lives must be strictly increasing and unique")
        self.half_lives_s = values
        self._ema: list[float] = []
        self._velocity: list[float] = []
        self._last_ts_ns: int | None = None
        self._last_price: float | None = None
        self._adjacent_sign: list[int] = [0] * max(0, len(values) - 1)
        self._adjacent_cross_ts_ns: list[int | None] = [None] * max(
            0, len(values) - 1
        )
        self._ordering_signature: tuple[int, ...] | None = None
        self._ordering_change_ts_ns: int | None = None

    @property
    def initialized(self) -> bool:
        return self._last_ts_ns is not None

    @property
    def feature_ready_ts_ns(self) -> int:
        if self._last_ts_ns is None:
            raise RuntimeError("EMA surface is not initialized")
        return int(self._last_ts_ns)

    def update(self, *, ts_ns: int, price: float) -> None:
        timestamp = int(ts_ns)
        value = float(price)
        if timestamp < 0 or not math.isfinite(value) or value <= 0.0:
            raise ValueError("EMA observation requires a valid timestamp and price")
        if self._last_ts_ns is None:
            self._last_ts_ns = timestamp
            self._last_price = value
            self._ema = [value] * len(self.half_lives_s)
            self._velocity = [0.0] * len(self.half_lives_s)
            return
        if timestamp < self._last_ts_ns:
            raise ValueError("EMA observation clock regressed")
        if timestamp == self._last_ts_ns:
            if not math.isclose(value, float(self._last_price), rel_tol=0.0, abs_tol=1e-12):
                raise ValueError("duplicate EMA timestamp changed canonical price")
            return

        delta_s = float(timestamp - self._last_ts_ns) / 1_000_000_000.0
        prior = tuple(self._ema)
        for index, half_life_s in enumerate(self.half_lives_s):
            decay = math.exp(-math.log(2.0) * delta_s / half_life_s)
            current = decay * prior[index] + (1.0 - decay) * value
            self._ema[index] = current
            self._velocity[index] = (current - prior[index]) / delta_s

        for index in range(len(self._ema) - 1):
            difference = self._ema[index] - self._ema[index + 1]
            sign = 1 if difference > 0.0 else -1 if difference < 0.0 else 0
            previous_sign = self._adjacent_sign[index]
            if sign and previous_sign and sign != previous_sign:
                self._adjacent_cross_ts_ns[index] = timestamp
            elif sign and previous_sign == 0:
                self._adjacent_cross_ts_ns[index] = timestamp
            if sign:
                self._adjacent_sign[index] = sign

        ordering_signature = tuple(self._adjacent_sign)
        if (
            all(value != 0 for value in ordering_signature)
            and ordering_signature != self._ordering_signature
        ):
            self._ordering_signature = ordering_signature
            self._ordering_change_ts_ns = timestamp

        self._last_ts_ns = timestamp
        self._last_price = value

    def pair_z_bps(self, fast_half_life_s: float, slow_half_life_s: float) -> float:
        if not self.initialized:
            raise RuntimeError("EMA surface is not initialized")
        fast_index = self.half_lives_s.index(float(fast_half_life_s))
        slow_index = self.half_lives_s.index(float(slow_half_life_s))
        if fast_index >= slow_index:
            raise ValueError("EMA pair requires fast_half_life < slow_half_life")
        return 10_000.0 * (
            self._ema[fast_index] - self._ema[slow_index]
        ) / float(self._last_price)

    def feature_row(
        self,
        *,
        side: str,
        causal_volatility_bps: float,
        tick_bps: float,
    ) -> dict[str, float | int]:
        if not self.initialized:
            raise RuntimeError("EMA surface is not initialized")
        volatility_scale = max(
            float(causal_volatility_bps),
            float(tick_bps),
            1e-12,
        )
        if not math.isfinite(volatility_scale) or volatility_scale <= 0.0:
            raise ValueError("EMA volatility normalization is unsupported")
        sign = _side_sign(side)
        mid = float(self._last_price)
        output: dict[str, float | int] = {
            "ema_surface_feature_ready_ts_ns": int(self.feature_ready_ts_ns),
            "ema_surface_canonical_mid": mid,
        }
        adjacent: list[float] = []
        for half_life_s, ema, velocity in zip(
            self.half_lives_s, self._ema, self._velocity, strict=True
        ):
            label = _half_life_label(half_life_s)
            output[f"ema_rel_mid_bps_{label}"] = sign * 10_000.0 * (ema - mid) / mid
            output[f"ema_velocity_bps_per_s_{label}"] = (
                sign * 10_000.0 * velocity / mid
            )

        for index, (fast, slow) in enumerate(
            zip(
                self.half_lives_s[:-1],
                self.half_lives_s[1:],
                strict=True,
            )
        ):
            fast_label = _half_life_label(fast)
            slow_label = _half_life_label(slow)
            z_bps = 10_000.0 * (self._ema[index] - self._ema[index + 1]) / mid
            adjacent.append(sign * z_bps)
            prefix = f"ema_adjacent_{fast_label}_{slow_label}"
            output[f"{prefix}_favorable_bps"] = sign * z_bps
            output[f"{prefix}_cross_sign_diagnostic"] = int(
                sign * self._adjacent_sign[index]
            )
            cross_ts = self._adjacent_cross_ts_ns[index]
            cross_age_s = (
                float(self.feature_ready_ts_ns - cross_ts) / 1_000_000_000.0
                if cross_ts is not None
                else 0.0
            )
            output[f"{prefix}_cross_age_log1p_s"] = math.log1p(cross_age_s)
            output[f"{prefix}_cross_age_missing"] = int(cross_ts is None)
            output[f"{prefix}_volatility_normalized"] = (
                sign * z_bps / volatility_scale
            )

        for index in range(len(adjacent) - 1):
            left = _half_life_label(self.half_lives_s[index])
            center = _half_life_label(self.half_lives_s[index + 1])
            right = _half_life_label(self.half_lives_s[index + 2])
            output[f"ema_curvature_{left}_{center}_{right}_favorable_bps"] = (
                adjacent[index] - adjacent[index + 1]
            )

        output["ema_adjacent_favorable_positive_fraction"] = (
            sum(value > 0.0 for value in adjacent) / len(adjacent)
            if adjacent
            else 0.0
        )
        output["ema_adjacent_favorable_mean_bps"] = (
            sum(adjacent) / len(adjacent) if adjacent else 0.0
        )
        ordering_age_s = (
            float(self.feature_ready_ts_ns - self._ordering_change_ts_ns)
            / 1_000_000_000.0
            if self._ordering_change_ts_ns is not None
            else 0.0
        )
        output["ema_ordering_persistence_log1p_s"] = math.log1p(
            ordering_age_s
        )
        output["ema_ordering_persistence_missing"] = int(
            self._ordering_change_ts_ns is None
        )
        return output


@dataclass(frozen=True)
class ArmWashoutState:
    inventory_btc: float
    campaign_active: bool
    active_order_count: int
    pending_submit_count: int
    pending_cancel_count: int
    pending_ack_count: int
    descendant_unterminal_count: int
    cursor_owner_count: int
    hazard_owner_count: int
    second_assignment_count: int

    def complete(self, *, flat_epsilon_btc: float) -> bool:
        return bool(
            abs(float(self.inventory_btc)) <= float(flat_epsilon_btc)
            and not self.campaign_active
            and self.active_order_count == 0
            and self.pending_submit_count == 0
            and self.pending_cancel_count == 0
            and self.pending_ack_count == 0
            and self.descendant_unterminal_count == 0
            and self.cursor_owner_count == 0
            and self.hazard_owner_count == 0
            and self.second_assignment_count == 0
        )


def joint_washout_complete(
    control: ArmWashoutState,
    candidate: ArmWashoutState,
    *,
    flat_epsilon_btc: float = 1e-10,
) -> bool:
    if flat_epsilon_btc <= 0.0:
        raise ValueError("flat epsilon must be positive")
    return control.complete(flat_epsilon_btc=flat_epsilon_btc) and candidate.complete(
        flat_epsilon_btc=flat_epsilon_btc
    )


def campaign_unit_weights(
    frame: pd.DataFrame,
    *,
    campaign_columns: Sequence[str] = ("day", "side", "campaign_id"),
) -> pd.Series:
    """Assign each campaign total training weight one across all of its forks."""

    missing = [column for column in campaign_columns if column not in frame.columns]
    if missing:
        raise KeyError(f"campaign weight columns are missing: {missing}")
    if frame.empty:
        return pd.Series(index=frame.index, dtype=float)
    counts = frame.groupby(list(campaign_columns), observed=True)[
        campaign_columns[0]
    ].transform("size")
    if (counts <= 0).any():
        raise ValueError("campaign fork denominator is invalid")
    weights = 1.0 / counts.astype(float)
    totals = weights.groupby(
        [frame[column] for column in campaign_columns], observed=True
    ).sum()
    if not totals.map(lambda value: math.isclose(value, 1.0, abs_tol=1e-12)).all():
        raise ValueError("campaign total training weight drifted from one")
    return weights


def model_feature_names(
    half_lives_s: Sequence[float] = EMA_HALF_LIVES_S,
) -> tuple[str, ...]:
    """Return continuous EMA features; hard cross fields remain diagnostic-only."""

    values = tuple(float(value) for value in half_lives_s)
    labels = tuple(_half_life_label(value) for value in values)
    output: list[str] = []
    for label in labels:
        output.extend(
            (
                f"ema_rel_mid_bps_{label}",
                f"ema_velocity_bps_per_s_{label}",
            )
        )
    for fast, slow in zip(labels[:-1], labels[1:], strict=True):
        prefix = f"ema_adjacent_{fast}_{slow}"
        output.extend(
            (
                f"{prefix}_favorable_bps",
                f"{prefix}_cross_age_log1p_s",
                f"{prefix}_cross_age_missing",
                f"{prefix}_volatility_normalized",
            )
        )
    for left, center, right in zip(
        labels[:-2], labels[1:-1], labels[2:], strict=True
    ):
        output.append(f"ema_curvature_{left}_{center}_{right}_favorable_bps")
    output.extend(
        (
            "ema_adjacent_favorable_positive_fraction",
            "ema_adjacent_favorable_mean_bps",
            "ema_ordering_persistence_log1p_s",
            "ema_ordering_persistence_missing",
        )
    )
    return tuple(output)


def validate_feature_row(
    row: Mapping[str, object],
    half_lives_s: Sequence[float] = EMA_HALF_LIVES_S,
) -> None:
    feature_names = model_feature_names(half_lives_s)
    missing = sorted(set(feature_names) - set(row))
    if missing:
        raise ValueError(f"EMA feature row is incomplete: {missing}")
    for name in feature_names:
        value = float(row[name])
        if not math.isfinite(value):
            raise ValueError(f"EMA model feature is non-finite: {name}")


__all__ = [
    "ADD_NOW",
    "ArmWashoutState",
    "ContinuousTimeEmaSurface",
    "EMA_HALF_LIVES_S",
    "ExternalDecision",
    "IDENTITY",
    "MarketGeneration",
    "SCHEMA_VERSION",
    "WAIT_ONE_EPOCH",
    "baseline_add_action",
    "campaign_unit_weights",
    "choose_against_baseline",
    "joint_washout_complete",
    "model_feature_names",
    "next_external_decision",
    "validate_feature_row",
]
