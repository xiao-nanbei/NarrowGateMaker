#!/usr/bin/env python3
"""Vectorized, exact-semantics owner/modelled-queue feature construction.

The authoritative scalar feature state remains the semantic reference.  This
module accelerates only the repeated regular-grid EMA updates between sparse
fill-visible decisions.  It validates every source window in causal order,
vectorizes channel and half-life updates with NumPy, and writes the result back
into the reference state objects before taking a snapshot.

No label, reward, arm trace, Validation, or holdout input is accepted here.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_features as feature_engine,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_feature_panel as panel,
)

IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_duration_v2."
    "owner_modeled_queue_feature_batch.v1"
)
DEFAULT_MAX_BATCH_WINDOWS = 32_768
_NANOSECONDS_PER_SECOND = 1_000_000_000.0
_LOG_TWO = math.log(2.0)


class ModeledFeatureBatchError(panel.ModeledFeaturePanelError):
    """Raised when batching would change the frozen scalar semantics."""


@dataclass(frozen=True, slots=True)
class BatchUpdateAudit:
    """Mechanical evidence that the vectorized path was actually exercised."""

    window_count: int
    channel_update_count: int
    numpy_vectorized_step_count: int
    scalar_boundary_update_count: int


def _as_finite_float(value: Any, *, channel: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise feature_engine.FeatureContractError(
            f"observed channel is nonfinite: {channel}"
        ) from exc
    if not math.isfinite(result):
        raise feature_engine.FeatureContractError(
            f"observed channel is nonfinite: {channel}"
        )
    return result


def _advance_pair_states(
    channel: Any,
    *,
    timestamps_ns: np.ndarray,
    ema_history: np.ndarray,
) -> None:
    """Apply the scalar nonzero-sign/crossover state machine in bulk."""

    for fast, slow in channel.pairs:
        fast_index = channel._index[fast]
        slow_index = channel._index[slow]
        distance = ema_history[:, fast_index] - ema_history[:, slow_index]
        signs = np.where(distance > 0.0, 1, np.where(distance < 0.0, -1, 0))
        nonzero_index = np.flatnonzero(signs)
        if not len(nonzero_index):
            continue

        nonzero_signs = signs[nonzero_index].astype(np.int8, copy=False)
        state = channel.pair_state[(fast, slow)]
        initial_sign = int(state.effective_sign)
        if initial_sign == 0:
            state.effective_sign = int(nonzero_signs[0])
            state.arrangement_start_ts_ns = int(timestamps_ns[nonzero_index[0]])
            prior_signs = nonzero_signs[:-1]
            current_signs = nonzero_signs[1:]
            transition_offset = 1
        else:
            prior_signs = np.concatenate(
                (np.asarray([initial_sign], dtype=np.int8), nonzero_signs[:-1])
            )
            current_signs = nonzero_signs
            transition_offset = 0

        transitions = np.flatnonzero(current_signs != prior_signs)
        if len(transitions):
            transition_position = int(transitions[-1]) + transition_offset
            source_index = int(nonzero_index[transition_position])
            timestamp = int(timestamps_ns[source_index])
            direction = int(nonzero_signs[transition_position])
            state.arrangement_start_ts_ns = timestamp
            state.last_cross_ts_ns = timestamp
            state.last_cross_direction = direction
        state.effective_sign = int(nonzero_signs[-1])


def _advance_channels_exact(
    state: feature_engine.CausalMultichannelEmaState,
    *,
    timestamps_ns: np.ndarray,
    values: np.ndarray,
    observed: np.ndarray,
) -> tuple[int, int]:
    """Advance all channels with scalar operation order and NumPy broadcasting.

    A compiled IIR filter is a little faster, but its accumulation differs from
    the scalar reference by one or two ULPs on long runs.  This loop is over
    windows only; every channel and half-life update inside a window is one
    NumPy operation with the frozen scalar multiply/add order.
    """

    channels = tuple(state.channels.values())
    channel_count = len(channels)
    half_lives = np.asarray(channels[0].half_lives_s, dtype=np.float64)
    half_life_count = len(half_lives)
    count = len(timestamps_ns)
    ema = np.full((channel_count, half_life_count), np.nan, dtype=np.float64)
    velocity = np.full_like(ema, np.nan)
    acceleration = np.full_like(ema, np.nan)
    initialized = np.zeros(channel_count, dtype=np.bool_)
    last_ts_ns = np.full(channel_count, -1, dtype=np.int64)
    last_value = np.full(channel_count, np.nan, dtype=np.float64)
    for index, channel in enumerate(channels):
        if channel.last_ts_ns is None:
            continue
        initialized[index] = True
        last_ts_ns[index] = int(channel.last_ts_ns)
        ema[index] = np.asarray(channel.ema, dtype=np.float64)
        velocity[index] = np.asarray(channel.velocity, dtype=np.float64)
        acceleration[index] = np.asarray(channel.acceleration, dtype=np.float64)
        last_value[index] = float(channel.last_value)

    ema_history = np.empty(
        (count, channel_count, half_life_count), dtype=np.float64
    )
    decay_cache: dict[int, np.ndarray] = {}
    scalar_boundaries = 0
    vectorized_steps = 0
    width_ns = int(state.contract.base_window_width_ns)
    for row_index in range(count):
        selected = np.flatnonzero(observed[row_index])
        if len(selected):
            was_initialized = initialized[selected].copy()
            new = selected[~was_initialized]
            if len(new):
                ema[new] = values[row_index, new, None]
                velocity[new] = 0.0
                acceleration[new] = 0.0
                initialized[new] = True
                last_ts_ns[new] = int(timestamps_ns[row_index])
                last_value[new] = values[row_index, new]

            existing = selected[was_initialized]
            if len(existing):
                deltas_ns = int(timestamps_ns[row_index]) - last_ts_ns[existing]
                if np.any(deltas_ns <= 0):
                    raise feature_engine.FeatureContractError(
                        "channel EMA clock must increase"
                    )
                for delta_ns in np.unique(deltas_ns):
                    group = existing[deltas_ns == delta_ns]
                    delta_key = int(delta_ns)
                    decay = decay_cache.get(delta_key)
                    if decay is None:
                        delta_s = float(delta_key) / _NANOSECONDS_PER_SECOND
                        decay = np.asarray(
                            [
                                math.exp(-_LOG_TWO * delta_s / half_life)
                                for half_life in half_lives
                            ],
                            dtype=np.float64,
                        )
                        decay_cache[delta_key] = decay
                    delta_s = float(delta_key) / _NANOSECONDS_PER_SECOND
                    prior_ema = ema[group].copy()
                    prior_velocity = velocity[group].copy()
                    current = decay * prior_ema + (1.0 - decay) * values[
                        row_index, group, None
                    ]
                    current_velocity = (current - prior_ema) / delta_s
                    ema[group] = current
                    velocity[group] = current_velocity
                    acceleration[group] = (
                        current_velocity - prior_velocity
                    ) / delta_s
                    last_ts_ns[group] = int(timestamps_ns[row_index])
                    last_value[group] = values[row_index, group]
                    if delta_key != width_ns:
                        scalar_boundaries += len(group)
            vectorized_steps += 1
        ema_history[row_index] = ema

    for channel_index, channel in enumerate(channels):
        channel.current_window_observed = bool(observed[-1, channel_index])
        selected = np.flatnonzero(observed[:, channel_index])
        if not len(selected):
            continue
        _advance_pair_states(
            channel,
            timestamps_ns=timestamps_ns[selected],
            ema_history=ema_history[selected, channel_index, :],
        )
        channel.ema = ema[channel_index].tolist()
        channel.velocity = velocity[channel_index].tolist()
        channel.acceleration = acceleration[channel_index].tolist()
        channel.last_value = float(last_value[channel_index])
        channel.last_ts_ns = int(last_ts_ns[channel_index])
    return vectorized_steps, scalar_boundaries


class BatchCausalMultichannelEmaState:
    """Batch updater backed by the reference scalar snapshot state."""

    def __init__(
        self,
        *,
        block: str,
        warmup_identity: str,
        half_lives_s: Sequence[float] = feature_engine.EMA_HALF_LIVES_S,
    ) -> None:
        if not str(warmup_identity).strip():
            raise ModeledFeatureBatchError("warmup identity is empty")
        self.block = str(block)
        self.warmup_identity = str(warmup_identity)
        self.state = feature_engine.CausalMultichannelEmaState(
            block=self.block,
            half_lives_s=half_lives_s,
        )
        supported_views = tuple(
            candidate
            for candidate in ("R0", "M1", "M2")
            if set(spec.name for spec in feature_engine.CHANNELS_BY_BLOCK[candidate])
            <= set(self.state.channels)
        )
        self._views: dict[str, feature_engine.CausalMultichannelEmaState] = {}
        for candidate in supported_views:
            view = feature_engine.CausalMultichannelEmaState(
                block=candidate,
                half_lives_s=half_lives_s,
            )
            view.channels = {
                name: self.state.channels[name]
                for name in (
                    spec.name for spec in feature_engine.CHANNELS_BY_BLOCK[candidate]
                )
            }
            self._views[candidate] = view
        self._window_count = 0
        self._channel_update_count = 0
        self._numpy_vectorized_step_count = 0
        self._scalar_boundary_update_count = 0

    @property
    def last_right_ts_ns(self) -> int | None:
        return self.state.last_right_ts_ns

    def _validate_observations(
        self,
        observations: Sequence[feature_engine.CausalWindowObservation],
    ) -> tuple[np.ndarray, np.ndarray]:
        width = int(self.state.contract.base_window_width_ns)
        required = set(self.state.channels)
        count = len(observations)
        right = np.fromiter(
            (int(row.right_ts_ns) for row in observations),
            dtype=np.int64,
            count=count,
        )
        left = np.fromiter(
            (int(row.left_ts_ns) for row in observations),
            dtype=np.int64,
            count=count,
        )
        ready = np.fromiter(
            (int(row.feature_ready_ts_ns) for row in observations),
            dtype=np.int64,
            count=count,
        )
        market = np.fromiter(
            (int(row.market_generation) for row in observations),
            dtype=np.int64,
            count=count,
        )
        depth = np.fromiter(
            (int(row.depth_generation) for row in observations),
            dtype=np.int64,
            count=count,
        )
        for row in observations:
            missing = required - {str(name) for name in row.values}
            if missing:
                raise ModeledFeatureBatchError(
                    f"native M2 observation lacks {self.block} channels: "
                    f"{sorted(missing)}"
                )
        if np.any(right - left != width):
            raise feature_engine.FeatureContractError("window width drifted")
        if np.any(left % width) or np.any(right % width):
            raise feature_engine.FeatureContractError(
                "window is not aligned to the frozen grid"
            )
        if np.any(ready < right):
            raise feature_engine.FeatureContractError(
                "window became ready before its right edge"
            )
        if self.state.last_right_ts_ns is not None:
            if int(right[0]) <= int(self.state.last_right_ts_ns):
                raise feature_engine.FeatureContractError(
                    "window right edge did not increase"
                )
            if int(left[0]) != int(self.state.last_right_ts_ns):
                raise feature_engine.FeatureContractError(
                    "missing windows must be emitted explicitly as source gaps"
                )
            if int(ready[0]) < int(self.state.last_feature_ready_ts_ns):
                raise feature_engine.FeatureContractError(
                    "feature-ready clock regressed"
                )
            if int(market[0]) <= int(self.state.last_market_generation):
                raise feature_engine.FeatureContractError(
                    "market generation did not increase"
                )
            if int(depth[0]) < int(self.state.last_depth_generation):
                raise feature_engine.FeatureContractError(
                    "depth generation regressed"
                )
        if count > 1:
            if np.any(np.diff(right) <= 0):
                raise feature_engine.FeatureContractError(
                    "window right edge did not increase"
                )
            if np.any(left[1:] != right[:-1]):
                raise feature_engine.FeatureContractError(
                    "missing windows must be emitted explicitly as source gaps"
                )
            if np.any(np.diff(ready) < 0):
                raise feature_engine.FeatureContractError(
                    "feature-ready clock regressed"
                )
            if np.any(np.diff(market) <= 0):
                raise feature_engine.FeatureContractError(
                    "market generation did not increase"
                )
            if np.any(np.diff(depth) < 0):
                raise feature_engine.FeatureContractError(
                    "depth generation regressed"
                )
        return right, ready

    def update_many(
        self,
        observations: Sequence[feature_engine.CausalWindowObservation],
    ) -> BatchUpdateAudit:
        """Advance a bounded causal batch with scalar-equivalent state changes."""

        rows = tuple(observations)
        if not rows:
            return BatchUpdateAudit(0, 0, 0, 0)
        right, _ = self._validate_observations(rows)
        invalid = np.fromiter(
            (bool(row.source_gap or row.source_stale) for row in rows),
            dtype=np.bool_,
            count=len(rows),
        )
        channel_names = tuple(self.state.channels)
        observed = np.zeros((len(rows), len(channel_names)), dtype=np.bool_)
        values = np.full((len(rows), len(channel_names)), np.nan, dtype=np.float64)
        for row_index, row in enumerate(rows):
            if invalid[row_index]:
                continue
            for channel_index, name in enumerate(channel_names):
                raw_value = row.values.get(name)
                if raw_value is None:
                    continue
                observed[row_index, channel_index] = True
                values[row_index, channel_index] = _as_finite_float(
                    raw_value, channel=name
                )
        vectorized_steps, scalar_boundaries = _advance_channels_exact(
            self.state,
            timestamps_ns=right,
            values=values,
            observed=observed,
        )
        channel_updates = int(observed.sum())

        self.state.last_right_ts_ns = int(rows[-1].right_ts_ns)
        self.state.last_feature_ready_ts_ns = int(rows[-1].feature_ready_ts_ns)
        self.state.last_market_generation = int(rows[-1].market_generation)
        self.state.last_depth_generation = int(rows[-1].depth_generation)
        self.state.window_count += len(rows)
        self.state.gap_window_count += int(invalid.sum())
        if any(bool(row.warmup_admitted) for row in rows):
            self.state.warmup_admitted = True
            self.state.warmup_identity = self.warmup_identity

        self._window_count += len(rows)
        self._channel_update_count += channel_updates
        self._numpy_vectorized_step_count += vectorized_steps
        self._scalar_boundary_update_count += scalar_boundaries
        return BatchUpdateAudit(
            window_count=len(rows),
            channel_update_count=channel_updates,
            numpy_vectorized_step_count=vectorized_steps,
            scalar_boundary_update_count=scalar_boundaries,
        )

    def mark_current_window_unobserved(self) -> None:
        self.state.mark_current_window_unobserved()

    def market_feature_row(
        self,
        *,
        block: str,
        side: str,
        decision_ts_ns: int,
    ) -> dict[str, Any]:
        """Snapshot through the scalar formatter after syncing shared metadata."""

        if block not in self._views:
            raise ModeledFeatureBatchError(
                f"{block} is not available from a {self.block} batch state"
            )
        view = self._views[block]
        for name in (
            "last_right_ts_ns",
            "last_feature_ready_ts_ns",
            "last_market_generation",
            "last_depth_generation",
            "window_count",
            "gap_window_count",
            "warmup_admitted",
            "warmup_identity",
        ):
            setattr(view, name, getattr(self.state, name))
        return panel._market_feature_row(
            view,
            side=str(side).upper(),
            decision_ts_ns=int(decision_ts_ns),
        )

    def cumulative_audit(self) -> BatchUpdateAudit:
        return BatchUpdateAudit(
            window_count=self._window_count,
            channel_update_count=self._channel_update_count,
            numpy_vectorized_step_count=self._numpy_vectorized_step_count,
            scalar_boundary_update_count=self._scalar_boundary_update_count,
        )


def build_feature_frames_batch(
    opportunities: pd.DataFrame,
    *,
    m1_observations: Iterable[feature_engine.CausalWindowObservation],
    m1_warmup_identity: str,
    m2_observations: Iterable[feature_engine.CausalWindowObservation] | None = None,
    m2_warmup_identity: str | None = None,
    m0_enrichment: pd.DataFrame | None = None,
    allow_reduced_m0: bool = False,
    m2_day_supported: bool | None = None,
    max_batch_windows: int = DEFAULT_MAX_BATCH_WINDOWS,
) -> tuple[dict[str, pd.DataFrame], panel.FeatureBuildAudit]:
    """Batch-equivalent split-source implementation of the scalar builder."""

    if not str(m1_warmup_identity).strip():
        raise ModeledFeatureBatchError("normalized M1 warmup identity is empty")
    if int(max_batch_windows) <= 0:
        raise ModeledFeatureBatchError("max_batch_windows must be positive")
    day_values = opportunities["utc_day"].astype(str).unique()
    if len(day_values) != 1:
        raise ModeledFeatureBatchError("feature build requires exactly one UTC day")
    day = str(day_values[0])
    if day in panel.PREFIX40_DAYS:
        frozen_m2_support = day in panel.M2_COMMON_SUPPORT_DAYS
        if m2_day_supported is not None and bool(m2_day_supported) != frozen_m2_support:
            raise ModeledFeatureBatchError(
                f"M2 support override disagrees with frozen prefix split for {day}"
            )
        m2_day_supported = frozen_m2_support
    elif m2_day_supported is None:
        raise ModeledFeatureBatchError(
            "non-prefix test/diagnostic day requires explicit M2 support identity"
        )
    m2_day_supported = bool(m2_day_supported)
    census = panel._validate_census_frame(
        opportunities.loc[:, list(panel.CENSUS_SAFE_PROJECTION_COLUMNS)].copy(),
        day=day,
        require_immutable_r0=True,
    )

    if m2_day_supported and m2_observations is None:
        raise ModeledFeatureBatchError(
            "M2-supported day requires a distinct raw-native M2 stream"
        )
    if not m2_day_supported and m2_observations is not None:
        raise ModeledFeatureBatchError(
            "raw-native M2 stream is forbidden on a frozen M2-excluded day"
        )
    if m2_day_supported and not str(m2_warmup_identity or "").strip():
        raise ModeledFeatureBatchError("raw-native M2 warmup identity is empty")

    if m0_enrichment is None:
        if not allow_reduced_m0:
            missing = sorted(
                set(feature_engine.M0_REQUIRED_FIELDS) - panel.DIRECT_M0_FROM_CENSUS
            )
            raise ModeledFeatureBatchError(
                "full M0 enrichment is required; explicitly opt into reduced "
                f"UNOBSERVED support to proceed without: {missing}"
            )
        m0_by_id: dict[str, dict[str, Any]] = {}
        full_m0_support = False
        support_identity = panel.REDUCED_M0_SUPPORT_IDENTITY
    else:
        normalized_m0 = panel.validate_m0_enrichment(census, m0_enrichment)
        m0_by_id = {
            str(row["opportunity_id"]): {
                name: row[name] for name in feature_engine.M0_REQUIRED_FIELDS
            }
            for row in normalized_m0.to_dict("records")
        }
        full_m0_support = True
        support_identity = panel.FULL_M0_SUPPORT_IDENTITY

    m1_state = BatchCausalMultichannelEmaState(
        block="M1",
        warmup_identity=m1_warmup_identity,
    )
    raw_m2_state = (
        BatchCausalMultichannelEmaState(
            block="M2",
            warmup_identity=str(m2_warmup_identity),
        )
        if m2_day_supported
        else None
    )
    m1_iterator = iter(m1_observations)
    next_m1 = next(m1_iterator, None)
    raw_m2_iterator = iter(m2_observations or ())
    next_raw_m2 = next(raw_m2_iterator, None)
    m1_observation_count = 0
    m1_last_right_ts_ns = 0
    raw_m2_observation_count = 0
    raw_m2_last_right_ts_ns = 0
    output_rows: dict[str, list[dict[str, Any]]] = {
        block: [] for block in panel.FEATURE_BLOCKS
    }
    pending_m1: list[feature_engine.CausalWindowObservation] = []
    pending_raw_m2: list[feature_engine.CausalWindowObservation] = []

    def flush_m1() -> None:
        nonlocal m1_observation_count, m1_last_right_ts_ns
        if not pending_m1:
            return
        m1_state.update_many(tuple(pending_m1))
        m1_observation_count += len(pending_m1)
        m1_last_right_ts_ns = int(pending_m1[-1].right_ts_ns)
        pending_m1.clear()

    def flush_raw_m2() -> None:
        nonlocal raw_m2_observation_count, raw_m2_last_right_ts_ns
        if not pending_raw_m2:
            return
        if raw_m2_state is None:
            raise ModeledFeatureBatchError("raw M2 pending state exists on excluded day")
        raw_m2_state.update_many(tuple(pending_raw_m2))
        raw_m2_observation_count += len(pending_raw_m2)
        raw_m2_last_right_ts_ns = int(pending_raw_m2[-1].right_ts_ns)
        pending_raw_m2.clear()

    for census_row in census.to_dict("records"):
        cutoff_ns = int(census_row["fill_visible_ts_ms"]) * 1_000_000
        while next_m1 is not None and int(next_m1.feature_ready_ts_ns) <= cutoff_ns:
            pending_m1.append(next_m1)
            next_m1 = next(m1_iterator, None)
            if len(pending_m1) >= int(max_batch_windows):
                flush_m1()
        flush_m1()
        while (
            raw_m2_state is not None
            and next_raw_m2 is not None
            and int(next_raw_m2.feature_ready_ts_ns) <= cutoff_ns
        ):
            pending_raw_m2.append(next_raw_m2)
            next_raw_m2 = next(raw_m2_iterator, None)
            if len(pending_raw_m2) >= int(max_batch_windows):
                flush_raw_m2()
        flush_raw_m2()

        if not m1_observation_count:
            raise ModeledFeatureBatchError(
                "first opportunity precedes every normalized M1 feature-ready window"
            )
        if m2_day_supported and not raw_m2_observation_count:
            raise ModeledFeatureBatchError(
                "first opportunity precedes every raw-native M2 feature-ready window"
            )
        if cutoff_ns - int(m1_state.last_right_ts_ns or 0) >= (
            feature_engine.BASE_WINDOW_WIDTH_NS
        ):
            m1_state.mark_current_window_unobserved()
        if (
            raw_m2_state is not None
            and cutoff_ns - int(raw_m2_state.last_right_ts_ns or 0)
            >= feature_engine.BASE_WINDOW_WIDTH_NS
        ):
            raw_m2_state.mark_current_window_unobserved()

        opportunity_id = str(census_row["opportunity_id"])
        m0 = (
            m0_by_id[opportunity_id]
            if full_m0_support
            else panel._reduced_m0_context(census_row)
        )
        common = panel._common_owner_row(census_row)
        immutable_r0 = panel._immutable_r0_row(census_row)
        m0_fields = {name: m0[name] for name in feature_engine.M0_REQUIRED_FIELDS}
        m0_predicates = panel._m0_boolean_predicates(
            m0,
            unit_qty_btc=float(common["unit_qty_btc"]),
        )
        m0_meta = {
            "m0_support_valid": bool(full_m0_support),
            "m0_support_identity": support_identity,
            "m0_observed_fields_json": json.dumps(
                sorted(feature_engine.M0_REQUIRED_FIELDS)
                if full_m0_support
                else sorted(panel.DIRECT_M0_FROM_CENSUS),
                separators=(",", ":"),
            ),
            "m0_unobserved_fields_json": json.dumps(
                []
                if full_m0_support
                else sorted(
                    set(feature_engine.M0_REQUIRED_FIELDS)
                    - panel.DIRECT_M0_FROM_CENSUS
                ),
                separators=(",", ":"),
            ),
            "m0_missing_value_semantics": (
                "none_full_explicit_enrichment"
                if full_m0_support
                else "null_means_UNOBSERVED_not_zero"
            ),
        }

        m1_market = m1_state.market_feature_row(
            block="M1", side=common["side"], decision_ts_ns=cutoff_ns
        )
        m2_market = (
            panel._supported_m2_market_row(
                m1_market,
                raw_m2_state.market_feature_row(
                    block="M2", side=common["side"], decision_ts_ns=cutoff_ns
                ),
                decision_ts_ns=cutoff_ns,
            )
            if m2_day_supported
            else panel._unsupported_m2_market_row(
                m1_market, decision_ts_ns=cutoff_ns
            )
        )

        output_rows["R0"].append(
            {
                **common,
                "feature_block": "R0",
                **immutable_r0,
                "m0_support_valid": False,
                "m0_support_identity": "not_part_of_R0_estimand",
                "support_valid": True,
            }
        )
        output_rows["M0"].append(
            {
                **common,
                "feature_block": "M0",
                **m0_fields,
                **m0_meta,
                **m0_predicates,
                "market_support_valid": True,
                "support_valid": bool(full_m0_support),
            }
        )
        output_rows["M1"].append(
            {
                **common,
                "feature_block": "M1",
                **m0_fields,
                **m0_meta,
                **m0_predicates,
                **immutable_r0,
                **m1_market,
                "support_valid": bool(
                    full_m0_support and m1_market["market_support_valid"]
                ),
            }
        )
        output_rows["M2"].append(
            {
                **common,
                "feature_block": "M2",
                **m0_fields,
                **m0_meta,
                **m0_predicates,
                **immutable_r0,
                **m2_market,
                "support_valid": bool(
                    full_m0_support
                    and m2_day_supported
                    and m2_market["market_support_valid"]
                ),
            }
        )

    frames = {
        block: pd.DataFrame(rows)
        .sort_values("exposure_fill_ordinal", kind="stable")
        .reset_index(drop=True)
        for block, rows in output_rows.items()
    }
    supported = {
        block: int(frame["support_valid"].astype(bool).sum())
        for block, frame in frames.items()
    }
    return frames, panel.FeatureBuildAudit(
        opportunity_count=int(len(census)),
        observation_count=int(m1_observation_count + raw_m2_observation_count),
        last_observation_right_ts_ns=max(
            int(m1_last_right_ts_ns), int(raw_m2_last_right_ts_ns)
        ),
        full_m0_support=bool(full_m0_support),
        support_identity=support_identity,
        m2_day_supported=bool(m2_day_supported),
        block_supported_rows=supported,
        normalized_m1_observation_count=int(m1_observation_count),
        normalized_m1_last_right_ts_ns=int(m1_last_right_ts_ns),
        raw_m2_observation_count=int(raw_m2_observation_count),
        raw_m2_last_right_ts_ns=int(raw_m2_last_right_ts_ns),
    )


__all__ = [
    "BatchCausalMultichannelEmaState",
    "BatchUpdateAudit",
    "DEFAULT_MAX_BATCH_WINDOWS",
    "IDENTITY",
    "ModeledFeatureBatchError",
    "build_feature_frames_batch",
]
