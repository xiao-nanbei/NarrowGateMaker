from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_features as feature_engine,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_feature_batch as batch,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_feature_panel as panel,
)

WIDTH_NS = feature_engine.BASE_WINDOW_WIDTH_NS
BASE_NS = 1_800_000_000_000_000_000


def _channel_value(name: str, index: int) -> float:
    phase = float(index) * 0.19
    reversal = -0.055 * max(index - 44, 0)
    if name == "mid_usdc_per_btc":
        return 60_000.0 + 0.05 * index + reversal + 0.18 * math.sin(phase)
    if name == "spread_bps":
        return 3.0 + 0.2 * math.sin(phase * 0.5)
    if "imbalance" in name or "deviation" in name:
        return 0.25 * math.sin(phase + len(name) * 0.03)
    if "age_s" in name:
        return 0.1 + 0.1 * (index % 7)
    return 1.0 + 0.01 * (index % 11) + 0.02 * math.cos(phase + len(name))


def _observations(
    count: int = 112,
    *,
    gap_index: int | None = None,
    missing_channel_index: int | None = None,
    delayed_ready_index: int | None = None,
) -> list[feature_engine.CausalWindowObservation]:
    names = tuple(spec.name for spec in feature_engine.CHANNELS_BY_BLOCK["M2"])
    rows = []
    for index in range(count):
        right = BASE_NS + (index + 1) * WIDTH_NS
        values: dict[str, float | None] = {
            name: _channel_value(name, index) for name in names
        }
        if index == missing_channel_index:
            values["signed_flow_imbalance"] = None
        ready_delay = 50_000_000 if index == delayed_ready_index else 0
        rows.append(
            feature_engine.CausalWindowObservation(
                left_ts_ns=right - WIDTH_NS,
                right_ts_ns=right,
                feature_ready_ts_ns=right + ready_delay,
                market_generation=index + 1,
                depth_generation=index + 1,
                values=values,
                source_gap=index == gap_index,
                warmup_admitted=index >= 4,
            )
        )
    return rows


def _project(
    observation: feature_engine.CausalWindowObservation,
    block: str,
) -> feature_engine.CausalWindowObservation:
    names = tuple(spec.name for spec in feature_engine.CHANNELS_BY_BLOCK[block])
    return feature_engine.CausalWindowObservation(
        left_ts_ns=observation.left_ts_ns,
        right_ts_ns=observation.right_ts_ns,
        feature_ready_ts_ns=observation.feature_ready_ts_ns,
        market_generation=observation.market_generation,
        depth_generation=observation.depth_generation,
        values={name: observation.values[name] for name in names},
        source_gap=observation.source_gap,
        source_stale=observation.source_stale,
        warmup_admitted=observation.warmup_admitted,
    )


def _assert_mapping_equivalent(
    actual: Mapping[str, object],
    expected: Mapping[str, object],
    *,
    atol: float = 3e-9,
) -> None:
    assert set(actual) == set(expected)
    for name, left in actual.items():
        right = expected[name]
        if isinstance(left, float) or isinstance(right, float):
            if left is None or right is None:
                assert left is right, name
            else:
                np.testing.assert_allclose(
                    float(left),
                    float(right),
                    rtol=3e-12,
                    atol=atol,
                    err_msg=name,
                )
        else:
            assert left == right, name


def _compare_at_cutoffs(
    observations: Sequence[feature_engine.CausalWindowObservation],
    cutoffs: Sequence[tuple[int, str]],
) -> tuple[batch.BatchUpdateAudit, list[dict[str, dict[str, object]]]]:
    batched = batch.BatchCausalMultichannelEmaState(
        block="M2",
        warmup_identity="synthetic-warmup",
    )
    scalar = {
        block: feature_engine.CausalMultichannelEmaState(block=block)
        for block in ("R0", "M1", "M2")
    }
    cursor = 0
    output = []
    for cutoff_ns, side in cutoffs:
        pending = []
        while (
            cursor < len(observations)
            and observations[cursor].feature_ready_ts_ns <= cutoff_ns
        ):
            observation = observations[cursor]
            pending.append(observation)
            for block, state in scalar.items():
                state.update(_project(observation, block))
                if observation.warmup_admitted:
                    state.warmup_admitted = True
                    state.warmup_identity = "synthetic-warmup"
            cursor += 1
        batched.update_many(tuple(pending))
        assert batched.last_right_ts_ns is not None
        if cutoff_ns - batched.last_right_ts_ns >= WIDTH_NS:
            batched.mark_current_window_unobserved()
            for state in scalar.values():
                state.mark_current_window_unobserved()

        cutoff_rows: dict[str, dict[str, object]] = {}
        for block, state in scalar.items():
            actual = batched.market_feature_row(
                block=block,
                side=side,
                decision_ts_ns=cutoff_ns,
            )
            expected = panel._market_feature_row(
                state,
                side=side,
                decision_ts_ns=cutoff_ns,
            )
            _assert_mapping_equivalent(actual, expected)
            cutoff_rows[block] = actual
        output.append(cutoff_rows)
    return batched.cumulative_audit(), output


def test_regular_100ms_batch_matches_scalar_for_r0_m1_m2_buy_sell_and_crosses() -> None:
    observations = _observations()
    audit, rows = _compare_at_cutoffs(
        observations,
        (
            (BASE_NS + 18 * WIDTH_NS, "BUY"),
            (BASE_NS + 47 * WIDTH_NS, "SELL"),
            (BASE_NS + 76 * WIDTH_NS, "BUY"),
            (BASE_NS + 112 * WIDTH_NS, "SELL"),
        ),
    )

    assert audit.window_count == len(observations)
    assert audit.channel_update_count == len(observations) * len(
        feature_engine.CHANNELS_BY_BLOCK["M2"]
    )
    assert audit.numpy_vectorized_step_count > 0
    assert audit.scalar_boundary_update_count == 0
    tri_values = [
        value
        for row in rows
        for name, value in row["M2"].items()
        if name.endswith("::last_cross_positive")
    ]
    assert any(value != int(feature_engine.TriState.UNOBSERVED) for value in tri_values)


def test_gap_and_missing_channel_are_strictly_equivalent_and_unobserved() -> None:
    observations = _observations(
        count=30,
        gap_index=12,
        missing_channel_index=20,
    )
    audit, rows = _compare_at_cutoffs(
        observations,
        (
            (BASE_NS + 13 * WIDTH_NS, "BUY"),
            (BASE_NS + 14 * WIDTH_NS, "SELL"),
            (BASE_NS + 21 * WIDTH_NS, "BUY"),
            (BASE_NS + 30 * WIDTH_NS, "SELL"),
        ),
    )

    assert audit.scalar_boundary_update_count > 0
    gap_row = rows[0]["M2"]
    assert gap_row["gap_window_count"] == 1
    assert gap_row["channel_support_valid"] is False
    assert gap_row["channel::mid_usdc_per_btc::observed"] == 0
    assert gap_row["value::mid_usdc_per_btc::ema::h1s"] is None
    missing_row = rows[2]["M2"]
    assert missing_row["channel::signed_flow_imbalance::observed"] == 0
    assert missing_row[
        "tri::signed_flow_imbalance__h0p5s__h1s::positive_ordering"
    ] == -1
    assert missing_row["channel::mid_usdc_per_btc::observed"] == 1


def test_feature_ready_cutoff_excludes_not_yet_ready_completed_window() -> None:
    observations = _observations(count=14, delayed_ready_index=9)
    _, rows = _compare_at_cutoffs(
        observations,
        ((BASE_NS + 10 * WIDTH_NS + 25_000_000, "BUY"),),
    )

    row = rows[0]["R0"]
    assert row["last_window_right_ts_ns"] == BASE_NS + 9 * WIDTH_NS
    assert row["feature_ready_ts_ns"] == BASE_NS + 9 * WIDTH_NS
    assert row["channel_support_valid"] is False
    assert row["tri::mid_usdc_per_btc__h0p5s__h1s::positive_ordering"] == -1


def test_irregular_or_omitted_window_fails_closed() -> None:
    observations = _observations(count=8)
    row = observations[4]
    observations[4] = feature_engine.CausalWindowObservation(
        left_ts_ns=row.left_ts_ns + 1,
        right_ts_ns=row.right_ts_ns,
        feature_ready_ts_ns=row.feature_ready_ts_ns,
        market_generation=row.market_generation,
        depth_generation=row.depth_generation,
        values=row.values,
        warmup_admitted=row.warmup_admitted,
    )
    state = batch.BatchCausalMultichannelEmaState(
        block="M2", warmup_identity="synthetic-warmup"
    )

    with pytest.raises(feature_engine.FeatureContractError, match="window width drifted"):
        state.update_many(observations)


def _census() -> pd.DataFrame:
    rows = []
    for ordinal, (window_index, side, role, before, after, units) in enumerate(
        (
            (18, "BUY", "opener", 0.0, 0.001, 1.0),
            (76, "SELL", "add", -0.001, -0.002, 2.0),
        ),
        start=1,
    ):
        visible_ns = BASE_NS + window_index * WIDTH_NS
        visible_ms = visible_ns // 1_000_000
        rows.append(
            {
                "schema_version": (
                    "multiscale_ema_boolean_cooldown_duration_opportunity.v1"
                ),
                "fill_clock_semantics": (
                    "native_exchange_event_revealed_at_replay_event_clock_"
                    "no_live_receive_time_claim"
                ),
                "live_receive_time_authority": False,
                "exposure_fill_ordinal": ordinal,
                "fill_visible_ts_ms": visible_ms,
                "fill_exchange_ts_ms": visible_ms,
                "side": side,
                "role_at_fill": role,
                "order_id": ordinal,
                "campaign_id": ordinal,
                "inventory_before_fill_btc": before,
                "inventory_after_fill_btc": after,
                "fill_qty_btc": 0.001,
                "unit_qty_btc": 0.001,
                "consecutive_units_before": units - 1.0,
                "consecutive_units_after": units,
                "prior_deadline_ts_ms": visible_ms - 1_000,
                "baseline_duration_ms": 85_000.0 * units,
                "baseline_deadline_ts_ms": visible_ms + int(85_000 * units),
                "canonical_mid": 60_000.0,
                "best_bid": 59_999.9,
                "best_ask": 60_000.1,
                "decision_visible_bbo_index": ordinal,
                "decision_visible_l2_index": ordinal,
                "market_event_index": ordinal,
                "utc_day": "2026-01-02",
                "campaign_side_id": f"2026-01-02:{ordinal}:{side}",
                "assignment_ts_ns": visible_ns,
                "opportunity_id": f"opportunity-{ordinal}",
                "source_profile": "native_formal_lifecycle",
                "formal_lifecycle_replay_eligible": True,
                "exact_queue_policy_eligible": False,
                "queue_path_semantics": (
                    "native_l2_exact_level_replay_model_without_"
                    "exchange_queue_authority"
                ),
            }
        )
        rows[-1].update(
            {name: True for name in panel.IMMUTABLE_R0_PREDICATE_COLUMNS}
        )
        rows[-1].update(
            {name: float(ordinal) for name in panel.IMMUTABLE_R0_CONTINUOUS_COLUMNS}
        )
    return pd.DataFrame(rows, columns=panel.CENSUS_SAFE_PROJECTION_COLUMNS)


def test_drop_in_frame_builder_matches_reference_columns_and_values() -> None:
    opportunities = _census()
    observations = _observations(count=76)
    expected, expected_audit = panel.build_feature_frames(
        opportunities,
        m1_observations=iter(_project(row, "M1") for row in observations),
        m1_warmup_identity="synthetic-m1-warmup",
        m2_observations=iter(observations),
        m2_warmup_identity="synthetic-m2-warmup",
        allow_reduced_m0=True,
        m2_day_supported=True,
    )
    actual, actual_audit = batch.build_feature_frames_batch(
        opportunities,
        m1_observations=iter(_project(row, "M1") for row in observations),
        m1_warmup_identity="synthetic-m1-warmup",
        m2_observations=iter(observations),
        m2_warmup_identity="synthetic-m2-warmup",
        allow_reduced_m0=True,
        m2_day_supported=True,
        max_batch_windows=19,
    )

    assert actual_audit == expected_audit
    for block in panel.FEATURE_BLOCKS:
        assert list(actual[block].columns) == list(expected[block].columns)
        pd.testing.assert_frame_equal(
            actual[block],
            expected[block],
            check_exact=False,
            rtol=3e-12,
            atol=3e-9,
        )


def test_batch_interface_is_outcome_blind() -> None:
    state = batch.BatchCausalMultichannelEmaState(
        block="M1", warmup_identity="synthetic-warmup"
    )
    audit = state.update_many(
        tuple(_project(row, "M1") for row in _observations(count=6))
    )

    assert audit.window_count == 6
    assert audit.channel_update_count == 6 * len(
        feature_engine.CHANNELS_BY_BLOCK["M1"]
    )
    assert not hasattr(audit, "reward")
    assert not hasattr(audit, "terminal_value")
