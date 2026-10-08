from __future__ import annotations

import numpy as np
import pytest

from strategy.fill_cooldown import (
    LINEAGE_CANDIDATE_ACTION,
    LINEAGE_CONTROL_ACTION,
    CausalVarianceRateStream,
    CausalVarianceSample,
    OnlineVarianceTimeEpisode,
    VarianceTimeEpisodeState,
    choose_lineage_randomized_action,
    integrate_variance_time_episode,
    normalize_lineage_randomization_probabilities,
    price_variance_to_bps2_rate,
    start_online_variance_time_episode,
)
from strategy.signal import Bar1s, SignalEngine


def _samples() -> list[CausalVarianceSample]:
    # mid=100 and sigma^2=0.01 gives exactly 100 bps^2/s.
    return [
        CausalVarianceSample(ts, 100.0, 0.01, True)
        for ts in range(1_000, 12_000, 1_000)
    ]


def test_variance_units_and_budget_crossing() -> None:
    assert price_variance_to_bps2_rate(0.01, 100.0) == pytest.approx(100.0)
    result = integrate_variance_time_episode(
        _samples(),
        episode_start_ts_ms=1_000,
        budget_bps2=250.0,
        minimum_wall_time_ms=1_000,
        maximum_wall_time_ms=10_000,
        max_feature_age_ms=2_000,
    )
    assert result.reason == "variance_budget"
    assert result.rearm_elapsed_ms == pytest.approx(2_500.0)
    assert result.rearm_ts_ms == 3_500


class _FixedDraw:
    def __init__(self, value: float) -> None:
        self.value = value

    def random(self) -> float:
        return self.value


def test_lineage_randomization_is_exact_two_arm_and_pre_path() -> None:
    probabilities = normalize_lineage_randomization_probabilities(None)
    assert probabilities == {
        LINEAGE_CONTROL_ACTION: 0.5,
        LINEAGE_CANDIDATE_ACTION: 0.5,
    }
    assert choose_lineage_randomized_action(
        _FixedDraw(0.499999), probabilities
    ) == (LINEAGE_CONTROL_ACTION, 0.499999)
    assert choose_lineage_randomized_action(
        _FixedDraw(0.5), probabilities
    ) == (LINEAGE_CANDIDATE_ACTION, 0.5)

    with pytest.raises(ValueError, match="exactly"):
        normalize_lineage_randomization_probabilities(
            {LINEAGE_CONTROL_ACTION: 1.0}
        )


def test_stale_samples_freeze_qv_and_maximum_wall_releases() -> None:
    result = integrate_variance_time_episode(
        [CausalVarianceSample(1_000, 100.0, 0.01, True)],
        episode_start_ts_ms=1_000,
        budget_bps2=1_000.0,
        minimum_wall_time_ms=1_000,
        maximum_wall_time_ms=5_000,
        max_feature_age_ms=1_000,
    )
    assert result.reason == "maximum_wall_time"
    assert result.accumulated_qv_bps2 == pytest.approx(100.0)
    assert result.stale_frozen_ms == pytest.approx(4_000.0)


def test_invalid_variance_interval_counts_as_frozen_time() -> None:
    result = integrate_variance_time_episode(
        [CausalVarianceSample(0, 100.0, 0.01, False)],
        episode_start_ts_ms=0,
        budget_bps2=1.0,
        minimum_wall_time_ms=0,
        maximum_wall_time_ms=5_000,
        max_feature_age_ms=10_000,
    )
    assert result.reason == "maximum_wall_time"
    assert result.valid_interval_ms == pytest.approx(0.0)
    assert result.stale_frozen_ms == pytest.approx(5_000.0)


def test_missing_leading_variance_interval_counts_as_frozen_time() -> None:
    result = integrate_variance_time_episode(
        [CausalVarianceSample(2_000, 100.0, 0.01, True)],
        episode_start_ts_ms=0,
        budget_bps2=10_000.0,
        minimum_wall_time_ms=0,
        maximum_wall_time_ms=5_000,
        max_feature_age_ms=10_000,
    )
    assert result.reason == "maximum_wall_time"
    assert result.valid_interval_ms == pytest.approx(3_000.0)
    assert result.stale_frozen_ms == pytest.approx(2_000.0)


def test_empty_variance_stream_counts_entire_episode_as_frozen() -> None:
    result = integrate_variance_time_episode(
        [],
        episode_start_ts_ms=0,
        budget_bps2=1.0,
        minimum_wall_time_ms=0,
        maximum_wall_time_ms=5_000,
        max_feature_age_ms=10_000,
    )
    assert result.reason == "maximum_wall_time"
    assert result.stale_frozen_ms == pytest.approx(5_000.0)


def test_episode_state_snapshot_roundtrip() -> None:
    state = VarianceTimeEpisodeState(
        side="SELL",
        episode_start_ts_ms=10,
        consecutive_same_side_fill_units=1.5,
        budget_bps2=2.0,
        accumulated_qv_bps2=0.7,
        last_feature_ready_ts_ms=20,
        stale_frozen_ms=3.0,
    )
    assert VarianceTimeEpisodeState.restore(state.snapshot()) == state


def test_online_variance_clock_releases_on_the_causal_path() -> None:
    stream = CausalVarianceRateStream(
        [1_000, 2_000, 3_000, 4_000],
        [100.0, 100.0, 100.0, 100.0],
        [True, True, True, True],
    )
    state = start_online_variance_time_episode(
        side="BUY",
        episode_start_ts_ms=1_000,
        baseline_cooldown_ms=85_000.0,
        consecutive_same_side_fill_units=1.0,
        reference_rate_bps2_per_s=250.0 / 85.0,
        minimum_wall_time_ms=1_000,
        maximum_wall_time_ms=10_000,
        max_feature_age_ms=2_000,
    )
    stream.advance(state, 3_499)
    assert state.active
    stream.advance(state, 3_500)
    assert not state.active
    assert state.ready_ts_ms == 3_500
    assert state.release_reason == "variance_budget"
    assert state.baseline_ready_ts_ms == 86_000


def test_online_variance_clock_freezes_stale_time_and_roundtrips() -> None:
    stream = CausalVarianceRateStream([1_000], [100.0], [True])
    state = start_online_variance_time_episode(
        side="SELL",
        episode_start_ts_ms=1_000,
        baseline_cooldown_ms=85_000.0,
        consecutive_same_side_fill_units=2.0,
        reference_rate_bps2_per_s=100.0,
        minimum_wall_time_ms=1_000,
        maximum_wall_time_ms=5_000,
        max_feature_age_ms=1_000,
    )
    stream.advance(state, 6_000)
    assert state.release_reason == "maximum_wall_time"
    assert state.accumulated_qv_bps2 == pytest.approx(100.0)
    assert state.stale_frozen_ms == pytest.approx(4_000.0)
    assert OnlineVarianceTimeEpisode.restore(state.snapshot()) == state


def test_signal_variance_snapshot_is_bucket_end_causal_and_has_no_floor() -> None:
    signal = SignalEngine(enable_ml=False)
    for index in range(61):
        signal._bar_buffer.append(  # noqa: SLF001 - contract-level test
            Bar1s(ts=index * 1_000, close=100.0)
        )
    snapshot = signal.causal_rolling_variance_snapshot()
    assert snapshot.valid
    assert snapshot.feature_ready_ts_ms == 61_000
    assert snapshot.sigma_sq_price_per_s == 0.0
    assert snapshot.sample_count == 60


def test_cpp_variance_time_matches_python() -> None:
    cpp = pytest.importorskip("narrowgate_cpp")
    if not hasattr(cpp, "integrate_variance_time_episode"):
        pytest.skip("installed narrowgate_cpp predates variance-time ABI")
    samples = _samples()
    python_result = integrate_variance_time_episode(
        samples,
        episode_start_ts_ms=1_000,
        budget_bps2=250.0,
        minimum_wall_time_ms=1_000,
        maximum_wall_time_ms=10_000,
        max_feature_age_ms=2_000,
    )
    native = cpp.integrate_variance_time_episode(
        np.asarray([row.feature_ready_ts_ms for row in samples], dtype=np.int64),
        np.asarray([row.mid_price for row in samples], dtype=np.float64),
        np.asarray([row.sigma_sq_price_per_s for row in samples], dtype=np.float64),
        np.asarray([row.valid for row in samples], dtype=np.uint8),
        1_000,
        250.0,
        1_000,
        10_000,
        2_000,
        -1,
    )
    assert native["reason"] == python_result.reason
    assert native["rearm_ts_ms"] == python_result.rearm_ts_ms
    assert native["rearm_elapsed_ms"] == pytest.approx(
        python_result.rearm_elapsed_ms
    )
    assert native["accumulated_qv_bps2"] == pytest.approx(
        python_result.accumulated_qv_bps2
    )
