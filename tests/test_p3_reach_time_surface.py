from __future__ import annotations

import numpy as np
import pytest

from features.feature_dag import P3_REACH_TIME_GRAPH
from research.families.f02_empirical_p3_touch.audit.p3_reach_time_surface import (
    INVALID_BINARY_LABEL,
    INVALID_REACH_TICKS,
    INVALID_TIME_MS,
    RIGHT_CENSORED_TIME_MS,
    ReachTimeGridSpec,
    build_cumulative_reach_ticks,
    first_reach_time_upper_ms,
    reach_indicator_at_horizon,
)


def _surface(*, valid_decisions: np.ndarray | None = None):
    spec = ReachTimeGridSpec(
        time_step_ms=100,
        max_horizon_ms=500,
        max_distance_ticks=20,
    )
    surface = build_cumulative_reach_ticks(
        decision_ts_ms=np.array([1_000, 2_000], dtype=np.int64),
        best_bid_ticks=np.array([1_000, 2_000], dtype=np.int64),
        best_ask_ticks=np.array([1_001, 2_001], dtype=np.int64),
        trade_ts_ms=np.array(
            [1_000, 1_001, 1_100, 1_250, 1_500, 2_100],
            dtype=np.int64,
        ),
        trade_price_ticks=np.array(
            [990, 999, 1_004, 997, 1_006, 1_999],
            dtype=np.int64,
        ),
        is_buyer_maker=np.array(
            [True, True, False, True, False, True],
            dtype=bool,
        ),
        valid_decisions=valid_decisions,
        spec=spec,
    )
    return surface, spec


def test_side_correct_strict_future_and_cumulative_paths() -> None:
    surface, _ = _surface()

    # The BUY trade exactly at the origin is excluded.  The 1ms trade reaches
    # one tick, then the 250ms trade extends that reach to three ticks.
    assert surface.buy_cumulative_reach_ticks[0].tolist() == [1, 1, 3, 3, 3]
    # SELL uses only aggressive buys and includes exact right endpoints.
    assert surface.sell_cumulative_reach_ticks[0].tolist() == [3, 3, 3, 3, 5]
    assert np.all(np.diff(surface.buy_cumulative_reach_ticks[0]) >= 0)
    assert np.all(np.diff(surface.sell_cumulative_reach_ticks[0]) >= 0)


def test_first_passage_is_interval_upper_endpoint_and_censors() -> None:
    surface, spec = _surface()

    assert first_reach_time_upper_ms(
        surface.buy_cumulative_reach_ticks,
        distance_ticks=3,
        spec=spec,
    ).tolist() == [300, RIGHT_CENSORED_TIME_MS]
    assert first_reach_time_upper_ms(
        surface.sell_cumulative_reach_ticks,
        distance_ticks=5,
        spec=spec,
    ).tolist() == [500, RIGHT_CENSORED_TIME_MS]


def test_horizon_query_requires_exact_grid_endpoint() -> None:
    surface, spec = _surface()

    assert reach_indicator_at_horizon(
        surface.buy_cumulative_reach_ticks,
        distance_ticks=3,
        horizon_ms=200,
        spec=spec,
    ).tolist() == [0, 0]
    assert reach_indicator_at_horizon(
        surface.buy_cumulative_reach_ticks,
        distance_ticks=3,
        horizon_ms=300,
        spec=spec,
    ).tolist() == [1, 0]
    with pytest.raises(ValueError, match="align"):
        reach_indicator_at_horizon(
            surface.buy_cumulative_reach_ticks,
            distance_ticks=3,
            horizon_ms=250,
            spec=spec,
        )


def test_invalid_bbo_or_explicit_mask_remains_distinct_from_censoring() -> None:
    surface, spec = _surface(valid_decisions=np.array([False, True], dtype=bool))

    assert np.all(surface.buy_cumulative_reach_ticks[0] == INVALID_REACH_TICKS)
    assert first_reach_time_upper_ms(
        surface.buy_cumulative_reach_ticks,
        distance_ticks=1,
        spec=spec,
    ).tolist() == [INVALID_TIME_MS, 100]
    assert reach_indicator_at_horizon(
        surface.buy_cumulative_reach_ticks,
        distance_ticks=1,
        horizon_ms=100,
        spec=spec,
    ).tolist() == [INVALID_BINARY_LABEL, 1]


def test_rejects_unsorted_inputs_and_non_integer_prices() -> None:
    spec = ReachTimeGridSpec(max_horizon_ms=100)
    common = {
        "decision_ts_ms": np.array([1_000], dtype=np.int64),
        "best_bid_ticks": np.array([1_000], dtype=np.int64),
        "best_ask_ticks": np.array([1_001], dtype=np.int64),
        "trade_ts_ms": np.array([1_002, 1_001], dtype=np.int64),
        "trade_price_ticks": np.array([1_000, 1_000], dtype=np.int64),
        "is_buyer_maker": np.array([True, True], dtype=bool),
        "spec": spec,
    }
    with pytest.raises(ValueError, match="non-decreasing"):
        build_cumulative_reach_ticks(**common)

    common["trade_ts_ms"] = np.array([1_001, 1_002], dtype=np.int64)
    common["trade_price_ticks"] = np.array([1_000.0, 1_000.0])
    with pytest.raises(TypeError, match="integer dtype"):
        build_cumulative_reach_ticks(**common)


def test_reach_time_graph_is_valid_and_keeps_labels_out_of_prediction() -> None:
    order = P3_REACH_TIME_GRAPH.validate()
    assert order[-1] == "p3_reach_time_probability_surface"
    prediction = P3_REACH_TIME_GRAPH.by_name()[
        "p3_reach_time_probability_surface"
    ]
    namespaces = {
        P3_REACH_TIME_GRAPH.by_name()[name].namespace
        for name in prediction.dependencies
    }
    assert "label" not in namespaces
    assert P3_REACH_TIME_GRAPH.graph_id == "p3_aggressive_reach_time_surface.v1"
