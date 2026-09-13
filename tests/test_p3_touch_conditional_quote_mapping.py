from __future__ import annotations

import numpy as np

from research.families.f02_empirical_p3_touch.audit.p3_touch_conditional_quote_mapping import (
    map_context_curves,
    materialize_day_overlay,
)


class _ExponentialConditionalModel:
    def predict(self, context, *, side, distances, row_indices):
        del context, row_indices
        side_scale = 1.0 if side == "BUY" else 0.8
        return side_scale * np.exp(-0.1 * np.asarray(distances, dtype=float))


class _FlatConditionalModel:
    def predict(self, context, *, side, distances, row_indices):
        del context, side, row_indices
        return np.ones_like(np.asarray(distances, dtype=float))


def _context() -> dict[str, np.ndarray]:
    start = np.asarray([1_722_470_460_000, 1_722_470_470_000], dtype=np.int64)
    return {
        "start_ts_ms": start,
        "feature_ready_ts_ms": start - 1,
        "mid": np.asarray([60_000.0, 60_010.0]),
        "spread": np.asarray([0.1, 0.1]),
        "fast_sigma": np.asarray([2.0, 2.0]),
        "slow_sigma": np.asarray([2.0, 2.0]),
    }


def test_pair_curve_mapping_matches_exponential_optimum_and_slope():
    mapped = map_context_curves(
        model=_ExponentialConditionalModel(),
        context=_context(),
        distance_grid=np.arange(0.5, 20.0 + 0.5, 0.5),
        chunk_windows=1,
    )

    assert mapped["mapping_valid"].tolist() == [1, 1]
    assert mapped["delta_star"].tolist() == [10.0, 10.0]
    np.testing.assert_allclose(mapped["kappa_eff"], 0.1, atol=1e-12)
    assert np.all(
        mapped["p_buy_at_delta_star"] > mapped["p_sell_at_delta_star"]
    )


def test_day_overlay_uses_v2_fallback_outside_valid_context():
    context = _context()
    mapped = map_context_curves(
        model=_ExponentialConditionalModel(),
        context=context,
        distance_grid=np.arange(0.5, 20.0 + 0.5, 0.5),
    )
    overlay = materialize_day_overlay(
        day="2024-08-01",
        mapped_context=mapped,
        context=context,
        fallback_delta_star=14.0,
        fallback_kappa_eff=0.067,
    )

    assert len(overlay["ts_ms"]) == 8640
    assert int(overlay["mapping_valid"].sum()) == 2
    assert np.count_nonzero(overlay["delta_star"] == 14.0) == 8638
    assert np.count_nonzero(overlay["kappa_eff"] == 0.067) == 8638


def test_grid_edge_optimum_is_invalid_for_central_slope_mapping():
    mapped = map_context_curves(
        model=_FlatConditionalModel(),
        context=_context(),
        distance_grid=np.arange(0.5, 20.0 + 0.5, 0.5),
    )

    assert mapped["delta_star"].tolist() == [20.0, 20.0]
    assert mapped["mapping_valid"].tolist() == [0, 0]
