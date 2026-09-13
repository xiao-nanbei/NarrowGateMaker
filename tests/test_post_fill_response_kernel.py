import math

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit.post_fill_response_kernel import (
    _hawkes_nll,
    _nonnegative_decay_fit,
    _window_quantity,
    chronological_split,
    fit_adverse_amplitude_model,
)


def test_hawkes_likelihood_prefers_clustered_kernel() -> None:
    events = [np.array([100.0, 100.2, 100.4, 5_000.0, 5_000.1])]
    clustered = _hawkes_nll(np.array([1e-5, 0.5, 1.0]), events, 10_000.0)
    nearly_poisson = _hawkes_nll(np.array([1e-5, 1e-6, 1.0]), events, 10_000.0)
    assert clustered < nearly_poisson


def test_decay_fit_recovers_half_life() -> None:
    horizons = np.array([1.0, 5.0, 20.0, 30.0])
    expected_half_life = 8.0
    values = 2.0 + 6.0 * np.exp(-math.log(2.0) * horizons / expected_half_life)
    fit = _nonnegative_decay_fit(horizons, values)
    assert fit["decay_supported"] is True
    assert abs(float(fit["half_life_s"]) - expected_half_life) < 0.5
    assert abs(float(fit["amplitude_ticks"]) - 6.0) < 0.2


def test_chronological_split_respects_embargo() -> None:
    days = [f"2026-01-{day:02d}" for day in range(1, 11)]
    split = chronological_split(days, train_days=4, validation_days=3, embargo_days=1)
    assert split["train"] == days[:4]
    assert split["embargo_after_train"] == days[4:5]
    assert split["validation"] == days[5:8]
    assert split["embargo_after_validation"] == days[8:9]
    assert split["late"] == days[9:]


def test_window_quantity_uses_padded_cumulative_sum() -> None:
    timestamps = np.array([1.0, 2.0, 3.0, 5.0])
    quantity = np.array([1.0, 2.0, 3.0, 4.0])
    cumulative = np.concatenate([[0.0], np.cumsum(quantity)])
    observed = _window_quantity(
        timestamps,
        cumulative,
        np.array([1.5, 3.0]),
        np.array([3.0, 5.0]),
    )
    assert np.allclose(observed, np.array([5.0, 7.0]))


def test_adverse_amplitude_coefficients_are_nonnegative() -> None:
    rows = []
    for idx in range(20):
        excitation = 1.0 + idx / 10.0
        rows.append(
            {
                "day": "2026-01-01" if idx < 10 else "2026-01-02",
                "hawkes_excitation_post_fill": excitation,
                "volatility_bps": 2.0,
                "refill_edge": -0.1,
                "repair_probability": 0.4,
                "markout_5s_bps": -(1.0 + excitation),
                "markout_30s_bps": -0.5,
                "fill_price": 100.0,
                "quote_distance": 1.0,
            }
        )
    model = fit_adverse_amplitude_model(
        pd.DataFrame(rows),
        ["2026-01-01", "2026-01-02"],
        tick_size=0.1,
        policy_horizon_s=5,
    )
    assert all(value >= 0.0 for value in model["coefficients_ticks"].values())
    assert model["reference_expected_adverse_ticks"] > 0.0
