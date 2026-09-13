import numpy as np
import pandas as pd

from research.families.f06_placement_fill_cif.audit.evaluate_request_state_race import (
    _known_targets,
    _probabilities_from_rates,
)


def test_competing_rate_projection_stays_on_probability_simplex() -> None:
    bundle = {
        "row_index": np.asarray([0, 0, 1], dtype=np.int64),
        "dt_seconds": np.asarray([0.5, 0.5, 1.0]),
        "fill_rate": np.asarray([0.2, 0.4, 0.1]),
        "ack_rate": np.asarray([0.3, 0.1, 0.5]),
    }

    fill, ack, survival = _probabilities_from_rates(
        bundle,
        2,
        np.ones(2),
        np.ones(2),
    )

    assert np.all(fill >= 0.0)
    assert np.all(ack >= 0.0)
    assert np.all(survival >= 0.0)
    np.testing.assert_allclose(fill + ack + survival, 1.0, atol=1e-12)


def test_pending_targets_respect_event_race_and_right_censoring() -> None:
    frame = pd.DataFrame(
        {
            "pending_risk_duration_ms": [50.0, 80.0, 40.0, 120.0],
            "pending_cancel_fill": [1, 0, 0, 0],
            "cancel_ack_observed": [0, 1, 0, 0],
            "first_pending_cancel_fill_ts_ns": [50_000_000, 0, 0, 0],
            "actual_cancel_ack_ts_ns": [0, 80_000_000, 0, 0],
            "pending_right_censored_by_gap": [0, 0, 1, 0],
        }
    )

    known, fill, ack = _known_targets(frame, "pending", 100)

    assert known.tolist() == [True, True, False, True]
    assert fill.tolist() == [1, 0, 0, 0]
    assert ack.tolist() == [0, 1, 0, 0]
