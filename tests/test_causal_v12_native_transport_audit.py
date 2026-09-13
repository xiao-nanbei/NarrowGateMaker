from __future__ import annotations

import numpy as np
import pytest

from research.families.f03_causal_13_head.audit.native_transport_audit import (
    classification_metrics,
    regression_metrics,
)


def test_classification_transport_metrics_recover_calibration() -> None:
    prediction = np.array([0.1, 0.2, 0.8, 0.9], dtype=np.float64)
    target = np.array([0.0, 0.0, 1.0, 1.0], dtype=np.float64)
    result = classification_metrics(
        target,
        prediction,
        np.ones(4, dtype=np.float64),
        0.5,
    )

    assert result["auc"] == 1.0
    assert result["brier_skill"] > 0.0
    assert np.isfinite(result["calibration_intercept_log_odds"])
    assert np.isfinite(result["calibration_slope"])


def test_regression_transport_metrics_preserve_direction() -> None:
    target = np.array([-2.0, -1.0, 1.0, 2.0], dtype=np.float64)
    prediction = target * 0.5
    result = regression_metrics(
        target,
        prediction,
        np.ones(4, dtype=np.float64),
        0.0,
    )

    assert result["spearman_ic"] == 1.0
    assert result["calibration_intercept"] == pytest.approx(0.0)
    assert result["calibration_slope"] == pytest.approx(2.0)
