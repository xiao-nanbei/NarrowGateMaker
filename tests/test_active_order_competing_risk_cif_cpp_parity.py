from __future__ import annotations

import numpy as np
import pytest

from research.families.f07_active_order_continuation.audit.active_order_competing_risk_cif_inference_v1_1 import (
    update_active_order_competing_risk_cif as python_update,
)

cpp = pytest.importorskip("narrowgate_cpp")


def _python_path(
    rates: np.ndarray,
    *,
    initial_last_edge: int = 10,
    initial_survival: float = 1.0,
    initial_cif: np.ndarray | None = None,
):
    edges = np.arange(
        initial_last_edge + 1,
        initial_last_edge + 1 + len(rates),
        dtype=np.int64,
    )
    return python_update(
        edges=edges,
        rates_per_s=rates,
        initial_last_edge=initial_last_edge,
        initial_survival=initial_survival,
        initial_cif=(np.zeros(4, dtype=np.float64) if initial_cif is None else initial_cif),
    )


def _cpp_path(
    rates: np.ndarray,
    *,
    initial_last_edge: int = 10,
    initial_survival: float = 1.0,
    initial_cif: np.ndarray | None = None,
):
    edges = np.arange(
        initial_last_edge + 1,
        initial_last_edge + 1 + len(rates),
        dtype=np.int64,
    )
    return cpp.update_active_order_competing_risk_cif(
        edges,
        np.asarray(rates, dtype=np.float64),
        initial_last_edge,
        initial_survival,
        np.zeros(4, dtype=np.float64) if initial_cif is None else initial_cif,
    )


def test_cpp_matches_python_over_time_varying_rates() -> None:
    rng = np.random.default_rng(20260805)
    rates = rng.lognormal(mean=-1.0, sigma=1.25, size=(300, 4))
    rates[::17] = 0.0

    python_result = _python_path(rates)
    result = _cpp_path(rates)

    np.testing.assert_allclose(
        result["survival_after"],
        python_result["survival_after"],
        rtol=2e-14,
        atol=2e-15,
    )
    np.testing.assert_allclose(
        result["cif_after"],
        python_result["cif_after"],
        rtol=2e-14,
        atol=2e-15,
    )
    assert result["final_last_edge"] == python_result["final_last_edge"]
    assert result["final_survival"] == pytest.approx(
        python_result["final_survival"], rel=2e-14, abs=2e-15
    )
    np.testing.assert_allclose(
        result["final_cif"],
        python_result["final_cif"],
        rtol=2e-14,
        atol=2e-15,
    )
    np.testing.assert_allclose(
        result["survival_after"] + result["cif_after"].sum(axis=1),
        1.0,
        rtol=0.0,
        atol=2e-15,
    )


def test_cpp_checkpoint_resume_matches_single_batch() -> None:
    rng = np.random.default_rng(7)
    rates = rng.uniform(0.0, 3.0, size=(80, 4))
    whole = _cpp_path(rates)

    first = _cpp_path(rates[:31])
    second = _cpp_path(
        rates[31:],
        initial_last_edge=int(first["final_last_edge"]),
        initial_survival=float(first["final_survival"]),
        initial_cif=np.asarray(first["final_cif"]),
    )

    assert second["final_last_edge"] == whole["final_last_edge"]
    assert second["final_survival"] == pytest.approx(whole["final_survival"], abs=2e-15)
    np.testing.assert_allclose(second["final_cif"], whole["final_cif"], rtol=0.0, atol=2e-15)


def test_v1_1_accepts_legal_rates_rejected_by_historical_exact_sum_path() -> None:
    rates = np.asarray([[0.1, 0.2, 0.3, 0.4]], dtype=np.float64)
    python_result = _python_path(rates)
    cpp_result = _cpp_path(rates)

    assert python_result["survival_after"][0] < 1.0
    np.testing.assert_allclose(cpp_result["cif_after"], python_result["cif_after"], atol=2e-15)


@pytest.mark.parametrize(
    ("edges", "rates", "message"),
    [
        (np.asarray([12], dtype=np.int64), np.zeros((1, 4)), "missed"),
        (np.asarray([10], dtype=np.int64), np.zeros((1, 4)), "duplicate"),
        (np.asarray([11], dtype=np.int64), np.asarray([[0.0, -1.0, 0.0, 0.0]]), "non-negative"),
        (np.asarray([11], dtype=np.int64), np.asarray([[0.0, np.nan, 0.0, 0.0]]), "finite"),
    ],
)
def test_cpp_fails_closed_on_invalid_grid_or_rates(
    edges: np.ndarray, rates: np.ndarray, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        cpp.update_active_order_competing_risk_cif(
            edges,
            rates,
            10,
            1.0,
            np.zeros(4, dtype=np.float64),
        )


def test_cpp_rejects_invalid_restored_probability_state() -> None:
    with pytest.raises(ValueError, match="conserve"):
        cpp.update_active_order_competing_risk_cif(
            np.empty(0, dtype=np.int64),
            np.empty((0, 4), dtype=np.float64),
            10,
            0.9,
            np.zeros(4, dtype=np.float64),
        )
