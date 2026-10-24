from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f06_placement_fill_cif.audit.policy_clock_fill_cif import (
    ack_conditional_hazard,
    ack_interval_hazard_matrix,
    audit_policy_request_parity,
    combine_fill_ack_hazards,
    derive_policy_clock_cif,
    exposure_only_fill_hazard_matrix,
    fill_survival_at_times,
    fit_ack_latency_contract,
    fit_exposure_only_fill_hazard_contract,
    predict_ack_latency_cdf,
)


def _lifecycles() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "cohort_id": ["a", "b", "c", "d"],
            "action_lifecycle_id": ["a:current", "b:current", "c:current", "d:current"],
            "day": ["2026-01-01"] * 4,
            "action": ["current"] * 4,
            "side": ["BUY", "BUY", "SELL", "SELL"],
            "inventory_role": ["opener", "opener", "add", "add"],
            "submit_ts_ns": [1_000_000] * 4,
            "activation_ts_ns": [1_000_000] * 4,
            "cancel_request_ts_ns": [6_000_000] * 4,
            "cancel_ack_ts_ns": [11_000_000, 16_000_000, 11_000_000, 16_000_000],
            "baseline_cancel_request_ts_ns": [6_000_000] * 4,
            "baseline_cancel_ack_ts_ns": [
                11_000_000,
                16_000_000,
                11_000_000,
                16_000_000,
            ],
            "cancel_request_reason": ["requote_replace"] * 4,
            "cancel_request_active_ms": [5.0] * 4,
            "cancel_ack_latency_ms": [5.0, 10.0, 5.0, 10.0],
        }
    )


def test_policy_request_parity_requires_reason_and_ordering() -> None:
    frame = _lifecycles()
    assert audit_policy_request_parity(frame)["passed"] is True
    frame.loc[0, "cancel_request_reason"] = ""
    failed = audit_policy_request_parity(frame)
    assert failed["passed"] is False
    assert failed["missing_request_reason"] == 1
    frame = _lifecycles()
    frame.loc[0, "baseline_cancel_request_ts_ns"] += 1
    failed = audit_policy_request_parity(frame)
    assert failed["passed"] is False
    assert failed["request_timestamp_mismatch_vs_baseline"] == 1
    frame = _lifecycles()
    frame.loc[0, "activation_ts_ns"] = 7_000_000
    diagnostic = audit_policy_request_parity(frame)
    assert diagnostic["passed"] is True
    assert diagnostic["request_before_activation"] == 1
    assert diagnostic["request_before_activation_is_inflight_order_diagnostic"] is True


def test_ack_hazard_is_zero_before_request_and_positive_after() -> None:
    frame = _lifecycles()
    contract = fit_ack_latency_contract(
        frame, bin_ms=1, maximum_latency_ms=100, prior_rows=0.0
    )
    hazard = ack_interval_hazard_matrix(
        contract,
        frame.iloc[[0]].reset_index(drop=True),
        interval_ms=5,
        bins=4,
    )
    assert hazard.shape == (1, 4)
    assert hazard[0, 0] == 0.0
    assert hazard[0, 1] > 0.0


def test_ack_hazard_counts_partial_interval_after_request() -> None:
    frame = _lifecycles().iloc[[0]].reset_index(drop=True)
    contract = fit_ack_latency_contract(
        frame, bin_ms=1, maximum_latency_ms=100, prior_rows=0.0
    )
    before_request = ack_conditional_hazard(
        contract,
        frame,
        np.asarray([0.0]),
        np.asarray([4.9]),
    )
    partial_after_request = ack_conditional_hazard(
        contract,
        frame,
        np.asarray([5.0]),
        np.asarray([10.0]),
    )
    assert before_request[0] == 0.0
    assert partial_after_request[0] > 0.99


def test_fill_and_ack_share_one_probability_simplex() -> None:
    fill, ack = combine_fill_ack_hazards(
        np.asarray([[0.20, 0.10]]),
        np.asarray([[0.50, 0.00]]),
    )
    assert np.all(fill >= 0.0)
    assert np.all(ack >= 0.0)
    assert np.all(fill + ack <= 1.0)
    assert ack[0, 0] > 0.0
    assert ack[0, 1] == 0.0
    assert fill[0, 0] > 0.0


def test_ack_latency_cdf_uses_side_role_cell_and_pooled_baseline() -> None:
    frame = _lifecycles()
    contract = fit_ack_latency_contract(
        frame, bin_ms=1, maximum_latency_ms=100, prior_rows=0.0
    )
    scored = predict_ack_latency_cdf(contract, frame, [5, 10])
    assert len(scored) == 8
    first = scored.loc[
        scored["action_lifecycle_id"].eq("a:current")
        & scored["latency_threshold_ms"].eq(5)
    ].iloc[0]
    assert first["ack_latency_target"] == 1
    assert 0.0 <= first["ack_latency_probability"] <= 1.0
    assert 0.0 <= first["baseline_ack_latency_probability"] <= 1.0


def test_fill_survival_honors_partial_market_state_interval() -> None:
    survival = fill_survival_at_times(
        np.asarray([[0.36, 0.20]]),
        np.asarray([[0.0, 50.0, 100.0]]),
        interval_ms=100,
    )
    assert survival[0, 0] == 1.0
    assert np.isclose(survival[0, 1], np.sqrt(0.64))
    assert np.isclose(survival[0, 2], 0.64)


def test_policy_clock_cif_uses_explicit_ack_time_race() -> None:
    frame = _lifecycles().iloc[[0]].reset_index(drop=True)
    frame["cancel_request_active_ms"] = 5.0
    contract = fit_ack_latency_contract(
        frame, bin_ms=1, maximum_latency_ms=100, prior_rows=0.0
    )
    fill, ack = derive_policy_clock_cif(
        np.zeros((1, 2), dtype=float),
        frame,
        np.asarray([[9.0, 10.0, 20.0]]),
        ack_latency_contract=contract,
        interval_ms=100,
    )
    assert fill[0, 0] == 0.0
    assert ack[0, 0] == 0.0
    assert ack[0, 1] > 0.99
    assert np.all(fill + ack <= 1.0)


def test_exposure_only_baseline_uses_side_role_rate() -> None:
    frame = _lifecycles()
    frame["risk_valid"] = 1
    frame["risk_end_ms"] = [500.0, 500.0, 1000.0, 1000.0]
    frame["event_time_ms"] = [200.0, np.nan, 400.0, np.nan]
    frame["event_observed"] = [1, 0, 1, 0]
    contract = fit_exposure_only_fill_hazard_contract(
        frame,
        interval_ms=100,
        maximum_support_ms=1000,
        prior_intervals=0.0,
    )
    hazard = exposure_only_fill_hazard_matrix(contract, frame, bins=3)
    assert hazard.shape == (4, 3)
    assert np.allclose(hazard[:, 0], hazard[:, 2])
    assert np.all(hazard > 0.0)
    assert not np.isclose(hazard[0, 0], hazard[2, 0])
