from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from research.families.f02_empirical_p3_touch.audit.p3_reach_time_conditioned_hazard import (
    ADMINISTRATIVE_CENSOR_MS,
    FAST_NORMALIZED_DISTANCE_FEATURE,
    FAST_SIGMA_FEATURE,
    RAW_DISTANCE_FEATURE,
    RIGHT_CENSORED_TIME_MS,
    SLOW_NORMALIZED_DISTANCE_FEATURE,
    SLOW_SIGMA_FEATURE,
    TIME_UPPER_FEATURE,
    HazardGridSpec,
    PositiveRateCalibration,
    build_hazard_risk_rows,
    fit_positive_rate_calibration,
    fit_side_hazard_model,
    hazards_to_first_passage,
    lightgbm_monotone_constraints,
    load_side_hazard_artifact,
    sample_distance_queries,
    save_side_hazard_artifact,
)


def _sample(*, side: str = "BUY", samples_per_origin: int = 3):
    return sample_distance_queries(
        origin_ids=np.array([1_000, 2_000], dtype=np.int64),
        distance_ticks=np.array([1, 2, 3, 4], dtype=np.int64),
        samples_per_origin=samples_per_origin,
        side=side,
        seed=20260804,
    )


def test_formal_grid_is_100ms_to_30s_only() -> None:
    grid = HazardGridSpec()
    assert grid.n_time_bins == 300
    assert grid.time_upper_ms()[[0, -1]].tolist() == [100, 30_000]
    with pytest.raises(ValueError, match="100 ms"):
        HazardGridSpec(time_step_ms=200)
    with pytest.raises(ValueError, match="30 second"):
        HazardGridSpec(max_horizon_ms=10_000)


def test_distance_sampling_is_deterministic_outcome_blind_and_ip_weighted() -> None:
    first = _sample(samples_per_origin=2)
    second = _sample(samples_per_origin=2)

    assert first.sampling_identity_sha256 == second.sampling_identity_sha256
    assert np.array_equal(first.origin_index, second.origin_index)
    assert np.array_equal(first.distance_index, second.distance_index)
    assert np.all(first.inclusion_probability == 0.5)
    assert np.all(first.inverse_probability_weight == 2.0)
    for origin in range(2):
        selected = first.distance_index[first.origin_index == origin]
        assert len(selected) == len(np.unique(selected)) == 2

    # Side is part of the frozen query identity, while outcomes are not an input.
    sell = _sample(side="SELL", samples_per_origin=2)
    assert sell.sampling_identity_sha256 != first.sampling_identity_sha256

    with pytest.raises(TypeError, match="integer dtype"):
        sample_distance_queries(
            origin_ids=[1, 2],
            distance_ticks=[1.0, 2.0],
            samples_per_origin=1,
            side="BUY",
            seed=1,
        )
    with pytest.raises(TypeError, match="must be an integer"):
        sample_distance_queries(
            origin_ids=[1, 2],
            distance_ticks=[1, 2],
            samples_per_origin=1.5,
            side="BUY",
            seed=1,
        )


def test_risk_rows_use_first_reach_upper_endpoint_and_explicit_censoring() -> None:
    queries = _sample(samples_per_origin=4)
    endpoints = np.array(
        [
            [100, 300, RIGHT_CENSORED_TIME_MS, 30_000],
            [200, RIGHT_CENSORED_TIME_MS, 400, 500],
        ],
        dtype=np.int32,
    )
    rows = build_hazard_risk_rows(
        first_reach_upper_ms=endpoints,
        queries=queries,
        context_features={
            FAST_SIGMA_FEATURE: np.array([1.0, 2.0]),
            SLOW_SIGMA_FEATURE: np.array([2.0, 4.0]),
            "spread_ticks": np.array([2.0, 3.0]),
        },
        context_feature_names=(FAST_SIGMA_FEATURE, SLOW_SIGMA_FEATURE, "spread_ticks"),
        tick_size=0.1,
        origin_weight=np.array([1.0, 2.0]),
    )

    assert rows.feature_names == (
        RAW_DISTANCE_FEATURE,
        TIME_UPPER_FEATURE,
        FAST_NORMALIZED_DISTANCE_FEATURE,
        SLOW_NORMALIZED_DISTANCE_FEATURE,
        FAST_SIGMA_FEATURE,
        SLOW_SIGMA_FEATURE,
        "spread_ticks",
    )
    for query_index in range(rows.query_count):
        mask = rows.query_index == query_index
        endpoint = int(rows.first_reach_upper_ms[mask][0])
        labels = rows.labels[mask]
        if endpoint == RIGHT_CENSORED_TIME_MS:
            assert len(labels) == 300
            assert not np.any(labels)
            assert np.all(rows.right_censored[mask])
            assert rows.time_upper_ms[mask][-1] == ADMINISTRATIVE_CENSOR_MS
        else:
            assert len(labels) == endpoint // 100
            assert labels[-1] == 1
            assert int(np.sum(labels)) == 1
            assert not np.any(rows.right_censored[mask])
            assert rows.time_upper_ms[mask][-1] == endpoint
    assert np.all(rows.query_inclusion_probability == 1.0)
    assert np.all(rows.query_inverse_probability_weight == 1.0)
    assert np.all(rows.sample_weight[rows.origin_index == 0] == 1.0)
    assert np.all(rows.sample_weight[rows.origin_index == 1] == 2.0)
    first = 0
    distance = rows.matrix[first, 0]
    time_s = rows.matrix[first, 1] / 1_000.0
    assert rows.matrix[first, 2] == pytest.approx(distance / np.sqrt(time_s))
    assert rows.matrix[first, 3] == pytest.approx(distance / (2.0 * np.sqrt(time_s)))


def test_risk_rows_apply_horvitz_thompson_weight_and_reject_bad_endpoint() -> None:
    queries = _sample(samples_per_origin=2)
    endpoints = np.full((2, 4), RIGHT_CENSORED_TIME_MS, dtype=np.int32)
    endpoints[0, queries.distance_index[0]] = 200
    rows = build_hazard_risk_rows(
        first_reach_upper_ms=endpoints,
        queries=queries,
        context_features={
            FAST_SIGMA_FEATURE: np.ones(2),
            SLOW_SIGMA_FEATURE: np.ones(2),
        },
        context_feature_names=(FAST_SIGMA_FEATURE, SLOW_SIGMA_FEATURE),
        tick_size=0.1,
    )
    assert np.all(rows.sample_weight == 2.0)

    endpoints[0, queries.distance_index[0]] = 250
    with pytest.raises(ValueError, match="invalid first-reach"):
        build_hazard_risk_rows(
            first_reach_upper_ms=endpoints,
            queries=queries,
            context_features={
                FAST_SIGMA_FEATURE: np.ones(2),
                SLOW_SIGMA_FEATURE: np.ones(2),
            },
            context_feature_names=(FAST_SIGMA_FEATURE, SLOW_SIGMA_FEATURE),
            tick_size=0.1,
        )


@pytest.mark.parametrize(
    "feature",
    (
        "source_profile",
        "calendar_year",
        "queue_ahead",
        "fill_count",
        "order_lifecycle",
        "terminal_pnl",
    ),
)
def test_nontradable_and_economic_features_fail_closed(feature: str) -> None:
    with pytest.raises(ValueError, match="forbidden"):
        lightgbm_monotone_constraints(
            (
                RAW_DISTANCE_FEATURE,
                TIME_UPPER_FEATURE,
                FAST_NORMALIZED_DISTANCE_FEATURE,
                SLOW_NORMALIZED_DISTANCE_FEATURE,
                feature,
            )
        )


def test_lightgbm_constraint_is_only_negative_on_raw_distance() -> None:
    names = (
        RAW_DISTANCE_FEATURE,
        TIME_UPPER_FEATURE,
        FAST_NORMALIZED_DISTANCE_FEATURE,
        SLOW_NORMALIZED_DISTANCE_FEATURE,
        FAST_SIGMA_FEATURE,
        SLOW_SIGMA_FEATURE,
        "spread_ticks",
    )
    assert lightgbm_monotone_constraints(names) == (-1, 0, -1, -1, 0, 0, 0)


def test_positive_rate_calibration_preserves_ordering() -> None:
    raw = np.array([0.001, 0.01, 0.1, 0.5], dtype=np.float64)
    calibration = PositiveRateCalibration(log_scale=0.4, log_power=-0.2)
    calibrated = calibration.apply(raw)
    assert np.all((calibrated > 0.0) & (calibrated < 1.0))
    assert np.all(np.diff(calibrated) > 0.0)

    rng = np.random.default_rng(13)
    fit_raw = rng.uniform(0.001, 0.3, size=2_000)
    labels = rng.binomial(1, np.minimum(0.95, 1.7 * fit_raw))
    fitted = fit_positive_rate_calibration(fit_raw, labels)
    assert fitted.scale > 0.0
    assert fitted.power > 0.0
    assert np.all(np.diff(fitted.apply(raw)) > 0.0)

    extreme = PositiveRateCalibration(log_scale=20.0, log_power=5.0).apply(raw)
    assert np.all(np.isfinite(extreme))
    assert np.all((extreme > 0.0) & (extreme < 1.0))


def test_nonmonotone_hazard_still_yields_monotone_cdf_and_conserves_mass() -> None:
    pattern = np.array([0.02, 0.30, 0.01, 0.15, 0.04], dtype=np.float64)
    hazards = np.resize(pattern, (2, 300))
    hazards[1] *= 0.5
    distribution = hazards_to_first_passage(hazards)

    assert np.any(np.diff(hazards[0]) < 0.0)
    assert np.all(np.diff(distribution.cdf, axis=1) >= -1e-15)
    assert distribution.max_terminal_mass_error < 5e-13
    assert np.allclose(
        distribution.event_mass.sum(axis=1) + distribution.right_censor_mass,
        1.0,
        rtol=0.0,
        atol=5e-13,
    )


def _synthetic_rows(*, side: str, seed: int):
    rng = np.random.default_rng(seed)
    origin_count = 24
    distances = np.array([1, 2, 3, 4, 5], dtype=np.int64)
    queries = sample_distance_queries(
        origin_ids=np.arange(origin_count, dtype=np.int64) + seed * 10_000,
        distance_ticks=distances,
        samples_per_origin=len(distances),
        side=side,
        seed=seed,
    )
    endpoints = np.full((origin_count, len(distances)), RIGHT_CENSORED_TIME_MS, dtype=np.int32)
    for origin in range(origin_count):
        for distance_index, distance in enumerate(distances):
            probability = 0.75 * np.exp(-0.45 * distance)
            if rng.random() < probability:
                event_bin = int(rng.integers(1, 31))
                endpoints[origin, distance_index] = event_bin * 100
    return build_hazard_risk_rows(
        first_reach_upper_ms=endpoints,
        queries=queries,
        context_features={
            FAST_SIGMA_FEATURE: rng.uniform(0.5, 2.0, size=origin_count),
            SLOW_SIGMA_FEATURE: rng.uniform(1.0, 3.0, size=origin_count),
            "spread_ticks": rng.uniform(1.0, 5.0, size=origin_count),
        },
        context_feature_names=(FAST_SIGMA_FEATURE, SLOW_SIGMA_FEATURE, "spread_ticks"),
        tick_size=0.1,
    )


def test_side_model_curve_is_distance_monotone_and_artifact_is_hash_bound(
    tmp_path: Path,
) -> None:
    train = _synthetic_rows(side="BUY", seed=7)
    calibration = _synthetic_rows(side="BUY", seed=11)
    model = fit_side_hazard_model(
        train,
        calibration,
        lightgbm_parameters={
            "num_threads": 1,
            "num_leaves": 7,
            "min_data_in_leaf": 20,
            "learning_rate": 0.1,
            "monotone_constraints_method": "advanced",
        },
        num_boost_round=12,
    )
    curve = model.predict_curve(
        context={FAST_SIGMA_FEATURE: 1.0, SLOW_SIGMA_FEATURE: 2.0, "spread_ticks": 2.5},
        distances_usdc_per_btc=np.array([0.1, 0.2, 0.3, 0.4, 0.5]),
    )
    assert np.all(np.diff(curve.hazards, axis=0) <= 1e-12)
    assert np.all(np.diff(curve.cdf, axis=0) <= 1e-11)

    artifact_dir = tmp_path / "buy_hazard"
    hashes = save_side_hazard_artifact(model, artifact_dir)
    loaded = load_side_hazard_artifact(
        artifact_dir,
        expected_side="BUY",
        expected_feature_names=train.feature_names,
        expected_artifact_identity_sha256=hashes.artifact_identity_sha256,
    )
    loaded_curve = loaded.predict_curve(
        context={FAST_SIGMA_FEATURE: 1.0, SLOW_SIGMA_FEATURE: 2.0, "spread_ticks": 2.5},
        distances_usdc_per_btc=np.array([0.1, 0.3, 0.5]),
    )
    assert loaded.artifact_identity_sha256 == hashes.artifact_identity_sha256
    assert np.allclose(
        loaded_curve.hazards,
        model.predict_curve(
            context={
                FAST_SIGMA_FEATURE: 1.0,
                SLOW_SIGMA_FEATURE: 2.0,
                "spread_ticks": 2.5,
            },
            distances_usdc_per_btc=np.array([0.1, 0.3, 0.5]),
        ).hazards,
        rtol=0.0,
        atol=1e-15,
    )

    with pytest.raises(ValueError, match="side mismatch"):
        load_side_hazard_artifact(
            artifact_dir,
            expected_side="SELL",
            expected_feature_names=train.feature_names,
        )
    with pytest.raises(ValueError, match="feature schema mismatch"):
        load_side_hazard_artifact(
            artifact_dir,
            expected_side="BUY",
            expected_feature_names=(*train.feature_names, "slow_sigma"),
        )

    metadata_path = artifact_dir / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["side"] = "SELL"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match="canonical metadata hash"):
        load_side_hazard_artifact(
            artifact_dir,
            expected_side="BUY",
            expected_feature_names=train.feature_names,
        )


def test_artifacts_are_side_specific() -> None:
    buy = _synthetic_rows(side="BUY", seed=3)
    sell = _synthetic_rows(side="SELL", seed=3)
    assert buy.side == "BUY"
    assert sell.side == "SELL"
    with pytest.raises(ValueError, match="one side"):
        fit_side_hazard_model(buy, sell, num_boost_round=1)
