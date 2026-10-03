from __future__ import annotations

import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

from features.feature_dag import P3_TOUCH_CONDITIONAL_GRAPH
from research.families.f02_empirical_p3_touch.audit.p3_touch_volatility_conditioned import (
    FEATURE_NAMES,
    MONOTONE_CONSTRAINTS,
    ConditionalTouchModel,
    build_model_matrix,
    canonical_sha256,
    deterministic_training_distances,
)
from research.families.f02_empirical_p3_touch.audit.p3_touch_window_context import (
    CONTEXT_FIELDS,
    apply_source_translation,
    extract_window_context,
    fit_source_translation,
)
from research.governance.public_machine_projection import source_document_path


def _write_synthetic_inputs(root: Path, *, include_future_spike: bool) -> tuple[Path, Path]:
    day = "2026-01-01"
    start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1_000)
    seconds = np.arange(0, 72, dtype=np.int64)
    mid = 100_000.0 + 0.2 * seconds
    if include_future_spike:
        mid[seconds == 61] += 10_000.0
    bbo = pd.DataFrame(
        {
            "timestamp": start + seconds * 1_000,
            "best_bid": mid - 0.05,
            "best_ask": mid + 0.05,
        }
    )
    bbo_path = root / f"bbo-{include_future_spike}.parquet"
    bbo.to_parquet(bbo_path, index=False)
    trades = pd.DataFrame(
        {
            "price": [99_990.0, 100_020.0],
            "transact_time": [start + 60_500, start + 60_700],
            "is_buyer_maker": [True, False],
        }
    )
    trade_path = root / "trades.csv"
    trades.to_csv(trade_path, index=False)
    return bbo_path, trade_path


def _one_row_context(**overrides: float) -> dict[str, np.ndarray]:
    values = {
        "start_ts_ms": 1_000,
        "feature_ready_ts_ms": 1_000,
        "best_bid": 99.9,
        "best_ask": 100.1,
        "mid": 100.0,
        "spread": 0.2,
        "fast_variance": 4.0,
        "slow_variance": 1.0,
        "fast_sigma": 2.0,
        "slow_sigma": 1.0,
        "volatility_ratio": 2.0,
        "book_age_ms": 0.0,
        "BUY": 5.0,
        "SELL": 4.0,
    }
    values.update(overrides)
    return {
        field: np.asarray([values[field]])
        for field in CONTEXT_FIELDS
    }


def test_p3_context_future_bbo_does_not_change_prior_window(tmp_path: Path) -> None:
    base_bbo, trades = _write_synthetic_inputs(tmp_path, include_future_spike=False)
    future_bbo, _ = _write_synthetic_inputs(tmp_path, include_future_spike=True)
    base = extract_window_context(
        day="2026-01-01",
        bbo_path=base_bbo,
        trade_path=trades,
        max_bbo_age_ms=5_000,
    )
    changed = extract_window_context(
        day="2026-01-01",
        bbo_path=future_bbo,
        trade_path=trades,
        max_bbo_age_ms=5_000,
    )
    target = int(pd.Timestamp("2026-01-01T00:01:00Z").timestamp() * 1_000)
    left = int(np.flatnonzero(base["start_ts_ms"] == target)[0])
    right = int(np.flatnonzero(changed["start_ts_ms"] == target)[0])
    for field in CONTEXT_FIELDS:
        assert base[field][left] == changed[field][right]
    assert base["feature_ready_ts_ms"][left] <= base["start_ts_ms"][left]


def test_source_translation_uses_bbo_only_and_preserves_reach_semantics() -> None:
    provider = _one_row_context()
    native = _one_row_context(
        best_bid=100.1,
        best_ask=100.4,
        mid=100.25,
        spread=0.3,
        fast_variance=9.0,
        slow_variance=4.0,
        fast_sigma=3.0,
        slow_sigma=2.0,
        volatility_ratio=1.5,
        BUY=5.2,
        SELL=3.7,
    )
    translation = fit_source_translation({"2026-01-01": (native, provider)})
    assert translation["future_touch_outcome_used"] is False
    corrected = apply_source_translation(provider, translation)
    assert np.isclose(corrected["best_bid"][0], native["best_bid"][0])
    assert np.isclose(corrected["best_ask"][0], native["best_ask"][0])
    assert np.isclose(corrected["fast_sigma"][0], native["fast_sigma"][0])
    assert np.isclose(corrected["slow_sigma"][0], native["slow_sigma"][0])
    assert np.isclose(corrected["BUY"][0], native["BUY"][0])
    assert np.isclose(corrected["SELL"][0], native["SELL"][0])


def test_training_distances_are_deterministic_and_stratified() -> None:
    first, first_rows = deterministic_training_distances(
        day="2026-01-01",
        side="BUY",
        n_windows=11,
        samples_per_window=4,
        distance_min=0.5,
        distance_max=120.0,
        seed=20260803,
    )
    second, second_rows = deterministic_training_distances(
        day="2026-01-01",
        side="BUY",
        n_windows=11,
        samples_per_window=4,
        distance_min=0.5,
        distance_max=120.0,
        seed=20260803,
    )
    assert np.array_equal(first, second)
    assert np.array_equal(first_rows, second_rows)
    reshaped = first.reshape(11, 4)
    edges = np.linspace(0.5, 120.0, 5)
    for index in range(4):
        assert np.all(reshaped[:, index] >= edges[index])
        assert np.all(reshaped[:, index] <= edges[index + 1])


def test_conditional_surface_is_monotone_after_shared_positive_calibration() -> None:
    rng = np.random.default_rng(7)
    distances = rng.uniform(0.5, 120.0, size=4_000)
    slow = rng.uniform(0.5, 3.0, size=4_000)
    matrix = np.column_stack(
        (
            distances,
            distances / (slow * np.sqrt(10.0)),
            rng.integers(0, 2, size=4_000),
            rng.uniform(0.1, 1.0, size=4_000),
            rng.uniform(0.01, 0.2, size=4_000),
            np.log(rng.uniform(0.5, 4.0, size=4_000)),
            np.log(slow),
            rng.normal(size=4_000),
            rng.integers(0, 2, size=4_000),
            rng.integers(0, 2, size=4_000),
        )
    ).astype(np.float32)
    probability = 1.0 / (1.0 + np.exp((distances - 20.0) / 8.0))
    labels = rng.binomial(1, probability)
    dataset = lgb.Dataset(matrix, label=labels, feature_name=list(FEATURE_NAMES))
    booster = lgb.train(
        {
            "objective": "binary",
            "verbosity": -1,
            "seed": 7,
            "num_threads": 2,
            "num_leaves": 15,
            "min_data_in_leaf": 30,
            "monotone_constraints": list(MONOTONE_CONSTRAINTS),
            "monotone_constraints_method": "advanced",
            "deterministic": True,
            "force_col_wise": True,
        },
        dataset,
        num_boost_round=25,
    )
    model = ConditionalTouchModel(
        booster,
        {"intercept": 0.2, "slope": 1.3},
        {"horizon_s": 10.0, "calm_upper": 0.75, "shock_lower": 1.5},
    )
    context = _one_row_context()
    grid = np.arange(0.5, 120.1, 0.5)
    matrix_grid = build_model_matrix(
        context,
        side="BUY",
        distances=grid,
        row_indices=np.zeros(len(grid), dtype=np.int64),
    )
    prediction = model.predict_matrix(matrix_grid)
    assert np.all(np.diff(prediction) <= 1e-12)


def test_p3_research_feature_graph_is_valid_and_label_safe() -> None:
    order = P3_TOUCH_CONDITIONAL_GRAPH.validate()
    assert order.index("p3_conditional_feature_vector") < order.index(
        "p3_conditional_touch_probability"
    )
    feature_node = P3_TOUCH_CONDITIONAL_GRAPH.by_name()[
        "p3_conditional_feature_vector"
    ]
    assert "p3_touch_label" not in feature_node.dependencies


def test_frozen_v4_spec_is_canonical_and_grants_no_authority() -> None:
    root = Path(__file__).resolve().parents[1]
    public_spec = root / (
        "research/families/f02_empirical_p3_touch/docs/"
        "p3_touch_volatility_conditioned_v4_spec_20260803.json"
    )
    spec = json.loads(
        source_document_path(public_spec, require_private=True).read_text(encoding="utf-8")
    )
    expected = spec.pop("canonical_spec_identity_sha256")
    assert canonical_sha256(spec) == expected
    assert spec["feature_graph"]["sha256"] == P3_TOUCH_CONDITIONAL_GRAPH.sha256()
    assert spec["permissions"] == {
        "action_authority": False,
        "development_only": True,
        "historical_panels_previously_read": True,
        "independent_confirmation": False,
        "live_authority": False,
        "overwrite_current_v2_artifact": False,
        "prediction_authority": False,
        "quote_mapping_authority": False,
        "sealed_holdout_read": False,
    }
