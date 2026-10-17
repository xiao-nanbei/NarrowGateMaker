from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.families.f07_active_order_continuation.audit.queue_value_models import (
    EmpiricalMicropriceArtifact,
    QueueReactiveHawkesArtifact,
    QueueReactiveRuntime,
    QueueValueModelBundle,
    QueueValueStateConfig,
    QueueValueStateEvaluator,
    fit_empirical_microprice,
    fit_queue_reactive_hawkes,
    fit_side_specific_queue_value_bundle,
)


def _training_panel(rows: int = 240) -> pd.DataFrame:
    index = np.arange(rows)
    side = np.where(index % 2 == 0, "BUY", "SELL")
    imbalance = np.where(index % 4 < 2, -0.8, 0.8)
    return pd.DataFrame(
        {
            "day": [f"2026-01-{1 + (value // 24):02d}" for value in index],
            "side": side,
            "interval_end_ts_ns": (index + 1) * 100_000_000,
            "interval_ms": np.full(rows, 100.0),
            "spread_ticks": np.where(index % 3 == 0, 1.0, 2.0),
            "book_imbalance": imbalance,
            "queue_fraction_left": (index % 10) / 10.0,
            "adverse_market_order_count": ((imbalance < 0.0) & (side == "BUY")).astype(float),
            "cancel_count": (index % 7 == 0).astype(float),
            "refill_count": (imbalance > 0.0).astype(float),
            "exchange_book_cancel_count": (index % 7 == 0).astype(float),
            "exchange_book_refill_count": (imbalance > 0.0).astype(float),
            "best_bid": np.full(rows, 99.9),
            "best_ask": np.full(rows, 100.1),
            "bid_qty": np.where(imbalance > 0.0, 10.0, 1.0),
            "ask_qty": np.where(imbalance > 0.0, 1.0, 10.0),
            "future_mid": 100.0 + 0.1 * np.sign(imbalance),
            "future_mid_first_hit_direction": np.sign(imbalance),
            "future_mid_first_hit_horizon_ms": np.full(rows, 250),
            "future_mid_first_hit_censored": np.zeros(rows),
        }
    )


def test_queue_reactive_artifact_round_trip_and_runtime_decay(tmp_path) -> None:
    artifact = fit_queue_reactive_hawkes(_training_panel())
    path = tmp_path / "queue.json"
    artifact.save(path)
    loaded = QueueReactiveHawkesArtifact.load(path)
    runtime = QueueReactiveRuntime(loaded)
    features = {
        "side": "BUY",
        "spread_ticks": 1.0,
        "book_imbalance": -0.8,
        "queue_fraction_left": 0.5,
    }
    runtime.observe(1_000_000_000, adverse_market_order=2.0)
    early = runtime.predict(1_000_000_000, features)
    late = runtime.predict(10_000_000_000, features)

    assert loaded.artifact_id == artifact.artifact_id
    assert (
        early.intensities_per_s["adverse_market_order"]
        >= late.intensities_per_s["adverse_market_order"]
    )
    assert set(early.intensities_per_s) == {
        "adverse_market_order",
        "cancel",
        "refill",
    }


def test_hawkes_half_life_cannot_beat_book_observation_resolution() -> None:
    panel = _training_panel()
    panel["book_state_resolution_ms"] = 1_000.0

    artifact = fit_queue_reactive_hawkes(panel)

    assert artifact.observation_resolution_ms == 1_000.0
    assert all(model.half_life_ms >= 1_000.0 for model in artifact.event_models.values())


def test_empirical_microprice_learns_book_conditioned_direction(tmp_path) -> None:
    artifact = fit_empirical_microprice(
        _training_panel(),
        tick_size=0.1,
        horizon_ms=250,
        min_cell_rows=5,
    )
    positive = artifact.predict(
        best_bid=99.9,
        best_ask=100.1,
        bid_qty=10.0,
        ask_qty=1.0,
    )
    negative = artifact.predict(
        best_bid=99.9,
        best_ask=100.1,
        bid_qty=1.0,
        ask_qty=10.0,
    )
    path = tmp_path / "micro.json"
    artifact.save(path)

    assert positive.expected_mid_delta_ticks > 0.0
    assert negative.expected_mid_delta_ticks < 0.0
    assert EmpiricalMicropriceArtifact.load(path).artifact_id == artifact.artifact_id


def test_first_hit_microprice_uses_hitting_direction() -> None:
    artifact = fit_empirical_microprice(
        _training_panel(),
        tick_size=0.1,
        horizon_ms=250,
        min_cell_rows=5,
        target_type="first_mid_hit",
    )

    assert artifact.target_type == "first_mid_hit"
    assert artifact.max_abs_ticks == 1.0
    assert artifact.predict(
        best_bid=99.9,
        best_ask=100.1,
        bid_qty=10.0,
        ask_qty=1.0,
    ).p_up > 0.5


def test_queue_value_state_uses_hysteresis_not_fixed_seconds() -> None:
    panel = _training_panel()
    queue = fit_queue_reactive_hawkes(panel)
    micro = fit_empirical_microprice(
        panel,
        tick_size=0.1,
        horizon_ms=250,
        min_cell_rows=5,
    )
    evaluator = QueueValueStateEvaluator(
        queue,
        micro,
        config=QueueValueStateConfig(
            entry_expected_ticks=-0.1,
            entry_adverse_probability=0.5,
            entry_flow_ratio=0.0,
            exit_expected_ticks=0.0,
            exit_adverse_probability=0.6,
            exit_flow_ratio=100.0,
        ),
    )
    features = {
        "side": "BUY",
        "spread_ticks": 2.0,
        "book_imbalance": -0.8,
        "queue_fraction_left": 0.5,
        "best_bid": 99.9,
        "best_ask": 100.1,
        "bid_qty": 1.0,
        "ask_qty": 10.0,
    }
    entered = evaluator.evaluate(side="BUY", features=features)
    held = evaluator.evaluate(
        side="BUY",
        features=features,
        was_active=True,
    )

    assert entered.active
    assert held.active
    assert entered.maker_expected_ticks < 0.0


def test_invalid_microprice_bbo_is_rejected() -> None:
    with pytest.raises(ValueError, match="invalid BBO"):
        fit_empirical_microprice(
            _training_panel().assign(best_ask=99.8),
            tick_size=0.1,
            horizon_ms=250,
        )


def test_empirical_microprice_excludes_right_censored_future_mid() -> None:
    panel = _training_panel(60)
    panel["future_mid_horizon_ms"] = 250
    panel["future_mid_censored"] = 0
    panel.loc[panel.index[-10:], "future_mid_censored"] = 1
    panel.loc[panel.index[-10:], "future_mid"] = 10_000.0

    artifact = fit_empirical_microprice(
        panel,
        tick_size=0.1,
        horizon_ms=250,
        min_cell_rows=5,
    )

    assert artifact.training_rows == 50
    assert abs(artifact.global_cell.expected_mid_delta_ticks) < 2.0


def test_side_specific_bundle_round_trip_freezes_calibration_days(
    tmp_path,
) -> None:
    panel = _training_panel(744)
    fit_days = [f"2026-01-{day:02d}" for day in range(1, 19)]
    embargo_days = ["2026-01-19"]
    calibration_days = [f"2026-01-{day:02d}" for day in range(20, 32)]

    bundle, predictions, report = fit_side_specific_queue_value_bundle(
        panel,
        fit_days=fit_days,
        internal_embargo_days=embargo_days,
        calibration_days=calibration_days,
        input_scope="local_only",
        tick_size=0.1,
        horizon_ms=250,
        historical_visibility="exchange_time_diagnostic",
    )
    path = tmp_path / "bundle.json"
    bundle.save(path)
    loaded = QueueValueModelBundle.load(path)

    assert set(loaded.sides) == {"BUY", "SELL"}
    assert loaded.fit_days == tuple(fit_days)
    assert loaded.internal_embargo_days == tuple(embargo_days)
    assert loaded.calibration_days == tuple(calibration_days)
    assert set(predictions["side"]) == {"BUY", "SELL"}
    assert set(report["sides"]) == {"BUY", "SELL"}
    assert loaded.evaluator("BUY").input_scope == "local_only"
    assert loaded.evaluator("SELL").input_scope == "local_only"


def test_calibration_constant_is_frozen_from_fit_not_holdout_rate() -> None:
    panel = _training_panel(744)
    fit_days = [f"2026-01-{day:02d}" for day in range(1, 19)]
    embargo_days = ["2026-01-19"]
    calibration_days = [f"2026-01-{day:02d}" for day in range(20, 32)]
    calibration_mask = panel["day"].isin(calibration_days)
    panel.loc[
        calibration_mask,
        "future_mid_first_hit_direction",
    ] = 0.0

    _, _, report = fit_side_specific_queue_value_bundle(
        panel,
        fit_days=fit_days,
        internal_embargo_days=embargo_days,
        calibration_days=calibration_days,
        input_scope="local_only",
        tick_size=0.1,
        horizon_ms=250,
        historical_visibility="exchange_time_diagnostic",
    )

    buy = report["sides"]["BUY"]["microprice"]
    assert buy["observed_class_rate"]["flat"] == 1.0
    assert buy["frozen_constant_class_rate"]["flat"] < 1.0


def test_side_bundle_filters_unidentified_native_queue_rows() -> None:
    panel = _training_panel(744)
    fit_days = [f"2026-01-{day:02d}" for day in range(1, 19)]
    embargo_days = ["2026-01-19"]
    calibration_days = [f"2026-01-{day:02d}" for day in range(20, 32)]
    panel["simulator_queue_source"] = "native_exchange_book"
    panel["exchange_book_queue_status"] = "exact"
    panel["exchange_book_queue_path_valid"] = 1
    panel["exchange_book_queue_ambiguous"] = 0
    panel.loc[panel.index[:5], "simulator_queue_source"] = (
        "topn_or_fitted_fallback"
    )
    panel.loc[panel.index[5:10], "exchange_book_queue_path_valid"] = 0
    panel.loc[panel.index[5:10], "exchange_book_queue_ambiguous"] = 1
    panel["cancel_count"] = 0.0
    panel["refill_count"] = 0.0

    bundle, _, report = fit_side_specific_queue_value_bundle(
        panel,
        fit_days=fit_days,
        internal_embargo_days=embargo_days,
        calibration_days=calibration_days,
        input_scope="local_only",
        tick_size=0.1,
        horizon_ms=250,
        historical_visibility="native_exchange_time_simulator",
        require_native_support=True,
        minimum_native_support=0.98,
    )

    assert report["native_support"]["excluded_rows"] == 5
    assert report["native_support"]["post_decision_path_loss_rows"] == 5
    assert report["native_support"]["post_decision_path_filter_applied"] is False
    assert report["native_support"]["passed"] is True
    assert report["queue_event_columns"] == {
        "adverse_market_order": "adverse_market_order_count",
        "cancel": "exchange_book_cancel_count",
        "refill": "exchange_book_refill_count",
    }
    assert bundle.side_model("BUY").queue_artifact.event_columns == (
        report["queue_event_columns"]
    )
    assert (
        bundle.side_model("BUY")
        .queue_artifact.event_models["cancel"]
        .global_rate_per_s
        > 0.0
    )
    assert (
        bundle.side_model("BUY")
        .queue_artifact.event_models["refill"]
        .global_rate_per_s
        > 0.0
    )
    assert (
        report["sides"]["BUY"]["hazard"]["cancel"]["observed_total"]
        > 0.0
    )
    assert (
        report["sides"]["BUY"]["hazard"]["refill"]["observed_total"]
        > 0.0
    )
    assert bundle.calibration_passed == report["calibration_passed"]


def test_policy_model_rejects_hidden_simulator_features() -> None:
    panel = _training_panel()
    panel["exchange_book_queue_path_valid"] = 1.0

    with pytest.raises(ValueError, match="simulator-only"):
        fit_queue_reactive_hawkes(
            panel,
            numeric_features=(
                "spread_ticks",
                "book_imbalance",
                "exchange_book_queue_path_valid",
            ),
        )
