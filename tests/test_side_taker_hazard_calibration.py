from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.families.f07_active_order_continuation.audit.queue_value_competing_risk import DEFAULT_FEATURES as LOCAL_FEATURES
from research.families.f08_side_taker_lifecycle.audit.side_taker_hazard_calibration import (
    DEFAULT_FEATURES,
    _complete_primary_daily_effect,
    audit_label_lineage,
    build_dataset_manifest,
    make_chronological_folds,
    run_chronological_calibration,
)


def _synthetic_panel() -> pd.DataFrame:
    rng = np.random.default_rng(23)
    rows: list[dict[str, object]] = []
    start = pd.Timestamp("2026-01-01", tz="UTC")
    decision_id = 0
    for day_index in range(14):
        day = (start + pd.Timedelta(days=day_index)).strftime("%Y-%m-%d")
        for row_index in range(180):
            side = "BUY" if row_index % 2 == 0 else "SELL"
            pressure = float(rng.uniform(-1.0, 1.0))
            direction = 1.0 if side == "BUY" else -1.0
            adverse = 0.04 + 0.31 / (1.0 + np.exp(-4.0 * direction * pressure))
            favorable = 0.04 + 0.23 / (1.0 + np.exp(4.0 * direction * pressure))
            cancel = 0.24
            repair = 0.06
            jump = max(0.01, 1.0 - adverse - favorable - cancel - repair)
            probabilities = np.asarray(
                [favorable, adverse, cancel, jump, repair], dtype=float
            )
            probabilities /= probabilities.sum()
            event = rng.choice(
                [
                    "favorable_fill",
                    "adverse_fill",
                    "cancel",
                    "adverse_price_jump",
                    "campaign_repair",
                ],
                p=probabilities,
            )
            decision_ts_ns = int(
                (start + pd.Timedelta(days=day_index, seconds=row_index)).timestamp()
                * 1_000_000_000
            )
            row: dict[str, object] = {
                "day": day,
                "side": side,
                "decision_id": f"d{decision_id}",
                "decision_ts_ns": decision_ts_ns,
                "taker_feature_ready_ts_ns": decision_ts_ns,
                "taker_feature_available": 1,
                "taker_policy_eligible": 0,
                "event_time_ms": 1_000.0,
                "first_event": event,
                "exchange_book_refill_count": float(
                    rng.poisson(0.25 + 0.15 * (1.0 - direction * pressure))
                ),
            }
            for feature in LOCAL_FEATURES:
                row[feature] = float(rng.normal())
            for feature in DEFAULT_FEATURES:
                row.setdefault(feature, float(rng.normal()))
            row["net_counterparty_pressure_100ms"] = pressure
            row["net_counterparty_pressure_500ms"] = pressure
            rows.append(row)
            decision_id += 1
    return pd.DataFrame(rows)


def test_chronological_folds_keep_embargo_and_forward_order() -> None:
    days = [f"2026-01-{value:02d}" for value in range(1, 15)]
    folds = make_chronological_folds(
        days,
        min_train_days=6,
        embargo_days=1,
        test_days=2,
    )

    assert len(folds) == 4
    for fold in folds:
        assert max(fold.train_days) < min(fold.embargo_days) < min(fold.test_days)
    test_days = [day for fold in folds for day in fold.test_days]
    assert len(test_days) == len(set(test_days))


def test_split_hazard_improves_side_dependent_synthetic_signal() -> None:
    predictions, report = run_chronological_calibration(
        _synthetic_panel(),
        min_train_days=6,
        embargo_days=1,
        test_days=2,
        alpha=0.25,
        minimum_primary_events=10,
    )

    assert not predictions.empty
    assert report["new_action_family_created"] is False
    assert report["comparison"]["clean_nested_side_slope_test"] is False
    assert report["estimand_valid_for_side_slope_heterogeneity"] is False
    assert report["estimand_valid_for_action_uplift"] is False
    assert report["predictive_split_gate_any_side"] is False
    assert (
        report["followup_randomized_experiment_registration_eligible"]
        is False
    )
    for side in ("BUY", "SELL"):
        primary = report["sides"][side]["primary_composite"]
        assert primary["support_passed"] is True
        assert primary["split_minus_pooled_balanced_log_loss"] < 0.0
        assert primary["predictive_split_gate_passed"] is False
        assert primary["estimand_valid"] is False
        assert (
            primary["followup_randomized_experiment_registration_eligible"]
            is False
        )
        assert primary["action_family_allowed"] is False


def test_primary_composite_uses_only_days_with_both_fill_heads() -> None:
    effect = _complete_primary_daily_effect(
        {
            "favorable_fill": pd.Series(
                {"2026-01-01": -0.2, "2026-01-02": -0.1}
            ),
            "adverse_fill": pd.Series({"2026-01-01": -0.4}),
        }
    )

    assert list(effect.index) == ["2026-01-01"]
    assert effect.iloc[0] == pytest.approx(-0.3)


def test_label_lineage_does_not_relabel_legacy_jump_as_native() -> None:
    frame = pd.DataFrame(
        {
            "first_event": ["adverse_price_jump"] * 3,
            "decision_ts_ns": [1_000_000, 2_000_000, 3_000_000],
            "first_event_ts_ns": [1_000_000, 4_000_000, 6_000_000],
            "event_time_ms": [0.0, 2.0, 3.0],
            "adverse_price_jump_ts_ns": [1_000_000, 4_000_000, 6_000_000],
            "future_mid_first_hit_ts_ns": [1_500_000, 4_000_000, 0],
            "label_censor_reason": ["", "", ""],
        }
    )

    audit = audit_label_lineage(frame)

    assert audit["jump_first_events"] == 3
    assert audit["legacy_jump_timestamp_matches"] == 3
    assert audit["native_first_hit_available_rows"] == 2
    assert audit["native_first_hit_timestamp_matches"] == 1
    assert audit["native_first_hit_timestamp_mismatches"] == 2
    assert audit["zero_ms_jump_first_events"] == 1
    assert audit["native_first_hit_used_in_competing_risk"] is False
    assert audit["action_independent_competing_risk_estimand"] is False


def test_dataset_manifest_has_one_identity_row_per_day(tmp_path) -> None:
    panel_path = tmp_path / "panel.parquet"
    panel_path.write_bytes(b"frozen-panel")
    frame = _synthetic_panel()

    manifest = build_dataset_manifest(frame, input_path=panel_path)

    assert len(manifest) == frame["day"].nunique()
    assert manifest["day"].is_monotonic_increasing
    assert manifest["rows"].sum() == len(frame)
    assert (manifest["buy_rows"] + manifest["sell_rows"]).equals(
        manifest["rows"]
    )
    assert manifest["source_panel_sha256"].nunique() == 1
