from __future__ import annotations

import copy
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    sell_first_fill_conditional_value as value_model,
)
from research.families.f10_live_replay_attribution.audit import (
    first_opener_decision_to_terminal_contract as lifecycle_contract,
)

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = (
    ROOT
    / "research"
    / "families"
    / "f10_live_replay_attribution"
    / "docs"
    / "sell_first_fill_conditional_value_feasibility_v3_spec_20260730.json"
)


def _spec() -> dict:
    payload = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    payload = copy.deepcopy(payload)
    payload["inference"]["bootstrap_samples"] = 100
    payload["inference"]["minimum_high_risk_rows"] = 10
    payload["inference"]["minimum_high_risk_days"] = 2
    payload["inference"]["minimum_grade_b_transport_oof_days"] = 2
    return payload


def _prepared_frame(spec: dict) -> pd.DataFrame:
    rng = np.random.default_rng(20260730)
    features = tuple(spec["decision_visible_features"]["model_features"])
    panel_days = (
        (
            value_model.PRIMARY_PANEL,
            spec["panels"]["development_primary_grade_a_days"],
            "A",
        ),
        (
            value_model.SENSITIVITY_PANEL,
            spec["panels"]["development_sensitivity_grade_b_days"],
            "B",
        ),
    )
    rows = []
    campaign_id = 0
    for panel, days, grade in panel_days:
        for day in days:
            for side in value_model.SIDES:
                for _ in range(20):
                    campaign_id += 1
                    state = rng.normal(size=len(features))
                    target = -0.02 - 0.015 * state[0] + rng.normal(scale=0.002)
                    if side == "BUY":
                        target += 0.004
                    row = {
                        "day": day,
                        "quality_grade": grade,
                        "campaign_id": campaign_id,
                        "decision_id": f"decision-{campaign_id}",
                        "order_id": campaign_id,
                        "side": side,
                        "analysis_panel": panel,
                        lifecycle_contract.PRIMARY_ESTIMAND: target,
                    }
                    row.update(dict(zip(features, state, strict=True)))
                    rows.append(row)
    return pd.DataFrame(rows)


def test_chronological_conditional_value_evaluation_is_side_and_panel_separate() -> None:
    spec = _spec()
    result = value_model.evaluate_prepared_trace(_prepared_frame(spec), spec)

    assert result.report["prediction_supported"]
    assert not result.report["validation_read"]
    assert not result.report["action_experiment_authorized"]
    assert set(result.oof_predictions["analysis_panel"]) == {
        value_model.PRIMARY_PANEL,
        value_model.SENSITIVITY_PANEL,
    }
    assert set(result.oof_predictions["side"]) == {"BUY", "SELL"}
    assert result.oof_predictions["training_panel"].eq(
        value_model.PRIMARY_PANEL
    ).all()
    assert (
        pd.to_datetime(result.oof_predictions["outer_train_max_day"])
        < pd.to_datetime(result.oof_predictions["day"])
    ).all()


def test_outer_folds_preserve_calendar_embargo() -> None:
    days = [f"2026-01-{day:02d}" for day in range(1, 21)]
    folds = value_model._outer_folds(
        days,
        minimum_train_days=10,
        embargo_calendar_days=1,
        maximum_test_block_days=3,
    )

    assert folds
    for train_days, test_days in folds:
        assert pd.Timestamp(max(train_days)) < (
            pd.Timestamp(min(test_days)) - pd.Timedelta(days=1)
        )


def test_minimum_grade_a_oof_day_gate_is_enforced() -> None:
    spec = _spec()
    spec["inference"]["minimum_grade_a_oof_days"] = 99

    with pytest.raises(ValueError, match="OOF day support failed"):
        value_model.evaluate_prepared_trace(_prepared_frame(spec), spec)
