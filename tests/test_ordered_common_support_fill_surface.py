from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import (
    STATIC_MODEL_FEATURES,
)
from research.families.f06_placement_fill_cif.audit.ordered_common_support_fill_surface import (
    ACTIONS,
    assert_paired_feature_contract,
    common_support_mask,
    fit_ordered_pre_request_model,
)


def _paired_frame(cohorts: int = 120) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    distance = {"closer_1tick": 4.0, "current": 5.0, "farther_1tick": 6.0}
    for cohort in range(cohorts):
        outcome = cohort % 4
        for action_index, action in enumerate(ACTIONS):
            fill = action_index <= outcome - 1 if outcome else False
            row: dict[str, object] = {
                "cohort_id": f"c{cohort}",
                "action_lifecycle_id": f"c{cohort}:{action}",
                "day": "2026-01-01",
                "side": "BUY",
                "inventory_role": ("opener", "add", "reducing")[cohort % 3],
                "action": action,
                "activation_status": "active",
                "pre_request_observed": 1,
                "pre_request_first_fill": int(fill),
                "pre_request_exposure_ms": 1_000.0 if fill else 5_000.0,
            }
            for feature in STATIC_MODEL_FEATURES:
                row[feature] = 0.0
            row["distance_ticks"] = distance[action]
            row["inventory_ratio"] = float((cohort % 5) - 2) / 10.0
            row[f"role_{row['inventory_role']}"] = 1.0
            rows.append(row)
    return pd.DataFrame(rows)


def test_ordered_hazard_has_no_action_escape_and_is_pathwise_monotone() -> None:
    frame = _paired_frame()
    model = fit_ordered_pre_request_model(
        frame,
        maximum_bins=8,
        random_seed=7,
        model_config={
            "minimum_fill_events": 2,
            "n_estimators": 30,
            "learning_rate": 0.08,
            "num_leaves": 5,
            "max_depth": 2,
            "min_child_samples": 10,
            "reg_lambda": 1.0,
        },
    )
    assert model.categorical_features == ("inventory_role",)
    assert not any(name.startswith("action_") for name in model.encoded_columns)

    curve = model.predict_cif(frame)
    final = curve.groupby("row_index", sort=False)["fill_cif"].last().to_numpy()
    probability = pd.DataFrame(
        {
            "cohort_id": frame["cohort_id"],
            "action": frame["action"],
            "probability": final,
        }
    ).pivot(index="cohort_id", columns="action", values="probability")
    assert np.all(probability["closer_1tick"] >= probability["current"] - 1e-12)
    assert np.all(probability["current"] >= probability["farther_1tick"] - 1e-12)


def test_paired_contract_rejects_an_action_varying_escape_feature() -> None:
    frame = _paired_frame(4)
    assert_paired_feature_contract(frame)
    frame.loc[frame["action"].eq("farther_1tick"), "toxicity"] = 1.0
    with pytest.raises(RuntimeError, match="action-varying feature"):
        assert_paired_feature_contract(frame)


def test_common_support_requires_all_three_actions_active() -> None:
    frame = _paired_frame(3)
    frame.loc[
        frame["cohort_id"].eq("c1") & frame["action"].eq("farther_1tick"),
        "activation_status",
    ] = "gtx_reject"
    mask = common_support_mask(frame)
    assert mask[frame["cohort_id"].eq("c0")].all()
    assert not mask[frame["cohort_id"].eq("c1")].any()
