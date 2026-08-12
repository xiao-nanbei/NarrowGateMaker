from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from models.audit.experiment_scorecard import score_profile_contract
from research.families.f09_campaign_action_uplift.audit import (
    sell_add_inventory_price_penalty_randomized_replay as randomized,
)
from research.families.f09_campaign_action_uplift.audit.sell_add_inventory_price_penalty import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
)


def _spec() -> dict[str, object]:
    days = [f"2026-06-{day:02d}" for day in range(1, 13)]
    return {
        "canonical_spec_identity_sha256": "f" * 64,
        "panels": {
            "development_days": days,
            "grade_a_days": days,
            "grade_b_days": [],
        },
        "behavior_policy": {
            "random_seed": 17,
            "probabilities": {
                CONTROL_ACTION: 0.5,
                CANDIDATE_ACTION: 0.5,
            },
        },
        "actions": {
            "candidate": {
                "inventory_unit_btc": 0.001,
                "step_bps_per_short_unit": 0.5,
                "maximum_penalty_bps": 1.5,
            }
        },
        "replay_contract": {"trace_campaigns_max_per_day": 10_000},
        "scorecard_profile": score_profile_contract("action_execution_v1"),
        "bootstrap": {"draws": 200, "seed": 20260801},
        "family_gates": {
            "actual_action_change_rate_lcb_min": 0.05,
            "minimum_sell_add_fill_retention": 0.90,
            "minimum_activity_retention": 0.75,
            "maximum_cap_truncation_rate": 0.20,
            "maximum_full_cap_truncation_rate": 0.10,
            "minimum_realized_to_requested_penalty_ratio": 0.80,
            "multilevel_short_loss_lcb_must_exceed": 0.0,
            "max_inventory_avoidance_lcb_min": 0.0,
        },
    }


def _panel() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for day_index, day in enumerate(_spec()["panels"]["development_days"]):
        for row_index in range(20):
            candidate = row_index % 2 == 1
            action = CANDIDATE_ACTION if candidate else CONTROL_ACTION
            reward = 0.03 if candidate else -0.03
            rows.append(
                {
                    "day": day,
                    "decision_id": f"{day}:{row_index}",
                    "campaign_id": day_index * 100 + row_index,
                    "side": "SELL",
                    "action": action,
                    "behavior_propensity": 0.5,
                    "decision_to_campaign_terminal_value_usdc": reward,
                    "lineage_mae": -0.01 if candidate else -0.04,
                    "lineage_max_abs_inventory": (
                        0.002 if candidate else 0.004
                    ),
                    "inventory_time_btc_s": 0.5 if candidate else 1.0,
                    "repair_event": 1.0 if candidate else 0.5,
                    "campaign_censored": 0.0 if candidate else 1.0,
                    "decision_to_terminal_s": 10.0 if candidate else 100.0,
                    "sell_add_fill_count": 1.0,
                    "intervention_fill_count": 1.0,
                    "actual_final_action_change_count": int(candidate),
                    "candidate_quote_count": 2 if candidate else 0,
                    "cap_truncation_count": 0,
                    "full_cap_truncation_count": 0,
                    "requested_penalty_bps_sum": 1.0 if candidate else 0.0,
                    "realized_penalty_bps_sum": 1.0 if candidate else 0.0,
                    "queue_reset_count": 0,
                    "replace_cancel_request_count": 0,
                    "order_submit_count": 2,
                    "multilevel_short_terminal_value_usdc": reward,
                    "support_valid": 1,
                }
            )
    return pd.DataFrame(rows)


def test_canonical_spec_identity_excludes_only_identity_field() -> None:
    payload = {"schema_version": randomized.SCHEMA_VERSION, "value": 1}
    identity = randomized.canonical_spec_sha256(payload)
    frozen = {**payload, "canonical_spec_identity_sha256": identity}

    assert randomized.canonical_spec_sha256(frozen) == identity
    assert randomized.canonical_spec_sha256({**frozen, "value": 2}) != identity


def test_config_freezes_curve_wall_clock_q90_off_and_python_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(randomized.full_path, "_configure_params", lambda base, day: {})

    params = randomized._configure_params(_spec(), {}, "2026-06-01")

    assert params["fill_cooldown_clock_mode"] == "wall_time"
    assert params["sell_add_inventory_price_penalty_enabled"] is True
    assert params["sell_add_inventory_price_penalty_step_bps"] == pytest.approx(0.5)
    assert params["sell_add_inventory_price_penalty_max_bps"] == pytest.approx(1.5)
    assert params["dynamic_fill_hazard_action_enabled"] is False
    assert params["dynamic_fill_hazard_cpp_parity_enabled"] is False
    assert params["replay_promotion_eligible"] is False


def test_panel_requires_exact_development_denominator_and_half_propensity() -> None:
    panel = _panel()
    randomized.validate_panel(panel, _spec())

    wrong_propensity = panel.copy()
    wrong_propensity.loc[0, "behavior_propensity"] = 0.49
    with pytest.raises(ValueError, match="0.5"):
        randomized.validate_panel(wrong_propensity, _spec())

    missing_day = panel[panel["day"] != panel.iloc[0]["day"]].copy()
    with pytest.raises(ValueError, match="exact Development"):
        randomized.validate_panel(missing_day, _spec())


def test_grade_a_scorecard_requires_economic_and_mechanical_gates() -> None:
    report, evidence, scorecard = randomized.evaluate_scope(
        _panel(),
        scope_id="grade_a_primary",
        spec=_spec(),
        primary=True,
    )

    assert report["actual_action_change"]["lcb95"] == pytest.approx(1.0)
    assert report["sell_add_fill_retention"] == pytest.approx(1.0)
    assert report["cap_diagnostics"]["cap_truncation_rate"] == pytest.approx(0.0)
    assert evidence is not None
    assert scorecard is not None
    assert not evidence["family_gate_failures"]
    assert scorecard["hard_gates"]["passed"]
    assert scorecard["ranking_eligible"]
    assert np.isfinite(scorecard["ranking_score"])


def test_cap_saturation_cannot_be_rescued_by_positive_reward() -> None:
    panel = _panel()
    candidate = panel["action"].eq(CANDIDATE_ACTION)
    panel.loc[candidate, "cap_truncation_count"] = 2
    panel.loc[candidate, "realized_penalty_bps_sum"] = 0.1

    report, evidence, scorecard = randomized.evaluate_scope(
        panel,
        scope_id="grade_a_primary",
        spec=_spec(),
        primary=True,
    )

    assert "cap_truncation_rate_above_gate" in report["family_gate_failures"]
    assert evidence is not None
    assert scorecard is not None
    assert not scorecard["ranking_eligible"]
    assert scorecard["ranking_score"] is None
