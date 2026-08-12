from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from models.audit.experiment_scorecard import score_profile_contract
from research.families.f09_campaign_action_uplift.audit import (
    multi_short_reducing_buy_aggression_randomized_replay as randomized,
)
from research.families.f09_campaign_action_uplift.audit.multi_short_reducing_buy_aggression import (
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
                "trigger_inventory_btc": -0.002,
                "release_inventory_btc": -0.001,
            }
        },
        "replay_contract": {"trace_campaigns_max_per_day": 10_000},
        "scorecard_profile": score_profile_contract("action_defense_v1"),
        "bootstrap": {"draws": 200, "seed": 20260801},
        "family_gates": {
            "actual_action_change_rate_lcb_min": 0.05,
            "multilevel_short_loss_lcb_must_exceed": 0.0,
            "max_inventory_avoidance_lcb_min": 0.0,
            "inventory_time_avoidance_lcb_min": 0.0,
            "campaign_mae_avoidance_lcb_min": 0.0,
            "policy_value_lcb_usdc_per_day_must_exceed": 0.0,
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
                    "intervention_fill_count": 1.0,
                    "reducing_buy_fill_count": 2.0 if candidate else 1.0,
                    "sell_exposure_fill_count": 1.0,
                    "actual_final_action_change_count": int(candidate),
                    "candidate_quote_count": 2 if candidate else 0,
                    "price_action_change_count": 2 if candidate else 0,
                    "defense_pause_observation_count": 1,
                    "defense_override_attempt_count": int(candidate),
                    "defense_override_effective_count": int(candidate),
                    "maker_violation_count": 0,
                    "action_generated_ioc_or_taker_count": 0,
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


def test_config_freezes_action_wall_clock_q90_off_and_python_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(randomized.full_path, "_configure_params", lambda base, day: {})

    params = randomized._configure_params(_spec(), {}, "2026-06-01")

    assert params["fill_cooldown_clock_mode"] == "wall_time"
    assert params["multi_short_reducing_buy_aggression_enabled"] is True
    assert params[
        "multi_short_reducing_buy_aggression_trigger_inventory_btc"
    ] == pytest.approx(-0.002)
    assert params[
        "multi_short_reducing_buy_aggression_release_inventory_btc"
    ] == pytest.approx(-0.001)
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
    assert report["total_fill_retention"] == pytest.approx(1.0)
    assert report["reducing_buy_fill_ratio"] == pytest.approx(2.0)
    assert report["maker_only_diagnostics"]["maker_violation_count"] == 0
    assert evidence is not None
    assert scorecard is not None
    assert not evidence["family_gate_failures"]
    assert scorecard["hard_gates"]["passed"]
    assert scorecard["ranking_eligible"]
    assert np.isfinite(scorecard["ranking_score"])


def test_maker_violation_cannot_be_rescued_by_positive_reward() -> None:
    panel = _panel()
    candidate = panel["action"].eq(CANDIDATE_ACTION)
    panel.loc[candidate, "maker_violation_count"] = 1

    with pytest.raises(ValueError, match="non-maker"):
        randomized.validate_panel(panel, _spec())
