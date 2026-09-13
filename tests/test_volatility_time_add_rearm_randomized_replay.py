from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from models.audit.experiment_scorecard import score_profile_contract
from research.families.f09_campaign_action_uplift.audit import (
    volatility_time_add_rearm_randomized_replay as randomized,
)
from strategy.fill_cooldown import (
    LINEAGE_CANDIDATE_ACTION,
    LINEAGE_CONTROL_ACTION,
)


def _spec() -> dict[str, object]:
    days = [f"2026-06-{day:02d}" for day in range(1, 13)]
    return {
        "canonical_spec_identity_sha256": "f" * 64,
        "panels": {"development_days": days},
        "scorecard_profile": score_profile_contract("action_execution_v1"),
        "bootstrap": {"draws": 200, "seed": 20260729},
        "family_gates": {
            "actual_action_change_rate_lcb_min": 0.0,
            "minimum_actual_action_change_days_per_side": 10,
            "minimum_fills_retention": 0.90,
        },
    }


def _panel(*, actual_action_change: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for day_index, day in enumerate(_spec()["panels"]["development_days"]):
        for row_index in range(20):
            candidate = row_index % 2 == 1
            action = (
                LINEAGE_CANDIDATE_ACTION
                if candidate
                else LINEAGE_CONTROL_ACTION
            )
            reward = 0.01 if candidate else -0.01
            rows.append(
                {
                    "day": day,
                    "decision_id": f"{day}:{row_index}",
                    "lineage_id": day_index * 100 + row_index,
                    "campaign_id": day_index * 100 + row_index,
                    "side": "BUY",
                    "action": action,
                    "behavior_propensity": 0.5,
                    "assignment_before_downstream_path": 1,
                    "assignment_fixed_within_lineage": 1,
                    "trigger_fill_excluded_from_reward": 1,
                    "reward": reward,
                    "fill_value": reward,
                    "campaign_cost": 0.0,
                    "queue_cost": 0.0,
                    "reward_identity_error": 0.0,
                    "terminal_campaign_pnl": 0.01 if candidate else -0.02,
                    "campaign_mae": -0.01 if candidate else -0.03,
                    "decision_to_terminal_s": 80.0 if candidate else 100.0,
                    "repair_event": 1,
                    "campaign_censored": 0,
                    "intervention_fill_count": 1,
                    "inventory_time_btc_s": 0.8 if candidate else 1.0,
                    "actual_final_action_change_count": (
                        actual_action_change if candidate else 0
                    ),
                    "support_valid": 1,
                    "full_cpp_tick_replay_authority": 0,
                }
            )
    return pd.DataFrame(rows)


def test_canonical_spec_identity_excludes_only_identity_field() -> None:
    payload = {"schema_version": randomized.SCHEMA_VERSION, "value": 1}
    identity = randomized.canonical_spec_identity_sha256(payload)
    frozen = {**payload, "canonical_spec_identity_sha256": identity}

    assert randomized.canonical_spec_identity_sha256(frozen) == identity
    changed = {**frozen, "value": 2}
    assert randomized.canonical_spec_identity_sha256(changed) != identity


def test_lineage_panel_requires_exact_half_propensity_and_unique_lineage() -> None:
    panel = _panel(actual_action_change=1)
    randomized.validate_lineage_panel(panel, _spec())

    wrong_propensity = panel.copy()
    wrong_propensity.loc[0, "behavior_propensity"] = 0.51
    with pytest.raises(ValueError, match="exact 0.5"):
        randomized.validate_lineage_panel(wrong_propensity, _spec())

    duplicate_lineage = panel.copy()
    duplicate_lineage.loc[1, "lineage_id"] = duplicate_lineage.loc[0, "lineage_id"]
    with pytest.raises(ValueError, match="lineage id"):
        randomized.validate_lineage_panel(duplicate_lineage, _spec())


def test_scorecard_requires_actual_final_action_change_per_side() -> None:
    report, evidence, scorecard = randomized.evaluate_side(
        _panel(actual_action_change=0),
        side="BUY",
        spec=_spec(),
        development_q10=-0.02,
    )

    assert report["actual_final_action_change"]["days"] == 0
    assert "actual_final_action_change_lcb_not_positive" in evidence[
        "family_gate_failures"
    ]
    assert not scorecard["ranking_eligible"]
    assert scorecard["ranking_score"] is None


def test_scorecard_can_rank_only_after_all_hard_gates_pass() -> None:
    report, evidence, scorecard = randomized.evaluate_side(
        _panel(actual_action_change=1),
        side="BUY",
        spec=_spec(),
        development_q10=-0.02,
    )

    assert report["actual_final_action_change"]["lcb95"] == pytest.approx(1.0)
    assert not evidence["family_gate_failures"]
    assert scorecard["hard_gates"]["passed"]
    assert scorecard["ranking_eligible"]
    assert np.isfinite(scorecard["ranking_score"])


def test_randomized_replay_config_keeps_q90_and_full_cpp_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(randomized.full_path, "_configure_params", lambda base, day: {})
    spec = {
        "behavior_policy": {
            "probabilities": {
                LINEAGE_CONTROL_ACTION: 0.5,
                LINEAGE_CANDIDATE_ACTION: 0.5,
            },
            "random_seed_base": 17,
        },
        "replay_contract": {
            "trace_lineages_max_per_day": 100,
            "cpp_q90_mismatch_trace_max": 10,
        },
        "reward_contract": {
            "fill_value_horizon_ms": 30_000,
            "markout_max_book_age_ms": 2_000,
        },
    }
    base = {
        "variance_clock": {
            "reference_rate_bps2_per_s": {"BUY": 1.0, "SELL": 2.0},
            "minimum_wall_time_ms": 5_000,
            "maximum_wall_time_ms": 600_000,
            "max_feature_age_ms": 2_000,
        }
    }

    params = randomized._configure_params(spec, base, "2026-06-01")

    assert params["fill_cooldown_clock_mode"] == "randomized_lineage"
    assert params["variance_time_lineage_fail_on_q90_pre_ack_fill"] is True
    assert params["dynamic_fill_hazard_cpp_parity_strict"] is True
    assert params["window_cache_write_enabled"] is False
    assert params["replay_promotion_eligible"] is False


def test_strict_quality_boolean_does_not_treat_false_string_as_true() -> None:
    parsed = randomized._strict_bool_series(
        pd.Series(["true", "false", "1", "0"]),
        label="quality",
    )
    assert parsed.tolist() == [True, False, True, False]
    with pytest.raises(ValueError, match="invalid booleans"):
        randomized._strict_bool_series(pd.Series(["unknown"]), label="quality")
