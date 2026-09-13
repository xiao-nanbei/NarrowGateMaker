from __future__ import annotations

import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit.local_action_ope_report import (
    _outcome_panels,
    _paired_action_contrast,
    _resolve_action_family,
)
from research.families.f09_campaign_action_uplift.audit.local_action_uplift import (
    OPE_FEATURES,
    QUEUE_VALUE_OPE_FEATURES,
    _run_day,
    native_censoring_reward_bounds,
    parse_args,
    validate_action_panel,
)
from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import (
    DEFAULT_FEATURE_SPECS,
    resolve_feature_specs,
)
from models.replay_policies import (
    LOCAL_ACTIONS,
    QUEUE_VALUE_CANCEL_REENTER_ACTIONS,
)


def _panel() -> pd.DataFrame:
    rows = []
    for idx, action in enumerate(LOCAL_ACTIONS):
        reward = float(idx)
        fill_value = reward + 0.5
        rows.append(
            {
                "day": f"2026-01-{idx + 1:02d}",
                "decision_id": f"decision-{idx}",
                "campaign_id": 1,
                "side": "BUY",
                "inventory_role": "add",
                "action": action,
                "behavior_propensity": 0.25,
                "reward": reward,
                "fill_value": fill_value,
                "campaign_cost": 0.5,
                "queue_cost": 0.0,
                "reward_identity_error": 0.0,
                **{f"behavior_prob_{name}": 0.25 for name in LOCAL_ACTIONS},
            }
        )
    return pd.DataFrame(rows)


def test_validate_action_panel_accepts_one_intervention_per_campaign() -> None:
    validate_action_panel(_panel())


def test_validate_action_panel_rejects_duplicate_campaign_intervention() -> None:
    frame = pd.concat([_panel(), _panel().iloc[[0]].assign(decision_id="extra")])
    with pytest.raises(ValueError, match="at most one"):
        validate_action_panel(frame)


def test_validate_action_panel_rejects_reward_double_count() -> None:
    frame = _panel()
    frame.loc[0, "reward_identity_error"] = 0.1
    with pytest.raises(ValueError, match="reward !="):
        validate_action_panel(frame)


def test_validate_action_panel_rejects_propensity_mismatch() -> None:
    frame = _panel()
    frame.loc[0, "behavior_propensity"] = 0.5

    with pytest.raises(ValueError, match="exact probability"):
        validate_action_panel(frame)


def test_validate_action_panel_rejects_more_than_one_tick() -> None:
    frame = _panel()
    frame["action_delta_ticks"] = [0.0, 0.5, 1.01, -1.0]

    with pytest.raises(ValueError, match="one tick"):
        validate_action_panel(frame)


def test_unfrozen_smoke_cli_can_name_new_queue_family(tmp_path) -> None:
    args = parse_args(
        [
            "--days",
            "2026-06-05",
            "--config",
            str(tmp_path / "config.yaml"),
            "--output-prefix",
            str(tmp_path / "smoke"),
            "--action-family",
            "queue_value_keep_cancel",
            "--eligible-sides",
            "BUY,SELL",
        ]
    )

    assert args.panel_role == "smoke"
    assert args.action_family == "queue_value_keep_cancel"
    assert args.eligible_sides == "BUY,SELL"


def test_run_day_uses_serialized_bundle_binding_not_main_local_args() -> None:
    with pytest.raises(
        SystemExit,
        match="--queue-competing-risk-bundle is valid only",
    ):
        _run_day(
            (
                "2026-06-05",
                "BTCUSDC",
                {
                    "_randomized_action_family": "queue_value_keep_cancel",
                    "queue_value_competing_risk_bundle_path": "unexpected.json",
                },
            )
        )


def test_ope_outcomes_keep_terminal_and_tail_targets_separate() -> None:
    frame = _panel()
    frame["terminal_campaign_pnl"] = [-6.0, -1.0, 0.0, 2.0]
    frame["intervention_fill_count"] = [1, 0, 0, 1]

    outcomes = _outcome_panels(frame, -5.0)

    assert outcomes["reward"]["ope_target"].tolist() == [0.0, 1.0, 2.0, 3.0]
    assert outcomes["terminal"]["ope_target"].tolist() == [-6.0, -1.0, 0.0, 2.0]
    assert outcomes["tail_avoidance"]["ope_target"].tolist() == [-1.0, -0.0, -0.0, -0.0]
    assert outcomes["intervention_fill"]["ope_target"].tolist() == [1.0, 0.0, 0.0, 1.0]


def test_queue_value_ope_features_are_trace_native_and_registered() -> None:
    queue_trace_columns = {
        "side",
        "inventory",
        "inventory_ratio",
        "campaign_age_s",
        "campaign_pnl_so_far",
        "campaign_mae_so_far",
        "campaign_add_count_so_far",
        "order_age_ms",
        "quote_distance_ticks",
        "queue_init",
        "queue_left",
        "queue_fraction_left",
        "queue_local_rank",
        "spread_ticks",
        "book_imbalance",
        "microprice_shift_bps",
        "l2_book_cancel_ratio",
        "l2_book_refresh_ratio",
        "l2_quote_flip_rate",
        "toxicity",
        "markout_ema",
        "maker_expected_ticks",
        "empirical_adverse_probability",
        "empirical_favorable_probability",
        "market_order_intensity",
        "cancel_intensity",
        "refill_intensity",
        "adverse_to_refill_ratio",
        "queue_state_key",
        "microprice_state_key",
    }
    generic_only = {
        "campaign_max_abs_qty_so_far",
        "campaign_adverse_excursion_so_far",
        "l2_near_depth_total",
        "quote_distance_bps",
        "quote_delta_to_bbo_ticks",
        "exact_l2_spread_bps",
        "baseline_visible_queue_ahead",
        "baseline_estimated_queue_ahead",
        "mid",
    }

    assert set(QUEUE_VALUE_OPE_FEATURES) == queue_trace_columns
    assert set(QUEUE_VALUE_OPE_FEATURES).isdisjoint(generic_only)
    assert set(OPE_FEATURES).intersection(generic_only)

    frame = pd.DataFrame(
        {
            name: ["state"] if name.endswith("_key") or name == "side" else [0.0]
            for name in QUEUE_VALUE_OPE_FEATURES
        }
    )
    registry = {spec.name: spec for spec in DEFAULT_FEATURE_SPECS}
    specs = resolve_feature_specs(
        frame,
        QUEUE_VALUE_OPE_FEATURES,
        registry=registry,
    )
    assert [spec.name for spec in specs] == list(QUEUE_VALUE_OPE_FEATURES)


def test_ope_report_resolves_queue_value_family_from_frozen_probabilities() -> None:
    panel = _panel().iloc[:2].copy()
    panel["action"] = ["keep", "cancel_until_state_exit"]
    panel["behavior_propensity"] = 0.5
    panel["behavior_prob_keep"] = 0.5
    panel["behavior_prob_cancel_until_state_exit"] = 0.5
    panel = panel.drop(
        columns=[f"behavior_prob_{action}" for action in LOCAL_ACTIONS],
        errors="ignore",
    )

    family = _resolve_action_family(panel)

    assert family["name"] == "queue_value_keep_cancel"
    assert family["baseline_action"] == "keep"
    assert family["actions"] == ("keep", "cancel_until_state_exit")
    assert family["features"] == QUEUE_VALUE_OPE_FEATURES
    assert family["require_zero_queue_cost"] is False


def test_ope_report_resolves_queue_value_cancel_reenter_family() -> None:
    panel = _panel().iloc[:2].copy()
    panel["action"] = list(QUEUE_VALUE_CANCEL_REENTER_ACTIONS)
    panel["behavior_propensity"] = 0.5
    for action in QUEUE_VALUE_CANCEL_REENTER_ACTIONS:
        panel[f"behavior_prob_{action}"] = 0.5
    panel = panel.drop(
        columns=[f"behavior_prob_{action}" for action in LOCAL_ACTIONS],
        errors="ignore",
    )

    family = _resolve_action_family(panel)

    assert family["name"] == "queue_value_cancel_reenter"
    assert family["baseline_action"] == "keep"
    assert family["actions"] == QUEUE_VALUE_CANCEL_REENTER_ACTIONS
    assert family["features"] == QUEUE_VALUE_OPE_FEATURES
    assert family["require_zero_queue_cost"] is False


def test_paired_action_contrast_is_candidate_minus_baseline() -> None:
    candidate = pd.DataFrame(
        {
            "day": ["2026-01-01", "2026-01-02"],
            "decision_id": ["a", "b"],
            "ope_dr_value": [2.0, 4.0],
        }
    )
    baseline = pd.DataFrame(
        {
            "day": ["2026-01-01", "2026-01-02"],
            "decision_id": ["a", "b"],
            "ope_dr_value": [1.0, 1.0],
        }
    )
    numerical_summary = {
        "numerical_ope_gate_passed": True,
        "overlap": {"effective_sample_size": 100.0},
    }

    rows, summary = _paired_action_contrast(
        candidate,
        baseline,
        candidate_action="cancel_until_state_exit",
        baseline_action="keep",
        candidate_summary=numerical_summary,
        baseline_summary=numerical_summary,
        bootstrap_trials=0,
        random_seed=7,
    )

    assert rows["ope_dr_uplift"].tolist() == [1.0, 3.0]
    assert summary["dr_uplift"] == 2.0
    assert summary["numerical_contrast_gate_passed"] is True


def test_native_censoring_bounds_keep_randomized_denominator() -> None:
    frame = pd.DataFrame(
        {
            "day": [
                "2026-01-01",
                "2026-01-02",
                "2026-01-01",
                "2026-01-02",
            ],
            "side": ["BUY"] * 4,
            "action": [
                "keep",
                "keep",
                "cancel_until_state_exit",
                "cancel_until_state_exit",
            ],
            "reward": [1.0, 999.0, 4.0, -999.0],
            "native_exchange_outcome_supported": [1, 0, 1, 0],
        }
    )

    report = native_censoring_reward_bounds(
        frame,
        actions=("keep", "cancel_until_state_exit"),
        reward_clip_usdc=(-10.0, 10.0),
        bootstrap_trials=0,
    )

    pooled = report["scopes"]["pooled"]
    assert pooled["arms"]["keep"]["rows"] == 2
    assert pooled["arms"]["keep"]["supported_rows"] == 1
    assert pooled["arms"]["keep"]["value_lower"] == pytest.approx(-4.5)
    assert pooled["arms"]["keep"]["value_upper"] == pytest.approx(5.5)
    cancel = pooled["arms"]["cancel_until_state_exit"]
    assert cancel["rows"] == 2
    assert cancel["supported_rows"] == 1
    assert cancel["value_lower"] == pytest.approx(-3.0)
    assert cancel["value_upper"] == pytest.approx(7.0)
    contrast = pooled["contrasts"]["cancel_until_state_exit_minus_keep"]
    assert contrast["uplift_lower"] == pytest.approx(-8.5)
    assert contrast["uplift_upper"] == pytest.approx(11.5)


def test_native_censoring_bounds_collapse_when_all_outcomes_supported() -> None:
    frame = pd.DataFrame(
        {
            "day": [
                "2026-01-01",
                "2026-01-02",
                "2026-01-01",
                "2026-01-02",
            ],
            "side": ["BUY"] * 4,
            "action": [
                "keep",
                "keep",
                "cancel_until_state_exit",
                "cancel_until_state_exit",
            ],
            "reward": [1.0, 1.0, 3.0, 3.0],
            "native_exchange_outcome_supported": [1, 1, 1, 1],
        }
    )

    report = native_censoring_reward_bounds(
        frame,
        actions=("keep", "cancel_until_state_exit"),
        reward_clip_usdc=(-10.0, 10.0),
        bootstrap_trials=20,
        random_seed=17,
    )

    contrast = report["scopes"]["pooled"]["contrasts"][
        "cancel_until_state_exit_minus_keep"
    ]
    assert contrast["uplift_lower"] == pytest.approx(2.0)
    assert contrast["uplift_upper"] == pytest.approx(2.0)
    assert contrast["lower_bound_bootstrap_p025"] == pytest.approx(2.0)
    assert contrast["strict_positive_lower_gate"] is True
