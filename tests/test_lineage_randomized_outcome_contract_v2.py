from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from research.families.f09_campaign_action_uplift.audit import (
    lineage_randomized_outcome_contract as contract,
)
from strategy.fill_cooldown import (
    LINEAGE_CANDIDATE_ACTION,
    LINEAGE_CONTROL_ACTION,
    StratifiedBernoulliLineageRandomizer,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = (
    ROOT
    / "research"
    / "families"
    / "f09_campaign_action_uplift"
    / "docs"
    / "lineage_randomized_outcome_contract_v2.json"
)
BASE_MS = 1_700_000_000_000


def _foundation() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def _equity_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "campaign_start_equity_usdc": [10.0],
            "assignment_equity_usdc": [12.0],
            "lineage_terminal_equity_usdc": [13.5],
            "campaign_terminal_equity_usdc": [14.0],
        }
    )


def test_foundation_contract_hash_and_authority_are_fail_closed() -> None:
    foundation = _foundation()
    contract.validate_foundation_contract(foundation)

    drifted = json.loads(json.dumps(foundation))
    drifted["permissions"]["validation_read"] = True
    with pytest.raises(ValueError, match="hash mismatch|cannot grant"):
        contract.validate_foundation_contract(drifted)


def test_future_action_must_freeze_adjustment_and_day_side_strata() -> None:
    producer_paths = (
        "research/families/f09_campaign_action_uplift/audit/"
        "lineage_randomized_outcome_contract.py",
        "tests/test_lineage_randomized_outcome_contract_v2.py",
    )
    registration = {
        "foundation_contract_id": contract.CONTRACT_ID,
        "action_family_id": "new_action_v1",
        "primary_estimand": contract.PRIMARY_ESTIMAND,
        "stratification_keys": ["UTC_day", "side"],
        "trace_schema_version": contract.TRACE_SCHEMA_VERSION,
        "mechanism_extension_schema": "none",
        "producer_identity": {
            path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
            for path in producer_paths
        },
        "randomization_seed": 17,
        "action_semantics": {
            "baseline": "registered_baseline",
            "candidate": "registered_candidate",
        },
        "assignment_before_downstream_path": True,
        "downstream_assignment_policy": (
            "persist_to_campaign_terminal_no_rerandomization"
        ),
        "maximum_assignments_per_campaign": 1,
        "covariate_adjustment": {
            "enabled": True,
            "mode": "lin_v1",
            "primary_or_sensitivity": "primary",
            "covariates": [
                "pre_assignment_campaign_pnl_usdc",
                "assignment_inventory_btc",
                "campaign_age_at_assignment_ms",
            ],
            "formula": (
                "Y_post ~ action + day_side_FE + centered_X + "
                "action:centered_X"
            ),
            "missing_policy": "fail",
            "variance": "UTC_day_cluster_robust",
            "frozen_before_outcome_read": True,
        },
    }
    contract.validate_action_registration(registration, _foundation())

    post_treatment = json.loads(json.dumps(registration))
    post_treatment["covariate_adjustment"]["covariates"].append(
        "lineage_reward_usdc"
    )
    with pytest.raises(ValueError, match="post-assignment"):
        contract.validate_action_registration(post_treatment, _foundation())


def test_post_assignment_accounting_excludes_pre_assignment_pnl() -> None:
    derived = contract.derive_post_assignment_outcomes(_equity_frame())

    assert derived.loc[0, "pre_assignment_campaign_pnl_usdc"] == pytest.approx(2.0)
    assert derived.loc[0, "lineage_reward_usdc"] == pytest.approx(1.5)
    assert derived.loc[0, "post_lineage_continuation_value_usdc"] == pytest.approx(0.5)
    assert derived.loc[0, contract.PRIMARY_ESTIMAND] == pytest.approx(2.0)
    assert derived.loc[0, "accounting_identity_error_usdc"] == pytest.approx(0.0)


def test_stratified_randomizer_keeps_conditional_half_propensity() -> None:
    randomizer = StratifiedBernoulliLineageRandomizer(
        seed=17,
        family_id="new_action_v1",
    )
    assignments = [
        randomizer.assign(
            utc_day="2026-07-29",
            side="BUY",
            pre_assignment_lineage_uid=f"uid-{index}",
        )
        for index in range(1000)
    ]
    assert all(row[2] == "2026-07-29|BUY" for row in assignments)
    assert all(
        (row[0] == LINEAGE_CONTROL_ACTION) == (row[1] < 0.5)
        for row in assignments
    )
    candidate_rate = np.mean(
        [row[0] == LINEAGE_CANDIDATE_ACTION for row in assignments]
    )
    assert 0.45 < candidate_rate < 0.55
    repeated = randomizer.assign(
        utc_day="2026-07-29",
        side="BUY",
        pre_assignment_lineage_uid="uid-10",
    )
    assert repeated == assignments[10]

    sell = randomizer.assign(
        utc_day="2026-07-29",
        side="SELL",
        pre_assignment_lineage_uid="sell-uid",
    )
    assert sell[2] == "2026-07-29|SELL"


def _replay_params() -> dict[str, object]:
    return {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "max_exec_book_age_s": 0.0,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "fill_cooldown": 85.0,
        "fill_cooldown_reducing": 0.0,
        "fill_cooldown_apply_reducing": False,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        "fill_cooldown_clock_mode": "randomized_lineage",
        "variance_time_reference_rate_buy_bps2_per_s": 1.0,
        "variance_time_reference_rate_sell_bps2_per_s": 1.0,
        "variance_time_minimum_wall_time_ms": 5_000,
        "variance_time_maximum_wall_time_ms": 600_000,
        "variance_time_max_feature_age_ms": 2_000,
        "variance_time_lineage_randomized_enabled": True,
        "trace_variance_time_lineage_max": 100,
        "variance_time_lineage_seed": 17,
        "lineage_randomized_outcome_contract_version": "v2",
        "lineage_randomized_family_id": "test_new_action_v1",
        "variance_time_lineage_probabilities": {
            LINEAGE_CONTROL_ACTION: 0.5,
            LINEAGE_CANDIDATE_ACTION: 0.5,
        },
        "variance_time_lineage_markout_max_age_ms": 2_000,
        "variance_time_lineage_fail_on_q90_pre_ack_fill": True,
    }


def test_authoritative_replay_emits_complete_native_v2_lineage_trace() -> None:
    offsets = list(range(0, 11_000, 1_000))
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + offset for offset in offsets], dtype=np.int64
            ),
            "price": np.asarray([100.0, 96.6, *([96.6] * 8), 90.0]),
            "quantity": np.asarray([0.0, 0.001, *([0.0] * 8), 0.001]),
            "is_buyer_maker": np.asarray(
                [False, True, *([False] * 8), True], dtype=np.uint8
            ),
        }
    )
    variance = {
        "feature_ready_ts_ms": np.asarray([BASE_MS], dtype=np.int64),
        "rate_bps2_per_s": np.asarray([100.0], dtype=np.float64),
        "valid": np.asarray([True], dtype=np.bool_),
    }
    result = bt._simulate_tick_with_engine(
        "python",
        trades,
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        _replay_params(),
        variance_time_data=variance,
    )
    trace = pd.DataFrame(result["_variance_time_lineage_trace"])
    events = pd.DataFrame(result["_variance_time_lineage_event_journal"])

    validated = contract.validate_native_lineage_trace(
        trace,
        _foundation(),
        event_journal=events,
        producer_audit=result["_lineage_randomized_trace_audit"],
    )

    assert len(validated) == 1
    assert validated.loc[0, "randomization_stratum"].endswith("|BUY")
    assert validated.loc[0, "accounting_identity_error_usdc"] == pytest.approx(0.0)
    assert result["lineage_randomized_outcome_contract_version"] == "v2"
    assert events.iloc[0]["event_type"] == "assignment"
    assert events.iloc[-1]["event_type"] == "campaign_terminal"
    assert result["_lineage_randomized_trace_audit"]["coverage_complete"] is True


def test_native_trace_missing_producer_field_fails_instead_of_filtering() -> None:
    foundation = _foundation()
    row = {column: 0 for column in contract.required_native_trace_columns()}
    row.update(
        {
            "trace_schema_version": contract.TRACE_SCHEMA_VERSION,
            "mechanism_extension_schema": "none",
            "lineage_uid": "uid",
            "lineage_id": 1,
            "campaign_id": 1,
            "day": "2026-07-29",
            "side": "BUY",
            "action": "baseline",
            "behavior_propensity": 0.5,
            "randomization_stratum": "2026-07-29|BUY",
            "assignment_before_downstream_path": 1,
            "assignment_fixed_within_lineage": 1,
            "assignment_inventory_btc": 0.001,
            "campaign_age_at_assignment_ms": 0,
            "assignment_ts_ms": 1,
            "lineage_terminal_ts_ms": 3,
            "campaign_terminal_ts_ms": 4,
            "variance_ready_status": "censored_before_ready",
            "clock_direction": "censored_unknown",
            "final_blocker": "none",
            "final_action_change_status": "no_eligible_decision",
            "lineage_terminal_reason": "censor",
            "campaign_terminal_reason": "censor",
        }
    )
    frame = pd.DataFrame([row]).drop(columns=["final_blocker"])

    with pytest.raises(ValueError, match="final_blocker"):
        contract.validate_native_lineage_trace(frame, foundation)


def test_native_trace_rejects_more_than_one_assignment_per_campaign() -> None:
    result = bt._simulate_tick_with_engine(
        "python",
        pd.DataFrame(
            {
                "transact_time": np.asarray(
                    [BASE_MS, BASE_MS + 1_000, BASE_MS + 10_000],
                    dtype=np.int64,
                ),
                "price": np.asarray([100.0, 96.6, 90.0]),
                "quantity": np.asarray([0.0, 0.001, 0.001]),
                "is_buyer_maker": np.asarray([False, True, True], dtype=np.uint8),
            }
        ),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        _replay_params(),
        variance_time_data={
            "feature_ready_ts_ms": np.asarray([BASE_MS], dtype=np.int64),
            "rate_bps2_per_s": np.asarray([100.0], dtype=np.float64),
            "valid": np.asarray([True], dtype=np.bool_),
        },
    )
    trace = pd.DataFrame(result["_variance_time_lineage_trace"])
    duplicated = pd.concat([trace, trace], ignore_index=True)
    duplicated.loc[1, "lineage_uid"] = f"{duplicated.loc[1, 'lineage_uid']}:duplicate"

    with pytest.raises(ValueError, match="more than one assignment"):
        contract.validate_native_lineage_trace(
            duplicated,
            _foundation(),
            event_journal=pd.DataFrame(
                result["_variance_time_lineage_event_journal"]
            ),
            producer_audit=result["_lineage_randomized_trace_audit"],
        )


def test_native_trace_requires_event_and_denominator_coverage() -> None:
    offsets = list(range(0, 11_000, 1_000))
    result = bt._simulate_tick_with_engine(
        "python",
        pd.DataFrame(
            {
                "transact_time": np.asarray(
                    [BASE_MS + offset for offset in offsets], dtype=np.int64
                ),
                "price": np.asarray([100.0, 96.6, *([96.6] * 8), 90.0]),
                "quantity": np.asarray([0.0, 0.001, *([0.0] * 8), 0.001]),
                "is_buyer_maker": np.asarray(
                    [False, True, *([False] * 8), True], dtype=np.uint8
                ),
            }
        ),
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        _replay_params(),
        variance_time_data={
            "feature_ready_ts_ms": np.asarray([BASE_MS], dtype=np.int64),
            "rate_bps2_per_s": np.asarray([100.0], dtype=np.float64),
            "valid": np.asarray([True], dtype=np.bool_),
        },
    )
    trace = pd.DataFrame(result["_variance_time_lineage_trace"])
    events = pd.DataFrame(result["_variance_time_lineage_event_journal"])
    bad_audit = json.loads(json.dumps(result["_lineage_randomized_trace_audit"]))
    bad_audit["denominator_counts"]["producer_validated"] = 0

    with pytest.raises(ValueError, match="producer denominator drifted"):
        contract.validate_native_lineage_trace(
            trace,
            _foundation(),
            event_journal=events,
            producer_audit=bad_audit,
        )

    with pytest.raises(ValueError, match="journal"):
        contract.validate_native_lineage_trace(
            trace,
            _foundation(),
            event_journal=events.iloc[:-1].copy(),
            producer_audit=result["_lineage_randomized_trace_audit"],
        )
