from __future__ import annotations

import copy

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_closeout_v1 as subject,
)


def _joint_report() -> dict[str, object]:
    return {
        "confirmatory_bands": {
            "bands": {
                "successor:BUY:E3_HIGHER_ORDER_BOOLEAN-ACTION_MATCHED": {
                    "mean_usdc": 0.1,
                    "lcb_usdc": -0.1,
                    "ucb_usdc": 0.3,
                }
            }
        },
    }


def _formal_buy_report() -> dict[str, object]:
    return {
        "scorecards": {
            f"BUY:{subject.OWNER_CANDIDATE}": {
                "hard_gates": {
                    "passed": False,
                    "failures": ["conditional_net_value_lower_bound_not_positive"],
                },
                "promotion_status": "development_failed_family_closed",
            }
        },
        "hierarchy": {
            "steps": {
                "BUY": [
                    {
                        "hypothesis": "successor:BUY:E1-B0",
                        "tested": True,
                        "passed": False,
                        "reason": ("day_or_week_simultaneous_lcb_not_above_economic_epsilon"),
                    }
                ]
            }
        },
    }


def test_outer_receipt_order_keeps_continuous_before_matched_controls() -> None:
    assert subject.OUTER_RECEIPT_CANDIDATE_ORDER[:9] == (
        "B0_CURRENT_EXACT",
        "B1_CAMPAIGN_AGE_ONLY",
        "B2_CAMPAIGN_PLUS_H16_H256",
        "B3_CURRENT_SEMANTIC_EQUIVALENT",
        "E1_FULL_EMA_BANK",
        "E2_DIRECTIONAL_EMA",
        "E3_HIGHER_ORDER_BOOLEAN",
        "M2_TRUE_INCREMENTAL",
        "CONTINUOUS_COMPARATOR",
    )
    assert subject.OUTER_RECEIPT_CANDIDATE_ORDER[9:] == subject.nested.MATCHED_CONTROL_NAMES


def test_owner_decision_preserves_failed_formal_evidence_and_locked_permissions() -> None:
    decision = subject.build_owner_decision(
        formal_buy_report=_formal_buy_report(),
        joint_report=_joint_report(),
        source_bindings={"BUY": {"result": "a" * 64}},
    )

    assert decision["research_supported"] is False
    assert decision["owner_risk_accepted"] is True
    assert decision["outcome_informed_owner_override"] is True
    assert decision["formal_closeout_mutated"] is False
    assert decision["permissions"]["action_authorized"] is False
    assert decision["permissions"]["live_authorized"] is False
    assert decision["evidence_boundary"]["new_economic_arm_run"] is False
    assert decision["canonical_owner_decision_sha256"] == subject.document_sha256(
        decision, "canonical_owner_decision_sha256"
    )


def test_owner_decision_rejects_missing_hard_gate_failure_or_passing_hierarchy() -> None:
    no_failure = _formal_buy_report()
    no_failure["scorecards"][f"BUY:{subject.OWNER_CANDIDATE}"]["hard_gates"]["failures"] = []
    with pytest.raises(subject.OwnerBuyE3CloseoutError):
        subject.build_owner_decision(
            formal_buy_report=no_failure,
            joint_report=_joint_report(),
            source_bindings={},
        )

    passing = copy.deepcopy(_formal_buy_report())
    passing["hierarchy"]["steps"]["BUY"][0]["passed"] = True
    with pytest.raises(subject.OwnerBuyE3CloseoutError):
        subject.build_owner_decision(
            formal_buy_report=passing,
            joint_report=_joint_report(),
            source_bindings={},
        )


def test_sparse_serialized_counts_are_materialized_as_integer_zero() -> None:
    left = pd.DataFrame(
        {
            "action_count::CONTROL_85N": [3.0],
            "action_count::FIXED_79S": [float("nan")],
        }
    )
    right = pd.DataFrame(
        {
            "action_count::CONTROL_85N": [2],
            "action_count::FIXED_173S": [1],
        }
    )

    normalized_left, normalized_right = subject.normalize_sparse_count_columns((left, right))

    assert normalized_left["action_count::FIXED_79S"].tolist() == [0]
    assert normalized_left["action_count::FIXED_173S"].tolist() == [0]
    assert normalized_right["action_count::FIXED_79S"].tolist() == [0]
    assert all(
        str(dtype) == "int64" for dtype in normalized_left.dtypes if str(dtype).startswith("int")
    )
