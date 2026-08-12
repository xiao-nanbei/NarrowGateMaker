from __future__ import annotations

from typing import Any

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_oof_truth_audit as audit,
)


def _policy(side: str, shape: str) -> dict[str, Any]:
    clauses = {
        "single_literal": [{"literals": [{"predicate": "a", "negated": False}]}],
        "two_literal_and": [
            {
                "literals": [
                    {"predicate": "a", "negated": False},
                    {"predicate": "b", "negated": True},
                ]
            }
        ],
        "two_clause_or": [
            {"literals": [{"predicate": "a", "negated": False}]},
            {"literals": [{"predicate": "b", "negated": True}]},
        ],
    }[shape]
    return {
        "side": side,
        "ordered_first_match_rules": [{"action": "FIXED", "clauses": clauses}],
    }


def _fixture() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    panels = {
        "prefix40": ("R0", "M0", "M1"),
        "prefix33": ("R0", "M0", "M1", "M2"),
    }
    report: dict[str, Any] = {
        "identity": audit.OOF_IDENTITY,
        "modeled_label_census": {"arm_rows": 120400},
        "results": {},
        "panel_denominators": {},
    }
    selected: dict[str, Any] = {}
    shape_cycle = ("single_literal", "two_literal_and", "two_clause_or")
    cursor = 0
    for panel, blocks in panels.items():
        report["results"][panel] = {}
        selected[panel] = {}
        report["panel_denominators"][panel] = {
            "sides": {
                "BUY": {"outer_oof_test_days": ["d1"] * (28 if panel == "prefix40" else 21)},
                "SELL": {"outer_oof_test_days": ["d1"] * (28 if panel == "prefix40" else 21)},
            }
        }
        for side in ("BUY", "SELL"):
            report["results"][panel][side] = {}
            selected[panel][side] = {}
            for block in blocks:
                mean = 0.001 if cursor < 3 else -0.001
                report["results"][panel][side][block] = {
                    "boolean": {
                        "partial_identification": {
                            "identified_mean_usdc": mean,
                            "identified_lcb_usdc": -0.002,
                        },
                        "deployment_gate": {
                            "passed_for_owner_repeated_policy_successor": False
                        },
                        "folds": [
                            {
                                "inner_partial_identification": {
                                    "identified_mean_usdc": 0.001 + fold * 0.0001
                                }
                            }
                            for fold in range(4)
                        ],
                    }
                }
                selected[panel][side][block] = {
                    "boolean": [
                        _policy(side, shape_cycle[(cursor + fold) % len(shape_cycle)])
                        for fold in range(4)
                    ]
                }
                cursor += 1
    owner_spec = {
        "identity": audit.OOF_IDENTITY,
        "modeled_label_source": {
            "opportunity_rows": 8600,
            "arm_rows": 68800,
            "arm_count_per_opportunity": 8,
        },
    }
    amendment = {
        "post_outer_oof_gate_contract": {
            "feature_family_selection": {
                "hierarchy": ["M0 absolute", "M1-M0 paired", "M2-M1 paired"],
                "multiple_comparison_control": "Bonferroni",
            }
        }
    }
    return report, selected, owner_spec, amendment


def test_truth_audit_separates_dense_slots_hierarchy_and_search_scope() -> None:
    report, selected, owner_spec, amendment = _fixture()
    result = audit.build_truth_audit(
        report=report,
        selected=selected,
        owner_spec=owner_spec,
        hierarchy_amendment=amendment,
    )
    assert result["label_census"] == {
        "opportunities": 8600,
        "executed_arm_rows": 68800,
        "arms_per_opportunity": 8,
        "historical_report_dense_side_action_slots": 120400,
        "dense_slots_are_executed_arms": False,
        "historical_arm_rows_field_is_misnamed": True,
    }
    assert result["oof"]["absolute_boolean_cells"] == 14
    assert result["oof"]["inner_selected_fold_count"] == 56
    assert result["oof"]["M0_absolute_passed_sides"] == []
    assert result["policy_search"]["outer_fold_policy_count"] == 56
    assert result["policy_search"]["multi_rule_ordered_policy_count"] == 0
    assert not result["feature_family_gate"]["frozen_hierarchy_fully_implemented"]
    assert not result["permissions"]["repeated_policy_run"]


def test_truth_audit_rejects_identity_drift() -> None:
    report, selected, owner_spec, amendment = _fixture()
    report["identity"] = "wrong"
    with pytest.raises(audit.TruthAuditError, match="identity drifted"):
        audit.build_truth_audit(
            report=report,
            selected=selected,
            owner_spec=owner_spec,
            hierarchy_amendment=amendment,
        )
