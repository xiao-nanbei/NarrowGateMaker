from __future__ import annotations

from collections import Counter
import math

import pytest

import research.families.f09_campaign_action_uplift.audit.post_cooldown_incremental_inventory_budget_feasibility as audit
from research.families.f09_campaign_action_uplift.audit.post_cooldown_incremental_inventory_budget_feasibility import (
    _assert_q90_off_result,
    _configure_budget_params,
    _control_equivalence_summary,
    _derive_grid,
    _market_source_manifest_identity,
    _order_path_counter,
    _panel_summary,
    _path_difference,
    _summarize_candidates,
    _validate_source_rehash_mode,
    canonical_sha256,
)


def test_v1_2_forces_q90_off_in_every_budget_arm(monkeypatch) -> None:
    monkeypatch.setattr(
        audit,
        "_configure_params",
        lambda baseline, day: {
            "dynamic_fill_hazard_shadow_enabled": True,
            "dynamic_fill_hazard_action_enabled": True,
            "dynamic_fill_hazard_cpp_parity_enabled": True,
        },
    )
    params = _configure_budget_params(
        {},
        {"replay_contract": {"buy_q90_action": "off_both_arms"}},
        "2026-04-17",
        budget_units=math.inf,
        target_side="BOTH",
        trace_limit=10,
    )
    assert params["dynamic_fill_hazard_shadow_enabled"] is False
    assert params["dynamic_fill_hazard_action_enabled"] is False
    assert params["dynamic_fill_hazard_cpp_parity_enabled"] is False
    assert params["dynamic_fill_hazard_mechanics_telemetry_enabled"] is False


def test_v1_2_rejects_any_q90_on_contract(monkeypatch) -> None:
    monkeypatch.setattr(audit, "_configure_params", lambda baseline, day: {})
    with pytest.raises(ValueError, match="only supports q90 OFF"):
        _configure_budget_params(
            {},
            {"replay_contract": {"buy_q90_action": "on"}},
            "2026-04-17",
            budget_units=1.0,
            target_side="SELL",
            trace_limit=10,
        )


def test_q90_off_result_rejects_evaluation_or_enabled_state() -> None:
    _assert_q90_off_result(
        {
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_eval_count": 0,
            "dynamic_fill_hazard_cancel_request_count": 0,
        }
    )
    with pytest.raises(RuntimeError, match="evaluations or actions"):
        _assert_q90_off_result({"dynamic_fill_hazard_eval_count": 1})
    with pytest.raises(RuntimeError, match="evaluations or actions"):
        _assert_q90_off_result({"dynamic_fill_hazard_action_enabled": True})


def test_formal_development_cannot_skip_source_rehash() -> None:
    with pytest.raises(ValueError, match="cannot skip source rehash"):
        _validate_source_rehash_mode(
            diagnostic_subset=False,
            skip_source_rehash=True,
        )
    _validate_source_rehash_mode(
        diagnostic_subset=True,
        skip_source_rehash=True,
    )


def _trace(
    day: str,
    side: str,
    *,
    consumed: float,
    hit: bool,
    supported: bool = True,
) -> dict[str, object]:
    return {
        "day": day,
        "side": side,
        "supported": int(supported),
        "censored": 0,
        "budget_hit": int(hit),
        "consumed_units": consumed,
        "blocked_planned_units": float(hit),
        "max_abs_inventory_units": consumed + 1.0,
        "reducing_order_budget_bypass_count": 1,
        "one_order_overshoot_count": 0,
    }


def test_path_counter_and_difference_remain_side_specific() -> None:
    control_rows = [
        {
            "side": "BUY",
            "submit_ts": 1,
            "price": 100.0,
            "outcome": "fill",
            "outcome_ts": 2,
            "fill_qty": 0.001,
            "remaining": 0.0,
        },
        {
            "side": "SELL",
            "submit_ts": 1,
            "price": 101.0,
            "outcome": "cancel",
            "outcome_ts": 3,
            "fill_qty": 0.0,
            "remaining": 0.001,
        },
    ]
    candidate_rows = [control_rows[1]]
    control = _order_path_counter(control_rows)
    candidate = _order_path_counter(candidate_rows)

    assert _path_difference(control, candidate, side="BUY") == {
        "candidate_only_order_outcomes": 0,
        "control_only_order_outcomes": 1,
    }
    assert _path_difference(control, candidate, side="SELL") == {
        "candidate_only_order_outcomes": 0,
        "control_only_order_outcomes": 0,
    }


def test_candidate_grid_is_derived_separately_by_side() -> None:
    controls = [
        {
            "budget_trace": [
                _trace("2026-01-01", "BUY", consumed=value, hit=False)
                for value in (1.0, 1.0, 2.0, 3.0)
            ]
            + [
                _trace("2026-01-01", "SELL", consumed=value, hit=False)
                for value in (2.0, 2.0, 3.0, 4.0)
            ]
        }
    ]

    assert _derive_grid(controls, 3) == {"BUY": [1, 2], "SELL": [2, 3]}


def test_mechanics_summary_requires_support_leverage_retention_and_integrity() -> None:
    days = [f"2026-01-{index:02d}" for index in range(1, 11)]
    controls = [
        {
            "day": day,
            "path_stats": {
                "BUY": {"fill_event_count": 10, "order_count": 20},
                "SELL": {"fill_event_count": 10, "order_count": 20},
            },
        }
        for day in days
    ]
    arms = []
    for day in days:
        for side in ("BUY", "SELL"):
            arms.append(
                {
                    "day": day,
                    "target_side": side,
                    "budget_units": 2,
                    "budget_trace": [
                        _trace(day, side, consumed=2.0, hit=index < 2)
                        for index in range(10)
                    ],
                    "path_stats": {
                        side: {"fill_event_count": 9, "order_count": 18}
                    },
                    "path_difference": {
                        "candidate_only_order_outcomes": 2,
                        "control_only_order_outcomes": 3,
                    },
                    "mechanics": {
                        "post_cooldown_incremental_inventory_budget_conservation_failures": 0
                    },
                }
            )
    candidates = [{"arms": arms}]
    spec = {
        "mechanics_gates": {
            "minimum_supported_episodes_per_side_budget": 100,
            "minimum_supported_days_per_side_budget": 10,
            "minimum_action_change_rate": 0.05,
            "maximum_action_change_rate": 0.50,
            "minimum_fill_retention": 0.85,
            "minimum_activity_retention": 0.75,
            "maximum_unsupported_rate": 0.05,
        }
    }

    summary = _summarize_candidates(
        controls,
        candidates,
        {"BUY": [2], "SELL": [2]},
        spec,
    )
    assert len(summary) == 2
    assert summary["mechanics_region_passed"].all()
    assert summary["final_action_change_rate"].tolist() == pytest.approx([0.2, 0.2])
    assert summary["fill_retention"].tolist() == pytest.approx([0.9, 0.9])


def test_grade_b_sensitivity_is_not_pooled_into_grade_a_primary() -> None:
    controls = [
        {
            "day": day,
            "path_stats": {
                "BUY": {"fill_event_count": 10, "order_count": 10},
                "SELL": {"fill_event_count": 10, "order_count": 10},
            },
        }
        for day in ("2026-01-01", "2026-01-02")
    ]
    candidates = []
    for day in ("2026-01-01", "2026-01-02"):
        candidates.append(
            {
                "day": day,
                "arms": [
                    {
                        "day": day,
                        "target_side": "SELL",
                        "budget_units": 1,
                        "budget_trace": [_trace(day, "SELL", consumed=1.0, hit=True)],
                        "path_stats": {"SELL": {"fill_event_count": 9, "order_count": 9}},
                        "path_difference": {
                            "candidate_only_order_outcomes": 0,
                            "control_only_order_outcomes": 1,
                        },
                        "mechanics": {
                            "post_cooldown_incremental_inventory_budget_conservation_failures": 0
                        },
                    }
                ],
            }
        )
    spec = {
        "mechanics_gates": {
            "minimum_supported_episodes_per_side_budget": 1,
            "minimum_supported_days_per_side_budget": 1,
            "minimum_action_change_rate": 0.0,
            "maximum_action_change_rate": 1.0,
            "minimum_fill_retention": 0.0,
            "minimum_activity_retention": 0.0,
            "maximum_unsupported_rate": 1.0,
        }
    }

    primary = _panel_summary(
        controls,
        candidates,
        {"SELL": [1]},
        spec,
        panel="grade_a_primary",
        days=["2026-01-01"],
    )
    sensitivity = _panel_summary(
        controls,
        candidates,
        {"SELL": [1]},
        spec,
        panel="grade_b_sensitivity",
        days=["2026-01-02"],
    )

    assert primary.iloc[0]["panel"] == "grade_a_primary"
    assert primary.iloc[0]["evaluated_days"] == 1
    assert sensitivity.iloc[0]["panel"] == "grade_b_sensitivity"
    assert sensitivity.iloc[0]["evaluated_days"] == 1


def test_side_path_difference_accepts_plain_mappings() -> None:
    key = '["SELL",1,101.0,"fill",2,0.001,0.0]'
    assert _path_difference(Counter({key: 2}), Counter({key: 1}), side="SELL") == {
        "candidate_only_order_outcomes": 0,
        "control_only_order_outcomes": 1,
    }


def test_diagnostic_control_equivalence_uses_only_evaluated_frozen_days() -> None:
    summary = _control_equivalence_summary(
        [
            {
                "day": "2026-04-17",
                "control_equivalence_checked": True,
                "control_equivalence_passed": True,
            }
        ],
        frozen_days=("2026-04-17", "2026-06-06", "2026-06-26"),
        evaluated_days=("2026-04-17",),
    )

    assert summary == {
        "frozen_days": ["2026-04-17", "2026-06-06", "2026-06-26"],
        "required_days_for_run": ["2026-04-17"],
        "checked_days": ["2026-04-17"],
        "missing_required_days": [],
        "failed_days": [],
        "passed": True,
        "scope": "evaluated_frozen_equivalence_days",
    }


def test_control_equivalence_does_not_pass_without_an_evaluated_check_day() -> None:
    summary = _control_equivalence_summary(
        [],
        frozen_days=("2026-04-17", "2026-06-06", "2026-06-26"),
        evaluated_days=("2026-04-18",),
    )

    assert summary["required_days_for_run"] == []
    assert summary["passed"] is None


def test_full_control_equivalence_still_requires_every_frozen_check_day() -> None:
    summary = _control_equivalence_summary(
        [
            {
                "day": "2026-04-17",
                "control_equivalence_checked": True,
                "control_equivalence_passed": True,
            }
        ],
        frozen_days=("2026-04-17", "2026-06-06", "2026-06-26"),
        evaluated_days=("2026-04-17", "2026-06-06", "2026-06-26"),
    )

    assert summary["missing_required_days"] == ["2026-06-06", "2026-06-26"]
    assert summary["passed"] is False


def test_skipped_source_rehash_preserves_declared_manifest_identity() -> None:
    spec = {
        "source_identity": {
            "market_manifest_canonical_sha256": "a" * 64,
            "market_manifest_rows": 1222,
            "market_manifest_bytes": 11625657192,
        }
    }

    assert _market_source_manifest_identity(spec, None) == {
        "canonical_sha256": "a" * 64,
        "rows": 1222,
        "bytes": 11625657192,
        "rehash_performed": False,
        "entries_materialized": False,
        "verification_status": "declared_frozen_identity_not_rehashed",
    }


def test_rehashed_source_manifest_must_match_all_declared_dimensions() -> None:
    manifest = [{"bytes": 7, "path": "/tmp/a", "sha256": "b" * 64}]
    spec = {
        "source_identity": {
            "market_manifest_canonical_sha256": canonical_sha256(manifest),
            "market_manifest_rows": 1,
            "market_manifest_bytes": 7,
        }
    }
    identity = _market_source_manifest_identity(spec, manifest)
    assert identity["rehash_performed"] is True
    assert identity["verification_status"] == "rehashed_and_matched_frozen_identity"

    spec["source_identity"]["market_manifest_rows"] = 2
    with pytest.raises(ValueError, match="rows expected 2, found 1"):
        _market_source_manifest_identity(spec, manifest)
