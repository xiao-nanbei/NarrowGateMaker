from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_final_refit_v1 as final_refit,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 import (
    ProspectiveFoldManifest,
    SuccessorSearchProfile,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    TriLiteral,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_persistent_policy_v3_inference import (
    SimultaneousBand,
    SimultaneousBandFamily,
)


def _family(values: dict[str, float]) -> SimultaneousBandFamily:
    return SimultaneousBandFamily(
        bands={
            name: SimultaneousBand(
                hypothesis=name,
                mean_usdc=value + 0.1,
                standard_error_usdc=0.01,
                lcb_usdc=value,
                ucb_usdc=value + 0.2,
                day_count=20,
            )
            for name, value in values.items()
        },
        critical_value=2.0,
        confidence=0.95,
        draws=99,
        seed=17,
        shared_days=("2026-08-23", "2026-08-24"),
        multiplier_support=(-1.0, 1.0),
    )


def _candidate_report() -> dict[str, object]:
    return {
        "outer_test_days": 20,
        "identified_days": 20,
        "feature_ready_active_days": 20,
        "nonbaseline_action_count": 100,
        "nonbaseline_action_rate": 0.5,
        "daily_positive_rate": 0.6,
        "unsupported_mass": 0.0,
        "common_row_count": 200,
        "common_campaign_count": 100,
        "paired_effective_sample_size": 100.0,
        "minimum_behavior_propensity": 1.0,
        "overlap_violations": 0,
        "fill_retention": 1.0,
        "leave_one_top_day": {"mean_usdc": 0.02},
        "leave_two_top_days": {"mean_usdc": 0.01},
        "risk_and_accounting": {
            "closed_campaign_delta_usdc_equal_day_mean": 0.02,
            "campaign_q10_delta_usdc_equal_day_mean": 0.01,
            "campaign_cvar10_delta_usdc_equal_day_mean": 0.01,
            "inventory_time_delta_btc_s_equal_day_mean": -1.0,
            "max_abs_inventory_delta_btc_equal_day_mean": -0.001,
        },
    }


def _fold_record(*, true_m2: bool = True) -> dict[str, object]:
    predicate = (
        "predicate::trade_flow::ema_h1_h8_cross"
        if true_m2
        else "predicate::ema_pair_h1s_h8s:cross_recent"
    )
    candidates = {}
    for candidate in nested.LEARNED_BOOLEAN_ORDER:
        candidate_predicate = predicate if candidate == "M2_TRUE_INCREMENTAL" else (
            "predicate::ema_pair_h1s_h8s:cross_recent"
        )
        candidates[candidate] = {
            "selected_profile": "bounded_full_universe_v1",
            "policy": {
                "decision_policy": {
                    "ordered_first_match_rules": [
                        {
                            "action": "FIXED_79S",
                            "clauses": [
                                {"literals": [{"predicate": candidate_predicate}]}
                            ],
                        }
                    ]
                }
            },
        }
    return {"side": "SELL", "candidates": candidates}


def _result(
    *,
    comparison_overrides: dict[str, float] | None = None,
    true_m2_folds: int = 4,
) -> nested.NestedOofExecutionResult:
    candidate_values = {
        f"SELL:{candidate}": 0.05 for candidate in nested.LEARNED_BOOLEAN_ORDER
    }
    comparison_values: dict[str, float] = {}
    for label, _, _ in nested.CONFIRMATORY_COMPARISONS:
        comparison_values[f"successor:SELL:{label}"] = (
            -0.05 if label.startswith("CONTINUOUS-") else 0.05
        )
    comparison_values.update(comparison_overrides or {})
    reports = {
        f"SELL:{candidate}": _candidate_report()
        for candidate in nested.LEARNED_BOOLEAN_ORDER
    }
    folds = tuple(
        _fold_record(true_m2=index < true_m2_folds) for index in range(4)
    )
    candidate_family = _family(candidate_values)
    confirmatory_family = _family(comparison_values)
    risk_family = _family(
        {
            f"SELL:{candidate}:{metric}": 0.05
            for candidate in nested.LEARNED_BOOLEAN_ORDER
            for metric in nested.RISK_METRIC_COLUMNS
        }
    )
    empty_family = _family({"unused": -0.1})
    return nested.NestedOofExecutionResult(
        oof_rows=pd.DataFrame(),
        fold_records=folds,
        candidate_reports=reports,
        stability={},
        candidate_bands=candidate_family,
        candidate_week_bands=candidate_family,
        hierarchy_bands=empty_family,
        hierarchy_week_bands=empty_family,
        confirmatory_bands=confirmatory_family,
        confirmatory_week_bands=confirmatory_family,
        risk_bands=risk_family,
        risk_week_bands=risk_family,
        scorecards={
            f"SELL:{candidate}": {
                "hard_gates": {"passed": True, "failures": []},
                "ranking_eligible": True,
                "ranking_score": 0.5,
            }
            for candidate in nested.LEARNED_BOOLEAN_ORDER
        },
        hierarchy={},
    )


def test_selects_highest_supported_incremental_rung() -> None:
    selection = final_refit.select_final_refit_candidate(_result(), side="SELL")

    assert selection.selected_candidate == "M2_TRUE_INCREMENTAL"
    assert selection.advancement_path == nested.LEARNED_BOOLEAN_ORDER
    assert selection.learning_algorithm_oof_supported is True
    assert selection.exact_final_artifact_oof_available is False


def test_e1_failure_abstains_instead_of_refitting() -> None:
    result = _result(
        comparison_overrides={
            "successor:SELL:E1_FULL_EMA_BANK-ACTION_MATCHED": -0.01
        }
    )
    selection = final_refit.select_final_refit_candidate(result, side="SELL")

    assert selection.selected_candidate is None
    assert selection.advancement_path == ()
    assert selection.learning_algorithm_oof_supported is False


def test_continuous_dominance_blocks_corresponding_boolean_rung() -> None:
    result = _result(
        comparison_overrides={
            "successor:SELL:CONTINUOUS-E1_FULL_EMA_BANK": 0.01
        }
    )
    selection = final_refit.select_final_refit_candidate(result, side="SELL")

    assert selection.selected_candidate is None
    assert any(
        check.name.endswith("CONTINUOUS-E1_FULL_EMA_BANK:day_not_dominated")
        and not check.passed
        for check in selection.checks
    )


def test_m2_name_requires_true_incremental_features_in_every_outer_policy() -> None:
    selection = final_refit.select_final_refit_candidate(
        _result(true_m2_folds=3),
        side="SELL",
    )

    assert selection.selected_candidate == "E3_HIGHER_ORDER_BOOLEAN"
    assert selection.advancement_path[-1] == "E3_HIGHER_ORDER_BOOLEAN"


def test_oof_identity_drift_fails_closed() -> None:
    result = replace(_result(), final_refit_performed=True)
    with pytest.raises(final_refit.FinalRefitError, match="identity drifted"):
        final_refit.select_final_refit_candidate(result, side="SELL")


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_compiled_candidate_has_research_runtime_tristate_parity(side: str) -> None:
    action = "FIXED_79S"
    policy = BooleanCooldownPolicy(
        side=side,
        rules=(
            BooleanRule(
                action=action,
                clauses=(
                    AndClause(
                        (
                            TriLiteral("predicate::ema_pair_h1s_h8s:cross_recent"),
                            TriLiteral("predicate::flow::death_cross", negated=True),
                        )
                    ),
                ),
            ),
        ),
    )
    fitted = nested.FittedCandidate(
        ladder_name="E2_DIRECTIONAL_EMA",
        side=side,
        policy=policy,
        selected_profile="bounded_full_universe_v1",
        training_days=("2026-08-13",),
        training_row_sha256="1" * 64,
        policy_payload={"decision_policy": policy.payload()},
        policy_sha256="2" * 64,
        fit_audit={},
        feature_pool_audit={},
    )

    audit = final_refit.audit_compiled_policy_research_runtime_parity(
        fitted,
        predicate_bundle_sha256="3" * 64,
    )

    assert audit.mismatch_count == 0
    assert audit.deterministic_case_count >= 8
    assert audit.warmup_fallback_valid is True
    assert audit.stale_fallback_valid is True
    assert audit.missing_column_fallback_valid is True
    assert audit.active_live_wiring_changed is False


def test_refit_artifact_has_no_exact_artifact_oof_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    result = _result(
        comparison_overrides={
            "successor:SELL:E2-E1": -0.01,
        }
    )
    profile = SuccessorSearchProfile()
    entry = nested.CandidateLadderEntry(
        "E1_FULL_EMA_BANK",
        "boolean",
        feature_names_by_side={"SELL": ("predicate::ema_pair_h1s_h8s:cross_recent",)},
        profiles=(profile,),
    )
    policy = BooleanCooldownPolicy(
        side="SELL",
        rules=(
            BooleanRule(
                action="FIXED_79S",
                clauses=(
                    AndClause(
                        (TriLiteral("predicate::ema_pair_h1s_h8s:cross_recent"),)
                    ),
                ),
            ),
        ),
    )
    fitted = nested.FittedCandidate(
        ladder_name="E1_FULL_EMA_BANK",
        side="SELL",
        policy=policy,
        selected_profile=profile.name,
        training_days=("2026-08-13",),
        training_row_sha256="1" * 64,
        policy_payload={"decision_policy": policy.payload()},
        policy_sha256="2" * 64,
        fit_audit={},
        feature_pool_audit={},
    )
    monkeypatch.setattr(final_refit, "_fit_boolean_candidate", lambda *args, **kwargs: fitted)

    days = tuple(
        (pd.Timestamp("2026-08-13") + pd.Timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(30)
    )
    folds = ProspectiveFoldManifest(
        active_days=days,
        outer_folds=(),
        manifest_sha256="3" * 64,
    )

    class Panel:
        metadata = pd.DataFrame({"side": ["SELL"]}, index=["row-1"])

        @staticmethod
        def validate(**kwargs) -> None:
            return None

    bundle = final_refit.refit_and_freeze_final_artifact(
        Panel(),  # type: ignore[arg-type]
        fold_manifest=folds,
        ladder=(entry,),
        result=result,
        side="SELL",
        locked_at_utc="2026-09-12T00:00:00Z",
    )

    assert bundle.artifact["exact_final_artifact_oof_available"] is False
    assert bundle.artifact["permissions"]["action_authorized"] is False
    assert bundle.fitted_candidate.learning_algorithm_fold_specific is False
    output = tmp_path / "final-artifact.json"
    receipt_sha = final_refit.write_final_refit_artifact(bundle, output)
    assert len(receipt_sha) == 64
    assert output.exists()
