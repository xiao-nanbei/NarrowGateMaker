from __future__ import annotations

from collections.abc import Mapping, Sequence

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    TriState,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    ChronologicalFold,
    NestedOofContractError,
    SearchConfig,
    TriLiteral,
    duration_vocabulary,
    evaluate_post_oof_deployment_gate,
    generate_bounded_candidates,
    prepare_long_form_panel,
    run_feature_block_comparison,
    run_nested_chronological_oof,
)


def _one_literal_policy(side: str, predicate: str, action_index: int) -> BooleanCooldownPolicy:
    return BooleanCooldownPolicy(
        side=side,
        rules=(
            BooleanRule(
                action=duration_vocabulary(side)[action_index],
                clauses=(AndClause((TriLiteral(predicate),)),),
            ),
        ),
    )


def _synthetic_long_form(
    *,
    days: Sequence[str] | None = None,
    panel_roles: Mapping[str, str] | None = None,
    uplift_by_role: Mapping[str, float] | None = None,
    predicate_by_role: Mapping[str, TriState] | None = None,
) -> pd.DataFrame:
    selected_days = tuple(days or (f"2026-01-{day:02d}" for day in range(1, 9)))
    rows: list[dict[str, object]] = []
    for side in ("BUY", "SELL"):
        vocabulary = duration_vocabulary(side)
        for day_index, day in enumerate(selected_days):
            campaign = f"{side}-{day}"
            for opportunity_index, role in enumerate(("opener", "add")):
                opportunity = f"{campaign}-{opportunity_index}"
                for action_index, action in enumerate(vocabulary):
                    # The least-bad nonbaseline action is still economically
                    # negative. This is intentional: exploration must execute
                    # it in outer OOF and let only the later gate abstain.
                    if action_index == 0:
                        value = 0.0
                    elif uplift_by_role is None:
                        value = -float(action_index)
                    else:
                        value = float(uplift_by_role[role]) * action_index
                    rows.append(
                        {
                            "opportunity_id": opportunity,
                            "utc_day": day,
                            "panel_role": (
                                panel_roles[day] if panel_roles is not None else "prefix40"
                            ),
                            "side": side,
                            "role_at_fill": role,
                            "campaign_id": campaign,
                            "duration_policy_id": action,
                            "terminal_value_usdc": value,
                            "strict_native_label": True,
                            "p_r0": int(
                                predicate_by_role[role]
                                if predicate_by_role is not None
                                else TriState.TRUE
                            ),
                            "p_m0": int(TriState.TRUE),
                            "p_m1": int(TriState.TRUE if day_index % 2 == 0 else TriState.FALSE),
                            "p_m2": int(TriState.TRUE),
                            "p_unknown": int(TriState.UNOBSERVED),
                        }
                    )
    return pd.DataFrame(rows)


def _outer_folds() -> tuple[ChronologicalFold, ...]:
    return (
        ChronologicalFold(
            "outer1",
            ("2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"),
            ("2026-01-05",),
        ),
        ChronologicalFold(
            "outer2",
            (
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
                "2026-01-04",
                "2026-01-05",
            ),
            ("2026-01-06", "2026-01-07"),
        ),
    )


def _small_search() -> SearchConfig:
    return SearchConfig(
        max_literals_per_clause=1,
        max_clauses_per_rule=1,
        max_rules_per_policy=1,
        max_clause_candidates=4,
        max_rule_candidates=4,
        max_policy_candidates=14,
        inner_folds=2,
        inner_minimum_train_days=2,
        minimum_action_opportunities=1,
        minimum_action_campaigns=1,
        minimum_action_days=1,
    )


def test_three_valued_boolean_and_ordered_first_match_are_fail_closed() -> None:
    assert TriLiteral("a", negated=True).evaluate_state(TriState.UNOBSERVED) is (
        TriState.UNOBSERVED
    )
    conjunction = AndClause((TriLiteral("a"), TriLiteral("b")))
    assert conjunction.evaluate_state({"a": 1, "b": -1}) is TriState.UNOBSERVED
    union = BooleanRule(
        action="FIXED_79S",
        clauses=(
            AndClause((TriLiteral("a"),)),
            AndClause((TriLiteral("b"),)),
        ),
    )
    assert union.evaluate_state({"a": 0, "b": 1}) is TriState.TRUE

    policy = BooleanCooldownPolicy(
        side="BUY",
        rules=(
            BooleanRule(
                action="FIXED_79S",
                clauses=(AndClause((TriLiteral("a", negated=True),)),),
            ),
            BooleanRule(
                action="FIXED_173S",
                clauses=(AndClause((TriLiteral("b"),)),),
            ),
        ),
    )
    unresolved = pd.DataFrame({"a": [-1], "b": [1]})
    assert policy.choose(unresolved).tolist() == ["CONTROL_85N"]
    first_false_second_true = pd.DataFrame({"a": [1], "b": [1]})
    assert policy.choose(first_false_second_true).tolist() == ["FIXED_173S"]


def test_bounded_universe_contains_and_or_and_ordered_rules() -> None:
    config = SearchConfig(
        max_literals_per_clause=2,
        max_clauses_per_rule=2,
        max_rules_per_policy=2,
        max_clause_candidates=12,
        max_rule_candidates=20,
        max_policy_candidates=100,
        inner_folds=1,
        inner_minimum_train_days=1,
        minimum_action_opportunities=1,
        minimum_action_campaigns=1,
        minimum_action_days=1,
    )
    candidates = generate_bounded_candidates(
        side="BUY",
        predicate_columns=("a", "b"),
        config=config,
    )
    assert any(
        len(clause.literals) == 2
        for policy in candidates
        for rule in policy.rules
        for clause in rule.clauses
    )
    assert any(len(rule.clauses) == 2 for policy in candidates for rule in policy.rules)
    assert any(len(policy.rules) == 2 for policy in candidates)
    assert all(policy.side == "BUY" for policy in candidates)
    assert all(policy.default_action == "CONTROL_85N" for policy in candidates)
    assert len(candidates) <= config.max_policy_candidates

    actions = [rule.action for policy in candidates for rule in policy.rules]
    action_counts = {action: actions.count(action) for action in duration_vocabulary("BUY")[1:]}
    assert set(actions) == set(duration_vocabulary("BUY")[1:])
    assert max(action_counts.values()) - min(action_counts.values()) <= 1

    clauses = {
        clause.key for policy in candidates for rule in policy.rules for clause in rule.clauses
    }
    bodies = {
        tuple(clause.key for clause in rule.clauses)
        for policy in candidates
        for rule in policy.rules
    }
    assert len(clauses) <= config.max_clause_candidates
    assert len(bodies) <= config.max_rule_candidates
    for predicate in ("a", "b"):
        assert {
            literal.negated
            for policy in candidates
            for rule in policy.rules
            for clause in rule.clauses
            for literal in clause.literals
            if literal.predicate == predicate
        } == {False, True}


def test_stratification_prevents_lexical_channel_starvation_and_is_deterministic() -> None:
    config = SearchConfig(
        max_literals_per_clause=2,
        max_clauses_per_rule=2,
        max_rules_per_policy=2,
        max_clause_candidates=10,
        max_rule_candidates=12,
        max_policy_candidates=28,
        inner_folds=1,
        inner_minimum_train_days=1,
        minimum_action_opportunities=1,
        minimum_action_campaigns=1,
        minimum_action_days=1,
    )

    def build(
        prefixes: tuple[str, str, str],
    ) -> tuple[tuple[BooleanCooldownPolicy, ...], dict[str, str]]:
        predicates = tuple(
            f"{prefix}_{semantic}"
            for prefix in prefixes
            for semantic in ("ordering", "cross", "slope")
        )
        channel_by_prefix = dict(zip(prefixes, ("mid", "flow", "depth"), strict=True))
        channels = {
            predicate: channel_by_prefix[predicate.rsplit("_", 1)[0]] for predicate in predicates
        }
        semantics = {predicate: predicate.rsplit("_", 1)[1] for predicate in predicates}
        candidates = generate_bounded_candidates(
            side="SELL",
            predicate_columns=tuple(reversed(predicates)),
            config=config,
            predicate_channel_groups=channels,
            predicate_semantic_groups=semantics,
        )
        repeated = generate_bounded_candidates(
            side="SELL",
            predicate_columns=predicates,
            config=config,
            predicate_channel_groups=channels,
            predicate_semantic_groups=semantics,
        )
        assert tuple(policy.candidate_id for policy in candidates) == tuple(
            policy.candidate_id for policy in repeated
        )
        return candidates, channels

    late_named, late_channels = build(("aaa_book", "mmm_trade", "zzz_depth"))
    renamed, renamed_channels = build(("zzz_book", "aaa_trade", "mmm_depth"))

    def represented_channels(
        candidates: Sequence[BooleanCooldownPolicy], channels: Mapping[str, str]
    ) -> set[str]:
        return {
            channels[literal.predicate]
            for policy in candidates
            for rule in policy.rules
            for clause in rule.clauses
            for literal in clause.literals
        }

    assert represented_channels(late_named, late_channels) == {"mid", "flow", "depth"}
    assert represented_channels(renamed, renamed_channels) == {"mid", "flow", "depth"}


def test_clock_group_mapping_allows_mixed_book_trade_clause_at_common_cutoff() -> None:
    predicates = ("book_order", "book_cross", "trade_order", "trade_cross", "context")
    clock_groups = {
        "book_order": "book",
        "book_cross": "book",
        "trade_order": "trade",
        "trade_cross": "trade",
        "context": "context",
    }
    candidates = generate_bounded_candidates(
        side="BUY",
        predicate_columns=predicates,
        config=SearchConfig(
            max_literals_per_clause=2,
            max_clauses_per_rule=2,
            max_rules_per_policy=2,
            max_clause_candidates=16,
            max_rule_candidates=20,
            max_policy_candidates=42,
            inner_folds=1,
            inner_minimum_train_days=1,
            minimum_action_opportunities=1,
            minimum_action_campaigns=1,
            minimum_action_days=1,
        ),
        predicate_channel_groups={
            predicate: predicate.split("_", 1)[0] for predicate in predicates
        },
        predicate_semantic_groups={
            predicate: predicate.rsplit("_", 1)[-1] for predicate in predicates
        },
        predicate_clock_groups=clock_groups,
    )
    assert any(
        len(clause.literals) == 2
        for policy in candidates
        for rule in policy.rules
        for clause in rule.clauses
    )
    assert any(
        {
            clock_groups[literal.predicate]
            for literal in clause.literals
            if clock_groups[literal.predicate] in {"book", "trade"}
        }
        == {"book", "trade"}
        for policy in candidates
        for rule in policy.rules
        for clause in rule.clauses
    )


def test_clock_group_mapping_requires_complete_accepted_values() -> None:
    config = SearchConfig(
        max_literals_per_clause=2,
        max_clauses_per_rule=1,
        max_rules_per_policy=1,
        max_clause_candidates=8,
        max_rule_candidates=8,
        max_policy_candidates=14,
        inner_folds=1,
        inner_minimum_train_days=1,
        minimum_action_opportunities=1,
        minimum_action_campaigns=1,
        minimum_action_days=1,
    )
    with pytest.raises(NestedOofContractError, match="mapping is missing"):
        generate_bounded_candidates(
            side="BUY",
            predicate_columns=("book", "trade"),
            config=config,
            predicate_clock_groups={"book": "book"},
        )
    with pytest.raises(NestedOofContractError, match="invalid predicate clock"):
        generate_bounded_candidates(
            side="BUY",
            predicate_columns=("book", "trade"),
            config=config,
            predicate_clock_groups={"book": "book", "trade": "receive"},
        )


def test_negative_inner_candidate_reaches_outer_oof_and_gate_alone_abstains() -> None:
    table = _synthetic_long_form()
    least_bad = _one_literal_policy("BUY", "p_r0", 1)
    worse = _one_literal_policy("BUY", "p_r0", 2)
    result = run_nested_chronological_oof(
        table,
        side="BUY",
        feature_block="R0",
        panel_scope="prefix40",
        predicate_columns=("p_r0",),
        outer_folds=_outer_folds(),
        search_config=_small_search(),
        candidate_policies=(least_bad, worse),
    )

    assert result.candidate_ids == (least_bad.candidate_id, least_bad.candidate_id)
    assert all(fold.inner_estimate.lcb_usdc < 0.0 for fold in result.folds)
    assert all(not fold.candidate_was_replaced_by_baseline for fold in result.folds)
    assert result.oof_rows["candidate_id"].eq(least_bad.candidate_id).all()
    assert result.oof_rows["selected_action"].eq("FIXED_79S").all()
    assert result.oof_rows["uplift_usdc"].eq(-1.0).all()
    assert result.estimate.mean_usdc == pytest.approx(-1.0)
    assert result.role_support["opener"]["action_opportunities"] > 0
    assert result.role_support["add"]["action_opportunities"] > 0
    for role in ("opener", "add"):
        audit = result.role_support[role]
        interval = audit["campaign_day_clustered_uplift_interval"]
        assert audit["policy_scope"] == ("one_shared_policy_per_side_roles_are_audit_only")
        assert audit["action_campaigns"] == 3
        assert audit["action_days"] == 3
        assert interval["mean_usdc"] == pytest.approx(-1.0)
        assert interval["interval_cluster_contract"] == (
            "utc_day_cluster_over_campaign_weighted_rows"
        )
        assert audit["tail_diagnostics"]["campaign"]["uplift"]["q10_usdc"] == pytest.approx(-1.0)
        assert audit["tail_diagnostics"]["utc_day"]["uplift"]["cvar10_usdc"] == pytest.approx(-1.0)
    assert result.permissions == {
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }

    # Two opportunities in each campaign receive 0.5 each, so every campaign
    # has total model weight exactly one.
    campaign_weights = result.oof_rows.groupby("campaign_cluster_id")["campaign_weight"].sum()
    assert campaign_weights.eq(1.0).all()

    gate = evaluate_post_oof_deployment_gate(result)
    assert gate.passed is False
    assert gate.decision == "abstain"
    assert "terminal_value_lcb_not_above_economic_epsilon" in gate.reasons
    assert gate.action_authorized is False
    assert gate.live_authorized is False


def test_zero_action_outer_fold_is_recorded_and_fails_only_post_oof_gate() -> None:
    table = _synthetic_long_form(uplift_by_role={"opener": 1.0, "add": 1.0})
    table.loc[table["utc_day"] == "2026-01-05", "p_r0"] = int(TriState.FALSE)
    policy = _one_literal_policy("BUY", "p_r0", 1)
    result = run_nested_chronological_oof(
        table,
        side="BUY",
        feature_block="R0",
        panel_scope="prefix40",
        predicate_columns=("p_r0",),
        outer_folds=_outer_folds(),
        search_config=_small_search(),
        candidate_policies=(policy,),
    )

    assert result.folds[0].outer_support.action_opportunities == 0
    assert result.folds[0].oof_rows["selected_nonbaseline"].eq(False).all()  # noqa: E712
    assert result.folds[1].outer_support.action_opportunities == 4
    assert result.combined_support.action_opportunities == 4
    assert result.combined_support.action_campaigns == 2
    assert result.combined_support.action_days == 2
    assert len(result.oof_rows) == 6

    gate = evaluate_post_oof_deployment_gate(result)
    assert gate.passed is False
    assert gate.zero_action_outer_folds == ("outer1",)
    assert "outer_fold_without_nonbaseline_action" in gate.reasons
    assert gate.outer_fold_support["outer1"] == result.folds[0].outer_support


def test_deployment_support_minimums_use_acted_campaigns_and_days() -> None:
    table = _synthetic_long_form(uplift_by_role={"opener": 1.0, "add": 1.0})
    table.loc[table["utc_day"] == "2026-01-06", "p_r0"] = int(TriState.FALSE)
    policy = _one_literal_policy("BUY", "p_r0", 1)
    folds = (
        ChronologicalFold(
            "outer1",
            ("2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"),
            ("2026-01-05", "2026-01-06"),
        ),
    )
    result = run_nested_chronological_oof(
        table,
        side="BUY",
        feature_block="R0",
        panel_scope="prefix40",
        predicate_columns=("p_r0",),
        outer_folds=folds,
        search_config=_small_search(),
        candidate_policies=(policy,),
    )

    assert result.estimate.campaigns == 2
    assert result.estimate.days == 2
    assert result.combined_support.action_campaigns == 1
    assert result.combined_support.action_days == 1
    gate = evaluate_post_oof_deployment_gate(
        result,
        minimum_campaigns=2,
        minimum_days=2,
    )
    assert "campaign_support_below_minimum" in gate.reasons
    assert "day_support_below_minimum" in gate.reasons


def test_role_field_is_mandatory_and_missing_values_fail_closed() -> None:
    table = _synthetic_long_form()
    with pytest.raises(NestedOofContractError, match="required role field"):
        prepare_long_form_panel(
            table.drop(columns=["role_at_fill"]),
            side="BUY",
            panel_scope="prefix40",
            predicate_columns=("p_r0",),
        )

    missing = table.copy()
    missing.loc[missing.index[0], "role_at_fill"] = None
    with pytest.raises(NestedOofContractError, match="role field contains a missing"):
        prepare_long_form_panel(
            missing,
            side="BUY",
            panel_scope="prefix40",
            predicate_columns=("p_r0",),
        )


def test_role_gate_rejects_harm_hidden_by_positive_side_pooled_estimate() -> None:
    table = _synthetic_long_form(uplift_by_role={"opener": 3.0, "add": -1.0})
    policy = _one_literal_policy("BUY", "p_r0", 1)
    result = run_nested_chronological_oof(
        table,
        side="BUY",
        feature_block="R0",
        panel_scope="prefix40",
        predicate_columns=("p_r0",),
        outer_folds=_outer_folds(),
        search_config=_small_search(),
        candidate_policies=(policy,),
    )

    assert result.estimate.lcb_usdc > 0.0
    assert result.role_support["opener"]["campaign_day_clustered_uplift_interval"]["lcb_usdc"] > 0.0
    assert result.role_support["add"]["campaign_day_clustered_uplift_interval"]["ucb_usdc"] < 0.0

    gate = evaluate_post_oof_deployment_gate(result)
    assert gate.passed is False
    assert gate.required_roles == ("opener", "add")
    assert gate.role_gates["opener"]["passed"] is True
    assert gate.role_gates["add"]["no_severe_negative_uplift"] is False
    assert gate.role_gates["add"]["campaign_q10_noninferior"] is False
    assert gate.role_gates["add"]["campaign_cvar10_noninferior"] is False
    assert "add_severe_negative_uplift_interval" in gate.reasons
    assert "add_campaign_q10_worsened" in gate.reasons
    assert "add_campaign_cvar10_worsened" in gate.reasons


def test_role_gate_requires_action_support_without_role_specific_training() -> None:
    table = _synthetic_long_form(
        uplift_by_role={"opener": 3.0, "add": -1.0},
        predicate_by_role={"opener": TriState.TRUE, "add": TriState.FALSE},
    )
    policy = _one_literal_policy("SELL", "p_r0", 1)
    result = run_nested_chronological_oof(
        table,
        side="SELL",
        feature_block="R0",
        panel_scope="prefix40",
        predicate_columns=("p_r0",),
        outer_folds=_outer_folds(),
        search_config=_small_search(),
        candidate_policies=(policy,),
    )

    assert result.candidate_ids == (policy.candidate_id, policy.candidate_id)
    assert result.role_support["opener"]["action_rate"] == pytest.approx(1.0)
    assert result.role_support["add"]["action_rate"] == pytest.approx(0.0)
    gate = evaluate_post_oof_deployment_gate(result)
    assert gate.passed is False
    assert "add_action_opportunity_support_below_minimum" in gate.reasons
    assert "add_action_campaign_support_below_minimum" in gate.reasons
    assert "add_action_day_support_below_minimum" in gate.reasons


def test_panel_scopes_preserve_added10_as_late_diagnostic() -> None:
    days = tuple(f"2026-01-{day:02d}" for day in range(1, 9))
    roles = {day: "prefix40" if index < 6 else "added10" for index, day in enumerate(days)}
    table = _synthetic_long_form(days=days, panel_roles=roles)
    pooled = prepare_long_form_panel(
        table,
        side="SELL",
        panel_scope="pooled50",
        predicate_columns=("p_r0",),
    )
    late = prepare_long_form_panel(
        table,
        side="SELL",
        panel_scope="added10",
        predicate_columns=("p_r0",),
    )
    assert pooled.panel_role_counts == {"added10": 4, "prefix40": 12}
    assert late.panel_role_counts == {"added10": 4}
    with pytest.raises(NestedOofContractError, match="Validation/holdout"):
        prepare_long_form_panel(
            table,
            side="SELL",
            panel_scope="validation",
            predicate_columns=("p_r0",),
        )


def test_feature_blocks_and_sides_are_reported_without_pooled_policy() -> None:
    table = _synthetic_long_form()
    comparison = run_feature_block_comparison(
        table,
        panel_scope="prefix40",
        predicate_columns_by_block={
            "R0": ("p_r0",),
            "M0": ("p_m0",),
            "M1": ("p_m0", "p_m1"),
            "M2": ("p_m0", "p_m1", "p_m2"),
        },
        outer_folds=_outer_folds(),
        search_config=SearchConfig(
            max_literals_per_clause=1,
            max_clauses_per_rule=1,
            max_rules_per_policy=1,
            max_clause_candidates=2,
            max_rule_candidates=2,
            max_policy_candidates=7,
            inner_folds=2,
            inner_minimum_train_days=2,
            minimum_action_opportunities=1,
            minimum_action_campaigns=1,
            minimum_action_days=1,
        ),
    )
    assert set(comparison.results) == {"BUY", "SELL"}
    assert all(set(blocks) == {"R0", "M0", "M1", "M2"} for blocks in comparison.results.values())
    assert all(
        result.side == side
        for side, blocks in comparison.results.items()
        for result in blocks.values()
    )
    assert comparison.pooled_side_policy_created is False
    assert comparison.action_authorized is False
    assert comparison.live_authorized is False
    with pytest.raises(NestedOofContractError, match="pooled-side"):
        run_nested_chronological_oof(
            table,
            side="POOLED",
            feature_block="R0",
            panel_scope="prefix40",
            predicate_columns=("p_r0",),
            outer_folds=_outer_folds(),
            search_config=_small_search(),
        )


def test_exact_duration_vocabulary_is_required_per_opportunity() -> None:
    table = _synthetic_long_form()
    first = table.loc[table["side"] == "BUY", "opportunity_id"].iloc[0]
    action = duration_vocabulary("BUY")[-1]
    broken = table.loc[
        ~(
            (table["side"] == "BUY")
            & (table["opportunity_id"] == first)
            & (table["duration_policy_id"] == action)
        )
    ]
    with pytest.raises(NestedOofContractError, match="exact side-specific"):
        prepare_long_form_panel(
            broken,
            side="BUY",
            panel_scope="prefix40",
            predicate_columns=("p_r0",),
        )
