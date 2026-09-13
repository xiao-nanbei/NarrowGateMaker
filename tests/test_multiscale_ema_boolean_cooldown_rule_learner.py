from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_rule_learner as learner,
)


def _synthetic_panel(
    *,
    side: str = "SELL",
    days: int = 8,
    campaigns_per_day: int = 10,
    ordered_days: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    contract = learner.load_frozen_search_contract()
    actions = contract.side_actions[side]
    rows: list[dict[str, object]] = []
    start = pd.Timestamp("2026-01-01", tz="UTC")
    ordinal = 0
    day_values = ordered_days or tuple(
        (start + pd.Timedelta(days=index)).strftime("%Y-%m-%d") for index in range(days)
    )
    for day_text in day_values:
        day = pd.Timestamp(day_text, tz="UTC")
        for campaign_index in range(campaigns_per_day):
            # Balanced truth table with nuisance combinations.  Neither a, b,
            # c nor d alone identifies the profitable state.
            bits = ordinal % 16
            a = bool(bits & 1)
            b = bool(bits & 2)
            c = bool(bits & 4)
            d = bool(bits & 8)
            target = (a and b) or (c and not d)
            opportunity_id = f"{side}-{day_text}-{campaign_index:03d}"
            assignment = int(day.timestamp() * 1_000_000_000) + (campaign_index + 1) * 1_000_000_000
            for action_index, action in enumerate(actions):
                value = 0.0
                if action == "FIXED_79S":
                    value = 5.0 if target else -5.0
                elif action != learner.CONTROL_ACTION:
                    value = -2.0 - 0.01 * action_index
                rows.append(
                    {
                        "opportunity_id": opportunity_id,
                        "side": side,
                        "utc_day": day_text,
                        "campaign_side_id": f"{side}-{day_text}-{campaign_index:03d}",
                        "assignment_ts_ns": assignment,
                        "washout_ts_ns": assignment + 500_000_000,
                        "joint_censored": False,
                        "candidate_policy_id": action,
                        learner.OUTCOME_VALUE_COLUMN: value,
                        "predicate::a": a,
                        "predicate::b": b,
                        "predicate::c": c,
                        "predicate::d": d,
                        "runner_metadata": "ignored-by-model",
                    }
                )
            ordinal += 1
    panel = pd.DataFrame(rows)
    return panel


def _with_frozen_predicate_schema(panel: pd.DataFrame) -> pd.DataFrame:
    contract = learner.load_frozen_search_contract()
    base = panel.drop(columns=[column for column in panel if column.startswith("predicate::")])
    predicates = pd.DataFrame(
        {
            name: np.full(len(base), index % 2 == 0, dtype=bool)
            for index, name in enumerate(contract.predicate_columns)
        },
        index=base.index,
    )
    cross_validity = pd.DataFrame(
        {
            (
                name.removeprefix("predicate::")
                .removesuffix(":last_cross_favorable")
                + "_cross_missing"
            ): np.zeros(len(base), dtype=bool)
            for name in contract.predicate_columns
            if name.endswith(":last_cross_favorable")
        },
        index=base.index,
    )
    return pd.concat([base, predicates, cross_validity], axis=1)


def _expected_formal_identity() -> learner.FormalInputIdentity:
    contract = learner.load_frozen_search_contract()
    return learner.FormalInputIdentity(
        ordered_utc_days=contract.ordered_development_days,
        opportunity_count=contract.expected_opportunities,
        arm_row_count=contract.expected_arm_rows,
        predicate_schema_sha256=contract.predicate_schema_sha256,
        outer_fold_source_sha256=contract.outer_fold_source_sha256,
        spec_sha256=contract.spec_sha256,
        outcome_blind_sha256=contract.outcome_blind_sha256,
    )


@pytest.fixture(scope="module")
def fitted_policy_bundle():
    panel = _synthetic_panel()
    policy, audit = learner.fit_side_policy(
        panel,
        side="SELL",
        economic_epsilon_usdc=0.1,
        max_literals_per_clause=2,
        max_clauses=2,
        bootstrap_samples=120,
        synthetic_mode=True,
    )
    return panel, policy, audit


def test_learns_sparse_dnf_a_and_b_or_c_and_not_d(fitted_policy_bundle) -> None:
    panel, policy, audit = fitted_policy_bundle

    assert audit.eligible_opportunities == 8 * 10
    assert policy.rules
    fixed = next(rule for rule in policy.rules if rule.action == "FIXED_79S")
    clause_keys = {clause.key for clause in fixed.clauses}
    assert (("predicate::a", False), ("predicate::b", False)) in clause_keys
    assert (("predicate::c", False), ("predicate::d", True)) in clause_keys

    truth = panel.drop_duplicates("opportunity_id")
    expected = (truth["predicate::a"] & truth["predicate::b"]) | (
        truth["predicate::c"] & ~truth["predicate::d"]
    )
    selected = (
        policy.choose(truth[["predicate::a", "predicate::b", "predicate::c", "predicate::d"]])
        == "FIXED_79S"
    )
    np.testing.assert_array_equal(selected, expected.to_numpy())
    artifact = policy.artifact()
    assert artifact["policy_sha256"] == policy.artifact()["policy_sha256"]
    assert not artifact["permissions"]["action_authorized"]
    assert not artifact["permissions"]["live_authorized"]
    assert artifact["beam_survivor_family_size"] > 1
    assert len(artifact["beam_survivor_family_sha256"]) == 64
    assert not artifact["beam_survivor_family_promotion_authority"]
    assert artifact["confidence"] == 0.95
    assert artifact["bootstrap_samples"] == 120
    assert "simultaneous_lcb_usdc" not in fixed.serialize()


def test_pooled_buy_sell_is_rejected() -> None:
    sell = _synthetic_panel(side="SELL")
    buy = _synthetic_panel(side="BUY")
    pooled = pd.concat([sell, buy], ignore_index=True)
    with pytest.raises(ValueError, match="pooled BUY/SELL"):
        learner.fit_side_policy(
            pooled,
            side="SELL",
            economic_epsilon_usdc=0.0,
            max_literals_per_clause=2,
            max_clauses=2,
            bootstrap_samples=100,
            synthetic_mode=True,
        )


def test_campaign_total_weight_is_one_and_joint_censor_is_whole_opportunity() -> None:
    panel = _synthetic_panel(campaigns_per_day=12)
    opportunities = panel["opportunity_id"].drop_duplicates().iloc[:2].tolist()
    shared_campaign = panel.loc[
        panel["opportunity_id"].eq(opportunities[0]), "campaign_side_id"
    ].iloc[0]
    panel.loc[panel["opportunity_id"].eq(opportunities[1]), "campaign_side_id"] = shared_campaign
    first = panel["opportunity_id"].iloc[0]
    panel.loc[panel["opportunity_id"].eq(first), "joint_censored"] = True
    normalized = learner.normalize_joint_panel(
        panel,
        side="SELL",
        synthetic_mode=True,
    )
    assert normalized.synthetic_test_only

    assert normalized.audit.joint_censored_opportunities == 1
    assert normalized.audit.excluded_opportunity_ids == (first,)
    totals = normalized.frame.groupby("campaign_side_id")["campaign_weight"].sum()
    np.testing.assert_allclose(totals, 1.0, atol=1e-12, rtol=0.0)
    shared_rows = normalized.frame["campaign_side_id"].eq(shared_campaign)
    # One of the two shared opportunities was jointly censored, so the single
    # admitted opportunity receives the campaign's full unit weight.
    assert normalized.frame.loc[shared_rows, "campaign_weight"].tolist() == [1.0]

    missing_arm = panel.loc[
        ~(
            panel["opportunity_id"].eq(panel["opportunity_id"].iloc[8])
            & panel["candidate_policy_id"].eq("FIXED_79S")
        )
    ].copy()
    with pytest.raises(ValueError, match="all eight arms"):
        learner.normalize_joint_panel(
            missing_arm,
            side="SELL",
            synthetic_mode=True,
        )


def test_chronology_is_strict_and_outer_rule_execution_does_not_fit(
    fitted_policy_bundle,
) -> None:
    panel = _synthetic_panel(days=40, campaigns_per_day=12)
    folds = learner.build_nested_chronological_folds(
        panel,
        side="SELL",
        synthetic_mode=True,
    )
    assert len(folds) == 4
    assert all(max(fold.train_days) < min(fold.test_days) for fold in folds)
    assert all(
        max(inner.train_days) < min(inner.test_days) for fold in folds for inner in fold.inner_folds
    )
    assert len({day for fold in folds for day in fold.test_days}) == 24

    _, frozen, _ = fitted_policy_bundle
    before_hash = frozen.artifact()["policy_sha256"]
    test = panel.loc[panel["utc_day"].isin(folds[0].test_days)].copy()
    executed = learner.apply_frozen_policy(
        frozen,
        test,
        side="SELL",
        synthetic_mode=True,
    )
    assert len(executed) == 6 * 12
    assert frozen.artifact()["policy_sha256"] == before_hash

    bad = panel.copy()
    first_test_day = folds[0].test_days[0]
    bad.loc[bad["utc_day"].lt(first_test_day), "washout_ts_ns"] = (
        int(pd.Timestamp(first_test_day, tz="UTC").timestamp() * 1_000_000_000) + 1
    )
    with pytest.raises(ValueError, match="chronology|admissible"):
        learner.build_nested_chronological_folds(
            bad,
            side="SELL",
            synthetic_mode=True,
        )


def test_policy_hash_changes_with_rule_payload(fitted_policy_bundle) -> None:
    _, policy, _ = fitted_policy_bundle
    artifact = policy.artifact()
    payload = dict(artifact)
    payload.pop("policy_sha256")
    expected = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
            "utf-8"
        )
    ).hexdigest()
    assert artifact["policy_sha256"] == expected


def test_formal_schema_selects_exact_predicate_namespace_and_freezes_zero_epsilon() -> None:
    panel = _with_frozen_predicate_schema(_synthetic_panel(days=12))
    with pytest.raises(ValueError, match="formal economic epsilon is frozen at zero"):
        learner.fit_side_policy(
            panel,
            side="SELL",
            economic_epsilon_usdc=0.01,
            formal_input_identity=_expected_formal_identity(),
        )

    contract = learner.load_frozen_search_contract()
    normalized = learner.normalize_joint_panel(panel, side="SELL")
    searchable = learner._searchable_predicate_columns(  # noqa: SLF001
        normalized.predicates
    )
    validity = tuple(
        column
        for column in normalized.predicates
        if column.startswith("__validity__::")
    )
    assert len(searchable) == 360
    assert searchable == contract.predicate_columns
    assert len(validity) == 45
    assert learner._sha256(list(searchable)) == (  # noqa: SLF001
        contract.predicate_schema_sha256
    )
    assert "runner_metadata" not in normalized.predicates.columns

    reordered = panel.loc[
        :,
        [
            *[column for column in panel if not column.startswith("predicate::")],
            contract.predicate_columns[1],
            contract.predicate_columns[0],
            *contract.predicate_columns[2:],
        ],
    ]
    with pytest.raises(ValueError, match="names/order"):
        learner.normalize_joint_panel(reordered, side="SELL")

    renamed = panel.rename(columns={contract.predicate_columns[-1]: "predicate::mutated_name"})
    with pytest.raises(ValueError, match="names/order"):
        learner.normalize_joint_panel(renamed, side="SELL")


def test_synthetic_mode_must_be_explicit() -> None:
    panel = _synthetic_panel()
    panel.attrs["synthetic_test_only"] = True
    with pytest.raises(ValueError, match="names/order"):
        learner.normalize_joint_panel(panel, side="SELL")
    normalized = learner.normalize_joint_panel(
        panel,
        side="SELL",
        synthetic_mode=True,
    )
    assert normalized.synthetic_test_only


def test_partial_outer_fold_request_fails_closed_before_search() -> None:
    panel = _with_frozen_predicate_schema(_synthetic_panel(days=12))
    with pytest.raises(ValueError, match="unique members"):
        learner.run_nested_chronological_oof(
            panel,
            side="SELL",
            synthetic_mode=True,
            outer_fold_indices=(0, 0),
        )
    with pytest.raises(ValueError, match="unique members"):
        learner.run_nested_chronological_oof(
            panel,
            side="SELL",
            synthetic_mode=True,
            outer_fold_indices=(4,),
        )


def test_formal_attestation_binds_days_denominator_schema_and_outer_folds() -> None:
    contract = learner.load_frozen_search_contract()
    small = replace(
        contract,
        ordered_development_days=tuple(
            (pd.Timestamp("2026-01-01") + pd.Timedelta(days=index)).strftime("%Y-%m-%d")
            for index in range(4)
        ),
        expected_opportunities=8,
        expected_arm_rows=64,
    )
    panel = _with_frozen_predicate_schema(_synthetic_panel(days=4, campaigns_per_day=2))
    identity = learner.attest_formal_input_panel(panel, contract=small)
    assert identity.opportunity_count == 8
    assert identity.arm_row_count == 64
    assert identity.predicate_schema_sha256 == contract.predicate_schema_sha256
    assert identity.outer_fold_source_sha256 == contract.outer_fold_source_sha256

    reduced = panel.iloc[:-1].copy()
    with pytest.raises(ValueError, match="68,800"):
        learner.attest_formal_input_panel(reduced, contract=small)


def test_formal_days_and_outer_folds_are_exactly_frozen() -> None:
    contract = learner.load_frozen_search_contract()
    panel = _with_frozen_predicate_schema(
        _synthetic_panel(
            campaigns_per_day=1,
            ordered_days=contract.ordered_development_days,
        )
    )
    folds = learner.build_nested_chronological_folds(panel, side="SELL")
    assert tuple(fold.test_days for fold in folds) == tuple(
        fold.test_days for fold in contract.frozen_outer_folds
    )
    assert tuple(fold.embargo_days for fold in folds) == tuple(
        (fold.embargo_day,) for fold in contract.frozen_outer_folds
    )

    wrong = panel.copy()
    wrong["utc_day"] = wrong["utc_day"].replace(
        {contract.ordered_development_days[-1]: "2026-06-27"}
    )
    with pytest.raises(ValueError, match="exact frozen ordered 40 days"):
        learner.build_nested_chronological_folds(wrong, side="SELL")


def test_unsupported_effective_first_match_mask_returns_policy_to_control(
    monkeypatch,
) -> None:
    panel = _synthetic_panel(days=12)
    normalized = learner.normalize_joint_panel(
        panel,
        side="SELL",
        synthetic_mode=True,
    )
    contract = learner.load_frozen_search_contract()
    mask_a = np.ones(len(normalized.frame), dtype=bool)
    mask_a[-5:] = False
    mask_b = np.zeros(len(normalized.frame), dtype=bool)
    mask_b[:50] = True
    mask_b[-5:] = True
    normalized.predicates.loc[:, "predicate::a"] = mask_a
    normalized.predicates.loc[:, "predicate::b"] = mask_b
    first = learner._ClauseCandidate(  # noqa: SLF001 - contract-level regression
        "FIXED_79S",
        learner.Clause((learner.Literal("predicate::a"),)),
        mask_a,
        1.0,
    )
    second = learner._ClauseCandidate(  # noqa: SLF001 - contract-level regression
        "FIXED_166S",
        learner.Clause((learner.Literal("predicate::b"),)),
        mask_b,
        0.1,
    )
    actions = np.where(mask_a, "FIXED_79S", np.where(mask_b, "FIXED_166S", "CONTROL_85N"))
    state = learner._PolicyState((first, second), actions, 2.0)  # noqa: SLF001

    def fake_band(*args, **kwargs):
        del args, kwargs
        return np.asarray([2.0]), np.asarray([1.0]), 0.5

    monkeypatch.setattr(learner, "_nested_cluster_bootstrap_lcbs", fake_band)
    policy = learner._state_to_policy(  # noqa: SLF001 - verifies fail-closed semantics
        state,
        (state,),
        normalized,
        contract,
        epsilon_usdc=0.0,
        bootstrap_samples=100,
        confidence=0.95,
        fold_identities=("synthetic",),
        formal_input_identity_sha256="synthetic_test_only",
    )
    assert policy.rules == ()
    assert np.all(policy.choose(normalized.predicates) == learner.CONTROL_ACTION)
    assert policy.beam_survivor_family_size == 1


def test_exploratory_candidate_is_not_cleared_by_pre_oof_negative_lcb(
    monkeypatch,
) -> None:
    panel = _synthetic_panel(days=12)
    normalized = learner.normalize_joint_panel(
        panel,
        side="SELL",
        synthetic_mode=True,
    )
    contract = learner.load_frozen_search_contract()
    mask = normalized.predicates["predicate::a"].to_numpy(dtype=bool)
    clause = learner.Clause((learner.Literal("predicate::a"),))
    candidate = learner._ClauseCandidate(  # noqa: SLF001
        "FIXED_79S",
        clause,
        mask,
        0.1,
    )
    actions = np.where(mask, "FIXED_79S", learner.CONTROL_ACTION)
    state = learner._PolicyState((candidate,), actions, 0.1)  # noqa: SLF001

    def negative_band(*args, **kwargs):
        del args, kwargs
        return np.asarray([0.1]), np.asarray([-0.2]), 0.3

    monkeypatch.setattr(learner, "_nested_cluster_bootstrap_lcbs", negative_band)
    screened = learner._state_to_policy(  # noqa: SLF001
        state,
        (state,),
        normalized,
        contract,
        epsilon_usdc=0.0,
        bootstrap_samples=100,
        confidence=0.95,
        fold_identities=("screened",),
        formal_input_identity_sha256="synthetic_test_only",
    )
    exploratory = learner._state_to_policy(  # noqa: SLF001
        state,
        (state,),
        normalized,
        contract,
        epsilon_usdc=0.0,
        bootstrap_samples=100,
        confidence=0.95,
        fold_identities=("exploratory",),
        formal_input_identity_sha256="synthetic_test_only",
        selection_mode=learner.EXPLORATORY_NONBASELINE_SELECTION,
        policy_identity=learner.EXPLORATORY_IDENTITY,
    )

    assert screened.rules == ()
    assert exploratory.rules
    assert exploratory.beam_survivor_family_conditional_policy_lcb_usdc < 0.0
    assert exploratory.selection_mode == learner.EXPLORATORY_NONBASELINE_SELECTION
    assert exploratory.artifact()["identity"] == learner.EXPLORATORY_IDENTITY
    assert not exploratory.artifact()["pre_outer_oof_positive_lcb_required"]


def test_negated_cross_favorable_requires_an_observed_cross() -> None:
    predicate = "predicate::ema_pair_h1s_h8s:last_cross_favorable"
    observed = "__validity__::ema_pair_h1s_h8s:cross_observed"
    frame = pd.DataFrame(
        {
            predicate: [False, False, True],
            observed: [False, True, True],
        }
    )

    values = learner.Literal(predicate, negated=True).evaluate(frame)

    assert values.tolist() == [False, True, False]
    with pytest.raises(ValueError, match="observed-state validity"):
        learner.Literal(predicate, negated=True).evaluate(frame[[predicate]])


def test_formal_confidence_and_bootstrap_fail_closed_and_enter_artifact(
    monkeypatch,
) -> None:
    panel = _with_frozen_predicate_schema(_synthetic_panel(days=4))
    identity = _expected_formal_identity()
    with pytest.raises(ValueError, match="FormalInputIdentity"):
        learner.fit_side_policy(panel, side="SELL")
    with pytest.raises(ValueError, match="0.95/500"):
        learner.fit_side_policy(
            panel,
            side="SELL",
            bootstrap_samples=499,
            confidence=0.95,
            formal_input_identity=identity,
        )
    with pytest.raises(ValueError, match="0.95/500"):
        learner.fit_side_policy(
            panel,
            side="SELL",
            bootstrap_samples=500,
            confidence=0.9,
            formal_input_identity=identity,
        )

    def fail_if_reference_search_runs(*args, **kwargs):
        del args, kwargs
        raise AssertionError("production fit called the slow reference search")

    monkeypatch.setattr(
        learner,
        "_search_rule_list_reference",
        fail_if_reference_search_runs,
    )
    policy, _ = learner.fit_side_policy(
        panel,
        side="SELL",
        max_literals_per_clause=1,
        max_clauses=2,
        bootstrap_samples=500,
        confidence=0.95,
        formal_input_identity=identity,
    )
    artifact = policy.artifact()
    assert artifact["confidence"] == 0.95
    assert artifact["bootstrap_samples"] == 500
    assert (
        artifact["formal_input_identity_sha256"]
        == identity.artifact()["formal_input_identity_sha256"]
    )


def test_campaign_weighted_objective_is_not_day_equal() -> None:
    executed = pd.DataFrame(
        {
            "policy_minus_control_usdc": [10.0, -1.0],
            "campaign_weight": [0.1, 10.0],
            "utc_day": ["2026-01-01", "2026-01-02"],
        }
    )
    observed = learner._weighted_policy_uplift(executed)  # noqa: SLF001
    expected = (10.0 * 0.1 - 1.0 * 10.0) / 10.1
    assert observed == pytest.approx(expected)
    assert observed != pytest.approx((10.0 - 1.0) / 2.0)


def _float_bits(values) -> np.ndarray:
    return np.asarray(values, dtype=np.float64).view(np.uint64)


def test_optimized_grid_is_bitwise_reference_equivalent_and_reuses_work() -> None:
    raw = _synthetic_panel(days=8, campaigns_per_day=10)
    panel = learner.normalize_joint_panel(raw, side="SELL", synthetic_mode=True)
    contract = replace(
        learner.load_frozen_search_contract(),
        max_literals_per_clause=(1, 2, 3),
        max_clauses=(2, 3),
        beam_width=8,
        minimum_days=2,
        minimum_campaigns=4,
        minimum_campaign_weight_fraction=0.0,
    )
    reference_counters = learner._SearchWorkCounters()  # noqa: SLF001
    reference_families = {}
    reference_policies = {}
    for max_literals in contract.max_literals_per_clause:
        expected_pool = learner._generate_clause_pool(  # noqa: SLF001
            panel,
            contract,
            max_literals=max_literals,
        )
        for max_clauses in contract.max_clauses:
            family = learner._search_rule_list_reference(  # noqa: SLF001
                panel,
                contract,
                max_literals=max_literals,
                max_clauses=max_clauses,
                work_counters=reference_counters,
            )
            key = (max_literals, max_clauses)
            reference_families[key] = family
            reference_policies[key] = learner._state_to_policy(  # noqa: SLF001
                family[0] if family else None,
                family,
                panel,
                contract,
                epsilon_usdc=0.0,
                bootstrap_samples=100,
                confidence=0.95,
                fold_identities=(),
                formal_input_identity_sha256="synthetic_test_only",
                work_counters=reference_counters,
            )
        optimized_pool = learner._generate_clause_depth_snapshots(  # noqa: SLF001
            panel,
            contract,
            max_literals=contract.max_literals_per_clause,
        )[max_literals]
        assert [candidate.key for candidate in optimized_pool] == [
            candidate.key for candidate in expected_pool
        ]
        np.testing.assert_array_equal(
            _float_bits([candidate.score for candidate in optimized_pool]),
            _float_bits([candidate.score for candidate in expected_pool]),
        )

    optimized_counters = learner._SearchWorkCounters()  # noqa: SLF001
    optimized = learner._fit_normalized_complexity_grid(  # noqa: SLF001
        panel,
        contract,
        economic_epsilon_usdc=0.0,
        bootstrap_samples=100,
        confidence=0.95,
        formal_input_identity_sha256="synthetic_test_only",
        work_counters=optimized_counters,
    )
    for key, reference_family in reference_families.items():
        optimized_family = optimized.snapshots.candidate_families[key]
        assert [state.key for state in optimized_family] == [
            state.key for state in reference_family
        ]
        np.testing.assert_array_equal(
            _float_bits([state.score for state in optimized_family]),
            _float_bits([state.score for state in reference_family]),
        )
        for reference_state, optimized_state in zip(
            reference_family,
            optimized_family,
            strict=True,
        ):
            np.testing.assert_array_equal(
                learner._state_actions(optimized_state, panel),  # noqa: SLF001
                reference_state.actions,
            )

        reference_policy = reference_policies[key]
        optimized_policy = optimized.policy_templates[key]
        assert optimized_policy.rules == reference_policy.rules
        assert optimized_policy.beam_survivor_family_sha256 == (
            reference_policy.beam_survivor_family_sha256
        )
        np.testing.assert_array_equal(
            _float_bits(
                [
                    optimized_policy.beam_survivor_family_conditional_critical_usdc,
                    optimized_policy.beam_survivor_family_conditional_policy_lcb_usdc,
                ]
            ),
            _float_bits(
                [
                    reference_policy.beam_survivor_family_conditional_critical_usdc,
                    reference_policy.beam_survivor_family_conditional_policy_lcb_usdc,
                ]
            ),
        )
        np.testing.assert_array_equal(
            optimized_policy.choose(panel.predicates),
            reference_policy.choose(panel.predicates),
        )

    grid_size = len(contract.max_literals_per_clause) * len(contract.max_clauses)
    assert reference_counters.bootstrap_draws_built == 100 * grid_size
    assert optimized_counters.bootstrap_draws_built == 100
    assert optimized_counters.clause_evaluations < reference_counters.clause_evaluations
    assert optimized_counters.rule_state_evaluations < (reference_counters.rule_state_evaluations)
    assert optimized_counters.rule_state_materializations < (
        reference_counters.rule_state_materializations
    )
    assert optimized_counters.rule_state_materializations < (
        optimized_counters.rule_state_evaluations
    )
    assert (
        0
        < optimized_counters.rule_state_exact_rescores
        < (optimized_counters.rule_state_evaluations)
    )
    assert optimized_counters.bootstrap_state_columns_built < (
        reference_counters.bootstrap_state_columns_built
    )


def test_production_fit_does_not_call_slow_reference_search(monkeypatch) -> None:
    raw = _synthetic_panel(days=4, campaigns_per_day=8)
    contract = replace(
        learner.load_frozen_search_contract(),
        max_literals_per_clause=(1, 2),
        max_clauses=(2,),
        beam_width=8,
        minimum_days=2,
        minimum_campaigns=4,
        minimum_campaign_weight_fraction=0.0,
    )
    monkeypatch.setattr(learner, "load_frozen_search_contract", lambda: contract)

    def reject_slow_reference(*args, **kwargs):
        del args, kwargs
        raise AssertionError("production fit called the slow reference search")

    monkeypatch.setattr(
        learner,
        "_search_rule_list_reference",
        reject_slow_reference,
    )
    policy, audit = learner.fit_side_policy(
        raw,
        side="SELL",
        economic_epsilon_usdc=0.0,
        max_literals_per_clause=2,
        max_clauses=2,
        bootstrap_samples=100,
        confidence=0.95,
        synthetic_mode=True,
    )

    assert audit.eligible_opportunities == 32
    assert policy.side == "SELL"
    assert policy.bootstrap_samples == 100


def _positive_nested_result() -> learner.NestedChronologicalResult:
    oof_rows: list[dict[str, object]] = []
    for fold in range(4):
        for campaign in range(3):
            oof_rows.append(
                {
                    "outer_fold": fold,
                    "opportunity_id": f"oof-{fold}-{campaign}",
                    "side": "SELL",
                    "utc_day": f"2026-02-{fold * 3 + campaign + 1:02d}",
                    "campaign_side_id": f"campaign-{fold}-{campaign}",
                    "campaign_weight": 1.0,
                    "chosen_action": "FIXED_79S",
                    "chosen_value_usdc": 2.0,
                    "control_value_usdc": 0.0,
                    "policy_minus_control_usdc": 2.0,
                    "policy_sha256": "a" * 64,
                }
            )
    evidence = pd.DataFrame(
        [
            {
                "outer_fold": fold,
                "max_literals_per_clause": 2,
                "max_clauses": 2,
                "inner_oof_mean_usdc": 1.0,
                "inner_oof_standard_error_usdc": 0.1,
                "inner_oof_campaign_weight": 10.0,
                "inner_selection_objective": "global_campaign_weighted",
                "selected": True,
            }
            for fold in range(4)
        ]
    )
    return learner.NestedChronologicalResult(
        oof=pd.DataFrame(oof_rows),
        complexity_evidence=evidence,
        chronology_audit=pd.DataFrame(
            {
                "future_training_leakage": [False],
                "outer_outcomes_used_for_fit": [False],
            }
        ),
        outer_policy_artifacts=(),
        panel_audit=learner.PanelAudit(80, 80, 0, (), 1.0, 1.0),
        permissions={
            "action_authorized": False,
            "live_authorized": False,
            "f09_registration_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    )


def test_outer_oof_gate_and_final_single_side_policy_freeze_are_pure() -> None:
    result = _positive_nested_result()
    gate = learner.evaluate_outer_oof_gate(
        result,
        side="SELL",
        bootstrap_samples=100,
        synthetic_mode=True,
    )
    assert gate.passed
    assert gate.lower_confidence_bound_usdc == pytest.approx(2.0)
    assert gate.artifact()["authority"] == ("outer_oof_gate_only_no_action_or_live_authority")

    panel = _synthetic_panel(days=8, campaigns_per_day=10)
    frozen = learner.freeze_final_side_policy(
        panel,
        result,
        side="SELL",
        economic_epsilon_usdc=0.1,
        bootstrap_samples=100,
        synthetic_mode=True,
    )
    assert frozen.side == "SELL"
    assert frozen.policy.rules
    artifact = frozen.artifact()
    assert artifact["policy_count_for_side"] == 1
    assert not artifact["permissions"]["action_authorized"]
    assert not artifact["permissions"]["live_authorized"]

    bundle = learner.serialize_final_policy_bundle((frozen,))
    assert bundle["policy_count"] == 1
    assert list(bundle["side_policies"]) == ["SELL"]
    assert not bundle["permissions"]["live_authorized"]


def test_failed_outer_oof_gate_cannot_freeze_final_policy() -> None:
    result = _positive_nested_result()
    result.oof.loc[:, "policy_minus_control_usdc"] = -1.0
    result.oof.loc[:, "chosen_value_usdc"] = -1.0
    with pytest.raises(ValueError, match="outer OOF gate did not pass"):
        learner.freeze_final_side_policy(
            _synthetic_panel(),
            result,
            side="SELL",
            bootstrap_samples=100,
            synthetic_mode=True,
        )
