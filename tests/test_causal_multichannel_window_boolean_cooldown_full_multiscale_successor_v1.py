from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 as successor,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_successor_transport_adapter_v1 as transport,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    CONTROL_ACTION,
    duration_vocabulary,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_persistent_policy_v3_inference import (
    SimultaneousBand,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_runtime_policy import (
    _direct_predicate,
)
from strategy.boolean_cooldown_live import (
    ReceiveTimeMidEmaWindows,
)
from strategy.boolean_cooldown_live import (
    RuntimeCooldownPolicyEvaluator as LivePredicateEvaluator,
)


def _live_rules(policy) -> tuple:
    return tuple(
        (
            rule.action,
            tuple(
                tuple((literal.predicate, literal.negated) for literal in clause.literals)
                for clause in rule.clauses
            ),
        )
        for rule in policy.rules
    )


def test_exact_owner_policy_semantics_separate_guard_from_economic_features() -> None:
    policy = successor.current_exact_owner_policy()

    audit = successor.audit_policy_semantics(policy, candidate_source_block="M2")

    assert audit.candidate_source_block == "M2"
    assert audit.compiled_feature_families == ("M0", "mid_ema")
    assert audit.uses_m2_incremental_features is False
    assert audit.readiness_guard_predicates == (successor.CURRENT_SHORT_CROSS,)
    assert set(audit.economic_branch_features) == {
        successor.CURRENT_CAMPAIGN_AGE,
        successor.CURRENT_LONG_CROSS,
    }
    assert len(audit.redundancies) == 1
    assert audit.live_artifact_rewritten is False


def test_exact_owner_policy_research_live_tri_state_parity() -> None:
    policy = successor.current_exact_owner_policy()
    live = LivePredicateEvaluator(
        rules=_live_rules(policy),
        policy_sha256=successor.ACTIVE_OWNER_POLICY_SHA256,
        predicate_bundle_sha256=successor.ACTIVE_PREDICATE_BUNDLE_SHA256,
    )

    for states in product((-1, 0, 1), repeat=3):
        values = dict(zip(policy.predicate_columns, states, strict=True))
        research_action = str(policy.choose(pd.DataFrame([values]))[0])
        decision = live.evaluate_predicates(
            side="SELL",
            predicate_values=values,
            baseline_duration_ms=85_000,
            snapshot_id=f"tri-state-{states}",
        )
        assert decision.action_id == research_action
        assert decision.duration_ms == (
            85_000
            if research_action == CONTROL_ACTION
            else int(research_action.removeprefix("FIXED_").removesuffix("S")) * 1_000
        )


def test_exact_owner_policy_payload_parser_preserves_documented_semantics() -> None:
    expected = successor.current_exact_owner_policy()
    payload = {
        "identity": successor.ACTIVE_OWNER_POLICY_IDENTITY,
        "policy": {
            "side": "SELL",
            "default_action": CONTROL_ACTION,
            "ordered_first_match_rules": [
                {
                    "action": rule.action,
                    "clauses": [
                        {
                            "literals": [
                                {
                                    "predicate": literal.predicate,
                                    "negated": literal.negated,
                                }
                                for literal in clause.literals
                            ]
                        }
                        for clause in rule.clauses
                    ],
                }
                for rule in expected.rules
            ],
        },
    }

    parsed = successor.parse_owner_boolean_policy(payload)

    assert parsed.candidate_id == expected.candidate_id
    assert parsed.predicate_columns == expected.predicate_columns


def test_campaign_age_threshold_equality_matches_research_and_live() -> None:
    feature_row = {"campaign_age_s": 85.0}
    research = int(
        _direct_predicate(
            successor.CURRENT_CAMPAIGN_AGE,
            feature_row=feature_row,
            baseline_duration_ms=85_000,
        )
    )
    windows = ReceiveTimeMidEmaWindows(warmup_s=0.1, max_feature_age_s=2.0)
    base = 1_800_000_000_000_000_000
    for index, mid in enumerate((100.0, 101.0, 99.0, 100.0)):
        windows.observe_depth(
            receive_ts_ns=base + index * 100_000_000,
            bids=((mid - 0.05, 1.0),),
            asks=((mid + 0.05, 1.0),),
            market_generation=index + 1,
            depth_generation=index + 1,
        )
    values, reason, _, _ = windows.predicate_values(
        decision_ts_ns=base + 300_000_000,
        campaign_age_s=85.0,
        baseline_duration_ms=85_000,
    )

    assert reason is None
    assert values is not None
    assert research == 0
    assert values[successor.CURRENT_CAMPAIGN_AGE] == research


def test_inner_feature_pool_never_uses_future_inner_fold_distribution() -> None:
    train = pd.Index([f"train-{index}" for index in range(8)])
    future = pd.Index([f"future-{index}" for index in range(8)])
    index = train.append(future)
    pair_features = [
        f"predicate::{prefix}:favorable"
        for prefix in successor.full_ema_pair_prefixes()
    ]
    candidates = ("predicate::m0::side_sell", *pair_features, "future_only")
    values: dict[str, list[int]] = {
        "predicate::m0::side_sell": [0, 1] * 8,
        "future_only": [-1] * 8 + [0, 1] * 4,
    }
    for offset, name in enumerate(pair_features):
        values[name] = [int((row + offset) % 2 == 0) for row in range(16)]
    features = pd.DataFrame(values, index=index, dtype=np.int8)
    metadata = pd.DataFrame(
        {
            "utc_day": ["2026-08-13"] * 4
            + ["2026-08-14"] * 4
            + ["2026-08-15"] * 8,
            "campaign_cluster_id": [f"campaign-{row}" for row in range(16)],
        },
        index=index,
    )

    selected, audit = successor.build_inner_train_feature_pool(
        features,
        metadata,
        train_index=train,
        candidates=candidates,
        feature_budget=46,
        fold_id="inner-1",
        required_features=("predicate::m0::side_sell",),
    )

    assert "future_only" not in selected
    assert audit.train_days == ("2026-08-13", "2026-08-14")
    assert audit.all_45_ema_pairs_eligible is True
    assert audit.eligible_ema_pair_count == 45
    assert audit.selected_ema_pair_count == 45


def test_unknown_action_targets_remain_missing_and_do_not_enter_fit() -> None:
    actions = duration_vocabulary("SELL")[1:3]
    truth = np.asarray(list(product((0, 1), repeat=4)), dtype=np.int8)
    matrix = np.repeat(truth, 10, axis=0)
    index = pd.Index([f"row-{row}" for row in range(len(matrix))])
    features = pd.DataFrame(matrix, index=index, columns=["p1", "p2", "p3", "p4"])
    action_one = np.where(
        (matrix[:, 0] == 1) & (matrix[:, 1] == 1) & (matrix[:, 2] == 1),
        2.0,
        -1.0,
    )
    action_two = np.where(
        (matrix[:, 0] == 0) & (matrix[:, 2] == 1) & (matrix[:, 3] == 1),
        3.0,
        -1.0,
    )
    outcomes = pd.DataFrame(
        {
            CONTROL_ACTION: np.zeros(len(index)),
            actions[0]: action_one,
            actions[1]: action_two,
        },
        index=index,
    )
    supported = pd.DataFrame(True, index=index, columns=outcomes.columns)
    supported.loc[index[:7], actions[0]] = False
    targets = successor.build_identified_action_targets(
        outcomes,
        supported,
        actions=actions,
    )
    metadata = pd.DataFrame(
        {
            "utc_day": [f"2026-08-{13 + row // 40:02d}" for row in range(len(index))],
            "campaign_cluster_id": [f"campaign-{row // 2}" for row in range(len(index))],
        },
        index=index,
    )
    profile = successor.SuccessorSearchProfile(
        feature_budget=4,
        max_depth=4,
        max_leaf_nodes=16,
        min_samples_leaf=2,
        max_rules=4,
        max_clauses_per_rule=8,
        max_literals_per_clause=4,
    )

    policy, audit = successor.fit_identified_action_policy(
        features,
        metadata,
        targets,
        side="SELL",
        feature_names=tuple(features.columns),
        profile=profile,
        random_seed=17,
    )

    assert targets.effects.loc[index[:7], actions[0]].isna().all()
    assert not (targets.effects.loc[index[:7], actions[0]] == 0.0).any()
    assert audit.uses_neutral_zero_targets is False
    assert audit.compiled_rule_count >= 2
    assert audit.maximum_clause_literals >= 3
    assert {rule.action for rule in policy.rules} == set(actions)
    by_action = {row.action: row for row in audit.action_audits}
    assert by_action[actions[0]].unidentified_rows == 7
    assert by_action[actions[1]].unidentified_rows == 0


def test_targets_use_exact_row_wise_owner_policy_as_control() -> None:
    actions = duration_vocabulary("SELL")[1:3]
    index = pd.Index(["row-1", "row-2", "row-3"])
    outcomes = pd.DataFrame(
        {
            CONTROL_ACTION: [1.0, 1.0, 1.0],
            actions[0]: [2.0, 4.0, 8.0],
            actions[1]: [3.0, 7.0, 9.0],
        },
        index=index,
    )
    supported = pd.DataFrame(True, index=index, columns=outcomes.columns)
    supported.loc["row-3", actions[1]] = False
    baseline_actions = pd.Series(
        [CONTROL_ACTION, actions[0], actions[1]],
        index=index,
    )

    targets = successor.build_identified_action_targets_against_policy(
        outcomes,
        supported,
        baseline_actions=baseline_actions,
        actions=actions,
    )

    assert targets.control_action == "ROW_WISE_EXACT_POLICY"
    assert targets.effects.loc["row-1", actions[0]] == 1.0
    assert targets.effects.loc["row-2", actions[1]] == 3.0
    assert np.isnan(targets.effects.loc["row-3", actions[0]])
    assert targets.identified.loc["row-3", actions[0]] == np.bool_(False)


def test_observed_only_tree_path_fails_closed_for_unobserved_runtime_state() -> None:
    clauses = successor.compile_observed_leaf_clauses(
        {
            "observed_guard": (-0.5, np.inf),
            "economic_branch": (0.5, np.inf),
        },
        max_clauses=4,
    )
    policy = successor.BooleanCooldownPolicy(
        side="SELL",
        rules=(
            successor.BooleanRule(
                action=duration_vocabulary("SELL")[1],
                clauses=clauses,
            ),
        ),
    )
    rows = pd.DataFrame(
        {
            "observed_guard": [-1, 0, 1],
            "economic_branch": [1, 1, 1],
        },
        dtype=np.int8,
    )

    decisions = policy.choose(rows)

    assert len(clauses) == 2
    assert decisions[0] == CONTROL_ACTION
    assert decisions[1] != CONTROL_ACTION
    assert decisions[2] != CONTROL_ACTION


def _prospective_day(day: str, **overrides):
    row = {
        "utc_day": day,
        "epoch_identity_sha256": "1" * 64,
        "session_manifest_sha256": "2" * 64,
        "utc_day_closed": True,
        "registered_treatment_interval_coverage_complete": True,
        "strategy_identity_valid": True,
        "source_complete": True,
        "receive_clock_valid": True,
        "feature_ready_clock_valid": True,
        "policy_decision_clock_valid": True,
        "lifecycle_valid": True,
        "callbacks_converged": True,
        "remote_local_admission_valid": True,
        "recorder_drops": 0,
        "severe_errors": 0,
        "eligible_events": 10,
        "feature_ready_active_treatment_events": 5,
    }
    row.update(overrides)
    return row


def _write_json(path: Path, payload) -> str:
    encoded = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode()
    path.write_bytes(encoded)
    return hashlib.sha256(encoded).hexdigest()


def test_prospective_day_selection_is_outcome_blind_and_excludes_inactive_days() -> None:
    start = date(2026, 8, 13)
    records = [
        _prospective_day("2026-08-12"),
        _prospective_day("2026-08-13", eligible_events=0, feature_ready_active_treatment_events=0),
        _prospective_day("2026-08-14", source_complete=False),
    ]
    records.extend(
        _prospective_day((start + timedelta(days=offset)).isoformat())
        for offset in range(2, 32)
    )

    admission = successor.select_prospective_development_days(records)

    assert admission.ready_for_new_economic_panel is True
    assert len(admission.selected_active_days) == 30
    assert admission.selected_active_days[0] == "2026-08-15"
    assert admission.selected_active_days[-1] == "2026-09-13"
    assert admission.rejected_days["2026-08-12"] == (
        "before_preregistration_cutoff",
    )
    assert "zero_feature_ready_active_treatment_events" in admission.rejected_days[
        "2026-08-13"
    ]
    assert admission.rejected_days["2026-08-14"] == ("source_incomplete",)


def test_prospective_panel_identity_binds_selected_day_source_manifests() -> None:
    records = [
        _prospective_day(
            (date(2026, 8, 13) + timedelta(days=offset)).isoformat()
        )
        for offset in range(30)
    ]
    changed = [dict(row) for row in records]
    changed[7]["session_manifest_sha256"] = "9" * 64

    first = successor.select_prospective_development_days(records)
    second = successor.select_prospective_development_days(changed)

    assert first.selected_active_days == second.selected_active_days
    assert first.selection_sha256 != second.selection_sha256


def test_prospective_day_admission_rejects_economic_selection_fields() -> None:
    payload = _prospective_day("2026-08-13", terminal_value_usdc=1.0)

    with np.testing.assert_raises(successor.SuccessorContractError):
        successor.parse_prospective_day_admission(payload)


def test_day_admission_is_produced_from_hash_bound_non_economic_manifests(
    tmp_path: Path,
) -> None:
    day = "2026-08-13"
    strategy_sha = "3" * 64
    epoch_sha = "4" * 64
    intervals = [["2026-08-13T01:00:00Z", "2026-08-13T03:00:00Z"]]
    interval_sha256 = _canonical_hash(
        {
            "utc_day": day,
            "registered_treatment_intervals_utc": intervals,
        }
    )
    common = {
        "identity": successor.IDENTITY,
        "utc_day": day,
        "strategy_identity_sha256": strategy_sha,
        "epoch_identity_sha256": epoch_sha,
        "registered_treatment_interval_count": 1,
        "registered_treatment_intervals_sha256": interval_sha256,
        "registered_treatment_interval_coverage_complete": True,
        "remote_local_admission_valid": True,
        "drop_count": 0,
        "error_count": 0,
    }
    lifecycle = {
        **common,
        "schema_version": successor.LIFECYCLE_DAY_ADMISSION_SCHEMA,
        "lifecycle_valid": True,
        "callbacks_converged": True,
    }
    market = {
        **common,
        "schema_version": successor.MARKET_DAY_ADMISSION_SCHEMA,
        "source_complete": True,
        "receive_clock_valid": True,
        "feature_ready_clock_valid": True,
    }
    decision = {
        **common,
        "schema_version": successor.DECISION_DAY_ADMISSION_SCHEMA,
        "policy_decision_clock_valid": True,
        "eligible_events": 10,
        "feature_ready_active_treatment_events": 4,
        "coverage_reason_counts": {
            successor.CoverageReason.ELIGIBLE_FEATURE_READY.value: 4,
            successor.CoverageReason.POLICY_CONTROL.value: 6,
        },
    }
    bindings = {}
    for name, payload in (
        ("lifecycle", lifecycle),
        ("market", market),
        ("decision", decision),
    ):
        path = tmp_path / f"{name}.json"
        bindings[name] = {"path": path.name, "sha256": _write_json(path, payload)}
    bundle = {
        "schema_version": successor.PROSPECTIVE_DAY_SOURCE_BUNDLE_SCHEMA,
        "identity": successor.IDENTITY,
        "utc_day": day,
        "utc_day_closed": True,
        "strategy_identity_sha256": strategy_sha,
        "epoch_identity_sha256": epoch_sha,
        "registered_treatment_intervals_utc": intervals,
        "bindings": bindings,
        "permissions": {
            "economic_outcomes_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    }
    bundle_path = tmp_path / "source-bundle.json"
    _write_json(bundle_path, bundle)

    admission = successor.produce_prospective_day_admission(bundle_path)
    receipt = tmp_path / "admitted" / "day.json"
    first_sha = successor.write_prospective_day_admission(admission, receipt)
    second_sha = successor.write_prospective_day_admission(admission, receipt)
    loaded = successor.load_prospective_day_admission(receipt)
    status = successor.prospective_status_payload((loaded,))

    assert admission.eligible is True
    assert loaded == admission
    assert admission.feature_ready_active_treatment_events == 4
    assert first_sha == second_sha == hashlib.sha256(receipt.read_bytes()).hexdigest()
    assert status["eligible_active_day_count"] == 1
    assert status["ready_for_new_economic_panel"] is False

    market_path = tmp_path / "market.json"
    drifted_market = json.loads(market_path.read_text(encoding="utf-8"))
    drifted_market["registered_treatment_intervals_sha256"] = "f" * 64
    bindings["market"]["sha256"] = _write_json(market_path, drifted_market)
    drifted_bundle = {**bundle, "bindings": bindings}
    drifted_bundle_path = tmp_path / "source-bundle-drifted.json"
    _write_json(drifted_bundle_path, drifted_bundle)
    with np.testing.assert_raises(successor.SuccessorContractError):
        successor.produce_prospective_day_admission(drifted_bundle_path)


def test_fold_manifest_freezes_four_outer_and_three_inner_folds(tmp_path: Path) -> None:
    start = date(2026, 8, 13)
    records = [
        _prospective_day((start + timedelta(days=offset)).isoformat())
        for offset in range(30)
    ]
    admission = successor.select_prospective_development_days(records)

    folds = successor.build_prospective_fold_manifest(admission)
    output = tmp_path / "folds.json"
    first_sha = successor.write_prospective_fold_manifest(folds, output)
    second_sha = successor.write_prospective_fold_manifest(folds, output)

    assert len(folds.outer_folds) == 4
    assert all(len(row["inner_folds"]) == 3 for row in folds.outer_folds)
    assert folds.outer_folds[0]["train_days"] == list(folds.active_days[:10])
    assert folds.outer_folds[0]["test_days"] == list(folds.active_days[10:15])
    assert folds.outer_folds[-1]["train_days"] == list(folds.active_days[:25])
    assert folds.outer_folds[-1]["test_days"] == list(folds.active_days[25:30])
    assert first_sha == second_sha == hashlib.sha256(output.read_bytes()).hexdigest()


def test_complete_hierarchy_includes_continuous_minus_boolean() -> None:
    bands = {}
    for side in ("BUY", "SELL"):
        for suffix in successor.COMPLETE_HIERARCHY_SUFFIXES:
            name = f"new:{side}:{suffix}"
            lcb = 0.1
            if side == "BUY" and suffix == "CONTINUOUS-BOOLEAN":
                lcb = -0.1
            bands[name] = SimultaneousBand(
                hypothesis=name,
                mean_usdc=0.2,
                standard_error_usdc=0.01,
                lcb_usdc=lcb,
                ucb_usdc=0.3,
                day_count=30,
            )

    decision = successor.apply_complete_feature_hierarchy(bands, prefix="new")

    assert len(decision.steps["BUY"]) == 5
    assert len(decision.steps["SELL"]) == 5
    assert decision.steps["BUY"][-1].hypothesis.endswith("CONTINUOUS-BOOLEAN")
    assert decision.steps["SELL"][-1].tested is True
    assert decision.steps["SELL"][-1].passed is False
    assert decision.steps["SELL"][-1].reason == "continuous_comparator_superior"
    assert decision.supported_sides == ("BUY",)


def test_replicating_unidentified_rows_cannot_change_identified_policy() -> None:
    actions = duration_vocabulary("SELL")[1:3]
    truth = np.asarray(list(product((0, 1), repeat=4)), dtype=np.int8)
    base_matrix = np.repeat(truth, 6, axis=0)

    def fit(extra_unknown_rows: int):
        unknown = np.zeros((extra_unknown_rows, 4), dtype=np.int8)
        matrix = np.vstack((base_matrix, unknown))
        index = pd.Index([f"row-{row}" for row in range(len(matrix))])
        features = pd.DataFrame(
            matrix,
            index=index,
            columns=["p1", "p2", "p3", "p4"],
        )
        first = np.where(
            (matrix[:, 0] == 1) & (matrix[:, 1] == 1) & (matrix[:, 2] == 1),
            2.0,
            -1.0,
        )
        second = np.where(
            (matrix[:, 0] == 0) & (matrix[:, 2] == 1) & (matrix[:, 3] == 1),
            3.0,
            -1.0,
        )
        outcomes = pd.DataFrame(
            {CONTROL_ACTION: 0.0, actions[0]: first, actions[1]: second},
            index=index,
        )
        supported = pd.DataFrame(True, index=index, columns=outcomes.columns)
        if extra_unknown_rows:
            supported.loc[index[-extra_unknown_rows:], list(actions)] = False
        targets = successor.build_identified_action_targets(
            outcomes,
            supported,
            actions=actions,
        )
        metadata = pd.DataFrame(
            {
                "utc_day": "2026-08-13",
                "campaign_cluster_id": [f"campaign-{row // 2}" for row in range(len(index))],
            },
            index=index,
        )
        profile = successor.SuccessorSearchProfile(
            feature_budget=4,
            max_depth=4,
            max_leaf_nodes=16,
            min_samples_leaf=2,
            max_rules=4,
            max_clauses_per_rule=8,
            max_literals_per_clause=4,
        )
        return successor.fit_identified_action_policy(
            features,
            metadata,
            targets,
            side="SELL",
            feature_names=tuple(features.columns),
            profile=profile,
            random_seed=31,
        )[0]

    assert fit(0).candidate_id == fit(25).candidate_id


def test_unidentified_rows_cannot_change_feature_distribution_screen() -> None:
    actions = duration_vocabulary("SELL")[1:2]

    def screen(extra_unknown_rows: int) -> tuple[tuple[str, ...], successor.FeaturePoolAudit]:
        identified_rows = 12
        index = pd.Index(
            [f"identified-{row}" for row in range(identified_rows)]
            + [f"unknown-{row}" for row in range(extra_unknown_rows)]
        )
        features = pd.DataFrame(
            {
                "identified_signal": [0, 1] * 6 + [-1] * extra_unknown_rows,
                "unsupported_only_signal": [0] * identified_rows
                + [row % 2 for row in range(extra_unknown_rows)],
            },
            index=index,
            dtype=np.int8,
        )
        outcomes = pd.DataFrame(
            {
                CONTROL_ACTION: np.zeros(len(index)),
                actions[0]: np.concatenate(
                    (np.zeros(identified_rows), np.zeros(extra_unknown_rows))
                ),
            },
            index=index,
        )
        supported = pd.DataFrame(True, index=index, columns=outcomes.columns)
        if extra_unknown_rows:
            supported.loc[index[-extra_unknown_rows:], actions[0]] = False
        targets = successor.build_identified_action_targets(
            outcomes,
            supported,
            actions=actions,
        )
        metadata = pd.DataFrame(
            {
                "utc_day": "2026-08-13",
                "campaign_cluster_id": [f"campaign-{row}" for row in range(len(index))],
            },
            index=index,
        )
        return successor.build_inner_train_feature_pool(
            features,
            metadata,
            train_index=index,
            candidates=tuple(features.columns),
            feature_budget=1,
            fold_id="identified-only-screen",
            targets=targets,
        )

    base_selected, base_audit = screen(0)
    copied_selected, copied_audit = screen(40)

    assert base_selected == copied_selected == ("identified_signal",)
    assert base_audit.selected_sha256 == copied_audit.selected_sha256
    assert base_audit.identified_screen_rows == copied_audit.identified_screen_rows == 12


def test_coverage_and_gtx_contracts_fail_closed() -> None:
    warmup = successor.classify_coverage(
        eligible=True,
        feature_ready=False,
        support_valid=False,
        action_id=CONTROL_ACTION,
        fallback_reason="receive_time_ema_warmup_incomplete",
    )
    candidate = successor.classify_coverage(
        eligible=True,
        feature_ready=True,
        support_valid=True,
        action_id="FIXED_166S",
        fallback_reason=None,
    )
    exact = successor.classify_gtx_exposure(
        exchange_error_code=-5022,
        exchange_reject_confirmed=True,
        activation_observed=False,
        transport_timeout=False,
        response_lost=False,
        ack_state_known=True,
    )
    unknown = successor.classify_gtx_exposure(
        exchange_error_code=-5022,
        exchange_reject_confirmed=False,
        activation_observed=False,
        transport_timeout=True,
        response_lost=True,
        ack_state_known=False,
    )
    unobserved = successor.classify_coverage(
        eligible=True,
        feature_ready=False,
        support_valid=True,
        action_id=CONTROL_ACTION,
        fallback_reason=None,
    )
    unidentified = successor.classify_coverage(
        eligible=True,
        feature_ready=True,
        support_valid=False,
        action_id=CONTROL_ACTION,
        fallback_reason=None,
    )

    assert warmup.reason is successor.CoverageReason.WARMUP_INCOMPLETE
    assert candidate.reason is successor.CoverageReason.ELIGIBLE_FEATURE_READY
    assert unobserved.reason is successor.CoverageReason.PREDICATE_UNOBSERVED
    assert unidentified.reason is successor.CoverageReason.LIFECYCLE_UNIDENTIFIED
    assert exact.encoding is successor.ExposureEncoding.EXACT_ZERO_EXPOSURE
    assert exact.point_identified is True
    assert unknown.encoding is successor.ExposureEncoding.CENSORED_UNKNOWN_EXPOSURE
    assert unknown.point_identified is False


def _single_predicate_policy(*, side: str, predicate: str):
    action = duration_vocabulary(side)[1]
    return successor.BooleanCooldownPolicy(
        side=side,
        rules=(
            successor.BooleanRule(
                action=action,
                clauses=(
                    successor.AndClause(
                        literals=(successor.TriLiteral(predicate=predicate),)
                    ),
                ),
            ),
        ),
    )


def test_research_policy_evaluator_preserves_tri_state_and_side_control() -> None:
    policy = _single_predicate_policy(side="SELL", predicate="predicate::test")
    evaluator = successor.ResearchBooleanCooldownPolicyEvaluator(
        policies={"BUY": None, "SELL": policy},
        policy_identity="synthetic_successor_fold_policy",
    )

    matched = evaluator.evaluate_predicates(
        side="SELL",
        predicate_values={"predicate::test": 1},
        baseline_duration_ms=85_000,
        snapshot_id="matched",
    )
    unobserved = evaluator.evaluate_predicates(
        side="SELL",
        predicate_values={"predicate::test": -1},
        baseline_duration_ms=85_000,
        snapshot_id="unobserved",
    )
    buy = evaluator.evaluate_predicates(
        side="BUY",
        predicate_values={},
        baseline_duration_ms=170_000,
        snapshot_id="buy",
    )

    assert matched.action_id == duration_vocabulary("SELL")[1]
    assert matched.duration_ms != 85_000
    assert matched.support_valid is True
    assert unobserved.action_id == CONTROL_ACTION
    assert unobserved.fallback_reason == "rule_unobserved:0"
    assert unobserved.support_valid is False
    assert buy.action_id == CONTROL_ACTION
    assert buy.duration_ms == 170_000
    assert buy.fallback_reason == "buy_control_by_contract"
    audit = evaluator.audit()
    assert audit["evaluations"] == 3
    assert audit["nonbaseline"] == 1
    assert audit["live_authorized"] is False


class _SyntheticRepeatedEvaluator:
    def __init__(self, policy_sha256: str) -> None:
        self.policy_sha256 = policy_sha256
        self.binding_valid = True


def _common_replay_identity() -> dict[str, object]:
    return {
        "market_source_sha256": "3" * 64,
        "lifecycle_source_sha256": "4" * 64,
        "latency_profile_sha256": "5" * 64,
        "random_seed": 42,
        "coverage_contract": "prospective_receive_time_common_v1",
        "transport_common_market_source_sha256": "9" * 64,
    }


def _canonical_hash(payload: object) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _transport_receipt(arm: str, *, supported: bool = True) -> dict[str, object]:
    body: dict[str, object] = {
        "schema_version": transport.TRANSPORT_RECEIPT_SCHEMA_VERSION,
        "identity": transport.TRANSPORT_IDENTITY,
        "arm": arm,
        "common_market_source_sha256": "9" * 64,
        "arm_fill_source_sha256": ("a" if arm == "control" else "b") * 64,
        "delay_artifact_sha256": None if arm == "control" else "c" * 64,
        "private_fill_visibility_authority": (
            "recorded_exact" if arm == "control" else "modeled_sensitivity"
        ),
        "book_event_count": 1,
        "book_visible_count": 1,
        "trade_event_count": 1,
        "trade_visible_count": 1,
        "fill_truth_count": 1,
        "private_fill_visible_count": 1 if supported else 0,
        "counterfactual_fill_censored_count": 0 if supported else 1,
        "source_gap_count": 0,
        "pre_exchange_clamp_count": 0,
        "head_of_line_clamp_count": 0,
        "clock_inversion_count": 0,
        "future_visibility_violation_count": 0,
        "ambiguous_same_timestamp_count": 0,
        "pending_private_fill_count": 0,
        "formal_replay_support_valid": supported,
        "live_equivalent": False,
        "thread_interleaving_replayed": False,
        "rest_user_stream_reconnect_replayed": False,
        "action_authorized": False,
        "live_policy_authorized": False,
        "exclusion_reasons": (
            () if supported else ("counterfactual_private_fill_delay_unsupported",)
        ),
    }
    return {**body, "transport_receipt_sha256": _canonical_hash(body)}


def test_paired_repeated_runner_executes_exact_owner_and_candidate_policies() -> None:
    common = _common_replay_identity()
    common_sha = _canonical_hash(common)
    owner = _SyntheticRepeatedEvaluator(successor.ACTIVE_OWNER_POLICY_SHA256)
    candidate = _SyntheticRepeatedEvaluator("6" * 64)
    calls: list[tuple[str, str]] = []

    def simulate(arm, evaluator, received_common):
        calls.append((arm, evaluator.policy_sha256))
        assert received_common == common
        return {
            "arm": arm,
            "policy_sha256": evaluator.policy_sha256,
            "common_input_identity_sha256": common_sha,
            "execution_copied_from_other_arm": False,
            "one_shot_effect_aggregation_used": False,
            "formal_support_valid": True,
            "formal_exclusion_reasons": [],
            "repeated_policy_evaluation_count": 12,
            "campaign_terminal_value_usdc": 10.0 if arm == "control" else 12.5,
            "transport_receipt": _transport_receipt(arm),
        }

    audit = successor.execute_paired_repeated_policy(
        utc_day="2026-08-13",
        common_input_identity=common,
        exact_owner_evaluator=owner,
        candidate_evaluator=candidate,
        simulator=simulate,
        formal_economic_mode=True,
        prospective_day_admission=successor.parse_prospective_day_admission(
            _prospective_day("2026-08-13")
        ),
    )

    assert calls == [
        ("control", successor.ACTIVE_OWNER_POLICY_SHA256),
        ("candidate", "6" * 64),
    ]
    assert audit.both_arms_executed_policy_function is True
    assert audit.formal_denominator_eligible is True
    assert audit.terminal_value_delta_usdc == 2.5
    assert audit.execution_copied_between_arms is False
    assert audit.one_shot_effect_aggregation_used is False


def test_unsupported_repeated_arm_exits_formal_denominator_without_zero_delta() -> None:
    common = _common_replay_identity()
    common_sha = _canonical_hash(common)
    owner = _SyntheticRepeatedEvaluator(successor.ACTIVE_OWNER_POLICY_SHA256)
    candidate = _SyntheticRepeatedEvaluator("7" * 64)
    calls: list[str] = []

    def simulate(arm, evaluator, _received_common):
        calls.append(arm)
        supported = arm == "control"
        return {
            "arm": arm,
            "policy_sha256": evaluator.policy_sha256,
            "common_input_identity_sha256": common_sha,
            "execution_copied_from_other_arm": False,
            "one_shot_effect_aggregation_used": False,
            "formal_support_valid": supported,
            "formal_exclusion_reasons": [] if supported else ["source_unavailable"],
            "repeated_policy_evaluation_count": 9 if supported else 0,
            "campaign_terminal_value_usdc": 10.0 if supported else None,
            "transport_receipt": _transport_receipt(arm),
        }

    audit = successor.execute_paired_repeated_policy(
        utc_day="2026-08-13",
        common_input_identity=common,
        exact_owner_evaluator=owner,
        candidate_evaluator=candidate,
        simulator=simulate,
        formal_economic_mode=True,
        prospective_day_admission=successor.parse_prospective_day_admission(
            _prospective_day("2026-08-13")
        ),
    )

    assert calls == ["control", "candidate"]
    assert audit.formal_denominator_eligible is False
    assert audit.terminal_value_delta_usdc is None
    assert "candidate:source_unavailable" in audit.exclusion_reasons
    assert "candidate:policy_function_not_executed" in audit.exclusion_reasons


def test_paired_repeated_runner_rejects_copy_and_one_shot_shortcuts() -> None:
    common = _common_replay_identity()
    common_sha = _canonical_hash(common)
    owner = _SyntheticRepeatedEvaluator(successor.ACTIVE_OWNER_POLICY_SHA256)
    candidate = _SyntheticRepeatedEvaluator("8" * 64)

    def copied(arm, evaluator, _received_common):
        return {
            "arm": arm,
            "policy_sha256": evaluator.policy_sha256,
            "common_input_identity_sha256": common_sha,
            "execution_copied_from_other_arm": arm == "candidate",
            "one_shot_effect_aggregation_used": False,
            "formal_support_valid": True,
            "formal_exclusion_reasons": [],
            "repeated_policy_evaluation_count": 1,
            "campaign_terminal_value_usdc": 1.0,
            "transport_receipt": _transport_receipt(arm),
        }

    with np.testing.assert_raises(successor.SuccessorContractError):
        successor.execute_paired_repeated_policy(
            utc_day="2026-08-13",
            common_input_identity=common,
            exact_owner_evaluator=owner,
            candidate_evaluator=candidate,
            simulator=copied,
            formal_economic_mode=True,
            prospective_day_admission=successor.parse_prospective_day_admission(
                _prospective_day("2026-08-13")
            ),
        )
