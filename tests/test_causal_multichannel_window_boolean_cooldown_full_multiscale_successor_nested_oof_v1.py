from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 as successor,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    CONTROL_ACTION,
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    TriLiteral,
    duration_vocabulary,
)


def _fixed_policy(side: str, predicate: str, action_offset: int) -> BooleanCooldownPolicy:
    action = duration_vocabulary(side)[1 + action_offset]
    return BooleanCooldownPolicy(
        side=side,
        rules=(
            BooleanRule(
                action=action,
                clauses=(AndClause((TriLiteral(predicate),)),),
            ),
        ),
    )


def _e2_pair_features(pair_features: tuple[str, ...]) -> tuple[str, ...]:
    suffixes = (
        "ordering_favorable",
        "last_cross_direction_golden",
        "cross_age_le_slow",
        "persistence_ge_slow",
        "signed_distance_ge_zero",
        "volatility_normalized_ge_one",
        "signed_distance_velocity_positive",
        "signed_distance_acceleration_positive",
        "converging",
        "expanding",
    )
    return tuple(
        f"{name.rsplit(':', 1)[0]}:{suffix}"
        for name in pair_features
        for suffix in suffixes
    )


def _fold_manifest(days: tuple[str, ...]) -> successor.ProspectiveFoldManifest:
    admission = successor.ProspectivePanelAdmission(
        selected_active_days=days,
        all_eligible_days=days,
        rejected_days={},
        required_active_days=30,
        ready_for_new_economic_panel=True,
        selection_sha256="a" * 64,
    )
    return successor.build_prospective_fold_manifest(admission)


def _synthetic_panel() -> tuple[
    nested.NestedOofPanel,
    tuple[str, ...],
    tuple[str, ...],
    str,
]:
    days = tuple((date(2026, 8, 13) + timedelta(days=offset)).isoformat() for offset in range(30))
    rows_per_day = 8
    index = pd.Index(
        [f"SELL:{day}:{slot}" for day in days for slot in range(rows_per_day)],
        name="opportunity_id",
    )
    utc_days = [day for day in days for _ in range(rows_per_day)]
    slots = np.tile(np.arange(rows_per_day), len(days))
    day_numbers = np.repeat(np.arange(len(days)), rows_per_day)
    assignments = np.asarray(
        [
            pd.Timestamp(day, tz="UTC").value + int(slot) * 1_000_000_000
            for day, slot in zip(utc_days, slots, strict=True)
        ],
        dtype=np.int64,
    )
    metadata = pd.DataFrame(
        {
            "utc_day": utc_days,
            "panel_role": nested.PANEL_ROLE,
            "side": "SELL",
            "campaign_cluster_id": [f"campaign-{row}" for row in range(len(index))],
            "assignment_ts_ns": assignments,
            "observation_end_ts_ns": assignments + 250_000_000,
            "role_at_fill": np.where(slots % 3 == 0, "opener", "add"),
        },
        index=index,
    )

    pair_features = tuple(
        f"predicate::{prefix}:ordering_favorable"
        for prefix in successor.full_ema_pair_prefixes()
    )
    e2_pair_features = _e2_pair_features(pair_features)
    m0 = "predicate::m0::campaign_age_gt_control_duration"
    directional = "predicate::ema_pair_h1s_h8s:last_cross_direction_golden"
    m2 = "predicate::trade_flow::aggressive_sell_volume_high"
    future_only = "predicate::depth::future_only_refill_state"
    values: dict[str, np.ndarray] = {}
    for offset, name in enumerate(e2_pair_features):
        values[name] = ((slots + day_numbers + offset) % (3 + offset % 4) == 0).astype(np.int8)
    values[m0] = ((slots + day_numbers) % 3 == 0).astype(np.int8)
    values[directional] = ((2 * slots + day_numbers) % 5 < 2).astype(np.int8)
    values[m2] = ((slots + 2 * day_numbers) % 4 == 0).astype(np.int8)
    values[future_only] = np.where(
        day_numbers < 10,
        -1,
        ((slots + day_numbers) % 2 == 0).astype(np.int8),
    ).astype(np.int8)
    boolean_features = pd.DataFrame(values, index=index, dtype=np.int8)
    continuous_features = pd.DataFrame(
        {
            "continuous::mid_trend": (
                0.8 * boolean_features[pair_features[0]].to_numpy(dtype=float)
                + 0.5 * boolean_features[directional].to_numpy(dtype=float)
                - 0.4
            ),
            "continuous::flow": boolean_features[m2].to_numpy(dtype=float) - 0.3,
        },
        index=index,
    )

    all_actions = tuple(
        dict.fromkeys((*duration_vocabulary("BUY"), *duration_vocabulary("SELL")))
    )
    outcomes = pd.DataFrame(0.0, index=index, columns=all_actions)
    sell_actions = duration_vocabulary("SELL")
    p0 = boolean_features[pair_features[0]].to_numpy(dtype=bool)
    direction = boolean_features[directional].to_numpy(dtype=bool)
    flow = boolean_features[m2].to_numpy(dtype=bool)
    outcomes[CONTROL_ACTION] = 0.0
    outcomes[sell_actions[1]] = np.where(p0, 0.070, -0.025)
    outcomes[sell_actions[2]] = np.where(direction, 0.055, -0.020)
    outcomes[sell_actions[3]] = np.where(flow, 0.085, -0.030)
    for offset, action in enumerate(sell_actions[4:], start=4):
        outcomes[action] = np.where((slots + offset) % 7 == 0, 0.025, -0.018)
    for action in set(duration_vocabulary("BUY")) - {CONTROL_ACTION}:
        outcomes[action] = np.where(p0, 0.010, -0.010)
    supported = pd.DataFrame(True, index=index, columns=outcomes.columns)
    owner = pd.Series(
        np.where((slots + day_numbers) % 7 == 0, sell_actions[1], CONTROL_ACTION),
        index=index,
        name="exact_owner_action",
    )
    panel = nested.NestedOofPanel(
        metadata=metadata,
        boolean_features=boolean_features,
        continuous_features=continuous_features,
        action_outcomes=outcomes,
        action_supported=supported,
        exact_owner_actions=owner,
    )
    return panel, days, pair_features, future_only


def _ladder(
    pair_features: tuple[str, ...],
    future_only: str,
) -> tuple[nested.CandidateLadderEntry, ...]:
    side = "SELL"
    m0 = "predicate::m0::campaign_age_gt_control_duration"
    directional = "predicate::ema_pair_h1s_h8s:last_cross_direction_golden"
    m2 = "predicate::trade_flow::aggressive_sell_volume_high"
    e2_pair_features = _e2_pair_features(pair_features)
    profile = successor.SuccessorSearchProfile(
        name="synthetic_bounded",
        feature_budget=64,
        max_depth=4,
        max_leaf_nodes=12,
        min_samples_leaf=2,
        max_rules=4,
        max_clauses_per_rule=8,
        max_literals_per_clause=4,
    )
    return (
        nested.CandidateLadderEntry("B0_CURRENT_EXACT", "exact_owner"),
        nested.CandidateLadderEntry(
            "B1_CAMPAIGN_AGE_ONLY",
            "fixed",
            fixed_policy_by_side={side: _fixed_policy(side, m0, 0)},
        ),
        nested.CandidateLadderEntry(
            "B2_CAMPAIGN_PLUS_H16_H256",
            "fixed",
            fixed_policy_by_side={side: _fixed_policy(side, directional, 1)},
        ),
        nested.CandidateLadderEntry(
            "B3_CURRENT_SEMANTIC_EQUIVALENT",
            "fixed",
            fixed_policy_by_side={side: _fixed_policy(side, pair_features[0], 0)},
        ),
        nested.CandidateLadderEntry(
            "E1_FULL_EMA_BANK",
            "boolean",
            feature_names_by_side={side: pair_features},
            profiles=(profile,),
        ),
        nested.CandidateLadderEntry(
            "E2_DIRECTIONAL_EMA",
            "boolean",
            feature_names_by_side={side: e2_pair_features},
            profiles=(profile,),
        ),
        nested.CandidateLadderEntry(
            "E3_HIGHER_ORDER_BOOLEAN",
            "boolean",
            feature_names_by_side={side: (*e2_pair_features, m0)},
            profiles=(profile,),
        ),
        nested.CandidateLadderEntry(
            "M2_TRUE_INCREMENTAL",
            "boolean",
            feature_names_by_side={
                side: (*e2_pair_features, m0, m2, future_only)
            },
            profiles=(profile,),
        ),
        nested.CandidateLadderEntry(
            "ACTION_MATCHED_CONTROLS",
            "action_matched",
            match_sources=nested.LEARNED_BOOLEAN_ORDER,
        ),
    )


class _SyntheticSequentialEvaluator:
    def __init__(self, panel: nested.NestedOofPanel, *, one_shot: bool = False) -> None:
        self.panel = panel
        self.one_shot = one_shot
        self.requests: list[nested.EvaluationRequest] = []

    def __call__(self, request: nested.EvaluationRequest) -> pd.DataFrame:
        self.requests.append(request)
        metadata = self.panel.metadata
        mask = (metadata["side"] == request.side) & metadata["utc_day"].isin(request.days)
        index = metadata.index[mask]
        combined = pd.concat(
            [
                self.panel.boolean_features.loc[index],
                self.panel.continuous_features.loc[index],
            ],
            axis=1,
        )
        owner = self.panel.exact_owner_actions.loc[index]
        selected = request.candidate.choose(combined, owner)
        action_columns = tuple(self.panel.action_outcomes.columns)
        result: list[dict[str, object]] = []
        for day in request.days:
            positions = np.flatnonzero(metadata.loc[index, "utc_day"].to_numpy() == day)
            day_index = index[positions]
            day_actions = selected[positions]
            day_owner = owner.loc[day_index].to_numpy(dtype=object)
            candidate_values = np.asarray(
                [
                    self.panel.action_outcomes.loc[row, action]
                    for row, action in zip(day_index, day_actions, strict=True)
                ],
                dtype=float,
            )
            owner_values = np.asarray(
                [
                    self.panel.action_outcomes.loc[row, action]
                    for row, action in zip(day_index, day_owner, strict=True)
                ],
                dtype=float,
            )
            candidate_supported = np.asarray(
                [
                    self.panel.action_supported.loc[row, action]
                    for row, action in zip(day_index, day_actions, strict=True)
                ],
                dtype=bool,
            )
            owner_supported = np.asarray(
                [
                    self.panel.action_supported.loc[row, action]
                    for row, action in zip(day_index, day_owner, strict=True)
                ],
                dtype=bool,
            )
            identified = bool(candidate_supported.all() and owner_supported.all())
            candidate_value = float(candidate_values.mean()) if identified else np.nan
            owner_value = float(owner_values.mean()) if identified else np.nan
            nonbaseline_count = int(np.sum(day_actions != day_owner))
            receipt_sha = hashlib.sha256(
                (
                    f"{request.fold_id}|{request.stage}|{request.side}|"
                    f"{request.candidate.policy_sha256}|{day}"
                ).encode("ascii")
            ).hexdigest()
            row: dict[str, object] = {
                "utc_day": day,
                "side": request.side,
                "panel_role": nested.PANEL_ROLE,
                "candidate_terminal_value_usdc": candidate_value,
                "exact_owner_terminal_value_usdc": owner_value,
                "candidate_closed_campaign_value_usdc": candidate_value,
                "exact_owner_closed_campaign_value_usdc": owner_value,
                "candidate_campaign_q10_usdc": candidate_value,
                "exact_owner_campaign_q10_usdc": owner_value,
                "candidate_campaign_cvar10_usdc": candidate_value,
                "exact_owner_campaign_cvar10_usdc": owner_value,
                "candidate_inventory_time_btc_s": 1.0 if identified else np.nan,
                "exact_owner_inventory_time_btc_s": 1.0 if identified else np.nan,
                "candidate_max_abs_inventory_btc": 0.001 if identified else np.nan,
                "exact_owner_max_abs_inventory_btc": 0.001 if identified else np.nan,
                "point_identified": identified,
                "policy_assignment_count": len(day_index),
                "nonbaseline_action_count": nonbaseline_count,
                "feature_ready_active_treatment_events": len(day_index),
                "common_row_count": len(day_index),
                "common_campaign_count": len(day_index),
                "candidate_fill_count": len(day_index),
                "exact_owner_fill_count": len(day_index),
                "candidate_negative_terminal_rate": (
                    float(candidate_value < 0.0) if identified else np.nan
                ),
                "exact_owner_negative_terminal_rate": (
                    float(owner_value < 0.0) if identified else np.nan
                ),
                "candidate_campaign_mae_usdc": (
                    abs(candidate_value) if identified else np.nan
                ),
                "exact_owner_campaign_mae_usdc": (
                    abs(owner_value) if identified else np.nan
                ),
                "candidate_repair_event_rate": 0.5 if identified else np.nan,
                "exact_owner_repair_event_rate": 0.5 if identified else np.nan,
                "candidate_mean_repair_time_s": 100.0 if identified else np.nan,
                "exact_owner_mean_repair_time_s": 100.0 if identified else np.nan,
                "candidate_censoring_rate": 0.0 if identified else np.nan,
                "exact_owner_censoring_rate": 0.0 if identified else np.nan,
                "repeated_sequential_policy": True,
                "one_shot_effect_aggregation_used": self.one_shot,
                "exact_current_owner_row_wise_baseline": True,
                "candidate_executed_policy_sha256": (
                    request.candidate.expected_executed_policy_sha256
                ),
                "exact_owner_executed_policy_sha256": (
                    successor.ACTIVE_OWNER_POLICY_SHA256
                ),
                "paired_replay_receipt_sha256": receipt_sha,
                "candidate_target_side": request.side,
                "same_market_source": True,
                "common_random_source": True,
                "arm_local_state": True,
            }
            row.update(
                {
                    f"action_count::{action}": int(np.sum(day_actions == action))
                    for action in action_columns
                }
            )
            roles = metadata.loc[day_index, "role_at_fill"].astype(str)
            row.update(
                {
                    "role_count::opener": int((roles == "opener").sum()),
                    "role_count::add": int((roles == "add").sum()),
                    "consecutive_units_count::1": len(day_index),
                    "fallback_count::eligible_feature_ready": nonbaseline_count,
                    "fallback_count::policy_control": len(day_index)
                    - nonbaseline_count,
                }
            )
            result.append(row)
        return pd.DataFrame(result)


def _mechanics_only(panel: nested.NestedOofPanel) -> nested.NestedOofPanel:
    return replace(
        panel,
        action_outcomes=None,
        action_supported=None,
        learning_label_request_sha256=None,
        learning_label_payload_sha256=None,
        learning_label_receipt_sha256=None,
    )


class _SyntheticFoldLabelProvider:
    def __init__(self, source: nested.NestedOofPanel) -> None:
        assert source.action_outcomes is not None
        assert source.action_supported is not None
        self.source = source
        self.requests: list[nested.FoldScopedOneShotLabelRequest] = []

    def __call__(
        self,
        request: nested.FoldScopedOneShotLabelRequest,
    ) -> nested.FoldScopedOneShotLabelBatch:
        self.requests.append(request)
        index = pd.Index(request.row_ids, name=self.source.metadata.index.name)
        outcomes = self.source.action_outcomes.loc[
            index, list(request.duration_vocabulary)
        ].copy()
        supported = self.source.action_supported.loc[
            index, list(request.duration_vocabulary)
        ].copy()
        return nested.bind_fold_scoped_one_shot_labels(
            request,
            outcomes=outcomes,
            supported=supported,
            provider_identity="synthetic_fold_scoped_provider.v1",
            provider_artifact_sha256="b" * 64,
        )


def _formal_run(
    panel: nested.NestedOofPanel,
    days: tuple[str, ...],
    pair_features: tuple[str, ...],
    future_only: str,
    provider: nested.FoldScopedOneShotLabelProvider,
) -> nested.NestedOofExecutionResult:
    ladder = _ladder(pair_features, future_only)
    continuous = nested.ContinuousComparatorEntry(
        feature_names_by_side={
            "SELL": ("continuous::mid_trend", "continuous::flow")
        },
        profiles=(ladder[4].profiles[0],),
    )
    return nested.run_nested_chronological_oof(
        _mechanics_only(panel),
        fold_manifest=_fold_manifest(days),
        ladder=ladder,
        continuous=continuous,
        evaluator=_SyntheticSequentialEvaluator(panel),
        label_provider=provider,
        config=nested.NestedOofConfig(
            sides=("SELL",),
            simultaneous_draws=19,
            simultaneous_seed=23,
        ),
    )


@pytest.fixture(scope="module")
def completed_run():
    panel, days, pair_features, future_only = _synthetic_panel()
    ladder = _ladder(pair_features, future_only)
    profile = ladder[4].profiles[0]
    continuous = nested.ContinuousComparatorEntry(
        feature_names_by_side={
            "SELL": ("continuous::mid_trend", "continuous::flow")
        },
        profiles=(profile,),
    )
    evaluator = _SyntheticSequentialEvaluator(panel)
    config = nested.NestedOofConfig(
        sides=("SELL",),
        simultaneous_draws=199,
        simultaneous_seed=17,
    )
    result = nested.run_nested_chronological_oof(
        panel,
        fold_manifest=_fold_manifest(days),
        ladder=ladder,
        continuous=continuous,
        evaluator=evaluator,
        config=config,
    )
    return result, evaluator, panel, days, ladder, continuous, config, future_only


@pytest.fixture(scope="module")
def formal_completed_run():
    panel, days, pair_features, future_only = _synthetic_panel()
    provider = _SyntheticFoldLabelProvider(panel)
    result = _formal_run(panel, days, pair_features, future_only, provider)
    return result, provider, panel, days


def test_executes_full_ladder_as_learning_algorithm_oof(completed_run) -> None:
    result, _, _, _, _, _, _, _ = completed_run
    expected = (
        set(successor.SUCCESSOR_CANDIDATE_LADDER)
        - {"ACTION_MATCHED_CONTROLS"}
        | set(nested.MATCHED_CONTROL_NAMES)
        | {nested.CONTINUOUS_COMPARATOR}
    )

    assert set(result.oof_rows["candidate_name"]) == expected
    assert len(result.oof_rows) == 20 * len(expected)
    assert result.evidence_scope == nested.OOF_EVIDENCE_SCOPE
    assert result.exact_final_artifact_oof_available is False
    assert result.final_refit_performed is False
    assert len(result.fold_records) == 4
    assert all(
        not row["outer_test_outcomes_used_for_fit"]
        for row in result.fold_records
        for row in row["candidates"].values()
    )
    report = result.report()
    assert report["permissions"]["action_authorized"] is False
    assert report["permissions"]["live_authorized"] is False
    assert "SELL:E1_FULL_EMA_BANK" in result.scorecards
    assert result.scorecards["SELL:E1_FULL_EMA_BANK"]["profile"]["profile_id"] == (
        "action_alpha_v1"
    )
    assert (
        "SELL:E1_FULL_EMA_BANK:negative_terminal_protection"
        in result.risk_bands.bands
    )


def test_formal_provider_is_called_once_per_outer_fold_with_only_outer_train_rows(
    formal_completed_run,
) -> None:
    result, provider, panel, days = formal_completed_run
    manifest = _fold_manifest(days)
    mechanics = _mechanics_only(panel)

    assert len(provider.requests) == len(manifest.outer_folds)
    for request, outer, record in zip(
        provider.requests,
        manifest.outer_folds,
        result.fold_records,
        strict=True,
    ):
        expected_index, _ = nested._purged_train_index(  # noqa: SLF001
            mechanics,
            side="SELL",
            train_days=outer["train_days"],
            test_days=outer["test_days"],
            fold_id=str(outer["fold_id"]),
            stage="test",
        )
        test_rows = set(
            mechanics.metadata.index[
                mechanics.metadata["utc_day"].isin(outer["test_days"])
            ]
        )
        assert request.side == "SELL"
        assert request.outer_fold_id == outer["fold_id"]
        assert request.train_days == tuple(outer["train_days"])
        assert request.row_ids == tuple(expected_index)
        assert not set(request.row_ids) & test_rows
        materialization = record["fold_scoped_label_materialization"]
        assert materialization["mode"] == "formal_fold_scoped_provider"
        assert materialization["outer_test_rows_requested"] == 0
        assert materialization["request_sha256"] == request.request_sha256
        assert len(materialization["receipt_sha256"]) == 64

    for record in result.fold_records:
        for name in (*nested.LEARNED_BOOLEAN_ORDER, nested.CONTINUOUS_COMPARATOR):
            candidate = record["candidates"][name]
            assert candidate["outer_test_outcomes_used_for_fit"] is False


def test_publishes_atomic_report_and_canonical_scorecards(
    completed_run,
    tmp_path,
) -> None:
    result, _, _, _, _, _, _, _ = completed_run
    manifest = nested.write_nested_oof_artifacts(result, tmp_path)

    assert (tmp_path / "nested_oof_report.json").is_file()
    assert (tmp_path / "SELL_E1_FULL_EMA_BANK.scorecard.json").is_file()
    assert (tmp_path / "manifest.json").is_file()
    scorecard = json.loads(
        (tmp_path / "SELL_E1_FULL_EMA_BANK.scorecard.json").read_text()
    )
    assert scorecard["profile"]["profile_id"] == "action_alpha_v1"
    assert manifest["exact_final_artifact_oof_available"] is False


def test_each_inner_fold_builds_its_own_purged_feature_pool(completed_run) -> None:
    result, _, _, _, _, _, _, future_only = completed_run
    first_outer = result.fold_records[0]
    evidence = first_outer["candidates"]["M2_TRUE_INCREMENTAL"][
        "inner_profile_evidence"
    ]
    assert len(evidence) == 1
    assert evidence[0]["valid"] is True
    assert len(evidence[0]["fold_fits"]) == 3
    for fit in evidence[0]["fold_fits"]:
        selected = fit["fit_audit"]["selected_feature_names"]
        assert future_only not in selected
        assert set(fit["feature_pool_audit"]["train_days"]) == set(
            fit["training_days"]
        )
        assert max(fit["training_days"]) < min(
            next(
                fold["test_days"]
                for fold in successor.build_prospective_fold_manifest(
                    successor.ProspectivePanelAdmission(
                        selected_active_days=tuple(
                            (date(2026, 8, 13) + timedelta(days=offset)).isoformat()
                            for offset in range(30)
                        ),
                        all_eligible_days=tuple(
                            (date(2026, 8, 13) + timedelta(days=offset)).isoformat()
                            for offset in range(30)
                        ),
                        rejected_days={},
                        required_active_days=30,
                        ready_for_new_economic_panel=True,
                        selection_sha256="a" * 64,
                    )
                ).outer_folds[0]["inner_folds"]
                if fold["fold_id"] == fit["fold_id"]
            )
        )


def test_outer_candidates_are_frozen_before_their_test_days(completed_run) -> None:
    _, evaluator, _, _, _, _, _, _ = completed_run
    outer = [request for request in evaluator.requests if request.stage == "outer_oof"]
    assert outer
    for request in outer:
        assert request.candidate.learning_algorithm_fold_specific is True
        if request.candidate.training_days:
            assert max(request.candidate.training_days) < min(request.days)


def test_exact_owner_is_the_row_wise_b0_and_never_a_fixed_control(completed_run) -> None:
    result, _, panel, _, _, _, _, _ = completed_run
    b0 = result.oof_rows.loc[result.oof_rows["candidate_name"] == "B0_CURRENT_EXACT"]

    assert (b0["delta_usdc"].abs() <= 1e-12).all()
    assert (b0["nonbaseline_action_count"] == 0).all()
    assert set(panel.exact_owner_actions) == {CONTROL_ACTION, "FIXED_79S"}
    assert result.candidate_reports["SELL:B0_CURRENT_EXACT"]["zero_difference_days"] == 20


def test_unknown_action_targets_remain_missing_and_zero_imputation_fails() -> None:
    panel, days, _, _ = _synthetic_panel()
    row = panel.metadata.index[0]
    action = duration_vocabulary("SELL")[2]
    supported = panel.action_supported.copy()
    outcomes = panel.action_outcomes.copy()
    supported.loc[row, action] = False
    outcomes.loc[row, action] = np.nan
    valid = replace(panel, action_outcomes=outcomes, action_supported=supported)
    valid.validate(active_days=days, sides=("SELL",))

    outcomes.loc[row, action] = 0.0
    invalid = replace(panel, action_outcomes=outcomes, action_supported=supported)
    with pytest.raises(nested.NestedOofExecutionError, match="neutral-zero"):
        invalid.validate(active_days=days, sides=("SELL",))


def test_formal_mechanics_panel_requires_fold_scoped_provider() -> None:
    panel, days, pair_features, future_only = _synthetic_panel()
    ladder = _ladder(pair_features, future_only)
    continuous = nested.ContinuousComparatorEntry(
        feature_names_by_side={"SELL": ("continuous::mid_trend",)},
        profiles=(ladder[4].profiles[0],),
    )

    with pytest.raises(
        nested.NestedOofExecutionError,
        match="requires the formal fold-scoped label provider",
    ):
        nested.run_nested_chronological_oof(
            _mechanics_only(panel),
            fold_manifest=_fold_manifest(days),
            ladder=ladder,
            continuous=continuous,
            evaluator=_SyntheticSequentialEvaluator(panel),
            config=nested.NestedOofConfig(sides=("SELL",), simultaneous_draws=19),
        )


def test_formal_provider_rejects_outer_test_labels() -> None:
    panel, days, pair_features, future_only = _synthetic_panel()

    class OuterTestLeakingProvider(_SyntheticFoldLabelProvider):
        def __call__(
            self,
            request: nested.FoldScopedOneShotLabelRequest,
        ) -> nested.FoldScopedOneShotLabelBatch:
            batch = super().__call__(request)
            future = self.source.metadata.loc[
                ~self.source.metadata.index.isin(request.row_ids)
                & (self.source.metadata["utc_day"] > max(request.train_days))
            ].index[0]
            outcomes = pd.concat(
                [batch.outcomes, self.source.action_outcomes.loc[[future], batch.outcomes.columns]]
            )
            supported = pd.concat(
                [batch.supported, self.source.action_supported.loc[[future], batch.supported.columns]]
            )
            return replace(batch, outcomes=outcomes, supported=supported)

    with pytest.raises(nested.NestedOofExecutionError, match="outer-train index"):
        _formal_run(
            panel,
            days,
            pair_features,
            future_only,
            OuterTestLeakingProvider(panel),
        )


@pytest.mark.parametrize(
    ("field", "message"),
    (
        ("request_sha256", "request SHA256 drifted"),
        ("row_sha256", "row SHA256 drifted"),
        ("receipt_sha256", "receipt SHA256 drifted"),
    ),
)
def test_formal_provider_rejects_hash_drift(field: str, message: str) -> None:
    panel, days, pair_features, future_only = _synthetic_panel()

    class HashDriftProvider(_SyntheticFoldLabelProvider):
        def __call__(
            self,
            request: nested.FoldScopedOneShotLabelRequest,
        ) -> nested.FoldScopedOneShotLabelBatch:
            return replace(super().__call__(request), **{field: "0" * 64})

    with pytest.raises(nested.NestedOofExecutionError, match=message):
        _formal_run(
            panel,
            days,
            pair_features,
            future_only,
            HashDriftProvider(panel),
        )


def test_formal_provider_rejects_wrong_row_even_with_valid_shape() -> None:
    panel, days, pair_features, future_only = _synthetic_panel()

    class WrongRowProvider(_SyntheticFoldLabelProvider):
        def __call__(
            self,
            request: nested.FoldScopedOneShotLabelRequest,
        ) -> nested.FoldScopedOneShotLabelBatch:
            batch = super().__call__(request)
            replacement = next(
                row for row in self.source.metadata.index if row not in request.row_ids
            )
            outcomes = batch.outcomes.copy()
            supported = batch.supported.copy()
            wrong_index = list(outcomes.index)
            wrong_index[-1] = replacement
            outcomes.index = pd.Index(wrong_index, name=outcomes.index.name)
            supported.index = pd.Index(wrong_index, name=supported.index.name)
            return replace(batch, outcomes=outcomes, supported=supported)

    with pytest.raises(nested.NestedOofExecutionError, match="outer-train index"):
        _formal_run(
            panel,
            days,
            pair_features,
            future_only,
            WrongRowProvider(panel),
        )


def test_formal_provider_rejects_neutral_zero_for_unsupported_target() -> None:
    panel, days, pair_features, future_only = _synthetic_panel()

    class NeutralZeroProvider(_SyntheticFoldLabelProvider):
        def __call__(
            self,
            request: nested.FoldScopedOneShotLabelRequest,
        ) -> nested.FoldScopedOneShotLabelBatch:
            batch = super().__call__(request)
            outcomes = batch.outcomes.copy()
            supported = batch.supported.copy()
            outcomes.iloc[0, 1] = 0.0
            supported.iloc[0, 1] = False
            return replace(batch, outcomes=outcomes, supported=supported)

    with pytest.raises(nested.NestedOofExecutionError, match="neutral-zero"):
        _formal_run(
            panel,
            days,
            pair_features,
            future_only,
            NeutralZeroProvider(panel),
        )


def test_duplicating_fully_unsupported_rows_does_not_change_formal_policy() -> None:
    panel, days, pair_features, future_only = _synthetic_panel()
    assert panel.action_outcomes is not None
    assert panel.action_supported is not None
    unsupported_rows = panel.metadata.index[::8]
    outcomes = panel.action_outcomes.copy()
    supported = panel.action_supported.copy()
    outcomes.loc[unsupported_rows, :] = np.nan
    supported.loc[unsupported_rows, :] = False
    source = replace(panel, action_outcomes=outcomes, action_supported=supported)

    duplicate_ids = pd.Index(
        [f"{row}:unsupported-copy" for row in unsupported_rows],
        name=panel.metadata.index.name,
    )

    def duplicated(frame: pd.DataFrame) -> pd.DataFrame:
        extra = frame.loc[unsupported_rows].copy()
        extra.index = duplicate_ids
        return pd.concat([frame, extra])

    duplicate_metadata = duplicated(source.metadata)
    duplicate_metadata.loc[duplicate_ids, "campaign_cluster_id"] = [
        f"unsupported-copy-campaign-{offset}" for offset in range(len(duplicate_ids))
    ]
    duplicate_metadata.loc[duplicate_ids, "assignment_ts_ns"] += 1
    duplicate_metadata.loc[duplicate_ids, "observation_end_ts_ns"] += 1
    duplicate_owner = source.exact_owner_actions.loc[unsupported_rows].copy()
    duplicate_owner.index = duplicate_ids
    duplicated_source = nested.NestedOofPanel(
        metadata=duplicate_metadata,
        boolean_features=duplicated(source.boolean_features),
        continuous_features=duplicated(source.continuous_features),
        exact_owner_actions=pd.concat([source.exact_owner_actions, duplicate_owner]),
        action_outcomes=duplicated(source.action_outcomes),
        action_supported=duplicated(source.action_supported),
    )

    def fitted(source_panel: nested.NestedOofPanel):
        mechanics = _mechanics_only(source_panel)
        outer = _fold_manifest(days).outer_folds[0]
        train_index, _ = nested._purged_train_index(  # noqa: SLF001
            mechanics,
            side="SELL",
            train_days=outer["train_days"],
            test_days=outer["test_days"],
            fold_id=str(outer["fold_id"]),
            stage="test",
        )
        learning, audit = nested._materialize_outer_fold_learning_panel(  # noqa: SLF001
            mechanics,
            provider=_SyntheticFoldLabelProvider(source_panel),
            side="SELL",
            outer_fold_id=str(outer["fold_id"]),
            train_days=outer["train_days"],
            train_index=train_index,
        )
        entry = _ladder(pair_features, future_only)[4]
        candidate = nested._fit_boolean_candidate(  # noqa: SLF001
            learning,
            entry=entry,
            side="SELL",
            train_index=learning.metadata.index,
            fold_id=str(outer["fold_id"]),
            profile=entry.profiles[0],
            random_seed=31,
        )
        return candidate, audit

    original_candidate, original_audit = fitted(source)
    duplicated_candidate, duplicated_audit = fitted(duplicated_source)

    assert duplicated_audit["row_count"] > original_audit["row_count"]
    assert duplicated_audit["receipt_sha256"] != original_audit["receipt_sha256"]
    assert (
        duplicated_candidate.fit_audit["selected_feature_names"]
        == original_candidate.fit_audit["selected_feature_names"]
    )
    assert (
        duplicated_candidate.policy_payload["decision_policy"]
        == original_candidate.policy_payload["decision_policy"]
    )


def test_one_shot_evaluator_cannot_masquerade_as_sequential() -> None:
    panel, days, pair_features, future_only = _synthetic_panel()
    ladder = _ladder(pair_features, future_only)
    continuous = nested.ContinuousComparatorEntry(
        feature_names_by_side={"SELL": ("continuous::mid_trend",)},
        profiles=(ladder[4].profiles[0],),
    )

    with pytest.raises(
        nested.NestedOofExecutionError,
        match="one-shot effects cannot be aggregated",
    ):
        nested.run_nested_chronological_oof(
            panel,
            fold_manifest=_fold_manifest(days),
            ladder=ladder,
            continuous=continuous,
            evaluator=_SyntheticSequentialEvaluator(panel, one_shot=True),
            config=nested.NestedOofConfig(sides=("SELL",), simultaneous_draws=19),
        )


def test_sequential_claim_requires_hash_bound_executed_policy() -> None:
    panel, days, pair_features, future_only = _synthetic_panel()
    ladder = _ladder(pair_features, future_only)
    continuous = nested.ContinuousComparatorEntry(
        feature_names_by_side={"SELL": ("continuous::mid_trend",)},
        profiles=(ladder[4].profiles[0],),
    )

    class DriftedPolicyEvaluator(_SyntheticSequentialEvaluator):
        def __call__(self, request: nested.EvaluationRequest) -> pd.DataFrame:
            rows = super().__call__(request)
            rows["candidate_executed_policy_sha256"] = "0" * 64
            return rows

    with pytest.raises(
        nested.NestedOofExecutionError,
        match="candidate executed-policy identity drifted",
    ):
        nested.run_nested_chronological_oof(
            panel,
            fold_manifest=_fold_manifest(days),
            ladder=ladder,
            continuous=continuous,
            evaluator=DriftedPolicyEvaluator(panel),
            config=nested.NestedOofConfig(sides=("SELL",), simultaneous_draws=19),
        )


def test_reports_stability_activity_duration_and_top_day_sensitivity(completed_run) -> None:
    result, _, _, _, _, _, _, _ = completed_run
    report = result.candidate_reports["SELL:E3_HIGHER_ORDER_BOOLEAN"]
    stability = result.stability["SELL:E3_HIGHER_ORDER_BOOLEAN"]

    assert report["outer_test_days"] == 20
    assert report["feature_ready_active_days"] > 0
    assert report["policy_assignment_count"] > 0
    assert report["action_mix"]
    assert report["duration_counts"]
    assert report["role_counts"]
    assert report["consecutive_units_counts"]
    assert report["fallback_counts"]
    assert report["fill_retention"] == 1.0
    assert report["daily_positive_rate"] >= 0.0
    assert report["unsupported_mass"] == 0.0
    assert report["paired_effective_sample_size"] == report["common_campaign_count"]
    assert report["risk_and_accounting"]
    assert "removed_days" in report["leave_one_top_day"]
    assert len(report["leave_two_top_days"]["removed_days"]) == 2
    assert stability["learning_algorithm_oof_only"] is True
    assert stability["exact_final_artifact_oof_available"] is False
    assert "pair_inclusion_frequency" in stability
    assert "adjacent_fold_predicate_jaccard" in stability


def test_hierarchy_includes_continuous_minus_boolean_as_fifth_step(completed_run) -> None:
    result, _, _, _, _, _, _, _ = completed_run
    steps = result.hierarchy["steps"]["SELL"]

    assert [step["hypothesis"].rsplit(":", 1)[-1] for step in steps] == [
        "E1-B0",
        "E2-E1",
        "E3-E2",
        "M2-E3",
        "CONTINUOUS-BOOLEAN",
    ]
    assert result.hierarchy[
        "continuous_minus_boolean_is_a_dominance_block_not_a_positive_value_step"
    ] is True
    assert "successor:SELL:CONTINUOUS-BOOLEAN" in result.hierarchy_bands.bands
    assert "successor:SELL:CONTINUOUS-BOOLEAN" in result.hierarchy_week_bands.bands
    assert "successor:SELL:E1-B0" in result.confirmatory_bands.bands
    assert (
        "successor:SELL:M2_TRUE_INCREMENTAL-ACTION_MATCHED"
        in result.confirmatory_week_bands.bands
    )
    assert (
        "successor:SELL:CONTINUOUS-E1_FULL_EMA_BANK"
        in result.confirmatory_bands.bands
    )
    assert "week_simultaneous_lcb_usdc" in steps[0]


def test_unidentified_day_is_not_dropped_from_hierarchy_denominator() -> None:
    rows = pd.DataFrame(
        {
            "side": ["SELL"] * 6,
            "candidate_name": ["candidate"] * 3 + ["reference"] * 3,
            "utc_day": ["2026-08-13", "2026-08-14", "2026-08-15"] * 2,
            "point_identified": [True, True, False, True, True, True],
            "delta_usdc": [0.2, 0.1, np.nan, 0.0, 0.0, 0.0],
        }
    )

    contrast, identified, total = nested._paired_contrast(  # noqa: SLF001
        rows,
        side="SELL",
        candidate="candidate",
        reference="reference",
    )

    assert tuple(contrast.index) == ("2026-08-13", "2026-08-14")
    assert identified == 2
    assert total == 3


def test_ladder_must_be_complete_and_e1_must_offer_all_45_pairs() -> None:
    panel, days, pair_features, future_only = _synthetic_panel()
    ladder = _ladder(pair_features, future_only)
    continuous = nested.ContinuousComparatorEntry(
        feature_names_by_side={"SELL": ("continuous::mid_trend",)},
        profiles=(ladder[4].profiles[0],),
    )
    config = nested.NestedOofConfig(sides=("SELL",), simultaneous_draws=19)

    with pytest.raises(nested.NestedOofExecutionError, match="candidate ladder"):
        nested.run_nested_chronological_oof(
            panel,
            fold_manifest=_fold_manifest(days),
            ladder=ladder[:-1],
            continuous=continuous,
            evaluator=_SyntheticSequentialEvaluator(panel),
            config=config,
        )

    broken = list(ladder)
    broken[4] = replace(
        broken[4], feature_names_by_side={"SELL": pair_features[:-1]}
    )
    with pytest.raises(nested.NestedOofExecutionError, match="all 45 EMA pairs"):
        nested.run_nested_chronological_oof(
            panel,
            fold_manifest=_fold_manifest(days),
            ladder=tuple(broken),
            continuous=continuous,
            evaluator=_SyntheticSequentialEvaluator(panel),
            config=config,
        )

    missing_convergence = tuple(
        name
        for name in ladder[5].feature_names_by_side["SELL"]
        if not (
            pair_features[0].rsplit(":", 1)[0] in name
            and name.endswith(":converging")
        )
    )
    broken[4] = ladder[4]
    broken[5] = replace(
        ladder[5], feature_names_by_side={"SELL": missing_convergence}
    )
    with pytest.raises(nested.NestedOofExecutionError, match="per-pair semantics"):
        nested.run_nested_chronological_oof(
            panel,
            fold_manifest=_fold_manifest(days),
            ladder=tuple(broken),
            continuous=continuous,
            evaluator=_SyntheticSequentialEvaluator(panel),
            config=config,
        )
