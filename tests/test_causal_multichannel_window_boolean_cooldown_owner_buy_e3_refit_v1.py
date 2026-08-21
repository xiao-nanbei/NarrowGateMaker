from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_repeated_policy_backend_v1 as backend,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_replay_adapter_v1 as replay_adapter,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 as successor,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_refit_v1 as subject,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    TriLiteral,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_predicates import (
    PredicateDefinition,
)


def _panel() -> nested.NestedOofPanel:
    days = tuple((date(2026, 7, 19) + timedelta(days=offset)).isoformat() for offset in range(30))
    index = pd.Index([f"BUY:{day}" for day in days], name="opportunity_id")
    metadata = pd.DataFrame(
        {"utc_day": days, "side": "BUY"},
        index=index,
    )
    action_outcomes = pd.DataFrame(
        0.0,
        index=index,
        columns=subject.EXPECTED_ACTIONS,
    )
    return nested.NestedOofPanel(
        metadata=metadata,
        boolean_features=pd.DataFrame(index=index),
        continuous_features=pd.DataFrame(index=index),
        exact_owner_actions=pd.Series("CONTROL_85N", index=index),
        action_outcomes=action_outcomes,
        action_supported=pd.DataFrame(True, index=index, columns=action_outcomes.columns),
        learning_label_request_sha256="1" * 64,
        learning_label_payload_sha256="2" * 64,
        learning_label_receipt_sha256="3" * 64,
    )


def _policy() -> BooleanCooldownPolicy:
    mid = "tri::mid_usdc_per_btc__h1s__h2s::positive_ordering"
    return BooleanCooldownPolicy(
        side="BUY",
        rules=(
            BooleanRule(
                action="FIXED_2048S",
                clauses=(
                    AndClause(
                        tuple(
                            sorted(
                                (
                                    TriLiteral(mid),
                                    TriLiteral(successor.CURRENT_CAMPAIGN_AGE),
                                )
                            )
                        )
                    ),
                ),
            ),
        ),
    )


def _source_bundle():
    definition = PredicateDefinition(
        name="tri::mid_usdc_per_btc__h1s__h2s::positive_ordering",
        kind="preserved_tri",
        source_field="tri::mid_usdc_per_btc__h1s__h2s::positive_ordering",
        block="M1",
        clock_group="book",
    )

    class Bundle:
        artifacts = {
            "book.BUY": SimpleNamespace(definitions=(definition,)),
            "trade.BUY": SimpleNamespace(definitions=()),
        }

        @staticmethod
        def receipt():
            return {
                "bundle_file_sha256": "4" * 64,
                "bundle_canonical_sha256": "5" * 64,
                "artifact_file_sha256": {"book.BUY": "6" * 64},
                "artifact_canonical_sha256": {"book.BUY": "7" * 64},
                "reference_days_are_2025": True,
            }

    return Bundle()


def _ladder() -> tuple[nested.CandidateLadderEntry, ...]:
    profile = successor.SuccessorSearchProfile(
        name=subject.OWNER_PROFILE,
        feature_budget=1024,
        max_depth=6,
        max_leaf_nodes=32,
        min_samples_leaf=30,
        max_rules=7,
        max_clauses_per_rule=16,
        max_literals_per_clause=6,
    )
    return (
        nested.CandidateLadderEntry(
            subject.OWNER_CANDIDATE,
            "boolean",
            feature_names_by_side={"BUY": tuple(_policy().predicate_columns)},
            profiles=(profile,),
        ),
    )


def _execution_manifest() -> dict:
    return {
        "execution_contract": subject.execution_contract(),
        "canonical_execution_manifest_sha256": "8" * 64,
        "public_base_commit": "9" * 40,
        "annotated_tag": "f05-owner-buy-e3-test",
        "bindings": {
            "source_manifest": {"sha256": "d" * 64},
            "panel_manifest": {"sha256": "e" * 64},
        },
    }


def test_execution_contract_forbids_fold_selection_and_candidate_substitution() -> None:
    contract = subject.execution_contract()
    assert contract["selected_side"] == "BUY"
    assert contract["selected_candidate"] == "E3_HIGHER_ORDER_BOOLEAN"
    assert contract["selected_profile"] == "e3_high_order_multirule_dnf_v1"
    assert contract["duration_vocabulary"] == list(subject.EXPECTED_ACTIONS)
    assert contract["full_development_refit_count"] == 1
    assert contract["outer_fold_policy_selection_allowed"] is False
    assert contract["outer_fold_rule_merge_allowed"] is False
    assert contract["literal_edit_allowed"] is False
    assert contract["candidate_substitution_allowed"] is False


def test_owner_refit_emits_unique_non_self_confirming_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    panel = _panel()
    policy = _policy()
    days = tuple(sorted(panel.metadata["utc_day"].astype(str).unique()))
    fitted = nested.FittedCandidate(
        ladder_name=subject.OWNER_CANDIDATE,
        side="BUY",
        policy=policy,
        selected_profile=subject.OWNER_PROFILE,
        training_days=days,
        training_row_sha256="a" * 64,
        policy_payload={"decision_policy": policy.payload()},
        policy_sha256=policy.candidate_id,
        fit_audit={"selected_feature_names": list(policy.predicate_columns)},
        feature_pool_audit={"scope": "all_development"},
    )
    calls = []

    def fake_fit(*args, **kwargs):
        calls.append(kwargs)
        return fitted

    monkeypatch.setattr(nested, "_fit_boolean_candidate", fake_fit)
    bundle = subject.fit_owner_buy_e3(
        panel,
        ladder=_ladder(),
        source_predicate_bundle=_source_bundle(),
        execution_manifest=_execution_manifest(),
        label_materialization_receipt_sha256="b" * 64,
        cpp_qualification_receipt_sha256="c" * 64,
        execution_preflight_receipt_sha256="f" * 64,
    )

    assert len(calls) == 1
    assert calls[0]["fold_id"] == subject.OWNER_FOLD_ID
    assert calls[0]["random_seed"] == subject.OWNER_SEED
    assert len(calls[0]["train_index"]) == len(panel.metadata)
    assert bundle.policy_artifact["evidence_boundary"]["research_supported"] is False
    assert bundle.policy_artifact["evidence_boundary"]["owner_risk_accepted"] is True
    assert bundle.policy_artifact["evidence_boundary"]["exact_artifact_oof_available"] is False
    assert bundle.artifact_manifest["exact_final_artifact_oof_available"] is False
    assert bundle.artifact_manifest["duration_vocabulary"] == list(subject.EXPECTED_ACTIONS)
    assert bundle.selected_predicate_bundle["uses_m2_incremental_features"] is False
    assert bundle.selected_predicate_bundle["uses_trade_predicates"] is False


def test_final_receipt_requires_all_four_parity_layers(tmp_path: Path) -> None:
    paths = []
    for name in ("manifest.json", "policy.json", "bundle.json"):
        path = tmp_path / name
        path.write_text("{}\n", encoding="ascii")
        paths.append(path)
    with pytest.raises(subject.OwnerBuyE3RefitError, match="four-layer"):
        subject.build_final_receipt(
            execution_manifest=_execution_manifest(),
            artifact_manifest_path=paths[0],
            policy_path=paths[1],
            predicate_bundle_path=paths[2],
            parity_receipt_paths={"research_compiled": paths[0]},
            sell_54_case_receipt_path=paths[0],
            runtime_regression_receipt_path=paths[0],
            deployment_gate_receipt_path=paths[0],
        )


def test_materializer_audits_24_predecessor_days_and_fresh_computes_six(
    tmp_path: Path,
) -> None:
    source = _panel()
    panel = nested.NestedOofPanel(
        metadata=source.metadata,
        boolean_features=source.boolean_features,
        continuous_features=source.continuous_features,
        exact_owner_actions=source.exact_owner_actions,
    )
    replay_inputs = pd.DataFrame(
        {
            "utc_day": panel.metadata["utc_day"],
            "side": "BUY",
            "stable_input": range(len(panel.metadata)),
        },
        index=panel.metadata.index,
    )
    bindings = backend.FormalExecutionBindings(
        execution_manifest_sha256="1" * 64,
        source_manifest_sha256="2" * 64,
        panel_manifest_sha256="3" * 64,
        fold_manifest_sha256="4" * 64,
        nested_fold_manifest_sha256="5" * 64,
        exact_owner_policy_sha256="6" * 64,
        exact_owner_predicate_bundle_sha256="7" * 64,
        exact_owner_private_config_sha256="8" * 64,
    )
    mechanics = SimpleNamespace(
        panel=panel,
        replay_inputs=replay_inputs,
        bindings=bindings,
    )
    days = tuple(panel.metadata["utc_day"].astype(str))
    cache = replay_adapter.DayReplayCache(tmp_path / "cache")
    scoped = backend._bind_outer_train_replay_scope(
        replay_inputs,
        outer_fold_id=subject.OWNER_FOLD_ID,
    )
    for ordinal, day in enumerate(days[:24]):
        day_rows = scoped.loc[scoped["utc_day"].astype(str) == day]
        semantic_sha = replay_adapter._one_shot_semantic_day_input_sha256(day_rows)
        source_key = replay_adapter.DayReplayCacheKey(
            adapter_artifact_sha256="a" * 64,
            source_manifest_sha256=bindings.source_manifest_sha256,
            panel_manifest_sha256=bindings.panel_manifest_sha256,
            fold_manifest_sha256=bindings.fold_manifest_sha256,
            execution_manifest_sha256="b" * 64,
            exact_owner_policy_sha256=bindings.exact_owner_policy_sha256,
            candidate_policy_sha256="c" * 64,
            side="BUY",
            stage=replay_adapter.ONE_SHOT_STAGE,
            fold_id="outer-source",
            utc_day=day,
            day_input_sha256=f"{ordinal + 1:064x}",
        )
        semantic_key = replay_adapter.OneShotSemanticCacheKey(
            adapter_artifact_sha256=source_key.adapter_artifact_sha256,
            source_manifest_sha256=source_key.source_manifest_sha256,
            panel_manifest_sha256=source_key.panel_manifest_sha256,
            fold_manifest_sha256=source_key.fold_manifest_sha256,
            execution_manifest_sha256=source_key.execution_manifest_sha256,
            exact_owner_policy_sha256=source_key.exact_owner_policy_sha256,
            candidate_policy_sha256=source_key.candidate_policy_sha256,
            side="BUY",
            utc_day=day,
            semantic_day_input_sha256=semantic_sha,
        )
        index = pd.Index(day_rows.index, name="opportunity_id")
        outcomes = pd.DataFrame(0.0, index=index, columns=subject.EXPECTED_ACTIONS)
        supported = pd.DataFrame(True, index=index, columns=subject.EXPECTED_ACTIONS)
        cache.admit_one_shot(
            source_key,
            outcomes,
            supported,
            evidence={"semantic_day_input_sha256": semantic_sha},
        )
        cache.register_one_shot_semantic(source_key, semantic_key)

    class FreshAdapter:
        identity = "current-owner-test-adapter"
        artifact_sha256 = "d" * 64

        def __init__(self) -> None:
            self.calls = []

        def generate_outer_train_one_shot_labels(self, request, rows):
            self.calls.append((request, rows.copy()))
            index = pd.Index(request.label_request.row_ids, name="opportunity_id")
            outcomes = pd.DataFrame(
                0.0,
                index=index,
                columns=subject.EXPECTED_ACTIONS,
            )
            supported = pd.DataFrame(
                True,
                index=index,
                columns=subject.EXPECTED_ACTIONS,
            )
            return backend.CanonicalOneShotReplayResult(
                outcomes=outcomes,
                supported=supported,
                receipt=backend.build_outer_train_label_replay_receipt(
                    request,
                    adapter_identity=self.identity,
                    adapter_artifact_sha256=self.artifact_sha256,
                ),
            )

    current = FreshAdapter()
    materialized = subject.materialize_full_development_buy_labels(
        mechanics,
        predecessor_cache_root=cache.root,
        fresh_adapter=current,
        owner_execution_manifest_sha256="e" * 64,
        predecessor_execution_manifest_sha256="b" * 64,
        predecessor_adapter_artifact_sha256="a" * 64,
        predecessor_candidate_bundle_sha256="c" * 64,
        expected_panel_opportunity_count=30,
    )

    assert len(current.calls) == 1
    assert tuple(current.calls[0][0].label_request.train_days) == days[24:]
    assert materialized.receipt["predecessor_day_count"] == 24
    assert materialized.receipt["fresh_day_count"] == 6
    assert materialized.receipt["strategy_dependent_cross_execution_cache_imported"] is False
    assert len(materialized.outcomes) == 30
    assert len(materialized.supported) == 30
    bound, batch = subject.bind_materialized_full_development_labels(
        panel,
        materialized,
    )
    assert bound.has_preconstructed_labels
    assert batch.provider_identity == subject.LABEL_PROVIDER_IDENTITY
    assert batch.provider_artifact_sha256 == materialized.receipt_sha256
