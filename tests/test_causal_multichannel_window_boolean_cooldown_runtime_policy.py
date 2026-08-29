from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
    CHANNELS_BY_BLOCK,
    CausalMultichannelEmaState,
    CausalWindowObservation,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_predicates import (
    ARTIFACT_SCHEMA,
    PredicateArtifact,
    PredicateDefinition,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_predicates import (
    IDENTITY as PREDICATE_IDENTITY,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_runtime_policy import (
    OWNER_POLICY_IDENTITY,
    OWNER_POLICY_SCHEMA,
    PREDICATE_BUNDLE_SCHEMA,
    CooldownRuntimePolicyEvaluator,
    RuntimeCooldownPolicyEvaluator,
    load_runtime_policy_evaluator,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    PROSPECTIVE_RECEIVE_TIME_PROFILE,
    CooldownAssignmentSnapshotV2,
    FrozenRow,
    capture_cooldown_assignment_snapshot,
)
from strategy.boolean_cooldown_live import LiveBooleanCooldownPolicy

BASE_NS = 1_800_000_000_000_000_000


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_canonical(path: Path, body: dict[str, Any]) -> dict[str, Any]:
    payload = {**body, "canonical_sha256": _canonical_sha256(body)}
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="ascii",
    )
    return payload


def _artifact(*, side: str, group: str) -> PredicateArtifact:
    source = (
        "value::mid_usdc_per_btc__h4s__h16s::signed_distance"
        if group == "book"
        else "value::signed_flow_imbalance__h4s__h16s::signed_distance"
    )
    name = f"tri::quantile::{source}::ge::q5000"
    return PredicateArtifact(
        schema_version=ARTIFACT_SCHEMA,
        identity=PREDICATE_IDENTITY,
        side=side,
        source_role="outcome_blind_2025_single_channel",
        reference_identity_sha256=("a" if group == "book" else "b") * 64,
        reference_days=("2025-08-01",),
        source_clock_identity={"shared": f"2025-{group}-clock"},
        clock_separated_2025=True,
        quantiles=(0.5,),
        input_schema=tuple(
            sorted(
                (
                    ("side", "text"),
                    ("utc_day", "text"),
                    (source, "numeric"),
                )
            )
        ),
        definitions=(
            PredicateDefinition(
                name=name,
                source_field=source,
                block="M2",
                kind="quantile_ge",
                clock_group=group,
                threshold=0.0,
                quantile=0.5,
            ),
        ),
    )


def _write_runtime_artifacts(
    root: Path,
    *,
    rules: list[dict[str, Any]],
    declared_predicates: list[str] | None = None,
) -> tuple[Path, Path, str, str]:
    artifact_dir = root / "artifacts"
    artifact_dir.mkdir(parents=True)
    artifact_bindings: dict[str, dict[str, str]] = {}
    bundle_groups: dict[str, dict[str, dict[str, str]]] = {"book": {}, "trade": {}}
    for group in ("book", "trade"):
        for side in ("BUY", "SELL"):
            artifact = _artifact(side=side, group=group)
            relative = Path("artifacts") / f"{group}_{side.lower()}.json"
            path = root / relative
            path.write_text(artifact.to_json() + "\n", encoding="ascii")
            file_sha = _file_sha256(path)
            bundle_groups[group][side] = {
                "path": str(relative),
                "sha256": file_sha,
            }
            artifact_bindings[f"{group}.{side}"] = {
                "path": str(path),
                "file_sha256": file_sha,
                "canonical_sha256": artifact.canonical_sha256,
                "reference_identity_sha256": artifact.reference_identity_sha256,
            }
    bundle_body = {
        "schema_version": PREDICATE_BUNDLE_SCHEMA,
        "identity": PREDICATE_IDENTITY,
        "book": bundle_groups["book"],
        "trade": bundle_groups["trade"],
        "m0_artifacts": [],
        "cross_clock_clause_authorized": False,
        "cross_clock_clause_scope": "2025_reference_rows_only",
        "strict_2026_target_snapshot": {
            "authority_owner": "2026_strict_denominator_study",
            "book_trade_predicates_may_be_combined_by_study": True,
            "required_condition": (
                "book and trade predicates are evaluated on the same admitted strict "
                "target snapshot and causal feature-ready cutoff"
            ),
        },
    }
    bundle_path = root / "predicate_bundle.json"
    bundle = _write_canonical(bundle_path, bundle_body)
    bundle_sha = _file_sha256(bundle_path)
    boolean_policy: dict[str, Any] = {
        "identity": (
            "causal_multichannel_window_boolean_cooldown_duration_v2."
            "nested_chronological_boolean_oof.v1"
        ),
        "side": "SELL",
        "ordered_first_match_rules": rules,
        "default_action": "CONTROL_85N",
        "permissions": {
            "owner_full_path_candidate": True,
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    if declared_predicates is not None:
        boolean_policy["predicate_columns"] = declared_predicates
    policy_body = {
        "schema_version": OWNER_POLICY_SCHEMA,
        "identity": OWNER_POLICY_IDENTITY,
        "evidence_route": "owner_risk_accepted_outcome_informed_successor",
        "selection": {
            "BUY": "CONTROL_85N",
            "SELL": "fixture_boolean_policy",
        },
        "policy": boolean_policy,
        "fit_audit": {},
        "predecessor_evidence": {},
        "bindings": {
            "panel": {
                "outcome_blind_2025_predicates": {
                    "bundle": {
                        "path": str(bundle_path),
                        "file_sha256": bundle_sha,
                        "canonical_sha256": bundle["canonical_sha256"],
                    },
                    "artifacts": artifact_bindings,
                }
            }
        },
        "permissions": {
            "research_supported": False,
            "repeated_policy_run": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    policy_path = root / "policy.json"
    _write_canonical(policy_path, policy_body)
    return policy_path, bundle_path, _file_sha256(policy_path), bundle_sha


def _literal(predicate: str, *, negated: bool = False) -> dict[str, Any]:
    return {"predicate": predicate, "negated": negated}


def _rule(action: str, *clauses: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    normalized = [
        {"literals": sorted(clause, key=lambda item: (item["predicate"], item["negated"]))}
        for clause in clauses
    ]
    normalized.sort(
        key=lambda clause: tuple(
            (item["predicate"], item["negated"]) for item in clause["literals"]
        )
    )
    return {"action": action, "clauses": normalized}


def _m0(*, side: str, decision_ns: int, campaign_age_s: float) -> dict[str, Any]:
    before = 0.0
    after = 0.001 if side == "BUY" else -0.001
    return {
        "assignment_ts_ns": decision_ns,
        "fill_visible_ts_ns": decision_ns,
        "side": side,
        "role_at_fill": "opener",
        "inventory_before_fill_btc": before,
        "inventory_after_fill_btc": after,
        "fill_qty_btc": 0.001,
        "order_qty_btc": 0.001,
        "cumulative_filled_qty_before_btc": 0.0,
        "cumulative_filled_qty_after_btc": 0.001,
        "remaining_order_qty_after_btc": 0.0,
        "partial_fill_ordinal": 1,
        "fill_is_partial": False,
        "order_age_s": 1.0,
        "queue_ahead_before_fill_btc": 0.0,
        "queue_state_before_fill": "known_zero",
        "target_price_tick": 640_000,
        "target_price_displayed_qty_btc": 0.0,
        "target_price_displayed_qty_status": "known_zero",
        "target_price_displayed_qty_known": True,
        "target_price_displayed_qty_is_queue_ahead": False,
        "consecutive_units_after": 1.0,
        "baseline_duration_ms": 85_000.0,
        "campaign_age_s": campaign_age_s,
        "campaign_add_count": 0,
        "campaign_mae_to_date_usdc": 0.0,
        "campaign_inventory_time_to_date_btc_s": 0.0,
        "last_same_side_fill_age_s": None,
        "last_opposite_side_fill_age_s": None,
        "cooldown_remaining_ms": 0.0,
        "cooldown_blocker_active": False,
        "cooldown_lineage_revision_before": 0,
        "cooldown_deadline_owner": "none",
    }


def _status() -> dict[str, Any]:
    return {"valid": True, "unknown": False, "reason": "valid"}


def _source(generation: int, cursor: str) -> dict[str, Any]:
    return {
        "generation": generation,
        "cursor": cursor,
        "feature_generation": generation,
        "feature_cursor": cursor,
        **_status(),
    }


def _snapshot(
    *,
    side: str = "SELL",
    campaign_age_s: float = 100.0,
    cross_after_initialization: bool = True,
) -> CooldownAssignmentSnapshotV2:
    state = CausalMultichannelEmaState(
        block="M2",
        warmup_admitted=True,
        warmup_identity="d-minus-1-fixture",
    )
    for index, level in enumerate((100.0, 102.0, 99.0), start=1):
        right = BASE_NS + index * BASE_WINDOW_WIDTH_NS
        values = {
            channel.name: level + channel_index / 100.0
            for channel_index, channel in enumerate(CHANNELS_BY_BLOCK["M2"])
        }
        if not cross_after_initialization:
            values = {
                channel.name: 100.0
                for channel in CHANNELS_BY_BLOCK["M2"]
            }
        state.update(
            CausalWindowObservation(
                left_ts_ns=right - BASE_WINDOW_WIDTH_NS,
                right_ts_ns=right,
                feature_ready_ts_ns=right,
                market_generation=index,
                depth_generation=index,
                values=values,
                warmup_admitted=True,
            )
        )
    decision_ns = BASE_NS + 3 * BASE_WINDOW_WIDTH_NS
    m0 = _m0(side=side, decision_ns=decision_ns, campaign_age_s=campaign_age_s)
    feature_row = state.feature_row(
        side=side,
        decision_ts_ns=decision_ns,
        m0_context=m0,
    )
    payload = {
        "snapshot_id": f"snapshot-{side.lower()}",
        "assignment_id": "assignment-1",
        "fill_event_id": "fill-1",
        "client_order_id": "order-1",
        "lineage_id": f"{side.lower()}-lineage",
        "lineage_revision": 1,
        "partial_fill_ordinal": 1,
        "partial_fill_qty_btc": 0.001,
        "visibility_profile": PROSPECTIVE_RECEIVE_TIME_PROFILE,
        "clocks": {
            "assignment": {"ts_ns": decision_ns, **_status()},
            "fill_exchange": {"ts_ns": decision_ns - 2_000_000, **_status()},
            "fill_receive": {"ts_ns": decision_ns - 1_000_000, **_status()},
            "fill_visible": {"ts_ns": decision_ns, **_status()},
            "feature_ready": {
                "ts_ns": feature_row["feature_ready_ts_ns"],
                **_status(),
            },
        },
        "sources": {
            "market": _source(3, "market-3"),
            "depth": _source(3, "depth-3"),
            "trade": _source(3, "trade-3"),
        },
        "identity_hashes": {
            "config_sha256": "a" * 64,
            "code_sha256": "b" * 64,
            "model_sha256": "c" * 64,
            "p3_sha256": "d" * 64,
            "feature_dag_sha256": "e" * 64,
            "execution_abi_sha256": "f" * 64,
            "baseline_identity_sha256": "1" * 64,
        },
        "m0_context": m0,
        "feature_row": feature_row,
    }
    return capture_cooldown_assignment_snapshot(payload)


def test_buy_is_always_control_and_empty_declared_columns_are_derived(
    tmp_path: Path,
) -> None:
    predicate = "predicate::m0::campaign_age_gt_control_duration"
    policy, bundle, policy_sha, bundle_sha = _write_runtime_artifacts(
        tmp_path,
        rules=[_rule("FIXED_166S", (_literal(predicate),))],
        declared_predicates=[],
    )
    evaluator = load_runtime_policy_evaluator(
        policy,
        expected_policy_sha256=policy_sha,
    )

    decision = evaluator.evaluate(_snapshot(side="BUY"), 85_000)

    assert decision.action_id == "CONTROL_85N"
    assert decision.duration_ms == 85_000
    assert decision.fallback_reason == "buy_control_by_contract"
    assert decision.support_valid is True
    assert decision.policy_sha256 == policy_sha
    assert decision.predicate_bundle_sha256 == bundle_sha
    assert evaluator.audit() == {
        "evaluations": 1,
        "supported": 1,
        "fallback": 1,
        "nonbaseline": 0,
        "action_counts": {"CONTROL_85N": 1},
        "duration_ms_sum": 85_000,
        "duration_ms_max": 85_000,
    }


def test_sell_rule_uses_boolean_policy_and_maps_fixed_seconds(tmp_path: Path) -> None:
    predicate = "predicate::m0::campaign_age_gt_control_duration"
    policy, bundle, _, _ = _write_runtime_artifacts(
        tmp_path,
        rules=[_rule("FIXED_166S", (_literal(predicate),))],
    )
    evaluator = CooldownRuntimePolicyEvaluator.from_files(
        policy_path=policy,
        predicate_bundle_path=bundle,
    )

    decision = evaluator.evaluate(
        _snapshot(side="SELL", campaign_age_s=100.0),
        85_000,
    )

    assert decision.action_id == "FIXED_166S"
    assert decision.duration_ms == 166_000
    assert decision.matched_rule_index == 0
    assert decision.fallback_reason is None
    assert decision.support_valid is True
    assert evaluator.audit() == {
        "evaluations": 1,
        "supported": 1,
        "fallback": 0,
        "nonbaseline": 1,
        "action_counts": {"FIXED_166S": 1},
        "duration_ms_sum": 166_000,
        "duration_ms_max": 166_000,
    }


def test_stable_loader_returns_requested_public_evaluator_type(tmp_path: Path) -> None:
    predicate = "predicate::m0::campaign_age_gt_control_duration"
    policy, _, policy_sha, _ = _write_runtime_artifacts(
        tmp_path,
        rules=[_rule("FIXED_166S", (_literal(predicate),))],
    )

    evaluator = load_runtime_policy_evaluator(
        policy,
        expected_policy_sha256=policy_sha,
    )

    assert isinstance(evaluator, RuntimeCooldownPolicyEvaluator)


def test_artifact_quantile_literal_is_transformed_on_snapshot_row(
    tmp_path: Path,
) -> None:
    source = "value::mid_usdc_per_btc__h4s__h16s::signed_distance"
    predicate = f"tri::quantile::{source}::ge::q5000"
    policy, bundle, _, _ = _write_runtime_artifacts(
        tmp_path,
        rules=[_rule("FIXED_211S", (_literal(predicate),))],
    )
    evaluator = CooldownRuntimePolicyEvaluator.from_files(
        policy_path=policy,
        predicate_bundle_path=bundle,
    )

    decision = evaluator.evaluate(_snapshot(side="SELL"), 85_000)

    assert decision.action_id in {"FIXED_211S", "CONTROL_85N"}
    assert decision.fallback_reason in {None, "no_rule_matched"}
    assert decision.support_valid is True


def test_unobserved_first_rule_blocks_later_rule(tmp_path: Path) -> None:
    cross = "predicate::ema_pair_h4s_h16s:cross_age_le_slow"
    campaign = "predicate::m0::campaign_age_gt_control_duration"
    policy, bundle, _, _ = _write_runtime_artifacts(
        tmp_path,
        rules=[
            _rule("FIXED_1748S", (_literal(cross),)),
            _rule("FIXED_166S", (_literal(campaign),)),
        ],
    )
    evaluator = CooldownRuntimePolicyEvaluator.from_files(
        policy_path=policy,
        predicate_bundle_path=bundle,
    )

    decision = evaluator.evaluate(
        _snapshot(
            side="SELL",
            campaign_age_s=100.0,
            cross_after_initialization=False,
        ),
        85_000,
    )

    assert decision.action_id == "CONTROL_85N"
    assert decision.duration_ms == 85_000
    assert decision.fallback_reason == "rule_unobserved:0"
    assert decision.matched_rule_index is None
    assert decision.support_valid is False


def test_stale_snapshot_and_missing_column_fail_closed(tmp_path: Path) -> None:
    predicate = "predicate::m0::campaign_age_gt_control_duration"
    policy, bundle, _, _ = _write_runtime_artifacts(
        tmp_path,
        rules=[_rule("FIXED_166S", (_literal(predicate),))],
    )
    evaluator = CooldownRuntimePolicyEvaluator.from_files(
        policy_path=policy,
        predicate_bundle_path=bundle,
    )
    snapshot = _snapshot(side="SELL")
    stale = replace(
        snapshot,
        policy_input_valid=False,
        policy_input=None,
        fallback_policy_id="CONTROL_85N",
        fallback_reason="feature_stream_stale",
    )
    stale_decision = evaluator.evaluate(stale, 85_000)
    assert stale_decision.action_id == "CONTROL_85N"
    assert stale_decision.fallback_reason == "snapshot_invalid:feature_stream_stale"
    assert stale_decision.support_valid is False

    feature = snapshot.feature_row.to_dict()
    feature.pop("campaign_age_s")
    assert snapshot.policy_input is not None
    missing_policy_input = replace(
        snapshot.policy_input,
        feature_row=FrozenRow.from_mapping(feature),
    )
    missing = replace(
        snapshot,
        feature_row=FrozenRow.from_mapping(feature),
        policy_input=missing_policy_input,
    )
    missing_decision = evaluator.evaluate(missing, 85_000)
    assert missing_decision.action_id == "CONTROL_85N"
    assert missing_decision.fallback_reason == "missing_feature_column:campaign_age_s"
    assert missing_decision.support_valid is False


def test_hash_and_side_drift_never_escape_runtime_boundary(tmp_path: Path) -> None:
    predicate = "predicate::m0::campaign_age_gt_control_duration"
    policy, bundle, _, _ = _write_runtime_artifacts(
        tmp_path,
        rules=[_rule("FIXED_166S", (_literal(predicate),))],
    )
    evaluator = CooldownRuntimePolicyEvaluator.from_files(
        policy_path=policy,
        predicate_bundle_path=bundle,
        expected_policy_sha256="0" * 64,
    )
    hash_decision = evaluator.evaluate(_snapshot(side="SELL"), 85_000)
    assert hash_decision.action_id == "CONTROL_85N"
    assert hash_decision.fallback_reason == (
        "runtime_binding_invalid:policy_file_sha256_mismatch"
    )
    assert hash_decision.support_valid is False

    valid = CooldownRuntimePolicyEvaluator.from_files(
        policy_path=policy,
        predicate_bundle_path=bundle,
    )
    snapshot = _snapshot(side="SELL")
    feature = snapshot.feature_row.to_dict()
    feature["side"] = "BUY"
    policy_input = replace(
        snapshot.policy_input,
        feature_row=FrozenRow.from_mapping(feature),
    )
    inconsistent = replace(
        snapshot,
        feature_row=FrozenRow.from_mapping(feature),
        policy_input=policy_input,
    )
    side_decision = valid.evaluate(inconsistent, 85_000)
    assert side_decision.action_id == "CONTROL_85N"
    assert side_decision.fallback_reason == "snapshot_side_inconsistent"
    assert side_decision.support_valid is False


def test_exact_predicate_entry_matches_snapshot_policy_choice(tmp_path: Path) -> None:
    predicate = "predicate::m0::campaign_age_gt_control_duration"
    policy, bundle, _, _ = _write_runtime_artifacts(
        tmp_path,
        rules=[_rule("FIXED_166S", (_literal(predicate),))],
    )
    snapshot_evaluator = CooldownRuntimePolicyEvaluator.from_files(
        policy_path=policy,
        predicate_bundle_path=bundle,
    )
    direct_evaluator = CooldownRuntimePolicyEvaluator.from_files(
        policy_path=policy,
        predicate_bundle_path=bundle,
    )

    snapshot_decision = snapshot_evaluator.evaluate(
        _snapshot(side="SELL", campaign_age_s=100.0),
        85_000,
    )
    direct_decision = direct_evaluator.evaluate_predicates(
        side="SELL",
        predicate_values={predicate: 1},
        baseline_duration_ms=85_000,
        snapshot_id="direct-row",
    )

    assert direct_decision.action_id == snapshot_decision.action_id
    assert direct_decision.duration_ms == snapshot_decision.duration_ms
    assert direct_decision.matched_rule_index == snapshot_decision.matched_rule_index
    assert direct_decision.support_valid is True


def test_live_receive_time_projection_executes_frozen_sell_rule(
    tmp_path: Path,
) -> None:
    cross_4_16 = "predicate::ema_pair_h4s_h16s:cross_age_le_slow"
    cross_16_256 = "predicate::ema_pair_h16s_h256s:cross_age_le_fast"
    campaign = "predicate::m0::campaign_age_gt_control_duration"
    policy, bundle, policy_sha, bundle_sha = _write_runtime_artifacts(
        tmp_path,
        rules=[
            _rule(
                "FIXED_1748S",
                (_literal(cross_4_16), _literal(campaign)),
                (_literal(cross_4_16, negated=True), _literal(campaign)),
            ),
            _rule(
                "FIXED_166S",
                (_literal(cross_16_256), _literal(campaign, negated=True)),
            ),
            _rule(
                "FIXED_211S",
                (
                    _literal(cross_16_256, negated=True),
                    _literal(campaign, negated=True),
                ),
            ),
        ],
    )
    runtime = LiveBooleanCooldownPolicy.from_files(
        policy_path=policy,
        policy_sha256=policy_sha,
        predicate_bundle_path=bundle,
        predicate_bundle_sha256=bundle_sha,
        warmup_s=0.1,
        max_feature_age_s=2.0,
    )

    for index, mid in enumerate((100.0, 102.0, 98.0, 99.0)):
        runtime.observe_depth(
            receive_ts_ns=BASE_NS + index * BASE_WINDOW_WIDTH_NS,
            bids=((mid - 0.05, 1.0),),
            asks=((mid + 0.05, 1.0),),
            market_generation=index + 1,
            depth_generation=index + 1,
        )

    decision = runtime.evaluate(
        side="SELL",
        baseline_duration_ms=85_000,
        campaign_age_s=10.0,
        decision_ts_ns=BASE_NS + 3 * BASE_WINDOW_WIDTH_NS,
        snapshot_id="live-fill-fixture",
    )

    assert decision.action_id == "FIXED_166S"
    assert decision.duration_ms == 166_000
    assert decision.support_valid is True
    assert decision.fallback_reason is None
    audit = runtime.audit()
    assert audit["evaluations"] == 1
    assert audit["nonbaseline"] == 1
    assert audit["windows"]["warmup_admitted"] == 1


def test_live_receive_time_projection_falls_back_during_cold_start(
    tmp_path: Path,
) -> None:
    predicates = (
        "predicate::ema_pair_h16s_h256s:cross_age_le_fast",
        "predicate::ema_pair_h4s_h16s:cross_age_le_slow",
        "predicate::m0::campaign_age_gt_control_duration",
    )
    policy, bundle, policy_sha, bundle_sha = _write_runtime_artifacts(
        tmp_path,
        rules=[
            _rule(
                "FIXED_166S",
                tuple(_literal(predicate) for predicate in predicates),
            )
        ],
    )
    runtime = LiveBooleanCooldownPolicy.from_files(
        policy_path=policy,
        policy_sha256=policy_sha,
        predicate_bundle_path=bundle,
        predicate_bundle_sha256=bundle_sha,
        warmup_s=100.0,
        max_feature_age_s=2.0,
    )
    for index, mid in enumerate((100.0, 101.0)):
        runtime.observe_depth(
            receive_ts_ns=BASE_NS + index * BASE_WINDOW_WIDTH_NS,
            bids=((mid - 0.05, 1.0),),
            asks=((mid + 0.05, 1.0),),
            market_generation=index + 1,
            depth_generation=index + 1,
        )

    decision = runtime.evaluate(
        side="SELL",
        baseline_duration_ms=85_000,
        campaign_age_s=10.0,
        decision_ts_ns=BASE_NS + BASE_WINDOW_WIDTH_NS,
        snapshot_id="cold-start",
    )

    assert decision.action_id == "CONTROL_85N"
    assert decision.duration_ms == 85_000
    assert decision.support_valid is False
    assert decision.fallback_reason == "receive_time_ema_warmup_incomplete"
