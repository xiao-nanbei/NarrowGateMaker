from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    IDENTITY,
    FeatureContractError,
    validate_m0_context,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_runtime_policy import (
    OWNER_POLICY_IDENTITY,
    OWNER_POLICY_SCHEMA,
    RuntimeCooldownPolicyEvaluator,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    PROSPECTIVE_RECEIVE_TIME_PROFILE,
    SNAPSHOT_SCHEMA_VERSION,
    CooldownAssignmentSnapshotV2,
    FrozenRow,
    PolicyInputV2,
)
from research.families.f05_fill_quality_quote_ev.audit.f05_streaming_boolean_cooldown_cpp_parity import (
    CppParityContractError,
    ResetUnboundStreamingState,
    RestoreStreamingCheckpoint,
    SaveStreamingCheckpoint,
    StreamingObservationCase,
    audit_owner_policy_json,
    audit_sell_m2_streaming_literal_closure,
    build_stream_wire_protocol,
    build_wire_protocol,
    compare_cpp_stream_with_python,
    compare_cpp_with_runtime,
    compile_cpp_cli,
    parity_case_from_snapshot,
    python_decision,
    reference_decision,
    run_cpp_cli,
    run_cpp_stream_cli,
)

CROSS_SHORT = "predicate::ema_pair_h4s_h16s:cross_age_le_slow"
CROSS_LONG = "predicate::ema_pair_h16s_h256s:cross_age_le_fast"
CAMPAIGN = "predicate::m0::campaign_age_gt_control_duration"
BASE_NS = 1_800_000_000_000_000_000
WINDOW_NS = 100_000_000


@dataclass(frozen=True, slots=True)
class _EmptyPredicateArtifact:
    definitions: tuple[Any, ...] = ()


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


def _write_policy(path: Path) -> str:
    policy = {
        "identity": (
            "causal_multichannel_window_boolean_cooldown_duration_v2."
            "nested_chronological_boolean_oof.v1"
        ),
        "side": "SELL",
        "ordered_first_match_rules": [
            {
                "action": "FIXED_1748S",
                "clauses": [
                    {
                        "literals": [
                            {"predicate": CROSS_SHORT, "negated": False},
                            {"predicate": CAMPAIGN, "negated": False},
                        ]
                    },
                    {
                        "literals": [
                            {"predicate": CROSS_SHORT, "negated": True},
                            {"predicate": CAMPAIGN, "negated": False},
                        ]
                    },
                ],
            },
            {
                "action": "FIXED_166S",
                "clauses": [
                    {
                        "literals": [
                            {"predicate": CROSS_LONG, "negated": False},
                            {"predicate": CAMPAIGN, "negated": True},
                        ]
                    }
                ],
            },
            {
                "action": "FIXED_211S",
                "clauses": [
                    {
                        "literals": [
                            {"predicate": CROSS_LONG, "negated": True},
                            {"predicate": CAMPAIGN, "negated": True},
                        ]
                    }
                ],
            },
        ],
        "default_action": "CONTROL_85N",
        "permissions": {
            "owner_full_path_candidate": True,
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    body = {
        "schema_version": OWNER_POLICY_SCHEMA,
        "identity": OWNER_POLICY_IDENTITY,
        "evidence_route": "owner_risk_accepted_outcome_informed_successor",
        "selection": {
            "BUY": "CONTROL_85N",
            "SELL": "deterministic_cpp_parity_fixture",
        },
        "policy": policy,
        "fit_audit": {},
        "predecessor_evidence": {},
        "bindings": {},
        "permissions": {
            "research_supported": False,
            "repeated_policy_run": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    payload = {**body, "canonical_sha256": _canonical_sha256(body)}
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="ascii",
    )
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _m0(
    *,
    side: str,
    role: str,
    baseline_duration_ms: int,
    campaign_age_s: float,
) -> dict[str, Any]:
    is_opener = role == "opener"
    if side == "SELL":
        before = 0.0 if is_opener else -0.001
        after = before - 0.001
    else:
        before = 0.0 if is_opener else 0.001
        after = before + 0.001
    units = baseline_duration_ms / 85_000.0
    return {
        "assignment_ts_ns": BASE_NS,
        "fill_visible_ts_ns": BASE_NS,
        "side": side,
        "role_at_fill": role,
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
        "consecutive_units_after": units,
        "baseline_duration_ms": float(baseline_duration_ms),
        "campaign_age_s": campaign_age_s,
        "campaign_add_count": 0 if is_opener else 1,
        "campaign_mae_to_date_usdc": 0.0,
        "campaign_inventory_time_to_date_btc_s": 0.0,
        "last_same_side_fill_age_s": None,
        "last_opposite_side_fill_age_s": None,
        "cooldown_remaining_ms": 0.0,
        "cooldown_blocker_active": False,
        "cooldown_lineage_revision_before": 0,
        "cooldown_deadline_owner": "none",
    }


def _snapshot(
    *,
    snapshot_id: str,
    side: str = "SELL",
    role: str = "opener",
    baseline_duration_ms: int = 85_000,
    campaign_age_s: float = 100.0,
    short_cross_age_s: float | None = 5.0,
    long_cross_age_s: float | None = 5.0,
) -> CooldownAssignmentSnapshotV2:
    m0 = _m0(
        side=side,
        role=role,
        baseline_duration_ms=baseline_duration_ms,
        campaign_age_s=campaign_age_s,
    )
    feature_row = {
        **m0,
        "feature_block": "M2",
        "support_valid": True,
        "channel_support_valid": True,
        "decision_ts_ns": BASE_NS,
        "channel::mid_usdc_per_btc::observed": 1,
        "value::mid_usdc_per_btc__h4s__h16s::cross_age_s": short_cross_age_s,
        "value::mid_usdc_per_btc__h16s__h256s::cross_age_s": long_cross_age_s,
    }
    frozen_feature = FrozenRow.from_mapping(feature_row)
    frozen_m0 = FrozenRow.from_mapping(m0)
    policy_input = PolicyInputV2(
        snapshot_id=snapshot_id,
        visibility_profile=PROSPECTIVE_RECEIVE_TIME_PROFILE,
        feature_block="M2",
        source_bundle_sha256="a" * 64,
        identity_hashes=FrozenRow.from_mapping({"fixture": "b" * 64}),
        m0_context=frozen_m0,
        feature_row=frozen_feature,
    )
    return CooldownAssignmentSnapshotV2(
        schema_version=SNAPSHOT_SCHEMA_VERSION,
        identity=IDENTITY,
        snapshot_id=snapshot_id,
        assignment_id=f"assignment-{snapshot_id}",
        fill_event_id=f"fill-{snapshot_id}",
        client_order_id=f"order-{snapshot_id}",
        lineage_id=f"lineage-{snapshot_id}",
        lineage_revision=1,
        partial_fill_ordinal=1,
        partial_fill_qty_btc=0.001,
        visibility_profile=PROSPECTIVE_RECEIVE_TIME_PROFILE,
        receive_time_transport_eligible=True,
        clocks=FrozenRow.from_mapping({}),
        sources=FrozenRow.from_mapping({}),
        source_bundle_sha256="a" * 64,
        identity_hashes=FrozenRow.from_mapping({"fixture": "b" * 64}),
        m0_context=frozen_m0,
        feature_block="M2",
        feature_row=frozen_feature,
        field_validity=FrozenRow.from_mapping({}),
        policy_input_valid=True,
        policy_input=policy_input,
        fallback_policy_id=None,
        fallback_reason=None,
        economic_outcomes_read=False,
    )


def _evaluator(policy_sha: str, policy: Any) -> RuntimeCooldownPolicyEvaluator:
    empty = _EmptyPredicateArtifact()
    return RuntimeCooldownPolicyEvaluator(
        policy_sha256=policy_sha,
        predicate_bundle_sha256="c" * 64,
        _policy=policy,
        _predicate_columns=policy.predicate_columns,
        _artifacts={"book.SELL": empty, "trade.SELL": empty},
        _binding_error=None,
    )


def _stream_observation(
    index: int,
    mid: float | None,
    *,
    case_id: str | None = None,
    campaign_age_s: float = 10.0,
    gap: bool = False,
) -> StreamingObservationCase:
    right = BASE_NS + index * WINDOW_NS
    return StreamingObservationCase(
        case_id=case_id or f"window-{index:04d}",
        snapshot_id=f"snapshot-{case_id or index}",
        left_ts_ns=right - WINDOW_NS,
        right_ts_ns=right,
        feature_ready_ts_ns=right + 20_000_000,
        decision_ts_ns=right + 25_000_000,
        market_generation=index,
        depth_generation=index,
        mid_usdc_per_btc=mid,
        source_gap=gap,
        campaign_age_s=campaign_age_s,
    )


def _stream_commands() -> tuple[Any, ...]:
    commands: list[Any] = []
    for index in range(1, 81):
        commands.append(
            _stream_observation(
                index,
                100.0 + 0.02 * index,
                campaign_age_s=100.0,
            )
        )
    commands.append(SaveStreamingCheckpoint("before-reversal"))
    commands.append(_stream_observation(81, 80.0, case_id="reversal-before-restore"))
    commands.append(RestoreStreamingCheckpoint("before-reversal"))
    commands.append(_stream_observation(81, 80.0, case_id="reversal-after-restore"))
    for index in range(82, 321):
        commands.append(
            _stream_observation(
                index,
                None if index == 150 else 80.0,
                campaign_age_s=100.0 if 120 <= index <= 130 else 10.0,
                gap=index == 150,
            )
        )
    commands.append(SaveStreamingCheckpoint("post-reversal"))
    commands.append(ResetUnboundStreamingState())
    commands.append(_stream_observation(500, 81.0, case_id="unbound-restart-fallback"))
    commands.append(RestoreStreamingCheckpoint("post-reversal"))
    commands.append(_stream_observation(321, 80.0, case_id="bound-restart-resume"))
    return tuple(commands)


@pytest.fixture(scope="module")
def cpp_binary() -> Path:
    root = Path(tempfile.mkdtemp(prefix="f05_cpp_parity_test_"))
    try:
        yield compile_cpp_cli(root)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_cpp_matches_python_runtime_for_tri_logic_and_ordered_rules(
    tmp_path: Path,
    cpp_binary: Path,
) -> None:
    policy_path = tmp_path / "policy.json"
    policy_sha = _write_policy(policy_path)
    audited = audit_owner_policy_json(
        policy_path,
        expected_policy_sha256=policy_sha,
    )
    evaluator = _evaluator(policy_sha, audited.policy)
    snapshots = (
        (
            "and_first_match",
            _snapshot(
                snapshot_id="and-first-match",
                campaign_age_s=100.0,
                short_cross_age_s=5.0,
                long_cross_age_s=5.0,
            ),
            85_000,
        ),
        (
            "or_not_branch",
            _snapshot(
                snapshot_id="or-not-branch",
                campaign_age_s=100.0,
                short_cross_age_s=20.0,
                long_cross_age_s=5.0,
            ),
            85_000,
        ),
        (
            "second_rule",
            _snapshot(
                snapshot_id="second-rule",
                campaign_age_s=10.0,
                short_cross_age_s=20.0,
                long_cross_age_s=5.0,
            ),
            85_000,
        ),
        (
            "third_rule_not",
            _snapshot(
                snapshot_id="third-rule-not",
                campaign_age_s=10.0,
                short_cross_age_s=20.0,
                long_cross_age_s=20.0,
            ),
            85_000,
        ),
        (
            "unobserved_blocks_later",
            _snapshot(
                snapshot_id="unobserved-blocks-later",
                campaign_age_s=100.0,
                short_cross_age_s=None,
                long_cross_age_s=5.0,
            ),
            85_000,
        ),
        (
            "buy_control",
            _snapshot(snapshot_id="buy-control", side="BUY"),
            85_000,
        ),
        (
            "add_role_and_control_magnitude",
            _snapshot(
                snapshot_id="add-role",
                role="add",
                baseline_duration_ms=170_000,
                campaign_age_s=10.0,
                short_cross_age_s=20.0,
                long_cross_age_s=5.0,
            ),
            170_000,
        ),
    )

    results = compare_cpp_with_runtime(
        binary=cpp_binary,
        policy=audited,
        evaluator=evaluator,
        snapshots=snapshots,
    )

    assert [result.action_id for result in results] == [
        "FIXED_1748S",
        "FIXED_1748S",
        "FIXED_166S",
        "FIXED_211S",
        "CONTROL_85N",
        "CONTROL_85N",
        "FIXED_166S",
    ]
    assert results[0].matched_rule_index == 0
    assert results[4].fallback_reason == "rule_unobserved:0"
    assert results[5].fallback_reason == "buy_control_by_contract"
    assert results[6].duration_ms == 166_000


def test_role_side_and_hash_mismatch_fail_closed(
    tmp_path: Path,
    cpp_binary: Path,
) -> None:
    policy_path = tmp_path / "policy.json"
    policy_sha = _write_policy(policy_path)
    audited = audit_owner_policy_json(policy_path)
    evaluator = _evaluator(policy_sha, audited.policy)
    snapshot = _snapshot(snapshot_id="base")
    valid_case = parity_case_from_snapshot(
        case_id="base",
        evaluator=evaluator,
        snapshot=snapshot,
        baseline_duration_ms=85_000,
    )

    reducing_m0 = {**snapshot.m0_context.to_dict(), "role_at_fill": "reducing"}
    with pytest.raises(FeatureContractError, match="BUY/SELL opener/add"):
        validate_m0_context(reducing_m0)
    invalid_role = replace(
        valid_case,
        case_id="invalid_role",
        snapshot_id="invalid-role",
        role_at_fill="reducing",
    )
    side_drift = replace(
        valid_case,
        case_id="side_drift",
        snapshot_id="side-drift",
        m0_side="BUY",
    )
    cpp_fail_closed = run_cpp_cli(
        cpp_binary,
        build_wire_protocol(audited, (invalid_role, side_drift)),
    )
    assert cpp_fail_closed == (
        reference_decision(audited, invalid_role),
        reference_decision(audited, side_drift),
    )
    assert cpp_fail_closed[0].fallback_reason == ("snapshot_invalid:role_not_exposure_increasing")
    assert cpp_fail_closed[1].fallback_reason == "snapshot_side_inconsistent"

    wrong_sha = "0" * 64
    invalid_python = RuntimeCooldownPolicyEvaluator.from_files(
        policy_path=policy_path,
        predicate_bundle_path=tmp_path / "not-needed-because-hash-fails.json",
        expected_policy_sha256=wrong_sha,
    )
    python_hash = python_decision(
        "base",
        invalid_python.evaluate(snapshot, 85_000),
    )
    cpp_hash = run_cpp_cli(
        cpp_binary,
        build_wire_protocol(
            audited,
            (valid_case,),
            expected_policy_sha256=wrong_sha,
        ),
    )[0]
    assert cpp_hash == python_hash
    assert cpp_hash.action_id == "CONTROL_85N"
    assert cpp_hash.fallback_reason == ("runtime_binding_invalid:policy_file_sha256_mismatch")
    assert cpp_hash.support_valid is False


def test_policy_json_schema_and_canonical_hash_are_audited(tmp_path: Path) -> None:
    policy_path = tmp_path / "policy.json"
    policy_sha = _write_policy(policy_path)
    audited = audit_owner_policy_json(
        policy_path,
        expected_policy_sha256=policy_sha,
    )
    assert audited.file_sha256 == policy_sha
    assert audited.policy.side == "SELL"
    assert len(audited.policy.rules) == 3
    assert audited.predicate_columns == tuple(sorted((CAMPAIGN, CROSS_LONG, CROSS_SHORT)))

    raw = json.loads(policy_path.read_text(encoding="utf-8"))
    raw["schema_version"] = "drifted.schema"
    body = dict(raw)
    body.pop("canonical_sha256")
    raw["canonical_sha256"] = _canonical_sha256(body)
    policy_path.write_text(
        json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="ascii",
    )
    with pytest.raises(CppParityContractError, match="owner_policy_identity_drifted"):
        audit_owner_policy_json(policy_path)


def test_frozen_sell_m2_literal_closure_is_exactly_eight_occurrences(
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "policy.json"
    _write_policy(policy_path)
    audited = audit_owner_policy_json(policy_path)
    closure = audit_sell_m2_streaming_literal_closure(audited.policy)

    assert len(closure.occurrences) == 8
    assert closure.unique_predicates == tuple(sorted((CAMPAIGN, CROSS_LONG, CROSS_SHORT)))
    assert closure.source_channels == ("mid_usdc_per_btc", "campaign_age_s")
    assert closure.ema_half_lives_s == (4.0, 16.0, 256.0)
    assert closure.ema_pairs_s == ((4.0, 16.0), (16.0, 256.0))

    raw = json.loads(policy_path.read_text(encoding="utf-8"))
    raw["policy"]["ordered_first_match_rules"][1]["clauses"][0]["literals"].pop()
    body = dict(raw)
    body.pop("canonical_sha256")
    raw["canonical_sha256"] = _canonical_sha256(body)
    policy_path.write_text(
        json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="ascii",
    )
    with pytest.raises(
        CppParityContractError,
        match="owner_policy_streaming_literal_closure_drifted",
    ):
        audit_owner_policy_json(policy_path)


def test_streaming_mid_ema_cross_missing_and_checkpoint_match_python(
    tmp_path: Path,
    cpp_binary: Path,
) -> None:
    policy_path = tmp_path / "policy.json"
    policy_sha = _write_policy(policy_path)
    audited = audit_owner_policy_json(policy_path, expected_policy_sha256=policy_sha)
    evaluator = _evaluator(policy_sha, audited.policy)
    commands = _stream_commands()

    results = compare_cpp_stream_with_python(
        binary=cpp_binary,
        policy=audited,
        evaluator=evaluator,
        commands=commands,
        warmup_admitted=True,
        warmup_identity="deterministic-d-minus-1:sha256",
    )
    by_case = {row.case_id: row for row in results}

    before = by_case["reversal-before-restore"]
    after = by_case["reversal-after-restore"]
    assert (
        before.ema_h4s,
        before.ema_h16s,
        before.ema_h256s,
        before.short_effective_sign,
        before.short_last_cross_ts_ns,
        before.long_effective_sign,
        before.long_last_cross_ts_ns,
        before.short_cross_predicate,
        before.long_cross_predicate,
        before.decision.action_id,
    ) == (
        after.ema_h4s,
        after.ema_h16s,
        after.ema_h256s,
        after.short_effective_sign,
        after.short_last_cross_ts_ns,
        after.long_effective_sign,
        after.long_last_cross_ts_ns,
        after.short_cross_predicate,
        after.long_cross_predicate,
        after.decision.action_id,
    )

    gap = by_case["window-0150"]
    assert gap.current_window_observed is False
    assert gap.support_valid is False
    assert gap.short_cross_predicate == gap.long_cross_predicate == -1
    assert gap.decision.fallback_reason == "snapshot_m2_support_invalid"
    assert by_case["window-0151"].current_window_observed is True
    assert by_case["window-0151"].support_valid is True

    assert {row.short_cross_predicate for row in results} == {-1, 0, 1}
    assert {row.long_cross_predicate for row in results} == {-1, 0, 1}
    assert {row.decision.action_id for row in results} >= {
        "CONTROL_85N",
        "FIXED_166S",
        "FIXED_211S",
        "FIXED_1748S",
    }

    unbound = by_case["unbound-restart-fallback"]
    assert unbound.warmup_admitted is False
    assert unbound.support_valid is False
    assert unbound.decision.action_id == "CONTROL_85N"
    assert unbound.decision.fallback_reason == "snapshot_m2_support_invalid"
    resumed = by_case["bound-restart-resume"]
    assert resumed.warmup_admitted is True
    assert resumed.window_count == 321
    assert resumed.support_valid is True


def test_streaming_policy_hash_mismatch_fails_closed(
    tmp_path: Path,
    cpp_binary: Path,
) -> None:
    policy_path = tmp_path / "policy.json"
    _write_policy(policy_path)
    audited = audit_owner_policy_json(policy_path)
    command = _stream_observation(1, 100.0, case_id="hash-mismatch")
    protocol = build_stream_wire_protocol(
        audited,
        (command,),
        warmup_admitted=True,
        warmup_identity="bound-warmup",
        expected_policy_sha256="0" * 64,
    )
    result = run_cpp_stream_cli(cpp_binary, protocol)[0]
    assert result.decision.action_id == "CONTROL_85N"
    assert result.decision.support_valid is False
    assert result.decision.fallback_reason == (
        "runtime_binding_invalid:policy_file_sha256_mismatch"
    )
