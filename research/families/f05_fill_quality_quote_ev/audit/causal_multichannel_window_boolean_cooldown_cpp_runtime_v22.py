"""Compile the exact active F05 owner artifact into the C++ replay ABI."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)
from strategy import boolean_cooldown_live as live_policy

IDENTITY = "f05_exact_owner_cpp_runtime_v22"
QUALIFICATION_SCOPE = "real_day_all_arm_full_replay_v22"
FEATURE_CLOCK_SEMANTICS = "historical_exchange_m2_v1"
_DURATION_RE = re.compile(r"^FIXED_([1-9][0-9]*)S$")


class CppOwnerRuntimeError(RuntimeError):
    """Raised when the exact owner artifact cannot compile without drift."""


def _literal(cpp: Any, index: int, *, negated: bool) -> Any:
    value = cpp.F05BooleanLiteral()
    value.predicate_index = int(index)
    value.negated = bool(negated)
    return value


def compile_owner_policy(
    cpp: Any,
    *,
    policy_path: Path,
    predicate_bundle_path: Path,
) -> Any:
    evaluator = live_policy.RuntimeCooldownPolicyEvaluator.from_files(
        policy_path=policy_path,
        policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
        predicate_bundle_path=predicate_bundle_path,
        predicate_bundle_sha256=offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
    )
    if not evaluator.binding_valid:
        raise CppOwnerRuntimeError(f"exact owner policy binding failed: {evaluator.binding_error}")
    payload = json.loads(policy_path.read_text(encoding="utf-8"))
    raw_policy = payload.get("policy")
    if not isinstance(raw_policy, Mapping):
        raise CppOwnerRuntimeError("exact owner policy payload is missing")
    if (
        raw_policy.get("side") != "SELL"
        or raw_policy.get("default_action") != live_policy.CONTROL_ACTION
    ):
        raise CppOwnerRuntimeError("exact owner side/default action drifted")
    predicate_columns = tuple(evaluator.predicate_columns)
    if predicate_columns != live_policy.OWNER_POLICY_SELECTED_PREDICATES:
        raise CppOwnerRuntimeError("exact owner predicate order drifted")
    predicate_index = {name: index for index, name in enumerate(predicate_columns)}

    compiled_rules = []
    for raw_rule in raw_policy.get("ordered_first_match_rules", ()):
        if not isinstance(raw_rule, Mapping):
            raise CppOwnerRuntimeError("exact owner rule is malformed")
        action_id = str(raw_rule.get("action", ""))
        match = _DURATION_RE.fullmatch(action_id)
        if match is None:
            raise CppOwnerRuntimeError("exact owner action identity drifted")
        clauses = []
        for raw_clause in raw_rule.get("clauses", ()):
            if not isinstance(raw_clause, Mapping):
                raise CppOwnerRuntimeError("exact owner clause is malformed")
            literals = []
            for raw_literal in raw_clause.get("literals", ()):
                if not isinstance(raw_literal, Mapping):
                    raise CppOwnerRuntimeError("exact owner literal is malformed")
                name = str(raw_literal.get("predicate", ""))
                if name not in predicate_index:
                    raise CppOwnerRuntimeError("exact owner predicate escaped its ABI")
                literals.append(
                    _literal(
                        cpp,
                        predicate_index[name],
                        negated=bool(raw_literal.get("negated", False)),
                    )
                )
            if not literals:
                raise CppOwnerRuntimeError("exact owner clause is empty")
            clause = cpp.F05BooleanClause()
            clause.literals = literals
            clauses.append(clause)
        if not clauses:
            raise CppOwnerRuntimeError("exact owner rule is empty")
        rule = cpp.F05BooleanRule()
        rule.action_id = action_id
        rule.duration_ms = int(match.group(1)) * 1_000
        rule.clauses = clauses
        compiled_rules.append(rule)
    if not compiled_rules:
        raise CppOwnerRuntimeError("exact owner compiled rule list is empty")

    policy = cpp.F05BooleanPolicy()
    policy.policy_sha256 = offline.ACTIVE_OWNER_POLICY_SHA256
    policy.predicate_bundle_sha256 = offline.ACTIVE_PREDICATE_BUNDLE_SHA256
    policy.predicate_columns = list(predicate_columns)
    policy.rules = compiled_rules
    policy.default_action = live_policy.CONTROL_ACTION
    return policy


def build_cpp_runtime_config(
    cpp: Any,
    *,
    policy_path: Path,
    predicate_bundle_path: Path,
    qualification_sha256: str,
) -> Any:
    if re.fullmatch(r"[0-9a-f]{64}", qualification_sha256) is None:
        raise CppOwnerRuntimeError("C++ qualification SHA256 is invalid")
    config = cpp.F05RepeatedBooleanCooldownConfig()
    config.parity_qualified = True
    config.parity_qualification_sha256 = qualification_sha256
    config.qualification_scope = QUALIFICATION_SCOPE
    config.feature_clock_semantics = FEATURE_CLOCK_SEMANTICS
    config.warmup_s = 2048.0
    config.max_feature_age_s = 5.0
    config.policy = compile_owner_policy(
        cpp,
        policy_path=policy_path,
        predicate_bundle_path=predicate_bundle_path,
    )
    return config


def build_cpp_runtime(
    cpp: Any,
    *,
    policy_path: Path,
    predicate_bundle_path: Path,
    qualification_sha256: str,
) -> Any:
    config = build_cpp_runtime_config(
        cpp,
        policy_path=policy_path,
        predicate_bundle_path=predicate_bundle_path,
        qualification_sha256=qualification_sha256,
    )
    runtime = cpp.F05RepeatedBooleanCooldownRuntime(config)
    if not runtime.parity_qualified:
        raise CppOwnerRuntimeError(
            f"compiled C++ owner runtime is invalid: {runtime.binding_error}"
        )
    return runtime


def build_shared_observation_tape(
    cpp: Any,
    arrays: Mapping[str, Any],
    *,
    content_sha256: str,
) -> Any:
    required = (
        "left_ts_ns",
        "right_ts_ns",
        "feature_ready_ts_ns",
        "market_generation",
        "depth_generation",
        "mid_usdc_per_btc",
        "source_gap",
        "source_stale",
        "warmup_admitted",
        "channel_support_valid",
    )
    if set(arrays) != set(required):
        raise CppOwnerRuntimeError("C++ observation array schema drifted")
    if re.fullmatch(r"[0-9a-f]{64}", content_sha256) is None:
        raise CppOwnerRuntimeError("C++ observation tape SHA256 is invalid")
    tape = cpp.build_f05_cooldown_window_tape(
        *(arrays[name] for name in required),
        content_sha256,
    )
    if int(tape.size) <= 0 or str(tape.content_sha256) != content_sha256:
        raise CppOwnerRuntimeError("C++ shared observation tape binding drifted")
    return tape


def build_target_predicate_row(cpp: Any, opportunity: Mapping[str, Any]) -> Any:
    row = cpp.F05CooldownPredicateRow()
    row.exposure_fill_ordinal = int(opportunity["exposure_fill_ordinal"])
    row.fill_ts_ms = int(opportunity["fill_visible_ts_ms"])
    row.side = cpp.Side.Buy if str(opportunity["side"]).upper() == "BUY" else cpp.Side.Sell
    row.campaign_id = int(opportunity["campaign_id"])
    row.snapshot_id = str(opportunity["opportunity_id"])
    row.policy_input_valid = bool(opportunity.get("feature::support_valid", False))
    row.support_valid = bool(opportunity.get("feature::support_valid", False))
    row.channel_support_valid = bool(opportunity.get("feature::channel_support_valid", False))
    row.snapshot_fallback_reason = str(opportunity.get("owner_fallback_reason") or "")
    row.predicate_values = []
    return row


def validate_target_predicate_row(
    cpp: Any,
    row: Any,
    opportunity: Mapping[str, Any],
    *,
    expected_predicate_count: int,
) -> None:
    if not isinstance(expected_predicate_count, int) or expected_predicate_count <= 0:
        raise CppOwnerRuntimeError("C++ predicate count is invalid")
    expected_side = cpp.Side.Buy if str(opportunity["side"]).upper() == "BUY" else cpp.Side.Sell
    identity = {
        "exposure_fill_ordinal": (
            int(row.exposure_fill_ordinal),
            int(opportunity["exposure_fill_ordinal"]),
        ),
        "fill_ts_ms": (int(row.fill_ts_ms), int(opportunity["fill_visible_ts_ms"])),
        "campaign_id": (int(row.campaign_id), int(opportunity["campaign_id"])),
        "snapshot_id": (str(row.snapshot_id), str(opportunity["opportunity_id"])),
    }
    drifted = [name for name, (actual, expected) in identity.items() if actual != expected]
    if drifted or row.side != expected_side:
        raise CppOwnerRuntimeError(
            "C++ target predicate-row identity drifted: " + ",".join(drifted or ["side"])
        )
    if int(row.exposure_fill_ordinal) <= 0 or int(row.fill_ts_ms) <= 0:
        raise CppOwnerRuntimeError("C++ target predicate-row identity is incomplete")
    values = list(row.predicate_values)
    if values and len(values) != expected_predicate_count:
        raise CppOwnerRuntimeError("C++ target predicate-row width drifted")
    allowed = {
        cpp.F05TriState.UNOBSERVED,
        cpp.F05TriState.FALSE,
        cpp.F05TriState.TRUE,
    }
    if any(value not in allowed for value in values):
        raise CppOwnerRuntimeError("C++ target predicate-row contains an invalid state")


__all__ = [
    "CppOwnerRuntimeError",
    "FEATURE_CLOCK_SEMANTICS",
    "IDENTITY",
    "QUALIFICATION_SCOPE",
    "build_cpp_runtime",
    "build_cpp_runtime_config",
    "build_shared_observation_tape",
    "build_target_predicate_row",
    "compile_owner_policy",
    "validate_target_predicate_row",
]
