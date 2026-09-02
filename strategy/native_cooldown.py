"""Optional zero-allocation C++ runtime for loaded F05 cooldown policies.

Artifact loading and identity validation remain Python startup work.  This
module only compiles the already validated rule/feature program into the fixed
capacity native hot path; no runtime file read or digest calculation occurs.
"""

from __future__ import annotations

import math
import os
import re
from collections.abc import Mapping
from typing import Any

_FIXED_DURATION_RE = re.compile(r"FIXED_(\d+)S")
_BUY_E3_SOURCE_RE = re.compile(
    r"^(?:tri|value)::mid_usdc_per_btc__h"
    r"(?P<fast>\d+(?:p\d+)?)s__h(?P<slow>\d+(?:p\d+)?)s::"
    r"(?P<metric>[a-z_]+)$"
)
_DIRECT_CAMPAIGN_AGE = "predicate::m0::campaign_age_gt_control_duration"


def native_cooldown_requested(value: bool | None = None) -> bool:
    if value is not None:
        return bool(value)
    return os.environ.get("NARROWGATE_CPP_COOLDOWN", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def native_strict() -> bool:
    return os.environ.get("NARROWGATE_CPP_STRICT", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _literal(cpp: Any, predicate_index: int, negated: bool) -> Any:
    value = cpp.F05BooleanLiteral()
    value.predicate_index = int(predicate_index)
    value.negated = bool(negated)
    return value


def compile_policy(cpp: Any, policy: Any, *, declarative: bool) -> Any:
    """Compile one startup-validated Python policy into the shared F05 ABI."""

    evaluator = policy.evaluator
    columns = tuple(str(value) for value in evaluator.predicate_columns)
    if not columns or tuple(sorted(columns)) != columns or len(set(columns)) != len(columns):
        raise ValueError("cooldown_cpp_predicate_columns_invalid")
    compiled = cpp.F05BooleanPolicy()
    compiled.policy_sha256 = str(evaluator.policy_sha256).lower()
    compiled.predicate_bundle_sha256 = str(evaluator.predicate_bundle_sha256).lower()
    compiled.predicate_columns = list(columns)
    compiled.default_action = "CONTROL_85N"
    column_index = {name: index for index, name in enumerate(columns)}
    rules = []
    for action_id, raw_clauses in evaluator.rules:
        match = _FIXED_DURATION_RE.fullmatch(str(action_id))
        if match is None:
            raise ValueError("cooldown_cpp_rule_action_invalid")
        rule = cpp.F05BooleanRule()
        rule.action_id = str(action_id)
        rule.duration_ms = int(match.group(1)) * 1_000
        clauses = []
        for raw_clause in raw_clauses:
            clause = cpp.F05BooleanClause()
            clause.literals = [
                _literal(cpp, column_index[str(name)], bool(negated))
                for name, negated in raw_clause
            ]
            if not clause.literals:
                raise ValueError("cooldown_cpp_rule_clause_empty")
            clauses.append(clause)
        if not clauses:
            raise ValueError("cooldown_cpp_rule_clauses_empty")
        rule.clauses = clauses
        rules.append(rule)
    compiled.rules = rules
    if not declarative:
        return compiled

    half_lives = tuple(float(value) for value in policy.ema_half_lives_s)
    pairs = tuple((float(fast), float(slow)) for fast, slow in policy.ema_pairs_s)
    half_life_index = {value: index for index, value in enumerate(half_lives)}
    if (
        not half_lives
        or tuple(sorted(half_lives)) != half_lives
        or len(half_life_index) != len(half_lives)
        or any(not math.isfinite(value) or value <= 0.0 for value in half_lives)
    ):
        raise ValueError("cooldown_cpp_ema_half_lives_invalid")
    compiled.ema_half_lives_s = list(half_lives)
    cpp_pairs = []
    pair_index: dict[tuple[float, float], int] = {}
    for fast, slow in pairs:
        if fast not in half_life_index or slow not in half_life_index or fast >= slow:
            raise ValueError("cooldown_cpp_ema_pair_invalid")
        pair_index[(fast, slow)] = len(cpp_pairs)
        pair = cpp.F05PredicatePair()
        pair.fast_ema_index = half_life_index[fast]
        pair.slow_ema_index = half_life_index[slow]
        cpp_pairs.append(pair)
    if len(pair_index) != len(pairs):
        raise ValueError("cooldown_cpp_ema_pair_duplicate")
    compiled.predicate_pairs = cpp_pairs

    metric_by_name = {
        "positive_ordering": cpp.F05PredicateMetric.POSITIVE_ORDERING,
        "last_cross_positive": cpp.F05PredicateMetric.LAST_CROSS_POSITIVE,
        "expanding": cpp.F05PredicateMetric.EXPANDING,
        "converging": cpp.F05PredicateMetric.CONVERGING,
        "abs_distance": cpp.F05PredicateMetric.ABS_DISTANCE,
        "cross_age_s": cpp.F05PredicateMetric.CROSS_AGE_S,
        "arrangement_persistence_s": cpp.F05PredicateMetric.ARRANGEMENT_PERSISTENCE_S,
        "signed_distance": cpp.F05PredicateMetric.SIGNED_DISTANCE,
        "signed_distance_velocity": cpp.F05PredicateMetric.SIGNED_DISTANCE_VELOCITY,
        "signed_distance_acceleration": cpp.F05PredicateMetric.SIGNED_DISTANCE_ACCELERATION,
    }
    raw_definitions = {str(name): value for name, value in policy.definitions.items()}
    direct = frozenset(str(name) for name in policy.direct_predicates)
    if direct - {_DIRECT_CAMPAIGN_AGE}:
        raise ValueError("cooldown_cpp_direct_predicate_unsupported")
    definitions = []
    for index, name in enumerate(columns):
        definition = cpp.F05PredicateDefinition()
        definition.predicate_index = index
        if name in direct:
            definition.metric = cpp.F05PredicateMetric.CAMPAIGN_AGE_GT_CONTROL
            definitions.append(definition)
            continue
        raw = raw_definitions.get(name)
        if not isinstance(raw, Mapping):
            raise ValueError("cooldown_cpp_predicate_definition_missing")
        match = _BUY_E3_SOURCE_RE.fullmatch(str(raw.get("source_field", "")))
        if match is None:
            raise ValueError("cooldown_cpp_predicate_source_unsupported")
        pair = (
            float(match.group("fast").replace("p", ".")),
            float(match.group("slow").replace("p", ".")),
        )
        metric = metric_by_name.get(match.group("metric"))
        if pair not in pair_index or metric is None:
            raise ValueError("cooldown_cpp_predicate_metric_or_pair_unsupported")
        definition.metric = metric
        definition.pair_index = pair_index[pair]
        kind = str(raw.get("kind", ""))
        if kind == "preserved_tri":
            definition.threshold_enabled = False
        elif kind == "quantile_ge":
            threshold = float(raw.get("threshold"))
            if not math.isfinite(threshold):
                raise ValueError("cooldown_cpp_predicate_threshold_invalid")
            definition.threshold_enabled = True
            definition.threshold = threshold
        else:
            raise ValueError("cooldown_cpp_predicate_kind_unsupported")
        definitions.append(definition)
    if set(raw_definitions) | set(direct) != set(columns):
        raise ValueError("cooldown_cpp_predicate_definition_set_drifted")
    compiled.predicate_definitions = definitions
    return compiled


def build_hot_path(
    policy: Any,
    *,
    profile: str,
    warmup_s: float,
    max_feature_age_s: float,
    requested: bool | None = None,
) -> tuple[Any | None, Any | None]:
    """Return ``(native_module, hot_path)`` or a safe Python fallback pair."""

    if not native_cooldown_requested(requested):
        return None, None
    try:
        import narrowgate_cpp as cpp

        if not bool(getattr(cpp, "NATIVE_LIVE_COOLDOWN_HOT_PATH_AVAILABLE", False)):
            raise RuntimeError("native_live_cooldown_hot_path_unavailable")
        is_buy = str(profile).upper() == "BUY"
        compiled = compile_policy(cpp, policy, declarative=is_buy)
        native_profile = (
            cpp.LiveCooldownProfile.BUY_E3
            if is_buy
            else cpp.LiveCooldownProfile.SELL_SELECTED
        )
        return cpp, cpp.NativeLiveCooldownHotPath(
            native_profile,
            compiled,
            float(warmup_s),
            float(max_feature_age_s),
        )
    except Exception:
        if native_strict():
            raise
        return None, None


def native_fallback_reason(cpp: Any, decision: Any) -> str | None:
    status = decision.status
    if status == cpp.LiveCooldownDecisionStatus.RULE_MATCHED:
        return None
    if status == cpp.LiveCooldownDecisionStatus.NO_RULE_MATCHED:
        return "no_rule_matched"
    if status == cpp.LiveCooldownDecisionStatus.NO_COMPLETED_WINDOW:
        return "no_completed_receive_time_window"
    if status == cpp.LiveCooldownDecisionStatus.WARMUP_INCOMPLETE:
        return "receive_time_ema_warmup_incomplete"
    if status == cpp.LiveCooldownDecisionStatus.FEATURE_STATE_STALE:
        return "receive_time_mid_state_stale"
    if status == cpp.LiveCooldownDecisionStatus.LATEST_WINDOW_UNOBSERVED:
        return "latest_completed_mid_window_unobserved"
    if status == cpp.LiveCooldownDecisionStatus.SELECTED_PREDICATE_UNOBSERVED:
        return "selected_predicate_state_unobserved"
    if status == cpp.LiveCooldownDecisionStatus.RULE_UNOBSERVED:
        return f"rule_unobserved:{int(decision.detail_index)}"
    raise RuntimeError("native_live_cooldown_status_unknown")


__all__ = [
    "build_hot_path",
    "compile_policy",
    "native_cooldown_requested",
    "native_fallback_reason",
    "native_strict",
]
