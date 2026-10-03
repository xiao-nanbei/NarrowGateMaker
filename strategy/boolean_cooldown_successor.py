"""Inactive runtime evaluator for a future hash-bound cooldown successor.

This module is intentionally not wired into MakerEngine.  It exists so a
research artifact can be compiled and checked against production-grade
three-valued semantics before any future deployment decision.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

CONTROL_ACTION = "CONTROL_85N"
_DURATION_RE = re.compile(r"^FIXED_([1-9][0-9]*)S$")


class SuccessorRuntimePolicyError(ValueError):
    """Raised when an inactive successor artifact cannot be compiled."""


def _tri_not(value: int) -> int:
    return -1 if value == -1 else 1 - value


def _tri_and(values: Sequence[int]) -> int:
    if any(value == 0 for value in values):
        return 0
    return -1 if any(value == -1 for value in values) else 1


def _tri_or(values: Sequence[int]) -> int:
    if any(value == 1 for value in values):
        return 1
    return -1 if any(value == -1 for value in values) else 0


@dataclass(frozen=True, slots=True)
class SuccessorRuntimeDecision:
    action_id: str
    duration_ms: int
    fallback_reason: str | None
    matched_rule_index: int | None
    support_valid: bool
    policy_sha256: str
    predicate_bundle_sha256: str


class InactiveSuccessorCooldownEvaluator:
    """Generic BUY/SELL evaluator with no live wiring or action authority."""

    def __init__(
        self,
        *,
        side: str,
        rules: tuple[tuple[str, tuple[tuple[tuple[str, bool], ...], ...]], ...],
        policy_sha256: str,
        predicate_bundle_sha256: str,
    ) -> None:
        normalized = str(side).upper()
        if normalized not in {"BUY", "SELL"}:
            raise SuccessorRuntimePolicyError("successor side must be BUY or SELL")
        if not rules:
            raise SuccessorRuntimePolicyError("successor runtime rules are empty")
        if any(_DURATION_RE.fullmatch(action) is None for action, _ in rules):
            raise SuccessorRuntimePolicyError("successor action vocabulary is invalid")
        self.side = normalized
        self.rules = rules
        self.policy_sha256 = str(policy_sha256)
        self.predicate_bundle_sha256 = str(predicate_bundle_sha256)
        self.predicate_columns = tuple(
            sorted(
                {
                    name
                    for _, clauses in rules
                    for clause in clauses
                    for name, _ in clause
                }
            )
        )
        if not self.predicate_columns:
            raise SuccessorRuntimePolicyError("successor predicate set is empty")

    @classmethod
    def from_policy_payload(
        cls,
        payload: Mapping[str, Any],
        *,
        policy_sha256: str,
        predicate_bundle_sha256: str,
    ) -> InactiveSuccessorCooldownEvaluator:
        side = str(payload.get("side", "")).upper()
        if payload.get("default_action") != CONTROL_ACTION:
            raise SuccessorRuntimePolicyError("successor default action must remain control")
        raw_rules = payload.get("ordered_first_match_rules")
        if not isinstance(raw_rules, list) or not raw_rules:
            raise SuccessorRuntimePolicyError("successor artifact has no ordered rules")
        parsed: list[tuple[str, tuple[tuple[tuple[str, bool], ...], ...]]] = []
        for raw_rule in raw_rules:
            if not isinstance(raw_rule, Mapping):
                raise SuccessorRuntimePolicyError("successor rule is malformed")
            action = str(raw_rule.get("action", ""))
            raw_clauses = raw_rule.get("clauses")
            if not isinstance(raw_clauses, list) or not raw_clauses:
                raise SuccessorRuntimePolicyError("successor rule lacks clauses")
            clauses: list[tuple[tuple[str, bool], ...]] = []
            for raw_clause in raw_clauses:
                literals = raw_clause.get("literals") if isinstance(raw_clause, Mapping) else None
                if not isinstance(literals, list) or not literals:
                    raise SuccessorRuntimePolicyError("successor clause lacks literals")
                clause: list[tuple[str, bool]] = []
                for raw_literal in literals:
                    if not isinstance(raw_literal, Mapping):
                        raise SuccessorRuntimePolicyError("successor literal is malformed")
                    name = str(raw_literal.get("predicate", "")).strip()
                    if not name:
                        raise SuccessorRuntimePolicyError("successor literal name is empty")
                    clause.append((name, bool(raw_literal.get("negated", False))))
                clauses.append(tuple(clause))
            parsed.append((action, tuple(clauses)))
        return cls(
            side=side,
            rules=tuple(parsed),
            policy_sha256=policy_sha256,
            predicate_bundle_sha256=predicate_bundle_sha256,
        )

    def _control(
        self,
        baseline_duration_ms: int,
        *,
        reason: str,
        support_valid: bool,
    ) -> SuccessorRuntimeDecision:
        return SuccessorRuntimeDecision(
            action_id=CONTROL_ACTION,
            duration_ms=baseline_duration_ms,
            fallback_reason=reason,
            matched_rule_index=None,
            support_valid=support_valid,
            policy_sha256=self.policy_sha256,
            predicate_bundle_sha256=self.predicate_bundle_sha256,
        )

    def evaluate_predicates(
        self,
        *,
        side: str,
        predicate_values: Mapping[str, Any],
        baseline_duration_ms: int | float,
        feature_ready: bool = True,
        stale: bool = False,
    ) -> SuccessorRuntimeDecision:
        try:
            baseline_float = float(baseline_duration_ms)
            baseline = int(round(baseline_float))
            if (
                not math.isfinite(baseline_float)
                or baseline <= 0
                or not math.isclose(baseline_float, float(baseline), abs_tol=1e-6)
            ):
                raise SuccessorRuntimePolicyError("baseline_duration_ms_invalid")
        except (TypeError, ValueError, OverflowError):
            baseline = 85_000
            return self._control(
                baseline,
                reason="baseline_duration_ms_invalid",
                support_valid=False,
            )
        if str(side).upper() != self.side:
            return self._control(
                baseline,
                reason="opposite_side_control_by_contract",
                support_valid=True,
            )
        if not feature_ready:
            return self._control(
                baseline,
                reason="warmup_incomplete",
                support_valid=False,
            )
        if stale:
            return self._control(
                baseline,
                reason="feature_stale",
                support_valid=False,
            )
        if tuple(sorted(str(name) for name in predicate_values)) != self.predicate_columns:
            return self._control(
                baseline,
                reason="runtime_predicate_columns_drifted",
                support_valid=False,
            )
        try:
            values = {name: int(predicate_values[name]) for name in self.predicate_columns}
        except (TypeError, ValueError):
            return self._control(
                baseline,
                reason="runtime_predicate_not_three_valued",
                support_valid=False,
            )
        if any(value not in {-1, 0, 1} for value in values.values()):
            return self._control(
                baseline,
                reason="runtime_predicate_not_three_valued",
                support_valid=False,
            )
        for index, (action, clauses) in enumerate(self.rules):
            clause_states = []
            for clause in clauses:
                clause_states.append(
                    _tri_and(
                        tuple(
                            _tri_not(values[name]) if negated else values[name]
                            for name, negated in clause
                        )
                    )
                )
            state = _tri_or(clause_states)
            if state == -1:
                return self._control(
                    baseline,
                    reason=f"rule_unobserved:{index}",
                    support_valid=False,
                )
            if state == 1:
                seconds = int(_DURATION_RE.fullmatch(action).group(1))
                return SuccessorRuntimeDecision(
                    action_id=action,
                    duration_ms=seconds * 1_000,
                    fallback_reason=None,
                    matched_rule_index=index,
                    support_valid=True,
                    policy_sha256=self.policy_sha256,
                    predicate_bundle_sha256=self.predicate_bundle_sha256,
                )
        return self._control(
            baseline,
            reason="no_rule_matched",
            support_valid=True,
        )


__all__ = [
    "CONTROL_ACTION",
    "InactiveSuccessorCooldownEvaluator",
    "SuccessorRuntimeDecision",
    "SuccessorRuntimePolicyError",
]
