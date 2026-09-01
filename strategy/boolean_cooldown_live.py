"""Receive-time live bridge for the frozen F05 owner cooldown policy."""

from __future__ import annotations

import hashlib
import json
import math
import re
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BASE_WINDOW_WIDTH_NS = 100_000_000
CONTROL_ACTION = "CONTROL_85N"
OWNER_POLICY_IDENTITY = "causal_multichannel_window_boolean_cooldown_owner_policy_v1"
OWNER_POLICY_SCHEMA = f"{OWNER_POLICY_IDENTITY}.artifact.v1"
OWNER_POLICY_SELECTED_PREDICATES = (
    "predicate::ema_pair_h16s_h256s:cross_age_le_fast",
    "predicate::ema_pair_h4s_h16s:cross_age_le_slow",
    "predicate::m0::campaign_age_gt_control_duration",
)
LIVE_FEATURE_TRANSPORT_IDENTITY = "receive_time_100ms_selected_mid_predicates_v1"

_DURATION_RE = re.compile(r"^FIXED_([1-9][0-9]*)S$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SELECTED_HALF_LIVES_S = (4.0, 16.0, 256.0)
_SELECTED_PAIRS = ((4.0, 16.0), (16.0, 256.0))


@dataclass(frozen=True, slots=True)
class CooldownDurationDecision:
    action_id: str
    duration_ms: int
    fallback_reason: str | None
    matched_rule_index: int | None
    policy_sha256: str
    predicate_bundle_sha256: str
    snapshot_id: str
    support_valid: bool


@dataclass(frozen=True, slots=True)
class LiveBooleanCooldownDecision:
    action_id: str
    duration_ms: int
    fallback_reason: str | None
    matched_rule_index: int | None
    support_valid: bool
    policy_sha256: str
    predicate_bundle_sha256: str
    feature_ready_ts_ns: int
    feature_age_ms: float


@dataclass(slots=True)
class _PairState:
    effective_sign: int = 0
    last_cross_ts_ns: int | None = None


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _load_bound_json(path: Path, expected_sha256: str, label: str) -> dict[str, Any]:
    expected = str(expected_sha256).strip().lower()
    raw_bytes = path.read_bytes()
    if (
        not _SHA256_RE.fullmatch(expected)
        or hashlib.sha256(raw_bytes).hexdigest() != expected
    ):
        raise ValueError(f"{label}_file_sha256_mismatch")
    raw = json.loads(raw_bytes.decode("utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{label}_root_not_object")
    canonical = str(raw.get("canonical_sha256", ""))
    body = dict(raw)
    body.pop("canonical_sha256", None)
    if not _SHA256_RE.fullmatch(canonical) or _canonical_sha256(body) != canonical:
        raise ValueError(f"{label}_canonical_sha256_mismatch")
    return raw


def _tri_not(value: int) -> int:
    return -1 if value == -1 else 1 - value


def _literal_state(value: int, negated: bool) -> int:
    if value not in (-1, 0, 1):
        raise ValueError("runtime_predicate_not_three_valued")
    return _tri_not(value) if negated else value


def _and_state(values: Sequence[int]) -> int:
    if any(value == 0 for value in values):
        return 0
    return -1 if any(value == -1 for value in values) else 1


def _or_state(values: Sequence[int]) -> int:
    if any(value == 1 for value in values):
        return 1
    return -1 if any(value == -1 for value in values) else 0


class RuntimeCooldownPolicyEvaluator:
    """Small production evaluator for one hash-bound sparse Boolean policy."""

    def __init__(
        self,
        *,
        rules: tuple[tuple[str, tuple[tuple[tuple[str, bool], ...], ...]], ...],
        policy_sha256: str,
        predicate_bundle_sha256: str,
    ) -> None:
        self._rules = rules
        self.policy_sha256 = policy_sha256
        self.predicate_bundle_sha256 = predicate_bundle_sha256
        self._predicate_columns = tuple(
            sorted(
                {
                    name
                    for _, clauses in rules
                    for clause in clauses
                    for name, _ in clause
                }
            )
        )
        self._lock = threading.Lock()
        self._evaluations = 0
        self._supported = 0
        self._fallback = 0
        self._nonbaseline = 0

    @classmethod
    def from_files(
        cls,
        *,
        policy_path: str | Path,
        policy_sha256: str,
        predicate_bundle_path: str | Path,
        predicate_bundle_sha256: str,
    ) -> RuntimeCooldownPolicyEvaluator:
        policy_path = Path(policy_path).expanduser().resolve()
        bundle_path = Path(predicate_bundle_path).expanduser().resolve()
        policy = _load_bound_json(policy_path, policy_sha256, "policy")
        bundle = _load_bound_json(
            bundle_path,
            predicate_bundle_sha256,
            "predicate_bundle",
        )
        if (
            policy.get("identity") != OWNER_POLICY_IDENTITY
            or policy.get("schema_version") != OWNER_POLICY_SCHEMA
            or policy.get("selection", {}).get("BUY") != CONTROL_ACTION
        ):
            raise ValueError("owner_policy_identity_drifted")
        bound_bundle = (
            policy.get("bindings", {})
            .get("panel", {})
            .get("outcome_blind_2025_predicates", {})
            .get("bundle", {})
        )
        if (
            bound_bundle.get("file_sha256") != str(predicate_bundle_sha256).lower()
            or bound_bundle.get("canonical_sha256")
            != bundle.get("canonical_sha256")
        ):
            raise ValueError("policy_predicate_bundle_binding_drifted")
        if (
            bundle.get("m0_artifacts") != []
            or bundle.get("cross_clock_clause_authorized") is not False
            or bundle.get("strict_2026_target_snapshot", {}).get(
                "book_trade_predicates_may_be_combined_by_study"
            )
            is not True
        ):
            raise ValueError("predicate_bundle_identity_drifted")

        policy_artifacts = (
            policy.get("bindings", {})
            .get("panel", {})
            .get("outcome_blind_2025_predicates", {})
            .get("artifacts", {})
        )
        if not isinstance(policy_artifacts, Mapping):
            raise ValueError("policy_predicate_artifact_bindings_missing")
        for group in ("book", "trade"):
            group_entries = bundle.get(group)
            if not isinstance(group_entries, Mapping):
                raise ValueError("predicate_bundle_artifact_group_missing")
            for side in ("BUY", "SELL"):
                entry = group_entries.get(side)
                binding = policy_artifacts.get(f"{group}.{side}")
                if not isinstance(entry, Mapping) or not isinstance(binding, Mapping):
                    raise ValueError("predicate_bundle_artifact_binding_missing")
                artifact_path = (bundle_path.parent / str(entry.get("path", ""))).resolve()
                artifact_sha = str(entry.get("sha256", ""))
                artifact = _load_bound_json(
                    artifact_path,
                    artifact_sha,
                    f"predicate_artifact_{group}_{side.lower()}",
                )
                if (
                    binding.get("file_sha256") != artifact_sha
                    or binding.get("canonical_sha256")
                    != artifact.get("canonical_sha256")
                    or binding.get("reference_identity_sha256")
                    != artifact.get("reference_identity_sha256")
                ):
                    raise ValueError("policy_predicate_artifact_binding_drifted")

        boolean = policy.get("policy")
        if not isinstance(boolean, Mapping):
            raise ValueError("owner_boolean_policy_missing")
        if str(boolean.get("side", "")).upper() != "SELL":
            raise ValueError("owner_policy_side_drifted")
        if boolean.get("default_action") != CONTROL_ACTION:
            raise ValueError("owner_policy_default_drifted")
        raw_rules = boolean.get("ordered_first_match_rules")
        if not isinstance(raw_rules, list) or not raw_rules:
            raise ValueError("owner_policy_rules_missing")

        parsed: list[tuple[str, tuple[tuple[tuple[str, bool], ...], ...]]] = []
        for raw_rule in raw_rules:
            if not isinstance(raw_rule, Mapping):
                raise ValueError("owner_policy_rule_invalid")
            action = str(raw_rule.get("action", ""))
            if _DURATION_RE.fullmatch(action) is None:
                raise ValueError("owner_policy_action_invalid")
            raw_clauses = raw_rule.get("clauses")
            if not isinstance(raw_clauses, list) or not raw_clauses:
                raise ValueError("owner_policy_clauses_missing")
            clauses: list[tuple[tuple[str, bool], ...]] = []
            for raw_clause in raw_clauses:
                literals = raw_clause.get("literals") if isinstance(raw_clause, Mapping) else None
                if not isinstance(literals, list) or not literals:
                    raise ValueError("owner_policy_literals_missing")
                clause = tuple(
                    (str(item.get("predicate", "")), bool(item.get("negated", False)))
                    for item in literals
                    if isinstance(item, Mapping)
                )
                if len(clause) != len(literals) or any(not name for name, _ in clause):
                    raise ValueError("owner_policy_literal_invalid")
                clauses.append(clause)
            parsed.append((action, tuple(clauses)))
        evaluator = cls(
            rules=tuple(parsed),
            policy_sha256=str(policy_sha256).lower(),
            predicate_bundle_sha256=str(predicate_bundle_sha256).lower(),
        )
        if evaluator.predicate_columns != OWNER_POLICY_SELECTED_PREDICATES:
            raise ValueError("owner_policy_selected_predicates_drifted")
        return evaluator

    @property
    def binding_valid(self) -> bool:
        return True

    @property
    def binding_error(self) -> None:
        return None

    @property
    def predicate_columns(self) -> tuple[str, ...]:
        return self._predicate_columns

    @property
    def rules(
        self,
    ) -> tuple[tuple[str, tuple[tuple[tuple[str, bool], ...], ...]], ...]:
        """Return the immutable compiled rule program for runtime adapters."""

        return self._rules

    def _control(
        self,
        *,
        baseline_duration_ms: int,
        snapshot_id: str,
        reason: str | None,
        support_valid: bool,
    ) -> CooldownDurationDecision:
        return CooldownDurationDecision(
            action_id=CONTROL_ACTION,
            duration_ms=baseline_duration_ms,
            fallback_reason=reason,
            matched_rule_index=None,
            policy_sha256=self.policy_sha256,
            predicate_bundle_sha256=self.predicate_bundle_sha256,
            snapshot_id=snapshot_id,
            support_valid=support_valid,
        )

    def evaluate_predicates(
        self,
        *,
        side: str,
        predicate_values: Mapping[str, Any],
        baseline_duration_ms: int | float,
        snapshot_id: str,
    ) -> CooldownDurationDecision:
        baseline = int(round(float(baseline_duration_ms)))
        snapshot = str(snapshot_id).strip() or "runtime-predicate-row"
        try:
            if baseline <= 0 or not math.isclose(
                float(baseline_duration_ms), float(baseline), abs_tol=1e-6
            ):
                raise ValueError("baseline_duration_ms_invalid")
            normalized_side = str(side).strip().upper()
            if normalized_side == "BUY":
                decision = self._control(
                    baseline_duration_ms=baseline,
                    snapshot_id=snapshot,
                    reason="buy_control_by_contract",
                    support_valid=True,
                )
            elif normalized_side != "SELL":
                raise ValueError("runtime_side_invalid")
            elif tuple(sorted(str(name) for name in predicate_values)) != self._predicate_columns:
                raise ValueError("runtime_predicate_columns_drifted")
            else:
                values = {name: int(predicate_values[name]) for name in self._predicate_columns}
                decision = self._control(
                    baseline_duration_ms=baseline,
                    snapshot_id=snapshot,
                    reason="no_rule_matched",
                    support_valid=True,
                )
                for index, (action, clauses) in enumerate(self._rules):
                    clause_states = []
                    for clause in clauses:
                        clause_states.append(
                            _and_state(
                                tuple(
                                    _literal_state(values[name], negated)
                                    for name, negated in clause
                                )
                            )
                        )
                    state = _or_state(clause_states)
                    if state == -1:
                        decision = self._control(
                            baseline_duration_ms=baseline,
                            snapshot_id=snapshot,
                            reason=f"rule_unobserved:{index}",
                            support_valid=False,
                        )
                        break
                    if state == 1:
                        seconds = int(_DURATION_RE.fullmatch(action).group(1))
                        decision = CooldownDurationDecision(
                            action_id=action,
                            duration_ms=seconds * 1_000,
                            fallback_reason=None,
                            matched_rule_index=index,
                            policy_sha256=self.policy_sha256,
                            predicate_bundle_sha256=self.predicate_bundle_sha256,
                            snapshot_id=snapshot,
                            support_valid=True,
                        )
                        break
        except Exception as exc:
            decision = self._control(
                baseline_duration_ms=max(85_000, baseline),
                snapshot_id=snapshot,
                reason=str(exc),
                support_valid=False,
            )
        with self._lock:
            self._evaluations += 1
            self._supported += int(decision.support_valid)
            self._fallback += int(decision.fallback_reason is not None)
            self._nonbaseline += int(decision.action_id != CONTROL_ACTION)
        return decision


class _SelectedMidEmaState:
    def __init__(self) -> None:
        self.ema: dict[float, float] = {}
        self.last_ts_ns: int | None = None
        self.current_window_observed = False
        self.pairs = {pair: _PairState() for pair in _SELECTED_PAIRS}

    def update(self, *, ts_ns: int, value: float | None, observed: bool) -> None:
        self.current_window_observed = bool(observed)
        if not observed:
            return
        if value is None or not math.isfinite(float(value)):
            raise ValueError("observed_mid_nonfinite")
        timestamp = int(ts_ns)
        current_value = float(value)
        if self.last_ts_ns is None:
            self.ema = {half_life: current_value for half_life in _SELECTED_HALF_LIVES_S}
            self.last_ts_ns = timestamp
            return
        if timestamp <= self.last_ts_ns:
            raise ValueError("mid_ema_clock_must_increase")
        delta_s = (timestamp - self.last_ts_ns) / 1_000_000_000.0
        prior = dict(self.ema)
        for half_life in _SELECTED_HALF_LIVES_S:
            decay = math.exp(-math.log(2.0) * delta_s / half_life)
            self.ema[half_life] = decay * prior[half_life] + (1.0 - decay) * current_value
        for pair in _SELECTED_PAIRS:
            distance = self.ema[pair[0]] - self.ema[pair[1]]
            sign = 1 if distance > 0.0 else -1 if distance < 0.0 else 0
            state = self.pairs[pair]
            if sign and state.effective_sign == 0:
                state.effective_sign = sign
            elif sign and sign != state.effective_sign:
                state.effective_sign = sign
                state.last_cross_ts_ns = timestamp
        self.last_ts_ns = timestamp

    def cross_age_le(self, *, pair: tuple[float, float], threshold_s: float, decision_ts_ns: int) -> int:
        if not self.current_window_observed:
            return -1
        cross_ts = self.pairs[pair].last_cross_ts_ns
        if cross_ts is None:
            return -1
        age_s = (int(decision_ts_ns) - cross_ts) / 1_000_000_000.0
        if not math.isfinite(age_s) or age_s < 0.0:
            return -1
        return int(age_s <= threshold_s)


class ReceiveTimeMidEmaWindows:
    """Finalize 100ms receive-time mid windows with past-only visibility."""

    def __init__(self, *, warmup_s: float, max_feature_age_s: float) -> None:
        if not math.isfinite(float(warmup_s)) or float(warmup_s) <= 0.0:
            raise ValueError("boolean cooldown warmup_s must be positive")
        if not math.isfinite(float(max_feature_age_s)) or float(max_feature_age_s) <= 0.0:
            raise ValueError("boolean cooldown max_feature_age_s must be positive")
        self.warmup_s = float(warmup_s)
        self.max_feature_age_s = float(max_feature_age_s)
        self._lock = threading.RLock()
        self._state = _SelectedMidEmaState()
        self._pending_left_ns: int | None = None
        self._pending_mid: float | None = None
        self._pending_depth_generation = 0
        self._feature_ready_ts_ns = 0
        self._warmup_start_right_ns: int | None = None
        self._warmup_admitted = False
        self._updates = 0
        self._windows = 0
        self._gap_windows = 0
        self._resets = 0
        self._invalid = 0
        self._out_of_order = 0
        self._last_error = ""

    def _reset_locked(self, reason: str) -> None:
        self._state = _SelectedMidEmaState()
        self._pending_left_ns = None
        self._pending_mid = None
        self._pending_depth_generation = 0
        self._feature_ready_ts_ns = 0
        self._warmup_start_right_ns = None
        self._warmup_admitted = False
        self._resets += 1
        self._last_error = str(reason)

    def _emit_locked(
        self,
        *,
        left_ns: int,
        feature_ready_ts_ns: int,
        mid: float | None,
        source_gap: bool,
    ) -> None:
        right_ns = int(left_ns + BASE_WINDOW_WIDTH_NS)
        if not source_gap and self._warmup_start_right_ns is None:
            self._warmup_start_right_ns = right_ns
        self._state.update(
            ts_ns=right_ns,
            value=None if source_gap else float(mid),
            observed=not source_gap,
        )
        self._feature_ready_ts_ns = int(feature_ready_ts_ns)
        self._windows += 1
        self._gap_windows += int(source_gap)
        if self._warmup_start_right_ns is not None:
            elapsed_s = (right_ns - self._warmup_start_right_ns) / 1_000_000_000.0
            self._warmup_admitted = elapsed_s >= self.warmup_s

    def observe_depth(
        self,
        *,
        receive_ts_ns: int,
        bids: Sequence[tuple[float, float]],
        asks: Sequence[tuple[float, float]],
        market_generation: int,
        depth_generation: int,
    ) -> None:
        del market_generation
        try:
            receive_ns = int(receive_ts_ns)
            if receive_ns <= 0 or not bids or not asks:
                raise ValueError("depth_callback_invalid")
            bid = float(bids[0][0])
            ask = float(asks[0][0])
            if not (math.isfinite(bid) and math.isfinite(ask) and 0.0 < bid < ask):
                raise ValueError("depth_callback_bbo_invalid")
            mid = (bid + ask) / 2.0
            left_ns = (receive_ns // BASE_WINDOW_WIDTH_NS) * BASE_WINDOW_WIDTH_NS
            with self._lock:
                self._updates += 1
                pending = self._pending_left_ns
                if pending is None:
                    self._pending_left_ns = left_ns
                    self._pending_mid = mid
                    self._pending_depth_generation = int(depth_generation)
                    return
                if left_ns < pending:
                    self._out_of_order += 1
                    return
                if left_ns == pending:
                    self._pending_mid = mid
                    self._pending_depth_generation = int(depth_generation)
                    return
                gap_windows = max(0, (left_ns - pending) // BASE_WINDOW_WIDTH_NS - 1)
                gap_s = gap_windows * BASE_WINDOW_WIDTH_NS / 1_000_000_000.0
                if gap_s > self.max_feature_age_s:
                    self._reset_locked("depth_gap_exceeded_execution_freshness")
                    self._pending_left_ns = left_ns
                    self._pending_mid = mid
                    self._pending_depth_generation = int(depth_generation)
                    return
                self._emit_locked(
                    left_ns=pending,
                    feature_ready_ts_ns=receive_ns,
                    mid=self._pending_mid,
                    source_gap=False,
                )
                for offset in range(1, int(gap_windows) + 1):
                    self._emit_locked(
                        left_ns=pending + offset * BASE_WINDOW_WIDTH_NS,
                        feature_ready_ts_ns=receive_ns,
                        mid=None,
                        source_gap=True,
                    )
                self._pending_left_ns = left_ns
                self._pending_mid = mid
                self._pending_depth_generation = int(depth_generation)
        except Exception as exc:
            with self._lock:
                self._invalid += 1
                self._last_error = f"{type(exc).__name__}:{exc}"

    def predicate_values(
        self,
        *,
        decision_ts_ns: int,
        campaign_age_s: float,
        baseline_duration_ms: int,
    ) -> tuple[dict[str, int] | None, str | None, int, float]:
        with self._lock:
            feature_ready = self._feature_ready_ts_ns
            age_ms = (
                max(0, int(decision_ts_ns) - feature_ready) / 1_000_000.0
                if feature_ready > 0
                else math.inf
            )
            if feature_ready <= 0:
                return None, "no_completed_receive_time_window", 0, age_ms
            if not self._warmup_admitted:
                return None, "receive_time_ema_warmup_incomplete", feature_ready, age_ms
            if age_ms > self.max_feature_age_s * 1_000.0:
                return None, "receive_time_mid_state_stale", feature_ready, age_ms
            if not self._state.current_window_observed:
                return None, "latest_completed_mid_window_unobserved", feature_ready, age_ms
            values = {
                "predicate::ema_pair_h4s_h16s:cross_age_le_slow": self._state.cross_age_le(
                    pair=(4.0, 16.0), threshold_s=16.0, decision_ts_ns=decision_ts_ns
                ),
                "predicate::ema_pair_h16s_h256s:cross_age_le_fast": self._state.cross_age_le(
                    pair=(16.0, 256.0), threshold_s=16.0, decision_ts_ns=decision_ts_ns
                ),
                "predicate::m0::campaign_age_gt_control_duration": int(
                    float(campaign_age_s) * 1_000.0 > int(baseline_duration_ms)
                ),
            }
            return values, None, feature_ready, age_ms

    def audit(self) -> dict[str, Any]:
        with self._lock:
            return {
                "updates": self._updates,
                "completed_windows": self._windows,
                "gap_windows": self._gap_windows,
                "resets": self._resets,
                "invalid_updates": self._invalid,
                "out_of_order_updates": self._out_of_order,
                "warmup_admitted": int(self._warmup_admitted),
                "feature_ready_ts_ns": self._feature_ready_ts_ns,
                "last_error": self._last_error,
            }


class LiveBooleanCooldownPolicy:
    """Hash-bound active owner policy with per-decision control fallback."""

    def __init__(
        self,
        *,
        evaluator: RuntimeCooldownPolicyEvaluator,
        warmup_s: float,
        max_feature_age_s: float,
    ) -> None:
        if evaluator.predicate_columns != OWNER_POLICY_SELECTED_PREDICATES:
            raise ValueError("boolean cooldown selected predicate family drifted")
        self.evaluator = evaluator
        self.windows = ReceiveTimeMidEmaWindows(
            warmup_s=warmup_s,
            max_feature_age_s=max_feature_age_s,
        )
        self._lock = threading.Lock()
        self._evaluations = 0
        self._supported = 0
        self._nonbaseline = 0
        self._fallback = 0
        self._last_action = CONTROL_ACTION
        self._last_fallback = ""
        self._last_decision_wall_s = 0.0

    @classmethod
    def from_files(
        cls,
        *,
        policy_path: str | Path,
        policy_sha256: str,
        predicate_bundle_path: str | Path,
        predicate_bundle_sha256: str,
        warmup_s: float,
        max_feature_age_s: float,
    ) -> LiveBooleanCooldownPolicy:
        evaluator = RuntimeCooldownPolicyEvaluator.from_files(
            policy_path=policy_path,
            policy_sha256=policy_sha256,
            predicate_bundle_path=predicate_bundle_path,
            predicate_bundle_sha256=predicate_bundle_sha256,
        )
        return cls(
            evaluator=evaluator,
            warmup_s=warmup_s,
            max_feature_age_s=max_feature_age_s,
        )

    def observe_depth(self, **kwargs: Any) -> None:
        self.windows.observe_depth(**kwargs)

    def evaluate(
        self,
        *,
        side: str,
        baseline_duration_ms: int,
        campaign_age_s: float,
        decision_ts_ns: int,
        snapshot_id: str,
    ) -> LiveBooleanCooldownDecision:
        values, reason, feature_ready, age_ms = self.windows.predicate_values(
            decision_ts_ns=int(decision_ts_ns),
            campaign_age_s=float(campaign_age_s),
            baseline_duration_ms=int(baseline_duration_ms),
        )
        if str(side).upper() == "BUY":
            values = {name: -1 for name in OWNER_POLICY_SELECTED_PREDICATES}
            reason = None
        if values is None:
            decision = CooldownDurationDecision(
                action_id=CONTROL_ACTION,
                duration_ms=int(baseline_duration_ms),
                fallback_reason=reason or "receive_time_policy_state_invalid",
                matched_rule_index=None,
                policy_sha256=self.evaluator.policy_sha256,
                predicate_bundle_sha256=self.evaluator.predicate_bundle_sha256,
                snapshot_id=str(snapshot_id),
                support_valid=False,
            )
        else:
            decision = self.evaluator.evaluate_predicates(
                side=str(side),
                predicate_values=values,
                baseline_duration_ms=int(baseline_duration_ms),
                snapshot_id=str(snapshot_id),
            )
        with self._lock:
            self._evaluations += 1
            self._supported += int(decision.support_valid)
            self._nonbaseline += int(decision.action_id != CONTROL_ACTION)
            self._fallback += int(decision.fallback_reason is not None)
            self._last_action = decision.action_id
            self._last_fallback = decision.fallback_reason or ""
            self._last_decision_wall_s = time.time()
        return LiveBooleanCooldownDecision(
            action_id=decision.action_id,
            duration_ms=decision.duration_ms,
            fallback_reason=decision.fallback_reason,
            matched_rule_index=decision.matched_rule_index,
            support_valid=decision.support_valid,
            policy_sha256=decision.policy_sha256,
            predicate_bundle_sha256=decision.predicate_bundle_sha256,
            feature_ready_ts_ns=feature_ready,
            feature_age_ms=age_ms,
        )

    def audit(self) -> dict[str, Any]:
        with self._lock:
            decision_age_s = (
                max(0.0, time.time() - self._last_decision_wall_s)
                if self._last_decision_wall_s > 0.0
                else math.inf
            )
            policy = {
                "enabled": 1,
                "transport_identity": LIVE_FEATURE_TRANSPORT_IDENTITY,
                "evaluations": self._evaluations,
                "supported": self._supported,
                "nonbaseline": self._nonbaseline,
                "fallback": self._fallback,
                "last_action": self._last_action,
                "last_fallback": self._last_fallback,
                "last_decision_age_s": decision_age_s,
                "policy_sha256": self.evaluator.policy_sha256,
                "predicate_bundle_sha256": self.evaluator.predicate_bundle_sha256,
            }
        return {**policy, "windows": self.windows.audit()}


__all__ = [
    "LIVE_FEATURE_TRANSPORT_IDENTITY",
    "LiveBooleanCooldownDecision",
    "LiveBooleanCooldownPolicy",
    "OWNER_POLICY_SELECTED_PREDICATES",
    "ReceiveTimeMidEmaWindows",
    "RuntimeCooldownPolicyEvaluator",
]
