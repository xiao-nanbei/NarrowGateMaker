"""Fail-closed runtime evaluator for the frozen owner cooldown policy.

The evaluator is deliberately independent from the replay engine.  It binds
one owner ``policy.json`` to the outcome-blind 2025 predicate bundle, projects
one :class:`CooldownAssignmentSnapshotV2` into the same M2 predicate view used
by research, and returns a duration decision without exposing exceptions to a
trading loop.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from numbers import Integral, Real
from pathlib import Path
from threading import Lock
from typing import Any

import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    CONTROL_ACTION,
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    NestedOofContractError,
    TriLiteral,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_predicates import (
    IDENTITY as PREDICATE_IDENTITY,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_predicates import (
    PredicateArtifact,
    PredicateContractError,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    CooldownAssignmentSnapshotV2,
)

OWNER_POLICY_IDENTITY = "causal_multichannel_window_boolean_cooldown_owner_policy_v1"
OWNER_POLICY_SCHEMA = f"{OWNER_POLICY_IDENTITY}.artifact.v1"
PREDICATE_BUNDLE_SCHEMA = (
    f"{PREDICATE_IDENTITY}.multiday_label_panel_nested_oof.v1.predicate_bundle.v1"
)
M2_FEATURE_BLOCK = "M2"
MINIMUM_CONTROL_DURATION_MS = 85_000

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FIXED_ACTION_RE = re.compile(r"^FIXED_([1-9][0-9]*)S$")
_LEGACY_MID_PAIR_RE = re.compile(
    r"^predicate::ema_pair_"
    r"(?P<fast>h[0-9]+(?:p[0-9]+)?s)_"
    r"(?P<slow>h[0-9]+(?:p[0-9]+)?s):"
    r"(?P<semantic>cross_age_le_fast|cross_age_le_slow)$"
)


class RuntimePolicyContractError(ValueError):
    """Internal binding or projection failure converted to control fallback."""


@dataclass(frozen=True, slots=True)
class CooldownDurationDecision:
    """One total-duration decision safe for direct replay/runtime consumption."""

    action_id: str
    duration_ms: int
    fallback_reason: str | None
    matched_rule_index: int | None
    policy_sha256: str
    predicate_bundle_sha256: str
    snapshot_id: str
    support_valid: bool


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_file_sha256(path: Path) -> str:
    try:
        return _file_sha256(path)
    except OSError:
        return ""


def _load_json(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimePolicyContractError(f"invalid_json:{path.name}") from exc
    if not isinstance(raw, Mapping):
        raise RuntimePolicyContractError(f"json_root_not_object:{path.name}")
    return dict(raw)


def _verify_canonical_document(raw: Mapping[str, Any], *, label: str) -> str:
    expected = str(raw.get("canonical_sha256", ""))
    body = dict(raw)
    body.pop("canonical_sha256", None)
    if not _SHA256_RE.fullmatch(expected) or _canonical_sha256(body) != expected:
        raise RuntimePolicyContractError(f"{label}_canonical_sha256_mismatch")
    return expected


def _require_sha256(value: Any, *, label: str) -> str:
    normalized = str(value)
    if not _SHA256_RE.fullmatch(normalized):
        raise RuntimePolicyContractError(f"{label}_is_not_sha256")
    return normalized


def _literal_columns(policy_payload: Mapping[str, Any]) -> tuple[str, ...]:
    rules = policy_payload.get("ordered_first_match_rules")
    if not isinstance(rules, Sequence) or isinstance(rules, (str, bytes)) or not rules:
        raise RuntimePolicyContractError("policy_rules_missing")
    names: set[str] = set()
    for rule in rules:
        if not isinstance(rule, Mapping):
            raise RuntimePolicyContractError("policy_rule_not_object")
        clauses = rule.get("clauses")
        if not isinstance(clauses, Sequence) or isinstance(clauses, (str, bytes)):
            raise RuntimePolicyContractError("policy_clauses_missing")
        for clause in clauses:
            if not isinstance(clause, Mapping):
                raise RuntimePolicyContractError("policy_clause_not_object")
            literals = clause.get("literals")
            if not isinstance(literals, Sequence) or isinstance(literals, (str, bytes)):
                raise RuntimePolicyContractError("policy_literals_missing")
            for literal in literals:
                if not isinstance(literal, Mapping):
                    raise RuntimePolicyContractError("policy_literal_not_object")
                name = str(literal.get("predicate", "")).strip()
                if not name:
                    raise RuntimePolicyContractError("policy_literal_name_missing")
                names.add(name)
    derived = tuple(sorted(names))
    declared = policy_payload.get("predicate_columns")
    if declared not in (None, []):
        if not isinstance(declared, Sequence) or isinstance(declared, (str, bytes)):
            raise RuntimePolicyContractError("policy_predicate_columns_invalid")
        if tuple(str(value) for value in declared) != derived:
            raise RuntimePolicyContractError("policy_predicate_columns_drifted")
    return derived


def _parse_policy(policy_payload: Mapping[str, Any]) -> tuple[BooleanCooldownPolicy, tuple[str, ...]]:
    predicate_columns = _literal_columns(policy_payload)
    parsed_rules: list[BooleanRule] = []
    try:
        for raw_rule in policy_payload["ordered_first_match_rules"]:
            clauses: list[AndClause] = []
            for raw_clause in raw_rule["clauses"]:
                literals = tuple(
                    TriLiteral(
                        predicate=str(raw_literal["predicate"]),
                        negated=bool(raw_literal.get("negated", False)),
                    )
                    for raw_literal in raw_clause["literals"]
                )
                clauses.append(AndClause(literals=literals))
            parsed_rules.append(
                BooleanRule(action=str(raw_rule["action"]), clauses=tuple(clauses))
            )
        policy = BooleanCooldownPolicy(
            side=str(policy_payload["side"]),
            rules=tuple(parsed_rules),
            default_action=str(policy_payload.get("default_action", "")),
        )
    except (KeyError, TypeError, ValueError, NestedOofContractError) as exc:
        raise RuntimePolicyContractError("boolean_policy_payload_invalid") from exc
    if policy.side != "SELL" or policy.default_action != CONTROL_ACTION:
        raise RuntimePolicyContractError("owner_policy_side_or_default_drifted")
    if policy.predicate_columns != predicate_columns:
        raise RuntimePolicyContractError("derived_predicate_columns_drifted")
    return policy, predicate_columns


def _artifact_group(artifact: PredicateArtifact) -> str:
    groups = {
        definition.clock_group
        for definition in artifact.definitions
        if definition.clock_group in {"book", "trade"}
    }
    if len(groups) != 1:
        raise RuntimePolicyContractError("predicate_artifact_clock_group_invalid")
    return next(iter(groups))


def _load_artifacts(
    *,
    bundle_path: Path,
    bundle: Mapping[str, Any],
    policy_binding: Mapping[str, Any],
) -> dict[str, PredicateArtifact]:
    root = bundle_path.parent.resolve()
    bound_artifacts = policy_binding.get("artifacts")
    if not isinstance(bound_artifacts, Mapping):
        raise RuntimePolicyContractError("policy_predicate_artifact_bindings_missing")
    artifacts: dict[str, PredicateArtifact] = {}
    for group in ("book", "trade"):
        group_rows = bundle.get(group)
        if not isinstance(group_rows, Mapping) or set(group_rows) != {"BUY", "SELL"}:
            raise RuntimePolicyContractError(f"predicate_bundle_{group}_sides_invalid")
        for side in ("BUY", "SELL"):
            entry = group_rows[side]
            if not isinstance(entry, Mapping):
                raise RuntimePolicyContractError("predicate_artifact_entry_invalid")
            relative = Path(str(entry.get("path", "")))
            if relative.is_absolute():
                raise RuntimePolicyContractError("predicate_artifact_path_not_relative")
            path = (root / relative).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                raise RuntimePolicyContractError("predicate_artifact_path_invalid")
            expected_file_sha = _require_sha256(
                entry.get("sha256"), label=f"{group}_{side}_file_sha256"
            )
            actual_file_sha = _file_sha256(path)
            if actual_file_sha != expected_file_sha:
                raise RuntimePolicyContractError("predicate_artifact_file_sha256_mismatch")
            try:
                artifact = PredicateArtifact.from_json(path.read_text(encoding="utf-8"))
            except (OSError, PredicateContractError) as exc:
                raise RuntimePolicyContractError("predicate_artifact_payload_invalid") from exc
            if artifact.side != side or _artifact_group(artifact) != group:
                raise RuntimePolicyContractError("predicate_artifact_side_or_clock_drifted")
            if (
                artifact.source_role != "outcome_blind_2025_single_channel"
                or not artifact.clock_separated_2025
                or not artifact.reference_days
                or any(not day.startswith("2025-") for day in artifact.reference_days)
            ):
                raise RuntimePolicyContractError("predicate_artifact_2025_identity_drifted")
            key = f"{group}.{side}"
            bound = bound_artifacts.get(key)
            if not isinstance(bound, Mapping):
                raise RuntimePolicyContractError("policy_predicate_artifact_binding_missing")
            if (
                str(bound.get("file_sha256")) != actual_file_sha
                or str(bound.get("canonical_sha256")) != artifact.canonical_sha256
                or str(bound.get("reference_identity_sha256"))
                != artifact.reference_identity_sha256
            ):
                raise RuntimePolicyContractError("policy_predicate_artifact_binding_drifted")
            artifacts[key] = artifact
    return artifacts


def _load_bound_runtime(
    *,
    policy_path: Path,
    predicate_bundle_path: Path,
    expected_policy_sha256: str | None,
    expected_predicate_bundle_sha256: str | None,
) -> tuple[
    BooleanCooldownPolicy,
    tuple[str, ...],
    dict[str, PredicateArtifact],
    str,
    str,
]:
    policy_sha = _file_sha256(policy_path)
    if expected_policy_sha256 is not None:
        if _require_sha256(expected_policy_sha256, label="expected_policy_sha256") != policy_sha:
            raise RuntimePolicyContractError("policy_file_sha256_mismatch")
    raw_policy = _load_json(policy_path)
    _verify_canonical_document(raw_policy, label="policy")
    if (
        raw_policy.get("identity") != OWNER_POLICY_IDENTITY
        or raw_policy.get("schema_version") != OWNER_POLICY_SCHEMA
        or raw_policy.get("selection", {}).get("BUY") != CONTROL_ACTION
    ):
        raise RuntimePolicyContractError("owner_policy_identity_drifted")
    raw_boolean = raw_policy.get("policy")
    if not isinstance(raw_boolean, Mapping):
        raise RuntimePolicyContractError("owner_boolean_policy_missing")
    policy, predicate_columns = _parse_policy(raw_boolean)

    bindings = raw_policy.get("bindings")
    panel = bindings.get("panel") if isinstance(bindings, Mapping) else None
    policy_predicates = (
        panel.get("outcome_blind_2025_predicates")
        if isinstance(panel, Mapping)
        else None
    )
    if not isinstance(policy_predicates, Mapping):
        raise RuntimePolicyContractError("owner_predicate_binding_missing")
    bound_bundle = policy_predicates.get("bundle")
    if not isinstance(bound_bundle, Mapping):
        raise RuntimePolicyContractError("owner_predicate_bundle_binding_missing")

    bundle_sha = _file_sha256(predicate_bundle_path)
    if expected_predicate_bundle_sha256 is not None:
        expected = _require_sha256(
            expected_predicate_bundle_sha256,
            label="expected_predicate_bundle_sha256",
        )
        if expected != bundle_sha:
            raise RuntimePolicyContractError("predicate_bundle_file_sha256_mismatch")
    if str(bound_bundle.get("file_sha256")) != bundle_sha:
        raise RuntimePolicyContractError("policy_predicate_bundle_file_binding_drifted")
    bundle = _load_json(predicate_bundle_path)
    bundle_canonical = _verify_canonical_document(bundle, label="predicate_bundle")
    if (
        bundle.get("identity") != PREDICATE_IDENTITY
        or bundle.get("schema_version") != PREDICATE_BUNDLE_SCHEMA
        or bundle.get("m0_artifacts") != []
        or bundle.get("cross_clock_clause_authorized") is not False
    ):
        raise RuntimePolicyContractError("predicate_bundle_identity_drifted")
    strict_target = bundle.get("strict_2026_target_snapshot")
    if (
        not isinstance(strict_target, Mapping)
        or strict_target.get("book_trade_predicates_may_be_combined_by_study") is not True
    ):
        raise RuntimePolicyContractError("predicate_bundle_target_join_semantics_missing")
    if str(bound_bundle.get("canonical_sha256")) != bundle_canonical:
        raise RuntimePolicyContractError("policy_predicate_bundle_canonical_binding_drifted")
    artifacts = _load_artifacts(
        bundle_path=predicate_bundle_path,
        bundle=bundle,
        policy_binding=policy_predicates,
    )
    return policy, predicate_columns, artifacts, policy_sha, bundle_sha


def _normalize_baseline_duration(value: Any) -> tuple[int, str | None]:
    if isinstance(value, bool) or not isinstance(value, Real):
        return MINIMUM_CONTROL_DURATION_MS, "baseline_duration_ms_invalid"
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0.0:
        return MINIMUM_CONTROL_DURATION_MS, "baseline_duration_ms_invalid"
    rounded = int(round(parsed))
    if not math.isclose(parsed, float(rounded), rel_tol=0.0, abs_tol=1e-6):
        return MINIMUM_CONTROL_DURATION_MS, "baseline_duration_ms_not_integral"
    return rounded, None


def _frozen_mapping(value: Any, *, label: str) -> dict[str, Any]:
    if hasattr(value, "to_dict"):
        raw = value.to_dict()
    elif isinstance(value, Mapping):
        raw = dict(value)
    else:
        raise RuntimePolicyContractError(f"{label}_not_mapping")
    if not isinstance(raw, Mapping):
        raise RuntimePolicyContractError(f"{label}_not_mapping")
    return dict(raw)


def _tri_value(value: Any, *, predicate: str) -> np.int8:
    if isinstance(value, bool):
        return np.int8(1 if value else 0)
    if isinstance(value, Integral) and int(value) in {-1, 0, 1}:
        return np.int8(int(value))
    raise RuntimePolicyContractError(f"predicate_not_three_valued:{predicate}")


def _half_life_value(label: str) -> float:
    try:
        value = float(label.removeprefix("h").removesuffix("s").replace("p", "."))
    except ValueError as exc:
        raise RuntimePolicyContractError("invalid_ema_half_life_label") from exc
    if not math.isfinite(value) or value <= 0.0:
        raise RuntimePolicyContractError("invalid_ema_half_life_label")
    return value


def _legacy_mid_pair_predicate(name: str, feature_row: Mapping[str, Any]) -> np.int8:
    match = _LEGACY_MID_PAIR_RE.fullmatch(name)
    if match is None:
        raise RuntimePolicyContractError(f"unsupported_policy_predicate:{name}")
    fast_label = match.group("fast")
    slow_label = match.group("slow")
    fast = _half_life_value(fast_label)
    slow = _half_life_value(slow_label)
    if fast >= slow:
        raise RuntimePolicyContractError("invalid_ema_pair_order")
    observed_column = "channel::mid_usdc_per_btc::observed"
    if observed_column not in feature_row:
        raise RuntimePolicyContractError(f"missing_feature_column:{observed_column}")
    observed = _tri_value(feature_row[observed_column], predicate=observed_column)
    if observed != 1:
        return np.int8(-1)
    cross_age_column = (
        f"value::mid_usdc_per_btc__{fast_label}__{slow_label}::cross_age_s"
    )
    if cross_age_column not in feature_row:
        raise RuntimePolicyContractError(f"missing_feature_column:{cross_age_column}")
    raw_age = feature_row[cross_age_column]
    if raw_age is None:
        return np.int8(-1)
    if isinstance(raw_age, bool) or not isinstance(raw_age, Real):
        raise RuntimePolicyContractError(f"invalid_feature_value:{cross_age_column}")
    age = float(raw_age)
    if not math.isfinite(age) or age < 0.0:
        return np.int8(-1)
    threshold = fast if match.group("semantic") == "cross_age_le_fast" else slow
    return np.int8(1 if age <= threshold else 0)


def _direct_predicate(
    name: str,
    *,
    feature_row: Mapping[str, Any],
    baseline_duration_ms: int,
) -> np.int8:
    if name == "predicate::m0::campaign_age_gt_control_duration":
        if "campaign_age_s" not in feature_row:
            raise RuntimePolicyContractError("missing_feature_column:campaign_age_s")
        raw_age = feature_row["campaign_age_s"]
        if raw_age is None:
            return np.int8(-1)
        if isinstance(raw_age, bool) or not isinstance(raw_age, Real):
            raise RuntimePolicyContractError("invalid_feature_value:campaign_age_s")
        age = float(raw_age)
        if not math.isfinite(age) or age < 0.0:
            return np.int8(-1)
        return np.int8(1 if age * 1_000.0 > baseline_duration_ms else 0)
    if name.startswith("predicate::ema_pair_"):
        return _legacy_mid_pair_predicate(name, feature_row)
    if name in feature_row:
        return _tri_value(feature_row[name], predicate=name)
    raise RuntimePolicyContractError(f"missing_policy_predicate:{name}")


def _utc_day(feature_row: Mapping[str, Any]) -> str:
    raw = feature_row.get("decision_ts_ns")
    if isinstance(raw, bool) or not isinstance(raw, Integral) or int(raw) <= 0:
        raise RuntimePolicyContractError("decision_ts_ns_invalid")
    return datetime.fromtimestamp(int(raw) / 1_000_000_000.0, tz=timezone.utc).strftime(
        "%Y-%m-%d"
    )


def _artifact_input_frame(
    feature_row: Mapping[str, Any], artifact: PredicateArtifact
) -> pd.DataFrame:
    derived_day = _utc_day(feature_row)
    columns: dict[str, pd.Series] = {}
    for name, family in artifact.input_schema:
        if name == "utc_day":
            value: Any = derived_day
        elif name in feature_row:
            value = feature_row[name]
        else:
            raise RuntimePolicyContractError(f"missing_feature_column:{name}")
        if family == "numeric":
            if value is None:
                numeric = float("nan")
            elif isinstance(value, bool) or not isinstance(value, Real):
                raise RuntimePolicyContractError(f"invalid_numeric_feature:{name}")
            else:
                numeric = float(value)
            columns[name] = pd.Series([numeric], dtype=np.float64)
        elif family == "bool":
            if value is not None and not isinstance(value, (bool, np.bool_)):
                raise RuntimePolicyContractError(f"invalid_bool_feature:{name}")
            columns[name] = pd.Series([value], dtype="boolean")
        elif family == "text":
            if value is not None and not isinstance(value, str):
                raise RuntimePolicyContractError(f"invalid_text_feature:{name}")
            columns[name] = pd.Series([value], dtype=object)
        else:
            raise RuntimePolicyContractError(f"unknown_artifact_dtype_family:{family}")
    return pd.DataFrame(columns)


def _transform_selected_artifact_predicates(
    *,
    artifact: PredicateArtifact,
    feature_row: Mapping[str, Any],
    required: set[str],
) -> pd.DataFrame:
    definition_names = {definition.name for definition in artifact.definitions}
    selected = tuple(sorted(required & definition_names))
    if not selected:
        return pd.DataFrame(index=pd.RangeIndex(1))
    try:
        transformed = artifact.transform(
            _artifact_input_frame(feature_row, artifact),
            expected_artifact_sha256=artifact.canonical_sha256,
        )
    except PredicateContractError as exc:
        raise RuntimePolicyContractError("predicate_artifact_transform_failed") from exc
    return transformed.columns.loc[:, list(selected)].copy()


def _combine_m2_predicate_view(
    *,
    policy_columns: Sequence[str],
    artifacts: Mapping[str, PredicateArtifact],
    feature_row: Mapping[str, Any],
    baseline_duration_ms: int,
) -> pd.DataFrame:
    required = set(policy_columns)
    book_artifact = artifacts["book.SELL"]
    trade_artifact = artifacts["trade.SELL"]
    book = _transform_selected_artifact_predicates(
        artifact=book_artifact,
        feature_row=feature_row,
        required=required,
    )
    trade = _transform_selected_artifact_predicates(
        artifact=trade_artifact,
        feature_row=feature_row,
        required=required,
    )
    artifact_columns = set(book) | set(trade)
    direct_names = tuple(sorted(required - artifact_columns))
    direct = pd.DataFrame(
        {
            name: pd.Series(
                [
                    _direct_predicate(
                        name,
                        feature_row=feature_row,
                        baseline_duration_ms=baseline_duration_ms,
                    )
                ],
                dtype=np.int8,
            )
            for name in direct_names
        }
    )
    collisions = (set(book) & set(trade)) | (set(book) & set(direct)) | (
        set(trade) & set(direct)
    )
    if collisions:
        raise RuntimePolicyContractError(
            f"predicate_name_collision:{','.join(sorted(collisions))}"
        )
    combined = pd.concat([book, trade, direct], axis=1)
    missing = sorted(required - set(combined))
    if missing:
        raise RuntimePolicyContractError(f"missing_policy_predicates:{missing}")
    return combined.loc[:, list(policy_columns)].astype(np.int8)


def _duration_for_action(action: str, baseline_duration_ms: int) -> int:
    if action == CONTROL_ACTION:
        return baseline_duration_ms
    match = _FIXED_ACTION_RE.fullmatch(action)
    if match is None:
        raise RuntimePolicyContractError(f"unsupported_duration_action:{action}")
    seconds = int(match.group(1))
    duration = seconds * 1_000
    if duration <= 0:
        raise RuntimePolicyContractError("fixed_duration_overflow")
    return duration


@dataclass(slots=True)
class RuntimeCooldownPolicyEvaluator:
    """Loaded runtime policy whose public evaluator never raises."""

    policy_sha256: str
    predicate_bundle_sha256: str
    _policy: BooleanCooldownPolicy | None
    _predicate_columns: tuple[str, ...]
    _artifacts: Mapping[str, PredicateArtifact]
    _binding_error: str | None
    _evaluations: int = 0
    _supported: int = 0
    _fallback: int = 0
    _nonbaseline: int = 0
    _action_counts: dict[str, int] = field(default_factory=dict)
    _duration_ms_sum: int = 0
    _duration_ms_max: int = 0
    _audit_lock: Any = field(default_factory=Lock, repr=False)

    @classmethod
    def from_files(
        cls,
        *,
        policy_path: str | Path,
        predicate_bundle_path: str | Path,
        expected_policy_sha256: str | None = None,
        expected_predicate_bundle_sha256: str | None = None,
    ) -> RuntimeCooldownPolicyEvaluator:
        resolved_policy = Path(policy_path).expanduser().resolve()
        resolved_bundle = Path(predicate_bundle_path).expanduser().resolve()
        policy_sha = _safe_file_sha256(resolved_policy)
        bundle_sha = _safe_file_sha256(resolved_bundle)
        try:
            (
                policy,
                predicate_columns,
                artifacts,
                policy_sha,
                bundle_sha,
            ) = _load_bound_runtime(
                policy_path=resolved_policy,
                predicate_bundle_path=resolved_bundle,
                expected_policy_sha256=expected_policy_sha256,
                expected_predicate_bundle_sha256=expected_predicate_bundle_sha256,
            )
        except Exception as exc:  # Runtime boundary: convert every load fault to control.
            reason = (
                str(exc)
                if isinstance(exc, RuntimePolicyContractError)
                else f"unexpected_binding_error:{type(exc).__name__}"
            )
            return cls(
                policy_sha256=policy_sha,
                predicate_bundle_sha256=bundle_sha,
                _policy=None,
                _predicate_columns=(),
                _artifacts={},
                _binding_error=reason,
            )
        return cls(
            policy_sha256=policy_sha,
            predicate_bundle_sha256=bundle_sha,
            _policy=policy,
            _predicate_columns=predicate_columns,
            _artifacts=artifacts,
            _binding_error=None,
        )

    def _control(
        self,
        *,
        baseline_duration_ms: int,
        snapshot_id: str,
        reason: str,
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

    def _evaluate_once(
        self,
        snapshot: CooldownAssignmentSnapshotV2,
        baseline_duration_ms: int | float,
    ) -> CooldownDurationDecision:
        """Evaluate one assignment and fail closed to its baseline duration."""

        snapshot_id = str(getattr(snapshot, "snapshot_id", ""))
        baseline, baseline_error = _normalize_baseline_duration(baseline_duration_ms)
        if baseline_error is not None:
            return self._control(
                baseline_duration_ms=baseline,
                snapshot_id=snapshot_id,
                reason=baseline_error,
                support_valid=False,
            )
        if self._binding_error is not None or self._policy is None:
            return self._control(
                baseline_duration_ms=baseline,
                snapshot_id=snapshot_id,
                reason=f"runtime_binding_invalid:{self._binding_error or 'unknown'}",
                support_valid=False,
            )
        try:
            if not isinstance(snapshot, CooldownAssignmentSnapshotV2):
                raise RuntimePolicyContractError("snapshot_type_invalid")
            if not snapshot.policy_input_valid or snapshot.policy_input is None:
                reason = snapshot.fallback_reason or "snapshot_policy_input_invalid"
                raise RuntimePolicyContractError(f"snapshot_invalid:{reason}")
            if snapshot.policy_input.snapshot_id != snapshot.snapshot_id:
                raise RuntimePolicyContractError("snapshot_policy_input_id_drifted")
            if snapshot.feature_block != M2_FEATURE_BLOCK:
                raise RuntimePolicyContractError("snapshot_feature_block_not_m2")
            feature_row = _frozen_mapping(snapshot.feature_row, label="feature_row")
            m0 = _frozen_mapping(snapshot.m0_context, label="m0_context")
            policy_feature_row = _frozen_mapping(
                snapshot.policy_input.feature_row,
                label="policy_input_feature_row",
            )
            if feature_row != policy_feature_row:
                raise RuntimePolicyContractError("snapshot_policy_feature_row_drifted")
            feature_side = str(feature_row.get("side", "")).upper()
            m0_side = str(m0.get("side", "")).upper()
            if feature_side not in {"BUY", "SELL"} or feature_side != m0_side:
                raise RuntimePolicyContractError("snapshot_side_inconsistent")
            frozen_baseline, frozen_error = _normalize_baseline_duration(
                feature_row.get("baseline_duration_ms")
            )
            if frozen_error is not None or frozen_baseline != baseline:
                raise RuntimePolicyContractError("snapshot_baseline_duration_drifted")
            if (
                feature_row.get("feature_block") != M2_FEATURE_BLOCK
                or feature_row.get("support_valid") is not True
                or feature_row.get("channel_support_valid") is not True
            ):
                raise RuntimePolicyContractError("snapshot_m2_support_invalid")
            if feature_side == "BUY":
                return self._control(
                    baseline_duration_ms=baseline,
                    snapshot_id=snapshot_id,
                    reason="buy_control_by_contract",
                    support_valid=True,
                )

            predicates = _combine_m2_predicate_view(
                policy_columns=self._predicate_columns,
                artifacts=self._artifacts,
                feature_row=feature_row,
                baseline_duration_ms=baseline,
            )
            chosen = str(self._policy.choose(predicates)[0])
            matched_index: int | None = None
            blocked_index: int | None = None
            for index, rule in enumerate(self._policy.rules):
                state = int(rule.evaluate(predicates)[0])
                if state == 1:
                    matched_index = index
                    if chosen != rule.action:
                        raise RuntimePolicyContractError("boolean_first_match_drifted")
                    break
                if state == -1:
                    blocked_index = index
                    if chosen != CONTROL_ACTION:
                        raise RuntimePolicyContractError("boolean_unobserved_drifted")
                    break
            if blocked_index is not None:
                return self._control(
                    baseline_duration_ms=baseline,
                    snapshot_id=snapshot_id,
                    reason=f"rule_unobserved:{blocked_index}",
                    support_valid=False,
                )
            if matched_index is None:
                if chosen != CONTROL_ACTION:
                    raise RuntimePolicyContractError("boolean_default_action_drifted")
                return self._control(
                    baseline_duration_ms=baseline,
                    snapshot_id=snapshot_id,
                    reason="no_rule_matched",
                    support_valid=True,
                )
            return CooldownDurationDecision(
                action_id=chosen,
                duration_ms=_duration_for_action(chosen, baseline),
                fallback_reason=None,
                matched_rule_index=matched_index,
                policy_sha256=self.policy_sha256,
                predicate_bundle_sha256=self.predicate_bundle_sha256,
                snapshot_id=snapshot_id,
                support_valid=True,
            )
        except Exception as exc:  # Trading-loop boundary: never propagate research faults.
            reason = (
                str(exc)
                if isinstance(exc, RuntimePolicyContractError)
                else f"runtime_evaluation_error:{type(exc).__name__}"
            )
            return self._control(
                baseline_duration_ms=baseline,
                snapshot_id=snapshot_id,
                reason=reason,
                support_valid=False,
            )

    @property
    def binding_valid(self) -> bool:
        """Whether every frozen policy and predicate dependency is bound."""

        return self._binding_error is None and self._policy is not None

    @property
    def binding_error(self) -> str | None:
        """Return the startup binding fault without exposing mutable internals."""

        return self._binding_error

    @property
    def predicate_columns(self) -> tuple[str, ...]:
        """Exact three-valued predicate family consumed by the frozen policy."""

        return self._predicate_columns

    def _evaluate_predicates_once(
        self,
        *,
        side: str,
        predicate_values: Mapping[str, Any],
        baseline_duration_ms: int | float,
        snapshot_id: str,
    ) -> CooldownDurationDecision:
        """Evaluate an already projected, exact three-valued policy row.

        This is the production bridge for a policy whose selected literals are
        computed from the same causal state machine as replay.  It deliberately
        accepts only the policy's complete, exact predicate set; callers cannot
        omit a selected literal or add an unbound runtime feature.
        """

        baseline, baseline_error = _normalize_baseline_duration(
            baseline_duration_ms
        )
        normalized_snapshot_id = str(snapshot_id).strip()
        if not normalized_snapshot_id:
            normalized_snapshot_id = "runtime-predicate-row"
        if baseline_error is not None:
            return self._control(
                baseline_duration_ms=baseline,
                snapshot_id=normalized_snapshot_id,
                reason=baseline_error,
                support_valid=False,
            )
        if self._binding_error is not None or self._policy is None:
            return self._control(
                baseline_duration_ms=baseline,
                snapshot_id=normalized_snapshot_id,
                reason=f"runtime_binding_invalid:{self._binding_error or 'unknown'}",
                support_valid=False,
            )
        try:
            normalized_side = str(side).strip().upper()
            if normalized_side not in {"BUY", "SELL"}:
                raise RuntimePolicyContractError("runtime_side_invalid")
            if normalized_side == "BUY":
                return self._control(
                    baseline_duration_ms=baseline,
                    snapshot_id=normalized_snapshot_id,
                    reason="buy_control_by_contract",
                    support_valid=True,
                )
            actual_columns = tuple(sorted(str(name) for name in predicate_values))
            if actual_columns != self._predicate_columns:
                raise RuntimePolicyContractError(
                    "runtime_predicate_columns_drifted"
                )
            predicates = pd.DataFrame(
                {
                    name: pd.Series(
                        [_tri_value(predicate_values[name], predicate=name)],
                        dtype=np.int8,
                    )
                    for name in self._predicate_columns
                }
            )
            chosen = str(self._policy.choose(predicates)[0])
            matched_index: int | None = None
            blocked_index: int | None = None
            for index, rule in enumerate(self._policy.rules):
                state = int(rule.evaluate(predicates)[0])
                if state == 1:
                    matched_index = index
                    if chosen != rule.action:
                        raise RuntimePolicyContractError(
                            "boolean_first_match_drifted"
                        )
                    break
                if state == -1:
                    blocked_index = index
                    if chosen != CONTROL_ACTION:
                        raise RuntimePolicyContractError(
                            "boolean_unobserved_drifted"
                        )
                    break
            if blocked_index is not None:
                return self._control(
                    baseline_duration_ms=baseline,
                    snapshot_id=normalized_snapshot_id,
                    reason=f"rule_unobserved:{blocked_index}",
                    support_valid=False,
                )
            if matched_index is None:
                if chosen != CONTROL_ACTION:
                    raise RuntimePolicyContractError(
                        "boolean_default_action_drifted"
                    )
                return self._control(
                    baseline_duration_ms=baseline,
                    snapshot_id=normalized_snapshot_id,
                    reason="no_rule_matched",
                    support_valid=True,
                )
            return CooldownDurationDecision(
                action_id=chosen,
                duration_ms=_duration_for_action(chosen, baseline),
                fallback_reason=None,
                matched_rule_index=matched_index,
                policy_sha256=self.policy_sha256,
                predicate_bundle_sha256=self.predicate_bundle_sha256,
                snapshot_id=normalized_snapshot_id,
                support_valid=True,
            )
        except Exception as exc:
            reason = (
                str(exc)
                if isinstance(exc, RuntimePolicyContractError)
                else f"runtime_evaluation_error:{type(exc).__name__}"
            )
            return self._control(
                baseline_duration_ms=baseline,
                snapshot_id=normalized_snapshot_id,
                reason=reason,
                support_valid=False,
            )

    def _record(self, decision: CooldownDurationDecision) -> None:
        with self._audit_lock:
            self._evaluations += 1
            self._supported += int(decision.support_valid)
            self._fallback += int(decision.fallback_reason is not None)
            self._nonbaseline += int(decision.action_id != CONTROL_ACTION)
            self._action_counts[decision.action_id] = (
                self._action_counts.get(decision.action_id, 0) + 1
            )
            self._duration_ms_sum += int(decision.duration_ms)
            self._duration_ms_max = max(
                self._duration_ms_max,
                int(decision.duration_ms),
            )

    def evaluate(
        self,
        snapshot: CooldownAssignmentSnapshotV2,
        baseline_duration_ms: int | float,
    ) -> CooldownDurationDecision:
        """Evaluate and account for one exposure-fill assignment."""

        decision = self._evaluate_once(snapshot, baseline_duration_ms)
        self._record(decision)
        return decision

    def evaluate_predicates(
        self,
        *,
        side: str,
        predicate_values: Mapping[str, Any],
        baseline_duration_ms: int | float,
        snapshot_id: str,
    ) -> CooldownDurationDecision:
        """Evaluate one exact runtime predicate row and account for it."""

        decision = self._evaluate_predicates_once(
            side=side,
            predicate_values=predicate_values,
            baseline_duration_ms=baseline_duration_ms,
            snapshot_id=snapshot_id,
        )
        self._record(decision)
        return decision

    def audit(self) -> dict[str, int | dict[str, int]]:
        """Return a JSON-safe cumulative runtime audit snapshot."""

        with self._audit_lock:
            return {
                "evaluations": int(self._evaluations),
                "supported": int(self._supported),
                "fallback": int(self._fallback),
                "nonbaseline": int(self._nonbaseline),
                "action_counts": {
                    str(action): int(count)
                    for action, count in sorted(self._action_counts.items())
                },
                "duration_ms_sum": int(self._duration_ms_sum),
                "duration_ms_max": int(self._duration_ms_max),
            }


# Compatibility alias for the first local bridge draft.  New integrations use
# RuntimeCooldownPolicyEvaluator and load_runtime_policy_evaluator below.
CooldownRuntimePolicyEvaluator = RuntimeCooldownPolicyEvaluator


def _invalid_evaluator(
    *,
    policy_sha256: str,
    predicate_bundle_sha256: str,
    reason: str,
) -> RuntimeCooldownPolicyEvaluator:
    return RuntimeCooldownPolicyEvaluator(
        policy_sha256=policy_sha256,
        predicate_bundle_sha256=predicate_bundle_sha256,
        _policy=None,
        _predicate_columns=(),
        _artifacts={},
        _binding_error=reason,
    )


def _bound_predicate_bundle_path(
    policy_path: Path,
    policy_payload: Mapping[str, Any],
) -> Path:
    bindings = policy_payload.get("bindings")
    panel = bindings.get("panel") if isinstance(bindings, Mapping) else None
    predicates = (
        panel.get("outcome_blind_2025_predicates")
        if isinstance(panel, Mapping)
        else None
    )
    bundle = predicates.get("bundle") if isinstance(predicates, Mapping) else None
    raw_path = bundle.get("path") if isinstance(bundle, Mapping) else None
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise RuntimePolicyContractError("owner_predicate_bundle_path_missing")
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = policy_path.parent / candidate
    return candidate.resolve()


def load_runtime_policy_evaluator(
    policy_path: Path,
    *,
    expected_policy_sha256: str,
) -> RuntimeCooldownPolicyEvaluator:
    """Load the policy and every predicate dependency from its own bindings."""

    resolved_policy = Path(policy_path).expanduser().resolve()
    policy_sha = _safe_file_sha256(resolved_policy)
    try:
        expected = _require_sha256(
            expected_policy_sha256,
            label="expected_policy_sha256",
        )
        if policy_sha != expected:
            raise RuntimePolicyContractError("policy_file_sha256_mismatch")
        raw = _load_json(resolved_policy)
        bundle_path = _bound_predicate_bundle_path(resolved_policy, raw)
    except Exception as exc:
        reason = (
            str(exc)
            if isinstance(exc, RuntimePolicyContractError)
            else f"unexpected_binding_error:{type(exc).__name__}"
        )
        return _invalid_evaluator(
            policy_sha256=policy_sha,
            predicate_bundle_sha256="",
            reason=reason,
        )
    return RuntimeCooldownPolicyEvaluator.from_files(
        policy_path=resolved_policy,
        predicate_bundle_path=bundle_path,
        expected_policy_sha256=expected,
    )


def load_runtime_policy(
    *,
    policy_path: str | Path,
    predicate_bundle_path: str | Path,
    expected_policy_sha256: str | None = None,
    expected_predicate_bundle_sha256: str | None = None,
) -> RuntimeCooldownPolicyEvaluator:
    """Load and hash-bind a reusable evaluator without raising on bad inputs."""

    return RuntimeCooldownPolicyEvaluator.from_files(
        policy_path=policy_path,
        predicate_bundle_path=predicate_bundle_path,
        expected_policy_sha256=expected_policy_sha256,
        expected_predicate_bundle_sha256=expected_predicate_bundle_sha256,
    )


__all__ = [
    "CooldownDurationDecision",
    "CooldownRuntimePolicyEvaluator",
    "RuntimeCooldownPolicyEvaluator",
    "load_runtime_policy",
    "load_runtime_policy_evaluator",
]
