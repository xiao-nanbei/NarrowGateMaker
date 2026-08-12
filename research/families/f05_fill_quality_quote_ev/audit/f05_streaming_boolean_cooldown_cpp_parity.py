"""Isolated C++ parity harness for the F05 streaming Boolean cooldown policy.

The harness does not link the replay engine or mutate runtime-bound files. It
keeps the original frozen-predicate evaluator and adds the minimum sufficient
streaming closure for the owner SELL M2 policy: canonical mid EMA half-lives
4/16/256 seconds, two cross-age states, one M0 campaign-age predicate, warmup,
gaps, and checkpoint/restart. The comparison authority remains the existing
Python ``CausalMultichannelEmaState`` and runtime predicate materializer.

This is deliberately not an all-M2 parity claim. The frozen policy's eight
literal occurrences reference only three unique predicates; unrelated M2
channels, quantile artifacts, and velocity/acceleration features stay outside
this isolated executable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_features as feature_engine,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_runtime_policy as runtime_policy,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    CONTROL_ACTION,
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    NestedOofContractError,
    TriLiteral,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    CooldownAssignmentSnapshotV2,
)

PROTOCOL_IDENTITY = "F05_STREAMING_BOOLEAN_COOLDOWN_PARITY_V2"
CPP_DIR = Path(__file__).resolve().parents[1] / "cpp"
CPP_SOURCE = CPP_DIR / "f05_streaming_boolean_cooldown_parity.cpp"
CPP_HEADER = CPP_DIR / "f05_streaming_boolean_cooldown_parity.hpp"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FIXED_ACTION_RE = re.compile(r"^FIXED_([1-9][0-9]*)S$")
SHORT_CROSS_PREDICATE = "predicate::ema_pair_h4s_h16s:cross_age_le_slow"
LONG_CROSS_PREDICATE = "predicate::ema_pair_h16s_h256s:cross_age_le_fast"
CAMPAIGN_AGE_PREDICATE = "predicate::m0::campaign_age_gt_control_duration"
MINIMAL_EMA_HALF_LIVES_S = (4.0, 16.0, 256.0)
MINIMAL_EMA_PAIRS_S = ((4.0, 16.0), (16.0, 256.0))


class CppParityContractError(RuntimeError):
    """Raised when policy, snapshot, wire, compiler, or parity contracts drift."""


@dataclass(frozen=True, slots=True)
class AuditedOwnerPolicy:
    path: Path
    file_sha256: str
    canonical_sha256: str
    policy: BooleanCooldownPolicy
    predicate_columns: tuple[str, ...]
    root_permissions: Mapping[str, bool]


@dataclass(frozen=True, slots=True)
class LiteralOccurrence:
    rule_index: int
    clause_index: int
    literal_index: int
    action: str
    predicate: str
    negated: bool


@dataclass(frozen=True, slots=True)
class StreamingLiteralClosure:
    occurrences: tuple[LiteralOccurrence, ...]
    unique_predicates: tuple[str, ...]
    source_channels: tuple[str, ...]
    ema_half_lives_s: tuple[float, ...]
    ema_pairs_s: tuple[tuple[float, float], ...]


@dataclass(frozen=True, slots=True)
class ParityCase:
    case_id: str
    snapshot_id: str
    feature_side: str
    m0_side: str
    role_at_fill: str
    baseline_duration_ms: int
    snapshot_baseline_duration_ms: int
    policy_input_valid: bool
    feature_block: str
    support_valid: bool
    channel_support_valid: bool
    snapshot_fallback_reason: str | None
    predicates: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class ParityDecision:
    case_id: str
    snapshot_id: str
    action_id: str
    duration_ms: int
    matched_rule_index: int | None
    support_valid: bool
    policy_sha256: str
    fallback_reason: str | None


@dataclass(frozen=True, slots=True)
class StreamingObservationCase:
    case_id: str
    snapshot_id: str
    left_ts_ns: int
    right_ts_ns: int
    feature_ready_ts_ns: int
    decision_ts_ns: int
    market_generation: int
    depth_generation: int
    mid_usdc_per_btc: float | None
    source_gap: bool = False
    source_stale: bool = False
    feature_side: str = "SELL"
    m0_side: str = "SELL"
    role_at_fill: str = "opener"
    baseline_duration_ms: int = 85_000
    snapshot_baseline_duration_ms: int = 85_000
    campaign_age_s: float | None = 0.0
    policy_input_valid: bool = True
    feature_block: str = "M2"


@dataclass(frozen=True, slots=True)
class SaveStreamingCheckpoint:
    name: str


@dataclass(frozen=True, slots=True)
class RestoreStreamingCheckpoint:
    name: str


@dataclass(frozen=True, slots=True)
class ResetUnboundStreamingState:
    pass


StreamingCommand = (
    StreamingObservationCase
    | SaveStreamingCheckpoint
    | RestoreStreamingCheckpoint
    | ResetUnboundStreamingState
)


@dataclass(frozen=True, slots=True)
class StreamingParityResult:
    case_id: str
    snapshot_id: str
    right_ts_ns: int
    feature_ready_ts_ns: int
    decision_ts_ns: int
    market_generation: int
    depth_generation: int
    window_count: int
    gap_window_count: int
    current_window_observed: bool
    warmup_admitted: bool
    support_valid: bool
    last_observed_ts_ns: int | None
    ema_h4s: float | None
    ema_h16s: float | None
    ema_h256s: float | None
    short_effective_sign: int
    short_arrangement_start_ts_ns: int | None
    short_last_cross_ts_ns: int | None
    short_last_cross_direction: int
    short_cross_age_s: float | None
    long_effective_sign: int
    long_arrangement_start_ts_ns: int | None
    long_last_cross_ts_ns: int | None
    long_last_cross_direction: int
    long_cross_age_s: float | None
    short_cross_predicate: int
    campaign_age_predicate: int
    long_cross_predicate: int
    decision: ParityDecision


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


def _object_sequence(value: Any, *, label: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise CppParityContractError(f"{label}_not_sequence")
    rows = list(value)
    if not rows or any(not isinstance(row, Mapping) for row in rows):
        raise CppParityContractError(f"{label}_invalid")
    return rows


def _false_permission(payload: Mapping[str, Any], name: str, *, label: str) -> None:
    if payload.get(name) is not False:
        raise CppParityContractError(f"{label}_{name}_must_be_false")


def audit_sell_m2_streaming_literal_closure(
    policy: BooleanCooldownPolicy,
) -> StreamingLiteralClosure:
    """Lock the actual eight literal occurrences and their minimal state closure."""

    occurrences = tuple(
        LiteralOccurrence(
            rule_index=rule_index,
            clause_index=clause_index,
            literal_index=literal_index,
            action=rule.action,
            predicate=literal.predicate,
            negated=literal.negated,
        )
        for rule_index, rule in enumerate(policy.rules)
        for clause_index, clause in enumerate(rule.clauses)
        for literal_index, literal in enumerate(clause.literals)
    )
    expected = (
        LiteralOccurrence(0, 0, 0, "FIXED_1748S", SHORT_CROSS_PREDICATE, False),
        LiteralOccurrence(0, 0, 1, "FIXED_1748S", CAMPAIGN_AGE_PREDICATE, False),
        LiteralOccurrence(0, 1, 0, "FIXED_1748S", SHORT_CROSS_PREDICATE, True),
        LiteralOccurrence(0, 1, 1, "FIXED_1748S", CAMPAIGN_AGE_PREDICATE, False),
        LiteralOccurrence(1, 0, 0, "FIXED_166S", LONG_CROSS_PREDICATE, False),
        LiteralOccurrence(1, 0, 1, "FIXED_166S", CAMPAIGN_AGE_PREDICATE, True),
        LiteralOccurrence(2, 0, 0, "FIXED_211S", LONG_CROSS_PREDICATE, True),
        LiteralOccurrence(2, 0, 1, "FIXED_211S", CAMPAIGN_AGE_PREDICATE, True),
    )
    if occurrences != expected:
        raise CppParityContractError("owner_policy_streaming_literal_closure_drifted")
    return StreamingLiteralClosure(
        occurrences=occurrences,
        unique_predicates=tuple(sorted({row.predicate for row in occurrences})),
        source_channels=("mid_usdc_per_btc", "campaign_age_s"),
        ema_half_lives_s=MINIMAL_EMA_HALF_LIVES_S,
        ema_pairs_s=MINIMAL_EMA_PAIRS_S,
    )


def audit_owner_policy_json(
    policy_path: str | Path,
    *,
    expected_policy_sha256: str | None = None,
) -> AuditedOwnerPolicy:
    """Validate the frozen owner policy document without loading replay state."""

    path = Path(policy_path).expanduser().resolve()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CppParityContractError("policy_json_invalid") from exc
    if not isinstance(raw, Mapping):
        raise CppParityContractError("policy_root_not_object")
    file_sha = _file_sha256(path)
    if expected_policy_sha256 is not None:
        if not _SHA256_RE.fullmatch(expected_policy_sha256):
            raise CppParityContractError("expected_policy_sha256_is_not_sha256")
        if file_sha != expected_policy_sha256:
            raise CppParityContractError("policy_file_sha256_mismatch")

    canonical = str(raw.get("canonical_sha256", ""))
    body = dict(raw)
    body.pop("canonical_sha256", None)
    if not _SHA256_RE.fullmatch(canonical) or _canonical_sha256(body) != canonical:
        raise CppParityContractError("policy_canonical_sha256_mismatch")
    if (
        raw.get("identity") != runtime_policy.OWNER_POLICY_IDENTITY
        or raw.get("schema_version") != runtime_policy.OWNER_POLICY_SCHEMA
    ):
        raise CppParityContractError("owner_policy_identity_drifted")
    selection = raw.get("selection")
    if not isinstance(selection, Mapping) or selection.get("BUY") != CONTROL_ACTION:
        raise CppParityContractError("owner_policy_buy_selection_drifted")

    root_permissions = raw.get("permissions")
    if not isinstance(root_permissions, Mapping):
        raise CppParityContractError("owner_policy_permissions_missing")
    _false_permission(root_permissions, "action_authorized", label="root")
    _false_permission(root_permissions, "live_authorized", label="root")

    policy_raw = raw.get("policy")
    if not isinstance(policy_raw, Mapping):
        raise CppParityContractError("owner_boolean_policy_missing")
    policy_permissions = policy_raw.get("permissions")
    if not isinstance(policy_permissions, Mapping):
        raise CppParityContractError("boolean_policy_permissions_missing")
    _false_permission(policy_permissions, "action_authorized", label="policy")
    _false_permission(policy_permissions, "live_authorized", label="policy")

    parsed_rules: list[BooleanRule] = []
    try:
        for raw_rule in _object_sequence(
            policy_raw.get("ordered_first_match_rules"), label="policy_rules"
        ):
            action = str(raw_rule.get("action", ""))
            clauses: list[AndClause] = []
            for raw_clause in _object_sequence(raw_rule.get("clauses"), label="policy_clauses"):
                literals: list[TriLiteral] = []
                for raw_literal in _object_sequence(
                    raw_clause.get("literals"), label="policy_literals"
                ):
                    negated = raw_literal.get("negated", False)
                    if not isinstance(negated, bool):
                        raise CppParityContractError("literal_negated_not_bool")
                    literals.append(
                        TriLiteral(
                            predicate=str(raw_literal.get("predicate", "")),
                            negated=negated,
                        )
                    )
                clauses.append(AndClause(literals=tuple(literals)))
            parsed_rules.append(BooleanRule(action=action, clauses=tuple(clauses)))
        policy = BooleanCooldownPolicy(
            side=str(policy_raw.get("side", "")),
            rules=tuple(parsed_rules),
            default_action=str(policy_raw.get("default_action", "")),
        )
    except (CppParityContractError, NestedOofContractError) as exc:
        raise CppParityContractError("boolean_policy_payload_invalid") from exc
    if policy.side != "SELL" or policy.default_action != CONTROL_ACTION:
        raise CppParityContractError("owner_policy_side_or_default_drifted")
    audit_sell_m2_streaming_literal_closure(policy)

    predicate_columns = policy.predicate_columns
    declared = policy_raw.get("predicate_columns")
    if declared not in (None, []):
        if not isinstance(declared, Sequence) or isinstance(declared, (str, bytes)):
            raise CppParityContractError("policy_predicate_columns_invalid")
        if tuple(str(value) for value in declared) != predicate_columns:
            raise CppParityContractError("policy_predicate_columns_drifted")
    return AuditedOwnerPolicy(
        path=path,
        file_sha256=file_sha,
        canonical_sha256=canonical,
        policy=policy,
        predicate_columns=predicate_columns,
        root_permissions={
            "action_authorized": False,
            "live_authorized": False,
        },
    )


def _integral_duration(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CppParityContractError(f"{label}_invalid")
    parsed = float(value)
    rounded = int(round(parsed))
    if parsed <= 0.0 or abs(parsed - rounded) > 1e-6:
        raise CppParityContractError(f"{label}_invalid")
    return rounded


def parity_case_from_snapshot(
    *,
    case_id: str,
    evaluator: runtime_policy.RuntimeCooldownPolicyEvaluator,
    snapshot: CooldownAssignmentSnapshotV2,
    baseline_duration_ms: int | float,
) -> ParityCase:
    """Project one immutable Python snapshot to the standalone C++ boundary."""

    baseline = _integral_duration(baseline_duration_ms, label="baseline_duration_ms")
    feature_row = snapshot.feature_row.to_dict()
    m0 = snapshot.m0_context.to_dict()
    snapshot_baseline = _integral_duration(
        feature_row.get("baseline_duration_ms"),
        label="snapshot_baseline_duration_ms",
    )
    predicates: dict[str, int] = {}
    feature_side = str(feature_row.get("side", "")).upper()
    if (
        snapshot.policy_input_valid
        and snapshot.policy_input is not None
        and snapshot.feature_block == "M2"
        and feature_side == "SELL"
        and feature_row.get("support_valid") is True
        and feature_row.get("channel_support_valid") is True
        and evaluator._policy is not None  # noqa: SLF001 - parity inspects frozen runtime.
    ):
        view = runtime_policy._combine_m2_predicate_view(  # noqa: SLF001
            policy_columns=evaluator._predicate_columns,  # noqa: SLF001
            artifacts=evaluator._artifacts,  # noqa: SLF001
            feature_row=feature_row,
            baseline_duration_ms=baseline,
        )
        predicates = {
            name: int(view.iloc[0][name])
            for name in evaluator._predicate_columns  # noqa: SLF001
        }
    return ParityCase(
        case_id=case_id,
        snapshot_id=snapshot.snapshot_id,
        feature_side=feature_side,
        m0_side=str(m0.get("side", "")).upper(),
        role_at_fill=str(m0.get("role_at_fill", "")).lower(),
        baseline_duration_ms=baseline,
        snapshot_baseline_duration_ms=snapshot_baseline,
        policy_input_valid=bool(snapshot.policy_input_valid),
        feature_block=str(snapshot.feature_block),
        support_valid=feature_row.get("support_valid") is True,
        channel_support_valid=feature_row.get("channel_support_valid") is True,
        snapshot_fallback_reason=snapshot.fallback_reason,
        predicates=predicates,
    )


def _wire_value(value: Any, *, label: str) -> str:
    text = str(value)
    if not text or "\t" in text or "\n" in text or "\r" in text:
        raise CppParityContractError(f"wire_value_invalid:{label}")
    return text


def _policy_wire_lines(
    policy: AuditedOwnerPolicy,
    *,
    expected_policy_sha256: str | None,
) -> list[str]:
    expected = expected_policy_sha256 or policy.file_sha256
    if not _SHA256_RE.fullmatch(expected):
        raise CppParityContractError("expected_policy_sha256_is_not_sha256")
    lines = [
        f"CONTRACT\t{PROTOCOL_IDENTITY}",
        f"POLICY_IDENTITY\t{runtime_policy.OWNER_POLICY_IDENTITY}",
        f"POLICY_SCHEMA\t{runtime_policy.OWNER_POLICY_SCHEMA}",
        f"POLICY_FILE_SHA256\t{policy.file_sha256}",
        f"EXPECTED_POLICY_FILE_SHA256\t{expected}",
        f"POLICY_SIDE\t{policy.policy.side}",
        f"DEFAULT_ACTION\t{policy.policy.default_action}",
        f"SELECTION_BUY\t{CONTROL_ACTION}",
        "ACTION_AUTHORIZED\t0",
        "LIVE_AUTHORIZED\t0",
        f"PREDICATE_COLUMNS\t{len(policy.predicate_columns)}",
    ]
    lines.extend(
        f"PREDICATE_COLUMN\t{_wire_value(name, label='predicate_column')}"
        for name in policy.predicate_columns
    )
    lines.append(f"RULES\t{len(policy.policy.rules)}")
    for rule in policy.policy.rules:
        lines.append(f"RULE\t{rule.action}\t{len(rule.clauses)}")
        for clause in rule.clauses:
            lines.append(f"CLAUSE\t{len(clause.literals)}")
            for literal in clause.literals:
                lines.append(
                    "LITERAL\t"
                    f"{_wire_value(literal.predicate, label='literal_predicate')}\t"
                    f"{int(literal.negated)}"
                )
    return lines


def build_wire_protocol(
    policy: AuditedOwnerPolicy,
    cases: Sequence[ParityCase],
    *,
    expected_policy_sha256: str | None = None,
) -> str:
    """Serialize one audited policy and deterministic cases for the tiny CLI."""

    if not cases:
        raise CppParityContractError("parity_cases_empty")
    case_ids = [case.case_id for case in cases]
    if len(case_ids) != len(set(case_ids)):
        raise CppParityContractError("parity_case_ids_not_unique")

    lines = _policy_wire_lines(
        policy,
        expected_policy_sha256=expected_policy_sha256,
    )

    lines.append(f"CASES\t{len(cases)}")
    for case in cases:
        fallback = case.snapshot_fallback_reason or "-"
        lines.append(
            "\t".join(
                (
                    "CASE",
                    _wire_value(case.case_id, label="case_id"),
                    _wire_value(case.snapshot_id, label="snapshot_id"),
                    _wire_value(case.feature_side, label="feature_side"),
                    _wire_value(case.m0_side, label="m0_side"),
                    _wire_value(case.role_at_fill, label="role_at_fill"),
                    str(case.baseline_duration_ms),
                    str(case.snapshot_baseline_duration_ms),
                    str(int(case.policy_input_valid)),
                    _wire_value(case.feature_block, label="feature_block"),
                    str(int(case.support_valid)),
                    str(int(case.channel_support_valid)),
                    _wire_value(fallback, label="snapshot_fallback_reason"),
                    str(len(case.predicates)),
                )
            )
        )
        for name, value in sorted(case.predicates.items()):
            if isinstance(value, bool) or int(value) not in {-1, 0, 1}:
                raise CppParityContractError(f"predicate_not_three_valued:{name}")
            lines.append(f"PREDICATE\t{_wire_value(name, label='predicate_name')}\t{int(value)}")
        lines.append("END_CASE")
    lines.append("END")
    return "\n".join(lines) + "\n"


def _optional_double_wire(value: float | None, *, label: str) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CppParityContractError(f"{label}_invalid")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise CppParityContractError(f"{label}_invalid")
    return format(parsed, ".17g")


def build_stream_wire_protocol(
    policy: AuditedOwnerPolicy,
    commands: Sequence[StreamingCommand],
    *,
    warmup_admitted: bool,
    warmup_identity: str,
    expected_policy_sha256: str | None = None,
) -> str:
    """Serialize causal mid-window updates and checkpoint commands for C++."""

    audit_sell_m2_streaming_literal_closure(policy.policy)
    if not commands:
        raise CppParityContractError("stream_commands_empty")
    if warmup_admitted and not warmup_identity:
        raise CppParityContractError("admitted_warmup_requires_bound_identity")
    lines = _policy_wire_lines(
        policy,
        expected_policy_sha256=expected_policy_sha256,
    )
    encoded_identity = (
        _wire_value(warmup_identity, label="warmup_identity") if warmup_identity else "-"
    )
    lines.extend(
        (
            f"STREAM_INIT\t{int(warmup_admitted)}\t{encoded_identity}",
            f"STREAM_COMMANDS\t{len(commands)}",
        )
    )
    observed_case_ids: list[str] = []
    for command in commands:
        if isinstance(command, StreamingObservationCase):
            observed_case_ids.append(command.case_id)
            lines.append(
                "\t".join(
                    (
                        "OBSERVE",
                        _wire_value(command.case_id, label="stream_case_id"),
                        _wire_value(command.snapshot_id, label="stream_snapshot_id"),
                        str(command.left_ts_ns),
                        str(command.right_ts_ns),
                        str(command.feature_ready_ts_ns),
                        str(command.decision_ts_ns),
                        str(command.market_generation),
                        str(command.depth_generation),
                        _optional_double_wire(
                            command.mid_usdc_per_btc,
                            label="mid_usdc_per_btc",
                        ),
                        str(int(command.source_gap)),
                        str(int(command.source_stale)),
                        _wire_value(command.feature_side, label="feature_side"),
                        _wire_value(command.m0_side, label="m0_side"),
                        _wire_value(command.role_at_fill, label="role_at_fill"),
                        str(command.baseline_duration_ms),
                        str(command.snapshot_baseline_duration_ms),
                        _optional_double_wire(
                            command.campaign_age_s,
                            label="campaign_age_s",
                        ),
                        str(int(command.policy_input_valid)),
                        _wire_value(command.feature_block, label="feature_block"),
                    )
                )
            )
        elif isinstance(command, SaveStreamingCheckpoint):
            lines.append(f"SAVE\t{_wire_value(command.name, label='checkpoint_name')}")
        elif isinstance(command, RestoreStreamingCheckpoint):
            lines.append(f"RESTORE\t{_wire_value(command.name, label='checkpoint_name')}")
        elif isinstance(command, ResetUnboundStreamingState):
            lines.append("RESET_UNBOUND")
        else:
            raise CppParityContractError("unknown_stream_command")
    if len(observed_case_ids) != len(set(observed_case_ids)):
        raise CppParityContractError("stream_case_ids_not_unique")
    lines.append("END")
    return "\n".join(lines) + "\n"


def compile_cpp_cli(
    output_dir: str | Path,
    *,
    compiler: str | None = None,
) -> Path:
    """Compile the isolated CLI without CMake, pybind, or replay dependencies."""

    if not CPP_SOURCE.is_file() or not CPP_HEADER.is_file():
        raise CppParityContractError("cpp_parity_sources_missing")
    selected = compiler or os.environ.get("CXX") or shutil.which("c++")
    if not selected:
        raise CppParityContractError("cpp_compiler_not_found")
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    binary = root / "f05_streaming_boolean_cooldown_parity_cli"
    command = [
        selected,
        "-std=c++20",
        "-O2",
        "-Wall",
        "-Wextra",
        "-Wpedantic",
        "-Werror",
        "-I",
        str(CPP_DIR),
        str(CPP_SOURCE),
        "-o",
        str(binary),
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise CppParityContractError(
            "cpp_compile_failed:\n" + (completed.stderr or completed.stdout)
        )
    return binary


def run_cpp_cli(binary: str | Path, protocol: str) -> tuple[ParityDecision, ...]:
    completed = subprocess.run(
        [str(Path(binary).resolve())],
        input=protocol,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise CppParityContractError(
            f"cpp_cli_failed:{completed.returncode}:{completed.stderr.strip()}"
        )
    decisions: list[ParityDecision] = []
    for line in completed.stdout.splitlines():
        fields = line.split("\t")
        if len(fields) != 9 or fields[0] != "RESULT":
            raise CppParityContractError("cpp_result_protocol_invalid")
        matched = int(fields[5])
        decisions.append(
            ParityDecision(
                case_id=fields[1],
                snapshot_id=fields[2],
                action_id=fields[3],
                duration_ms=int(fields[4]),
                matched_rule_index=None if matched < 0 else matched,
                support_valid=fields[6] == "1",
                policy_sha256=fields[7],
                fallback_reason=None if fields[8] == "-" else fields[8],
            )
        )
    return tuple(decisions)


def _nullable_int(value: str) -> int | None:
    return None if value == "-" else int(value)


def _nullable_float(value: str) -> float | None:
    return None if value == "-" else float(value)


def run_cpp_stream_cli(
    binary: str | Path,
    protocol: str,
) -> tuple[StreamingParityResult, ...]:
    """Run and parse the isolated streaming state/materialization protocol."""

    completed = subprocess.run(
        [str(Path(binary).resolve())],
        input=protocol,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise CppParityContractError(
            f"cpp_stream_cli_failed:{completed.returncode}:{completed.stderr.strip()}"
        )
    results: list[StreamingParityResult] = []
    for line in completed.stdout.splitlines():
        fields = line.split("\t")
        if len(fields) != 36 or fields[0] != "STREAM_RESULT":
            raise CppParityContractError("cpp_stream_result_protocol_invalid")
        matched = int(fields[32])
        decision = ParityDecision(
            case_id=fields[1],
            snapshot_id=fields[2],
            action_id=fields[30],
            duration_ms=int(fields[31]),
            matched_rule_index=None if matched < 0 else matched,
            support_valid=fields[33] == "1",
            policy_sha256=fields[34],
            fallback_reason=None if fields[35] == "-" else fields[35],
        )
        results.append(
            StreamingParityResult(
                case_id=fields[1],
                snapshot_id=fields[2],
                right_ts_ns=int(fields[3]),
                feature_ready_ts_ns=int(fields[4]),
                decision_ts_ns=int(fields[5]),
                market_generation=int(fields[6]),
                depth_generation=int(fields[7]),
                window_count=int(fields[8]),
                gap_window_count=int(fields[9]),
                current_window_observed=fields[10] == "1",
                warmup_admitted=fields[11] == "1",
                support_valid=fields[12] == "1",
                last_observed_ts_ns=_nullable_int(fields[13]),
                ema_h4s=_nullable_float(fields[14]),
                ema_h16s=_nullable_float(fields[15]),
                ema_h256s=_nullable_float(fields[16]),
                short_effective_sign=int(fields[17]),
                short_arrangement_start_ts_ns=_nullable_int(fields[18]),
                short_last_cross_ts_ns=_nullable_int(fields[19]),
                short_last_cross_direction=int(fields[20]),
                short_cross_age_s=_nullable_float(fields[21]),
                long_effective_sign=int(fields[22]),
                long_arrangement_start_ts_ns=_nullable_int(fields[23]),
                long_last_cross_ts_ns=_nullable_int(fields[24]),
                long_last_cross_direction=int(fields[25]),
                long_cross_age_s=_nullable_float(fields[26]),
                short_cross_predicate=int(fields[27]),
                campaign_age_predicate=int(fields[28]),
                long_cross_predicate=int(fields[29]),
                decision=decision,
            )
        )
    return tuple(results)


def _deterministic_m2_values(mid_usdc_per_btc: float | None) -> dict[str, float | None]:
    values = {
        str(channel["name"]): 1.0 for channel in feature_engine.feature_schema()["blocks"]["M2"]
    }
    values["mid_usdc_per_btc"] = mid_usdc_per_btc
    return values


def _stream_m0_context(observation: StreamingObservationCase) -> dict[str, Any]:
    side = observation.m0_side.upper()
    role = observation.role_at_fill.lower()
    units = observation.snapshot_baseline_duration_ms / 85_000.0
    if side == "SELL":
        after = -0.001 * units
        before = 0.0 if role == "opener" else after + 0.001
    else:
        after = 0.001 * units
        before = 0.0 if role == "opener" else after - 0.001
    return {
        "assignment_ts_ns": observation.decision_ts_ns,
        "fill_visible_ts_ns": observation.decision_ts_ns,
        "side": side,
        "role_at_fill": role,
        "inventory_before_fill_btc": before,
        "inventory_after_fill_btc": after,
        "fill_qty_btc": abs(after - before),
        "order_qty_btc": abs(after - before),
        "cumulative_filled_qty_before_btc": 0.0,
        "cumulative_filled_qty_after_btc": abs(after - before),
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
        "baseline_duration_ms": float(observation.snapshot_baseline_duration_ms),
        "campaign_age_s": observation.campaign_age_s,
        "campaign_add_count": 0 if role == "opener" else max(1, round(units) - 1),
        "campaign_mae_to_date_usdc": 0.0,
        "campaign_inventory_time_to_date_btc_s": 0.0,
        "last_same_side_fill_age_s": None,
        "last_opposite_side_fill_age_s": None,
        "cooldown_remaining_ms": 0.0,
        "cooldown_blocker_active": False,
        "cooldown_lineage_revision_before": 0,
        "cooldown_deadline_owner": "none",
    }


def _python_stream_result(
    *,
    policy: AuditedOwnerPolicy,
    evaluator: runtime_policy.RuntimeCooldownPolicyEvaluator,
    state: feature_engine.CausalMultichannelEmaState,
    observation: StreamingObservationCase,
) -> StreamingParityResult:
    feature_row = state.feature_row(
        side=observation.feature_side,
        decision_ts_ns=observation.decision_ts_ns,
        m0_context=_stream_m0_context(observation),
    )
    view = runtime_policy._combine_m2_predicate_view(  # noqa: SLF001
        policy_columns=evaluator._predicate_columns,  # noqa: SLF001
        artifacts=evaluator._artifacts,  # noqa: SLF001
        feature_row=feature_row,
        baseline_duration_ms=observation.baseline_duration_ms,
    )
    predicates = {
        name: int(view.iloc[0][name])
        for name in evaluator._predicate_columns  # noqa: SLF001
    }
    parity_case = ParityCase(
        case_id=observation.case_id,
        snapshot_id=observation.snapshot_id,
        feature_side=observation.feature_side,
        m0_side=observation.m0_side,
        role_at_fill=observation.role_at_fill,
        baseline_duration_ms=observation.baseline_duration_ms,
        snapshot_baseline_duration_ms=observation.snapshot_baseline_duration_ms,
        policy_input_valid=observation.policy_input_valid,
        feature_block=observation.feature_block,
        support_valid=feature_row["support_valid"] is True,
        channel_support_valid=feature_row["channel_support_valid"] is True,
        snapshot_fallback_reason=None,
        predicates=predicates,
    )
    checkpoint = state.checkpoint()
    mid = checkpoint["channels"]["mid_usdc_per_btc"]
    half_life_index = {
        float(value): index for index, value in enumerate(checkpoint["half_lives_s"])
    }
    short_pair = mid["pair_state"]["4|16"]
    long_pair = mid["pair_state"]["16|256"]
    short_key = feature_engine.pair_key("mid_usdc_per_btc", 4.0, 16.0)
    long_key = feature_engine.pair_key("mid_usdc_per_btc", 16.0, 256.0)
    initialized = mid["last_ts_ns"] is not None
    return StreamingParityResult(
        case_id=observation.case_id,
        snapshot_id=observation.snapshot_id,
        right_ts_ns=observation.right_ts_ns,
        feature_ready_ts_ns=observation.feature_ready_ts_ns,
        decision_ts_ns=observation.decision_ts_ns,
        market_generation=observation.market_generation,
        depth_generation=observation.depth_generation,
        window_count=int(checkpoint["window_count"]),
        gap_window_count=int(checkpoint["gap_window_count"]),
        current_window_observed=bool(mid["current_window_observed"]),
        warmup_admitted=bool(checkpoint["warmup_admitted"]),
        support_valid=feature_row["support_valid"] is True,
        last_observed_ts_ns=mid["last_ts_ns"],
        ema_h4s=(float(mid["ema"][half_life_index[4.0]]) if initialized else None),
        ema_h16s=(float(mid["ema"][half_life_index[16.0]]) if initialized else None),
        ema_h256s=(float(mid["ema"][half_life_index[256.0]]) if initialized else None),
        short_effective_sign=int(short_pair["effective_sign"]),
        short_arrangement_start_ts_ns=short_pair["arrangement_start_ts_ns"],
        short_last_cross_ts_ns=short_pair["last_cross_ts_ns"],
        short_last_cross_direction=int(short_pair["last_cross_direction"]),
        short_cross_age_s=feature_row.get(f"value::{short_key}::cross_age_s"),
        long_effective_sign=int(long_pair["effective_sign"]),
        long_arrangement_start_ts_ns=long_pair["arrangement_start_ts_ns"],
        long_last_cross_ts_ns=long_pair["last_cross_ts_ns"],
        long_last_cross_direction=int(long_pair["last_cross_direction"]),
        long_cross_age_s=feature_row.get(f"value::{long_key}::cross_age_s"),
        short_cross_predicate=predicates[SHORT_CROSS_PREDICATE],
        campaign_age_predicate=predicates[CAMPAIGN_AGE_PREDICATE],
        long_cross_predicate=predicates[LONG_CROSS_PREDICATE],
        decision=reference_decision(policy, parity_case),
    )


def python_stream_reference(
    *,
    policy: AuditedOwnerPolicy,
    evaluator: runtime_policy.RuntimeCooldownPolicyEvaluator,
    commands: Sequence[StreamingCommand],
    warmup_admitted: bool,
    warmup_identity: str,
) -> tuple[StreamingParityResult, ...]:
    """Run the same commands through the existing full Python M2 state."""

    state = feature_engine.CausalMultichannelEmaState(
        block="M2",
        warmup_admitted=warmup_admitted,
        warmup_identity=warmup_identity,
    )
    checkpoints: dict[str, dict[str, Any]] = {}
    results: list[StreamingParityResult] = []
    for command in commands:
        if isinstance(command, StreamingObservationCase):
            state.update(
                feature_engine.CausalWindowObservation(
                    left_ts_ns=command.left_ts_ns,
                    right_ts_ns=command.right_ts_ns,
                    feature_ready_ts_ns=command.feature_ready_ts_ns,
                    market_generation=command.market_generation,
                    depth_generation=command.depth_generation,
                    values=_deterministic_m2_values(command.mid_usdc_per_btc),
                    source_gap=command.source_gap,
                    source_stale=command.source_stale,
                )
            )
            results.append(
                _python_stream_result(
                    policy=policy,
                    evaluator=evaluator,
                    state=state,
                    observation=command,
                )
            )
        elif isinstance(command, SaveStreamingCheckpoint):
            if command.name in checkpoints:
                raise CppParityContractError("duplicate_python_stream_checkpoint")
            checkpoints[command.name] = state.checkpoint()
        elif isinstance(command, RestoreStreamingCheckpoint):
            try:
                checkpoint = checkpoints[command.name]
            except KeyError as exc:
                raise CppParityContractError("python_stream_checkpoint_not_found") from exc
            state = feature_engine.CausalMultichannelEmaState.restore(checkpoint)
        elif isinstance(command, ResetUnboundStreamingState):
            state = feature_engine.CausalMultichannelEmaState(block="M2")
        else:
            raise CppParityContractError("unknown_python_stream_command")
    return tuple(results)


def _close_optional_float(
    observed: float | None,
    expected: float | None,
) -> bool:
    if observed is None or expected is None:
        return observed is expected
    return math.isclose(observed, expected, rel_tol=2e-14, abs_tol=2e-12)


def compare_cpp_stream_with_python(
    *,
    binary: str | Path,
    policy: AuditedOwnerPolicy,
    evaluator: runtime_policy.RuntimeCooldownPolicyEvaluator,
    commands: Sequence[StreamingCommand],
    warmup_admitted: bool,
    warmup_identity: str,
) -> tuple[StreamingParityResult, ...]:
    """Assert state, predicate, and decision parity for every observation."""

    expected = python_stream_reference(
        policy=policy,
        evaluator=evaluator,
        commands=commands,
        warmup_admitted=warmup_admitted,
        warmup_identity=warmup_identity,
    )
    observed = run_cpp_stream_cli(
        binary,
        build_stream_wire_protocol(
            policy,
            commands,
            warmup_admitted=warmup_admitted,
            warmup_identity=warmup_identity,
        ),
    )
    if len(observed) != len(expected):
        raise CppParityContractError("python_cpp_stream_result_count_mismatch")
    float_fields = (
        "ema_h4s",
        "ema_h16s",
        "ema_h256s",
        "short_cross_age_s",
        "long_cross_age_s",
    )
    mismatches: list[dict[str, Any]] = []
    for actual, wanted in zip(observed, expected, strict=True):
        actual_values = {
            field: getattr(actual, field)
            for field in actual.__dataclass_fields__
            if field not in float_fields
        }
        wanted_values = {
            field: getattr(wanted, field)
            for field in wanted.__dataclass_fields__
            if field not in float_fields
        }
        float_match = all(
            _close_optional_float(getattr(actual, field), getattr(wanted, field))
            for field in float_fields
        )
        if actual_values != wanted_values or not float_match:
            mismatches.append(
                {
                    "case_id": wanted.case_id,
                    "python": wanted,
                    "cpp": actual,
                }
            )
    if mismatches:
        raise CppParityContractError(f"python_cpp_streaming_state_mismatch:{mismatches[:3]!r}")
    return observed


def python_decision(
    case_id: str,
    decision: runtime_policy.CooldownDurationDecision,
) -> ParityDecision:
    return ParityDecision(
        case_id=case_id,
        snapshot_id=decision.snapshot_id,
        action_id=decision.action_id,
        duration_ms=decision.duration_ms,
        matched_rule_index=decision.matched_rule_index,
        support_valid=decision.support_valid,
        policy_sha256=decision.policy_sha256,
        fallback_reason=decision.fallback_reason,
    )


def _duration_for_action(action: str, baseline_duration_ms: int) -> int:
    if action == CONTROL_ACTION:
        return baseline_duration_ms
    match = _FIXED_ACTION_RE.fullmatch(action)
    if match is None:
        raise CppParityContractError(f"unsupported_duration_action:{action}")
    return int(match.group(1)) * 1_000


def reference_decision(
    policy: AuditedOwnerPolicy,
    case: ParityCase,
    *,
    binding_error: str | None = None,
) -> ParityDecision:
    """Python prevalidation reference for cases invalid before snapshot creation."""

    def control(reason: str, support: bool, duration: int | None = None) -> ParityDecision:
        return ParityDecision(
            case_id=case.case_id,
            snapshot_id=case.snapshot_id,
            action_id=CONTROL_ACTION,
            duration_ms=duration or case.baseline_duration_ms,
            matched_rule_index=None,
            support_valid=support,
            policy_sha256=policy.file_sha256,
            fallback_reason=reason,
        )

    if case.baseline_duration_ms <= 0:
        return control("baseline_duration_ms_invalid", False, 85_000)
    if binding_error:
        return control(f"runtime_binding_invalid:{binding_error}", False)
    if not case.policy_input_valid:
        reason = case.snapshot_fallback_reason or "snapshot_policy_input_invalid"
        return control(f"snapshot_invalid:{reason}", False)
    if case.snapshot_baseline_duration_ms != case.baseline_duration_ms:
        return control("snapshot_baseline_duration_drifted", False)
    if case.feature_side != case.m0_side or case.feature_side not in {"BUY", "SELL"}:
        return control("snapshot_side_inconsistent", False)
    if case.role_at_fill not in {"opener", "add"}:
        return control("snapshot_invalid:role_not_exposure_increasing", False)
    if case.feature_block != "M2":
        return control("snapshot_feature_block_not_m2", False)
    if not case.support_valid or not case.channel_support_valid:
        return control("snapshot_m2_support_invalid", False)
    if case.feature_side == "BUY":
        return control("buy_control_by_contract", True)

    frame = pd.DataFrame([dict(case.predicates)], dtype="int8")
    for index, rule in enumerate(policy.policy.rules):
        try:
            state = int(rule.evaluate(frame)[0])
        except NestedOofContractError as exc:
            return control(str(exc), False)
        if state == 1:
            return ParityDecision(
                case_id=case.case_id,
                snapshot_id=case.snapshot_id,
                action_id=rule.action,
                duration_ms=_duration_for_action(rule.action, case.baseline_duration_ms),
                matched_rule_index=index,
                support_valid=True,
                policy_sha256=policy.file_sha256,
                fallback_reason=None,
            )
        if state == -1:
            return control(f"rule_unobserved:{index}", False)
    return control("no_rule_matched", True)


def compare_cpp_with_runtime(
    *,
    binary: str | Path,
    policy: AuditedOwnerPolicy,
    evaluator: runtime_policy.RuntimeCooldownPolicyEvaluator,
    snapshots: Sequence[tuple[str, CooldownAssignmentSnapshotV2, int]],
) -> tuple[ParityDecision, ...]:
    """Evaluate the same valid snapshots through Python and standalone C++."""

    cases = tuple(
        parity_case_from_snapshot(
            case_id=case_id,
            evaluator=evaluator,
            snapshot=snapshot,
            baseline_duration_ms=baseline,
        )
        for case_id, snapshot, baseline in snapshots
    )
    python_results = tuple(
        python_decision(case_id, evaluator.evaluate(snapshot, baseline))
        for case_id, snapshot, baseline in snapshots
    )
    cpp_results = run_cpp_cli(binary, build_wire_protocol(policy, cases))
    if cpp_results != python_results:
        mismatches = [
            {
                "case_id": expected.case_id,
                "python": expected,
                "cpp": actual,
            }
            for expected, actual in zip(python_results, cpp_results, strict=True)
            if expected != actual
        ]
        raise CppParityContractError(f"python_cpp_decision_mismatch:{mismatches!r}")
    return cpp_results


def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    audit_parser = subparsers.add_parser("audit-policy")
    audit_parser.add_argument("--policy", type=Path, required=True)
    audit_parser.add_argument("--expected-sha256")
    compile_parser = subparsers.add_parser("compile")
    compile_parser.add_argument("--output-dir", type=Path, required=True)
    compile_parser.add_argument("--compiler")
    args = parser.parse_args()
    if args.command == "audit-policy":
        audited = audit_owner_policy_json(
            args.policy,
            expected_policy_sha256=args.expected_sha256,
        )
        closure = audit_sell_m2_streaming_literal_closure(audited.policy)
        print(
            json.dumps(
                {
                    "policy_path": str(audited.path),
                    "file_sha256": audited.file_sha256,
                    "canonical_sha256": audited.canonical_sha256,
                    "side": audited.policy.side,
                    "rule_count": len(audited.policy.rules),
                    "predicate_columns": list(audited.predicate_columns),
                    "literal_occurrence_count": len(closure.occurrences),
                    "literal_occurrences": [
                        {
                            "rule_index": row.rule_index,
                            "clause_index": row.clause_index,
                            "literal_index": row.literal_index,
                            "action": row.action,
                            "predicate": row.predicate,
                            "negated": row.negated,
                        }
                        for row in closure.occurrences
                    ],
                    "minimal_streaming_closure": {
                        "source_channels": list(closure.source_channels),
                        "ema_half_lives_s": list(closure.ema_half_lives_s),
                        "ema_pairs_s": [list(pair) for pair in closure.ema_pairs_s],
                        "all_m2_parity_claimed": False,
                    },
                    "action_authorized": False,
                    "live_authorized": False,
                },
                sort_keys=True,
            )
        )
        return 0
    binary = compile_cpp_cli(args.output_dir, compiler=args.compiler)
    print(binary)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())


__all__ = [
    "AuditedOwnerPolicy",
    "CAMPAIGN_AGE_PREDICATE",
    "CppParityContractError",
    "LiteralOccurrence",
    "LONG_CROSS_PREDICATE",
    "MINIMAL_EMA_HALF_LIVES_S",
    "MINIMAL_EMA_PAIRS_S",
    "ParityCase",
    "ParityDecision",
    "ResetUnboundStreamingState",
    "RestoreStreamingCheckpoint",
    "SHORT_CROSS_PREDICATE",
    "SaveStreamingCheckpoint",
    "StreamingLiteralClosure",
    "StreamingObservationCase",
    "StreamingParityResult",
    "audit_owner_policy_json",
    "audit_sell_m2_streaming_literal_closure",
    "build_stream_wire_protocol",
    "build_wire_protocol",
    "compare_cpp_stream_with_python",
    "compare_cpp_with_runtime",
    "compile_cpp_cli",
    "parity_case_from_snapshot",
    "python_decision",
    "reference_decision",
    "run_cpp_cli",
    "run_cpp_stream_cli",
]
