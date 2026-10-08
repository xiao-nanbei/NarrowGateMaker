"""Atomic, outcome-blind fill snapshots for cooldown-duration v2.

The snapshot is the only policy-input boundary for
``causal_multichannel_window_boolean_cooldown_duration_v2``.  It captures one
immutable row at a strategy-visible exposure-increasing fill callback.  Bad
identity, schema, causality, or source-lineage data fail closed.  Missing but
well-described source support remains auditable and forces ``CONTROL_85N``.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from numbers import Integral, Real
from typing import Any

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
    CHANNELS_BY_BLOCK,
    EMA_HALF_LIVES_S,
    IDENTITY,
    M0_NULLABLE_FIELDS,
    M0_REQUIRED_FIELDS,
    MAX_EXPLICIT_WINDOW_COUNT,
    SCHEMA_VERSION,
    FeatureContractError,
    TriState,
    ema_pairs,
    pair_key,
    validate_m0_context,
)

SNAPSHOT_SCHEMA_VERSION = f"{IDENTITY}.assignment_snapshot.v2"
CONTROL_POLICY_ID = "CONTROL_85N"
SUPPORTED_FEATURE_BLOCKS = ("R0", "M1", "M2")

HISTORICAL_EXCHANGE_EVENT_PROFILE = (
    "historical_exchange_event_visibility_exploratory"
)
PROSPECTIVE_RECEIVE_TIME_PROFILE = "prospective_receive_feature_ready_transport"
VISIBILITY_PROFILES = (
    HISTORICAL_EXCHANGE_EVENT_PROFILE,
    PROSPECTIVE_RECEIVE_TIME_PROFILE,
)

CLOCK_NAMES = (
    "assignment",
    "fill_exchange",
    "fill_receive",
    "fill_visible",
    "feature_ready",
)
SOURCE_NAMES = ("market", "depth", "trade")
IDENTITY_HASH_FIELDS = (
    "config_sha256",
    "code_sha256",
    "model_sha256",
    "p3_sha256",
    "feature_dag_sha256",
    "execution_abi_sha256",
    "baseline_identity_sha256",
)

_TOP_LEVEL_FIELDS = frozenset(
    {
        "snapshot_id",
        "assignment_id",
        "fill_event_id",
        "client_order_id",
        "lineage_id",
        "lineage_revision",
        "partial_fill_ordinal",
        "partial_fill_qty_btc",
        "visibility_profile",
        "clocks",
        "sources",
        "identity_hashes",
        "m0_context",
        "feature_row",
    }
)
_CLOCK_FIELDS = frozenset({"ts_ns", "valid", "unknown", "reason"})
_SOURCE_FIELDS = frozenset(
    {
        "generation",
        "cursor",
        "feature_generation",
        "feature_cursor",
        "valid",
        "unknown",
        "reason",
    }
)
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_BAD_ID_STRINGS = frozenset({"", "nan", "none", "null", "nat"})
_ECONOMIC_OUTCOME_TOKENS = frozenset(
    {
        "pnl",
        "profit",
        "reward",
        "markout",
        "terminalvalue",
        "terminal_value",
        "economicoutcome",
        "economic_outcome",
        "label",
    }
)


class SnapshotContractError(ValueError):
    """Raised when an assignment snapshot violates its frozen contract."""


@dataclass(frozen=True, slots=True)
class FieldValidity:
    """Validity metadata for one immutable snapshot field."""

    valid: bool
    unknown: bool
    reason: str

    def __post_init__(self) -> None:
        if type(self.valid) is not bool or type(self.unknown) is not bool:
            raise SnapshotContractError("validity flags must be bool")
        reason = _nonempty_text(self.reason, "validity reason")
        object.__setattr__(self, "reason", reason)


@dataclass(frozen=True, slots=True)
class FrozenRow(Mapping[str, Any]):
    """Small immutable mapping used to avoid retaining caller-owned dicts."""

    _items: tuple[tuple[str, Any], ...]

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> FrozenRow:
        items = tuple(
            (str(key), _freeze_value(value))
            for key, value in sorted(row.items(), key=lambda item: str(item[0]))
        )
        keys = tuple(key for key, _ in items)
        if len(keys) != len(set(keys)):
            raise SnapshotContractError("frozen row contains duplicate keys")
        return cls(items)

    def __getitem__(self, key: str) -> Any:
        for name, value in self._items:
            if name == key:
                return value
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return (name for name, _ in self._items)

    def __len__(self) -> int:
        return len(self._items)

    def to_dict(self) -> dict[str, Any]:
        return {name: _thaw_value(value) for name, value in self._items}


@dataclass(frozen=True, slots=True)
class ClockBinding:
    ts_ns: int | None
    status: FieldValidity


@dataclass(frozen=True, slots=True)
class SourceBinding:
    generation: int | None
    cursor: str | None
    feature_generation: int | None
    feature_cursor: str | None
    status: FieldValidity


@dataclass(frozen=True, slots=True)
class PolicyInputV2:
    """Input exposed to the Boolean learner only after all gates pass."""

    snapshot_id: str
    visibility_profile: str
    feature_block: str
    source_bundle_sha256: str
    identity_hashes: FrozenRow
    m0_context: FrozenRow
    feature_row: FrozenRow


@dataclass(frozen=True, slots=True)
class CooldownAssignmentSnapshotV2:
    """One deeply immutable exposure-fill assignment row."""

    schema_version: str
    identity: str
    snapshot_id: str
    assignment_id: str
    fill_event_id: str
    client_order_id: str
    lineage_id: str
    lineage_revision: int
    partial_fill_ordinal: int
    partial_fill_qty_btc: float
    visibility_profile: str
    receive_time_transport_eligible: bool
    clocks: FrozenRow
    sources: FrozenRow
    source_bundle_sha256: str
    identity_hashes: FrozenRow
    m0_context: FrozenRow
    feature_block: str
    feature_row: FrozenRow
    field_validity: FrozenRow
    policy_input_valid: bool
    policy_input: PolicyInputV2 | None
    fallback_policy_id: str | None
    fallback_reason: str | None
    economic_outcomes_read: bool = False

    def __post_init__(self) -> None:
        if self.schema_version != SNAPSHOT_SCHEMA_VERSION:
            raise SnapshotContractError("snapshot schema identity drifted")
        if self.identity != IDENTITY:
            raise SnapshotContractError("research identity drifted")
        if self.economic_outcomes_read:
            raise SnapshotContractError("economic outcomes are prohibited")
        if self.policy_input_valid:
            if self.policy_input is None:
                raise SnapshotContractError("valid policy snapshot lacks input")
            if self.fallback_policy_id is not None or self.fallback_reason is not None:
                raise SnapshotContractError("valid policy snapshot cannot fall back")
        else:
            if self.policy_input is not None:
                raise SnapshotContractError("invalid policy snapshot exposed input")
            if self.fallback_policy_id != CONTROL_POLICY_ID:
                raise SnapshotContractError("invalid snapshot must use CONTROL_85N")
            _nonempty_text(self.fallback_reason, "fallback reason")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "identity": self.identity,
            "snapshot_id": self.snapshot_id,
            "assignment_id": self.assignment_id,
            "fill_event_id": self.fill_event_id,
            "client_order_id": self.client_order_id,
            "lineage_id": self.lineage_id,
            "lineage_revision": self.lineage_revision,
            "partial_fill_ordinal": self.partial_fill_ordinal,
            "partial_fill_qty_btc": self.partial_fill_qty_btc,
            "visibility_profile": self.visibility_profile,
            "receive_time_transport_eligible": self.receive_time_transport_eligible,
            "clocks": self.clocks.to_dict(),
            "sources": self.sources.to_dict(),
            "source_bundle_sha256": self.source_bundle_sha256,
            "identity_hashes": self.identity_hashes.to_dict(),
            "m0_context": self.m0_context.to_dict(),
            "feature_block": self.feature_block,
            "feature_row": self.feature_row.to_dict(),
            "field_validity": self.field_validity.to_dict(),
            "policy_input_valid": self.policy_input_valid,
            "fallback_policy_id": self.fallback_policy_id,
            "fallback_reason": self.fallback_reason,
            "economic_outcomes_read": False,
        }


def _freeze_value(value: Any) -> Any:
    if isinstance(value, FrozenRow):
        return value
    if isinstance(value, Mapping):
        return FrozenRow.from_mapping(value)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, set):
        return tuple(sorted(_freeze_value(item) for item in value))
    return value


def _thaw_value(value: Any) -> Any:
    if isinstance(value, FrozenRow):
        return value.to_dict()
    if isinstance(value, tuple):
        return [_thaw_value(item) for item in value]
    if isinstance(value, FieldValidity):
        return {
            "valid": value.valid,
            "unknown": value.unknown,
            "reason": value.reason,
        }
    if isinstance(value, ClockBinding):
        return {
            "ts_ns": value.ts_ns,
            "status": _thaw_value(value.status),
        }
    if isinstance(value, SourceBinding):
        return {
            "generation": value.generation,
            "cursor": value.cursor,
            "feature_generation": value.feature_generation,
            "feature_cursor": value.feature_cursor,
            "status": _thaw_value(value.status),
        }
    return value


def _nonempty_text(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise SnapshotContractError(f"{label} must be a string")
    text = value.strip()
    if text.lower() in _BAD_ID_STRINGS:
        raise SnapshotContractError(f"{label} is empty or NaN-like")
    return text


def _valid_identifier(value: Any, label: str) -> str:
    text = _nonempty_text(value, label)
    if any(character.isspace() for character in text):
        raise SnapshotContractError(f"{label} must not contain whitespace")
    return text


def _valid_sha256(value: Any, label: str) -> str:
    text = _valid_identifier(value, label)
    if _SHA256_RE.fullmatch(text) is None:
        raise SnapshotContractError(f"{label} must be an exact SHA256")
    return text.lower()


def _strict_int(value: Any, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise SnapshotContractError(f"{label} must be an integer")
    result = int(value)
    if result < minimum:
        raise SnapshotContractError(f"{label} must be >= {minimum}")
    return result


def _finite_float(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise SnapshotContractError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise SnapshotContractError(f"{label} must be finite")
    return result


def _bool(value: Any, label: str) -> bool:
    if type(value) is not bool:
        raise SnapshotContractError(f"{label} must be bool")
    return value


def _binary(value: Any, label: str) -> int:
    if isinstance(value, bool):
        return int(value)
    result = _strict_int(value, label)
    if result not in {0, 1}:
        raise SnapshotContractError(f"{label} must be 0 or 1")
    return result


def _tri_state(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise SnapshotContractError(f"{label} must use TriState integers")
    result = int(value)
    if result not in {int(state) for state in TriState}:
        raise SnapshotContractError(f"{label} is outside TriState")
    return result


def _looks_economic(name: str) -> bool:
    lowered = name.lower()
    compact = re.sub(r"[^a-z0-9_]", "", lowered)
    pieces = set(re.split(r"[^a-z0-9]+", lowered))
    return bool(_ECONOMIC_OUTCOME_TOKENS.intersection(pieces)) or any(
        token in compact for token in _ECONOMIC_OUTCOME_TOKENS
    )


def _exact_mapping(
    value: Any,
    *,
    label: str,
    expected: frozenset[str] | set[str],
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SnapshotContractError(f"{label} must be a mapping")
    keys = {str(key) for key in value}
    extra = sorted(keys - set(expected))
    missing = sorted(set(expected) - keys)
    if extra:
        outcome = [name for name in extra if _looks_economic(name)]
        if outcome:
            raise SnapshotContractError(
                f"economic outcomes are prohibited in {label}: {outcome}"
            )
        raise SnapshotContractError(f"unknown {label} columns: {extra}")
    if missing:
        raise SnapshotContractError(f"missing {label} columns: {missing}")
    return value


def _parse_status(row: Mapping[str, Any], label: str) -> FieldValidity:
    return FieldValidity(
        valid=_bool(row["valid"], f"{label}.valid"),
        unknown=_bool(row["unknown"], f"{label}.unknown"),
        reason=_nonempty_text(row["reason"], f"{label}.reason"),
    )


def _parse_clock(value: Any, name: str) -> ClockBinding:
    row = _exact_mapping(value, label=f"clock.{name}", expected=_CLOCK_FIELDS)
    status = _parse_status(row, f"clock.{name}")
    timestamp = row["ts_ns"]
    if timestamp is None:
        parsed = None
    else:
        parsed = _strict_int(timestamp, f"clock.{name}.ts_ns", minimum=1)
    if status.valid and (status.unknown or parsed is None):
        raise SnapshotContractError(f"valid clock {name} cannot be unknown")
    if status.unknown and parsed is not None:
        raise SnapshotContractError(f"unknown clock {name} must not invent a value")
    return ClockBinding(ts_ns=parsed, status=status)


def _optional_generation(value: Any, label: str) -> int | None:
    if value is None:
        return None
    return _strict_int(value, label)


def _optional_cursor(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _valid_identifier(value, label)


def _parse_source(value: Any, name: str) -> SourceBinding:
    row = _exact_mapping(value, label=f"source.{name}", expected=_SOURCE_FIELDS)
    status = _parse_status(row, f"source.{name}")
    generation = _optional_generation(
        row["generation"], f"source.{name}.generation"
    )
    feature_generation = _optional_generation(
        row["feature_generation"], f"source.{name}.feature_generation"
    )
    cursor = _optional_cursor(row["cursor"], f"source.{name}.cursor")
    feature_cursor = _optional_cursor(
        row["feature_cursor"], f"source.{name}.feature_cursor"
    )
    if status.valid and (
        status.unknown
        or generation is None
        or feature_generation is None
        or cursor is None
        or feature_cursor is None
    ):
        raise SnapshotContractError(f"valid source {name} is incomplete")
    if status.unknown and any(
        item is not None
        for item in (generation, feature_generation, cursor, feature_cursor)
    ):
        raise SnapshotContractError(f"unknown source {name} must not invent bindings")
    if generation != feature_generation or cursor != feature_cursor:
        raise SnapshotContractError(
            f"source {name} generation/cursor was silently mixed"
        )
    return SourceBinding(
        generation=generation,
        cursor=cursor,
        feature_generation=feature_generation,
        feature_cursor=feature_cursor,
        status=status,
    )


def _ordered_unique(values: list[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))


def _channel_feature_columns(channel: str) -> tuple[str, ...]:
    columns = [f"channel::{channel}::observed"]
    for half_life in EMA_HALF_LIVES_S:
        label = f"h{float(half_life):g}s".replace(".", "p")
        columns.extend(
            (
                f"value::{channel}::ema::{label}",
                f"value::{channel}::slope::{label}",
                f"value::{channel}::curvature::{label}",
            )
        )
    for fast, slow in ema_pairs():
        prefix = pair_key(channel, fast, slow)
        columns.extend(
            (
                f"tri::{prefix}::positive_ordering",
                f"tri::{prefix}::last_cross_positive",
                f"value::{prefix}::cross_age_s",
                f"value::{prefix}::arrangement_persistence_s",
                f"value::{prefix}::signed_distance",
                f"value::{prefix}::abs_distance",
                f"value::{prefix}::signed_distance_velocity",
                f"value::{prefix}::signed_distance_acceleration",
                f"tri::{prefix}::expanding",
                f"tri::{prefix}::converging",
            )
        )
    return _ordered_unique(columns)


_FEATURE_COMMON_COLUMNS = (
    "schema_version",
    "identity",
    "feature_block",
    "base_window_width_ns",
    "maximum_explicit_window_count",
    "last_window_right_ts_ns",
    "feature_ready_ts_ns",
    "decision_ts_ns",
    "market_generation",
    "depth_generation",
    "window_count",
    "gap_window_count",
    "warmup_admitted",
    "warmup_identity",
    "support_valid",
    *M0_REQUIRED_FIELDS,
    "channel_support_valid",
)


def expected_feature_columns(block: str) -> tuple[str, ...]:
    if block not in SUPPORTED_FEATURE_BLOCKS:
        raise SnapshotContractError("snapshot feature block must be R0, M1, or M2")
    columns = list(_FEATURE_COMMON_COLUMNS)
    for channel in CHANNELS_BY_BLOCK[block]:
        columns.extend(_channel_feature_columns(channel.name))
    return _ordered_unique(columns)


def _minimal_channel_columns(channel: str) -> set[str]:
    columns = {f"channel::{channel}::observed"}
    for fast, slow in ema_pairs():
        prefix = pair_key(channel, fast, slow)
        columns.add(f"tri::{prefix}::positive_ordering")
        columns.add(f"tri::{prefix}::last_cross_positive")
    return columns


def _same_value(left: Any, right: Any) -> bool:
    if isinstance(left, Real) and not isinstance(left, bool):
        if not isinstance(right, Real) or isinstance(right, bool):
            return False
        return math.isclose(float(left), float(right), rel_tol=0.0, abs_tol=1e-12)
    return left == right


def _normalize_feature_row(
    raw: Any,
    *,
    m0: Mapping[str, Any],
    clocks: Mapping[str, ClockBinding],
    sources: Mapping[str, SourceBinding],
    statuses: dict[str, FieldValidity],
) -> tuple[str, dict[str, Any], list[str]]:
    if not isinstance(raw, Mapping):
        raise SnapshotContractError("feature_row must be a mapping")
    if "feature_block" not in raw:
        raise SnapshotContractError("feature_row lacks feature_block")
    block = str(raw["feature_block"])
    expected = set(expected_feature_columns(block))
    raw_keys = {str(key) for key in raw}
    extra = sorted(raw_keys - expected)
    if extra:
        outcome = [name for name in extra if _looks_economic(name)]
        if outcome:
            raise SnapshotContractError(
                f"economic outcomes are prohibited in feature_row: {outcome}"
            )
        raise SnapshotContractError(f"unknown feature_row columns: {extra}")

    required = set(_FEATURE_COMMON_COLUMNS)
    for channel in CHANNELS_BY_BLOCK[block]:
        required.update(_minimal_channel_columns(channel.name))
    missing_required = sorted(required - raw_keys)
    if missing_required:
        raise SnapshotContractError(
            f"missing required feature_row columns: {missing_required}"
        )

    if raw["schema_version"] != SCHEMA_VERSION or raw["identity"] != IDENTITY:
        raise SnapshotContractError("feature identity/schema drifted")
    if _strict_int(raw["base_window_width_ns"], "feature base window", minimum=1) != (
        BASE_WINDOW_WIDTH_NS
    ):
        raise SnapshotContractError("feature base window drifted")
    if _strict_int(
        raw["maximum_explicit_window_count"],
        "feature maximum window count",
        minimum=1,
    ) != MAX_EXPLICIT_WINDOW_COUNT:
        raise SnapshotContractError("feature window bound drifted")

    fill_visible = clocks["fill_visible"].ts_ns
    feature_ready = clocks["feature_ready"].ts_ns
    assert fill_visible is not None and feature_ready is not None
    if _strict_int(raw["feature_ready_ts_ns"], "feature ready", minimum=1) != (
        feature_ready
    ):
        raise SnapshotContractError("feature-ready clock binding drifted")
    if _strict_int(raw["decision_ts_ns"], "feature decision", minimum=1) != (
        fill_visible
    ):
        raise SnapshotContractError("feature decision is not fill-visible cutoff")
    window_right = _strict_int(
        raw["last_window_right_ts_ns"], "last window right", minimum=1
    )
    if window_right > feature_ready:
        raise SnapshotContractError("feature window ends after it became ready")

    market_generation = _strict_int(
        raw["market_generation"], "feature market generation"
    )
    depth_generation = _strict_int(
        raw["depth_generation"], "feature depth generation"
    )
    if market_generation != sources["market"].feature_generation:
        raise SnapshotContractError("feature market generation was silently mixed")
    if depth_generation != sources["depth"].feature_generation:
        raise SnapshotContractError("feature depth generation was silently mixed")

    window_count = _strict_int(raw["window_count"], "feature window count", minimum=1)
    gap_count = _strict_int(raw["gap_window_count"], "feature gap count")
    if gap_count > window_count:
        raise SnapshotContractError("feature gap count exceeds window count")
    warmup_admitted = _bool(raw["warmup_admitted"], "feature warmup_admitted")
    support_valid = _bool(raw["support_valid"], "feature support_valid")
    channel_support_valid = _bool(
        raw["channel_support_valid"], "feature channel_support_valid"
    )

    normalized: dict[str, Any] = {}
    for name in _FEATURE_COMMON_COLUMNS:
        normalized[name] = raw[name]
        statuses[f"feature.{name}"] = FieldValidity(True, False, "valid")

    warmup_identity = raw["warmup_identity"]
    if warmup_admitted:
        normalized["warmup_identity"] = _valid_identifier(
            warmup_identity, "feature warmup identity"
        )
    elif isinstance(warmup_identity, str) and not warmup_identity.strip():
        normalized["warmup_identity"] = None
        statuses["feature.warmup_identity"] = FieldValidity(
            False, True, "warmup_not_admitted"
        )
    else:
        normalized["warmup_identity"] = _valid_identifier(
            warmup_identity, "feature warmup identity"
        )

    for name in M0_REQUIRED_FIELDS:
        if not _same_value(raw[name], m0[name]):
            raise SnapshotContractError(f"feature/M0 field drifted: {name}")
        normalized[name] = m0[name]

    observed_channels: list[str] = []
    invalid_policy_fields: list[str] = []
    for channel_spec in CHANNELS_BY_BLOCK[block]:
        channel = channel_spec.name
        observed_key = f"channel::{channel}::observed"
        observed = bool(_binary(raw[observed_key], observed_key))
        normalized[observed_key] = int(observed)
        statuses[f"feature.{observed_key}"] = FieldValidity(True, False, "valid")
        full_columns = set(_channel_feature_columns(channel))
        minimal_columns = _minimal_channel_columns(channel)
        if observed:
            observed_channels.append(channel)
            missing = sorted(full_columns - raw_keys)
            if missing:
                raise SnapshotContractError(
                    f"observed channel {channel} lacks columns: {missing}"
                )
        else:
            stale = sorted(
                name
                for name in (full_columns - minimal_columns).intersection(raw_keys)
                if raw[name] is not None
            )
            if stale:
                raise SnapshotContractError(
                    f"unobserved channel {channel} retained stale values: {stale}"
                )

        for fast, slow in ema_pairs():
            prefix = pair_key(channel, fast, slow)
            ordering_key = f"tri::{prefix}::positive_ordering"
            cross_key = f"tri::{prefix}::last_cross_positive"
            ordering = _tri_state(raw[ordering_key], ordering_key)
            last_cross = _tri_state(raw[cross_key], cross_key)
            if not observed and (
                ordering != int(TriState.UNOBSERVED)
                or last_cross != int(TriState.UNOBSERVED)
            ):
                raise SnapshotContractError(
                    f"unobserved channel {channel} leaked Boolean state"
                )
            normalized[ordering_key] = ordering
            normalized[cross_key] = last_cross
            unknown_ordering = ordering == int(TriState.UNOBSERVED)
            unknown_cross = last_cross == int(TriState.UNOBSERVED)
            statuses[f"feature.{ordering_key}"] = FieldValidity(
                observed,
                unknown_ordering,
                "no_ordering_observed" if unknown_ordering else "valid",
            )
            statuses[f"feature.{cross_key}"] = FieldValidity(
                observed,
                unknown_cross,
                "no_cross_observed" if unknown_cross else "valid",
            )

        for name in full_columns - minimal_columns:
            path = f"feature.{name}"
            if not observed:
                normalized[name] = None
                statuses[path] = FieldValidity(False, True, "channel_unobserved")
                invalid_policy_fields.append(path)
                continue
            value = raw[name]
            allow_unknown = name.endswith(
                ("::cross_age_s", "::arrangement_persistence_s")
            )
            if value is None and allow_unknown:
                normalized[name] = None
                statuses[path] = FieldValidity(True, True, "event_not_yet_observed")
                continue
            if name.startswith("tri::"):
                normalized[name] = _binary(value, name)
            else:
                number = _finite_float(value, name)
                if name.endswith("::abs_distance") and number < 0.0:
                    raise SnapshotContractError(f"{name} must be non-negative")
                if name.endswith(
                    ("::cross_age_s", "::arrangement_persistence_s")
                ) and number < 0.0:
                    raise SnapshotContractError(f"{name} must be non-negative")
                normalized[name] = number
            statuses[path] = FieldValidity(True, False, "valid")

    calculated_channel_support = len(observed_channels) == len(CHANNELS_BY_BLOCK[block])
    if channel_support_valid != calculated_channel_support:
        raise SnapshotContractError("channel support flag disagrees with feature rows")
    if support_valid != bool(warmup_admitted and channel_support_valid):
        raise SnapshotContractError("feature support flag is internally inconsistent")

    normalized["schema_version"] = SCHEMA_VERSION
    normalized["identity"] = IDENTITY
    normalized["feature_block"] = block
    normalized["base_window_width_ns"] = BASE_WINDOW_WIDTH_NS
    normalized["maximum_explicit_window_count"] = MAX_EXPLICIT_WINDOW_COUNT
    normalized["last_window_right_ts_ns"] = window_right
    normalized["feature_ready_ts_ns"] = feature_ready
    normalized["decision_ts_ns"] = fill_visible
    normalized["market_generation"] = market_generation
    normalized["depth_generation"] = depth_generation
    normalized["window_count"] = window_count
    normalized["gap_window_count"] = gap_count
    normalized["warmup_admitted"] = warmup_admitted
    normalized["support_valid"] = support_valid
    normalized["channel_support_valid"] = channel_support_valid

    for name in expected_feature_columns(block):
        if name not in normalized:
            normalized[name] = None
            statuses[f"feature.{name}"] = FieldValidity(
                False, True, "field_not_emitted"
            )
            invalid_policy_fields.append(f"feature.{name}")
    return block, normalized, invalid_policy_fields


def _source_bundle_sha256(sources: Mapping[str, SourceBinding]) -> str:
    payload = {
        name: {
            "generation": source.generation,
            "cursor": source.cursor,
            "feature_generation": source.feature_generation,
            "feature_cursor": source.feature_cursor,
            "valid": source.status.valid,
            "unknown": source.status.unknown,
            "reason": source.status.reason,
        }
        for name, source in sorted(sources.items())
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def capture_cooldown_assignment_snapshot(
    payload: Mapping[str, Any],
) -> CooldownAssignmentSnapshotV2:
    """Validate and freeze one outcome-blind fill-visible assignment row."""

    row = _exact_mapping(payload, label="snapshot", expected=_TOP_LEVEL_FIELDS)
    statuses: dict[str, FieldValidity] = {}

    identifiers = {
        name: _valid_identifier(row[name], name)
        for name in (
            "snapshot_id",
            "assignment_id",
            "fill_event_id",
            "client_order_id",
            "lineage_id",
        )
    }
    for name in identifiers:
        statuses[name] = FieldValidity(True, False, "valid")

    lineage_revision = _strict_int(
        row["lineage_revision"], "lineage_revision", minimum=1
    )
    partial_fill_ordinal = _strict_int(
        row["partial_fill_ordinal"], "partial_fill_ordinal", minimum=1
    )
    partial_fill_qty = _finite_float(
        row["partial_fill_qty_btc"], "partial_fill_qty_btc"
    )
    if partial_fill_qty <= 0.0:
        raise SnapshotContractError("partial fill quantity must be positive")
    statuses["lineage_revision"] = FieldValidity(True, False, "valid")
    statuses["partial_fill_ordinal"] = FieldValidity(True, False, "valid")
    statuses["partial_fill_qty_btc"] = FieldValidity(True, False, "valid")

    visibility_profile = _valid_identifier(
        row["visibility_profile"], "visibility_profile"
    )
    if visibility_profile not in VISIBILITY_PROFILES:
        raise SnapshotContractError("unsupported visibility profile")
    statuses["visibility_profile"] = FieldValidity(True, False, "valid")

    clock_rows = _exact_mapping(
        row["clocks"], label="clocks", expected=set(CLOCK_NAMES)
    )
    clocks = {name: _parse_clock(clock_rows[name], name) for name in CLOCK_NAMES}
    for name, clock in clocks.items():
        statuses[f"clock.{name}"] = clock.status
    for name in ("assignment", "fill_visible", "feature_ready"):
        if not clocks[name].status.valid or clocks[name].status.unknown:
            raise SnapshotContractError(f"causal cutoff clock {name} must be known")
    assignment_ts = clocks["assignment"].ts_ns
    fill_visible_ts = clocks["fill_visible"].ts_ns
    feature_ready_ts = clocks["feature_ready"].ts_ns
    assert assignment_ts is not None
    assert fill_visible_ts is not None
    assert feature_ready_ts is not None
    if not feature_ready_ts <= fill_visible_ts <= assignment_ts:
        raise SnapshotContractError(
            "causal clocks require feature_ready <= fill_visible <= assignment"
        )
    fill_exchange = clocks["fill_exchange"].ts_ns
    fill_receive = clocks["fill_receive"].ts_ns
    if visibility_profile == HISTORICAL_EXCHANGE_EVENT_PROFILE:
        if fill_receive is not None or not clocks["fill_receive"].status.unknown:
            raise SnapshotContractError(
                "historical exchange-event profile cannot claim fill receive time"
            )
    elif not clocks["fill_receive"].status.valid or fill_receive is None:
        raise SnapshotContractError(
            "prospective receive-time profile requires a valid fill receive clock"
        )
    if fill_exchange is not None and fill_receive is not None:
        if fill_exchange > fill_receive:
            raise SnapshotContractError("fill exchange clock exceeds receive clock")
    if fill_receive is not None and fill_receive > fill_visible_ts:
        raise SnapshotContractError("fill receive clock exceeds visible clock")
    if fill_exchange is not None and fill_exchange > fill_visible_ts:
        raise SnapshotContractError("fill exchange clock exceeds visible clock")

    source_rows = _exact_mapping(
        row["sources"], label="sources", expected=set(SOURCE_NAMES)
    )
    sources = {
        name: _parse_source(source_rows[name], name) for name in SOURCE_NAMES
    }
    raw_feature_row = row["feature_row"]
    raw_feature_block = (
        str(raw_feature_row.get("feature_block", ""))
        if isinstance(raw_feature_row, Mapping)
        else ""
    )
    if raw_feature_block in {"M1", "M2"} and (
        sources["market"].generation != sources["depth"].generation
        or sources["market"].feature_generation
        != sources["depth"].feature_generation
    ):
        raise SnapshotContractError(
            "M1/M2 market and depth do not share one atomic generation"
        )
    for name, source in sources.items():
        statuses[f"source.{name}.generation"] = source.status
        statuses[f"source.{name}.cursor"] = source.status
        statuses[f"source.{name}.feature_generation"] = source.status
        statuses[f"source.{name}.feature_cursor"] = source.status

    hash_rows = _exact_mapping(
        row["identity_hashes"],
        label="identity_hashes",
        expected=set(IDENTITY_HASH_FIELDS),
    )
    identity_hashes = {
        name: _valid_sha256(hash_rows[name], f"identity_hashes.{name}")
        for name in IDENTITY_HASH_FIELDS
    }
    for name in identity_hashes:
        statuses[f"identity_hashes.{name}"] = FieldValidity(
            True, False, "valid"
        )

    m0_raw = _exact_mapping(
        row["m0_context"], label="m0_context", expected=set(M0_REQUIRED_FIELDS)
    )
    try:
        m0 = validate_m0_context(m0_raw)
    except FeatureContractError as exc:
        raise SnapshotContractError(f"invalid M0 context: {exc}") from exc
    if int(m0["assignment_ts_ns"]) != assignment_ts:
        raise SnapshotContractError("M0 assignment clock drifted")
    if int(m0["fill_visible_ts_ns"]) != fill_visible_ts:
        raise SnapshotContractError("M0 fill-visible clock drifted")
    if not math.isclose(
        float(m0["fill_qty_btc"]), partial_fill_qty, rel_tol=0.0, abs_tol=1e-12
    ):
        raise SnapshotContractError("lineage partial quantity disagrees with M0")
    if int(m0["partial_fill_ordinal"]) != partial_fill_ordinal:
        raise SnapshotContractError("lineage partial ordinal disagrees with M0")
    for name in M0_REQUIRED_FIELDS:
        unknown = name in M0_NULLABLE_FIELDS and m0[name] is None
        statuses[f"m0.{name}"] = FieldValidity(
            not unknown,
            unknown,
            "event_not_yet_observed" if unknown else "valid",
        )

    block, feature_row, invalid_feature_fields = _normalize_feature_row(
        row["feature_row"],
        m0=m0,
        clocks=clocks,
        sources=sources,
        statuses=statuses,
    )

    fallback_reasons: list[str] = []
    required_clocks = [
        "assignment",
        "fill_exchange",
        "fill_visible",
        "feature_ready",
    ]
    if visibility_profile == PROSPECTIVE_RECEIVE_TIME_PROFILE:
        required_clocks.append("fill_receive")
    for name in required_clocks:
        status = clocks[name].status
        if not status.valid or status.unknown:
            fallback_reasons.append(f"clock.{name}:{status.reason}")
    required_sources = {
        "R0": ("market",),
        "M1": ("market", "depth"),
        "M2": SOURCE_NAMES,
    }[block]
    for name in required_sources:
        status = sources[name].status
        if not status.valid or status.unknown:
            fallback_reasons.append(f"source.{name}:{status.reason}")
    if not bool(feature_row["warmup_admitted"]):
        fallback_reasons.append("warmup_not_admitted")
    if not bool(feature_row["channel_support_valid"]):
        fallback_reasons.append("channel_support_invalid")
    if not bool(feature_row["support_valid"]):
        fallback_reasons.append("feature_support_invalid")
    if invalid_feature_fields:
        fallback_reasons.append(
            f"invalid_policy_fields:{len(set(invalid_feature_fields))}"
        )

    source_bundle_sha256 = _source_bundle_sha256(sources)
    statuses["source_bundle_sha256"] = FieldValidity(True, False, "valid")
    policy_valid = not fallback_reasons
    frozen_hashes = FrozenRow.from_mapping(identity_hashes)
    frozen_m0 = FrozenRow.from_mapping(m0)
    frozen_features = FrozenRow.from_mapping(feature_row)
    policy_input = (
        PolicyInputV2(
            snapshot_id=identifiers["snapshot_id"],
            visibility_profile=visibility_profile,
            feature_block=block,
            source_bundle_sha256=source_bundle_sha256,
            identity_hashes=frozen_hashes,
            m0_context=frozen_m0,
            feature_row=frozen_features,
        )
        if policy_valid
        else None
    )

    return CooldownAssignmentSnapshotV2(
        schema_version=SNAPSHOT_SCHEMA_VERSION,
        identity=IDENTITY,
        snapshot_id=identifiers["snapshot_id"],
        assignment_id=identifiers["assignment_id"],
        fill_event_id=identifiers["fill_event_id"],
        client_order_id=identifiers["client_order_id"],
        lineage_id=identifiers["lineage_id"],
        lineage_revision=lineage_revision,
        partial_fill_ordinal=partial_fill_ordinal,
        partial_fill_qty_btc=partial_fill_qty,
        visibility_profile=visibility_profile,
        receive_time_transport_eligible=(
            visibility_profile == PROSPECTIVE_RECEIVE_TIME_PROFILE
        ),
        clocks=FrozenRow.from_mapping(clocks),
        sources=FrozenRow.from_mapping(sources),
        source_bundle_sha256=source_bundle_sha256,
        identity_hashes=frozen_hashes,
        m0_context=frozen_m0,
        feature_block=block,
        feature_row=frozen_features,
        field_validity=FrozenRow.from_mapping(statuses),
        policy_input_valid=policy_valid,
        policy_input=policy_input,
        fallback_policy_id=None if policy_valid else CONTROL_POLICY_ID,
        fallback_reason=(
            None if policy_valid else ";".join(dict.fromkeys(fallback_reasons))
        ),
        economic_outcomes_read=False,
    )


build_cooldown_assignment_snapshot = capture_cooldown_assignment_snapshot


def snapshot_schema(feature_block: str) -> dict[str, Any]:
    """Return the bounded input schema without reading any outcome."""

    return {
        "identity": IDENTITY,
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "top_level_columns": sorted(_TOP_LEVEL_FIELDS),
        "clock_columns": sorted(_CLOCK_FIELDS),
        "clock_names": list(CLOCK_NAMES),
        "visibility_profiles": list(VISIBILITY_PROFILES),
        "source_columns": sorted(_SOURCE_FIELDS),
        "source_names": list(SOURCE_NAMES),
        "identity_hash_columns": list(IDENTITY_HASH_FIELDS),
        "m0_columns": list(M0_REQUIRED_FIELDS),
        "feature_block": feature_block,
        "feature_columns": list(expected_feature_columns(feature_block)),
        "unknown_columns_allowed": False,
        "economic_outcomes_allowed": False,
        "unsupported_policy_fallback": CONTROL_POLICY_ID,
    }


__all__ = [
    "CLOCK_NAMES",
    "CONTROL_POLICY_ID",
    "CooldownAssignmentSnapshotV2",
    "FieldValidity",
    "FrozenRow",
    "IDENTITY_HASH_FIELDS",
    "HISTORICAL_EXCHANGE_EVENT_PROFILE",
    "PolicyInputV2",
    "PROSPECTIVE_RECEIVE_TIME_PROFILE",
    "SNAPSHOT_SCHEMA_VERSION",
    "SOURCE_NAMES",
    "SUPPORTED_FEATURE_BLOCKS",
    "VISIBILITY_PROFILES",
    "SnapshotContractError",
    "build_cooldown_assignment_snapshot",
    "capture_cooldown_assignment_snapshot",
    "expected_feature_columns",
    "snapshot_schema",
]
