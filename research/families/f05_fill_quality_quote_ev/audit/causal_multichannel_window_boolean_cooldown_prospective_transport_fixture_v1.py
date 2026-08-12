"""Isolated prospective receive-time fixture for F05 cooldown snapshots.

The fixture accepts synthetic or previously recorded row dictionaries.  It is
deliberately not connected to ``ws_handler``, ``MakerEngine``, live config, or
the active restart-aware execution.  A successful fixture audit proves only
that the supplied rows can form one causal ``CooldownAssignmentSnapshotV2``;
it never grants real-capture, action, or live authority.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from numbers import Integral, Real
from pathlib import Path
from types import MappingProxyType
from typing import Any

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    CONTROL_POLICY_ID,
    IDENTITY_HASH_FIELDS,
    PROSPECTIVE_RECEIVE_TIME_PROFILE,
    CooldownAssignmentSnapshotV2,
    SnapshotContractError,
    capture_cooldown_assignment_snapshot,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_prospective_transport_fixture_v1"
REPORT_SCHEMA_VERSION = f"{IDENTITY}.audit.v1"
MANIFEST_SCHEMA_VERSION = f"{IDENTITY}.manifest.v1"
DIRECTORY_AUDIT_SCHEMA_VERSION = f"{IDENTITY}.directory_audit.v1"
DEFAULT_MANIFEST_NAME = "f05_prospective_transport_manifest.v1.json"
SOURCE_NAMES = ("market", "depth", "trade")
REQUIRED_ROLES = (
    "market_source",
    "depth_source",
    "trade_source",
    "private_fill",
    "lifecycle",
    "feature_companion",
    "assignment_companion",
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_OUTCOME_TOKENS = (
    "pnl",
    "profit",
    "reward",
    "markout",
    "terminal_value",
    "economic_outcome",
    "label",
)

_SOURCE_ROW_FIELDS = frozenset(
    {
        "source_name",
        "exchange_ts_ns",
        "receive_ts_ns",
        "feature_ready_ts_ns",
        "generation",
        "cursor",
        "feature_generation",
        "feature_cursor",
        "atomic_snapshot_id",
        "atomic_generation",
        "feature_row_sha256",
        "runtime_identity_sha256",
        "sequence_gap_count",
        "source_gap",
        "recorder_drop_count",
    }
)
_PRIVATE_FILL_ROW_FIELDS = frozenset(
    {
        "snapshot_id",
        "assignment_id",
        "fill_event_id",
        "client_order_id",
        "lifecycle_id",
        "lineage_id",
        "lineage_revision",
        "partial_fill_ordinal",
        "partial_fill_qty_btc",
        "assignment_ts_ns",
        "fill_exchange_ts_ns",
        "fill_receive_ts_ns",
        "fill_visible_ts_ns",
        "feature_ready_ts_ns",
        "feature_row_sha256",
        "m0_context_sha256",
        "market_generation",
        "depth_generation",
        "trade_generation",
        "runtime_identity_sha256",
        "source_gap",
        "recorder_drop_count",
        "writer_drop_count",
    }
)
_LIFECYCLE_ROW_FIELDS = frozenset(
    {
        "event_id",
        "lifecycle_id",
        "client_order_id",
        "lifecycle_sequence",
        "lifecycle_event",
        "event_visibility_ts_ns",
        "event_exchange_ts_ns",
        "event_exchange_clock_valid",
        "source_callback_received_ts_ns",
        "source_callback_id",
        "source_callback_event_ordinal",
        "source_callback_event_count",
        "remaining_quantity_before",
        "remaining_quantity_after",
        "runtime_identity_sha256",
    }
)
_FEATURE_COMPANION_FIELDS = frozenset(
    {
        "snapshot_id",
        "feature_row_sha256",
        "feature_row",
        "runtime_identity_sha256",
    }
)
_ASSIGNMENT_COMPANION_FIELDS = frozenset(
    {
        "snapshot_id",
        "m0_context_sha256",
        "m0_context",
        "identity_hashes",
        "runtime_identity_sha256",
    }
)
_MANIFEST_FIELDS = frozenset(
    {
        "schema_version",
        "identity",
        "capture_id",
        "runtime_identity_sha256",
        "identity_hashes",
        "freshness_contract",
        "required_roles",
        "artifacts",
        "health",
        "manifest_sha256",
    }
)
_ARTIFACT_FIELDS = frozenset({"role", "path", "format", "row_count", "sha256"})
_HEALTH_FIELDS = frozenset(
    {
        "market_tape_drop_count",
        "private_fill_drop_count",
        "lifecycle_writer_drop_count",
        "feature_companion_drop_count",
        "assignment_companion_drop_count",
        "error_count",
    }
)
_FRESHNESS_FIELDS = frozenset(
    {"frozen_before_capture", "max_visible_age_ns_by_source", "contract_sha256"}
)
_SUPPORTED_ARTIFACT_FORMATS = frozenset({"jsonl", "jsonl_gzip", "parquet"})


class ProspectiveTransportFixtureError(ValueError):
    """Raised only for an invalid frozen fixture contract."""


@dataclass(frozen=True, slots=True)
class ProspectiveTransportFixtureContract:
    """Outcome-blind identity and freshness limits supplied by the caller."""

    expected_runtime_identity_sha256: str
    expected_identity_hashes: Mapping[str, str]
    max_visible_age_ns_by_source: Mapping[str, int]
    feature_block: str = "M2"

    def __post_init__(self) -> None:
        runtime_sha = _require_sha256(
            self.expected_runtime_identity_sha256,
            "expected runtime identity",
        )
        identity_hashes = _normalize_identity_hashes(self.expected_identity_hashes)
        if set(self.max_visible_age_ns_by_source) != set(SOURCE_NAMES):
            raise ProspectiveTransportFixtureError(
                "freshness contract must name market, depth, and trade"
            )
        ages: dict[str, int] = {}
        for source in SOURCE_NAMES:
            ages[source] = _strict_int(
                self.max_visible_age_ns_by_source[source],
                f"maximum visible age for {source}",
                minimum=1,
            )
        if self.feature_block not in {"R0", "M1", "M2"}:
            raise ProspectiveTransportFixtureError("unsupported feature block")
        object.__setattr__(self, "expected_runtime_identity_sha256", runtime_sha)
        object.__setattr__(
            self,
            "expected_identity_hashes",
            MappingProxyType(identity_hashes),
        )
        object.__setattr__(
            self,
            "max_visible_age_ns_by_source",
            MappingProxyType(ages),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "expected_runtime_identity_sha256": (self.expected_runtime_identity_sha256),
            "expected_identity_hashes": dict(self.expected_identity_hashes),
            "max_visible_age_ns_by_source": dict(self.max_visible_age_ns_by_source),
            "feature_block": self.feature_block,
        }


@dataclass(frozen=True, slots=True)
class ProspectiveTransportFixtureResult:
    """One fail-closed snapshot envelope and its mechanics-only audit."""

    snapshot: CooldownAssignmentSnapshotV2 | None
    audit: Mapping[str, Any]
    fallback_policy_id: str | None
    fallback_reason: str | None


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _strict_int(value: Any, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise ValueError(f"{label} must be an integer")
    result = int(value)
    if result < minimum:
        raise ValueError(f"{label} must be >= {minimum}")
    return result


def _finite_float(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{label} must be finite")
    return result


def _require_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    if any(character.isspace() for character in value.strip()):
        raise ValueError(f"{label} must not contain whitespace")
    return value.strip()


def _require_sha256(value: Any, label: str) -> str:
    text = _require_text(value, label).lower()
    if _SHA256_RE.fullmatch(text) is None:
        raise ValueError(f"{label} must be an exact SHA256")
    return text


def _require_bool(value: Any, label: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{label} must be bool")
    return value


def _exact_row(
    value: Any,
    *,
    label: str,
    expected: frozenset[str],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a mapping")
    row = {str(key): item for key, item in value.items()}
    missing = sorted(expected - set(row))
    extra = sorted(set(row) - expected)
    if missing or extra:
        raise ValueError(f"{label} schema drifted: missing={missing} extra={extra}")
    return row


def _find_forbidden_outcome_key(value: Any, prefix: str = "") -> str | None:
    if isinstance(value, Mapping):
        for raw_key, nested in value.items():
            key = str(raw_key)
            path = f"{prefix}.{key}" if prefix else key
            lowered = key.lower()
            if any(token in lowered for token in _FORBIDDEN_OUTCOME_TOKENS):
                return path
            found = _find_forbidden_outcome_key(nested, path)
            if found is not None:
                return found
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            found = _find_forbidden_outcome_key(nested, f"{prefix}[{index}]")
            if found is not None:
                return found
    return None


def _normalize_identity_hashes(value: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ProspectiveTransportFixtureError("identity hashes must be a mapping")
    if set(value) != set(IDENTITY_HASH_FIELDS):
        raise ProspectiveTransportFixtureError(
            "identity hashes do not match CooldownAssignmentSnapshotV2"
        )
    return {
        field: _require_sha256(value[field], f"identity hash {field}")
        for field in IDENTITY_HASH_FIELDS
    }


def _source_binding(row: Mapping[str, Any], *, valid: bool, reason: str) -> dict[str, Any]:
    return {
        "generation": _strict_int(row["generation"], "source generation"),
        "cursor": _require_text(row["cursor"], "source cursor"),
        "feature_generation": _strict_int(row["feature_generation"], "source feature generation"),
        "feature_cursor": _require_text(row["feature_cursor"], "source feature cursor"),
        "valid": bool(valid),
        "unknown": False,
        "reason": reason,
    }


def _clock_status(ts_ns: int) -> dict[str, Any]:
    return {"ts_ns": int(ts_ns), "valid": True, "unknown": False, "reason": "valid"}


def _failure_result(
    *,
    contract: ProspectiveTransportFixtureContract,
    gates: Mapping[str, bool],
    failures: list[str],
    details: Mapping[str, Any],
    input_hashes: Mapping[str, str],
    snapshot: CooldownAssignmentSnapshotV2 | None = None,
) -> ProspectiveTransportFixtureResult:
    reason = ";".join(dict.fromkeys(failures)) or "fixture_validation_failed"
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "failed_closed",
        "contract_sha256": canonical_sha256(contract.to_dict()),
        "input_row_sha256": dict(sorted(input_hashes.items())),
        "gates": dict(gates),
        "failure_reasons": list(dict.fromkeys(failures)),
        "details": dict(details),
        "snapshot": (
            {
                "created": True,
                "snapshot_id": snapshot.snapshot_id,
                "policy_input_valid": snapshot.policy_input_valid,
                "source_bundle_sha256": snapshot.source_bundle_sha256,
            }
            if snapshot is not None
            else {"created": False, "policy_input_valid": False}
        ),
        "fallback_policy_id": CONTROL_POLICY_ID,
        "permissions": {
            "fixture_transport_valid": False,
            "real_bounded_capture_authority": False,
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
        "economic_outcomes_read": False,
    }
    report["canonical_report_sha256"] = canonical_sha256(report)
    return ProspectiveTransportFixtureResult(
        snapshot=snapshot,
        audit=MappingProxyType(report),
        fallback_policy_id=CONTROL_POLICY_ID,
        fallback_reason=reason,
    )


def produce_and_audit_prospective_snapshot(
    *,
    contract: ProspectiveTransportFixtureContract,
    market_row: Mapping[str, Any],
    depth_row: Mapping[str, Any],
    trade_row: Mapping[str, Any],
    private_fill_row: Mapping[str, Any],
    lifecycle_row: Mapping[str, Any],
    identity_hashes: Mapping[str, Any],
    m0_context: Mapping[str, Any],
    feature_row: Mapping[str, Any],
) -> ProspectiveTransportFixtureResult:
    """Join recorded rows into one prospective snapshot or fail to control."""

    inputs = {
        "market": market_row,
        "depth": depth_row,
        "trade": trade_row,
        "private_fill": private_fill_row,
        "lifecycle": lifecycle_row,
        "identity_hashes": identity_hashes,
        "m0_context": m0_context,
        "feature_row": feature_row,
    }
    input_hashes: dict[str, str] = {}
    gates: dict[str, bool] = {
        "input_schema_exact": False,
        "economic_fields_absent": False,
        "runtime_identity_match": False,
        "identity_hashes_match": False,
        "source_binding_self_consistent": False,
        "source_clock_ordering": False,
        "feature_ready_before_fill_cutoff": False,
        "source_freshness": False,
        "zero_sequence_gap": False,
        "zero_recorder_writer_drop": False,
        "atomic_market_depth_generation": False,
        "feature_source_binding": False,
        "fill_clock_ordering": False,
        "lifecycle_join": False,
        "snapshot_contract": False,
        "snapshot_policy_input_valid": False,
    }
    failures: list[str] = []
    details: dict[str, Any] = {}

    forbidden = _find_forbidden_outcome_key(inputs)
    if forbidden is not None:
        failures.append(f"economic_field_forbidden:{forbidden}")
        return _failure_result(
            contract=contract,
            gates=gates,
            failures=failures,
            details=details,
            input_hashes=input_hashes,
        )
    gates["economic_fields_absent"] = True

    try:
        sources = {
            "market": _exact_row(market_row, label="market row", expected=_SOURCE_ROW_FIELDS),
            "depth": _exact_row(depth_row, label="depth row", expected=_SOURCE_ROW_FIELDS),
            "trade": _exact_row(trade_row, label="trade row", expected=_SOURCE_ROW_FIELDS),
        }
        fill = _exact_row(
            private_fill_row,
            label="private fill row",
            expected=_PRIVATE_FILL_ROW_FIELDS,
        )
        lifecycle = _exact_row(
            lifecycle_row,
            label="lifecycle row",
            expected=_LIFECYCLE_ROW_FIELDS,
        )
        actual_identity_hashes = _normalize_identity_hashes(identity_hashes)
        input_hashes = {name: canonical_sha256(value) for name, value in inputs.items()}
        gates["input_schema_exact"] = True
    except (ProspectiveTransportFixtureError, TypeError, ValueError) as exc:
        failures.append(f"input_schema_invalid:{exc}")
        return _failure_result(
            contract=contract,
            gates=gates,
            failures=failures,
            details=details,
            input_hashes=input_hashes,
        )

    try:
        for name, row in sources.items():
            if _require_text(row["source_name"], f"{name} source name") != name:
                raise ValueError(f"{name} row source_name drifted")
        runtime_values = {
            _require_sha256(row["runtime_identity_sha256"], "runtime identity")
            for row in (*sources.values(), fill, lifecycle)
        }
        gates["runtime_identity_match"] = runtime_values == {
            contract.expected_runtime_identity_sha256
        }
        if not gates["runtime_identity_match"]:
            failures.append("runtime_identity_mismatch")

        gates["identity_hashes_match"] = actual_identity_hashes == dict(
            contract.expected_identity_hashes
        )
        if not gates["identity_hashes_match"]:
            failures.append("identity_hash_mismatch")

        source_clock_details: dict[str, Any] = {}
        source_bindings_consistent = True
        source_clock_ordered = True
        source_before_cutoff = True
        source_fresh = True
        zero_gap = True
        zero_drop = True
        fill_visible_ts = _strict_int(
            fill["fill_visible_ts_ns"], "fill visible timestamp", minimum=1
        )
        assignment_ts = _strict_int(fill["assignment_ts_ns"], "assignment timestamp", minimum=1)
        for name, row in sources.items():
            exchange = _strict_int(row["exchange_ts_ns"], f"{name} exchange timestamp", minimum=1)
            receive = _strict_int(row["receive_ts_ns"], f"{name} receive timestamp", minimum=1)
            ready = _strict_int(
                row["feature_ready_ts_ns"],
                f"{name} feature-ready timestamp",
                minimum=1,
            )
            generation = _strict_int(row["generation"], f"{name} generation")
            feature_generation = _strict_int(
                row["feature_generation"], f"{name} feature generation"
            )
            cursor = _require_text(row["cursor"], f"{name} cursor")
            feature_cursor = _require_text(row["feature_cursor"], f"{name} feature cursor")
            ordered = exchange <= receive <= ready
            before_cutoff = ready <= fill_visible_ts <= assignment_ts
            visible_age = assignment_ts - ready
            fresh = visible_age <= int(contract.max_visible_age_ns_by_source[name])
            gap_count = _strict_int(row["sequence_gap_count"], f"{name} sequence gap count")
            source_gap = _require_bool(row["source_gap"], f"{name} source gap")
            drops = _strict_int(row["recorder_drop_count"], f"{name} recorder drops")
            binding_consistent = generation == feature_generation and cursor == feature_cursor
            source_bindings_consistent &= binding_consistent
            source_clock_ordered &= ordered
            source_before_cutoff &= before_cutoff
            source_fresh &= fresh
            zero_gap &= gap_count == 0 and not source_gap
            zero_drop &= drops == 0
            source_clock_details[name] = {
                "exchange_to_receive_ns": receive - exchange,
                "receive_to_feature_ready_ns": ready - receive,
                "visible_age_ns": visible_age,
                "maximum_visible_age_ns": int(contract.max_visible_age_ns_by_source[name]),
                "clock_ordered": ordered,
                "before_fill_cutoff": before_cutoff,
                "fresh": fresh,
                "sequence_gap_count": gap_count,
                "recorder_drop_count": drops,
            }
        gates["source_binding_self_consistent"] = source_bindings_consistent
        gates["source_clock_ordering"] = source_clock_ordered
        gates["feature_ready_before_fill_cutoff"] = source_before_cutoff
        gates["source_freshness"] = source_fresh
        gates["zero_sequence_gap"] = zero_gap and not _require_bool(
            fill["source_gap"], "private fill source gap"
        )
        gates["zero_recorder_writer_drop"] = zero_drop and all(
            _strict_int(fill[field], f"private fill {field}") == 0
            for field in ("recorder_drop_count", "writer_drop_count")
        )
        for gate in (
            "source_binding_self_consistent",
            "source_clock_ordering",
            "feature_ready_before_fill_cutoff",
            "source_freshness",
            "zero_sequence_gap",
            "zero_recorder_writer_drop",
        ):
            if not gates[gate]:
                failures.append(gate)
        details["source_clock_transport"] = source_clock_details

        market = sources["market"]
        depth = sources["depth"]
        gates["atomic_market_depth_generation"] = all(
            (
                _require_text(market["atomic_snapshot_id"], "market atomic id")
                == _require_text(depth["atomic_snapshot_id"], "depth atomic id"),
                _strict_int(market["atomic_generation"], "market atomic generation")
                == _strict_int(depth["atomic_generation"], "depth atomic generation"),
                _strict_int(market["generation"], "market generation")
                == _strict_int(depth["generation"], "depth generation"),
                _strict_int(market["feature_generation"], "market feature generation")
                == _strict_int(depth["feature_generation"], "depth feature generation"),
            )
        )
        if not gates["atomic_market_depth_generation"]:
            failures.append("atomic_market_depth_generation")

        feature_sha = canonical_sha256(feature_row)
        m0_sha = canonical_sha256(m0_context)
        source_feature_hashes = {
            _require_sha256(row["feature_row_sha256"], "feature row SHA256")
            for row in sources.values()
        }
        feature_ready_values = {
            _strict_int(row["feature_ready_ts_ns"], "source feature-ready", minimum=1)
            for row in sources.values()
        }
        feature_ready_ts = _strict_int(
            fill["feature_ready_ts_ns"], "fill feature-ready timestamp", minimum=1
        )
        gates["feature_source_binding"] = all(
            (
                source_feature_hashes == {feature_sha},
                _require_sha256(fill["feature_row_sha256"], "fill feature row SHA256")
                == feature_sha,
                _require_sha256(fill["m0_context_sha256"], "fill M0 SHA256") == m0_sha,
                max(feature_ready_values) == feature_ready_ts,
                int(feature_row.get("feature_ready_ts_ns", -1)) == feature_ready_ts,
                int(feature_row.get("decision_ts_ns", -1)) == fill_visible_ts,
                str(feature_row.get("feature_block", "")) == contract.feature_block,
                int(feature_row.get("market_generation", -1))
                == _strict_int(market["feature_generation"], "market feature generation"),
                int(feature_row.get("depth_generation", -1))
                == _strict_int(depth["feature_generation"], "depth feature generation"),
                _strict_int(fill["market_generation"], "fill market generation")
                == _strict_int(market["feature_generation"], "market feature generation"),
                _strict_int(fill["depth_generation"], "fill depth generation")
                == _strict_int(depth["feature_generation"], "depth feature generation"),
                _strict_int(fill["trade_generation"], "fill trade generation")
                == _strict_int(
                    sources["trade"]["feature_generation"],
                    "trade feature generation",
                ),
            )
        )
        if not gates["feature_source_binding"]:
            failures.append("feature_source_binding")

        fill_exchange_ts = _strict_int(
            fill["fill_exchange_ts_ns"], "fill exchange timestamp", minimum=1
        )
        fill_receive_ts = _strict_int(
            fill["fill_receive_ts_ns"], "fill receive timestamp", minimum=1
        )
        gates["fill_clock_ordering"] = (
            fill_exchange_ts <= fill_receive_ts <= fill_visible_ts <= assignment_ts
        ) and feature_ready_ts <= fill_visible_ts
        if not gates["fill_clock_ordering"]:
            failures.append("fill_clock_ordering")

        lifecycle_event = _require_text(lifecycle["lifecycle_event"], "lifecycle event")
        remaining_before = _finite_float(
            lifecycle["remaining_quantity_before"], "remaining quantity before"
        )
        remaining_after = _finite_float(
            lifecycle["remaining_quantity_after"], "remaining quantity after"
        )
        lifecycle_fill_qty = remaining_before - remaining_after
        callback_ordinal = _strict_int(
            lifecycle["source_callback_event_ordinal"],
            "callback event ordinal",
            minimum=1,
        )
        callback_count = _strict_int(
            lifecycle["source_callback_event_count"],
            "callback event count",
            minimum=1,
        )
        gates["lifecycle_join"] = all(
            (
                lifecycle_event in {"partial_fill", "full_fill"},
                _require_text(lifecycle["event_id"], "lifecycle event id")
                == _require_text(fill["fill_event_id"], "fill event id"),
                _require_text(lifecycle["lifecycle_id"], "lifecycle id")
                == _require_text(fill["lifecycle_id"], "fill lifecycle id"),
                _require_text(lifecycle["client_order_id"], "lifecycle client order id")
                == _require_text(fill["client_order_id"], "fill client order id"),
                _strict_int(lifecycle["lifecycle_sequence"], "lifecycle sequence", minimum=1)
                >= _strict_int(fill["partial_fill_ordinal"], "partial fill ordinal", minimum=1),
                _require_bool(
                    lifecycle["event_exchange_clock_valid"],
                    "lifecycle exchange clock validity",
                ),
                _strict_int(
                    lifecycle["event_exchange_ts_ns"],
                    "lifecycle exchange timestamp",
                    minimum=1,
                )
                == fill_exchange_ts,
                _strict_int(
                    lifecycle["source_callback_received_ts_ns"],
                    "lifecycle callback receive timestamp",
                    minimum=1,
                )
                == fill_receive_ts,
                _strict_int(
                    lifecycle["event_visibility_ts_ns"],
                    "lifecycle visibility timestamp",
                    minimum=1,
                )
                == fill_visible_ts,
                callback_ordinal <= callback_count,
                remaining_before > 0.0,
                remaining_after >= 0.0,
                math.isclose(
                    lifecycle_fill_qty,
                    _finite_float(fill["partial_fill_qty_btc"], "partial fill quantity"),
                    rel_tol=0.0,
                    abs_tol=1e-12,
                ),
                lifecycle_event != "full_fill" or remaining_after <= 1e-12,
                lifecycle_event != "partial_fill" or remaining_after > 1e-12,
            )
        )
        if not gates["lifecycle_join"]:
            failures.append("lifecycle_join")
        details["lifecycle_join"] = {
            "lifecycle_id": str(fill["lifecycle_id"]),
            "event_id": str(fill["fill_event_id"]),
            "source_callback_id": str(lifecycle["source_callback_id"]),
            "lifecycle_event": lifecycle_event,
            "lifecycle_fill_qty_btc": lifecycle_fill_qty,
        }

        pre_snapshot_valid = all(
            value
            for name, value in gates.items()
            if name not in {"snapshot_contract", "snapshot_policy_input_valid"}
        )
        source_reason = (
            "valid_prospective_receive_time_source"
            if pre_snapshot_valid
            else "transport_preflight_failed:" + ",".join(dict.fromkeys(failures))
        )
        payload = {
            "snapshot_id": _require_text(fill["snapshot_id"], "snapshot id"),
            "assignment_id": _require_text(fill["assignment_id"], "assignment id"),
            "fill_event_id": _require_text(fill["fill_event_id"], "fill event id"),
            "client_order_id": _require_text(fill["client_order_id"], "client order id"),
            "lineage_id": _require_text(fill["lineage_id"], "lineage id"),
            "lineage_revision": _strict_int(
                fill["lineage_revision"], "lineage revision", minimum=1
            ),
            "partial_fill_ordinal": _strict_int(
                fill["partial_fill_ordinal"], "partial fill ordinal", minimum=1
            ),
            "partial_fill_qty_btc": _finite_float(
                fill["partial_fill_qty_btc"], "partial fill quantity"
            ),
            "visibility_profile": PROSPECTIVE_RECEIVE_TIME_PROFILE,
            "clocks": {
                "assignment": _clock_status(assignment_ts),
                "fill_exchange": _clock_status(fill_exchange_ts),
                "fill_receive": _clock_status(fill_receive_ts),
                "fill_visible": _clock_status(fill_visible_ts),
                "feature_ready": _clock_status(feature_ready_ts),
            },
            "sources": {
                name: _source_binding(
                    row,
                    valid=pre_snapshot_valid,
                    reason=source_reason,
                )
                for name, row in sources.items()
            },
            "identity_hashes": actual_identity_hashes,
            "m0_context": dict(m0_context),
            "feature_row": dict(feature_row),
        }
        snapshot = capture_cooldown_assignment_snapshot(payload)
        gates["snapshot_contract"] = True
        gates["snapshot_policy_input_valid"] = snapshot.policy_input_valid
        if not snapshot.policy_input_valid:
            failures.append(f"snapshot_policy_input_invalid:{snapshot.fallback_reason}")
    except (SnapshotContractError, TypeError, ValueError, KeyError) as exc:
        failures.append(f"snapshot_or_join_invalid:{type(exc).__name__}:{exc}")
        return _failure_result(
            contract=contract,
            gates=gates,
            failures=failures,
            details=details,
            input_hashes=input_hashes,
        )

    if failures or not all(gates.values()):
        return _failure_result(
            contract=contract,
            gates=gates,
            failures=failures,
            details=details,
            input_hashes=input_hashes,
            snapshot=snapshot,
        )

    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "fixture_passed_no_live_authority",
        "contract_sha256": canonical_sha256(contract.to_dict()),
        "input_row_sha256": dict(sorted(input_hashes.items())),
        "gates": gates,
        "failure_reasons": [],
        "details": details,
        "snapshot": {
            "created": True,
            "snapshot_id": snapshot.snapshot_id,
            "policy_input_valid": snapshot.policy_input_valid,
            "visibility_profile": snapshot.visibility_profile,
            "receive_time_transport_eligible": (snapshot.receive_time_transport_eligible),
            "source_bundle_sha256": snapshot.source_bundle_sha256,
            "canonical_snapshot_sha256": canonical_sha256(snapshot.to_dict()),
        },
        "fallback_policy_id": None,
        "permissions": {
            "fixture_transport_valid": True,
            "real_bounded_capture_authority": False,
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
        "economic_outcomes_read": False,
    }
    report["canonical_report_sha256"] = canonical_sha256(report)
    return ProspectiveTransportFixtureResult(
        snapshot=snapshot,
        audit=MappingProxyType(report),
        fallback_policy_id=None,
        fallback_reason=None,
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_artifact_path(root: Path, value: Any) -> Path:
    relative = Path(_require_text(value, "artifact path"))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("artifact path must be a safe relative path")
    path = root / relative
    if path.is_symlink():
        raise ValueError("artifact path must not be a symlink")
    resolved_root = root.resolve()
    resolved = path.resolve()
    if resolved_root not in resolved.parents:
        raise ValueError("artifact path escaped the fixture directory")
    if not resolved.is_file():
        raise ValueError(f"artifact file is missing: {relative.as_posix()}")
    return resolved


def _read_jsonl_rows(path: Path, *, compressed: bool) -> list[dict[str, Any]]:
    opener = gzip.open if compressed else open
    rows: list[dict[str, Any]] = []
    with opener(path, "rt", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path.name}:{line_number}") from exc
            if not isinstance(row, Mapping):
                raise ValueError(f"JSONL row is not an object at {path.name}:{line_number}")
            rows.append({str(key): value for key, value in row.items()})
    return rows


def _read_parquet_rows(path: Path) -> list[dict[str, Any]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - project runtime carries pyarrow
        raise ValueError("Parquet adapter requires pyarrow") from exc
    try:
        return [dict(row) for row in pq.read_table(path).to_pylist()]
    except Exception as exc:
        raise ValueError(f"invalid Parquet artifact: {path.name}") from exc


def _read_artifact_rows(path: Path, artifact_format: str) -> list[dict[str, Any]]:
    if artifact_format == "jsonl":
        return _read_jsonl_rows(path, compressed=False)
    if artifact_format == "jsonl_gzip":
        return _read_jsonl_rows(path, compressed=True)
    if artifact_format == "parquet":
        return _read_parquet_rows(path)
    raise ValueError(f"unsupported artifact format: {artifact_format}")


def _manifest_without_hash(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in manifest.items() if key != "manifest_sha256"}


def _directory_audit_report(
    *,
    status: str,
    root: Path,
    gates: Mapping[str, bool],
    blockers: Sequence[str],
    missing_roles: Sequence[str],
    available_roles: Sequence[str],
    file_audit: Sequence[Mapping[str, Any]],
    row_counts_by_role: Mapping[str, int],
    fill_audits: Sequence[Mapping[str, Any]],
    manifest_sha256: str | None,
    available_unadapted_roles: Sequence[str] = (),
) -> dict[str, Any]:
    passed = status == "fixture_directory_passed_no_live_authority"
    report: dict[str, Any] = {
        "schema_version": DIRECTORY_AUDIT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": status,
        "root": str(root),
        "manifest_sha256": manifest_sha256,
        "required_roles": list(REQUIRED_ROLES),
        "available_roles": sorted(set(available_roles)),
        "available_unadapted_roles": sorted(set(available_unadapted_roles)),
        "missing_roles": sorted(set(missing_roles)),
        "blockers": list(dict.fromkeys(blockers)),
        "gates": dict(gates),
        "files": [dict(row) for row in file_audit],
        "row_counts_by_role": dict(sorted(row_counts_by_role.items())),
        "fill_join_count": len(fill_audits),
        "fill_audits": [dict(row) for row in fill_audits],
        "permissions": {
            "fixture_directory_valid": passed,
            "real_bounded_capture_authority": False,
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
        "economic_outcomes_read": False,
    }
    report["canonical_report_sha256"] = canonical_sha256(report)
    return report


def _legacy_bounded_summary_report(root: Path, summary_path: Path) -> dict[str, Any]:
    blockers = [
        "f05_atomic_manifest_missing",
        "existing_bounded_tapes_are_not_f05_fixture_rows",
        "freshness_contract_missing",
    ]
    available_unadapted: set[str] = set()
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        files = summary.get("files", [])
        if not isinstance(files, list):
            raise ValueError("bounded summary files must be a list")
        for row in files:
            if not isinstance(row, Mapping):
                continue
            path = str(row.get("path", ""))
            if "logs/market_tape/" in path:
                counts = row.get("event_counts", {})
                if isinstance(counts, Mapping):
                    for event_type in ("book", "depth", "trade"):
                        if int(counts.get(event_type, 0) or 0) > 0:
                            available_unadapted.add(f"raw_local_{event_type}_receive_time")
            elif "logs/external_venues/" in path:
                available_unadapted.add("raw_external_venue_receive_time")
        if int(summary.get("file_count", len(files))) == 7:
            available_unadapted.add("bounded_seven_file_integrity_summary")
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        blockers.append(f"bounded_summary_invalid:{type(exc).__name__}:{exc}")
    gates = {
        "atomic_manifest_valid": False,
        "required_roles_complete": False,
        "file_integrity": False,
        "row_counts_match": False,
        "zero_drop_health": False,
        "freshness_contract_frozen": False,
        "fill_joins_unique": False,
        "all_snapshot_audits_passed": False,
    }
    return _directory_audit_report(
        status="failed_closed",
        root=root,
        gates=gates,
        blockers=blockers,
        missing_roles=REQUIRED_ROLES,
        available_roles=(),
        available_unadapted_roles=tuple(available_unadapted),
        file_audit=(),
        row_counts_by_role={},
        fill_audits=(),
        manifest_sha256=None,
    )


def audit_recorded_fixture_directory(
    root: Path,
    *,
    manifest_name: str = DEFAULT_MANIFEST_NAME,
) -> dict[str, Any]:
    """Read one atomic recorded fixture directory and run all fill audits.

    This function is read-only.  A legacy bounded seven-tape directory without
    the F05 manifest returns structured missing roles and can never pass.
    """

    fixture_root = Path(root).expanduser().resolve()
    manifest_path = fixture_root / manifest_name
    if not manifest_path.is_file():
        summary_path = fixture_root / "summary.json"
        if summary_path.is_file():
            return _legacy_bounded_summary_report(fixture_root, summary_path)
        return _directory_audit_report(
            status="failed_closed",
            root=fixture_root,
            gates={
                "atomic_manifest_valid": False,
                "required_roles_complete": False,
                "file_integrity": False,
                "row_counts_match": False,
                "zero_drop_health": False,
                "freshness_contract_frozen": False,
                "fill_joins_unique": False,
                "all_snapshot_audits_passed": False,
            },
            blockers=["f05_atomic_manifest_missing", "freshness_contract_missing"],
            missing_roles=REQUIRED_ROLES,
            available_roles=(),
            file_audit=(),
            row_counts_by_role={},
            fill_audits=(),
            manifest_sha256=None,
        )

    gates = {
        "atomic_manifest_valid": False,
        "required_roles_complete": False,
        "file_integrity": False,
        "row_counts_match": False,
        "zero_drop_health": False,
        "freshness_contract_frozen": False,
        "fill_joins_unique": False,
        "all_snapshot_audits_passed": False,
    }
    blockers: list[str] = []
    file_audit: list[dict[str, Any]] = []
    rows_by_role: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    available_roles: list[str] = []
    missing_roles: list[str] = list(REQUIRED_ROLES)
    observed_manifest_sha: str | None = None

    try:
        manifest_raw = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest = _exact_row(
            manifest_raw,
            label="F05 transport manifest",
            expected=_MANIFEST_FIELDS,
        )
        forbidden = _find_forbidden_outcome_key(manifest)
        if forbidden is not None:
            raise ValueError(f"manifest contains economic field: {forbidden}")
        if manifest["schema_version"] != MANIFEST_SCHEMA_VERSION:
            raise ValueError("manifest schema version drifted")
        if manifest["identity"] != IDENTITY:
            raise ValueError("manifest identity drifted")
        _require_text(manifest["capture_id"], "capture id")
        runtime_sha = _require_sha256(
            manifest["runtime_identity_sha256"], "manifest runtime identity"
        )
        identity_hashes = _normalize_identity_hashes(manifest["identity_hashes"])
        observed_manifest_sha = _require_sha256(manifest["manifest_sha256"], "manifest SHA256")
        if observed_manifest_sha != canonical_sha256(_manifest_without_hash(manifest)):
            raise ValueError("manifest self hash mismatch")
        declared_roles = manifest["required_roles"]
        if not isinstance(declared_roles, list) or tuple(declared_roles) != REQUIRED_ROLES:
            raise ValueError("manifest required roles drifted")
        artifacts = manifest["artifacts"]
        if not isinstance(artifacts, list):
            raise ValueError("manifest artifacts must be a list")
        health = _exact_row(manifest["health"], label="manifest health", expected=_HEALTH_FIELDS)
        gates["zero_drop_health"] = all(
            _strict_int(value, f"health {name}") == 0 for name, value in health.items()
        )
        if not gates["zero_drop_health"]:
            blockers.append("nonzero_drop_or_error_health")
        gates["atomic_manifest_valid"] = True
    except (
        OSError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        ProspectiveTransportFixtureError,
    ) as exc:
        blockers.append(f"manifest_invalid:{type(exc).__name__}:{exc}")
        return _directory_audit_report(
            status="failed_closed",
            root=fixture_root,
            gates=gates,
            blockers=blockers,
            missing_roles=missing_roles,
            available_roles=available_roles,
            file_audit=file_audit,
            row_counts_by_role={},
            fill_audits=(),
            manifest_sha256=observed_manifest_sha,
        )

    freshness_raw = manifest["freshness_contract"]
    contract: ProspectiveTransportFixtureContract | None = None
    if freshness_raw is None:
        blockers.append("freshness_contract_missing")
    else:
        try:
            freshness = _exact_row(
                freshness_raw,
                label="freshness contract",
                expected=_FRESHNESS_FIELDS,
            )
            freshness_body = {
                "frozen_before_capture": freshness["frozen_before_capture"],
                "max_visible_age_ns_by_source": freshness["max_visible_age_ns_by_source"],
            }
            if not _require_bool(freshness["frozen_before_capture"], "freshness frozen flag"):
                raise ValueError("freshness contract was not frozen before capture")
            if _require_sha256(
                freshness["contract_sha256"], "freshness contract SHA256"
            ) != canonical_sha256(freshness_body):
                raise ValueError("freshness contract hash mismatch")
            contract = ProspectiveTransportFixtureContract(
                expected_runtime_identity_sha256=runtime_sha,
                expected_identity_hashes=identity_hashes,
                max_visible_age_ns_by_source=freshness["max_visible_age_ns_by_source"],
                feature_block="M2",
            )
            gates["freshness_contract_frozen"] = True
        except (TypeError, ValueError, ProspectiveTransportFixtureError) as exc:
            blockers.append(f"freshness_contract_invalid:{type(exc).__name__}:{exc}")

    artifact_paths: set[str] = set()
    file_integrity = True
    row_counts_match = True
    for index, artifact_raw in enumerate(manifest["artifacts"]):
        try:
            artifact = _exact_row(
                artifact_raw,
                label=f"artifact {index}",
                expected=_ARTIFACT_FIELDS,
            )
            role = _require_text(artifact["role"], "artifact role")
            if role not in REQUIRED_ROLES:
                raise ValueError(f"unknown artifact role: {role}")
            relative_path = _require_text(artifact["path"], "artifact path")
            if relative_path in artifact_paths:
                raise ValueError("artifact path is duplicated")
            artifact_paths.add(relative_path)
            artifact_format = _require_text(artifact["format"], "artifact format")
            if artifact_format not in _SUPPORTED_ARTIFACT_FORMATS:
                raise ValueError(f"unsupported artifact format: {artifact_format}")
            expected_rows = _strict_int(artifact["row_count"], "artifact row count")
            expected_sha = _require_sha256(artifact["sha256"], "artifact SHA256")
            path = _safe_artifact_path(fixture_root, relative_path)
            actual_sha = _file_sha256(path)
            sha_match = actual_sha == expected_sha
            if not sha_match:
                raise ValueError("artifact SHA256 mismatch")
            rows = _read_artifact_rows(path, artifact_format)
            count_match = len(rows) == expected_rows
            if not count_match:
                row_counts_match = False
                raise ValueError("artifact row count mismatch")
            forbidden = _find_forbidden_outcome_key(rows)
            if forbidden is not None:
                raise ValueError(f"artifact contains economic field: {forbidden}")
            rows_by_role[role].extend(rows)
            available_roles.append(role)
            file_audit.append(
                {
                    "role": role,
                    "path": relative_path,
                    "format": artifact_format,
                    "row_count": len(rows),
                    "sha256": actual_sha,
                    "sha256_match": True,
                    "gzip_valid": artifact_format != "jsonl_gzip" or True,
                    "row_count_match": True,
                }
            )
        except (OSError, EOFError, TypeError, ValueError, gzip.BadGzipFile) as exc:
            file_integrity = False
            blockers.append(f"artifact_invalid:{index}:{type(exc).__name__}:{exc}")
            file_audit.append(
                {
                    "role": str(artifact_raw.get("role", "unknown"))
                    if isinstance(artifact_raw, Mapping)
                    else "unknown",
                    "path": str(artifact_raw.get("path", ""))
                    if isinstance(artifact_raw, Mapping)
                    else "",
                    "valid": False,
                    "failure_reason": f"{type(exc).__name__}:{exc}",
                }
            )
    available_set = set(available_roles)
    missing_roles = [role for role in REQUIRED_ROLES if role not in available_set]
    gates["required_roles_complete"] = not missing_roles
    gates["file_integrity"] = file_integrity
    gates["row_counts_match"] = row_counts_match and file_integrity
    if missing_roles:
        blockers.append("required_roles_missing")

    row_counts_by_role = {role: len(rows_by_role.get(role, [])) for role in REQUIRED_ROLES}
    if (
        not gates["required_roles_complete"]
        or not gates["file_integrity"]
        or not gates["row_counts_match"]
        or contract is None
    ):
        return _directory_audit_report(
            status="failed_closed",
            root=fixture_root,
            gates=gates,
            blockers=blockers,
            missing_roles=missing_roles,
            available_roles=available_roles,
            file_audit=file_audit,
            row_counts_by_role=row_counts_by_role,
            fill_audits=(),
            manifest_sha256=observed_manifest_sha,
        )

    try:
        feature_by_snapshot: dict[str, dict[str, Any]] = {}
        for raw in rows_by_role["feature_companion"]:
            row = _exact_row(
                raw,
                label="feature companion row",
                expected=_FEATURE_COMPANION_FIELDS,
            )
            snapshot_id = _require_text(row["snapshot_id"], "feature snapshot id")
            if snapshot_id in feature_by_snapshot:
                raise ValueError("duplicate feature companion join")
            if (
                _require_sha256(row["runtime_identity_sha256"], "feature runtime identity")
                != contract.expected_runtime_identity_sha256
            ):
                raise ValueError("feature companion runtime identity mismatch")
            if _require_sha256(
                row["feature_row_sha256"], "feature companion SHA256"
            ) != canonical_sha256(row["feature_row"]):
                raise ValueError("feature companion payload hash mismatch")
            feature_by_snapshot[snapshot_id] = row

        assignment_by_snapshot: dict[str, dict[str, Any]] = {}
        for raw in rows_by_role["assignment_companion"]:
            row = _exact_row(
                raw,
                label="assignment companion row",
                expected=_ASSIGNMENT_COMPANION_FIELDS,
            )
            snapshot_id = _require_text(row["snapshot_id"], "assignment snapshot id")
            if snapshot_id in assignment_by_snapshot:
                raise ValueError("duplicate assignment companion join")
            if (
                _require_sha256(row["runtime_identity_sha256"], "assignment runtime identity")
                != contract.expected_runtime_identity_sha256
            ):
                raise ValueError("assignment companion runtime identity mismatch")
            if _require_sha256(
                row["m0_context_sha256"], "assignment M0 SHA256"
            ) != canonical_sha256(row["m0_context"]):
                raise ValueError("assignment companion payload hash mismatch")
            if _normalize_identity_hashes(row["identity_hashes"]) != dict(
                contract.expected_identity_hashes
            ):
                raise ValueError("assignment identity hashes mismatch")
            assignment_by_snapshot[snapshot_id] = row

        private_by_event: dict[str, dict[str, Any]] = {}
        private_by_snapshot: dict[str, dict[str, Any]] = {}
        for raw in rows_by_role["private_fill"]:
            row = _exact_row(raw, label="private fill row", expected=_PRIVATE_FILL_ROW_FIELDS)
            event_id = _require_text(row["fill_event_id"], "private fill event id")
            snapshot_id = _require_text(row["snapshot_id"], "private fill snapshot id")
            if event_id in private_by_event or snapshot_id in private_by_snapshot:
                raise ValueError("duplicate private fill join")
            private_by_event[event_id] = row
            private_by_snapshot[snapshot_id] = row

        lifecycle_by_event: dict[str, dict[str, Any]] = {}
        for raw in rows_by_role["lifecycle"]:
            row = _exact_row(raw, label="lifecycle row", expected=_LIFECYCLE_ROW_FIELDS)
            event_id = _require_text(row["event_id"], "lifecycle event id")
            if event_id in lifecycle_by_event:
                raise ValueError("duplicate lifecycle fill join")
            lifecycle_by_event[event_id] = row

        source_indexes: dict[str, dict[tuple[str, int], dict[str, Any]]] = {}
        for source, role in (
            ("market", "market_source"),
            ("depth", "depth_source"),
            ("trade", "trade_source"),
        ):
            index: dict[tuple[str, int], dict[str, Any]] = {}
            for raw in rows_by_role[role]:
                row = _exact_row(raw, label=f"{source} source row", expected=_SOURCE_ROW_FIELDS)
                if _require_text(row["source_name"], "source name") != source:
                    raise ValueError(f"{role} contains another source")
                key = (
                    _require_sha256(row["feature_row_sha256"], "feature row SHA256"),
                    _strict_int(row["feature_generation"], "feature generation"),
                )
                if key in index:
                    raise ValueError(f"duplicate {source} source join")
                index[key] = row
            source_indexes[source] = index

        fill_audits: list[dict[str, Any]] = []
        for snapshot_id, fill in sorted(private_by_snapshot.items()):
            feature_companion = feature_by_snapshot.get(snapshot_id)
            assignment_companion = assignment_by_snapshot.get(snapshot_id)
            lifecycle = lifecycle_by_event.get(str(fill["fill_event_id"]))
            if feature_companion is None:
                raise ValueError("private fill lacks feature companion join")
            if assignment_companion is None:
                raise ValueError("private fill lacks assignment companion join")
            if lifecycle is None:
                raise ValueError("private fill lacks lifecycle join")
            feature_sha = _require_sha256(fill["feature_row_sha256"], "fill feature row SHA256")
            source_rows: dict[str, dict[str, Any]] = {}
            for source, generation_field in (
                ("market", "market_generation"),
                ("depth", "depth_generation"),
                ("trade", "trade_generation"),
            ):
                key = (
                    feature_sha,
                    _strict_int(fill[generation_field], generation_field),
                )
                source_row = source_indexes[source].get(key)
                if source_row is None:
                    raise ValueError(f"private fill lacks {source} source join")
                source_rows[source] = source_row
            result = produce_and_audit_prospective_snapshot(
                contract=contract,
                market_row=source_rows["market"],
                depth_row=source_rows["depth"],
                trade_row=source_rows["trade"],
                private_fill_row=fill,
                lifecycle_row=lifecycle,
                identity_hashes=assignment_companion["identity_hashes"],
                m0_context=assignment_companion["m0_context"],
                feature_row=feature_companion["feature_row"],
            )
            fill_audits.append(
                {
                    "snapshot_id": snapshot_id,
                    "fill_event_id": str(fill["fill_event_id"]),
                    "status": str(result.audit["status"]),
                    "canonical_report_sha256": str(result.audit["canonical_report_sha256"]),
                    "policy_input_valid": bool(
                        result.snapshot is not None and result.snapshot.policy_input_valid
                    ),
                    "fallback_reason": result.fallback_reason,
                }
            )
        if set(feature_by_snapshot) != set(private_by_snapshot):
            raise ValueError("orphan or missing feature companion rows")
        if set(assignment_by_snapshot) != set(private_by_snapshot):
            raise ValueError("orphan or missing assignment companion rows")
        if set(lifecycle_by_event) != set(private_by_event):
            raise ValueError("orphan or missing lifecycle fill rows")
        gates["fill_joins_unique"] = True
        gates["all_snapshot_audits_passed"] = bool(fill_audits) and all(
            row["status"] == "fixture_passed_no_live_authority" and row["policy_input_valid"]
            for row in fill_audits
        )
        if not gates["all_snapshot_audits_passed"]:
            blockers.append("one_or_more_snapshot_audits_failed")
    except (
        KeyError,
        TypeError,
        ValueError,
        ProspectiveTransportFixtureError,
    ) as exc:
        blockers.append(f"fill_join_invalid:{type(exc).__name__}:{exc}")
        fill_audits = []

    passed = all(gates.values()) and not blockers
    return _directory_audit_report(
        status=("fixture_directory_passed_no_live_authority" if passed else "failed_closed"),
        root=fixture_root,
        gates=gates,
        blockers=blockers,
        missing_roles=missing_roles,
        available_roles=available_roles,
        file_audit=file_audit,
        row_counts_by_role=row_counts_by_role,
        fill_audits=fill_audits,
        manifest_sha256=observed_manifest_sha,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--manifest-name", default=DEFAULT_MANIFEST_NAME)
    args = parser.parse_args(argv)
    report = audit_recorded_fixture_directory(
        args.root,
        manifest_name=str(args.manifest_name),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "fixture_directory_passed_no_live_authority" else 2


__all__ = [
    "DEFAULT_MANIFEST_NAME",
    "DIRECTORY_AUDIT_SCHEMA_VERSION",
    "IDENTITY",
    "MANIFEST_SCHEMA_VERSION",
    "REPORT_SCHEMA_VERSION",
    "REQUIRED_ROLES",
    "ProspectiveTransportFixtureContract",
    "ProspectiveTransportFixtureError",
    "ProspectiveTransportFixtureResult",
    "canonical_sha256",
    "audit_recorded_fixture_directory",
    "main",
    "produce_and_audit_prospective_snapshot",
]


if __name__ == "__main__":
    raise SystemExit(main())
