"""Freeze and admit the mechanics-only F07 40-day lifecycle inputs.

This module validates immutable input artifacts before the event-lockstep
runner is allowed to start.  It never reads economic outcomes and never runs
the 40-day replay or lockstep itself.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import tempfile
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from data_paths import relocate_marketdata_path
from execution.order_lifecycle_journal_v2 import (
    ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
    ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
    validate_order_lifecycle_journal_v2_payload,
)
from execution.order_lifecycle_journal_writer_v2 import (
    ORDER_LIFECYCLE_JOURNAL_WRITER_V2_HEALTH_VERSION,
    ORDER_LIFECYCLE_JOURNAL_WRITER_V2_IDENTITY_VERSION,
    ORDER_LIFECYCLE_JOURNAL_WRITER_V2_PART_VERSION,
)
from execution.order_lifecycle_quantity_contract import (
    ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID,
    TERMINAL_REMAINDER_ABS_TOLERANCE_BTC,
    persisted_terminal_remainder_is_zero,
)
from research.families.f07_active_order_continuation.audit.order_lifecycle_v2_cpp_event_stream_binding import (
    CPP_EVENT_STREAM_BINDING_SCHEMA_VERSION,
    CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
    projection_schema_sha256,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "f07_order_lifecycle_v2_40day_input_admission_v1"
FROZEN_IDENTITY_SCHEMA_VERSION = "f07_order_lifecycle_v2_40day_input_identity.v1"
PANEL_ADMISSION_SCHEMA_VERSION = "f07_order_lifecycle_v2_40day_admission.v1"
DAY_ADMISSION_SCHEMA_VERSION = "f07_order_lifecycle_v2_day_admission.v1"
REPORT_SCHEMA_VERSION = "f07_order_lifecycle_v2_40day_preflight_report.v1"
REPORT_ENVELOPE_SCHEMA_VERSION = "f07_order_lifecycle_v2_40day_preflight_result.v1"
REPLAY_CAPABILITY_SCHEMA_VERSION = "f07_order_lifecycle_v2_replay_capabilities.v1"
CPP_BINDING_SCHEMA_VERSION = CPP_EVENT_STREAM_BINDING_SCHEMA_VERSION
REQUIRED_DAY_COUNT = 40
DEFAULT_IDENTITY = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "order_lifecycle_v2_40day_input_admission_v1_contract_20260805.json"
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_ECONOMIC_FRAGMENTS = (
    "pnl",
    "reward",
    "markout",
    "campaign_value",
    "profit",
)
_ALLOWED_OUTCOME_KEYS = frozenset({"economic_outcomes_read"})
_TERMINAL_LEGACY_EVENTS = frozenset(
    {
        "full_fill",
        "cancel_ack",
        "reject",
        "expiry",
        "expired",
        "day_end_censor",
        "local_shutdown_censor",
    }
)
_EXCHANGE_CLOCK_LEGACY_EVENTS = frozenset(
    {
        "activate",
        "partial_fill",
        "full_fill",
        "cancel_reject",
        "cancel_rejected",
        "cancel_ack",
        "reject",
        "expiry",
        "expired",
    }
)
_LEGACY_REQUIRED_COLUMNS = (
    "symbol",
    "order_id",
    "event_type",
    "event_ts_ns",
    "event_seq",
    "event_reason",
    "state_before",
    "state_after",
    "order_submit_ts_ns",
    "order_qty",
    "remaining_qty",
)
_LEGACY_DUAL_CLOCK_COLUMNS = (
    "event_visibility_ts_ns",
    "event_exchange_ts_ns",
)
_PART_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "batch_id",
        "runtime_identity_sha256",
        "journal_schema_version",
        "journal_schema_sha256",
        "storage_format",
        "data_file",
        "data_sha256",
        "row_count",
        "lifecycle_id",
        "client_order_id",
        "source_callback_id",
        "source_callback_type",
        "first_lifecycle_sequence",
        "last_lifecycle_sequence",
        "first_event_id",
        "last_event_id",
        "event_ids",
        "checkpoint_before",
        "checkpoint_after",
        "contains_local_shutdown_censor",
        "committed_ts_ns",
        "economic_outcomes_read",
    }
)
_WRITER_IDENTITY_KEYS = frozenset(
    {
        "schema_version",
        "journal_schema_version",
        "journal_schema_sha256",
        "storage_format",
        "runtime_identity",
        "runtime_identity_sha256",
        "economic_outcomes_read",
        "q90_action_authorized",
    }
)
_DAY_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "day",
        "frozen_input_identity_sha256",
        "interval_identity_sha256",
        "baseline_runtime_identity_sha256",
        "admission_state",
        "atomic_publish_method",
        "journal_v2",
        "legacy_trace",
        "market_data_artifacts",
        "producer_capabilities",
        "economic_outcomes_read",
        "canonical_manifest_sha256",
    }
)
_PANEL_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "identity",
        "frozen_input_identity_sha256",
        "admission_state",
        "atomic_publish_method",
        "ordered_utc_days",
        "day_manifests",
        "economic_outcomes_read",
        "lockstep_executed",
        "canonical_manifest_sha256",
    }
)
_ADMISSION_CONTRACT_KEYS = frozenset(
    {
        "replay_session_scope",
        "lifecycle_sequence_starts_at_one_per_target_day",
        "prospective_live_epoch_transport_supported",
        "daily_source_role_contracts",
    }
)


class InputAdmissionError(ValueError):
    """Raised when an immutable admission artifact is malformed."""

    def __init__(self, code: str, detail: str, *, artifact: str = "") -> None:
        super().__init__(detail)
        self.code = str(code)
        self.detail = str(detail)
        self.artifact = str(artifact)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def canonical_document_sha256(value: Mapping[str, object], hash_field: str) -> str:
    payload = dict(value)
    payload.pop(hash_field, None)
    return canonical_sha256(payload)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def journal_schema_sha256() -> str:
    return canonical_sha256(
        {
            "schema_version": ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION,
            "columns": list(ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS),
        }
    )


def legacy_required_schema_sha256() -> str:
    return canonical_sha256(
        {
            "required_columns": list(_LEGACY_REQUIRED_COLUMNS),
            "dual_clock_columns": list(_LEGACY_DUAL_CLOCK_COLUMNS),
        }
    )


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise InputAdmissionError(
            "json_integrity_failure",
            f"cannot parse JSON: {type(exc).__name__}:{exc}",
            artifact=str(path),
        ) from exc
    if not isinstance(payload, dict):
        raise InputAdmissionError(
            "json_object_required",
            "JSON artifact must be an object",
            artifact=str(path),
        )
    return payload


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and bool(_SHA256_RE.fullmatch(value))


def _required_id(value: object, *, label: str) -> str:
    normalized = str(value).strip()
    if not normalized or normalized.lower() in {"nan", "none", "null"}:
        raise InputAdmissionError("required_identity_missing", f"{label} is required")
    return normalized


def _require_exact_keys(
    value: Mapping[str, object], expected: set[str] | frozenset[str], *, label: str
) -> None:
    actual = set(map(str, value))
    if actual != set(expected):
        raise InputAdmissionError(
            "schema_mismatch",
            f"{label} keys differ: missing={sorted(set(expected) - actual)} "
            f"extra={sorted(actual - set(expected))}",
        )


def _assert_mechanics_only(value: object, *, path: str = "root") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            lowered = str(key).lower()
            if lowered not in _ALLOWED_OUTCOME_KEYS and any(
                fragment in lowered for fragment in _FORBIDDEN_ECONOMIC_FRAGMENTS
            ):
                raise InputAdmissionError(
                    "economic_field_forbidden",
                    f"mechanics-only admission contains forbidden field {path}.{key}",
                )
            _assert_mechanics_only(nested, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, nested in enumerate(value):
            _assert_mechanics_only(nested, path=f"{path}[{index}]")


def _parse_utc(value: object, *, label: str) -> datetime:
    raw = str(value)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InputAdmissionError("invalid_utc_timestamp", f"{label}: {raw}") from exc
    if parsed.tzinfo != timezone.utc:
        raise InputAdmissionError("non_utc_timestamp", f"{label} must be UTC")
    return parsed


def _day_bounds(day: str) -> tuple[datetime, datetime, datetime]:
    try:
        start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise InputAdmissionError("invalid_utc_day", f"invalid UTC day: {day}") from exc
    return start - timedelta(days=1), start, start + timedelta(days=1)


def expected_day_interval(day: str) -> dict[str, object]:
    warmup, start, end = _day_bounds(day)
    payload: dict[str, object] = {
        "day": day,
        "warmup_interval": {
            "start_utc": warmup.isoformat().replace("+00:00", "Z"),
            "end_utc": start.isoformat().replace("+00:00", "Z"),
            "denominator": False,
        },
        "target_interval": {
            "start_utc": start.isoformat().replace("+00:00", "Z"),
            "end_utc": end.isoformat().replace("+00:00", "Z"),
            "denominator": True,
        },
    }
    payload["interval_identity_sha256"] = canonical_sha256(payload)
    return payload


def _resolve_path(value: object, *, base: Path) -> Path:
    raw = Path(_required_id(value, label="artifact path")).expanduser()
    if not raw.is_absolute():
        raw = base / raw
    relocated = relocate_marketdata_path(raw)
    return relocated.resolve()


def _validate_artifact(
    artifact: Mapping[str, object],
    *,
    base: Path,
    label: str,
    verify_payload: bool = True,
) -> Path:
    required = {"path", "sha256", "size_bytes"}
    if set(artifact) != required:
        raise InputAdmissionError(
            "artifact_identity_schema_mismatch",
            f"{label} artifact identity must contain {sorted(required)}",
        )
    path = _resolve_path(artifact["path"], base=base)
    if not path.is_file():
        raise InputAdmissionError("artifact_missing", f"{label} is missing", artifact=str(path))
    if path.name.startswith(".") or ".partial" in path.name or path.suffix == ".tmp":
        raise InputAdmissionError(
            "non_atomic_artifact_name",
            f"{label} still has a temporary filename",
            artifact=str(path),
        )
    expected_size = int(artifact["size_bytes"])
    if path.stat().st_size != expected_size:
        raise InputAdmissionError(
            "artifact_size_mismatch",
            f"{label} size expected={expected_size} actual={path.stat().st_size}",
            artifact=str(path),
        )
    expected_hash = str(artifact["sha256"])
    if not _is_sha256(expected_hash):
        raise InputAdmissionError("invalid_sha256", f"{label} SHA256 is malformed")
    if verify_payload:
        actual_hash = file_sha256(path)
        if actual_hash != expected_hash:
            raise InputAdmissionError(
                "artifact_sha256_mismatch",
                f"{label} SHA256 expected={expected_hash} actual={actual_hash}",
                artifact=str(path),
            )
    return path


def artifact_identity(path: str | Path) -> dict[str, object]:
    resolved = Path(path).expanduser().resolve()
    return {
        "path": str(resolved),
        "sha256": file_sha256(resolved),
        "size_bytes": int(resolved.stat().st_size),
    }


def _parquet_schema_sha256(path: Path) -> str:
    schema = pq.ParquetFile(path).schema_arrow
    return canonical_sha256(
        [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": bool(field.nullable),
            }
            for field in schema
        ]
    )


def _validate_gzip(path: Path) -> None:
    try:
        with gzip.open(path, "rb") as handle:
            for _chunk in iter(lambda: handle.read(1024 * 1024), b""):
                pass
    except Exception as exc:
        raise InputAdmissionError(
            "gzip_integrity_failure",
            f"gzip CRC/decompression failed: {type(exc).__name__}:{exc}",
            artifact=str(path),
        ) from exc


def _validate_canonical_document(
    payload: Mapping[str, object], *, hash_field: str, label: str
) -> str:
    claimed = str(payload.get(hash_field, ""))
    actual = canonical_document_sha256(payload, hash_field)
    if claimed != actual:
        raise InputAdmissionError(
            "canonical_hash_mismatch",
            f"{label} canonical hash expected={claimed} actual={actual}",
        )
    return actual


def validate_frozen_identity(
    identity: Mapping[str, object], *, base: Path = ROOT
) -> dict[str, object]:
    _assert_mechanics_only(identity, path="frozen_identity")
    if identity.get("schema_version") != FROZEN_IDENTITY_SCHEMA_VERSION:
        raise InputAdmissionError("identity_schema_mismatch", "unsupported frozen identity")
    if identity.get("identity") != IDENTITY:
        raise InputAdmissionError("identity_name_mismatch", "unexpected frozen identity name")
    canonical = _validate_canonical_document(
        identity,
        hash_field="canonical_identity_sha256",
        label="frozen identity",
    )
    panel = identity.get("panel")
    if not isinstance(panel, Mapping):
        raise InputAdmissionError("panel_missing", "frozen identity panel is required")
    days = tuple(map(str, panel.get("ordered_utc_days") or ()))
    if len(days) != REQUIRED_DAY_COUNT or days != tuple(sorted(set(days))):
        raise InputAdmissionError(
            "frozen_panel_invalid",
            "frozen panel must contain exactly 40 unique chronological UTC days",
        )
    intervals = panel.get("day_intervals")
    if not isinstance(intervals, Sequence) or isinstance(intervals, (str, bytes)):
        raise InputAdmissionError("day_intervals_missing", "day intervals must be an array")
    expected_intervals = [expected_day_interval(day) for day in days]
    if list(intervals) != expected_intervals:
        raise InputAdmissionError(
            "day_interval_identity_mismatch",
            "target or D-1 warmup intervals drifted from ordered UTC days",
        )

    baseline = identity.get("baseline_runtime_identity")
    if not isinstance(baseline, Mapping):
        raise InputAdmissionError("baseline_identity_missing", "baseline runtime identity missing")
    claimed_baseline = str(baseline.get("canonical_sha256", ""))
    baseline_payload = dict(baseline)
    baseline_payload.pop("canonical_sha256", None)
    if claimed_baseline != canonical_sha256(baseline_payload):
        raise InputAdmissionError(
            "baseline_runtime_identity_hash_mismatch",
            "baseline runtime identity canonical hash mismatch",
        )
    for field in ("q90_action_enabled", "economic_outcomes_read"):
        if bool(baseline.get(field)):
            raise InputAdmissionError(
                "mechanics_permission_drift",
                f"baseline runtime {field} must remain false",
            )
    if baseline.get("initial_state_mode") != "daily_fresh_start":
        raise InputAdmissionError(
            "baseline_initial_state_scope_mismatch",
            "40-day daily lockstep requires daily_fresh_start initial state",
        )
    if baseline.get("replay_session_scope") != "fresh_start_per_target_day":
        raise InputAdmissionError(
            "baseline_replay_session_scope_mismatch",
            "40-day daily lockstep requires fresh_start_per_target_day",
        )
    baseline_paths: dict[str, Path] = {}
    for field in ("pointer_artifact", "identity_artifact"):
        artifact = baseline.get(field)
        if not isinstance(artifact, Mapping):
            raise InputAdmissionError("baseline_artifact_missing", f"{field} is required")
        baseline_paths[field] = _validate_artifact(artifact, base=base, label=field)
    pointer_payload = _load_json(baseline_paths["pointer_artifact"])
    identity_payload = _load_json(baseline_paths["identity_artifact"])
    if (
        pointer_payload.get("baseline_id") != baseline.get("baseline_id")
        or identity_payload.get("baseline_id") != baseline.get("baseline_id")
    ):
        raise InputAdmissionError(
            "baseline_artifact_identity_mismatch",
            "baseline pointer, identity, and frozen runtime name different baselines",
        )
    if pointer_payload.get("identity_sha256") != file_sha256(
        baseline_paths["identity_artifact"]
    ):
        raise InputAdmissionError(
            "baseline_pointer_identity_hash_mismatch",
            "baseline pointer does not bind the admitted identity bytes",
        )
    if pointer_payload.get("live_config_sha256") != baseline.get(
        "operational_config_sha256"
    ):
        raise InputAdmissionError(
            "baseline_config_identity_mismatch",
            "baseline pointer config hash differs from frozen runtime identity",
        )
    config = identity_payload.get("config")
    model = identity_payload.get("model")
    p3 = identity_payload.get("p3")
    if not all(isinstance(value, Mapping) for value in (config, model, p3)):
        raise InputAdmissionError(
            "baseline_identity_contract_missing",
            "baseline identity must expose config, model, and P3 contracts",
        )
    if config["sha256"] != baseline.get("operational_config_sha256"):
        raise InputAdmissionError(
            "baseline_config_identity_mismatch",
            "baseline identity config hash differs from frozen runtime identity",
        )
    if bool(config.get("dynamic_fill_hazard_action_enabled", True)):
        raise InputAdmissionError(
            "baseline_q90_action_scope_mismatch",
            "admitted mechanics baseline requires q90 action OFF",
        )
    if model["bundle_meta_sha256"] != baseline.get("model_bundle_sha256"):
        raise InputAdmissionError(
            "baseline_model_identity_mismatch",
            "baseline model bundle differs from frozen runtime identity",
        )
    if model["feature_dag_sha256"] != baseline.get("feature_dag_sha256"):
        raise InputAdmissionError(
            "baseline_feature_dag_identity_mismatch",
            "baseline Feature DAG differs from frozen runtime identity",
        )
    if p3["sha256"] != baseline.get("p3_sha256"):
        raise InputAdmissionError(
            "baseline_p3_identity_mismatch",
            "baseline P3 differs from frozen runtime identity",
        )

    implementation = identity.get("implementation_identities")
    if not isinstance(implementation, Mapping) or not implementation:
        raise InputAdmissionError(
            "implementation_identity_missing", "implementation identities are required"
        )
    for relative, expected in implementation.items():
        if not _is_sha256(expected):
            raise InputAdmissionError("invalid_sha256", f"invalid implementation SHA: {relative}")
        path = _resolve_path(relative, base=base)
        if not path.is_file():
            raise InputAdmissionError(
                "implementation_missing", f"implementation file missing: {relative}"
            )
        actual = file_sha256(path)
        if actual != str(expected):
            raise InputAdmissionError(
                "implementation_hash_mismatch",
                f"{relative}: expected={expected} actual={actual}",
                artifact=str(path),
            )

    schemas = identity.get("schema_identities")
    if not isinstance(schemas, Mapping):
        raise InputAdmissionError("schema_identity_missing", "schema identities are required")
    if schemas.get("journal_v2_schema_version") != ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION:
        raise InputAdmissionError("journal_schema_version_mismatch", "journal-v2 schema drift")
    if schemas.get("journal_v2_schema_sha256") != journal_schema_sha256():
        raise InputAdmissionError("journal_schema_hash_mismatch", "journal-v2 schema hash drift")
    if schemas.get("legacy_required_schema_sha256") != legacy_required_schema_sha256():
        raise InputAdmissionError("legacy_schema_hash_mismatch", "legacy schema contract drift")

    source = identity.get("market_data_source_identities")
    if not isinstance(source, Mapping):
        raise InputAdmissionError("source_identity_missing", "market source identities missing")
    source_artifacts = source.get("artifacts")
    if not isinstance(source_artifacts, Sequence) or not source_artifacts:
        raise InputAdmissionError("source_artifacts_missing", "source artifacts are required")
    for index, artifact in enumerate(source_artifacts):
        if not isinstance(artifact, Mapping):
            raise InputAdmissionError("artifact_identity_schema_mismatch", "source artifact invalid")
        _validate_artifact(artifact, base=base, label=f"source_identity[{index}]")
    claimed_source = str(source.get("canonical_sha256", ""))
    source_payload = dict(source)
    source_payload.pop("canonical_sha256", None)
    if claimed_source != canonical_sha256(source_payload):
        raise InputAdmissionError("source_identity_hash_mismatch", "market source hash mismatch")

    admission = identity.get("admission_contract")
    if not isinstance(admission, Mapping) or set(admission) != _ADMISSION_CONTRACT_KEYS:
        raise InputAdmissionError(
            "admission_contract_schema_mismatch",
            "frozen admission contract keys differ",
        )
    if admission["replay_session_scope"] != "fresh_start_per_target_day":
        raise InputAdmissionError(
            "replay_session_scope_mismatch",
            "daily lockstep only supports fresh_start_per_target_day",
        )
    if not bool(admission["lifecycle_sequence_starts_at_one_per_target_day"]):
        raise InputAdmissionError(
            "daily_lifecycle_sequence_origin_not_frozen",
            "daily lifecycle sequence must restart at one for each target day",
        )
    if bool(admission["prospective_live_epoch_transport_supported"]):
        raise InputAdmissionError(
            "live_transport_permission_drift",
            "daily fresh-start input identity cannot authorize live epoch transport",
        )
    source_role_contracts = admission["daily_source_role_contracts"]
    if not isinstance(source_role_contracts, Sequence) or isinstance(
        source_role_contracts, (str, bytes)
    ):
        raise InputAdmissionError(
            "daily_source_role_contract_missing",
            "daily source role contracts must be an array",
        )

    permissions = identity.get("permissions")
    if not isinstance(permissions, Mapping) or any(bool(value) for value in permissions.values()):
        raise InputAdmissionError(
            "permission_drift", "frozen input identity cannot grant downstream permissions"
        )
    return {
        "canonical_identity_sha256": canonical,
        "days": days,
        "intervals": expected_intervals,
        "baseline_runtime_identity_sha256": claimed_baseline,
        "market_data_source_identity_sha256": claimed_source,
    }


def load_frozen_identity(path: str | Path = DEFAULT_IDENTITY) -> tuple[dict[str, Any], Path]:
    resolved = Path(path).expanduser().resolve()
    payload = _load_json(resolved)
    validate_frozen_identity(payload, base=ROOT)
    return payload, resolved


def expected_runtime_identity(
    identity: Mapping[str, object], day: str
) -> dict[str, object]:
    validated = validate_frozen_identity(identity, base=ROOT)
    interval_by_day = {
        str(row["day"]): row for row in validated["intervals"]  # type: ignore[index]
    }
    if day not in interval_by_day:
        raise InputAdmissionError("unknown_day", f"day is outside frozen panel: {day}")
    baseline = identity["baseline_runtime_identity"]
    schemas = identity["schema_identities"]
    return {
        "identity_schema_version": "f07_order_lifecycle_v2_40day_runtime_identity.v1",
        "frozen_input_identity": IDENTITY,
        "frozen_input_identity_sha256": validated["canonical_identity_sha256"],
        "baseline_id": baseline["baseline_id"],
        "baseline_runtime_identity_sha256": validated[
            "baseline_runtime_identity_sha256"
        ],
        "operational_config_sha256": baseline["operational_config_sha256"],
        "model_bundle_sha256": baseline["model_bundle_sha256"],
        "p3_sha256": baseline["p3_sha256"],
        "feature_dag_sha256": baseline["feature_dag_sha256"],
        "execution_abi": baseline["execution_abi"],
        "runtime_code_identity_sha256": canonical_sha256(
            identity["implementation_identities"]
        ),
        "replay_adapter_id": "order_lifecycle_journal_v2.python_replay_adapter.v1",
        "journal_schema_version": schemas["journal_v2_schema_version"],
        "journal_schema_sha256": schemas["journal_v2_schema_sha256"],
        "legacy_trace_schema_contract_sha256": schemas[
            "legacy_required_schema_sha256"
        ],
        "market_data_source_identity_sha256": validated[
            "market_data_source_identity_sha256"
        ],
        "day": day,
        "warmup_interval": interval_by_day[day]["warmup_interval"],
        "target_interval": interval_by_day[day]["target_interval"],
        "initial_state_mode": baseline["initial_state_mode"],
        "replay_session_scope": baseline["replay_session_scope"],
        "q90_action_enabled": False,
        "economic_outcomes_read": False,
        "q90_action_authorized": False,
    }


def _failure(
    code: str,
    detail: str,
    *,
    day: str = "",
    artifact: str = "",
) -> dict[str, str]:
    return {
        "code": str(code),
        "day": str(day),
        "artifact": str(artifact),
        "detail": str(detail),
    }


def _source_artifact_path(
    value: Mapping[str, object], *, base: Path, label: str
) -> Path:
    required = {
        "role",
        "path",
        "sha256",
        "size_bytes",
        "format",
        "compression",
        "interval_start_utc",
        "interval_end_utc",
    }
    if set(value) != required:
        raise InputAdmissionError(
            "source_artifact_schema_mismatch",
            f"{label} source artifact keys differ",
        )
    path = _validate_artifact(
        {key: value[key] for key in ("path", "sha256", "size_bytes")},
        base=base,
        label=label,
    )
    compression = str(value["compression"])
    if compression not in {"none", "gzip"}:
        raise InputAdmissionError(
            "unsupported_compression", f"{label} compression={compression}"
        )
    if compression == "gzip":
        _validate_gzip(path)
    elif str(value["format"]) == "parquet":
        try:
            _metadata = pq.ParquetFile(path).metadata
        except Exception as exc:
            raise InputAdmissionError(
                "parquet_integrity_failure",
                f"{label}: {type(exc).__name__}:{exc}",
                artifact=str(path),
            ) from exc
    start = _parse_utc(value["interval_start_utc"], label=f"{label}.interval_start")
    end = _parse_utc(value["interval_end_utc"], label=f"{label}.interval_end")
    if start >= end:
        raise InputAdmissionError(
            "source_interval_invalid", f"{label} interval is empty or reversed"
        )
    return path


def _validate_source_coverage(
    artifacts: Sequence[Mapping[str, object]],
    *,
    identity: Mapping[str, object],
    day: str,
    base: Path,
) -> dict[str, int]:
    interval = expected_day_interval(day)
    required = identity["admission_contract"]["daily_source_role_contracts"]
    role_rows: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for index, artifact in enumerate(artifacts):
        _source_artifact_path(artifact, base=base, label=f"market_data_artifacts[{index}]")
        role_rows[str(artifact["role"])].append(artifact)

    counts: dict[str, int] = {}
    for contract in required:
        role = str(contract["role"])
        rows = role_rows.get(role, [])
        minimum = int(contract["minimum_files"])
        if len(rows) < minimum:
            raise InputAdmissionError(
                "source_role_file_count_below_minimum",
                f"{day} {role}: required={minimum} actual={len(rows)}",
            )
        coverage = str(contract["coverage"])
        if coverage == "warmup_exact":
            expected_start = _parse_utc(
                interval["warmup_interval"]["start_utc"], label="warmup start"
            )
            expected_end = _parse_utc(
                interval["warmup_interval"]["end_utc"], label="warmup end"
            )
        elif coverage == "target_exact":
            expected_start = _parse_utc(
                interval["target_interval"]["start_utc"], label="target start"
            )
            expected_end = _parse_utc(
                interval["target_interval"]["end_utc"], label="target end"
            )
        elif coverage == "warmup_and_target_exact":
            expected_start = _parse_utc(
                interval["warmup_interval"]["start_utc"], label="warmup start"
            )
            expected_end = _parse_utc(
                interval["target_interval"]["end_utc"], label="target end"
            )
        else:
            raise InputAdmissionError(
                "source_role_coverage_contract_unknown", f"unsupported coverage={coverage}"
            )
        ranges = sorted(
            (
                _parse_utc(row["interval_start_utc"], label=f"{role} start"),
                _parse_utc(row["interval_end_utc"], label=f"{role} end"),
            )
            for row in rows
        )
        cursor = expected_start
        for start, end in ranges:
            if start != cursor:
                raise InputAdmissionError(
                    "source_role_interval_gap_or_overlap",
                    f"{day} {role}: expected next={cursor.isoformat()} actual={start.isoformat()}",
                )
            cursor = end
        if cursor != expected_end:
            raise InputAdmissionError(
                "source_role_interval_incomplete",
                f"{day} {role}: expected end={expected_end.isoformat()} actual={cursor.isoformat()}",
            )
        counts[role] = len(rows)
    return counts


def _read_part(
    manifest_path: Path,
    *,
    expected_runtime_sha: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], Path]:
    record = _load_json(manifest_path)
    if set(record) != _PART_MANIFEST_KEYS:
        raise InputAdmissionError(
            "journal_part_manifest_schema_mismatch",
            "journal part manifest keys differ",
            artifact=str(manifest_path),
        )
    if record["schema_version"] != ORDER_LIFECYCLE_JOURNAL_WRITER_V2_PART_VERSION:
        raise InputAdmissionError(
            "journal_part_version_mismatch", "unsupported journal part manifest"
        )
    if record["runtime_identity_sha256"] != expected_runtime_sha:
        raise InputAdmissionError(
            "journal_runtime_identity_mismatch", "part runtime identity differs"
        )
    if record["journal_schema_version"] != ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION:
        raise InputAdmissionError("journal_schema_version_mismatch", "part schema drift")
    if record["journal_schema_sha256"] != journal_schema_sha256():
        raise InputAdmissionError("journal_schema_hash_mismatch", "part schema hash drift")
    if record["storage_format"] != "parquet":
        raise InputAdmissionError("journal_storage_format_mismatch", "Parquet is required")
    if bool(record["economic_outcomes_read"]):
        raise InputAdmissionError("economic_outcome_access", "journal part read economics")
    batch_id = str(record["batch_id"])
    if manifest_path.name != f"part-{batch_id}.manifest.json":
        raise InputAdmissionError(
            "journal_part_filename_mismatch", "part manifest filename is not content addressed"
        )
    data_path = (manifest_path.parent / str(record["data_file"])).resolve()
    if data_path.name != f"part-{batch_id}.parquet" or not data_path.is_file():
        raise InputAdmissionError(
            "journal_part_payload_missing", "journal part payload is missing", artifact=str(data_path)
        )
    if file_sha256(data_path) != str(record["data_sha256"]):
        raise InputAdmissionError(
            "journal_part_payload_sha256_mismatch",
            "journal part payload hash differs",
            artifact=str(data_path),
        )
    try:
        table = pq.read_table(data_path)
    except Exception as exc:
        raise InputAdmissionError(
            "journal_part_parquet_integrity_failure",
            f"{type(exc).__name__}:{exc}",
            artifact=str(data_path),
        ) from exc
    if tuple(table.column_names) != tuple(ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS):
        raise InputAdmissionError(
            "journal_part_arrow_schema_mismatch", "journal-v2 Parquet columns differ"
        )
    rows = table.to_pylist()
    if len(rows) != int(record["row_count"]) or not rows:
        raise InputAdmissionError(
            "journal_part_row_count_mismatch",
            f"manifest={record['row_count']} parquet={len(rows)}",
        )
    for row in rows:
        validate_order_lifecycle_journal_v2_payload(row)
    event_ids = [str(row["event_id"]) for row in rows]
    if event_ids != list(map(str, record["event_ids"])):
        raise InputAdmissionError("journal_part_event_identity_mismatch", "event IDs differ")
    if (
        rows[0]["lifecycle_id"] != record["lifecycle_id"]
        or rows[0]["client_order_id"] != record["client_order_id"]
        or rows[0]["source_callback_id"] != record["source_callback_id"]
        or rows[0]["source_callback_type"] != record["source_callback_type"]
        or rows[0]["lifecycle_sequence"] != int(record["first_lifecycle_sequence"])
        or rows[-1]["lifecycle_sequence"] != int(record["last_lifecycle_sequence"])
        or rows[0]["event_id"] != record["first_event_id"]
        or rows[-1]["event_id"] != record["last_event_id"]
    ):
        raise InputAdmissionError(
            "journal_part_manifest_payload_mismatch", "part manifest disagrees with payload"
        )
    expected_batch_id = canonical_sha256(
        {
            "schema_version": ORDER_LIFECYCLE_JOURNAL_WRITER_V2_PART_VERSION,
            "lifecycle_id": rows[0]["lifecycle_id"],
            "event_ids": event_ids,
            "source_callback_id": rows[0]["source_callback_id"],
            "checkpoint_after": dict(record["checkpoint_after"]),
        }
    )
    if batch_id != expected_batch_id:
        raise InputAdmissionError(
            "journal_part_content_address_mismatch", "batch ID is not content addressed"
        )
    return record, rows, data_path


def _validate_journal(
    section: Mapping[str, object],
    *,
    day: str,
    expected_runtime: Mapping[str, object],
    base: Path,
) -> dict[str, object]:
    required = {
        "session_root",
        "runtime_identity_artifact",
        "health_artifact",
        "live_health_artifact",
        "part_manifest_artifacts",
        "expected_part_count",
        "expected_row_count",
        "expected_order_count",
        "expected_event_count",
    }
    if set(section) != required:
        raise InputAdmissionError("journal_section_schema_mismatch", "journal section keys differ")
    session_root = _resolve_path(section["session_root"], base=base)
    if not session_root.is_dir():
        raise InputAdmissionError(
            "journal_session_missing", "journal session root is missing", artifact=str(session_root)
        )
    if list(session_root.rglob("*.partial*")) or list(session_root.rglob("*.tmp")):
        raise InputAdmissionError(
            "journal_partial_artifact_present", "journal session contains a partial artifact"
        )
    runtime_artifact = section["runtime_identity_artifact"]
    health_artifact = section["health_artifact"]
    if not isinstance(runtime_artifact, Mapping) or not isinstance(health_artifact, Mapping):
        raise InputAdmissionError("journal_artifact_identity_missing", "journal identity/health missing")
    runtime_path = _validate_artifact(
        runtime_artifact, base=base, label="journal runtime identity"
    )
    runtime_wrapper = _load_json(runtime_path)
    if set(runtime_wrapper) != _WRITER_IDENTITY_KEYS:
        raise InputAdmissionError(
            "writer_identity_schema_mismatch", "writer runtime identity keys differ"
        )
    if runtime_wrapper["schema_version"] != ORDER_LIFECYCLE_JOURNAL_WRITER_V2_IDENTITY_VERSION:
        raise InputAdmissionError("writer_identity_version_mismatch", "writer identity version drift")
    if runtime_wrapper["journal_schema_version"] != ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION:
        raise InputAdmissionError("journal_schema_version_mismatch", "writer schema version drift")
    if runtime_wrapper["journal_schema_sha256"] != journal_schema_sha256():
        raise InputAdmissionError("journal_schema_hash_mismatch", "writer schema hash drift")
    if runtime_wrapper["storage_format"] != "parquet":
        raise InputAdmissionError("journal_storage_format_mismatch", "writer must use Parquet")
    if bool(runtime_wrapper["economic_outcomes_read"]) or bool(
        runtime_wrapper["q90_action_authorized"]
    ):
        raise InputAdmissionError("mechanics_permission_drift", "writer permissions drifted")
    runtime = dict(runtime_wrapper["runtime_identity"])
    if runtime != dict(expected_runtime):
        raise InputAdmissionError(
            "daily_runtime_identity_mismatch", "writer runtime identity differs from frozen day"
        )
    runtime_sha = canonical_sha256(runtime)
    if runtime_wrapper["runtime_identity_sha256"] != runtime_sha:
        raise InputAdmissionError(
            "daily_runtime_identity_hash_mismatch", "writer runtime hash differs"
        )

    part_artifacts = section["part_manifest_artifacts"]
    if not isinstance(part_artifacts, Sequence) or isinstance(part_artifacts, (str, bytes)):
        raise InputAdmissionError("journal_parts_missing", "journal part manifest list is required")
    if len(part_artifacts) != int(section["expected_part_count"]):
        raise InputAdmissionError(
            "journal_part_count_mismatch", "declared part count differs from artifact list"
        )
    listed_paths: list[Path] = []
    for index, artifact in enumerate(part_artifacts):
        if not isinstance(artifact, Mapping):
            raise InputAdmissionError("artifact_identity_schema_mismatch", "invalid part artifact")
        listed_paths.append(
            _validate_artifact(artifact, base=base, label=f"journal part manifest[{index}]")
        )
    discovered = sorted((session_root / "parts").glob("part-*.manifest.json"))
    if set(listed_paths) != set(path.resolve() for path in discovered):
        raise InputAdmissionError(
            "journal_part_manifest_set_mismatch", "listed and discovered part manifests differ"
        )

    records: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    payload_paths: set[Path] = set()
    for path in listed_paths:
        record, part_rows, data_path = _read_part(
            path, expected_runtime_sha=runtime_sha
        )
        records.append(record)
        rows.extend(part_rows)
        payload_paths.add(data_path)
    discovered_payloads = set((session_root / "parts").glob("part-*.parquet"))
    if payload_paths != {path.resolve() for path in discovered_payloads}:
        raise InputAdmissionError(
            "journal_orphan_payload", "journal has an unmanifested or missing payload"
        )
    if len(rows) != int(section["expected_row_count"]):
        raise InputAdmissionError(
            "journal_row_count_mismatch",
            f"declared={section['expected_row_count']} actual={len(rows)}",
        )
    event_ids = [str(row["event_id"]) for row in rows]
    if len(event_ids) != len(set(event_ids)):
        raise InputAdmissionError("journal_duplicate_event_id", "journal event IDs are duplicated")
    lifecycle_sequences: set[tuple[str, int]] = set()
    clients: set[str] = set()
    event_counts: Counter[str] = Counter()
    terminal_sub_lot: list[dict[str, object]] = []
    target_start = _parse_utc(expected_runtime["target_interval"]["start_utc"], label="target")
    target_end = _parse_utc(expected_runtime["target_interval"]["end_utc"], label="target")
    for row in rows:
        visibility = datetime.fromtimestamp(
            int(row["event_visibility_ts_ns"]) / 1_000_000_000.0,
            tz=timezone.utc,
        )
        if not (target_start <= visibility < target_end):
            raise InputAdmissionError(
                "journal_event_outside_target_day", f"journal event lies outside {day}"
            )
        pair = (str(row["lifecycle_id"]), int(row["lifecycle_sequence"]))
        if pair in lifecycle_sequences:
            raise InputAdmissionError(
                "journal_duplicate_lifecycle_sequence", "lifecycle sequence is duplicated"
            )
        lifecycle_sequences.add(pair)
        clients.add(str(row["client_order_id"]))
        event = str(row["lifecycle_event"])
        event_counts[event] += 1
        remaining = float(row["remaining_quantity_after"])
        if event == "full_fill" and not persisted_terminal_remainder_is_zero(
            remaining
        ):
            terminal_sub_lot.append(
                {
                    "client_order_id": str(row["client_order_id"]),
                    "remaining_quantity": remaining,
                }
            )

    by_lifecycle: dict[str, list[dict[str, Any]]] = defaultdict(list)
    rows_by_lifecycle: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        rows_by_lifecycle[str(row["lifecycle_id"])].append(row)
    for lifecycle_id, lifecycle_rows in rows_by_lifecycle.items():
        ordered_rows = sorted(
            lifecycle_rows, key=lambda row: int(row["lifecycle_sequence"])
        )
        observed_sequences = [
            int(row["lifecycle_sequence"]) for row in ordered_rows
        ]
        if observed_sequences != list(range(1, len(ordered_rows) + 1)):
            raise InputAdmissionError(
                "journal_lifecycle_sequence_not_fresh_start_contiguous",
                (
                    f"{lifecycle_id} sequence must start at one and remain contiguous; "
                    f"observed={observed_sequences}"
                ),
            )
        terminals = [
            row
            for row in ordered_rows
            if str(row["terminal_observation"]) != "NONE"
        ]
        if len(terminals) != 1 or terminals[0] is not ordered_rows[-1]:
            raise InputAdmissionError(
                "journal_terminal_cardinality_invalid",
                f"{lifecycle_id} must have exactly one final terminal/censor observation",
            )
    for record in records:
        by_lifecycle[str(record["lifecycle_id"])].append(record)
    for lifecycle_id, lifecycle_records in by_lifecycle.items():
        ordered = sorted(lifecycle_records, key=lambda row: int(row["first_lifecycle_sequence"]))
        last_sequence = 0
        last_event_id = ""
        for record in ordered:
            before = record["checkpoint_before"]
            if (
                int(before["last_emitted_sequence"]) != last_sequence
                or str(before["last_event_id"]) != last_event_id
            ):
                raise InputAdmissionError(
                    "journal_cursor_chain_broken", f"cursor chain broken for {lifecycle_id}"
                )
            last_sequence = int(record["last_lifecycle_sequence"])
            last_event_id = str(record["last_event_id"])

    health_path = _validate_artifact(
        health_artifact, base=base, label="journal writer health"
    )
    health = _load_json(health_path)
    if health.get("schema_version") != ORDER_LIFECYCLE_JOURNAL_WRITER_V2_HEALTH_VERSION:
        raise InputAdmissionError("writer_health_schema_mismatch", "writer health version drift")
    if health.get("runtime_identity_sha256") != runtime_sha:
        raise InputAdmissionError("writer_health_identity_mismatch", "writer health identity differs")
    if int(health.get("rows_dropped", -1)) != 0:
        raise InputAdmissionError("writer_drop_nonzero", "writer rows_dropped must be zero")
    if int(health.get("error_count", -1)) != 0:
        raise InputAdmissionError("writer_error_nonzero", "writer error_count must be zero")
    if not bool(health.get("formal_collection_valid", False)):
        raise InputAdmissionError("writer_collection_invalid", "writer collection is invalid")
    if not bool(health.get("closed", False)) or health.get("state") != "closed":
        raise InputAdmissionError("writer_not_flushed_closed", "writer must be closed after flush")
    if int(health.get("rows_committed", -1)) != len(rows):
        raise InputAdmissionError("writer_health_row_mismatch", "health row count differs")
    if int(health.get("callbacks_committed", -1)) != len(records):
        raise InputAdmissionError("writer_health_part_mismatch", "health callback count differs")
    batch_ids = {str(record["batch_id"]) for record in records}
    if rows and (
        int(health.get("last_flush_ts_ns", 0)) <= 0
        or str(health.get("last_flush_batch_id", "")) not in batch_ids
    ):
        raise InputAdmissionError("writer_flush_missing", "writer flush identity is missing")
    if int(health.get("orphan_payload_count", -1)) != 0:
        raise InputAdmissionError("writer_orphan_payload_nonzero", "writer reports orphan payload")

    live_health_artifact = section["live_health_artifact"]
    if live_health_artifact is not None:
        if not isinstance(live_health_artifact, Mapping):
            raise InputAdmissionError("live_health_identity_invalid", "live health identity invalid")
        live_health_path = _validate_artifact(
            live_health_artifact, base=base, label="live writer health"
        )
        live_health = _load_json(live_health_path)
        if int(live_health.get("drop_count", -1)) != 0:
            raise InputAdmissionError("live_writer_drop_nonzero", "live writer drop_count nonzero")
        if int(live_health.get("error_count", -1)) != 0:
            raise InputAdmissionError("live_writer_error_nonzero", "live writer error_count nonzero")
        if not bool(live_health.get("formal_collection_valid", False)):
            raise InputAdmissionError("live_writer_collection_invalid", "live writer invalid")

    if len(clients) != int(section["expected_order_count"]):
        raise InputAdmissionError("journal_order_count_mismatch", "journal order count differs")
    if len(event_ids) != int(section["expected_event_count"]):
        raise InputAdmissionError("journal_event_count_mismatch", "journal event count differs")
    return {
        "rows": rows,
        "client_order_ids": clients,
        "event_counts": dict(sorted(event_counts.items())),
        "cancel_reject_count": int(event_counts["cancel_rejected"]),
        "terminal_sub_lot_rows": terminal_sub_lot,
        "row_count": len(rows),
        "order_count": len(clients),
        "part_count": len(records),
        "runtime_identity_sha256": runtime_sha,
    }


def _legacy_client_order_id(row: Mapping[str, object]) -> str:
    submit_ns = int(row["order_submit_ts_ns"])
    if submit_ns <= 0 or submit_ns % 1_000_000 != 0:
        raise InputAdmissionError(
            "legacy_order_identity_invalid", "legacy submit timestamp is not millisecond aligned"
        )
    return (
        f"replay-{str(row['symbol']).upper()}-{int(row['order_id'])}-"
        f"{submit_ns // 1_000_000}"
    )


def _validate_legacy(
    section: Mapping[str, object], *, day: str, base: Path
) -> dict[str, object]:
    required = {
        "artifact",
        "schema_sha256",
        "row_count",
        "order_count",
        "event_count",
        "clock_semantics",
    }
    if set(section) != required:
        raise InputAdmissionError("legacy_section_schema_mismatch", "legacy section keys differ")
    artifact = section["artifact"]
    if not isinstance(artifact, Mapping):
        raise InputAdmissionError("legacy_artifact_missing", "legacy trace artifact is required")
    path = _validate_artifact(artifact, base=base, label="legacy trace")
    try:
        parquet = pq.ParquetFile(path)
        names = tuple(parquet.schema_arrow.names)
    except Exception as exc:
        raise InputAdmissionError(
            "legacy_parquet_integrity_failure",
            f"{type(exc).__name__}:{exc}",
            artifact=str(path),
        ) from exc
    missing = sorted(set(_LEGACY_REQUIRED_COLUMNS) - set(names))
    if missing:
        raise InputAdmissionError(
            "legacy_required_columns_missing", f"legacy trace missing columns: {missing}"
        )
    actual_schema = _parquet_schema_sha256(path)
    if actual_schema != str(section["schema_sha256"]):
        raise InputAdmissionError(
            "legacy_schema_sha256_mismatch",
            f"declared={section['schema_sha256']} actual={actual_schema}",
        )
    columns = list(_LEGACY_REQUIRED_COLUMNS)
    columns.extend(column for column in _LEGACY_DUAL_CLOCK_COLUMNS if column in names)
    if "day" in names:
        columns.append("day")
    rows = pq.read_table(path, columns=columns).to_pylist()
    if len(rows) != int(section["row_count"]):
        raise InputAdmissionError(
            "legacy_row_count_mismatch",
            f"declared={section['row_count']} actual={len(rows)}",
        )
    event_sequences: set[int] = set()
    clients: set[str] = set()
    cancel_reject_count = 0
    sub_lot_rows: list[dict[str, object]] = []
    terminal_counts_by_client: Counter[str] = Counter()
    has_visibility = "event_visibility_ts_ns" in names
    has_exchange = "event_exchange_ts_ns" in names
    missing_visibility = 0
    missing_required_exchange = 0
    mixed_clock_rows = 0
    for row in rows:
        sequence = int(row["event_seq"])
        if sequence <= 0 or sequence in event_sequences:
            raise InputAdmissionError(
                "legacy_duplicate_event_sequence", "legacy event sequence is invalid or duplicated"
            )
        event_sequences.add(sequence)
        event_ts_ns = int(row["event_ts_ns"])
        row_day = datetime.fromtimestamp(
            event_ts_ns / 1_000_000_000.0, tz=timezone.utc
        ).strftime("%Y-%m-%d")
        if row_day != day or ("day" in row and str(row["day"]) != day):
            raise InputAdmissionError(
                "legacy_event_outside_target_day", f"legacy event lies outside {day}"
            )
        client = _legacy_client_order_id(row)
        clients.add(client)
        event = str(row["event_type"]).strip().lower()
        if event in {"cancel_reject", "cancel_rejected"}:
            cancel_reject_count += 1
        if event in _TERMINAL_LEGACY_EVENTS:
            terminal_counts_by_client[client] += 1
        remaining = float(row["remaining_qty"])
        if (
            event == "full_fill" or str(row["state_after"]).strip().lower() == "filled"
        ) and not persisted_terminal_remainder_is_zero(remaining):
            sub_lot_rows.append(
                {"client_order_id": client, "remaining_quantity": remaining}
            )
        if has_visibility:
            visibility = row.get("event_visibility_ts_ns")
            if visibility is None or int(visibility) <= 0:
                missing_visibility += 1
        if event in _EXCHANGE_CLOCK_LEGACY_EVENTS:
            exchange = row.get("event_exchange_ts_ns") if has_exchange else None
            if exchange is None or int(exchange) <= 0:
                missing_required_exchange += 1
            elif has_visibility and row.get("event_visibility_ts_ns") is not None:
                if int(exchange) > int(row["event_visibility_ts_ns"]):
                    mixed_clock_rows += 1

    invalid_terminal_clients = sorted(
        client for client in clients if terminal_counts_by_client[client] != 1
    )
    if invalid_terminal_clients:
        raise InputAdmissionError(
            "legacy_terminal_cardinality_invalid",
            "legacy order must have exactly one terminal/censor event: "
            f"{invalid_terminal_clients[:5]}",
        )

    if has_visibility and has_exchange:
        if missing_visibility == 0 and missing_required_exchange == 0 and mixed_clock_rows == 0:
            observed_clock_semantics = "explicit_dual_clock"
        else:
            observed_clock_semantics = "mixed_clock_coverage"
    elif not has_visibility and not has_exchange:
        observed_clock_semantics = "single_event_ts_only"
    else:
        observed_clock_semantics = "mixed_clock_columns"
    if observed_clock_semantics != str(section["clock_semantics"]):
        raise InputAdmissionError(
            "legacy_clock_semantics_mismatch",
            f"declared={section['clock_semantics']} observed={observed_clock_semantics}",
        )
    if len(clients) != int(section["order_count"]):
        raise InputAdmissionError("legacy_order_count_mismatch", "legacy order count differs")
    if len(rows) != int(section["event_count"]):
        raise InputAdmissionError("legacy_event_count_mismatch", "legacy event count differs")
    return {
        "rows": rows,
        "client_order_ids": clients,
        "row_count": len(rows),
        "order_count": len(clients),
        "event_count": len(rows),
        "clock_semantics": observed_clock_semantics,
        "missing_visibility_count": missing_visibility,
        "missing_required_exchange_count": missing_required_exchange,
        "exchange_after_visibility_count": mixed_clock_rows,
        "cancel_reject_count": cancel_reject_count,
        "terminal_sub_lot_rows": sub_lot_rows,
    }


def _validate_capabilities(
    section: Mapping[str, object],
    *,
    identity: Mapping[str, object],
    journal: Mapping[str, object],
    legacy: Mapping[str, object],
    base: Path,
) -> dict[str, object]:
    required = {
        "replay_capability_artifact",
        "replay_session_scope",
        "carry_in_lifecycle_count",
        "left_truncation_supported",
        "prospective_live_epoch_transport_authorized",
        "cancel_reject_route_supported",
        "cancel_reject_observed_event_count",
        "sub_lot_terminal_remainder_contract",
        "sub_lot_terminal_remainder_count",
        "cpp_event_stream_binding",
    }
    if set(section) != required:
        raise InputAdmissionError(
            "producer_capability_schema_mismatch", "producer capabilities keys differ"
        )
    capability_artifact = section["replay_capability_artifact"]
    if not isinstance(capability_artifact, Mapping):
        raise InputAdmissionError(
            "replay_capability_artifact_missing", "replay capability artifact is required"
        )
    capability_path = _validate_artifact(
        capability_artifact, base=base, label="replay producer capability"
    )
    capability = _load_json(capability_path)
    if capability.get("schema_version") != REPLAY_CAPABILITY_SCHEMA_VERSION:
        raise InputAdmissionError(
            "replay_capability_schema_mismatch", "replay capability version drift"
        )
    implementation_sha = canonical_sha256(identity["implementation_identities"])
    if capability.get("runtime_code_identity_sha256") != implementation_sha:
        raise InputAdmissionError(
            "replay_capability_code_identity_mismatch", "capability code identity differs"
        )
    replay_scope = str(section["replay_session_scope"])
    if replay_scope != "fresh_start_per_target_day":
        raise InputAdmissionError(
            "replay_session_scope_mismatch",
            "daily lockstep only supports fresh_start_per_target_day",
        )
    if capability.get("replay_session_scope") != replay_scope:
        raise InputAdmissionError(
            "replay_capability_session_scope_mismatch",
            "daily and static replay session scopes differ",
        )
    if int(section["carry_in_lifecycle_count"]) != 0:
        raise InputAdmissionError(
            "daily_fresh_start_carry_in_forbidden",
            "daily fresh-start lockstep cannot admit carry-in lifecycles",
        )
    if bool(section["left_truncation_supported"]):
        raise InputAdmissionError(
            "daily_left_truncation_scope_drift",
            "daily fresh-start lockstep does not own left-truncated carry-in",
        )
    if bool(section["prospective_live_epoch_transport_authorized"]):
        raise InputAdmissionError(
            "live_transport_permission_drift",
            "daily lockstep cannot authorize prospective live epoch transport",
        )
    route_supported = bool(section["cancel_reject_route_supported"])
    if route_supported != bool(capability.get("cancel_reject_route_supported", False)):
        raise InputAdmissionError(
            "cancel_reject_capability_mismatch", "daily and static cancel-reject support differ"
        )
    observed_cancel_reject = min(
        int(journal["cancel_reject_count"]), int(legacy["cancel_reject_count"])
    )
    if int(section["cancel_reject_observed_event_count"]) != observed_cancel_reject:
        raise InputAdmissionError(
            "cancel_reject_observed_count_mismatch", "cancel-reject count differs"
        )
    sub_lot_count = len(journal["terminal_sub_lot_rows"]) + len(
        legacy["terminal_sub_lot_rows"]
    )
    if int(section["sub_lot_terminal_remainder_count"]) != sub_lot_count:
        raise InputAdmissionError(
            "sub_lot_terminal_remainder_count_mismatch", "sub-lot count differs"
        )
    contract = str(section["sub_lot_terminal_remainder_contract"])
    if contract != ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID:
        raise InputAdmissionError(
            "sub_lot_terminal_contract_unsupported", "unsupported sub-lot terminal contract"
        )

    cpp_section = section["cpp_event_stream_binding"]
    if not isinstance(cpp_section, Mapping) or set(cpp_section) != {
        "status",
        "artifact",
        "abi_version",
    }:
        raise InputAdmissionError(
            "cpp_event_stream_binding_schema_mismatch", "C++ binding section invalid"
        )
    cpp_artifact = cpp_section["artifact"]
    if not isinstance(cpp_artifact, Mapping):
        raise InputAdmissionError("cpp_event_stream_binding_missing", "C++ binding missing")
    cpp_path = _validate_artifact(cpp_artifact, base=base, label="C++ event-stream binding")
    cpp_payload = _load_json(cpp_path)
    if cpp_payload.get("schema_version") != CPP_BINDING_SCHEMA_VERSION:
        raise InputAdmissionError("cpp_binding_schema_mismatch", "C++ binding schema drift")
    claimed_binding_sha = str(cpp_payload.get("canonical_binding_sha256", ""))
    if claimed_binding_sha != canonical_document_sha256(
        cpp_payload, "canonical_binding_sha256"
    ):
        raise InputAdmissionError(
            "cpp_binding_canonical_hash_mismatch",
            "C++ binding canonical hash differs",
        )
    if cpp_payload.get("runtime_code_identity_sha256") != implementation_sha:
        raise InputAdmissionError("cpp_binding_code_identity_mismatch", "C++ code binding differs")
    if cpp_payload.get("abi_version") != CPP_EVENT_STREAM_MIRROR_ABI_VERSION:
        raise InputAdmissionError("cpp_binding_abi_mismatch", "C++ binding ABI is unsupported")
    if cpp_payload.get("journal_schema_version") != ORDER_LIFECYCLE_JOURNAL_V2_SCHEMA_VERSION:
        raise InputAdmissionError(
            "cpp_binding_journal_schema_mismatch", "C++ journal schema version differs"
        )
    if cpp_payload.get("journal_schema_sha256") != journal_schema_sha256():
        raise InputAdmissionError("cpp_binding_journal_schema_mismatch", "C++ journal schema differs")
    if cpp_payload.get("projection_schema_sha256") != projection_schema_sha256():
        raise InputAdmissionError(
            "cpp_binding_projection_schema_mismatch",
            "C++ mechanics projection schema differs",
        )
    if cpp_payload.get("quantity_contract_id") != ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID:
        raise InputAdmissionError(
            "cpp_binding_quantity_contract_mismatch",
            "C++ terminal-remainder contract differs",
        )
    if float(cpp_payload.get("terminal_remainder_abs_tolerance_btc", -1.0)) != (
        TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
    ):
        raise InputAdmissionError(
            "cpp_binding_terminal_tolerance_mismatch",
            "C++ terminal-remainder tolerance differs",
        )
    if float(cpp_payload.get("persisted_terminal_remainder_btc", -1.0)) != 0.0:
        raise InputAdmissionError(
            "cpp_binding_terminal_zero_mismatch",
            "C++ terminal remainder is not canonical exact zero",
        )
    if not bool(cpp_payload.get("cancel_reject_active_branch_observed", False)) or not bool(
        cpp_payload.get("cancel_reject_partially_filled_branch_observed", False)
    ):
        raise InputAdmissionError(
            "cpp_binding_cancel_reject_branch_coverage_incomplete",
            "C++ binding evidence lacks one cancel-reject continuation phase",
        )
    if (
        not bool(cpp_payload.get("mechanics_only", False))
        or bool(cpp_payload.get("economic_outcomes_read", True))
        or bool(cpp_payload.get("formal_40day_lockstep_executed", True))
    ):
        raise InputAdmissionError(
            "cpp_binding_scope_violation",
            "C++ binding artifact exceeds mechanics-only preflight scope",
        )
    lockstep_report_sha = str(cpp_payload.get("lockstep_report_sha256", ""))
    if not _is_sha256(lockstep_report_sha):
        raise InputAdmissionError(
            "cpp_binding_lockstep_report_missing",
            "C++ binding lacks a valid lockstep report identity",
        )
    cpp_implementations = cpp_payload.get("implementation_artifacts")
    if not isinstance(cpp_implementations, Sequence) or isinstance(
        cpp_implementations, (str, bytes)
    ) or not cpp_implementations:
        raise InputAdmissionError(
            "cpp_binding_implementation_artifacts_missing",
            "C++ binding implementation artifacts are required",
        )
    for index, artifact in enumerate(cpp_implementations):
        if not isinstance(artifact, Mapping):
            raise InputAdmissionError(
                "cpp_binding_implementation_artifact_invalid",
                "C++ binding implementation artifact is invalid",
            )
        _validate_artifact(
            artifact,
            base=cpp_path.parent,
            label=f"C++ binding implementation[{index}]",
        )
    if cpp_section["status"] != cpp_payload.get("status"):
        raise InputAdmissionError("cpp_binding_status_mismatch", "C++ binding status differs")
    if cpp_section["abi_version"] != cpp_payload.get("abi_version"):
        raise InputAdmissionError("cpp_binding_abi_mismatch", "C++ binding ABI differs")
    return {
        "replay_session_scope": replay_scope,
        "carry_in_lifecycle_count": 0,
        "prospective_live_epoch_transport_authorized": False,
        "cancel_reject_route_supported": route_supported,
        "cancel_reject_observed_event_count": observed_cancel_reject,
        "sub_lot_terminal_remainder_count": sub_lot_count,
        "sub_lot_terminal_remainder_contract": contract,
        "terminal_remainder_abs_tolerance_btc": (
            TERMINAL_REMAINDER_ABS_TOLERANCE_BTC
        ),
        "cpp_event_stream_binding_status": str(cpp_section["status"]),
        "cpp_event_stream_abi_version": str(cpp_section["abi_version"]),
    }


def _validate_day(
    manifest_path: Path,
    *,
    identity: Mapping[str, object],
    expected_day: str,
) -> dict[str, object]:
    payload = _load_json(manifest_path)
    _assert_mechanics_only(payload, path=f"day[{expected_day}]")
    _require_exact_keys(payload, _DAY_MANIFEST_KEYS, label="daily admission manifest")
    if payload["schema_version"] != DAY_ADMISSION_SCHEMA_VERSION:
        raise InputAdmissionError("day_manifest_schema_mismatch", "unsupported daily manifest")
    _validate_canonical_document(
        payload, hash_field="canonical_manifest_sha256", label="daily admission manifest"
    )
    if str(payload["day"]) != expected_day:
        raise InputAdmissionError("day_manifest_day_mismatch", "daily manifest day differs")
    frozen_sha = canonical_document_sha256(identity, "canonical_identity_sha256")
    if payload["frozen_input_identity_sha256"] != frozen_sha:
        raise InputAdmissionError("day_frozen_identity_mismatch", "daily frozen identity differs")
    interval = expected_day_interval(expected_day)
    if payload["interval_identity_sha256"] != interval["interval_identity_sha256"]:
        raise InputAdmissionError("day_interval_identity_mismatch", "daily interval differs")
    baseline_sha = identity["baseline_runtime_identity"]["canonical_sha256"]
    if payload["baseline_runtime_identity_sha256"] != baseline_sha:
        raise InputAdmissionError("day_baseline_identity_mismatch", "daily baseline differs")
    if payload["admission_state"] != "complete":
        raise InputAdmissionError("day_admission_incomplete", "daily manifest is not complete")
    if payload["atomic_publish_method"] != "fsync_tempfile_replace":
        raise InputAdmissionError("day_admission_not_atomic", "daily manifest is not atomic")
    if bool(payload["economic_outcomes_read"]):
        raise InputAdmissionError("economic_outcome_access", "daily admission read economics")
    expected_runtime = expected_runtime_identity(identity, expected_day)
    journal = _validate_journal(
        payload["journal_v2"],
        day=expected_day,
        expected_runtime=expected_runtime,
        base=manifest_path.parent,
    )
    legacy = _validate_legacy(
        payload["legacy_trace"], day=expected_day, base=manifest_path.parent
    )
    if journal["client_order_ids"] != legacy["client_order_ids"]:
        raise InputAdmissionError(
            "journal_legacy_order_identity_mismatch",
            "journal-v2 and legacy order sets differ",
        )
    source_artifacts = payload["market_data_artifacts"]
    if not isinstance(source_artifacts, Sequence) or isinstance(source_artifacts, (str, bytes)):
        raise InputAdmissionError("market_data_artifacts_missing", "market artifacts are required")
    source_counts = _validate_source_coverage(
        source_artifacts,
        identity=identity,
        day=expected_day,
        base=manifest_path.parent,
    )
    capabilities = _validate_capabilities(
        payload["producer_capabilities"],
        identity=identity,
        journal=journal,
        legacy=legacy,
        base=manifest_path.parent,
    )
    return {
        "day": expected_day,
        "journal_rows": journal["row_count"],
        "legacy_rows": legacy["row_count"],
        "orders": journal["order_count"],
        "journal_parts": journal["part_count"],
        "legacy_clock_semantics": legacy["clock_semantics"],
        "legacy_missing_visibility_count": legacy["missing_visibility_count"],
        "legacy_missing_required_exchange_count": legacy[
            "missing_required_exchange_count"
        ],
        "legacy_exchange_after_visibility_count": legacy[
            "exchange_after_visibility_count"
        ],
        "cancel_reject_route_supported": capabilities[
            "cancel_reject_route_supported"
        ],
        "cancel_reject_observed_event_count": capabilities[
            "cancel_reject_observed_event_count"
        ],
        "sub_lot_terminal_remainder_count": capabilities[
            "sub_lot_terminal_remainder_count"
        ],
        "cpp_event_stream_binding_status": capabilities[
            "cpp_event_stream_binding_status"
        ],
        "cpp_event_stream_abi_version": capabilities[
            "cpp_event_stream_abi_version"
        ],
        "market_data_artifact_counts": source_counts,
        "runtime_identity_sha256": journal["runtime_identity_sha256"],
        "replay_session_scope": capabilities["replay_session_scope"],
        "carry_in_lifecycle_count": capabilities["carry_in_lifecycle_count"],
        "prospective_live_epoch_transport_authorized": capabilities[
            "prospective_live_epoch_transport_authorized"
        ],
    }


def preflight_40day_admission(
    *,
    frozen_identity: Mapping[str, object],
    panel_manifest_path: str | Path,
) -> dict[str, object]:
    identity_state = validate_frozen_identity(frozen_identity, base=ROOT)
    panel_path = Path(panel_manifest_path).expanduser().resolve()
    failures: list[dict[str, str]] = []
    day_reports: list[dict[str, object]] = []
    panel_hash = ""
    panel_days: list[str] = []
    references: list[Mapping[str, object]] = []
    try:
        panel = _load_json(panel_path)
        _assert_mechanics_only(panel, path="panel_admission")
        _require_exact_keys(panel, _PANEL_MANIFEST_KEYS, label="panel admission manifest")
        if panel["schema_version"] != PANEL_ADMISSION_SCHEMA_VERSION:
            raise InputAdmissionError("panel_manifest_schema_mismatch", "unsupported panel manifest")
        if panel["identity"] != IDENTITY:
            raise InputAdmissionError("panel_identity_mismatch", "panel identity differs")
        panel_hash = _validate_canonical_document(
            panel, hash_field="canonical_manifest_sha256", label="panel admission manifest"
        )
        if panel["frozen_input_identity_sha256"] != identity_state[
            "canonical_identity_sha256"
        ]:
            raise InputAdmissionError("panel_frozen_identity_mismatch", "panel frozen identity differs")
        if panel["admission_state"] != "complete":
            raise InputAdmissionError("panel_admission_incomplete", "panel is not complete")
        if panel["atomic_publish_method"] != "fsync_tempfile_replace":
            raise InputAdmissionError("panel_admission_not_atomic", "panel manifest is not atomic")
        if bool(panel["economic_outcomes_read"]):
            raise InputAdmissionError("economic_outcome_access", "panel admission read economics")
        if bool(panel["lockstep_executed"]):
            raise InputAdmissionError(
                "preflight_scope_violation", "input admission cannot claim lockstep execution"
            )
        panel_days = list(map(str, panel["ordered_utc_days"]))
        if len(panel_days) != len(set(panel_days)):
            raise InputAdmissionError("duplicate_day", "panel admission contains duplicate days")
        if panel_days != list(identity_state["days"]):
            raise InputAdmissionError(
                "ordered_day_denominator_mismatch", "panel days differ from frozen 40 days"
            )
        references = list(panel["day_manifests"])
        if len(references) != REQUIRED_DAY_COUNT:
            raise InputAdmissionError(
                "daily_manifest_count_mismatch", "panel must reference exactly 40 daily manifests"
            )
    except InputAdmissionError as exc:
        failures.append(_failure(exc.code, exc.detail, artifact=exc.artifact or str(panel_path)))

    for ordinal, day in enumerate(identity_state["days"]):
        if ordinal >= len(references):
            failures.append(
                _failure("daily_manifest_missing", "daily manifest reference is missing", day=day)
            )
            continue
        reference = references[ordinal]
        try:
            if not isinstance(reference, Mapping) or set(reference) != {"day", "artifact"}:
                raise InputAdmissionError(
                    "daily_manifest_reference_schema_mismatch", "daily reference is invalid"
                )
            if str(reference["day"]) != day:
                raise InputAdmissionError(
                    "daily_manifest_reference_order_mismatch", "daily reference order differs"
                )
            artifact = reference["artifact"]
            if not isinstance(artifact, Mapping):
                raise InputAdmissionError(
                    "daily_manifest_artifact_missing", "daily manifest artifact is required"
                )
            path = _validate_artifact(
                artifact, base=panel_path.parent, label=f"daily manifest {day}"
            )
            day_report = _validate_day(path, identity=frozen_identity, expected_day=day)
            day_reports.append(day_report)
        except InputAdmissionError as exc:
            failures.append(
                _failure(exc.code, exc.detail, day=day, artifact=exc.artifact)
            )

    current_status = frozen_identity.get("current_producer_status")
    if not isinstance(current_status, Mapping):
        failures.append(
            _failure("current_producer_status_missing", "current producer status is absent")
        )
    else:
        required_status = {
            "legacy_explicit_dual_clock",
            "cancel_reject_route",
            "sub_lot_terminal_contract",
            "cpp_event_stream_binding",
        }
        for key in sorted(required_status):
            if not bool(current_status.get(key, False)):
                failures.append(
                    _failure(
                        f"current_{key}_unsupported",
                        f"frozen producer status does not support {key}",
                    )
                )

    clock_counts = Counter(str(row["legacy_clock_semantics"]) for row in day_reports)
    cancel_reject_days = sum(
        int(bool(row["cancel_reject_route_supported"])) for row in day_reports
    )
    cpp_bound_days = sum(
        int(row["cpp_event_stream_binding_status"] == "bound") for row in day_reports
    )
    fresh_start_days = sum(
        int(
            row["replay_session_scope"] == "fresh_start_per_target_day"
            and int(row["carry_in_lifecycle_count"]) == 0
            and not bool(row["prospective_live_epoch_transport_authorized"])
        )
        for row in day_reports
    )
    sub_lot_count = sum(
        int(row["sub_lot_terminal_remainder_count"]) for row in day_reports
    )
    if clock_counts and clock_counts.get("explicit_dual_clock", 0) != REQUIRED_DAY_COUNT:
        failures.append(
            _failure(
                "legacy_dual_clock_coverage_incomplete",
                f"legacy clock coverage={dict(sorted(clock_counts.items()))}",
            )
        )
    if cancel_reject_days != REQUIRED_DAY_COUNT and day_reports:
        failures.append(
            _failure(
                "cancel_reject_route_coverage_incomplete",
                f"supported_days={cancel_reject_days}/{REQUIRED_DAY_COUNT}",
            )
        )
    if sub_lot_count:
        failures.append(
            _failure(
                "sub_lot_terminal_remainder_nonzero",
                f"terminal remainder observations={sub_lot_count}",
            )
        )
    if cpp_bound_days != REQUIRED_DAY_COUNT and day_reports:
        failures.append(
            _failure(
                "cpp_event_stream_binding_incomplete",
                f"bound_days={cpp_bound_days}/{REQUIRED_DAY_COUNT}",
            )
        )
    if fresh_start_days != REQUIRED_DAY_COUNT and day_reports:
        failures.append(
            _failure(
                "daily_fresh_start_scope_coverage_incomplete",
                f"fresh_start_days={fresh_start_days}/{REQUIRED_DAY_COUNT}",
            )
        )

    failures = sorted(
        failures,
        key=lambda row: (row["code"], row["day"], row["artifact"], row["detail"]),
    )
    qualified_days = REQUIRED_DAY_COUNT - len(
        {row["day"] for row in failures if row["day"]}
    )
    all_days_reported = [str(row["day"]) for row in day_reports] == list(
        identity_state["days"]
    )
    gates = {
        "frozen_ordered_40_day_denominator": panel_days == list(identity_state["days"]),
        "all_daily_atomic_manifests_admitted": all_days_reported,
        "writer_integrity_and_zero_drop_error": all_days_reported
        and not any(
            "writer" in row["code"] or "journal_part" in row["code"]
            for row in failures
        ),
        "legacy_explicit_dual_clock_coverage": clock_counts.get(
            "explicit_dual_clock", 0
        )
        == REQUIRED_DAY_COUNT,
        "cancel_reject_route_bound": cancel_reject_days == REQUIRED_DAY_COUNT,
        "sub_lot_terminal_remainder_zero": sub_lot_count == 0 and all_days_reported,
        "cpp_event_stream_bound": cpp_bound_days == REQUIRED_DAY_COUNT,
        "daily_fresh_start_session_scope": fresh_start_days == REQUIRED_DAY_COUNT,
        "market_source_intervals_and_integrity": all_days_reported
        and not any(row["code"].startswith("source_") for row in failures),
        "mechanics_only_scope": not any(
            row["code"] in {"economic_outcome_access", "economic_field_forbidden"}
            for row in failures
        ),
    }
    eligible = bool(all(gates.values()) and not failures)
    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "scope": {
            "mechanics_only": True,
            "economic_outcomes_read": False,
            "formal_40day_lockstep_executed": False,
            "replay_session_scope": "fresh_start_per_target_day",
            "utc_day_role": "offline_denominator_and_cluster_unit_not_live_state_reset",
            "prospective_live_epoch_transport_compatible": False,
            "live_transport_successor_requirement": (
                "complete_baseline_epoch_session_or_explicit_carry_in_cursor_and_left_truncation"
            ),
        },
        "frozen_input_identity_sha256": identity_state["canonical_identity_sha256"],
        "panel_admission_manifest": {
            "path": str(panel_path),
            "canonical_sha256": panel_hash,
        },
        "ordered_utc_days": list(identity_state["days"]),
        "counts": {
            "required_days": REQUIRED_DAY_COUNT,
            "daily_reports_completed": len(day_reports),
            "admission_qualified_days": qualified_days if not failures else len(day_reports),
            "journal_rows": sum(int(row["journal_rows"]) for row in day_reports),
            "legacy_rows": sum(int(row["legacy_rows"]) for row in day_reports),
            "orders": sum(int(row["orders"]) for row in day_reports),
            "journal_parts": sum(int(row["journal_parts"]) for row in day_reports),
        },
        "coverage_and_limitations": {
            "legacy_clock_semantics_day_counts": dict(sorted(clock_counts.items())),
            "legacy_single_clock_is_not_treated_as_dual_clock": True,
            "cancel_reject_supported_days": cancel_reject_days,
            "cancel_reject_observed_event_count": sum(
                int(row["cancel_reject_observed_event_count"]) for row in day_reports
            ),
            "sub_lot_terminal_remainder_count": sub_lot_count,
            "cpp_event_stream_bound_days": cpp_bound_days,
            "cpp_event_stream_binding_is_not_inferred_from_cif_kernel_parity": True,
            "fresh_start_session_scope_days": fresh_start_days,
            "daily_sequence_origin": 1,
            "carry_in_lifecycle_supported": False,
            "left_truncation_supported": False,
        },
        "day_reports": day_reports,
        "failure_reasons": failures,
        "gates": gates,
        "lockstep_execution_eligible": eligible,
        "live_transport_execution_eligible": False,
        "permissions": {
            "cif_training": False,
            "economic_evaluation": False,
            "q90_action": False,
            "prospective_live_epoch_transport": False,
            "live_deployment": False,
        },
    }
    report["canonical_report_sha256"] = canonical_sha256(report)
    return report


def atomic_write_report(path: str | Path, report: Mapping[str, object]) -> Path:
    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(report)
    claimed = str(payload.pop("canonical_report_sha256", ""))
    if claimed != canonical_sha256(payload):
        raise InputAdmissionError("report_hash_mismatch", "report canonical hash differs")
    envelope = {
        "schema_version": REPORT_ENVELOPE_SCHEMA_VERSION,
        "report_sha256": claimed,
        "report": dict(report),
    }
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_canonical_bytes(envelope) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--identity", default=str(DEFAULT_IDENTITY))
    parser.add_argument("--admission-manifest", required=True)
    parser.add_argument("--output", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    identity, _ = load_frozen_identity(args.identity)
    report = preflight_40day_admission(
        frozen_identity=identity,
        panel_manifest_path=args.admission_manifest,
    )
    atomic_write_report(args.output, report)
    print(json.dumps(report, sort_keys=True, indent=2))
    return 0 if bool(report["lockstep_execution_eligible"]) else 2


if __name__ == "__main__":
    raise SystemExit(main())
