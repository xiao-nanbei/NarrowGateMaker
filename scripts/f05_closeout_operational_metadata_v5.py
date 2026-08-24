#!/usr/bin/env python3
"""Plan or publish the no-external-shadow BUY E3 operational metadata successor.

This is additive operational evidence plumbing.  It does not contact a live
host, read economic outcomes, alter a strategy, or write an evidence volume.
All deployment-specific values that do not yet exist (the fresh PID/epoch,
resource/capture/admission hashes, 3600-second lifecycle, and post-health
identity) must arrive in one private, canonical, mode-0600 manifest.

The three-file publication is an ordered, resumable transaction:

    immutable replacement receipt -> mutable current pointer -> mutable catalog

Each individual publication is atomic.  The receipt is create-only, and its
deterministic pending hardlink can be repaired after a crash without accepting
an nlink=2 final identity.  Reversed or otherwise mixed publication order is
rejected fail closed.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_current_host_resource_gate_v6 as resource_v6,
)
from scripts import audit_private_evidence
from scripts import f05_buy_e3_active_capture_v6 as active_v4
from scripts import f05_buy_e3_cross_host_transport_v5 as transport_v2
from scripts import f05_buy_e3_external_venues_disabled_config_successor as config_successor
from scripts import f05_buy_e3_final_evidence_v4 as historical_final_v4
from scripts import f05_buy_e3_final_evidence_v5 as final_v5

MANIFEST_SCHEMA: Final = "f05_buy_e3_operational_metadata_v5_activation_manifest.v1"
MANIFEST_STATUS: Final = "external_venues_disabled_metadata_inputs_frozen"
MANIFEST_CANONICAL_FIELD: Final = "canonical_operational_metadata_manifest_sha256"

RECEIPT_SCHEMA: Final = "narrowgate.live_replacement_activation_receipt.v2"
RECEIPT_STATUS: Final = "completed_active_direct_v4_external_venues_disabled_evidence_closed"
RECEIPT_CANONICAL_FIELD: Final = "canonical_replacement_activation_receipt_sha256"

POINTER_SCHEMA: Final = "narrowgate_live_remote_pointer.v1"
CATALOG_SCHEMA: Final = "narrowgate_private_artifact_catalog_v1"
SUPERSEDED_REASON: Final = "superseded_due_external_venue_shadow_enabled"

EXECUTION: Final = {
    "execution_commit": "07ef93733a3a685caba945c7761a48473e403072",
    "execution_tree": "ff505cd81a8eb11f2087d2ae27e7986fd99b0444",
    "annotated_operational_tag": "f05-owner-buy-e3-direct-live-v4-20260824",
    "annotated_operational_tag_object": "da83fa0b4aed00e4d04ea3faa212b2fb27a81f0d",
    "tag_peeled_commit": "07ef93733a3a685caba945c7761a48473e403072",
}
RELEASE: Final = {
    "schema_version": transport_v2.FROZEN_FINAL_RELEASE_SCHEMA,
    "status": transport_v2.FROZEN_FINAL_RELEASE_STATUS,
    "file_sha256": transport_v2.FROZEN_FINAL_RELEASE_FILE_SHA256,
    "canonical_field": "canonical_active_release_sha256",
    "canonical_sha256": transport_v2.FROZEN_FINAL_RELEASE_CANONICAL_SHA256,
}
ACTIVE_CONFIG_SHA256: Final = "ad153012b14e725a3ac24f0ddbe02bc353168a13ec827b777cc94761020524ec"
DISABLED_CONFIG_SHA256: Final = "10158a92177cd87b77fdb24a2a477dcab4b41cfb29208cf96c19953edafe166f"
ARTIFACT_SHA256: Final = transport_v2.FROZEN_FINAL_ARTIFACT_SHA256

NO_NEW_AUTHORITY: Final = {"research": False, "action": False, "live": False}
EVIDENCE_BOUNDARY: Final = {
    "economic_outcomes_read": False,
    "economic_values_persisted": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "new_economic_arm_run": False,
    "shadow_created": False,
    "companion_created": False,
    "hypothetical_live_actions_scored": False,
}

CONTENT_FIELDS: Final = (
    "path",
    "schema_version",
    "status",
    "file_sha256",
    "canonical_field",
    "canonical_sha256",
    "size_bytes",
    "mode",
)
CONTENT_FIELDS_NO_PATH: Final = CONTENT_FIELDS[1:]

SOURCE_IDENTITIES: Final = {
    "direct_release": (
        RELEASE["schema_version"],
        RELEASE["status"],
        RELEASE["canonical_field"],
    ),
    "config_correction": (
        config_successor.SCHEMA_VERSION,
        config_successor.STATUS,
        config_successor.CANONICAL_FIELD,
    ),
    "resource_gate": (
        resource_v6.RESOURCE_SCHEMA,
        resource_v6.RESOURCE_STATUS,
        resource_v6.RESOURCE_CANONICAL_FIELD,
    ),
    "active_process_capture": (
        active_v4.SCHEMA_VERSION,
        active_v4.STATUS,
        active_v4.CANONICAL_FIELD,
    ),
    "cross_host_admission": (
        transport_v2.ADMISSION_SCHEMA,
        transport_v2.ADMISSION_STATUS,
        transport_v2.ADMISSION_CANONICAL_FIELD,
    ),
    "lifecycle_admission": (
        "prospective_lifecycle_remote_session_admission.v1",
        None,
        "admission_identity_sha256",
    ),
    # The post-health schema is created only after the new process exists.  Its
    # exact schema/status/canonical field are therefore manifest-bound below.
    "post_health": (None, None, None),
    "final_activation_envelope": (
        final_v5.ENVELOPE_SCHEMA,
        final_v5.ENVELOPE_STATUS,
        final_v5.ENVELOPE_CANONICAL_FIELD,
    ),
    "final_operational_completion": (
        final_v5.COMPLETION_SCHEMA,
        final_v5.COMPLETION_STATUS,
        final_v5.COMPLETION_CANONICAL_FIELD,
    ),
    "final_composition": (
        final_v5.COMPOSITION_SCHEMA,
        final_v5.COMPOSITION_STATUS,
        final_v5.COMPOSITION_CANONICAL_FIELD,
    ),
    "final_attempt": (
        final_v5.ATTEMPT_FINAL_SCHEMA,
        final_v5.ATTEMPT_FINAL_STATUS,
        final_v5.ATTEMPT_FINAL_CANONICAL_FIELD,
    ),
    "final_proof": (
        final_v5.EVIDENCE_RELEASE_SCHEMA,
        final_v5.EVIDENCE_RELEASE_STATUS,
        final_v5.EVIDENCE_RELEASE_CANONICAL_FIELD,
    ),
    "historical_v4_proof": (
        historical_final_v4.EVIDENCE_RELEASE_SCHEMA,
        historical_final_v4.EVIDENCE_RELEASE_STATUS,
        historical_final_v4.EVIDENCE_RELEASE_CANONICAL_FIELD,
    ),
}

LIFECYCLE_ASSERTIONS: Final = {
    "epoch_id",
    "epoch_identity_sha256",
    "config_sha256",
    "requested_duration_s",
    "observed_duration_s",
    "health_error_count",
    "health_drop_count",
    "stable_double_read_passed",
    "formal_collection_valid",
}
POST_HEALTH_ASSERTIONS: Final = {
    "epoch_id",
    "pid",
    "pid_start_ticks",
    "config_sha256",
    "process_alive",
    "health_error_count",
    "health_drop_count",
    "external_venues_enabled",
    "buy_e3_shadow_enabled",
    "buy_e3_companion_enabled",
    "buy_e3_policy_enabled",
    "observed_utc",
}

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
MAX_JSON_BYTES: Final = 64 << 20


class OperationalMetadataV5Error(RuntimeError):
    """Raised when any input or transaction state fails closed."""


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return _sha(encoded)


def _document_sha(payload: Mapping[str, Any], field: str) -> str:
    projected = dict(payload)
    projected.pop(field, None)
    return _canonical(projected)


def _render(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode()


def _timestamp(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise OperationalMetadataV5Error(f"{label} must be an explicit UTC Z timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OperationalMetadataV5Error(f"{label} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise OperationalMetadataV5Error(f"{label} is not UTC")
    return value


def _sha256(value: Any, label: str) -> str:
    normalized = str(value)
    if _SHA256_RE.fullmatch(normalized) is None:
        raise OperationalMetadataV5Error(f"{label} is not a lowercase SHA256")
    return normalized


def _json_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise OperationalMetadataV5Error(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _read_regular(
    path: Path,
    *,
    mode: int | None = None,
    allowed_nlinks: frozenset[int] = frozenset({1}),
) -> tuple[bytes, os.stat_result]:
    target = Path(os.path.abspath(os.fspath(path.expanduser())))
    before = target.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink not in allowed_nlinks
        or before.st_size < 0
        or before.st_size > MAX_JSON_BYTES
        or (mode is not None and stat.S_IMODE(before.st_mode) != mode)
    ):
        raise OperationalMetadataV5Error(f"unsafe file identity: {target}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(target, flags)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise OperationalMetadataV5Error(f"file changed while opening: {target}")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, 1 << 20)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_JSON_BYTES:
                raise OperationalMetadataV5Error(f"JSON file is too large: {target}")
            chunks.append(chunk)
        after_fd = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    after_path = target.lstat()
    identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    if identity != (
        after_fd.st_dev,
        after_fd.st_ino,
        after_fd.st_size,
        after_fd.st_mtime_ns,
    ) or identity != (
        after_path.st_dev,
        after_path.st_ino,
        after_path.st_size,
        after_path.st_mtime_ns,
    ):
        raise OperationalMetadataV5Error(f"file changed while reading: {target}")
    return b"".join(chunks), before


def _load_json(
    path: Path,
    *,
    mode: int | None = None,
    allowed_nlinks: frozenset[int] = frozenset({1}),
) -> tuple[dict[str, Any], bytes, os.stat_result]:
    raw, metadata = _read_regular(path, mode=mode, allowed_nlinks=allowed_nlinks)
    try:
        payload = json.loads(raw, object_pairs_hook=_json_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationalMetadataV5Error(f"invalid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise OperationalMetadataV5Error(f"JSON root is not an object: {path}")
    return payload, raw, metadata


def _content(payload: Mapping[str, Any], path: Path, raw: bytes, mode: int) -> dict[str, Any]:
    field = payload.get("canonical_field")
    if not isinstance(field, str):
        candidates = [
            key
            for key in payload
            if key == "admission_identity_sha256"
            or (key.startswith("canonical_") and key.endswith("sha256"))
        ]
        if len(candidates) != 1:
            raise OperationalMetadataV5Error(f"canonical field is ambiguous: {path}")
        field = candidates[0]
    canonical = payload.get(field)
    _sha256(canonical, f"{path.name} canonical SHA256")
    if _document_sha(payload, field) != canonical:
        raise OperationalMetadataV5Error(f"canonical recomputation drift: {path}")
    return {
        "path": str(Path(os.path.abspath(os.fspath(path.expanduser())))),
        "schema_version": payload.get("schema_version"),
        "status": payload.get("status"),
        "file_sha256": _sha(raw),
        "canonical_field": field,
        "canonical_sha256": canonical,
        "size_bytes": len(raw),
        "mode": f"{mode:04o}",
    }


def _without_path(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {key: binding[key] for key in CONTENT_FIELDS_NO_PATH}


def _json_pointer(payload: Any, pointer: str) -> Any:
    if not isinstance(pointer, str) or not pointer.startswith("/"):
        raise OperationalMetadataV5Error("JSON assertion pointer must start with '/'")
    current = payload
    for raw_component in pointer[1:].split("/"):
        component = raw_component.replace("~1", "/").replace("~0", "~")
        if isinstance(current, Mapping) and component in current:
            current = current[component]
        elif isinstance(current, list) and component.isdigit() and int(component) < len(current):
            current = current[int(component)]
        else:
            raise OperationalMetadataV5Error(f"JSON assertion path is missing: {pointer}")
    return current


def _contains_mapping(payload: Any, expected: Mapping[str, Any]) -> bool:
    if isinstance(payload, Mapping):
        if all(payload.get(key) == value for key, value in expected.items()):
            return True
        return any(_contains_mapping(value, expected) for value in payload.values())
    if isinstance(payload, list):
        return any(_contains_mapping(value, expected) for value in payload)
    return False


def _execution_projection_matches(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    return (
        value.get("execution_commit") == EXECUTION["execution_commit"]
        and value.get("execution_tree") == EXECUTION["execution_tree"]
        and value.get("annotated_tag", value.get("annotated_operational_tag"))
        == EXECUTION["annotated_operational_tag"]
        and value.get("annotated_tag_object", value.get("annotated_operational_tag_object"))
        == EXECUTION["annotated_operational_tag_object"]
        and value.get("tag_peeled_commit") == EXECUTION["tag_peeled_commit"]
    )


def _validate_manifest(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload, raw, _metadata = _load_json(path, mode=0o600)
    expected_top = {
        "schema_version",
        "status",
        "generated_utc",
        "receipt_id",
        "repository_root",
        "transaction",
        "runtime",
        "lifecycle",
        "post_health",
        "source_assertions",
        "sources",
        "permissions",
        "evidence_boundary",
        MANIFEST_CANONICAL_FIELD,
    }
    if (
        set(payload) != expected_top
        or payload.get("schema_version") != MANIFEST_SCHEMA
        or payload.get("status") != MANIFEST_STATUS
        or payload.get("permissions") != NO_NEW_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get(MANIFEST_CANONICAL_FIELD) != _document_sha(payload, MANIFEST_CANONICAL_FIELD)
    ):
        raise OperationalMetadataV5Error("operational metadata manifest identity drifted")
    _timestamp(payload["generated_utc"], "manifest generated_utc")
    if not isinstance(payload["receipt_id"], str) or not payload["receipt_id"]:
        raise OperationalMetadataV5Error("manifest receipt_id is missing")
    root = Path(str(payload["repository_root"])).expanduser()
    if not root.is_absolute():
        raise OperationalMetadataV5Error("manifest repository_root must be absolute")

    transaction = payload.get("transaction")
    expected_transaction = {
        "pointer_path",
        "catalog_path",
        "replacement_receipt_path",
        "predecessor_pointer",
        "predecessor_catalog",
        "predecessor_activation",
        "predecessor_activation_artifact_id",
        "replacement_activation_artifact_id",
    }
    if not isinstance(transaction, Mapping) or set(transaction) != expected_transaction:
        raise OperationalMetadataV5Error("manifest transaction fields drifted")
    for name in ("pointer_path", "catalog_path", "replacement_receipt_path"):
        value = Path(str(transaction[name])).expanduser()
        if not value.is_absolute():
            raise OperationalMetadataV5Error(f"manifest {name} must be absolute")
    for name in ("predecessor_pointer", "predecessor_catalog"):
        value = transaction.get(name)
        if not isinstance(value, Mapping) or set(value) != {"file_sha256", "size_bytes"}:
            raise OperationalMetadataV5Error(f"manifest {name} binding drifted")
        _sha256(value["file_sha256"], f"manifest {name} SHA256")
        if not isinstance(value["size_bytes"], int) or value["size_bytes"] <= 0:
            raise OperationalMetadataV5Error(f"manifest {name} size is invalid")
    predecessor = transaction.get("predecessor_activation")
    if not isinstance(predecessor, Mapping) or set(predecessor) != set(CONTENT_FIELDS):
        raise OperationalMetadataV5Error("manifest predecessor activation binding drifted")
    if (
        predecessor.get("schema_version") != RECEIPT_SCHEMA.replace(".v2", ".v1")
        or predecessor.get("status") != "completed_active_direct_v4_evidence_closed"
        or predecessor.get("canonical_field") != RECEIPT_CANONICAL_FIELD
    ):
        raise OperationalMetadataV5Error("manifest predecessor v4 activation identity drifted")
    if not isinstance(transaction["predecessor_activation_artifact_id"], str) or not isinstance(
        transaction["replacement_activation_artifact_id"], str
    ):
        raise OperationalMetadataV5Error("manifest catalog artifact ids are invalid")

    runtime = payload.get("runtime")
    expected_runtime = {
        "host",
        "process",
        "epoch",
        "execution",
        "release_remote_path",
        "active_config_sha256",
        "disabled_config_sha256",
        "runtime_code_sha256",
        "artifact_sha256",
    }
    if not isinstance(runtime, Mapping) or set(runtime) != expected_runtime:
        raise OperationalMetadataV5Error("manifest runtime fields drifted")
    if runtime["execution"] != EXECUTION:
        raise OperationalMetadataV5Error("manifest execution is not exact direct-v4 07ef")
    if (
        runtime["active_config_sha256"] != ACTIVE_CONFIG_SHA256
        or runtime["disabled_config_sha256"] != DISABLED_CONFIG_SHA256
        or runtime["artifact_sha256"] != ARTIFACT_SHA256
    ):
        raise OperationalMetadataV5Error("manifest config/artifact identity drifted")
    _sha256(runtime["runtime_code_sha256"], "runtime code SHA256")
    release_path = Path(str(runtime["release_remote_path"]))
    if not release_path.is_absolute():
        raise OperationalMetadataV5Error("runtime release path must be absolute")
    host = runtime.get("host")
    host_fields = {
        "provider",
        "region",
        "city",
        "ssh_target",
        "public_ipv4",
        "instance_id",
        "instance_type",
        "repo_root",
    }
    if (
        not isinstance(host, Mapping)
        or set(host) != host_fields
        or not all(isinstance(host[field], str) and host[field] for field in host_fields)
    ):
        raise OperationalMetadataV5Error("manifest host identity is incomplete")
    process = runtime.get("process")
    if not isinstance(process, Mapping) or set(process) != {
        "pid",
        "pid_start_ticks",
        "process_identity_sha256",
        "runtime_identity_recorded_utc",
        "active_capture_utc",
    }:
        raise OperationalMetadataV5Error("manifest process identity drifted")
    if (
        not isinstance(process["pid"], int)
        or isinstance(process["pid"], bool)
        or process["pid"] <= 1
        or not isinstance(process["pid_start_ticks"], int)
        or isinstance(process["pid_start_ticks"], bool)
        or process["pid_start_ticks"] <= 0
    ):
        raise OperationalMetadataV5Error("manifest process PID identity is invalid")
    _sha256(process["process_identity_sha256"], "process identity SHA256")
    _timestamp(process["runtime_identity_recorded_utc"], "runtime identity timestamp")
    _timestamp(process["active_capture_utc"], "active capture timestamp")
    epoch = runtime.get("epoch")
    if not isinstance(epoch, Mapping) or set(epoch) != {
        "epoch_id",
        "started_ts_ns",
        "started_utc",
        "identity_sha256",
        "predecessor_authority_end_utc",
    }:
        raise OperationalMetadataV5Error("manifest epoch identity drifted")
    if not isinstance(epoch["epoch_id"], str) or not epoch["epoch_id"].startswith("prospective-"):
        raise OperationalMetadataV5Error("manifest epoch id is invalid")
    if not isinstance(epoch["started_ts_ns"], int) or epoch["started_ts_ns"] <= 0:
        raise OperationalMetadataV5Error("manifest epoch start ns is invalid")
    _sha256(epoch["identity_sha256"], "epoch identity SHA256")
    start = _timestamp(epoch["started_utc"], "epoch start timestamp")
    end = _timestamp(epoch["predecessor_authority_end_utc"], "predecessor end timestamp")
    if datetime.fromisoformat(end.replace("Z", "+00:00")) > datetime.fromisoformat(
        start.replace("Z", "+00:00")
    ):
        raise OperationalMetadataV5Error("predecessor authority ends after successor start")

    lifecycle = payload.get("lifecycle")
    if not isinstance(lifecycle, Mapping) or set(lifecycle) != {
        "requested_duration_s",
        "observed_duration_s",
        "health_error_count",
        "health_drop_count",
        "stable_double_read_passed",
        "formal_collection_valid",
        "runtime_identity_sha256",
        "runtime_code_identity_sha256",
        "part_count",
        "row_count",
        "event_id_count",
        "lifecycle_count",
        "cursor_count",
        "remote_payload_deleted",
    }:
        raise OperationalMetadataV5Error("manifest lifecycle fields drifted")
    observed = float(lifecycle["observed_duration_s"])
    if (
        float(lifecycle["requested_duration_s"]) != 3600.0
        or not 3500.0 <= observed <= 3700.0
        or lifecycle["health_error_count"] != 0
        or lifecycle["health_drop_count"] != 0
        or lifecycle["stable_double_read_passed"] is not True
        or lifecycle["formal_collection_valid"] is not True
    ):
        raise OperationalMetadataV5Error("fresh lifecycle is not a valid bounded 3600s window")
    for field in ("runtime_identity_sha256", "runtime_code_identity_sha256"):
        _sha256(lifecycle[field], f"lifecycle {field}")
    for field in ("part_count", "row_count", "event_id_count", "lifecycle_count", "cursor_count"):
        if not isinstance(lifecycle[field], int) or lifecycle[field] <= 0:
            raise OperationalMetadataV5Error(f"lifecycle {field} must be positive")

    health = payload.get("post_health")
    if not isinstance(health, Mapping) or set(health) != {
        "observed_utc",
        "process_alive",
        "health_error_count",
        "health_drop_count",
        "external_venues_enabled",
        "buy_e3_shadow_enabled",
        "buy_e3_companion_enabled",
        "buy_e3_policy_enabled",
        "tracked_dirty_count",
    }:
        raise OperationalMetadataV5Error("manifest post-health fields drifted")
    _timestamp(health["observed_utc"], "post-health observed timestamp")
    if health != {
        "observed_utc": health["observed_utc"],
        "process_alive": True,
        "health_error_count": 0,
        "health_drop_count": 0,
        "external_venues_enabled": False,
        "buy_e3_shadow_enabled": False,
        "buy_e3_companion_enabled": False,
        "buy_e3_policy_enabled": True,
        "tracked_dirty_count": 0,
    }:
        raise OperationalMetadataV5Error("fresh post-health is not clean no-external BUY E3")

    assertions = payload.get("source_assertions")
    if not isinstance(assertions, Mapping) or set(assertions) != {
        "lifecycle_admission",
        "post_health",
    }:
        raise OperationalMetadataV5Error("manifest source assertions drifted")
    if (
        not isinstance(assertions["lifecycle_admission"], Mapping)
        or not isinstance(assertions["post_health"], Mapping)
        or set(assertions["lifecycle_admission"]) != LIFECYCLE_ASSERTIONS
        or set(assertions["post_health"]) != POST_HEALTH_ASSERTIONS
    ):
        raise OperationalMetadataV5Error("manifest assertion names drifted")

    sources = payload.get("sources")
    if not isinstance(sources, Mapping) or set(sources) != set(SOURCE_IDENTITIES):
        raise OperationalMetadataV5Error("manifest source roles drifted")
    for role, binding in sources.items():
        if not isinstance(binding, Mapping) or set(binding) != set(CONTENT_FIELDS):
            raise OperationalMetadataV5Error(f"manifest {role} content binding drifted")
        if not Path(str(binding["path"])).expanduser().is_absolute():
            raise OperationalMetadataV5Error(f"manifest {role} source path must be absolute")

    chronology = tuple(
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        for value in (
            epoch["started_utc"],
            process["active_capture_utc"],
            health["observed_utc"],
            payload["generated_utc"],
        )
    )
    if chronology != tuple(sorted(chronology)):
        raise OperationalMetadataV5Error(
            "epoch/capture/post-health/metadata chronology is not monotonic"
        )
    binding = _content(payload, path, raw, 0o600)
    return dict(payload), binding


def _validate_sources(manifest: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    payloads: dict[str, Any] = {}
    bindings: dict[str, Any] = {}
    for role, expected in manifest["sources"].items():
        path = Path(str(expected["path"]))
        mode = int(str(expected["mode"]), 8)
        payload, raw, _metadata = _load_json(path, mode=mode)
        actual = _content(payload, path, raw, mode)
        if actual != dict(expected):
            raise OperationalMetadataV5Error(f"{role} exact content identity drifted")
        schema, status_value, canonical_field = SOURCE_IDENTITIES[role]
        if (
            (schema is not None and actual["schema_version"] != schema)
            or (status_value is not None and actual["status"] != status_value)
            or (canonical_field is not None and actual["canonical_field"] != canonical_field)
        ):
            raise OperationalMetadataV5Error(f"{role} schema/status identity drifted")
        payloads[role] = payload
        bindings[role] = actual

    if _without_path(bindings["direct_release"]) != {
        **RELEASE,
        "size_bytes": bindings["direct_release"]["size_bytes"],
        "mode": "0600",
    }:
        raise OperationalMetadataV5Error("direct release-v2 identity drifted")

    correction = payloads["config_correction"]
    corrected = correction.get("corrected_config_pair", {})
    if (
        corrected.get("active_sha256") != ACTIVE_CONFIG_SHA256
        or corrected.get("disabled_sha256") != DISABLED_CONFIG_SHA256
        or corrected.get("external_venues_enabled") is not False
        or correction.get("semantic_diff", {}).get("external_network_shadow_disabled") is not True
    ):
        raise OperationalMetadataV5Error("no-external config correction semantics drifted")

    resource = payloads["resource_gate"]
    if (
        not _execution_projection_matches(resource.get("runtime_execution"))
        or resource.get("config_correction") != _without_path(bindings["config_correction"])
        or resource.get("checks", {}).get("external_venues_disabled_throughout") is not True
    ):
        raise OperationalMetadataV5Error("resource-v6 cross-binding drifted")

    active = payloads["active_process_capture"]
    process = active.get("active_process", {})
    runtime = manifest["runtime"]
    if (
        active.get("runtime_authority") != _without_path(bindings["direct_release"])
        or active.get("resource_receipt") != _without_path(bindings["resource_gate"])
        or active.get("config_correction") != _without_path(bindings["config_correction"])
        or process.get("pid") != runtime["process"]["pid"]
        or process.get("pid_start_ticks") != runtime["process"]["pid_start_ticks"]
        or process.get("config_sha256") != ACTIVE_CONFIG_SHA256
        or process.get("artifact_sha256") != ARTIFACT_SHA256
        or active.get("checks", {}).get("external_venues_disabled") is not True
        or active.get("checks", {}).get("shadow_flags_disabled") is not True
    ):
        raise OperationalMetadataV5Error("active-v4 cross-binding drifted")

    cross = payloads["cross_host_admission"]
    portable = cross.get("portable_evidence", {})
    source_receipts = portable.get("source_receipts", {})
    expected_portable = {
        "config_correction": _without_path(bindings["config_correction"]),
        "current_host_resource_gate": _without_path(bindings["resource_gate"]),
        "active_process_capture": _without_path(bindings["active_process_capture"]),
    }
    if (
        portable.get("runtime_execution") != EXECUTION
        or portable.get("runtime_authority")
        != {
            **_without_path(bindings["direct_release"]),
            "execution": EXECUTION,
            "runtime_authority": True,
        }
        or portable.get("exact_artifact", {}).get("artifact_sha256") != ARTIFACT_SHA256
        or any(source_receipts.get(role) != binding for role, binding in expected_portable.items())
    ):
        raise OperationalMetadataV5Error("cross-host-v2 portable evidence drifted")

    assertions = manifest["source_assertions"]
    lifecycle_expected = {
        "epoch_id": runtime["epoch"]["epoch_id"],
        "epoch_identity_sha256": runtime["epoch"]["identity_sha256"],
        "config_sha256": ACTIVE_CONFIG_SHA256,
        "requested_duration_s": 3600,
        "observed_duration_s": manifest["lifecycle"]["observed_duration_s"],
        "health_error_count": 0,
        "health_drop_count": 0,
        "stable_double_read_passed": True,
        "formal_collection_valid": True,
    }
    for name, expected_value in lifecycle_expected.items():
        observed_value = _json_pointer(
            payloads["lifecycle_admission"], assertions["lifecycle_admission"][name]
        )
        if name in {"requested_duration_s", "observed_duration_s"}:
            if float(observed_value) != float(expected_value):
                raise OperationalMetadataV5Error(f"lifecycle {name} assertion drifted")
        elif observed_value != expected_value:
            raise OperationalMetadataV5Error(f"lifecycle {name} assertion drifted")

    health_expected = {
        "epoch_id": runtime["epoch"]["epoch_id"],
        "pid": runtime["process"]["pid"],
        "pid_start_ticks": runtime["process"]["pid_start_ticks"],
        "config_sha256": ACTIVE_CONFIG_SHA256,
        "process_alive": True,
        "health_error_count": 0,
        "health_drop_count": 0,
        "external_venues_enabled": False,
        "buy_e3_shadow_enabled": False,
        "buy_e3_companion_enabled": False,
        "buy_e3_policy_enabled": True,
        "observed_utc": manifest["post_health"]["observed_utc"],
    }
    for name, expected_value in health_expected.items():
        if (
            _json_pointer(payloads["post_health"], assertions["post_health"][name])
            != expected_value
        ):
            raise OperationalMetadataV5Error(f"post-health {name} assertion drifted")

    final_links = (
        ("final_activation_envelope", "cross_host_admission"),
        ("final_operational_completion", "final_activation_envelope"),
        ("final_operational_completion", "lifecycle_admission"),
        ("final_composition", "final_activation_envelope"),
        ("final_composition", "final_operational_completion"),
        ("final_attempt", "final_composition"),
        ("final_proof", "final_attempt"),
    )
    for parent, child in final_links:
        if not _contains_mapping(payloads[parent], _without_path(bindings[child])):
            raise OperationalMetadataV5Error(f"{parent} does not bind {child}")
    proof = payloads["final_proof"]
    if (
        proof.get("runtime_execution") != EXECUTION
        or proof.get("runtime_authority")
        != {
            **_without_path(bindings["direct_release"]),
            "execution": EXECUTION,
            "runtime_authority": True,
        }
        or proof.get("exact_artifact", {}).get("artifact_sha256") != ARTIFACT_SHA256
        or proof.get("config_correction") != _without_path(bindings["config_correction"])
        or proof.get("research_supported") is not False
        or proof.get("owner_risk_accepted") is not True
        or proof.get("authority_provenance", {}).get("new_authority_granted") is not False
        or proof.get("evidence_state", {}).get("external_venues_disabled_active_config_exact")
        is not True
    ):
        raise OperationalMetadataV5Error("final-v5 proof authority drifted")
    return payloads, bindings


def _finding_fingerprints(audit: Mapping[str, Any]) -> tuple[str, ...]:
    findings = audit.get("findings")
    if not isinstance(findings, list):
        raise OperationalMetadataV5Error("metadata audit findings are missing")
    return tuple(sorted(_canonical(finding) for finding in findings))


def _audit_baseline(audit: Mapping[str, Any]) -> dict[str, Any]:
    fingerprints = _finding_fingerprints(audit)
    return {
        "schema_version": audit.get("schema_version"),
        "mode": audit.get("mode"),
        "comparison_semantics": "after_finding_set_minus_before_finding_set",
        "preexisting_findings_may_remain": True,
        "required_new_finding_count": 0,
        "finding_fingerprints": list(fingerprints),
        "finding_set_sha256": _canonical(list(fingerprints)),
        "finding_count": len(fingerprints),
    }


def _assert_no_new_findings(
    baseline: Mapping[str, Any], after: Mapping[str, Any]
) -> dict[str, Any]:
    before = set(baseline.get("finding_fingerprints", []))
    observed = set(_finding_fingerprints(after))
    new = sorted(observed - before)
    if new:
        raise OperationalMetadataV5Error(f"metadata audit introduced {len(new)} new finding(s)")
    return {
        "comparison_semantics": "after_finding_set_minus_before_finding_set",
        "baseline_finding_count": len(before),
        "after_finding_count": len(observed),
        "new_finding_count": 0,
        "passed": True,
    }


def _catalog_context(catalog: Mapping[str, Any], transaction: Mapping[str, Any]) -> dict[str, Any]:
    entries = catalog.get("entries")
    if catalog.get("schema_version") != CATALOG_SCHEMA or not isinstance(entries, list):
        raise OperationalMetadataV5Error("predecessor catalog schema drifted")
    pointer = [
        entry for entry in entries if entry.get("artifact_id") == "repository-live-remote-current"
    ]
    old_id = transaction["predecessor_activation_artifact_id"]
    old = [entry for entry in entries if entry.get("artifact_id") == old_id]
    if len(pointer) != 1 or len(old) != 1:
        raise OperationalMetadataV5Error("predecessor catalog entries are ambiguous")
    return {
        "generated_at_utc": catalog.get("generated_at_utc"),
        "entry_count": len(entries),
        "pointer_entry": deepcopy(pointer[0]),
        "predecessor_activation_entry": deepcopy(old[0]),
    }


def _receipt_payload(
    manifest: Mapping[str, Any],
    manifest_binding: Mapping[str, Any],
    bindings: Mapping[str, Any],
    predecessor_pointer: Mapping[str, Any],
    catalog_context: Mapping[str, Any],
    audit_baseline: Mapping[str, Any],
) -> dict[str, Any]:
    runtime = manifest["runtime"]
    payload: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA,
        "status": RECEIPT_STATUS,
        "receipt_id": manifest["receipt_id"],
        "generated_utc": manifest["generated_utc"],
        "scope": "same_runtime_direct_v4_external_venue_shadow_disabled_fresh_epoch_evidence",
        "activation_manifest": dict(manifest_binding),
        "predecessor_pointer": {
            "file_sha256": manifest["transaction"]["predecessor_pointer"]["file_sha256"],
            "size_bytes": manifest["transaction"]["predecessor_pointer"]["size_bytes"],
            "snapshot": deepcopy(predecessor_pointer),
        },
        "predecessor_catalog_context": deepcopy(catalog_context),
        "catalog_transaction": {
            "predecessor_activation_artifact_id": manifest["transaction"][
                "predecessor_activation_artifact_id"
            ],
            "replacement_activation_artifact_id": manifest["transaction"][
                "replacement_activation_artifact_id"
            ],
        },
        "historical_superseded_v4": {
            "classification": SUPERSEDED_REASON,
            "activation_receipt": dict(manifest["transaction"]["predecessor_activation"]),
            "proof_evidence_release": dict(bindings["historical_v4_proof"]),
            "activation_bytes_preserved_immutable": True,
            "proof_bytes_preserved_immutable": True,
            "used_as_current_authority": False,
        },
        "current": {
            "host": deepcopy(runtime["host"]),
            "process": deepcopy(runtime["process"]),
            "epoch": deepcopy(runtime["epoch"]),
            "execution": dict(EXECUTION),
            "runtime_code_sha256": runtime["runtime_code_sha256"],
            "active_config_sha256": ACTIVE_CONFIG_SHA256,
            "disabled_config_sha256": DISABLED_CONFIG_SHA256,
            "artifact_sha256": ARTIFACT_SHA256,
            "release_remote_path": runtime["release_remote_path"],
            "release": dict(bindings["direct_release"]),
        },
        "current_operational_evidence": {
            role: dict(bindings[role])
            for role in (
                "config_correction",
                "resource_gate",
                "active_process_capture",
                "cross_host_admission",
                "lifecycle_admission",
                "post_health",
                "final_activation_envelope",
                "final_operational_completion",
                "final_composition",
                "final_attempt",
                "final_proof",
            )
        },
        "lifecycle": deepcopy(manifest["lifecycle"]),
        "post_health": deepcopy(manifest["post_health"]),
        "metadata_audit_baseline": deepcopy(audit_baseline),
        "authority": {
            "research_supported": False,
            "owner_risk_accepted": True,
            "outcome_informed_owner_override": True,
            "runtime_authority": "immutable_direct_v4_owner_release_v2",
            "runtime_authority_replaced": False,
            "proof_release_replaces_runtime_authority": False,
            "new_authority_granted": False,
        },
        "permissions": dict(NO_NEW_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[RECEIPT_CANONICAL_FIELD] = _document_sha(payload, RECEIPT_CANONICAL_FIELD)
    return payload


def _binding_from_bytes(
    path: Path, payload: Mapping[str, Any], data: bytes, canonical_field: str
) -> dict[str, Any]:
    return {
        "path": str(Path(os.path.abspath(os.fspath(path.expanduser())))),
        "schema_version": payload["schema_version"],
        "status": payload["status"],
        "file_sha256": _sha(data),
        "canonical_field": canonical_field,
        "canonical_sha256": payload[canonical_field],
        "size_bytes": len(data),
        "mode": "0600",
    }


def _pointer_payload(
    receipt: Mapping[str, Any], receipt_binding: Mapping[str, Any]
) -> dict[str, Any]:
    payload = deepcopy(receipt["predecessor_pointer"]["snapshot"])
    if payload.get("schema_version") != POINTER_SCHEMA or payload.get("status") != "current_active":
        raise OperationalMetadataV5Error("predecessor pointer schema/status drifted")
    runtime = receipt["current"]
    process = runtime["process"]
    epoch = runtime["epoch"]
    prior_epoch_id = payload.get("prospective_epoch_id")
    if prior_epoch_id == epoch["epoch_id"]:
        raise OperationalMetadataV5Error("successor epoch must differ from superseded v4 epoch")
    old_current_evidence = deepcopy(payload.get("current_operational_evidence"))
    if not isinstance(old_current_evidence, Mapping):
        raise OperationalMetadataV5Error("predecessor current evidence is missing")
    host = runtime["host"]
    for field in (
        "provider",
        "region",
        "city",
        "ssh_target",
        "public_ipv4",
        "instance_id",
        "instance_type",
        "repo_root",
    ):
        payload[field] = host[field]
    payload.update(
        {
            "status": "current_active",
            "activated_utc": epoch["started_utc"],
            "maker_started_utc": epoch["started_utc"],
            "runtime_identity_recorded_utc": process["runtime_identity_recorded_utc"],
            "prospective_epoch_id": epoch["epoch_id"],
            "prospective_epoch_started_ts_ns": epoch["started_ts_ns"],
            "prospective_epoch_identity_sha256": epoch["identity_sha256"],
            "runtime_code_sha256": runtime["runtime_code_sha256"],
            "config_sha256": ACTIVE_CONFIG_SHA256,
            "pointer_publication_status": RECEIPT_STATUS,
            "current_process_id": process["pid"],
            "current_process_start_ticks": process["pid_start_ticks"],
            "current_activation_receipt": {
                "path": receipt_binding["path"],
                "sha256": receipt_binding["file_sha256"],
                "canonical_sha256": receipt_binding["canonical_sha256"],
                "bytes": receipt_binding["size_bytes"],
            },
            "current_buy_e3_release": {
                "identity": RELEASE["schema_version"],
                "status": RELEASE["status"],
                "immutable_release_status": RELEASE["status"],
                "active_release_path": runtime["release_remote_path"],
                "active_release_file_sha256": RELEASE["file_sha256"],
                "active_release_canonical_sha256": RELEASE["canonical_sha256"],
                "research_supported": False,
                "owner_risk_accepted": True,
                "outcome_informed_owner_override": True,
                "scope": "BUY_exposure_increasing_executed_fill_only",
                "sell_owner_policy_unchanged": True,
                "shadow_or_companion_created": False,
                "external_venues_enabled": False,
                **{
                    "execution_commit": EXECUTION["execution_commit"],
                    "execution_tree": EXECUTION["execution_tree"],
                    "annotated_tag": EXECUTION["annotated_operational_tag"],
                    "annotated_tag_object": EXECUTION["annotated_operational_tag_object"],
                },
                "active_config_sha256": ACTIVE_CONFIG_SHA256,
                "disabled_config_sha256": DISABLED_CONFIG_SHA256,
                "post_release_evidence_status": final_v5.EVIDENCE_RELEASE_STATUS,
                "proof_release_replaces_runtime_authority": False,
            },
            "current_operational_evidence": {
                role: dict(binding)
                for role, binding in receipt["current_operational_evidence"].items()
            },
            "current_evidence_health": {
                "snapshot_utc": receipt["post_health"]["observed_utc"],
                "snapshot_semantics": "fresh_external_venues_disabled_post_health_plus_3600s_lifecycle_and_final_v5",
                "attestation_status": "accepted",
                "identity_exact": True,
                "tracked_dirty_count": 0,
                "live_maker_running": True,
                "process_id": process["pid"],
                "process_start_ticks": process["pid_start_ticks"],
                "lifecycle_session_id": epoch["epoch_id"],
                "lifecycle_requested_duration_s": receipt["lifecycle"]["requested_duration_s"],
                "lifecycle_observed_duration_s": receipt["lifecycle"]["observed_duration_s"],
                "lifecycle_error_count": 0,
                "lifecycle_drop_count": 0,
                "lifecycle_stable_double_read_passed": True,
                "external_venues_enabled": False,
                "buy_e3_shadow_enabled": False,
                "buy_e3_companion_enabled": False,
                "buy_e3_policy_enabled": True,
                "final_proof_status": final_v5.EVIDENCE_RELEASE_STATUS,
                "final_proof_canonical_sha256": receipt["current_operational_evidence"][
                    "final_proof"
                ]["canonical_sha256"],
                "research_supported": False,
                "owner_risk_accepted": True,
                "economic_outcomes_read": False,
                "validation_read": False,
                "sealed_holdout_read": False,
            },
            "historical_superseded_operational_evidence": [
                *deepcopy(payload.get("historical_superseded_operational_evidence", [])),
                {
                    "prospective_epoch_id": prior_epoch_id,
                    "classification": SUPERSEDED_REASON,
                    "activation_receipt": deepcopy(
                        receipt["historical_superseded_v4"]["activation_receipt"]
                    ),
                    "proof_evidence_release": deepcopy(
                        receipt["historical_superseded_v4"]["proof_evidence_release"]
                    ),
                    "operational_evidence_snapshot": old_current_evidence,
                    "current_authority": False,
                },
            ],
        }
    )

    epochs = payload.get("host_epochs")
    if not isinstance(epochs, list):
        raise OperationalMetadataV5Error("predecessor pointer host_epochs are missing")
    current_rows = [row for row in epochs if row.get("status") == "current_active"]
    if len(current_rows) != 1 or current_rows[0].get("prospective_epoch_id") != prior_epoch_id:
        raise OperationalMetadataV5Error("predecessor current epoch is ambiguous")
    if any(row.get("prospective_epoch_id") == epoch["epoch_id"] for row in epochs):
        raise OperationalMetadataV5Error("successor epoch already exists in predecessor pointer")
    old_row = current_rows[0]
    old_row["status"] = "historical_superseded_same_instance_epoch"
    old_row["superseded_reason"] = SUPERSEDED_REASON
    old_row["live_authority_end_utc"] = epoch["predecessor_authority_end_utc"]
    old_row["superseded_by_prospective_epoch_id"] = epoch["epoch_id"]
    epochs.append(
        {
            "epoch_key": ":".join(
                (
                    str(host["provider"]).lower(),
                    host["region"],
                    host["instance_id"],
                    epoch["epoch_id"],
                )
            ),
            "network_locator_key": ":".join(
                (str(host["provider"]).lower(), host["region"], host["public_ipv4"])
            ),
            "status": "current_active",
            "state_sync_start_utc": epoch["started_utc"],
            "runtime_identity_recorded_utc": process["runtime_identity_recorded_utc"],
            "prospective_epoch_id": epoch["epoch_id"],
            "prospective_epoch_started_ts_ns": epoch["started_ts_ns"],
            "prospective_epoch_identity_sha256": epoch["identity_sha256"],
            "public_ipv4": host["public_ipv4"],
            "instance_id": host["instance_id"],
            "instance_type": host["instance_type"],
            "runtime_code_sha256": runtime["runtime_code_sha256"],
            "config_sha256": ACTIVE_CONFIG_SHA256,
            "external_venues_enabled": False,
            "buy_e3_active_release_file_sha256": RELEASE["file_sha256"],
            "buy_e3_active_release_canonical_sha256": RELEASE["canonical_sha256"],
            "active_pid": process["pid"],
            "active_pid_start_ticks": process["pid_start_ticks"],
            **EXECUTION,
            "lifecycle_admission_manifest_sha256": receipt["current_operational_evidence"][
                "lifecycle_admission"
            ]["file_sha256"],
            "cross_host_admission_sha256": receipt["current_operational_evidence"][
                "cross_host_admission"
            ]["file_sha256"],
            "final_proof_sha256": receipt["current_operational_evidence"]["final_proof"][
                "file_sha256"
            ],
            "final_proof_canonical_sha256": receipt["current_operational_evidence"]["final_proof"][
                "canonical_sha256"
            ],
        }
    )

    gaps = payload.get("evidence_coverage_gaps")
    if not isinstance(gaps, list):
        raise OperationalMetadataV5Error("predecessor pointer coverage gaps are missing")
    if epoch["predecessor_authority_end_utc"] != epoch["started_utc"]:
        gaps.append(
            {
                "interval_semantics": "half_open_utc",
                "start_utc": epoch["predecessor_authority_end_utc"],
                "end_utc": epoch["started_utc"],
                "classification": "external_venue_shadow_disable_restart_transition_fail_closed",
                "reason": "no single admitted process epoch spans the config-correction restart",
                "rows_in_interval_must_fail_closed": True,
            }
        )

    policy = payload.get("current_query_policy")
    if not isinstance(policy, Mapping) or not isinstance(
        policy.get("fill_trade_query_order"), list
    ):
        raise OperationalMetadataV5Error("predecessor query policy is missing")
    prior_order = list(policy["fill_trade_query_order"])
    policy["fill_trade_query_order"] = [
        "partition_request_by_instance_id_and_prospective_epoch_id_before_reading_rows",
        f"query_current_epoch_{epoch['epoch_id']}_from_{epoch['started_utc']}",
        f"query_superseded_epoch_{prior_epoch_id}_only_before_{epoch['predecessor_authority_end_utc']}",
        *[
            row
            for row in prior_order
            if row
            != "partition_request_by_instance_id_and_prospective_epoch_id_before_reading_rows"
            and str(prior_epoch_id) not in row
        ],
    ]
    policy["same_instance_epoch_rule"] = (
        "external_shadow_enabled_v4_and_external_shadow_disabled_successor_are_distinct"
    )
    policy["current_operational_metadata_updated_utc"] = receipt["generated_utc"]
    return payload


def _catalog_payload(
    predecessor: Mapping[str, Any],
    receipt: Mapping[str, Any],
    receipt_binding: Mapping[str, Any],
    pointer_data: bytes,
) -> dict[str, Any]:
    payload = deepcopy(predecessor)
    if payload.get("schema_version") != CATALOG_SCHEMA or not isinstance(
        payload.get("entries"), list
    ):
        raise OperationalMetadataV5Error("predecessor catalog schema drifted")
    context = receipt["predecessor_catalog_context"]
    if (
        _catalog_context(
            payload,
            {
                "predecessor_activation_artifact_id": receipt["catalog_transaction"][
                    "predecessor_activation_artifact_id"
                ]
            },
        )
        != context
    ):
        raise OperationalMetadataV5Error("predecessor catalog patch context drifted")
    entries = payload["entries"]
    pointer_entry = next(
        entry for entry in entries if entry.get("artifact_id") == "repository-live-remote-current"
    )
    old_id = receipt["catalog_transaction"]["predecessor_activation_artifact_id"]
    old_entry = next(entry for entry in entries if entry.get("artifact_id") == old_id)
    new_id = receipt["catalog_transaction"]["replacement_activation_artifact_id"]
    if any(entry.get("artifact_id") == new_id for entry in entries):
        raise OperationalMetadataV5Error("replacement catalog artifact id already exists")
    pointer_entry.update(
        {
            "sha256": _sha(pointer_data),
            "bytes": len(pointer_data),
            "last_verified_utc": receipt["generated_utc"],
            "notes": "Mutable current remote authority is bound only to the fresh no-external-shadow resource-v6, active-v4, cross-host-v2, 3600-second lifecycle, post-health, and final-v5 evidence chain.",
        }
    )
    old_entry["operational_status"] = SUPERSEDED_REASON
    old_entry["superseded_by_artifact_id"] = new_id
    old_entry["last_verified_utc"] = receipt["generated_utc"]
    entries.append(
        {
            "artifact_id": new_id,
            "role": "live_direct_v4_external_venues_disabled_evidence_completion_receipt",
            "local_path": receipt_binding["path"],
            "source_document": None,
            "source_line_before_migration": None,
            "sha256": receipt_binding["file_sha256"],
            "bytes": receipt_binding["size_bytes"],
            "availability": "private_not_distributed",
            "panel_role": "operational",
            "read_gate": "owner_only",
            "last_verified_utc": receipt["generated_utc"],
            "related_public_docs": ["docs/live_host_and_historical_data_access_20260811.md"],
            "public_projection": None,
            "notes": "Private create-only activation receipt for the fresh external-venues-disabled BUY E3 process. No research, action, or live authority is granted by this metadata successor.",
        }
    )
    payload["generated_at_utc"] = receipt["generated_utc"]
    return payload


def _catalog_predecessor_from_successor(
    successor: Mapping[str, Any], receipt: Mapping[str, Any]
) -> dict[str, Any]:
    payload = deepcopy(successor)
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise OperationalMetadataV5Error("successor catalog entries are missing")
    context = receipt["predecessor_catalog_context"]
    new_id = receipt["catalog_transaction"]["replacement_activation_artifact_id"]
    old_id = receipt["catalog_transaction"]["predecessor_activation_artifact_id"]
    if len([entry for entry in entries if entry.get("artifact_id") == new_id]) != 1:
        raise OperationalMetadataV5Error("successor catalog replacement entry is ambiguous")
    entries[:] = [entry for entry in entries if entry.get("artifact_id") != new_id]
    pointer_indexes = [
        index
        for index, entry in enumerate(entries)
        if entry.get("artifact_id") == "repository-live-remote-current"
    ]
    old_indexes = [
        index for index, entry in enumerate(entries) if entry.get("artifact_id") == old_id
    ]
    if len(pointer_indexes) != 1 or len(old_indexes) != 1:
        raise OperationalMetadataV5Error("successor catalog patch entries are ambiguous")
    entries[pointer_indexes[0]] = deepcopy(context["pointer_entry"])
    entries[old_indexes[0]] = deepcopy(context["predecessor_activation_entry"])
    payload["generated_at_utc"] = context["generated_at_utc"]
    return payload


def _reject_secrets(value: Any, path: str = "$") -> None:
    forbidden_keys = {
        "api_key",
        "apikey",
        "api_secret",
        "apisecret",
        "secret",
        "password",
        "token",
        "private_key",
        "secret_access_key",
    }
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key).lower() in forbidden_keys:
                raise OperationalMetadataV5Error(f"secret-shaped catalog field: {path}.{key}")
            _reject_secrets(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_secrets(child, f"{path}[{index}]")
    elif isinstance(value, str):
        patterns = (
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
            r"AKIA[0-9A-Z]{16}",
            r"(?i)(?:api[_-]?key|api[_-]?secret|password|secret[_-]?access[_-]?key)\s*[:=]\s*[^\s,}]+",
            r"(?i)\b(?:https?|ssh)://[^/\s:@]+:[^/\s@]+@",
        )
        if any(re.search(pattern, value) for pattern in patterns):
            raise OperationalMetadataV5Error(f"credential-shaped catalog value: {path}")


def _resolver_projection(pointer: Mapping[str, Any]) -> dict[str, Any]:
    required = (
        "provider",
        "region",
        "city",
        "ssh_target",
        "public_ipv4",
        "instance_id",
        "instance_type",
        "repo_root",
        "prospective_epoch_id",
        "prospective_epoch_identity_sha256",
        "current_process_id",
        "current_process_start_ticks",
        "config_sha256",
        "runtime_code_sha256",
    )
    if pointer.get("schema_version") != POINTER_SCHEMA or pointer.get("status") != "current_active":
        raise OperationalMetadataV5Error("successor pointer is not current_active")
    projection = {field: pointer.get(field) for field in required}
    if any(value in (None, "") for value in projection.values()):
        raise OperationalMetadataV5Error("successor resolver projection is incomplete")
    return projection


def _validate_metadata(
    manifest: Mapping[str, Any],
    receipt: Mapping[str, Any],
    receipt_data: bytes,
    pointer: Mapping[str, Any],
    pointer_data: bytes,
    catalog: Mapping[str, Any],
    catalog_data: bytes,
) -> dict[str, Any]:
    if receipt.get(RECEIPT_CANONICAL_FIELD) != _document_sha(receipt, RECEIPT_CANONICAL_FIELD):
        raise OperationalMetadataV5Error("replacement receipt canonical drifted")
    transaction = manifest["transaction"]
    receipt_path = Path(str(transaction["replacement_receipt_path"]))
    receipt_binding = _binding_from_bytes(
        receipt_path, receipt, receipt_data, RECEIPT_CANONICAL_FIELD
    )
    if pointer.get("current_activation_receipt") != {
        "path": receipt_binding["path"],
        "sha256": receipt_binding["file_sha256"],
        "canonical_sha256": receipt_binding["canonical_sha256"],
        "bytes": receipt_binding["size_bytes"],
    }:
        raise OperationalMetadataV5Error("pointer-to-receipt binding drifted")
    runtime = manifest["runtime"]
    expected_resolver = {
        **runtime["host"],
        "prospective_epoch_id": runtime["epoch"]["epoch_id"],
        "prospective_epoch_identity_sha256": runtime["epoch"]["identity_sha256"],
        "current_process_id": runtime["process"]["pid"],
        "current_process_start_ticks": runtime["process"]["pid_start_ticks"],
        "config_sha256": ACTIVE_CONFIG_SHA256,
        "runtime_code_sha256": runtime["runtime_code_sha256"],
    }
    if _resolver_projection(pointer) != expected_resolver:
        raise OperationalMetadataV5Error("successor resolver identity drifted")
    current_epochs = [
        row for row in pointer["host_epochs"] if row.get("status") == "current_active"
    ]
    if (
        len(current_epochs) != 1
        or current_epochs[0].get("prospective_epoch_id") != runtime["epoch"]["epoch_id"]
    ):
        raise OperationalMetadataV5Error("successor current epoch is ambiguous")
    historical = pointer.get("historical_superseded_operational_evidence", [])
    if not any(row.get("classification") == SUPERSEDED_REASON for row in historical):
        raise OperationalMetadataV5Error("superseded v4 history is missing")
    if set(pointer.get("current_operational_evidence", {})) != {
        "config_correction",
        "resource_gate",
        "active_process_capture",
        "cross_host_admission",
        "lifecycle_admission",
        "post_health",
        "final_activation_envelope",
        "final_operational_completion",
        "final_composition",
        "final_attempt",
        "final_proof",
    }:
        raise OperationalMetadataV5Error("successor current evidence roles drifted")
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise OperationalMetadataV5Error("successor catalog entries are missing")
    pointer_entries = [
        entry for entry in entries if entry.get("artifact_id") == "repository-live-remote-current"
    ]
    receipt_entries = [
        entry
        for entry in entries
        if entry.get("artifact_id") == transaction["replacement_activation_artifact_id"]
    ]
    old_entries = [
        entry
        for entry in entries
        if entry.get("artifact_id") == transaction["predecessor_activation_artifact_id"]
    ]
    if len(pointer_entries) != 1 or len(receipt_entries) != 1 or len(old_entries) != 1:
        raise OperationalMetadataV5Error("successor catalog cross-binding is ambiguous")
    if (pointer_entries[0].get("sha256"), pointer_entries[0].get("bytes")) != (
        _sha(pointer_data),
        len(pointer_data),
    ) or (receipt_entries[0].get("sha256"), receipt_entries[0].get("bytes")) != (
        _sha(receipt_data),
        len(receipt_data),
    ):
        raise OperationalMetadataV5Error("successor catalog content binding drifted")
    if old_entries[0].get("operational_status") != SUPERSEDED_REASON:
        raise OperationalMetadataV5Error("catalog does not preserve v4 as superseded history")
    _reject_secrets(receipt)
    _reject_secrets(pointer)
    _reject_secrets(catalog)
    return {
        "receipt": {
            "file_sha256": _sha(receipt_data),
            "canonical_sha256": receipt[RECEIPT_CANONICAL_FIELD],
            "size_bytes": len(receipt_data),
        },
        "pointer": {"file_sha256": _sha(pointer_data), "size_bytes": len(pointer_data)},
        "catalog": {"file_sha256": _sha(catalog_data), "size_bytes": len(catalog_data)},
        "resolver_exact": True,
        "catalog_secret_scan_passed": True,
        "cross_bind_passed": True,
    }


def _fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_new(path: Path, data: bytes) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(data)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OperationalMetadataV5Error(f"short write: {path}")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _pending(path: Path, kind: str) -> Path:
    return path.with_name(f".{path.name}.{kind}-pending-v5")


def _read_exact_any_link(path: Path, data: bytes, links: frozenset[int]) -> os.stat_result:
    observed, metadata = _read_regular(path, mode=0o600, allowed_nlinks=links)
    if observed != data:
        raise OperationalMetadataV5Error(f"pending/published bytes differ from plan: {path}")
    return metadata


def _publish_create_only(path: Path, data: bytes) -> None:
    """Create an immutable file and recover the link-before-unlink crash point."""

    pending = _pending(path, "create")
    if path.exists():
        metadata = _read_exact_any_link(path, data, frozenset({1, 2}))
        if metadata.st_nlink == 2:
            if not pending.exists():
                raise OperationalMetadataV5Error("published nlink=2 receipt lacks recovery link")
            pending_meta = _read_exact_any_link(pending, data, frozenset({2}))
            if (metadata.st_dev, metadata.st_ino) != (pending_meta.st_dev, pending_meta.st_ino):
                raise OperationalMetadataV5Error("receipt recovery hardlink inode drifted")
            pending.unlink()
            _fsync_dir(path.parent)
            _read_exact_any_link(path, data, frozenset({1}))
        elif pending.exists():
            raise OperationalMetadataV5Error("orphan receipt pending path is ambiguous")
        return
    if pending.exists():
        _read_exact_any_link(pending, data, frozenset({1}))
    else:
        _write_new(pending, data)
        _fsync_dir(path.parent)
    os.link(pending, path, follow_symlinks=False)
    _fsync_dir(path.parent)
    # A crash here leaves two exact links.  The first branch repairs it.
    pending.unlink()
    _fsync_dir(path.parent)
    _read_exact_any_link(path, data, frozenset({1}))


def _atomic_replace(path: Path, data: bytes, *, kind: str) -> None:
    pending = _pending(path, kind)
    if pending.exists():
        _read_exact_any_link(pending, data, frozenset({1}))
    else:
        _write_new(pending, data)
        _fsync_dir(path.parent)
    os.replace(pending, path)
    _fsync_dir(path.parent)
    _read_exact_any_link(path, data, frozenset({1}))


AuditFn = Callable[[Path], Mapping[str, Any]]
FailureFn = Callable[[str], None]


def _default_audit(root: Path) -> Mapping[str, Any]:
    return audit_private_evidence.audit(
        root,
        mode=audit_private_evidence.METADATA_ONLY,
        deny_locked=True,
        allowlist_manifest=None,
    )


def execute(
    manifest_path: Path,
    *,
    apply: bool = False,
    audit_fn: AuditFn = _default_audit,
    failure_hook: FailureFn | None = None,
) -> dict[str, Any]:
    manifest, manifest_binding = _validate_manifest(manifest_path)
    transaction = manifest["transaction"]
    pointer_path = Path(str(transaction["pointer_path"]))
    catalog_path = Path(str(transaction["catalog_path"]))
    receipt_path = Path(str(transaction["replacement_receipt_path"]))
    if len({pointer_path.parent, catalog_path.parent, receipt_path.parent}) != 1:
        raise OperationalMetadataV5Error("metadata transaction files must share one directory")
    if not pointer_path.parent.is_dir():
        raise OperationalMetadataV5Error("metadata transaction directory is unavailable")

    lock_descriptor = os.open(pointer_path.parent, os.O_RDONLY)
    try:
        fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
        source_payloads, bindings = _validate_sources(manifest)
        del source_payloads

        pointer_payload, pointer_raw, _pointer_meta = _load_json(pointer_path, mode=0o600)
        catalog_payload, catalog_raw, _catalog_meta = _load_json(catalog_path, mode=0o600)
        predecessor_activation_path = Path(str(transaction["predecessor_activation"]["path"]))
        old_activation, old_raw, _old_meta = _load_json(predecessor_activation_path, mode=0o600)
        old_binding = _content(old_activation, predecessor_activation_path, old_raw, 0o600)
        if old_binding != dict(transaction["predecessor_activation"]):
            raise OperationalMetadataV5Error("predecessor v4 activation receipt drifted")

        pointer_old = (_sha(pointer_raw), len(pointer_raw)) == (
            transaction["predecessor_pointer"]["file_sha256"],
            transaction["predecessor_pointer"]["size_bytes"],
        )
        catalog_old = (_sha(catalog_raw), len(catalog_raw)) == (
            transaction["predecessor_catalog"]["file_sha256"],
            transaction["predecessor_catalog"]["size_bytes"],
        )

        receipt_exists = receipt_path.exists()
        receipt_pending_path = _pending(receipt_path, "create")
        receipt_pending_exists = receipt_pending_path.exists()
        receipt_state = "missing"
        if not receipt_exists and not receipt_pending_exists:
            if not pointer_old or not catalog_old:
                raise OperationalMetadataV5Error(
                    "publication order inverted: pointer/catalog advanced before receipt"
                )
            audit_baseline = _audit_baseline(audit_fn(Path(str(manifest["repository_root"]))))
            context = _catalog_context(catalog_payload, transaction)
            planned_receipt = _receipt_payload(
                manifest,
                manifest_binding,
                bindings,
                pointer_payload,
                context,
                audit_baseline,
            )
            receipt_data = _render(planned_receipt)
        else:
            if not pointer_old and not receipt_exists:
                raise OperationalMetadataV5Error(
                    "publication order inverted: pointer advanced before receipt"
                )
            candidate = receipt_path if receipt_exists else receipt_pending_path
            allowed_links = frozenset({1, 2}) if receipt_exists else frozenset({1})
            published, receipt_data, receipt_meta = _load_json(
                candidate, mode=0o600, allowed_nlinks=allowed_links
            )
            receipt_state = "published" if receipt_exists else "pending_create_only"
            if receipt_exists and receipt_meta.st_nlink == 2:
                if not receipt_pending_path.exists():
                    raise OperationalMetadataV5Error("nlink=2 receipt is not recoverable")
                pending_meta = _read_exact_any_link(
                    receipt_pending_path, receipt_data, frozenset({2})
                )
                if (receipt_meta.st_dev, receipt_meta.st_ino) != (
                    pending_meta.st_dev,
                    pending_meta.st_ino,
                ):
                    raise OperationalMetadataV5Error("nlink=2 recovery inode drifted")
                receipt_state = "published_recoverable_nlink2"
            planned_receipt = published
            if (
                planned_receipt.get("activation_manifest") != manifest_binding
                or planned_receipt.get("predecessor_pointer", {}).get("file_sha256")
                != transaction["predecessor_pointer"]["file_sha256"]
            ):
                raise OperationalMetadataV5Error("published receipt manifest/precondition drifted")
            expected_receipt = _receipt_payload(
                manifest,
                manifest_binding,
                bindings,
                planned_receipt["predecessor_pointer"]["snapshot"],
                planned_receipt["predecessor_catalog_context"],
                planned_receipt["metadata_audit_baseline"],
            )
            if planned_receipt != expected_receipt or receipt_data != _render(expected_receipt):
                raise OperationalMetadataV5Error("published replacement receipt differs from plan")

        receipt_binding = _binding_from_bytes(
            receipt_path, planned_receipt, receipt_data, RECEIPT_CANONICAL_FIELD
        )
        planned_pointer = _pointer_payload(planned_receipt, receipt_binding)
        planned_pointer_data = _render(planned_pointer)
        if not pointer_old and pointer_raw != planned_pointer_data:
            raise OperationalMetadataV5Error("pointer is neither predecessor nor exact successor")
        pointer_new = pointer_raw == planned_pointer_data

        if catalog_old:
            planned_catalog = _catalog_payload(
                catalog_payload, planned_receipt, receipt_binding, planned_pointer_data
            )
            planned_catalog_data = _render(planned_catalog)
            catalog_new = False
        else:
            predecessor_catalog = _catalog_predecessor_from_successor(
                catalog_payload, planned_receipt
            )
            predecessor_catalog_data = _render(predecessor_catalog)
            if (_sha(predecessor_catalog_data), len(predecessor_catalog_data)) != (
                transaction["predecessor_catalog"]["file_sha256"],
                transaction["predecessor_catalog"]["size_bytes"],
            ):
                raise OperationalMetadataV5Error("successor catalog cannot reconstruct predecessor")
            planned_catalog = _catalog_payload(
                predecessor_catalog, planned_receipt, receipt_binding, planned_pointer_data
            )
            planned_catalog_data = _render(planned_catalog)
            if catalog_raw != planned_catalog_data:
                raise OperationalMetadataV5Error(
                    "catalog is neither predecessor nor exact successor"
                )
            catalog_new = True

        if catalog_new and not pointer_new:
            raise OperationalMetadataV5Error(
                "publication order inverted: catalog advanced before pointer"
            )
        validated = _validate_metadata(
            manifest,
            planned_receipt,
            receipt_data,
            planned_pointer,
            planned_pointer_data,
            planned_catalog,
            planned_catalog_data,
        )
        state_before = {
            "receipt": receipt_state,
            "pointer": "successor" if pointer_new else "predecessor",
            "catalog": "successor" if catalog_new else "predecessor",
        }
        result: dict[str, Any] = {
            **validated,
            "mode": "apply" if apply else "dry_run",
            "status": "planned_or_resumed_exact_transaction",
            "state_before": state_before,
            "ordered_steps": ["receipt", "pointer", "catalog"],
            "metadata_audit_contract": {
                "comparison_semantics": "after_finding_set_minus_before_finding_set",
                "preexisting_findings_may_remain": True,
                "required_new_finding_count": 0,
            },
            "source_count": len(bindings),
            "writes_performed": False,
        }
        if not apply:
            return result

        _publish_create_only(receipt_path, receipt_data)
        if failure_hook is not None:
            failure_hook("receipt")
        if not pointer_new:
            _atomic_replace(pointer_path, planned_pointer_data, kind="pointer")
        if failure_hook is not None:
            failure_hook("pointer")
        if not catalog_new:
            _atomic_replace(catalog_path, planned_catalog_data, kind="catalog")
        if failure_hook is not None:
            failure_hook("catalog")

        actual_receipt, actual_receipt_raw, receipt_meta = _load_json(receipt_path, mode=0o600)
        actual_pointer, actual_pointer_raw, pointer_meta = _load_json(pointer_path, mode=0o600)
        actual_catalog, actual_catalog_raw, catalog_meta = _load_json(catalog_path, mode=0o600)
        if (actual_receipt_raw, actual_pointer_raw, actual_catalog_raw) != (
            receipt_data,
            planned_pointer_data,
            planned_catalog_data,
        ) or (receipt_meta.st_nlink, pointer_meta.st_nlink, catalog_meta.st_nlink) != (1, 1, 1):
            raise OperationalMetadataV5Error("post-publication byte/link identity drifted")
        _validate_metadata(
            manifest,
            actual_receipt,
            actual_receipt_raw,
            actual_pointer,
            actual_pointer_raw,
            actual_catalog,
            actual_catalog_raw,
        )
        audit_after = audit_fn(Path(str(manifest["repository_root"])))
        result["metadata_audit"] = _assert_no_new_findings(
            planned_receipt["metadata_audit_baseline"], audit_after
        )
        result["writes_performed"] = True
        result["post_write_verified"] = True
        result["state_after"] = {
            "receipt": "published_nlink1",
            "pointer": "successor",
            "catalog": "successor",
        }
        return result
    finally:
        fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
        os.close(lock_descriptor)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = execute(args.manifest, apply=args.apply)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
