#!/usr/bin/env python3
"""Compose and independently validate the owner BUY E3 final evidence chain.

The tool is intentionally schema-driven.  It does not import the parity or
deployment implementations whose receipts it validates, and it never opens an
economic row file.  Every JSON dependency is re-read with duplicate-key and
non-finite-number rejection, content-addressed, permission checked, and then
cross-validated against its parents before an immutable receipt is admitted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

IDENTITY: Final = "causal_multichannel_window_boolean_cooldown_owner_buy_e3_v1"
SOURCE_ROLE_SCHEMA: Final = f"{IDENTITY}.source_role_resolution_receipt.v1"
COMPOSITION_SCHEMA: Final = f"{IDENTITY}.final_composition_receipt.v1"
SOURCE_ROLE_IDENTITY: Final = f"{IDENTITY}.source_role_resolution"
COMPOSITION_IDENTITY: Final = f"{IDENTITY}.final_composition"
EXPECTED_DAY_COUNT: Final = 30

LAYER1_ROLE: Final = "parity_research_compiled"
LAYER2_ROLE: Final = "parity_development_snapshot"
LAYER3_ROLE: Final = "parity_streaming_offline"
LAYER4_DAY_PREFIX: Final = "layer4_day::"
LAYER4_MECHANICS_ROLE: Final = "layer4_mechanics"

STATIC_STATUS_BY_ROLE: Final[dict[str, str]] = {
    "formal_buy_component_manifest": "formal_buy_component_manifest_bound",
    "exact_predicate_bundle": "exact_predicate_bundle_bound",
}

BASE_ROLE_ORDER: Final[tuple[str, ...]] = (
    "formal_buy_component_manifest",
    "formal_buy_component_validation",
    "joint_closeout_manifest",
    "owner_decision",
    "attempt_execution_manifest",
    "source_execution_manifest",
    "cpp_builder_preflight",
    "cpp_quick_preflight",
    "cpp_qualification",
    "owner_execution_preflight",
    "source_role_resolution",
    "label_materialization",
    "refit_receipt",
    "exact_artifact_manifest",
    "exact_policy",
    "exact_predicate_bundle",
    LAYER1_ROLE,
    LAYER2_ROLE,
    LAYER3_ROLE,
    LAYER4_MECHANICS_ROLE,
    "layer4_contract",
)

TAIL_ROLE_ORDER: Final[tuple[str, ...]] = (
    "layer4_final",
    "sell_54_case",
    "runtime_regression",
    "deployment_gate",
)

CANONICAL_FIELDS: Final[dict[str, tuple[str, ...]]] = {
    "formal_buy_component_manifest": ("canonical_artifact_manifest_sha256",),
    "formal_buy_component_validation": ("canonical_validation_receipt_sha256",),
    "joint_closeout_manifest": ("canonical_manifest_sha256",),
    "owner_decision": ("canonical_owner_decision_sha256",),
    "attempt_execution_manifest": ("canonical_execution_manifest_sha256",),
    "source_execution_manifest": ("canonical_execution_manifest_sha256",),
    "cpp_builder_preflight": ("canonical_receipt_sha256",),
    "cpp_quick_preflight": ("canonical_receipt_sha256",),
    "cpp_qualification": ("canonical_receipt_sha256",),
    "owner_execution_preflight": ("canonical_preflight_receipt_sha256",),
    "source_role_resolution": ("canonical_source_role_resolution_receipt_sha256",),
    "label_materialization": ("canonical_materialization_receipt_sha256",),
    "refit_receipt": ("canonical_refit_run_receipt_sha256",),
    "exact_artifact_manifest": ("artifact_sha256",),
    "exact_policy": ("canonical_sha256",),
    "exact_predicate_bundle": ("canonical_sha256",),
    LAYER1_ROLE: ("canonical_receipt_sha256",),
    LAYER2_ROLE: ("canonical_receipt_sha256",),
    LAYER3_ROLE: ("canonical_receipt_sha256",),
    LAYER4_MECHANICS_ROLE: ("canonical_mechanics_identity_receipt_sha256",),
    "layer4_contract": ("canonical_contract_sha256",),
    "layer4_final": (
        "canonical_receipt_sha256",
        "canonical_layer4_receipt_sha256",
    ),
    "sell_54_case": ("canonical_receipt_sha256",),
    "runtime_regression": ("canonical_receipt_sha256",),
    "deployment_gate": ("canonical_amendment_receipt_sha256",),
}

FALSE_EVIDENCE_KEYS: Final[frozenset[str]] = frozenset(
    {
        "validation_read",
        "validation_accessed",
        "sealed_holdout_read",
        "sealed_holdout_accessed",
        "action_authorized",
        "live_authorized",
        "economic_values_exposed",
        "economic_values_used_for_selection",
        "economic_values_persisted",
        "economic_values_persisted_in_receipt",
        "hypothetical_actions_scored",
        "hypothetical_live_actions_scored",
        "new_economic_arm_run",
        "exact_artifact_oof_available",
        "exact_final_artifact_oof_available",
        "old_oof_estimate_applies_to_exact_artifact",
        "old_oof_estimate_applies_to_exact_owner_artifact",
    }
)

_SHA_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
_UTC_DAY_RE: Final = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_MECHANICS_BODY_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "schema_version",
        "selected_days",
        "file_sha256",
        "metadata_sha256",
        "boolean_features_sha256",
        "primitive_boolean_features_sha256",
        "continuous_features_sha256",
        "exact_owner_actions_sha256",
        "replay_inputs_sha256",
        "predicate_view_receipt",
        "bindings",
        "economic_outcomes_present",
    }
)


class FinalCompositionError(RuntimeError):
    """Raised when a final evidence dependency fails closed."""


@dataclass(frozen=True, slots=True)
class CompositionInputs:
    formal_buy_component_manifest: Path
    formal_buy_component_validation: Path
    joint_closeout_manifest: Path
    owner_decision: Path
    attempt_execution_manifest: Path
    source_execution_manifest: Path
    cpp_builder_preflight: Path
    cpp_quick_preflight: Path
    cpp_qualification: Path
    owner_execution_preflight: Path
    label_materialization: Path
    refit_receipt: Path
    exact_artifact_manifest: Path
    exact_policy: Path
    exact_predicate_bundle: Path
    parity_research_compiled: Path
    parity_development_snapshot: Path
    parity_streaming_offline: Path
    layer4_mechanics: Path
    layer4_contract: Path
    layer4_day_receipts: tuple[Path, ...]
    layer4_final: Path
    sell_54_case: Path
    runtime_regression: Path
    deployment_gate: Path


def canonical_sha256(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise FinalCompositionError("payload is not canonical JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


def document_sha256(payload: Mapping[str, Any], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return canonical_sha256(body)


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                payload,
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            ).encode("ascii")
            + b"\n"
        )
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise FinalCompositionError("receipt is not finite canonical JSON") from exc


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1 << 20), b""):
                digest.update(block)
    except OSError as exc:
        raise FinalCompositionError(f"cannot hash evidence file: {path}") from exc
    return digest.hexdigest()


def _reject_constant(value: str) -> Any:
    raise FinalCompositionError(f"non-finite JSON constant is forbidden: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise FinalCompositionError(f"duplicate JSON key is forbidden: {key}")
        result[key] = value
    return result


def strict_load_json(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        text = raw.decode("ascii")
        payload = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except FinalCompositionError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FinalCompositionError(f"evidence JSON is unreadable: {path}") from exc
    if not isinstance(payload, dict):
        raise FinalCompositionError(f"evidence JSON root must be an object: {path}")
    return payload


def _require_sha(value: Any, label: str) -> str:
    normalized = str(value)
    if _SHA_RE.fullmatch(normalized) is None:
        raise FinalCompositionError(f"{label} is not a lowercase SHA256")
    return normalized


def _require_bool(value: Any, expected: bool, label: str) -> None:
    if value is not expected:
        raise FinalCompositionError(f"{label} must be {expected}")


def _field(payload: Mapping[str, Any], *path: str) -> Any:
    value: Any = payload
    for part in path:
        if not isinstance(value, Mapping) or part not in value:
            raise FinalCompositionError(f"required field is missing: {'.'.join(path)}")
        value = value[part]
    return value


def _optional_field(payload: Mapping[str, Any], *path: str) -> Any:
    value: Any = payload
    for part in path:
        if not isinstance(value, Mapping) or part not in value:
            return None
        value = value[part]
    return value


def _require_equal(actual: Any, expected: Any, label: str) -> None:
    if actual != expected:
        raise FinalCompositionError(f"{label} drifted")


def _evidence_root(path: Path) -> Path:
    root = path.expanduser()
    try:
        root_lstat = root.lstat()
    except OSError as exc:
        raise FinalCompositionError("evidence root is unavailable") from exc
    if stat.S_ISLNK(root_lstat.st_mode) or not stat.S_ISDIR(root_lstat.st_mode):
        raise FinalCompositionError("evidence root must be a real directory")
    return root.resolve(strict=True)


def _lexical_under_root(path: Path, root: Path) -> Path:
    candidate = path.expanduser()
    if not candidate.is_absolute():
        candidate = root / candidate
    candidate = Path(os.path.abspath(candidate))
    try:
        common = Path(os.path.commonpath((str(root), str(candidate))))
    except ValueError as exc:
        raise FinalCompositionError("evidence path is outside its root") from exc
    if common != root:
        raise FinalCompositionError(f"evidence path escapes its root: {candidate}")
    return candidate


def _assert_no_symlink_chain(path: Path, root: Path, *, final_may_be_missing: bool) -> None:
    lexical = _lexical_under_root(path, root)
    relative = lexical.relative_to(root)
    current = root
    for index, part in enumerate(relative.parts):
        current = current / part
        is_final = index == len(relative.parts) - 1
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            if final_may_be_missing and is_final:
                return
            raise FinalCompositionError(f"evidence path component is missing: {current}") from None
        except OSError as exc:
            raise FinalCompositionError(f"cannot inspect evidence path: {current}") from exc
        if stat.S_ISLNK(metadata.st_mode):
            raise FinalCompositionError(f"symlink evidence is forbidden: {current}")


def _admit_input_path(path: Path, root: Path) -> Path:
    lexical = _lexical_under_root(path, root)
    _assert_no_symlink_chain(lexical, root, final_may_be_missing=False)
    try:
        resolved = lexical.resolve(strict=True)
        metadata = resolved.stat()
    except OSError as exc:
        raise FinalCompositionError(f"evidence file is unavailable: {lexical}") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise FinalCompositionError(f"evidence path is not a regular file: {lexical}")
    mode = stat.S_IMODE(metadata.st_mode)
    if mode != 0o600:
        raise FinalCompositionError(
            f"evidence permission drifted: {lexical} is {mode:04o}, expected 0600"
        )
    if Path(os.path.commonpath((str(root), str(resolved)))) != root:
        raise FinalCompositionError(f"resolved evidence path escapes root: {resolved}")
    return resolved


def _validate_private_file_binding(
    binding: Any,
    *,
    label: str,
    expected_path: Path | None = None,
) -> Path:
    if not isinstance(binding, Mapping) or set(binding) != {
        "path",
        "file_sha256",
        "size_bytes",
        "mode",
    }:
        raise FinalCompositionError(f"{label} file binding is malformed")
    raw = Path(str(binding["path"])).expanduser()
    if not raw.is_absolute() or ".." in raw.parts:
        raise FinalCompositionError(f"{label} path is not an absolute canonical path")
    path = Path(os.path.abspath(raw))
    for candidate in (path, *path.parents):
        try:
            metadata = candidate.lstat()
        except OSError as exc:
            raise FinalCompositionError(f"{label} path is unavailable") from exc
        if stat.S_ISLNK(metadata.st_mode):
            raise FinalCompositionError(f"{label} path traverses a symlink")
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode):
        raise FinalCompositionError(f"{label} is not a regular file")
    if stat.S_IMODE(metadata.st_mode) != 0o600 or binding["mode"] != "0600":
        raise FinalCompositionError(f"{label} permission drifted")
    try:
        expected_size = int(binding["size_bytes"])
    except (TypeError, ValueError) as exc:
        raise FinalCompositionError(f"{label} size is malformed") from exc
    if (
        file_sha256(path) != _require_sha(binding["file_sha256"], f"{label} file SHA256")
        or metadata.st_size != expected_size
    ):
        raise FinalCompositionError(f"{label} file binding drifted")
    if expected_path is not None and path != expected_path.resolve(strict=True):
        raise FinalCompositionError(f"{label} path drifted")
    return path


def _validate_referenced_document(
    binding: Any,
    *,
    label: str,
    expected_path: Path | None = None,
) -> tuple[dict[str, Any], Path]:
    if not isinstance(binding, Mapping) or set(binding) != {
        "file",
        "schema_version",
        "identity",
        "canonical_field",
        "canonical_sha256",
    }:
        raise FinalCompositionError(f"{label} document binding is malformed")
    path = _validate_private_file_binding(
        binding["file"], label=label, expected_path=expected_path
    )
    payload = strict_load_json(path)
    field = str(binding["canonical_field"])
    if (
        payload.get("schema_version") != binding["schema_version"]
        or payload.get("identity") != binding["identity"]
        or payload.get(field) != _require_sha(
            binding["canonical_sha256"], f"{label} canonical SHA256"
        )
        or payload.get(field) != document_sha256(payload, field)
    ):
        raise FinalCompositionError(f"{label} document identity drifted")
    return payload, path


def _admit_output_path(path: Path, root: Path) -> Path:
    lexical = _lexical_under_root(path, root)
    if lexical.exists() or lexical.is_symlink():
        raise FinalCompositionError(f"immutable output already exists: {lexical}")
    parent = lexical.parent
    current = root
    for part in parent.relative_to(root).parts:
        current = current / part
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            try:
                current.mkdir(mode=0o700)
            except FileExistsError:
                metadata = current.lstat()
            else:
                metadata = current.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise FinalCompositionError(f"symlink output parent is forbidden: {current}")
        if not stat.S_ISDIR(metadata.st_mode):
            raise FinalCompositionError(f"output parent is not a directory: {current}")
    return lexical


def _relative_path(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _walk_mappings(value: Any) -> Sequence[Mapping[str, Any]]:
    collected: list[Mapping[str, Any]] = []

    def visit(node: Any) -> None:
        if isinstance(node, Mapping):
            collected.append(node)
            for child in node.values():
                visit(child)
        elif isinstance(node, list):
            for child in node:
                visit(child)

    visit(value)
    return collected


def _validate_evidence_boundaries(payload: Mapping[str, Any], role: str) -> None:
    mappings = _walk_mappings(payload)
    for mapping in mappings:
        for key in FALSE_EVIDENCE_KEYS:
            if key in mapping and mapping[key] is not False:
                raise FinalCompositionError(f"{role}.{key} must remain false")
        if "research_supported" in mapping and mapping["research_supported"] is not False:
            raise FinalCompositionError(f"{role}.research_supported must remain false")
    required_false = {"validation_read", "sealed_holdout_read"}
    if role not in {"exact_predicate_bundle", "label_materialization"}:
        required_false.update(("action_authorized", "live_authorized"))
    for key in sorted(required_false):
        if not any(mapping.get(key) is False for mapping in mappings if key in mapping):
            raise FinalCompositionError(f"{role} lacks an explicit false {key} boundary")


def _canonical_field(role: str, payload: Mapping[str, Any]) -> str:
    if role.startswith(LAYER4_DAY_PREFIX):
        candidates = ("canonical_day_receipt_sha256",)
    else:
        try:
            candidates = CANONICAL_FIELDS[role]
        except KeyError as exc:
            raise FinalCompositionError(f"unknown evidence role: {role}") from exc
    present = [field for field in candidates if field in payload]
    if len(present) != 1:
        raise FinalCompositionError(
            f"{role} must contain exactly one admitted canonical field: {candidates}"
        )
    field = present[0]
    embedded = _require_sha(payload[field], f"{role}.{field}")
    observed = document_sha256(payload, field)
    if embedded != observed:
        raise FinalCompositionError(f"{role} canonical document SHA256 drifted")
    return field


def _schema_matches(role: str, schema: str) -> bool:
    exact = {
        "joint_closeout_manifest": f"{IDENTITY}.closeout_manifest.v1",
        "owner_decision": f"{IDENTITY}.owner_decision.v1",
        "attempt_execution_manifest": f"{IDENTITY}.execution_manifest.v1",
        "owner_execution_preflight": f"{IDENTITY}.execution_preflight_receipt.v1",
        "label_materialization": f"{IDENTITY}.full_development_label_materialization.v1",
        "refit_receipt": f"{IDENTITY}.refit_run_receipt.v1",
        "exact_artifact_manifest": f"{IDENTITY}.full_development_refit.v1",
        "exact_policy": f"{IDENTITY}.artifact.v1",
        "exact_predicate_bundle": f"{IDENTITY}.selected_predicate_bundle.v1",
        LAYER1_ROLE: f"{IDENTITY}.parity_receipt.v1",
        LAYER2_ROLE: f"{IDENTITY}.parity_receipt.v1",
        LAYER3_ROLE: f"{IDENTITY}.parity_receipt.v1",
        LAYER4_MECHANICS_ROLE: (
            f"{IDENTITY}.outcome_blind_mechanics_identity_receipt.v1"
        ),
        "sell_54_case": f"{IDENTITY}.parity_receipt.v1",
        "runtime_regression": f"{IDENTITY}.runtime_regression_test_receipt.v2",
        # The post-freeze auditor is amendment v2, but it introduces the first
        # Layer-4 contract document.  The document schema is therefore v1 and
        # carries an explicit v2 amendment identity.
        "layer4_contract": f"{IDENTITY}.layer4_lockstep_contract.v1",
        "layer4_final": f"{IDENTITY}.parity_receipt.v2",
        "deployment_gate": f"{IDENTITY}.deployment_gate_amendment.v2",
        "source_role_resolution": SOURCE_ROLE_SCHEMA,
    }
    if role in exact:
        return schema == exact[role]
    if role.startswith(LAYER4_DAY_PREFIX):
        return schema == f"{IDENTITY}.repeated_policy_lockstep_day.v2"
    suffixes = {
        "formal_buy_component_manifest": ".component_artifact_manifest.v1",
        "formal_buy_component_validation": ".component_validation.v1",
        "source_execution_manifest": ".execution_manifest.v1",
        "cpp_builder_preflight": ".builder_preflight.v1",
        "cpp_quick_preflight": ".receipt.v1",
        "cpp_qualification": ".receipt.v1",
    }
    suffix = suffixes.get(role)
    return suffix is not None and schema.endswith(suffix)


def _source_status(role: str, payload: Mapping[str, Any]) -> tuple[str, str]:
    raw = payload.get("status")
    if isinstance(raw, str) and raw:
        return raw, "source_field"
    if role in STATIC_STATUS_BY_ROLE:
        return STATIC_STATUS_BY_ROLE[role], "role_contract"
    raise FinalCompositionError(f"{role} has no nonempty status")


def _validate_role_shape(role: str, payload: Mapping[str, Any]) -> None:
    schema = payload.get("schema_version")
    identity = payload.get("identity")
    if not isinstance(schema, str) or not _schema_matches(role, schema):
        raise FinalCompositionError(f"{role} schema is not admitted: {schema!r}")
    if not isinstance(identity, str) or not identity:
        raise FinalCompositionError(f"{role} identity is missing")
    exact_owner_identity_roles = {
        "joint_closeout_manifest",
        "owner_decision",
        "attempt_execution_manifest",
        "owner_execution_preflight",
        "refit_receipt",
        "exact_artifact_manifest",
        "exact_policy",
        "exact_predicate_bundle",
        LAYER1_ROLE,
        LAYER2_ROLE,
        LAYER3_ROLE,
        LAYER4_MECHANICS_ROLE,
        "layer4_contract",
        "layer4_final",
        "sell_54_case",
        "runtime_regression",
        "deployment_gate",
    }
    if role.startswith(LAYER4_DAY_PREFIX):
        exact_owner_identity_roles.add(role)
    if role in exact_owner_identity_roles and identity != IDENTITY:
        raise FinalCompositionError(f"{role} owner identity drifted")
    if role == "label_materialization" and identity != (
        f"{IDENTITY}.full_development_label_materializer_v1"
    ):
        raise FinalCompositionError("label materialization identity drifted")
    if role == "source_role_resolution" and identity != SOURCE_ROLE_IDENTITY:
        raise FinalCompositionError("source-role identity drifted")
    if role.startswith(LAYER4_DAY_PREFIX) and schema.endswith(".v1"):
        raise FinalCompositionError("Layer4 v1 day receipts are forbidden")
    _source_status(role, payload)
    _canonical_field(role, payload)
    _validate_evidence_boundaries(payload, role)


def _binding(role: str, path: Path, root: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    _validate_role_shape(role, payload)
    canonical_field = _canonical_field(role, payload)
    status, status_source = _source_status(role, payload)
    metadata = path.stat()
    return {
        "role": role,
        "path": _relative_path(path, root),
        "file_sha256": file_sha256(path),
        "size_bytes": metadata.st_size,
        "mode": "0600",
        "schema_version": payload["schema_version"],
        "identity": payload["identity"],
        "status": status,
        "status_source": status_source,
        "canonical_field": canonical_field,
        "canonical_sha256": payload[canonical_field],
    }


def _predicted_binding(
    role: str,
    path: Path,
    root: Path,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    _validate_role_shape(role, payload)
    encoded = _json_bytes(payload)
    canonical_field = _canonical_field(role, payload)
    status, status_source = _source_status(role, payload)
    return {
        "role": role,
        "path": _relative_path(path, root),
        "file_sha256": hashlib.sha256(encoded).hexdigest(),
        "size_bytes": len(encoded),
        "mode": "0600",
        "schema_version": payload["schema_version"],
        "identity": payload["identity"],
        "status": status,
        "status_source": status_source,
        "canonical_field": canonical_field,
        "canonical_sha256": payload[canonical_field],
    }


def _load_bound_inputs(
    inputs: CompositionInputs,
    root: Path,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    role_paths: list[tuple[str, Path]] = [
        (role, getattr(inputs, role))
        for role in BASE_ROLE_ORDER
        if role not in {"source_role_resolution", "layer4_contract"}
    ]
    role_paths.append(("layer4_contract", inputs.layer4_contract))
    if len(inputs.layer4_day_receipts) != EXPECTED_DAY_COUNT:
        raise FinalCompositionError(
            f"Layer4 must provide exactly {EXPECTED_DAY_COUNT} ordered day receipts"
        )
    for index, path in enumerate(inputs.layer4_day_receipts):
        role_paths.append((f"{LAYER4_DAY_PREFIX}{index:02d}", path))
    role_paths.extend((role, getattr(inputs, role)) for role in TAIL_ROLE_ORDER)
    documents: dict[str, dict[str, Any]] = {}
    bindings: dict[str, dict[str, Any]] = {}
    seen_paths: set[Path] = set()
    for role, supplied_path in role_paths:
        path = _admit_input_path(supplied_path, root)
        if path in seen_paths:
            raise FinalCompositionError(f"an evidence file was assigned twice: {path}")
        seen_paths.add(path)
        payload = strict_load_json(path)
        documents[role] = payload
        bindings[role] = _binding(role, path, root, payload)
    return documents, bindings


def _permissions_are_false(payload: Mapping[str, Any], role: str) -> None:
    permissions = payload.get("permissions")
    if not isinstance(permissions, Mapping):
        raise FinalCompositionError(f"{role} permissions are missing")
    for field in ("action_authorized", "live_authorized", "validation_read", "sealed_holdout_read"):
        _require_bool(permissions.get(field), False, f"{role}.permissions.{field}")


def _binding_sha(bindings: Mapping[str, Mapping[str, Any]], role: str) -> str:
    return _require_sha(bindings[role]["canonical_sha256"], f"{role} canonical SHA256")


def _validate_formal_chain(
    documents: Mapping[str, Mapping[str, Any]],
    bindings: Mapping[str, Mapping[str, Any]],
) -> str:
    component = documents["formal_buy_component_manifest"]
    validation = documents["formal_buy_component_validation"]
    closeout = documents["joint_closeout_manifest"]
    decision = documents["owner_decision"]
    _require_equal(component.get("formal_side"), "BUY", "formal BUY component side")
    _permissions_are_false(component, "formal BUY component manifest")
    learning_sha = _require_sha(
        component.get("canonical_artifact_manifest_sha256"),
        "formal learning algorithm artifact SHA256",
    )
    _require_equal(validation.get("formal_side"), "BUY", "formal validation side")
    _require_equal(
        validation.get("status"),
        "passed_exact_component_result_report_scorecards_and_cache",
        "formal component validation status",
    )
    _require_equal(
        _optional_field(validation, "component_result", "canonical_sha256"),
        component.get("component_result_canonical_sha256"),
        "formal component result canonical binding",
    )
    _require_equal(
        _optional_field(validation, "source_execution", "execution_manifest_sha256"),
        component.get("source_execution_manifest_sha256"),
        "formal component execution binding",
    )
    _permissions_are_false(validation, "formal BUY component validation")
    _require_equal(closeout.get("identity"), IDENTITY, "joint closeout identity")
    _require_equal(
        closeout.get("status"),
        "formal_statistics_rebuilt_owner_override_recorded",
        "joint closeout status",
    )
    _permissions_are_false(closeout, "joint closeout")
    closeout_files = closeout.get("files")
    owner_file = (
        closeout_files.get("owner_decision.json") if isinstance(closeout_files, Mapping) else None
    )
    if not isinstance(owner_file, Mapping):
        raise FinalCompositionError("joint closeout does not bind owner_decision.json")
    _require_equal(
        owner_file.get("sha256"),
        bindings["owner_decision"]["file_sha256"],
        "joint closeout owner decision file binding",
    )
    _require_equal(decision.get("identity"), IDENTITY, "owner decision identity")
    _require_equal(
        decision.get("status"),
        "owner_override_recorded_artifact_not_yet_frozen",
        "owner decision status",
    )
    for field, expected in (
        ("research_supported", False),
        ("owner_risk_accepted", True),
        ("outcome_informed_owner_override", True),
        ("formal_closeout_mutated", False),
        ("formal_hierarchy_passed", False),
        ("formal_hard_gates_passed", False),
    ):
        _require_bool(decision.get(field), expected, f"owner decision {field}")
    _require_bool(
        _field(decision, "evidence_boundary", "exact_final_artifact_oof_available"),
        False,
        "owner decision exact artifact OOF",
    )
    _permissions_are_false(decision, "owner decision")
    buy_source = _optional_field(decision, "source_bindings", "BUY")
    if not isinstance(buy_source, Mapping):
        raise FinalCompositionError("owner decision BUY source binding is missing")
    _require_equal(
        buy_source.get("result_canonical_sha256"),
        component.get("component_result_canonical_sha256"),
        "owner decision formal BUY component binding",
    )
    return learning_sha


def _validate_execution_chain(
    documents: Mapping[str, Mapping[str, Any]],
    bindings: Mapping[str, Mapping[str, Any]],
) -> tuple[str, str, str]:
    execution = documents["attempt_execution_manifest"]
    source = documents["source_execution_manifest"]
    builder = documents["cpp_builder_preflight"]
    quick = documents["cpp_quick_preflight"]
    qualification = documents["cpp_qualification"]
    preflight = documents["owner_execution_preflight"]
    _require_equal(execution.get("identity"), IDENTITY, "attempt identity")
    _require_equal(execution.get("status"), "pre_refit_owner_execution_bound", "attempt status")
    _require_equal(
        _field(execution, "execution_contract", "selected_side"),
        "BUY",
        "attempt selected side",
    )
    _permissions_are_false(execution, "attempt execution manifest")
    if _GIT_SHA_RE.fullmatch(str(execution.get("public_base_commit", ""))) is None:
        raise FinalCompositionError("attempt execution commit is not a Git object id")
    annotated_tag = execution.get("annotated_tag")
    if (
        not isinstance(annotated_tag, str)
        or not annotated_tag
        or any(character.isspace() for character in annotated_tag)
    ):
        raise FinalCompositionError("attempt annotated tag is invalid")
    execution_sha = _binding_sha(bindings, "attempt_execution_manifest")
    execution_file_bindings = execution.get("bindings")
    if not isinstance(execution_file_bindings, Mapping):
        raise FinalCompositionError("attempt file bindings are missing")
    for key, role in (
        ("owner_decision", "owner_decision"),
        ("joint_closeout_manifest", "joint_closeout_manifest"),
        ("source_execution_manifest", "source_execution_manifest"),
    ):
        record = execution_file_bindings.get(key)
        if not isinstance(record, Mapping):
            raise FinalCompositionError(f"attempt binding is missing: {key}")
        _require_equal(record.get("sha256"), bindings[role]["file_sha256"], f"attempt {key}")
    _require_equal(execution.get("backend"), source.get("backend"), "source mechanics backend")
    _require_equal(execution.get("executor"), source.get("executor"), "source mechanics executor")
    source_sides = _optional_field(source, "execution_contract", "formal_sides")
    if source_sides is None:
        source_sides = source.get("formal_sides")
    _require_equal(source_sides, ["SELL"], "historical source formal sides")
    for payload, label in ((builder, "C++ builder"), (quick, "C++ quick preflight")):
        _require_equal(
            payload.get("execution_manifest_sha256"), execution_sha, f"{label} execution"
        )
    _require_equal(
        builder.get("status"),
        "passed_all_3516_zero_economic_builder_walk",
        "C++ builder status",
    )
    _require_equal(builder.get("opportunity_count"), 3516, "C++ builder opportunity count")
    builder_sha = _binding_sha(bindings, "cpp_builder_preflight")
    quick_sha = _binding_sha(bindings, "cpp_quick_preflight")
    _require_equal(
        quick.get("all_panel_builder_preflight_receipt_sha256"),
        builder_sha,
        "C++ quick builder parent",
    )
    _require_equal(quick.get("mismatch_count"), 0, "C++ quick mismatch count")
    _require_equal(quick.get("zero_mismatch_arm_count"), quick.get("arm_count"), "C++ quick arms")
    _require_equal(
        quick.get("status"),
        "passed_first_opportunity_all_side_specific_arms_lockstep",
        "C++ quick status",
    )
    contract = qualification.get("qualification_contract")
    if not isinstance(contract, Mapping):
        raise FinalCompositionError("C++ qualification contract is missing")
    _require_equal(
        contract.get("execution_manifest_sha256"), execution_sha, "qualification execution"
    )
    _require_equal(
        contract.get("all_panel_builder_preflight_receipt_sha256"),
        builder_sha,
        "qualification builder parent",
    )
    _require_equal(
        contract.get("first_opportunity_all_arm_preflight_receipt_sha256"),
        quick_sha,
        "qualification quick parent",
    )
    _require_equal(
        qualification.get("zero_mismatch_arm_count"),
        qualification.get("arm_count"),
        "qualification all-arm count",
    )
    _require_equal(
        qualification.get("status"),
        "passed_real_day_all_opportunity_all_arm_lockstep",
        "qualification status",
    )
    qualification_sha = _binding_sha(bindings, "cpp_qualification")
    _require_equal(
        preflight.get("execution_manifest_canonical_sha256"),
        execution_sha,
        "owner preflight execution",
    )
    _require_equal(
        preflight.get("cpp_qualification_receipt_sha256"),
        qualification_sha,
        "owner preflight qualification",
    )
    _require_equal(
        preflight.get("status"),
        "owner_execution_preflight_complete",
        "owner preflight status",
    )
    _require_equal(
        _optional_field(preflight, "formal_zero_economic_preflight", "status"),
        "formal_offline_replay_mechanics_ready",
        "owner zero-economic preflight status",
    )
    return execution_sha, qualification_sha, _binding_sha(bindings, "owner_execution_preflight")


def _validate_refit_and_artifact_chain(
    documents: Mapping[str, Mapping[str, Any]],
    bindings: Mapping[str, Mapping[str, Any]],
    *,
    execution_sha: str,
    qualification_sha: str,
    preflight_sha: str,
) -> tuple[str, tuple[str, ...]]:
    labels = documents["label_materialization"]
    refit = documents["refit_receipt"]
    manifest = documents["exact_artifact_manifest"]
    policy = documents["exact_policy"]
    bundle = documents["exact_predicate_bundle"]
    _require_equal(labels.get("side"), "BUY", "label materialization side")
    _require_equal(
        labels.get("status"),
        "full_development_buy_labels_materialized",
        "label materialization status",
    )
    _require_equal(
        labels.get("full_day_count"), EXPECTED_DAY_COUNT, "label materialization day count"
    )
    _require_equal(
        labels.get("fresh_execution_manifest_sha256"), execution_sha, "fresh labels execution"
    )
    _require_equal(
        labels.get("strategy_dependent_cross_execution_cache_imported"),
        False,
        "label cache contract",
    )
    fresh_adapter_identity = labels.get("fresh_adapter_identity")
    if not isinstance(fresh_adapter_identity, str) or not fresh_adapter_identity:
        raise FinalCompositionError("label materialization fresh adapter identity is missing")
    _require_sha(
        labels.get("fresh_adapter_artifact_sha256"),
        "label materialization fresh adapter artifact SHA256",
    )
    label_sha = _binding_sha(bindings, "label_materialization")
    for field, expected in (
        ("execution_manifest_canonical_sha256", execution_sha),
        ("cpp_qualification_receipt_sha256", qualification_sha),
        ("execution_preflight_receipt_sha256", preflight_sha),
        ("label_materialization_receipt_sha256", label_sha),
    ):
        _require_equal(refit.get(field), expected, f"refit {field}")
    _require_equal(
        refit.get("status"),
        "owner_buy_e3_full_development_refit_complete",
        "refit status",
    )
    _require_equal(refit.get("full_development_refit_count"), 1, "full Development refit count")
    for field in (
        "outer_fold_policy_selected",
        "outer_fold_rules_merged",
        "literal_edited",
        "candidate_substituted",
        "research_supported",
        "exact_artifact_oof_available",
    ):
        _require_bool(refit.get(field), False, f"refit {field}")
    _require_bool(refit.get("owner_risk_accepted"), True, "refit owner override")
    artifact_sha = _binding_sha(bindings, "exact_artifact_manifest")
    _require_equal(refit.get("artifact_sha256"), artifact_sha, "refit exact artifact")
    _require_equal(manifest.get("status"), "exact_buy_e3_artifact_frozen", "exact artifact status")
    _require_equal(
        manifest.get("policy_file_sha256"),
        bindings["exact_policy"]["file_sha256"],
        "artifact policy file",
    )
    _require_equal(
        manifest.get("predicate_bundle_file_sha256"),
        bindings["exact_predicate_bundle"]["file_sha256"],
        "artifact predicate bundle file",
    )
    _require_equal(
        manifest.get("policy_canonical_sha256"),
        _binding_sha(bindings, "exact_policy"),
        "artifact policy canonical",
    )
    _require_equal(
        manifest.get("predicate_bundle_canonical_sha256"),
        _binding_sha(bindings, "exact_predicate_bundle"),
        "artifact predicate canonical",
    )
    _require_equal(
        manifest.get("label_materialization_receipt_sha256"), label_sha, "artifact labels"
    )
    _require_equal(
        manifest.get("cpp_one_shot_qualification_receipt_sha256"),
        qualification_sha,
        "artifact qualification",
    )
    _require_equal(
        manifest.get("execution_preflight_receipt_sha256"), preflight_sha, "artifact preflight"
    )
    _require_equal(policy.get("side"), "BUY", "exact policy side")
    _require_equal(
        policy.get("status"), "owner_refit_frozen_not_self_confirmed", "exact policy status"
    )
    _require_equal(bundle.get("side"), "BUY", "predicate bundle side")
    _require_equal(
        policy.get("predicate_bundle_file_sha256"),
        bindings["exact_predicate_bundle"]["file_sha256"],
        "policy bundle file",
    )
    boundary = policy.get("evidence_boundary")
    if not isinstance(boundary, Mapping):
        raise FinalCompositionError("exact policy evidence boundary is missing")
    for field, expected in (
        ("research_supported", False),
        ("owner_risk_accepted", True),
        ("outcome_informed_owner_override", True),
        ("formal_hierarchy_passed", False),
        ("formal_hard_gates_passed", False),
        ("exact_artifact_oof_available", False),
    ):
        _require_bool(boundary.get(field), expected, f"exact policy {field}")
    _require_bool(bundle.get("uses_trade_predicates"), False, "predicate bundle trade use")
    _require_bool(bundle.get("uses_depth_predicates"), False, "predicate bundle depth use")
    _require_bool(bundle.get("uses_m2_incremental_features"), False, "predicate bundle M2 use")
    days = manifest.get("training_days")
    if not isinstance(days, list) or len(days) != EXPECTED_DAY_COUNT:
        raise FinalCompositionError("exact artifact must bind 30 training days")
    normalized_days = tuple(str(day) for day in days)
    if normalized_days != tuple(sorted(set(normalized_days))) or any(
        _UTC_DAY_RE.fullmatch(day) is None for day in normalized_days
    ):
        raise FinalCompositionError("exact artifact training days are not canonical")
    return artifact_sha, normalized_days


def _validate_mechanics_identity_chain(
    documents: Mapping[str, Mapping[str, Any]],
    bindings: Mapping[str, Mapping[str, Any]],
    *,
    execution_sha: str,
    ordered_days: tuple[str, ...],
) -> tuple[str, str]:
    receipt = documents[LAYER4_MECHANICS_ROLE]
    _require_equal(
        receipt.get("schema_amendment"),
        f"{IDENTITY}.layer4_receipt_binding_amendment.v2",
        "mechanics receipt amendment",
    )
    _require_equal(
        receipt.get("status"),
        "outcome_blind_mechanics_identity_materialized",
        "mechanics receipt status",
    )
    _require_bool(
        receipt.get("economic_outcomes_present"), False, "mechanics receipt outcomes"
    )
    body = receipt.get("mechanics_body")
    if not isinstance(body, Mapping) or set(body) != _MECHANICS_BODY_FIELDS:
        raise FinalCompositionError("mechanics canonical body is malformed")
    _require_bool(
        body.get("economic_outcomes_present"), False, "mechanics canonical body outcomes"
    )
    mechanics_sha = _require_sha(
        receipt.get("mechanics_receipt_sha256"), "mechanics receipt SHA256"
    )
    _require_equal(canonical_sha256(body), mechanics_sha, "mechanics canonical body")
    _require_equal(
        tuple(str(day) for day in body.get("selected_days", ())),
        ordered_days,
        "mechanics selected days",
    )
    for field in (
        "metadata_sha256",
        "boolean_features_sha256",
        "primitive_boolean_features_sha256",
        "continuous_features_sha256",
        "exact_owner_actions_sha256",
        "replay_inputs_sha256",
    ):
        _require_sha(body.get(field), f"mechanics body {field}")
    predicate_receipt = body.get("predicate_view_receipt")
    if not isinstance(predicate_receipt, Mapping) or (
        predicate_receipt.get("economic_outcomes_read") is not False
    ):
        raise FinalCompositionError("mechanics predicate-view receipt is not outcome blind")

    owner_binding = receipt.get("owner_execution_attempt")
    if not isinstance(owner_binding, Mapping):
        raise FinalCompositionError("mechanics owner execution binding is missing")
    # owner_execution_attempt uses the attempt binding shape rather than the
    # generic referenced-document shape.
    owner_manifest_path = _validate_private_file_binding(
        owner_binding.get("manifest"), label="mechanics owner attempt2 manifest"
    )
    owner_document = strict_load_json(owner_manifest_path)
    _require_equal(
        owner_document,
        documents["attempt_execution_manifest"],
        "mechanics owner attempt2 document",
    )
    _require_equal(
        owner_binding.get("canonical_execution_manifest_sha256"),
        execution_sha,
        "mechanics owner execution manifest",
    )
    _require_equal(
        owner_binding.get("execution_commit"),
        owner_document.get("public_base_commit"),
        "mechanics owner execution commit",
    )
    _require_equal(
        owner_binding.get("annotated_tag"),
        owner_document.get("annotated_tag"),
        "mechanics owner execution tag",
    )
    _require_equal(
        _optional_field(owner_binding, "manifest", "file_sha256"),
        bindings["attempt_execution_manifest"]["file_sha256"],
        "mechanics owner execution file",
    )

    source_identity = receipt.get("source_identity")
    if not isinstance(source_identity, Mapping) or set(source_identity) != {
        "source_execution_manifest",
        "source_manifest",
        "panel_manifest",
        "outcome_blind_predicate_bundle",
        "panel_file_sha256",
        "fold_manifest_sha256",
        "nested_fold_manifest_sha256",
    }:
        raise FinalCompositionError("mechanics source identity is malformed")
    source_execution, _source_execution_path = _validate_referenced_document(
        source_identity["source_execution_manifest"],
        label="mechanics source execution manifest",
    )
    source_manifest, _source_manifest_path = _validate_referenced_document(
        source_identity["source_manifest"], label="mechanics source manifest"
    )
    panel_manifest, _panel_manifest_path = _validate_referenced_document(
        source_identity["panel_manifest"], label="mechanics panel manifest"
    )
    _predicate_bundle, _predicate_path = _validate_referenced_document(
        source_identity["outcome_blind_predicate_bundle"],
        label="mechanics outcome-blind predicate bundle",
    )
    _require_equal(
        source_execution,
        documents["source_execution_manifest"],
        "mechanics source execution document",
    )
    _require_equal(
        _optional_field(
            source_identity, "source_execution_manifest", "file", "file_sha256"
        ),
        bindings["source_execution_manifest"]["file_sha256"],
        "mechanics source execution file",
    )
    attempt_bindings = owner_document.get("bindings")
    if not isinstance(attempt_bindings, Mapping):
        raise FinalCompositionError("owner attempt2 source bindings are missing")
    for role, source_role in (
        ("source_execution_manifest", "source_execution_manifest"),
        ("source_manifest", "source_manifest"),
        ("panel_manifest", "panel_manifest"),
        ("outcome_blind_2025_predicate_bundle", "outcome_blind_predicate_bundle"),
    ):
        attempt_file = attempt_bindings.get(role)
        if not isinstance(attempt_file, Mapping):
            raise FinalCompositionError(f"owner attempt2 {role} binding is missing")
        _require_equal(
            attempt_file.get("sha256"),
            _optional_field(source_identity, source_role, "file", "file_sha256"),
            f"mechanics owner/source file {role}",
        )
    fold_sha = _require_sha(
        source_identity.get("fold_manifest_sha256"), "mechanics fold manifest SHA256"
    )
    nested_fold_sha = _require_sha(
        source_identity.get("nested_fold_manifest_sha256"),
        "mechanics nested-fold manifest SHA256",
    )
    _require_equal(
        fold_sha, owner_document.get("fold_manifest_sha256"), "mechanics owner fold manifest"
    )
    _require_equal(
        nested_fold_sha,
        owner_document.get("nested_fold_manifest_sha256"),
        "mechanics owner nested-fold manifest",
    )
    _require_equal(
        source_execution.get("fold_manifest_sha256"),
        fold_sha,
        "mechanics source execution fold manifest",
    )
    _require_equal(
        source_execution.get("nested_fold_manifest_sha256"),
        nested_fold_sha,
        "mechanics source execution nested-fold manifest",
    )
    _require_equal(
        tuple(source_manifest.get("selected_days", ())),
        ordered_days,
        "mechanics source manifest days",
    )
    _require_equal(
        tuple(panel_manifest.get("selected_days", ())),
        ordered_days,
        "mechanics panel manifest days",
    )
    _require_bool(
        panel_manifest.get("economic_outcomes_present"),
        False,
        "mechanics panel outcomes",
    )
    panel_files = panel_manifest.get("files")
    body_files = body.get("file_sha256")
    if not isinstance(panel_files, Mapping) or not isinstance(body_files, Mapping):
        raise FinalCompositionError("mechanics panel file identities are missing")
    expected_panel_files = {
        str(role): _require_sha(record.get("sha256"), f"mechanics panel {role} SHA256")
        for role, record in panel_files.items()
        if isinstance(record, Mapping)
    }
    _require_equal(
        dict(body_files), expected_panel_files, "mechanics canonical body panel files"
    )
    _require_equal(
        source_identity.get("panel_file_sha256"),
        expected_panel_files,
        "mechanics source panel files",
    )
    formal_bindings = body.get("bindings")
    if not isinstance(formal_bindings, Mapping) or set(formal_bindings) != {
        "execution_manifest_sha256",
        "source_manifest_sha256",
        "panel_manifest_sha256",
        "fold_manifest_sha256",
        "nested_fold_manifest_sha256",
        "exact_owner_policy_sha256",
        "exact_owner_predicate_bundle_sha256",
        "exact_owner_private_config_sha256",
    }:
        raise FinalCompositionError("mechanics FormalExecutionBindings are malformed")
    expected_formal_bindings = {
        "execution_manifest_sha256": execution_sha,
        "source_manifest_sha256": source_identity["source_manifest"]["canonical_sha256"],
        "panel_manifest_sha256": source_identity["panel_manifest"]["canonical_sha256"],
        "fold_manifest_sha256": fold_sha,
        "nested_fold_manifest_sha256": nested_fold_sha,
        "exact_owner_policy_sha256": panel_manifest.get(
            "exact_current_owner_policy_sha256"
        ),
        "exact_owner_predicate_bundle_sha256": panel_manifest.get(
            "exact_current_predicate_bundle_sha256"
        ),
        "exact_owner_private_config_sha256": panel_manifest.get(
            "exact_current_private_config_sha256"
        ),
    }
    for field, value in expected_formal_bindings.items():
        expected_formal_bindings[field] = _require_sha(value, f"mechanics binding {field}")
    _require_equal(
        dict(formal_bindings), expected_formal_bindings, "mechanics FormalExecutionBindings"
    )
    _require_equal(
        expected_formal_bindings["exact_owner_predicate_bundle_sha256"],
        source_identity["outcome_blind_predicate_bundle"]["file"]["file_sha256"],
        "mechanics outcome-blind predicate source",
    )
    return (
        _binding_sha(bindings, LAYER4_MECHANICS_ROLE),
        mechanics_sha,
    )


def _validate_parity_chain(
    documents: Mapping[str, Mapping[str, Any]],
    bindings: Mapping[str, Mapping[str, Any]],
    *,
    execution_sha: str,
    learning_sha: str,
    artifact_sha: str,
    ordered_days: tuple[str, ...],
    evidence_root: Path,
) -> str:
    expected_layers = {
        LAYER1_ROLE: ("research_compiled", ("mismatch_count",)),
        LAYER2_ROLE: (
            "development_snapshot",
            ("predicate_projection_mismatch_count", "action_duration_mismatch_count"),
        ),
        LAYER3_ROLE: ("streaming_offline", ("feature_mismatch_count",)),
    }
    for role, (layer, mismatch_fields) in expected_layers.items():
        receipt = documents[role]
        _require_equal(receipt.get("status"), "parity_complete", f"{layer} parity status")
        _require_equal(receipt.get("layer"), layer, f"{layer} parity layer")
        _require_equal(receipt.get("artifact_sha256"), artifact_sha, f"{layer} artifact")
        _require_equal(
            receipt.get("artifact_manifest_file_sha256"),
            bindings["exact_artifact_manifest"]["file_sha256"],
            f"{layer} artifact manifest file",
        )
        _require_equal(
            receipt.get("policy_file_sha256"),
            bindings["exact_policy"]["file_sha256"],
            f"{layer} policy file",
        )
        _require_equal(
            receipt.get("predicate_bundle_file_sha256"),
            bindings["exact_predicate_bundle"]["file_sha256"],
            f"{layer} predicate bundle file",
        )
        for field in mismatch_fields:
            _require_equal(
                _optional_field(receipt, "evidence", field),
                0,
                f"{layer} {field}",
            )
    mechanics_canonical_sha, mechanics_sha = _validate_mechanics_identity_chain(
        documents,
        bindings,
        execution_sha=execution_sha,
        ordered_days=ordered_days,
    )
    contract = documents["layer4_contract"]
    _require_equal(
        contract.get("schema_amendment"),
        f"{IDENTITY}.layer4_receipt_binding_amendment.v2",
        "Layer4 contract amendment",
    )
    _require_equal(
        contract.get("status"),
        "layer4_lockstep_contract_frozen",
        "Layer4 contract status",
    )
    if "mechanics_receipt_sha256" in contract:
        raise FinalCompositionError("legacy Layer4 bare mechanics SHA is forbidden")
    mechanics_binding = contract.get("mechanics_identity_receipt")
    if not isinstance(mechanics_binding, Mapping) or set(mechanics_binding) != {
        "receipt",
        "schema_version",
        "canonical_receipt_sha256",
        "mechanics_receipt_sha256",
    }:
        raise FinalCompositionError("Layer4 mechanics receipt file binding is missing")
    mechanics_path = (
        evidence_root / str(bindings[LAYER4_MECHANICS_ROLE]["path"])
    ).resolve(strict=True)
    _validate_private_file_binding(
        mechanics_binding["receipt"],
        label="Layer4 mechanics identity receipt",
        expected_path=mechanics_path,
    )
    _require_equal(
        _optional_field(mechanics_binding, "receipt", "file_sha256"),
        bindings[LAYER4_MECHANICS_ROLE]["file_sha256"],
        "Layer4 mechanics receipt file",
    )
    _require_equal(
        mechanics_binding.get("schema_version"),
        documents[LAYER4_MECHANICS_ROLE].get("schema_version"),
        "Layer4 mechanics receipt schema",
    )
    _require_equal(
        mechanics_binding.get("canonical_receipt_sha256"),
        mechanics_canonical_sha,
        "Layer4 mechanics canonical receipt",
    )
    _require_equal(
        mechanics_binding.get("mechanics_receipt_sha256"),
        mechanics_sha,
        "Layer4 embedded mechanics receipt",
    )
    contract_sha = _binding_sha(bindings, "layer4_contract")
    _require_equal(
        _optional_field(
            contract,
            "execution_attempt",
            "canonical_execution_manifest_sha256",
        ),
        execution_sha,
        "Layer4 execution manifest",
    )
    _require_equal(
        contract.get("learning_algorithm_artifact_sha256"),
        learning_sha,
        "Layer4 learning algorithm",
    )
    component = documents["formal_buy_component_manifest"]
    _require_equal(
        _optional_field(
            contract,
            "formal_learning_algorithm",
            "manifest",
            "file_sha256",
        ),
        bindings["formal_buy_component_manifest"]["file_sha256"],
        "Layer4 learning algorithm manifest file",
    )
    _require_equal(
        _optional_field(
            contract,
            "formal_learning_algorithm",
            "formal_v24_execution_manifest_sha256",
        ),
        component.get("source_execution_manifest_sha256"),
        "Layer4 formal v24 execution",
    )
    _require_equal(
        _optional_field(
            contract,
            "formal_learning_algorithm",
            "component_result_canonical_sha256",
        ),
        component.get("component_result_canonical_sha256"),
        "Layer4 component result",
    )
    _require_equal(
        _optional_field(
            contract,
            "formal_learning_algorithm",
            "nested_oof_artifact_manifest_canonical_sha256",
        ),
        component.get("nested_oof_artifact_manifest_canonical_sha256"),
        "Layer4 nested OOF manifest",
    )
    _require_sha(
        _optional_field(
            contract,
            "source_predicate_bundle",
            "bundle",
            "file_sha256",
        ),
        "Layer4 source predicate bundle file",
    )
    _require_sha(
        _optional_field(contract, "parity_source", "amendment_file_sha256"),
        "Layer4 amendment source file",
    )
    _require_sha(
        _optional_field(contract, "parity_source", "v1_parity_file_sha256"),
        "Layer4 v1 parity source file",
    )
    _require_equal(
        _optional_field(contract, "exact_artifact", "artifact_sha256"),
        artifact_sha,
        "Layer4 exact artifact",
    )
    _require_equal(
        _optional_field(
            contract,
            "exact_artifact",
            "artifact_manifest",
            "file_sha256",
        ),
        bindings["exact_artifact_manifest"]["file_sha256"],
        "Layer4 artifact manifest file",
    )
    _require_equal(
        _optional_field(contract, "exact_artifact", "policy", "file_sha256"),
        bindings["exact_policy"]["file_sha256"],
        "Layer4 policy file",
    )
    _require_equal(
        _optional_field(
            contract,
            "exact_artifact",
            "predicate_bundle",
            "file_sha256",
        ),
        bindings["exact_predicate_bundle"]["file_sha256"],
        "Layer4 predicate file",
    )
    contract_days = tuple(str(day) for day in contract.get("ordered_development_days", ()))
    _require_equal(contract_days, ordered_days, "Layer4 ordered Development days")
    admitted_days: list[dict[str, str]] = []
    for index, utc_day in enumerate(ordered_days):
        role = f"{LAYER4_DAY_PREFIX}{index:02d}"
        receipt = documents[role]
        _require_equal(receipt.get("status"), "day_lockstep_complete", f"Layer4 {utc_day} status")
        _require_equal(receipt.get("utc_day"), utc_day, f"Layer4 day {index}")
        _require_equal(
            receipt.get("layer4_lockstep_contract_sha256"),
            contract_sha,
            f"Layer4 {utc_day} contract",
        )
        _require_equal(
            receipt.get("layer4_lockstep_contract_file_sha256"),
            bindings["layer4_contract"]["file_sha256"],
            f"Layer4 {utc_day} contract file",
        )
        _require_equal(
            receipt.get("learning_algorithm_artifact_sha256"),
            learning_sha,
            f"Layer4 {utc_day} learning algorithm",
        )
        _require_equal(
            receipt.get("artifact_sha256"), artifact_sha, f"Layer4 {utc_day} exact artifact"
        )
        _require_equal(
            receipt.get("artifact_manifest_file_sha256"),
            bindings["exact_artifact_manifest"]["file_sha256"],
            f"Layer4 {utc_day} artifact file",
        )
        _require_equal(
            receipt.get("policy_file_sha256"),
            bindings["exact_policy"]["file_sha256"],
            f"Layer4 {utc_day} policy file",
        )
        _require_equal(
            receipt.get("predicate_bundle_file_sha256"),
            bindings["exact_predicate_bundle"]["file_sha256"],
            f"Layer4 {utc_day} bundle file",
        )
        _require_equal(
            receipt.get("mechanics_identity_receipt"),
            mechanics_binding,
            f"Layer4 {utc_day} mechanics receipt",
        )
        _require_equal(
            _optional_field(receipt, "lockstep", "mismatch_count"), 0, f"Layer4 {utc_day} mismatch"
        )
        admitted_days.append(
            {
                "utc_day": utc_day,
                "file_name": f"{utc_day}.json",
                "file_sha256": str(bindings[role]["file_sha256"]),
                "canonical_day_receipt_sha256": str(bindings[role]["canonical_sha256"]),
            }
        )
    final = documents["layer4_final"]
    _require_equal(final.get("status"), "parity_complete", "Layer4 final status")
    _require_equal(final.get("layer"), "repeated_policy_lockstep", "Layer4 final layer")
    _require_equal(
        final.get("layer4_lockstep_contract_sha256"),
        contract_sha,
        "Layer4 final contract",
    )
    _require_equal(
        final.get("layer4_lockstep_contract_file_sha256"),
        bindings["layer4_contract"]["file_sha256"],
        "Layer4 final contract file",
    )
    _require_equal(
        final.get("learning_algorithm_artifact_sha256"),
        learning_sha,
        "Layer4 final learning algorithm",
    )
    _require_equal(final.get("artifact_sha256"), artifact_sha, "Layer4 final exact artifact")
    _require_equal(
        final.get("artifact_manifest_file_sha256"),
        bindings["exact_artifact_manifest"]["file_sha256"],
        "Layer4 final artifact manifest file",
    )
    _require_equal(
        final.get("policy_file_sha256"),
        bindings["exact_policy"]["file_sha256"],
        "Layer4 final policy file",
    )
    _require_equal(
        final.get("predicate_bundle_file_sha256"),
        bindings["exact_predicate_bundle"]["file_sha256"],
        "Layer4 final predicate bundle file",
    )
    _require_equal(
        final.get("mechanics_identity_receipt"),
        mechanics_binding,
        "Layer4 final mechanics receipt",
    )
    _require_equal(
        _optional_field(final, "evidence", "day_receipts"),
        admitted_days,
        "Layer4 final ordered day receipts",
    )
    _require_equal(
        _optional_field(final, "evidence", "day_count"),
        EXPECTED_DAY_COUNT,
        "Layer4 final day count",
    )
    _require_equal(_optional_field(final, "evidence", "mismatch_count"), 0, "Layer4 final mismatch")
    return contract_sha


def _validate_operational_gate_chain(
    documents: Mapping[str, Mapping[str, Any]],
    bindings: Mapping[str, Mapping[str, Any]],
    *,
    execution: Mapping[str, Any],
    artifact_sha: str,
) -> None:
    sell = documents["sell_54_case"]
    _require_equal(sell.get("status"), "parity_complete", "SELL safeguard status")
    _require_equal(sell.get("layer"), "sell_owner_54_case_unchanged", "SELL safeguard layer")
    _require_equal(sell.get("artifact_sha256"), artifact_sha, "SELL safeguard artifact")
    _require_equal(
        sell.get("artifact_manifest_file_sha256"),
        bindings["exact_artifact_manifest"]["file_sha256"],
        "SELL safeguard artifact manifest file",
    )
    _require_equal(
        sell.get("policy_file_sha256"),
        bindings["exact_policy"]["file_sha256"],
        "SELL safeguard policy file",
    )
    _require_equal(
        sell.get("predicate_bundle_file_sha256"),
        bindings["exact_predicate_bundle"]["file_sha256"],
        "SELL safeguard predicate bundle file",
    )
    for field, expected in (
        ("sell_tri_state_cases", 27),
        ("buy_tri_state_cases", 27),
        ("mismatch_count", 0),
    ):
        _require_equal(
            _optional_field(sell, "evidence", field), expected, f"SELL safeguard {field}"
        )
    regression = documents["runtime_regression"]
    _require_equal(regression.get("status"), "passed", "runtime regression status")
    _require_equal(regression.get("failed"), 0, "runtime regression failures")
    _require_equal(regression.get("errors"), 0, "runtime regression errors")
    _require_equal(regression.get("return_code"), 0, "runtime regression return code")
    _require_equal(
        regression.get("passed"),
        regression.get("expected_passed"),
        "runtime regression completed test count",
    )
    if not isinstance(regression.get("passed"), int) or int(regression["passed"]) <= 0:
        raise FinalCompositionError("runtime regression did not complete any test")
    _require_equal(regression.get("artifact_sha256"), artifact_sha, "runtime regression artifact")
    execution_commit = str(execution.get("public_base_commit", ""))
    execution_tag = str(execution.get("annotated_tag", ""))
    regression_execution = regression.get("execution_identity")
    if not isinstance(regression_execution, Mapping):
        raise FinalCompositionError("runtime regression execution identity is missing")
    _require_equal(
        regression_execution.get("execution_commit"),
        execution_commit,
        "runtime regression commit",
    )
    _require_equal(
        regression_execution.get("annotated_tag"),
        execution_tag,
        "runtime regression tag",
    )
    _require_equal(
        regression_execution.get("tag_peeled_commit"),
        execution_commit,
        "runtime regression tag peel",
    )
    for field in ("execution_tree", "annotated_tag_object"):
        if _GIT_SHA_RE.fullmatch(str(regression_execution.get(field, ""))) is None:
            raise FinalCompositionError(f"runtime regression {field} is not a Git object id")
    coverage = regression.get("coverage")
    if not isinstance(coverage, Mapping) or not coverage or any(
        value is not True for value in coverage.values()
    ):
        raise FinalCompositionError("runtime regression coverage is incomplete")
    superseded = regression.get("superseded_v1_failed_attempt")
    if (
        not isinstance(superseded, Mapping)
        or superseded.get("present") is not True
        or superseded.get("role") != "superseded_failed_attempt_only"
        or superseded.get("eligible_for_gate_satisfaction") is not False
        or superseded.get("status") != "failed"
    ):
        raise FinalCompositionError("runtime regression did not preserve the superseded v1 failure")

    gate = documents["deployment_gate"]
    _require_equal(
        gate.get("status"),
        "disabled_deploy_gate_passed_activation_not_yet_authorized",
        "deployment gate status",
    )
    gate_execution = gate.get("execution_identity")
    gate_artifact = gate.get("artifact_binding")
    gate_configs = gate.get("config_binding")
    process = gate.get("disabled_process_identity")
    resource = gate.get("resource_window")
    rollback = gate.get("rollback_identities")
    activation = gate.get("activation_contract")
    if any(
        not isinstance(value, Mapping)
        for value in (
            gate_execution,
            gate_artifact,
            gate_configs,
            process,
            resource,
            rollback,
            activation,
        )
    ):
        raise FinalCompositionError("deployment amendment is structurally incomplete")
    _require_equal(gate_execution.get("execution_commit"), execution_commit, "deployment commit")
    _require_equal(gate_execution.get("annotated_tag"), execution_tag, "deployment tag")
    _require_equal(
        gate_execution.get("tag_peeled_commit"), execution_commit, "deployment tag peel"
    )
    _require_equal(gate_artifact.get("artifact_sha256"), artifact_sha, "deployment artifact")
    artifact_files = gate_artifact.get("artifact_files")
    if not isinstance(artifact_files, Mapping):
        raise FinalCompositionError("deployment artifact triple is missing")
    for role, binding_role in (
        ("manifest", "exact_artifact_manifest"),
        ("policy", "exact_policy"),
        ("predicate_bundle", "exact_predicate_bundle"),
    ):
        record = artifact_files.get(role)
        if not isinstance(record, Mapping):
            raise FinalCompositionError(f"deployment artifact file is missing: {role}")
        _require_equal(
            record.get("sha256"),
            bindings[binding_role]["file_sha256"],
            f"deployment artifact file {role}",
        )
    disabled_config = gate_configs.get("disabled")
    active_config = gate_configs.get("active")
    if not isinstance(disabled_config, Mapping) or not isinstance(active_config, Mapping):
        raise FinalCompositionError("deployment config pair is missing")
    _require_bool(disabled_config.get("enabled"), False, "disabled deployment config")
    _require_bool(active_config.get("enabled"), True, "active deployment config")
    for config, label in ((disabled_config, "disabled"), (active_config, "active")):
        _require_equal(config.get("artifact_sha256"), artifact_sha, f"{label} config artifact")
        _require_bool(
            config.get("artifact_loaded_with_from_files"),
            True,
            f"{label} config artifact load",
        )
    _require_equal(
        process.get("artifact_sha256"), artifact_sha, "disabled process artifact"
    )
    _require_equal(
        process.get("canonical_process_identity_sha256"),
        document_sha256(process, "canonical_process_identity_sha256"),
        "disabled process canonical identity",
    )
    _require_equal(
        process.get("config_sha256"),
        disabled_config.get("config_sha256"),
        "disabled process config",
    )
    _require_equal(
        resource.get("status"),
        "concurrent_disabled_live_benchmark_passed",
        "deployment resource window status",
    )
    _require_equal(
        resource.get("canonical_resource_window_sha256"),
        document_sha256(resource, "canonical_resource_window_sha256"),
        "deployment resource window canonical identity",
    )
    checks = resource.get("checks")
    if (
        not isinstance(checks, Mapping)
        or not checks
        or any(value is not True for value in checks.values())
    ):
        raise FinalCompositionError("deployment gate checks are not all true")
    _require_equal(
        int(resource.get("live_pid", -1)),
        int(process.get("pid", -2)),
        "deployment resource/live PID",
    )
    if set(rollback) != {"primary_disabled", "deep_predecessor"}:
        raise FinalCompositionError("deployment rollback identities are incomplete")
    for name, rollback_identity in rollback.items():
        if not isinstance(rollback_identity, Mapping):
            raise FinalCompositionError(f"deployment rollback identity is malformed: {name}")
        _require_bool(
            rollback_identity.get("buy_e3_enabled"),
            False,
            f"deployment rollback {name} BUY E3",
        )
        _require_equal(
            rollback_identity.get("buy_deadline_identity"),
            "B0",
            f"deployment rollback {name} deadline",
        )
    for field, expected in (
        ("restart_only", True),
        ("sighup_allowed", False),
        ("fresh_pid_required", True),
        ("external_narrowgate_live_config_required", True),
        ("warmup_executes_natural_b0", True),
        ("hypothetical_scorer_allowed", False),
    ):
        _require_bool(activation.get(field), expected, f"deployment activation {field}")
    _permissions_are_false(gate, "deployment gate amendment")


def _build_source_role_resolution(
    documents: Mapping[str, Mapping[str, Any]],
    bindings: Mapping[str, Mapping[str, Any]],
    *,
    execution_sha: str,
    qualification_sha: str,
    preflight_sha: str,
    artifact_sha: str,
) -> dict[str, Any]:
    execution = documents["attempt_execution_manifest"]
    source = documents["source_execution_manifest"]
    labels = documents["label_materialization"]
    manifest = documents["exact_artifact_manifest"]
    source_sides = _optional_field(source, "execution_contract", "formal_sides")
    if source_sides is None:
        source_sides = source.get("formal_sides")
    payload: dict[str, Any] = {
        "schema_version": SOURCE_ROLE_SCHEMA,
        "identity": SOURCE_ROLE_IDENTITY,
        "status": "passed_outcome_blind_source_role_resolution",
        "historical_source_execution": {
            "manifest_canonical_sha256": _binding_sha(bindings, "source_execution_manifest"),
            "formal_sides": list(source_sides),
            "role": "outcome_blind_source_mechanics_only",
            "backend_contract_sha256": canonical_sha256(source.get("backend")),
            "executor_contract_sha256": canonical_sha256(source.get("executor")),
            "historical_sell_only_scope_does_not_define_owner_refit_side": True,
        },
        "owner_execution": {
            "manifest_canonical_sha256": execution_sha,
            "selected_side": _field(execution, "execution_contract", "selected_side"),
            "backend_contract_sha256": canonical_sha256(execution.get("backend")),
            "executor_contract_sha256": canonical_sha256(execution.get("executor")),
            "active_label_executor_role": "qualified_dual_side_canonical_adapter",
            "label_side": labels.get("side"),
            "fresh_adapter_identity": labels.get("fresh_adapter_identity"),
            "fresh_adapter_artifact_sha256": labels.get("fresh_adapter_artifact_sha256"),
            "cpp_qualification_receipt_sha256": qualification_sha,
            "owner_preflight_receipt_sha256": preflight_sha,
            "exact_artifact_sha256": artifact_sha,
            "exact_artifact_side": "BUY",
        },
        "resolution": {
            "source_backend_and_executor_match": execution.get("backend") == source.get("backend")
            and execution.get("executor") == source.get("executor"),
            "historical_formal_sides": ["SELL"],
            "actual_label_and_refit_side": "BUY",
            "exact_artifact_training_days": list(manifest.get("training_days", ())),
            "source_role_is_not_an_economic_execution_claim": True,
        },
        "source_bindings": {
            role: {
                "file_sha256": bindings[role]["file_sha256"],
                "canonical_sha256": bindings[role]["canonical_sha256"],
            }
            for role in (
                "attempt_execution_manifest",
                "source_execution_manifest",
                "cpp_qualification",
                "owner_execution_preflight",
                "label_materialization",
                "exact_artifact_manifest",
            )
        },
        "economic_values_exposed": False,
        "economic_values_used_for_selection": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
        "permissions": {
            "research_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    }
    if payload["resolution"]["source_backend_and_executor_match"] is not True:
        raise FinalCompositionError("source role resolution backend/executor drifted")
    payload["canonical_source_role_resolution_receipt_sha256"] = document_sha256(
        payload,
        "canonical_source_role_resolution_receipt_sha256",
    )
    return payload


def _ordered_roles(days: tuple[str, ...]) -> tuple[str, ...]:
    return (
        *BASE_ROLE_ORDER,
        *(f"{LAYER4_DAY_PREFIX}{index:02d}" for index, _day in enumerate(days)),
        *TAIL_ROLE_ORDER,
    )


def _build_final_payload(
    documents: Mapping[str, Mapping[str, Any]],
    bindings: Mapping[str, Mapping[str, Any]],
    *,
    learning_sha: str,
    execution_sha: str,
    qualification_sha: str,
    preflight_sha: str,
    artifact_sha: str,
    ordered_days: tuple[str, ...],
    layer4_contract_sha: str,
) -> dict[str, Any]:
    ordered_roles = _ordered_roles(ordered_days)
    ordered_evidence = [dict(bindings[role]) for role in ordered_roles]
    payload: dict[str, Any] = {
        "schema_version": COMPOSITION_SCHEMA,
        "identity": COMPOSITION_IDENTITY,
        "status": "owner_buy_e3_final_evidence_composed",
        "formal_learning_algorithm": {
            "identity": "formal_v24_buy_learning_algorithm",
            "artifact_manifest_role": "formal_buy_component_manifest",
            "learning_algorithm_artifact_sha256": learning_sha,
            "exact_artifact_sha256": artifact_sha,
            "identities_are_distinct": learning_sha != artifact_sha,
            "old_oof_applies_to_learning_algorithm_only": True,
            "exact_artifact_oof_available": False,
        },
        "formal_closeout_and_owner_override": {
            "formal_component_manifest_sha256": _binding_sha(
                bindings, "formal_buy_component_manifest"
            ),
            "formal_component_validation_sha256": _binding_sha(
                bindings, "formal_buy_component_validation"
            ),
            "joint_closeout_manifest_sha256": _binding_sha(bindings, "joint_closeout_manifest"),
            "owner_decision_sha256": _binding_sha(bindings, "owner_decision"),
            "research_supported": False,
            "owner_risk_accepted": True,
            "outcome_informed_owner_override": True,
            "formal_hierarchy_passed": False,
            "formal_hard_gates_passed": False,
        },
        "execution_chain": {
            "execution_manifest_sha256": execution_sha,
            "cpp_builder_preflight_sha256": _binding_sha(bindings, "cpp_builder_preflight"),
            "cpp_quick_preflight_sha256": _binding_sha(bindings, "cpp_quick_preflight"),
            "cpp_qualification_sha256": qualification_sha,
            "owner_preflight_sha256": preflight_sha,
            "source_role_resolution_sha256": _binding_sha(bindings, "source_role_resolution"),
            "label_materialization_sha256": _binding_sha(bindings, "label_materialization"),
            "refit_receipt_sha256": _binding_sha(bindings, "refit_receipt"),
        },
        "exact_artifact": {
            "artifact_sha256": artifact_sha,
            "artifact_manifest_file_sha256": bindings["exact_artifact_manifest"]["file_sha256"],
            "policy_file_sha256": bindings["exact_policy"]["file_sha256"],
            "predicate_bundle_file_sha256": bindings["exact_predicate_bundle"]["file_sha256"],
            "training_days": list(ordered_days),
            "exact_artifact_oof_available": False,
        },
        "four_layer_parity": {
            "research_compiled_sha256": _binding_sha(bindings, LAYER1_ROLE),
            "development_snapshot_sha256": _binding_sha(bindings, LAYER2_ROLE),
            "streaming_offline_sha256": _binding_sha(bindings, LAYER3_ROLE),
            "mechanics_identity_receipt_sha256": _binding_sha(
                bindings, LAYER4_MECHANICS_ROLE
            ),
            "mechanics_identity_receipt_file_sha256": bindings[
                LAYER4_MECHANICS_ROLE
            ]["file_sha256"],
            "outcome_blind_mechanics_sha256": documents[LAYER4_MECHANICS_ROLE][
                "mechanics_receipt_sha256"
            ],
            "layer4_contract_sha256": layer4_contract_sha,
            "layer4_day_receipt_sha256": [
                _binding_sha(bindings, f"{LAYER4_DAY_PREFIX}{index:02d}")
                for index in range(EXPECTED_DAY_COUNT)
            ],
            "layer4_final_sha256": _binding_sha(bindings, "layer4_final"),
            "ordered_day_count": EXPECTED_DAY_COUNT,
        },
        "sell_safeguard": {
            "receipt_sha256": _binding_sha(bindings, "sell_54_case"),
            "mismatch_count": 0,
        },
        "runtime_regression": {
            "receipt_sha256": _binding_sha(bindings, "runtime_regression"),
            "failed": 0,
        },
        "deployment_evidence": {
            "deployment_gate_sha256": _binding_sha(bindings, "deployment_gate"),
            "deployment_gate_schema": documents["deployment_gate"]["schema_version"],
            "disabled_process_identity_sha256": _require_sha(
                _optional_field(
                    documents["deployment_gate"],
                    "disabled_process_identity",
                    "canonical_process_identity_sha256",
                ),
                "disabled process identity SHA256",
            ),
            "concurrent_resource_window_sha256": _require_sha(
                _optional_field(
                    documents["deployment_gate"],
                    "resource_window",
                    "canonical_resource_window_sha256",
                ),
                "concurrent resource window SHA256",
            ),
            "activation_authorized": False,
        },
        "evidence_boundary": {
            "panel_role": "Development",
            "validation_read": False,
            "sealed_holdout_read": False,
            "economic_values_exposed": False,
            "economic_values_used_for_selection": False,
            "new_economic_arm_run": False,
            "hypothetical_live_actions_scored": False,
            "exact_artifact_oof_available": False,
        },
        "permissions": {
            "research_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
        "ordered_evidence": ordered_evidence,
        "ordered_evidence_sha256": canonical_sha256(ordered_evidence),
    }
    if payload["formal_learning_algorithm"]["identities_are_distinct"] is not True:
        raise FinalCompositionError("learning algorithm and exact artifact SHA must be distinct")
    payload["canonical_final_composition_receipt_sha256"] = document_sha256(
        payload,
        "canonical_final_composition_receipt_sha256",
    )
    return payload


def _atomic_write_json_no_overwrite(path: Path, payload: Mapping[str, Any]) -> str:
    encoded = _json_bytes(payload)
    if path.exists() or path.is_symlink():
        raise FinalCompositionError(f"immutable output already exists: {path}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            raise FinalCompositionError(f"immutable output already exists: {path}") from exc
        temporary.unlink()
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
    if stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise FinalCompositionError("atomic receipt permission drifted")
    return hashlib.sha256(encoded).hexdigest()


def compose_final_composition(
    *,
    evidence_root: Path,
    inputs: CompositionInputs,
    source_role_output: Path,
    output: Path,
) -> dict[str, Any]:
    root = _evidence_root(evidence_root)
    source_output = _admit_output_path(source_role_output, root)
    final_output = _admit_output_path(output, root)
    if source_output == final_output:
        raise FinalCompositionError("source-role and final receipt outputs must differ")
    documents, bindings = _load_bound_inputs(inputs, root)
    learning_sha = _validate_formal_chain(documents, bindings)
    execution_sha, qualification_sha, preflight_sha = _validate_execution_chain(
        documents,
        bindings,
    )
    artifact_sha, ordered_days = _validate_refit_and_artifact_chain(
        documents,
        bindings,
        execution_sha=execution_sha,
        qualification_sha=qualification_sha,
        preflight_sha=preflight_sha,
    )
    layer4_contract_sha = _validate_parity_chain(
        documents,
        bindings,
        execution_sha=execution_sha,
        learning_sha=learning_sha,
        artifact_sha=artifact_sha,
        ordered_days=ordered_days,
        evidence_root=root,
    )
    _validate_operational_gate_chain(
        documents,
        bindings,
        execution=documents["attempt_execution_manifest"],
        artifact_sha=artifact_sha,
    )
    source_payload = _build_source_role_resolution(
        documents,
        bindings,
        execution_sha=execution_sha,
        qualification_sha=qualification_sha,
        preflight_sha=preflight_sha,
        artifact_sha=artifact_sha,
    )
    documents = dict(documents)
    bindings = dict(bindings)
    documents["source_role_resolution"] = source_payload
    bindings["source_role_resolution"] = _predicted_binding(
        "source_role_resolution",
        source_output,
        root,
        source_payload,
    )
    final_payload = _build_final_payload(
        documents,
        bindings,
        learning_sha=learning_sha,
        execution_sha=execution_sha,
        qualification_sha=qualification_sha,
        preflight_sha=preflight_sha,
        artifact_sha=artifact_sha,
        ordered_days=ordered_days,
        layer4_contract_sha=layer4_contract_sha,
    )
    _atomic_write_json_no_overwrite(source_output, source_payload)
    actual_source_binding = _binding(
        "source_role_resolution",
        source_output,
        root,
        strict_load_json(source_output),
    )
    _require_equal(
        actual_source_binding,
        bindings["source_role_resolution"],
        "written source-role receipt binding",
    )
    _atomic_write_json_no_overwrite(final_output, final_payload)
    return validate_final_composition(evidence_root=root, receipt_path=final_output)


def _documents_from_final_receipt(
    receipt: Mapping[str, Any],
    root: Path,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], tuple[str, ...]]:
    ordered = receipt.get("ordered_evidence")
    if not isinstance(ordered, list):
        raise FinalCompositionError("final receipt ordered evidence is missing")
    if receipt.get("ordered_evidence_sha256") != canonical_sha256(ordered):
        raise FinalCompositionError("ordered evidence SHA256 drifted")
    artifact_days = _optional_field(receipt, "exact_artifact", "training_days")
    if not isinstance(artifact_days, list):
        raise FinalCompositionError("final receipt training days are missing")
    days = tuple(str(day) for day in artifact_days)
    expected_roles = _ordered_roles(days)
    observed_roles = tuple(
        str(item.get("role", "")) if isinstance(item, Mapping) else "" for item in ordered
    )
    if observed_roles != expected_roles:
        raise FinalCompositionError("final receipt evidence roles are missing, extra, or reordered")
    documents: dict[str, dict[str, Any]] = {}
    bindings: dict[str, dict[str, Any]] = {}
    seen_paths: set[str] = set()
    for raw in ordered:
        if not isinstance(raw, dict):
            raise FinalCompositionError("evidence binding is not an object")
        role = str(raw["role"])
        relative = str(raw.get("path", ""))
        if not relative or relative in seen_paths or Path(relative).is_absolute():
            raise FinalCompositionError("evidence binding path is invalid or duplicated")
        seen_paths.add(relative)
        path = _admit_input_path(root / relative, root)
        payload = strict_load_json(path)
        observed = _binding(role, path, root, payload)
        if observed != raw:
            raise FinalCompositionError(f"evidence binding drifted: {role}")
        documents[role] = payload
        bindings[role] = observed
    return documents, bindings, days


def validate_final_composition(
    *,
    evidence_root: Path,
    receipt_path: Path,
) -> dict[str, Any]:
    root = _evidence_root(evidence_root)
    final_path = _admit_input_path(receipt_path, root)
    receipt = strict_load_json(final_path)
    if (
        receipt.get("schema_version") != COMPOSITION_SCHEMA
        or receipt.get("identity") != COMPOSITION_IDENTITY
        or receipt.get("status") != "owner_buy_e3_final_evidence_composed"
        or receipt.get("canonical_final_composition_receipt_sha256")
        != document_sha256(receipt, "canonical_final_composition_receipt_sha256")
    ):
        raise FinalCompositionError("final composition receipt identity drifted")
    _validate_evidence_boundaries(receipt, "final composition")
    _permissions_are_false(receipt, "final composition")
    documents, bindings, days = _documents_from_final_receipt(receipt, root)
    learning_sha = _validate_formal_chain(documents, bindings)
    execution_sha, qualification_sha, preflight_sha = _validate_execution_chain(
        documents,
        bindings,
    )
    artifact_sha, artifact_days = _validate_refit_and_artifact_chain(
        documents,
        bindings,
        execution_sha=execution_sha,
        qualification_sha=qualification_sha,
        preflight_sha=preflight_sha,
    )
    _require_equal(days, artifact_days, "final receipt artifact days")
    layer4_contract_sha = _validate_parity_chain(
        documents,
        bindings,
        execution_sha=execution_sha,
        learning_sha=learning_sha,
        artifact_sha=artifact_sha,
        ordered_days=artifact_days,
        evidence_root=root,
    )
    _validate_operational_gate_chain(
        documents,
        bindings,
        execution=documents["attempt_execution_manifest"],
        artifact_sha=artifact_sha,
    )
    expected_source = _build_source_role_resolution(
        documents,
        bindings,
        execution_sha=execution_sha,
        qualification_sha=qualification_sha,
        preflight_sha=preflight_sha,
        artifact_sha=artifact_sha,
    )
    _require_equal(
        documents["source_role_resolution"],
        expected_source,
        "source-role resolution receipt",
    )
    expected_final = _build_final_payload(
        documents,
        bindings,
        learning_sha=learning_sha,
        execution_sha=execution_sha,
        qualification_sha=qualification_sha,
        preflight_sha=preflight_sha,
        artifact_sha=artifact_sha,
        ordered_days=artifact_days,
        layer4_contract_sha=layer4_contract_sha,
    )
    _require_equal(receipt, expected_final, "final composition receipt")
    return dict(receipt)


def _path_argument(value: str) -> Path:
    return Path(value)


def _add_input_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--formal-buy-component-manifest", type=_path_argument, required=True)
    parser.add_argument("--formal-buy-component-validation", type=_path_argument, required=True)
    parser.add_argument("--joint-closeout-manifest", type=_path_argument, required=True)
    parser.add_argument("--owner-decision", type=_path_argument, required=True)
    parser.add_argument("--attempt-execution-manifest", type=_path_argument, required=True)
    parser.add_argument("--source-execution-manifest", type=_path_argument, required=True)
    parser.add_argument("--cpp-builder-preflight", type=_path_argument, required=True)
    parser.add_argument("--cpp-quick-preflight", type=_path_argument, required=True)
    parser.add_argument("--cpp-qualification", type=_path_argument, required=True)
    parser.add_argument("--owner-execution-preflight", type=_path_argument, required=True)
    parser.add_argument("--label-materialization", type=_path_argument, required=True)
    parser.add_argument("--refit-receipt", type=_path_argument, required=True)
    parser.add_argument("--exact-artifact-manifest", type=_path_argument, required=True)
    parser.add_argument("--exact-policy", type=_path_argument, required=True)
    parser.add_argument("--exact-predicate-bundle", type=_path_argument, required=True)
    parser.add_argument("--parity-research-compiled", type=_path_argument, required=True)
    parser.add_argument("--parity-development-snapshot", type=_path_argument, required=True)
    parser.add_argument("--parity-streaming-offline", type=_path_argument, required=True)
    parser.add_argument("--layer4-mechanics", type=_path_argument, required=True)
    parser.add_argument("--layer4-contract", type=_path_argument, required=True)
    parser.add_argument(
        "--layer4-day-receipt",
        type=_path_argument,
        action="append",
        required=True,
        help="Repeat exactly 30 times in frozen chronological order.",
    )
    parser.add_argument("--layer4-final", type=_path_argument, required=True)
    parser.add_argument("--sell-54-case", type=_path_argument, required=True)
    parser.add_argument("--runtime-regression", type=_path_argument, required=True)
    parser.add_argument("--deployment-gate", type=_path_argument, required=True)


def _inputs_from_args(args: argparse.Namespace) -> CompositionInputs:
    return CompositionInputs(
        formal_buy_component_manifest=args.formal_buy_component_manifest,
        formal_buy_component_validation=args.formal_buy_component_validation,
        joint_closeout_manifest=args.joint_closeout_manifest,
        owner_decision=args.owner_decision,
        attempt_execution_manifest=args.attempt_execution_manifest,
        source_execution_manifest=args.source_execution_manifest,
        cpp_builder_preflight=args.cpp_builder_preflight,
        cpp_quick_preflight=args.cpp_quick_preflight,
        cpp_qualification=args.cpp_qualification,
        owner_execution_preflight=args.owner_execution_preflight,
        label_materialization=args.label_materialization,
        refit_receipt=args.refit_receipt,
        exact_artifact_manifest=args.exact_artifact_manifest,
        exact_policy=args.exact_policy,
        exact_predicate_bundle=args.exact_predicate_bundle,
        parity_research_compiled=args.parity_research_compiled,
        parity_development_snapshot=args.parity_development_snapshot,
        parity_streaming_offline=args.parity_streaming_offline,
        layer4_mechanics=args.layer4_mechanics,
        layer4_contract=args.layer4_contract,
        layer4_day_receipts=tuple(args.layer4_day_receipt),
        layer4_final=args.layer4_final,
        sell_54_case=args.sell_54_case,
        runtime_regression=args.runtime_regression,
        deployment_gate=args.deployment_gate,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    compose = subparsers.add_parser("compose")
    compose.add_argument("--evidence-root", type=_path_argument, required=True)
    _add_input_arguments(compose)
    compose.add_argument("--source-role-output", type=_path_argument, required=True)
    compose.add_argument("--output", type=_path_argument, required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--evidence-root", type=_path_argument, required=True)
    validate.add_argument("--receipt", type=_path_argument, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "compose":
        result = compose_final_composition(
            evidence_root=args.evidence_root,
            inputs=_inputs_from_args(args),
            source_role_output=args.source_role_output,
            output=args.output,
        )
    else:
        result = validate_final_composition(
            evidence_root=args.evidence_root,
            receipt_path=args.receipt,
        )
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


__all__ = [
    "COMPOSITION_IDENTITY",
    "COMPOSITION_SCHEMA",
    "CompositionInputs",
    "EXPECTED_DAY_COUNT",
    "FinalCompositionError",
    "IDENTITY",
    "SOURCE_ROLE_IDENTITY",
    "SOURCE_ROLE_SCHEMA",
    "canonical_sha256",
    "compose_final_composition",
    "document_sha256",
    "file_sha256",
    "strict_load_json",
    "validate_final_composition",
]


if __name__ == "__main__":
    raise SystemExit(main())
