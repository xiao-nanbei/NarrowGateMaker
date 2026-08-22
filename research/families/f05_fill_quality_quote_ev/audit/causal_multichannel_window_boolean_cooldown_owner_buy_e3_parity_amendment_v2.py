#!/usr/bin/env python3
"""Post-freeze Layer-4 receipt binding amendment for the BUY E3 owner artifact.

This module does not change the research identity or policy semantics.  It
reuses the frozen v1 artifact loader and Layers 1-3, while replacing only the
Layer-4 resumable receipt protocol.  The v2 protocol derives the formal BUY
learning-algorithm identity from its component artifact manifest and binds
that identity into the contract, every daily receipt, and the final receipt.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_formal_component_closeout_v1 as component_closeout,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_predicate_view_v1 as predicate_view,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_v1 as parity_v1,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_refit_v1 as refit,
)

IDENTITY = parity_v1.IDENTITY
SCHEMA_AMENDMENT = f"{IDENTITY}.layer4_receipt_binding_amendment.v2"
LAYER4_CONTRACT_SCHEMA = f"{IDENTITY}.layer4_lockstep_contract.v1"
LOCKSTEP_DAY_SCHEMA_V2 = f"{IDENTITY}.repeated_policy_lockstep_day.v2"
LAYER4_RECEIPT_SCHEMA_V2 = f"{IDENTITY}.parity_receipt.v2"
LAYER4_LAYER = parity_v1.REPEATED_POLICY_LOCKSTEP_LAYER

# These constants identify the admitted evidence sources.  The learning
# algorithm SHA is still derived from the component manifest; the constant is
# an admission check against substituting another self-consistent manifest.
FORMAL_V24_BUY_COMPONENT_CANONICAL_SHA256 = (
    "de056921335450619f7d8099d545125f1d7d6045ebc448dc2526e63c4cb72072"
)
FORMAL_V24_EXECUTION_MANIFEST_SHA256 = (
    "2021a70f2f15f4fff82240cdc494556413da0fc24d369be00fd60628bcf3395a"
)
ATTEMPT2_EXECUTION_MANIFEST_SHA256 = (
    "3d016578fb31acc6850e3032fb96a1e45c54ead55c1f6d7a1102e9be27a9133d"
)
ATTEMPT2_EXECUTION_COMMIT = "c170493ea5838b6e3a715006db352c0a484d3943"
ATTEMPT2_EXECUTION_TAG = "f05-owner-buy-e3-live-attempt2-20260821"

EXPECTED_DAY_COUNT = refit.EXPECTED_DAY_COUNT
_SHA256_LENGTH = 64
_PRIVATE_MODE = 0o600
_BOUNDARY = {
    "economic_values_exposed": False,
    "economic_values_used_for_selection": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "hypothetical_live_scoring": False,
}
_PERMISSIONS = {
    "research_authorized": False,
    "action_authorized": False,
    "live_authorized": False,
}
_LOCKSTEP_RESULT_FIELDS = {
    "summary_signature_sha256",
    "campaign_frame_sha256",
    "fill_frame_sha256",
    "decision_frame_sha256",
    "decision_count",
    "campaign_count",
    "fill_count",
    "mismatch_count",
}


class OwnerBuyE3ParityAmendmentError(RuntimeError):
    """Raised when the amended Layer-4 evidence chain does not close."""


# Re-export the frozen artifact loader and the unchanged mechanics-only layers.
LoadedExactArtifact = parity_v1.LoadedExactArtifact
load_exact_artifact = parity_v1.load_exact_artifact
run_research_compiled_parity = parity_v1.run_research_compiled_parity
run_development_snapshot_parity = parity_v1.run_development_snapshot_parity
run_streaming_offline_parity = parity_v1.run_streaming_offline_parity


def _require_sha256(value: Any, label: str) -> str:
    digest = str(value).strip().lower()
    if len(digest) != _SHA256_LENGTH or any(
        character not in "0123456789abcdef" for character in digest
    ):
        raise OwnerBuyE3ParityAmendmentError(f"{label} is not a lowercase SHA256")
    return digest


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise OwnerBuyE3ParityAmendmentError("non-canonical JSON value") from exc
    return hashlib.sha256(encoded).hexdigest()


def _document_sha256(value: Mapping[str, Any], field: str) -> str:
    body = dict(value)
    body.pop(field, None)
    return _canonical_sha256(body)


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                value,
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            ).encode("ascii")
            + b"\n"
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise OwnerBuyE3ParityAmendmentError("receipt contains non-canonical JSON") from exc


def _reject_duplicate_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise OwnerBuyE3ParityAmendmentError(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def _reject_json_constant(value: str) -> None:
    raise OwnerBuyE3ParityAmendmentError(f"non-finite JSON constant: {value}")


def _absolute_path(path: str | Path) -> Path:
    raw = Path(path).expanduser()
    if ".." in raw.parts:
        raise OwnerBuyE3ParityAmendmentError("path traversal is forbidden")
    return Path(os.path.abspath(raw))


def _reject_symlink_chain(path: Path, *, label: str) -> None:
    for candidate in (path, *path.parents):
        if candidate.is_symlink():
            raise OwnerBuyE3ParityAmendmentError(f"{label} traverses a symlink")


def _regular_file(path: str | Path, *, label: str, private: bool = True) -> Path:
    absolute = _absolute_path(path)
    _reject_symlink_chain(absolute, label=label)
    try:
        metadata = absolute.lstat()
    except OSError as exc:
        raise OwnerBuyE3ParityAmendmentError(f"{label} is missing") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise OwnerBuyE3ParityAmendmentError(f"{label} is not a regular file")
    if absolute.resolve(strict=True) != absolute:
        raise OwnerBuyE3ParityAmendmentError(f"{label} path drifted")
    if private and stat.S_IMODE(metadata.st_mode) != _PRIVATE_MODE:
        raise OwnerBuyE3ParityAmendmentError(f"{label} mode is not 0600")
    return absolute


def _safe_output_path(path: str | Path, *, label: str) -> Path:
    destination = _absolute_path(path)
    _reject_symlink_chain(destination, label=label)
    if destination.exists() or destination.is_symlink():
        raise OwnerBuyE3ParityAmendmentError(f"immutable {label} already exists")
    parent = destination.parent
    _reject_symlink_chain(parent, label=f"{label} parent")
    parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink_chain(parent, label=f"{label} parent")
    if not parent.is_dir():
        raise OwnerBuyE3ParityAmendmentError(f"{label} parent is not a directory")
    return destination


def _load_json(path: str | Path, *, label: str, private: bool = True) -> dict[str, Any]:
    source = _regular_file(path, label=label, private=private)
    try:
        payload = json.loads(
            source.read_text(encoding="ascii"),
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_json_constant,
        )
    except OwnerBuyE3ParityAmendmentError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise OwnerBuyE3ParityAmendmentError(f"{label} is unreadable") from exc
    if not isinstance(payload, dict):
        raise OwnerBuyE3ParityAmendmentError(f"{label} root is not an object")
    _canonical_sha256(payload)
    return payload


def _atomic_write_json(path: str | Path, payload: Mapping[str, Any]) -> str:
    destination = _safe_output_path(path, label="receipt")
    encoded = _json_bytes(payload)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, _PRIVATE_MODE)
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError as exc:
            raise OwnerBuyE3ParityAmendmentError(
                f"immutable receipt already exists: {destination}"
            ) from exc
        directory_fd = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    if stat.S_IMODE(destination.stat().st_mode) != _PRIVATE_MODE:
        raise OwnerBuyE3ParityAmendmentError("atomic receipt mode drifted")
    return hashlib.sha256(encoded).hexdigest()


def _file_binding(path: str | Path, *, label: str) -> dict[str, Any]:
    source = _regular_file(path, label=label, private=True)
    metadata = source.stat()
    return {
        "path": str(source),
        "file_sha256": _file_sha256(source),
        "size_bytes": metadata.st_size,
        "mode": format(stat.S_IMODE(metadata.st_mode), "04o"),
    }


def _validate_file_binding(binding: Any, *, label: str) -> Path:
    if not isinstance(binding, Mapping) or set(binding) != {
        "path",
        "file_sha256",
        "size_bytes",
        "mode",
    }:
        raise OwnerBuyE3ParityAmendmentError(f"{label} binding is malformed")
    source = _regular_file(str(binding["path"]), label=label, private=True)
    if (
        _file_sha256(source) != _require_sha256(binding["file_sha256"], f"{label} file SHA256")
        or source.stat().st_size != binding["size_bytes"]
        or binding["mode"] != "0600"
    ):
        raise OwnerBuyE3ParityAmendmentError(f"{label} binding drifted")
    return source


def _ordered_days(values: Sequence[str], *, expected_count: int = EXPECTED_DAY_COUNT) -> tuple[str, ...]:
    days = tuple(str(value) for value in values)
    try:
        parsed = tuple(date.fromisoformat(value) for value in days)
    except ValueError as exc:
        raise OwnerBuyE3ParityAmendmentError("Development day is not ISO-8601") from exc
    if (
        len(days) != expected_count
        or len(set(days)) != len(days)
        or parsed != tuple(sorted(parsed))
    ):
        raise OwnerBuyE3ParityAmendmentError("ordered Development day identity drifted")
    return days


def _validate_formal_component_manifest(path: str | Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _load_json(path, label="formal v24 BUY component artifact manifest")
    canonical = _require_sha256(
        payload.get("canonical_artifact_manifest_sha256"),
        "formal BUY component canonical artifact manifest SHA256",
    )
    component_result = _require_sha256(
        payload.get("component_result_canonical_sha256"),
        "component result canonical SHA256",
    )
    nested_oof = _require_sha256(
        payload.get("nested_oof_artifact_manifest_canonical_sha256"),
        "nested OOF artifact manifest canonical SHA256",
    )
    execution = _require_sha256(
        payload.get("source_execution_manifest_sha256"),
        "formal v24 execution manifest SHA256",
    )
    if (
        payload.get("schema_version")
        != f"{component_closeout.IDENTITY}.component_artifact_manifest.v1"
        or payload.get("identity")
        != f"{component_closeout.IDENTITY}:formal_v24_buy_component_artifacts"
        or payload.get("formal_side") != "BUY"
        or payload.get("permissions") != component_closeout.EXPECTED_COMPONENT_PERMISSIONS
        or canonical != _document_sha256(payload, "canonical_artifact_manifest_sha256")
        or canonical != FORMAL_V24_BUY_COMPONENT_CANONICAL_SHA256
        or execution != FORMAL_V24_EXECUTION_MANIFEST_SHA256
    ):
        raise OwnerBuyE3ParityAmendmentError("formal v24 BUY component identity drifted")
    role_shas = {
        "learning_algorithm_artifact": canonical,
        "component_result": component_result,
        "nested_oof_manifest": nested_oof,
        "v24_execution_manifest": execution,
        "component_manifest_file": _file_sha256(_regular_file(path, label="formal component manifest")),
    }
    if len(set(role_shas.values())) != len(role_shas):
        raise OwnerBuyE3ParityAmendmentError("formal component SHA roles were conflated")
    return payload, {
        "manifest": _file_binding(path, label="formal v24 BUY component artifact manifest"),
        "learning_algorithm_artifact_sha256": canonical,
        "component_result_canonical_sha256": component_result,
        "nested_oof_artifact_manifest_canonical_sha256": nested_oof,
        "formal_v24_execution_manifest_sha256": execution,
    }


def _validate_attempt2_manifest(path: str | Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _load_json(path, label="owner attempt2 execution manifest")
    canonical = _require_sha256(
        payload.get("canonical_execution_manifest_sha256"),
        "attempt2 execution manifest canonical SHA256",
    )
    commit = str(payload.get("public_base_commit", ""))
    tag = str(payload.get("annotated_tag", ""))
    if (
        payload.get("schema_version") != refit.EXECUTION_MANIFEST_SCHEMA
        or payload.get("identity") != IDENTITY
        or payload.get("status") != "pre_refit_owner_execution_bound"
        or canonical != _document_sha256(payload, "canonical_execution_manifest_sha256")
        or canonical != ATTEMPT2_EXECUTION_MANIFEST_SHA256
        or commit != ATTEMPT2_EXECUTION_COMMIT
        or tag != ATTEMPT2_EXECUTION_TAG
        or payload.get("permissions")
        != {
            "research_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        }
    ):
        raise OwnerBuyE3ParityAmendmentError("owner attempt2 execution identity drifted")
    return payload, {
        "manifest": _file_binding(path, label="owner attempt2 execution manifest"),
        "canonical_execution_manifest_sha256": canonical,
        "execution_commit": commit,
        "annotated_tag": tag,
    }


def _validate_artifact_documents(
    artifact: LoadedExactArtifact | None,
    bindings: Mapping[str, Any],
    *,
    ordered_days: Sequence[str],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest_path = _validate_file_binding(bindings.get("artifact_manifest"), label="artifact manifest")
    policy_path = _validate_file_binding(bindings.get("policy"), label="artifact policy")
    bundle_path = _validate_file_binding(bindings.get("predicate_bundle"), label="artifact predicate bundle")
    manifest = _load_json(manifest_path, label="artifact manifest")
    policy = _load_json(policy_path, label="artifact policy")
    bundle = _load_json(bundle_path, label="artifact predicate bundle")
    artifact_sha = _require_sha256(bindings.get("artifact_sha256"), "exact artifact SHA256")
    if (
        manifest.get("identity") != IDENTITY
        or manifest.get("status") != "exact_buy_e3_artifact_frozen"
        or manifest.get("artifact_sha256") != artifact_sha
        or artifact_sha != _document_sha256(manifest, "artifact_sha256")
        or manifest.get("policy_file_sha256") != bindings["policy"]["file_sha256"]
        or manifest.get("predicate_bundle_file_sha256")
        != bindings["predicate_bundle"]["file_sha256"]
        or tuple(manifest.get("training_days", ())) != tuple(ordered_days)
        or policy.get("identity") != IDENTITY
        or policy.get("canonical_sha256") != _document_sha256(policy, "canonical_sha256")
        or bundle.get("identity") != IDENTITY
        or bundle.get("canonical_sha256") != _document_sha256(bundle, "canonical_sha256")
    ):
        raise OwnerBuyE3ParityAmendmentError("exact BUY E3 artifact identity drifted")
    if artifact is not None and (
        artifact.manifest_path != manifest_path
        or artifact.policy_path != policy_path
        or artifact.predicate_bundle_path != bundle_path
        or artifact.manifest_file_sha256 != bindings["artifact_manifest"]["file_sha256"]
        or artifact.policy_file_sha256 != bindings["policy"]["file_sha256"]
        or artifact.predicate_bundle_file_sha256 != bindings["predicate_bundle"]["file_sha256"]
        or artifact.artifact_sha256 != artifact_sha
        or dict(artifact.manifest) != manifest
        or dict(artifact.policy_document) != policy
        or dict(artifact.predicate_bundle_document) != bundle
    ):
        raise OwnerBuyE3ParityAmendmentError("loaded exact artifact drifted from contract")
    return manifest, policy, bundle


def _validate_source_predicate_bundle(
    binding: Mapping[str, Any],
    expected: predicate_view.FrozenPredicateBundle | None,
) -> dict[str, Any]:
    path = _validate_file_binding(binding.get("bundle"), label="source predicate bundle")
    payload = _load_json(path, label="source predicate bundle")
    canonical = _require_sha256(
        binding.get("canonical_sha256"), "source predicate bundle canonical SHA256"
    )
    if (
        payload.get("canonical_sha256") != canonical
        or canonical != _document_sha256(payload, "canonical_sha256")
    ):
        raise OwnerBuyE3ParityAmendmentError("source predicate bundle canonical identity drifted")
    if expected is not None and (
        expected.path != path
        or expected.file_sha256 != binding["bundle"]["file_sha256"]
        or expected.canonical_sha256 != canonical
    ):
        raise OwnerBuyE3ParityAmendmentError("loaded source predicate bundle drifted")
    return payload


def _parity_source_binding() -> dict[str, str]:
    amendment_path = _regular_file(Path(__file__), label="Layer-4 amendment source", private=False)
    v1_path = _regular_file(Path(parity_v1.__file__), label="v1 parity source", private=False)
    return {
        "amendment_file_sha256": _file_sha256(amendment_path),
        "v1_parity_file_sha256": _file_sha256(v1_path),
    }


def freeze_layer4_lockstep_contract(
    *,
    output_path: Path,
    formal_buy_component_artifact_manifest_path: Path,
    owner_execution_manifest_path: Path,
    artifact: LoadedExactArtifact,
    mechanics_receipt_sha256: str,
    source_predicate_bundle: predicate_view.FrozenPredicateBundle,
    ordered_development_days: Sequence[str],
) -> Mapping[str, Any]:
    """Freeze the immutable Layer-4 v2 binding contract.

    The learning-algorithm SHA is intentionally absent from this signature.
    It is derived from the admitted formal BUY component manifest.
    """

    days = _ordered_days(ordered_development_days)
    _component, formal = _validate_formal_component_manifest(
        formal_buy_component_artifact_manifest_path
    )
    _attempt, execution = _validate_attempt2_manifest(owner_execution_manifest_path)
    artifact_bindings = {
        "artifact_manifest": _file_binding(artifact.manifest_path, label="artifact manifest"),
        "policy": _file_binding(artifact.policy_path, label="artifact policy"),
        "predicate_bundle": _file_binding(
            artifact.predicate_bundle_path, label="artifact predicate bundle"
        ),
        "artifact_sha256": _require_sha256(artifact.artifact_sha256, "exact artifact SHA256"),
    }
    _validate_artifact_documents(artifact, artifact_bindings, ordered_days=days)
    source_binding = {
        "bundle": _file_binding(source_predicate_bundle.path, label="source predicate bundle"),
        "canonical_sha256": _require_sha256(
            source_predicate_bundle.canonical_sha256,
            "source predicate bundle canonical SHA256",
        ),
    }
    _validate_source_predicate_bundle(source_binding, source_predicate_bundle)
    receipt: dict[str, Any] = {
        "schema_version": LAYER4_CONTRACT_SCHEMA,
        "schema_amendment": SCHEMA_AMENDMENT,
        "identity": IDENTITY,
        "status": "layer4_lockstep_contract_frozen",
        "formal_learning_algorithm": formal,
        "learning_algorithm_artifact_sha256": formal[
            "learning_algorithm_artifact_sha256"
        ],
        "execution_attempt": execution,
        "exact_artifact": artifact_bindings,
        "mechanics_receipt_sha256": _require_sha256(
            mechanics_receipt_sha256, "mechanics receipt SHA256"
        ),
        "source_predicate_bundle": source_binding,
        "parity_source": _parity_source_binding(),
        "ordered_development_days": list(days),
        "evidence_boundary": dict(_BOUNDARY),
        "permissions": dict(_PERMISSIONS),
    }
    receipt["canonical_contract_sha256"] = _document_sha256(
        receipt, "canonical_contract_sha256"
    )
    _validate_contract_payload(
        receipt,
        artifact=artifact,
        expected_mechanics_receipt_sha256=mechanics_receipt_sha256,
        source_predicate_bundle=source_predicate_bundle,
        ordered_development_days=days,
    )
    _atomic_write_json(output_path, receipt)
    return receipt


def _validate_contract_payload(
    payload: Mapping[str, Any],
    *,
    artifact: LoadedExactArtifact | None = None,
    expected_mechanics_receipt_sha256: str | None = None,
    source_predicate_bundle: predicate_view.FrozenPredicateBundle | None = None,
    ordered_development_days: Sequence[str] | None = None,
) -> dict[str, Any]:
    if (
        payload.get("schema_version") != LAYER4_CONTRACT_SCHEMA
        or payload.get("schema_amendment") != SCHEMA_AMENDMENT
        or payload.get("identity") != IDENTITY
        or payload.get("status") != "layer4_lockstep_contract_frozen"
        or payload.get("evidence_boundary") != _BOUNDARY
        or payload.get("permissions") != _PERMISSIONS
        or payload.get("canonical_contract_sha256")
        != _document_sha256(payload, "canonical_contract_sha256")
    ):
        raise OwnerBuyE3ParityAmendmentError("Layer-4 contract identity drifted")
    days = _ordered_days(payload.get("ordered_development_days", ()))
    if ordered_development_days is not None and days != _ordered_days(ordered_development_days):
        raise OwnerBuyE3ParityAmendmentError("Layer-4 contract Development days drifted")

    formal_binding = payload.get("formal_learning_algorithm")
    if not isinstance(formal_binding, Mapping):
        raise OwnerBuyE3ParityAmendmentError("formal learning algorithm binding is missing")
    component_path = _validate_file_binding(
        formal_binding.get("manifest"), label="formal v24 BUY component artifact manifest"
    )
    _component, derived_formal = _validate_formal_component_manifest(component_path)
    if dict(formal_binding) != derived_formal:
        raise OwnerBuyE3ParityAmendmentError("formal learning algorithm binding drifted")
    learning_sha = derived_formal["learning_algorithm_artifact_sha256"]
    if payload.get("learning_algorithm_artifact_sha256") != learning_sha:
        raise OwnerBuyE3ParityAmendmentError("learning algorithm SHA role drifted")

    execution_binding = payload.get("execution_attempt")
    if not isinstance(execution_binding, Mapping):
        raise OwnerBuyE3ParityAmendmentError("execution attempt binding is missing")
    attempt_path = _validate_file_binding(
        execution_binding.get("manifest"), label="owner attempt2 execution manifest"
    )
    _attempt, derived_execution = _validate_attempt2_manifest(attempt_path)
    if dict(execution_binding) != derived_execution:
        raise OwnerBuyE3ParityAmendmentError("execution attempt binding drifted")

    artifact_binding = payload.get("exact_artifact")
    if not isinstance(artifact_binding, Mapping):
        raise OwnerBuyE3ParityAmendmentError("exact artifact binding is missing")
    _validate_artifact_documents(artifact, artifact_binding, ordered_days=days)

    mechanics_sha = _require_sha256(
        payload.get("mechanics_receipt_sha256"), "mechanics receipt SHA256"
    )
    if expected_mechanics_receipt_sha256 is not None and mechanics_sha != _require_sha256(
        expected_mechanics_receipt_sha256, "expected mechanics receipt SHA256"
    ):
        raise OwnerBuyE3ParityAmendmentError("mechanics receipt SHA256 drifted")
    source_binding = payload.get("source_predicate_bundle")
    if not isinstance(source_binding, Mapping):
        raise OwnerBuyE3ParityAmendmentError("source predicate bundle binding is missing")
    _validate_source_predicate_bundle(source_binding, source_predicate_bundle)
    if payload.get("parity_source") != _parity_source_binding():
        raise OwnerBuyE3ParityAmendmentError("parity source bytes drifted")
    return dict(payload)


def validate_layer4_lockstep_contract(
    path: Path,
    *,
    artifact: LoadedExactArtifact | None = None,
    expected_mechanics_receipt_sha256: str | None = None,
    source_predicate_bundle: predicate_view.FrozenPredicateBundle | None = None,
    ordered_development_days: Sequence[str] | None = None,
) -> Mapping[str, Any]:
    contract_path = _regular_file(path, label="Layer-4 contract", private=True)
    payload = _load_json(contract_path, label="Layer-4 contract")
    return _validate_contract_payload(
        payload,
        artifact=artifact,
        expected_mechanics_receipt_sha256=expected_mechanics_receipt_sha256,
        source_predicate_bundle=source_predicate_bundle,
        ordered_development_days=ordered_development_days,
    )


def _normalize_lockstep_result(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _LOCKSTEP_RESULT_FIELDS:
        raise OwnerBuyE3ParityAmendmentError("Layer-4 day result shape drifted")
    result = dict(value)
    for field in (
        "summary_signature_sha256",
        "campaign_frame_sha256",
        "fill_frame_sha256",
        "decision_frame_sha256",
    ):
        result[field] = _require_sha256(result[field], f"Layer-4 {field}")
    for field in ("decision_count", "campaign_count", "fill_count", "mismatch_count"):
        number = result[field]
        if isinstance(number, bool) or not isinstance(number, int) or number < 0:
            raise OwnerBuyE3ParityAmendmentError(f"Layer-4 {field} is invalid")
    if result["mismatch_count"] != 0:
        raise OwnerBuyE3ParityAmendmentError("Layer-4 day mismatch is nonzero")
    return result


def _day_input_sha256(rows: pd.DataFrame, utc_day: str) -> str:
    if rows.empty or "day_input_sha256" not in rows:
        raise OwnerBuyE3ParityAmendmentError(f"{utc_day} day input identity is missing")
    values = tuple(sorted(set(rows["day_input_sha256"].astype(str))))
    if len(values) != 1:
        raise OwnerBuyE3ParityAmendmentError(f"{utc_day} day input identity drifted")
    return _require_sha256(values[0], f"{utc_day} day input SHA256")


def _day_receipt_payload(
    *,
    contract: Mapping[str, Any],
    contract_file_sha256: str,
    utc_day: str,
    day_input_sha256: str,
    result: Mapping[str, Any],
) -> dict[str, Any]:
    artifact = contract["exact_artifact"]
    source = contract["source_predicate_bundle"]
    parity_source = contract["parity_source"]
    receipt: dict[str, Any] = {
        "schema_version": LOCKSTEP_DAY_SCHEMA_V2,
        "schema_amendment": SCHEMA_AMENDMENT,
        "identity": IDENTITY,
        "status": "day_lockstep_complete",
        "utc_day": utc_day,
        "layer4_lockstep_contract_sha256": contract["canonical_contract_sha256"],
        "layer4_lockstep_contract_file_sha256": contract_file_sha256,
        "learning_algorithm_artifact_sha256": contract[
            "learning_algorithm_artifact_sha256"
        ],
        "artifact_sha256": artifact["artifact_sha256"],
        "artifact_manifest_file_sha256": artifact["artifact_manifest"]["file_sha256"],
        "policy_file_sha256": artifact["policy"]["file_sha256"],
        "predicate_bundle_file_sha256": artifact["predicate_bundle"]["file_sha256"],
        "mechanics_receipt_sha256": contract["mechanics_receipt_sha256"],
        "source_predicate_bundle_file_sha256": source["bundle"]["file_sha256"],
        "parity_source_file_sha256": parity_source["amendment_file_sha256"],
        "v1_parity_source_file_sha256": parity_source["v1_parity_file_sha256"],
        "day_input_sha256": day_input_sha256,
        "lockstep": _normalize_lockstep_result(result),
        "economic_values_materialized_by_replay": True,
        "evidence_boundary": dict(_BOUNDARY),
        "permissions": dict(_PERMISSIONS),
    }
    receipt["canonical_day_receipt_sha256"] = _document_sha256(
        receipt, "canonical_day_receipt_sha256"
    )
    return receipt


def _load_day_receipt(
    path: Path,
    *,
    contract: Mapping[str, Any],
    contract_file_sha256: str,
    utc_day: str,
    expected_day_input_sha256: str,
) -> dict[str, Any]:
    receipt = _load_json(path, label=f"{utc_day} Layer-4 day receipt")
    if receipt.get("schema_version") == parity_v1.LOCKSTEP_DAY_SCHEMA:
        raise OwnerBuyE3ParityAmendmentError("v1 Layer-4 day receipts are never reusable")
    expected = _day_receipt_payload(
        contract=contract,
        contract_file_sha256=contract_file_sha256,
        utc_day=utc_day,
        day_input_sha256=expected_day_input_sha256,
        result=receipt.get("lockstep", {}),
    )
    if receipt != expected:
        raise OwnerBuyE3ParityAmendmentError(f"{utc_day} Layer-4 day receipt drifted")
    return receipt


def _safe_day_root(path: Path, *, expected_days: Sequence[str]) -> Path:
    root = _absolute_path(path)
    _reject_symlink_chain(root, label="Layer-4 day receipt directory")
    root.mkdir(parents=True, exist_ok=True)
    _reject_symlink_chain(root, label="Layer-4 day receipt directory")
    if not root.is_dir():
        raise OwnerBuyE3ParityAmendmentError("Layer-4 day receipt root is not a directory")
    allowed = {f"{day}.json" for day in expected_days}
    observed = {entry.name for entry in root.iterdir()}
    if not observed.issubset(allowed):
        raise OwnerBuyE3ParityAmendmentError("unexpected Layer-4 day receipt file exists")
    return root


def run_repeated_policy_lockstep_parity_v2(
    artifact: LoadedExactArtifact,
    *,
    mechanics: Any,
    source_predicate_bundle: predicate_view.FrozenPredicateBundle,
    contract_path: Path,
    day_receipt_dir: Path,
    output_path: Path,
) -> Mapping[str, Any]:
    """Run or resume Layer 4 under the v2 receipt-binding amendment."""

    days = _ordered_days(tuple(mechanics.selected_days))
    contract_file = _regular_file(contract_path, label="Layer-4 contract", private=True)
    contract_file_sha = _file_sha256(contract_file)
    contract = validate_layer4_lockstep_contract(
        contract_file,
        artifact=artifact,
        expected_mechanics_receipt_sha256=mechanics.mechanics_receipt_sha256,
        source_predicate_bundle=source_predicate_bundle,
        ordered_development_days=days,
    )
    destination = _safe_output_path(output_path, label="Layer-4 final receipt")
    day_root = _safe_day_root(day_receipt_dir, expected_days=days)
    if "utc_day" not in mechanics.replay_inputs:
        raise OwnerBuyE3ParityAmendmentError("Layer-4 replay inputs lack utc_day")

    admitted: list[dict[str, Any]] = []
    portable_binding: Mapping[str, Any] | None = None
    with tempfile.TemporaryDirectory(prefix="f05-buy-e3-lockstep-v2-") as temporary:
        temporary_root = Path(temporary)
        for utc_day in days:
            rows = mechanics.replay_inputs.loc[
                mechanics.replay_inputs["utc_day"].astype(str) == utc_day
            ].copy()
            day_input_sha = _day_input_sha256(rows, utc_day)
            day_path = day_root / f"{utc_day}.json"
            if day_path.exists() or day_path.is_symlink():
                day_receipt = _load_day_receipt(
                    day_path,
                    contract=contract,
                    contract_file_sha256=contract_file_sha,
                    utc_day=utc_day,
                    expected_day_input_sha256=day_input_sha,
                )
            else:
                if portable_binding is None:
                    portable_binding = parity_v1.replay_adapter._resolve_execution_options(
                        mechanics.replay_inputs
                    ).binding
                result = parity_v1._run_lockstep_day(
                    artifact=artifact,
                    source_predicate_bundle=source_predicate_bundle,
                    learning_algorithm_artifact_sha256=contract[
                        "learning_algorithm_artifact_sha256"
                    ],
                    utc_day=utc_day,
                    rows=rows,
                    portable_binding=portable_binding,
                    temporary_root=temporary_root,
                )
                day_receipt = _day_receipt_payload(
                    contract=contract,
                    contract_file_sha256=contract_file_sha,
                    utc_day=utc_day,
                    day_input_sha256=day_input_sha,
                    result=result,
                )
                _atomic_write_json(day_path, day_receipt)
            admitted.append(
                {
                    "utc_day": utc_day,
                    "file_name": day_path.name,
                    "file_sha256": _file_sha256(day_path),
                    "canonical_day_receipt_sha256": day_receipt[
                        "canonical_day_receipt_sha256"
                    ],
                }
            )

    exact_artifact = contract["exact_artifact"]
    receipt: dict[str, Any] = {
        "schema_version": LAYER4_RECEIPT_SCHEMA_V2,
        "schema_amendment": SCHEMA_AMENDMENT,
        "identity": IDENTITY,
        "status": "parity_complete",
        "layer": LAYER4_LAYER,
        "layer4_lockstep_contract_sha256": contract["canonical_contract_sha256"],
        "layer4_lockstep_contract_file_sha256": contract_file_sha,
        "learning_algorithm_artifact_sha256": contract[
            "learning_algorithm_artifact_sha256"
        ],
        "artifact_sha256": exact_artifact["artifact_sha256"],
        "artifact_manifest_file_sha256": exact_artifact["artifact_manifest"]["file_sha256"],
        "policy_file_sha256": exact_artifact["policy"]["file_sha256"],
        "predicate_bundle_file_sha256": exact_artifact["predicate_bundle"]["file_sha256"],
        "evidence": {
            "day_count": len(days),
            "ordered_development_days": list(days),
            "day_receipts": admitted,
            "day_receipts_sha256": _canonical_sha256(admitted),
            "mechanics_receipt_sha256": contract["mechanics_receipt_sha256"],
            "source_predicate_bundle_file_sha256": contract[
                "source_predicate_bundle"
            ]["bundle"]["file_sha256"],
            "parity_source": dict(contract["parity_source"]),
            "mismatch_count": 0,
            "deadline_lockstep": True,
            "fill_lockstep": True,
            "campaign_lockstep": True,
        },
        "economic_values_materialized_by_replay": True,
        "evidence_boundary": dict(_BOUNDARY),
        "permissions": dict(_PERMISSIONS),
    }
    receipt["canonical_receipt_sha256"] = _document_sha256(
        receipt, "canonical_receipt_sha256"
    )
    _atomic_write_json(destination, receipt)
    return validate_layer4_receipt_v2(
        destination,
        contract_path=contract_file,
        day_receipt_dir=day_root,
        artifact=artifact,
        mechanics_receipt_sha256=mechanics.mechanics_receipt_sha256,
        source_predicate_bundle=source_predicate_bundle,
    )


def validate_layer4_receipt_v2(
    path: Path,
    *,
    contract_path: Path,
    day_receipt_dir: Path,
    artifact: LoadedExactArtifact | None = None,
    mechanics_receipt_sha256: str | None = None,
    source_predicate_bundle: predicate_view.FrozenPredicateBundle | None = None,
) -> Mapping[str, Any]:
    contract_file = _regular_file(contract_path, label="Layer-4 contract", private=True)
    contract_file_sha = _file_sha256(contract_file)
    contract = validate_layer4_lockstep_contract(
        contract_file,
        artifact=artifact,
        expected_mechanics_receipt_sha256=mechanics_receipt_sha256,
        source_predicate_bundle=source_predicate_bundle,
    )
    days = _ordered_days(contract["ordered_development_days"])
    root = _safe_day_root(day_receipt_dir, expected_days=days)
    expected_names = {f"{day}.json" for day in days}
    if {entry.name for entry in root.iterdir()} != expected_names:
        raise OwnerBuyE3ParityAmendmentError("Layer-4 day receipt set is incomplete")
    final = _load_json(path, label="Layer-4 final receipt")
    exact_artifact = contract["exact_artifact"]
    if (
        final.get("schema_version") != LAYER4_RECEIPT_SCHEMA_V2
        or final.get("schema_amendment") != SCHEMA_AMENDMENT
        or final.get("identity") != IDENTITY
        or final.get("status") != "parity_complete"
        or final.get("layer") != LAYER4_LAYER
        or final.get("layer4_lockstep_contract_sha256")
        != contract["canonical_contract_sha256"]
        or final.get("layer4_lockstep_contract_file_sha256") != contract_file_sha
        or final.get("learning_algorithm_artifact_sha256")
        != contract["learning_algorithm_artifact_sha256"]
        or final.get("artifact_sha256") != exact_artifact["artifact_sha256"]
        or final.get("artifact_manifest_file_sha256")
        != exact_artifact["artifact_manifest"]["file_sha256"]
        or final.get("policy_file_sha256") != exact_artifact["policy"]["file_sha256"]
        or final.get("predicate_bundle_file_sha256")
        != exact_artifact["predicate_bundle"]["file_sha256"]
        or final.get("economic_values_materialized_by_replay") is not True
        or final.get("evidence_boundary") != _BOUNDARY
        or final.get("permissions") != _PERMISSIONS
        or final.get("canonical_receipt_sha256")
        != _document_sha256(final, "canonical_receipt_sha256")
    ):
        raise OwnerBuyE3ParityAmendmentError("Layer-4 final receipt identity drifted")
    evidence = final.get("evidence")
    if not isinstance(evidence, Mapping):
        raise OwnerBuyE3ParityAmendmentError("Layer-4 final evidence is missing")
    admitted = evidence.get("day_receipts")
    if not isinstance(admitted, list) or [item.get("utc_day") for item in admitted] != list(days):
        raise OwnerBuyE3ParityAmendmentError("Layer-4 day receipt order drifted")
    validated: list[dict[str, Any]] = []
    for utc_day, binding in zip(days, admitted, strict=True):
        if not isinstance(binding, Mapping) or binding.get("file_name") != f"{utc_day}.json":
            raise OwnerBuyE3ParityAmendmentError("Layer-4 day receipt binding drifted")
        day_path = root / f"{utc_day}.json"
        raw = _load_json(day_path, label=f"{utc_day} Layer-4 day receipt")
        day_input_sha = _require_sha256(raw.get("day_input_sha256"), f"{utc_day} day input SHA256")
        day_receipt = _load_day_receipt(
            day_path,
            contract=contract,
            contract_file_sha256=contract_file_sha,
            utc_day=utc_day,
            expected_day_input_sha256=day_input_sha,
        )
        expected_binding = {
            "utc_day": utc_day,
            "file_name": day_path.name,
            "file_sha256": _file_sha256(day_path),
            "canonical_day_receipt_sha256": day_receipt[
                "canonical_day_receipt_sha256"
            ],
        }
        if dict(binding) != expected_binding:
            raise OwnerBuyE3ParityAmendmentError("Layer-4 day receipt file binding drifted")
        validated.append(expected_binding)
    if (
        evidence.get("day_count") != len(days)
        or evidence.get("ordered_development_days") != list(days)
        or evidence.get("day_receipts_sha256") != _canonical_sha256(validated)
        or evidence.get("mechanics_receipt_sha256")
        != contract["mechanics_receipt_sha256"]
        or evidence.get("source_predicate_bundle_file_sha256")
        != contract["source_predicate_bundle"]["bundle"]["file_sha256"]
        or evidence.get("parity_source") != contract["parity_source"]
        or evidence.get("mismatch_count") != 0
        or evidence.get("deadline_lockstep") is not True
        or evidence.get("fill_lockstep") is not True
        or evidence.get("campaign_lockstep") is not True
    ):
        raise OwnerBuyE3ParityAmendmentError("Layer-4 final evidence drifted")
    return final


def _artifact_from_cli(args: argparse.Namespace) -> LoadedExactArtifact:
    return load_exact_artifact(
        artifact_manifest_path=Path(args.artifact_manifest),
        artifact_manifest_file_sha256=args.artifact_manifest_file_sha256,
        expected_artifact_sha256=args.artifact_sha256,
        policy_path=Path(args.policy),
        policy_file_sha256=args.policy_file_sha256,
        predicate_bundle_path=Path(args.predicate_bundle),
        predicate_bundle_file_sha256=args.predicate_bundle_file_sha256,
    )


def _add_artifact_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--artifact-manifest", required=True)
    parser.add_argument("--artifact-manifest-file-sha256", required=True)
    parser.add_argument("--artifact-sha256", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--policy-file-sha256", required=True)
    parser.add_argument("--predicate-bundle", required=True)
    parser.add_argument("--predicate-bundle-file-sha256", required=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze-contract")
    _add_artifact_arguments(freeze)
    freeze.add_argument("--formal-buy-component-artifact-manifest", required=True)
    freeze.add_argument("--owner-execution-manifest", required=True)
    freeze.add_argument("--mechanics-receipt-sha256", required=True)
    freeze.add_argument("--source-predicate-bundle", required=True)
    freeze.add_argument("--source-predicate-bundle-file-sha256", required=True)
    freeze.add_argument("--development-day", action="append", required=True)
    freeze.add_argument("--output", required=True)
    validate = commands.add_parser("validate-contract")
    validate.add_argument("--contract", required=True)
    validate_final = commands.add_parser("validate-layer4")
    validate_final.add_argument("--contract", required=True)
    validate_final.add_argument("--day-receipt-dir", required=True)
    validate_final.add_argument("--receipt", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "freeze-contract":
        artifact = _artifact_from_cli(args)
        source_bundle = predicate_view.load_frozen_predicate_bundle(
            args.source_predicate_bundle,
            expected_file_sha256=args.source_predicate_bundle_file_sha256,
        )
        receipt = freeze_layer4_lockstep_contract(
            output_path=Path(args.output),
            formal_buy_component_artifact_manifest_path=Path(
                args.formal_buy_component_artifact_manifest
            ),
            owner_execution_manifest_path=Path(args.owner_execution_manifest),
            artifact=artifact,
            mechanics_receipt_sha256=args.mechanics_receipt_sha256,
            source_predicate_bundle=source_bundle,
            ordered_development_days=args.development_day,
        )
    elif args.command == "validate-contract":
        receipt = validate_layer4_lockstep_contract(Path(args.contract))
    else:
        receipt = validate_layer4_receipt_v2(
            Path(args.receipt),
            contract_path=Path(args.contract),
            day_receipt_dir=Path(args.day_receipt_dir),
        )
    print(json.dumps(receipt, sort_keys=True, ensure_ascii=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
