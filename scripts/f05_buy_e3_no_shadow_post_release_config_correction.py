#!/usr/bin/env python3
"""Freeze the no-shadow config pair against the exact direct-owner release-v3.

This additive receipt is portable evidence only.  It does not modify either
config, grant authority, connect to a market stream, or inspect economic,
Validation, or holdout data.  The producer is intentionally unusable until the
final release-v3 byte and runtime execution identities are frozen below.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final

import yaml

from scripts import f05_buy_e3_direct_owner_release_v3 as release_v3

OWNER: Final = "causal_multichannel_window_boolean_cooldown_owner_buy_e3_v1"
SCHEMA_VERSION: Final = f"{OWNER}.no_shadow_post_release_config_correction.v1"
STATUS: Final = "direct_owner_release_v3_and_no_shadow_config_pair_frozen"
CANONICAL_FIELD: Final = "canonical_config_correction_sha256"

# Filled once, from the reviewed immutable release-v3 receipt and annotated
# runtime tag.  Empty values make every authority entry point fail closed.
FROZEN_RELEASE_FILE_SHA256: Final = (
    "310d86d30bd875a7029b3e2f784877c6802ab7b05b0f639383e68bb81a458f49"
)
FROZEN_RELEASE_CANONICAL_SHA256: Final = (
    "81d4449301d29828162a7fb57f52c855803682a697639b6d9cfa2d38a2846b8f"
)
FROZEN_RELEASE_SIZE_BYTES: Final = 15_386
FROZEN_RUNTIME_EXECUTION: Final = {
    "execution_commit": "eacb6ccb1f4437d99d8385ba3f46ba6012f5c1de",
    "execution_tree": "0343bd5586b337385cf2aa0d7a643f5c32b0da77",
    "annotated_operational_tag": "f05-owner-buy-e3-no-shadow-runtime-v3-20260824",
    "annotated_operational_tag_object": "3878ea05252ef8f274b6f74ee7a984431c53b892",
    "tag_peeled_commit": "eacb6ccb1f4437d99d8385ba3f46ba6012f5c1de",
}
FROZEN_RUNTIME_SUPPLEMENT_BINDING: Final[dict[str, Any] | None] = {
    "schema_version": "f05_buy_e3_no_global_flow_shadow_runtime_fix_supplement.v1",
    "status": "runtime_no_shadow_fix_verified_no_e3_or_sell_semantic_change",
    "file_sha256": "4dc5a379e927380fe282d8dd5167291f3ca3caba3699dbf4457cedb5e3b4ebb7",
    "canonical_field": "canonical_supplement_sha256",
    "canonical_sha256": "bd157ac169d0158ce19c6caf8e4686faf4b47a8a44a8179039a7223d1484393e",
    "size_bytes": 11_880,
    "mode": "0600",
}

PARENT_DISABLED_SHA256: Final = release_v3.OLD_DISABLED_CONFIG_SHA256
PARENT_ACTIVE_SHA256: Final = release_v3.OLD_ACTIVE_CONFIG_SHA256
PARENT_DISABLED_SEMANTIC_SHA256: Final = (
    "0b967e222b33118926d61341addcbe4130ca015ac9ca51dbe7477384d6cddd6e"
)
PARENT_ACTIVE_SEMANTIC_SHA256: Final = (
    "ad4baa6f3c02e668ecb2ab8fb97a6d7f3736c56a6f84a827dd5eb2eb88ce7b10"
)
PARENT_DISABLED_SIZE: Final = 27367
PARENT_ACTIVE_SIZE: Final = 27366
CORRECTED_DISABLED_SHA256: Final = release_v3.NEW_DISABLED_CONFIG_SHA256
CORRECTED_ACTIVE_SHA256: Final = release_v3.NEW_ACTIVE_CONFIG_SHA256
CORRECTED_DISABLED_SIZE: Final = release_v3.NEW_DISABLED_CONFIG_SIZE
CORRECTED_ACTIVE_SIZE: Final = release_v3.NEW_ACTIVE_CONFIG_SIZE
CORRECTED_DISABLED_SEMANTIC_SHA256: Final = release_v3.NEW_DISABLED_CONFIG_SEMANTIC_SHA256
CORRECTED_ACTIVE_SEMANTIC_SHA256: Final = release_v3.NEW_ACTIVE_CONFIG_SEMANTIC_SHA256
ADDED_FALSE_PATHS: Final = tuple(release_v3.NEW_CONFIG_ADDITIONS)
ACTIVE_DISABLED_DIFFERENCE: Final = release_v3.CONFIG_PAIR_DIFFERENCE
REQUIRED_FALSE_PATHS: Final = tuple(release_v3.REQUIRED_FALSE_CONFIG_PATHS)

CONTENT_BINDING_FIELDS: Final = frozenset(
    {
        "schema_version",
        "status",
        "file_sha256",
        "canonical_field",
        "canonical_sha256",
        "size_bytes",
        "mode",
    }
)
CONFIG_BINDING_FIELDS: Final = frozenset({"file_sha256", "semantic_sha256", "size_bytes", "mode"})
COLLECTOR_EXECUTION_FIELDS: Final = frozenset(
    {
        "repository_root",
        "execution_commit",
        "execution_tree",
        "annotated_tag",
        "annotated_tag_object",
        "tag_peeled_commit",
        "direct_successor_commit_is_ancestor",
        "runtime_authority_checkout",
    }
)
AUTHORITY_DESIGN: Final = {
    "runtime_authority": "immutable_direct_owner_release_v3",
    "config_correction_is_evidence_only": True,
    "config_correction_grants_authority": False,
    "release_v3_does_not_depend_on_config_correction": True,
    "resource_and_active_receipts_must_bind_both": True,
}
PERMISSIONS: Final = {"research": False, "action": False, "live": False}
EVIDENCE_BOUNDARY: Final = {
    "economic_outcomes_read": False,
    "economic_values_persisted": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "new_economic_arm_run": False,
    "shadow_created": False,
    "companion_created": False,
    "shadow_or_companion_collection_enabled": False,
    "hypothetical_live_actions_scored": False,
}
TOP_LEVEL_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "collector_execution",
        "runtime_authority",
        "predecessor_config_pair",
        "corrected_config_pair",
        "semantic_diff",
        "authority_design",
        "permissions",
        "evidence_boundary",
        CANONICAL_FIELD,
    }
)
MAX_JSON_BYTES: Final = 16 << 20


class NoShadowConfigCorrectionError(RuntimeError):
    """Raised when post-release config evidence is incomplete or drifts."""


@dataclass(frozen=True)
class _Document:
    path: Path
    payload: dict[str, Any]
    raw: bytes
    metadata: os.stat_result


class _UniqueSafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueSafeLoader,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    output: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in output:
            raise NoShadowConfigCorrectionError(f"duplicate YAML key: {key}")
        output[key] = loader.construct_object(value_node, deep=deep)
    return output


_UniqueSafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _reject_duplicate_json(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise NoShadowConfigCorrectionError(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def _canonical(payload: Mapping[str, Any], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def document_sha256(payload: Mapping[str, Any], field: str) -> str:
    """Return the receipt's canonical SHA256 without mutating the document."""

    return _canonical(payload, field)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _open_regular(path: Path, label: str, *, mode: int = 0o600) -> tuple[Path, os.stat_result]:
    candidate = path.expanduser().absolute()
    if candidate.is_symlink() or not candidate.is_file():
        raise NoShadowConfigCorrectionError(f"{label} is not a regular file")
    target = candidate.resolve(strict=True)
    metadata = target.stat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != mode
        or metadata.st_size <= 0
        or metadata.st_size > MAX_JSON_BYTES
    ):
        raise NoShadowConfigCorrectionError(f"{label} has unsafe identity or permissions")
    return target, metadata


def _open_json(path: Path, label: str) -> _Document:
    target, before = _open_regular(path, label)
    try:
        raw = target.read_bytes()
        payload = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_json,
            parse_constant=lambda value: (_ for _ in ()).throw(
                NoShadowConfigCorrectionError(f"non-finite JSON value: {value}")
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise NoShadowConfigCorrectionError(f"{label} is unreadable JSON") from exc
    after = target.stat()
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise NoShadowConfigCorrectionError(f"{label} changed while read")
    if not isinstance(payload, dict):
        raise NoShadowConfigCorrectionError(f"{label} root is not an object")
    return _Document(target, payload, raw, before)


def _open_yaml(path: Path, label: str) -> tuple[dict[str, Any], dict[str, Any]]:
    target, metadata = _open_regular(path, label)
    try:
        raw = target.read_bytes()
        payload = yaml.load(raw.decode("utf-8"), Loader=_UniqueSafeLoader)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise NoShadowConfigCorrectionError(f"{label} is unreadable YAML") from exc
    if not isinstance(payload, dict):
        raise NoShadowConfigCorrectionError(f"{label} root is not a mapping")
    semantic = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return payload, {
        "file_sha256": hashlib.sha256(raw).hexdigest(),
        "semantic_sha256": semantic,
        "size_bytes": metadata.st_size,
        "mode": f"{stat.S_IMODE(metadata.st_mode):04o}",
    }


def _path_value(payload: Mapping[str, Any], dotted: str) -> Any:
    current: Any = payload
    for name in dotted.split("."):
        if not isinstance(current, Mapping) or name not in current:
            raise NoShadowConfigCorrectionError(f"config path is missing: {dotted}")
        current = current[name]
    return current


def _without_path(payload: Mapping[str, Any], dotted: str) -> dict[str, Any]:
    clone = json.loads(json.dumps(payload))
    names = dotted.split(".")
    current = clone
    for name in names[:-1]:
        child = current.get(name)
        if not isinstance(child, dict):
            raise NoShadowConfigCorrectionError(f"config path is missing: {dotted}")
        current = child
    if names[-1] not in current:
        raise NoShadowConfigCorrectionError(f"config path is missing: {dotted}")
    del current[names[-1]]
    return clone


def _release_key_paths(value: Any, prefix: str = "") -> list[str]:
    rows: list[str] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if "release" in str(key).lower():
                rows.append(path)
            rows.extend(_release_key_paths(child, path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            rows.extend(_release_key_paths(child, f"{prefix}[{index}]"))
    return rows


def _validate_pair(
    *,
    predecessor_disabled_path: Path,
    predecessor_active_path: Path,
    corrected_disabled_path: Path,
    corrected_active_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    old_disabled, old_disabled_binding = _open_yaml(
        predecessor_disabled_path, "predecessor disabled config"
    )
    old_active, old_active_binding = _open_yaml(
        predecessor_active_path, "predecessor active config"
    )
    new_disabled, new_disabled_binding = _open_yaml(
        corrected_disabled_path, "corrected disabled config"
    )
    new_active, new_active_binding = _open_yaml(corrected_active_path, "corrected active config")
    expected = (
        (
            old_disabled_binding,
            PARENT_DISABLED_SHA256,
            PARENT_DISABLED_SEMANTIC_SHA256,
            PARENT_DISABLED_SIZE,
        ),
        (
            old_active_binding,
            PARENT_ACTIVE_SHA256,
            PARENT_ACTIVE_SEMANTIC_SHA256,
            PARENT_ACTIVE_SIZE,
        ),
        (
            new_disabled_binding,
            CORRECTED_DISABLED_SHA256,
            CORRECTED_DISABLED_SEMANTIC_SHA256,
            CORRECTED_DISABLED_SIZE,
        ),
        (
            new_active_binding,
            CORRECTED_ACTIVE_SHA256,
            CORRECTED_ACTIVE_SEMANTIC_SHA256,
            CORRECTED_ACTIVE_SIZE,
        ),
    )
    for binding, file_sha, semantic_sha, size in expected:
        if (
            set(binding) != CONFIG_BINDING_FIELDS
            or binding["file_sha256"] != file_sha
            or (semantic_sha is not None and binding["semantic_sha256"] != semantic_sha)
            or (size is not None and binding["size_bytes"] != size)
            or binding["mode"] != "0600"
        ):
            raise NoShadowConfigCorrectionError("config byte or semantic identity drifted")
    if (
        _without_path(old_disabled, ACTIVE_DISABLED_DIFFERENCE)
        != _without_path(old_active, ACTIVE_DISABLED_DIFFERENCE)
        or _path_value(old_disabled, ACTIVE_DISABLED_DIFFERENCE) is not False
        or _path_value(old_active, ACTIVE_DISABLED_DIFFERENCE) is not True
        or _without_path(new_disabled, ACTIVE_DISABLED_DIFFERENCE)
        != _without_path(new_active, ACTIVE_DISABLED_DIFFERENCE)
        or _path_value(new_disabled, ACTIVE_DISABLED_DIFFERENCE) is not False
        or _path_value(new_active, ACTIVE_DISABLED_DIFFERENCE) is not True
    ):
        raise NoShadowConfigCorrectionError("active/disabled config pair drifted")
    for old, new in ((old_disabled, new_disabled), (old_active, new_active)):
        projected = json.loads(json.dumps(new))
        for dotted in ADDED_FALSE_PATHS:
            if _path_value(projected, dotted) is not False:
                raise NoShadowConfigCorrectionError(f"successor did not disable {dotted}")
            projected = _without_path(projected, dotted)
        if projected != old:
            raise NoShadowConfigCorrectionError("config changed outside two shadow additions")
    for config in (new_disabled, new_active):
        for dotted in REQUIRED_FALSE_PATHS:
            if _path_value(config, dotted) is not False:
                raise NoShadowConfigCorrectionError(f"required false config drifted: {dotted}")
        if _release_key_paths(config):
            raise NoShadowConfigCorrectionError("release authority field was added to YAML")
    return (
        {"disabled": old_disabled_binding, "active": old_active_binding},
        {"disabled": new_disabled_binding, "active": new_active_binding},
    )


def _require_sha(value: Any, label: str, *, length: int = 64) -> str:
    normalized = str(value)
    if len(normalized) != length or any(ch not in "0123456789abcdef" for ch in normalized):
        raise NoShadowConfigCorrectionError(f"{label} is not a lowercase hash")
    return normalized


def _frozen_authority_ready() -> None:
    for label, value, length in (
        ("release file", FROZEN_RELEASE_FILE_SHA256, 64),
        ("release canonical", FROZEN_RELEASE_CANONICAL_SHA256, 64),
        ("runtime commit", FROZEN_RUNTIME_EXECUTION.get("execution_commit"), 40),
        ("runtime tree", FROZEN_RUNTIME_EXECUTION.get("execution_tree"), 40),
        (
            "runtime tag object",
            FROZEN_RUNTIME_EXECUTION.get("annotated_operational_tag_object"),
            40,
        ),
    ):
        _require_sha(value, label, length=length)
    if (
        not isinstance(FROZEN_RELEASE_SIZE_BYTES, int)
        or isinstance(FROZEN_RELEASE_SIZE_BYTES, bool)
        or FROZEN_RELEASE_SIZE_BYTES <= 0
        or not FROZEN_RUNTIME_EXECUTION.get("annotated_operational_tag")
        or FROZEN_RUNTIME_EXECUTION.get("tag_peeled_commit")
        != FROZEN_RUNTIME_EXECUTION.get("execution_commit")
        or not isinstance(FROZEN_RUNTIME_SUPPLEMENT_BINDING, Mapping)
        or set(FROZEN_RUNTIME_SUPPLEMENT_BINDING) != CONTENT_BINDING_FIELDS
    ):
        raise NoShadowConfigCorrectionError("frozen release-v3 authority is incomplete")


def _release_binding(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    _frozen_authority_ready()
    opened = _open_json(path, "direct owner release-v3")
    payload = opened.payload
    canonical = _require_sha(payload.get(release_v3.CANONICAL_FIELD), "release canonical")
    binding = {
        "schema_version": release_v3.SCHEMA_VERSION,
        "status": release_v3.STATUS,
        "file_sha256": hashlib.sha256(opened.raw).hexdigest(),
        "canonical_field": release_v3.CANONICAL_FIELD,
        "canonical_sha256": canonical,
        "size_bytes": opened.metadata.st_size,
        "mode": f"{stat.S_IMODE(opened.metadata.st_mode):04o}",
    }
    if (
        set(payload) != release_v3.TOP_LEVEL_FIELDS
        or payload.get("schema_version") != release_v3.SCHEMA_VERSION
        or payload.get("identity") != release_v3.IDENTITY
        or payload.get("status") != release_v3.STATUS
        or binding["file_sha256"] != FROZEN_RELEASE_FILE_SHA256
        or binding["canonical_sha256"] != FROZEN_RELEASE_CANONICAL_SHA256
        or binding["size_bytes"] != FROZEN_RELEASE_SIZE_BYTES
        or binding["mode"] != "0600"
        or canonical != _canonical(payload, release_v3.CANONICAL_FIELD)
        or payload.get("execution") != FROZEN_RUNTIME_EXECUTION
        or payload.get("runtime_fix_supplement") != dict(FROZEN_RUNTIME_SUPPLEMENT_BINDING or {})
        or payload.get("no_shadow_runtime_contract") != release_v3.NO_SHADOW_RUNTIME_CONTRACT
        or payload.get("pending_current_runtime_evidence")
        != release_v3.PENDING_CURRENT_RUNTIME_EVIDENCE
        or payload.get("research_supported") is not False
        or payload.get("owner_risk_accepted") is not True
        or payload.get("action_authorized") is not True
        or payload.get("live_authorized") is not True
    ):
        raise NoShadowConfigCorrectionError("direct owner release-v3 identity drifted")
    return dict(payload), binding


def capture_collector_execution(root: Path, annotated_tag: str) -> dict[str, Any]:
    repository = root.expanduser().resolve(strict=True)
    if subprocess.run(
        ("git", "status", "--porcelain=v1", "--untracked-files=all"),
        cwd=repository,
        check=True,
        capture_output=True,
        timeout=20.0,
    ).stdout:
        raise NoShadowConfigCorrectionError("collector worktree is not clean")

    def git(*args: str) -> str:
        return (
            subprocess.run(
                ("git", *args),
                cwd=repository,
                check=True,
                capture_output=True,
                timeout=20.0,
            )
            .stdout.decode("ascii")
            .strip()
        )

    commit = git("rev-parse", "HEAD")
    tree = git("rev-parse", "HEAD^{tree}")
    tag_object = git("rev-parse", f"refs/tags/{annotated_tag}^{{tag}}")
    peeled = git("rev-parse", f"refs/tags/{annotated_tag}^{{commit}}")
    ancestor = (
        subprocess.run(
            (
                "git",
                "merge-base",
                "--is-ancestor",
                str(FROZEN_RUNTIME_EXECUTION["execution_commit"]),
                commit,
            ),
            cwd=repository,
            check=False,
            capture_output=True,
            timeout=20.0,
        ).returncode
        == 0
    )
    if peeled != commit:
        raise NoShadowConfigCorrectionError("collector annotated tag does not peel to HEAD")
    return {
        "repository_root": str(repository),
        "execution_commit": _require_sha(commit, "collector commit", length=40),
        "execution_tree": _require_sha(tree, "collector tree", length=40),
        "annotated_tag": annotated_tag,
        "annotated_tag_object": _require_sha(tag_object, "collector tag object", length=40),
        "tag_peeled_commit": _require_sha(peeled, "collector peeled commit", length=40),
        "direct_successor_commit_is_ancestor": ancestor,
        "runtime_authority_checkout": False,
    }


def build_receipt(
    *,
    collector_repository_root: Path,
    collector_annotated_tag: str,
    direct_release_v3_path: Path,
    predecessor_disabled_config_path: Path,
    predecessor_active_config_path: Path,
    corrected_disabled_config_path: Path,
    corrected_active_config_path: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    _release, release_binding = _release_binding(direct_release_v3_path)
    predecessor, corrected = _validate_pair(
        predecessor_disabled_path=predecessor_disabled_config_path,
        predecessor_active_path=predecessor_active_config_path,
        corrected_disabled_path=corrected_disabled_config_path,
        corrected_active_path=corrected_active_config_path,
    )
    execution = capture_collector_execution(collector_repository_root, collector_annotated_tag)
    timestamp = generated_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    if not timestamp.endswith("Z"):
        raise NoShadowConfigCorrectionError("receipt timestamp is not UTC Z")
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": OWNER,
        "status": STATUS,
        "generated_utc": timestamp,
        "collector_execution": execution,
        "runtime_authority": release_binding,
        "predecessor_config_pair": predecessor,
        "corrected_config_pair": corrected,
        "semantic_diff": {
            "added_false_paths": list(ADDED_FALSE_PATHS),
            "active_disabled_only_difference": ACTIVE_DISABLED_DIFFERENCE,
            "required_false_paths": list(REQUIRED_FALSE_PATHS),
            "release_fields_present_in_yaml": False,
            "release_authority_injected_by_environment": True,
            "e3_artifact_decision_quote_and_sell_semantics_unchanged": True,
        },
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(PERMISSIONS),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[CANONICAL_FIELD] = _canonical(payload, CANONICAL_FIELD)
    return payload


def _content_binding(document: _Document) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": STATUS,
        "file_sha256": hashlib.sha256(document.raw).hexdigest(),
        "canonical_field": CANONICAL_FIELD,
        "canonical_sha256": document.payload[CANONICAL_FIELD],
        "size_bytes": document.metadata.st_size,
        "mode": f"{stat.S_IMODE(document.metadata.st_mode):04o}",
    }


def validate_content_receipt(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate portable bytes without reopening runtime/config path provenance."""
    _frozen_authority_ready()
    opened = _open_json(path, "post-release config correction")
    payload = opened.payload
    runtime_authority = payload.get("runtime_authority")
    predecessor = payload.get("predecessor_config_pair")
    corrected = payload.get("corrected_config_pair")
    execution = payload.get("collector_execution")
    if (
        set(payload) != TOP_LEVEL_FIELDS
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("identity") != OWNER
        or payload.get("status") != STATUS
        or not isinstance(execution, Mapping)
        or set(execution) != COLLECTOR_EXECUTION_FIELDS
        or not PurePosixPath(str(execution.get("repository_root", ""))).is_absolute()
        or execution.get("runtime_authority_checkout") is not False
        or not isinstance(execution.get("direct_successor_commit_is_ancestor"), bool)
        or not isinstance(runtime_authority, Mapping)
        or set(runtime_authority) != CONTENT_BINDING_FIELDS
        or runtime_authority.get("schema_version") != release_v3.SCHEMA_VERSION
        or runtime_authority.get("status") != release_v3.STATUS
        or runtime_authority.get("file_sha256") != FROZEN_RELEASE_FILE_SHA256
        or runtime_authority.get("canonical_field") != release_v3.CANONICAL_FIELD
        or runtime_authority.get("canonical_sha256") != FROZEN_RELEASE_CANONICAL_SHA256
        or runtime_authority.get("size_bytes") != FROZEN_RELEASE_SIZE_BYTES
        or runtime_authority.get("mode") != "0600"
        or not isinstance(predecessor, Mapping)
        or set(predecessor) != {"disabled", "active"}
        or not isinstance(corrected, Mapping)
        or set(corrected) != {"disabled", "active"}
        or any(
            not isinstance(pair.get(role), Mapping) or set(pair[role]) != CONFIG_BINDING_FIELDS
            for pair in (predecessor, corrected)
            for role in ("disabled", "active")
        )
        or predecessor["disabled"]["file_sha256"] != PARENT_DISABLED_SHA256
        or predecessor["active"]["file_sha256"] != PARENT_ACTIVE_SHA256
        or predecessor["disabled"]["semantic_sha256"] != PARENT_DISABLED_SEMANTIC_SHA256
        or predecessor["active"]["semantic_sha256"] != PARENT_ACTIVE_SEMANTIC_SHA256
        or predecessor["disabled"]["size_bytes"] != PARENT_DISABLED_SIZE
        or predecessor["active"]["size_bytes"] != PARENT_ACTIVE_SIZE
        or corrected["disabled"]["file_sha256"] != CORRECTED_DISABLED_SHA256
        or corrected["active"]["file_sha256"] != CORRECTED_ACTIVE_SHA256
        or corrected["disabled"]["semantic_sha256"] != CORRECTED_DISABLED_SEMANTIC_SHA256
        or corrected["active"]["semantic_sha256"] != CORRECTED_ACTIVE_SEMANTIC_SHA256
        or corrected["disabled"]["size_bytes"] != CORRECTED_DISABLED_SIZE
        or corrected["active"]["size_bytes"] != CORRECTED_ACTIVE_SIZE
        or any(
            pair[role]["mode"] != "0600"
            for pair in (predecessor, corrected)
            for role in ("disabled", "active")
        )
        or payload.get("semantic_diff")
        != {
            "added_false_paths": list(ADDED_FALSE_PATHS),
            "active_disabled_only_difference": ACTIVE_DISABLED_DIFFERENCE,
            "required_false_paths": list(REQUIRED_FALSE_PATHS),
            "release_fields_present_in_yaml": False,
            "release_authority_injected_by_environment": True,
            "e3_artifact_decision_quote_and_sell_semantics_unchanged": True,
        }
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or payload.get("permissions") != PERMISSIONS
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get(CANONICAL_FIELD) != _canonical(payload, CANONICAL_FIELD)
    ):
        raise NoShadowConfigCorrectionError("config correction identity drifted")
    return dict(payload), _content_binding(opened)


def _write_exclusive(path: Path, payload: Mapping[str, Any]) -> str:
    target = path.expanduser().absolute()
    target.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8")
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        target.unlink(missing_ok=True)
        raise
    return _file_sha256(target)


def finalize_receipt(*, output_path: Path, **kwargs: Any) -> tuple[dict[str, Any], str]:
    payload = build_receipt(**kwargs)
    file_sha = _write_exclusive(output_path, payload)
    observed, _binding = validate_content_receipt(output_path)
    if observed != payload:
        raise NoShadowConfigCorrectionError("written config correction changed")
    return payload, file_sha


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    finalize = subparsers.add_parser("finalize")
    finalize.add_argument("--collector-repository-root", type=Path, required=True)
    finalize.add_argument("--collector-annotated-tag", required=True)
    finalize.add_argument("--direct-release-v3", type=Path, required=True)
    finalize.add_argument("--predecessor-disabled-config", type=Path, required=True)
    finalize.add_argument("--predecessor-active-config", type=Path, required=True)
    finalize.add_argument("--corrected-disabled-config", type=Path, required=True)
    finalize.add_argument("--corrected-active-config", type=Path, required=True)
    finalize.add_argument("--output", type=Path, required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--receipt", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "finalize":
        payload, file_sha = finalize_receipt(
            collector_repository_root=args.collector_repository_root,
            collector_annotated_tag=args.collector_annotated_tag,
            direct_release_v3_path=args.direct_release_v3,
            predecessor_disabled_config_path=args.predecessor_disabled_config,
            predecessor_active_config_path=args.predecessor_active_config,
            corrected_disabled_config_path=args.corrected_disabled_config,
            corrected_active_config_path=args.corrected_active_config,
            output_path=args.output,
        )
    else:
        payload, _binding = validate_content_receipt(args.receipt)
        file_sha = _file_sha256(args.receipt)
    print(
        json.dumps(
            {
                "schema_version": payload["schema_version"],
                "status": payload["status"],
                "file_sha256": file_sha,
                "canonical_sha256": payload[CANONICAL_FIELD],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
