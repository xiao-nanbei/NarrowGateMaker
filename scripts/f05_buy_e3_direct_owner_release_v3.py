#!/usr/bin/env python3
"""Freeze the exact BUY E3 owner release for the no-shadow runtime successor.

This is an additive operational authority successor.  It inherits the exact
direct-v4 release-v2 authority and artifact, binds the runtime-only no-shadow
repair, and keeps all new resource/active/transport/lifecycle evidence pending.
It never reads economic, Validation, or holdout evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import math
import os
import stat
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import yaml

try:
    from scripts import f05_buy_e3_active_release as legacy_release
    from scripts import f05_buy_e3_direct_owner_release as artifact_release
    from scripts import f05_buy_e3_direct_owner_release_v2 as release_v2
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    import f05_buy_e3_active_release as legacy_release
    import f05_buy_e3_direct_owner_release as artifact_release
    import f05_buy_e3_direct_owner_release_v2 as release_v2


SCHEMA_VERSION: Final = (
    "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
    "direct_owner_active_release.v3"
)
IDENTITY: Final = SCHEMA_VERSION
STATUS: Final = "owner_authorized_direct_live_no_shadow_runtime_pending_evidence"
CANONICAL_FIELD: Final = "canonical_active_release_sha256"

EXACT_ARTIFACT_SHA256: Final = (
    "17e99df737157c6587602e6b496eadbecbed0a98d025da1d1db4cc8ef670786d"
)
PARENT_EXECUTION: Final = {
    "execution_commit": "07ef93733a3a685caba945c7761a48473e403072",
    "execution_tree": "ff505cd81a8eb11f2087d2ae27e7986fd99b0444",
    "annotated_operational_tag": "f05-owner-buy-e3-direct-live-v4-20260824",
    "annotated_operational_tag_object": "da83fa0b4aed00e4d04ea3faa212b2fb27a81f0d",
    "tag_peeled_commit": "07ef93733a3a685caba945c7761a48473e403072",
}
PARENT_RELEASE_V2_BINDING: Final = {
    "schema_version": release_v2.SCHEMA_VERSION,
    "status": release_v2.STATUS,
    "file_sha256": "ff888f4b5973563275c2b97e1554d45c9d686ef15d686440bf096521aab17fc2",
    "canonical_field": CANONICAL_FIELD,
    "canonical_sha256": "823ca1e4d53e968eb0afc53c4d2cad99cc17aac696548baa1700e800a4579702",
    "size_bytes": 7757,
    "mode": "0600",
}
PARENT_LIFECYCLE_SUPPLEMENT: Final = {
    "schema_version": "f05_buy_e3_lifecycle_reject_fix_supplement.v1",
    "status": "lifecycle_only_runtime_fix_verified_no_economic_change",
    "file_sha256": "c7a83f37f679ab94f7c0c670d53a43d894295d94cc74927e3a83fd3313336e87",
    "canonical_field": "canonical_supplement_sha256",
    "canonical_sha256": "e69c4edb2025937a8569cbedd3163f3ec3b953a17fc904218e4df332dc1f221d",
    "size_bytes": 43428,
    "mode": "0600",
}

OLD_DISABLED_CONFIG_SHA256: Final = (
    "10158a92177cd87b77fdb24a2a477dcab4b41cfb29208cf96c19953edafe166f"
)
OLD_ACTIVE_CONFIG_SHA256: Final = (
    "ad153012b14e725a3ac24f0ddbe02bc353168a13ec827b777cc94761020524ec"
)
NEW_DISABLED_CONFIG_SHA256: Final = (
    "d92fdec7ce89586f56fb1a6c80a6bc6fbe96b50023bd8c481cae730606c75204"
)
NEW_ACTIVE_CONFIG_SHA256: Final = (
    "3d8463c47c1cc2ff2017c9f6e7a963c77a8edb0cc692c48d89b03ee09bff772e"
)
NEW_DISABLED_CONFIG_SIZE: Final = 27444
NEW_ACTIVE_CONFIG_SIZE: Final = 27443
NEW_DISABLED_CONFIG_SEMANTIC_SHA256: Final = (
    "3e8f1c6b829f88ce250896e7ff810c22d3c9102bc4c10f8d9ead883facedc2a8"
)
NEW_ACTIVE_CONFIG_SEMANTIC_SHA256: Final = (
    "f5cb3d238edfdff4c2353e2d7cd5c2d948d36ef0be1e7733ce0dda73ea7a21c2"
)
NEW_CONFIG_ADDITIONS: Final = (
    "multi_market.global_flow_shadow_enabled",
    "multi_market.global_reference_shadow_enabled",
)
CONFIG_PAIR_DIFFERENCE: Final = "strategy.buy_e3_cooldown_policy_enabled"
REQUIRED_FALSE_CONFIG_PATHS: Final = (
    "external_venues.enabled",
    "multi_market.global_flow_shadow_enabled",
    "multi_market.global_reference_shadow_enabled",
    "strategy.buy_fill_selection_shadow_enabled",
    "strategy.dynamic_fill_hazard_shadow_enabled",
    "strategy.cross_venue_fair_price_shadow_enabled",
    "depth_execution.shadow_enabled",
    "logging.inventory_campaign_shadow_enabled",
    "logging.market_tape_enabled",
)

RUNTIME_SUPPLEMENT_SCHEMA: Final = (
    "f05_buy_e3_no_global_flow_shadow_runtime_fix_supplement.v1"
)
RUNTIME_SUPPLEMENT_STATUS: Final = (
    "runtime_no_shadow_fix_verified_no_e3_or_sell_semantic_change"
)
RUNTIME_SUPPLEMENT_CANONICAL_FIELD: Final = "canonical_supplement_sha256"
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
FILE_BINDING_FIELDS: Final = frozenset(
    {"file_sha256", "semantic_sha256", "size_bytes", "mode"}
)
CHANGED_FILE_BINDING_FIELDS: Final = frozenset({"git_blob_sha1", "file_sha256"})

AUTHORIZATION_BASIS: Final = {
    "authority": "explicit_owner_directive",
    "directive_id": "deploy_exact_buy_e3_without_shadow_or_companion_collection_20260824",
    "authority_inherited_from_parent_release_v2": True,
    "owner_risk_scope_unchanged": True,
    "no_shadow_runtime_fix_only": True,
    "does_not_relabel_research_evidence": True,
}
SCOPE: Final = dict(release_v2.SCOPE)
ROLLBACK: Final = dict(release_v2.ROLLBACK)
HISTORICAL_EVIDENCE: Final = {
    "historical_evidence_state": dict(release_v2.HISTORICAL_EVIDENCE_STATE),
    "historical_attempt4_anchor": dict(release_v2.HISTORICAL_ATTEMPT4_ANCHOR),
    "exact_v5_recovery": dict(release_v2.EXACT_V5_RECOVERY),
    "direct_v4_runtime_evidence_historical_only": True,
    "config_only_v5_v6_v7_attempts_historical_only": True,
    "old_runtime_evidence_reused_for_new_runtime_admission": False,
    "research_supported": False,
}
PENDING_CURRENT_RUNTIME_EVIDENCE: Final = {
    "resource_gate_complete": False,
    "active_capture_complete": False,
    "cross_host_admission_complete": False,
    "lifecycle_orico_admission_complete": False,
    "final_evidence_composition_complete": False,
}
RUNTIME_FIX_CONTRACT: Final = {
    "parent_execution_commit": PARENT_EXECUTION["execution_commit"],
    "e3_artifact_unchanged": True,
    "e3_decision_ast_unchanged": True,
    "buy_action_vocabulary": [
        "CONTROL_85N",
        "FIXED_79S",
        "FIXED_173S",
        "FIXED_223S",
        "FIXED_356S",
        "FIXED_640S",
        "FIXED_709S",
        "FIXED_2048S",
    ],
    "quote_price_and_size_semantics_unchanged": True,
    "sell_owner_policy_unchanged": True,
    "sell_runtime_semantics_unchanged": True,
    "new_strategy_arm_created": False,
    "lifecycle_only_v2_contract_reused": False,
    "no_authority_widening": True,
}
NO_SHADOW_RUNTIME_CONTRACT: Final = {
    "external_venues_enabled": False,
    "global_flow_shadow_enabled": False,
    "global_reference_shadow_enabled": False,
    "global_flow_evaluator_effective": False,
    "global_reference_evaluator_effective": False,
    "shadow_or_companion_collection_authorized": False,
    "shadow_or_companion_collection_active": False,
    "external_source_entries_inert": True,
    "release_binding_source": "restart_only_environment_authority",
    "release_fields_present_in_yaml": False,
}
EVIDENCE_BOUNDARY: Final = {
    "old_oof_applies_to_learning_algorithm_only": True,
    "exact_artifact_oof_available": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "shadow_created": False,
    "companion_created": False,
    "shadow_or_companion_collection_enabled": False,
    "new_economic_arm_run": False,
    "economic_values_read": False,
}
TOP_LEVEL_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "research_supported",
        "formal_hierarchy_passed",
        "formal_hard_gates_passed",
        "owner_risk_accepted",
        "outcome_informed_owner_override",
        "action_authorized",
        "live_authorized",
        "authorization_basis",
        "scope",
        "execution",
        "parent_runtime_authority",
        "exact_artifact",
        "historical_evidence",
        "config_pair",
        "runtime_fix_contract",
        "runtime_fix_supplement",
        "changed_repository_files",
        "no_shadow_runtime_contract",
        "pending_current_runtime_evidence",
        "rollback",
        "evidence_boundary",
        CANONICAL_FIELD,
    }
)


class DirectOwnerReleaseV3Error(RuntimeError):
    """Raised when the no-shadow owner release fails closed."""


@dataclass(frozen=True)
class _ConfigDocument:
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
            raise DirectOwnerReleaseV3Error(f"duplicate YAML key: {key}")
        output[key] = loader.construct_object(value_node, deep=deep)
    return output


_UniqueSafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _reject_non_finite(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise DirectOwnerReleaseV3Error("config contains a non-finite number")
    if isinstance(value, Mapping):
        for child in value.values():
            _reject_non_finite(child)
    elif isinstance(value, list):
        for child in value:
            _reject_non_finite(child)


def _open_config(path: Path, label: str) -> _ConfigDocument:
    candidate = path.expanduser().absolute()
    legacy_release._reject_symlink_components(candidate, label)  # noqa: SLF001
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise DirectOwnerReleaseV3Error(f"{label} could not be opened safely") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
        ):
            raise DirectOwnerReleaseV3Error(
                f"{label} is not a private 0600 single-link file"
            )
        raw = legacy_release._read_all(descriptor, before.st_size, label)  # noqa: SLF001
        after = os.fstat(descriptor)
        if (
            not legacy_release._same_file_state(before, after)  # noqa: SLF001
            or len(raw) != before.st_size
        ):
            raise DirectOwnerReleaseV3Error(f"{label} changed while read")
    finally:
        os.close(descriptor)
    try:
        legacy_release._reject_symlink_components(candidate, label)  # noqa: SLF001
        path_after = candidate.lstat()
    except FileNotFoundError as exc:
        raise DirectOwnerReleaseV3Error(f"{label} disappeared while read") from exc
    if not legacy_release._same_file_state(after, path_after):  # noqa: SLF001
        raise DirectOwnerReleaseV3Error(f"{label} path was replaced while read")
    try:
        payload = yaml.load(raw.decode("utf-8"), Loader=_UniqueSafeLoader)
    except (UnicodeError, yaml.YAMLError) as exc:
        raise DirectOwnerReleaseV3Error(f"{label} is unreadable YAML") from exc
    if not isinstance(payload, dict):
        raise DirectOwnerReleaseV3Error(f"{label} root is not a mapping")
    _reject_non_finite(payload)
    return _ConfigDocument(payload=payload, raw=raw, metadata=after)


def _leaf_diff(left: Any, right: Any, prefix: str = "") -> list[str]:
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        result: list[str] = []
        for key in sorted(set(left) | set(right)):
            path = f"{prefix}.{key}" if prefix else str(key)
            if key not in left or key not in right:
                result.append(path)
            else:
                result.extend(_leaf_diff(left[key], right[key], path))
        return result
    if isinstance(left, list) and isinstance(right, list):
        result = []
        for index in range(max(len(left), len(right))):
            path = f"{prefix}[{index}]"
            if index >= len(left) or index >= len(right):
                result.append(path)
            else:
                result.extend(_leaf_diff(left[index], right[index], path))
        return result
    return [] if left == right else [prefix]


def _path_value(payload: Mapping[str, Any], path: str) -> Any:
    value: Any = payload
    for component in path.split("."):
        if not isinstance(value, Mapping) or component not in value:
            raise DirectOwnerReleaseV3Error(f"config field missing: {path}")
        value = value[component]
    return value


def _reject_yaml_release_fields(payload: Mapping[str, Any], prefix: str = "") -> None:
    for key, value in payload.items():
        name = str(key)
        path = f"{prefix}.{name}" if prefix else name
        lowered = name.lower()
        if "buy_e3" in lowered and "release" in lowered:
            raise DirectOwnerReleaseV3Error(
                f"BUY E3 release authority must not be embedded in YAML: {path}"
            )
        if isinstance(value, Mapping):
            _reject_yaml_release_fields(value, path)


def _config_file_binding(document: _ConfigDocument) -> dict[str, Any]:
    return {
        "file_sha256": hashlib.sha256(document.raw).hexdigest(),
        "semantic_sha256": legacy_release.canonical_sha256(document.payload),
        "size_bytes": len(document.raw),
        "mode": "0600",
    }


def _config_pair(
    *,
    old_disabled_path: Path,
    old_active_path: Path,
    disabled_path: Path,
    active_path: Path,
) -> dict[str, Any]:
    old_disabled = _open_config(old_disabled_path, "predecessor disabled config")
    old_active = _open_config(old_active_path, "predecessor active config")
    disabled = _open_config(disabled_path, "no-shadow disabled config")
    active = _open_config(active_path, "no-shadow active config")
    hashes = {
        "old_disabled": hashlib.sha256(old_disabled.raw).hexdigest(),
        "old_active": hashlib.sha256(old_active.raw).hexdigest(),
        "disabled": hashlib.sha256(disabled.raw).hexdigest(),
        "active": hashlib.sha256(active.raw).hexdigest(),
    }
    expected = {
        "old_disabled": OLD_DISABLED_CONFIG_SHA256,
        "old_active": OLD_ACTIVE_CONFIG_SHA256,
        "disabled": NEW_DISABLED_CONFIG_SHA256,
        "active": NEW_ACTIVE_CONFIG_SHA256,
    }
    if hashes != expected:
        raise DirectOwnerReleaseV3Error("no-shadow config file identity drifted")
    if len(disabled.raw) != NEW_DISABLED_CONFIG_SIZE or len(active.raw) != NEW_ACTIVE_CONFIG_SIZE:
        raise DirectOwnerReleaseV3Error("no-shadow config file size drifted")
    for document in (old_disabled, old_active, disabled, active):
        _reject_yaml_release_fields(document.payload)
    if _leaf_diff(old_disabled.payload, disabled.payload) != list(NEW_CONFIG_ADDITIONS):
        raise DirectOwnerReleaseV3Error("disabled config changed outside the two shadow flags")
    if _leaf_diff(old_active.payload, active.payload) != list(NEW_CONFIG_ADDITIONS):
        raise DirectOwnerReleaseV3Error("active config changed outside the two shadow flags")
    if _leaf_diff(disabled.payload, active.payload) != [CONFIG_PAIR_DIFFERENCE]:
        raise DirectOwnerReleaseV3Error("no-shadow config pair differs outside BUY E3 enablement")
    if _path_value(disabled.payload, CONFIG_PAIR_DIFFERENCE) is not False:
        raise DirectOwnerReleaseV3Error("disabled config does not select B0")
    if _path_value(active.payload, CONFIG_PAIR_DIFFERENCE) is not True:
        raise DirectOwnerReleaseV3Error("active config does not enable exact BUY E3")
    for label, document in (("disabled", disabled), ("active", active)):
        for path in REQUIRED_FALSE_CONFIG_PATHS:
            if _path_value(document.payload, path) is not False:
                raise DirectOwnerReleaseV3Error(f"{label} config did not disable {path}")
        if _path_value(document.payload, "external_venues.shadow_only") is not True:
            raise DirectOwnerReleaseV3Error("external shadow-only marker drifted")
    disabled_binding = _config_file_binding(disabled)
    active_binding = _config_file_binding(active)
    if (
        disabled_binding["semantic_sha256"] != NEW_DISABLED_CONFIG_SEMANTIC_SHA256
        or active_binding["semantic_sha256"] != NEW_ACTIVE_CONFIG_SEMANTIC_SHA256
    ):
        raise DirectOwnerReleaseV3Error("no-shadow config semantic SHA256 drifted")
    return {
        "schema_version": "f05_buy_e3_no_shadow_config_pair.v1",
        "status": "exact_no_shadow_config_pair_frozen",
        "predecessor": {
            "disabled_file_sha256": OLD_DISABLED_CONFIG_SHA256,
            "active_file_sha256": OLD_ACTIVE_CONFIG_SHA256,
        },
        "disabled": disabled_binding,
        "active": active_binding,
        "old_to_new_semantic_additions": list(NEW_CONFIG_ADDITIONS),
        "active_disabled_only_difference": CONFIG_PAIR_DIFFERENCE,
        "required_false_paths": list(REQUIRED_FALSE_CONFIG_PATHS),
        "external_shadow_only_marker_inert": True,
        "release_fields_present_in_yaml": False,
    }


def _parent_release(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    document = legacy_release._open_document(path, "direct-v4 release-v2")  # noqa: SLF001
    payload = document.payload
    binding = {
        "schema_version": payload.get("schema_version"),
        "status": payload.get("status"),
        "file_sha256": hashlib.sha256(document.raw).hexdigest(),
        "canonical_field": CANONICAL_FIELD,
        "canonical_sha256": payload.get(CANONICAL_FIELD),
        "size_bytes": len(document.raw),
        "mode": "0600",
    }
    if binding != PARENT_RELEASE_V2_BINDING:
        raise DirectOwnerReleaseV3Error("direct-v4 release-v2 identity drifted")
    if set(payload) != release_v2.TOP_LEVEL_FIELDS:
        raise DirectOwnerReleaseV3Error("direct-v4 release-v2 fields drifted")
    if legacy_release.document_sha256(payload, CANONICAL_FIELD) != binding["canonical_sha256"]:
        raise DirectOwnerReleaseV3Error("direct-v4 release-v2 canonical drifted")
    fixed = {
        "execution": PARENT_EXECUTION,
        "historical_evidence_state": release_v2.HISTORICAL_EVIDENCE_STATE,
        "historical_attempt4_anchor": release_v2.HISTORICAL_ATTEMPT4_ANCHOR,
        "exact_v5_recovery": release_v2.EXACT_V5_RECOVERY,
        "lifecycle_fix_contract": release_v2.LIFECYCLE_FIX_CONTRACT,
        "lifecycle_fix_supplement": PARENT_LIFECYCLE_SUPPLEMENT,
        "scope": SCOPE,
        "rollback": ROLLBACK,
        "evidence_boundary": release_v2.EVIDENCE_BOUNDARY,
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "outcome_informed_owner_override": True,
        "action_authorized": True,
        "live_authorized": True,
    }
    if any(payload.get(key) != value for key, value in fixed.items()):
        raise DirectOwnerReleaseV3Error("direct-v4 release-v2 authority drifted")
    exact_artifact = payload.get("exact_artifact")
    if not isinstance(exact_artifact, Mapping) or exact_artifact.get(
        "artifact_sha256"
    ) != EXACT_ARTIFACT_SHA256:
        raise DirectOwnerReleaseV3Error("direct-v4 exact artifact drifted")
    legacy_release._validate_portable_release_bindings(  # noqa: SLF001
        exact_artifact.get("roles"),
        legacy_release.ARTIFACT_ROLES,
        "direct-v4 release-v2 artifact roles",
    )
    return dict(payload), binding


def _git_file_binding(root: Path, commit: str, path: str) -> dict[str, str]:
    blob = legacy_release._git(root, "rev-parse", f"{commit}:{path}")  # noqa: SLF001
    try:
        raw = subprocess.run(
            ("git", "show", f"{commit}:{path}"),
            cwd=root,
            check=True,
            capture_output=True,
            timeout=20.0,
        ).stdout
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise DirectOwnerReleaseV3Error(f"cannot bind changed source: {path}") from exc
    return {"git_blob_sha1": blob, "file_sha256": hashlib.sha256(raw).hexdigest()}


def _changed_repository_files(root: Path, execution: Mapping[str, str]) -> dict[str, Any]:
    commit = str(execution["execution_commit"])
    raw = legacy_release._git(  # noqa: SLF001
        root,
        "diff",
        "--name-status",
        PARENT_EXECUTION["execution_commit"],
        commit,
    )
    paths: list[str] = []
    for line in raw.splitlines():
        fields = line.split("\t")
        if len(fields) != 2 or fields[0] not in {"A", "M"}:
            raise DirectOwnerReleaseV3Error(
                "runtime successor diff must contain only added or modified files"
            )
        paths.append(fields[1])
    if not paths:
        raise DirectOwnerReleaseV3Error("runtime successor changed-file map is empty")
    return {path: _git_file_binding(root, commit, path) for path in sorted(paths)}


def _runtime_supplement(
    path: Path,
    *,
    execution: Mapping[str, str],
    config_pair: Mapping[str, Any],
    changed_files: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        supplement = importlib.import_module(
            "scripts.f05_buy_e3_no_shadow_runtime_fix_supplement"
        )
    except ModuleNotFoundError as exc:
        raise DirectOwnerReleaseV3Error(
            "runtime supplement validator is not integrated"
        ) from exc
    try:
        payload, binding = supplement.validate_content_receipt(path)
    except Exception as exc:  # the supplement owns its exact exception type
        raise DirectOwnerReleaseV3Error("runtime supplement validation failed") from exc
    if (
        not isinstance(payload, Mapping)
        or not isinstance(binding, Mapping)
        or set(binding) != CONTENT_BINDING_FIELDS
        or binding.get("schema_version") != RUNTIME_SUPPLEMENT_SCHEMA
        or binding.get("status") != RUNTIME_SUPPLEMENT_STATUS
        or binding.get("canonical_field") != RUNTIME_SUPPLEMENT_CANONICAL_FIELD
        or binding.get("mode") != "0600"
    ):
        raise DirectOwnerReleaseV3Error("runtime supplement exact7 binding drifted")
    if payload.get("parent_execution") != PARENT_EXECUTION:
        raise DirectOwnerReleaseV3Error("runtime supplement parent execution drifted")
    if payload.get("execution") != execution:
        raise DirectOwnerReleaseV3Error("runtime supplement execution drifted")
    if payload.get("changed_repository_files") != changed_files:
        raise DirectOwnerReleaseV3Error("runtime supplement changed-file map drifted")
    supplement_pair = payload.get("config_pair")
    if not isinstance(supplement_pair, Mapping):
        raise DirectOwnerReleaseV3Error("runtime supplement config pair is missing")
    expected_hashes = {
        NEW_DISABLED_CONFIG_SHA256,
        NEW_ACTIVE_CONFIG_SHA256,
    }
    observed_hashes = {
        str(value)
        for value in _walk_values(supplement_pair)
        if isinstance(value, str) and len(value) == 64
    }
    if not expected_hashes.issubset(observed_hashes):
        raise DirectOwnerReleaseV3Error("runtime supplement config hashes drifted")
    no_shadow = payload.get("no_shadow_runtime_contract")
    if not isinstance(no_shadow, Mapping):
        raise DirectOwnerReleaseV3Error("runtime supplement no-shadow contract is missing")
    for key in (
        "global_flow_shadow_enabled",
        "global_reference_shadow_enabled",
        "global_flow_evaluator_effective",
        "global_reference_evaluator_effective",
    ):
        if no_shadow.get(key) is not False:
            raise DirectOwnerReleaseV3Error(f"runtime supplement did not disable {key}")
    return dict(payload), dict(binding)


def _walk_values(value: Any):
    if isinstance(value, Mapping):
        for child in value.values():
            yield from _walk_values(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_values(child)
    else:
        yield value


def _artifact(artifact_paths: Mapping[str, Path], expected: Mapping[str, Any]) -> dict[str, Any]:
    artifact, roles = artifact_release._artifact_release(artifact_paths)  # noqa: SLF001
    observed = {"artifact_sha256": artifact["artifact_sha256"], "roles": roles}
    if observed != expected:
        raise DirectOwnerReleaseV3Error("exact BUY E3 artifact differs from release-v2")
    return observed


def build_direct_owner_release_v3(
    *,
    repository_root: Path,
    annotated_operational_tag: str,
    artifact_paths: Mapping[str, Path],
    parent_release_v2_path: Path,
    old_disabled_config_path: Path,
    old_active_config_path: Path,
    disabled_config_path: Path,
    active_config_path: Path,
    runtime_fix_supplement_path: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    execution = legacy_release._operational_git_identity(  # noqa: SLF001
        repository_root,
        annotated_operational_tag,
    )
    if execution == PARENT_EXECUTION:
        raise DirectOwnerReleaseV3Error("release-v3 execution did not advance from direct-v4")
    if subprocess.run(
        (
            "git",
            "merge-base",
            "--is-ancestor",
            PARENT_EXECUTION["execution_commit"],
            execution["execution_commit"],
        ),
        cwd=repository_root,
        check=False,
        capture_output=True,
        timeout=20.0,
    ).returncode != 0:
        raise DirectOwnerReleaseV3Error("release-v3 execution does not descend from direct-v4")
    parent_payload, parent_binding = _parent_release(parent_release_v2_path)
    exact_artifact = _artifact(artifact_paths, parent_payload["exact_artifact"])
    config_pair = _config_pair(
        old_disabled_path=old_disabled_config_path,
        old_active_path=old_active_config_path,
        disabled_path=disabled_config_path,
        active_path=active_config_path,
    )
    changed_files = _changed_repository_files(repository_root, execution)
    _supplement_payload, supplement_binding = _runtime_supplement(
        runtime_fix_supplement_path,
        execution=execution,
        config_pair=config_pair,
        changed_files=changed_files,
    )
    timestamp = generated_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    legacy_release._timestamp(timestamp, "direct owner v3 release timestamp")  # noqa: SLF001
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": STATUS,
        "generated_utc": timestamp,
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "outcome_informed_owner_override": True,
        "action_authorized": True,
        "live_authorized": True,
        "authorization_basis": dict(AUTHORIZATION_BASIS),
        "scope": dict(SCOPE),
        "execution": dict(execution),
        "parent_runtime_authority": {
            "release": parent_binding,
            "execution": dict(PARENT_EXECUTION),
        },
        "exact_artifact": exact_artifact,
        "historical_evidence": dict(HISTORICAL_EVIDENCE),
        "config_pair": config_pair,
        "runtime_fix_contract": dict(RUNTIME_FIX_CONTRACT),
        "runtime_fix_supplement": supplement_binding,
        "changed_repository_files": changed_files,
        "no_shadow_runtime_contract": dict(NO_SHADOW_RUNTIME_CONTRACT),
        "pending_current_runtime_evidence": dict(PENDING_CURRENT_RUNTIME_EVIDENCE),
        "rollback": dict(ROLLBACK),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[CANONICAL_FIELD] = legacy_release.document_sha256(payload, CANONICAL_FIELD)
    return payload


def validate_direct_owner_release_v3(
    path: Path,
    *,
    repository_root: Path,
    artifact_paths: Mapping[str, Path],
    parent_release_v2_path: Path,
    old_disabled_config_path: Path,
    old_active_config_path: Path,
    disabled_config_path: Path,
    active_config_path: Path,
    runtime_fix_supplement_path: Path,
) -> dict[str, Any]:
    document = legacy_release._open_document(path, "direct owner v3 active release")  # noqa: SLF001
    payload = document.payload
    if set(payload) != TOP_LEVEL_FIELDS:
        raise DirectOwnerReleaseV3Error("direct owner v3 release fields drifted")
    canonical = legacy_release._require_sha256(  # noqa: SLF001
        payload.get(CANONICAL_FIELD),
        "direct owner v3 canonical SHA256",
    )
    if canonical != legacy_release.document_sha256(payload, CANONICAL_FIELD):
        raise DirectOwnerReleaseV3Error("direct owner v3 canonical drifted")
    execution = payload.get("execution")
    if not isinstance(execution, Mapping):
        raise DirectOwnerReleaseV3Error("direct owner v3 execution is missing")
    expected = build_direct_owner_release_v3(
        repository_root=repository_root,
        annotated_operational_tag=str(execution.get("annotated_operational_tag", "")),
        artifact_paths=artifact_paths,
        parent_release_v2_path=parent_release_v2_path,
        old_disabled_config_path=old_disabled_config_path,
        old_active_config_path=old_active_config_path,
        disabled_config_path=disabled_config_path,
        active_config_path=active_config_path,
        runtime_fix_supplement_path=runtime_fix_supplement_path,
        generated_utc=legacy_release._timestamp(  # noqa: SLF001
            payload.get("generated_utc"),
            "direct owner v3 release timestamp",
        ),
    )
    if payload != expected:
        raise DirectOwnerReleaseV3Error("direct owner v3 release semantic identity drifted")
    return dict(payload)


def finalize_direct_owner_release_v3(
    *,
    output_path: Path,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
    payload = build_direct_owner_release_v3(**kwargs)
    file_sha256 = legacy_release._write_exclusive(output_path, payload)  # noqa: SLF001
    validated = validate_direct_owner_release_v3(
        output_path,
        repository_root=kwargs["repository_root"],
        artifact_paths=kwargs["artifact_paths"],
        parent_release_v2_path=kwargs["parent_release_v2_path"],
        old_disabled_config_path=kwargs["old_disabled_config_path"],
        old_active_config_path=kwargs["old_active_config_path"],
        disabled_config_path=kwargs["disabled_config_path"],
        active_config_path=kwargs["active_config_path"],
        runtime_fix_supplement_path=kwargs["runtime_fix_supplement_path"],
    )
    if validated != payload:
        raise DirectOwnerReleaseV3Error("written direct owner v3 release changed")
    return payload, file_sha256


def _artifact_paths(args: argparse.Namespace) -> dict[str, Path]:
    return {
        "manifest": args.artifact_manifest,
        "policy": args.policy,
        "predicate_bundle": args.predicate_bundle,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("finalize", "validate"):
        command = commands.add_parser(name)
        command.add_argument("--repository-root", type=Path, required=True)
        command.add_argument("--parent-release-v2", type=Path, required=True)
        command.add_argument("--old-disabled-config", type=Path, required=True)
        command.add_argument("--old-active-config", type=Path, required=True)
        command.add_argument("--disabled-config", type=Path, required=True)
        command.add_argument("--active-config", type=Path, required=True)
        command.add_argument("--runtime-fix-supplement", type=Path, required=True)
        command.add_argument("--artifact-manifest", type=Path, required=True)
        command.add_argument("--policy", type=Path, required=True)
        command.add_argument("--predicate-bundle", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True)
        if name == "finalize":
            command.add_argument("--annotated-operational-tag", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    common = {
        "repository_root": args.repository_root,
        "artifact_paths": _artifact_paths(args),
        "parent_release_v2_path": args.parent_release_v2,
        "old_disabled_config_path": args.old_disabled_config,
        "old_active_config_path": args.old_active_config,
        "disabled_config_path": args.disabled_config,
        "active_config_path": args.active_config,
        "runtime_fix_supplement_path": args.runtime_fix_supplement,
    }
    if args.command == "validate":
        payload = validate_direct_owner_release_v3(args.output, **common)
        print(payload[CANONICAL_FIELD])
        return 0
    payload, file_sha256 = finalize_direct_owner_release_v3(
        output_path=args.output,
        annotated_operational_tag=args.annotated_operational_tag,
        **common,
    )
    print(f"file_sha256={file_sha256}")
    print(f"canonical_sha256={payload[CANONICAL_FIELD]}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
