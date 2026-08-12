#!/usr/bin/env python3
"""Finalize the limited owner modeled-queue OOF decision without rereading outcomes.

The finalizer consumes only the atomically admitted JSON control plane emitted
by ``causal_multichannel_window_boolean_cooldown_modeled_oof.py``. It never
opens ``outer_oof.parquet`` or any other economic table. A Boolean gate may
nominate an owner-route repeated-policy successor; the continuous comparator
is diagnostic-only by construction.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from data_paths import resolve_portable_path
from research.governance.public_machine_projection import (
    PublicMachineProjectionError,
    source_document_path,
    source_identity_sha256,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2_owner_modeled_queue_v1"
REPORT_SCHEMA = f"{IDENTITY}.nested_oof_output.v1"
OOF_MANIFEST_SCHEMA = f"{IDENTITY}.atomic_admission.v1"
EXECUTION_AMENDMENT_SCHEMA = f"{IDENTITY}.oof_execution_amendment.v1"
SPEC_SCHEMA = "causal_multichannel_window_boolean_cooldown_duration_v2.owner_modeled_queue.spec.v1"
CONFIG_SCHEMA = (
    "causal_multichannel_window_boolean_cooldown_duration_v2.owner_modeled_queue.study_config.v1"
)
EVIDENCE_ROUTE = "owner_risk_accepted_modelled_queue_exploration"
QUEUE_AUTHORITY = "modelled_queue_without_exchange_queue_authority"
CONTROL_ACTION = "CONTROL_85N"
SIDES = ("BUY", "SELL")

POLICY_BUNDLE_SCHEMA = f"{IDENTITY}.post_oof_owner_policy_bundle.v2"
NO_PASS_SCHEMA = f"{IDENTITY}.post_oof_limited_no_pass.v2"
MANIFEST_SCHEMA = f"{IDENTITY}.post_oof_atomic_admission.v1"
POLICY_BUNDLE_NAME = "owner_policy_bundle.json"
NO_PASS_NAME = "limited_oof_no_pass.json"
# Compatibility aliases for callers that imported the old constant names.
CLOSURE_SCHEMA = NO_PASS_SCHEMA
CLOSURE_NAME = NO_PASS_NAME
MANIFEST_NAME = "manifest.json"
SUCCESS_NAME = "_SUCCESS"

REQUIRED_OOF_JSON = (
    "frozen_config.json",
    "frozen_owner_spec.json",
    "bindings.json",
    "report.json",
    "selected_candidates.json",
)

DEFAULT_SPEC_PATH = Path(
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "owner_modeled_queue_v1_spec_20260811.json"
)
DEFAULT_SPEC_SHA256 = "362cb1848da44e8b6f4e274ab4e99f7077e9cea7efafcbd77cc404e53774c666"
DEFAULT_CONFIG_PATH = Path(
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "owner_modeled_queue_v1_study_config_20260811.json"
)
DEFAULT_CONFIG_SHA256 = "636074fdbf52b363bcde953926db0a529e5f9ac349324cddd4473f70f56e6659"


class PostOofFinalizationError(RuntimeError):
    """Raised when a bound input or post-OOF authority contract drifts."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("ascii")).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    candidate = Path(path)
    if candidate.is_symlink() or not candidate.is_file():
        raise PostOofFinalizationError(f"{role} is missing or is a symlink: {candidate}")
    try:
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PostOofFinalizationError(f"cannot read {role}: {candidate}") from exc
    if not isinstance(payload, dict):
        raise PostOofFinalizationError(f"{role} root must be an object")
    return payload


def _require_file_hash(path: Path, expected: str, *, role: str) -> str:
    if len(str(expected)) != 64:
        raise PostOofFinalizationError(f"{role} expected SHA256 is invalid")
    actual = _file_sha256(path)
    if actual != expected:
        raise PostOofFinalizationError(f"{role} SHA256 mismatch")
    return actual


def _require_exact_source_document(path: Path, expected: str, *, role: str) -> Path:
    """Verify a public projection, then return its exact execution source."""

    public_path = Path(path).expanduser().resolve()
    if len(str(expected)) != 64:
        raise PostOofFinalizationError(f"{role} expected SHA256 is invalid")
    try:
        observed_source_sha256 = source_identity_sha256(public_path)
        exact_source_path = source_document_path(public_path, require_private=True)
    except (OSError, PublicMachineProjectionError) as exc:
        raise PostOofFinalizationError(
            f"{role} exact source is unavailable or invalid"
        ) from exc
    if observed_source_sha256 != expected:
        raise PostOofFinalizationError(f"{role} SHA256 mismatch")
    return exact_source_path


def _require_false_fields(
    payload: Mapping[str, Any],
    names: Sequence[str],
    *,
    role: str,
) -> None:
    if any(payload.get(name) is not False for name in names):
        raise PostOofFinalizationError(f"{role} permissions drifted")


def _resolve_bound_path(value: Any) -> Path:
    path = resolve_portable_path(str(value))
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[4] / path
    return path.resolve()


def _resolve_exact_source_bound_path(value: Any, *, role: str) -> Path:
    """Resolve a historical public locator to the exact private source bytes."""

    resolved = _resolve_bound_path(value)
    try:
        return source_document_path(resolved, require_private=True)
    except (OSError, PublicMachineProjectionError) as exc:
        raise PostOofFinalizationError(
            f"{role} exact source is unavailable or invalid"
        ) from exc


def _validate_internal_canonical(
    payload: Mapping[str, Any],
    field: str,
    *,
    role: str,
) -> str:
    observed = str(payload.get(field, ""))
    body = dict(payload)
    body.pop(field, None)
    if len(observed) != 64 or observed != _canonical_sha256(body):
        raise PostOofFinalizationError(f"{role} canonical SHA256 drifted")
    return observed


def _inventory_by_name(manifest: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    rows = manifest.get("files")
    if not isinstance(rows, list):
        raise PostOofFinalizationError("OOF manifest file inventory is missing")
    inventory: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise PostOofFinalizationError("OOF manifest file row is invalid")
        name = str(row.get("relative_path", ""))
        if not name or Path(name).name != name or name in inventory:
            raise PostOofFinalizationError("OOF manifest file inventory is non-canonical")
        inventory[name] = row
    return inventory


def _validate_oof_admission(oof_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    root = Path(oof_dir).expanduser().resolve()
    if root.is_symlink() or not root.is_dir():
        raise PostOofFinalizationError("OOF output directory is missing or is a symlink")
    manifest_path = root / MANIFEST_NAME
    manifest = _load_json(manifest_path, role="OOF manifest")
    manifest_sha = _file_sha256(manifest_path)
    success_path = root / SUCCESS_NAME
    if success_path.is_symlink() or not success_path.is_file():
        raise PostOofFinalizationError("OOF _SUCCESS marker is missing")
    try:
        success_value = success_path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError) as exc:
        raise PostOofFinalizationError("OOF _SUCCESS marker is invalid") from exc
    if success_value != manifest_sha:
        raise PostOofFinalizationError("OOF _SUCCESS does not bind its manifest")
    if (
        manifest.get("schema_version") != OOF_MANIFEST_SCHEMA
        or manifest.get("identity") != IDENTITY
        or manifest.get("evidence_route") != EVIDENCE_ROUTE
        or manifest.get("queue_authority") != QUEUE_AUTHORITY
    ):
        raise PostOofFinalizationError("OOF manifest identity/schema drifted")
    permissions = manifest.get("permissions")
    if not isinstance(permissions, Mapping):
        raise PostOofFinalizationError("OOF manifest permissions are missing")
    _require_false_fields(
        permissions,
        ("strict_queue_authorized", "action_authorized", "live_authorized"),
        role="OOF manifest",
    )
    inventory = _inventory_by_name(manifest)
    missing = set(REQUIRED_OOF_JSON) - set(inventory)
    if missing:
        raise PostOofFinalizationError(
            f"OOF manifest lacks required JSON controls: {sorted(missing)}"
        )
    loaded: dict[str, Any] = {}
    for name in REQUIRED_OOF_JSON:
        path = root / name
        row = inventory[name]
        expected_size = row.get("bytes")
        expected_sha = str(row.get("sha256", ""))
        if path.is_symlink() or not path.is_file():
            raise PostOofFinalizationError(f"bound OOF JSON is missing: {name}")
        if expected_size != path.stat().st_size or _file_sha256(path) != expected_sha:
            raise PostOofFinalizationError(f"bound OOF JSON SHA256 mismatch: {name}")
        loaded[name] = _load_json(path, role=f"OOF {name}")
    return {
        "root": str(root),
        "manifest_path": str(manifest_path),
        "manifest_file_sha256": manifest_sha,
        "success_marker_file_sha256": _file_sha256(success_path),
        "manifest": manifest,
        "inventory": inventory,
    }, loaded


def _validate_frozen_documents(
    *,
    config_path: Path,
    expected_config_sha256: str,
    spec_path: Path,
    expected_spec_sha256: str,
    copied_config: Mapping[str, Any],
    copied_spec: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], Path, Path]:
    config_resolved = Path(config_path).expanduser().resolve()
    spec_resolved = _require_exact_source_document(
        spec_path,
        expected_spec_sha256,
        role="frozen owner spec",
    )
    _require_file_hash(config_resolved, expected_config_sha256, role="frozen config")
    config = _load_json(config_resolved, role="frozen config")
    spec = _load_json(spec_resolved, role="frozen owner spec")
    if config != copied_config or spec != copied_spec:
        raise PostOofFinalizationError("OOF frozen config/spec copies drifted from bound files")
    if (
        config.get("schema_version") != CONFIG_SCHEMA
        or config.get("identity") != IDENTITY
        or config.get("config_status") != "frozen_before_owner_oof_economic_read"
    ):
        raise PostOofFinalizationError("frozen config identity/schema/status drifted")
    if (
        spec.get("schema_version") != SPEC_SCHEMA
        or spec.get("identity") != IDENTITY
        or spec.get("status") != "pre_economic_oof_frozen"
    ):
        raise PostOofFinalizationError("frozen owner spec identity/schema/status drifted")
    config_permissions = config.get("permissions")
    spec_permissions = spec.get("permissions")
    if not isinstance(config_permissions, Mapping) or not isinstance(spec_permissions, Mapping):
        raise PostOofFinalizationError("frozen config/spec permissions are missing")
    _require_false_fields(
        config_permissions,
        ("validation_read", "sealed_holdout_read", "action_authorized", "live_authorized"),
        role="frozen config",
    )
    _require_false_fields(
        spec_permissions,
        ("validation_read", "sealed_holdout_read", "action_authorized", "live_authorized"),
        role="frozen owner spec",
    )
    if manifest.get("config_sha256") != expected_config_sha256:
        raise PostOofFinalizationError("OOF manifest config SHA256 drifted")
    if manifest.get("owner_spec_sha256") != expected_spec_sha256:
        raise PostOofFinalizationError("OOF manifest owner-spec SHA256 drifted")
    inventory = _inventory_by_name(manifest)
    if inventory["frozen_config.json"].get("sha256") != expected_config_sha256:
        raise PostOofFinalizationError("OOF frozen config copy SHA256 drifted")
    if inventory["frozen_owner_spec.json"].get("sha256") != expected_spec_sha256:
        raise PostOofFinalizationError("OOF frozen owner-spec copy SHA256 drifted")
    strict = spec.get("strict_native_boundary")
    promotion = spec.get("promotion_contract")
    if not isinstance(strict, Mapping) or not isinstance(promotion, Mapping):
        raise PostOofFinalizationError("owner spec authority boundary is missing")
    _require_false_fields(
        strict,
        (
            "strict_label_execution_eligible",
            "exact_queue_policy_eligible",
            "strict_queue_authority_inherited",
        ),
        role="owner spec strict-native boundary",
    )
    if (
        promotion.get("only_supported_side_may_advance") is not True
        or promotion.get("support_label") != "owner_risk_accepted_promotion"
        or promotion.get("strict_research_supported_promotion_available") is not False
        or promotion.get("repeated_policy_required_after_oof") is not True
    ):
        raise PostOofFinalizationError("owner spec promotion contract drifted")
    return config, spec, config_resolved, spec_resolved


def _validate_execution_amendment(
    path: Path,
    *,
    expected_sha256: str,
    config_path: Path,
    config_sha256: str,
    spec_path: Path,
    spec_sha256: str,
) -> tuple[dict[str, Any], Path]:
    resolved = _require_exact_source_document(
        path,
        expected_sha256,
        role="OOF execution amendment",
    )
    payload = _load_json(resolved, role="OOF execution amendment")
    if (
        payload.get("schema_version") != EXECUTION_AMENDMENT_SCHEMA
        or payload.get("identity") != IDENTITY
        or payload.get("status") != "frozen_before_owner_oof_economic_read"
    ):
        raise PostOofFinalizationError("OOF execution amendment identity/schema/status drifted")
    permissions = payload.get("permissions")
    if not isinstance(permissions, Mapping):
        raise PostOofFinalizationError("OOF execution amendment permissions are missing")
    _require_false_fields(
        permissions,
        ("validation_read", "sealed_holdout_read", "action_authorized", "live_authorized"),
        role="OOF execution amendment",
    )
    _validate_internal_canonical(
        payload,
        "execution_identity_sha256",
        role="OOF execution amendment",
    )
    artifacts = payload.get("artifact_bindings")
    if not isinstance(artifacts, Mapping):
        raise PostOofFinalizationError("OOF execution amendment artifact bindings are missing")
    for name, expected_path, expected_hash in (
        ("frozen_config", config_path, config_sha256),
        ("frozen_owner_spec", spec_path, spec_sha256),
    ):
        row = artifacts.get(name)
        if not isinstance(row, Mapping):
            raise PostOofFinalizationError(f"OOF execution amendment lacks {name}")
        if (
            (
                _resolve_exact_source_bound_path(
                    row.get("path"),
                    role="OOF execution amendment frozen_owner_spec",
                )
                if name == "frozen_owner_spec"
                else _resolve_bound_path(row.get("path"))
            )
            != expected_path
            or row.get("sha256") != expected_hash
            or row.get("identity") != IDENTITY
        ):
            raise PostOofFinalizationError(f"OOF execution amendment {name} binding drifted")
    return payload, resolved


def _expected_panel_days(spec: Mapping[str, Any], panel: str) -> tuple[str, ...]:
    development = spec.get("development_days")
    panels = spec.get("analysis_panels")
    if not isinstance(development, list) or not isinstance(panels, Mapping):
        raise PostOofFinalizationError("owner spec panel denominator is missing")
    contract = panels.get(panel)
    if not isinstance(contract, Mapping):
        raise PostOofFinalizationError(f"owner spec lacks panel {panel}")
    excluded = contract.get("excluded_days", [])
    if not isinstance(excluded, list):
        raise PostOofFinalizationError(f"owner spec panel exclusions are invalid: {panel}")
    days = tuple(str(day) for day in development if day not in set(map(str, excluded)))
    if contract.get("days") != len(days):
        raise PostOofFinalizationError(f"owner spec panel day count drifted: {panel}")
    return days


def _eligible_panels(
    config: Mapping[str, Any],
    spec: Mapping[str, Any],
    report: Mapping[str, Any],
) -> tuple[str, ...]:
    spec_panels = spec.get("analysis_panels")
    config_panels = config.get("analysis_panels")
    post_gate = config.get("post_oof_gate")
    if (
        not isinstance(spec_panels, Mapping)
        or not isinstance(config_panels, list)
        or not isinstance(post_gate, Mapping)
    ):
        raise PostOofFinalizationError("eligible-prefix panel contract is missing")
    spec_eligible = {
        str(name)
        for name, contract in spec_panels.items()
        if isinstance(contract, Mapping)
        and contract.get("may_grant_owner_exploratory_support") is True
    }
    eligible = tuple(map(str, config_panels))
    if set(eligible) != spec_eligible or len(eligible) != len(spec_eligible):
        raise PostOofFinalizationError("config eligible-prefix panels drifted from owner spec")
    if (
        post_gate.get("only_prefix_panels_may_grant_owner_support") is not True
        or post_gate.get("added10_or_pooled50_may_grant_support") is not False
    ):
        raise PostOofFinalizationError("post-OOF prefix-panel authority drifted")
    results = report.get("results")
    denominators = report.get("panel_denominators")
    if not isinstance(results, Mapping) or not isinstance(denominators, Mapping):
        raise PostOofFinalizationError("OOF report panel results are missing")
    if set(results) != set(eligible) or set(denominators) != set(eligible):
        raise PostOofFinalizationError("OOF report panel scope drifted")
    for panel in eligible:
        contract = spec_panels[panel]
        denominator = denominators.get(panel)
        if not isinstance(contract, Mapping) or not isinstance(denominator, Mapping):
            raise PostOofFinalizationError(f"OOF panel denominator is invalid: {panel}")
        expected_days = _expected_panel_days(spec, panel)
        expected_blocks = contract.get("eligible_feature_blocks")
        if not isinstance(expected_blocks, list):
            raise PostOofFinalizationError(f"owner spec panel blocks are invalid: {panel}")
        if (
            denominator.get("days") != list(expected_days)
            or denominator.get("day_count") != len(expected_days)
            or denominator.get("eligible_feature_blocks") != expected_blocks
            or denominator.get("all_blocks_use_common_scope_denominator") is not True
        ):
            raise PostOofFinalizationError(f"OOF panel denominator drifted: {panel}")
    return eligible


def _validate_policy_payload(
    payload: Mapping[str, Any],
    *,
    side: str,
    vocabulary: set[str],
) -> str:
    if (
        payload.get("identity")
        != "causal_multichannel_window_boolean_cooldown_duration_v2."
        "nested_chronological_boolean_oof.v1"
        or payload.get("side") != side
        or payload.get("default_action") != CONTROL_ACTION
    ):
        raise PostOofFinalizationError(f"{side} Boolean policy identity drifted")
    permissions = payload.get("permissions")
    rules = payload.get("ordered_first_match_rules")
    if not isinstance(permissions, Mapping) or not isinstance(rules, list) or not rules:
        raise PostOofFinalizationError(f"{side} Boolean policy structure is invalid")
    _require_false_fields(
        permissions,
        ("action_authorized", "live_authorized"),
        role=f"{side} Boolean policy",
    )
    for rule in rules:
        if not isinstance(rule, Mapping) or rule.get("action") not in vocabulary - {CONTROL_ACTION}:
            raise PostOofFinalizationError(f"{side} Boolean policy action is invalid")
        clauses = rule.get("clauses")
        if not isinstance(clauses, list) or not clauses:
            raise PostOofFinalizationError(f"{side} Boolean policy clause is invalid")
        for clause in clauses:
            literals = clause.get("literals") if isinstance(clause, Mapping) else None
            if not isinstance(literals, list) or not literals:
                raise PostOofFinalizationError(f"{side} Boolean policy literals are invalid")
            for literal in literals:
                if (
                    not isinstance(literal, Mapping)
                    or not str(literal.get("predicate", ""))
                    or not isinstance(literal.get("negated"), bool)
                ):
                    raise PostOofFinalizationError(f"{side} Boolean policy literal is invalid")
    return _canonical_sha256(payload)


def _validate_gate(
    gate: Mapping[str, Any],
    *,
    panel: str,
    method: str,
) -> bool:
    if (
        gate.get("evaluated_after_outer_oof") is not True
        or gate.get("evidence_route") != EVIDENCE_ROUTE
        or gate.get("panel_scope") != panel
    ):
        raise PostOofFinalizationError(f"{panel}/{method} post-OOF gate identity drifted")
    _require_false_fields(
        gate,
        ("action_authorized", "live_authorized", "strict_queue_authorized"),
        role=f"{panel}/{method} post-OOF gate",
    )
    passed = gate.get("passed_for_owner_repeated_policy_successor")
    reasons = gate.get("reasons")
    if not isinstance(passed, bool) or not isinstance(reasons, list):
        raise PostOofFinalizationError(f"{panel}/{method} post-OOF gate is invalid")
    if method == "continuous":
        if (
            passed
            or gate.get("decision") != "diagnostic_only"
            or "continuous_comparator_cannot_nominate_a_policy_successor" not in reasons
        ):
            raise PostOofFinalizationError("continuous comparator attempted to nominate a policy")
        return False
    expected_decision = "owner_replay_candidate_supported" if passed else "abstain"
    if gate.get("decision") != expected_decision or (passed and reasons):
        raise PostOofFinalizationError(f"{panel}/Boolean gate decision drifted")
    if not passed and not reasons:
        raise PostOofFinalizationError(f"{panel}/Boolean abstention lacks a reason")
    return passed


def _validate_permissions_and_report(
    report: Mapping[str, Any],
    *,
    config_sha256: str,
    binding_sha256: str,
) -> None:
    if (
        report.get("schema_version") != REPORT_SCHEMA
        or report.get("identity") != IDENTITY
        or report.get("evidence_route") != EVIDENCE_ROUTE
        or report.get("queue_authority") != QUEUE_AUTHORITY
        or report.get("strict_queue_policy_eligible") is not False
        or report.get("validation_read") is not False
        or report.get("sealed_holdout_read") is not False
        or report.get("config_sha256") != config_sha256
        or report.get("binding_sha256") != binding_sha256
    ):
        raise PostOofFinalizationError("OOF report identity/hash/permission boundary drifted")
    permissions = report.get("permissions")
    if not isinstance(permissions, Mapping):
        raise PostOofFinalizationError("OOF report permissions are missing")
    _require_false_fields(
        permissions,
        ("action_authorized", "live_authorized"),
        role="OOF report",
    )
    if permissions.get("research_authority") != "owner_route_exploratory_only":
        raise PostOofFinalizationError("OOF report research authority drifted")
    not_run = report.get("not_run_panels")
    if not isinstance(not_run, Mapping) or set(not_run) != {"added10", "pooled50"}:
        raise PostOofFinalizationError("OOF report not-run panel boundary drifted")
    for panel, row in not_run.items():
        if (
            not isinstance(row, Mapping)
            or row.get("modeled_labels_imputed") is not False
            or row.get("economic_oof_run") is not False
            or row.get("may_grant_support") is not False
        ):
            raise PostOofFinalizationError(f"OOF report {panel} authority drifted")


@dataclass(frozen=True, slots=True)
class ValidatedDecision:
    input_bindings: Mapping[str, Any]
    supported_sides: tuple[str, ...]
    side_policies: Mapping[str, Any]
    report_canonical_sha256: str
    decision_type: str


def validate_post_oof_input(
    oof_dir: Path,
    *,
    config_path: Path,
    expected_config_sha256: str,
    spec_path: Path,
    expected_spec_sha256: str,
    execution_amendment_path: Path,
    expected_execution_amendment_sha256: str,
) -> ValidatedDecision:
    """Validate the JSON control plane and derive the exact side decision."""

    admission, files = _validate_oof_admission(oof_dir)
    manifest = admission["manifest"]
    config, spec, config_resolved, spec_resolved = _validate_frozen_documents(
        config_path=config_path,
        expected_config_sha256=expected_config_sha256,
        spec_path=spec_path,
        expected_spec_sha256=expected_spec_sha256,
        copied_config=files["frozen_config.json"],
        copied_spec=files["frozen_owner_spec.json"],
        manifest=manifest,
    )
    amendment, amendment_resolved = _validate_execution_amendment(
        execution_amendment_path,
        expected_sha256=expected_execution_amendment_sha256,
        config_path=config_resolved,
        config_sha256=expected_config_sha256,
        spec_path=spec_resolved,
        spec_sha256=expected_spec_sha256,
    )
    bindings = files["bindings.json"]
    binding_sha = _validate_internal_canonical(
        bindings,
        "binding_sha256",
        role="OOF bindings",
    )
    if manifest.get("binding_sha256") != binding_sha:
        raise PostOofFinalizationError("OOF manifest binding SHA256 drifted")
    for name, expected_path, expected_sha in (
        ("frozen_config", config_resolved, expected_config_sha256),
        ("frozen_owner_spec", spec_resolved, expected_spec_sha256),
    ):
        row = bindings.get(name)
        if (
            not isinstance(row, Mapping)
            or (
                _resolve_exact_source_bound_path(
                    row.get("path"),
                    role="OOF bindings frozen_owner_spec",
                )
                if name == "frozen_owner_spec"
                else _resolve_bound_path(row.get("path"))
            )
            != expected_path
            or row.get("sha256") != expected_sha
        ):
            raise PostOofFinalizationError(f"OOF bindings {name} drifted")
    execution_row = bindings.get("execution_amendment")
    if (
        not isinstance(execution_row, Mapping)
        or _resolve_exact_source_bound_path(
            execution_row.get("path"),
            role="OOF bindings execution amendment",
        )
        != amendment_resolved
        or execution_row.get("sha256") != expected_execution_amendment_sha256
        or execution_row.get("execution_identity_sha256")
        != amendment.get("execution_identity_sha256")
        or execution_row.get("artifact_bindings") != amendment.get("artifact_bindings")
    ):
        raise PostOofFinalizationError("OOF execution-amendment binding drifted")

    report = files["report.json"]
    _validate_permissions_and_report(
        report,
        config_sha256=expected_config_sha256,
        binding_sha256=binding_sha,
    )
    panels = _eligible_panels(config, spec, report)
    selected = files["selected_candidates.json"]
    if set(selected) != set(panels):
        raise PostOofFinalizationError("selected-candidate panel scope drifted")
    results = report["results"]
    vocabularies = spec.get("duration_vocabulary")
    if not isinstance(vocabularies, Mapping):
        raise PostOofFinalizationError("owner spec duration vocabulary is missing")

    side_policies: dict[str, Any] = {}
    supported_sides: list[str] = []
    for side in SIDES:
        vocabulary = set(map(str, vocabularies.get(side, [])))
        if CONTROL_ACTION not in vocabulary or len(vocabulary) < 2:
            raise PostOofFinalizationError(f"{side} duration vocabulary drifted")
        supported_cells: list[dict[str, Any]] = []
        non_supporting_absolute_passes: list[dict[str, str]] = []
        for panel in panels:
            panel_results = results.get(panel)
            panel_candidates = selected.get(panel)
            if not isinstance(panel_results, Mapping) or not isinstance(panel_candidates, Mapping):
                raise PostOofFinalizationError(f"OOF panel result is invalid: {panel}")
            side_results = panel_results.get(side)
            side_candidates = panel_candidates.get(side)
            if not isinstance(side_results, Mapping) or not isinstance(side_candidates, Mapping):
                raise PostOofFinalizationError(f"OOF side result is invalid: {panel}/{side}")
            blocks = report["panel_denominators"][panel]["eligible_feature_blocks"]
            if set(side_results) != set(blocks) or set(side_candidates) != set(blocks):
                raise PostOofFinalizationError(f"OOF feature-block scope drifted: {panel}/{side}")
            for block in blocks:
                cell = side_results[block]
                candidate_cell = side_candidates[block]
                if not isinstance(cell, Mapping) or not isinstance(candidate_cell, Mapping):
                    raise PostOofFinalizationError(f"OOF cell is invalid: {panel}/{side}/{block}")
                boolean = cell.get("boolean")
                continuous = cell.get("continuous")
                if not isinstance(boolean, Mapping) or not isinstance(continuous, Mapping):
                    raise PostOofFinalizationError(
                        f"OOF method summary is invalid: {panel}/{side}/{block}"
                    )
                for summary, method, expected_name in (
                    (boolean, "boolean", "bounded_sparse_boolean_dnf"),
                    (continuous, "continuous", "continuous_multioutput_decision_tree"),
                ):
                    if (
                        summary.get("side") != side
                        or summary.get("feature_block") != block
                        or summary.get("panel_scope") != panel
                        or summary.get("method") != expected_name
                        or not isinstance(summary.get("deployment_gate"), Mapping)
                    ):
                        raise PostOofFinalizationError(
                            f"OOF method identity drifted: {panel}/{side}/{block}/{method}"
                        )
                boolean_passed = _validate_gate(
                    boolean["deployment_gate"], panel=panel, method="boolean"
                )
                _validate_gate(continuous["deployment_gate"], panel=panel, method="continuous")
                boolean_candidates = candidate_cell.get("boolean")
                continuous_candidates = candidate_cell.get("continuous")
                if not isinstance(boolean_candidates, list) or not isinstance(
                    continuous_candidates, list
                ):
                    raise PostOofFinalizationError(
                        f"OOF selected candidates are invalid: {panel}/{side}/{block}"
                    )
                validated_policies = []
                for candidate in boolean_candidates:
                    if not isinstance(candidate, Mapping):
                        raise PostOofFinalizationError("Boolean candidate payload is invalid")
                    candidate_sha = _validate_policy_payload(
                        candidate,
                        side=side,
                        vocabulary=vocabulary,
                    )
                    validated_policies.append(
                        {
                            "policy_canonical_sha256": candidate_sha,
                            "policy": dict(candidate),
                        }
                    )
                folds = boolean.get("folds")
                if not isinstance(folds, list) or len(folds) != len(validated_policies):
                    raise PostOofFinalizationError(
                        f"OOF fold-policy count drifted: {panel}/{side}/{block}"
                    )
                for fold, policy in zip(folds, validated_policies, strict=True):
                    if (
                        not isinstance(fold, Mapping)
                        or fold.get("selected_candidate_id") != policy["policy_canonical_sha256"]
                    ):
                        raise PostOofFinalizationError(
                            f"OOF fold-policy identity drifted: {panel}/{side}/{block}"
                        )
                # The frozen v3 hierarchy starts at M0. R0 is reproduction-only,
                # while M1/M2 require paired incremental evidence that this report
                # does not contain. An independent absolute pass in those blocks
                # therefore cannot nominate a side.
                if boolean_passed and block == "M0":
                    if not validated_policies:
                        raise PostOofFinalizationError(
                            f"passing Boolean cell lacks policies: {panel}/{side}/{block}"
                        )
                    supported_cells.append(
                        {
                            "panel_scope": panel,
                            "feature_block": block,
                            "promotion_route": "owner_risk_accepted_promotion",
                            "deployment_gate": dict(boolean["deployment_gate"]),
                            "outer_fold_policy_bundle": validated_policies,
                        }
                    )
                elif boolean_passed:
                    non_supporting_absolute_passes.append(
                        {"panel_scope": panel, "feature_block": block}
                    )
        if supported_cells:
            supported_sides.append(side)
            side_policies[side] = {
                "mode": "owner_risk_accepted_boolean_candidate_bundle",
                "fallback_action": CONTROL_ACTION,
                "supported_cells": supported_cells,
                "feature_family_gate": {
                    "support_bearing_stage": "M0_absolute",
                    "paired_M1_minus_M0_evaluated": False,
                    "paired_M2_minus_M1_evaluated": False,
                    "non_supporting_absolute_passes": non_supporting_absolute_passes,
                },
            }
        else:
            side_policies[side] = {
                "mode": "control_only",
                "fixed_action": CONTROL_ACTION,
                "reason": "no_M0_absolute_post_oof_gate_passed",
                "feature_family_gate": {
                    "support_bearing_stage": "M0_absolute",
                    "paired_M1_minus_M0_evaluated": False,
                    "paired_M2_minus_M1_evaluated": False,
                    "non_supporting_absolute_passes": non_supporting_absolute_passes,
                },
            }

    input_bindings = {
        "oof_output_root": admission["root"],
        "oof_manifest_path": admission["manifest_path"],
        "oof_manifest_file_sha256": admission["manifest_file_sha256"],
        "oof_success_marker_file_sha256": admission["success_marker_file_sha256"],
        "oof_report_file_sha256": admission["inventory"]["report.json"]["sha256"],
        "oof_selected_candidates_file_sha256": admission["inventory"]["selected_candidates.json"][
            "sha256"
        ],
        "oof_binding_sha256": binding_sha,
        "frozen_config_path": str(config_resolved),
        "frozen_config_file_sha256": expected_config_sha256,
        "frozen_owner_spec_path": str(spec_resolved),
        "frozen_owner_spec_file_sha256": expected_spec_sha256,
        "execution_amendment_path": str(amendment_resolved),
        "execution_amendment_file_sha256": expected_execution_amendment_sha256,
        "execution_identity_sha256": amendment["execution_identity_sha256"],
    }
    return ValidatedDecision(
        input_bindings=input_bindings,
        supported_sides=tuple(supported_sides),
        side_policies=side_policies,
        report_canonical_sha256=_canonical_sha256(report),
        decision_type=(
            "owner_M0_policy_bundle"
            if supported_sides
            else "limited_modeled_queue_one_shot_oof_no_pass"
        ),
    )


LOCKED_PERMISSIONS = {
    "validation_read": False,
    "sealed_holdout_read": False,
    "repeated_policy_run": False,
    "restart_aware_run": False,
    "transport_run": False,
    "action_authorized": False,
    "live_authorized": False,
}


def _artifact_payload(decision: ValidatedDecision) -> tuple[str, dict[str, Any]]:
    common = {
        "identity": IDENTITY,
        "evidence_route": EVIDENCE_ROUTE,
        "queue_authority": QUEUE_AUTHORITY,
        "input_bindings": dict(decision.input_bindings),
        "oof_report_canonical_sha256": decision.report_canonical_sha256,
        "additional_economic_tables_read": False,
        "continuous_comparator_policy_nomination_allowed": False,
        "feature_family_gate_interpretation": {
            "frozen_hierarchy": [
                "M0_absolute",
                "paired_M1_minus_M0_with_M1_absolute",
                "paired_M2_minus_M1_with_M2_absolute",
            ],
            "implemented_in_bound_oof_report": "independent_absolute_cell_gates",
            "support_bearing_stage_available": "M0_absolute_only",
            "paired_M1_minus_M0_evaluated": False,
            "paired_M2_minus_M1_evaluated": False,
            "continuous_minus_boolean_evaluated": False,
        },
        "permissions": dict(LOCKED_PERMISSIONS),
    }
    if decision.supported_sides:
        payload = {
            "schema_version": POLICY_BUNDLE_SCHEMA,
            **common,
            "status": "owner_risk_accepted_boolean_policy_bundle_frozen",
            "promotion_route": "owner_risk_accepted_promotion",
            "supported_sides": list(decision.supported_sides),
            "side_policies": dict(decision.side_policies),
            "structural_eligibility": {
                "repeated_policy_implementation_eligible": True,
                "repeated_policy_economic_authorized": False,
                "restart_aware_execution_eligible": False,
                "transport_execution_eligible": False,
            },
        }
        name = POLICY_BUNDLE_NAME
    else:
        payload = {
            "schema_version": NO_PASS_SCHEMA,
            **common,
            "status": "limited_owner_modeled_queue_one_shot_oof_no_supported_M0_policy",
            "supported_sides": [],
            "side_policies": dict(decision.side_policies),
            "structural_eligibility": {
                "repeated_policy_implementation_eligible": False,
                "repeated_policy_economic_authorized": False,
                "restart_aware_execution_eligible": False,
                "transport_execution_eligible": False,
            },
            "no_pass_scope": {
                "tested_identity": IDENTITY,
                "tested_estimand": "owner_modeled_queue_one_shot_outer_oof_boolean_duration_policy",
                "strict_native_labels_generated": False,
                "frozen_hierarchy_fully_evaluated": False,
                "does_not_claim_entire_ema_architecture_failed": True,
                "does_not_close": [
                    "strict_raw_native_or_receive_time_identified_labels",
                    "paired_M1_minus_M0_incremental_value",
                    "paired_M2_minus_M1_incremental_value",
                    "continuous_minus_boolean_incremental_value",
                    "broader_multi_rule_ordered_boolean_policy",
                    "repeated_policy_full_path_economics",
                ],
            },
        }
        name = NO_PASS_NAME
    payload["canonical_sha256"] = _canonical_sha256(payload)
    return name, payload


def preflight_post_oof(
    oof_dir: Path,
    *,
    config_path: Path,
    expected_config_sha256: str,
    spec_path: Path,
    expected_spec_sha256: str,
    execution_amendment_path: Path,
    expected_execution_amendment_sha256: str,
) -> dict[str, Any]:
    decision = validate_post_oof_input(
        oof_dir,
        config_path=config_path,
        expected_config_sha256=expected_config_sha256,
        spec_path=spec_path,
        expected_spec_sha256=expected_spec_sha256,
        execution_amendment_path=execution_amendment_path,
        expected_execution_amendment_sha256=expected_execution_amendment_sha256,
    )
    artifact_name, artifact = _artifact_payload(decision)
    return {
        "identity": IDENTITY,
        "status": "post_oof_preflight_passed",
        "decision_type": decision.decision_type,
        "supported_sides": list(decision.supported_sides),
        "planned_artifact": artifact_name,
        "planned_artifact_canonical_sha256": artifact["canonical_sha256"],
        "economic_tables_read": [],
        "permissions": dict(LOCKED_PERMISSIONS),
    }


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(_canonical_json(payload) + "\n", encoding="ascii")
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def finalize_post_oof(
    oof_dir: Path,
    output_dir: Path,
    *,
    config_path: Path,
    expected_config_sha256: str,
    spec_path: Path,
    expected_spec_sha256: str,
    execution_amendment_path: Path,
    expected_execution_amendment_sha256: str,
) -> dict[str, Any]:
    """Validate and atomically publish one M0 bundle or limited no-pass result."""

    decision = validate_post_oof_input(
        oof_dir,
        config_path=config_path,
        expected_config_sha256=expected_config_sha256,
        spec_path=spec_path,
        expected_spec_sha256=expected_spec_sha256,
        execution_amendment_path=execution_amendment_path,
        expected_execution_amendment_sha256=expected_execution_amendment_sha256,
    )
    artifact_name, artifact = _artifact_payload(decision)
    destination = Path(output_dir).expanduser().resolve()
    if destination.exists():
        raise PostOofFinalizationError(f"refusing to replace existing output: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = destination.parent / f".{destination.name}.staging-{uuid.uuid4().hex}"
    stage.mkdir()
    try:
        artifact_path = stage / artifact_name
        _write_json(artifact_path, artifact)
        artifact_file_sha = _file_sha256(artifact_path)
        manifest: dict[str, Any] = {
            "schema_version": MANIFEST_SCHEMA,
            "identity": IDENTITY,
            "status": "atomic_post_oof_decision_admitted",
            "decision_type": decision.decision_type,
            "supported_sides": list(decision.supported_sides),
            "input_bindings": dict(decision.input_bindings),
            "files": [
                {
                    "relative_path": artifact_name,
                    "bytes": artifact_path.stat().st_size,
                    "file_sha256": artifact_file_sha,
                    "canonical_sha256": artifact["canonical_sha256"],
                }
            ],
            "economic_tables_read": [],
            "permissions": dict(LOCKED_PERMISSIONS),
        }
        manifest["canonical_manifest_sha256"] = _canonical_sha256(manifest)
        manifest_path = stage / MANIFEST_NAME
        _write_json(manifest_path, manifest)
        manifest_file_sha = _file_sha256(manifest_path)
        success_path = stage / SUCCESS_NAME
        success_path.write_text(manifest_file_sha + "\n", encoding="ascii")
        with success_path.open("rb") as handle:
            os.fsync(handle.fileno())
        _fsync_directory(stage)
        os.replace(stage, destination)
        _fsync_directory(destination.parent)
        return {
            **manifest,
            "manifest_path": str(destination / MANIFEST_NAME),
            "manifest_file_sha256": manifest_file_sha,
            "success_marker_path": str(destination / SUCCESS_NAME),
            "output": str(destination),
        }
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--oof-dir", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--config-sha256", default=DEFAULT_CONFIG_SHA256)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC_PATH)
    parser.add_argument("--spec-sha256", default=DEFAULT_SPEC_SHA256)
    parser.add_argument("--execution-amendment", type=Path, required=True)
    parser.add_argument("--execution-amendment-sha256", required=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preflight = commands.add_parser("preflight")
    _add_common_arguments(preflight)
    finalize = commands.add_parser("finalize")
    _add_common_arguments(finalize)
    finalize.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    kwargs = {
        "config_path": args.config,
        "expected_config_sha256": args.config_sha256,
        "spec_path": args.spec,
        "expected_spec_sha256": args.spec_sha256,
        "execution_amendment_path": args.execution_amendment,
        "expected_execution_amendment_sha256": args.execution_amendment_sha256,
    }
    if args.command == "preflight":
        result = preflight_post_oof(args.oof_dir, **kwargs)
    else:
        result = finalize_post_oof(args.oof_dir, args.output, **kwargs)
    print(_canonical_json(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CLOSURE_NAME",
    "CONTROL_ACTION",
    "IDENTITY",
    "MANIFEST_NAME",
    "NO_PASS_NAME",
    "POLICY_BUNDLE_NAME",
    "PostOofFinalizationError",
    "finalize_post_oof",
    "preflight_post_oof",
    "validate_post_oof_input",
]
