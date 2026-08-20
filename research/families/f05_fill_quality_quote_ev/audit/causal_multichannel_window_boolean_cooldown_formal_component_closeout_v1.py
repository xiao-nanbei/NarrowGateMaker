#!/usr/bin/env python3
"""Validate, materialize, and compose the completed F05 formal components.

The formal BUY and SELL calculations were executed by different clean source
commits under one unchanged research contract.  This module contains no policy
or replay logic.  It verifies immutable cache entries, publishes embedded OOF
reports and scorecards, materializes BUY through the exact formal-v24 source,
and binds the two side components without pooling or re-estimating them.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

IDENTITY = "f05_full_multiscale_successor_formal_component_closeout_v1"
REPORT_SCHEMA = (
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1."
    "nested_chronological_oof.v1"
)
REPORT_SCOPE = "learning_algorithm_fold_specific_policies"
SCORECARD_SCHEMA = "narrowgate_experiment_scorecard.v1"
SCORE_PROFILE_ID = "action_alpha_v1"
EXPECTED_REPORT_PERMISSIONS = {
    "final_policy_frozen": False,
    "action_authorized": False,
    "live_authorized": False,
    "validation_read": False,
    "sealed_holdout_read": False,
}
EXPECTED_COMPONENT_PERMISSIONS = {
    "validation_read": False,
    "sealed_holdout_read": False,
    "action_authorized": False,
    "live_authorized": False,
}
EXPECTED_STAGE_COUNTS = {
    "outer_train_one_shot": 67,
    "inner_oof": 250,
    "outer_oof": 260,
}
EXPECTED_CACHE_UNITS = sum(EXPECTED_STAGE_COUNTS.values())
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class FormalComponentCloseoutError(RuntimeError):
    """Raised when a component or composition identity fails closed."""


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def document_sha256(value: Mapping[str, Any], field: str) -> str:
    payload = dict(value)
    payload.pop(field, None)
    return canonical_sha256(payload)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _reject_duplicate_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise FormalComponentCloseoutError(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_pairs,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FormalComponentCloseoutError(f"cannot read {label}: {path}") from exc
    if not isinstance(value, dict):
        raise FormalComponentCloseoutError(f"{label} must be a JSON object")
    canonical_sha256(value)
    return value


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
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


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> str:
    encoded = _json_bytes(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != encoded:
            raise FormalComponentCloseoutError(f"immutable JSON drifted: {path}")
        return hashlib.sha256(encoded).hexdigest()
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = handle.name
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if temporary is not None and os.path.exists(temporary):
            os.unlink(temporary)
    return hashlib.sha256(encoded).hexdigest()


def _require_sha(value: Any, *, label: str) -> str:
    normalized = str(value)
    if _SHA_RE.fullmatch(normalized) is None:
        raise FormalComponentCloseoutError(f"{label} is not a SHA256")
    return normalized


def _require_private_write_permissions(path: Path, *, label: str) -> int:
    mode = path.stat().st_mode & 0o777
    if mode & 0o022:
        raise FormalComponentCloseoutError(f"{label} is group/world writable: {path}")
    return mode


def _scorecard_digest(scorecard: Mapping[str, Any], *, label: str) -> str:
    if scorecard.get("schema_version") != SCORECARD_SCHEMA:
        raise FormalComponentCloseoutError(f"{label} scorecard schema drifted")
    profile = scorecard.get("profile")
    if not isinstance(profile, Mapping) or profile.get("profile_id") != SCORE_PROFILE_ID:
        raise FormalComponentCloseoutError(f"{label} score profile drifted")
    profile_payload = dict(profile)
    profile_sha = _require_sha(
        profile_payload.pop("profile_sha256", ""),
        label=f"{label} profile SHA256",
    )
    if profile_payload.pop("frozen_before_outcome", None) is not True:
        raise FormalComponentCloseoutError(f"{label} score profile was not frozen")
    if canonical_sha256(profile_payload) != profile_sha:
        raise FormalComponentCloseoutError(f"{label} score profile hash drifted")
    input_identity = scorecard.get("input_identity")
    if not isinstance(input_identity, Mapping):
        raise FormalComponentCloseoutError(f"{label} input identity is missing")
    if scorecard.get("input_identity_sha256") != canonical_sha256(input_identity):
        raise FormalComponentCloseoutError(f"{label} input identity hash drifted")
    embedded = _require_sha(
        scorecard.get("scorecard_sha256", ""),
        label=f"{label} scorecard SHA256",
    )
    if embedded != document_sha256(scorecard, "scorecard_sha256"):
        raise FormalComponentCloseoutError(f"{label} scorecard hash drifted")
    return embedded


def validate_nested_report(
    report: Mapping[str, Any],
    *,
    expected_side: str,
) -> dict[str, Any]:
    side = str(expected_side).upper()
    if side not in {"BUY", "SELL"}:
        raise FormalComponentCloseoutError("expected report side must be BUY or SELL")
    if (
        report.get("schema_version") != REPORT_SCHEMA
        or report.get("oof_evidence_scope") != REPORT_SCOPE
        or report.get("exact_final_artifact_oof_available") is not False
        or report.get("final_refit_performed") is not False
        or report.get("permissions") != EXPECTED_REPORT_PERMISSIONS
        or int(report.get("outer_fold_count", -1)) != 4
        or int(report.get("outer_oof_row_count", -1)) != 260
    ):
        raise FormalComponentCloseoutError("nested OOF report contract drifted")
    profile = report.get("score_profile_contract")
    if not isinstance(profile, Mapping) or profile.get("profile_id") != SCORE_PROFILE_ID:
        raise FormalComponentCloseoutError("nested OOF score profile drifted")
    candidate_reports = report.get("candidate_reports")
    scorecards = report.get("scorecards")
    if not isinstance(candidate_reports, Mapping) or not isinstance(scorecards, Mapping):
        raise FormalComponentCloseoutError("nested OOF candidate artifacts are missing")
    if set(candidate_reports) != set(scorecards) or len(scorecards) != 13:
        raise FormalComponentCloseoutError("nested OOF candidate set drifted")
    expected_prefix = f"{side}:"
    if any(not str(key).startswith(expected_prefix) for key in scorecards):
        raise FormalComponentCloseoutError("nested OOF report pooled sides")
    scorecard_hashes = {
        str(key): _scorecard_digest(value, label=str(key))
        for key, value in sorted(scorecards.items())
        if isinstance(value, Mapping)
    }
    if len(scorecard_hashes) != len(scorecards):
        raise FormalComponentCloseoutError("nested OOF scorecard payload drifted")
    return {
        "report_canonical_sha256": canonical_sha256(report),
        "scorecard_count": len(scorecard_hashes),
        "scorecard_set_sha256": canonical_sha256(scorecard_hashes),
        "scorecard_sha256": scorecard_hashes,
        "outer_fold_count": 4,
        "outer_oof_row_count": 260,
    }


def publish_nested_report(
    report: Mapping[str, Any],
    *,
    expected_side: str,
    output_dir: Path,
) -> dict[str, Any]:
    validation = validate_nested_report(report, expected_side=expected_side)
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    bindings: dict[str, dict[str, Any]] = {}
    report_path = root / "nested_oof_report.json"
    bindings["nested_oof_report"] = {
        "path": report_path.name,
        "sha256": atomic_write_json(report_path, report),
        "size_bytes": report_path.stat().st_size,
    }
    scorecards = report["scorecards"]
    for key, scorecard in sorted(scorecards.items()):
        safe_key = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(key))
        path = root / f"{safe_key}.scorecard.json"
        bindings[f"scorecard::{key}"] = {
            "path": path.name,
            "sha256": atomic_write_json(path, scorecard),
            "size_bytes": path.stat().st_size,
        }
    manifest: dict[str, Any] = {
        "schema_version": f"{REPORT_SCHEMA}.artifact_manifest.v1",
        "identity": REPORT_SCHEMA,
        "formal_side": str(expected_side).upper(),
        "oof_evidence_scope": REPORT_SCOPE,
        "exact_final_artifact_oof_available": False,
        "bindings": bindings,
        "permissions": {
            "action_authorized": False,
            "live_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    }
    manifest["canonical_manifest_sha256"] = document_sha256(
        manifest, "canonical_manifest_sha256"
    )
    manifest_path = root / "manifest.json"
    manifest_file_sha = atomic_write_json(manifest_path, manifest)
    return {
        **validation,
        "artifact_manifest_canonical_sha256": manifest["canonical_manifest_sha256"],
        "artifact_manifest_file_sha256": manifest_file_sha,
        "artifact_count": len(bindings),
    }


def audit_cache(
    cache_root: Path,
    *,
    execution_manifest_sha256: str,
    expected_side: str,
    expected_stage_counts: Mapping[str, int] = EXPECTED_STAGE_COUNTS,
) -> dict[str, Any]:
    manifest_sha = _require_sha(
        execution_manifest_sha256,
        label="execution manifest SHA256",
    )
    side = str(expected_side).upper()
    root = cache_root.expanduser().resolve()
    progress_root = root / "progress"
    entries_root = root / "entries"
    if not progress_root.is_dir() or not entries_root.is_dir():
        raise FormalComponentCloseoutError("cache roots are missing")
    units: list[dict[str, Any]] = []
    stage_counts: Counter[str] = Counter()
    excluded_states: Counter[str] = Counter()
    for progress_path in sorted(progress_root.glob("*.json")):
        progress = load_json(progress_path, label="cache progress receipt")
        key = progress.get("cache_key")
        if not isinstance(key, Mapping) or key.get("execution_manifest_sha256") != manifest_sha:
            continue
        key_side = str(key.get("side", "")).upper()
        if key_side != side:
            excluded_states[f"{key_side}:{progress.get('state')}"] += 1
            continue
        key_sha = _require_sha(
            progress.get("cache_key_sha256", ""),
            label="cache key SHA256",
        )
        if key_sha != progress_path.stem:
            raise FormalComponentCloseoutError("progress filename does not match cache key")
        if progress.get("state") != "complete":
            raise FormalComponentCloseoutError("selected cache unit is not complete")
        if progress.get("receipt_sha256") != document_sha256(progress, "receipt_sha256"):
            raise FormalComponentCloseoutError("progress receipt hash drifted")
        progress_mode = _require_private_write_permissions(
            progress_path,
            label="cache progress receipt",
        )
        entry_path = entries_root / key_sha / "manifest.json"
        if not entry_path.is_file():
            raise FormalComponentCloseoutError("complete cache entry is missing")
        entry = load_json(entry_path, label="cache entry manifest")
        if (
            entry.get("cache_key_sha256") != key_sha
            or entry.get("cache_key") != dict(key)
            or entry.get("complete") is not True
            or entry.get("atomic_admission") is not True
            or entry.get("receipt_sha256") != document_sha256(entry, "receipt_sha256")
        ):
            raise FormalComponentCloseoutError("cache entry identity drifted")
        entry_schema = str(entry.get("schema_version", ""))
        if canonical_sha256({"schema_version": entry_schema, **dict(key)}) != key_sha:
            raise FormalComponentCloseoutError("cache key canonical hash drifted")
        entry_mode = _require_private_write_permissions(
            entry_path,
            label="cache entry manifest",
        )
        files = entry.get("files")
        if not isinstance(files, Mapping) or not files:
            raise FormalComponentCloseoutError("cache entry has no payload files")
        file_bindings: dict[str, dict[str, Any]] = {}
        for name, raw_binding in sorted(files.items()):
            if not isinstance(raw_binding, Mapping):
                raise FormalComponentCloseoutError("cache file binding is malformed")
            relative = Path(str(raw_binding.get("file", "")))
            if relative.is_absolute() or ".." in relative.parts:
                raise FormalComponentCloseoutError("cache file binding escaped its entry")
            path = entry_path.parent / relative
            expected_sha = _require_sha(
                raw_binding.get("sha256", ""),
                label="cache payload SHA256",
            )
            if not path.is_file() or file_sha256(path) != expected_sha:
                raise FormalComponentCloseoutError("cache payload hash drifted")
            if "size_bytes" in raw_binding and path.stat().st_size != int(
                raw_binding["size_bytes"]
            ):
                raise FormalComponentCloseoutError("cache payload size drifted")
            mode = _require_private_write_permissions(path, label="cache payload")
            file_bindings[str(name)] = {
                "file": relative.as_posix(),
                "sha256": expected_sha,
                "size_bytes": path.stat().st_size,
                "mode": format(mode, "04o"),
            }
        stage = str(key.get("stage", ""))
        stage_counts[stage] += 1
        units.append(
            {
                "cache_key_sha256": key_sha,
                "stage": stage,
                "fold_id": str(key.get("fold_id", "")),
                "utc_day": str(key.get("utc_day", "")),
                "progress_receipt_sha256": progress["receipt_sha256"],
                "cache_receipt_sha256": entry["receipt_sha256"],
                "progress_mode": format(progress_mode, "04o"),
                "entry_manifest_mode": format(entry_mode, "04o"),
                "files": file_bindings,
            }
        )
    normalized_counts = dict(sorted(stage_counts.items()))
    required_counts = {
        str(key): int(value) for key, value in sorted(expected_stage_counts.items())
    }
    if normalized_counts != required_counts or len(units) != sum(required_counts.values()):
        raise FormalComponentCloseoutError(
            f"cache stage counts drifted: {normalized_counts} != {required_counts}"
        )
    receipt: dict[str, Any] = {
        "schema_version": f"{IDENTITY}.cache_audit.v1",
        "identity": f"{IDENTITY}:{side.lower()}_cache_audit",
        "status": "passed_all_selected_cache_units_complete_and_hash_valid",
        "execution_manifest_sha256": manifest_sha,
        "formal_side": side,
        "complete_cache_units": len(units),
        "stage_counts": normalized_counts,
        "excluded_execution_states": dict(sorted(excluded_states.items())),
        "cache_unit_set_sha256": canonical_sha256(units),
        "cache_units": units,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_cache_audit_sha256"] = document_sha256(
        receipt, "canonical_cache_audit_sha256"
    )
    return receipt


def validate_component_result(
    *,
    manifest_path: Path,
    result_path: Path,
    cache_root: Path,
    expected_side: str,
    expected_manifest_sha256: str,
    expected_commit: str,
    expected_tag: str,
    canonical_result_field: str = "canonical_result_sha256",
) -> dict[str, Any]:
    manifest = load_json(manifest_path, label="formal execution manifest")
    result = load_json(result_path, label="formal component result")
    expected_sha = _require_sha(
        expected_manifest_sha256,
        label="expected execution manifest SHA256",
    )
    if (
        manifest.get("canonical_execution_manifest_sha256") != expected_sha
        or document_sha256(manifest, "canonical_execution_manifest_sha256") != expected_sha
        or manifest.get("public_base_commit") != expected_commit
        or manifest.get("annotated_tag") != expected_tag
        or manifest.get("permissions") != EXPECTED_COMPONENT_PERMISSIONS
    ):
        raise FormalComponentCloseoutError("formal execution manifest drifted")
    embedded_result_sha = _require_sha(
        result.get(canonical_result_field, ""),
        label="component result SHA256",
    )
    if embedded_result_sha != document_sha256(result, canonical_result_field):
        raise FormalComponentCloseoutError("formal component result hash drifted")
    side = str(expected_side).upper()
    if (
        result.get("execution_manifest_sha256") != expected_sha
        or result.get("formal_sides") != [side]
        or result.get("repeated_sequential_policy") is not True
        or result.get("one_shot_effect_aggregation_used") is not False
        or result.get("validation_read") is not False
        or result.get("sealed_holdout_read") is not False
        or result.get("permissions") != EXPECTED_COMPONENT_PERMISSIONS
    ):
        raise FormalComponentCloseoutError("formal component result contract drifted")
    report = result.get("nested_oof_report")
    if not isinstance(report, Mapping):
        raise FormalComponentCloseoutError("formal component has no nested OOF report")
    report_validation = validate_nested_report(report, expected_side=side)
    cache_audit = audit_cache(
        cache_root,
        execution_manifest_sha256=expected_sha,
        expected_side=side,
    )
    manifest_mode = _require_private_write_permissions(
        manifest_path,
        label="formal execution manifest",
    )
    result_mode = _require_private_write_permissions(
        result_path,
        label="formal component result",
    )
    receipt: dict[str, Any] = {
        "schema_version": f"{IDENTITY}.component_validation.v1",
        "identity": f"{IDENTITY}:{side.lower()}_component_validation",
        "status": "passed_exact_component_result_report_scorecards_and_cache",
        "formal_side": side,
        "source_execution": {
            "execution_manifest_sha256": expected_sha,
            "public_base_commit": expected_commit,
            "annotated_tag": expected_tag,
        },
        "execution_manifest": {
            "file_sha256": file_sha256(manifest_path),
            "mode": format(manifest_mode, "04o"),
        },
        "component_result": {
            "canonical_sha256": embedded_result_sha,
            "file_sha256": file_sha256(result_path),
            "mode": format(result_mode, "04o"),
        },
        "nested_oof": report_validation,
        "cache_audit": {
            "canonical_cache_audit_sha256": cache_audit[
                "canonical_cache_audit_sha256"
            ],
            "cache_unit_set_sha256": cache_audit["cache_unit_set_sha256"],
            "complete_cache_units": cache_audit["complete_cache_units"],
            "stage_counts": cache_audit["stage_counts"],
            "excluded_execution_states": cache_audit["excluded_execution_states"],
        },
        "permissions": dict(EXPECTED_COMPONENT_PERMISSIONS),
    }
    receipt["canonical_validation_receipt_sha256"] = document_sha256(
        receipt, "canonical_validation_receipt_sha256"
    )
    return receipt


def audit_and_publish_component(
    *,
    manifest_path: Path,
    result_path: Path,
    cache_root: Path,
    output_dir: Path,
    expected_side: str,
    expected_manifest_sha256: str,
    expected_commit: str,
    expected_tag: str,
    canonical_result_field: str = "canonical_result_sha256",
) -> dict[str, Any]:
    receipt = validate_component_result(
        manifest_path=manifest_path,
        result_path=result_path,
        cache_root=cache_root,
        expected_side=expected_side,
        expected_manifest_sha256=expected_manifest_sha256,
        expected_commit=expected_commit,
        expected_tag=expected_tag,
        canonical_result_field=canonical_result_field,
    )
    result = load_json(result_path, label="formal component result")
    published = publish_nested_report(
        result["nested_oof_report"],
        expected_side=expected_side,
        output_dir=output_dir,
    )
    cache_audit = audit_cache(
        cache_root,
        execution_manifest_sha256=expected_manifest_sha256,
        expected_side=expected_side,
    )
    cache_path = output_dir / "cache_audit_receipt.json"
    validation_path = output_dir / "formal_component_validation_receipt.json"
    atomic_write_json(cache_path, cache_audit)
    atomic_write_json(validation_path, receipt)
    output = {
        **receipt,
        "published_nested_oof": published,
        "cache_audit_file_sha256": file_sha256(cache_path),
        "validation_receipt_file_sha256": file_sha256(validation_path),
    }
    return output


def _git_output(source_root: Path, *args: str) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(source_root), *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise FormalComponentCloseoutError("cannot verify exact source checkout") from exc
    return completed.stdout.strip()


def _unexpected_replay(*_args: Any, **_kwargs: Any) -> Any:
    raise FormalComponentCloseoutError(
        "cache-only BUY materialization attempted fresh replay"
    )


def _preload_cpp_extension(path: Path, *, expected_sha256: str) -> None:
    extension = path.expanduser().resolve()
    expected = _require_sha(expected_sha256, label="C++ extension SHA256")
    if not extension.is_file() or file_sha256(extension) != expected:
        raise FormalComponentCloseoutError("exact C++ extension bytes drifted")
    spec = importlib.util.spec_from_file_location("narrowgate_cpp", extension)
    if spec is None or spec.loader is None:
        raise FormalComponentCloseoutError("exact C++ extension cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules["narrowgate_cpp"] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop("narrowgate_cpp", None)
        raise
    loaded_path = Path(str(getattr(module, "__file__", ""))).resolve()
    if loaded_path != extension or file_sha256(loaded_path) != expected:
        raise FormalComponentCloseoutError("loaded C++ extension identity drifted")


def materialize_v24_buy(
    *,
    source_root: Path,
    cpp_extension_path: Path,
    expected_cpp_extension_sha256: str,
    execution_manifest_path: Path,
    cache_root: Path,
    output_dir: Path,
    expected_manifest_sha256: str,
    expected_commit: str,
    expected_tag: str,
) -> dict[str, Any]:
    source = source_root.expanduser().resolve()
    if _git_output(source, "rev-parse", "HEAD") != expected_commit:
        raise FormalComponentCloseoutError("exact BUY source commit drifted")
    if _git_output(source, "status", "--porcelain"):
        raise FormalComponentCloseoutError("exact BUY source checkout is dirty")
    _preload_cpp_extension(
        cpp_extension_path,
        expected_sha256=expected_cpp_extension_sha256,
    )

    from research.families.f05_fill_quality_quote_ev.audit import (  # noqa: PLC0415
        causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
    )
    from research.families.f05_fill_quality_quote_ev.audit import (  # noqa: PLC0415
        causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_orchestrator_v1 as orchestrator,
    )
    from research.families.f05_fill_quality_quote_ev.audit import (  # noqa: PLC0415
        causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_repeated_policy_backend_v1 as backend,
    )
    from research.families.f05_fill_quality_quote_ev.audit import (  # noqa: PLC0415
        causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_replay_adapter_v1 as adapter_module,
    )
    from research.families.f05_fill_quality_quote_ev.audit import (  # noqa: PLC0415
        causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
    )

    imported = (
        Path(nested.__file__).resolve(),
        Path(backend.__file__).resolve(),
        Path(adapter_module.__file__).resolve(),
        Path(offline.__file__).resolve(),
        Path(orchestrator.__file__).resolve(),
    )
    if any(source not in path.parents for path in imported):
        raise FormalComponentCloseoutError("BUY materializer imported non-v24 source")

    pre_cache = audit_cache(
        cache_root,
        execution_manifest_sha256=expected_manifest_sha256,
        expected_side="BUY",
    )
    bundle = orchestrator.load_formal_offline_bundle(execution_manifest_path)
    if (
        bundle.execution_manifest.get("canonical_execution_manifest_sha256")
        != expected_manifest_sha256
        or bundle.execution_manifest.get("public_base_commit") != expected_commit
        or bundle.execution_manifest.get("annotated_tag") != expected_tag
    ):
        raise FormalComponentCloseoutError("BUY formal bundle identity drifted")

    adapter = backend._load_canonical_replay_adapter(bundle)
    schema_preflight = backend._preflight_bound_panel_schema(bundle, adapter)
    if schema_preflight.get("status") != backend.FORMAL_PANEL_SCHEMA_READY_STATUS:
        raise FormalComponentCloseoutError("BUY panel schema preflight failed")
    mechanics = backend.load_outcome_blind_mechanics(bundle)
    preflight = backend._preflight_adapter(mechanics, adapter)
    if preflight.get("status") != backend.MECHANICS_READY_STATUS:
        raise FormalComponentCloseoutError("BUY adapter preflight failed")

    guarded_names = (
        "run_global_one_shot_day_jobs",
        "run_global_policy_day_jobs",
        "_run_global_b0_control_jobs",
        "_run_day_jobs",
    )
    original_runners = {
        name: getattr(adapter_module, name)
        for name in guarded_names
        if hasattr(adapter_module, name)
    }
    for name in original_runners:
        setattr(adapter_module, name, _unexpected_replay)
    try:
        ladder, continuous = adapter.build_search_contract(mechanics)
        provider = backend.CanonicalFoldScopedLabelProvider(mechanics, adapter)
        evaluator = backend.CanonicalSequentialEvaluator(mechanics, adapter)
        result = nested.run_nested_chronological_oof(
            mechanics.panel,
            fold_manifest=mechanics.fold_manifest,
            ladder=ladder,
            continuous=continuous,
            evaluator=evaluator,
            label_provider=provider,
            config=nested.NestedOofConfig(
                sides=("BUY",),
                panel_role=offline.PANEL_ROLE,
                earliest_eligible_day=None,
            ),
        )
    finally:
        for name, runner in original_runners.items():
            setattr(adapter_module, name, runner)

    post_cache = audit_cache(
        cache_root,
        execution_manifest_sha256=expected_manifest_sha256,
        expected_side="BUY",
    )
    if pre_cache["canonical_cache_audit_sha256"] != post_cache[
        "canonical_cache_audit_sha256"
    ]:
        raise FormalComponentCloseoutError("BUY cache changed during materialization")
    report = result.report()
    report_validation = validate_nested_report(report, expected_side="BUY")
    component: dict[str, Any] = {
        "schema_version": f"{IDENTITY}.buy_component.v1",
        "identity": f"{IDENTITY}:formal_v24_buy_component",
        "status": "buy_learning_algorithm_nested_oof_materialized_from_exact_v24_cache",
        "component_scope": "buy_only_learning_algorithm_oof_from_formal_v24",
        "formal_sides": ["BUY"],
        "execution_manifest_sha256": mechanics.bindings.execution_manifest_sha256,
        "source_manifest_sha256": mechanics.bindings.source_manifest_sha256,
        "panel_manifest_sha256": mechanics.bindings.panel_manifest_sha256,
        "fold_manifest_sha256": mechanics.bindings.fold_manifest_sha256,
        "nested_fold_manifest_sha256": mechanics.bindings.nested_fold_manifest_sha256,
        "mechanics_receipt_sha256": mechanics.mechanics_receipt_sha256,
        "exact_owner_policy_sha256": mechanics.bindings.exact_owner_policy_sha256,
        "exact_owner_predicate_bundle_sha256": (
            mechanics.bindings.exact_owner_predicate_bundle_sha256
        ),
        "exact_owner_private_config_sha256": (
            mechanics.bindings.exact_owner_private_config_sha256
        ),
        "canonical_replay_adapter_identity": adapter.identity,
        "canonical_replay_adapter_sha256": adapter.artifact_sha256,
        "source_execution": {
            "public_base_commit": expected_commit,
            "annotated_tag": expected_tag,
        },
        "materialization_contract": {
            "cache_only": True,
            "fresh_replay_allowed": False,
            "fresh_replay_used": False,
            "cross_execution_strategy_cache_reuse_used": False,
            "complete_cache_units": EXPECTED_CACHE_UNITS,
            "cache_unit_set_sha256": post_cache["cache_unit_set_sha256"],
            "materializer_artifact_sha256": file_sha256(Path(__file__).resolve()),
            "cpp_extension_sha256": _require_sha(
                expected_cpp_extension_sha256,
                label="C++ extension SHA256",
            ),
            "exact_source_module_sha256": {
                str(path.relative_to(source)): file_sha256(path) for path in imported
            },
        },
        "label_replay_receipts": list(provider.receipts),
        "sequential_replay_receipts": list(evaluator.receipts),
        "nested_oof_report": report,
        "nested_oof_report_sha256": report_validation["report_canonical_sha256"],
        "repeated_sequential_policy": True,
        "one_shot_effect_aggregation_used": False,
        "economic_outcomes_read": True,
        "validation_read": False,
        "sealed_holdout_read": False,
        "permissions": dict(EXPECTED_COMPONENT_PERMISSIONS),
    }
    component["canonical_component_result_sha256"] = document_sha256(
        component, "canonical_component_result_sha256"
    )

    destination = output_dir.expanduser().resolve()
    if destination.exists():
        raise FormalComponentCloseoutError("immutable BUY component output already exists")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent)
    )
    try:
        published = publish_nested_report(
            report,
            expected_side="BUY",
            output_dir=staging,
        )
        cache_path = staging / "cache_audit_receipt.json"
        component_path = staging / "component_result.json"
        folds_path = staging / "fold_records.json"
        rows_path = staging / "outer_oof_rows.parquet"
        atomic_write_json(cache_path, post_cache)
        atomic_write_json(component_path, component)
        atomic_write_json(folds_path, {"fold_records": list(result.fold_records)})
        result.oof_rows.to_parquet(rows_path, index=True)
        os.chmod(rows_path, 0o600)
        bindings = {
            path.name: {
                "sha256": file_sha256(path),
                "size_bytes": path.stat().st_size,
                "mode": format(path.stat().st_mode & 0o777, "04o"),
            }
            for path in sorted(staging.iterdir())
            if path.is_file()
        }
        artifact_manifest: dict[str, Any] = {
            "schema_version": f"{IDENTITY}.component_artifact_manifest.v1",
            "identity": f"{IDENTITY}:formal_v24_buy_component_artifacts",
            "formal_side": "BUY",
            "source_execution_manifest_sha256": expected_manifest_sha256,
            "component_result_canonical_sha256": component[
                "canonical_component_result_sha256"
            ],
            "nested_oof_artifact_manifest_canonical_sha256": published[
                "artifact_manifest_canonical_sha256"
            ],
            "bindings": bindings,
            "permissions": dict(EXPECTED_COMPONENT_PERMISSIONS),
        }
        artifact_manifest["canonical_artifact_manifest_sha256"] = document_sha256(
            artifact_manifest, "canonical_artifact_manifest_sha256"
        )
        atomic_write_json(staging / "component_artifact_manifest.json", artifact_manifest)
        os.replace(staging, destination)
    finally:
        if staging.exists():
            for path in sorted(staging.glob("**/*"), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            staging.rmdir()
    return {
        "status": component["status"],
        "component_result_canonical_sha256": component[
            "canonical_component_result_sha256"
        ],
        "cache_audit_canonical_sha256": post_cache[
            "canonical_cache_audit_sha256"
        ],
        "cache_unit_set_sha256": post_cache["cache_unit_set_sha256"],
        "complete_cache_units": post_cache["complete_cache_units"],
        "stage_counts": post_cache["stage_counts"],
        "nested_oof": report_validation,
        "output_dir": str(destination),
    }


def compose_components(
    *,
    buy_result_path: Path,
    buy_validation_path: Path,
    sell_result_path: Path,
    sell_validation_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    buy = load_json(buy_result_path, label="BUY component result")
    sell = load_json(sell_result_path, label="SELL component result")
    buy_validation = load_json(buy_validation_path, label="BUY validation receipt")
    sell_validation = load_json(sell_validation_path, label="SELL validation receipt")
    buy_sha = _require_sha(
        buy.get("canonical_component_result_sha256", ""),
        label="BUY component result SHA256",
    )
    sell_sha = _require_sha(
        sell.get("canonical_result_sha256", ""),
        label="SELL component result SHA256",
    )
    if buy_sha != document_sha256(buy, "canonical_component_result_sha256"):
        raise FormalComponentCloseoutError("BUY component result drifted")
    if sell_sha != document_sha256(sell, "canonical_result_sha256"):
        raise FormalComponentCloseoutError("SELL component result drifted")
    if buy.get("formal_sides") != ["BUY"] or sell.get("formal_sides") != ["SELL"]:
        raise FormalComponentCloseoutError("component side identity drifted")
    shared_fields = (
        "source_manifest_sha256",
        "panel_manifest_sha256",
        "fold_manifest_sha256",
        "nested_fold_manifest_sha256",
        "exact_owner_policy_sha256",
        "exact_owner_predicate_bundle_sha256",
        "exact_owner_private_config_sha256",
    )
    shared_bindings: dict[str, str] = {}
    for field in shared_fields:
        if buy.get(field) != sell.get(field):
            raise FormalComponentCloseoutError(f"cross-component binding drifted: {field}")
        shared_bindings[field] = _require_sha(buy.get(field, ""), label=field)
    validate_nested_report(buy["nested_oof_report"], expected_side="BUY")
    validate_nested_report(sell["nested_oof_report"], expected_side="SELL")
    if (
        buy_validation.get("canonical_validation_receipt_sha256")
        != document_sha256(buy_validation, "canonical_validation_receipt_sha256")
        or sell_validation.get("canonical_validation_receipt_sha256")
        != document_sha256(sell_validation, "canonical_validation_receipt_sha256")
    ):
        raise FormalComponentCloseoutError("component validation receipt drifted")
    if buy_validation.get("formal_side") != "BUY" or sell_validation.get(
        "formal_side"
    ) != "SELL":
        raise FormalComponentCloseoutError("component validation side drifted")
    receipt: dict[str, Any] = {
        "schema_version": f"{IDENTITY}.cross_commit_composition.v1",
        "identity": f"{IDENTITY}:buy_v24_sell_repaired_composition",
        "status": "passed_separate_side_composition_without_reestimation",
        "composition_semantics": {
            "operation": "bind_two_precomputed_side_components",
            "pooled_training": False,
            "economic_reestimation": False,
            "cross_execution_strategy_cache_reuse": False,
            "combined_policy_frozen": False,
            "component_results_remain_authoritative": True,
        },
        "components": {
            "BUY": {
                "result_canonical_sha256": buy_sha,
                "result_file_sha256": file_sha256(buy_result_path),
                "execution_manifest_sha256": buy["execution_manifest_sha256"],
                "public_base_commit": buy["source_execution"]["public_base_commit"],
                "annotated_tag": buy["source_execution"]["annotated_tag"],
                "validation_receipt_sha256": buy_validation[
                    "canonical_validation_receipt_sha256"
                ],
            },
            "SELL": {
                "result_canonical_sha256": sell_sha,
                "result_file_sha256": file_sha256(sell_result_path),
                "execution_manifest_sha256": sell["execution_manifest_sha256"],
                "public_base_commit": sell_validation["source_execution"][
                    "public_base_commit"
                ],
                "annotated_tag": sell_validation["source_execution"]["annotated_tag"],
                "validation_receipt_sha256": sell_validation[
                    "canonical_validation_receipt_sha256"
                ],
            },
        },
        "shared_research_bindings": shared_bindings,
        "evidence_boundary": {
            "panel_role": "Development",
            "exact_final_artifact_oof_available": False,
            "final_refit_performed": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
        "permissions": dict(EXPECTED_COMPONENT_PERMISSIONS),
    }
    receipt["canonical_composition_receipt_sha256"] = document_sha256(
        receipt, "canonical_composition_receipt_sha256"
    )
    atomic_write_json(output_path, receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    audit = subparsers.add_parser("audit-and-publish")
    audit.add_argument("--manifest", type=Path, required=True)
    audit.add_argument("--result", type=Path, required=True)
    audit.add_argument("--cache-root", type=Path, required=True)
    audit.add_argument("--output-dir", type=Path, required=True)
    audit.add_argument("--side", choices=("BUY", "SELL"), required=True)
    audit.add_argument("--manifest-sha256", required=True)
    audit.add_argument("--commit", required=True)
    audit.add_argument("--tag", required=True)
    audit.add_argument("--canonical-result-field", default="canonical_result_sha256")

    materialize = subparsers.add_parser("materialize-v24-buy")
    materialize.add_argument("--source-root", type=Path, required=True)
    materialize.add_argument("--cpp-extension", type=Path, required=True)
    materialize.add_argument("--cpp-extension-sha256", required=True)
    materialize.add_argument("--manifest", type=Path, required=True)
    materialize.add_argument("--cache-root", type=Path, required=True)
    materialize.add_argument("--output-dir", type=Path, required=True)
    materialize.add_argument("--manifest-sha256", required=True)
    materialize.add_argument("--commit", required=True)
    materialize.add_argument("--tag", required=True)

    compose = subparsers.add_parser("compose")
    compose.add_argument("--buy-result", type=Path, required=True)
    compose.add_argument("--buy-validation", type=Path, required=True)
    compose.add_argument("--sell-result", type=Path, required=True)
    compose.add_argument("--sell-validation", type=Path, required=True)
    compose.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "audit-and-publish":
        payload = audit_and_publish_component(
            manifest_path=args.manifest,
            result_path=args.result,
            cache_root=args.cache_root,
            output_dir=args.output_dir,
            expected_side=args.side,
            expected_manifest_sha256=args.manifest_sha256,
            expected_commit=args.commit,
            expected_tag=args.tag,
            canonical_result_field=args.canonical_result_field,
        )
    elif args.command == "materialize-v24-buy":
        payload = materialize_v24_buy(
            source_root=args.source_root,
            cpp_extension_path=args.cpp_extension,
            expected_cpp_extension_sha256=args.cpp_extension_sha256,
            execution_manifest_path=args.manifest,
            cache_root=args.cache_root,
            output_dir=args.output_dir,
            expected_manifest_sha256=args.manifest_sha256,
            expected_commit=args.commit,
            expected_tag=args.tag,
        )
    else:
        payload = compose_components(
            buy_result_path=args.buy_result,
            buy_validation_path=args.buy_validation,
            sell_result_path=args.sell_result,
            sell_validation_path=args.sell_validation,
            output_path=args.output,
        )
    print(json.dumps(payload, sort_keys=True, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
