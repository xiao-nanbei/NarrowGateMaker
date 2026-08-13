#!/usr/bin/env python3
"""Canonical, non-injectable formal entry for the offline F05 successor.

The command accepts one hash-bound execution manifest.  Its prebound panel is
strictly outcome-blind mechanics: economic labels must be generated inside each
outer-train fold by the fixed backend.  It never accepts a DataFrame, handwritten
fold, evaluator callback, day list, or one-shot result.  Source admission may be
audited in a dirty worktree, but formal economics is allowed only from the clean
annotated tag frozen in the execution manifest.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import re
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from data_paths import resolve_portable_path
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_mechanics_v1 as mechanics,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)

IDENTITY = f"{offline.IDENTITY}.formal_orchestrator_v1"
SCHEMA_VERSION = f"{IDENTITY}.execution_manifest.v1"
PANEL_SCHEMA_VERSION = f"{offline.IDENTITY}.nested_oof_panel_manifest.v1"
CANONICAL_BACKEND_MODULE = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
    "offline_repeated_policy_backend_v1"
)
CANONICAL_BACKEND_FUNCTION = "run_canonical_offline_economics"
FORMAL_RESULT_SCHEMA = f"{IDENTITY}.formal_result.v1"

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_TAG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,199}$")
_PANEL_FILES = (
    "metadata",
    "boolean_features",
    "continuous_features",
    "exact_owner_actions",
    "replay_inputs",
)
_FORBIDDEN_ECONOMIC_PANEL_FILES = frozenset(
    {
        "action_outcomes",
        "action_supported",
        "economic_outcomes",
        "one_shot_training_labels",
    }
)


class OfflineOrchestratorError(RuntimeError):
    """Raised when formal economics can be bypassed or its identity drifts."""


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _document_sha256(value: Mapping[str, Any], field: str) -> str:
    payload = dict(value)
    payload.pop(field, None)
    return _canonical_sha256(payload)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OfflineOrchestratorError(f"cannot load {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise OfflineOrchestratorError(f"{label} root must be an object")
    return payload


def _resolve_bound_file(
    binding: Mapping[str, Any],
    *,
    label: str,
    repository_root: Path,
) -> Path:
    if not {"path", "sha256"} <= set(binding):
        raise OfflineOrchestratorError(f"{label} binding is incomplete")
    digest = str(binding.get("sha256"))
    if _SHA_RE.fullmatch(digest) is None:
        raise OfflineOrchestratorError(f"{label} SHA256 is invalid")
    try:
        path = resolve_portable_path(
            str(binding.get("path")),
            root=repository_root,
        ).expanduser().resolve()
    except (RuntimeError, ValueError) as exc:
        raise OfflineOrchestratorError(f"{label} path is not portable") from exc
    if not path.is_file() or _file_sha256(path) != digest:
        raise OfflineOrchestratorError(f"{label} file hash drifted")
    if "size_bytes" in binding and int(binding["size_bytes"]) != path.stat().st_size:
        raise OfflineOrchestratorError(f"{label} file size drifted")
    return path


def _portable_path(path: Path, *, repository_root: Path) -> str:
    """Encode a formal binding without publishing a machine-specific locator."""

    roots = mechanics.PortableRoots.from_layout(
        offline.default_layout(),
        repository_root=repository_root,
    )
    resolved = path.expanduser().resolve()
    for marker, configured_root in roots.marker_roots():
        try:
            relative = resolved.relative_to(configured_root)
        except ValueError:
            continue
        return marker if not relative.parts else f"{marker}/{relative.as_posix()}"
    raise OfflineOrchestratorError(
        "formal binding lies outside the governed portable roots"
    )


def _binding(path: Path, *, repository_root: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise OfflineOrchestratorError(f"formal bound file is missing: {resolved}")
    return {
        "path": _portable_path(resolved, repository_root=repository_root),
        "sha256": _file_sha256(resolved),
        "size_bytes": int(resolved.stat().st_size),
    }


def _git(*args: str, root: Path) -> str:
    try:
        result = subprocess.run(
            ("git", *args),
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise OfflineOrchestratorError(f"git identity probe failed: {' '.join(args)}") from exc
    return result.stdout.strip()


def _validate_clean_annotated_tag(
    *,
    repository_root: Path,
    commit_sha: str,
    tag: str,
) -> None:
    # Git SHA-1 repositories still expose 40 characters.  The manifest stores the
    # exact native object id and validates it separately from artifact SHA256.
    if len(commit_sha) not in {40, 64} or re.fullmatch(r"[0-9a-f]+", commit_sha) is None:
        raise OfflineOrchestratorError("public base commit is malformed")
    if _TAG_RE.fullmatch(tag) is None:
        raise OfflineOrchestratorError("formal execution tag is malformed")
    if _git("status", "--porcelain", root=repository_root):
        raise OfflineOrchestratorError("formal economics require a clean worktree")
    head = _git("rev-parse", "HEAD", root=repository_root)
    if head != commit_sha:
        raise OfflineOrchestratorError("HEAD drifted from the formal execution commit")
    if _git("cat-file", "-t", f"refs/tags/{tag}", root=repository_root) != "tag":
        raise OfflineOrchestratorError("formal execution requires an annotated tag")
    if _git("rev-parse", f"refs/tags/{tag}^{{}}", root=repository_root) != head:
        raise OfflineOrchestratorError("formal execution tag does not identify HEAD")


@dataclass(frozen=True, slots=True, init=False)
class FormalOfflineBundle:
    execution_manifest_path: Path
    execution_manifest: Mapping[str, Any]
    source_manifest_path: Path
    source_manifest: Mapping[str, Any]
    panel_manifest_path: Path
    panel_manifest: Mapping[str, Any]
    panel_files: Mapping[str, Path]
    repository_root: Path


def _new_formal_offline_bundle(
    *,
    execution_manifest_path: Path,
    execution_manifest: Mapping[str, Any],
    source_manifest_path: Path,
    source_manifest: Mapping[str, Any],
    panel_manifest_path: Path,
    panel_manifest: Mapping[str, Any],
    panel_files: Mapping[str, Path],
    repository_root: Path,
) -> FormalOfflineBundle:
    """Construct a bundle only after the strict loader validates every binding."""

    bundle = object.__new__(FormalOfflineBundle)
    values = {
        "execution_manifest_path": execution_manifest_path,
        "execution_manifest": execution_manifest,
        "source_manifest_path": source_manifest_path,
        "source_manifest": source_manifest,
        "panel_manifest_path": panel_manifest_path,
        "panel_manifest": panel_manifest,
        "panel_files": panel_files,
        "repository_root": repository_root,
    }
    for field, value in values.items():
        object.__setattr__(bundle, field, value)
    return bundle


def _validate_panel_manifest(
    panel: Mapping[str, Any],
    *,
    source: Mapping[str, Any],
    repository_root: Path,
) -> dict[str, Path]:
    if panel.get("schema_version") != PANEL_SCHEMA_VERSION:
        raise OfflineOrchestratorError("canonical nested-OOF panel schema drifted")
    if panel.get("identity") != offline.IDENTITY:
        raise OfflineOrchestratorError("canonical panel identity drifted")
    if panel.get("canonical_panel_manifest_sha256") != _document_sha256(
        panel, "canonical_panel_manifest_sha256"
    ):
        raise OfflineOrchestratorError("canonical panel manifest hash drifted")
    if panel.get("source_manifest_sha256") != source.get("canonical_manifest_sha256"):
        raise OfflineOrchestratorError("panel is not bound to the admitted source manifest")
    if tuple(panel.get("selected_days") or ()) != tuple(source.get("selected_days") or ()):
        raise OfflineOrchestratorError("panel day order drifted from source admission")
    if panel.get("panel_role") != offline.PANEL_ROLE:
        raise OfflineOrchestratorError("panel role is not family-specific historical Development")
    if panel.get("mechanics_identity") != mechanics.MECHANICS_IDENTITY:
        raise OfflineOrchestratorError("canonical panel mechanics identity drifted")
    if panel.get("source_authority") != mechanics.SOURCE_AUTHORITY:
        raise OfflineOrchestratorError("canonical panel source authority drifted")
    if panel.get("queue_identity") != offline.QUEUE_IDENTITY:
        raise OfflineOrchestratorError("panel queue identity drifted")
    if panel.get("exact_queue_policy_eligible") is not False:
        raise OfflineOrchestratorError("modeled-queue panel claimed exact queue authority")
    if panel.get("same_millisecond_ambiguity_policy") != "censor":
        raise OfflineOrchestratorError("same-millisecond ambiguity is not censored")
    if panel.get("economic_outcomes_present") is not False:
        raise OfflineOrchestratorError(
            "canonical panel must declare economic_outcomes_present=false"
        )
    if panel.get("one_shot_training_labels_precomputed") is not False:
        raise OfflineOrchestratorError(
            "canonical panel must declare one_shot_training_labels_precomputed=false"
        )
    if panel.get("outer_train_label_generation_required") is not True:
        raise OfflineOrchestratorError(
            "canonical panel must declare outer_train_label_generation_required=true"
        )
    if panel.get("one_shot_effect_aggregation_used") is not False:
        raise OfflineOrchestratorError("one-shot effects cannot enter formal policy economics")
    if panel.get("repeated_sequential_policy_required") is not True:
        raise OfflineOrchestratorError("panel does not require repeated sequential policy replay")
    if panel.get("validation_read") is not False or panel.get("sealed_holdout_read") is not False:
        raise OfflineOrchestratorError("Validation or sealed holdout entered the panel")
    if panel.get("exact_current_owner_policy_sha256") != offline.ACTIVE_OWNER_POLICY_SHA256:
        raise OfflineOrchestratorError("panel control is not exact current owner B0")
    if panel.get("exact_current_predicate_bundle_sha256") != offline.ACTIVE_PREDICATE_BUNDLE_SHA256:
        raise OfflineOrchestratorError("panel owner predicate identity drifted")
    if panel.get("exact_current_private_config_sha256") != offline.ACTIVE_PRIVATE_CONFIG_SHA256:
        raise OfflineOrchestratorError("panel owner private-config identity drifted")
    if panel.get("permissions") != {
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }:
        raise OfflineOrchestratorError("canonical panel permissions drifted")
    owner_artifacts = panel.get("owner_artifacts")
    expected_owner = {
        "policy": offline.ACTIVE_OWNER_POLICY_SHA256,
        "predicate_bundle": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "private_config": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
    }
    if not isinstance(owner_artifacts, Mapping) or set(owner_artifacts) != set(expected_owner):
        raise OfflineOrchestratorError("canonical panel owner artifact census drifted")
    for role, expected_sha256 in expected_owner.items():
        owner_path = _resolve_bound_file(
            owner_artifacts[role],
            label=f"exact owner {role}",
            repository_root=repository_root,
        )
        if _file_sha256(owner_path) != expected_sha256:
            raise OfflineOrchestratorError(f"exact owner {role} identity drifted")
    receipts = panel.get("day_receipt_sha256")
    source_receipts = {
        row["utc_day"]: row["day_receipt_sha256"]
        for row in source.get("target_day_receipts", ())
        if isinstance(row, Mapping)
        and row.get("utc_day") in set(source.get("selected_days", ()))
    }
    if not isinstance(receipts, Mapping) or dict(receipts) != source_receipts:
        raise OfflineOrchestratorError("panel day receipts drifted from source admission")
    files = panel.get("files")
    if not isinstance(files, Mapping):
        raise OfflineOrchestratorError("canonical mechanics panel files must be an object")
    forbidden_files = set(files) & _FORBIDDEN_ECONOMIC_PANEL_FILES
    if forbidden_files:
        roles = ", ".join(sorted(forbidden_files))
        raise OfflineOrchestratorError(
            f"economic label files are forbidden in the mechanics panel: {roles}"
        )
    if set(files) != set(_PANEL_FILES):
        raise OfflineOrchestratorError("canonical panel file census is incomplete")
    return {
        role: _resolve_bound_file(
            files[role],
            label=f"panel {role}",
            repository_root=repository_root,
        )
        for role in _PANEL_FILES
    }


def _load_formal_offline_bundle(
    execution_manifest_path: Path,
    *,
    verify_source_bytes: bool = True,
    require_clean_tag: bool = True,
) -> FormalOfflineBundle:
    """Internal loader with test-only verification controls."""

    path = execution_manifest_path.expanduser().resolve()
    manifest = _load_json(path, label="formal execution manifest")
    if manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("identity") != IDENTITY:
        raise OfflineOrchestratorError("formal execution identity drifted")
    if manifest.get("status") != "pre_economic_formal_execution_bound":
        raise OfflineOrchestratorError("formal execution status drifted")
    if manifest.get("canonical_execution_manifest_sha256") != _document_sha256(
        manifest, "canonical_execution_manifest_sha256"
    ):
        raise OfflineOrchestratorError("formal execution manifest hash drifted")
    if manifest.get("backend") != {
        "module": CANONICAL_BACKEND_MODULE,
        "function": CANONICAL_BACKEND_FUNCTION,
        "custom_evaluator_allowed": False,
    }:
        raise OfflineOrchestratorError("formal backend identity drifted")
    if manifest.get("execution_contract") != {
        "control": "B0_CURRENT_EXACT",
        "sequential_repeated_policy": True,
        "one_shot_effect_aggregation_used": False,
        "outer_test_candidate_freeze_required": True,
        "action_alpha_v1_required": True,
    }:
        raise OfflineOrchestratorError("formal execution contract drifted")
    permissions = manifest.get("permissions")
    if permissions != {
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }:
        raise OfflineOrchestratorError("formal execution permissions drifted")
    root_value = manifest.get("repository_root")
    try:
        repository_root = resolve_portable_path(
            str(root_value),
            root=Path(__file__).resolve().parents[4],
        ).resolve()
    except (RuntimeError, ValueError) as exc:
        raise OfflineOrchestratorError("repository root binding is not portable") from exc
    source_path = _resolve_bound_file(
        manifest.get("source_manifest") or {},
        label="canonical source manifest",
        repository_root=repository_root,
    )
    source = offline.validate_canonical_manifest(
        source_path,
        rehash_sources=verify_source_bytes,
    )
    if len(source.get("selected_days", ())) != offline.REQUIRED_DAYS:
        raise OfflineOrchestratorError("canonical source gate has fewer than 30 admitted days")
    if manifest.get("source_contract") != {
        "panel_role": offline.PANEL_ROLE,
        "queue_identity": offline.QUEUE_IDENTITY,
        "selected_day_count": offline.REQUIRED_DAYS,
        "selection_sha256": source.get("selection_sha256"),
        "day_receipts_revalidated": True,
        "economic_outcomes_read": False,
    }:
        raise OfflineOrchestratorError("formal source contract drifted")
    panel_path = _resolve_bound_file(
        manifest.get("panel_manifest") or {},
        label="canonical panel manifest",
        repository_root=repository_root,
    )
    panel = _load_json(panel_path, label="canonical panel manifest")
    panel_files = _validate_panel_manifest(
        panel,
        source=source,
        repository_root=repository_root,
    )
    source_folds = source.get("fold_manifest")
    if not isinstance(source_folds, Mapping):
        raise OfflineOrchestratorError("source admission lacks frozen folds")
    if manifest.get("fold_manifest_sha256") != source_folds.get("fold_manifest_sha256"):
        raise OfflineOrchestratorError("formal fold manifest drifted")
    try:
        expected_nested_folds = offline.derive_bound_nested_fold_manifest(source)
    except offline.OfflineSourceGateError as exc:
        raise OfflineOrchestratorError(
            "source admission cannot derive the frozen 4x3 nested folds"
        ) from exc
    if manifest.get("nested_fold_manifest") != expected_nested_folds:
        raise OfflineOrchestratorError("formal nested-fold manifest drifted")
    if manifest.get("nested_fold_manifest_sha256") != expected_nested_folds.get(
        "nested_fold_manifest_sha256"
    ):
        raise OfflineOrchestratorError("formal nested-fold SHA256 drifted")
    if require_clean_tag:
        _validate_clean_annotated_tag(
            repository_root=repository_root,
            commit_sha=str(manifest.get("public_base_commit", "")),
            tag=str(manifest.get("annotated_tag", "")),
        )
    return _new_formal_offline_bundle(
        execution_manifest_path=path,
        execution_manifest=manifest,
        source_manifest_path=source_path,
        source_manifest=source,
        panel_manifest_path=panel_path,
        panel_manifest=panel,
        panel_files=panel_files,
        repository_root=repository_root,
    )


def load_formal_offline_bundle(
    execution_manifest_path: Path,
) -> FormalOfflineBundle:
    """Load the sole formal input with source-byte and clean-tag checks required."""

    return _load_formal_offline_bundle(
        execution_manifest_path,
        verify_source_bytes=True,
        require_clean_tag=True,
    )


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="ascii",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = handle.name
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def bind_formal_execution_manifest(
    panel_manifest_path: Path,
    output_path: Path,
    *,
    annotated_tag: str,
    repository_root: Path | None = None,
) -> Mapping[str, Any]:
    """Derive the sole formal input from an admitted panel at a clean tag.

    The caller cannot supply dates, folds, owner identities, backend functions, or
    an evaluator.  Every one of those fields is recovered from the revalidated
    source and panel admission chain.
    """

    root = (repository_root or Path(__file__).resolve().parents[4]).expanduser().resolve()
    destination = output_path.expanduser().resolve()
    if destination.exists():
        raise OfflineOrchestratorError(
            f"immutable formal execution manifest already exists: {destination}"
        )
    commit_sha = _git("rev-parse", "HEAD", root=root)
    _validate_clean_annotated_tag(
        repository_root=root,
        commit_sha=commit_sha,
        tag=annotated_tag,
    )
    panel_path = panel_manifest_path.expanduser().resolve()
    try:
        panel = mechanics.validate_panel(
            panel_path,
            layout=offline.default_layout(),
            repository_root=root,
        )
    except (mechanics.OfflineMechanicsError, OSError, ValueError) as exc:
        raise OfflineOrchestratorError(
            "canonical mechanics panel failed full admission validation"
        ) from exc
    source_binding = panel.get("source_manifest")
    if not isinstance(source_binding, Mapping):
        raise OfflineOrchestratorError("canonical panel lacks its source binding")
    source_path = _resolve_bound_file(
        source_binding,
        label="canonical source manifest",
        repository_root=root,
    )
    source = offline.validate_canonical_manifest(source_path, rehash_sources=True)
    if len(source.get("selected_days", ())) != offline.REQUIRED_DAYS:
        raise OfflineOrchestratorError("canonical source gate has fewer than 30 admitted days")
    _validate_panel_manifest(panel, source=source, repository_root=root)
    folds = source.get("fold_manifest")
    if not isinstance(folds, Mapping) or _SHA_RE.fullmatch(
        str(folds.get("fold_manifest_sha256", ""))
    ) is None:
        raise OfflineOrchestratorError("canonical source admission lacks frozen fold identity")
    try:
        nested_folds = offline.derive_bound_nested_fold_manifest(source)
    except offline.OfflineSourceGateError as exc:
        raise OfflineOrchestratorError(
            "canonical source admission cannot freeze the complete 4x3 fold contract"
        ) from exc
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "pre_economic_formal_execution_bound",
        "repository_root": "${NARROWGATE_ROOT}",
        "public_base_commit": commit_sha,
        "annotated_tag": annotated_tag,
        "source_manifest": _binding(source_path, repository_root=root),
        "panel_manifest": _binding(panel_path, repository_root=root),
        "fold_manifest_sha256": folds["fold_manifest_sha256"],
        "nested_fold_manifest": nested_folds,
        "nested_fold_manifest_sha256": nested_folds[
            "nested_fold_manifest_sha256"
        ],
        "backend": {
            "module": CANONICAL_BACKEND_MODULE,
            "function": CANONICAL_BACKEND_FUNCTION,
            "custom_evaluator_allowed": False,
        },
        "execution_contract": {
            "control": "B0_CURRENT_EXACT",
            "sequential_repeated_policy": True,
            "one_shot_effect_aggregation_used": False,
            "outer_test_candidate_freeze_required": True,
            "action_alpha_v1_required": True,
        },
        "source_contract": {
            "panel_role": offline.PANEL_ROLE,
            "queue_identity": offline.QUEUE_IDENTITY,
            "selected_day_count": offline.REQUIRED_DAYS,
            "selection_sha256": source.get("selection_sha256"),
            "day_receipts_revalidated": True,
            "economic_outcomes_read": False,
        },
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    manifest["canonical_execution_manifest_sha256"] = _document_sha256(
        manifest, "canonical_execution_manifest_sha256"
    )
    _atomic_json(destination, manifest)
    try:
        load_formal_offline_bundle(destination)
    except Exception:
        destination.unlink(missing_ok=True)
        raise
    return manifest


def run_formal_offline_economics(
    execution_manifest_path: Path,
    *,
    output_dir: Path,
) -> Mapping[str, Any]:
    """Run only the repository-owned backend after complete formal admission."""

    bundle = load_formal_offline_bundle(execution_manifest_path)
    try:
        backend = importlib.import_module(CANONICAL_BACKEND_MODULE)
    except ModuleNotFoundError as exc:
        raise OfflineOrchestratorError(
            "canonical repeated-policy backend is not implemented"
        ) from exc
    runner = getattr(backend, CANONICAL_BACKEND_FUNCTION, None)
    if not callable(runner):
        raise OfflineOrchestratorError("canonical backend function is unavailable")
    result = runner(execution_manifest_path)
    if not isinstance(result, Mapping):
        raise OfflineOrchestratorError("canonical backend did not return a result manifest")
    if result.get("schema_version") != FORMAL_RESULT_SCHEMA:
        raise OfflineOrchestratorError("canonical backend result schema drifted")
    if result.get("execution_manifest_sha256") != bundle.execution_manifest.get(
        "canonical_execution_manifest_sha256"
    ):
        raise OfflineOrchestratorError("formal result is not bound to its execution manifest")
    if result.get("repeated_sequential_policy") is not True or result.get(
        "one_shot_effect_aggregation_used"
    ) is not False:
        raise OfflineOrchestratorError("formal result did not execute sequential policies")
    if result.get("exact_owner_policy_sha256") != offline.ACTIVE_OWNER_POLICY_SHA256:
        raise OfflineOrchestratorError("formal result control identity drifted")
    if result.get("validation_read") is not False or result.get("sealed_holdout_read") is not False:
        raise OfflineOrchestratorError("formal result read a forbidden evidence split")
    output = output_dir.expanduser().resolve()
    payload = dict(result)
    payload["canonical_result_sha256"] = _document_sha256(
        payload, "canonical_result_sha256"
    )
    _atomic_json(output / "formal_result.json", payload)
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    bind = subparsers.add_parser("bind")
    bind.add_argument("panel_manifest", type=Path)
    bind.add_argument("output_manifest", type=Path)
    bind.add_argument("--annotated-tag", required=True)
    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("manifest", type=Path)
    preflight.add_argument("--output", type=Path)
    run = subparsers.add_parser("run")
    run.add_argument("manifest", type=Path)
    run.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "bind":
        result = bind_formal_execution_manifest(
            args.panel_manifest,
            args.output_manifest,
            annotated_tag=args.annotated_tag,
        )
        result = {
            "identity": IDENTITY,
            "status": result["status"],
            "execution_manifest_sha256": result["canonical_execution_manifest_sha256"],
            "economic_outcomes_read": False,
        }
    elif args.command == "preflight":
        backend = importlib.import_module(CANONICAL_BACKEND_MODULE)
        preflight_runner = getattr(
            backend,
            "preflight_canonical_offline_economics",
            None,
        )
        if not callable(preflight_runner):
            raise OfflineOrchestratorError("canonical backend preflight is unavailable")
        result = preflight_runner(args.manifest)
        if args.output is not None:
            output_path = args.output.expanduser().resolve()
            if output_path.exists():
                raise OfflineOrchestratorError(
                    f"immutable formal preflight receipt already exists: {output_path}"
                )
            payload = dict(result)
            payload["canonical_preflight_sha256"] = _document_sha256(
                payload,
                "canonical_preflight_sha256",
            )
            _atomic_json(output_path, payload)
            result = payload
    else:
        result = run_formal_offline_economics(
            args.manifest,
            output_dir=args.output_dir,
        )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
