"""Audit a minimal v9 prospective-epoch/journal-v2 production release.

This command is deliberately read-only unless ``--output`` is supplied.  It
never invokes SSH, copies files, edits a live configuration, or restarts the
maker.  Its job is to prove that a release candidate is a narrow successor of
the frozen remote v9 runtime instead of the repository's broad ``make deploy``
surface.
"""

from __future__ import annotations

import argparse
import ast
import difflib
import hashlib
import json
import os
import sys
import tempfile
import uuid
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.governance.public_machine_projection import (  # noqa: E402
    projection_for,
    source_document_path,
    source_identity_sha256,
)
from scripts.live_remote_pointer import active_live_remote_fields  # noqa: E402

try:
    from scripts.build_prospective_lifecycle_narrow_release import (
        REQUIRED_RUNTIME_EVIDENCE as NARROW_RELEASE_RUNTIME_EVIDENCE,
    )
    from scripts.build_prospective_lifecycle_narrow_release import validate_staging
except ModuleNotFoundError as exc:
    if exc.name != "scripts":
        raise
    from build_prospective_lifecycle_narrow_release import (
        REQUIRED_RUNTIME_EVIDENCE as NARROW_RELEASE_RUNTIME_EVIDENCE,
    )
    from build_prospective_lifecycle_narrow_release import validate_staging

SCHEMA_VERSION = "prospective_lifecycle_minimal_production_release.v1"
FROZEN_V9_BASELINE_SHA256 = "bfe835bf4b76fc675cd450eccf248cd1a3d179e2f9755425b40889f042c44638"

EPHEMERAL_ROOT = Path(
    os.environ.get("NARROWGATE_EPHEMERAL_ROOT", tempfile.gettempdir())
).expanduser()
_ACTIVE_REMOTE = active_live_remote_fields(ROOT)
DEFAULT_REMOTE_ROOT = os.environ.get(
    "NARROWGATE_REMOTE_ROOT",
    _ACTIVE_REMOTE.get("repo_root", str(Path.home() / ROOT.name)),
)
DEFAULT_REMOTE_FORMAL_ROOT = str(
    PurePosixPath(DEFAULT_REMOTE_ROOT) / "formal_collection"
)

REMOTE_V9_SUPPLEMENTAL_SHA256 = {
    "strategy/order_manager.py": (
        "fb246ac4ef64207be42b317688a0e1c3e7b13f586b0514718301a80fe2235db9"
    ),
    "live/ws_handler.py": (
        "c76683bf7fab8d975d80d78b48230e7d50f2fa50605e31b42a1b03a5156a3fd3"
    ),
}

REMOTE_V9_SNAPSHOT_DEFAULTS = {
    "live/main.py": EPHEMERAL_ROOT / "narrowgate_remote_main.py",
    "live/config.py": EPHEMERAL_ROOT / "narrowgate_remote_config.py",
    "strategy/maker_engine.py": EPHEMERAL_ROOT / "narrowgate_remote_maker_engine.py",
    "strategy/order_manager.py": Path(
        EPHEMERAL_ROOT / "narrowgate_remote_runtime_v9/order_manager.py"
    ),
    "live/ws_handler.py": EPHEMERAL_ROOT / "narrowgate_remote_runtime_v9/ws_handler.py",
}

# These files need a narrow patch made against the exact v9 predecessor.  The
# current working-tree versions are not release payloads because they include
# unrelated research and baseline changes.
PATCH_EXISTING_FILES = (
    "live/main.py",
    "live/config.py",
    "strategy/maker_engine.py",
    "strategy/order_manager.py",
    "live/ws_handler.py",
)

# New runtime modules that are specific to the prospective epoch/journal-v2
# boundary.  No C++ source is part of this release.
ADD_RUNTIME_FILES = (
    "execution/order_lifecycle.py",
    "execution/order_lifecycle_quantity_contract.py",
    "execution/order_lifecycle_journal_storage_v2.py",
    "execution/order_lifecycle_journal_v2.py",
    "execution/order_lifecycle_journal_writer_v2.py",
    "execution/order_lifecycle_live_writer_v2.py",
    "models/replay/baseline_epoch_manifest.py",
    "models/replay/prospective_baseline_epoch.py",
)

GENERATED_RUNTIME_FILES = (
    "execution/prospective_lifecycle_state_capture_v1.py",
    "models/replay/__init__.py",
)

ADD_OPERATIONS_FILES = (
    "execution/order_lifecycle_remote_spool_v2.py",
    "scripts/lifecycle_journal_v2_collector.py",
)

IDENTITY_FILES = (
    "research/families/f10_live_replay_attribution/docs/"
    "operational_baseline_identity_20260804_v9.json",
)

# Existing files are not to be replaced by this release.  Their remote hashes
# must instead be captured and checked before assembling the v9 successor.
REMOTE_HASH_REQUIRED_FILES = (
    "strategy/signal.py",
    "strategy/inventory_manager.py",
    "features/feature_dag.py",
)

REMOTE_PATCH_SOURCE_REQUIRED_FILES = PATCH_EXISTING_FILES

REQUIRED_CANDIDATE_SYMBOLS = {
    "live/main.py": (
        "prospective_epoch_runtime_code_paths",
        "initialize_prospective_lifecycle_collection",
        "start_engine_with_prospective_collection",
    ),
    "live/config.py": ("LifecycleJournalV2Config",),
    "strategy/maker_engine.py": (
        "MakerEngine.set_order_lifecycle_live_writer_v2",
        "MakerEngine.order_lifecycle_live_writer_v2_health_snapshot",
        "MakerEngine.prospective_epoch_initial_runtime_state",
        "MakerEngine.record_reconciled_order_lifecycle",
        "MakerEngine._on_order_lifecycle_event",
    ),
    "strategy/order_manager.py": (
        "Order.remaining_qty",
        "OrderManager.cancel_rejected",
        "OrderManager.lifecycle_snapshot",
    ),
    "live/ws_handler.py": ("WSHandler._on_user_message",),
}

# Markers that prove a working-tree entry point contains concerns outside this
# release.  Finding one is not a criticism of the implementation; it means the
# entire file cannot be copied over v9 as a minimal production patch.
MIXED_CONCERN_MARKERS = {
    "live/main.py": (
        "record_startup_runtime_identity",
        "q90_action_runtime_policy",
        "write_runtime_identity",
    ),
    "live/config.py": (
        "exact_opportunity_tape_enabled",
        "q90_action_runtime_policy",
        "require_q90_action_restart",
    ),
    "strategy/maker_engine.py": (
        "ExactOpportunityDailyWriter",
        "project_external_adverse_quote_edge",
        "_evaluate_dynamic_fill_hazard_prospective_recovery",
    ),
}

REQUIRED_EVIDENCE_GATES = NARROW_RELEASE_RUNTIME_EVIDENCE


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.partial-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(
                payload,
                handle,
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _read_json(path: Path, name: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{name} must be a JSON object")
    return payload


def _diff_counts(before: Path, after: Path) -> dict[str, int]:
    before_lines = before.read_text(encoding="utf-8").splitlines()
    after_lines = after.read_text(encoding="utf-8").splitlines()
    added = 0
    removed = 0
    for row in difflib.ndiff(before_lines, after_lines):
        if row.startswith("+ "):
            added += 1
        elif row.startswith("- "):
            removed += 1
    return {"added_lines": added, "removed_lines": removed}


def _parse_python(path: Path) -> None:
    ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _defined_symbols(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    symbols: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            symbols.add(node.name)
        if isinstance(node, ast.ClassDef):
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    symbols.add(f"{node.name}.{child.name}")
    return symbols


def _file_record(path: Path, repo_root: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(repo_root).as_posix(),
        "sha256": _sha256(path),
        "size_bytes": path.stat().st_size,
    }


def _load_evidence(path: Path | None) -> dict[str, bool]:
    if path is None:
        return {}
    payload = _read_json(path, "release evidence")
    evidence = payload.get("gates", payload)
    if not isinstance(evidence, dict):
        raise ValueError("release evidence gates must be a mapping")
    return {str(key): bool(value) for key, value in evidence.items()}


def _load_remote_dependency_hashes(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    payload = _read_json(path, "remote dependency manifest")
    files = payload.get("files", payload)
    if not isinstance(files, dict):
        raise ValueError("remote dependency manifest files must be a mapping")
    normalized = {}
    for logical_path, raw_hash in files.items():
        value = str(raw_hash).strip().lower()
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError(f"invalid remote dependency SHA256 for {logical_path}")
        normalized[str(logical_path)] = value
    return normalized


def _validate_release_config(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("release config must be a mapping")
    lifecycle = payload.get("lifecycle_journal_v2")
    strategy = payload.get("strategy")
    ml = payload.get("ml")
    if not isinstance(lifecycle, dict):
        raise ValueError("release config lacks lifecycle_journal_v2")
    if not isinstance(strategy, dict) or not isinstance(ml, dict):
        raise ValueError("release config lacks strategy or ml mapping")
    expected = {
        "enabled": True,
        "storage_profile": "bounded_remote_spool",
        "required_mount": DEFAULT_REMOTE_FORMAL_ROOT,
        "remote_session_max_duration_s": 3600,
        "remote_session_max_bytes": 4 * 1024 * 1024 * 1024,
        "baseline_identity_sha256": FROZEN_V9_BASELINE_SHA256,
    }
    for field, expected_value in expected.items():
        if lifecycle.get(field) != expected_value:
            raise ValueError(
                f"release config lifecycle_journal_v2.{field} must equal {expected_value!r}"
            )
    roots = [
        str(lifecycle.get("root", "")),
        str(lifecycle.get("prospective_epoch_root", "")),
    ]
    allowlisted = lifecycle.get("remote_spool_allowlisted_roots")
    if not isinstance(allowlisted, list) or allowlisted != [DEFAULT_REMOTE_FORMAL_ROOT]:
        raise ValueError("release config must freeze the single EC2 spool allowlist")
    for root in roots:
        if PurePosixPath(DEFAULT_REMOTE_FORMAL_ROOT) not in PurePosixPath(root).parents:
            raise ValueError("release config journal roots must be inside the EC2 spool")
    if roots[0] == roots[1]:
        raise ValueError("release config journal and epoch roots must differ")
    baseline_path = str(lifecycle.get("baseline_identity_path", ""))
    if baseline_path != IDENTITY_FILES[0]:
        raise ValueError("release config baseline identity path must bind v9")
    frozen_flags = {
        "dynamic_fill_hazard_shadow_enabled": True,
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_shadow_enabled": False,
        "buy_fill_selection_live_enabled": False,
        "use_bar_pricing": False,
    }
    for field, expected_value in frozen_flags.items():
        if strategy.get(field) is not expected_value:
            raise ValueError(
                f"release config strategy.{field} must preserve v9 value {expected_value!r}"
            )
    if ml.get("enabled") is not True:
        raise ValueError("release config must preserve causal-v12 ML enabled")
    return {
        "path": str(path.resolve()),
        "sha256": _sha256(path),
        "lifecycle_journal_v2": {
            **expected,
            "root": roots[0],
            "prospective_epoch_root": roots[1],
            "remote_spool_allowlisted_roots": allowlisted,
            "baseline_identity_path": baseline_path,
        },
        "strategy_flags": frozen_flags,
        "ml_enabled": True,
    }


def audit_release(
    *,
    repo_root: Path,
    baseline_identity_path: Path,
    remote_snapshots: Mapping[str, Path],
    candidate_root: Path | None = None,
    evidence_path: Path | None = None,
    remote_dependency_manifest_path: Path | None = None,
    release_config_path: Path | None = None,
) -> dict[str, Any]:
    """Return a deterministic, fail-closed production-release audit."""

    repo_root = repo_root.resolve()
    baseline_identity_path = baseline_identity_path.resolve()
    blockers: list[dict[str, Any]] = []

    baseline_source_sha256 = source_identity_sha256(baseline_identity_path)
    baseline_projection = projection_for(baseline_identity_path)
    if baseline_source_sha256 != FROZEN_V9_BASELINE_SHA256:
        blockers.append(
            {
                "id": "baseline_identity_hash_mismatch",
                "detail": str(baseline_identity_path),
            }
        )
    baseline_source_path = source_document_path(
        baseline_identity_path,
        require_private=False,
    )
    baseline = _read_json(baseline_source_path, "v9 baseline identity")
    runtime_code = baseline.get("runtime_code")
    if not isinstance(runtime_code, dict):
        raise ValueError("v9 baseline identity lacks runtime_code")

    remote_records: dict[str, Any] = {}
    for logical_path in REMOTE_PATCH_SOURCE_REQUIRED_FILES:
        raw_snapshot = remote_snapshots.get(logical_path)
        if raw_snapshot is None:
            blockers.append(
                {
                    "id": "missing_remote_v9_patch_source",
                    "detail": logical_path,
                }
            )
            continue
        snapshot = Path(raw_snapshot).resolve()
        if not snapshot.is_file():
            blockers.append(
                {
                    "id": "missing_remote_v9_snapshot",
                    "detail": logical_path,
                }
            )
            continue
        actual = _sha256(snapshot)
        expected = str(
            runtime_code.get(logical_path, "")
            or REMOTE_V9_SUPPLEMENTAL_SHA256.get(logical_path, "")
        )
        matched = actual == expected if expected else None
        remote_records[logical_path] = {
            "snapshot_path": str(snapshot),
            "sha256": actual,
            "expected_sha256": expected,
            "matches_frozen_v9": matched,
        }
        if matched is False:
            blockers.append(
                {
                    "id": "remote_v9_predecessor_hash_mismatch",
                    "detail": logical_path,
                }
            )

    local_records: dict[str, Any] = {}
    mixed_concerns: dict[str, list[str]] = {}
    for logical_path, snapshot in remote_snapshots.items():
        local_path = repo_root / logical_path
        if not local_path.is_file() or not Path(snapshot).is_file():
            continue
        text = local_path.read_text(encoding="utf-8")
        found = [marker for marker in MIXED_CONCERN_MARKERS.get(logical_path, ()) if marker in text]
        if found:
            mixed_concerns[logical_path] = found
        local_records[logical_path] = {
            **_file_record(local_path, repo_root),
            **_diff_counts(Path(snapshot), local_path),
            "whole_file_minimal_release_candidate": not found,
        }
    if mixed_concerns and candidate_root is None:
        blockers.append(
            {
                "id": "working_tree_entrypoints_mix_unrelated_concerns",
                "detail": mixed_concerns,
            }
        )

    missing_local_files = []
    add_records = []
    for logical_path in (*ADD_RUNTIME_FILES, *ADD_OPERATIONS_FILES, *IDENTITY_FILES):
        path = repo_root / logical_path
        if not path.is_file():
            missing_local_files.append(logical_path)
            continue
        if path.suffix == ".py":
            _parse_python(path)
        add_records.append(_file_record(path, repo_root))
    if missing_local_files:
        blockers.append(
            {
                "id": "missing_minimal_release_file",
                "detail": missing_local_files,
            }
        )

    # Existing runtime files must be narrow successors of their exact remote
    # predecessors. New modules are independently hash-bound above.
    candidate_records = []
    candidate_staging_validation: dict[str, Any] = {}
    if candidate_root is None:
        blockers.append(
            {
                "id": "v9_successor_patch_not_frozen",
                "detail": list(PATCH_EXISTING_FILES),
            }
        )
    else:
        candidate_root = candidate_root.resolve()
        for logical_path in PATCH_EXISTING_FILES:
            path = candidate_root / logical_path
            if not path.is_file():
                blockers.append(
                    {
                        "id": "missing_v9_successor_patch_file",
                        "detail": logical_path,
                    }
                )
                continue
            _parse_python(path)
            symbols = _defined_symbols(path)
            missing_symbols = [
                name for name in REQUIRED_CANDIDATE_SYMBOLS[logical_path] if name not in symbols
            ]
            if missing_symbols:
                blockers.append(
                    {
                        "id": "v9_successor_patch_missing_required_symbol",
                        "detail": {logical_path: missing_symbols},
                    }
                )
            text = path.read_text(encoding="utf-8")
            forbidden = [
                marker for marker in MIXED_CONCERN_MARKERS.get(logical_path, ()) if marker in text
            ]
            if forbidden:
                blockers.append(
                    {
                        "id": "v9_successor_patch_contains_unrelated_marker",
                        "detail": {logical_path: forbidden},
                    }
                )
            candidate_records.append(_file_record(path, candidate_root))
        staging_manifest = candidate_root / "release_manifest.json"
        if not staging_manifest.is_file():
            blockers.append(
                {
                    "id": "candidate_release_manifest_missing",
                    "detail": str(staging_manifest),
                }
            )
        else:
            try:
                candidate_staging_validation = validate_staging(candidate_root)
            except Exception as exc:
                blockers.append(
                    {
                        "id": "candidate_release_manifest_invalid",
                        "detail": f"{type(exc).__name__}: {exc}",
                    }
                )

    baseline_runtime_drift: dict[str, Any] = {}
    for logical_path, expected in sorted(runtime_code.items()):
        local_path = repo_root / logical_path
        if not local_path.is_file() or len(str(expected)) != 64:
            continue
        actual = _sha256(local_path)
        if actual != expected:
            baseline_runtime_drift[logical_path] = {
                "frozen_v9_sha256": expected,
                "working_tree_sha256": actual,
            }
    remote_dependency_hashes = _load_remote_dependency_hashes(remote_dependency_manifest_path)
    missing_remote_dependencies = [
        path for path in REMOTE_HASH_REQUIRED_FILES if path not in remote_dependency_hashes
    ]
    for logical_path in ("strategy/signal.py", "features/feature_dag.py"):
        remote_hash = remote_dependency_hashes.get(logical_path)
        frozen_hash = str(runtime_code.get(logical_path, ""))
        if remote_hash is not None and remote_hash != frozen_hash:
            blockers.append(
                {
                    "id": "remote_dependency_drifted_from_v9",
                    "detail": logical_path,
                }
            )
    if missing_remote_dependencies:
        blockers.append(
            {
                "id": "remote_dependency_hash_evidence_missing",
                "detail": missing_remote_dependencies,
            }
        )

    release_config = _validate_release_config(release_config_path)
    if not release_config:
        blockers.append(
            {
                "id": "production_release_config_not_frozen",
                "detail": "supply a v9-preserving config with bounded_remote_spool enabled",
            }
        )

    evidence = _load_evidence(evidence_path)
    missing_evidence = [name for name in REQUIRED_EVIDENCE_GATES if not evidence.get(name)]
    if missing_evidence:
        blockers.append(
            {
                "id": "production_release_evidence_incomplete",
                "detail": missing_evidence,
            }
        )

    release_payload = {
        "patch_existing_from_exact_v9": [*PATCH_EXISTING_FILES, "live/config.yaml"],
        "add_runtime_files": list(ADD_RUNTIME_FILES),
        "add_generated_runtime_files": list(GENERATED_RUNTIME_FILES),
        "add_operations_files": list(ADD_OPERATIONS_FILES),
        "identity_files": list(IDENTITY_FILES),
        "confirmed_remote_new_files": [
            *list(ADD_RUNTIME_FILES),
            *list(ADD_OPERATIONS_FILES),
            *list(GENERATED_RUNTIME_FILES),
        ],
        "hash_bind_without_replacement": list(REMOTE_HASH_REQUIRED_FILES),
        "excluded": [
            "Makefile deploy target",
            "private config files other than the exact-v9 live/config.yaml successor",
            "cpp/**",
            "features/feature_dag.py working-tree replacement",
            "strategy action or model changes",
            "research outcome artifacts",
        ],
    }
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "audit_mode": "read_only_no_deploy_no_ssh",
        "remote_baseline_id": str(baseline.get("baseline_id", "")),
        "remote_baseline_identity_path": str(baseline_identity_path),
        "remote_baseline_identity_sha256": baseline_source_sha256,
        "remote_baseline_public_projection_sha256": (
            baseline_projection.public_projection_sha256
            if baseline_projection is not None
            else None
        ),
        "remote_baseline_private_source_available": (
            baseline_projection.private_source_available
            if baseline_projection is not None
            else None
        ),
        "remote_v9_snapshots": remote_records,
        "working_tree_entrypoint_diffs": local_records,
        "working_tree_mixed_concerns": mixed_concerns,
        "working_tree_v9_runtime_drift": baseline_runtime_drift,
        "remote_dependency_hashes": remote_dependency_hashes,
        "release_config": release_config,
        "minimal_release_payload": release_payload,
        "local_add_file_identities": add_records,
        "candidate_patch_file_identities": candidate_records,
        "candidate_staging_validation": candidate_staging_validation,
        "required_evidence_gates": list(REQUIRED_EVIDENCE_GATES),
        "provided_evidence": evidence,
        "safe_to_deploy_directly_from_remote_v9": not blockers,
        "safe_to_use_full_make_deploy": False,
        "deployment_executed": False,
        "blockers": blockers,
        "next_release_boundary": (
            "build and freeze the six-file v9 successor patch plus the new-file "
            "journal payload; run the bound tests, ABI checks, one-hour "
            "p99 window, spool admission, and rollback rehearsal"
        ),
    }
    manifest["manifest_sha256"] = _canonical_sha256(manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument(
        "--baseline-identity",
        type=Path,
        default=(
            Path(__file__).resolve().parents[1]
            / "research/families/f10_live_replay_attribution/docs/"
            "operational_baseline_identity_20260804_v9.json"
        ),
    )
    parser.add_argument(
        "--remote-main", type=Path, default=REMOTE_V9_SNAPSHOT_DEFAULTS["live/main.py"]
    )
    parser.add_argument(
        "--remote-config", type=Path, default=REMOTE_V9_SNAPSHOT_DEFAULTS["live/config.py"]
    )
    parser.add_argument(
        "--remote-maker-engine",
        type=Path,
        default=REMOTE_V9_SNAPSHOT_DEFAULTS["strategy/maker_engine.py"],
    )
    parser.add_argument(
        "--remote-order-manager",
        type=Path,
        default=REMOTE_V9_SNAPSHOT_DEFAULTS["strategy/order_manager.py"],
    )
    parser.add_argument(
        "--remote-ws-handler",
        type=Path,
        default=REMOTE_V9_SNAPSHOT_DEFAULTS["live/ws_handler.py"],
    )
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--remote-dependency-manifest", type=Path)
    parser.add_argument("--release-config", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    manifest = audit_release(
        repo_root=args.repo_root,
        baseline_identity_path=args.baseline_identity,
        remote_snapshots={
            "live/main.py": args.remote_main,
            "live/config.py": args.remote_config,
            "strategy/maker_engine.py": args.remote_maker_engine,
            "strategy/order_manager.py": args.remote_order_manager,
            "live/ws_handler.py": args.remote_ws_handler,
        },
        candidate_root=args.candidate_root,
        evidence_path=args.evidence,
        remote_dependency_manifest_path=args.remote_dependency_manifest,
        release_config_path=args.release_config,
    )
    if args.output is not None:
        _atomic_write_json(args.output.resolve(), manifest)
    print(json.dumps(manifest, sort_keys=True))
    return 0 if manifest["safe_to_deploy_directly_from_remote_v9"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
