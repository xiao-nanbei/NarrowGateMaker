"""Build the deterministic warmup-before-WebSocket v9 baseline successor.

This builder is deliberately local-only. It reconstructs a rollback-safe
baseline stage from exact v9 predecessor captures, changes only the startup
order in ``live/main.py``, and emits an unauthorised manifest. It never uses
SSH, deploys files, changes a current pointer, or reads economic outcomes.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import shutil
import sys
import tempfile
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

SCHEMA_VERSION = "warmup_before_websocket_baseline_successor.v1"
RELEASE_ID = "v9_warmup_before_websocket_baseline_successor_20260805"
SUCCESSOR_BASELINE_ID = (
    "btc_usdc_v9_strategy_warmup_before_websocket_baseline_integrity_successor_20260805"
)
STARTUP_CONTRACT = "warmup_before_websocket.v1"
MANIFEST_NAME = "baseline_successor_manifest.json"

PREDECESSOR_IDENTITY_RELATIVE = (
    "research/families/f10_live_replay_attribution/docs/"
    "operational_baseline_identity_20260804_v9.json"
)
FROZEN_V9_BASELINE_SHA256 = "bfe835bf4b76fc675cd450eccf248cd1a3d179e2f9755425b40889f042c44638"
FROZEN_V9_CONFIG_SHA256 = "889f605dc6a057874a8070fd86cbd21a0c8eb050156315c1dc6f48ec9acb48f5"

EPHEMERAL_ROOT = Path(
    os.environ.get("NARROWGATE_EPHEMERAL_ROOT", tempfile.gettempdir())
).expanduser()
PREDECESSOR_SOURCE_DEFAULTS = {
    "live/main.py": EPHEMERAL_ROOT / "narrowgate_remote_main.py",
    "live/config.py": EPHEMERAL_ROOT / "narrowgate_remote_config.py",
    "strategy/maker_engine.py": EPHEMERAL_ROOT / "narrowgate_remote_maker_engine.py",
    "strategy/order_manager.py": (
        EPHEMERAL_ROOT / "narrowgate_remote_runtime_v9/order_manager.py"
    ),
    "live/ws_handler.py": EPHEMERAL_ROOT / "narrowgate_remote_runtime_v9/ws_handler.py",
    "live/config.yaml": EPHEMERAL_ROOT / "narrowgate_remote_runtime_v9/config.yaml",
}

EXPECTED_PREDECESSOR_SHA256 = {
    "live/main.py": "f0f0ffe919f05df0fa17b2cc62b5d5815cb3ec4cf6b718d1efe011003e33bc6b",
    "live/config.py": "eb9a72d4bb2361ecf0f2617ee6fbc53329517d62d77ee381c85a6c59873fb8a7",
    "strategy/maker_engine.py": (
        "9dcca8b0b92313758f2c35093516eafbc6940d5fec6fea028f190c6bc68cc23c"
    ),
    "strategy/order_manager.py": (
        "fb246ac4ef64207be42b317688a0e1c3e7b13f586b0514718301a80fe2235db9"
    ),
    "live/ws_handler.py": ("c76683bf7fab8d975d80d78b48230e7d50f2fa50605e31b42a1b03a5156a3fd3"),
    "live/config.yaml": FROZEN_V9_CONFIG_SHA256,
}

OLD_STARTUP_BLOCK = """        # Start WebSocket
        if not args.dry_run:
            ws.start(rest)
        else:
            logger.info("[DRY-RUN] Skipping WebSocket connections")

        # Start engine
        engine.start()
"""

NEW_STARTUP_BLOCK = """        # Start engine and complete REST warmup before market-data intake
        engine.start()

        # Start WebSocket only after engine.start() has returned
        if not args.dry_run:
            ws.start(rest)
        else:
            logger.info("[DRY-RUN] Skipping WebSocket connections")
"""

JOURNAL_RUNTIME_MARKERS = (
    "LifecycleJournalV2Config",
    "OrderLifecycleLiveWriterV2",
    "initialize_prospective_lifecycle_collection",
    "lifecycle_journal_v2",
    "PROSPECTIVE_BASELINE_EPOCH_BOUND",
    "set_order_lifecycle_live_writer_v2",
)

EXPECTED_ACTION_ENABLEMENT = {
    "ml_enabled": True,
    "dynamic_fill_hazard_shadow_enabled": True,
    "dynamic_fill_hazard_action_enabled": False,
    "buy_fill_selection_shadow_enabled": False,
    "buy_fill_selection_live_enabled": False,
    "buy_fill_selection_live_model_path": "",
}

PRESERVED_RUNTIME_PATHS = (
    "cpp/narrowgate_cpp/streaming_features.cpp",
    "features/feature_dag.py",
    "live/run.sh",
    "market_fusion.py",
    "scripts/audit_quote_snapshot_integrity.py",
    "scripts/preflight_live_deploy.py",
    "strategy/model_contract.py",
    "strategy/quote_core.py",
    "strategy/signal.py",
)


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


def _read_object(path: Path, label: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _require_regular_file(path: Path, expected_sha256: str, label: str) -> None:
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"{label} must be a regular file: {path}")
    observed = _sha256(path)
    if observed != expected_sha256:
        raise ValueError(
            f"{label} SHA256 mismatch: expected={expected_sha256} observed={observed} path={path}"
        )


def _replace_once(source: str, old: str, new: str, *, label: str) -> str:
    count = source.count(old)
    if count != 1:
        raise ValueError(f"{label} expected one exact startup block, found {count}")
    return source.replace(old, new, 1)


class _StartupCallVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def visit_Call(self, node: ast.Call) -> None:
        function = node.func
        if isinstance(function, ast.Attribute) and isinstance(function.value, ast.Name):
            key = f"{function.value.id}.{function.attr}"
            if key in {"engine.start", "ws.start"}:
                self.calls.append((key, node.lineno))
        self.generic_visit(node)


def _startup_call_lines(source: str, *, label: str) -> dict[str, int]:
    tree = ast.parse(source, filename=label)
    main_node = next(
        (
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "main"
        ),
        None,
    )
    if main_node is None:
        raise ValueError(f"{label} does not define main()")
    visitor = _StartupCallVisitor()
    visitor.visit(main_node)
    grouped: dict[str, list[int]] = {"engine.start": [], "ws.start": []}
    for name, line in visitor.calls:
        grouped[name].append(line)
    drifted = {name: lines for name, lines in grouped.items() if len(lines) != 1}
    if drifted:
        raise ValueError(f"{label} startup calls are not unique: {drifted}")
    return {name: lines[0] for name, lines in grouped.items()}


def _build_main(predecessor: str) -> str:
    predecessor_lines = _startup_call_lines(
        predecessor,
        label="v9 predecessor live/main.py",
    )
    if predecessor_lines["ws.start"] >= predecessor_lines["engine.start"]:
        raise ValueError("v9 predecessor no longer contains the unsafe startup order")
    candidate = _replace_once(
        predecessor,
        OLD_STARTUP_BLOCK,
        NEW_STARTUP_BLOCK,
        label="v9 startup-order patch",
    )
    _validate_main_successor(predecessor=predecessor, candidate=candidate)
    return candidate


def _validate_main_successor(*, predecessor: str, candidate: str) -> None:
    predecessor_lines = _startup_call_lines(
        predecessor,
        label="v9 predecessor live/main.py",
    )
    if predecessor_lines["ws.start"] >= predecessor_lines["engine.start"]:
        raise ValueError("predecessor startup order is not the frozen unsafe order")
    candidate_lines = _startup_call_lines(
        candidate,
        label="successor live/main.py",
    )
    if candidate_lines["engine.start"] >= candidate_lines["ws.start"]:
        raise ValueError(
            "successor must order engine.start before ws.start under "
            f"startup_contract={STARTUP_CONTRACT}"
        )
    if candidate.count(NEW_STARTUP_BLOCK) != 1:
        raise ValueError("successor startup block does not match the frozen contract")
    restored = candidate.replace(NEW_STARTUP_BLOCK, OLD_STARTUP_BLOCK, 1)
    if restored != predecessor:
        raise ValueError("successor contains changes beyond the startup-order block")


def _action_enablement_from_config(config: Mapping[str, Any]) -> dict[str, Any]:
    strategy = config.get("strategy")
    ml = config.get("ml")
    if not isinstance(strategy, Mapping) or not isinstance(ml, Mapping):
        raise ValueError("v9 config lacks strategy or ml mapping")
    return {
        "ml_enabled": ml.get("enabled"),
        "dynamic_fill_hazard_shadow_enabled": strategy.get("dynamic_fill_hazard_shadow_enabled"),
        "dynamic_fill_hazard_action_enabled": strategy.get("dynamic_fill_hazard_action_enabled"),
        "buy_fill_selection_shadow_enabled": strategy.get("buy_fill_selection_shadow_enabled"),
        "buy_fill_selection_live_enabled": strategy.get("buy_fill_selection_live_enabled"),
        "buy_fill_selection_live_model_path": strategy.get("buy_fill_selection_live_model_path"),
    }


def _semantic_projection(
    *,
    identity: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    runtime_code = identity.get("runtime_code")
    if not isinstance(runtime_code, Mapping):
        raise ValueError("v9 identity lacks runtime_code")
    missing_runtime = sorted(set(PRESERVED_RUNTIME_PATHS) - set(runtime_code))
    if missing_runtime:
        raise ValueError(
            "v9 identity lacks preserved runtime hashes: " + ", ".join(missing_runtime)
        )
    return {
        "config_sha256": FROZEN_V9_CONFIG_SHA256,
        "config_semantic_sha256": _canonical_sha256(config),
        "action_enablement": _action_enablement_from_config(config),
        "model": identity.get("model"),
        "p3": identity.get("p3"),
        "preserved_runtime_code": {path: runtime_code[path] for path in PRESERVED_RUNTIME_PATHS},
    }


def _verify_baseline_contract(
    *,
    identity: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    if identity.get("schema_version") != "narrowgate_operational_baseline_identity.v9":
        raise ValueError("predecessor identity is not the frozen v9 schema")
    if identity.get("baseline_id") != (
        "btc_usdc_causal_v12_quote_snapshot_atomicity_v2_q90_shadow_"
        "buy_fill_selection_retired_baseline_20260804"
    ):
        raise ValueError("predecessor v9 baseline_id drifted")
    identity_config = identity.get("config")
    permissions = identity.get("permissions")
    model = identity.get("model")
    p3 = identity.get("p3")
    if not all(isinstance(value, Mapping) for value in (identity_config, permissions, model, p3)):
        raise ValueError("predecessor identity lacks config/model/P3/permissions")
    if identity_config.get("sha256") != FROZEN_V9_CONFIG_SHA256:
        raise ValueError("predecessor identity config hash drifted")
    observed_actions = _action_enablement_from_config(config)
    if observed_actions != EXPECTED_ACTION_ENABLEMENT:
        raise ValueError(
            "v9 action enablement drifted: "
            f"expected={EXPECTED_ACTION_ENABLEMENT} observed={observed_actions}"
        )
    for field, expected in EXPECTED_ACTION_ENABLEMENT.items():
        if field in identity_config and identity_config.get(field) != expected:
            raise ValueError(f"v9 identity action field drifted: {field}")
    if permissions.get("q90_action_live_authorized") is not False:
        raise ValueError("v9 identity authorizes q90 action")
    if permissions.get("buy_fill_selection_action_authorized") is not False:
        raise ValueError("v9 identity authorizes BUY fill selection")
    if model.get("enabled") is not True or model.get("heads_loaded") != 13:
        raise ValueError("v9 causal-v12 model binding drifted")
    ml = config.get("ml")
    if not isinstance(ml, Mapping) or ml.get("model_dir") != model.get("directory"):
        raise ValueError("v9 config and identity model directory disagree")
    expected_p3_path = f"{model['directory']}/fill_prob_params.json"
    if p3.get("path") != expected_p3_path:
        raise ValueError("v9 P3 path is not bound to the frozen model directory")
    if "lifecycle_journal_v2" in config:
        raise ValueError("v9 config unexpectedly enables lifecycle journal semantics")
    return _semantic_projection(identity=identity, config=config)


def _verify_inputs(
    *,
    repo_root: Path,
    predecessor_sources: Mapping[str, Path],
    predecessor_identity_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if set(predecessor_sources) != set(EXPECTED_PREDECESSOR_SHA256):
        raise ValueError("predecessor source set does not match the frozen recipe")
    for logical_path, expected_sha256 in EXPECTED_PREDECESSOR_SHA256.items():
        _require_regular_file(
            Path(predecessor_sources[logical_path]),
            expected_sha256,
            f"predecessor {logical_path}",
        )
    expected_identity = repo_root / PREDECESSOR_IDENTITY_RELATIVE
    if predecessor_identity_path.resolve(strict=True) != expected_identity.resolve(strict=True):
        raise ValueError("predecessor identity path is not the frozen v9 identity")
    _require_regular_file(
        predecessor_identity_path,
        FROZEN_V9_BASELINE_SHA256,
        "predecessor v9 identity",
    )
    identity = _read_object(predecessor_identity_path, "predecessor v9 identity")
    config_path = Path(predecessor_sources["live/config.yaml"])
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError("v9 config must be a mapping")
    projection = _verify_baseline_contract(identity=identity, config=config)
    return identity, projection


def _write_exclusive(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    path.chmod(0o644)


def _file_record(
    *,
    logical_path: str,
    predecessor_sha256: str,
    candidate_path: Path,
    mode: str,
) -> dict[str, Any]:
    return {
        "path": logical_path,
        "mode": mode,
        "predecessor_sha256": predecessor_sha256,
        "sha256": _sha256(candidate_path),
        "size_bytes": candidate_path.stat().st_size,
    }


def _stable_start_probe_contract() -> dict[str, Any]:
    return {
        "schema_version": "warmup_before_websocket_stable_start_probe.v1",
        "same_pid_required": True,
        "canonical_bucket_s": 10.0,
        "minimum_completed_canonical_buckets": 2,
        "minimum_observed_duration_s": 20.0,
        "required_ordered_log_markers": [
            "MakerEngine started",
            "All WebSocket streams started",
            "Entering main loop...",
        ],
        "forbidden_log_markers": [
            "Fatal error:",
            "completed 10s feature bucket lacks an exact causal 1s grid",
            "duplicate-grid",
        ],
        "runtime_probe_required_before_rollback_authorization": True,
        "runtime_probe_status": "not_run_local_builder",
    }


def _assert_journal_disabled(
    *,
    staged_files: Mapping[str, bytes],
    config: Mapping[str, Any],
) -> None:
    if "lifecycle_journal_v2" in config:
        raise ValueError("successor config must not contain lifecycle_journal_v2")
    for logical_path, data in staged_files.items():
        if logical_path.endswith((".py", ".yaml")):
            text = data.decode("utf-8")
            found = [marker for marker in JOURNAL_RUNTIME_MARKERS if marker in text]
            if found:
                raise ValueError(
                    f"successor unexpectedly enables lifecycle journal in {logical_path}: {found}"
                )


def _build_payload(
    *,
    stage_root: Path,
    predecessor_sources: Mapping[str, Path],
    identity: Mapping[str, Any],
    semantic_projection: Mapping[str, Any],
) -> dict[str, Any]:
    predecessor_bytes = {
        logical_path: Path(source).read_bytes()
        for logical_path, source in predecessor_sources.items()
    }
    predecessor_main = predecessor_bytes["live/main.py"].decode("utf-8")
    candidate_main = _build_main(predecessor_main).encode("utf-8")
    staged_files = dict(predecessor_bytes)
    staged_files["live/main.py"] = candidate_main
    config = yaml.safe_load(staged_files["live/config.yaml"].decode("utf-8"))
    if not isinstance(config, dict):
        raise AssertionError("validated config ceased to be a mapping")
    _assert_journal_disabled(staged_files=staged_files, config=config)

    records = []
    for logical_path in EXPECTED_PREDECESSOR_SHA256:
        output = stage_root / logical_path
        _write_exclusive(output, staged_files[logical_path])
        mode = (
            "patch_v9_startup_order_only" if logical_path == "live/main.py" else "restore_exact_v9"
        )
        records.append(
            _file_record(
                logical_path=logical_path,
                predecessor_sha256=EXPECTED_PREDECESSOR_SHA256[logical_path],
                candidate_path=output,
                mode=mode,
            )
        )

    semantic_identity_sha256 = _canonical_sha256(semantic_projection)
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "successor_baseline_id": SUCCESSOR_BASELINE_ID,
        "startup_contract": STARTUP_CONTRACT,
        "build_mode": "deterministic_local_stage_only_no_ssh_no_deploy",
        "predecessor_binding": {
            "baseline_id": identity["baseline_id"],
            "identity_path": PREDECESSOR_IDENTITY_RELATIVE,
            "identity_sha256": FROZEN_V9_BASELINE_SHA256,
            "startup_contract": "unsafe_websocket_before_warmup.v0",
            "historical_identity_modified": False,
        },
        "rollback_binding": {
            "target_baseline_id": SUCCESSOR_BASELINE_ID,
            "target_role": "safe_rollback_baseline_candidate",
            "startup_contract": STARTUP_CONTRACT,
            "exact_restore_paths": sorted(EXPECTED_PREDECESSOR_SHA256),
            "stable_start_probe": _stable_start_probe_contract(),
            "rollback_authorized": False,
            "deployment_evidence_bound": False,
        },
        "files": sorted(records, key=lambda row: row["path"]),
        "strategy_config_semantic_equality": {
            "passed": True,
            "predecessor_semantic_identity_sha256": semantic_identity_sha256,
            "successor_semantic_identity_sha256": semantic_identity_sha256,
            "config_byte_equal": True,
            "config_semantic_equal": True,
            "unchanged_v9_files_byte_equal": True,
            "model_binding_equal": True,
            "p3_binding_equal": True,
            "action_enablement_equal": True,
            "strategy_or_quote_parameters_changed": False,
            "only_semantic_difference": (
                "live.main startup order: engine.start returns before ws.start"
            ),
            "action_enablement": semantic_projection["action_enablement"],
            "model": semantic_projection["model"],
            "p3": semantic_projection["p3"],
            "preserved_runtime_code": semantic_projection["preserved_runtime_code"],
        },
        "journal_boundary": {
            "lifecycle_journal_enabled": False,
            "lifecycle_journal_config_present": False,
            "lifecycle_journal_runtime_imported": False,
            "journal_payload_files_included": False,
            "economic_outcomes_read": False,
        },
        "permissions": {
            "local_stage_built": True,
            "deployment_executed": False,
            "deployment_authorized": False,
            "rollback_authorized": False,
            "registry_modified": False,
            "current_pointer_modified": False,
            "strategy_action_authorized": False,
            "economic_research_authorized": False,
        },
    }
    manifest["manifest_sha256"] = _canonical_sha256(manifest)
    _write_exclusive(
        stage_root / MANIFEST_NAME,
        (
            json.dumps(
                manifest,
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8"),
    )
    return manifest


def validate_staging(
    candidate_root: Path,
    *,
    repo_root: Path | None = None,
    predecessor_sources: Mapping[str, Path] | None = None,
    predecessor_identity_path: Path | None = None,
) -> dict[str, Any]:
    repo = (repo_root or Path(__file__).resolve().parents[1]).expanduser().resolve(strict=True)
    sources = dict(predecessor_sources or PREDECESSOR_SOURCE_DEFAULTS)
    identity_path = (predecessor_identity_path or repo / PREDECESSOR_IDENTITY_RELATIVE).expanduser()
    identity, semantic_projection = _verify_inputs(
        repo_root=repo,
        predecessor_sources=sources,
        predecessor_identity_path=identity_path,
    )
    root = candidate_root.expanduser().resolve(strict=True)
    if not root.is_dir() or root.is_symlink():
        raise ValueError("candidate root must be a non-symlink directory")
    manifest = _read_object(root / MANIFEST_NAME, "baseline successor manifest")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("baseline successor manifest schema drifted")
    claimed_manifest_sha256 = str(manifest.pop("manifest_sha256", ""))
    actual_manifest_sha256 = _canonical_sha256(manifest)
    manifest["manifest_sha256"] = claimed_manifest_sha256
    if claimed_manifest_sha256 != actual_manifest_sha256:
        raise ValueError("baseline successor manifest canonical hash mismatch")
    if manifest.get("startup_contract") != STARTUP_CONTRACT:
        raise ValueError("baseline successor startup contract drifted")

    expected_paths = {MANIFEST_NAME, *EXPECTED_PREDECESSOR_SHA256}
    actual_paths = {path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()}
    if actual_paths != expected_paths:
        raise ValueError(
            "baseline successor file set drifted: "
            f"missing={sorted(expected_paths - actual_paths)} "
            f"extra={sorted(actual_paths - expected_paths)}"
        )
    records = manifest.get("files")
    if not isinstance(records, list):
        raise ValueError("baseline successor manifest files must be a list")
    record_by_path = {str(record.get("path", "")): record for record in records}
    if set(record_by_path) != set(EXPECTED_PREDECESSOR_SHA256):
        raise ValueError("baseline successor manifest file records drifted")
    for logical_path, predecessor_sha256 in EXPECTED_PREDECESSOR_SHA256.items():
        record = record_by_path[logical_path]
        path = (root / logical_path).resolve(strict=True)
        if root not in path.parents or path.is_symlink() or not path.is_file():
            raise ValueError(f"staged file escaped or is not regular: {logical_path}")
        if _sha256(path) != record.get("sha256"):
            raise ValueError(f"staged file SHA256 mismatch: {logical_path}")
        if record.get("predecessor_sha256") != predecessor_sha256:
            raise ValueError(f"predecessor file binding drifted: {logical_path}")
        expected_mode = (
            "patch_v9_startup_order_only" if logical_path == "live/main.py" else "restore_exact_v9"
        )
        if record.get("mode") != expected_mode:
            raise ValueError(f"staged file mode drifted: {logical_path}")
        if logical_path != "live/main.py" and _sha256(path) != predecessor_sha256:
            raise ValueError(f"v9 restore file is not byte-exact: {logical_path}")
        if logical_path.endswith(".py"):
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    predecessor_main = Path(sources["live/main.py"]).read_text(encoding="utf-8")
    candidate_main = (root / "live/main.py").read_text(encoding="utf-8")
    _validate_main_successor(
        predecessor=predecessor_main,
        candidate=candidate_main,
    )
    candidate_config_bytes = (root / "live/config.yaml").read_bytes()
    predecessor_config_bytes = Path(sources["live/config.yaml"]).read_bytes()
    if candidate_config_bytes != predecessor_config_bytes:
        raise ValueError("successor config is not byte-equal to v9")
    candidate_config = yaml.safe_load(candidate_config_bytes.decode("utf-8"))
    if not isinstance(candidate_config, dict):
        raise ValueError("successor config must be a mapping")
    _assert_journal_disabled(
        staged_files={path: (root / path).read_bytes() for path in EXPECTED_PREDECESSOR_SHA256},
        config=candidate_config,
    )
    successor_projection = _verify_baseline_contract(
        identity=identity,
        config=candidate_config,
    )
    expected_semantic_sha256 = _canonical_sha256(semantic_projection)
    if _canonical_sha256(successor_projection) != expected_semantic_sha256:
        raise ValueError("successor strategy/config semantic identity drifted")
    equality = manifest.get("strategy_config_semantic_equality")
    if not isinstance(equality, Mapping) or equality.get("passed") is not True:
        raise ValueError("strategy/config semantic equality is not bound")
    if {
        equality.get("predecessor_semantic_identity_sha256"),
        equality.get("successor_semantic_identity_sha256"),
    } != {expected_semantic_sha256}:
        raise ValueError("strategy/config semantic identity hash drifted")
    journal = manifest.get("journal_boundary")
    expected_journal = {
        "lifecycle_journal_enabled": False,
        "lifecycle_journal_config_present": False,
        "lifecycle_journal_runtime_imported": False,
        "journal_payload_files_included": False,
        "economic_outcomes_read": False,
    }
    if journal != expected_journal:
        raise ValueError("journal-disabled boundary drifted")
    rollback = manifest.get("rollback_binding")
    if not isinstance(rollback, Mapping):
        raise ValueError("rollback binding is missing")
    if rollback.get("startup_contract") != STARTUP_CONTRACT:
        raise ValueError("rollback startup contract drifted")
    if rollback.get("stable_start_probe") != _stable_start_probe_contract():
        raise ValueError("rollback stable-start probe drifted")
    permissions = manifest.get("permissions")
    if not isinstance(permissions, Mapping) or any(
        permissions.get(field) is not False
        for field in (
            "deployment_executed",
            "deployment_authorized",
            "rollback_authorized",
            "registry_modified",
            "current_pointer_modified",
            "strategy_action_authorized",
            "economic_research_authorized",
        )
    ):
        raise ValueError("local-only permission boundary drifted")
    return {
        "schema_version": "warmup_before_websocket_baseline_successor_validation.v1",
        "release_id": RELEASE_ID,
        "manifest_sha256": claimed_manifest_sha256,
        "startup_contract": STARTUP_CONTRACT,
        "file_count": len(records),
        "only_startup_order_changed": True,
        "strategy_config_semantic_equality_passed": True,
        "journal_enabled": False,
        "economic_outcomes_read": False,
        "deployment_authorized": False,
        "rollback_authorized": False,
        "payload_valid": True,
    }


def build_release(
    *,
    repo_root: Path,
    output_root: Path,
    predecessor_sources: Mapping[str, Path] | None = None,
    predecessor_identity_path: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    repo = repo_root.expanduser().resolve(strict=True)
    expected_python = (repo / ".venv/bin/python").resolve(strict=True)
    if Path(sys.executable).resolve() != expected_python or sys.version_info < (3, 10):
        raise RuntimeError("builder must run with repository .venv Python >=3.10")
    sources = dict(predecessor_sources or PREDECESSOR_SOURCE_DEFAULTS)
    identity_path = (predecessor_identity_path or repo / PREDECESSOR_IDENTITY_RELATIVE).expanduser()
    identity, semantic_projection = _verify_inputs(
        repo_root=repo,
        predecessor_sources=sources,
        predecessor_identity_path=identity_path,
    )
    output = output_root.expanduser().resolve()
    if output.exists():
        raise FileExistsError(f"baseline successor output already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.parent / (f".{output.name}.partial-{os.getpid()}-{uuid.uuid4().hex}")
    temporary.mkdir()
    try:
        manifest = _build_payload(
            stage_root=temporary,
            predecessor_sources=sources,
            identity=identity,
            semantic_projection=semantic_projection,
        )
        validation = validate_staging(
            temporary,
            repo_root=repo,
            predecessor_sources=sources,
            predecessor_identity_path=identity_path,
        )
        os.replace(temporary, output)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    final_validation = validate_staging(
        output,
        repo_root=repo,
        predecessor_sources=sources,
        predecessor_identity_path=identity_path,
    )
    if final_validation != validation:
        raise AssertionError("validation changed after atomic publication")
    return manifest, final_validation


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    build.add_argument("--output-root", type=Path, required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--candidate-root", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "build":
        manifest, validation = build_release(
            repo_root=args.repo_root,
            output_root=args.output_root,
        )
        print(
            json.dumps(
                {
                    "manifest_sha256": manifest["manifest_sha256"],
                    "validation": validation,
                },
                sort_keys=True,
            )
        )
        return 0
    result = validate_staging(args.candidate_root)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
