#!/usr/bin/env python3
"""
NarrowGate Maker — 实盘入口。

启动流程:
  1. 加载配置 (config.yaml + 环境变量)
  2. 初始化REST客户端 + 做市引擎
  3. 同步交易所持仓
  4. 启动WebSocket (行情 + 用户数据)
  5. 进入主循环 (requote + 健康检查)
  6. SIGINT/SIGTERM → 优雅退出

Usage:
  # 测试网
  BINANCE_API_KEY=xxx BINANCE_API_SECRET=yyy python live/main.py

  # 正式网
  BINANCE_API_KEY=xxx BINANCE_API_SECRET=yyy python live/main.py --live

  # 正式本地 dry-run (校验后退出，不创建网络客户端或订单路径)
  python live/main.py --dry-run --config live/formal_dry_run_public.yaml
"""

import argparse
import hashlib
import importlib
import json
import logging
import logging.handlers
import math
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

# Add project root to path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from live.config import install_reload_handler, load_config, set_engine_ref
from live.runtime_policy import (
    f05_boolean_cooldown_runtime_policy,
    f05_buy_e3_active_release_runtime_authority,
    f05_buy_e3_runtime_policy,
    q90_action_runtime_policy,
    write_runtime_identity,
)
from live.ws_handler import WSHandler
from features.feature_dag import TEN_SECOND_CAUSAL_GRAPH
from execution.order_lifecycle_live_writer_v2 import OrderLifecycleLiveWriterV2
from models.replay.prospective_baseline_epoch import (
    live_clock_semantics_identity,
    publish_prospective_baseline_epoch,
    snapshot_action_enablement,
    snapshot_data_source_identity,
)
from strategy.maker_engine import MakerEngine


CPP_RUNTIME_FLAGS = (
    "NARROWGATE_CPP_QUOTE_CORE",
    "NARROWGATE_CPP_SIGNAL_FEATURES",
    "NARROWGATE_CPP_GLOBAL_FLOW",
    "NARROWGATE_CPP_LIVE_ROUTING",
    "NARROWGATE_CPP_STRICT",
)

FORMAL_DRY_RUN_SCHEMA = "narrowgate.live_dry_run.v1"
DEFAULT_DRY_RUN_TIMEOUT_S = 30.0
DRY_RUN_TIMEOUT_EXIT_CODE = 124

PROSPECTIVE_EPOCH_RUNTIME_CODE_ROOTS = ("live", "strategy", "execution", "features")
PROSPECTIVE_EPOCH_RUNTIME_CODE_FILES = (
    "market_fusion.py",
    "models/replay/baseline_epoch_manifest.py",
    "models/replay/prospective_baseline_epoch.py",
)


class FormalDryRunTimeout(TimeoutError):
    """Raised when local validation exceeds its explicit deadline."""


def _positive_finite_seconds(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timeout must be a number") from exc
    if not math.isfinite(seconds) or seconds <= 0.0:
        raise argparse.ArgumentTypeError("timeout must be finite and greater than zero")
    return seconds


def _configured_model_dir(cfg) -> Path:
    raw = str(getattr(cfg.ml, "model_dir", "") or "").strip()
    if not raw:
        raise ValueError("ml.model_dir must identify a model bundle")
    model_dir = Path(raw).expanduser()
    if not model_dir.is_absolute():
        model_dir = ROOT / model_dir
    return model_dir.resolve()


def run_formal_dry_run(
    config_path: Path,
    *,
    timeout_s: float = DEFAULT_DRY_RUN_TIMEOUT_S,
    output=None,
) -> int:
    """Validate local startup inputs under a deadline and emit one JSON result."""

    if not math.isfinite(timeout_s) or timeout_s <= 0.0:
        raise ValueError("dry-run timeout must be finite and greater than zero")
    if output is None:
        output = sys.stdout
    started = time.monotonic()
    summary = {
        "schema_version": FORMAL_DRY_RUN_SCHEMA,
        "mode": "formal_dry_run",
        "status": "failed",
        "exit_code": 1,
        "timeout_s": float(timeout_s),
        "termination": "validation_failed",
        "config": {"path": str(Path(config_path).expanduser())},
        "safety": {
            "network_allowed": False,
            "exchange_clients_created": 0,
            "threads_started": 0,
            "order_path_entered": False,
            "orders_submitted": 0,
        },
        "authority": {
            "scope": "local_validation_only",
            "live_trading_authorized": False,
            "remote_deploy_authorized": False,
        },
    }

    def deadline_exceeded(_signum, _frame):
        raise FormalDryRunTimeout(
            f"formal dry-run exceeded {float(timeout_s):g}s deadline"
        )

    previous_handler = signal.getsignal(signal.SIGALRM)
    deadline_armed = False
    exit_code = 1
    try:
        signal.signal(signal.SIGALRM, deadline_exceeded)
        signal.setitimer(signal.ITIMER_REAL, timeout_s)
        deadline_armed = True
        resolved_config = Path(config_path).expanduser().resolve()
        cfg = load_config(resolved_config)

        from strategy.model_contract import (
            REQUIRED_FEATURE_DAG_ID,
            REQUIRED_FEATURE_DAG_SHA256,
            REQUIRED_MODEL_HEADS,
            validate_model_bundle,
        )

        model_dir = _configured_model_dir(cfg)
        model_metadata = validate_model_bundle(model_dir)
        p3_path = model_dir / "fill_prob_params.json"
        if not p3_path.is_file():
            raise ValueError(f"model bundle is missing fill_prob_params.json: {p3_path}")
        summary.update(
            {
                "status": "passed",
                "exit_code": 0,
                "termination": "completed",
                "config": {
                    "path": str(resolved_config),
                    "sha256": hashlib.sha256(resolved_config.read_bytes()).hexdigest(),
                },
                "model_contract": {
                    "model_dir": str(model_dir),
                    "required_head_count": len(REQUIRED_MODEL_HEADS),
                    "validated_head_count": len(model_metadata),
                    "validated_heads": sorted(model_metadata),
                    "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
                    "feature_dag_sha256": REQUIRED_FEATURE_DAG_SHA256,
                    "p3_path": str(p3_path),
                    "p3_sha256": hashlib.sha256(p3_path.read_bytes()).hexdigest(),
                    "ml_enabled": bool(cfg.ml.enabled),
                },
            }
        )
        exit_code = 0
    except FormalDryRunTimeout as exc:
        exit_code = DRY_RUN_TIMEOUT_EXIT_CODE
        summary.update(
            {
                "status": "timed_out",
                "exit_code": exit_code,
                "termination": "deadline_exceeded",
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
        )
    except Exception as exc:
        summary["error"] = {"type": type(exc).__name__, "message": str(exc)}
    finally:
        if deadline_armed:
            signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)

    summary["elapsed_ms"] = round((time.monotonic() - started) * 1000.0, 3)
    print(
        json.dumps(summary, sort_keys=True, separators=(",", ":")),
        file=output,
        flush=True,
    )
    return exit_code

STARTUP_ATTESTATION_SCHEMA = "narrowgate_buy_e3_startup_attestation.v4"
RUNNING_CHECKOUT_SCHEMA = "narrowgate_running_checkout_identity.v2"
INTERPRETER_IDENTITY_SCHEMA = "narrowgate_interpreter_identity.v1"
NATIVE_RUNTIME_IDENTITY_SCHEMA = "narrowgate_native_runtime_identity.v1"
STARTUP_ATTESTATION_GATE_NAMES = (
    "fill_cooldown_state_available",
    "fill_cooldown_state_schema_v2",
    "fill_cooldown_restore_mode_valid",
    "fill_cooldown_checkpoint_binding_valid",
    "fill_cooldown_deadline_contract_valid",
    "fill_cooldown_artifact_contract_valid",
    "buy_e3_active_release_contract_valid",
    "buy_e3_active_release_matches_checkout",
    "shadow_config_explicit",
    "global_flow_shadow_backend_contract_valid",
    "global_reference_shadow_state_contract_valid",
    "git_toplevel_matches_repo",
    "git_pre_snapshot_available",
    "git_pre_snapshot_stable",
    "git_pre_worktree_clean",
    "runtime_source_manifest_available",
    "runtime_files_match_head",
    "loaded_module_origins_available",
    "loaded_module_origins_under_repo",
    "loaded_module_origins_match_runtime_sources",
    "interpreter_identity_available",
    "interpreter_identity_stable",
    "native_runtime_matches_initial_identity",
    "native_runtime_contract_valid",
    "native_runtime_identity_available",
    "native_runtime_identity_stable",
    "git_post_snapshot_available",
    "git_post_snapshot_stable",
    "git_post_worktree_clean",
    "git_snapshot_stable",
    "safe_to_start_live_loops",
)
KEY_LOADED_RUNTIME_MODULES = {
    "live_main": ("live.main", "live/main.py"),
    "live_config": ("live.config", "live/config.py"),
    "live_runtime_policy": ("live.runtime_policy", "live/runtime_policy.py"),
    "live_ws_handler": ("live.ws_handler", "live/ws_handler.py"),
    "maker_engine": ("strategy.maker_engine", "strategy/maker_engine.py"),
    "signal_engine": ("strategy.signal", "strategy/signal.py"),
    "global_flow": ("strategy.global_flow", "strategy/global_flow.py"),
    "boolean_cooldown_live": (
        "strategy.boolean_cooldown_live",
        "strategy/boolean_cooldown_live.py",
    ),
    "boolean_cooldown_buy_e3": (
        "strategy.boolean_cooldown_buy_e3",
        "strategy/boolean_cooldown_buy_e3.py",
    ),
}


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _git_output(*args: str) -> bytes:
    completed = subprocess.run(
        ("git", *args),
        cwd=ROOT,
        check=True,
        capture_output=True,
        timeout=10.0,
    )
    return completed.stdout


def _git_snapshot() -> dict:
    def capture() -> tuple[str, str, bytes]:
        commit = _git_output("rev-parse", "HEAD").decode("ascii").strip()
        tree = _git_output("rev-parse", "HEAD^{tree}").decode("ascii").strip()
        status = _git_output("status", "--porcelain=v1", "--untracked-files=all")
        return commit, tree, status

    first = capture()
    second = capture()
    commit, tree, status = first
    return {
        "commit": commit,
        "tree": tree,
        "status_porcelain_sha256": _sha256_bytes(status),
        "status_entry_count": len(status.splitlines()),
        "worktree_clean": not status,
        "snapshot_internally_stable": first == second,
    }


def _runtime_source_rows() -> list[dict]:
    rows = []
    for relative in sorted({value[1] for value in KEY_LOADED_RUNTIME_MODULES.values()}):
        path = ROOT / relative
        working = path.read_bytes()
        head = _git_output("show", f"HEAD:{relative}")
        rows.append(
            {
                "path": relative,
                "working_file_sha256": _sha256_bytes(working),
                "head_blob_sha256": _sha256_bytes(head),
                "working_size_bytes": len(working),
                "head_blob_size_bytes": len(head),
                "matches_head_blob": working == head,
            }
        )
    return rows


def _runtime_source_manifest_sha256(rows: list[dict]) -> str:
    return _sha256_bytes(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def _file_byte_identity(path: str | Path) -> dict:
    reported = Path(path).expanduser().absolute()
    resolved = reported.resolve(strict=True)
    payload = resolved.read_bytes()
    return {
        "reported_path": str(reported),
        "resolved_path": str(resolved),
        "sha256": _sha256_bytes(payload),
        "size_bytes": len(payload),
    }


def _loaded_module_origins(source_rows: list[dict]) -> dict:
    source_by_path = {row["path"]: row for row in source_rows}
    output = {}
    for role, (module_name, expected_relative) in KEY_LOADED_RUNTIME_MODULES.items():
        module = importlib.import_module(module_name)
        origin = Path(str(module.__file__)).resolve(strict=True)
        relative = origin.relative_to(ROOT).as_posix()
        if relative != expected_relative:
            raise RuntimeError(f"loaded module origin drifted: {role}")
        output[role] = {
            "module_name": module_name,
            "origin_path": str(origin),
            "repository_relative_path": relative,
            "source_sha256": source_by_path[relative]["working_file_sha256"],
        }
    return output


def _native_runtime_file_identity(native_runtime: dict) -> dict | None:
    enabled = any(
        bool(native_runtime.get(name, False))
        for name in CPP_RUNTIME_FLAGS
        if name != "NARROWGATE_CPP_STRICT"
    )
    if not enabled:
        return None
    module_path = str(native_runtime.get("module", "")).strip()
    if not module_path or module_path.startswith("unavailable:"):
        raise RuntimeError("enabled native runtime has no loadable module identity")
    return _file_byte_identity(module_path)


def _empty_startup_attestation() -> dict:
    return {
        "schema_version": STARTUP_ATTESTATION_SCHEMA,
        "status": "rejected",
        "attested_at_utc": "",
        "fill_cooldown_state": {},
        "shadow_runtime_identity": {},
        "running_checkout": {},
        "loaded_module_origins": {},
        "interpreter_identity": {},
        "native_runtime_identity": {},
        "gates": {name: False for name in STARTUP_ATTESTATION_GATE_NAMES},
        "errors": [],
    }


def build_startup_attestation(*, engine: MakerEngine, native_runtime: dict) -> dict:
    """Bind the checkout and restored cooldown state before live loops start."""

    git_toplevel = Path(
        _git_output("rev-parse", "--show-toplevel").decode("utf-8").strip()
    ).resolve(strict=True)
    interpreter_before = _file_byte_identity(sys.executable)
    native_before = _native_runtime_file_identity(native_runtime)
    pre_snapshot = _git_snapshot()
    source_rows = _runtime_source_rows()
    loaded_origins = _loaded_module_origins(source_rows)
    fill_state = engine.fill_cooldown_state_snapshot()
    shadow_runtime = engine.shadow_runtime_snapshot()
    active_release = engine.buy_e3_active_release_identity()
    post_snapshot = _git_snapshot()
    interpreter_after = _file_byte_identity(sys.executable)
    native_after = _native_runtime_file_identity(native_runtime)

    snapshots_equal = pre_snapshot == post_snapshot
    runtime_files_match = bool(source_rows) and all(row["matches_head_blob"] for row in source_rows)
    loaded_under_repo = all(
        Path(row["origin_path"]).is_relative_to(ROOT) for row in loaded_origins.values()
    )
    source_by_path = {row["path"]: row for row in source_rows}
    loaded_match_sources = all(
        row["source_sha256"]
        == source_by_path[row["repository_relative_path"]]["working_file_sha256"]
        for row in loaded_origins.values()
    )
    native_enabled = native_before is not None
    native_identity = {
        "schema_version": NATIVE_RUNTIME_IDENTITY_SCHEMA,
        "profile": str(native_runtime.get("profile", "")),
        "platform": sys.platform,
        "enabled": native_enabled,
        "reported_module_path": (
            str(native_runtime.get("module", "")) if native_enabled else "disabled"
        ),
        "loaded_module_origin_path": (
            native_before["resolved_path"] if native_before is not None else None
        ),
        "before": native_before,
        "after": native_after,
        "stable": native_before == native_after,
    }
    interpreter_identity = {
        "schema_version": INTERPRETER_IDENTITY_SCHEMA,
        "version": ".".join(map(str, sys.version_info[:3])),
        "before": interpreter_before,
        "after": interpreter_after,
        "stable": interpreter_before == interpreter_after,
    }
    stable_snapshot = {
        "pre_snapshot_internally_stable": bool(pre_snapshot["snapshot_internally_stable"]),
        "post_snapshot_internally_stable": bool(post_snapshot["snapshot_internally_stable"]),
        "commit_identical": pre_snapshot["commit"] == post_snapshot["commit"],
        "tree_identical": pre_snapshot["tree"] == post_snapshot["tree"],
        "status_identical": (
            pre_snapshot["status_porcelain_sha256"] == post_snapshot["status_porcelain_sha256"]
        ),
        "runtime_files_match_head": runtime_files_match,
        "stable": snapshots_equal and runtime_files_match,
    }
    checkout = {
        "schema_version": RUNNING_CHECKOUT_SCHEMA,
        "git_commit": post_snapshot["commit"],
        "git_tree": post_snapshot["tree"],
        "git_worktree_clean": bool(post_snapshot["worktree_clean"]),
        "pre_snapshot": pre_snapshot,
        "post_snapshot": post_snapshot,
        "stable_snapshot": stable_snapshot,
        "runtime_source_file_count": len(source_rows),
        "runtime_source_manifest_sha256": _runtime_source_manifest_sha256(source_rows),
        "runtime_source_files": source_rows,
    }
    restore_mode = str(fill_state.get("restore_mode", ""))
    checkpoint_loaded = fill_state.get("checkpoint_loaded")
    checkpoint_sequence = fill_state.get("checkpoint_sequence")
    buy_identity = str(fill_state.get("buy_deadline_identity", ""))
    buy_remaining_ms = fill_state.get("buy_remaining_ms")
    active_identity_reader = getattr(engine, "_active_buy_e3_deadline_identity", None)
    runtime_active_identity = (
        str(active_identity_reader())
        if callable(active_identity_reader)
        else "B0"
    )
    release_required = runtime_active_identity.startswith("BUY_E3:")
    release_available = all(
        bool(active_release.get(name))
        for name in (
            "path",
            "file_sha256",
            "file_canonical_sha256",
            "execution_commit",
            "execution_tree",
            "annotated_operational_tag",
            "annotated_operational_tag_object",
        )
    )
    release_contract_valid = (
        release_available if release_required else not any(active_release.values())
    )
    release_matches_checkout = (
        (
            active_release.get("execution_commit") == checkout["git_commit"]
            and active_release.get("execution_tree") == checkout["git_tree"]
        )
        if release_required
        else True
    )
    flow_enabled = bool(shadow_runtime.get("global_flow_shadow_enabled", False))
    reference_enabled = bool(
        shadow_runtime.get("global_reference_shadow_enabled", False)
    )
    flow_backend = shadow_runtime.get("global_flow_backend", {})
    flow_zero_fields = (
        "native",
        "market_count",
        "trade_batches",
        "trade_events_seen",
        "trade_events_accepted",
        "book_events_seen",
        "book_events_accepted",
        "out_of_order_events",
        "stale_trade_events",
        "trade_overflow_events",
        "book_overflow_events",
    )
    flow_backend_zero = all(
        type(flow_backend.get(name)) is int and flow_backend.get(name) == 0
        for name in flow_zero_fields
    )
    shadow_config_explicit = bool(
        shadow_runtime.get("global_flow_shadow_config_explicit", False)
        and shadow_runtime.get("global_reference_shadow_config_explicit", False)
    )
    global_flow_contract_valid = bool(
        flow_enabled
        or (
            flow_backend_zero
            and not shadow_runtime.get("global_flow_native_effective", False)
            and shadow_runtime.get("state_restore_contract")
            == "shadow_state_never_restored"
        )
    )
    global_reference_contract_valid = bool(
        reference_enabled
        or shadow_runtime.get("global_reference_bridge_basis_sample_count") == 0
    )
    admitted_restore_modes = {
        "fresh_b0_no_checkpoint",
        "exact_same_artifact_resume",
        "rollback_to_b0",
        "artifact_identity_changed_to_b0",
        "b0_checkpoint_resume",
        "expired_to_b0",
    }
    checkpoint_binding_valid = (
        (
            restore_mode == "fresh_b0_no_checkpoint"
            and not release_required
            and checkpoint_loaded is False
            and isinstance(checkpoint_sequence, int)
            and not isinstance(checkpoint_sequence, bool)
            and checkpoint_sequence == 0
        )
        or (
            restore_mode != "fresh_b0_no_checkpoint"
            and checkpoint_loaded is True
            and isinstance(checkpoint_sequence, int)
            and not isinstance(checkpoint_sequence, bool)
            and checkpoint_sequence > 0
        )
    )
    remaining_valid = (
        isinstance(buy_remaining_ms, int)
        and not isinstance(buy_remaining_ms, bool)
        and buy_remaining_ms >= 0
    )
    if restore_mode == "exact_same_artifact_resume":
        deadline_contract_valid = (
            remaining_valid
            and buy_remaining_ms > 0
            and buy_identity.startswith("BUY_E3:")
            and buy_identity == runtime_active_identity
        )
    elif restore_mode in {
        "rollback_to_b0",
        "artifact_identity_changed_to_b0",
        "b0_checkpoint_resume",
    }:
        deadline_contract_valid = remaining_valid and buy_identity == "B0"
    elif restore_mode in {"fresh_b0_no_checkpoint", "expired_to_b0"}:
        deadline_contract_valid = (
            remaining_valid and buy_remaining_ms == 0 and buy_identity == "B0"
        )
    else:
        deadline_contract_valid = False
    gates = {
        "fill_cooldown_state_available": bool(fill_state),
        "fill_cooldown_state_schema_v2": (
            fill_state.get("schema_version") == "narrowgate_fill_cooldown_state.v2"
        ),
        "fill_cooldown_restore_mode_valid": restore_mode in admitted_restore_modes,
        "fill_cooldown_checkpoint_binding_valid": checkpoint_binding_valid,
        "fill_cooldown_deadline_contract_valid": deadline_contract_valid,
        "fill_cooldown_artifact_contract_valid": (
            (
                restore_mode == "exact_same_artifact_resume"
                and runtime_active_identity.startswith("BUY_E3:")
                and buy_identity == runtime_active_identity
            )
            or (
                restore_mode == "artifact_identity_changed_to_b0"
                and runtime_active_identity.startswith("BUY_E3:")
            )
            or (
                restore_mode == "rollback_to_b0"
                and runtime_active_identity == "B0"
            )
            or restore_mode
            in {
                "b0_checkpoint_resume",
                "expired_to_b0",
            }
            or (
                restore_mode == "fresh_b0_no_checkpoint"
                and runtime_active_identity == "B0"
            )
        ),
        "buy_e3_active_release_contract_valid": release_contract_valid,
        "buy_e3_active_release_matches_checkout": release_matches_checkout,
        "shadow_config_explicit": shadow_config_explicit,
        "global_flow_shadow_backend_contract_valid": global_flow_contract_valid,
        "global_reference_shadow_state_contract_valid": (
            global_reference_contract_valid
        ),
        "git_toplevel_matches_repo": git_toplevel == ROOT,
        "git_pre_snapshot_available": bool(pre_snapshot),
        "git_pre_snapshot_stable": bool(pre_snapshot["snapshot_internally_stable"]),
        "git_pre_worktree_clean": bool(pre_snapshot["worktree_clean"]),
        "runtime_source_manifest_available": bool(source_rows),
        "runtime_files_match_head": runtime_files_match,
        "loaded_module_origins_available": (set(loaded_origins) == set(KEY_LOADED_RUNTIME_MODULES)),
        "loaded_module_origins_under_repo": loaded_under_repo,
        "loaded_module_origins_match_runtime_sources": loaded_match_sources,
        "interpreter_identity_available": bool(interpreter_identity),
        "interpreter_identity_stable": bool(interpreter_identity["stable"]),
        "native_runtime_matches_initial_identity": native_before == native_after,
        "native_runtime_contract_valid": (
            (not native_enabled and native_runtime.get("module") == "disabled")
            or (native_enabled and native_before is not None)
        )
        and bool(
            native_runtime.get("NARROWGATE_CPP_GLOBAL_FLOW_EFFECTIVE", False)
        )
        == bool(shadow_runtime.get("global_flow_native_effective", False)),
        "native_runtime_identity_available": bool(native_identity),
        "native_runtime_identity_stable": bool(native_identity["stable"]),
        "git_post_snapshot_available": bool(post_snapshot),
        "git_post_snapshot_stable": bool(post_snapshot["snapshot_internally_stable"]),
        "git_post_worktree_clean": bool(post_snapshot["worktree_clean"]),
        "git_snapshot_stable": bool(stable_snapshot["stable"]),
        "safe_to_start_live_loops": False,
    }
    gates["safe_to_start_live_loops"] = all(
        value for name, value in gates.items() if name != "safe_to_start_live_loops"
    )
    errors = sorted(name for name, passed in gates.items() if not passed)
    return {
        "schema_version": STARTUP_ATTESTATION_SCHEMA,
        "status": "accepted" if not errors else "rejected",
        "attested_at_utc": datetime.now(UTC).isoformat(),
        "fill_cooldown_state": fill_state,
        "shadow_runtime_identity": shadow_runtime,
        "buy_e3_active_release": active_release,
        "running_checkout": checkout,
        "loaded_module_origins": loaded_origins,
        "interpreter_identity": interpreter_identity,
        "native_runtime_identity": native_identity,
        "gates": gates,
        "errors": errors,
    }


def prospective_epoch_runtime_code_paths(repo_root: Path) -> tuple[str, ...]:
    """Enumerate the Python runtime surface bound by a prospective epoch."""

    paths = set(PROSPECTIVE_EPOCH_RUNTIME_CODE_FILES)
    for relative_root in PROSPECTIVE_EPOCH_RUNTIME_CODE_ROOTS:
        root = repo_root / relative_root
        paths.update(
            candidate.relative_to(repo_root).as_posix()
            for candidate in root.rglob("*.py")
            if "__pycache__" not in candidate.parts
        )
    missing = sorted(relative for relative in paths if not (repo_root / relative).is_file())
    if missing:
        raise RuntimeError(
            "prospective epoch runtime code identity has missing files: " + ", ".join(missing)
        )
    return tuple(sorted(paths))


def audit_native_runtime(logger: logging.Logger, *, cfg=None) -> dict:
    """Log the persisted runtime profile and fail fast for broken strict native mode."""
    values = {name: os.environ.get(name, "0") for name in CPP_RUNTIME_FLAGS}
    enabled = {
        name: str(value).strip().lower() in {"1", "true", "yes", "on"}
        for name, value in values.items()
    }
    profile = os.environ.get("NARROWGATE_LIVE_PROFILE_NAME", "unmanaged")
    module_path = "disabled"
    required = set()
    if enabled["NARROWGATE_CPP_QUOTE_CORE"]:
        required.add("compute_quote_core_live")
    if enabled["NARROWGATE_CPP_LIVE_ROUTING"]:
        required.add("compute_live_routing_decision")
    if enabled["NARROWGATE_CPP_SIGNAL_FEATURES"]:
        required.update({"SignalFeatureEngine", "SIGNAL_FEATURE_NAMES"})
    global_flow_effective = bool(
        enabled["NARROWGATE_CPP_GLOBAL_FLOW"]
        and cfg is not None
        and bool(getattr(getattr(cfg, "multi_market", None), "global_flow_shadow_enabled", False))
    )
    if enabled["NARROWGATE_CPP_GLOBAL_FLOW"]:
        required.add("TradeBarAggregator")
    if global_flow_effective:
        required.add("NativeGlobalFlowEngine")

    if required:
        try:
            module = importlib.import_module("narrowgate_cpp")
            module_path = str(getattr(module, "__file__", "<unknown>"))
            missing = sorted(name for name in required if not hasattr(module, name))
            if missing:
                raise RuntimeError(f"narrowgate_cpp missing APIs: {', '.join(missing)}")
            if enabled["NARROWGATE_CPP_QUOTE_CORE"]:
                abi_fields = {
                    "QuoteFlags": ("delta_cap", "final_compressed", "cap_exposure_block"),
                    "SideQuoteContext": ("cap_exposure_block",),
                }
                missing_fields = []
                for class_name, field_names in abi_fields.items():
                    cls = getattr(module, class_name, None)
                    instance = cls() if cls is not None else None
                    for field_name in field_names:
                        if instance is None or not hasattr(instance, field_name):
                            missing_fields.append(f"{class_name}.{field_name}")
                if missing_fields:
                    raise RuntimeError(
                        "narrowgate_cpp ABI missing fields: " + ", ".join(missing_fields)
                    )
            if enabled["NARROWGATE_CPP_GLOBAL_FLOW"]:
                aggregator = module.TradeBarAggregator(False)
                if not hasattr(aggregator, "update_batch"):
                    raise RuntimeError("narrowgate_cpp ABI missing TradeBarAggregator.update_batch")
        except Exception as exc:
            if enabled["NARROWGATE_CPP_STRICT"]:
                raise RuntimeError(
                    f"strict native profile {profile!r} failed preflight: {exc}"
                ) from exc
            logger.warning("Native runtime requested but unavailable: %s", exc)
            module_path = f"unavailable:{exc}"

    logger.info(
        "NATIVE_PROFILE name=%s quote_core=%d signal_features=%d "
        "global_flow_requested=%d global_flow_effective=%d "
        "live_routing=%d strict=%d module=%s",
        profile,
        int(enabled["NARROWGATE_CPP_QUOTE_CORE"]),
        int(enabled["NARROWGATE_CPP_SIGNAL_FEATURES"]),
        int(enabled["NARROWGATE_CPP_GLOBAL_FLOW"]),
        int(global_flow_effective),
        int(enabled["NARROWGATE_CPP_LIVE_ROUTING"]),
        int(enabled["NARROWGATE_CPP_STRICT"]),
        module_path,
    )
    return {
        "profile": profile,
        "module": module_path,
        **enabled,
        "NARROWGATE_CPP_GLOBAL_FLOW_REQUESTED": enabled[
            "NARROWGATE_CPP_GLOBAL_FLOW"
        ],
        "NARROWGATE_CPP_GLOBAL_FLOW_EFFECTIVE": global_flow_effective,
    }


def setup_logging(cfg):
    """Configure logging.  cfg.logging paths must already be absolute."""
    level = getattr(logging, cfg.logging.level.upper(), logging.INFO)

    handlers = []
    if cfg.logging.console:
        handlers.append(logging.StreamHandler(sys.stdout))
    if cfg.logging.file:
        log_path = Path(cfg.logging.file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        rotating = logging.handlers.RotatingFileHandler(
            str(log_path), maxBytes=10_000_000, backupCount=5
        )
        handlers.append(rotating)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )


def collect_global_shadow_health(*, engine: MakerEngine, cfg, logger) -> tuple[dict, dict]:
    """Collect optional shadow state without evaluating disabled diagnostics."""
    ref_enabled = bool(cfg.multi_market.global_reference_shadow_enabled)
    flow_enabled = bool(cfg.multi_market.global_flow_shadow_enabled)
    disabled_runtime = None
    disabled_runtime_error = None
    if not ref_enabled or not flow_enabled:
        try:
            disabled_runtime = engine.signal.shadow_runtime_snapshot()
        except Exception as exc:
            disabled_runtime_error = exc
            logger.warning("Disabled shadow runtime identity failed: %s", exc)
    ref = {
        "enabled": int(ref_enabled),
        "state_error": 0,
        "valid": 0,
        "confidence": 0.0,
        "spot": 0.0,
        "perp": 0.0,
        "divergence": 0.0,
        "residual": 0.0,
        "fresh_spot": 0,
        "fresh_perp": 0,
        "dispersion": 0.0,
        "basis_samples": 0,
        "reason": "disabled_by_config",
    }
    if not ref_enabled:
        if disabled_runtime_error is not None:
            ref.update({"state_error": 1, "reason": "error"})
        else:
            basis_samples = disabled_runtime.get(
                "global_reference_bridge_basis_sample_count"
            )
            if type(basis_samples) is not int or basis_samples < 0:
                ref.update({"state_error": 1, "reason": "identity_malformed"})
            else:
                ref["basis_samples"] = basis_samples
            if disabled_runtime.get("global_reference_shadow_enabled") is not False:
                ref.update({"state_error": 1, "reason": "config_mismatch"})
    else:
        try:
            state = engine.signal.global_reference_state(tick_size=cfg.tick_size)
            ref.update(
                {
                    "valid": int(state.valid),
                    "confidence": state.confidence,
                    "spot": state.global_spot_move_bps,
                    "perp": state.global_perp_move_bps,
                    "divergence": state.perp_spot_divergence_bps,
                    "residual": state.residual_bps,
                    "fresh_spot": state.fresh_spot_venues,
                    "fresh_perp": state.fresh_perp_venues,
                    "dispersion": state.cross_venue_dispersion_bps,
                    "basis_samples": state.bridge_basis_sample_count,
                    "reason": state.validity_reason,
                }
            )
        except Exception as exc:
            logger.warning("Global reference shadow state failed: %s", exc)
            ref.update({"state_error": 1, "reason": "error"})

    flow = {
        "enabled": int(flow_enabled),
        "state_error": 0,
        "valid": 0,
        "pressure": 0.0,
        "pending": 0.0,
        "spot_pressure": 0.0,
        "perp_pressure": 0.0,
        "spot_agreement": 0.0,
        "perp_agreement": 0.0,
        "fresh_spot": 0,
        "fresh_perp": 0,
        "native": 0,
        "market_count": 0,
        "trade_batches": 0,
        "trade_events_seen": 0,
        "trade_events_accepted": 0,
        "book_events_seen": 0,
        "book_events_accepted": 0,
        "out_of_order_events": 0,
        "stale_trade_events": 0,
        "trade_overflow_events": 0,
        "book_overflow_events": 0,
        "reason": "disabled_by_config",
    }
    if not flow_enabled:
        if disabled_runtime_error is not None:
            flow.update({"state_error": 1, "reason": "error"})
        else:
            backend = disabled_runtime.get("global_flow_backend", {})
            backend_fields = (
                "native",
                "market_count",
                "trade_batches",
                "trade_events_seen",
                "trade_events_accepted",
                "book_events_seen",
                "book_events_accepted",
                "out_of_order_events",
                "stale_trade_events",
                "trade_overflow_events",
                "book_overflow_events",
            )
            if (
                not isinstance(backend, dict)
                or set(backend) != set(backend_fields)
                or any(type(backend.get(name)) is not int for name in backend_fields)
            ):
                flow.update({"state_error": 1, "reason": "identity_malformed"})
            else:
                for name in backend_fields:
                    flow[name] = backend[name]
            if (
                disabled_runtime.get("global_flow_shadow_enabled") is not False
                or disabled_runtime.get("global_flow_native_effective") is not False
            ):
                flow.update({"state_error": 1, "reason": "config_mismatch"})
    else:
        try:
            state = engine.signal.global_flow_state()
            backend = engine.signal.global_flow_backend_snapshot()
            flow_100 = state.window(100)
            flow_spot = flow_100.get("spot", {})
            flow_perp = flow_100.get("perp", {})
            flow.update(
                {
                    "valid": int(flow_100.get("valid", 0)),
                    "pressure": float(flow_100.get("global_flow_pressure", float("nan"))),
                    "pending": float(flow_100.get("global_minus_bridge_bps", float("nan"))),
                    "spot_pressure": float(flow_spot.get("flow_pressure", float("nan"))),
                    "perp_pressure": float(flow_perp.get("flow_pressure", float("nan"))),
                    "spot_agreement": float(flow_spot.get("venue_agreement", 0.0)),
                    "perp_agreement": float(flow_perp.get("venue_agreement", 0.0)),
                    "fresh_spot": int(flow_spot.get("fresh_venues", 0)),
                    "fresh_perp": int(flow_perp.get("fresh_venues", 0)),
                    "reason": "evaluated",
                }
            )
            for name in (
                "native",
                "market_count",
                "trade_batches",
                "trade_events_seen",
                "trade_events_accepted",
                "book_events_seen",
                "book_events_accepted",
                "out_of_order_events",
                "stale_trade_events",
                "trade_overflow_events",
                "book_overflow_events",
            ):
                flow[name] = int(backend.get(name, 0))
        except Exception as exc:
            logger.warning("Global flow shadow state failed: %s", exc)
            flow.update({"state_error": 1, "reason": "error"})
    return ref, flow


def record_startup_runtime_identity(
    *,
    cfg,
    config_path: Path,
    native_runtime: dict,
    dry_run: bool,
    engine: MakerEngine | None = None,
) -> tuple[Path, dict]:
    """Persist and return the identity that actually governs this process."""
    resolved_config = config_path.expanduser().resolve()
    q90_policy = q90_action_runtime_policy(bool(cfg.strategy.dynamic_fill_hazard_action_enabled))
    q90_policy_fields = {key: value for key, value in q90_policy.items() if key != "schema_version"}
    f05_policy = f05_boolean_cooldown_runtime_policy(
        bool(cfg.strategy.boolean_cooldown_policy_enabled),
        evidence_route=cfg.strategy.boolean_cooldown_evidence_route,
    )
    f05_policy_fields = {key: value for key, value in f05_policy.items() if key != "schema_version"}
    f05_buy_e3_policy = f05_buy_e3_runtime_policy(
        bool(cfg.strategy.buy_e3_cooldown_policy_enabled),
        evidence_route=cfg.strategy.buy_e3_cooldown_evidence_route,
    )
    f05_buy_e3_policy_fields = {
        key: value for key, value in f05_buy_e3_policy.items() if key != "schema_version"
    }
    f05_buy_e3_active_release = f05_buy_e3_active_release_runtime_authority(
        bool(cfg.strategy.buy_e3_cooldown_policy_enabled),
        require_present=engine is not None,
    )
    f05_buy_e3_active_release_fields = {
        key: value
        for key, value in f05_buy_e3_active_release.items()
        if key != "schema_version"
    }
    model_dir = Path(str(cfg.ml.model_dir)).expanduser()
    if not model_dir.is_absolute():
        model_dir = ROOT / model_dir
    log_file = Path(str(cfg.logging.file)).expanduser()
    identity_path = (
        log_file.parent / "runtime_identity.json"
        if str(cfg.logging.file).strip()
        else ROOT / "logs" / "runtime_identity.json"
    )
    identity = {
        "schema_version": "narrowgate_live_runtime_identity.v1",
        "recorded_at_utc": datetime.now(UTC).isoformat(),
        "pid": os.getpid(),
        "python_executable": sys.executable,
        "python_version": ".".join(map(str, sys.version_info[:3])),
        "config_path": str(resolved_config),
        "config_sha256": hashlib.sha256(resolved_config.read_bytes()).hexdigest(),
        "dry_run": bool(dry_run),
        "testnet": bool(cfg.api.testnet),
        "ml_enabled": bool(cfg.ml.enabled),
        "model_dir": str(model_dir.resolve()),
        "global_flow_shadow_enabled": bool(
            cfg.multi_market.global_flow_shadow_enabled
        ),
        "global_flow_shadow_config_explicit": bool(
            getattr(
                cfg.multi_market,
                "_global_flow_shadow_enabled_explicit",
                False,
            )
        ),
        "global_reference_shadow_enabled": bool(
            cfg.multi_market.global_reference_shadow_enabled
        ),
        "global_reference_shadow_config_explicit": bool(
            getattr(
                cfg.multi_market,
                "_global_reference_shadow_enabled_explicit",
                False,
            )
        ),
        "buy_fill_selection_live_enabled": bool(cfg.strategy.buy_fill_selection_live_enabled),
        "buy_fill_selection_shadow_enabled": bool(cfg.strategy.buy_fill_selection_shadow_enabled),
        "dynamic_fill_hazard_shadow_enabled": bool(cfg.strategy.dynamic_fill_hazard_shadow_enabled),
        "order_lifecycle_journal_v2_enabled": bool(cfg.lifecycle_journal_v2.enabled),
        "order_lifecycle_journal_v2_storage_profile": str(cfg.lifecycle_journal_v2.storage_profile),
        "native_runtime": native_runtime,
        "q90_runtime_policy_schema_version": q90_policy["schema_version"],
        **q90_policy_fields,
        "f05_boolean_cooldown_runtime_policy_schema_version": f05_policy["schema_version"],
        **f05_policy_fields,
        "f05_boolean_cooldown_policy_sha256": str(cfg.strategy.boolean_cooldown_policy_sha256)
        .strip()
        .lower(),
        "f05_boolean_cooldown_predicate_bundle_sha256": str(
            cfg.strategy.boolean_cooldown_predicate_bundle_sha256
        )
        .strip()
        .lower(),
        "f05_boolean_cooldown_ema_warmup_s": float(cfg.strategy.boolean_cooldown_ema_warmup_s),
        "f05_buy_e3_runtime_policy_schema_version": f05_buy_e3_policy["schema_version"],
        **f05_buy_e3_policy_fields,
        "f05_buy_e3_active_release_authority_schema_version": (
            f05_buy_e3_active_release["schema_version"]
        ),
        **{
            f"f05_buy_e3_{key}": value
            for key, value in f05_buy_e3_active_release_fields.items()
        },
        "f05_buy_e3_artifact_manifest_sha256": str(
            cfg.strategy.buy_e3_cooldown_artifact_manifest_sha256
        )
        .strip()
        .lower(),
        "f05_buy_e3_artifact_sha256": str(cfg.strategy.buy_e3_cooldown_artifact_sha256)
        .strip()
        .lower(),
        "f05_buy_e3_policy_sha256": str(cfg.strategy.buy_e3_cooldown_policy_sha256).strip().lower(),
        "f05_buy_e3_predicate_bundle_sha256": str(
            cfg.strategy.buy_e3_cooldown_predicate_bundle_sha256
        )
        .strip()
        .lower(),
        "f05_buy_e3_ema_warmup_s": float(cfg.strategy.buy_e3_cooldown_ema_warmup_s),
    }
    if engine is not None:
        attestation = build_startup_attestation(
            engine=engine,
            native_runtime=native_runtime,
        )
        identity["startup_attestation"] = attestation
    write_runtime_identity(identity_path, identity)
    if engine is not None and identity["startup_attestation"]["status"] != "accepted":
        raise RuntimeError(
            "startup attestation rejected: " + ", ".join(identity["startup_attestation"]["errors"])
        )
    return identity_path, identity


def _initial_exchange_open_orders(rest, *, symbol: str) -> list[dict]:
    get_orders = getattr(rest, "get_orders", None)
    if not callable(get_orders):
        raise RuntimeError("enabled lifecycle_journal_v2 requires a get_orders startup audit")
    rows = get_orders(symbol=symbol)
    normalized = []
    for row in rows or []:
        if not isinstance(row, dict):
            raise ValueError("exchange open-order startup audit returned a non-object")
        normalized.append(
            {
                "symbol": str(row.get("symbol", symbol)),
                "client_order_id": str(row.get("clientOrderId", "")),
                "exchange_order_id": str(row.get("orderId", "")),
                "side": str(row.get("side", "")),
                "status": str(row.get("status", "")),
                "price": str(row.get("price", "")),
                "original_quantity": str(row.get("origQty", "")),
                "executed_quantity": str(row.get("executedQty", "")),
            }
        )
    return normalized


def initialize_prospective_lifecycle_collection(
    *,
    cfg,
    engine: MakerEngine,
    rest,
    config_path: Path,
    native_runtime: dict,
) -> tuple[object | None, OrderLifecycleLiveWriterV2 | None]:
    """Create one fully bound epoch and async writer before the main loop."""

    settings = cfg.lifecycle_journal_v2
    if not bool(settings.enabled):
        return None, None
    exchange_open_orders = _initial_exchange_open_orders(rest, symbol=cfg.symbol)
    if exchange_open_orders:
        raise RuntimeError(
            "prospective epoch requires zero exchange-open orders after startup cancel"
        )
    account = rest.account()
    initial_state = engine.prospective_epoch_initial_runtime_state(
        account_snapshot={
            "wallet_balance": str(account.get("totalWalletBalance", "")),
            "available_balance": str(account.get("availableBalance", "")),
        },
        exchange_open_orders=exchange_open_orders,
    )
    if initial_state["order_lifecycle"]["active_local_orders"]:
        raise RuntimeError("prospective epoch requires zero active local orders before collection")
    model_dir = Path(str(cfg.ml.model_dir)).expanduser()
    if not model_dir.is_absolute():
        model_dir = ROOT / model_dir
    model_dir = model_dir.resolve(strict=True)
    epoch = publish_prospective_baseline_epoch(
        output_root=settings.prospective_epoch_root,
        required_mount=settings.required_mount,
        repo_root=ROOT,
        config_path=config_path,
        baseline_identity_path=settings.baseline_identity_path,
        expected_baseline_identity_sha256=settings.baseline_identity_sha256,
        model_dir=model_dir,
        p3_path=model_dir / "fill_prob_params.json",
        feature_dag_sha256=TEN_SECOND_CAUSAL_GRAPH.sha256(),
        runtime_code_paths=prospective_epoch_runtime_code_paths(ROOT),
        native_runtime=native_runtime,
        native_module_path=native_runtime.get("module"),
        action_enablement=snapshot_action_enablement(cfg),
        initial_runtime_state=initial_state,
        data_source_identity=snapshot_data_source_identity(cfg),
        clock_semantics=live_clock_semantics_identity(),
        start_ts_ns=time.time_ns(),
        require_mounted=True,
        storage_profile=settings.storage_profile,
        remote_spool_allowlisted_roots=settings.remote_spool_allowlisted_roots,
        collection_bounds={
            "max_duration_s": settings.remote_session_max_duration_s,
            "max_bytes": settings.remote_session_max_bytes,
        },
    )
    writer = None
    try:
        writer = OrderLifecycleLiveWriterV2(
            settings.root,
            session_id=epoch.epoch_id,
            baseline_epoch_id=epoch.epoch_id,
            runtime_identity=epoch.writer_runtime_identity(),
            queue_size=settings.queue_size,
            storage_format=settings.storage_format,
            heartbeat_interval_s=settings.heartbeat_interval_s,
            initial_active_order_ids=(),
            storage_profile=settings.storage_profile,
            epoch_root=epoch.epoch_root,
            session_max_duration_s=settings.remote_session_max_duration_s,
            session_max_bytes=settings.remote_session_max_bytes,
        )
        engine.set_order_lifecycle_live_writer_v2(
            writer,
            shutdown_drain_timeout_s=settings.shutdown_drain_timeout_s,
        )
    except Exception:
        if writer is not None:
            writer.close(drain_timeout_s=0.0)
        raise
    return epoch, writer


def start_engine_with_prospective_collection(
    *,
    cfg,
    engine: MakerEngine,
    ws: WSHandler,
    rest,
    config_path: Path,
    native_runtime: dict,
    dry_run: bool,
) -> tuple[object | None, OrderLifecycleLiveWriterV2 | None]:
    """Start warmup, bind collection, then permit the first WS event."""

    engine.start()
    startup_open_orders = _initial_exchange_open_orders(rest, symbol=cfg.symbol)
    if startup_open_orders:
        raise RuntimeError("startup open-order ownership did not converge after cancel")
    # The pre-start position sync can become stale if a predecessor order fills
    # while startup cancellation is converging.  Once zero open orders is
    # authoritative, require one more account-position snapshot before the
    # epoch or user stream can become visible.
    engine.sync_position(required=True)
    epoch, writer = initialize_prospective_lifecycle_collection(
        cfg=cfg,
        engine=engine,
        rest=rest,
        config_path=config_path,
        native_runtime=native_runtime,
    )
    if not dry_run:
        ws.start(rest)
    else:
        logging.getLogger("main").info("[DRY-RUN] Skipping WebSocket connections")
    return epoch, writer


def create_rest_client(cfg, dry_run=False):
    """Create Binance Futures REST client."""
    if dry_run:
        raise ValueError(
            "legacy simulated REST dry-run was removed; use live/main.py --dry-run"
        )

    from binance.um_futures import UMFutures

    base_url = (
        "https://testnet.binancefuture.com" if cfg.api.testnet else "https://fapi.binance.com"
    )

    client = UMFutures(
        key=cfg.api.key,
        secret=cfg.api.secret,
        base_url=base_url,
    )
    return client


def resolve_logging_paths(cfg):
    """Resolve relative logging paths against the project root."""
    for field in (
        "file",
        "fill_cooldown_checkpoint",
        "trade_log",
        "quote_log",
        "order_outcome_log",
        "buy_fill_selection_shadow_log",
        "dynamic_fill_hazard_shadow_log",
        "dynamic_fill_hazard_action_log",
        "state_conditioned_policy_shadow_log",
        "cross_venue_fair_price_shadow_log",
        "inventory_campaign_shadow_log",
        "live_perf_telemetry_log",
        "quote_snapshot_integrity_log",
    ):
        value = getattr(cfg.logging, field, None)
        if value and not Path(value).is_absolute():
            setattr(cfg.logging, field, str(ROOT / value))


def main():
    parser = argparse.ArgumentParser(description="NarrowGate Maker Engine")
    parser.add_argument("--live", action="store_true", help="Use mainnet (override testnet config)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config/model contract locally, then exit",
    )
    parser.add_argument(
        "--dry-run-timeout-s",
        type=_positive_finite_seconds,
        default=DEFAULT_DRY_RUN_TIMEOUT_S,
        help=(
            "Formal dry-run deadline in seconds "
            f"(default: {DEFAULT_DRY_RUN_TIMEOUT_S:g})"
        ),
    )
    parser.add_argument("--config", type=str, default=None, help="Path to config.yaml")
    args = parser.parse_args()

    if args.dry_run and args.live:
        parser.error("--dry-run and --live are mutually exclusive")

    config_path = Path(args.config) if args.config else ROOT / "live" / "config.yaml"
    if args.dry_run:
        return run_formal_dry_run(
            config_path,
            timeout_s=args.dry_run_timeout_s,
        )

    # Load config only after the formal dry-run branch has exited.
    cfg = load_config(config_path)
    resolved_config_path = config_path.expanduser().resolve()

    if args.live:
        cfg.api.testnet = False

    resolve_logging_paths(cfg)
    # Setup logging
    setup_logging(cfg)
    logger = logging.getLogger("main")

    logger.info("=" * 60)
    native_runtime = audit_native_runtime(logger, cfg=cfg)
    project_name = getattr(cfg, "project_name", "NarrowGate")
    logger.info(f"{project_name} Maker Engine Starting")
    logger.info(f"  Symbol:    {cfg.symbol}")
    logger.info(f"  Testnet:   {cfg.api.testnet}")
    logger.info("  Mode:      live")
    logger.info(f"  ML:        {cfg.ml.enabled}")
    logger.info(
        f"  γ={cfg.strategy.gamma} fallback_κ={cfg.strategy.kappa} (P3 κ_eff used when available)"
    )
    logger.info(f"  Order size: {cfg.strategy.order_size} BTC")
    logger.info(f"  Max inv:   {cfg.strategy.max_inventory} BTC")
    logger.info(f"  Requote:   {cfg.strategy.requote_interval}s")
    logger.info("=" * 60)

    # Validate API keys
    if not cfg.api.key or not cfg.api.secret:
        logger.error(
            "API key/secret not set. Use env vars BINANCE_API_KEY / "
            "BINANCE_API_SECRET, or set in config.yaml. On the live host, "
            "start with ./live/run.sh start|restart so live/.env is sourced."
        )
        sys.exit(1)

    # Create REST client
    rest = create_rest_client(cfg)

    # Create engine
    engine = MakerEngine(cfg, rest)
    restored_fill_cooldown = engine.restore_fill_cooldown_checkpoint()
    logger.info(
        "FILL_COOLDOWN_RESTORE mode=%s checkpoint_loaded=%d sequence=%d "
        "buy_identity=%s buy_remaining_ms=%d",
        restored_fill_cooldown["restore_mode"],
        int(restored_fill_cooldown["checkpoint_loaded"]),
        int(restored_fill_cooldown["checkpoint_sequence"]),
        restored_fill_cooldown["buy_deadline_identity"],
        int(restored_fill_cooldown["buy_remaining_ms"]),
    )

    # Bind the clean checkout, exact artifact, and restored deadline before
    # WebSockets, exchange synchronization, or live loops can begin.
    runtime_identity_path, runtime_identity = record_startup_runtime_identity(
        cfg=cfg,
        config_path=resolved_config_path,
        native_runtime=native_runtime,
        dry_run=False,
        engine=engine,
    )
    logger.info(
        "RUNTIME_IDENTITY path=%s identity=%s",
        runtime_identity_path,
        json.dumps(runtime_identity, sort_keys=True),
    )
    if runtime_identity["q90_owner_override_effective"]:
        logger.warning(
            "OWNER_RISK_ACCEPTED_OVERRIDE q90_action=ON authority=%s",
            runtime_identity["q90_action_runtime_authority"],
        )
    if runtime_identity["f05_boolean_cooldown_owner_override_effective"]:
        logger.warning(
            "OWNER_RISK_ACCEPTED_OVERRIDE f05_boolean_cooldown=ON authority=%s",
            runtime_identity["f05_boolean_cooldown_runtime_authority"],
        )
    if runtime_identity["f05_buy_e3_owner_override_effective"]:
        logger.warning(
            "OWNER_RISK_ACCEPTED_OVERRIDE f05_buy_e3=ON authority=%s artifact=%s",
            runtime_identity["f05_buy_e3_runtime_authority"],
            runtime_identity["f05_buy_e3_artifact_sha256"],
        )

    # Create WebSocket handler
    ws = WSHandler(engine, cfg)
    engine.set_ws_handler(ws)

    # Graceful shutdown
    shutdown_event = False

    def handle_shutdown(signum, frame):
        nonlocal shutdown_event
        if shutdown_event:
            logger.warning("Force exit")
            sys.exit(1)
        shutdown_event = True
        logger.info(f"Received signal {signum}, shutting down...")

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Install config hot-reload (SIGHUP)
    set_engine_ref(engine)
    install_reload_handler()

    try:
        # Sync position
        logger.info("Syncing exchange position...")
        engine.sync_position()

        # Check account
        if not args.dry_run:
            try:
                acct = rest.account()
                balance = float(acct.get("totalWalletBalance", 0))
                available = float(acct.get("availableBalance", 0))
                logger.info(
                    f"Account: balance={balance:.2f} USD-equivalent, "
                    f"available={available:.2f} USD-equivalent"
                )
            except Exception as e:
                logger.warning(f"Account check failed: {e}")

        # Preflight checks (mainnet safety)
        if not args.dry_run:
            try:
                # Check position mode (must be One-way for reduceOnly logic)
                pos_mode = rest.get_position_mode()
                dual = pos_mode.get("dualSidePosition", False)
                if dual:
                    logger.error(
                        "PREFLIGHT FAILED: Account is in Hedge Mode "
                        "(dualSidePosition=true). This engine requires "
                        "One-way Mode. Change via Binance app/web → "
                        "Preferences → Position Mode → One-way."
                    )
                    sys.exit(1)
                logger.info("Position mode: One-way (OK)")

                # Check margin type
                # Try to set CROSSED margin — if already set, this is a no-op
                try:
                    rest.change_margin_type(symbol=cfg.symbol, marginType="CROSSED")
                    logger.info("Margin type: set to CROSSED")
                except Exception as e:
                    msg = str(e)
                    if "No need to change" in msg or "-4046" in msg:
                        logger.info("Margin type: CROSSED (already set)")
                    else:
                        logger.warning(f"Margin type check: {e}")

                # The engine constructor already loads the bundle strictly.
                # Repeat the lightweight contract here so the preflight log
                # records the exact head count and P3 identity.
                from pathlib import Path as _P
                from strategy.model_contract import validate_model_bundle

                model_dir = _P(getattr(cfg.ml, "model_dir", "models/saved"))
                if not model_dir.is_absolute():
                    model_dir = _P(__file__).resolve().parent.parent / model_dir
                required_models = ["fill_prob_params.json"]
                # The configured bundle must remain restart-safe even while
                # inference is disabled.  Validation reads metadata only; it
                # does not load LightGBM trees into the live process.
                model_metadata = validate_model_bundle(model_dir)
                logger.info(
                    "Models: %d strict LightGBM heads validated in %s (active=%s)",
                    len(model_metadata),
                    model_dir,
                    cfg.ml.enabled,
                )
                for mf in required_models:
                    if not (model_dir / mf).exists():
                        raise RuntimeError(f"PREFLIGHT: Missing {mf} in {model_dir}")

            except SystemExit:
                raise
            except Exception as e:
                logger.error(f"PREFLIGHT FAILED: {e}")
                raise

        # Warm up and cancel startup orders before publishing the epoch.  The
        # writer is attached before any WebSocket can deliver a live event.
        prospective_epoch, _ = start_engine_with_prospective_collection(
            cfg=cfg,
            engine=engine,
            ws=ws,
            rest=rest,
            config_path=resolved_config_path,
            native_runtime=native_runtime,
            dry_run=bool(args.dry_run),
        )
        if prospective_epoch is not None:
            logger.info(
                "PROSPECTIVE_BASELINE_EPOCH_BOUND id=%s identity=%s manifest=%s",
                prospective_epoch.epoch_id,
                prospective_epoch.identity_sha256,
                prospective_epoch.manifest_path,
            )

        # Main loop
        logger.info("Entering main loop...")
        sync_interval = 60  # sync position every 60s
        last_sync = time.time()
        health_interval = 60  # health check every 60s
        last_health = time.time()
        stale_interval = 30  # check stale orders every 30s
        last_stale = time.time()

        while not shutdown_event:
            now = time.time()
            now_ns = time.time_ns()

            # Refresh one generation-consistent q90 view before policy code.
            ws.maintain_deep_book(now_ns=now_ns)
            ws.maintain_active_order_depth_paths(now_ns=now_ns)

            # Engine tick (handles requote interval internally)
            if engine.is_running:
                engine.tick()

            # Periodic position sync
            if now - last_sync >= sync_interval:
                # REST sync 是 user stream 的审计兜底；发现差异后的硬降级由 MakerEngine 判定，
                # 主循环只负责固定节奏触发，不在这里直接停策略。
                engine.sync_position()
                last_sync = now

            # Stale submit reconciliation. An unknown REST response must never
            # be rewritten as a confirmed pre-activation rejection.
            if now - last_stale >= stale_interval:
                stale = engine.orders.get_stale_orders(max_age=30.0)
                for o in stale:
                    logger.warning(
                        f"STALE order {o.client_order_id} "
                        f"stuck in PENDING_NEW for "
                        f"{now - o.create_time:.0f}s, reconciling"
                    )
                    resolution = engine.reconcile_pending_new_order(o)
                    logger.warning(
                        "STALE_PENDING_NEW_RECONCILE cid=%s resolution=%s",
                        o.client_order_id,
                        resolution,
                    )
                stale_cancel = engine.orders.get_stale_pending_cancel_orders(max_age=30.0)
                for o in stale_cancel:
                    # Open-order absence is not a terminal ACK.  Query the
                    # individual order so cumulative fills and the exact
                    # terminal status are reconciled before ownership release.
                    resolution = engine.reconcile_pending_cancel_order(o)
                    logger.warning(
                        "STALE_PENDING_CANCEL_RECONCILE cid=%s resolution=%s",
                        o.client_order_id,
                        resolution,
                    )
                last_stale = now

            # Health check
            if now - last_health >= health_interval:
                snap = engine.inventory.snapshot
                inv_exp = engine.inventory.inventory_exposure_snapshot()
                camp = engine.inventory.campaign_snapshot()
                buy_fill_sel = engine.buy_fill_selection_live_snapshot()
                fill_hazard = engine.dynamic_fill_hazard_shadow_snapshot()
                boolean_cooldown = engine.boolean_cooldown_policy_snapshot()
                buy_e3_cooldown = engine.buy_e3_cooldown_policy_snapshot()
                market_tape = ws.market_tape_snapshot()
                deep_book = ws.deep_book_snapshot()
                active_order_depth = ws.active_order_depth_snapshot()
                external_sources = ws.external_venue_snapshot()
                external_enabled = len(external_sources)
                external_stale = sum(int(source.get("stale", 1)) for source in external_sources)
                external_trade_stale = sum(
                    int(source.get("trade_stale", 1)) for source in external_sources
                )
                external_errors = sum(
                    int(source.get("error_count", 0)) for source in external_sources
                )
                external_record_depth = sum(
                    int(source.get("record_queue_depth", 0)) for source in external_sources
                )
                external_record_hwm = max(
                    (
                        int(source.get("record_queue_high_watermark", 0))
                        for source in external_sources
                    ),
                    default=0,
                )
                external_record_max_age_ms = max(
                    (
                        float(source.get("record_max_queue_age_ms", 0.0))
                        for source in external_sources
                    ),
                    default=0.0,
                )
                external_record_dropped = sum(
                    int(source.get("record_dropped", 0)) for source in external_sources
                )
                external_source_ids = (
                    "|".join(str(source.get("market_id", "unknown")) for source in external_sources)
                    or "none"
                )
                request_rtts = [
                    float(source.get("request_rtt_ms", float("nan"))) for source in external_sources
                ]
                external_request_rtt_ms = max(
                    (value for value in request_rtts if math.isfinite(value)),
                    default=0.0,
                )
                external_book_age_ms = max(
                    (float(source.get("book_age_ms", float("inf"))) for source in external_sources),
                    default=float("inf"),
                )
                external_trade_age_ms = max(
                    (
                        float(source.get("trade_age_ms", float("inf")))
                        for source in external_sources
                    ),
                    default=float("inf"),
                )
                external_book_event_age_ms = max(
                    (
                        float(source.get("book_event_age_ms", float("inf")))
                        for source in external_sources
                    ),
                    default=float("inf"),
                )
                external_trade_event_age_ms = max(
                    (
                        float(source.get("trade_event_age_ms", float("inf")))
                        for source in external_sources
                    ),
                    default=float("inf"),
                )
                global_ref_values, global_flow_backend_values = (
                    collect_global_shadow_health(
                        engine=engine,
                        cfg=cfg,
                        logger=logger,
                    )
                )
                global_flow_values = global_flow_backend_values
                logger.info(
                    f"HEALTH pos={snap.qty:+.4f} "
                    f"rpnl={snap.realized_pnl:.2f} "
                    f"upnl={snap.unrealized_pnl:.2f} "
                    f"daily={engine.inventory.daily_pnl:.2f} "
                    f"absInvTime={inv_exp['abs_inventory_time_s']:.2f}btc_s "
                    f"avgAbsInv={inv_exp['time_avg_abs_inventory']:.6f} "
                    f"notionalInvTime={inv_exp['notional_inventory_time_s']:.2f}usd_s "
                    f"pnlPerInvHr={inv_exp['daily_pnl_per_abs_inventory_hour']:.4f} "
                    f"dayBuyAvgPx={inv_exp['daily_buy_avg_fill_price']:.1f} "
                    f"daySellAvgPx={inv_exp['daily_sell_avg_fill_price']:.1f} "
                    f"dayBuyQty={inv_exp['daily_buy_fill_qty']:.4f} "
                    f"daySellQty={inv_exp['daily_sell_fill_qty']:.4f} "
                    f"campActive={int(camp.active)} "
                    f"campAge={camp.age_s:.1f}s "
                    f"campMaxInv={camp.max_abs_qty:.4f} "
                    f"campPnl={camp.total_pnl:.2f} "
                    f"campMAE={camp.adverse_excursion:.2f} "
                    f"campIncFills={camp.exposure_increasing_fills} "
                    f"campRedFills={camp.reducing_fills} "
                    f"campBuyFills={camp.buy_fills} "
                    f"campSellFills={camp.sell_fills} "
                    f"buyFillSelEval={buy_fill_sel['eval_count']} "
                    f"buyFillSelHit={buy_fill_sel['hit_count']} "
                    f"buyFillSelHitRate={buy_fill_sel['hit_rate']:.4f} "
                    f"buyFillSelAction={buy_fill_sel['action_count']} "
                    f"buyFillSelActionRate={buy_fill_sel['action_rate']:.4f} "
                    f"buyFillSelLastHitAge={buy_fill_sel['last_hit_age_s']:.1f}s "
                    f"fillHazardEnabled={fill_hazard['enabled']} "
                    f"fillHazardRows={fill_hazard['rows']} "
                    f"fillHazardValid={fill_hazard['valid_rows']} "
                    f"fillHazardInvalid={fill_hazard['invalid_rows']} "
                    f"fillHazardValidRate={fill_hazard['valid_rate']:.4f} "
                    f"fillHazardLastAge={fill_hazard['last_age_s']:.1f}s "
                    f"fillHazardActionAuthorized={fill_hazard['action_authorized']} "
                    f"fillHazardActionThreshold={fill_hazard['action_threshold']:.8f} "
                    f"fillHazardActionLastScore={fill_hazard['action_last_score']:.8f} "
                    f"fillHazardActionHold={fill_hazard['action_hold']} "
                    f"fillHazardActionHoldAge={fill_hazard['action_hold_age_s']:.1f}s "
                    f"fillHazardActionCancels={fill_hazard['action_cancel_count']} "
                    f"fillHazardActionReentries={fill_hazard['action_reentry_count']} "
                    f"fillHazardActionKeeps={fill_hazard['action_keep_count']} "
                    f"fillHazardActionInvalidHold={fill_hazard['action_invalid_hold_count']} "
                    f"booleanCooldownEnabled={boolean_cooldown['enabled']} "
                    f"booleanCooldownUpdates={boolean_cooldown['windows']['updates']} "
                    f"booleanCooldownEval={boolean_cooldown['evaluations']} "
                    f"booleanCooldownSupported={boolean_cooldown['supported']} "
                    f"booleanCooldownNonbaseline={boolean_cooldown['nonbaseline']} "
                    f"booleanCooldownFallback={boolean_cooldown['fallback']} "
                    f"booleanCooldownLastAction={boolean_cooldown['last_action']} "
                    f"booleanCooldownWarm={boolean_cooldown['windows']['warmup_admitted']} "
                    f"booleanCooldownWindows={boolean_cooldown['windows']['completed_windows']} "
                    f"booleanCooldownGaps={boolean_cooldown['windows']['gap_windows']} "
                    f"booleanCooldownResets={boolean_cooldown['windows']['resets']} "
                    f"booleanCooldownInvalid={boolean_cooldown['windows']['invalid_updates']} "
                    f"buyE3CooldownEnabled={buy_e3_cooldown['enabled']} "
                    f"buyE3CooldownUpdates={buy_e3_cooldown['windows']['updates']} "
                    f"buyE3CooldownEval={buy_e3_cooldown['evaluations']} "
                    f"buyE3CooldownSupported={buy_e3_cooldown['supported']} "
                    f"buyE3CooldownNonbaseline={buy_e3_cooldown['nonbaseline']} "
                    f"buyE3CooldownFallback={buy_e3_cooldown['fallback']} "
                    f"buyE3CooldownLastAction={buy_e3_cooldown['last_action']} "
                    f"buyE3CooldownDecisionP99Us={buy_e3_cooldown['decision_latency_p99_us']:.1f} "
                    f"buyE3CooldownWarm={buy_e3_cooldown['windows']['warmup_time_admitted']} "
                    f"buyE3CooldownWindows={buy_e3_cooldown['windows']['completed_windows']} "
                    f"buyE3CooldownGapResets={buy_e3_cooldown['windows']['gap_resets']} "
                    f"buyE3CooldownResets={buy_e3_cooldown['windows']['resets']} "
                    f"buyE3CooldownInvalid={buy_e3_cooldown['windows']['invalid_updates']} "
                    f"marketTapeEnabled={market_tape['enabled']} "
                    f"marketTapeWritten={market_tape['written']} "
                    f"marketTapeDropped={market_tape['dropped']} "
                    f"marketTapeInvalid={market_tape['invalid']} "
                    f"marketTapeQueueDepth={market_tape['queue_depth']} "
                    f"marketTapeQueueHwm={market_tape['queue_high_watermark']} "
                    f"marketTapeQueueAgeMs={market_tape['queue_age_ms']:.1f} "
                    f"marketTapeMaxQueueAgeMs={market_tape['max_queue_age_ms']:.1f} "
                    f"deepBookEnabled={deep_book['enabled']} "
                    f"deepBookValid={deep_book['valid']} "
                    f"deepBookStale={deep_book['stale']} "
                    f"deepBookAgeMs={deep_book['age_ms']:.1f} "
                    f"deepBookGeneration={deep_book['generation']} "
                    f"deepBookLastUpdate={deep_book['last_update_id']} "
                    f"deepBookBidLevels={deep_book['bid_levels']} "
                    f"deepBookAskLevels={deep_book['ask_levels']} "
                    f"deepBookGaps={deep_book['gap_count']} "
                    f"deepBookResyncs={deep_book['resync_count']} "
                    f"deepBookStaleRestarts={deep_book['stale_restart_count']} "
                    f"deepBookBuffer={deep_book['buffer_events']} "
                    f"deepBookTrades={deep_book['trade_count']} "
                    f"activeDeepTracked={active_order_depth['tracked']} "
                    f"activeDeepRetained={active_order_depth['retained']} "
                    f"activeDeepValid={active_order_depth['valid']} "
                    f"activeDeepAmbiguous={active_order_depth['ambiguous']} "
                    f"activeDeepUncovered={active_order_depth['uncovered']} "
                    f"activeDeepMaxAgeMs={active_order_depth['max_age_ms']:.1f} "
                    f"externalSources={external_enabled} "
                    f"externalSourceIds={external_source_ids} "
                    f"externalStale={external_stale} "
                    f"externalTradeStale={external_trade_stale} "
                    f"externalErrors={external_errors} "
                    f"externalRecordDepth={external_record_depth} "
                    f"externalRecordHwm={external_record_hwm} "
                    f"externalRecordMaxAgeMs={external_record_max_age_ms:.1f} "
                    f"externalRecordDropped={external_record_dropped} "
                    f"externalRequestRttMs={external_request_rtt_ms:.1f} "
                    f"externalBookAgeMs={external_book_age_ms:.1f} "
                    f"externalTradeAgeMs={external_trade_age_ms:.1f} "
                    f"externalBookEventAgeMs={external_book_event_age_ms:.1f} "
                    f"externalTradeEventAgeMs={external_trade_event_age_ms:.1f} "
                    f"globalRefShadowEnabled={global_ref_values['enabled']} "
                    f"globalRefStateError={global_ref_values['state_error']} "
                    f"globalRefValid={global_ref_values['valid']} "
                    f"globalRefConfidence={global_ref_values['confidence']:.4f} "
                    f"globalSpotMoveBps={global_ref_values['spot']:.4f} "
                    f"globalPerpMoveBps={global_ref_values['perp']:.4f} "
                    f"globalPerpSpotDivBps={global_ref_values['divergence']:.4f} "
                    f"globalResidualBps={global_ref_values['residual']:.4f} "
                    f"globalRefFreshSpot={global_ref_values['fresh_spot']} "
                    f"globalRefFreshPerp={global_ref_values['fresh_perp']} "
                    f"globalRefDispersionBps={global_ref_values['dispersion']:.4f} "
                    f"globalRefBasisSamples={global_ref_values['basis_samples']} "
                    f"globalFlowShadowEnabled={global_flow_values['enabled']} "
                    f"globalFlowStateError={global_flow_values['state_error']} "
                    f"globalFlow100Valid={global_flow_values['valid']} "
                    f"globalFlow100Pressure={global_flow_values['pressure']:.4f} "
                    f"globalFlow100PendingBps={global_flow_values['pending']:.4f} "
                    f"globalFlow100SpotPressure={global_flow_values['spot_pressure']:.4f} "
                    f"globalFlow100PerpPressure={global_flow_values['perp_pressure']:.4f} "
                    f"globalFlow100SpotAgreement={global_flow_values['spot_agreement']:.4f} "
                    f"globalFlow100PerpAgreement={global_flow_values['perp_agreement']:.4f} "
                    f"globalFlow100FreshSpot={global_flow_values['fresh_spot']} "
                    f"globalFlow100FreshPerp={global_flow_values['fresh_perp']} "
                    f"globalFlowNative={global_flow_backend_values['native']} "
                    f"globalFlowMarkets={global_flow_backend_values['market_count']} "
                    f"globalFlowTradeBatches={global_flow_backend_values['trade_batches']} "
                    f"globalFlowTradeEvents={global_flow_backend_values['trade_events_seen']} "
                    f"globalFlowTradeAccepted={global_flow_backend_values['trade_events_accepted']} "
                    f"globalFlowBookEvents={global_flow_backend_values['book_events_seen']} "
                    f"globalFlowBookAccepted={global_flow_backend_values['book_events_accepted']} "
                    f"globalFlowOOO={global_flow_backend_values['out_of_order_events']} "
                    f"globalFlowStaleTrades={global_flow_backend_values['stale_trade_events']} "
                    f"globalFlowTradeOverflow={global_flow_backend_values['trade_overflow_events']} "
                    f"globalFlowBookOverflow={global_flow_backend_values['book_overflow_events']} "
                    f"globalRefReason={global_ref_values['reason']} "
                    f"globalFlowReason={global_flow_values['reason']} "
                    f"state={snap.state.name} "
                    f"orders={engine.orders.active_count()} "
                    f"requotes={engine._requote_count}"
                )
                lifecycle_v2_health = engine.order_lifecycle_live_writer_v2_health_snapshot()
                if lifecycle_v2_health.get("enabled"):
                    logger.info(
                        "ORDER_LIFECYCLE_JOURNAL_V2_HEALTH profile=%s "
                        "remoteSpoolValid=%d formalValid=%d "
                        "queue=%d hwm=%d drops=%d errors=%d enqueueP99Us=%.1f "
                        "writeP99Ms=%.3f maxRssMb=%.1f lastFlushNs=%d",
                        str(lifecycle_v2_health.get("storage_profile", "")),
                        int(
                            bool(
                                lifecycle_v2_health.get(
                                    "remote_spool_valid",
                                    False,
                                )
                            )
                        ),
                        int(
                            bool(
                                lifecycle_v2_health.get(
                                    "formal_collection_valid",
                                    False,
                                )
                            )
                        ),
                        int(lifecycle_v2_health.get("queue_depth", 0)),
                        int(lifecycle_v2_health.get("queue_hwm", 0)),
                        int(lifecycle_v2_health.get("drop_count", 0)),
                        int(lifecycle_v2_health.get("error_count", 0)),
                        float(
                            lifecycle_v2_health.get(
                                "enqueue_latency_p99_us",
                                0.0,
                            )
                        ),
                        float(
                            lifecycle_v2_health.get(
                                "write_latency_p99_ms",
                                0.0,
                            )
                        ),
                        float(lifecycle_v2_health.get("process_max_rss_mb", 0.0)),
                        int(
                            lifecycle_v2_health.get(
                                "last_worker_flush_ts_ns",
                                0,
                            )
                        ),
                    )
                last_health = now

            # Sleep to avoid busy loop (short sleep, events arrive via WS callbacks)
            time.sleep(0.1)

    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
    finally:
        logger.info("Shutting down...")
        ws.stop()
        engine.stop()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    raise SystemExit(main())
