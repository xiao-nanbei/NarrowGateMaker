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

  # 进程完全停止后，生成 signed REST 交易所对账权威文件
  python live/main.py --write-stopped-reconciliation /absolute/path.json --config /private/config.yaml
"""

import argparse
import atexit
import copy
import hashlib
import importlib
import json
import logging
import logging.handlers
import math
import os
import queue
import signal
import subprocess
import sys
import sysconfig
import threading
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

# Add project root to path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from execution.order_lifecycle_live_writer_v2 import OrderLifecycleLiveWriterV2
from execution.runtime_evidence_writer import RuntimeEvidenceWriter
from features.feature_dag import TEN_SECOND_CAUSAL_GRAPH
from live import deployment_runtime as locked_runtime
from live.binance_usdm_transport import (
    BinanceUsdMOrderGateway,
    BinanceUsdMRestClients,
    BinanceUsdMWebSocketOrderConfig,
    BinanceUsdMWebSocketOrderGateway,
    binance_usdm_rest_base_url,
    create_binance_usdm_rest_clients,
    create_binance_usdm_websocket_order_gateway,
)
from live.config import (
    install_reload_handler,
    load_config,
    revalidate_loaded_config_source,
    set_engine_ref,
    set_restart_only_config_sha256,
)
from live.runtime_gc import GcPauseMonitor
from live.runtime_policy import (
    admit_runtime_policies,
    deployment_envelope_runtime_authority,
    write_runtime_identity,
)
from live.ws_handler import WSHandler
from models.replay.prospective_baseline_epoch import (
    live_clock_semantics_identity,
    publish_prospective_baseline_epoch,
    snapshot_action_enablement,
    snapshot_data_source_identity,
)
from strategy.maker_engine import (
    MakerEngine,
    validate_live_artifact_authority,
    validate_native_live_routing_policy_compatibility,
)
from strategy.native_runtime import (  # noqa: E402
    load_native_module,
    validate_native_capabilities,
    validate_replace_continuation,
)
from strategy.quote_core import QUOTE_CORE_CPP_ABI_FIELDS

POSITION_RISK_RECONCILIATION_ENDPOINT = "/fapi/v2/positionRisk"

CPP_RUNTIME_FLAGS = (
    "NARROWGATE_CPP_QUOTE_CORE",
    "NARROWGATE_CPP_QUOTE_POLICY_STAGE",
    "NARROWGATE_CPP_SIGNAL_FEATURES",
    "NARROWGATE_CPP_LIGHTGBM_INFERENCE",
    "NARROWGATE_CPP_GLOBAL_FLOW",
    "NARROWGATE_CPP_LIVE_ROUTING",
    "NARROWGATE_CPP_REPLACE_CONTINUATION",
    "NARROWGATE_CPP_COOLDOWN",
    "NARROWGATE_CPP_ORDER_ACTION_PLAN",
    "NARROWGATE_CPP_FINAL_ORDER_PLAN",
    "NARROWGATE_CPP_STRICT",
)
OPTIONAL_CPP_RUNTIME_FLAGS = frozenset(
    {
        "NARROWGATE_CPP_QUOTE_POLICY_STAGE",
        "NARROWGATE_CPP_FINAL_ORDER_PLAN",
        "NARROWGATE_CPP_LIGHTGBM_INFERENCE",
    }
)
CPP_MODULE_TOKEN_ENV = "NARROWGATE_CPP_EXPECT_MODULE_TOKEN"
NATIVE_COOLDOWN_REQUIRED_APIS = frozenset(
    {
        "F05BooleanClause",
        "F05BooleanLiteral",
        "F05BooleanPolicy",
        "F05BooleanRule",
        "F05PredicateDefinition",
        "F05PredicateMetric",
        "F05PredicatePair",
        "LiveCooldownDecisionStatus",
        "LiveCooldownProfile",
        "NATIVE_LIVE_COOLDOWN_HOT_PATH_AVAILABLE",
        "NativeLiveCooldownHotPath",
    }
)
NATIVE_ORDER_ACTION_REQUIRED_APIS = frozenset(
    {
        "compute_live_order_action_plan",
        "LiveOrderAction",
        "LivePlannerOrderState",
        "NATIVE_LIVE_ORDER_ACTION_PLAN_AVAILABLE",
        "LIVE_ORDER_SIDE_FLAG_ROUTE_ALLOWED",
        "LIVE_ORDER_SIDE_FLAG_ALLOW_POST",
        "LIVE_ORDER_SIDE_FLAG_ALLOW_EXPOSURE",
        "LIVE_ORDER_SIDE_FLAG_FORCE_UPDATE",
        "LIVE_ORDER_SIDE_FLAG_USE_PROVIDED_NEEDS_UPDATE",
        "LIVE_ORDER_SIDE_FLAG_PROVIDED_NEEDS_UPDATE",
        "LIVE_ORDER_REPLACE_FLAG_PENDING_COALESCE",
        "LIVE_ORDER_REPLACE_FLAG_CANCEL_FIRST_EXPOSURE",
        "LIVE_ORDER_REASON_THROTTLE_PRICE",
        "LIVE_ORDER_REASON_THROTTLE_AGE",
        "LIVE_ORDER_REASON_PENDING_LIFECYCLE",
        "LIVE_ORDER_REASON_CONFIGURED_CANCEL_FIRST",
    }
)
NATIVE_FINAL_ORDER_PLAN_REQUIRED_APIS = frozenset(
    {
        "compute_live_final_order_plan",
        "LiveFinalOrderPlanStatus",
        "NATIVE_LIVE_FINAL_ORDER_PLAN_AVAILABLE",
        "LIVE_FINAL_ORDER_PLAN_BOUNDARY_ABI",
        "LIVE_FINAL_ORDER_BOUNDARY_FLAG_P3_SIDE_BBO_FLOOR",
    }
)

FORMAL_DRY_RUN_SCHEMA = "narrowgate.live_dry_run.v1"
DEFAULT_DRY_RUN_TIMEOUT_S = 30.0
DRY_RUN_TIMEOUT_EXIT_CODE = 124
EXECUTION_STATE_UNCERTAIN_EXIT_CODE = 78
RUNTIME_HEALTH_SCHEMA = "narrowgate.live_runtime_health.v1"
LIVE_MAIN_LOOP_FALLBACK_WAIT_S = 0.1
STARTUP_USER_STREAM_READY_TIMEOUT_S = 30.0
ASYNC_LOG_QUEUE_CAPACITY = 4_096

PROSPECTIVE_EPOCH_RUNTIME_CODE_ROOTS = ("live", "strategy", "execution", "features")
PROSPECTIVE_EPOCH_RUNTIME_CODE_FILES = (
    "market_fusion.py",
    "models/replay/continuous_accounting.py",
    "models/replay/baseline_epoch_manifest.py",
    "models/replay/prospective_baseline_epoch.py",
)


class _ShutdownSignalFlag:
    """Minimal Python signal callback; intentionally performs no I/O or locking."""

    __slots__ = ("requested",)

    def __init__(self) -> None:
        self.requested = False

    def __call__(self, _signum: int, _frame: object) -> None:
        # A Python signal may interrupt this same thread while it owns any
        # application lock.  A plain reference assignment is the only action;
        # the main loop's bounded wait observes it within 100 ms.
        self.requested = True


class FormalDryRunTimeout(TimeoutError):
    """Raised when local validation exceeds its explicit deadline."""


class _LiveMainLoopWakeup:
    """Wake the sole decision loop without changing its 100 ms safety clock."""

    def __init__(self, fallback_wait_s: float = LIVE_MAIN_LOOP_FALLBACK_WAIT_S):
        self._event = threading.Event()
        self._fallback_wait_s = float(fallback_wait_s)

    def notify_replacement_terminal(self) -> None:
        self._event.set()

    def notify_shutdown(self) -> None:
        self._event.set()

    def wait(self) -> bool:
        notified = self._event.wait(timeout=self._fallback_wait_s)
        # Clear only after the wait. If a notifier races with this clear, its
        # authoritative ready/shutdown state is consumed by the immediately
        # following loop iteration; it cannot be delayed by another wait.
        self._event.clear()
        return notified


def arm_websocket_order_ab_runtime_guard(
    *,
    gateway,
    max_runtime_s: float,
    on_expire,
    timer_factory=threading.Timer,
):
    """Preconnect and arm the independent hard stop for a short WS A/B."""

    result = arm_order_gateway_experiment_runtime_guard(
        websocket_gateway=gateway,
        websocket_max_runtime_s=max_runtime_s,
        async_order_lanes_enabled=False,
        async_order_lane_max_runtime_s=0.0,
        on_expire=lambda _arms, _runtime_s: on_expire(),
        timer_factory=timer_factory,
    )
    assert result is not None
    return result[0]


def arm_order_gateway_experiment_runtime_guard(
    *,
    websocket_gateway,
    websocket_max_runtime_s: float,
    async_order_lanes_enabled: bool,
    async_order_lane_max_runtime_s: float,
    async_order_lane_deadline_monotonic: float | None = None,
    on_expire,
    timer_factory=threading.Timer,
    monotonic=time.monotonic,
):
    """Arm one active hard stop at the earliest enabled gateway deadline."""

    candidates: list[tuple[str, float]] = []
    if websocket_gateway is not None:
        websocket_runtime_s = float(websocket_max_runtime_s)
        if (
            not math.isfinite(websocket_runtime_s)
            or not 1.0 <= websocket_runtime_s <= 3_600.0
        ):
            raise ValueError(
                "WebSocket order A/B max runtime must be in [1, 3600]"
            )
        websocket_gateway.start()
        candidates.append(("websocket_order_ab", websocket_runtime_s))
    if bool(async_order_lanes_enabled):
        async_runtime_s = float(async_order_lane_max_runtime_s)
        if (
            not math.isfinite(async_runtime_s)
            or not 1.0 <= async_runtime_s <= 3_600.0
        ):
            raise ValueError(
                "async order-lane max runtime must be in [1, 3600]"
            )
        if async_order_lane_deadline_monotonic is not None:
            async_deadline = float(async_order_lane_deadline_monotonic)
            if not math.isfinite(async_deadline):
                raise ValueError("async order-lane deadline must be finite")
            # The transport rejects at this same absolute monotonic deadline.
            # Starting a second duration clock here would extend the bounded
            # experiment by however long startup and preflight took.
            async_runtime_s = max(0.0, async_deadline - float(monotonic()))
        candidates.append(("async_order_lanes", async_runtime_s))
    if not candidates:
        return None

    runtime_s = min(value for _name, value in candidates)
    limiting_arms = tuple(
        name for name, value in candidates if value == runtime_s
    )
    timer = timer_factory(
        runtime_s,
        lambda: on_expire(limiting_arms, runtime_s),
    )
    timer.daemon = True
    timer.start()
    return timer, runtime_s, limiting_arms


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
        if (
            bool(cfg.strategy.boolean_cooldown_policy_enabled)
            or bool(cfg.strategy.buy_e3_cooldown_policy_enabled)
        ):
            raise ValueError(
                "formal dry-run does not admit private live cooldown policies; "
                "use the deployment preflight and envelope verifier"
            )

        from strategy.model_contract import (
            REQUIRED_FEATURE_DAG_ID,
            REQUIRED_FEATURE_DAG_SHA256,
            REQUIRED_MODEL_HEADS,
            validate_model_bundle,
        )

        model_dir = _configured_model_dir(cfg)
        model_metadata = validate_model_bundle(
            model_dir,
            expected_symbol=cfg.symbol,
        )
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

STARTUP_ATTESTATION_SCHEMA = "narrowgate_startup_attestation.v1"
RUNNING_CHECKOUT_SCHEMA = "narrowgate_running_checkout_identity.v2"
INTERPRETER_IDENTITY_SCHEMA = "narrowgate_interpreter_identity.v1"
NATIVE_RUNTIME_IDENTITY_SCHEMA = "narrowgate_native_runtime_identity.v1"
NATIVE_ABI_CONTRACT_SCHEMA = "narrowgate_native_live_safety_abi.v1"
NATIVE_QUOTE_ABI_FIELDS = {
    "QuoteFlags": ("delta_cap", "final_compressed", "cap_exposure_block"),
    "SideQuoteContext": ("cap_exposure_block",),
}
STARTUP_ATTESTATION_GATE_NAMES = (
    "fill_cooldown_state_available",
    "fill_cooldown_state_schema_v2",
    "fill_cooldown_restore_mode_valid",
    "fill_cooldown_checkpoint_binding_valid",
    "fill_cooldown_deadline_contract_valid",
    "fill_cooldown_artifact_contract_valid",
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
DEPLOYMENT_ENVELOPE_STARTUP_ATTESTATION_GATE_NAMES = (
    *STARTUP_ATTESTATION_GATE_NAMES[:-1],
    "deployment_envelope_authority_valid",
    "repository_module_closure_available",
    "repository_module_closure_complete",
    "mandatory_safety_modules_loaded",
    "safe_to_start_live_loops",
)
KEY_LOADED_RUNTIME_MODULES = {
    "live_main": ("live.main", "live/main.py"),
    "live_config": ("live.config", "live/config.py"),
    "live_runtime_policy": ("live.runtime_policy", "live/runtime_policy.py"),
    "live_ws_handler": ("live.ws_handler", "live/ws_handler.py"),
    "maker_engine": ("strategy.maker_engine", "strategy/maker_engine.py"),
    "native_order_action": (
        "strategy.native_order_action",
        "strategy/native_order_action.py",
    ),
    "signal_engine": ("strategy.signal", "strategy/signal.py"),
    "global_flow": ("strategy.global_flow", "strategy/global_flow.py"),
    "global_reference": (
        "strategy.global_reference",
        "strategy/global_reference.py",
    ),
    "boolean_cooldown_live": (
        "strategy.boolean_cooldown_live",
        "strategy/boolean_cooldown_live.py",
    ),
    "boolean_cooldown_buy_e3": (
        "strategy.boolean_cooldown_buy_e3",
        "strategy/boolean_cooldown_buy_e3.py",
    ),
}
DEPLOYMENT_ENVELOPE_KEY_LOADED_RUNTIME_MODULES = {
    **KEY_LOADED_RUNTIME_MODULES,
    "inventory_manager": ("strategy.inventory_manager", "strategy/inventory_manager.py"),
    "order_manager": ("strategy.order_manager", "strategy/order_manager.py"),
    "quote_core": ("strategy.quote_core", "strategy/quote_core.py"),
    "replay_controls": ("strategy.replay_controls", "strategy/replay_controls.py"),
    "continuous_accounting": (
        "models.replay.continuous_accounting",
        "models/replay/continuous_accounting.py",
    ),
    "order_lifecycle_live_writer": (
        "execution.order_lifecycle_live_writer_v2",
        "execution/order_lifecycle_live_writer_v2.py",
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
        "status_entry_count": len(status.splitlines()),
        "worktree_clean": not status,
        "snapshot_internally_stable": first == second,
    }


def _repository_loaded_python_modules() -> tuple[tuple[str, str], ...]:
    modules: set[tuple[str, str]] = set()
    for module_name, module in tuple(sys.modules.items()):
        raw_origin = getattr(module, "__file__", None)
        if not raw_origin:
            continue
        try:
            origin = Path(str(raw_origin)).resolve(strict=True)
            relative = origin.relative_to(ROOT)
        except (OSError, ValueError):
            continue
        if origin.suffix in {".so", ".dylib", ".pyd"}:
            continue
        if origin.suffix == ".pyc" and "__pycache__" in relative.parts:
            source_name = origin.name.split(".", 1)[0] + ".py"
            origin = origin.parent.parent / source_name
            relative = origin.relative_to(ROOT)
        if origin.suffix == ".py" and origin.is_file():
            modules.add((str(module_name), relative.as_posix()))
    return tuple(sorted(modules))


def _runtime_source_rows(*, loaded_paths: Sequence[str] | None = None) -> list[dict]:
    rows = []
    if loaded_paths is None:
        paths = {value[1] for value in KEY_LOADED_RUNTIME_MODULES.values()}
    else:
        paths = set(prospective_epoch_runtime_code_paths(ROOT))
        paths.update({"live/run.sh", "live/profiles/native.env"})
        paths.update(loaded_paths)
    for relative in sorted(paths):
        path = ROOT / relative
        resolved = path.resolve(strict=True)
        tracked_type = _git_output("cat-file", "-t", f"HEAD:{relative}").decode(
            "ascii"
        ).strip()
        rows.append(
            {
                "path": relative,
                "tracked_in_head": tracked_type == "blob",
                "regular_file_under_repository": (
                    not path.is_symlink()
                    and resolved.is_file()
                    and resolved.is_relative_to(ROOT)
                ),
            }
        )
    return rows


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


def _loaded_module_origins(
    source_rows: list[dict],
    module_identities: Mapping[str, tuple[str, str]] | None = None,
) -> dict:
    source_by_path = {row["path"]: row for row in source_rows}
    output = {}
    identities = KEY_LOADED_RUNTIME_MODULES if module_identities is None else module_identities
    for role, (module_name, expected_relative) in identities.items():
        module = importlib.import_module(module_name)
        origin = Path(str(module.__file__)).resolve(strict=True)
        relative = origin.relative_to(ROOT).as_posix()
        if relative != expected_relative:
            raise RuntimeError(f"loaded module origin drifted: {role}")
        if relative not in source_by_path:
            raise RuntimeError(f"loaded module is outside runtime source closure: {role}")
        output[role] = {
            "module_name": module_name,
            "origin_path": str(origin),
            "repository_relative_path": relative,
        }
    return output


def _repository_loaded_module_closure(source_rows: list[dict]) -> list[dict]:
    """Return every loaded repository module, not only the mandatory safety set."""

    source_by_path = {row["path"]: row for row in source_rows}
    closure: list[dict] = []
    for module_name, module in sorted(sys.modules.items()):
        raw_origin = getattr(module, "__file__", None)
        if not raw_origin:
            continue
        try:
            origin = Path(str(raw_origin)).resolve(strict=True)
            relative = origin.relative_to(ROOT).as_posix()
        except (OSError, ValueError):
            continue
        if origin.suffix in {".so", ".dylib", ".pyd"}:
            continue
        if relative.endswith(".pyc") and "/__pycache__/" in relative:
            relative = str(
                Path(relative).parent.parent
                / (Path(relative).name.split(".", 1)[0] + ".py")
            )
        if relative not in source_by_path:
            raise RuntimeError(f"loaded repository module is outside runtime source closure: {module_name}")
        row = {
            "module_name": module_name,
            "origin_path": str(ROOT / relative),
            "repository_relative_path": relative,
        }
        closure.append(row)
    return sorted(
        closure,
        key=lambda row: (row["module_name"], row["repository_relative_path"]),
    )


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


def _bind_successor_cpp_module_token(expected_venv: Path) -> str:
    token = f"{expected_venv.expanduser().resolve(strict=True)}{os.sep}"
    supplied = os.environ.get(CPP_MODULE_TOKEN_ENV, "")
    if supplied and supplied != token:
        raise RuntimeError("native module token differs from deployment authority")
    os.environ[CPP_MODULE_TOKEN_ENV] = token
    return token


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


def build_startup_attestation(
    *,
    engine: MakerEngine,
    native_runtime: dict,
    running_config_sha256: str,
    safety_authority: Mapping[str, Any] | None = None,
) -> dict:
    """Bind the checkout and restored cooldown state before live loops start."""

    git_toplevel = Path(
        _git_output("rev-parse", "--show-toplevel").decode("utf-8").strip()
    ).resolve(strict=True)
    interpreter_before = _file_byte_identity(sys.executable)
    native_before = _native_runtime_file_identity(native_runtime)
    pre_snapshot = _git_snapshot()
    successor = safety_authority is not None
    if successor:
        for module_name, _relative in DEPLOYMENT_ENVELOPE_KEY_LOADED_RUNTIME_MODULES.values():
            importlib.import_module(module_name)
        loaded_modules_before = _repository_loaded_python_modules()
        source_rows = _runtime_source_rows(
            loaded_paths=tuple(path for _module, path in loaded_modules_before)
        )
        loaded_origins = _loaded_module_origins(
            source_rows,
            DEPLOYMENT_ENVELOPE_KEY_LOADED_RUNTIME_MODULES,
        )
        repository_module_closure = _repository_loaded_module_closure(source_rows)
        loaded_modules_after = _repository_loaded_python_modules()
    else:
        loaded_modules_before = ()
        source_rows = _runtime_source_rows()
        loaded_origins = _loaded_module_origins(source_rows)
        repository_module_closure = []
        loaded_modules_after = ()
    fill_state = engine.fill_cooldown_state_snapshot()
    shadow_runtime = engine.shadow_runtime_snapshot()
    post_snapshot = _git_snapshot()
    interpreter_after = _file_byte_identity(sys.executable)
    native_after = _native_runtime_file_identity(native_runtime)
    safety_authority_valid = False
    if safety_authority is not None:
        safety_config_sha256 = (
            running_config_sha256 == safety_authority.get("config_file_sha256")
        )
        safety_authority_valid = (
            safety_authority.get("execution_commit") == post_snapshot["commit"]
            and safety_authority.get("execution_tree") == post_snapshot["tree"]
            and safety_config_sha256
            and native_after is not None
            and safety_authority.get("native_module_sha256") == native_after["sha256"]
            and str(safety_authority.get("native_wheel_sha256", "")) != ""
            and safety_authority.get("native_soabi")
            == str(sysconfig.get_config_var("SOABI"))
            and native_runtime.get("locked_runtime", {}).get("validated") is True
            and native_runtime.get("locked_runtime", {}).get(
                "release_root_sha256"
            )
            == safety_authority.get("canonical_sha256")
            and interpreter_after.get("sha256")
            == safety_authority.get("locked_runtime_interpreter", {}).get(
                "executable_sha256"
            )
        )

    snapshots_equal = pre_snapshot == post_snapshot
    runtime_files_match = bool(source_rows) and all(
        row["tracked_in_head"] and row["regular_file_under_repository"]
        for row in source_rows
    )
    loaded_under_repo = all(
        Path(row["origin_path"]).is_relative_to(ROOT) for row in loaded_origins.values()
    )
    source_by_path = {row["path"]: row for row in source_rows}
    loaded_match_sources = all(
        row["repository_relative_path"] in source_by_path
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
    if successor:
        native_identity["abi_contract"] = native_runtime.get("abi_contract")
        native_identity["locked_runtime"] = native_runtime.get("locked_runtime")
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
            pre_snapshot["worktree_clean"] == post_snapshot["worktree_clean"]
            and pre_snapshot["status_entry_count"]
            == post_snapshot["status_entry_count"]
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
    flow_enabled = shadow_runtime.get("global_flow_shadow_enabled")
    reference_enabled = shadow_runtime.get("global_reference_shadow_enabled")
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
        shadow_runtime.get("global_flow_shadow_config_explicit") is True
        and shadow_runtime.get("global_reference_shadow_config_explicit") is True
    )
    global_flow_contract_valid = bool(
        flow_enabled is False
        and flow_backend_zero
        and shadow_runtime.get("global_flow_native_effective") is False
        and shadow_runtime.get("state_restore_contract")
        == "shadow_state_never_restored"
    )
    basis_sample_count = shadow_runtime.get(
        "global_reference_bridge_basis_sample_count"
    )
    global_reference_contract_valid = bool(
        reference_enabled is False
        and type(basis_sample_count) is int
        and basis_sample_count == 0
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
        "loaded_module_origins_available": (
            set(loaded_origins)
            == set(
                DEPLOYMENT_ENVELOPE_KEY_LOADED_RUNTIME_MODULES
                if successor
                else KEY_LOADED_RUNTIME_MODULES
            )
        ),
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
    if successor:
        gates.update(
            {
                "deployment_envelope_authority_valid": safety_authority_valid,
                "repository_module_closure_available": bool(repository_module_closure),
                "repository_module_closure_complete": (
                    loaded_modules_before == loaded_modules_after
                    and all(
                        row["repository_relative_path"] in source_by_path
                        for row in repository_module_closure
                    )
                ),
                "mandatory_safety_modules_loaded": set(loaded_origins)
                == set(DEPLOYMENT_ENVELOPE_KEY_LOADED_RUNTIME_MODULES),
            }
        )
    gates["safe_to_start_live_loops"] = all(
        value for name, value in gates.items() if name != "safe_to_start_live_loops"
    )
    errors = sorted(name for name, passed in gates.items() if not passed)
    attestation = {
        "schema_version": STARTUP_ATTESTATION_SCHEMA,
        "status": "accepted" if not errors else "rejected",
        "attested_at_utc": datetime.now(UTC).isoformat(),
        "fill_cooldown_state": fill_state,
        "shadow_runtime_identity": shadow_runtime,
        "running_checkout": checkout,
        "loaded_module_origins": loaded_origins,
        "interpreter_identity": interpreter_identity,
        "native_runtime_identity": native_identity,
        "gates": gates,
        "errors": errors,
    }
    if successor:
        attestation["deployment_envelope"] = {
            "path": str((safety_authority or {}).get("path", "")),
            "canonical_sha256": str(
                (safety_authority or {}).get("canonical_sha256", "")
            ),
        }
        attestation["loaded_repository_module_closure"] = repository_module_closure
    return attestation


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


def audit_native_runtime(
    logger: logging.Logger, *, cfg=None, safety_authority: dict | None = None
) -> dict:
    """Log the persisted runtime profile and fail fast for broken strict native mode."""
    values = {name: os.environ.get(name, "0") for name in CPP_RUNTIME_FLAGS}
    enabled = {
        name: str(value).strip().lower() in {"1", "true", "yes", "on"}
        for name, value in values.items()
    }
    if cfg is not None:
        validate_native_live_routing_policy_compatibility(cfg)
    profile = os.environ.get("NARROWGATE_LIVE_PROFILE_NAME", "unmanaged")
    module_path = "disabled"
    native_build = {"available": False}
    required = set()
    if enabled["NARROWGATE_CPP_QUOTE_CORE"]:
        required.add("compute_quote_core_live")
    if enabled["NARROWGATE_CPP_QUOTE_POLICY_STAGE"]:
        if cfg is not None and not enabled["NARROWGATE_CPP_QUOTE_CORE"]:
            raise RuntimeError(
                "native quote-policy stage requires native quote core"
            )
        strategy_cfg = getattr(cfg, "strategy", None) if cfg is not None else None
        if strategy_cfg is not None and (
            bool(getattr(strategy_cfg, "ber_exposure_add_only", False))
            or bool(getattr(strategy_cfg, "local_extreme_guard_enabled", False))
            or float(getattr(strategy_cfg, "fragile_order_ttl_s", 0.0) or 0.0)
            > 0.0
        ):
            raise RuntimeError(
                "native quote-policy stage does not admit two-pass BER or "
                "local-extreme/fragile-TTL policies"
            )
        required.update(
            {
                "NativeQuotePolicyStage",
                "NativeQuotePolicyStageResult",
                "NATIVE_QUOTE_POLICY_STAGE_AVAILABLE",
            }
        )
    if enabled["NARROWGATE_CPP_LIVE_ROUTING"]:
        required.add("compute_live_routing_decision")
    if enabled["NARROWGATE_CPP_SIGNAL_FEATURES"]:
        required.update({"SignalFeatureEngine", "SIGNAL_FEATURE_NAMES"})
    if enabled["NARROWGATE_CPP_LIGHTGBM_INFERENCE"]:
        if not enabled["NARROWGATE_CPP_STRICT"]:
            raise RuntimeError(
                "native LightGBM live inference requires strict native mode"
            )
        required.update(
            {
                "LIGHTGBM_BUNDLE_HEAD_NAMES",
                "NATIVE_LIGHTGBM_BUNDLE_INFERENCE_AVAILABLE",
                "NativeLightgbmBundle",
            }
        )
    if enabled["NARROWGATE_CPP_REPLACE_CONTINUATION"]:
        required.update(
            {
                "NativeReplaceContinuationState",
                "ReplaceContinuationEventKind",
                "Side",
            }
        )
    if enabled["NARROWGATE_CPP_COOLDOWN"]:
        required.update(NATIVE_COOLDOWN_REQUIRED_APIS)
    if enabled["NARROWGATE_CPP_ORDER_ACTION_PLAN"]:
        required.update(NATIVE_ORDER_ACTION_REQUIRED_APIS)
    if enabled["NARROWGATE_CPP_FINAL_ORDER_PLAN"]:
        required.update(NATIVE_ORDER_ACTION_REQUIRED_APIS)
        required.update(NATIVE_FINAL_ORDER_PLAN_REQUIRED_APIS)
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
            module = load_native_module()
            module_path = str(getattr(module, "__file__", "<unknown>"))
            validate_native_capabilities(module, symbols=tuple(sorted(required)))
            if enabled["NARROWGATE_CPP_QUOTE_CORE"]:
                quote_fields = {**NATIVE_QUOTE_ABI_FIELDS, **QUOTE_CORE_CPP_ABI_FIELDS}
                validate_native_capabilities(module, fields=quote_fields)
            if enabled["NARROWGATE_CPP_GLOBAL_FLOW"]:
                aggregator = module.TradeBarAggregator(False)
                if not hasattr(aggregator, "update_batch"):
                    raise RuntimeError("narrowgate_cpp ABI missing TradeBarAggregator.update_batch")
            if enabled["NARROWGATE_CPP_LIGHTGBM_INFERENCE"] and not bool(
                module.NATIVE_LIGHTGBM_BUNDLE_INFERENCE_AVAILABLE
            ):
                raise RuntimeError(
                    "narrowgate_cpp LightGBM bundle inference is unavailable"
                )
            if enabled["NARROWGATE_CPP_LIGHTGBM_INFERENCE"]:
                from strategy.model_contract import REQUIRED_MODEL_HEADS

                if tuple(module.LIGHTGBM_BUNDLE_HEAD_NAMES) != tuple(
                    REQUIRED_MODEL_HEADS
                ):
                    raise RuntimeError(
                        "narrowgate_cpp LightGBM bundle head order differs "
                        "from the Python model contract"
                    )
            if enabled["NARROWGATE_CPP_REPLACE_CONTINUATION"]:
                validate_replace_continuation(module)
            if enabled["NARROWGATE_CPP_COOLDOWN"] and not bool(
                module.NATIVE_LIVE_COOLDOWN_HOT_PATH_AVAILABLE
            ):
                raise RuntimeError(
                    "narrowgate_cpp cooldown hot-path capability is unavailable"
                )
            if enabled["NARROWGATE_CPP_ORDER_ACTION_PLAN"] and not bool(
                module.NATIVE_LIVE_ORDER_ACTION_PLAN_AVAILABLE
            ):
                raise RuntimeError(
                    "narrowgate_cpp order-action planner capability is unavailable"
                )
            if enabled["NARROWGATE_CPP_FINAL_ORDER_PLAN"] and not bool(
                module.NATIVE_LIVE_FINAL_ORDER_PLAN_AVAILABLE
            ):
                raise RuntimeError(
                    "narrowgate_cpp final-order planner capability is unavailable"
                )
            if enabled["NARROWGATE_CPP_QUOTE_POLICY_STAGE"] and not bool(
                module.NATIVE_QUOTE_POLICY_STAGE_AVAILABLE
            ):
                raise RuntimeError(
                    "narrowgate_cpp quote-policy stage capability is unavailable"
                )
            native_build = {
                "available": all(
                    hasattr(module, name)
                    for name in (
                        "NATIVE_LIVE_BUILD_PROFILE",
                        "NATIVE_LIVE_BUILD_COMPILE_OPTIONS",
                        "NATIVE_LIVE_BUILD_IS_PRODUCTION",
                        "NATIVE_LIVE_BUILD_VECTOR_WIDTH_BITS",
                    )
                ),
                "profile": str(
                    getattr(module, "NATIVE_LIVE_BUILD_PROFILE", "unknown")
                ),
                "compile_options": str(
                    getattr(
                        module,
                        "NATIVE_LIVE_BUILD_COMPILE_OPTIONS",
                        "unknown",
                    )
                ),
                "production": bool(
                    getattr(module, "NATIVE_LIVE_BUILD_IS_PRODUCTION", False)
                ),
                "preferred_vector_width_bits": int(
                    getattr(module, "NATIVE_LIVE_BUILD_VECTOR_WIDTH_BITS", 0)
                ),
            }
        except Exception as exc:
            if enabled["NARROWGATE_CPP_STRICT"]:
                raise RuntimeError(
                    f"strict native profile {profile!r} failed preflight: {exc}"
                ) from exc
            if enabled["NARROWGATE_CPP_REPLACE_CONTINUATION"]:
                raise RuntimeError(
                    "explicit native replacement-continuation failed preflight: "
                    f"{exc}"
                ) from exc
            if enabled["NARROWGATE_CPP_ORDER_ACTION_PLAN"]:
                raise RuntimeError(
                    "explicit native order-action planner failed preflight: "
                    f"{exc}"
                ) from exc
            if enabled["NARROWGATE_CPP_FINAL_ORDER_PLAN"]:
                raise RuntimeError(
                    "explicit native final-order planner failed preflight: "
                    f"{exc}"
                ) from exc
            raise RuntimeError(
                f"requested native profile {profile!r} failed preflight: {exc}"
            ) from exc

    logger.info(
        "NATIVE_PROFILE name=%s quote_core=%d signal_features=%d "
        "lightgbm_inference=%d "
        "quote_policy_stage=%d global_flow_requested=%d global_flow_effective=%d "
        "live_routing=%d replace_continuation=%d cooldown=%d "
        "order_action_plan=%d final_order_plan=%d strict=%d module=%s",
        profile,
        int(enabled["NARROWGATE_CPP_QUOTE_CORE"]),
        int(enabled["NARROWGATE_CPP_SIGNAL_FEATURES"]),
        int(enabled["NARROWGATE_CPP_LIGHTGBM_INFERENCE"]),
        int(enabled["NARROWGATE_CPP_QUOTE_POLICY_STAGE"]),
        int(enabled["NARROWGATE_CPP_GLOBAL_FLOW"]),
        int(global_flow_effective),
        int(enabled["NARROWGATE_CPP_LIVE_ROUTING"]),
        int(enabled["NARROWGATE_CPP_REPLACE_CONTINUATION"]),
        int(enabled["NARROWGATE_CPP_COOLDOWN"]),
        int(enabled["NARROWGATE_CPP_ORDER_ACTION_PLAN"]),
        int(enabled["NARROWGATE_CPP_FINAL_ORDER_PLAN"]),
        int(enabled["NARROWGATE_CPP_STRICT"]),
        module_path,
    )
    abi_contract = {
        "schema_version": NATIVE_ABI_CONTRACT_SCHEMA,
        "required_apis": sorted(required),
        "required_quote_fields": {
            name: list(fields) for name, fields in sorted(NATIVE_QUOTE_ABI_FIELDS.items())
        },
        "validated": bool(not required or not module_path.startswith("unavailable:")),
    }
    if safety_authority is not None:
        candidate = Path(module_path).expanduser()
        if (
            profile != "native"
            or any(
                not enabled[name]
                for name in CPP_RUNTIME_FLAGS
                if name not in OPTIONAL_CPP_RUNTIME_FLAGS
            )
            or global_flow_effective is not False
            or module_path == "disabled"
            or module_path.startswith("unavailable:")
            or candidate.is_symlink()
            or not candidate.is_file()
            or _sha256_bytes(candidate.resolve(strict=True).read_bytes())
            != safety_authority.get("native_module_sha256")
            or str(sysconfig.get_config_var("SOABI"))
            != safety_authority.get("native_soabi")
        ):
            raise RuntimeError("native runtime differs from deployment authority")
        install_receipt_path = Path(
            str(safety_authority.get("install_receipt_path", ""))
        ).expanduser()
        if (
            not install_receipt_path.is_absolute()
            or install_receipt_path.is_symlink()
            or not install_receipt_path.is_file()
        ):
            raise RuntimeError(
                "locked runtime install receipt differs from deployment authority"
            )
        frozen_interpreter = safety_authority.get("locked_runtime_interpreter")
        if not isinstance(frozen_interpreter, Mapping):
            raise RuntimeError("locked runtime interpreter authority is missing")
        selected_venv = ROOT / ".venv-active"
        expected_venv = install_receipt_path.parent / (
            f"venv-{safety_authority.get('execution_commit', '')}"
        )
        expected_python = expected_venv / "bin" / "python3"
        reported_python = Path(sys.executable).expanduser().absolute()
        try:
            selector_target = os.readlink(selected_venv)
            resolved_selector = selected_venv.resolve(strict=True)
            locked_runtime_python = reported_python.resolve(strict=True)
        except OSError as exc:
            raise RuntimeError("successor venv selector authority is unavailable") from exc
        if (
            not selected_venv.is_symlink()
            or selector_target != str(expected_venv)
            or resolved_selector != expected_venv
            or reported_python != selected_venv / "bin" / "python3"
            or locked_runtime_python != expected_python
            or expected_venv.is_symlink()
            or not expected_venv.is_dir()
            or expected_python.is_symlink()
            or not expected_python.is_file()
            or _sha256_bytes(expected_python.read_bytes())
            != frozen_interpreter.get("executable_sha256")
        ):
            raise RuntimeError("successor venv selector authority drifted")
        try:
            locked_receipt = locked_runtime.validate_startup_runtime(
                venv_python=locked_runtime_python,
                pip_runner_python=locked_runtime_python,
                receipt_path=install_receipt_path,
                expected_receipt_sha256=str(
                    safety_authority["install_receipt_canonical_sha256"]
                ),
                expected_lock_sha256=str(
                    safety_authority["runtime_lock_canonical_sha256"]
                ),
                expected_wheelhouse_sha256=str(
                    safety_authority["wheelhouse_canonical_sha256"]
                ),
                expected_root_wheel_sha256=str(
                    safety_authority["root_wheel_sha256"]
                ),
                expected_native_wheel_sha256=str(
                    safety_authority["native_wheel_sha256"]
                ),
                expected_python_version=str(frozen_interpreter["version"]),
                expected_soabi=str(frozen_interpreter["soabi"]),
                expected_compiler=str(frozen_interpreter["compiler"]),
                expected_openssl_runtime=str(
                    frozen_interpreter["openssl_runtime"]
                ),
                expected_interpreter_executable_sha256=str(
                    frozen_interpreter["executable_sha256"]
                ),
            )
        except (KeyError, locked_runtime.LockedRuntimeError) as exc:
            raise RuntimeError("locked successor runtime validation failed") from exc
        if (
            locked_receipt.get("interpreter") != frozen_interpreter
            or locked_receipt.get("installed_record_aggregate_sha256")
            != safety_authority.get("installed_record_aggregate_sha256")
        ):
            raise RuntimeError("locked successor runtime receipt authority drifted")
        _bind_successor_cpp_module_token(expected_venv)
    runtime_identity = {
        "profile": profile,
        "module": module_path,
        **enabled,
        "NARROWGATE_CPP_GLOBAL_FLOW_REQUESTED": enabled[
            "NARROWGATE_CPP_GLOBAL_FLOW"
        ],
        "NARROWGATE_CPP_GLOBAL_FLOW_EFFECTIVE": global_flow_effective,
        "native_build": native_build,
        "abi_contract": abi_contract,
    }
    if safety_authority is not None:
        runtime_identity["locked_runtime"] = {
            "validated": True,
            "release_root_sha256": safety_authority["canonical_sha256"],
            "venv_selector_path": str(selected_venv),
            "venv_selector_target": selector_target,
            "venv_real_path": str(expected_venv),
            "python_real_path": str(locked_runtime_python),
            "install_receipt_path": str(install_receipt_path.resolve(strict=True)),
        }
    return runtime_identity


class _DrainingQueueListener(logging.handlers.QueueListener):
    """Queue listener that exposes worker failure and never blocks forever."""

    _fallback_lock = threading.Lock()

    def __init__(
        self,
        record_queue: queue.Queue[logging.LogRecord | None],
        *handlers: logging.Handler,
        stop_timeout_s: float = 2.0,
    ) -> None:
        super().__init__(record_queue, *handlers)
        self._failure_lock = threading.Lock()
        self._failure: BaseException | None = None
        self._stop_lock = threading.Lock()
        self._stop_timeout_s = float(stop_timeout_s)
        self._sentinel_enqueued = False
        if not math.isfinite(self._stop_timeout_s) or self._stop_timeout_s <= 0.0:
            raise ValueError("stop_timeout_s must be finite and positive")

    @classmethod
    def _emit_worker_fallback(
        cls,
        marker: str,
        *,
        record: logging.LogRecord | None = None,
        detail: str = "",
    ) -> None:
        if record is None:
            message = "none"
            level_name = "none"
            logger_name = "none"
        else:
            try:
                message = record.getMessage()
            except Exception:  # pragma: no cover - malformed external record
                message = repr(record.msg)
            level_name = record.levelname
            logger_name = record.name
        payload = (
            f"{marker} synchronous_stderr_fallback=1 "
            f"level={level_name} logger={logger_name} "
            f"detail={detail} message={message}\n"
        ).encode("utf-8", errors="backslashreplace")
        with cls._fallback_lock:
            try:
                os.write(2, payload)
            except OSError:  # pragma: no cover - process teardown edge
                pass

    def _capture_failure(
        self,
        exc: BaseException,
        record: logging.LogRecord,
    ) -> None:
        with self._failure_lock:
            if self._failure is None:
                self._failure = exc
        self._emit_worker_fallback(
            "ASYNC_LOG_LISTENER_FAILED",
            record=record,
            detail=f"{type(exc).__name__}:{exc}",
        )

    @property
    def failure(self) -> BaseException | None:
        with self._failure_lock:
            return self._failure

    def health_snapshot(self) -> dict[str, object]:
        failure = self.failure
        thread = self._thread
        return {
            "valid": failure is None,
            "worker_alive": bool(thread is not None and thread.is_alive()),
            "failure_type": None if failure is None else type(failure).__name__,
            "failure_message": None if failure is None else str(failure),
        }

    def _drain_failed_queue_to_stderr(self) -> None:
        while True:
            try:
                record = self.queue.get_nowait()
            except queue.Empty:
                return
            try:
                if record is not self._sentinel:
                    self._emit_worker_fallback(
                        "ASYNC_LOG_LISTENER_UNDELIVERED",
                        record=record,
                        detail="worker_unavailable",
                    )
            finally:
                self.queue.task_done()

    def _monitor(self) -> None:
        while True:
            record = self.dequeue(True)
            if record is self._sentinel:
                self.queue.task_done()
                return
            try:
                self.handle(record)
            except BaseException as exc:
                self._capture_failure(exc, record)
                self.queue.task_done()
                self._drain_failed_queue_to_stderr()
                return
            self.queue.task_done()

    def enqueue_sentinel(self) -> None:
        self.queue.put(self._sentinel, timeout=self._stop_timeout_s)

    def stop(self) -> None:
        with self._stop_lock:
            thread = self._thread
            if thread is None:
                return
            if not thread.is_alive():
                self._thread = None
                self._drain_failed_queue_to_stderr()
            else:
                if not self._sentinel_enqueued:
                    try:
                        self.enqueue_sentinel()
                    except queue.Full as exc:
                        self._emit_worker_fallback(
                            "ASYNC_LOG_LISTENER_STOP_TIMEOUT",
                            detail="sentinel_queue_full",
                        )
                        raise RuntimeError(
                            "async logging listener stop timed out on full queue"
                        ) from exc
                    self._sentinel_enqueued = True
                thread.join(timeout=self._stop_timeout_s)
                if thread.is_alive():
                    self._emit_worker_fallback(
                        "ASYNC_LOG_LISTENER_STOP_TIMEOUT",
                        detail="worker_join_timeout",
                    )
                    raise RuntimeError(
                        "async logging listener did not stop before deadline"
                    )
                self._thread = None
            failure = self.failure
            if failure is not None:
                raise RuntimeError("async logging listener worker failed") from failure


class _OrderedQueueHandler(logging.handlers.QueueHandler):
    """Linearize producers without letting a logging fault escape to callers.

    Queue/listener failures remain fail-closed runtime health failures through
    :meth:`raise_if_failed`.  A normal ``logger.*`` call only records the
    failure and emits the synchronous stderr alarm, so it cannot interrupt an
    exchange safety latch or the next shutdown cleanup step.
    """

    _late_fallback_lock = threading.Lock()

    def __init__(
        self,
        record_queue: queue.Queue[logging.LogRecord | None],
        listener: _DrainingQueueListener,
    ) -> None:
        super().__init__(record_queue)
        self._listener = listener
        self._admission_condition = threading.Condition(threading.Lock())
        self._accepting = True
        self._in_flight = 0
        self._next_ticket = 0
        self._serving_ticket = 0
        self._failure: RuntimeError | None = None
        self._stop_started = False
        self._stopped = False
        self._stop_error: BaseException | None = None

    @classmethod
    def _emit_sync_stderr(cls, marker: str, record: logging.LogRecord) -> None:
        """Write an allocation-light fallback independent of the async sink."""

        try:
            message = record.getMessage()
        except BaseException:  # pragma: no cover - malformed external LogRecord
            try:
                message = repr(record.msg)
            except BaseException:
                message = "<unformattable-log-record>"
        payload = (
            f"{marker} synchronous_stderr_fallback=1 "
            f"level={record.levelname} logger={record.name} message={message}\n"
        ).encode("utf-8", errors="backslashreplace")
        with cls._late_fallback_lock:
            try:
                os.write(2, payload)
            except BaseException:  # pragma: no cover - process teardown edge
                pass

    @classmethod
    def _emit_after_shutdown(cls, record: logging.LogRecord) -> None:
        """Preserve a producer that selected this handler before detach."""

        cls._emit_sync_stderr("ASYNC_LOG_AFTER_SHUTDOWN", record)

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """Copy the record without formatting it on the producer thread."""

        return copy.copy(record)

    def handle(self, record: logging.LogRecord) -> bool:
        """Assign a FIFO ticket before filters and account for shutdown races."""

        critical_fallback_emitted = record.levelno >= logging.CRITICAL
        if critical_fallback_emitted:
            # Do this before admission so a stalled filter or listener cannot
            # hide the process's last safety message.
            self._emit_sync_stderr("ASYNC_LOG_CRITICAL", record)
        with self._admission_condition:
            listener_failure = self._listener.failure
            if listener_failure is not None and self._failure is None:
                self._failure = RuntimeError(
                    "async logging listener worker failed; live must fail closed"
                )
                self._accepting = False
            if self._failure is not None:
                if not critical_fallback_emitted:
                    self._emit_sync_stderr("ASYNC_LOG_HANDLER_FAILED", record)
                return False
            if not self._accepting:
                if not critical_fallback_emitted:
                    self._emit_after_shutdown(record)
                return False
            ticket = self._next_ticket
            self._next_ticket += 1
            self._in_flight += 1
            while ticket != self._serving_ticket:
                self._admission_condition.wait()
        try:
            listener_failure = self._listener.failure
            with self._admission_condition:
                if listener_failure is not None and self._failure is None:
                    self._failure = RuntimeError(
                        "async logging listener worker failed; live must fail closed"
                    )
                    self._accepting = False
                failure = self._failure
            if failure is not None:
                if not critical_fallback_emitted:
                    self._emit_sync_stderr("ASYNC_LOG_HANDLER_FAILED", record)
                return False
            try:
                accepted = self.filter(record)
            except BaseException:
                failure = RuntimeError(
                    "async logging handler filter failed; live must fail closed"
                )
                with self._admission_condition:
                    if self._failure is None:
                        self._failure = failure
                    self._accepting = False
                    self._admission_condition.notify_all()
                if not critical_fallback_emitted:
                    self._emit_sync_stderr("ASYNC_LOG_HANDLER_FAILED", record)
                return False
            if accepted:
                self.emit(record)
            return accepted
        finally:
            with self._admission_condition:
                self._serving_ticket += 1
                self._in_flight -= 1
                self._admission_condition.notify_all()

    def emit(self, record: logging.LogRecord) -> None:
        listener_failure = self._listener.failure
        if listener_failure is not None:
            failure = RuntimeError(
                "async logging listener worker failed; live must fail closed"
            )
            with self._admission_condition:
                if self._failure is None:
                    self._failure = failure
                self._accepting = False
                self._admission_condition.notify_all()
            self._emit_sync_stderr("ASYNC_LOG_HANDLER_FAILED", record)
            return
        try:
            prepared = self.prepare(record)
            self.enqueue(prepared)
        except queue.Full:
            failure = RuntimeError(
                "async logging queue full; live runtime must fail closed"
            )
            with self._admission_condition:
                if self._failure is None:
                    self._failure = failure
                else:
                    failure = self._failure
                self._accepting = False
                self._admission_condition.notify_all()
            self._emit_sync_stderr(
                (
                    "ASYNC_LOG_QUEUE_FULL "
                    f"capacity={self.queue.maxsize} action=fail_closed"
                ),
                record,
            )
            return
        except BaseException as exc:
            failure = RuntimeError(
                "async logging enqueue failed; live runtime must fail closed: "
                f"{type(exc).__name__}:{exc}"
            )
            with self._admission_condition:
                if self._failure is None:
                    self._failure = failure
                self._accepting = False
                self._admission_condition.notify_all()
            self._emit_sync_stderr("ASYNC_LOG_HANDLER_FAILED", record)
            return
        listener_failure = self._listener.failure
        if listener_failure is not None:
            # The listener either consumed this record before failing or stop()
            # will preserve it from the residual queue. Emit an immediate alarm
            # as well so the producer cannot mistake enqueue for durable output.
            failure = RuntimeError(
                "async logging listener worker failed; live must fail closed"
            )
            with self._admission_condition:
                if self._failure is None:
                    self._failure = failure
                self._accepting = False
                self._admission_condition.notify_all()
            self._emit_sync_stderr("ASYNC_LOG_HANDLER_FAILED", record)
            return

    def health_snapshot(self) -> dict[str, object]:
        listener = self._listener.health_snapshot()
        with self._admission_condition:
            failure = self._failure
            accepting = self._accepting
            stopped = self._stopped
        return {
            "valid": failure is None and bool(listener["valid"]),
            "accepting": accepting,
            "stopped": stopped,
            "queue_depth": self.queue.qsize(),
            "queue_capacity": self.queue.maxsize,
            "failure_type": None if failure is None else type(failure).__name__,
            "failure_message": None if failure is None else str(failure),
            "listener": listener,
        }

    def raise_if_failed(self) -> None:
        listener_failure = self._listener.failure
        with self._admission_condition:
            failure = self._failure
        if failure is not None:
            raise RuntimeError("async logging handler failed") from failure
        if listener_failure is not None:
            raise RuntimeError("async logging listener worker failed") from (
                listener_failure
            )

    def stop_and_drain(self) -> None:
        """Stop admission only after every record accepted before shutdown."""

        with self._admission_condition:
            if self._stopped:
                return
            if self._stop_started:
                while self._stop_started:
                    self._admission_condition.wait()
                if self._stopped:
                    return
                if self._stop_error is not None:
                    raise RuntimeError("async logging stop failed") from (
                        self._stop_error
                    )
            self._stop_started = True
            self._stop_error = None
            self._accepting = False
            while self._in_flight:
                self._admission_condition.wait()
        stop_error: BaseException | None = None
        try:
            self._listener.stop()
        except BaseException as exc:
            stop_error = exc
            raise
        finally:
            with self._admission_condition:
                self._stop_started = False
                self._stop_error = stop_error
                self._stopped = stop_error is None
                self._admission_condition.notify_all()


class _AsyncLoggingRuntime:
    """Own the one live logging queue, listener, and downstream handlers."""

    def __init__(
        self,
        *,
        root_logger: logging.Logger,
        queue_handler: _OrderedQueueHandler,
        listener: _DrainingQueueListener,
        sink_handlers: tuple[logging.Handler, ...],
        previous_handlers: tuple[logging.Handler, ...],
        previous_level: int,
    ) -> None:
        self.root_logger = root_logger
        self.queue_handler = queue_handler
        self.listener = listener
        self.sink_handlers = sink_handlers
        self.previous_handlers = previous_handlers
        self.previous_level = previous_level
        self._close_lock = threading.Lock()
        self._closed = False

    def health_snapshot(self) -> dict[str, object]:
        health = self.queue_handler.health_snapshot()
        health["closed"] = self._closed
        return health

    def raise_if_failed(self) -> None:
        self.queue_handler.raise_if_failed()

    def close(self) -> None:
        """Drain while attached, atomically restore handlers, then close sinks."""

        with self._close_lock:
            if self._closed:
                return
            drain_error: BaseException | None = None
            try:
                # Keep the handler selected while admission closes. Producers
                # racing shutdown are synchronously preserved on stderr.
                self.queue_handler.stop_and_drain()
            except BaseException as exc:
                drain_error = exc
            finally:
                # Assign the complete handler set in one operation: there is no
                # interval in which the root logger has no destination.
                self.root_logger.handlers = list(self.previous_handlers)
                self.root_logger.setLevel(self.previous_level)

            worker = self.listener._thread
            if worker is not None and worker.is_alive():
                # The worker can still be inside handler.emit(). Closing that
                # handler here is a use-after-close race. Keep the runtime
                # retryable; the process is already fail-closed by the error.
                self.listener._emit_worker_fallback(
                    "ASYNC_LOG_SINK_CLOSE_DEFERRED",
                    detail="listener_worker_still_alive",
                )
                if drain_error is not None:
                    raise drain_error
                raise RuntimeError(
                    "async logging listener remains alive after shutdown"
                )

            self._closed = True
            try:
                self.queue_handler.close()
                for handler in self.sink_handlers:
                    try:
                        handler.flush()
                    finally:
                        handler.close()
            finally:
                if drain_error is not None:
                    raise drain_error


_ACTIVE_LOGGING_RUNTIME_LOCK = threading.Lock()
_ACTIVE_LOGGING_RUNTIME: _AsyncLoggingRuntime | None = None


def shutdown_logging(runtime: _AsyncLoggingRuntime | None = None) -> None:
    """Drain one configured runtime; with no argument, drain the active one."""

    global _ACTIVE_LOGGING_RUNTIME
    with _ACTIVE_LOGGING_RUNTIME_LOCK:
        selected = _ACTIVE_LOGGING_RUNTIME if runtime is None else runtime
        if selected is _ACTIVE_LOGGING_RUNTIME:
            _ACTIVE_LOGGING_RUNTIME = None
    if selected is not None:
        selected.close()


def setup_logging(
    cfg,
    *,
    queue_capacity: int = ASYNC_LOG_QUEUE_CAPACITY,
) -> _AsyncLoggingRuntime:
    """Configure one bounded async fan-out. Paths must already be absolute."""

    global _ACTIVE_LOGGING_RUNTIME
    capacity = int(queue_capacity)
    if capacity <= 0:
        raise ValueError("queue_capacity must be positive")
    shutdown_logging()

    level = getattr(logging, cfg.logging.level.upper(), logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sink_handlers: list[logging.Handler] = []
    listener: _DrainingQueueListener | None = None
    queue_handler: _OrderedQueueHandler | None = None
    root_logger: logging.Logger | None = None
    previous_handlers: tuple[logging.Handler, ...] = ()
    previous_level = logging.NOTSET
    try:
        if cfg.logging.console:
            sink_handlers.append(logging.StreamHandler(sys.stdout))
        if cfg.logging.file:
            log_path = Path(cfg.logging.file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            sink_handlers.append(
                logging.handlers.RotatingFileHandler(
                    str(log_path), maxBytes=10_000_000, backupCount=5
                )
            )
        for handler in sink_handlers:
            handler.setFormatter(formatter)

        record_queue: queue.Queue[logging.LogRecord | None] = queue.Queue(
            maxsize=capacity
        )
        listener = _DrainingQueueListener(record_queue, *sink_handlers)
        queue_handler = _OrderedQueueHandler(record_queue, listener)
        root_logger = logging.getLogger()
        previous_handlers = tuple(root_logger.handlers)
        previous_level = root_logger.level

        listener.start()
        for handler in previous_handlers:
            root_logger.removeHandler(handler)
        root_logger.addHandler(queue_handler)
        root_logger.setLevel(level)
        runtime = _AsyncLoggingRuntime(
            root_logger=root_logger,
            queue_handler=queue_handler,
            listener=listener,
            sink_handlers=tuple(sink_handlers),
            previous_handlers=previous_handlers,
            previous_level=previous_level,
        )
    except BaseException as primary_error:
        cleanup_errors: list[tuple[str, BaseException]] = []
        if root_logger is not None:
            try:
                root_logger.handlers = list(previous_handlers)
                root_logger.setLevel(previous_level)
            except BaseException as exc:  # pragma: no cover - custom logger edge
                cleanup_errors.append(("logging-root-restore", exc))
        listener_stopped = listener is None
        if listener is not None:
            try:
                listener.stop()
                listener_stopped = True
            except BaseException as exc:
                cleanup_errors.append(("logging-listener", exc))
                worker = listener._thread
                listener_stopped = not bool(worker is not None and worker.is_alive())
        if listener_stopped:
            if queue_handler is not None:
                try:
                    queue_handler.close()
                except BaseException as exc:  # pragma: no cover - stdlib edge
                    cleanup_errors.append(("logging-queue-handler", exc))
            for handler in sink_handlers:
                try:
                    handler.close()
                except BaseException as exc:  # pragma: no cover - stdlib edge
                    cleanup_errors.append(("logging-sink", exc))
        _note_startup_cleanup_errors(primary_error, cleanup_errors)
        raise

    with _ACTIVE_LOGGING_RUNTIME_LOCK:
        _ACTIVE_LOGGING_RUNTIME = runtime
    return runtime


def _safe_runtime_log(
    logger: logging.Logger,
    level: int,
    message: object,
    *args: object,
    exc_info: object | None = None,
) -> None:
    """Best-effort diagnostic that can never alter safety control flow."""

    try:
        logger.log(level, message, *args, exc_info=exc_info)
        return
    except BaseException as logging_error:
        try:
            rendered = str(message)
            if args:
                rendered = rendered % args
        except BaseException:
            rendered = "<unformattable-runtime-log>"
        try:
            detail = f"{type(logging_error).__name__}:{logging_error}"
        except BaseException:
            detail = "<unformattable-logging-error>"
        try:
            payload = (
                "RUNTIME_LOG_CALL_FAILED synchronous_stderr_fallback=1 "
                f"level={logging.getLevelName(level)} detail={detail} "
                f"message={rendered}\n"
            ).encode("utf-8", errors="backslashreplace")
            with _DrainingQueueListener._fallback_lock:
                os.write(2, payload)
        except BaseException:  # pragma: no cover - process teardown edge
            pass


atexit.register(shutdown_logging)


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
    policy_admission: Mapping[str, Any],
    safety_authority: Mapping[str, Any] | None = None,
    dry_run: bool,
    engine: MakerEngine | None = None,
    startup_exchange_reconciliation: Mapping[str, Any] | None = None,
) -> tuple[Path, dict]:
    """Persist and return the identity that actually governs this process."""
    resolved_config = config_path.expanduser().resolve()
    model_dir = Path(str(cfg.ml.model_dir)).expanduser()
    if not model_dir.is_absolute():
        model_dir = ROOT / model_dir
    log_file = Path(str(cfg.logging.file)).expanduser()
    identity_path = (
        log_file.parent / "runtime_identity.json"
        if str(cfg.logging.file).strip()
        else ROOT / "logs" / "runtime_identity.json"
    )
    has_loaded_source_identity = bool(
        getattr(cfg, "_source_file_path", None)
        or getattr(cfg, "_source_file_sha256", None)
        or getattr(cfg, "_source_file_identity", None)
    )
    if has_loaded_source_identity:
        config_sha256 = revalidate_loaded_config_source(cfg, resolved_config)
    elif engine is not None:
        raise RuntimeError("live engine config lacks its loaded source identity")
    else:
        config_sha256 = hashlib.sha256(resolved_config.read_bytes()).hexdigest()
    identity = {
        "schema_version": "narrowgate_live_runtime_identity.v1",
        "recorded_at_utc": datetime.now(UTC).isoformat(),
        "pid": os.getpid(),
        "python_executable": sys.executable,
        "python_version": ".".join(map(str, sys.version_info[:3])),
        "config_path": str(resolved_config),
        "config_sha256": config_sha256,
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
        "policy_admission": dict(policy_admission),
        "dynamic_fill_hazard_action_enabled": bool(cfg.strategy.dynamic_fill_hazard_action_enabled),
        "f05_boolean_cooldown_enabled": bool(cfg.strategy.boolean_cooldown_policy_enabled),
        "f05_boolean_cooldown_ema_warmup_s": float(cfg.strategy.boolean_cooldown_ema_warmup_s),
        "f05_buy_e3_enabled": bool(cfg.strategy.buy_e3_cooldown_policy_enabled),
        "f05_buy_e3_ema_warmup_s": float(cfg.strategy.buy_e3_cooldown_ema_warmup_s),
    }
    if engine is not None:
        attestation = build_startup_attestation(
            engine=engine,
            native_runtime=native_runtime,
            running_config_sha256=config_sha256,
            safety_authority=safety_authority,
        )
        identity["startup_attestation"] = attestation
    if startup_exchange_reconciliation is not None:
        identity["startup_exchange_reconciliation"] = dict(
            startup_exchange_reconciliation
        )
    if has_loaded_source_identity:
        revalidate_loaded_config_source(cfg, resolved_config)
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


def _strict_stopped_position_rows(
    rows: object,
    *,
    symbol: str,
) -> list[dict[str, str | int]]:
    """Normalize one strict one-way positionRisk row for a stopped barrier."""

    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], Mapping):
        raise RuntimeError("stopped reconciliation requires exactly one positionRisk row")
    row = rows[0]
    if str(row.get("symbol", "")) != symbol or str(row.get("positionSide", "")) != "BOTH":
        raise RuntimeError("stopped reconciliation requires one-way/BOTH positionRisk")
    try:
        quantity = Decimal(str(row.get("positionAmt", "")))
        entry_price = Decimal(str(row.get("entryPrice", "")))
        update_time_ms = int(row.get("updateTime", 0) or 0)
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise RuntimeError("stopped reconciliation positionRisk is malformed") from exc
    if (
        not quantity.is_finite()
        or not entry_price.is_finite()
        or entry_price < 0
        or update_time_ms < 0
    ):
        raise RuntimeError("stopped reconciliation positionRisk is invalid")
    return [
        {
            "symbol": symbol,
            "position_side": "BOTH",
            "position_amt": str(row.get("positionAmt", "")),
            "entry_price": str(row.get("entryPrice", "")),
            "update_time_ms": update_time_ms,
        }
    ]


def build_stopped_exchange_reconciliation(
    rest,
    *,
    symbol: str,
    api_key: str,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    """Double-read signed exchange state while every local maker is stopped."""

    get_orders = getattr(rest, "get_orders", None)
    get_position_risk = getattr(rest, "get_position_risk", None)
    if not callable(get_orders) or not callable(get_position_risk):
        raise RuntimeError("stopped reconciliation requires signed Futures endpoints")
    first_orders = get_orders(symbol=symbol)
    if first_orders != []:
        raise RuntimeError("stopped reconciliation requires zero exchange open orders")
    first_position = _strict_stopped_position_rows(
        get_position_risk(symbol=symbol), symbol=symbol
    )
    second_orders = get_orders(symbol=symbol)
    if second_orders != []:
        raise RuntimeError("exchange open orders appeared during stopped reconciliation")
    second_position = _strict_stopped_position_rows(
        get_position_risk(symbol=symbol), symbol=symbol
    )
    if second_position != first_position:
        raise RuntimeError("positionRisk drifted during stopped reconciliation")
    payload: dict[str, Any] = {
        "schema_version": "narrowgate_stopped_exchange_reconciliation.v1",
        "status": "signed_open_orders_zero_exact_position_stable",
        "generated_utc": generated_utc
        or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "symbol": symbol,
        "open_order_count": 0,
        "signed_endpoints": [
            "/fapi/v1/openOrders",
            POSITION_RISK_RECONCILIATION_ENDPOINT,
        ],
        "signed_read_sequence": [
            "/fapi/v1/openOrders",
            POSITION_RISK_RECONCILIATION_ENDPOINT,
            "/fapi/v1/openOrders",
            POSITION_RISK_RECONCILIATION_ENDPOINT,
        ],
        "account_key_sha256": _sha256_bytes(str(api_key).encode("utf-8")),
        "position_rows": first_position,
    }
    canonical_raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    payload["canonical_exchange_reconciliation_sha256"] = _sha256_bytes(
        canonical_raw
    )
    return payload


def write_stopped_exchange_reconciliation(
    rest,
    *,
    symbol: str,
    api_key: str,
    output_path: Path,
    generated_utc: str | None = None,
) -> dict[str, str]:
    """Atomically publish a private stopped-exchange reconciliation authority."""

    candidate = output_path.expanduser()
    if not candidate.is_absolute() or "\x00" in str(candidate):
        raise ValueError("stopped reconciliation output path must be absolute")
    if candidate.exists() or candidate.is_symlink():
        raise ValueError("stopped reconciliation output must be create-only")
    payload = build_stopped_exchange_reconciliation(
        rest,
        symbol=symbol,
        api_key=api_key,
        generated_utc=generated_utc,
    )
    write_runtime_identity(candidate, payload)
    resolved = candidate.resolve(strict=True)
    metadata = resolved.stat()
    if metadata.st_mode & 0o777 != 0o600 or metadata.st_nlink != 1:
        raise RuntimeError("stopped reconciliation output permissions drifted")
    return {
        "path": str(resolved),
        "canonical_sha256": str(
            payload["canonical_exchange_reconciliation_sha256"]
        ),
    }


def validate_startup_exchange_reconciliation_lineage(
    rest,
    *,
    engine: MakerEngine,
    symbol: str,
    api_key: str,
) -> dict[str, str]:
    """Bind the new process to the stopped transaction's signed barrier."""

    path_text = os.environ.get(
        "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_PATH", ""
    ).strip()
    path = Path(path_text).expanduser()
    if (
        not path_text
        or not path.is_absolute()
        or path.is_symlink()
        or not path.is_file()
    ):
        raise RuntimeError("startup exchange reconciliation authority is missing")
    resolved = path.resolve(strict=True)
    metadata = resolved.stat()
    if metadata.st_mode & 0o777 != 0o600:
        raise RuntimeError("startup exchange reconciliation inode drifted")
    before = resolved.read_bytes()
    expected_canonical_sha256 = os.environ.get(
        "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256", ""
    ).strip().lower()
    running_account_key_sha256 = _sha256_bytes(str(api_key).encode("utf-8"))
    if len(expected_canonical_sha256) != 64 or any(
        character not in "0123456789abcdef"
        for character in expected_canonical_sha256
    ):
        raise RuntimeError("startup exchange reconciliation binding is missing or drifted")
    try:
        payload = json.loads(before)
    except json.JSONDecodeError as exc:
        raise RuntimeError("startup exchange reconciliation is not JSON") from exc
    canonical = dict(payload) if isinstance(payload, dict) else {}
    expected_canonical = canonical.pop(
        "canonical_exchange_reconciliation_sha256", None
    )
    actual_canonical = _sha256_bytes(
        json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    )
    position_rows = payload.get("position_rows") if isinstance(payload, dict) else None
    if (
        payload.get("schema_version")
        != "narrowgate_stopped_exchange_reconciliation.v1"
        or payload.get("status")
        != "signed_open_orders_zero_exact_position_stable"
        or payload.get("symbol") != symbol
        or payload.get("open_order_count") != 0
        or payload.get("signed_endpoints")
        != ["/fapi/v1/openOrders", POSITION_RISK_RECONCILIATION_ENDPOINT]
        or expected_canonical != actual_canonical
        or expected_canonical != expected_canonical_sha256
        or payload.get("account_key_sha256") != running_account_key_sha256
        or not isinstance(position_rows, list)
        or len(position_rows) != 1
        or position_rows[0].get("position_side") != "BOTH"
    ):
        raise RuntimeError("startup exchange reconciliation authority drifted")
    orders = rest.get_orders(symbol=symbol)
    if orders != []:
        raise RuntimeError("exchange orders appeared after the stopped barrier")
    current_rows: list[dict[str, str | int]] = []
    for item in rest.get_position_risk(symbol=symbol) or []:
        if isinstance(item, Mapping) and str(item.get("symbol", "")) == symbol:
            current_rows.append(
                {
                    "symbol": symbol,
                    "position_side": str(item.get("positionSide", "BOTH")),
                    "position_amt": str(item.get("positionAmt", "")),
                    "entry_price": str(item.get("entryPrice", "")),
                    "update_time_ms": int(item.get("updateTime", 0) or 0),
                }
            )
    current_rows.sort(key=lambda row: str(row["position_side"]))
    if current_rows != position_rows or resolved.read_bytes() != before:
        raise RuntimeError("startup position differs from the stopped signed barrier")
    stopped_position = position_rows[0]
    try:
        stopped_qty = Decimal(str(stopped_position["position_amt"]))
        stopped_entry = Decimal(str(stopped_position["entry_price"]))
        stopped_update_time_ms = int(stopped_position["update_time_ms"])
        local_position = engine.inventory.snapshot
        local_qty = Decimal(str(local_position.qty))
        local_entry = Decimal(str(local_position.avg_entry_price))
        local_barrier = engine.inventory.reconciliation_snapshot()
        local_update_time_ms = int(local_barrier["snapshot_update_time_ms"])
    except (AttributeError, KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise RuntimeError(
            "startup local position reconciliation identity is unavailable"
        ) from exc
    if (
        not stopped_qty.is_finite()
        or not stopped_entry.is_finite()
        or not local_qty.is_finite()
        or not local_entry.is_finite()
        or local_qty != stopped_qty
        or local_entry != stopped_entry
        or local_update_time_ms != stopped_update_time_ms
    ):
        raise RuntimeError(
            "startup local inventory was not seeded from the stopped signed barrier"
        )
    logging.getLogger("main").info(
        "STARTUP_EXCHANGE_RECONCILIATION_LINEAGE canonical_sha256=%s",
        expected_canonical_sha256,
    )
    return {
        "path": str(resolved),
        "canonical_sha256": expected_canonical_sha256,
    }


def initialize_prospective_lifecycle_collection(
    *,
    cfg,
    engine: MakerEngine,
    rest,
    config_path: Path,
    native_runtime: dict,
    safety_authority: Mapping[str, Any],
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
    checkout = _git_snapshot()
    if (
        checkout["snapshot_internally_stable"] is not True
        or checkout["worktree_clean"] is not True
        or checkout["commit"] != safety_authority.get("execution_commit")
        or checkout["tree"] != safety_authority.get("execution_tree")
    ):
        raise RuntimeError("prospective epoch checkout differs from deployment release root")
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
        release_source={
            "commit": checkout["commit"],
            "tree": checkout["tree"],
            "release_root_sha256": safety_authority.get("canonical_sha256"),
            "worktree_clean": True,
        },
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
    safety_authority: Mapping[str, Any],
    dry_run: bool,
    exchange_reconciliation_required: bool = False,
    market_snapshot_client=None,
    listen_key_client=None,
) -> tuple[
    object | None,
    OrderLifecycleLiveWriterV2 | None,
    dict[str, Any] | None,
    int | None,
]:
    """Bind private recovery and collection before admitting market events."""

    engine.start()
    startup_open_orders = _initial_exchange_open_orders(rest, symbol=cfg.symbol)
    if startup_open_orders:
        raise RuntimeError("startup open-order ownership did not converge after cancel")
    logging.getLogger("main").info(
        "STARTUP_CANCEL_AND_OPEN_ORDERS_COMPLETE open_orders=0"
    )
    # Recover the durable fill checkpoint only after stale-order cancellation
    # is authoritatively complete.  No quote decision is admitted before this
    # function returns, so a recovered cooldown cannot race a new order.
    fill_gap_recovery = engine.reconcile_fill_cooldown_checkpoint_gap()
    logging.getLogger("main").info(
        "FILL_COOLDOWN_GAP_RECONCILIATION phase=post_cancel mode=%s recovered=%d",
        fill_gap_recovery["mode"],
        int(fill_gap_recovery["recovered_fill_count"]),
    )

    # Install the startup position seed after both cancellation and fill-gap
    # recovery.  Seeding before this point could hide a fill in the crash gap.
    engine.sync_position(required=True)
    admitted_user_stream_generation: int | None = None
    epoch = None
    writer = None
    exchange_binding = None
    if not dry_run:
        # The private stream is needed to close the accountTrades recovery
        # interval.  Public market streams must remain stopped until the
        # prospective epoch has captured its initial signal state and attached
        # the lifecycle writer.  Otherwise a cross-market trade can create a
        # partial native aggregator between startup and the epoch boundary.
        ws.start_private_user_stream(
            rest,
            listen_key_client=listen_key_client,
        )
        ready_deadline = time.monotonic() + STARTUP_USER_STREAM_READY_TIMEOUT_S
        while True:
            remaining_s = ready_deadline - time.monotonic()
            if remaining_s <= 0.0 or not ws.wait_for_user_stream_ready(remaining_s):
                raise RuntimeError(
                    "private user stream did not become ready before quote admission"
                )
            # Serialize whole private callbacks across reconciliation, initial
            # state capture, epoch publication, and writer attachment.  A
            # callback waiting here has not incremented its cursor or mutated
            # economic state; after release it is delivered to the new writer.
            with ws.hold_user_event_callbacks():
                before = ws.user_event_safety_snapshot()
                before_generation = int(
                    before.get("user_stream_generation", 0) or 0
                )
                before_event_count = int(before.get("user_event_count", 0) or 0)
                if (
                    not bool(before.get("user_stream_connected"))
                    or before_generation <= 0
                ):
                    continue

                # Close the interval between durable checkpoint recovery and
                # private-stream readiness through exact accountTrades
                # reconciliation.  The normal fill dedupe applies each unseen
                # fill once; the separate cooldown-gap applier must not run a
                # second time here.
                engine.sync_position(required=True)
                if _initial_exchange_open_orders(rest, symbol=cfg.symbol):
                    raise RuntimeError(
                        "startup open-order ownership changed before quote admission"
                    )
                after = ws.user_event_safety_snapshot()
                if (
                    not bool(after.get("user_stream_connected"))
                    or int(after.get("user_stream_generation", 0) or 0)
                    != before_generation
                    or int(after.get("user_event_count", 0) or 0)
                    != before_event_count
                ):
                    logging.getLogger("main").warning(
                        "STARTUP_USER_STREAM_CHANGED beforeGeneration=%d "
                        "afterGeneration=%d beforeEvents=%d afterEvents=%d; "
                        "repeating exact recovery barrier",
                        before_generation,
                        int(after.get("user_stream_generation", 0) or 0),
                        before_event_count,
                        int(after.get("user_event_count", 0) or 0),
                    )
                    continue

                if exchange_reconciliation_required:
                    exchange_binding = validate_startup_exchange_reconciliation_lineage(
                        rest,
                        engine=engine,
                        symbol=cfg.symbol,
                        api_key=str(cfg.api.key),
                    )
                # The writer is attached before the callback lock is released.
                # Events already waiting in the WebSocket thread are therefore
                # prospective rows, never a torn piece of the initial state.
                epoch, writer = initialize_prospective_lifecycle_collection(
                    cfg=cfg,
                    engine=engine,
                    rest=rest,
                    config_path=config_path,
                    native_runtime=native_runtime,
                    safety_authority=safety_authority,
                )
                final_boundary_state = ws.user_event_safety_snapshot()
                if (
                    not bool(final_boundary_state.get("user_stream_connected"))
                    or int(
                        final_boundary_state.get("user_stream_generation", 0) or 0
                    )
                    != before_generation
                    or int(final_boundary_state.get("user_event_count", 0) or 0)
                    != before_event_count
                ):
                    raise RuntimeError(
                        "private user stream changed while publishing startup evidence"
                    )
                admitted_user_stream_generation = before_generation
            break
    else:
        logging.getLogger("main").info("[DRY-RUN] Skipping WebSocket connections")

        if exchange_reconciliation_required:
            exchange_binding = validate_startup_exchange_reconciliation_lineage(
                rest,
                engine=engine,
                symbol=cfg.symbol,
                api_key=str(cfg.api.key),
            )
        epoch, writer = initialize_prospective_lifecycle_collection(
            cfg=cfg,
            engine=engine,
            rest=rest,
            config_path=config_path,
            native_runtime=native_runtime,
            safety_authority=safety_authority,
        )
    if admitted_user_stream_generation is not None:
        pre_market_user_state = ws.user_event_safety_snapshot()
        if (
            not bool(pre_market_user_state.get("user_stream_connected"))
            or int(pre_market_user_state.get("user_stream_generation", 0) or 0)
            != admitted_user_stream_generation
        ):
            raise RuntimeError(
                "private user stream changed while finalizing startup evidence"
            )
        ws.start_public_market_streams(
            rest,
            market_snapshot_client=market_snapshot_client,
            expected_user_stream_generation=admitted_user_stream_generation,
        )
        final_user_state = ws.user_event_safety_snapshot()
        if (
            not bool(final_user_state.get("user_stream_connected"))
            or int(final_user_state.get("user_stream_generation", 0) or 0)
            != admitted_user_stream_generation
        ):
            raise RuntimeError(
                "private user stream changed while starting public market streams"
            )
        engine.signal.start_metrics_polling()
    return epoch, writer, exchange_binding, admitted_user_stream_generation


def _bind_position_risk_v2(client):
    """Bind the complete position snapshot endpoint to one query client."""

    def get_position_risk_v2(**kwargs):
        return client.sign_request(
            "GET", POSITION_RISK_RECONCILIATION_ENDPOINT, kwargs
        )

    client.get_position_risk = get_position_risk_v2
    return client


def create_rest_client(cfg, dry_run=False):
    """Create one compatibility/query client for one-shot owner commands."""
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
        timeout=float(cfg.api.timeout_s),
    )
    # V3 omits symbols with neither a position nor an open order.  The
    # reconciliation contract needs V2's explicit zero-position row and
    # exchange updateTime; an empty V3 response has neither.
    return _bind_position_risk_v2(client)


def create_rest_clients(cfg) -> BinanceUsdMRestClients:
    """Create the isolated persistent REST roles used by live runtime."""

    clients = create_binance_usdm_rest_clients(
        key=str(cfg.api.key),
        secret=str(cfg.api.secret),
        base_url=binance_usdm_rest_base_url(testnet=bool(cfg.api.testnet)),
        timeout_s=float(cfg.api.timeout_s),
    )
    _bind_position_risk_v2(clients.reconciliation)
    _bind_position_risk_v2(clients.reconciliation_worker)
    return clients


def create_websocket_order_ab_gateway(cfg):
    """Create, but do not connect, the explicit short-lived WS A/B gateway."""

    if str(cfg.api.order_transport) != "websocket_api_ab":
        return None
    settings = cfg.api.websocket_order_ab
    config = BinanceUsdMWebSocketOrderConfig(
        enabled=True,
        url=str(settings.url),
        connect_timeout_s=float(settings.connect_timeout_s),
        request_timeout_s=float(settings.request_timeout_s),
        recv_window_ms=int(settings.recv_window_ms),
        latency_sample_limit=int(settings.latency_sample_limit),
        max_runtime_s=float(settings.max_runtime_s),
    )
    return create_binance_usdm_websocket_order_gateway(
        key=str(cfg.api.key),
        secret=str(cfg.api.secret),
        config=config,
    )


def resolve_logging_paths(cfg):
    """Resolve relative logging paths against the project root."""
    for field in (
        "file",
        "fill_cooldown_checkpoint",
        "trade_log",
        "quote_log",
        "order_outcome_log",
        "order_gateway_receipt_log",
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


def runtime_health_state_path(cfg) -> Path:
    """Use the operational log directory so run.sh status can read one fact file."""

    log_file = Path(str(getattr(cfg.logging, "file", "") or "logs/maker.log"))
    if not log_file.is_absolute():
        log_file = ROOT / log_file
    return log_file.parent / "runtime_health.json"


def collect_runtime_safety_health(
    *,
    engine,
    ws,
    order_gateway=None,
    gc_pause_monitor: GcPauseMonitor | None = None,
    logging_runtime: _AsyncLoggingRuntime | None = None,
    now_monotonic_s: float | None = None,
) -> dict[str, object]:
    """Collect only general process/stream safety facts, never research state."""

    now_monotonic_s = (
        time.monotonic()
        if now_monotonic_s is None
        else float(now_monotonic_s)
    )
    quote = engine.runtime_safety_snapshot(now_monotonic_s=now_monotonic_s)
    user = ws.user_event_safety_snapshot(now_monotonic_s=now_monotonic_s)
    continuation = quote.get("replace_terminal_continuation", {})
    position_reconciliation = quote.get(
        "periodic_position_reconciliation",
        {},
    )
    evidence_health_reader = getattr(
        engine,
        "runtime_evidence_writer_health_snapshot",
        None,
    )
    evidence = (
        evidence_health_reader()
        if callable(evidence_health_reader)
        else {"enabled": False}
    )
    order_gateway_health = (
        order_gateway.health_snapshot()
        if order_gateway is not None
        else {"active_transport": "unknown", "websocket_api": {"enabled": False}}
    )
    gc_pauses = (
        gc_pause_monitor.snapshot()
        if gc_pause_monitor is not None
        else {
            "count": 0,
            "total_ns": 0,
            "max_ns": 0,
            "last_ns": 0,
            "generation_counts": (0, 0, 0),
            "pause_bucket_upper_ns": (),
            "pause_bucket_counts": (),
        }
    )
    logging_health = (
        logging_runtime.health_snapshot()
        if logging_runtime is not None
        else {"valid": True, "configured": False}
    )
    return {
        "schemaVersion": RUNTIME_HEALTH_SCHEMA,
        "recordedAtNs": time.time_ns(),
        "pid": os.getpid(),
        "quoteLoopRunning": bool(quote["quote_loop_running"]),
        "ownershipConflictLatched": bool(
            quote["ownership_conflict_latched"]
        ),
        "fatalRuntimeLatched": bool(quote["fatal_runtime_latched"]),
        "reconciliationRequired": bool(quote["reconciliation_required"]),
        "reconciliationPending": bool(
            quote.get("reconciliation_pending", False)
        ),
        "fatalReason": str(quote["fatal_runtime_reason"]),
        "lastTickAge": quote["last_tick_age_s"],
        "lastUserEventAge": user["last_user_event_age_s"],
        "userEventCount": int(user["user_event_count"]),
        "userStreamConnected": bool(user["user_stream_connected"]),
        "userStreamGeneration": int(user["user_stream_generation"]),
        "replaceTerminalContinuationArmCount": int(
            continuation.get("arm_count", 0)
        ),
        "replaceTerminalContinuationPublishCount": int(
            continuation.get("publish_count", 0)
        ),
        "replaceTerminalContinuationDecisionCount": int(
            continuation.get("decision_count", 0)
        ),
        "replaceTerminalContinuationDropCount": int(
            continuation.get("drop_count", 0)
        ),
        "replaceTerminalContinuationPendingCount": int(
            continuation.get("pending_count", 0)
        ),
        "replaceTerminalContinuationInFlightCount": int(
            continuation.get("in_flight_count", 0)
        ),
        "replaceTerminalContinuationBuyDecisionCount": int(
            continuation.get("buy_decision_count", 0)
        ),
        "replaceTerminalContinuationSellDecisionCount": int(
            continuation.get("sell_decision_count", 0)
        ),
        "replaceTerminalContinuationDecisionLatencySumNs": int(
            continuation.get("decision_latency_sum_ns", 0)
        ),
        "replaceTerminalContinuationDecisionLatencyMaxNs": int(
            continuation.get("decision_latency_max_ns", 0)
        ),
        "runtimeEvidenceWriterEnabled": bool(evidence.get("enabled", False)),
        "runtimeEvidenceWriterValid": bool(evidence.get("valid", True)),
        "runtimeEvidenceWriterQueueDepth": int(evidence.get("queue_depth", 0)),
        "runtimeEvidenceWriterQueueHighWatermark": int(
            evidence.get("queue_high_watermark", 0)
        ),
        "runtimeEvidenceWriterQueueFullCount": int(
            evidence.get("queue_full_count", 0)
        ),
        "runtimeEvidenceWriterUncommittedCount": int(
            evidence.get("uncommitted_count", 0)
        ),
        "runtimeEvidenceWriterErrorCount": int(evidence.get("error_count", 0)),
        "runtimeEvidenceWriterFatalError": str(evidence.get("fatal_error", "")),
        "gcPauseCount": int(gc_pauses["count"]),
        "gcPauseTotalNs": int(gc_pauses["total_ns"]),
        "gcPauseMaxNs": int(gc_pauses["max_ns"]),
        "gcPauseLastNs": int(gc_pauses["last_ns"]),
        "gcPauseGenerationCounts": list(gc_pauses["generation_counts"]),
        "gcPauseBucketUpperNs": list(gc_pauses["pause_bucket_upper_ns"]),
        "gcPauseBucketCounts": list(gc_pauses["pause_bucket_counts"]),
        "logging": logging_health,
        "positionReconciliation": dict(position_reconciliation),
        "orderGateway": order_gateway_health,
    }


def shutdown_requires_operator_reconciliation(
    final_safety: dict[str, object],
) -> bool:
    """Return whether shutdown must use the non-restarting uncertainty exit."""

    return bool(
        final_safety.get("reconciliation_required")
        or final_safety.get("reconciliation_pending")
        or final_safety.get("ownership_conflict_latched")
    )


def resolve_live_shutdown_exit(
    *,
    engine,
    fatal_error: BaseException | None,
    fatal_traceback,
    cleanup_errors: list[BaseException],
) -> int:
    """Resolve the process exit only after every shutdown component has run."""

    logger = logging.getLogger("main")
    final_safety = engine.runtime_safety_snapshot()
    if shutdown_requires_operator_reconciliation(final_safety):
        if cleanup_errors:
            _safe_runtime_log(
                logger,
                logging.CRITICAL,
                "Execution-state uncertainty also encountered %d cleanup error(s)",
                len(cleanup_errors),
            )
        _safe_runtime_log(
            logger,
            logging.CRITICAL,
            "Execution state is uncertain at shutdown; exiting %d for "
            "operator-gated reconciliation (reason=%s pending=%d)",
            EXECUTION_STATE_UNCERTAIN_EXIT_CODE,
            final_safety.get("fatal_runtime_reason", "unknown"),
            int(bool(final_safety.get("reconciliation_pending"))),
        )
        return EXECUTION_STATE_UNCERTAIN_EXIT_CODE

    if fatal_error is not None:
        if cleanup_errors:
            _safe_runtime_log(
                logger,
                logging.CRITICAL,
                "Fatal exit also encountered %d cleanup error(s)",
                len(cleanup_errors),
            )
        raise fatal_error.with_traceback(fatal_traceback)
    if cleanup_errors:
        raise RuntimeError(
            f"live shutdown failed with {len(cleanup_errors)} cleanup error(s)"
        ) from cleanup_errors[0]
    return 0


def _quiesce_callbacks_then_stop_engine(
    *,
    ws: Any,
    engine: Any,
    cleanup_errors: list[BaseException],
) -> bool:
    """Stop callback producers before any final economic reconciliation.

    A user-data callback that survives ``ws.stop()`` can still mutate the
    order ledger and enqueue evidence.  In that state it is unsafe to run the
    engine's final exact reconciliation or to close the WAL/evidence/network
    dependencies beneath the callback.  The only permitted action is the
    ledger-independent exchange cancel-all path, followed by an uncertainty
    exit that leaves those dependencies alive until process teardown.

    Return ``True`` only when callback quiescence was proven.  ``engine.stop``
    is attempted only in that case; its own failure is recorded but does not
    undo the callback-quiescence proof.
    """

    logger = logging.getLogger("main")
    try:
        engine.revoke_new_order_authority_for_shutdown()
    except BaseException as authority_error:
        cleanup_errors.append(authority_error)
        try:
            engine.latch_runtime_fatal(
                reason="NEW_ORDER_SHUTDOWN_BARRIER_FAILED",
                error=authority_error,
                reconciliation_required=True,
                defer_reconciliation=True,
            )
        except BaseException as latch_error:
            cleanup_errors.append(latch_error)
        try:
            logger.critical(
                "NEW_ORDER_SHUTDOWN_BARRIER_FAILED action=fail_closed",
                exc_info=(
                    type(authority_error),
                    authority_error,
                    authority_error.__traceback__,
                ),
            )
        except BaseException:
            pass
    try:
        ws.stop()
    except BaseException as ws_error:
        cleanup_errors.append(ws_error)
        try:
            engine.latch_runtime_fatal(
                reason="USER_CALLBACK_QUIESCENCE_FAILED",
                error=ws_error,
                reconciliation_required=True,
                defer_reconciliation=True,
            )
        except BaseException as latch_error:
            cleanup_errors.append(latch_error)
        # Do not trust the latch's logging/callback path to have reached its
        # built-in cancel attempt.  A second idempotent symbol cancel-all is
        # preferable to leaving exchange exposure after a partially executed
        # fatal latch.  This path must not read or mutate the local ledger.
        try:
            canceled = bool(engine._emergency_cancel_all_exchange_orders())
            if not canceled:
                cleanup_errors.append(
                    RuntimeError(
                        "unquiesced user callbacks and exchange cancel-all failed"
                    )
                )
        except BaseException as cancel_error:
            cleanup_errors.append(cancel_error)
        try:
            logger.critical(
                "USER_CALLBACK_QUIESCENCE_FAILED "
                "action=exchange_cancel_all_and_uncertainty_exit "
                "engineExactReconciliation=deferred dependencyClose=deferred",
                exc_info=(type(ws_error), ws_error, ws_error.__traceback__),
            )
        except BaseException:
            # Logging has its own fail-closed health path.  Never let a failed
            # alarm suppress the safety cancellation attempted above.
            pass
        return False

    try:
        engine.stop()
    except BaseException as engine_error:
        cleanup_errors.append(engine_error)
        _safe_runtime_log(
            logger,
            logging.CRITICAL,
            "Shutdown engine cleanup failed: %s",
            engine_error,
            exc_info=True,
        )
    return True


def write_runtime_safety_health(cfg, payload: dict[str, object]) -> Path:
    """Atomically publish the latest operational health snapshot with mode 0600."""

    path = runtime_health_state_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise ValueError(f"runtime health path must not be a symlink: {path}")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return path


def runtime_safety_health_payload_factory(
    *,
    engine,
    ws,
    order_gateway=None,
    gc_pause_monitor: GcPauseMonitor | None = None,
    logging_runtime: _AsyncLoggingRuntime | None = None,
):
    """Build one worker-side collector without reading drifting loop locals."""

    def collect() -> dict[str, object]:
        return collect_runtime_safety_health(
            engine=engine,
            ws=ws,
            order_gateway=order_gateway,
            gc_pause_monitor=gc_pause_monitor,
            logging_runtime=logging_runtime,
        )

    return collect


def _close_runtime_evidence_writer_with_final_health(
    *,
    cfg,
    runtime_evidence_writer: RuntimeEvidenceWriter,
    engine,
    ws,
    order_gateway,
    gc_pause_monitor: GcPauseMonitor | None,
    logging_runtime: _AsyncLoggingRuntime | None,
    cleanup_errors: list[BaseException],
) -> dict[str, object] | None:
    """Close evidence and preserve truthful final health after worker failure.

    The ordinary final snapshot stays in the single FIFO while that writer is
    healthy. If its worker has already failed, no new item can be admitted and
    the last periodic health file would otherwise continue to look healthy.
    Once ``close`` proves the worker is no longer alive, publish one atomic
    snapshot directly from the live safety sources. This fallback does not
    repair or validate the failed evidence collection; the payload exposes the
    writer's actual invalid/fatal state.
    """

    logger = logging.getLogger("main")
    fallback_required = False
    initial_health = runtime_evidence_writer.health_snapshot()
    if (
        bool(initial_health.get("valid", False))
        and bool(initial_health.get("accepting", False))
        and bool(initial_health.get("worker_alive", False))
    ):
        try:
            runtime_evidence_writer.enqueue_json_snapshot_factory(
                runtime_health_state_path(cfg),
                runtime_safety_health_payload_factory(
                    engine=engine,
                    ws=ws,
                    order_gateway=order_gateway,
                    gc_pause_monitor=gc_pause_monitor,
                    logging_runtime=logging_runtime,
                ),
            )
        except BaseException as exc:
            fallback_required = True
            cleanup_errors.append(exc)
            _safe_runtime_log(
                logger,
                logging.CRITICAL,
                "Final runtime health publication failed: %s",
                exc,
                exc_info=True,
            )
    else:
        fallback_required = True
        unavailable_error = RuntimeError(
            "runtime evidence FIFO was unavailable before final health: "
            f"{initial_health.get('fatal_error') or 'writer_not_accepting'}"
        )
        cleanup_errors.append(unavailable_error)
        _safe_runtime_log(
            logger,
            logging.CRITICAL,
            "Final runtime health publication failed: evidence FIFO invalid: %s",
            unavailable_error,
        )

    evidence_health: dict[str, object] | None = None
    try:
        evidence_health = runtime_evidence_writer.close(
            drain_timeout_s=10.0,
        )
        _safe_runtime_log(
            logger,
            logging.INFO,
            "RUNTIME_EVIDENCE_WRITER_CLOSED rows=%d health=%d "
            "tasks=%d hwm=%d queueFull=%d errors=%d",
            int(evidence_health["csv_rows_committed"]),
            int(evidence_health["json_snapshots_committed"]),
            int(evidence_health["tasks_committed"]),
            int(evidence_health["queue_high_watermark"]),
            int(evidence_health["queue_full_count"]),
            int(evidence_health["error_count"]),
        )
    except BaseException as exc:
        fallback_required = True
        cleanup_errors.append(exc)
        _safe_runtime_log(
            logger,
            logging.CRITICAL,
            "Runtime evidence writer shutdown failed: %s",
            exc,
            exc_info=True,
        )

    if not fallback_required:
        return evidence_health

    final_writer_health = runtime_evidence_writer.health_snapshot()
    if bool(final_writer_health.get("worker_alive", False)):
        error = RuntimeError(
            "cannot publish direct final runtime health while the evidence "
            "writer worker remains alive"
        )
        cleanup_errors.append(error)
        _safe_runtime_log(logger, logging.CRITICAL, "%s", error)
        return evidence_health

    try:
        payload = collect_runtime_safety_health(
            engine=engine,
            ws=ws,
            order_gateway=order_gateway,
            gc_pause_monitor=gc_pause_monitor,
            logging_runtime=logging_runtime,
        )
        write_runtime_safety_health(cfg, payload)
        _safe_runtime_log(
            logger,
            logging.CRITICAL,
            "FINAL_RUNTIME_HEALTH_DIRECT_FALLBACK "
            "writerValid=%d fatalRuntimeLatched=%d",
            int(bool(payload["runtimeEvidenceWriterValid"])),
            int(bool(payload["fatalRuntimeLatched"])),
        )
    except BaseException as exc:
        cleanup_errors.append(exc)
        _safe_runtime_log(
            logger,
            logging.CRITICAL,
            "Final runtime health publication failed: direct fallback: %s",
            exc,
            exc_info=True,
        )
    return evidence_health


def _runtime_age_text(value: object) -> str:
    return "unknown" if value is None else f"{float(value):.1f}s"


def _disabled_deep_book_health() -> tuple[dict[str, object], dict[str, object]]:
    """Return the stable HEALTH shape without touching disabled book state."""

    return (
        {
            "enabled": 0,
            "valid": 0,
            "stale": 1,
            "age_ms": float("inf"),
            "generation": 0,
            "last_update_id": 0,
            "bid_levels": 0,
            "ask_levels": 0,
            "gap_count": 0,
            "resync_count": 0,
            "stale_restart_count": 0,
            "buffer_events": 0,
            "trade_count": 0,
        },
        {
            "tracked": 0,
            "retained": 0,
            "valid": 0,
            "ambiguous": 0,
            "uncovered": 0,
            "max_age_ms": 0.0,
        },
    )


def maintain_optional_deep_book(ws, *, now_ns: int) -> bool:
    """Maintain deep-book state only when the configured feature is active."""

    enabled = bool(getattr(ws.cfg.websocket, "deep_book_enabled", False))
    if enabled:
        ws.maintain_deep_book(now_ns=now_ns)
        ws.maintain_active_order_depth_paths(now_ns=now_ns)
    return enabled


def optional_deep_book_health(
    ws,
) -> tuple[dict[str, object], dict[str, object]]:
    """Collect deep-book HEALTH without taking disabled feature locks."""

    if bool(getattr(ws.cfg.websocket, "deep_book_enabled", False)):
        return ws.deep_book_snapshot(), ws.active_order_depth_snapshot()
    return _disabled_deep_book_health()


def _note_startup_cleanup_errors(
    primary_error: BaseException,
    cleanup_errors: Sequence[tuple[str, BaseException]],
) -> None:
    """Preserve cleanup failures without hiding the startup root cause."""

    add_note = getattr(primary_error, "add_note", None)
    if not callable(add_note):  # pragma: no cover - Python >=3.11 in production
        return
    for component_name, cleanup_error in cleanup_errors:
        add_note(
            "startup cleanup failed for "
            f"{component_name}: {type(cleanup_error).__name__}: {cleanup_error}"
        )


def _close_unstarted_engine_resources(engine: Any) -> None:
    """Close constructor-owned resources without issuing exchange writes."""

    failures: list[tuple[str, BaseException]] = []
    signal_engine = getattr(engine, "signal", None)
    stop_signal = getattr(signal_engine, "stop", None)
    if callable(stop_signal):
        try:
            stop_signal()
        except BaseException as exc:
            failures.append(("signal", exc))
    close_checkpoint = getattr(engine, "close_fill_cooldown_checkpoint_store", None)
    if callable(close_checkpoint):
        try:
            close_checkpoint()
        except BaseException as exc:
            failures.append(("fill-cooldown-checkpoint", exc))
    exact_runtime = getattr(engine, "_exact_opportunity_tape_runtime", None)
    if exact_runtime is not None:
        try:
            exact_runtime.close()
            engine._exact_opportunity_tape_runtime = None
        except BaseException as exc:
            failures.append(("exact-opportunity-writer", exc))
    if failures:
        error = RuntimeError(
            f"{len(failures)} unstarted engine resource(s) failed to close"
        )
        _note_startup_cleanup_errors(error, failures)
        raise error from failures[0][1]


def _cleanup_failed_live_startup(
    *,
    logging_runtime: _AsyncLoggingRuntime | None,
    rest_clients: BinanceUsdMRestClients | None = None,
    websocket_order_gateway: BinanceUsdMWebSocketOrderGateway | None = None,
    order_gateway: BinanceUsdMOrderGateway | None = None,
    engine: MakerEngine | None = None,
    ws: WSHandler | None = None,
    runtime_evidence_writer: RuntimeEvidenceWriter | None = None,
) -> tuple[tuple[str, BaseException], ...]:
    """Best-effort reverse-order cleanup for pre-main-loop construction.

    No callback here may issue a cancel/new request: startup has not yet
    established an admitted exchange state.  The primary construction error
    remains authoritative; cleanup failures are returned as annotations.
    """

    callbacks: list[tuple[str, Any]] = []
    if runtime_evidence_writer is not None:
        callbacks.append(
            (
                "runtime-evidence-writer",
                lambda: runtime_evidence_writer.close(drain_timeout_s=1.0),
            )
        )
    if ws is not None:
        callbacks.append(("websocket-handler", ws.stop))
    if engine is not None:
        callbacks.append(
            (
                "unstarted-engine-resources",
                lambda: _close_unstarted_engine_resources(engine),
            )
        )
    if order_gateway is not None:
        callbacks.append(("order-gateway", order_gateway.close))
    elif websocket_order_gateway is not None:
        # If the composite constructor itself failed, it never took ownership
        # of the optional WebSocket transport.
        callbacks.append(("websocket-order-gateway", websocket_order_gateway.close))
    if rest_clients is not None:
        callbacks.append(("REST-roles", rest_clients.close))
    if logging_runtime is not None:
        callbacks.append(("logging", lambda: shutdown_logging(logging_runtime)))

    failures: list[tuple[str, BaseException]] = []
    for component_name, callback in callbacks:
        try:
            callback()
        except BaseException as exc:
            failures.append((component_name, exc))
    return tuple(failures)


def _create_live_runtime_components(
    *,
    cfg: Any,
    safety_authority: Mapping[str, Any],
    logging_runtime: _AsyncLoggingRuntime,
) -> tuple[
    BinanceUsdMRestClients,
    Any,
    BinanceUsdMWebSocketOrderGateway | None,
    BinanceUsdMOrderGateway,
    MakerEngine,
    _LiveMainLoopWakeup,
    WSHandler,
    RuntimeEvidenceWriter,
]:
    """Construct all live owners as one exception-safe startup transaction."""

    logger = logging.getLogger("main")
    rest_clients: BinanceUsdMRestClients | None = None
    websocket_order_gateway: BinanceUsdMWebSocketOrderGateway | None = None
    order_gateway: BinanceUsdMOrderGateway | None = None
    engine: MakerEngine | None = None
    ws: WSHandler | None = None
    runtime_evidence_writer: RuntimeEvidenceWriter | None = None
    try:
        rest_clients = create_rest_clients(cfg)
        reconciliation_client = rest_clients.reconciliation
        websocket_order_gateway = create_websocket_order_ab_gateway(cfg)
        order_gateway = BinanceUsdMOrderGateway(
            rest_order_client=rest_clients.order,
            rest_buy_order_client=rest_clients.order_buy,
            rest_sell_order_client=rest_clients.order_sell,
            rest_safety_order_client=rest_clients.order_safety,
            websocket_order_gateway=websocket_order_gateway,
            async_order_lanes_enabled=bool(cfg.api.async_order_lanes_enabled),
            cross_side_order_lanes_enabled=bool(
                cfg.api.cross_side_order_lanes_enabled
            ),
            async_order_lane_capacity=int(cfg.api.async_order_lane_capacity),
            async_order_lane_drain_timeout_s=float(
                cfg.api.async_order_lane_drain_timeout_s
            ),
            async_order_lane_max_runtime_s=float(
                cfg.api.async_order_lane_max_runtime_s
            ),
        )
        logger.info(
            "BINANCE_USDM_TRANSPORT roles=%s order=%s websocket_order_ab=%s",
            ",".join(rest_clients.identity()["roles"]),
            order_gateway.active_transport,
            "enabled" if websocket_order_gateway is not None else "disabled",
        )
        logger.info(
            "BINANCE_USDM_ORDER_LANES async=%s cross_side=%s capacity=%d",
            "enabled" if order_gateway.async_order_lanes_enabled else "disabled",
            (
                "enabled"
                if order_gateway.cross_side_order_lanes_enabled
                else "disabled"
            ),
            int(cfg.api.async_order_lane_capacity),
        )

        engine = MakerEngine(
            cfg,
            reconciliation_client,
            artifact_authority=safety_authority,
            order_gateway=order_gateway,
            reconciliation_client=reconciliation_client,
            background_reconciliation_client=rest_clients.reconciliation_worker,
            metrics_client=rest_clients.metrics,
        )
        main_loop_wakeup = _LiveMainLoopWakeup()
        engine.set_replace_terminal_continuation_wakeup(
            main_loop_wakeup.notify_replacement_terminal
        )
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

        ws = WSHandler(engine, cfg)
        engine.set_event_source(ws)
        runtime_evidence_writer = RuntimeEvidenceWriter()
        order_gateway.set_runtime_evidence_writer(
            runtime_evidence_writer,
            str(cfg.logging.order_gateway_receipt_log),
        )
        engine.set_runtime_evidence_writer(runtime_evidence_writer)
    except BaseException as primary_error:
        cleanup_errors = _cleanup_failed_live_startup(
            logging_runtime=logging_runtime,
            rest_clients=rest_clients,
            websocket_order_gateway=websocket_order_gateway,
            order_gateway=order_gateway,
            engine=engine,
            ws=ws,
            runtime_evidence_writer=runtime_evidence_writer,
        )
        _note_startup_cleanup_errors(primary_error, cleanup_errors)
        raise

    return (
        rest_clients,
        reconciliation_client,
        websocket_order_gateway,
        order_gateway,
        engine,
        main_loop_wakeup,
        ws,
        runtime_evidence_writer,
    )


def main():
    parser = argparse.ArgumentParser(description="NarrowGate Maker Engine")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--live", action="store_true", help="Use mainnet (override testnet config)")
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config/model contract locally, then exit",
    )
    mode.add_argument(
        "--write-stopped-reconciliation",
        type=Path,
        help="Write a signed stopped-exchange reconciliation to an absolute path",
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

    config_path = Path(args.config) if args.config else ROOT / "live" / "config.yaml"
    if args.dry_run:
        return run_formal_dry_run(
            config_path,
            timeout_s=args.dry_run_timeout_s,
        )

    # Load config only after the formal dry-run branch has exited.
    cfg = load_config(config_path)
    if args.write_stopped_reconciliation is not None:
        if not cfg.api.key or not cfg.api.secret:
            parser.error("stopped reconciliation requires API credentials")
        rest = create_rest_client(cfg)
        try:
            result = write_stopped_exchange_reconciliation(
                rest,
                symbol=cfg.symbol,
                api_key=str(cfg.api.key),
                output_path=args.write_stopped_reconciliation,
            )
        finally:
            session = getattr(rest, "session", None)
            close_session = getattr(session, "close", None)
            if callable(close_session):
                close_session()
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    resolved_config_path = config_path.expanduser().resolve()
    safety_authority = deployment_envelope_runtime_authority()
    running_config_sha256 = _sha256_bytes(resolved_config_path.read_bytes())
    expected_config_sha256 = safety_authority["config_file_sha256"]
    checkout = _git_snapshot()
    if (
        running_config_sha256 != expected_config_sha256
        or checkout["commit"] != safety_authority["execution_commit"]
        or checkout["tree"] != safety_authority["execution_tree"]
        or checkout["worktree_clean"] is not True
    ):
        raise RuntimeError("checkout/config differs from deployment authority")
    from strategy.model_contract import (
        resolve_model_authorization_manifest,
        validate_model_bundle,
    )

    model_dir = _configured_model_dir(cfg)
    if not (model_dir / "fill_prob_params.json").is_file():
        raise RuntimeError(f"PREFLIGHT: Missing fill_prob_params.json in {model_dir}")
    model_metadata = validate_model_bundle(
        model_dir,
        require_live_authorization=True,
        expected_symbol=cfg.symbol,
    )
    model_authorization_path = resolve_model_authorization_manifest(
        model_dir,
        model_metadata,
    )
    validate_live_artifact_authority(
        cfg,
        artifact_authority=safety_authority,
        model_authorization_path=model_authorization_path,
    )
    policy_admission = admit_runtime_policies(
        vars(cfg.strategy), deployment_authority=safety_authority
    )
    set_restart_only_config_sha256(running_config_sha256)

    if args.live:
        cfg.api.testnet = False

    resolve_logging_paths(cfg)
    # Setup logging
    logging_runtime = setup_logging(cfg)
    logger = logging.getLogger("main")
    try:
        logger.info("=" * 60)
        native_runtime = audit_native_runtime(
            logger, cfg=cfg, safety_authority=safety_authority
        )
        project_name = getattr(cfg, "project_name", "NarrowGate")
        logger.info(f"{project_name} Maker Engine Starting")
        logger.info(f"  Symbol:    {cfg.symbol}")
        logger.info(f"  Testnet:   {cfg.api.testnet}")
        logger.info("  Mode:      live")
        logger.info(f"  ML:        {cfg.ml.enabled}")
        logger.info(
            f"  γ={cfg.strategy.gamma} fallback_κ={cfg.strategy.kappa} "
            "(P3 κ_eff used when available)"
        )
        logger.info(f"  Order size: {cfg.strategy.order_size} BTC")
        logger.info(f"  Max inv:   {cfg.strategy.max_inventory} BTC")
        logger.info(f"  Requote:   {cfg.strategy.requote_interval}s")
        logger.info("=" * 60)
    except BaseException as primary_error:
        startup_cleanup_errors = _cleanup_failed_live_startup(
            logging_runtime=logging_runtime,
        )
        _note_startup_cleanup_errors(primary_error, startup_cleanup_errors)
        raise

    # Validate API keys
    if not cfg.api.key or not cfg.api.secret:
        try:
            logger.error(
                "API key/secret not set. Use env vars BINANCE_API_KEY / "
                "BINANCE_API_SECRET, or set in config.yaml. On the live host, "
                "start with ./live/run.sh start|restart so live/.env is sourced."
            )
        except BaseException as primary_error:
            startup_cleanup_errors = _cleanup_failed_live_startup(
                logging_runtime=logging_runtime,
            )
            _note_startup_cleanup_errors(primary_error, startup_cleanup_errors)
            raise
        shutdown_logging(logging_runtime)
        return 1

    # Construct every session/thread/writer under one transaction.  The
    # helper closes all earlier owners if any later constructor or attachment
    # fails, before an exchange-admitted startup state exists.
    (
        rest_clients,
        rest,
        websocket_order_gateway,
        order_gateway,
        engine,
        main_loop_wakeup,
        ws,
        runtime_evidence_writer,
    ) = _create_live_runtime_components(
        cfg=cfg,
        safety_authority=safety_authority,
        logging_runtime=logging_runtime,
    )

    # Graceful shutdown
    shutdown_signal = _ShutdownSignalFlag()
    order_gateway_experiment_timer: threading.Timer | None = None

    fatal_error: BaseException | None = None
    fatal_traceback = None
    cleanup_errors: list[BaseException] = []
    gc_pause_monitor = GcPauseMonitor()
    try:
        signal.signal(signal.SIGINT, shutdown_signal)
        signal.signal(signal.SIGTERM, shutdown_signal)

        # Install config hot-reload (SIGHUP)
        set_engine_ref(engine)
        install_reload_handler()
        gc_pause_monitor.install()
    except BaseException as primary_error:
        try:
            gc_pause_monitor.close()
        except BaseException as cleanup_error:
            _note_startup_cleanup_errors(
                primary_error,
                (("GC-pause-monitor", cleanup_error),),
            )
        startup_cleanup_errors = _cleanup_failed_live_startup(
            logging_runtime=logging_runtime,
            rest_clients=rest_clients,
            websocket_order_gateway=websocket_order_gateway,
            order_gateway=order_gateway,
            engine=engine,
            ws=ws,
            runtime_evidence_writer=runtime_evidence_writer,
        )
        _note_startup_cleanup_errors(primary_error, startup_cleanup_errors)
        raise

    try:
        if not args.dry_run:
            # Preconnect inside the cleanup transaction and arm exactly one
            # timer at the earlier of the enabled experimental deadlines.
            def expire_order_gateway_experiment(
                limiting_arms: tuple[str, ...],
                runtime_s: float,
            ) -> None:
                if shutdown_signal.requested:
                    return
                shutdown_signal.requested = True
                main_loop_wakeup.notify_shutdown()
                logger.warning(
                    "BINANCE_USDM_ORDER_GATEWAY_EXPERIMENT_MAX_RUNTIME "
                    "limitingArms=%s elapsed_s=%.3f action=graceful_shutdown",
                    "|".join(limiting_arms),
                    runtime_s,
                )

            guard = arm_order_gateway_experiment_runtime_guard(
                websocket_gateway=websocket_order_gateway,
                websocket_max_runtime_s=float(
                    cfg.api.websocket_order_ab.max_runtime_s
                ),
                async_order_lanes_enabled=bool(
                    cfg.api.async_order_lanes_enabled
                ),
                async_order_lane_max_runtime_s=float(
                    cfg.api.async_order_lane_max_runtime_s
                ),
                async_order_lane_deadline_monotonic=(
                    order_gateway.async_order_lane_deadline_monotonic
                ),
                on_expire=expire_order_gateway_experiment,
            )
            if guard is not None:
                (
                    order_gateway_experiment_timer,
                    experiment_runtime_s,
                    limiting_arms,
                ) = guard
                logger.warning(
                    "BINANCE_USDM_ORDER_GATEWAY_EXPERIMENT_ARMED "
                    "websocket=%d async=%d crossSide=%d hardStopS=%.3f "
                    "limitingArms=%s",
                    int(websocket_order_gateway is not None),
                    int(bool(cfg.api.async_order_lanes_enabled)),
                    int(bool(cfg.api.cross_side_order_lanes_enabled)),
                    experiment_runtime_s,
                    "|".join(limiting_arms),
                )

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

                # Reuse the already authorized startup metadata, including
                # when inference is disabled and signal has loaded no trees.
                logger.info(
                    "Models: %d strict LightGBM heads validated in %s (active=%s)",
                    len(model_metadata),
                    model_dir,
                    cfg.ml.enabled,
                )

            except SystemExit:
                raise
            except Exception as e:
                logger.error(f"PREFLIGHT FAILED: {e}")
                raise

        # Warm up and cancel startup orders before publishing the epoch.  The
        # private stream closes the reconciliation gap under a callback
        # barrier; the writer is attached before public market events begin.
        (
            prospective_epoch,
            _,
            startup_exchange_reconciliation,
            admitted_user_stream_generation,
        ) = start_engine_with_prospective_collection(
            cfg=cfg,
            engine=engine,
            ws=ws,
            rest=rest,
            config_path=resolved_config_path,
            native_runtime=native_runtime,
            safety_authority=safety_authority,
            dry_run=bool(args.dry_run),
            exchange_reconciliation_required=True,
            market_snapshot_client=rest_clients.market_snapshot,
            listen_key_client=rest_clients.listen_key,
        )

        # Only publish the governing runtime identity after startup order
        # cancellation, checkpoint gap recovery, exact position sync, and the
        # private user stream have all converged.  No quote is admitted before
        # this point.
        runtime_identity_path, runtime_identity = record_startup_runtime_identity(
            cfg=cfg,
            config_path=resolved_config_path,
            native_runtime=native_runtime,
            policy_admission=policy_admission,
            safety_authority=safety_authority,
            dry_run=False,
            engine=engine,
            startup_exchange_reconciliation=startup_exchange_reconciliation,
        )
        logger.info(
            "RUNTIME_IDENTITY path=%s identity=%s",
            runtime_identity_path,
            json.dumps(runtime_identity, sort_keys=True),
        )
        for policy in policy_admission["approved_policies"]:
            logger.warning("RELEASE_POLICY_APPROVAL %s=ON", policy)
        if prospective_epoch is not None:
            logger.info(
                "PROSPECTIVE_BASELINE_EPOCH_BOUND id=%s identity=%s manifest=%s",
                prospective_epoch.epoch_id,
                prospective_epoch.identity_sha256,
                prospective_epoch.manifest_path,
            )

        if not args.dry_run and admitted_user_stream_generation is None:
            raise RuntimeError("startup did not admit a private user-stream generation")
        if admitted_user_stream_generation is not None:
            engine.set_admitted_user_stream_generation(
                admitted_user_stream_generation
            )

        # Main loop
        logger.info("Entering main loop...")
        sync_interval = 60  # sync position every 60s
        last_sync = time.time()
        health_interval = 60  # health check every 60s
        last_health = time.time()
        stale_interval = 30  # check stale orders every 30s
        last_stale = time.time()
        safety_state_interval = 1.0
        last_safety_state = 0.0

        while not shutdown_signal.requested:
            now = time.time()
            now_ns = time.time_ns()

            # The deep-book/action-path feature is restart-bound and normally
            # disabled.  Keep its locks and snapshot assembly completely off
            # the main decision loop unless this runtime explicitly enabled it.
            maintain_optional_deep_book(ws, now_ns=now_ns)

            # Engine tick (handles requote interval internally)
            logging_runtime.raise_if_failed()
            runtime_evidence_writer.raise_if_failed()
            # Cold P1 -> accountTrades -> P2 reads run on one worker.  Only
            # this main-loop poll may validate their captured ledger generation
            # and commit the resulting exact reconciliation barrier.
            engine.maintain_periodic_position_sync()
            engine.raise_if_runtime_fatal()
            logging_runtime.raise_if_failed()
            if engine.is_running:
                engine.tick()
            engine.raise_if_runtime_fatal()
            runtime_evidence_writer.raise_if_failed()
            logging_runtime.raise_if_failed()

            if now - last_safety_state >= safety_state_interval:
                runtime_evidence_writer.enqueue_json_snapshot_factory(
                    runtime_health_state_path(cfg),
                    runtime_safety_health_payload_factory(
                        engine=engine,
                        ws=ws,
                        order_gateway=order_gateway,
                        gc_pause_monitor=gc_pause_monitor,
                        logging_runtime=logging_runtime,
                    ),
                )
                last_safety_state = now

            # Periodic position sync
            if now - last_sync >= sync_interval:
                # REST sync 是 user stream 的审计兜底；发现差异后的硬降级由 MakerEngine 判定，
                # 主循环只负责固定节奏触发。网络读取在单飞 cold worker 中完成，
                # 账本提交仍由后续 main-loop poll 串行执行。
                engine.maintain_periodic_position_sync(request=True)
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
                def publish_periodic_health() -> None:
                    gateway_health = order_gateway.health_snapshot()
                    websocket_gateway_health = gateway_health.get("websocket_api", {})
                    if websocket_gateway_health.get("enabled"):
                        logger.info(
                            "ORDER_GATEWAY_HEALTH transport=%s connected=%d "
                            "generation=%d requests=%d successes=%d unknown=%d "
                            "p99Ms=%s lastClientOrderId=%s",
                            gateway_health.get("active_transport", "unknown"),
                            int(bool(websocket_gateway_health.get("connected"))),
                            int(websocket_gateway_health.get("connection_generation", 0)),
                            int(websocket_gateway_health.get("counters", {}).get("requests", 0)),
                            int(websocket_gateway_health.get("counters", {}).get("successes", 0)),
                            int(
                                websocket_gateway_health.get("counters", {}).get(
                                    "timeouts", 0
                                )
                                + websocket_gateway_health.get("counters", {}).get(
                                    "disconnects", 0
                                )
                                + websocket_gateway_health.get("counters", {}).get(
                                    "exchange_unknown", 0
                                )
                                + websocket_gateway_health.get("counters", {}).get(
                                    "protocol_errors", 0
                                )
                            ),
                            websocket_gateway_health.get("latency_ms", {}).get("p99"),
                            websocket_gateway_health.get("last_receipt", {}).get(
                                "client_order_id", ""
                            ),
                        )
                    snap = engine.inventory.snapshot
                    inv_exp = engine.inventory.inventory_exposure_snapshot()
                    camp = engine.inventory.campaign_snapshot()
                    buy_fill_sel = engine.buy_fill_selection_live_snapshot()
                    fill_hazard = engine.dynamic_fill_hazard_shadow_snapshot()
                    boolean_cooldown = engine.boolean_cooldown_policy_snapshot()
                    buy_e3_cooldown = engine.buy_e3_cooldown_policy_snapshot()
                    market_tape = ws.market_tape_snapshot()
                    deep_book, active_order_depth = optional_deep_book_health(ws)
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
                    runtime_safety = collect_runtime_safety_health(
                        engine=engine,
                        ws=ws,
                        order_gateway=order_gateway,
                        gc_pause_monitor=gc_pause_monitor,
                        logging_runtime=logging_runtime,
                    )
                    active_order_count = engine.orders.active_count()
                    requote_count = engine._requote_count
                    daily_pnl = engine.inventory.daily_pnl
                    logger.info(
                        f"HEALTH pos={snap.qty:+.4f} "
                        f"quoteLoopRunning={int(runtime_safety['quoteLoopRunning'])} "
                        f"ownershipConflictLatched={int(runtime_safety['ownershipConflictLatched'])} "
                        f"fatalRuntimeLatched={int(runtime_safety['fatalRuntimeLatched'])} "
                        f"reconciliationRequired={int(runtime_safety['reconciliationRequired'])} "
                        f"fatalReason={runtime_safety['fatalReason'] or 'none'} "
                        f"lastTickAge={_runtime_age_text(runtime_safety['lastTickAge'])} "
                        f"lastUserEventAge={_runtime_age_text(runtime_safety['lastUserEventAge'])} "
                        f"userStreamConnected={int(runtime_safety['userStreamConnected'])} "
                        f"userStreamGeneration={runtime_safety['userStreamGeneration']} "
                        f"rtcArm={runtime_safety['replaceTerminalContinuationArmCount']} "
                        f"rtcPublish={runtime_safety['replaceTerminalContinuationPublishCount']} "
                        f"rtcDecision={runtime_safety['replaceTerminalContinuationDecisionCount']} "
                        f"rtcDrop={runtime_safety['replaceTerminalContinuationDropCount']} "
                        f"rtcPending={runtime_safety['replaceTerminalContinuationPendingCount']} "
                        f"rtcInFlight={runtime_safety['replaceTerminalContinuationInFlightCount']} "
                        f"rtcBuy={runtime_safety['replaceTerminalContinuationBuyDecisionCount']} "
                        f"rtcSell={runtime_safety['replaceTerminalContinuationSellDecisionCount']} "
                        f"rtcDecisionLatencySumNs={runtime_safety['replaceTerminalContinuationDecisionLatencySumNs']} "
                        f"rtcDecisionLatencyMaxNs={runtime_safety['replaceTerminalContinuationDecisionLatencyMaxNs']} "
                        f"evidenceValid={int(runtime_safety['runtimeEvidenceWriterValid'])} "
                        f"evidenceDepth={runtime_safety['runtimeEvidenceWriterQueueDepth']} "
                        f"evidenceHwm={runtime_safety['runtimeEvidenceWriterQueueHighWatermark']} "
                        f"evidenceFull={runtime_safety['runtimeEvidenceWriterQueueFullCount']} "
                        f"evidenceUncommitted={runtime_safety['runtimeEvidenceWriterUncommittedCount']} "
                        f"evidenceErrors={runtime_safety['runtimeEvidenceWriterErrorCount']} "
                        f"rpnl={snap.realized_pnl:.2f} "
                        f"upnl={snap.unrealized_pnl:.2f} "
                        f"daily={daily_pnl:.2f} "
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
                        f"orders={active_order_count} "
                        f"requotes={requote_count}"
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
                runtime_evidence_writer.enqueue_task(
                    "periodic_health",
                    publish_periodic_health,
                )
                last_health = now

            # Keep the existing 100 ms safety/maintenance cadence, but let an
            # authoritative replacement terminal or shutdown end the wait.
            # Ordinary market callbacks never receive this wakeup handle and
            # therefore cannot turn the 5--10 second quote cadence into a
            # market-event-driven cadence.
            if shutdown_signal.requested:
                break
            main_loop_wakeup.wait()

    except BaseException as exc:
        fatal_error = exc
        fatal_traceback = exc.__traceback__
        if not isinstance(exc, (SystemExit, KeyboardInterrupt)):
            try:
                engine.latch_runtime_fatal(
                    reason="UNCAUGHT_LIVE_FATAL",
                    error=exc,
                    reconciliation_required=False,
                )
            except BaseException as latch_error:
                cleanup_errors.append(latch_error)
                _safe_runtime_log(
                    logger,
                    logging.CRITICAL,
                    "Runtime fatal latch raised during uncaught-error handling: %s",
                    latch_error,
                    exc_info=True,
                )
        _safe_runtime_log(
            logger,
            logging.CRITICAL,
            "Fatal error: %s",
            exc,
            exc_info=True,
        )
    finally:
        _safe_runtime_log(logger, logging.INFO, "Shutting down...")
        if order_gateway_experiment_timer is not None:
            order_gateway_experiment_timer.cancel()
        callbacks_quiesced = _quiesce_callbacks_then_stop_engine(
            ws=ws,
            engine=engine,
            cleanup_errors=cleanup_errors,
        )
        gateway_shutdown_complete = False
        if callbacks_quiesced:
            try:
                order_gateway.close()
                gateway_shutdown_complete = bool(
                    getattr(order_gateway, "shutdown_complete", True)
                )
                if not gateway_shutdown_complete:
                    raise RuntimeError(
                        "order gateway returned before its shutdown completed"
                    )
            except BaseException as exc:
                cleanup_errors.append(exc)
                _safe_runtime_log(
                    logger,
                    logging.CRITICAL,
                    "Shutdown order-gateway cleanup failed: %s",
                    exc,
                    exc_info=True,
                )
        else:
            _safe_runtime_log(
                logger,
                logging.CRITICAL,
                "USER_CALLBACK_DEPENDENCY_CLOSE_DEFERRED "
                "resources=order_gateway|REST_roles|fill_cooldown_WAL|"
                "runtime_evidence_writer action=process_fail_closed_exit",
            )

        if gateway_shutdown_complete:
            # Every asynchronous response callback has returned before its
            # network clients, checkpoint store, or evidence writer can close.
            # A gateway drain failure leaves these dependencies alive until
            # the already-fatal process exits instead of creating use-after-
            # close races in a late callback.
            for component_name, stop_component in (
                ("REST roles", rest_clients.close),
                (
                    "fill cooldown WAL",
                    engine.close_fill_cooldown_checkpoint_store,
                ),
            ):
                try:
                    stop_component()
                except BaseException as exc:
                    cleanup_errors.append(exc)
                    _safe_runtime_log(
                        logger,
                        logging.CRITICAL,
                        "Shutdown %s cleanup failed: %s",
                        component_name,
                        exc,
                        exc_info=True,
                    )
            _close_runtime_evidence_writer_with_final_health(
                cfg=cfg,
                runtime_evidence_writer=runtime_evidence_writer,
                engine=engine,
                ws=ws,
                order_gateway=order_gateway,
                gc_pause_monitor=gc_pause_monitor,
                logging_runtime=logging_runtime,
                cleanup_errors=cleanup_errors,
            )
        else:
            _safe_runtime_log(
                logger,
                logging.CRITICAL,
                "ORDER_GATEWAY_DEPENDENCY_CLOSE_DEFERRED "
                "resources=REST_roles|fill_cooldown_WAL|runtime_evidence_writer "
                "action=process_fail_closed_exit",
            )
        try:
            gc_pause_monitor.close()
        except BaseException as exc:
            cleanup_errors.append(exc)
            _safe_runtime_log(
                logger,
                logging.CRITICAL,
                "GC pause monitor cleanup failed: %s",
                exc,
                exc_info=True,
            )
        _safe_runtime_log(logger, logging.INFO, "Shutdown complete")

    try:
        return resolve_live_shutdown_exit(
            engine=engine,
            fatal_error=fatal_error,
            fatal_traceback=fatal_traceback,
            cleanup_errors=cleanup_errors,
        )
    finally:
        shutdown_logging(logging_runtime)


if __name__ == "__main__":
    raise SystemExit(main())
