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
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from live.config import install_reload_handler, load_config, set_engine_ref
from live.runtime_policy import (
    f05_boolean_cooldown_runtime_policy,
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
        raise argparse.ArgumentTypeError(
            "timeout must be finite and greater than zero"
        )
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
            raise ValueError(
                f"model bundle is missing fill_prob_params.json: {p3_path}"
            )

        summary.update(
            {
                "status": "passed",
                "exit_code": 0,
                "termination": "completed",
                "config": {
                    "path": str(resolved_config),
                    "sha256": hashlib.sha256(
                        resolved_config.read_bytes()
                    ).hexdigest(),
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
            "prospective epoch runtime code identity has missing files: "
            + ", ".join(missing)
        )
    return tuple(sorted(paths))


def audit_native_runtime(logger: logging.Logger) -> dict:
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
    if enabled["NARROWGATE_CPP_GLOBAL_FLOW"]:
        required.update({"NativeGlobalFlowEngine", "TradeBarAggregator"})

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
                    raise RuntimeError(
                        "narrowgate_cpp ABI missing TradeBarAggregator.update_batch"
                    )
        except Exception as exc:
            if enabled["NARROWGATE_CPP_STRICT"]:
                raise RuntimeError(
                    f"strict native profile {profile!r} failed preflight: {exc}"
                ) from exc
            logger.warning("Native runtime requested but unavailable: %s", exc)
            module_path = f"unavailable:{exc}"

    logger.info(
        "NATIVE_PROFILE name=%s quote_core=%d signal_features=%d "
        "global_flow=%d live_routing=%d strict=%d module=%s",
        profile,
        int(enabled["NARROWGATE_CPP_QUOTE_CORE"]),
        int(enabled["NARROWGATE_CPP_SIGNAL_FEATURES"]),
        int(enabled["NARROWGATE_CPP_GLOBAL_FLOW"]),
        int(enabled["NARROWGATE_CPP_LIVE_ROUTING"]),
        int(enabled["NARROWGATE_CPP_STRICT"]),
        module_path,
    )
    return {"profile": profile, "module": module_path, **enabled}


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
            str(log_path), maxBytes=10_000_000, backupCount=5)
        handlers.append(rotating)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )


def record_startup_runtime_identity(
    *,
    cfg,
    config_path: Path,
    native_runtime: dict,
    dry_run: bool,
) -> tuple[Path, dict]:
    """Persist and return the identity that actually governs this process."""
    resolved_config = config_path.expanduser().resolve()
    q90_policy = q90_action_runtime_policy(
        bool(cfg.strategy.dynamic_fill_hazard_action_enabled)
    )
    q90_policy_fields = {
        key: value
        for key, value in q90_policy.items()
        if key != "schema_version"
    }
    f05_policy = f05_boolean_cooldown_runtime_policy(
        bool(cfg.strategy.boolean_cooldown_policy_enabled),
        evidence_route=cfg.strategy.boolean_cooldown_evidence_route,
    )
    f05_policy_fields = {
        key: value
        for key, value in f05_policy.items()
        if key != "schema_version"
    }
    f05_buy_e3_policy = f05_buy_e3_runtime_policy(
        bool(cfg.strategy.buy_e3_cooldown_policy_enabled),
        evidence_route=cfg.strategy.buy_e3_cooldown_evidence_route,
    )
    f05_buy_e3_policy_fields = {
        key: value
        for key, value in f05_buy_e3_policy.items()
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
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
        "python_executable": sys.executable,
        "python_version": ".".join(map(str, sys.version_info[:3])),
        "config_path": str(resolved_config),
        "config_sha256": hashlib.sha256(resolved_config.read_bytes()).hexdigest(),
        "dry_run": bool(dry_run),
        "testnet": bool(cfg.api.testnet),
        "ml_enabled": bool(cfg.ml.enabled),
        "model_dir": str(model_dir.resolve()),
        "buy_fill_selection_live_enabled": bool(
            cfg.strategy.buy_fill_selection_live_enabled
        ),
        "buy_fill_selection_shadow_enabled": bool(
            cfg.strategy.buy_fill_selection_shadow_enabled
        ),
        "dynamic_fill_hazard_shadow_enabled": bool(
            cfg.strategy.dynamic_fill_hazard_shadow_enabled
        ),
        "order_lifecycle_journal_v2_enabled": bool(
            cfg.lifecycle_journal_v2.enabled
        ),
        "order_lifecycle_journal_v2_storage_profile": str(
            cfg.lifecycle_journal_v2.storage_profile
        ),
        "native_runtime": native_runtime,
        "q90_runtime_policy_schema_version": q90_policy["schema_version"],
        **q90_policy_fields,
        "f05_boolean_cooldown_runtime_policy_schema_version": f05_policy[
            "schema_version"
        ],
        **f05_policy_fields,
        "f05_boolean_cooldown_policy_sha256": str(
            cfg.strategy.boolean_cooldown_policy_sha256
        ).strip().lower(),
        "f05_boolean_cooldown_predicate_bundle_sha256": str(
            cfg.strategy.boolean_cooldown_predicate_bundle_sha256
        ).strip().lower(),
        "f05_boolean_cooldown_ema_warmup_s": float(
            cfg.strategy.boolean_cooldown_ema_warmup_s
        ),
        "f05_buy_e3_runtime_policy_schema_version": f05_buy_e3_policy[
            "schema_version"
        ],
        **f05_buy_e3_policy_fields,
        "f05_buy_e3_artifact_manifest_sha256": str(
            cfg.strategy.buy_e3_cooldown_artifact_manifest_sha256
        ).strip().lower(),
        "f05_buy_e3_artifact_sha256": str(
            cfg.strategy.buy_e3_cooldown_artifact_sha256
        ).strip().lower(),
        "f05_buy_e3_policy_sha256": str(
            cfg.strategy.buy_e3_cooldown_policy_sha256
        ).strip().lower(),
        "f05_buy_e3_predicate_bundle_sha256": str(
            cfg.strategy.buy_e3_cooldown_predicate_bundle_sha256
        ).strip().lower(),
        "f05_buy_e3_ema_warmup_s": float(
            cfg.strategy.buy_e3_cooldown_ema_warmup_s
        ),
    }
    write_runtime_identity(identity_path, identity)
    return identity_path, identity


def _initial_exchange_open_orders(rest, *, symbol: str) -> list[dict]:
    get_orders = getattr(rest, "get_orders", None)
    if not callable(get_orders):
        raise RuntimeError(
            "enabled lifecycle_journal_v2 requires a get_orders startup audit"
        )
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
        raise RuntimeError(
            "prospective epoch requires zero active local orders before collection"
        )
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
        raise RuntimeError(
            "startup open-order ownership did not converge after cancel"
        )
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

    base_url = ("https://testnet.binancefuture.com"
                if cfg.api.testnet
                else "https://fapi.binance.com")

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
    parser.add_argument("--live", action="store_true",
                        help="Use mainnet (override testnet config)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate config/model contract locally, then exit")
    parser.add_argument(
        "--dry-run-timeout-s",
        type=_positive_finite_seconds,
        default=DEFAULT_DRY_RUN_TIMEOUT_S,
        help=(
            "Formal dry-run deadline in seconds "
            f"(default: {DEFAULT_DRY_RUN_TIMEOUT_S:g})"
        ),
    )
    parser.add_argument("--config", type=str, default=None,
                        help="Path to config.yaml")
    args = parser.parse_args()

    if args.dry_run and args.live:
        parser.error("--dry-run and --live are mutually exclusive")

    config_path = (
        Path(args.config) if args.config else ROOT / "live" / "config.yaml"
    )
    if args.dry_run:
        return run_formal_dry_run(
            config_path,
            timeout_s=args.dry_run_timeout_s,
        )

    # Load config
    cfg = load_config(config_path)
    resolved_config_path = config_path.expanduser().resolve()

    if args.live:
        cfg.api.testnet = False

    resolve_logging_paths(cfg)

    # Setup logging
    setup_logging(cfg)
    logger = logging.getLogger("main")

    logger.info("=" * 60)
    native_runtime = audit_native_runtime(logger)
    runtime_identity_path, runtime_identity = record_startup_runtime_identity(
        cfg=cfg,
        config_path=resolved_config_path,
        native_runtime=native_runtime,
        dry_run=False,
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
    project_name = getattr(cfg, "project_name", "NarrowGate")
    logger.info(f"{project_name} Maker Engine Starting")
    logger.info(f"  Symbol:    {cfg.symbol}")
    logger.info(f"  Testnet:   {cfg.api.testnet}")
    logger.info("  Mode:      live")
    logger.info(f"  ML:        {cfg.ml.enabled}")
    logger.info(f"  γ={cfg.strategy.gamma} fallback_κ={cfg.strategy.kappa} (P3 κ_eff used when available)")
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
                rest.change_margin_type(
                    symbol=cfg.symbol, marginType="CROSSED"
                )
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
            dry_run=False,
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
                    logger.warning(f"STALE order {o.client_order_id} "
                                   f"stuck in PENDING_NEW for "
                                   f"{now - o.create_time:.0f}s, reconciling")
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
                external_trade_stale = sum(int(source.get("trade_stale", 1)) for source in external_sources)
                external_errors = sum(int(source.get("error_count", 0)) for source in external_sources)
                external_record_depth = sum(
                    int(source.get("record_queue_depth", 0)) for source in external_sources
                )
                external_record_hwm = max(
                    (int(source.get("record_queue_high_watermark", 0)) for source in external_sources),
                    default=0,
                )
                external_record_max_age_ms = max(
                    (float(source.get("record_max_queue_age_ms", 0.0)) for source in external_sources),
                    default=0.0,
                )
                external_record_dropped = sum(
                    int(source.get("record_dropped", 0)) for source in external_sources
                )
                external_source_ids = "|".join(
                    str(source.get("market_id", "unknown")) for source in external_sources
                ) or "none"
                request_rtts = [
                    float(source.get("request_rtt_ms", float("nan")))
                    for source in external_sources
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
                    (float(source.get("trade_age_ms", float("inf"))) for source in external_sources),
                    default=float("inf"),
                )
                external_book_event_age_ms = max(
                    (float(source.get("book_event_age_ms", float("inf"))) for source in external_sources),
                    default=float("inf"),
                )
                external_trade_event_age_ms = max(
                    (float(source.get("trade_event_age_ms", float("inf"))) for source in external_sources),
                    default=float("inf"),
                )
                try:
                    global_ref = engine.signal.global_reference_state(tick_size=cfg.tick_size)
                    global_ref_values = {
                        "valid": int(global_ref.valid),
                        "confidence": global_ref.confidence,
                        "spot": global_ref.global_spot_move_bps,
                        "perp": global_ref.global_perp_move_bps,
                        "divergence": global_ref.perp_spot_divergence_bps,
                        "residual": global_ref.residual_bps,
                        "fresh_spot": global_ref.fresh_spot_venues,
                        "fresh_perp": global_ref.fresh_perp_venues,
                        "dispersion": global_ref.cross_venue_dispersion_bps,
                        "basis_samples": global_ref.bridge_basis_sample_count,
                        "reason": global_ref.validity_reason,
                    }
                except Exception as exc:
                    logger.warning("Global reference shadow state failed: %s", exc)
                    global_ref_values = {
                        "valid": 0,
                        "confidence": 0.0,
                        "spot": float("nan"),
                        "perp": float("nan"),
                        "divergence": float("nan"),
                        "residual": float("nan"),
                        "fresh_spot": 0,
                        "fresh_perp": 0,
                        "dispersion": float("nan"),
                        "basis_samples": 0,
                        "reason": "error",
                    }
                try:
                    global_flow = engine.signal.global_flow_state()
                    global_flow_backend = engine.signal.global_flow_backend_snapshot()
                    flow_100 = global_flow.window(100)
                    flow_spot = flow_100.get("spot", {})
                    flow_perp = flow_100.get("perp", {})
                    global_flow_values = {
                        "valid": int(flow_100.get("valid", 0)),
                        "pressure": float(
                            flow_100.get("global_flow_pressure", float("nan"))
                        ),
                        "pending": float(
                            flow_100.get("global_minus_bridge_bps", float("nan"))
                        ),
                        "spot_pressure": float(
                            flow_spot.get("flow_pressure", float("nan"))
                        ),
                        "perp_pressure": float(
                            flow_perp.get("flow_pressure", float("nan"))
                        ),
                        "spot_agreement": float(flow_spot.get("venue_agreement", 0.0)),
                        "perp_agreement": float(flow_perp.get("venue_agreement", 0.0)),
                        "fresh_spot": int(flow_spot.get("fresh_venues", 0)),
                        "fresh_perp": int(flow_perp.get("fresh_venues", 0)),
                    }
                    global_flow_backend_values = {
                        "native": int(global_flow_backend.get("native", 0)),
                        "market_count": int(global_flow_backend.get("market_count", 0)),
                        "trade_batches": int(global_flow_backend.get("trade_batches", 0)),
                        "trade_events_seen": int(
                            global_flow_backend.get("trade_events_seen", 0)
                        ),
                        "trade_events_accepted": int(
                            global_flow_backend.get("trade_events_accepted", 0)
                        ),
                        "book_events_seen": int(
                            global_flow_backend.get("book_events_seen", 0)
                        ),
                        "out_of_order_events": int(
                            global_flow_backend.get("out_of_order_events", 0)
                        ),
                        "stale_trade_events": int(
                            global_flow_backend.get("stale_trade_events", 0)
                        ),
                        "trade_overflow_events": int(
                            global_flow_backend.get("trade_overflow_events", 0)
                        ),
                        "book_overflow_events": int(
                            global_flow_backend.get("book_overflow_events", 0)
                        ),
                    }
                except Exception as exc:
                    logger.warning("Global flow shadow state failed: %s", exc)
                    global_flow_values = {
                        "valid": 0,
                        "pressure": float("nan"),
                        "pending": float("nan"),
                        "spot_pressure": float("nan"),
                        "perp_pressure": float("nan"),
                        "spot_agreement": 0.0,
                        "perp_agreement": 0.0,
                        "fresh_spot": 0,
                        "fresh_perp": 0,
                    }
                    global_flow_backend_values = {
                        "native": 0,
                        "market_count": 0,
                        "trade_batches": 0,
                        "trade_events_seen": 0,
                        "trade_events_accepted": 0,
                        "book_events_seen": 0,
                        "out_of_order_events": 0,
                        "stale_trade_events": 0,
                        "trade_overflow_events": 0,
                        "book_overflow_events": 0,
                    }
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
                    f"globalFlowOOO={global_flow_backend_values['out_of_order_events']} "
                    f"globalFlowStaleTrades={global_flow_backend_values['stale_trade_events']} "
                    f"globalFlowTradeOverflow={global_flow_backend_values['trade_overflow_events']} "
                    f"globalFlowBookOverflow={global_flow_backend_values['book_overflow_events']} "
                    f"globalRefReason={global_ref_values['reason']} "
                    f"state={snap.state.name} "
                    f"orders={engine.orders.active_count()} "
                    f"requotes={engine._requote_count}"
                )
                lifecycle_v2_health = (
                    engine.order_lifecycle_live_writer_v2_health_snapshot()
                )
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
