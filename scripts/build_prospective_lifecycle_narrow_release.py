"""Build and validate the journal-v2 narrow successor of frozen remote v9.

The builder is local-only.  It never invokes SSH, changes a live file, or
deploys a payload.  Existing runtime files are reconstructed from exact remote
predecessor copies plus an allowlisted lifecycle transplant.  New modules are
copied only when their frozen local SHA256 matches this release recipe.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research.governance.public_machine_projection import (  # noqa: E402
    projection_for,
    source_document_path,
    source_identity_sha256,
)
from scripts.live_remote_pointer import active_live_remote_fields  # noqa: E402

SCHEMA_VERSION = "prospective_lifecycle_narrow_release.v1"
RELEASE_ID = "prospective_lifecycle_journal_v2_remote_v9_successor_20260805"
MANIFEST_NAME = "release_manifest.json"
FROZEN_V9_BASELINE_SHA256 = (
    "bfe835bf4b76fc675cd450eccf248cd1a3d179e2f9755425b40889f042c44638"
)
FROZEN_V9_CONFIG_SHA256 = (
    "889f605dc6a057874a8070fd86cbd21a0c8eb050156315c1dc6f48ec9acb48f5"
)

EPHEMERAL_ROOT = Path(
    os.environ.get("NARROWGATE_EPHEMERAL_ROOT", tempfile.gettempdir())
).expanduser()
_ACTIVE_REMOTE = active_live_remote_fields(REPO_ROOT)
DEFAULT_REMOTE_ROOT = os.environ.get(
    "NARROWGATE_REMOTE_ROOT",
    _ACTIVE_REMOTE.get("repo_root", str(Path.home() / REPO_ROOT.name)),
)
DEFAULT_REMOTE_FORMAL_ROOT = str(
    PurePosixPath(DEFAULT_REMOTE_ROOT) / "formal_collection"
)
REMOTE_SOURCE_DEFAULTS = {
    "live/main.py": EPHEMERAL_ROOT / "narrowgate_remote_main.py",
    "live/config.py": EPHEMERAL_ROOT / "narrowgate_remote_config.py",
    "strategy/maker_engine.py": EPHEMERAL_ROOT / "narrowgate_remote_maker_engine.py",
    "strategy/order_manager.py": Path(
        EPHEMERAL_ROOT / "narrowgate_remote_runtime_v9/order_manager.py"
    ),
    "live/ws_handler.py": EPHEMERAL_ROOT / "narrowgate_remote_runtime_v9/ws_handler.py",
    "live/config.yaml": EPHEMERAL_ROOT / "narrowgate_remote_runtime_v9/config.yaml",
}

EXPECTED_REMOTE_SHA256 = {
    "live/main.py": "f0f0ffe919f05df0fa17b2cc62b5d5815cb3ec4cf6b718d1efe011003e33bc6b",
    "live/config.py": "eb9a72d4bb2361ecf0f2617ee6fbc53329517d62d77ee381c85a6c59873fb8a7",
    "strategy/maker_engine.py": "9dcca8b0b92313758f2c35093516eafbc6940d5fec6fea028f190c6bc68cc23c",
    "strategy/order_manager.py": "fb246ac4ef64207be42b317688a0e1c3e7b13f586b0514718301a80fe2235db9",
    "live/ws_handler.py": "c76683bf7fab8d975d80d78b48230e7d50f2fa50605e31b42a1b03a5156a3fd3",
    "live/config.yaml": FROZEN_V9_CONFIG_SHA256,
}

# These broad working-tree files are never copied whole.  Only named lifecycle
# definitions are transplanted after their exact source identity is checked.
EXPECTED_TRANSPLANT_SOURCE_SHA256 = {
    "live/main.py": "f79cce825b722b1fbdb002b84d5be2c46a55a39b88d5e771bd2cb56301de4b0c",
    "live/config.py": "74013f6af666751ff5cd48db5f40de2a907745b4ed369e93e33e75cf28f05d60",
    "strategy/maker_engine.py": "8647110c1d0099498a063f3f2fb36f8634d88a2471b00ae81df54116c7dad3ce",
    "strategy/order_manager.py": "0f08be64cb394ab697dc1c620c454b685c19e7bd95a6001b02af93a6b1a09976",
}

NEW_FILE_SHA256 = {
    "execution/order_lifecycle.py": "2981b6154f8e7e5aaa2af6c8f2e2720877f7ad214b2a2692f31f8af291496d33",
    "execution/order_lifecycle_quantity_contract.py": "dcb37675dca018142a1e44ae207f9c4bdf3eda3a48426676d2069cf17ba04e52",
    "execution/order_lifecycle_journal_storage_v2.py": "291c7ddc08de26d15ea1ec839b3c5c5c31fe7a398cd20b3eaf8ac3dde99726f4",
    "execution/order_lifecycle_journal_v2.py": "0d87c9c4d5d7c8bf0535289c284a071a50627444773edffd862fa789b6b14451",
    "execution/order_lifecycle_journal_writer_v2.py": "7f648a1d56c1cacdab9c769e1882ed4ba1fe7c3bc0de40046c40dcc709c0b839",
    "execution/order_lifecycle_live_writer_v2.py": "b66c923bf1a2f76afbde62c91ac7b3bc45448b629563a2d549e5029cbe71d450",
    "execution/order_lifecycle_remote_spool_v2.py": "4efc4e5d4d400712b8cb26688822c38de02a3edb3f468c3e7914a090768ea369",
    "models/replay/baseline_epoch_manifest.py": "7393602838e9985ae4685a863d6d9b30496c640434abdc7349cdee46ad967429",
    "models/replay/prospective_baseline_epoch.py": "17ebd23e7d72695ba5501e169967a113d1e0ceac276f1b96c179776509d297ed",
    "scripts/lifecycle_journal_v2_collector.py": "f9f2a7435a56d3cda59273feb53803a56495f69e5369914f6eee2dfe4351a444",
    "research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260804_v9.json": FROZEN_V9_BASELINE_SHA256,
}

GENERATED_NEW_FILES = (
    "execution/prospective_lifecycle_state_capture_v1.py",
    "models/replay/__init__.py",
)

PATCH_EXISTING_FILES = tuple(EXPECTED_REMOTE_SHA256)
NEW_FILES = (*tuple(NEW_FILE_SHA256), *GENERATED_NEW_FILES)
REMOTE_ABSENCE_REQUIRED = tuple(
    path
    for path in NEW_FILES
    if path.startswith("execution/") or path.startswith("models/replay/")
)

PRESERVED_REMOTE_IDENTITIES = {
    "features/feature_dag.py": "68554be350b225f4ad6af02c4cf9ac4cadc9be92b7f4c7c547d8a10f7f27342f",
    "strategy/signal.py": "10eb1db8687cdf64b890b44f05e09bfe66d150277fcc0bef218a5ec0df0b3e5f",
    "strategy/inventory_manager.py": "cf60a38bd48e9e9327400833a18e02fdc91be910b7bbaf5470af34cb25551903",
}

REQUIRED_RUNTIME_EVIDENCE = (
    "remote_python_3_12_13_verified",
    "remote_pyarrow_24_0_0_verified",
    "native_extension_path_and_sha256_bound",
    "staged_overlay_import_smoke_passed",
    "targeted_lifecycle_tests_passed_on_remote_venv",
    "initial_state_13_domain_completeness_passed",
    "zero_pre_epoch_native_events",
    "one_hour_zero_drop_zero_error",
    "producer_enqueue_p99_le_100us",
    "producer_enqueue_max_le_1000us",
    "quote_loop_p99_regression_le_5pct",
    "writer_queue_hwm_le_2048",
    "writer_cpu_le_10pct_one_core",
    "writer_rss_delta_le_256mib",
    "writer_write_p99_le_250ms",
    "maker_thread_filesystem_calls_zero",
    "bounded_spool_admission_roundtrip_passed",
    "rollback_restart_rehearsed",
)

FORBIDDEN_PAYLOAD_PREFIXES = (
    "cpp/",
    "features/",
    "models/saved",
)
FORBIDDEN_PATCH_MARKERS = {
    "live/main.py": (
        "q90_action_runtime_policy",
        "record_startup_runtime_identity",
        "write_runtime_identity",
    ),
    "live/config.py": (
        "exact_opportunity_tape_enabled",
        "require_q90_action_restart",
        "q90_action_runtime_policy",
    ),
    "strategy/maker_engine.py": (
        "ExactOpportunityDailyWriter",
        "project_external_adverse_quote_edge",
        "_evaluate_dynamic_fill_hazard_prospective_recovery",
        "order_lifecycle_journal_payload",
    ),
    "live/ws_handler.py": ("terminal_active_order_depth_path",),
}

REQUIRED_SYMBOLS = {
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
    "execution/order_lifecycle.py": (
        "OrderLifecyclePhase",
        "TerminalPolicyRoute",
        "QuantityWeightedOrderLifecycle",
        "terminal_policy_route",
    ),
    "execution/prospective_lifecycle_state_capture_v1.py": (
        "capture_prospective_initial_runtime_state",
    ),
}

REMOTE_EVIDENCE_RELATIVE = (
    "research/shared/replay_lifecycle/docs/"
    "prospective_lifecycle_remote_v9_dependency_evidence_20260805.json"
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


def _require_hash(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"{label} must be a regular file: {path}")
    actual = _sha256(path)
    if actual != expected:
        raise ValueError(
            f"{label} SHA256 mismatch: expected={expected} actual={actual} path={path}"
        )


def _read_json(path: Path, label: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _replace_once(source: str, old: str, new: str, *, label: str) -> str:
    count = source.count(old)
    if count != 1:
        raise ValueError(f"{label} expected one anchor, found {count}")
    return source.replace(old, new, 1)


def _replace_count(
    source: str,
    old: str,
    new: str,
    *,
    count: int,
    label: str,
) -> str:
    observed = source.count(old)
    if observed != count:
        raise ValueError(f"{label} expected {count} anchors, found {observed}")
    return source.replace(old, new)


def _extract_definition(
    source: str,
    name: str,
    *,
    class_name: str | None = None,
) -> str:
    tree = ast.parse(source)
    nodes: Sequence[ast.stmt] = tree.body
    if class_name is not None:
        owner = next(
            (
                node
                for node in tree.body
                if isinstance(node, ast.ClassDef) and node.name == class_name
            ),
            None,
        )
        if owner is None:
            raise ValueError(f"missing transplant class {class_name}")
        nodes = owner.body
    matches = [
        node
        for node in nodes
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        and node.name == name
    ]
    if len(matches) != 1:
        raise ValueError(f"transplant definition {class_name or '<module>'}.{name} drifted")
    node = matches[0]
    lines = source.splitlines(keepends=True)
    if node.end_lineno is None:
        raise ValueError(f"transplant definition {name} lacks end_lineno")
    start_lineno = min(
        (decorator.lineno for decorator in node.decorator_list),
        default=node.lineno,
    )
    return "".join(lines[start_lineno - 1 : node.end_lineno]).rstrip() + "\n"


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


def _build_main(remote: str, local: str) -> str:
    imports = """from features.feature_dag import TEN_SECOND_CAUSAL_GRAPH
from execution.order_lifecycle_live_writer_v2 import OrderLifecycleLiveWriterV2
from models.replay.prospective_baseline_epoch import (
    live_clock_semantics_identity,
    publish_prospective_baseline_epoch,
    snapshot_action_enablement,
    snapshot_data_source_identity,
)
"""
    remote = _replace_once(
        remote,
        "from live.ws_handler import WSHandler\nfrom strategy.maker_engine import MakerEngine\n",
        "from live.ws_handler import WSHandler\n" + imports + "from strategy.maker_engine import MakerEngine\n",
        label="main lifecycle imports",
    )
    definitions = "".join(
        _extract_definition(local, name)
        for name in (
            "prospective_epoch_runtime_code_paths",
            "_initial_exchange_open_orders",
            "initialize_prospective_lifecycle_collection",
            "start_engine_with_prospective_collection",
        )
    )
    constants = """PROSPECTIVE_EPOCH_RUNTIME_CODE_ROOTS = (
    "live",
    "strategy",
    "execution",
    "features",
)
PROSPECTIVE_EPOCH_RUNTIME_CODE_FILES = (
    "market_fusion.py",
    "models/replay/baseline_epoch_manifest.py",
    "models/replay/prospective_baseline_epoch.py",
)

"""
    remote = _replace_once(
        remote,
        "def audit_native_runtime(logger: logging.Logger) -> dict:\n",
        constants + definitions + "\ndef audit_native_runtime(logger: logging.Logger) -> dict:\n",
        label="main lifecycle definitions",
    )
    remote = _replace_once(
        remote,
        "    config_path = Path(args.config) if args.config else None\n"
        "    cfg = load_config(config_path)\n",
        "    config_path = Path(args.config) if args.config else None\n"
        "    cfg = load_config(config_path)\n"
        "    resolved_config_path = (\n"
        "        config_path.expanduser().resolve()\n"
        "        if config_path is not None\n"
        "        else (ROOT / \"live\" / \"config.yaml\").resolve()\n"
        "    )\n",
        label="main resolved config",
    )
    remote = _replace_once(
        remote,
        "    audit_native_runtime(logger)\n",
        "    native_runtime = audit_native_runtime(logger)\n",
        label="main native identity",
    )
    old_start = """        # Start WebSocket
        if not args.dry_run:
            ws.start(rest)
        else:
            logger.info("[DRY-RUN] Skipping WebSocket connections")

        # Start engine
        engine.start()
"""
    new_start = """        # Establish a clean startup boundary and bind the writer before
        # the first WebSocket callback can enter the lifecycle risk set.
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
"""
    remote = _replace_once(remote, old_start, new_start, label="main startup ordering")
    old_reconcile = """                            engine.orders.reconcile_pending_cancel(
                                o.client_order_id,
                                exchange_open=exchange_order is not None,
                                exchange_oid=exchange_oid,
                            )
"""
    new_reconcile = """                            reconciled = engine.orders.reconcile_pending_cancel(
                                o.client_order_id,
                                exchange_open=exchange_order is not None,
                                exchange_oid=exchange_oid,
                            )
                            if reconciled and exchange_order is not None:
                                engine.record_reconciled_order_lifecycle(
                                    o.client_order_id,
                                    "cancel_rejected_reconciled",
                                )
"""
    return _replace_once(
        remote,
        old_reconcile,
        new_reconcile,
        label="main reconcile lifecycle",
    )


def _build_config(remote: str, local: str) -> str:
    remote = _replace_once(
        remote,
        "import yaml\n\nCONFIG_PATH = Path(__file__).parent / \"config.yaml\"\n",
        "import yaml\n\n"
        "from execution.order_lifecycle_journal_storage_v2 import (\n"
        "    LOCAL_ORICO_REPLAY_ADMISSION,\n"
        "    validate_lifecycle_journal_storage,\n"
        ")\n\n"
        "CONFIG_PATH = Path(__file__).parent / \"config.yaml\"\n",
        label="config lifecycle imports",
    )
    lifecycle_class = _extract_definition(local, "LifecycleJournalV2Config")
    remote = _replace_once(
        remote,
        "@dataclass\nclass PerfConfig:\n",
        lifecycle_class + "\n@dataclass\nclass PerfConfig:\n",
        label="config lifecycle dataclass",
    )
    remote = _replace_once(
        remote,
        "    logging: LogConfig = field(default_factory=LogConfig)\n"
        "    performance: PerfConfig = field(default_factory=PerfConfig)\n",
        "    logging: LogConfig = field(default_factory=LogConfig)\n"
        "    lifecycle_journal_v2: LifecycleJournalV2Config = field(\n"
        "        default_factory=LifecycleJournalV2Config\n"
        "    )\n"
        "    performance: PerfConfig = field(default_factory=PerfConfig)\n",
        label="config lifecycle field",
    )
    remote = _replace_once(
        remote,
        "        \"logging\",\n        \"performance\",\n",
        "        \"logging\",\n        \"lifecycle_journal_v2\",\n        \"performance\",\n",
        label="config lifecycle section key",
    )
    remote = _replace_once(
        remote,
        "        (\"logging\", LogConfig), (\"performance\", PerfConfig),\n",
        "        (\"logging\", LogConfig),\n"
        "        (\"lifecycle_journal_v2\", LifecycleJournalV2Config),\n"
        "        (\"performance\", PerfConfig),\n",
        label="config lifecycle parser",
    )
    reset_anchor = """    cfg.strategy.fill_cooldown_consecutive_reset_policy = (
        normalize_consecutive_reset_policy(
            cfg.strategy.fill_cooldown_consecutive_reset_policy,
            require_explicit=True,
        )
    )
"""
    lifecycle_validation = """    lifecycle_v2 = cfg.lifecycle_journal_v2
    if int(lifecycle_v2.queue_size) <= 0:
        raise ValueError("lifecycle_journal_v2.queue_size must be positive")
    if float(lifecycle_v2.heartbeat_interval_s) <= 0.0:
        raise ValueError("lifecycle_journal_v2.heartbeat_interval_s must be positive")
    if float(lifecycle_v2.shutdown_drain_timeout_s) < 0.0:
        raise ValueError(
            "lifecycle_journal_v2.shutdown_drain_timeout_s cannot be negative"
        )
    if str(lifecycle_v2.storage_format).strip().lower() not in {"parquet", "jsonl"}:
        raise ValueError(
            "lifecycle_journal_v2.storage_format must be parquet or jsonl"
        )
    if not math.isfinite(float(lifecycle_v2.remote_session_max_duration_s)) or not (
        60.0 <= float(lifecycle_v2.remote_session_max_duration_s) <= 86_400.0
    ):
        raise ValueError(
            "lifecycle_journal_v2.remote_session_max_duration_s must be in [60, 86400]"
        )
    if not (
        1024 * 1024
        <= int(lifecycle_v2.remote_session_max_bytes)
        <= 100 * 1024 * 1024 * 1024
    ):
        raise ValueError(
            "lifecycle_journal_v2.remote_session_max_bytes must be in [1 MiB, 100 GiB]"
        )
    validate_lifecycle_journal_storage(
        profile=lifecycle_v2.storage_profile,
        journal_root=lifecycle_v2.root,
        prospective_epoch_root=lifecycle_v2.prospective_epoch_root,
        required_mount=lifecycle_v2.required_mount,
        remote_spool_allowlisted_roots=lifecycle_v2.remote_spool_allowlisted_roots,
        enabled=bool(lifecycle_v2.enabled),
    )
    if bool(lifecycle_v2.enabled):
        if not str(lifecycle_v2.baseline_identity_path).strip():
            raise ValueError(
                "enabled lifecycle_journal_v2 requires baseline_identity_path"
            )
        baseline_sha = str(lifecycle_v2.baseline_identity_sha256).strip().lower()
        if len(baseline_sha) != 64 or any(
            char not in "0123456789abcdef" for char in baseline_sha
        ):
            raise ValueError(
                "enabled lifecycle_journal_v2 requires baseline_identity_sha256"
            )
"""
    remote = _replace_once(
        remote,
        reset_anchor,
        reset_anchor + lifecycle_validation,
        label="config lifecycle validation",
    )
    remote = _replace_once(
        remote,
        "def reload_config(*_args):\n"
        "    \"\"\"SIGHUP handler — reload config from disk and propagate to engine.\"\"\"\n"
        "    try:\n",
        "def reload_config(*_args):\n"
        "    \"\"\"SIGHUP handler — reload config from disk and propagate to engine.\"\"\"\n"
        "    global _cfg\n"
        "    with _lock:\n"
        "        previous_cfg = _cfg\n"
        "    try:\n",
        label="config reload snapshot",
    )
    return _replace_once(
        remote,
        "    except Exception as e:\n        logger.error(f\"Reload failed: {e}\")\n",
        "    except Exception as e:\n"
        "        with _lock:\n"
        "            _cfg = previous_cfg\n"
        "        logger.error(f\"Reload failed: {e}\")\n",
        label="config reload rollback",
    )


def _build_maker(remote: str, local: str) -> str:
    del local
    remote = _replace_once(
        remote,
        "from market_fusion import default_reference_symbol, normalize_symbol\n",
        "from market_fusion import default_reference_symbol, normalize_symbol\n"
        "from execution.prospective_lifecycle_state_capture_v1 import (\n"
        "    capture_prospective_initial_runtime_state,\n"
        ")\n",
        label="maker initial-state capture import",
    )
    remote = _replace_once(
        remote,
        "        self._order_context_lock = threading.RLock()\n",
        "        self._order_context_lock = threading.RLock()\n"
        "        self._order_lifecycle_live_writer_v2 = None\n"
        "        self._order_lifecycle_live_writer_v2_shutdown_timeout_s = 5.0\n",
        label="maker writer state",
    )
    remote = _replace_once(
        remote,
        "        self.orders = OrderManager(\n"
        "            on_fill=self._on_fill,\n"
        "            on_cancel=self._on_cancel,\n"
        "        )\n",
        "        self.orders = OrderManager(\n"
        "            on_fill=self._on_fill,\n"
        "            on_cancel=self._on_cancel,\n"
        "            on_lifecycle_event=self._on_order_lifecycle_event,\n"
        "        )\n",
        label="maker order manager callback",
    )
    methods = """    def set_order_lifecycle_live_writer_v2(
        self,
        writer,
        *,
        shutdown_drain_timeout_s: float,
    ) -> None:
        if self._order_lifecycle_live_writer_v2 is not None:
            raise RuntimeError("order lifecycle live writer v2 is already attached")
        self._order_lifecycle_live_writer_v2 = writer
        self._order_lifecycle_live_writer_v2_shutdown_timeout_s = max(
            0.0,
            float(shutdown_drain_timeout_s),
        )

    def order_lifecycle_live_writer_v2_health_snapshot(self) -> dict[str, Any]:
        runtime = self._order_lifecycle_live_writer_v2
        if runtime is None:
            return {"enabled": False, "state": "disabled"}
        return {"enabled": True, **runtime.health_snapshot()}

    def prospective_epoch_initial_runtime_state(
        self,
        *,
        account_snapshot: Optional[dict[str, Any]] = None,
        exchange_open_orders: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        return capture_prospective_initial_runtime_state(
            self,
            account_snapshot=account_snapshot,
            exchange_open_orders=exchange_open_orders,
        )

    def _on_order_lifecycle_event(
        self,
        order: Any,
        event_type: str,
        event: dict[str, Any],
    ) -> None:
        runtime = self._order_lifecycle_live_writer_v2
        if runtime is not None:
            runtime.enqueue_order_event(order, str(event_type), dict(event or {}))

    def record_reconciled_order_lifecycle(
        self,
        client_order_id: str,
        event_type: str,
    ) -> None:
        order = self.orders.get_order(str(client_order_id))
        lifecycle = getattr(order, "lifecycle", None) if order is not None else None
        events = lifecycle.events() if lifecycle is not None else ()
        if not events:
            return
        self._on_order_lifecycle_event(
            order,
            str(event_type),
            {"_local_receive_ts_ns": int(events[-1]["visibility_ts_ns"])},
        )

"""
    remote = _replace_once(
        remote,
        "    def on_config_reload(self, cfg):\n",
        methods + "    def on_config_reload(self, cfg):\n",
        label="maker lifecycle methods",
    )
    remote = _replace_once(
        remote,
        "    def on_config_reload(self, cfg):\n"
        "        \"\"\"Apply runtime config and propagate changes to signal + ws handler.\"\"\"\n"
        "        old_cfg = self.cfg\n",
        "    def on_config_reload(self, cfg):\n"
        "        \"\"\"Apply runtime config and propagate changes to signal + ws handler.\"\"\"\n"
        "        if cfg.lifecycle_journal_v2 != self.cfg.lifecycle_journal_v2:\n"
        "            raise ValueError(\n"
        "                \"lifecycle_journal_v2 configuration is restart-only and cannot be hot-reloaded\"\n"
        "            )\n"
        "        old_cfg = self.cfg\n",
        label="maker restart-only config",
    )
    rest_clock = """    @staticmethod
    def _rest_exchange_timestamp_ns(response: Any) -> int:
        if not isinstance(response, dict):
            return 0
        for field in ("transactTime", "updateTime", "workingTime"):
            value_ms = int(response.get(field, 0) or 0)
            if value_ms > 0:
                return value_ms * 1_000_000
        return 0

"""
    remote = _replace_once(
        remote,
        "    def _fmt_qty(self, qty: float) -> str:\n",
        rest_clock + "    def _fmt_qty(self, qty: float) -> str:\n",
        label="maker REST exchange clock",
    )
    remote = _replace_count(
        remote,
        "            self.orders.confirm_new(cid, oid)\n",
        "            self.orders.confirm_new(\n"
        "                cid,\n"
        "                oid,\n"
        "                exchange_ts_ns=self._rest_exchange_timestamp_ns(resp),\n"
        "            )\n",
        count=2,
        label="maker primary activation clocks",
    )
    remote = _replace_once(
        remote,
        "                self.orders.confirm_new(cid, resp.get(\"orderId\", 0))\n",
        "                self.orders.confirm_new(\n"
        "                    cid,\n"
        "                    resp.get(\"orderId\", 0),\n"
        "                    exchange_ts_ns=self._rest_exchange_timestamp_ns(resp),\n"
        "                )\n",
        label="maker emergency activation clock",
    )
    old_cancel_all = """        try:
            rest_start = time.perf_counter()
            try:
                self.rest.cancel_open_orders(symbol=self.cfg.symbol)
            finally:
                self._record_perf_rest_latency(
                    "cancel_all", (time.perf_counter() - rest_start) * 1_000_000.0
                )
            for o in active:
                self.orders.mark_pending_cancel(o.client_order_id)
            logger.debug(f"Canceled {len(active)} orders")
        except Exception as e:
            logger.error(f"Cancel all orders failed: {e}")
"""
    new_cancel_all = """        marked_ids = []
        for order in active:
            self.orders.mark_pending_cancel(order.client_order_id)
            marked_ids.append(order.client_order_id)
        try:
            rest_start = time.perf_counter()
            try:
                self.rest.cancel_open_orders(symbol=self.cfg.symbol)
            finally:
                self._record_perf_rest_latency(
                    "cancel_all", (time.perf_counter() - rest_start) * 1_000_000.0
                )
            logger.debug(f"Canceled {len(active)} orders")
        except Exception as e:
            for client_order_id in marked_ids:
                self.orders.cancel_rejected(client_order_id, str(e))
            logger.error(f"Cancel all orders failed: {e}")
"""
    remote = _replace_once(
        remote,
        old_cancel_all,
        new_cancel_all,
        label="maker cancel-all race",
    )
    remote = _replace_once(
        remote,
        "        except Exception as e:\n"
        "            logger.error(f\"Cancel order {cid} failed: {e}\")\n"
        "            return False\n",
        "        except Exception as e:\n"
        "            self.orders.cancel_rejected(cid, str(e))\n"
        "            logger.error(f\"Cancel order {cid} failed: {e}\")\n"
        "            return False\n",
        label="maker single cancel rejection",
    )
    remote = _replace_once(
        remote,
        "        self.orders.cancel_all_local()\n        logger.info(\"MakerEngine stopped\")\n",
        "        self.orders.cancel_all_local()\n"
        "        lifecycle_runtime = self._order_lifecycle_live_writer_v2\n"
        "        if lifecycle_runtime is not None:\n"
        "            health = lifecycle_runtime.close(\n"
        "                drain_timeout_s=(\n"
        "                    self._order_lifecycle_live_writer_v2_shutdown_timeout_s\n"
        "                )\n"
        "            )\n"
        "            logger.info(\n"
        "                \"ORDER_LIFECYCLE_JOURNAL_V2_CLOSED rows=%d drops=%d errors=%d valid=%d\",\n"
        "                int(health.get(\"rows_committed\", 0)),\n"
        "                int(health.get(\"drop_count\", 0)),\n"
        "                int(health.get(\"error_count\", 0)),\n"
        "                int(bool(health.get(\"formal_collection_valid\", False))),\n"
        "            )\n"
        "            self._order_lifecycle_live_writer_v2 = None\n"
        "        logger.info(\"MakerEngine stopped\")\n",
        label="maker writer shutdown",
    )
    return remote


def _build_state_capture(local_maker: str) -> str:
    helpers = _extract_definition(local_maker, "_prospective_state_plain")
    helpers += "\n" + _extract_definition(
        local_maker,
        "_prospective_state_fingerprint",
    )
    method = textwrap.dedent(
        _extract_definition(
            local_maker,
            "prospective_epoch_initial_runtime_state",
            class_name="MakerEngine",
        )
    )
    method = _replace_once(
        method,
        "def prospective_epoch_initial_runtime_state(\n",
        "def capture_prospective_initial_runtime_state(\n",
        label="state-capture exported function",
    )
    header = '''"""Fail-closed 13-domain state capture for a prospective live epoch.

Generated deterministically by the narrow-release builder from the reviewed
lifecycle-only method.  It is a new runtime file and does not modify quoting.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from features.feature_dag import TEN_SECOND_CAUSAL_GRAPH
from models.replay.prospective_baseline_epoch import (
    CPP_FEATURE_RECONSTRUCTION_CONTRACT,
    PROSPECTIVE_INITIAL_STATE_COMPLETENESS_SCHEMA_VERSION,
    PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS,
    PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS,
    PYTHON_FEATURE_STATE_CONTRACT,
)

'''
    return header + helpers + "\n" + method


def _build_models_replay_init() -> str:
    return '''"""Narrow lifecycle release package.

The production overlay intentionally avoids eager imports from research-only
replay modules. Runtime entry points import their required modules explicitly.
"""
'''


def _build_order_manager(remote: str, local: str) -> str:
    # The reviewed local successor contains only lifecycle-state additions.  It
    # is still treated as a patch and remains bound to the exact remote hash.
    del remote
    result = local
    result = _replace_once(
        result,
        "        with self._lock:\n"
        "            self._orders[cid] = order\n"
        "        logger.debug(f\"ORDER_CREATE {cid} {side.value} {quantity}@{price}\")\n",
        "        with self._lock:\n"
        "            self._orders[cid] = order\n"
        "        if self._on_lifecycle_event:\n"
        "            self._on_lifecycle_event(\n"
        "                order,\n"
        "                \"submit\",\n"
        "                {\"_local_receive_ts_ns\": now_ns},\n"
        "            )\n"
        "        logger.debug(f\"ORDER_CREATE {cid} {side.value} {quantity}@{price}\")\n",
        label="order manager submit callback",
    )
    old_pending = """        now_ns = time.time_ns()
        with self._lock:
            o = self._orders.get(cid)
            if o and o.state in (OrderState.OPEN, OrderState.PARTIALLY_FILLED):
                o.state = OrderState.PENDING_CANCEL
                o.update_time = now_ns / 1_000_000_000.0
                if o.lifecycle is not None:
                    o.lifecycle.request_cancel(now_ns)
"""
    new_pending = """        now_ns = time.time_ns()
        marked = None
        with self._lock:
            o = self._orders.get(cid)
            if o and o.state in (OrderState.OPEN, OrderState.PARTIALLY_FILLED):
                o.state = OrderState.PENDING_CANCEL
                o.update_time = now_ns / 1_000_000_000.0
                if o.lifecycle is not None:
                    o.lifecycle.request_cancel(now_ns)
                marked = o
        if marked is not None and self._on_lifecycle_event:
            self._on_lifecycle_event(
                marked,
                "cancel_request",
                {"_local_receive_ts_ns": now_ns},
            )
"""
    return _replace_once(
        result,
        old_pending,
        new_pending,
        label="order manager cancel-request callback",
    )


def _build_ws_handler(remote: str) -> str:
    remote = _replace_once(
        remote,
        "    def _on_user_message(self, _, message):\n"
        "        \"\"\"Route user data messages.\"\"\"\n"
        "        try:\n",
        "    def _on_user_message(self, _, message):\n"
        "        \"\"\"Route user data messages.\"\"\"\n"
        "        receive_ts_ns = time.time_ns()\n"
        "        try:\n",
        label="ws receive clock",
    )
    return _replace_once(
        remote,
        "            if event_type == \"ORDER_TRADE_UPDATE\":\n"
        "                order_data = data.get(\"o\", {})\n"
        "                self.engine.orders.on_order_update(order_data)\n",
        "            if event_type == \"ORDER_TRADE_UPDATE\":\n"
        "                order_data = data.get(\"o\", {})\n"
        "                order_data[\"_local_receive_ts_ns\"] = receive_ts_ns\n"
        "                order_data[\"_feature_ready_ts_ns\"] = time.time_ns()\n"
        "                self.engine.orders.on_order_update(order_data)\n",
        label="ws user event clocks",
    )


def _build_config_yaml(remote: str) -> str:
    raw = yaml.safe_load(remote)
    if not isinstance(raw, dict) or "lifecycle_journal_v2" in raw:
        raise ValueError("remote v9 config must be a mapping without lifecycle_journal_v2")
    suffix = f"""

# Prospective lifecycle collection only. Existing strategy/model/P3 values above
# are frozen byte-for-byte from operational baseline v9.
lifecycle_journal_v2:
  enabled: true
  storage_profile: "bounded_remote_spool"
  required_mount: "{DEFAULT_REMOTE_FORMAL_ROOT}"
  root: "{DEFAULT_REMOTE_FORMAL_ROOT}/order_lifecycle_journal_v2"
  prospective_epoch_root: "{DEFAULT_REMOTE_FORMAL_ROOT}/prospective_baseline_epochs"
  remote_spool_allowlisted_roots:
    - "{DEFAULT_REMOTE_FORMAL_ROOT}"
  remote_session_max_duration_s: 3600
  remote_session_max_bytes: 4294967296
  baseline_identity_path: "research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260804_v9.json"
  baseline_identity_sha256: "bfe835bf4b76fc675cd450eccf248cd1a3d179e2f9755425b40889f042c44638"
  storage_format: "parquet"
  queue_size: 8192
  heartbeat_interval_s: 5.0
  shutdown_drain_timeout_s: 5.0
"""
    candidate = remote.rstrip() + suffix
    parsed = yaml.safe_load(candidate)
    lifecycle = parsed.pop("lifecycle_journal_v2", None)
    if parsed != raw:
        raise AssertionError("candidate config changed an existing v9 parameter")
    if not isinstance(lifecycle, dict) or lifecycle.get("enabled") is not True:
        raise AssertionError("candidate lifecycle config is incomplete")
    return candidate


def _release_sources(repo_root: Path) -> dict[str, Path]:
    sources: dict[str, Path] = {}
    for logical_path in NEW_FILE_SHA256:
        public_path = repo_root / logical_path
        sources[logical_path] = source_document_path(
            public_path,
            require_private=projection_for(public_path) is not None,
        )
    return sources


def _verify_inputs(
    *,
    repo_root: Path,
    remote_sources: Mapping[str, Path],
) -> dict[str, Any]:
    if tuple(remote_sources) != PATCH_EXISTING_FILES:
        raise ValueError("remote source set must exactly match the release recipe")
    for logical_path, expected in EXPECTED_REMOTE_SHA256.items():
        _require_hash(Path(remote_sources[logical_path]), expected, f"remote {logical_path}")
    for logical_path, expected in EXPECTED_TRANSPLANT_SOURCE_SHA256.items():
        _require_hash(repo_root / logical_path, expected, f"transplant {logical_path}")
    for logical_path, expected in NEW_FILE_SHA256.items():
        public_path = repo_root / logical_path
        observed = source_identity_sha256(public_path)
        if observed != expected:
            raise ValueError(
                f"new file {logical_path} SHA256 mismatch: "
                f"expected {expected}, observed source identity {observed}"
            )
    evidence_path = repo_root / REMOTE_EVIDENCE_RELATIVE
    evidence = _read_json(evidence_path, "remote v9 dependency evidence")
    if evidence.get("audit_mode") != "read_only_no_deploy":
        raise ValueError("remote dependency evidence is not read-only")
    if evidence.get("config", {}).get("sha256") != FROZEN_V9_CONFIG_SHA256:
        raise ValueError("remote config evidence drifted")
    absent = set(map(str, evidence.get("new_files_absent_on_remote", ())))
    missing_absence = sorted(set(REMOTE_ABSENCE_REQUIRED) - absent)
    if missing_absence:
        raise ValueError(
            "remote new-file absence evidence is missing: " + ", ".join(missing_absence)
        )
    remote_hashes = evidence.get("files")
    if not isinstance(remote_hashes, dict):
        raise ValueError("remote dependency evidence lacks file hashes")
    for logical_path, expected in {
        **EXPECTED_REMOTE_SHA256,
        **PRESERVED_REMOTE_IDENTITIES,
    }.items():
        if logical_path == "live/config.yaml":
            continue
        observed = remote_hashes.get(logical_path)
        if observed is not None and observed != expected:
            raise ValueError(f"remote evidence drifted for {logical_path}")
    return evidence


def _write_file(path: Path, content: str | bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = content.encode("utf-8") if isinstance(content, str) else content
    with path.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    path.chmod(0o644)


def _file_record(path: Path, root: Path, mode: str) -> dict[str, Any]:
    return {
        "mode": mode,
        "path": path.relative_to(root).as_posix(),
        "sha256": _sha256(path),
        "size_bytes": path.stat().st_size,
    }


def _local_overlay_import_smoke(stage_root: Path, repo_root: Path) -> None:
    code = r'''import dataclasses
import importlib
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
import execution
import live
import models
import strategy

execution.__path__.insert(0, str(root / "execution"))
live.__path__.insert(0, str(root / "live"))
models.__path__.insert(0, str(root / "models"))
strategy.__path__.insert(0, str(root / "strategy"))
for name in (
    "execution.order_lifecycle",
    "execution.order_lifecycle_quantity_contract",
    "execution.order_lifecycle_journal_storage_v2",
    "execution.order_lifecycle_journal_v2",
    "execution.order_lifecycle_journal_writer_v2",
    "execution.order_lifecycle_live_writer_v2",
    "execution.prospective_lifecycle_state_capture_v1",
    "models.replay",
    "models.replay.baseline_epoch_manifest",
    "models.replay.prospective_baseline_epoch",
    "strategy.order_manager",
    "strategy.maker_engine",
    "live.config",
    "live.ws_handler",
    "live.main",
):
    sys.modules.pop(name, None)
for name in (
    "execution.order_lifecycle",
    "execution.order_lifecycle_live_writer_v2",
    "execution.prospective_lifecycle_state_capture_v1",
    "models.replay.prospective_baseline_epoch",
    "strategy.order_manager",
    "strategy.maker_engine",
    "live.config",
    "live.ws_handler",
    "live.main",
):
    importlib.import_module(name)
config_module = importlib.import_module("live.config")
if not dataclasses.is_dataclass(config_module.LifecycleJournalV2Config):
    raise SystemExit("staged LifecycleJournalV2Config is not a dataclass")
parsed = config_module._dataclass_from_dict(
    config_module.LifecycleJournalV2Config,
    {},
    path="lifecycle_journal_v2",
)
if not isinstance(parsed, config_module.LifecycleJournalV2Config):
    raise SystemExit("staged lifecycle config parser returned the wrong type")
'''
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    completed = subprocess.run(
        [sys.executable, "-c", code, str(stage_root)],
        cwd=repo_root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"local staged overlay import smoke failed: {detail}")


def _build_payload(
    *,
    repo_root: Path,
    stage_root: Path,
    remote_sources: Mapping[str, Path],
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    remote_text = {
        path: Path(source).read_text(encoding="utf-8")
        for path, source in remote_sources.items()
    }
    local_text = {
        path: (repo_root / path).read_text(encoding="utf-8")
        for path in EXPECTED_TRANSPLANT_SOURCE_SHA256
    }
    candidates = {
        "live/main.py": _build_main(
            remote_text["live/main.py"], local_text["live/main.py"]
        ),
        "live/config.py": _build_config(
            remote_text["live/config.py"], local_text["live/config.py"]
        ),
        "strategy/maker_engine.py": _build_maker(
            remote_text["strategy/maker_engine.py"],
            local_text["strategy/maker_engine.py"],
        ),
        "strategy/order_manager.py": _build_order_manager(
            remote_text["strategy/order_manager.py"],
            local_text["strategy/order_manager.py"],
        ),
        "live/ws_handler.py": _build_ws_handler(remote_text["live/ws_handler.py"]),
        "live/config.yaml": _build_config_yaml(remote_text["live/config.yaml"]),
    }
    records = []
    for logical_path in PATCH_EXISTING_FILES:
        output = stage_root / logical_path
        _write_file(output, candidates[logical_path])
        records.append(_file_record(output, stage_root, "patch_existing_exact_v9"))
    for logical_path, source in _release_sources(repo_root).items():
        output = stage_root / logical_path
        _write_file(output, source.read_bytes())
        records.append(_file_record(output, stage_root, "add_new"))
    state_capture_path = stage_root / "execution/prospective_lifecycle_state_capture_v1.py"
    _write_file(
        state_capture_path,
        _build_state_capture(local_text["strategy/maker_engine.py"]),
    )
    records.append(_file_record(state_capture_path, stage_root, "add_new"))
    replay_init_path = stage_root / "models/replay/__init__.py"
    _write_file(replay_init_path, _build_models_replay_init())
    records.append(_file_record(replay_init_path, stage_root, "add_new"))
    _local_overlay_import_smoke(stage_root, repo_root)
    original_config = yaml.safe_load(remote_text["live/config.yaml"])
    candidate_config = yaml.safe_load(candidates["live/config.yaml"])
    lifecycle_config = candidate_config.pop("lifecycle_journal_v2")
    if candidate_config != original_config:
        raise AssertionError("release config semantic preservation failed")
    baseline_public_path = (
        repo_root
        / "research/families/f10_live_replay_attribution/docs/"
        "operational_baseline_identity_20260804_v9.json"
    )
    baseline = _read_json(
        source_document_path(baseline_public_path, require_private=True),
        "frozen v9 baseline",
    )
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "build_mode": "deterministic_local_overlay_no_deploy_no_ssh",
        "baseline_identity_sha256": FROZEN_V9_BASELINE_SHA256,
        "predecessors": {
            path: {
                "sha256": EXPECTED_REMOTE_SHA256[path],
                "remote_path": str(PurePosixPath(DEFAULT_REMOTE_ROOT) / path),
            }
            for path in PATCH_EXISTING_FILES
        },
        "files": sorted(records, key=lambda row: row["path"]),
        "new_file_remote_absence": {
            path: True for path in REMOTE_ABSENCE_REQUIRED
        },
        "config_semantics": {
            "predecessor_sha256": FROZEN_V9_CONFIG_SHA256,
            "all_existing_fields_unchanged": True,
            "only_new_top_level_section": "lifecycle_journal_v2",
            "lifecycle_journal_v2": lifecycle_config,
        },
        "preserved_not_in_payload": {
            **PRESERVED_REMOTE_IDENTITIES,
            "cpp/narrowgate_cpp/streaming_features.cpp": baseline["runtime_code"][
                "cpp/narrowgate_cpp/streaming_features.cpp"
            ],
            "model.bundle_meta_sha256": baseline["model"]["bundle_meta_sha256"],
            "model.feature_dag_sha256": baseline["model"]["feature_dag_sha256"],
            "p3.sha256": baseline["p3"]["sha256"],
        },
        "transplant_contract": {
            "source_sha256": dict(EXPECTED_TRANSPLANT_SOURCE_SHA256),
            "whole_worktree_entrypoints_copied": False,
            "order_lifecycle_py_classification": "new_file",
            "strategy_or_model_parameters_changed": False,
        },
        "remote_source_evidence_schema": evidence.get("schema_version"),
        "local_overlay_import_smoke_passed": True,
        "deployment_authorized": False,
        "deployment_executed": False,
        "runtime_evidence_required": list(REQUIRED_RUNTIME_EVIDENCE),
        "blockers": [
            {
                "id": "runtime_evidence_not_yet_bound",
                "detail": list(REQUIRED_RUNTIME_EVIDENCE),
            }
        ],
    }
    manifest["manifest_sha256"] = _canonical_sha256(manifest)
    _write_file(
        stage_root / MANIFEST_NAME,
        json.dumps(
            manifest,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n",
    )
    return manifest


def validate_staging(candidate_root: Path) -> dict[str, Any]:
    root = candidate_root.expanduser().resolve(strict=True)
    if not root.is_dir() or root.is_symlink():
        raise ValueError("candidate root must be a non-symlink directory")
    manifest = _read_json(root / MANIFEST_NAME, "release manifest")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("release manifest schema mismatch")
    claimed_manifest_hash = str(manifest.pop("manifest_sha256", ""))
    actual_manifest_hash = _canonical_sha256(manifest)
    manifest["manifest_sha256"] = claimed_manifest_hash
    if claimed_manifest_hash != actual_manifest_hash:
        raise ValueError("release manifest canonical hash mismatch")
    expected_paths = {MANIFEST_NAME}
    records = manifest.get("files")
    if not isinstance(records, list):
        raise ValueError("release manifest files must be a list")
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("release manifest file record must be an object")
        logical_path = str(record.get("path", ""))
        expected_paths.add(logical_path)
        path = (root / logical_path).resolve(strict=True)
        if root not in path.parents or path.is_symlink() or not path.is_file():
            raise ValueError(f"release file escaped or is not regular: {logical_path}")
        if _sha256(path) != record.get("sha256"):
            raise ValueError(f"release file hash mismatch: {logical_path}")
        if logical_path.endswith(".py"):
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if any(logical_path.startswith(prefix) for prefix in FORBIDDEN_PAYLOAD_PREFIXES):
            raise ValueError(f"forbidden payload path: {logical_path}")
    actual_paths = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
    }
    if actual_paths != expected_paths:
        raise ValueError(
            "release payload file set drifted: "
            f"missing={sorted(expected_paths - actual_paths)} "
            f"extra={sorted(actual_paths - expected_paths)}"
        )
    record_by_path = {str(row["path"]): row for row in records}
    for logical_path in PATCH_EXISTING_FILES:
        if record_by_path.get(logical_path, {}).get("mode") != (
            "patch_existing_exact_v9"
        ):
            raise ValueError(f"patch-existing classification drifted: {logical_path}")
    for logical_path in NEW_FILES:
        if record_by_path.get(logical_path, {}).get("mode") != "add_new":
            raise ValueError(f"new-file classification drifted: {logical_path}")
    if manifest.get("new_file_remote_absence", {}).get(
        "execution/order_lifecycle.py"
    ) is not True:
        raise ValueError("execution/order_lifecycle.py must be classified as new")
    for logical_path, symbols in REQUIRED_SYMBOLS.items():
        defined = _defined_symbols(root / logical_path)
        missing = sorted(set(symbols) - defined)
        if missing:
            raise ValueError(f"required symbols missing in {logical_path}: {missing}")
    config_tree = ast.parse(
        (root / "live/config.py").read_text(encoding="utf-8"),
        filename=str(root / "live/config.py"),
    )
    lifecycle_node = next(
        (
            node
            for node in config_tree.body
            if isinstance(node, ast.ClassDef)
            and node.name == "LifecycleJournalV2Config"
        ),
        None,
    )
    if lifecycle_node is None or not any(
        isinstance(decorator, ast.Name) and decorator.id == "dataclass"
        for decorator in lifecycle_node.decorator_list
    ):
        raise ValueError("LifecycleJournalV2Config must retain its dataclass decorator")
    for logical_path, markers in FORBIDDEN_PATCH_MARKERS.items():
        text = (root / logical_path).read_text(encoding="utf-8")
        found = [marker for marker in markers if marker in text]
        if found:
            raise ValueError(f"unrelated markers in {logical_path}: {found}")
    original_config_path = REMOTE_SOURCE_DEFAULTS["live/config.yaml"]
    _require_hash(original_config_path, FROZEN_V9_CONFIG_SHA256, "remote config")
    original_config = yaml.safe_load(original_config_path.read_text(encoding="utf-8"))
    candidate_config = yaml.safe_load(
        (root / "live/config.yaml").read_text(encoding="utf-8")
    )
    lifecycle = candidate_config.pop("lifecycle_journal_v2", None)
    if candidate_config != original_config:
        raise ValueError("candidate config changed existing v9 semantics")
    if lifecycle != manifest["config_semantics"]["lifecycle_journal_v2"]:
        raise ValueError("candidate lifecycle config disagrees with manifest")
    if manifest.get("deployment_authorized") is not False:
        raise ValueError("narrow release must remain unauthorized before runtime gates")
    if manifest.get("deployment_executed") is not False:
        raise ValueError("narrow release manifest cannot claim deployment")
    if manifest.get("local_overlay_import_smoke_passed") is not True:
        raise ValueError("local staged overlay import smoke is not bound")
    return {
        "schema_version": "prospective_lifecycle_narrow_release_validation.v1",
        "release_id": manifest["release_id"],
        "manifest_sha256": claimed_manifest_hash,
        "file_count": len(records),
        "config_existing_fields_unchanged": True,
        "order_lifecycle_py_classification": "new_file",
        "payload_valid": True,
        "local_overlay_import_smoke_passed": True,
        "deployment_authorized": False,
        "runtime_evidence_remaining": list(REQUIRED_RUNTIME_EVIDENCE),
    }


def build_release(
    *,
    repo_root: Path,
    output_root: Path,
    remote_sources: Mapping[str, Path] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    repo = repo_root.expanduser().resolve(strict=True)
    expected_python = (repo / ".venv/bin/python").resolve(strict=True)
    if Path(sys.executable).resolve() != expected_python or sys.version_info < (3, 10):
        raise RuntimeError("builder must run with the repository .venv Python >=3.10")
    sources = dict(remote_sources or REMOTE_SOURCE_DEFAULTS)
    evidence = _verify_inputs(repo_root=repo, remote_sources=sources)
    output = output_root.expanduser().resolve()
    if output.exists():
        raise FileExistsError(f"release output already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.parent / f".{output.name}.partial-{os.getpid()}-{uuid.uuid4().hex}"
    temporary.mkdir()
    try:
        manifest = _build_payload(
            repo_root=repo,
            stage_root=temporary,
            remote_sources=sources,
            evidence=evidence,
        )
        validation = validate_staging(temporary)
        os.replace(temporary, output)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    final_validation = validate_staging(output)
    if validation != final_validation:
        raise AssertionError("release validation changed after atomic publication")
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
    validation = validate_staging(args.candidate_root)
    print(json.dumps(validation, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
