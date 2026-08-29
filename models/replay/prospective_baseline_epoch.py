"""Fully bound prospective baseline epochs for the next controlled live start.

This successor intentionally does not repair historical epochs.  It publishes
one new identity at a controlled process boundary and binds every lifecycle
clock input needed by future live/replay transport work.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any

from data_paths import resolve_portable_path
from execution.order_lifecycle_journal_storage_v2 import (
    BOUNDED_REMOTE_SPOOL,
    LOCAL_ORICO_REPLAY_ADMISSION,
    validate_remote_spool_path,
)
from models.replay.baseline_epoch_manifest import (
    REQUIRED_IDENTITY_FIELDS,
    canonical_sha256,
    epoch_identity_sha256,
)
from strategy.replay_controls import (
    LOSS_COOLDOWN_SEMANTICS,
    LOSS_COOLDOWN_SNAPSHOT_SCHEMA,
    LOSS_COOLDOWN_SNAPSHOT_STATE_FIELDS,
    ConsecutiveLossCooldown,
)

PROSPECTIVE_BASELINE_EPOCH_SCHEMA_VERSION = "narrowgate_prospective_baseline_epoch.v1"
PROSPECTIVE_BASELINE_INITIAL_STATE_SCHEMA_VERSION = (
    "narrowgate_prospective_baseline_initial_runtime_state.v2"
)
PROSPECTIVE_INITIAL_STATE_COMPLETENESS_SCHEMA_VERSION = (
    "narrowgate_prospective_initial_state_completeness.v1"
)
CPP_FEATURE_RECONSTRUCTION_CONTRACT = (
    "canonical_python_bar_and_feature_history.v1"
)
PYTHON_FEATURE_STATE_CONTRACT = "python_authoritative_bar_and_feature_history.v1"
PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS = (
    "account_and_exchange",
    "inventory_accounting",
    "campaign",
    "reward_path_loss_cooldown",
    "adverse_markout_pause",
    "sync_degrade",
    "defense_and_stale_guards",
    "fill_cooldown_lineage",
    "order_lifecycle",
    "q90_runtime",
    "post_fill_response",
    "quote_policy_clocks",
    "signal_feature_dag_warmup",
)
PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS = {
    domain: f"narrowgate_initial_state_{domain}.v1"
    for domain in PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS
}
PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS["reward_path_loss_cooldown"] = (
    LOSS_COOLDOWN_SNAPSHOT_SCHEMA
)
PROSPECTIVE_INITIAL_STATE_REQUIRED_FIELDS = {
    "account_and_exchange": ("account", "exchange_open_orders"),
    "inventory_accounting": (
        "quantity_btc",
        "average_entry_price",
        "cost_basis",
        "realized_pnl",
        "inventory_time_last_ts",
    ),
    "campaign": ("active", "campaign_id", "start_side", "fills"),
    "reward_path_loss_cooldown": (
        "semantics",
        *LOSS_COOLDOWN_SNAPSHOT_STATE_FIELDS,
        "cooldown_until_wall_s",
        "last_cooldown_cancel_time_wall_s",
    ),
    "adverse_markout_pause": ("ema_bid", "ema_ask", "pending", "pause_until_wall_s"),
    "sync_degrade": ("last_seen_sync_adjust_seq", "degrade_until_wall_s"),
    "defense_and_stale_guards": (
        "consecutive_quote_snapshot_blocks",
        "flat_unilateral_started_wall_s",
        "quote_context_sha256",
    ),
    "fill_cooldown_lineage": (
        "same_side_fill_units",
        "fill_cooldown_until_wall_s",
        "last_fill_side",
    ),
    "order_lifecycle": (
        "active_local_orders",
        "bid_client_order_id",
        "ask_client_order_id",
    ),
    "q90_runtime": ("action_hold", "shadow_order_count", "shadow_orders_sha256"),
    "post_fill_response": ("add_side", "excitation", "last_update_ms"),
    "quote_policy_clocks": (
        "last_requote_time_wall_s",
        "requote_count",
        "dynamic_rq",
    ),
    "signal_feature_dag_warmup": (
        "feature_dag_sha256",
        "causal_cutoff_exclusive_ms",
        "last_emitted_bucket_ms",
        "bar_history_coverage",
        "feature_history_coverage",
        "state_sha256",
        "cpp_engine_seeded",
        "cpp_backend_state",
    ),
}


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def sha256_file(path: str | Path) -> str:
    resolved = Path(path).expanduser().resolve(strict=True)
    digest = hashlib.sha256()
    with resolved.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json_fsync(path: Path, payload: Mapping[str, Any]) -> None:
    with path.open("x", encoding="utf-8") as handle:
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


def _plain(value: Any) -> Any:
    if is_dataclass(value):
        return _plain(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _plain(nested) for key, nested in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_plain(nested) for nested in value]
    if isinstance(value, Path):
        return str(value)
    return value


def _require_sha256(label: str, value: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise ValueError(f"{label} must be a 64-character SHA256")
    return normalized


def validate_initial_runtime_state_completeness(
    state: Mapping[str, Any],
) -> None:
    """Require explicit coverage of every decision-relevant state domain."""

    completeness = state.get("completeness")
    if not isinstance(completeness, Mapping):
        raise ValueError("initial runtime state completeness contract is missing")
    if completeness.get("schema_version") != (
        PROSPECTIVE_INITIAL_STATE_COMPLETENESS_SCHEMA_VERSION
    ):
        raise ValueError("initial runtime state completeness schema is unsupported")
    required = tuple(map(str, completeness.get("required_domains", ())))
    if required != PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS:
        raise ValueError("initial runtime state required domains drifted")
    captured = {str(value) for value in completeness.get("captured_domains", ())}
    missing = [
        domain
        for domain in PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS
        if domain not in captured or domain not in state
    ]
    if missing:
        raise ValueError(
            "initial runtime state domains are missing: " + ", ".join(missing)
        )
    for domain in PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS:
        payload = state.get(domain)
        if not isinstance(payload, Mapping) or not payload:
            raise ValueError(f"initial runtime state domain is empty: {domain}")
        expected_schema = PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[domain]
        if payload.get("schema_version") != expected_schema:
            raise ValueError(
                f"initial runtime state domain schema mismatch: {domain}"
            )
        absent_fields = [
            field
            for field in PROSPECTIVE_INITIAL_STATE_REQUIRED_FIELDS[domain]
            if field not in payload
        ]
        if absent_fields:
            raise ValueError(
                f"initial runtime state domain fields are missing for {domain}: "
                + ", ".join(absent_fields)
            )
    loss_cooldown_state = state["reward_path_loss_cooldown"]
    if loss_cooldown_state.get("semantics") != LOSS_COOLDOWN_SEMANTICS:
        raise ValueError("initial loss-cooldown semantics are stale")
    try:
        ConsecutiveLossCooldown.restore(loss_cooldown_state)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "initial loss-cooldown snapshot is incomplete or inconsistent"
        ) from exc
    signal_state = state["signal_feature_dag_warmup"]
    _require_sha256(
        "initial signal Feature DAG SHA256",
        str(signal_state["feature_dag_sha256"]),
    )
    _require_sha256(
        "initial signal state SHA256",
        str(signal_state["state_sha256"]),
    )
    bar_coverage = signal_state["bar_history_coverage"]
    feature_coverage = signal_state["feature_history_coverage"]
    backend_state = signal_state["cpp_backend_state"]
    for label, payload in (
        ("bar history coverage", bar_coverage),
        ("feature history coverage", feature_coverage),
        ("C++ backend state", backend_state),
    ):
        if not isinstance(payload, Mapping) or not payload:
            raise ValueError(f"initial signal {label} is empty")
    if int(bar_coverage.get("row_count", -1)) < 0:
        raise ValueError("initial signal bar history row count is invalid")
    if int(feature_coverage.get("row_count", -1)) < 0:
        raise ValueError("initial signal feature history row count is invalid")
    last_bucket = int(signal_state["last_emitted_bucket_ms"])
    cutoff = int(signal_state["causal_cutoff_exclusive_ms"])
    if last_bucket < 0 or cutoff < 0:
        raise ValueError("initial signal causal cutoff is invalid")
    expected_cutoff = last_bucket + 10_000 if last_bucket > 0 else 0
    if cutoff != expected_cutoff:
        raise ValueError("initial signal causal cutoff disagrees with emitted bucket")
    if int(feature_coverage.get("last_bucket_ms", last_bucket)) != last_bucket:
        raise ValueError("initial signal feature coverage disagrees with emitted bucket")
    for field in (
        "feature_engine_present",
        "reconstruction_contract",
        "expected_bar_count",
        "actual_bar_count",
        "expected_history_count",
        "actual_history_count",
        "global_flow_native_enabled",
        "global_flow_boundary_event_count",
        "cross_aggregator_count",
    ):
        if field not in backend_state:
            raise ValueError(f"initial signal C++ backend field is missing: {field}")
    feature_engine_present = bool(backend_state["feature_engine_present"])
    reconstruction_contract = str(backend_state["reconstruction_contract"])
    if feature_engine_present:
        if reconstruction_contract != CPP_FEATURE_RECONSTRUCTION_CONTRACT:
            raise ValueError(
                "initial signal C++ reconstruction contract is unsupported"
            )
        if int(backend_state["actual_bar_count"]) != int(
            backend_state["expected_bar_count"]
        ):
            raise ValueError("initial signal C++ bar count mismatch")
        if int(backend_state["actual_history_count"]) != int(
            backend_state["expected_history_count"]
        ):
            raise ValueError("initial signal C++ history count mismatch")
    else:
        if reconstruction_contract != PYTHON_FEATURE_STATE_CONTRACT:
            raise ValueError(
                "initial signal Python feature-state contract is unsupported"
            )
        if bool(signal_state["cpp_engine_seeded"]):
            raise ValueError("initial signal absent C++ engine cannot be seeded")
        if int(backend_state["actual_bar_count"]) != 0 or int(
            backend_state["actual_history_count"]
        ) != 0:
            raise ValueError("initial signal absent C++ engine has native state")
    if int(backend_state["global_flow_boundary_event_count"]) != 0:
        raise ValueError("initial signal global-flow boundary is nonempty")
    if int(backend_state["cross_aggregator_count"]) != 0:
        raise ValueError("initial signal cross-aggregator boundary is nonempty")
    unsupported = tuple(
        str(value).strip()
        for value in completeness.get("unsupported_initial_state_fields", ())
        if str(value).strip()
    )
    if unsupported:
        raise ValueError(
            "initial runtime state has unsupported fields: " + ", ".join(unsupported)
        )
    if completeness.get("binding_status") != "fully_bound":
        raise ValueError("initial runtime state is not fully bound")


def require_external_collection_root(
    root: str | Path,
    *,
    required_mount: str | Path | None = None,
    require_mounted: bool = True,
) -> tuple[Path, Path]:
    mount = (
        resolve_portable_path("${NARROWGATE_STORAGE_ROOT}")
        if required_mount is None
        else Path(required_mount).expanduser()
    ).resolve()
    resolved = Path(root).expanduser().resolve()
    if mount != resolved and mount not in resolved.parents:
        raise ValueError("prospective collection root must be inside the required mount")
    if require_mounted and (not mount.exists() or not os.path.ismount(mount)):
        raise RuntimeError(f"required prospective collection mount is unavailable: {mount}")
    if not mount.exists() or not mount.is_dir():
        raise RuntimeError(f"required prospective collection mount is not a directory: {mount}")
    return resolved, mount


def release_source_identity(source: Mapping[str, Any]) -> dict[str, Any]:
    """Bind Git-tracked source once through the deployment release root."""

    if set(source) != {
        "commit",
        "tree",
        "release_root_sha256",
        "worktree_clean",
    }:
        raise ValueError("release source identity fields drifted")
    commit = str(source["commit"]).strip().lower()
    tree = str(source["tree"]).strip().lower()
    release_root = _require_sha256(
        "deployment release root SHA256", source["release_root_sha256"]
    )
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise ValueError("release source commit is malformed")
    if len(tree) != 40 or any(character not in "0123456789abcdef" for character in tree):
        raise ValueError("release source tree is malformed")
    if source["worktree_clean"] is not True:
        raise ValueError("release source checkout must be clean")
    payload = {
        "schema_version": "narrowgate_prospective_release_source_identity.v1",
        "commit": commit,
        "tree": tree,
        "release_root_sha256": release_root,
        "worktree_clean": True,
    }
    return {**payload, "sha256": canonical_sha256(payload)}


def directory_content_identity(path: str | Path) -> dict[str, Any]:
    root = Path(path).expanduser().resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f"model bundle path is not a directory: {root}")
    files: dict[str, str] = {}
    for candidate in sorted(root.rglob("*")):
        if candidate.is_symlink():
            raise ValueError(f"model bundle cannot contain symlinks: {candidate}")
        if candidate.is_file():
            files[candidate.relative_to(root).as_posix()] = sha256_file(candidate)
    if not files:
        raise ValueError("model bundle is empty")
    content = {
        "schema_version": "narrowgate_model_bundle_content_identity.v1",
        "files": files,
    }
    return {"root": str(root), **content, "sha256": canonical_sha256(content)}


def snapshot_action_enablement(config: Any) -> dict[str, Any]:
    plain = _plain(config)
    selected: dict[str, Any] = {}

    def walk(value: Any, path: tuple[str, ...]) -> None:
        if isinstance(value, Mapping):
            for key, nested in sorted(value.items()):
                walk(nested, (*path, str(key)))
            return
        key = ".".join(path).lower()
        permission_fragments = (
            "enabled",
            "action",
            "shadow",
            "guard",
            "pause",
            "selector",
            "policy_mode",
        )
        if any(fragment in key for fragment in permission_fragments):
            selected[".".join(path)] = value

    walk(plain, ())
    if not selected:
        raise ValueError("action enablement identity is empty")
    return {
        "schema_version": "narrowgate_action_enablement_identity.v1",
        "fields": selected,
    }


def snapshot_data_source_identity(config: Any) -> dict[str, Any]:
    plain = _plain(config)
    if not isinstance(plain, Mapping):
        raise TypeError("live config must serialize to an object")
    return {
        "schema_version": "narrowgate_live_data_source_identity.v1",
        "symbol": plain.get("symbol"),
        "websocket": plain.get("websocket"),
        "multi_market": plain.get("multi_market"),
        "external_venues": plain.get("external_venues"),
    }


def live_clock_semantics_identity() -> dict[str, Any]:
    return {
        "schema_version": "narrowgate_live_clock_semantics.v1",
        "calendar_clock": "utc_ns",
        "exchange_clock": "exchange_event_time_ns",
        "visibility_clock": "local_callback_receive_time_ns",
        "decision_clock": "local_wall_time_ns",
        "latency_clock": "monotonic_perf_counter_ns",
        "feature_visibility_rule": "feature_ready_ts_ns<=decision_ts_ns",
        "quantity_time_outputs": ["exchange_time", "visibility_time"],
        "utc_midnight_splits_epoch": False,
        "missing_exchange_clock_policy": "null_physical_exposure_and_invalidate_tape_row",
    }


def execution_abi_identity(
    *,
    native_runtime: Mapping[str, Any],
    native_module_path: str | Path | None,
) -> dict[str, Any]:
    module_hash = None
    normalized_module = str(native_module_path or "").strip()
    if normalized_module and normalized_module not in {"disabled", "<unknown>"}:
        module = Path(normalized_module).expanduser()
        if module.is_file():
            module_hash = sha256_file(module)
        elif normalized_module.startswith("unavailable:"):
            module_hash = None
        else:
            raise ValueError(f"native runtime module does not exist: {module}")
    normalized_runtime = {
        str(key): value
        for key, value in _plain(native_runtime).items()
        if str(key) != "module"
    }
    payload = {
        "schema_version": "narrowgate_execution_abi_identity.v1",
        "native_runtime": normalized_runtime,
        "native_module_name": (
            Path(normalized_module).name
            if normalized_module and normalized_module not in {"disabled", "<unknown>"}
            else normalized_module
        ),
        "native_module_sha256": module_hash,
        "order_lifecycle_schema": "order_lifecycle_journal.v2",
        "order_lifecycle_writer_schema": "order_lifecycle_journal_writer.v2",
        "terminal_routing_abi": "q90_terminal_routing.v4",
    }
    return payload


@dataclass(frozen=True, slots=True)
class ProspectiveBaselineEpoch:
    epoch_id: str
    epoch_root: Path
    manifest_path: Path
    initial_runtime_state_path: Path
    identity: Mapping[str, str]
    identity_sha256: str
    storage_profile: str = LOCAL_ORICO_REPLAY_ADMISSION
    collection_bounds: Mapping[str, Any] | None = None

    def writer_runtime_identity(self) -> dict[str, Any]:
        return {
            "schema_version": "narrowgate_live_lifecycle_writer_runtime_identity.v1",
            "baseline_epoch_id": self.epoch_id,
            "baseline_epoch_identity_sha256": self.identity_sha256,
            "storage_profile": self.storage_profile,
            "local_admission_complete": False,
            "collection_bounds": _plain(self.collection_bounds or {}),
            **dict(self.identity),
        }


def publish_prospective_baseline_epoch(
    *,
    output_root: str | Path,
    required_mount: str | Path,
    repo_root: str | Path,
    config_path: str | Path,
    baseline_identity_path: str | Path,
    expected_baseline_identity_sha256: str,
    model_dir: str | Path,
    p3_path: str | Path,
    feature_dag_sha256: str,
    release_source: Mapping[str, Any],
    native_runtime: Mapping[str, Any],
    native_module_path: str | Path | None,
    action_enablement: Mapping[str, Any],
    initial_runtime_state: Mapping[str, Any],
    data_source_identity: Mapping[str, Any],
    clock_semantics: Mapping[str, Any],
    start_ts_ns: int | None = None,
    require_mounted: bool = True,
    storage_profile: str = LOCAL_ORICO_REPLAY_ADMISSION,
    remote_spool_allowlisted_roots: Sequence[str | Path] = (),
    collection_bounds: Mapping[str, Any] | None = None,
) -> ProspectiveBaselineEpoch:
    normalized_storage_profile = str(storage_profile).strip()
    if normalized_storage_profile == BOUNDED_REMOTE_SPOOL:
        bounds = dict(collection_bounds or {})
        if set(bounds) != {"max_duration_s", "max_bytes"}:
            raise ValueError("bounded remote epoch requires exact collection bounds")
        if float(bounds["max_duration_s"]) <= 0.0 or int(bounds["max_bytes"]) <= 0:
            raise ValueError("bounded remote epoch collection bounds must be positive")
        root, _ = validate_remote_spool_path(
            output_root,
            allowlisted_roots=remote_spool_allowlisted_roots,
            field_name="prospective_epoch_root",
        )
    elif normalized_storage_profile == LOCAL_ORICO_REPLAY_ADMISSION:
        bounds = {}
        root, _ = require_external_collection_root(
            output_root,
            required_mount=required_mount,
            require_mounted=require_mounted,
        )
    else:
        raise ValueError("unsupported prospective epoch storage profile")
    repo = Path(repo_root).expanduser().resolve(strict=True)
    config = Path(config_path).expanduser().resolve(strict=True)
    baseline = Path(baseline_identity_path).expanduser()
    if not baseline.is_absolute():
        baseline = repo / baseline
    baseline = baseline.resolve(strict=True)
    expected_baseline = _require_sha256(
        "expected baseline identity SHA256", expected_baseline_identity_sha256
    )
    actual_baseline = sha256_file(baseline)
    if actual_baseline != expected_baseline:
        raise ValueError("operational baseline identity SHA256 mismatch")
    validate_initial_runtime_state_completeness(initial_runtime_state)

    code_identity = release_source_identity(release_source)
    model_identity = directory_content_identity(model_dir)
    p3 = Path(p3_path).expanduser().resolve(strict=True)
    if not p3.is_file():
        raise ValueError("P3 identity path is not a file")
    feature_dag_hash = _require_sha256("feature DAG SHA256", feature_dag_sha256)
    execution_abi = execution_abi_identity(
        native_runtime=native_runtime,
        native_module_path=native_module_path,
    )

    state_payload = {
        "schema_version": PROSPECTIVE_BASELINE_INITIAL_STATE_SCHEMA_VERSION,
        "captured_ts_ns": int(start_ts_ns or time.time_ns()),
        "state": _plain(initial_runtime_state),
    }
    state_sha = canonical_sha256(state_payload)
    identity = {
        "runtime_code_sha256": str(code_identity["sha256"]),
        "config_sha256": sha256_file(config),
        "model_bundle_sha256": str(model_identity["sha256"]),
        "p3_sha256": sha256_file(p3),
        "feature_dag_sha256": feature_dag_hash,
        "execution_abi_sha256": canonical_sha256(execution_abi),
        "action_enablement_sha256": canonical_sha256(_plain(action_enablement)),
        "initial_runtime_state_sha256": state_sha,
        "data_source_identity_sha256": canonical_sha256(_plain(data_source_identity)),
        "clock_semantics_sha256": canonical_sha256(_plain(clock_semantics)),
    }
    if tuple(identity) != REQUIRED_IDENTITY_FIELDS:
        raise AssertionError("prospective epoch identity fields drifted")
    identity_hash = epoch_identity_sha256(identity)
    started = int(state_payload["captured_ts_ns"])
    epoch_id = f"prospective-{started}-{identity_hash[:12]}"
    root.mkdir(parents=True, exist_ok=True)
    temporary = root / f".{epoch_id}.partial-{os.getpid()}-{uuid.uuid4().hex}"
    final = root / epoch_id
    if final.exists():
        raise FileExistsError(f"prospective epoch already exists: {final}")
    temporary.mkdir(parents=False)
    try:
        state_path = temporary / "initial_runtime_state.json"
        evidence_path = temporary / "identity_evidence.json"
        manifest_path = temporary / "epoch_manifest.json"
        _write_json_fsync(state_path, state_payload)
        evidence = {
            "schema_version": "narrowgate_prospective_baseline_identity_evidence.v1",
            "operational_baseline_identity": {
                "path": str(baseline),
                "sha256": actual_baseline,
            },
            "runtime_code": code_identity,
            "config": {"path": str(config), "sha256": identity["config_sha256"]},
            "model_bundle": model_identity,
            "p3": {"path": str(p3), "sha256": identity["p3_sha256"]},
            "feature_dag": {"sha256": feature_dag_hash},
            "execution_abi": execution_abi,
            "action_enablement": _plain(action_enablement),
            "data_source_identity": _plain(data_source_identity),
            "clock_semantics": _plain(clock_semantics),
            "storage_profile": normalized_storage_profile,
            "collection_bounds": bounds,
        }
        _write_json_fsync(evidence_path, evidence)
        manifest = {
            "schema_version": PROSPECTIVE_BASELINE_EPOCH_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "start_ts_ns": started,
            "end_ts_ns": None,
            "start_reason": "controlled_process_start",
            "binding_status": "fully_bound",
            "storage_profile": normalized_storage_profile,
            "remote_spool_only": normalized_storage_profile == BOUNDED_REMOTE_SPOOL,
            "local_admission_complete": False,
            "collection_bounds": bounds,
            "identity": identity,
            "identity_sha256": identity_hash,
            "initial_runtime_state": {
                "path": "initial_runtime_state.json",
                "canonical_sha256": state_sha,
            },
            "identity_evidence": {
                "path": "identity_evidence.json",
                "canonical_sha256": canonical_sha256(evidence),
            },
            "historical_epochs_backfilled": False,
            "formal_collection_valid": False,
            "formal_collection_valid_reason": "awaiting_admitted_journal_v2_events",
            "permissions": {
                "lifecycle_estimation_authorized": False,
                "economic_estimation_authorized": False,
                "action_authorized": False,
                "live_policy_authorized": False,
            },
        }
        _write_json_fsync(manifest_path, manifest)
        _fsync_directory(temporary)
        os.replace(temporary, final)
        _fsync_directory(root)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return ProspectiveBaselineEpoch(
        epoch_id=epoch_id,
        epoch_root=final,
        manifest_path=final / "epoch_manifest.json",
        initial_runtime_state_path=final / "initial_runtime_state.json",
        identity=identity,
        identity_sha256=identity_hash,
        storage_profile=normalized_storage_profile,
        collection_bounds=bounds,
    )
