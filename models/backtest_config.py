"""Shared live-config to backtest parameter helpers."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import stat
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

import live.config as live_config_module
from data_paths import resolve_portable_path
from research.governance.public_machine_projection import (
    projection_for,
)

try:
    from models.queue_calibration import calibration_path, load_daily_queue_calibration
    from models.symbol_paths import model_dir as symbol_model_dir
except ImportError:
    from queue_calibration import calibration_path, load_daily_queue_calibration
    from symbol_paths import model_dir as symbol_model_dir

from live.config import RegimeConfig, load_config, to_backtest_params
from models.replay_contract import rest_return_sample_rows
from strategy.quote_core import finite_positive_quote_coefficient
from strategy.replay_controls import (
    LOSS_COOLDOWN_SEMANTICS,
    SYNC_DEGRADE_SEMANTICS,
)

TICK_DEFAULTS: Mapping[str, Any] = {
    # Replay-only global fill gate. Live exchange fills never read this value.
    "maker_fill_prob": 1.0,
    "dynamic_cap_enabled": False,
    "dynamic_cap_alpha": 0.5,
    "dynamic_cap_max_mult": 2.0,
    "dynamic_cap_var_baseline": 0.0,
    "dynamic_cap_liq_beta": 0.0,
    "dynamic_cap_liq_baseline": 0.0,
    "dynamic_cap_min_mult": 1.0,
    "depth_kappa_ratio": 0.3,
    "new_order_latency_ms": 0.0,
    "cancel_order_latency_ms": 0.0,
    "latency_jitter_ms": 0.0,
    "require_historical_bbo": False,
    "queue_ahead_mode": "exact_level",
    "queue_price_tolerance": 0.051,
    "requote_clock": "fixed",
    # Exploratory runs retain the historical trade-driven clock unless the
    # caller opts in. Formal replay switches this to ``merged`` below so
    # wall-clock lifecycle state cannot wait for the next execution trade.
    "replay_event_clock": "trade",
    "replay_clock_interval_ms": 100,
    # Zero retains the legacy clock; current async live mapping selects 100ms.
    "replay_main_loop_sleep_ms": 0,
    "replace_terminal_continuation": False,
    "fill_cooldown_apply_reducing": False,
    "consecutive_loss_cooldown_semantics": LOSS_COOLDOWN_SEMANTICS,
    "sync_adjust_replay_mode": "disabled",
    "sync_adjust_event_tape_path": "",
    "sync_adjust_event_tape_sha256": "",
    "sync_adjust_event_environment": "",
    "sync_adjust_semantics": SYNC_DEGRADE_SEMANTICS,
    "sync_adjust_stress_seed": 20260729,
    "sync_adjust_stress_interval_s": 21_600.0,
    # Live circuit breakers enter TIMEOUT_CLOSING and work inventory through
    # reduce-only maker orders.  The old replay-only immediate taker close is
    # retained as an explicit compatibility mode, never as the formal default.
    "circuit_breaker_exit_mode": "maker_close",
}

ML_PARAM_KEYS = ("vol_blend", "skew_strength", "asym_strength", "ret_skew", "gamma_dir_bonus")
COOLDOWN_POLICY_PARAM_KEYS = (
    "boolean_cooldown_policy_enabled",
    "boolean_cooldown_policy_path",
    "boolean_cooldown_policy_sha256",
    "boolean_cooldown_predicate_bundle_path",
    "boolean_cooldown_predicate_bundle_sha256",
    "boolean_cooldown_ema_warmup_s",
    "buy_e3_cooldown_policy_enabled",
    "buy_e3_cooldown_artifact_manifest_path",
    "buy_e3_cooldown_artifact_manifest_sha256",
    "buy_e3_cooldown_artifact_sha256",
    "buy_e3_cooldown_policy_path",
    "buy_e3_cooldown_policy_sha256",
    "buy_e3_cooldown_predicate_bundle_path",
    "buy_e3_cooldown_predicate_bundle_sha256",
    "buy_e3_cooldown_ema_warmup_s",
    "max_exec_book_visible_age_s",
)

ROOT = Path(__file__).resolve().parent.parent
PUBLIC_TEMPLATE_CONFIG = ROOT / "live" / "config.yaml"
CURRENT_OPERATIONAL_BASELINE_POINTER = (
    ROOT / "docs" / "private" / "operational_baseline.current.local.json"
)
CURRENT_BACKTEST_MECHANICS_POINTER = (
    ROOT / "docs" / "private" / "backtest_mechanics.current.local.json"
)
DEFAULT_LIQ_BASELINE = RegimeConfig().liq_baseline
OPERATIONAL_BASELINE_POINTER_SCHEMAS = frozenset(
    {
        "narrowgate_operational_baseline_pointer.v1",
        "narrowgate_operational_baseline_pointer.v2",
    }
)
MAX_AUTHORITY_FILE_BYTES = 64 << 20
PUBLIC_AUTHORITY_MODES = frozenset({0o644})
PRIVATE_AUTHORITY_MODES = frozenset({0o600})


@dataclass(slots=True)
class DefaultTickMechanicsResolution:
    """Own one exact baseline and its fresh per-day overlay."""

    baseline: Any
    overlay: Any

    def close(self) -> None:
        self.baseline.close()

    def __enter__(self) -> DefaultTickMechanicsResolution:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def parse_config_snapshot(
    source: bytes,
    *,
    source_path: Path,
    source_identity: tuple[int, int, int, int],
    validate_live_storage: bool = True,
) -> Any:
    """Parse an already verified live-config snapshot without reopening it."""
    path = _lexical_absolute(source_path)
    if not isinstance(source, bytes) or len(source_identity) != 4:
        raise ValueError("config snapshot identity is malformed")
    raw = yaml.safe_load(source.decode("utf-8")) or {}
    config = live_config_module._parse(raw)
    config._source_file_path = str(path)
    config._source_file_sha256 = hashlib.sha256(source).hexdigest()
    config._source_file_identity = source_identity
    if os.environ.get("BINANCE_API_KEY"):
        config.api.key = os.environ["BINANCE_API_KEY"]
    if os.environ.get("BINANCE_API_SECRET"):
        config.api.secret = os.environ["BINANCE_API_SECRET"]
    live_config_module._validate_config(config, validate_live_storage=validate_live_storage)
    return config


def _strict_json_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise RuntimeError(f"Duplicate JSON key in operational authority: {key}")
        payload[key] = value
    return payload


def _strict_json_snapshot(raw: bytes, path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_strict_json_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Operational authority JSON is invalid: {path}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Operational authority JSON root is invalid: {path}")
    return payload


def _lexical_absolute(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path.expanduser())))


def _open_authority_parent(path: Path) -> tuple[Path, int]:
    target = _lexical_absolute(path)
    if not target.is_absolute() or not hasattr(os, "O_NOFOLLOW"):
        raise RuntimeError("Secure operational authority reads are unsupported")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    flags |= getattr(os, "O_CLOEXEC", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(target.anchor, flags)
        for component in target.parent.parts[1:]:
            next_descriptor = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
    except OSError as exc:
        if descriptor is not None:
            os.close(descriptor)
        if isinstance(exc, FileNotFoundError):
            raise FileNotFoundError(target) from exc
        raise RuntimeError(
            f"Operational authority path contains an unavailable or symlinked ancestor: {target}"
        ) from exc
    if descriptor is None:
        raise RuntimeError(f"Operational authority parent is unavailable: {target}")
    return target, descriptor


def _authority_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _secure_authority_snapshot(
    path: Path,
    *,
    allowed_modes: frozenset[int],
) -> tuple[bytes, os.stat_result, Path]:
    target, parent_fd = _open_authority_parent(path)
    descriptor: int | None = None
    try:
        before = os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or stat.S_IMODE(before.st_mode) not in allowed_modes
            or before.st_nlink != 1
            or before.st_size < 0
            or before.st_size > MAX_AUTHORITY_FILE_BYTES
        ):
            raise RuntimeError(f"Operational authority file identity is unsafe: {target}")
        descriptor = os.open(
            target.name,
            os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        opened = os.fstat(descriptor)
        if _authority_identity(opened) != _authority_identity(before):
            raise RuntimeError(f"Operational authority changed while opening: {target}")
        chunks: list[bytes] = []
        observed = 0
        while True:
            chunk = os.read(descriptor, 1 << 20)
            if not chunk:
                break
            observed += len(chunk)
            if observed > MAX_AUTHORITY_FILE_BYTES:
                raise RuntimeError(f"Operational authority is oversized: {target}")
            chunks.append(chunk)
        after_fd = os.fstat(descriptor)
        after_path = os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
        raw = b"".join(chunks)
        if (
            _authority_identity(before) != _authority_identity(after_fd)
            or _authority_identity(before) != _authority_identity(after_path)
            or len(raw) != before.st_size
        ):
            raise RuntimeError(f"Operational authority changed during read: {target}")
        _again_target, again_fd = _open_authority_parent(target)
        try:
            if _authority_identity(os.fstat(parent_fd)) != _authority_identity(os.fstat(again_fd)):
                raise RuntimeError(f"Operational authority parent changed during read: {target}")
        finally:
            os.close(again_fd)
        return raw, before, target
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise RuntimeError(f"Operational authority read failed: {target}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent_fd)


def _secure_json_authority(
    path: Path,
    *,
    allowed_modes: frozenset[int],
) -> tuple[dict[str, Any], bytes, os.stat_result, Path]:
    raw, metadata, target = _secure_authority_snapshot(path, allowed_modes=allowed_modes)
    return _strict_json_snapshot(raw, target), raw, metadata, target


def _verified_projection_identity(
    path: Path,
    raw: bytes,
    *,
    public_allowed_modes: frozenset[int],
) -> tuple[Any, str, str]:
    public_sha256 = hashlib.sha256(raw).hexdigest()
    projection = projection_for(path, verify_private_if_available=False)
    after_raw, _after_metadata, _target = _secure_authority_snapshot(
        path, allowed_modes=public_allowed_modes
    )
    if after_raw != raw:
        raise RuntimeError(f"Operational projection changed during validation: {path}")
    if projection is None:
        return None, public_sha256, public_sha256
    expected_public = (
        projection.source_private_sha256
        if projection.materialized_identity == "private_source"
        else projection.public_projection_sha256
    )
    if public_sha256 != expected_public:
        raise RuntimeError(f"Operational projection public identity drifted: {path}")
    if projection.private_source_available:
        private_raw, _private_metadata, _private_target = _secure_authority_snapshot(
            projection.private_source_path,
            allowed_modes=frozenset({0o600}),
        )
        if hashlib.sha256(private_raw).hexdigest() != projection.source_private_sha256:
            raise RuntimeError(f"Operational projection private identity drifted: {path}")
    return projection, public_sha256, projection.source_private_sha256


def _resolve_authority_path(raw: Any, *, root: Path = ROOT) -> Path:
    text = str(raw)
    private_prefix = "${NARROWGATE_PRIVATE_CONFIG_ROOT}"
    if text == private_prefix or text.startswith(f"{private_prefix}/"):
        configured = Path(
            os.environ.get("NARROWGATE_PRIVATE_CONFIG_ROOT", root / "docs" / "private")
        ).expanduser()
        suffix = text[len(private_prefix) :].lstrip("/")
        path = configured / suffix if suffix else configured
    else:
        path = resolve_portable_path(text, root=root)
    if not path.is_absolute():
        path = root / path
    return _lexical_absolute(path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_repo_path(raw: Any, *, root: Path = ROOT) -> Path:
    path = resolve_portable_path(str(raw), root=root)
    if not path.is_absolute():
        path = root / path
    return path.resolve()


def _audit_operational_runtime_code(
    identity: Mapping[str, Any],
    *,
    root: Path,
) -> dict[str, Any]:
    """Compare an immutable deployment's runtime hashes with this checkout.

    The operational config remains useful as a paired control after local code
    evolves, but that run is a runtime overlay rather than an exact replay of
    the frozen deployment.  Keep those identities distinct without preventing
    development replays from using the same config/model/P3 inputs.
    """

    declared = identity.get("runtime_code")
    if not isinstance(declared, Mapping):
        return {
            "declared": False,
            "matches": None,
            "deployment_scope": "",
            "checked_paths": [],
            "missing_paths": [],
            "mismatched_paths": {},
            "workspace_sha256": "",
        }

    checked_paths: list[str] = []
    missing_paths: list[str] = []
    mismatched_paths: dict[str, dict[str, str]] = {}
    workspace_rows: list[tuple[str, str]] = []
    for raw_path, raw_expected in sorted(declared.items(), key=lambda item: str(item[0])):
        relative = str(raw_path)
        if relative == "deployment_scope":
            continue
        expected = str(raw_expected).lower()
        if len(expected) != 64 or any(char not in "0123456789abcdef" for char in expected):
            continue
        checked_paths.append(relative)
        path = _resolve_repo_path(relative, root=root)
        if not path.is_file():
            missing_paths.append(relative)
            workspace_rows.append((relative, "missing"))
            continue
        actual = _sha256_file(path)
        workspace_rows.append((relative, actual))
        if actual != expected:
            mismatched_paths[relative] = {
                "expected": expected,
                "actual": actual,
            }

    workspace_sha256 = hashlib.sha256(
        json.dumps(workspace_rows, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "declared": True,
        "matches": not missing_paths and not mismatched_paths,
        "deployment_scope": str(declared.get("deployment_scope", "")),
        "checked_paths": checked_paths,
        "missing_paths": missing_paths,
        "mismatched_paths": mismatched_paths,
        "workspace_sha256": workspace_sha256,
    }


def _expected_v12_flat_projection(identity: Mapping[str, Any]) -> dict[str, Any]:
    """Derive the compatibility projection from the frozen v12 identity.

    The v2 governance pointer may not invent or silently alter old control-arm
    semantics.  Every compatibility field is therefore either projected from
    the recursively hash-verified v12 identity or fixed to the exact v12
    pointer contract where that identity did not carry a named scalar.
    """

    config = identity.get("config") or {}
    policy = identity.get("f05_boolean_cooldown") or {}
    permissions = identity.get("permissions") or {}
    model = identity.get("model") or {}
    if not all(isinstance(value, Mapping) for value in (config, policy, permissions, model)):
        raise RuntimeError("Frozen v12 identity compatibility fields are malformed")
    return {
        "ml_enabled": config.get("ml_enabled"),
        "buy_fill_selection_shadow_enabled": config.get("buy_fill_selection_shadow_enabled"),
        "buy_fill_selection_live_enabled": config.get("buy_fill_selection_live_enabled"),
        "dynamic_fill_hazard_shadow_enabled": config.get("dynamic_fill_hazard_shadow_enabled"),
        "dynamic_fill_hazard_action_enabled": config.get("dynamic_fill_hazard_action_enabled"),
        "cross_venue_fair_price_shadow_enabled": config.get(
            "cross_venue_fair_price_shadow_enabled"
        ),
        "inventory_campaign_shadow_enabled": config.get("inventory_campaign_shadow_enabled"),
        "depth_execution_shadow_enabled": config.get("depth_execution_shadow_enabled"),
        "depth_imbalance_asymmetry_enabled": config.get("depth_imbalance_asymmetry_enabled"),
        "boolean_cooldown_policy_enabled": config.get("boolean_cooldown_policy_enabled"),
        "boolean_cooldown_policy_identity": policy.get("identity"),
        "boolean_cooldown_policy_sha256": policy.get("policy_sha256"),
        "boolean_cooldown_predicate_bundle_sha256": policy.get("predicate_bundle_sha256"),
        "boolean_cooldown_evidence_route": config.get("boolean_cooldown_evidence_route"),
        "boolean_cooldown_research_hard_gates_passed": identity.get("research_gate_passed"),
        "boolean_cooldown_owner_risk_accepted": policy.get("owner_override_required"),
        "boolean_cooldown_owner_live_authorized": permissions.get(
            "owner_risk_accepted_live_authorized"
        ),
        "q90_action_status": "suspended_terminal_active_riskset_integrity",
        "quote_snapshot_atomicity_contract": "v2",
        "max_exec_book_visible_age_s": config.get("max_exec_book_visible_age_s"),
        "max_exec_book_source_lag_s": config.get("max_exec_book_source_lag_s"),
        "baseline_integrity_gate_passed": identity.get("baseline_integrity_gate_passed"),
        "model_directory": model.get("directory"),
        "bundle_meta_sha256": model.get("bundle_meta_sha256"),
    }


def load_operational_baseline_binding(
    *,
    root: Path = ROOT,
    pointer_path: Path | None = None,
) -> dict[str, Any] | None:
    """Load and verify the mutable pointer to the immutable current baseline.

    A public checkout intentionally lacks ``docs/private`` and therefore falls
    back to the public template. When the private config is present, every
    pointer, identity, and config hash is fail-closed so a backtest cannot
    silently use a stale rolling control.
    """
    pointer = (
        _lexical_absolute(pointer_path)
        if pointer_path is not None
        else (
            CURRENT_OPERATIONAL_BASELINE_POINTER
            if root == ROOT
            else root / "docs" / "private" / "operational_baseline.current.local.json"
        )
    )
    try:
        payload, pointer_raw, _pointer_metadata, pointer = _secure_json_authority(
            pointer, allowed_modes=PUBLIC_AUTHORITY_MODES
        )
    except FileNotFoundError:
        return None

    (
        pointer_projection,
        pointer_public_sha256,
        pointer_source_sha256,
    ) = _verified_projection_identity(
        pointer, pointer_raw, public_allowed_modes=PUBLIC_AUTHORITY_MODES
    )
    pointer_schema = str(payload.get("schema_version", ""))
    if pointer_schema not in OPERATIONAL_BASELINE_POINTER_SCHEMAS:
        raise RuntimeError(f"Unsupported operational baseline pointer: {pointer}")
    if not bool(payload.get("backtest_default_control_authorized", False)):
        raise RuntimeError(f"Backtest control is not authorized by {pointer}")

    identity_path = _resolve_authority_path(payload.get("identity_path", ""), root=root)
    try:
        (
            governance_identity,
            identity_raw,
            _identity_metadata,
            identity_path,
        ) = _secure_json_authority(identity_path, allowed_modes=PUBLIC_AUTHORITY_MODES)
    except FileNotFoundError:
        raise RuntimeError(f"Operational baseline identity is missing: {identity_path}") from None
    (
        identity_projection,
        identity_public_sha256,
        identity_source_sha256,
    ) = _verified_projection_identity(
        identity_path,
        identity_raw,
        public_allowed_modes=PUBLIC_AUTHORITY_MODES,
    )
    expected_identity_sha256 = str(payload.get("identity_sha256", ""))
    # The mutable current pointer binds the bytes a public consumer reads. A
    # registered projection separately verifies the frozen private source
    # identity, so redaction cannot silently rewrite the executed identity.
    identity_pointer_sha256 = (
        identity_projection.public_projection_sha256
        if identity_projection is not None
        else identity_source_sha256
    )
    if identity_pointer_sha256 != expected_identity_sha256:
        raise RuntimeError(
            "Operational baseline identity SHA256 mismatch: "
            f"{identity_path} expected={payload.get('identity_sha256')} "
            f"public={identity_public_sha256} source={identity_source_sha256}"
        )
    if governance_identity.get("baseline_id") != payload.get("baseline_id"):
        raise RuntimeError("Operational baseline pointer and identity IDs disagree")
    governance_permissions = governance_identity.get("permissions") or {}
    if not bool(governance_permissions.get("operational_baseline_active", False)):
        raise RuntimeError("Operational baseline identity is not active")
    if pointer_schema == "narrowgate_operational_baseline_pointer.v2":
        if (
            governance_permissions.get("governance_locator_publication_authorized") is not True
            or governance_permissions.get("baseline_promotion_authorized") is not False
        ):
            raise RuntimeError("Operational locator governance permissions drifted")
    elif not bool(governance_permissions.get("baseline_promotion_authorized", False)):
        raise RuntimeError("Operational baseline identity lacks promotion authority")
    if not bool(governance_permissions.get("backtest_default_control_authorized", False)):
        raise RuntimeError("Operational baseline identity lacks backtest authority")
    normalized_pointer = dict(payload)
    backtest_identity_path = identity_path
    backtest_identity_projection = identity_projection
    backtest_identity_source_sha256 = identity_source_sha256
    backtest_identity_pointer_sha256 = identity_pointer_sha256
    identity = governance_identity
    if pointer_schema == "narrowgate_operational_baseline_pointer.v2":
        current_live = payload.get("current_live_binding") or {}
        backtest_default = payload.get("backtest_default_binding") or {}
        backtest_flat = payload.get("backtest_default_flat_projection") or {}
        identity_current_live = governance_identity.get("current_live") or {}
        identity_backtest = governance_identity.get("backtest_default") or {}
        identity_predecessor = governance_identity.get("predecessor") or {}
        identity_top_config = governance_identity.get("config") or {}
        if (
            not isinstance(current_live, Mapping)
            or dict(current_live) != {"availability": "private_not_distributed"}
            or not isinstance(identity_current_live, Mapping)
            or dict(identity_current_live) != {"availability": "private_not_distributed"}
            or set(backtest_default)
            != {
                "baseline_id",
                "identity_path",
                "identity_sha256",
                "config_path",
                "config_sha256",
                "current_live_alias_allowed",
                "exact_buy_e3_replay_baseline_available",
                "current_live_e3_evidence_is_backtest_economic_authority",
                "control_arm",
                "predecessor_control_arm",
                "replay_baseline_path",
                "replay_baseline_sha256",
                "replay_ber_clock_semantics",
            }
            or set(identity_backtest)
            != {
                "status",
                "identity",
                "identity_sha256",
                "config_locator",
                "config_sha256",
                "control_arm",
                "predecessor_control_arm",
                "replay_baseline_path",
                "replay_baseline_sha256",
                "replay_ber_clock_semantics",
                "exact_buy_e3_replay_baseline_available",
                "current_live_config_may_replace_backtest_default",
                "current_live_evidence_is_backtest_economic_authority",
                "historical_v12_economic_evidence_reinterpreted",
            }
        ):
            raise RuntimeError("Private deployment boundary field sets drifted")
        if (
            set(payload)
            != {
                "schema_version",
                "updated_at_utc",
                "baseline_id",
                "identity_path",
                "identity_sha256",
                "current_live_binding",
                "backtest_default_binding",
                "backtest_default_flat_projection",
                "backtest_default_control_authorized",
                "backtest_default_control_scope",
                "historical_v12_identity_modified",
                "no_cross_layer_substitution",
            }
            or set(governance_identity)
            != {
                "schema_version",
                "baseline_id",
                "effective_at_utc",
                "operational_status",
                "promotion_class",
                "predecessor",
                "current_live",
                "backtest_default",
                "config",
                "model",
                "p3",
                "permissions",
            }
            or governance_identity.get("schema_version")
            != "narrowgate_operational_baseline_identity.private_boundary.v1"
            or governance_identity.get("operational_status")
            != "private_deployment_not_distributed"
            or governance_identity.get("promotion_class")
            != "public_backtest_locator_no_live_or_economic_authority"
            or payload.get("backtest_default_control_scope")
            != "immutable_backtest_control"
            or payload.get("historical_v12_identity_modified") is not False
            or governance_identity.get("effective_at_utc") != payload.get("updated_at_utc")
            or governance_permissions.get("operational_baseline_active") is not True
            or governance_permissions.get("governance_locator_publication_authorized") is not True
            or governance_permissions.get("baseline_promotion_authorized") is not False
            or governance_permissions.get("backtest_default_control_authorized") is not True
            or governance_permissions.get("private_release_authority_required") is not True
            or set(governance_permissions)
            != {
                "operational_baseline_active",
                "governance_locator_publication_authorized",
                "baseline_promotion_authorized",
                "backtest_default_control_authorized",
                "private_release_authority_required",
            }
        ):
            raise RuntimeError("Private deployment governance envelope drifted")
        if (
            payload.get("no_cross_layer_substitution") is not True
            or identity_predecessor
            != {
                "baseline_id": backtest_default.get("baseline_id"),
                "identity": backtest_default.get("identity_path"),
                "identity_sha256": backtest_default.get("identity_sha256"),
                "historical_identity_modified": False,
                "status": "immutable_backtest_default_control",
            }
            or identity_top_config
            != {
                "scope": "backtest_default_immutable_v12_control",
                "canonical_private_source": backtest_default.get("config_path"),
                "sha256": backtest_default.get("config_sha256"),
            }
            or identity_backtest.get("status")
            != "immutable_backtest_default_control"
            or identity_backtest.get("identity") != backtest_default.get("identity_path")
            or identity_backtest.get("identity_sha256") != backtest_default.get("identity_sha256")
            or backtest_default.get("config_sha256") != identity_backtest.get("config_sha256")
            or backtest_default.get("config_path") != identity_backtest.get("config_locator")
            or backtest_default.get("exact_buy_e3_replay_baseline_available") is not False
            or backtest_default.get("current_live_alias_allowed") is not False
            or backtest_default.get("current_live_e3_evidence_is_backtest_economic_authority")
            is not False
            or identity_backtest.get("control_arm") != backtest_default.get("control_arm")
            or identity_backtest.get("predecessor_control_arm")
            != backtest_default.get("predecessor_control_arm")
            or identity_backtest.get("replay_baseline_path")
            != backtest_default.get("replay_baseline_path")
            or identity_backtest.get("replay_baseline_sha256")
            != backtest_default.get("replay_baseline_sha256")
            or identity_backtest.get("replay_ber_clock_semantics")
            != backtest_default.get("replay_ber_clock_semantics")
            or identity_backtest.get("exact_buy_e3_replay_baseline_available") is not False
            or identity_backtest.get("current_live_config_may_replace_backtest_default")
            is not False
            or identity_backtest.get("current_live_evidence_is_backtest_economic_authority")
            is not False
            or identity_backtest.get("historical_v12_economic_evidence_reinterpreted") is not False
            or not isinstance(backtest_flat, Mapping)
        ):
            raise RuntimeError("Operational baseline authority layers drifted")

        backtest_identity_path = _resolve_authority_path(
            backtest_default.get("identity_path", ""), root=root
        )
        try:
            (
                identity,
                backtest_identity_raw,
                _backtest_identity_metadata,
                backtest_identity_path,
            ) = _secure_json_authority(backtest_identity_path, allowed_modes=PUBLIC_AUTHORITY_MODES)
        except FileNotFoundError:
            raise RuntimeError(
                f"Frozen backtest identity is missing: {backtest_identity_path}"
            ) from None
        (
            backtest_identity_projection,
            _backtest_identity_public_sha256,
            backtest_identity_source_sha256,
        ) = _verified_projection_identity(
            backtest_identity_path,
            backtest_identity_raw,
            public_allowed_modes=PUBLIC_AUTHORITY_MODES,
        )
        backtest_identity_pointer_sha256 = (
            backtest_identity_projection.public_projection_sha256
            if backtest_identity_projection is not None
            else backtest_identity_source_sha256
        )
        if (
            backtest_identity_pointer_sha256 != backtest_default.get("identity_sha256")
            or identity_backtest.get("identity") != backtest_default.get("identity_path")
            or identity_backtest.get("identity_sha256") != backtest_default.get("identity_sha256")
        ):
            raise RuntimeError("Frozen v12 backtest identity binding drifted")
        if identity.get("baseline_id") != backtest_default.get("baseline_id"):
            raise RuntimeError("Frozen v12 backtest baseline id drifted")
        expected_governance_p3 = {
            "path": (identity.get("p3") or {}).get("path"),
            "sha256": (identity.get("p3") or {}).get("sha256"),
            "schema_version": (identity.get("p3") or {}).get("schema_version"),
            "changed_by_successor": False,
        }
        if (
            governance_identity.get("model") != identity.get("model")
            or governance_identity.get("p3") != expected_governance_p3
        ):
            raise RuntimeError("Frozen v12 model/P3 governance projection drifted")
        expected_backtest_flat = _expected_v12_flat_projection(identity)
        if dict(backtest_flat) != expected_backtest_flat:
            raise RuntimeError("Frozen v12 backtest compatibility projection drifted")
        normalized_pointer.update(deepcopy(dict(backtest_flat)))
        normalized_pointer.update(
            {
                "baseline_id": backtest_default["baseline_id"],
                "identity_path": backtest_default["identity_path"],
                "identity_sha256": backtest_default["identity_sha256"],
                "live_config_path": backtest_default["config_path"],
                "live_config_candidates": [backtest_default["config_path"]],
                "live_config_sha256": backtest_default["config_sha256"],
                "backtest_control_arm": backtest_default["control_arm"],
                "backtest_predecessor_control_arm": backtest_default["predecessor_control_arm"],
                "backtest_replay_baseline_path": backtest_default["replay_baseline_path"],
                "backtest_replay_baseline_sha256": backtest_default["replay_baseline_sha256"],
                "backtest_replay_ber_clock_semantics": backtest_default[
                    "replay_ber_clock_semantics"
                ],
                "model_directory": backtest_flat["model_directory"],
                "bundle_meta_sha256": backtest_flat["bundle_meta_sha256"],
            }
        )

    permissions = identity.get("permissions") or {}
    if not bool(permissions.get("operational_baseline_active", False)):
        raise RuntimeError("Backtest baseline identity is not active")
    if not bool(permissions.get("baseline_promotion_authorized", False)):
        raise RuntimeError("Backtest baseline identity lacks promotion authority")
    if not bool(permissions.get("backtest_default_control_authorized", False)):
        raise RuntimeError("Backtest baseline identity lacks backtest authority")
    runtime_code_audit = _audit_operational_runtime_code(identity, root=root)

    replay_baseline: dict[str, Any] | None = None
    replay_baseline_path: Path | None = None
    raw_replay_baseline_path = str(
        normalized_pointer.get("backtest_replay_baseline_path", "")
    ).strip()
    if raw_replay_baseline_path:
        replay_baseline_path = _resolve_authority_path(raw_replay_baseline_path, root=root)
        try:
            (
                replay_baseline,
                replay_baseline_raw,
                _replay_metadata,
                replay_baseline_path,
            ) = _secure_json_authority(replay_baseline_path, allowed_modes=PUBLIC_AUTHORITY_MODES)
        except FileNotFoundError:
            raise RuntimeError(
                f"Operational replay baseline is missing: {replay_baseline_path}"
            ) from None
        expected_replay_sha256 = str(normalized_pointer.get("backtest_replay_baseline_sha256", ""))
        actual_replay_sha256 = hashlib.sha256(replay_baseline_raw).hexdigest()
        if actual_replay_sha256 != expected_replay_sha256:
            raise RuntimeError(
                "Operational replay baseline SHA256 mismatch: "
                f"{replay_baseline_path} expected={expected_replay_sha256} "
                f"actual={actual_replay_sha256}"
            )
        replay_permissions = replay_baseline.get("permissions") or {}
        replay_semantics = replay_baseline.get("replay_semantics") or {}
        if not bool(replay_permissions.get("backtest_default_control_authorized", False)):
            raise RuntimeError("Operational replay baseline lacks backtest authority")
        expected_clock = str(normalized_pointer.get("backtest_replay_ber_clock_semantics", ""))
        if str(replay_semantics.get("ber_clock_identity", "")) != expected_clock:
            raise RuntimeError("Operational replay baseline BER clock identity drifted")

    preferred_config_path = _resolve_authority_path(
        normalized_pointer.get("live_config_path", ""), root=root
    )
    candidate_values = list(normalized_pointer.get("live_config_candidates") or [])
    if not candidate_values:
        candidate_values = [normalized_pointer.get("live_config_path", "")]
    config_path = preferred_config_path
    config_exists = False
    config_snapshot_raw: bytes | None = None
    config_snapshot_identity: tuple[int, int, int, int] | None = None
    expected_config_sha256 = str(normalized_pointer.get("live_config_sha256", ""))
    config_allowed_modes = (
        PRIVATE_AUTHORITY_MODES
        if pointer_schema == "narrowgate_operational_baseline_pointer.v2"
        else frozenset({0o600, 0o644})
    )
    for raw_candidate in candidate_values:
        candidate = _resolve_authority_path(raw_candidate, root=root)
        try:
            candidate_raw, candidate_metadata, candidate = _secure_authority_snapshot(
                candidate, allowed_modes=config_allowed_modes
            )
        except FileNotFoundError:
            continue
        candidate_sha256 = hashlib.sha256(candidate_raw).hexdigest()
        if candidate_sha256 == expected_config_sha256:
            config_path = candidate
            config_exists = True
            config_snapshot_raw = candidate_raw
            config_snapshot_identity = (
                candidate_metadata.st_dev,
                candidate_metadata.st_ino,
                candidate_metadata.st_size,
                candidate_metadata.st_mtime_ns,
            )
            break
        if candidate == preferred_config_path:
            raise RuntimeError(
                "Operational baseline config SHA256 mismatch: "
                f"{candidate} expected={expected_config_sha256} actual={candidate_sha256}"
            )
    if (
        pointer_schema == "narrowgate_operational_baseline_pointer.v2"
        and not config_exists
        and (
            (root / "docs" / "private").is_dir()
            or bool(os.environ.get("NARROWGATE_PRIVATE_CONFIG_ROOT", "").strip())
        )
    ):
        raise RuntimeError(
            "Immutable v12 backtest config is missing from an owner/private checkout"
        )
    model_path: Path | None = None
    if config_exists:
        if config_snapshot_raw is None or config_snapshot_identity is None:
            raise RuntimeError("Operational baseline config snapshot is missing")
        config_sha256 = hashlib.sha256(config_snapshot_raw).hexdigest()
        identity_config_sha256 = str((identity.get("config") or {}).get("sha256", ""))
        if config_sha256 != expected_config_sha256:
            raise RuntimeError(
                "Operational baseline config SHA256 mismatch: "
                f"{config_path} expected={expected_config_sha256} actual={config_sha256}"
            )
        if config_sha256 != identity_config_sha256:
            raise RuntimeError("Operational baseline identity and config hashes disagree")

        model_path = _resolve_authority_path(
            normalized_pointer.get("model_directory", ""), root=root
        )
        if not model_path.is_dir():
            raise RuntimeError(f"Operational baseline model directory is missing: {model_path}")
        identity_model = identity.get("model") or {}
        if str(identity_model.get("directory", "")) != str(
            normalized_pointer.get("model_directory", "")
        ):
            raise RuntimeError("Operational baseline pointer and identity model paths disagree")
        model_authority_modes = (
            PRIVATE_AUTHORITY_MODES
            if pointer_schema == "narrowgate_operational_baseline_pointer.v2"
            else PUBLIC_AUTHORITY_MODES
        )
        bundle_meta = model_path / "bundle_meta.json"
        expected_bundle_sha256 = str(normalized_pointer.get("bundle_meta_sha256", ""))
        try:
            bundle_raw, _bundle_metadata, _bundle_path = _secure_authority_snapshot(
                bundle_meta, allowed_modes=model_authority_modes
            )
        except FileNotFoundError:
            bundle_raw = b""
        if hashlib.sha256(bundle_raw).hexdigest() != expected_bundle_sha256:
            raise RuntimeError("Operational baseline bundle_meta SHA256 mismatch")
        if expected_bundle_sha256 != str(identity_model.get("bundle_meta_sha256", "")):
            raise RuntimeError("Operational baseline identity and bundle hashes disagree")
        training_summary = model_path / "training_summary.json"
        try:
            training_raw, _training_metadata, training_summary = _secure_authority_snapshot(
                training_summary, allowed_modes=model_authority_modes
            )
            _training_projection, _training_public, training_source = _verified_projection_identity(
                training_summary,
                training_raw,
                public_allowed_modes=model_authority_modes,
            )
        except FileNotFoundError:
            training_source = ""
        if training_source != str(identity_model.get("training_summary_sha256", "")):
            raise RuntimeError("Operational baseline training_summary SHA256 mismatch")
        p3 = identity.get("p3") or {}
        p3_path = _resolve_authority_path(p3.get("path", ""), root=root)
        try:
            p3_raw, _p3_metadata, p3_path = _secure_authority_snapshot(
                p3_path, allowed_modes=model_authority_modes
            )
            _p3_projection, _p3_public, p3_source = _verified_projection_identity(
                p3_path, p3_raw, public_allowed_modes=model_authority_modes
            )
        except FileNotFoundError:
            p3_source = ""
        if p3_source != str(p3.get("sha256", "")):
            raise RuntimeError("Operational baseline P3 SHA256 mismatch")

    current_live_binding = (
        deepcopy(dict(payload.get("current_live_binding") or {}))
        if pointer_schema == "narrowgate_operational_baseline_pointer.v2"
        else None
    )
    return {
        # Compatibility consumers receive the recursively verified backtest
        # projection. The ordinary public v2 governance pointer remains
        # separately available and must never relabel a v12 replay as E3/v13.
        "pointer": normalized_pointer,
        "governance_pointer": payload,
        "pointer_path": pointer,
        "pointer_sha256": pointer_public_sha256,
        "pointer_source_sha256": pointer_source_sha256,
        "pointer_public_projection_sha256": (
            pointer_projection.public_projection_sha256 if pointer_projection is not None else None
        ),
        "identity": identity,
        "identity_path": backtest_identity_path,
        "identity_sha256": backtest_identity_pointer_sha256,
        "identity_source_sha256": backtest_identity_source_sha256,
        "identity_public_projection_sha256": (
            backtest_identity_projection.public_projection_sha256
            if backtest_identity_projection is not None
            else None
        ),
        "current_live_identity": governance_identity,
        "current_live_identity_path": identity_path,
        "current_live_identity_sha256": identity_pointer_sha256,
        "current_live_identity_source_sha256": identity_source_sha256,
        "current_live_identity_public_projection_sha256": (
            identity_projection.public_projection_sha256
            if identity_projection is not None
            else None
        ),
        "config_path": config_path,
        "config_exists": config_exists,
        "verified_config_raw": config_snapshot_raw,
        "verified_config_identity": config_snapshot_identity,
        "verified_config_allowed_modes": config_allowed_modes,
        "config_scope": (
            "immutable_v12_backtest_default"
            if pointer_schema == "narrowgate_operational_baseline_pointer.v2"
            else "legacy_current_operational_baseline"
        ),
        "current_live_config": current_live_binding,
        "model_path": model_path,
        "runtime_code_audit": runtime_code_audit,
        "replay_baseline": replay_baseline,
        "replay_baseline_path": replay_baseline_path,
    }


def resolve_backtest_config_path(
    path: str | Path | None = None,
    *,
    root: Path = ROOT,
    pointer_path: Path | None = None,
) -> Path:
    """Resolve explicit config, environment override, or current baseline."""
    if path is not None:
        return resolve_portable_path(path, root=root).resolve()
    env_path = str(os.environ.get("MM_LIVE_CONFIG", "") or "").strip()
    if env_path:
        return resolve_portable_path(env_path, root=root).resolve()
    binding = load_operational_baseline_binding(root=root, pointer_path=pointer_path)
    if binding is not None and bool(binding["config_exists"]):
        return Path(binding["config_path"])
    return (PUBLIC_TEMPLATE_CONFIG if root == ROOT else root / "live" / "config.yaml").resolve()


def operational_baseline_config_candidates(
    *,
    root: Path = ROOT,
    pointer_path: Path | None = None,
) -> set[Path]:
    """Return declared current-config paths without validating their bytes."""
    pointer = (
        _lexical_absolute(pointer_path)
        if pointer_path is not None
        else (
            CURRENT_OPERATIONAL_BASELINE_POINTER
            if root == ROOT
            else root / "docs" / "private" / "operational_baseline.current.local.json"
        )
    )
    try:
        payload, _raw, _metadata, _pointer = _secure_json_authority(
            pointer, allowed_modes=PUBLIC_AUTHORITY_MODES
        )
    except (OSError, RuntimeError):
        return set()
    schema = str(payload.get("schema_version", ""))
    if schema == "narrowgate_operational_baseline_pointer.v2":
        backtest = payload.get("backtest_default_binding") or {}
        raw_candidates = [backtest.get("config_path", "")]
    else:
        raw_candidates = list(payload.get("live_config_candidates") or [])
        if not raw_candidates:
            raw_candidates = [payload.get("live_config_path", "")]
    return {
        _resolve_authority_path(raw, root=root) for raw in raw_candidates if str(raw or "").strip()
    }


def _verified_binding_config_params(binding: Mapping[str, Any]) -> dict[str, Any]:
    raw = binding.get("verified_config_raw")
    identity = binding.get("verified_config_identity")
    path = binding.get("config_path")
    allowed_modes = binding.get("verified_config_allowed_modes")
    if (
        not isinstance(raw, bytes)
        or not isinstance(identity, tuple)
        or len(identity) != 4
        or not isinstance(path, Path)
        or not isinstance(allowed_modes, frozenset)
        or not allowed_modes
        or not all(isinstance(mode, int) for mode in allowed_modes)
    ):
        raise RuntimeError("Operational baseline verified config snapshot is unavailable")
    observed_raw, observed_metadata, observed_path = _secure_authority_snapshot(
        path, allowed_modes=allowed_modes
    )
    observed_identity = (
        observed_metadata.st_dev,
        observed_metadata.st_ino,
        observed_metadata.st_size,
        observed_metadata.st_mtime_ns,
    )
    if observed_path != path or observed_raw != raw or observed_identity != identity:
        raise RuntimeError("Operational baseline config changed after verification")
    config = parse_config_snapshot(
        raw,
        source_path=path,
        source_identity=identity,
    )
    return to_backtest_params(config)


REPLAY_LOCATOR_FIELDS = frozenset({
    "ml.model_dir",
    "strategy.boolean_cooldown_policy_path",
    "strategy.boolean_cooldown_predicate_bundle_path",
    "strategy.buy_e3_cooldown_artifact_manifest_path",
    "strategy.buy_e3_cooldown_policy_path",
    "strategy.buy_e3_cooldown_predicate_bundle_path",
})


def _load_replay_locator_params(config_path: Path, projection_path: Path) -> dict[str, Any]:
    """Resolve local artifact locations without rewriting the source config."""
    projection = json.loads(projection_path.read_bytes())
    if (
        not isinstance(projection, dict)
        or set(projection) != {
            "schema_version", "visibility", "authority", "source_config", "locator_overrides",
        }
        or projection["schema_version"] != "narrowgate_local_replay_locator_projection.v1"
        or projection["visibility"] != "local_only_do_not_publish"
        or projection["authority"] != "none_locator_only"
    ):
        raise ValueError("invalid local replay locator-only projection")
    source = projection["source_config"]
    overrides = projection["locator_overrides"]
    if (
        not isinstance(source, dict) or set(source) != {"path", "sha256"}
        or not isinstance(source["path"], str) or not isinstance(source["sha256"], str)
        or Path(source["path"]).expanduser().resolve() != config_path.resolve()
    ):
        raise ValueError("replay locator projection does not name the selected source config")
    if (
        not isinstance(overrides, dict) or not overrides
        or set(overrides) - REPLAY_LOCATOR_FIELDS
    ):
        raise ValueError("replay locator overrides may contain only the six model/policy paths")
    raw, identity = live_config_module._stable_config_bytes(config_path)
    source_sha = hashlib.sha256(raw).hexdigest()
    if source_sha != source["sha256"]:
        raise ValueError("replay locator source config SHA256 mismatch")
    # Replay never opens the remote live journal. All semantic config checks
    # still run, and the immutable source bytes/identity remain the original.
    config = parse_config_snapshot(
        raw, source_path=config_path, source_identity=identity, validate_live_storage=False,
    )
    resolved: dict[str, str] = {}
    for name, value in overrides.items():
        if not isinstance(value, str) or not value.strip() or not Path(value).is_absolute():
            raise ValueError(f"replay locator {name} must be an absolute existing path")
        candidate = Path(value).expanduser().resolve(strict=True)
        if not (candidate.is_dir() if name == "ml.model_dir" else candidate.is_file()):
            raise ValueError(f"replay locator {name} has the wrong filesystem type")
        section, field = name.split(".")
        setattr(getattr(config, section), field, str(candidate))
        resolved[name] = str(candidate)
    params = to_backtest_params(config)
    params["_replay_locator_projection"] = {
        "path": str(projection_path.resolve()), "source_config": dict(source),
        "locator_overrides": resolved, "authority": "none_locator_only",
    }
    params["_config_source_sha256"] = config._source_file_sha256
    return params


def load_live_config_as_params(
    path: str | Path | None = None, *, locator_projection_path: str | Path | None = None,
) -> dict[str, Any]:
    """Read live YAML and return the flat replay-compatible parameter map."""
    if locator_projection_path is not None:
        if path is None:
            raise ValueError("replay locator projection requires an explicit config path")
        return _load_replay_locator_params(
            Path(path).expanduser().resolve(), Path(locator_projection_path).expanduser().resolve(),
        )
    if path is None and not str(os.environ.get("MM_LIVE_CONFIG", "") or "").strip():
        binding = load_operational_baseline_binding()
        if binding is not None and bool(binding["config_exists"]):
            return _verified_binding_config_params(binding)
    config_path = resolve_backtest_config_path(path)
    return to_backtest_params(load_config(config_path))


def _resolve_project_path(raw: Any) -> Path | None:
    """Resolve a live-config path relative to the project root."""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    path = resolve_portable_path(text, root=ROOT)
    if not path.is_absolute():
        path = ROOT / path
    return path.resolve()


def build_backtest_base_params(
    live_params: Mapping[str, Any],
    *,
    p3_delta_star: float = 0.0,
    p3_kappa_eff: float = 0.0,
    queue_calibration: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the shared bar/tick-compatible backtest parameter dict."""
    # 这里是“把 live config 映射到 replay 参数”的唯一共享入口。
    # 新增 live guard/policy 参数时要先进这里，再分别做 Python/C++ parity。
    params = {
        "gamma": finite_positive_quote_coefficient("gamma", live_params["gamma"]),
        "quote_math_mode": str(live_params.get("quote_math_mode", "legacy_v0")),
        "inventory_reference_qty": finite_positive_quote_coefficient(
            "inventory_reference_qty",
            live_params.get("inventory_reference_qty", 1.0),
        ),
        "kappa": live_params["kappa"],
        "order_size": live_params["order_size"],
        "max_inventory": live_params["max_inventory"],
        "max_daily_loss": live_params.get("max_daily_loss"),
        "max_position_value": live_params.get("max_position_value"),
        "emergency_close_dd": live_params.get("emergency_close_dd"),
        "requote_interval": live_params.get("requote_interval", 10.0),
        "quote_horizon_s": live_params.get("quote_horizon_s", 1.0),
        "historical_p3_scalar_adapter_enabled": bool(
            live_params.get("historical_p3_scalar_adapter_enabled", True)
        ),
        "p3_side_bbo_floor_enabled": bool(
            live_params.get("p3_side_bbo_floor_enabled", False)
        ),
        "maker_fee": live_params["maker_fee"],
        "ml_enabled": bool(live_params.get("ml_enabled", True)),
        "model_dir": live_params.get("model_dir", ""),
        "skew_strength": live_params.get("skew_strength", 0.0),
        "vol_blend": live_params.get("vol_blend", 0.0),
        "dir_threshold": live_params.get("dir_threshold", 0.05),
        "asym_strength": live_params.get("asym_strength", 0.0),
        "gamma_dir_bonus": live_params.get("gamma_dir_bonus", 0.0),
        "regime_enabled": live_params.get("regime_enabled", False),
        "vol_baseline": live_params.get("vol_baseline", 3.0),
        "gamma_scale_min": live_params.get("gamma_scale_min", 0.5),
        "gamma_scale_max": live_params.get("gamma_scale_max", 2.0),
        "ret_skew": live_params.get("ret_skew", 0.0),
        "f03_ret_action_horizon_s": live_params.get(
            "f03_ret_action_horizon_s", 0.0
        ),
        "f03_ret_action_compatible": bool(
            live_params.get("f03_ret_action_compatible", False)
        ),
        "taker_fee": live_params.get("taker_fee", 0.0004),
        "max_spread_bps": live_params.get("max_spread_bps", 0.0),
        "replace_min_price_change_ticks": live_params.get("replace_min_price_change_ticks", 0.0),
        "replace_min_price_change_ticks_reducing": live_params.get(
            "replace_min_price_change_ticks_reducing",
            live_params.get("replace_min_price_change_ticks", 0.0),
        ),
        "replace_min_interval_ms": live_params.get("replace_min_interval_ms", 0.0),
        "replace_min_interval_ms_reducing": live_params.get(
            "replace_min_interval_ms_reducing",
            live_params.get("replace_min_interval_ms", 0.0),
        ),
        "replace_pending_coalesce": bool(live_params.get("replace_pending_coalesce", False)),
        "replace_terminal_continuation": bool(
            live_params.get("replace_terminal_continuation", False)
        ),
        "replace_cancel_first_exposure_increasing": bool(
            live_params.get("replace_cancel_first_exposure_increasing", False)
        ),
        "position_timeout": live_params.get("position_timeout", 0.0),
        "kappa_ratio": live_params.get("kappa_ratio", 1.0),
        "queue_depth": live_params.get("queue_depth", 0.0),
        "eta": live_params.get("eta", 0.0),
        "exit_urgency_strength": live_params.get("exit_urgency_strength", 0.0),
        "circuit_breaker_sigma": live_params.get("circuit_breaker_sigma", 0.0),
        "pnl_volatility_horizon_s": live_params.get("pnl_volatility_horizon_s", 300.0),
        "max_exec_book_visible_age_s": live_params.get("max_exec_book_visible_age_s", 5.0),
        "max_exec_book_source_lag_s": live_params.get("max_exec_book_source_lag_s", 5.0),
        "inventory_skew_strength": live_params.get("inventory_skew_strength", 0.0),
        "inventory_asym_strength": live_params.get("inventory_asym_strength", 0.0),
        "inventory_signal_fade_strength": live_params.get("inventory_signal_fade_strength", 0.0),
        "lot_size": live_params.get("lot_size", 0.001),
        "book_imb_strength": live_params.get("book_imb_strength", 0.0),
        "rq_min": live_params.get("rq_min", live_params.get("requote_interval", 10.0)),
        "rq_max": live_params.get("rq_max", live_params.get("requote_interval", 10.0)),
        "liq_baseline": live_params.get("liq_baseline", 200.0),
        "gamma_liq_scale_min": live_params.get("gamma_liq_scale_min", 0.5),
        "gamma_liq_scale_max": live_params.get("gamma_liq_scale_max", 3.0),
        "p3_delta_star": p3_delta_star,
        "p3_kappa_eff": p3_kappa_eff,
        "kappa_depth_baseline": live_params.get("kappa_depth_baseline", 50.0),
        "thin_depth_threshold": live_params.get("thin_depth_threshold", 0.0),
        "inv_gamma_enabled": True,
        "fill_dist_decay": live_params.get("fill_dist_decay", 0.0),
        "ret_shift_max_pct": live_params.get("ret_shift_max_pct", 0.3),
        "ret_demean_halflife": live_params.get("ret_demean_halflife", 0),
        "ber_guard_thresh": live_params.get("ber_guard_thresh", 1.2),
        "ber_spread_mult": live_params.get("ber_spread_mult", 2.0),
        "ber_exposure_add_only": live_params.get("ber_exposure_add_only", False),
        "vol_power": live_params.get("vol_power", 1.5),
        "markout_horizon_s": live_params.get("markout_horizon_s", 10.0),
        "markout_ema_span_fills": live_params.get("markout_ema_span_fills", 50),
        "markout_spread_scale": live_params.get("markout_spread_scale", 0.2),
        "markout_side_asymmetry_sign": live_params.get("markout_side_asymmetry_sign", 1.0),
        "spread_cap_mode": live_params.get("spread_cap_mode", "pause_exposure"),
        "adverse_markout_pause_hybrid": live_params.get("adverse_markout_pause_hybrid", False),
        "adverse_markout_pause_base_s": live_params.get("adverse_markout_pause_base_s", 120.0),
        "adverse_markout_pause_min_s": live_params.get("adverse_markout_pause_min_s", 120.0),
        "adverse_markout_pause_max_s": live_params.get("adverse_markout_pause_max_s", 900.0),
        "adverse_markout_decay_tau_s": live_params.get("adverse_markout_decay_tau_s", 0.0),
        "adverse_markout_max_resolve_gap_s": live_params.get(
            "adverse_markout_max_resolve_gap_s", 30.0
        ),
        "urgency_time_weight": live_params.get("urgency_time_weight", 0.3),
        "urgency_pnl_weight": live_params.get("urgency_pnl_weight", 0.3),
        "urgency_signal_weight": live_params.get("urgency_signal_weight", 0.4),
        "toxicity_horizon_s": live_params.get("toxicity_horizon_s", 10),
        "fill_cooldown": live_params.get("fill_cooldown", 0.0),
        "fill_cooldown_consecutive_reset_policy": live_params.get(
            "fill_cooldown_consecutive_reset_policy", ""
        ),
        "fill_cooldown_apply_reducing": live_params.get("fill_cooldown_apply_reducing", False),
        "fill_cooldown_reducing": live_params.get("fill_cooldown_reducing", 0.0),
        "fill_cooldown_reducing_campaign_only": live_params.get(
            "fill_cooldown_reducing_campaign_only", False
        ),
        "fill_cooldown_reducing_inv_threshold": live_params.get(
            "fill_cooldown_reducing_inv_threshold", 0.0
        ),
        "fill_cooldown_reducing_inv_ratio": live_params.get(
            "fill_cooldown_reducing_inv_ratio", 0.0
        ),
        "fill_cooldown_reducing_age_s": live_params.get("fill_cooldown_reducing_age_s", 0.0),
        "fill_cooldown_reducing_vol_ref": live_params.get("fill_cooldown_reducing_vol_ref", 0.0),
        "fill_cooldown_reducing_vol_min_mult": live_params.get(
            "fill_cooldown_reducing_vol_min_mult", 0.5
        ),
        "fill_cooldown_reducing_vol_max_mult": live_params.get(
            "fill_cooldown_reducing_vol_max_mult", 2.0
        ),
        # Campaign/lifecycle research controls.  These are not promoted live
        # switches by default, but the Python replay can evaluate them as
        # shadow arms inside the same current-config baseline.
        "campaign_stop_add_enabled": bool(live_params.get("campaign_stop_add_enabled", False)),
        "campaign_stop_add_inv_threshold": live_params.get("campaign_stop_add_inv_threshold", 0.0),
        "campaign_stop_add_age_s": live_params.get("campaign_stop_add_age_s", 0.0),
        "campaign_soft_control_enabled": bool(
            live_params.get("campaign_soft_control_enabled", False)
        ),
        "campaign_soft_inv_threshold": live_params.get("campaign_soft_inv_threshold", 0.0),
        "campaign_soft_age_s": live_params.get("campaign_soft_age_s", 0.0),
        "campaign_soft_spread_mult": live_params.get("campaign_soft_spread_mult", 1.0),
        "campaign_soft_gate_enabled": bool(live_params.get("campaign_soft_gate_enabled", False)),
        "campaign_soft_gate_campaign_inv_ref": live_params.get(
            "campaign_soft_gate_campaign_inv_ref", 0.006
        ),
        "campaign_soft_gate_campaign_age_ref_s": live_params.get(
            "campaign_soft_gate_campaign_age_ref_s", 3600.0
        ),
        "campaign_soft_gate_trend_ret_ref": live_params.get(
            "campaign_soft_gate_trend_ret_ref", 2e-5
        ),
        "campaign_soft_gate_refill_ref": live_params.get("campaign_soft_gate_refill_ref", 0.10),
        "campaign_soft_gate_campaign_score": live_params.get(
            "campaign_soft_gate_campaign_score", 1.0
        ),
        "campaign_soft_gate_trend_score": live_params.get("campaign_soft_gate_trend_score", 1.0),
        "campaign_soft_gate_refill_edge_max": live_params.get(
            "campaign_soft_gate_refill_edge_max", 0.0
        ),
        "campaign_soft_gate_reversion_max": live_params.get(
            "campaign_soft_gate_reversion_max", 0.5
        ),
        "campaign_soft_gate_side": live_params.get("campaign_soft_gate_side", "BOTH"),
        "adaptive_add_cooldown_enabled": bool(
            live_params.get("adaptive_add_cooldown_enabled", False)
        ),
        "adaptive_add_cooldown_min_mult": live_params.get("adaptive_add_cooldown_min_mult", 0.5),
        "adaptive_add_cooldown_max_mult": live_params.get("adaptive_add_cooldown_max_mult", 2.5),
        "adaptive_add_cooldown_w_markout": live_params.get("adaptive_add_cooldown_w_markout", 0.0),
        "adaptive_add_cooldown_w_flow": live_params.get("adaptive_add_cooldown_w_flow", 0.0),
        "adaptive_add_cooldown_w_campaign": live_params.get(
            "adaptive_add_cooldown_w_campaign", 0.0
        ),
        "adaptive_add_cooldown_w_trend": live_params.get("adaptive_add_cooldown_w_trend", 0.0),
        "adaptive_add_cooldown_w_refill_weak": live_params.get(
            "adaptive_add_cooldown_w_refill_weak", 0.0
        ),
        "adaptive_add_cooldown_w_refill_good": live_params.get(
            "adaptive_add_cooldown_w_refill_good", 0.0
        ),
        "adaptive_add_cooldown_w_reversion": live_params.get(
            "adaptive_add_cooldown_w_reversion", 0.0
        ),
        "adaptive_add_cooldown_mo_ref": live_params.get("adaptive_add_cooldown_mo_ref", 50.0),
        "adaptive_add_cooldown_flow_ref": live_params.get("adaptive_add_cooldown_flow_ref", 2.0),
        "adaptive_add_cooldown_campaign_inv_ref": live_params.get(
            "adaptive_add_cooldown_campaign_inv_ref", 0.006
        ),
        "adaptive_add_cooldown_campaign_age_ref_s": live_params.get(
            "adaptive_add_cooldown_campaign_age_ref_s", 3600.0
        ),
        "adaptive_add_cooldown_trend_ret_ref": live_params.get(
            "adaptive_add_cooldown_trend_ret_ref", 2e-5
        ),
        "adaptive_add_cooldown_refill_ref": live_params.get(
            "adaptive_add_cooldown_refill_ref", 0.10
        ),
        "adaptive_add_cooldown_reversion_ref": live_params.get(
            "adaptive_add_cooldown_reversion_ref", 1.0
        ),
        "adaptive_add_cooldown_gate_enabled": bool(
            live_params.get("adaptive_add_cooldown_gate_enabled", False)
        ),
        "adaptive_add_cooldown_gate_mult": live_params.get("adaptive_add_cooldown_gate_mult", 1.75),
        "adaptive_add_cooldown_gate_campaign_score": live_params.get(
            "adaptive_add_cooldown_gate_campaign_score", 1.0
        ),
        "adaptive_add_cooldown_gate_trend_score": live_params.get(
            "adaptive_add_cooldown_gate_trend_score", 1.0
        ),
        "adaptive_add_cooldown_gate_refill_edge_max": live_params.get(
            "adaptive_add_cooldown_gate_refill_edge_max", 0.0
        ),
        "adaptive_add_cooldown_gate_reversion_max": live_params.get(
            "adaptive_add_cooldown_gate_reversion_max", 0.5
        ),
        "adaptive_add_cooldown_gate_side": live_params.get(
            "adaptive_add_cooldown_gate_side", "BOTH"
        ),
        "symmetric_size": live_params.get("symmetric_size", False),
        # Exogenous sync-degrade transitions require a frozen environment tape
        # for promotion evidence. Censor/stress modes are explicit diagnostics.
        "sync_adjust_degrade_enabled": bool(live_params.get("sync_adjust_degrade_enabled", False)),
        "sync_adjust_degrade_count": int(live_params.get("sync_adjust_degrade_count", 0) or 0),
        "sync_adjust_abs_qty_threshold": float(
            live_params.get("sync_adjust_abs_qty_threshold", 0.0) or 0.0
        ),
        "sync_adjust_degrade_window_s": float(
            live_params.get("sync_adjust_degrade_window_s", 0.0) or 0.0
        ),
        "sync_adjust_pause_s": float(live_params.get("sync_adjust_pause_s", 0.0) or 0.0),
        "sync_adjust_reconnect_user_stream": bool(
            live_params.get("sync_adjust_reconnect_user_stream", False)
        ),
        "sync_adjust_cancel_orders": bool(live_params.get("sync_adjust_cancel_orders", False)),
        "sync_adjust_replay_mode": "disabled",
        "sync_adjust_event_tape_path": "",
        "sync_adjust_event_tape_sha256": "",
        "sync_adjust_event_environment": "",
        "sync_adjust_semantics": SYNC_DEGRADE_SEMANTICS,
        "sync_adjust_stress_seed": 20260729,
        "sync_adjust_stress_interval_s": 21_600.0,
        "max_consecutive_losses": int(live_params.get("max_consecutive_losses", 0) or 0),
        "cooldown_after_loss": float(live_params.get("cooldown_after_loss", 0.0) or 0.0),
        "consecutive_loss_cooldown_semantics": LOSS_COOLDOWN_SEMANTICS,
        "buy_fill_selection_live_enabled": bool(
            live_params.get("buy_fill_selection_live_enabled", False)
        ),
        "buy_fill_selection_live_model_path": live_params.get(
            "buy_fill_selection_live_model_path", ""
        ),
        "buy_fill_selection_live_score_threshold": float(
            live_params.get("buy_fill_selection_live_score_threshold", 0.50) or 0.50
        ),
        "buy_fill_selection_live_spread_mult_cap": float(
            live_params.get("buy_fill_selection_live_spread_mult_cap", 1.0) or 1.0
        ),
        "buy_fill_selection_live_apply_reducing": bool(
            live_params.get("buy_fill_selection_live_apply_reducing", False)
        ),
        "buy_fill_selection_live_max_missing_features": int(
            live_params.get("buy_fill_selection_live_max_missing_features", 99) or 99
        ),
        # Python native-deep replay implements the full BUY q90 cancel/ACK/
        # recovery/re-entry path. C++ fails fast because it does not consume
        # the strategy-independent snapshot/delta tape.
        "dynamic_fill_hazard_shadow_enabled": bool(
            live_params.get("dynamic_fill_hazard_shadow_enabled", False)
        ),
        "dynamic_fill_hazard_shadow_model_path": live_params.get(
            "dynamic_fill_hazard_shadow_model_path", ""
        ),
        "dynamic_fill_hazard_shadow_model_sha256": live_params.get(
            "dynamic_fill_hazard_shadow_model_sha256", ""
        ),
        "dynamic_fill_hazard_shadow_sides": live_params.get(
            "dynamic_fill_hazard_shadow_sides", "BUY"
        ),
        "dynamic_fill_hazard_shadow_exposure_ms": float(
            live_params.get("dynamic_fill_hazard_shadow_exposure_ms", 100.0) or 100.0
        ),
        "dynamic_fill_hazard_shadow_price_jump_ticks": float(
            live_params.get(
                "dynamic_fill_hazard_shadow_price_jump_ticks",
                1.0,
            )
            or 1.0
        ),
        "dynamic_fill_hazard_action_enabled": bool(
            live_params.get("dynamic_fill_hazard_action_enabled", False)
        ),
        "dynamic_fill_hazard_action_policy_path": live_params.get(
            "dynamic_fill_hazard_action_policy_path", ""
        ),
        "dynamic_fill_hazard_action_policy_sha256": live_params.get(
            "dynamic_fill_hazard_action_policy_sha256", ""
        ),
    }
    for name in (
        "eta_inventory",
        "a_spread",
        "risk_per_order",
        "execution_intensity_slope",
        "risk_horizon_s",
        "trade_intensity_acceleration_spread_mult",
        "cara_risk_aversion",
    ):
        raw_value = live_params.get(name)
        if raw_value is not None:
            params[name] = finite_positive_quote_coefficient(name, raw_value)
    _apply_order_transport(params, live_params)
    # This is a replay execution override, not a live strategy/YAML field.
    if "replay_main_loop_sleep_ms" in live_params:
        params["replay_main_loop_sleep_ms"] = live_params["replay_main_loop_sleep_ms"]
    params.update({
        key: live_params[key] for key in COOLDOWN_POLICY_PARAM_KEYS if key in live_params
    })
    if queue_calibration is not None:
        # queue calibration 是机制对齐工具，不是 alpha；正式 OOS 评估必须用 fit 以外日期验证。
        params["_queue_calibration"] = queue_calibration
    return params


def _apply_order_transport(params: dict[str, Any], live_params: Mapping[str, Any]) -> None:
    """Project live scheduling once for both raw-config and built-params callers."""
    if live_params.get("order_transport", "rest") != "rest":
        raise ValueError("replay currently models REST only, not a WebSocket order gateway")
    if live_params.get("cross_side_order_lanes_enabled", False):
        raise ValueError("cross-side concurrent order lanes are not implemented in replay")
    if not live_params.get("async_order_lanes_enabled", False):
        return
    capacity = live_params.get("async_order_lane_capacity", 8)
    if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
        raise ValueError("async_order_lane_capacity must be a positive integer")
    if params.get("rest_gateway_timing_mode", "sampled_async_fifo") != "sampled_async_fifo":
        raise ValueError("async live configuration requires sampled_async_fifo replay")
    params.update(
        order_transport="rest",
        async_order_lanes_enabled=True,
        cross_side_order_lanes_enabled=False,
        async_order_lane_capacity=capacity,
        rest_gateway_timing_mode="sampled_async_fifo",
    )
    params.setdefault("replay_event_clock", "merged")
    params.setdefault("replay_clock_interval_ms", 100)
    params.setdefault("replay_main_loop_sleep_ms", 100)


def apply_tick_defaults(
    params: dict[str, Any], *, require_historical_bbo: bool | None = None
) -> dict[str, Any]:
    """Apply tick-replay defaults that are not explicit in older live configs."""
    _apply_order_transport(params, params)
    for key, value in TICK_DEFAULTS.items():
        params.setdefault(key, value)
    params.setdefault("dynamic_cap_base_bps", params.get("max_spread_bps", 0.0))
    params["queue_base"] = float(params.get("queue_base", 5.0))
    params["queue_decay"] = float(params.get("queue_decay", 0.1))
    if require_historical_bbo is not None:
        params["require_historical_bbo"] = bool(require_historical_bbo)
    return params


def apply_cli_overrides(
    params: dict[str, Any],
    args: argparse.Namespace,
    mapping: Mapping[str, str],
) -> dict[str, Any]:
    """Apply explicitly provided argparse values to a backtest parameter dict."""
    for cli_name, param_name in mapping.items():
        val = getattr(args, cli_name.replace("-", "_"), None)
        if val is not None:
            params[param_name] = val
    return params


def disable_ml_params(params: dict[str, Any]) -> dict[str, Any]:
    for param_name in ML_PARAM_KEYS:
        params[param_name] = 0.0
    return params


def add_fill_probability_params(
    params: dict[str, Any],
    *,
    model_path: Path,
    label: str = "P3",
    strict: bool = False,
) -> dict[str, Any]:
    params["p3_kappa_eff"] = 0.0
    params["fill_probability_calibrated"] = False
    params["fill_probability_model_path"] = str(model_path)
    params["fill_probability_schema_version"] = ""
    params["fill_probability_model_type"] = ""
    params["fill_probability_event_type"] = ""
    params["fill_probability_horizon_s"] = 0.0
    params["fill_probability_distance_origin"] = ""
    params["fill_probability_distance_unit"] = ""
    params["fill_probability_side"] = ""
    params["fill_probability_queue_included"] = None
    params["fill_probability_artifact_sha256"] = ""
    params["p3_identity_required"] = bool(strict)
    try:
        try:
            from research.families.f02_empirical_p3_touch.fill_probability import (
                FillProbabilityModel,
            )
        except ImportError:
            from fill_probability import FillProbabilityModel

        fill_model = FillProbabilityModel.load(model_path)
        p3_identity = (
            fill_model.semantic_identity(require_artifact_hash=True)
            if fill_model.model_type == "empirical_survival"
            else {
                "event_type": "",
                "horizon_s": 0.0,
                "distance_origin": "",
                "distance_unit": "",
                "side": "",
                "queue_included": None,
                "artifact_sha256": "",
            }
        )
        params["p3_delta_star"] = fill_model.optimal_delta()
        p3_kappa_eff = fill_model.effective_kappa()
        if p3_kappa_eff > 0:
            params["p3_kappa_eff"] = p3_kappa_eff
        if params["p3_delta_star"] <= 0.0 or params["p3_kappa_eff"] <= 0.0:
            raise ValueError(f"{label} calibration must provide positive delta_star and kappa_eff")
        params["fill_probability_calibrated"] = True
        params["fill_probability_schema_version"] = str(fill_model.schema_version)
        params["fill_probability_model_type"] = str(fill_model.model_type)
        params["fill_probability_event_type"] = str(p3_identity["event_type"])
        params["fill_probability_horizon_s"] = float(p3_identity["horizon_s"])
        params["fill_probability_distance_origin"] = str(
            p3_identity["distance_origin"]
        )
        params["fill_probability_distance_unit"] = str(p3_identity["distance_unit"])
        params["fill_probability_side"] = str(p3_identity["side"])
        params["fill_probability_queue_included"] = bool(
            p3_identity["queue_included"]
        )
        params["fill_probability_artifact_sha256"] = str(p3_identity["artifact_sha256"])
        print(
            f"  {label}: delta_star={params['p3_delta_star']:.4f}, "
            f"kappa_eff={params['p3_kappa_eff']:.4f}, path={model_path}"
        )
    except Exception as exc:
        if strict:
            raise RuntimeError(
                f"{label} fill calibration unavailable: {model_path}: {exc}"
            ) from exc
        # 探索性入口保持历史 fail-open 行为；formal replay 使用 strict=True。
        print(f"  [WARN] {label} fill model unavailable: {exc}")
        params["p3_delta_star"] = 0.0
        params["p3_kappa_eff"] = 0.0
    return params


def add_queue_calibration_params(
    params: dict[str, Any],
    *,
    symbol: str,
    strict: bool = False,
    path: str | Path | None = None,
) -> dict[str, Any]:
    path = Path(path).expanduser().resolve() if path is not None else calibration_path(symbol)
    queue_calibration = load_daily_queue_calibration(
        symbol=symbol,
        path=path,
    )
    params["queue_calibration_path"] = str(path)
    params["queue_calibration_loaded"] = False
    params["queue_calibration_day_count"] = 0
    params["queue_calibration_schema_version"] = ""
    params["queue_calibration_apply_mode"] = ""
    params["queue_calibration_fit_days"] = []
    params["queue_calibration_replay_params"] = {}
    params["queue_calibration_diagnostic_only"] = False
    params["queue_calibration_diagnostic_parent_sha256"] = ""
    params["queue_calibration_diagnostic_note"] = ""
    if queue_calibration.get("days"):
        params["_queue_calibration"] = queue_calibration
        params["queue_calibration_loaded"] = True
        params["queue_calibration_day_count"] = len(queue_calibration["days"])
        params["queue_calibration_schema_version"] = str(
            queue_calibration.get("schema_version", "")
        )
        params["queue_calibration_apply_mode"] = str(queue_calibration.get("apply_mode", ""))
        params["queue_calibration_fit_days"] = list(queue_calibration.get("fit_days") or [])
        params["queue_calibration_diagnostic_only"] = bool(
            queue_calibration.get("diagnostic_only", False)
        )
        params["queue_calibration_diagnostic_parent_sha256"] = str(
            queue_calibration.get("diagnostic_parent_sha256", "") or ""
        )
        params["queue_calibration_diagnostic_note"] = str(
            queue_calibration.get("diagnostic_note", "") or ""
        )
        replay_params = dict(queue_calibration.get("replay_params") or {})
        params["queue_calibration_replay_params"] = replay_params
        for key, value in replay_params.items():
            if key.startswith("queue_"):
                params[key] = max(0.0, float(value))
        print(f"  Queue calibration loaded: {len(queue_calibration['days'])} days")
    elif strict:
        raise RuntimeError(f"Queue calibration has no usable days: {path}")
    return params


def validate_formal_replay_calibration(
    params: dict[str, Any],
    *,
    require_latency: bool = True,
) -> dict[str, Any]:
    """Fail fast when a replay is being used as formal promotion evidence."""
    errors: list[str] = []
    if not bool(params.get("_config_explicit", False)):
        errors.append("an explicit non-template --config is required")
    config_path = Path(str(params.get("_config_path", "") or ""))
    if config_path.exists():
        try:
            config_text = config_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            config_text = ""
        if "PUBLIC TEMPLATE" in config_text.upper():
            errors.append(f"public template config is not a formal baseline: {config_path}")

    if not bool(params.get("fill_probability_calibrated", False)):
        errors.append("fill-probability/effective-kappa calibration is missing")
    if params.get("fill_probability_schema_version") != "narrowgate_p3_touch_calibration.v2":
        errors.append("formal replay requires causal exact-tick P3 calibration v2")
    if params.get("fill_probability_model_type") != "empirical_survival":
        errors.append("formal replay requires empirical P3 survival calibration")
    if params.get("fill_probability_event_type") != "touch":
        errors.append("formal replay requires event_type=touch for P3")
    if not math.isclose(
        float(params.get("fill_probability_horizon_s", 0.0) or 0.0),
        10.0,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        errors.append("formal replay requires P3 horizon_s=10")
    if params.get("fill_probability_distance_unit") != "USDC_per_BTC":
        errors.append("formal replay requires P3 distance_unit=USDC_per_BTC")
    if (
        params.get("fill_probability_distance_origin")
        != "same_side_best_bid_or_ask_at_window_start"
    ):
        errors.append("formal replay requires the same-side-BBO origin")
    if params.get("fill_probability_side") != "pooled_buy_sell":
        errors.append("formal replay requires pooled BUY/SELL identity")
    if params.get("fill_probability_queue_included") is not False:
        errors.append("formal replay requires queue_included=false")
    p3_sha256 = str(params.get("fill_probability_artifact_sha256", "") or "")
    if len(p3_sha256) != 64 or any(ch not in "0123456789abcdef" for ch in p3_sha256):
        errors.append("formal replay requires the exact P3 artifact SHA256")
    p3_path = Path(str(params.get("fill_probability_model_path", "") or ""))
    if not p3_path.is_file():
        errors.append("formal replay requires the exact P3 artifact file")
    elif hashlib.sha256(p3_path.read_bytes()).hexdigest() != p3_sha256:
        errors.append("P3 artifact SHA256 does not match the executed file")
    if float(params.get("p3_delta_star", 0.0) or 0.0) <= 0.0:
        errors.append("p3_delta_star must be positive")
    if float(params.get("p3_kappa_eff", 0.0) or 0.0) <= 0.0:
        errors.append("p3_kappa_eff must be positive")
    if not bool(params.get("queue_calibration_loaded", False)):
        errors.append("daily queue calibration is missing")
    if params.get("queue_calibration_schema_version") != "narrowgate_queue_calibration.v3":
        errors.append("formal replay requires queue calibration v3")
    if params.get("queue_calibration_apply_mode") != "frozen_default":
        errors.append("formal replay requires frozen-default queue calibration")
    if not params.get("queue_calibration_fit_days"):
        errors.append("queue calibration fit-day identity is missing")
    queue_replay_params = params.get("queue_calibration_replay_params") or {}
    required_queue_params = {
        "queue_ahead_base_mult",
        "queue_deplete_base_mult",
        "queue_ahead_buy_exposure_mult",
        "queue_ahead_buy_reducing_mult",
        "queue_ahead_sell_exposure_mult",
        "queue_ahead_sell_reducing_mult",
    }
    if not required_queue_params.issubset(queue_replay_params):
        errors.append("queue calibration v3 replay parameter identity is incomplete")
    for key in sorted(required_queue_params):
        if key not in queue_replay_params:
            continue
        artifact_value = float(queue_replay_params[key])
        actual_value = float(params.get(key, math.nan))
        if not math.isfinite(actual_value) or not math.isclose(
            actual_value, artifact_value, rel_tol=0.0, abs_tol=1e-12
        ):
            errors.append(
                f"final replay parameter {key}={actual_value!r} does not match "
                f"queue artifact value {artifact_value!r}"
            )
    if not bool(params.get("require_historical_bbo", False)):
        errors.append("require_historical_bbo must be enabled")
    if str(params.get("replay_event_clock", "trade") or "trade").lower() != "merged":
        errors.append("formal replay requires replay_event_clock=merged")
    if int(params.get("replay_clock_interval_ms", 0) or 0) <= 0:
        errors.append("replay_clock_interval_ms must be positive")
    if (
        str(params.get("circuit_breaker_exit_mode", "maker_close") or "maker_close").lower()
        != "maker_close"
    ):
        errors.append("formal replay requires circuit_breaker_exit_mode=maker_close")

    model_dir = _resolve_project_path(params.get("resolved_model_dir") or params.get("model_dir"))
    if model_dir is not None:
        meta_paths = sorted(
            path for path in model_dir.glob("*_meta.json") if path.name != "bundle_meta.json"
        )
        if not meta_paths:
            errors.append(f"ML model metadata is missing: {model_dir}")
        target_meta_count = 0
        for meta_path in meta_paths:
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                errors.append(f"invalid ML metadata {meta_path.name}: {exc}")
                continue
            # Only per-target artifacts own feature visibility metadata.
            if not isinstance(meta.get("feature_cols"), list):
                continue
            target_meta_count += 1
            if meta.get("feature_timestamp_semantics") != "left_label_bucket_end":
                errors.append(
                    f"ML model lacks bucket-end feature visibility contract: {meta_path.name}"
                )
            if int(meta.get("feature_bucket_ms", 0) or 0) != 10_000:
                errors.append(f"ML feature bucket contract is not 10s: {meta_path.name}")
            if int(meta.get("label_semantics_version", 0) or 0) < 2:
                errors.append(f"ML label horizon semantics are not explicit: {meta_path.name}")
            if not str(meta.get("feature_manifest_sha256", "") or ""):
                errors.append(f"ML feature manifest identity is missing: {meta_path.name}")
            if not str(meta.get("feature_daily_manifest_sha256", "") or ""):
                errors.append(f"ML daily feature manifest identity is missing: {meta_path.name}")
            availability = meta.get("feature_availability_train")
            if not isinstance(availability, dict) or not availability:
                errors.append(f"ML feature availability audit is missing: {meta_path.name}")
            elif any(float(value or 0.0) <= 0.0 for value in availability.values()):
                errors.append(f"ML metadata contains zero-support feature: {meta_path.name}")
        if meta_paths and target_meta_count == 0:
            errors.append(f"per-target ML metadata is missing: {model_dir}")

    if require_latency:
        new_samples = params.get("_new_order_latency_samples_ms", ())
        cancel_samples = params.get("_cancel_order_latency_samples_ms", ())

        def has_positive_sample(values: Any) -> bool:
            if values is None:
                return False
            try:
                iterator = iter(values)
            except TypeError:
                iterator = iter((values,))
            for value in iterator:
                try:
                    sample = float(value)
                except (TypeError, ValueError):
                    continue
                if math.isfinite(sample) and sample > 0.0:
                    return True
            return False

        empirical_latency = has_positive_sample(new_samples) and has_positive_sample(cancel_samples)
        paired_samples = rest_return_sample_rows(params)
        if paired_samples is not None:
            # The async runtime consumes these same-request triples directly.
            # Do not fabricate old one-dimensional latency arrays to satisfy
            # an availability check for an obsolete consumer.
            empirical_latency = all(
                has_positive_sample(rows[:, 1]) and has_positive_sample(rows[:, 2])
                for rows in paired_samples.values()
            )
        static_latency = (
            float(params.get("new_order_latency_ms", 0.0) or 0.0) > 0.0
            and float(params.get("cancel_order_latency_ms", 0.0) or 0.0) > 0.0
        )
        if not empirical_latency and not static_latency:
            errors.append("empirical or non-zero new/cancel latency calibration is missing")

    if errors:
        raise RuntimeError("Strict calibration failed: " + "; ".join(errors))
    params["strict_calibration"] = True
    params["strict_calibration_validated"] = True
    params["strict_calibration_require_latency"] = bool(require_latency)
    return params


def _private_mechanics_boundary_marker() -> None:
    """Owner mechanics resolvers are distributed only with the private repository."""
    return None


def load_tick_base_params(
    *,
    symbol: str | None = None,
    config_path: str | Path | None = None,
    locator_projection_path: str | Path | None = None,
    configure_symbol: Callable[..., Any] | None = None,
    require_historical_bbo: bool | None = None,
    queue_base: float | None = None,
    queue_decay: float | None = None,
    queue_ahead_mode: str | None = None,
    queue_price_tolerance: float | None = None,
    min_historical_book_coverage: float | None = None,
    maker_fill_prob: float | None = None,
    include_fill_probability: bool = True,
    include_queue_calibration: bool = True,
    queue_calibration_path: str | Path | None = None,
    strict_calibration: bool = False,
) -> dict[str, Any]:
    """Load live config and attach common tick-replay calibration artifacts."""
    if locator_projection_path is not None and config_path is None:
        raise ValueError("replay locator projection requires an explicit config path")
    env_config_explicit = bool(str(os.environ.get("MM_LIVE_CONFIG", "") or "").strip())
    explicit_selection = config_path is not None or env_config_explicit
    baseline_binding: dict[str, Any] | None = None
    if not explicit_selection:
        baseline_binding = load_operational_baseline_binding()
        if baseline_binding is not None and bool(baseline_binding["config_exists"]):
            resolved_config_path = Path(baseline_binding["config_path"])
            params = _verified_binding_config_params(baseline_binding)
        else:
            resolved_config_path = resolve_backtest_config_path(config_path)
            params = load_live_config_as_params(resolved_config_path)
    else:
        resolved_config_path = resolve_backtest_config_path(config_path)
        params = (
            load_live_config_as_params(resolved_config_path)
            if locator_projection_path is None
            else load_live_config_as_params(
                resolved_config_path, locator_projection_path=locator_projection_path,
            )
        )
        if resolved_config_path in operational_baseline_config_candidates():
            baseline_binding = load_operational_baseline_binding()
    bound_to_operational_baseline = bool(
        baseline_binding is not None
        and baseline_binding["config_exists"]
        and resolved_config_path == baseline_binding["config_path"]
    )
    params["_config_path"] = str(resolved_config_path)
    params["_config_explicit"] = bool(
        config_path is not None or env_config_explicit or bound_to_operational_baseline
    )
    runtime_code_audit = (
        baseline_binding.get("runtime_code_audit", {}) if baseline_binding is not None else {}
    )
    runtime_code_matches = runtime_code_audit.get("matches")
    params["_config_source"] = (
        (
            "operational_baseline_pointer"
            if runtime_code_matches is not False
            else "operational_baseline_config_runtime_overlay"
        )
        if bound_to_operational_baseline
        else ("explicit" if params["_config_explicit"] else "public_template")
    )
    if bound_to_operational_baseline and baseline_binding is not None:
        baseline_id = str(baseline_binding["pointer"]["baseline_id"])
        params["operational_baseline_runtime_code_declared"] = bool(
            runtime_code_audit.get("declared", False)
        )
        params["operational_baseline_runtime_code_match"] = runtime_code_matches
        params["operational_baseline_runtime_workspace_sha256"] = str(
            runtime_code_audit.get("workspace_sha256", "")
        )
        params["operational_baseline_runtime_mismatched_paths"] = sorted(
            runtime_code_audit.get("mismatched_paths", {})
        )
        params["operational_baseline_runtime_missing_paths"] = list(
            runtime_code_audit.get("missing_paths", [])
        )
        if runtime_code_matches is False:
            params["operational_baseline_config_id"] = baseline_id
            params["operational_baseline_runtime_overlay"] = True
        else:
            params["operational_baseline_id"] = baseline_id
            params["operational_baseline_runtime_overlay"] = False
        params["operational_baseline_pointer_path"] = str(baseline_binding["pointer_path"])
        params["operational_baseline_pointer_sha256"] = str(baseline_binding["pointer_sha256"])
        params["operational_baseline_identity_path"] = str(baseline_binding["identity_path"])
        params["operational_baseline_identity_sha256"] = str(baseline_binding["identity_sha256"])
        replay_baseline_path = baseline_binding.get("replay_baseline_path")
        if replay_baseline_path is not None:
            params["operational_replay_baseline_path"] = str(replay_baseline_path)
            params["operational_replay_baseline_sha256"] = str(
                baseline_binding["pointer"]["backtest_replay_baseline_sha256"]
            )
        control_arm = str(baseline_binding["pointer"]["backtest_control_arm"])
        params["backtest_control_arm"] = (
            f"{control_arm}__runtime_overlay" if runtime_code_matches is False else control_arm
        )
    params["strict_calibration"] = bool(strict_calibration)
    params["strict_calibration_validated"] = False
    resolved_symbol = (symbol or params.get("symbol") or "").upper()
    environment_model = os.environ.get("MM_MODEL_DIR")
    if locator_projection_path is not None and environment_model and (
        _resolve_project_path(environment_model) != _resolve_project_path(params.get("model_dir"))
    ):
        raise ValueError("MM_MODEL_DIR conflicts with the selected replay locator projection")
    model_dir_override = _resolve_project_path(environment_model or params.get("model_dir"))
    if model_dir_override is not None:
        params["model_dir"] = str(model_dir_override)
        params["resolved_model_dir"] = str(model_dir_override)
    if configure_symbol is not None:
        if model_dir_override is not None:
            configure_symbol(resolved_symbol or None, model_dir_override=model_dir_override)
        else:
            configure_symbol(resolved_symbol or None)
    if resolved_symbol:
        params["symbol"] = resolved_symbol

    apply_tick_defaults(params, require_historical_bbo=require_historical_bbo)
    if strict_calibration:
        params["replay_event_clock"] = "merged"
    if queue_base is not None:
        params["queue_base"] = float(queue_base)
    if queue_decay is not None:
        params["queue_decay"] = float(queue_decay)
    if queue_ahead_mode is not None:
        params["queue_ahead_mode"] = queue_ahead_mode
    if queue_price_tolerance is not None:
        params["queue_price_tolerance"] = float(queue_price_tolerance)
    if min_historical_book_coverage is not None:
        params["min_historical_book_coverage"] = min_historical_book_coverage
    if maker_fill_prob is not None:
        params["maker_fill_prob"] = maker_fill_prob

    model_dir_for_artifacts = model_dir_override or symbol_model_dir(resolved_symbol or None)
    model_path = model_dir_for_artifacts / "fill_prob_params.json"
    if include_fill_probability:
        add_fill_probability_params(params, model_path=model_path, strict=strict_calibration)
        p3_kappa_eff_override = float(
            params.get("p3_kappa_eff_override", 0.0) or 0.0
        )
        if not math.isfinite(p3_kappa_eff_override) or p3_kappa_eff_override != 0.0:
            raise ValueError(
                "nonzero p3_kappa_eff_override cannot inherit the empirical "
                "touch artifact identity"
            )
    if include_queue_calibration and resolved_symbol:
        add_queue_calibration_params(
            params,
            symbol=resolved_symbol,
            strict=strict_calibration,
            path=queue_calibration_path,
        )
    return params
