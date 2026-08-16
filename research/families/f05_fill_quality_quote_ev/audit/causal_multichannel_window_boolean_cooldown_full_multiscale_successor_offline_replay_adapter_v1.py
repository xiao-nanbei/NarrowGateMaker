"""Canonical offline replay adapter for the F05 full-multiscale successor.

This module is intentionally offline-only.  It binds the successor search
contract and the formal replay protocol, but it does not create a live hook,
observer, companion, or shadow path.  Economic replay fails closed until the
outcome-blind ``replay_inputs`` table contains the complete portable inputs
needed by the existing one-shot and repeated-policy replay bridges.

The adapter also defines deterministic day-level scheduling and an atomic,
hash-bound cache contract.  Every fold keeps an isolated receipt.  An already
admitted outer-train day may be rebound across expanding folds only when the
complete side/day opportunity frame is semantically identical after removing
the two provider-owned fold-scope columns.
"""

from __future__ import annotations

import fcntl
import hashlib
import importlib
import json
import math
import os
import re
import shutil
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal

import pandas as pd

from data_paths import resolve_portable_path
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_cpp_observation_tape_v21 as cpp_observation_tape,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_cpp_runtime_v22 as cpp_runtime_v22,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_features as feature_schema,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_native_observation_batch_v1 as observation_batch,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_predicate_view_v1 as predicate_view,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_repeated_policy_backend_v1 as backend,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 as successor,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_offline_day_input_cache_v1 as day_input_cache,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_runtime_policy as runtime_policy,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_shared_prefix as shared_prefix,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    TriLiteral,
    duration_vocabulary,
)

IDENTITY = backend.CANONICAL_REPLAY_ADAPTER_IDENTITY
MECHANICS_MISSING_STATUS = backend.CANONICAL_FIELDS_BLOCKED_STATUS
REPLAY_ENGINE = "python"
QUEUE_IDENTITY = offline.QUEUE_IDENTITY
SAME_MILLISECOND_AMBIGUITY_POLICY = "censor"
DEFAULT_DAY_WORKERS = 6
MIN_DAY_WORKERS = 1
MAX_DAY_WORKERS = 8
GLOBAL_SEQUENTIAL_WORKER_TOKENS = 10
DEFAULT_GLOBAL_POLICY_DAY_WORKERS = GLOBAL_SEQUENTIAL_WORKER_TOKENS
MAX_GLOBAL_POLICY_DAY_WORKERS = GLOBAL_SEQUENTIAL_WORKER_TOKENS
ONE_SHOT_DAY_PARENT_WORKERS = 0
ONE_SHOT_SUPERVISOR_WORKERS = 0
FORMAL_SHARED_PREFIX_ARM_WORKERS = 10
DAY_INPUT_MATERIALIZATION_WORKERS = 2
ONE_SHOT_TOTAL_WORKER_TOKENS = ONE_SHOT_DAY_PARENT_WORKERS + FORMAL_SHARED_PREFIX_ARM_WORKERS
REQUIRED_ADDITIONAL_CONTEXT_DAYS = (
    "2026-06-29",
    "2026-07-03",
    "2026-07-16",
    "2026-08-06",
)

DAY_CACHE_SCHEMA = f"{IDENTITY}.day_cache.v2"
DAY_PROGRESS_SCHEMA = f"{IDENTITY}.day_progress.v2"
ONE_SHOT_SEMANTIC_CACHE_SCHEMA = f"{IDENTITY}.one_shot_semantic_cache.v1"
B0_CONTROL_CACHE_SCHEMA = f"{IDENTITY}.b0_control_day_cache.v1"
EXECUTOR_ACCELERATION_IDENTITY = "f05_full_multiscale_offline_replay_executor_cpp_one_shot_v3"
DAY_INPUT_CACHE_IDENTITY = "f05_full_multiscale_offline_replay_executor_acceleration_v2"
DAY_INPUT_MMAP_BINDING_SCHEMA = f"{DAY_INPUT_CACHE_IDENTITY}.day_input_mmap.v1"
ONE_SHOT_STAGE = "outer_train_one_shot"
SEQUENTIAL_STAGES = frozenset({"inner_oof", "outer_oof"})
_ONE_SHOT_FOLD_SCOPE_COLUMNS = frozenset({"fold_row_role", "outer_fold_id"})
_GLOBAL_POLICY_DAY_WORKER_ENV = "NARROWGATE_F05_GLOBAL_POLICY_DAY_WORKER"
_GLOBAL_ONE_SHOT_DAY_WORKER_ENV = "NARROWGATE_F05_GLOBAL_ONE_SHOT_DAY_WORKER"

FIXED_ONE_SHOT_REPLAY_MODULE = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "multiscale_ema_boolean_cooldown_duration_policy_study"
)
FIXED_REPEATED_POLICY_BRIDGE_MODULE = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
    "repeated_policy_v1"
)
FIXED_OWNER_FULL_PATH_MODULE = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_owner_full_path_v1"
)
FIXED_BACKTEST_MODULE = "models.backtest_tick"
FIXED_B0_PROJECTION_MODULE = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
    "offline_b0_mechanics_adapter_v1"
)
FIXED_SNAPSHOT_EMITTER_MODULE = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_replay_emitter"
)
FIXED_PANEL_BUILDER_MODULE = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
    "offline_panel_builder_v1"
)
FIXED_OBSERVATION_CACHE_MODULE = (
    "research.families.f05_fill_quality_quote_ev.audit."
    "causal_multichannel_window_boolean_cooldown_native_observation_cache"
)
FIXED_OWNER_PREDICATE_BUNDLE_PATH = (
    "${NARROWGATE_ROOT}/models/private/f05_boolean_cooldown_owner_v1/predicate_bundle.json"
)
FIXED_OWNER_POLICY_PATH = (
    "${NARROWGATE_ROOT}/models/private/f05_boolean_cooldown_owner_v1/policy.json"
)

_FIXED_CANONICAL_API_SYMBOLS: Mapping[str, tuple[str, str]] = {
    "canonical_backtest_tick_arm_executor_binding": (
        FIXED_OWNER_FULL_PATH_MODULE,
        "_simulate_python_arm",
    ),
    "canonical_snapshot_emitter_factory_binding": (
        FIXED_SNAPSHOT_EMITTER_MODULE,
        "CooldownV2ReplayEmitter",
    ),
    "canonical_day_projection_binding": (
        FIXED_B0_PROJECTION_MODULE,
        "_materialize_replay_inputs",
    ),
    "canonical_one_shot_duration_arm_binding": (
        FIXED_ONE_SHOT_REPLAY_MODULE,
        "_run_duration_arm",
    ),
    "canonical_repeated_policy_bridge_binding": (
        FIXED_REPEATED_POLICY_BRIDGE_MODULE,
        "build_target_side_candidate_evaluator",
    ),
}

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_COMMON_REPLAY_COLUMNS = frozenset(
    {
        "utc_day",
        "opportunity_id",
        "side",
        "replay_engine",
        "queue_identity",
        "same_millisecond_ambiguity_policy",
        "exact_owner_policy_sha256",
        "exact_owner_predicate_bundle_sha256",
        "exact_owner_private_config_sha256",
        "exact_owner_action",
        "replay_input_receipt_sha256",
        "economic_outcomes_read",
        "labels_read",
        "candidate_actions_generated",
    }
)
_EXECUTABLE_REPLAY_COLUMNS = frozenset(
    {
        "portable_replay_binding_path",
        "portable_replay_binding_sha256",
        "portable_day_cache_root",
        "day_replay_workers",
        "day_input_sha256",
        "market_window_identity_sha256",
        "model_overlay_identity_sha256",
        "latency_identity_sha256",
        "queue_random_identity_sha256",
        "assignment_ts_ns",
        "observation_end_ts_ns",
        "fill_visible_ts_ms",
        "campaign_id",
        "order_id",
        "exposure_fill_ordinal",
        "baseline_duration_ms",
        "role_at_fill",
        "inventory_after_fill_btc",
        "assignment_equity_usdc",
        "d_plus_1_utc_day",
        "d_plus_1_market_identity_sha256",
        "d_plus_1_feature_identity_sha256",
        "d_plus_1_native_observation_sha256",
        "d_plus_1_context_receipt_sha256",
        "d_plus_1_new_target_assignments_allowed",
        "target_day_end_terminalized",
        "assignment_to_common_washout_required",
    }
)
_LABEL_SCOPE_COLUMNS = frozenset({"outer_fold_id", "fold_row_role"})
_FORBIDDEN_INJECTION_PARTS = (
    "custom_adapter",
    "custom_evaluator",
    "evaluator_callable",
    "executor_callable",
    "precomputed_economic",
    "precomputed_outcome",
    "one_shot_aggregation",
)
_M2_INCREMENTAL_CHANNELS = frozenset(
    spec.name
    for spec in feature_schema.CHANNELS_BY_BLOCK["M2"]
    if spec.name not in {item.name for item in feature_schema.CHANNELS_BY_BLOCK["M1"]}
)
_NON_MID_CHANNELS = frozenset(
    spec.name for spec in feature_schema.CHANNELS_BY_BLOCK["M2"] if spec.name != "mid_usdc_per_btc"
)
_E2_SEMANTIC_TOKENS: Mapping[str, tuple[str, ...]] = {
    "direction": (
        "last_cross_direction",
        "last_cross_positive",
        "last_cross_favorable",
        "golden",
        "death",
        "ordering",
    ),
    "last_cross": ("last_cross", "golden", "death"),
    "recency": ("cross_age", "recency"),
    "persistence": ("persistence",),
    "distance": ("signed_distance", "favorable_distance", "abs_distance", "distance_ge"),
    "normalized_distance": (
        "normalized_distance",
        "volatility_normalized",
        "provider_sigma",
    ),
    "slope": ("distance_velocity", "slope"),
    "curvature": ("distance_acceleration", "curvature"),
    "convergence": ("converging", "convergence"),
    "expansion": ("expanding", "expansion"),
}


class OfflineReplayAdapterError(RuntimeError):
    """Raised when a caller or replay product violates the adapter contract."""


class OfflineReplayAdapterMechanicsMissing(OfflineReplayAdapterError):
    """Raised before economic reads when canonical replay mechanics are absent."""

    def __init__(self, missing: Sequence[str], *, context: str) -> None:
        normalized = tuple(sorted({str(value) for value in missing if str(value)}))
        self.status = MECHANICS_MISSING_STATUS
        self.context = str(context)
        self.missing = normalized
        detail = ", ".join(normalized) if normalized else "unspecified mechanics"
        super().__init__(f"{self.status}: {self.context}: {detail}")


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


def _fixed_api_bindings() -> dict[str, dict[str, str]]:
    bindings: dict[str, dict[str, str]] = {}
    for role, (module_name, symbol_name) in _FIXED_CANONICAL_API_SYMBOLS.items():
        module = importlib.import_module(module_name)
        symbol = getattr(module, symbol_name, None)
        source = Path(str(getattr(module, "__file__", ""))).resolve()
        if not callable(symbol) or not source.is_file():
            raise OfflineReplayAdapterMechanicsMissing(
                (role,), context="fixed canonical replay API"
            )
        bindings[role] = {
            "module": module_name,
            "symbol": symbol_name,
            "module_sha256": _file_sha256(source),
        }
    return bindings


def _expected_fixed_bridge() -> dict[str, Any]:
    return {
        "one_shot_replay_module": FIXED_ONE_SHOT_REPLAY_MODULE,
        "repeated_policy_bridge_module": FIXED_REPEATED_POLICY_BRIDGE_MODULE,
        "owner_full_path_module": FIXED_OWNER_FULL_PATH_MODULE,
        "backtest_module": FIXED_BACKTEST_MODULE,
        "canonical_api_bindings": _fixed_api_bindings(),
        "custom_evaluator_allowed": False,
        "replay_engine": REPLAY_ENGINE,
        "queue_identity": QUEUE_IDENTITY,
        "same_millisecond_ambiguity_policy": SAME_MILLISECOND_AMBIGUITY_POLICY,
    }


def _validate_fixed_bridge(value: Any, *, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise OfflineReplayAdapterMechanicsMissing(
            tuple(_FIXED_CANONICAL_API_SYMBOLS), context=context
        )
    observed = dict(value)
    expected = _expected_fixed_bridge()
    api_bindings = observed.get("canonical_api_bindings")
    if not isinstance(api_bindings, Mapping):
        raise OfflineReplayAdapterMechanicsMissing(
            tuple(_FIXED_CANONICAL_API_SYMBOLS), context=context
        )
    missing = tuple(sorted(set(_FIXED_CANONICAL_API_SYMBOLS) - set(api_bindings)))
    if missing:
        raise OfflineReplayAdapterMechanicsMissing(missing, context=context)
    if observed != expected:
        raise OfflineReplayAdapterError(
            "portable canonical replay API module, symbol, or source SHA drifted"
        )
    return expected


def _rebind_historical_fixed_bridge(value: Any, *, context: str) -> Mapping[str, Any]:
    """Rebind source SHAs while preserving the frozen canonical API graph."""

    if not isinstance(value, Mapping):
        raise OfflineReplayAdapterMechanicsMissing(
            tuple(_FIXED_CANONICAL_API_SYMBOLS), context=context
        )
    observed = dict(value)
    expected = _expected_fixed_bridge()
    observed_api = observed.get("canonical_api_bindings")
    expected_api = expected["canonical_api_bindings"]
    if not isinstance(observed_api, Mapping):
        raise OfflineReplayAdapterMechanicsMissing(
            tuple(_FIXED_CANONICAL_API_SYMBOLS), context=context
        )
    missing = tuple(sorted(set(expected_api) - set(observed_api)))
    if missing:
        raise OfflineReplayAdapterMechanicsMissing(missing, context=context)
    if set(observed_api) != set(expected_api):
        raise OfflineReplayAdapterError(
            "historical portable canonical replay API role census drifted"
        )
    observed_without_api = {
        name: item for name, item in observed.items() if name != "canonical_api_bindings"
    }
    expected_without_api = {
        name: item for name, item in expected.items() if name != "canonical_api_bindings"
    }
    if observed_without_api != expected_without_api:
        raise OfflineReplayAdapterError("historical portable canonical replay contract drifted")
    for role, current in expected_api.items():
        historical = observed_api[role]
        if not isinstance(historical, Mapping):
            raise OfflineReplayAdapterError(
                "historical portable canonical replay API binding is malformed"
            )
        if (
            set(historical) != {"module", "symbol", "module_sha256"}
            or historical.get("module") != current["module"]
            or historical.get("symbol") != current["symbol"]
            or _SHA_RE.fullmatch(str(historical.get("module_sha256", ""))) is None
        ):
            raise OfflineReplayAdapterError(
                "historical portable canonical replay module or symbol drifted"
            )
    return expected


def _frame_sha256(frame: pd.DataFrame | pd.Series) -> str:
    value = frame.to_frame() if isinstance(frame, pd.Series) else frame
    header = {
        "columns": [str(column) for column in value.columns],
        "dtypes": [str(dtype) for dtype in value.dtypes],
        "index_name": None if value.index.name is None else str(value.index.name),
        "index_dtype": str(value.index.dtype),
        "rows": int(len(value)),
    }
    digest = hashlib.sha256()
    digest.update(_canonical_sha256(header).encode("ascii"))
    hashed = pd.util.hash_pandas_object(value, index=True, categorize=False)
    digest.update(hashed.to_numpy(dtype="<u8", copy=False).tobytes())
    return digest.hexdigest()


def _require_sha(value: Any, *, label: str) -> str:
    digest = str(value)
    if _SHA_RE.fullmatch(digest) is None:
        raise OfflineReplayAdapterError(f"{label} is not a lowercase SHA256")
    return digest


def _load_frozen_duration_action_contract() -> tuple[Mapping[str, Any], dict[str, tuple[Any, ...]]]:
    """Load only the frozen outcome-blind duration action vocabulary.

    The historical study's full loader also opens its old baseline pointer and
    execution plan. Neither object defines an action arm in this successor,
    whose B0 and replay inputs are bound independently.
    """

    study = importlib.import_module(FIXED_ONE_SHOT_REPLAY_MODULE)
    required_symbols = (
        "IDENTITY",
        "OUTCOME_BLIND_INPUTS",
        "OUTCOME_BLIND_INPUTS_SHA256",
        "_validate_file",
        "_load_source_json",
        "_duration_actions",
    )
    if any(not hasattr(study, name) for name in required_symbols):
        raise OfflineReplayAdapterMechanicsMissing(
            ("frozen_duration_action_contract",),
            context="fixed outcome-blind duration vocabulary",
        )
    try:
        study._validate_file(
            study.OUTCOME_BLIND_INPUTS,
            study.OUTCOME_BLIND_INPUTS_SHA256,
            role="frozen outcome-blind duration inputs",
        )
        contract = study._load_source_json(
            study.OUTCOME_BLIND_INPUTS,
            role="frozen outcome-blind duration inputs",
        )
        if (
            contract.get("identity") != study.IDENTITY
            or contract.get("schema_version") != f"{study.IDENTITY}.outcome_blind_inputs.v1"
        ):
            raise ValueError("outcome-blind duration identity drifted")
        permissions = contract.get("permissions")
        if not isinstance(permissions, Mapping) or any(
            permissions.get(field) is not False
            for field in (
                "development_economic_labels_read",
                "validation_read",
                "sealed_holdout_read",
                "action_authorized",
                "live_authorized",
            )
        ):
            raise ValueError("outcome-blind duration permissions drifted")
        actions_by_side: dict[str, tuple[Any, ...]] = {}
        action_payload: dict[str, list[dict[str, Any]]] = {}
        for side in ("BUY", "SELL"):
            actions = tuple(study._duration_actions(contract, side))
            expected = duration_vocabulary(side)
            if tuple(action.policy_id for action in actions) != expected:
                raise ValueError(f"{side} frozen duration vocabulary drifted")
            rows: list[dict[str, Any]] = []
            for action in actions:
                payload = dict(action.payload())
                if not str(payload.get("duration_semantics", "")).strip():
                    raise ValueError(f"{side} duration semantics are empty")
                if action.policy_id == "CONTROL_85N":
                    if (
                        action.engine_action != "CONTROL_85N"
                        or action.fixed_duration_s is not None
                        or action.fixed_duration_ms is not None
                    ):
                        raise ValueError("CONTROL_85N duration identity drifted")
                else:
                    matched = re.fullmatch(r"FIXED_([1-9][0-9]*)S", action.policy_id)
                    if (
                        matched is None
                        or action.engine_action != "FIXED_DURATION_MS"
                        or action.fixed_duration_s != int(matched.group(1))
                        or action.fixed_duration_ms != int(matched.group(1)) * 1_000
                    ):
                        raise ValueError(f"{side} fixed duration identity drifted")
                rows.append(payload)
            actions_by_side[side] = actions
            action_payload[side] = rows
        source_sha256 = _require_sha(
            study.OUTCOME_BLIND_INPUTS_SHA256,
            label="outcome-blind duration contract SHA256",
        )
    except OfflineReplayAdapterError:
        raise
    except Exception as exc:
        raise OfflineReplayAdapterMechanicsMissing(
            ("frozen_duration_action_contract",),
            context="fixed outcome-blind duration vocabulary",
        ) from exc
    binding = {
        "schema_version": f"{IDENTITY}.duration_action_contract.v1",
        "source_identity": f"{study.IDENTITY}.outcome_blind_inputs.v1",
        "source_sha256": source_sha256,
        "duration_action_universe_sha256": _canonical_sha256(action_payload),
        "actions": action_payload,
        "historical_operational_baseline_read": False,
        "historical_execution_plan_read": False,
        "economic_outcomes_read": False,
    }
    return binding, actions_by_side


def _normalize_day(value: Any) -> str:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise OfflineReplayAdapterError(f"invalid UTC day: {value!r}") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert("UTC").tz_localize(None)
    if parsed != parsed.normalize():
        raise OfflineReplayAdapterError(f"UTC day has a time component: {value!r}")
    return parsed.strftime("%Y-%m-%d")


def _normalize_side(value: Any) -> str:
    side = str(value).strip().upper()
    if side not in {"BUY", "SELL"}:
        raise OfflineReplayAdapterError(f"invalid cooldown side: {value!r}")
    return side


def _requires_control_prefix_parity(side: Any, policy_id: Any, exact_owner_action: Any) -> bool:
    normalized_side = _normalize_side(side)
    normalized_policy = str(policy_id).strip()
    normalized_owner = str(exact_owner_action).strip()
    vocabulary = duration_vocabulary(normalized_side)
    if normalized_policy not in vocabulary or normalized_owner not in vocabulary:
        raise OfflineReplayAdapterError("duration policy is outside the frozen vocabulary")
    return normalized_policy == normalized_owner


def _validated_worker_count(value: Any = DEFAULT_DAY_WORKERS) -> int:
    if isinstance(value, bool):
        raise OfflineReplayAdapterError("day replay worker count cannot be Boolean")
    try:
        workers = int(value)
    except (TypeError, ValueError) as exc:
        raise OfflineReplayAdapterError("day replay worker count is not an integer") from exc
    if workers < MIN_DAY_WORKERS or workers > MAX_DAY_WORKERS:
        raise OfflineReplayAdapterError(
            f"day replay workers must be in [{MIN_DAY_WORKERS}, {MAX_DAY_WORKERS}]"
        )
    return workers


def _validated_global_worker_count(
    value: Any = DEFAULT_GLOBAL_POLICY_DAY_WORKERS,
) -> int:
    if isinstance(value, bool):
        raise OfflineReplayAdapterError("global worker token count cannot be Boolean")
    try:
        workers = int(value)
    except (TypeError, ValueError) as exc:
        raise OfflineReplayAdapterError("global worker token count is not an integer") from exc
    if workers < 1 or workers > MAX_GLOBAL_POLICY_DAY_WORKERS:
        raise OfflineReplayAdapterError(
            f"global worker tokens must be in [1, {MAX_GLOBAL_POLICY_DAY_WORKERS}]"
        )
    return workers


def _json_safe_cache_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _json_safe_cache_value(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_json_safe_cache_value(item) for item in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    scalar = getattr(value, "item", None)
    if callable(scalar):
        normalized = scalar()
        if normalized is value:
            raise OfflineReplayAdapterError("B0 cache value could not be normalized")
        return _json_safe_cache_value(normalized)
    raise OfflineReplayAdapterError(f"B0 cache value has unsupported type: {type(value).__name__}")


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="ascii") as handle:
            json.dump(
                payload,
                handle,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="ascii"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OfflineReplayAdapterError(f"cannot read {label}") from exc
    if not isinstance(payload, dict):
        raise OfflineReplayAdapterError(f"{label} is not a JSON object")
    return payload


@dataclass(frozen=True, slots=True)
class DayReplayCacheKey:
    """Complete identity for one reusable day-level replay result."""

    adapter_artifact_sha256: str
    source_manifest_sha256: str
    panel_manifest_sha256: str
    fold_manifest_sha256: str
    execution_manifest_sha256: str
    exact_owner_policy_sha256: str
    candidate_policy_sha256: str
    side: str
    stage: str
    fold_id: str
    utc_day: str
    day_input_sha256: str

    def __post_init__(self) -> None:
        for name in (
            "adapter_artifact_sha256",
            "source_manifest_sha256",
            "panel_manifest_sha256",
            "fold_manifest_sha256",
            "execution_manifest_sha256",
            "exact_owner_policy_sha256",
            "candidate_policy_sha256",
            "day_input_sha256",
        ):
            _require_sha(getattr(self, name), label=name)
        object.__setattr__(self, "side", _normalize_side(self.side))
        object.__setattr__(self, "utc_day", _normalize_day(self.utc_day))
        if not str(self.stage).strip() or not str(self.fold_id).strip():
            raise OfflineReplayAdapterError("cache stage and fold identity must be non-empty")

    def payload(self) -> dict[str, str]:
        return asdict(self)

    @property
    def cache_key_sha256(self) -> str:
        return _canonical_sha256({"schema_version": DAY_CACHE_SCHEMA, **self.payload()})


@dataclass(frozen=True, slots=True)
class B0ControlCacheKey:
    """Candidate-independent identity for one exact-owner full-day control path."""

    adapter_artifact_sha256: str
    source_manifest_sha256: str
    panel_manifest_sha256: str
    fold_manifest_sha256: str
    execution_manifest_sha256: str
    exact_owner_policy_sha256: str
    exact_owner_predicate_bundle_sha256: str
    exact_owner_private_config_sha256: str
    fixed_bridge_sha256: str
    replay_engine: str
    queue_identity: str
    same_millisecond_ambiguity_policy: str
    side: str
    stage: str
    fold_id: str
    utc_day: str
    day_input_sha256: str
    canonical_day_input_binding_sha256: str
    market_window_identity_sha256: str
    model_overlay_identity_sha256: str
    latency_identity_sha256: str
    queue_random_identity_sha256: str
    replay_input_receipt_sha256: str
    target_day_semantics_sha256: str

    def __post_init__(self) -> None:
        for name in (
            "adapter_artifact_sha256",
            "source_manifest_sha256",
            "panel_manifest_sha256",
            "fold_manifest_sha256",
            "execution_manifest_sha256",
            "exact_owner_policy_sha256",
            "exact_owner_predicate_bundle_sha256",
            "exact_owner_private_config_sha256",
            "fixed_bridge_sha256",
            "day_input_sha256",
            "canonical_day_input_binding_sha256",
            "market_window_identity_sha256",
            "model_overlay_identity_sha256",
            "latency_identity_sha256",
            "queue_random_identity_sha256",
            "replay_input_receipt_sha256",
            "target_day_semantics_sha256",
        ):
            _require_sha(getattr(self, name), label=name)
        if self.exact_owner_policy_sha256 != offline.ACTIVE_OWNER_POLICY_SHA256:
            raise OfflineReplayAdapterError("B0 control owner policy SHA256 drifted")
        if self.exact_owner_predicate_bundle_sha256 != offline.ACTIVE_PREDICATE_BUNDLE_SHA256:
            raise OfflineReplayAdapterError("B0 control predicate bundle SHA256 drifted")
        if self.exact_owner_private_config_sha256 != offline.ACTIVE_PRIVATE_CONFIG_SHA256:
            raise OfflineReplayAdapterError("B0 control private config SHA256 drifted")
        if self.replay_engine != REPLAY_ENGINE:
            raise OfflineReplayAdapterError("B0 control replay engine drifted")
        if self.queue_identity != QUEUE_IDENTITY:
            raise OfflineReplayAdapterError("B0 control queue identity drifted")
        if self.same_millisecond_ambiguity_policy != SAME_MILLISECOND_AMBIGUITY_POLICY:
            raise OfflineReplayAdapterError("B0 control same-millisecond semantics drifted")
        object.__setattr__(self, "side", _normalize_side(self.side))
        object.__setattr__(self, "utc_day", _normalize_day(self.utc_day))
        if self.stage not in SEQUENTIAL_STAGES or not str(self.fold_id).strip():
            raise OfflineReplayAdapterError("B0 control fold scope drifted")

    def payload(self) -> dict[str, str]:
        return asdict(self)

    @property
    def cache_key_sha256(self) -> str:
        return _canonical_sha256({"schema_version": B0_CONTROL_CACHE_SCHEMA, **self.payload()})


@dataclass(frozen=True, slots=True)
class B0ControlPath:
    """Exact B0 arm output admitted independently of any candidate policy."""

    summary: Mapping[str, Any]
    campaigns: pd.DataFrame
    fills: pd.DataFrame
    decisions: pd.DataFrame


@dataclass(frozen=True, slots=True)
class OneShotProcessTopology:
    """One ten-thread C++ arm budget with no nested process pool."""

    total_worker_tokens: int = ONE_SHOT_TOTAL_WORKER_TOKENS
    day_parent_workers: int = ONE_SHOT_DAY_PARENT_WORKERS
    supervisor_workers: int = ONE_SHOT_SUPERVISOR_WORKERS
    arm_workers: int = FORMAL_SHARED_PREFIX_ARM_WORKERS

    def __post_init__(self) -> None:
        values = (
            self.total_worker_tokens,
            self.day_parent_workers,
            self.supervisor_workers,
            self.arm_workers,
        )
        if any(isinstance(value, bool) for value in values):
            raise OfflineReplayAdapterError("one-shot process topology must be integral")
        if int(self.total_worker_tokens) != GLOBAL_SEQUENTIAL_WORKER_TOKENS:
            raise OfflineReplayAdapterError("one-shot topology escaped the ten-token contract")
        if int(self.day_parent_workers) != ONE_SHOT_DAY_PARENT_WORKERS:
            raise OfflineReplayAdapterError("one-shot day-parent topology drifted")
        if int(self.supervisor_workers) != ONE_SHOT_SUPERVISOR_WORKERS:
            raise OfflineReplayAdapterError("one-shot supervisor topology drifted")
        if int(self.arm_workers) != FORMAL_SHARED_PREFIX_ARM_WORKERS:
            raise OfflineReplayAdapterError("one-shot arm topology drifted")
        if int(self.day_parent_workers) != 0 or int(self.supervisor_workers) != 0:
            raise OfflineReplayAdapterError("C++ one-shot topology cannot retain Python parents")
        if int(self.arm_workers) != int(self.total_worker_tokens):
            raise OfflineReplayAdapterError("one-shot CPU topology oversubscribes tokens")

    def payload(self) -> dict[str, int | bool]:
        return {
            "total_worker_tokens": int(self.total_worker_tokens),
            "day_parent_workers": int(self.day_parent_workers),
            "supervisor_workers": int(self.supervisor_workers),
            "arm_workers": int(self.arm_workers),
            "nested_process_pool": False,
            "shared_prefix_posix_fork": False,
            "global_cpp_arm_thread_pool": True,
            "shared_read_only_observation_tape": True,
        }


def _one_shot_topology_from_payload(value: Any) -> OneShotProcessTopology:
    if not isinstance(value, Mapping):
        raise OfflineReplayAdapterError("one-shot process topology is missing")
    expected = {
        "total_worker_tokens",
        "day_parent_workers",
        "supervisor_workers",
        "arm_workers",
        "nested_process_pool",
        "shared_prefix_posix_fork",
        "global_cpp_arm_thread_pool",
        "shared_read_only_observation_tape",
    }
    if set(value) != expected:
        raise OfflineReplayAdapterError("one-shot process topology schema drifted")
    if (
        value["nested_process_pool"] is not False
        or value["shared_prefix_posix_fork"] is not False
        or value["global_cpp_arm_thread_pool"] is not True
        or value["shared_read_only_observation_tape"] is not True
    ):
        raise OfflineReplayAdapterError("one-shot process topology semantics drifted")
    return OneShotProcessTopology(
        total_worker_tokens=value["total_worker_tokens"],
        day_parent_workers=value["day_parent_workers"],
        supervisor_workers=value["supervisor_workers"],
        arm_workers=value["arm_workers"],
    )


@dataclass(frozen=True, slots=True)
class SequentialReplayAccelerationOptions:
    """Explicit opt-in for bulk-only immutable day-input mmap acceleration."""

    day_input_cache_root: Path
    identity: str = DAY_INPUT_CACHE_IDENTITY
    cache_module_identity: str = day_input_cache.CACHE_IDENTITY
    cache_module_artifact_sha256: str = ""

    def __post_init__(self) -> None:
        if self.identity != DAY_INPUT_CACHE_IDENTITY:
            raise OfflineReplayAdapterError("executor acceleration identity drifted")
        if self.cache_module_identity != day_input_cache.CACHE_IDENTITY:
            raise OfflineReplayAdapterError("day-input cache module identity drifted")
        observed_module_sha = _file_sha256(Path(day_input_cache.__file__).resolve())
        supplied_module_sha = self.cache_module_artifact_sha256 or observed_module_sha
        if (
            _require_sha(
                supplied_module_sha,
                label="day-input cache module artifact SHA256",
            )
            != observed_module_sha
        ):
            raise OfflineReplayAdapterError("day-input cache module artifact drifted")
        object.__setattr__(self, "cache_module_artifact_sha256", observed_module_sha)
        root = Path(self.day_input_cache_root).expanduser().resolve()
        day_input_cache.ReplayDayInputCache(root)
        object.__setattr__(self, "day_input_cache_root", root)

    def payload(self) -> dict[str, str]:
        return {
            "identity": self.identity,
            "cache_module_identity": self.cache_module_identity,
            "cache_module_artifact_sha256": self.cache_module_artifact_sha256,
            "day_input_cache_root": str(self.day_input_cache_root),
        }


@dataclass(frozen=True, slots=True)
class DayInputMmapBinding:
    """Verified mmap bundle bound to one candidate-independent target day."""

    acceleration_identity: str
    cache_module_identity: str
    cache_module_artifact_sha256: str
    day_input_cache_root: str
    materialization_key_sha256: str
    canonical_input_binding_sha256: str
    identity: day_input_cache.DayInputCacheIdentity
    schema: day_input_cache.ReplayDayInputSchema
    binding: day_input_cache.DayInputCacheBinding

    def __post_init__(self) -> None:
        options = SequentialReplayAccelerationOptions(
            day_input_cache_root=Path(self.day_input_cache_root),
            identity=self.acceleration_identity,
            cache_module_identity=self.cache_module_identity,
            cache_module_artifact_sha256=self.cache_module_artifact_sha256,
        )
        object.__setattr__(self, "day_input_cache_root", str(options.day_input_cache_root))
        _require_sha(self.materialization_key_sha256, label="mmap materialization key SHA256")
        _require_sha(
            self.canonical_input_binding_sha256,
            label="mmap canonical input binding SHA256",
        )
        if not isinstance(self.identity, day_input_cache.DayInputCacheIdentity):
            raise OfflineReplayAdapterError("mmap day-input identity type drifted")
        if not isinstance(self.schema, day_input_cache.ReplayDayInputSchema):
            raise OfflineReplayAdapterError("mmap day-input schema type drifted")
        if not isinstance(self.binding, day_input_cache.DayInputCacheBinding):
            raise OfflineReplayAdapterError("mmap day-input binding type drifted")
        if self.identity.request_identity_sha256 != self.binding.request_identity_sha256:
            raise OfflineReplayAdapterError("mmap request identity drifted")
        if self.schema.schema_sha256 != self.binding.schema_sha256:
            raise OfflineReplayAdapterError("mmap schema identity drifted")

    def payload(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "schema_version": DAY_INPUT_MMAP_BINDING_SCHEMA,
            "acceleration_identity": self.acceleration_identity,
            "cache_module_identity": self.cache_module_identity,
            "cache_module_artifact_sha256": self.cache_module_artifact_sha256,
            "day_input_cache_root": self.day_input_cache_root,
            "materialization_key_sha256": self.materialization_key_sha256,
            "canonical_input_binding_sha256": self.canonical_input_binding_sha256,
            "identity": self.identity.payload(),
            "schema": self.schema.payload(),
            "binding": self.binding.payload(),
        }
        body["receipt_sha256"] = _document_sha256(body, "receipt_sha256")
        return body

    @property
    def receipt_sha256(self) -> str:
        return str(self.payload()["receipt_sha256"])


def _acceleration_options_from_payload(value: Any) -> SequentialReplayAccelerationOptions:
    if not isinstance(value, Mapping):
        raise OfflineReplayAdapterError("executor acceleration payload is malformed")
    expected = {
        "identity",
        "cache_module_identity",
        "cache_module_artifact_sha256",
        "day_input_cache_root",
    }
    if set(value) != expected:
        raise OfflineReplayAdapterError("executor acceleration payload schema drifted")
    return SequentialReplayAccelerationOptions(
        day_input_cache_root=Path(str(value["day_input_cache_root"])),
        identity=str(value["identity"]),
        cache_module_identity=str(value["cache_module_identity"]),
        cache_module_artifact_sha256=str(value["cache_module_artifact_sha256"]),
    )


def _day_input_mmap_binding_from_payload(value: Any) -> DayInputMmapBinding:
    if not isinstance(value, Mapping):
        raise OfflineReplayAdapterError("day-input mmap binding is malformed")
    expected = {
        "schema_version",
        "acceleration_identity",
        "cache_module_identity",
        "cache_module_artifact_sha256",
        "day_input_cache_root",
        "materialization_key_sha256",
        "canonical_input_binding_sha256",
        "identity",
        "schema",
        "binding",
        "receipt_sha256",
    }
    payload = dict(value)
    if (
        set(payload) != expected
        or payload.get("schema_version") != DAY_INPUT_MMAP_BINDING_SCHEMA
        or payload.get("receipt_sha256") != _document_sha256(payload, "receipt_sha256")
    ):
        raise OfflineReplayAdapterError("day-input mmap binding receipt drifted")
    identity_payload = payload["identity"]
    schema_payload = payload["schema"]
    binding_payload = payload["binding"]
    if not all(
        isinstance(item, Mapping) for item in (identity_payload, schema_payload, binding_payload)
    ):
        raise OfflineReplayAdapterError("day-input mmap nested binding is malformed")
    try:
        identity = day_input_cache.DayInputCacheIdentity.create(
            utc_day=str(identity_payload["utc_day"]),
            continuation_day=str(identity_payload["continuation_day"]),
            market_id=str(identity_payload["market_id"]),
            source_receipts=dict(identity_payload["source_receipts"]),
            bbo_source=str(identity_payload["bbo_source"]),
            l2_source=str(identity_payload["l2_source"]),
            clock_identity=str(identity_payload["clock_identity"]),
            clock_identity_sha256=str(identity_payload["clock_identity_sha256"]),
            engine_identity=str(identity_payload["engine_identity"]),
            engine_identity_sha256=str(identity_payload["engine_identity_sha256"]),
            market_window_identity_sha256=str(identity_payload["market_window_identity_sha256"]),
            model_overlay_identity_sha256=str(identity_payload["model_overlay_identity_sha256"]),
            latency_identity_sha256=str(identity_payload["latency_identity_sha256"]),
            queue_random_identity_sha256=str(identity_payload["queue_random_identity_sha256"]),
            replay_input_receipt_sha256=str(identity_payload["replay_input_receipt_sha256"]),
            params_identity_sha256=str(identity_payload["params_identity_sha256"]),
        )
        schema = day_input_cache.ReplayDayInputSchema(
            ml_main_array_count=int(schema_payload["ml_main_array_count"]),
            ml_feature_keys=tuple(schema_payload["ml_feature_keys"]),
            trades_columns=tuple(schema_payload["trades_columns"]),
            bbo_columns=tuple(schema_payload["bbo_columns"]),
            l2_columns=tuple(schema_payload["l2_columns"]),
            derived_columns=tuple(schema_payload["derived_columns"]),
        )
        binding = day_input_cache.DayInputCacheBinding(**dict(binding_payload))
    except (KeyError, TypeError, ValueError, day_input_cache.OfflineDayInputCacheError) as exc:
        raise OfflineReplayAdapterError("day-input mmap nested binding drifted") from exc
    if (
        identity.payload() != dict(identity_payload)
        or schema.payload() != dict(schema_payload)
        or binding.payload() != dict(binding_payload)
    ):
        raise OfflineReplayAdapterError("day-input mmap nested binding schema drifted")
    return DayInputMmapBinding(
        acceleration_identity=str(payload["acceleration_identity"]),
        cache_module_identity=str(payload["cache_module_identity"]),
        cache_module_artifact_sha256=str(payload["cache_module_artifact_sha256"]),
        day_input_cache_root=str(payload["day_input_cache_root"]),
        materialization_key_sha256=str(payload["materialization_key_sha256"]),
        canonical_input_binding_sha256=str(payload["canonical_input_binding_sha256"]),
        identity=identity,
        schema=schema,
        binding=binding,
    )


@dataclass(frozen=True, slots=True)
class OneShotSemanticCacheKey:
    """Fold-agnostic identity for one complete outer-train side/day frame."""

    adapter_artifact_sha256: str
    source_manifest_sha256: str
    panel_manifest_sha256: str
    fold_manifest_sha256: str
    execution_manifest_sha256: str
    exact_owner_policy_sha256: str
    candidate_policy_sha256: str
    side: str
    utc_day: str
    semantic_day_input_sha256: str

    def __post_init__(self) -> None:
        for name in (
            "adapter_artifact_sha256",
            "source_manifest_sha256",
            "panel_manifest_sha256",
            "fold_manifest_sha256",
            "execution_manifest_sha256",
            "exact_owner_policy_sha256",
            "candidate_policy_sha256",
            "semantic_day_input_sha256",
        ):
            _require_sha(getattr(self, name), label=name)
        object.__setattr__(self, "side", _normalize_side(self.side))
        object.__setattr__(self, "utc_day", _normalize_day(self.utc_day))

    def payload(self) -> dict[str, str]:
        return asdict(self)

    @property
    def semantic_key_sha256(self) -> str:
        return _canonical_sha256(
            {"schema_version": ONE_SHOT_SEMANTIC_CACHE_SCHEMA, **self.payload()}
        )


def _one_shot_semantic_day_input_sha256(rows: pd.DataFrame) -> str:
    """Hash every input byte except the two provider-owned fold-scope columns."""

    missing = sorted(_ONE_SHOT_FOLD_SCOPE_COLUMNS - set(rows.columns))
    if missing:
        raise OfflineReplayAdapterError(
            "semantic one-shot input lacks fold-scope columns: " + ", ".join(missing)
        )
    semantic = rows.drop(columns=sorted(_ONE_SHOT_FOLD_SCOPE_COLUMNS))
    return _frame_sha256(semantic)


class DayReplayCache:
    """Atomic cache for deterministic day-level one-shot and sequential results."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.entries = self.root / "entries"
        self.shards = self.root / "opportunity_shards"
        self.progress = self.root / "progress"
        self.locks = self.root / "locks"
        self.global_arm_pool = self.root / "shared_prefix_global_arm_pool"
        self.semantic_one_shot = self.root / "semantic_one_shot"
        self.semantic_locks = self.root / "semantic_locks"
        self.b0_control_entries = self.root / "b0_control_entries"
        self.b0_control_locks = self.root / "b0_control_locks"
        self.day_input_mmap_bindings = self.root / "day_input_mmap_bindings"
        self.day_input_mmap_locks = self.root / "day_input_mmap_locks"

    def _entry(self, key: DayReplayCacheKey) -> Path:
        return self.entries / key.cache_key_sha256

    def _semantic_entry(self, key: OneShotSemanticCacheKey) -> Path:
        return self.semantic_one_shot / f"{key.semantic_key_sha256}.json"

    def _b0_control_entry(self, key: B0ControlCacheKey) -> Path:
        return self.b0_control_entries / key.cache_key_sha256

    @contextmanager
    def lock(self, key: DayReplayCacheKey) -> Iterator[None]:
        self.locks.mkdir(parents=True, exist_ok=True)
        path = self.locks / f"{key.cache_key_sha256}.lock"
        with path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def semantic_lock(self, key: OneShotSemanticCacheKey) -> Iterator[None]:
        self.semantic_locks.mkdir(parents=True, exist_ok=True)
        path = self.semantic_locks / f"{key.semantic_key_sha256}.lock"
        with path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def b0_control_lock(self, key: B0ControlCacheKey) -> Iterator[None]:
        self.b0_control_locks.mkdir(parents=True, exist_ok=True)
        path = self.b0_control_locks / f"{key.cache_key_sha256}.lock"
        with path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def day_input_mmap_lock(self, materialization_key_sha256: str) -> Iterator[None]:
        key = _require_sha(
            materialization_key_sha256,
            label="mmap materialization key SHA256",
        )
        self.day_input_mmap_locks.mkdir(parents=True, exist_ok=True)
        path = self.day_input_mmap_locks / f"{key}.lock"
        with path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def write_progress(
        self,
        key: DayReplayCacheKey,
        *,
        state: Literal["queued", "running", "complete", "failed"],
        detail: str | None = None,
        counters: Mapping[str, int] | None = None,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        path = self.progress / f"{key.cache_key_sha256}.json"
        prior: dict[str, Any] = {}
        if path.is_file():
            try:
                prior = _read_json(path, label="day replay progress")
            except OfflineReplayAdapterError:
                prior = {}
        normalized_counters: dict[str, int] = {}
        for name, value in dict(counters or {}).items():
            if isinstance(value, bool) or int(value) < 0:
                raise OfflineReplayAdapterError("day replay progress counter is invalid")
            normalized_counters[str(name)] = int(value)
        body: dict[str, Any] = {
            "schema_version": DAY_PROGRESS_SCHEMA,
            "cache_key_sha256": key.cache_key_sha256,
            "cache_key": key.payload(),
            "state": state,
            "detail": detail,
            "counters": dict(sorted(normalized_counters.items())),
            "queued_at_utc": prior.get("queued_at_utc", now),
            "started_at_utc": (
                prior.get("started_at_utc")
                if state == "queued"
                else (prior.get("started_at_utc") or now)
            ),
            "updated_at_utc": now,
            "completed_at_utc": now if state in {"complete", "failed"} else None,
        }
        body["receipt_sha256"] = _document_sha256(body, "receipt_sha256")
        _atomic_json(path, body)

    def opportunity_root(self, key: DayReplayCacheKey) -> Path:
        return self.shards / key.cache_key_sha256

    def _manifest(self, key: DayReplayCacheKey) -> dict[str, Any] | None:
        path = self._entry(key) / "manifest.json"
        if not path.is_file():
            return None
        payload = _read_json(path, label="day replay cache manifest")
        if (
            payload.get("schema_version") != DAY_CACHE_SCHEMA
            or payload.get("cache_key_sha256") != key.cache_key_sha256
            or payload.get("cache_key") != key.payload()
            or payload.get("complete") is not True
            or payload.get("receipt_sha256") != _document_sha256(payload, "receipt_sha256")
        ):
            raise OfflineReplayAdapterError("day replay cache manifest drifted")
        return payload

    def _admit_frames(
        self,
        key: DayReplayCacheKey,
        *,
        kind: Literal["one_shot", "sequential"],
        frames: Mapping[str, pd.DataFrame],
        evidence: Mapping[str, Any] | None = None,
    ) -> None:
        final = self._entry(key)
        with self.lock(key):
            if self._manifest(key) is not None:
                return
            final.parent.mkdir(parents=True, exist_ok=True)
            staging = final.parent / f".{key.cache_key_sha256}.{uuid.uuid4().hex}.partial"
            staging.mkdir(parents=True, exist_ok=False)
            try:
                files: dict[str, Any] = {}
                for name, frame in sorted(frames.items()):
                    path = staging / f"{name}.parquet"
                    frame.to_parquet(path, index=True)
                    files[name] = {
                        "file": path.name,
                        "sha256": _file_sha256(path),
                        "rows": int(len(frame)),
                        "frame_sha256": _frame_sha256(frame),
                    }
                body: dict[str, Any] = {
                    "schema_version": DAY_CACHE_SCHEMA,
                    "kind": kind,
                    "cache_key_sha256": key.cache_key_sha256,
                    "cache_key": key.payload(),
                    "files": files,
                    "evidence": dict(evidence or {}),
                    "complete": True,
                    "atomic_admission": True,
                }
                body["receipt_sha256"] = _document_sha256(body, "receipt_sha256")
                _atomic_json(staging / "manifest.json", body)
                os.replace(staging, final)
            finally:
                if staging.exists():
                    shutil.rmtree(staging)

    def admit_one_shot(
        self,
        key: DayReplayCacheKey,
        outcomes: pd.DataFrame,
        supported: pd.DataFrame,
        *,
        evidence: Mapping[str, Any] | None = None,
    ) -> None:
        self._admit_frames(
            key,
            kind="one_shot",
            frames={"outcomes": outcomes, "supported": supported},
            evidence=evidence,
        )

    def admit_sequential(
        self,
        key: DayReplayCacheKey,
        rows: pd.DataFrame,
        *,
        evidence: Mapping[str, Any] | None = None,
    ) -> None:
        self._admit_frames(
            key,
            kind="sequential",
            frames={"rows": rows},
            evidence=evidence,
        )

    def _load_frames(
        self,
        key: DayReplayCacheKey,
        *,
        expected_kind: str,
        names: Sequence[str],
    ) -> tuple[pd.DataFrame, ...] | None:
        manifest = self._manifest(key)
        if manifest is None:
            return None
        if manifest.get("kind") != expected_kind or set(manifest.get("files", {})) != set(names):
            raise OfflineReplayAdapterError("day replay cache payload kind drifted")
        loaded: list[pd.DataFrame] = []
        for name in names:
            binding = manifest["files"][name]
            path = self._entry(key) / str(binding["file"])
            if not path.is_file() or _file_sha256(path) != binding["sha256"]:
                raise OfflineReplayAdapterError("day replay cache file hash drifted")
            frame = pd.read_parquet(path)
            if int(binding["rows"]) != len(frame) or binding["frame_sha256"] != _frame_sha256(
                frame
            ):
                raise OfflineReplayAdapterError("day replay cache frame identity drifted")
            loaded.append(frame)
        return tuple(loaded)

    def load_one_shot(self, key: DayReplayCacheKey) -> tuple[pd.DataFrame, pd.DataFrame] | None:
        loaded = self._load_frames(key, expected_kind="one_shot", names=("outcomes", "supported"))
        if loaded is None:
            return None
        return loaded[0], loaded[1]

    def _semantic_manifest(self, key: OneShotSemanticCacheKey) -> dict[str, Any] | None:
        path = self._semantic_entry(key)
        if not path.is_file():
            return None
        payload = _read_json(path, label="one-shot semantic cache manifest")
        if (
            payload.get("schema_version") != ONE_SHOT_SEMANTIC_CACHE_SCHEMA
            or payload.get("semantic_key_sha256") != key.semantic_key_sha256
            or payload.get("semantic_key") != key.payload()
            or payload.get("complete") is not True
            or payload.get("receipt_sha256") != _document_sha256(payload, "receipt_sha256")
        ):
            raise OfflineReplayAdapterError("one-shot semantic cache manifest drifted")
        source_payload = payload.get("source_cache_key")
        if not isinstance(source_payload, Mapping):
            raise OfflineReplayAdapterError("semantic cache source key is malformed")
        try:
            source_key = DayReplayCacheKey(**dict(source_payload))
        except (TypeError, OfflineReplayAdapterError) as exc:
            raise OfflineReplayAdapterError("semantic cache source key is malformed") from exc
        if payload.get("source_cache_key_sha256") != source_key.cache_key_sha256:
            raise OfflineReplayAdapterError("semantic cache source key hash drifted")
        self._validate_semantic_source_key(source_key, key)
        source_manifest = self._manifest(source_key)
        if (
            source_manifest is None
            or source_manifest.get("kind") != "one_shot"
            or source_manifest.get("receipt_sha256") != payload.get("source_cache_receipt_sha256")
        ):
            raise OfflineReplayAdapterError("semantic cache source receipt drifted")
        expected_frames = payload.get("source_frame_sha256")
        if not isinstance(expected_frames, Mapping):
            raise OfflineReplayAdapterError("semantic cache frame binding is malformed")
        observed_frames = {
            name: binding.get("frame_sha256")
            for name, binding in source_manifest.get("files", {}).items()
            if isinstance(binding, Mapping)
        }
        if dict(expected_frames) != observed_frames:
            raise OfflineReplayAdapterError("semantic cache frame binding drifted")
        return payload

    @staticmethod
    def _validate_semantic_source_key(
        source_key: DayReplayCacheKey,
        semantic_key: OneShotSemanticCacheKey,
    ) -> None:
        if source_key.stage != ONE_SHOT_STAGE:
            raise OfflineReplayAdapterError("semantic cache source stage drifted")
        for name in (
            "adapter_artifact_sha256",
            "source_manifest_sha256",
            "panel_manifest_sha256",
            "fold_manifest_sha256",
            "execution_manifest_sha256",
            "exact_owner_policy_sha256",
            "candidate_policy_sha256",
            "side",
            "utc_day",
        ):
            if getattr(source_key, name) != getattr(semantic_key, name):
                raise OfflineReplayAdapterError(f"semantic cache source {name} drifted")

    def register_one_shot_semantic(
        self,
        source_key: DayReplayCacheKey,
        semantic_key: OneShotSemanticCacheKey,
    ) -> None:
        """Bind one admitted fold cache as the reusable semantic side/day source."""

        self._validate_semantic_source_key(source_key, semantic_key)
        source_manifest = self._manifest(source_key)
        source_frames = self.load_one_shot(source_key)
        if source_manifest is None or source_frames is None:
            raise OfflineReplayAdapterError(
                "cannot register an incomplete one-shot semantic source"
            )
        source_evidence = source_manifest.get("evidence")
        if (
            not isinstance(source_evidence, Mapping)
            or source_evidence.get("semantic_day_input_sha256")
            != semantic_key.semantic_day_input_sha256
        ):
            raise OfflineReplayAdapterError("one-shot source does not bind its semantic day input")
        with self.semantic_lock(semantic_key):
            existing = self._semantic_manifest(semantic_key)
            if existing is not None:
                loaded = self.load_semantic_one_shot(semantic_key)
                if loaded is None:
                    raise OfflineReplayAdapterError("registered semantic cache could not be loaded")
                for observed, expected in zip(source_frames, loaded[:2], strict=True):
                    if _frame_sha256(observed) != _frame_sha256(expected):
                        raise OfflineReplayAdapterError(
                            "semantic one-shot frames disagree across fold receipts"
                        )
                return
            frame_sha256 = {
                name: binding["frame_sha256"] for name, binding in source_manifest["files"].items()
            }
            body: dict[str, Any] = {
                "schema_version": ONE_SHOT_SEMANTIC_CACHE_SCHEMA,
                "semantic_key_sha256": semantic_key.semantic_key_sha256,
                "semantic_key": semantic_key.payload(),
                "source_cache_key_sha256": source_key.cache_key_sha256,
                "source_cache_key": source_key.payload(),
                "source_cache_receipt_sha256": source_manifest["receipt_sha256"],
                "source_frame_sha256": frame_sha256,
                "complete": True,
                "atomic_admission": True,
            }
            body["receipt_sha256"] = _document_sha256(body, "receipt_sha256")
            _atomic_json(self._semantic_entry(semantic_key), body)

    def load_semantic_one_shot(
        self, key: OneShotSemanticCacheKey
    ) -> tuple[pd.DataFrame, pd.DataFrame, Mapping[str, Any]] | None:
        manifest = self._semantic_manifest(key)
        if manifest is None:
            return None
        source_key = DayReplayCacheKey(**dict(manifest["source_cache_key"]))
        loaded = self.load_one_shot(source_key)
        if loaded is None:
            raise OfflineReplayAdapterError("semantic cache source disappeared")
        source_manifest = self._manifest(source_key)
        if source_manifest is None:
            raise OfflineReplayAdapterError("semantic cache source manifest disappeared")
        evidence = {
            "semantic_reuse": True,
            "semantic_key_sha256": key.semantic_key_sha256,
            "semantic_day_input_sha256": key.semantic_day_input_sha256,
            "semantic_cache_receipt_sha256": manifest["receipt_sha256"],
            "source_cache_key_sha256": source_key.cache_key_sha256,
            "source_cache_receipt_sha256": source_manifest["receipt_sha256"],
        }
        return loaded[0], loaded[1], evidence

    def load_sequential(self, key: DayReplayCacheKey) -> pd.DataFrame | None:
        loaded = self._load_frames(key, expected_kind="sequential", names=("rows",))
        return None if loaded is None else loaded[0]

    def _b0_control_manifest(self, key: B0ControlCacheKey) -> dict[str, Any] | None:
        path = self._b0_control_entry(key) / "manifest.json"
        if not path.is_file():
            return None
        payload = _read_json(path, label="B0 control cache manifest")
        if (
            payload.get("schema_version") != B0_CONTROL_CACHE_SCHEMA
            or payload.get("kind") != "exact_b0_control_full_day_path"
            or payload.get("cache_key_sha256") != key.cache_key_sha256
            or payload.get("cache_key") != key.payload()
            or payload.get("candidate_policy_bound") is not False
            or payload.get("complete") is not True
            or payload.get("atomic_admission") is not True
            or payload.get("receipt_sha256") != _document_sha256(payload, "receipt_sha256")
        ):
            raise OfflineReplayAdapterError("B0 control cache manifest drifted")
        return payload

    @staticmethod
    def _validate_b0_control_path(key: B0ControlCacheKey, path: B0ControlPath) -> B0ControlPath:
        if not isinstance(path, B0ControlPath):
            raise OfflineReplayAdapterError("B0 control cache payload type drifted")
        if not isinstance(path.summary, Mapping):
            raise OfflineReplayAdapterError("B0 control summary is malformed")
        for name, frame in (
            ("campaigns", path.campaigns),
            ("fills", path.fills),
            ("decisions", path.decisions),
        ):
            if not isinstance(frame, pd.DataFrame):
                raise OfflineReplayAdapterError(f"B0 control {name} frame is malformed")
        summary = path.summary
        if (
            summary.get("engine") != REPLAY_ENGINE
            or summary.get("python_authoritative") is not True
            or summary.get("repeated_policy_enabled") is not True
        ):
            raise OfflineReplayAdapterError("B0 control execution identity drifted")
        audit = summary.get("cooldown_duration_policy_audit")
        if (
            not isinstance(audit, Mapping)
            or audit.get("policy_sha256") != key.exact_owner_policy_sha256
            or audit.get("predicate_bundle_sha256") != key.exact_owner_predicate_bundle_sha256
        ):
            raise OfflineReplayAdapterError("B0 control policy audit drifted")
        if not path.decisions.empty:
            if (
                "policy_sha256" not in path.decisions.columns
                or not path.decisions["policy_sha256"]
                .astype(str)
                .eq(key.exact_owner_policy_sha256)
                .all()
            ):
                raise OfflineReplayAdapterError("B0 control decision policy drifted")
        return path

    def _load_b0_control_unlocked(
        self, key: B0ControlCacheKey
    ) -> tuple[B0ControlPath, Mapping[str, Any]] | None:
        manifest = self._b0_control_manifest(key)
        if manifest is None:
            return None
        root = self._b0_control_entry(key)
        summary_binding = manifest.get("summary")
        frame_bindings = manifest.get("frames")
        if not isinstance(summary_binding, Mapping) or not isinstance(frame_bindings, Mapping):
            raise OfflineReplayAdapterError("B0 control cache file binding is malformed")
        summary_path = root / str(summary_binding.get("file", ""))
        if not summary_path.is_file() or _file_sha256(summary_path) != summary_binding.get(
            "sha256"
        ):
            raise OfflineReplayAdapterError("B0 control summary file hash drifted")
        summary_payload = _read_json(summary_path, label="B0 control summary")
        summary = summary_payload.get("summary")
        if not isinstance(summary, Mapping) or _canonical_sha256(summary) != summary_binding.get(
            "canonical_sha256"
        ):
            raise OfflineReplayAdapterError("B0 control summary identity drifted")
        frames: dict[str, pd.DataFrame] = {}
        if set(frame_bindings) != {"campaigns", "fills", "decisions"}:
            raise OfflineReplayAdapterError("B0 control frame vocabulary drifted")
        for name in ("campaigns", "fills", "decisions"):
            binding = frame_bindings[name]
            if not isinstance(binding, Mapping):
                raise OfflineReplayAdapterError("B0 control frame binding is malformed")
            frame_path = root / str(binding.get("file", ""))
            if not frame_path.is_file() or _file_sha256(frame_path) != binding.get("sha256"):
                raise OfflineReplayAdapterError(f"B0 control {name} file hash drifted")
            frame = pd.read_parquet(frame_path)
            if len(frame) != int(binding.get("rows", -1)) or _frame_sha256(frame) != binding.get(
                "frame_sha256"
            ):
                raise OfflineReplayAdapterError(f"B0 control {name} frame identity drifted")
            frames[name] = frame
        control = self._validate_b0_control_path(
            key,
            B0ControlPath(
                summary=dict(summary),
                campaigns=frames["campaigns"],
                fills=frames["fills"],
                decisions=frames["decisions"],
            ),
        )
        evidence = {
            "cache_key_sha256": key.cache_key_sha256,
            "cache_receipt_sha256": manifest["receipt_sha256"],
            "candidate_policy_bound": False,
        }
        return control, evidence

    def _admit_b0_control_unlocked(self, key: B0ControlCacheKey, control: B0ControlPath) -> None:
        control = self._validate_b0_control_path(key, control)
        final = self._b0_control_entry(key)
        if self._b0_control_manifest(key) is not None:
            raise OfflineReplayAdapterError("B0 control cache appeared during admission")
        final.parent.mkdir(parents=True, exist_ok=True)
        staging = final.parent / f".{key.cache_key_sha256}.{uuid.uuid4().hex}.partial"
        staging.mkdir(parents=True, exist_ok=False)
        try:
            safe_summary = _json_safe_cache_value(control.summary)
            if not isinstance(safe_summary, Mapping):
                raise OfflineReplayAdapterError("B0 control summary normalization failed")
            summary_path = staging / "summary.json"
            _atomic_json(summary_path, {"summary": safe_summary})
            frame_bindings: dict[str, Any] = {}
            for name, frame in (
                ("campaigns", control.campaigns),
                ("fills", control.fills),
                ("decisions", control.decisions),
            ):
                path = staging / f"{name}.parquet"
                frame.to_parquet(path, index=True)
                with path.open("rb") as handle:
                    os.fsync(handle.fileno())
                frame_bindings[name] = {
                    "file": path.name,
                    "sha256": _file_sha256(path),
                    "rows": int(len(frame)),
                    "frame_sha256": _frame_sha256(frame),
                }
            body: dict[str, Any] = {
                "schema_version": B0_CONTROL_CACHE_SCHEMA,
                "kind": "exact_b0_control_full_day_path",
                "cache_key_sha256": key.cache_key_sha256,
                "cache_key": key.payload(),
                "summary": {
                    "file": summary_path.name,
                    "sha256": _file_sha256(summary_path),
                    "canonical_sha256": _canonical_sha256(safe_summary),
                },
                "frames": frame_bindings,
                "candidate_policy_bound": False,
                "complete": True,
                "atomic_admission": True,
            }
            body["receipt_sha256"] = _document_sha256(body, "receipt_sha256")
            _atomic_json(staging / "manifest.json", body)
            os.replace(staging, final)
        finally:
            if staging.exists():
                shutil.rmtree(staging)

    def load_b0_control(
        self, key: B0ControlCacheKey
    ) -> tuple[B0ControlPath, Mapping[str, Any]] | None:
        return self._load_b0_control_unlocked(key)

    def load_or_compute_b0_control(
        self,
        key: B0ControlCacheKey,
        compute: Callable[[], B0ControlPath],
    ) -> tuple[B0ControlPath, Mapping[str, Any]]:
        """Serialize B0 computation per exact key and admit exactly one writer."""

        if not callable(compute):
            raise OfflineReplayAdapterError("B0 control computation is not callable")
        with self.b0_control_lock(key):
            cached = self._load_b0_control_unlocked(key)
            if cached is not None:
                control, evidence = cached
                return control, {**dict(evidence), "reused": True}
            computed = self._validate_b0_control_path(key, compute())
            self._admit_b0_control_unlocked(key, computed)
            admitted = self._load_b0_control_unlocked(key)
            if admitted is None:
                raise OfflineReplayAdapterError("B0 control admission disappeared")
            control, evidence = admitted
            return control, {**dict(evidence), "reused": False}

    def load_day_input_mmap_binding(
        self,
        materialization_key_sha256: str,
        *,
        acceleration: SequentialReplayAccelerationOptions,
        verify_bundle: bool = True,
    ) -> DayInputMmapBinding | None:
        key = _require_sha(
            materialization_key_sha256,
            label="mmap materialization key SHA256",
        )
        path = self.day_input_mmap_bindings / f"{key}.json"
        if not path.is_file():
            return None
        contract = _day_input_mmap_binding_from_payload(
            _read_json(path, label="day-input mmap adapter binding")
        )
        if (
            contract.materialization_key_sha256 != key
            or contract.acceleration_identity != acceleration.identity
            or contract.cache_module_identity != acceleration.cache_module_identity
            or contract.cache_module_artifact_sha256 != acceleration.cache_module_artifact_sha256
            or Path(contract.day_input_cache_root).resolve() != acceleration.day_input_cache_root
        ):
            raise OfflineReplayAdapterError("day-input mmap acceleration binding drifted")
        if verify_bundle:
            try:
                with day_input_cache.open_replay_day_inputs(
                    acceleration.day_input_cache_root,
                    identity=contract.identity,
                    schema=contract.schema,
                    expected=contract.binding,
                ):
                    pass
            except day_input_cache.OfflineDayInputCacheError as exc:
                raise OfflineReplayAdapterError(
                    "day-input mmap bundle failed verification"
                ) from exc
        return contract

    def admit_day_input_mmap_binding(
        self,
        contract: DayInputMmapBinding,
        *,
        acceleration: SequentialReplayAccelerationOptions,
    ) -> DayInputMmapBinding:
        if (
            contract.acceleration_identity != acceleration.identity
            or contract.cache_module_identity != acceleration.cache_module_identity
            or contract.cache_module_artifact_sha256 != acceleration.cache_module_artifact_sha256
            or Path(contract.day_input_cache_root).resolve() != acceleration.day_input_cache_root
        ):
            raise OfflineReplayAdapterError("day-input mmap admission identity drifted")
        key = contract.materialization_key_sha256
        with self.day_input_mmap_lock(key):
            existing = self.load_day_input_mmap_binding(
                key,
                acceleration=acceleration,
                verify_bundle=True,
            )
            if existing is not None:
                if existing.payload() != contract.payload():
                    raise OfflineReplayAdapterError("day-input mmap immutable binding disagrees")
                return existing
            try:
                with day_input_cache.open_replay_day_inputs(
                    acceleration.day_input_cache_root,
                    identity=contract.identity,
                    schema=contract.schema,
                    expected=contract.binding,
                ):
                    pass
            except day_input_cache.OfflineDayInputCacheError as exc:
                raise OfflineReplayAdapterError(
                    "day-input mmap admission lacks a valid bundle"
                ) from exc
            _atomic_json(
                self.day_input_mmap_bindings / f"{key}.json",
                contract.payload(),
            )
            admitted = self.load_day_input_mmap_binding(
                key,
                acceleration=acceleration,
                verify_bundle=True,
            )
            if admitted is None:
                raise OfflineReplayAdapterError("day-input mmap binding disappeared")
            return admitted


def _run_candidate_with_b0_control_cache(
    *,
    cache: DayReplayCache,
    b0_key: B0ControlCacheKey,
    compute_control: Callable[[], B0ControlPath],
    compute_candidate: Callable[[], Any],
    control_pre_materialized: bool = False,
    candidate_is_exact_b0: bool = False,
) -> tuple[B0ControlPath, Any, Mapping[str, Any]]:
    """Reuse exact B0; non-B0 candidate callbacks are always executed."""

    if control_pre_materialized:
        cached = cache.load_b0_control(b0_key)
        if cached is None:
            raise OfflineReplayAdapterError("pre-materialized B0 control cache is missing")
        control, evidence = cached
        evidence = {**dict(evidence), "reused": True, "pre_materialized": True}
    else:
        control, evidence = cache.load_or_compute_b0_control(b0_key, compute_control)
    if candidate_is_exact_b0:
        candidate = (
            dict(control.summary),
            control.campaigns.copy(deep=True),
            control.fills.copy(deep=True),
            control.decisions.copy(deep=True),
        )
    else:
        candidate = compute_candidate()
    return control, candidate, evidence


@dataclass(frozen=True, slots=True)
class _DayReplayJob:
    kind: Literal["one_shot", "sequential", "b0_control", "day_input_materialize"]
    utc_day: str
    cache_key: DayReplayCacheKey
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _DayReplayJobResult:
    utc_day: str
    cache_key_sha256: str
    frames: Mapping[str, pd.DataFrame]
    evidence: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class _ExecutionOptions:
    binding: Mapping[str, Any]
    cache: DayReplayCache
    workers: int


@dataclass(frozen=True, slots=True)
class SequentialPolicyDayBatchItem:
    """One frozen policy request submitted to the global policy-by-day pool."""

    request: backend.CanonicalSequentialReplayRequest
    replay_inputs: pd.DataFrame


@dataclass(frozen=True, slots=True)
class _PreparedSequentialReplay:
    request: backend.CanonicalSequentialReplayRequest
    options: _ExecutionOptions
    receipt: Mapping[str, Any]
    cached_frames: tuple[pd.DataFrame, ...]
    jobs: tuple[_DayReplayJob, ...]


def _bound_path(value: Any, expected_sha: Any, *, label: str) -> Path:
    path = resolve_portable_path(str(value)).resolve()
    digest = _require_sha(expected_sha, label=f"{label} SHA256")
    if not path.is_file() or _file_sha256(path) != digest:
        raise OfflineReplayAdapterMechanicsMissing((label,), context="day projection")
    return path


def _canonical_day_request(
    *, binding: Mapping[str, Any], utc_day: str, replay_inputs: pd.DataFrame
) -> Any:
    panel_builder = importlib.import_module(FIXED_PANEL_BUILDER_MODULE)
    projections = binding.get("day_projections")
    raw = projections.get(utc_day) if isinstance(projections, Mapping) else None
    if not isinstance(raw, Mapping):
        raise OfflineReplayAdapterMechanicsMissing(
            (f"canonical_day_projection:{utc_day}",), context="day projection"
        )
    expected_keys = {
        "utc_day",
        "panel_role",
        "queue_identity",
        "same_millisecond_ambiguity_policy",
        "bbo_path",
        "bbo_sha256",
        "l2_path",
        "l2_sha256",
        "features_path",
        "features_sha256",
        "source_manifest_path",
        "source_manifest_sha256",
        "book_view_manifest_path",
        "book_view_manifest_sha256",
        "features_manifest_path",
        "features_manifest_sha256",
        "private_config_path",
        "private_config_sha256",
        "native_observation_root",
        "source_receipts",
        "input_binding_sha256",
        "projection_receipt_sha256",
    }
    if set(raw) != expected_keys:
        raise OfflineReplayAdapterError("canonical day projection schema drifted")
    payload = dict(raw)
    receipt = payload.pop("projection_receipt_sha256")
    if receipt != _canonical_sha256(payload):
        raise OfflineReplayAdapterError("canonical day projection receipt drifted")
    if str(_unique_column_value(replay_inputs, "day_input_sha256")) != str(receipt):
        raise OfflineReplayAdapterError("day input does not bind the canonical projection receipt")
    if _normalize_day(payload["utc_day"]) != utc_day:
        raise OfflineReplayAdapterError("canonical day projection UTC day drifted")
    if (
        payload["panel_role"] != offline.PANEL_ROLE
        or payload["queue_identity"] != QUEUE_IDENTITY
        or payload["same_millisecond_ambiguity_policy"] != SAME_MILLISECOND_AMBIGUITY_POLICY
    ):
        raise OfflineReplayAdapterError("canonical day projection identity drifted")
    paths = {
        name: _bound_path(payload[f"{name}_path"], payload[f"{name}_sha256"], label=name)
        for name in (
            "bbo",
            "l2",
            "features",
            "source_manifest",
            "book_view_manifest",
            "features_manifest",
            "private_config",
        )
    }
    source_receipts = payload["source_receipts"]
    if not isinstance(source_receipts, Mapping):
        raise OfflineReplayAdapterError("canonical day source receipts are malformed")
    request = panel_builder.DayMaterializationRequest(
        utc_day=utc_day,
        panel_role=payload["panel_role"],
        queue_identity=payload["queue_identity"],
        same_millisecond_ambiguity_policy=payload["same_millisecond_ambiguity_policy"],
        bbo_path=paths["bbo"],
        l2_path=paths["l2"],
        features_path=paths["features"],
        source_manifest_path=paths["source_manifest"],
        book_view_manifest_path=paths["book_view_manifest"],
        features_manifest_path=paths["features_manifest"],
        private_config_path=paths["private_config"],
        native_observation_root=resolve_portable_path(
            str(payload["native_observation_root"])
        ).resolve(),
        source_receipts=dict(source_receipts),
        input_binding_sha256=str(payload["input_binding_sha256"]),
    )
    expected_context = panel_builder._sequential_context_bindings(request)
    for column, value in expected_context.items():
        if str(_unique_column_value(replay_inputs, column)) != str(value):
            raise OfflineReplayAdapterError(f"canonical D+1 receipt drifted: {column}")
    return request


def _canonical_day_projection(job: _DayReplayJob) -> tuple[Any, Any]:
    return _canonical_day_projection_from_rows(
        utc_day=job.utc_day,
        binding=job.payload.get("portable_binding"),
        rows=job.payload.get("replay_inputs"),
    )


def _canonical_day_projection_from_rows(
    *,
    utc_day: str,
    binding: Any,
    rows: Any,
) -> tuple[Any, Any]:
    if not isinstance(rows, pd.DataFrame) or not isinstance(binding, Mapping):
        raise OfflineReplayAdapterError("fixed day projection payload is malformed")
    request = _canonical_day_request(binding=binding, utc_day=utc_day, replay_inputs=rows)
    projection_module = importlib.import_module(FIXED_B0_PROJECTION_MODULE)
    replay = projection_module._materialize_replay_inputs(request)
    _validate_projected_replay(rows=rows, request=request, replay=replay)
    return request, replay


def _validate_projected_replay(*, rows: pd.DataFrame, request: Any, replay: Any) -> None:
    expected = {
        "market_window_identity_sha256": replay.market_window_identity_sha256,
        "model_overlay_identity_sha256": replay.model_overlay_identity_sha256,
        "latency_identity_sha256": replay.latency_identity_sha256,
        "queue_random_identity_sha256": replay.queue_random_identity_sha256,
        "replay_input_receipt_sha256": replay.replay_input_receipt_sha256,
    }
    for column, value in expected.items():
        if set(rows[column].astype(str)) != {str(value)}:
            raise OfflineReplayAdapterError(f"canonical projection drifted: {column}")
    if replay.continuation_day != str(_unique_column_value(rows, "d_plus_1_utc_day")):
        raise OfflineReplayAdapterError("canonical projection lost D+1 continuation")
    if replay.utc_day != request.utc_day:
        raise OfflineReplayAdapterError("canonical projection target day drifted")


def _day_input_materialization_key(
    job: _DayReplayJob,
    acceleration: SequentialReplayAccelerationOptions,
) -> str:
    binding = job.payload.get("portable_binding")
    projections = binding.get("day_projections") if isinstance(binding, Mapping) else None
    raw = projections.get(job.utc_day) if isinstance(projections, Mapping) else None
    if not isinstance(raw, Mapping):
        raise OfflineReplayAdapterError("mmap canonical day projection is missing")
    projection = dict(raw)
    receipt = projection.pop("projection_receipt_sha256", None)
    if receipt != _canonical_sha256(projection):
        raise OfflineReplayAdapterError("mmap canonical day projection receipt drifted")
    canonical_input = _require_sha(
        projection.get("input_binding_sha256"),
        label="mmap canonical input binding SHA256",
    )
    return _canonical_sha256(
        {
            "schema_version": f"{DAY_INPUT_CACHE_IDENTITY}.materialization_key.v1",
            "acceleration": acceleration.payload(),
            "adapter_artifact_sha256": job.cache_key.adapter_artifact_sha256,
            "source_manifest_sha256": job.cache_key.source_manifest_sha256,
            "panel_manifest_sha256": job.cache_key.panel_manifest_sha256,
            "execution_manifest_sha256": job.cache_key.execution_manifest_sha256,
            "utc_day": job.utc_day,
            "canonical_projection_receipt_sha256": receipt,
            "canonical_input_binding_sha256": canonical_input,
            "replay_engine": REPLAY_ENGINE,
            "queue_identity": QUEUE_IDENTITY,
            "same_millisecond_ambiguity_policy": SAME_MILLISECOND_AMBIGUITY_POLICY,
        }
    )


def _day_input_source_receipts(request: Any) -> dict[str, str]:
    receipts = request.source_receipts
    if not isinstance(receipts, Mapping):
        raise OfflineReplayAdapterError("mmap source receipts are malformed")

    def digest(component: str, names: Sequence[str]) -> str:
        return _canonical_sha256(
            {
                "schema_version": f"{DAY_INPUT_CACHE_IDENTITY}.source_receipt.v1",
                "component": component,
                "receipts": {
                    name: _require_sha(
                        receipts.get(name),
                        label=f"mmap {component} source receipt {name}",
                    )
                    for name in names
                },
            }
        )

    return {
        "trades": digest(
            "trades",
            (
                "source_manifest_canonical_sha256",
                "target_day_receipt_sha256",
                "context_source_receipts_sha256",
                "continuation_source_day_receipt_sha256",
            ),
        ),
        "bbo": digest(
            "bbo",
            (
                "context_book_receipts_sha256",
                "bbo_sha256",
                "continuation_bbo_sha256",
            ),
        ),
        "l2": digest(
            "l2",
            (
                "context_book_receipts_sha256",
                "l2_sha256",
                "continuation_l2_sha256",
            ),
        ),
        "ml_overlay": digest(
            "ml_overlay",
            (
                "context_feature_receipts_sha256",
                "features_daily_manifest_sha256",
                "features_day_file_sha256",
                "continuation_features_day_file_sha256",
                "feature_dag_sha256",
            ),
        ),
    }


def _build_day_input_mmap_binding(
    *,
    job: _DayReplayJob,
    request: Any,
    replay: Any,
    acceleration: SequentialReplayAccelerationOptions,
) -> DayInputMmapBinding:
    materialization_key = _day_input_materialization_key(job, acceleration)
    ml_main_array_count = len(replay.ml_data) - 1
    if ml_main_array_count <= 0:
        raise OfflineReplayAdapterError("mmap ML overlay shape drifted")
    clock_payload = {
        "schema_version": f"{DAY_INPUT_CACHE_IDENTITY}.clock.v1",
        "replay_event_clock": str(replay.params.get("replay_event_clock")),
        "trade_clock": "transact_time_ms",
        "book_clock": "ts_ms",
        "feature_ready_clock": "ml_overlay_main_000_ms",
        "same_millisecond_ambiguity_policy": SAME_MILLISECOND_AMBIGUITY_POLICY,
    }
    engine_payload = {
        "schema_version": f"{DAY_INPUT_CACHE_IDENTITY}.engine.v1",
        "replay_engine": REPLAY_ENGINE,
        "queue_identity": QUEUE_IDENTITY,
        "projection_module": FIXED_B0_PROJECTION_MODULE,
        "projection_artifact_sha256": _fixed_api_bindings()["canonical_day_projection_binding"][
            "module_sha256"
        ],
    }
    identity, schema, arrays = day_input_cache.target_day_context_from_replay_inputs(
        replay,
        source_receipts=_day_input_source_receipts(request),
        clock_identity=f"{DAY_INPUT_CACHE_IDENTITY}.exchange_time_merged",
        clock_identity_sha256=_canonical_sha256(clock_payload),
        engine_identity=f"{DAY_INPUT_CACHE_IDENTITY}.python_modeled_queue",
        engine_identity_sha256=_canonical_sha256(engine_payload),
        ml_main_array_count=ml_main_array_count,
    )
    try:
        binding = day_input_cache.admit_replay_day_inputs(
            acceleration.day_input_cache_root,
            identity=identity,
            schema=schema,
            inputs=arrays,
        )
    except day_input_cache.OfflineDayInputCacheError as exc:
        raise OfflineReplayAdapterError("day-input mmap materialization failed") from exc
    return DayInputMmapBinding(
        acceleration_identity=acceleration.identity,
        cache_module_identity=acceleration.cache_module_identity,
        cache_module_artifact_sha256=acceleration.cache_module_artifact_sha256,
        day_input_cache_root=str(acceleration.day_input_cache_root),
        materialization_key_sha256=materialization_key,
        canonical_input_binding_sha256=str(request.input_binding_sha256),
        identity=identity,
        schema=schema,
        binding=binding,
    )


def _execute_day_input_materialization(job: _DayReplayJob) -> _DayReplayJobResult:
    """Cold-materialize one candidate-independent day bundle without economics."""

    acceleration = _acceleration_options_from_payload(job.payload.get("day_input_acceleration"))
    expected_key = _require_sha(
        job.payload.get("day_input_materialization_key_sha256"),
        label="expected mmap materialization key SHA256",
    )
    observed_key = _day_input_materialization_key(job, acceleration)
    if observed_key != expected_key:
        raise OfflineReplayAdapterError("day-input mmap materialization key drifted")
    request, replay = _canonical_day_projection(job)
    contract = _build_day_input_mmap_binding(
        job=job,
        request=request,
        replay=replay,
        acceleration=acceleration,
    )
    if contract.materialization_key_sha256 != expected_key:
        raise OfflineReplayAdapterError("day-input mmap materialization result drifted")
    return _DayReplayJobResult(
        utc_day=job.utc_day,
        cache_key_sha256=expected_key,
        frames={},
        evidence={"day_input_mmap_binding": contract.payload()},
    )


@contextmanager
def _canonical_day_projection_context(
    job: _DayReplayJob,
) -> Iterator[tuple[Any, Any, Mapping[str, Any] | None]]:
    raw_contract = job.payload.get("day_input_mmap_binding")
    if raw_contract is None:
        request, replay = _canonical_day_projection(job)
        yield request, replay, None
        return
    contract = _day_input_mmap_binding_from_payload(raw_contract)
    acceleration = SequentialReplayAccelerationOptions(
        day_input_cache_root=Path(contract.day_input_cache_root),
        identity=contract.acceleration_identity,
        cache_module_identity=contract.cache_module_identity,
        cache_module_artifact_sha256=contract.cache_module_artifact_sha256,
    )
    expected_materialization_key = _day_input_materialization_key(job, acceleration)
    if contract.materialization_key_sha256 != expected_materialization_key:
        raise OfflineReplayAdapterError("day-input mmap job binding drifted")
    rows = job.payload.get("replay_inputs")
    binding = job.payload.get("portable_binding")
    if not isinstance(rows, pd.DataFrame) or not isinstance(binding, Mapping):
        raise OfflineReplayAdapterError("mmap fixed day projection payload is malformed")
    request = _canonical_day_request(
        binding=binding,
        utc_day=job.utc_day,
        replay_inputs=rows,
    )
    if str(request.input_binding_sha256) != contract.canonical_input_binding_sha256:
        raise OfflineReplayAdapterError("day-input mmap canonical input drifted")
    projection_module = importlib.import_module(FIXED_B0_PROJECTION_MODULE)
    try:
        with day_input_cache.open_replay_day_inputs(
            acceleration.day_input_cache_root,
            identity=contract.identity,
            schema=contract.schema,
            expected=contract.binding,
        ) as opened:
            replay = opened.to_replay_inputs(projection_module._ReplayInputs)
            _validate_projected_replay(rows=rows, request=request, replay=replay)
            yield (
                request,
                replay,
                {
                    "acceleration_identity": acceleration.identity,
                    "cache_module_identity": acceleration.cache_module_identity,
                    "cache_module_artifact_sha256": acceleration.cache_module_artifact_sha256,
                    "materialization_key_sha256": contract.materialization_key_sha256,
                    "binding_receipt_sha256": contract.receipt_sha256,
                    "cache_identity_sha256": contract.binding.cache_identity_sha256,
                    "admission_receipt_sha256": contract.binding.admission_receipt_sha256,
                    "read_only_mmap": True,
                },
            )
    except day_input_cache.OfflineDayInputCacheError as exc:
        raise OfflineReplayAdapterError("day-input mmap worker open failed") from exc


def _day_identity_hashes(request: Any) -> dict[str, str]:
    projection = importlib.import_module(FIXED_B0_PROJECTION_MODULE)
    return dict(projection.CanonicalB0MechanicsAdapter().identity_hashes(request))


def _build_day_snapshot_emitter(
    request: Any,
    replay: Any,
    *,
    utc_day: str,
    identity_hashes: Mapping[str, str],
) -> Any:
    panel_builder = importlib.import_module(FIXED_PANEL_BUILDER_MODULE)
    cache_module = importlib.import_module(FIXED_OBSERVATION_CACHE_MODULE)
    emitter_module = importlib.import_module(FIXED_SNAPSHOT_EMITTER_MODULE)
    cutoff_ns = (int(pd.Timestamp(utc_day, tz="UTC").timestamp()) + 86_400) * 1_000_000_000
    target = cache_module.open_admitted_observation_cache(
        request.native_observation_root, utc_day, deep=False
    )
    continuation = cache_module.open_admitted_observation_cache(
        request.native_observation_root, replay.continuation_day, deep=False
    )
    observations = panel_builder._stitch_observation_caches(
        target.observations(),
        continuation.observations_between(
            start_feature_ready_ts_ns=cutoff_ns,
            end_feature_ready_ts_ns=cutoff_ns + 86_400 * 1_000_000_000,
        ),
    )
    return emitter_module.CooldownV2ReplayEmitter(
        feature_block="M2",
        observations=observations,
        warmup_cutoff_ts_ns=cutoff_ns - 86_400 * 1_000_000_000,
        warmup_identity=str(request.source_receipts["native_source_binding_sha256"]),
        identity_hashes=identity_hashes,
        source_cursor_prefixes={
            "market": f"offline-sequential-market:{utc_day}",
            "depth": f"offline-sequential-depth:{utc_day}",
            "trade": f"offline-sequential-trade:{utc_day}",
        },
        retain_snapshots=False,
    )


class _ExactOwnerArtifactEvaluator:
    """Exact B0 evaluator using the same artifact projection as the live policy."""

    def __init__(
        self,
        *,
        expected_identity_hashes: Mapping[str, str],
        policy_path: Path | None = None,
        predicate_bundle_path: Path | None = None,
    ) -> None:
        self.policy_identity = successor.ACTIVE_OWNER_POLICY_IDENTITY
        self._expected_identity_hashes = {
            str(name): _require_sha(value, label=f"snapshot identity {name}")
            for name, value in expected_identity_hashes.items()
        }
        self._policy_path = (
            resolve_portable_path(FIXED_OWNER_POLICY_PATH).resolve()
            if policy_path is None
            else Path(policy_path).expanduser().resolve()
        )
        self._predicate_bundle_path = (
            resolve_portable_path(FIXED_OWNER_PREDICATE_BUNDLE_PATH).resolve()
            if predicate_bundle_path is None
            else Path(predicate_bundle_path).expanduser().resolve()
        )
        self._delegate = runtime_policy.load_runtime_policy(
            policy_path=self._policy_path,
            predicate_bundle_path=self._predicate_bundle_path,
            expected_policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
            expected_predicate_bundle_sha256=offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        )
        self.policy_sha256 = str(self._delegate.policy_sha256)
        self.predicate_bundle_sha256 = str(self._delegate.predicate_bundle_sha256)
        if not self.binding_valid:
            raise OfflineReplayAdapterMechanicsMissing(
                ("exact_owner_artifact_evaluator",),
                context=str(self.binding_error or "runtime binding invalid"),
            )

    @property
    def binding_valid(self) -> bool:
        return bool(
            self._delegate.binding_valid
            and self.policy_sha256 == offline.ACTIVE_OWNER_POLICY_SHA256
            and self.predicate_bundle_sha256 == offline.ACTIVE_PREDICATE_BUNDLE_SHA256
        )

    @property
    def binding_error(self) -> str | None:
        if self._delegate.binding_error is not None:
            return str(self._delegate.binding_error)
        if self.policy_sha256 != offline.ACTIVE_OWNER_POLICY_SHA256:
            return "exact_owner_policy_sha256_drifted"
        if self.predicate_bundle_sha256 != offline.ACTIVE_PREDICATE_BUNDLE_SHA256:
            return "exact_owner_predicate_bundle_sha256_drifted"
        return None

    def _validate_snapshot_identity(self, snapshot: Any) -> None:
        if not isinstance(snapshot, successor.CooldownAssignmentSnapshotV2):
            raise OfflineReplayAdapterError("exact owner snapshot type drifted")
        observed = snapshot.identity_hashes.to_dict()
        for name, expected in self._expected_identity_hashes.items():
            if observed.get(name) != expected:
                raise OfflineReplayAdapterError(
                    f"exact owner snapshot identity hash drifted: {name}"
                )

    def evaluate(self, snapshot: Any, baseline_duration_ms: Any) -> Any:
        self._validate_snapshot_identity(snapshot)
        decision = self._delegate.evaluate(snapshot, baseline_duration_ms)
        if (
            str(decision.policy_sha256) != offline.ACTIVE_OWNER_POLICY_SHA256
            or str(decision.predicate_bundle_sha256) != offline.ACTIVE_PREDICATE_BUNDLE_SHA256
        ):
            raise OfflineReplayAdapterError("exact owner decision identity drifted")
        return decision

    def evaluate_predicates(
        self,
        *,
        side: str,
        predicate_values: Mapping[str, Any],
        baseline_duration_ms: Any,
        snapshot_id: str,
    ) -> Any:
        return self._delegate.evaluate_predicates(
            side=side,
            predicate_values=predicate_values,
            baseline_duration_ms=baseline_duration_ms,
            snapshot_id=snapshot_id,
        )

    def audit(self) -> dict[str, Any]:
        return {
            "identity": self.policy_identity,
            "policy_sha256": self.policy_sha256,
            "predicate_bundle_sha256": self.predicate_bundle_sha256,
            "artifact_aware_snapshot_projection": True,
            "expected_snapshot_identity_hashes": dict(
                sorted(self._expected_identity_hashes.items())
            ),
            "delegate": self._delegate.audit(),
            "research_only": True,
            "action_authorized": False,
            "live_authorized": False,
        }


def _build_exact_owner_artifact_evaluator(
    *, expected_identity_hashes: Mapping[str, str]
) -> _ExactOwnerArtifactEvaluator:
    return _ExactOwnerArtifactEvaluator(expected_identity_hashes=expected_identity_hashes)


class _TargetDayOnlyEvaluator:
    def __init__(
        self,
        delegate: Any,
        fallback: Any,
        *,
        predicate_bundle_sha256: str,
        cutoff_ns: int,
    ) -> None:
        self._delegate = delegate
        self._fallback = fallback
        self._cutoff_ns = int(cutoff_ns)
        self.policy_identity = str(delegate.policy_identity)
        self.policy_sha256 = str(delegate.policy_sha256)
        self.predicate_bundle_sha256 = _require_sha(
            predicate_bundle_sha256,
            label="target-day evaluator predicate bundle SHA256",
        )
        self.baseline_action_by_snapshot: dict[str, str] = {}
        self.d_plus_1_fallbacks = 0

    @property
    def binding_valid(self) -> bool:
        return bool(self._delegate.binding_valid and self._fallback.binding_valid)

    @property
    def binding_error(self) -> str | None:
        return None if self.binding_valid else "candidate_or_b0_binding_invalid"

    def evaluate(self, snapshot: Any, baseline_duration_ms: Any) -> Any:
        baseline = self._fallback.evaluate(snapshot, baseline_duration_ms)
        snapshot_id = str(snapshot.snapshot_id)
        self.baseline_action_by_snapshot[snapshot_id] = str(baseline.action_id)
        assignment_ns = int(snapshot.m0_context.to_dict()["assignment_ts_ns"])
        if assignment_ns >= self._cutoff_ns:
            self.d_plus_1_fallbacks += 1
            return replace(
                baseline,
                policy_sha256=self.policy_sha256,
                predicate_bundle_sha256=self.predicate_bundle_sha256,
            )
        return self._delegate.evaluate(snapshot, baseline_duration_ms)

    def audit(self) -> dict[str, Any]:
        return {
            **dict(self._delegate.audit()),
            "d_plus_1_new_target_assignments_allowed": False,
            "d_plus_1_exact_b0_fallback_count": self.d_plus_1_fallbacks,
        }


def _exact_owner_runtime_params(
    request: Any,
    replay: Any,
    *,
    utc_day: str,
    identity_hashes: Mapping[str, str],
) -> dict[str, Any]:
    params = dict(replay.params)
    params["cooldown_v2_snapshot_emitter"] = _build_day_snapshot_emitter(
        request,
        replay,
        utc_day=utc_day,
        identity_hashes=identity_hashes,
    )
    params["cooldown_duration_policy_evaluator"] = _build_exact_owner_artifact_evaluator(
        expected_identity_hashes=identity_hashes
    )
    return params


def _cpp_exact_owner_runtime_params(
    replay: Any,
    *,
    identity_hashes: Mapping[str, str],
    qualification_receipt_sha256: str,
) -> dict[str, Any]:
    """Bind Python authority without materializing its unused observation objects."""

    receipt_sha256 = _require_sha(
        qualification_receipt_sha256,
        label="C++ one-shot qualification receipt SHA256",
    )
    params = dict(replay.params)
    params["cooldown_v2_snapshot_emitter"] = SimpleNamespace(
        identity="f05_cpp_formal_owner_snapshot_binding_v21",
        execution_role="non_executed_python_authority_binding",
        qualification_receipt_sha256=receipt_sha256,
        identity_hashes_sha256=_canonical_sha256(dict(sorted(identity_hashes.items()))),
    )
    params["cooldown_duration_policy_evaluator"] = _build_exact_owner_artifact_evaluator(
        expected_identity_hashes=identity_hashes
    )
    params["_cooldown_duration_policy_cpp_binding_only"] = True
    return params


def _shared_prefix_target_contracts(
    rows: pd.DataFrame,
    *,
    arm_ids: Sequence[str] | None = None,
    owner_action_only: bool = False,
) -> tuple[dict[str, Any], ...]:
    contracts: list[dict[str, Any]] = []
    for opportunity_id, row in rows.iterrows():
        side = _normalize_side(row["side"])
        if owner_action_only and arm_ids is not None:
            raise OfflineReplayAdapterError(
                "owner-only shared-prefix targets cannot accept a fixed arm list"
            )
        vocabulary = (
            (str(row["exact_owner_action"]),)
            if owner_action_only
            else tuple(
                str(value) for value in (duration_vocabulary(side) if arm_ids is None else arm_ids)
            )
        )
        contracts.append(
            {
                "opportunity_id": str(opportunity_id),
                "exposure_fill_ordinal": int(row["exposure_fill_ordinal"]),
                "fill_visible_ts_ms": int(row["fill_visible_ts_ms"]),
                "side": side,
                "order_id": int(row["order_id"]),
                "campaign_id": int(row["campaign_id"]),
                "expected_owner_action": str(row["exact_owner_action"]),
                "arm_ids": vocabulary,
            }
        )
    return tuple(contracts)


def _validate_shared_prefix_duration_trace(
    trace: Mapping[str, Any],
    *,
    opportunity: Mapping[str, Any],
    action_id: str,
) -> None:
    expected_action = "CONTROL_85N" if action_id == "CONTROL_85N" else "FIXED_DURATION_MS"
    expected = {
        "schema_version": "multiscale_ema_boolean_cooldown_duration_fork_trace.v3",
        "action": expected_action,
        "side": str(opportunity["side"]),
        "campaign_id": int(opportunity["campaign_id"]),
        "target_exposure_fill_ordinal": int(opportunity["exposure_fill_ordinal"]),
        "target_order_id": int(opportunity["order_id"]),
        "assignment_ts_ms": int(opportunity["fill_visible_ts_ms"]),
        "exact_owner_action": str(opportunity["exact_owner_action"]),
        "exact_owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        "exact_owner_baseline_policy_enabled": True,
        "washout_protocol": "first_flat_exposure_quarantine_scheduler_drained_v2",
        "control_path_exact_until_quarantine": True,
    }
    for field, value in expected.items():
        if trace.get(field) != value:
            raise OfflineReplayAdapterError(f"shared-prefix duration trace drifted: {field}")
    for field, tolerance in (
        ("assignment_inventory_btc", 1e-12),
        ("assignment_equity_usdc", 1e-12),
        ("baseline_duration_ms", 1e-9),
    ):
        source = {
            "assignment_inventory_btc": "inventory_after_fill_btc",
            "assignment_equity_usdc": "assignment_equity_usdc",
            "baseline_duration_ms": "baseline_duration_ms",
        }[field]
        if not math.isclose(
            float(trace.get(field, math.nan)),
            float(opportunity[source]),
            rel_tol=0.0,
            abs_tol=tolerance,
        ):
            raise OfflineReplayAdapterError(f"shared-prefix assignment state drifted: {field}")
    expected_applied = (
        float(opportunity["baseline_duration_ms"])
        if action_id == "CONTROL_85N"
        else float(shared_prefix.ARM_DURATION_MS[action_id])
    )
    if not math.isclose(
        float(trace.get("applied_duration_ms", math.nan)),
        expected_applied,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        raise OfflineReplayAdapterError("shared-prefix applied duration drifted")
    if (
        int(trace.get("reducing_quote_change_count", -1)) != 0
        or int(trace.get("second_assignment_count", -1)) != 0
    ):
        raise OfflineReplayAdapterError("shared-prefix permission contract drifted")
    right_censored = bool(trace.get("right_censored", False))
    complete = bool(trace.get("arm_washout_complete", False))
    value = trace.get("assignment_to_washout_value_usdc")
    if right_censored:
        if complete or value is not None:
            raise OfflineReplayAdapterError("right-censored shared-prefix arm retained value")
        return
    if (
        not complete
        or str(trace.get("terminal_reason")) != "arm_economic_washout"
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise OfflineReplayAdapterError("shared-prefix arm lacks economic washout")
    for field in (
        "active_or_pending_order_count",
        "pending_submit_count",
        "pending_cancel_count",
        "pending_ack_count",
        "cursor_owner_count",
        "hazard_owner_count",
    ):
        if int(trace.get(field, -1)) != 0:
            raise OfflineReplayAdapterError(f"shared-prefix washout retained {field}")
    if (
        bool(trace.get("campaign_active", True))
        or abs(float(trace.get("terminal_inventory_btc", math.nan))) > 1e-10
        or abs(float(trace.get("accounting_residual_usdc", math.nan))) > 1e-6
    ):
        raise OfflineReplayAdapterError("shared-prefix terminal state drifted")


def _collect_shared_prefix_day_frames(
    *,
    rows: pd.DataFrame,
    vocabulary: Sequence[str],
    manifest_paths: Sequence[str],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    outcomes = pd.DataFrame(index=rows.index, columns=vocabulary, dtype=float)
    supported = pd.DataFrame(False, index=rows.index, columns=vocabulary, dtype=bool)
    admitted: list[dict[str, Any]] = []
    observed_ids: set[str] = set()
    for manifest_text in manifest_paths:
        manifest_path = Path(manifest_text).resolve()
        manifest = _read_json(manifest_path, label="shared-prefix opportunity manifest")
        target = manifest.get("opportunity_contract", {}).get("target_binding")
        if not isinstance(target, Mapping):
            raise OfflineReplayAdapterError("shared-prefix target binding is missing")
        opportunity_id = str(target.get("opportunity_id", ""))
        if opportunity_id not in rows.index or opportunity_id in observed_ids:
            raise OfflineReplayAdapterError("shared-prefix opportunity denominator drifted")
        observed_ids.add(opportunity_id)
        opportunity = rows.loc[opportunity_id].to_dict()
        actual_arms = tuple(str(row["arm_id"]) for row in manifest["arms"])
        if actual_arms != tuple(vocabulary):
            raise OfflineReplayAdapterError("shared-prefix arm order drifted")
        for arm in manifest["arms"]:
            action_id = str(arm["arm_id"])
            arm_path = manifest_path.parent / str(arm["path"])
            payload = _read_json(arm_path, label="shared-prefix arm result")
            trace = payload.get("fork_trace")
            if not isinstance(trace, Mapping):
                raise OfflineReplayAdapterError("shared-prefix arm trace is malformed")
            _validate_shared_prefix_duration_trace(
                trace,
                opportunity=opportunity,
                action_id=action_id,
            )
            eligible = (
                payload.get("strict_execution_contract", {}).get("economic_point_label_status")
                == "eligible_modeled_queue_ambiguity_censored"
                and bool(trace.get("arm_washout_complete", False))
                and not bool(trace.get("right_censored", False))
            )
            if eligible:
                outcomes.loc[opportunity_id, action_id] = float(
                    trace["assignment_to_washout_value_usdc"]
                )
                supported.loc[opportunity_id, action_id] = True
            else:
                outcomes.loc[opportunity_id, action_id] = float("nan")
        admitted.append(
            {
                "opportunity_id": opportunity_id,
                "manifest_sha256": _file_sha256(manifest_path),
            }
        )
    if observed_ids != set(str(value) for value in rows.index):
        raise OfflineReplayAdapterError("shared-prefix output missed an opportunity")
    evidence = {
        "execution_semantics": "posix_fork_copy_on_write_at_fill_callback",
        "opportunity_count": len(admitted),
        "arm_count": len(admitted) * len(vocabulary),
        "opportunity_manifest_set_sha256": _canonical_sha256(
            sorted(admitted, key=lambda row: row["opportunity_id"])
        ),
        "modeled_queue_economics_authorized": True,
        "exact_owner_baseline_policy_enabled": True,
        "portable_restore_authority": False,
    }
    return outcomes, supported, evidence


def _validate_shared_prefix_day_audit(
    audit: Mapping[str, Any],
    *,
    target_count: int,
    arms_per_target: int,
    modeled_queue_economics_authorized: bool,
    topology: OneShotProcessTopology | None = None,
) -> None:
    dispatched = int(audit.get("opportunities_dispatched", -1))
    resumed = int(audit.get("opportunities_resumed", -1))
    expected_new_arms = dispatched * arms_per_target
    manifest_paths = tuple(audit.get("completed_manifest_paths") or ())
    if (
        int(audit.get("target_opportunity_count", -1)) != target_count
        or int(audit.get("target_opportunities_matched", -1)) != target_count
        or dispatched < 0
        or resumed < 0
        or dispatched + resumed != target_count
        or int(audit.get("arm_processes_completed", -1)) != expected_new_arms
        or len(manifest_paths) != target_count
        or audit.get("modeled_queue_economics_authorized") is not modeled_queue_economics_authorized
        or audit.get("exact_owner_baseline_policy_enabled") is not True
    ):
        raise OfflineReplayAdapterError("shared-prefix day execution audit drifted")
    if topology is not None and (
        int(audit.get("max_parallel_arms", -1)) != topology.arm_workers
        or int(audit.get("max_inflight_opportunity_snapshots", -1)) != topology.supervisor_workers
        or int(audit.get("peak_concurrent_arms", -1)) > topology.arm_workers
        or int(audit.get("peak_concurrent_supervisors", -1)) > topology.supervisor_workers
    ):
        raise OfflineReplayAdapterError("shared-prefix process topology drifted")


def _execute_one_shot_day_python_legacy(job: _DayReplayJob) -> _DayReplayJobResult:
    topology = _one_shot_topology_from_payload(job.payload.get("one_shot_topology"))
    if os.environ.get(_GLOBAL_ONE_SHOT_DAY_WORKER_ENV) != "1":
        raise OfflineReplayAdapterError("one-shot day escaped its global worker pool")
    study = importlib.import_module(FIXED_ONE_SHOT_REPLAY_MODULE)
    backtest = importlib.import_module(FIXED_BACKTEST_MODULE)
    rows = job.payload["replay_inputs"]
    vocabulary = tuple(str(value) for value in job.payload["duration_vocabulary"])
    side = _normalize_side(_unique_column_value(rows, "side"))
    if tuple(duration_vocabulary(side)) != vocabulary:
        raise OfflineReplayAdapterError("fixed duration action vocabulary drifted")
    cache = DayReplayCache(Path(job.payload["cache_root"]))
    targets = _shared_prefix_target_contracts(rows, arm_ids=vocabulary)
    completed = 0

    def report_progress(_index: int, _manifest_path: Path, _resumed: bool) -> None:
        nonlocal completed
        completed += 1
        cache.write_progress(
            job.cache_key,
            state="running",
            counters={
                "total_opportunities": len(targets),
                "completed_opportunities": completed,
                "total_arms": len(targets) * len(vocabulary),
                "completed_arms": completed * len(vocabulary),
            },
        )

    with _canonical_day_projection_context(job) as (request, replay, mmap_evidence):
        if mmap_evidence is None or mmap_evidence.get("read_only_mmap") is not True:
            raise OfflineReplayAdapterError("formal one-shot day did not open read-only mmap")
        identity_hashes = _day_identity_hashes(request)
        executor = shared_prefix.PosixCooldownSharedPrefixExecutor(
            output_root=cache.opportunity_root(job.cache_key),
            target_day=job.utc_day,
            source_contract_sha256=job.cache_key.cache_key_sha256,
            execution_identity_hashes=identity_hashes,
            max_parallel_arms=topology.arm_workers,
            max_inflight_opportunity_snapshots=topology.supervisor_workers,
            require_strict_native=False,
            modeled_queue_economics_authorized=True,
            exact_owner_policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
            target_opportunities=targets,
            global_pool_root=cache.global_arm_pool,
            recover_interrupted_staging=True,
            progress=report_progress,
        )
        params = study._prepare_base_params(
            _exact_owner_runtime_params(
                request,
                replay,
                utc_day=job.utc_day,
                identity_hashes=identity_hashes,
            ),
            trace_opportunities=False,
        )
        params["cooldown_duration_shared_prefix_executor"] = executor
        params["cooldown_duration_parent_stop_ts_ms"] = int(
            (pd.Timestamp(job.utc_day, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1_000
        )
        params["exchange_book_queue_ambiguity_trace_max"] = 64
        cache.write_progress(
            job.cache_key,
            state="running",
            counters={
                "total_opportunities": len(targets),
                "completed_opportunities": 0,
                "total_arms": len(targets) * len(vocabulary),
                "completed_arms": 0,
            },
        )
        started = time.perf_counter()
        try:
            result = backtest._simulate_tick_with_engine(
                "python",
                replay.trades,
                replay.var_ts_ms,
                replay.var_ssq,
                params,
                ml_data=replay.ml_data,
                bbo_data=replay.bbo_data,
                l2_data=replay.l2_data,
                var_ti=replay.var_ti,
                var_retsq=replay.var_retsq,
            )
        except BaseException:
            executor.abort()
            raise
        audit = dict(result.get("_cooldown_duration_shared_prefix_audit") or {})
        _validate_shared_prefix_day_audit(
            audit,
            target_count=len(targets),
            arms_per_target=len(vocabulary),
            modeled_queue_economics_authorized=True,
            topology=topology,
        )
        outcomes, supported, evidence = _collect_shared_prefix_day_frames(
            rows=rows,
            vocabulary=vocabulary,
            manifest_paths=tuple(audit["completed_manifest_paths"]),
        )
        evidence.update(
            {
                "day_wall_time_s": time.perf_counter() - started,
                "resumed_opportunity_count": int(audit["opportunities_resumed"]),
                "new_opportunity_count": int(audit["opportunities_dispatched"]),
                "max_parallel_arms": int(audit["max_parallel_arms"]),
                "one_shot_topology": topology.payload(),
                "day_input_mmap": dict(mmap_evidence),
            }
        )
    return _DayReplayJobResult(
        utc_day=job.utc_day,
        cache_key_sha256=job.cache_key.cache_key_sha256,
        frames={"outcomes": outcomes, "supported": supported},
        evidence=evidence,
    )


def _execute_one_shot_day(
    job: _DayReplayJob,
    *,
    arm_pool: ThreadPoolExecutor | None = None,
) -> _DayReplayJobResult:
    """Run all one-shot arms in C++ against one immutable day projection."""

    topology = _one_shot_topology_from_payload(job.payload.get("one_shot_topology"))
    qualification_receipt_sha256 = _require_sha(
        job.payload.get("cpp_qualification_receipt_sha256"),
        label="C++ one-shot qualification receipt SHA256",
    )
    if (
        job.payload.get("cpp_qualification_identity")
        != "f05_cpp_one_shot_real_day_all_arm_lockstep_v23"
    ):
        raise OfflineReplayAdapterError("C++ one-shot qualification identity drifted")
    rows = job.payload.get("replay_inputs")
    if not isinstance(rows, pd.DataFrame) or rows.empty:
        raise OfflineReplayAdapterError("C++ one-shot day rows are malformed")
    vocabulary = tuple(str(value) for value in job.payload["duration_vocabulary"])
    side = _normalize_side(_unique_column_value(rows, "side"))
    if tuple(duration_vocabulary(side)) != vocabulary:
        raise OfflineReplayAdapterError("fixed duration action vocabulary drifted")
    action_contract, actions_by_side = _load_frozen_duration_action_contract()
    actions = actions_by_side[side]
    if tuple(action.policy_id for action in actions) != vocabulary:
        raise OfflineReplayAdapterError("C++ one-shot action object order drifted")

    cache = DayReplayCache(Path(job.payload["cache_root"]))
    outcomes = pd.DataFrame(index=rows.index.copy(), columns=vocabulary, dtype=float)
    supported = pd.DataFrame(False, index=rows.index.copy(), columns=vocabulary, dtype=bool)
    total_arms = len(rows) * len(actions)
    cache.write_progress(
        job.cache_key,
        state="running",
        counters={
            "total_opportunities": len(rows),
            "completed_opportunities": 0,
            "total_arms": total_arms,
            "completed_arms": 0,
        },
    )

    with _canonical_day_projection_context(job) as (request, replay, mmap_evidence):
        if mmap_evidence is None or mmap_evidence.get("read_only_mmap") is not True:
            raise OfflineReplayAdapterError("formal C++ one-shot lacks read-only mmap")
        identity_hashes = _day_identity_hashes(request)
        try:
            import narrowgate_cpp as cpp
        except ImportError as exc:
            raise OfflineReplayAdapterError("formal C++ one-shot extension is unavailable") from exc
        tape = cpp_observation_tape.load_cpp_observation_tape(
            request.native_observation_root,
            target_day=job.utc_day,
            continuation_day=replay.continuation_day,
            deep_validate=False,
        )
        shared_tape = cpp_runtime_v22.build_shared_observation_tape(
            cpp,
            tape.arrays,
            content_sha256=str(tape.receipt["array_sha256"]),
        )
        policy_path = resolve_portable_path(FIXED_OWNER_POLICY_PATH).resolve()
        predicate_path = resolve_portable_path(FIXED_OWNER_PREDICATE_BUNDLE_PATH).resolve()
        runtime_config = cpp_runtime_v22.build_cpp_runtime_config(
            cpp,
            policy_path=policy_path,
            predicate_bundle_path=predicate_path,
            qualification_sha256=qualification_receipt_sha256,
        )
        base = _cpp_exact_owner_runtime_params(
            replay,
            identity_hashes=identity_hashes,
            qualification_receipt_sha256=qualification_receipt_sha256,
        )
        shared = {
            "ml_data": replay.ml_data,
            "bbo_data": replay.bbo_data,
            "l2_data": replay.l2_data,
            "var_ti": replay.var_ti,
            "var_retsq": replay.var_retsq,
        }
        tasks = tuple(
            (str(opportunity_id), row.to_dict(), action)
            for opportunity_id, row in rows.sort_index().iterrows()
            for action in actions
        )

        def execute_arm(
            opportunity_id: str,
            opportunity: Mapping[str, Any],
            action: Any,
        ) -> tuple[str, str, bool, float | None, float]:
            predicate_row = cpp_runtime_v22.build_target_predicate_row(
                cpp,
                opportunity,
            )
            cpp_runtime_v22.validate_target_predicate_row(
                cpp,
                predicate_row,
                opportunity,
                expected_predicate_count=len(runtime_config.policy.predicate_columns),
            )
            arm_base = dict(base)
            arm_base.update(
                {
                    "cooldown_duration_policy_cpp_runtime": (
                        cpp.F05RepeatedBooleanCooldownRuntime(runtime_config)
                    ),
                    "cooldown_duration_policy_cpp_parity_qualified": True,
                    "cooldown_duration_policy_cpp_event_loop_parity_qualified": True,
                    "cooldown_duration_policy_cpp_parity_receipt_sha256": (
                        qualification_receipt_sha256
                    ),
                    "_cooldown_duration_policy_cpp_window_tape_handle": shared_tape,
                    "_cooldown_duration_policy_cpp_predicate_rows": [predicate_row],
                }
            )
            trace, elapsed = importlib.import_module(
                FIXED_ONE_SHOT_REPLAY_MODULE
            )._run_duration_arm(
                opportunity,
                action,
                window=replay,
                base=arm_base,
                shared=shared,
                engine="cpp",
                require_control_prefix_parity=False,
                exact_owner_baseline_policy_enabled=True,
                expected_exact_owner_action=str(opportunity["exact_owner_action"]),
                expected_exact_owner_policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
            )
            _validate_shared_prefix_duration_trace(
                trace,
                opportunity=opportunity,
                action_id=str(action.policy_id),
            )
            eligible = bool(trace["arm_washout_complete"]) and not bool(trace["right_censored"])
            value = float(trace["assignment_to_washout_value_usdc"]) if eligible else None
            return opportunity_id, str(action.policy_id), eligible, value, float(elapsed)

        started = time.perf_counter()
        completed = 0
        arm_wall_time_s = 0.0
        owned_pool = arm_pool is None
        pool = arm_pool or ThreadPoolExecutor(max_workers=topology.arm_workers)
        try:
            futures = [pool.submit(execute_arm, *task) for task in tasks]
            for future in as_completed(futures):
                opportunity_id, action_id, eligible, value, elapsed = future.result()
                arm_wall_time_s += elapsed
                supported.loc[opportunity_id, action_id] = eligible
                outcomes.loc[opportunity_id, action_id] = value if eligible else float("nan")
                completed += 1
                if completed == total_arms or completed % len(actions) == 0:
                    cache.write_progress(
                        job.cache_key,
                        state="running",
                        counters={
                            "total_opportunities": len(rows),
                            "completed_opportunities": completed // len(actions),
                            "total_arms": total_arms,
                            "completed_arms": completed,
                        },
                    )
        finally:
            if owned_pool:
                pool.shutdown(wait=True, cancel_futures=True)
        if completed != total_arms or outcomes.index.tolist() != rows.index.tolist():
            raise OfflineReplayAdapterError("C++ one-shot arm denominator drifted")
        evidence = {
            "execution_semantics": ("cpp_full_day_direct_replay_shared_observation_tape_v23"),
            "formal_engine": "cpp",
            "qualification_identity": job.payload["cpp_qualification_identity"],
            "qualification_receipt_sha256": qualification_receipt_sha256,
            "qualification_scope": cpp_runtime_v22.QUALIFICATION_SCOPE,
            "opportunity_count": len(rows),
            "arm_count": total_arms,
            "modeled_queue_economics_authorized": True,
            "exact_owner_baseline_policy_enabled": True,
            "python_sequential_engine_remains_authoritative": True,
            "cpp_worker_tokens": topology.arm_workers,
            "nested_process_pool": False,
            "shared_read_only_observation_tape": True,
            "observation_tape_receipt": dict(tape.receipt),
            "day_input_mmap": dict(mmap_evidence),
            "duration_action_contract_sha256": _canonical_sha256(action_contract),
            "owner_policy_sha256": _file_sha256(policy_path),
            "owner_predicate_bundle_sha256": _file_sha256(predicate_path),
            "cpp_extension_sha256": _file_sha256(Path(cpp.__file__).resolve()),
            "cpp_runtime_module_sha256": _file_sha256(Path(cpp_runtime_v22.__file__).resolve()),
            "day_wall_time_s": time.perf_counter() - started,
            "arm_wall_time_s_total": arm_wall_time_s,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        }
    return _DayReplayJobResult(
        utc_day=job.utc_day,
        cache_key_sha256=job.cache_key.cache_key_sha256,
        frames={"outcomes": outcomes, "supported": supported},
        evidence=evidence,
    )


def _execute_exact_owner_one_day_mechanics(
    *,
    utc_day: str,
    portable_binding: Mapping[str, Any],
    rows: pd.DataFrame,
) -> Mapping[str, Any]:
    """Replay every admitted opportunity under its exact owner action only."""

    request, replay = _canonical_day_projection_from_rows(
        utc_day=utc_day,
        binding=portable_binding,
        rows=rows,
    )
    study = importlib.import_module(FIXED_ONE_SHOT_REPLAY_MODULE)
    backtest = importlib.import_module(FIXED_BACKTEST_MODULE)
    _load_frozen_duration_action_contract()
    identity_hashes = _day_identity_hashes(request)
    action_counts: dict[str, int] = {}
    side_counts: dict[str, int] = {}
    role_counts: dict[str, int] = {}
    for _opportunity_id, opportunity in rows.iterrows():
        side = _normalize_side(opportunity["side"])
        action_id = str(opportunity["exact_owner_action"])
        action_counts[action_id] = action_counts.get(action_id, 0) + 1
        side_counts[side] = side_counts.get(side, 0) + 1
        role = str(opportunity["role_at_fill"])
        role_counts[role] = role_counts.get(role, 0) + 1
    row_count = int(len(rows))
    if sum(action_counts.values()) != row_count:
        raise OfflineReplayAdapterError("one-day mechanics opportunity census drifted")
    targets = _shared_prefix_target_contracts(rows, owner_action_only=True)
    complete_count = 0
    right_censored_count = 0
    with tempfile.TemporaryDirectory(
        prefix="narrowgate-f05-exact-owner-cow-mechanics-"
    ) as directory:
        root = Path(directory)
        executor = shared_prefix.PosixCooldownSharedPrefixExecutor(
            output_root=root / "opportunities",
            target_day=utc_day,
            source_contract_sha256=_canonical_sha256(
                {
                    "identity": f"{IDENTITY}.exact_owner_one_day_mechanics.v2",
                    "utc_day": utc_day,
                    "row_ids": [str(value) for value in rows.index],
                }
            ),
            execution_identity_hashes=identity_hashes,
            max_parallel_arms=FORMAL_SHARED_PREFIX_ARM_WORKERS,
            require_strict_native=False,
            modeled_queue_economics_authorized=False,
            exact_owner_policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
            target_opportunities=targets,
            global_pool_root=root / "global_arm_pool",
        )
        params = study._prepare_base_params(
            _exact_owner_runtime_params(
                request,
                replay,
                utc_day=utc_day,
                identity_hashes=identity_hashes,
            ),
            trace_opportunities=False,
        )
        params["cooldown_duration_shared_prefix_executor"] = executor
        params["cooldown_duration_parent_stop_ts_ms"] = int(
            (pd.Timestamp(utc_day, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1_000
        )
        started = time.perf_counter()
        try:
            result = backtest._simulate_tick_with_engine(
                "python",
                replay.trades,
                replay.var_ts_ms,
                replay.var_ssq,
                params,
                ml_data=replay.ml_data,
                bbo_data=replay.bbo_data,
                l2_data=replay.l2_data,
                var_ti=replay.var_ti,
                var_retsq=replay.var_retsq,
            )
        except BaseException:
            executor.abort()
            raise
        replay_wall_time_s = time.perf_counter() - started
        audit = dict(result.get("_cooldown_duration_shared_prefix_audit") or {})
        manifests = tuple(audit.get("completed_manifest_paths") or ())
        _validate_shared_prefix_day_audit(
            audit,
            target_count=row_count,
            arms_per_target=1,
            modeled_queue_economics_authorized=False,
        )
        observed_ids: set[str] = set()
        for manifest_text in manifests:
            manifest_path = Path(manifest_text)
            manifest = _read_json(
                manifest_path,
                label="one-day shared-prefix mechanics manifest",
            )
            target = manifest.get("opportunity_contract", {}).get("target_binding")
            if not isinstance(target, Mapping):
                raise OfflineReplayAdapterError("one-day mechanics target binding is missing")
            opportunity_id = str(target["opportunity_id"])
            if opportunity_id not in rows.index or opportunity_id in observed_ids:
                raise OfflineReplayAdapterError("one-day mechanics opportunity identity drifted")
            observed_ids.add(opportunity_id)
            expected_action = str(rows.loc[opportunity_id, "exact_owner_action"])
            if tuple(str(arm["arm_id"]) for arm in manifest["arms"]) != (expected_action,):
                raise OfflineReplayAdapterError("one-day mechanics owner arm drifted")
            arm_path = manifest_path.parent / str(manifest["arms"][0]["path"])
            payload = _read_json(
                arm_path,
                label="one-day shared-prefix mechanics arm",
            )
            trace = payload.get("fork_trace")
            if not isinstance(trace, Mapping) or (
                trace.get("exact_owner_action") != expected_action
                or trace.get("exact_owner_baseline_policy_enabled") is not True
                or not math.isclose(
                    float(trace.get("applied_duration_ms", math.nan)),
                    float(trace.get("exact_owner_baseline_duration_ms", math.nan)),
                    rel_tol=0.0,
                    abs_tol=1e-9,
                )
                or trace.get("assignment_to_washout_value_usdc") is not None
                or payload.get("strict_execution_contract", {}).get("economic_point_label_status")
                != "unsupported_redacted"
            ):
                raise OfflineReplayAdapterError(
                    "one-day mechanics exact-owner no-op parity drifted"
                )
            if bool(trace.get("arm_washout_complete", False)) and not bool(
                trace.get("right_censored", False)
            ):
                complete_count += 1
            else:
                right_censored_count += 1
        if observed_ids != set(str(value) for value in rows.index):
            raise OfflineReplayAdapterError("one-day mechanics missed an admitted opportunity")
    return {
        "utc_day": utc_day,
        "opportunity_count": row_count,
        "exact_owner_noop_parity_count": row_count,
        "complete_washout_count": complete_count,
        "right_censored_count": right_censored_count,
        "action_counts": dict(sorted(action_counts.items())),
        "side_counts": dict(sorted(side_counts.items())),
        "role_counts": dict(sorted(role_counts.items())),
        "market_window_identity_sha256": replay.market_window_identity_sha256,
        "model_overlay_identity_sha256": replay.model_overlay_identity_sha256,
        "latency_identity_sha256": replay.latency_identity_sha256,
        "queue_random_identity_sha256": replay.queue_random_identity_sha256,
        "trace_schema_version": ("multiscale_ema_boolean_cooldown_duration_fork_trace.v3"),
        "execution_semantics": "posix_fork_copy_on_write_at_fill_callback",
        "arm_worker_count": FORMAL_SHARED_PREFIX_ARM_WORKERS,
        "replay_wall_time_s": replay_wall_time_s,
        "economic_values_computed_inside_replay": True,
        "economic_values_persisted": False,
        "economic_values_used_for_selection": False,
    }


def _execute_b0_control_day(job: _DayReplayJob) -> _DayReplayJobResult:
    if job.kind != "b0_control":
        raise OfflineReplayAdapterError("B0 materialization job kind drifted")
    with _canonical_day_projection_context(job) as (
        request,
        replay,
        mmap_evidence,
    ):
        return _execute_b0_control_day_projected(
            job,
            request=request,
            replay=replay,
            mmap_evidence=mmap_evidence,
        )


def _execute_b0_control_day_projected(
    job: _DayReplayJob,
    *,
    request: Any,
    replay: Any,
    mmap_evidence: Mapping[str, Any] | None,
) -> _DayReplayJobResult:
    owner = importlib.import_module(FIXED_OWNER_FULL_PATH_MODULE)
    identity_hashes = _day_identity_hashes(request)
    cutoff_ns = (int(pd.Timestamp(job.utc_day, tz="UTC").timestamp()) + 86_400) * 1_000_000_000

    def emitter_factory() -> Any:
        return _build_day_snapshot_emitter(
            request,
            replay,
            utc_day=job.utc_day,
            identity_hashes=identity_hashes,
        )

    guarded_control = _TargetDayOnlyEvaluator(
        _build_exact_owner_artifact_evaluator(expected_identity_hashes=identity_hashes),
        _build_exact_owner_artifact_evaluator(expected_identity_hashes=identity_hashes),
        predicate_bundle_sha256=offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        cutoff_ns=cutoff_ns,
    )
    window = SimpleNamespace(
        trades=replay.trades,
        var_ts_ms=replay.var_ts_ms,
        var_ssq=replay.var_ssq,
        bbo_data=replay.bbo_data,
        l2_data=replay.l2_data,
        var_ti=replay.var_ti,
        var_retsq=replay.var_retsq,
    )
    cache_root = job.payload.get("cache_root")
    if not isinstance(cache_root, str) or not cache_root.strip():
        raise OfflineReplayAdapterError("B0 materialization cache root is missing")
    cache = DayReplayCache(Path(cache_root))
    b0_key = _b0_control_cache_key(job=job, request=request, replay=replay)
    expected_key = job.payload.get("b0_control_cache_key")
    if not isinstance(expected_key, Mapping) or dict(expected_key) != b0_key.payload():
        raise OfflineReplayAdapterError("B0 materialization key drifted")
    with tempfile.TemporaryDirectory(prefix="f05-offline-b0-control-") as temporary:
        root = Path(temporary)

        def compute_control() -> B0ControlPath:
            summary, campaigns, fills, decisions = owner._simulate_python_arm(
                day=job.utc_day,
                arm=owner.CANDIDATE_ARM,
                window=window,
                ml_data=replay.ml_data,
                base=replay.params,
                progress_path=root / "control-progress.json",
                progress_interval_events=owner.DEFAULT_PROGRESS_INTERVAL_EVENTS,
                emitter=emitter_factory(),
                evaluator=guarded_control,
            )
            return B0ControlPath(
                summary=summary,
                campaigns=campaigns,
                fills=fills,
                decisions=decisions,
            )

        _, b0_cache_evidence = cache.load_or_compute_b0_control(b0_key, compute_control)
    evidence: dict[str, Any] = {"b0_control_cache": dict(b0_cache_evidence)}
    if mmap_evidence is not None:
        evidence["day_input_mmap"] = dict(mmap_evidence)
    return _DayReplayJobResult(
        utc_day=job.utc_day,
        cache_key_sha256=b0_key.cache_key_sha256,
        frames={},
        evidence=evidence,
    )


def _execute_sequential_day(job: _DayReplayJob) -> _DayReplayJobResult:
    with _canonical_day_projection_context(job) as (
        request,
        replay,
        mmap_evidence,
    ):
        return _execute_sequential_day_projected(
            job,
            request=request,
            replay=replay,
            mmap_evidence=mmap_evidence,
        )


def _execute_sequential_day_projected(
    job: _DayReplayJob,
    *,
    request: Any,
    replay: Any,
    mmap_evidence: Mapping[str, Any] | None,
) -> _DayReplayJobResult:
    repeated = importlib.import_module(FIXED_REPEATED_POLICY_BRIDGE_MODULE)
    owner = importlib.import_module(FIXED_OWNER_FULL_PATH_MODULE)
    fitted = job.payload["candidate"]
    target_side = _normalize_side(job.payload["target_side"])
    cutoff_ns = (int(pd.Timestamp(job.utc_day, tz="UTC").timestamp()) + 86_400) * 1_000_000_000
    identity_hashes = _day_identity_hashes(request)

    def emitter_factory() -> Any:
        return _build_day_snapshot_emitter(
            request,
            replay,
            utc_day=job.utc_day,
            identity_hashes=identity_hashes,
        )

    exact_owner = _build_exact_owner_artifact_evaluator(expected_identity_hashes=identity_hashes)
    if fitted.expected_executed_policy_sha256 == offline.ACTIVE_OWNER_POLICY_SHA256:
        candidate_delegate = _build_exact_owner_artifact_evaluator(
            expected_identity_hashes=identity_hashes
        )
        candidate_predicate_sha = offline.ACTIVE_PREDICATE_BUNDLE_SHA256
    else:
        allowed_policy_types = (
            BooleanCooldownPolicy,
            nested._ContinuousActionPolicy,
            nested._ActionMatchedPolicy,
        )
        if not isinstance(fitted.policy, allowed_policy_types):
            raise OfflineReplayAdapterMechanicsMissing(
                ("compiled_fixed_repeated_policy",),
                context=f"sequential:{job.utc_day}",
            )
        fold_policy_identity = _fold_policy_identity(fitted)
        artifact = repeated.ArtifactIdentityBinding(
            executed_artifact_scope=(repeated.ExecutedArtifactScope.LEARNING_ALGORITHM_FOLD_POLICY),
            executed_policy_identity=fold_policy_identity,
            executed_policy_sha256=fitted.expected_executed_policy_sha256,
            executed_predicate_bundle_sha256=_canonical_sha256(fitted.policy_payload),
            learning_algorithm_identity=fold_policy_identity,
            learning_algorithm_artifact_sha256=fitted.policy_sha256,
        )
        if isinstance(fitted.policy, BooleanCooldownPolicy):
            frozen_predicates = predicate_view.load_frozen_predicate_bundle(
                resolve_portable_path(FIXED_OWNER_PREDICATE_BUNDLE_PATH).resolve(),
                expected_file_sha256=offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
            )
            target_evaluator = successor.ResearchBooleanCooldownPolicyEvaluator(
                policies={
                    "BUY": fitted.policy if target_side == "BUY" else None,
                    "SELL": fitted.policy if target_side == "SELL" else None,
                },
                policy_identity=artifact.executed_policy_identity,
                policy_sha256=artifact.executed_policy_sha256,
                predicate_bundle_sha256=artifact.executed_predicate_bundle_sha256,
                expected_identity_hashes=identity_hashes,
            )

            class _ArtifactAwareTargetEvaluator:
                def __init__(self) -> None:
                    self.policy_identity = artifact.executed_policy_identity
                    self.policy_sha256 = artifact.executed_policy_sha256
                    self.predicate_bundle_sha256 = artifact.executed_predicate_bundle_sha256
                    self._evaluations = 0

                @property
                def binding_valid(self) -> bool:
                    return True

                @property
                def binding_error(self) -> None:
                    return None

                def evaluate(self, snapshot: Any, baseline_duration_ms: Any) -> Any:
                    self._evaluations += 1
                    if not isinstance(snapshot, successor.CooldownAssignmentSnapshotV2):
                        raise OfflineReplayAdapterError("candidate snapshot type drifted")
                    if not snapshot.policy_input_valid or snapshot.policy_input is None:
                        raise OfflineReplayAdapterError("candidate snapshot policy input invalid")
                    if snapshot.policy_input.snapshot_id != snapshot.snapshot_id:
                        raise OfflineReplayAdapterError("candidate snapshot identity drifted")
                    feature = snapshot.feature_row.to_dict()
                    policy_feature = snapshot.policy_input.feature_row.to_dict()
                    if feature != policy_feature:
                        raise OfflineReplayAdapterError("candidate policy feature row drifted")
                    if (
                        snapshot.feature_block != "M2"
                        or feature.get("feature_block") != "M2"
                        or feature.get("support_valid") is not True
                        or feature.get("channel_support_valid") is not True
                        or feature.get("warmup_admitted") is not True
                    ):
                        raise OfflineReplayAdapterError("candidate feature support invalid")
                    side = str(feature.get("side", "")).upper()
                    if side != target_side:
                        raise OfflineReplayAdapterError("candidate target side drifted")
                    try:
                        baseline_value = float(baseline_duration_ms)
                        frozen_baseline_value = float(feature.get("baseline_duration_ms"))
                    except (TypeError, ValueError) as exc:
                        raise OfflineReplayAdapterError(
                            "candidate baseline duration is not numeric"
                        ) from exc
                    if (
                        not math.isfinite(baseline_value)
                        or baseline_value <= 0.0
                        or not baseline_value.is_integer()
                        or not math.isfinite(frozen_baseline_value)
                        or frozen_baseline_value <= 0.0
                        or not frozen_baseline_value.is_integer()
                    ):
                        raise OfflineReplayAdapterError("candidate baseline duration invalid")
                    baseline = int(baseline_value)
                    frozen_baseline = int(frozen_baseline_value)
                    if frozen_baseline != baseline:
                        raise OfflineReplayAdapterError("candidate baseline duration drifted")
                    observed_hashes = snapshot.identity_hashes.to_dict()
                    if any(
                        observed_hashes.get(name) != expected
                        for name, expected in identity_hashes.items()
                    ):
                        raise OfflineReplayAdapterError("candidate snapshot hash drifted")
                    values = predicate_view.materialize_snapshot_predicates(
                        predicate_names=fitted.policy.predicate_columns,
                        feature_row=feature,
                        side=side,
                        baseline_duration_ms=baseline,
                        bundle=frozen_predicates,
                    )
                    return target_evaluator.evaluate_predicates(
                        side=side,
                        predicate_values=values,
                        baseline_duration_ms=baseline,
                        snapshot_id=str(snapshot.snapshot_id),
                    )

                def evaluate_predicates(
                    self,
                    *,
                    side: str,
                    predicate_values: Mapping[str, Any],
                    baseline_duration_ms: Any,
                    snapshot_id: str,
                ) -> Any:
                    return target_evaluator.evaluate_predicates(
                        side=side,
                        predicate_values=predicate_values,
                        baseline_duration_ms=baseline_duration_ms,
                        snapshot_id=snapshot_id,
                    )

                def audit(self) -> dict[str, Any]:
                    return {
                        "identity": self.policy_identity,
                        "policy_sha256": self.policy_sha256,
                        "predicate_bundle_sha256": self.predicate_bundle_sha256,
                        "evaluations": self._evaluations,
                        "artifact_aware_predicate_view": predicate_view.IDENTITY,
                        "frozen_2025_predicate_bundle_sha256": (frozen_predicates.file_sha256),
                        "delegate": target_evaluator.audit(),
                        "research_only": True,
                    }

            candidate_delegate = repeated.TargetSideDelegatingEvaluator(
                target_side=repeated.CandidateTargetSide(target_side),
                target_evaluator=_ArtifactAwareTargetEvaluator(),
                b0_evaluator=_build_exact_owner_artifact_evaluator(
                    expected_identity_hashes=identity_hashes
                ),
                artifact_binding=artifact,
            )
        else:

            class _FixedDecisionPolicyEvaluator:
                def __init__(self, policy: Any) -> None:
                    self._policy = policy
                    self._b0 = _build_exact_owner_artifact_evaluator(
                        expected_identity_hashes=identity_hashes
                    )
                    self.policy_identity = artifact.executed_policy_identity
                    self.policy_sha256 = artifact.executed_policy_sha256
                    self.predicate_bundle_sha256 = artifact.executed_predicate_bundle_sha256
                    self._evaluations = 0

                @property
                def binding_valid(self) -> bool:
                    return bool(self._b0.binding_valid)

                @property
                def binding_error(self) -> str | None:
                    return self._b0.binding_error

                def evaluate(self, snapshot: Any, baseline_duration_ms: Any) -> Any:
                    self._evaluations += 1
                    feature = snapshot.feature_row.to_dict()
                    side = str(feature.get("side", "")).upper()
                    if side != target_side:
                        baseline = self._b0.evaluate(snapshot, baseline_duration_ms)
                        return replace(
                            baseline,
                            policy_sha256=self.policy_sha256,
                            predicate_bundle_sha256=self.predicate_bundle_sha256,
                        )
                    names = tuple(str(value) for value in self._policy.predicate_columns)
                    missing = sorted(set(names) - set(feature))
                    if missing:
                        raise OfflineReplayAdapterError(
                            f"fixed candidate policy inputs are missing: {missing}"
                        )
                    chosen = str(
                        self._policy.choose(
                            pd.DataFrame(
                                ({name: feature[name] for name in names},),
                                index=pd.Index((str(snapshot.snapshot_id),)),
                            )
                        )[0]
                    )
                    baseline_ms = int(float(baseline_duration_ms))
                    return successor.CooldownDurationDecision(
                        action_id=chosen,
                        duration_ms=successor._duration_for_action(chosen, baseline_ms),
                        fallback_reason=None,
                        matched_rule_index=None,
                        policy_sha256=self.policy_sha256,
                        predicate_bundle_sha256=self.predicate_bundle_sha256,
                        snapshot_id=str(snapshot.snapshot_id),
                        support_valid=True,
                    )

                def audit(self) -> dict[str, Any]:
                    return {
                        "identity": self.policy_identity,
                        "policy_sha256": self.policy_sha256,
                        "predicate_bundle_sha256": self.predicate_bundle_sha256,
                        "evaluations": self._evaluations,
                        "fixed_adapter_compiled": True,
                        "custom_evaluator_injection": False,
                    }

            candidate_delegate = _FixedDecisionPolicyEvaluator(fitted.policy)
        candidate_predicate_sha = artifact.executed_predicate_bundle_sha256

    guarded_candidate = _TargetDayOnlyEvaluator(
        candidate_delegate,
        _build_exact_owner_artifact_evaluator(expected_identity_hashes=identity_hashes),
        predicate_bundle_sha256=candidate_predicate_sha,
        cutoff_ns=cutoff_ns,
    )
    guarded_control = _TargetDayOnlyEvaluator(
        exact_owner,
        _build_exact_owner_artifact_evaluator(expected_identity_hashes=identity_hashes),
        predicate_bundle_sha256=offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        cutoff_ns=cutoff_ns,
    )
    window = SimpleNamespace(
        trades=replay.trades,
        var_ts_ms=replay.var_ts_ms,
        var_ssq=replay.var_ssq,
        bbo_data=replay.bbo_data,
        l2_data=replay.l2_data,
        var_ti=replay.var_ti,
        var_retsq=replay.var_retsq,
    )
    cache_root = job.payload.get("cache_root")
    if not isinstance(cache_root, str) or not cache_root.strip():
        raise OfflineReplayAdapterError("sequential B0 cache root is missing")
    cache = DayReplayCache(Path(cache_root))
    b0_key = _b0_control_cache_key(job=job, request=request, replay=replay)
    with tempfile.TemporaryDirectory(prefix="f05-offline-sequential-") as temporary:
        root = Path(temporary)

        def compute_b0_control() -> B0ControlPath:
            summary, campaigns, fills, decisions = owner._simulate_python_arm(
                day=job.utc_day,
                arm=owner.CANDIDATE_ARM,
                window=window,
                ml_data=replay.ml_data,
                base=replay.params,
                progress_path=root / "control-progress.json",
                progress_interval_events=owner.DEFAULT_PROGRESS_INTERVAL_EVENTS,
                emitter=emitter_factory(),
                evaluator=guarded_control,
            )
            return B0ControlPath(
                summary=summary,
                campaigns=campaigns,
                fills=fills,
                decisions=decisions,
            )

        def compute_candidate() -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
            return owner._simulate_python_arm(
                day=job.utc_day,
                arm=owner.CANDIDATE_ARM,
                window=window,
                ml_data=replay.ml_data,
                base=replay.params,
                progress_path=root / "candidate-progress.json",
                progress_interval_events=owner.DEFAULT_PROGRESS_INTERVAL_EVENTS,
                emitter=emitter_factory(),
                evaluator=guarded_candidate,
            )

        control_path, candidate_path, b0_cache_evidence = _run_candidate_with_b0_control_cache(
            cache=cache,
            b0_key=b0_key,
            compute_control=compute_b0_control,
            compute_candidate=compute_candidate,
            control_pre_materialized=bool(job.payload.get("b0_control_pre_materialized", False)),
            candidate_is_exact_b0=(
                str(fitted.ladder_name) == "B0_CURRENT_EXACT"
                and fitted.expected_executed_policy_sha256 == offline.ACTIVE_OWNER_POLICY_SHA256
            ),
        )
        control_summary = control_path.summary
        control_campaigns = control_path.campaigns
        control_fills = control_path.fills
        candidate_summary, candidate_campaigns, candidate_fills, candidate_decisions = (
            candidate_path
        )

    identified = not (
        tuple(control_summary.get("metric_blockers") or ())
        or tuple(candidate_summary.get("metric_blockers") or ())
    )

    def value(summary: Mapping[str, Any], name: str) -> float:
        return float(summary[name]) if identified else float("nan")

    def campaign_rate(frame: pd.DataFrame, predicate: pd.Series) -> float:
        return (
            float(predicate.mean())
            if identified and not frame.empty
            else (0.0 if identified else float("nan"))
        )

    policy_count = int(len(candidate_decisions))
    if (
        str(fitted.ladder_name) == "B0_CURRENT_EXACT"
        and fitted.expected_executed_policy_sha256 == offline.ACTIVE_OWNER_POLICY_SHA256
    ):
        nonbaseline = 0
    else:
        nonbaseline = sum(
            str(row["action_id"])
            != guarded_candidate.baseline_action_by_snapshot.get(str(row["snapshot_id"]))
            for row in candidate_decisions.to_dict("records")
        )
    result: dict[str, Any] = {
        "utc_day": job.utc_day,
        "side": target_side,
        "panel_role": offline.PANEL_ROLE,
        "candidate_terminal_value_usdc": value(candidate_summary, "terminal_mtm_pnl_usdc"),
        "exact_owner_terminal_value_usdc": value(control_summary, "terminal_mtm_pnl_usdc"),
        "point_identified": identified,
        "policy_assignment_count": policy_count,
        "nonbaseline_action_count": int(nonbaseline),
        "feature_ready_active_treatment_events": int(
            candidate_decisions["support_valid"].astype(bool).sum()
        ),
        "repeated_sequential_policy": True,
        "one_shot_effect_aggregation_used": False,
        "exact_current_owner_row_wise_baseline": True,
        "candidate_executed_policy_sha256": fitted.expected_executed_policy_sha256,
        "exact_owner_executed_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        "exact_owner_control_cache_key_sha256": b0_key.cache_key_sha256,
        "exact_owner_control_cache_receipt_sha256": b0_cache_evidence["cache_receipt_sha256"],
        "exact_owner_control_cache_reused": bool(b0_cache_evidence["reused"]),
        "candidate_target_side": target_side,
        "same_market_source": True,
        "common_random_source": True,
        "arm_local_state": True,
        "common_row_count": max(1, min(len(control_fills), len(candidate_fills))),
        "common_campaign_count": max(1, min(len(control_campaigns), len(candidate_campaigns))),
        "candidate_closed_campaign_value_usdc": value(
            candidate_summary, "closed_campaign_value_usdc"
        ),
        "exact_owner_closed_campaign_value_usdc": value(
            control_summary, "closed_campaign_value_usdc"
        ),
        "candidate_campaign_q10_usdc": value(candidate_summary, "campaign_q10_usdc"),
        "exact_owner_campaign_q10_usdc": value(control_summary, "campaign_q10_usdc"),
        "candidate_campaign_cvar10_usdc": value(candidate_summary, "campaign_cvar10_usdc"),
        "exact_owner_campaign_cvar10_usdc": value(control_summary, "campaign_cvar10_usdc"),
        "candidate_inventory_time_btc_s": value(candidate_summary, "abs_inventory_time_btc_s"),
        "exact_owner_inventory_time_btc_s": value(control_summary, "abs_inventory_time_btc_s"),
        "candidate_max_abs_inventory_btc": value(candidate_summary, "max_inventory_btc"),
        "exact_owner_max_abs_inventory_btc": value(control_summary, "max_inventory_btc"),
        "candidate_fill_count": int(candidate_summary["fills_total"]),
        "exact_owner_fill_count": int(control_summary["fills_total"]),
        "candidate_negative_terminal_rate": campaign_rate(
            candidate_campaigns,
            candidate_campaigns.get("terminal_value_usdc", pd.Series(dtype=float)) < 0,
        ),
        "exact_owner_negative_terminal_rate": campaign_rate(
            control_campaigns,
            control_campaigns.get("terminal_value_usdc", pd.Series(dtype=float)) < 0,
        ),
        "candidate_campaign_mae_usdc": abs(value(candidate_summary, "campaign_mae_usdc")),
        "exact_owner_campaign_mae_usdc": abs(value(control_summary, "campaign_mae_usdc")),
        "candidate_repair_event_rate": value(candidate_summary, "repair_event_rate"),
        "exact_owner_repair_event_rate": value(control_summary, "repair_event_rate"),
        "candidate_mean_repair_time_s": value(candidate_summary, "mean_closed_repair_time_s"),
        "exact_owner_mean_repair_time_s": value(control_summary, "mean_closed_repair_time_s"),
        "candidate_censoring_rate": campaign_rate(
            candidate_campaigns,
            ~candidate_campaigns.get("closed", pd.Series(dtype=bool)).astype(bool),
        ),
        "exact_owner_censoring_rate": campaign_rate(
            control_campaigns,
            ~control_campaigns.get("closed", pd.Series(dtype=bool)).astype(bool),
        ),
    }
    result["paired_replay_receipt_sha256"] = _paired_replay_receipt_sha256(
        utc_day=job.utc_day,
        target_side=target_side,
        candidate_policy_sha256=fitted.expected_executed_policy_sha256,
        market_window_identity_sha256=replay.market_window_identity_sha256,
        model_overlay_identity_sha256=replay.model_overlay_identity_sha256,
        b0_control_cache_key_sha256=b0_key.cache_key_sha256,
        b0_control_cache_receipt_sha256=str(b0_cache_evidence["cache_receipt_sha256"]),
    )
    records = candidate_decisions.to_dict("records")
    group_values = {
        "action_count::": [str(row["action_id"]) for row in records],
        "role_count::": [str(row["role_at_fill"]) for row in records],
        "consecutive_units_count::": [
            str(max(1, int(round(float(row["baseline_duration_ms"]) / 85_000.0))))
            for row in records
        ],
        "fallback_count::": [str(row.get("fallback_reason") or "none") for row in records],
    }
    for prefix, labels in group_values.items():
        if not labels:
            result[f"{prefix}none"] = 0
            continue
        for label in sorted(set(labels)):
            result[f"{prefix}{label}"] = labels.count(label)
    evidence = {"b0_control_cache": dict(b0_cache_evidence)}
    if mmap_evidence is not None:
        evidence["day_input_mmap"] = dict(mmap_evidence)
    return _DayReplayJobResult(
        utc_day=job.utc_day,
        cache_key_sha256=job.cache_key.cache_key_sha256,
        frames={"rows": pd.DataFrame((result,))},
        evidence=evidence,
    )


def _paired_replay_receipt_sha256(
    *,
    utc_day: str,
    target_side: str,
    candidate_policy_sha256: str,
    market_window_identity_sha256: str,
    model_overlay_identity_sha256: str,
    b0_control_cache_key_sha256: str,
    b0_control_cache_receipt_sha256: str,
) -> str:
    payload = {
        "identity": f"{IDENTITY}.paired_sequential_day.v2",
        "utc_day": _normalize_day(utc_day),
        "target_side": _normalize_side(target_side),
        "candidate_policy_sha256": _require_sha(
            candidate_policy_sha256, label="paired candidate policy SHA256"
        ),
        "exact_owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        "market_window_identity_sha256": _require_sha(
            market_window_identity_sha256,
            label="paired market window SHA256",
        ),
        "model_overlay_identity_sha256": _require_sha(
            model_overlay_identity_sha256,
            label="paired model overlay SHA256",
        ),
        "exact_owner_control_cache_key_sha256": _require_sha(
            b0_control_cache_key_sha256,
            label="paired B0 cache key SHA256",
        ),
        "exact_owner_control_cache_receipt_sha256": _require_sha(
            b0_control_cache_receipt_sha256,
            label="paired B0 cache receipt SHA256",
        ),
        "d_plus_1_new_target_assignments_allowed": False,
    }
    return _canonical_sha256(payload)


def _fold_policy_identity(fitted: nested.FittedCandidate) -> str:
    if not isinstance(fitted, nested.FittedCandidate):
        raise OfflineReplayAdapterError("fold policy identity requires FittedCandidate")
    parts = (
        str(fitted.ladder_name).strip(),
        str(fitted.side).strip().upper(),
        str(fitted.selected_profile).strip(),
    )
    if any(not part for part in parts):
        raise OfflineReplayAdapterError("fold policy identity component is empty")
    return ":".join(parts)


def _execute_fixed_day_job(job: _DayReplayJob) -> _DayReplayJobResult:
    """Run the sole fixed bridge; caller-supplied executors are never accepted."""

    forbidden = _forbidden_injection_columns(tuple(job.payload))
    if forbidden:
        raise OfflineReplayAdapterError(
            f"arbitrary replay/evaluator injection is forbidden: {list(forbidden)}"
        )
    _validate_fixed_bridge(job.payload.get("fixed_bridge"), context=f"{job.kind}:{job.utc_day}")
    if job.kind == "day_input_materialize":
        return _execute_day_input_materialization(job)
    if job.kind == "one_shot":
        return _execute_one_shot_day(job)
    if job.kind == "b0_control":
        return _execute_b0_control_day(job)
    if job.kind == "sequential":
        return _execute_sequential_day(job)
    raise OfflineReplayAdapterError("fixed day replay kind drifted")


def _run_day_jobs(
    jobs: Sequence[_DayReplayJob], *, workers: int
) -> tuple[_DayReplayJobResult, ...]:
    """Run UTC-sorted jobs with deterministic result order and bounded workers."""

    count = _validated_worker_count(workers)
    ordered = tuple(sorted(jobs, key=lambda job: (job.utc_day, job.cache_key.cache_key_sha256)))
    if len({job.cache_key.cache_key_sha256 for job in ordered}) != len(ordered):
        raise OfflineReplayAdapterError("day replay job cache keys are duplicated")
    if count == 1:
        return tuple(_execute_fixed_day_job(job) for job in ordered)
    with ProcessPoolExecutor(max_workers=count) as pool:
        return tuple(pool.map(_execute_fixed_day_job, ordered, chunksize=1))


def _mark_global_policy_day_worker() -> None:
    os.environ[_GLOBAL_POLICY_DAY_WORKER_ENV] = "1"


def _mark_global_one_shot_day_worker() -> None:
    os.environ[_GLOBAL_ONE_SHOT_DAY_WORKER_ENV] = "1"


def _global_policy_day_execution_key(job: _DayReplayJob) -> tuple[str, ...]:
    return (
        job.utc_day,
        job.cache_key.stage,
        job.cache_key.side,
        job.cache_key.fold_id,
        job.cache_key.candidate_policy_sha256,
        job.cache_key.cache_key_sha256,
    )


def _global_policy_day_result_key(job: _DayReplayJob) -> tuple[str, ...]:
    return (
        job.cache_key.stage,
        job.cache_key.side,
        job.cache_key.fold_id,
        job.cache_key.candidate_policy_sha256,
        job.utc_day,
        job.cache_key.cache_key_sha256,
    )


def run_global_policy_day_jobs(
    jobs: Sequence[_DayReplayJob],
    *,
    total_worker_tokens: int = DEFAULT_GLOBAL_POLICY_DAY_WORKERS,
) -> tuple[_DayReplayJobResult, ...]:
    """Run all frozen policy-by-day jobs in one non-nestable bounded pool."""

    if os.environ.get(_GLOBAL_POLICY_DAY_WORKER_ENV) == "1":
        raise OfflineReplayAdapterError("nested global policy-day pools are forbidden")
    workers = _validated_global_worker_count(total_worker_tokens)
    execution_order = tuple(sorted(jobs, key=_global_policy_day_execution_key))
    if any(job.kind != "sequential" for job in execution_order):
        raise OfflineReplayAdapterError("global policy-day scheduling accepts sequential jobs only")
    keys = tuple(job.cache_key.cache_key_sha256 for job in execution_order)
    if len(set(keys)) != len(keys):
        raise OfflineReplayAdapterError("global policy-day cache keys are duplicated")
    if not execution_order:
        return ()
    if workers == 1:
        executed = tuple(_execute_fixed_day_job(job) for job in execution_order)
    else:
        with ProcessPoolExecutor(
            max_workers=workers,
            initializer=_mark_global_policy_day_worker,
        ) as pool:
            executed = tuple(pool.map(_execute_fixed_day_job, execution_order, chunksize=1))
    result_by_key = {result.cache_key_sha256: result for result in executed}
    if set(result_by_key) != set(keys):
        raise OfflineReplayAdapterError("global policy-day worker result drifted")
    contract_order = sorted(execution_order, key=_global_policy_day_result_key)
    return tuple(result_by_key[job.cache_key.cache_key_sha256] for job in contract_order)


def run_global_one_shot_day_jobs(
    jobs: Sequence[_DayReplayJob],
    *,
    total_worker_tokens: int = ONE_SHOT_TOTAL_WORKER_TOKENS,
) -> tuple[_DayReplayJobResult, ...]:
    """Run every day through one persistent, non-nestable C++ arm pool."""

    if os.environ.get(_GLOBAL_ONE_SHOT_DAY_WORKER_ENV) == "1":
        raise OfflineReplayAdapterError("nested global one-shot pools are forbidden")
    topology = OneShotProcessTopology(total_worker_tokens=total_worker_tokens)
    ordered = tuple(sorted(jobs, key=_global_policy_day_execution_key))
    if any(job.kind != "one_shot" for job in ordered):
        raise OfflineReplayAdapterError("global one-shot scheduling accepts one-shot jobs only")
    keys = tuple(job.cache_key.cache_key_sha256 for job in ordered)
    if len(set(keys)) != len(keys):
        raise OfflineReplayAdapterError("global one-shot cache keys are duplicated")
    for job in ordered:
        observed = _one_shot_topology_from_payload(job.payload.get("one_shot_topology"))
        if observed != topology or "day_input_mmap_binding" not in job.payload:
            raise OfflineReplayAdapterError("global one-shot job escaped frozen acceleration")
    if not ordered:
        return ()
    with ThreadPoolExecutor(max_workers=topology.arm_workers) as pool:
        executed = tuple(_execute_one_shot_day(job, arm_pool=pool) for job in ordered)
    result_by_key = {result.cache_key_sha256: result for result in executed}
    if set(result_by_key) != set(keys):
        raise OfflineReplayAdapterError("global one-shot worker result drifted")
    return tuple(result_by_key[key] for key in keys)


def _run_global_b0_control_jobs(
    jobs: Sequence[_DayReplayJob],
    *,
    total_worker_tokens: int,
) -> tuple[_DayReplayJobResult, ...]:
    if os.environ.get(_GLOBAL_POLICY_DAY_WORKER_ENV) == "1":
        raise OfflineReplayAdapterError("nested global B0 pools are forbidden")
    workers = _validated_global_worker_count(total_worker_tokens)
    ordered = tuple(sorted(jobs, key=_global_policy_day_execution_key))
    if any(job.kind != "b0_control" for job in ordered):
        raise OfflineReplayAdapterError("B0 materialization pool received a candidate job")
    expected_keys = tuple(
        _prospective_b0_control_cache_key(job).cache_key_sha256 for job in ordered
    )
    if len(set(expected_keys)) != len(expected_keys):
        raise OfflineReplayAdapterError("B0 materialization keys are duplicated")
    if not ordered:
        return ()
    if workers == 1:
        executed = tuple(_execute_fixed_day_job(job) for job in ordered)
    else:
        with ProcessPoolExecutor(
            max_workers=workers,
            initializer=_mark_global_policy_day_worker,
        ) as pool:
            executed = tuple(pool.map(_execute_fixed_day_job, ordered, chunksize=1))
    result_by_key = {result.cache_key_sha256: result for result in executed}
    if set(result_by_key) != set(expected_keys):
        raise OfflineReplayAdapterError("B0 materialization result identity drifted")
    return tuple(result_by_key[key] for key in sorted(expected_keys))


def _run_global_day_input_materialization_jobs(
    jobs: Sequence[_DayReplayJob],
    *,
    total_worker_tokens: int,
) -> tuple[_DayReplayJobResult, ...]:
    """Materialize each immutable target-day input once before B0/candidates."""

    if os.environ.get(_GLOBAL_POLICY_DAY_WORKER_ENV) == "1":
        raise OfflineReplayAdapterError("nested global day-input pools are forbidden")
    workers = _validated_global_worker_count(total_worker_tokens)
    ordered = tuple(
        sorted(
            jobs,
            key=lambda job: (
                job.utc_day,
                str(job.payload.get("day_input_materialization_key_sha256", "")),
            ),
        )
    )
    if any(job.kind != "day_input_materialize" for job in ordered):
        raise OfflineReplayAdapterError("day-input materialization pool received replay work")
    expected_keys = tuple(
        _require_sha(
            job.payload.get("day_input_materialization_key_sha256"),
            label="expected mmap materialization key SHA256",
        )
        for job in ordered
    )
    if len(set(expected_keys)) != len(expected_keys):
        raise OfflineReplayAdapterError("day-input materialization keys are duplicated")
    if not ordered:
        return ()
    if workers == 1:
        executed = tuple(_execute_fixed_day_job(job) for job in ordered)
    else:
        with ProcessPoolExecutor(
            max_workers=workers,
            initializer=_mark_global_policy_day_worker,
        ) as pool:
            executed = tuple(pool.map(_execute_fixed_day_job, ordered, chunksize=1))
    result_by_key = {result.cache_key_sha256: result for result in executed}
    if set(result_by_key) != set(expected_keys):
        raise OfflineReplayAdapterError("day-input materialization result identity drifted")
    return tuple(result_by_key[key] for key in sorted(expected_keys))


def _clause(*literals: TriLiteral) -> AndClause:
    return AndClause(tuple(sorted(literals)))


def _rule(action: str, *clauses: AndClause) -> BooleanRule:
    return BooleanRule(action=action, clauses=tuple(sorted(clauses, key=lambda item: item.key)))


def _fixed_policy(side: str, kind: str) -> BooleanCooldownPolicy:
    vocabulary = duration_vocabulary(side)
    long_action = vocabulary[-1]
    short_action = vocabulary[2]
    medium_action = vocabulary[3]
    age = TriLiteral(successor.CURRENT_CAMPAIGN_AGE)
    not_age = TriLiteral(successor.CURRENT_CAMPAIGN_AGE, True)
    long_cross = TriLiteral(successor.CURRENT_LONG_CROSS)
    not_long_cross = TriLiteral(successor.CURRENT_LONG_CROSS, True)
    short_cross = TriLiteral(successor.CURRENT_SHORT_CROSS)
    not_short_cross = TriLiteral(successor.CURRENT_SHORT_CROSS, True)
    if kind == "B1_CAMPAIGN_AGE_ONLY":
        rules = (_rule(long_action, _clause(age)),)
    elif kind == "B2_CAMPAIGN_PLUS_H16_H256":
        rules = (
            _rule(long_action, _clause(age)),
            _rule(short_action, _clause(long_cross, not_age)),
            _rule(medium_action, _clause(not_long_cross, not_age)),
        )
    elif kind == "B3_CURRENT_SEMANTIC_EQUIVALENT":
        rules = (
            _rule(
                long_action,
                _clause(short_cross, age),
                _clause(not_short_cross, age),
            ),
            _rule(short_action, _clause(long_cross, not_age)),
            _rule(medium_action, _clause(not_long_cross, not_age)),
        )
    else:  # pragma: no cover - private caller is frozen above.
        raise OfflineReplayAdapterError(f"unknown fixed candidate: {kind}")
    return BooleanCooldownPolicy(side=side, rules=rules)


def _contains_pair(name: str, prefix: str) -> bool:
    expected = successor._parse_ema_pair(prefix)
    observed = successor._parse_ema_pair(str(name).lower())
    return expected is not None and observed == expected


def _is_mid_ema_predicate(name: str) -> bool:
    lowered = str(name).lower()
    return "::mid_usdc_per_btc__h" in lowered or lowered.startswith("predicate::ema_pair_")


def _is_non_mid_market_predicate(name: str) -> bool:
    lowered = str(name).lower()
    return any(
        f"::{channel.lower()}::" in lowered or f"::{channel.lower()}__" in lowered
        for channel in _NON_MID_CHANNELS
    )


def _is_true_m2_predicate(name: str) -> bool:
    lowered = str(name).lower()
    return any(
        f"::{channel.lower()}::" in lowered or f"::{channel.lower()}__" in lowered
        for channel in _M2_INCREMENTAL_CHANNELS
    )


def _profile(
    name: str, *, feature_count: int, higher_order: bool
) -> successor.SuccessorSearchProfile:
    return successor.SuccessorSearchProfile(
        name=name,
        feature_budget=max(1024, int(feature_count)),
        max_depth=6 if higher_order else 4,
        max_leaf_nodes=32 if higher_order else 16,
        min_samples_leaf=30,
        max_rules=7 if higher_order else 3,
        max_clauses_per_rule=16 if higher_order else 8,
        max_literals_per_clause=6 if higher_order else 3,
    )


def _validate_e2_semantics(names: Sequence[str]) -> None:
    lowered = tuple(str(name).lower() for name in names)
    missing: list[str] = []
    for prefix in successor.full_ema_pair_prefixes():
        pair_names = tuple(name for name in lowered if _contains_pair(name, prefix))
        for semantic, tokens in _E2_SEMANTIC_TOKENS.items():
            if semantic == "normalized_distance":
                present = any(any(token in name for token in tokens) for name in pair_names) or any(
                    "tri::quantile::" in name and "distance" in name for name in pair_names
                )
            else:
                present = any(any(token in name for token in tokens) for name in pair_names)
            if not present:
                missing.append(f"{prefix}:{semantic}")
    if missing:
        raise OfflineReplayAdapterMechanicsMissing(missing, context="E2 per-pair semantic universe")


def _require_exact_b0_bindings(bindings: backend.FormalExecutionBindings) -> None:
    if not isinstance(bindings, backend.FormalExecutionBindings):
        raise OfflineReplayAdapterError("formal replay bindings use a custom type")
    expected = {
        "exact_owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        "exact_owner_predicate_bundle_sha256": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "exact_owner_private_config_sha256": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
    }
    for name, value in expected.items():
        if getattr(bindings, name) != value:
            raise OfflineReplayAdapterError(f"exact B0 binding drifted: {name}")


def _forbidden_injection_columns(columns: Sequence[Any]) -> tuple[str, ...]:
    return tuple(
        sorted(
            str(column)
            for column in columns
            if any(token in str(column).lower() for token in _FORBIDDEN_INJECTION_PARTS)
        )
    )


def _validate_replay_input_frame(
    replay_inputs: pd.DataFrame,
    *,
    bindings: backend.FormalExecutionBindings,
    replay_input_sha256: str,
    side: str,
    days: Sequence[str],
    allow_purged_day_subset: bool = False,
) -> pd.DataFrame:
    if not isinstance(replay_inputs, pd.DataFrame) or replay_inputs.empty:
        raise OfflineReplayAdapterError("replay inputs must be a non-empty DataFrame")
    _require_exact_b0_bindings(bindings)
    if replay_input_sha256 != _frame_sha256(replay_inputs):
        raise OfflineReplayAdapterError("replay input frame SHA256 drifted")
    if forbidden := _forbidden_injection_columns(replay_inputs.columns):
        raise OfflineReplayAdapterError(
            f"arbitrary replay/evaluator injection is forbidden: {list(forbidden)}"
        )
    missing = sorted(_COMMON_REPLAY_COLUMNS - set(replay_inputs.columns))
    if missing:
        raise OfflineReplayAdapterMechanicsMissing(missing, context="common replay inputs")
    rows = replay_inputs.copy()
    if rows.index.has_duplicates:
        raise OfflineReplayAdapterError("replay input opportunity index is duplicated")
    identifiers = rows["opportunity_id"].astype(str)
    if identifiers.str.strip().eq("").any() or tuple(identifiers) != tuple(
        str(value) for value in rows.index
    ):
        raise OfflineReplayAdapterError("replay input opportunity identity drifted")
    rows["utc_day"] = rows["utc_day"].map(_normalize_day)
    expected_days = tuple(_normalize_day(day) for day in days)
    observed_days = set(rows["utc_day"])
    expected_day_set = set(expected_days)
    if (
        not expected_days
        or not observed_days <= expected_day_set
        or (not allow_purged_day_subset and observed_days != expected_day_set)
    ):
        raise OfflineReplayAdapterError("replay input day scope drifted")
    rows["side"] = rows["side"].map(_normalize_side)
    if set(rows["side"]) != {_normalize_side(side)}:
        raise OfflineReplayAdapterError("replay inputs pooled or changed sides")
    fixed_values = {
        "replay_engine": REPLAY_ENGINE,
        "queue_identity": QUEUE_IDENTITY,
        "same_millisecond_ambiguity_policy": SAME_MILLISECOND_AMBIGUITY_POLICY,
        "exact_owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        "exact_owner_predicate_bundle_sha256": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        "exact_owner_private_config_sha256": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
        "economic_outcomes_read": False,
        "labels_read": False,
        "candidate_actions_generated": False,
    }
    for column, expected in fixed_values.items():
        values = set(rows[column].tolist())
        if values != {expected}:
            raise OfflineReplayAdapterError(f"replay input contract drifted: {column}")
    allowed_owner_actions = set(duration_vocabulary(_normalize_side(side)))
    if set(rows["exact_owner_action"].astype(str)) - allowed_owner_actions:
        raise OfflineReplayAdapterError("row-wise exact owner action vocabulary drifted")
    for column in (
        "replay_input_receipt_sha256",
        "exact_owner_policy_sha256",
        "exact_owner_predicate_bundle_sha256",
        "exact_owner_private_config_sha256",
    ):
        if (
            not rows[column]
            .astype(str)
            .map(lambda value: _SHA_RE.fullmatch(value) is not None)
            .all()
        ):
            raise OfflineReplayAdapterError(f"invalid replay input SHA256: {column}")
    return rows


def _require_executable_replay_inputs(
    rows: pd.DataFrame,
    *,
    context: str,
    label_scope: bool,
) -> None:
    required = set(_EXECUTABLE_REPLAY_COLUMNS)
    if label_scope:
        required.update(_LABEL_SCOPE_COLUMNS)
    missing = sorted(required - set(rows.columns))
    if missing:
        raise OfflineReplayAdapterMechanicsMissing(missing, context=context)


def _unique_column_value(rows: pd.DataFrame, column: str) -> Any:
    values = rows[column].drop_duplicates().tolist()
    if len(values) != 1:
        raise OfflineReplayAdapterError(f"replay input binding is not unique: {column}")
    return values[0]


def _validate_d_plus_one_contract(rows: pd.DataFrame) -> None:
    expected = rows["utc_day"].map(
        lambda value: (pd.Timestamp(value) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    )
    observed = rows["d_plus_1_utc_day"].map(_normalize_day)
    if not observed.equals(expected):
        raise OfflineReplayAdapterError("D+1 context day is not the next natural UTC day")
    for column in (
        "d_plus_1_market_identity_sha256",
        "d_plus_1_feature_identity_sha256",
        "d_plus_1_native_observation_sha256",
        "d_plus_1_context_receipt_sha256",
    ):
        if (
            not rows[column]
            .astype(str)
            .map(lambda value: _SHA_RE.fullmatch(value) is not None)
            .all()
        ):
            raise OfflineReplayAdapterError(f"D+1 context SHA256 is invalid: {column}")
    if rows["d_plus_1_new_target_assignments_allowed"].astype(bool).any():
        raise OfflineReplayAdapterError("D+1 context cannot create target assignments")
    if rows["target_day_end_terminalized"].astype(bool).any():
        raise OfflineReplayAdapterError("target UTC day-end cannot be treated as terminal")
    if not rows["assignment_to_common_washout_required"].astype(bool).all():
        raise OfflineReplayAdapterError("assignment-to-common-washout continuation is required")
    assignment = pd.to_numeric(rows["assignment_ts_ns"], errors="coerce")
    observation_end = pd.to_numeric(rows["observation_end_ts_ns"], errors="coerce")
    expected_end = rows["utc_day"].map(
        lambda value: int((pd.Timestamp(value, tz="UTC") + pd.Timedelta(days=2)).value)
    )
    if (
        assignment.isna().any()
        or observation_end.isna().any()
        or not observation_end.astype("int64").equals(expected_end.astype("int64"))
        or (observation_end <= assignment).any()
    ):
        raise OfflineReplayAdapterError(
            "observation_end_ts_ns is not the common outcome-blind D+1 bound"
        )


def _resolve_execution_options(rows: pd.DataFrame) -> _ExecutionOptions:
    binding_path = resolve_portable_path(
        str(_unique_column_value(rows, "portable_replay_binding_path"))
    ).resolve()
    expected_binding_sha = _require_sha(
        _unique_column_value(rows, "portable_replay_binding_sha256"),
        label="portable replay binding SHA256",
    )
    if not binding_path.is_file() or _file_sha256(binding_path) != expected_binding_sha:
        raise OfflineReplayAdapterMechanicsMissing(
            ("valid_portable_replay_binding",), context="portable execution binding"
        )
    binding = _read_json(binding_path, label="portable replay binding")
    expected_fixed_bridge = _rebind_historical_fixed_bridge(
        binding.get("fixed_bridge"), context="portable execution binding rebind"
    )
    execution_binding = dict(binding)
    execution_binding["fixed_bridge"] = dict(expected_fixed_bridge)
    if (
        execution_binding.get("schema_version") != f"{IDENTITY}.portable_replay_binding.v1"
        or execution_binding.get("identity")
        != "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_sequential_replay_input_v2"
        or not str(execution_binding.get("panel_identity", "")).endswith(
            ".offline_sequential_panel_v2"
        )
        or execution_binding.get("selected_day_count") != offline.REQUIRED_DAYS
        or len(execution_binding.get("selected_days", ())) != offline.REQUIRED_DAYS
        or dict(execution_binding.get("fixed_bridge", {})) != dict(expected_fixed_bridge)
        or execution_binding.get("target_day_end_terminalized") is not False
        or execution_binding.get("d_plus_1_new_target_assignments_allowed") is not False
        or execution_binding.get("assignment_to_common_washout_required") is not True
    ):
        raise OfflineReplayAdapterError("portable replay binding contract drifted")
    _validate_observation_batch_binding(execution_binding)
    cache_root = resolve_portable_path(
        str(_unique_column_value(rows, "portable_day_cache_root"))
    ).resolve()
    workers = _validated_worker_count(_unique_column_value(rows, "day_replay_workers"))
    governed_cache_root = (
        offline.default_layout().project_data_root / "cache" / "replay_dag"
    ).resolve()
    try:
        cache_root.relative_to(governed_cache_root)
    except ValueError as exc:
        raise OfflineReplayAdapterError(
            "portable replay cache escaped the governed replay_dag root"
        ) from exc
    if workers != DEFAULT_DAY_WORKERS:
        raise OfflineReplayAdapterError("formal day replay worker identity drifted")
    return _ExecutionOptions(
        binding=execution_binding,
        cache=DayReplayCache(cache_root),
        workers=workers,
    )


def _validate_observation_batch_binding(binding: Mapping[str, Any]) -> None:
    raw = binding.get("native_observation_batch_manifest")
    if not isinstance(raw, Mapping):
        raise OfflineReplayAdapterMechanicsMissing(
            ("native_observation_batch_manifest",),
            context="30-target/34-context observation contract",
        )
    path = resolve_portable_path(str(raw.get("path", ""))).resolve()
    expected_file_sha = _require_sha(
        raw.get("file_sha256"), label="native observation batch file SHA256"
    )
    expected_canonical_sha = _require_sha(
        raw.get("canonical_manifest_sha256"),
        label="native observation batch canonical SHA256",
    )
    if not path.is_file() or _file_sha256(path) != expected_file_sha:
        raise OfflineReplayAdapterMechanicsMissing(
            ("valid_native_observation_batch_manifest",),
            context="30-target/34-context observation contract",
        )
    payload = _read_json(path, label="native observation batch manifest")
    if (
        payload.get("identity") != observation_batch.IDENTITY
        or payload.get("schema_version") != observation_batch.SCHEMA_VERSION
        or payload.get("canonical_manifest_sha256") != expected_canonical_sha
        or expected_canonical_sha != _document_sha256(payload, "canonical_manifest_sha256")
    ):
        raise OfflineReplayAdapterError("native observation batch identity drifted")
    selected = tuple(_normalize_day(value) for value in payload.get("selected_target_days", ()))
    context = tuple(_normalize_day(value) for value in payload.get("observation_context_days", ()))
    continuation = tuple(
        _normalize_day(value) for value in payload.get("continuation_only_days", ())
    )
    if (
        len(selected) != offline.REQUIRED_DAYS
        or len(set(selected)) != offline.REQUIRED_DAYS
        or payload.get("selected_target_day_count") != offline.REQUIRED_DAYS
    ):
        raise OfflineReplayAdapterError("observation batch must bind 30 target days")
    if (
        len(context) != 34
        or len(set(context)) != 34
        or payload.get("observation_context_day_count") != 34
        or set(context) != set(selected) | set(continuation)
    ):
        raise OfflineReplayAdapterMechanicsMissing(
            ("34_distinct_observation_context_days",),
            context="30-target/34-context observation contract",
        )
    missing_continuation = sorted(set(REQUIRED_ADDITIONAL_CONTEXT_DAYS) - set(continuation))
    if missing_continuation:
        raise OfflineReplayAdapterMechanicsMissing(
            tuple(f"D+1:{day}" for day in missing_continuation),
            context="continuation-only observation days",
        )
    if set(continuation) != set(REQUIRED_ADDITIONAL_CONTEXT_DAYS):
        raise OfflineReplayAdapterError("continuation-only day universe drifted")
    if payload.get("continuation_days_create_target_assignments") is not False:
        raise OfflineReplayAdapterError(
            "continuation-only days cannot create assignments or economic rows"
        )
    permissions = payload.get("permissions")
    if not isinstance(permissions, Mapping) or any(
        value is not False for value in permissions.values()
    ):
        raise OfflineReplayAdapterError("native observation batch permissions must remain false")
    explicit_economic_contract = "continuation_days_create_economic_test_rows" in payload
    if (
        explicit_economic_contract
        and payload.get("continuation_days_create_economic_test_rows") is not False
    ):
        raise OfflineReplayAdapterError(
            "continuation-only days cannot create assignments or economic rows"
        )
    day_rows = payload.get("days")
    if not isinstance(day_rows, list) or len(day_rows) != 34:
        raise OfflineReplayAdapterMechanicsMissing(
            ("34_observation_day_receipts",),
            context="30-target/34-context observation contract",
        )
    by_day = {
        _normalize_day(row.get("utc_day")): row for row in day_rows if isinstance(row, Mapping)
    }
    if set(by_day) != set(context):
        raise OfflineReplayAdapterError("observation day receipt census drifted")
    for day in continuation:
        row = by_day[day]
        missing_fields = {
            "observation_role",
            "target_assignment_eligible",
        } - set(row)
        if missing_fields:
            raise OfflineReplayAdapterMechanicsMissing(
                tuple(f"{day}:{field}" for field in sorted(missing_fields)),
                context="continuation-only row semantics",
            )
        if (
            row.get("observation_role") != "continuation_only"
            or row.get("target_assignment_eligible") is not False
        ):
            raise OfflineReplayAdapterError(
                f"continuation-only day is assignment/economic eligible: {day}"
            )
        row_has_economic_contract = "economic_test_row_eligible" in row
        row_has_washout_contract = "washout_continuation_eligible" in row
        if row_has_economic_contract != row_has_washout_contract:
            raise OfflineReplayAdapterError(
                f"continuation-only day has a partial explicit semantics contract: {day}"
            )
        if row_has_economic_contract and (
            row.get("economic_test_row_eligible") is not False
            or row.get("washout_continuation_eligible") is not True
        ):
            raise OfflineReplayAdapterError(
                f"continuation-only day is assignment/economic eligible: {day}"
            )
    if explicit_economic_contract != all(
        "economic_test_row_eligible" in by_day[day]
        and "washout_continuation_eligible" in by_day[day]
        for day in continuation
    ):
        raise OfflineReplayAdapterError(
            "native observation batch has a mixed continuation semantics contract"
        )


def _validate_one_shot_frames(
    request: backend.CanonicalOuterTrainReplayRequest,
    outcomes: pd.DataFrame,
    supported: pd.DataFrame,
) -> None:
    expected_index = pd.Index(
        request.label_request.row_ids,
        name=outcomes.index.name if isinstance(outcomes, pd.DataFrame) else None,
    )
    nested._validate_action_label_frames(
        outcomes,
        supported,
        expected_index=expected_index,
        required_vocabulary=request.label_request.duration_vocabulary,
        exact_vocabulary=True,
    )


def _validate_sequential_rows(
    request: backend.CanonicalSequentialReplayRequest,
    rows: pd.DataFrame,
) -> pd.DataFrame:
    if not isinstance(rows, pd.DataFrame):
        raise OfflineReplayAdapterError("sequential replay rows are not a DataFrame")
    if (
        "one_shot_effect_aggregation_used" in rows
        and rows["one_shot_effect_aggregation_used"].astype(bool).any()
    ):
        raise OfflineReplayAdapterError("one-shot aggregation is forbidden")
    if (
        "repeated_sequential_policy" in rows
        and not rows["repeated_sequential_policy"].astype(bool).all()
    ):
        raise OfflineReplayAdapterError("non-sequential policy economics are forbidden")
    if (
        "exact_current_owner_row_wise_baseline" in rows
        and not rows["exact_current_owner_row_wise_baseline"].astype(bool).all()
    ):
        raise OfflineReplayAdapterError("exact B0 row-wise baseline drifted")
    try:
        return nested._validate_evaluation(rows, request.evaluation_request)
    except nested.NestedOofExecutionError as exc:
        raise OfflineReplayAdapterError(str(exc)) from exc


def _concat_sequential_day_results(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        raise OfflineReplayAdapterError("sequential replay returned no day frames")
    count_columns = sorted(
        {
            column
            for frame in frames
            for column in frame.columns
            if any(column.startswith(prefix) for prefix in nested.REQUIRED_COUNT_PREFIXES)
        }
    )
    normalized: list[pd.DataFrame] = []
    for frame in frames:
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            raise OfflineReplayAdapterError("sequential replay day frame is empty")
        day = frame.copy()
        present = [column for column in count_columns if column in day]
        for column in present:
            values = pd.to_numeric(day[column], errors="coerce")
            if values.isna().any() or (values < 0).any() or values.mod(1).ne(0).any():
                raise OfflineReplayAdapterError(f"sequential day count {column!r} is invalid")
            day[column] = values.astype("int64")
        for column in count_columns:
            if column not in day:
                day[column] = pd.Series(0, index=day.index, dtype="int64")
        normalized.append(day)
    return pd.concat(normalized, axis=0, ignore_index=True)


def _cache_key(
    *,
    adapter_artifact_sha256: str,
    bindings: backend.FormalExecutionBindings,
    candidate_policy_sha256: str,
    side: str,
    stage: str,
    fold_id: str,
    utc_day: str,
    day_rows: pd.DataFrame,
) -> DayReplayCacheKey:
    return DayReplayCacheKey(
        adapter_artifact_sha256=adapter_artifact_sha256,
        source_manifest_sha256=bindings.source_manifest_sha256,
        panel_manifest_sha256=bindings.panel_manifest_sha256,
        fold_manifest_sha256=bindings.fold_manifest_sha256,
        execution_manifest_sha256=bindings.execution_manifest_sha256,
        exact_owner_policy_sha256=bindings.exact_owner_policy_sha256,
        candidate_policy_sha256=candidate_policy_sha256,
        side=side,
        stage=stage,
        fold_id=fold_id,
        utc_day=utc_day,
        day_input_sha256=_frame_sha256(day_rows),
    )


def _prospective_b0_control_cache_key(job: _DayReplayJob) -> B0ControlCacheKey:
    if job.kind not in {"sequential", "b0_control"}:
        raise OfflineReplayAdapterError("B0 control cache requires a sequential job")
    rows = job.payload.get("replay_inputs")
    if not isinstance(rows, pd.DataFrame):
        raise OfflineReplayAdapterError("B0 control cache input frame is malformed")
    observed_day_input = _frame_sha256(rows)
    if observed_day_input != job.cache_key.day_input_sha256:
        raise OfflineReplayAdapterError("B0 control day input SHA256 drifted")
    if job.cache_key.exact_owner_policy_sha256 != offline.ACTIVE_OWNER_POLICY_SHA256:
        raise OfflineReplayAdapterError("B0 control exact-owner binding drifted")
    fixed_bridge = _validate_fixed_bridge(
        job.payload.get("fixed_bridge"), context=f"B0 control:{job.utc_day}"
    )
    binding = job.payload.get("portable_binding")
    projections = binding.get("day_projections") if isinstance(binding, Mapping) else None
    raw_projection = projections.get(job.utc_day) if isinstance(projections, Mapping) else None
    if not isinstance(raw_projection, Mapping):
        raise OfflineReplayAdapterError("B0 control day projection is missing")
    projection_payload = dict(raw_projection)
    projection_receipt = projection_payload.pop("projection_receipt_sha256", None)
    if projection_receipt != _canonical_sha256(projection_payload):
        raise OfflineReplayAdapterError("B0 control day projection receipt drifted")
    input_binding_sha256 = _require_sha(
        projection_payload.get("input_binding_sha256"),
        label="B0 canonical day input binding SHA256",
    )
    market_window_sha = _require_sha(
        _unique_column_value(rows, "market_window_identity_sha256"),
        label="B0 market window identity SHA256",
    )
    model_overlay_sha = _require_sha(
        _unique_column_value(rows, "model_overlay_identity_sha256"),
        label="B0 model overlay identity SHA256",
    )
    latency_sha = _require_sha(
        _unique_column_value(rows, "latency_identity_sha256"),
        label="B0 latency identity SHA256",
    )
    queue_random_sha = _require_sha(
        _unique_column_value(rows, "queue_random_identity_sha256"),
        label="B0 queue-random identity SHA256",
    )
    replay_receipt_sha = _require_sha(
        _unique_column_value(rows, "replay_input_receipt_sha256"),
        label="B0 replay-input receipt SHA256",
    )
    target_semantics = {
        "identity": f"{IDENTITY}.b0_target_day_semantics.v1",
        "utc_day": job.utc_day,
        "side": job.cache_key.side,
        "stage": job.cache_key.stage,
        "fold_id": job.cache_key.fold_id,
        "target_day_cutoff_ts_ns": int(
            (pd.Timestamp(job.utc_day, tz="UTC") + pd.Timedelta(days=1)).value
        ),
        "d_plus_1_utc_day": str(_unique_column_value(rows, "d_plus_1_utc_day")),
        "d_plus_1_context_receipt_sha256": str(
            _unique_column_value(rows, "d_plus_1_context_receipt_sha256")
        ),
        "d_plus_1_new_target_assignments_allowed": bool(
            _unique_column_value(rows, "d_plus_1_new_target_assignments_allowed")
        ),
        "target_day_end_terminalized": bool(
            _unique_column_value(rows, "target_day_end_terminalized")
        ),
        "assignment_to_common_washout_required": bool(
            _unique_column_value(rows, "assignment_to_common_washout_required")
        ),
        "canonical_day_input_binding_sha256": input_binding_sha256,
        "replay_input_receipt_sha256": replay_receipt_sha,
        "day_input_sha256": observed_day_input,
        "control_arm_semantics": "exact_current_owner_policy_repeated_full_day",
    }
    if (
        target_semantics["d_plus_1_new_target_assignments_allowed"] is not False
        or target_semantics["target_day_end_terminalized"] is not False
        or target_semantics["assignment_to_common_washout_required"] is not True
    ):
        raise OfflineReplayAdapterError("B0 control target-day semantics drifted")
    return B0ControlCacheKey(
        adapter_artifact_sha256=job.cache_key.adapter_artifact_sha256,
        source_manifest_sha256=job.cache_key.source_manifest_sha256,
        panel_manifest_sha256=job.cache_key.panel_manifest_sha256,
        fold_manifest_sha256=job.cache_key.fold_manifest_sha256,
        execution_manifest_sha256=job.cache_key.execution_manifest_sha256,
        exact_owner_policy_sha256=job.cache_key.exact_owner_policy_sha256,
        exact_owner_predicate_bundle_sha256=offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        exact_owner_private_config_sha256=offline.ACTIVE_PRIVATE_CONFIG_SHA256,
        fixed_bridge_sha256=_canonical_sha256(dict(fixed_bridge)),
        replay_engine=REPLAY_ENGINE,
        queue_identity=QUEUE_IDENTITY,
        same_millisecond_ambiguity_policy=SAME_MILLISECOND_AMBIGUITY_POLICY,
        side=job.cache_key.side,
        stage=job.cache_key.stage,
        fold_id=job.cache_key.fold_id,
        utc_day=job.utc_day,
        day_input_sha256=observed_day_input,
        canonical_day_input_binding_sha256=input_binding_sha256,
        market_window_identity_sha256=market_window_sha,
        model_overlay_identity_sha256=model_overlay_sha,
        latency_identity_sha256=latency_sha,
        queue_random_identity_sha256=queue_random_sha,
        replay_input_receipt_sha256=replay_receipt_sha,
        target_day_semantics_sha256=_canonical_sha256(target_semantics),
    )


def _b0_control_cache_key(
    *,
    job: _DayReplayJob,
    request: Any,
    replay: Any,
) -> B0ControlCacheKey:
    key = _prospective_b0_control_cache_key(job)
    observed = {
        "canonical_day_input_binding_sha256": str(request.input_binding_sha256),
        "market_window_identity_sha256": str(replay.market_window_identity_sha256),
        "model_overlay_identity_sha256": str(replay.model_overlay_identity_sha256),
        "latency_identity_sha256": str(replay.latency_identity_sha256),
        "queue_random_identity_sha256": str(replay.queue_random_identity_sha256),
        "replay_input_receipt_sha256": str(replay.replay_input_receipt_sha256),
    }
    for name, value in observed.items():
        if getattr(key, name) != value:
            raise OfflineReplayAdapterError(f"B0 projected {name} drifted")
    return key


def _one_shot_semantic_cache_key(
    *,
    adapter_artifact_sha256: str,
    bindings: backend.FormalExecutionBindings,
    candidate_policy_sha256: str,
    side: str,
    utc_day: str,
    day_rows: pd.DataFrame,
) -> OneShotSemanticCacheKey:
    return OneShotSemanticCacheKey(
        adapter_artifact_sha256=adapter_artifact_sha256,
        source_manifest_sha256=bindings.source_manifest_sha256,
        panel_manifest_sha256=bindings.panel_manifest_sha256,
        fold_manifest_sha256=bindings.fold_manifest_sha256,
        execution_manifest_sha256=bindings.execution_manifest_sha256,
        exact_owner_policy_sha256=bindings.exact_owner_policy_sha256,
        candidate_policy_sha256=candidate_policy_sha256,
        side=side,
        utc_day=utc_day,
        semantic_day_input_sha256=_one_shot_semantic_day_input_sha256(day_rows),
    )


def _prepare_sequential_replay(
    *,
    adapter_artifact_sha256: str,
    request: backend.CanonicalSequentialReplayRequest,
    replay_inputs: pd.DataFrame,
) -> _PreparedSequentialReplay:
    if not isinstance(request, backend.CanonicalSequentialReplayRequest):
        raise OfflineReplayAdapterError("custom sequential replay request is forbidden")
    evaluation = request.evaluation_request
    if not isinstance(evaluation, nested.EvaluationRequest):
        raise OfflineReplayAdapterError("custom evaluation request is forbidden")
    if evaluation.stage not in SEQUENTIAL_STAGES:
        raise OfflineReplayAdapterError("sequential replay stage is not inner/outer OOF")
    side = _normalize_side(evaluation.side)
    rows = _validate_replay_input_frame(
        replay_inputs,
        bindings=request.bindings,
        replay_input_sha256=request.replay_input_sha256,
        side=side,
        days=evaluation.days,
    )
    if (
        evaluation.candidate.expected_executed_policy_sha256 != (offline.ACTIVE_OWNER_POLICY_SHA256)
        and evaluation.candidate.policy is None
    ):
        raise OfflineReplayAdapterError("candidate policy artifact is missing")
    _require_executable_replay_inputs(
        rows, context="paired sequential day replay", label_scope=False
    )
    _validate_d_plus_one_contract(rows)
    options = _resolve_execution_options(rows)
    receipt = backend.build_sequential_replay_receipt(
        request,
        adapter_identity=IDENTITY,
        adapter_artifact_sha256=adapter_artifact_sha256,
    )
    cached_frames: list[pd.DataFrame] = []
    jobs: list[_DayReplayJob] = []
    for day in sorted(set(rows["utc_day"])):
        day_rows = rows.loc[rows["utc_day"] == day].copy()
        key = _cache_key(
            adapter_artifact_sha256=adapter_artifact_sha256,
            bindings=request.bindings,
            candidate_policy_sha256=(evaluation.candidate.expected_executed_policy_sha256),
            side=side,
            stage=evaluation.stage,
            fold_id=evaluation.fold_id,
            utc_day=day,
            day_rows=day_rows,
        )
        cached = options.cache.load_sequential(key)
        if cached is not None:
            cached_frames.append(cached)
            continue
        options.cache.write_progress(key, state="queued")
        jobs.append(
            _DayReplayJob(
                kind="sequential",
                utc_day=day,
                cache_key=key,
                payload={
                    "fixed_bridge": options.binding["fixed_bridge"],
                    "portable_binding": options.binding,
                    "cache_root": str(options.cache.root),
                    "replay_inputs": day_rows,
                    "candidate": evaluation.candidate,
                    "target_side": side,
                },
            )
        )
    return _PreparedSequentialReplay(
        request=request,
        options=options,
        receipt=receipt,
        cached_frames=tuple(cached_frames),
        jobs=tuple(jobs),
    )


def _build_bulk_b0_and_candidate_phases(
    prepared: Sequence[_PreparedSequentialReplay],
) -> tuple[tuple[_DayReplayJob, ...], tuple[_DayReplayJob, ...]]:
    cache_roots = {str(item.options.cache.root) for item in prepared}
    if len(cache_roots) != 1:
        raise OfflineReplayAdapterError("global policy-day batch must use one governed cache root")
    representative_by_b0: dict[str, tuple[B0ControlCacheKey, _DayReplayJob]] = {}
    candidate_jobs: list[_DayReplayJob] = []
    for item in prepared:
        for job in item.jobs:
            key = _prospective_b0_control_cache_key(job)
            existing = item.options.cache.load_b0_control(key)
            if existing is None:
                representative_by_b0.setdefault(key.cache_key_sha256, (key, job))
            candidate_jobs.append(
                replace(
                    job,
                    payload={
                        **dict(job.payload),
                        "b0_control_pre_materialized": True,
                    },
                )
            )
    b0_jobs: list[_DayReplayJob] = []
    for key_sha in sorted(representative_by_b0):
        key, representative = representative_by_b0[key_sha]
        payload = dict(representative.payload)
        payload.pop("candidate", None)
        payload.pop("target_side", None)
        payload["b0_control_cache_key"] = key.payload()
        b0_jobs.append(replace(representative, kind="b0_control", payload=payload))
    return (
        tuple(sorted(b0_jobs, key=_global_policy_day_execution_key)),
        tuple(sorted(candidate_jobs, key=_global_policy_day_execution_key)),
    )


def _bind_day_jobs_to_input_mmaps(
    jobs: Sequence[_DayReplayJob],
    *,
    cache: DayReplayCache,
    acceleration: SequentialReplayAccelerationOptions,
    total_worker_tokens: int,
) -> tuple[_DayReplayJob, ...]:
    """Cold-build or warm-open one mmap bundle per immutable day input."""

    if not jobs:
        raise OfflineReplayAdapterError("day-input mmap batch is empty")
    representative_by_key: dict[str, _DayReplayJob] = {}
    contracts: dict[str, DayInputMmapBinding] = {}
    for job in jobs:
        materialization_key = _day_input_materialization_key(job, acceleration)
        representative_by_key.setdefault(materialization_key, job)
    cold_representatives: dict[str, _DayReplayJob] = {}
    for materialization_key, representative in sorted(representative_by_key.items()):
        cached = cache.load_day_input_mmap_binding(
            materialization_key,
            acceleration=acceleration,
            verify_bundle=True,
        )
        if cached is None:
            cold_representatives[materialization_key] = representative
        else:
            contracts[materialization_key] = cached

    materialization_jobs: list[_DayReplayJob] = []
    for materialization_key in sorted(cold_representatives):
        representative = cold_representatives[materialization_key]
        payload = dict(representative.payload)
        for field in (
            "candidate",
            "target_side",
            "b0_control_pre_materialized",
            "b0_control_cache_key",
            "day_input_mmap_binding",
        ):
            payload.pop(field, None)
        payload.update(
            {
                "day_input_acceleration": acceleration.payload(),
                "day_input_materialization_key_sha256": materialization_key,
            }
        )
        materialization_jobs.append(
            replace(
                representative,
                kind="day_input_materialize",
                payload=payload,
            )
        )

    results = _run_global_day_input_materialization_jobs(
        materialization_jobs,
        total_worker_tokens=min(total_worker_tokens, DAY_INPUT_MATERIALIZATION_WORKERS),
    )
    for result in results:
        evidence = result.evidence
        if not isinstance(evidence, Mapping) or set(evidence) != {"day_input_mmap_binding"}:
            raise OfflineReplayAdapterError("day-input materialization evidence drifted")
        contract = _day_input_mmap_binding_from_payload(evidence["day_input_mmap_binding"])
        if contract.materialization_key_sha256 != result.cache_key_sha256:
            raise OfflineReplayAdapterError("day-input materialization receipt drifted")
        contracts[result.cache_key_sha256] = cache.admit_day_input_mmap_binding(
            contract,
            acceleration=acceleration,
        )

    expected_keys = {_day_input_materialization_key(job, acceleration) for job in jobs}
    if set(contracts) != expected_keys:
        raise OfflineReplayAdapterError("day-input mmap binding census drifted")

    return tuple(
        replace(
            job,
            payload={
                **dict(job.payload),
                "day_input_mmap_binding": contracts[
                    _day_input_materialization_key(job, acceleration)
                ].payload(),
            },
        )
        for job in jobs
    )


def _bind_bulk_day_input_mmaps(
    prepared: Sequence[_PreparedSequentialReplay],
    *,
    acceleration: SequentialReplayAccelerationOptions,
    total_worker_tokens: int,
) -> tuple[_PreparedSequentialReplay, ...]:
    """Bind every dependency-ready sequential job to a verified mmap bundle."""

    if not prepared:
        raise OfflineReplayAdapterError("day-input mmap batch is empty")
    cache_roots = {str(item.options.cache.root) for item in prepared}
    if len(cache_roots) != 1:
        raise OfflineReplayAdapterError("day-input mmap batch must use one governed replay cache")
    rebound_jobs = _bind_day_jobs_to_input_mmaps(
        tuple(job for item in prepared for job in item.jobs),
        cache=prepared[0].options.cache,
        acceleration=acceleration,
        total_worker_tokens=total_worker_tokens,
    )
    by_key = {job.cache_key.cache_key_sha256: job for job in rebound_jobs}
    return tuple(
        replace(
            item,
            jobs=tuple(by_key[job.cache_key.cache_key_sha256] for job in item.jobs),
        )
        for item in prepared
    )


def _mark_prepared_jobs(
    prepared: Sequence[_PreparedSequentialReplay],
    *,
    state: Literal["running", "failed"],
    detail: str | None = None,
) -> None:
    for item in prepared:
        for job in item.jobs:
            item.options.cache.write_progress(job.cache_key, state=state, detail=detail)


def _admit_prepared_sequential_results(
    prepared: _PreparedSequentialReplay,
    results_by_key: Mapping[str, _DayReplayJobResult],
) -> tuple[pd.DataFrame, ...]:
    collected = list(prepared.cached_frames)
    expected_keys = {job.cache_key.cache_key_sha256 for job in prepared.jobs}
    observed_keys = set(results_by_key)
    if observed_keys != expected_keys:
        raise OfflineReplayAdapterError("global policy-day result identity drifted")
    for job in sorted(prepared.jobs, key=_global_policy_day_result_key):
        result = results_by_key[job.cache_key.cache_key_sha256]
        if result.cache_key_sha256 != job.cache_key.cache_key_sha256:
            raise OfflineReplayAdapterError("sequential day cache identity drifted")
        if set(result.frames) != {"rows"}:
            raise OfflineReplayAdapterError("sequential day replay payload drifted")
        day_result = result.frames["rows"].copy()
        prepared.options.cache.admit_sequential(
            job.cache_key,
            day_result,
            evidence=result.evidence,
        )
        prepared.options.cache.write_progress(job.cache_key, state="complete")
        collected.append(day_result)
    return tuple(collected)


def _finalize_prepared_sequential_replay(
    prepared: _PreparedSequentialReplay,
    frames: Sequence[pd.DataFrame],
) -> backend.CanonicalSequentialReplayResult:
    request = prepared.request
    result_rows = _concat_sequential_day_results(frames)
    result_rows["sequential_batch_receipt_sha256"] = prepared.receipt["receipt_sha256"]
    result_rows["execution_manifest_sha256"] = request.bindings.execution_manifest_sha256
    result_rows["source_manifest_sha256"] = request.bindings.source_manifest_sha256
    result_rows["panel_manifest_sha256"] = request.bindings.panel_manifest_sha256
    result_rows["fold_manifest_sha256"] = request.bindings.fold_manifest_sha256
    validated = _validate_sequential_rows(request, result_rows)
    return backend.CanonicalSequentialReplayResult(
        rows=validated,
        receipt=prepared.receipt,
    )


class _CanonicalOfflineReplayAdapter:
    """Fixed implementation of ``backend.CanonicalReplayAdapter``."""

    identity = IDENTITY

    def __init__(
        self,
        *,
        acceleration: SequentialReplayAccelerationOptions | None = None,
        global_worker_tokens: int = DEFAULT_GLOBAL_POLICY_DAY_WORKERS,
        cpp_qualification_receipt_sha256: str | None = None,
    ) -> None:
        if acceleration is not None and not isinstance(
            acceleration, SequentialReplayAccelerationOptions
        ):
            raise OfflineReplayAdapterError("executor acceleration options type drifted")
        self._acceleration = acceleration
        self._global_worker_tokens = _validated_global_worker_count(global_worker_tokens)
        self._cpp_qualification_receipt_sha256 = (
            None
            if cpp_qualification_receipt_sha256 is None
            else _require_sha(
                cpp_qualification_receipt_sha256,
                label="C++ one-shot qualification receipt SHA256",
            )
        )

    @property
    def artifact_sha256(self) -> str:
        return _file_sha256(Path(__file__).resolve())

    def preflight_formal_panel_schema(
        self,
        *,
        metadata_columns: Sequence[str],
        replay_input_columns: Sequence[str],
        exact_owner_action_columns: Sequence[str],
    ) -> Mapping[str, Any]:
        """Audit canonical field coverage from bound Parquet schemas only."""

        metadata = frozenset(str(value) for value in metadata_columns)
        replay = frozenset(str(value) for value in replay_input_columns)
        owner_actions = frozenset(str(value) for value in exact_owner_action_columns)
        missing = set(nested.REQUIRED_METADATA_COLUMNS) - metadata
        missing_replay = set(_COMMON_REPLAY_COLUMNS | _EXECUTABLE_REPLAY_COLUMNS) - replay
        if "exact_owner_action" in missing_replay and "exact_owner_action" in owner_actions:
            missing_replay.remove("exact_owner_action")
        for target, source in backend.REPLAY_METADATA_DIRECT_BINDINGS.items():
            if target in missing_replay and source in metadata:
                missing_replay.remove(target)
        if (
            "fill_visible_ts_ms" in missing_replay
            and backend.REPLAY_FILL_VISIBLE_MS_SOURCE in metadata
        ):
            missing_replay.remove("fill_visible_ts_ms")
        missing.update(missing_replay)
        fields = sorted(missing)
        return {
            "identity": self.identity,
            "status": (
                MECHANICS_MISSING_STATUS if fields else backend.FORMAL_PANEL_SCHEMA_READY_STATUS
            ),
            "adapter_artifact_sha256": self.artifact_sha256,
            "missing_canonical_fields": fields,
            "fixed_canonical_api_bindings": _fixed_api_bindings(),
            "permissions": {
                "economic_outcomes_read": False,
                "validation_read": False,
                "sealed_holdout_read": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        }

    def _preflight_all_fold_zero_economic_contracts(
        self,
        mechanics: backend.OutcomeBlindMechanics,
        rows: pd.DataFrame,
        ladder: Sequence[nested.CandidateLadderEntry],
        continuous: nested.ContinuousComparatorEntry,
    ) -> Mapping[str, Any]:
        """Walk every frozen fold and side without executing an economic arm."""

        selected_days = set(mechanics.selected_days)
        outer_folds = tuple(mechanics.fold_manifest.outer_folds)
        if len(outer_folds) != 4:
            raise OfflineReplayAdapterError("zero-economic contract walk requires four outer folds")
        side_outer_contracts = 0
        side_inner_contracts = 0
        day_slots = 0
        for side in ("BUY", "SELL"):
            side_rows = rows.loc[rows["side"].map(_normalize_side) == side]
            if side_rows.empty:
                raise OfflineReplayAdapterError(f"zero-economic contract walk lacks {side} rows")
            for outer in outer_folds:
                if not isinstance(outer, Mapping):
                    raise OfflineReplayAdapterError(
                        "zero-economic outer-fold contract is malformed"
                    )
                train_days = tuple(str(day) for day in outer.get("train_days", ()))
                test_days = tuple(str(day) for day in outer.get("test_days", ()))
                if (
                    not train_days
                    or not test_days
                    or set(train_days) & set(test_days)
                    or not set(train_days + test_days) <= selected_days
                    or max(train_days) >= min(test_days)
                ):
                    raise OfflineReplayAdapterError("zero-economic outer-fold chronology drifted")
                for day in train_days + test_days:
                    if side_rows.loc[side_rows["utc_day"] == day].empty:
                        raise OfflineReplayAdapterError(
                            f"zero-economic fold lacks {side} support on {day}"
                        )
                side_outer_contracts += 1
                day_slots += len(train_days) + len(test_days)
                inner_folds = tuple(outer.get("inner_folds", ()))
                if len(inner_folds) != 3:
                    raise OfflineReplayAdapterError(
                        "zero-economic contract walk requires three inner folds"
                    )
                for inner in inner_folds:
                    if not isinstance(inner, Mapping):
                        raise OfflineReplayAdapterError(
                            "zero-economic inner-fold contract is malformed"
                        )
                    inner_train = tuple(str(day) for day in inner.get("train_days", ()))
                    inner_test = tuple(str(day) for day in inner.get("test_days", ()))
                    if (
                        not inner_train
                        or not inner_test
                        or set(inner_train) & set(inner_test)
                        or not set(inner_train + inner_test) <= set(train_days)
                        or max(inner_train) >= min(inner_test)
                    ):
                        raise OfflineReplayAdapterError(
                            "zero-economic inner-fold chronology drifted"
                        )
                    for day in inner_train + inner_test:
                        if side_rows.loc[side_rows["utc_day"] == day].empty:
                            raise OfflineReplayAdapterError(
                                f"zero-economic inner fold lacks {side} support on {day}"
                            )
                    side_inner_contracts += 1
                    day_slots += len(inner_train) + len(inner_test)
        return {
            "status": "all_fold_zero_economic_contract_walk_complete",
            "side_count": 2,
            "outer_fold_count": 4,
            "inner_fold_count": 12,
            "side_outer_contract_count": side_outer_contracts,
            "side_inner_contract_count": side_inner_contracts,
            "fold_day_slots_checked": day_slots,
            "candidate_ladder_count": len(ladder),
            "continuous_comparator_bound": continuous.name,
            "global_worker_tokens": self._global_worker_tokens,
            "mmap_acceleration_bound": self._acceleration is not None,
            "one_shot_topology": OneShotProcessTopology(
                total_worker_tokens=self._global_worker_tokens
            ).payload(),
            "day_input_materialization_workers": min(
                self._global_worker_tokens,
                DAY_INPUT_MATERIALIZATION_WORKERS,
            ),
            "economic_outcomes_read": False,
        }

    def preflight_formal_economics(
        self,
        mechanics: backend.OutcomeBlindMechanics,
    ) -> Mapping[str, Any]:
        """Validate the fixed portable bridge without reading an economic outcome."""

        if not isinstance(mechanics, backend.OutcomeBlindMechanics):
            raise OfflineReplayAdapterError("formal preflight requires OutcomeBlindMechanics")
        _require_exact_b0_bindings(mechanics.bindings)
        status = backend.MECHANICS_READY_STATUS
        missing: list[str] = []
        duration_action_contract: Mapping[str, Any] | None = None
        all_fold_walk: Mapping[str, Any] | None = None
        try:
            duration_action_contract, _actions = _load_frozen_duration_action_contract()
        except OfflineReplayAdapterMechanicsMissing as exc:
            status = MECHANICS_MISSING_STATUS
            missing = list(exc.missing)
        if status == MECHANICS_MISSING_STATUS:
            pass
        elif not isinstance(mechanics.replay_inputs, pd.DataFrame) or mechanics.replay_inputs.empty:
            status = MECHANICS_MISSING_STATUS
            missing = sorted(_COMMON_REPLAY_COLUMNS | _EXECUTABLE_REPLAY_COLUMNS)
        else:
            try:
                validated_sides: list[pd.DataFrame] = []
                for side in sorted(set(mechanics.replay_inputs["side"].map(_normalize_side))):
                    side_rows = mechanics.replay_inputs.loc[
                        mechanics.replay_inputs["side"].map(_normalize_side) == side
                    ].copy()
                    validated_sides.append(
                        _validate_replay_input_frame(
                            side_rows,
                            bindings=mechanics.bindings,
                            replay_input_sha256=_frame_sha256(side_rows),
                            side=side,
                            days=tuple(sorted(set(side_rows["utc_day"]))),
                        )
                    )
                rows = pd.concat(validated_sides, axis=0).loc[mechanics.replay_inputs.index]
                _require_executable_replay_inputs(
                    rows, context="formal replay preflight", label_scope=False
                )
                _validate_d_plus_one_contract(rows)
                options = _resolve_execution_options(rows)
                for day in sorted(set(rows["utc_day"])):
                    _canonical_day_request(
                        binding=options.binding,
                        utc_day=day,
                        replay_inputs=rows.loc[rows["utc_day"] == day],
                    )
                ladder, continuous = self.build_search_contract(mechanics)
                all_fold_walk = self._preflight_all_fold_zero_economic_contracts(
                    mechanics,
                    rows,
                    ladder,
                    continuous,
                )
            except OfflineReplayAdapterMechanicsMissing as exc:
                status = MECHANICS_MISSING_STATUS
                missing = list(exc.missing)
            except (KeyError, IndexError):
                status = MECHANICS_MISSING_STATUS
                missing = sorted(_COMMON_REPLAY_COLUMNS | _EXECUTABLE_REPLAY_COLUMNS)
        return {
            "identity": self.identity,
            "status": status,
            "adapter_artifact_sha256": self.artifact_sha256,
            "missing_canonical_fields": missing,
            "fixed_canonical_api_bindings": _fixed_api_bindings(),
            "duration_action_contract": duration_action_contract,
            "all_fold_zero_economic_contract_walk": all_fold_walk,
            "permissions": {
                "economic_outcomes_read": False,
                "validation_read": False,
                "sealed_holdout_read": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        }

    def run_exact_owner_one_day_mechanics(
        self,
        mechanics: backend.OutcomeBlindMechanics,
    ) -> Mapping[str, Any]:
        """Exercise the first admitted UTC day without retaining economic values."""

        if not isinstance(mechanics, backend.OutcomeBlindMechanics):
            raise OfflineReplayAdapterError("one-day mechanics requires OutcomeBlindMechanics")
        _require_exact_b0_bindings(mechanics.bindings)
        if not mechanics.selected_days:
            raise OfflineReplayAdapterError("one-day mechanics lacks admitted days")
        utc_day = mechanics.selected_days[0]
        rows = mechanics.replay_inputs.loc[mechanics.replay_inputs["utc_day"] == utc_day].copy()
        if rows.empty or set(rows["side"].map(_normalize_side)) != {"BUY", "SELL"}:
            raise OfflineReplayAdapterError(
                "one-day mechanics must cover both sides on the first admitted day"
            )
        validated: list[pd.DataFrame] = []
        for side in ("BUY", "SELL"):
            side_rows = rows.loc[rows["side"].map(_normalize_side) == side].copy()
            validated.append(
                _validate_replay_input_frame(
                    side_rows,
                    bindings=mechanics.bindings,
                    replay_input_sha256=_frame_sha256(side_rows),
                    side=side,
                    days=(utc_day,),
                )
            )
        rows = pd.concat(validated, axis=0).loc[rows.index]
        _require_executable_replay_inputs(
            rows,
            context="exact-owner one-day mechanics",
            label_scope=False,
        )
        _validate_d_plus_one_contract(rows)
        options = _resolve_execution_options(rows)
        diagnostic = dict(
            _execute_exact_owner_one_day_mechanics(
                utc_day=utc_day,
                portable_binding=options.binding,
                rows=rows,
            )
        )
        result: dict[str, Any] = {
            "schema_version": f"{IDENTITY}.exact_owner_one_day_mechanics.v2",
            "identity": self.identity,
            "status": "exact_owner_one_day_mechanics_complete",
            "adapter_artifact_sha256": self.artifact_sha256,
            "execution_manifest_sha256": (mechanics.bindings.execution_manifest_sha256),
            "source_manifest_sha256": mechanics.bindings.source_manifest_sha256,
            "panel_manifest_sha256": mechanics.bindings.panel_manifest_sha256,
            "fold_manifest_sha256": mechanics.bindings.fold_manifest_sha256,
            "mechanics_receipt_sha256": mechanics.mechanics_receipt_sha256,
            "exact_owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
            "worker_count": 1,
            **diagnostic,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        }
        result["receipt_sha256"] = _document_sha256(result, "receipt_sha256")
        return result

    def build_search_contract(
        self,
        mechanics: backend.OutcomeBlindMechanics,
    ) -> tuple[
        tuple[nested.CandidateLadderEntry, ...],
        nested.ContinuousComparatorEntry,
    ]:
        if not isinstance(mechanics, backend.OutcomeBlindMechanics):
            raise OfflineReplayAdapterError("search contract requires OutcomeBlindMechanics")
        _require_exact_b0_bindings(mechanics.bindings)
        names = tuple(sorted(str(name) for name in mechanics.panel.boolean_features.columns))
        required_fixed = {
            successor.CURRENT_CAMPAIGN_AGE,
            successor.CURRENT_SHORT_CROSS,
            successor.CURRENT_LONG_CROSS,
        }
        if missing_fixed := sorted(required_fixed - set(names)):
            raise OfflineReplayAdapterMechanicsMissing(
                missing_fixed, context="fixed candidate predicates"
            )
        prefixes = successor.full_ema_pair_prefixes()
        e1 = tuple(
            name
            for name in names
            if _is_mid_ema_predicate(name)
            and any(
                _contains_pair(name, prefix)
                and any(token in name.lower() for token in ("ordering", "favorable"))
                for prefix in prefixes
            )
        )
        universe = successor.audit_full_ema_universe(e1)
        if not universe["all_45_pairs_present"]:
            raise OfflineReplayAdapterMechanicsMissing(
                tuple(
                    f"ema_pair_h{str(fast).replace('.', 'p')}s_h{str(slow).replace('.', 'p')}s"
                    for fast, slow in universe["missing_pairs_s"]
                ),
                context="E1 full 45-pair universe",
            )
        e2 = tuple(
            name
            for name in names
            if _is_mid_ema_predicate(name)
            and name not in {successor.CURRENT_SHORT_CROSS, successor.CURRENT_LONG_CROSS}
            and any(_contains_pair(name, prefix) for prefix in prefixes)
        )
        _validate_e2_semantics(e2)
        m0 = tuple(
            name
            for name in names
            if name.startswith("predicate::m0::") or name.startswith("tri::m0::")
        )
        if successor.CURRENT_CAMPAIGN_AGE not in m0:
            m0 = tuple(sorted((*m0, successor.CURRENT_CAMPAIGN_AGE)))
        e3 = tuple(sorted(set((*m0, *e2))))
        multichannel_incremental = tuple(
            name for name in names if _is_non_mid_market_predicate(name)
        )
        m2_incremental = tuple(
            name for name in multichannel_incremental if _is_true_m2_predicate(name)
        )
        if not m2_incremental:
            raise OfflineReplayAdapterMechanicsMissing(
                ("true_trade_or_depth_predicate",), context="M2 incremental universe"
            )
        m2 = tuple(sorted(set((*e3, *multichannel_incremental))))
        continuous = tuple(
            sorted(str(name) for name in mechanics.panel.continuous_features.columns)
        )
        if not continuous:
            raise OfflineReplayAdapterMechanicsMissing(
                ("continuous_feature",), context="continuous comparator"
            )
        sides = ("BUY", "SELL")
        fixed_by_name = {
            name: {side: _fixed_policy(side, name) for side in sides}
            for name in (
                "B1_CAMPAIGN_AGE_ONLY",
                "B2_CAMPAIGN_PLUS_H16_H256",
                "B3_CURRENT_SEMANTIC_EQUIVALENT",
            )
        }
        ladder = (
            nested.CandidateLadderEntry("B0_CURRENT_EXACT", "exact_owner"),
            nested.CandidateLadderEntry(
                "B1_CAMPAIGN_AGE_ONLY",
                "fixed",
                fixed_policy_by_side=fixed_by_name["B1_CAMPAIGN_AGE_ONLY"],
                required_features_by_side={
                    side: (successor.CURRENT_CAMPAIGN_AGE,) for side in sides
                },
            ),
            nested.CandidateLadderEntry(
                "B2_CAMPAIGN_PLUS_H16_H256",
                "fixed",
                fixed_policy_by_side=fixed_by_name["B2_CAMPAIGN_PLUS_H16_H256"],
                required_features_by_side={
                    side: (successor.CURRENT_CAMPAIGN_AGE, successor.CURRENT_LONG_CROSS)
                    for side in sides
                },
            ),
            nested.CandidateLadderEntry(
                "B3_CURRENT_SEMANTIC_EQUIVALENT",
                "fixed",
                fixed_policy_by_side=fixed_by_name["B3_CURRENT_SEMANTIC_EQUIVALENT"],
                required_features_by_side={side: tuple(sorted(required_fixed)) for side in sides},
            ),
            nested.CandidateLadderEntry(
                "E1_FULL_EMA_BANK",
                "boolean",
                feature_names_by_side={side: e1 for side in sides},
                profiles=(
                    _profile("e1_all_45_pairs_v1", feature_count=len(e1), higher_order=False),
                ),
            ),
            nested.CandidateLadderEntry(
                "E2_DIRECTIONAL_EMA",
                "boolean",
                feature_names_by_side={side: e2 for side in sides},
                profiles=(
                    _profile(
                        "e2_all_pairs_all_semantics_v1",
                        feature_count=len(e2),
                        higher_order=False,
                    ),
                ),
            ),
            nested.CandidateLadderEntry(
                "E3_HIGHER_ORDER_BOOLEAN",
                "boolean",
                feature_names_by_side={side: e3 for side in sides},
                profiles=(
                    _profile(
                        "e3_high_order_multirule_dnf_v1",
                        feature_count=len(e3),
                        higher_order=True,
                    ),
                ),
            ),
            nested.CandidateLadderEntry(
                "M2_TRUE_INCREMENTAL",
                "boolean",
                feature_names_by_side={side: m2 for side in sides},
                required_features_by_side={side: m2_incremental for side in sides},
                profiles=(
                    _profile(
                        "m2_true_trade_depth_increment_v1",
                        feature_count=len(m2),
                        higher_order=True,
                    ),
                ),
            ),
            nested.CandidateLadderEntry(
                "ACTION_MATCHED_CONTROLS",
                "action_matched",
                match_sources=nested.LEARNED_BOOLEAN_ORDER,
            ),
        )
        comparator = nested.ContinuousComparatorEntry(
            feature_names_by_side={side: continuous for side in sides},
            profiles=(
                _profile(
                    "continuous_full_feature_comparator_v1",
                    feature_count=len(continuous),
                    higher_order=True,
                ),
            ),
        )
        nested._validate_ladder(ladder, sides)
        e3_profile = ladder[6].profiles[0]
        if (
            e3_profile.max_rules < 2
            or e3_profile.max_clauses_per_rule < 2
            or e3_profile.max_literals_per_clause < 3
        ):
            raise OfflineReplayAdapterError("E3 does not make high-order AND/OR/NOT reachable")
        if set(m2) - set(e3) != set(multichannel_incremental):
            raise OfflineReplayAdapterError("M2 cumulative market feature construction drifted")
        if not set(m2_incremental) <= set(m2) - set(e3):
            raise OfflineReplayAdapterError("M2 lacks true trade/depth incremental predicates")
        return ladder, comparator

    def generate_outer_train_one_shot_labels(
        self,
        request: backend.CanonicalOuterTrainReplayRequest,
        replay_inputs: pd.DataFrame,
    ) -> backend.CanonicalOneShotReplayResult:
        if not isinstance(request, backend.CanonicalOuterTrainReplayRequest):
            raise OfflineReplayAdapterError("custom one-shot replay request is forbidden")
        label = request.label_request
        if not isinstance(label, nested.FoldScopedOneShotLabelRequest):
            raise OfflineReplayAdapterError("custom label request is forbidden")
        side = _normalize_side(label.side)
        if tuple(label.duration_vocabulary) != tuple(duration_vocabulary(side)):
            raise OfflineReplayAdapterError("one-shot duration vocabulary drifted")
        if label.row_sha256 != _canonical_sha256(list(label.row_ids)):
            raise OfflineReplayAdapterError("one-shot row identity hash drifted")
        request_body = {
            "schema": f"{nested.IDENTITY}.fold_scoped_one_shot_label_request.v1",
            "side": side,
            "outer_fold_id": label.outer_fold_id,
            "train_days": list(label.train_days),
            "row_ids": list(label.row_ids),
            "row_sha256": label.row_sha256,
            "mechanics_sha256": label.mechanics_sha256,
            "duration_vocabulary": list(label.duration_vocabulary),
        }
        if label.request_sha256 != _canonical_sha256(request_body):
            raise OfflineReplayAdapterError("one-shot request hash drifted")
        rows = _validate_replay_input_frame(
            replay_inputs,
            bindings=request.bindings,
            replay_input_sha256=request.replay_input_sha256,
            side=side,
            days=label.train_days,
            allow_purged_day_subset=True,
        )
        if tuple(str(value) for value in rows.index) != tuple(label.row_ids):
            raise OfflineReplayAdapterError("one-shot replay rows escaped the purged request")
        missing_scope = sorted({"fold_row_role", "outer_fold_id"}.difference(rows.columns))
        if missing_scope:
            raise OfflineReplayAdapterError(
                "formal label replay scope is missing provider-owned fields: "
                + ", ".join(missing_scope)
            )
        if set(rows["fold_row_role"].astype(str)) != {"outer_train"}:
            raise OfflineReplayAdapterError("label replay row role is not outer_train")
        if set(rows["outer_fold_id"].astype(str)) != {label.outer_fold_id}:
            raise OfflineReplayAdapterError("one-shot outer fold identity drifted")
        _require_executable_replay_inputs(
            rows, context="outer-train one-shot replay", label_scope=True
        )
        _validate_d_plus_one_contract(rows)
        options = _resolve_execution_options(rows)
        candidate_bundle_sha = _canonical_sha256(
            {
                "identity": f"{IDENTITY}.one_shot_duration_bundle.v1",
                "side": side,
                "duration_vocabulary": list(label.duration_vocabulary),
            }
        )
        collected_outcomes: list[pd.DataFrame] = []
        collected_supported: list[pd.DataFrame] = []
        jobs: list[_DayReplayJob] = []
        semantic_by_cache_key: dict[str, OneShotSemanticCacheKey] = {}
        topology = OneShotProcessTopology(total_worker_tokens=self._global_worker_tokens)
        if self._cpp_qualification_receipt_sha256 is None:
            raise OfflineReplayAdapterError(
                "formal C++ one-shot execution lacks its lockstep receipt"
            )
        for day in sorted(set(rows["utc_day"])):
            day_rows = rows.loc[rows["utc_day"] == day].copy()
            key = _cache_key(
                adapter_artifact_sha256=self.artifact_sha256,
                bindings=request.bindings,
                candidate_policy_sha256=candidate_bundle_sha,
                side=side,
                stage=ONE_SHOT_STAGE,
                fold_id=label.outer_fold_id,
                utc_day=day,
                day_rows=day_rows,
            )
            semantic_key = _one_shot_semantic_cache_key(
                adapter_artifact_sha256=self.artifact_sha256,
                bindings=request.bindings,
                candidate_policy_sha256=candidate_bundle_sha,
                side=side,
                utc_day=day,
                day_rows=day_rows,
            )
            cached = options.cache.load_one_shot(key)
            if cached is not None:
                options.cache.register_one_shot_semantic(key, semantic_key)
                collected_outcomes.append(cached[0])
                collected_supported.append(cached[1])
                continue
            semantic_cached = options.cache.load_semantic_one_shot(semantic_key)
            if semantic_cached is not None:
                semantic_outcomes, semantic_supported, semantic_evidence = semantic_cached
                day_ids = tuple(str(value) for value in day_rows.index)
                expected_index = pd.Index(
                    day_ids,
                    name=semantic_outcomes.index.name,
                )
                nested._validate_action_label_frames(
                    semantic_outcomes,
                    semantic_supported,
                    expected_index=expected_index,
                    required_vocabulary=label.duration_vocabulary,
                    exact_vocabulary=True,
                )
                options.cache.admit_one_shot(
                    key,
                    semantic_outcomes,
                    semantic_supported,
                    evidence=semantic_evidence,
                )
                options.cache.write_progress(
                    key,
                    state="complete",
                    detail="semantic_fold_reuse",
                    counters={
                        "total_opportunities": len(semantic_outcomes),
                        "completed_opportunities": len(semantic_outcomes),
                        "total_arms": int(semantic_outcomes.size),
                        "completed_arms": int(semantic_outcomes.size),
                    },
                )
                options.cache.register_one_shot_semantic(key, semantic_key)
                collected_outcomes.append(semantic_outcomes)
                collected_supported.append(semantic_supported)
                continue
            options.cache.write_progress(key, state="queued")
            semantic_by_cache_key[key.cache_key_sha256] = semantic_key
            jobs.append(
                _DayReplayJob(
                    kind="one_shot",
                    utc_day=day,
                    cache_key=key,
                    payload={
                        "fixed_bridge": options.binding["fixed_bridge"],
                        "portable_binding": options.binding,
                        "cache_root": str(options.cache.root),
                        "replay_inputs": day_rows,
                        "duration_vocabulary": label.duration_vocabulary,
                        "one_shot_topology": topology.payload(),
                        "cpp_qualification_identity": (
                            "f05_cpp_one_shot_real_day_all_arm_lockstep_v23"
                        ),
                        "cpp_qualification_receipt_sha256": (
                            self._cpp_qualification_receipt_sha256
                        ),
                    },
                )
            )
        if jobs:
            try:
                if self._acceleration is None:
                    raise OfflineReplayAdapterError(
                        "formal one-shot execution requires read-only mmap acceleration"
                    )
                jobs = list(
                    _bind_day_jobs_to_input_mmaps(
                        jobs,
                        cache=options.cache,
                        acceleration=self._acceleration,
                        total_worker_tokens=self._global_worker_tokens,
                    )
                )
                for job in jobs:
                    options.cache.write_progress(job.cache_key, state="running")
                results = run_global_one_shot_day_jobs(
                    jobs,
                    total_worker_tokens=self._global_worker_tokens,
                )
            except BaseException as exc:
                for job in jobs:
                    options.cache.write_progress(
                        job.cache_key, state="failed", detail=type(exc).__name__
                    )
                raise
            by_key = {job.cache_key.cache_key_sha256: job for job in jobs}
            for result in results:
                job = by_key[result.cache_key_sha256]
                if set(result.frames) != {"outcomes", "supported"}:
                    raise OfflineReplayAdapterError("one-shot day replay payload drifted")
                outcomes = result.frames["outcomes"]
                supported = result.frames["supported"]
                day_ids = tuple(str(value) for value in job.payload["replay_inputs"].index)
                expected_index = pd.Index(day_ids, name=outcomes.index.name)
                nested._validate_action_label_frames(
                    outcomes,
                    supported,
                    expected_index=expected_index,
                    required_vocabulary=label.duration_vocabulary,
                    exact_vocabulary=True,
                )
                if not isinstance(result.evidence, Mapping):
                    raise OfflineReplayAdapterError("one-shot shared-prefix evidence is missing")
                options.cache.admit_one_shot(
                    job.cache_key,
                    outcomes,
                    supported,
                    evidence={
                        **dict(result.evidence),
                        "semantic_reuse": False,
                        "semantic_key_sha256": semantic_by_cache_key[
                            job.cache_key.cache_key_sha256
                        ].semantic_key_sha256,
                        "semantic_day_input_sha256": semantic_by_cache_key[
                            job.cache_key.cache_key_sha256
                        ].semantic_day_input_sha256,
                    },
                )
                options.cache.register_one_shot_semantic(
                    job.cache_key,
                    semantic_by_cache_key[job.cache_key.cache_key_sha256],
                )
                options.cache.write_progress(
                    job.cache_key,
                    state="complete",
                    counters={
                        "total_opportunities": len(outcomes),
                        "completed_opportunities": len(outcomes),
                        "total_arms": int(outcomes.size),
                        "completed_arms": int(outcomes.size),
                    },
                )
                collected_outcomes.append(outcomes)
                collected_supported.append(supported)
        outcomes = pd.concat(collected_outcomes, axis=0).loc[list(label.row_ids)]
        supported = pd.concat(collected_supported, axis=0).loc[list(label.row_ids)]
        _validate_one_shot_frames(request, outcomes, supported)
        return backend.CanonicalOneShotReplayResult(
            outcomes=outcomes,
            supported=supported,
            receipt=backend.build_outer_train_label_replay_receipt(
                request,
                adapter_identity=self.identity,
                adapter_artifact_sha256=self.artifact_sha256,
            ),
        )

    def evaluate_repeated_policy(
        self,
        request: backend.CanonicalSequentialReplayRequest,
        replay_inputs: pd.DataFrame,
    ) -> backend.CanonicalSequentialReplayResult:
        prepared = _prepare_sequential_replay(
            adapter_artifact_sha256=self.artifact_sha256,
            request=request,
            replay_inputs=replay_inputs,
        )
        if prepared.jobs:
            _mark_prepared_jobs((prepared,), state="running")
            try:
                results = _run_day_jobs(
                    prepared.jobs,
                    workers=prepared.options.workers,
                )
            except Exception as exc:
                _mark_prepared_jobs(
                    (prepared,),
                    state="failed",
                    detail=type(exc).__name__,
                )
                raise
            results_by_key = {result.cache_key_sha256: result for result in results}
        else:
            results_by_key = {}
        frames = _admit_prepared_sequential_results(prepared, results_by_key)
        return _finalize_prepared_sequential_replay(prepared, frames)

    def evaluate_many(
        self,
        items: Sequence[SequentialPolicyDayBatchItem],
        *,
        total_worker_tokens: int = DEFAULT_GLOBAL_POLICY_DAY_WORKERS,
    ) -> tuple[backend.CanonicalSequentialReplayResult, ...]:
        """Evaluate many frozen policies through one global policy-by-day pool."""

        if not items:
            raise OfflineReplayAdapterError("global policy-day batch is empty")
        prepared: list[_PreparedSequentialReplay] = []
        for item in items:
            if not isinstance(item, SequentialPolicyDayBatchItem):
                raise OfflineReplayAdapterError("global policy-day batch item type drifted")
            prepared.append(
                _prepare_sequential_replay(
                    adapter_artifact_sha256=self.artifact_sha256,
                    request=item.request,
                    replay_inputs=item.replay_inputs,
                )
            )
        receipt_ids = [str(item.receipt.get("receipt_sha256", "")) for item in prepared]
        if len(set(receipt_ids)) != len(receipt_ids):
            raise OfflineReplayAdapterError("global policy-day batch contains duplicate requests")
        jobs = tuple(job for item in prepared for job in item.jobs)
        if jobs:
            try:
                if self._acceleration is not None:
                    prepared = list(
                        _bind_bulk_day_input_mmaps(
                            prepared,
                            acceleration=self._acceleration,
                            total_worker_tokens=total_worker_tokens,
                        )
                    )
                b0_jobs, candidate_jobs = _build_bulk_b0_and_candidate_phases(prepared)
                _run_global_b0_control_jobs(
                    b0_jobs,
                    total_worker_tokens=total_worker_tokens,
                )
                cache = prepared[0].options.cache
                for candidate_job in candidate_jobs:
                    b0_key = _prospective_b0_control_cache_key(candidate_job)
                    if cache.load_b0_control(b0_key) is None:
                        raise OfflineReplayAdapterError(
                            "bulk B0 materialization did not admit every control"
                        )
                _mark_prepared_jobs(prepared, state="running")
                results = run_global_policy_day_jobs(
                    candidate_jobs,
                    total_worker_tokens=total_worker_tokens,
                )
            except Exception as exc:
                _mark_prepared_jobs(
                    prepared,
                    state="failed",
                    detail=type(exc).__name__,
                )
                raise
            all_results = {result.cache_key_sha256: result for result in results}
            if len(all_results) != len(results):
                raise OfflineReplayAdapterError(
                    "global policy-day results contain duplicate cache keys"
                )
        else:
            _validated_global_worker_count(total_worker_tokens)
            all_results = {}
        output: list[backend.CanonicalSequentialReplayResult] = []
        for item in prepared:
            item_keys = {job.cache_key.cache_key_sha256 for job in item.jobs}
            frames = _admit_prepared_sequential_results(
                item,
                {key: all_results[key] for key in item_keys},
            )
            output.append(_finalize_prepared_sequential_replay(item, frames))
        return tuple(output)

    def evaluate_repeated_policies(
        self,
        items: Sequence[backend.CanonicalSequentialReplayBatchRequest],
    ) -> tuple[backend.CanonicalSequentialReplayBatchResult, ...]:
        """Backend batch ABI bound to request identities and expected receipts."""

        if isinstance(items, (str, bytes)) or not isinstance(items, Sequence) or not items:
            raise OfflineReplayAdapterError("canonical sequential batch is empty or malformed")
        normalized = tuple(items)
        request_sha256s: list[str] = []
        compat_items: list[SequentialPolicyDayBatchItem] = []
        expected_receipts: list[Mapping[str, Any]] = []
        for item in normalized:
            if not isinstance(item, backend.CanonicalSequentialReplayBatchRequest):
                raise OfflineReplayAdapterError("canonical sequential batch item type drifted")
            request_sha = _require_sha(
                item.request_sha256,
                label="canonical sequential batch request SHA256",
            )
            evaluation = item.replay_request.evaluation_request
            if (
                not isinstance(evaluation, nested.EvaluationRequest)
                or request_sha != evaluation.request_sha256
            ):
                raise OfflineReplayAdapterError("canonical sequential request identity drifted")
            expected = backend.build_sequential_replay_receipt(
                item.replay_request,
                adapter_identity=self.identity,
                adapter_artifact_sha256=self.artifact_sha256,
            )
            if dict(item.expected_receipt) != expected:
                raise OfflineReplayAdapterError("canonical sequential expected receipt drifted")
            request_sha256s.append(request_sha)
            expected_receipts.append(expected)
            compat_items.append(
                SequentialPolicyDayBatchItem(
                    request=item.replay_request,
                    replay_inputs=item.replay_inputs,
                )
            )
        if len(set(request_sha256s)) != len(request_sha256s):
            raise OfflineReplayAdapterError("canonical sequential batch requests are duplicated")
        results = self.evaluate_many(
            tuple(compat_items),
            total_worker_tokens=self._global_worker_tokens,
        )
        if len(results) != len(normalized):
            raise OfflineReplayAdapterError("canonical sequential batch result census drifted")
        output: list[backend.CanonicalSequentialReplayBatchResult] = []
        for request_sha, expected, result in zip(
            request_sha256s,
            expected_receipts,
            results,
            strict=True,
        ):
            if not isinstance(result, backend.CanonicalSequentialReplayResult) or dict(
                result.receipt
            ) != dict(expected):
                raise OfflineReplayAdapterError("canonical sequential batch result receipt drifted")
            output.append(
                backend.CanonicalSequentialReplayBatchResult(
                    request_sha256=request_sha,
                    result=result,
                )
            )
        return tuple(output)

    def evaluate_repeated_policy_batch(
        self,
        items: Sequence[SequentialPolicyDayBatchItem],
        *,
        total_worker_tokens: int = DEFAULT_GLOBAL_POLICY_DAY_WORKERS,
    ) -> tuple[backend.CanonicalSequentialReplayResult, ...]:
        """Compatibility alias for backend callers migrating to ``evaluate_many``."""

        return self.evaluate_many(
            items,
            total_worker_tokens=total_worker_tokens,
        )


def build_canonical_replay_adapter(
    *,
    acceleration: SequentialReplayAccelerationOptions | None = None,
    global_worker_tokens: int = DEFAULT_GLOBAL_POLICY_DAY_WORKERS,
    cpp_qualification_receipt_sha256: str | None = None,
) -> backend.CanonicalReplayAdapter:
    """Build the sole fixed adapter; no caller-supplied evaluator is accepted."""

    return _CanonicalOfflineReplayAdapter(
        acceleration=acceleration,
        global_worker_tokens=global_worker_tokens,
        cpp_qualification_receipt_sha256=cpp_qualification_receipt_sha256,
    )


__all__ = [
    "B0_CONTROL_CACHE_SCHEMA",
    "B0ControlCacheKey",
    "B0ControlPath",
    "DAY_INPUT_CACHE_IDENTITY",
    "DAY_INPUT_MATERIALIZATION_WORKERS",
    "DAY_CACHE_SCHEMA",
    "DEFAULT_DAY_WORKERS",
    "DEFAULT_GLOBAL_POLICY_DAY_WORKERS",
    "DAY_INPUT_MMAP_BINDING_SCHEMA",
    "DayReplayCache",
    "DayReplayCacheKey",
    "DayInputMmapBinding",
    "EXECUTOR_ACCELERATION_IDENTITY",
    "IDENTITY",
    "GLOBAL_SEQUENTIAL_WORKER_TOKENS",
    "MAX_DAY_WORKERS",
    "MAX_GLOBAL_POLICY_DAY_WORKERS",
    "MECHANICS_MISSING_STATUS",
    "MIN_DAY_WORKERS",
    "OfflineReplayAdapterError",
    "OfflineReplayAdapterMechanicsMissing",
    "OneShotProcessTopology",
    "ONE_SHOT_DAY_PARENT_WORKERS",
    "ONE_SHOT_SUPERVISOR_WORKERS",
    "ONE_SHOT_TOTAL_WORKER_TOKENS",
    "REQUIRED_ADDITIONAL_CONTEXT_DAYS",
    "SequentialReplayAccelerationOptions",
    "SequentialPolicyDayBatchItem",
    "build_canonical_replay_adapter",
    "run_global_one_shot_day_jobs",
    "run_global_policy_day_jobs",
]
