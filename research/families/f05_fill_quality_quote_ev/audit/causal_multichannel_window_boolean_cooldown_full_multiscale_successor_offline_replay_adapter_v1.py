"""Canonical offline replay adapter for the F05 full-multiscale successor.

This module is intentionally offline-only.  It binds the successor search
contract and the formal replay protocol, but it does not create a live hook,
observer, companion, or shadow path.  Economic replay fails closed until the
outcome-blind ``replay_inputs`` table contains the complete portable inputs
needed by the existing one-shot and repeated-policy replay bridges.

The adapter also defines deterministic day-level scheduling and an atomic,
hash-bound cache contract.  A cache entry is scoped to one UTC day, side,
fold, stage, candidate policy, exact B0 identity, and formal source identity;
outer-train labels can therefore never be reused across folds.
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
import uuid
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal

import pandas as pd

from data_paths import resolve_portable_path
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
    causal_multichannel_window_boolean_cooldown_runtime_policy as runtime_policy,
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
REQUIRED_ADDITIONAL_CONTEXT_DAYS = (
    "2026-06-29",
    "2026-07-03",
    "2026-07-16",
    "2026-08-06",
)

DAY_CACHE_SCHEMA = f"{IDENTITY}.day_cache.v1"
DAY_PROGRESS_SCHEMA = f"{IDENTITY}.day_progress.v1"
ONE_SHOT_STAGE = "outer_train_one_shot"
SEQUENTIAL_STAGES = frozenset({"inner_oof", "outer_oof"})

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
    "${NARROWGATE_ROOT}/models/private/f05_boolean_cooldown_owner_v1/"
    "predicate_bundle.json"
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
    if spec.name
    not in {item.name for item in feature_schema.CHANNELS_BY_BLOCK["M1"]}
)
_NON_MID_CHANNELS = frozenset(
    spec.name
    for spec in feature_schema.CHANNELS_BY_BLOCK["M2"]
    if spec.name != "mid_usdc_per_btc"
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


def _load_frozen_duration_action_contract() -> tuple[
    Mapping[str, Any], dict[str, tuple[Any, ...]]
]:
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
            or contract.get("schema_version")
            != f"{study.IDENTITY}.outcome_blind_inputs.v1"
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


def _requires_control_prefix_parity(
    side: Any, policy_id: Any, exact_owner_action: Any
) -> bool:
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
        return _canonical_sha256(
            {"schema_version": DAY_CACHE_SCHEMA, **self.payload()}
        )


class DayReplayCache:
    """Atomic cache for deterministic day-level one-shot and sequential results."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.entries = self.root / "entries"
        self.progress = self.root / "progress"
        self.locks = self.root / "locks"

    def _entry(self, key: DayReplayCacheKey) -> Path:
        return self.entries / key.cache_key_sha256

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

    def write_progress(
        self,
        key: DayReplayCacheKey,
        *,
        state: Literal["queued", "running", "complete", "failed"],
        detail: str | None = None,
    ) -> None:
        body: dict[str, Any] = {
            "schema_version": DAY_PROGRESS_SCHEMA,
            "cache_key_sha256": key.cache_key_sha256,
            "cache_key": key.payload(),
            "state": state,
            "detail": detail,
        }
        body["receipt_sha256"] = _document_sha256(body, "receipt_sha256")
        _atomic_json(self.progress / f"{key.cache_key_sha256}.json", body)

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
            or payload.get("receipt_sha256")
            != _document_sha256(payload, "receipt_sha256")
        ):
            raise OfflineReplayAdapterError("day replay cache manifest drifted")
        return payload

    def _admit_frames(
        self,
        key: DayReplayCacheKey,
        *,
        kind: Literal["one_shot", "sequential"],
        frames: Mapping[str, pd.DataFrame],
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
    ) -> None:
        self._admit_frames(
            key,
            kind="one_shot",
            frames={"outcomes": outcomes, "supported": supported},
        )

    def admit_sequential(self, key: DayReplayCacheKey, rows: pd.DataFrame) -> None:
        self._admit_frames(key, kind="sequential", frames={"rows": rows})

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
        if manifest.get("kind") != expected_kind or set(manifest.get("files", {})) != set(
            names
        ):
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

    def load_one_shot(
        self, key: DayReplayCacheKey
    ) -> tuple[pd.DataFrame, pd.DataFrame] | None:
        loaded = self._load_frames(
            key, expected_kind="one_shot", names=("outcomes", "supported")
        )
        if loaded is None:
            return None
        return loaded[0], loaded[1]

    def load_sequential(self, key: DayReplayCacheKey) -> pd.DataFrame | None:
        loaded = self._load_frames(key, expected_kind="sequential", names=("rows",))
        return None if loaded is None else loaded[0]


@dataclass(frozen=True, slots=True)
class _DayReplayJob:
    kind: Literal["one_shot", "sequential"]
    utc_day: str
    cache_key: DayReplayCacheKey
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _DayReplayJobResult:
    utc_day: str
    cache_key_sha256: str
    frames: Mapping[str, pd.DataFrame]


@dataclass(frozen=True, slots=True)
class _ExecutionOptions:
    binding: Mapping[str, Any]
    cache: DayReplayCache
    workers: int


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
        or payload["same_millisecond_ambiguity_policy"]
        != SAME_MILLISECOND_AMBIGUITY_POLICY
    ):
        raise OfflineReplayAdapterError("canonical day projection identity drifted")
    paths = {
        name: _bound_path(
            payload[f"{name}_path"], payload[f"{name}_sha256"], label=name
        )
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
        same_millisecond_ambiguity_policy=payload[
            "same_millisecond_ambiguity_policy"
        ],
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
    request = _canonical_day_request(
        binding=binding, utc_day=utc_day, replay_inputs=rows
    )
    projection_module = importlib.import_module(FIXED_B0_PROJECTION_MODULE)
    replay = projection_module._materialize_replay_inputs(request)
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
    return request, replay


def _day_identity_hashes(request: Any) -> dict[str, str]:
    projection = importlib.import_module(FIXED_B0_PROJECTION_MODULE)
    return dict(
        projection.CanonicalB0MechanicsAdapter().identity_hashes(request)
    )


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
    cutoff_ns = (
        int(pd.Timestamp(utc_day, tz="UTC").timestamp()) + 86_400
    ) * 1_000_000_000
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
        warmup_identity=str(
            request.source_receipts["native_source_binding_sha256"]
        ),
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
        self.predicate_bundle_sha256 = str(
            self._delegate.predicate_bundle_sha256
        )
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
            and self.predicate_bundle_sha256
            == offline.ACTIVE_PREDICATE_BUNDLE_SHA256
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
            or str(decision.predicate_bundle_sha256)
            != offline.ACTIVE_PREDICATE_BUNDLE_SHA256
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
    return _ExactOwnerArtifactEvaluator(
        expected_identity_hashes=expected_identity_hashes
    )


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
    params["cooldown_duration_policy_evaluator"] = (
        _build_exact_owner_artifact_evaluator(
            expected_identity_hashes=identity_hashes
        )
    )
    return params


def _execute_one_shot_day(job: _DayReplayJob) -> _DayReplayJobResult:
    request, replay = _canonical_day_projection(job)
    study = importlib.import_module(FIXED_ONE_SHOT_REPLAY_MODULE)
    backtest = importlib.import_module(FIXED_BACKTEST_MODULE)
    rows = job.payload["replay_inputs"]
    vocabulary = tuple(str(value) for value in job.payload["duration_vocabulary"])
    side = _normalize_side(_unique_column_value(rows, "side"))
    _duration_binding, actions_by_side = _load_frozen_duration_action_contract()
    actions = {action.policy_id: action for action in actions_by_side[side]}
    if tuple(actions) != vocabulary:
        raise OfflineReplayAdapterError("fixed duration action vocabulary drifted")
    identity_hashes = _day_identity_hashes(request)
    window = SimpleNamespace(
        trades=replay.trades,
        var_ts_ms=replay.var_ts_ms,
        var_ssq=replay.var_ssq,
    )
    shared = {
        "ml_data": replay.ml_data,
        "bbo_data": replay.bbo_data,
        "l2_data": replay.l2_data,
        "var_ti": replay.var_ti,
        "var_retsq": replay.var_retsq,
    }
    control_params = study._prepare_base_params(
        _exact_owner_runtime_params(
            request,
            replay,
            utc_day=job.utc_day,
            identity_hashes=identity_hashes,
        ),
        trace_opportunities=False,
    )
    control_params["trace_fills_max"] = study.TRACE_LIMIT
    control_result = backtest._simulate_tick_with_engine(
        "python",
        replay.trades,
        replay.var_ts_ms,
        replay.var_ssq,
        control_params,
        **shared,
    )
    control_fills = tuple(control_result.get("_fill_trace") or ())
    outcomes = pd.DataFrame(index=rows.index, columns=vocabulary, dtype=float)
    supported = pd.DataFrame(False, index=rows.index, columns=vocabulary, dtype=bool)
    for opportunity_id, opportunity in rows.iterrows():
        raw = opportunity.to_dict()
        exact_owner_action = str(raw["exact_owner_action"])
        for action_id in vocabulary:
            require_control_parity = _requires_control_prefix_parity(
                side, action_id, exact_owner_action
            )
            arm_base = _exact_owner_runtime_params(
                request,
                replay,
                utc_day=job.utc_day,
                identity_hashes=identity_hashes,
            )
            if require_control_parity:
                arm_base["trace_fills_max"] = study.TRACE_LIMIT
            trace, _elapsed = study._run_duration_arm(
                raw,
                actions[action_id],
                window=window,
                base=arm_base,
                shared=shared,
                engine="python",
                authoritative_control_fills=(
                    control_fills if require_control_parity else None
                ),
                require_control_prefix_parity=require_control_parity,
                exact_owner_baseline_policy_enabled=True,
                expected_exact_owner_action=exact_owner_action,
                expected_exact_owner_policy_sha256=(
                    offline.ACTIVE_OWNER_POLICY_SHA256
                ),
            )
            complete = bool(trace.get("arm_washout_complete", False)) and not bool(
                trace.get("right_censored", False)
            )
            if complete:
                outcomes.loc[opportunity_id, action_id] = float(
                    trace["assignment_to_washout_value_usdc"]
                )
                supported.loc[opportunity_id, action_id] = True
            else:
                outcomes.loc[opportunity_id, action_id] = float("nan")
    return _DayReplayJobResult(
        utc_day=job.utc_day,
        cache_key_sha256=job.cache_key.cache_key_sha256,
        frames={"outcomes": outcomes, "supported": supported},
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
    _duration_binding, actions_by_side = _load_frozen_duration_action_contract()
    identity_hashes = _day_identity_hashes(request)
    window = SimpleNamespace(
        trades=replay.trades,
        var_ts_ms=replay.var_ts_ms,
        var_ssq=replay.var_ssq,
    )
    shared = {
        "ml_data": replay.ml_data,
        "bbo_data": replay.bbo_data,
        "l2_data": replay.l2_data,
        "var_ti": replay.var_ti,
        "var_retsq": replay.var_retsq,
    }
    control_params = study._prepare_base_params(
        _exact_owner_runtime_params(
            request,
            replay,
            utc_day=utc_day,
            identity_hashes=identity_hashes,
        ),
        trace_opportunities=False,
    )
    control_params["trace_fills_max"] = study.TRACE_LIMIT
    control_result = backtest._simulate_tick_with_engine(
        "python",
        replay.trades,
        replay.var_ts_ms,
        replay.var_ssq,
        control_params,
        **shared,
    )
    control_fills = tuple(control_result.get("_fill_trace") or ())
    action_counts: dict[str, int] = {}
    side_counts: dict[str, int] = {}
    role_counts: dict[str, int] = {}
    complete_count = 0
    right_censored_count = 0
    for _opportunity_id, opportunity in rows.iterrows():
        raw = opportunity.to_dict()
        side = _normalize_side(raw["side"])
        action_id = str(raw["exact_owner_action"])
        actions = {
            action.policy_id: action for action in actions_by_side[side]
        }
        if action_id not in actions:
            raise OfflineReplayAdapterError(
                "one-day mechanics owner action escaped the frozen vocabulary"
            )
        arm_base = _exact_owner_runtime_params(
            request,
            replay,
            utc_day=utc_day,
            identity_hashes=identity_hashes,
        )
        # The parity gate compares the fork's fill prefix with the authoritative
        # control prefix. Both arms must therefore retain the same fill trace.
        arm_base["trace_fills_max"] = study.TRACE_LIMIT
        trace, _elapsed = study._run_duration_arm(
            raw,
            actions[action_id],
            window=window,
            base=arm_base,
            shared=shared,
            engine="python",
            authoritative_control_fills=control_fills,
            require_control_prefix_parity=True,
            exact_owner_baseline_policy_enabled=True,
            expected_exact_owner_action=action_id,
            expected_exact_owner_policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
        )
        if (
            trace.get("schema_version")
            != "multiscale_ema_boolean_cooldown_duration_fork_trace.v3"
            or trace.get("exact_owner_action") != action_id
        ):
            raise OfflineReplayAdapterError(
                "one-day mechanics exact-owner trace identity drifted"
            )
        action_counts[action_id] = action_counts.get(action_id, 0) + 1
        side_counts[side] = side_counts.get(side, 0) + 1
        role = str(raw["role_at_fill"])
        role_counts[role] = role_counts.get(role, 0) + 1
        if bool(trace.get("arm_washout_complete", False)) and not bool(
            trace.get("right_censored", False)
        ):
            complete_count += 1
        else:
            right_censored_count += 1
    row_count = int(len(rows))
    if sum(action_counts.values()) != row_count:
        raise OfflineReplayAdapterError("one-day mechanics opportunity census drifted")
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
        "trace_schema_version": (
            "multiscale_ema_boolean_cooldown_duration_fork_trace.v3"
        ),
        "economic_values_computed_inside_replay": True,
        "economic_values_persisted": False,
        "economic_values_used_for_selection": False,
    }


def _execute_sequential_day(job: _DayReplayJob) -> _DayReplayJobResult:
    request, replay = _canonical_day_projection(job)
    repeated = importlib.import_module(FIXED_REPEATED_POLICY_BRIDGE_MODULE)
    owner = importlib.import_module(FIXED_OWNER_FULL_PATH_MODULE)
    fitted = job.payload["candidate"]
    target_side = _normalize_side(job.payload["target_side"])
    cutoff_ns = (
        int(pd.Timestamp(job.utc_day, tz="UTC").timestamp()) + 86_400
    ) * 1_000_000_000
    identity_hashes = _day_identity_hashes(request)

    def emitter_factory() -> Any:
        return _build_day_snapshot_emitter(
            request,
            replay,
            utc_day=job.utc_day,
            identity_hashes=identity_hashes,
        )

    exact_owner = _build_exact_owner_artifact_evaluator(
        expected_identity_hashes=identity_hashes
    )
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
            executed_artifact_scope=(
                repeated.ExecutedArtifactScope.LEARNING_ALGORITHM_FOLD_POLICY
            ),
            executed_policy_identity=fold_policy_identity,
            executed_policy_sha256=fitted.expected_executed_policy_sha256,
            executed_predicate_bundle_sha256=_canonical_sha256(
                fitted.policy_payload
            ),
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
                    self.predicate_bundle_sha256 = (
                        artifact.executed_predicate_bundle_sha256
                    )
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
                        frozen_baseline_value = float(
                            feature.get("baseline_duration_ms")
                        )
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
                        "frozen_2025_predicate_bundle_sha256": (
                            frozen_predicates.file_sha256
                        ),
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
                    self.predicate_bundle_sha256 = (
                        artifact.executed_predicate_bundle_sha256
                    )
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
                        duration_ms=successor._duration_for_action(
                            chosen, baseline_ms
                        ),
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

    class _TargetDayOnlyEvaluator:
        def __init__(self, delegate: Any, fallback: Any) -> None:
            self._delegate = delegate
            self._fallback = fallback
            self.policy_identity = str(delegate.policy_identity)
            self.policy_sha256 = str(delegate.policy_sha256)
            self.predicate_bundle_sha256 = str(candidate_predicate_sha)
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
            assignment_ns = int(
                snapshot.m0_context.to_dict()["assignment_ts_ns"]
            )
            if assignment_ns >= cutoff_ns:
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

    guarded_candidate = _TargetDayOnlyEvaluator(
        candidate_delegate,
        _build_exact_owner_artifact_evaluator(
            expected_identity_hashes=identity_hashes
        ),
    )
    guarded_control = _TargetDayOnlyEvaluator(
        exact_owner,
        _build_exact_owner_artifact_evaluator(
            expected_identity_hashes=identity_hashes
        ),
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
    with tempfile.TemporaryDirectory(prefix="f05-offline-sequential-") as temporary:
        root = Path(temporary)
        control_summary, control_campaigns, control_fills, control_decisions = (
            owner._simulate_python_arm(
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
        )
        candidate_summary, candidate_campaigns, candidate_fills, candidate_decisions = (
            owner._simulate_python_arm(
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
        )

    identified = not (
        tuple(control_summary.get("metric_blockers") or ())
        or tuple(candidate_summary.get("metric_blockers") or ())
    )

    def value(summary: Mapping[str, Any], name: str) -> float:
        return float(summary[name]) if identified else float("nan")

    def campaign_rate(frame: pd.DataFrame, predicate: pd.Series) -> float:
        return float(predicate.mean()) if identified and not frame.empty else (
            0.0 if identified else float("nan")
        )

    policy_count = int(len(candidate_decisions))
    nonbaseline = sum(
        str(row["action_id"])
        != guarded_candidate.baseline_action_by_snapshot.get(str(row["snapshot_id"]))
        for row in candidate_decisions.to_dict("records")
    )
    result: dict[str, Any] = {
        "utc_day": job.utc_day,
        "side": target_side,
        "panel_role": offline.PANEL_ROLE,
        "candidate_terminal_value_usdc": value(
            candidate_summary, "terminal_mtm_pnl_usdc"
        ),
        "exact_owner_terminal_value_usdc": value(
            control_summary, "terminal_mtm_pnl_usdc"
        ),
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
        "candidate_target_side": target_side,
        "same_market_source": True,
        "common_random_source": True,
        "arm_local_state": True,
        "common_row_count": max(1, min(len(control_fills), len(candidate_fills))),
        "common_campaign_count": max(
            1, min(len(control_campaigns), len(candidate_campaigns))
        ),
        "candidate_closed_campaign_value_usdc": value(
            candidate_summary, "closed_campaign_value_usdc"
        ),
        "exact_owner_closed_campaign_value_usdc": value(
            control_summary, "closed_campaign_value_usdc"
        ),
        "candidate_campaign_q10_usdc": value(candidate_summary, "campaign_q10_usdc"),
        "exact_owner_campaign_q10_usdc": value(control_summary, "campaign_q10_usdc"),
        "candidate_campaign_cvar10_usdc": value(
            candidate_summary, "campaign_cvar10_usdc"
        ),
        "exact_owner_campaign_cvar10_usdc": value(
            control_summary, "campaign_cvar10_usdc"
        ),
        "candidate_inventory_time_btc_s": value(
            candidate_summary, "abs_inventory_time_btc_s"
        ),
        "exact_owner_inventory_time_btc_s": value(
            control_summary, "abs_inventory_time_btc_s"
        ),
        "candidate_max_abs_inventory_btc": value(
            candidate_summary, "max_inventory_btc"
        ),
        "exact_owner_max_abs_inventory_btc": value(
            control_summary, "max_inventory_btc"
        ),
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
        "candidate_mean_repair_time_s": value(
            candidate_summary, "mean_closed_repair_time_s"
        ),
        "exact_owner_mean_repair_time_s": value(
            control_summary, "mean_closed_repair_time_s"
        ),
        "candidate_censoring_rate": campaign_rate(
            candidate_campaigns,
            ~candidate_campaigns.get("closed", pd.Series(dtype=bool)).astype(bool),
        ),
        "exact_owner_censoring_rate": campaign_rate(
            control_campaigns,
            ~control_campaigns.get("closed", pd.Series(dtype=bool)).astype(bool),
        ),
    }
    result["paired_replay_receipt_sha256"] = _canonical_sha256(
        {
            "identity": f"{IDENTITY}.paired_sequential_day.v1",
            "utc_day": job.utc_day,
            "target_side": target_side,
            "candidate_policy_sha256": fitted.expected_executed_policy_sha256,
            "exact_owner_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
            "market_window_identity_sha256": replay.market_window_identity_sha256,
            "model_overlay_identity_sha256": replay.model_overlay_identity_sha256,
            "d_plus_1_new_target_assignments_allowed": False,
        }
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
    return _DayReplayJobResult(
        utc_day=job.utc_day,
        cache_key_sha256=job.cache_key.cache_key_sha256,
        frames={"rows": pd.DataFrame((result,))},
    )


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
    _validate_fixed_bridge(
        job.payload.get("fixed_bridge"), context=f"{job.kind}:{job.utc_day}"
    )
    if job.kind == "one_shot":
        return _execute_one_shot_day(job)
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
    return "::mid_usdc_per_btc__h" in lowered or lowered.startswith(
        "predicate::ema_pair_"
    )


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


def _profile(name: str, *, feature_count: int, higher_order: bool) -> successor.SuccessorSearchProfile:
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
        raise OfflineReplayAdapterMechanicsMissing(
            missing, context="E2 per-pair semantic universe"
        )


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
    if not expected_days or set(rows["utc_day"]) != set(expected_days):
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
        if not rows[column].astype(str).map(lambda value: _SHA_RE.fullmatch(value) is not None).all():
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
        if not rows[column].astype(str).map(lambda value: _SHA_RE.fullmatch(value) is not None).all():
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
    expected_fixed_bridge = _validate_fixed_bridge(
        binding.get("fixed_bridge"), context="portable execution binding"
    )
    if (
        binding.get("schema_version") != f"{IDENTITY}.portable_replay_binding.v1"
        or binding.get("identity")
        != "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
        "offline_sequential_replay_input_v2"
        or not str(binding.get("panel_identity", "")).endswith(
            ".offline_sequential_panel_v2"
        )
        or binding.get("selected_day_count") != offline.REQUIRED_DAYS
        or len(binding.get("selected_days", ())) != offline.REQUIRED_DAYS
        or dict(binding.get("fixed_bridge", {})) != dict(expected_fixed_bridge)
        or binding.get("target_day_end_terminalized") is not False
        or binding.get("d_plus_1_new_target_assignments_allowed") is not False
        or binding.get("assignment_to_common_washout_required") is not True
    ):
        raise OfflineReplayAdapterError("portable replay binding contract drifted")
    _validate_observation_batch_binding(binding)
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
    return _ExecutionOptions(binding=binding, cache=DayReplayCache(cache_root), workers=workers)


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
    context = tuple(
        _normalize_day(value) for value in payload.get("observation_context_days", ())
    )
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
    missing_continuation = sorted(
        set(REQUIRED_ADDITIONAL_CONTEXT_DAYS) - set(continuation)
    )
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
        raise OfflineReplayAdapterError(
            "native observation batch permissions must remain false"
        )
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
        _normalize_day(row.get("utc_day")): row
        for row in day_rows
        if isinstance(row, Mapping)
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
        if (
            row_has_economic_contract
            and (
                row.get("economic_test_row_eligible") is not False
                or row.get("washout_continuation_eligible") is not True
            )
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
    if "one_shot_effect_aggregation_used" in rows and rows[
        "one_shot_effect_aggregation_used"
    ].astype(bool).any():
        raise OfflineReplayAdapterError("one-shot aggregation is forbidden")
    if "repeated_sequential_policy" in rows and not rows[
        "repeated_sequential_policy"
    ].astype(bool).all():
        raise OfflineReplayAdapterError("non-sequential policy economics are forbidden")
    if "exact_current_owner_row_wise_baseline" in rows and not rows[
        "exact_current_owner_row_wise_baseline"
    ].astype(bool).all():
        raise OfflineReplayAdapterError("exact B0 row-wise baseline drifted")
    try:
        return nested._validate_evaluation(rows, request.evaluation_request)
    except nested.NestedOofExecutionError as exc:
        raise OfflineReplayAdapterError(str(exc)) from exc


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


class _CanonicalOfflineReplayAdapter:
    """Fixed implementation of ``backend.CanonicalReplayAdapter``."""

    identity = IDENTITY

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
                MECHANICS_MISSING_STATUS
                if fields
                else backend.FORMAL_PANEL_SCHEMA_READY_STATUS
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
                for side in sorted(
                    set(mechanics.replay_inputs["side"].map(_normalize_side))
                ):
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
                rows = pd.concat(validated_sides, axis=0).loc[
                    mechanics.replay_inputs.index
                ]
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
            raise OfflineReplayAdapterError(
                "one-day mechanics requires OutcomeBlindMechanics"
            )
        _require_exact_b0_bindings(mechanics.bindings)
        if not mechanics.selected_days:
            raise OfflineReplayAdapterError("one-day mechanics lacks admitted days")
        utc_day = mechanics.selected_days[0]
        rows = mechanics.replay_inputs.loc[
            mechanics.replay_inputs["utc_day"] == utc_day
        ].copy()
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
            "schema_version": f"{IDENTITY}.exact_owner_one_day_mechanics.v1",
            "identity": self.identity,
            "status": "exact_owner_one_day_mechanics_complete",
            "adapter_artifact_sha256": self.artifact_sha256,
            "execution_manifest_sha256": (
                mechanics.bindings.execution_manifest_sha256
            ),
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
            and name
            not in {successor.CURRENT_SHORT_CROSS, successor.CURRENT_LONG_CROSS}
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
        )
        if tuple(str(value) for value in rows.index) != tuple(label.row_ids):
            raise OfflineReplayAdapterError("one-shot replay rows escaped the purged request")
        missing_scope = sorted(
            {"fold_row_role", "outer_fold_id"}.difference(rows.columns)
        )
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
            cached = options.cache.load_one_shot(key)
            if cached is not None:
                collected_outcomes.append(cached[0])
                collected_supported.append(cached[1])
                continue
            options.cache.write_progress(key, state="queued")
            jobs.append(
                _DayReplayJob(
                    kind="one_shot",
                    utc_day=day,
                    cache_key=key,
                    payload={
                        "fixed_bridge": options.binding["fixed_bridge"],
                        "portable_binding": options.binding,
                        "replay_inputs": day_rows,
                        "duration_vocabulary": label.duration_vocabulary,
                    },
                )
            )
        if jobs:
            for job in jobs:
                options.cache.write_progress(job.cache_key, state="running")
            try:
                results = _run_day_jobs(jobs, workers=options.workers)
            except Exception as exc:
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
                options.cache.admit_one_shot(job.cache_key, outcomes, supported)
                options.cache.write_progress(job.cache_key, state="complete")
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
        if evaluation.candidate.expected_executed_policy_sha256 != (
            offline.ACTIVE_OWNER_POLICY_SHA256
        ) and evaluation.candidate.policy is None:
            raise OfflineReplayAdapterError("candidate policy artifact is missing")
        _require_executable_replay_inputs(
            rows, context="paired sequential day replay", label_scope=False
        )
        _validate_d_plus_one_contract(rows)
        options = _resolve_execution_options(rows)
        receipt = backend.build_sequential_replay_receipt(
            request,
            adapter_identity=self.identity,
            adapter_artifact_sha256=self.artifact_sha256,
        )
        collected: list[pd.DataFrame] = []
        jobs: list[_DayReplayJob] = []
        for day in sorted(set(rows["utc_day"])):
            day_rows = rows.loc[rows["utc_day"] == day].copy()
            key = _cache_key(
                adapter_artifact_sha256=self.artifact_sha256,
                bindings=request.bindings,
                candidate_policy_sha256=(
                    evaluation.candidate.expected_executed_policy_sha256
                ),
                side=side,
                stage=evaluation.stage,
                fold_id=evaluation.fold_id,
                utc_day=day,
                day_rows=day_rows,
            )
            cached = options.cache.load_sequential(key)
            if cached is not None:
                collected.append(cached)
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
                        "replay_inputs": day_rows,
                        "candidate": evaluation.candidate,
                        "target_side": side,
                    },
                )
            )
        if jobs:
            for job in jobs:
                options.cache.write_progress(job.cache_key, state="running")
            try:
                results = _run_day_jobs(jobs, workers=options.workers)
            except Exception as exc:
                for job in jobs:
                    options.cache.write_progress(
                        job.cache_key, state="failed", detail=type(exc).__name__
                    )
                raise
            by_key = {job.cache_key.cache_key_sha256: job for job in jobs}
            for result in results:
                job = by_key[result.cache_key_sha256]
                if set(result.frames) != {"rows"}:
                    raise OfflineReplayAdapterError("sequential day replay payload drifted")
                day_result = result.frames["rows"].copy()
                options.cache.admit_sequential(job.cache_key, day_result)
                options.cache.write_progress(job.cache_key, state="complete")
                collected.append(day_result)
        result_rows = pd.concat(collected, axis=0, ignore_index=True)
        result_rows["sequential_batch_receipt_sha256"] = receipt["receipt_sha256"]
        result_rows["execution_manifest_sha256"] = (
            request.bindings.execution_manifest_sha256
        )
        result_rows["source_manifest_sha256"] = request.bindings.source_manifest_sha256
        result_rows["panel_manifest_sha256"] = request.bindings.panel_manifest_sha256
        result_rows["fold_manifest_sha256"] = request.bindings.fold_manifest_sha256
        validated = _validate_sequential_rows(request, result_rows)
        return backend.CanonicalSequentialReplayResult(rows=validated, receipt=receipt)


def build_canonical_replay_adapter() -> backend.CanonicalReplayAdapter:
    """Build the sole fixed adapter; no caller-supplied evaluator is accepted."""

    return _CanonicalOfflineReplayAdapter()


__all__ = [
    "DAY_CACHE_SCHEMA",
    "DEFAULT_DAY_WORKERS",
    "DayReplayCache",
    "DayReplayCacheKey",
    "IDENTITY",
    "MAX_DAY_WORKERS",
    "MECHANICS_MISSING_STATUS",
    "MIN_DAY_WORKERS",
    "OfflineReplayAdapterError",
    "OfflineReplayAdapterMechanicsMissing",
    "REQUIRED_ADDITIONAL_CONTEXT_DAYS",
    "build_canonical_replay_adapter",
]
