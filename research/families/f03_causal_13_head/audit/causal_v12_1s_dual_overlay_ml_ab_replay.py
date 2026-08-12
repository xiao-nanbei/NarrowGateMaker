#!/usr/bin/env python3
"""Dual-overlay replay ABI for v9 10s ML-ON versus causal-v12 1s ML-ON.

The predecessor ``causal_v12_1s_ml_ab_replay`` compared ML-OFF with the 1s
candidate.  That pair is not the frozen F03 cadence estimand.  This successor
keeps ML enabled in both arms and replaces only the bound prediction schedule.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from models.backtest_config import load_operational_baseline_binding
from models.replay_cache_components import (
    MODEL_OVERLAY_SCHEMA_SHA256,
    MODEL_OVERLAY_SCHEMA_VERSION,
    load_model_overlay,
)
from models.replay_cache_components import (
    canonical_sha256 as component_canonical_sha256,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_ml_ab_replay as candidate_abi,
)

SCHEMA_VERSION = "causal_v12_1s_dual_overlay_ml_ab_replay.v1"
CONTROL_CADENCE_MS = 10_000
CONTROL_MAIN_ARRAY_COUNT = 21
ARMS = ("v9_10s_ml_on", "candidate_1s_ml_on")


class DualOverlayReplayError(ValueError):
    """Raised when either ML-ON schedule is not authoritative."""


@dataclass(frozen=True, slots=True)
class V9ControlSchedule:
    utc_day: str
    ml_data: tuple[Any, ...]
    ready_ts_ms: np.ndarray
    target_grid_row_count: int
    cache_root: Path
    identity: Mapping[str, Any]
    identity_sha256: str
    manifest_path: Path
    manifest_sha256: str
    data_path: Path
    data_sha256: str
    model_bundle_identity_sha256: str


@dataclass(frozen=True, slots=True)
class DualMLOnArms:
    control: Mapping[str, Any]
    candidate: Mapping[str, Any]
    baseline_id: str
    baseline_identity_path: Path
    baseline_identity_sha256: str
    baseline_config_path: Path
    baseline_config_sha256: str


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DualOverlayReplayError(f"invalid {role}: {path}") from exc
    if not isinstance(payload, dict):
        raise DualOverlayReplayError(f"{role} must be a JSON object")
    return payload


def _artifact_path(binding: Mapping[str, Any], field: str) -> Path:
    row = binding.get(field)
    if not isinstance(row, Mapping):
        raise DualOverlayReplayError(f"control overlay lacks {field} binding")
    path = Path(str(row.get("path", ""))).expanduser().resolve()
    if not path.is_file():
        raise DualOverlayReplayError(f"control overlay {field} is missing: {path}")
    if _sha256_file(path) != row.get("sha256"):
        raise DualOverlayReplayError(f"control overlay {field} SHA256 drift")
    if int(row.get("size_bytes", -1)) != path.stat().st_size:
        raise DualOverlayReplayError(f"control overlay {field} size drift")
    return path


def _validate_complete_control_grid(ready_ts_ms: np.ndarray, *, utc_day: str) -> int:
    try:
        day_start = int(np.datetime64(utc_day, "ms").astype(np.int64))
    except ValueError as exc:
        raise DualOverlayReplayError("control overlay has invalid UTC day") from exc
    # The first row is the D-1 terminal bucket becoming visible at target-day
    # 00:00:00.  This matches the candidate's canonical sample-and-hold grid.
    expected = np.arange(
        day_start,
        day_start + 86_400_000,
        CONTROL_CADENCE_MS,
        dtype=np.int64,
    )
    if not np.array_equal(ready_ts_ms, expected):
        missing = np.setdiff1d(expected, ready_ts_ms, assume_unique=False)
        extra = np.setdiff1d(ready_ts_ms, expected, assume_unique=False)
        raise DualOverlayReplayError(
            f"{utc_day} v9 control overlay is not the complete causal 10s visibility grid; "
            f"missing={len(missing)} extra={len(extra)}"
        )
    return len(expected)


def load_bound_v9_control_overlay(
    binding: Mapping[str, Any],
    *,
    expected_day: str,
    expected_model_bundle_identity_sha256: str,
) -> V9ControlSchedule:
    """Load one hash-bound current-v9 10s overlay and validate causal coverage."""

    identity = binding.get("identity")
    identity_sha = str(binding.get("identity_sha256", ""))
    if not isinstance(identity, Mapping):
        raise DualOverlayReplayError("control overlay lacks identity payload")
    if component_canonical_sha256(dict(identity)) != identity_sha:
        raise DualOverlayReplayError("control overlay identity hash is not reproducible")
    if identity.get("day") != expected_day or identity.get("symbol") != "BTCUSDC":
        raise DualOverlayReplayError("control overlay day/symbol differs from replay day")
    if identity.get("run_ml_inference") is not True:
        raise DualOverlayReplayError("control overlay was not produced by model inference")
    observed_bundle = str(identity.get("model_bundle_identity_sha256", ""))
    if observed_bundle != expected_model_bundle_identity_sha256:
        raise DualOverlayReplayError("control overlay bundle differs from v9 pointer bundle")

    manifest_path = _artifact_path(binding, "manifest")
    data_path = _artifact_path(binding, "data")
    manifest = _load_json(manifest_path, role="v9 control overlay manifest")
    if (
        manifest.get("schema_version") != MODEL_OVERLAY_SCHEMA_VERSION
        or manifest.get("schema_sha256") != MODEL_OVERLAY_SCHEMA_SHA256
        or manifest.get("identity") != dict(identity)
        or manifest.get("identity_sha256") != identity_sha
    ):
        raise DualOverlayReplayError("control overlay manifest identity/schema drift")
    layout = manifest.get("layout")
    if not isinstance(layout, Mapping) or (
        int(layout.get("main_count", -1)) != CONTROL_MAIN_ARRAY_COUNT
        or layout.get("feature_mapping_present") is not True
    ):
        raise DualOverlayReplayError("control overlay does not contain the full v9 ABI")
    files = manifest.get("files")
    if not isinstance(files, Mapping) or set(files) != {data_path.name}:
        raise DualOverlayReplayError("control overlay data manifest is incomplete")
    file_row = files[data_path.name]
    if not isinstance(file_row, Mapping) or (
        file_row.get("sha256") != _sha256_file(data_path)
        or int(file_row.get("size_bytes", -1)) != data_path.stat().st_size
    ):
        raise DualOverlayReplayError("control overlay data receipt drift")

    cache_root = Path(str(binding.get("cache_root", ""))).expanduser().resolve()
    loaded = load_model_overlay(cache_root=cache_root, identity=identity)
    if not isinstance(loaded, tuple) or len(loaded) != CONTROL_MAIN_ARRAY_COUNT + 1:
        raise DualOverlayReplayError("control overlay payload is not the full v9 tuple")
    ready = np.asarray(loaded[0], dtype=np.int64)
    if ready.ndim != 1 or len(ready) == 0 or len(np.unique(ready)) != len(ready):
        raise DualOverlayReplayError("control overlay visibility timestamps are invalid")
    target_grid_rows = _validate_complete_control_grid(ready, utc_day=expected_day)
    for index, value in enumerate(loaded[:CONTROL_MAIN_ARRAY_COUNT]):
        array = np.asarray(value)
        if array.ndim != 1 or len(array) != len(ready):
            raise DualOverlayReplayError(f"control overlay main array {index} is misaligned")
        if index and not np.isfinite(array.astype(np.float64, copy=False)).all():
            raise DualOverlayReplayError(f"control overlay main array {index} is nonfinite")
    feature_mapping = loaded[-1]
    if not isinstance(feature_mapping, Mapping) or not feature_mapping:
        raise DualOverlayReplayError("control overlay lacks its model feature mapping")
    for name, value in feature_mapping.items():
        array = np.asarray(value)
        if array.ndim != 1 or len(array) != len(ready) or array.dtype.hasobject:
            raise DualOverlayReplayError(f"control overlay feature {name} is invalid")
    ready.setflags(write=False)
    return V9ControlSchedule(
        utc_day=expected_day,
        ml_data=loaded,
        ready_ts_ms=ready,
        target_grid_row_count=target_grid_rows,
        cache_root=cache_root,
        identity=dict(identity),
        identity_sha256=identity_sha,
        manifest_path=manifest_path,
        manifest_sha256=_sha256_file(manifest_path),
        data_path=data_path,
        data_sha256=_sha256_file(data_path),
        model_bundle_identity_sha256=observed_bundle,
    )


def bind_current_v9_dual_ml_on_arms(
    base_params: Mapping[str, Any],
    *,
    pointer_path: Path | None = None,
) -> DualMLOnArms:
    """Return byte-equivalent params for two ML-ON schedules."""

    binding = load_operational_baseline_binding(pointer_path=pointer_path)
    if binding is None:
        raise DualOverlayReplayError("current operational baseline binding is missing")
    pointer = binding["pointer"]
    identity = binding["identity"]
    config = identity.get("config") or {}
    if identity.get("schema_version") != candidate_abi.EXPECTED_BASELINE_SCHEMA or (
        identity.get("baseline_id") != candidate_abi.EXPECTED_BASELINE_ID
        or pointer.get("baseline_id") != candidate_abi.EXPECTED_BASELINE_ID
    ):
        raise DualOverlayReplayError("dual-overlay replay is not bound to current v9")
    if pointer.get("ml_enabled") is not True or config.get("ml_enabled") is not True:
        raise DualOverlayReplayError("both cadence arms require v9 ML enabled")
    if any(
        bool(value)
        for value in (
            pointer.get("dynamic_fill_hazard_action_enabled"),
            pointer.get("buy_fill_selection_live_enabled"),
            config.get("dynamic_fill_hazard_action_enabled"),
            config.get("buy_fill_selection_live_enabled"),
        )
    ):
        raise DualOverlayReplayError("q90 action and BUY fill-selection must be OFF")
    params = dict(base_params)
    if bool(params.get("dynamic_fill_hazard_action_enabled", False)) or bool(
        params.get("buy_fill_selection_live_enabled", False)
    ):
        raise DualOverlayReplayError("runtime params violate the v9 control actions")
    params["ml_enabled"] = True
    return DualMLOnArms(
        control=dict(params),
        candidate=dict(params),
        baseline_id=str(pointer["baseline_id"]),
        baseline_identity_path=Path(binding["identity_path"]),
        baseline_identity_sha256=str(binding["identity_sha256"]),
        baseline_config_path=Path(binding["config_path"]),
        baseline_config_sha256=str(pointer["live_config_sha256"]),
    )


def run_dual_overlay_tick_replay(
    *,
    window: Any,
    base_params: Mapping[str, Any],
    control_schedule: V9ControlSchedule,
    candidate_schedule: candidate_abi.OneSecondPredictionSchedule,
    engine: str = "cpp",
    pointer_path: Path | None = None,
    simulate: Callable[..., Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run the frozen 10s ML-ON control and true 1s ML-ON candidate."""

    if control_schedule.utc_day != candidate_schedule.utc_day:
        raise DualOverlayReplayError("control and candidate overlay days differ")
    if getattr(window, "ml_data", None) is not None:
        raise DualOverlayReplayError("dual-overlay replay requires a model-free market window")
    if str(getattr(window, "book_source_authority", "")) != "native_formal_lifecycle":
        raise DualOverlayReplayError("dual-overlay replay requires native lifecycle authority")
    arms = bind_current_v9_dual_ml_on_arms(base_params, pointer_path=pointer_path)
    if arms.control != arms.candidate or arms.control.get("ml_enabled") is not True:
        raise DualOverlayReplayError("paired params must be identical and ML enabled")
    if simulate is None:
        from models import backtest_tick as bt

        simulate = bt._simulate_tick_with_engine
    shared = {
        "bbo_data": window.bbo_data,
        "l2_data": window.l2_data,
        "var_ti": window.var_ti,
        "var_retsq": window.var_retsq,
    }
    schedules = {
        ARMS[0]: control_schedule.ml_data,
        ARMS[1]: candidate_schedule.ml_data,
    }
    results: dict[str, Mapping[str, Any]] = {}
    for arm in ARMS:
        results[arm] = simulate(
            engine,
            window.trades,
            window.var_ts_ms,
            window.var_ssq,
            dict(getattr(arms, "control" if arm == ARMS[0] else "candidate")),
            ml_data=schedules[arm],
            **shared,
        )
    return {
        "identity": {
            "schema_version": SCHEMA_VERSION,
            "utc_day": control_schedule.utc_day,
            "comparison": "candidate_1s_ml_on_minus_v9_10s_ml_on",
            "both_arms_ml_enabled": True,
            "predecessor_ml_off_control_forbidden": True,
            "only_arm_difference": "feature_model_and_inference_cadence",
            "control_overlay_identity_sha256": control_schedule.identity_sha256,
            "control_model_bundle_identity_sha256": (control_schedule.model_bundle_identity_sha256),
            "candidate_overlay_identity_sha256": (candidate_schedule.overlay_identity_sha256),
            "candidate_bundle_sha256": candidate_schedule.research_bundle_sha256,
            "baseline_identity_sha256": arms.baseline_identity_sha256,
            "baseline_config_sha256": arms.baseline_config_sha256,
            "economic_outcomes_read": True,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authority": False,
            "live_authority": False,
        },
        "arms": results,
    }
