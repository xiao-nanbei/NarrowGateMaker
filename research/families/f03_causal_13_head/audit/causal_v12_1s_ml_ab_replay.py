#!/usr/bin/env python3
"""Successor ABI from an admitted causal-v12 1s overlay to tick replay.

This module deliberately does not modify or call the historical 10s
``models.backtest_tick.load_ml_predictions`` loader.  It validates the complete
13-head daily overlay, projects the five prediction values consumed by the
existing quote ABI, and runs paired ML-OFF/ML-ON paths against the current v9
operational control.  Merely loading or preparing this ABI reads no economic
outcome and grants no prediction, action, or live authority.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

from models.backtest_config import (
    ML_PARAM_KEYS,
    disable_ml_params,
    load_operational_baseline_binding,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_prediction_overlay as overlays,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema
from research.families.f03_causal_13_head.audit import causal_v12_1s_training as training

SCHEMA_VERSION = "causal_v12_1s_ml_ab_replay_successor_abi.v1"
EXPECTED_BASELINE_SCHEMA = "narrowgate_operational_baseline_identity.v9"
EXPECTED_BASELINE_ID = (
    "btc_usdc_causal_v12_quote_snapshot_atomicity_v2_q90_shadow_"
    "buy_fill_selection_retired_baseline_20260804"
)
ARMS = ("ml_off", "ml_on")
REPLAY_HEADS = ("dir_10s", "vol_10s", "ret_10s", "tox_bid_10s", "tox_ask_10s")
CLASSIFICATION_HEADS = frozenset(head for head, spec in training.HEAD_SPECS.items() if spec[3])
VOLATILITY_HEADS = frozenset(head for head in training.HEAD_SPECS if head.startswith("vol_"))


class OneSecondReplayABIError(ValueError):
    """Raised when the 1s overlay or paired replay identity is not authoritative."""


@dataclass(frozen=True, slots=True)
class OneSecondPredictionSchedule:
    utc_day: str
    decision_ts_ms: np.ndarray
    feature_ready_ts_ms: np.ndarray
    predictions: Mapping[str, np.ndarray]
    manifest_path: Path
    manifest_sha256: str
    overlay_path: Path
    overlay_sha256: str
    overlay_identity_sha256: str
    research_bundle_sha256: str
    test_only: bool

    @property
    def ml_data(self) -> tuple[np.ndarray, ...]:
        """Return the frozen existing quote ABI without exporting a scalar model.

        The replay cursor treats ``decision_ts_ms`` as the availability time.
        A row selected at second ``s`` is sample-and-held for events in
        ``[s, s + 1000ms)``.  ``feature_ready_ts_ms`` is separately verified to
        be no later than that canonical decision timestamp.
        """

        return (
            self.decision_ts_ms,
            self.predictions["dir_10s"],
            self.predictions["vol_10s"],
            self.predictions["ret_10s"],
            self.predictions["tox_bid_10s"],
            self.predictions["tox_ask_10s"],
        )


@dataclass(frozen=True, slots=True)
class V9MLABArms:
    ml_off: Mapping[str, Any]
    ml_on: Mapping[str, Any]
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


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _load_json_object(path: Path, *, role: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OneSecondReplayABIError(f"invalid {role}: {path}") from exc
    if not isinstance(payload, dict):
        raise OneSecondReplayABIError(f"{role} must be a JSON object")
    return payload


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


def _validate_bundle_head_bindings(identity_payload: Mapping[str, Any]) -> str:
    bundle = identity_payload.get("research_bundle")
    if not isinstance(bundle, Mapping):
        raise OneSecondReplayABIError("overlay identity lacks research bundle binding")
    bundle_sha256 = bundle.get("bundle_sha256")
    if not _is_sha256(bundle_sha256):
        raise OneSecondReplayABIError("overlay identity lacks research bundle SHA256")
    rows = bundle.get("heads")
    if not isinstance(rows, list) or len(rows) != len(training.HEAD_SPECS):
        raise OneSecondReplayABIError("overlay must bind all 13 research heads")
    observed: list[str] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise OneSecondReplayABIError("overlay contains an invalid head binding")
        head = str(row.get("head", ""))
        if head not in training.HEAD_SPECS or head in observed:
            raise OneSecondReplayABIError("overlay head bindings are incomplete or duplicated")
        if not _is_sha256(row.get("model_sha256")) or not _is_sha256(row.get("metadata_sha256")):
            raise OneSecondReplayABIError(f"overlay head {head} lacks model/meta hashes")
        observed.append(head)
    if observed != list(training.HEAD_SPECS):
        raise OneSecondReplayABIError("overlay head order differs from the frozen 13-head schema")
    return str(bundle_sha256)


def _validate_physical_research_bundle(
    identity_payload: Mapping[str, Any],
    *,
    expected_bundle_sha256: str,
) -> None:
    bundle = identity_payload["research_bundle"]
    bundle_path = Path(str(bundle.get("bundle_path", ""))).expanduser().resolve()
    if not bundle_path.is_file():
        raise OneSecondReplayABIError("formal replay research bundle is missing")
    if _sha256_file(bundle_path) != expected_bundle_sha256:
        raise OneSecondReplayABIError("formal replay research bundle SHA256 mismatch")
    try:
        admitted = overlays.load_admitted_research_bundle(bundle_path.parent)
    except overlays.PredictionOverlayMaterializationError as exc:
        raise OneSecondReplayABIError(
            "formal replay research bundle failed physical admission"
        ) from exc
    if admitted.bundle_sha256 != expected_bundle_sha256:
        raise OneSecondReplayABIError("formal replay bundle identity mismatch")
    expected_heads = {
        row["head"]: (row["model_sha256"], row["metadata_sha256"]) for row in bundle["heads"]
    }
    actual_heads = {
        artifact.head: (artifact.model_sha256, artifact.metadata_sha256)
        for artifact in admitted.heads
    }
    if actual_heads != expected_heads:
        raise OneSecondReplayABIError("formal replay 13-head physical hashes drifted")


def _validate_prediction_values(predictions: Mapping[str, np.ndarray]) -> None:
    if tuple(predictions) != tuple(training.HEAD_SPECS):
        raise OneSecondReplayABIError("overlay prediction columns differ from all 13 heads")
    for head, values in predictions.items():
        if values.ndim != 1 or not np.isfinite(values).all():
            raise OneSecondReplayABIError(f"overlay contains invalid values for {head}")
        if head in CLASSIFICATION_HEADS and np.any((values < 0.0) | (values > 1.0)):
            raise OneSecondReplayABIError(f"classification values are outside [0,1] for {head}")
        if head in VOLATILITY_HEADS and np.any(values < 0.0):
            raise OneSecondReplayABIError(f"volatility values are negative for {head}")


def load_admitted_one_second_overlay(
    overlay_dir: Path,
    *,
    allow_test_only: bool = False,
) -> OneSecondPredictionSchedule:
    """Load one atomic 1s overlay and fail closed on any identity drift."""

    output_dir = overlay_dir.expanduser().resolve()
    manifest_path = output_dir / overlays.MANIFEST_FILENAME
    overlay_path = output_dir / overlays.OVERLAY_FILENAME
    success_path = output_dir / overlays.SUCCESS_FILENAME
    for path in (manifest_path, overlay_path, success_path):
        if not path.is_file():
            raise OneSecondReplayABIError(
                f"prediction overlay is not atomically admitted; missing {path.name}"
            )
    manifest_sha256 = _sha256_file(manifest_path)
    if success_path.read_text(encoding="ascii").strip() != manifest_sha256:
        raise OneSecondReplayABIError("prediction overlay _SUCCESS binding is invalid")
    manifest = _load_json_object(manifest_path, role="prediction overlay manifest")
    if manifest.get("schema_version") != overlays.ARTIFACT_SCHEMA_VERSION:
        raise OneSecondReplayABIError("prediction overlay artifact schema mismatch")
    if manifest.get("identity") != schema.IDENTITY:
        raise OneSecondReplayABIError("prediction overlay is not the 1s successor identity")
    if manifest.get("atomic_admission") is not True:
        raise OneSecondReplayABIError("prediction overlay is not atomically admitted")
    if manifest.get("overlay_schema") != overlays.prediction_overlay_schema_payload():
        raise OneSecondReplayABIError("prediction overlay schema payload mismatch")
    if manifest.get("head_count") != len(training.HEAD_SPECS):
        raise OneSecondReplayABIError("prediction overlay must contain all 13 heads")
    if manifest.get("feature_bucket_ms") != schema.CADENCE_MS:
        raise OneSecondReplayABIError("prediction overlay is not canonical 1s cadence")
    forbidden_true = (
        "labels_read",
        "economic_outcomes_read",
        "training_performed",
        "prediction_authorized",
        "action_authorized",
        "live_authorized",
    )
    if any(manifest.get(field) is not False for field in forbidden_true):
        raise OneSecondReplayABIError("prediction overlay violates research-only permissions")
    test_only = bool(manifest.get("test_only"))
    if test_only and not allow_test_only:
        raise OneSecondReplayABIError("test-only prediction overlay cannot enter formal replay")

    identity_payload = manifest.get("cache_identity_payload")
    overlay_identity_sha256 = manifest.get("cache_identity_sha256")
    if not isinstance(identity_payload, Mapping) or not _is_sha256(overlay_identity_sha256):
        raise OneSecondReplayABIError("prediction overlay lacks a reproducible cache identity")
    if _canonical_sha256(identity_payload) != overlay_identity_sha256:
        raise OneSecondReplayABIError("prediction overlay cache identity cannot be reproduced")
    research_bundle_sha256 = _validate_bundle_head_bindings(identity_payload)
    if not test_only:
        _validate_physical_research_bundle(
            identity_payload,
            expected_bundle_sha256=research_bundle_sha256,
        )

    overlay_info = manifest.get("overlay")
    overlay_sha256 = _sha256_file(overlay_path)
    if (
        not isinstance(overlay_info, Mapping)
        or overlay_info.get("path") != overlays.OVERLAY_FILENAME
        or overlay_info.get("sha256") != overlay_sha256
        or overlay_info.get("compression") != "zstd"
    ):
        raise OneSecondReplayABIError("prediction overlay file identity mismatch")

    parquet = pq.ParquetFile(overlay_path)
    if not parquet.schema_arrow.equals(
        overlays.prediction_overlay_arrow_schema(), check_metadata=False
    ):
        raise OneSecondReplayABIError("prediction overlay Parquet schema mismatch")
    rows = int(parquet.metadata.num_rows)
    if overlay_info.get("rows") != rows or rows <= 0:
        raise OneSecondReplayABIError("prediction overlay row denominator mismatch")
    if not test_only and rows != overlays.AUTHORITATIVE_DAILY_ROWS:
        raise OneSecondReplayABIError("formal daily overlay must contain exactly 86,400 rows")

    table = pq.read_table(overlay_path)
    cutoff = table["cutoff_exclusive_ms"].to_numpy(zero_copy_only=False).astype(np.int64)
    decision = table["decision_ts_ms"].to_numpy(zero_copy_only=False).astype(np.int64)
    ready = table["feature_ready_ts_ms"].to_numpy(zero_copy_only=False).astype(np.int64)
    utc_day = str(manifest.get("utc_day", ""))
    try:
        day_start_ms = int(
            datetime.strptime(utc_day, "%Y-%m-%d").replace(tzinfo=UTC).timestamp() * 1_000
        )
    except ValueError as exc:
        raise OneSecondReplayABIError("prediction overlay has an invalid UTC day") from exc
    expected = day_start_ms + np.arange(rows, dtype=np.int64) * schema.CADENCE_MS
    if not np.array_equal(cutoff, expected) or not np.array_equal(decision, expected):
        raise OneSecondReplayABIError("overlay decisions are not the strict canonical 1s grid")
    if np.any(ready > decision):
        raise OneSecondReplayABIError("overlay contains feature_ready > decision")
    if len(np.unique(decision)) != rows:
        raise OneSecondReplayABIError("overlay decision timestamps are not unique")

    predictions = {
        head: table[overlays.PREDICTION_COLUMN_BY_HEAD[head]]
        .to_numpy(zero_copy_only=False)
        .astype(np.float64)
        for head in training.HEAD_SPECS
    }
    _validate_prediction_values(predictions)
    for values in (decision, ready, *predictions.values()):
        values.setflags(write=False)
    return OneSecondPredictionSchedule(
        utc_day=utc_day,
        decision_ts_ms=decision,
        feature_ready_ts_ms=ready,
        predictions=predictions,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha256,
        overlay_path=overlay_path,
        overlay_sha256=overlay_sha256,
        overlay_identity_sha256=str(overlay_identity_sha256),
        research_bundle_sha256=research_bundle_sha256,
        test_only=test_only,
    )


def sample_and_hold_indices(
    prediction_ts_ms: np.ndarray,
    event_ts_ms: Sequence[int] | np.ndarray,
) -> np.ndarray:
    """Map events to the latest canonical prediction without future reads."""

    prediction_ts = np.asarray(prediction_ts_ms, dtype=np.int64)
    event_ts = np.asarray(event_ts_ms, dtype=np.int64)
    if prediction_ts.ndim != 1 or event_ts.ndim != 1:
        raise OneSecondReplayABIError("sample-and-hold inputs must be one-dimensional")
    if len(prediction_ts) == 0 or np.any(np.diff(prediction_ts) != schema.CADENCE_MS):
        raise OneSecondReplayABIError("sample-and-hold requires the canonical 1s grid")
    indices = np.searchsorted(prediction_ts, event_ts, side="right") - 1
    indices[event_ts < prediction_ts[0]] = -1
    return indices.astype(np.int64, copy=False)


def bind_current_v9_ml_ab_arms(
    base_params: Mapping[str, Any],
    *,
    pointer_path: Path | None = None,
) -> V9MLABArms:
    """Bind paired arms so only the frozen model switch can differ."""

    binding = load_operational_baseline_binding(pointer_path=pointer_path)
    if binding is None:
        raise OneSecondReplayABIError("current operational baseline binding is missing")
    pointer = binding["pointer"]
    identity = binding["identity"]
    if identity.get("schema_version") != EXPECTED_BASELINE_SCHEMA:
        raise OneSecondReplayABIError("current operational baseline is not v9")
    if identity.get("baseline_id") != EXPECTED_BASELINE_ID:
        raise OneSecondReplayABIError("current operational baseline ID differs from frozen v9")
    if pointer.get("baseline_id") != EXPECTED_BASELINE_ID:
        raise OneSecondReplayABIError("operational pointer does not bind frozen v9")
    pointer_flags = (
        bool(pointer.get("dynamic_fill_hazard_action_enabled")),
        bool(pointer.get("buy_fill_selection_live_enabled")),
    )
    identity_config = identity.get("config") or {}
    identity_flags = (
        bool(identity_config.get("dynamic_fill_hazard_action_enabled")),
        bool(identity_config.get("buy_fill_selection_live_enabled")),
    )
    if pointer_flags != (False, False) or identity_flags != (False, False):
        raise OneSecondReplayABIError(
            "v9 ML A/B requires q90 action OFF and BUY fill-selection OFF"
        )
    if pointer.get("ml_enabled") is not True or identity_config.get("ml_enabled") is not True:
        raise OneSecondReplayABIError("v9 operational baseline must bind ML enabled")

    ml_on = dict(base_params)
    ml_off = dict(base_params)
    for params in (ml_on, ml_off):
        if bool(params.get("dynamic_fill_hazard_action_enabled", False)):
            raise OneSecondReplayABIError("q90 action must be OFF in both replay arms")
        if bool(params.get("buy_fill_selection_live_enabled", False)):
            raise OneSecondReplayABIError("BUY fill-selection must be OFF in both replay arms")
    ml_on["ml_enabled"] = True
    ml_off["ml_enabled"] = False
    disable_ml_params(ml_off)
    allowed_differences = {"ml_enabled", *ML_PARAM_KEYS}
    observed_differences = {
        key for key in set(ml_on) | set(ml_off) if ml_on.get(key) != ml_off.get(key)
    }
    if not observed_differences.issubset(allowed_differences):
        raise OneSecondReplayABIError("ML A/B arms differ outside the model switch")
    for invariant in (
        "dynamic_fill_hazard_action_enabled",
        "dynamic_fill_hazard_shadow_enabled",
        "buy_fill_selection_live_enabled",
        "buy_fill_selection_shadow_enabled",
    ):
        if ml_off.get(invariant) != ml_on.get(invariant):
            raise OneSecondReplayABIError(f"ML A/B drifted invariant {invariant}")

    return V9MLABArms(
        ml_off=ml_off,
        ml_on=ml_on,
        baseline_id=EXPECTED_BASELINE_ID,
        baseline_identity_path=Path(binding["identity_path"]),
        baseline_identity_sha256=str(binding["identity_sha256"]),
        baseline_config_path=Path(binding["config_path"]),
        baseline_config_sha256=str(pointer["live_config_sha256"]),
    )


def replay_identity_payload(
    schedule: OneSecondPredictionSchedule,
    arms: V9MLABArms,
    *,
    engine: str,
) -> dict[str, Any]:
    if engine not in {"python", "cpp"}:
        raise OneSecondReplayABIError("engine must be python or cpp")
    return {
        "schema_version": SCHEMA_VERSION,
        "utc_day": schedule.utc_day,
        "cadence_ms": schema.CADENCE_MS,
        "decision_clock": "canonical_utc_second",
        "sample_and_hold": "[decision_ts_ms,decision_ts_ms+1000ms)",
        "feature_ready_rule": "feature_ready_ts_ms<=decision_ts_ms",
        "all_13_heads_validated": True,
        "replay_projection_heads": list(REPLAY_HEADS),
        "overlay": {
            "manifest_path": str(schedule.manifest_path),
            "manifest_sha256": schedule.manifest_sha256,
            "overlay_path": str(schedule.overlay_path),
            "overlay_sha256": schedule.overlay_sha256,
            "overlay_identity_sha256": schedule.overlay_identity_sha256,
            "research_bundle_sha256": schedule.research_bundle_sha256,
        },
        "baseline": {
            "baseline_id": arms.baseline_id,
            "identity_path": str(arms.baseline_identity_path),
            "identity_sha256": arms.baseline_identity_sha256,
            "config_path": str(arms.baseline_config_path),
            "config_sha256": arms.baseline_config_sha256,
            "q90_action_enabled": False,
            "buy_fill_selection_enabled": False,
        },
        "arms": list(ARMS),
        "only_arm_difference": "causal_v12_1s_model_switch",
        "engine": engine,
        "historical_10s_loader_called": False,
        "economic_outcomes_read": False,
        "full_path_ml_ab_run": False,
        "prediction_authority": False,
        "action_authority": False,
        "live_authority": False,
    }


def load_model_free_tick_window(
    day: str,
    base_params: Mapping[str, Any],
    **window_options: Any,
) -> Any:
    """Load market state while making the historical ML loader unreachable."""

    forbidden = {"load_ml", "require_ml", "run_ml_inference"} & set(window_options)
    if forbidden:
        raise OneSecondReplayABIError(
            "successor window options cannot override model-free loading: "
            + ", ".join(sorted(forbidden))
        )
    from models.data_windows import load_tick_window

    return load_tick_window(
        day,
        dict(base_params),
        load_ml=False,
        require_ml=False,
        run_ml_inference=False,
        **window_options,
    )


def run_paired_tick_replay(
    *,
    window: Any,
    base_params: Mapping[str, Any],
    schedule: OneSecondPredictionSchedule,
    engine: str = "cpp",
    pointer_path: Path | None = None,
    simulate: Callable[..., Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run paired full paths using an already-loaded model-free market window.

    The caller must build ``window`` with ``load_tick_window(..., load_ml=False)``.
    This is the only execution entry point: it never invokes the old 10s loader.
    """

    if getattr(window, "ml_data", None) is not None:
        raise OneSecondReplayABIError(
            "successor runner requires a model-free window; old loader output was supplied"
        )
    if str(getattr(window, "book_source_authority", "")) != "native_formal_lifecycle":
        raise OneSecondReplayABIError("successor economic replay requires native lifecycle data")
    arms = bind_current_v9_ml_ab_arms(base_params, pointer_path=pointer_path)
    identity = replay_identity_payload(schedule, arms, engine=engine)
    if simulate is None:
        from models import backtest_tick as bt

        simulate = bt._simulate_tick_with_engine

    shared = {
        "bbo_data": window.bbo_data,
        "l2_data": window.l2_data,
        "var_ti": window.var_ti,
        "var_retsq": window.var_retsq,
    }
    results: dict[str, Mapping[str, Any]] = {}
    for arm in ARMS:
        params = dict(getattr(arms, arm))
        ml_data = None if arm == "ml_off" else schedule.ml_data
        results[arm] = simulate(
            engine,
            window.trades,
            window.var_ts_ms,
            window.var_ssq,
            params,
            ml_data=ml_data,
            **shared,
        )
    executed_identity = dict(identity)
    executed_identity["economic_outcomes_read"] = True
    executed_identity["full_path_ml_ab_run"] = True
    return {"identity": executed_identity, "arms": results}


def run_daily_paired_tick_replay(
    *,
    day: str,
    overlay_dir: Path,
    base_params: Mapping[str, Any],
    engine: str = "cpp",
    pointer_path: Path | None = None,
    window_options: Mapping[str, Any] | None = None,
    window_loader: Callable[..., Any] | None = None,
    simulate: Callable[..., Mapping[str, Any]] | None = None,
    allow_test_only: bool = False,
) -> dict[str, Any]:
    """Authoritative per-day successor runner with strict day ownership."""

    schedule = load_admitted_one_second_overlay(
        overlay_dir,
        allow_test_only=allow_test_only,
    )
    if schedule.utc_day != day:
        raise OneSecondReplayABIError(
            f"overlay UTC day {schedule.utc_day} differs from replay day {day}"
        )
    loader = load_model_free_tick_window if window_loader is None else window_loader
    options = dict(window_options or {})
    window = loader(day, base_params, **options)
    return run_paired_tick_replay(
        window=window,
        base_params=base_params,
        schedule=schedule,
        engine=engine,
        pointer_path=pointer_path,
        simulate=simulate,
    )
