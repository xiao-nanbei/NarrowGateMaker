#!/usr/bin/env python3
"""Frozen training mechanics for the causal-v12 canonical 1s successor.

This module does not train models or read prediction/economic outcomes.  It
binds the inherited 2025 train-only day split and defines head-specific
overlap weighting for labels evaluated every second.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np

from data_paths import resolve_portable_path

SCHEMA_VERSION = "causal_v12_1s_training_contract.v1"
IDENTITY = "causal_v12_cadence_1s_source_aware_semantics_successor_v1"
REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_DESIGN_PATH = (
    REPO_ROOT
    / "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_training_contract_v1_design_20260805.json"
)
NS_PER_SECOND = 1_000_000_000

HEAD_MAXIMUM_FUTURE_DEPENDENCY_S = {
    "dir_10s": 20,
    "ret_10s": 20,
    "vol_10s": 10,
    "dir_30s": 60,
    "ret_30s": 60,
    "vol_30s": 30,
    "dir_60s": 120,
    "ret_60s": 120,
    "vol_60s": 60,
    "tox_bid_5s": 10,
    "tox_ask_5s": 10,
    "tox_bid_10s": 20,
    "tox_ask_10s": 20,
}


class TrainingContractError(ValueError):
    """Raised when a proposed 1s training identity drifts from its contract."""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _resolve(path: str | Path, *, root: Path = REPO_ROOT) -> Path:
    candidate = resolve_portable_path(path, root=root)
    if not candidate.is_absolute():
        candidate = root / candidate
    return candidate.resolve(strict=True)


def _ordered_days(payload: Mapping[str, Any], key: str) -> tuple[str, ...]:
    values = tuple(str(value) for value in payload.get(key, ()))
    if not values or values != tuple(sorted(set(values))):
        raise TrainingContractError(f"{key} must be sorted, unique, and nonempty")
    for value in values:
        try:
            np.datetime64(value, "D")
        except ValueError as exc:
            raise TrainingContractError(f"invalid UTC day in {key}: {value}") from exc
    return values


def _validate_execution_binding(
    name: str,
    value: Any,
    *,
    repo_root: Path,
) -> None:
    if not isinstance(value, Mapping) or set(value) != {"path", "sha256"}:
        raise TrainingContractError(
            f"bound execution artifact {name} must contain only path and sha256"
        )
    expected_sha = str(value["sha256"])
    if len(expected_sha) != 64 or any(character not in "0123456789abcdef" for character in expected_sha):
        raise TrainingContractError(f"bound execution artifact {name} has invalid SHA256")
    path = _resolve(str(value["path"]), root=repo_root)
    if _sha256(path) != expected_sha:
        raise TrainingContractError(f"bound execution artifact {name} SHA256 mismatch")


def validate_training_design(
    design: Mapping[str, Any],
    *,
    repo_root: Path = REPO_ROOT,
) -> dict[str, Any]:
    """Validate the outcome-blind design and inherited train-only membership."""

    if design.get("schema_version") != SCHEMA_VERSION:
        raise TrainingContractError("unsupported 1s training contract schema")
    if design.get("identity") != IDENTITY:
        raise TrainingContractError("unexpected 1s training identity")
    if int(design.get("inference_cadence_ms", 0)) != 1_000:
        raise TrainingContractError("the successor cadence must be exactly 1 second")
    if design.get("candidate_cadence_set_ms") != [1_000]:
        raise TrainingContractError("this identity may freeze only the 1s cadence")
    if design.get("label_horizon_change_in_scope") is not False:
        raise TrainingContractError("label horizons must remain unchanged")

    boundaries = design.get("authority_boundaries")
    expected_boundaries = {
        "economic_outcomes_read": False,
        "prediction_outcomes_read": False,
        "model_training_executed": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    if boundaries != expected_boundaries:
        raise TrainingContractError("authority boundaries drifted")

    source = design.get("inherited_train_only_membership")
    if not isinstance(source, Mapping):
        raise TrainingContractError("inherited training membership is missing")
    source_path = _resolve(str(source.get("path", "")), root=repo_root)
    expected_sha = str(source.get("sha256", ""))
    if _sha256(source_path) != expected_sha:
        raise TrainingContractError("inherited training spec SHA256 mismatch")
    inherited = json.loads(source_path.read_text(encoding="utf-8"))
    if inherited.get("schema_version") != "narrowgate_13_head_train_only_selection.v1":
        raise TrainingContractError("inherited training spec schema mismatch")
    fit_days = _ordered_days(inherited, "fit_days")
    embargo_days = _ordered_days(inherited, "embargo_days")
    selection_days = _ordered_days(inherited, "selection_days")
    refit_days = _ordered_days(inherited, "refit_days")
    if (len(fit_days), len(embargo_days), len(selection_days), len(refit_days)) != (
        52,
        1,
        13,
        66,
    ):
        raise TrainingContractError("inherited 52/1/13/66 day counts drifted")
    groups = (set(fit_days), set(embargo_days), set(selection_days))
    if any(groups[i] & groups[j] for i in range(3) for j in range(i + 1, 3)):
        raise TrainingContractError("fit, embargo, and selection days overlap")
    if tuple(sorted(set().union(*groups))) != refit_days:
        raise TrainingContractError("refit days must equal fit + embargo + selection")
    if not max(fit_days) < min(embargo_days) <= max(embargo_days) < min(
        selection_days
    ):
        raise TrainingContractError("inherited train-only split is not chronological")
    if list(inherited.get("head_names", ())) != list(
        HEAD_MAXIMUM_FUTURE_DEPENDENCY_S
    ):
        raise TrainingContractError("inherited ordered 13-head identity drifted")

    dependencies = design.get("head_maximum_future_dependency_s")
    if dependencies != HEAD_MAXIMUM_FUTURE_DEPENDENCY_S:
        raise TrainingContractError("head future-dependency contract drifted")
    if design.get("base_sample_weight") != {
        "formula": "exp(-lambda * days_ago / 30.44)",
        "lambda": 0.1,
        "reference_date": "2026-07-23",
        "source": "causal_v12_feature_manifest_sha256_5409a398",
    }:
        raise TrainingContractError("inherited base sample-weight contract drifted")
    weighting = design.get("overlapping_label_weighting")
    expected_weighting = {
        "method": "average_reciprocal_concurrency_within_utc_day",
        "interval": "[decision_ts,decision_ts+maximum_future_dependency_s)",
        "base_weight_combination": "multiply_then_preserve_valid_day_weight_sum",
        "invalid_or_censored_label_weight": 0.0,
        "cadence_ns": NS_PER_SECOND,
        "normalization_scope": "utc_day_x_head",
    }
    if weighting != expected_weighting:
        raise TrainingContractError("overlapping-label weighting contract drifted")

    fold = design.get("chronological_training_fold")
    if not isinstance(fold, Mapping):
        raise TrainingContractError("chronological training fold is missing")
    if fold.get("fit_days") != list(fit_days):
        raise TrainingContractError("fit day membership drifted")
    if fold.get("embargo_days") != list(embargo_days):
        raise TrainingContractError("embargo day membership drifted")
    if fold.get("selection_days") != list(selection_days):
        raise TrainingContractError("selection day membership drifted")
    if fold.get("refit_days") != list(refit_days):
        raise TrainingContractError("refit day membership drifted")
    if fold.get("selection_method") != (
        "inner_chronological_early_stopping_then_full_refit"
    ):
        raise TrainingContractError("training selection method drifted")
    if fold.get("external_2026_panels_diagnostic_only") is not True:
        raise TrainingContractError("previously read 2026 panels must remain diagnostic")

    bindings = design.get("required_execution_artifacts")
    if not isinstance(bindings, Mapping):
        raise TrainingContractError("required execution artifacts are missing")
    missing = sorted(name for name, value in bindings.items() if value is None)
    bound = sorted(name for name, value in bindings.items() if value is not None)
    for name in bound:
        _validate_execution_binding(name, bindings[name], repo_root=repo_root)
    return {
        "schema_version": "causal_v12_1s_training_contract_audit.v1",
        "identity": IDENTITY,
        "fit_days": list(fit_days),
        "embargo_days": list(embargo_days),
        "selection_days": list(selection_days),
        "refit_days": list(refit_days),
        "head_count": len(HEAD_MAXIMUM_FUTURE_DEPENDENCY_S),
        "training_execution_eligible": not missing,
        "missing_execution_artifacts": missing,
        "bound_execution_artifacts": bound,
        "economic_outcomes_read": False,
        "prediction_outcomes_read": False,
    }


def load_and_validate_training_design(
    path: str | Path = DEFAULT_DESIGN_PATH,
) -> dict[str, Any]:
    design_path = _resolve(path)
    design = json.loads(design_path.read_text(encoding="utf-8"))
    return validate_training_design(design)


def overlap_adjusted_sample_weights(
    decision_ts_ns: Sequence[int] | np.ndarray,
    valid_label: Sequence[bool] | np.ndarray,
    *,
    maximum_future_dependency_s: int,
    base_weight: Sequence[float] | np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Return normalized weights and raw average uniqueness per UTC day.

    Each valid label owns a half-open future interval on the canonical 1s grid.
    Raw uniqueness is the interval-average reciprocal concurrency.  The
    adjustment is normalized within each UTC day/head so it does not silently
    change the inherited base-weight scale or LightGBM regularization scale.
    """

    timestamps = np.asarray(decision_ts_ns, dtype=np.int64)
    valid = np.asarray(valid_label, dtype=bool)
    if timestamps.ndim != 1 or valid.shape != timestamps.shape:
        raise TrainingContractError("timestamps and valid_label must be aligned 1D arrays")
    if timestamps.size and (
        np.any(timestamps <= 0)
        or np.any(timestamps % NS_PER_SECOND != 0)
        or np.any(np.diff(timestamps) <= 0)
    ):
        raise TrainingContractError(
            "decision timestamps must be positive, unique, increasing 1s boundaries"
        )
    horizon = int(maximum_future_dependency_s)
    if horizon <= 0:
        raise TrainingContractError("maximum future dependency must be positive")
    if base_weight is None:
        base = np.ones(timestamps.size, dtype=np.float64)
    else:
        base = np.asarray(base_weight, dtype=np.float64)
        if base.shape != timestamps.shape:
            raise TrainingContractError("base_weight must align with timestamps")
        if np.any(~np.isfinite(base)) or np.any(base < 0.0):
            raise TrainingContractError("base_weight must be finite and nonnegative")

    adjusted = np.zeros(timestamps.size, dtype=np.float64)
    uniqueness = np.zeros(timestamps.size, dtype=np.float64)
    if not timestamps.size:
        return adjusted, uniqueness

    utc_day = timestamps // (86_400 * NS_PER_SECOND)
    boundaries = np.flatnonzero(np.r_[True, utc_day[1:] != utc_day[:-1], True])
    for start, stop in zip(boundaries[:-1], boundaries[1:], strict=True):
        day_ts = timestamps[start:stop]
        if day_ts.size > 1 and np.any(np.diff(day_ts) != NS_PER_SECOND):
            raise TrainingContractError("each UTC-day panel must be a complete 1s grid")
        day_valid = valid[start:stop]
        valid_positions = np.flatnonzero(day_valid)
        if valid_positions.size and np.any(valid_positions + horizon > day_ts.size):
            raise TrainingContractError(
                "valid labels must be censored before their future interval leaves the UTC day"
            )
        concurrency_diff = np.zeros(day_ts.size + 1, dtype=np.int64)
        np.add.at(concurrency_diff, valid_positions, 1)
        np.add.at(concurrency_diff, valid_positions + horizon, -1)
        concurrency = np.cumsum(concurrency_diff[:-1])
        reciprocal = np.zeros(day_ts.size, dtype=np.float64)
        positive = concurrency > 0
        reciprocal[positive] = 1.0 / concurrency[positive]
        prefix = np.r_[0.0, np.cumsum(reciprocal)]
        raw = (
            prefix[valid_positions + horizon] - prefix[valid_positions]
        ) / horizon
        # Average reciprocal concurrency is mathematically in (0, 1].  Long
        # prefix sums can exceed 1 by a few ULPs after subtraction; canonicalize
        # only that numerical residue and keep material departures fail-closed.
        prefix_scale = max(1.0, float(np.max(np.abs(prefix))))
        tolerance = max(
            64.0 * np.finfo(np.float64).eps,
            4.0 * float(np.spacing(prefix_scale)) / horizon,
        )
        if np.any(raw <= 0.0) or np.any(raw > 1.0 + tolerance):
            raise TrainingContractError(
                "overlap uniqueness left its mathematical (0,1] support: "
                f"min={float(np.min(raw)):.17g}, max={float(np.max(raw)):.17g}, "
                f"tolerance={tolerance:.17g}"
            )
        raw = np.minimum(raw, 1.0)
        uniqueness[start + valid_positions] = raw
        base_valid = base[start + valid_positions]
        denominator = float(np.dot(base_valid, raw))
        numerator = float(np.sum(base_valid))
        if valid_positions.size and denominator <= 0.0:
            raise TrainingContractError("overlap uniqueness normalization is undefined")
        if denominator > 0.0:
            adjusted[start + valid_positions] = base_valid * raw * (
                numerator / denominator
            )
    return adjusted, uniqueness


def main() -> int:
    print(json.dumps(load_and_validate_training_design(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
