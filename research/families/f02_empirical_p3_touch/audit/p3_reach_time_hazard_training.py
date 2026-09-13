"""Prediction-only training and evaluation for the F02 reach-time hazard.

This runner consumes the frozen, reusable context and first-passage label
caches.  It never reads queue state, order lifecycle, fills, PnL, rewards, or
live action state.  Every model is side-specific and every OOF prediction is
made by an expanding chronological fold fixed before any outcome is read.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, TextIO

import lightgbm as lgb
import numpy as np
import pandas as pd

from data_paths import resolve_portable_path
from features.feature_dag import P3_REACH_TIME_CONDITIONED_GRAPH
from research.families.f02_empirical_p3_touch.audit import (
    p3_reach_time_cache as label_node,
)
from research.families.f02_empirical_p3_touch.audit import (
    p3_reach_time_conditioned_hazard as hazard_node,
)
from research.families.f02_empirical_p3_touch.audit import (
    p3_reach_time_context as context_node,
)
from research.families.f02_empirical_p3_touch.audit import (
    p3_reach_time_source_manifest as source_manifest_node,
)
from research.governance.paths import resolve_research_path
from research.governance.public_machine_projection import (
    source_document_path,
    source_identity_sha256,
)
try:
    from scripts import build_p3_reach_time_caches as cache_builder
except ModuleNotFoundError as exc:
    if exc.name not in {"scripts", "scripts.build_p3_reach_time_caches"}:
        raise
    cache_builder = None

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "p3_aggressive_reach_time_conditioned_hazard_v1"
SPEC_SCHEMA_VERSION = "narrowgate.p3_reach_time_conditioned_hazard_spec.v1"
REPORT_SCHEMA_VERSION = "narrowgate.p3_reach_time_hazard_training_report.v1"
RUN_SCHEMA_VERSION = "narrowgate.p3_reach_time_hazard_training_run.v1"
FOLD_SCHEMA_VERSION = "narrowgate.p3_reach_time_hazard_fold_result.v1"

DEFAULT_SPEC_PATH = ROOT / (
    "research/families/f02_empirical_p3_touch/docs/"
    "p3_aggressive_reach_time_conditioned_hazard_v1_spec_20260804.json"
)
DEFAULT_SOURCE_MANIFEST_PATH = ROOT / (
    "research/families/f02_empirical_p3_touch/docs/"
    "p3_reach_time_source_day_manifest_v1_20260804.json"
)

EXPECTED_CONTEXT_FEATURES = (
    hazard_node.FAST_SIGMA_FEATURE,
    hazard_node.SLOW_SIGMA_FEATURE,
    "spread_ticks",
    "spread_bps",
    "volatility_ratio",
    "book_age_ms",
)
EXPECTED_CACHE_PARAMETERS: dict[str, int | float] = {
    "tick_size": 0.1,
    "cadence_ms": 10_000,
    "past_warmup_s": 60,
    "administrative_censor_ms": 30_000,
    "max_bbo_age_ms": 5_000,
    "fast_window_s": 10,
    "slow_window_s": 60,
    "variance_floor": 1e-6,
    "time_step_ms": 100,
    "max_distance_ticks": 1_200,
}
EXPECTED_LIGHTGBM_PARAMETERS: dict[str, int | float | str] = {
    "learning_rate": 0.03,
    "num_leaves": 31,
    "min_data_in_leaf": 500,
    "lambda_l2": 1.0,
    "max_bin": 127,
    "feature_fraction": 1.0,
    "bagging_fraction": 1.0,
    "num_threads": 8,
    "monotone_constraints_method": "advanced",
    "seed": 20260804,
}
CALIBRATION_HORIZONS_MS = (1_000, 5_000, 10_000, 30_000)
CALIBRATION_DISTANCE_BANDS = (
    (5, 20),
    (21, 100),
    (101, 400),
    (401, 1_200),
)
BOOTSTRAP_DRAWS = 20_000
DEFAULT_MAX_EXPANDED_ROWS = 25_000_000
_SHA256_LENGTH = 64


def sha256_file(path: Path) -> str:
    """Return a streaming SHA256 for one required artifact."""

    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_without(payload: Mapping[str, Any], field: str) -> str:
    value = dict(payload)
    value.pop(field, None)
    return hazard_node.canonical_sha256(value)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=target.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, target)


def _atomic_npz(path: Path, **arrays: np.ndarray) -> str:
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb", suffix=".npz", dir=target.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        np.savez_compressed(handle, **arrays)
    os.replace(temporary, target)
    return sha256_file(target)


def _emit(stream: TextIO | None, payload: Mapping[str, Any]) -> None:
    if stream is None:
        return
    stream.write(json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n")
    stream.flush()


def _require_file_identity(identity: Mapping[str, Any], *, label: str) -> Path:
    path = resolve_research_path(str(identity.get("path", "")))
    if not path.is_file():
        raise FileNotFoundError(f"{label} missing: {path}")
    expected = str(identity.get("sha256", ""))
    if len(expected) != _SHA256_LENGTH:
        raise ValueError(f"{label} has no valid SHA256")
    observed = source_identity_sha256(path)
    if observed != expected:
        raise ValueError(f"{label} hash mismatch: observed={observed} expected={expected}")
    return path


def _strict_day_sequence(values: Sequence[Any], *, label: str) -> tuple[str, ...]:
    days = tuple(str(value) for value in values)
    if not days or list(days) != sorted(days) or len(days) != len(set(days)):
        raise ValueError(f"{label} must contain unique chronological UTC dates")
    for day in days:
        if pd.Timestamp(day).date().isoformat() != day:
            raise ValueError(f"{label} contains a non-canonical date: {day!r}")
    return days


@dataclass(frozen=True)
class FoldContract:
    fold: int
    train_days: tuple[str, ...]
    calibration_days: tuple[str, ...]
    test_days: tuple[str, ...]


@dataclass(frozen=True)
class FrozenTrainingContract:
    spec_path: Path
    spec_file_sha256: str
    canonical_spec_sha256: str
    source_manifest_path: Path
    source_manifest_file_sha256: str
    source_manifest_canonical_sha256: str
    source_manifest: Mapping[str, Any]
    folds: tuple[FoldContract, ...]
    fit_days: tuple[str, ...]
    final_train_days: tuple[str, ...]
    final_calibration_days: tuple[str, ...]
    historical_diagnostic_days: tuple[str, ...]
    overlap_days: tuple[str, ...]
    context_feature_names: tuple[str, ...]
    train_origins_per_day: int
    calibration_origins_per_day: int
    evaluation_origins_per_day: int
    distance_ticks: tuple[int, ...]
    distance_queries_per_origin: int
    sampling_seed: int
    lightgbm_parameters: Mapping[str, Any]
    num_boost_round: int
    prediction_gates: Mapping[str, Any]
    implementation_identities: tuple[Mapping[str, Any], ...]


def _validate_spec_core(spec: Mapping[str, Any]) -> None:
    if spec.get("schema_version") != SPEC_SCHEMA_VERSION:
        raise ValueError("unsupported F02 reach-time hazard spec schema")
    if spec.get("identity") != IDENTITY:
        raise ValueError("F02 reach-time hazard spec identity mismatch")
    observed = str(spec.get("canonical_spec_sha256", ""))
    if _canonical_without(spec, "canonical_spec_sha256") != observed:
        raise ValueError("F02 reach-time hazard canonical spec hash mismatch")
    if spec.get("classification") != "prediction_only_no_action_or_live_authority":
        raise ValueError("F02 reach-time training must remain prediction-only")
    governance = spec.get("governance")
    if not isinstance(governance, Mapping):
        raise ValueError("F02 reach-time governance contract is missing")
    for field in (
        "economic_outcomes_read",
        "action_authorized",
        "shadow_authorized",
        "live_authorized",
    ):
        if governance.get(field) is not False:
            raise ValueError(f"F02 prediction-only governance requires {field}=false")

    estimand = spec.get("estimand")
    if not isinstance(estimand, Mapping):
        raise ValueError("F02 reach-time estimand is missing")
    required_estimand = {
        "time_step_ms": 100,
        "administrative_right_censor_ms": 30_000,
        "distance_tick_size_usdc_per_btc": 0.1,
        "distance_support_ticks_inclusive": [5, 1_200],
        "decision_origin": "canonical_10s_only",
        "activation_queue_fill_lifecycle_or_value_estimand": False,
    }
    for field, expected in required_estimand.items():
        if estimand.get(field) != expected:
            raise ValueError(f"F02 reach-time estimand drifted at {field}")

    feature_contract = spec.get("feature_contract")
    if not isinstance(feature_contract, Mapping):
        raise ValueError("F02 reach-time feature contract is missing")
    if tuple(feature_contract.get("structural_features", ())) != hazard_node.STRUCTURAL_FEATURES:
        raise ValueError("F02 reach-time structural feature schema drifted")
    if tuple(feature_contract.get("context_features", ())) != EXPECTED_CONTEXT_FEATURES:
        raise ValueError("F02 reach-time context feature schema drifted")
    if feature_contract.get("source_identity_or_year_tradable_feature") is not False:
        raise ValueError("source identity and year must not be tradable features")
    if feature_contract.get("label_feature_dependency") is not False:
        raise ValueError("label nodes must not feed the feature graph")

    model = spec.get("model_contract")
    if not isinstance(model, Mapping):
        raise ValueError("F02 reach-time model contract is missing")
    if tuple(model.get("side_specific_models", ())) != hazard_node.SIDES:
        raise ValueError("F02 reach-time side model contract drifted")
    if model.get("objective") != "binary_discrete_interval_hazard":
        raise ValueError("F02 reach-time objective drifted")
    if dict(model.get("lightgbm_parameters", {})) != EXPECTED_LIGHTGBM_PARAMETERS:
        raise ValueError("F02 reach-time LightGBM parameters drifted")
    if model.get("num_boost_round") != 180:
        raise ValueError("F02 reach-time boost-round contract drifted")
    if model.get("calibration") != "positive_hazard_rate_power_on_prior_chronological_days":
        raise ValueError("F02 reach-time calibration contract drifted")

    graph = spec.get("feature_dag")
    if not isinstance(graph, Mapping):
        raise ValueError("F02 reach-time Feature DAG binding is missing")
    if graph.get("graph_id") != P3_REACH_TIME_CONDITIONED_GRAPH.graph_id:
        raise ValueError("F02 reach-time Feature DAG ID mismatch")
    if graph.get("graph_sha256") != P3_REACH_TIME_CONDITIONED_GRAPH.sha256():
        raise ValueError("F02 reach-time Feature DAG hash mismatch")
    if hazard_node.canonical_sha256(graph.get("manifest")) != hazard_node.canonical_sha256(
        P3_REACH_TIME_CONDITIONED_GRAPH.manifest()
    ):
        raise ValueError("F02 reach-time Feature DAG manifest mismatch")


def _validate_folds(
    chronological: Mapping[str, Any], source_manifest: Mapping[str, Any]
) -> tuple[tuple[FoldContract, ...], tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    fit_days = _strict_day_sequence(chronological.get("fit_days", ()), label="fit_days")
    historical = _strict_day_sequence(
        chronological.get("historical_diagnostic_days", ()),
        label="historical_diagnostic_days",
    )
    final_train = _strict_day_sequence(
        chronological.get("final_train_days", ()), label="final_train_days"
    )
    final_calibration = _strict_day_sequence(
        chronological.get("final_calibration_days", ()),
        label="final_calibration_days",
    )
    if len(fit_days) != 156 or len(historical) != 44:
        raise ValueError("formal F02 panel must retain 156 fit and 44 diagnostic days")
    if final_train != fit_days[:-12] or final_calibration != fit_days[-12:]:
        raise ValueError("final F02 train/calibration split drifted")
    if chronological.get("historical_diagnostic_previously_read") is not True:
        raise ValueError("historical diagnostics must remain marked previously read")
    if chronological.get("historical_diagnostic_independent_confirmation") is not False:
        raise ValueError("historical diagnostics cannot become independent confirmation")
    if chronological.get("sealed_holdout_read") is not False:
        raise ValueError("sealed holdout must remain unread")

    folds_raw = chronological.get("folds")
    if not isinstance(folds_raw, list) or len(folds_raw) != 4:
        raise ValueError("formal F02 training requires exactly four OOF folds")
    expected_counts = ((60, 12, 21), (81, 12, 21), (102, 12, 21), (123, 12, 21))
    folds: list[FoldContract] = []
    seen_test: set[str] = set()
    for expected_fold, (raw, counts) in enumerate(zip(folds_raw, expected_counts, strict=True), 1):
        if int(raw.get("fold", -1)) != expected_fold or raw.get("strict_chronology") is not True:
            raise ValueError("F02 OOF fold identity or chronology flag drifted")
        train = _strict_day_sequence(raw.get("train_days", ()), label=f"fold {expected_fold} train")
        calibration = _strict_day_sequence(
            raw.get("calibration_days", ()), label=f"fold {expected_fold} calibration"
        )
        test = _strict_day_sequence(raw.get("test_days", ()), label=f"fold {expected_fold} test")
        if (len(train), len(calibration), len(test)) != counts:
            raise ValueError(f"F02 fold {expected_fold} day counts drifted")
        if not (max(train) < min(calibration) < max(calibration) < min(test)):
            raise ValueError(f"F02 fold {expected_fold} is not strictly chronological")
        if set(train) & set(calibration) or set(train) & set(test) or set(calibration) & set(test):
            raise ValueError(f"F02 fold {expected_fold} split overlaps")
        if seen_test & set(test):
            raise ValueError("F02 OOF test days repeat across folds")
        seen_test.update(test)
        folds.append(FoldContract(expected_fold, train, calibration, test))
    if any(
        day not in fit_days
        for fold in folds
        for day in (*fold.train_days, *fold.calibration_days, *fold.test_days)
    ):
        raise ValueError("F02 OOF fold uses a day outside the fit panel")
    if tuple(folds[-1].train_days) != fit_days[:123]:
        raise ValueError("F02 expanding-fold ownership drifted")

    panel_days = {
        str(row["name"]): tuple(str(day) for day in row["dates"])
        for row in source_manifest["panels"]
    }
    manifest_fit = (*panel_days["fit_2025_provider"], *panel_days["fit_2026_native"])
    manifest_historical = (
        *panel_days["historical_2026_validation_diagnostic"],
        *panel_days["historical_2026_test_diagnostic"],
    )
    if fit_days != manifest_fit or historical != manifest_historical:
        raise ValueError("F02 spec panels differ from the frozen source manifest")
    return tuple(folds), fit_days, final_train, final_calibration


def load_frozen_training_contract(
    spec_path: Path = DEFAULT_SPEC_PATH,
    *,
    source_manifest_path: Path | None = None,
) -> FrozenTrainingContract:
    """Load and strictly bind the frozen prediction-only training identity."""

    public_spec_path = resolve_research_path(spec_path)
    actual_spec_path = source_document_path(public_spec_path, require_private=False)
    spec = json.loads(actual_spec_path.read_text(encoding="utf-8"))
    _validate_spec_core(spec)

    implementation = spec.get("implementation_identities")
    if not isinstance(implementation, list) or len(implementation) != 5:
        raise ValueError("F02 implementation identity set drifted")
    for index, identity in enumerate(implementation):
        if not isinstance(identity, Mapping):
            raise ValueError("F02 implementation identity is malformed")
        _require_file_identity(identity, label=f"F02 implementation {index}")

    source_identity = spec.get("source_manifest")
    if not isinstance(source_identity, Mapping):
        raise ValueError("F02 source manifest identity is missing")
    frozen_source_path = resolve_research_path(str(source_identity.get("path", "")))
    actual_source_path = (
        resolve_research_path(source_manifest_path)
        if source_manifest_path is not None
        else frozen_source_path
    )
    if actual_source_path != frozen_source_path:
        raise ValueError("F02 source manifest path differs from the frozen spec")
    _require_file_identity(source_identity, label="F02 source manifest")
    actual_source_path = source_document_path(actual_source_path, require_private=False)
    source_manifest = json.loads(actual_source_path.read_text(encoding="utf-8"))
    source_manifest_node.validate_source_day_manifest(source_manifest)
    canonical_manifest = source_manifest_node.canonical_manifest_sha256(source_manifest)
    if canonical_manifest != source_manifest.get("canonical_manifest_sha256"):
        raise ValueError("F02 source manifest canonical hash mismatch")
    if canonical_manifest != source_identity.get("canonical_sha256"):
        raise ValueError("F02 source manifest canonical identity differs from spec")
    if len(source_manifest["weighted_day_records"]) != 200:
        raise ValueError("F02 source manifest must contain 200 weighted days")
    overlap_days = tuple(str(row["date"]) for row in source_manifest["overlap_records"])
    if len(overlap_days) != 48 or len(overlap_days) != len(set(overlap_days)):
        raise ValueError("F02 source manifest must contain 48 unique overlap days")

    chronological = spec.get("chronological_oof")
    if not isinstance(chronological, Mapping):
        raise ValueError("F02 chronological OOF contract is missing")
    folds, fit_days, final_train, final_calibration = _validate_folds(
        chronological, source_manifest
    )

    sampling = spec.get("sampling_contract")
    expected_sampling = {
        "origin_sampling": "sha256_rank_without_replacement_within_utc_source_day",
        "train_origins_per_day": 64,
        "calibration_origins_per_day": 64,
        "evaluation_origins_per_day": 128,
        "distance_population_ticks": [5, 1_200],
        "distance_population_step_ticks": 1,
        "distance_queries_per_origin": 8,
        "distance_sampling": "hash_affine_systematic_without_replacement_v1",
        "distance_inclusion_weight": "Horvitz_Thompson",
        "sampling_seed": 20260804,
        "outcome_informed_sampling": False,
        "full_100ms_risk_intervals_retained_for_sampled_queries": True,
    }
    if sampling != expected_sampling:
        raise ValueError("F02 outcome-blind sampling contract drifted")

    gates = spec.get("prediction_gates_frozen_before_fit")
    if not isinstance(gates, Mapping):
        raise ValueError("F02 prediction gates are missing")
    required_gate_fields = {
        "hard_context_coverage_min",
        "owner_context_coverage_min",
        "oof_fold_count_min",
        "distance_monotonicity_violations_max",
        "time_cdf_monotonicity_violations_max",
        "probability_mass_error_max",
        "side_specific_integrated_brier_improvement_day_cluster_lcb_gt",
        "side_specific_daily_brier_improvement_rate_min",
        "source_overlap_prediction_mae_max",
        "unsupported_prediction_policy",
    }
    if set(gates) != required_gate_fields:
        raise ValueError("F02 prediction gate schema drifted")

    return FrozenTrainingContract(
        spec_path=actual_spec_path,
        spec_file_sha256=sha256_file(actual_spec_path),
        canonical_spec_sha256=str(spec["canonical_spec_sha256"]),
        source_manifest_path=actual_source_path,
        source_manifest_file_sha256=sha256_file(actual_source_path),
        source_manifest_canonical_sha256=canonical_manifest,
        source_manifest=source_manifest,
        folds=folds,
        fit_days=fit_days,
        final_train_days=final_train,
        final_calibration_days=final_calibration,
        historical_diagnostic_days=tuple(chronological["historical_diagnostic_days"]),
        overlap_days=overlap_days,
        context_feature_names=EXPECTED_CONTEXT_FEATURES,
        train_origins_per_day=64,
        calibration_origins_per_day=64,
        evaluation_origins_per_day=128,
        distance_ticks=tuple(range(5, 1_201)),
        distance_queries_per_origin=8,
        sampling_seed=20260804,
        lightgbm_parameters=dict(EXPECTED_LIGHTGBM_PARAMETERS),
        num_boost_round=180,
        prediction_gates=dict(gates),
        implementation_identities=tuple(dict(row) for row in implementation),
    )


@dataclass(frozen=True)
class CacheEntry:
    day: str
    source: str
    panel: str | None
    weighted: bool
    source_record_sha256: str
    context_cache_key: str
    label_cache_key: str
    context_path: Path
    label_path: Path
    context_rows: int
    label_rows: int
    summary_sha256: str


@dataclass(frozen=True)
class CacheCatalog:
    entries: Mapping[tuple[str, str], CacheEntry]
    summary_identities: tuple[Mapping[str, Any], ...]

    def require(self, day: str, source: str) -> CacheEntry:
        key = (str(day), str(source))
        try:
            return self.entries[key]
        except KeyError as exc:
            raise KeyError(f"F02 cache entry missing for {key}") from exc


def _validate_cache_parameters(parameters: Mapping[str, Any]) -> None:
    if set(parameters) != set(EXPECTED_CACHE_PARAMETERS):
        raise ValueError("F02 cache parameter schema drifted")
    for field, expected in EXPECTED_CACHE_PARAMETERS.items():
        observed = parameters[field]
        if isinstance(expected, float):
            if not math.isclose(float(observed), expected, rel_tol=0.0, abs_tol=1e-15):
                raise ValueError(f"F02 cache parameter drifted at {field}")
        elif observed != expected:
            raise ValueError(f"F02 cache parameter drifted at {field}")


def _validate_cache_data_manifest(
    path: Path,
    *,
    expected_key: str,
    expected_schema: str,
    expected_rows: int,
    expected_identity: Mapping[str, Any],
) -> None:
    manifest_path = path.with_suffix(path.suffix + ".manifest.json")
    if not path.is_file() or not manifest_path.is_file():
        raise FileNotFoundError(f"incomplete F02 cache entry: {path.parent}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if _canonical_without(manifest, "canonical_manifest_sha256") != manifest.get(
        "canonical_manifest_sha256"
    ):
        raise ValueError(f"F02 cache canonical manifest hash mismatch: {path}")
    if manifest.get("schema_version") != expected_schema:
        raise ValueError(f"F02 cache schema mismatch: {path}")
    if manifest.get("cache_key") != expected_key:
        raise ValueError(f"F02 cache key mismatch: {path}")
    if int(manifest.get("rows", -1)) != expected_rows:
        raise ValueError(f"F02 cache row count mismatch: {path}")
    if manifest.get("identity") != dict(expected_identity):
        raise ValueError(f"F02 cache identity mismatch: {path}")
    if manifest.get("data_path") != str(path):
        raise ValueError(f"F02 cache data path mismatch: {path}")
    if sha256_file(path) != manifest.get("data_sha256"):
        raise ValueError(f"F02 cache payload hash mismatch: {path}")


def load_cache_catalog(
    summary_paths: Sequence[Path],
    *,
    contract: FrozenTrainingContract,
) -> CacheCatalog:
    """Validate cache summaries and every context/label manifest before fit."""

    if cache_builder is None:
        raise RuntimeError(
            "F02 cache training requires the source-checkout cache builder; "
            "the compact public wheel intentionally excludes scripts/"
        )
    if not summary_paths:
        raise ValueError("at least one F02 cache summary is required")
    entries: dict[tuple[str, str], CacheEntry] = {}
    summaries: list[Mapping[str, Any]] = []
    expected_cache_module_hashes = {
        "orchestrator": sha256_file(Path(cache_builder.__file__).resolve()),
        "context_node": sha256_file(Path(context_node.__file__).resolve()),
        "label_cache_node": sha256_file(Path(label_node.__file__).resolve()),
        "label_surface_node": sha256_file(
            ROOT / "research/families/f02_empirical_p3_touch/audit/p3_reach_time_surface.py"
        ),
        "source_manifest_node": sha256_file(Path(source_manifest_node.__file__).resolve()),
    }
    for raw_path in summary_paths:
        summary_path = resolve_portable_path(raw_path).resolve()
        if not summary_path.is_file():
            raise FileNotFoundError(f"F02 cache summary missing: {summary_path}")
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if summary.get("schema_version") != cache_builder.SCHEMA_VERSION:
            raise ValueError("unsupported F02 cache summary schema")
        if _canonical_without(summary, "canonical_summary_sha256") != summary.get(
            "canonical_summary_sha256"
        ):
            raise ValueError("F02 cache summary canonical hash mismatch")
        if summary.get("dry_run") is not False:
            raise ValueError("dry-run cache summaries cannot feed F02 training")
        for flag in ("economic_outcomes_read", "queue_inputs_read", "order_lifecycle_inputs_read"):
            if summary.get(flag) is not False:
                raise ValueError(f"F02 cache summary requires {flag}=false")
        source_identity = summary.get("source_manifest")
        expected_source = {
            "path": str(contract.source_manifest_path),
            "sha256": contract.source_manifest_file_sha256,
            "canonical_manifest_sha256": contract.source_manifest_canonical_sha256,
        }
        if source_identity != expected_source:
            raise ValueError("F02 cache summary source-manifest identity mismatch")
        _validate_cache_parameters(summary.get("parameters", {}))
        if summary.get("module_sha256") != expected_cache_module_hashes:
            raise ValueError("F02 cache-builder module identity drifted")
        if int(summary.get("job_count", -1)) != len(summary.get("jobs", ())):
            raise ValueError("F02 cache summary job count mismatch")
        summary_digest = sha256_file(summary_path)
        summaries.append(
            {
                "path": str(summary_path),
                "sha256": summary_digest,
                "canonical_summary_sha256": summary["canonical_summary_sha256"],
            }
        )
        for row in summary["jobs"]:
            if row.get("context_status") == "planned" or row.get("label_status") == "planned":
                raise ValueError("planned F02 cache rows cannot feed training")
            context_rows = int(row.get("context_rows", -1))
            label_rows = int(row.get("label_rows", -1))
            if context_rows <= 0 or label_rows != context_rows:
                raise ValueError("F02 context and label row counts differ")
            entry = CacheEntry(
                day=str(row["day"]),
                source=str(row["source"]),
                panel=(str(row["panel"]) if row.get("panel") is not None else None),
                weighted=bool(row["weighted"]),
                source_record_sha256=str(row["source_record_sha256"]),
                context_cache_key=str(row["context_cache_key"]),
                label_cache_key=str(row["label_cache_key"]),
                context_path=resolve_portable_path(str(row["context_path"])).resolve(),
                label_path=resolve_portable_path(str(row["label_path"])).resolve(),
                context_rows=context_rows,
                label_rows=label_rows,
                summary_sha256=summary_digest,
            )
            key = (entry.day, entry.source)
            prior = entries.get(key)
            if prior is not None:
                same_cache = (
                    prior.context_cache_key == entry.context_cache_key
                    and prior.label_cache_key == entry.label_cache_key
                    and prior.context_path == entry.context_path
                    and prior.label_path == entry.label_path
                )
                if not same_cache:
                    raise ValueError(f"conflicting F02 cache summaries for {key}")
                if prior.weighted:
                    continue
            entries[key] = entry

    weighted_by_key = {
        (str(row["date"]), str(row["primary_source"])): row
        for row in contract.source_manifest["weighted_day_records"]
    }
    overlap_provider_by_key = {
        (str(row["date"]), "provider"): row for row in contract.source_manifest["overlap_records"]
    }
    allowed = set(weighted_by_key) | set(overlap_provider_by_key)
    if set(entries) - allowed:
        raise ValueError(
            f"F02 cache summaries contain unsupported rows: {sorted(set(entries) - allowed)}"
        )
    if set(weighted_by_key) - set(entries):
        raise ValueError("F02 weighted cache surface is incomplete")
    if set(overlap_provider_by_key) - set(entries):
        raise ValueError("F02 provider overlap cache surface is incomplete")

    source_records = {
        (str(row["date"]), str(row["source"])): row
        for row in (
            *contract.source_manifest["provider_records"],
            *contract.source_manifest["native_records"],
        )
    }
    for key, entry in sorted(entries.items()):
        source_record = source_records.get(key)
        if (
            source_record is None
            or source_record.get("record_sha256") != entry.source_record_sha256
        ):
            raise ValueError(f"F02 cache source ownership mismatch for {key}")
        weighted = weighted_by_key.get(key)
        if weighted is not None:
            if not entry.weighted or entry.panel != weighted["panel"]:
                raise ValueError(f"F02 weighted cache ownership mismatch for {key}")
        context_identity = {
            "day": entry.day,
            "source_profile": entry.source,
            "panel": entry.panel,
            "weighted": entry.weighted,
            "selection_mode": "weighted_primary" if entry.weighted else "overlap_source_comparison",
            "source_record_sha256": entry.source_record_sha256,
            "parameters": dict(EXPECTED_CACHE_PARAMETERS),
            "economic_outcomes_read": False,
            "queue_inputs_read": False,
            "order_lifecycle_inputs_read": False,
            "node": cache_builder.CONTEXT_CACHE_NODE,
            "bbo_sha256": source_record["files"]["bbo"]["sha256"],
            "context_code_identity_sha256": hazard_node.canonical_sha256(
                {
                    "schema_version": context_node.SCHEMA_VERSION,
                    "context_module_sha256": expected_cache_module_hashes["context_node"],
                    "source_record_sha256": entry.source_record_sha256,
                    "parameters": dict(EXPECTED_CACHE_PARAMETERS),
                }
            ),
        }
        context_manifest = json.loads(
            entry.context_path.with_suffix(entry.context_path.suffix + ".manifest.json").read_text(
                encoding="utf-8"
            )
        )
        observed_context_identity = context_manifest.get("identity", {})
        for field, value in context_identity.items():
            if observed_context_identity.get(field) != value:
                raise ValueError(f"F02 context identity drifted at {field} for {key}")
        if set(observed_context_identity) != set(context_identity):
            raise ValueError(f"F02 context identity schema mismatch for {key}")
        _validate_cache_data_manifest(
            entry.context_path,
            expected_key=entry.context_cache_key,
            expected_schema=context_node.SCHEMA_VERSION,
            expected_rows=entry.context_rows,
            expected_identity=observed_context_identity,
        )

        label_manifest = json.loads(
            entry.label_path.with_suffix(entry.label_path.suffix + ".manifest.json").read_text(
                encoding="utf-8"
            )
        )
        observed_label_identity = label_manifest.get("identity", {})
        expected_label_fields = {
            **{
                key: value
                for key, value in context_identity.items()
                if key not in {"node", "bbo_sha256", "context_code_identity_sha256"}
            },
            "node": cache_builder.LABEL_CACHE_NODE,
            "context_cache_key": entry.context_cache_key,
            "official_aggtrades_sha256": source_record["files"]["official_aggtrades"]["sha256"],
            "label_code_identity_sha256": hazard_node.canonical_sha256(
                {
                    "schema_version": label_node.SCHEMA_VERSION,
                    "label_cache_module_sha256": expected_cache_module_hashes["label_cache_node"],
                    "label_surface_module_sha256": expected_cache_module_hashes[
                        "label_surface_node"
                    ],
                    "source_record_sha256": entry.source_record_sha256,
                    "parameters": dict(EXPECTED_CACHE_PARAMETERS),
                }
            ),
        }
        for field, value in expected_label_fields.items():
            if observed_label_identity.get(field) != value:
                raise ValueError(f"F02 label identity drifted at {field} for {key}")
        if set(observed_label_identity) != set(expected_label_fields):
            raise ValueError(f"F02 label identity schema mismatch for {key}")
        _validate_cache_data_manifest(
            entry.label_path,
            expected_key=entry.label_cache_key,
            expected_schema=label_node.SCHEMA_VERSION,
            expected_rows=entry.label_rows,
            expected_identity=observed_label_identity,
        )
    return CacheCatalog(entries=dict(entries), summary_identities=tuple(summaries))


def sha256_rank_origin_indices(
    origin_ids: Sequence[int] | np.ndarray,
    *,
    count: int,
    seed: int,
    day: str,
    purpose: str,
) -> np.ndarray:
    """Choose origins by an outcome-blind SHA256 rank within one UTC day."""

    origins = np.asarray(origin_ids)
    if origins.ndim != 1 or not np.issubdtype(origins.dtype, np.integer):
        raise TypeError("origin IDs must be a one-dimensional integer vector")
    if len(origins) != len(np.unique(origins)) or len(origins) == 0:
        raise ValueError("origin IDs must be non-empty and unique")
    if isinstance(count, bool) or int(count) <= 0 or int(count) > len(origins):
        raise ValueError("origin sample count leaves available context support")
    if not str(purpose).strip():
        raise ValueError("origin sampling purpose must be non-empty")
    ranks = [
        (
            hashlib.sha256(f"{int(seed)}|{day}|{purpose}|{int(origin)}".encode("ascii")).digest(),
            int(index),
        )
        for index, origin in enumerate(origins)
    ]
    selected = sorted(index for _, index in sorted(ranks)[: int(count)])
    return np.asarray(selected, dtype=np.int64)


def expansion_upper_bound_rows(
    *,
    day_count: int,
    origins_per_day: int,
    distance_queries_per_origin: int,
    time_bins: int = 300,
) -> int:
    values = (day_count, origins_per_day, distance_queries_per_origin, time_bins)
    if any(isinstance(value, bool) or int(value) <= 0 for value in values):
        raise ValueError("risk-row expansion factors must be positive integers")
    return math.prod(int(value) for value in values)


def enforce_expansion_cap(estimated_rows: int, maximum_rows: int) -> None:
    if isinstance(maximum_rows, bool) or int(maximum_rows) <= 0:
        raise ValueError("maximum expanded rows must be positive")
    if int(estimated_rows) > int(maximum_rows):
        raise RuntimeError(
            "F02 risk-row expansion exceeds safety cap: "
            f"estimated={int(estimated_rows)} cap={int(maximum_rows)}"
        )


@dataclass(frozen=True)
class LoadedDay:
    entry: CacheEntry
    context: pd.DataFrame
    surface: Any


def _load_day(entry: CacheEntry) -> LoadedDay:
    context, context_manifest = context_node.load_context_cache(
        entry.context_path,
        expected_cache_key=entry.context_cache_key,
    )
    origins, surface, label_manifest = label_node.load_label_cache(
        entry.label_path,
        expected_cache_key=entry.label_cache_key,
    )
    expected_origins = context["origin_ts_ms"].to_numpy(dtype=np.int64)
    if origins.shape != expected_origins.shape or not np.array_equal(origins, expected_origins):
        raise ValueError(f"F02 context/label origin mismatch for {entry.day} {entry.source}")
    if (
        context_manifest.get("identity", {}).get("source_record_sha256")
        != entry.source_record_sha256
    ):
        raise ValueError("F02 context cache source identity drifted after catalog validation")
    if label_manifest.get("identity", {}).get("source_record_sha256") != entry.source_record_sha256:
        raise ValueError("F02 label cache source identity drifted after catalog validation")
    expected_time = hazard_node.DEFAULT_GRID_SPEC.time_upper_ms()
    if not np.array_equal(np.asarray(surface.time_upper_ms, dtype=np.int32), expected_time):
        raise ValueError("F02 label cache time grid differs from the frozen hazard grid")
    return LoadedDay(entry=entry, context=context, surface=surface)


def first_reach_upper_endpoints(
    cumulative_reach_ticks: np.ndarray,
    *,
    distance_ticks: Sequence[int] | np.ndarray,
    time_upper_ms: Sequence[int] | np.ndarray,
) -> np.ndarray:
    """Convert a cumulative aggressive-reach path into first-passage endpoints."""

    cumulative_raw = np.asarray(cumulative_reach_ticks)
    distances_raw = np.asarray(distance_ticks)
    times_raw = np.asarray(time_upper_ms)
    if cumulative_raw.ndim != 2 or not np.issubdtype(cumulative_raw.dtype, np.integer):
        raise TypeError("cumulative reach must be a two-dimensional integer matrix")
    if distances_raw.ndim != 1 or not np.issubdtype(distances_raw.dtype, np.integer):
        raise TypeError("distance support must be a one-dimensional integer vector")
    if times_raw.ndim != 1 or not np.issubdtype(times_raw.dtype, np.integer):
        raise TypeError("time grid must be a one-dimensional integer vector")
    cumulative = cumulative_raw.astype(np.int64, copy=False)
    distances = distances_raw.astype(np.int64, copy=False)
    times = times_raw.astype(np.int32, copy=False)
    if cumulative.shape[1] != len(times):
        raise ValueError("cumulative reach and time-grid widths differ")
    if np.any(np.diff(cumulative, axis=1) < 0):
        raise ValueError("cumulative reach decreases over time")
    if len(distances) == 0 or np.any(distances <= 0) or np.any(np.diff(distances) <= 0):
        raise ValueError("distance support must be positive and strictly increasing")
    if len(times) == 0 or np.any(times <= 0) or np.any(np.diff(times) <= 0):
        raise ValueError("time grid must be positive and strictly increasing")

    endpoints = np.full(
        (cumulative.shape[0], len(distances)),
        hazard_node.RIGHT_CENSORED_TIME_MS,
        dtype=np.int32,
    )
    for origin_index, path in enumerate(cumulative):
        first_indices = np.searchsorted(path, distances, side="left")
        reached = first_indices < len(times)
        endpoints[origin_index, reached] = times[first_indices[reached]]
    return endpoints


def _context_mapping(frame: pd.DataFrame, names: Sequence[str]) -> dict[str, np.ndarray]:
    if tuple(names) != EXPECTED_CONTEXT_FEATURES:
        raise ValueError("F02 context feature order differs from the frozen schema")
    return {name: frame[name].to_numpy(dtype=np.float64) for name in names}


def _selected_loaded_day(
    loaded: LoadedDay,
    *,
    origin_count: int,
    seed: int,
    purpose: str,
) -> tuple[pd.DataFrame, Any, np.ndarray]:
    indices = sha256_rank_origin_indices(
        loaded.context["origin_ts_ms"].to_numpy(dtype=np.int64),
        count=origin_count,
        seed=seed,
        day=loaded.entry.day,
        purpose=purpose,
    )
    selected_context = loaded.context.iloc[indices].reset_index(drop=True)
    return selected_context, loaded.surface, indices


def _side_cumulative(surface: Any, side: str, indices: np.ndarray) -> np.ndarray:
    normalized = str(side).upper()
    if normalized == "BUY":
        matrix = surface.buy_cumulative_reach_ticks
    elif normalized == "SELL":
        matrix = surface.sell_cumulative_reach_ticks
    else:
        raise ValueError(f"unsupported F02 side: {side!r}")
    return np.asarray(matrix, dtype=np.int16)[indices]


def _queries_for_day(
    context: pd.DataFrame,
    *,
    side: str,
    contract: FrozenTrainingContract,
) -> hazard_node.DistanceQuerySample:
    return hazard_node.sample_distance_queries(
        origin_ids=context["origin_ts_ms"].to_numpy(dtype=np.int64),
        distance_ticks=np.asarray(contract.distance_ticks, dtype=np.int64),
        samples_per_origin=contract.distance_queries_per_origin,
        side=side,
        seed=contract.sampling_seed,
    )


def _day_endpoints_and_queries(
    loaded: LoadedDay,
    *,
    side: str,
    origin_count: int,
    purpose: str,
    contract: FrozenTrainingContract,
) -> tuple[pd.DataFrame, np.ndarray, hazard_node.DistanceQuerySample, np.ndarray]:
    selected_context, surface, indices = _selected_loaded_day(
        loaded,
        origin_count=origin_count,
        seed=contract.sampling_seed,
        purpose=purpose,
    )
    cumulative = _side_cumulative(surface, side, indices)
    endpoints = first_reach_upper_endpoints(
        cumulative,
        distance_ticks=np.asarray(contract.distance_ticks, dtype=np.int64),
        time_upper_ms=np.asarray(surface.time_upper_ms, dtype=np.int32),
    )
    queries = _queries_for_day(selected_context, side=side, contract=contract)
    return selected_context, endpoints, queries, cumulative


def _row_count_from_queries(
    endpoints: np.ndarray,
    queries: hazard_node.DistanceQuerySample,
) -> int:
    selected = endpoints[queries.origin_index, queries.distance_index]
    bins = np.where(
        selected == hazard_node.RIGHT_CENSORED_TIME_MS,
        hazard_node.DEFAULT_GRID_SPEC.n_time_bins,
        selected // hazard_node.TIME_STEP_MS,
    )
    if np.any(bins <= 0) or np.any(bins > hazard_node.DEFAULT_GRID_SPEC.n_time_bins):
        raise ValueError("F02 selected first-passage endpoints leave the risk grid")
    return int(np.sum(bins, dtype=np.int64))


def _build_day_risk_rows(
    loaded: LoadedDay,
    *,
    side: str,
    origin_count: int,
    purpose: str,
    contract: FrozenTrainingContract,
) -> hazard_node.HazardRiskRows:
    context, endpoints, queries, _ = _day_endpoints_and_queries(
        loaded,
        side=side,
        origin_count=origin_count,
        purpose=purpose,
        contract=contract,
    )
    return hazard_node.build_hazard_risk_rows(
        first_reach_upper_ms=endpoints,
        queries=queries,
        context_features=_context_mapping(context, contract.context_feature_names),
        context_feature_names=contract.context_feature_names,
        tick_size=float(EXPECTED_CACHE_PARAMETERS["tick_size"]),
    )


def accumulate_empirical_cdf_counts(
    counts: np.ndarray,
    cumulative_reach_ticks: np.ndarray,
    *,
    distance_ticks: Sequence[int] | np.ndarray,
) -> None:
    """Accumulate exact train-only side x distance x time reach counts."""

    cumulative = np.asarray(cumulative_reach_ticks, dtype=np.int64)
    distances = np.asarray(distance_ticks, dtype=np.int64)
    if counts.shape != (len(distances), cumulative.shape[1]):
        raise ValueError("empirical baseline accumulator shape mismatch")
    maximum = int(distances[-1])
    for time_index in range(cumulative.shape[1]):
        values = np.clip(cumulative[:, time_index], 0, maximum)
        histogram = np.bincount(values, minlength=maximum + 1)
        tail = np.cumsum(histogram[::-1], dtype=np.int64)[::-1]
        counts[:, time_index] += tail[distances]


@dataclass
class RiskRowStore:
    directory: Path
    row_count: int
    feature_names: tuple[str, ...]
    matrix: np.memmap
    labels: np.memmap
    sample_weight: np.memmap
    sampling_identities: tuple[str, ...]
    empirical_cdf: np.ndarray | None
    empirical_origin_count: int

    def flush(self) -> None:
        self.matrix.flush()
        self.labels.flush()
        self.sample_weight.flush()


def _weighted_entry_by_day(
    contract: FrozenTrainingContract,
    catalog: CacheCatalog,
) -> dict[str, CacheEntry]:
    mapping: dict[str, CacheEntry] = {}
    for row in contract.source_manifest["weighted_day_records"]:
        day = str(row["date"])
        mapping[day] = catalog.require(day, str(row["primary_source"]))
    return mapping


def materialize_risk_row_store(
    *,
    days: Sequence[str],
    side: str,
    origin_count: int,
    purpose: str,
    contract: FrozenTrainingContract,
    catalog: CacheCatalog,
    scratch_root: Path,
    maximum_expanded_rows: int = DEFAULT_MAX_EXPANDED_ROWS,
    collect_empirical_baseline: bool = False,
) -> RiskRowStore:
    """Stream daily caches into compact memmaps for one side and split."""

    ordered_days = tuple(str(day) for day in days)
    if not ordered_days or len(ordered_days) != len(set(ordered_days)):
        raise ValueError("risk-row materialization requires unique non-empty days")
    upper = expansion_upper_bound_rows(
        day_count=len(ordered_days),
        origins_per_day=origin_count,
        distance_queries_per_origin=contract.distance_queries_per_origin,
        time_bins=hazard_node.DEFAULT_GRID_SPEC.n_time_bins,
    )
    enforce_expansion_cap(upper, maximum_expanded_rows)
    by_day = _weighted_entry_by_day(contract, catalog)
    missing = set(ordered_days) - set(by_day)
    if missing:
        raise ValueError(f"risk-row days are outside the weighted cache panel: {sorted(missing)}")

    actual_rows = 0
    empirical_counts = (
        np.zeros(
            (len(contract.distance_ticks), hazard_node.DEFAULT_GRID_SPEC.n_time_bins),
            dtype=np.int64,
        )
        if collect_empirical_baseline
        else None
    )
    empirical_origins = 0
    for day in ordered_days:
        loaded = _load_day(by_day[day])
        _, endpoints, queries, cumulative = _day_endpoints_and_queries(
            loaded,
            side=side,
            origin_count=origin_count,
            purpose=purpose,
            contract=contract,
        )
        actual_rows += _row_count_from_queries(endpoints, queries)
        if empirical_counts is not None:
            accumulate_empirical_cdf_counts(
                empirical_counts,
                cumulative,
                distance_ticks=contract.distance_ticks,
            )
            empirical_origins += len(cumulative)
    if actual_rows <= 0 or actual_rows > upper:
        raise RuntimeError("F02 actual risk-row expansion is invalid")

    root = Path(scratch_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix=f"p3-{purpose}-{side.lower()}-", dir=root))
    feature_names = (*hazard_node.STRUCTURAL_FEATURES, *contract.context_feature_names)
    matrix = np.memmap(
        directory / "matrix.f32",
        mode="w+",
        dtype=np.float32,
        shape=(actual_rows, len(feature_names)),
    )
    labels = np.memmap(directory / "labels.u8", mode="w+", dtype=np.uint8, shape=(actual_rows,))
    weights = np.memmap(
        directory / "weights.f64", mode="w+", dtype=np.float64, shape=(actual_rows,)
    )
    cursor = 0
    sampling_identities: list[str] = []
    try:
        for day in ordered_days:
            rows = _build_day_risk_rows(
                _load_day(by_day[day]),
                side=side,
                origin_count=origin_count,
                purpose=purpose,
                contract=contract,
            )
            if rows.feature_names != feature_names:
                raise ValueError("F02 daily risk-row feature schema drifted")
            stop = cursor + rows.row_count
            if stop > actual_rows:
                raise RuntimeError("F02 risk-row materialization exceeded its planned size")
            matrix[cursor:stop] = rows.matrix
            labels[cursor:stop] = rows.labels
            weights[cursor:stop] = rows.sample_weight
            sampling_identities.append(rows.sampling_identity_sha256)
            cursor = stop
        if cursor != actual_rows:
            raise RuntimeError("F02 risk-row materialization ended at the wrong row count")
        matrix.flush()
        labels.flush()
        weights.flush()
    except Exception:
        del matrix, labels, weights
        shutil.rmtree(directory, ignore_errors=True)
        raise

    empirical_cdf = None
    if empirical_counts is not None:
        if empirical_origins != len(ordered_days) * origin_count:
            raise RuntimeError("F02 empirical baseline origin count drifted")
        empirical_cdf = empirical_counts.astype(np.float64) / float(empirical_origins)
        if np.any(np.diff(empirical_cdf, axis=0) > 0.0):
            raise ArithmeticError("train-only empirical CDF increases with distance")
        if np.any(np.diff(empirical_cdf, axis=1) < 0.0):
            raise ArithmeticError("train-only empirical CDF decreases with time")
    return RiskRowStore(
        directory=directory,
        row_count=actual_rows,
        feature_names=feature_names,
        matrix=matrix,
        labels=labels,
        sample_weight=weights,
        sampling_identities=tuple(sampling_identities),
        empirical_cdf=empirical_cdf,
        empirical_origin_count=empirical_origins,
    )


def _fit_side_from_stores(
    train: RiskRowStore,
    calibration: RiskRowStore,
    *,
    side: str,
    contract: FrozenTrainingContract,
) -> hazard_node.SideHazardModel:
    if train.feature_names != calibration.feature_names:
        raise ValueError("F02 train/calibration memmap feature schemas differ")
    if len(np.unique(np.asarray(train.labels))) != 2:
        raise ValueError("F02 training rows require both hazard outcomes")
    if len(np.unique(np.asarray(calibration.labels))) != 2:
        raise ValueError("F02 calibration rows require both hazard outcomes")
    constraints = hazard_node.lightgbm_monotone_constraints(train.feature_names)
    parameters: dict[str, Any] = {
        "objective": "binary",
        "verbosity": -1,
        "deterministic": True,
        "force_col_wise": True,
        **dict(contract.lightgbm_parameters),
        "monotone_constraints": list(constraints),
    }
    dataset = lgb.Dataset(
        train.matrix,
        label=train.labels,
        weight=train.sample_weight,
        feature_name=list(train.feature_names),
        free_raw_data=True,
    )
    booster = lgb.train(parameters, dataset, num_boost_round=contract.num_boost_round)
    raw_calibration = np.asarray(booster.predict(calibration.matrix), dtype=np.float64)
    positive_calibration = hazard_node.fit_positive_rate_calibration(
        raw_calibration,
        np.asarray(calibration.labels, dtype=np.uint8),
        np.asarray(calibration.sample_weight, dtype=np.float64),
    )
    return hazard_node.SideHazardModel(
        side=side,
        booster=booster,
        feature_names=train.feature_names,
        calibration=positive_calibration,
    )


def _dispose_store(store: RiskRowStore) -> None:
    store.flush()
    for array in (store.matrix, store.labels, store.sample_weight):
        mmap = getattr(array, "_mmap", None)
        if mmap is not None:
            mmap.close()
    shutil.rmtree(store.directory, ignore_errors=True)


def _prediction_matrix(
    context: pd.DataFrame,
    queries: hazard_node.DistanceQuerySample,
    *,
    feature_names: Sequence[str],
    context_feature_names: Sequence[str],
    tick_size: float,
) -> np.ndarray:
    names = tuple(feature_names)
    expected = (*hazard_node.STRUCTURAL_FEATURES, *tuple(context_feature_names))
    if names != expected:
        raise ValueError("F02 prediction feature schema differs from the frozen contract")
    time_grid = hazard_node.DEFAULT_GRID_SPEC.time_upper_ms().astype(np.float64)
    distance = np.repeat(queries.distance_ticks.astype(np.float64) * tick_size, len(time_grid))
    times = np.tile(time_grid, queries.query_count)
    query_origins = np.repeat(queries.origin_index, len(time_grid))
    matrix = np.empty((len(distance), len(names)), dtype=np.float32)
    matrix[:, 0] = distance
    matrix[:, 1] = times
    fast = context[hazard_node.FAST_SIGMA_FEATURE].to_numpy(dtype=np.float64)[query_origins]
    slow = context[hazard_node.SLOW_SIGMA_FEATURE].to_numpy(dtype=np.float64)[query_origins]
    if np.any(fast <= 0.0) or np.any(slow <= 0.0):
        raise ValueError("F02 prediction context contains non-positive volatility")
    sqrt_time = np.sqrt(times / 1_000.0)
    matrix[:, 2] = distance / (fast * sqrt_time)
    matrix[:, 3] = distance / (slow * sqrt_time)
    for offset, name in enumerate(
        context_feature_names, start=len(hazard_node.STRUCTURAL_FEATURES)
    ):
        matrix[:, offset] = context[name].to_numpy(dtype=np.float64)[query_origins]
    if not np.all(np.isfinite(matrix)):
        raise ValueError("F02 prediction matrix contains non-finite values")
    return matrix


def _predict_query_distribution(
    model: hazard_node.SideHazardModel,
    context: pd.DataFrame,
    queries: hazard_node.DistanceQuerySample,
    *,
    context_feature_names: Sequence[str],
) -> hazard_node.FirstPassageDistribution:
    matrix = _prediction_matrix(
        context,
        queries,
        feature_names=model.feature_names,
        context_feature_names=context_feature_names,
        tick_size=float(EXPECTED_CACHE_PARAMETERS["tick_size"]),
    )
    hazards = model.predict_hazards(matrix).reshape(
        queries.query_count, hazard_node.DEFAULT_GRID_SPEC.n_time_bins
    )
    return hazard_node.hazards_to_first_passage(hazards)


def _observed_query_cdf(
    endpoints: np.ndarray,
    queries: hazard_node.DistanceQuerySample,
) -> tuple[np.ndarray, np.ndarray]:
    selected = endpoints[queries.origin_index, queries.distance_index].astype(np.int32)
    time_grid = hazard_node.DEFAULT_GRID_SPEC.time_upper_ms()
    observed = (
        (selected[:, None] != hazard_node.RIGHT_CENSORED_TIME_MS)
        & (time_grid[None, :] >= selected[:, None])
    ).astype(np.float64)
    return selected, observed


def _weighted_mean(values: np.ndarray, weights: np.ndarray) -> float:
    numeric = np.asarray(values, dtype=np.float64)
    weight = np.asarray(weights, dtype=np.float64)
    if numeric.ndim != 1 or weight.shape != numeric.shape:
        raise ValueError("weighted mean vectors must have equal one-dimensional shapes")
    if np.any(~np.isfinite(numeric)) or np.any(~np.isfinite(weight)) or np.any(weight <= 0.0):
        raise ValueError("weighted mean inputs must be finite with positive weights")
    return float(np.sum(numeric * weight) / np.sum(weight))


def _calibration_components(
    predicted_cdf: np.ndarray,
    observed_cdf: np.ndarray,
    distance_ticks: np.ndarray,
    weights: np.ndarray,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for horizon_ms in CALIBRATION_HORIZONS_MS:
        time_index = horizon_ms // hazard_node.TIME_STEP_MS - 1
        for lower, upper in CALIBRATION_DISTANCE_BANDS:
            mask = (distance_ticks >= lower) & (distance_ticks <= upper)
            if not np.any(mask):
                continue
            selected_weight = weights[mask]
            prediction = predicted_cdf[mask, time_index]
            observation = observed_cdf[mask, time_index]
            rows.append(
                {
                    "horizon_ms": horizon_ms,
                    "distance_ticks_inclusive": [lower, upper],
                    "query_count": int(np.count_nonzero(mask)),
                    "weight_sum": float(np.sum(selected_weight)),
                    "predicted_weighted_sum": float(np.sum(prediction * selected_weight)),
                    "observed_weighted_sum": float(np.sum(observation * selected_weight)),
                    "brier_weighted_sum": float(
                        np.sum(np.square(prediction - observation) * selected_weight)
                    ),
                }
            )
    return rows


def _merge_calibration_components(
    component_groups: Iterable[Sequence[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    totals: dict[tuple[int, int, int], dict[str, float | int]] = {}
    for components in component_groups:
        for row in components:
            lower, upper = (int(value) for value in row["distance_ticks_inclusive"])
            key = (int(row["horizon_ms"]), lower, upper)
            total = totals.setdefault(
                key,
                {
                    "query_count": 0,
                    "weight_sum": 0.0,
                    "predicted_weighted_sum": 0.0,
                    "observed_weighted_sum": 0.0,
                    "brier_weighted_sum": 0.0,
                },
            )
            for field in total:
                total[field] = total[field] + row[field]  # type: ignore[operator]
    output: list[dict[str, Any]] = []
    for (horizon, lower, upper), total in sorted(totals.items()):
        weight_sum = float(total["weight_sum"])
        if weight_sum <= 0.0:
            raise ArithmeticError("F02 calibration slice has non-positive total weight")
        predicted = float(total["predicted_weighted_sum"]) / weight_sum
        observed = float(total["observed_weighted_sum"]) / weight_sum
        output.append(
            {
                "horizon_ms": horizon,
                "distance_ticks_inclusive": [lower, upper],
                "query_count": int(total["query_count"]),
                "weight_sum": weight_sum,
                "predicted_probability": predicted,
                "observed_probability": observed,
                "prediction_minus_observation": predicted - observed,
                "brier": float(total["brier_weighted_sum"]) / weight_sum,
            }
        )
    return output


def _distribution_invariants(
    distribution: hazard_node.FirstPassageDistribution,
    queries: hazard_node.DistanceQuerySample,
) -> dict[str, int | float]:
    time_violations = int(np.count_nonzero(np.diff(distribution.cdf, axis=1) < -1e-12))
    distance_hazard_violations = 0
    distance_cdf_violations = 0
    for origin in range(queries.origin_count):
        indices = np.flatnonzero(queries.origin_index == origin)
        if len(indices) <= 1:
            continue
        order = np.argsort(queries.distance_index[indices], kind="stable")
        ordered = indices[order]
        distance_hazard_violations += int(
            np.count_nonzero(np.diff(distribution.hazards[ordered], axis=0) > 1e-12)
        )
        distance_cdf_violations += int(
            np.count_nonzero(np.diff(distribution.cdf[ordered], axis=0) > 1e-11)
        )
    return {
        "time_cdf_monotonicity_violations": time_violations,
        "distance_hazard_monotonicity_violations": distance_hazard_violations,
        "distance_cdf_monotonicity_violations": distance_cdf_violations,
        "maximum_probability_mass_error": distribution.max_terminal_mass_error,
    }


def evaluate_loaded_day(
    loaded: LoadedDay,
    *,
    model: hazard_node.SideHazardModel,
    empirical_cdf: np.ndarray,
    side: str,
    contract: FrozenTrainingContract,
    purpose: str = "evaluation",
) -> dict[str, Any]:
    context, endpoints, queries, _ = _day_endpoints_and_queries(
        loaded,
        side=side,
        origin_count=contract.evaluation_origins_per_day,
        purpose=purpose,
        contract=contract,
    )
    if empirical_cdf.shape != (
        len(contract.distance_ticks),
        hazard_node.DEFAULT_GRID_SPEC.n_time_bins,
    ):
        raise ValueError("F02 empirical baseline surface has the wrong shape")
    distribution = _predict_query_distribution(
        model,
        context,
        queries,
        context_feature_names=contract.context_feature_names,
    )
    _, observed = _observed_query_cdf(endpoints, queries)
    baseline = empirical_cdf[queries.distance_index]
    weights = queries.inverse_probability_weight
    model_query_loss = np.mean(np.square(distribution.cdf - observed), axis=1)
    baseline_query_loss = np.mean(np.square(baseline - observed), axis=1)
    model_brier = _weighted_mean(model_query_loss, weights)
    baseline_brier = _weighted_mean(baseline_query_loss, weights)
    invariants = _distribution_invariants(distribution, queries)
    return {
        "day": loaded.entry.day,
        "source": loaded.entry.source,
        "panel": loaded.entry.panel,
        "side": str(side).upper(),
        "origin_count": len(context),
        "query_count": queries.query_count,
        "sampling_identity_sha256": queries.sampling_identity_sha256,
        "model_integrated_brier": model_brier,
        "train_empirical_integrated_brier": baseline_brier,
        "integrated_brier_improvement": baseline_brier - model_brier,
        "calibration_components": _calibration_components(
            distribution.cdf,
            observed,
            queries.distance_ticks,
            weights,
        ),
        "invariants": invariants,
    }


def _evaluate_days(
    *,
    days: Sequence[str],
    model: hazard_node.SideHazardModel,
    empirical_cdf: np.ndarray,
    side: str,
    contract: FrozenTrainingContract,
    catalog: CacheCatalog,
    purpose: str,
) -> list[dict[str, Any]]:
    by_day = _weighted_entry_by_day(contract, catalog)
    return [
        evaluate_loaded_day(
            _load_day(by_day[str(day)]),
            model=model,
            empirical_cdf=empirical_cdf,
            side=side,
            contract=contract,
            purpose=purpose,
        )
        for day in days
    ]


def paired_day_bootstrap(
    improvements: Sequence[float] | np.ndarray,
    *,
    seed: int,
    draws: int = BOOTSTRAP_DRAWS,
) -> dict[str, Any]:
    values = np.asarray(improvements, dtype=np.float64)
    if values.ndim != 1 or len(values) < 2 or np.any(~np.isfinite(values)):
        raise ValueError("paired day bootstrap requires at least two finite day values")
    if isinstance(draws, bool) or int(draws) <= 0:
        raise ValueError("paired day bootstrap draws must be positive")
    rng = np.random.default_rng(int(seed))
    means = np.empty(int(draws), dtype=np.float64)
    chunk = 2_000
    for start in range(0, int(draws), chunk):
        stop = min(int(draws), start + chunk)
        indices = rng.integers(0, len(values), size=(stop - start, len(values)))
        means[start:stop] = np.mean(values[indices], axis=1)
    return {
        "day_count": len(values),
        "mean_improvement": float(np.mean(values)),
        "median_improvement": float(np.median(values)),
        "daily_positive_rate": float(np.mean(values > 0.0)),
        "paired_day_cluster_bootstrap_draws": int(draws),
        "ci95": [float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))],
    }


def _summarize_daily_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    seed: int,
) -> dict[str, Any]:
    if not rows:
        raise ValueError("F02 daily evaluation summary is empty")
    improvements = np.asarray(
        [float(row["integrated_brier_improvement"]) for row in rows], dtype=np.float64
    )
    invariant_totals: dict[str, int | float] = {
        "time_cdf_monotonicity_violations": 0,
        "distance_hazard_monotonicity_violations": 0,
        "distance_cdf_monotonicity_violations": 0,
        "maximum_probability_mass_error": 0.0,
    }
    for row in rows:
        invariant = row["invariants"]
        for field in (
            "time_cdf_monotonicity_violations",
            "distance_hazard_monotonicity_violations",
            "distance_cdf_monotonicity_violations",
        ):
            invariant_totals[field] = int(invariant_totals[field]) + int(invariant[field])
        invariant_totals["maximum_probability_mass_error"] = max(
            float(invariant_totals["maximum_probability_mass_error"]),
            float(invariant["maximum_probability_mass_error"]),
        )
    return {
        "proper_score": paired_day_bootstrap(improvements, seed=seed),
        "calibration_slices": _merge_calibration_components(
            row["calibration_components"] for row in rows
        ),
        "invariants": invariant_totals,
    }


def _artifact_binding_base(
    contract: FrozenTrainingContract,
    catalog: CacheCatalog,
) -> dict[str, Any]:
    return {
        "identity": IDENTITY,
        "spec": {
            "path": str(contract.spec_path),
            "sha256": contract.spec_file_sha256,
            "canonical_spec_sha256": contract.canonical_spec_sha256,
        },
        "source_manifest": {
            "path": str(contract.source_manifest_path),
            "sha256": contract.source_manifest_file_sha256,
            "canonical_manifest_sha256": contract.source_manifest_canonical_sha256,
        },
        "cache_summaries": list(catalog.summary_identities),
        "feature_dag": {
            "graph_id": P3_REACH_TIME_CONDITIONED_GRAPH.graph_id,
            "graph_sha256": P3_REACH_TIME_CONDITIONED_GRAPH.sha256(),
        },
        "frozen_implementation_identities": [
            dict(row) for row in contract.implementation_identities
        ],
        "training_runner": {
            "path": str(Path(__file__).resolve()),
            "sha256": sha256_file(Path(__file__).resolve()),
        },
        "economic_outcomes_read": False,
        "queue_inputs_read": False,
        "order_lifecycle_inputs_read": False,
        "action_authorized": False,
        "shadow_authorized": False,
        "live_authorized": False,
    }


def _fit_side_bundle(
    *,
    target: Path,
    side: str,
    train_days: Sequence[str],
    calibration_days: Sequence[str],
    contract: FrozenTrainingContract,
    catalog: CacheCatalog,
    scratch_root: Path,
    maximum_expanded_rows: int,
    split_identity: str,
) -> tuple[hazard_node.SideHazardModel, np.ndarray, dict[str, Any]]:
    target = Path(target).resolve()
    if target.exists():
        raise FileExistsError(f"F02 side artifact target already exists: {target}")
    target.mkdir(parents=True)
    train_store: RiskRowStore | None = None
    calibration_store: RiskRowStore | None = None
    try:
        train_store = materialize_risk_row_store(
            days=train_days,
            side=side,
            origin_count=contract.train_origins_per_day,
            purpose="train",
            contract=contract,
            catalog=catalog,
            scratch_root=scratch_root,
            maximum_expanded_rows=maximum_expanded_rows,
            collect_empirical_baseline=True,
        )
        calibration_store = materialize_risk_row_store(
            days=calibration_days,
            side=side,
            origin_count=contract.calibration_origins_per_day,
            purpose="calibration",
            contract=contract,
            catalog=catalog,
            scratch_root=scratch_root,
            maximum_expanded_rows=maximum_expanded_rows,
            collect_empirical_baseline=False,
        )
        model = _fit_side_from_stores(
            train_store,
            calibration_store,
            side=side,
            contract=contract,
        )
        if train_store.empirical_cdf is None:
            raise RuntimeError("F02 train-only empirical baseline was not materialized")
        empirical_cdf = np.asarray(train_store.empirical_cdf, dtype=np.float64).copy()
        model_hashes = hazard_node.save_side_hazard_artifact(model, target / "model")
        baseline_path = target / "train_empirical_side_distance_time_cdf.npz"
        baseline_sha256 = _atomic_npz(
            baseline_path,
            distance_ticks=np.asarray(contract.distance_ticks, dtype=np.int16),
            time_upper_ms=hazard_node.DEFAULT_GRID_SPEC.time_upper_ms(),
            cdf=empirical_cdf,
        )
        binding = {
            "schema_version": "narrowgate.p3_reach_time_hazard_side_binding.v1",
            **_artifact_binding_base(contract, catalog),
            "split_identity": split_identity,
            "side": str(side).upper(),
            "train_days": list(train_days),
            "calibration_days": list(calibration_days),
            "training": {
                "train_risk_rows": train_store.row_count,
                "calibration_risk_rows": calibration_store.row_count,
                "train_origins": train_store.empirical_origin_count,
                "train_sampling_identities_sha256": hazard_node.canonical_sha256(
                    train_store.sampling_identities
                ),
                "calibration_sampling_identities_sha256": hazard_node.canonical_sha256(
                    calibration_store.sampling_identities
                ),
                "feature_names": list(train_store.feature_names),
                "lightgbm_parameters": dict(contract.lightgbm_parameters),
                "num_boost_round": contract.num_boost_round,
            },
            "model_artifact": {
                "relative_path": "model",
                **asdict(model_hashes),
            },
            "train_empirical_baseline": {
                "relative_path": baseline_path.name,
                "sha256": baseline_sha256,
                "estimand": "train_only_side_x_distance_x_time_empirical_cdf",
            },
        }
        binding["canonical_binding_sha256"] = hazard_node.canonical_sha256(binding)
        _atomic_json(target / "binding.json", binding)
        return model, empirical_cdf, binding
    except Exception:
        shutil.rmtree(target, ignore_errors=True)
        raise
    finally:
        if train_store is not None:
            _dispose_store(train_store)
        if calibration_store is not None:
            _dispose_store(calibration_store)


def context_coverage_report(
    contract: FrozenTrainingContract,
    catalog: CacheCatalog,
) -> dict[str, Any]:
    expected_per_day = len(
        context_node.canonical_origins_ms(
            "2026-01-01",
            cadence_ms=int(EXPECTED_CACHE_PARAMETERS["cadence_ms"]),
            past_warmup_s=int(EXPECTED_CACHE_PARAMETERS["past_warmup_s"]),
            administrative_censor_ms=int(EXPECTED_CACHE_PARAMETERS["administrative_censor_ms"]),
        )
    )
    groups: dict[tuple[str, str], list[tuple[int, int]]] = defaultdict(list)
    daily: list[dict[str, Any]] = []
    for row in contract.source_manifest["weighted_day_records"]:
        entry = catalog.require(str(row["date"]), str(row["primary_source"]))
        if entry.context_rows > expected_per_day:
            raise ValueError("F02 context cache has more than the canonical daily origin count")
        coverage = entry.context_rows / float(expected_per_day)
        daily.append(
            {
                "day": entry.day,
                "source": entry.source,
                "panel": entry.panel,
                "available_origins": entry.context_rows,
                "canonical_origins": expected_per_day,
                "coverage": coverage,
            }
        )
        groups[(entry.source, str(entry.panel))].append((entry.context_rows, expected_per_day))
    available = sum(int(row["available_origins"]) for row in daily)
    expected = sum(int(row["canonical_origins"]) for row in daily)
    by_source_panel = [
        {
            "source": source,
            "panel": panel,
            "available_origins": sum(value[0] for value in values),
            "canonical_origins": sum(value[1] for value in values),
            "coverage": sum(value[0] for value in values)
            / float(sum(value[1] for value in values)),
        }
        for (source, panel), values in sorted(groups.items())
    ]
    return {
        "pooled_available_origins": available,
        "pooled_canonical_origins": expected,
        "pooled_coverage": available / float(expected),
        "minimum_daily_coverage": min(float(row["coverage"]) for row in daily),
        "by_source_panel": by_source_panel,
        "daily": daily,
    }


def _common_context_pair(
    provider: CacheEntry,
    native: CacheEntry,
    *,
    contract: FrozenTrainingContract,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    provider_frame, _ = context_node.load_context_cache(
        provider.context_path, expected_cache_key=provider.context_cache_key
    )
    native_frame, _ = context_node.load_context_cache(
        native.context_path, expected_cache_key=native.context_cache_key
    )
    common = np.intersect1d(
        provider_frame["origin_ts_ms"].to_numpy(dtype=np.int64),
        native_frame["origin_ts_ms"].to_numpy(dtype=np.int64),
        assume_unique=True,
    )
    indices = sha256_rank_origin_indices(
        common,
        count=contract.evaluation_origins_per_day,
        seed=contract.sampling_seed,
        day=provider.day,
        purpose="source_overlap",
    )
    selected_origins = common[indices]
    provider_indexed = provider_frame.set_index("origin_ts_ms", drop=False)
    native_indexed = native_frame.set_index("origin_ts_ms", drop=False)
    provider_selected = provider_indexed.loc[selected_origins].reset_index(drop=True)
    native_selected = native_indexed.loc[selected_origins].reset_index(drop=True)
    if not np.array_equal(
        provider_selected["origin_ts_ms"].to_numpy(dtype=np.int64),
        native_selected["origin_ts_ms"].to_numpy(dtype=np.int64),
    ):
        raise RuntimeError("F02 overlap source contexts are not paired by canonical origin")
    return provider_selected, native_selected


def evaluate_source_overlap(
    *,
    models: Mapping[str, hazard_node.SideHazardModel],
    contract: FrozenTrainingContract,
    catalog: CacheCatalog,
) -> dict[str, Any]:
    daily: list[dict[str, Any]] = []
    for day in contract.overlap_days:
        provider = catalog.require(day, "provider")
        native = catalog.require(day, "native")
        provider_context, native_context = _common_context_pair(provider, native, contract=contract)
        for side in hazard_node.SIDES:
            queries = _queries_for_day(provider_context, side=side, contract=contract)
            provider_distribution = _predict_query_distribution(
                models[side],
                provider_context,
                queries,
                context_feature_names=contract.context_feature_names,
            )
            native_distribution = _predict_query_distribution(
                models[side],
                native_context,
                queries,
                context_feature_names=contract.context_feature_names,
            )
            difference = np.abs(provider_distribution.cdf - native_distribution.cdf)
            daily.append(
                {
                    "day": day,
                    "side": side,
                    "origin_count": len(provider_context),
                    "query_count": queries.query_count,
                    "prediction_mae": float(np.mean(difference)),
                    "prediction_max_abs": float(np.max(difference)),
                    "sampling_identity_sha256": queries.sampling_identity_sha256,
                }
            )
    by_side: dict[str, Any] = {}
    for side in hazard_node.SIDES:
        values = [float(row["prediction_mae"]) for row in daily if row["side"] == side]
        if len(values) != len(contract.overlap_days):
            raise RuntimeError("F02 source-overlap result lost a side/day cell")
        by_side[side] = {
            "day_count": len(values),
            "mean_absolute_error": float(np.mean(values)),
            "maximum_daily_mean_absolute_error": float(np.max(values)),
        }
    return {
        "comparison": "provider_context_prediction_vs_native_context_prediction",
        "labels_read": False,
        "day_count": len(contract.overlap_days),
        "by_side": by_side,
        "daily": daily,
    }


def _run_fold(
    *,
    fold: FoldContract,
    output_root: Path,
    contract: FrozenTrainingContract,
    catalog: CacheCatalog,
    scratch_root: Path,
    maximum_expanded_rows: int,
    progress_stream: TextIO | None,
) -> dict[str, Any]:
    final_path = output_root / f"fold_{fold.fold:02d}"
    if final_path.exists():
        raise FileExistsError(f"F02 fold output already exists: {final_path}")
    stage = Path(tempfile.mkdtemp(prefix=f".fold_{fold.fold:02d}-", dir=output_root))
    try:
        side_results: dict[str, Any] = {}
        for side_index, side in enumerate(hazard_node.SIDES):
            _emit(
                progress_stream,
                {"event": "fold_side_start", "fold": fold.fold, "side": side},
            )
            model, empirical_cdf, binding = _fit_side_bundle(
                target=stage / side,
                side=side,
                train_days=fold.train_days,
                calibration_days=fold.calibration_days,
                contract=contract,
                catalog=catalog,
                scratch_root=scratch_root,
                maximum_expanded_rows=maximum_expanded_rows,
                split_identity=f"oof_fold_{fold.fold:02d}",
            )
            daily = _evaluate_days(
                days=fold.test_days,
                model=model,
                empirical_cdf=empirical_cdf,
                side=side,
                contract=contract,
                catalog=catalog,
                purpose="evaluation",
            )
            side_results[side] = {
                "artifact_binding": binding,
                "daily": daily,
                "summary": _summarize_daily_rows(
                    daily,
                    seed=contract.sampling_seed + 100 * fold.fold + side_index,
                ),
            }
            _emit(
                progress_stream,
                {"event": "fold_side_complete", "fold": fold.fold, "side": side},
            )
        result = {
            "schema_version": FOLD_SCHEMA_VERSION,
            "identity": IDENTITY,
            "fold": fold.fold,
            "train_days": list(fold.train_days),
            "calibration_days": list(fold.calibration_days),
            "test_days": list(fold.test_days),
            "by_side": side_results,
            "economic_outcomes_read": False,
            "action_authorized": False,
            "live_authorized": False,
        }
        result["canonical_fold_result_sha256"] = hazard_node.canonical_sha256(result)
        _atomic_json(stage / "fold_result.json", result)
        os.replace(stage, final_path)
        return result
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def _run_final_model(
    *,
    output_root: Path,
    contract: FrozenTrainingContract,
    catalog: CacheCatalog,
    scratch_root: Path,
    maximum_expanded_rows: int,
    progress_stream: TextIO | None,
) -> tuple[dict[str, hazard_node.SideHazardModel], dict[str, Any]]:
    final_path = output_root / "final_model"
    if final_path.exists():
        raise FileExistsError(f"F02 final-model output already exists: {final_path}")
    stage = Path(tempfile.mkdtemp(prefix=".final_model-", dir=output_root))
    try:
        models: dict[str, hazard_node.SideHazardModel] = {}
        side_results: dict[str, Any] = {}
        for side_index, side in enumerate(hazard_node.SIDES):
            _emit(progress_stream, {"event": "final_side_start", "side": side})
            model, empirical_cdf, binding = _fit_side_bundle(
                target=stage / side,
                side=side,
                train_days=contract.final_train_days,
                calibration_days=contract.final_calibration_days,
                contract=contract,
                catalog=catalog,
                scratch_root=scratch_root,
                maximum_expanded_rows=maximum_expanded_rows,
                split_identity="final_fit_for_historical_transport_diagnostic",
            )
            models[side] = model
            historical_daily = _evaluate_days(
                days=contract.historical_diagnostic_days,
                model=model,
                empirical_cdf=empirical_cdf,
                side=side,
                contract=contract,
                catalog=catalog,
                purpose="historical_diagnostic",
            )
            side_results[side] = {
                "artifact_binding": binding,
                "historical_diagnostic_daily": historical_daily,
                "historical_diagnostic_summary": _summarize_daily_rows(
                    historical_daily,
                    seed=contract.sampling_seed + 10_000 + side_index,
                ),
            }
            _emit(progress_stream, {"event": "final_side_complete", "side": side})
        result = {
            "schema_version": "narrowgate.p3_reach_time_hazard_final_model_result.v1",
            "identity": IDENTITY,
            "train_days": list(contract.final_train_days),
            "calibration_days": list(contract.final_calibration_days),
            "historical_diagnostic_days": list(contract.historical_diagnostic_days),
            "historical_diagnostic_previously_read": True,
            "historical_diagnostic_independent_confirmation": False,
            "historical_diagnostic_used_for_gates": False,
            "by_side": side_results,
            "economic_outcomes_read": False,
            "action_authorized": False,
            "live_authorized": False,
        }
        result["canonical_final_model_result_sha256"] = hazard_node.canonical_sha256(result)
        _atomic_json(stage / "final_model_result.json", result)
        os.replace(stage, final_path)
        return models, result
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def _aggregate_oof(
    fold_results: Sequence[Mapping[str, Any]],
    *,
    contract: FrozenTrainingContract,
) -> dict[str, Any]:
    by_side: dict[str, Any] = {}
    for side_index, side in enumerate(hazard_node.SIDES):
        rows: list[Mapping[str, Any]] = []
        for fold in fold_results:
            rows.extend(fold["by_side"][side]["daily"])
        if len(rows) != 84 or len({str(row["day"]) for row in rows}) != 84:
            raise RuntimeError("F02 OOF aggregation must contain 84 unique test days per side")
        by_side[side] = {
            "daily": rows,
            "summary": _summarize_daily_rows(
                rows,
                seed=contract.sampling_seed + 20_000 + side_index,
            ),
        }
    return {"fold_count": len(fold_results), "by_side": by_side}


def _prediction_gate_report(
    *,
    oof: Mapping[str, Any],
    coverage: Mapping[str, Any],
    overlap: Mapping[str, Any],
    contract: FrozenTrainingContract,
) -> dict[str, Any]:
    gates = contract.prediction_gates
    fold_count_passed = int(oof["fold_count"]) >= int(gates["oof_fold_count_min"])
    side_score: dict[str, Any] = {}
    all_score_passed = True
    total_distance_violations = 0
    total_time_violations = 0
    maximum_mass_error = 0.0
    for side in hazard_node.SIDES:
        summary = oof["by_side"][side]["summary"]
        score = summary["proper_score"]
        invariant = summary["invariants"]
        lower_passed = float(score["ci95"][0]) > float(
            gates["side_specific_integrated_brier_improvement_day_cluster_lcb_gt"]
        )
        daily_rate_passed = float(score["daily_positive_rate"]) >= float(
            gates["side_specific_daily_brier_improvement_rate_min"]
        )
        side_score[side] = {
            "day_cluster_lcb_passed": lower_passed,
            "daily_improvement_rate_passed": daily_rate_passed,
            "passed": lower_passed and daily_rate_passed,
        }
        all_score_passed &= bool(side_score[side]["passed"])
        total_distance_violations += int(
            invariant["distance_hazard_monotonicity_violations"]
        ) + int(invariant["distance_cdf_monotonicity_violations"])
        total_time_violations += int(invariant["time_cdf_monotonicity_violations"])
        maximum_mass_error = max(
            maximum_mass_error, float(invariant["maximum_probability_mass_error"])
        )
    distance_passed = total_distance_violations <= int(
        gates["distance_monotonicity_violations_max"]
    )
    time_passed = total_time_violations <= int(gates["time_cdf_monotonicity_violations_max"])
    mass_passed = maximum_mass_error <= float(gates["probability_mass_error_max"])
    overlap_by_side = {
        side: float(overlap["by_side"][side]["mean_absolute_error"])
        <= float(gates["source_overlap_prediction_mae_max"])
        for side in hazard_node.SIDES
    }
    overlap_passed = all(overlap_by_side.values())
    coverage_value = float(coverage["pooled_coverage"])
    hard_coverage_passed = coverage_value >= float(gates["hard_context_coverage_min"])
    owner_coverage_passed = coverage_value >= float(gates["owner_context_coverage_min"])
    noncoverage_passed = all(
        (
            fold_count_passed,
            all_score_passed,
            distance_passed,
            time_passed,
            mass_passed,
            overlap_passed,
        )
    )
    normal_passed = noncoverage_passed and hard_coverage_passed
    owner_passed = noncoverage_passed and owner_coverage_passed
    if normal_passed:
        status = "research_supported_prediction_evidence"
    elif owner_passed:
        status = "owner_risk_accepted_prediction_evidence"
    else:
        status = "prediction_gates_failed"
    return {
        "status": status,
        "normal_hard_gate_path_passed": normal_passed,
        "owner_coverage_override_path_passed": owner_passed,
        "owner_override_cannot_create_action_or_live_authority": True,
        "fold_count": {
            "observed": int(oof["fold_count"]),
            "minimum": int(gates["oof_fold_count_min"]),
            "passed": fold_count_passed,
        },
        "context_coverage": {
            "observed": coverage_value,
            "hard_minimum": float(gates["hard_context_coverage_min"]),
            "owner_minimum": float(gates["owner_context_coverage_min"]),
            "hard_passed": hard_coverage_passed,
            "owner_passed": owner_coverage_passed,
        },
        "proper_score_by_side": side_score,
        "distance_monotonicity": {
            "violations": total_distance_violations,
            "maximum": int(gates["distance_monotonicity_violations_max"]),
            "passed": distance_passed,
        },
        "time_cdf_monotonicity": {
            "violations": total_time_violations,
            "maximum": int(gates["time_cdf_monotonicity_violations_max"]),
            "passed": time_passed,
        },
        "probability_mass": {
            "maximum_error": maximum_mass_error,
            "maximum": float(gates["probability_mass_error_max"]),
            "passed": mass_passed,
        },
        "source_overlap_prediction_mae": {
            "maximum": float(gates["source_overlap_prediction_mae_max"]),
            "by_side_passed": overlap_by_side,
            "passed": overlap_passed,
        },
        "economic_outcomes_read": False,
        "action_authorized": False,
        "shadow_authorized": False,
        "live_authorized": False,
    }


def run_hazard_training(
    *,
    cache_summary_paths: Sequence[Path],
    output_dir: Path,
    scratch_root: Path,
    spec_path: Path = DEFAULT_SPEC_PATH,
    source_manifest_path: Path | None = None,
    maximum_expanded_rows: int = DEFAULT_MAX_EXPANDED_ROWS,
    progress_stream: TextIO | None = None,
    invocation_identity: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the frozen four-fold OOF audit and final diagnostic model."""

    contract = load_frozen_training_contract(
        spec_path,
        source_manifest_path=source_manifest_path,
    )
    catalog = load_cache_catalog(cache_summary_paths, contract=contract)
    output_root = Path(output_dir).expanduser().resolve()
    if output_root.exists():
        raise FileExistsError(f"F02 training output already exists: {output_root}")
    output_root.parent.mkdir(parents=True, exist_ok=True)
    output_root.mkdir()
    scratch = Path(scratch_root).expanduser().resolve()
    scratch.mkdir(parents=True, exist_ok=True)
    coverage = context_coverage_report(contract, catalog)

    run_identity: dict[str, Any] = {
        "schema_version": RUN_SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "running_prediction_only",
        **_artifact_binding_base(contract, catalog),
        "invocation_identity": dict(invocation_identity or {}),
        "maximum_expanded_rows_per_side_split": int(maximum_expanded_rows),
        "bootstrap": {
            "method": "paired_utc_day_cluster_nonparametric",
            "draws": BOOTSTRAP_DRAWS,
            "seed_source": "frozen_sampling_seed_plus_fixed_side_fold_offset",
        },
        "calibration_slice_contract": {
            "horizons_ms": list(CALIBRATION_HORIZONS_MS),
            "distance_tick_bands_inclusive": [list(band) for band in CALIBRATION_DISTANCE_BANDS],
        },
    }
    run_identity["canonical_run_identity_sha256"] = hazard_node.canonical_sha256(run_identity)
    _atomic_json(output_root / "run_identity.json", run_identity)
    _emit(
        progress_stream,
        {
            "event": "training_preflight_complete",
            "output_dir": str(output_root),
            "cache_entries": len(catalog.entries),
        },
    )

    fold_results: list[dict[str, Any]] = []
    for fold in contract.folds:
        _emit(progress_stream, {"event": "fold_start", "fold": fold.fold})
        fold_results.append(
            _run_fold(
                fold=fold,
                output_root=output_root,
                contract=contract,
                catalog=catalog,
                scratch_root=scratch,
                maximum_expanded_rows=maximum_expanded_rows,
                progress_stream=progress_stream,
            )
        )
        _emit(progress_stream, {"event": "fold_complete", "fold": fold.fold})

    oof = _aggregate_oof(fold_results, contract=contract)
    models, final_result = _run_final_model(
        output_root=output_root,
        contract=contract,
        catalog=catalog,
        scratch_root=scratch,
        maximum_expanded_rows=maximum_expanded_rows,
        progress_stream=progress_stream,
    )
    overlap = evaluate_source_overlap(models=models, contract=contract, catalog=catalog)
    gate_report = _prediction_gate_report(
        oof=oof,
        coverage=coverage,
        overlap=overlap,
        contract=contract,
    )
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "preflight_run_identity_sha256": run_identity["canonical_run_identity_sha256"],
        "spec": run_identity["spec"],
        "source_manifest": run_identity["source_manifest"],
        "cache_summaries": run_identity["cache_summaries"],
        "context_coverage": coverage,
        "chronological_oof": oof,
        "source_overlap_transport": overlap,
        "historical_diagnostic": final_result,
        "prediction_gates": gate_report,
        "governance": {
            "prediction_only": True,
            "historical_44_days_previously_read_and_diagnostic_only": True,
            "historical_diagnostics_used_for_gates": False,
            "sealed_holdout_read": False,
            "economic_outcomes_read": False,
            "queue_inputs_read": False,
            "order_lifecycle_inputs_read": False,
            "action_authorized": False,
            "shadow_authorized": False,
            "live_authorized": False,
            "operational_p3_v2_replaced": False,
        },
    }
    report["canonical_report_sha256"] = hazard_node.canonical_sha256(report)
    _atomic_json(output_root / "report.json", report)
    completed_identity = dict(run_identity)
    completed_identity.pop("canonical_run_identity_sha256", None)
    completed_identity["status"] = "complete_prediction_only"
    completed_identity["report"] = {
        "relative_path": "report.json",
        "sha256": sha256_file(output_root / "report.json"),
        "canonical_report_sha256": report["canonical_report_sha256"],
    }
    completed_identity["canonical_run_identity_sha256"] = hazard_node.canonical_sha256(
        completed_identity
    )
    _atomic_json(output_root / "run_identity.json", completed_identity)
    _emit(
        progress_stream,
        {
            "event": "training_complete",
            "report": str(output_root / "report.json"),
            "prediction_status": gate_report["status"],
        },
    )
    return report
