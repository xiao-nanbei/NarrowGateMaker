#!/usr/bin/env python3
"""Freeze the outcome-blind F02 reach-time hazard training identity."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from features.feature_dag import P3_REACH_TIME_CONDITIONED_GRAPH
from research.families.f02_empirical_p3_touch.audit.p3_reach_time_conditioned_hazard import (
    FAST_NORMALIZED_DISTANCE_FEATURE,
    FAST_SIGMA_FEATURE,
    RAW_DISTANCE_FEATURE,
    SLOW_NORMALIZED_DISTANCE_FEATURE,
    SLOW_SIGMA_FEATURE,
    TIME_UPPER_FEATURE,
    canonical_sha256,
)
from research.families.f02_empirical_p3_touch.audit.p3_reach_time_source_manifest import (
    canonical_manifest_sha256,
    validate_source_day_manifest,
)

ROOT = Path(__file__).resolve().parents[1]
IDENTITY = "p3_aggressive_reach_time_conditioned_hazard_v1"
SPEC_SCHEMA = "narrowgate.p3_reach_time_conditioned_hazard_spec.v1"
SPEC_PATH = ROOT / (
    "research/families/f02_empirical_p3_touch/docs/"
    "p3_aggressive_reach_time_conditioned_hazard_v1_spec_20260804.json"
)
SPEC_MD_PATH = SPEC_PATH.with_suffix(".md")
SOURCE_MANIFEST_PATH = ROOT / (
    "research/families/f02_empirical_p3_touch/docs/"
    "p3_reach_time_source_day_manifest_v1_20260804.json"
)

CONTEXT_FEATURES = (
    FAST_SIGMA_FEATURE,
    SLOW_SIGMA_FEATURE,
    "spread_ticks",
    "spread_bps",
    "volatility_ratio",
    "book_age_ms",
)
STRUCTURAL_FEATURES = (
    RAW_DISTANCE_FEATURE,
    TIME_UPPER_FEATURE,
    FAST_NORMALIZED_DISTANCE_FEATURE,
    SLOW_NORMALIZED_DISTANCE_FEATURE,
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def chronological_oof_folds(fit_days: Sequence[str]) -> list[dict[str, Any]]:
    """Return the frozen four expanding folds without inspecting outcomes."""

    days = tuple(str(day) for day in fit_days)
    if len(days) != 156 or list(days) != sorted(days) or len(days) != len(set(days)):
        raise ValueError("formal F02 fit panel must be 156 unique chronological days")
    folds: list[dict[str, Any]] = []
    for fold_id, test_start in enumerate((72, 93, 114, 135), start=1):
        calibration_start = test_start - 12
        test_stop = test_start + 21
        folds.append(
            {
                "fold": fold_id,
                "train_days": list(days[:calibration_start]),
                "calibration_days": list(days[calibration_start:test_start]),
                "test_days": list(days[test_start:test_stop]),
                "train_count": calibration_start,
                "calibration_count": 12,
                "test_count": 21,
                "strict_chronology": (
                    days[calibration_start - 1]
                    < days[calibration_start]
                    < days[test_start]
                ),
            }
        )
    if any(not fold["strict_chronology"] for fold in folds):
        raise ValueError("formal F02 OOF folds are not strictly chronological")
    return folds


def _identity(path: Path) -> dict[str, Any]:
    target = path.resolve()
    if not target.is_file():
        raise FileNotFoundError(target)
    return {"path": str(target), "sha256": sha256_file(target)}


def _load_source_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    validate_source_day_manifest(payload)
    if payload["canonical_manifest_sha256"] != canonical_manifest_sha256(payload):
        raise ValueError("source manifest canonical identity drifted")
    return payload


def build_spec(source_manifest: Mapping[str, Any]) -> dict[str, Any]:
    panels = {row["name"]: row for row in source_manifest["panels"]}
    provider_fit = tuple(panels["fit_2025_provider"]["dates"])
    native_fit = tuple(panels["fit_2026_native"]["dates"])
    fit_days = (*provider_fit, *native_fit)
    diagnostics = (
        *panels["historical_2026_validation_diagnostic"]["dates"],
        *panels["historical_2026_test_diagnostic"]["dates"],
    )
    if len(provider_fit) != 93 or len(native_fit) != 63 or len(diagnostics) != 44:
        raise ValueError("source panel counts differ from the frozen F02 identity")

    code_paths = (
        ROOT / "features/feature_dag.py",
        ROOT
        / "research/families/f02_empirical_p3_touch/audit/p3_reach_time_context.py",
        ROOT
        / "research/families/f02_empirical_p3_touch/audit/p3_reach_time_cache.py",
        ROOT
        / "research/families/f02_empirical_p3_touch/audit/p3_reach_time_surface.py",
        ROOT
        / (
            "research/families/f02_empirical_p3_touch/audit/"
            "p3_reach_time_conditioned_hazard.py"
        ),
    )
    spec: dict[str, Any] = {
        "schema_version": SPEC_SCHEMA,
        "identity": IDENTITY,
        "last_materially_modified": "2026-08-04",
        "status": "training_spec_frozen_prediction_and_economics_unread",
        "classification": "prediction_only_no_action_or_live_authority",
        "source_manifest": {
            **_identity(SOURCE_MANIFEST_PATH),
            "canonical_sha256": source_manifest["canonical_manifest_sha256"],
            "weighted_day_count": len(source_manifest["weighted_day_records"]),
            "provider_fit_days": len(provider_fit),
            "native_fit_days": len(native_fit),
            "historical_diagnostic_days": len(diagnostics),
            "overlap_comparison_days": len(source_manifest["overlap_records"]),
        },
        "feature_dag": {
            "graph_id": P3_REACH_TIME_CONDITIONED_GRAPH.graph_id,
            "graph_sha256": P3_REACH_TIME_CONDITIONED_GRAPH.sha256(),
            "manifest": P3_REACH_TIME_CONDITIONED_GRAPH.manifest(),
        },
        "implementation_identities": [_identity(path) for path in code_paths],
        "estimand": {
            "name": "side_specific_aggressive_reach_first_passage_cdf",
            "formula": "F_reach,s(t,d|x)=P(T_aggressive_reach,s(d)<=t|x)",
            "time_step_ms": 100,
            "administrative_right_censor_ms": 30_000,
            "censor_is_market_constant": False,
            "distance_tick_size_usdc_per_btc": 0.1,
            "distance_support_ticks_inclusive": [5, 1200],
            "distance_support_usdc_per_btc_inclusive": [0.5, 120.0],
            "decision_origin": "canonical_10s_only",
            "canonical_origin_reused_at_100ms_live": False,
            "activation_queue_fill_lifecycle_or_value_estimand": False,
        },
        "feature_contract": {
            "structural_features": list(STRUCTURAL_FEATURES),
            "context_features": list(CONTEXT_FEATURES),
            "normalized_distance_formula": "d/(sigma_price*sqrt(time_upper_ms/1000))",
            "raw_and_normalized_distance_monotone_nonincreasing": True,
            "source_identity_or_year_tradable_feature": False,
            "label_feature_dependency": False,
        },
        "sampling_contract": {
            "origin_sampling": "sha256_rank_without_replacement_within_utc_source_day",
            "train_origins_per_day": 64,
            "calibration_origins_per_day": 64,
            "evaluation_origins_per_day": 128,
            "distance_population_ticks": [5, 1200],
            "distance_population_step_ticks": 1,
            "distance_queries_per_origin": 8,
            "distance_sampling": "hash_affine_systematic_without_replacement_v1",
            "distance_inclusion_weight": "Horvitz_Thompson",
            "sampling_seed": 20260804,
            "outcome_informed_sampling": False,
            "full_100ms_risk_intervals_retained_for_sampled_queries": True,
        },
        "chronological_oof": {
            "fit_days": list(fit_days),
            "fit_day_count": len(fit_days),
            "folds": chronological_oof_folds(fit_days),
            "final_train_days": list(fit_days[:-12]),
            "final_calibration_days": list(fit_days[-12:]),
            "historical_diagnostic_days": list(diagnostics),
            "historical_diagnostic_previously_read": True,
            "historical_diagnostic_independent_confirmation": False,
            "sealed_holdout_read": False,
        },
        "model_contract": {
            "side_specific_models": ["BUY", "SELL"],
            "objective": "binary_discrete_interval_hazard",
            "lightgbm_parameters": {
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
            },
            "num_boost_round": 180,
            "calibration": "positive_hazard_rate_power_on_prior_chronological_days",
            "cdf_integration": "1-product(1-hazard)",
        },
        "prediction_gates_frozen_before_fit": {
            "hard_context_coverage_min": 0.98,
            "owner_context_coverage_min": 0.95,
            "oof_fold_count_min": 4,
            "distance_monotonicity_violations_max": 0,
            "time_cdf_monotonicity_violations_max": 0,
            "probability_mass_error_max": 1e-10,
            "side_specific_integrated_brier_improvement_day_cluster_lcb_gt": 0.0,
            "side_specific_daily_brier_improvement_rate_min": 0.55,
            "source_overlap_prediction_mae_max": 0.01,
            "unsupported_prediction_policy": "fail_closed",
        },
        "governance": {
            "normal_hard_gate_path": "research_supported_prediction_evidence",
            "owner_coverage_override_path": "owner_risk_accepted_prediction_evidence",
            "owner_override_cannot_create_action_or_live_authority": True,
            "prediction_success_cannot_replace_operational_p3_v2": True,
            "quote_mapping_requires_separate_full_path_action_identity": True,
            "economic_outcomes_read": False,
            "action_authorized": False,
            "shadow_authorized": False,
            "live_authorized": False,
        },
    }
    spec["canonical_spec_sha256"] = canonical_sha256(spec)
    return spec


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _markdown(spec: Mapping[str, Any]) -> str:
    source = spec["source_manifest"]
    return f"""# P3 Aggressive-Reach Conditioned Hazard v1

Last materially modified: 2026-08-04

Status: training Spec frozen; prediction and economic results unread.

This identity estimates the side-specific 100ms first-passage hazard through a
30-second administrative right censor and integrates it into the complete
reach-time CDF. The censor is a reporting/support boundary, not an assumed
order lifetime.

The weighted source panel contains {source['weighted_day_count']} unique UTC
source-days: {source['provider_fit_days']} 2025 provider fit days,
{source['native_fit_days']} 2026 native fit days, and
{source['historical_diagnostic_days']} previously read 2026 diagnostic days.
The {source['overlap_comparison_days']} provider/native overlap days are
transport comparisons only and are never double weighted.

Four expanding chronological OOF folds are frozen before model fitting. Raw
distance and both volatility-normalized distances are constrained
nonincreasing. Source identity and calendar year are excluded from the
tradable feature vector.

Passing this Spec can establish prediction evidence only. It cannot replace
operational P3 v2, generate a quote, authorize an action, create a shadow, or
authorize live deployment. Any economic use requires an independently frozen
full-path action identity.

Canonical Spec SHA256: `{spec['canonical_spec_sha256']}`
"""


def main() -> int:
    source = _load_source_manifest(SOURCE_MANIFEST_PATH)
    spec = build_spec(source)
    _atomic_text(SPEC_PATH, json.dumps(spec, indent=2, sort_keys=True) + "\n")
    _atomic_text(SPEC_MD_PATH, _markdown(spec))
    print(SPEC_PATH)
    print(SPEC_MD_PATH)
    print(spec["canonical_spec_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
