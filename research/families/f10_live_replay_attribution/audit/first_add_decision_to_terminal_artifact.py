#!/usr/bin/env python3
"""Build and audit the frozen F10 first-add Development evidence artifact.

The module is deliberately evidence-only. It can consume the native
``_first_add_decision_to_terminal_trace`` emitted by ``models.backtest_tick``
or call an injected per-day producer, but it cannot read a later panel, rank a
policy, register an action, or authorize live deployment.
"""

from __future__ import annotations

import argparse
import importlib
import inspect
import json
import math
import shutil
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.families.f10_live_replay_attribution.audit import (
    first_add_decision_to_terminal_contract as contract,
)

ARTIFACT_SCHEMA_VERSION = "first_add_decision_to_terminal_evidence.v1"
MANIFEST_SCHEMA_VERSION = "first_add_decision_to_terminal_manifest.v1"
TRACE_RESULT_KEY = "_first_add_decision_to_terminal_trace"
TRACE_AUDIT_RESULT_KEY = "_first_add_decision_to_terminal_trace_audit"
PRIMARY_PANEL = "development_primary_grade_a"
SENSITIVITY_PANEL = "development_sensitivity_grade_b"
DEFAULT_BOOTSTRAP_DRAWS = 2_000
DEFAULT_BOOTSTRAP_SEED = 20_260_729
DEFAULT_SPEC_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "first_add_decision_to_terminal_loss_diagnostic_v1_spec_20260729.json"
)

RunDay = Callable[..., Mapping[str, Any]]
QualityValidator = Callable[[Mapping[str, Any]], pd.DataFrame]

EVIDENCE_PERMISSIONS = {
    "validation_read": False,
    "sealed_holdout_read": False,
    "ranking_or_selection_authorized": False,
    "action_experiment_authorized": False,
    "live_deployment_authorized": False,
}

AUDIT_COUNT_COLUMNS = (
    "selected_campaign_count",
    "emitted_row_count",
    "unique_campaign_count",
    "exact_join_count",
)
AUDIT_ZERO_COLUMNS = (
    "feature_clock_violation_count",
    "open_record_count",
)


@dataclass(frozen=True)
class NativeTraceBuild:
    """Native trace plus the producer denominator and byte identity."""

    trace: pd.DataFrame
    producer_audit: pd.DataFrame
    producer_identity: dict[str, Any]


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("first-add spec must be a JSON object")
    return payload


def _panel_days(spec: Mapping[str, Any]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    panels = spec["panels"]
    primary = tuple(str(day) for day in panels["development_primary_grade_a_days"])
    sensitivity = tuple(
        str(day) for day in panels["development_sensitivity_grade_b_days"]
    )
    return primary, sensitivity


def _day_contract(spec: Mapping[str, Any]) -> dict[str, tuple[str, str]]:
    primary, sensitivity = _panel_days(spec)
    return {
        **{day: ("A", PRIMARY_PANEL) for day in primary},
        **{day: ("B", SENSITIVITY_PANEL) for day in sensitivity},
    }


def _ordered_development_days(spec: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(sorted(_day_contract(spec)))


def _decision_feature_columns(spec: Mapping[str, Any]) -> tuple[str, ...]:
    features = spec.get("decision_visible_features") or {}
    columns = tuple(features.get("campaign_state") or ()) + tuple(
        features.get("local_microstructure") or ()
    )
    if not columns or len(columns) != len(set(columns)):
        raise ValueError("first-add decision-visible feature contract is invalid")
    return tuple(str(column) for column in columns)


def load_frozen_spec(
    path: Path = DEFAULT_SPEC_PATH,
    *,
    quality_validator: QualityValidator = contract.validate_quality_identity,
) -> tuple[dict[str, Any], pd.DataFrame]:
    """Load the byte-frozen spec and validate its external quality identity."""

    spec_path = Path(path).expanduser().resolve()
    spec = _load_json(spec_path)
    contract.validate_spec(spec)
    quality = quality_validator(spec)
    if not isinstance(quality, pd.DataFrame):
        raise TypeError("quality validator must return a DataFrame")

    allowed = set(_day_contract(spec))
    later = set(str(day) for day in spec["panels"]["validation_days_not_read"])
    later.update(
        str(day) for day in spec["panels"]["sealed_holdout_days_not_read"]
    )
    later.update(str(day) for day in spec["panels"]["embargo_days_not_read"])
    if allowed & later:
        raise ValueError("frozen Development overlaps an embargo or later panel")
    return spec, quality.copy()


def _numeric_finite(frame: pd.DataFrame, column: str) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    array = values.to_numpy(dtype=float)
    if values.isna().any() or not np.isfinite(array).all():
        raise ValueError(f"first-add evidence has invalid {column}")
    return values


def validate_trace_artifact(
    frame: pd.DataFrame,
    spec: Mapping[str, Any],
    *,
    require_complete_development: bool = True,
) -> pd.DataFrame:
    """Validate the native trace and attach its immutable analysis panel."""

    contract.validate_spec(spec)
    validated = contract.validate_native_trace(frame, spec)
    missing_features = sorted(set(_decision_feature_columns(spec)) - set(validated))
    if missing_features:
        raise ValueError(
            "first-add evidence is missing decision-visible features: "
            + ", ".join(missing_features)
        )

    for column in _decision_feature_columns(spec):
        if column == "queue_ahead_btc":
            continue
        validated[column] = _numeric_finite(validated, column)
    queue = pd.to_numeric(validated["queue_ahead_btc"], errors="coerce")
    queue_finite = np.isfinite(queue.to_numpy(dtype=float))
    if "queue_ahead_source" not in validated:
        raise ValueError("first-add evidence omitted queue availability identity")
    known_source = validated["queue_ahead_source"].astype(str).isin(
        ("native_exchange_book_exact", "native_exchange_book_known_zero")
    )
    if (known_source & ~queue_finite).any():
        raise ValueError("first-add evidence lost a known native queue value")
    validated["queue_ahead_btc"] = queue
    validated["queue_ahead_available"] = queue_finite.astype(np.uint8)
    for nonnegative in (
        "campaign_age_ms",
        "exposure_increasing_fill_count_so_far",
        "reducing_fill_count_so_far",
        "quote_distance_ticks",
        "queue_ahead_btc",
    ):
        if (validated[nonnegative] < 0.0).any():
            raise ValueError(f"first-add evidence has negative {nonnegative}")

    day_contract = _day_contract(spec)
    observed = set(validated["day"].astype(str))
    expected = set(day_contract)
    if not observed.issubset(expected):
        raise ValueError("first-add evidence read an embargo or later-panel day")
    if require_complete_development and observed != expected:
        missing = sorted(expected - observed)
        extra = sorted(observed - expected)
        raise ValueError(
            "first-add evidence Development denominator is incomplete: "
            f"missing={missing}, extra={extra}"
        )

    validated["day"] = validated["day"].astype(str)
    validated["analysis_panel"] = validated["day"].map(
        {day: panel for day, (_, panel) in day_contract.items()}
    )
    if validated["analysis_panel"].isna().any():
        raise ValueError("first-add evidence panel assignment failed")
    return validated.sort_values(
        ["day", "decision_ts_ms", "campaign_id"], kind="stable"
    ).reset_index(drop=True)


def _extract_day_result(
    result: Mapping[str, Any],
    *,
    day: str,
    quality_grade: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not isinstance(result, Mapping):
        raise TypeError(f"run_day({day}) must return a mapping")
    if TRACE_RESULT_KEY not in result:
        raise ValueError(f"run_day({day}) omitted {TRACE_RESULT_KEY}")
    if TRACE_AUDIT_RESULT_KEY not in result:
        raise ValueError(f"run_day({day}) omitted {TRACE_AUDIT_RESULT_KEY}")
    raw_trace = result[TRACE_RESULT_KEY]
    if isinstance(raw_trace, pd.DataFrame):
        frame = raw_trace.copy()
    elif isinstance(raw_trace, Sequence) and not isinstance(raw_trace, (str, bytes)):
        if not all(isinstance(row, Mapping) for row in raw_trace):
            raise TypeError(
                f"run_day({day}) {TRACE_RESULT_KEY} rows must be mappings"
            )
        frame = pd.DataFrame(list(raw_trace))
    else:
        raise TypeError(
            f"run_day({day}) {TRACE_RESULT_KEY} must be a DataFrame or row sequence"
        )
    if frame.empty:
        raise ValueError(f"run_day({day}) produced no first-add trace rows")
    if set(frame["day"].astype(str)) != {day}:
        raise ValueError(f"run_day({day}) returned rows for another UTC day")
    audit = result[TRACE_AUDIT_RESULT_KEY]
    if not isinstance(audit, Mapping):
        raise TypeError(f"run_day({day}) {TRACE_AUDIT_RESULT_KEY} must be a mapping")
    normalized = {
        "day": day,
        "quality_grade": quality_grade,
        **dict(audit),
    }
    return frame, normalized


def validate_producer_audit(
    audit_frame: pd.DataFrame,
    trace: pd.DataFrame,
    spec: Mapping[str, Any],
) -> pd.DataFrame:
    """Validate producer-side coverage independently of trace row validity."""

    contract.validate_spec(spec)
    required = {
        "day",
        "quality_grade",
        "trace_schema_version",
        "coverage_complete",
        *AUDIT_COUNT_COLUMNS,
        *AUDIT_ZERO_COLUMNS,
    }
    missing = sorted(required - set(audit_frame))
    if missing:
        raise ValueError("first-add producer audit is missing: " + ", ".join(missing))
    if audit_frame.empty or audit_frame["day"].astype(str).duplicated().any():
        raise ValueError("first-add producer audit must contain one row per UTC day")

    audit = audit_frame.copy()
    audit["day"] = audit["day"].astype(str)
    day_contract = _day_contract(spec)
    if set(audit["day"]) != set(day_contract):
        raise ValueError("first-add producer audit Development denominator is incomplete")
    expected_grade = audit["day"].map(
        {day: grade for day, (grade, _) in day_contract.items()}
    )
    if not audit["quality_grade"].astype(str).eq(expected_grade).all():
        raise ValueError("first-add producer audit quality grade drifted")
    if not audit["trace_schema_version"].astype(str).eq(
        contract.TRACE_SCHEMA_VERSION
    ).all():
        raise ValueError("first-add producer audit trace schema drifted")
    coverage = audit["coverage_complete"].map(
        lambda value: isinstance(value, (bool, np.bool_)) and bool(value)
    )
    if not coverage.all():
        raise ValueError("first-add producer audit coverage_complete is false")

    for column in (*AUDIT_COUNT_COLUMNS, *AUDIT_ZERO_COLUMNS):
        values = pd.to_numeric(audit[column], errors="coerce")
        if (
            values.isna().any()
            or (values < 0).any()
            or not np.equal(values, values.astype(int)).all()
        ):
            raise ValueError(f"first-add producer audit has invalid {column}")
        audit[column] = values.astype(int)
    for column in AUDIT_ZERO_COLUMNS:
        if not audit[column].eq(0).all():
            raise ValueError(f"first-add producer audit requires {column}=0")

    equal_counts = audit.loc[:, AUDIT_COUNT_COLUMNS].nunique(axis=1).eq(1)
    if not equal_counts.all():
        raise ValueError("first-add producer selected/emitted/unique/exact_join differ")
    emitted_by_day = trace.groupby(trace["day"].astype(str)).size()
    expected_emitted = audit["day"].map(emitted_by_day).fillna(0).astype(int)
    if not audit["emitted_row_count"].eq(expected_emitted).all():
        raise ValueError("first-add producer emitted count differs from trace rows")
    return audit.sort_values("day", kind="stable").reset_index(drop=True)


def build_trace_from_days(
    spec: Mapping[str, Any],
    run_day: RunDay,
) -> NativeTraceBuild:
    """Call an injected producer once per frozen Development day.

    The callback receives keyword arguments ``day``, ``quality_grade``, and
    ``spec``. Its return value must be the normal replay result mapping with a
    native ``_first_add_decision_to_terminal_trace`` DataFrame.
    """

    contract.validate_spec(spec)
    day_contract = _day_contract(spec)
    frames: list[pd.DataFrame] = []
    audits: list[dict[str, Any]] = []
    for day in _ordered_development_days(spec):
        grade, _ = day_contract[day]
        result = run_day(day=day, quality_grade=grade, spec=spec)
        frame, audit = _extract_day_result(
            result,
            day=day,
            quality_grade=grade,
        )
        if not frame["quality_grade"].astype(str).eq(grade).all():
            raise ValueError(f"run_day({day}) returned the wrong quality grade")
        frames.append(frame)
        audits.append(audit)
    trace = validate_trace_artifact(pd.concat(frames, ignore_index=True), spec)
    producer_audit = validate_producer_audit(pd.DataFrame(audits), trace, spec)
    return NativeTraceBuild(
        trace=trace,
        producer_audit=producer_audit,
        producer_identity=describe_callback(run_day),
    )


def _bootstrap_mean(
    values: Sequence[float],
    *,
    draws: int,
    seed: int,
) -> dict[str, Any]:
    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    if array.size < 2:
        return {
            "estimate": float(array.mean()) if array.size else None,
            "lower_95": None,
            "upper_95": None,
            "supported": False,
        }
    if draws < 100:
        raise ValueError("day-cluster bootstrap requires at least 100 draws")
    rng = np.random.default_rng(seed)
    sampled = rng.choice(array, size=(int(draws), array.size), replace=True)
    means = sampled.mean(axis=1)
    return {
        "estimate": float(array.mean()),
        "lower_95": float(np.quantile(means, 0.025)),
        "upper_95": float(np.quantile(means, 0.975)),
        "supported": True,
    }


def _daily_outcomes(trace: pd.DataFrame) -> pd.DataFrame:
    outcome = contract.PRIMARY_ESTIMAND
    rows: list[dict[str, Any]] = []
    for (panel, side, day), group in trace.groupby(
        ["analysis_panel", "side", "day"], sort=True, observed=True
    ):
        values = group[outcome].to_numpy(dtype=float)
        rows.append(
            {
                "analysis_panel": str(panel),
                "side": str(side),
                "day": str(day),
                "rows": int(len(group)),
                "mean_value_usdc": float(values.mean()),
                "median_value_usdc": float(np.median(values)),
                "q10_value_usdc": float(np.quantile(values, 0.10)),
                "negative_value_rate": float(np.mean(values < 0.0)),
            }
        )
    return pd.DataFrame(rows)


def _outcome_summary(
    daily: pd.DataFrame,
    *,
    draws: int,
    seed: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    metrics = (
        "mean_value_usdc",
        "median_value_usdc",
        "q10_value_usdc",
        "negative_value_rate",
    )
    offset = 0
    for (panel, side), group in daily.groupby(
        ["analysis_panel", "side"], sort=True, observed=True
    ):
        for metric in metrics:
            interval = _bootstrap_mean(
                group[metric].to_numpy(dtype=float),
                draws=draws,
                seed=seed + offset,
            )
            rows.append(
                {
                    "summary_kind": "outcome",
                    "analysis_panel": str(panel),
                    "side": str(side),
                    "feature": None,
                    "metric": metric,
                    "rows": int(group["rows"].sum()),
                    "day_clusters": int(group["day"].nunique()),
                    **interval,
                }
            )
            offset += 1
    return pd.DataFrame(rows)


def _daily_feature_contrasts(
    trace: pd.DataFrame,
    feature: str,
) -> list[float]:
    outcome = contract.PRIMARY_ESTIMAND
    contrasts: list[float] = []
    for _, day_rows in trace.groupby("day", sort=True):
        values = day_rows[feature].to_numpy(dtype=float)
        finite = np.isfinite(values)
        day_rows = day_rows.loc[finite]
        values = values[finite]
        if len(day_rows) < 2 or np.unique(values).size < 2:
            continue
        threshold = float(np.median(values))
        high = day_rows.loc[day_rows[feature] > threshold, outcome]
        low = day_rows.loc[day_rows[feature] <= threshold, outcome]
        if high.empty or low.empty:
            continue
        contrasts.append(float(high.mean() - low.mean()))
    return contrasts


def _mechanism_summary(
    trace: pd.DataFrame,
    spec: Mapping[str, Any],
    *,
    draws: int,
    seed: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    offset = 10_000
    for (panel, side), group in trace.groupby(
        ["analysis_panel", "side"], sort=True, observed=True
    ):
        for feature in _decision_feature_columns(spec):
            contrasts = _daily_feature_contrasts(group, feature)
            interval = _bootstrap_mean(
                contrasts,
                draws=draws,
                seed=seed + offset,
            )
            rows.append(
                {
                    "summary_kind": "mechanism",
                    "analysis_panel": str(panel),
                    "side": str(side),
                    "feature": feature,
                    "metric": "within_day_high_minus_low_value_usdc",
                    "rows": int(len(group)),
                    "day_clusters": int(len(contrasts)),
                    "split_rule": "within_utc_day_feature_median_outcome_blind",
                    **interval,
                }
            )
            offset += 1
    return pd.DataFrame(rows)


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for source in frame.to_dict(orient="records"):
        row: dict[str, Any] = {}
        for key, value in source.items():
            if value is None or (isinstance(value, float) and math.isnan(value)):
                row[key] = None
            elif isinstance(value, np.generic):
                row[key] = value.item()
            else:
                row[key] = value
        records.append(row)
    return records


def evaluate_trace(
    frame: pd.DataFrame,
    producer_audit: pd.DataFrame,
    spec: Mapping[str, Any],
    *,
    bootstrap_draws: int = DEFAULT_BOOTSTRAP_DRAWS,
    bootstrap_seed: int = DEFAULT_BOOTSTRAP_SEED,
) -> dict[str, Any]:
    """Build evidence-only daily and mechanism summaries."""

    trace = validate_trace_artifact(frame, spec)
    audit = validate_producer_audit(producer_audit, trace, spec)
    daily = _daily_outcomes(trace)
    outcome = _outcome_summary(
        daily,
        draws=int(bootstrap_draws),
        seed=int(bootstrap_seed),
    )
    mechanism = _mechanism_summary(
        trace,
        spec,
        draws=int(bootstrap_draws),
        seed=int(bootstrap_seed),
    )
    primary, sensitivity = _panel_days(spec)
    report: dict[str, Any] = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "identity": contract.IDENTITY,
        "mode": "development_observational_evidence_only",
        "decision": "development_evidence_built_no_action_authority",
        "primary_estimand": contract.PRIMARY_ESTIMAND,
        "unit": "USDC_per_first_add_decision",
        "development_outcome_read": True,
        "panel_coverage": {
            PRIMARY_PANEL: {
                "expected_days": list(primary),
                "observed_days": sorted(
                    trace.loc[trace["analysis_panel"].eq(PRIMARY_PANEL), "day"].unique()
                ),
                "rows": int(trace["analysis_panel"].eq(PRIMARY_PANEL).sum()),
            },
            SENSITIVITY_PANEL: {
                "expected_days": list(sensitivity),
                "observed_days": sorted(
                    trace.loc[
                        trace["analysis_panel"].eq(SENSITIVITY_PANEL), "day"
                    ].unique()
                ),
                "rows": int(trace["analysis_panel"].eq(SENSITIVITY_PANEL).sum()),
            },
        },
        "native_producer_audit": {
            "utc_days": int(audit["day"].nunique()),
            "coverage_complete": bool(audit["coverage_complete"].all()),
            "selected_campaign_count": int(audit["selected_campaign_count"].sum()),
            "emitted_row_count": int(audit["emitted_row_count"].sum()),
            "unique_campaign_count": int(audit["unique_campaign_count"].sum()),
            "exact_join_count": int(audit["exact_join_count"].sum()),
            "feature_clock_violation_count": int(
                audit["feature_clock_violation_count"].sum()
            ),
            "open_record_count": int(audit["open_record_count"].sum()),
        },
        "feature_availability": {
            "queue_ahead_available_rows": int(trace["queue_ahead_available"].sum()),
            "queue_ahead_unavailable_rows": int(
                len(trace) - int(trace["queue_ahead_available"].sum())
            ),
            "missing_queue_is_never_imputed_in_f10_evidence": True,
        },
        "bootstrap": {
            "unit": "UTC_day",
            "day_weighting": "equal_weight_daily_statistics",
            "draws": int(bootstrap_draws),
            "seed": int(bootstrap_seed),
            "interval": "pointwise_95_percentile",
        },
        "outcome_summary": _records(outcome),
        "mechanism_summary": _records(mechanism),
        "limitations": {
            "observational_not_action_uplift": True,
            "grade_b_is_sensitivity_only_and_never_pooled": True,
            "mechanism_splits_are_descriptive_not_policy_rules": True,
            "mechanism_intervals_are_pointwise_not_simultaneous": True,
            "later_panels_read": False,
        },
        "permissions": dict(EVIDENCE_PERMISSIONS),
        "ranking_score": None,
        "artifacts": {},
    }
    return {
        "trace": trace,
        "producer_audit": audit,
        "daily_summary": daily,
        "outcome_summary": outcome,
        "mechanism_summary": mechanism,
        "report": report,
    }


def _verified_file_identity(value: Mapping[str, Any], *, label: str) -> dict[str, str]:
    path = Path(str(value.get("path", ""))).expanduser().resolve()
    expected = str(value.get("sha256", "")).lower()
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"{label} must be a regular frozen file")
    if len(expected) != 64 or any(char not in "0123456789abcdef" for char in expected):
        raise ValueError(f"{label} SHA256 is invalid")
    if contract.sha256_file(path) != expected:
        raise ValueError(f"{label} SHA256 mismatch")
    return {"path": str(path), "sha256": expected}


def _native_spec_identity(value: Mapping[str, Any]) -> dict[str, str] | None:
    nested = value.get("native_producer_spec")
    if isinstance(nested, Mapping):
        return _verified_file_identity(nested, label="native producer spec")
    path = value.get("native_producer_spec_path")
    sha256 = value.get("native_producer_spec_sha256")
    if path is None and sha256 is None:
        return None
    return _verified_file_identity(
        {"path": path, "sha256": sha256},
        label="native producer spec",
    )


def describe_callback(run_day: RunDay) -> dict[str, Any]:
    """Freeze callback bytes and, when supplied, its native producer spec."""

    callback_object = run_day
    try:
        source = inspect.getsourcefile(callback_object)
    except TypeError:
        source = inspect.getsourcefile(callback_object.__call__)
    callback_identity: dict[str, Any] = {
        "module": getattr(callback_object, "__module__", None),
        "qualname": getattr(callback_object, "__qualname__", None),
    }
    if source is not None and Path(source).is_file():
        callback_identity.update(
            _verified_file_identity(
                {
                    "path": str(Path(source).resolve()),
                    "sha256": contract.sha256_file(Path(source).resolve()),
                },
                label="run_day callback source",
            )
        )

    provider = getattr(callback_object, "producer_identity", None)
    if provider is None:
        module = inspect.getmodule(callback_object)
        provider = getattr(module, "producer_identity", None) if module else None
    declared: dict[str, Any] = {}
    native_spec: dict[str, str] | None = None
    if provider is not None:
        if not callable(provider):
            raise TypeError("run_day producer_identity must be callable")
        supplied = provider()
        if not isinstance(supplied, Mapping):
            raise TypeError("run_day producer_identity() must return a mapping")
        declared = dict(supplied)
        try:
            json.dumps(declared, sort_keys=True, ensure_ascii=True)
        except (TypeError, ValueError) as exc:
            raise TypeError("run_day producer_identity() must be JSON-serializable") from exc
        native_spec = _native_spec_identity(declared)
    if "sha256" not in callback_identity and native_spec is None:
        raise ValueError(
            "run_day identity requires callback source SHA or a frozen native spec"
        )
    identity: dict[str, Any] = {
        "kind": "injected_run_day_callback",
        "callback": callback_identity,
    }
    if declared:
        identity["declared"] = declared
    if native_spec is not None:
        identity["native_producer_spec"] = native_spec
    return identity


def load_producer_identity(path: Path) -> dict[str, Any]:
    """Load an input-trace producer identity with a verified native spec."""

    identity_path = Path(path).expanduser().resolve()
    payload = _load_json(identity_path)
    native_spec = _native_spec_identity(payload)
    if native_spec is None and isinstance(payload.get("producer_identity"), Mapping):
        native_spec = _native_spec_identity(payload["producer_identity"])
    if native_spec is None:
        raise ValueError("input trace identity requires a frozen native producer spec")
    return {
        "kind": "prebuilt_native_trace",
        "identity_file": {
            "path": str(identity_path),
            "sha256": contract.sha256_file(identity_path),
        },
        "declared": payload,
        "native_producer_spec": native_spec,
    }


def validate_producer_identity(value: Mapping[str, Any]) -> dict[str, Any]:
    """Require a frozen callback source or native producer specification."""

    if not isinstance(value, Mapping):
        raise TypeError("producer identity must be a mapping")
    identity = dict(value)
    native_spec = _native_spec_identity(identity)
    callback_source: dict[str, str] | None = None
    callback = identity.get("callback")
    if isinstance(callback, Mapping) and callback.get("path") is not None:
        callback_source = _verified_file_identity(
            callback,
            label="run_day callback source",
        )
        identity["callback"] = {**dict(callback), **callback_source}
    if native_spec is None and callback_source is None:
        raise ValueError(
            "producer identity requires native producer spec or callback source SHA"
        )
    if native_spec is not None:
        identity["native_producer_spec"] = native_spec
    try:
        json.dumps(identity, sort_keys=True, ensure_ascii=True)
    except (TypeError, ValueError) as exc:
        raise TypeError("producer identity must be JSON-serializable") from exc
    return identity


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def write_evidence_artifacts(
    evaluation: Mapping[str, Any],
    *,
    output_dir: Path,
    spec_path: Path,
    spec: Mapping[str, Any],
    producer_identity: Mapping[str, Any],
) -> dict[str, Path]:
    """Atomically write trace, summaries, report, and manifest."""

    contract.validate_spec(spec)
    frozen_producer_identity = validate_producer_identity(producer_identity)
    output = Path(output_dir).expanduser().resolve()
    if output.exists():
        raise FileExistsError(f"first-add evidence output already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output.name}.partial.", dir=output.parent)
    )
    names = {
        "trace": "first_add_decision_to_terminal_trace.parquet",
        "producer_audit": "native_producer_audit.parquet",
        "daily_summary": "daily_summary.parquet",
        "outcome_summary": "outcome_summary.parquet",
        "mechanism_summary": "mechanism_summary.parquet",
        "report": "report.json",
        "manifest": "manifest.json",
    }
    try:
        for key in (
            "trace",
            "producer_audit",
            "daily_summary",
            "outcome_summary",
            "mechanism_summary",
        ):
            value = evaluation.get(key)
            if not isinstance(value, pd.DataFrame):
                raise TypeError(f"evaluation {key} must be a DataFrame")
            value.to_parquet(staging / names[key], index=False)

        report = dict(evaluation["report"])
        if report.get("mode") != "development_observational_evidence_only":
            raise ValueError("first-add report exceeded evidence-only mode")
        if any(bool(value) for value in (report.get("permissions") or {}).values()):
            raise ValueError("first-add report unexpectedly grants authority")

        final_paths = {key: output / name for key, name in names.items()}
        data_artifacts = {
            key: {
                "path": str(final_paths[key]),
                "sha256": contract.sha256_file(staging / names[key]),
            }
            for key in (
                "trace",
                "producer_audit",
                "daily_summary",
                "outcome_summary",
                "mechanism_summary",
            )
        }
        resolved_spec = Path(spec_path).expanduser().resolve()
        builder_path = Path(__file__).resolve()
        report.update(
            {
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "identity_provenance": {
                    "frozen_spec": {
                        "path": str(resolved_spec),
                        "sha256": contract.sha256_file(resolved_spec),
                        "canonical_spec_sha256": contract.canonical_spec_sha256(spec),
                    },
                    "quality_ledger": dict(spec["quality_identity"]),
                    "builder_module": {
                        "path": str(builder_path),
                        "sha256": contract.sha256_file(builder_path),
                    },
                    "producer": frozen_producer_identity,
                },
                "artifacts": data_artifacts,
            }
        )
        _write_json(staging / names["report"], report)
        report_identity = {
            "path": str(final_paths["report"]),
            "sha256": contract.sha256_file(staging / names["report"]),
        }
        manifest = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "identity": contract.IDENTITY,
            "mode": "development_observational_evidence_only",
            "decision": report["decision"],
            "development_outcome_read": True,
            "validation_read": False,
            "sealed_holdout_read": False,
            "frozen_spec": report["identity_provenance"]["frozen_spec"],
            "producer": frozen_producer_identity,
            "artifacts": {**data_artifacts, "report": report_identity},
            "permissions": dict(EVIDENCE_PERMISSIONS),
            "ranking_score": None,
        }
        _write_json(staging / names["manifest"], manifest)
        staging.replace(output)
        return final_paths
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _load_runner(reference: str) -> RunDay:
    module_name, separator, attribute = reference.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError("runner must use module.path:callable format")
    runner = getattr(importlib.import_module(module_name), attribute)
    if not callable(runner):
        raise TypeError(f"runner is not callable: {reference}")
    return runner


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC_PATH)
    parser.add_argument("--output-dir", type=Path, required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input-trace", type=Path)
    source.add_argument("--runner", help="Injected producer as module.path:callable")
    parser.add_argument(
        "--input-audit",
        type=Path,
        help="Required producer-audit parquet when --input-trace is used",
    )
    parser.add_argument(
        "--producer-identity",
        type=Path,
        help="Required native-producer identity JSON when --input-trace is used",
    )
    parser.add_argument("--bootstrap-draws", type=int, default=DEFAULT_BOOTSTRAP_DRAWS)
    parser.add_argument("--bootstrap-seed", type=int, default=DEFAULT_BOOTSTRAP_SEED)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    spec_path = args.spec.expanduser().resolve()
    spec, _ = load_frozen_spec(spec_path)
    if args.runner:
        if args.input_audit is not None or args.producer_identity is not None:
            raise ValueError("runner mode obtains audit and identity from its callback")
        runner = _load_runner(str(args.runner))
        build = build_trace_from_days(spec, runner)
        trace = build.trace
        producer_audit = build.producer_audit
        producer = build.producer_identity
    else:
        if args.input_audit is None or args.producer_identity is None:
            raise ValueError(
                "--input-trace requires --input-audit and --producer-identity"
            )
        input_path = args.input_trace.expanduser().resolve()
        trace = validate_trace_artifact(pd.read_parquet(input_path), spec)
        audit_path = args.input_audit.expanduser().resolve()
        producer_audit = validate_producer_audit(
            pd.read_parquet(audit_path), trace, spec
        )
        producer = load_producer_identity(args.producer_identity)
        producer["native_trace"] = {
            "path": str(input_path),
            "sha256": contract.sha256_file(input_path),
        }
        producer["native_trace_audit"] = {
            "path": str(audit_path),
            "sha256": contract.sha256_file(audit_path),
        }
    evaluation = evaluate_trace(
        trace,
        producer_audit,
        spec,
        bootstrap_draws=int(args.bootstrap_draws),
        bootstrap_seed=int(args.bootstrap_seed),
    )
    paths = write_evidence_artifacts(
        evaluation,
        output_dir=args.output_dir,
        spec_path=spec_path,
        spec=spec,
        producer_identity=producer,
    )
    print(
        json.dumps(
            {
                "decision": evaluation["report"]["decision"],
                "manifest": str(paths["manifest"]),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
