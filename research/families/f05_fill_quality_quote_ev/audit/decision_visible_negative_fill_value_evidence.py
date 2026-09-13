"""Development-only M0 evidence for decision-visible negative fill value.

The evaluator consumes one byte-frozen F10 native lifecycle parquet and fits
small side-specific linear models.  It is prediction evidence only: it cannot
read later panels, register an action, or authorize live behavior.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from research.families.f10_live_replay_attribution.audit import (
    first_add_decision_to_terminal_contract as f10_contract,
)
from research.governance.paths import resolve_research_path

FIT_SCHEMA_VERSION = "decision_visible_negative_fill_value_evidence.fit.v1"
METHOD_SCHEMA_VERSION = "decision_visible_negative_fill_value_evidence.method.v1"
METHOD_SCHEMA_VERSION_V1_1 = (
    "decision_visible_negative_fill_value_evidence.method.v1_1"
)
REPORT_SCHEMA_VERSION = "decision_visible_negative_fill_value_evidence.report.v1"
IDENTITY = "decision_visible_negative_fill_value_evidence_m0_v1"
IDENTITY_V1_1 = "decision_visible_negative_fill_value_evidence_m0_v1_1"
SUPPORTED_IDENTITIES = (IDENTITY, IDENTITY_V1_1)
TARGET_COLUMN = f10_contract.PRIMARY_ESTIMAND

PRIMARY_PANEL = "grade_a_primary"
SENSITIVITY_PANEL = "grade_b_sensitivity"
SIDES = ("BUY", "SELL")

CAMPAIGN_STATE_FEATURES = (
    "inventory_btc",
    "campaign_age_ms",
    "campaign_pnl_so_far_usdc",
    "campaign_mae_so_far_usdc",
    "exposure_increasing_fill_count_so_far",
    "reducing_fill_count_so_far",
)
LOCAL_MICROSTRUCTURE_FEATURES = (
    "quote_distance_ticks",
    "queue_ahead_btc",
    "microprice_shift_bps",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "local_toxicity",
    "parent_aggtrade_flow_imbalance",
)
LOCAL_MICROSTRUCTURE_FEATURES_V1_1 = (
    *LOCAL_MICROSTRUCTURE_FEATURES,
    "queue_ahead_available",
)

_FORBIDDEN_PERMISSION_KEYS = (
    "validation_read",
    "sealed_holdout_read",
    "action_registration",
    "action_experiment_authorized",
    "live_deployment",
    "live_deployment_authorized",
)


@dataclass(frozen=True)
class M0Evaluation:
    """In-memory Development evidence; no artifact is written implicitly."""

    oof_predictions: pd.DataFrame
    report: dict[str, Any]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_fit_identity_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_fit_identity_sha256", None)
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def canonical_method_contract_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_method_contract_sha256", None)
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require_sha256(value: object, *, name: str) -> str:
    text = str(value).lower()
    if (
        len(text) != 64
        or any(char not in "0123456789abcdef" for char in text)
        or len(set(text)) == 1
    ):
        raise ValueError(f"{name} is not a frozen SHA256")
    return text


def _ordered_days(values: object, *, name: str) -> tuple[str, ...]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise ValueError(f"{name} must be an ordered day sequence")
    days = tuple(str(value) for value in values)
    if len(days) != len(set(days)) or days != tuple(sorted(days)):
        raise ValueError(f"{name} must be unique and chronological")
    for day in days:
        try:
            parsed = pd.Timestamp(day)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} contains an invalid day") from exc
        if parsed.strftime("%Y-%m-%d") != day:
            raise ValueError(f"{name} must use YYYY-MM-DD UTC days")
    return days


def _load_method_contract(
    identity: Mapping[str, Any],
    *,
    expected_identity: str = IDENTITY,
) -> dict[str, Any]:
    path = resolve_research_path(
        str(identity.get("path", "")), require_exists=False
    )
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise ValueError("F05 M0 method contract must be an absolute regular file")
    expected_file_hash = _require_sha256(
        identity.get("sha256"), name="method contract SHA256"
    )
    if sha256_file(path) != expected_file_hash:
        raise ValueError("F05 M0 method contract file hash mismatch")
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected_schema = (
        METHOD_SCHEMA_VERSION_V1_1
        if expected_identity == IDENTITY_V1_1
        else METHOD_SCHEMA_VERSION
    )
    if payload.get("schema_version") != expected_schema:
        raise ValueError("unexpected F05 M0 method schema")
    if payload.get("identity") != expected_identity:
        raise ValueError("unexpected F05 M0 method identity")
    allowed_status = (
        "frozen_structural_queue_missingness_amendment_before_f05_model_fit"
        if expected_identity == IDENTITY_V1_1
        else "frozen_before_native_f10_development_outcome_read"
    )
    if payload.get("status") != allowed_status:
        raise ValueError("F05 M0 method was not frozen before outcome read")
    canonical = _require_sha256(
        payload.get("canonical_method_contract_sha256"),
        name="canonical method contract SHA256",
    )
    if canonical_method_contract_sha256(payload) != canonical:
        raise ValueError("F05 M0 method contract canonical hash mismatch")
    if canonical != str(identity.get("canonical_method_contract_sha256", "")):
        raise ValueError("F05 M0 fit identity references another method contract")
    f10 = payload.get("f10_spec_identity") or {}
    f10_path = resolve_research_path(
        str(f10.get("path", "")), require_exists=False
    )
    if not f10_path.is_absolute() or not f10_path.is_file():
        raise ValueError("F05 M0 method F10 spec is missing")
    if sha256_file(f10_path) != str(f10.get("sha256", "")):
        raise ValueError("F05 M0 method F10 spec file hash mismatch")
    f10_spec = json.loads(f10_path.read_text(encoding="utf-8"))
    f10_contract.validate_spec(f10_spec)
    if f10_spec["canonical_spec_sha256"] != f10.get("canonical_spec_sha256"):
        raise ValueError("F05 M0 method F10 canonical identity drifted")
    permissions = payload.get("permissions") or {}
    if any(bool(permissions.get(key, False)) for key in _FORBIDDEN_PERMISSION_KEYS):
        raise ValueError("F05 M0 method contract grants forbidden authority")
    return payload


def validate_fit_identity(payload: Mapping[str, Any]) -> None:
    """Validate the separately frozen fit identity before reading outcomes."""

    if payload.get("schema_version") != FIT_SCHEMA_VERSION:
        raise ValueError("unexpected F05 M0 fit schema")
    fit_identity = str(payload.get("identity", ""))
    if fit_identity not in SUPPORTED_IDENTITIES:
        raise ValueError("unexpected F05 M0 identity")
    if payload.get("status") != "frozen_native_f10_artifact_development_only":
        raise ValueError("F05 M0 remains blocked until a native F10 artifact is frozen")
    frozen_hash = _require_sha256(
        payload.get("canonical_fit_identity_sha256"),
        name="canonical_fit_identity_sha256",
    )
    if canonical_fit_identity_sha256(payload) != frozen_hash:
        raise ValueError("F05 M0 fit identity hash mismatch")
    method = _load_method_contract(
        payload.get("method_contract_identity") or {},
        expected_identity=fit_identity,
    )

    source = payload.get("f10_source") or {}
    if source.get("trace_schema_version") != f10_contract.TRACE_SCHEMA_VERSION:
        raise ValueError("F05 M0 requires the native F10 trace schema")
    _require_sha256(source.get("artifact_sha256"), name="F10 artifact SHA256")
    _require_sha256(source.get("spec_file_sha256"), name="F10 spec file SHA256")
    _require_sha256(
        source.get("spec_canonical_sha256"),
        name="F10 canonical spec SHA256",
    )
    for key in ("artifact_path", "spec_path"):
        path = Path(str(source.get(key, ""))).expanduser()
        if not path.is_absolute():
            raise ValueError(f"F05 M0 {key} must be absolute")
    if not bool(source.get("exact_native_join_required", False)):
        raise ValueError("F05 M0 cannot weaken the exact native join")
    if not bool(source.get("feature_ready_clock_required", False)):
        raise ValueError("F05 M0 cannot weaken the feature-ready clock")
    producer = source.get("producer_audit") or {}
    producer_counts = {
        key: int(producer.get(key, -1))
        for key in (
            "candidate_campaigns",
            "emitted_rows",
            "exact_join_rows",
        )
    }
    if producer_counts["candidate_campaigns"] < 1 or len(
        set(producer_counts.values())
    ) != 1:
        raise ValueError("F05 M0 native producer join coverage must equal one")
    if int(producer.get("nearest_time_match_rows", -1)) != 0:
        raise ValueError("F05 M0 native producer used a nearest-time join")
    if int(producer.get("feature_clock_violation_rows", -1)) != 0:
        raise ValueError("F05 M0 native producer has feature-clock violations")

    target = payload.get("authoritative_target") or {}
    if (
        target.get("column") != TARGET_COLUMN
        or target.get("unit") != "USDC_per_first_add_decision"
        or not bool(target.get("direct_usdc_prediction", False))
        or bool(target.get("alternative_authoritative_targets", True))
    ):
        raise ValueError("F05 M0 direct USDC authoritative target drifted")
    if target != method.get("authoritative_target"):
        raise ValueError("F05 M0 target differs from the frozen method contract")

    panels = payload.get("panels") or {}
    primary = _ordered_days(panels.get("grade_a_primary_days"), name="Grade-A days")
    sensitivity = _ordered_days(
        panels.get("grade_b_sensitivity_days"),
        name="Grade-B days",
    )
    if len(primary) != 24 or len(sensitivity) != 16:
        raise ValueError("F05 M0 must preserve exactly 24 A and 16 B days")
    if set(primary) & set(sensitivity):
        raise ValueError("F05 M0 primary and sensitivity panels overlap")
    if panels.get("pooling") != "forbidden":
        raise ValueError("F05 M0 cannot pool Grade A and Grade B")
    if panels.get("grade_b_role") != "sensitivity_only":
        raise ValueError("F05 M0 Grade B must remain sensitivity-only")
    method_panels = method.get("panels") or {}
    if (
        panels.get("pooling") != method_panels.get("pooling")
        or panels.get("grade_b_role") != method_panels.get("grade_b_role")
    ):
        raise ValueError("F05 M0 panel policy differs from the frozen method")

    model = payload.get("model") or {}
    if model.get("estimator") != "standardized_ridge_direct_usdc":
        raise ValueError("F05 M0 estimator drifted")
    if tuple(model.get("campaign_state_features") or ()) != CAMPAIGN_STATE_FEATURES:
        raise ValueError("F05 M0 campaign-state feature contract drifted")
    expected_local_features = (
        LOCAL_MICROSTRUCTURE_FEATURES_V1_1
        if fit_identity == IDENTITY_V1_1
        else LOCAL_MICROSTRUCTURE_FEATURES
    )
    if tuple(model.get("local_microstructure_features") or ()) != expected_local_features:
        raise ValueError("F05 M0 local feature contract drifted")
    alpha = float(model.get("ridge_alpha", np.nan))
    if not np.isfinite(alpha) or alpha <= 0.0:
        raise ValueError("F05 M0 ridge_alpha must be frozen and positive")
    if not bool(model.get("buy_sell_separate", False)):
        raise ValueError("F05 M0 must fit BUY and SELL separately")
    if bool(model.get("hyperparameter_search", True)):
        raise ValueError("F05 M0 cannot tune model complexity on outcomes")
    if model != method.get("model"):
        raise ValueError("F05 M0 model differs from the frozen method contract")

    chronology = payload.get("chronology") or {}
    if chronology.get("outer_folds") != "expanding_calendar_past_only":
        raise ValueError("F05 M0 must use expanding chronological OOF")
    if int(chronology.get("minimum_train_days", 0)) != 10:
        raise ValueError("F05 M0 minimum_train_days drifted")
    embargo_days = int(chronology.get("embargo_calendar_days", -1))
    if embargo_days != 1:
        raise ValueError("F05 M0 must keep the one-calendar-day embargo")
    test_block_days = int(chronology.get("test_block_days", 0))
    if not 1 <= test_block_days <= 3:
        raise ValueError("F05 M0 test blocks must contain one to three days")
    if chronology != method.get("chronology"):
        raise ValueError("F05 M0 chronology differs from the frozen method contract")

    risk = payload.get("high_risk") or {}
    quantile = float(risk.get("outer_train_prediction_quantile", np.nan))
    if not 0.0 < quantile < 0.5:
        raise ValueError("F05 M0 high-risk quantile must be frozen below 0.5")
    if risk.get("threshold_source") != "outer_train_predictions_only":
        raise ValueError("F05 M0 high-risk state must be frozen inside train")
    if risk != method.get("high_risk"):
        raise ValueError("F05 M0 high-risk rule differs from the frozen method")

    inference = payload.get("inference") or {}
    if inference.get("bootstrap") != "day_then_campaign_cluster":
        raise ValueError("F05 M0 bootstrap contract drifted")
    if int(inference.get("bootstrap_samples", 0)) < 100:
        raise ValueError("F05 M0 requires at least 100 bootstrap samples")
    if int(inference.get("simultaneous_metric_family_size", 0)) != 8:
        raise ValueError("F05 M0 simultaneous interval family must contain 8 metrics")
    confidence = float(inference.get("familywise_confidence", np.nan))
    if not 0.8 <= confidence < 1.0:
        raise ValueError("F05 M0 familywise confidence is invalid")
    if int(inference.get("minimum_high_risk_rows", 0)) < 1:
        raise ValueError("F05 M0 high-risk row support is not frozen")
    if int(inference.get("minimum_high_risk_days", 0)) < 2:
        raise ValueError("F05 M0 high-risk day support is not frozen")
    daily_threshold = float(
        inference.get("daily_negative_direction_threshold", np.nan)
    )
    if not 0.5 < daily_threshold <= 1.0:
        raise ValueError("F05 M0 daily direction threshold is invalid")
    valid_fraction = float(
        inference.get("minimum_valid_bootstrap_fraction", np.nan)
    )
    if not 0.8 <= valid_fraction <= 1.0:
        raise ValueError("F05 M0 bootstrap validity threshold is invalid")
    if inference != method.get("inference"):
        raise ValueError("F05 M0 inference differs from the frozen method contract")

    access = payload.get("outcome_access") or {}
    if access.get("scope") != "development_only":
        raise ValueError("F05 M0 may read Development only")
    if not bool(access.get("fit_identity_frozen_before_read", False)):
        raise ValueError("F05 M0 fit identity was not frozen before outcome read")
    if access != method.get("outcome_access"):
        raise ValueError("F05 M0 outcome access differs from the frozen method")
    permissions = payload.get("permissions") or {}
    if any(bool(permissions.get(key, False)) for key in _FORBIDDEN_PERMISSION_KEYS):
        raise ValueError("F05 M0 fit identity grants forbidden authority")
    if permissions != method.get("permissions"):
        raise ValueError("F05 M0 permissions differ from the frozen method contract")


def _load_frozen_inputs(
    fit_identity: Mapping[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    validate_fit_identity(fit_identity)
    source = fit_identity["f10_source"]
    artifact_path = resolve_research_path(
        str(source["artifact_path"]), require_exists=False
    )
    spec_path = resolve_research_path(
        str(source["spec_path"]), require_exists=False
    )
    for path, label in ((artifact_path, "artifact"), (spec_path, "spec")):
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"F05 M0 F10 {label} must be a regular frozen file")

    artifact_hash = sha256_file(artifact_path)
    if artifact_hash != str(source["artifact_sha256"]).lower():
        raise ValueError("F05 M0 F10 parquet SHA256 mismatch")
    spec_hash = sha256_file(spec_path)
    if spec_hash != str(source["spec_file_sha256"]).lower():
        raise ValueError("F05 M0 F10 spec file SHA256 mismatch")
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    f10_contract.validate_spec(spec)
    if (
        str(spec["canonical_spec_sha256"]).lower()
        != str(source["spec_canonical_sha256"]).lower()
    ):
        raise ValueError("F05 M0 F10 canonical spec identity mismatch")

    fit_panels = fit_identity["panels"]
    f10_panels = spec["panels"]
    if tuple(fit_panels["grade_a_primary_days"]) != tuple(
        f10_panels["development_primary_grade_a_days"]
    ) or tuple(fit_panels["grade_b_sensitivity_days"]) != tuple(
        f10_panels["development_sensitivity_grade_b_days"]
    ):
        raise ValueError("F05 M0 panel identity differs from frozen F10")

    frame = pd.read_parquet(artifact_path)
    if sha256_file(artifact_path) != artifact_hash:
        raise ValueError("F05 M0 F10 parquet changed while it was being read")
    frame = f10_contract.validate_native_trace(frame, spec)
    producer = source["producer_audit"]
    if len(frame) != int(producer["emitted_rows"]):
        raise ValueError("F05 M0 F10 parquet row count differs from producer audit")
    return frame, spec


def _numeric_finite(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    output = pd.DataFrame(index=frame.index)
    for column in columns:
        if column not in frame:
            raise ValueError(f"F05 M0 F10 parquet is missing feature: {column}")
        values = pd.to_numeric(frame[column], errors="coerce")
        array = values.to_numpy(dtype=float)
        if values.isna().any() or not np.isfinite(array).all():
            raise ValueError(f"F05 M0 feature is not finite: {column}")
        output[column] = array
    return output


def _validate_and_partition(
    frame: pd.DataFrame,
    fit_identity: Mapping[str, Any],
) -> pd.DataFrame:
    primary = tuple(fit_identity["panels"]["grade_a_primary_days"])
    sensitivity = tuple(fit_identity["panels"]["grade_b_sensitivity_days"])
    expected_days = set(primary) | set(sensitivity)
    observed_days = set(frame["day"].astype(str))
    if observed_days != expected_days:
        missing = sorted(expected_days - observed_days)
        extra = sorted(observed_days - expected_days)
        raise ValueError(
            "F05 M0 F10 parquet does not contain the exact 24A/16B Development "
            f"denominator; missing={missing}, extra={extra}"
        )

    output = frame.copy()
    output["day"] = output["day"].astype(str)
    panel_by_day = {
        **{day: PRIMARY_PANEL for day in primary},
        **{day: SENSITIVITY_PANEL for day in sensitivity},
    }
    output["m0_panel"] = output["day"].map(panel_by_day)
    expected_grade = output["m0_panel"].map(
        {PRIMARY_PANEL: "A", SENSITIVITY_PANEL: "B"}
    )
    if not output["quality_grade"].astype(str).eq(expected_grade).all():
        raise ValueError("F05 M0 detected Grade-A/Grade-B panel mixing")
    if output.duplicated(["day", "campaign_id"]).any():
        raise ValueError("F05 M0 requires one native row per day/campaign")

    fit_identity_name = str(fit_identity["identity"])
    local_features = tuple(
        fit_identity["model"]["local_microstructure_features"]
    )
    if fit_identity_name == IDENTITY_V1_1:
        if "queue_ahead_source" not in output:
            raise ValueError("F05 M0 v1.1 requires native queue source identity")
        queue = pd.to_numeric(output["queue_ahead_btc"], errors="coerce")
        queue_finite = np.isfinite(queue.to_numpy(dtype=float))
        source_known = output["queue_ahead_source"].astype(str).isin(
            ("native_exchange_book_exact", "native_exchange_book_known_zero")
        )
        if (source_known & ~queue_finite).any():
            raise ValueError("F05 M0 v1.1 lost a known native queue value")
        if "queue_ahead_available" in output:
            declared = pd.to_numeric(
                output["queue_ahead_available"], errors="coerce"
            )
            if declared.isna().any() or not np.array_equal(
                declared.to_numpy(dtype=np.uint8),
                queue_finite.astype(np.uint8),
            ):
                raise ValueError("F05 M0 v1.1 queue availability drifted")
        output["queue_ahead_available"] = queue_finite.astype(np.float64)
        output["queue_ahead_btc"] = queue.fillna(0.0)
    features = CAMPAIGN_STATE_FEATURES + local_features
    numeric = _numeric_finite(output, features + (TARGET_COLUMN,))
    for column in (*features, TARGET_COLUMN):
        output[column] = numeric[column].to_numpy(dtype=float)
    return output


def _ridge(alpha: float) -> Pipeline:
    return Pipeline(
        [
            ("scale", StandardScaler()),
            ("model", Ridge(alpha=alpha, fit_intercept=True)),
        ]
    )


def _outer_folds(
    days: Sequence[str],
    *,
    minimum_train_days: int,
    embargo_calendar_days: int,
    test_block_days: int,
) -> list[tuple[tuple[str, ...], tuple[str, ...]]]:
    ordered = tuple(sorted(dict.fromkeys(str(day) for day in days)))
    folds: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
    cursor = minimum_train_days
    while cursor < len(ordered):
        test_days = ordered[cursor : cursor + test_block_days]
        test_start = pd.Timestamp(test_days[0])
        cutoff = test_start - pd.Timedelta(days=embargo_calendar_days)
        train_days = tuple(
            day for day in ordered[:cursor] if pd.Timestamp(day) < cutoff
        )
        if len(train_days) >= minimum_train_days:
            folds.append((train_days, test_days))
        cursor += test_block_days
    return folds


def _fit_panel_side(
    frame: pd.DataFrame,
    *,
    fit_identity: Mapping[str, Any],
    panel: str,
    side: str,
) -> pd.DataFrame:
    cell = frame.loc[
        frame["m0_panel"].eq(panel) & frame["side"].astype(str).str.upper().eq(side)
    ].copy()
    if cell.empty:
        raise ValueError(f"F05 M0 has no {panel} {side} rows")
    chronology = fit_identity["chronology"]
    folds = _outer_folds(
        sorted(cell["day"].unique()),
        minimum_train_days=int(chronology["minimum_train_days"]),
        embargo_calendar_days=int(chronology["embargo_calendar_days"]),
        test_block_days=int(chronology["test_block_days"]),
    )
    if not folds:
        raise ValueError(f"F05 M0 has no supported OOF folds for {panel} {side}")

    alpha = float(fit_identity["model"]["ridge_alpha"])
    quantile = float(
        fit_identity["high_risk"]["outer_train_prediction_quantile"]
    )
    baseline_features = list(CAMPAIGN_STATE_FEATURES)
    full_features = baseline_features + list(
        fit_identity["model"]["local_microstructure_features"]
    )
    output: list[pd.DataFrame] = []
    for fold_number, (train_days, test_days) in enumerate(folds):
        train = cell.loc[cell["day"].isin(train_days)].copy()
        test = cell.loc[cell["day"].isin(test_days)].copy()
        if train.empty or test.empty:
            continue
        if pd.Timestamp(train["day"].max()) >= (
            pd.Timestamp(test["day"].min())
            - pd.Timedelta(days=int(chronology["embargo_calendar_days"]))
        ):
            raise ValueError("F05 M0 outer fold violated the past-only embargo")

        baseline = _ridge(alpha)
        local = _ridge(alpha)
        target = train[TARGET_COLUMN].to_numpy(dtype=float)
        baseline.fit(train[baseline_features], target)
        local.fit(train[full_features], target)
        train_local_prediction = np.asarray(
            local.predict(train[full_features]), dtype=float
        )
        risk_threshold = float(np.quantile(train_local_prediction, quantile))

        scored = test[
            [
                "day",
                "quality_grade",
                "campaign_id",
                "decision_id",
                "order_id",
                "side",
                TARGET_COLUMN,
            ]
        ].copy()
        scored["m0_panel"] = panel
        scored["outer_fold"] = fold_number
        scored["outer_train_day_count"] = len(train_days)
        scored["outer_train_max_day"] = max(train_days)
        scored["prediction_campaign_state_usdc"] = np.asarray(
            baseline.predict(test[baseline_features]), dtype=float
        )
        scored["prediction_local_microstructure_usdc"] = np.asarray(
            local.predict(test[full_features]), dtype=float
        )
        scored["high_risk_threshold_usdc"] = risk_threshold
        scored["high_risk"] = scored[
            "prediction_local_microstructure_usdc"
        ].le(risk_threshold)
        scored["squared_error_improvement_usdc2"] = (
            scored[TARGET_COLUMN]
            - scored["prediction_campaign_state_usdc"]
        ).pow(2) - (
            scored[TARGET_COLUMN]
            - scored["prediction_local_microstructure_usdc"]
        ).pow(2)
        scored["absolute_error_improvement_usdc"] = (
            scored[TARGET_COLUMN]
            - scored["prediction_campaign_state_usdc"]
        ).abs() - (
            scored[TARGET_COLUMN]
            - scored["prediction_local_microstructure_usdc"]
        ).abs()
        output.append(scored)
    if not output:
        raise ValueError(f"F05 M0 produced no OOF predictions for {panel} {side}")
    result = pd.concat(output, ignore_index=True)
    if result.duplicated(["day", "campaign_id"]).any():
        raise ValueError("F05 M0 produced duplicate OOF campaign predictions")
    return result


def _metric_values(frame: pd.DataFrame) -> dict[str, float]:
    high_risk = frame.loc[frame["high_risk"]]
    return {
        "local_mse_improvement_usdc2": float(
            frame["squared_error_improvement_usdc2"].mean()
        ),
        "high_risk_mean_value_usdc": (
            float(high_risk[TARGET_COLUMN].mean()) if not high_risk.empty else np.nan
        ),
    }


def _day_campaign_bootstrap(
    frame: pd.DataFrame,
    *,
    samples: int,
    seed: int,
    confidence: float,
    minimum_valid_fraction: float,
    simultaneous_family_size: int,
) -> dict[str, Any]:
    days = np.asarray(sorted(frame["day"].unique()), dtype=object)
    if len(days) < 2:
        raise ValueError("F05 M0 bootstrap requires at least two OOF days")
    rows_by_day = {
        str(day): frame.loc[frame["day"].eq(day)].reset_index(drop=True)
        for day in days
    }
    rng = np.random.default_rng(seed)
    draws: dict[str, list[float]] = {
        "local_mse_improvement_usdc2": [],
        "high_risk_mean_value_usdc": [],
    }
    for _ in range(samples):
        sampled_days = rng.choice(days, size=len(days), replace=True)
        chunks: list[pd.DataFrame] = []
        for day in sampled_days:
            source = rows_by_day[str(day)]
            positions = rng.integers(0, len(source), size=len(source))
            chunks.append(source.iloc[positions])
        sampled = pd.concat(chunks, ignore_index=True)
        metrics = _metric_values(sampled)
        for name, value in metrics.items():
            if np.isfinite(value):
                draws[name].append(float(value))

    tail_probability = (1.0 - confidence) / (2.0 * simultaneous_family_size)
    result: dict[str, Any] = {
        "method": "day_then_campaign_cluster_percentile_bonferroni",
        "samples_requested": samples,
        "familywise_confidence": confidence,
        "metric_family_size": simultaneous_family_size,
        "tail_probability_per_metric": tail_probability,
        "intervals": {},
    }
    for name, values in draws.items():
        valid_fraction = len(values) / samples
        if valid_fraction < minimum_valid_fraction:
            raise ValueError(f"F05 M0 bootstrap support failed for {name}")
        array = np.asarray(values, dtype=float)
        result["intervals"][name] = {
            "valid_samples": len(values),
            "valid_fraction": valid_fraction,
            "lcb": float(np.quantile(array, tail_probability)),
            "median": float(np.quantile(array, 0.5)),
            "ucb": float(np.quantile(array, 1.0 - tail_probability)),
        }
    return result


def _summarize_cell(
    frame: pd.DataFrame,
    *,
    fit_identity: Mapping[str, Any],
    panel: str,
    side: str,
    seed_offset: int,
) -> dict[str, Any]:
    inference = fit_identity["inference"]
    high_risk = frame.loc[frame["high_risk"]]
    high_risk_days = int(high_risk["day"].nunique())
    if len(high_risk) < int(inference["minimum_high_risk_rows"]) or (
        high_risk_days < int(inference["minimum_high_risk_days"])
    ):
        raise ValueError(f"F05 M0 high-risk support failed for {panel} {side}")

    target = frame[TARGET_COLUMN].to_numpy(dtype=float)
    baseline_prediction = frame["prediction_campaign_state_usdc"].to_numpy(
        dtype=float
    )
    local_prediction = frame["prediction_local_microstructure_usdc"].to_numpy(
        dtype=float
    )
    point = _metric_values(frame)
    bootstrap = _day_campaign_bootstrap(
        frame,
        samples=int(inference["bootstrap_samples"]),
        seed=int(inference["bootstrap_seed"]) + seed_offset,
        confidence=float(inference["familywise_confidence"]),
        minimum_valid_fraction=float(
            inference["minimum_valid_bootstrap_fraction"]
        ),
        simultaneous_family_size=int(
            inference["simultaneous_metric_family_size"]
        ),
    )
    daily = (
        frame.groupby("day", sort=True)
        .agg(
            rows=("campaign_id", "size"),
            local_mse_improvement_usdc2=(
                "squared_error_improvement_usdc2",
                "mean",
            ),
        )
        .reset_index()
    )
    high_risk_daily = (
        high_risk.groupby("day", sort=True)[TARGET_COLUMN].mean().rename(
            "high_risk_mean_value_usdc"
        )
    )
    daily = daily.merge(high_risk_daily, on="day", how="left")
    high_risk_daily_valid = daily["high_risk_mean_value_usdc"].dropna()
    daily_negative_fraction = float((high_risk_daily_valid < 0.0).mean())
    local_daily_positive_fraction = float(
        (daily["local_mse_improvement_usdc2"] > 0.0).mean()
    )
    intervals = bootstrap["intervals"]
    daily_threshold = float(inference["daily_negative_direction_threshold"])
    prediction_supported = bool(
        intervals["local_mse_improvement_usdc2"]["lcb"] > 0.0
        and intervals["high_risk_mean_value_usdc"]["ucb"] < 0.0
        and daily_negative_fraction >= daily_threshold
    )
    return {
        "panel": panel,
        "side": side,
        "rows": int(len(frame)),
        "days": int(frame["day"].nunique()),
        "campaigns": int(frame[["day", "campaign_id"]].drop_duplicates().shape[0]),
        "outer_folds": int(frame["outer_fold"].nunique()),
        "direct_usdc_prediction": {
            "target": TARGET_COLUMN,
            "unit": "USDC_per_first_add_decision",
            "campaign_state_mse_usdc2": float(
                np.mean(np.square(target - baseline_prediction))
            ),
            "local_microstructure_mse_usdc2": float(
                np.mean(np.square(target - local_prediction))
            ),
            "local_mse_improvement_usdc2": point[
                "local_mse_improvement_usdc2"
            ],
            "campaign_state_mae_usdc": float(
                np.mean(np.abs(target - baseline_prediction))
            ),
            "local_microstructure_mae_usdc": float(
                np.mean(np.abs(target - local_prediction))
            ),
            "local_mae_improvement_usdc": float(
                frame["absolute_error_improvement_usdc"].mean()
            ),
        },
        "high_risk": {
            "definition": "outer_train_local_prediction_at_or_below_frozen_quantile",
            "quantile": float(
                fit_identity["high_risk"]["outer_train_prediction_quantile"]
            ),
            "rows": int(len(high_risk)),
            "days": high_risk_days,
            "mean_value_usdc": point["high_risk_mean_value_usdc"],
            "daily_negative_fraction": daily_negative_fraction,
            "daily_negative_direction_threshold": daily_threshold,
        },
        "cross_day_stability": {
            "oof_days": int(len(daily)),
            "high_risk_supported_days": int(len(high_risk_daily_valid)),
            "high_risk_negative_days": int((high_risk_daily_valid < 0.0).sum()),
            "local_mse_improvement_positive_fraction": local_daily_positive_fraction,
        },
        "cluster_bootstrap": bootstrap,
        "prediction_supported": prediction_supported,
        "action_authorized": False,
        "live_authorized": False,
    }


def evaluate_frozen_f10_parquet(
    fit_identity: Mapping[str, Any],
) -> M0Evaluation:
    """Run the frozen Development-only M0 evaluation in memory.

    SHA256 and lifecycle validation happen before modeling. Grade A and Grade B
    are fitted, scored, and bootstrapped independently; no pooled metric exists.
    """

    raw, _ = _load_frozen_inputs(fit_identity)
    frame = _validate_and_partition(raw, fit_identity)
    predictions: list[pd.DataFrame] = []
    summaries: dict[str, dict[str, Any]] = {
        PRIMARY_PANEL: {},
        SENSITIVITY_PANEL: {},
    }
    seed_offset = 0
    for panel in (PRIMARY_PANEL, SENSITIVITY_PANEL):
        for side in SIDES:
            oof = _fit_panel_side(
                frame,
                fit_identity=fit_identity,
                panel=panel,
                side=side,
            )
            predictions.append(oof)
            summaries[panel][side] = _summarize_cell(
                oof,
                fit_identity=fit_identity,
                panel=panel,
                side=side,
                seed_offset=seed_offset,
            )
            seed_offset += 10_003

    oof_predictions = pd.concat(predictions, ignore_index=True)
    sensitivity_checks: dict[str, Any] = {}
    primary_supported: dict[str, bool] = {}
    for side in SIDES:
        primary = summaries[PRIMARY_PANEL][side]
        sensitivity = summaries[SENSITIVITY_PANEL][side]
        primary_high_risk = float(primary["high_risk"]["mean_value_usdc"])
        sensitivity_high_risk = float(sensitivity["high_risk"]["mean_value_usdc"])
        direction_not_reversed = bool(
            primary_high_risk == 0.0
            or sensitivity_high_risk == 0.0
            or np.sign(primary_high_risk) == np.sign(sensitivity_high_risk)
        )
        sensitivity_checks[side] = {
            "separately_fitted": True,
            "separately_scored": True,
            "high_risk_direction_not_reversed": direction_not_reversed,
            "contributes_to_primary_metric": False,
        }
        primary_supported[side] = bool(
            primary["prediction_supported"] and direction_not_reversed
        )

    source = fit_identity["f10_source"]
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": fit_identity["identity"],
        "status": "development_prediction_evidence_only",
        "fit_identity_sha256": fit_identity["canonical_fit_identity_sha256"],
        "f10_artifact_sha256": source["artifact_sha256"],
        "f10_spec_canonical_sha256": source["spec_canonical_sha256"],
        "target": {
            "column": TARGET_COLUMN,
            "unit": "USDC_per_first_add_decision",
            "direct_prediction": True,
        },
        "model": {
            "estimator": "standardized_ridge_direct_usdc",
            "ridge_alpha": float(fit_identity["model"]["ridge_alpha"]),
            "campaign_state_feature_count": len(CAMPAIGN_STATE_FEATURES),
            "local_incremental_feature_count": len(
                fit_identity["model"]["local_microstructure_features"]
            ),
            "buy_sell_separate": True,
            "hyperparameter_search": False,
        },
        "panel_contract": {
            "grade_a_days": 24,
            "grade_b_days": 16,
            "pooled_fit": False,
            "pooled_metric": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
        "panels": summaries,
        "grade_b_sensitivity_checks": sensitivity_checks,
        "primary_prediction_supported_by_side": primary_supported,
        "permissions": {
            "prediction_evidence_only": True,
            "action_registration": False,
            "action_experiment_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "live_deployment_authorized": False,
        },
    }
    return M0Evaluation(oof_predictions=oof_predictions, report=report)


__all__ = [
    "CAMPAIGN_STATE_FEATURES",
    "FIT_SCHEMA_VERSION",
    "IDENTITY",
    "IDENTITY_V1_1",
    "LOCAL_MICROSTRUCTURE_FEATURES",
    "LOCAL_MICROSTRUCTURE_FEATURES_V1_1",
    "METHOD_SCHEMA_VERSION",
    "METHOD_SCHEMA_VERSION_V1_1",
    "M0Evaluation",
    "PRIMARY_PANEL",
    "REPORT_SCHEMA_VERSION",
    "SENSITIVITY_PANEL",
    "TARGET_COLUMN",
    "canonical_fit_identity_sha256",
    "canonical_method_contract_sha256",
    "evaluate_frozen_f10_parquet",
    "sha256_file",
    "validate_fit_identity",
]
