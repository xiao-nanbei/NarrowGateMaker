#!/usr/bin/env python3
"""Corrected native OOF audit for the F05 v1.2 source-grid study."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_add_wait_incremental_value_v1_2_source_grid_study as source_study,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_add_wait_incremental_value_v1_2_study as label_study,
)
from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_add_wait_incremental_value import (
    campaign_unit_weights,
)

ROOT = source_study.ROOT
IDENTITY = source_study.IDENTITY
OUTPUT = source_study.OUTPUT
SCHEMA_VERSION = f"{IDENTITY}.source_grid_native_oof.v3"
EXECUTION_AMENDMENT = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "multiscale_ema_add_wait_incremental_value_v1_2_"
    "source_grid_oof_execution_amendment_v3_20260809.json"
)


class SourceGridOofStudyError(RuntimeError):
    """Fail closed on the corrected native OOF execution identity."""


def _validate_artifact(binding: Mapping[str, Any], *, role: str) -> Path:
    path = Path(str(binding.get("path", "")))
    if not path.is_absolute():
        path = ROOT / path
    path = path.expanduser().resolve()
    expected = str(binding.get("sha256", ""))
    if not path.is_file() or label_study._sha256_file(path) != expected:
        raise SourceGridOofStudyError(f"{role} artifact drifted: {path}")
    return path


def _validate_execution_amendment() -> dict[str, Any]:
    if not EXECUTION_AMENDMENT.is_file():
        raise SourceGridOofStudyError("source-grid OOF execution amendment v3 is not frozen")
    payload = label_study._load_json(EXECUTION_AMENDMENT)
    if payload.get("identity") != IDENTITY:
        raise SourceGridOofStudyError("source-grid OOF amendment identity drifted")
    correction = payload.get("oof_denominator_correction") or {}
    if (
        correction.get("complete_label_rows") != 320
        or correction.get("outer_oof_rows") != 192
        or correction.get("outer_oof_rows_per_side") != 96
    ):
        raise SourceGridOofStudyError("source-grid OOF denominator contract drifted")
    for row in payload.get("artifacts") or ():
        _validate_artifact(row, role=str(row.get("role", "execution artifact")))
    return payload


def _expected_oof_membership(
    panel: pd.DataFrame,
    folds: Sequence[Mapping[str, Any]],
) -> tuple[set[str], dict[str, int], tuple[str, ...]]:
    day_to_fold: dict[str, int] = {}
    for fold in folds:
        fold_id = int(fold["fold"])
        for day in fold["test_days"]:
            day = str(day)
            if day in day_to_fold:
                raise SourceGridOofStudyError(
                    f"outer OOF test day appears in multiple folds: {day}"
                )
            day_to_fold[day] = fold_id
    test_days = tuple(sorted(day_to_fold))
    expected = panel.loc[panel["utc_day"].astype(str).isin(test_days)].copy()
    if expected["opportunity_id"].duplicated().any():
        raise SourceGridOofStudyError("expected native OOF opportunity IDs are not unique")
    side_counts = expected.groupby("side", observed=True).size().to_dict()
    if (
        len(test_days) != 24
        or len(expected) != 192
        or side_counts != {"BUY": 96, "SELL": 96}
    ):
        raise SourceGridOofStudyError(
            "frozen outer OOF membership is not 24 days / 192 rows / 96 per side"
        )
    return set(expected["opportunity_id"].astype(str)), day_to_fold, test_days


def evaluate(*, output: Path = OUTPUT) -> dict[str, Any]:
    label_study._spec()
    source_study._validate_correction()
    source_study._validate_execution_amendment()
    _validate_execution_amendment()
    panel, label_manifest = source_study._load_complete_labels(output)
    encoder, encoder_manifest = source_study._load_encoder(output)

    raw_ema = panel[list(encoder.feature_names)].to_numpy(dtype=np.float64)
    encoded = encoder.transform(raw_ema)
    encoded_columns = tuple(
        f"ema_2025_source_grid_pc_{index:03d}"
        for index in range(encoded.shape[1])
    )
    for index, name in enumerate(encoded_columns):
        panel[name] = encoded[:, index]
    panel["campaign_weight"] = campaign_unit_weights(
        panel,
        campaign_columns=("utc_day", "side", "prospective_campaign_side_id"),
    )
    weight_sums = panel.groupby(
        ["utc_day", "side", "prospective_campaign_side_id"], observed=True
    )["campaign_weight"].sum()
    weight_error = float((weight_sums - 1.0).abs().max())
    if weight_error > 1e-12:
        raise SourceGridOofStudyError("campaign total training weight drifted")

    predecessor_spec = label_study._load_json(label_study.predecessor.SPEC)
    folds = predecessor_spec["chronological_oof"]["folds"]
    expected_ids, day_to_fold, test_days = _expected_oof_membership(panel, folds)
    oof_rows: list[pd.DataFrame] = []
    for side in ("SELL", "BUY"):
        side_frame = panel.loc[panel["side"].eq(side)].copy()
        for fold in folds:
            test = side_frame.loc[
                side_frame["utc_day"].isin(fold["test_days"])
            ].copy()
            if test.empty:
                raise SourceGridOofStudyError(
                    f"{side} fold {fold['fold']} has no test rows"
                )
            first_test_ts = int(test["ts_ms"].min())
            train = side_frame.loc[
                side_frame["utc_day"].isin(
                    fold["fit_day_candidates_after_day_embargo"]
                )
                & side_frame["joint_washout_ts_ms"].lt(first_test_ts)
            ].copy()
            if train.empty:
                raise SourceGridOofStudyError(
                    f"{side} fold {fold['fold']} has no training rows"
                )
            m0 = label_study.predecessor._fit_predict(
                train, test, label_study.predecessor.M0_FEATURES
            )
            m1 = label_study._fit_predict_m1(
                train, test, encoded_columns=encoded_columns
            )
            rows = test[
                [
                    "opportunity_id",
                    "utc_day",
                    "side",
                    "cooldown_phase",
                    "prospective_campaign_side_id",
                    "campaign_weight",
                    "add_minus_wait_value_usdc",
                ]
            ].copy()
            rows["fold"] = int(fold["fold"])
            rows["prediction_m0"] = m0
            rows["prediction_m1"] = m1
            oof_rows.append(rows)

    oof = pd.concat(oof_rows, ignore_index=True)
    actual_ids = set(oof["opportunity_id"].astype(str))
    if oof["opportunity_id"].duplicated().any() or actual_ids != expected_ids:
        raise SourceGridOofStudyError(
            "native OOF rows do not exactly match the frozen outer test-day union"
        )
    expected_fold = oof["utc_day"].astype(str).map(day_to_fold)
    if expected_fold.isna().any() or not np.array_equal(
        expected_fold.to_numpy(dtype=np.int64),
        oof["fold"].to_numpy(dtype=np.int64),
    ):
        raise SourceGridOofStudyError("native OOF fold ownership drifted")

    oof["squared_error_reduction"] = (
        (oof["add_minus_wait_value_usdc"] - oof["prediction_m0"]) ** 2
        - (oof["add_minus_wait_value_usdc"] - oof["prediction_m1"]) ** 2
    )
    oof["absolute_error_reduction"] = (
        (oof["add_minus_wait_value_usdc"] - oof["prediction_m0"]).abs()
        - (oof["add_minus_wait_value_usdc"] - oof["prediction_m1"]).abs()
    )
    oof_path = output / "native_oof_predictions_source_grid_v3.parquet"
    label_study._atomic_parquet(oof_path, oof)

    side_reports: dict[str, Any] = {}
    for side in ("SELL", "BUY"):
        rows = oof.loc[oof["side"].eq(side)].copy()
        squared = label_study.predecessor._nested_cluster_interval(
            rows, "squared_error_reduction", draws=20_000, seed=20_260_809
        )
        absolute = label_study.predecessor._nested_cluster_interval(
            rows, "absolute_error_reduction", draws=20_000, seed=20_260_809
        )
        fold_support = {
            str(fold): int(count)
            for fold, count in rows.groupby("fold", observed=True).size().items()
        }
        side_reports[side] = {
            "native_oof_rows": int(len(rows)),
            "native_oof_days": int(rows["utc_day"].nunique()),
            "native_oof_campaigns": int(
                rows["prospective_campaign_side_id"].nunique()
            ),
            "fold_support": fold_support,
            "squared_error_reduction": squared,
            "absolute_error_reduction": absolute,
            "m1_incremental_prediction_gate_passed": bool(
                squared["lcb_95"] > 0.0
                and absolute["lcb_95"] > 0.0
                and set(fold_support) == {"1", "2", "3", "4"}
            ),
        }

    report = {
        "schema_version": f"{SCHEMA_VERSION}.report",
        "identity": IDENTITY,
        "status": "development_native_oof_prediction_evidence_read",
        "execution_amendment_sha256": label_study._sha256_file(
            EXECUTION_AMENDMENT
        ),
        "source_grid_execution_amendment_v2_sha256": label_study._sha256_file(
            source_study.EXECUTION_AMENDMENT
        ),
        "label_sha256": label_manifest["label_sha256"],
        "provider_encoder_sha256": encoder_manifest["artifact_sha256"],
        "provider_encoder_manifest_sha256": label_study._sha256_file(
            output / "ema_encoder_2025_provider_source_grid_manifest.json"
        ),
        "provider_training_days": encoder_manifest["training_day_count"],
        "provider_training_rows": encoder_manifest["training_rows_both_sides"],
        "provider_sampling_stride": encoder_manifest["sampling_stride"],
        "provider_economic_outcomes_read": False,
        "native_complete_label_rows": int(len(panel)),
        "native_training_history_rows": int(len(panel) - len(oof)),
        "native_oof_rows": int(len(oof)),
        "native_oof_test_days": list(test_days),
        "native_oof_predictions_path": str(oof_path),
        "native_oof_predictions_sha256": label_study._sha256_file(oof_path),
        "right_censored_labels": 0,
        "campaign_total_weight_max_abs_error": weight_error,
        "oof_denominator_semantics": (
            "outer_fold_test_day_union_only; earlier Development days are training "
            "history and are not OOF scoring rows"
        ),
        "side_reports": side_reports,
        "f09_registration_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    label_study._atomic_json(output / "report_source_grid_v3.json", report)
    return report


def preflight(*, output: Path = OUTPUT) -> dict[str, Any]:
    label_study._spec()
    source_study._validate_correction()
    source_study._validate_execution_amendment()
    amendment = _validate_execution_amendment()
    panel, label_manifest = source_study._load_complete_labels(output)
    predecessor_spec = label_study._load_json(label_study.predecessor.SPEC)
    expected_ids, _, test_days = _expected_oof_membership(
        panel, predecessor_spec["chronological_oof"]["folds"]
    )
    source_study._load_encoder(output)
    return {
        "identity": IDENTITY,
        "execution_amendment_sha256": label_study._sha256_file(
            EXECUTION_AMENDMENT
        ),
        "execution_artifacts": len(amendment.get("artifacts") or ()),
        "label_sha256": label_manifest["label_sha256"],
        "native_complete_labels": int(len(panel)),
        "native_outer_oof_rows": int(len(expected_ids)),
        "native_outer_oof_days": int(len(test_days)),
        "action_authorized": False,
        "live_authorized": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "evaluate"))
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    result = (
        preflight(output=args.output)
        if args.command == "preflight"
        else evaluate(output=args.output)
    )
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
