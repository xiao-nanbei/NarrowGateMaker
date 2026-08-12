#!/usr/bin/env python3
"""Source-grid 2025 EMA pretraining and native F05 v1.2 OOF audit."""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from models.replay.f05_ema_provider_source_grid import (
    provider_ema_source_grid_batches,
    provider_encoder_feature_names,
)
from models.replay.f05_ema_source_encoder import (
    FullRankEmaEncoder,
    fit_full_rank_encoder,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_add_wait_incremental_value_v1_2_study as label_study,
)
from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_add_wait_incremental_value import (
    campaign_unit_weights,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = label_study.IDENTITY
SCHEMA_VERSION = f"{IDENTITY}.source_grid_development.v2"
OUTPUT = label_study.OUTPUT
CORRECTION = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "multiscale_ema_add_wait_incremental_value_v1_2_"
    "provider_source_grid_correction_20260809.json"
)
EXECUTION_AMENDMENT = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "multiscale_ema_add_wait_incremental_value_v1_2_"
    "source_grid_execution_amendment_v2_20260809.json"
)
OLD_EXECUTION_AMENDMENT = label_study.EXECUTION_AMENDMENT


class SourceGridStudyError(RuntimeError):
    """Fail closed on source-grid, label, or execution identity drift."""


def _validate_artifact(binding: dict[str, Any], *, role: str) -> Path:
    path = Path(str(binding.get("path", "")))
    if not path.is_absolute():
        path = ROOT / path
    path = path.expanduser().resolve()
    if not path.is_file() or label_study._sha256_file(path) != str(
        binding.get("sha256", "")
    ):
        raise SourceGridStudyError(f"{role} artifact drifted: {path}")
    return path


def _validate_correction() -> dict[str, Any]:
    payload = label_study._load_json(CORRECTION)
    if payload.get("identity") != IDENTITY:
        raise SourceGridStudyError("provider source-grid correction identity drifted")
    old = payload["superseded_execution_semantics"]
    if old.get("encoder_artifact_written") is not False or old.get(
        "oof_predictions_written"
    ) is not False:
        raise SourceGridStudyError("superseded 10-second attempt read evidence")
    corrected = payload["corrected_provider_semantics"]
    if (
        corrected.get("source_artifact_resolution_ms") != 100
        or corrected.get("sampling_stride")
        != "none_use_every_admitted_target_day_source_row"
        or corrected.get("provider_economic_labels") != "forbidden"
    ):
        raise SourceGridStudyError("provider source-grid semantics drifted")
    return payload


def _validate_execution_amendment() -> dict[str, Any]:
    if not EXECUTION_AMENDMENT.is_file():
        raise SourceGridStudyError("source-grid execution amendment v2 is not frozen")
    payload = label_study._load_json(EXECUTION_AMENDMENT)
    if payload.get("identity") != IDENTITY:
        raise SourceGridStudyError("source-grid execution amendment identity drifted")
    for row in payload.get("artifacts") or ():
        _validate_artifact(row, role=str(row.get("role", "execution artifact")))
    return payload


def _load_complete_labels(output: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    manifest_path = output / "label_manifest.json"
    manifest = label_study._load_json(manifest_path)
    if manifest.get("execution_amendment_sha256") != label_study._sha256_file(
        OLD_EXECUTION_AMENDMENT
    ):
        raise SourceGridStudyError("cross-day labels lost their v1 execution identity")
    label_path = Path(str(manifest["label_path"]))
    if label_study._sha256_file(label_path) != manifest.get("label_sha256"):
        raise SourceGridStudyError("cross-day label panel drifted")
    panel = pd.read_parquet(label_path)
    if len(panel) != 320 or panel["right_censored"].astype(bool).any():
        raise SourceGridStudyError(
            "all 320 native labels must reach common economic washout"
        )
    return panel, manifest


def _provider_source_paths(day: str) -> tuple[Path, Path]:
    prior = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
    root = label_study.PROVIDER_ROOT / "bbo"
    return (
        root / f"BTCUSDC-bbo-{prior}.parquet",
        root / f"BTCUSDC-bbo-{day}.parquet",
    )


def fit_2025_encoder(*, output: Path = OUTPUT) -> dict[str, Any]:
    label_study._spec()
    _validate_correction()
    _validate_execution_amendment()
    days = label_study._provider_days()
    receipts: list[dict[str, Any]] = []

    def batches() -> Any:
        for day in days:
            prior_path, target_path = _provider_source_paths(day)
            for path in (prior_path, target_path):
                if not path.is_file():
                    raise SourceGridStudyError(
                        f"provider source-grid input is missing: {path}"
                    )
            receipt: dict[str, Any] = {
                "day": day,
                "prior_bbo": {
                    "path": str(prior_path),
                    "sha256": label_study._sha256_file(prior_path),
                },
                "target_bbo": {
                    "path": str(target_path),
                    "sha256": label_study._sha256_file(target_path),
                },
                "side_rows": {},
            }
            source_audit: dict[str, Any] | None = None
            for side, matrix, audit in provider_ema_source_grid_batches(
                pd.read_parquet(prior_path),
                pd.read_parquet(target_path),
                day=day,
            ):
                if source_audit is None:
                    source_audit = audit
                elif source_audit != audit:
                    raise SourceGridStudyError("BUY/SELL source-grid audits differ")
                receipt["side_rows"][side] = int(len(matrix))
                yield matrix
            if set(receipt["side_rows"]) != {"BUY", "SELL"}:
                raise SourceGridStudyError("provider source-grid side support drifted")
            receipt["source_grid"] = source_audit
            receipts.append(receipt)

    names = provider_encoder_feature_names()
    encoder = fit_full_rank_encoder(batches(), feature_names=names)
    artifact_path = output / "ema_encoder_2025_provider_source_grid.npz"
    label_study._atomic_npz(
        artifact_path,
        feature_names=np.asarray(encoder.feature_names, dtype="U128"),
        mean=encoder.mean,
        scale=encoder.scale,
        components=encoder.components,
        eigenvalues=encoder.eigenvalues,
        training_rows=np.asarray([encoder.training_rows], dtype=np.int64),
    )
    total_target_rows = sum(
        int(row["source_grid"]["target_rows"]) for row in receipts
    )
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}.provider_encoder",
        "identity": IDENTITY,
        "correction_sha256": label_study._sha256_file(CORRECTION),
        "execution_amendment_sha256": label_study._sha256_file(
            EXECUTION_AMENDMENT
        ),
        "artifact_path": str(artifact_path),
        "artifact_sha256": label_study._sha256_file(artifact_path),
        "feature_names": list(encoder.feature_names),
        "excluded_feature_suffixes": ["_volatility_normalized"],
        "component_count": len(encoder.feature_names),
        "component_selection": "none_full_rank",
        "training_days": list(days),
        "training_day_count": len(days),
        "target_source_rows_per_side": total_target_rows,
        "training_rows_both_sides": encoder.training_rows,
        "source_resolution_ms": 100,
        "sampling_stride": "none_all_admitted_source_rows",
        "source_receipts": receipts,
        "provider_feature_files_read": False,
        "economic_outcomes_read": False,
        "provider_queue_or_lifecycle_authority": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    label_study._atomic_json(
        output / "ema_encoder_2025_provider_source_grid_manifest.json", manifest
    )
    return manifest


def _load_encoder(output: Path) -> tuple[FullRankEmaEncoder, dict[str, Any]]:
    manifest = label_study._load_json(
        output / "ema_encoder_2025_provider_source_grid_manifest.json"
    )
    artifact = Path(str(manifest["artifact_path"]))
    if label_study._sha256_file(artifact) != manifest.get("artifact_sha256"):
        raise SourceGridStudyError("2025 source-grid EMA encoder drifted")
    with np.load(artifact, allow_pickle=False) as values:
        encoder = FullRankEmaEncoder(
            feature_names=tuple(str(value) for value in values["feature_names"]),
            mean=np.array(values["mean"], copy=True),
            scale=np.array(values["scale"], copy=True),
            components=np.array(values["components"], copy=True),
            eigenvalues=np.array(values["eigenvalues"], copy=True),
            training_rows=int(values["training_rows"][0]),
        )
    encoder.validate()
    if (
        int(manifest.get("training_day_count", -1)) != 66
        or tuple(manifest.get("feature_names") or ())
        != provider_encoder_feature_names()
    ):
        raise SourceGridStudyError("2025 source-grid encoder schema drifted")
    return encoder, manifest


def evaluate(*, output: Path = OUTPUT) -> dict[str, Any]:
    label_study._spec()
    _validate_correction()
    _validate_execution_amendment()
    panel, label_manifest = _load_complete_labels(output)
    encoder, encoder_manifest = _load_encoder(output)
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
        raise SourceGridStudyError("campaign total training weight drifted")

    predecessor_spec = label_study._load_json(label_study.predecessor.SPEC)
    oof_rows: list[pd.DataFrame] = []
    for side in ("SELL", "BUY"):
        side_frame = panel.loc[panel["side"].eq(side)].copy()
        for fold in predecessor_spec["chronological_oof"]["folds"]:
            test = side_frame.loc[
                side_frame["utc_day"].isin(fold["test_days"])
            ].copy()
            first_test_ts = int(test["ts_ms"].min())
            train = side_frame.loc[
                side_frame["utc_day"].isin(
                    fold["fit_day_candidates_after_day_embargo"]
                )
                & side_frame["joint_washout_ts_ms"].lt(first_test_ts)
            ].copy()
            if train.empty or test.empty:
                raise SourceGridStudyError(
                    f"{side} fold {fold['fold']} lacks train/test rows"
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
    if len(oof) != 320 or oof["opportunity_id"].duplicated().any():
        raise SourceGridStudyError("native OOF denominator drifted")
    oof["squared_error_reduction"] = (
        (oof["add_minus_wait_value_usdc"] - oof["prediction_m0"]) ** 2
        - (oof["add_minus_wait_value_usdc"] - oof["prediction_m1"]) ** 2
    )
    oof["absolute_error_reduction"] = (
        (oof["add_minus_wait_value_usdc"] - oof["prediction_m0"]).abs()
        - (oof["add_minus_wait_value_usdc"] - oof["prediction_m1"]).abs()
    )
    oof_path = output / "native_oof_predictions_source_grid.parquet"
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
        "correction_sha256": label_study._sha256_file(CORRECTION),
        "execution_amendment_sha256": label_study._sha256_file(
            EXECUTION_AMENDMENT
        ),
        "label_sha256": label_manifest["label_sha256"],
        "provider_encoder_sha256": encoder_manifest["artifact_sha256"],
        "provider_training_days": encoder_manifest["training_day_count"],
        "provider_training_rows": encoder_manifest["training_rows_both_sides"],
        "provider_sampling_stride": encoder_manifest["sampling_stride"],
        "provider_economic_outcomes_read": False,
        "native_oof_predictions_path": str(oof_path),
        "native_oof_predictions_sha256": label_study._sha256_file(oof_path),
        "selected_opportunities": int(len(panel)),
        "right_censored_labels": 0,
        "campaign_total_weight_max_abs_error": weight_error,
        "side_reports": side_reports,
        "f09_registration_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    label_study._atomic_json(output / "report_source_grid.json", report)
    return report


def preflight(*, output: Path = OUTPUT) -> dict[str, Any]:
    label_study._spec()
    _validate_correction()
    amendment = _validate_execution_amendment()
    panel, manifest = _load_complete_labels(output)
    days = label_study._provider_days()
    missing = [
        str(path)
        for day in days
        for path in _provider_source_paths(day)
        if not path.is_file()
    ]
    if missing:
        raise SourceGridStudyError(f"provider source files are missing: {missing[:3]}")
    return {
        "identity": IDENTITY,
        "execution_amendment_sha256": label_study._sha256_file(
            EXECUTION_AMENDMENT
        ),
        "execution_artifacts": len(amendment.get("artifacts") or ()),
        "label_sha256": manifest["label_sha256"],
        "native_complete_labels": len(panel),
        "provider_training_days": len(days),
        "provider_source_resolution_ms": 100,
        "provider_sampling_stride": "none_all_admitted_source_rows",
        "provider_economic_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("preflight", "fit-2025-encoder", "evaluate")
    )
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    if args.command == "preflight":
        result = preflight(output=args.output)
    elif args.command == "fit-2025-encoder":
        result = fit_2025_encoder(output=args.output)
    else:
        result = evaluate(output=args.output)
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
