#!/usr/bin/env python3
"""Fit placement competing CIFs with past-only role-aware nested calibration."""

from __future__ import annotations

import argparse
import gc
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from models.audit.experiment_manifest import (
    git_workspace_identity,
    write_code_checkpoint,
)
from research.families.f06_placement_fill_cif import FAMILY_DOCS
from research.families.f06_placement_fill_cif.audit.competing_curve_fill_cif import (
    IDENTITY_COLUMNS,
    apply_competing_baseline,
    competing_labels_at_horizons,
    fit_competing_baseline_rates,
    predict_competing_cif_at_horizons,
)
from research.families.f06_placement_fill_cif.audit.direct_fill_cif import make_expanding_folds
from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import (
    DATA_ROOT,
    MODEL_FEATURES,
    ROOT,
    _load_partitions,
    _sha256,
    build_sampled_risk_rows,
    derive_duration_contract,
    expand_action_lifecycles,
    fit_activation_contract,
    fit_hazard_model,
    fit_hazard_offset,
)
from research.families.f06_placement_fill_cif.audit.placement_fill_spec import (
    load_placement_fill_spec,
)
from research.governance.historical_reproduction import (
    add_historical_reproduction_argument,
    require_historical_reproduction,
    stamp_historical_reproduction_output,
    verify_frozen_source_identity,
)

DEFAULT_SPEC = (
    FAMILY_DOCS / "placement_fill_role_calibrated_competing_cif_v5_spec_20260727.json"
)
DEFAULT_OUTPUT = (
    DATA_ROOT
    / "reports"
    / "placement_fill_role_calibrated_competing_cif_v5_development_20260727"
)

SCHEMA_VERSION = "placement_fill_role_calibrated_competing_cif.v1"
MODEL_KIND = "side_model_with_past_only_role_specific_cause_offsets"


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def _risk_kwargs(
    spec: Mapping[str, Any], maximum_support_ms: int
) -> dict[str, Any]:
    contract = spec["development_fit"]
    return {
        "interval_ms": int(contract["risk_interval_ms"]),
        "maximum_support_ms": int(maximum_support_ms),
        "maximum_negative_intervals_per_action": int(
            contract["maximum_negative_intervals_per_action"]
        ),
        "sampling_strategy": str(contract["risk_sampling"]),
        "hazard_causes": tuple(contract["hazard_causes"]),
    }


def fit_role_calibrated_side(
    train: pd.DataFrame,
    *,
    spec: Mapping[str, Any],
    maximum_support_ms: int,
) -> tuple[Any, dict[str, dict[str, float]], dict[str, Any]]:
    """Fit the side model, then calibrate each role on later train-only days."""

    contract = spec["development_fit"]
    calibration = contract["role_aware_nested_calibration"]
    roles = tuple(str(value) for value in calibration["roles"])
    calibration_day_count = int(calibration["calibration_days"])
    days = sorted(train["day"].astype(str).unique())
    if len(days) <= calibration_day_count:
        raise ValueError("outer train lacks pre-calibration core-fit days")
    core_days = days[:-calibration_day_count]
    calibration_days = days[-calibration_day_count:]
    kwargs = _risk_kwargs(spec, maximum_support_ms)
    core_rows = build_sampled_risk_rows(
        train.loc[train["day"].isin(core_days)], **kwargs
    )
    model_contract = {
        **contract["model"],
        "hazard_causes": list(contract["hazard_causes"]),
    }
    model = fit_hazard_model(core_rows, model_contract)

    role_offsets: dict[str, dict[str, float]] = {}
    role_identity: dict[str, Any] = {}
    calibration_frame = train.loc[train["day"].isin(calibration_days)]
    for role in roles:
        role_lifecycles = calibration_frame.loc[
            calibration_frame["inventory_role"].astype(str).str.lower().eq(role)
        ]
        if role_lifecycles.empty:
            raise ValueError(f"nested calibration lacks role={role}")
        rows = build_sampled_risk_rows(role_lifecycles, **kwargs)
        targets = rows["target"].to_numpy(dtype=np.int8)
        if not bool((targets == 1).any()) or not bool((targets == 2).any()):
            raise ValueError(f"nested calibration lacks both causes for role={role}")
        offset = fit_hazard_offset(model, rows)
        if not isinstance(offset, Mapping):
            raise TypeError("nested competing calibration returned scalar offset")
        role_offsets[role] = {
            "fill": float(offset["fill"]),
            "cancel_ack": float(offset["cancel_ack"]),
        }
        role_identity[role] = {
            "sampled_rows": int(len(rows)),
            "fill_event_intervals": int((targets == 1).sum()),
            "cancel_ack_event_intervals": int((targets == 2).sum()),
            "hazard_offset": role_offsets[role],
        }
        del rows
    identity = {
        "core_fit_days": core_days,
        "nested_calibration_days": calibration_days,
        "core_sampled_rows": int(len(core_rows)),
        "role_calibration": role_identity,
        "past_only": bool(max(core_days) < min(calibration_days)),
    }
    del core_rows
    gc.collect()
    return model, role_offsets, identity


def predict_role_calibrated_competing_cif(
    model: Any,
    lifecycles: pd.DataFrame,
    horizons_ms: Sequence[int],
    *,
    role_offsets: Mapping[str, Mapping[str, float]],
    activation_contract: Mapping[str, Any],
    interval_ms: int,
    maximum_support_ms: int,
    chunk_size: int,
) -> pd.DataFrame:
    roles = set(lifecycles["inventory_role"].astype(str).str.lower().unique())
    missing = sorted(roles - set(role_offsets))
    if missing:
        raise ValueError(f"missing role-aware offsets: {missing}")
    outputs: list[pd.DataFrame] = []
    for role in sorted(roles):
        subset = lifecycles.loc[
            lifecycles["inventory_role"].astype(str).str.lower().eq(role)
        ]
        outputs.append(
            predict_competing_cif_at_horizons(
                model,
                subset,
                horizons_ms,
                activation_contract=activation_contract,
                hazard_offset=role_offsets[role],
                interval_ms=int(interval_ms),
                maximum_support_ms=int(maximum_support_ms),
                chunk_size=int(chunk_size),
            )
        )
    return pd.concat(outputs, ignore_index=True)


def _load_spec(path: Path) -> dict[str, Any]:
    payload = load_placement_fill_spec(path)
    if payload.get("schema_version") != (
        "narrowgate_placement_fill_full_curve_spec.v5"
    ):
        raise RuntimeError("unsupported role-calibrated placement spec")
    if payload.get("research_status") != (
        "frozen_before_v5_role_calibrated_development_fit"
    ):
        raise RuntimeError("role-calibrated placement spec is not frozen")
    for name in ("implementation", "evaluator"):
        expected = str(payload["lineage"][f"{name}_sha256"])
        verify_frozen_source_identity(str(payload["lineage"][name]), expected)
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    add_historical_reproduction_argument(parser)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--smoke-days", type=int, default=0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    args.spec = args.spec.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()
    reproduction_identity = require_historical_reproduction(
        runner_id="f06.role_calibrated_competing_curve_cif",
        enabled=bool(args.historical_reproduction),
        spec_path=args.spec,
    )
    spec = _load_spec(args.spec)
    wide = _load_partitions(spec)
    if int(args.smoke_days) > 0:
        selected = sorted(wide["day"].astype(str).unique())[: int(args.smoke_days)]
        wide = wide.loc[wide["day"].isin(selected)].copy()
    lifecycles = expand_action_lifecycles(wide)
    fit_contract = spec["development_fit"]
    duration = derive_duration_contract(
        lifecycles,
        interval_ms=int(fit_contract["risk_interval_ms"]),
        report_quantiles=spec["reporting"]["development_exposure_quantiles"],
        maximum_support_quantile=float(
            spec["reporting"]["maximum_support_quantile"]
        ),
    )
    frozen_horizons = {
        str(key): int(value)
        for key, value in spec["reporting"]["frozen_empirical_horizons_ms"].items()
    }
    if not args.smoke_days and frozen_horizons != duration["report_quantiles"]:
        raise RuntimeError("Development exposure quantiles changed after v5 freeze")
    maximum_support_ms = int(duration["maximum_support_ms"])
    if not args.smoke_days and maximum_support_ms != int(
        spec["reporting"]["frozen_maximum_support_ms"]
    ):
        raise RuntimeError("Development maximum support changed after v5 freeze")
    report_horizons = sorted(
        set(frozen_horizons.values())
        | {
            int(value)
            for value in spec["reporting"]["legacy_diagnostic_horizons_ms"]
        }
    )
    days = sorted(lifecycles["day"].astype(str).unique())
    minimum_train_days = int(fit_contract["minimum_train_days"])
    if args.smoke_days:
        minimum_train_days = max(2, min(len(days) - 2, minimum_train_days))
    folds = make_expanding_folds(
        days,
        min_train_days=minimum_train_days,
        embargo_days=int(fit_contract["embargo_days"]),
        test_days=int(fit_contract["outer_test_days"]),
    )

    oof_parts: list[pd.DataFrame] = []
    fold_identity: list[dict[str, Any]] = []
    for fold in folds:
        for side in ("BUY", "SELL"):
            train = lifecycles.loc[
                lifecycles["day"].isin(fold["train_days"])
                & lifecycles["side"].eq(side)
            ]
            test = lifecycles.loc[
                lifecycles["day"].isin(fold["test_days"])
                & lifecycles["side"].eq(side)
            ].copy()
            if train.empty or test.empty:
                continue
            model, role_offsets, fit_identity = fit_role_calibrated_side(
                train, spec=spec, maximum_support_ms=maximum_support_ms
            )
            activation = fit_activation_contract(train)
            prediction = predict_role_calibrated_competing_cif(
                model,
                test,
                report_horizons,
                role_offsets=role_offsets,
                activation_contract=activation,
                interval_ms=int(fit_contract["risk_interval_ms"]),
                maximum_support_ms=maximum_support_ms,
                chunk_size=int(fit_contract["prediction_chunk_size"]),
            )
            labels = competing_labels_at_horizons(test, report_horizons)
            scored = prediction.merge(
                labels,
                on=list(IDENTITY_COLUMNS) + ["horizon_ms"],
                how="inner",
                validate="one_to_one",
            )
            rates = fit_competing_baseline_rates(train, report_horizons)
            baseline_fill, baseline_cancel = apply_competing_baseline(
                scored, rates
            )
            scored["baseline_fill_probability"] = baseline_fill.astype(np.float32)
            scored["baseline_cancel_ack_probability"] = baseline_cancel.astype(
                np.float32
            )
            scored["baseline_no_event_probability"] = (
                1.0 - baseline_fill - baseline_cancel
            ).astype(np.float32)
            scored["fold"] = int(fold["fold"])
            oof_parts.append(scored)
            fold_identity.append(
                {
                    "fold": int(fold["fold"]),
                    "side": side,
                    "train_days": list(fold["train_days"]),
                    "embargo_days": list(fold["embargo_days"]),
                    "test_days": list(fold["test_days"]),
                    **fit_identity,
                }
            )
            del train, test, model, prediction, labels, scored
            gc.collect()
    if not oof_parts:
        raise RuntimeError("v5 role-calibrated fit produced no OOF rows")
    oof = pd.concat(oof_parts, ignore_index=True)

    final_models: dict[str, Any] = {}
    final_fit: dict[str, Any] = {}
    for side in ("BUY", "SELL"):
        side_rows = lifecycles.loc[lifecycles["side"].eq(side)]
        model, role_offsets, identity = fit_role_calibrated_side(
            side_rows, spec=spec, maximum_support_ms=maximum_support_ms
        )
        final_models[side] = {
            "model": model,
            "role_hazard_offsets": role_offsets,
        }
        final_fit[side] = identity
    activation_contract = fit_activation_contract(lifecycles)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    code = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        args.output_dir / "code_checkpoint", repo_root=ROOT, code_identity=code
    )
    oof_path = args.output_dir / "oof_role_calibrated_predictions.parquet"
    artifact_path = args.output_dir / "role_calibrated_competing_cif.joblib"
    oof.to_parquet(oof_path, index=False, compression="zstd")
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "model_kind": MODEL_KIND,
        "model_features": MODEL_FEATURES,
        "models": final_models,
        "activation_contract": activation_contract,
        "duration_contract": duration,
        "risk_interval_ms": int(fit_contract["risk_interval_ms"]),
        "maximum_support_ms": maximum_support_ms,
        "fixed_horizons_are_report_only": True,
        "active_order_keep_replace": "separate_not_built",
        "campaign_repair": "separate_not_built",
        "action_or_live_authorization": False,
    }
    joblib.dump(artifact, artifact_path)
    report = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "model_kind": MODEL_KIND,
        "development_days": days,
        "development_cohorts": int(lifecycles["cohort_id"].nunique()),
        "development_action_lifecycles": int(len(lifecycles)),
        "duration_contract": duration,
        "report_horizons_ms": report_horizons,
        "legacy_horizons_are_report_only": True,
        "horizon_cell_prediction_gate": False,
        "curve_level_gate": spec["reporting"]["curve_level_gate"],
        "curve_level_status": "not_evaluated",
        "validation_access_allowed": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authorization": False,
        "folds": fold_identity,
        "final_fit": final_fit,
        "spec_sha256": _sha256(args.spec),
        "git": code,
        "checkpoint": checkpoint,
        "outputs": {
            "oof_role_calibrated_predictions": {
                "path": str(oof_path),
                "sha256": _sha256(oof_path),
            },
            "artifact": {
                "path": str(artifact_path),
                "sha256": _sha256(artifact_path),
            },
        },
    }
    report_path = args.output_dir / "report.json"
    _atomic_json(report, report_path)
    stamp_historical_reproduction_output(args.output_dir, reproduction_identity)
    print(
        json.dumps(
            {
                "development_days": len(days),
                "oof_rows": len(oof),
                "report": str(report_path),
                "validation_read": False,
                "action_or_live_authorization": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
