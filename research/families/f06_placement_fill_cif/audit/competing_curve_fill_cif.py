#!/usr/bin/env python3
"""Fit and export fill/cancel-ACK competing CIFs for placement orders."""

from __future__ import annotations

import argparse
import gc
import json
import math
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
from research.families.f06_placement_fill_cif.audit.direct_fill_cif import make_expanding_folds
from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import (
    DATA_ROOT,
    MODEL_FEATURES,
    ROOT,
    STATIC_MODEL_FEATURES,
    _activation_values,
    _dynamic_features,
    _fit_side,
    _hazard_probabilities,
    _load_partitions,
    _numeric,
    _sha256,
    derive_duration_contract,
    expand_action_lifecycles,
    fit_activation_contract,
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
    FAMILY_DOCS / "placement_fill_full_curve_competing_cif_v4_spec_20260727.json"
)
DEFAULT_OUTPUT = (
    DATA_ROOT
    / "reports"
    / "placement_fill_full_curve_competing_cif_v4_development_20260727"
)

SCHEMA_VERSION = "placement_fill_full_curve_competing_cif.v1"
MODEL_KIND = "side_specific_fill_cancel_ack_discrete_time_hazard"

IDENTITY_COLUMNS = (
    "action_lifecycle_id",
    "cohort_id",
    "day",
    "side",
    "inventory_role",
    "action",
)


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def predict_competing_cif_at_horizons(
    model: Any,
    lifecycles: pd.DataFrame,
    horizons_ms: Sequence[int],
    *,
    activation_contract: Mapping[str, Any],
    hazard_offset: Mapping[str, float],
    interval_ms: int,
    maximum_support_ms: int,
    chunk_size: int = 5000,
    activation_latency_key: str = "latency_p50_ms",
) -> pd.DataFrame:
    """Return decision-time fill and cancel-ACK CIFs on one probability simplex."""

    horizons = sorted({int(value) for value in horizons_ms})
    if not horizons or horizons[0] <= 0:
        raise ValueError("competing-CIF horizons must be positive")
    if horizons[-1] > int(maximum_support_ms):
        raise ValueError("requested competing-CIF horizon exceeds model support")
    if not isinstance(hazard_offset, Mapping):
        raise TypeError("competing-CIF prediction requires cause-specific offsets")

    outputs: list[pd.DataFrame] = []
    for start in range(0, len(lifecycles), int(chunk_size)):
        chunk = lifecycles.iloc[start : start + int(chunk_size)].reset_index(drop=True)
        activation_probability, activation_latency = _activation_values(
            chunk, activation_contract, latency_key=activation_latency_key
        )
        maximum_active_ms = np.maximum(
            0.0, float(horizons[-1]) - activation_latency
        )
        maximum_bins = int(
            math.ceil(float(maximum_active_ms.max()) / int(interval_ms))
        )
        if maximum_bins <= 0:
            fill_conditional = np.zeros((len(chunk), len(horizons)), dtype=float)
            cancel_conditional = np.zeros_like(fill_conditional)
        else:
            repeated = chunk.loc[
                chunk.index.repeat(maximum_bins), STATIC_MODEL_FEATURES
            ]
            elapsed = np.tile(
                np.arange(1, maximum_bins + 1, dtype=np.int32)
                * int(interval_ms),
                len(chunk),
            )
            dynamic = _dynamic_features(repeated.reset_index(drop=True), elapsed)
            fill_flat, cancel_flat = _hazard_probabilities(
                model, dynamic, hazard_offset
            )
            fill_hazard = fill_flat.reshape(len(chunk), maximum_bins)
            cancel_hazard = cancel_flat.reshape(len(chunk), maximum_bins)
            no_event = np.clip(
                1.0 - fill_hazard - cancel_hazard, 1e-7, 1.0
            )
            survival_after = np.cumprod(no_event, axis=1)
            survival_before = np.column_stack(
                [np.ones(len(chunk), dtype=float), survival_after[:, :-1]]
            )
            fill_after = np.cumsum(survival_before * fill_hazard, axis=1)
            cancel_after = np.cumsum(survival_before * cancel_hazard, axis=1)
            fill_columns: list[np.ndarray] = []
            cancel_columns: list[np.ndarray] = []
            for horizon in horizons:
                active_ms = np.maximum(
                    0.0, float(horizon) - activation_latency
                )
                full_bins = np.floor(active_ms / int(interval_ms)).astype(
                    np.int32
                )
                remainder = active_ms - full_bins * int(interval_ms)
                fill_cif = np.zeros(len(chunk), dtype=float)
                cancel_cif = np.zeros(len(chunk), dtype=float)
                survival = np.ones(len(chunk), dtype=float)
                full_mask = full_bins > 0
                if bool(full_mask.any()):
                    rows = np.flatnonzero(full_mask)
                    columns = full_bins[full_mask] - 1
                    fill_cif[full_mask] = fill_after[rows, columns]
                    cancel_cif[full_mask] = cancel_after[rows, columns]
                    survival[full_mask] = survival_after[rows, columns]
                partial_mask = (remainder > 0.0) & (full_bins < maximum_bins)
                if bool(partial_mask.any()):
                    rows = np.flatnonzero(partial_mask)
                    columns = full_bins[partial_mask]
                    next_fill = fill_hazard[rows, columns]
                    next_cancel = cancel_hazard[rows, columns]
                    total = next_fill + next_cancel
                    fraction = remainder[partial_mask] / float(interval_ms)
                    partial_event = 1.0 - np.power(
                        np.clip(1.0 - total, 1e-7, 1.0), fraction
                    )
                    fill_share = np.divide(
                        next_fill,
                        total,
                        out=np.zeros_like(next_fill),
                        where=total > 0.0,
                    )
                    cancel_share = np.divide(
                        next_cancel,
                        total,
                        out=np.zeros_like(next_cancel),
                        where=total > 0.0,
                    )
                    at_risk = survival[partial_mask] * partial_event
                    fill_cif[partial_mask] += at_risk * fill_share
                    cancel_cif[partial_mask] += at_risk * cancel_share
                fill_columns.append(fill_cif)
                cancel_columns.append(cancel_cif)
            fill_conditional = np.column_stack(fill_columns)
            cancel_conditional = np.column_stack(cancel_columns)

        fill_probability = activation_probability[:, None] * fill_conditional
        cancel_probability = activation_probability[:, None] * cancel_conditional
        no_event_probability = np.clip(
            1.0 - fill_probability - cancel_probability, 0.0, 1.0
        )
        base = chunk.loc[:, IDENTITY_COLUMNS].copy()
        for index, horizon in enumerate(horizons):
            part = base.copy()
            part["horizon_ms"] = int(horizon)
            part["activation_probability"] = activation_probability.astype(
                np.float32
            )
            part["fill_probability"] = fill_probability[:, index].astype(
                np.float32
            )
            part["cancel_ack_probability"] = cancel_probability[:, index].astype(
                np.float32
            )
            part["no_event_probability"] = no_event_probability[:, index].astype(
                np.float32
            )
            outputs.append(part)
    return pd.concat(outputs, ignore_index=True)


def competing_labels_at_horizons(
    lifecycles: pd.DataFrame, horizons_ms: Sequence[int]
) -> pd.DataFrame:
    """Return mutually exclusive fill, cancel-ACK, and no-event labels."""

    fill_time = _numeric(
        lifecycles, "placement_event_time_ms", math.nan
    ).to_numpy(dtype=float)
    submit = _numeric(lifecycles, "submit_ts_ns").to_numpy(dtype=np.int64)
    cancel_ack = _numeric(lifecycles, "cancel_ack_ts_ns").to_numpy(
        dtype=np.int64
    )
    cancel_event = _numeric(
        lifecycles, "cancel_event_observed"
    ).to_numpy(dtype=bool)
    cancel_time = np.where(
        cancel_event & (cancel_ack > submit),
        (cancel_ack - submit) / 1_000_000.0,
        np.nan,
    )
    terminal_time = _numeric(
        lifecycles, "placement_nonfill_terminal_ms", math.nan
    ).to_numpy(dtype=float)
    observation_end = _numeric(
        lifecycles, "placement_observation_end_ms"
    ).to_numpy(dtype=float)
    base = lifecycles.loc[:, IDENTITY_COLUMNS].copy()
    outputs: list[pd.DataFrame] = []
    for horizon in sorted({int(value) for value in horizons_ms}):
        fill = np.isfinite(fill_time) & (fill_time <= float(horizon))
        cancel = (
            (~fill)
            & np.isfinite(cancel_time)
            & (cancel_time <= float(horizon))
        )
        terminal = np.isfinite(terminal_time) & (
            terminal_time <= float(horizon)
        )
        observed = fill | cancel | terminal | (observation_end >= float(horizon))
        part = base.loc[observed].copy()
        part["horizon_ms"] = int(horizon)
        part["fill_target"] = fill[observed].astype(np.int8)
        part["cancel_ack_target"] = cancel[observed].astype(np.int8)
        part["no_event_target"] = (~(fill | cancel))[observed].astype(np.int8)
        outputs.append(part)
    return pd.concat(outputs, ignore_index=True)


def fit_competing_baseline_rates(
    train: pd.DataFrame, horizons_ms: Sequence[int]
) -> dict[str, tuple[float, float]]:
    labels = competing_labels_at_horizons(train, horizons_ms)
    side_rates = labels.groupby(["side", "horizon_ms"], observed=True)[
        ["fill_target", "cancel_ack_target"]
    ].mean()
    rates: dict[str, tuple[float, float]] = {}
    for (side, role, action, horizon), group in labels.groupby(
        ["side", "inventory_role", "action", "horizon_ms"], observed=True
    ):
        prior = side_rates.loc[(side, horizon)]
        denominator = len(group) + 200.0
        fill = (
            float(group["fill_target"].sum())
            + 200.0 * float(prior["fill_target"])
        ) / denominator
        cancel = (
            float(group["cancel_ack_target"].sum())
            + 200.0 * float(prior["cancel_ack_target"])
        ) / denominator
        if fill + cancel > 1.0:
            scale = (fill + cancel) / (1.0 - 1e-7)
            fill /= scale
            cancel /= scale
        key = f"{side}|{str(role).lower()}|{action}|{int(horizon)}"
        rates[key] = (float(fill), float(cancel))
    return rates


def apply_competing_baseline(
    frame: pd.DataFrame, rates: Mapping[str, tuple[float, float]]
) -> tuple[np.ndarray, np.ndarray]:
    values = [
        rates[f"{side}|{str(role).lower()}|{action}|{int(horizon)}"]
        for side, role, action, horizon in zip(
            frame["side"],
            frame["inventory_role"],
            frame["action"],
            frame["horizon_ms"],
            strict=False,
        )
    ]
    array = np.asarray(values, dtype=float)
    return array[:, 0], array[:, 1]


def _load_spec(path: Path) -> dict[str, Any]:
    payload = load_placement_fill_spec(path)
    if payload.get("schema_version") != (
        "narrowgate_placement_fill_full_curve_spec.v4"
    ):
        raise RuntimeError("unsupported competing full-curve placement spec")
    if payload.get("research_status") != (
        "frozen_before_v4_competing_curve_development_fit"
    ):
        raise RuntimeError("competing full-curve placement spec is not frozen")
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
        runner_id="f06.competing_curve_fill_cif",
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
        raise RuntimeError("Development exposure quantiles changed after v4 freeze")
    maximum_support_ms = int(duration["maximum_support_ms"])
    if not args.smoke_days and maximum_support_ms != int(
        spec["reporting"]["frozen_maximum_support_ms"]
    ):
        raise RuntimeError("Development maximum support changed after v4 freeze")
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
            model, offset, fit_identity = _fit_side(
                train, spec=spec, maximum_support_ms=maximum_support_ms
            )
            if not isinstance(offset, Mapping):
                raise TypeError("v4 fit did not return cause-specific offsets")
            activation = fit_activation_contract(train)
            prediction = predict_competing_cif_at_horizons(
                model,
                test,
                report_horizons,
                activation_contract=activation,
                hazard_offset=offset,
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
        raise RuntimeError("v4 competing full-curve fit produced no OOF rows")
    oof = pd.concat(oof_parts, ignore_index=True)

    final_models: dict[str, Any] = {}
    final_fit: dict[str, Any] = {}
    for side in ("BUY", "SELL"):
        side_rows = lifecycles.loc[lifecycles["side"].eq(side)]
        model, offset, identity = _fit_side(
            side_rows, spec=spec, maximum_support_ms=maximum_support_ms
        )
        final_models[side] = {"model": model, "hazard_offset": offset}
        final_fit[side] = identity
    activation_contract = fit_activation_contract(lifecycles)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    code = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        args.output_dir / "code_checkpoint", repo_root=ROOT, code_identity=code
    )
    oof_path = args.output_dir / "oof_competing_predictions.parquet"
    artifact_path = args.output_dir / "competing_curve_fill_cif.joblib"
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
        "placement_estimand": (
            "P(T_fill<=t,T_fill<T_cancelACK|do(placement),x0)"
        ),
        "exported_causes": ["fill", "cancel_ack", "no_event"],
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
        "active_order_keep_replace": "separate_not_built",
        "campaign_repair": "separate_not_built",
        "folds": fold_identity,
        "final_fit": final_fit,
        "spec_sha256": _sha256(args.spec),
        "git": code,
        "checkpoint": checkpoint,
        "outputs": {
            "oof_competing_predictions": {
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
