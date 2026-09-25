"""One bounded Tardis BTCUSDT information increment on the F03 direction path.

This research-only adapter does not reinterpret USDT prices as USDC prices or
replace the frozen F03 bundle. M0 and M1 fit the same existing direction label;
their other quote outputs come from independent frozen F03 signal instances.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
from uuid import uuid4

import numpy as np
import pandas as pd

from data.feature_cursor import FeatureCursor
from data.facts import save_private_json
from data.observation import model_row
from data.runtime import ConsumerBundle
from research.families.f03_causal_13_head.public_input_panel import FEATURE_COLUMNS

EXECUTION_MARKET = "binance_futures:perpetual:BTCUSDC"
REFERENCE_MARKET = "binance_futures:perpetual:BTCUSDT"
REFERENCE_FEATURE_PREFIX = "binance_futures_perpetual_BTCUSDT__"
PANEL_SCHEMA = "narrowgate.f04.tardis_direction_panel.v1"
MODEL_SCHEMA = "narrowgate.f04.tardis_direction_pair.v1"
LABEL = "label_touch_conditioned_up_probability_10000ms"
END = "label_outcome_end_touch_conditioned_up_probability_10000ms"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _plan(path: str | Path) -> tuple[dict, str]:
    path = Path(path)
    plan = json.loads(path.read_text())
    if (plan.get("schema") != "narrowgate.f04.tardis_two_market_first_batch.v1"
            or plan.get("status") != "frozen_before_reference_production_and_evaluation_outcome_read"
            or plan.get("markets") != {
                "execution": EXECUTION_MARKET,
                "reference": REFERENCE_MARKET,
                "reference_currency_conversion": "none; only currency-invariant reference features admitted",
            }
            or plan.get("prediction", {}).get("target", "").split(",", 1)[0]
            != "existing F03 label_touch_conditioned_up_probability_10000ms"):
        raise ValueError("unsupported or unfrozen F04 first-batch contract")
    days = plan.get("fit_days_utc", [])
    if len(days) != len(set(days)) or sorted(days) != days or not days:
        raise ValueError("ordered unique F04 fit days required")
    if set(plan.get("bound_existing_fit_inputs", {})) != set(days):
        raise ValueError("every fit day needs its frozen execution and label identity")
    return plan, _sha(path)


def _feature_metadata(bundle: ConsumerBundle, columns: tuple[str, ...]) -> dict:
    metadata = {key: bundle.manifest[key] for key in (
        "input_contract_id", "observation_contract_id", "feature_contract_id")}
    return {**metadata, "feature_cols": list(columns), "missing_policy": "native_nan"}


def _verified_reference_cursor(root: str | Path, plan: dict, execution: ConsumerBundle,
                               *, plan_sha: str, unit: str,
                               bound_execution_sha: str,
                               relocation_manifest: str | Path | None = None,
                               original_execution_manifest: str | Path | None = None) -> FeatureCursor:
    cursor = FeatureCursor(root)
    ref_plan = cursor.bundle.manifest["plan"]
    exec_plan = execution.manifest["plan"]
    profile = ref_plan["observation_profile"]
    declared = plan["observation"]["reference"]
    expected_profile = dict(exec_plan["observation_profile"])
    for key in ("profile_id", "clock_policy", "market_delay_ns", "processing_ns",
                "measured_latency_path", "measured_latency_sha256",
                "measured_latency_market_id"):
        expected_profile[key] = declared[key]
    actual_execution_sha = _sha(execution.root / "manifest.json")
    reference_parent_sha = ref_plan.get("execution_parent_manifest_sha256")
    if reference_parent_sha != actual_execution_sha:
        if (unit != plan["evaluation"]["shard"] or relocation_manifest is None
                or original_execution_manifest is None):
            raise ValueError("F04 reference execution parent differs from the accepted input")
        from models.replay.settlement_recovery import verify_manifest_relocation

        original = Path(original_execution_manifest)
        previous = Path(relocation_manifest)
        if (_sha(original) != reference_parent_sha or _sha(previous) != bound_execution_sha):
            raise ValueError("F04 reference parent relocation identity changed")
        verify_manifest_relocation(original, previous,
                                   original_sha256=reference_parent_sha,
                                   relocated_sha256=bound_execution_sha)
        verify_manifest_relocation(previous, execution.root / "manifest.json",
                                   original_sha256=bound_execution_sha,
                                   relocated_sha256=actual_execution_sha)
    if (ref_plan["market_id"] != REFERENCE_MARKET
            or (ref_plan["start_ns"], ref_plan["end_ns"])
            != (exec_plan["start_ns"], exec_plan["end_ns"])
            or ref_plan.get("frozen_contract_sha256") != plan_sha
            or ref_plan.get("frozen_unit") != unit
            or ref_plan.get("bound_execution_manifest_sha256") != bound_execution_sha
            or profile != expected_profile):
        raise ValueError("F04 reference bundle has a different market, interval or clock")
    return cursor


def _reference_row(cursor: FeatureCursor, decision_ns: int, *, columns: tuple[str, ...],
                   max_age_ns: int) -> tuple[dict[str, float], int | None, bool]:
    try:
        frame = cursor.at(decision_ns, max_age_ns=max_age_ns)
    except ValueError as exc:
        if str(exc) != "missing or stale causal feature context":
            raise
        return {name: math.nan for name in columns}, None, True
    values = cursor.row(decision_ns, columns=columns, missing_policy="native_nan",
                        max_age_ns=max_age_ns)
    return values, frame.cutoff_ns, False


def build_training_panel(plan_path: str | Path, execution_roots: dict[str, str | Path],
                         reference_roots: dict[str, str | Path],
                         label_roots: dict[str, str | Path], output: str | Path,
                         *, source_archive: str | Path | None = None) -> dict:
    """Create-only raw denominator and fit eligibility from real bound producers.

    The two arms use exactly the same label rows. A missing reference feature is
    retained as NaN, never converted into an invented market observation.
    """
    plan, plan_sha = _plan(plan_path)
    days = plan["fit_days_utc"]
    if any(set(roots) != set(days) for roots in (execution_roots, reference_roots, label_roots)):
        raise ValueError("F04 panel roots must cover exactly the frozen fit days")
    columns = tuple(plan["prediction"]["reference_columns"])
    if not columns or len(columns) != len(set(columns)) or any(
            name in {"mid", "close", "vwap_10s", "weighted_mid_proxy"}
            for name in columns):
        raise ValueError("reference features must be distinct, non-price and currency invariant")
    max_age_ns = plan["observation"]["reference_asof_max_age_ns"]
    if type(max_age_ns) is not int or max_age_ns < 0:
        raise ValueError("invalid reference as-of age")
    output = Path(output)
    if output.exists():
        raise FileExistsError("F04 panel exists; frozen inputs are never overwritten")
    if list(output.parent.glob(output.name + ".*.part")):
        raise FileExistsError("incomplete F04 panel exists; inspect before retry")
    rows, parents, day_counts = [], [], {}
    for day in days:
        expected = plan["bound_existing_fit_inputs"][day]
        execution = ConsumerBundle(execution_roots[day])
        execution_manifest = execution.root / "manifest.json"
        labels_root = Path(label_roots[day])
        label_manifest = labels_root / "manifest.json"
        if (_sha(execution_manifest) != expected["execution_consumer_manifest_sha256"]
                or _sha(label_manifest) != expected["label_manifest_sha256"]):
            raise ValueError(f"frozen F04 parent identity changed on {day}")
        if execution.manifest["plan"]["market_id"] != EXECUTION_MARKET:
            raise ValueError("F04 execution input is not BTCUSDC perpetual")
        # Training consumes the already accepted feature Parquet.  A relocated
        # ConsumerBundle may retain its producer's absolute fact paths; those
        # facts are needed for replay, not for this read-only training view.
        execution.table("features")
        label_meta = json.loads(label_manifest.read_text())
        if (label_meta.get("schema") != "f03.public_label_day.v1"
                or label_meta.get("day") != day
                or label_meta.get("consumer_manifest_sha256") != _sha(execution_manifest)
                or label_meta.get("feature_cols") != list(FEATURE_COLUMNS)
                or _sha(labels_root / "labels.parquet") != label_meta.get("labels_sha256")):
            raise ValueError("F04 cannot relabel an incompatible or changed F03 day")
        reference = _verified_reference_cursor(
            reference_roots[day], plan, execution, plan_sha=plan_sha, unit=day,
            bound_execution_sha=expected["execution_consumer_manifest_sha256"])
        labeled = pd.read_parquet(labels_root / "labels.parquet")
        if (not isinstance(labeled.index, pd.DatetimeIndex) or labeled.index.tz is None
                or not labeled.index.is_monotonic_increasing or labeled.index.has_duplicates
                or not {LABEL, END, *FEATURE_COLUMNS} <= set(labeled)):
            raise ValueError("F04 training labels need ordered ready-time rows and actual ends")
        decision_ns = labeled.index.tz_convert("UTC").as_unit("ns").asi8
        end_times = pd.to_datetime(labeled[END], utc=True)
        end_ns = end_times.astype("int64").to_numpy()
        day_start = pd.Timestamp(day, tz="UTC").value
        day_end = day_start + 86_400_000_000_000
        if np.any((decision_ns < day_start) | (decision_ns >= day_end)):
            raise ValueError("label decisions escaped their frozen UTC fit day")
        known = pd.to_numeric(labeled[LABEL], errors="coerce").to_numpy(dtype=float)
        eligible = (np.isfinite(known) & end_times.notna().to_numpy()
                    & (end_ns >= decision_ns) & (end_ns < day_end))
        if np.any(np.isfinite(known) & end_times.notna().to_numpy() & (end_ns < decision_ns)):
            raise ValueError("label actual outcome end precedes its decision")
        count = {"declared_decisions": len(labeled), "fit_eligible": int(eligible.sum()),
                 "missing_or_censored_label": int((~np.isfinite(known)).sum()),
                 "missing_actual_end": int(end_times.isna().sum()),
                 "outcome_end_at_or_after_day_end": int((end_times.notna().to_numpy()
                     & (end_ns >= day_end)).sum()), "missing_reference_context": 0}
        for index, decision in enumerate(decision_ns):
            reference_values, ref_cutoff, context_missing = _reference_row(
                reference, int(decision), columns=columns, max_age_ns=max_age_ns)
            count["missing_reference_context"] += context_missing
            row = {"day": day, "decision_ns": int(decision), LABEL: known[index],
                   "actual_outcome_end_ns": int(end_ns[index]) if not pd.isna(end_times.iloc[index]) else None,
                   "fit_eligible": bool(eligible[index]),
                   "reference_context_missing": context_missing,
                   "reference_frame_cutoff_ns": ref_cutoff}
            row.update({name: labeled.iloc[index][name] for name in FEATURE_COLUMNS})
            row.update({f"{REFERENCE_FEATURE_PREFIX}{name}": reference_values[name] for name in columns})
            rows.append(row)
        day_counts[day] = count
        parents.append({"day": day, "execution_consumer_manifest_sha256": _sha(execution_manifest),
                        "label_manifest_sha256": _sha(label_manifest),
                        "reference_consumer_manifest_sha256": _sha(reference.bundle.root / "manifest.json")})
    panel = pd.DataFrame(rows)
    # A censored row introduces nulls; plain pandas coercion would turn a
    # nanosecond clock into float64 and silently lose the actual endpoint.
    for name in ("actual_outcome_end_ns", "reference_frame_cutoff_ns"):
        panel[name] = pd.array([row[name] for row in rows], dtype="Int64")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = output.with_name(output.name + "." + uuid4().hex + ".part")
    stage.mkdir(mode=0o700)
    try:
        panel.to_parquet(stage / "panel.parquet", compression="zstd", index=False)
        result = {"schema": PANEL_SCHEMA, "visibility": "local_only_do_not_publish",
                  "plan_sha256": plan_sha, "parents": parents, "rows": len(panel),
                  "source_archive_sha256": _sha(Path(source_archive)) if source_archive else None,
                  "day_counts": day_counts, "feature_cols_M0": list(FEATURE_COLUMNS),
                  "feature_cols_M1": [*FEATURE_COLUMNS, *(f"{REFERENCE_FEATURE_PREFIX}{n}" for n in columns)],
                  "target": LABEL, "actual_outcome_end": END,
                  "panel_sha256": _sha(stage / "panel.parquet"),
                  "evaluation_outcomes_read": False}
        save_private_json(stage / "manifest.json", result)
        os.rename(stage, output)
        return result
    except BaseException as exc:
        # Preserve the failed staging identity; an operator must verify it
        # before any renewed real-data derivation.
        save_private_json(stage / "failure.json", {"type": type(exc).__name__, "message": str(exc)})
        raise


def fit_direction_pair(plan_path: str | Path, panel_root: str | Path,
                       output: str | Path, *, source_archive: str | Path | None = None) -> dict:
    """Fit fixed-budget M0/M1 LightGBM binary heads on identical eligible rows."""
    import lightgbm as lgb

    plan, plan_sha = _plan(plan_path)
    if plan["budget"] != {"new_fit_calls": 2, "inner_selection_calls": 0,
            "refits": 0, "new_full_account_replays_M0": 1,
            "new_full_account_replays_M1": 1,
            "F03_reference_replay_if_existing_not_strictly_comparable": 1,
            "additional_candidates_or_evaluation_accounts": 0,
            "initial_independent_workers": 1, "maximum_independent_workers": 2}:
        raise ValueError("F04 fit budget differs from its frozen first batch")
    panel_root = Path(panel_root)
    meta = json.loads((panel_root / "manifest.json").read_text())
    if (meta.get("schema") != PANEL_SCHEMA or meta.get("plan_sha256") != plan_sha
            or _sha(panel_root / "panel.parquet") != meta.get("panel_sha256")
            or [row["day"] for row in meta.get("parents", [])] != plan["fit_days_utc"]):
        raise ValueError("unbound F04 training panel")
    source_sha = _sha(Path(source_archive)) if source_archive is not None else None
    if meta.get("source_archive_sha256") != source_sha:
        raise ValueError("F04 panel and fitted source snapshot differ")
    for parent in meta["parents"]:
        expected = plan["bound_existing_fit_inputs"][parent["day"]]
        if (parent.get("execution_consumer_manifest_sha256")
                != expected["execution_consumer_manifest_sha256"]
                or parent.get("label_manifest_sha256") != expected["label_manifest_sha256"]
                or not isinstance(parent.get("reference_consumer_manifest_sha256"), str)
                or len(parent["reference_consumer_manifest_sha256"]) != 64):
            raise ValueError("F04 training panel parent identity changed")
    expected_m0 = list(FEATURE_COLUMNS)
    expected_m1 = [*expected_m0, *(f"{REFERENCE_FEATURE_PREFIX}{name}"
                                  for name in plan["prediction"]["reference_columns"])]
    if (meta.get("feature_cols_M0") != expected_m0
            or meta.get("feature_cols_M1") != expected_m1):
        raise ValueError("F04 training feature contract changed")
    panel = pd.read_parquet(panel_root / "panel.parquet")
    if len(panel) != meta["rows"] or list(dict.fromkeys(panel["day"])) != plan["fit_days_utc"]:
        raise ValueError("F04 training panel row support changed")
    required = {"day", "decision_ns", "actual_outcome_end_ns", "fit_eligible",
                "reference_frame_cutoff_ns", LABEL, *expected_m1}
    if not required <= set(panel) or not pd.api.types.is_integer_dtype(panel["decision_ns"]):
        raise ValueError("F04 training panel lacks exact decision/outcome support")
    decisions = panel["decision_ns"].to_numpy(dtype="int64")
    actual_end = panel["actual_outcome_end_ns"]
    if not pd.api.types.is_integer_dtype(actual_end):
        raise ValueError("F04 training actual outcome end lost integer precision")
    ref_cutoff = panel["reference_frame_cutoff_ns"]
    if not pd.api.types.is_integer_dtype(ref_cutoff):
        raise ValueError("F04 reference cutoff lost integer precision")
    observed_ref = ref_cutoff.notna().to_numpy()
    if np.any(ref_cutoff[observed_ref].to_numpy(dtype="int64") > decisions[observed_ref]):
        raise ValueError("F04 training reference was not decision-ready")
    known = pd.to_numeric(panel[LABEL], errors="coerce").to_numpy(dtype=float)
    expected_fit = np.zeros(len(panel), dtype=bool)
    for day in plan["fit_days_utc"]:
        rows = panel["day"].eq(day).to_numpy()
        day_decisions = decisions[rows]
        day_start = pd.Timestamp(day, tz="UTC").value
        day_end = day_start + 86_400_000_000_000
        if (not rows.any() or np.any(day_decisions < day_start)
                or np.any(day_decisions >= day_end)
                or np.any(np.diff(day_decisions) <= 0)):
            raise ValueError("F04 training decision support changed")
        observed_end = actual_end[rows].notna().to_numpy()
        day_actual_end = actual_end[rows].fillna(0).to_numpy(dtype="int64")
        expected_fit[rows] = (np.isfinite(known[rows]) & observed_end
                              & (day_actual_end >= day_decisions)
                              & (day_actual_end < day_end))
        if (meta["day_counts"][day]["fit_eligible"] != int(expected_fit[rows].sum())
                or meta["day_counts"][day]["declared_decisions"] != int(rows.sum())):
            raise ValueError("F04 training panel day counts changed")
    if not np.array_equal(panel["fit_eligible"].to_numpy(dtype=bool), expected_fit):
        raise ValueError("F04 training eligibility bypassed actual outcome end")
    selected = panel.loc[panel["fit_eligible"].astype(bool)]
    y = pd.to_numeric(selected[LABEL], errors="coerce").to_numpy(dtype=float)
    if not len(y) or not np.isin(y, [0.0, 1.0]).all() or set(y) != {0.0, 1.0}:
        raise ValueError("F04 direction fit requires both observed binary classes")
    output = Path(output)
    if output.exists():
        raise FileExistsError("F04 fitted pair exists; no implicit refit or overwrite")
    if list(output.parent.glob(output.name + ".*.part")):
        raise FileExistsError("incomplete F04 fit exists; inspect before retry")
    stage = output.with_name(output.name + "." + uuid4().hex + ".part")
    stage.parent.mkdir(parents=True, exist_ok=True)
    stage.mkdir(mode=0o700)
    specs = {}
    try:
        for arm in ("M0", "M1"):
            cols = meta[f"feature_cols_{arm}"]
            matrix = selected.loc[:, cols].to_numpy(dtype=float)
            if np.isinf(matrix).any() or not np.isfinite(matrix).any(axis=0).all():
                raise ValueError(f"{arm} input has infinity or an entirely unknown feature")
            model = lgb.train({"objective": "binary", "metric": "binary_logloss",
                               "num_leaves": 31, "learning_rate": 0.05,
                               "min_data_in_leaf": 100, "seed": 42,
                               "num_threads": 2, "deterministic": True,
                               "force_col_wise": True, "verbosity": -1},
                              lgb.Dataset(matrix, label=y, feature_name=cols,
                                          free_raw_data=True), num_boost_round=160)
            path = stage / f"{arm}.txt"
            model.save_model(str(path))
            if model.feature_name() != cols:
                raise ValueError("LightGBM changed the frozen F04 feature order")
            specs[arm] = {"model_sha256": _sha(path), "feature_cols": cols,
                          "fitted_rows": len(y), "positive_rows": int(y.sum()),
                          "negative_rows": int(len(y) - y.sum()),
                          "trees": model.num_trees(), "target": LABEL}
        result = {"schema": MODEL_SCHEMA, "visibility": "local_only_do_not_publish",
                  "plan_sha256": plan_sha, "panel_manifest_sha256": _sha(panel_root / "manifest.json"),
                  "panel_sha256": meta["panel_sha256"], "arms": specs,
                  "source_archive_sha256": source_sha,
                  "fit_calls": 2, "inner_selection_calls": 0, "refits": 0,
                  "frozen_F03_inf_model_manifest_sha256": plan["prediction"]["frozen_F03_inf_model_manifest_sha256"],
                  "evaluation_outcomes_read": False}
        save_private_json(stage / "manifest.json", result)
        os.rename(stage, output)
        return result
    except BaseException as exc:
        save_private_json(stage / "failure.json", {"type": type(exc).__name__, "message": str(exc)})
        raise


class DirectionModel:
    """Hash- and feature-order-bound fitted arm, not a synthetic predictor."""

    def __init__(self, root: str | Path, arm: str, *, plan_path: str | Path):
        import lightgbm as lgb

        if arm not in {"M0", "M1"}:
            raise ValueError("F04 arm must be M0 or M1")
        plan, plan_sha = _plan(plan_path)
        root = Path(root)
        manifest = json.loads((root / "manifest.json").read_text())
        if (manifest.get("schema") != MODEL_SCHEMA or manifest.get("plan_sha256") != plan_sha
                or manifest.get("frozen_F03_inf_model_manifest_sha256")
                != plan["prediction"]["frozen_F03_inf_model_manifest_sha256"]):
            raise ValueError("F04 model does not belong to this frozen plan")
        spec = manifest["arms"][arm]
        path = root / f"{arm}.txt"
        if _sha(path) != spec["model_sha256"]:
            raise ValueError("F04 model bytes changed")
        self.booster = lgb.Booster(model_file=str(path))
        self.feature_cols = tuple(spec["feature_cols"])
        if self.booster.feature_name() != list(self.feature_cols):
            raise ValueError("F04 booster internal feature order mismatch")
        self.arm, self.plan, self.manifest = arm, plan, manifest

    def predict(self, matrix: np.ndarray) -> np.ndarray:
        if matrix.ndim != 2 or matrix.shape[1] != len(self.feature_cols) or np.isinf(matrix).any():
            raise ValueError("F04 prediction row/schema invalid")
        # The frozen first-batch budget allows two internal model threads;
        # LightGBM's process default can otherwise consume the entire host.
        values = np.asarray(self.booster.predict(matrix, num_threads=2), dtype=float)
        if values.shape != (len(matrix),) or not np.isfinite(values).all():
            raise ValueError("F04 direction model returned invalid predictions")
        return np.clip(values, 0.0, 1.0)


class TardisDirectionSignalEngine:
    """Preserve 12 frozen heads and state; replace only the paired direction head."""

    def __init__(self, execution_root: str | Path, reference_root: str | Path | None,
                 frozen_base_model_root: str | Path, direction_model: DirectionModel,
                 *, relocation_manifest: str | Path | None = None,
                 original_execution_manifest: str | Path | None = None):
        self.execution = ConsumerBundle(execution_root)
        if self.execution.manifest["plan"]["market_id"] != EXECUTION_MARKET:
            raise ValueError("F04 replay execution bundle must be BTCUSDC")
        self.model = direction_model
        self.plan = direction_model.plan
        frozen_base_model_root = Path(frozen_base_model_root)
        if (_sha(frozen_base_model_root / "public_input_model.json")
                != self.plan["prediction"]["frozen_F03_inf_model_manifest_sha256"]):
            raise ValueError("F04 common outputs require the exact frozen F03/inf bundle")
        from strategy.signal import SignalEngine
        self.base = SignalEngine.from_public_models(
            frozen_base_model_root, symbol="BTCUSDC", ret_demean_halflife=0)
        self.reference = None
        if direction_model.arm == "M1":
            if reference_root is None:
                raise ValueError("M1 requires an independently bound BTCUSDT input")
            self.reference = _verified_reference_cursor(
                reference_root, self.plan, self.execution,
                plan_sha=direction_model.manifest["plan_sha256"],
                unit=self.plan["evaluation"]["shard"],
                bound_execution_sha=self.plan["evaluation"]["execution_consumer_manifest_sha256"],
                relocation_manifest=relocation_manifest,
                original_execution_manifest=original_execution_manifest)
        elif reference_root is not None:
            raise ValueError("M0 cannot secretly consume a reference bundle")
        self.reference_context_missing = 0
        self.reference_values_missing = 0
        self.decisions = 0

    def compute_feature_frames(self, frames) -> list:
        frames = tuple(frames)
        base = self.base.compute_feature_frames(frames)
        if len(base) != len(frames):
            raise ValueError("frozen F03 signal did not cover every F04 decision")
        local_meta = _feature_metadata(self.execution, FEATURE_COLUMNS)
        reference_cols = tuple(self.plan["prediction"]["reference_columns"])
        rows = []
        for frame in frames:
            decision_ns = frame.cutoff_ns
            local = dict(zip(FEATURE_COLUMNS, model_row(frame, local_meta,
                decision_ns=decision_ns), strict=True))
            row = dict(local)
            if self.reference is not None:
                values, _, missing = _reference_row(self.reference, decision_ns,
                    columns=reference_cols,
                    max_age_ns=self.plan["observation"]["reference_asof_max_age_ns"])
                self.reference_context_missing += int(missing)
                self.reference_values_missing += int(any(not math.isfinite(float(v)) for v in values.values()))
                row.update({f"{REFERENCE_FEATURE_PREFIX}{key}": value for key, value in values.items()})
            rows.append([row[name] for name in self.model.feature_cols])
        predicted = self.model.predict(np.asarray(rows, dtype=float))
        for original, direction in zip(base, predicted, strict=True):
            original.touch_conditioned_up_probability_10000ms = float(direction)
        self.decisions += len(frames)
        return base

    def compute_signal(self, *, feature_frame, decision_ns):
        if decision_ns != feature_frame.cutoff_ns:
            raise ValueError("F04 decision clock must equal its ready feature cutoff")
        return self.compute_feature_frames((feature_frame,))[0]

    def report(self) -> dict:
        return {"arm": self.model.arm, "decisions": self.decisions,
                "reference_context_missing": self.reference_context_missing,
                "reference_values_missing": self.reference_values_missing,
                "reference_market": REFERENCE_MARKET if self.reference is not None else None,
                "reference_observation_parity": "simulated_not_measured" if self.reference is not None else None}


def main() -> None:
    import argparse
    import sys
    from research.families.f04_external_market_alpha.daylight_stage import (
        admit_worker, run_daylight,
    )

    parser = argparse.ArgumentParser(description="Frozen first-batch F04 direction panel and fit")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    stages = parser.add_subparsers(dest="stage", required=True)
    panel_parser = stages.add_parser("panel")
    panel_parser.add_argument("--plan", type=Path, required=True)
    panel_parser.add_argument("--mapping", type=Path, required=True,
                              help="private explicit day-to-consumer/label paths")
    panel_parser.add_argument("--output", type=Path, required=True)
    panel_parser.add_argument("--source-archive", type=Path, required=True)
    fit_parser = stages.add_parser("fit")
    fit_parser.add_argument("--plan", type=Path, required=True)
    fit_parser.add_argument("--panel", type=Path, required=True)
    fit_parser.add_argument("--output", type=Path, required=True)
    fit_parser.add_argument("--source-archive", type=Path, required=True)
    args = parser.parse_args()
    if not args.worker:
        raise SystemExit(run_daylight(
            [sys.executable, "-m",
             "research.families.f04_external_market_alpha.tardis_direction",
             "--worker", *sys.argv[1:]], minimum_seconds=7_200))
    admit_worker(minimum_seconds=7_200)
    if args.stage == "panel":
        mapping = json.loads(args.mapping.read_text())
        if set(mapping) != {"execution_roots", "reference_roots", "label_roots"}:
            raise ValueError("F04 panel mapping needs three exact day-to-root maps")
        result = build_training_panel(args.plan, mapping["execution_roots"],
            mapping["reference_roots"], mapping["label_roots"], args.output,
            source_archive=args.source_archive)
    else:
        result = fit_direction_pair(args.plan, args.panel, args.output,
                                    source_archive=args.source_archive)
    print(json.dumps({"stage": args.stage, "output": str(args.output),
                      "schema": result["schema"], "evaluation_outcomes_read": False}, sort_keys=True))


if __name__ == "__main__":
    main()
