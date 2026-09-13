#!/usr/bin/env python3
from __future__ import annotations

"""
Step 4: LightGBM models for market-making signal enhancement.

Three compatibility target families at 10s/30s/60s names:
  1. Post-fill direction (binary classification) → quote diagnostics
  2. Post-fill return    (regression)             → quote diagnostics
  3. Volatility        (regression)             → spread sizing

Local benchmark assumptions:
  ✦  Apple Silicon / ARM64 runs benefit from native LightGBM + NEON
  ✦  nthread defaults to local physical/available cores, capped by MM_LGB_THREADS
  ✦  Unified-memory sizing is a local training convenience, not a live x86 claim

Usage:
  .venv/bin/python -m research.families.f03_causal_13_head.ml_model                   # train all models
  .venv/bin/python -m research.families.f03_causal_13_head.ml_model --target dir_10s  # diagnostic single head
  .venv/bin/python -m research.families.f03_causal_13_head.ml_model --tune            # Optuna search

Source-profile and taker-feature ablations use this same entrypoint with an
explicit versioned model directory and experiment id. They always train all 13
heads and remain research-only until separately promoted.

Predictive metrics (AUC/MAE/IC) remain diagnostic only.
The return/direction label permits a fill during the first h seconds and then
observes h seconds after that fill, so its decision-to-outcome span is h..2h;
it is not a fixed-forward-h return. Bundle selection should use causal replay,
markout, campaign outcomes, and action uplift rather than predictive metrics.
"""

import argparse
import gc
import hashlib
import json
import math
import os
import sys
import time
from dataclasses import dataclass
from multiprocessing import cpu_count
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

try:
    from models.backtest_config import build_backtest_base_params
    from research.families.f03_causal_13_head.feature_variants import (
        apply_feature_variant,
        available_variants,
        feature_variant_contract,
        normalize_variant,
        write_bundle_meta,
    )
    from models.symbol_paths import ROOT, DEFAULT_SYMBOL, paths_for
    from strategy.model_contract import (
        REQUIRED_FEATURE_DAG_ID,
        REQUIRED_FEATURE_DAG_SHA256,
        REQUIRED_FEATURE_SEMANTICS_VERSION,
        REQUIRED_MODEL_HEADS,
        absolute_price_variance_unit_contract,
        validate_variance_unit_contract,
    )
except ImportError:
    from backtest_config import build_backtest_base_params
    from feature_variants import (
        apply_feature_variant,
        available_variants,
        feature_variant_contract,
        normalize_variant,
        write_bundle_meta,
    )
    from symbol_paths import ROOT, DEFAULT_SYMBOL, paths_for
    from strategy.model_contract import (
        REQUIRED_FEATURE_DAG_ID,
        REQUIRED_FEATURE_DAG_SHA256,
        REQUIRED_FEATURE_SEMANTICS_VERSION,
        REQUIRED_MODEL_HEADS,
        absolute_price_variance_unit_contract,
        validate_variance_unit_contract,
    )
from market_fusion import default_reference_symbol
from calendar_features import legacy_calendar_feature_names

try:
    from data_quality import filter_frame_for_orderbook_quality
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from data_quality import filter_frame_for_orderbook_quality

_SYMBOL_PATHS = paths_for(DEFAULT_SYMBOL)
SYMBOL = _SYMBOL_PATHS.symbol
DATA_DIR = _SYMBOL_PATHS.feature_dir
MODEL_DIR = _SYMBOL_PATHS.model_dir
RESULTS_DIR = _SYMBOL_PATHS.results_dir


def configure_symbol(symbol=None, *, model_dir_override=None):
    global SYMBOL, DATA_DIR, MODEL_DIR, RESULTS_DIR
    paths = paths_for(symbol)
    SYMBOL = paths.symbol
    DATA_DIR = paths.feature_dir
    MODEL_DIR = paths.model_dir
    RESULTS_DIR = paths.results_dir
    if model_dir_override is not None:
        MODEL_DIR = Path(model_dir_override).expanduser().resolve()

N_THREADS = max(1, int(os.environ.get("MM_LGB_THREADS", min(cpu_count(), 6))))
ACTIVE_SOURCE_PROFILE = "all"
ACTIVE_FEATURE_VARIANT = "base"
ACTIVE_EXPERIMENT_ID = None
ACTIVE_ARTIFACT_AUTHORITY = "candidate_bundle"


@dataclass(frozen=True)
class TrainOnlySelectionContract:
    schema_version: str
    spec_path: str
    spec_sha256: str
    source_authority: str
    fit_days: tuple[str, ...]
    embargo_days: tuple[str, ...]
    selection_days: tuple[str, ...]
    refit_days: tuple[str, ...]
    feature_manifest_sha256: str
    feature_dag_sha256: str
    source_manifest_sha256: str
    train_source_identity_sha256: str
    sample_weight_policy: dict | None = None
    feature_cols: tuple[str, ...] | None = None

    def to_metadata(self) -> dict:
        metadata = {
            "schema_version": self.schema_version,
            "spec_path": self.spec_path,
            "spec_sha256": self.spec_sha256,
            "source_authority": self.source_authority,
            "fit_days": list(self.fit_days),
            "embargo_days": list(self.embargo_days),
            "selection_days": list(self.selection_days),
            "refit_days": list(self.refit_days),
            "feature_manifest_sha256": self.feature_manifest_sha256,
            "feature_dag_sha256": self.feature_dag_sha256,
            "source_manifest_sha256": self.source_manifest_sha256,
            "train_source_identity_sha256": self.train_source_identity_sha256,
            "external_panel_read_during_fit": False,
        }
        if self.sample_weight_policy is not None:
            metadata["sample_weight_policy"] = dict(self.sample_weight_policy)
        if self.feature_cols is not None:
            metadata["feature_cols"] = list(self.feature_cols)
        return metadata


def _ordered_day_tuple(payload: dict, key: str) -> tuple[str, ...]:
    values = tuple(str(value) for value in payload.get(key, ()))
    if not values or values != tuple(sorted(set(values))):
        raise ValueError(f"{key} must be a non-empty sorted unique UTC-day list")
    for value in values:
        try:
            pd.Timestamp(value, tz="UTC")
        except Exception as exc:
            raise ValueError(f"invalid UTC day in {key}: {value!r}") from exc
    return values


def _validate_sample_weight_policy(payload, refit_days: tuple[str, ...]) -> dict | None:
    if payload is None:
        return None
    if not isinstance(payload, dict) or payload.get("schema_version") not in {
        "narrowgate.f03.time_half_life_daily.v1", "narrowgate.f03.time_half_life_daily.v2"
    }:
        raise ValueError("unsupported sample weight policy")
    _weight_policy_decision_offset(payload)
    half_life = payload.get("half_life_days")
    if half_life not in ("inf", 240, 120, 60) or isinstance(half_life, bool):
        raise ValueError("half_life_days must be inf, 240, 120, or 60")
    reference = pd.Timestamp(payload.get("reference_date"), tz="UTC")
    if reference != reference.normalize() or reference.strftime("%Y-%m-%d") != (
        payload.get("reference_date")
    ) or reference.strftime("%Y-%m-%d") < max(refit_days):
        raise ValueError("time-weight reference_date must be a UTC day after all training dates")
    if payload.get("date_normalization") != "effective_head_rows":
        raise ValueError("time weighting requires effective-head-row daily normalization")
    previous = payload.get("previous_time_weight")
    if not isinstance(previous, dict) or previous.get("formula") != (
        "exp(-lambda * days_ago / 30.44)"
    ):
        raise ValueError("previous_time_weight must identify the replaced time-only formula")
    lam = float(previous.get("lambda", float("nan")))
    if not np.isfinite(lam) or lam < 0:
        raise ValueError("invalid previous time-weight lambda")
    prior_reference = pd.to_datetime(previous.get("reference_date"), utc=True)
    if pd.isna(prior_reference):
        raise ValueError("previous time-weight reference_date is missing")
    end_columns = payload.get("outcome_end_columns")
    if not isinstance(end_columns, dict) or set(end_columns) != set(MODEL_SPECS):
        raise ValueError("outcome_end_columns must bind all 13 head endpoints")
    if any(
        not isinstance(column, str) or not column.startswith("label_outcome_end_")
        for column in end_columns.values()
    ):
        raise ValueError("outcome endpoint columns must use the label_outcome_end_ namespace")
    return dict(payload)


def _weight_policy_decision_offset(policy):
    """Versioned index semantics; never apply legacy bar offset to ready frames."""
    if policy.get("schema_version") == "narrowgate.f03.time_half_life_daily.v2":
        if policy.get("decision_clock") != "feature_ready_index":
            raise ValueError("v2 weighting requires explicit feature_ready_index decision clock")
        return pd.Timedelta(0)
    if policy.get("schema_version") == "narrowgate.f03.time_half_life_daily.v1":
        if policy.get("decision_clock", "legacy_left_label_plus_10s") != "legacy_left_label_plus_10s":
            raise ValueError("legacy weight schema cannot reinterpret its decision clock")
        return pd.Timedelta(seconds=10)
    raise ValueError("unsupported sample weight policy")


def _load_train_source_identity(
    feature_manifest_path: Path,
    refit_days: tuple[str, ...],
    *,
    source_authority: str = "provider_normalized_causal",
) -> dict[str, str]:
    """Bind selected source rows without inventing a provider/native authority."""

    feature_payload = json.loads(feature_manifest_path.read_text(encoding="utf-8"))
    execution_source = feature_payload.get("execution_l2_source") or {}
    source_manifest_path = Path(
        str(execution_source.get("manifest_path") or "")
    ).expanduser()
    if not source_manifest_path.is_file():
        raise ValueError("feature manifest does not bind a readable L2 source manifest")
    actual_manifest_sha256 = hashlib.sha256(source_manifest_path.read_bytes()).hexdigest()
    declared_manifest_sha256 = str(execution_source.get("manifest_sha256") or "")
    if actual_manifest_sha256 != declared_manifest_sha256:
        raise ValueError("L2 source manifest hash differs from feature manifest")

    source_payload = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    if source_authority not in {"provider_normalized_causal", "canonical_exchange_clock"}:
        raise ValueError(f"unsupported training source authority: {source_authority!r}")
    source_rows = source_payload.get(
        "source_files" if source_authority == "provider_normalized_causal" else "sources",
        (),
    )
    by_day = {}
    for row in source_rows:
        day = str(row.get("day") or "")
        if day in by_day:
            raise ValueError(f"duplicate L2 source day: {day}")
        if day:
            by_day[day] = row
    missing = sorted(set(refit_days) - set(by_day))
    if missing:
        raise ValueError(f"provider source identity is missing refit days: {missing[:5]}")
    identity_rows = []
    for day in refit_days:
        row = by_day[day]
        if source_authority == "canonical_exchange_clock":
            if (
                row.get("source_kind") != "narrowgate.daily_book.v1"
                or row.get("source_clock") != "fused_exchange"
            ):
                raise ValueError(f"refit day {day} lacks the canonical exchange-clock contract")
            files = row.get("processed_files") or []
            by_kind = {item.get("kind"): item for item in files}
            if len(by_kind) != len(files) or not {"bbo", "l2", "clock"} <= set(by_kind):
                raise ValueError(f"refit day {day} lacks unique book/clock identities")
            digests = [row.get("raw_sha256"), row.get("quality_sha256")]
            digests.extend(by_kind[kind].get("sha256") for kind in ("bbo", "l2", "clock"))
            if any(
                not isinstance(value, str) or len(value) != 64
                or any(char not in "0123456789abcdef" for char in value)
                for value in digests
            ):
                raise ValueError(f"refit day {day} has incomplete content identities")
            identity_rows.append({
                "day": day,
                "source_authority": source_authority,
                "source_kind": row["source_kind"],
                "source_clock": row["source_clock"],
                "raw_sha256": row["raw_sha256"],
                "quality_sha256": row["quality_sha256"],
                "processed_files": [by_kind[kind] for kind in ("bbo", "l2", "clock")],
            })
            continue
        authority = str(row.get("source_authority") or "")
        if authority != "provider_normalized_causal":
            raise ValueError(
                f"refit day {day} is not provider-normalized: {authority!r}"
            )
        identity_rows.append(
            {
                "day": day,
                "source_authority": authority,
                "bbo_sha256": str(row.get("bbo_sha256") or ""),
                "l2_sha256": str(row.get("l2_sha256") or ""),
            }
        )
    canonical = json.dumps(
        identity_rows,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "source_manifest_sha256": actual_manifest_sha256,
        "train_source_identity_sha256": hashlib.sha256(canonical).hexdigest(),
    }


def load_train_only_selection_contract(
    spec_path: Path,
) -> TrainOnlySelectionContract:
    """Load a frozen inner-selection contract without reading later panels."""

    path = Path(spec_path).expanduser().resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    schema = str(payload.get("schema_version") or "")
    if schema != "narrowgate_13_head_train_only_selection.v1":
        raise ValueError(f"unsupported train-only selection schema: {schema!r}")
    fit_days = _ordered_day_tuple(payload, "fit_days")
    embargo_days = _ordered_day_tuple(payload, "embargo_days")
    selection_days = _ordered_day_tuple(payload, "selection_days")
    refit_days = _ordered_day_tuple(payload, "refit_days")
    groups = (set(fit_days), set(embargo_days), set(selection_days))
    if any(groups[i] & groups[j] for i in range(3) for j in range(i + 1, 3)):
        raise ValueError("fit, embargo, and selection days must be disjoint")
    if tuple(sorted(groups[0] | groups[1] | groups[2])) != refit_days:
        raise ValueError("refit_days must equal fit + embargo + selection days")
    if not (max(fit_days) < min(embargo_days) <= max(embargo_days) < min(selection_days)):
        raise ValueError("train-only selection days are not chronological")

    identity = _feature_panel_identity()
    expected_manifest = str(payload.get("feature_manifest_sha256") or "")
    expected_dag = str(payload.get("feature_dag_sha256") or "")
    if expected_manifest != identity["feature_manifest_sha256"]:
        raise ValueError("selection spec feature manifest hash mismatch")
    if expected_dag != identity["feature_dag_sha256"]:
        raise ValueError("selection spec feature DAG hash mismatch")
    public_input = bool(identity.get("public_input_contract"))
    semantics = identity["feature_semantics_version"] if public_input else REQUIRED_FEATURE_SEMANTICS_VERSION
    dag_id = identity["feature_dag_id"] if public_input else REQUIRED_FEATURE_DAG_ID
    if int(payload.get("feature_semantics_version", 0) or 0) != semantics:
        raise ValueError("selection spec feature semantics mismatch")
    if str(payload.get("feature_dag_id") or "") != dag_id:
        raise ValueError("selection spec feature DAG id mismatch")
    if list(payload.get("head_names") or ()) != list(MODEL_SPECS):
        raise ValueError("selection spec must freeze the complete ordered 13-head list")
    implementation_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if str(payload.get("training_implementation_sha256") or "") != (
        implementation_sha256
    ):
        raise ValueError("selection spec training implementation hash mismatch")
    if str(payload.get("training_experiment_contract_sha256") or "") != (
        training_experiment_contract_sha256()
    ):
        raise ValueError("selection spec training experiment contract hash mismatch")
    manifest_train_days = tuple(identity["feature_panel_split"].get("train", ()))
    if manifest_train_days != refit_days:
        raise ValueError("selection spec refit days differ from feature train split")
    source_authority = str(payload.get("source_authority") or "")
    if source_authority not in {"provider_normalized_causal", "canonical_exchange_clock", "tardis_public_input"}:
        raise ValueError("selection spec has unsupported source authority")
    if payload.get("external_panel_read_during_fit") is not False:
        raise ValueError("selection spec must forbid external-panel reads during fit")
    source_args = {} if source_authority == "provider_normalized_causal" else {
        "source_authority": source_authority,
    }
    if source_authority == "tardis_public_input":
        if not identity.get("public_input_contract"):
            raise ValueError("public source authority requires the new public feature contract")
        from research.families.f03_causal_13_head.public_input_panel import training_source_identity
        source_identity = training_source_identity(Path(identity["feature_manifest_path"]), refit_days)
    else:
        if identity.get("public_input_contract"):
            raise ValueError("new public inputs cannot claim retired source authority")
        source_identity = _load_train_source_identity(
            Path(identity["feature_manifest_path"]), refit_days, **source_args,
        )
    for key in ("source_manifest_sha256", "train_source_identity_sha256"):
        if str(payload.get(key) or "") != source_identity[key]:
            raise ValueError(f"selection spec {key} mismatch")

    weight_policy = _validate_sample_weight_policy(
        payload.get("sample_weight_policy"), refit_days,
    )
    feature_cols = None
    if weight_policy is not None:
        feature_payload = json.loads(Path(identity["feature_manifest_path"]).read_text())
        if feature_payload.get("sample_weight") != weight_policy["previous_time_weight"]:
            raise ValueError("previous time weight does not match feature manifest")
        columns = payload.get("feature_cols")
        if (
            not isinstance(columns, list) or not columns
            or any(not isinstance(column, str) or not _is_feature_column(column) for column in columns)
            or len(columns) != len(set(columns))
        ):
            raise ValueError("weighted selection must freeze unique ordered feature_cols without label/weight helpers")
        feature_cols = tuple(columns)

    return TrainOnlySelectionContract(
        schema_version=schema,
        spec_path=str(path),
        spec_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        source_authority=source_authority,
        fit_days=fit_days,
        embargo_days=embargo_days,
        selection_days=selection_days,
        refit_days=refit_days,
        feature_manifest_sha256=expected_manifest,
        feature_dag_sha256=expected_dag,
        source_manifest_sha256=source_identity["source_manifest_sha256"],
        train_source_identity_sha256=source_identity[
            "train_source_identity_sha256"
        ],
        sample_weight_policy=weight_policy,
        feature_cols=feature_cols,
    )


def split_train_only_selection(
    frame: pd.DataFrame,
    contract: TrainOnlySelectionContract,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return inner fit, inner selection, and all-Development refit frames."""

    timestamps = pd.to_datetime(frame.index, utc=True)
    row_days = pd.Index(timestamps.strftime("%Y-%m-%d"))
    observed = tuple(sorted(set(row_days)))
    if observed != contract.refit_days:
        missing = sorted(set(contract.refit_days) - set(observed))
        extra = sorted(set(observed) - set(contract.refit_days))
        raise ValueError(
            "dataset_train day identity differs from selection contract; "
            f"missing={missing[:5]} extra={extra[:5]}"
        )
    fit_mask = row_days.isin(contract.fit_days)
    selection_mask = row_days.isin(contract.selection_days)
    refit_mask = row_days.isin(contract.refit_days)
    fit = frame.loc[fit_mask].copy()
    selection = frame.loc[selection_mask].copy()
    refit = frame.loc[refit_mask].copy()
    if fit.empty or selection.empty or len(refit) != len(frame):
        raise ValueError("train-only selection produced an empty or incomplete frame")
    if pd.Timestamp(fit.index.max()) >= pd.Timestamp(selection.index.min()):
        raise ValueError("inner fit is not strictly earlier than inner selection")
    return fit, selection, refit


def _execution_count_unit_from_panel(payload: dict) -> str:
    """Carry the actual execution-market count unit into every fitted head.

    Legacy manifests predate per-day bar provenance and retain the old packet
    unit. A manifest that does supply provenance must not hide mixed/unknown
    measurement units behind that compatibility default.
    """
    rows = (payload.get("bar_source") or {}).get("daily_files", [])
    units = {row.get("trade_count_unit", "UNKNOWN") for row in rows}
    if not rows:
        return "native_aggregate_packet"
    if len(units) != 1 or not units <= {"individual_execution", "native_aggregate_packet"}:
        raise ValueError("execution bar inputs have mixed or unknown trade count units")
    return next(iter(units))


def _reference_count_unit_from_panel(payload: dict) -> str:
    source = payload.get("reference_bar_source")
    if source is None:
        return "native_aggregate_packet"  # Legacy manifests did not bind reference counts.
    rows = source.get("daily_files", [])
    units = {row.get("trade_count_unit", "UNKNOWN") for row in rows}
    if not rows or len(units) != 1 or not units <= {"individual_execution", "native_aggregate_packet"}:
        raise ValueError("reference Bar inputs have mixed or unknown trade count units")
    if not source.get("symbol") or any(row.get("symbol") != source["symbol"] for row in rows):
        raise ValueError("reference Bar inputs have mixed or missing symbols")
    return next(iter(units))


def _feature_panel_identity() -> dict:
    public_path = DATA_DIR / "public_feature_manifest.json"
    if public_path.exists():
        from research.families.f03_causal_13_head.public_input_panel import training_identity
        return training_identity(public_path)
    manifest_path = DATA_DIR / "causal_feature_manifest.json"
    if not manifest_path.exists():
        raise RuntimeError(
            "Training requires causal_feature_manifest.json; rebuild features "
            "with the versioned causal feature pipeline first"
        )
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if int(payload.get("schema_version", 0) or 0) < 2:
        raise RuntimeError(f"Legacy feature manifest is not trainable: {manifest_path}")
    if payload.get("feature_timestamp_semantics") != "left_label_bucket_end":
        raise RuntimeError(f"Invalid feature visibility contract: {manifest_path}")
    if int(payload.get("feature_semantics_version", 0) or 0) != (
        REQUIRED_FEATURE_SEMANTICS_VERSION
    ):
        raise RuntimeError(
            f"Training requires feature semantics v{REQUIRED_FEATURE_SEMANTICS_VERSION}: "
            f"{manifest_path}"
        )
    if str(payload.get("feature_dag_id") or "") != REQUIRED_FEATURE_DAG_ID:
        raise RuntimeError(
            f"Training requires feature DAG {REQUIRED_FEATURE_DAG_ID}: {manifest_path}"
        )
    if str(payload.get("feature_dag_sha256") or "") != REQUIRED_FEATURE_DAG_SHA256:
        raise RuntimeError(
            f"Training requires the current feature DAG hash: {manifest_path}"
        )
    expected_unit_contract = absolute_price_variance_unit_contract(SYMBOL)
    if str(payload.get("symbol") or "") != expected_unit_contract["symbol"]:
        raise RuntimeError(
            f"Training feature symbol differs from configured symbol: {manifest_path}"
        )
    try:
        volatility_unit_contract = validate_variance_unit_contract(
            payload.get("volatility_unit_contract"),
            symbol=SYMBOL,
        )
    except ValueError as exc:
        raise RuntimeError(
            f"Training requires an explicit volatility unit contract: {manifest_path}"
        ) from exc
    if payload.get("label_volatility_units") != volatility_unit_contract["variance_units"]:
        raise RuntimeError(
            f"Training feature manifest has inconsistent volatility units: {manifest_path}"
        )
    calibration = payload.get("label_quote_calibration") or {}
    if (
        calibration.get("schema_version") != "narrowgate_p3_touch_calibration.v2"
        or calibration.get("model_type") != "empirical_survival"
        or not str(calibration.get("sha256", "") or "")
        or float(calibration.get("p3_delta_star", 0.0) or 0.0) <= 0.0
        or float(calibration.get("p3_kappa_eff", 0.0) or 0.0) <= 0.0
    ):
        raise RuntimeError(
            f"Training requires explicit empirical P3 calibration identity: {manifest_path}"
        )
    return {
        "feature_manifest_path": str(manifest_path),
        "execution_trade_count_unit": _execution_count_unit_from_panel(payload),
        "reference_trade_count_unit": _reference_count_unit_from_panel(payload),
        "reference_trade_symbol": (payload.get("reference_bar_source") or {}).get("symbol"),
        "reference_observation_limits": {
            "training_counts": "declared_reference_bar_source_not_reconstructed_live_messages",
            "live_f_l_counts": "observed_id_range_not_proof_of_internal_id_completeness",
            "live_child_execution_timestamps_reconstructed": False,
            "live_173_feature_message_exactness_proven": False,
            "legacy_business_baseline_requalified": False,
        },
        "feature_manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "feature_timestamp_semantics": payload["feature_timestamp_semantics"],
        "feature_bucket_ms": 10_000,
        "feature_sampling_interval_ms": 10_000,
        "feature_semantics_version": int(
            payload.get("feature_semantics_version", 0) or 0
        ),
        "feature_dag_id": str(payload.get("feature_dag_id", "") or ""),
        "feature_dag_sha256": str(
            payload.get("feature_dag_sha256", "") or ""
        ),
        "feature_cutoff_semantics": str(
            payload.get("feature_cutoff_semantics", "") or ""
        ),
        "calendar_timestamp_semantics": str(
            payload.get("calendar_timestamp_semantics", "") or ""
        ),
        "microstructure_5s_semantics": str(
            payload.get("microstructure_5s_semantics", "") or ""
        ),
        "label_semantics_version": int(
            payload.get("label_semantics_version", 0) or 0
        ),
        "label_window_semantics": str(
            payload.get("label_window_semantics", "") or ""
        ),
        "feature_daily_manifest_sha256": str(
            payload.get("daily_manifest_sha256", "") or ""
        ),
        "feature_panel_split": payload.get("split", {}),
        "feature_warmup_policy": str(payload.get("warmup_policy", "") or ""),
        "volatility_unit_contract": volatility_unit_contract,
        "feature_label_quote_calibration": calibration,
        "feature_label_quote_policy": payload.get("label_quote_policy", {}),
    }


def release_memory():
    gc.collect()
    try:
        import pyarrow as pa
        pa.default_memory_pool().release_unused()
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════════
#  Feature / label definitions
# ═══════════════════════════════════════════════════════════════════

LABEL_COLS = [
    "label_ret_10s", "label_dir_10s", "label_vol_10s",
    "label_ret_30s", "label_dir_30s", "label_vol_30s",
    "label_ret_60s", "label_dir_60s", "label_vol_60s",
    "label_tox_bid_5s", "label_tox_ask_5s",
    "label_tox_bid_10s", "label_tox_ask_10s",
]
WEIGHT_COL = "sample_weight"
TRAINING_AUXILIARY_COLS = {
    "day_id", "age", "age_d", "half_life", "half_life_days", "time_weight",
    "raw_weight", "normalized_weight",
}


def _is_feature_column(column: str) -> bool:
    return (
        not column.startswith("label_")
        and column != WEIGHT_COL
        and column not in TRAINING_AUXILIARY_COLS
        and column not in DROP_COLS
    )

# columns to exclude from features (raw OHLCV that leak or are redundant)
# depth_* and notional_* excluded: live cannot reproduce ±0.2-5% percentage buckets
DROP_COLS = {"__index_level_0__", "open", "high", "low", "vwap",
             "depth_imb_02", "depth_imb_1pct", "depth_imb_2pct", "depth_imb_5pct",
             "depth_slope_bid", "depth_slope_ask", "depth_concentration",
             "notional_imb_02",
             "depth_imb_02_d3", "depth_imb_02_d6",
             "depth_imb_02_ma6", "depth_imb_02_ma30",
             "depth_imb_divergence",
             "bid_depth_02_norm", "ask_depth_02_norm"}
DROP_COLS.update(legacy_calendar_feature_names())

SOURCE_PROFILE_PREFIXES = {
    "all": None,
    "local_only": (),
    "local_ref_perp": ("cv_ref_perp_",),
    "local_ref_spot": ("cv_ref_spot_",),
    "local_exec_spot": ("cv_exec_spot_",),
    "chain_ref_perp_ref_spot": ("cv_ref_perp_", "cv_ref_spot_"),
    "binance_all": ("cv_ref_perp_", "cv_ref_spot_", "cv_exec_spot_"),
    "local_external": ("cv_external_", "external_", "venue_agreement_", "cross_venue_"),
    "all_sources": None,
}
SOURCE_COLUMN_PREFIXES = (
    "cv_ref_perp_",
    "cv_ref_spot_",
    "cv_exec_spot_",
    "cv_external_",
    "external_",
    "venue_agreement_",
    "cross_venue_",
)

SOURCE_PROFILE_CONTRACT_SCHEMA = "narrowgate_source_profile_ablation.v1"
SOURCE_PROFILE_ABLATION_PROFILES = (
    "local_only",
    "local_ref_perp",
    "local_ref_spot",
    "local_exec_spot",
    "chain_ref_perp_ref_spot",
    "binance_all",
    "local_external",
)

# Model configs: name → (label_col, objective, metric, is_classification)
MODEL_SPECS = {}
for h in [10, 30, 60]:
    MODEL_SPECS[f"dir_{h}s"] = (
        f"label_dir_{h}s", "binary", "auc", True
    )
    MODEL_SPECS[f"ret_{h}s"] = (
        f"label_ret_{h}s", "regression", "mae", False
    )
    MODEL_SPECS[f"vol_{h}s"] = (
        f"label_vol_{h}s", "regression", "mae", False
    )

for h in [5, 10]:
    MODEL_SPECS[f"tox_bid_{h}s"] = (
        f"label_tox_bid_{h}s", "binary", "auc", True
    )
    MODEL_SPECS[f"tox_ask_{h}s"] = (
        f"label_tox_ask_{h}s", "binary", "auc", True
    )

if set(MODEL_SPECS) != set(REQUIRED_MODEL_HEADS):
    raise RuntimeError("training and runtime 13-head contracts disagree")


def training_experiment_contract() -> dict:
    profiles = {}
    for profile, prefixes in SOURCE_PROFILE_PREFIXES.items():
        profiles[profile] = {
            "included_source_prefixes": None if prefixes is None else list(prefixes),
            "formal_ablation_profile": profile in SOURCE_PROFILE_ABLATION_PROFILES,
        }
    return {
        "schema": "narrowgate_13_head_predictive_ablation.v1",
        "required_heads": list(REQUIRED_MODEL_HEADS),
        "source_profile_contract_schema": SOURCE_PROFILE_CONTRACT_SCHEMA,
        "source_profiles": profiles,
        "taker_feature_contract": feature_variant_contract(),
        "train_only_selection_options": {
            "schema_version": "narrowgate_13_head_train_only_selection.v1",
            "sample_weight_policy_schema": "narrowgate.f03.time_half_life_daily.v1",
            "half_life_days": ["inf", 240, 120, 60],
            "date_normalization": "effective_head_rows",
            "phase_normalization": "fit_and_refit_separately_mean_one",
            "selection_eval_time_weighted": False,
            "actual_outcome_endpoint_namespace": "label_outcome_end_<head>",
            "previous_time_weight": "verified_against_manifest_and_rows_then_replaced",
            "source_authorities": ["provider_normalized_causal", "canonical_exchange_clock"],
            "external_panel_reads": False,
        },
        "invariants": {
            "causal_feature_manifest_required": True,
            "complete_13_head_bundle_required": True,
            "independent_model_dir_required": True,
            "promotion_authority": "research_only",
        },
    }


def training_experiment_contract_sha256() -> str:
    payload = json.dumps(
        training_experiment_contract(),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def validate_training_request(
    *,
    source_profile: str,
    feature_variant: str,
    experiment_id: str | None,
    model_dir: Path | None,
    target: str | None,
    predict: bool,
) -> None:
    if source_profile not in SOURCE_PROFILE_PREFIXES:
        raise ValueError(f"Unknown source profile: {source_profile}")
    variant = normalize_variant(feature_variant)
    if target is not None and target not in MODEL_SPECS:
        raise ValueError(f"Unknown model target: {target}")

    is_ablation = source_profile != "all" or variant != "base"
    if predict or not is_ablation:
        return
    if model_dir is None:
        raise ValueError(
            "predictive ablations require an explicit versioned --model-dir"
        )
    if not str(experiment_id or "").strip():
        raise ValueError("predictive ablations require --experiment-id")
    if target is not None:
        raise ValueError(
            "predictive ablations must train the complete strict 13-head bundle; "
            "--target is diagnostic-only for the base profile"
        )

BACKTEST_SORT_CHOICES = [
    "selection_score",
    "inventory_adjusted_pnl",
    "pnl",
    "avg_markout",
    "sharpe",
    "pnl_per_day",
]


# ═══════════════════════════════════════════════════════════════════
#  Data loading
# ═══════════════════════════════════════════════════════════════════

def load_public_feature_panel(bundle, *, model_metadata):
    """Consume the common FeatureFrame without old filters/labels or ref zeros.

    This prepares X only. A new separately frozen label/split contract remains
    mandatory before fitting; it does not reopen historical research evidence.
    """
    from data.runtime import ConsumerBundle
    from data.observation import model_row
    source = ConsumerBundle(bundle)
    frames = tuple(source.frames())
    rows = [model_row(frame, model_metadata, decision_ns=frame.cutoff_ns) for frame in frames]
    panel = pd.DataFrame(rows, columns=model_metadata["feature_cols"],
        index=pd.Index([frame.cutoff_ns for frame in frames], name="feature_ready_ns"))
    panel.attrs.update({key: source.manifest[key] for key in
        ("input_contract_id", "observation_contract_id", "feature_contract_id")})
    panel.attrs["research_use"] = "requires_new_label_and_split_contract"
    return panel


def load_split(name, columns=None, *, preserve_calendar: bool = False):
    path = DATA_DIR / f"dataset_{name}.parquet"
    if not path.exists():
        print(f"Error: {path} not found")
        sys.exit(1)
    df = pd.read_parquet(path, columns=columns)
    if not preserve_calendar:
        df = filter_frame_for_orderbook_quality(df, SYMBOL, label=f"dataset_{name}")
    df = optimize_frame_memory(df)
    col_msg = "all cols" if columns is None else f"{len(columns)} cols"
    mem_mb = df.memory_usage(deep=True).sum() / 1e6
    print(f"  {name}: {len(df):>10,} rows  ({path.stat().st_size/1e6:.0f} MB, "
          f"{col_msg}, mem={mem_mb:.0f} MB)")
    return df


def split_schema_columns(name):
    path = DATA_DIR / f"dataset_{name}.parquet"
    if not path.exists():
        print(f"Error: {path} not found")
        sys.exit(1)
    return list(pq.ParquetFile(path).schema_arrow.names)


def default_feature_columns(name):
    return feature_columns_for_profile(name, ACTIVE_SOURCE_PROFILE)


def feature_columns_for_profile(name: str, profile: str) -> list[str]:
    cols = split_schema_columns(name)
    feature_cols = [
        c for c in cols
        if _is_feature_column(c)
    ]
    return _filter_source_profile(feature_cols, profile)


def _filter_source_profile(feature_cols: list[str], profile: str) -> list[str]:
    allowed = SOURCE_PROFILE_PREFIXES.get(profile)
    if profile not in SOURCE_PROFILE_PREFIXES:
        raise ValueError(f"Unknown source profile: {profile}")
    if allowed is None:
        return list(feature_cols)

    out = []
    for col in feature_cols:
        is_source = col.startswith(SOURCE_COLUMN_PREFIXES)
        if not is_source or col.startswith(allowed):
            out.append(col)
    return out


def optimize_frame_memory(df):
    float_cols = df.select_dtypes(include=["float64"]).columns
    for col in float_cols:
        df[col] = df[col].astype(np.float32)

    int_cols = df.select_dtypes(include=["int64"]).columns
    for col in int_cols:
        series = df[col]
        if series.empty or series.isna().any():
            continue
        cmin = series.min()
        cmax = series.max()
        if cmin >= 0:
            if cmax <= np.iinfo(np.uint8).max:
                df[col] = series.astype(np.uint8)
            elif cmax <= np.iinfo(np.uint16).max:
                df[col] = series.astype(np.uint16)
            elif cmax <= np.iinfo(np.uint32).max:
                df[col] = series.astype(np.uint32)
        elif cmin >= np.iinfo(np.int8).min and cmax <= np.iinfo(np.int8).max:
            df[col] = series.astype(np.int8)
        elif cmin >= np.iinfo(np.int16).min and cmax <= np.iinfo(np.int16).max:
            df[col] = series.astype(np.int16)
        elif cmin >= np.iinfo(np.int32).min and cmax <= np.iinfo(np.int32).max:
            df[col] = series.astype(np.int32)
    return df


def load_experiment_split(
    name: str,
    *,
    label_cols: list[str] | tuple[str, ...] = (),
    source_profile: str | None = None,
    feature_variant: str | None = None,
    expected_feature_cols: list[str] | tuple[str, ...] | None = None,
    auxiliary_cols: list[str] | tuple[str, ...] = (),
    preserve_calendar: bool = False,
) -> pd.DataFrame:
    """Load one causal split and apply the frozen predictive-ablation contract."""

    profile = source_profile or ACTIVE_SOURCE_PROFILE
    variant = normalize_variant(feature_variant or ACTIVE_FEATURE_VARIANT)
    base_feature_cols = feature_columns_for_profile(name, profile)
    columns = list(dict.fromkeys(
        base_feature_cols + list(label_cols) + list(auxiliary_cols) + [WEIGHT_COL]
    ))
    load_options = {"preserve_calendar": True} if preserve_calendar else {}
    frame = load_split(name, columns=columns, **load_options)
    frame = apply_feature_variant(frame, variant, symbol=SYMBOL)

    if expected_feature_cols is not None:
        expected = list(expected_feature_cols)
        missing = sorted(set(expected) - set(frame.columns))
        if missing:
            raise RuntimeError(
                f"{name} split cannot reproduce feature variant {variant!r}; "
                f"missing columns: {missing}"
            )
        keep = list(dict.fromkeys(expected + list(label_cols) + list(auxiliary_cols) + [WEIGHT_COL]))
        frame = frame[keep]
    return frame


def prepare_xy(df, label_col):
    """Split into X, y, w — drop labels, weight, and leak-prone cols."""
    feature_cols = [c for c in df.columns if _is_feature_column(c)]
    mask = df[label_col].notna()
    # label 的有效性来自 feature_engineer/data_quality 的 horizon mask；
    # 这里不要再补 forward/back fill，否则会把坏日或长 gap 后的未来收益混入训练。
    X = df.loc[mask, feature_cols].astype(np.float32, copy=False)
    y = df.loc[mask, label_col]
    w = df.loc[mask, WEIGHT_COL].astype(np.float32, copy=False) if WEIGHT_COL in df.columns else None
    return X, y, w, feature_cols


def prepare_time_weighted_xy(
    df: pd.DataFrame,
    name: str,
    contract: TrainOnlySelectionContract,
    *,
    phase: str,
) -> tuple[pd.DataFrame, pd.Series, pd.Series | None, list[str], dict]:
    """Purge actual outcome endpoints, then replace time weights per head/day.

    Endpoint columns are label metadata, not predictors. No outer-panel file is
    read here. An endpoint at the next excluded day's midnight is out of bounds.
    """
    policy = contract.sample_weight_policy
    if policy is None:
        raise ValueError("time-weighted preparation requires a weight policy")
    phase_days = {
        "fit": contract.fit_days,
        "selection": contract.selection_days,
        "refit": contract.refit_days,
    }[phase]
    index = pd.to_datetime(df.index, utc=True)
    if index.isna().any() or not index.is_monotonic_increasing or index.has_duplicates:
        raise ValueError("training rows require unique ordered finite UTC timestamps")
    row_days = index.strftime("%Y-%m-%d")
    if not set(row_days) <= set(phase_days):
        raise ValueError(f"{phase} contains undeclared training dates")

    previous = policy["previous_time_weight"]
    previous_ref = pd.to_datetime(previous["reference_date"], utc=True)
    previous_age = np.maximum((previous_ref - index).total_seconds() / 86400.0, 0.0)
    expected_previous = np.exp(-float(previous["lambda"]) * previous_age / 30.44)
    if WEIGHT_COL not in df or not np.allclose(
        pd.to_numeric(df[WEIGHT_COL], errors="coerce"), expected_previous,
        rtol=2e-6, atol=1e-8,
    ):
        raise ValueError("sample_weight is not the declared time-only weight; do not multiply or discard unknown weights")

    endpoint_column = policy["outcome_end_columns"][name]
    if endpoint_column not in df:
        raise ValueError(f"missing actual outcome endpoint column: {endpoint_column}")
    endpoints = df[endpoint_column]
    if pd.api.types.is_numeric_dtype(endpoints.dtype):
        raise ValueError("outcome endpoints require explicit datetime UTC units")
    ends = pd.to_datetime(endpoints, utc=True, errors="coerce")
    decisions = index + _weight_policy_decision_offset(policy)
    label_col = MODEL_SPECS[name][0]
    valid_label = df[label_col].notna().to_numpy()
    known_end = ends.notna().to_numpy()
    if np.any(valid_label & known_end & (ends.to_numpy() < decisions.to_numpy())):
        raise ValueError("label outcome ends before its decision is visible")

    # Build each allowed day's contiguous right boundary. For sampled training
    # dates these normally stop at midnight; contiguous dates remain supported.
    day_set = set(phase_days)
    boundaries = {}
    for day in reversed(phase_days):
        next_day = (pd.Timestamp(day, tz="UTC") + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        boundaries[day] = boundaries[next_day] if next_day in day_set else pd.Timestamp(next_day, tz="UTC")
    right = pd.DatetimeIndex([boundaries[day] for day in row_days])
    inside = (decisions < right) & (ends.to_numpy() < right.to_numpy())
    mask = valid_label & known_end & inside
    if contract.feature_cols is None:
        raise ValueError("time-weighted training requires frozen feature_cols")
    feature_cols = list(contract.feature_cols)
    missing_features = sorted(set(feature_cols) - set(df.columns))
    if missing_features:
        raise ValueError(f"frozen features are missing: {missing_features}")
    X = df.loc[mask, feature_cols].astype(np.float32)
    y = df.loc[mask, label_col]
    valid_days = row_days[mask]
    counts = pd.Series(valid_days).value_counts()
    if X.empty:
        raise ValueError(f"{name}/{phase} has no effective labels after outcome purge")
    reference = pd.Timestamp(policy["reference_date"], tz="UTC")
    ages = (reference - index[mask].normalize()).days.to_numpy(dtype=np.float64)
    half_life = policy["half_life_days"]
    time_weights = np.ones(len(X)) if half_life == "inf" else np.exp2(-ages / float(half_life))
    weights = time_weights / np.asarray([counts[day] for day in valid_days], dtype=np.float64)
    weights /= weights.mean()
    w = None if phase == "selection" else pd.Series(weights, index=X.index, dtype=np.float64)
    reported_weights = np.ones(len(X)) if w is None else weights
    day_rows = []
    for day in phase_days:
        select = valid_days == day
        age = int((reference - pd.Timestamp(day, tz="UTC")).days)
        day_rows.append({
            "day": day,
            "effective_rows": int(select.sum()),
            "age_days": age,
            "time_weight": 1.0 if half_life == "inf" or w is None else float(2 ** (-age / float(half_life))),
            "weight_sum": float(reported_weights[select].sum()),
        })
    total = float(reported_weights.sum())
    report = {
        "phase": phase,
        "decision_clock": policy.get("decision_clock", "legacy_left_label_plus_10s"),
        "input_rows": len(df),
        "effective_rows": len(X),
        "valid_label_rows_before_purge": int(valid_label.sum()),
        "missing_outcome_end_rows": int((valid_label & ~known_end).sum()),
        "outcome_outside_phase_days_rows": int((valid_label & known_end & ~inside).sum()),
        "effective_day_count": int(len(counts)),
        "unsupported_days": [day for day in phase_days if day not in counts],
        "first_decision_utc": decisions[mask].min().isoformat(),
        "last_decision_utc": decisions[mask].max().isoformat(),
        "last_outcome_utc": ends.loc[mask].max().isoformat(),
        "weight_mean": float(reported_weights.mean()),
        "weight_sum": total,
        "weighted_effective_rows_concentration_only": float(total ** 2 / np.square(reported_weights).sum()),
        "recent_weight_share": {
            f"last_{days}_calendar_days": float(reported_weights[ages < days].sum() / total)
            for days in (30, 60, 120)
        },
        "unweighted_early_stopping": w is None,
        "previous_weight_replaced_not_multiplied": True,
        "daily": day_rows,
    }
    return X, y, w, feature_cols, report


def drop_all_missing_training_features(
    train_df: pd.DataFrame,
    *other_frames: pd.DataFrame | None,
) -> list[str]:
    """Remove features that have no finite training observation.

    A feature that is always missing offline but finite online creates a silent
    train/live distribution mismatch.  Missing values with real support remain
    valid LightGBM inputs; only zero-support columns are removed.
    """
    candidates = [
        col
        for col in train_df.columns
        if _is_feature_column(col)
    ]
    dropped: list[str] = []
    for col in candidates:
        values = pd.to_numeric(train_df[col], errors="coerce").to_numpy(copy=False)
        if not np.isfinite(values).any():
            dropped.append(col)
    if dropped:
        train_df.drop(columns=dropped, inplace=True)
        for frame in other_frames:
            if frame is not None:
                frame.drop(columns=[c for c in dropped if c in frame], inplace=True)
    return dropped


def _infer_market_stage(feature_cols: list[str]) -> str:
    cols = set(feature_cols)
    if any(c.startswith("cv_exec_spot_") or c.startswith("cv_ref_spot_") for c in cols):
        return "enhanced"
    if any(c.startswith("cv_ref_perp_") for c in cols):
        return "minimal"
    return "single"


def _feature_schema_meta(feature_cols: list[str]) -> dict:
    reference = default_reference_symbol(SYMBOL)
    stage = _infer_market_stage(feature_cols)
    variant_meta = feature_variant_contract()["variants"][ACTIVE_FEATURE_VARIANT]
    source_prefixes = SOURCE_PROFILE_PREFIXES[ACTIVE_SOURCE_PROFILE]
    anchors = []
    if stage == "enhanced":
        anchors = [SYMBOL, reference]
    # metadata 是模型 bundle 的 ABI。live/backtest 加载模型时靠这些字段判断
    # feature stage/source profile 是否和当前配置一致，避免 enhanced/minimal 混用。
    return {
        "symbol": SYMBOL,
        "market_stage": stage,
        "reference_symbol": reference if stage in {"minimal", "enhanced"} else None,
        "spot_anchor_symbols": anchors,
        "source_profile": ACTIVE_SOURCE_PROFILE,
        "source_profile_contract_schema": SOURCE_PROFILE_CONTRACT_SCHEMA,
        "source_profile_included_prefixes": (
            None if source_prefixes is None else list(source_prefixes)
        ),
        "feature_variant": ACTIVE_FEATURE_VARIANT,
        "feature_variant_contract_schema": feature_variant_contract()["schema"],
        "feature_variant_added_cols": variant_meta["added_feature_cols"],
        "feature_variant_dropped_cols": variant_meta["dropped_feature_cols"],
        "training_experiment_id": ACTIVE_EXPERIMENT_ID,
        "predictive_ablation_contract_sha256": (
            training_experiment_contract_sha256()
        ),
        "promotion_authority": ACTIVE_ARTIFACT_AUTHORITY,
        "calendar_schema": "canonical_cal_only_v1",
        "legacy_calendar_aliases": [],
    }


# ═══════════════════════════════════════════════════════════════════
#  LightGBM training
# ═══════════════════════════════════════════════════════════════════

def get_params(objective, is_cls):
    """Default LightGBM params tuned for local research runs.

    The defaults were measured on Apple Silicon/ARM64.  Treat them as a
    training/backtest starting point; x86 Linux live or research hosts should
    set MM_LGB_THREADS / MM_LGB_HIST_POOL_MB and re-benchmark.
    """
    base = {
        "objective": objective,
        "n_jobs": N_THREADS,
        "verbose": -1,
        "seed": 42,
        "n_estimators": 2000,
        "learning_rate": 0.05,
        "num_leaves": 127,
        "max_depth": 8,
        "min_child_samples": 500,
        "subsample": 0.8,
        "subsample_freq": 1,
        "colsample_bytree": 0.8,
        "reg_alpha": 0.1,
        "reg_lambda": 1.0,
        "max_bin": 255,
        "histogram_pool_size": float(os.environ.get("MM_LGB_HIST_POOL_MB", "256")),
        "force_col_wise": True,
    }
    if is_cls:
        base["metric"] = "auc"
        base["is_unbalance"] = True
    else:
        base["metric"] = "mae"
    return base


def _training_clock_metadata(identity, frames):
    """Derive index meaning from the panel, never from its sampling cadence."""
    meaning = identity.get("feature_timestamp_semantics")
    cutoff = identity.get("feature_cutoff_semantics")
    public = identity.get("public_input_contract") or {}
    if meaning == "feature_ready_index":
        if (cutoff != meaning or public.get("decision_time_semantics") != meaning
                or "feature_bucket_ms" in identity):
            raise ValueError("conflicting feature_ready_index clock declarations")
        result = {"feature_timestamp_semantics": meaning}
    elif meaning == "left_label_bucket_end":
        if public or cutoff == "feature_ready_index":
            raise ValueError("conflicting legacy left-label clock declarations")
        result = {"feature_timestamp_semantics": meaning, "feature_bucket_ms": 10_000}
    else:
        raise ValueError("explicit panel timestamp semantics required before training")
    for frame in frames:
        if frame is not None and frame.attrs.get("decision_time_semantics", meaning) != meaning:
            raise ValueError("training frame and panel clock declarations differ")
    result["feature_sampling_interval_ms"] = 10_000
    return result


def train_one(
    name,
    train_df,
    val_df,
    params_override=None,
    *,
    verbose: bool = False,
    refit_df: pd.DataFrame | None = None,
    selection_contract: TrainOnlySelectionContract | None = None,
):
    """Train a single LightGBM model, return model + metrics."""
    label_col, objective, metric, is_cls = MODEL_SPECS[name]
    panel_identity = _feature_panel_identity()
    clock_metadata = _training_clock_metadata(panel_identity, (train_df, val_df, refit_df))
    if selection_contract is not None and selection_contract.sample_weight_policy is not None:
        offset = _weight_policy_decision_offset(selection_contract.sample_weight_policy)
        expected_offset = pd.Timedelta(seconds=(
            0 if clock_metadata["feature_timestamp_semantics"] == "feature_ready_index" else 10
        ))
        if offset != expected_offset:
            raise ValueError("weighting and panel clock declarations differ")

    weighting_reports = None
    weighted = selection_contract is not None and selection_contract.sample_weight_policy is not None
    if weighted:
        if refit_df is None:
            raise ValueError("time-weighted inner selection requires a full training refit")
        X_tr, y_tr, w_tr, feat_cols, fit_report = prepare_time_weighted_xy(
            train_df, name, selection_contract, phase="fit",
        )
        X_va, y_va, w_va, _, selection_report = prepare_time_weighted_xy(
            val_df, name, selection_contract, phase="selection",
        )
        X_full, y_full, w_full, availability_cols, refit_report = prepare_time_weighted_xy(
            refit_df, name, selection_contract, phase="refit",
        )
        X_availability = X_full
        weighting_reports = {"fit": fit_report, "selection": selection_report, "refit": refit_report}
    else:
        X_tr, y_tr, w_tr, feat_cols = prepare_xy(train_df, label_col)
        X_va, y_va, w_va, _ = prepare_xy(val_df, label_col)
        feature_availability_source = refit_df if refit_df is not None else train_df
        X_availability, _, _, availability_cols = prepare_xy(
            feature_availability_source, label_col,
        )
    if availability_cols != feat_cols:
        raise RuntimeError("selection and refit feature schemas differ")
    feature_availability = {
        col: float(
            np.isfinite(pd.to_numeric(X_availability[col], errors="coerce")).mean()
        )
        for col in feat_cols
    }

    params = get_params(objective, is_cls)
    if params_override:
        params.update(params_override)

    print(f"\n{'='*60}")
    print(f"  Training: {name}  ({objective})")
    print(f"  Train: {len(X_tr):,}  Val: {len(X_va):,}  "
          f"Features: {len(feat_cols)}")
    print(f"{'='*60}")

    callbacks = [
        lgb.log_evaluation(period=200 if verbose else 0),
        lgb.early_stopping(stopping_rounds=100, verbose=verbose),
    ]

    selection_model = (
        lgb.LGBMClassifier(**params) if is_cls else lgb.LGBMRegressor(**params)
    )

    t0 = time.perf_counter()
    fit_kw = {
        "X": X_tr, "y": y_tr,
        "sample_weight": w_tr,
        "eval_set": [(X_va, y_va)],
        "eval_sample_weight": [w_va],
        "callbacks": callbacks,
    }
    selection_model.fit(**fit_kw)
    selection_time_s = time.perf_counter() - t0

    best_iter = int(selection_model.best_iteration_ or params["n_estimators"])
    # LightGBM uses 'l1' internally for MAE, 'auc' for AUC
    metric_key = metric
    if metric not in selection_model.best_score_["valid_0"]:
        # try common aliases
        alias_map = {"mae": "l1", "mse": "l2"}
        metric_key = alias_map.get(metric, metric)
    best_score = selection_model.best_score_["valid_0"][metric_key]

    refit_time_s = 0.0
    model = selection_model
    final_training_rows = len(X_tr)
    selection_training_rows = len(X_tr)
    selection_validation_rows = len(X_va)
    if refit_df is not None:
        if weighted:
            full_cols = availability_cols
        else:
            X_full, y_full, w_full, full_cols = prepare_xy(refit_df, label_col)
        if full_cols != feat_cols:
            raise RuntimeError("full refit feature schema differs from selection schema")
        refit_params = dict(params)
        refit_params["n_estimators"] = max(1, best_iter)
        model = (
            lgb.LGBMClassifier(**refit_params)
            if is_cls
            else lgb.LGBMRegressor(**refit_params)
        )
        t_refit = time.perf_counter()
        model.fit(X_full, y_full, sample_weight=w_full)
        refit_time_s = time.perf_counter() - t_refit
        final_training_rows = len(X_full)
        del X_full, y_full, w_full
        release_memory()
    t_train = selection_time_s + refit_time_s

    print(f"\n  Best iter: {best_iter}  "
          f"{'AUC' if is_cls else 'MAE'}: {best_score:.6f}  "
          f"Time: {t_train:.1f}s")

    # ── Feature importance ──
    imp = pd.Series(model.feature_importances_, index=feat_cols)
    imp = imp.sort_values(ascending=False)
    if verbose:
        print("\n  Top-15 features:")
        for i, (f, v) in enumerate(imp.head(15).items(), 1):
            print(f"    {i:2d}. {f:<30s} {v:>6d}")

    del X_tr, y_tr, w_tr, X_va, y_va, w_va, X_availability, imp
    if model is not selection_model:
        del selection_model
    release_memory()

    metadata = {
        "name": name,
        **_feature_schema_meta(feat_cols),
        **panel_identity,
        "best_iteration": best_iter,
        f"val_{metric}": best_score,
        "train_time_s": round(t_train, 1),
        "selection_time_s": round(selection_time_s, 1),
        "refit_time_s": round(refit_time_s, 1),
        "selection_training_rows": int(selection_training_rows),
        "selection_validation_rows": int(selection_validation_rows),
        "selection_input_rows_before_masks": int(len(train_df)),
        "selection_validation_input_rows_before_masks": int(len(val_df)),
        "final_training_rows": int(final_training_rows),
        "selection_fit_params": params,
        "refit_fit_params": refit_params if refit_df is not None else None,
        "n_features": len(feat_cols),
        "feature_cols": feat_cols,
        **clock_metadata,
        "feature_availability_train": feature_availability,
        "label_semantics": (
            "fill_within_h_then_markout_h_after_fill; decision outcome spans h_to_2h"
            if label_col.startswith(("label_ret_", "label_dir_"))
            else (
                "fixed_forward_h_absolute_price_variance"
                if label_col.startswith("label_vol_")
                else "fill_within_h_then_side_adverse_markout"
            )
        ),
    }
    if label_col.startswith(("label_ret_", "label_dir_")):
        horizon_token = label_col.rsplit("_", 1)[-1]
        horizon_s = int(horizon_token.removesuffix("s"))
        metadata["outcome_identity"] = {
            "schema_version": "narrowgate.f03.fill_conditioned_outcome.v1",
            "event_type": "fill_conditioned_first_touch_then_markout",
            "fill_window_s": horizon_s,
            "markout_after_fill_s": horizon_s,
            "outcome_min_s": horizon_s,
            "outcome_max_s": 2 * horizon_s,
            "max_future_dependency_s": 2 * horizon_s,
            "price_origin": "simulated_fill_price",
            "return_unit": "fraction_of_decision_close",
            "direct_quote_center_compatible": False,
        }
    if selection_contract is not None:
        metadata["train_only_selection"] = selection_contract.to_metadata()
    if weighting_reports is not None:
        metadata["per_head_time_weighting"] = weighting_reports
        metadata["class_imbalance_adjustment"] = {
            "is_unbalance": bool(params.get("is_unbalance", False)),
            "daily_weight_sums_before_class_adjustment": True,
        }
    return model, metadata


# ═══════════════════════════════════════════════════════════════════
#  Evaluation on test set
# ═══════════════════════════════════════════════════════════════════

def evaluate_test(model, name, test_df):
    """Evaluate model on held-out test set."""
    label_col, objective, metric, is_cls = MODEL_SPECS[name]
    X_te, y_te, w_te, _ = prepare_xy(test_df, label_col)
    try:
        if is_cls:
            if hasattr(model, "predict_proba"):
                y_prob = model.predict_proba(X_te)[:, 1]
            else:
                y_prob = model.predict(X_te)
            from sklearn.metrics import roc_auc_score, accuracy_score, log_loss
            auc = roc_auc_score(y_te, y_prob, sample_weight=w_te)
            acc = accuracy_score(y_te, (y_prob > 0.5).astype(int),
                                 sample_weight=w_te)
            ll = log_loss(y_te, y_prob, sample_weight=w_te)
            print(f"  Test — AUC: {auc:.4f}  Acc: {acc:.4f}  LogLoss: {ll:.4f}")
            return {"test_auc": auc, "test_acc": acc, "test_logloss": ll}
        y_pred = model.predict(X_te)
        mae = np.average(np.abs(y_te - y_pred), weights=w_te)
        mse = np.average((y_te - y_pred) ** 2, weights=w_te)
        rmse = math.sqrt(mse)
        # IC (information coefficient = rank correlation)
        from scipy.stats import spearmanr
        ic, _ = spearmanr(y_te, y_pred)
        print(f"  Test — MAE: {mae:.6f}  RMSE: {rmse:.6f}  IC: {ic:.4f}")
        return {"test_mae": mae, "test_rmse": rmse, "test_ic": ic}
    finally:
        del X_te, y_te, w_te
        if "y_prob" in locals():
            del y_prob
        if "y_pred" in locals():
            del y_pred
        release_memory()


def evaluate_test_from_disk(
    model,
    name,
    feature_cols,
    *,
    source_profile: str | None = None,
    feature_variant: str | None = None,
):
    label_col, _, _, _ = MODEL_SPECS[name]
    test_df = load_experiment_split(
        "test",
        label_cols=[label_col],
        source_profile=source_profile,
        feature_variant=feature_variant,
        expected_feature_cols=feature_cols,
    )
    try:
        return evaluate_test(model, name, test_df)
    finally:
        del test_df
        release_memory()


# ═══════════════════════════════════════════════════════════════════
#  Save / load
# ═══════════════════════════════════════════════════════════════════

def save_model(model, name, meta):
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model_path = MODEL_DIR / f"{name}.txt"
    model.booster_.save_model(str(model_path))

    save_meta(name, meta)
    print(f"  Saved → {model_path}  ({model_path.stat().st_size/1e3:.0f} KB)")


def save_meta(name, meta):
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    meta_path = MODEL_DIR / f"{name}_meta.json"
    # convert non-serialisable types
    clean = {}
    for k, v in meta.items():
        if isinstance(v, (np.integer,)):
            clean[k] = int(v)
        elif isinstance(v, (np.floating,)):
            clean[k] = float(v)
        else:
            clean[k] = v
    with open(meta_path, "w") as f:
        json.dump(clean, f, indent=2)


def write_training_summary(
    targets: list[str],
    metrics: list[dict],
    *,
    source_profile: str,
    feature_variant: str = "base",
    experiment_id: str | None = None,
    selection_contract: TrainOnlySelectionContract | None = None,
) -> Path:
    artifacts = []
    for name in targets:
        for suffix in (".txt", "_meta.json"):
            path = MODEL_DIR / f"{name}{suffix}"
            if not path.is_file():
                raise FileNotFoundError(f"incomplete model bundle: {path}")
            artifacts.append(
                {
                    "path": path.name,
                    "size_bytes": int(path.stat().st_size),
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
            )
    bundle_meta_path = MODEL_DIR / "bundle_meta.json"
    if bundle_meta_path.is_file():
        artifacts.append(
            {
                "path": bundle_meta_path.name,
                "size_bytes": int(bundle_meta_path.stat().st_size),
                "sha256": hashlib.sha256(bundle_meta_path.read_bytes()).hexdigest(),
            }
        )
    feature_identity = _feature_panel_identity()
    if feature_identity.get("public_input_contract"):
        from research.families.f03_causal_13_head.public_input_panel import publish_model_contract
        if selection_contract is None:
            raise ValueError("public model publication requires frozen training selection")
        publish_model_contract(MODEL_DIR, feature_identity, targets,
                               selection_contract=selection_contract.to_metadata())
    payload = {
        "schema": "narrowgate_13_head_training_summary.v1",
        "symbol": SYMBOL,
        "source_profile": source_profile,
        "source_profile_contract_schema": SOURCE_PROFILE_CONTRACT_SCHEMA,
        "feature_variant": normalize_variant(feature_variant),
        "feature_variant_contract_schema": feature_variant_contract()["schema"],
        "training_experiment_id": experiment_id,
        "predictive_ablation_contract_sha256": (
            training_experiment_contract_sha256()
        ),
        "promotion_authority": ACTIVE_ARTIFACT_AUTHORITY,
        "target_count": len(targets),
        "targets": targets,
        "feature_identity": feature_identity,
        "metrics": metrics,
        "artifacts": artifacts,
    }
    if selection_contract is not None:
        payload["train_only_selection"] = selection_contract.to_metadata()
    path = MODEL_DIR / "training_summary.json"
    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            default=lambda value: value.item()
            if isinstance(value, np.generic)
            else str(value),
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def load_model(name):
    model_path = MODEL_DIR / f"{name}.txt"
    meta_path = MODEL_DIR / f"{name}_meta.json"
    if not model_path.exists():
        return None, None
    booster = lgb.Booster(model_file=str(model_path))
    with open(meta_path) as f:
        meta = json.load(f)
    return booster, meta


# ═══════════════════════════════════════════════════════════════════
#  Hyperparameter tuning (Optuna)
# ═══════════════════════════════════════════════════════════════════

def tune_one(name, train_df, val_df, n_trials=50, *, verbose: bool = False):
    """Bayesian hyperparameter search with Optuna."""
    try:
        import optuna
        optuna.logging.set_verbosity(optuna.logging.WARNING)
    except ImportError:
        print("Install optuna first: pip3 install optuna")
        return None

    label_col, objective, metric, is_cls = MODEL_SPECS[name]
    X_tr, y_tr, w_tr, feat_cols = prepare_xy(train_df, label_col)
    X_va, y_va, w_va, _ = prepare_xy(val_df, label_col)

    def objective_fn(trial):
        p = {
            "objective": objective,
            "n_jobs": N_THREADS,
            "verbose": -1,
            "seed": 42,
            "n_estimators": 3000,
            "learning_rate": trial.suggest_float("lr", 0.01, 0.15, log=True),
            "num_leaves": trial.suggest_int("num_leaves", 31, 255),
            "max_depth": trial.suggest_int("max_depth", 4, 12),
            "min_child_samples": trial.suggest_int("min_child", 100, 2000, log=True),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "subsample_freq": 1,
            "colsample_bytree": trial.suggest_float("colsample", 0.5, 1.0),
            "reg_alpha": trial.suggest_float("alpha", 1e-3, 10.0, log=True),
            "reg_lambda": trial.suggest_float("lambda", 1e-3, 10.0, log=True),
        }
        if is_cls:
            p["metric"] = "auc"
            p["is_unbalance"] = True
        else:
            p["metric"] = "mae"

        mdl = (lgb.LGBMClassifier(**p) if is_cls
               else lgb.LGBMRegressor(**p))
        mdl.fit(
            X_tr, y_tr, sample_weight=w_tr,
            eval_set=[(X_va, y_va)],
            eval_sample_weight=[[w_va] if w_va is not None else None][0],
            callbacks=[
                lgb.early_stopping(80, verbose=False),
                lgb.log_evaluation(0),
            ],
        )
        metric_key = metric
        if metric_key not in mdl.best_score_["valid_0"]:
            metric_key = {"mae": "l1", "mse": "l2"}.get(metric, metric)
        score = mdl.best_score_["valid_0"][metric_key]
        return score  # cls: auc (maximize), reg: mae (minimize)

    study = optuna.create_study(
        direction="maximize" if is_cls else "minimize"
    )
    study.optimize(objective_fn, n_trials=n_trials, show_progress_bar=verbose)

    print(f"\n  Best trial for {name}:")
    print(f"    Score: {study.best_value:.6f}")
    print(f"    Params: {study.best_params}")
    return study.best_params


# ═══════════════════════════════════════════════════════════════════
#  Backtest integration: generate predictions
# ═══════════════════════════════════════════════════════════════════

def generate_predictions(test_df, models_dict):
    """Generate a compact prediction frame for backtest integration."""
    result = pd.DataFrame(index=test_df.index)
    for name, (model, meta) in models_dict.items():
        label_col, _, _, is_cls = MODEL_SPECS[name]
        feat_cols = meta["feature_cols"]
        if is_cls:
            # Booster predict returns probabilities directly
            result[f"pred_{name}"] = model.predict(test_df[feat_cols])
        else:
            result[f"pred_{name}"] = model.predict(test_df[feat_cols])
        release_memory()
    return result


def _model_bundle_experiment_identity(models_dict) -> tuple[str, str]:
    profiles = {
        str(meta.get("source_profile") or "all")
        for _, meta in models_dict.values()
    }
    variants = {
        normalize_variant(meta.get("feature_variant") or "base")
        for _, meta in models_dict.values()
    }
    if len(profiles) != 1 or len(variants) != 1:
        raise RuntimeError(
            "model heads do not share one source-profile/feature-variant identity"
        )
    return profiles.pop(), variants.pop()


def generate_predictions_from_disk(models_dict):
    feature_cols = []
    for _, meta in models_dict.values():
        feature_cols.extend(meta["feature_cols"])
    feature_cols = list(dict.fromkeys(feature_cols))
    source_profile, feature_variant = _model_bundle_experiment_identity(models_dict)
    test_df = load_experiment_split(
        "test",
        source_profile=source_profile,
        feature_variant=feature_variant,
        expected_feature_cols=feature_cols,
    )
    try:
        return generate_predictions(test_df, models_dict)
    finally:
        del test_df
        release_memory()


def _build_backtest_base_params(live_params, p3_delta_star=0.0, p3_kappa_eff=0.0):
    return build_backtest_base_params(
        live_params,
        p3_delta_star=p3_delta_star,
        p3_kappa_eff=p3_kappa_eff,
    )


def _clean_value(value):
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    return value


def _select_backtest_metrics(result):
    fields = [
        "selection_score",
        "pnl",
        "inventory_adjusted_pnl",
        "inventory_pnl",
        "inventory_cost",
        "avg_markout",
        "avg_markout_bid",
        "avg_markout_ask",
        "markout_count",
        "sharpe",
        "pnl_per_day",
        "fills_per_day",
        "avg_inventory",
        "vol_blend",
        "skew",
        "asym",
        "gdir",
        "ret_skew",
        "markout_ema_span_fills",
        "markout_spread_scale",
        "gamma",
        "kappa",
    ]
    return {field: _clean_value(result.get(field)) for field in fields if field in result}


def _is_ml_enabled_result(result):
    return any(
        result.get(field, 0.0) > 0
        for field in ("vol_blend", "skew", "asym", "gdir", "ret_skew")
    )


def evaluate_bundle_backtest(pred_df, config_path=None,
                             sort_by="selection_score",
                             sweep=False):
    required = {"pred_dir_10s", "pred_vol_10s", "pred_ret_10s"}
    if not required.issubset(pred_df.columns):
        missing = sorted(required - set(pred_df.columns))
        print(f"\nSkipping bundle backtest: missing columns {missing}")
        return None

    try:
        from models import backtest_ml as bt
        from models.backtest_config import load_live_config_as_params
    except ImportError:
        import backtest_ml as bt
        from backtest_config import load_live_config_as_params

    bt.configure_symbol(SYMBOL)
    cfg = Path(config_path) if config_path else ROOT / "live" / "config.yaml"
    live_params = load_live_config_as_params(cfg)

    print("\nRunning bundle backtest on test predictions …")
    bars = bt.load_test_bars()
    ts, hi, lo, cl, ssq, pred_dir, pred_vol, pred_ret, book_imb, trade_intensity, depth_near = \
        bt.build_ml_arrays(bars, pred_df)
    tox_horizon = int(live_params.get("toxicity_horizon_s", 10))
    tox_bid, tox_ask = bt.build_toxicity_arrays(
        ts, pred_df, pred_dir=pred_dir, toxicity_horizon_s=tox_horizon,
    )
    del bars

    p3_delta_star = 0.0
    p3_kappa_eff = 0.0
    try:
        try:
            from research.families.f02_empirical_p3_touch.fill_probability import FillProbabilityModel
        except ImportError:
            from fill_probability import FillProbabilityModel
        fp_path = MODEL_DIR / "fill_prob_params.json"
        if fp_path.exists():
            fp_model = FillProbabilityModel.load(fp_path)
            p3_delta_star = fp_model.optimal_delta()
            p3_kappa_eff = fp_model.effective_kappa()
    except Exception as exc:
        print(f"  P3 model not loaded for bundle backtest: {exc}")

    base = _build_backtest_base_params(
        live_params,
        p3_delta_star=p3_delta_star,
        p3_kappa_eff=p3_kappa_eff,
    )

    current = bt.simulate_ml(
        ts, hi, lo, cl, ssq, pred_dir, pred_vol, pred_ret, base,
        book_imb=book_imb, trade_intensity=trade_intensity, depth_near=depth_near,
        tox_bid=tox_bid, tox_ask=tox_ask,
    )
    current["result_source"] = "current_live"
    print(f"  Live-config backtest: PnL=${current['pnl']:.2f}  "
          f"InvAdj=${current.get('inventory_adjusted_pnl', 0.0):.2f}  "
          f"AvgMarkout={current.get('avg_markout', 0.0):.2f}  "
          f"Sharpe={current['sharpe']:.2f}")

    summary = {
        "symbol": SYMBOL,
        "config_path": str(cfg),
        "sort_by": sort_by,
        "current": _select_backtest_metrics(current),
    }

    if sweep:
        results = bt.run_sweep(
            ts, hi, lo, cl, ssq, pred_dir, pred_vol, pred_ret,
            book_imb, base, trade_intensity=trade_intensity,
            grid=bt.SWEEP_GRID_LIVE, depth_near=depth_near, sort_by=sort_by,
            tox_bid=tox_bid, tox_ask=tox_ask,
        )
        combined = [dict(r, result_source="sweep") for r in results]
        combined.append(dict(current))
        ranked = bt._sort_results(bt._attach_selection_scores(combined), sort_by)

        current_rank = None
        current_ranked = None
        for idx, result in enumerate(ranked, 1):
            if result.get("result_source") == "current_live":
                current_rank = idx
                current_ranked = result
                break

        if current_ranked is not None and current_rank is not None:
            summary["current"] = _select_backtest_metrics(current_ranked)
            summary["current_rank"] = current_rank
            summary["ranked_pool_size"] = len(ranked)
            print(f"  Current live config rank ({sort_by}): {current_rank}/{len(ranked)}  "
                  f"Selection={current_ranked.get('selection_score', 0.0):.2f}")

        overall_best = bt._pick_robust_best(ranked, sort_by=sort_by, top_k=min(5, len(ranked)))
        if overall_best is not None:
            summary["best_overall"] = _select_backtest_metrics(overall_best)
            summary["best_overall_source"] = overall_best.get("result_source", "sweep")
            print(f"  Combined best ({sort_by}): source={summary['best_overall_source']}  "
                  f"PnL=${overall_best['pnl']:.2f}  "
                  f"InvAdj=${overall_best.get('inventory_adjusted_pnl', 0.0):.2f}  "
                  f"AvgMarkout={overall_best.get('avg_markout', 0.0):.2f}  "
                  f"Selection={overall_best.get('selection_score', 0.0):.2f}")

        sweep_ranked = [result for result in ranked if result.get("result_source") == "sweep"]
        sweep_best = bt._pick_robust_best(
            sweep_ranked,
            sort_by=sort_by,
            top_k=min(5, len(sweep_ranked)),
        ) if sweep_ranked else None
        if sweep_best is not None:
            summary["sweep_best"] = _select_backtest_metrics(sweep_best)
            print(f"  Sweep best ({sort_by}): PnL=${sweep_best['pnl']:.2f}  "
                  f"InvAdj=${sweep_best.get('inventory_adjusted_pnl', 0.0):.2f}  "
                  f"AvgMarkout={sweep_best.get('avg_markout', 0.0):.2f}  "
                  f"Selection={sweep_best.get('selection_score', 0.0):.2f}")

        baseline_ranked = [result for result in ranked if not _is_ml_enabled_result(result)]
        ml_ranked = [result for result in ranked if _is_ml_enabled_result(result)]
        best_baseline = bt._best_result(baseline_ranked, sort_by=sort_by)
        best_ml = bt._best_result(ml_ranked, sort_by=sort_by)
        if best_baseline is not None:
            summary["best_baseline"] = _select_backtest_metrics(best_baseline)
            summary["best_baseline_source"] = best_baseline.get("result_source", "sweep")
        if best_ml is not None:
            summary["best_ml"] = _select_backtest_metrics(best_ml)
            summary["best_ml_source"] = best_ml.get("result_source", "sweep")
        if best_baseline is not None and best_ml is not None:
            ml_edge = {
                "selection_score": _clean_value(best_ml.get("selection_score", 0.0) - best_baseline.get("selection_score", 0.0)),
                "pnl": _clean_value(best_ml.get("pnl", 0.0) - best_baseline.get("pnl", 0.0)),
                "inventory_adjusted_pnl": _clean_value(best_ml.get("inventory_adjusted_pnl", 0.0) - best_baseline.get("inventory_adjusted_pnl", 0.0)),
                "avg_markout": _clean_value(best_ml.get("avg_markout", 0.0) - best_baseline.get("avg_markout", 0.0)),
            }
            summary["ml_edge_vs_baseline"] = ml_edge
            print(f"  Best ML vs best baseline: ΔSelection={ml_edge['selection_score']:.2f}  "
                  f"ΔPnL=${ml_edge['pnl']:.2f}  "
                  f"ΔInvAdj=${ml_edge['inventory_adjusted_pnl']:.2f}  "
                  f"ΔAvgMarkout={ml_edge['avg_markout']:.2f}")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = RESULTS_DIR / "bundle_backtest_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"  Bundle backtest summary saved → {summary_path}")
    return summary


# ═══════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="LightGBM ML models for MM")
    ap.add_argument("--data-bundle", type=Path, help="Validate common FeatureFrame input without fitting")
    ap.add_argument("--data-model-contract", type=Path, help="Explicit common input feature schema/missing policy")
    ap.add_argument("--symbol", default=DEFAULT_SYMBOL,
                    help=f"Symbol (default {DEFAULT_SYMBOL}; MM_SYMBOL also supported)")
    ap.add_argument(
        "--feature-dir",
        type=Path,
        default=None,
        help="versioned feature/dataset directory; defaults to the symbol canonical path",
    )
    ap.add_argument(
        "--model-dir",
        type=Path,
        default=None,
        help="versioned model output directory; never overwrite a promoted bundle implicitly",
    )
    ap.add_argument("--target", default=None,
                    help="Train single target (e.g. dir_10s, ret_30s, vol_60s)")
    ap.add_argument("--tune", action="store_true",
                    help="Optuna hyperparameter tuning before training")
    ap.add_argument("--n-trials", type=int, default=50)
    ap.add_argument("--predict", action="store_true",
                    help="Only generate predictions (load saved models)")
    ap.add_argument("--config", default=str(ROOT / "live" / "config.yaml"),
                    help="Live config used for bundle backtest evaluation")
    ap.add_argument("--sort-by", choices=BACKTEST_SORT_CHOICES,
                    default="selection_score",
                    help="Trading metric used for backtest ranking")
    ap.add_argument("--bundle-backtest", action="store_true",
                    help="Run live-config bundle backtest inside this process")
    ap.add_argument("--backtest-sweep", action="store_true",
                    help="Run backtest_ml live-grid sweep after generating predictions")
    ap.add_argument("--verbose", action="store_true",
                    help="Print LightGBM eval logs, Optuna progress bars, and feature importances")
    ap.add_argument(
        "--source-profile",
        choices=sorted(SOURCE_PROFILE_PREFIXES),
        default="all",
        help="Feature-source ablation profile. Default keeps the existing full schema.",
    )
    ap.add_argument(
        "--feature-variant",
        choices=available_variants(),
        default="base",
        help="Versioned taker/depth feature ablation applied causally to every split.",
    )
    ap.add_argument(
        "--experiment-id",
        default=None,
        help="Required identity for non-base source or feature ablations.",
    )
    ap.add_argument(
        "--train-only-selection-spec",
        type=Path,
        default=None,
        help=(
            "frozen inner chronological selection contract; selects best "
            "iteration inside dataset_train, refits all train days, and never "
            "reads dataset_val/dataset_test during fitting"
        ),
    )
    ap.add_argument(
        "--print-experiment-contract",
        action="store_true",
        help="Print the versioned source-profile/taker-feature contract and exit.",
    )
    args = ap.parse_args()
    if args.data_bundle is not None or args.data_model_contract is not None:
        if args.data_bundle is None or args.data_model_contract is None:
            ap.error("common-input check requires --data-bundle and --data-model-contract")
        panel = load_public_feature_panel(args.data_bundle, model_metadata=json.loads(args.data_model_contract.read_text()))
        print(json.dumps({"rows": len(panel), "features": len(panel.columns), "training": "not_run", **panel.attrs}))
        return
    if args.print_experiment_contract:
        print(json.dumps(training_experiment_contract(), indent=2, sort_keys=True))
        return

    global ACTIVE_SOURCE_PROFILE, ACTIVE_FEATURE_VARIANT, ACTIVE_EXPERIMENT_ID
    global ACTIVE_ARTIFACT_AUTHORITY
    ACTIVE_SOURCE_PROFILE = args.source_profile
    ACTIVE_FEATURE_VARIANT = normalize_variant(args.feature_variant)
    ACTIVE_EXPERIMENT_ID = args.experiment_id
    ACTIVE_ARTIFACT_AUTHORITY = (
        "research_only"
        if args.train_only_selection_spec is not None
        or ACTIVE_SOURCE_PROFILE != "all"
        or ACTIVE_FEATURE_VARIANT != "base"
        else "candidate_bundle"
    )
    validate_training_request(
        source_profile=ACTIVE_SOURCE_PROFILE,
        feature_variant=ACTIVE_FEATURE_VARIANT,
        experiment_id=ACTIVE_EXPERIMENT_ID,
        model_dir=args.model_dir,
        target=args.target,
        predict=args.predict,
    )
    configure_symbol(args.symbol, model_dir_override=args.model_dir)
    if args.feature_dir is not None:
        globals()["DATA_DIR"] = args.feature_dir.expanduser().resolve()

    import platform
    ncpu = cpu_count()
    print(f"\n{'='*60}")
    print(f"  {SYMBOL} LightGBM Model Training")
    print(f"  Chip: {platform.processor() or platform.machine() or 'unknown'}  "
          f"Cores: {ncpu}  LightGBM: {lgb.__version__}")
    print(f"{'='*60}\n")
    print(f"  Source profile: {ACTIVE_SOURCE_PROFILE}")
    print(f"  Feature variant: {ACTIVE_FEATURE_VARIANT}")
    if ACTIVE_EXPERIMENT_ID:
        print(f"  Experiment ID: {ACTIVE_EXPERIMENT_ID}")

    targets = [args.target] if args.target else list(MODEL_SPECS.keys())

    selection_contract = None
    if args.train_only_selection_spec is not None:
        if args.predict:
            raise ValueError("--train-only-selection-spec is training-only")
        if args.model_dir is None or not str(args.experiment_id or "").strip():
            raise ValueError(
                "train-only selection requires --model-dir and --experiment-id"
            )
        if args.target is not None:
            raise ValueError(
                "train-only selection must retrain the complete 13-head bundle"
            )
        selection_contract = load_train_only_selection_contract(
            args.train_only_selection_spec
        )
        if args.tune:
            raise ValueError(
                "this train-only selection identity freezes hyperparameter tuning off"
            )

    if args.predict:
        # ── Prediction-only mode ──
        models_dict = {}
        for name in targets:
            booster, meta = load_model(name)
            if booster is None:
                print(f"  {name}: no saved model, skipping")
                continue
            models_dict[name] = (booster, meta)
        if models_dict:
            print("Loading test features for compact predictions …")
            pred_df = generate_predictions_from_disk(models_dict)
            out = RESULTS_DIR / "test_predictions.parquet"
            RESULTS_DIR.mkdir(parents=True, exist_ok=True)
            pred_df.to_parquet(out)
            print(f"\nPredictions saved → {out}  "
                  f"({out.stat().st_size/1e6:.1f} MB)")
            if args.bundle_backtest or args.backtest_sweep:
                evaluate_bundle_backtest(
                    pred_df,
                    config_path=args.config,
                    sort_by=args.sort_by,
                    sweep=args.backtest_sweep,
                )
            else:
                print("Skipping inline bundle backtest; run models/backtest_ml.py separately.")
            del pred_df
            release_memory()
        return

    # ── Load data ──
    print(
        "Loading train-only dataset …"
        if selection_contract is not None
        else "Loading train/val datasets …"
    )
    label_cols_needed = [MODEL_SPECS[name][0] for name in targets]
    weighted_selection = (
        selection_contract is not None and selection_contract.sample_weight_policy is not None
    )
    auxiliary_cols = list(selection_contract.sample_weight_policy["outcome_end_columns"].values()) if weighted_selection else []
    train_df = load_experiment_split(
        "train", label_cols=label_cols_needed, auxiliary_cols=auxiliary_cols,
        preserve_calendar=weighted_selection,
        expected_feature_cols=selection_contract.feature_cols if weighted_selection else None,
    )
    if selection_contract is not None:
        selection_train_df, val_df, train_df = split_train_only_selection(
            train_df,
            selection_contract,
        )
    else:
        selection_train_df = train_df
        val_df = load_experiment_split("val", label_cols=label_cols_needed)
    test_df = None
    # The weighted experiment compares only time decay; a zero-support column
    # is reported in per-head availability, not silently removed from its ABI.
    dropped = [] if weighted_selection else drop_all_missing_training_features(train_df, val_df, test_df)
    if selection_contract is not None and dropped:
        selection_train_df.drop(
            columns=[column for column in dropped if column in selection_train_df],
            inplace=True,
        )
    if dropped:
        print(
            "  Dropped zero-support training features: "
            + ", ".join(dropped)
        )
    print()

    all_metrics = []
    pending_test_eval = []
    t_total = time.perf_counter()
    for name in targets:
        params_override = None
        if args.tune:
            print(f"\n--- Tuning {name} ({args.n_trials} trials) ---")
            best_params = tune_one(
                name,
                selection_train_df,
                val_df,
                args.n_trials,
                verbose=args.verbose,
            )
            if best_params:
                params_override = {
                    "learning_rate": best_params["lr"],
                    "num_leaves": best_params["num_leaves"],
                    "max_depth": best_params["max_depth"],
                    "min_child_samples": best_params["min_child"],
                    "subsample": best_params["subsample"],
                    "colsample_bytree": best_params["colsample"],
                    "reg_alpha": best_params["alpha"],
                    "reg_lambda": best_params["lambda"],
                    "n_estimators": 3000,
                }

        model, meta = train_one(
            name,
            selection_train_df,
            val_df,
            params_override,
            verbose=args.verbose,
            refit_df=train_df if selection_contract is not None else None,
            selection_contract=selection_contract,
        )
        all_metrics.append(meta)
        save_model(model, name, meta)
        pending_test_eval.append(meta)
        del model
        release_memory()

    t_all = time.perf_counter() - t_total

    del train_df, val_df, selection_train_df
    release_memory()

    if pending_test_eval and selection_contract is None:
        print("\nEvaluating test set with train/val released …")
        for meta in pending_test_eval:
            booster, _ = load_model(meta["name"])
            if booster is None:
                continue
            test_metrics = evaluate_test_from_disk(
                booster,
                meta["name"],
                meta["feature_cols"],
                source_profile=meta.get("source_profile"),
                feature_variant=meta.get("feature_variant"),
            )
            meta.update(test_metrics)
            save_meta(meta["name"], meta)
            del booster
            release_memory()

    write_bundle_meta(
        MODEL_DIR,
        ACTIVE_FEATURE_VARIANT,
        extra={
            "schema": "narrowgate_13_head_predictive_ablation_bundle.v1",
            "symbol": SYMBOL,
            "source_profile": ACTIVE_SOURCE_PROFILE,
            "source_profile_contract_schema": SOURCE_PROFILE_CONTRACT_SCHEMA,
            "training_experiment_id": ACTIVE_EXPERIMENT_ID,
            "predictive_ablation_contract_sha256": (
                training_experiment_contract_sha256()
            ),
            "targets": targets,
            "promotion_authority": (
                ACTIVE_ARTIFACT_AUTHORITY
            ),
        },
    )
    training_summary_path = write_training_summary(
        targets,
        all_metrics,
        source_profile=ACTIVE_SOURCE_PROFILE,
        feature_variant=ACTIVE_FEATURE_VARIANT,
        experiment_id=ACTIVE_EXPERIMENT_ID,
        selection_contract=selection_contract,
    )
    print(f"Training identity saved -> {training_summary_path}")

    # ── Summary ──
    print(f"\n{'='*80}")
    print(f"  Training Summary  ({t_all:.1f}s total, {ncpu} cores)")
    print(f"{'='*80}")
    print(f"{'Model':<12s}  {'Iters':>5s}  {'Features':>8s}  {'Val':>12s}  "
          f"{'Test':>12s}  {'Time':>6s}")
    print(f"{'-'*80}")
    for m in all_metrics:
        name = m["name"]
        _, _, metric, is_cls = MODEL_SPECS[name]
        val_s = f"{m.get(f'val_{metric}', 0):.4f}"
        if selection_contract is not None:
            test_s = "NOT_READ"
        elif is_cls:
            test_s = f"AUC={m.get('test_auc', 0):.4f}"
        else:
            test_s = f"IC={m.get('test_ic', 0):.4f}"
        print(f"  {name:<12s}  {m['best_iteration']:>5d}  "
              f"{m.get('n_features', 0):>8d}  "
              f"{val_s:>12s}  {test_s:>12s}  "
              f"{m['train_time_s']:>5.1f}s")
    print(f"{'='*80}")
    print("  Predictive metrics are diagnostic only; bundle selection should use backtest metrics.")

    # ── Generate test predictions for backtest ──
    if selection_contract is not None:
        print(
            "\nSkipping later-panel prediction generation; "
            "outer-panel evaluation is a separate explicitly selected operation."
        )
        return

    print("\nGenerating test-set predictions for backtest …")

    models_dict = {}
    for name in targets:
        booster, meta = load_model(name)
        if booster is not None:
            models_dict[name] = (booster, meta)
    if models_dict:
        if test_df is not None:
            pred_df = generate_predictions(test_df, models_dict)
            del test_df
            release_memory()
        else:
            pred_df = generate_predictions_from_disk(models_dict)
        out = RESULTS_DIR / "test_predictions.parquet"
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        pred_df.to_parquet(out)
        print(f"Predictions saved → {out}  "
              f"({out.stat().st_size/1e6:.1f} MB)")
        if args.bundle_backtest or args.backtest_sweep:
            evaluate_bundle_backtest(
                pred_df,
                config_path=args.config,
                sort_by=args.sort_by,
                sweep=args.backtest_sweep,
            )
        else:
            print("Skipping inline bundle backtest; run models/backtest_ml.py separately.")
        del pred_df
        release_memory()
    print()


if __name__ == "__main__":
    main()
