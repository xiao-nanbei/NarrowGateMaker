#!/usr/bin/env python3
"""Train research-only BTCUSDC models with external venue history.

Two model families are intentionally kept separate:

* ``10s`` is retained for historical reproduction of the former feature/model
  cadence.
* ``fast1s`` builds a compact one-second state from local/Binance trades and
  Bitget/Bybit/OKX spot/perpetual state. Target horizons come from an explicit
  dense experiment grid; the trainer estimates the M1-minus-M0 information
  decay curve and selects a horizon on Development only.

Neither family is wired into live trading by this script.  Every artifact is
marked research-only because historical trade-time archives do not reproduce
the live receive-time feature ABI.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import os
import re
import sys
import time
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from scipy.stats import spearmanr
from sklearn.metrics import brier_score_loss, mean_absolute_error, roc_auc_score

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root  # noqa: E402
from research.families.f04_external_market_alpha.external_venue_features import (  # noqa: E402
    FAST_LABEL_CLOSE_COLUMN,
    FEATURE_SCHEMA_VERSION,
    build_fast_feature_day,
    enrich_10s_feature_day,
    normalize_fast_target_horizons,
)
from research.families.f03_causal_13_head import ml_model as production_ml  # noqa: E402

MODEL_SCHEMA_VERSION = "external_venue_model.v3"
DEFAULT_MANIFEST = (
    ROOT
    / "logs"
    / "data_audit"
    / "cleanup_20260705_align_btcusdt_reference"
    / "minimal_complete_good_days_2026.csv"
)
DEFAULT_LATE_MANIFEST = ROOT / "logs" / "data_audit" / "extended_good_days_through_20260706.csv"
TARGET_SPECS_10S = {
    "dir_10s": ("label_dir_10s", "binary"),
    "ret_10s": ("label_ret_10s", "regression"),
    "ret_30s": ("label_ret_30s", "regression"),
    "ret_60s": ("label_ret_60s", "regression"),
    "vol_10s": ("label_vol_10s", "regression"),
    "tox_bid_10s": ("label_tox_bid_10s", "binary"),
    "tox_ask_10s": ("label_tox_ask_10s", "binary"),
}
FAST_TARGET_KINDS = ("dir", "move")
_FAST_TARGET_PATTERN = re.compile(r"^(dir|move)_(\d+)s$")
_FAST_LABEL_PATTERN = re.compile(r"^label_fast_(ret|dir|move)_(\d+)s$")
PROFILES = ("m0_local_binance", "m1_external_all")


@dataclass(frozen=True)
class ChronologicalSplit:
    train: tuple[str, ...]
    embargo_1: tuple[str, ...]
    validation: tuple[str, ...]
    embargo_2: tuple[str, ...]
    test: tuple[str, ...]
    late: tuple[str, ...]


@dataclass(frozen=True)
class DevelopmentTrainingSplit:
    fit: tuple[str, ...]
    embargo: tuple[str, ...]
    early_stop: tuple[str, ...]


def fast_target_specs(
    horizons_s: Iterable[int],
    *,
    kinds: Iterable[str] = ("dir",),
) -> dict[str, tuple[str, str]]:
    """Build target identities from an explicit dense horizon grid."""

    horizons = normalize_fast_target_horizons(horizons_s)
    selected_kinds = tuple(dict.fromkeys(str(kind) for kind in kinds))
    if not selected_kinds or any(kind not in FAST_TARGET_KINDS for kind in selected_kinds):
        raise ValueError(f"fast target kinds must be drawn from {FAST_TARGET_KINDS}")
    return {
        f"{kind}_{horizon}s": (f"label_fast_{kind}_{horizon}s", "binary")
        for kind in selected_kinds
        for horizon in horizons
    }


def target_spec(target: str, *, cadence: str | None = None) -> tuple[str, str]:
    if cadence != "fast1s" and target in TARGET_SPECS_10S:
        return TARGET_SPECS_10S[target]
    match = _FAST_TARGET_PATTERN.fullmatch(str(target))
    if match is None:
        raise ValueError(f"unsupported external target: {target}")
    if cadence not in (None, "fast1s"):
        raise ValueError(f"{target} is not a {cadence} target")
    kind, horizon = match.groups()
    return f"label_fast_{kind}_{int(horizon)}s", "binary"


def load_days(path: Path) -> list[str]:
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"empty manifest: {path}")
    column = "day" if "day" in frame else frame.columns[0]
    days = sorted(set(frame[column].astype(str)))
    invalid = [day for day in days if len(day) != 10 or day[4] != "-" or day[7] != "-"]
    if invalid:
        raise ValueError(f"invalid days in {path}: {invalid[:3]}")
    return days


def chronological_split(
    days: list[str],
    *,
    train_days: int,
    validation_days: int,
    test_days: int,
    embargo_days: int,
    late_days: Iterable[str] = (),
) -> ChronologicalSplit:
    required = train_days + validation_days + test_days + 2 * embargo_days
    if len(days) != required:
        raise ValueError(
            f"declared split consumes {required} days but manifest has {len(days)}; "
            "set explicit split sizes"
        )
    cursor = 0
    train = tuple(days[cursor : cursor + train_days])
    cursor += train_days
    embargo_1 = tuple(days[cursor : cursor + embargo_days])
    cursor += embargo_days
    validation = tuple(days[cursor : cursor + validation_days])
    cursor += validation_days
    embargo_2 = tuple(days[cursor : cursor + embargo_days])
    cursor += embargo_days
    test = tuple(days[cursor : cursor + test_days])
    late = tuple(sorted(day for day in set(late_days) if day > days[-1]))
    return ChronologicalSplit(train, embargo_1, validation, embargo_2, test, late)


def development_training_split(
    train_days: Iterable[str],
    *,
    early_stop_days: int,
    embargo_days: int,
) -> DevelopmentTrainingSplit:
    """Reserve a past-only tail of Development for early stopping.

    The outer Validation panel must never tune LightGBM iteration count.
    """

    days = tuple(str(day) for day in train_days)
    stop_count = int(early_stop_days)
    gap_count = int(embargo_days)
    if stop_count <= 0 or gap_count < 0:
        raise ValueError("early-stop days must be positive and embargo nonnegative")
    fit_count = len(days) - stop_count - gap_count
    if fit_count < 2:
        raise ValueError("Development train panel is too short for inner early stopping")
    return DevelopmentTrainingSplit(
        fit=days[:fit_count],
        embargo=days[fit_count : fit_count + gap_count],
        early_stop=days[fit_count + gap_count :],
    )


def _cache_dir(data_dir: Path, cadence: str) -> Path:
    return Path(data_dir) / "model_features" / FEATURE_SCHEMA_VERSION / cadence


def _cache_path(data_dir: Path, cadence: str, day: str) -> Path:
    return _cache_dir(data_dir, cadence) / f"BTCUSDC-{cadence}-{day}.parquet"


def _build_cache_day(data_dir: str, cadence: str, day: str, force: bool) -> dict[str, Any]:
    root = Path(data_dir)
    output = _cache_path(root, cadence, day)
    if output.exists() and not force:
        return {
            "day": day,
            "path": str(output),
            "status": "cached",
            "rows": pq.ParquetFile(output).metadata.num_rows,
        }
    if cadence == "10s":
        base_path = root / "features_btcusdc" / f"features_{day}.parquet"
        if not base_path.exists():
            raise FileNotFoundError(base_path)
        frame = enrich_10s_feature_day(pd.read_parquet(base_path), root, day)
    elif cadence == "fast1s":
        frame = build_fast_feature_day(root, day)
    else:
        raise ValueError(f"unsupported cadence: {cadence}")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".parquet.tmp")
    frame.to_parquet(temporary, engine="pyarrow", compression="zstd")
    os.replace(temporary, output)
    return {"day": day, "path": str(output), "status": "built", "rows": len(frame)}


def build_cache(
    data_dir: Path,
    cadence: str,
    days: list[str],
    *,
    workers: int,
    force: bool,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    pending: list[str] = []
    if not force:
        for day in days:
            path = _cache_path(data_dir, cadence, day)
            if path.exists():
                results.append(
                    {
                        "day": day,
                        "path": str(path),
                        "status": "cached",
                        "rows": pq.ParquetFile(path).metadata.num_rows,
                    }
                )
            else:
                pending.append(day)
    else:
        pending = list(days)

    if not pending:
        print(f"{cadence}: {len(results)} cache hits; no worker pool started", flush=True)
        return sorted(results, key=lambda item: item["day"])

    with ProcessPoolExecutor(max_workers=max(1, int(workers))) as pool:
        futures = {
            pool.submit(_build_cache_day, str(data_dir), cadence, day, force): day
            for day in pending
        }
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(
                f"[{len(results):>3}/{len(days)}] {cadence} {result['day']} "
                f"{result['status']} rows={result['rows']:,}",
                flush=True,
            )
    return sorted(results, key=lambda item: item["day"])


def _schema(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def feature_columns(data_dir: Path, cadence: str, profile: str, sample_day: str) -> list[str]:
    columns = _schema(_cache_path(data_dir, cadence, sample_day))
    labels = {label for label, _ in TARGET_SPECS_10S.values()}
    if cadence == "10s":
        features = [
            name
            for name in columns
            if name not in labels
            and name != production_ml.WEIGHT_COL
            and name not in production_ml.DROP_COLS
            and name not in {"day", "__index_level_0__"}
        ]
    else:
        features = [
            name
            for name in columns
            if name.startswith(("fast_local_", "fast_binance_ref_perp_", "cv_external_"))
        ]
    if profile == "m0_local_binance":
        features = [name for name in features if not name.startswith("cv_external_")]
    elif profile != "m1_external_all":
        raise ValueError(f"unknown profile: {profile}")
    return features


def _apply_external_delay(frame: pd.DataFrame, delay_s: int) -> pd.DataFrame:
    """Delay historical external state without fragmenting the local frame."""
    if delay_s <= 0:
        return frame
    external_columns = [name for name in frame if name.startswith("cv_external_")]
    if not external_columns:
        return frame
    delayed = frame.loc[:, external_columns].shift(delay_s)
    age_columns = [name for name in external_columns if name.endswith("_source_age_ms")]
    if age_columns:
        delayed.loc[:, age_columns] = delayed.loc[:, age_columns] + delay_s * 1000.0
        delayed.loc[:, age_columns] = delayed.loc[:, age_columns].fillna(10_000.0)
    other_columns = [name for name in external_columns if name not in age_columns]
    delayed.loc[:, other_columns] = delayed.loc[:, other_columns].fillna(0.0)
    return pd.concat(
        [frame.drop(columns=external_columns), delayed],
        axis=1,
    )


def _derive_fast_labels(frame: pd.DataFrame, labels: Iterable[str]) -> pd.DataFrame:
    requested = tuple(dict.fromkeys(str(label) for label in labels))
    if not requested:
        return frame
    if FAST_LABEL_CLOSE_COLUMN not in frame:
        raise ValueError(
            f"fast cache lacks {FAST_LABEL_CLOSE_COLUMN}; rebuild under {FEATURE_SCHEMA_VERSION}"
        )
    output = frame.copy()
    close = pd.to_numeric(output[FAST_LABEL_CLOSE_COLUMN], errors="coerce")
    for label_name in requested:
        match = _FAST_LABEL_PATTERN.fullmatch(label_name)
        if match is None:
            raise ValueError(f"unsupported dynamic fast label: {label_name}")
        kind, horizon_text = match.groups()
        horizon = int(horizon_text)
        future_return = np.log(close.shift(-horizon) / close)
        if kind == "ret":
            values = future_return
        elif kind == "dir":
            values = pd.Series(np.nan, index=output.index, dtype=float)
            values.loc[future_return.gt(0.0)] = 1.0
            values.loc[future_return.lt(0.0)] = 0.0
        else:
            values = future_return.ne(0.0).where(future_return.notna()).astype(float)
        output[label_name] = values.astype(np.float32)
    return output


def _read_cache_frame(path: Path, cadence: str, columns: Iterable[str]) -> pd.DataFrame:
    requested = tuple(dict.fromkeys(str(column) for column in columns))
    available = set(_schema(path))
    dynamic_labels = [
        name for name in requested if name not in available and _FAST_LABEL_PATTERN.fullmatch(name)
    ]
    missing = [name for name in requested if name not in available and name not in dynamic_labels]
    if missing:
        raise ValueError(f"{path}: missing requested columns {missing[:5]}")
    read_columns = [name for name in requested if name in available]
    if dynamic_labels:
        if cadence != "fast1s":
            raise ValueError("dynamic fast labels are valid only for fast1s caches")
        if FAST_LABEL_CLOSE_COLUMN not in available:
            raise ValueError(
                f"{path}: dynamic labels require {FAST_LABEL_CLOSE_COLUMN}; rebuild cache"
            )
        if FAST_LABEL_CLOSE_COLUMN not in read_columns:
            read_columns.append(FAST_LABEL_CLOSE_COLUMN)
    frame = pd.read_parquet(path, columns=read_columns)
    if dynamic_labels:
        frame = _derive_fast_labels(frame, dynamic_labels)
    return frame.loc[:, list(requested)]


def _read_days(
    data_dir: Path,
    cadence: str,
    days: Iterable[str],
    columns: list[str],
    *,
    row_stride: int = 1,
    external_delay_s: int = 0,
) -> pd.DataFrame:
    frames = []
    for day in days:
        path = _cache_path(data_dir, cadence, day)
        frame = _read_cache_frame(path, cadence, columns)
        if cadence == "fast1s" and external_delay_s > 0:
            frame = _apply_external_delay(frame, external_delay_s)
        if row_stride > 1:
            frame = frame.iloc[::row_stride]
        frame["_day"] = day
        frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=[*columns, "_day"])
    return pd.concat(frames, axis=0)


def _params(objective: str, *, threads: int) -> dict[str, Any]:
    common = {
        "n_estimators": 900,
        "learning_rate": 0.035,
        "num_leaves": 63,
        "max_depth": 7,
        "min_child_samples": 1000,
        "subsample": 0.80,
        "subsample_freq": 1,
        "colsample_bytree": 0.72,
        "reg_alpha": 0.25,
        "reg_lambda": 1.5,
        "max_bin": 127,
        "n_jobs": max(1, int(threads)),
        "random_state": 42,
        "verbosity": -1,
        "force_col_wise": True,
    }
    if objective == "binary":
        common.update(objective="binary", metric="auc")
    else:
        common.update(objective="regression_l1", metric="l1")
    return common


def _clean_xy(
    frame: pd.DataFrame, features: list[str], label: str
) -> tuple[pd.DataFrame, pd.Series]:
    target = pd.to_numeric(frame[label], errors="coerce")
    valid = target.notna()
    x = frame.loc[valid, features].replace([np.inf, -np.inf], np.nan).copy()
    age_columns = [name for name in features if name.endswith(("_source_age_ms", "_age_ms"))]
    if age_columns:
        x.loc[:, age_columns] = x.loc[:, age_columns].fillna(10_000.0)
    x = x.fillna(0.0).astype(np.float32)
    return x, target.loc[valid].astype(np.float32)


def _metric(
    frame: pd.DataFrame,
    actual: pd.Series,
    prediction: np.ndarray,
    objective: str,
) -> dict[str, Any]:
    if objective == "binary":
        if actual.nunique() < 2:
            auc = math.nan
        else:
            auc = float(roc_auc_score(actual, prediction))
        return {
            "auc": auc,
            "brier": float(brier_score_loss(actual, prediction)),
            "positive_rate": float(actual.mean()),
        }
    corr = spearmanr(actual.to_numpy(), prediction, nan_policy="omit").statistic
    return {
        "mae": float(mean_absolute_error(actual, prediction)),
        "spearman": float(corr) if np.isfinite(corr) else math.nan,
        "target_std": float(actual.std()),
    }


def _daily_metrics(
    days: pd.Series,
    actual: pd.Series,
    prediction: np.ndarray,
    objective: str,
) -> pd.DataFrame:
    work = pd.DataFrame(
        {"day": days.astype(str).to_numpy(), "actual": actual.to_numpy(), "prediction": prediction}
    )
    rows = []
    for day, group in work.groupby("day", sort=True):
        if objective == "binary":
            value = (
                float(roc_auc_score(group["actual"], group["prediction"]))
                if group["actual"].nunique() >= 2
                else math.nan
            )
            rows.append({"day": day, "rows": len(group), "auc": value})
        else:
            corr = spearmanr(group["actual"], group["prediction"], nan_policy="omit").statistic
            rows.append(
                {
                    "day": day,
                    "rows": len(group),
                    "mae": float(mean_absolute_error(group["actual"], group["prediction"])),
                    "spearman": float(corr) if np.isfinite(corr) else math.nan,
                }
            )
    return pd.DataFrame(rows)


def _model_prediction(model: Any, frame: pd.DataFrame, objective: str) -> np.ndarray:
    if objective == "binary" and hasattr(model, "predict_proba"):
        return np.asarray(model.predict_proba(frame)[:, 1])
    return np.asarray(model.predict(frame))


def _score_days_streaming(
    model: Any,
    data_dir: Path,
    cadence: str,
    days: Iterable[str],
    features: list[str],
    label: str,
    objective: str,
    external_delay_s: int,
) -> tuple[dict[str, Any], pd.DataFrame]:
    actual_parts: list[np.ndarray] = []
    prediction_parts: list[np.ndarray] = []
    daily_parts: list[pd.DataFrame] = []
    for day in days:
        frame = _read_cache_frame(_cache_path(data_dir, cadence, day), cadence, [*features, label])
        if cadence == "fast1s" and external_delay_s > 0:
            frame = _apply_external_delay(frame, external_delay_s)
        x_day, y_day = _clean_xy(frame, features, label)
        prediction = _model_prediction(model, x_day, objective)
        actual_parts.append(y_day.to_numpy(dtype=np.float32, copy=False))
        prediction_parts.append(np.asarray(prediction, dtype=np.float32))
        day_values = pd.Series(day, index=y_day.index)
        daily_parts.append(_daily_metrics(day_values, y_day, prediction, objective))
        del frame, x_day, y_day, prediction
        gc.collect()
    if not actual_parts:
        return {}, pd.DataFrame()
    actual = pd.Series(np.concatenate(actual_parts))
    prediction = np.concatenate(prediction_parts)
    metric = _metric(pd.DataFrame(), actual, prediction, objective)
    return metric, pd.concat(daily_parts, ignore_index=True)


def _save_model(
    model: Any,
    output_dir: Path,
    *,
    cadence: str,
    profile: str,
    target: str,
    features: list[str],
    split: ChronologicalSplit,
    development_split: DevelopmentTrainingSplit,
    manifest_hash: str,
    latency_profile: str,
    external_delay_s: int,
) -> tuple[Path, Path]:
    directory = output_dir / cadence / profile
    directory.mkdir(parents=True, exist_ok=True)
    model_path = directory / f"{target}.txt"
    meta_path = directory / f"{target}_meta.json"
    model.booster_.save_model(str(model_path))
    meta_path.write_text(
        json.dumps(
            {
                "research_only": True,
                "reason": "historical trade-time external features lack the live receive-time ABI",
                "model_schema_version": MODEL_SCHEMA_VERSION,
                "feature_schema_version": FEATURE_SCHEMA_VERSION,
                "cadence": cadence,
                "profile": profile,
                "target": target,
                "feature_cols": features,
                "market_data_latency_profile": latency_profile,
                "external_visibility_delay_s": external_delay_s,
                "manifest_sha256": manifest_hash,
                "split": {
                    "train": list(split.train),
                    "embargo_1": list(split.embargo_1),
                    "validation": list(split.validation),
                    "embargo_2": list(split.embargo_2),
                    "test": list(split.test),
                    "late": list(split.late),
                },
                "development_training_split": {
                    "fit": list(development_split.fit),
                    "embargo": list(development_split.embargo),
                    "early_stop": list(development_split.early_stop),
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return model_path, meta_path


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def train_target(
    data_dir: Path,
    output_dir: Path,
    cadence: str,
    profile: str,
    target: str,
    split: ChronologicalSplit,
    *,
    threads: int,
    train_row_stride: int,
    validation_row_stride: int,
    early_stop_days: int,
    early_stop_embargo_days: int,
    score_panels: Iterable[str],
    manifest_hash: str,
    latency_profile: str,
    external_delay_s: int,
) -> tuple[dict[str, Any], list[pd.DataFrame]]:
    label, objective = target_spec(target, cadence=cadence)
    features = feature_columns(data_dir, cadence, profile, split.train[0])
    columns = [*features, label]
    inner_split = development_training_split(
        split.train,
        early_stop_days=early_stop_days,
        embargo_days=early_stop_embargo_days,
    )
    train = _read_days(
        data_dir,
        cadence,
        inner_split.fit,
        columns,
        row_stride=train_row_stride if cadence == "fast1s" else 1,
        external_delay_s=external_delay_s,
    )
    validation = _read_days(
        data_dir,
        cadence,
        inner_split.early_stop,
        columns,
        row_stride=validation_row_stride if cadence == "fast1s" else 1,
        external_delay_s=external_delay_s,
    )
    x_train, y_train = _clean_xy(train, features, label)
    x_valid, y_valid = _clean_xy(validation, features, label)
    print(
        f"training {cadence}/{profile}/{target}: train={len(x_train):,} "
        f"validation={len(x_valid):,} features={len(features)}",
        flush=True,
    )
    model_class = lgb.LGBMClassifier if objective == "binary" else lgb.LGBMRegressor
    model = model_class(**_params(objective, threads=threads))
    started = time.perf_counter()
    model.fit(
        x_train,
        y_train,
        eval_set=[(x_valid, y_valid)],
        callbacks=[lgb.early_stopping(75, verbose=False), lgb.log_evaluation(100)],
    )
    elapsed = time.perf_counter() - started
    row: dict[str, Any] = {
        "cadence": cadence,
        "profile": profile,
        "target": target,
        "objective": objective,
        "latency_profile": latency_profile,
        "external_delay_s": external_delay_s,
        "features": len(features),
        "train_rows": len(x_train),
        "early_stop_validation_rows": len(x_valid),
        "fit_days": len(inner_split.fit),
        "early_stop_embargo_days": len(inner_split.embargo),
        "early_stop_days": len(inner_split.early_stop),
        "best_iteration": int(model.best_iteration_ or 0),
        "train_time_s": elapsed,
    }
    del train, validation, x_train, y_train, x_valid, y_valid
    gc.collect()

    daily_outputs: list[pd.DataFrame] = []

    requested_panels = set(str(panel) for panel in score_panels)
    for panel_name, panel_days in (
        ("validation", split.validation),
        ("test", split.test),
        ("late", split.late),
    ):
        if panel_name not in requested_panels:
            continue
        if not panel_days:
            continue
        panel_metric, panel_daily = _score_days_streaming(
            model,
            data_dir,
            cadence,
            panel_days,
            features,
            label,
            objective,
            external_delay_s,
        )
        row.update(
            {
                **{f"{panel_name}_{key}": value for key, value in panel_metric.items()},
            }
        )
        row[f"{panel_name}_rows"] = int(panel_daily["rows"].sum()) if not panel_daily.empty else 0
        daily_outputs.append(
            panel_daily.assign(cadence=cadence, profile=profile, target=target, panel=panel_name)
        )

    model_path, meta_path = _save_model(
        model,
        output_dir,
        cadence=cadence,
        profile=profile,
        target=target,
        features=features,
        split=split,
        development_split=inner_split,
        manifest_hash=manifest_hash,
        latency_profile=latency_profile,
        external_delay_s=external_delay_s,
    )
    row.update(model_path=str(model_path), meta_path=str(meta_path))
    return row, daily_outputs


def _paired_summary(metrics: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (cadence, target), group in metrics.groupby(["cadence", "target"], sort=True):
        base = group[group["profile"].eq("m0_local_binance")]
        candidate = group[group["profile"].eq("m1_external_all")]
        if base.empty or candidate.empty:
            continue
        item: dict[str, Any] = {"cadence": cadence, "target": target}
        objective = str(candidate.iloc[0]["objective"])
        for panel in ("validation", "test", "late"):
            metric = "auc" if objective == "binary" else "mae"
            base_col = f"{panel}_{metric}"
            if (
                base_col not in group
                or pd.isna(base.iloc[0].get(base_col))
                or pd.isna(candidate.iloc[0].get(base_col))
            ):
                continue
            base_value = float(base.iloc[0][base_col])
            candidate_value = float(candidate.iloc[0][base_col])
            gain = (
                candidate_value - base_value
                if objective == "binary"
                else base_value - candidate_value
            )
            item[f"{panel}_m0_{metric}"] = base_value
            item[f"{panel}_m1_{metric}"] = candidate_value
            item[f"{panel}_gain"] = gain

            daily_panel = daily[
                daily["cadence"].eq(cadence) & daily["target"].eq(target) & daily["panel"].eq(panel)
            ]
            pivot = daily_panel.pivot(index="day", columns="profile", values=metric).dropna()
            if set(PROFILES).issubset(pivot.columns):
                daily_gain = (
                    pivot["m1_external_all"] - pivot["m0_local_binance"]
                    if objective == "binary"
                    else pivot["m0_local_binance"] - pivot["m1_external_all"]
                )
                item[f"{panel}_positive_days"] = int((daily_gain > 0.0).sum())
                item[f"{panel}_paired_days"] = int(len(daily_gain))
                item[f"{panel}_median_daily_gain"] = float(daily_gain.median())
        rows.append(item)
    return pd.DataFrame(rows)


def build_horizon_decay_curve(
    daily: pd.DataFrame,
    *,
    panel: str,
    target_kind: str = "dir",
    bootstrap_trials: int = 2_000,
    random_seed: int = 20260727,
    min_paired_days: int = 10,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Select a horizon from a complete Development daily-gain curve.

    Selection uses only the named panel and a simultaneous day-cluster band
    across every scanned horizon. Test/late outcomes are not inputs. The
    horizon grid itself remains an explicit experiment-level engineering
    support choice.
    """

    if target_kind not in FAST_TARGET_KINDS:
        raise ValueError(f"unknown fast target kind: {target_kind}")
    scoped = daily[
        daily["cadence"].eq("fast1s")
        & daily["panel"].eq(panel)
        & daily["target"].astype(str).str.startswith(f"{target_kind}_")
    ].copy()
    horizon_series: list[pd.Series] = []
    for target, target_rows in scoped.groupby("target", sort=False):
        match = _FAST_TARGET_PATTERN.fullmatch(str(target))
        if match is None or match.group(1) != target_kind:
            continue
        horizon = int(match.group(2))
        pivot = target_rows.pivot(index="day", columns="profile", values="auc")
        if not set(PROFILES).issubset(pivot.columns):
            continue
        gain = pivot["m1_external_all"] - pivot["m0_local_binance"]
        horizon_series.append(gain.rename(horizon))
    if not horizon_series:
        raise ValueError(f"no paired {target_kind} horizon rows for panel={panel}")
    matrix = pd.concat(horizon_series, axis=1, join="inner").sort_index(axis=1).dropna()
    if len(matrix) < int(min_paired_days):
        raise ValueError(
            f"horizon decay requires {min_paired_days} common days; found {len(matrix)}"
        )

    values = matrix.to_numpy(dtype=float)
    means = values.mean(axis=0)
    rng = np.random.default_rng(int(random_seed))
    trials = max(200, int(bootstrap_trials))
    sampled = rng.integers(0, len(matrix), size=(trials, len(matrix)))
    boot_means = values[sampled, :].mean(axis=1)
    centered = boot_means - means
    simultaneous_radius = float(np.quantile(np.max(np.abs(centered), axis=1), 0.95))
    lower = means - simultaneous_radius
    upper = means + simultaneous_radius
    selected_indices = np.argmax(boot_means, axis=1)
    selection_frequency = np.bincount(selected_indices, minlength=values.shape[1]) / float(trials)
    horizons = np.asarray(matrix.columns, dtype=int)
    best_index = int(np.argmax(means))
    eligible = np.flatnonzero(lower > 0.0)
    selected_index = int(eligible[np.argmax(lower[eligible])]) if eligible.size else None

    curve = pd.DataFrame(
        {
            "horizon_s": horizons,
            "paired_days": len(matrix),
            "mean_daily_auc_gain": means,
            "median_daily_auc_gain": matrix.median(axis=0).to_numpy(dtype=float),
            "positive_day_rate": (matrix > 0.0).mean(axis=0).to_numpy(dtype=float),
            "simultaneous_lcb_95": lower,
            "simultaneous_ucb_95": upper,
            "bootstrap_best_frequency": selection_frequency,
            "formal_positive_band": lower > 0.0,
        }
    )
    peak_gain = float(means[best_index])
    half_life_horizon: int | None = None
    if peak_gain > 0.0:
        after_peak = np.flatnonzero(
            (np.arange(len(horizons)) > best_index) & (means <= 0.5 * peak_gain)
        )
        if after_peak.size:
            half_life_horizon = int(horizons[int(after_peak[0])])
    contract = {
        "schema_version": "external_information_decay_selection.v1",
        "selection_panel": panel,
        "selection_panel_role": "development_screening",
        "selection_uses_test_or_late": False,
        "target_kind": target_kind,
        "horizon_grid_s": horizons.tolist(),
        "paired_days": int(len(matrix)),
        "bootstrap": {
            "cluster": "UTC_day",
            "trials": trials,
            "seed": int(random_seed),
            "band": "two-sided_95pct_simultaneous_max_abs",
        },
        "diagnostic_peak_horizon_s": int(horizons[best_index]),
        "diagnostic_peak_mean_daily_auc_gain": peak_gain,
        "empirical_post_peak_half_gain_horizon_s": half_life_horizon,
        "formal_selected_horizon_s": (
            int(horizons[selected_index]) if selected_index is not None else None
        ),
        "formal_selection_rule": (
            "maximize simultaneous lower bound among horizons whose "
            "simultaneous 95% lower bound is positive"
        ),
        "prediction_only": True,
        "live_action_authority": False,
        "confirmation_authority": (
            "requires a separately frozen family split; historical panel names "
            "do not imply untouched evidence"
        ),
    }
    return curve, contract


def rescore_existing_bundle(
    data_dir: Path,
    output_dir: Path,
    split: ChronologicalSplit,
    *,
    cadences: Iterable[str],
    targets_by_cadence: dict[str, Iterable[str]],
    profiles: Iterable[str],
    panels: Iterable[str],
    external_delay_s: int,
) -> None:
    metrics_path = output_dir / "metrics.csv"
    daily_path = output_dir / "daily_metrics.csv"
    if not metrics_path.exists() or not daily_path.exists():
        raise FileNotFoundError(f"existing metrics bundle is required under {output_dir}")
    metrics = pd.read_csv(metrics_path)
    daily = pd.read_csv(daily_path)
    panel_days = {
        "validation": split.validation,
        "test": split.test,
        "late": split.late,
    }
    selected_panels = tuple(dict.fromkeys(str(panel) for panel in panels))

    for cadence in cadences:
        for target in targets_by_cadence[cadence]:
            label, objective = target_spec(target, cadence=cadence)
            for profile in profiles:
                row_mask = (
                    metrics["cadence"].eq(cadence)
                    & metrics["target"].eq(target)
                    & metrics["profile"].eq(profile)
                )
                if int(row_mask.sum()) != 1:
                    raise ValueError(
                        f"expected one metrics row for {cadence}/{profile}/{target}; "
                        f"found {int(row_mask.sum())}"
                    )
                model_dir = output_dir / cadence / profile
                meta = json.loads((model_dir / f"{target}_meta.json").read_text(encoding="utf-8"))
                features = list(meta["feature_cols"])
                model = lgb.Booster(model_file=str(model_dir / f"{target}.txt"))
                for panel in selected_panels:
                    days = panel_days[panel]
                    if not days:
                        continue
                    panel_metric, panel_daily = _score_days_streaming(
                        model,
                        data_dir,
                        cadence,
                        days,
                        features,
                        label,
                        objective,
                        external_delay_s,
                    )
                    for key, value in panel_metric.items():
                        metrics.loc[row_mask, f"{panel}_{key}"] = value
                    metrics.loc[row_mask, f"{panel}_rows"] = int(panel_daily["rows"].sum())
                    old_mask = (
                        daily["cadence"].eq(cadence)
                        & daily["target"].eq(target)
                        & daily["profile"].eq(profile)
                        & daily["panel"].eq(panel)
                    )
                    daily = pd.concat(
                        [
                            daily.loc[~old_mask],
                            panel_daily.assign(
                                cadence=cadence,
                                profile=profile,
                                target=target,
                                panel=panel,
                            ),
                        ],
                        ignore_index=True,
                    )
                print(f"rescored {cadence}/{profile}/{target}", flush=True)

    metrics.to_csv(metrics_path, index=False)
    daily = daily.sort_values(["cadence", "target", "panel", "profile", "day"])
    daily.to_csv(daily_path, index=False)
    paired = _paired_summary(metrics, daily)
    paired.to_csv(output_dir / "paired_summary.csv", index=False)
    if not paired.empty:
        print(paired.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--late-manifest", type=Path, default=DEFAULT_LATE_MANIFEST)
    parser.add_argument("--data-dir", type=Path, default=data_root(ROOT))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--cadences",
        nargs="+",
        choices=("10s", "fast1s"),
        default=("fast1s",),
        help="The 10s cadence is retained only for historical reproduction.",
    )
    parser.add_argument(
        "--targets-10s",
        nargs="+",
        choices=sorted(TARGET_SPECS_10S),
        default=tuple(TARGET_SPECS_10S),
    )
    parser.add_argument(
        "--fast-horizons-s",
        nargs="+",
        type=int,
        help=(
            "Explicit integer-second horizon grid. Formal fast1s runs must "
            "declare this grid or --fast-horizon-max-s."
        ),
    )
    parser.add_argument("--fast-horizon-min-s", type=int, default=1)
    parser.add_argument("--fast-horizon-max-s", type=int)
    parser.add_argument("--fast-horizon-step-s", type=int, default=1)
    parser.add_argument(
        "--fast-target-kinds",
        nargs="+",
        choices=FAST_TARGET_KINDS,
        default=("dir",),
    )
    parser.add_argument(
        "--targets-fast",
        nargs="+",
        help="Deprecated historical compatibility list such as dir_1s dir_3s.",
    )
    parser.add_argument("--profiles", nargs="+", choices=PROFILES, default=PROFILES)
    parser.add_argument("--train-days", type=int, default=69)
    parser.add_argument("--validation-days", type=int, default=20)
    parser.add_argument("--test-days", type=int, default=20)
    parser.add_argument("--embargo-days", type=int, default=1)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--fast-train-row-stride", type=int, default=2)
    parser.add_argument("--fast-validation-row-stride", type=int, default=5)
    parser.add_argument("--early-stop-days", type=int, default=7)
    parser.add_argument("--early-stop-embargo-days", type=int, default=1)
    parser.add_argument("--horizon-bootstrap-trials", type=int, default=2_000)
    parser.add_argument("--horizon-bootstrap-seed", type=int, default=20260727)
    parser.add_argument("--horizon-min-paired-days", type=int, default=10)
    parser.add_argument(
        "--horizon-selection-artifact",
        type=Path,
        help=(
            "Frozen Development selection JSON. Required before fast1s test/late "
            "scoring and restricts scoring to its one selected horizon."
        ),
    )
    parser.add_argument(
        "--external-delay-s",
        type=int,
        default=0,
        help="Whole-second visibility delay applied to external fast-head features.",
    )
    parser.add_argument(
        "--latency-profile",
        default="exchange_zero",
        help="Environment/profile label persisted with every model artifact.",
    )
    parser.add_argument("--force-cache", action="store_true")
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Resume a partially completed training grid after validating each "
            "existing model, metadata file, and requested daily score panel."
        ),
    )
    parser.add_argument("--build-only", action="store_true")
    parser.add_argument(
        "--score-only",
        action="store_true",
        help="Rescore an existing model bundle after cache/evaluation updates.",
    )
    parser.add_argument(
        "--score-panels",
        nargs="+",
        choices=("validation", "test", "late"),
        default=("validation",),
        help=(
            "Panels to read. Default reads Development-selection Validation "
            "only; add test/late explicitly after the selection artifact freezes."
        ),
    )
    args = parser.parse_args()
    if args.build_only and args.score_only:
        parser.error("--build-only and --score-only are mutually exclusive")
    if args.targets_fast:
        for target in args.targets_fast:
            match = _FAST_TARGET_PATTERN.fullmatch(str(target))
            if match is None:
                parser.error(f"invalid historical fast target: {target}")
        targets_fast = tuple(dict.fromkeys(str(target) for target in args.targets_fast))
        fast_horizons = normalize_fast_target_horizons(
            int(_FAST_TARGET_PATTERN.fullmatch(target).group(2)) for target in targets_fast
        )
        selected_fast_target_kinds = tuple(
            dict.fromkeys(
                _FAST_TARGET_PATTERN.fullmatch(target).group(1) for target in targets_fast
            )
        )
        fast_target_mode = "legacy_explicit_targets"
    else:
        if args.fast_horizons_s:
            fast_horizons = normalize_fast_target_horizons(args.fast_horizons_s)
        elif args.fast_horizon_max_s is not None:
            if args.fast_horizon_step_s <= 0:
                parser.error("--fast-horizon-step-s must be positive")
            if args.fast_horizon_max_s < args.fast_horizon_min_s:
                parser.error("fast horizon max must be at least the min")
            fast_horizons = normalize_fast_target_horizons(
                range(
                    args.fast_horizon_min_s,
                    args.fast_horizon_max_s + 1,
                    args.fast_horizon_step_s,
                )
            )
        elif "fast1s" in args.cadences:
            parser.error(
                "fast1s research requires an explicit --fast-horizons-s or "
                "--fast-horizon-max-s; no 1/3/5 default is permitted"
            )
        else:
            fast_horizons = ()
        targets_fast = (
            tuple(fast_target_specs(fast_horizons, kinds=args.fast_target_kinds))
            if fast_horizons
            else ()
        )
        selected_fast_target_kinds = tuple(args.fast_target_kinds)
        fast_target_mode = "dense_declared_grid"

    selection_artifact_identity: dict[str, Any] | None = None
    protected_panels = {"test", "late"}.intersection(args.score_panels)
    if args.horizon_selection_artifact is not None:
        selection_path = args.horizon_selection_artifact.expanduser().resolve()
        selection = json.loads(selection_path.read_text(encoding="utf-8"))
        if selection.get("schema_version") != "external_information_decay_selection.v1":
            parser.error("unsupported horizon selection artifact")
        if selection.get("model_schema_version") != MODEL_SCHEMA_VERSION:
            parser.error("selection artifact model schema does not match")
        if selection.get("feature_schema_version") != FEATURE_SCHEMA_VERSION:
            parser.error("selection artifact feature schema does not match")
        if selection.get("manifest_sha256") != _hash_file(args.manifest):
            parser.error("selection artifact data manifest does not match")
        if int(selection.get("external_visibility_delay_s", -1)) != max(0, args.external_delay_s):
            parser.error("selection artifact external delay does not match")
        if str(selection.get("market_data_latency_profile", "")) != str(args.latency_profile):
            parser.error("selection artifact latency profile does not match")
        selected_horizon = selection.get("formal_selected_horizon_s")
        if selected_horizon is None:
            parser.error("selection artifact has no formally eligible horizon")
        selected_horizon = int(selected_horizon)
        if selected_horizon not in fast_horizons:
            parser.error("selected horizon is outside the declared horizon grid")
        if selection.get("horizon_grid_s") != list(fast_horizons):
            parser.error("selection artifact horizon grid does not match this run")
        selected_kind = str(selection.get("target_kind", ""))
        if selected_kind not in FAST_TARGET_KINDS:
            parser.error("selection artifact target kind is unsupported")
        targets_fast = (f"{selected_kind}_{selected_horizon}s",)
        selected_fast_target_kinds = (selected_kind,)
        selection_artifact_identity = {
            "path": str(selection_path),
            "sha256": _hash_file(selection_path),
            "selected_horizon_s": selected_horizon,
        }
    elif protected_panels and "fast1s" in args.cadences:
        parser.error(
            "fast1s test/late scoring requires --horizon-selection-artifact; "
            "the full horizon curve may be read only on validation"
        )

    data_dir = args.data_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    days = load_days(args.manifest)
    late_candidates = load_days(args.late_manifest) if args.late_manifest.exists() else []
    split = chronological_split(
        days,
        train_days=args.train_days,
        validation_days=args.validation_days,
        test_days=args.test_days,
        embargo_days=args.embargo_days,
        late_days=late_candidates,
    )
    declared_development_split = development_training_split(
        split.train,
        early_stop_days=max(1, args.early_stop_days),
        embargo_days=max(0, args.early_stop_embargo_days),
    )
    score_day_map = {
        "validation": split.validation,
        "test": split.test,
        "late": split.late,
    }
    all_days = list(
        dict.fromkeys(
            [
                *split.train,
                *(day for panel in args.score_panels for day in score_day_map[panel]),
            ]
        )
    )
    manifest_hash = _hash_file(args.manifest)
    split_path = output_dir / "split_manifest.json"
    split_path.write_text(
        json.dumps(
            {
                "model_schema_version": MODEL_SCHEMA_VERSION,
                "feature_schema_version": FEATURE_SCHEMA_VERSION,
                "research_only": True,
                "manifest": str(args.manifest.resolve()),
                "manifest_sha256": manifest_hash,
                "train": list(split.train),
                "embargo_1": list(split.embargo_1),
                "validation": list(split.validation),
                "embargo_2": list(split.embargo_2),
                "test": list(split.test),
                "late": list(split.late),
                "cache_days": all_days,
                "unrequested_panels_cached": False,
                "fast_target_contract": {
                    "mode": fast_target_mode,
                    "horizon_grid_s": list(fast_horizons),
                    "target_kinds": list(selected_fast_target_kinds),
                    "grid_identity": "judgmental_engineering_support_not_theory",
                    "selection_panel": "validation",
                    "confirmation_panels_read": list(args.score_panels),
                    "selection_artifact": selection_artifact_identity,
                },
                "development_early_stop_contract": {
                    "fit": list(declared_development_split.fit),
                    "embargo": list(declared_development_split.embargo),
                    "early_stop": list(declared_development_split.early_stop),
                    "outer_validation_used_for_early_stopping": False,
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    cache_rows = []
    for cadence in args.cadences:
        cache_rows.extend(
            build_cache(
                data_dir,
                cadence,
                all_days,
                workers=args.workers,
                force=args.force_cache,
            )
        )
    pd.DataFrame(cache_rows).to_csv(output_dir / "cache_manifest.csv", index=False)
    if args.build_only:
        print(f"saved {split_path}")
        return

    metrics_path = output_dir / "metrics.csv"
    daily_path = output_dir / "daily_metrics.csv"
    if args.resume and metrics_path.exists() and daily_path.exists():
        existing_metrics = pd.read_csv(metrics_path)
        existing_daily = pd.read_csv(daily_path)
        metrics: list[dict[str, Any]] = existing_metrics.to_dict("records")
        daily_frames: list[pd.DataFrame] = [existing_daily]
    elif args.resume and (metrics_path.exists() or daily_path.exists()):
        parser.error("--resume requires both metrics.csv and daily_metrics.csv")
    else:
        metrics = []
        daily_frames = []
    targets_by_cadence = {"10s": args.targets_10s, "fast1s": targets_fast}
    if args.score_only:
        rescore_existing_bundle(
            data_dir,
            output_dir,
            split,
            cadences=args.cadences,
            targets_by_cadence=targets_by_cadence,
            profiles=args.profiles,
            panels=args.score_panels,
            external_delay_s=max(0, args.external_delay_s),
        )
        print(f"rescored existing research bundle under {output_dir}")
        return

    for cadence in args.cadences:
        for target in targets_by_cadence[cadence]:
            for profile in args.profiles:
                if args.resume and metrics:
                    existing_rows = [
                        row
                        for row in metrics
                        if row.get("cadence") == cadence
                        and row.get("target") == target
                        and row.get("profile") == profile
                    ]
                    if existing_rows:
                        if len(existing_rows) != 1:
                            raise ValueError(
                                f"resume found duplicate metrics for {cadence}/{profile}/{target}"
                            )
                        model_dir = output_dir / cadence / profile
                        required_artifacts = (
                            model_dir / f"{target}.txt",
                            model_dir / f"{target}_meta.json",
                        )
                        missing_artifacts = [
                            str(path) for path in required_artifacts if not path.exists()
                        ]
                        if missing_artifacts:
                            raise FileNotFoundError(
                                f"resume artifacts missing for {cadence}/{profile}/{target}: "
                                f"{missing_artifacts}"
                            )
                        existing_daily = pd.concat(daily_frames, ignore_index=True)
                        scoped = existing_daily.loc[
                            existing_daily["cadence"].eq(cadence)
                            & existing_daily["target"].eq(target)
                            & existing_daily["profile"].eq(profile)
                        ]
                        panel_days = {
                            "validation": split.validation,
                            "test": split.test,
                            "late": split.late,
                        }
                        for panel in args.score_panels:
                            observed = set(
                                scoped.loc[scoped["panel"].eq(panel), "day"].astype(str)
                            )
                            expected = set(panel_days[panel])
                            if observed != expected:
                                raise ValueError(
                                    f"resume daily panel mismatch for "
                                    f"{cadence}/{profile}/{target}/{panel}: "
                                    f"observed={len(observed)} expected={len(expected)}"
                                )
                        print(
                            f"resuming past completed {cadence}/{profile}/{target}",
                            flush=True,
                        )
                        continue
                row, daily = train_target(
                    data_dir,
                    output_dir,
                    cadence,
                    profile,
                    target,
                    split,
                    threads=args.threads,
                    train_row_stride=max(1, args.fast_train_row_stride),
                    validation_row_stride=max(1, args.fast_validation_row_stride),
                    early_stop_days=max(1, args.early_stop_days),
                    early_stop_embargo_days=max(0, args.early_stop_embargo_days),
                    score_panels=args.score_panels,
                    manifest_hash=manifest_hash,
                    latency_profile=args.latency_profile,
                    external_delay_s=max(0, args.external_delay_s),
                )
                metrics.append(row)
                daily_frames.extend(daily)
                pd.DataFrame(metrics).to_csv(metrics_path, index=False)
                pd.concat(daily_frames, ignore_index=True).to_csv(
                    daily_path, index=False
                )
                print(f"completed {cadence}/{profile}/{target}", flush=True)

    metric_frame = pd.DataFrame(metrics)
    daily_frame = pd.concat(daily_frames, ignore_index=True)
    paired = _paired_summary(metric_frame, daily_frame)
    paired.to_csv(output_dir / "paired_summary.csv", index=False)
    if (
        "fast1s" in args.cadences
        and "dir" in selected_fast_target_kinds
        and "validation" in args.score_panels
    ):
        curve, selection = build_horizon_decay_curve(
            daily_frame,
            panel="validation",
            target_kind="dir",
            bootstrap_trials=args.horizon_bootstrap_trials,
            random_seed=args.horizon_bootstrap_seed,
            min_paired_days=args.horizon_min_paired_days,
        )
        curve_path = output_dir / "direction_horizon_decay_curve.csv"
        curve.to_csv(curve_path, index=False)
        selection.update(
            {
                "model_schema_version": MODEL_SCHEMA_VERSION,
                "feature_schema_version": FEATURE_SCHEMA_VERSION,
                "manifest_sha256": manifest_hash,
                "split_manifest_sha256": _hash_file(split_path),
                "metrics_sha256": _hash_file(output_dir / "metrics.csv"),
                "daily_metrics_sha256": _hash_file(output_dir / "daily_metrics.csv"),
                "curve_sha256": _hash_file(curve_path),
                "external_visibility_delay_s": max(0, args.external_delay_s),
                "market_data_latency_profile": args.latency_profile,
            }
        )
        (output_dir / "direction_horizon_selection.json").write_text(
            json.dumps(selection, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(paired.to_string(index=False))
    print(f"saved research bundle and reports under {output_dir}")


if __name__ == "__main__":
    main()
