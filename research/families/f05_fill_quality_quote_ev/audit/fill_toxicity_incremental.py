#!/usr/bin/env python3
"""Compare local-only M0 with causal external-flow M1 fill-toxicity models."""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.compose import TransformedTargetRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import brier_score_loss, mean_absolute_error, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.audit.support import norm_side  # noqa: E402

SCHEMA_VERSION = "fill_toxicity_incremental.v1.1"
EXTERNAL_VENUES = ("bitget", "bybit", "okx")
MARKET_METRICS = (
    "flow_pressure",
    "mid_move_bps",
    "trade_imbalance",
    "l1_ofi_normalized",
    "bid_depletion",
    "bid_refill",
    "ask_depletion",
    "ask_refill",
)
LOCAL_CONTEXT_NUMERIC = (
    "age_ms",
    "spread_mult",
    "size_mult",
    "inventory_ratio",
    "toxicity",
    "markout_ema",
    "depth_age_s",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total",
    "mid",
    "target_price",
    "target_qty",
)
CAMPAIGN_FEATURES = (
    "inventory_before",
    "inventory_after",
    "abs_inventory_before",
    "campaign_age_s_before",
    "campaign_max_abs_inventory_before",
    "campaign_add_count_before",
    "campaign_fill_count_before",
)


@dataclass(frozen=True)
class DayFold:
    panel: str
    fold: int
    train_days: tuple[str, ...]
    embargo_days: tuple[str, ...]
    test_days: tuple[str, ...]


def load_frames(paths: list[Path]) -> pd.DataFrame:
    frames = [pd.read_csv(path) for path in paths]
    if not frames:
        raise ValueError("at least one fill-toxicity input is required")
    frame = pd.concat(frames, ignore_index=True, sort=False)
    if "day" not in frame:
        raise ValueError("fill-toxicity rows require day")
    return frame


def _numeric(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce")


def match_order_outcomes(
    fills: pd.DataFrame,
    outcomes_path: Path | None,
    *,
    tolerance_s: float = 3.0,
    price_tolerance: float = 0.2,
) -> pd.DataFrame:
    """Attach order context by an exact replay/live order identity.

    ``tolerance_s`` and ``price_tolerance`` remain in the signature for CLI
    compatibility, but are deliberately unused.  A side/time/price proximity
    match can silently attach another order during an active quote burst and
    is not an admissible research join.
    """

    del tolerance_s, price_tolerance
    frame = fills.copy()
    frame["side"] = frame["side"].map(norm_side)
    frame["fill_ts"] = _numeric(frame, "fill_ts")
    frame["local_context_match_method"] = "none"
    frame["local_context_match_id"] = ""
    if outcomes_path is None:
        frame["local_context_matched"] = 0
        return frame
    outcomes = pd.read_csv(outcomes_path)
    outcomes = outcomes[
        outcomes.get("event_type", pd.Series(index=outcomes.index, dtype=str))
        .astype(str)
        .str.lower()
        .isin({"filled", "partial_fill"})
    ].copy()
    if outcomes.empty:
        frame["local_context_matched"] = 0
        return frame
    outcomes["side"] = outcomes["side"].map(norm_side)
    outcomes["outcome_ts"] = _numeric(outcomes, "timestamp")
    outcomes["outcome_price"] = _numeric(outcomes, "avg_fill_price").fillna(
        _numeric(outcomes, "price")
    )
    identity_column = next(
        (
            name
            for name in ("decision_id", "client_order_id")
            if name in frame.columns and name in outcomes.columns
        ),
        None,
    )
    if identity_column is None:
        frame["local_context_matched"] = 0
        frame["local_context_match_method"] = "missing_exact_identity"
        return frame

    def _identity(values: pd.Series) -> pd.Series:
        normalized = values.astype("string").str.strip()
        return normalized.mask(normalized.isin({"", "<NA>", "nan", "None"}))

    frame["_exact_match_id"] = _identity(frame[identity_column])
    outcomes["_exact_match_id"] = _identity(outcomes[identity_column])
    outcomes = outcomes[outcomes["_exact_match_id"].notna()].copy()
    if outcomes.empty:
        frame["local_context_matched"] = 0
        frame["local_context_match_method"] = f"empty_exact_{identity_column}"
        return frame.drop(columns="_exact_match_id")

    def _utc_day(values: pd.Series) -> pd.Series:
        numeric = pd.to_numeric(values, errors="coerce")
        # Existing audit files store second timestamps; nanosecond timestamps
        # are not accepted here because this join is only for live/order CSVs.
        return pd.to_datetime(numeric, unit="s", utc=True, errors="coerce").dt.strftime(
            "%Y-%m-%d"
        )

    if "day" in frame.columns:
        frame["_exact_match_day"] = frame["day"].astype("string")
    else:
        frame["_exact_match_day"] = _utc_day(frame["fill_ts"])
    if "day" in outcomes.columns:
        outcomes["_exact_match_day"] = outcomes["day"].astype("string")
    else:
        outcomes["_exact_match_day"] = _utc_day(outcomes["outcome_ts"])

    keep = [
        "_exact_match_day",
        "_exact_match_id",
        "side",
        "outcome_ts",
        "outcome_price",
        "decision_id",
        "client_order_id",
        "mode",
        "reason_mask",
        "reason_text",
        *LOCAL_CONTEXT_NUMERIC,
    ]
    keep = [name for name in keep if name in outcomes]
    outcomes = outcomes[keep].copy()
    outcomes = outcomes.rename(
        columns={
            name: f"local_{name}"
            for name in outcomes.columns
            if name
            not in {
                "_exact_match_day",
                "_exact_match_id",
                "side",
                "outcome_ts",
                "outcome_price",
            }
        }
    )
    # Multiple partial-fill updates may exist for one exact order identity.
    # The latest filled/partial-fill row is the deterministic order-context
    # snapshot, and many fill rows may safely reference it.
    outcomes = outcomes.sort_values("outcome_ts", kind="stable").drop_duplicates(
        ["_exact_match_day", "_exact_match_id"], keep="last"
    )
    merged = frame.reset_index(names="_source_index").merge(
        outcomes,
        on=["_exact_match_day", "_exact_match_id"],
        how="left",
        suffixes=("", "_outcome"),
        validate="many_to_one",
    )
    matched = merged["outcome_ts"].notna()
    merged["local_context_matched"] = matched.astype(int)
    merged["local_context_match_method"] = np.where(
        matched, f"exact_{identity_column}", f"unmatched_exact_{identity_column}"
    )
    merged["local_context_match_id"] = merged["_exact_match_id"].fillna("")
    local_columns = [name for name in merged if name.startswith("local_")]
    value_columns = [
        name
        for name in local_columns
        if name not in {"local_context_matched", "local_context_match_method", "local_context_match_id"}
    ]
    merged.loc[~matched, value_columns] = np.nan
    return (
        merged.sort_values("_source_index")
        .drop(columns=["_source_index", "_exact_match_day", "_exact_match_id"])
        .reset_index(drop=True)
    )


def add_inventory_campaign_state(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for name in (
        "inventory_before",
        "inventory_after",
        "abs_inventory_before",
        "campaign_age_s_before",
        "campaign_max_abs_inventory_before",
        "campaign_add_count_before",
        "campaign_fill_count_before",
        "campaign_id",
        "campaign_flattened",
        "campaign_open_risk",
        "campaign_terminal_pnl",
        "campaign_tail",
    ):
        output[name] = np.nan
    output["inventory_role"] = "unknown"
    group_column = (
        "market_data_latency_mode"
        if "market_data_latency_mode" in output
        else None
    )
    grouped = output.groupby(group_column, sort=False, dropna=False) if group_column else [("all", output)]
    for _, group in grouped:
        ordered = group.sort_values(["fill_ts", "fill_id"], kind="stable")
        campaign_number = 0
        campaign_start = 0.0
        campaign_max = 0.0
        campaign_adds = 0
        campaign_fills = 0
        active_indices: list[int] = []
        realized_sum = 0.0
        for index, row in ordered.iterrows():
            side = norm_side(row.get("side"))
            qty = float(pd.to_numeric(pd.Series([row.get("qty")]), errors="coerce").iloc[0] or 0.0)
            signed_qty = qty if side == "BUY" else -qty
            inventory_after = float(
                pd.to_numeric(pd.Series([row.get("position_after")]), errors="coerce").iloc[0]
            )
            if not math.isfinite(inventory_after):
                continue
            inventory_before = inventory_after - signed_qty
            if abs(inventory_before) <= 1e-12 and abs(inventory_after) > 1e-12:
                campaign_number += 1
                campaign_start = float(row["fill_ts"])
                campaign_max = 0.0
                campaign_adds = 0
                campaign_fills = 0
                active_indices = []
                realized_sum = 0.0
            if abs(inventory_before) <= 1e-12 and abs(inventory_after) > 1e-12:
                role = "opener"
            elif abs(inventory_after) > abs(inventory_before) + 1e-12:
                role = "add"
            elif abs(inventory_after) < abs(inventory_before) - 1e-12:
                role = "reducing"
            else:
                role = "flat_or_flip"
            output.at[index, "inventory_before"] = inventory_before
            output.at[index, "inventory_after"] = inventory_after
            output.at[index, "abs_inventory_before"] = abs(inventory_before)
            output.at[index, "inventory_role"] = role
            output.at[index, "campaign_id"] = campaign_number
            output.at[index, "campaign_age_s_before"] = max(
                0.0, float(row["fill_ts"]) - campaign_start
            )
            output.at[index, "campaign_max_abs_inventory_before"] = campaign_max
            output.at[index, "campaign_add_count_before"] = campaign_adds
            output.at[index, "campaign_fill_count_before"] = campaign_fills
            active_indices.append(index)
            campaign_fills += 1
            campaign_adds += int(role in {"opener", "add"})
            campaign_max = max(campaign_max, abs(inventory_after))
            realized = pd.to_numeric(
                pd.Series([row.get("realized_pnl")]), errors="coerce"
            ).iloc[0]
            if math.isfinite(float(realized)):
                realized_sum += float(realized)
            if abs(inventory_after) <= 1e-12 and active_indices:
                output.loc[active_indices, "campaign_flattened"] = 1
                output.loc[active_indices, "campaign_open_risk"] = 0
                output.loc[active_indices, "campaign_terminal_pnl"] = realized_sum
                output.loc[active_indices, "campaign_tail"] = int(realized_sum <= -5.0)
                active_indices = []
                campaign_start = 0.0
                campaign_max = 0.0
                campaign_adds = 0
                campaign_fills = 0
                realized_sum = 0.0
        if active_indices:
            output.loc[active_indices, "campaign_flattened"] = 0
            output.loc[active_indices, "campaign_open_risk"] = 1
    return output


def _horizons(frame: pd.DataFrame) -> list[str]:
    values = set()
    for name in frame.columns:
        if name.startswith("execution_flow_pressure_"):
            values.add(name.rsplit("_", 1)[-1])
    return sorted(values, key=lambda value: int(value.removesuffix("ms")))


def build_external_consensus(
    frame: pd.DataFrame,
    *,
    included_venues: tuple[str, ...],
) -> pd.DataFrame:
    output: dict[str, pd.Series] = {}
    for suffix in _horizons(frame):
        factor_frames: dict[str, dict[str, pd.Series]] = {}
        for factor in ("spot", "perp"):
            metrics: dict[str, pd.Series] = {}
            fresh_columns = [
                f"{venue}_{factor}_book_fresh_{suffix}"
                for venue in included_venues
            ]
            fresh = pd.concat(
                [_numeric(frame, name) for name in fresh_columns], axis=1
            )
            fresh.columns = list(included_venues)
            metrics["fresh_count"] = (fresh > 0.0).sum(axis=1).astype(float)
            for metric in MARKET_METRICS:
                columns = [
                    f"{venue}_{factor}_{metric}_{suffix}"
                    for venue in included_venues
                ]
                values = pd.concat([_numeric(frame, name) for name in columns], axis=1)
                values.columns = list(included_venues)
                values = values.where(fresh > 0.0)
                if metric in {"bid_depletion", "bid_refill", "ask_depletion", "ask_refill"}:
                    aggregate = values.sum(axis=1, min_count=1)
                else:
                    aggregate = values.median(axis=1, skipna=True)
                metrics[metric] = aggregate
                output[f"xv_{factor}_{metric}_{suffix}"] = aggregate
            pressures = pd.concat(
                [
                    _numeric(frame, f"{venue}_{factor}_flow_pressure_{suffix}").where(
                        fresh[venue] > 0.0
                    )
                    for venue in included_venues
                ],
                axis=1,
            )
            nonzero = pressures.where(pressures.abs() > 1e-12)
            positives = (nonzero > 0.0).sum(axis=1)
            negatives = (nonzero < 0.0).sum(axis=1)
            denominator = nonzero.notna().sum(axis=1).replace(0, np.nan)
            agreement = pd.concat([positives, negatives], axis=1).max(axis=1) / denominator
            moves = pd.concat(
                [
                    _numeric(frame, f"{venue}_{factor}_mid_move_bps_{suffix}").where(
                        fresh[venue] > 0.0
                    )
                    for venue in included_venues
                ],
                axis=1,
            )
            move_median = moves.median(axis=1, skipna=True)
            dispersion = moves.sub(move_median, axis=0).abs().median(axis=1, skipna=True)
            output[f"xv_{factor}_fresh_count_{suffix}"] = metrics["fresh_count"]
            output[f"xv_{factor}_agreement_{suffix}"] = agreement
            output[f"xv_{factor}_dispersion_bps_{suffix}"] = dispersion
            factor_frames[factor] = metrics
        output[f"xv_perp_minus_spot_move_bps_{suffix}"] = (
            factor_frames["perp"]["mid_move_bps"]
            - factor_frames["spot"]["mid_move_bps"]
        )
        global_move = pd.concat(
            [
                factor_frames["spot"]["mid_move_bps"],
                factor_frames["perp"]["mid_move_bps"],
            ],
            axis=1,
        ).median(axis=1, skipna=True)
        global_flow = pd.concat(
            [
                factor_frames["spot"]["flow_pressure"],
                factor_frames["perp"]["flow_pressure"],
            ],
            axis=1,
        ).median(axis=1, skipna=True)
        output[f"xv_global_minus_execution_bps_{suffix}"] = global_move - _numeric(
            frame, f"execution_mid_move_bps_{suffix}"
        )
        output[f"xv_global_minus_bridge_bps_{suffix}"] = global_move - _numeric(
            frame, f"bridge_mid_move_bps_{suffix}"
        )
        output[f"xv_flow_minus_execution_{suffix}"] = global_flow - _numeric(
            frame, f"execution_flow_pressure_{suffix}"
        )
    return pd.DataFrame(output, index=frame.index)


def local_feature_columns(frame: pd.DataFrame) -> list[str]:
    columns = [
        name
        for name in frame.columns
        if name.startswith(("execution_", "bridge_"))
        and pd.api.types.is_numeric_dtype(frame[name])
    ]
    columns.extend(
        f"local_{name}"
        for name in LOCAL_CONTEXT_NUMERIC
        if f"local_{name}" in frame
    )
    columns.extend(name for name in CAMPAIGN_FEATURES if name in frame)
    return list(dict.fromkeys(columns))


def chronological_folds(
    days: list[str],
    *,
    min_train_days: int,
    test_days: int,
    embargo_days: int,
    late_days: int,
) -> list[DayFold]:
    ordered = sorted(dict.fromkeys(str(day) for day in days if str(day)))
    folds: list[DayFold] = []
    late_count = min(max(0, late_days), max(0, len(ordered) - min_train_days - embargo_days))
    development = ordered[:-late_count] if late_count else ordered
    cursor = max(1, min_train_days)
    fold = 0
    while cursor + embargo_days < len(development):
        test_start = cursor + embargo_days
        test = development[test_start : test_start + max(1, test_days)]
        if not test:
            break
        folds.append(
            DayFold(
                panel="chronological",
                fold=fold,
                train_days=tuple(development[:cursor]),
                embargo_days=tuple(development[cursor:test_start]),
                test_days=tuple(test),
            )
        )
        fold += 1
        cursor = test_start + len(test)
    if late_count:
        late_start = len(ordered) - late_count
        embargo_start = max(0, late_start - embargo_days)
        train = ordered[:embargo_start]
        if len(train) >= min_train_days:
            folds.append(
                DayFold(
                    panel="late_holdout",
                    fold=fold,
                    train_days=tuple(train),
                    embargo_days=tuple(ordered[embargo_start:late_start]),
                    test_days=tuple(ordered[late_start:]),
                )
            )
    return folds


def blocked_day_folds(
    days: list[str],
    *,
    folds: int,
    late_days: int,
) -> list[DayFold]:
    """Build day-isolated cross-fit folds on the development panel.

    Training may include dates later than a test date, so this panel is
    explicitly labelled blocked-day cross-fit rather than walk-forward OOS.
    The declared late panel is excluded entirely.
    """

    ordered = sorted(dict.fromkeys(str(day) for day in days if str(day)))
    late_count = min(max(0, late_days), len(ordered))
    development = ordered[:-late_count] if late_count else ordered
    fold_count = max(0, min(int(folds), len(development)))
    if fold_count < 2:
        return []
    output: list[DayFold] = []
    for fold in range(fold_count):
        test = tuple(development[fold::fold_count])
        test_set = set(test)
        train = tuple(day for day in development if day not in test_set)
        if not train or not test:
            continue
        output.append(
            DayFold(
                panel="blocked_day_crossfit",
                fold=fold,
                train_days=train,
                embargo_days=(),
                test_days=test,
            )
        )
    return output


def _model(kind: str):
    if kind == "regression":
        base = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scale", StandardScaler()),
                ("model", Ridge(alpha=10.0)),
            ]
        )
        return TransformedTargetRegressor(
            regressor=base,
            transformer=StandardScaler(),
        )
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
            ("model", LogisticRegression(C=0.25, max_iter=2_000)),
        ]
    )


def _usable_features(train: pd.DataFrame, columns: list[str]) -> list[str]:
    return [
        name
        for name in columns
        if name in train
        and _numeric(train, name).notna().sum() >= 2
        and _numeric(train, name).nunique(dropna=True) >= 2
    ]


def _score(
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    features: list[str],
    target: str,
    kind: str,
) -> dict[str, Any] | None:
    train_target = _numeric(train, target)
    test_target = _numeric(test, target)
    train_mask = train_target.notna()
    test_mask = test_target.notna()
    if train_mask.sum() < 2 or test_mask.sum() < 1:
        return None
    usable = _usable_features(train.loc[train_mask], features)
    if not usable:
        return None
    y_train = train_target.loc[train_mask].to_numpy(dtype=float)
    y_test = test_target.loc[test_mask].to_numpy(dtype=float)
    if kind == "classification" and (len(np.unique(y_train)) < 2 or len(np.unique(y_test)) < 2):
        return None
    model = _model(kind)
    model.fit(train.loc[train_mask, usable], y_train)
    if kind == "classification":
        prediction = model.predict_proba(test.loc[test_mask, usable])[:, 1]
        return {
            "rows": int(test_mask.sum()),
            "feature_count": len(usable),
            "brier": float(brier_score_loss(y_test, prediction)),
            "auc": float(roc_auc_score(y_test, prediction)),
        }
    prediction = np.asarray(model.predict(test.loc[test_mask, usable]), dtype=float)
    correlation = spearmanr(y_test, prediction).statistic if len(y_test) >= 3 else math.nan
    return {
        "rows": int(test_mask.sum()),
        "feature_count": len(usable),
        "mae": float(mean_absolute_error(y_test, prediction)),
        "spearman": float(correlation) if math.isfinite(float(correlation)) else math.nan,
    }


def evaluate_incremental(
    frame: pd.DataFrame,
    *,
    folds: list[DayFold],
    extreme_adverse_bps: float,
    min_train_rows: int,
    min_test_rows: int,
) -> list[dict[str, Any]]:
    working = frame.copy()
    markout_targets = sorted(
        name
        for name in working
        if name.startswith("markout_") and name.endswith("ms_bps")
    )
    targets: list[tuple[str, str]] = [(name, "regression") for name in markout_targets]
    for name in markout_targets:
        extreme = name.replace("markout_", "extreme_adverse_").replace("_bps", "")
        working[extreme] = (_numeric(working, name) <= extreme_adverse_bps).astype(float)
        working.loc[_numeric(working, name).isna(), extreme] = np.nan
        targets.append((extreme, "classification"))
    for name, kind in (
        ("campaign_terminal_pnl", "regression"),
        ("campaign_flattened", "classification"),
        ("campaign_open_risk", "classification"),
        ("campaign_tail", "classification"),
    ):
        if name in working:
            targets.append((name, kind))

    results: list[dict[str, Any]] = []
    local_columns = local_feature_columns(working)
    latency_modes = (
        sorted(working["market_data_latency_mode"].dropna().astype(str).unique())
        if "market_data_latency_mode" in working
        else ["unknown"]
    )
    venue_sets = {
        "full": EXTERNAL_VENUES,
        "leave_bitget_out": ("bybit", "okx"),
        "leave_bybit_out": ("bitget", "okx"),
        "leave_okx_out": ("bitget", "bybit"),
    }
    for venue_set_name, venues in venue_sets.items():
        consensus = build_external_consensus(working, included_venues=venues)
        candidate = pd.concat([working, consensus], axis=1)
        external_columns = list(consensus.columns)
        for latency_mode in latency_modes:
            latency_frame = (
                candidate[candidate["market_data_latency_mode"].astype(str) == latency_mode]
                if "market_data_latency_mode" in candidate
                else candidate
            )
            for side in ("ALL", "BUY", "SELL"):
                side_frame = (
                    latency_frame
                    if side == "ALL"
                    else latency_frame[latency_frame["side"] == side]
                )
                for role in ("ALL", "opener", "add", "reducing"):
                    group = (
                        side_frame
                        if role == "ALL"
                        else side_frame[side_frame["inventory_role"] == role]
                    )
                    for fold in folds:
                        train = group[group["day"].astype(str).isin(fold.train_days)]
                        test = group[group["day"].astype(str).isin(fold.test_days)]
                        if len(train) < min_train_rows or len(test) < min_test_rows:
                            continue
                        for target, kind in targets:
                            m0 = _score(
                                train,
                                test,
                                features=local_columns,
                                target=target,
                                kind=kind,
                            )
                            m1 = _score(
                                train,
                                test,
                                features=[*local_columns, *external_columns],
                                target=target,
                                kind=kind,
                            )
                            if m0 is None or m1 is None:
                                continue
                            row = {
                                "panel": fold.panel,
                                "fold": fold.fold,
                                "train_first_day": fold.train_days[0],
                                "train_last_day": fold.train_days[-1],
                                "test_first_day": fold.test_days[0],
                                "test_last_day": fold.test_days[-1],
                                "embargo_days": "|".join(fold.embargo_days),
                                "latency_mode": latency_mode,
                                "venue_set": venue_set_name,
                                "side": side,
                                "inventory_role": role,
                                "target": target,
                                "kind": kind,
                                "train_rows": len(train),
                                "test_rows": len(test),
                                **{f"m0_{key}": value for key, value in m0.items()},
                                **{f"m1_{key}": value for key, value in m1.items()},
                            }
                            if kind == "classification":
                                row["brier_improvement_m1_vs_m0"] = m0["brier"] - m1["brier"]
                                row["auc_delta_m1_vs_m0"] = m1["auc"] - m0["auc"]
                            else:
                                row["mae_improvement_m1_vs_m0"] = m0["mae"] - m1["mae"]
                                row["spearman_delta_m1_vs_m0"] = m1["spearman"] - m0["spearman"]
                            results.append(row)
    return results


def summarize(frame: pd.DataFrame, metrics: pd.DataFrame) -> dict[str, Any]:
    matched = int(_numeric(frame, "local_context_matched").fillna(0).sum())
    summary: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": "ok" if len(metrics) else "insufficient_chronological_data",
        "fills": len(frame),
        "days": int(frame["day"].astype(str).nunique()),
        "local_context_matched": matched,
        "local_context_match_rate": matched / max(len(frame), 1),
        "inventory_role_counts": frame["inventory_role"].value_counts(dropna=False).to_dict(),
        "metric_rows": len(metrics),
        "policy_effect": "none_observational_incremental_audit",
    }
    if len(metrics):
        full = metrics[metrics["venue_set"] == "full"]
        for column in (
            "mae_improvement_m1_vs_m0",
            "spearman_delta_m1_vs_m0",
            "brier_improvement_m1_vs_m0",
            "auc_delta_m1_vs_m0",
        ):
            if column in full:
                values = pd.to_numeric(full[column], errors="coerce").dropna()
                summary[f"full_median_{column}"] = float(values.median()) if len(values) else math.nan
                summary[f"full_positive_rate_{column}"] = float((values > 0.0).mean()) if len(values) else math.nan
    return summary


def render_markdown(summary: dict[str, Any], metrics: pd.DataFrame) -> str:
    lines = [
        "# Fill Toxicity Incremental M0/M1 Audit",
        "",
        f"- Status: `{summary['status']}`",
        f"- Fills: `{summary['fills']}` across `{summary['days']}` UTC days",
        f"- Local context match: `{summary['local_context_match_rate']:.2%}`",
        f"- Metric rows: `{summary['metric_rows']}`",
        "- Policy effect: `none`",
        "",
        "M0 contains only causal Binance execution/bridge flow, order context, "
        "and campaign state. M1 adds external spot/perpetual consensus rebuilt "
        "from the selected venue set. Leave-one-out never reuses the full-venue "
        "aggregate.",
        "",
    ]
    if metrics.empty:
        lines.extend(
            [
                "No chronological fold met the declared day/row denominator. "
                "This is an infrastructure smoke, not evidence for or against "
                "external information.",
                "",
            ]
        )
    else:
        lines.extend(["## Aggregate Increment", ""])
        for key, value in summary.items():
            if key.startswith("full_median_") or key.startswith("full_positive_rate_"):
                lines.append(f"- `{key}`: `{value:.6f}`")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", action="append", type=Path, required=True)
    parser.add_argument("--order-outcomes", type=Path)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--min-train-days", type=int, default=20)
    parser.add_argument("--test-days", type=int, default=5)
    parser.add_argument("--embargo-days", type=int, default=1)
    parser.add_argument("--late-days", type=int, default=4)
    parser.add_argument("--blocked-folds", type=int, default=5)
    parser.add_argument("--min-train-rows", type=int, default=100)
    parser.add_argument("--min-test-rows", type=int, default=20)
    parser.add_argument("--extreme-adverse-bps", type=float, default=-1.0)
    args = parser.parse_args()

    frame = load_frames(args.input)
    frame = match_order_outcomes(frame, args.order_outcomes)
    frame = add_inventory_campaign_state(frame)
    folds = chronological_folds(
        frame["day"].astype(str).tolist(),
        min_train_days=args.min_train_days,
        test_days=args.test_days,
        embargo_days=args.embargo_days,
        late_days=args.late_days,
    )
    folds.extend(
        blocked_day_folds(
            frame["day"].astype(str).tolist(),
            folds=args.blocked_folds,
            late_days=args.late_days,
        )
    )
    metric_rows = evaluate_incremental(
        frame,
        folds=folds,
        extreme_adverse_bps=args.extreme_adverse_bps,
        min_train_rows=args.min_train_rows,
        min_test_rows=args.min_test_rows,
    )
    metrics = pd.DataFrame(metric_rows)
    summary = summarize(frame, metrics)
    prefix = args.output_prefix.expanduser().resolve()
    prefix.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(prefix.with_suffix(".joined_fills.csv"), index=False)
    metrics.to_csv(prefix.with_suffix(".fold_metrics.csv"), index=False)
    prefix.with_suffix(".json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    prefix.with_suffix(".md").write_text(
        render_markdown(summary, metrics), encoding="utf-8"
    )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
