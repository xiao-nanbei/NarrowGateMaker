#!/usr/bin/env python3
"""Audit frozen fast-3s direction M1 against M0 and randomized actions.

The external model is research-only. Historical external trade states use a
causal one-second visibility delay; scores are joined backward to the latest
feature-ready second. This module never changes replay or live policy.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, roc_auc_score

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import (  # noqa: E402
    OPEConfig,
    evaluate_offline_policy,
    write_outputs,
)

SCHEMA_VERSION = "fast3_direction_policy_audit.v1"
PROFILES = ("m0_local_binance", "m1_external_all")
ACTION_FEATURES = (
    "side",
    "inventory_role",
    "inventory",
    "inventory_ratio",
    "campaign_age_s",
    "campaign_max_abs_qty_so_far",
    "campaign_pnl_so_far",
    "campaign_adverse_excursion_so_far",
    "toxicity",
    "markout_ema",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total",
    "mid",
    "best_bid",
    "best_ask",
    "fast3_m0_up_probability",
    "fast3_m1_up_probability",
    "fast3_adverse_probability_m0",
    "fast3_adverse_probability_m1",
    "fast3_external_adverse_delta",
)


def _load_model_bundle(model_root: Path, profile: str) -> tuple[lgb.Booster, dict[str, Any]]:
    directory = model_root / "fast1s" / profile
    metadata = json.loads((directory / "dir_3s_meta.json").read_text(encoding="utf-8"))
    if metadata.get("target") != "dir_3s":
        raise ValueError(f"unexpected target for {profile}")
    return lgb.Booster(model_file=str(directory / "dir_3s.txt")), metadata


def _score_day(
    day: str,
    cache_dir: Path,
    bundles: dict[str, tuple[lgb.Booster, dict[str, Any]]],
) -> pd.DataFrame:
    path = cache_dir / "fast1s" / f"BTCUSDC-fast1s-{day}.parquet"
    if not path.exists():
        return pd.DataFrame()
    all_features = sorted(
        {name for _, meta in bundles.values() for name in meta["feature_cols"]}
    )
    state = pd.read_parquet(path, columns=all_features)
    state = state.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    output = pd.DataFrame(index=state.index)
    for profile, (model, meta) in bundles.items():
        features = list(meta["feature_cols"])
        view = state.loc[:, features].copy()
        delay = int(meta.get("external_visibility_delay_s", 0))
        if profile == "m1_external_all" and delay > 0:
            external = [name for name in features if name.startswith("cv_external_")]
            view.loc[:, external] = view.loc[:, external].shift(delay).fillna(0.0)
            for name in external:
                if name.endswith("_source_age_ms"):
                    view[name] += delay * 1000.0
        output[f"fast3_{profile}_up_probability"] = model.predict(view)
    output["feature_ready_ts_ns"] = output.index.astype("int64")
    return output.reset_index(drop=True)


def enrich_scores(
    frame: pd.DataFrame,
    *,
    timestamp_col: str,
    cache_dir: Path,
    bundles: dict[str, tuple[lgb.Booster, dict[str, Any]]],
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    source = frame.copy()
    source["day"] = source["day"].astype(str).str.slice(0, 10)
    source["_event_ts_ns"] = pd.to_numeric(source[timestamp_col], errors="coerce")
    if source["_event_ts_ns"].dropna().median() < 1e15:
        source["_event_ts_ns"] *= 1_000_000_000.0
    source = source[source["_event_ts_ns"].notna()].copy()
    source["_event_ts_ns"] = source["_event_ts_ns"].round().astype("int64")
    for day, day_rows in source.groupby("day", sort=True):
        states = _score_day(day, cache_dir, bundles)
        if states.empty:
            continue
        ordered = day_rows.sort_values("_event_ts_ns").copy()
        merged = pd.merge_asof(
            ordered,
            states.sort_values("feature_ready_ts_ns"),
            left_on="_event_ts_ns",
            right_on="feature_ready_ts_ns",
            direction="backward",
            tolerance=2_000_000_000,
        )
        rows.append(merged)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True, sort=False)
    out = out.rename(
        columns={
            "fast3_m0_local_binance_up_probability": "fast3_m0_up_probability",
            "fast3_m1_external_all_up_probability": "fast3_m1_up_probability",
        }
    )
    side = out["side"].astype(str).str.upper()
    out["fast3_adverse_probability_m0"] = np.where(
        side.eq("BUY"), 1.0 - out["fast3_m0_up_probability"], out["fast3_m0_up_probability"]
    )
    out["fast3_adverse_probability_m1"] = np.where(
        side.eq("BUY"), 1.0 - out["fast3_m1_up_probability"], out["fast3_m1_up_probability"]
    )
    out["fast3_external_adverse_delta"] = (
        out["fast3_adverse_probability_m1"] - out["fast3_adverse_probability_m0"]
    )
    return out


def _safe_auc(y: pd.Series, score: pd.Series) -> float:
    valid = y.notna() & score.notna()
    return float(roc_auc_score(y[valid], score[valid])) if y[valid].nunique() == 2 else math.nan


def outcome_metrics(frame: pd.DataFrame, split: dict[str, list[str]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    filled = frame[pd.to_numeric(frame.get("filled", 0), errors="coerce").fillna(0).gt(0)].copy()
    for panel in ("validation", "test", "late"):
        panel_rows = filled[filled["day"].isin(split.get(panel, []))]
        for side in ("ALL", "BUY", "SELL"):
            scoped = panel_rows if side == "ALL" else panel_rows[panel_rows["side"].eq(side)]
            if scoped.empty:
                continue
            row: dict[str, Any] = {"panel": panel, "side": side, "fills": len(scoped)}
            markout = pd.to_numeric(scoped.get("markout_30s_bps"), errors="coerce")
            toxic = (markout < 0.0).where(markout.notna())
            tail = (markout <= -2.5).where(markout.notna())
            terminal = pd.to_numeric(scoped.get("terminal_final_total_pnl_delta"), errors="coerce")
            campaign_source = scoped.assign(_terminal=terminal)
            campaign_source = campaign_source[
                campaign_source.get("campaign_id", pd.Series(index=scoped.index)).notna()
            ].copy()
            campaign_count = 0
            campaign_stats: dict[str, float] = {}
            if not campaign_source.empty:
                campaign_source["campaign_id"] = campaign_source["campaign_id"].astype(str)
                campaign_groups = campaign_source.groupby(["day", "campaign_id"], sort=False)
                campaign_count = campaign_groups.ngroups
                campaign_frame = campaign_groups.agg(
                    terminal_pnl=("_terminal", "last"),
                    m0_adverse=("fast3_adverse_probability_m0", "max"),
                    m1_adverse=("fast3_adverse_probability_m1", "max"),
                ).reset_index()
                for profile in ("m0", "m1"):
                    campaign_stats[profile] = float(
                        campaign_frame["terminal_pnl"].corr(
                            campaign_frame[f"{profile}_adverse"], method="spearman"
                        )
                    )
            row["campaigns"] = campaign_count
            for profile in ("m0", "m1"):
                adverse = pd.to_numeric(scoped[f"fast3_adverse_probability_{profile}"], errors="coerce")
                row[f"{profile}_toxicity_auc"] = _safe_auc(toxic, adverse)
                row[f"{profile}_tail_auc"] = _safe_auc(tail, adverse)
                valid = markout.notna() & adverse.notna()
                row[f"{profile}_markout_spearman"] = float(markout[valid].corr(adverse[valid], method="spearman"))
                row[f"{profile}_campaign_pnl_spearman"] = campaign_stats.get(
                    profile, math.nan
                )
                row[f"{profile}_toxicity_brier"] = float(
                    brier_score_loss(toxic[toxic.notna()].astype(int), adverse[toxic.notna()])
                ) if toxic.notna().any() else math.nan
            for metric in ("toxicity_auc", "tail_auc"):
                row[f"delta_{metric}"] = row[f"m1_{metric}"] - row[f"m0_{metric}"]
            # More negative correlation means adverse probability ranks worse outcomes better.
            for metric in ("markout_spearman", "campaign_pnl_spearman"):
                row[f"delta_{metric}"] = row[f"m0_{metric}"] - row[f"m1_{metric}"]
            row["delta_toxicity_brier"] = row["m0_toxicity_brier"] - row["m1_toxicity_brier"]
            rows.append(row)
    return pd.DataFrame(rows)


def _candidate_actions(frame: pd.DataFrame, family: str) -> pd.Series:
    adverse = frame["fast3_adverse_probability_m1"]
    delta = frame["fast3_external_adverse_delta"]
    candidate = pd.Series("baseline", index=frame.index, dtype="string")
    if family == "adverse_widen":
        candidate.loc[(adverse >= 0.55) & (delta > 0.0)] = "widen_1tick"
    elif family == "adverse_recenter":
        candidate.loc[(adverse >= 0.55) & (delta > 0.0)] = "recenter_1tick"
    elif family == "favorable_keep":
        candidate.loc[(adverse <= 0.45) & (delta < 0.0)] = "prevent_over_widen"
    else:
        raise ValueError(f"unknown policy family: {family}")
    return candidate


def action_uplift(
    panel: pd.DataFrame,
    *,
    output_prefix: Path,
    split: dict[str, list[str]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    registry_path = output_prefix.parent / f"{output_prefix.name}.feature_registry.json"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        json.dumps(
            {
                "features": [
                    {
                        "name": name,
                        "kind": (
                            "categorical"
                            if name in {"side", "inventory_role"}
                            else "numeric"
                        ),
                        "available_at": "decision",
                        "description": (
                            "Frozen causal fast3 score using latest feature-ready second."
                            if name.startswith("fast3_")
                            else "Replay decision-time state."
                        ),
                    }
                    for name in ACTION_FEATURES
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    terminal = pd.to_numeric(panel["terminal_campaign_pnl"], errors="coerce")
    intervention_count = pd.to_numeric(panel["intervention_fill_count"], errors="coerce").fillna(0.0)
    markout_sum = pd.to_numeric(
        panel["fill_markout_30s_qty_weighted_bps_sum"], errors="coerce"
    ).fillna(0.0)
    targets = {
        "reward": pd.to_numeric(panel["reward"], errors="coerce"),
        "campaign_terminal": terminal,
        "campaign_tail_avoidance": -(terminal <= -5.0).astype(float),
        "toxic_fill_avoidance": -((intervention_count > 0.0) & (markout_sum < 0.0)).astype(float),
    }
    for family in ("adverse_widen", "adverse_recenter", "favorable_keep"):
        for target_name, target in targets.items():
            candidate = panel.copy()
            candidate["ope_target"] = target
            candidate["candidate_action"] = _candidate_actions(candidate, family)
            ope_rows, folds, actions, summary = evaluate_offline_policy(
                candidate,
                feature_names=ACTION_FEATURES,
                feature_registry_path=registry_path,
                config=OPEConfig(
                    reward_col="ope_target",
                    split_mode="chronological",
                    min_train_days=30,
                    test_days=10,
                    embargo_days=1,
                    min_train_rows=500,
                    min_action_rows=50,
                    min_effective_sample_size=50.0,
                    bootstrap_trials=500,
                    random_seed=20260713,
                ),
            )
            write_outputs(
                output_prefix.parent / f"{output_prefix.name}_{family}_{target_name}",
                ope_rows,
                folds,
                actions,
                summary,
            )
            for panel_name in ("validation", "test", "late"):
                scoped = ope_rows[ope_rows["day"].isin(split.get(panel_name, []))]
                valid = scoped[
                    pd.to_numeric(scoped["ope_prediction_valid"], errors="coerce").eq(1)
                ].copy()
                uplift = pd.to_numeric(valid["ope_dr_value"], errors="coerce") - pd.to_numeric(
                    valid["_ope_reward"], errors="coerce"
                )
                weights = pd.to_numeric(
                    valid["ope_clipped_importance_weight"], errors="coerce"
                ).fillna(0.0)
                ess = (
                    float(weights.sum() ** 2 / np.square(weights).sum())
                    if np.square(weights).sum() > 0
                    else 0.0
                )
                daily = valid.assign(_uplift=uplift).groupby("day")["_uplift"].mean()
                rng = np.random.default_rng(20260713)
                if len(daily):
                    samples = np.asarray(
                        [
                            daily.iloc[rng.integers(0, len(daily), len(daily))].mean()
                            for _ in range(1_000)
                        ]
                    )
                    p025, p975 = np.quantile(samples, [0.025, 0.975])
                else:
                    p025 = p975 = math.nan
                rows.append(
                    {
                        "family": family,
                        "target": target_name,
                        "panel": panel_name,
                        "rows": len(scoped),
                        "candidate_rows": int(
                            (_candidate_actions(scoped, family) != "baseline").sum()
                        ),
                        "dr_uplift": (
                            float(uplift.mean()) if uplift.notna().any() else math.nan
                        ),
                        "uplift_p025": float(p025),
                        "uplift_p975": float(p975),
                        "ess": ess,
                        "daily_positive": int((daily > 0).sum()),
                        "daily_total": len(daily),
                        "formal_support": bool(len(scoped) > 0 and ess >= 50.0),
                    }
                )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--model-root", type=Path, required=True)
    parser.add_argument("--order-glob", required=True)
    parser.add_argument("--action-panel", type=Path, required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    args = parser.parse_args()
    bundles = {profile: _load_model_bundle(args.model_root, profile) for profile in PROFILES}
    split = next(iter(bundles.values()))[1]["split"]
    cache_dir = args.data_dir / "model_features" / "external_venue_trade_state.v2"

    order_paths = sorted(args.data_dir.glob(args.order_glob))
    orders = pd.concat(
        [pd.read_csv(path, low_memory=False) for path in order_paths],
        ignore_index=True,
        sort=False,
    )
    orders["side"] = orders["side"].astype(str).str.upper()
    enriched_orders = enrich_scores(
        orders, timestamp_col="timestamp", cache_dir=cache_dir, bundles=bundles
    )
    metrics = outcome_metrics(enriched_orders, split)

    actions = pd.read_csv(args.action_panel)
    actions["side"] = actions["side"].astype(str).str.upper()
    enriched_actions = enrich_scores(
        actions, timestamp_col="decision_ts_ns", cache_dir=cache_dir, bundles=bundles
    )
    uplift = action_uplift(enriched_actions, output_prefix=args.output_prefix, split=split)

    args.output_prefix.parent.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(args.output_prefix.with_suffix(".outcomes.csv"), index=False)
    uplift.to_csv(args.output_prefix.with_suffix(".action_uplift.csv"), index=False)
    enriched_actions.to_csv(args.output_prefix.with_suffix(".action_panel.csv"), index=False)
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "order_rows": len(enriched_orders),
        "filled_rows": int(pd.to_numeric(enriched_orders.get("filled", 0), errors="coerce").fillna(0).gt(0).sum()),
        "action_rows": len(enriched_actions),
        "model_root": str(args.model_root),
        "external_visibility_delay_s": bundles["m1_external_all"][1].get("external_visibility_delay_s"),
        "split": split,
    }
    args.output_prefix.with_suffix(".json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
