#!/usr/bin/env python3
"""Run the owner-path sparse conditional-P3 terminal-value diagnostic.

This is a full-information Development proxy diagnostic.  It reuses the
frozen F06 baseline-terminal overlay and never represents that overlay as a
regenerated assignment-to-washout action path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

from data_paths import relocate_marketdata_path
from research.families.f06_placement_fill_cif.audit import (
    placement_marginal_fill_value_feasibility as marginal_value,
)


IDENTITY = "conditional_p3_joint_quote_sparse_value_diagnostic_v1"
SPEC_SCHEMA_VERSION = "conditional_p3_joint_quote_sparse_value_diagnostic.v1.spec"
REPORT_SCHEMA_VERSION = "conditional_p3_joint_quote_sparse_value_diagnostic.v1.report"
GRID_ACTIONS = (
    "closer_4tick",
    "closer_2tick",
    "closer_1tick",
    "current",
    "farther_1tick",
    "farther_2tick",
    "farther_4tick",
)
JOINT_ACTIONS = (
    "baseline__BUY_current__SELL_current",
    "BUY_closer_1tick__SELL_current",
    "BUY_farther_1tick__SELL_current",
    "BUY_closer_2tick__SELL_current",
    "BUY_farther_2tick__SELL_current",
    "BUY_closer_4tick__SELL_current",
    "BUY_farther_4tick__SELL_current",
    "SELL_closer_1tick__BUY_current",
    "SELL_farther_1tick__BUY_current",
    "SELL_closer_2tick__BUY_current",
    "SELL_farther_2tick__BUY_current",
    "SELL_closer_4tick__BUY_current",
    "SELL_farther_4tick__BUY_current",
)
BASELINE_ACTION = JOINT_ACTIONS[0]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _canonical_identity(payload: Mapping[str, Any], field: str) -> str:
    normalized = dict(payload)
    normalized.pop(field, None)
    return canonical_sha256(normalized)


def _identity(path: Path) -> dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "sha256": sha256_file(path),
        "size_bytes": int(path.stat().st_size),
    }


def _require_identity(identity: Mapping[str, Any], *, label: str) -> Path:
    raw = Path(str(identity["path"])).expanduser()
    path = relocate_marketdata_path(raw).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"{label} missing: {path}")
    if sha256_file(path) != str(identity["sha256"]):
        raise ValueError(f"{label} SHA256 changed")
    if "size_bytes" in identity and path.stat().st_size != int(identity["size_bytes"]):
        raise ValueError(f"{label} size changed")
    return path


def _action_definition(action: str) -> tuple[str | None, str, int, int]:
    if action == BASELINE_ACTION:
        return None, "current", 0, 0
    side, grid_action = action.split("__", 1)[0].split("_", 1)
    direction, gap_text = grid_action.rsplit("_", 1)
    gap = int(gap_text.removesuffix("tick"))
    distance_delta = -gap if direction == "closer" else gap
    return side, grid_action, distance_delta if side == "BUY" else 0, (
        distance_delta if side == "SELL" else 0
    )


def _load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SPEC_SCHEMA_VERSION:
        raise ValueError("unsupported sparse value diagnostic schema")
    field = "canonical_spec_identity_sha256"
    if _canonical_identity(spec, field) != spec.get(field):
        raise ValueError("sparse value diagnostic canonical identity mismatch")
    for label, identity in spec["identities"].items():
        _require_identity(identity, label=label)
    if tuple(spec["development_days"]) != tuple(sorted(spec["development_days"])):
        raise ValueError("Development days must be chronological")
    if len(spec["development_days"]) != 28:
        raise ValueError("owner diagnostic must preserve the 28-day denominator")
    if tuple(spec["joint_actions"]) != JOINT_ACTIONS:
        raise ValueError("joint action set changed")
    if float(spec["evaluation"]["economic_epsilon_usdc"]) < 0.0:
        raise ValueError("economic epsilon must be non-negative")
    permissions = spec["permissions"]
    for field in (
        "validation_read",
        "sealed_holdout_read",
        "action_authorized",
        "live_authorized",
    ):
        if bool(permissions.get(field, False)):
            raise ValueError(f"sparse diagnostic cannot grant {field}")
    if not bool(permissions.get("development_economic_outcomes_read", False)):
        raise ValueError("owner diagnostic must disclose its economic read")
    return spec


def _paired_support(side_support: pd.DataFrame) -> pd.DataFrame:
    counts = side_support.groupby(["day", "decision_ts_ns"], observed=True)["side"].nunique()
    keys = counts.loc[counts.eq(2)].index
    paired = side_support.set_index(["day", "decision_ts_ns"]).loc[keys].reset_index()
    if paired.duplicated(["day", "decision_ts_ns", "side"]).any():
        raise ValueError("paired side support is not unique")
    if len(keys) != 282 or len(paired) != 564:
        raise ValueError("owner diagnostic denominator changed")
    if set(paired["side"].astype(str)) != {"BUY", "SELL"}:
        raise ValueError("paired support must contain BUY and SELL")
    return paired.sort_values(["day", "decision_ts_ns", "side"], kind="stable")


def _load_valued_actions(
    *,
    day: str,
    cohort_ids: set[str],
    partition_root: Path,
    f06_spec: Mapping[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    manifest_path = partition_root / "partitions" / f"day={day}" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if str(manifest.get("day")) != day:
        raise ValueError(f"F06 value partition day mismatch: {day}")
    source_path = _require_identity(manifest["source_panel"], label=f"{day} source panel")
    mechanics_path = _require_identity(
        manifest["paired_mechanics"], label=f"{day} paired mechanics"
    )
    bbo_path = _require_identity(manifest["bbo"], label=f"{day} BBO")
    source_columns = [
        "cohort_id",
        "day",
        "side",
        "inventory_role",
        "campaign_id",
        "submit_ts_ns",
        "campaign_age_s",
        "inventory",
        "mid",
        "campaign_pnl_so_far",
        "cancel_request_reason",
        "current__first_fill_ts_ns",
        "current__fill_qty",
        "current__price_tick",
    ]
    source = pd.read_parquet(source_path, columns=source_columns)
    actions = pd.read_parquet(mechanics_path)
    actions = actions.loc[actions["cohort_id"].astype(str).isin(cohort_ids)].copy()
    bbo_ts, bbo_mid = marginal_value._load_bbo(
        bbo_path,
        expected_sha256=str(manifest["bbo"]["sha256"]),
    )
    starts = marginal_value._campaign_start_table(source)
    mapping = marginal_value._map_cohorts_to_campaigns(
        source,
        starts,
        opener_tolerance_ms=float(
            f06_spec["campaign_overlay"]["opener_start_match_tolerance_ms"]
        ),
    )
    terminals = marginal_value.reconstruct_campaign_terminals(
        source,
        bbo_ts_ms=bbo_ts,
        bbo_mid=bbo_mid,
        max_bbo_age_ms=float(f06_spec["market_marks"]["max_bbo_age_ms"]),
    )
    context = source.loc[:, ["cohort_id"]].merge(
        mapping,
        on="cohort_id",
        validate="one_to_one",
    )
    valued = marginal_value._value_actions(
        actions,
        context,
        terminals,
        bbo_ts_ms=bbo_ts,
        bbo_mid=bbo_mid,
        max_bbo_age_ms=float(f06_spec["market_marks"]["max_bbo_age_ms"]),
        common_clock_ms=int(f06_spec["common_clock"]["clock_ms"]),
    )
    if valued.duplicated(["cohort_id", "action"]).any():
        raise ValueError(f"{day} valued action identity is not unique")
    expected_rows = len(cohort_ids) * len(GRID_ACTIONS)
    if len(valued) != expected_rows:
        raise ValueError(f"{day} valued action grid is incomplete")
    audit = {
        "day": day,
        "partition_manifest": _identity(manifest_path),
        "source_panel": _identity(source_path),
        "paired_mechanics": _identity(mechanics_path),
        "bbo": _identity(bbo_path),
        "cohort_ids": int(len(cohort_ids)),
        "valued_action_rows": int(len(valued)),
    }
    return valued, audit


def _build_potential_outcomes(
    paired: pd.DataFrame,
    *,
    valued_by_day: Mapping[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    bucket_audit: list[dict[str, Any]] = []
    for (day, decision_ts_ns), bucket in paired.groupby(
        ["day", "decision_ts_ns"], observed=True, sort=True
    ):
        side_rows = {str(row.side): row for row in bucket.itertuples(index=False)}
        valued = valued_by_day[str(day)].set_index(["cohort_id", "action"])
        side_values: dict[str, dict[str, Any]] = {}
        complete = True
        for side in ("BUY", "SELL"):
            source = side_rows[side]
            cohort_id = str(source.cohort_id)
            action_values: dict[str, Any] = {}
            for grid_action in GRID_ACTIONS:
                try:
                    record = valued.loc[(cohort_id, grid_action)]
                except KeyError as exc:
                    raise ValueError("valued action grid changed") from exc
                overlay = float(record["campaign_terminal_overlay_usdc"])
                supported = bool(record["campaign_terminal_overlay_supported"])
                if not supported or not np.isfinite(overlay):
                    complete = False
                action_values[grid_action] = {
                    "overlay": overlay,
                    "filled": bool(record["filled"]),
                    "fill_qty": float(record["fill_qty"]),
                }
            side_values[side] = action_values
        bucket_id = f"{day}:{int(decision_ts_ns)}"
        bucket_audit.append(
            {
                "day": str(day),
                "decision_ts_ns": int(decision_ts_ns),
                "bucket_id": bucket_id,
                "complete_terminal_overlay": bool(complete),
            }
        )
        if not complete:
            continue

        buy = side_rows["BUY"]
        sell = side_rows["SELL"]
        for action in JOINT_ACTIONS:
            changed_side, grid_action, buy_delta, sell_delta = _action_definition(action)
            buy_action = grid_action if changed_side == "BUY" else "current"
            sell_action = grid_action if changed_side == "SELL" else "current"
            buy_increment = (
                side_values["BUY"][buy_action]["overlay"]
                - side_values["BUY"]["current"]["overlay"]
            )
            sell_increment = (
                side_values["SELL"][sell_action]["overlay"]
                - side_values["SELL"]["current"]["overlay"]
            )
            target = float(buy_increment + sell_increment)
            buy_candidate_p = float(getattr(buy, f"{buy_action}__p_touch"))
            sell_candidate_p = float(getattr(sell, f"{sell_action}__p_touch"))
            rows.append(
                {
                    "day": str(day),
                    "decision_ts_ns": int(decision_ts_ns),
                    "feature_ready_ts_ns": int(
                        max(buy.feature_ready_ts_ns, sell.feature_ready_ts_ns)
                    ),
                    "bucket_id": bucket_id,
                    "action": action,
                    "changed_side": changed_side or "NONE",
                    "buy_role": str(buy.inventory_role),
                    "sell_role": str(sell.inventory_role),
                    "spread_ticks": int(buy.best_ask_ticks - buy.best_bid_ticks),
                    "buy_distance_delta_ticks": int(buy_delta),
                    "sell_distance_delta_ticks": int(sell_delta),
                    "buy_current_p_touch": float(buy.current__p_touch),
                    "sell_current_p_touch": float(sell.current__p_touch),
                    "buy_candidate_p_touch": buy_candidate_p,
                    "sell_candidate_p_touch": sell_candidate_p,
                    "buy_p_touch_delta": buy_candidate_p - float(buy.current__p_touch),
                    "sell_p_touch_delta": sell_candidate_p - float(sell.current__p_touch),
                    "incremental_terminal_overlay_usdc": target,
                    "candidate_filled_qty_btc": float(
                        side_values["BUY"][buy_action]["fill_qty"]
                        + side_values["SELL"][sell_action]["fill_qty"]
                    ),
                    "baseline_filled_qty_btc": float(
                        side_values["BUY"]["current"]["fill_qty"]
                        + side_values["SELL"]["current"]["fill_qty"]
                    ),
                }
            )
    panel = pd.DataFrame(rows)
    audit = pd.DataFrame(bucket_audit)
    if panel.empty:
        raise ValueError("no complete terminal-overlay buckets remain")
    if panel.duplicated(["bucket_id", "action"]).any():
        raise ValueError("potential-outcome panel is not unique")
    counts = panel.groupby("bucket_id", observed=True)["action"].nunique()
    if not counts.eq(len(JOINT_ACTIONS)).all():
        raise ValueError("potential-outcome action set is incomplete")
    if (panel["feature_ready_ts_ns"] > panel["decision_ts_ns"]).any():
        raise ValueError("future P3 feature time entered the value panel")
    return panel, audit


def _feature_matrix(panel: pd.DataFrame) -> pd.DataFrame:
    numeric = panel.loc[
        :,
        [
            "spread_ticks",
            "buy_distance_delta_ticks",
            "sell_distance_delta_ticks",
            "buy_current_p_touch",
            "sell_current_p_touch",
            "buy_candidate_p_touch",
            "sell_candidate_p_touch",
            "buy_p_touch_delta",
            "sell_p_touch_delta",
        ],
    ].astype(float)
    categories = pd.get_dummies(
        panel.loc[:, ["action", "buy_role", "sell_role"]],
        columns=["action", "buy_role", "sell_role"],
        dtype=float,
    )
    expected = [f"action_{action}" for action in JOINT_ACTIONS]
    categories = categories.reindex(
        columns=[
            *expected,
            "buy_role_add",
            "buy_role_opener",
            "buy_role_reducing",
            "sell_role_add",
            "sell_role_opener",
            "sell_role_reducing",
        ],
        fill_value=0.0,
    )
    return pd.concat([numeric.reset_index(drop=True), categories.reset_index(drop=True)], axis=1)


def _folds(days: tuple[str, ...], spec: Mapping[str, Any]) -> list[dict[str, Any]]:
    config = spec["evaluation"]
    cursor = int(config["min_train_days"])
    embargo = int(config["embargo_days"])
    test_days = int(config["test_days"])
    folds: list[dict[str, Any]] = []
    while cursor + embargo < len(days):
        start = cursor + embargo
        stop = min(len(days), start + test_days)
        folds.append(
            {
                "fold": len(folds),
                "train_days": days[:cursor],
                "embargo_days": days[cursor:start],
                "test_days": days[start:stop],
            }
        )
        cursor = stop
    if len(folds) != 3:
        raise ValueError("owner diagnostic must preserve three expanding OOF folds")
    return folds


def _day_cluster_matrix(
    frame: pd.DataFrame,
    *,
    value_column: str,
    samples: int,
    seed: int,
) -> tuple[pd.Series, np.ndarray, tuple[str, ...]]:
    pivot = frame.pivot(index=["day", "bucket_id"], columns="action", values=value_column)
    pivot = pivot.reindex(columns=JOINT_ACTIONS)
    if pivot.isna().any().any():
        raise ValueError("candidate matrix contains missing outcomes")
    daily_sum = pivot.groupby(level="day").sum()
    daily_count = pivot.groupby(level="day").size().astype(float)
    point = daily_sum.sum(axis=0) / float(daily_count.sum())
    rng = np.random.default_rng(int(seed))
    draw = rng.integers(0, len(daily_sum), size=(int(samples), len(daily_sum)))
    sums = daily_sum.to_numpy(dtype=float)
    counts = daily_count.to_numpy(dtype=float)
    bootstrap = sums[draw].sum(axis=1) / counts[draw].sum(axis=1)[:, None]
    return point, bootstrap, tuple(pivot.columns.astype(str))


def _screen_actions(
    train: pd.DataFrame,
    *,
    spec: Mapping[str, Any],
    fold: int,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    config = spec["evaluation"]
    point, bootstrap, actions = _day_cluster_matrix(
        train,
        value_column="incremental_terminal_overlay_usdc",
        samples=int(config["bootstrap_samples"]),
        seed=int(config["random_seed"]) + 1_000 + int(fold),
    )
    point_array = point.to_numpy(dtype=float)
    max_deviation = np.max(np.abs(bootstrap - point_array[None, :]), axis=1)
    critical = float(np.quantile(max_deviation, float(config["confidence"])))
    lower = point_array - critical
    upper = point_array + critical
    epsilon = float(config["economic_epsilon_usdc"])
    result = pd.DataFrame(
        {
            "fold": int(fold),
            "action": actions,
            "train_mean_usdc": point_array,
            "simultaneous_lcb_usdc": lower,
            "simultaneous_ucb_usdc": upper,
            "simultaneous_critical_usdc": critical,
            "supported": lower > epsilon,
        }
    )
    result.loc[result["action"].eq(BASELINE_ACTION), "supported"] = False
    return result, tuple(result.loc[result["supported"], "action"].astype(str))


def _bootstrap_policy(oof: pd.DataFrame, spec: Mapping[str, Any]) -> dict[str, Any]:
    config = spec["evaluation"]
    daily = oof.groupby("day", observed=True)["selected_value_usdc"].agg(["sum", "count"])
    point = float(daily["sum"].sum() / daily["count"].sum())
    rng = np.random.default_rng(int(config["random_seed"]) + 90_000)
    draw = rng.integers(
        0,
        len(daily),
        size=(int(config["bootstrap_samples"]), len(daily)),
    )
    sums = daily["sum"].to_numpy(dtype=float)
    counts = daily["count"].to_numpy(dtype=float)
    samples = sums[draw].sum(axis=1) / counts[draw].sum(axis=1)
    alpha = 1.0 - float(config["confidence"])
    return {
        "mean_usdc_per_bucket": point,
        "lower_usdc_per_bucket": float(np.quantile(samples, alpha / 2.0)),
        "upper_usdc_per_bucket": float(np.quantile(samples, 1.0 - alpha / 2.0)),
        "oof_days": int(len(daily)),
        "oof_buckets": int(len(oof)),
    }


def _evaluate(panel: pd.DataFrame, spec: Mapping[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    days = tuple(spec["development_days"])
    features = _feature_matrix(panel)
    feature_names = tuple(features.columns)
    working = panel.reset_index(drop=True).copy()
    working["_row"] = np.arange(len(working), dtype=np.int64)
    selection_parts: list[pd.DataFrame] = []
    oof_rows: list[dict[str, Any]] = []
    chronology: list[dict[str, Any]] = []
    for fold in _folds(days, spec):
        train = working.loc[working["day"].isin(fold["train_days"])].copy()
        test = working.loc[working["day"].isin(fold["test_days"])].copy()
        screen, supported = _screen_actions(train, spec=spec, fold=int(fold["fold"]))
        selection_parts.append(screen)
        scaler = StandardScaler()
        train_x = scaler.fit_transform(features.iloc[train["_row"].to_numpy(dtype=int)])
        train_y = train["incremental_terminal_overlay_usdc"].to_numpy(dtype=float)
        model = Ridge(alpha=float(spec["model"]["ridge_alpha"]), fit_intercept=True)
        model.fit(train_x, train_y, sample_weight=np.repeat(1.0 / len(JOINT_ACTIONS), len(train)))
        test_x = scaler.transform(features.iloc[test["_row"].to_numpy(dtype=int)])
        test = test.copy()
        test["q_hat_usdc"] = model.predict(test_x)
        for bucket_id, bucket in test.groupby("bucket_id", observed=True, sort=False):
            candidate = bucket.loc[bucket["action"].isin(supported)].copy()
            if candidate.empty:
                chosen = bucket.loc[bucket["action"].eq(BASELINE_ACTION)].iloc[0]
            else:
                candidate.sort_values(["q_hat_usdc", "action"], ascending=[False, True], inplace=True)
                best = candidate.iloc[0]
                if float(best["q_hat_usdc"]) <= float(
                    spec["evaluation"]["economic_epsilon_usdc"]
                ):
                    chosen = bucket.loc[bucket["action"].eq(BASELINE_ACTION)].iloc[0]
                else:
                    chosen = best
            oof_rows.append(
                {
                    "fold": int(fold["fold"]),
                    "day": str(chosen["day"]),
                    "bucket_id": str(bucket_id),
                    "selected_action": str(chosen["action"]),
                    "selected_q_hat_usdc": float(chosen["q_hat_usdc"]),
                    "selected_value_usdc": float(
                        chosen["incremental_terminal_overlay_usdc"]
                    ),
                    "policy_is_baseline": bool(chosen["action"] == BASELINE_ACTION),
                    "supported_actions": list(supported),
                }
            )
        chronology.append(
            {
                "fold": int(fold["fold"]),
                "train_min_day": min(fold["train_days"]),
                "train_max_day": max(fold["train_days"]),
                "embargo_days": list(fold["embargo_days"]),
                "test_min_day": min(fold["test_days"]),
                "test_max_day": max(fold["test_days"]),
                "future_training_leakage": False,
            }
        )
    oof = pd.DataFrame(oof_rows)
    selection = pd.concat(selection_parts, ignore_index=True)
    policy = _bootstrap_policy(oof, spec)
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "decision": (
            "owner_proxy_signal_requires_regenerated_full_path_successor"
            if policy["lower_usdc_per_bucket"]
            > float(spec["evaluation"]["economic_epsilon_usdc"])
            else "owner_proxy_signal_not_supported_stop_before_action_identity"
        ),
        "hard_gate_path": {
            "predecessor_passed": False,
            "result_rewritten": False,
        },
        "owner_progression_path": {
            "continued": True,
            "promotion_route": "owner_risk_accepted_promotion",
        },
        "estimand": {
            "target": "single_side_baseline_terminal_overlay_delta_usdc",
            "unit": "USDC_per_canonical_joint_quote_bucket",
            "quantity_weighted": True,
            "p3_multiplied_outside_value_model": False,
            "full_information_counterfactual_overlay": True,
            "full_regenerated_path": False,
            "cross_side_interactions_identified": False,
            "action_authority_eligible": False,
        },
        "support": {
            "preflight_paired_buckets": 282,
            "complete_value_buckets": int(panel["bucket_id"].nunique()),
            "potential_outcome_rows": int(len(panel)),
            "development_days": int(panel["day"].nunique()),
            "outer_oof_days": int(oof["day"].nunique()),
            "outer_oof_buckets": int(len(oof)),
            "folds": chronology,
        },
        "model": {
            "family": "fixed_ridge_full_information_proxy_Q",
            "ridge_alpha": float(spec["model"]["ridge_alpha"]),
            "feature_columns": list(feature_names),
            "hyperparameter_search": False,
        },
        "selection": {
            "method": "past_only_day_cluster_simultaneous_screen_then_OOF_argmax",
            "economic_epsilon_usdc": float(
                spec["evaluation"]["economic_epsilon_usdc"]
            ),
            "policy": policy,
            "baseline_rate": float(oof["policy_is_baseline"].mean()),
            "supported_action_fold_count": int(selection["supported"].sum()),
        },
        "permissions": spec["permissions"],
    }
    return oof, selection, report


def _atomic_output(
    *,
    output_dir: Path,
    panel: pd.DataFrame,
    bucket_audit: pd.DataFrame,
    day_audit: pd.DataFrame,
    oof: pd.DataFrame,
    selection: pd.DataFrame,
    report: Mapping[str, Any],
) -> None:
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f"{output_dir.name}.", dir=output_dir.parent))
    try:
        files: dict[str, dict[str, Any]] = {}
        for name, frame in (
            ("potential_outcomes.parquet", panel),
            ("bucket_audit.parquet", bucket_audit),
            ("day_audit.parquet", day_audit),
            ("oof_policy.parquet", oof),
            ("selection_evidence.parquet", selection),
        ):
            path = stage / name
            frame.to_parquet(path, index=False, compression="zstd")
            files[name] = _identity(path)
            files[name]["path"] = name
        report_path = stage / "report.json"
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        files["report.json"] = _identity(report_path)
        files["report.json"]["path"] = "report.json"
        manifest = {
            "schema_version": "conditional_p3_joint_quote_sparse_value_diagnostic.v1.output_manifest",
            "identity": IDENTITY,
            "files": files,
        }
        (stage / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (stage / "COMPLETE").write_text(canonical_sha256(manifest) + "\n", encoding="ascii")
        if output_dir.exists():
            shutil.rmtree(output_dir)
        os.replace(stage, output_dir)
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def run(spec_path: Path, output_dir: Path) -> dict[str, Any]:
    spec_path = spec_path.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    spec = _load_spec(spec_path)
    owner_result_path = _require_identity(
        spec["identities"]["owner_progression_result"],
        label="owner progression result",
    )
    owner_result = json.loads(owner_result_path.read_text(encoding="utf-8"))
    if owner_result.get("decision") != (
        "owner_progression_authorized_sparse_development_value_diagnostic"
    ):
        raise ValueError("owner progression does not authorize this diagnostic")
    if bool(owner_result["hard_gate_path"]["passed"]):
        raise ValueError("owner progression unexpectedly rewrote the hard gate")

    side_path = _require_identity(
        spec["identities"]["preflight_side_support"],
        label="preflight side support",
    )
    paired = _paired_support(pd.read_parquet(side_path))
    f06_report_path = _require_identity(
        spec["identities"]["f06_value_report"],
        label="F06 value report",
    )
    f06_report = json.loads(f06_report_path.read_text(encoding="utf-8"))
    f06_spec_path = _require_identity(
        f06_report["input_identities"]["spec"],
        label="F06 value Spec",
    )
    f06_spec = json.loads(f06_spec_path.read_text(encoding="utf-8"))
    partition_root = f06_report_path.parent

    valued_by_day: dict[str, pd.DataFrame] = {}
    day_audit: list[dict[str, Any]] = []
    for day in spec["development_days"]:
        day_rows = paired.loc[paired["day"].astype(str).eq(str(day))]
        cohort_ids = set(day_rows["cohort_id"].astype(str))
        valued, audit = _load_valued_actions(
            day=str(day),
            cohort_ids=cohort_ids,
            partition_root=partition_root,
            f06_spec=f06_spec,
        )
        valued_by_day[str(day)] = valued
        day_audit.append(audit)

    panel, bucket_audit = _build_potential_outcomes(
        paired,
        valued_by_day=valued_by_day,
    )
    oof, selection, report = _evaluate(panel, spec)
    report.update(
        {
            "spec": _identity(spec_path),
            "identities": spec["identities"],
            "value_coverage": {
                "complete_buckets": int(
                    bucket_audit["complete_terminal_overlay"].sum()
                ),
                "incomplete_buckets": int(
                    (~bucket_audit["complete_terminal_overlay"]).sum()
                ),
                "complete_fraction": float(
                    bucket_audit["complete_terminal_overlay"].mean()
                ),
            },
        }
    )
    _atomic_output(
        output_dir=output_dir,
        panel=panel,
        bucket_audit=bucket_audit,
        day_audit=pd.DataFrame(day_audit),
        oof=oof,
        selection=selection,
        report=report,
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = run(args.spec, args.output_dir)
    print(
        json.dumps(
            {
                "identity": report["identity"],
                "decision": report["decision"],
                "support": report["support"],
                "selection": report["selection"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
