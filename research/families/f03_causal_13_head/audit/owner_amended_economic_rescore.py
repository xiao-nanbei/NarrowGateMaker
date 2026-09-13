#!/usr/bin/env python3
"""Rescore frozen causal-v12 paths under an owner-amended economic contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.governance.paths import resolve_research_path

SCHEMA_VERSION = "causal_v12_owner_amended_economic_rescore.v2"
ARMS = ("ml_off", "ml_on")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def canonical_spec_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_spec_identity_sha256", None)
    return canonical_sha256(normalized)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _require_file(identity: Mapping[str, Any], label: str) -> Path:
    path = resolve_research_path(str(identity["path"]))
    if not path.is_file():
        raise FileNotFoundError(f"{label} missing: {path}")
    observed = sha256_file(path)
    if observed != str(identity["sha256"]):
        raise ValueError(
            f"{label} hash mismatch: observed={observed} expected={identity['sha256']}"
        )
    return path


def load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported owner-amended economic rescore schema")
    if canonical_spec_sha256(spec) != spec.get("canonical_spec_identity_sha256"):
        raise ValueError("owner-amended rescore canonical spec hash mismatch")
    if sha256_file(Path(__file__).resolve()) != spec["implementation_sha256"]:
        raise ValueError("owner-amended rescore implementation hash mismatch")
    if spec["permissions"].get("independent_confirmation", False):
        raise ValueError("post-hoc owner amendment cannot create confirmation")
    for authority in ("prediction_authority", "action_authority", "live_authority"):
        if spec["permissions"].get(authority, False):
            raise ValueError(f"owner-amended rescore cannot grant {authority}")
    all_days: list[str] = []
    for source in spec["sources"]:
        days = [str(day) for day in source["days"]]
        if days != sorted(days) or len(days) != len(set(days)):
            raise ValueError(f"source {source['label']} days are not unique/ordered")
        all_days.extend(days)
        _require_file(source["daily"], f"{source['label']}.daily")
        _require_file(source["campaigns"], f"{source['label']}.campaigns")
    if len(all_days) != len(set(all_days)):
        raise ValueError("owner-amended source dates overlap")
    return spec


def _bootstrap_delta(values: np.ndarray, *, draws: int, seed: int) -> dict[str, Any]:
    if values.ndim != 1 or len(values) == 0:
        raise ValueError("bootstrap values must be one-dimensional and non-empty")
    rng = np.random.default_rng(seed)
    sampled = rng.choice(values, size=(draws, len(values)), replace=True).mean(axis=1)
    return {
        "days": int(len(values)),
        "sum_delta": float(np.sum(values)),
        "mean_daily_delta": float(np.mean(values)),
        "median_daily_delta": float(np.median(values)),
        "positive_days": int(np.sum(values > 0.0)),
        "positive_day_rate": float(np.mean(values > 0.0)),
        "ci95_day_cluster_bootstrap": [
            float(np.quantile(sampled, 0.025)),
            float(np.quantile(sampled, 0.975)),
        ],
    }


def _paired_daily(daily: pd.DataFrame, metric: str) -> pd.DataFrame:
    wide = daily.pivot(index="day", columns="arm", values=metric).dropna()
    if set(wide.columns) != set(ARMS):
        raise ValueError(f"metric {metric} lacks paired arms")
    return wide.loc[:, list(ARMS)].sort_index()


def _metric_evidence(
    daily: pd.DataFrame,
    metric: str,
    *,
    draws: int,
    seed: int,
) -> dict[str, Any]:
    wide = _paired_daily(daily, metric)
    return _bootstrap_delta(
        (wide["ml_on"] - wide["ml_off"]).to_numpy(dtype=float),
        draws=draws,
        seed=seed,
    )


def _campaign_day_values(
    campaigns: pd.DataFrame,
    daily: pd.DataFrame,
) -> pd.DataFrame:
    index = pd.MultiIndex.from_product(
        [sorted(daily["day"].unique()), ARMS], names=["day", "arm"]
    )
    grouped = campaigns.groupby(["day", "arm"], sort=True)
    rows = grouped.apply(
        lambda group: pd.Series(
            {
                "closed_campaign_value_usdc": float(
                    group.loc[group["closed"].astype(bool), "terminal_value_usdc"].sum()
                ),
                "day_end_open_mtm_value_usdc": float(
                    group.loc[~group["closed"].astype(bool), "terminal_value_usdc"].sum()
                ),
                "closed_campaigns": int(group["closed"].astype(bool).sum()),
                "day_end_open_campaigns": int((~group["closed"].astype(bool)).sum()),
            }
        ),
        include_groups=False,
    )
    return rows.reindex(index, fill_value=0.0).reset_index()


def _loss_fill_selectivity(
    daily: pd.DataFrame,
    *,
    draws: int,
    seed: int,
) -> dict[str, Any]:
    pnl = _paired_daily(daily, "terminal_mtm_pnl_usdc")
    fills = _paired_daily(daily, "fills_total")
    joined = pnl.add_prefix("pnl_").join(fills.add_prefix("fills_"))

    def calculate(frame: pd.DataFrame) -> tuple[float, float, float, float]:
        pnl_off = float(frame["pnl_ml_off"].sum())
        pnl_on = float(frame["pnl_ml_on"].sum())
        fills_off = float(frame["fills_ml_off"].sum())
        fills_on = float(frame["fills_ml_on"].sum())
        if pnl_off >= 0.0 or fills_on >= fills_off:
            raise ValueError(
                "loss/fill selectivity requires negative control PnL and fewer ML-ON fills"
            )
        loss_reduction = (pnl_on - pnl_off) / abs(pnl_off)
        fill_reduction = (fills_off - fills_on) / fills_off
        elasticity = loss_reduction / fill_reduction
        proportional_pnl_on = pnl_off * (fills_on / fills_off)
        selectivity_surplus = pnl_on - proportional_pnl_on
        return loss_reduction, fill_reduction, elasticity, selectivity_surplus

    point = calculate(joined)
    rng = np.random.default_rng(seed)
    values: list[tuple[float, float, float, float]] = []
    for indices in rng.integers(0, len(joined), size=(draws, len(joined))):
        values.append(calculate(joined.iloc[indices]))
    sampled = np.asarray(values, dtype=float)
    labels = (
        "relative_loss_reduction",
        "relative_fill_reduction",
        "loss_reduction_to_fill_reduction_ratio",
        "proportional_thinning_selectivity_surplus_usdc",
    )
    result: dict[str, Any] = {}
    for index, label in enumerate(labels):
        result[label] = {
            "point_estimate": float(point[index]),
            "ci95_day_cluster_bootstrap": [
                float(np.quantile(sampled[:, index], 0.025)),
                float(np.quantile(sampled[:, index], 0.975)),
            ],
            "bootstrap_positive_fraction": float(np.mean(sampled[:, index] > 0.0)),
        }
    return result


def evaluate(spec: Mapping[str, Any]) -> dict[str, Any]:
    daily_frames: list[pd.DataFrame] = []
    campaign_frames: list[pd.DataFrame] = []
    for source in spec["sources"]:
        role = str(source["role"])
        days = set(str(day) for day in source["days"])
        daily = pd.read_csv(resolve_research_path(str(source["daily"]["path"])))
        campaigns = pd.read_parquet(
            resolve_research_path(str(source["campaigns"]["path"]))
        )
        daily = daily[daily["panel_role"].eq(role) & daily["day"].isin(days)].copy()
        campaigns = campaigns[
            campaigns["panel_role"].eq(role) & campaigns["day"].isin(days)
        ].copy()
        observed_days = set(daily["day"].unique())
        if observed_days != days:
            raise ValueError(
                f"{source['label']} daily denominator mismatch: {observed_days} != {days}"
            )
        if len(daily) != 2 * len(days):
            raise ValueError(f"{source['label']} does not contain one row per arm/day")
        daily_frames.append(daily)
        campaign_frames.append(campaigns)

    daily = pd.concat(daily_frames, ignore_index=True)
    campaigns = pd.concat(campaign_frames, ignore_index=True)
    draws = int(spec["bootstrap"]["draws"])
    seed = int(spec["bootstrap"]["seed"])
    campaign_daily = _campaign_day_values(campaigns, daily)

    closed = _metric_evidence(
        campaign_daily,
        "closed_campaign_value_usdc",
        draws=draws,
        seed=seed,
    )
    day_end_open = _metric_evidence(
        campaign_daily,
        "day_end_open_mtm_value_usdc",
        draws=draws,
        seed=seed + 1,
    )
    terminal = _metric_evidence(
        daily, "terminal_mtm_pnl_usdc", draws=draws, seed=seed + 2
    )
    q10 = _metric_evidence(
        daily, "campaign_q10_usdc", draws=draws, seed=seed + 3
    )
    cvar = _metric_evidence(
        daily, "campaign_cvar10_usdc", draws=draws, seed=seed + 4
    )
    buy_value = _metric_evidence(
        daily, "buy_maker_value_30s_bps", draws=draws, seed=seed + 5
    )
    sell_value = _metric_evidence(
        daily, "sell_maker_value_30s_bps", draws=draws, seed=seed + 6
    )
    selectivity = _loss_fill_selectivity(daily, draws=draws, seed=seed + 7)

    totals = daily.groupby("arm", sort=True).sum(numeric_only=True)
    fill_retention = float(
        totals.loc["ml_on", "fills_total"] / totals.loc["ml_off", "fills_total"]
    )
    inventory_time_ratio = float(
        totals.loc["ml_on", "abs_inventory_time_btc_s"]
        / totals.loc["ml_off", "abs_inventory_time_btc_s"]
    )
    gates = spec["gates"]
    hard_gates = {
        "closed_campaign_value_lcb_positive": bool(
            closed["ci95_day_cluster_bootstrap"][0] > 0.0
        ),
        "fill_activity_within_owner_band": bool(
            float(gates["minimum_fill_retention"])
            <= fill_retention
            <= float(gates["maximum_fill_retention"])
        ),
        "loss_fill_selectivity_lcb_above_one": bool(
            selectivity["loss_reduction_to_fill_reduction_ratio"][
                "ci95_day_cluster_bootstrap"
            ][0]
            > float(gates["minimum_loss_fill_selectivity_ratio"])
        ),
        "inventory_time_nonworse": bool(
            inventory_time_ratio <= float(gates["maximum_inventory_time_ratio"])
        ),
        "campaign_q10_nonworse": bool(q10["mean_daily_delta"] >= 0.0),
        "campaign_cvar10_nonworse": bool(cvar["mean_daily_delta"] >= 0.0),
        "buy_maker_value_nonworse": bool(
            buy_value["mean_daily_delta"]
            >= -float(gates["side_maker_value_tolerance_bps"])
        ),
        "sell_maker_value_nonworse": bool(
            sell_value["mean_daily_delta"]
            >= -float(gates["side_maker_value_tolerance_bps"])
        ),
    }
    all_passed = all(hard_gates.values())
    closed_delta = float(closed["sum_delta"])
    terminal_delta = float(terminal["sum_delta"])
    return {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": spec["experiment_id"],
        "decision": (
            "owner_amended_historical_economic_screen_passed_without_authority"
            if all_passed
            else "owner_amended_mean_and_selectivity_supported_campaign_q10_unresolved"
        ),
        "days": int(daily["day"].nunique()),
        "comparison": "ml_on_minus_ml_off",
        "cluster_unit": "UTC_day_without_strategy_state_reset_authority",
        "primary_closed_campaign_value": closed,
        "secondary_total_terminal_mtm": terminal,
        "diagnostic_day_end_open_mtm": day_end_open,
        "closed_campaign_share_of_total_pnl_delta": (
            closed_delta / terminal_delta if terminal_delta else 0.0
        ),
        "loss_fill_selectivity": selectivity,
        "fill_retention": fill_retention,
        "inventory_time_ratio": inventory_time_ratio,
        "campaign_q10": q10,
        "campaign_cvar10": cvar,
        "buy_maker_value_30s_bps": buy_value,
        "sell_maker_value_30s_bps": sell_value,
        "hard_gates": hard_gates,
        "all_hard_gates_passed": all_passed,
        "ranking_score": None,
        "permissions": dict(spec["permissions"]),
        "provenance": dict(spec["value_provenance"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    spec = load_spec(args.spec.expanduser().resolve())
    report = evaluate(spec)
    _atomic_json(args.output.expanduser().resolve(), report)
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
