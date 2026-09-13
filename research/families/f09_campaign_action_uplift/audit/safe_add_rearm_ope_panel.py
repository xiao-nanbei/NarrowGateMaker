#!/usr/bin/env python3
"""Validate and persist actual R0/R1/R2 safe-add rearm interventions.

Rows must come from the authoritative Python replay's
``_safe_add_rearm_intervention_trace``.  This module no longer randomizes
observational shadow probes after the fact: the logged action changed the
replayed order and campaign path, and the supplied propensity is the exact
behavior-policy probability used at assignment time.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.causal_path_features import (
    CAUSAL_PATH_FEATURE_COLUMNS,
    validate_causal_path_mapping,
)
from models.replay_policies import SAFE_ADD_REARM_ACTIONS

SCHEMA_VERSION = "safe_add_rearm_randomized_panel.v1"
OPE_FEATURES = (
    "side",
    "inventory_role",
    "inventory",
    "inventory_ratio",
    "campaign_age_s",
    "campaign_max_abs_qty_so_far",
    "campaign_pnl_so_far",
    "campaign_adverse_excursion_so_far",
    "campaign_exposure_increasing_fills_so_far",
    "campaign_reducing_fills_so_far",
    "toxicity",
    "markout_ema",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total",
    "fill_cooldown_elapsed_ms",
    "fill_cooldown_total_ms",
    "fill_cooldown_remaining_ms",
    "fill_cooldown_consecutive_units",
    "mid",
    "best_bid",
    "best_ask",
    "base_price",
    "base_size",
)

SUPPORT_COLUMNS = (
    "day",
    "decision_id",
    "decision_ts_ms",
    "campaign_id",
    "side",
    "inventory_role",
    "action",
    "behavior_propensity",
    *(f"behavior_prob_{action}" for action in SAFE_ADD_REARM_ACTIONS),
    "action_allow_post",
    "action_delta_ticks",
    "action_effective",
    "action_clamp_reason",
    "intervention_order_submit_count",
    "intervention_fill_count",
)


def validate_support_panel(frame: pd.DataFrame) -> None:
    """Validate action overlap and execution support without reading outcomes."""

    if frame.empty:
        raise ValueError("support preflight produced no safe-add interventions")
    missing = sorted(set(SUPPORT_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"safe-add support panel missing columns: {missing}")
    if frame["decision_id"].astype(str).duplicated().any():
        raise ValueError("decision_id must be unique")
    if frame.groupby(["day", "campaign_id"], sort=False).size().max() != 1:
        raise ValueError("each campaign may contain at most one intervention")
    side = frame["side"].astype(str).str.upper()
    if set(side) - {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    if set(frame["inventory_role"].astype(str)) != {"add"}:
        raise ValueError("safe-add interventions may target add-side orders only")

    actions = frame["action"].astype(str)
    if set(actions) - set(SAFE_ADD_REARM_ACTIONS):
        raise ValueError("the panel contains an unregistered safe-add action")
    probability_columns = [
        f"behavior_prob_{action}" for action in SAFE_ADD_REARM_ACTIONS
    ]
    behavior = frame[probability_columns].apply(pd.to_numeric, errors="coerce")
    values = behavior.to_numpy(dtype=float)
    if not np.isfinite(values).all() or (values <= 0.0).any():
        raise ValueError("all behavior actions require finite positive overlap")
    if not np.allclose(values.sum(axis=1), 1.0, atol=1e-10, rtol=0.0):
        raise ValueError("behavior probabilities must sum to one")
    action_index = {
        action: index for index, action in enumerate(SAFE_ADD_REARM_ACTIONS)
    }
    selected_indices = np.asarray(
        [action_index[action] for action in actions], dtype=int
    )
    logged_probability = values[np.arange(len(frame)), selected_indices]
    supplied_propensity = pd.to_numeric(
        frame["behavior_propensity"], errors="coerce"
    ).to_numpy(dtype=float)
    if not np.isfinite(supplied_propensity).all() or not np.allclose(
        supplied_propensity, logged_probability, atol=1e-10, rtol=0.0
    ):
        raise ValueError(
            "behavior_propensity must match the selected action's exact probability"
        )

    allow_post = pd.to_numeric(frame["action_allow_post"], errors="coerce")
    delta_ticks = pd.to_numeric(frame["action_delta_ticks"], errors="coerce")
    submit_count = pd.to_numeric(
        frame["intervention_order_submit_count"], errors="coerce"
    )
    fill_count = pd.to_numeric(frame["intervention_fill_count"], errors="coerce")
    if any(
        series.isna().any()
        for series in (allow_post, delta_ticks, submit_count, fill_count)
    ):
        raise ValueError("support fields must be numeric")
    r0 = actions == "r0_block"
    r1 = actions == "r1_rearm"
    r2 = actions == "r2_rearm_widen_1tick"
    if (
        (allow_post[r0] != 0).any()
        or (submit_count[r0] != 0).any()
        or (fill_count[r0] != 0).any()
    ):
        raise ValueError("R0 must remain blocked and cannot own an intervention fill")
    if (allow_post[r1 | r2] != 1).any() or (submit_count[r1 | r2] < 1).any():
        raise ValueError("R1/R2 must enter the actual replay order state machine")
    if not np.allclose(delta_ticks[r0 | r1], 0.0, atol=1e-8, rtol=0.0):
        raise ValueError("R0/R1 must preserve the baseline quote price")
    if (delta_ticks.abs() > 1.0 + 1e-8).any():
        raise ValueError("safe-add price changes must be bounded to one tick")
    if (delta_ticks[r2 & (side == "BUY")] > 1e-8).any() or (
        delta_ticks[r2 & (side == "SELL")] < -1e-8
    ).any():
        raise ValueError("R2 must move the add quote away from the market")


def support_only_panel(frame: pd.DataFrame) -> pd.DataFrame:
    """Strip every reward/outcome field after full replay validation."""

    validate_randomized_panel(frame)
    support = frame.loc[:, list(SUPPORT_COLUMNS)].copy()
    validate_support_panel(support)
    return support


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_randomized_panel(frame: pd.DataFrame) -> None:
    """Fail closed on propensity, action, reward, or campaign violations."""

    if frame.empty:
        raise ValueError("randomized replay produced no safe-add interventions")
    required = {
        "day",
        "decision_id",
        "campaign_id",
        "side",
        "inventory_role",
        "action",
        "behavior_propensity",
        "action_allow_post",
        "action_delta_ticks",
        "intervention_order_submit_count",
        "intervention_fill_count",
        "reward",
        "fill_value",
        "campaign_cost",
        "queue_cost",
        "reward_identity_error",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"safe-add randomized panel missing columns: {missing}")
    if frame["decision_id"].astype(str).duplicated().any():
        raise ValueError("decision_id must be unique")
    if frame.groupby(["day", "campaign_id"], sort=False).size().max() != 1:
        raise ValueError("each campaign may contain at most one intervention")
    if set(frame["side"].astype(str).str.upper()) - {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    if set(frame["inventory_role"].astype(str)) != {"add"}:
        raise ValueError("safe-add interventions may target add-side orders only")
    present_path_columns = set(CAUSAL_PATH_FEATURE_COLUMNS) & set(frame.columns)
    if present_path_columns:
        missing_path_columns = sorted(
            set(CAUSAL_PATH_FEATURE_COLUMNS) - set(frame.columns)
        )
        if missing_path_columns:
            raise ValueError(
                "safe-add panel has a partial causal path mapping: "
                f"{missing_path_columns}"
            )
        for row in frame.loc[:, list(CAUSAL_PATH_FEATURE_COLUMNS)].to_dict("records"):
            validate_causal_path_mapping(row)
    actions = frame["action"].astype(str)
    if set(actions) - set(SAFE_ADD_REARM_ACTIONS):
        raise ValueError("the panel contains an unregistered safe-add action")

    probability_columns = [
        f"behavior_prob_{action}" for action in SAFE_ADD_REARM_ACTIONS
    ]
    missing_probabilities = sorted(set(probability_columns) - set(frame.columns))
    if missing_probabilities:
        raise ValueError(
            f"safe-add panel missing complete propensity vector: {missing_probabilities}"
        )
    behavior = frame[probability_columns].apply(pd.to_numeric, errors="coerce")
    values = behavior.to_numpy(dtype=float)
    if not np.isfinite(values).all() or (values <= 0.0).any():
        raise ValueError("all behavior actions require finite positive overlap")
    if not np.allclose(values.sum(axis=1), 1.0, atol=1e-10, rtol=0.0):
        raise ValueError("behavior probabilities must sum to one")
    action_index = {
        action: index for index, action in enumerate(SAFE_ADD_REARM_ACTIONS)
    }
    selected_indices = np.asarray(
        [action_index[action] for action in actions], dtype=int
    )
    logged_probability = values[np.arange(len(frame)), selected_indices]
    supplied_propensity = pd.to_numeric(
        frame["behavior_propensity"], errors="coerce"
    ).to_numpy(dtype=float)
    if not np.isfinite(supplied_propensity).all() or not np.allclose(
        supplied_propensity,
        logged_probability,
        atol=1e-10,
        rtol=0.0,
    ):
        raise ValueError(
            "behavior_propensity must match the selected action's exact probability"
        )

    allow_post = pd.to_numeric(frame["action_allow_post"], errors="coerce")
    delta_ticks = pd.to_numeric(frame["action_delta_ticks"], errors="coerce")
    fill_count = pd.to_numeric(frame["intervention_fill_count"], errors="coerce")
    submit_count = pd.to_numeric(
        frame["intervention_order_submit_count"], errors="coerce"
    )
    if (
        allow_post.isna().any()
        or delta_ticks.isna().any()
        or fill_count.isna().any()
        or submit_count.isna().any()
    ):
        raise ValueError("action and fill fields must be numeric")
    r0 = actions == "r0_block"
    r1 = actions == "r1_rearm"
    r2 = actions == "r2_rearm_widen_1tick"
    if (
        (allow_post[r0] != 0).any()
        or (submit_count[r0] != 0).any()
        or (fill_count[r0] != 0).any()
    ):
        raise ValueError("R0 must remain blocked and cannot own an intervention fill")
    if (allow_post[r1 | r2] != 1).any():
        raise ValueError("R1/R2 must submit the randomized add-side quote")
    if (submit_count[r1 | r2] < 1).any():
        raise ValueError("R1/R2 must enter the actual replay order state machine")
    if not np.allclose(delta_ticks[r0 | r1], 0.0, atol=1e-8, rtol=0.0):
        raise ValueError("R0/R1 must preserve the baseline quote price")
    if (delta_ticks.abs() > 1.0 + 1e-8).any():
        raise ValueError("safe-add price changes must be bounded to one tick")
    side = frame["side"].astype(str).str.upper()
    if (delta_ticks[r2 & (side == "BUY")] > 1e-8).any() or (
        delta_ticks[r2 & (side == "SELL")] < -1e-8
    ).any():
        raise ValueError("R2 must move the add quote away from the market")

    queue_cost = pd.to_numeric(frame["queue_cost"], errors="coerce")
    if queue_cost.isna().any() or not np.allclose(
        queue_cost, 0.0, atol=1e-12, rtol=0.0
    ):
        raise ValueError("v1 requires zero pre-existing queue/reset cost")
    identity_error = pd.to_numeric(
        frame["reward_identity_error"], errors="coerce"
    ).abs()
    if identity_error.isna().any() or float(identity_error.max()) > 1e-9:
        raise ValueError("reward != fill_value - campaign_cost - queue_cost")


def build_randomized_panel(records: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Return a normalized intervention panel and audit metadata."""

    panel = records.copy()
    panel["day"] = panel["day"].astype("string").fillna("").str.slice(0, 10)
    panel["side"] = panel["side"].astype("string").fillna("").str.upper()
    panel["campaign_id"] = pd.to_numeric(
        panel["campaign_id"], errors="coerce"
    ).astype("Int64")
    panel = panel.sort_values(
        ["day", "campaign_id", "decision_ts_ms", "side"], kind="stable"
    ).reset_index(drop=True)
    validate_randomized_panel(panel)
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "engine": "python_authoritative_randomized_replay",
        "rows": int(len(panel)),
        "days": int(panel["day"].nunique()),
        "campaigns": int(panel[["day", "campaign_id"]].drop_duplicates().shape[0]),
        "action_counts": {
            str(key): int(value)
            for key, value in panel["action"].value_counts().sort_index().items()
        },
        "one_intervention_per_campaign": True,
        "propensity_source": "logged_behavior_policy_at_replay_assignment",
        "reward_scope": "decision_to_actual_campaign_terminal_mtm",
        "campaign_cost_available": True,
        "queue_reset_cost_available": True,
        "reducing_side_modified": False,
        "order_size_modified": False,
        "inventory_limit_modified": False,
        "action_bearing_evidence": True,
        "strategy_evidence": False,
        "promotion_status": "not_evaluated",
    }
    return panel, metadata


def write_panel(prefix: Path, panel: pd.DataFrame, metadata: dict) -> dict[str, str]:
    prefix.parent.mkdir(parents=True, exist_ok=True)
    paths = {
        "panel": str(prefix.with_suffix(".action_panel.csv")),
        "metadata": str(prefix.with_suffix(".action_panel.json")),
    }
    panel.to_csv(paths["panel"], index=False)
    Path(paths["metadata"]).write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interventions-csv", type=Path, required=True)
    parser.add_argument("--out-prefix", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = args.interventions_csv.expanduser().resolve()
    panel, metadata = build_randomized_panel(pd.read_csv(source))
    metadata["source"] = {
        "path": str(source),
        "sha256": _sha256(source),
        "bytes": source.stat().st_size,
    }
    paths = write_panel(args.out_prefix.expanduser().resolve(), panel, metadata)
    print(json.dumps({"metadata": metadata, "artifacts": paths}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
