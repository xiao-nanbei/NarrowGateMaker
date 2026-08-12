#!/usr/bin/env python3
"""Learn and audit a state-conditioned safe-add rearm policy.

The source panel is the action-bearing R0/R1/R2 randomized replay panel.  The
old five-second arm is not re-optimized here: five seconds is only the earliest
observation floor at which the panel has support.  Within each chronological
test fold, BUY and SELL action-Q models are fitted on past days only.  The
candidate keeps R0 (continue blocking) unless a supported R1/R2 action has
higher predicted decision-to-campaign value in the current causal local state.

This is a development audit.  The already-consumed 2026-07-07..11 panel is
deliberately excluded; promotion requires genuinely later good days collected
after this family has been frozen.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import (
    OPEConfig,
    evaluate_offline_policy,
)
from research.families.f09_campaign_action_uplift.audit.safe_add_rearm_ope_panel import (
    SUPPORT_COLUMNS,
    validate_randomized_panel,
    validate_support_panel,
)
from research.families.f09_campaign_action_uplift.causal_path_features import (
    CAUSAL_PATH_FEATURE_VERSION,
    CAUSAL_PATH_POLICY_FEATURES,
)
from models.replay_policies import SAFE_ADD_REARM_ACTIONS

SCHEMA_VERSION = "safe_add_rearm_state_policy.v2"
SPEC_SCHEMA_VERSION = "safe_add_rearm_state_policy_spec.v2"
FAMILY_ID = "safe_add_rearm_state_conditioned_local_v1"
POLICY_NAME = "side_specific_supported_q_argmax"
CONTROL_ACTION = "r0_block"
TARGETS = ("reward", "terminal", "tail_avoidance")

# Deliberately excludes raw price levels, external reference fields, terminal
# outcomes, and the constant quote-flip field in the frozen source panel.
STATE_FEATURES = (
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
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total",
    "fill_cooldown_elapsed_ms",
    "fill_cooldown_remaining_ms",
    "fill_cooldown_consecutive_units",
)

POLICY_FAMILIES: dict[str, dict[str, Any]] = {
    "snapshot_v1": {
        "family_id": FAMILY_ID,
        "policy_name": POLICY_NAME,
        "features": STATE_FEATURES,
        "path_metadata_required": False,
    },
    "causal_path_v2": {
        "family_id": "safe_add_rearm_state_conditioned_path_v2",
        "policy_name": "side_specific_causal_path_supported_q_argmax",
        "features": STATE_FEATURES + CAUSAL_PATH_POLICY_FEATURES,
        "path_metadata_required": True,
    },
}


def _policy_family(name: str) -> dict[str, Any]:
    if name not in POLICY_FAMILIES:
        raise ValueError(f"unknown state-policy feature family: {name}")
    return POLICY_FAMILIES[name]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def _read_days(path: Path) -> list[str]:
    frame = pd.read_csv(path)
    if "day" not in frame:
        raise ValueError(f"day manifest lacks day column: {path}")
    days = frame["day"].astype(str).str.slice(0, 10).tolist()
    if not days or days != sorted(days) or len(days) != len(set(days)):
        raise ValueError(f"day manifest must be non-empty, sorted, and unique: {path}")
    return days


def _source_identity(
    panel: pd.DataFrame,
    *,
    metadata: dict[str, Any],
    days: list[str],
    source_family: dict[str, Any],
    feature_names: tuple[str, ...],
) -> dict[str, Any]:
    missing_support = sorted(set(SUPPORT_COLUMNS) - set(panel.columns))
    if missing_support:
        raise ValueError(f"source panel lacks support columns: {missing_support}")
    validate_support_panel(panel.loc[:, list(SUPPORT_COLUMNS)].copy())
    observed_days = sorted(panel["day"].astype(str).str.slice(0, 10).unique())
    if observed_days != days:
        raise ValueError("source panel days differ from development manifest")
    if not source_family.get("family_frozen"):
        raise ValueError("source randomized action family is not frozen")
    if tuple(source_family.get("actions", ())) != tuple(SAFE_ADD_REARM_ACTIONS):
        raise ValueError("source action registry differs from replay registry")
    observation_floor_s = float(source_family.get("selected_elapsed_s", math.nan))
    if not math.isfinite(observation_floor_s) or observation_floor_s <= 0.0:
        raise ValueError("source family has no valid observation floor")
    if float(metadata.get("elapsed_s", math.nan)) != observation_floor_s:
        raise ValueError("source panel elapsed differs from frozen source family")
    elapsed = pd.to_numeric(panel["target_elapsed_ms"], errors="coerce")
    if elapsed.isna().any() or not np.allclose(
        elapsed, observation_floor_s * 1000.0, atol=1e-8, rtol=0.0
    ):
        raise ValueError("source rows differ from the frozen observation floor")
    if "external_reference_used" in panel and pd.to_numeric(
        panel["external_reference_used"], errors="coerce"
    ).fillna(0).any():
        raise ValueError("state-conditioned v1 must remain local-only")
    missing_features = sorted(set(feature_names) - set(panel.columns))
    if missing_features:
        raise ValueError(f"source panel lacks state features: {missing_features}")
    return {
        "observation_floor_s": observation_floor_s,
        "config_sha256": str(metadata.get("config_sha256", "")),
        "latency_profile_id": str(metadata.get("latency_profile_id", "")),
        "days": len(days),
        "first_day": days[0],
        "last_day": days[-1],
    }


def freeze_spec(
    *,
    panel_path: Path,
    metadata_path: Path,
    days_path: Path,
    source_family_path: Path,
    path_metadata_path: Path | None,
    output_path: Path,
    config: OPEConfig,
    feature_family: str,
) -> dict[str, Any]:
    family = _policy_family(feature_family)
    feature_names = tuple(family["features"])
    panel = pd.read_csv(panel_path)
    metadata = _read_json(metadata_path)
    days = _read_days(days_path)
    source_family = _read_json(source_family_path)
    identity = _source_identity(
        panel,
        metadata=metadata,
        days=days,
        source_family=source_family,
        feature_names=feature_names,
    )
    path_source: dict[str, Any] = {}
    if family["path_metadata_required"]:
        if path_metadata_path is None:
            raise ValueError("causal_path_v2 requires --path-metadata")
        path_metadata = _read_json(path_metadata_path)
        if path_metadata.get("feature_version") != CAUSAL_PATH_FEATURE_VERSION:
            raise ValueError("causal path feature version differs from the code registry")
        if str(path_metadata.get("output_panel_sha256", "")) != _sha256(panel_path):
            raise ValueError("causal path metadata does not identify the source panel")
        if float(path_metadata.get("valid_rate", 0.0)) < 1.0:
            raise ValueError("causal path policy requires complete valid path coverage")
        path_source = {
            "path_metadata_sha256": _sha256(path_metadata_path),
            "path_feature_version": CAUSAL_PATH_FEATURE_VERSION,
            "path_valid_rate": float(path_metadata["valid_rate"]),
        }
    spec = {
        "schema_version": SPEC_SCHEMA_VERSION,
        "family_id": family["family_id"],
        "feature_family": feature_family,
        "family_frozen": True,
        "source": {
            "development_panel_sha256": _sha256(panel_path),
            "development_metadata_sha256": _sha256(metadata_path),
            "development_days_sha256": _sha256(days_path),
            "source_family_sha256": _sha256(source_family_path),
            **path_source,
            **identity,
        },
        "decision_surface": {
            "observation_floor_s": identity["observation_floor_s"],
            "observation_floor_is_tunable_policy_parameter": False,
            "one_randomized_intervention_per_campaign": True,
            "side_models": ["BUY", "SELL"],
            "inventory_role": "add",
            "external_reference_used": False,
        },
        "policy": {
            "name": family["policy_name"],
            "actions": list(SAFE_ADD_REARM_ACTIONS),
            "default_action": CONTROL_ACTION,
            "selection_target": "decision_to_campaign_terminal_reward",
            "rule": (
                "choose the supported action with maximum cross-fitted Q; "
                "ties or no positive advantage over R0 remain R0"
            ),
            "features": list(feature_names),
            "model": "ridge_action_q",
            "ridge_alpha": config.ridge_alpha,
        },
        "evaluation": {
            "config": asdict(config),
            "targets": list(TARGETS),
            "tail_threshold_usdc": -5.0,
            "later_2026_07_07_through_11_consumed_and_excluded": True,
            "promotion_requires_days_after": "2026-07-11",
        },
        "code": {
            "state_policy_module_sha256": _sha256(Path(__file__).resolve()),
            "ope_module_sha256": _sha256(
                Path(__file__).with_name("offline_policy_evaluation.py").resolve()
            ),
            "causal_path_module_sha256": _sha256(
                Path(__file__).parents[1].joinpath("causal_path_features.py").resolve()
            ),
        },
        "freeze_read_outcomes": False,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(spec, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return spec


def _verify_spec(
    spec: dict[str, Any],
    *,
    panel_path: Path,
    metadata_path: Path,
    days_path: Path,
    source_family_path: Path,
    path_metadata_path: Path | None,
    panel: pd.DataFrame,
    metadata: dict[str, Any],
    days: list[str],
) -> dict[str, Any]:
    feature_family = str(spec.get("feature_family", ""))
    family = _policy_family(feature_family)
    feature_names = tuple(family["features"])
    if spec.get("schema_version") != SPEC_SCHEMA_VERSION:
        raise ValueError("state-policy spec schema mismatch")
    if (
        spec.get("family_id") != family["family_id"]
        or not spec.get("family_frozen")
    ):
        raise ValueError("state-policy family is not frozen")
    source_family = _read_json(source_family_path)
    identity = _source_identity(
        panel,
        metadata=metadata,
        days=days,
        source_family=source_family,
        feature_names=feature_names,
    )
    expected = {
        "development_panel_sha256": _sha256(panel_path),
        "development_metadata_sha256": _sha256(metadata_path),
        "development_days_sha256": _sha256(days_path),
        "source_family_sha256": _sha256(source_family_path),
        "config_sha256": identity["config_sha256"],
        "latency_profile_id": identity["latency_profile_id"],
    }
    if family["path_metadata_required"]:
        if path_metadata_path is None:
            raise ValueError("causal_path_v2 requires --path-metadata")
        path_metadata = _read_json(path_metadata_path)
        expected.update(
            {
                "path_metadata_sha256": _sha256(path_metadata_path),
                "path_feature_version": CAUSAL_PATH_FEATURE_VERSION,
                "path_valid_rate": float(path_metadata.get("valid_rate", 0.0)),
            }
        )
        if str(path_metadata.get("output_panel_sha256", "")) != _sha256(panel_path):
            raise ValueError("causal path metadata does not identify the source panel")
    for key, value in expected.items():
        if str(spec.get("source", {}).get(key, "")) != str(value):
            raise ValueError(f"state-policy source identity mismatch: {key}")
    expected_code = {
        "state_policy_module_sha256": _sha256(Path(__file__).resolve()),
        "ope_module_sha256": _sha256(
            Path(__file__).with_name("offline_policy_evaluation.py").resolve()
        ),
        "causal_path_module_sha256": _sha256(
            Path(__file__).parents[1].joinpath("causal_path_features.py").resolve()
        ),
    }
    for key, value in expected_code.items():
        if str(spec.get("code", {}).get(key, "")) != value:
            raise ValueError(f"state-policy code identity mismatch: {key}")
    policy = spec.get("policy", {})
    if tuple(policy.get("features", ())) != feature_names:
        raise ValueError("state-policy feature registry changed after freeze")
    if policy.get("name") != family["policy_name"]:
        raise ValueError("state-policy name changed after freeze")
    if tuple(policy.get("actions", ())) != tuple(SAFE_ADD_REARM_ACTIONS):
        raise ValueError("state-policy action registry changed after freeze")
    if policy.get("default_action") != CONTROL_ACTION:
        raise ValueError("state-policy default action must remain R0")
    return identity


def _target_panel(
    panel: pd.DataFrame, target: str, *, tail_threshold: float
) -> pd.DataFrame:
    output = panel.copy()
    if target == "reward":
        values = pd.to_numeric(output["reward"], errors="coerce")
    elif target == "terminal":
        values = pd.to_numeric(output["terminal_campaign_pnl"], errors="coerce")
    elif target == "tail_avoidance":
        terminal = pd.to_numeric(output["terminal_campaign_pnl"], errors="coerce")
        values = -(terminal <= float(tail_threshold)).astype(float)
    else:  # pragma: no cover - fixed internal registry
        raise ValueError(f"unknown target: {target}")
    output["ope_target"] = values
    return output


def _bootstrap(
    paired: pd.DataFrame, *, trials: int, seed: int
) -> dict[str, float | int]:
    valid = paired[np.isfinite(pd.to_numeric(paired["dr_contrast"], errors="coerce"))]
    if valid.empty or trials <= 0:
        return {
            "trials": 0,
            "days": int(valid["day"].nunique()),
            "p025": math.nan,
            "p50": math.nan,
            "p975": math.nan,
        }
    clusters = valid.groupby("day", sort=True)["dr_contrast"].agg(["sum", "count"])
    sums = clusters["sum"].to_numpy(dtype=float)
    counts = clusters["count"].to_numpy(dtype=float)
    rng = np.random.default_rng(seed)
    samples = np.empty(int(trials), dtype=float)
    for idx in range(int(trials)):
        chosen = rng.integers(0, len(clusters), size=len(clusters))
        samples[idx] = sums[chosen].sum() / max(counts[chosen].sum(), 1.0)
    return {
        "trials": int(trials),
        "days": int(len(clusters)),
        "p025": float(np.quantile(samples, 0.025)),
        "p50": float(np.quantile(samples, 0.50)),
        "p975": float(np.quantile(samples, 0.975)),
    }


def _paired_contrast(
    candidate: pd.DataFrame,
    control: pd.DataFrame,
    *,
    scope: str,
    target: str,
    trials: int,
    seed: int,
    policy_name: str = POLICY_NAME,
) -> tuple[dict[str, Any], pd.DataFrame]:
    candidate_cols = [
        "decision_id",
        "day",
        "side",
        "ope_fold",
        "ope_candidate_action",
        "ope_dr_value",
        "ope_prediction_valid",
        "ope_clipped_importance_weight",
        "ope_unsupported_candidate_mass",
    ]
    left = candidate[candidate_cols].rename(
        columns={
            "ope_dr_value": "candidate_dr",
            "ope_prediction_valid": "candidate_valid",
            "ope_clipped_importance_weight": "candidate_weight",
            "ope_unsupported_candidate_mass": "candidate_unsupported_mass",
        }
    )
    right = control[
        [
            "decision_id",
            "ope_dr_value",
            "ope_prediction_valid",
            "ope_clipped_importance_weight",
            "ope_unsupported_candidate_mass",
        ]
    ].rename(
        columns={
            "ope_dr_value": "control_dr",
            "ope_prediction_valid": "control_valid",
            "ope_clipped_importance_weight": "control_weight",
            "ope_unsupported_candidate_mass": "control_unsupported_mass",
        }
    )
    paired = left.merge(right, on="decision_id", how="inner", validate="one_to_one")
    paired = paired[
        (paired["candidate_valid"] == 1)
        & (paired["control_valid"] == 1)
        & np.isfinite(pd.to_numeric(paired["candidate_dr"], errors="coerce"))
        & np.isfinite(pd.to_numeric(paired["control_dr"], errors="coerce"))
    ].copy()
    paired["dr_contrast"] = paired["candidate_dr"] - paired["control_dr"]
    interval = _bootstrap(paired, trials=trials, seed=seed)
    daily = paired.groupby("day", sort=True)["dr_contrast"].mean()
    candidate_weight = paired["candidate_weight"].to_numpy(dtype=float)
    control_weight = paired["control_weight"].to_numpy(dtype=float)

    def effective_sample_size(weight: np.ndarray) -> float:
        denominator = float(np.square(weight).sum())
        return float(weight.sum() ** 2 / denominator) if denominator > 0.0 else 0.0

    row = {
        "scope": scope,
        "target": target,
        "unit": "probability" if target == "tail_avoidance" else "USDC",
        "candidate": policy_name,
        "control": CONTROL_ACTION,
        "rows": int(len(paired)),
        "days": int(paired["day"].nunique()),
        "dr_contrast": float(paired["dr_contrast"].mean()),
        "dr_contrast_p025": interval["p025"],
        "dr_contrast_p50": interval["p50"],
        "dr_contrast_p975": interval["p975"],
        "candidate_ess": effective_sample_size(candidate_weight),
        "control_ess": effective_sample_size(control_weight),
        "candidate_unsupported_mass": float(
            paired["candidate_unsupported_mass"].mean()
        ),
        "control_unsupported_mass": float(
            paired["control_unsupported_mass"].mean()
        ),
        "daily_positive_days": int((daily > 0.0).sum()),
        "daily_negative_days": int((daily < 0.0).sum()),
        "daily_zero_days": int((daily == 0.0).sum()),
        "daily_positive_rate": float((daily > 0.0).mean()),
    }
    paired.insert(0, "scope", scope)
    paired.insert(1, "target", target)
    return row, paired


def _candidate_actions_from_reward_rows(
    panel: pd.DataFrame, reward_rows: pd.DataFrame
) -> pd.DataFrame:
    mapping = reward_rows.set_index("decision_id")["ope_candidate_action"]
    if mapping.index.duplicated().any() or mapping.eq("").any():
        raise ValueError("reward policy produced invalid candidate action mapping")
    output = panel.copy()
    output["candidate_action"] = CONTROL_ACTION
    selected = output["decision_id"].map(mapping)
    output.loc[selected.notna(), "candidate_action"] = selected[selected.notna()]
    return output


def _evaluate_scope(
    panel: pd.DataFrame,
    *,
    scope: str,
    config: OPEConfig,
    tail_threshold: float,
    bootstrap_trials: int,
    random_seed: int,
    feature_names: tuple[str, ...],
    policy_name: str,
) -> tuple[
    dict[str, tuple[pd.DataFrame, pd.DataFrame]],
    list[dict[str, Any]],
    list[pd.DataFrame],
    pd.DataFrame,
]:
    reward_panel = _target_panel(panel, "reward", tail_threshold=tail_threshold)
    learned_cfg = OPEConfig(**{**asdict(config), "learn_supported_policy": True})
    reward_candidate, _, _, _ = evaluate_offline_policy(
        reward_panel,
        feature_names=feature_names,
        config=learned_cfg,
    )
    candidate_panel = _candidate_actions_from_reward_rows(panel, reward_candidate)
    control_panel = panel.copy()
    control_panel["candidate_action"] = CONTROL_ACTION

    evaluations: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    contrast_rows: list[dict[str, Any]] = []
    paired_rows: list[pd.DataFrame] = []
    for target_idx, target in enumerate(TARGETS):
        candidate_target = _target_panel(
            candidate_panel, target, tail_threshold=tail_threshold
        )
        control_target = _target_panel(
            control_panel, target, tail_threshold=tail_threshold
        )
        fixed_cfg = OPEConfig(**{**asdict(config), "learn_supported_policy": False})
        candidate_rows, _, _, _ = evaluate_offline_policy(
            candidate_target,
            feature_names=feature_names,
            config=fixed_cfg,
        )
        control_rows, _, _, _ = evaluate_offline_policy(
            control_target,
            feature_names=feature_names,
            config=fixed_cfg,
        )
        row, paired = _paired_contrast(
            candidate_rows,
            control_rows,
            scope=scope,
            target=target,
            trials=bootstrap_trials,
            seed=random_seed + target_idx * 1_003 + (0 if scope == "buy" else 101),
            policy_name=policy_name,
        )
        contrast_rows.append(row)
        paired_rows.append(paired)
        evaluations[target] = (candidate_rows, control_rows)

    q_columns = [f"ope_q_{action}" for action in SAFE_ADD_REARM_ACTIONS]
    action_rows = reward_candidate[
        [
            "decision_id",
            "day",
            "side",
            "ope_fold",
            "action",
            "ope_candidate_action",
            *q_columns,
        ]
    ].copy()
    action_rows["predicted_advantage_over_r0"] = 0.0
    control_q = pd.to_numeric(
        action_rows[f"ope_q_{CONTROL_ACTION}"], errors="coerce"
    )
    for action in SAFE_ADD_REARM_ACTIONS:
        selected = action_rows["ope_candidate_action"] == action
        action_rows.loc[selected, "predicted_advantage_over_r0"] = (
            pd.to_numeric(
                action_rows.loc[selected, f"ope_q_{action}"], errors="coerce"
            )
            - control_q.loc[selected]
        )
    action_rows["candidate_rearm"] = (
        action_rows["ope_candidate_action"] != CONTROL_ACTION
    ).astype(int)
    return evaluations, contrast_rows, paired_rows, action_rows


def _markdown(
    contrasts: pd.DataFrame,
    action_mix: pd.DataFrame,
    metadata: dict[str, Any],
) -> str:
    lines = [
        "# State-Conditioned Safe-Add Rearm Development Audit",
        "",
        f"- Family: `{metadata['family_id']}`",
        f"- Feature family: `{metadata['feature_family']}`",
        f"- Observation floor: `{metadata['observation_floor_s']:.0f}s` (support boundary, not a searched cooldown)",
        "- Policy: separate BUY/SELL cross-fitted action-Q; default R0 unless supported R1/R2 has higher predicted value",
        "- Inputs: causal local state only; no external reference",
        "- Split: chronological 50 train days / 1 embargo / 10 test days",
        "- Previously consumed 2026-07-07..11: excluded",
        "",
        "## Direct DR Contrast vs R0",
        "",
        "| scope | target | DR uplift [2.5%,97.5%] | candidate ESS | daily + / - |",
        "|---|---|---:|---:|---:|",
    ]
    for row in contrasts.to_dict("records"):
        lines.append(
            f"| {row['scope']} | {row['target']} | {row['dr_contrast']:+.6f} "
            f"[{row['dr_contrast_p025']:+.6f},{row['dr_contrast_p975']:+.6f}] | "
            f"{row['candidate_ess']:.1f} | "
            f"{row['daily_positive_days']} / {row['daily_negative_days']} |"
        )
    lines.extend(
        [
            "",
            "## Learned Action Mix",
            "",
            "| side | action | rows | rate | predicted advantage mean |",
            "|---|---|---:|---:|---:|",
        ]
    )
    for row in action_mix.to_dict("records"):
        lines.append(
            f"| {row['side']} | `{row['ope_candidate_action']}` | {row['rows']} | "
            f"{row['rate']:.4f} | {row['predicted_advantage_mean']:+.6f} |"
        )
    lines.extend(
        [
            "",
            "## Decision",
            "",
            f"- Development strict gate: `{metadata['development_strict_gate']}`",
            f"- Eligible for a genuinely new later panel: `{metadata['eligible_for_new_later']}`",
            "- This report cannot promote live/C++/config changes. Even a development pass must be frozen before evaluating days after 2026-07-11.",
            "",
        ]
    )
    return "\n".join(lines)


def evaluate_state_policy(
    *,
    panel_path: Path,
    metadata_path: Path,
    days_path: Path,
    source_family_path: Path,
    path_metadata_path: Path | None,
    spec_path: Path,
    output_prefix: Path,
    config: OPEConfig,
    tail_threshold: float,
    expected_feature_family: str,
) -> dict[str, Any]:
    panel = pd.read_csv(panel_path)
    validate_randomized_panel(panel)
    metadata = _read_json(metadata_path)
    days = _read_days(days_path)
    spec = _read_json(spec_path)
    feature_family = str(spec.get("feature_family", ""))
    if feature_family != expected_feature_family:
        raise ValueError("runtime feature family differs from frozen spec")
    family = _policy_family(feature_family)
    feature_names = tuple(family["features"])
    policy_name = str(family["policy_name"])
    identity = _verify_spec(
        spec,
        panel_path=panel_path,
        metadata_path=metadata_path,
        days_path=days_path,
        source_family_path=source_family_path,
        path_metadata_path=path_metadata_path,
        panel=panel,
        metadata=metadata,
        days=days,
    )
    frozen_cfg = OPEConfig(**spec["evaluation"]["config"])
    if asdict(frozen_cfg) != asdict(config):
        raise ValueError("runtime OPE configuration differs from frozen spec")
    if float(spec["evaluation"]["tail_threshold_usdc"]) != float(tail_threshold):
        raise ValueError("runtime tail threshold differs from frozen spec")

    all_contrasts: list[dict[str, Any]] = []
    all_paired: list[pd.DataFrame] = []
    all_actions: list[pd.DataFrame] = []
    for scope, side in (("buy", "BUY"), ("sell", "SELL")):
        scoped = panel[panel["side"].astype(str).str.upper() == side].copy()
        _, contrasts, paired, actions = _evaluate_scope(
            scoped,
            scope=scope,
            config=config,
            tail_threshold=tail_threshold,
            bootstrap_trials=config.bootstrap_trials,
            random_seed=config.random_seed,
            feature_names=feature_names,
            policy_name=policy_name,
        )
        all_contrasts.extend(contrasts)
        all_paired.extend(paired)
        all_actions.append(actions)

    paired_frame = pd.concat(all_paired, ignore_index=True)
    for target_idx, target in enumerate(TARGETS):
        pooled = paired_frame[paired_frame["target"] == target].copy()
        interval = _bootstrap(
            pooled,
            trials=config.bootstrap_trials,
            seed=config.random_seed + 10_000 + target_idx,
        )
        daily = pooled.groupby("day", sort=True)["dr_contrast"].mean()

        def pooled_ess(column: str) -> float:
            weight = pooled[column].to_numpy(dtype=float)
            denominator = float(np.square(weight).sum())
            return (
                float(weight.sum() ** 2 / denominator)
                if denominator > 0.0
                else 0.0
            )

        all_contrasts.append(
            {
                "scope": "pooled",
                "target": target,
                "unit": "probability" if target == "tail_avoidance" else "USDC",
                "candidate": policy_name,
                "control": CONTROL_ACTION,
                "rows": int(len(pooled)),
                "days": int(pooled["day"].nunique()),
                "dr_contrast": float(pooled["dr_contrast"].mean()),
                "dr_contrast_p025": interval["p025"],
                "dr_contrast_p50": interval["p50"],
                "dr_contrast_p975": interval["p975"],
                "candidate_ess": pooled_ess("candidate_weight"),
                "control_ess": pooled_ess("control_weight"),
                "candidate_unsupported_mass": float(
                    pooled["candidate_unsupported_mass"].mean()
                ),
                "control_unsupported_mass": float(
                    pooled["control_unsupported_mass"].mean()
                ),
                "daily_positive_days": int((daily > 0.0).sum()),
                "daily_negative_days": int((daily < 0.0).sum()),
                "daily_zero_days": int((daily == 0.0).sum()),
                "daily_positive_rate": float((daily > 0.0).mean()),
            }
        )
    contrasts_frame = pd.DataFrame(all_contrasts).sort_values(
        ["scope", "target"]
    )
    action_rows = pd.concat(all_actions, ignore_index=True)
    action_mix = (
        action_rows.groupby(["side", "ope_candidate_action"], sort=True)
        .agg(
            rows=("decision_id", "size"),
            predicted_advantage_mean=("predicted_advantage_over_r0", "mean"),
        )
        .reset_index()
    )
    totals = action_mix.groupby("side")["rows"].transform("sum")
    action_mix["rate"] = action_mix["rows"] / totals
    fold_action_mix = (
        action_rows.groupby(
            ["side", "ope_fold", "ope_candidate_action"], sort=True
        )
        .size()
        .rename("rows")
        .reset_index()
    )
    fold_totals = fold_action_mix.groupby(["side", "ope_fold"])["rows"].transform(
        "sum"
    )
    fold_action_mix["rate"] = fold_action_mix["rows"] / fold_totals

    pooled = contrasts_frame[contrasts_frame["scope"] == "pooled"].set_index(
        "target"
    )
    development_strict_gate = bool(
        pooled.loc["reward", "dr_contrast_p025"] > 0.0
        and pooled.loc["terminal", "dr_contrast_p025"] >= 0.0
        and pooled.loc["tail_avoidance", "dr_contrast_p025"] >= 0.0
    )
    candidate_rate = float(action_rows["candidate_rearm"].mean())
    eligible_for_new_later = bool(development_strict_gate and candidate_rate > 0.0)
    result_metadata = {
        "schema_version": SCHEMA_VERSION,
        "family_id": family["family_id"],
        "feature_family": feature_family,
        "policy_name": policy_name,
        "source_spec_sha256": _sha256(spec_path),
        "source_panel_sha256": _sha256(panel_path),
        "observation_floor_s": identity["observation_floor_s"],
        "observation_floor_is_tunable_policy_parameter": False,
        "development_days": len(days),
        "development_first_day": days[0],
        "development_last_day": days[-1],
        "features": list(feature_names),
        "causal_path_feature_version": (
            CAUSAL_PATH_FEATURE_VERSION
            if family["path_metadata_required"]
            else ""
        ),
        "actions": list(SAFE_ADD_REARM_ACTIONS),
        "default_action": CONTROL_ACTION,
        "candidate_rearm_rate": candidate_rate,
        "development_strict_gate": development_strict_gate,
        "eligible_for_new_later": eligible_for_new_later,
        "later_2026_07_07_through_11_used": False,
        "live_changed": False,
        "cpp_changed": False,
        "baseline_changed": False,
        "config": asdict(config),
    }

    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    contrasts_frame.to_csv(
        output_prefix.with_suffix(".dr_contrasts.csv"), index=False
    )
    paired_frame.to_csv(output_prefix.with_suffix(".dr_rows.csv"), index=False)
    action_rows.to_csv(output_prefix.with_suffix(".action_rows.csv"), index=False)
    action_mix.to_csv(output_prefix.with_suffix(".action_mix.csv"), index=False)
    fold_action_mix.to_csv(
        output_prefix.with_suffix(".fold_action_mix.csv"), index=False
    )
    output_prefix.with_suffix(".metadata.json").write_text(
        json.dumps(result_metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    output_prefix.with_suffix(".report.md").write_text(
        _markdown(contrasts_frame, action_mix, result_metadata), encoding="utf-8"
    )
    return result_metadata


def _base_config(args: argparse.Namespace) -> OPEConfig:
    return OPEConfig(
        reward_col="ope_target",
        split_mode="chronological",
        min_train_days=args.min_train_days,
        test_days=args.test_days,
        embargo_days=args.embargo_days,
        min_train_rows=args.min_train_rows,
        min_action_rows=args.min_action_rows,
        min_behavior_propensity=args.min_behavior_propensity,
        max_importance_weight=args.max_importance_weight,
        max_unsupported_mass=args.max_unsupported_mass,
        min_effective_sample_size=args.min_effective_sample_size,
        ridge_alpha=args.ridge_alpha,
        bootstrap_trials=args.bootstrap_trials,
        random_seed=args.random_seed,
    )


def _common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--development-panel", type=Path, required=True)
    parser.add_argument("--development-metadata", type=Path, required=True)
    parser.add_argument("--development-days", type=Path, required=True)
    parser.add_argument("--source-family", type=Path, required=True)
    parser.add_argument("--path-metadata", type=Path)
    parser.add_argument(
        "--feature-family",
        choices=sorted(POLICY_FAMILIES),
        default="snapshot_v1",
    )
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--min-train-days", type=int, default=50)
    parser.add_argument("--test-days", type=int, default=10)
    parser.add_argument("--embargo-days", type=int, default=1)
    parser.add_argument("--min-train-rows", type=int, default=500)
    parser.add_argument("--min-action-rows", type=int, default=100)
    parser.add_argument("--min-behavior-propensity", type=float, default=0.02)
    parser.add_argument("--max-importance-weight", type=float, default=20.0)
    parser.add_argument("--max-unsupported-mass", type=float, default=0.05)
    parser.add_argument("--min-effective-sample-size", type=float, default=100.0)
    parser.add_argument("--ridge-alpha", type=float, default=10.0)
    parser.add_argument("--bootstrap-trials", type=int, default=1000)
    parser.add_argument("--random-seed", type=int, default=20260714)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    _common_arguments(freeze)
    evaluate = subparsers.add_parser("evaluate")
    _common_arguments(evaluate)
    evaluate.add_argument("--output-prefix", type=Path, required=True)
    evaluate.add_argument("--tail-threshold", type=float, default=-5.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = {
        name: value.expanduser().resolve()
        for name, value in {
            "panel": args.development_panel,
            "metadata": args.development_metadata,
            "days": args.development_days,
            "source_family": args.source_family,
            "spec": args.spec,
        }.items()
    }
    path_metadata = (
        args.path_metadata.expanduser().resolve()
        if args.path_metadata is not None
        else None
    )
    config = _base_config(args)
    if args.command == "freeze":
        result = freeze_spec(
            panel_path=paths["panel"],
            metadata_path=paths["metadata"],
            days_path=paths["days"],
            source_family_path=paths["source_family"],
            path_metadata_path=path_metadata,
            output_path=paths["spec"],
            config=config,
            feature_family=args.feature_family,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return
    result = evaluate_state_policy(
        panel_path=paths["panel"],
        metadata_path=paths["metadata"],
        days_path=paths["days"],
        source_family_path=paths["source_family"],
        path_metadata_path=path_metadata,
        spec_path=paths["spec"],
        output_prefix=args.output_prefix.expanduser().resolve(),
        config=config,
        tail_threshold=args.tail_threshold,
        expected_feature_family=args.feature_family,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
