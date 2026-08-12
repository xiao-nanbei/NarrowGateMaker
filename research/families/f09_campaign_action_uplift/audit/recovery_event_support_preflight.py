#!/usr/bin/env python3
"""Freeze a bounded-footprint recovery-event rearm family without PnL.

The preflight reads only decision-time shock/refill/recovery state plus the
baseline arm's intervention fill count.  It never loads reward, PnL, markout,
campaign terminal, MAE, or duration columns.  The fill-retention estimate is
conservative: every baseline fill attached to a blocked entry row is assumed
lost, even though formal replay may rearm after a later recovery event.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from models.audit.evidence_split import (
    PANEL_ORDER,
    build_explicit_evidence_split,
    sha256_file,
    validate_evidence_split,
)
from models.audit.experiment_manifest import git_workspace_identity
from models.audit.experiment_scorecard import score_profile_contract
from models.replay_policies import (
    STATE_CONDITIONED_REARM_ACTIONS,
    RecoveryEventSpec,
    evaluate_recovery_event,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "recovery_event_rearm_family.v1"
PREFLIGHT_SCHEMA_VERSION = "recovery_event_support_preflight.v1"
POLICY_VERSION = "composite_recovery_v2"
BASELINE_ACTION = "baseline_rearm"
CANDIDATE_ACTION = "continue_block_until_recovery"
SUPPORT_COLUMNS = (
    "day",
    "decision_id",
    "campaign_id",
    "side",
    "action",
    "behavior_propensity",
    "behavior_prob_baseline_rearm",
    "behavior_prob_continue_block_until_recovery",
    "path_feature_valid",
    "path_l2_snapshot_count",
    "path_book_age_ms",
    "shock_adverse_flow_imbalance_1s",
    "shock_adverse_flow_imbalance_5s",
    "shock_adverse_flow_imbalance_since_fill",
    "refill_recovery_ratio",
    "refill_current_vs_start_ratio",
    "recovery_microprice_ratio",
    "intervention_fill_count",
)


def _canonical_sha256(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(raw).hexdigest()


def _sibling_artifact(panel_path: Path, suffix: str) -> Path:
    raw = str(panel_path)
    marker = ".action_panel.csv"
    if not raw.endswith(marker):
        raise ValueError(
            "support panel path must end with .action_panel.csv so its frozen "
            "baseline metadata can be resolved"
        )
    return Path(raw[: -len(marker)] + suffix).resolve()


def _baseline_identity(panel_path: Path) -> dict[str, Any]:
    metadata_path = _sibling_artifact(panel_path, ".metadata.json")
    contract_path = _sibling_artifact(panel_path, ".replay_contract.json")
    if not metadata_path.is_file() or not contract_path.is_file():
        raise FileNotFoundError(
            "support panel requires its frozen metadata and replay contract"
        )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    artifacts = contract.get("artifacts") or {}
    required = {
        "config_sha256": str(metadata.get("config_sha256", "")),
        "p3_sha256": str((artifacts.get("p3") or {}).get("sha256", "")),
        "queue_sha256": str(metadata.get("queue_artifact_sha256", "")),
        "latency_sha256": str(metadata.get("telemetry_sha256", "")),
    }
    missing = sorted(key for key, value in required.items() if not value)
    if missing:
        raise ValueError(f"support baseline identity is incomplete: {missing}")
    return {
        **required,
        "model_sha256": str((artifacts.get("model") or {}).get("sha256", "")),
        "source_replay_contract_sha256": str(
            contract.get("contract_sha256", "")
        ),
        "source_metadata": {
            "path": str(metadata_path),
            "sha256": sha256_file(metadata_path),
        },
        "source_replay_contract": {
            "path": str(contract_path),
            "sha256": sha256_file(contract_path),
        },
    }


def load_support_panel(path: Path, *, side: str) -> pd.DataFrame:
    """Load only causal state and activity-support columns."""

    resolved = path.expanduser().resolve()
    header = pd.read_csv(resolved, nrows=0)
    missing = sorted(set(SUPPORT_COLUMNS) - set(header.columns))
    if missing:
        raise ValueError(f"support panel is missing columns: {missing}")
    frame = pd.read_csv(resolved, usecols=list(SUPPORT_COLUMNS))
    frame["side"] = frame["side"].astype(str).str.upper()
    if set(frame["side"]) != {str(side).upper()}:
        raise ValueError("support panel must contain exactly the requested side")
    if frame["decision_id"].astype(str).duplicated().any():
        raise ValueError("support decision_id must be unique")
    if frame.groupby(["day", "campaign_id"], sort=False).size().max() != 1:
        raise ValueError("support panel must contain one intervention per campaign")
    actions = set(frame["action"].astype(str))
    if actions != set(STATE_CONDITIONED_REARM_ACTIONS):
        raise ValueError("support panel must retain exact two-action overlap")
    if not np.allclose(
        pd.to_numeric(frame["behavior_propensity"], errors="raise"),
        0.5,
        atol=1e-12,
    ):
        raise ValueError("support panel behavior propensity must remain 0.5")
    for column in (
        "behavior_prob_baseline_rearm",
        "behavior_prob_continue_block_until_recovery",
    ):
        if not np.allclose(
            pd.to_numeric(frame[column], errors="raise"),
            0.5,
            atol=1e-12,
        ):
            raise ValueError("support probability vector must remain exact 50/50")
    numeric = frame[list(SUPPORT_COLUMNS[6:])].apply(
        pd.to_numeric, errors="coerce"
    )
    if not np.isfinite(numeric.to_numpy(dtype=float)).all():
        raise ValueError("support state/activity columns must be finite")
    frame[list(SUPPORT_COLUMNS[6:])] = numeric
    return frame


def recovery_scores(
    frame: pd.DataFrame,
    *,
    max_book_age_ms: float = 2_000.0,
    adverse_flow_reference_floor: float = 0.05,
    component_epsilon: float = 1e-6,
) -> pd.DataFrame:
    """Return the exact replay recovery components for every entry row."""

    spec = RecoveryEventSpec(
        score_threshold=1.0,
        max_book_age_ms=max_book_age_ms,
        adverse_flow_reference_floor=adverse_flow_reference_floor,
        component_epsilon=component_epsilon,
    )
    rows = []
    feature_columns = SUPPORT_COLUMNS[8:-1]
    for values in frame[list(feature_columns)].to_dict("records"):
        decision = evaluate_recovery_event(values, spec)
        rows.append(
            {
                "data_valid": int(decision.data_valid),
                "shock_decay_score": decision.shock_decay_score,
                "refill_score": decision.refill_score,
                "microprice_recovery_score": (
                    decision.microprice_recovery_score
                ),
                "queue_recovery_score": decision.queue_recovery_score,
                "recovery_score": decision.recovery_score,
            }
        )
    return pd.DataFrame(rows, index=frame.index)


def build_support_grid(
    frame: pd.DataFrame,
    *,
    quantiles: tuple[float, ...],
    target_candidate_rate: float,
    minimum_candidate_rate: float,
    maximum_candidate_rate: float,
    minimum_preflight_fill_retention: float,
    minimum_candidate_rows: int,
    minimum_candidate_days: int,
    minimum_baseline_fill_events: int,
) -> pd.DataFrame:
    scores = recovery_scores(frame)
    valid = scores["data_valid"].eq(1)
    if not valid.any():
        raise ValueError("support panel has no valid recovery path")
    baseline = frame["action"].astype(str).eq(BASELINE_ACTION)
    fill_events = pd.to_numeric(
        frame["intervention_fill_count"], errors="raise"
    ).clip(lower=0.0)
    baseline_fill_events = float(fill_events[baseline].sum())
    rows: list[dict[str, Any]] = []
    valid_scores = scores.loc[valid, "recovery_score"].to_numpy(dtype=float)
    for quantile in quantiles:
        threshold = float(
            np.quantile(valid_scores, float(quantile), method="higher")
        )
        candidate = valid & scores["recovery_score"].lt(threshold)
        candidate_rate = float(candidate.mean())
        blocked_baseline_fill_events = float(
            fill_events[baseline & candidate].sum()
        )
        fill_retention = (
            1.0 - blocked_baseline_fill_events / baseline_fill_events
            if baseline_fill_events > 0.0
            else 0.0
        )
        candidate_rows = int(candidate.sum())
        candidate_days = int(frame.loc[candidate, "day"].astype(str).nunique())
        eligible = bool(
            minimum_candidate_rate <= candidate_rate <= maximum_candidate_rate
            and fill_retention >= minimum_preflight_fill_retention
            and candidate_rows >= int(minimum_candidate_rows)
            and candidate_days >= int(minimum_candidate_days)
            and baseline_fill_events >= float(minimum_baseline_fill_events)
        )
        rows.append(
            {
                "quantile": float(quantile),
                "score_threshold": threshold,
                "candidate_rate": candidate_rate,
                "distance_to_target_rate": abs(
                    candidate_rate - float(target_candidate_rate)
                ),
                "candidate_rows": candidate_rows,
                "candidate_days": candidate_days,
                "baseline_fill_events": baseline_fill_events,
                "blocked_baseline_fill_events": blocked_baseline_fill_events,
                "conservative_fill_retention": fill_retention,
                "valid_state_rate": float(valid.mean()),
                "eligible": eligible,
            }
        )
    return pd.DataFrame(rows)


def select_support_row(grid: pd.DataFrame) -> dict[str, Any] | None:
    eligible = grid[grid["eligible"].astype(bool)].copy()
    if eligible.empty:
        return None
    eligible = eligible.sort_values(
        [
            "distance_to_target_rate",
            "conservative_fill_retention",
            "candidate_rows",
            "score_threshold",
        ],
        ascending=[True, False, False, True],
        kind="stable",
    )
    return eligible.iloc[0].to_dict()


def _parse_side_path(value: str) -> tuple[str, Path]:
    try:
        side, raw_path = value.split("=", 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be SIDE=PATH") from exc
    side = side.strip().upper()
    if side not in {"BUY", "SELL"}:
        raise argparse.ArgumentTypeError("side must be BUY or SELL")
    return side, Path(raw_path).expanduser().resolve()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel", action="append", type=_parse_side_path, required=True)
    parser.add_argument("--source-evidence-split", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--target-candidate-rate", type=float, default=0.15)
    parser.add_argument("--minimum-candidate-rate", type=float, default=0.05)
    parser.add_argument("--maximum-candidate-rate", type=float, default=0.30)
    parser.add_argument("--minimum-formal-fill-retention", type=float, default=0.85)
    parser.add_argument("--minimum-preflight-fill-retention", type=float, default=0.87)
    parser.add_argument("--minimum-candidate-rows", type=int, default=50)
    parser.add_argument("--minimum-candidate-days", type=int, default=10)
    parser.add_argument("--minimum-baseline-fill-events", type=int, default=100)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if not (
        0.0 < args.minimum_candidate_rate
        <= args.target_candidate_rate
        <= args.maximum_candidate_rate
        < 1.0
    ):
        raise SystemExit("candidate-rate budget is invalid")
    if not (
        0.0 < args.minimum_formal_fill_retention
        <= args.minimum_preflight_fill_retention
        <= 1.0
    ):
        raise SystemExit("fill-retention budgets are invalid")

    source_split_path = args.source_evidence_split.expanduser().resolve()
    source_split = json.loads(source_split_path.read_text(encoding="utf-8"))
    source_panels = validate_evidence_split(source_split)
    source_manifest = Path(source_split["source_manifest_path"]).resolve()
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    quantiles = tuple(float(value) for value in np.arange(0.05, 0.301, 0.01))
    profile = score_profile_contract("action_defense_v1")
    workspace = git_workspace_identity(ROOT)
    results: dict[str, Any] = {}

    supplied: dict[str, Path] = {}
    for side, path in args.panel:
        if side in supplied:
            raise ValueError(f"duplicate support panel for {side}")
        supplied[side] = path

    for side, panel_path in sorted(supplied.items()):
        frame = load_support_panel(panel_path, side=side)
        baseline_identity = _baseline_identity(panel_path)
        grid = build_support_grid(
            frame,
            quantiles=quantiles,
            target_candidate_rate=float(args.target_candidate_rate),
            minimum_candidate_rate=float(args.minimum_candidate_rate),
            maximum_candidate_rate=float(args.maximum_candidate_rate),
            minimum_preflight_fill_retention=float(
                args.minimum_preflight_fill_retention
            ),
            minimum_candidate_rows=int(args.minimum_candidate_rows),
            minimum_candidate_days=int(args.minimum_candidate_days),
            minimum_baseline_fill_events=int(args.minimum_baseline_fill_events),
        )
        selected = select_support_row(grid)
        side_dir = output_dir / side.lower()
        side_dir.mkdir(parents=True, exist_ok=True)
        grid_path = side_dir / "support_grid.csv"
        grid.to_csv(grid_path, index=False)
        if selected is None:
            results[side] = {
                "support_passed": False,
                "support_grid": str(grid_path),
            }
            continue

        family_id = f"{side.lower()}_recovery_event_rearm_v1"
        behavior_probabilities = {
            BASELINE_ACTION: 0.5,
            CANDIDATE_ACTION: 0.5,
        }
        evidence = build_explicit_evidence_split(
            source_manifest,
            family_id=family_id,
            panels={name: source_panels[name] for name in PANEL_ORDER},
            behavior_probabilities=behavior_probabilities,
            sides=[side],
            eligibility=(
                f"first baseline-eligible {side} exposure-increasing add "
                "decision after the actual baseline cooldown; candidate holds "
                "only in the frozen low-recovery state"
            ),
        )
        evidence_path = side_dir / "evidence_split.json"
        evidence_path.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        family = {
            "schema_version": SCHEMA_VERSION,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "family_id": family_id,
            "side": side,
            "policy_version": POLICY_VERSION,
            "status": "frozen_before_outcome_replay",
            "actions": list(STATE_CONDITIONED_REARM_ACTIONS),
            "behavior_probabilities": behavior_probabilities,
            "surface": (
                "first baseline-eligible post-cooldown exposure-increasing add "
                "decision per campaign"
            ),
            "candidate": (
                "continue blocking add quote cycles until the four-path "
                "recovery event is observed"
            ),
            "recovery_event": {
                "score_threshold": float(selected["score_threshold"]),
                "aggregation": "equal_weight_geometric_mean",
                "components": {
                    "shock_decay": (
                        "clip(1 - positive_adverse_flow_1s / "
                        "max(positive_adverse_flow_5s, "
                        "positive_adverse_flow_since_fill, 0.05), 0, 1)"
                    ),
                    "refill": "clip(refill_recovery_ratio, 0, 1)",
                    "microprice_recovery": (
                        "clip(recovery_microprice_ratio, 0, 1)"
                    ),
                    "queue_recovery": (
                        "clip(refill_current_vs_start_ratio, 0, 1)"
                    ),
                },
                "entry": "candidate effective when valid recovery_score < threshold",
                "exit": "release once valid recovery_score >= threshold",
                "invalid_entry_fallback": BASELINE_ACTION,
                "invalid_active_episode": "remain blocked until valid recovery",
                "max_book_age_ms": 2_000.0,
                "component_epsilon": 1e-6,
            },
            "support_only_selection": {
                "schema_version": PREFLIGHT_SCHEMA_VERSION,
                "selection_uses_value_outcomes": False,
                "loaded_columns": list(SUPPORT_COLUMNS),
                "forbidden_metrics": [
                    "reward",
                    "pnl",
                    "markout",
                    "campaign_cost",
                    "terminal_campaign_pnl",
                    "mae",
                    "duration",
                ],
                "target_candidate_rate": float(args.target_candidate_rate),
                "candidate_rate_budget": [
                    float(args.minimum_candidate_rate),
                    float(args.maximum_candidate_rate),
                ],
                "minimum_preflight_fill_retention": float(
                    args.minimum_preflight_fill_retention
                ),
                "minimum_formal_fill_retention": float(
                    args.minimum_formal_fill_retention
                ),
                "selected": selected,
                "fill_retention_interpretation": (
                    "conservative lower bound from baseline-assigned rows; "
                    "assumes every fill attached to a blocked entry is lost"
                ),
            },
            "formal_hard_gates": {
                "candidate_rate": [
                    float(args.minimum_candidate_rate),
                    float(args.maximum_candidate_rate),
                ],
                "fills_retention": float(args.minimum_formal_fill_retention),
                "reducing_side_modified": False,
                "order_size_modified": False,
                "inventory_limit_modified": False,
                "external_reference_used": False,
            },
            "baseline": baseline_identity,
            "invariants": {
                "size_modified": False,
                "order_size_modified": False,
                "reducing_side_modified": False,
                "inventory_limit_modified": False,
                "taker_order_added": False,
                "external_reference_used": False,
            },
            "scorecard_profile": profile,
            "source_support_panel": {
                "path": str(panel_path),
                "sha256": sha256_file(panel_path),
            },
            "source_evidence_split": {
                "path": str(source_split_path),
                "sha256": sha256_file(source_split_path),
            },
            "evidence_split": {
                "path": str(evidence_path),
                "sha256": sha256_file(evidence_path),
            },
            "support_grid": {
                "path": str(grid_path),
                "sha256": sha256_file(grid_path),
            },
            "workspace_identity": workspace,
            "one_intervention_per_campaign": True,
            "reducing_side_modified": False,
            "order_size_modified": False,
            "inventory_limit_modified": False,
            "external_reference_used": False,
        }
        family["family_spec_sha256"] = _canonical_sha256(family)
        family_path = side_dir / "frozen_family_spec.json"
        family_path.write_text(
            json.dumps(family, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        results[side] = {
            "support_passed": True,
            "family_id": family_id,
            "candidate_rate": float(selected["candidate_rate"]),
            "conservative_fill_retention": float(
                selected["conservative_fill_retention"]
            ),
            "score_threshold": float(selected["score_threshold"]),
            "support_grid": str(grid_path),
            "evidence_split": str(evidence_path),
            "family_spec": str(family_path),
            "family_spec_sha256": family["family_spec_sha256"],
        }

    summary_path = output_dir / "preflight_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "schema_version": PREFLIGHT_SCHEMA_VERSION,
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_evidence_split_sha256": sha256_file(source_split_path),
                "results": results,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"summary": str(summary_path), "results": results}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
