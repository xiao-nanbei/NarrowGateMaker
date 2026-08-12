#!/usr/bin/env python3
"""Select a safe-add elapsed threshold using action support only.

This preflight deliberately never reads reward, PnL, markout, campaign cost,
MAE, duration, or terminal outcome columns.  It freezes an identifiable action
family before any outcome analysis is permitted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from research.families.f09_campaign_action_uplift.audit.safe_add_rearm_ope_panel import validate_support_panel
from models.replay_policies import SAFE_ADD_REARM_ACTIONS

SCHEMA_VERSION = "safe_add_rearm_support_preflight.v1"
CANDIDATE_ACTIONS = ("r1_rearm", "r2_rearm_widen_1tick")
SIDES = ("BUY", "SELL")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def summarize_support_panel(
    panel: pd.DataFrame,
    *,
    elapsed_s: float,
    min_cell_assignments: int,
    min_cell_filled_orders: int,
) -> tuple[pd.DataFrame, dict]:
    """Return only assignment/submission/fill support for one elapsed value."""

    validate_support_panel(panel)
    normalized = panel.copy()
    normalized["side"] = normalized["side"].astype(str).str.upper()
    normalized["action"] = normalized["action"].astype(str)
    normalized["intervention_order_submit_count"] = pd.to_numeric(
        normalized["intervention_order_submit_count"], errors="raise"
    )
    normalized["intervention_fill_count"] = pd.to_numeric(
        normalized["intervention_fill_count"], errors="raise"
    )

    rows: list[dict] = []
    for side in SIDES:
        for action in SAFE_ADD_REARM_ACTIONS:
            cell = normalized[
                (normalized["side"] == side) & (normalized["action"] == action)
            ]
            rows.append(
                {
                    "elapsed_s": float(elapsed_s),
                    "side": side,
                    "action": action,
                    "assignments": int(len(cell)),
                    "submitted_orders": int(
                        cell["intervention_order_submit_count"].sum()
                    ),
                    "filled_orders": int(
                        (cell["intervention_fill_count"] > 0).sum()
                    ),
                    "fill_events": int(cell["intervention_fill_count"].sum()),
                    "days_with_assignments": int(cell["day"].nunique()),
                    "days_with_fills": int(
                        cell.loc[cell["intervention_fill_count"] > 0, "day"].nunique()
                    ),
                }
            )
    cells = pd.DataFrame(rows)
    candidates = cells[cells["action"].isin(CANDIDATE_ACTIONS)]
    min_assignments = int(candidates["assignments"].min())
    min_filled_orders = int(candidates["filled_orders"].min())
    all_candidate_cells_present = bool((candidates["assignments"] > 0).all())
    support_pass = bool(
        all_candidate_cells_present
        and min_assignments >= int(min_cell_assignments)
        and min_filled_orders >= int(min_cell_filled_orders)
    )
    probability_columns = [
        f"behavior_prob_{action}" for action in SAFE_ADD_REARM_ACTIONS
    ]
    unique_probabilities = normalized[probability_columns].drop_duplicates()
    if len(unique_probabilities) != 1:
        raise ValueError("support preflight requires one frozen propensity vector")
    probabilities = {
        action: float(unique_probabilities.iloc[0][f"behavior_prob_{action}"])
        for action in SAFE_ADD_REARM_ACTIONS
    }
    summary = {
        "elapsed_s": float(elapsed_s),
        "rows": int(len(normalized)),
        "days": int(normalized["day"].nunique()),
        "campaigns": int(
            normalized[["day", "campaign_id"]].drop_duplicates().shape[0]
        ),
        "min_candidate_cell_assignments": min_assignments,
        "min_candidate_cell_filled_orders": min_filled_orders,
        "candidate_filled_orders_total": int(candidates["filled_orders"].sum()),
        "candidate_fill_events_total": int(candidates["fill_events"].sum()),
        "candidate_days_with_fills_min": int(candidates["days_with_fills"].min()),
        "all_candidate_cells_present": all_candidate_cells_present,
        "support_preflight_pass": support_pass,
        "behavior_probabilities": probabilities,
    }
    return cells, summary


def select_elapsed(summaries: pd.DataFrame) -> dict | None:
    """Choose solely by worst-cell support, with longer elapsed as tie-breaker."""

    eligible = summaries[summaries["support_preflight_pass"].astype(bool)].copy()
    if eligible.empty:
        return None
    eligible = eligible.sort_values(
        [
            "min_candidate_cell_filled_orders",
            "min_candidate_cell_assignments",
            "candidate_filled_orders_total",
            "elapsed_s",
        ],
        ascending=[False, False, False, False],
        kind="stable",
    )
    return eligible.iloc[0].to_dict()


def _parse_panel(value: str) -> tuple[float, Path]:
    try:
        elapsed, raw_path = value.split("=", 1)
        return float(elapsed), Path(raw_path).expanduser().resolve()
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("panel must be ELAPSED_SECONDS=PATH") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel", action="append", type=_parse_panel, required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--preflight-spec", type=Path, default=None)
    parser.add_argument("--min-cell-assignments", type=int, default=10)
    parser.add_argument("--min-cell-filled-orders", type=int, default=3)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.min_cell_assignments <= 0 or args.min_cell_filled_orders <= 0:
        raise SystemExit("support thresholds must be positive")
    elapsed_values = [elapsed for elapsed, _ in args.panel]
    if len(elapsed_values) != len(set(elapsed_values)):
        raise SystemExit("each elapsed value may be supplied once")

    all_cells: list[pd.DataFrame] = []
    summaries: list[dict] = []
    sources: list[dict] = []
    for elapsed_s, path in sorted(args.panel):
        panel = pd.read_csv(path)
        cells, summary = summarize_support_panel(
            panel,
            elapsed_s=elapsed_s,
            min_cell_assignments=int(args.min_cell_assignments),
            min_cell_filled_orders=int(args.min_cell_filled_orders),
        )
        all_cells.append(cells)
        summaries.append(summary)
        sources.append(
            {
                "elapsed_s": elapsed_s,
                "path": str(path),
                "sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }
        )

    cells_frame = pd.concat(all_cells, ignore_index=True)
    summary_frame = pd.DataFrame(summaries).drop(columns=["behavior_probabilities"])
    selected = select_elapsed(summary_frame)
    probabilities = summaries[0]["behavior_probabilities"]
    if any(item["behavior_probabilities"] != probabilities for item in summaries[1:]):
        raise ValueError("all elapsed panels must use the same propensity vector")

    prefix = args.output_prefix.expanduser().resolve()
    prefix.parent.mkdir(parents=True, exist_ok=True)
    cells_path = prefix.with_suffix(".support_cells.csv")
    summary_path = prefix.with_suffix(".support_summary.csv")
    family_path = prefix.with_suffix(".frozen_action_family.json")
    cells_frame.to_csv(cells_path, index=False)
    summary_frame.to_csv(summary_path, index=False)
    family = {
        "schema_version": SCHEMA_VERSION,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "selection_uses_outcomes": False,
        "forbidden_selection_metrics": [
            "pnl",
            "reward",
            "markout",
            "campaign_cost",
            "campaign_terminal_pnl",
            "mae",
            "duration",
        ],
        "selection_rule": (
            "pass invariants and thresholds; maximize worst BUY/SELL x R1/R2 "
            "filled-order support, then assignments, total filled orders, and "
            "finally prefer longer elapsed"
        ),
        "thresholds": {
            "min_cell_assignments": int(args.min_cell_assignments),
            "min_cell_filled_orders": int(args.min_cell_filled_orders),
        },
        "selected_elapsed_s": (
            None if selected is None else float(selected["elapsed_s"])
        ),
        "family_frozen": selected is not None,
        "actions": list(SAFE_ADD_REARM_ACTIONS),
        "behavior_probabilities": probabilities,
        "eligibility": "first add-side quote blocked only by fill cooldown",
        "one_intervention_per_campaign": True,
        "reducing_side_modified": False,
        "order_size_modified": False,
        "inventory_limit_modified": False,
        "outcome_status": "embargoed_not_read",
        "sources": sources,
        "artifacts": {
            "support_cells": str(cells_path),
            "support_summary": str(summary_path),
        },
    }
    if args.preflight_spec is not None:
        spec_path = args.preflight_spec.expanduser().resolve()
        family["preflight_spec"] = {
            "path": str(spec_path),
            "sha256": _sha256(spec_path),
            "content": json.loads(spec_path.read_text(encoding="utf-8")),
        }
    family_path.write_text(
        json.dumps(family, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "selected_elapsed_s": family["selected_elapsed_s"],
                "family_frozen": family["family_frozen"],
                "support_cells": str(cells_path),
                "support_summary": str(summary_path),
                "frozen_action_family": str(family_path),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
