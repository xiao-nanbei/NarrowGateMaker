from __future__ import annotations

import argparse
import csv
import json
from datetime import date, timedelta
from pathlib import Path

from research.families.f09_campaign_action_uplift.audit.campaign_tail_score import run


def _write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def test_campaign_tail_score_is_side_specific_and_add_only(tmp_path: Path) -> None:
    paths: list[Path] = []
    start = date(2026, 1, 1)
    for day_idx in range(10):
        day = (start + timedelta(days=day_idx)).isoformat()
        rows: list[dict[str, str]] = []
        for side in ("BUY", "SELL"):
            q_before = "0.004" if side == "BUY" else "-0.004"
            for idx in range(12):
                tail = idx >= 8
                score_feature = (idx / 11.0) if side == "BUY" else (1.0 - idx / 11.0)
                rows.append({
                    "day": day,
                    "utc": f"{day}T00:00:{idx:02d}Z",
                    "client_order_id": f"{day}-{side}-{idx}",
                    "side": side,
                    "q_before": q_before,
                    "inventory_role": "add",
                    "filled": "1",
                    "fill_ts": str(1_800_000_000 + day_idx * 86_400 + idx),
                    "filled_qty": "0.001",
                    "fill_q_before": q_before,
                    "fill_inventory_role": "add",
                    "fill_role_source": "exact_trace",
                    "campaign_id": f"{side}-{idx}",
                    "campaign_outcome_risk_score": f"{score_feature:.6f}",
                    "quote_distance_bps": f"{2.0 + score_feature:.6f}",
                    "near_depth_total": "1.5",
                    "queue_local_rank": "0.5",
                    "terminal_campaign_label": "loss_tail" if tail else "positive_flat",
                    "terminal_campaign_tail_loss": "1" if tail else "0",
                    "terminal_final_total_pnl_delta": "-2.0" if tail else "1.0",
                    "terminal_early_drawdown_20m": "2.5" if tail else "0.1",
                    "terminal_campaign_duration_s": "4000" if tail else "300",
                    "terminal_campaign_max_abs_inventory": "0.012" if tail else "0.005",
                })
        # These rows prove that opener/reducing orders stay out of the add-on denominator.
        rows.extend([
            {
                "day": day,
                "client_order_id": f"{day}-opener",
                "side": "BUY",
                "q_before": "0",
                "filled": "0",
            },
            {
                "day": day,
                "client_order_id": f"{day}-reducing",
                "side": "SELL",
                "q_before": "0.004",
                "filled": "0",
            },
        ])
        path = tmp_path / f"{day}.order_level.csv"
        _write_rows(path, rows)
        paths.append(path)

    prefix = tmp_path / "tail_score"
    summary = run(argparse.Namespace(
        order_level_csv=[str(path) for path in paths],
        order_level_filelist=None,
        out_prefix=str(prefix),
        folds=5,
        bins=4,
        alpha=2.0,
        contribution_scale=0.8,
        clip_contribution=2.0,
        initial_inventory=0.0,
        target_mode="loss_tail",
        include_preexisting_tail_state=False,
    ))

    assert summary["denominator"]["buy_add_orders"] == 120
    assert summary["denominator"]["sell_add_orders"] == 120
    assert summary["side_summary"]["BUY"]["oos_auc_tail_risk"] > 0.7
    assert summary["side_summary"]["SELL"]["oos_auc_tail_risk"] > 0.7

    score_path = Path(summary["outputs"]["score_extension"])
    with score_path.open(newline="") as f:
        scores = list(csv.DictReader(f))
    assert len(scores) == 240
    assert {row["inventory_role"] for row in scores} == {"add"}
    assert all(row["addon_campaign_tail_score_oos"] for row in scores)

    model = json.loads(Path(summary["outputs"]["model"]).read_text(encoding="utf-8"))
    assert set(model["full_shadow_models"]) == {"BUY", "SELL"}
