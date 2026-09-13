#!/usr/bin/env python3
"""Paired panel audit for the causal post-fill stop-add policy.

The report treats each UTC day as the pairing unit.  Campaign paths can split
after an action changes, so campaign IDs are not falsely paired across arms;
campaign quality is compared through daily aggregate rates and per-campaign
normalizations instead.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


BASELINE_ARM = "baseline"


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return numeric if math.isfinite(numeric) else default


def _weighted_mean(frame: pd.DataFrame, value: str, weight: str) -> float:
    if value not in frame.columns or weight not in frame.columns:
        return math.nan
    values = pd.to_numeric(frame.get(value), errors="coerce")
    weights = pd.to_numeric(frame.get(weight), errors="coerce")
    valid = values.notna() & weights.notna() & (weights > 0)
    if not valid.any():
        return math.nan
    return float(np.average(values[valid], weights=weights[valid]))


def _sum(frame: pd.DataFrame, column: str) -> float:
    if column not in frame.columns:
        return 0.0
    return float(pd.to_numeric(frame.get(column), errors="coerce").fillna(0.0).sum())


def _rate(frame: pd.DataFrame, numerator: str, denominator: str) -> float:
    den = _sum(frame, denominator)
    return _sum(frame, numerator) / den if den > 0.0 else math.nan


def _arm_rows(daily: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    arms = sorted(str(value) for value in daily["arm"].dropna().unique())
    candidates = [arm for arm in arms if arm != BASELINE_ARM]
    if BASELINE_ARM not in arms or len(candidates) != 1:
        raise ValueError(
            f"paired stop-add audit expects baseline plus one candidate; arms={arms}"
        )
    candidate = candidates[0]
    baseline = daily.loc[daily["arm"] == BASELINE_ARM].copy()
    arm = daily.loc[daily["arm"] == candidate].copy()
    if baseline["day"].duplicated().any() or arm["day"].duplicated().any():
        raise ValueError("daily input contains duplicate day/arm rows")
    shared = sorted(set(baseline["day"]) & set(arm["day"]))
    missing = sorted(set(baseline["day"]) ^ set(arm["day"]))
    if missing:
        raise ValueError(f"unpaired UTC days: {missing}")
    return (
        baseline.set_index("day").loc[shared].reset_index(),
        arm.set_index("day").loc[shared].reset_index(),
        candidate,
    )


def summarize_panel(name: str, path: Path) -> dict[str, Any]:
    daily = pd.read_csv(path)
    required = {"day", "arm", "replay_pnl", "terminal_pnl_sum", "fills_total"}
    missing = sorted(required - set(daily.columns))
    if missing:
        raise ValueError(f"{path} missing columns: {missing}")
    baseline, arm, candidate = _arm_rows(daily)

    raw_base = _sum(baseline, "replay_pnl")
    raw_arm = _sum(arm, "replay_pnl")
    terminal_base = _sum(baseline, "terminal_pnl_sum")
    terminal_arm = _sum(arm, "terminal_pnl_sum")
    fills_base = _sum(baseline, "fills_total")
    fills_arm = _sum(arm, "fills_total")
    campaigns_base = _sum(baseline, "campaigns")
    campaigns_arm = _sum(arm, "campaigns")
    tail_base = _sum(baseline, "loss_tail")
    tail_arm = _sum(arm, "loss_tail")
    inv_time_base = _sum(baseline, "replay_abs_inventory_time_s")
    inv_time_arm = _sum(arm, "replay_abs_inventory_time_s")

    raw_delta = pd.to_numeric(arm["replay_pnl"], errors="coerce") - pd.to_numeric(
        baseline["replay_pnl"], errors="coerce"
    )
    terminal_delta = pd.to_numeric(
        arm["terminal_pnl_sum"], errors="coerce"
    ) - pd.to_numeric(baseline["terminal_pnl_sum"], errors="coerce")
    fill_delta = pd.to_numeric(arm["fills_total"], errors="coerce") - pd.to_numeric(
        baseline["fills_total"], errors="coerce"
    )
    inv_delta = pd.to_numeric(
        arm["replay_abs_inventory_time_s"], errors="coerce"
    ) - pd.to_numeric(baseline["replay_abs_inventory_time_s"], errors="coerce")
    changed = (
        raw_delta.abs().fillna(0.0) > 1e-9
    ) | (
        terminal_delta.abs().fillna(0.0) > 1e-9
    ) | (
        fill_delta.abs().fillna(0.0) > 1e-9
    ) | (
        inv_delta.abs().fillna(0.0) > 1e-9
    )

    def _per_100(value: float, count: float) -> float:
        return 100.0 * value / count if count > 0.0 else math.nan

    action_rates: dict[str, dict[str, float]] = {}
    for action in ("place", "replace", "keep", "pause", "pending_coalesce"):
        column = f"decision_{action}_count"
        if column not in daily.columns:
            continue
        action_rates[action] = {
            "baseline": _rate(baseline, column, "decision_total"),
            "arm": _rate(arm, column, "decision_total"),
        }
        action_rates[action]["delta"] = (
            action_rates[action]["arm"] - action_rates[action]["baseline"]
        )

    result = {
        "panel": name,
        "daily_path": str(path),
        "candidate_arm": candidate,
        "days": int(len(baseline)),
        "changed_days": int(changed.sum()),
        "noop_days": int((~changed).sum()),
        "raw": {
            "baseline": raw_base,
            "arm": raw_arm,
            "delta": raw_arm - raw_base,
            "median_daily_delta": _finite(raw_delta.median(), math.nan),
            "positive_days_all": int((raw_delta > 1e-9).sum()),
            "negative_days_all": int((raw_delta < -1e-9).sum()),
            "positive_rate_changed": (
                float((raw_delta[changed] > 1e-9).mean()) if changed.any() else math.nan
            ),
            "activity_adjusted_per_100_fills_delta": (
                _per_100(raw_arm, fills_arm) - _per_100(raw_base, fills_base)
            ),
        },
        "campaign_terminal": {
            "baseline": terminal_base,
            "arm": terminal_arm,
            "delta": terminal_arm - terminal_base,
            "median_daily_delta": _finite(terminal_delta.median(), math.nan),
            "per_100_campaigns_delta": (
                _per_100(terminal_arm, campaigns_arm)
                - _per_100(terminal_base, campaigns_base)
            ),
        },
        "activity": {
            "fills_baseline": fills_base,
            "fills_arm": fills_arm,
            "fills_retention": fills_arm / fills_base if fills_base > 0.0 else math.nan,
            "campaigns_baseline": campaigns_base,
            "campaigns_arm": campaigns_arm,
            "campaign_count_ratio": (
                campaigns_arm / campaigns_base if campaigns_base > 0.0 else math.nan
            ),
            "buy_fill_share_baseline": _weighted_mean(
                baseline, "buy_fill_share", "fills_total"
            ),
            "buy_fill_share_arm": _weighted_mean(arm, "buy_fill_share", "fills_total"),
        },
        "campaign_quality": {
            "tail_baseline": tail_base,
            "tail_arm": tail_arm,
            "tail_delta": tail_arm - tail_base,
            "tail_per_100_campaigns_delta": (
                _per_100(tail_arm, campaigns_arm)
                - _per_100(tail_base, campaigns_base)
            ),
            "bad_rate_baseline": _rate(baseline, "bad_campaigns", "campaigns"),
            "bad_rate_arm": _rate(arm, "bad_campaigns", "campaigns"),
            "repair_rate_baseline": _rate(
                baseline, "repaired_campaigns", "campaigns"
            ),
            "repair_rate_arm": _rate(arm, "repaired_campaigns", "campaigns"),
            "duration_mean_s_baseline": _weighted_mean(
                baseline, "duration_mean_s", "campaigns"
            ),
            "duration_mean_s_arm": _weighted_mean(arm, "duration_mean_s", "campaigns"),
            "early_20m_drawdown_mean_baseline": _weighted_mean(
                baseline, "early_20m_drawdown_mean", "campaigns"
            ),
            "early_20m_drawdown_mean_arm": _weighted_mean(
                arm, "early_20m_drawdown_mean", "campaigns"
            ),
            "max_adverse_excursion_baseline": _weighted_mean(
                baseline, "replay_campaign_max_adverse_excursion", "campaigns"
            ),
            "max_adverse_excursion_arm": _weighted_mean(
                arm, "replay_campaign_max_adverse_excursion", "campaigns"
            ),
        },
        "inventory": {
            "abs_inventory_time_baseline": inv_time_base,
            "abs_inventory_time_arm": inv_time_arm,
            "abs_inventory_time_delta": inv_time_arm - inv_time_base,
            "abs_inventory_time_ratio": (
                inv_time_arm / inv_time_base if inv_time_base > 0.0 else math.nan
            ),
            "max_inventory_baseline": _finite(
                pd.to_numeric(baseline["replay_max_inventory"], errors="coerce").max(),
                math.nan,
            ),
            "max_inventory_arm": _finite(
                pd.to_numeric(arm["replay_max_inventory"], errors="coerce").max(),
                math.nan,
            ),
        },
        "markout": {
            "buy_baseline": _weighted_mean(baseline, "avg_markout_bid", "fills_bid_buy"),
            "buy_arm": _weighted_mean(arm, "avg_markout_bid", "fills_bid_buy"),
            "sell_baseline": _weighted_mean(baseline, "avg_markout_ask", "fills_ask_sell"),
            "sell_arm": _weighted_mean(arm, "avg_markout_ask", "fills_ask_sell"),
        },
        "policy_occupancy": {
            "evaluations": _sum(arm, "multi_market_policy_eval_count"),
            "hits": _sum(arm, "multi_market_policy_hit_count"),
            "effective_blocks": _sum(
                arm, "multi_market_policy_effective_block_count"
            ),
            "bid_effective_blocks": _sum(
                arm, "bid_multi_market_policy_effective_block_count"
            ),
            "ask_effective_blocks": _sum(
                arm, "ask_multi_market_policy_effective_block_count"
            ),
        },
        "action_rates": action_rates,
    }
    result["hard_gates"] = {
        "raw_delta_positive": bool(result["raw"]["delta"] > 0.0),
        "campaign_terminal_delta_positive": bool(
            result["campaign_terminal"]["delta"] > 0.0
        ),
        "fills_retention_ge_98pct": bool(
            result["activity"]["fills_retention"] >= 0.98
        ),
        "tail_not_increased": bool(result["campaign_quality"]["tail_delta"] <= 0.0),
    }
    result["hard_gates"]["all_pass"] = all(result["hard_gates"].values())
    return result


def _fmt(value: Any, digits: int = 3) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{numeric:.{digits}f}" if math.isfinite(numeric) else "n/a"


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Causal post-fill stop-add paired audit",
        "",
        "Each UTC day is paired against the current rolling live baseline. Campaign IDs are not "
        "paired after path divergence; terminal quality is normalized within each panel.",
        "",
        "| Panel | Days / changed | Raw delta | Campaign terminal delta | Fills retained | Tail delta | Inv-time ratio | Gate |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for panel in payload["panels"]:
        lines.append(
            "| {panel} | {days} / {changed} | {raw} | {terminal} | {fills} | {tail} | {inv} | {gate} |".format(
                panel=panel["panel"],
                days=panel["days"],
                changed=panel["changed_days"],
                raw=_fmt(panel["raw"]["delta"]),
                terminal=_fmt(panel["campaign_terminal"]["delta"]),
                fills=_fmt(100.0 * panel["activity"]["fills_retention"], 2) + "%",
                tail=_fmt(panel["campaign_quality"]["tail_delta"], 0),
                inv=_fmt(panel["inventory"]["abs_inventory_time_ratio"], 3),
                gate="PASS" if panel["hard_gates"]["all_pass"] else "FAIL",
            )
        )
    lines.extend(
        [
            "",
            "## Panel details",
            "",
        ]
    )
    for panel in payload["panels"]:
        quality = panel["campaign_quality"]
        activity = panel["activity"]
        occupancy = panel["policy_occupancy"]
        lines.extend(
            [
                f"### {panel['panel']}",
                "",
                f"- Daily raw wins/losses/no-op: {panel['raw']['positive_days_all']} / "
                f"{panel['raw']['negative_days_all']} / {panel['noop_days']}; changed-day win rate "
                f"{_fmt(100.0 * panel['raw']['positive_rate_changed'], 1)}%.",
                f"- Activity-adjusted raw delta per 100 fills: "
                f"{_fmt(panel['raw']['activity_adjusted_per_100_fills_delta'], 4)}; "
                f"campaign terminal delta per 100 campaigns: "
                f"{_fmt(panel['campaign_terminal']['per_100_campaigns_delta'], 4)}.",
                f"- Campaign bad rate: {_fmt(quality['bad_rate_baseline'], 4)} -> "
                f"{_fmt(quality['bad_rate_arm'], 4)}; repair rate: "
                f"{_fmt(quality['repair_rate_baseline'], 4)} -> "
                f"{_fmt(quality['repair_rate_arm'], 4)}.",
                f"- Mean duration: {_fmt(quality['duration_mean_s_baseline'], 1)}s -> "
                f"{_fmt(quality['duration_mean_s_arm'], 1)}s; BUY share: "
                f"{_fmt(activity['buy_fill_share_baseline'], 4)} -> "
                f"{_fmt(activity['buy_fill_share_arm'], 4)}.",
                f"- Policy evaluations/hits/effective blocks: "
                f"{_fmt(occupancy['evaluations'], 0)} / {_fmt(occupancy['hits'], 0)} / "
                f"{_fmt(occupancy['effective_blocks'], 0)} "
                f"(BUY { _fmt(occupancy['bid_effective_blocks'], 0) }, "
                f"SELL { _fmt(occupancy['ask_effective_blocks'], 0) }).",
                "",
            ]
        )
    lines.extend(
        [
            "## Interpretation boundaries",
            "",
            "- `chronological` means each repair model was trained only on earlier UTC days with an embargo. It is not a permanently untouched policy holdout because the project has examined these market days before.",
            "- `blocked71` is trained only on retained39 but is not chronological for every blocked day.",
            "- `late4` uses a model trained through 2026-07-02. The 2026-07-03 market day has appeared in prior diagnostics, so the panel is later-in-time, not pristine forever-OOS evidence.",
            "- External historical inputs are causal one-second right-edge states. This audit can support a post-fill campaign moderator; it cannot establish a 10-100ms cancel or re-center edge.",
            "- A policy hit is a qualifying quote decision. An effective block can still be behaviorally inert when the order lifecycle would not have produced a new fill; changed-day counts expose that distinction.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--panel",
        action="append",
        required=True,
        metavar="NAME=DAILY_CSV",
        help="Repeat for chronological, blocked71, and late4 panels.",
    )
    parser.add_argument("--out-prefix", type=Path, required=True)
    args = parser.parse_args()

    panels = []
    for spec in args.panel:
        if "=" not in spec:
            raise SystemExit(f"invalid --panel {spec!r}; expected NAME=PATH")
        name, raw_path = spec.split("=", 1)
        panels.append(summarize_panel(name.strip(), Path(raw_path).expanduser()))
    payload = {
        "schema_version": "post_fill_stop_add_paired_audit.v1",
        "panels": panels,
        "all_panels_pass": bool(panels) and all(
            panel["hard_gates"]["all_pass"] for panel in panels
        ),
    }
    args.out_prefix.parent.mkdir(parents=True, exist_ok=True)
    json_path = args.out_prefix.with_suffix(".json")
    md_path = args.out_prefix.with_suffix(".md")
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(render_markdown(payload), encoding="utf-8")
    print(json_path)
    print(md_path)


if __name__ == "__main__":
    main()
