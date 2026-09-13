"""Identity-bound comparison of the one frozen F04 paired economic account.

The independent accounts are compared, never spliced into a continuous return.
This reader does not run an executor, fit a model, or select a winner.
"""

from __future__ import annotations

import json
import math
import os
from collections import defaultdict
from pathlib import Path
from uuid import uuid4

import pandas as pd

from data.facts import save_private_json
from research.families.f04_external_market_alpha.tardis_direction import _sha


def _arm(root: Path, expected_arm: str) -> tuple[dict, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    receipt = json.loads((root / "accounting.json").read_text())
    if (receipt.get("schema") != "narrowgate.f04.first_pair_economic_arm.v1"
            or receipt.get("arm") != expected_arm
            or receipt.get("accounting", {}).get("economic_complete") is not True
            or receipt["accounting"].get("all_in_net_pnl") is None):
        raise ValueError(f"{expected_arm} is not a complete F04 economic account")
    frames = []
    for name in ("fills.parquet", "orders.parquet", "decisions.parquet"):
        spec = receipt.get("trace_files", {}).get(name)
        if not spec or _sha(root / name) != spec["sha256"]:
            raise ValueError(f"{expected_arm} trace identity changed: {name}")
        frame = pd.read_parquet(root / name)
        if len(frame) != spec["rows"]:
            raise ValueError(f"{expected_arm} trace row count changed: {name}")
        frames.append(frame)
    if _sha(root / "utc-equity-marks.json") != receipt["utc_equity_marks_sha256"]:
        raise ValueError(f"{expected_arm} UTC mark identity changed")
    if any(receipt["trace_coverage"][name]["unrecorded"] for name in (
            "fills", "order_outcomes", "routed_decisions")):
        raise ValueError(f"{expected_arm} retained trace is truncated")
    return receipt, *frames


def _fill_risk(fills: pd.DataFrame, account: dict) -> dict:
    required = {"fill_sequence", "fill_ts", "side", "fill_qty", "quote_px", "fill_fee_usdc"}
    if not required <= set(fills):
        raise ValueError("F04 fill trace lacks the complete economic fields")
    if fills["fill_sequence"].tolist() != list(range(len(fills))):
        raise ValueError("F04 fills lost physical execution order")
    start, end = account["account_start_ns"], account["account_end_ns"]
    prev, inventory, peak, abs_time = start, 0., 0., 0.
    quantity = notional = fees = 0.
    side_counts = {"BUY": 0, "SELL": 0}
    side_quantity = {"BUY": 0., "SELL": 0.}
    for row in fills.itertuples(index=False):
        ts = int(row.fill_ts) * 1_000_000
        if not start <= prev <= ts < end or row.side not in side_counts:
            raise ValueError("F04 fill time or side invalid")
        size, price, fee = float(row.fill_qty), float(row.quote_px), float(row.fill_fee_usdc)
        if not all(math.isfinite(value) for value in (size, price, fee)) or size <= 0 or price <= 0:
            raise ValueError("F04 fill quantity, price or fee invalid")
        abs_time += abs(inventory) * (ts - prev) / 3_600_000_000_000
        inventory += size if row.side == "BUY" else -size
        peak = max(peak, abs(inventory))
        side_counts[row.side] += 1
        side_quantity[row.side] += size
        quantity += size
        notional += size * price
        fees += fee
        prev = ts
    abs_time += abs(inventory) * (end - prev) / 3_600_000_000_000
    if (not math.isclose(inventory, account["terminal_inventory"], abs_tol=1e-10)
            or not math.isclose(fees, account["fees"], abs_tol=1e-8)):
        raise ValueError("F04 reconstructed inventory or fees differ from settlement")
    parts = (account["realized_trading_pnl"] + account["terminal_unrealized_pnl"]
             - account["fees"] + account["funding_cashflow"])
    if not math.isclose(parts, account["all_in_net_pnl"], abs_tol=1e-8):
        raise ValueError("F04 economic decomposition does not reconcile")
    return {"fills": len(fills), "fill_counts_by_side": side_counts,
            "btc_quantity_by_side": side_quantity, "absolute_btc_quantity": quantity,
            "usdc_fill_notional": notional, "peak_absolute_inventory_btc": peak,
            "absolute_inventory_time_btc_hours": abs_time}


def _order_summary(orders: pd.DataFrame) -> dict:
    required = {"order_id", "side", "outcome", "price", "quantity"}
    if not required <= set(orders):
        raise ValueError("F04 order outcomes lack actual order identity or price")
    return {"outcome_events": len(orders),
            "distinct_order_ids": int(orders["order_id"].nunique()),
            "outcomes": {str(key): int(value) for key, value in
                         orders["outcome"].value_counts(dropna=False).items()},
            "sides": {str(key): int(value) for key, value in
                      orders["side"].value_counts(dropna=False).items()}}


def _decision_changes(left: pd.DataFrame, right: pd.DataFrame) -> dict:
    required = {"decision_id", "final_price", "final_size", "action", "pred_dir"}
    if not required <= set(left) or not required <= set(right):
        raise ValueError("F04 routed decisions lack actual quote/action fields")

    def indexed(frame):
        ordinals = defaultdict(int)
        rows = {}
        for row in frame.loc[:, list(required)].to_dict("records"):
            name = row["decision_id"]
            key = (name, ordinals[name])
            ordinals[name] += 1
            rows[key] = row
        return rows

    a, b = indexed(left), indexed(right)
    common = a.keys() & b.keys()
    return {"matched_decision_occurrences": len(common),
            "M0_only_decision_occurrences": len(a.keys() - b.keys()),
            "M1_only_decision_occurrences": len(b.keys() - a.keys()),
            "changed_action": sum(a[key]["action"] != b[key]["action"] for key in common),
            "changed_final_quote_price": sum(a[key]["final_price"] != b[key]["final_price"]
                                             for key in common),
            "changed_final_quote_size": sum(a[key]["final_size"] != b[key]["final_size"]
                                            for key in common),
            "changed_direction_prediction": sum(a[key]["pred_dir"] != b[key]["pred_dir"]
                                                for key in common),
            "matching_rule": "exact_decision_id_and_within_id_occurrence_not_state_matched_counterfactual"}


def compare_economic_pair(m0_root: str | Path, m1_root: str | Path,
                          output: str | Path) -> dict:
    """Read both accepted arms, preserve all denominators, publish once."""
    m0, fills0, orders0, decisions0 = _arm(Path(m0_root), "M0")
    m1, fills1, orders1, decisions1 = _arm(Path(m1_root), "M1")
    shared = ("plan_sha256", "source_archive_sha256", "model_manifest_sha256", "execution_manifest_sha256",
              "frozen_f03_manifest_sha256", "config_sha256", "timing_sha256",
              "funding_source_identity")
    if any(m0[key] != m1[key] for key in shared) or m0["input"] != m1["input"]:
        raise ValueError("F04 M0/M1 did not share one frozen economic environment")
    if m0["reference_manifest_sha256"] is not None or m1["reference_manifest_sha256"] is None:
        raise ValueError("F04 local/reference information arms were not separated")
    a, b = m0["accounting"], m1["accounting"]
    for key in ("account_start_ns", "account_end_ns", "initial_capital",
                "valuation_origin", "funding_policy_feedback", "tie_policy"):
        if a[key] != b[key]:
            raise ValueError(f"F04 paired account contract changed: {key}")
    risk0, risk1 = _fill_risk(fills0, a), _fill_risk(fills1, b)
    order0, order1 = _order_summary(orders0), _order_summary(orders1)
    decision_changes = _decision_changes(decisions0, decisions1)
    result = {
        "schema": "narrowgate.f04.tardis_first_pair_comparison.v1",
        "plan_sha256": m0["plan_sha256"],
        "independent_accounts_not_continuous_return": True,
        "M0": {"all_in_net_pnl_usdc": a["all_in_net_pnl"],
               "realized_trading_pnl_usdc": a["realized_trading_pnl"],
               "terminal_unrealized_pnl_usdc": a["terminal_unrealized_pnl"],
               "fees_usdc": a["fees"], "funding_cashflow_usdc": a["funding_cashflow"],
               "terminal_inventory_btc": a["terminal_inventory"],
               "terminal_valuation_price_usdc_per_btc": a["valuation_price"],
               "order_outcomes_recorded": len(orders0), "routed_decisions_recorded": len(decisions0),
               "order_summary": order0, **risk0},
        "M1": {"all_in_net_pnl_usdc": b["all_in_net_pnl"],
               "realized_trading_pnl_usdc": b["realized_trading_pnl"],
               "terminal_unrealized_pnl_usdc": b["terminal_unrealized_pnl"],
               "fees_usdc": b["fees"], "funding_cashflow_usdc": b["funding_cashflow"],
               "terminal_inventory_btc": b["terminal_inventory"],
               "terminal_valuation_price_usdc_per_btc": b["valuation_price"],
               "order_outcomes_recorded": len(orders1), "routed_decisions_recorded": len(decisions1),
               "order_summary": order1, **risk1},
        "M1_minus_M0_net_pnl_usdc": b["all_in_net_pnl"] - a["all_in_net_pnl"],
        "decision_changes": decision_changes,
        "risk_scope": "fill-reconstructed_inventory_only; intraday_drawdown_unknown_without_marked_equity_path",
        "economic_receipt_sha256": {"M0": _sha(Path(m0_root) / "accounting.json"),
                                   "M1": _sha(Path(m1_root) / "accounting.json")},
    }
    output = Path(output)
    if output.exists():
        raise FileExistsError("F04 pair comparison is create-only")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = output.with_name(output.name + "." + uuid4().hex + ".part")
    save_private_json(stage, result)
    try:
        os.link(stage, output)
    finally:
        stage.unlink()
    return result
