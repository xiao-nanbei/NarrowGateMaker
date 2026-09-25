"""Independent-account settlement for complete shared-input execution traces."""

import math
from hashlib import sha256

from data.runtime import ConsumerBundle
from models.replay.continuous_accounting import funding_cashflow_usdc


def settle_public_replay(root, result, *, initial_capital, max_mark_age_ns, funding=None):
    """MTM, not synthetic liquidation; settlement precedes equal-time fills.

    Funding is a separately verified accounting input. Its explicit schedule
    must be completely represented; an empty/missing response is not zero.
    Cash is reconstructed and reconciled with the executor before reporting.
    """
    if not math.isfinite(initial_capital) or initial_capital <= 0:
        raise ValueError("positive finite initial capital required")
    if type(max_mark_age_ns) is not int or max_mark_age_ns < 0:
        raise ValueError("nonnegative integer valuation age required")
    bundle = ConsumerBundle(root)
    plan = bundle.manifest["plan"]
    if result["public_input_contract"].get("input_manifest_id") != sha256((bundle.root / "manifest.json").read_bytes()).hexdigest():
        raise ValueError("economic result input manifest mismatch")
    start = result["public_input_contract"]["account_start_ns"]
    end = plan["end_ns"]
    if type(start) is not int or not plan["start_ns"] <= start < end:
        raise ValueError("invalid independent account interval")
    fills = result["_fill_trace"]
    if len(fills) != result["fills_total"]:
        raise ValueError("incomplete fill trace cannot establish economics")
    if [r["fill_sequence"] for r in fills] != list(range(len(fills))):
        raise ValueError("fill trace must preserve producer notification sequence")
    delayed = result.get("private_fill_visibility_enabled", False)
    if delayed:
        if result.get("economic_fill_contract") != "match_facts_and_local_notifications.v1":
            raise ValueError("delayed fills require producer matching facts; notification sorting is not a migration")
        facts = result["_economic_fill_trace"]
        if len(facts) != result["private_fill_exchange_match_count"]:
            raise ValueError("incomplete economic matching facts")
        if (any(type(r["match_sequence"]) is not int for r in facts)
                or [r["match_sequence"] for r in facts] != list(range(len(facts)))):
            raise ValueError("economic matching identities must be unique and contiguous")
        notified = set()
        local_cash = local_q = 0.0
        last_visible = None
        for row in fills:
            seq = row["economic_match_sequence"]
            if type(seq) is not int or not 0 <= seq < len(facts) or seq in notified:
                raise ValueError("duplicate or missing economic notification reference")
            fact = facts[seq]
            for name in ("order_id", "fill_ts", "side", "fill_qty", "quote_px", "fill_fee_usdc"):
                if row[name] != fact[name]:
                    raise ValueError(f"notification differs from matching fact: {name}")
            clock = row["fill_clock_context"]
            if not (clock["match_ts_ms"] == fact["fill_ts"]
                    <= clock["visible_ts_ms"] <= clock["processed_ts_ms"]):
                raise ValueError("invalid matching/notification clock binding")
            if last_visible is not None and clock["processed_ts_ms"] < last_visible:
                raise ValueError("local notification processing clock regressed")
            last_visible = clock["processed_ts_ms"]
            notified.add(seq)
            signed = fact["fill_qty"] * (1 if fact["side"] == "BUY" else -1)
            local_q += signed
            local_cash -= signed * fact["quote_px"] + fact["fill_fee_usdc"]
        if (len(notified) != result["private_fill_visible_count"]
                or len(facts) - len(notified) != result["private_fill_pending_visibility_count"]):
            raise ValueError("matching and notification counts do not reconcile")
        if not math.isclose(local_q, result["final_inventory"], abs_tol=1e-10):
            raise ValueError("local notification inventory does not reconcile")
        if not math.isclose(local_cash, result["cash_before_terminal"], abs_tol=1e-8):
            raise ValueError("local notification cash ledger does not reconcile")
        # No sort: this list was captured at the matching sites, including facts
        # whose notification has not arrived by the account cutoff.
        fills = facts
    events = []
    for row in fills:
        ts = row["fill_ts"] * 1_000_000
        if type(row["fill_ts"]) is not int or not start <= ts < end:
            raise ValueError("fill outside independent account interval")
        if row["side"] not in {"BUY", "SELL"} or row["fill_qty"] <= 0 or row["quote_px"] <= 0:
            raise ValueError("invalid economic fill")
        if not all(math.isfinite(row[k]) for k in ("fill_qty", "quote_px", "fill_fee_usdc")):
            raise ValueError("nonfinite economic fill")
        events.append((ts, 1, row))
    if any(a[0] > b[0] for a, b in zip(events, events[1:], strict=False)):
        raise ValueError("economic fill clock regressed")
    if funding is not None:
        if (funding["market_id"] != plan["market_id"] or
                funding["coverage_start_ns"] > start or funding["coverage_end_ns"] < end
                or not funding.get("source_identity")):
            raise ValueError("funding market, coverage and source binding required")
        rows = funding["events"]
        clocks = [row["settlement_ns"] for row in rows]
        expected = funding["expected_settlements_ns"]
        if (clocks != sorted(set(clocks)) or clocks != expected
                or any(type(t) is not int or not start < t <= end for t in clocks)):
            raise ValueError("funding schedule incomplete or outside account")
        for row in rows:
            funding_cashflow_usdc(0., row["mark_price"], row["rate"])
            events.append((row["settlement_ns"], 0, row))
    cash = q = fees = payments = realized = entry = 0.
    for _, kind, row in sorted(events, key=lambda item: (item[0], item[1])):
        if kind == 0:
            payment = funding_cashflow_usdc(q, row["mark_price"], row["rate"])
            payments += payment
            continue
        size = row["fill_qty"] * (1 if row["side"] == "BUY" else -1)
        price, fee = row["quote_px"], row["fill_fee_usdc"]
        cash -= size * price + fee
        fees += fee
        new_q = q + size
        if q == 0 or q * size > 0:
            entry = (abs(q) * entry + abs(size) * price) / abs(new_q)
        else:
            realized += min(abs(q), abs(size)) * (price - entry) * (1 if q > 0 else -1)
            if q * new_q < 0:
                entry = price
            elif abs(new_q) < 1e-12:
                new_q, entry = 0., 0.
        q = new_q
    terminal_inventory = result["economic_match_inventory"] if delayed else result["final_inventory"]
    terminal_cash = result["economic_match_cash"] if delayed else result["cash_before_terminal"]
    if delayed and not math.isclose(q, result["exchange_inventory_at_window_end"], abs_tol=1e-10):
        raise ValueError("economic matching facts do not reconcile with exchange inventory")
    if not math.isclose(q, terminal_inventory, abs_tol=1e-10):
        raise ValueError("terminal inventory does not reconcile")
    if not math.isclose(cash, terminal_cash, abs_tol=1e-8):
        raise ValueError("cash ledger does not reconcile; unsupported non-fill cashflow")
    # Only materialize the selected terminal observation, never a Python
    # object graph for the complete multi-day Top20 tape.
    import numpy as np
    last = None
    for depth in bundle.batches("depth"):
        eligible = np.flatnonzero(depth["ready_ns"].to_numpy() < end)
        if len(eligible):
            last = depth.slice(int(eligible[-1]), 1).to_pylist()[0]
    age = None if last is None or last["source_asof_ns"] is None else end - last["source_asof_ns"]
    mark = None
    if (last and last["valid"] and not last["stale"] and age is not None
            and 0 <= age <= max_mark_age_ns and last["bid_px"] and last["ask_px"]):
        mark = (float(last["bid_px"][0]) + float(last["ask_px"][0])) / 2
    before = cash if q == 0 else None if mark is None else cash + q * mark
    complete = before is not None and funding is not None
    return {"accounting_contract": "public_independent_mtm.v1", "initial_capital": initial_capital,
            "fill_clock_basis": "producer_matching_facts" if delayed else "immediate_execution_trace",
            "economic_fills_total": len(fills),
            "account_start_ns": start, "account_end_ns": end,
            "terminal_inventory": q, "realized_trading_pnl": realized,
            "terminal_unrealized_pnl": 0. if q == 0 else None if mark is None else q * (mark - entry),
            "fees": fees, "funding_cashflow": payments if funding is not None else None,
            "pnl_before_funding": before,
            "all_in_net_pnl": before + payments if complete else None,
            "terminal_equity": initial_capital + before + payments if complete else None,
            "economic_complete": complete, "economic_admission": False,
            "valuation_price": mark, "valuation_age_ns": age,
            "valuation_origin": "delivered_BBO_mid_not_official_mark",
            "terminal_liquidation_applied": False,
            "funding_policy_feedback": "post_execution_accounting_only",
            "tie_policy": "funding_before_equal_time_fills_end_settlement_before_MTM"}
