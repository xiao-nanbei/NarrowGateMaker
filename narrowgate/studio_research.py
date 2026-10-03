"""Small registered research summaries; never infer economics from file presence."""

import math
import re

SCHEMA = "studio_research_result.v1"
BINDINGS = (
    "experiment_id", "input_manifest_id", "evaluation_contract_id",
    "slice_manifest_id", "split_manifest_id", "latency_profile_id",
    "accounting_contract_id", "initial_state_id", "runtime_contract_id",
)
METRICS = (
    "pnl_before_funding", "funding_cashflow", "all_in_net_pnl", "fee_cost",
    "terminal_inventory", "terminal_unrealized_pnl", "fill_count", "order_count",
)
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}\Z")


def project(value):
    if not isinstance(value, dict) or value.get("schema_version") != SCHEMA:
        raise ValueError("unsupported research result schema")
    result = {"schema_version": SCHEMA}
    for key in (*BINDINGS, "result_id", "arm", "model_id", "currency"):
        item = value.get(key)
        if item is not None and (not isinstance(item, str) or not IDENTIFIER.fullmatch(item)):
            raise ValueError(f"{key}: expected a logical ID, not a file path or URL")
        result[key] = item
    if not result["result_id"]:
        raise ValueError("result_id is required")
    if type(value.get("economic_complete")) is not bool:
        raise ValueError("economic_complete must be explicit")
    metrics = value.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError("metrics must be an object")
    result["metrics"] = {}
    for key in METRICS:
        number = metrics.get(key)
        if number is not None and (type(number) not in (int, float) or not math.isfinite(number)):
            raise ValueError(f"{key}: expected finite number or null")
        result["metrics"][key] = number
    complete = value["economic_complete"]
    if complete and any(result["metrics"][key] is None for key in (
        "pnl_before_funding", "funding_cashflow", "all_in_net_pnl", "fee_cost",
        "terminal_inventory", "terminal_unrealized_pnl",
    )):
        raise ValueError("complete economics requires funding, fees and terminal accounting")
    if complete and not math.isclose(
        result["metrics"]["all_in_net_pnl"],
        result["metrics"]["pnl_before_funding"] + result["metrics"]["funding_cashflow"],
        rel_tol=1e-9, abs_tol=1e-8,
    ):
        raise ValueError("net PnL does not reconcile with signed funding")
    if not complete and result["metrics"]["all_in_net_pnl"] is not None:
        raise ValueError("incomplete economics must keep all_in_net_pnl null")
    result["economic_complete"] = complete
    return result


def from_summaries(summaries):
    results, issues = [], []
    for value in summaries.values():
        if isinstance(value, dict) and value.get("schema_version") == SCHEMA:
            try:
                results.append(project(value))
            except ValueError as exc:
                issues.append(str(exc))
    if len(results) > 1:
        return None, ["Multiple research results: register one result summary per job"]
    return (results[0] if results else None), issues


def compare(left, right):
    differences = [key for key in (*BINDINGS, "currency")
                   if left.get(key) is None or right.get(key) is None or left[key] != right[key]]
    rows = []
    for key in METRICS:
        a, b = left["metrics"][key], right["metrics"][key]
        compatible = not differences and a is not None and b is not None
        rows.append({"metric": key, "left": a, "right": b,
                     "delta": b - a if compatible else None})
    return {"compatible": not differences, "differences": differences, "metrics": rows,
            "economic_comparison_complete": not differences and left["economic_complete"]
            and right["economic_complete"]}
