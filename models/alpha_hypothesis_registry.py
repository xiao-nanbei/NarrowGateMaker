#!/usr/bin/env python3
"""Generate a mechanism hypothesis registry from alpha evidence ledger outputs.

The registry is a bridge between evidence and parameter testing.  It is not a
backtest runner: it classifies bucket evidence into rejected, sparse-watch,
risk-control, or testable hypotheses so we do not jump straight into a noisy
parameter sweep.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import backtest_tick as bt  # noqa: E402
from models.symbol_paths import DEFAULT_SYMBOL  # noqa: E402


EX_POST_FAMILIES = {"shock_label"}
XMARKET_FAMILIES = {
    "reference_confirm",
    "spot_confirm",
    "ref_adverse_ret",
    "spot_adverse_ret",
    "xmarket_confirm",
}
LOCAL_MECHANISM_FAMILIES = {
    "guard_state",
    "reason_bucket",
    "near_depth",
    "queue_rank",
    "distance",
    "toxicity",
    "quote_ev_pred",
    "raw_bias",
}


def _read_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def _ledger_stem_path(stem: str) -> Path:
    path = Path(stem).expanduser()
    if path.suffix:
        # Allow passing one concrete ledger output path.
        name = path.name
        for suffix in (
            ".bucket_stability.csv",
            ".fill_rollup.csv",
            ".daily_summary.csv",
            ".json",
            ".md",
        ):
            if name.endswith(suffix):
                return path.parent / name[: -len(suffix)]
    return path


def _mechanism_family(row: pd.Series) -> str:
    family = str(row.get("bucket_family", ""))
    bucket = str(row.get("bucket", ""))
    if family in EX_POST_FAMILIES:
        return "ex_post_attribution_only"
    if family in XMARKET_FAMILIES:
        if "adverse" in bucket or "confirmed" in bucket:
            return "xmarket_reference_retreat"
        if "favorable" in bucket or "unconfirmed" in bucket or "none_confirmed" in bucket:
            return "xmarket_favorable_or_unconfirmed"
        return "xmarket_other"
    if family in {"guard_state", "reason_bucket"}:
        return "local_guard_or_reason"
    if family in LOCAL_MECHANISM_FAMILIES:
        return "local_microstructure"
    return "misc"


def _entry_condition(row: pd.Series) -> str:
    family = str(row.get("bucket_family", ""))
    bucket = str(row.get("bucket", ""))
    side = str(row.get("side", ""))
    if family == "reference_confirm":
        return f"{side}: reference_confirmation == {bucket.replace('ref_', '')}"
    if family == "spot_confirm":
        return f"{side}: spot_confirmation == {bucket.replace('spot_', '')}"
    if family == "xmarket_confirm":
        return f"{side}: xmarket_confirm_bucket == {bucket}"
    if family == "ref_adverse_ret":
        return f"{side}: ref_adverse_ret_bucket == {bucket}"
    if family == "spot_adverse_ret":
        return f"{side}: spot_adverse_ret_bucket == {bucket}"
    if family == "guard_state":
        return f"{side}: guard_state == {bucket}"
    if family == "reason_bucket":
        return f"{side}: queue_reason_bucket == {bucket}"
    if family == "shock_label":
        return f"{side}: shock_label == {bucket} (ex-post only)"
    return f"{side}: {family} == {bucket}"


def _expected_effect(row: pd.Series) -> str:
    family = str(row.get("bucket_family", ""))
    bucket = str(row.get("bucket", ""))
    ev = float(row.get("weighted_avg_ev_30s", 0.0) or 0.0)
    if family == "shock_label":
        return "explain only; never use as live gate"
    if ev < 0 and ("confirmed" in bucket or "adverse" in bucket):
        return "risk-control retreat: widen/reduce-size/shorten-TTL on exposure-increasing side"
    if ev > 0 and ("favorable" in bucket or "unconfirmed" in bucket or "none_confirmed" in bucket):
        return "alpha watch: avoid over-defending; require more support before any narrowing"
    if ev > 0:
        return "alpha watch: validate with daily OOS and live/shadow labels"
    return "reject as alpha; keep only if it reduces tail risk without hiding toxic fills"


def _promotion_status(row: pd.Series, *, min_support_days: int, min_total_fills: int) -> str:
    family = str(row.get("bucket_family", ""))
    verdict = str(row.get("verdict", ""))
    fills = int(row.get("total_fills", 0) or 0)
    support_days = int(row.get("support_days", 0) or 0)
    ev = float(row.get("weighted_avg_ev_30s", 0.0) or 0.0)
    bucket = str(row.get("bucket", ""))
    if family in EX_POST_FAMILIES:
        return "ex_post_only"
    if fills < min_total_fills:
        return "ignore_sparse"
    if verdict == "stable_positive" and support_days >= min_support_days:
        return "testable"
    if verdict == "sparse_cross_day_positive" or (ev > 0 and support_days < min_support_days):
        return "sparse_watch"
    if ev < 0 and ("confirmed" in bucket or "adverse" in bucket) and support_days >= min_support_days:
        return "risk_control_retreat_evidence"
    if verdict == "negative_or_mixed":
        return "rejected"
    return "diagnostic"


def _next_test(row: pd.Series, status: str) -> str:
    family = str(row.get("bucket_family", ""))
    bucket = str(row.get("bucket", ""))
    if status == "testable":
        return "create 1-3 minimal daily smoke arms; do not widen parameter grid yet"
    if status == "sparse_watch":
        return "increase support with more retained days and live/shadow labels; no parameter arm yet"
    if status == "risk_control_retreat_evidence":
        return "design conservative retreat arm: widen/size-cut/TTL-cut only on exposure-increasing side"
    if status == "ex_post_only":
        return "keep for attribution reports only; prohibit live/replay gating"
    if status == "rejected":
        return "do not tune around this bucket; keep as rejected evidence unless new OOS data reverses it"
    if family == "shock_label" or "absorbed" in bucket:
        return "audit only because label may depend on future markout"
    return "manual review"


def _risk(row: pd.Series, status: str) -> str:
    family = str(row.get("bucket_family", ""))
    if status == "sparse_watch":
        return "sample is positive in rollup but no sufficient support day; high false-discovery risk"
    if status == "risk_control_retreat_evidence":
        return "can reduce toxic fills but may reduce inventory-reducing quotes or lower fill count if implemented too broadly"
    if family == "shock_label":
        return "look-ahead leakage if used as a gate"
    if status == "testable":
        return "may optimize replay bucket; require shadow/live confirmation"
    return "low immediate risk because no promotion is allowed"


def build_registry(stability: pd.DataFrame, *, min_support_days: int, min_total_fills: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if stability.empty:
        return pd.DataFrame()
    for _, row in stability.iterrows():
        status = _promotion_status(row, min_support_days=min_support_days, min_total_fills=min_total_fills)
        if status == "ignore_sparse":
            continue
        side = str(row.get("side", ""))
        family = str(row.get("bucket_family", ""))
        bucket = str(row.get("bucket", ""))
        mechanism = _mechanism_family(row)
        rows.append({
            "hypothesis_id": f"{side.lower()}__{family}__{bucket}".replace(" ", "_").replace("+", "plus").replace("=", "eq"),
            "mechanism_family": mechanism,
            "side": side,
            "bucket_family": family,
            "bucket": bucket,
            "entry_condition": _entry_condition(row),
            "expected_effect": _expected_effect(row),
            "evidence_source": "alpha_evidence_ledger.bucket_stability",
            "support_fills": int(row.get("total_fills", 0) or 0),
            "support_days": int(row.get("support_days", 0) or 0),
            "positive_days": int(row.get("positive_days", 0) or 0),
            "positive_support_days": int(row.get("positive_support_days", 0) or 0),
            "weighted_avg_ev_30s": float(row.get("weighted_avg_ev_30s", 0.0) or 0.0),
            "weighted_positive_30s_rate": float(row.get("weighted_positive_30s_rate", 0.0) or 0.0),
            "worst_day_ev_30s": float(row.get("worst_day_ev_30s", 0.0) or 0.0),
            "best_day_ev_30s": float(row.get("best_day_ev_30s", 0.0) or 0.0),
            "ledger_verdict": str(row.get("verdict", "")),
            "promotion_status": status,
            "risk": _risk(row, status),
            "next_test": _next_test(row, status),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    order = {
        "testable": 0,
        "risk_control_retreat_evidence": 1,
        "sparse_watch": 2,
        "diagnostic": 3,
        "rejected": 4,
        "ex_post_only": 5,
    }
    out["_sort"] = out["promotion_status"].map(order).fillna(99)
    out = out.sort_values(
        ["_sort", "weighted_avg_ev_30s", "support_fills"],
        ascending=[True, False, False],
    ).drop(columns=["_sort"])
    return out


def _df_to_md(frame: pd.DataFrame, *, max_rows: int = 80) -> str:
    if frame.empty:
        return "_No rows._"
    shown = frame.head(max_rows).copy()
    for col in shown.columns:
        if pd.api.types.is_float_dtype(shown[col]):
            shown[col] = shown[col].map(lambda v: "" if pd.isna(v) else f"{v:.6g}")
    lines = [
        "| " + " | ".join(shown.columns) + " |",
        "|" + "|".join("---" for _ in shown.columns) + "|",
    ]
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[col]) if not pd.isna(row[col]) else "" for col in shown.columns) + " |")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--ledger-stem", required=True, help="Stem for alpha_evidence_ledger_<tag>_<symbol> outputs.")
    parser.add_argument("--tag", default=time.strftime("%Y%m%d_%H%M%S"))
    parser.add_argument("--min-support-days", type=int, default=3)
    parser.add_argument("--min-total-fills", type=int, default=30)
    args = parser.parse_args()

    bt.configure_symbol(args.symbol)
    stem = _ledger_stem_path(args.ledger_stem)
    stability = _read_required(stem.with_suffix(".bucket_stability.csv"))
    registry = build_registry(
        stability,
        min_support_days=args.min_support_days,
        min_total_fills=args.min_total_fills,
    )

    out_stem = bt.RESULTS_DIR / f"alpha_hypothesis_registry_{args.tag}_{args.symbol.lower()}"
    csv_path = out_stem.with_suffix(".csv")
    json_path = out_stem.with_suffix(".json")
    md_path = out_stem.with_suffix(".md")
    registry.to_csv(csv_path, index=False)
    payload = {
        "symbol": args.symbol.upper(),
        "ledger_stem": str(stem),
        "min_support_days": args.min_support_days,
        "min_total_fills": args.min_total_fills,
        "rows": int(len(registry)),
        "status_counts": registry["promotion_status"].value_counts().to_dict() if not registry.empty else {},
        "outputs": {
            "csv": str(csv_path),
            "json": str(json_path),
            "markdown": str(md_path),
        },
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = [
        "# Alpha Hypothesis Registry",
        "",
        "This file classifies evidence buckets before any parameter test.",
        "",
        "## Status Counts",
        "",
        _df_to_md(pd.DataFrame(payload["status_counts"].items(), columns=["status", "count"])),
        "",
        "## Registry",
        "",
        _df_to_md(registry),
        "",
    ]
    md_path.write_text("\n".join(lines), encoding="utf-8")
    for path in (csv_path, json_path, md_path):
        print(f"Saved {path}")
    if registry.empty:
        print("No registry rows.")
    else:
        print(registry[[
            "hypothesis_id",
            "promotion_status",
            "support_fills",
            "support_days",
            "weighted_avg_ev_30s",
            "next_test",
        ]].head(30).to_string(index=False))


if __name__ == "__main__":
    main()
