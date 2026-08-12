#!/usr/bin/env python3
"""Fail-closed alignment of the owner full-path control to the 50-day baseline."""

from __future__ import annotations

import argparse
import json
import math
import os
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from data_paths import data_root

IDENTITY = "causal_multichannel_window_boolean_cooldown_owner_full_path_baseline_alignment_v1"
CONTROL_ARM = "current_live_held_global_ber_control"
DEFAULT_OWNER_ROOT = Path(tempfile.gettempdir()) / (
    "causal_multichannel_window_boolean_cooldown_owner_full_path_v1"
)
DEFAULT_REFERENCE = data_root(Path(__file__).resolve().parents[4]) / (
    "reports/"
    "current_live_held_ber_baseline_50d_20260810/daily.parquet"
)
DEFAULT_TOLERANCE = 1e-9

EXACT_METRICS = (
    "fills_total",
    "fills_bid",
    "fills_ask",
    "campaign_count",
    "campaign_closed_count",
)
FLOAT_METRICS = (
    "terminal_mtm_pnl_usdc",
    "closed_campaign_value_usdc",
    "campaign_terminal_value_usdc",
    "campaign_accounting_error_usdc",
    "final_inventory_btc",
    "max_inventory_btc",
    "abs_inventory_time_btc_s",
    "campaign_q10_usdc",
    "campaign_cvar10_usdc",
    "campaign_mae_usdc",
    "negative_campaign_terminal_value_usdc",
    "multi_level_long_terminal_value_usdc",
    "multi_level_short_terminal_value_usdc",
    "buy_maker_value_30s_bps",
    "sell_maker_value_30s_bps",
)


class BaselineAlignmentError(RuntimeError):
    """Raised when the owner control cannot be aligned to the reference baseline."""


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw_tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp = Path(raw_tmp)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def _load_owner_controls(owner_root: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    days_root = owner_root / "days"
    if not days_root.is_dir():
        raise BaselineAlignmentError(f"owner day root is missing: {days_root}")
    for success in sorted(days_root.glob("*/_SUCCESS")):
        summary_path = success.parent / "summary.json"
        if not summary_path.is_file():
            raise BaselineAlignmentError(f"admitted day has no summary: {summary_path}")
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
        controls = [
            dict(row)
            for row in payload.get("arms", ())
            if str(row.get("arm")) == CONTROL_ARM
        ]
        if len(controls) != 1:
            raise BaselineAlignmentError(
                f"{success.parent.name} must contain exactly one control row"
            )
        rows.append(controls[0])
    if not rows:
        raise BaselineAlignmentError("no admitted owner control days were found")
    frame = pd.DataFrame(rows).sort_values("day").reset_index(drop=True)
    if frame["day"].duplicated().any():
        raise BaselineAlignmentError("owner control contains duplicate UTC days")
    return frame


def _load_reference(reference_path: Path) -> pd.DataFrame:
    if not reference_path.is_file():
        raise BaselineAlignmentError(f"reference baseline is missing: {reference_path}")
    frame = pd.read_parquet(reference_path)
    frame = frame.loc[frame["arm"].astype(str).eq(CONTROL_ARM)].copy()
    frame = frame.sort_values("day").reset_index(drop=True)
    if frame.empty or frame["day"].duplicated().any():
        raise BaselineAlignmentError("reference baseline denominator is invalid")
    return frame


def validate_alignment(
    *,
    owner_root: Path = DEFAULT_OWNER_ROOT,
    reference_path: Path = DEFAULT_REFERENCE,
    tolerance: float = DEFAULT_TOLERANCE,
    require_complete: bool = False,
) -> dict[str, Any]:
    if not math.isfinite(tolerance) or tolerance < 0.0:
        raise BaselineAlignmentError("tolerance must be finite and nonnegative")
    owner = _load_owner_controls(owner_root)
    reference = _load_reference(reference_path)
    reference_days = reference["day"].astype(str).tolist()
    owner_days = owner["day"].astype(str).tolist()
    unknown_days = sorted(set(owner_days) - set(reference_days))
    missing_days = [day for day in reference_days if day not in set(owner_days)]
    if unknown_days:
        raise BaselineAlignmentError(f"owner control has unknown UTC days: {unknown_days}")
    if require_complete and missing_days:
        raise BaselineAlignmentError(
            f"complete alignment requires all reference days; missing: {missing_days}"
        )

    joined = owner.merge(
        reference,
        on="day",
        how="inner",
        validate="one_to_one",
        suffixes=("_owner", "_reference"),
    )
    metric_results: dict[str, Any] = {}
    failures: list[dict[str, Any]] = []
    for metric in EXACT_METRICS + FLOAT_METRICS:
        left_name = f"{metric}_owner"
        right_name = f"{metric}_reference"
        if left_name not in joined or right_name not in joined:
            raise BaselineAlignmentError(f"alignment metric is missing: {metric}")
        left = pd.to_numeric(joined[left_name], errors="raise")
        right = pd.to_numeric(joined[right_name], errors="raise")
        delta = (left - right).abs()
        allowed = 0.0 if metric in EXACT_METRICS else tolerance
        bad = delta > allowed
        max_abs = float(delta.max()) if len(delta) else 0.0
        metric_results[metric] = {
            "max_abs_mismatch": max_abs,
            "mismatch_days": int(bad.sum()),
            "allowed_abs_mismatch": allowed,
        }
        for idx in joined.index[bad]:
            failures.append(
                {
                    "day": str(joined.at[idx, "day"]),
                    "metric": metric,
                    "owner": float(left.at[idx]),
                    "reference": float(right.at[idx]),
                    "absolute_mismatch": float(delta.at[idx]),
                    "allowed_abs_mismatch": allowed,
                }
            )

    passed = not failures and (not require_complete or not missing_days)
    return {
        "schema_version": "1.0",
        "identity": IDENTITY,
        "status": "passed" if passed else "failed",
        "require_complete": require_complete,
        "tolerance": tolerance,
        "owner_root": str(owner_root.resolve()),
        "reference_path": str(reference_path.resolve()),
        "reference_day_count": len(reference_days),
        "aligned_day_count": len(joined),
        "missing_reference_days": missing_days,
        "unknown_owner_days": unknown_days,
        "metric_results": metric_results,
        "failures": failures,
        "economic_interpretation_allowed": bool(passed and require_complete),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner-root", type=Path, default=DEFAULT_OWNER_ROOT)
    parser.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    parser.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE)
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    report = validate_alignment(
        owner_root=args.owner_root,
        reference_path=args.reference,
        tolerance=args.tolerance,
        require_complete=args.require_complete,
    )
    if args.output is not None:
        _atomic_json(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
