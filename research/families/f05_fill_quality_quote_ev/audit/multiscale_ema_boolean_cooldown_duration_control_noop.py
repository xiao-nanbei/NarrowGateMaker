#!/usr/bin/env python3
"""Attest that disabled duration research hooks reproduce the 40-day control."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import uuid
from pathlib import Path
from typing import Any

import pandas as pd

from data_paths import data_root
from models import backtest_tick as bt
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_add_wait_incremental_value_v1_1_study as source_runner,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
BASELINE = ROOT / (
    "research/families/f10_live_replay_attribution/docs/"
    "current_live_held_ber_replay_baseline_40d_20260809.json"
)
DEFAULT_OUTPUT = DATA_ROOT / (
    "reports/"
    "multiscale_ema_boolean_cooldown_duration_policy_v1_20260809/"
    "control_noop_attestation.json"
)

CORE_FILL_FIELDS = (
    "order_id",
    "side",
    "fill_ts",
    "fill_qty",
    "price",
    "inventory_before_fill",
    "inventory_after_fill",
    "queue_before",
    "rem_before",
)


class NoopAttestationError(RuntimeError):
    """Fail closed when current disabled-hook replay drifts from control."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _fill_fingerprint(frame: pd.DataFrame) -> str:
    missing = sorted(set(CORE_FILL_FIELDS) - set(frame.columns))
    if missing:
        raise NoopAttestationError(f"fill fingerprint schema is incomplete: {missing}")
    rows: list[dict[str, Any]] = []
    for row in frame.loc[:, CORE_FILL_FIELDS].to_dict("records"):
        rows.append(
            {
                name: (
                    str(value)
                    if name == "side"
                    else int(value)
                    if name in {"order_id", "fill_ts"}
                    else float(value)
                )
                for name, value in row.items()
            }
        )
    return _canonical_sha256(rows)


def attest(*, output: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
    daily_path = Path(str(baseline["source"]["daily_path"]))
    fills_path = Path(str(baseline["source"]["fills_path"]))
    if _sha256(daily_path) != baseline["source"]["daily_sha256"]:
        raise NoopAttestationError("authoritative daily artifact hash drifted")
    if _sha256(fills_path) != baseline["source"]["fills_sha256"]:
        raise NoopAttestationError("authoritative fill artifact hash drifted")
    expected_daily = pd.read_parquet(daily_path).set_index("day")
    expected_fills = pd.read_parquet(fills_path)
    spec, plan = source_runner._spec_and_plan()
    days = tuple(baseline["panel"]["ordered_utc_days"])
    audits: list[dict[str, Any]] = []
    for day in days:
        window, _, params, audit = source_runner._load_day_inputs(
            day,
            spec=spec,
            plan=plan,
        )
        replay_params = dict(params)
        replay_params["trace_fills_max"] = 1_000_000
        replay_params["trace_cooldown_duration_opportunities_max"] = 0
        replay_params["cooldown_duration_fork_enabled"] = False
        result = bt._simulate_tick_with_engine(
            "cpp",
            window.trades,
            window.var_ts_ms,
            window.var_ssq,
            replay_params,
            **audit["shared"],
        )
        observed = pd.DataFrame(result.get("_fill_trace") or ())
        expected = expected_fills.loc[
            expected_fills["day"].astype(str).eq(day)
        ].reset_index(drop=True)
        if len(observed) != len(expected):
            raise NoopAttestationError(f"{day} control fill count drifted")
        observed_fingerprint = _fill_fingerprint(observed)
        expected_fingerprint = _fill_fingerprint(expected)
        if observed_fingerprint != expected_fingerprint:
            raise NoopAttestationError(f"{day} control fill path drifted")
        expected_day = expected_daily.loc[day]
        checks = {
            "terminal_mtm_pnl_usdc": (
                float(result["pnl"]),
                float(expected_day["terminal_mtm_pnl_usdc"]),
            ),
            "final_inventory_btc": (
                float(result["final_inventory"]),
                float(expected_day["final_inventory_btc"]),
            ),
            "fills_bid": (
                float(result["fills_bid"]),
                float(expected_day["fills_bid"]),
            ),
            "fills_ask": (
                float(result["fills_ask"]),
                float(expected_day["fills_ask"]),
            ),
        }
        for name, (left, right) in checks.items():
            if not math.isclose(left, right, rel_tol=0.0, abs_tol=1e-9):
                raise NoopAttestationError(f"{day} control {name} drifted")
        audits.append(
            {
                "utc_day": day,
                "fill_count": int(len(observed)),
                "fill_fingerprint_sha256": observed_fingerprint,
                "terminal_mtm_pnl_usdc": float(result["pnl"]),
                "final_inventory_btc": float(result["final_inventory"]),
                "fills_bid": int(result["fills_bid"]),
                "fills_ask": int(result["fills_ask"]),
            }
        )
    cpp_extension_path = Path(bt._load_cpp_tick_replay().__file__).resolve()
    payload = {
        "schema_version": (
            "multiscale_ema_boolean_cooldown_duration_policy_v1."
            "control_noop_attestation.v1"
        ),
        "identity": "multiscale_ema_boolean_cooldown_duration_policy_v1",
        "status": "passed",
        "baseline_path": str(BASELINE),
        "baseline_sha256": _sha256(BASELINE),
        "backtest_tick_sha256": _sha256(ROOT / "models/backtest_tick.py"),
        "cpp_runtime": {
            "extension_path": str(cpp_extension_path),
            "extension_sha256": _sha256(cpp_extension_path),
            "tick_replay_cpp_sha256": _sha256(
                ROOT / "cpp/narrowgate_cpp/tick_replay.cpp"
            ),
            "tick_replay_hpp_sha256": _sha256(
                ROOT / "cpp/narrowgate_cpp/tick_replay.hpp"
            ),
            "bindings_cpp_sha256": _sha256(
                ROOT / "cpp/narrowgate_cpp/bindings.cpp"
            ),
        },
        "day_count": len(audits),
        "fill_count": int(sum(row["fill_count"] for row in audits)),
        "all_fill_paths_equal": True,
        "all_daily_accounting_equal": True,
        "candidate_economic_outcomes_read": False,
        "days": audits,
    }
    _atomic_json(output, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    result = attest(output=args.output)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "output_sha256": _sha256(args.output),
                "status": result["status"],
                "day_count": result["day_count"],
                "fill_count": result["fill_count"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
