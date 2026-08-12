#!/usr/bin/env python3
"""Run a mechanics-only q90 replay on one live-overlap UTC day.

The simulator still performs its normal accounting internally, but this
wrapper accesses and persists only whitelisted scheduler, book, and q90
lifecycle counters.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from models import backtest_tick as bt
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from research.families.f10_live_replay_attribution.audit import (
    buy_q90_portfolio_path_attribution as historical_q90,
)


SCHEMA_VERSION = "buy_q90_same_date_mechanics.v1"
IDENTITY = "buy_q90_live_action_rate_transport_parity_v1_same_date_mechanics"
DEFAULT_SPEC_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "buy_q90_live_action_rate_transport_parity_v1_same_date_mechanics_spec_20260731.json"
)

ALLOWED_RESULT_KEYS = (
    "dynamic_fill_hazard_action_enabled",
    "dynamic_fill_hazard_eval_count",
    "dynamic_fill_hazard_valid_eval_count",
    "dynamic_fill_hazard_invalid_eval_count",
    "dynamic_fill_hazard_keep_count",
    "dynamic_fill_hazard_cancel_request_count",
    "dynamic_fill_hazard_cancel_ack_count",
    "dynamic_fill_hazard_pre_ack_fill_count",
    "dynamic_fill_hazard_recovery_count",
    "dynamic_fill_hazard_reentry_count",
    "dynamic_fill_hazard_cpp_parity_passed",
    "dynamic_fill_hazard_cpp_mismatch_count",
    "dynamic_fill_hazard_cpp_evaluation_count",
    "dynamic_fill_hazard_cpp_lifecycle_count",
    "dynamic_fill_hazard_cpp_identity",
    "exchange_book_events_consumed",
    "exchange_book_source_gap_events",
    "exchange_book_invalid_sequence_messages",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def canonical_spec_sha256(spec: Mapping[str, Any]) -> str:
    payload = dict(spec)
    payload.pop("canonical_spec_sha256", None)
    return canonical_sha256(payload)


def load_spec(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("same-date q90 mechanics spec must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected same-date q90 mechanics schema")
    if payload.get("identity") != IDENTITY:
        raise ValueError("unexpected same-date q90 mechanics identity")
    if payload.get("status") != "frozen_before_same_date_replay_output_read":
        raise ValueError("same-date q90 mechanics status drifted")
    frozen = str(payload.get("canonical_spec_sha256", ""))
    if len(frozen) != 64 or canonical_spec_sha256(payload) != frozen:
        raise ValueError("same-date q90 mechanics spec hash mismatch")
    if payload.get("day") != "2026-07-25":
        raise ValueError("same-date q90 mechanics day drifted")
    if payload.get("quality_grade") != "B":
        raise ValueError("same-date q90 mechanics must remain Grade-B sensitivity")
    if not bool(payload.get("economic_outputs_prohibited", False)):
        raise ValueError("same-date q90 mechanics cannot access economics")
    permissions = payload.get("permissions") or {}
    if not permissions or any(bool(value) for value in permissions.values()):
        raise ValueError("same-date q90 mechanics cannot grant permissions")
    return payload


def _require_identity(path: Path, expected: str, label: str) -> None:
    if not Path(path).is_file():
        raise FileNotFoundError(f"{label} is missing: {path}")
    actual = sha256_file(Path(path))
    if actual != str(expected):
        raise ValueError(
            f"{label} hash mismatch: expected {expected}, found {actual}"
        )


def _mechanics_only(result: Mapping[str, Any]) -> dict[str, Any]:
    output = {key: result.get(key) for key in ALLOWED_RESULT_KEYS}
    evaluations = int(output["dynamic_fill_hazard_eval_count"] or 0)
    valid = int(output["dynamic_fill_hazard_valid_eval_count"] or 0)
    cancels = int(output["dynamic_fill_hazard_cancel_request_count"] or 0)
    output.update(
        {
            "valid_probability_per_evaluation": (
                valid / evaluations if evaluations > 0 else math.nan
            ),
            "cancel_probability_per_evaluation": (
                cancels / evaluations if evaluations > 0 else math.nan
            ),
            "cancel_probability_per_valid_evaluation": (
                cancels / valid if valid > 0 else math.nan
            ),
        }
    )
    return output


def run_same_date(spec_path: Path) -> dict[str, Any]:
    spec = load_spec(spec_path)
    for key, label in (
        ("normalized_l2", "same-date normalized L2"),
        ("normalized_bbo", "same-date normalized BBO"),
        ("individual_trades", "same-date individual trades"),
    ):
        identity = spec["source_identity"][key]
        _require_identity(Path(identity["path"]), str(identity["sha256"]), label)

    parent_spec_path = Path(spec["parent_historical_q90_spec"]["path"])
    _require_identity(
        parent_spec_path,
        str(spec["parent_historical_q90_spec"]["sha256"]),
        "parent historical q90 spec",
    )
    historical_spec = historical_q90._load_json(parent_spec_path)
    historical_q90.validate_spec(historical_spec)
    source = copy.deepcopy(
        historical_q90._runtime_source_contract(historical_spec)
    )
    normalized_l2_path = Path(
        spec["source_identity"]["normalized_l2"]["path"]
    ).expanduser().resolve()
    normalized_root = normalized_l2_path.parents[1]
    source["source_identity"]["normalized_l2_root"] = str(normalized_root)
    bt.BBO_DIR = normalized_root / "bbo"
    bt.L2_DIR = normalized_root / "l2"
    day = str(spec["day"])
    base_params = historical_q90.full_path._configure_params(source, day)
    window = historical_q90.full_path._load_window(source, day, base_params)
    params = historical_q90._configure_arm_params(
        source,
        day,
        historical_spec,
        q90_enabled=True,
    )
    params.update(
        {
            "trace_fills_max": 0,
            "trace_decisions_max": 0,
            "trace_quotes_max": 0,
            "trace_campaign_repair_max": 0,
            "trace_first_add_decision_to_terminal_max": 0,
            "trace_first_opener_decision_to_terminal_max": 0,
            "collect_curves": False,
            "window_cache_write_enabled": False,
            "replay_promotion_eligible": False,
        }
    )
    tape = CryptoHFTExchangeBookTape(
        raw_root=Path(source["source_identity"]["native_orderbook_root"]),
        day=day,
        symbol="BTCUSDC",
        tick_size=float(params.get("tick_size", bt.TICK)),
        warmup_hours=int(
            source["replay_contract"]["native_warmup_hours"]
        ),
        strict_complete=True,
    )
    started = time.monotonic()
    result = bt._simulate_tick_with_engine(
        "python",
        window["trades"],
        window["var_ts_ms"],
        window["var_ssq"],
        params,
        ml_data=None,
        bbo_data=window.get("bbo_data"),
        l2_data=window.get("l2_data"),
        var_ti=window.get("var_ti"),
        var_retsq=window.get("var_retsq"),
        exchange_book_event_tape=tape,
    )
    runtime_s = float(time.monotonic() - started)
    mechanics = _mechanics_only(result)
    if int(mechanics["exchange_book_source_gap_events"] or 0) != 0:
        raise RuntimeError("same-date q90 native source gap detected")
    if int(mechanics["exchange_book_invalid_sequence_messages"] or 0) != 0:
        raise RuntimeError("same-date q90 native sequence failure")
    if not bool(mechanics["dynamic_fill_hazard_cpp_parity_passed"]):
        raise RuntimeError("same-date q90 Python/C++ parity failed")
    if int(mechanics["dynamic_fill_hazard_cpp_mismatch_count"] or 0) != 0:
        raise RuntimeError("same-date q90 Python/C++ mismatch detected")

    trade_ts = pd.to_numeric(
        window["trades"]["transact_time"], errors="raise"
    )
    elapsed_hours = max(
        1e-12,
        float(trade_ts.iloc[-1] - trade_ts.iloc[0]) / 3_600_000.0,
    )
    cancels = int(mechanics["dynamic_fill_hazard_cancel_request_count"])
    evaluations = int(mechanics["dynamic_fill_hazard_eval_count"])
    output = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "day": day,
        "quality_grade": str(spec["quality_grade"]),
        "quality_scope": str(spec["quality_scope"]),
        "runtime_s": runtime_s,
        "elapsed_hours": elapsed_hours,
        "mechanics": mechanics,
        "rates": {
            "evaluations_per_hour": evaluations / elapsed_hours,
            "cancel_requests_per_hour": cancels / elapsed_hours,
            "cancel_requests_per_evaluation": (
                cancels / evaluations if evaluations > 0 else math.nan
            ),
        },
        "economic_outputs_read": False,
        "permissions": dict(spec["permissions"]),
    }
    return output


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC_PATH)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    output = _json_safe(run_same_date(args.spec))
    path = Path(args.output).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            output,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps(output["rates"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
