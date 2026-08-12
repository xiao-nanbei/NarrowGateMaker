#!/usr/bin/env python3
"""Produce the frozen F10 first-opener native Development trace.

This is a baseline replay producer, not an evaluator. It runs the current
corrected wall-time baseline over the preregistered 22 Grade-A and 11 Grade-B
Development days, checkpoints each day atomically, and refuses later panels.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import pandas as pd

from data import normalized_l2_registry as l2_registry
from models import backtest_tick as bt
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from research.families.f09_campaign_action_uplift.audit import (
    volatility_time_add_rearm_full_path_preflight as full_path,
)
from research.families.f10_live_replay_attribution.audit import (
    first_opener_decision_to_terminal_contract as contract,
)
from research.governance.paths import resolve_research_path

SCHEMA_VERSION = "first_opener_decision_to_terminal_native_producer.v3"
RUN_SCHEMA_VERSION = "first_opener_decision_to_terminal_native_run.v3"
BAR_PAYLOAD_SCHEMA_VERSION = "first_opener_replay_bar_payload_manifest.v1"
IDENTITY = "first_opener_decision_to_terminal_native_producer_v3"
ROOT = Path(__file__).resolve().parents[4]
DEFAULT_PRODUCER_SPEC_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "first_opener_decision_to_terminal_native_producer_v3_spec_20260730.json"
)


def _development_days(f10_spec: Mapping[str, Any]) -> tuple[str, ...]:
    panels = f10_spec.get("panels") or {}
    return tuple(
        sorted(
            {
                *map(str, panels.get("development_primary_grade_a_days", ())),
                *map(
                    str,
                    panels.get("development_sensitivity_grade_b_days", ()),
                ),
            }
        )
    )


def _previous_natural_utc_day(day: str) -> str:
    return (pd.Timestamp(day) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")


REQUIRED_IMPLEMENTATION_PATHS = frozenset(
    {
        "research/families/f10_live_replay_attribution/audit/first_opener_decision_to_terminal_runner.py",
        "research/families/f10_live_replay_attribution/audit/first_opener_decision_to_terminal_contract.py",
        "research/families/f09_campaign_action_uplift/audit/volatility_time_add_rearm_full_path_preflight.py",
        "models/backtest_tick.py",
        "models/backtest_config.py",
        "models/data_windows.py",
        "models/exchange_book_replay.py",
        "models/replay_contract.py",
        "strategy/fill_cooldown.py",
        "strategy/replay_controls.py",
        "strategy/dynamic_fill_hazard_model.py",
        "strategy/quote_core.py",
        "strategy/policy_guards.py",
        "strategy/signal.py",
        "live/config.py",
        "tests/test_first_opener_decision_to_terminal_runner.py",
        "tests/test_first_opener_decision_to_terminal_contract.py",
        "tests/test_first_add_decision_to_terminal_native_trace.py",
    }
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_spec_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_spec_sha256", None)
    raw = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _require_identity(path: Path, expected: str, label: str) -> None:
    resolved = resolve_research_path(path, require_exists=False)
    if not resolved.is_file():
        raise FileNotFoundError(f"{label} is missing: {resolved}")
    actual = sha256_file(resolved)
    if actual != str(expected):
        raise ValueError(
            f"{label} hash mismatch: expected {expected}, found {actual}"
        )


def _load_json(path: Path) -> dict[str, Any]:
    resolved = resolve_research_path(path)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("native producer spec must be a JSON object")
    return payload


def _load_json_array(path: Path) -> list[dict[str, Any]]:
    resolved = resolve_research_path(path)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not all(
        isinstance(row, dict) for row in payload
    ):
        raise ValueError(f"expected a JSON array of objects: {path}")
    return payload


def _verify_payload_record(record: Mapping[str, Any], label: str) -> dict[str, Any]:
    path = resolve_research_path(
        str(record.get("path", "")), require_exists=False
    )
    expected_size = int(record.get("bytes", record.get("size_bytes", -1)))
    expected_sha = str(record.get("sha256", ""))
    if not path.is_file():
        raise FileNotFoundError(f"{label} payload is missing: {path}")
    actual_size = int(path.stat().st_size)
    if expected_size < 0 or actual_size != expected_size:
        raise ValueError(
            f"{label} payload size drifted: expected {expected_size}, "
            f"found {actual_size}: {path}"
        )
    _require_identity(path, expected_sha, label)
    return {
        "path": str(path),
        "bytes": actual_size,
        "sha256": expected_sha,
        "source_type": str(record.get("source_type", "")),
    }


def _trade_payload_record(base: Mapping[str, Any], day: str) -> dict[str, Any]:
    manifest_path = Path(
        str(base["execution_trade_identity"]["manifest"]["path"])
    ).expanduser()
    manifest = _load_json(manifest_path)
    matches = [
        row
        for row in manifest.get("daily_files", ())
        if str(row.get("day", "")) == str(day)
    ]
    if len(matches) != 1:
        raise ValueError(f"execution-trade manifest identity is ambiguous on {day}")
    row = matches[0]
    return {
        "path": str(row.get("raw_file", "")),
        "bytes": int(row.get("raw_size_bytes", -1)),
        "sha256": str(row.get("raw_sha256", "")),
        "source_type": "individual_execution_trades",
    }


def _market_payload_records(
    producer: Mapping[str, Any],
    day: str,
) -> list[dict[str, Any]]:
    manifest_path = Path(
        str(producer["market_source_manifest_identity"]["path"])
    ).expanduser()
    rows = _load_json_array(manifest_path)
    prefix = f"{day}:"
    selected = [
        row
        for row in rows
        if any(
            str(use) == str(day) or str(use).startswith(prefix)
            for use in row.get("used_by", ())
        )
    ]
    source_types = {str(row.get("source_type", "")) for row in selected}
    required_types = {
        "normalized_bbo",
        "normalized_l2",
        "native_snapshot_delta",
    }
    if not selected or not required_types.issubset(source_types):
        raise ValueError(f"market payload manifest is incomplete on {day}")
    return selected


def _bar_payload_records(
    producer: Mapping[str, Any],
    day: str,
) -> list[dict[str, Any]]:
    identity = producer.get("bar_payload_manifest_identity") or {}
    manifest_path = Path(str(identity.get("path", ""))).expanduser()
    manifest = _load_json(manifest_path)
    if manifest.get("schema_version") != BAR_PAYLOAD_SCHEMA_VERSION:
        raise ValueError("first-opener bar payload manifest schema drifted")
    matches = [
        row
        for row in manifest.get("days", ())
        if str(row.get("day", "")) == str(day)
    ]
    if len(matches) != 1:
        raise ValueError(f"bar payload manifest identity is ambiguous on {day}")
    records = list(matches[0].get("payloads", ()))
    if not records:
        raise ValueError(f"bar payload manifest is empty on {day}")
    return records


def _verify_day_payloads(
    producer: Mapping[str, Any],
    base: Mapping[str, Any],
    day: str,
) -> dict[str, Any]:
    records = [
        _trade_payload_record(base, day),
        *_market_payload_records(producer, day),
        *_bar_payload_records(producer, day),
    ]
    verified = [
        _verify_payload_record(record, f"{day}:{index}")
        for index, record in enumerate(records)
    ]
    canonical = json.dumps(
        verified,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return {
        "day": str(day),
        "payload_count": int(len(verified)),
        "payload_identity_sha256": hashlib.sha256(canonical).hexdigest(),
        "source_type_counts": {
            source_type: int(
                sum(row["source_type"] == source_type for row in verified)
            )
            for source_type in sorted({row["source_type"] for row in verified})
        },
    }


def build_bar_payload_manifest(
    f10_spec: Mapping[str, Any],
    output_path: Path,
    *,
    bars_root: Path | None = None,
) -> dict[str, Any]:
    """Freeze only the causal 1s bar files consumed by this producer."""

    contract.validate_spec(f10_spec)
    target_days = list(_development_days(f10_spec))
    root = Path(bars_root or bt.BARS_DIR).expanduser().resolve()
    days: list[dict[str, Any]] = []
    for day in target_days:
        target = pd.Timestamp(day, tz="UTC")
        context_days = (
            (target - pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
            target.strftime("%Y-%m-%d"),
        )
        payloads: list[dict[str, Any]] = []
        for source_day in context_days:
            path = root / f"BTCUSDC-1s-{source_day}.parquet"
            if not path.is_file():
                raise FileNotFoundError(
                    f"causal variance bar payload is missing for {day}: {path}"
                )
            payloads.append(
                {
                    "path": str(path),
                    "bytes": int(path.stat().st_size),
                    "sha256": sha256_file(path),
                    "source_type": "causal_1s_variance_bar",
                    "source_day": source_day,
                    "used_by": (
                        f"{day}:target"
                        if source_day == day
                        else f"{day}:warmup_d_minus_1"
                    ),
                }
            )
        days.append({"day": day, "payloads": payloads})
    payload = {
        "schema_version": BAR_PAYLOAD_SCHEMA_VERSION,
        "identity": "first_opener_replay_bar_payload_manifest_v3",
        "symbol": "BTCUSDC",
        "bars_root": str(root),
        "target_day_count": len(target_days),
        "days": days,
    }
    _atomic_json(Path(output_path), payload)
    return payload


def validate_formal_day_universe(
    f10_spec: Mapping[str, Any],
    base: Mapping[str, Any],
    *,
    verify_hashes: bool = False,
) -> dict[str, Any]:
    """Validate every target and D-1 context day before spawning replay."""

    days = _development_days(f10_spec)
    if not days:
        raise ValueError("native producer Development universe is empty")
    dataset_root = Path(
        str(base["source_identity"]["normalized_l2_root"])
    ).expanduser().resolve()
    contexts: list[dict[str, Any]] = []
    for day in days:
        context_days = (_previous_natural_utc_day(day), day)
        try:
            l2_registry.require_formal_days(
                dataset_root,
                context_days,
                verify_hashes=bool(verify_hashes),
            )
        except l2_registry.FormalEligibilityError as exc:
            raise ValueError(
                f"frozen Development day is not native-formal with D-1: "
                f"{day}: {exc}"
            ) from exc
        contexts.append(
            {
                "target_day": day,
                "warmup_day": context_days[0],
            }
        )
    return {
        "dataset_root": str(dataset_root),
        "target_day_count": len(days),
        "contexts": contexts,
    }


def _validate_baseline_artifacts(base: Mapping[str, Any]) -> None:
    identities = (
        ("operational config", base["operational_config_identity"]),
        ("normalized L2 manifest", base["source_identity"]["normalized_l2_manifest"]),
        ("normalized L2 quality", base["source_identity"]["normalized_l2_quality"]),
        ("queue calibration", base["source_identity"]["queue_calibration"]),
        ("P3 artifact", base["source_identity"]["p3_artifact"]),
        ("execution trades manifest", base["execution_trade_identity"]["manifest"]),
        ("execution trades quality", base["execution_trade_identity"]["quality_report"]),
        ("latency samples", base["latency_identity"]["samples"]),
        ("BUY q90 model", base["buy_q90_identity"]["model"]),
        ("BUY q90 policy", base["buy_q90_identity"]["policy"]),
        ("source split", base["panels"]["source_split"]),
    )
    for label, identity in identities:
        _require_identity(
            Path(str(identity.get("path", ""))),
            str(identity.get("sha256", "")),
            label,
        )


def validate_producer_spec(payload: Mapping[str, Any]) -> None:
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected native producer schema")
    if payload.get("identity") != IDENTITY:
        raise ValueError("unexpected native producer identity")
    frozen_hash = str(payload.get("canonical_spec_sha256", ""))
    if len(frozen_hash) != 64 or canonical_spec_sha256(payload) != frozen_hash:
        raise ValueError("native producer canonical hash mismatch")
    permissions = payload.get("permissions") or {}
    required_permissions = {
        "development_outcome_read",
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
    }
    if set(permissions) != required_permissions or any(
        bool(value) for value in permissions.values()
    ):
        raise ValueError("native producer cannot read later panels or grant authority")
    replay = payload.get("replay_contract") or {}
    required_replay = {
        "engine": "python_authoritative",
        "initial_state": "daily_fresh_start",
        "fill_cooldown_clock": "wall_time_85n",
        "buy_q90": "enabled_cpp_kernel_lockstep",
        "trace_schema_version": contract.TRACE_SCHEMA_VERSION,
        "decision_feature_clock": "modeled_receive_ready_source_asof",
    }
    for key, expected in required_replay.items():
        if replay.get(key) != expected:
            raise ValueError(f"native producer replay contract drifted: {key}")
    if int(replay.get("trace_rows_max_per_day", 0) or 0) <= 0:
        raise ValueError("native producer trace bound is invalid")
    support = payload.get("support_gate") or {}
    minimum_coverage = float(
        support.get("minimum_true_opener_campaign_coverage", 0.0) or 0.0
    )
    if not 0.0 < minimum_coverage <= 1.0:
        raise ValueError("native producer true-opener support gate is invalid")

    f10 = payload.get("f10_spec_identity") or {}
    f10_path = Path(str(f10.get("path", ""))).expanduser()
    _require_identity(f10_path, str(f10.get("sha256", "")), "F10 spec")
    contract.validate_spec(_load_json(f10_path))
    base = payload.get("baseline_contract_identity") or {}
    base_path = Path(str(base.get("path", ""))).expanduser()
    _require_identity(base_path, str(base.get("sha256", "")), "baseline spec")
    base_payload = _load_json(base_path)
    if base_payload.get("schema_version") != full_path.SCHEMA_VERSION:
        raise ValueError("native producer baseline schema drifted")
    _validate_baseline_artifacts(base_payload)
    market = payload.get("market_source_manifest_identity") or {}
    _require_identity(
        Path(str(market.get("path", ""))),
        str(market.get("sha256", "")),
        "market source manifest",
    )
    bar_manifest = payload.get("bar_payload_manifest_identity") or {}
    bar_manifest_path = Path(str(bar_manifest.get("path", ""))).expanduser()
    _require_identity(
        bar_manifest_path,
        str(bar_manifest.get("sha256", "")),
        "bar payload manifest",
    )
    bar_payload = _load_json(bar_manifest_path)
    if bar_payload.get("schema_version") != BAR_PAYLOAD_SCHEMA_VERSION:
        raise ValueError("bar payload manifest schema drifted")
    f10_payload = _load_json(f10_path)
    expected_days = _development_days(f10_payload)
    bar_days = tuple(
        sorted(str(row.get("day", "")) for row in bar_payload.get("days", ()))
    )
    if (
        int(bar_payload.get("target_day_count", -1)) != len(expected_days)
        or bar_days != expected_days
    ):
        raise ValueError("bar payload manifest Development denominator drifted")
    native = payload.get("native_module_identity") or {}
    _require_identity(
        Path(str(native.get("path", ""))),
        str(native.get("sha256", "")),
        "BUY q90 native module",
    )
    implementation = payload.get("implementation_identity") or {}
    if set(implementation) != set(REQUIRED_IMPLEMENTATION_PATHS):
        raise ValueError("native producer implementation identity is incomplete")
    for relative, expected in implementation.items():
        _require_identity(ROOT / str(relative), str(expected), str(relative))


def load_producer_spec(
    path: Path = DEFAULT_PRODUCER_SPEC_PATH,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], pd.DataFrame]:
    producer_path = resolve_research_path(path)
    producer = _load_json(producer_path)
    validate_producer_spec(producer)
    f10_path = Path(producer["f10_spec_identity"]["path"]).expanduser().resolve()
    f10_spec = _load_json(f10_path)
    quality = contract.validate_quality_identity(f10_spec)
    base_path = Path(
        producer["baseline_contract_identity"]["path"]
    ).expanduser().resolve()
    base = _load_json(base_path)
    return producer, f10_spec, base, quality


def producer_identity(
    path: Path = DEFAULT_PRODUCER_SPEC_PATH,
) -> dict[str, Any]:
    resolved = resolve_research_path(path)
    producer = _load_json(resolved)
    validate_producer_spec(producer)
    spec_identity = {
        "path": str(resolved),
        "sha256": sha256_file(resolved),
    }
    return {
        "kind": "frozen_native_replay_producer",
        "identity": IDENTITY,
        "native_producer_spec": spec_identity,
        "canonical_spec_sha256": str(producer["canonical_spec_sha256"]),
        "runner_path": str(Path(__file__).resolve()),
        "runner_sha256": sha256_file(Path(__file__).resolve()),
        "baseline_contract_identity": dict(
            producer["baseline_contract_identity"]
        ),
        "market_source_manifest_identity": dict(
            producer["market_source_manifest_identity"]
        ),
        "bar_payload_manifest_identity": dict(
            producer["bar_payload_manifest_identity"]
        ),
        "native_module_identity": dict(producer["native_module_identity"]),
    }


def _grade_by_day(f10_spec: Mapping[str, Any]) -> dict[str, str]:
    panels = f10_spec["panels"]
    return {
        **{
            str(day): "A"
            for day in panels["development_primary_grade_a_days"]
        },
        **{
            str(day): "B"
            for day in panels["development_sensitivity_grade_b_days"]
        },
    }


def _validate_trace_audit(audit: Mapping[str, Any], day: str) -> None:
    required_zero = (
        "feature_clock_violation_count",
        "open_record_count",
    )
    if not bool(audit.get("coverage_complete", False)):
        raise RuntimeError(f"first-opener trace coverage is incomplete on {day}")
    campaign_count = int(audit.get("campaign_count", -1) or 0)
    eligible_count = int(
        audit.get("eligible_true_opener_campaign_count", -1) or 0
    )
    unsupported_count = int(
        audit.get("unsupported_nonopener_open_campaign_count", -1) or 0
    )
    counts = [
        int(audit.get(key, -1) or 0)
        for key in (
            "eligible_true_opener_campaign_count",
            "selected_campaign_count",
            "emitted_row_count",
            "unique_campaign_count",
            "exact_join_count",
        )
    ]
    declared_coverage = float(audit.get("true_opener_campaign_coverage", -1.0))
    expected_coverage = float(eligible_count) / max(campaign_count, 1)
    if (
        campaign_count <= 0
        or unsupported_count < 0
        or eligible_count + unsupported_count != campaign_count
        or min(counts) <= 0
        or len(set(counts)) != 1
        or abs(declared_coverage - expected_coverage) > 1e-12
    ):
        raise RuntimeError(f"first-opener denominator drifted on {day}: {counts}")
    if any(int(audit.get(key, -1) or 0) != 0 for key in required_zero):
        raise RuntimeError(f"first-opener causal audit failed on {day}: {audit}")


def _configure_params(
    producer: Mapping[str, Any],
    base: Mapping[str, Any],
    day: str,
    quality_grade: str,
) -> dict[str, Any]:
    params = full_path._configure_params(base, day)
    replay = producer["replay_contract"]
    params.update(
        {
            "fill_cooldown_clock_mode": "wall_time",
            "variance_time_lineage_randomized_enabled": False,
            "trace_variance_time_lineage_max": 0,
            "trace_decisions_max": 0,
            "trace_quotes_max": 0,
            "trace_fills_max": 0,
            "trace_first_add_decision_to_terminal_max": 0,
            "trace_first_opener_decision_to_terminal_max": 0,
            "first_add_trace_quality_grade": "",
            "first_opener_trace_quality_grade": "",
            "first_opener_trace_schema_version": contract.TRACE_SCHEMA_VERSION,
            "collect_curves": False,
            "decision_trace_profile": "full",
            "window_cache_write_enabled": False,
            "require_formal_l2": True,
            "verify_formal_l2_hashes": True,
            "replay_purpose": "formal",
            "replay_initial_state_mode": "fresh_start",
            "replay_promotion_eligible": False,
            "sync_adjust_replay_mode": "disabled",
            "dynamic_fill_hazard_cpp_parity_enabled": True,
            "dynamic_fill_hazard_cpp_parity_strict": True,
            "dynamic_fill_hazard_cpp_parity_trace_max": int(
                replay["q90_mismatch_trace_max"]
            ),
        }
    )
    params["trace_first_opener_decision_to_terminal_max"] = int(
        replay["trace_rows_max_per_day"]
    )
    params["first_opener_trace_quality_grade"] = str(quality_grade)
    return params


def run_day(
    *,
    day: str,
    quality_grade: str,
    spec: Mapping[str, Any],
    producer_spec_path: Path = DEFAULT_PRODUCER_SPEC_PATH,
) -> dict[str, Any]:
    """Artifact-builder callback for one frozen Development day."""

    producer, frozen_f10, base, _ = load_producer_spec(producer_spec_path)
    contract.validate_spec(spec)
    if contract.canonical_spec_sha256(spec) != contract.canonical_spec_sha256(
        frozen_f10
    ):
        raise ValueError("artifact builder supplied a different F10 spec")
    grade_by_day = _grade_by_day(frozen_f10)
    if grade_by_day.get(str(day)) != str(quality_grade):
        raise ValueError(f"day or quality grade is outside frozen Development: {day}")

    source_payload_audit = _verify_day_payloads(producer, base, str(day))
    params = _configure_params(producer, base, str(day), str(quality_grade))
    window = full_path._load_window(base, str(day), params)
    tape = CryptoHFTExchangeBookTape(
        raw_root=Path(base["source_identity"]["native_orderbook_root"]),
        day=str(day),
        symbol="BTCUSDC",
        tick_size=float(params.get("tick_size", bt.TICK)),
        warmup_hours=int(base["replay_contract"]["native_warmup_hours"]),
        strict_complete=True,
    )
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
    audit = dict(result.get("_first_opener_decision_to_terminal_trace_audit") or {})
    _validate_trace_audit(audit, str(day))
    if int(result.get("exchange_book_source_gap_events", 0) or 0) != 0:
        raise RuntimeError(f"native source gap encountered on {day}")
    if int(result.get("exchange_book_invalid_sequence_messages", 0) or 0) != 0:
        raise RuntimeError(f"native sequence failure encountered on {day}")
    native_events = int(result.get("exchange_book_events_consumed", 0) or 0)
    if native_events <= 0:
        raise RuntimeError(f"native exchange-book tape was not consumed on {day}")
    if not bool(result.get("dynamic_fill_hazard_cpp_parity_passed", False)):
        raise RuntimeError(f"BUY q90 Python/C++ lockstep failed on {day}")
    if int(result.get("dynamic_fill_hazard_cpp_mismatch_count", 0) or 0) != 0:
        raise RuntimeError(f"BUY q90 mismatch was nonzero on {day}")
    expected_module = str(producer["native_module_identity"]["sha256"])
    actual_module = str(
        (result.get("dynamic_fill_hazard_cpp_identity") or {}).get(
            "native_module_sha256", ""
        )
    )
    if actual_module != expected_module:
        raise RuntimeError(
            f"BUY q90 native module drifted: expected {expected_module}, "
            f"found {actual_module}"
        )

    trace = pd.DataFrame(result.get("_first_opener_decision_to_terminal_trace") or ())
    contract.validate_native_trace(trace, frozen_f10)
    return {
        "_first_opener_decision_to_terminal_trace": trace,
        "_first_opener_decision_to_terminal_trace_audit": audit,
        "_producer_runtime_audit": {
            "day": str(day),
            "quality_grade": str(quality_grade),
            "fills_total": int(result.get("fills_bid", 0) or 0)
            + int(result.get("fills_ask", 0) or 0),
            "campaign_count": int(result.get("campaign_count", 0) or 0),
            "trace_rows": int(len(trace)),
            "native_events": native_events,
            "q90_evaluations": int(
                result.get("dynamic_fill_hazard_cpp_evaluation_count", 0) or 0
            ),
            "source_payload_audit": source_payload_audit,
        },
    }


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def _run_day_for_pool(
    producer_spec_path: str,
    day: str,
    quality_grade: str,
    f10_spec: Mapping[str, Any],
) -> tuple[str, dict[str, Any], float]:
    started = time.monotonic()
    try:
        result = run_day(
            day=day,
            quality_grade=quality_grade,
            spec=f10_spec,
            producer_spec_path=Path(producer_spec_path),
        )
    except SystemExit as exc:
        raise RuntimeError(f"native producer failed on {day}: {exc}") from exc
    return day, result, float(time.monotonic() - started)


def _run_day_task(
    task: tuple[str, str, str, Mapping[str, Any]],
) -> tuple[str, dict[str, Any], float]:
    return _run_day_for_pool(*task)


def _checkpoint_day(
    output: Path,
    day: str,
    result: Mapping[str, Any],
    runtime_s: float,
    producer_spec_sha256: str,
    run_mode: str,
) -> dict[str, Any]:
    day_dir = output / "days"
    day_dir.mkdir(parents=True, exist_ok=True)
    trace_path = day_dir / f"{day}.parquet"
    audit_path = day_dir / f"{day}.json"
    trace = result["_first_opener_decision_to_terminal_trace"]
    if not isinstance(trace, pd.DataFrame):
        raise TypeError("native producer trace checkpoint must be a DataFrame")
    _atomic_parquet(trace_path, trace)
    payload = {
        "day": day,
        "runtime_s": float(runtime_s),
        "trace_rows": int(len(trace)),
        "trace_path": str(trace_path),
        "trace_sha256": sha256_file(trace_path),
        "producer_spec_sha256": producer_spec_sha256,
        "run_mode": str(run_mode),
        "trace_audit": dict(
            result["_first_opener_decision_to_terminal_trace_audit"]
        ),
        "runtime_audit": dict(result["_producer_runtime_audit"]),
    }
    _atomic_json(audit_path, payload)
    return payload


def _load_checkpoint(
    output: Path,
    day: str,
    producer_spec_sha256: str,
    run_mode: str,
) -> tuple[pd.DataFrame, dict[str, Any]] | None:
    trace_path = output / "days" / f"{day}.parquet"
    audit_path = output / "days" / f"{day}.json"
    if not trace_path.is_file() or not audit_path.is_file():
        return None
    audit = _load_json(audit_path)
    if audit.get("producer_spec_sha256") != producer_spec_sha256:
        raise ValueError(f"checkpoint producer identity drifted on {day}")
    if audit.get("run_mode") != str(run_mode):
        raise ValueError(f"checkpoint run mode drifted on {day}")
    _require_identity(trace_path, str(audit.get("trace_sha256", "")), day)
    _validate_trace_audit(audit.get("trace_audit") or {}, day)
    return pd.read_parquet(trace_path), audit


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--producer-spec", type=Path, default=DEFAULT_PRODUCER_SPEC_PATH)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--dates", nargs="*")
    parser.add_argument("--allow-partial-diagnostic", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    producer_path = args.producer_spec.expanduser().resolve()
    producer, f10_spec, base, _ = load_producer_spec(producer_path)
    producer_hash = sha256_file(producer_path)
    grade_by_day = _grade_by_day(f10_spec)
    expected_days = tuple(sorted(grade_by_day))
    requested = tuple(args.dates or expected_days)
    unknown = sorted(set(requested) - set(expected_days))
    if unknown:
        raise ValueError(f"requested dates are outside frozen Development: {unknown}")
    if set(requested) != set(expected_days) and not args.allow_partial_diagnostic:
        raise ValueError("formal native production requires all frozen Development days")

    formal_day_audit = validate_formal_day_universe(
        f10_spec,
        base,
        verify_hashes=False,
    )

    output_root = args.output_dir.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    complete = set(requested) == set(expected_days)
    run_mode = (
        "formal_development_native_production"
        if complete
        else "partial_diagnostic_only"
    )
    output = output_root / (
        "formal_development" if complete else "diagnostic_partial"
    )
    output.mkdir(parents=True, exist_ok=True)
    free_gib = shutil.disk_usage(output_root).free / (1024**3)
    minimum_free = float(producer["storage_gate"]["minimum_free_gib"])
    if free_gib < minimum_free:
        raise RuntimeError(
            f"native producer storage gate failed: {free_gib:.2f} < {minimum_free:.2f} GiB"
        )

    frames: dict[str, pd.DataFrame] = {}
    day_audits: dict[str, dict[str, Any]] = {}
    pending: list[str] = []
    for day in requested:
        checkpoint = _load_checkpoint(output, day, producer_hash, run_mode)
        if checkpoint is None:
            pending.append(day)
        else:
            frames[day], day_audits[day] = checkpoint

    workers = max(1, min(int(args.workers), len(pending) or 1))
    if workers == 1:
        iterator = (
            _run_day_for_pool(str(producer_path), day, grade_by_day[day], f10_spec)
            for day in pending
        )
        for day, result, runtime_s in iterator:
            day_audits[day] = _checkpoint_day(
                output, day, result, runtime_s, producer_hash, run_mode
            )
            frames[day] = result["_first_opener_decision_to_terminal_trace"]
            print(f"{day}: rows={len(frames[day])} runtime={runtime_s:.1f}s", flush=True)
    else:
        tasks = [
            (
                str(producer_path),
                day,
                grade_by_day[day],
                f10_spec,
            )
            for day in pending
        ]
        with get_context("spawn").Pool(processes=workers) as pool:
            for day, result, runtime_s in pool.imap_unordered(
                _run_day_task,
                tasks,
                chunksize=1,
            ):
                day_audits[day] = _checkpoint_day(
                    output, day, result, runtime_s, producer_hash, run_mode
                )
                frames[day] = result["_first_opener_decision_to_terminal_trace"]
                print(
                    f"{day}: rows={len(frames[day])} runtime={runtime_s:.1f}s",
                    flush=True,
                )

    combined = pd.concat([frames[day] for day in requested], ignore_index=True)
    contract.validate_native_trace(combined, f10_spec)
    panel_by_day = {
        **{
            str(day): "grade_a_primary"
            for day in f10_spec["panels"]["development_primary_grade_a_days"]
        },
        **{
            str(day): "grade_b_sensitivity"
            for day in f10_spec["panels"]["development_sensitivity_grade_b_days"]
        },
    }
    feature_support: list[dict[str, Any]] = []
    minimum_unique = int(
        f10_spec["decision_visible_features"][
            "minimum_unique_values_per_panel_side"
        ]
    )
    maximum_dominant_fraction = float(
        f10_spec["decision_visible_features"][
            "maximum_dominant_value_fraction_per_panel_side"
        ]
    )
    minimum_nonconstant_day_fraction = float(
        f10_spec["decision_visible_features"][
            "minimum_nonconstant_day_fraction_per_panel_side"
        ]
    )
    combined_support = combined.assign(
        _panel=combined["day"].astype(str).map(panel_by_day),
        _side=combined["side"].astype(str).str.upper(),
    )
    for panel in ("grade_a_primary", "grade_b_sensitivity"):
        for side in ("SELL", "BUY"):
            cell = combined_support.loc[
                combined_support["_panel"].eq(panel)
                & combined_support["_side"].eq(side)
            ]
            for feature in contract.MODEL_FEATURES:
                unique_values = int(cell[feature].nunique(dropna=False))
                dominant_fraction = (
                    float(cell[feature].value_counts(dropna=False).max())
                    / float(len(cell))
                    if len(cell)
                    else 1.0
                )
                per_day_unique = cell.groupby("day", sort=False)[
                    feature
                ].nunique(dropna=False)
                nonconstant_day_fraction = (
                    float(per_day_unique.gt(1).mean())
                    if len(per_day_unique)
                    else 0.0
                )
                passed = bool(
                    unique_values >= minimum_unique
                    and dominant_fraction <= maximum_dominant_fraction
                    and nonconstant_day_fraction
                    >= minimum_nonconstant_day_fraction
                )
                feature_support.append(
                    {
                        "panel": panel,
                        "side": side,
                        "feature": feature,
                        "rows": int(len(cell)),
                        "unique_values": unique_values,
                        "minimum_unique_values": minimum_unique,
                        "dominant_value_fraction": dominant_fraction,
                        "maximum_dominant_value_fraction": (
                            maximum_dominant_fraction
                        ),
                        "nonconstant_day_fraction": (
                            nonconstant_day_fraction
                        ),
                        "minimum_nonconstant_day_fraction": (
                            minimum_nonconstant_day_fraction
                        ),
                        "passed": passed,
                    }
                )
    feature_support_passed = bool(
        feature_support and all(row["passed"] for row in feature_support)
    )
    if set(requested) == set(expected_days) and not feature_support_passed:
        failed = [
            row
            for row in feature_support
            if not bool(row["passed"])
        ]
        raise RuntimeError(
            "formal native producer feature-lineage support failed: "
            f"{failed}"
        )
    trace_path = output / "first_opener_decision_to_terminal_trace.parquet"
    _atomic_parquet(trace_path, combined)
    audit_frame = pd.DataFrame(
        [
            {
                "day": day,
                "quality_grade": grade_by_day[day],
                **dict(day_audits[day]["trace_audit"]),
            }
            for day in requested
        ]
    )
    audit_path = output / "native_producer_audit.parquet"
    _atomic_parquet(audit_path, audit_frame)
    campaign_count = int(audit_frame["campaign_count"].sum())
    eligible_true_opener_count = int(
        audit_frame["eligible_true_opener_campaign_count"].sum()
    )
    unsupported_nonopener_count = int(
        audit_frame["unsupported_nonopener_open_campaign_count"].sum()
    )
    true_opener_coverage = float(eligible_true_opener_count) / max(
        campaign_count, 1
    )
    minimum_true_opener_coverage = float(
        producer["support_gate"]["minimum_true_opener_campaign_coverage"]
    )
    support_passed = bool(
        eligible_true_opener_count + unsupported_nonopener_count == campaign_count
        and true_opener_coverage >= minimum_true_opener_coverage
    )
    if complete and not support_passed:
        raise RuntimeError(
            "formal native producer true-opener support gate failed: "
            f"{true_opener_coverage:.6f} < {minimum_true_opener_coverage:.6f}"
        )
    manifest = {
        "schema_version": RUN_SCHEMA_VERSION,
        "identity": IDENTITY,
        "mode": run_mode,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "producer_identity": producer_identity(producer_path),
        "formal_day_audit": formal_day_audit,
        "requested_days": list(requested),
        "expected_days": list(expected_days),
        "complete_development": complete,
        "trace_rows": int(len(combined)),
        "trace_path": str(trace_path),
        "trace_sha256": sha256_file(trace_path),
        "producer_audit_path": str(audit_path),
        "producer_audit_sha256": sha256_file(audit_path),
        "true_opener_support": {
            "campaign_count": campaign_count,
            "eligible_true_opener_campaign_count": eligible_true_opener_count,
            "unsupported_nonopener_open_campaign_count": (
                unsupported_nonopener_count
            ),
            "coverage": true_opener_coverage,
            "minimum_coverage": minimum_true_opener_coverage,
            "passed": support_passed,
            "judgmental_engineering_threshold": True,
        },
        "decision_feature_support": {
            "model_features": list(contract.MODEL_FEATURES),
            "queue_ahead_is_diagnostic_only": True,
            "passed": feature_support_passed,
            "cells": feature_support,
        },
        "day_audits": [day_audits[day] for day in requested],
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
    }
    _atomic_json(output / "producer_manifest.json", manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
