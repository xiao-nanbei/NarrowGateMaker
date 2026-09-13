#!/usr/bin/env python3
"""Produce the frozen F10 first-add native Development trace.

This is a baseline replay producer, not an evaluator. It runs the current
corrected wall-time baseline over the preregistered 24 Grade-A and 16 Grade-B
Development days, checkpoints each day atomically, and refuses later panels.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from models import backtest_tick as bt
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from research.families.f09_campaign_action_uplift.audit import (
    volatility_time_add_rearm_full_path_preflight as full_path,
)
from research.families.f10_live_replay_attribution.audit import (
    first_add_decision_to_terminal_contract as contract,
)
from research.governance.paths import resolve_research_path

SCHEMA_VERSION = "first_add_decision_to_terminal_native_producer.v1"
IDENTITY = "first_add_decision_to_terminal_native_producer_v1"
ROOT = Path(__file__).resolve().parents[4]
DEFAULT_PRODUCER_SPEC_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "first_add_decision_to_terminal_native_producer_v1_spec_20260729.json"
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
    if any(bool(value) for value in permissions.values()):
        raise ValueError("native producer cannot read later panels or grant authority")
    replay = payload.get("replay_contract") or {}
    required_replay = {
        "engine": "python_authoritative",
        "initial_state": "daily_fresh_start",
        "fill_cooldown_clock": "wall_time_85n",
        "buy_q90": "enabled_cpp_kernel_lockstep",
        "trace_schema_version": contract.TRACE_SCHEMA_VERSION,
    }
    for key, expected in required_replay.items():
        if replay.get(key) != expected:
            raise ValueError(f"native producer replay contract drifted: {key}")
    if int(replay.get("trace_rows_max_per_day", 0) or 0) <= 0:
        raise ValueError("native producer trace bound is invalid")

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
    native = payload.get("native_module_identity") or {}
    _require_identity(
        Path(str(native.get("path", ""))),
        str(native.get("sha256", "")),
        "BUY q90 native module",
    )
    for relative, expected in (payload.get("implementation_identity") or {}).items():
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
        raise RuntimeError(f"first-add trace coverage is incomplete on {day}")
    counts = [
        int(audit.get(key, -1) or 0)
        for key in (
            "selected_campaign_count",
            "emitted_row_count",
            "unique_campaign_count",
            "exact_join_count",
        )
    ]
    if min(counts) <= 0 or len(set(counts)) != 1:
        raise RuntimeError(f"first-add denominator drifted on {day}: {counts}")
    if any(int(audit.get(key, -1) or 0) != 0 for key in required_zero):
        raise RuntimeError(f"first-add causal audit failed on {day}: {audit}")


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
            "trace_first_add_decision_to_terminal_max": int(
                replay["trace_rows_max_per_day"]
            ),
            "first_add_trace_quality_grade": str(quality_grade),
            "collect_curves": False,
            "decision_trace_profile": "full",
            "window_cache_write_enabled": False,
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
    audit = dict(result.get("_first_add_decision_to_terminal_trace_audit") or {})
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

    trace = pd.DataFrame(result.get("_first_add_decision_to_terminal_trace") or ())
    contract.validate_native_trace(trace, frozen_f10)
    return {
        "_first_add_decision_to_terminal_trace": trace,
        "_first_add_decision_to_terminal_trace_audit": audit,
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
    result = run_day(
        day=day,
        quality_grade=quality_grade,
        spec=f10_spec,
        producer_spec_path=Path(producer_spec_path),
    )
    return day, result, float(time.monotonic() - started)


def _checkpoint_day(
    output: Path,
    day: str,
    result: Mapping[str, Any],
    runtime_s: float,
    producer_spec_sha256: str,
) -> dict[str, Any]:
    day_dir = output / "days"
    day_dir.mkdir(parents=True, exist_ok=True)
    trace_path = day_dir / f"{day}.parquet"
    audit_path = day_dir / f"{day}.json"
    trace = result["_first_add_decision_to_terminal_trace"]
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
        "trace_audit": dict(
            result["_first_add_decision_to_terminal_trace_audit"]
        ),
        "runtime_audit": dict(result["_producer_runtime_audit"]),
    }
    _atomic_json(audit_path, payload)
    return payload


def _load_checkpoint(
    output: Path,
    day: str,
    producer_spec_sha256: str,
) -> tuple[pd.DataFrame, dict[str, Any]] | None:
    trace_path = output / "days" / f"{day}.parquet"
    audit_path = output / "days" / f"{day}.json"
    if not trace_path.is_file() or not audit_path.is_file():
        return None
    audit = _load_json(audit_path)
    if audit.get("producer_spec_sha256") != producer_spec_sha256:
        raise ValueError(f"checkpoint producer identity drifted on {day}")
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
    producer, f10_spec, _, _ = load_producer_spec(producer_path)
    producer_hash = sha256_file(producer_path)
    grade_by_day = _grade_by_day(f10_spec)
    expected_days = tuple(sorted(grade_by_day))
    requested = tuple(args.dates or expected_days)
    unknown = sorted(set(requested) - set(expected_days))
    if unknown:
        raise ValueError(f"requested dates are outside frozen Development: {unknown}")
    if set(requested) != set(expected_days) and not args.allow_partial_diagnostic:
        raise ValueError("formal native production requires all 40 Development days")

    output = args.output_dir.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    free_gib = shutil.disk_usage(output).free / (1024**3)
    minimum_free = float(producer["storage_gate"]["minimum_free_gib"])
    if free_gib < minimum_free:
        raise RuntimeError(
            f"native producer storage gate failed: {free_gib:.2f} < {minimum_free:.2f} GiB"
        )

    frames: dict[str, pd.DataFrame] = {}
    day_audits: dict[str, dict[str, Any]] = {}
    pending: list[str] = []
    for day in requested:
        checkpoint = _load_checkpoint(output, day, producer_hash)
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
                output, day, result, runtime_s, producer_hash
            )
            frames[day] = result["_first_add_decision_to_terminal_trace"]
            print(f"{day}: rows={len(frames[day])} runtime={runtime_s:.1f}s", flush=True)
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    _run_day_for_pool,
                    str(producer_path),
                    day,
                    grade_by_day[day],
                    f10_spec,
                ): day
                for day in pending
            }
            for future in as_completed(futures):
                day, result, runtime_s = future.result()
                day_audits[day] = _checkpoint_day(
                    output, day, result, runtime_s, producer_hash
                )
                frames[day] = result["_first_add_decision_to_terminal_trace"]
                print(
                    f"{day}: rows={len(frames[day])} runtime={runtime_s:.1f}s",
                    flush=True,
                )

    combined = pd.concat([frames[day] for day in requested], ignore_index=True)
    contract.validate_native_trace(combined, f10_spec)
    trace_path = output / "first_add_decision_to_terminal_trace.parquet"
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
    complete = set(requested) == set(expected_days)
    manifest = {
        "schema_version": "first_add_decision_to_terminal_native_run.v1",
        "identity": IDENTITY,
        "mode": (
            "formal_development_native_production"
            if complete
            else "partial_diagnostic_only"
        ),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "producer_identity": producer_identity(producer_path),
        "requested_days": list(requested),
        "expected_days": list(expected_days),
        "complete_development": complete,
        "trace_rows": int(len(combined)),
        "trace_path": str(trace_path),
        "trace_sha256": sha256_file(trace_path),
        "producer_audit_path": str(audit_path),
        "producer_audit_sha256": sha256_file(audit_path),
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
