#!/usr/bin/env python3
"""Mechanics-only 40-day lockstep for the BUY q90 ABI v4 successor.

The runner intentionally reads a narrow allowlist of lifecycle, native-book,
and Python/C++ parity outputs. Economic replay outputs are neither selected nor
serialized. The operational configuration must keep q90 action disabled; the
runner enables the action only inside its isolated Development replay so the
cancel/ACK/prospective-reentry state machine can be exercised.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from data_paths import LEGACY_MARKETDATA_ROOT, relocate_marketdata_path
from models import backtest_tick as bt
from models.backtest_config import (
    load_tick_base_params,
    validate_formal_replay_calibration,
)
from models.data_windows import _load_cached_window, _window_cache_path
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from models.replay_contract import configure_fixed_latency_distribution

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "buy_q90_abi_v4_40day_lockstep_v1_6"
SCHEMA_VERSION = "buy_q90_abi_v4_40day_lockstep.v1.6"
DEFAULT_SPEC = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "buy_q90_abi_v4_40day_lockstep_v1_6_spec_20260803.json"
)

FORBIDDEN_ECONOMIC_KEY_FRAGMENTS = (
    "pnl",
    "markout",
    "reward",
    "campaign",
    "toxicity",
    "profit",
    "loss_usdc",
)

RESULT_ALLOWLIST = (
    "dynamic_fill_hazard_action_enabled",
    "dynamic_fill_hazard_action_application",
    "dynamic_fill_hazard_eval_count",
    "dynamic_fill_hazard_valid_eval_count",
    "dynamic_fill_hazard_invalid_eval_count",
    "dynamic_fill_hazard_keep_count",
    "dynamic_fill_hazard_cancel_request_count",
    "dynamic_fill_hazard_cancel_ack_count",
    "dynamic_fill_hazard_pre_ack_fill_count",
    "dynamic_fill_hazard_recovery_count",
    "dynamic_fill_hazard_reentry_count",
    "dynamic_fill_hazard_post_cancel_recovery_count",
    "dynamic_fill_hazard_prospective_eval_count",
    "dynamic_fill_hazard_prospective_valid_count",
    "dynamic_fill_hazard_prospective_invalid_count",
    "dynamic_fill_hazard_post_terminal_hazard_reuse_count",
    "dynamic_fill_hazard_post_terminal_cursor_reuse_count",
    "dynamic_fill_hazard_unsupported_terminal_route_count",
    "dynamic_fill_hazard_retain_invalid_count",
    "dynamic_fill_hazard_hold_active_end",
    "dynamic_fill_hazard_hold_phase_end",
    "dynamic_fill_hazard_terminal_cursor_retention_end",
    "dynamic_fill_hazard_invalid_reason_counts",
    "dynamic_fill_hazard_invalid_attribution_closed",
    "dynamic_fill_hazard_future_feature_time_count",
    "dynamic_fill_hazard_cpp_parity_enabled",
    "dynamic_fill_hazard_cpp_parity_passed",
    "dynamic_fill_hazard_cpp_identity",
    "dynamic_fill_hazard_cpp_book_event_count",
    "dynamic_fill_hazard_cpp_activation_count",
    "dynamic_fill_hazard_cpp_evaluation_count",
    "dynamic_fill_hazard_cpp_lifecycle_count",
    "dynamic_fill_hazard_cpp_visibility_ambiguity_sync_count",
    "dynamic_fill_hazard_cpp_mismatch_count",
    "dynamic_fill_hazard_cpp_mismatch_by_stage",
    "dynamic_fill_hazard_cpp_mismatch_trace",
    "dynamic_fill_hazard_cpp_counters",
    "dynamic_fill_hazard_cpp_sequence_stats",
    "dynamic_fill_hazard_model_family_id",
    "dynamic_fill_hazard_model_sha256",
    "dynamic_fill_hazard_policy_id",
    "dynamic_fill_hazard_policy_sha256",
    "exchange_book_events_consumed",
    "exchange_book_consumed_events",
    "exchange_book_source_gap_events",
    "exchange_book_invalid_sequence_messages",
    "exchange_book_message_time_reversals",
    "_dynamic_fill_hazard_lifecycle_journal",
    "_dynamic_fill_hazard_lifecycle_journal_audit",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    raw = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def canonical_spec_sha256(spec: Mapping[str, Any]) -> str:
    payload = dict(spec)
    payload.pop("canonical_spec_sha256", None)
    return canonical_sha256(payload)


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _relocate(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _relocate(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_relocate(item) for item in value]
    if isinstance(value, str) and value.startswith(str(LEGACY_MARKETDATA_ROOT)):
        return str(relocate_marketdata_path(value))
    return value


def require_identity(identity: Mapping[str, Any], label: str) -> Path:
    path = Path(str(identity["path"])).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"{label} missing: {path}")
    actual = sha256_file(path)
    if actual != str(identity["sha256"]):
        raise ValueError(
            f"{label} hash mismatch: expected={identity['sha256']} actual={actual}"
        )
    return path


def validate_spec(spec: Mapping[str, Any]) -> None:
    if spec.get("schema_version") != SCHEMA_VERSION or spec.get("identity") != IDENTITY:
        raise ValueError("unexpected q90 ABI v4 lockstep identity")
    if canonical_spec_sha256(spec) != str(spec.get("canonical_spec_sha256", "")):
        raise ValueError("q90 ABI v4 lockstep canonical spec hash mismatch")
    days = tuple(map(str, spec.get("development_days") or ()))
    if len(days) != 40 or len(set(days)) != 40 or days != tuple(sorted(days)):
        raise ValueError("q90 ABI v4 lockstep requires the frozen 40-day panel")
    permissions = spec.get("permissions") or {}
    forbidden_true = [
        key
        for key, value in permissions.items()
        if key != "development_mechanics_execution_allowed" and bool(value)
    ]
    if forbidden_true or not bool(
        permissions.get("development_mechanics_execution_allowed", False)
    ):
        raise ValueError(f"q90 ABI v4 lockstep permission drift: {forbidden_true}")
    for key in RESULT_ALLOWLIST:
        lowered = key.lower()
        if any(fragment in lowered for fragment in FORBIDDEN_ECONOMIC_KEY_FRAGMENTS):
            raise AssertionError(f"economic key escaped mechanics allowlist: {key}")


def _load_source(spec: Mapping[str, Any]) -> dict[str, Any]:
    source_path = require_identity(spec["frozen_40day_identity"], "frozen 40-day identity")
    frozen = _load_json(source_path)
    predecessor = frozen["source_contract_identity"]
    predecessor_path = require_identity(predecessor, "frozen source replay contract")
    source = _relocate(_load_json(predecessor_path))
    if list(map(str, source["panels"]["development_days"])) != list(
        map(str, spec["development_days"])
    ):
        raise ValueError("40-day denominator drifted from frozen source")
    return source


def _configure_params(
    spec: Mapping[str, Any],
    source: Mapping[str, Any],
    day: str,
) -> dict[str, Any]:
    config_path = require_identity(spec["operational_config_identity"], "operational config")
    # The operational pointer is a separate governance identity and may lag the
    # explicitly frozen mechanics input. Load an exact-byte temporary alias so
    # this successor validates the bound YAML rather than mutating that pointer.
    alias = Path(tempfile.gettempdir()) / (
        "narrowgate_q90_abi_v4_" + str(spec["operational_config_identity"]["sha256"])
        + ".yaml"
    )
    if not alias.is_file() or sha256_file(alias) != sha256_file(config_path):
        alias.write_bytes(config_path.read_bytes())
    params = load_tick_base_params(
        symbol="BTCUSDC",
        config_path=alias,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        queue_calibration_path=source["source_identity"]["queue_calibration"]["path"],
        strict_calibration=True,
    )
    if bool(params.get("dynamic_fill_hazard_action_enabled", False)):
        raise ValueError("operational q90 action must remain disabled")
    if not bool(params.get("dynamic_fill_hazard_shadow_enabled", False)):
        raise ValueError("operational q90 shadow must remain enabled")
    replay = source["replay_contract"]
    params.update(
        {
            "ml_enabled": False,
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "replay_event_clock": "merged",
            "replay_clock_interval_ms": int(replay["clock_interval_ms"]),
            "exchange_book_queue_mode": "strict",
            "queue_l2_cancel_ahead_enabled": False,
            "decision_trace_profile": "mechanics_only",
            "trace_decisions_max": 0,
            "trace_quotes_max": 0,
            "trace_fills_max": 0,
            "trace_first_add_decision_to_terminal_max": 0,
            "trace_first_opener_decision_to_terminal_max": 0,
            "collect_curves": False,
            "rng_seed": int(replay["rng_seed"]),
            "sync_adjust_replay_mode": "disabled",
            "replay_purpose": "f10_q90_abi_v4_mechanics_lockstep",
            "replay_initial_state_mode": "fresh_start",
            "replay_promotion_eligible": False,
            "initial_inventory": 0.0,
            "initial_entry_price": 0.0,
            "fill_cooldown_clock_mode": "wall_time",
            "window_cache_write_enabled": False,
            "legacy_monolithic_window_cache_write_enabled": False,
            "dynamic_fill_hazard_action_enabled": True,
            "dynamic_fill_hazard_action_application": "apply",
            "dynamic_fill_hazard_cpp_parity_enabled": True,
            "dynamic_fill_hazard_cpp_parity_strict": True,
            "dynamic_fill_hazard_cpp_parity_trace_max": 1000,
            "dynamic_fill_hazard_mechanics_telemetry_enabled": True,
        }
    )
    trade = source["execution_trade_identity"]
    params.update(
        {
            "individual_trades_manifest_path": trade["manifest"]["path"],
            "individual_trades_manifest_sha256": trade["manifest"]["sha256"],
            "individual_trades_integrity_report_path": trade["quality_report"]["path"],
            "individual_trades_integrity_report_sha256": trade["quality_report"]["sha256"],
        }
    )
    latency = bt._load_live_perf_latency_samples(
        Path(source["latency_identity"]["samples"]["path"]),
        mode=str(source["latency_identity"]["mode"]),
    )
    params["_new_order_latency_samples_ms"] = latency["new_order_latency_samples_ms"]
    params["_cancel_order_latency_samples_ms"] = latency["cancel_order_latency_samples_ms"]
    configure_fixed_latency_distribution(
        params,
        scenario="baseline",
        profile_id=str(source["latency_identity"]["profile_id"]),
        environment=str(source["latency_identity"]["environment"]),
        baseline_clip_quantile=float(
            source["latency_identity"]["baseline_clip_quantile"]
        ),
    )
    validate_formal_replay_calibration(params, require_latency=True)
    if str(params.get("fill_cooldown_consecutive_reset_policy")) != "opposite_fill_only":
        raise ValueError("q90 lockstep requires opposite_fill_only cooldown reset")
    params["_q90_lockstep_day"] = str(day)
    return params


def exact_cache_path(
    spec: Mapping[str, Any],
    source: Mapping[str, Any],
    day: str,
    params: dict[str, Any],
) -> Path:
    cache_dir = Path(str(spec["storage"]["cache_root"])).expanduser().resolve()
    feature_dir = Path(os.environ.get("MM_FEATURE_DIR", bt.FEATURES_DIR)).expanduser().resolve()
    exact = _window_cache_path(
        cache_dir,
        str(day),
        params,
        load_ml=False,
        require_ml=False,
        run_ml_inference=False,
        feature_dir=feature_dir,
        require_target_feature_files=False,
        cross_market_enabled=False,
        with_ml_cache=False,
        require_historical_bbo=True,
    )
    if exact.is_file():
        return exact
    # The authoritative data was moved to ORICO after these cache keys were
    # created, changing source path/mtime signatures without changing payload
    # bytes. Bind the newest compatible v13 day cache explicitly instead of
    # rebuilding hundreds of GiB on the internal disk.
    candidates = sorted(
        cache_dir.glob(f"btcusdc_{day}_tick_window_v13_*.pkl"),
        key=lambda path: (path.stat().st_mtime_ns, path.name),
        reverse=True,
    )
    if not candidates:
        return exact
    return candidates[0]


def build_input_cache_manifest(
    spec: Mapping[str, Any], source: Mapping[str, Any]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in map(str, spec["development_days"]):
        params = _configure_params(spec, source, day)
        cache = exact_cache_path(spec, source, day, params)
        if not cache.is_file():
            raise FileNotFoundError(f"required read-only window cache missing: {cache}")
        tape = CryptoHFTExchangeBookTape(
            raw_root=Path(source["source_identity"]["native_orderbook_root"]),
            day=day,
            symbol="BTCUSDC",
            tick_size=float(params.get("tick_size", bt.TICK)),
            warmup_hours=int(source["replay_contract"]["native_warmup_hours"]),
            strict_complete=True,
        )
        rows.append(
            {
                "day": day,
                "window_cache_path": str(cache),
                "window_cache_sha256": sha256_file(cache),
                "window_cache_bytes": int(cache.stat().st_size),
                "native_tape_paths": [str(path.resolve()) for path in tape.source_paths],
                "native_tape_sha256": [sha256_file(path) for path in tape.source_paths],
            }
        )
    return rows


def validate_identities(spec: Mapping[str, Any]) -> dict[str, Any]:
    validate_spec(spec)
    source = _load_source(spec)
    require_identity(spec["v4_implementation_identity"], "q90 v4 implementation identity")
    require_identity(spec["operational_config_identity"], "operational config")
    require_identity(spec["model_identity"], "q90 model")
    require_identity(spec["policy_identity"], "q90 policy")
    require_identity(spec["native_module_identity"], "q90 native module")
    for relative, expected in (spec.get("implementation_sha256") or {}).items():
        path = ROOT / str(relative)
        actual = sha256_file(path)
        if actual != str(expected):
            raise ValueError(
                f"implementation hash mismatch for {relative}: {actual} != {expected}"
            )
    return source


def _load_window(
    spec: Mapping[str, Any],
    source: Mapping[str, Any],
    day: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    cache = exact_cache_path(spec, source, day, params)
    before = cache.stat()
    cached = _load_cached_window(cache)
    if cached is None:
        raise RuntimeError(f"bound window cache is incompatible: {cache}")
    window = cached.to_dict()
    after = cache.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError(f"read-only window cache changed during load: {cache}")
    return window


def _mechanics_only(result: Mapping[str, Any]) -> dict[str, Any]:
    selected = {key: result.get(key) for key in RESULT_ALLOWLIST}
    journal = list(selected.pop("_dynamic_fill_hazard_lifecycle_journal") or ())
    audit = dict(selected.pop("_dynamic_fill_hazard_lifecycle_journal_audit") or {})
    lifecycle_events = Counter(str(row.get("lifecycle_event", "")) for row in journal)
    terminal_routes = Counter(
        str(row.get("terminal_policy_route", ""))
        for row in journal
        if str(row.get("phase_after", "")) == "EXCHANGE_TERMINAL"
    )
    terminal_reasons = Counter(
        str(row.get("terminal_reason", ""))
        for row in journal
        if str(row.get("phase_after", "")) == "EXCHANGE_TERMINAL"
    )
    transitions = Counter(
        f"{row.get('phase_before', '')}->{row.get('phase_after', '')}"
        for row in journal
    )
    return {
        **selected,
        "lifecycle_audit": audit,
        "lifecycle_journal_row_count": len(journal),
        "lifecycle_event_counts": dict(sorted(lifecycle_events.items())),
        "terminal_route_counts": dict(sorted(terminal_routes.items())),
        "terminal_reason_counts": dict(sorted(terminal_reasons.items())),
        "lifecycle_transition_counts": dict(sorted(transitions.items())),
    }


def run_day(spec_path: Path, day: str) -> dict[str, Any]:
    started = time.monotonic()
    spec = _load_json(spec_path)
    source = validate_identities(spec)
    params = _configure_params(spec, source, day)
    window = _load_window(spec, source, day, params)
    tape = CryptoHFTExchangeBookTape(
        raw_root=Path(source["source_identity"]["native_orderbook_root"]),
        day=day,
        symbol="BTCUSDC",
        tick_size=float(params.get("tick_size", bt.TICK)),
        warmup_hours=int(source["replay_contract"]["native_warmup_hours"]),
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
    mechanics = _mechanics_only(result)
    checks = {
        "cpp_mismatch_zero": int(mechanics["dynamic_fill_hazard_cpp_mismatch_count"] or 0) == 0,
        "cpp_parity_passed": bool(mechanics["dynamic_fill_hazard_cpp_parity_passed"]),
        "post_terminal_hazard_reuse_zero": int(
            mechanics["dynamic_fill_hazard_post_terminal_hazard_reuse_count"] or 0
        ) == 0,
        "post_terminal_cursor_reuse_zero": int(
            mechanics["dynamic_fill_hazard_post_terminal_cursor_reuse_count"] or 0
        ) == 0,
        "terminal_cursor_retention_zero": int(
            mechanics["dynamic_fill_hazard_terminal_cursor_retention_end"] or 0
        ) == 0,
        "unsupported_terminal_route_zero": int(
            mechanics["dynamic_fill_hazard_unsupported_terminal_route_count"] or 0
        ) == 0
        and int(mechanics["lifecycle_audit"].get("unsupported_terminal_route_count", 0)) == 0,
        "invalid_attribution_closed": bool(
            mechanics["dynamic_fill_hazard_invalid_attribution_closed"]
        ),
        "native_source_gap_zero": int(mechanics["exchange_book_source_gap_events"] or 0) == 0,
        "native_invalid_sequence_zero": int(
            mechanics["exchange_book_invalid_sequence_messages"] or 0
        ) == 0,
    }
    if not all(checks.values()):
        raise RuntimeError(f"q90 ABI v4 lockstep failed on {day}: {checks}")
    return {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "day": str(day),
        "runtime_s": float(time.monotonic() - started),
        "checks": checks,
        "mechanics": mechanics,
    }


def _run_task(task: tuple[str, str]) -> tuple[str, dict[str, Any]]:
    spec_path, day = task
    return day, run_day(Path(spec_path), day)


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def aggregate(spec: Mapping[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: str(row["day"]))
    if [row["day"] for row in ordered] != list(map(str, spec["development_days"])):
        raise ValueError("completed q90 lockstep denominator is not the frozen 40 days")
    sums: Counter[str] = Counter()
    invalid_reasons: Counter[str] = Counter()
    exposure_terminal = 0
    exposure_complete = 0
    exposure_invalid_rows = 0
    for row in ordered:
        mechanics = row["mechanics"]
        for key in (
            "dynamic_fill_hazard_eval_count",
            "dynamic_fill_hazard_cancel_request_count",
            "dynamic_fill_hazard_cancel_ack_count",
            "dynamic_fill_hazard_pre_ack_fill_count",
            "dynamic_fill_hazard_recovery_count",
            "dynamic_fill_hazard_reentry_count",
            "dynamic_fill_hazard_post_cancel_recovery_count",
            "dynamic_fill_hazard_prospective_eval_count",
            "dynamic_fill_hazard_prospective_valid_count",
            "dynamic_fill_hazard_prospective_invalid_count",
            "dynamic_fill_hazard_cpp_mismatch_count",
            "dynamic_fill_hazard_cpp_visibility_ambiguity_sync_count",
            "dynamic_fill_hazard_post_terminal_hazard_reuse_count",
            "dynamic_fill_hazard_post_terminal_cursor_reuse_count",
            "dynamic_fill_hazard_unsupported_terminal_route_count",
        ):
            sums[key] += int(mechanics.get(key, 0) or 0)
        audit = mechanics["lifecycle_audit"]
        exposure_terminal += int(audit.get("terminal_row_count", 0) or 0)
        exposure_complete += int(
            audit.get("terminal_exchange_exposure_complete_count", 0) or 0
        )
        exposure_invalid_rows += int(
            audit.get("exchange_exposure_invalid_row_count", 0) or 0
        )
        invalid_reasons.update(audit.get("exchange_exposure_invalid_reason_counts") or {})
    gates = {
        "all_40_days_completed": len(ordered) == 40,
        "python_cpp_mismatch_zero": sums["dynamic_fill_hazard_cpp_mismatch_count"] == 0,
        "post_terminal_hazard_reuse_zero": sums[
            "dynamic_fill_hazard_post_terminal_hazard_reuse_count"
        ] == 0,
        "post_terminal_cursor_reuse_zero": sums[
            "dynamic_fill_hazard_post_terminal_cursor_reuse_count"
        ] == 0,
        "unsupported_terminal_route_zero": sums[
            "dynamic_fill_hazard_unsupported_terminal_route_count"
        ] == 0,
        "all_daily_checks_passed": all(
            all(row["checks"].values()) for row in ordered
        ),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "classification": "Development mechanics parity only",
        "development_days": [row["day"] for row in ordered],
        "daily_runtime_s": {row["day"]: row["runtime_s"] for row in ordered},
        "mechanics_totals": dict(sums),
        "exchange_exposure": {
            "terminal_row_count": exposure_terminal,
            "terminal_complete_count": exposure_complete,
            "terminal_complete_coverage": (
                exposure_complete / exposure_terminal if exposure_terminal else None
            ),
            "invalid_row_count": exposure_invalid_rows,
            "invalid_reason_counts": dict(sorted(invalid_reasons.items())),
        },
        "gates": gates,
        "mechanics_supported": bool(all(gates.values())),
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "q90_action_operationally_enabled": False,
        "deployment_performed": False,
        "threshold_changed": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--days", nargs="*")
    args = parser.parse_args(argv)

    spec_path = args.spec.expanduser().resolve()
    spec = _load_json(spec_path)
    source = validate_identities(spec)
    manifest = build_input_cache_manifest(spec, source)
    actual_manifest = canonical_sha256(manifest)
    if actual_manifest != str(spec["input_cache_manifest_canonical_sha256"]):
        raise ValueError(
            "input/cache manifest drifted: "
            f"expected={spec['input_cache_manifest_canonical_sha256']} actual={actual_manifest}"
        )
    output_root = (
        args.output_root.expanduser().resolve()
        if args.output_root is not None
        else Path(str(spec["storage"]["output_root"])).expanduser().resolve()
    )
    _atomic_json(output_root / "input_cache_manifest.json", manifest)
    if args.preflight_only:
        print(json.dumps({"preflight_passed": True, "rows": len(manifest)}, indent=2))
        return 0

    days = list(map(str, args.days or spec["development_days"]))
    unknown = sorted(set(days) - set(map(str, spec["development_days"])))
    if unknown:
        raise ValueError(f"requested non-Development days: {unknown}")
    rows: list[dict[str, Any]] = []
    tasks = [(str(spec_path), day) for day in days]
    if int(args.workers) <= 1:
        for _, day in tasks:
            row = run_day(spec_path, day)
            rows.append(row)
            _atomic_json(output_root / "days" / f"{day}.json", row)
            print(f"{day}: passed in {row['runtime_s']:.1f}s", flush=True)
    else:
        with ProcessPoolExecutor(max_workers=int(args.workers)) as pool:
            future_map = {pool.submit(_run_task, task): task[1] for task in tasks}
            for future in as_completed(future_map):
                day, row = future.result()
                rows.append(row)
                _atomic_json(output_root / "days" / f"{day}.json", row)
                print(f"{day}: passed in {row['runtime_s']:.1f}s", flush=True)

    if len(days) == 40:
        report = aggregate(spec, rows)
        _atomic_json(output_root / "report.json", report)
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        _atomic_json(output_root / "partial_report.json", rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
