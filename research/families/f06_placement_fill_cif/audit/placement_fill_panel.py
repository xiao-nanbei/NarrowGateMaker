#!/usr/bin/env python3
"""Stream the frozen Development new-placement lifecycle panel by UTC day.

This builder owns only the pre-submit placement estimand.  It deliberately
does not create KEEP/REPLACE rows: those actions start from an already-active
order and belong to ``active_order_continuation_surface_v1``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import multiprocessing
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from data.build_active_order_queue_tape import build_active_order_queue_tape
from data_paths import data_root, marketdata_root, window_cache_root
from models import backtest_tick as bt
from models.audit.content_addressed_cache import (
    DirectoryContentAddressedCache,
    ParquetContentAddressedCache,
)
from models.audit.experiment_manifest import (
    git_workspace_identity,
    write_code_checkpoint,
)
from models.data_windows import WINDOW_CACHE_VERSION
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from research.families.f06_placement_fill_cif import FAMILY_DOCS, FAMILY_ROOT
from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle import (
    ACTION_ORDER,
    SCHEMA_VERSION,
)
from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle_smoke import (
    _file_identity,
    _individual_trade_identity,
    _sha256,
    build_cohorts,
)
from research.families.f06_placement_fill_cif.audit.placement_fill_spec import (
    load_placement_fill_spec,
)
from research.families.f06_placement_fill_cif.audit.sparse_order_lifecycle import (
    build_watch_manifest,
    simulate_sparse_paired_placements,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
DEFAULT_SPEC = FAMILY_DOCS / "placement_fill_cif_v1_spec_20260726.json"
DEFAULT_OUTPUT = DATA_ROOT / "reports" / "placement_fill_cif_v1_development_20260726"
DEFAULT_NORMALIZED = DATA_ROOT / "normalized_l2_100ms_v2_minimal141_20260727"
DEFAULT_FEATURE_CONTEXT = DATA_ROOT / "features_btcusdc_causal_v10_minimal141_context_20260728"
DEFAULT_RAW_BOOK = marketdata_root() / "cryptohftdata"
DEFAULT_QUEUE = (
    DATA_ROOT
    / "reports"
    / "formal_recalibration_20260715"
    / "BTCUSDC-queue-calibration-v3-fit-20260710_11-q070.json"
)
DEFAULT_VISIBILITY = (
    DATA_ROOT
    / "live_calibration_snapshots"
    / "20260719"
    / "day_20260718"
    / "quote_decisions_2026-07-18.csv"
)
DEFAULT_WINDOW_CACHE = window_cache_root(ROOT)
DEFAULT_PAIRED_MECHANICS_CACHE = DEFAULT_WINDOW_CACHE / "paired_placement_mechanics_v2"
DEFAULT_SPARSE_QUEUE_TAPE_CACHE = DEFAULT_WINDOW_CACHE / "active_order_queue_tape_v3"
BUILDER_SCHEMA_VERSION = "placement_fill_panel_builder.v3"
PAIRED_MECHANICS_SCHEMA_VERSION = "paired_placement_mechanics_cache.v2"
SPARSE_TAPE_CACHE_SCHEMA_VERSION = "active_order_queue_tape_cache.v2"

FROZEN_IMPLEMENTATION_PATHS = {
    "placement_fill_panel.py": Path(__file__),
    "paired_order_lifecycle.py": FAMILY_ROOT / "audit" / "paired_order_lifecycle.py",
    "paired_order_lifecycle_smoke.py": (FAMILY_ROOT / "audit" / "paired_order_lifecycle_smoke.py"),
    "request_state_features.py": FAMILY_ROOT / "audit" / "request_state_features.py",
    "request_state_panel.py": FAMILY_ROOT / "audit" / "request_state_panel.py",
    "request_state_race.py": FAMILY_ROOT / "audit" / "request_state_race.py",
    "risk_set_expansion.py": FAMILY_ROOT / "audit" / "risk_set_expansion.py",
    "sparse_order_lifecycle.py": FAMILY_ROOT / "audit" / "sparse_order_lifecycle.py",
    "content_addressed_cache.py": ROOT / "models" / "audit" / "content_addressed_cache.py",
    "native_gap_segments.py": ROOT / "models" / "audit" / "native_gap_segments.py",
    "full_curve_fill_cif.py": FAMILY_ROOT / "audit" / "full_curve_fill_cif.py",
    "active_order_queue.py": ROOT / "models" / "active_order_queue.py",
    "build_active_order_queue_tape.py": ROOT / "data" / "build_active_order_queue_tape.py",
    "download_cryptohft_orderbook.py": ROOT / "data" / "download_cryptohft_orderbook.py",
    "data_quality.py": ROOT / "data_quality.py",
    "local_order_value_replay.py": (
        ROOT
        / "research"
        / "families"
        / "f07_active_order_continuation"
        / "audit"
        / "local_order_value_replay.py"
    ),
    "data_windows.py": ROOT / "models" / "data_windows.py",
    "tick_ab.py": ROOT / "models" / "tick_ab.py",
    "backtest_tick.py": ROOT / "models" / "backtest_tick.py",
    "backtest_config.py": ROOT / "models" / "backtest_config.py",
    "live/config.py": ROOT / "live" / "config.py",
    "request_state_features.cpp": FAMILY_ROOT / "cpp" / "request_state_features.cpp",
    "request_state_features.hpp": FAMILY_ROOT / "cpp" / "request_state_features.hpp",
    "risk_set_expansion.cpp": FAMILY_ROOT / "cpp" / "risk_set_expansion.cpp",
    "risk_set_expansion.hpp": FAMILY_ROOT / "cpp" / "risk_set_expansion.hpp",
    "sparse_order_lifecycle.cpp": FAMILY_ROOT / "cpp" / "sparse_order_lifecycle.cpp",
    "sparse_order_lifecycle.hpp": FAMILY_ROOT / "cpp" / "sparse_order_lifecycle.hpp",
    "bindings.cpp": ROOT / "cpp" / "narrowgate_cpp" / "bindings.cpp",
    "cpp/CMakeLists.txt": ROOT / "cpp" / "CMakeLists.txt",
}
FROZEN_NATIVE_MODULE_KEY = "narrowgate_cpp.cpython-312-darwin.so"


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _canonical_hash(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _ephemeral_file_identity(path: Path) -> dict[str, Any]:
    identity = _file_identity(path)
    identity["path"] = path.name
    identity["retained"] = False
    return identity


def _native_module_identity() -> dict[str, Any]:
    try:
        import narrowgate_cpp  # type: ignore
    except Exception as exc:  # pragma: no cover - environment contract
        raise RuntimeError("formal placement panel requires narrowgate_cpp") from exc
    module_path = Path(str(narrowgate_cpp.__file__)).resolve()
    identity = _file_identity(module_path)
    identity["abi"] = "sparse_order_lifecycle.v1"
    return identity


def _require_identity(path: Path, expected: str, label: str) -> None:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"missing {label}: {resolved}")
    actual = _sha256(resolved)
    if actual != str(expected):
        raise RuntimeError(f"{label} identity changed: expected={expected} actual={actual}")


def _frozen_source_path(source_identity: Mapping[str, Any], key: str) -> Path:
    path = Path(str(source_identity[key]))
    if not path.is_absolute():
        path = ROOT / path
    return path.expanduser().resolve()


def _require_selected_path(selected: Path, frozen: Path, label: str) -> None:
    if selected.resolve() != frozen.resolve():
        raise RuntimeError(
            f"{label} differs from the frozen source identity: "
            f"selected={selected.resolve()} frozen={frozen.resolve()}"
        )


def _validate_spec(spec_path: Path, *, selected_config: Path) -> dict[str, Any]:
    spec = load_placement_fill_spec(spec_path)
    if not str(spec.get("research_status", "")).startswith("frozen_before"):
        raise RuntimeError("placement spec is not frozen before panel access")
    active = spec.get("active_order_estimand", {})
    if active.get("status") != "separate_and_not_built_by_this_pipeline":
        raise RuntimeError("KEEP/REPLACE estimand is not explicitly separate")
    if set(active.get("actions", ())) != {"keep", "replace"}:
        raise RuntimeError("active-order action identity changed")
    if set(spec["placement_estimand"]["actions"]) != set(ACTION_ORDER):
        raise RuntimeError("placement action identity changed")
    sources = spec["source_identity"]
    for path_key, hash_key in (
        ("strict_day_manifest", "strict_day_manifest_sha256"),
        ("normalized_l2_manifest", "normalized_l2_manifest_sha256"),
        ("normalized_l2_daily_quality", "normalized_l2_daily_quality_sha256"),
        ("feature_context_manifest", "feature_context_manifest_sha256"),
        ("config", "config_sha256"),
        ("p3_artifact", "p3_artifact_sha256"),
        ("queue_calibration", "queue_calibration_sha256"),
        ("latency_telemetry", "latency_telemetry_sha256"),
        ("book_visibility_profile", "book_visibility_profile_sha256"),
    ):
        if path_key == "config":
            _require_identity(selected_config, str(sources[hash_key]), path_key)
            continue
        path = Path(str(sources[path_key]))
        if not path.is_absolute():
            path = ROOT / path
        _require_identity(path, str(sources[hash_key]), path_key)
    implementation = spec.get("implementation_identity_at_freeze", {})
    expected_keys = set(FROZEN_IMPLEMENTATION_PATHS) | {FROZEN_NATIVE_MODULE_KEY}
    if set(implementation) != expected_keys:
        raise RuntimeError("frozen implementation identity is incomplete")
    for name, path in FROZEN_IMPLEMENTATION_PATHS.items():
        _require_identity(path, str(implementation[name]), name)
    native = _native_module_identity()
    if native["sha256"] != str(implementation[FROZEN_NATIVE_MODULE_KEY]):
        raise RuntimeError("installed native module differs from frozen ABI")
    return spec


def _select_panel_days(
    spec: Mapping[str, Any], panel_name: str, requested: Sequence[str]
) -> list[str]:
    if panel_name not in spec["panels"]:
        raise RuntimeError(f"unknown frozen panel: {panel_name}")
    frozen = [str(day) for day in spec["panels"][panel_name]["days"]]
    if not requested:
        return frozen
    selected = sorted({str(day) for day in requested})
    forbidden = sorted(set(selected) - set(frozen))
    if forbidden:
        raise RuntimeError(
            f"placement builder is frozen to {panel_name}; forbidden days=" + ",".join(forbidden)
        )
    return [day for day in frozen if day in set(selected)]


def _truthy_csv_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).strip().lower() in {"1", "true", "yes"}


def _validate_formal_days(
    spec: Mapping[str, Any],
    days: Sequence[str],
    *,
    normalized_root: Path,
    feature_context_dir: Path,
) -> None:
    strict_path = Path(str(spec["source_identity"]["strict_day_manifest"]))
    strict = pd.read_csv(strict_path, dtype={"day": str})
    if "day" not in strict.columns:
        raise RuntimeError("strict day manifest has no day column")
    strict_days = frozenset(str(day)[:10] for day in strict["day"])
    absent = sorted(set(days) - strict_days)
    if absent:
        raise RuntimeError(
            "panel days are absent from the frozen strict manifest: " + ",".join(absent)
        )

    quality_path = Path(str(spec["source_identity"]["normalized_l2_daily_quality"]))
    quality = pd.read_csv(quality_path, dtype={"day": str}).set_index("day")
    missing = sorted(set(days) - set(quality.index.astype(str)))
    if missing:
        raise RuntimeError(f"quality registry lacks days: {missing}")
    required = (
        "formal_eligible",
        "warmup_valid",
        "sequence_valid",
        "coverage_99_valid",
    )
    failed: list[str] = []
    for day in days:
        row = quality.loc[day]
        if not all(_truthy_csv_value(row[name]) for name in required):
            failed.append(day)
    if failed:
        raise RuntimeError("panel contains non-formal native-L2 days: " + ",".join(failed))

    missing_trades: list[str] = []
    duplicate_trades: list[str] = []
    missing_context: list[str] = []
    missing_feature_context: list[str] = []
    trade_root = bt.RAW_TRADES_DIR / "BTCUSDC"
    for day in days:
        candidates = [
            path
            for path in (
                trade_root / f"BTCUSDC-trades-{day}.csv",
                trade_root / f"BTCUSDC-trades-{day}.csv.gz",
            )
            if path.is_file()
        ]
        if not candidates:
            missing_trades.append(day)
        elif len(candidates) != 1:
            duplicate_trades.append(day)
    # Each normalized target file already contains the midnight state rebuilt
    # from the native D-1 snapshot/delta warmup. A separately normalized D-1
    # file is useful causal context when present, but is not part of the target
    # day's formal eligibility contract.
    for context_day in days:
        for kind in ("bbo", "l2"):
            path = normalized_root / kind / f"BTCUSDC-{kind}-{context_day}.parquet"
            if not path.is_file():
                missing_context.append(str(path))
        feature_path = feature_context_dir / f"features_{context_day}.parquet"
        if not feature_path.is_file():
            missing_feature_context.append(str(feature_path))
    if missing_trades or duplicate_trades or missing_context or missing_feature_context:
        raise RuntimeError(
            "formal source preflight failed: "
            f"missing_trade_days={missing_trades} "
            f"duplicate_trade_days={duplicate_trades} "
            f"missing_context_files={missing_context[:8]} "
            f"missing_feature_context={missing_feature_context[:8]}"
        )


def _validate_panel_access(
    *,
    spec_path: Path,
    spec: Mapping[str, Any],
    panel_name: str,
    gate_report: Path | None,
) -> None:
    if panel_name == "development":
        return
    if gate_report is None or not gate_report.is_file():
        raise RuntimeError(f"{panel_name} requires an explicit gate report")
    report = json.loads(gate_report.read_text(encoding="utf-8"))
    if report.get("family_id") != spec.get("family_id"):
        raise RuntimeError("panel gate report belongs to another family")
    if report.get("spec_sha256") != _sha256(spec_path):
        raise RuntimeError("panel gate report belongs to another spec identity")
    if bool(report.get("action_or_live_authorization")):
        raise RuntimeError("prediction panel report must not authorize live action")
    if panel_name == "validation":
        if not bool(report.get("development_prediction_gate_passed")):
            raise RuntimeError("Development did not unlock Validation")
        if bool(report.get("validation_read")):
            raise RuntimeError("Validation was already marked read")
        return
    prior_name = (
        "validation_prediction_gate_passed"
        if panel_name == "sealed_holdout"
        else "sealed_holdout_prediction_gate_passed"
    )
    if not bool(report.get(prior_name)):
        raise RuntimeError(f"{panel_name} is not unlocked by {prior_name}")


def _storage_preflight(
    output_dir: Path,
    *,
    remaining_days: int,
    estimated_day_bytes: int,
    reserve_gib: float,
    output_multiplier: float,
) -> dict[str, float]:
    usage = shutil.disk_usage(output_dir.parent)
    free_bytes = int(usage.free)
    reserve_bytes = int(float(reserve_gib) * 1024**3)
    estimated_bytes = max(1, int(remaining_days)) * max(1, int(estimated_day_bytes))
    required_bytes = reserve_bytes + int(float(output_multiplier) * estimated_bytes)
    if free_bytes < required_bytes:
        raise RuntimeError(
            "placement storage gate failed: "
            f"free_gib={free_bytes / 1024**3:.2f} "
            f"required_gib={required_bytes / 1024**3:.2f}"
        )
    return {
        "free_gib": free_bytes / 1024**3,
        "estimated_remaining_gib": estimated_bytes / 1024**3,
        "required_gib": required_bytes / 1024**3,
    }


def _baseline_command(
    *,
    day: str,
    trace_dir: Path,
    args: argparse.Namespace,
) -> list[str]:
    prefix = trace_dir / "baseline"
    command = [
        sys.executable,
        "-m",
        "research.families.f07_active_order_continuation.audit.local_order_value_replay",
        "--days",
        day,
        "--symbol",
        "BTCUSDC",
        "--config",
        str(args.config),
        "--output-prefix",
        str(prefix),
        "--workers",
        "1",
        "--trace-max-per-day",
        str(args.trace_max_per_day),
        "--lifecycle-event-profile",
        "placement_start_stop",
        "--decision-trace-max-per-day",
        str(args.decision_trace_max_per_day),
        "--fill-horizon-ms",
        "1000",
        "--strict-calibration",
        "--queue-calibration-artifact",
        str(args.queue_calibration),
        "--live-perf-telemetry",
        str(args.latency_telemetry),
        "--live-perf-latency-mode",
        "avg",
        "--latency-profile-id",
        "private_not_distributed",
        "--exec-book-visibility-profile",
        str(args.book_visibility_profile),
        "--exec-book-visibility-profile-id",
        "private_not_distributed",
        "--exec-book-visibility-delay-seed",
        "20260718",
        "--execution-trade-source",
        "trades",
        "--feature-context-dir",
        str(args.feature_context_dir),
        "--feature-context-manifest-sha256",
        str(args.feature_context_manifest_sha256),
        "--formal-quality-day-manifest",
        str(args.strict_day_manifest),
        "--formal-quality-day-manifest-sha256",
        str(args.strict_day_manifest_sha256),
        "--bbo-dir",
        str(args.normalized_root / "bbo"),
        "--l2-dir",
        str(args.normalized_root / "l2"),
        "--exchange-book-raw-root",
        str(args.raw_book_root),
        "--exchange-book-mode",
        "strict",
        "--exchange-book-warmup-hours",
        "24",
        "--dynamic-fill-hazard-action-mode",
        "separate_frozen_treatment",
        "--trace-export-only",
        "--watch-manifest-out",
        str(trace_dir / "baseline.watch.parquet"),
        "--watch-trace-max-per-day",
        str(args.trace_max_per_day),
    ]
    if args.window_cache_dir is not None:
        command.extend(("--window-cache-dir", str(args.window_cache_dir)))
    if bool(args.refresh_window_cache):
        command.append("--refresh-window-cache")
    return command


def _run_command(command: Sequence[str], log_path: Path) -> None:
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.run(
            list(command),
            cwd=ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    if process.returncode != 0:
        tail = log_path.read_text(encoding="utf-8", errors="replace")[-8000:]
        raise RuntimeError(f"baseline trace failed with exit={process.returncode}:\n{tail}")


def _isolate_worker_process_group() -> None:
    """Put a pool worker and every subprocess it starts in one killable group."""
    if os.name != "posix" or multiprocessing.current_process().name == "MainProcess":
        return
    if os.getpgrp() != os.getpid():
        os.setpgid(0, 0)


def _terminate_executor_process_groups(
    executor: concurrent.futures.ProcessPoolExecutor,
) -> None:
    processes = list(getattr(executor, "_processes", {}).values())
    for process in processes:
        if process is None or not process.is_alive():
            continue
        try:
            os.killpg(int(process.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            process.terminate()


def _build_day(
    day: str,
    *,
    panel_name: str,
    output_dir: Path,
    run_identity_sha256: str,
    spec: Mapping[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    _isolate_worker_process_group()
    final_dir = output_dir / "partitions" / f"day={day}"
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    staging_root = output_dir / "staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f"day={day}.", dir=staging_root))
    trace_dir = stage / "traces"
    trace_dir.mkdir(parents=True)
    command = _baseline_command(day=day, trace_dir=trace_dir, args=args)
    _run_command(command, stage / "baseline.log")

    partial = trace_dir / "baseline.partial"
    daily_path = partial / f"{day}.daily.json"
    decisions_path = partial / f"{day}.decisions.parquet"
    lifecycle_path = partial / f"{day}.lifecycle.parquet"
    quotes_path = partial / f"{day}.quotes.parquet"
    for path in (daily_path, decisions_path, lifecycle_path, quotes_path):
        if not path.is_file():
            raise RuntimeError(f"baseline trace omitted {path.name}")
    daily = json.loads(daily_path.read_text(encoding="utf-8"))
    if int(daily.get("decision_rows", 0)) >= int(args.decision_trace_max_per_day):
        raise RuntimeError(f"{day} decision trace was truncated")
    if int(daily.get("quote_rows", 0)) >= int(args.trace_max_per_day):
        raise RuntimeError(f"{day} quote trace was truncated")
    if int(daily.get("lifecycle_rows", 0)) >= int(args.trace_max_per_day):
        raise RuntimeError(f"{day} lifecycle trace was truncated")

    decisions = pd.read_parquet(decisions_path)
    lifecycle = pd.read_parquet(lifecycle_path)
    quotes = pd.read_parquet(quotes_path)
    cohorts, join_audit = build_cohorts(
        decisions,
        lifecycle,
        quotes,
        day=day,
        tick_size=0.1,
        lot_size=0.001,
        max_cohorts=int(args.max_cohorts_per_day),
        max_horizon_ms=10_000,
    )
    if not cohorts:
        raise RuntimeError(f"{day} produced no placement cohorts")
    if len(cohorts) >= int(args.max_cohorts_per_day):
        raise RuntimeError(f"{day} placement cohort trace was truncated")
    if int(join_audit.get("missing_decision", 0)) != 0:
        raise RuntimeError(f"{day} has submit rows without a causal decision: {join_audit}")

    bt.configure_symbol("BTCUSDC")
    trades = bt.load_individual_trades(
        days=[day],
        quality_allowed_days=(day,),
    )
    tape = CryptoHFTExchangeBookTape(
        raw_root=args.raw_book_root,
        day=day,
        symbol="BTCUSDC",
        tick_size=0.1,
        warmup_hours=24,
        strict_complete=True,
    )
    tape_identity = tape.identity(include_sha256=True)
    tape_sources = tape_identity.get("files")
    if not isinstance(tape_sources, list):
        raise RuntimeError("native tape identity omitted its source files")
    for source in tape_sources:
        if not isinstance(source, dict):
            raise RuntimeError("native tape source identity is malformed")
        source.pop("mtime_ns", None)
    trade_identity = _individual_trade_identity("BTCUSDC", day)
    watch_manifest_path = stage / "sparse_watches.parquet"
    watch_manifest_tmp = watch_manifest_path.with_suffix(".parquet.tmp")
    build_watch_manifest(cohorts, tick_size=0.1).to_parquet(
        watch_manifest_tmp,
        index=False,
        compression="zstd",
    )
    watch_manifest_tmp.replace(watch_manifest_path)
    native_module = _native_module_identity()
    mechanics_identity = {
        "schema_version": PAIRED_MECHANICS_SCHEMA_VERSION,
        "day": day,
        "cohort_count": int(len(cohorts)),
        "baseline_traces": {
            "decisions": _ephemeral_file_identity(decisions_path),
            "lifecycle": _ephemeral_file_identity(lifecycle_path),
            "quotes": _ephemeral_file_identity(quotes_path),
        },
        "individual_trades": trade_identity,
        "native_tape": tape_identity,
        "sparse_watch_manifest": _ephemeral_file_identity(watch_manifest_path),
        "mechanics": {
            "tick_size": 0.1,
            "lot_size": 0.001,
            "actions": list(ACTION_ORDER),
            "fail_on_monotonicity": True,
        },
        "implementation": {
            "paired_order_lifecycle": _file_identity(
                ROOT / "models" / "audit" / "paired_order_lifecycle.py"
            ),
            "paired_order_lifecycle_smoke": _file_identity(
                ROOT / "models" / "audit" / "paired_order_lifecycle_smoke.py"
            ),
            "exchange_book_replay": _file_identity(ROOT / "models" / "exchange_book_replay.py"),
            "native_book_parser": _file_identity(
                ROOT / "data" / "build_active_order_queue_tape.py"
            ),
            "sparse_order_lifecycle": _file_identity(
                ROOT / "models" / "audit" / "sparse_order_lifecycle.py"
            ),
            "sparse_order_lifecycle_cpp": _file_identity(
                ROOT / "cpp" / "narrowgate_cpp" / "sparse_order_lifecycle.cpp"
            ),
            "sparse_order_lifecycle_hpp": _file_identity(
                ROOT / "cpp" / "narrowgate_cpp" / "sparse_order_lifecycle.hpp"
            ),
            "bindings_cpp": _file_identity(ROOT / "cpp" / "narrowgate_cpp" / "bindings.cpp"),
            "cmake": _file_identity(ROOT / "cpp" / "CMakeLists.txt"),
            "content_addressed_cache": _file_identity(
                ROOT / "models" / "audit" / "content_addressed_cache.py"
            ),
            "native_module": native_module,
        },
    }
    mechanics_cache = ParquetContentAddressedCache(
        args.paired_mechanics_cache_dir,
        namespace="day",
    )
    cached_mechanics = mechanics_cache.load(mechanics_identity)
    if cached_mechanics is None:
        sparse_identity = {
            "schema_version": SPARSE_TAPE_CACHE_SCHEMA_VERSION,
            "day": day,
            "symbol": "BTCUSDC",
            "tick_size": 0.1,
            "warmup_hours": 24,
            "watch_manifest": _ephemeral_file_identity(watch_manifest_path),
            "native_tape": tape_identity,
            "implementation": {
                "builder": _file_identity(ROOT / "data" / "build_active_order_queue_tape.py"),
                "downloader_parser": _file_identity(
                    ROOT / "data" / "download_cryptohft_orderbook.py"
                ),
                "adapter": _file_identity(ROOT / "models" / "audit" / "sparse_order_lifecycle.py"),
                "content_addressed_cache": _file_identity(
                    ROOT / "models" / "audit" / "content_addressed_cache.py"
                ),
            },
        }
        sparse_cache = DirectoryContentAddressedCache(
            args.sparse_queue_tape_cache_dir,
            namespace="day",
        )

        def build_sparse_tape(payload_dir: Path) -> Mapping[str, Any]:
            return build_active_order_queue_tape(
                watch_manifest=watch_manifest_path,
                raw_root=args.raw_book_root,
                output_dir=payload_dir,
                symbol="BTCUSDC",
                tick_size=0.1,
                warmup_hours=24,
                reuse_raw_only=True,
            )

        sparse_record = sparse_cache.get_or_build(
            sparse_identity,
            build_sparse_tape,
        )
        queue_mults = {
            round(float(child.queue_deplete_mult), 12)
            for cohort in cohorts
            for child in cohort.children.values()
        }
        if len(queue_mults) != 1:
            raise RuntimeError(f"{day} sparse native v1 requires one queue multiplier")
        rows, simulation = simulate_sparse_paired_placements(
            cohorts,
            tape_dir=sparse_record.payload_dir,
            trades=trades,
            tick_size=0.1,
            lot_size=0.001,
            queue_deplete_mult=float(next(iter(queue_mults))),
            fail_on_monotonicity=True,
        )
        simulation["sparse_tape_cache"] = {
            "key": sparse_record.key,
            "hit": bool(sparse_record.hit),
            "identity_sha256": str(sparse_record.manifest["identity_sha256"]),
            "files": list(sparse_record.manifest["files"]),
            "summary": dict(sparse_record.manifest.get("metadata", {})),
        }
        mechanics_record = mechanics_cache.store(
            mechanics_identity,
            rows,
            metadata={"simulation": simulation},
        )
        mechanics_cache_hit = False
    else:
        mechanics_record = cached_mechanics
        rows = cached_mechanics.frame
        simulation = dict(cached_mechanics.manifest.get("metadata", {}).get("simulation", {}))
        mechanics_cache_hit = True
    watch_manifest_path.unlink(missing_ok=True)
    if not simulation:
        raise RuntimeError(f"{day} paired mechanics cache lacks simulation audit")
    if int(simulation["monotonicity_violations"]) != 0:
        raise RuntimeError(f"{day} violated placement distance monotonicity")
    if not bool((pd.to_numeric(rows["feature_ready_ts_ns"]) <= rows["submit_ts_ns"]).all()):
        raise RuntimeError(f"{day} contains future-ready decision features")
    if {"keep", "replace"} & set(rows.filter(regex=r"__(action)$").astype(str).stack().str.lower()):
        raise RuntimeError("active-order KEEP/REPLACE leaked into placement panel")

    panel_path = stage / "placement.parquet"
    temporary = panel_path.with_suffix(".parquet.tmp")
    rows.to_parquet(temporary, index=False, compression="zstd")
    temporary.replace(panel_path)
    manifest = {
        "schema_version": BUILDER_SCHEMA_VERSION,
        "panel_schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "day": day,
        "panel": panel_name,
        "estimand": "new_placement_direct_fill_cif",
        "active_order_estimand": "separate_not_built",
        "run_identity_sha256": run_identity_sha256,
        "rows": int(len(rows)),
        "side_counts": {str(k): int(v) for k, v in rows["side"].value_counts().items()},
        "role_counts": {str(k): int(v) for k, v in rows["inventory_role"].value_counts().items()},
        "join_audit": join_audit,
        "simulation": simulation,
        "paired_mechanics_cache": {
            "key": mechanics_record.key,
            "hit": mechanics_cache_hit,
            "payload_sha256": str(mechanics_record.manifest["payload_sha256"]),
            "identity_sha256": str(mechanics_record.manifest["identity_sha256"]),
        },
        "sparse_queue_tape_cache": {
            "path": str(args.sparse_queue_tape_cache_dir),
            "schema_version": SPARSE_TAPE_CACHE_SCHEMA_VERSION,
        },
        "baseline_daily": daily,
        "baseline_command": [
            str(value).replace(str(stage), "{partition_stage}") for value in command
        ],
        "input_artifacts": {
            "decisions": _ephemeral_file_identity(decisions_path),
            "lifecycle": _ephemeral_file_identity(lifecycle_path),
            "quotes": _ephemeral_file_identity(quotes_path),
            "individual_trades": trade_identity,
        },
        "derived_baseline_trace_retention": "deleted_after_panel_admission",
        "native_tape_identity": tape_identity,
        "panel_path": str(final_dir / "placement.parquet"),
        "panel_sha256": _sha256(panel_path),
        "action_or_live_authorization": False,
    }
    _atomic_json(manifest, stage / "manifest.json")
    shutil.rmtree(trace_dir)
    (stage / "COMPLETE").write_text(run_identity_sha256 + "\n", encoding="ascii")
    if final_dir.exists():
        raise RuntimeError(f"refusing to replace completed partition {final_dir}")
    stage.replace(final_dir)
    return manifest


def _partition_manifest(
    output_dir: Path, day: str, run_identity_sha256: str
) -> dict[str, Any] | None:
    directory = output_dir / "partitions" / f"day={day}"
    manifest_path = directory / "manifest.json"
    panel_path = directory / "placement.parquet"
    complete_path = directory / "COMPLETE"
    if not (manifest_path.is_file() and panel_path.is_file() and complete_path.is_file()):
        return None
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("run_identity_sha256") != run_identity_sha256:
        raise RuntimeError(f"{day} partition belongs to another run identity")
    if manifest.get("panel_sha256") != _sha256(panel_path):
        raise RuntimeError(f"{day} partition checksum mismatch")
    return manifest


def _preflight_manifest(
    *,
    args: argparse.Namespace,
    spec: Mapping[str, Any],
    days: Sequence[str],
    panel_name: str,
) -> dict[str, Any]:
    code = git_workspace_identity(ROOT)
    implementation = {
        name: _file_identity(path)
        for name, path in {
            "placement_fill_panel": Path(__file__),
            "paired_order_lifecycle": FAMILY_ROOT / "audit" / "paired_order_lifecycle.py",
            "paired_order_lifecycle_smoke": (
                FAMILY_ROOT / "audit" / "paired_order_lifecycle_smoke.py"
            ),
            "exchange_book_replay": ROOT / "models" / "exchange_book_replay.py",
            "local_order_value_replay": (
                ROOT
                / "research"
                / "families"
                / "f07_active_order_continuation"
                / "audit"
                / "local_order_value_replay.py"
            ),
            "order_lifecycle": ROOT / "models" / "audit" / "order_lifecycle.py",
            "backtest_tick": ROOT / "models" / "backtest_tick.py",
            "sparse_order_lifecycle": (FAMILY_ROOT / "audit" / "sparse_order_lifecycle.py"),
            "active_order_queue_tape": ROOT / "data" / "build_active_order_queue_tape.py",
            "sparse_order_lifecycle_cpp": (FAMILY_ROOT / "cpp" / "sparse_order_lifecycle.cpp"),
            "sparse_order_lifecycle_hpp": (FAMILY_ROOT / "cpp" / "sparse_order_lifecycle.hpp"),
            "bindings_cpp": ROOT / "cpp" / "narrowgate_cpp" / "bindings.cpp",
            "cmake": ROOT / "cpp" / "CMakeLists.txt",
            "content_addressed_cache": ROOT / "models" / "audit" / "content_addressed_cache.py",
        }.items()
    }
    implementation["native_module"] = _native_module_identity()
    identity = {
        "schema_version": BUILDER_SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "panel": panel_name,
        "days": list(days),
        "spec": _file_identity(args.spec),
        "config": _file_identity(args.config),
        "normalized_manifest": _file_identity(args.normalized_root / "manifest.json"),
        "daily_quality": _file_identity(args.normalized_root / "daily_quality.csv"),
        "strict_day_manifest": _file_identity(args.strict_day_manifest),
        "queue_calibration": _file_identity(args.queue_calibration),
        "latency_telemetry": _file_identity(args.latency_telemetry),
        "book_visibility_profile": _file_identity(args.book_visibility_profile),
        "feature_context_manifest": _file_identity(
            args.feature_context_dir / "causal_feature_manifest.json"
        ),
        "implementation": implementation,
        "git": code,
        "trace_limits": {
            "lifecycle_event_profile": "placement_start_stop",
            "trace_max_per_day": int(args.trace_max_per_day),
            "decision_trace_max_per_day": int(args.decision_trace_max_per_day),
            "max_cohorts_per_day": int(args.max_cohorts_per_day),
        },
        "window_cache": {
            "path": str(args.window_cache_dir) if args.window_cache_dir else None,
            "refresh": bool(args.refresh_window_cache),
            "cache_version": str(WINDOW_CACHE_VERSION),
        },
        "paired_mechanics_cache": {
            "path": str(args.paired_mechanics_cache_dir),
            "schema_version": PAIRED_MECHANICS_SCHEMA_VERSION,
        },
        "sparse_queue_tape_cache": {
            "path": str(args.sparse_queue_tape_cache_dir),
            "schema_version": SPARSE_TAPE_CACHE_SCHEMA_VERSION,
        },
        "active_order_keep_replace": "separate_not_built",
    }
    identity["run_identity_sha256"] = _canonical_hash(identity)
    return identity


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Exact config bytes frozen by --spec; no mutable current-live default.",
    )
    parser.add_argument("--normalized-root", type=Path, default=DEFAULT_NORMALIZED)
    parser.add_argument(
        "--feature-context-dir",
        type=Path,
        default=DEFAULT_FEATURE_CONTEXT,
    )
    parser.add_argument("--raw-book-root", type=Path, default=DEFAULT_RAW_BOOK)
    parser.add_argument("--queue-calibration", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--latency-telemetry", type=Path, required=True)
    parser.add_argument("--book-visibility-profile", type=Path, default=DEFAULT_VISIBILITY)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--panel-name",
        choices=("development", "validation", "sealed_holdout", "late_evidence"),
        default="development",
    )
    parser.add_argument("--gate-report", type=Path)
    parser.add_argument("--days", nargs="*", default=[])
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--trace-max-per-day", type=int, default=100_000)
    parser.add_argument("--decision-trace-max-per-day", type=int, default=100_000)
    parser.add_argument("--max-cohorts-per-day", type=int, default=100_000)
    parser.add_argument("--estimated-day-mib", type=float, default=24.0)
    parser.add_argument("--window-cache-dir", type=Path, default=DEFAULT_WINDOW_CACHE)
    parser.add_argument(
        "--no-window-cache",
        action="store_true",
        help=(
            "Disable the large full-window pickle cache while retaining the "
            "content-addressed sparse-tape and paired-mechanics caches."
        ),
    )
    parser.add_argument(
        "--paired-mechanics-cache-dir",
        type=Path,
        default=DEFAULT_PAIRED_MECHANICS_CACHE,
    )
    parser.add_argument(
        "--sparse-queue-tape-cache-dir",
        type=Path,
        default=DEFAULT_SPARSE_QUEUE_TAPE_CACHE,
    )
    parser.add_argument("--refresh-window-cache", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    for name in (
        "spec",
        "config",
        "normalized_root",
        "feature_context_dir",
        "raw_book_root",
        "queue_calibration",
        "latency_telemetry",
        "book_visibility_profile",
        "output_dir",
        "paired_mechanics_cache_dir",
        "sparse_queue_tape_cache_dir",
    ):
        setattr(args, name, getattr(args, name).expanduser().resolve())
    if args.no_window_cache:
        args.window_cache_dir = None
    elif args.window_cache_dir is not None:
        args.window_cache_dir = args.window_cache_dir.expanduser().resolve()
    if args.gate_report is not None:
        args.gate_report = args.gate_report.expanduser().resolve()
    if (
        min(
            int(args.trace_max_per_day),
            int(args.decision_trace_max_per_day),
            int(args.max_cohorts_per_day),
        )
        <= 0
    ):
        raise SystemExit("trace and cohort limits must be positive")
    spec = _validate_spec(args.spec, selected_config=args.config)
    source_identity = spec["source_identity"]
    args.strict_day_manifest = (
        Path(str(source_identity["strict_day_manifest"])).expanduser().resolve()
    )
    args.strict_day_manifest_sha256 = str(source_identity["strict_day_manifest_sha256"])
    args.feature_context_manifest_sha256 = str(source_identity["feature_context_manifest_sha256"])
    _require_selected_path(
        args.queue_calibration,
        _frozen_source_path(source_identity, "queue_calibration"),
        "queue calibration",
    )
    _require_selected_path(
        args.latency_telemetry,
        _frozen_source_path(source_identity, "latency_telemetry"),
        "latency telemetry",
    )
    _require_selected_path(
        args.book_visibility_profile,
        _frozen_source_path(source_identity, "book_visibility_profile"),
        "book visibility profile",
    )
    _require_selected_path(
        args.raw_book_root,
        _frozen_source_path(source_identity, "native_exchange_book_root"),
        "native exchange-book root",
    )
    _require_selected_path(
        args.normalized_root / "manifest.json",
        _frozen_source_path(source_identity, "normalized_l2_manifest"),
        "normalized L2 manifest",
    )
    _require_selected_path(
        args.normalized_root / "daily_quality.csv",
        _frozen_source_path(source_identity, "normalized_l2_daily_quality"),
        "normalized L2 quality registry",
    )
    frozen_feature_manifest = _frozen_source_path(source_identity, "feature_context_manifest")
    selected_feature_manifest = (
        args.feature_context_dir / "causal_feature_manifest.json"
    ).resolve()
    _require_selected_path(
        selected_feature_manifest,
        frozen_feature_manifest,
        "feature context manifest",
    )
    _validate_panel_access(
        spec_path=args.spec,
        spec=spec,
        panel_name=str(args.panel_name),
        gate_report=args.gate_report,
    )
    days = _select_panel_days(spec, str(args.panel_name), args.days)
    _validate_formal_days(
        spec,
        days,
        normalized_root=args.normalized_root,
        feature_context_dir=args.feature_context_dir,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    preflight = _preflight_manifest(
        args=args,
        spec=spec,
        days=days,
        panel_name=str(args.panel_name),
    )
    identity_path = args.output_dir / "preflight_manifest.json"
    if identity_path.is_file():
        prior = json.loads(identity_path.read_text(encoding="utf-8"))
        if prior.get("run_identity_sha256") != preflight["run_identity_sha256"]:
            raise RuntimeError("refusing to resume across a different run identity")
    else:
        _atomic_json(preflight, identity_path)
        write_code_checkpoint(
            args.output_dir / "code_checkpoint",
            repo_root=ROOT,
            code_identity=preflight["git"],
        )

    completed: dict[str, dict[str, Any]] = {}
    pending: list[str] = []
    for day in days:
        manifest = _partition_manifest(args.output_dir, day, preflight["run_identity_sha256"])
        if manifest is None:
            pending.append(day)
        else:
            completed[day] = manifest
    storage = _storage_preflight(
        args.output_dir,
        remaining_days=len(pending),
        estimated_day_bytes=int(float(args.estimated_day_mib) * 1024**2),
        reserve_gib=float(spec["storage_gate"]["minimum_free_gib_reserve"]),
        output_multiplier=float(
            spec["storage_gate"]["minimum_free_multiplier_of_estimated_output"]
        ),
    )
    print(
        json.dumps(
            {"completed": len(completed), "pending": len(pending), "storage": storage},
            sort_keys=True,
        ),
        flush=True,
    )

    workers = max(1, min(int(args.workers), max(1, len(pending))))
    if workers == 1:
        for index, day in enumerate(pending, 1):
            print(f"[{index:02d}/{len(pending):02d}] {day}", flush=True)
            completed[day] = _build_day(
                day,
                panel_name=str(args.panel_name),
                output_dir=args.output_dir,
                run_identity_sha256=preflight["run_identity_sha256"],
                spec=spec,
                args=args,
            )
    elif pending:
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=workers)
        day_iter = iter(pending)
        futures: dict[concurrent.futures.Future, str] = {}

        def submit_next() -> bool:
            try:
                next_day = next(day_iter)
            except StopIteration:
                return False
            future = executor.submit(
                _build_day,
                next_day,
                panel_name=str(args.panel_name),
                output_dir=args.output_dir,
                run_identity_sha256=preflight["run_identity_sha256"],
                spec=spec,
                args=args,
            )
            futures[future] = next_day
            return True

        for _ in range(workers):
            submit_next()
        try:
            while futures:
                done, _ = concurrent.futures.wait(
                    futures,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    day = futures.pop(future)
                    completed[day] = future.result()
                    print(f"complete {day}", flush=True)
                    submit_next()
        except BaseException:
            for future in futures:
                future.cancel()
            _terminate_executor_process_groups(executor)
            executor.shutdown(wait=True, cancel_futures=True)
            raise
        else:
            executor.shutdown(wait=True, cancel_futures=True)

    index_rows = []
    for day in days:
        manifest = completed[day]
        index_rows.append(
            {
                "day": day,
                "rows": int(manifest["rows"]),
                "buy_rows": int(manifest["side_counts"].get("BUY", 0)),
                "sell_rows": int(manifest["side_counts"].get("SELL", 0)),
                "monotonicity_violations": int(manifest["simulation"]["monotonicity_violations"]),
                "panel_sha256": str(manifest["panel_sha256"]),
            }
        )
    index = pd.DataFrame(index_rows)
    index_name = f"{args.panel_name}_index.csv"
    temporary = args.output_dir / f"{index_name}.tmp"
    index.to_csv(temporary, index=False)
    temporary.replace(args.output_dir / index_name)
    final = {
        **preflight,
        "status": f"{args.panel_name}_panel_complete",
        "partition_count": int(len(index)),
        "rows": int(index["rows"].sum()),
        "storage_after": _storage_preflight(
            args.output_dir,
            remaining_days=0,
            estimated_day_bytes=1,
            reserve_gib=float(spec["storage_gate"]["minimum_free_gib_reserve"]),
            output_multiplier=0.0,
        ),
        "validation_read": bool(args.panel_name == "validation"),
        "sealed_holdout_read": bool(args.panel_name == "sealed_holdout"),
        "active_order_keep_replace": "separate_not_built",
    }
    _atomic_json(final, args.output_dir / "manifest.json")
    print(json.dumps({"rows": final["rows"], "days": len(index)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
