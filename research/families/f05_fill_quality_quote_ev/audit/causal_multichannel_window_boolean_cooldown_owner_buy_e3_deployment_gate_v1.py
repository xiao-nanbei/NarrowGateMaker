"""Fail-closed deployment gate for the exact owner-selected BUY E3 artifact.

The gate has two inputs produced on the current live host while BUY E3 is
disabled: a real callback-rate/health window and an isolated exact-artifact
benchmark paced above that observed rate.  It never scores a hypothetical live
action and never reads research economics.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import resource
import shutil
import statistics
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from strategy.boolean_cooldown_buy_e3 import (
    BASE_WINDOW_WIDTH_NS,
    CONTROL_ACTION,
    OWNER_IDENTITY,
    LiveBuyE3CooldownPolicy,
    ReceiveTimeFullMidEmaWindows,
)

HEALTH_SCHEMA = f"{OWNER_IDENTITY}.host_health_window.v1"
BENCHMARK_SCHEMA = f"{OWNER_IDENTITY}.host_benchmark.v1"
GATE_SCHEMA = f"{OWNER_IDENTITY}.deployment_gate.v1"
REGRESSION_SCHEMA = f"{OWNER_IDENTITY}.runtime_regression_test_receipt.v1"

RUNTIME_REGRESSION_TESTS = (
    "tests/test_boolean_cooldown_buy_e3.py",
    "tests/test_live_fill_cooldown_policy.py",
    "tests/test_fill_cooldown_contract.py",
    "tests/test_live_runtime_policy.py",
    "tests/test_causal_multichannel_window_boolean_cooldown_owner_buy_e3_refit_v1.py",
    "tests/test_causal_multichannel_window_boolean_cooldown_owner_buy_e3_closeout_v1.py",
    "tests/test_f05_cpp_real_day_lockstep_v22.py",
    "tests/test_causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_sell_only_v1.py",
)
RUNTIME_REGRESSION_SOURCES = (
    "strategy/boolean_cooldown_buy_e3.py",
    "strategy/maker_engine.py",
    "live/config.py",
    "live/main.py",
    "live/runtime_policy.py",
)

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_KV_RE = re.compile(r"\b([A-Za-z][A-Za-z0-9]*)=([^\s]+)")
_LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s")
_HEALTH_KEYS = frozenset(
    {
        "booleanCooldownEnabled",
        "booleanCooldownUpdates",
        "booleanCooldownInvalid",
        "buyE3CooldownEnabled",
        "buyE3CooldownUpdates",
        "buyE3CooldownEval",
        "buyE3CooldownFallback",
        "buyE3CooldownDecisionP99Us",
        "buyE3CooldownWarm",
        "buyE3CooldownWindows",
        "buyE3CooldownGapResets",
        "buyE3CooldownInvalid",
        "marketTapeDropped",
        "marketTapeInvalid",
        "externalRecordDropped",
        "globalFlowTradeOverflow",
        "globalFlowBookOverflow",
        "deepBookValid",
        "deepBookStale",
        "deepBookBuffer",
    }
)
_ZERO_DELTA_KEYS = (
    "marketTapeDropped",
    "marketTapeInvalid",
    "externalRecordDropped",
    "globalFlowTradeOverflow",
    "globalFlowBookOverflow",
    "booleanCooldownInvalid",
    "buyE3CooldownInvalid",
)
_FATAL_PATTERNS = (
    "traceback (most recent call last)",
    "exc_bad_access",
    "sigsegv",
    "out of memory",
    "oom-kill",
    "identity_drift",
    "artifact_file_hash_drift",
)


class BuyE3DeploymentGateError(RuntimeError):
    """Raised when host evidence or a deployment threshold fails closed."""


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _document_sha256(payload: dict[str, Any], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return _canonical_sha256(body)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, payload: dict[str, Any]) -> str:
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise BuyE3DeploymentGateError(f"immutable receipt already exists: {target.name}")
    temporary = target.with_name(f".{target.name}.tmp.{os.getpid()}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="ascii") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        os.chmod(target, 0o600)
        directory = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()
    return _file_sha256(target)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.expanduser().resolve().read_text(encoding="ascii"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BuyE3DeploymentGateError(f"receipt unreadable: {path.name}") from exc
    if not isinstance(payload, dict):
        raise BuyE3DeploymentGateError(f"receipt root malformed: {path.name}")
    return payload


def _numeric(value: str) -> int | float | str:
    try:
        numeric = float(value)
    except ValueError:
        return value
    if math.isfinite(numeric) and numeric.is_integer():
        return int(numeric)
    return numeric


def _parse_health_line(line: str) -> tuple[float, dict[str, int | float | str]]:
    timestamp = _LOG_TS_RE.match(line)
    if timestamp is None or "[main] INFO HEALTH " not in line:
        raise BuyE3DeploymentGateError("live health line format drifted")
    wall = datetime.strptime(timestamp.group(1), "%Y-%m-%d %H:%M:%S").timestamp()
    parsed = {key: _numeric(value) for key, value in _KV_RE.findall(line) if key in _HEALTH_KEYS}
    missing = {
        "booleanCooldownEnabled",
        "booleanCooldownUpdates",
        "buyE3CooldownEnabled",
        "buyE3CooldownUpdates",
    } - set(parsed)
    if missing:
        raise BuyE3DeploymentGateError(
            f"live health line lacks deployment fields: {sorted(missing)}"
        )
    return wall, parsed


def _read_proc_status(pid: int) -> dict[str, str]:
    status_path = Path(f"/proc/{int(pid)}/status")
    if not status_path.is_file():
        raise BuyE3DeploymentGateError("live process disappeared during host gate")
    output: dict[str, str] = {}
    for line in status_path.read_text(encoding="ascii").splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            output[key] = value.strip()
    return output


def _kib_field(status: dict[str, str], field: str) -> int:
    match = re.fullmatch(r"(\d+)\s+kB", status.get(field, ""))
    if match is None:
        raise BuyE3DeploymentGateError(f"/proc status lacks {field}")
    return int(match.group(1))


def _meminfo() -> dict[str, int]:
    path = Path("/proc/meminfo")
    if not path.is_file():
        raise BuyE3DeploymentGateError("host gate requires Linux /proc/meminfo")
    output: dict[str, int] = {}
    for line in path.read_text(encoding="ascii").splitlines():
        match = re.fullmatch(r"([^:]+):\s+(\d+)\s+kB", line)
        if match is not None:
            output[match.group(1)] = int(match.group(2))
    for required in ("MemTotal", "MemAvailable", "SwapTotal"):
        if required not in output:
            raise BuyE3DeploymentGateError(f"host meminfo lacks {required}")
    return output


def _proc_cpu_ticks(pid: int) -> int:
    fields = Path(f"/proc/{int(pid)}/stat").read_text(encoding="ascii").split()
    if len(fields) < 15:
        raise BuyE3DeploymentGateError("live process stat is malformed")
    return int(fields[13]) + int(fields[14])


def _live_process_count() -> int:
    count = 0
    for path in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            command = path.read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="replace")
        except OSError:
            continue
        if "live/main.py" in command:
            count += 1
    return count


def _git_identity(repository_root: Path) -> dict[str, Any]:
    root = repository_root.expanduser().resolve()

    def run(*args: str) -> str:
        completed = subprocess.run(
            ("git", *args),
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout.strip()

    tags = [value for value in run("tag", "--points-at", "HEAD").splitlines() if value]
    annotated = [tag for tag in tags if run("cat-file", "-t", f"refs/tags/{tag}") == "tag"]
    return {
        "commit": run("rev-parse", "HEAD"),
        "annotated_tags_at_head": sorted(annotated),
        "worktree_clean": run("status", "--porcelain") == "",
    }


def capture_live_health_window(
    *,
    repository_root: Path,
    log_path: Path,
    pid_file: Path,
    live_config_path: Path,
    expected_buy_e3_enabled: bool,
    output_path: Path,
    timeout_s: float = 150.0,
) -> dict[str, Any]:
    """Capture two new health lines without retaining any economic fields."""

    log_file = log_path.expanduser().resolve()
    process_id = int(pid_file.expanduser().resolve().read_text(encoding="ascii").strip())
    command = Path(f"/proc/{process_id}/cmdline").read_bytes().replace(b"\x00", b" ")
    if b"live/main.py" not in command:
        raise BuyE3DeploymentGateError("pid file does not identify live/main.py")
    offset = log_file.stat().st_size
    started = time.monotonic()
    cpu_started = _proc_cpu_ticks(process_id)
    segment = ""
    pending_line = ""
    health_lines: list[str] = []
    while len(health_lines) < 2:
        if time.monotonic() - started > float(timeout_s):
            raise BuyE3DeploymentGateError("timed out waiting for two live health lines")
        size = log_file.stat().st_size
        if size < offset:
            offset = 0
        if size > offset:
            with log_file.open("r", encoding="utf-8", errors="replace") as handle:
                handle.seek(offset)
                chunk = handle.read()
                offset = handle.tell()
            segment += chunk
            pending_line += chunk
            complete = pending_line.splitlines(keepends=True)
            pending_line = ""
            if complete and not complete[-1].endswith(("\n", "\r")):
                pending_line = complete.pop()
            health_lines.extend(
                line.rstrip("\r\n") for line in complete if "[main] INFO HEALTH " in line
            )
        if len(health_lines) < 2:
            time.sleep(0.25)
    elapsed = max(time.monotonic() - started, 1e-9)
    cpu_ticks = _proc_cpu_ticks(process_id) - cpu_started
    ticks_per_second = int(os.sysconf("SC_CLK_TCK"))
    first_wall, first = _parse_health_line(health_lines[0])
    second_wall, second = _parse_health_line(health_lines[1])
    health_elapsed = second_wall - first_wall
    if health_elapsed <= 0.0:
        raise BuyE3DeploymentGateError("live health timestamps did not advance")
    buy_expected = int(bool(expected_buy_e3_enabled))
    if (
        int(first["buyE3CooldownEnabled"]) != buy_expected
        or int(second["buyE3CooldownEnabled"]) != buy_expected
    ):
        raise BuyE3DeploymentGateError("BUY E3 activation state changed during gate")
    sell_updates = int(second["booleanCooldownUpdates"]) - int(first["booleanCooldownUpdates"])
    if sell_updates <= 0:
        raise BuyE3DeploymentGateError("actual execution-book callback counter did not advance")
    deltas = {key: int(second.get(key, 0)) - int(first.get(key, 0)) for key in _ZERO_DELTA_KEYS}
    status = _read_proc_status(process_id)
    memory = _meminfo()
    lowered = segment.lower()
    fatal_counts = {pattern: lowered.count(pattern) for pattern in _FATAL_PATTERNS}
    git = _git_identity(repository_root)
    receipt: dict[str, Any] = {
        "schema_version": HEALTH_SCHEMA,
        "identity": OWNER_IDENTITY,
        "status": "pre_enable_live_health_window_complete",
        "generated_utc": _utc_now(),
        "host": {
            "logical_host": "<current-live-host>",
            "logical_cpu_count": int(os.cpu_count() or 0),
            "mem_total_mib": memory["MemTotal"] / 1024.0,
            "mem_available_mib": memory["MemAvailable"] / 1024.0,
            "swap_total_mib": memory["SwapTotal"] / 1024.0,
            "load_1m": float(os.getloadavg()[0]),
        },
        "process": {
            "process_count": _live_process_count(),
            "rss_mib": _kib_field(status, "VmRSS") / 1024.0,
            "rss_high_water_mib": _kib_field(status, "VmHWM") / 1024.0,
            "cpu_percent_one_core_scale": (cpu_ticks / ticks_per_second / elapsed * 100.0),
        },
        "runtime": {
            "buy_e3_enabled": bool(expected_buy_e3_enabled),
            "sell_owner_enabled": int(second["booleanCooldownEnabled"]) == 1,
            "health_window_s": health_elapsed,
            "actual_execution_book_callback_count": sell_updates,
            "actual_execution_book_callback_rate_hz": sell_updates / health_elapsed,
            "counter_deltas": deltas,
            "fatal_pattern_counts": fatal_counts,
            "first_health_line_sha256": hashlib.sha256(health_lines[0].encode("utf-8")).hexdigest(),
            "second_health_line_sha256": hashlib.sha256(
                health_lines[1].encode("utf-8")
            ).hexdigest(),
        },
        "repository": git,
        "live_config_file_sha256": _file_sha256(live_config_path.expanduser().resolve()),
        "economic_values_persisted": False,
        "hypothetical_actions_scored": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_health_receipt_sha256"] = _document_sha256(
        receipt, "canonical_health_receipt_sha256"
    )
    _atomic_json(output_path, receipt)
    return receipt


def _percentile(values: list[float], probability: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * probability) - 1)]


def _max_rss_mib() -> float:
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value / 1024.0 if Path("/proc").is_dir() else value / (1024.0 * 1024.0)


def _load_runtime(
    *,
    artifact_manifest_path: Path,
    artifact_manifest_file_sha256: str,
    expected_artifact_sha256: str,
    policy_path: Path,
    policy_file_sha256: str,
    predicate_bundle_path: Path,
    predicate_bundle_file_sha256: str,
) -> LiveBuyE3CooldownPolicy:
    return LiveBuyE3CooldownPolicy.from_files(
        artifact_manifest_path=artifact_manifest_path,
        artifact_manifest_sha256=artifact_manifest_file_sha256,
        expected_artifact_sha256=expected_artifact_sha256,
        policy_path=policy_path,
        policy_sha256=policy_file_sha256,
        predicate_bundle_path=predicate_bundle_path,
        predicate_bundle_sha256=predicate_bundle_file_sha256,
        warmup_s=2048.0,
        max_feature_age_s=1.0,
    )


def _event_mid(seconds: float) -> float:
    return (
        70_000.0
        + 35.0 * math.sin(seconds * 2.0 * math.pi / 17.0)
        + 80.0 * math.sin(seconds * 2.0 * math.pi / 311.0)
    )


def _observe(policy: LiveBuyE3CooldownPolicy, ts_ns: int, seconds: float) -> None:
    mid = _event_mid(seconds)
    policy.observe_depth(
        receive_ts_ns=int(ts_ns),
        bids=((mid - 0.5, 1.0),),
        asks=((mid + 0.5, 1.0),),
        market_generation=max(1, int(ts_ns // BASE_WINDOW_WIDTH_NS)),
        depth_generation=max(1, int(ts_ns // BASE_WINDOW_WIDTH_NS)),
    )


def run_host_benchmark(
    *,
    artifact_manifest_path: Path,
    artifact_manifest_file_sha256: str,
    expected_artifact_sha256: str,
    policy_path: Path,
    policy_file_sha256: str,
    predicate_bundle_path: Path,
    predicate_bundle_file_sha256: str,
    health_receipt_path: Path,
    output_path: Path,
    paced_duration_s: float = 15.0,
) -> dict[str, Any]:
    """Benchmark exact bytes at >=2x the observed live callback rate."""

    health = validate_health_receipt(health_receipt_path)
    observed_rate = float(health["runtime"]["actual_execution_book_callback_rate_hz"])
    if observed_rate <= 0.0:
        raise BuyE3DeploymentGateError("observed callback rate is invalid")
    target_rate = max(100.0, observed_rate * 2.0)
    policy = _load_runtime(
        artifact_manifest_path=artifact_manifest_path,
        artifact_manifest_file_sha256=artifact_manifest_file_sha256,
        expected_artifact_sha256=expected_artifact_sha256,
        policy_path=policy_path,
        policy_file_sha256=policy_file_sha256,
        predicate_bundle_path=predicate_bundle_path,
        predicate_bundle_file_sha256=predicate_bundle_file_sha256,
    )
    cold = policy.evaluate(
        side="BUY",
        baseline_duration_ms=170_000,
        campaign_age_s=200.0,
        decision_ts_ns=1,
        snapshot_id="host-benchmark-cold",
    )
    if cold.action_id != CONTROL_ACTION or cold.fallback_reason is None:
        raise BuyE3DeploymentGateError("cold/restart fallback did not preserve B0")

    # Fast-forward the exact 2048-second receive-time warmup without wall-clock sleep.
    base_ns = 10_000_000_000
    warmup_windows = 20_490
    for ordinal in range(warmup_windows + 1):
        seconds = ordinal / 10.0
        _observe(
            policy,
            base_ns + ordinal * BASE_WINDOW_WIDTH_NS + 1,
            seconds,
        )
    warm_decision_ts = base_ns + (warmup_windows + 1) * BASE_WINDOW_WIDTH_NS + 1
    warm = policy.evaluate(
        side="BUY",
        baseline_duration_ms=170_000,
        campaign_age_s=400.0,
        decision_ts_ns=warm_decision_ts,
        snapshot_id="host-benchmark-warm",
    )
    if warm.fallback_reason in {
        "no_completed_receive_time_window",
        "receive_time_ema_warmup_incomplete",
        "selected_predicate_state_unobserved",
    }:
        raise BuyE3DeploymentGateError("warmup did not identify selected artifact state")

    duration = max(2.0, float(paced_duration_s))
    callback_count = max(1, int(math.ceil(target_rate * duration)))
    interval_s = 1.0 / target_rate
    callback_latency_us: list[float] = []
    wall_started = time.perf_counter()
    cpu_started = time.process_time()
    synthetic_ns = warm_decision_ts
    for ordinal in range(callback_count):
        deadline = wall_started + ordinal * interval_s
        remaining = deadline - time.perf_counter()
        if remaining > 0.0005:
            time.sleep(remaining - 0.00025)
        while time.perf_counter() < deadline:
            pass
        synthetic_ns += max(1, int(round(1_000_000_000.0 / target_rate)))
        started_ns = time.perf_counter_ns()
        _observe(policy, synthetic_ns, 2049.0 + ordinal / target_rate)
        callback_latency_us.append((time.perf_counter_ns() - started_ns) / 1_000.0)
    wall_elapsed = max(time.perf_counter() - wall_started, 1e-9)
    cpu_elapsed = max(time.process_time() - cpu_started, 0.0)

    decision_latency_us: list[float] = []
    decision_actions: set[str] = set()
    for ordinal in range(1_000):
        started_ns = time.perf_counter_ns()
        decision = policy.evaluate(
            side="BUY",
            baseline_duration_ms=85_000 * (1 + ordinal % 4),
            campaign_age_s=float(ordinal % 600),
            decision_ts_ns=synthetic_ns,
            snapshot_id=f"host-benchmark-decision-{ordinal}",
        )
        decision_latency_us.append((time.perf_counter_ns() - started_ns) / 1_000.0)
        decision_actions.add(decision.action_id)
        if decision.fallback_reason in {
            "runtime_artifact_binding_invalid",
            "runtime_artifact_file_hash_drift",
        }:
            raise BuyE3DeploymentGateError("artifact binding drifted during benchmark")

    # Exercise short gap, out-of-order, and stale-gap reset semantics directly.
    lifecycle = ReceiveTimeFullMidEmaWindows(warmup_s=2048.0, max_feature_age_s=1.0)
    event = {
        "bids": ((69_999.5, 1.0),),
        "asks": ((70_000.5, 1.0),),
        "market_generation": 1,
        "depth_generation": 1,
    }
    lifecycle.observe_depth(receive_ts_ns=100_000_001, **event)
    lifecycle.observe_depth(receive_ts_ns=400_000_001, **event)
    lifecycle.observe_depth(receive_ts_ns=300_000_001, **event)
    lifecycle.observe_depth(receive_ts_ns=1_600_000_001, **event)
    lifecycle_audit = lifecycle.audit()
    if (
        lifecycle_audit["gap_windows"] != 2
        or lifecycle_audit["out_of_order_updates"] != 1
        or lifecycle_audit["gap_resets"] != 1
    ):
        raise BuyE3DeploymentGateError("gap/out-of-order lifecycle contract drifted")

    # Verify runtime file drift fails closed using exact-byte temporary copies.
    with tempfile.TemporaryDirectory(prefix="f05-buy-e3-drift-") as temporary:
        root = Path(temporary)
        copied = {}
        for label, source in {
            "manifest": artifact_manifest_path,
            "policy": policy_path,
            "bundle": predicate_bundle_path,
        }.items():
            target = root / source.name
            shutil.copyfile(source, target)
            copied[label] = target
        drift_runtime = _load_runtime(
            artifact_manifest_path=copied["manifest"],
            artifact_manifest_file_sha256=artifact_manifest_file_sha256,
            expected_artifact_sha256=expected_artifact_sha256,
            policy_path=copied["policy"],
            policy_file_sha256=policy_file_sha256,
            predicate_bundle_path=copied["bundle"],
            predicate_bundle_file_sha256=predicate_bundle_file_sha256,
        )
        copied["policy"].write_text("{}\n", encoding="ascii")
        drift = drift_runtime.evaluate(
            side="BUY",
            baseline_duration_ms=170_000,
            campaign_age_s=400.0,
            decision_ts_ns=synthetic_ns,
            snapshot_id="host-benchmark-hash-drift",
        )
        if (
            drift.action_id != CONTROL_ACTION
            or drift.fallback_reason != "runtime_artifact_file_hash_drift"
        ):
            raise BuyE3DeploymentGateError("runtime hash drift did not fail closed")

    host_cpu_count = int(os.cpu_count() or 0)
    receipt: dict[str, Any] = {
        "schema_version": BENCHMARK_SCHEMA,
        "identity": OWNER_IDENTITY,
        "status": "exact_artifact_host_benchmark_complete",
        "generated_utc": _utc_now(),
        "artifact_sha256": str(expected_artifact_sha256),
        "artifact_files": {
            "manifest": str(artifact_manifest_file_sha256),
            "policy": str(policy_file_sha256),
            "predicate_bundle": str(predicate_bundle_file_sha256),
        },
        "health_receipt_sha256": health["canonical_health_receipt_sha256"],
        "host": {
            "logical_host": "<current-live-host>",
            "logical_cpu_count": host_cpu_count,
            "max_rss_mib": _max_rss_mib(),
        },
        "callback_benchmark": {
            "observed_live_rate_hz": observed_rate,
            "target_rate_hz": target_rate,
            "target_to_observed_ratio": target_rate / observed_rate,
            "duration_s": wall_elapsed,
            "callback_count": callback_count,
            "achieved_rate_hz": callback_count / wall_elapsed,
            "latency_p50_us": statistics.median(callback_latency_us),
            "latency_p99_us": _percentile(callback_latency_us, 0.99),
            "latency_max_us": max(callback_latency_us),
            "cpu_percent_total_host_scale": (
                cpu_elapsed / wall_elapsed / max(1, host_cpu_count) * 100.0
            ),
        },
        "decision_benchmark": {
            "decision_count": len(decision_latency_us),
            "latency_p50_us": statistics.median(decision_latency_us),
            "latency_p99_us": _percentile(decision_latency_us, 0.99),
            "latency_max_us": max(decision_latency_us),
            "observed_action_ids": sorted(decision_actions),
        },
        "lifecycle_checks": {
            "cold_restart_fell_back_to_b0": True,
            "full_warmup_completed": True,
            "selected_state_identified_after_warmup": True,
            "short_gap_unobserved_windows": lifecycle_audit["gap_windows"],
            "out_of_order_updates_ignored": lifecycle_audit["out_of_order_updates"],
            "stale_gap_resets": lifecycle_audit["gap_resets"],
            "artifact_hash_drift_fell_back_to_b0": True,
        },
        "economic_values_persisted": False,
        "hypothetical_live_actions_scored": False,
        "benchmark_actions_not_logged_as_live": True,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_benchmark_receipt_sha256"] = _document_sha256(
        receipt, "canonical_benchmark_receipt_sha256"
    )
    _atomic_json(output_path, receipt)
    return receipt


def validate_health_receipt(path: Path) -> dict[str, Any]:
    payload = _read_json(path)
    if (
        payload.get("schema_version") != HEALTH_SCHEMA
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("status") != "pre_enable_live_health_window_complete"
        or payload.get("canonical_health_receipt_sha256")
        != _document_sha256(payload, "canonical_health_receipt_sha256")
        or payload.get("economic_values_persisted") is not False
        or payload.get("hypothetical_actions_scored") is not False
    ):
        raise BuyE3DeploymentGateError("host health receipt identity drifted")
    return payload


def validate_benchmark_receipt(path: Path, *, expected_artifact_sha256: str) -> dict[str, Any]:
    payload = _read_json(path)
    if (
        payload.get("schema_version") != BENCHMARK_SCHEMA
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("status") != "exact_artifact_host_benchmark_complete"
        or payload.get("artifact_sha256") != expected_artifact_sha256
        or payload.get("canonical_benchmark_receipt_sha256")
        != _document_sha256(payload, "canonical_benchmark_receipt_sha256")
        or payload.get("economic_values_persisted") is not False
        or payload.get("hypothetical_live_actions_scored") is not False
    ):
        raise BuyE3DeploymentGateError("host benchmark receipt identity drifted")
    return payload


def build_deployment_gate_receipt(
    *,
    health_receipt_path: Path,
    benchmark_receipt_path: Path,
    expected_artifact_sha256: str,
    expected_execution_commit: str,
    expected_execution_tag: str,
    output_path: Path,
) -> dict[str, Any]:
    health = validate_health_receipt(health_receipt_path)
    benchmark = validate_benchmark_receipt(
        benchmark_receipt_path,
        expected_artifact_sha256=expected_artifact_sha256,
    )
    if benchmark.get("health_receipt_sha256") != health.get("canonical_health_receipt_sha256"):
        raise BuyE3DeploymentGateError("benchmark was not bound to host health window")
    callback = benchmark["callback_benchmark"]
    decision = benchmark["decision_benchmark"]
    host = health["host"]
    process = health["process"]
    runtime = health["runtime"]
    repository = health["repository"]
    checks = {
        "exact_2_vcpu_host": int(host["logical_cpu_count"]) == 2,
        "host_memory_class_2gib": 1_800.0 <= float(host["mem_total_mib"]) <= 2_300.0,
        "host_available_memory_at_least_512mib": float(host["mem_available_mib"]) >= 512.0,
        "single_live_process": int(process["process_count"]) == 1,
        "live_process_rss_at_most_512mib": float(process["rss_mib"]) <= 512.0,
        "live_process_cpu_at_most_one_core": float(process["cpu_percent_one_core_scale"]) <= 100.0,
        "repository_clean": repository.get("worktree_clean") is True,
        "execution_commit_exact": repository.get("commit") == expected_execution_commit,
        "execution_tag_present": expected_execution_tag
        in repository.get("annotated_tags_at_head", []),
        "buy_e3_disabled_during_gate": runtime.get("buy_e3_enabled") is False,
        "sell_owner_remained_enabled": runtime.get("sell_owner_enabled") is True,
        "actual_callback_rate_observed": float(runtime["actual_execution_book_callback_rate_hz"])
        > 0.0,
        "zero_queue_drop_and_invalid_deltas": all(
            int(value) == 0 for value in runtime["counter_deltas"].values()
        ),
        "zero_fatal_log_patterns": all(
            int(value) == 0 for value in runtime["fatal_pattern_counts"].values()
        ),
        "benchmark_at_least_2x_actual_rate": float(callback["target_to_observed_ratio"]) >= 2.0,
        "benchmark_achieved_target_rate": float(callback["achieved_rate_hz"])
        >= 0.95 * float(callback["target_rate_hz"]),
        "callback_p99_at_most_2ms": float(callback["latency_p99_us"]) <= 2_000.0,
        "decision_p99_at_most_10ms": float(decision["latency_p99_us"]) <= 10_000.0,
        "benchmark_cpu_at_most_one_core": float(callback["cpu_percent_total_host_scale"]) <= 50.0,
        "benchmark_rss_at_most_256mib": float(benchmark["host"]["max_rss_mib"]) <= 256.0,
        "restart_gap_hash_fallback_checks_passed": all(
            (
                benchmark["lifecycle_checks"]["cold_restart_fell_back_to_b0"],
                benchmark["lifecycle_checks"]["full_warmup_completed"],
                benchmark["lifecycle_checks"]["selected_state_identified_after_warmup"],
                benchmark["lifecycle_checks"]["artifact_hash_drift_fell_back_to_b0"],
                benchmark["lifecycle_checks"]["short_gap_unobserved_windows"] == 2,
                benchmark["lifecycle_checks"]["out_of_order_updates_ignored"] == 1,
                benchmark["lifecycle_checks"]["stale_gap_resets"] == 1,
            )
        ),
    }
    passed = all(checks.values())
    receipt: dict[str, Any] = {
        "schema_version": GATE_SCHEMA,
        "identity": OWNER_IDENTITY,
        "status": "deployment_gate_passed" if passed else "deployment_gate_failed",
        "generated_utc": _utc_now(),
        "artifact_sha256": expected_artifact_sha256,
        "execution_commit": expected_execution_commit,
        "execution_tag": expected_execution_tag,
        "health_receipt_sha256": health["canonical_health_receipt_sha256"],
        "benchmark_receipt_sha256": benchmark["canonical_benchmark_receipt_sha256"],
        "checks": checks,
        "activation_allowed": passed,
        "economic_values_persisted": False,
        "hypothetical_live_actions_scored": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    receipt["canonical_deployment_gate_receipt_sha256"] = _document_sha256(
        receipt, "canonical_deployment_gate_receipt_sha256"
    )
    _atomic_json(output_path, receipt)
    if not passed:
        failed = sorted(name for name, value in checks.items() if not value)
        raise BuyE3DeploymentGateError(f"deployment gate failed: {failed}")
    return receipt


def validate_deployment_gate_receipt(
    path: Path, *, expected_artifact_sha256: str
) -> dict[str, Any]:
    payload = _read_json(path)
    if (
        payload.get("schema_version") != GATE_SCHEMA
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("status") != "deployment_gate_passed"
        or payload.get("activation_allowed") is not True
        or payload.get("artifact_sha256") != expected_artifact_sha256
        or not isinstance(payload.get("checks"), dict)
        or not all(payload["checks"].values())
        or payload.get("canonical_deployment_gate_receipt_sha256")
        != _document_sha256(payload, "canonical_deployment_gate_receipt_sha256")
    ):
        raise BuyE3DeploymentGateError("deployment gate receipt identity drifted")
    return payload


def run_runtime_regression_tests(
    *,
    repository_root: Path,
    expected_artifact_sha256: str,
    expected_execution_commit: str,
    expected_execution_tag: str,
    output_path: Path,
    python_executable: Path | None = None,
) -> dict[str, Any]:
    """Run the fixed integration suite and preserve a hash-bound receipt."""

    root = repository_root.expanduser().resolve()
    git = _git_identity(root)
    if (
        git["commit"] != expected_execution_commit
        or expected_execution_tag not in git["annotated_tags_at_head"]
        or git["worktree_clean"] is not True
    ):
        raise BuyE3DeploymentGateError("runtime regression checkout is not frozen")
    executable = (
        python_executable.expanduser().resolve()
        if python_executable is not None
        else (root / ".venv/bin/python").resolve()
    )
    command = [
        str(executable),
        "-m",
        "pytest",
        "-q",
        *RUNTIME_REGRESSION_TESTS,
    ]
    completed = subprocess.run(
        command,
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    combined = f"{completed.stdout}\n{completed.stderr}"
    matches = re.findall(r"(\d+) passed", combined)
    passed_count = int(matches[-1]) if matches else 0
    failed_matches = re.findall(r"(\d+) failed", combined)
    failed_count = int(failed_matches[-1]) if failed_matches else 0
    status = (
        "passed"
        if completed.returncode == 0 and passed_count > 0 and failed_count == 0
        else "failed"
    )
    receipt: dict[str, Any] = {
        "schema_version": REGRESSION_SCHEMA,
        "identity": OWNER_IDENTITY,
        "status": status,
        "generated_utc": _utc_now(),
        "artifact_sha256": expected_artifact_sha256,
        "execution_commit": expected_execution_commit,
        "execution_tag": expected_execution_tag,
        "passed": passed_count,
        "failed": failed_count,
        "return_code": completed.returncode,
        "test_files": {path: _file_sha256(root / path) for path in RUNTIME_REGRESSION_TESTS},
        "runtime_sources": {path: _file_sha256(root / path) for path in RUNTIME_REGRESSION_SOURCES},
        "coverage": {
            "buy_disabled_equals_b0": True,
            "exposure_increasing_buy_only": True,
            "reducing_buy_unchanged": True,
            "fixed_action_total_cooldown": True,
            "b0_consecutive_units": True,
            "partial_fill_and_consecutive_units": True,
            "restart_and_rollback": True,
            "gap_and_out_of_order": True,
            "stale_unobserved_hash_drift_fallback": True,
            "sell_integration_unchanged": True,
        },
        "economic_values_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_receipt_sha256"] = _document_sha256(receipt, "canonical_receipt_sha256")
    _atomic_json(output_path, receipt)
    if status != "passed":
        raise BuyE3DeploymentGateError("runtime regression test suite failed")
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    health = subparsers.add_parser("capture-health")
    health.add_argument("--repository-root", type=Path, required=True)
    health.add_argument("--log", type=Path, required=True)
    health.add_argument("--pid-file", type=Path, required=True)
    health.add_argument("--live-config", type=Path, required=True)
    health.add_argument("--expected-buy-e3-enabled", type=int, choices=(0, 1), default=0)
    health.add_argument("--output", type=Path, required=True)
    health.add_argument("--timeout-s", type=float, default=150.0)

    benchmark = subparsers.add_parser("benchmark")
    benchmark.add_argument("--artifact-manifest", type=Path, required=True)
    benchmark.add_argument("--artifact-manifest-file-sha256", required=True)
    benchmark.add_argument("--artifact-sha256", required=True)
    benchmark.add_argument("--policy", type=Path, required=True)
    benchmark.add_argument("--policy-file-sha256", required=True)
    benchmark.add_argument("--predicate-bundle", type=Path, required=True)
    benchmark.add_argument("--predicate-bundle-file-sha256", required=True)
    benchmark.add_argument("--health-receipt", type=Path, required=True)
    benchmark.add_argument("--output", type=Path, required=True)
    benchmark.add_argument("--paced-duration-s", type=float, default=15.0)

    gate = subparsers.add_parser("finalize")
    gate.add_argument("--health-receipt", type=Path, required=True)
    gate.add_argument("--benchmark-receipt", type=Path, required=True)
    gate.add_argument("--artifact-sha256", required=True)
    gate.add_argument("--execution-commit", required=True)
    gate.add_argument("--execution-tag", required=True)
    gate.add_argument("--output", type=Path, required=True)

    regression = subparsers.add_parser("regression")
    regression.add_argument("--repository-root", type=Path, required=True)
    regression.add_argument("--artifact-sha256", required=True)
    regression.add_argument("--execution-commit", required=True)
    regression.add_argument("--execution-tag", required=True)
    regression.add_argument("--output", type=Path, required=True)
    regression.add_argument("--python", type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.command == "capture-health":
        payload = capture_live_health_window(
            repository_root=args.repository_root,
            log_path=args.log,
            pid_file=args.pid_file,
            live_config_path=args.live_config,
            expected_buy_e3_enabled=bool(args.expected_buy_e3_enabled),
            output_path=args.output,
            timeout_s=args.timeout_s,
        )
        print(json.dumps({"status": payload["status"]}, sort_keys=True))
        return 0
    if args.command == "benchmark":
        payload = run_host_benchmark(
            artifact_manifest_path=args.artifact_manifest,
            artifact_manifest_file_sha256=args.artifact_manifest_file_sha256,
            expected_artifact_sha256=args.artifact_sha256,
            policy_path=args.policy,
            policy_file_sha256=args.policy_file_sha256,
            predicate_bundle_path=args.predicate_bundle,
            predicate_bundle_file_sha256=args.predicate_bundle_file_sha256,
            health_receipt_path=args.health_receipt,
            output_path=args.output,
            paced_duration_s=args.paced_duration_s,
        )
        print(json.dumps({"status": payload["status"]}, sort_keys=True))
        return 0
    if args.command == "regression":
        payload = run_runtime_regression_tests(
            repository_root=args.repository_root,
            expected_artifact_sha256=args.artifact_sha256,
            expected_execution_commit=args.execution_commit,
            expected_execution_tag=args.execution_tag,
            output_path=args.output,
            python_executable=args.python,
        )
        print(json.dumps({"status": payload["status"]}, sort_keys=True))
        return 0
    payload = build_deployment_gate_receipt(
        health_receipt_path=args.health_receipt,
        benchmark_receipt_path=args.benchmark_receipt,
        expected_artifact_sha256=args.artifact_sha256,
        expected_execution_commit=args.execution_commit,
        expected_execution_tag=args.execution_tag,
        output_path=args.output,
    )
    print(json.dumps({"status": payload["status"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BENCHMARK_SCHEMA",
    "BuyE3DeploymentGateError",
    "GATE_SCHEMA",
    "HEALTH_SCHEMA",
    "REGRESSION_SCHEMA",
    "build_deployment_gate_receipt",
    "capture_live_health_window",
    "run_host_benchmark",
    "run_runtime_regression_tests",
    "validate_benchmark_receipt",
    "validate_deployment_gate_receipt",
    "validate_health_receipt",
]
