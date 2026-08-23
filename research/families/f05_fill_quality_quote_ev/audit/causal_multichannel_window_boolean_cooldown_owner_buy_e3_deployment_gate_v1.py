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
import stat
import statistics
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable, Mapping, Sequence
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
COMPATIBLE_REGRESSION_SCHEMA = f"{OWNER_IDENTITY}.compatible_runtime_regression_test_receipt.v2"
CONCURRENT_RESOURCE_SCHEMA = f"{OWNER_IDENTITY}.compatible_concurrent_resource_window.v2"
SELL_PARITY_SCHEMA = f"{OWNER_IDENTITY}.parity_receipt.v1"
CONCURRENT_CAPTURE_AUTHORITY = "direct_linux_proc_live_health_exact_benchmark_v1"

RUNTIME_REGRESSION_TESTS = (
    "tests/test_boolean_cooldown_buy_e3.py",
    "tests/test_live_fill_cooldown_policy.py",
    "tests/test_fill_cooldown_contract.py",
    "tests/test_fill_cooldown_checkpoint.py",
    "tests/test_live_runtime_policy.py",
    "tests/test_causal_multichannel_window_boolean_cooldown_owner_buy_e3_refit_v1.py",
    "tests/test_causal_multichannel_window_boolean_cooldown_owner_buy_e3_closeout_v1.py",
    "tests/test_f05_cpp_real_day_lockstep_v22.py",
    "tests/test_causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_sell_only_v1.py",
    "tests/test_causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_amendment_v2.py",
    "tests/test_deploy_f05_buy_e3_owner_v1.py",
    "tests/test_f05_buy_e3_execution_attempt.py",
    "tests/test_f05_buy_e3_stability_receipts.py",
    "tests/test_f05_buy_e3_durability_gate.py",
    "tests/test_live_buy_e3_startup_attestation.py",
    "tests/test_live_run_authority_environment.py",
    "tests/test_causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_v1.py",
    "tests/test_causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_amendment_v2.py",
    "tests/test_f05_buy_e3_active_release.py",
    "tests/test_live_buy_e3_active_release_runtime.py",
    "tests/test_prospective_baseline_epoch.py",
    "tests/test_private_evidence_governance.py",
)
RUNTIME_REGRESSION_SOURCES = (
    "pyproject.toml",
    "strategy/boolean_cooldown_buy_e3.py",
    "strategy/maker_engine.py",
    "live/config.py",
    "live/main.py",
    "live/run.sh",
    "live/runtime_policy.py",
    "models/replay/prospective_baseline_epoch.py",
    "scripts/audit_private_evidence.py",
    "scripts/deploy_f05_buy_e3_owner_v1.py",
    "scripts/f05_buy_e3_active_release.py",
    "scripts/f05_buy_e3_execution_attempt.py",
    "scripts/f05_buy_e3_final_composition_contract.py",
    "scripts/f05_buy_e3_stability_receipts.py",
    "scripts/f05_buy_e3_durability_gate.py",
    "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_v1.py",
    "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_v1.py",
    "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_owner_buy_e3_final_composition_amendment_v2.py",
    "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_v1.py",
)

SELL_54_CASE_SOURCE_PATHS = (
    "strategy/boolean_cooldown_live.py",
    "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1.py",
    "research/families/f05_fill_quality_quote_ev/audit/causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_v1.py",
)

MIN_MEM_AVAILABLE_MIB = 512.0
MAX_LIVE_RSS_MIB = 512.0
MAX_BENCHMARK_RSS_MIB = 256.0
MAX_COMBINED_RSS_MIB = 768.0
MIN_RATE_MULTIPLIER = 2.0
MAX_CALLBACK_P99_US = 2_000.0
MAX_DECISION_P99_US = 10_000.0
MAX_IMMUTABLE_JSON_BYTES = 64 << 20
RESOURCE_CHECK_NAMES = (
    "exact_2_vcpu_host",
    "host_memory_class_2gib",
    "direct_collector_authority",
    "benchmark_process_overlap_proven",
    "same_live_pid_start_identity",
    "post_health_after_benchmark_exit",
    "concurrent_live_and_benchmark_observed",
    "min_mem_available_at_least_512mib",
    "live_rss_at_most_512mib",
    "benchmark_rss_at_most_256mib",
    "combined_rss_at_most_768mib",
    "no_oom_events",
    "no_swap_activity",
    "zero_drop_invalid_overflow_delta",
    "deep_book_buffer_zero",
    "true_2x_observed_rate",
    "callback_p99_at_most_2ms",
    "decision_p99_at_most_10ms",
    "post_benchmark_same_pid_health",
)
RESOURCE_SAMPLE_FIELDS = (
    "monotonic_ns",
    "live_pid",
    "live_pid_start_ticks",
    "benchmark_pid",
    "benchmark_pid_start_ticks",
    "benchmark_running",
    "health_generation",
    "health_line_sha256",
    "mem_available_mib",
    "live_rss_mib",
    "benchmark_rss_mib",
    "deep_book_buffer",
    "oom_events",
    "swap_in_kib",
    "swap_out_kib",
    "counter_values",
)
RESOURCE_CAPTURE_FIELDS = (
    "authority",
    "collector_pid",
    "benchmark_command_sha256",
    "benchmark_pid",
    "benchmark_pid_start_ticks",
    "live_pid",
    "live_pid_start_ticks",
    "benchmark_launch_monotonic_ns",
    "first_overlap_sample_monotonic_ns",
    "last_overlap_sample_monotonic_ns",
    "benchmark_exit_monotonic_ns",
    "post_health_observed_monotonic_ns",
    "pre_health_generation",
    "post_health_generation",
    "pre_health_line_sha256",
    "post_health_line_sha256",
    "pre_counter_values",
    "post_counter_values",
    "pre_deep_book_buffer",
    "post_deep_book_buffer",
    "benchmark_returncode",
    "benchmark_stdout_sha256",
    "benchmark_stderr_sha256",
    "post_health_after_benchmark_exit",
)
REQUIRED_ZERO_COUNTERS = (
    "marketTapeDropped",
    "marketTapeInvalid",
    "externalRecordDropped",
    "globalFlowTradeOverflow",
    "globalFlowBookOverflow",
    "booleanCooldownInvalid",
    "buyE3CooldownInvalid",
)
DEPLOYMENT_GATE_CHECK_NAMES = (
    "exact_2_vcpu_host",
    "host_memory_class_2gib",
    "host_available_memory_at_least_512mib",
    "single_live_process",
    "live_process_rss_at_most_512mib",
    "live_process_cpu_at_most_one_core",
    "repository_clean",
    "execution_commit_exact",
    "execution_tag_present",
    "buy_e3_disabled_during_gate",
    "sell_owner_remained_enabled",
    "actual_callback_rate_observed",
    "zero_queue_drop_and_invalid_deltas",
    "zero_fatal_log_patterns",
    "benchmark_at_least_2x_actual_rate",
    "benchmark_achieved_target_rate",
    "callback_p99_at_most_2ms",
    "decision_p99_at_most_10ms",
    "benchmark_cpu_at_most_one_core",
    "benchmark_rss_at_most_256mib",
    "restart_gap_hash_fallback_checks_passed",
)
EVIDENCE_BOUNDARY = {
    "economic_values_persisted": False,
    "hypothetical_live_actions_scored": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "action_authorized": False,
    "live_authorized": False,
}

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


def _immutable_json(path: Path, payload: Mapping[str, Any]) -> str:
    """Write a receipt once; a used identity is never replaced or recycled."""

    target = path.expanduser().absolute()
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if target.parent.is_symlink() or not target.parent.is_dir():
        raise BuyE3DeploymentGateError("receipt parent is not a stable directory")
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    directory_fd = os.open(target.parent, directory_flags)
    descriptor: int | None = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(target.name, flags, 0o600, dir_fd=directory_fd)
        except FileExistsError as exc:
            raise BuyE3DeploymentGateError(
                f"immutable receipt already exists: {target.name}"
            ) from exc
        encoded = (
            json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        ).encode("ascii")
        offset = 0
        while offset < len(encoded):
            written = os.write(descriptor, encoded[offset:])
            if written <= 0:
                raise OSError("receipt write made no progress")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        os.fsync(directory_fd)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory_fd)
    return _file_sha256(target)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.expanduser().resolve().read_text(encoding="ascii"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BuyE3DeploymentGateError(f"receipt unreadable: {path.name}") from exc
    if not isinstance(payload, dict):
        raise BuyE3DeploymentGateError(f"receipt root malformed: {path.name}")
    return payload


def _stat_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _open_parent_nofollow(path: Path) -> tuple[Path, int]:
    target = Path(os.path.abspath(os.fspath(path.expanduser())))
    parts = target.parts
    if not target.is_absolute() or len(parts) < 2 or parts[-1] in {"", ".", ".."}:
        raise BuyE3DeploymentGateError("receipt path is malformed")
    if not hasattr(os, "O_DIRECTORY") or not hasattr(os, "O_NOFOLLOW"):
        raise BuyE3DeploymentGateError("secure no-follow receipt reads are unsupported")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    directory_flags |= getattr(os, "O_CLOEXEC", 0)
    directory_fd: int | None = None
    try:
        directory_fd = os.open(target.anchor, directory_flags)
        for component in parts[1:-1]:
            next_fd = os.open(component, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
    except OSError as exc:
        if directory_fd is not None:
            os.close(directory_fd)
        raise BuyE3DeploymentGateError(
            f"receipt parent is unavailable or contains a symlink: {target.name}"
        ) from exc
    if directory_fd is None:
        raise BuyE3DeploymentGateError(f"receipt parent is unavailable: {target.name}")
    return target, directory_fd


def _read_immutable_json_record(
    path: Path,
) -> tuple[dict[str, Any], Path, str, tuple[int, ...]]:
    target, directory_fd = _open_parent_nofollow(path)
    descriptor: int | None = None
    try:
        if not hasattr(os, "O_NOFOLLOW"):
            raise BuyE3DeploymentGateError("secure no-follow receipt reads are unsupported")
        flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        descriptor = os.open(target.name, flags, dir_fd=directory_fd)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or before.st_uid != os.getuid()
            or before.st_size <= 0
            or before.st_size > MAX_IMMUTABLE_JSON_BYTES
        ):
            raise BuyE3DeploymentGateError(
                f"receipt must be an owner-held single-link 0600 bounded file: {target.name}"
            )
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(1 << 20, MAX_IMMUTABLE_JSON_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_IMMUTABLE_JSON_BYTES:
                raise BuyE3DeploymentGateError(f"receipt is too large: {target.name}")
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        try:
            directory_entry = os.stat(
                target.name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise BuyE3DeploymentGateError(
                f"receipt path changed while it was read: {target.name}"
            ) from exc
        identity = _stat_identity(before)
        if (
            len(raw) != before.st_size
            or _stat_identity(after) != identity
            or directory_entry.st_dev != before.st_dev
            or directory_entry.st_ino != before.st_ino
        ):
            raise BuyE3DeploymentGateError(
                f"receipt path or bytes changed while it was read: {target.name}"
            )
        try:
            payload = json.loads(raw.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BuyE3DeploymentGateError(f"receipt unreadable: {target.name}") from exc
        if not isinstance(payload, dict):
            raise BuyE3DeploymentGateError(f"receipt root malformed: {target.name}")
        return payload, target, hashlib.sha256(raw).hexdigest(), identity
    except OSError as exc:
        raise BuyE3DeploymentGateError(f"receipt unavailable: {target.name}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory_fd)


def _read_immutable_json(path: Path) -> tuple[dict[str, Any], Path]:
    payload, target, _raw_sha256, _identity = _read_immutable_json_record(path)
    return payload, target


def _strict_finite(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BuyE3DeploymentGateError(f"{label} is not a strict numeric value")
    number = float(value)
    if not math.isfinite(number):
        raise BuyE3DeploymentGateError(f"{label} is not finite")
    return number


def _strict_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise BuyE3DeploymentGateError(f"{label} is not a positive integer")
    return value


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value)
    if _SHA_RE.fullmatch(normalized) is None:
        raise BuyE3DeploymentGateError(f"{label} is not SHA-256")
    return normalized


def _counter_map(sample: Mapping[str, Any]) -> dict[str, int]:
    counters = sample.get("counter_values")
    if not isinstance(counters, Mapping) or set(counters) != set(REQUIRED_ZERO_COUNTERS):
        raise BuyE3DeploymentGateError("resource sample counters are incomplete")
    normalized: dict[str, int] = {}
    for key in REQUIRED_ZERO_COUNTERS:
        value = counters[key]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise BuyE3DeploymentGateError(f"resource counter is malformed: {key}")
        normalized[key] = value
    return normalized


def _exact_mapping(raw: Any, fields: Sequence[str], label: str) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != set(fields):
        raise BuyE3DeploymentGateError(f"{label} fields drifted")
    return dict(raw)


def _pytest_nodeids(output: str) -> tuple[str, ...]:
    nodeids = tuple(
        sorted(
            {
                line.strip()
                for line in output.splitlines()
                if "::" in line and not line.lstrip().startswith(("<", "="))
            }
        )
    )
    if not nodeids:
        raise BuyE3DeploymentGateError("runtime regression collection produced no nodeids")
    return nodeids


def _nodeid_source_counts(nodeids: Sequence[str]) -> dict[str, int]:
    counts = {path: 0 for path in RUNTIME_REGRESSION_TESTS}
    for nodeid in nodeids:
        source = nodeid.split("::", 1)[0]
        if source not in counts:
            raise BuyE3DeploymentGateError(
                f"runtime regression collected an unexpected test source: {source}"
            )
        counts[source] += 1
    missing = sorted(path for path, count in counts.items() if count == 0)
    if missing:
        raise BuyE3DeploymentGateError(f"runtime regression collected no nodeids from: {missing}")
    return counts


def _lexical_python_executable(path: Path) -> Path:
    """Keep the venv entrypoint path so Python activates that environment."""

    executable = Path(os.path.abspath(os.fspath(path.expanduser())))
    if not executable.is_file():
        raise BuyE3DeploymentGateError("runtime regression Python is unavailable")
    return executable


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


def _proc_start_ticks(pid: int) -> int:
    try:
        raw = Path(f"/proc/{int(pid)}/stat").read_text(encoding="ascii")
    except OSError as exc:
        raise BuyE3DeploymentGateError("process identity disappeared") from exc
    close = raw.rfind(")")
    fields = raw[close + 2 :].split() if close >= 0 else []
    if len(fields) <= 19:
        raise BuyE3DeploymentGateError("process start identity is malformed")
    return _strict_positive_int(int(fields[19]), "process start ticks")


def _proc_snapshot(pid: int) -> dict[str, Any]:
    process_id = _strict_positive_int(pid, "process PID")
    try:
        command_bytes = Path(f"/proc/{process_id}/cmdline").read_bytes()
    except OSError as exc:
        raise BuyE3DeploymentGateError("process command line disappeared") from exc
    command = tuple(
        value.decode("utf-8", errors="replace") for value in command_bytes.split(b"\x00") if value
    )
    if not command:
        raise BuyE3DeploymentGateError("process command line is empty")
    return {
        "pid": process_id,
        "pid_start_ticks": _proc_start_ticks(process_id),
        "cmdline": list(command),
        "cmdline_sha256": _canonical_sha256(list(command)),
    }


def _vmstat_counters() -> dict[str, int]:
    path = Path("/proc/vmstat")
    if not path.is_file():
        raise BuyE3DeploymentGateError("resource capture requires Linux /proc/vmstat")
    values: dict[str, int] = {}
    for line in path.read_text(encoding="ascii").splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0] in {"oom_kill", "pswpin", "pswpout"}:
            values[fields[0]] = int(fields[1])
    missing = {"oom_kill", "pswpin", "pswpout"} - set(values)
    if missing:
        raise BuyE3DeploymentGateError(f"host vmstat lacks counters: {sorted(missing)}")
    return values


def _health_resource_state(line: str, *, generation: int) -> dict[str, Any]:
    wall_timestamp, parsed = _parse_health_line(line)
    missing = {*REQUIRED_ZERO_COUNTERS, "deepBookBuffer"} - set(parsed)
    if missing:
        raise BuyE3DeploymentGateError(f"live health line lacks resource fields: {sorted(missing)}")
    if int(parsed["buyE3CooldownEnabled"]) != 0:
        raise BuyE3DeploymentGateError("BUY E3 was not disabled during resource capture")
    if int(parsed["booleanCooldownEnabled"]) != 1:
        raise BuyE3DeploymentGateError("SELL owner policy was not enabled during capture")
    counters = _counter_map(
        {"counter_values": {key: int(parsed[key]) for key in REQUIRED_ZERO_COUNTERS}}
    )
    deep_buffer = parsed.get("deepBookBuffer")
    if isinstance(deep_buffer, bool) or not isinstance(deep_buffer, int):
        raise BuyE3DeploymentGateError("health deep-book buffer is malformed")
    return {
        "generation": generation,
        "wall_timestamp": wall_timestamp,
        "line_sha256": hashlib.sha256(line.encode("utf-8")).hexdigest(),
        "counter_values": counters,
        "deep_book_buffer": deep_buffer,
    }


class _LiveHealthTail:
    """Read only HEALTH lines and retain no operational log payload text."""

    def __init__(self, path: Path, *, initial_tail_bytes: int = 4 << 20) -> None:
        self.path = path.expanduser().resolve(strict=True)
        self.offset = 0
        self.pending = b""
        self.generation = 0
        self.latest: dict[str, Any] | None = None
        size = self.path.stat().st_size
        start = max(0, size - int(initial_tail_bytes))
        with self.path.open("rb") as handle:
            handle.seek(start)
            payload = handle.read()
            self.offset = handle.tell()
        if start > 0:
            _discarded, separator, payload = payload.partition(b"\n")
            if not separator:
                payload = b""
        self._consume(payload)
        if self.latest is None:
            raise BuyE3DeploymentGateError("live log has no parseable HEALTH state")

    def _consume(self, payload: bytes) -> None:
        combined = self.pending + payload
        lines = combined.splitlines(keepends=True)
        self.pending = b""
        if lines and not lines[-1].endswith((b"\n", b"\r")):
            self.pending = lines.pop()
        for raw in lines:
            if b"[main] INFO HEALTH " not in raw:
                continue
            line = raw.rstrip(b"\r\n").decode("utf-8", errors="strict")
            self.generation += 1
            self.latest = _health_resource_state(line, generation=self.generation)

    def snapshot(self) -> dict[str, Any]:
        size = self.path.stat().st_size
        if size < self.offset:
            raise BuyE3DeploymentGateError("live log rotated during resource capture")
        if size > self.offset:
            with self.path.open("rb") as handle:
                handle.seek(self.offset)
                payload = handle.read()
                self.offset = handle.tell()
            self._consume(payload)
        if self.latest is None:
            raise BuyE3DeploymentGateError("live HEALTH state disappeared")
        return dict(self.latest)


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
        "worktree_clean": run("status", "--porcelain", "--untracked-files=all") == "",
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
            target.chmod(0o600)
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
    _immutable_json(output_path, receipt)
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
        **EVIDENCE_BOUNDARY,
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
    payload, _target = _read_immutable_json(path)
    expected_fields = {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "artifact_sha256",
        "execution_commit",
        "execution_tag",
        "health_receipt_sha256",
        "benchmark_receipt_sha256",
        "checks",
        "activation_allowed",
        *EVIDENCE_BOUNDARY,
        "canonical_deployment_gate_receipt_sha256",
    }
    checks = payload.get("checks")
    if (
        set(payload) != expected_fields
        or payload.get("schema_version") != GATE_SCHEMA
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("status") != "deployment_gate_passed"
        or payload.get("activation_allowed") is not True
        or payload.get("artifact_sha256") != expected_artifact_sha256
        or not isinstance(checks, dict)
        or set(checks) != set(DEPLOYMENT_GATE_CHECK_NAMES)
        or any(checks[name] is not True for name in DEPLOYMENT_GATE_CHECK_NAMES)
        or any(payload.get(field) is not value for field, value in EVIDENCE_BOUNDARY.items())
        or payload.get("canonical_deployment_gate_receipt_sha256")
        != _document_sha256(payload, "canonical_deployment_gate_receipt_sha256")
    ):
        raise BuyE3DeploymentGateError("deployment gate receipt identity drifted")
    return payload


def _resource_sample(
    raw: Mapping[str, Any],
    *,
    expected_live_pid: int,
    expected_live_start_ticks: int,
    expected_benchmark_pid: int,
    expected_benchmark_start_ticks: int,
) -> dict[str, Any]:
    sample = _exact_mapping(raw, RESOURCE_SAMPLE_FIELDS, "resource sample")
    for field, expected in (
        ("live_pid", expected_live_pid),
        ("live_pid_start_ticks", expected_live_start_ticks),
        ("benchmark_pid", expected_benchmark_pid),
        ("benchmark_pid_start_ticks", expected_benchmark_start_ticks),
    ):
        if _strict_positive_int(sample.get(field), field) != expected:
            raise BuyE3DeploymentGateError(f"resource sample identity drifted: {field}")
    if sample.get("benchmark_running") is not True:
        raise BuyE3DeploymentGateError("resource sample was not concurrent with benchmark")
    for field in ("monotonic_ns", "health_generation"):
        _strict_positive_int(sample.get(field), field)
    _require_sha256(sample.get("health_line_sha256"), "resource health line hash")
    for field in (
        "mem_available_mib",
        "live_rss_mib",
        "benchmark_rss_mib",
        "deep_book_buffer",
        "oom_events",
        "swap_in_kib",
        "swap_out_kib",
    ):
        number = _strict_finite(sample.get(field), field)
        if number < 0.0:
            raise BuyE3DeploymentGateError(f"resource sample is negative: {field}")
    if type(sample.get("deep_book_buffer")) is not int:
        raise BuyE3DeploymentGateError("resource deep-book buffer is not an integer")
    for field in ("oom_events", "swap_in_kib", "swap_out_kib"):
        if type(sample.get(field)) is not int:
            raise BuyE3DeploymentGateError(f"resource sample is not an integer: {field}")
    sample["counter_values"] = _counter_map(sample)
    return sample


def _resource_capture(
    raw: Mapping[str, Any],
    *,
    samples: Sequence[Mapping[str, Any]],
    expected_live_pid: int,
    expected_live_start_ticks: int,
) -> dict[str, Any]:
    capture = _exact_mapping(raw, RESOURCE_CAPTURE_FIELDS, "resource capture")
    if capture.get("authority") != CONCURRENT_CAPTURE_AUTHORITY:
        raise BuyE3DeploymentGateError("resource capture authority is not direct")
    for field in (
        "collector_pid",
        "benchmark_pid",
        "benchmark_pid_start_ticks",
        "live_pid",
        "live_pid_start_ticks",
        "benchmark_launch_monotonic_ns",
        "first_overlap_sample_monotonic_ns",
        "last_overlap_sample_monotonic_ns",
        "benchmark_exit_monotonic_ns",
        "post_health_observed_monotonic_ns",
        "pre_health_generation",
        "post_health_generation",
    ):
        _strict_positive_int(capture.get(field), field)
    if (
        capture["live_pid"] != expected_live_pid
        or capture["live_pid_start_ticks"] != expected_live_start_ticks
        or capture["benchmark_pid"] == expected_live_pid
    ):
        raise BuyE3DeploymentGateError("resource capture process identity drifted")
    for field in (
        "benchmark_command_sha256",
        "pre_health_line_sha256",
        "post_health_line_sha256",
        "benchmark_stdout_sha256",
        "benchmark_stderr_sha256",
    ):
        _require_sha256(capture.get(field), field)
    if (
        type(capture.get("benchmark_returncode")) is not int
        or capture["benchmark_returncode"] != 0
        or capture.get("post_health_after_benchmark_exit") is not True
    ):
        raise BuyE3DeploymentGateError("resource benchmark did not complete concurrently")
    pre_counters = _counter_map({"counter_values": capture.get("pre_counter_values")})
    post_counters = _counter_map({"counter_values": capture.get("post_counter_values")})
    if pre_counters != post_counters:
        raise BuyE3DeploymentGateError("resource health counters advanced across benchmark")
    for field in ("pre_deep_book_buffer", "post_deep_book_buffer"):
        if type(capture.get(field)) is not int or capture[field] != 0:
            raise BuyE3DeploymentGateError("resource deep-book buffer was not zero")
    if capture["post_health_generation"] <= capture["pre_health_generation"]:
        raise BuyE3DeploymentGateError("resource post-health state was not fresh")
    times = [int(sample["monotonic_ns"]) for sample in samples]
    if (
        len(times) < 2
        or times != sorted(set(times))
        or capture["benchmark_launch_monotonic_ns"] > times[0]
        or capture["first_overlap_sample_monotonic_ns"] != times[0]
        or capture["last_overlap_sample_monotonic_ns"] != times[-1]
        or times[-1] >= capture["benchmark_exit_monotonic_ns"]
        or capture["post_health_observed_monotonic_ns"] < capture["benchmark_exit_monotonic_ns"]
    ):
        raise BuyE3DeploymentGateError("resource benchmark overlap chronology drifted")
    for sample in samples:
        if (
            sample["counter_values"] != pre_counters
            or sample["deep_book_buffer"] != 0
            or not capture["pre_health_generation"]
            <= sample["health_generation"]
            <= capture["post_health_generation"]
        ):
            raise BuyE3DeploymentGateError("resource sample health state drifted")
    return capture


def build_concurrent_resource_receipt(
    *,
    samples: Sequence[Mapping[str, Any]],
    capture_provenance: Mapping[str, Any],
    benchmark_receipt: Mapping[str, Any],
    pre_process_identity: Mapping[str, Any],
    post_process_identity: Mapping[str, Any],
    logical_cpu_count: int,
    mem_total_mib: int | float,
    expected_artifact_sha256: str,
    expected_execution_commit: str,
    expected_execution_tag: str,
    output_path: Path,
) -> dict[str, Any]:
    """Bind a real simultaneous disabled-live plus benchmark resource window."""

    if len(samples) < 2:
        raise BuyE3DeploymentGateError("concurrent resource window is too short")
    if isinstance(logical_cpu_count, bool) or not isinstance(logical_cpu_count, int):
        raise BuyE3DeploymentGateError("logical CPU count is malformed")
    total_memory = _strict_finite(mem_total_mib, "host total memory")
    pre_pid = _strict_positive_int(pre_process_identity.get("pid"), "pre-window live PID")
    post_pid = _strict_positive_int(post_process_identity.get("pid"), "post-window live PID")
    pre_start_ticks = _strict_positive_int(
        pre_process_identity.get("pid_start_ticks"), "pre-window live start ticks"
    )
    post_start_ticks = _strict_positive_int(
        post_process_identity.get("pid_start_ticks"), "post-window live start ticks"
    )
    pre_process_sha = _require_sha256(
        pre_process_identity.get("canonical_process_identity_sha256"),
        "pre-window process identity",
    )
    post_process_sha = _require_sha256(
        post_process_identity.get("canonical_process_identity_sha256"),
        "post-window process identity",
    )
    if (
        pre_pid != post_pid
        or pre_start_ticks != post_start_ticks
        or pre_process_sha != post_process_sha
    ):
        raise BuyE3DeploymentGateError("post-window health is not the same disabled process")
    for field, expected in (
        ("artifact_sha256", expected_artifact_sha256),
        ("execution_commit", expected_execution_commit),
    ):
        if (
            pre_process_identity.get(field) != expected
            or post_process_identity.get(field) != expected
        ):
            raise BuyE3DeploymentGateError(f"resource process identity drifted: {field}")
    callback = benchmark_receipt.get("callback_benchmark")
    decision = benchmark_receipt.get("decision_benchmark")
    if not isinstance(callback, Mapping) or not isinstance(decision, Mapping):
        raise BuyE3DeploymentGateError("benchmark receipt is malformed")
    if benchmark_receipt.get("artifact_sha256") != expected_artifact_sha256:
        raise BuyE3DeploymentGateError("resource benchmark binds another artifact")
    benchmark_sha = _require_sha256(
        benchmark_receipt.get("canonical_benchmark_receipt_sha256"),
        "benchmark receipt canonical hash",
    )
    if benchmark_sha != _document_sha256(
        dict(benchmark_receipt), "canonical_benchmark_receipt_sha256"
    ):
        raise BuyE3DeploymentGateError("benchmark receipt canonical hash drifted")
    benchmark_pid = _strict_positive_int(capture_provenance.get("benchmark_pid"), "benchmark PID")
    benchmark_start_ticks = _strict_positive_int(
        capture_provenance.get("benchmark_pid_start_ticks"), "benchmark start ticks"
    )
    normalized_samples = [
        _resource_sample(
            sample,
            expected_live_pid=pre_pid,
            expected_live_start_ticks=pre_start_ticks,
            expected_benchmark_pid=benchmark_pid,
            expected_benchmark_start_ticks=benchmark_start_ticks,
        )
        for sample in samples
    ]
    capture = _resource_capture(
        capture_provenance,
        samples=normalized_samples,
        expected_live_pid=pre_pid,
        expected_live_start_ticks=pre_start_ticks,
    )
    observed_rate = _strict_finite(callback.get("observed_live_rate_hz"), "observed rate")
    achieved_rate = _strict_finite(callback.get("achieved_rate_hz"), "achieved rate")
    callback_p99 = _strict_finite(callback.get("latency_p99_us"), "callback p99")
    decision_p99 = _strict_finite(decision.get("latency_p99_us"), "decision p99")
    counters = [_counter_map(sample) for sample in normalized_samples]
    baseline_counters = counters[0]
    mem_available = [
        _strict_finite(sample.get("mem_available_mib"), "available memory")
        for sample in normalized_samples
    ]
    live_rss = [
        _strict_finite(sample.get("live_rss_mib"), "live RSS") for sample in normalized_samples
    ]
    benchmark_rss = [
        _strict_finite(sample.get("benchmark_rss_mib"), "benchmark RSS")
        for sample in normalized_samples
    ]
    combined_rss = [
        live + benchmark for live, benchmark in zip(live_rss, benchmark_rss, strict=True)
    ]
    deep_buffers = [int(sample["deep_book_buffer"]) for sample in normalized_samples]
    oom_events = [int(sample["oom_events"]) for sample in normalized_samples]
    swap_in = [int(sample["swap_in_kib"]) for sample in normalized_samples]
    swap_out = [int(sample["swap_out_kib"]) for sample in normalized_samples]
    achieved_ratio = achieved_rate / observed_rate if observed_rate > 0.0 else 0.0
    checks = {
        "exact_2_vcpu_host": logical_cpu_count == 2,
        "host_memory_class_2gib": 1_800.0 <= total_memory <= 2_300.0,
        "direct_collector_authority": capture["authority"] == CONCURRENT_CAPTURE_AUTHORITY,
        "benchmark_process_overlap_proven": all(
            sample["benchmark_running"] is True for sample in normalized_samples
        ),
        "same_live_pid_start_identity": (
            capture["live_pid"] == pre_pid and capture["live_pid_start_ticks"] == pre_start_ticks
        ),
        "post_health_after_benchmark_exit": (capture["post_health_after_benchmark_exit"] is True),
        "concurrent_live_and_benchmark_observed": any(value > 0.0 for value in benchmark_rss),
        "min_mem_available_at_least_512mib": min(mem_available) >= MIN_MEM_AVAILABLE_MIB,
        "live_rss_at_most_512mib": max(live_rss) <= MAX_LIVE_RSS_MIB,
        "benchmark_rss_at_most_256mib": max(benchmark_rss) <= MAX_BENCHMARK_RSS_MIB,
        "combined_rss_at_most_768mib": max(combined_rss) <= MAX_COMBINED_RSS_MIB,
        "no_oom_events": min(oom_events) >= 0 and max(oom_events) == min(oom_events),
        "no_swap_activity": (
            min(swap_in) >= 0
            and min(swap_out) >= 0
            and max(swap_in) == min(swap_in)
            and max(swap_out) == min(swap_out)
        ),
        "zero_drop_invalid_overflow_delta": all(
            row[key] == baseline_counters[key] for row in counters for key in REQUIRED_ZERO_COUNTERS
        ),
        "deep_book_buffer_zero": all(value == 0 for value in deep_buffers),
        "true_2x_observed_rate": (
            observed_rate > 0.0 and achieved_rate >= MIN_RATE_MULTIPLIER * observed_rate
        ),
        "callback_p99_at_most_2ms": callback_p99 <= MAX_CALLBACK_P99_US,
        "decision_p99_at_most_10ms": decision_p99 <= MAX_DECISION_P99_US,
        "post_benchmark_same_pid_health": True,
    }
    receipt: dict[str, Any] = {
        "schema_version": CONCURRENT_RESOURCE_SCHEMA,
        "identity": OWNER_IDENTITY,
        "status": "concurrent_disabled_live_benchmark_passed",
        "generated_utc": _utc_now(),
        "artifact_sha256": expected_artifact_sha256,
        "execution_commit": expected_execution_commit,
        "execution_tag": expected_execution_tag,
        "host": {
            "logical_cpu_count": logical_cpu_count,
            "mem_total_mib": total_memory,
        },
        "sample_count": len(normalized_samples),
        "live_pid": pre_pid,
        "live_pid_start_ticks": pre_start_ticks,
        "pre_process_identity_sha256": pre_process_sha,
        "post_process_identity_sha256": post_process_sha,
        "benchmark_receipt_sha256": benchmark_sha,
        "thresholds": {
            "min_mem_available_mib": MIN_MEM_AVAILABLE_MIB,
            "max_live_rss_mib": MAX_LIVE_RSS_MIB,
            "max_benchmark_rss_mib": MAX_BENCHMARK_RSS_MIB,
            "max_combined_rss_mib": MAX_COMBINED_RSS_MIB,
            "min_achieved_to_observed_rate": MIN_RATE_MULTIPLIER,
            "max_callback_p99_us": MAX_CALLBACK_P99_US,
            "max_decision_p99_us": MAX_DECISION_P99_US,
        },
        "observed": {
            "min_mem_available_mib": min(mem_available),
            "max_live_rss_mib": max(live_rss),
            "max_benchmark_rss_mib": max(benchmark_rss),
            "max_combined_rss_mib": max(combined_rss),
            "achieved_to_observed_rate": achieved_ratio,
            "callback_p99_us": callback_p99,
            "decision_p99_us": decision_p99,
        },
        "capture": capture,
        "samples": normalized_samples,
        "checks": checks,
        "sample_series_sha256": _canonical_sha256(normalized_samples),
        **EVIDENCE_BOUNDARY,
    }
    receipt["canonical_resource_receipt_sha256"] = _document_sha256(
        receipt, "canonical_resource_receipt_sha256"
    )
    _immutable_json(output_path, receipt)
    if not all(checks.values()):
        failed = sorted(name for name, passed in checks.items() if not passed)
        raise BuyE3DeploymentGateError(f"concurrent resource gate failed: {failed}")
    return receipt


def _load_disabled_phase_process_identity(
    path: Path,
    *,
    expected_artifact_sha256: str,
    expected_execution_commit: str,
) -> dict[str, Any]:
    receipt, _target = _read_immutable_json(path)
    process = receipt.get("actual_process_identity")
    if (
        receipt.get("phase") != "disabled-deploy"
        or receipt.get("status") != "phase_complete"
        or receipt.get("canonical_receipt_sha256")
        != _document_sha256(receipt, "canonical_receipt_sha256")
        or not isinstance(process, Mapping)
        or process.get("canonical_process_identity_sha256")
        != _document_sha256(process, "canonical_process_identity_sha256")
        or process.get("artifact_sha256") != expected_artifact_sha256
        or process.get("execution_commit") != expected_execution_commit
        or process.get("buy_e3_enabled") is not False
        or process.get("owner_override_effective") is not False
        or process.get("initial_buy_deadline_identity") != "B0"
        or process.get("e3_deadline_imported") is not False
    ):
        raise BuyE3DeploymentGateError("disabled phase process receipt is not admissible")
    _strict_positive_int(process.get("pid"), "disabled live PID")
    _strict_positive_int(process.get("pid_start_ticks"), "disabled live start ticks")
    _require_sha256(process.get("canonical_process_identity_sha256"), "disabled process identity")
    return dict(process)


def _host_resource_identity() -> dict[str, Any]:
    memory = _meminfo()
    return {
        "logical_cpu_count": int(os.cpu_count() or 0),
        "mem_total_mib": memory["MemTotal"] / 1024.0,
    }


def _linux_resource_metrics(live_pid: int, benchmark_pid: int) -> dict[str, Any]:
    memory = _meminfo()
    vmstat = _vmstat_counters()
    return {
        "mem_available_mib": memory["MemAvailable"] / 1024.0,
        "live_rss_mib": _kib_field(_read_proc_status(live_pid), "VmRSS") / 1024.0,
        "benchmark_rss_mib": (_kib_field(_read_proc_status(benchmark_pid), "VmRSS") / 1024.0),
        "oom_events": vmstat["oom_kill"],
        "swap_in_kib": vmstat["pswpin"],
        "swap_out_kib": vmstat["pswpout"],
    }


def _benchmark_subprocess_command(
    *,
    python_executable: Path,
    artifact_manifest_path: Path,
    artifact_manifest_file_sha256: str,
    expected_artifact_sha256: str,
    policy_path: Path,
    policy_file_sha256: str,
    predicate_bundle_path: Path,
    predicate_bundle_file_sha256: str,
    health_receipt_path: Path,
    benchmark_output_path: Path,
    paced_duration_s: float,
) -> tuple[str, ...]:
    return (
        str(python_executable),
        str(Path(__file__).resolve()),
        "benchmark",
        "--artifact-manifest",
        str(artifact_manifest_path),
        "--artifact-manifest-file-sha256",
        artifact_manifest_file_sha256,
        "--artifact-sha256",
        expected_artifact_sha256,
        "--policy",
        str(policy_path),
        "--policy-file-sha256",
        policy_file_sha256,
        "--predicate-bundle",
        str(predicate_bundle_path),
        "--predicate-bundle-file-sha256",
        predicate_bundle_file_sha256,
        "--health-receipt",
        str(health_receipt_path),
        "--output",
        str(benchmark_output_path),
        "--paced-duration-s",
        str(float(paced_duration_s)),
    )


def _assert_process_snapshot(
    snapshot: Mapping[str, Any],
    *,
    expected_pid: int,
    expected_start_ticks: int,
    command_fragment: str,
) -> dict[str, Any]:
    observed = _exact_mapping(
        snapshot,
        ("pid", "pid_start_ticks", "cmdline", "cmdline_sha256"),
        "process snapshot",
    )
    command = observed.get("cmdline")
    if (
        observed.get("pid") != expected_pid
        or observed.get("pid_start_ticks") != expected_start_ticks
        or not isinstance(command, list)
        or not command
        or observed.get("cmdline_sha256") != _canonical_sha256(command)
        or not any(command_fragment in str(value) for value in command)
    ):
        raise BuyE3DeploymentGateError("process snapshot is stale or unexpected")
    return observed


def capture_concurrent_resource_receipt(
    *,
    repository_root: Path,
    disabled_process_identity: Mapping[str, Any],
    pid_file: Path,
    live_log_path: Path,
    artifact_manifest_path: Path,
    artifact_manifest_file_sha256: str,
    expected_artifact_sha256: str,
    policy_path: Path,
    policy_file_sha256: str,
    predicate_bundle_path: Path,
    predicate_bundle_file_sha256: str,
    health_receipt_path: Path,
    expected_execution_commit: str,
    expected_execution_tag: str,
    benchmark_output_path: Path,
    output_path: Path,
    python_executable: Path | None = None,
    paced_duration_s: float = 15.0,
    sample_interval_s: float = 0.1,
    post_health_timeout_s: float = 150.0,
    _popen_factory: Callable[..., Any] | None = None,
    _process_snapshot_provider: Callable[[int], Mapping[str, Any]] | None = None,
    _resource_metrics_provider: Callable[[int, int], Mapping[str, Any]] | None = None,
    _health_state_provider: Callable[[], Mapping[str, Any]] | None = None,
    _host_identity_provider: Callable[[], Mapping[str, Any]] | None = None,
    _git_identity_provider: Callable[[Path], Mapping[str, Any]] | None = None,
    _monotonic_ns: Callable[[], int] = time.monotonic_ns,
    _sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Run and sample the exact benchmark; no external sample payload is accepted."""

    interval = _strict_finite(sample_interval_s, "resource sample interval")
    post_timeout = _strict_finite(post_health_timeout_s, "post-health timeout")
    duration = _strict_finite(paced_duration_s, "benchmark duration")
    if interval <= 0.0 or post_timeout <= 0.0 or duration < 2.0:
        raise BuyE3DeploymentGateError("resource capture timing is invalid")
    root = repository_root.expanduser().resolve(strict=True)
    executable = (python_executable or Path(sys.executable)).expanduser().resolve(strict=True)
    benchmark_output = benchmark_output_path.expanduser().absolute()
    resource_output = output_path.expanduser().absolute()
    for target in (benchmark_output, resource_output):
        if target.exists() or target.is_symlink():
            raise BuyE3DeploymentGateError(f"immutable receipt already exists: {target.name}")
    expected_pid = _strict_positive_int(disabled_process_identity.get("pid"), "live PID")
    expected_start_ticks = _strict_positive_int(
        disabled_process_identity.get("pid_start_ticks"), "live start ticks"
    )
    if disabled_process_identity.get("artifact_sha256") != expected_artifact_sha256:
        raise BuyE3DeploymentGateError("disabled process binds another artifact")
    if disabled_process_identity.get("execution_commit") != expected_execution_commit:
        raise BuyE3DeploymentGateError("disabled process binds another execution")
    try:
        pid_from_file = int(pid_file.expanduser().resolve(strict=True).read_text().strip())
    except (OSError, ValueError) as exc:
        raise BuyE3DeploymentGateError("live PID file is unreadable") from exc
    if pid_from_file != expected_pid:
        raise BuyE3DeploymentGateError("live PID file is stale")
    process_provider = _process_snapshot_provider or _proc_snapshot
    metrics_provider = _resource_metrics_provider or _linux_resource_metrics
    host_provider = _host_identity_provider or _host_resource_identity
    git_provider = _git_identity_provider or _git_identity
    popen_factory = _popen_factory or subprocess.Popen
    health_provider: Callable[[], Mapping[str, Any]]
    if _health_state_provider is None:
        tail = _LiveHealthTail(live_log_path)
        health_provider = tail.snapshot
    else:
        health_provider = _health_state_provider
    _assert_process_snapshot(
        process_provider(expected_pid),
        expected_pid=expected_pid,
        expected_start_ticks=expected_start_ticks,
        command_fragment="live/main.py",
    )
    host = _exact_mapping(host_provider(), ("logical_cpu_count", "mem_total_mib"), "host identity")
    if type(host["logical_cpu_count"]) is not int or host["logical_cpu_count"] != 2:
        raise BuyE3DeploymentGateError("resource capture requires exactly 2 vCPUs")
    total_memory = _strict_finite(host["mem_total_mib"], "host total memory")
    if not 1_800.0 <= total_memory <= 2_300.0:
        raise BuyE3DeploymentGateError("resource capture requires the 2-GiB host class")
    git = git_provider(root)
    if (
        git.get("commit") != expected_execution_commit
        or expected_execution_tag not in git.get("annotated_tags_at_head", [])
        or git.get("worktree_clean") is not True
    ):
        raise BuyE3DeploymentGateError("resource capture checkout is not frozen")
    health_receipt = validate_health_receipt(health_receipt_path)
    if (
        health_receipt.get("runtime", {}).get("buy_e3_enabled") is not False
        or health_receipt.get("repository", {}).get("commit") != expected_execution_commit
    ):
        raise BuyE3DeploymentGateError("resource capture health receipt is not disabled/frozen")
    pre_health = dict(health_provider())
    command = _benchmark_subprocess_command(
        python_executable=executable,
        artifact_manifest_path=artifact_manifest_path.expanduser().resolve(strict=True),
        artifact_manifest_file_sha256=artifact_manifest_file_sha256,
        expected_artifact_sha256=expected_artifact_sha256,
        policy_path=policy_path.expanduser().resolve(strict=True),
        policy_file_sha256=policy_file_sha256,
        predicate_bundle_path=predicate_bundle_path.expanduser().resolve(strict=True),
        predicate_bundle_file_sha256=predicate_bundle_file_sha256,
        health_receipt_path=health_receipt_path.expanduser().resolve(strict=True),
        benchmark_output_path=benchmark_output,
        paced_duration_s=duration,
    )
    launch_ns = _strict_positive_int(_monotonic_ns(), "benchmark launch time")
    process: Any | None = None
    samples: list[dict[str, Any]] = []
    stdout = ""
    stderr = ""
    try:
        process = popen_factory(
            command,
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        benchmark_pid = _strict_positive_int(process.pid, "benchmark PID")
        benchmark_snapshot = process_provider(benchmark_pid)
        benchmark_start_ticks = _strict_positive_int(
            benchmark_snapshot.get("pid_start_ticks"), "benchmark start ticks"
        )
        _assert_process_snapshot(
            benchmark_snapshot,
            expected_pid=benchmark_pid,
            expected_start_ticks=benchmark_start_ticks,
            command_fragment="benchmark",
        )
        max_runtime_ns = int((duration + 180.0) * 1_000_000_000)
        while process.poll() is None:
            if _monotonic_ns() - launch_ns > max_runtime_ns:
                raise BuyE3DeploymentGateError("exact benchmark exceeded resource timeout")
            health = dict(health_provider())
            live_snapshot = _assert_process_snapshot(
                process_provider(expected_pid),
                expected_pid=expected_pid,
                expected_start_ticks=expected_start_ticks,
                command_fragment="live/main.py",
            )
            current_benchmark = _assert_process_snapshot(
                process_provider(benchmark_pid),
                expected_pid=benchmark_pid,
                expected_start_ticks=benchmark_start_ticks,
                command_fragment="benchmark",
            )
            metrics = _exact_mapping(
                metrics_provider(expected_pid, benchmark_pid),
                (
                    "mem_available_mib",
                    "live_rss_mib",
                    "benchmark_rss_mib",
                    "oom_events",
                    "swap_in_kib",
                    "swap_out_kib",
                ),
                "resource metrics",
            )
            sample_ns = _strict_positive_int(_monotonic_ns(), "resource sample time")
            if process.poll() is not None:
                break
            sample = {
                "monotonic_ns": sample_ns,
                "live_pid": live_snapshot["pid"],
                "live_pid_start_ticks": live_snapshot["pid_start_ticks"],
                "benchmark_pid": current_benchmark["pid"],
                "benchmark_pid_start_ticks": current_benchmark["pid_start_ticks"],
                "benchmark_running": True,
                "health_generation": health["generation"],
                "health_line_sha256": health["line_sha256"],
                **metrics,
                "deep_book_buffer": health["deep_book_buffer"],
                "counter_values": health["counter_values"],
            }
            samples.append(sample)
            _sleep(interval)
        benchmark_exit_ns = _strict_positive_int(_monotonic_ns(), "benchmark exit time")
        stdout, stderr = process.communicate(timeout=10.0)
        returncode = process.returncode
        if returncode != 0:
            raise BuyE3DeploymentGateError("exact benchmark subprocess failed")
        if len(samples) < 2:
            raise BuyE3DeploymentGateError(
                "benchmark did not overlap at least two direct resource samples"
            )
        exit_health_generation = int(dict(health_provider())["generation"])
        post_deadline_ns = benchmark_exit_ns + int(post_timeout * 1_000_000_000)
        post_health: dict[str, Any] | None = None
        post_observed_ns = benchmark_exit_ns
        while _monotonic_ns() <= post_deadline_ns:
            candidate = dict(health_provider())
            observed_ns = _strict_positive_int(_monotonic_ns(), "post-health time")
            _assert_process_snapshot(
                process_provider(expected_pid),
                expected_pid=expected_pid,
                expected_start_ticks=expected_start_ticks,
                command_fragment="live/main.py",
            )
            if int(candidate["generation"]) > exit_health_generation:
                post_health = candidate
                post_observed_ns = observed_ns
                break
            _sleep(min(interval, 0.25))
        if post_health is None:
            raise BuyE3DeploymentGateError("no fresh post-benchmark HEALTH state arrived")
        post_snapshot = _assert_process_snapshot(
            process_provider(expected_pid),
            expected_pid=expected_pid,
            expected_start_ticks=expected_start_ticks,
            command_fragment="live/main.py",
        )
        benchmark_receipt = validate_benchmark_receipt(
            benchmark_output, expected_artifact_sha256=expected_artifact_sha256
        )
        capture = {
            "authority": CONCURRENT_CAPTURE_AUTHORITY,
            "collector_pid": os.getpid(),
            "benchmark_command_sha256": _canonical_sha256(list(command)),
            "benchmark_pid": benchmark_pid,
            "benchmark_pid_start_ticks": benchmark_start_ticks,
            "live_pid": expected_pid,
            "live_pid_start_ticks": expected_start_ticks,
            "benchmark_launch_monotonic_ns": launch_ns,
            "first_overlap_sample_monotonic_ns": samples[0]["monotonic_ns"],
            "last_overlap_sample_monotonic_ns": samples[-1]["monotonic_ns"],
            "benchmark_exit_monotonic_ns": benchmark_exit_ns,
            "post_health_observed_monotonic_ns": post_observed_ns,
            "pre_health_generation": pre_health["generation"],
            "post_health_generation": post_health["generation"],
            "pre_health_line_sha256": pre_health["line_sha256"],
            "post_health_line_sha256": post_health["line_sha256"],
            "pre_counter_values": pre_health["counter_values"],
            "post_counter_values": post_health["counter_values"],
            "pre_deep_book_buffer": pre_health["deep_book_buffer"],
            "post_deep_book_buffer": post_health["deep_book_buffer"],
            "benchmark_returncode": returncode,
            "benchmark_stdout_sha256": hashlib.sha256(stdout.encode()).hexdigest(),
            "benchmark_stderr_sha256": hashlib.sha256(stderr.encode()).hexdigest(),
            "post_health_after_benchmark_exit": post_observed_ns >= benchmark_exit_ns,
        }
        return build_concurrent_resource_receipt(
            samples=samples,
            capture_provenance=capture,
            benchmark_receipt=benchmark_receipt,
            pre_process_identity=disabled_process_identity,
            post_process_identity={**dict(disabled_process_identity), **post_snapshot},
            logical_cpu_count=host["logical_cpu_count"],
            mem_total_mib=total_memory,
            expected_artifact_sha256=expected_artifact_sha256,
            expected_execution_commit=expected_execution_commit,
            expected_execution_tag=expected_execution_tag,
            output_path=resource_output,
        )
    except Exception:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10.0)
        raise


def validate_concurrent_resource_receipt(
    path: Path,
    *,
    expected_artifact_sha256: str,
    expected_execution_commit: str,
    expected_execution_tag: str,
    expected_disabled_process_identity: Mapping[str, Any],
) -> dict[str, Any]:
    payload, _target = _read_immutable_json(path)
    fields = {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "artifact_sha256",
        "execution_commit",
        "execution_tag",
        "host",
        "sample_count",
        "live_pid",
        "live_pid_start_ticks",
        "pre_process_identity_sha256",
        "post_process_identity_sha256",
        "benchmark_receipt_sha256",
        "thresholds",
        "observed",
        "capture",
        "samples",
        "checks",
        "sample_series_sha256",
        *EVIDENCE_BOUNDARY,
        "canonical_resource_receipt_sha256",
    }
    host = _exact_mapping(payload.get("host"), ("logical_cpu_count", "mem_total_mib"), "host")
    thresholds = _exact_mapping(
        payload.get("thresholds"),
        (
            "min_mem_available_mib",
            "max_live_rss_mib",
            "max_benchmark_rss_mib",
            "max_combined_rss_mib",
            "min_achieved_to_observed_rate",
            "max_callback_p99_us",
            "max_decision_p99_us",
        ),
        "resource thresholds",
    )
    expected_thresholds = {
        "min_mem_available_mib": MIN_MEM_AVAILABLE_MIB,
        "max_live_rss_mib": MAX_LIVE_RSS_MIB,
        "max_benchmark_rss_mib": MAX_BENCHMARK_RSS_MIB,
        "max_combined_rss_mib": MAX_COMBINED_RSS_MIB,
        "min_achieved_to_observed_rate": MIN_RATE_MULTIPLIER,
        "max_callback_p99_us": MAX_CALLBACK_P99_US,
        "max_decision_p99_us": MAX_DECISION_P99_US,
    }
    observed = _exact_mapping(
        payload.get("observed"),
        (
            "min_mem_available_mib",
            "max_live_rss_mib",
            "max_benchmark_rss_mib",
            "max_combined_rss_mib",
            "achieved_to_observed_rate",
            "callback_p99_us",
            "decision_p99_us",
        ),
        "resource observations",
    )
    values = {name: _strict_finite(value, name) for name, value in observed.items()}
    checks = _exact_mapping(payload.get("checks"), RESOURCE_CHECK_NAMES, "resource checks")
    process_sha = _require_sha256(
        expected_disabled_process_identity.get("canonical_process_identity_sha256"),
        "disabled process identity",
    )
    process_pid = _strict_positive_int(
        expected_disabled_process_identity.get("pid"), "disabled process PID"
    )
    process_start_ticks = _strict_positive_int(
        expected_disabled_process_identity.get("pid_start_ticks"),
        "disabled process start ticks",
    )
    samples_raw = payload.get("samples")
    capture_raw = payload.get("capture")
    if not isinstance(samples_raw, list) or not isinstance(capture_raw, Mapping):
        raise BuyE3DeploymentGateError("concurrent resource samples are malformed")
    benchmark_pid = _strict_positive_int(capture_raw.get("benchmark_pid"), "benchmark PID")
    benchmark_start_ticks = _strict_positive_int(
        capture_raw.get("benchmark_pid_start_ticks"), "benchmark start ticks"
    )
    samples = [
        _resource_sample(
            sample,
            expected_live_pid=process_pid,
            expected_live_start_ticks=process_start_ticks,
            expected_benchmark_pid=benchmark_pid,
            expected_benchmark_start_ticks=benchmark_start_ticks,
        )
        for sample in samples_raw
    ]
    capture = _resource_capture(
        capture_raw,
        samples=samples,
        expected_live_pid=process_pid,
        expected_live_start_ticks=process_start_ticks,
    )
    sample_mem = [float(sample["mem_available_mib"]) for sample in samples]
    sample_live_rss = [float(sample["live_rss_mib"]) for sample in samples]
    sample_benchmark_rss = [float(sample["benchmark_rss_mib"]) for sample in samples]
    sample_combined_rss = [
        live + benchmark
        for live, benchmark in zip(sample_live_rss, sample_benchmark_rss, strict=True)
    ]
    expected_sample_observations = {
        "min_mem_available_mib": min(sample_mem),
        "max_live_rss_mib": max(sample_live_rss),
        "max_benchmark_rss_mib": max(sample_benchmark_rss),
        "max_combined_rss_mib": max(sample_combined_rss),
    }
    host_cpu_count = host["logical_cpu_count"]
    if (
        set(payload) != fields
        or payload.get("schema_version") != CONCURRENT_RESOURCE_SCHEMA
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("status") != "concurrent_disabled_live_benchmark_passed"
        or payload.get("artifact_sha256") != expected_artifact_sha256
        or payload.get("execution_commit") != expected_execution_commit
        or payload.get("execution_tag") != expected_execution_tag
        or type(host_cpu_count) is not int
        or host_cpu_count != 2
        or not 1_800.0 <= _strict_finite(host["mem_total_mib"], "host total memory") <= 2_300.0
        or _strict_positive_int(payload.get("sample_count"), "resource sample count") < 2
        or payload.get("sample_count") != len(samples)
        or payload.get("live_pid") != process_pid
        or payload.get("live_pid_start_ticks") != process_start_ticks
        or payload.get("pre_process_identity_sha256") != process_sha
        or payload.get("post_process_identity_sha256") != process_sha
        or thresholds != expected_thresholds
        or any(
            values[field] != expected for field, expected in expected_sample_observations.items()
        )
        or values["min_mem_available_mib"] < MIN_MEM_AVAILABLE_MIB
        or values["max_live_rss_mib"] > MAX_LIVE_RSS_MIB
        or values["max_benchmark_rss_mib"] > MAX_BENCHMARK_RSS_MIB
        or values["max_combined_rss_mib"] > MAX_COMBINED_RSS_MIB
        or values["achieved_to_observed_rate"] < MIN_RATE_MULTIPLIER
        or values["callback_p99_us"] > MAX_CALLBACK_P99_US
        or values["decision_p99_us"] > MAX_DECISION_P99_US
        or any(checks[name] is not True for name in RESOURCE_CHECK_NAMES)
        or capture != capture_raw
        or samples != samples_raw
        or payload.get("sample_series_sha256") != _canonical_sha256(samples)
        or any(payload.get(field) is not value for field, value in EVIDENCE_BOUNDARY.items())
        or payload.get("canonical_resource_receipt_sha256")
        != _document_sha256(payload, "canonical_resource_receipt_sha256")
    ):
        raise BuyE3DeploymentGateError("concurrent resource receipt identity drifted")
    _require_sha256(payload.get("benchmark_receipt_sha256"), "resource benchmark hash")
    _require_sha256(payload.get("sample_series_sha256"), "resource sample-series hash")
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
    """Run a source-bound nodeid manifest with no brittle passed-count constant."""

    root = repository_root.expanduser().resolve()
    git = _git_identity(root)
    if (
        git["commit"] != expected_execution_commit
        or expected_execution_tag not in git["annotated_tags_at_head"]
        or git["worktree_clean"] is not True
    ):
        raise BuyE3DeploymentGateError("runtime regression checkout is not frozen")
    executable = _lexical_python_executable(
        python_executable if python_executable is not None else root / ".venv/bin/python"
    )
    collect_command = [
        str(executable),
        "-m",
        "pytest",
        "--collect-only",
        "-q",
        *RUNTIME_REGRESSION_TESTS,
    ]
    collected = subprocess.run(
        collect_command,
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if collected.returncode != 0:
        raise BuyE3DeploymentGateError("runtime regression nodeid collection failed")
    nodeids = _pytest_nodeids(f"{collected.stdout}\n{collected.stderr}")
    nodeid_source_counts = _nodeid_source_counts(nodeids)
    with tempfile.TemporaryDirectory(prefix="f05-buy-e3-regression-") as temporary:
        junit_path = Path(temporary) / "junit.xml"
        command = [
            str(executable),
            "-m",
            "pytest",
            "-q",
            f"--junitxml={junit_path}",
            *nodeids,
        ]
        completed = subprocess.run(
            command,
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        try:
            junit_root = ET.parse(junit_path).getroot()
        except (OSError, ET.ParseError) as exc:
            raise BuyE3DeploymentGateError("runtime regression JUnit receipt is missing") from exc
        suites = (
            [junit_root] if junit_root.tag == "testsuite" else list(junit_root.findall("testsuite"))
        )
        test_count = sum(int(suite.attrib.get("tests", "0")) for suite in suites)
        failure_count = sum(int(suite.attrib.get("failures", "0")) for suite in suites)
        error_count = sum(int(suite.attrib.get("errors", "0")) for suite in suites)
        skipped_count = sum(int(suite.attrib.get("skipped", "0")) for suite in suites)
    passed_count = test_count - failure_count - error_count - skipped_count
    status = (
        "passed"
        if (
            completed.returncode == 0
            and test_count == len(nodeids)
            and passed_count == len(nodeids)
            and failure_count == 0
            and error_count == 0
            and skipped_count == 0
        )
        else "failed"
    )
    test_files = {path: _file_sha256(root / path) for path in RUNTIME_REGRESSION_TESTS}
    runtime_sources = {path: _file_sha256(root / path) for path in RUNTIME_REGRESSION_SOURCES}
    receipt: dict[str, Any] = {
        "schema_version": COMPATIBLE_REGRESSION_SCHEMA,
        "identity": OWNER_IDENTITY,
        "status": status,
        "generated_utc": _utc_now(),
        "artifact_sha256": expected_artifact_sha256,
        "execution_commit": expected_execution_commit,
        "execution_tag": expected_execution_tag,
        "python_executable": str(executable),
        "python_file_sha256": _file_sha256(executable),
        "collect_command": collect_command,
        "run_command": command,
        "nodeids": list(nodeids),
        "nodeid_manifest_sha256": _canonical_sha256(list(nodeids)),
        "nodeid_source_counts": nodeid_source_counts,
        "collected": len(nodeids),
        "executed": test_count,
        "passed": passed_count,
        "failed": failure_count,
        "errors": error_count,
        "skipped": skipped_count,
        "collection_return_code": collected.returncode,
        "return_code": completed.returncode,
        "collection_stdout_sha256": hashlib.sha256(collected.stdout.encode()).hexdigest(),
        "collection_stderr_sha256": hashlib.sha256(collected.stderr.encode()).hexdigest(),
        "run_stdout_sha256": hashlib.sha256(completed.stdout.encode()).hexdigest(),
        "run_stderr_sha256": hashlib.sha256(completed.stderr.encode()).hexdigest(),
        "test_files": test_files,
        "runtime_sources": runtime_sources,
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
        "economic_values_persisted": False,
        "hypothetical_live_actions_scored": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_receipt_sha256"] = _document_sha256(receipt, "canonical_receipt_sha256")
    _immutable_json(output_path, receipt)
    if status != "passed":
        raise BuyE3DeploymentGateError("runtime regression test suite failed")
    return receipt


def validate_runtime_regression_receipt(
    path: Path,
    *,
    repository_root: Path,
    expected_artifact_sha256: str,
    expected_execution_commit: str,
    expected_execution_tag: str,
) -> dict[str, Any]:
    payload, _target = _read_immutable_json(path)
    fields = {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "artifact_sha256",
        "execution_commit",
        "execution_tag",
        "python_executable",
        "python_file_sha256",
        "collect_command",
        "run_command",
        "nodeids",
        "nodeid_manifest_sha256",
        "nodeid_source_counts",
        "collected",
        "executed",
        "passed",
        "failed",
        "errors",
        "skipped",
        "collection_return_code",
        "return_code",
        "collection_stdout_sha256",
        "collection_stderr_sha256",
        "run_stdout_sha256",
        "run_stderr_sha256",
        "test_files",
        "runtime_sources",
        "coverage",
        *EVIDENCE_BOUNDARY,
        "canonical_receipt_sha256",
    }
    nodeids_raw = payload.get("nodeids")
    if (
        not isinstance(nodeids_raw, list)
        or not nodeids_raw
        or any(not isinstance(nodeid, str) or "::" not in nodeid for nodeid in nodeids_raw)
        or nodeids_raw != sorted(set(nodeids_raw))
    ):
        raise BuyE3DeploymentGateError("runtime regression nodeid manifest drifted")
    root = repository_root.expanduser().resolve(strict=True)
    expected_test_files = {path: _file_sha256(root / path) for path in RUNTIME_REGRESSION_TESTS}
    expected_sources = {path: _file_sha256(root / path) for path in RUNTIME_REGRESSION_SOURCES}
    expected_nodeid_source_counts = _nodeid_source_counts(nodeids_raw)
    executable = _lexical_python_executable(
        Path(str(payload.get("python_executable", "")))
    )
    expected_collect = [
        str(executable),
        "-m",
        "pytest",
        "--collect-only",
        "-q",
        *RUNTIME_REGRESSION_TESTS,
    ]
    expected_run = [
        str(executable),
        "-m",
        "pytest",
        "-q",
        "<junit-output>",
        *nodeids_raw,
    ]
    observed_run = list(payload.get("run_command", []))
    if len(observed_run) >= 5 and str(observed_run[4]).startswith("--junitxml="):
        observed_run[4] = "<junit-output>"
    coverage = payload.get("coverage")
    expected_coverage = {
        "buy_disabled_equals_b0",
        "exposure_increasing_buy_only",
        "reducing_buy_unchanged",
        "fixed_action_total_cooldown",
        "b0_consecutive_units",
        "partial_fill_and_consecutive_units",
        "restart_and_rollback",
        "gap_and_out_of_order",
        "stale_unobserved_hash_drift_fallback",
        "sell_integration_unchanged",
    }
    if (
        set(payload) != fields
        or payload.get("schema_version") != COMPATIBLE_REGRESSION_SCHEMA
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("status") != "passed"
        or payload.get("artifact_sha256") != expected_artifact_sha256
        or payload.get("execution_commit") != expected_execution_commit
        or payload.get("execution_tag") != expected_execution_tag
        or payload.get("python_file_sha256") != _file_sha256(executable)
        or payload.get("collect_command") != expected_collect
        or observed_run != expected_run
        or payload.get("nodeid_manifest_sha256") != _canonical_sha256(nodeids_raw)
        or payload.get("nodeid_source_counts") != expected_nodeid_source_counts
        or payload.get("collected") != len(nodeids_raw)
        or payload.get("executed") != len(nodeids_raw)
        or payload.get("passed") != len(nodeids_raw)
        or any(payload.get(field) != 0 for field in ("failed", "errors", "skipped"))
        or payload.get("collection_return_code") != 0
        or payload.get("return_code") != 0
        or payload.get("test_files") != expected_test_files
        or payload.get("runtime_sources") != expected_sources
        or not isinstance(coverage, Mapping)
        or set(coverage) != expected_coverage
        or any(coverage[name] is not True for name in expected_coverage)
        or any(payload.get(field) is not value for field, value in EVIDENCE_BOUNDARY.items())
        or payload.get("canonical_receipt_sha256")
        != _document_sha256(payload, "canonical_receipt_sha256")
    ):
        raise BuyE3DeploymentGateError("runtime regression receipt identity drifted")
    for field in (
        "collection_stdout_sha256",
        "collection_stderr_sha256",
        "run_stdout_sha256",
        "run_stderr_sha256",
    ):
        _require_sha256(payload.get(field), f"runtime regression {field}")
    return payload


def validate_sell_owner_54_case_receipt(
    path: Path,
    *,
    repository_root: Path,
    expected_artifact_sha256: str,
    expected_artifact_files: Mapping[str, str],
) -> dict[str, Any]:
    """Validate the real parity receipt and bind the exact evaluator sources."""

    payload, target, receipt_file_sha256, _identity = _read_immutable_json_record(path)
    fields = {
        "schema_version",
        "identity",
        "status",
        "layer",
        "artifact_sha256",
        "artifact_manifest_file_sha256",
        "policy_file_sha256",
        "predicate_bundle_file_sha256",
        "evidence",
        "economic_values_materialized_by_replay",
        "economic_values_exposed",
        "economic_values_used_for_selection",
        "validation_read",
        "sealed_holdout_read",
        "action_authorized",
        "live_authorized",
        "canonical_receipt_sha256",
    }
    evidence = _exact_mapping(
        payload.get("evidence"),
        (
            "policy_sha256",
            "predicate_bundle_sha256",
            "predicate_columns",
            "sell_tri_state_cases",
            "buy_tri_state_cases",
            "mismatch_count",
            "documented_semantics_equal",
            "runtime_binding_valid",
        ),
        "SELL 54-case evidence",
    )
    if (
        set(payload) != fields
        or payload.get("schema_version") != SELL_PARITY_SCHEMA
        or payload.get("identity") != OWNER_IDENTITY
        or payload.get("status") != "parity_complete"
        or payload.get("layer") != "sell_owner_54_case_unchanged"
        or payload.get("artifact_sha256") != expected_artifact_sha256
        or payload.get("artifact_manifest_file_sha256") != expected_artifact_files.get("manifest")
        or payload.get("policy_file_sha256") != expected_artifact_files.get("policy")
        or payload.get("predicate_bundle_file_sha256")
        != expected_artifact_files.get("predicate_bundle")
        or evidence.get("sell_tri_state_cases") != 27
        or evidence.get("buy_tri_state_cases") != 27
        or evidence.get("mismatch_count") != 0
        or evidence.get("documented_semantics_equal") is not True
        or evidence.get("runtime_binding_valid") is not True
        or not isinstance(evidence.get("predicate_columns"), list)
        or not evidence["predicate_columns"]
        or payload.get("economic_values_materialized_by_replay") is not False
        or payload.get("economic_values_exposed") is not False
        or payload.get("economic_values_used_for_selection") is not False
        or payload.get("validation_read") is not False
        or payload.get("sealed_holdout_read") is not False
        or payload.get("action_authorized") is not False
        or payload.get("live_authorized") is not False
        or payload.get("canonical_receipt_sha256")
        != _document_sha256(payload, "canonical_receipt_sha256")
    ):
        raise BuyE3DeploymentGateError("SELL 54-case receipt identity drifted")
    _require_sha256(evidence.get("policy_sha256"), "SELL policy hash")
    _require_sha256(evidence.get("predicate_bundle_sha256"), "SELL predicate bundle hash")
    root = repository_root.expanduser().resolve(strict=True)
    source_files = {source: _file_sha256(root / source) for source in SELL_54_CASE_SOURCE_PATHS}
    return {
        "path": str(target),
        "file_sha256": receipt_file_sha256,
        "canonical_receipt_sha256": payload["canonical_receipt_sha256"],
        "sell_policy_sha256": evidence["policy_sha256"],
        "sell_predicate_bundle_sha256": evidence["predicate_bundle_sha256"],
        "source_files": source_files,
        "source_manifest_sha256": _canonical_sha256(source_files),
    }


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

    concurrent = subparsers.add_parser("capture-concurrent")
    concurrent.add_argument("--repository-root", type=Path, required=True)
    concurrent.add_argument("--disabled-phase-receipt", type=Path, required=True)
    concurrent.add_argument("--pid-file", type=Path, required=True)
    concurrent.add_argument("--live-log", type=Path, required=True)
    concurrent.add_argument("--artifact-manifest", type=Path, required=True)
    concurrent.add_argument("--artifact-manifest-file-sha256", required=True)
    concurrent.add_argument("--artifact-sha256", required=True)
    concurrent.add_argument("--policy", type=Path, required=True)
    concurrent.add_argument("--policy-file-sha256", required=True)
    concurrent.add_argument("--predicate-bundle", type=Path, required=True)
    concurrent.add_argument("--predicate-bundle-file-sha256", required=True)
    concurrent.add_argument("--health-receipt", type=Path, required=True)
    concurrent.add_argument("--execution-commit", required=True)
    concurrent.add_argument("--execution-tag", required=True)
    concurrent.add_argument("--benchmark-output", type=Path, required=True)
    concurrent.add_argument("--output", type=Path, required=True)
    concurrent.add_argument("--python", type=Path)
    concurrent.add_argument("--paced-duration-s", type=float, default=15.0)
    concurrent.add_argument("--sample-interval-s", type=float, default=0.1)
    concurrent.add_argument("--post-health-timeout-s", type=float, default=150.0)

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
    if args.command == "capture-concurrent":
        disabled_process = _load_disabled_phase_process_identity(
            args.disabled_phase_receipt,
            expected_artifact_sha256=args.artifact_sha256,
            expected_execution_commit=args.execution_commit,
        )
        payload = capture_concurrent_resource_receipt(
            repository_root=args.repository_root,
            disabled_process_identity=disabled_process,
            pid_file=args.pid_file,
            live_log_path=args.live_log,
            artifact_manifest_path=args.artifact_manifest,
            artifact_manifest_file_sha256=args.artifact_manifest_file_sha256,
            expected_artifact_sha256=args.artifact_sha256,
            policy_path=args.policy,
            policy_file_sha256=args.policy_file_sha256,
            predicate_bundle_path=args.predicate_bundle,
            predicate_bundle_file_sha256=args.predicate_bundle_file_sha256,
            health_receipt_path=args.health_receipt,
            expected_execution_commit=args.execution_commit,
            expected_execution_tag=args.execution_tag,
            benchmark_output_path=args.benchmark_output,
            output_path=args.output,
            python_executable=args.python,
            paced_duration_s=args.paced_duration_s,
            sample_interval_s=args.sample_interval_s,
            post_health_timeout_s=args.post_health_timeout_s,
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
    "COMPATIBLE_REGRESSION_SCHEMA",
    "BuyE3DeploymentGateError",
    "CONCURRENT_CAPTURE_AUTHORITY",
    "CONCURRENT_RESOURCE_SCHEMA",
    "GATE_SCHEMA",
    "HEALTH_SCHEMA",
    "REGRESSION_SCHEMA",
    "build_concurrent_resource_receipt",
    "build_deployment_gate_receipt",
    "capture_concurrent_resource_receipt",
    "capture_live_health_window",
    "run_host_benchmark",
    "run_runtime_regression_tests",
    "validate_benchmark_receipt",
    "validate_concurrent_resource_receipt",
    "validate_deployment_gate_receipt",
    "validate_health_receipt",
    "validate_runtime_regression_receipt",
    "validate_sell_owner_54_case_receipt",
]
