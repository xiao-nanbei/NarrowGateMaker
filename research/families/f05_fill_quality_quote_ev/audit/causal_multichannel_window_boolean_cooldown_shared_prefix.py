"""POSIX copy-on-write shared-prefix executor for cooldown duration arms.

The Python replay reaches one strategy-visible exposure fill exactly once and
forks a bounded supervisor that retains that exact copy-on-write frame.  The
baseline parent keeps replaying while supervisors share a global two-arm POSIX
token pool.  Every arm therefore inherits the same market, book, order,
inventory, campaign, cooldown, and EMA state without replaying the prefix or
serializing the monolithic simulator frame.

This executor is deliberately process-local and Python-only.  It does not
claim portable checkpoint restore authority.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import signal
import time
import traceback
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_strict_checkpoint import (
    ARM_DURATION_MS,
    BUY_ARMS,
    SELL_ARMS,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2"
SCHEMA_VERSION = f"{IDENTITY}.posix_cow_shared_prefix.v6"
ARM_RESULT_SCHEMA_VERSION = f"{IDENTITY}.strict_native_one_shot_arm.v7"
OPPORTUNITY_MANIFEST_SCHEMA_VERSION = (
    f"{IDENTITY}.strict_native_one_shot_opportunity.v6"
)
SUPERVISOR_ERROR_SCHEMA_VERSION = f"{SCHEMA_VERSION}.supervisor_error.v1"
DEFAULT_GLOBAL_ARM_PROCESSES = 2
MAX_GLOBAL_ARM_PROCESSES = 8
MAX_INFLIGHT_OPPORTUNITY_SNAPSHOTS = 4
_POOL_POLL_INTERVAL_S = 0.01
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
STRICT_QUEUE_SCOPE = "strategy_independent_native_snapshot_delta_exchange_time_v1"
STRICT_COUNTER_FIELDS = (
    "exchange_book_queue_lookup_count",
    "exchange_book_queue_exact_count",
    "exchange_book_queue_known_zero_count",
    "exchange_book_queue_missing_count",
    "exchange_book_queue_invalidated_order_count",
    "exchange_book_queue_ambiguous_event_count",
    "exchange_book_cancel_trade_ambiguous_order_count",
    "exchange_book_cancel_book_ambiguous_order_count",
    "exchange_book_events_consumed",
    "exchange_book_events_accepted",
    "exchange_book_events_rejected",
    "exchange_book_source_gap_events",
    "exchange_book_invalid_sequence_messages",
    "exchange_book_sequence_gaps",
    "exchange_book_message_time_reversals",
    "exchange_book_event_timestamp_fallback_events",
    "exchange_book_receive_timestamp_fallback_events",
    "exchange_book_unknown_timestamp_source_events",
)
STRICT_HARD_ZERO_FIELDS = (
    "exchange_book_source_gap_events",
    "exchange_book_invalid_sequence_messages",
    "exchange_book_sequence_gaps",
    "exchange_book_message_time_reversals",
    "exchange_book_event_timestamp_fallback_events",
    "exchange_book_receive_timestamp_fallback_events",
    "exchange_book_unknown_timestamp_source_events",
)
STRICT_PREFIX_SOURCE_HARD_ZERO_FIELDS = STRICT_HARD_ZERO_FIELDS
STRICT_LABEL_UNSUPPORTED_FIELDS = (
    "exchange_book_queue_missing_count",
    "exchange_book_queue_invalidated_order_count",
    "exchange_book_queue_ambiguous_event_count",
    "exchange_book_cancel_trade_ambiguous_order_count",
    "exchange_book_cancel_book_ambiguous_order_count",
)
MISSING_TRACE_FIELDS = {
    "order_id",
    "side",
    "price",
    "price_tick",
    "activate_ts_ms",
    "status",
    "reason",
    "asof_exchange_ts_ns",
    "segment_id",
    "snapshot_min_tick",
    "snapshot_max_tick",
}


class SharedPrefixExecutionError(RuntimeError):
    """Raised when one shared-prefix opportunity cannot be admitted."""


def _canonical_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _require_sha256(value: Any, field: str) -> str:
    normalized = str(value).strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise SharedPrefixExecutionError(f"{field} must be a lowercase SHA256")
    return normalized


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
    encoded = _canonical_bytes(payload)
    with temp.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)
    _fsync_directory(path.parent)


@dataclass(frozen=True, slots=True)
class SharedPrefixArmSelection:
    arm_id: str
    action: str
    fixed_duration_ms: float
    opportunity_identity_sha256: str
    strict_counter_baseline: tuple[tuple[str, int], ...]
    exchange_book_queue_missing_trace_cursor: int
    exchange_book_queue_missing_count_at_assignment: int
    exact_owner_action: str | None = None
    exact_owner_policy_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class SharedPrefixExecutionAudit:
    opportunities_dispatched: int
    opportunities_resumed: int
    opportunities_skipped_after_limit: int
    opportunities_skipped_outside_target_day: int
    arm_processes_completed: int
    max_parallel_arms: int
    completed_manifest_paths: tuple[str, ...]
    supervisor_processes_completed: int
    executor_wall_time_s: float
    supervisor_wall_time_s_total: float
    arm_wall_time_s_total: float
    peak_concurrent_supervisors: int
    peak_concurrent_arms: int
    max_inflight_opportunity_snapshots: int
    pending_supervisors: int
    target_opportunity_count: int = 0
    target_opportunities_matched: int = 0
    opportunities_skipped_outside_target_set: int = 0
    modeled_queue_economics_authorized: bool = False
    exact_owner_baseline_policy_enabled: bool = False
    asynchronous_parent_replay: bool = True
    simulator_checkpoint_semantics: str = "posix_fork_copy_on_write_at_fill_callback"
    portable_restore_authority: bool = False
    economic_outcomes_read_by_parent: bool = False


@dataclass(frozen=True, slots=True)
class _PendingArm:
    arm_id: str
    slot_fd: int
    started_ns: int
    started_at_utc: str


@dataclass(frozen=True, slots=True)
class _PendingSupervisor:
    pid: int
    opportunity_index: int
    opportunity_identity_sha256: str
    destination: Path
    staging: Path
    arms: tuple[str, ...]
    expected_owner_action: str | None


_TARGET_KEY_FIELDS = (
    "exposure_fill_ordinal",
    "fill_visible_ts_ms",
    "side",
    "order_id",
    "campaign_id",
)


def _target_key(payload: Mapping[str, Any]) -> tuple[int, int, str, int, int]:
    try:
        key = (
            int(payload["exposure_fill_ordinal"]),
            int(payload["fill_visible_ts_ms"]),
            str(payload["side"]).upper(),
            int(payload["order_id"]),
            int(payload["campaign_id"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SharedPrefixExecutionError(
            "shared-prefix target opportunity key is malformed"
        ) from exc
    if key[0] <= 0 or key[1] <= 0 or key[2] not in {"BUY", "SELL"}:
        raise SharedPrefixExecutionError(
            "shared-prefix target opportunity key is invalid"
        )
    if key[3] < 0 or key[4] <= 0:
        raise SharedPrefixExecutionError(
            "shared-prefix target order/campaign identity is invalid"
        )
    return key


class PosixCooldownSharedPrefixExecutor:
    """Asynchronously execute eight duration arms from each retained frame."""

    def __init__(
        self,
        *,
        output_root: Path,
        target_day: str,
        source_contract_sha256: str,
        execution_identity_hashes: Mapping[str, str],
        max_parallel_arms: int = DEFAULT_GLOBAL_ARM_PROCESSES,
        max_opportunities: int | None = None,
        require_strict_native: bool = True,
        modeled_queue_economics_authorized: bool = False,
        exact_owner_policy_sha256: str | None = None,
        target_opportunities: Sequence[Mapping[str, Any]] | None = None,
        global_pool_root: Path | None = None,
        recover_interrupted_staging: bool = False,
        progress: Callable[[int, Path, bool], None] | None = None,
    ) -> None:
        if not hasattr(os, "fork"):
            raise SharedPrefixExecutionError("POSIX fork is unavailable")
        if not 1 <= int(max_parallel_arms) <= MAX_GLOBAL_ARM_PROCESSES:
            raise SharedPrefixExecutionError(
                "max_parallel_arms escaped the bounded POSIX worker range"
            )
        if max_opportunities is not None and int(max_opportunities) <= 0:
            raise SharedPrefixExecutionError(
                "max_opportunities must be positive when provided"
            )
        if target_opportunities is not None and max_opportunities is not None:
            raise SharedPrefixExecutionError(
                "an explicit target set cannot share max_opportunities"
            )
        if not str(target_day).strip():
            raise SharedPrefixExecutionError("target_day is empty")
        if modeled_queue_economics_authorized and require_strict_native:
            raise SharedPrefixExecutionError(
                "modeled-queue economics cannot claim strict-native authority"
            )
        required_hashes = {
            "baseline_identity_sha256",
            "config_sha256",
            "code_sha256",
            "model_sha256",
            "p3_sha256",
            "feature_dag_sha256",
            "execution_abi_sha256",
        }
        if set(execution_identity_hashes) != required_hashes:
            raise SharedPrefixExecutionError(
                "shared-prefix execution identity schema drifted"
            )
        self.output_root = Path(output_root).expanduser().resolve()
        self.target_day = str(target_day)
        self.source_contract_sha256 = _require_sha256(
            source_contract_sha256,
            "source_contract_sha256",
        )
        self.execution_identity_hashes = {
            key: _require_sha256(value, key)
            for key, value in execution_identity_hashes.items()
        }
        self.max_parallel_arms = int(max_parallel_arms)
        self.max_opportunities = (
            None if max_opportunities is None else int(max_opportunities)
        )
        self.require_strict_native = bool(require_strict_native)
        self.modeled_queue_economics_authorized = bool(
            modeled_queue_economics_authorized
        )
        self.exact_owner_policy_sha256 = (
            None
            if exact_owner_policy_sha256 is None
            else _require_sha256(
                exact_owner_policy_sha256,
                "exact_owner_policy_sha256",
            )
        )
        self.recover_interrupted_staging = bool(recover_interrupted_staging)
        self.progress = progress
        self._target_opportunities = self._normalize_target_opportunities(
            target_opportunities
        )
        self._target_opportunity_keys_seen: set[
            tuple[int, int, str, int, int]
        ] = set()
        self._role = "baseline_parent"
        self._selection: SharedPrefixArmSelection | None = None
        self._arm_output_path: Path | None = None
        self._opportunities_dispatched = 0
        self._opportunities_resumed = 0
        self._opportunities_skipped_after_limit = 0
        self._opportunities_skipped_outside_target_day = 0
        self._opportunities_skipped_outside_target_set = 0
        self._arm_processes_completed = 0
        self._supervisor_processes_completed = 0
        self._completed_manifests: dict[int, str] = {}
        self._pending_supervisors: dict[int, _PendingSupervisor] = {}
        self._peak_concurrent_supervisors = 0
        self._supervisor_wall_time_s_total = 0.0
        self._arm_wall_time_s_total = 0.0
        self._executor_started_ns = time.monotonic_ns()
        self.max_inflight_opportunity_snapshots = (
            MAX_INFLIGHT_OPPORTUNITY_SNAPSHOTS
        )
        self._pool_run_id = uuid.uuid4().hex
        self._pool_root = (
            self.output_root / ".posix-cow-global-arm-pool"
            if global_pool_root is None
            else Path(global_pool_root).expanduser().resolve()
        )
        self._pool_metrics_path = self._pool_root / f"run-{self._pool_run_id}.json"
        self._pool_metrics_lock_path = self._pool_root / (
            f"run-{self._pool_run_id}.lock"
        )

    @property
    def exact_owner_baseline_policy_enabled(self) -> bool:
        return self.exact_owner_policy_sha256 is not None

    @staticmethod
    def _normalize_target_opportunities(
        rows: Sequence[Mapping[str, Any]] | None,
    ) -> dict[tuple[int, int, str, int, int], dict[str, Any]] | None:
        if rows is None:
            return None
        normalized: dict[tuple[int, int, str, int, int], dict[str, Any]] = {}
        required = {
            *_TARGET_KEY_FIELDS,
            "opportunity_id",
            "expected_owner_action",
            "arm_ids",
        }
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping) or set(row) != required:
                raise SharedPrefixExecutionError(
                    f"target_opportunities[{index}] schema drifted"
                )
            key = _target_key(row)
            if key in normalized:
                raise SharedPrefixExecutionError(
                    "target_opportunities contains a duplicate event identity"
                )
            opportunity_id = str(row["opportunity_id"]).strip()
            expected_owner_action = str(row["expected_owner_action"]).strip()
            arm_ids = tuple(str(value) for value in row["arm_ids"])
            allowed_arms = BUY_ARMS if key[2] == "BUY" else SELL_ARMS
            if not opportunity_id or expected_owner_action not in allowed_arms:
                raise SharedPrefixExecutionError(
                    "target opportunity external/owner identity is invalid"
                )
            if (
                not arm_ids
                or len(set(arm_ids)) != len(arm_ids)
                or any(value not in allowed_arms for value in arm_ids)
            ):
                raise SharedPrefixExecutionError(
                    "target opportunity arm vocabulary is invalid"
                )
            normalized[key] = {
                "opportunity_id": opportunity_id,
                "expected_owner_action": expected_owner_action,
                "arm_ids": arm_ids,
            }
        if not normalized:
            raise SharedPrefixExecutionError("target_opportunities is empty")
        return normalized

    @property
    def is_arm_child(self) -> bool:
        return self._role == "arm_child"

    @property
    def bounded_parent_stop_requested(self) -> bool:
        """Return true once a bounded parent has forked its frozen denominator."""

        if self._role != "baseline_parent":
            return False
        started = (
            self._opportunities_dispatched
            + self._opportunities_resumed
            + len(self._pending_supervisors)
        )
        if self._target_opportunities is not None:
            return started >= len(self._target_opportunities)
        if self.max_opportunities is None:
            return False
        return started >= self.max_opportunities

    def _empty_pool_metrics(self) -> dict[str, Any]:
        return {
            "schema_version": f"{SCHEMA_VERSION}.pool_metrics.v1",
            "run_id": self._pool_run_id,
            "active_supervisor_pids": [],
            "active_arm_pids": [],
            "arm_supervisor_pids": {},
            "peak_concurrent_supervisors": 0,
            "peak_concurrent_arms": 0,
        }

    def _ensure_global_pool(self) -> None:
        self._pool_root.mkdir(parents=True, exist_ok=True)
        for slot in range(self.max_parallel_arms):
            descriptor = os.open(
                self._pool_root / f"arm-slot-{slot}.lock",
                os.O_CREAT | os.O_RDWR,
                0o600,
            )
            os.close(descriptor)
        for slot in range(self.max_inflight_opportunity_snapshots):
            descriptor = os.open(
                self._pool_root / f"supervisor-slot-{slot}.lock",
                os.O_CREAT | os.O_RDWR,
                0o600,
            )
            os.close(descriptor)
        if not self._pool_metrics_path.exists():
            try:
                _write_atomic_json(
                    self._pool_metrics_path,
                    self._empty_pool_metrics(),
                )
            except FileExistsError:
                pass

    def _mutate_pool_metrics(
        self,
        *,
        add_supervisor_pid: int | None = None,
        remove_supervisor_pid: int | None = None,
        add_arm: tuple[int, int] | None = None,
        remove_arm_pid: int | None = None,
    ) -> dict[str, Any]:
        self._ensure_global_pool()
        lock_fd = os.open(
            self._pool_metrics_lock_path,
            os.O_CREAT | os.O_RDWR,
            0o600,
        )
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            if self._pool_metrics_path.exists():
                state = json.loads(
                    self._pool_metrics_path.read_text(encoding="ascii")
                )
            else:
                state = self._empty_pool_metrics()
            if state.get("schema_version") != f"{SCHEMA_VERSION}.pool_metrics.v1":
                raise SharedPrefixExecutionError("global arm-pool metric schema drifted")
            if state.get("run_id") != self._pool_run_id:
                raise SharedPrefixExecutionError("global arm-pool run identity drifted")

            supervisors = {int(pid) for pid in state["active_supervisor_pids"]}
            arms = {int(pid) for pid in state["active_arm_pids"]}
            owners = {
                int(pid): int(owner)
                for pid, owner in state["arm_supervisor_pids"].items()
            }
            if add_supervisor_pid is not None:
                supervisors.add(int(add_supervisor_pid))
            if remove_arm_pid is not None:
                arms.discard(int(remove_arm_pid))
                owners.pop(int(remove_arm_pid), None)
            if remove_supervisor_pid is not None:
                supervisor_pid = int(remove_supervisor_pid)
                supervisors.discard(supervisor_pid)
                for arm_pid, owner_pid in tuple(owners.items()):
                    if owner_pid == supervisor_pid:
                        arms.discard(arm_pid)
                        owners.pop(arm_pid, None)
            if add_arm is not None:
                arm_pid, supervisor_pid = (int(value) for value in add_arm)
                if supervisor_pid not in supervisors:
                    raise SharedPrefixExecutionError(
                        "arm registered without an active supervisor"
                    )
                arms.add(arm_pid)
                owners[arm_pid] = supervisor_pid
            if len(arms) > self.max_parallel_arms:
                raise SharedPrefixExecutionError(
                    "global arm pool exceeded its frozen process limit"
                )

            occupied_supervisors = self._count_occupied_global_slots(
                prefix="supervisor",
                count=self.max_inflight_opportunity_snapshots,
            )
            occupied_arms = self._count_occupied_global_slots(
                prefix="arm",
                count=self.max_parallel_arms,
            )
            state.update(
                {
                    "active_supervisor_pids": sorted(supervisors),
                    "active_arm_pids": sorted(arms),
                    "arm_supervisor_pids": {
                        str(pid): owners[pid] for pid in sorted(owners)
                    },
                    "peak_concurrent_supervisors": max(
                        int(state["peak_concurrent_supervisors"]),
                        len(supervisors),
                        occupied_supervisors,
                    ),
                    "peak_concurrent_arms": max(
                        int(state["peak_concurrent_arms"]),
                        len(arms),
                        occupied_arms,
                    ),
                }
            )
            _write_atomic_json(self._pool_metrics_path, state)
            return state
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    def _read_pool_metrics(self) -> dict[str, Any]:
        return self._mutate_pool_metrics()

    def _count_occupied_global_slots(self, *, prefix: str, count: int) -> int:
        occupied = 0
        for slot in range(count):
            descriptor = os.open(
                self._pool_root / f"{prefix}-slot-{slot}.lock",
                os.O_RDWR,
            )
            try:
                fcntl.flock(
                    descriptor,
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            except BlockingIOError:
                occupied += 1
            else:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
        return occupied

    def _try_acquire_global_slot(self, *, prefix: str, count: int) -> int | None:
        self._ensure_global_pool()
        for slot in range(count):
            descriptor = os.open(
                self._pool_root / f"{prefix}-slot-{slot}.lock",
                os.O_RDWR,
            )
            try:
                fcntl.flock(
                    descriptor,
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            except BlockingIOError:
                os.close(descriptor)
                continue
            return descriptor
        return None

    def _acquire_global_slot(self, *, prefix: str, count: int) -> int:
        while True:
            descriptor = self._try_acquire_global_slot(
                prefix=prefix,
                count=count,
            )
            if descriptor is not None:
                return descriptor
            time.sleep(_POOL_POLL_INTERVAL_S)

    def _acquire_global_supervisor_slot(self) -> int:
        return self._acquire_global_slot(
            prefix="supervisor",
            count=self.max_inflight_opportunity_snapshots,
        )

    @staticmethod
    def _release_global_slot(descriptor: int) -> None:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    @classmethod
    def _release_global_arm_slot(cls, descriptor: int) -> None:
        cls._release_global_slot(descriptor)

    @staticmethod
    def _validated_strict_counter_map(
        value: Any,
        *,
        field: str,
    ) -> dict[str, int]:
        if not isinstance(value, Mapping) or set(value) != set(
            STRICT_COUNTER_FIELDS
        ):
            raise SharedPrefixExecutionError(
                f"{field} strict counter schema drifted"
            )
        normalized: dict[str, int] = {}
        for name in STRICT_COUNTER_FIELDS:
            raw = value[name]
            if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                raise SharedPrefixExecutionError(
                    f"{field}.{name} must be a nonnegative integer"
                )
            normalized[name] = int(raw)
        return normalized

    @staticmethod
    def _validated_missing_trace(value: Any, *, field: str) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            raise SharedPrefixExecutionError(f"{field} must be a list")
        normalized: list[dict[str, Any]] = []
        observed_keys: set[tuple[int, int]] = set()
        for index, row in enumerate(value):
            if not isinstance(row, dict) or set(row) != MISSING_TRACE_FIELDS:
                raise SharedPrefixExecutionError(
                    f"{field}[{index}] schema drifted"
                )
            if not isinstance(row["side"], str) or not row["side"]:
                raise SharedPrefixExecutionError(
                    f"{field}[{index}].side is invalid"
                )
            price = row["price"]
            if isinstance(price, bool) or not isinstance(price, (int, float)):
                raise SharedPrefixExecutionError(
                    f"{field}[{index}].price is invalid"
                )
            if not math.isfinite(float(price)):
                raise SharedPrefixExecutionError(
                    f"{field}[{index}].price is not finite"
                )
            for name in (
                "order_id",
                "price_tick",
                "activate_ts_ms",
                "asof_exchange_ts_ns",
                "segment_id",
            ):
                if isinstance(row[name], bool) or not isinstance(row[name], int):
                    raise SharedPrefixExecutionError(
                        f"{field}[{index}].{name} is invalid"
                    )
            trace_key = (int(row["order_id"]), int(row["activate_ts_ms"]))
            if trace_key in observed_keys:
                raise SharedPrefixExecutionError(
                    f"{field}[{index}] duplicates a missing-seed trace row"
                )
            observed_keys.add(trace_key)
            for name in ("status", "reason"):
                if not isinstance(row[name], str):
                    raise SharedPrefixExecutionError(
                        f"{field}[{index}].{name} is invalid"
                    )
            for name in ("snapshot_min_tick", "snapshot_max_tick"):
                if row[name] is not None and (
                    isinstance(row[name], bool) or not isinstance(row[name], int)
                ):
                    raise SharedPrefixExecutionError(
                        f"{field}[{index}].{name} is invalid"
                    )
            normalized.append(dict(row))
        return normalized

    def _opportunity_identity(
        self,
        opportunity: Mapping[str, Any],
        *,
        target_binding: Mapping[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        required = {
            "exposure_fill_ordinal",
            "partial_fill_ordinal",
            "fill_visible_ts_ms",
            "fill_exchange_ts_ms",
            "side",
            "role_at_fill",
            "order_id",
            "campaign_id",
            "fill_qty_btc",
            "baseline_duration_ms",
            "cooldown_v2_snapshot_id",
            "cooldown_v2_source_bundle_sha256",
            "exchange_book_queue_mode",
            "exchange_book_queue_scope",
            "strict_counter_baseline",
            "exchange_book_queue_missing_trace_cursor",
            "exchange_book_queue_missing_count_at_assignment",
        }
        missing = sorted(required - set(opportunity))
        if missing:
            raise SharedPrefixExecutionError(
                f"shared-prefix opportunity is incomplete: {missing}"
            )
        side = str(opportunity["side"]).upper()
        if side not in {"BUY", "SELL"}:
            raise SharedPrefixExecutionError("opportunity side is invalid")
        role = str(opportunity["role_at_fill"]).lower()
        if role not in {"opener", "add"}:
            raise SharedPrefixExecutionError("opportunity role is invalid")
        if self.require_strict_native:
            if opportunity["exchange_book_queue_mode"] != "strict":
                raise SharedPrefixExecutionError(
                    "shared-prefix labels require strict native queue mode"
                )
            if opportunity["exchange_book_queue_scope"] != (
                "strategy_independent_native_snapshot_delta_exchange_time_v1"
            ):
                raise SharedPrefixExecutionError(
                    "shared-prefix native queue scope drifted"
                )
        strict_counter_baseline = self._validated_strict_counter_map(
            opportunity["strict_counter_baseline"],
            field="opportunity.strict_counter_baseline",
        )
        trace_cursor = opportunity["exchange_book_queue_missing_trace_cursor"]
        missing_at_assignment = opportunity[
            "exchange_book_queue_missing_count_at_assignment"
        ]
        for field, value in (
            ("exchange_book_queue_missing_trace_cursor", trace_cursor),
            (
                "exchange_book_queue_missing_count_at_assignment",
                missing_at_assignment,
            ),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise SharedPrefixExecutionError(f"opportunity.{field} is invalid")
        if (
            int(trace_cursor) != int(missing_at_assignment)
            or int(missing_at_assignment)
            != strict_counter_baseline["exchange_book_queue_missing_count"]
        ):
            raise SharedPrefixExecutionError(
                "shared-prefix missing-trace assignment cursor/count drifted"
            )
        prefix_source_failures = [
            name
            for name in STRICT_PREFIX_SOURCE_HARD_ZERO_FIELDS
            if strict_counter_baseline[name] != 0
        ]
        if self.require_strict_native and prefix_source_failures:
            raise SharedPrefixExecutionError(
                "shared-prefix source/clock hard-zero counters failed: "
                f"{prefix_source_failures}"
            )
        prefix_queue_failures = [
            name
            for name in STRICT_LABEL_UNSUPPORTED_FIELDS
            if strict_counter_baseline[name] != 0
        ]
        if self.require_strict_native and prefix_queue_failures:
            raise SharedPrefixExecutionError(
                "shared-prefix queue evidence is not exact before assignment: "
                f"{prefix_queue_failures}"
            )
        if (
            self.require_strict_native
            and strict_counter_baseline["exchange_book_events_consumed"] <= 0
        ):
            raise SharedPrefixExecutionError(
                "shared-prefix consumed no native book events"
            )
        normalized_opportunity = {
            key: opportunity[key]
            for key in sorted(required)
            if key != "strict_counter_baseline"
        }
        normalized_opportunity["strict_counter_baseline"] = (
            strict_counter_baseline
        )
        normalized_target_binding: dict[str, Any] | None = None
        if target_binding is not None:
            expected_owner_action = str(
                target_binding["expected_owner_action"]
            )
            observed_owner_action = str(
                opportunity.get("repeated_policy_action_id", "")
            )
            observed_owner_policy = str(
                opportunity.get("repeated_policy_policy_sha256", "")
            )
            if self.exact_owner_policy_sha256 is None:
                raise SharedPrefixExecutionError(
                    "target-bound execution lacks exact-owner policy identity"
                )
            if observed_owner_policy != self.exact_owner_policy_sha256:
                raise SharedPrefixExecutionError(
                    "shared-prefix exact-owner policy identity drifted"
                )
            if observed_owner_action != expected_owner_action:
                raise SharedPrefixExecutionError(
                    "shared-prefix exact-owner action drifted"
                )
            normalized_target_binding = {
                "opportunity_id": str(target_binding["opportunity_id"]),
                "expected_owner_action": expected_owner_action,
                "exact_owner_policy_sha256": self.exact_owner_policy_sha256,
                "arm_ids": list(target_binding["arm_ids"]),
            }
        body = {
            "schema_version": SCHEMA_VERSION,
            "identity": IDENTITY,
            "target_day": self.target_day,
            "source_contract_sha256": self.source_contract_sha256,
            "execution_identity_hashes": dict(self.execution_identity_hashes),
            "opportunity": normalized_opportunity,
            "target_binding": normalized_target_binding,
            "economic_evidence_mode": (
                "strict_native"
                if self.require_strict_native
                else (
                    "modeled_queue_with_same_millisecond_ambiguity_censoring"
                    if self.modeled_queue_economics_authorized
                    else "engineering_test_without_economic_labels"
                )
            ),
            "checkpoint_semantics": "posix_fork_copy_on_write_at_fill_callback",
            "portable_restore_authority": False,
            "economic_outcomes_read_before_fork": False,
        }
        return _canonical_sha256(body), body

    def _destination(self, identity_sha256: str) -> Path:
        return self.output_root / self.target_day / identity_sha256

    def _validate_arm_payload(
        self,
        payload: Any,
        *,
        expected_arm_id: str,
        expected_identity_sha256: str,
        expected_owner_action: str | None = None,
    ) -> None:
        if not isinstance(payload, dict):
            raise SharedPrefixExecutionError("arm result must be a JSON object")
        required_fields = {
            "schema_version",
            "identity",
            "opportunity_identity_sha256",
            "arm_id",
            "action",
            "fixed_duration_ms",
            "fork_trace",
            "prefix_execution_contract",
            "strict_execution_contract",
            "canonical_result_sha256",
        }
        if set(payload) != required_fields:
            raise SharedPrefixExecutionError("arm result schema drifted")
        if payload["schema_version"] != ARM_RESULT_SCHEMA_VERSION:
            raise SharedPrefixExecutionError("arm result schema version drifted")
        if payload["identity"] != IDENTITY:
            raise SharedPrefixExecutionError("arm result identity drifted")
        if payload["opportunity_identity_sha256"] != expected_identity_sha256:
            raise SharedPrefixExecutionError(
                "arm output shared-prefix identity drifted"
            )
        if payload["arm_id"] != expected_arm_id:
            raise SharedPrefixExecutionError("arm output identity drifted")
        expected_action = (
            "CONTROL_85N"
            if expected_arm_id == "CONTROL_85N"
            else "FIXED_DURATION_MS"
        )
        if payload["action"] != expected_action:
            raise SharedPrefixExecutionError("arm output action drifted")
        expected_duration = float(ARM_DURATION_MS[expected_arm_id] or 0)
        if float(payload["fixed_duration_ms"]) != expected_duration:
            raise SharedPrefixExecutionError("arm output duration drifted")
        if not isinstance(payload["fork_trace"], dict) or not payload["fork_trace"]:
            raise SharedPrefixExecutionError("arm result lacks fork trace")
        prefix = payload["prefix_execution_contract"]
        required_prefix = {
            "exchange_book_queue_mode",
            "exchange_book_queue_scope",
            "strict_native_required",
            "economic_evidence_mode",
            "exchange_book_queue_missing_trace_cursor",
            "exchange_book_queue_missing_count_at_assignment",
            *STRICT_COUNTER_FIELDS,
        }
        if not isinstance(prefix, dict) or set(prefix) != required_prefix:
            raise SharedPrefixExecutionError(
                "arm prefix execution schema drifted"
            )
        prefix_counters = self._validated_strict_counter_map(
            {name: prefix[name] for name in STRICT_COUNTER_FIELDS},
            field="arm.prefix_execution_contract",
        )
        if bool(prefix["strict_native_required"]) != self.require_strict_native:
            raise SharedPrefixExecutionError(
                "arm prefix strict-native requirement drifted"
            )
        if self.require_strict_native:
            if prefix["exchange_book_queue_mode"] != "strict":
                raise SharedPrefixExecutionError(
                    "arm prefix queue mode is not strict"
                )
            if prefix["exchange_book_queue_scope"] != STRICT_QUEUE_SCOPE:
                raise SharedPrefixExecutionError(
                    "arm prefix strict queue scope drifted"
                )
            if prefix_counters["exchange_book_events_consumed"] <= 0:
                raise SharedPrefixExecutionError(
                    "arm prefix consumed no native book events"
                )
            prefix_source_failures = [
                name
                for name in STRICT_PREFIX_SOURCE_HARD_ZERO_FIELDS
                if prefix_counters[name] != 0
            ]
            if prefix_source_failures:
                raise SharedPrefixExecutionError(
                    "arm prefix source/clock hard-zero counters failed: "
                    f"{prefix_source_failures}"
                )
            prefix_queue_failures = [
                name
                for name in STRICT_LABEL_UNSUPPORTED_FIELDS
                if prefix_counters[name] != 0
            ]
            if prefix_queue_failures:
                raise SharedPrefixExecutionError(
                    "arm prefix queue evidence is not exact: "
                    f"{prefix_queue_failures}"
                )
        if (
            prefix["exchange_book_queue_missing_trace_cursor"]
            != prefix["exchange_book_queue_missing_count_at_assignment"]
            or prefix["exchange_book_queue_missing_count_at_assignment"]
            != prefix_counters["exchange_book_queue_missing_count"]
        ):
            raise SharedPrefixExecutionError(
                "arm prefix missing-trace cursor/count drifted"
            )
        execution = payload["strict_execution_contract"]
        if not isinstance(execution, dict):
            raise SharedPrefixExecutionError("arm strict execution contract is invalid")
        required_execution = {
            "exchange_book_queue_mode",
            "exchange_book_queue_scope",
            "exchange_book_queue_ambiguity_trace",
            "exchange_book_queue_missing_trace",
            "strict_native_required",
            "strict_native_label_eligible",
            "strict_native_label_unsupported_reasons",
            "modeled_queue_label_eligible",
            "modeled_queue_label_unsupported_reasons",
            "economic_evidence_mode",
            "economic_point_label_status",
            *STRICT_COUNTER_FIELDS,
        }
        if set(execution) != required_execution:
            raise SharedPrefixExecutionError("arm strict execution schema drifted")
        ambiguity_trace = execution["exchange_book_queue_ambiguity_trace"]
        if not isinstance(ambiguity_trace, list) or len(ambiguity_trace) > 64:
            raise SharedPrefixExecutionError(
                "arm queue-ambiguity trace is invalid"
            )
        trace_fields = {
            "reason",
            "ambiguous",
            "event_ts_ms",
            "order_id",
            "side",
            "state",
            "price_tick",
            "activate_ts_ms",
            "cancel_ts_ms",
            "queue_seed_status",
        }
        for row in ambiguity_trace:
            if not isinstance(row, dict) or set(row) != trace_fields:
                raise SharedPrefixExecutionError(
                    "arm queue-ambiguity trace schema drifted"
                )
            if type(row["ambiguous"]) is not bool:
                raise SharedPrefixExecutionError(
                    "arm queue-ambiguity trace flag is invalid"
                )
            if row["event_ts_ms"] is not None and not isinstance(
                row["event_ts_ms"], int
            ):
                raise SharedPrefixExecutionError(
                    "arm queue-ambiguity event clock is invalid"
                )
            for name in (
                "order_id",
                "price_tick",
                "activate_ts_ms",
                "cancel_ts_ms",
            ):
                if isinstance(row[name], bool) or not isinstance(row[name], int):
                    raise SharedPrefixExecutionError(
                        f"arm queue-ambiguity {name} is invalid"
                    )
        missing_trace = self._validated_missing_trace(
            execution["exchange_book_queue_missing_trace"],
            field="arm.exchange_book_queue_missing_trace",
        )
        if len(missing_trace) != int(execution["exchange_book_queue_missing_count"]):
            raise SharedPrefixExecutionError(
                "arm missing-trace row count does not match treatment counter"
            )
        for row in ambiguity_trace:
            for name in ("reason", "side", "state", "queue_seed_status"):
                if not isinstance(row[name], str):
                    raise SharedPrefixExecutionError(
                        f"arm queue-ambiguity {name} is invalid"
                    )
        if bool(execution["strict_native_required"]) != self.require_strict_native:
            raise SharedPrefixExecutionError("arm strict-native requirement drifted")
        self._validated_strict_counter_map(
            {name: execution[name] for name in STRICT_COUNTER_FIELDS},
            field="arm.strict_execution_contract",
        )
        if self.require_strict_native:
            if execution["exchange_book_queue_mode"] != "strict":
                raise SharedPrefixExecutionError("arm queue mode is not strict")
            if execution["exchange_book_queue_scope"] != STRICT_QUEUE_SCOPE:
                raise SharedPrefixExecutionError("arm strict queue scope drifted")
            nonzero_hard = [
                name
                for name in STRICT_HARD_ZERO_FIELDS
                if int(execution[name]) != 0
            ]
            if nonzero_hard:
                raise SharedPrefixExecutionError(
                    "arm strict source/queue hard-zero counters failed: "
                    f"{nonzero_hard}"
                )
            expected_strict_unsupported = [
                name
                for name in STRICT_LABEL_UNSUPPORTED_FIELDS
                if int(execution[name]) != 0
            ]
            expected_modeled_unsupported = ["strict_native_mode"]
            expected_evidence_mode = "strict_native"
            expected_point_label_status = (
                "unsupported_redacted"
                if expected_strict_unsupported
                else "eligible"
            )
        elif self.modeled_queue_economics_authorized:
            expected_strict_unsupported = ["modeled_queue_not_strict_native"]
            expected_modeled_unsupported = []
            expected_evidence_mode = (
                "modeled_queue_with_same_millisecond_ambiguity_censoring"
            )
            expected_point_label_status = "eligible_modeled_queue_ambiguity_censored"
        else:
            expected_strict_unsupported = [
                "engineering_test_without_strict_native"
            ]
            expected_modeled_unsupported = [
                "engineering_test_without_economic_labels"
            ]
            expected_evidence_mode = "engineering_test_without_economic_labels"
            expected_point_label_status = "unsupported_redacted"
        if execution["strict_native_label_unsupported_reasons"] != (
            expected_strict_unsupported
        ):
            raise SharedPrefixExecutionError(
                "arm strict-label unsupported reasons drifted"
            )
        if bool(execution["strict_native_label_eligible"]) == bool(
            expected_strict_unsupported
        ):
            raise SharedPrefixExecutionError(
                "arm strict-label eligibility is inconsistent"
            )
        if execution["modeled_queue_label_unsupported_reasons"] != (
            expected_modeled_unsupported
        ):
            raise SharedPrefixExecutionError(
                "arm modeled-queue unsupported reasons drifted"
            )
        if bool(execution["modeled_queue_label_eligible"]) == bool(
            expected_modeled_unsupported
        ):
            raise SharedPrefixExecutionError(
                "arm modeled-queue eligibility is inconsistent"
            )
        if execution["economic_evidence_mode"] != expected_evidence_mode:
            raise SharedPrefixExecutionError(
                "arm economic evidence mode drifted"
            )
        if prefix["economic_evidence_mode"] != expected_evidence_mode:
            raise SharedPrefixExecutionError(
                "arm prefix economic evidence mode drifted"
            )
        if execution["economic_point_label_status"] != expected_point_label_status:
            raise SharedPrefixExecutionError(
                "arm economic point-label status is inconsistent"
            )
        fork_value = payload["fork_trace"].get(
            "assignment_to_washout_value_usdc"
        )
        if expected_point_label_status == "unsupported_redacted" and fork_value is not None:
            raise SharedPrefixExecutionError(
                "unsupported arm retained an economic point label"
            )
        if expected_point_label_status != "unsupported_redacted" and (
            not isinstance(fork_value, (int, float))
            or not math.isfinite(float(fork_value))
        ) and not bool(payload["fork_trace"].get("right_censored", False)):
            raise SharedPrefixExecutionError(
                "eligible completed arm lacks an economic point label"
            )
        if expected_owner_action is not None:
            trace = payload["fork_trace"]
            if (
                trace.get("exact_owner_baseline_policy_enabled") is not True
                or trace.get("exact_owner_action") != expected_owner_action
                or trace.get("exact_owner_policy_sha256")
                != self.exact_owner_policy_sha256
            ):
                raise SharedPrefixExecutionError(
                    "shared-prefix arm lost its exact-owner baseline identity"
                )
            if expected_arm_id == expected_owner_action and not math.isclose(
                float(trace.get("applied_duration_ms", math.nan)),
                float(trace.get("exact_owner_baseline_duration_ms", math.nan)),
                rel_tol=0.0,
                abs_tol=1e-9,
            ):
                raise SharedPrefixExecutionError(
                    "exact-owner no-op arm changed the target duration"
                )
        embedded_sha256 = _require_sha256(
            payload["canonical_result_sha256"],
            "canonical_result_sha256",
        )
        canonical_body = dict(payload)
        canonical_body.pop("canonical_result_sha256")
        if _canonical_sha256(canonical_body) != embedded_sha256:
            raise SharedPrefixExecutionError("arm canonical result SHA256 drifted")

    def _validate_completed_destination(
        self,
        destination: Path,
        *,
        expected_identity_sha256: str,
        expected_arms: Sequence[str],
        expected_owner_action: str | None = None,
    ) -> dict[str, Any]:
        success = destination / "_SUCCESS"
        manifest_path = destination / "manifest.json"
        if not success.is_file() or not manifest_path.is_file():
            raise SharedPrefixExecutionError(
                f"shared-prefix destination is incomplete: {destination}"
            )
        manifest = json.loads(manifest_path.read_text(encoding="ascii"))
        if manifest.get("schema_version") != OPPORTUNITY_MANIFEST_SCHEMA_VERSION:
            raise SharedPrefixExecutionError("opportunity manifest schema drifted")
        if manifest.get("opportunity_identity_sha256") != expected_identity_sha256:
            raise SharedPrefixExecutionError("opportunity manifest identity drifted")
        rows = manifest.get("arms")
        if not isinstance(rows, list):
            raise SharedPrefixExecutionError("opportunity arm manifest is invalid")
        actual_arms = tuple(str(row.get("arm_id")) for row in rows)
        if actual_arms != tuple(expected_arms) or len(set(actual_arms)) != len(
            actual_arms
        ):
            raise SharedPrefixExecutionError(
                "opportunity manifest does not contain exactly eight ordered arms "
                "or the frozen target arm subset"
            )
        timing = manifest.get("execution_timing")
        required_timing = {
            "supervisor_pid",
            "supervisor_started_at_utc",
            "supervisor_completed_at_utc",
            "supervisor_wall_time_s",
            "arm_wall_time_s_by_arm",
            "global_peak_concurrent_supervisors_observed",
            "global_peak_concurrent_arms_observed",
        }
        if not isinstance(timing, dict) or set(timing) != required_timing:
            raise SharedPrefixExecutionError(
                "opportunity execution timing schema drifted"
            )
        if int(timing["supervisor_pid"]) <= 0:
            raise SharedPrefixExecutionError("opportunity supervisor PID is invalid")
        for field in ("supervisor_started_at_utc", "supervisor_completed_at_utc"):
            parsed = datetime.fromisoformat(str(timing[field]))
            if parsed.tzinfo is None:
                raise SharedPrefixExecutionError(
                    "opportunity execution timing lacks timezone"
                )
        supervisor_wall_time_s = float(timing["supervisor_wall_time_s"])
        if not math.isfinite(supervisor_wall_time_s) or supervisor_wall_time_s < 0:
            raise SharedPrefixExecutionError(
                "opportunity supervisor wall time is invalid"
            )
        arm_timings = timing["arm_wall_time_s_by_arm"]
        if not isinstance(arm_timings, dict) or set(arm_timings) != set(
            expected_arms
        ):
            raise SharedPrefixExecutionError("opportunity arm timing identity drifted")
        peak_supervisors = int(
            timing["global_peak_concurrent_supervisors_observed"]
        )
        peak_arms = int(timing["global_peak_concurrent_arms_observed"])
        if not 1 <= peak_supervisors <= self.max_inflight_opportunity_snapshots:
            raise SharedPrefixExecutionError(
                "opportunity peak supervisor count is invalid"
            )
        if not 1 <= peak_arms <= self.max_parallel_arms:
            raise SharedPrefixExecutionError("opportunity peak arm count is invalid")
        expected_files = {"manifest.json", "_SUCCESS"} | {
            f"arm-{arm_id}.json" for arm_id in expected_arms
        }
        actual_files = {path.name for path in destination.iterdir()}
        if actual_files != expected_files:
            raise SharedPrefixExecutionError(
                "opportunity admission contains unexpected or missing files"
            )
        for row in rows:
            if set(row) != {
                "arm_id",
                "path",
                "size_bytes",
                "sha256",
                "wall_time_s",
            }:
                raise SharedPrefixExecutionError("opportunity arm row schema drifted")
            arm_id = str(row["arm_id"])
            arm_timing = arm_timings.get(arm_id)
            if not isinstance(arm_timing, dict) or set(arm_timing) != {
                "started_at_utc",
                "completed_at_utc",
                "wall_time_s",
            }:
                raise SharedPrefixExecutionError("opportunity arm timing schema drifted")
            for field in ("started_at_utc", "completed_at_utc"):
                parsed = datetime.fromisoformat(str(arm_timing[field]))
                if parsed.tzinfo is None:
                    raise SharedPrefixExecutionError(
                        "opportunity arm timing lacks timezone"
                    )
            arm_wall_time_s = float(arm_timing["wall_time_s"])
            if not math.isfinite(arm_wall_time_s) or arm_wall_time_s < 0:
                raise SharedPrefixExecutionError(
                    "opportunity arm wall time is invalid"
                )
            if float(row["wall_time_s"]) != arm_wall_time_s:
                raise SharedPrefixExecutionError(
                    "opportunity arm wall time drifted between manifest sections"
                )
            arm_path = destination / str(row["path"])
            if not arm_path.is_file():
                raise SharedPrefixExecutionError("admitted arm file is missing")
            if hashlib.sha256(arm_path.read_bytes()).hexdigest() != row["sha256"]:
                raise SharedPrefixExecutionError("admitted arm SHA256 drifted")
            payload = json.loads(arm_path.read_text(encoding="ascii"))
            self._validate_arm_payload(
                payload,
                expected_arm_id=str(row["arm_id"]),
                expected_identity_sha256=expected_identity_sha256,
                expected_owner_action=expected_owner_action,
            )
        success_payload = json.loads(success.read_text(encoding="ascii"))
        if success_payload.get("manifest_sha256") != hashlib.sha256(
            manifest_path.read_bytes()
        ).hexdigest():
            raise SharedPrefixExecutionError("opportunity success marker drifted")
        return manifest

    def _wait_one(
        self,
        pending: dict[int, _PendingArm],
        arm_timings: dict[str, dict[str, Any]],
    ) -> None:
        pid, status = os.waitpid(-1, 0)
        arm = pending.pop(pid, None)
        if arm is None:
            raise SharedPrefixExecutionError("waited for an unknown arm process")
        completed_ns = time.monotonic_ns()
        completed_at_utc = datetime.now(UTC).isoformat()
        try:
            self._mutate_pool_metrics(remove_arm_pid=pid)
        finally:
            self._release_global_arm_slot(arm.slot_fd)
        arm_timings[arm.arm_id] = {
            "started_at_utc": arm.started_at_utc,
            "completed_at_utc": completed_at_utc,
            "wall_time_s": (completed_ns - arm.started_ns) / 1_000_000_000.0,
        }
        if not os.WIFEXITED(status) or os.WEXITSTATUS(status) != 0:
            raise SharedPrefixExecutionError(
                f"shared-prefix arm {arm.arm_id} exited unsuccessfully"
            )
        self._arm_processes_completed += 1

    def _terminate_pending_arms(self, pending: dict[int, _PendingArm]) -> None:
        for pid in tuple(pending):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        for pid, arm in tuple(pending.items()):
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass
            try:
                self._mutate_pool_metrics(remove_arm_pid=pid)
            finally:
                self._release_global_arm_slot(arm.slot_fd)
            pending.pop(pid, None)

    def _run_supervisor(
        self,
        *,
        opportunity_identity_sha256: str,
        opportunity_body: Mapping[str, Any],
        arms: Sequence[str],
        staging: Path,
        destination: Path,
    ) -> SharedPrefixArmSelection | None:
        supervisor_pid = os.getpid()
        supervisor_started_ns = time.monotonic_ns()
        supervisor_started_at_utc = datetime.now(UTC).isoformat()
        pending: dict[int, _PendingArm] = {}
        arm_timings: dict[str, dict[str, Any]] = {}

        def terminate_supervisor(_signum: int, _frame: Any) -> None:
            self._terminate_pending_arms(pending)
            os._exit(72)

        signal.signal(signal.SIGTERM, terminate_supervisor)
        signal.signal(signal.SIGINT, terminate_supervisor)
        try:
            for arm_id in arms:
                while len(pending) >= self.max_parallel_arms:
                    self._wait_one(pending, arm_timings)
                slot_fd = self._try_acquire_global_slot(
                    prefix="arm",
                    count=self.max_parallel_arms,
                )
                while slot_fd is None:
                    if pending:
                        self._wait_one(pending, arm_timings)
                    else:
                        time.sleep(_POOL_POLL_INTERVAL_S)
                    slot_fd = self._try_acquire_global_slot(
                        prefix="arm",
                        count=self.max_parallel_arms,
                    )
                started_ns = time.monotonic_ns()
                started_at_utc = datetime.now(UTC).isoformat()
                try:
                    pid = os.fork()
                except BaseException:
                    self._release_global_arm_slot(slot_fd)
                    raise
                if pid == 0:
                    signal.signal(signal.SIGTERM, signal.SIG_DFL)
                    signal.signal(signal.SIGINT, signal.SIG_DFL)
                    for inherited_arm in pending.values():
                        os.close(inherited_arm.slot_fd)
                    self._role = "arm_child"
                    action = (
                        "CONTROL_85N"
                        if arm_id == "CONTROL_85N"
                        else "FIXED_DURATION_MS"
                    )
                    duration = ARM_DURATION_MS[arm_id]
                    strict_counter_baseline = self._validated_strict_counter_map(
                        opportunity_body["opportunity"][
                            "strict_counter_baseline"
                        ],
                        field="opportunity.strict_counter_baseline",
                    )
                    self._selection = SharedPrefixArmSelection(
                        arm_id=str(arm_id),
                        action=action,
                        fixed_duration_ms=float(duration or 0),
                        opportunity_identity_sha256=opportunity_identity_sha256,
                        strict_counter_baseline=tuple(
                            (name, strict_counter_baseline[name])
                            for name in STRICT_COUNTER_FIELDS
                        ),
                        exchange_book_queue_missing_trace_cursor=int(
                            opportunity_body["opportunity"][
                                "exchange_book_queue_missing_trace_cursor"
                            ]
                        ),
                        exchange_book_queue_missing_count_at_assignment=int(
                            opportunity_body["opportunity"][
                                "exchange_book_queue_missing_count_at_assignment"
                            ]
                        ),
                        exact_owner_action=(
                            None
                            if opportunity_body.get("target_binding") is None
                            else str(
                                opportunity_body["target_binding"][
                                    "expected_owner_action"
                                ]
                            )
                        ),
                        exact_owner_policy_sha256=(
                            None
                            if opportunity_body.get("target_binding") is None
                            else str(
                                opportunity_body["target_binding"][
                                    "exact_owner_policy_sha256"
                                ]
                            )
                        ),
                    )
                    self._arm_output_path = staging / f"arm-{arm_id}.json"
                    return self._selection
                try:
                    self._mutate_pool_metrics(add_arm=(pid, supervisor_pid))
                except BaseException:
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    os.waitpid(pid, 0)
                    self._release_global_arm_slot(slot_fd)
                    raise
                pending[pid] = _PendingArm(
                    arm_id=str(arm_id),
                    slot_fd=slot_fd,
                    started_ns=started_ns,
                    started_at_utc=started_at_utc,
                )
            while pending:
                self._wait_one(pending, arm_timings)
        except BaseException:
            self._terminate_pending_arms(pending)
            raise

        rows = []
        for arm_id in arms:
            path = staging / f"arm-{arm_id}.json"
            if not path.is_file():
                raise SharedPrefixExecutionError(
                    f"shared-prefix arm output is missing: {arm_id}"
                )
            payload = json.loads(path.read_text(encoding="ascii"))
            self._validate_arm_payload(
                payload,
                expected_arm_id=str(arm_id),
                expected_identity_sha256=opportunity_identity_sha256,
                expected_owner_action=(
                    None
                    if opportunity_body.get("target_binding") is None
                    else str(
                        opportunity_body["target_binding"][
                            "expected_owner_action"
                        ]
                    )
                ),
            )
            rows.append(
                {
                    "arm_id": str(arm_id),
                    "path": path.name,
                    "size_bytes": int(path.stat().st_size),
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "wall_time_s": float(arm_timings[str(arm_id)]["wall_time_s"]),
                }
            )
        supervisor_completed_ns = time.monotonic_ns()
        supervisor_completed_at_utc = datetime.now(UTC).isoformat()
        global_metrics = self._read_pool_metrics()
        manifest = {
            "schema_version": OPPORTUNITY_MANIFEST_SCHEMA_VERSION,
            "identity": IDENTITY,
            "opportunity_identity_sha256": opportunity_identity_sha256,
            "opportunity_contract": dict(opportunity_body),
            "arm_count": len(rows),
            "arms": rows,
            "all_arms_share_one_posix_cow_prefix": True,
            "max_parallel_arms": self.max_parallel_arms,
            "execution_timing": {
                "supervisor_pid": supervisor_pid,
                "supervisor_started_at_utc": supervisor_started_at_utc,
                "supervisor_completed_at_utc": supervisor_completed_at_utc,
                "supervisor_wall_time_s": (
                    supervisor_completed_ns - supervisor_started_ns
                )
                / 1_000_000_000.0,
                "arm_wall_time_s_by_arm": {
                    arm_id: dict(arm_timings[arm_id]) for arm_id in arms
                },
                "global_peak_concurrent_supervisors_observed": int(
                    global_metrics["peak_concurrent_supervisors"]
                ),
                "global_peak_concurrent_arms_observed": int(
                    global_metrics["peak_concurrent_arms"]
                ),
            },
            "portable_restore_authority": False,
            "atomic_admission": True,
        }
        _write_atomic_json(staging / "manifest.json", manifest)
        manifest_sha = hashlib.sha256(
            (staging / "manifest.json").read_bytes()
        ).hexdigest()
        _write_atomic_json(
            staging / "_SUCCESS",
            {
                "schema_version": OPPORTUNITY_MANIFEST_SCHEMA_VERSION,
                "opportunity_identity_sha256": opportunity_identity_sha256,
                "manifest_sha256": manifest_sha,
            },
        )
        if destination.exists():
            raise SharedPrefixExecutionError(
                "shared-prefix destination appeared during admission"
            )
        os.replace(staging, destination)
        _fsync_directory(destination.parent)
        return None

    def _validate_completed_destination_isolated(
        self,
        destination: Path,
        *,
        expected_identity_sha256: str,
        expected_arms: Sequence[str],
        expected_owner_action: str | None = None,
    ) -> None:
        read_fd, write_fd = os.pipe()
        validator_pid = os.fork()
        if validator_pid == 0:
            os.close(read_fd)
            self._role = "validation_child"
            try:
                self._validate_completed_destination(
                    destination,
                    expected_identity_sha256=expected_identity_sha256,
                    expected_arms=expected_arms,
                    expected_owner_action=expected_owner_action,
                )
            except BaseException as exc:
                message = str(exc).encode("ascii", errors="replace")[:4_096]
                try:
                    os.write(write_fd, message)
                finally:
                    os.close(write_fd)
                os._exit(76)
            os.close(write_fd)
            os._exit(0)
        os.close(write_fd)
        _, status = os.waitpid(validator_pid, 0)
        message = os.read(read_fd, 4_096).decode("ascii", errors="replace")
        os.close(read_fd)
        if not os.WIFEXITED(status) or os.WEXITSTATUS(status) != 0:
            raise SharedPrefixExecutionError(
                message or "isolated shared-prefix admission validation failed"
            )

    def _record_completed_supervisor(self, job: _PendingSupervisor) -> None:
        self._validate_completed_destination_isolated(
            job.destination,
            expected_identity_sha256=job.opportunity_identity_sha256,
            expected_arms=job.arms,
            expected_owner_action=job.expected_owner_action,
        )
        manifest_path = job.destination / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="ascii"))
        timing = manifest["execution_timing"]
        self._supervisor_wall_time_s_total += float(
            timing["supervisor_wall_time_s"]
        )
        self._arm_wall_time_s_total += sum(
            float(row["wall_time_s"])
            for row in timing["arm_wall_time_s_by_arm"].values()
        )
        self._opportunities_dispatched += 1
        self._supervisor_processes_completed += 1
        self._arm_processes_completed += len(job.arms)
        self._completed_manifests[job.opportunity_index] = str(manifest_path)
        if self.progress is not None:
            self.progress(job.opportunity_index, manifest_path, False)

    def _abort_pending_supervisors(self) -> None:
        jobs = tuple(self._pending_supervisors.values())
        for job in jobs:
            try:
                os.kill(job.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        for job in jobs:
            try:
                os.waitpid(job.pid, 0)
            except ChildProcessError:
                pass
            self._mutate_pool_metrics(remove_supervisor_pid=job.pid)
            self._pending_supervisors.pop(job.pid, None)

    @staticmethod
    def _supervisor_failure_detail(job: _PendingSupervisor) -> str:
        error_path = job.staging / "_ERROR.json"
        if not error_path.is_file():
            return "shared-prefix supervisor failed; partial staging retained"
        try:
            payload = json.loads(error_path.read_text(encoding="ascii"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return "shared-prefix supervisor failed; unreadable error record retained"
        if payload.get("schema_version") != SUPERVISOR_ERROR_SCHEMA_VERSION:
            return "shared-prefix supervisor failed; error record schema drifted"
        error_type = str(payload.get("error_type", "supervisor_error"))
        error = str(payload.get("error", "unknown supervisor failure"))
        return f"shared-prefix supervisor failed: {error_type}: {error}"

    def _reap_one_supervisor(self, *, block: bool) -> bool:
        while self._pending_supervisors:
            for pid, job in tuple(self._pending_supervisors.items()):
                waited_pid, status = os.waitpid(pid, os.WNOHANG)
                if waited_pid == 0:
                    continue
                self._pending_supervisors.pop(pid)
                self._mutate_pool_metrics(remove_supervisor_pid=pid)
                if not os.WIFEXITED(status) or os.WEXITSTATUS(status) != 0:
                    self._abort_pending_supervisors()
                    raise SharedPrefixExecutionError(
                        self._supervisor_failure_detail(job)
                    )
                try:
                    self._record_completed_supervisor(job)
                except BaseException:
                    self._abort_pending_supervisors()
                    raise
                return True
            if not block:
                return False
            time.sleep(_POOL_POLL_INTERVAL_S)
        return False

    def _reap_available_supervisors(self) -> None:
        while self._reap_one_supervisor(block=False):
            pass

    def _drain_supervisors(self) -> None:
        while self._pending_supervisors:
            self._reap_one_supervisor(block=True)

    @staticmethod
    def _pid_is_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _recover_interrupted_artifacts(
        self,
        *,
        destination: Path,
        identity_sha256: str,
        lock_path: Path,
    ) -> None:
        stale_staging = tuple(
            destination.parent.glob(f".{identity_sha256}.staging.*")
        )
        if not lock_path.exists() and not stale_staging:
            return
        if not self.recover_interrupted_staging:
            if lock_path.exists():
                raise SharedPrefixExecutionError(
                    "shared-prefix opportunity lock exists; refusing concurrent execution"
                )
            raise SharedPrefixExecutionError(
                "stale shared-prefix staging exists; refusing implicit recovery"
            )
        if lock_path.exists():
            try:
                lock = json.loads(lock_path.read_text(encoding="ascii"))
                owner_pid = int(lock.get("owner_pid", -1))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
                owner_pid = -1
            if self._pid_is_alive(owner_pid):
                raise SharedPrefixExecutionError(
                    "shared-prefix opportunity is owned by a live process"
                )
        quarantine = (
            self.output_root
            / "_interrupted"
            / self.target_day
            / identity_sha256
            / uuid.uuid4().hex
        )
        quarantine.mkdir(parents=True, exist_ok=False)
        if lock_path.exists():
            os.replace(lock_path, quarantine / "lock.json")
        for index, path in enumerate(stale_staging):
            os.replace(path, quarantine / f"staging-{index}")
        _write_atomic_json(
            quarantine / "recovery.json",
            {
                "schema_version": f"{SCHEMA_VERSION}.interrupted_recovery.v1",
                "opportunity_identity_sha256": identity_sha256,
                "recovered_at_utc": datetime.now(UTC).isoformat(),
                "prior_lock_present": (quarantine / "lock.json").exists(),
                "prior_staging_count": len(stale_staging),
                "economic_result_admitted": False,
            },
        )
        _fsync_directory(destination.parent)

    def abort(self) -> None:
        """Terminate in-flight supervisors without touching admitted shards."""

        if self._role == "baseline_parent":
            self._abort_pending_supervisors()

    def dispatch(
        self,
        opportunity: Mapping[str, Any],
    ) -> SharedPrefixArmSelection | None:
        """Retain one COW snapshot and let the baseline parent keep replaying."""

        if self._role != "baseline_parent":
            raise SharedPrefixExecutionError(
                "an arm attempted a second shared-prefix assignment"
            )
        self._reap_available_supervisors()
        while (
            len(self._pending_supervisors)
            >= self.max_inflight_opportunity_snapshots
        ):
            self._reap_one_supervisor(block=True)
        fill_day = datetime.fromtimestamp(
            int(opportunity.get("fill_visible_ts_ms", 0)) / 1_000.0,
            tz=UTC,
        ).date().isoformat()
        if fill_day != self.target_day:
            self._opportunities_skipped_outside_target_day += 1
            return None
        target_key = _target_key(opportunity)
        target_binding = (
            None
            if self._target_opportunities is None
            else self._target_opportunities.get(target_key)
        )
        if self._target_opportunities is not None and target_binding is None:
            self._opportunities_skipped_outside_target_set += 1
            return None
        if target_key in self._target_opportunity_keys_seen:
            raise SharedPrefixExecutionError(
                "shared-prefix target opportunity was observed twice"
            )
        completed = (
            self._opportunities_dispatched
            + self._opportunities_resumed
            + len(self._pending_supervisors)
        )
        if self.max_opportunities is not None and completed >= self.max_opportunities:
            self._opportunities_skipped_after_limit += 1
            return None
        identity_sha256, body = self._opportunity_identity(
            opportunity,
            target_binding=target_binding,
        )
        side = str(opportunity["side"]).upper()
        arms = (
            BUY_ARMS if side == "BUY" else SELL_ARMS
        ) if target_binding is None else tuple(target_binding["arm_ids"])
        expected_owner_action = (
            None
            if target_binding is None
            else str(target_binding["expected_owner_action"])
        )
        self._target_opportunity_keys_seen.add(target_key)
        destination = self._destination(identity_sha256)
        if destination.exists():
            self._validate_completed_destination_isolated(
                destination,
                expected_identity_sha256=identity_sha256,
                expected_arms=arms,
                expected_owner_action=expected_owner_action,
            )
            self._opportunities_resumed += 1
            opportunity_index = completed + 1
            self._completed_manifests[opportunity_index] = str(
                destination / "manifest.json"
            )
            if self.progress is not None:
                self.progress(opportunity_index, destination / "manifest.json", True)
            return None

        destination.parent.mkdir(parents=True, exist_ok=True)
        lock_path = destination.parent / f".{identity_sha256}.lock"
        self._recover_interrupted_artifacts(
            destination=destination,
            identity_sha256=identity_sha256,
            lock_path=lock_path,
        )
        lock_fd: int | None = None
        handed_off = False
        try:
            lock_fd = os.open(
                lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError as exc:
            raise SharedPrefixExecutionError(
                "shared-prefix opportunity lock exists; refusing concurrent execution"
            ) from exc
        try:
            lock_payload = _canonical_bytes(
                {
                    "schema_version": SCHEMA_VERSION,
                    "opportunity_identity_sha256": identity_sha256,
                    "owner_pid": os.getpid(),
                    "run_id": self._pool_run_id,
                }
            )
            os.write(lock_fd, lock_payload)
            os.fsync(lock_fd)
            _fsync_directory(destination.parent)

            if destination.exists():
                self._validate_completed_destination_isolated(
                    destination,
                    expected_identity_sha256=identity_sha256,
                    expected_arms=arms,
                    expected_owner_action=expected_owner_action,
                )
                self._opportunities_resumed += 1
                opportunity_index = completed + 1
                self._completed_manifests[opportunity_index] = str(
                    destination / "manifest.json"
                )
                if self.progress is not None:
                    self.progress(
                        opportunity_index,
                        destination / "manifest.json",
                        True,
                    )
                return None

            stale_staging = tuple(
                destination.parent.glob(f".{identity_sha256}.staging.*")
            )
            if stale_staging:
                raise SharedPrefixExecutionError(
                    "stale shared-prefix staging exists; refusing implicit recovery"
                )
            staging = destination.parent / (
                f".{identity_sha256}.staging.{os.getpid()}.{uuid.uuid4().hex}"
            )
            staging.mkdir()
            _fsync_directory(destination.parent)
            self._ensure_global_pool()
            supervisor_slot_fd = self._acquire_global_supervisor_slot()
            try:
                supervisor_pid = os.fork()
            except BaseException:
                self._release_global_slot(supervisor_slot_fd)
                raise
            if supervisor_pid == 0:
                self._role = "supervisor"
                selection: SharedPrefixArmSelection | None = None
                exit_code = 0
                try:
                    self._mutate_pool_metrics(add_supervisor_pid=os.getpid())
                    selection = self._run_supervisor(
                        opportunity_identity_sha256=identity_sha256,
                        opportunity_body=body,
                        arms=arms,
                        staging=staging,
                        destination=destination,
                    )
                except BaseException as exc:
                    exit_code = 73
                    try:
                        _write_atomic_json(
                            staging / "_ERROR.json",
                            {
                                "schema_version": SUPERVISOR_ERROR_SCHEMA_VERSION,
                                "opportunity_identity_sha256": identity_sha256,
                                "supervisor_pid": os.getpid(),
                                "error_type": type(exc).__name__,
                                "error": str(exc),
                                "traceback": traceback.format_exc()[-16_384:],
                            },
                        )
                    except BaseException:
                        pass
                if selection is not None:
                    os.close(supervisor_slot_fd)
                    handed_off = True
                    return selection
                try:
                    self._mutate_pool_metrics(remove_supervisor_pid=os.getpid())
                except BaseException:
                    exit_code = 73
                try:
                    os.close(lock_fd)
                    lock_path.unlink(missing_ok=True)
                    _fsync_directory(destination.parent)
                except BaseException:
                    exit_code = 73
                try:
                    self._release_global_slot(supervisor_slot_fd)
                except BaseException:
                    exit_code = 73
                os._exit(exit_code)
            os.close(supervisor_slot_fd)
            handed_off = True
            opportunity_index = completed + 1
            self._pending_supervisors[supervisor_pid] = _PendingSupervisor(
                pid=supervisor_pid,
                opportunity_index=opportunity_index,
                opportunity_identity_sha256=identity_sha256,
                destination=destination,
                staging=staging,
                arms=tuple(arms),
                expected_owner_action=expected_owner_action,
            )
            self._peak_concurrent_supervisors = max(
                self._peak_concurrent_supervisors,
                len(self._pending_supervisors),
            )
            return None
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
            if not handed_off:
                lock_path.unlink(missing_ok=True)
                _fsync_directory(destination.parent)

    def finalize_simulation_result(self, result: Mapping[str, Any]) -> None:
        """Atomically persist a compact arm result and terminate that child."""

        if not self.is_arm_child:
            return
        selection = self._selection
        output_path = self._arm_output_path
        if selection is None or output_path is None:
            os._exit(74)
        try:
            fork_trace = result.get("_cooldown_duration_fork_trace")
            if not isinstance(fork_trace, Mapping) or not fork_trace:
                raise SharedPrefixExecutionError("arm result lacks fork trace")
            if str(fork_trace.get("action")) != selection.action:
                raise SharedPrefixExecutionError("arm action/result drifted")
            strict_counter_baseline = dict(selection.strict_counter_baseline)
            if set(strict_counter_baseline) != set(STRICT_COUNTER_FIELDS):
                raise SharedPrefixExecutionError(
                    "arm selection strict counter baseline drifted"
                )
            cumulative_counters = {
                name: int(result.get(name, 0) or 0)
                for name in STRICT_COUNTER_FIELDS
            }
            treatment_counters: dict[str, int] = {}
            for name in STRICT_COUNTER_FIELDS:
                baseline_value = int(strict_counter_baseline[name])
                cumulative_value = int(cumulative_counters[name])
                if cumulative_value < baseline_value:
                    raise SharedPrefixExecutionError(
                        "arm strict counter moved backwards after assignment: "
                        f"{name}"
                    )
                treatment_counters[name] = cumulative_value - baseline_value
            cumulative_missing_trace = self._validated_missing_trace(
                result.get("_exchange_book_queue_missing_trace"),
                field="result._exchange_book_queue_missing_trace",
            )
            cumulative_missing_count = cumulative_counters[
                "exchange_book_queue_missing_count"
            ]
            if len(cumulative_missing_trace) != cumulative_missing_count:
                raise SharedPrefixExecutionError(
                    "cumulative missing trace is truncated or inconsistent"
                )
            trace_cursor = selection.exchange_book_queue_missing_trace_cursor
            missing_at_assignment = (
                selection.exchange_book_queue_missing_count_at_assignment
            )
            if (
                trace_cursor != missing_at_assignment
                or missing_at_assignment
                != strict_counter_baseline["exchange_book_queue_missing_count"]
                or trace_cursor > len(cumulative_missing_trace)
            ):
                raise SharedPrefixExecutionError(
                    "arm missing-trace assignment cursor/count drifted"
                )
            treatment_missing_trace = cumulative_missing_trace[trace_cursor:]
            if len(treatment_missing_trace) != treatment_counters[
                "exchange_book_queue_missing_count"
            ]:
                raise SharedPrefixExecutionError(
                    "treatment missing trace does not match treatment counter"
                )
            prefix_execution_contract = {
                "exchange_book_queue_mode": str(
                    result.get("exchange_book_queue_mode", "")
                ),
                "exchange_book_queue_scope": str(
                    result.get("exchange_book_queue_scope", "")
                ),
                **strict_counter_baseline,
                "exchange_book_queue_missing_trace_cursor": trace_cursor,
                "exchange_book_queue_missing_count_at_assignment": (
                    missing_at_assignment
                ),
                "strict_native_required": self.require_strict_native,
                "economic_evidence_mode": (
                    "strict_native"
                    if self.require_strict_native
                    else (
                        "modeled_queue_with_same_millisecond_ambiguity_censoring"
                        if self.modeled_queue_economics_authorized
                        else "engineering_test_without_economic_labels"
                    )
                ),
            }
            execution_contract = {
                "exchange_book_queue_mode": str(
                    result.get("exchange_book_queue_mode", "")
                ),
                "exchange_book_queue_scope": str(
                    result.get("exchange_book_queue_scope", "")
                ),
                **treatment_counters,
                "exchange_book_queue_missing_trace": treatment_missing_trace,
                "exchange_book_queue_ambiguity_trace": [
                    dict(row)
                    for row in result.get(
                        "_exchange_book_queue_ambiguity_trace", ()
                    )
                ],
                "strict_native_required": self.require_strict_native,
            }
            strict_unsupported_reasons = (
                [
                    name
                    for name in STRICT_LABEL_UNSUPPORTED_FIELDS
                    if int(execution_contract[name]) != 0
                ]
                if self.require_strict_native
                else (
                    ["modeled_queue_not_strict_native"]
                    if self.modeled_queue_economics_authorized
                    else ["engineering_test_without_strict_native"]
                )
            )
            modeled_unsupported_reasons = (
                []
                if self.modeled_queue_economics_authorized
                else (
                    ["strict_native_mode"]
                    if self.require_strict_native
                    else ["engineering_test_without_economic_labels"]
                )
            )
            economic_evidence_mode = str(
                prefix_execution_contract["economic_evidence_mode"]
            )
            point_label_eligible = bool(
                (self.require_strict_native and not strict_unsupported_reasons)
                or self.modeled_queue_economics_authorized
            )
            execution_contract.update(
                {
                    "strict_native_label_eligible": not strict_unsupported_reasons,
                    "strict_native_label_unsupported_reasons": (
                        strict_unsupported_reasons
                    ),
                    "modeled_queue_label_eligible": (
                        self.modeled_queue_economics_authorized
                    ),
                    "modeled_queue_label_unsupported_reasons": (
                        modeled_unsupported_reasons
                    ),
                    "economic_evidence_mode": economic_evidence_mode,
                    "economic_point_label_status": (
                        (
                            "eligible"
                            if self.require_strict_native
                            else "eligible_modeled_queue_ambiguity_censored"
                        )
                        if point_label_eligible
                        else "unsupported_redacted"
                    ),
                }
            )
            stored_fork_trace = dict(fork_trace)
            if not point_label_eligible:
                stored_fork_trace["assignment_to_washout_value_usdc"] = None
            payload = {
                "schema_version": ARM_RESULT_SCHEMA_VERSION,
                "identity": IDENTITY,
                "opportunity_identity_sha256": (
                    selection.opportunity_identity_sha256
                ),
                "arm_id": selection.arm_id,
                "action": selection.action,
                "fixed_duration_ms": selection.fixed_duration_ms,
                "fork_trace": stored_fork_trace,
                "prefix_execution_contract": prefix_execution_contract,
                "strict_execution_contract": execution_contract,
            }
            payload["canonical_result_sha256"] = _canonical_sha256(payload)
            _write_atomic_json(output_path, payload)
        except BaseException:
            os._exit(75)
        os._exit(0)

    def _audit_snapshot(
        self,
        pool_metrics: Mapping[str, Any],
    ) -> SharedPrefixExecutionAudit:
        peak_supervisors = max(
            self._peak_concurrent_supervisors,
            int(pool_metrics["peak_concurrent_supervisors"]),
        )
        peak_arms = int(pool_metrics["peak_concurrent_arms"])
        if peak_supervisors > self.max_inflight_opportunity_snapshots:
            raise SharedPrefixExecutionError(
                "global supervisor pool exceeded its frozen bound"
            )
        if peak_arms > self.max_parallel_arms:
            raise SharedPrefixExecutionError(
                "global arm pool exceeded its frozen bound"
            )
        return SharedPrefixExecutionAudit(
            opportunities_dispatched=int(self._opportunities_dispatched),
            opportunities_resumed=int(self._opportunities_resumed),
            opportunities_skipped_after_limit=int(
                self._opportunities_skipped_after_limit
            ),
            opportunities_skipped_outside_target_day=int(
                self._opportunities_skipped_outside_target_day
            ),
            arm_processes_completed=int(self._arm_processes_completed),
            max_parallel_arms=self.max_parallel_arms,
            completed_manifest_paths=tuple(
                self._completed_manifests[index]
                for index in sorted(self._completed_manifests)
            ),
            supervisor_processes_completed=int(
                self._supervisor_processes_completed
            ),
            executor_wall_time_s=(
                time.monotonic_ns() - self._executor_started_ns
            )
            / 1_000_000_000.0,
            supervisor_wall_time_s_total=float(
                self._supervisor_wall_time_s_total
            ),
            arm_wall_time_s_total=float(self._arm_wall_time_s_total),
            peak_concurrent_supervisors=peak_supervisors,
            peak_concurrent_arms=peak_arms,
            max_inflight_opportunity_snapshots=(
                self.max_inflight_opportunity_snapshots
            ),
            pending_supervisors=len(self._pending_supervisors),
            target_opportunity_count=(
                0
                if self._target_opportunities is None
                else len(self._target_opportunities)
            ),
            target_opportunities_matched=len(
                self._target_opportunity_keys_seen
            ),
            opportunities_skipped_outside_target_set=int(
                self._opportunities_skipped_outside_target_set
            ),
            modeled_queue_economics_authorized=(
                self.modeled_queue_economics_authorized
            ),
            exact_owner_baseline_policy_enabled=(
                self.exact_owner_baseline_policy_enabled
            ),
        )

    def audit(self) -> SharedPrefixExecutionAudit:
        if self.is_arm_child:
            return self._audit_snapshot(self._read_pool_metrics())
        if self._role != "baseline_parent":
            raise SharedPrefixExecutionError(
                "only the baseline parent or an arm child may read shared-prefix audit"
            )
        self._drain_supervisors()
        pool_metrics = self._read_pool_metrics()
        if pool_metrics["active_supervisor_pids"] or pool_metrics["active_arm_pids"]:
            raise SharedPrefixExecutionError(
                "global child pool was not empty after supervisor drain"
            )
        if self._target_opportunities is not None and (
            self._target_opportunity_keys_seen
            != set(self._target_opportunities)
        ):
            missing = len(
                set(self._target_opportunities)
                - self._target_opportunity_keys_seen
            )
            raise SharedPrefixExecutionError(
                f"shared-prefix parent missed {missing} frozen target opportunities"
            )
        return self._audit_snapshot(pool_metrics)


__all__ = [
    "ARM_RESULT_SCHEMA_VERSION",
    "OPPORTUNITY_MANIFEST_SCHEMA_VERSION",
    "MAX_GLOBAL_ARM_PROCESSES",
    "MAX_INFLIGHT_OPPORTUNITY_SNAPSHOTS",
    "PosixCooldownSharedPrefixExecutor",
    "SCHEMA_VERSION",
    "STRICT_COUNTER_FIELDS",
    "STRICT_HARD_ZERO_FIELDS",
    "STRICT_LABEL_UNSUPPORTED_FIELDS",
    "STRICT_PREFIX_SOURCE_HARD_ZERO_FIELDS",
    "SharedPrefixArmSelection",
    "SharedPrefixExecutionAudit",
    "SharedPrefixExecutionError",
]
