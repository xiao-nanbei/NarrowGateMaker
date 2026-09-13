"""Restart-aware paired execution orchestration with atomic checkpoints.

The module is intentionally strategy-agnostic.  A bound adapter owns the
market/order simulation while this layer owns the shared calendar clock,
restart operations, arm isolation, authority tiers, and durable resume.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from data_paths import external_cache_root
from models.replay.replay_state_checkpoint import (
    RESTART_RESET_FIELDS,
    ContinuousReplayState,
)
from models.replay.replay_state_checkpoint import (
    canonical_sha256 as state_sha256,
)
from models.replay.restart_aware_continuous_ab import (
    DAY_MS,
    CalendarSourceBinding,
    ContinuousABPlan,
    canonical_sha256,
    day_start_ms,
    sha256_file,
)

SCHEMA_VERSION = "restart_aware_continuous_execution.v1"
OPERATION_SCHEMA_VERSION = f"{SCHEMA_VERSION}.operation"
CHECKPOINT_SCHEMA_VERSION = f"{SCHEMA_VERSION}.checkpoint"
PROGRESS_SCHEMA_VERSION = f"{SCHEMA_VERSION}.progress"
OPERATION_KINDS = frozenset(
    {
        "online",
        "cancel_drain",
        "offline_gap",
        "warmup_resume",
        "utc_accounting",
        "panel_terminal",
    }
)
NO_QUOTING_KINDS = frozenset(
    {"cancel_drain", "offline_gap", "warmup_resume", "utc_accounting", "panel_terminal"}
)
NO_FILL_KINDS = frozenset({"offline_gap", "warmup_resume", "utc_accounting", "panel_terminal"})
ROOT = Path(__file__).resolve().parents[2]


class ContinuousExecutionError(RuntimeError):
    """Raised before an invalid continuous execution can advance."""


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="ascii") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _stable_seed(identity_sha256: str, operation_id: str, base_seed: int) -> int:
    payload = f"{identity_sha256}:{operation_id}:{int(base_seed)}".encode("ascii")
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big") & 0x7FFF_FFFF


@dataclass(frozen=True, slots=True)
class ContinuousOperation:
    sequence: int
    operation_id: str
    kind: str
    day: str
    start_ts_ms: int
    end_ts_ms: int
    source_day: str
    gap_id: str
    warmup_lookback_start_ts_ms: int | None
    exact_queue_authority: bool
    exact_lifecycle_authority: bool
    continuous_economic_sensitivity_authority: bool
    source_identity_sha256: str
    source_artifact_manifest_sha256: str
    restart_timeline_sha256: str
    random_seed: int
    random_path_sha256: str

    def validate(self) -> None:
        if self.sequence <= 0 or not self.operation_id:
            raise ContinuousExecutionError("operation identity is invalid")
        if self.kind not in OPERATION_KINDS:
            raise ContinuousExecutionError(f"unsupported operation kind: {self.kind}")
        if self.start_ts_ms > self.end_ts_ms:
            raise ContinuousExecutionError("operation clock moved backward")
        if self.kind in {"warmup_resume", "utc_accounting", "panel_terminal"}:
            if self.start_ts_ms != self.end_ts_ms:
                raise ContinuousExecutionError(f"{self.kind} must be an instantaneous operation")
        elif self.start_ts_ms == self.end_ts_ms:
            raise ContinuousExecutionError(f"{self.kind} operation is empty")
        if self.kind != "online" and self.exact_queue_authority:
            raise ContinuousExecutionError("non-online operation received exact queue authority")
        if self.kind not in {"online", "cancel_drain"} and self.exact_lifecycle_authority:
            raise ContinuousExecutionError(
                "non-risk-set operation received exact lifecycle authority"
            )
        if not self.continuous_economic_sensitivity_authority:
            raise ContinuousExecutionError("continuous economic sensitivity authority is absent")
        for value in (
            self.source_identity_sha256,
            self.source_artifact_manifest_sha256,
            self.restart_timeline_sha256,
            self.random_path_sha256,
        ):
            if len(value) != 64:
                raise ContinuousExecutionError("operation hash identity is incomplete")
        if self.kind == "warmup_resume" and self.warmup_lookback_start_ts_ms is None:
            raise ContinuousExecutionError("warmup operation lacks its past-only lookback")


def _source_identity(source: CalendarSourceBinding) -> str:
    return canonical_sha256(source.identity_payload())


def _append_operation(
    rows: list[ContinuousOperation],
    *,
    plan: ContinuousABPlan,
    source: CalendarSourceBinding,
    kind: str,
    day: str,
    start_ts_ms: int,
    end_ts_ms: int,
    gap_id: str = "",
    warmup_lookback_start_ts_ms: int | None = None,
    base_seed: int,
) -> None:
    sequence = len(rows) + 1
    operation_id = f"op-{sequence:05d}-{day}-{kind}"
    seed = _stable_seed(plan.restart_timeline_sha256, operation_id, base_seed)
    exact_queue = bool(source.exact_queue_authority and kind == "online")
    exact_lifecycle = bool(source.exact_lifecycle_authority and kind in {"online", "cancel_drain"})
    row = ContinuousOperation(
        sequence=sequence,
        operation_id=operation_id,
        kind=kind,
        day=day,
        start_ts_ms=int(start_ts_ms),
        end_ts_ms=int(end_ts_ms),
        source_day=source.day,
        gap_id=gap_id,
        warmup_lookback_start_ts_ms=warmup_lookback_start_ts_ms,
        exact_queue_authority=exact_queue,
        exact_lifecycle_authority=exact_lifecycle,
        continuous_economic_sensitivity_authority=True,
        source_identity_sha256=_source_identity(source),
        source_artifact_manifest_sha256=plan.source_artifact_manifest_sha256,
        restart_timeline_sha256=plan.restart_timeline_sha256,
        random_seed=seed,
        random_path_sha256=canonical_sha256(
            {
                "base_seed": int(base_seed),
                "operation_id": operation_id,
                "random_seed": seed,
                "shared_between_arms": True,
            }
        ),
    )
    row.validate()
    rows.append(row)


def build_continuous_operations(
    plan: ContinuousABPlan,
    *,
    base_seed: int,
) -> tuple[ContinuousOperation, ...]:
    """Compile the frozen calendar into one ordered, arm-shared operation tape."""

    plan.validate()
    rows: list[ContinuousOperation] = []
    intervals = tuple(plan.restart_intervals)
    for day, source in zip(plan.calendar_days, plan.source_bindings, strict=True):
        start = day_start_ms(day)
        end = start + DAY_MS
        cursor = start
        overlapping = tuple(
            row
            for row in intervals
            if row.resume_ts_ms >= start
            and int(
                row.quote_stop_ts_ms
                if row.quote_stop_ts_ms is not None
                else row.offline_start_ts_ms
            )
            < end
        )
        for interval in overlapping:
            quote_stop = int(
                interval.quote_stop_ts_ms
                if interval.quote_stop_ts_ms is not None
                else interval.offline_start_ts_ms
            )
            online_end = min(end, max(start, quote_stop))
            if cursor < online_end:
                _append_operation(
                    rows,
                    plan=plan,
                    source=source,
                    kind="online",
                    day=day,
                    start_ts_ms=cursor,
                    end_ts_ms=online_end,
                    base_seed=base_seed,
                )
            # A following gap can begin less than one cancel-drain window after
            # the previous resume.  Its nominal quote-stop then lies inside the
            # preceding offline interval.  Keep the frozen gap identities, but
            # continue blocking from the actual resume cursor instead of
            # creating an overlapping or fictional active-order interval.
            drain_start = max(cursor, start, quote_stop)
            drain_end = min(end, interval.offline_start_ts_ms)
            if drain_start < drain_end:
                _append_operation(
                    rows,
                    plan=plan,
                    source=source,
                    kind="cancel_drain",
                    day=day,
                    start_ts_ms=drain_start,
                    end_ts_ms=drain_end,
                    gap_id=interval.gap_id,
                    base_seed=base_seed,
                )
            gap_start = max(start, interval.offline_start_ts_ms)
            gap_end = min(end, interval.resume_ts_ms)
            if gap_start < gap_end:
                _append_operation(
                    rows,
                    plan=plan,
                    source=source,
                    kind="offline_gap",
                    day=day,
                    start_ts_ms=gap_start,
                    end_ts_ms=gap_end,
                    gap_id=interval.gap_id,
                    base_seed=base_seed,
                )
            if start <= interval.resume_ts_ms < end:
                _append_operation(
                    rows,
                    plan=plan,
                    source=source,
                    kind="warmup_resume",
                    day=day,
                    start_ts_ms=interval.resume_ts_ms,
                    end_ts_ms=interval.resume_ts_ms,
                    gap_id=interval.gap_id,
                    warmup_lookback_start_ts_ms=interval.warmup_lookback_start_ts_ms,
                    base_seed=base_seed,
                )
            cursor = max(cursor, min(end, interval.resume_ts_ms))
        if cursor < end:
            _append_operation(
                rows,
                plan=plan,
                source=source,
                kind="online",
                day=day,
                start_ts_ms=cursor,
                end_ts_ms=end,
                base_seed=base_seed,
            )
        _append_operation(
            rows,
            plan=plan,
            source=source,
            kind="utc_accounting",
            day=day,
            start_ts_ms=end,
            end_ts_ms=end,
            base_seed=base_seed,
        )
    final_source = plan.source_bindings[-1]
    panel_end = day_start_ms(plan.calendar_end_day) + DAY_MS
    _append_operation(
        rows,
        plan=plan,
        source=final_source,
        kind="panel_terminal",
        day=plan.calendar_end_day,
        start_ts_ms=panel_end,
        end_ts_ms=panel_end,
        base_seed=base_seed,
    )
    for previous, current in zip(rows, rows[1:], strict=False):
        if current.start_ts_ms < previous.end_ts_ms:
            raise ContinuousExecutionError("compiled operation tape overlaps")
    if sum(row.kind == "utc_accounting" for row in rows) != 71:
        raise ContinuousExecutionError("operation tape lacks exact UTC accounting slices")
    if sum(row.kind == "panel_terminal" for row in rows) != 1:
        raise ContinuousExecutionError("operation tape lacks one panel terminal")
    return tuple(rows)


@dataclass(frozen=True, slots=True)
class ArmCheckpoint:
    arm_id: str
    operation_sequence: int
    operation_id: str
    state: ContinuousReplayState
    engine_state: Mapping[str, Any]
    engine_state_sha256: str
    previous_checkpoint_sha256: str

    def payload(self) -> dict[str, Any]:
        state_payload = self.state.to_dict()
        return {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "arm_id": self.arm_id,
            "operation_sequence": self.operation_sequence,
            "operation_id": self.operation_id,
            "state": state_payload,
            "state_sha256": state_sha256(state_payload),
            "engine_state": dict(self.engine_state),
            "engine_state_sha256": self.engine_state_sha256,
            "previous_checkpoint_sha256": self.previous_checkpoint_sha256,
        }

    def validate(self) -> None:
        if self.arm_id != self.state.arm_id:
            raise ContinuousExecutionError("checkpoint arm and economic state differ")
        self.state.validate()
        if canonical_sha256(dict(self.engine_state)) != self.engine_state_sha256:
            raise ContinuousExecutionError("checkpoint engine-state hash mismatch")
        if self.operation_sequence < 0:
            raise ContinuousExecutionError("checkpoint operation sequence is invalid")


@dataclass(frozen=True, slots=True)
class OperationResult:
    operation_id: str
    random_path_sha256: str
    state: ContinuousReplayState
    engine_state: Mapping[str, Any]
    quote_count: int
    fill_count: int
    terminal_fill_count: int
    cancel_request_count: int
    cancel_ack_count: int
    cancel_reject_count: int
    runtime_reset_applied: bool
    panel_terminal_mtm_applied: bool = False
    detail: Mapping[str, Any] | None = None


class ContinuousExecutionAdapter(Protocol):
    """Adapter implemented by the bound stateful market/order replay engine."""

    def initialize_arm(
        self,
        *,
        arm_id: str,
        policy_artifacts: Mapping[str, Any],
        first_operation: ContinuousOperation,
    ) -> ArmCheckpoint: ...

    def execute_operation(
        self,
        *,
        operation: ContinuousOperation,
        checkpoint: ArmCheckpoint,
        policy_artifacts: Mapping[str, Any],
    ) -> OperationResult: ...


def _economic_carry_payload(state: ContinuousReplayState) -> dict[str, Any]:
    return {
        "cash_usdc": state.cash_usdc,
        "position_btc": state.position_btc,
        "average_entry_price": state.average_entry_price,
        "cumulative_realized_pnl_usdc": state.cumulative_realized_pnl_usdc,
        "cumulative_fees_usdc": state.cumulative_fees_usdc,
        "economic_campaign": (
            asdict(state.economic_campaign) if state.economic_campaign is not None else None
        ),
        "equity_anchor_usdc": state.equity_anchor_usdc,
    }


def _validate_operation_result(
    operation: ContinuousOperation,
    before: ArmCheckpoint,
    result: OperationResult,
) -> None:
    if result.operation_id != operation.operation_id:
        raise ContinuousExecutionError("adapter result operation identity mismatch")
    if result.random_path_sha256 != operation.random_path_sha256:
        raise ContinuousExecutionError("adapter did not consume the shared random path")
    if result.state.arm_id != before.arm_id:
        raise ContinuousExecutionError("adapter crossed arm state")
    result.state.validate()
    if result.state.checkpoint_ts_ms != operation.end_ts_ms:
        raise ContinuousExecutionError("adapter checkpoint did not end on operation clock")
    counters = (
        result.quote_count,
        result.fill_count,
        result.terminal_fill_count,
        result.cancel_request_count,
        result.cancel_ack_count,
        result.cancel_reject_count,
    )
    if any(int(value) < 0 for value in counters):
        raise ContinuousExecutionError("adapter returned a negative operation counter")
    if operation.kind in NO_QUOTING_KINDS and result.quote_count:
        raise ContinuousExecutionError(f"{operation.kind} produced a new quote")
    if operation.kind in NO_FILL_KINDS and result.fill_count:
        raise ContinuousExecutionError(f"{operation.kind} produced a fill")
    if result.terminal_fill_count > result.fill_count:
        raise ContinuousExecutionError("terminal fill count exceeds all fills")
    if operation.kind == "cancel_drain":
        if not result.runtime_reset_applied:
            raise ContinuousExecutionError("cancel drain omitted the production restart reset")
        if not result.state.restart_safe or result.state.quoting_enabled:
            raise ContinuousExecutionError("cancel drain retained live order state")
        if tuple(result.state.runtime_reset_fields) != tuple(RESTART_RESET_FIELDS):
            raise ContinuousExecutionError("restart reset field contract drifted")
        if result.cancel_ack_count + result.terminal_fill_count < result.cancel_request_count:
            raise ContinuousExecutionError("cancel drain ended before terminal ACK/fill")
    elif operation.kind == "offline_gap":
        if _economic_carry_payload(before.state) != _economic_carry_payload(result.state):
            raise ContinuousExecutionError("offline gap changed carried economic state")
        if not result.state.restart_safe or result.state.quoting_enabled:
            raise ContinuousExecutionError("offline gap retained quote/order state")
    elif operation.kind == "warmup_resume":
        if not result.state.feature_warmup_ready or not result.state.quoting_enabled:
            raise ContinuousExecutionError("warmup did not causally re-enable quoting")
        if result.runtime_reset_applied:
            raise ContinuousExecutionError("warmup applied a second runtime reset")
    elif operation.kind == "utc_accounting":
        if result.runtime_reset_applied:
            raise ContinuousExecutionError("UTC accounting boundary reset runtime state")
        if result.state.restart_generation != before.state.restart_generation:
            raise ContinuousExecutionError("UTC midnight changed restart generation")
    elif operation.kind == "panel_terminal":
        if not result.panel_terminal_mtm_applied:
            raise ContinuousExecutionError("panel terminal omitted its single inventory MTM")
        if not result.state.restart_safe or result.state.quoting_enabled:
            raise ContinuousExecutionError("panel terminal retained live orders")


def _checkpoint_from_result(
    result: OperationResult,
    *,
    sequence: int,
    previous_sha256: str,
) -> ArmCheckpoint:
    engine_state = dict(result.engine_state)
    checkpoint = ArmCheckpoint(
        arm_id=result.state.arm_id,
        operation_sequence=sequence,
        operation_id=result.operation_id,
        state=result.state,
        engine_state=engine_state,
        engine_state_sha256=canonical_sha256(engine_state),
        previous_checkpoint_sha256=previous_sha256,
    )
    checkpoint.validate()
    return checkpoint


def checkpoint_sha256(checkpoint: ArmCheckpoint) -> str:
    return canonical_sha256(checkpoint.payload())


def _load_checkpoint(path: Path) -> ArmCheckpoint:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = str(payload.pop("checkpoint_sha256", ""))
    if canonical_sha256(payload) != expected:
        raise ContinuousExecutionError("persisted checkpoint hash mismatch")
    state_payload = payload["state"]
    if state_sha256(state_payload) != payload["state_sha256"]:
        raise ContinuousExecutionError("persisted economic-state hash mismatch")
    state = ContinuousReplayState.from_dict(state_payload)
    checkpoint = ArmCheckpoint(
        arm_id=str(payload["arm_id"]),
        operation_sequence=int(payload["operation_sequence"]),
        operation_id=str(payload["operation_id"]),
        state=state,
        engine_state=dict(payload["engine_state"]),
        engine_state_sha256=str(payload["engine_state_sha256"]),
        previous_checkpoint_sha256=str(payload["previous_checkpoint_sha256"]),
    )
    checkpoint.validate()
    return checkpoint


def _write_checkpoint(path: Path, checkpoint: ArmCheckpoint) -> str:
    payload = checkpoint.payload()
    digest = canonical_sha256(payload)
    atomic_json(path, {**payload, "checkpoint_sha256": digest})
    return digest


def _load_progress(path: Path, *, plan_identity_sha256: str) -> dict[str, Any]:
    if not path.is_file():
        return {
            "schema_version": PROGRESS_SCHEMA_VERSION,
            "plan_identity_sha256": plan_identity_sha256,
            "completed_operation_sequence": 0,
            "completed_operation_id": "",
            "checkpoint_sha256": {},
            "economic_results_aggregated": False,
            "promotion_authorized": False,
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        payload.get("schema_version") != PROGRESS_SCHEMA_VERSION
        or payload.get("plan_identity_sha256") != plan_identity_sha256
    ):
        raise ContinuousExecutionError("resume progress belongs to another execution plan")
    return payload


def execute_continuous_plan(
    *,
    plan_identity_sha256: str,
    operations: Sequence[ContinuousOperation],
    policy_artifacts: Mapping[str, Mapping[str, Any]],
    output_root: Path,
    adapter: ContinuousExecutionAdapter,
    max_operations: int | None = None,
    allow_test_output_root: bool = False,
) -> dict[str, Any]:
    """Execute or atomically resume a hash-bound paired operation tape.

    This function deliberately does not aggregate or rank economic outcomes.
    It publishes only operation receipts and state checkpoints.
    """

    if set(policy_artifacts) != {"control", "candidate"}:
        raise ContinuousExecutionError("execution requires exact control and candidate artifacts")
    if len(plan_identity_sha256) != 64 or not operations:
        raise ContinuousExecutionError("execution plan identity is incomplete")
    root = output_root.expanduser().resolve()
    required_root = external_cache_root(ROOT).resolve()
    if not allow_test_output_root:
        try:
            root.relative_to(required_root)
        except ValueError as exc:
            raise ContinuousExecutionError(
                "continuous execution output must be in the configured external cache"
            ) from exc
    for operation in operations:
        operation.validate()

    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".execution.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        progress_path = root / "progress.json"
        progress = _load_progress(progress_path, plan_identity_sha256=plan_identity_sha256)
        completed = int(progress["completed_operation_sequence"])
        checkpoints: dict[str, ArmCheckpoint] = {}
        checkpoint_digests: dict[str, str] = {}
        if completed:
            admitted = root / "operations" / f"{completed:05d}"
            for arm in ("control", "candidate"):
                checkpoint_path = admitted / f"checkpoint-{arm}.json"
                checkpoints[arm] = _load_checkpoint(checkpoint_path)
                checkpoint_digests[arm] = checkpoint_sha256(checkpoints[arm])
                if checkpoint_digests[arm] != progress["checkpoint_sha256"][arm]:
                    raise ContinuousExecutionError("progress/checkpoint resume hash mismatch")
        else:
            first = operations[0]
            for arm in ("control", "candidate"):
                checkpoint = adapter.initialize_arm(
                    arm_id=arm,
                    policy_artifacts=policy_artifacts[arm],
                    first_operation=first,
                )
                checkpoint.validate()
                if checkpoint.operation_sequence != 0:
                    raise ContinuousExecutionError("initial checkpoint sequence must be zero")
                checkpoints[arm] = checkpoint
                checkpoint_digests[arm] = checkpoint_sha256(checkpoint)
            if _economic_carry_payload(checkpoints["control"].state) != _economic_carry_payload(
                checkpoints["candidate"].state
            ):
                raise ContinuousExecutionError("paired arms did not start from common economics")

        pending = list(operations[completed:])
        if max_operations is not None:
            if int(max_operations) < 0:
                raise ContinuousExecutionError("max_operations cannot be negative")
            pending = pending[: int(max_operations)]
        for operation in pending:
            if operation.sequence != completed + 1:
                raise ContinuousExecutionError("operation resume sequence is not contiguous")
            results: dict[str, OperationResult] = {}
            next_checkpoints: dict[str, ArmCheckpoint] = {}
            for arm in ("control", "candidate"):
                result = adapter.execute_operation(
                    operation=operation,
                    checkpoint=checkpoints[arm],
                    policy_artifacts=policy_artifacts[arm],
                )
                _validate_operation_result(operation, checkpoints[arm], result)
                results[arm] = result
                next_checkpoints[arm] = _checkpoint_from_result(
                    result,
                    sequence=operation.sequence,
                    previous_sha256=checkpoint_digests[arm],
                )
            if results["control"].random_path_sha256 != results["candidate"].random_path_sha256:
                raise ContinuousExecutionError("paired arms consumed different random paths")

            staging = root / ".staging" / f"{operation.sequence:05d}-{uuid.uuid4().hex}"
            final = root / "operations" / f"{operation.sequence:05d}"
            if final.exists():
                raise ContinuousExecutionError("operation output exists without matching progress")
            staging.mkdir(parents=True)
            try:
                receipt = {
                    "schema_version": OPERATION_SCHEMA_VERSION,
                    "plan_identity_sha256": plan_identity_sha256,
                    "operation": asdict(operation),
                    "authority": {
                        "exact_queue": operation.exact_queue_authority,
                        "exact_lifecycle": operation.exact_lifecycle_authority,
                        "continuous_economic_sensitivity": True,
                        "provider_or_gap_never_upgraded": bool(
                            not operation.exact_queue_authority or operation.kind != "online"
                        ),
                    },
                    "arms": {
                        arm: {
                            "quote_count": results[arm].quote_count,
                            "fill_count": results[arm].fill_count,
                            "terminal_fill_count": results[arm].terminal_fill_count,
                            "cancel_request_count": results[arm].cancel_request_count,
                            "cancel_ack_count": results[arm].cancel_ack_count,
                            "cancel_reject_count": results[arm].cancel_reject_count,
                            "runtime_reset_applied": results[arm].runtime_reset_applied,
                            "panel_terminal_mtm_applied": (results[arm].panel_terminal_mtm_applied),
                            "detail": dict(results[arm].detail or {}),
                        }
                        for arm in ("control", "candidate")
                    },
                    "economic_results_aggregated": False,
                    "promotion_authorized": False,
                }
                atomic_json(staging / "receipt.json", receipt)
                new_digests: dict[str, str] = {}
                for arm in ("control", "candidate"):
                    new_digests[arm] = _write_checkpoint(
                        staging / f"checkpoint-{arm}.json", next_checkpoints[arm]
                    )
                manifest = {
                    "schema_version": OPERATION_SCHEMA_VERSION,
                    "plan_identity_sha256": plan_identity_sha256,
                    "operation_sequence": operation.sequence,
                    "operation_id": operation.operation_id,
                    "receipt_sha256": sha256_file(staging / "receipt.json"),
                    "checkpoint_sha256": new_digests,
                }
                atomic_json(staging / "manifest.json", manifest)
                atomic_text(staging / "_SUCCESS", sha256_file(staging / "manifest.json") + "\n")
                final.parent.mkdir(parents=True, exist_ok=True)
                os.replace(staging, final)
                _fsync_directory(final.parent)
            finally:
                if staging.exists():
                    shutil.rmtree(staging)
            completed = operation.sequence
            checkpoints = next_checkpoints
            checkpoint_digests = new_digests
            progress = {
                "schema_version": PROGRESS_SCHEMA_VERSION,
                "plan_identity_sha256": plan_identity_sha256,
                "completed_operation_sequence": completed,
                "completed_operation_id": operation.operation_id,
                "checkpoint_sha256": checkpoint_digests,
                "panel_complete": completed == len(operations),
                "economic_results_aggregated": False,
                "promotion_authorized": False,
            }
            atomic_json(progress_path, progress)
        return progress
