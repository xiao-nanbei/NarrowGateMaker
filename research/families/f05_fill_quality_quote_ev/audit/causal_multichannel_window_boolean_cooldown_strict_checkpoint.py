"""Outcome-blind strict-native source and shared-prefix checkpoint contract.

This module deliberately admits metadata only.  It does not serialize or
restore ``backtest_tick`` simulator state.  A later runner may use the
contracts here to prove that eight duration arms are intended to branch from
one immutable prefix, but execution remains fail-closed until a real Python
simulator state serializer/restorer is implemented and independently tested.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2"
SOURCE_CONTRACT_SCHEMA = f"{IDENTITY}.strict_native_source.v1"
CHECKPOINT_SCHEMA = f"{IDENTITY}.shared_prefix_checkpoint_metadata.v1"
RESTORE_CONTRACT_SCHEMA = f"{IDENTITY}.arm_restore_contract.v1"
ADMISSION_SCHEMA = f"{IDENTITY}.checkpoint_metadata_admission.v1"

RAW_TAPE_SCHEMA = "native_exchange_book_tape.v1"
RAW_SOURCE_FORMAT = "cryptohft_raw_snapshot_delta_parquet_zstd"
STRICT_QUEUE_SCOPE = (
    "strategy_independent_native_snapshot_delta_exchange_time_v1"
)
SIMULATOR_STATE_STATUS = "identity_only_not_serialized"
MAX_ADMISSION_BYTES = 4 * 1024 * 1024

BUY_ARMS = (
    "CONTROL_85N",
    "FIXED_79S",
    "FIXED_173S",
    "FIXED_223S",
    "FIXED_356S",
    "FIXED_640S",
    "FIXED_709S",
    "FIXED_2048S",
)
SELL_ARMS = (
    "CONTROL_85N",
    "FIXED_79S",
    "FIXED_166S",
    "FIXED_211S",
    "FIXED_349S",
    "FIXED_660S",
    "FIXED_686S",
    "FIXED_1748S",
)
ARM_DURATION_MS = {
    "CONTROL_85N": None,
    "FIXED_79S": 79_000,
    "FIXED_166S": 166_000,
    "FIXED_173S": 173_000,
    "FIXED_211S": 211_000,
    "FIXED_223S": 223_000,
    "FIXED_349S": 349_000,
    "FIXED_356S": 356_000,
    "FIXED_640S": 640_000,
    "FIXED_660S": 660_000,
    "FIXED_686S": 686_000,
    "FIXED_709S": 709_000,
    "FIXED_1748S": 1_748_000,
    "FIXED_2048S": 2_048_000,
}

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_HOUR_RE = re.compile(r"^(?:[01]\d|2[0-3])$")


class StrictCheckpointError(ValueError):
    """Raised when strict-native checkpoint metadata is not admissible."""


def canonical_json_bytes(payload: Any) -> bytes:
    """Return the one canonical JSON encoding used by all identities here."""

    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_sha256(value: Any, field: str) -> str:
    normalized = str(value).strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise StrictCheckpointError(f"{field} must be a lowercase SHA256")
    return normalized


def _parse_day(value: Any, field: str) -> date:
    text = str(value)
    if not _DAY_RE.fullmatch(text):
        raise StrictCheckpointError(f"{field} must be YYYY-MM-DD")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise StrictCheckpointError(f"{field} is not a valid UTC day") from exc


def _exact_keys(payload: Mapping[str, Any], expected: set[str], role: str) -> None:
    actual = set(payload)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise StrictCheckpointError(
            f"{role} schema mismatch: missing={missing}, extra={extra}"
        )


def _without_hash(payload: Mapping[str, Any], field: str) -> dict[str, Any]:
    body = dict(payload)
    body.pop(field, None)
    return body


def _validate_embedded_hash(
    payload: Mapping[str, Any],
    *,
    field: str,
    role: str,
) -> None:
    expected = _require_sha256(payload.get(field), f"{role}.{field}")
    actual = canonical_sha256(_without_hash(payload, field))
    if actual != expected:
        raise StrictCheckpointError(f"{role} canonical SHA256 drifted")


def _utc_day_for_ns(value_ns: int) -> str:
    return datetime.fromtimestamp(value_ns / 1_000_000_000, tz=UTC).date().isoformat()


@dataclass(frozen=True)
class RawNativeHourIdentity:
    utc_day: str
    hour: int
    path: str
    size_bytes: int
    mtime_ns: int
    sha256: str

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RawNativeDayIdentity:
    utc_day: str
    role: str
    parser_identity_sha256: str
    hours: tuple[RawNativeHourIdentity, ...]

    @property
    def raw_snapshot_delta_identity_sha256(self) -> str:
        return canonical_sha256(
            {
                "utc_day": self.utc_day,
                "source_format": RAW_SOURCE_FORMAT,
                "parser_identity_sha256": self.parser_identity_sha256,
                "hours": [row.to_payload() for row in self.hours],
            }
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "utc_day": self.utc_day,
            "role": self.role,
            "source_format": RAW_SOURCE_FORMAT,
            "parser_identity_sha256": self.parser_identity_sha256,
            "hour_count": len(self.hours),
            "hours": [row.to_payload() for row in self.hours],
            "raw_snapshot_delta_identity_sha256": (
                self.raw_snapshot_delta_identity_sha256
            ),
        }


@dataclass(frozen=True)
class NativeTapeBinding:
    role: str
    tape_day: str
    covered_days: tuple[str, ...]
    tape_identity_sha256: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "tape_day": self.tape_day,
            "covered_days": list(self.covered_days),
            "tape_identity_sha256": self.tape_identity_sha256,
        }


@dataclass(frozen=True)
class StrictNativeSourceContract:
    target_day: str
    symbol: str
    market_id: str
    tick_size: float
    parser_identity_sha256: str
    ordered_days: tuple[RawNativeDayIdentity, ...]
    tape_bindings: tuple[NativeTapeBinding, NativeTapeBinding]

    @property
    def canonical_identity_sha256(self) -> str:
        return canonical_sha256(self._body())

    def _body(self) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_CONTRACT_SCHEMA,
            "identity": IDENTITY,
            "target_day": self.target_day,
            "symbol": self.symbol,
            "market_id": self.market_id,
            "tick_size": self.tick_size,
            "parser_identity_sha256": self.parser_identity_sha256,
            "ordered_days": [row.to_payload() for row in self.ordered_days],
            "tape_bindings": [row.to_payload() for row in self.tape_bindings],
            "window_contract": {
                "warmup": "previous natural UTC day, complete 24h",
                "target": "target natural UTC day, complete 24h",
                "continuation": "next natural UTC day, complete 24h",
                "ordered_hour_count": 72,
            },
            "engine": "python",
            "exchange_book_queue_mode": "strict",
            "exchange_book_queue_scope": STRICT_QUEUE_SCOPE,
            "raw_source_format": RAW_SOURCE_FORMAT,
            "normalized_l2_exact_authority": False,
            "cpp_restore_authority": False,
            "exact_historical_receive_time_authority": False,
            "economic_outcomes_read": False,
        }

    def to_payload(self) -> dict[str, Any]:
        payload = self._body()
        payload["canonical_identity_sha256"] = self.canonical_identity_sha256
        return payload

    def day(self, role: str) -> RawNativeDayIdentity:
        matches = [row for row in self.ordered_days if row.role == role]
        if len(matches) != 1:
            raise StrictCheckpointError(f"source contract lacks one {role!r} day")
        return matches[0]


def _source_hour_from_tape_row(
    row: Mapping[str, Any],
    *,
    symbol: str,
) -> RawNativeHourIdentity:
    _exact_keys(
        row,
        {"path", "size_bytes", "mtime_ns", "sha256"},
        "native tape file identity",
    )
    path = Path(str(row["path"])).expanduser()
    if not path.is_absolute():
        raise StrictCheckpointError("native source path must be absolute")
    if path.name != f"{symbol}_orderbook.parquet.zst":
        raise StrictCheckpointError("source is not raw CryptoHFT orderbook data")
    day_text = path.parent.parent.name
    hour_text = path.parent.name
    _parse_day(day_text, "native source path day")
    if not _HOUR_RE.fullmatch(hour_text):
        raise StrictCheckpointError("native source path hour is invalid")
    size_bytes = int(row["size_bytes"])
    mtime_ns = int(row["mtime_ns"])
    if size_bytes <= 0 or mtime_ns < 0:
        raise StrictCheckpointError("native source size/mtime is invalid")
    return RawNativeHourIdentity(
        utc_day=day_text,
        hour=int(hour_text),
        path=str(path),
        size_bytes=size_bytes,
        mtime_ns=mtime_ns,
        sha256=_require_sha256(row["sha256"], "native source sha256"),
    )


def _validate_tape_identity(
    payload: Mapping[str, Any],
    *,
    expected_tape_day: str,
    expected_source_days: tuple[str, ...],
    parser_identity_sha256: str,
    expected_continuation_hours: int = 0,
) -> tuple[RawNativeHourIdentity, ...]:
    required = {
        "schema_version",
        "day",
        "symbol",
        "market_id",
        "tick_size",
        "exchange_clock",
        "warmup_hours",
        "continuation_hours",
        "strict_complete",
        "missing_paths",
        "files",
    }
    missing = sorted(required - set(payload))
    if missing:
        raise StrictCheckpointError(f"native tape identity missing fields: {missing}")
    if payload["schema_version"] != RAW_TAPE_SCHEMA:
        raise StrictCheckpointError("normalized/non-native tape is forbidden")
    if str(payload["day"]) != expected_tape_day:
        raise StrictCheckpointError("native tape day is missing, duplicated, or out of order")
    if int(payload["warmup_hours"]) != 24:
        raise StrictCheckpointError("strict source requires exactly 24h D-1 warmup")
    if int(payload["continuation_hours"]) != int(expected_continuation_hours):
        raise StrictCheckpointError("native tape continuation identity drifted")
    if payload["strict_complete"] is not True or list(payload["missing_paths"]):
        raise StrictCheckpointError("native tape is not strict-complete")
    if payload["exchange_clock"] != (
        "transaction_time_with_event_then_receive_fallback"
    ):
        raise StrictCheckpointError("native tape exchange-clock identity drifted")
    _require_sha256(parser_identity_sha256, "parser_identity_sha256")

    symbol = str(payload["symbol"]).upper()
    raw_files = payload["files"]
    if not isinstance(raw_files, list):
        raise StrictCheckpointError("native tape files must be an ordered list")
    hours = tuple(
        _source_hour_from_tape_row(row, symbol=symbol) for row in raw_files
    )
    expected = tuple(
        (day_text, hour)
        for day_text in expected_source_days
        for hour in range(24)
    )
    actual = tuple((row.utc_day, row.hour) for row in hours)
    if actual != expected:
        raise StrictCheckpointError(
            "native tape does not contain the expected unique ordered UTC hours"
        )
    if len({row.path for row in hours}) != len(hours):
        raise StrictCheckpointError("native tape contains duplicate source paths")
    return hours


def build_strict_native_source_contract(
    *,
    target_day: str,
    target_tape_identity: Mapping[str, Any],
    continuation_tape_identity: Mapping[str, Any],
    parser_identity_sha256: str,
) -> StrictNativeSourceContract:
    """Bind two current tape identities into one D-1/D/D+1 contract.

    ``CryptoHFTExchangeBookTape(day=D)`` currently covers D-1 and D.  A second
    tape with ``day=D+1`` covers D and D+1.  Their D overlap must be byte- and
    metadata-identical; the merged contract then contains exactly 72 hours.
    """

    target = _parse_day(target_day, "target_day")
    warmup_day = (target - timedelta(days=1)).isoformat()
    target_text = target.isoformat()
    continuation_day = (target + timedelta(days=1)).isoformat()
    parser_sha = _require_sha256(parser_identity_sha256, "parser_identity_sha256")

    target_hours = _validate_tape_identity(
        target_tape_identity,
        expected_tape_day=target_text,
        expected_source_days=(warmup_day, target_text),
        parser_identity_sha256=parser_sha,
    )
    continuation_hours = _validate_tape_identity(
        continuation_tape_identity,
        expected_tape_day=continuation_day,
        expected_source_days=(target_text, continuation_day),
        parser_identity_sha256=parser_sha,
    )
    for key in ("symbol", "market_id", "tick_size"):
        if target_tape_identity[key] != continuation_tape_identity[key]:
            raise StrictCheckpointError(f"overlapping native tapes disagree on {key}")

    by_hour: dict[tuple[str, int], RawNativeHourIdentity] = {}
    for row in (*target_hours, *continuation_hours):
        key = (row.utc_day, row.hour)
        prior = by_hour.get(key)
        if prior is not None and prior != row:
            raise StrictCheckpointError(
                "target-day overlap has inconsistent raw source identities"
            )
        by_hour[key] = row
    expected_keys = tuple(
        (day_text, hour)
        for day_text in (warmup_day, target_text, continuation_day)
        for hour in range(24)
    )
    if tuple(sorted(by_hour)) != expected_keys:
        raise StrictCheckpointError("merged source is not three consecutive UTC days")

    roles = ("warmup", "target", "continuation")
    day_texts = (warmup_day, target_text, continuation_day)
    ordered_days = tuple(
        RawNativeDayIdentity(
            utc_day=day_text,
            role=role,
            parser_identity_sha256=parser_sha,
            hours=tuple(by_hour[(day_text, hour)] for hour in range(24)),
        )
        for role, day_text in zip(roles, day_texts, strict=True)
    )
    contract = StrictNativeSourceContract(
        target_day=target_text,
        symbol=str(target_tape_identity["symbol"]).upper(),
        market_id=str(target_tape_identity["market_id"]),
        tick_size=float(target_tape_identity["tick_size"]),
        parser_identity_sha256=parser_sha,
        ordered_days=ordered_days,
        tape_bindings=(
            NativeTapeBinding(
                role="target_with_warmup",
                tape_day=target_text,
                covered_days=(warmup_day, target_text),
                tape_identity_sha256=canonical_sha256(target_tape_identity),
            ),
            NativeTapeBinding(
                role="continuation_with_target_warmup",
                tape_day=continuation_day,
                covered_days=(target_text, continuation_day),
                tape_identity_sha256=canonical_sha256(continuation_tape_identity),
            ),
        ),
    )
    validate_source_contract_payload(contract.to_payload())
    return contract


def build_strict_native_source_contract_from_single_tape(
    *,
    target_day: str,
    tape_identity: Mapping[str, Any],
    parser_identity_sha256: str,
) -> StrictNativeSourceContract:
    """Bind one executable D-1/D/D+1 tape with 24h continuation."""

    target = _parse_day(target_day, "target_day")
    day_texts = (
        (target - timedelta(days=1)).isoformat(),
        target.isoformat(),
        (target + timedelta(days=1)).isoformat(),
    )
    parser_sha = _require_sha256(parser_identity_sha256, "parser_identity_sha256")
    hours = _validate_tape_identity(
        tape_identity,
        expected_tape_day=target.isoformat(),
        expected_source_days=day_texts,
        parser_identity_sha256=parser_sha,
        expected_continuation_hours=24,
    )
    by_hour = {(row.utc_day, row.hour): row for row in hours}
    roles = ("warmup", "target", "continuation")
    ordered_days = tuple(
        RawNativeDayIdentity(
            utc_day=day_text,
            role=role,
            parser_identity_sha256=parser_sha,
            hours=tuple(by_hour[(day_text, hour)] for hour in range(24)),
        )
        for role, day_text in zip(roles, day_texts, strict=True)
    )
    contract = StrictNativeSourceContract(
        target_day=target.isoformat(),
        symbol=str(tape_identity["symbol"]).upper(),
        market_id=str(tape_identity["market_id"]),
        tick_size=float(tape_identity["tick_size"]),
        parser_identity_sha256=parser_sha,
        ordered_days=ordered_days,
        tape_bindings=(
            NativeTapeBinding(
                role="target_with_warmup_and_continuation",
                tape_day=target.isoformat(),
                covered_days=day_texts,
                tape_identity_sha256=canonical_sha256(tape_identity),
            ),
        ),
    )
    validate_source_contract_payload(contract.to_payload())
    return contract


def validate_source_files(
    contract: StrictNativeSourceContract,
    *,
    verify_sha256: bool = True,
) -> None:
    """Revalidate all 72 raw files without reading any strategy outcomes."""

    validate_source_contract_payload(contract.to_payload())
    for day_identity in contract.ordered_days:
        for row in day_identity.hours:
            path = Path(row.path)
            if not path.is_file():
                raise StrictCheckpointError(f"missing native source file: {path}")
            stat = path.stat()
            if stat.st_size != row.size_bytes or stat.st_mtime_ns != row.mtime_ns:
                raise StrictCheckpointError(f"native source metadata drifted: {path}")
            if verify_sha256 and file_sha256(path) != row.sha256:
                raise StrictCheckpointError(f"native source SHA256 drifted: {path}")


@dataclass(frozen=True)
class MarketCursorMetadata:
    stream_identity_sha256: str
    event_ordinal: int
    market_generation: int
    exchange_ts_ns: int
    receive_ts_ns: int
    feature_ready_ts_ns: int
    visibility_clock: str

    @property
    def canonical_identity_sha256(self) -> str:
        return canonical_sha256(asdict(self))

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["canonical_identity_sha256"] = self.canonical_identity_sha256
        return payload


@dataclass(frozen=True)
class NativeTapeCursorMetadata:
    source_contract_sha256: str
    raw_day_identity_sha256: str
    utc_day: str
    hour: int
    tape_event_ordinal: int
    source_event_ordinal: int
    exchange_ts_ns: int
    segment_id: int
    last_update_id: int | None

    @property
    def canonical_identity_sha256(self) -> str:
        return canonical_sha256(asdict(self))

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["canonical_identity_sha256"] = self.canonical_identity_sha256
        return payload


@dataclass(frozen=True)
class SharedPrefixCheckpointMetadata:
    target_day: str
    opportunity_id: str
    side: str
    role: str
    fill_visible_event_id: str
    fill_exchange_ts_ns: int
    fill_visible_ts_ns: int
    source_contract_sha256: str
    market_cursor: MarketCursorMetadata
    native_tape_cursor: NativeTapeCursorMetadata
    strategy_state_identity_sha256: str
    ema_checkpoint_sha256: str
    baseline_identity_sha256: str
    config_sha256: str
    code_sha256: str
    model_sha256: str
    p3_sha256: str
    feature_dag_sha256: str
    execution_abi_sha256: str

    def _prefix_body(self) -> dict[str, Any]:
        return {
            "target_day": self.target_day,
            "opportunity_id": self.opportunity_id,
            "side": self.side,
            "role": self.role,
            "fill_visible_event_id": self.fill_visible_event_id,
            "fill_exchange_ts_ns": self.fill_exchange_ts_ns,
            "fill_visible_ts_ns": self.fill_visible_ts_ns,
            "source_contract_sha256": self.source_contract_sha256,
            "market_cursor_sha256": self.market_cursor.canonical_identity_sha256,
            "native_tape_cursor_sha256": (
                self.native_tape_cursor.canonical_identity_sha256
            ),
            "strategy_state_identity_sha256": self.strategy_state_identity_sha256,
            "ema_checkpoint_sha256": self.ema_checkpoint_sha256,
            "baseline_identity_sha256": self.baseline_identity_sha256,
            "config_sha256": self.config_sha256,
            "code_sha256": self.code_sha256,
            "model_sha256": self.model_sha256,
            "p3_sha256": self.p3_sha256,
            "feature_dag_sha256": self.feature_dag_sha256,
            "execution_abi_sha256": self.execution_abi_sha256,
        }

    @property
    def shared_prefix_identity_sha256(self) -> str:
        return canonical_sha256(self._prefix_body())

    def _body(self) -> dict[str, Any]:
        return {
            "schema_version": CHECKPOINT_SCHEMA,
            "identity": IDENTITY,
            **self._prefix_body(),
            "market_cursor": self.market_cursor.to_payload(),
            "native_tape_cursor": self.native_tape_cursor.to_payload(),
            "shared_prefix_identity_sha256": self.shared_prefix_identity_sha256,
            "checkpoint_event": "strategy-visible exposure-increasing fill callback",
            "simulator_state_serialization_status": SIMULATOR_STATE_STATUS,
            "simulator_state_artifact_present": False,
            "restore_execution_eligible": False,
            "economic_outcomes_read": False,
        }

    @property
    def canonical_checkpoint_sha256(self) -> str:
        return canonical_sha256(self._body())

    def to_payload(self) -> dict[str, Any]:
        payload = self._body()
        payload["canonical_checkpoint_sha256"] = self.canonical_checkpoint_sha256
        return payload


def validate_checkpoint_metadata(
    checkpoint: SharedPrefixCheckpointMetadata,
    *,
    source_contract: StrictNativeSourceContract,
) -> None:
    validate_source_contract_payload(source_contract.to_payload())
    payload = checkpoint.to_payload()
    validate_checkpoint_payload(payload)
    if checkpoint.source_contract_sha256 != source_contract.canonical_identity_sha256:
        raise StrictCheckpointError("checkpoint source-contract identity drifted")
    if checkpoint.target_day != source_contract.target_day:
        raise StrictCheckpointError("checkpoint target day drifted")
    target_identity = source_contract.day("target")
    cursor = checkpoint.native_tape_cursor
    if cursor.source_contract_sha256 != source_contract.canonical_identity_sha256:
        raise StrictCheckpointError("native cursor source-contract identity drifted")
    if cursor.raw_day_identity_sha256 != (
        target_identity.raw_snapshot_delta_identity_sha256
    ):
        raise StrictCheckpointError("native cursor raw-day identity drifted")


@dataclass(frozen=True)
class ArmRestoreBinding:
    arm_id: str
    side: str
    duration_ms: int | None
    duration_semantics: str
    checkpoint_sha256: str
    shared_prefix_identity_sha256: str
    source_contract_sha256: str
    strategy_state_identity_sha256: str
    ema_checkpoint_sha256: str

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ArmRestoreContract:
    side: str
    checkpoint_sha256: str
    shared_prefix_identity_sha256: str
    source_contract_sha256: str
    bindings: tuple[ArmRestoreBinding, ...]

    def _body(self) -> dict[str, Any]:
        return {
            "schema_version": RESTORE_CONTRACT_SCHEMA,
            "identity": IDENTITY,
            "side": self.side,
            "checkpoint_sha256": self.checkpoint_sha256,
            "shared_prefix_identity_sha256": self.shared_prefix_identity_sha256,
            "source_contract_sha256": self.source_contract_sha256,
            "arm_count": len(self.bindings),
            "bindings": [row.to_payload() for row in self.bindings],
            "required_engine": "python",
            "required_exchange_book_queue_mode": "strict",
            "required_exchange_book_queue_scope": STRICT_QUEUE_SCOPE,
            "normalized_l2_exact_authority": False,
            "cpp_restore_authority": False,
            "all_arms_share_one_prefix": True,
            "economic_outcome_fields": [],
            "economic_outcomes_read": False,
            "simulator_state_serialization_status": SIMULATOR_STATE_STATUS,
            "metadata_restore_validation_eligible": True,
            "restore_execution_eligible": False,
        }

    @property
    def canonical_restore_contract_sha256(self) -> str:
        return canonical_sha256(self._body())

    def to_payload(self) -> dict[str, Any]:
        payload = self._body()
        payload["canonical_restore_contract_sha256"] = (
            self.canonical_restore_contract_sha256
        )
        return payload


def build_arm_restore_contract(
    checkpoint: SharedPrefixCheckpointMetadata,
) -> ArmRestoreContract:
    arms = BUY_ARMS if checkpoint.side == "BUY" else SELL_ARMS
    bindings = tuple(
        ArmRestoreBinding(
            arm_id=arm_id,
            side=checkpoint.side,
            duration_ms=ARM_DURATION_MS[arm_id],
            duration_semantics=(
                "85000ms * max(1.0, consecutive_same_side_fill_units_after_callback)"
                if arm_id == "CONTROL_85N"
                else "fixed total duration from target fill-visible callback"
            ),
            checkpoint_sha256=checkpoint.canonical_checkpoint_sha256,
            shared_prefix_identity_sha256=(
                checkpoint.shared_prefix_identity_sha256
            ),
            source_contract_sha256=checkpoint.source_contract_sha256,
            strategy_state_identity_sha256=(
                checkpoint.strategy_state_identity_sha256
            ),
            ema_checkpoint_sha256=checkpoint.ema_checkpoint_sha256,
        )
        for arm_id in arms
    )
    contract = ArmRestoreContract(
        side=checkpoint.side,
        checkpoint_sha256=checkpoint.canonical_checkpoint_sha256,
        shared_prefix_identity_sha256=checkpoint.shared_prefix_identity_sha256,
        source_contract_sha256=checkpoint.source_contract_sha256,
        bindings=bindings,
    )
    validate_arm_restore_contract(contract, checkpoint=checkpoint)
    return contract


def validate_arm_restore_contract(
    contract: ArmRestoreContract,
    *,
    checkpoint: SharedPrefixCheckpointMetadata,
) -> None:
    validate_restore_contract_payload(contract.to_payload())
    if contract.side != checkpoint.side:
        raise StrictCheckpointError("restore contract side drifted")
    if contract.checkpoint_sha256 != checkpoint.canonical_checkpoint_sha256:
        raise StrictCheckpointError("restore contract checkpoint drifted")
    if contract.shared_prefix_identity_sha256 != (
        checkpoint.shared_prefix_identity_sha256
    ):
        raise StrictCheckpointError("restore contract shared prefix drifted")
    for row in contract.bindings:
        if row.strategy_state_identity_sha256 != (
            checkpoint.strategy_state_identity_sha256
        ) or row.ema_checkpoint_sha256 != checkpoint.ema_checkpoint_sha256:
            raise StrictCheckpointError("arm binding does not share prefix state")


def validate_source_contract_payload(payload: Mapping[str, Any]) -> None:
    expected = {
        "schema_version",
        "identity",
        "target_day",
        "symbol",
        "market_id",
        "tick_size",
        "parser_identity_sha256",
        "ordered_days",
        "tape_bindings",
        "window_contract",
        "engine",
        "exchange_book_queue_mode",
        "exchange_book_queue_scope",
        "raw_source_format",
        "normalized_l2_exact_authority",
        "cpp_restore_authority",
        "exact_historical_receive_time_authority",
        "economic_outcomes_read",
        "canonical_identity_sha256",
    }
    _exact_keys(payload, expected, "strict-native source contract")
    if payload["schema_version"] != SOURCE_CONTRACT_SCHEMA or payload["identity"] != IDENTITY:
        raise StrictCheckpointError("strict-native source identity drifted")
    if payload["engine"] != "python":
        raise StrictCheckpointError("C++ strict-native checkpoint authority is forbidden")
    if payload["exchange_book_queue_mode"] != "strict" or payload[
        "exchange_book_queue_scope"
    ] != STRICT_QUEUE_SCOPE:
        raise StrictCheckpointError("strict-native queue identity drifted")
    if payload["raw_source_format"] != RAW_SOURCE_FORMAT:
        raise StrictCheckpointError("normalized-L2 exact authority is forbidden")
    symbol = str(payload["symbol"])
    if not symbol or symbol != symbol.upper():
        raise StrictCheckpointError("strict-native symbol identity is invalid")
    if float(payload["tick_size"]) <= 0.0:
        raise StrictCheckpointError("strict-native tick size must be positive")
    if not str(payload["market_id"]).endswith(f":{symbol}"):
        raise StrictCheckpointError("strict-native market identity drifted")
    for field in (
        "normalized_l2_exact_authority",
        "cpp_restore_authority",
        "exact_historical_receive_time_authority",
        "economic_outcomes_read",
    ):
        if payload[field] is not False:
            raise StrictCheckpointError(f"{field} must remain false")
    parser_sha = _require_sha256(
        payload["parser_identity_sha256"], "parser_identity_sha256"
    )
    target = _parse_day(payload["target_day"], "target_day")
    expected_days = (
        (target - timedelta(days=1)).isoformat(),
        target.isoformat(),
        (target + timedelta(days=1)).isoformat(),
    )
    rows = payload["ordered_days"]
    if not isinstance(rows, list) or len(rows) != 3:
        raise StrictCheckpointError("source contract requires three ordered days")
    actual_days: list[str] = []
    for row, expected_role, expected_day in zip(
        rows, ("warmup", "target", "continuation"), expected_days, strict=True
    ):
        _exact_keys(
            row,
            {
                "utc_day",
                "role",
                "source_format",
                "parser_identity_sha256",
                "hour_count",
                "hours",
                "raw_snapshot_delta_identity_sha256",
            },
            "raw native day identity",
        )
        if row["role"] != expected_role or row["utc_day"] != expected_day:
            raise StrictCheckpointError("source days are missing, duplicated, or out of order")
        if row["source_format"] != RAW_SOURCE_FORMAT:
            raise StrictCheckpointError("raw snapshot/delta identity is required")
        if row["parser_identity_sha256"] != parser_sha:
            raise StrictCheckpointError("raw day parser identity drifted")
        hours = row["hours"]
        if row["hour_count"] != 24 or not isinstance(hours, list) or len(hours) != 24:
            raise StrictCheckpointError("each raw day must contain exactly 24 hours")
        parsed_hours: list[RawNativeHourIdentity] = []
        for hour_row in hours:
            _exact_keys(
                hour_row,
                {"utc_day", "hour", "path", "size_bytes", "mtime_ns", "sha256"},
                "raw native hour identity",
            )
            parsed = RawNativeHourIdentity(
                utc_day=str(hour_row["utc_day"]),
                hour=int(hour_row["hour"]),
                path=str(hour_row["path"]),
                size_bytes=int(hour_row["size_bytes"]),
                mtime_ns=int(hour_row["mtime_ns"]),
                sha256=_require_sha256(hour_row["sha256"], "raw hour sha256"),
            )
            path = Path(parsed.path)
            if not path.is_absolute() or path.name != (
                f"{symbol}_orderbook.parquet.zst"
            ):
                raise StrictCheckpointError(
                    "raw native hour path is not a CryptoHFT orderbook source"
                )
            if path.parent.parent.name != expected_day or path.parent.name != (
                f"{parsed.hour:02d}"
            ):
                raise StrictCheckpointError("raw native hour path identity drifted")
            if parsed.size_bytes <= 0 or parsed.mtime_ns < 0:
                raise StrictCheckpointError("raw native hour metadata is invalid")
            parsed_hours.append(parsed)
        if tuple((item.utc_day, item.hour) for item in parsed_hours) != tuple(
            (expected_day, hour) for hour in range(24)
        ):
            raise StrictCheckpointError("raw native hours are missing or out of order")
        day_body = {
            "utc_day": expected_day,
            "source_format": RAW_SOURCE_FORMAT,
            "parser_identity_sha256": parser_sha,
            "hours": [item.to_payload() for item in parsed_hours],
        }
        if canonical_sha256(day_body) != row["raw_snapshot_delta_identity_sha256"]:
            raise StrictCheckpointError("raw snapshot/delta day identity drifted")
        actual_days.append(expected_day)
    if len(set(actual_days)) != 3:
        raise StrictCheckpointError("source contract contains duplicate UTC days")
    window = payload["window_contract"]
    if window != {
        "warmup": "previous natural UTC day, complete 24h",
        "target": "target natural UTC day, complete 24h",
        "continuation": "next natural UTC day, complete 24h",
        "ordered_hour_count": 72,
    }:
        raise StrictCheckpointError("D-1/D/D+1 window contract drifted")
    bindings = payload["tape_bindings"]
    if not isinstance(bindings, list) or len(bindings) not in {1, 2}:
        raise StrictCheckpointError("source contract requires one or two tape bindings")
    expected_bindings = (
        (
            (
                "target_with_warmup_and_continuation",
                expected_days[1],
                expected_days,
            ),
        )
        if len(bindings) == 1
        else (
            ("target_with_warmup", expected_days[1], expected_days[:2]),
            (
                "continuation_with_target_warmup",
                expected_days[2],
                expected_days[1:],
            ),
        )
    )
    for row, (role, tape_day, covered) in zip(
        bindings, expected_bindings, strict=True
    ):
        _exact_keys(
            row,
            {"role", "tape_day", "covered_days", "tape_identity_sha256"},
            "native tape binding",
        )
        if row["role"] != role or row["tape_day"] != tape_day or tuple(
            row["covered_days"]
        ) != tuple(covered):
            raise StrictCheckpointError("native tape bindings are out of order")
        _require_sha256(row["tape_identity_sha256"], "tape identity sha256")
    _validate_embedded_hash(
        payload,
        field="canonical_identity_sha256",
        role="strict-native source contract",
    )


def _validate_market_cursor(payload: Mapping[str, Any], fill_visible_ts_ns: int) -> None:
    _exact_keys(
        payload,
        {
            "stream_identity_sha256",
            "event_ordinal",
            "market_generation",
            "exchange_ts_ns",
            "receive_ts_ns",
            "feature_ready_ts_ns",
            "visibility_clock",
            "canonical_identity_sha256",
        },
        "market cursor",
    )
    _require_sha256(payload["stream_identity_sha256"], "market stream identity")
    if int(payload["event_ordinal"]) < 0 or int(payload["market_generation"]) < 0:
        raise StrictCheckpointError("market cursor ordinal/generation is invalid")
    exchange_ns = int(payload["exchange_ts_ns"])
    receive_ns = int(payload["receive_ts_ns"])
    ready_ns = int(payload["feature_ready_ts_ns"])
    if not (0 < exchange_ns <= receive_ns <= ready_ns <= fill_visible_ts_ns):
        raise StrictCheckpointError("market cursor violates causal visibility order")
    if payload["visibility_clock"] not in {
        "exchange_time_diagnostic",
        "receive_feature_ready",
    }:
        raise StrictCheckpointError("market cursor visibility clock is unsupported")
    _validate_embedded_hash(
        payload, field="canonical_identity_sha256", role="market cursor"
    )


def _validate_native_cursor(
    payload: Mapping[str, Any],
    *,
    fill_exchange_ts_ns: int,
    target_day: str,
) -> None:
    _exact_keys(
        payload,
        {
            "source_contract_sha256",
            "raw_day_identity_sha256",
            "utc_day",
            "hour",
            "tape_event_ordinal",
            "source_event_ordinal",
            "exchange_ts_ns",
            "segment_id",
            "last_update_id",
            "canonical_identity_sha256",
        },
        "native tape cursor",
    )
    for key in ("source_contract_sha256", "raw_day_identity_sha256"):
        _require_sha256(payload[key], f"native cursor {key}")
    if payload["utc_day"] != target_day or not 0 <= int(payload["hour"]) <= 23:
        raise StrictCheckpointError("native cursor is outside the target day")
    for key in ("tape_event_ordinal", "source_event_ordinal", "segment_id"):
        if int(payload[key]) < 0:
            raise StrictCheckpointError(f"native cursor {key} is negative")
    exchange_ns = int(payload["exchange_ts_ns"])
    if not 0 < exchange_ns <= fill_exchange_ts_ns:
        raise StrictCheckpointError("native cursor is after the fill exchange event")
    if _utc_day_for_ns(exchange_ns) != target_day:
        raise StrictCheckpointError("native cursor timestamp/day mismatch")
    _validate_embedded_hash(
        payload, field="canonical_identity_sha256", role="native tape cursor"
    )


def validate_checkpoint_payload(payload: Mapping[str, Any]) -> None:
    expected = {
        "schema_version",
        "identity",
        "target_day",
        "opportunity_id",
        "side",
        "role",
        "fill_visible_event_id",
        "fill_exchange_ts_ns",
        "fill_visible_ts_ns",
        "source_contract_sha256",
        "market_cursor_sha256",
        "native_tape_cursor_sha256",
        "strategy_state_identity_sha256",
        "ema_checkpoint_sha256",
        "baseline_identity_sha256",
        "config_sha256",
        "code_sha256",
        "model_sha256",
        "p3_sha256",
        "feature_dag_sha256",
        "execution_abi_sha256",
        "market_cursor",
        "native_tape_cursor",
        "shared_prefix_identity_sha256",
        "checkpoint_event",
        "simulator_state_serialization_status",
        "simulator_state_artifact_present",
        "restore_execution_eligible",
        "economic_outcomes_read",
        "canonical_checkpoint_sha256",
    }
    _exact_keys(payload, expected, "shared-prefix checkpoint metadata")
    if payload["schema_version"] != CHECKPOINT_SCHEMA or payload["identity"] != IDENTITY:
        raise StrictCheckpointError("checkpoint identity drifted")
    target_day = _parse_day(payload["target_day"], "checkpoint target_day").isoformat()
    if payload["side"] not in {"BUY", "SELL"} or payload["role"] not in {
        "opener",
        "add",
    }:
        raise StrictCheckpointError("checkpoint side/role is invalid")
    if not str(payload["opportunity_id"]).strip() or not str(
        payload["fill_visible_event_id"]
    ).strip():
        raise StrictCheckpointError("checkpoint event identity is empty")
    fill_exchange_ns = int(payload["fill_exchange_ts_ns"])
    fill_visible_ns = int(payload["fill_visible_ts_ns"])
    if not 0 < fill_exchange_ns <= fill_visible_ns:
        raise StrictCheckpointError("fill visibility precedes fill exchange time")
    if _utc_day_for_ns(fill_visible_ns) != target_day:
        raise StrictCheckpointError("fill-visible event is outside target day")
    hash_fields = (
        "source_contract_sha256",
        "market_cursor_sha256",
        "native_tape_cursor_sha256",
        "strategy_state_identity_sha256",
        "ema_checkpoint_sha256",
        "baseline_identity_sha256",
        "config_sha256",
        "code_sha256",
        "model_sha256",
        "p3_sha256",
        "feature_dag_sha256",
        "execution_abi_sha256",
        "shared_prefix_identity_sha256",
    )
    for field in hash_fields:
        _require_sha256(payload[field], f"checkpoint {field}")
    _validate_market_cursor(payload["market_cursor"], fill_visible_ns)
    _validate_native_cursor(
        payload["native_tape_cursor"],
        fill_exchange_ts_ns=fill_exchange_ns,
        target_day=target_day,
    )
    if payload["market_cursor_sha256"] != payload["market_cursor"][
        "canonical_identity_sha256"
    ] or payload["native_tape_cursor_sha256"] != payload["native_tape_cursor"][
        "canonical_identity_sha256"
    ]:
        raise StrictCheckpointError("checkpoint cursor identity drifted")
    if payload["checkpoint_event"] != (
        "strategy-visible exposure-increasing fill callback"
    ):
        raise StrictCheckpointError("checkpoint event semantics drifted")
    if payload["simulator_state_serialization_status"] != SIMULATOR_STATE_STATUS:
        raise StrictCheckpointError("simulator serialization capability was overstated")
    for field in (
        "simulator_state_artifact_present",
        "restore_execution_eligible",
        "economic_outcomes_read",
    ):
        if payload[field] is not False:
            raise StrictCheckpointError(f"checkpoint {field} must remain false")
    prefix_keys = (
        "target_day",
        "opportunity_id",
        "side",
        "role",
        "fill_visible_event_id",
        "fill_exchange_ts_ns",
        "fill_visible_ts_ns",
        "source_contract_sha256",
        "market_cursor_sha256",
        "native_tape_cursor_sha256",
        "strategy_state_identity_sha256",
        "ema_checkpoint_sha256",
        "baseline_identity_sha256",
        "config_sha256",
        "code_sha256",
        "model_sha256",
        "p3_sha256",
        "feature_dag_sha256",
        "execution_abi_sha256",
    )
    if canonical_sha256({key: payload[key] for key in prefix_keys}) != payload[
        "shared_prefix_identity_sha256"
    ]:
        raise StrictCheckpointError("shared-prefix identity drifted")
    _validate_embedded_hash(
        payload,
        field="canonical_checkpoint_sha256",
        role="shared-prefix checkpoint",
    )


def validate_restore_contract_payload(payload: Mapping[str, Any]) -> None:
    expected = {
        "schema_version",
        "identity",
        "side",
        "checkpoint_sha256",
        "shared_prefix_identity_sha256",
        "source_contract_sha256",
        "arm_count",
        "bindings",
        "required_engine",
        "required_exchange_book_queue_mode",
        "required_exchange_book_queue_scope",
        "normalized_l2_exact_authority",
        "cpp_restore_authority",
        "all_arms_share_one_prefix",
        "economic_outcome_fields",
        "economic_outcomes_read",
        "simulator_state_serialization_status",
        "metadata_restore_validation_eligible",
        "restore_execution_eligible",
        "canonical_restore_contract_sha256",
    }
    _exact_keys(payload, expected, "arm restore contract")
    if payload["schema_version"] != RESTORE_CONTRACT_SCHEMA or payload["identity"] != IDENTITY:
        raise StrictCheckpointError("arm restore identity drifted")
    side = str(payload["side"])
    if side not in {"BUY", "SELL"}:
        raise StrictCheckpointError("arm restore side is invalid")
    arms = BUY_ARMS if side == "BUY" else SELL_ARMS
    for key in (
        "checkpoint_sha256",
        "shared_prefix_identity_sha256",
        "source_contract_sha256",
    ):
        _require_sha256(payload[key], f"restore {key}")
    bindings = payload["bindings"]
    if payload["arm_count"] != 8 or not isinstance(bindings, list) or len(bindings) != 8:
        raise StrictCheckpointError("restore contract requires exactly eight arms")
    expected_binding_keys = {
        "arm_id",
        "side",
        "duration_ms",
        "duration_semantics",
        "checkpoint_sha256",
        "shared_prefix_identity_sha256",
        "source_contract_sha256",
        "strategy_state_identity_sha256",
        "ema_checkpoint_sha256",
    }
    actual_arms: list[str] = []
    shared_state: set[tuple[str, ...]] = set()
    for row in bindings:
        _exact_keys(row, expected_binding_keys, "arm restore binding")
        arm_id = str(row["arm_id"])
        actual_arms.append(arm_id)
        if row["side"] != side or row["duration_ms"] != ARM_DURATION_MS.get(arm_id):
            raise StrictCheckpointError("arm duration/side identity drifted")
        if row["checkpoint_sha256"] != payload["checkpoint_sha256"] or row[
            "shared_prefix_identity_sha256"
        ] != payload["shared_prefix_identity_sha256"] or row[
            "source_contract_sha256"
        ] != payload["source_contract_sha256"]:
            raise StrictCheckpointError("not all arms share one prefix")
        for key in (
            "checkpoint_sha256",
            "shared_prefix_identity_sha256",
            "source_contract_sha256",
            "strategy_state_identity_sha256",
            "ema_checkpoint_sha256",
        ):
            _require_sha256(row[key], f"arm binding {key}")
        shared_state.add(
            (
                row["checkpoint_sha256"],
                row["shared_prefix_identity_sha256"],
                row["source_contract_sha256"],
                row["strategy_state_identity_sha256"],
                row["ema_checkpoint_sha256"],
            )
        )
    if tuple(actual_arms) != arms or len(set(actual_arms)) != 8:
        raise StrictCheckpointError("arm set is missing, duplicated, or out of order")
    if len(shared_state) != 1:
        raise StrictCheckpointError("arm bindings do not share one state identity")
    if payload["required_engine"] != "python":
        raise StrictCheckpointError("C++ restore authority is forbidden")
    if payload["required_exchange_book_queue_mode"] != "strict" or payload[
        "required_exchange_book_queue_scope"
    ] != STRICT_QUEUE_SCOPE:
        raise StrictCheckpointError("restore strict-native identity drifted")
    if payload["economic_outcome_fields"] != [] or payload[
        "economic_outcomes_read"
    ] is not False:
        raise StrictCheckpointError("economic outcome fields are forbidden")
    if payload["simulator_state_serialization_status"] != SIMULATOR_STATE_STATUS:
        raise StrictCheckpointError("simulator serialization capability was overstated")
    required_bools = {
        "normalized_l2_exact_authority": False,
        "cpp_restore_authority": False,
        "all_arms_share_one_prefix": True,
        "metadata_restore_validation_eligible": True,
        "restore_execution_eligible": False,
    }
    for field, expected_value in required_bools.items():
        if payload[field] is not expected_value:
            raise StrictCheckpointError(f"restore contract {field} drifted")
    _validate_embedded_hash(
        payload,
        field="canonical_restore_contract_sha256",
        role="arm restore contract",
    )


def _load_json_without_duplicate_keys(path: Path) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise StrictCheckpointError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        payload = json.loads(
            path.read_text(encoding="ascii"),
            object_pairs_hook=reject_duplicates,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise StrictCheckpointError(f"invalid checkpoint admission JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise StrictCheckpointError("checkpoint admission must be a JSON object")
    return payload


def admit_checkpoint_metadata(
    destination: Path,
    *,
    source_contract: StrictNativeSourceContract,
    checkpoint: SharedPrefixCheckpointMetadata,
    restore_contract: ArmRestoreContract,
    verify_source_files_now: bool = True,
) -> dict[str, Any]:
    """Atomically admit bounded metadata, never simulator state or outcomes."""

    destination = Path(destination)
    if destination.exists():
        raise StrictCheckpointError(f"checkpoint admission already exists: {destination}")
    validate_checkpoint_metadata(checkpoint, source_contract=source_contract)
    validate_arm_restore_contract(restore_contract, checkpoint=checkpoint)
    if verify_source_files_now:
        validate_source_files(source_contract)
    body = {
        "schema_version": ADMISSION_SCHEMA,
        "identity": IDENTITY,
        "source_contract": source_contract.to_payload(),
        "checkpoint": checkpoint.to_payload(),
        "arm_restore_contract": restore_contract.to_payload(),
        "source_files_revalidated_at_admission": bool(verify_source_files_now),
        "simulator_state_serialization_status": SIMULATOR_STATE_STATUS,
        "metadata_only": True,
        "restore_execution_eligible": False,
        "economic_outcomes_read": False,
    }
    manifest = dict(body)
    manifest["canonical_manifest_sha256"] = canonical_sha256(body)
    encoded = canonical_json_bytes(manifest) + b"\n"
    if len(encoded) > MAX_ADMISSION_BYTES:
        raise StrictCheckpointError("checkpoint metadata admission exceeds size bound")

    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent / f".{destination.name}.{uuid.uuid4().hex}.partial"
    staging.mkdir(parents=False, exist_ok=False)
    try:
        manifest_path = staging / "manifest.json"
        manifest_path.write_bytes(encoded)
        (staging / "_SUCCESS").write_text(
            f"{file_sha256(manifest_path)}\n", encoding="ascii"
        )
        os.replace(staging, destination)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return manifest


def validate_checkpoint_admission(
    destination: Path,
    *,
    expected_source_contract_sha256: str | None = None,
    expected_checkpoint_sha256: str | None = None,
) -> dict[str, Any]:
    destination = Path(destination)
    manifest_path = destination / "manifest.json"
    success_path = destination / "_SUCCESS"
    if not manifest_path.is_file() or not success_path.is_file():
        raise StrictCheckpointError("checkpoint admission is incomplete")
    if {path.name for path in destination.iterdir()} != {"manifest.json", "_SUCCESS"}:
        raise StrictCheckpointError(
            "metadata-only admission contains an unexpected state or outcome artifact"
        )
    if manifest_path.stat().st_size > MAX_ADMISSION_BYTES:
        raise StrictCheckpointError("checkpoint admission exceeds size bound")
    marker = success_path.read_text(encoding="ascii").strip()
    if marker != file_sha256(manifest_path):
        raise StrictCheckpointError("checkpoint admission marker drifted")
    payload = _load_json_without_duplicate_keys(manifest_path)
    _exact_keys(
        payload,
        {
            "schema_version",
            "identity",
            "source_contract",
            "checkpoint",
            "arm_restore_contract",
            "source_files_revalidated_at_admission",
            "simulator_state_serialization_status",
            "metadata_only",
            "restore_execution_eligible",
            "economic_outcomes_read",
            "canonical_manifest_sha256",
        },
        "checkpoint admission",
    )
    if payload["schema_version"] != ADMISSION_SCHEMA or payload["identity"] != IDENTITY:
        raise StrictCheckpointError("checkpoint admission identity drifted")
    if payload["simulator_state_serialization_status"] != SIMULATOR_STATE_STATUS:
        raise StrictCheckpointError("checkpoint admission overstates restore support")
    if payload["metadata_only"] is not True or payload[
        "restore_execution_eligible"
    ] is not False or payload["economic_outcomes_read"] is not False:
        raise StrictCheckpointError("checkpoint admission capability drifted")
    _validate_embedded_hash(
        payload,
        field="canonical_manifest_sha256",
        role="checkpoint admission",
    )
    validate_source_contract_payload(payload["source_contract"])
    validate_checkpoint_payload(payload["checkpoint"])
    validate_restore_contract_payload(payload["arm_restore_contract"])
    source_sha = payload["source_contract"]["canonical_identity_sha256"]
    checkpoint_sha = payload["checkpoint"]["canonical_checkpoint_sha256"]
    restore = payload["arm_restore_contract"]
    if payload["checkpoint"]["source_contract_sha256"] != source_sha or restore[
        "source_contract_sha256"
    ] != source_sha:
        raise StrictCheckpointError("admitted source identity is not transitively bound")
    if restore["checkpoint_sha256"] != checkpoint_sha or restore[
        "shared_prefix_identity_sha256"
    ] != payload["checkpoint"]["shared_prefix_identity_sha256"]:
        raise StrictCheckpointError("admitted checkpoint identity is not transitively bound")
    if expected_source_contract_sha256 is not None and source_sha != _require_sha256(
        expected_source_contract_sha256, "expected_source_contract_sha256"
    ):
        raise StrictCheckpointError("admitted source contract is unexpected")
    if expected_checkpoint_sha256 is not None and checkpoint_sha != _require_sha256(
        expected_checkpoint_sha256, "expected_checkpoint_sha256"
    ):
        raise StrictCheckpointError("admitted checkpoint is unexpected")
    return payload
