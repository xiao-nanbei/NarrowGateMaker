from __future__ import annotations

import copy
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_strict_checkpoint as subject,
)


def _file_rows(tmp_path: Path) -> dict[tuple[str, int], dict[str, object]]:
    rows: dict[tuple[str, int], dict[str, object]] = {}
    for offset in (-1, 0, 1):
        day = (date(2026, 1, 2) + timedelta(days=offset)).isoformat()
        for hour in range(24):
            path = (
                tmp_path
                / "raw"
                / "binance_futures"
                / day
                / f"{hour:02d}"
                / "BTCUSDC_orderbook.parquet.zst"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(f"{day}/{hour:02d}/raw-snapshot-delta".encode("ascii"))
            stat = path.stat()
            rows[(day, hour)] = {
                "path": str(path),
                "size_bytes": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "sha256": subject.file_sha256(path),
            }
    return rows


def _tape_identity(
    tape_day: str,
    source_days: tuple[str, ...],
    rows: dict[tuple[str, int], dict[str, object]],
) -> dict[str, object]:
    return {
        "schema_version": subject.RAW_TAPE_SCHEMA,
        "day": tape_day,
        "symbol": "BTCUSDC",
        "market_id": "binance_futures:perpetual:BTCUSDC",
        "tick_size": 0.1,
        "exchange_clock": "transaction_time_with_event_then_receive_fallback",
        "warmup_hours": 24,
        "continuation_hours": 0,
        "strict_complete": True,
        "missing_paths": [],
        "files": [
            copy.deepcopy(rows[(day, hour)])
            for day in source_days
            for hour in range(24)
        ],
    }


def _source_contract(tmp_path: Path) -> subject.StrictNativeSourceContract:
    rows = _file_rows(tmp_path)
    return subject.build_strict_native_source_contract(
        target_day="2026-01-02",
        target_tape_identity=_tape_identity(
            "2026-01-02", ("2026-01-01", "2026-01-02"), rows
        ),
        continuation_tape_identity=_tape_identity(
            "2026-01-03", ("2026-01-02", "2026-01-03"), rows
        ),
        parser_identity_sha256="a" * 64,
    )


def _checkpoint(
    contract: subject.StrictNativeSourceContract,
) -> subject.SharedPrefixCheckpointMetadata:
    fill_exchange_ns = int(
        datetime(2026, 1, 2, 12, 0, 0, tzinfo=UTC).timestamp()
        * 1_000_000_000
    )
    fill_visible_ns = fill_exchange_ns + 2_000_000
    target_identity = contract.day("target")
    return subject.SharedPrefixCheckpointMetadata(
        target_day="2026-01-02",
        opportunity_id="2026-01-02:BUY:fill-17",
        side="BUY",
        role="add",
        fill_visible_event_id="private-fill-visible-17",
        fill_exchange_ts_ns=fill_exchange_ns,
        fill_visible_ts_ns=fill_visible_ns,
        source_contract_sha256=contract.canonical_identity_sha256,
        market_cursor=subject.MarketCursorMetadata(
            stream_identity_sha256="b" * 64,
            event_ordinal=12_345,
            market_generation=456,
            exchange_ts_ns=fill_exchange_ns - 8_000_000,
            receive_ts_ns=fill_exchange_ns - 5_000_000,
            feature_ready_ts_ns=fill_exchange_ns - 1_000_000,
            visibility_clock="receive_feature_ready",
        ),
        native_tape_cursor=subject.NativeTapeCursorMetadata(
            source_contract_sha256=contract.canonical_identity_sha256,
            raw_day_identity_sha256=(
                target_identity.raw_snapshot_delta_identity_sha256
            ),
            utc_day="2026-01-02",
            hour=11,
            tape_event_ordinal=987_654,
            source_event_ordinal=4_321,
            exchange_ts_ns=fill_exchange_ns - 500_000,
            segment_id=8,
            last_update_id=999_001,
        ),
        strategy_state_identity_sha256="c" * 64,
        ema_checkpoint_sha256="d" * 64,
        baseline_identity_sha256="e" * 64,
        config_sha256="f" * 64,
        code_sha256="1" * 64,
        model_sha256="2" * 64,
        p3_sha256="3" * 64,
        feature_dag_sha256="4" * 64,
        execution_abi_sha256="5" * 64,
    )


def test_source_contract_binds_exactly_d_minus_1_d_and_d_plus_1(
    tmp_path: Path,
) -> None:
    contract = _source_contract(tmp_path)

    assert [row.utc_day for row in contract.ordered_days] == [
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
    ]
    assert [row.role for row in contract.ordered_days] == [
        "warmup",
        "target",
        "continuation",
    ]
    assert sum(len(row.hours) for row in contract.ordered_days) == 72
    assert contract.to_payload()["engine"] == "python"
    assert not contract.to_payload()["normalized_l2_exact_authority"]
    subject.validate_source_files(contract)


def test_source_contract_accepts_one_executable_continuation_tape(
    tmp_path: Path,
) -> None:
    rows = _file_rows(tmp_path)
    tape = _tape_identity(
        "2026-01-02",
        ("2026-01-01", "2026-01-02", "2026-01-03"),
        rows,
    )
    tape["continuation_hours"] = 24

    contract = subject.build_strict_native_source_contract_from_single_tape(
        target_day="2026-01-02",
        tape_identity=tape,
        parser_identity_sha256="a" * 64,
    )

    assert len(contract.tape_bindings) == 1
    assert contract.tape_bindings[0].covered_days == (
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
    )
    assert sum(len(day.hours) for day in contract.ordered_days) == 72


@pytest.mark.parametrize("failure", ["missing_hour", "duplicate_day", "out_of_order"])
def test_source_contract_rejects_incomplete_or_nonconsecutive_tapes(
    tmp_path: Path,
    failure: str,
) -> None:
    rows = _file_rows(tmp_path)
    target = _tape_identity(
        "2026-01-02", ("2026-01-01", "2026-01-02"), rows
    )
    continuation = _tape_identity(
        "2026-01-03", ("2026-01-02", "2026-01-03"), rows
    )
    if failure == "missing_hour":
        continuation["files"].pop()
    elif failure == "duplicate_day":
        continuation["day"] = "2026-01-02"
    else:
        continuation["files"][0], continuation["files"][1] = (
            continuation["files"][1],
            continuation["files"][0],
        )

    with pytest.raises(subject.StrictCheckpointError):
        subject.build_strict_native_source_contract(
            target_day="2026-01-02",
            target_tape_identity=target,
            continuation_tape_identity=continuation,
            parser_identity_sha256="a" * 64,
        )


def test_source_contract_rejects_overlap_drift_and_normalized_identity(
    tmp_path: Path,
) -> None:
    rows = _file_rows(tmp_path)
    target = _tape_identity(
        "2026-01-02", ("2026-01-01", "2026-01-02"), rows
    )
    continuation = _tape_identity(
        "2026-01-03", ("2026-01-02", "2026-01-03"), rows
    )
    continuation["files"][0]["sha256"] = "9" * 64
    with pytest.raises(subject.StrictCheckpointError, match="inconsistent"):
        subject.build_strict_native_source_contract(
            target_day="2026-01-02",
            target_tape_identity=target,
            continuation_tape_identity=continuation,
            parser_identity_sha256="a" * 64,
        )

    target["schema_version"] = "normalized_l2_100ms_v1"
    with pytest.raises(subject.StrictCheckpointError, match="non-native"):
        subject.build_strict_native_source_contract(
            target_day="2026-01-02",
            target_tape_identity=target,
            continuation_tape_identity=_tape_identity(
                "2026-01-03", ("2026-01-02", "2026-01-03"), rows
            ),
            parser_identity_sha256="a" * 64,
        )


def test_checkpoint_is_immutable_causal_and_hash_bound(tmp_path: Path) -> None:
    contract = _source_contract(tmp_path)
    checkpoint = _checkpoint(contract)
    subject.validate_checkpoint_metadata(checkpoint, source_contract=contract)

    with pytest.raises(FrozenInstanceError):
        checkpoint.side = "SELL"  # type: ignore[misc]
    changed = replace(
        checkpoint,
        market_cursor=replace(checkpoint.market_cursor, event_ordinal=12_346),
    )
    assert changed.canonical_checkpoint_sha256 != checkpoint.canonical_checkpoint_sha256

    future_cursor = replace(
        checkpoint,
        market_cursor=replace(
            checkpoint.market_cursor,
            feature_ready_ts_ns=checkpoint.fill_visible_ts_ns + 1,
        ),
    )
    with pytest.raises(subject.StrictCheckpointError, match="causal visibility"):
        subject.validate_checkpoint_metadata(future_cursor, source_contract=contract)


def test_all_eight_arm_bindings_share_one_prefix_and_have_no_outcomes(
    tmp_path: Path,
) -> None:
    contract = _source_contract(tmp_path)
    checkpoint = _checkpoint(contract)
    restore = subject.build_arm_restore_contract(checkpoint)

    assert tuple(row.arm_id for row in restore.bindings) == subject.BUY_ARMS
    assert len({row.checkpoint_sha256 for row in restore.bindings}) == 1
    assert len({row.shared_prefix_identity_sha256 for row in restore.bindings}) == 1
    payload = restore.to_payload()
    assert payload["economic_outcome_fields"] == []
    assert payload["simulator_state_serialization_status"] == (
        subject.SIMULATOR_STATE_STATUS
    )
    assert not payload["restore_execution_eligible"]

    payload["bindings"][0]["terminal_pnl"] = 1.0
    payload["canonical_restore_contract_sha256"] = subject.canonical_sha256(
        {
            key: value
            for key, value in payload.items()
            if key != "canonical_restore_contract_sha256"
        }
    )
    with pytest.raises(subject.StrictCheckpointError, match="schema mismatch"):
        subject.validate_restore_contract_payload(payload)


def test_restore_contract_rejects_cpp_or_divergent_prefix(tmp_path: Path) -> None:
    contract = _source_contract(tmp_path)
    checkpoint = _checkpoint(contract)
    payload = subject.build_arm_restore_contract(checkpoint).to_payload()
    payload["required_engine"] = "cpp"
    payload["canonical_restore_contract_sha256"] = subject.canonical_sha256(
        {
            key: value
            for key, value in payload.items()
            if key != "canonical_restore_contract_sha256"
        }
    )
    with pytest.raises(subject.StrictCheckpointError, match=r"C\+\+"):
        subject.validate_restore_contract_payload(payload)

    payload = subject.build_arm_restore_contract(checkpoint).to_payload()
    payload["bindings"][7]["shared_prefix_identity_sha256"] = "8" * 64
    payload["canonical_restore_contract_sha256"] = subject.canonical_sha256(
        {
            key: value
            for key, value in payload.items()
            if key != "canonical_restore_contract_sha256"
        }
    )
    with pytest.raises(subject.StrictCheckpointError, match="share one prefix"):
        subject.validate_restore_contract_payload(payload)


def test_metadata_admission_is_atomic_bounded_and_not_restore_ready(
    tmp_path: Path,
) -> None:
    contract = _source_contract(tmp_path)
    checkpoint = _checkpoint(contract)
    restore = subject.build_arm_restore_contract(checkpoint)
    destination = tmp_path / "admitted" / checkpoint.canonical_checkpoint_sha256

    admitted = subject.admit_checkpoint_metadata(
        destination,
        source_contract=contract,
        checkpoint=checkpoint,
        restore_contract=restore,
    )
    loaded = subject.validate_checkpoint_admission(
        destination,
        expected_source_contract_sha256=contract.canonical_identity_sha256,
        expected_checkpoint_sha256=checkpoint.canonical_checkpoint_sha256,
    )

    assert admitted == loaded
    assert set(path.name for path in destination.iterdir()) == {
        "manifest.json",
        "_SUCCESS",
    }
    assert loaded["metadata_only"]
    assert not loaded["restore_execution_eligible"]
    assert loaded["simulator_state_serialization_status"] == (
        "identity_only_not_serialized"
    )
    (destination / "simulator_state.pkl").write_bytes(b"not admissible")
    with pytest.raises(subject.StrictCheckpointError, match="unexpected state"):
        subject.validate_checkpoint_admission(destination)
    (destination / "simulator_state.pkl").unlink()
    with pytest.raises(subject.StrictCheckpointError, match="already exists"):
        subject.admit_checkpoint_metadata(
            destination,
            source_contract=contract,
            checkpoint=checkpoint,
            restore_contract=restore,
        )
