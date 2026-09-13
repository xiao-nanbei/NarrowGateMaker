from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from data.build_active_order_queue_tape import LogicalMessage
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_strict_labels as strict_labels_module,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_native_features import (
    NativeM2BookFeatureAccumulator,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_shared_prefix import (
    ARM_RESULT_SCHEMA_VERSION,
    OPPORTUNITY_MANIFEST_SCHEMA_VERSION,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_shared_prefix import (
    SCHEMA_VERSION as SHARED_PREFIX_SCHEMA_VERSION,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_strict_labels import (
    _STRICT_SOURCE_RESULT_FIELDS,
    DAY_SCHEMA_VERSION,
    FULL_SUPPORT_IDENTITY,
    REDUCED_SUPPORT_IDENTITY,
    RUNNER_IDENTITY,
    StrictLabelError,
    _bind_prebuilt_strict_native_cache,
    _canonical_sha256,
    _execution_amendment_binding,
    _frozen_audit_payload,
    _prebuild_strict_native_cache,
    _sha256,
    _SnapshotSpool,
    _target_72h_hours,
    _validate_day_admission,
    _validate_support_identity,
)


def test_strict_source_counter_result_abi_is_complete() -> None:
    assert set(_STRICT_SOURCE_RESULT_FIELDS) == {
        "source_gap_events",
        "sequence_gaps",
        "invalid_sequence_messages",
        "message_time_reversals",
        "event_timestamp_fallback_events",
        "receive_timestamp_fallback_events",
        "unknown_timestamp_source_events",
    }
    assert all(
        field.startswith("exchange_book_")
        for field in _STRICT_SOURCE_RESULT_FIELDS.values()
    )


def _install_v9_amendment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "execution-amendment-v9.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": (
                    "causal_multichannel_window_boolean_cooldown_duration_v2."
                    "execution_amendment.v9"
                ),
                "identity": "causal_multichannel_window_boolean_cooldown_duration_v2",
                "predecessor_execution_amendment": {
                    "sha256": strict_labels_module.EXECUTION_AMENDMENT_V8_SHA256,
                },
                "formal_identity_hardening_replacement": {
                    "shared_prefix_schema": SHARED_PREFIX_SCHEMA_VERSION,
                    "opportunity_manifest_schema": OPPORTUNITY_MANIFEST_SCHEMA_VERSION,
                    "arm_result_schema": ARM_RESULT_SCHEMA_VERSION,
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="ascii",
    )
    monkeypatch.setattr(strict_labels_module, "EXECUTION_AMENDMENT_V9", path)


def test_snapshot_spool_streams_parent_rows_to_parquet(tmp_path: Path) -> None:
    spool = _SnapshotSpool(day="2026-01-02")
    rows = [
        {
            "snapshot_id": f"snapshot-{index}",
            "assignment_id": f"assignment-{index}",
            "fill_event_id": f"fill-{index}",
            "client_order_id": f"order-{index}",
            "lineage_id": f"lineage-{index}",
            "lineage_revision": index,
            "partial_fill_ordinal": index + 1,
            "partial_fill_qty_btc": 0.001,
            "visibility_profile": "historical_exchange_time",
            "receive_time_transport_eligible": False,
            "source_bundle_sha256": "a" * 64,
            "feature_block": "M2",
            "m0_context_json": json.dumps({"assignment_ts_ns": index + 1}),
            "feature_row_json": json.dumps({"value::mid": 100.0 + index}),
            "snapshot_payload_json": "{}",
            "snapshot_payload_sha256": "b" * 64,
            "policy_input_valid": True,
            "fallback_policy_id": None,
            "fallback_reason": None,
            "economic_outcomes_read": False,
        }
        for index in range(3)
    ]
    for row in rows:
        spool.append(row)
    spool_path = spool.path
    destination = tmp_path / "snapshots.parquet"

    assert spool.write_parquet(destination, batch_rows=2) == 3

    observed = pd.read_parquet(destination)
    normalized = observed.astype(object).where(pd.notna(observed), None)
    assert normalized.to_dict(orient="records") == rows
    assert not spool_path.exists()


def test_strict_label_cache_is_prebuilt_and_read_only_before_fork(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_root = tmp_path / "raw"
    target = datetime(2026, 1, 2, tzinfo=UTC)
    for offset in range(-24, 48):
        hour = target + timedelta(hours=offset)
        source = (
            raw_root
            / "binance_futures"
            / hour.strftime("%Y-%m-%d")
            / hour.strftime("%H")
            / "BTCUSDC_orderbook.parquet.zst"
        )
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(hour.isoformat().encode("ascii"))
    parser_calls: list[Path] = []

    def fake_parser(path: Path, tick_size: float):
        parser_calls.append(path)
        hour = datetime.strptime(
            f"{path.parent.parent.name} {path.parent.name}",
            "%Y-%m-%d %H",
        ).replace(tzinfo=UTC)
        timestamp_ms = int(hour.timestamp() * 1_000) + 100
        yield LogicalMessage(
            event_type="snapshot",
            exchange_ts_ms=timestamp_ms,
            receive_time_ms=timestamp_ms + 2,
            receive_time_ns=(timestamp_ms + 2) * 1_000_000,
            event_time_ms=timestamp_ms - 1,
            transaction_time_ms=timestamp_ms,
            first_update_id=None,
            final_update_id=None,
            previous_final_update_id=None,
            last_update_id=100,
            levels=[("bid", 900_000, 1.0), ("ask", 900_001, 1.0)],
        )

    monkeypatch.setattr(
        "data.build_active_order_queue_tape.iter_cryptohft_logical_messages",
        fake_parser,
    )
    tape = CryptoHFTExchangeBookTape(
        raw_root=raw_root,
        day="2026-01-02",
        symbol="BTCUSDC",
        tick_size=0.1,
        warmup_hours=24,
        continuation_hours=24,
        strict_complete=True,
        cache_dir=tmp_path / "cache",
    )

    read_only_tape, audit = _prebuild_strict_native_cache(tape)

    assert len(parser_calls) == 72
    assert read_only_tape.cache_read_only is True
    assert audit["cache_contract"]["complete_hour_count"] == 72
    assert audit["source_scheduler_stats"]["consumed_events"] == 72
    assert audit["validation_cache_stats"]["hour_hits"] == 72
    assert len(tuple(read_only_tape)) == 72


def test_full_and_reduced_source_support_identities_cannot_mix() -> None:
    spec = {
        "ordered_utc_days": {
            "prefix40": [f"2026-01-{day:02d}" for day in range(1, 41)],
            "added10": [f"2026-02-{day:02d}" for day in range(1, 11)],
        },
        "source_separation": {
            "strict_native_2026": {
                "reduced_support_days": ["2026-01-02", "2026-02-03"]
            }
        },
    }
    _validate_support_identity(
        day="2026-01-01",
        support_identity=FULL_SUPPORT_IDENTITY,
        spec=spec,
    )
    _validate_support_identity(
        day="2026-01-02",
        support_identity=REDUCED_SUPPORT_IDENTITY,
        spec=spec,
    )
    with pytest.raises(StrictLabelError, match="belongs to"):
        _validate_support_identity(
            day="2026-01-02",
            support_identity=FULL_SUPPORT_IDENTITY,
            spec=spec,
        )


def test_prebuilt_target_receipt_bypasses_duplicate_source_scan(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    target = datetime(2026, 1, 2, tzinfo=UTC)
    for offset in range(-24, 48):
        hour = target + timedelta(hours=offset)
        source = (
            raw_root
            / "binance_futures"
            / hour.strftime("%Y-%m-%d")
            / hour.strftime("%H")
            / "BTCUSDC_orderbook.parquet.zst"
        )
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(b"receipt-bound")
    cache_root = tmp_path / "cache"
    tape = CryptoHFTExchangeBookTape(
        raw_root=raw_root,
        day="2026-01-02",
        symbol="BTCUSDC",
        tick_size=0.1,
        warmup_hours=24,
        continuation_hours=24,
        strict_complete=True,
        cache_dir=cache_root,
    )
    segment_path = tmp_path / "segment.json"
    segment_path.write_text("{}\n", encoding="ascii")
    receipt = {
        "schema_version": (
            "causal_multichannel_window_boolean_cooldown_duration_v2."
            "strict_label_panel_runner.v1.native_cache_target_72h_receipt.v3"
        ),
        "identity": "causal_multichannel_window_boolean_cooldown_duration_v2",
        "day": "2026-01-02",
        "complete_hour_count": 72,
        "native_cache_root": str(cache_root.resolve()),
        "hours": [
            {"utc_hour": utc_hour}
            for utc_hour in _target_72h_hours("2026-01-02")
        ],
        "segment_receipt_path": str(segment_path.resolve()),
        "segment_receipt_sha256": _sha256(segment_path),
        "derived_from_validated_segment_hours": True,
        "scheduler_replay_count": 0,
        "economic_outcomes_read": False,
        "arms_run": False,
        "nested_oof_run": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    receipt["canonical_identity_sha256"] = _canonical_sha256(receipt)
    receipt_path = tmp_path / "target.json"
    receipt_path.write_text(
        json.dumps(receipt, sort_keys=True) + "\n",
        encoding="ascii",
    )

    read_only, audit = _bind_prebuilt_strict_native_cache(
        tape,
        day="2026-01-02",
        receipt_path=receipt_path,
    )

    assert read_only.cache_read_only is True
    assert audit["complete_hour_count"] == 72
    assert audit["target_scheduler_replay_count"] == 0
    assert audit["segment_scheduler_replay_count"] == 1


def test_native_audit_mapping_proxy_is_manifest_serializable() -> None:
    accumulator = NativeM2BookFeatureAccumulator()
    assert accumulator.unobserved_reason_counts is not None
    accumulator.unobserved_reason_counts["source_gap"] = 3

    payload = _frozen_audit_payload(accumulator.freeze())

    assert payload["unobserved_reason_counts"] == {"source_gap": 3}
    json.dumps(payload, sort_keys=True, allow_nan=False)


def _write_day_admission(
    root: Path,
    *,
    max_opportunities: int | None,
    missing_trace: list[dict[str, object]],
) -> Path:
    day = "2026-01-02"
    root.mkdir()
    snapshots = root / "assignment_snapshots.parquet"
    pd.DataFrame({"snapshot_id": ["snapshot-1"]}).to_parquet(snapshots)
    source = root / "source_contract.json"
    source.write_text("{}\n", encoding="ascii")
    label = root.parent / "label.json"
    label.write_text(
        json.dumps({"schema_version": OPPORTUNITY_MANIFEST_SCHEMA_VERSION}) + "\n",
        encoding="ascii",
    )
    target_end_ms = int(
        (
            datetime.fromisoformat(day).replace(tzinfo=UTC)
            + timedelta(days=1)
        ).timestamp()
        * 1_000
    )
    manifest = {
        "schema_version": DAY_SCHEMA_VERSION,
        "identity": RUNNER_IDENTITY,
        "target_day": day,
        "feature_block": "M2",
        "source_support_identity": FULL_SUPPORT_IDENTITY,
        "max_opportunities": max_opportunities,
        "execution_amendment": _execution_amendment_binding(),
        "shared_prefix_execution_audit": {
            "opportunities_dispatched": (
                1 if max_opportunities is not None else 0
            ),
            "opportunities_resumed": 0,
        },
        "parent_stop_audit": {
            "configured_stop_ts_ms": target_end_ms,
            "triggered": True,
            "trigger_ts_ms": (
                target_end_ms
                if max_opportunities is None
                else target_end_ms - 1
            ),
            "reason": (
                "target_day_end"
                if max_opportunities is None
                else "max_opportunities_reached"
            ),
            "target_day_boundary_observed": max_opportunities is None,
            "new_assignments_after_target_day_boundary": 0,
        },
        "strict_native_queue": {
            "missing_queue_seed_count": len(missing_trace),
            "missing_queue_seed_trace": missing_trace,
            "source_gap_events": 0,
            "missing_trace_unbounded": True,
        },
        "strict_native_source_counters": {
            field: 0
            for field in (
                "source_gap_events",
                "sequence_gaps",
                "invalid_sequence_messages",
                "message_time_reversals",
                "event_timestamp_fallback_events",
                "receive_timestamp_fallback_events",
                "unknown_timestamp_source_events",
            )
        },
        "assignment_snapshots": {"sha256": _sha256(snapshots)},
        "source_contract": {"sha256": _sha256(source)},
        "one_shot_label_manifests": [
            {
                "path": str(label),
                "size_bytes": label.stat().st_size,
                "sha256": _sha256(label),
            }
        ],
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True) + "\n",
        encoding="ascii",
    )
    (root / "_SUCCESS").write_text(
        json.dumps({"manifest_sha256": _sha256(manifest_path)}) + "\n",
        encoding="ascii",
    )
    return root


def test_engineering_admission_retains_complete_queue_missing_trace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_v9_amendment(tmp_path, monkeypatch)
    directory = _write_day_admission(
        tmp_path / "day",
        max_opportunities=1,
        missing_trace=[{"reason": "outside_snapshot_range"}],
    )

    observed = _validate_day_admission(
        directory,
        expected_day="2026-01-02",
        expected_feature_block="M2",
        expected_support_identity=FULL_SUPPORT_IDENTITY,
        expected_max_opportunities=1,
    )

    assert observed["strict_native_queue"]["missing_queue_seed_count"] == 1


def test_formal_admission_rejects_any_queue_seed_support_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_v9_amendment(tmp_path, monkeypatch)
    directory = _write_day_admission(
        tmp_path / "day",
        max_opportunities=None,
        missing_trace=[{"reason": "outside_snapshot_range"}],
    )

    with pytest.raises(StrictLabelError, match="incomplete strict-native"):
        _validate_day_admission(
            directory,
            expected_day="2026-01-02",
            expected_feature_block="M2",
            expected_support_identity=FULL_SUPPORT_IDENTITY,
            expected_max_opportunities=None,
        )
