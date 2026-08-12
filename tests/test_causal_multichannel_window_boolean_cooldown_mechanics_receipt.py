from __future__ import annotations

import json
from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_mechanics_receipt as receipt,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")


def _fixture(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    malformed_trace: bool = False,
    leaked_point_label: bool = False,
) -> None:
    execution_root = (
        root
        / f"support_identity={receipt.SUPPORT_IDENTITY}"
        / f"feature_block={receipt.FEATURE_BLOCK}"
        / f"execution_identity={receipt.strict_labels.FORMAL_EXECUTION_IDENTITY}"
    )
    day_root = execution_root / "days" / receipt.TARGET_DAY
    label_root = execution_root / "labels" / receipt.TARGET_DAY / "opportunity"
    snapshots = day_root / "assignment_snapshots.parquet"
    source = day_root / "source_contract.json"
    snapshots.parent.mkdir(parents=True, exist_ok=True)
    snapshots.write_bytes(b"parquet-placeholder")
    _write_json(source, {"source": "strict-native"})

    arm_bindings = []
    for index, arm_id in enumerate(receipt.features.BUY_DURATION_POLICY_IDS):
        unsupported = index == 0
        trace = []
        missing_count = 0
        if unsupported:
            missing_count = 1
            trace = [
                {
                    "order_id": "order-1",
                    "side": "BUY",
                    "price": 100.0,
                    "price_tick": 1000,
                    "activate_ts_ms": 1,
                    "status": "ACTIVE",
                    "reason": "missing_snapshot_level",
                    "asof_exchange_ts_ns": 1_000_000,
                    "segment_id": "segment-1",
                    "snapshot_min_tick": 999,
                    "snapshot_max_tick": 1001,
                }
            ]
            if malformed_trace:
                trace = []
        arm_payload = {
            "schema_version": receipt.shared_prefix.ARM_RESULT_SCHEMA_VERSION,
            "identity": receipt.IDENTITY,
            "arm_id": arm_id,
            "strict_execution_contract": {
                "exchange_book_queue_missing_count": missing_count,
                "exchange_book_queue_missing_trace": trace,
                "exchange_book_queue_invalidated_order_count": 0,
                "exchange_book_queue_ambiguous_event_count": 0,
                "strict_native_label_eligible": not unsupported,
                "economic_point_label_status": (
                    "unsupported_redacted" if unsupported else "eligible"
                ),
            },
            "fork_trace": {
                "side": "BUY",
                "assignment_to_washout_value_usdc": (
                    1.0 if (not unsupported or leaked_point_label) else None
                )
            },
        }
        arm_path = label_root / f"arm-{arm_id}.json"
        _write_json(arm_path, arm_payload)
        arm_bindings.append(
            {
                "arm_id": arm_id,
                "path": arm_path.name,
                "sha256": receipt._sha256(arm_path),
                "size_bytes": arm_path.stat().st_size,
            }
        )

    opportunity_path = label_root / "manifest.json"
    _write_json(
        opportunity_path,
        {
            "schema_version": receipt.shared_prefix.OPPORTUNITY_MANIFEST_SCHEMA_VERSION,
            "identity": receipt.IDENTITY,
            "arm_count": 8,
            "arms": arm_bindings,
        },
    )
    day_manifest = {
        "schema_version": receipt.strict_labels.DAY_SCHEMA_VERSION,
        "economic_outcomes_read_by_runner": False,
        "nested_oof_run": False,
        "action_authorized": False,
        "live_authorized": False,
        "assignment_snapshots": {
            "path": str(snapshots),
            "sha256": receipt._sha256(snapshots),
            "size_bytes": snapshots.stat().st_size,
        },
        "source_contract": {
            "path": str(source),
            "sha256": receipt._sha256(source),
            "size_bytes": source.stat().st_size,
        },
        "one_shot_label_manifests": [
            {
                "path": str(opportunity_path),
                "sha256": receipt._sha256(opportunity_path),
                "size_bytes": opportunity_path.stat().st_size,
            }
        ],
        "strict_native_queue": {
            "missing_queue_seed_count": 0,
            "invalidated_order_count": 0,
            "ambiguous_event_count": 0,
            "source_gap_events": 0,
        },
    }
    day_manifest_path = day_root / "manifest.json"
    _write_json(day_manifest_path, day_manifest)
    _write_json(
        day_root / "_SUCCESS",
        {"manifest_sha256": receipt._sha256(day_manifest_path)},
    )
    monkeypatch.setattr(
        receipt.strict_labels,
        "_validate_day_admission",
        lambda *_args, **_kwargs: day_manifest,
    )


def test_audit_day_accepts_complete_redacted_eight_arm_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fixture(tmp_path, monkeypatch)

    audit = receipt._audit_day(tmp_path)

    assert audit["arm_count"] == 8
    assert audit["unsupported_arm_count"] == 1
    assert audit["redacted_arm_count"] == 1
    assert audit["queue_missing_trace_row_count"] == 1
    assert audit["aggregate_economic_values_read"] is False
    assert audit["eligible_arm_point_values_accessed"] is False


def test_audit_day_rejects_truncated_missing_trace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fixture(tmp_path, monkeypatch, malformed_trace=True)

    with pytest.raises(receipt.MechanicsReceiptError, match="trace is incomplete"):
        receipt._audit_day(tmp_path)


def test_audit_day_rejects_unredacted_unsupported_point_label(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fixture(tmp_path, monkeypatch, leaked_point_label=True)

    with pytest.raises(receipt.MechanicsReceiptError, match="retained a point label"):
        receipt._audit_day(tmp_path)
