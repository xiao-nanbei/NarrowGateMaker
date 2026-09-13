from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import execution.order_lifecycle_remote_spool_v2 as remote_spool
from execution.order_lifecycle import QuantityWeightedOrderLifecycle
from execution.order_lifecycle_journal_storage_v2 import BOUNDED_REMOTE_SPOOL
from execution.order_lifecycle_live_writer_v2 import OrderLifecycleLiveWriterV2
from models.replay.baseline_epoch_manifest import (
    REQUIRED_IDENTITY_FIELDS,
    epoch_identity_sha256,
)


def _closed_remote_spool(tmp_path: Path) -> tuple[Path, Path]:
    epoch_id = "prospective-remote-1"
    identity = {
        field: f"{index + 1:064x}"
        for index, field in enumerate(REQUIRED_IDENTITY_FIELDS)
    }
    identity_sha256 = epoch_identity_sha256(identity)
    bounds = {"max_duration_s": 60.0, "max_bytes": 1024 * 1024}
    epoch_root = tmp_path / "prospective_baseline_epochs" / epoch_id
    epoch_root.mkdir(parents=True)
    (epoch_root / "epoch_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "narrowgate_prospective_baseline_epoch.v1",
                "epoch_id": epoch_id,
                "binding_status": "fully_bound",
                "storage_profile": BOUNDED_REMOTE_SPOOL,
                "remote_spool_only": True,
                "local_admission_complete": False,
                "collection_bounds": bounds,
                "identity": identity,
                "identity_sha256": identity_sha256,
            }
        ),
        encoding="utf-8",
    )
    journal_root = tmp_path / "order_lifecycle_journal_v2"
    runtime = OrderLifecycleLiveWriterV2(
        journal_root,
        session_id=epoch_id,
        baseline_epoch_id=epoch_id,
        runtime_identity={
            "baseline_epoch_id": epoch_id,
            "baseline_epoch_identity_sha256": identity_sha256,
            "storage_profile": BOUNDED_REMOTE_SPOOL,
            "local_admission_complete": False,
            "collection_bounds": bounds,
            **identity,
        },
        storage_format="jsonl",
        heartbeat_interval_s=0.01,
        storage_profile=BOUNDED_REMOTE_SPOOL,
        epoch_root=epoch_root,
        session_max_duration_s=60.0,
        session_max_bytes=1024 * 1024,
    )
    order = SimpleNamespace(
        client_order_id="client-1",
        order_id=0,
        symbol="BTCUSDC",
        side=SimpleNamespace(value="BUY"),
        lifecycle=QuantityWeightedOrderLifecycle(0.001, 1_000_000_000),
    )
    assert runtime.enqueue_order_event(order, "submit") is True
    runtime.close(drain_timeout_s=1.0)
    return journal_root / f"session-{epoch_id}", epoch_root


def _allow_tmp(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = tmp_path.resolve()

    def validate(path, *, allowlisted_roots, field_name):
        del allowlisted_roots, field_name
        return Path(path).resolve(), root

    monkeypatch.setattr(remote_spool, "validate_remote_spool_path", validate)


def test_closed_remote_spool_builds_manifest_without_admission_or_transfer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_root, epoch_root = _closed_remote_spool(tmp_path)
    _allow_tmp(monkeypatch, tmp_path)

    payload = remote_spool.inspect_bounded_remote_spool(
        session_root=session_root,
        epoch_root=epoch_root,
        allowlisted_roots=(tmp_path,),
    )
    assert payload["file_count"] > 0
    assert payload["payload_bytes"] > 0
    assert payload["transfer_executed"] is False
    assert payload["local_orico_admission_complete"] is False
    assert payload["formal_collection_valid"] is False
    assert payload["seven_tape_capture_contract_modified"] is False
    assert len(payload["rsync_files_from"]) == payload["file_count"]

    output = remote_spool.publish_bounded_remote_spool_manifest(
        session_root=session_root,
        epoch_root=epoch_root,
        allowlisted_roots=(tmp_path,),
    )
    published = json.loads(output.read_text(encoding="utf-8"))
    assert published["session_id"] == "prospective-remote-1"
    assert published["transfer_executed"] is False


def test_remote_spool_manifest_rejects_unclosed_or_locally_admitted_tape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_root, epoch_root = _closed_remote_spool(tmp_path)
    _allow_tmp(monkeypatch, tmp_path)
    health_path = session_root / "live_health.json"
    health = json.loads(health_path.read_text(encoding="utf-8"))
    health["state"] = "collecting"
    health_path.write_text(json.dumps(health), encoding="utf-8")
    with pytest.raises(ValueError, match="has not reached bounded_complete"):
        remote_spool.inspect_bounded_remote_spool(
            session_root=session_root,
            epoch_root=epoch_root,
            allowlisted_roots=(tmp_path,),
        )

    health["state"] = "closed"
    health["formal_collection_valid"] = True
    health_path.write_text(json.dumps(health), encoding="utf-8")
    with pytest.raises(ValueError, match="must not claim local formal admission"):
        remote_spool.inspect_bounded_remote_spool(
            session_root=session_root,
            epoch_root=epoch_root,
            allowlisted_roots=(tmp_path,),
        )
