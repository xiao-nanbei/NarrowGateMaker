from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from execution.order_lifecycle import QuantityWeightedOrderLifecycle
from execution.order_lifecycle_journal_v2 import OrderLifecycleJournalV2SourceCallback
from execution.order_lifecycle_journal_writer_v2 import (
    OrderLifecycleJournalRuntimeBridgeV2,
    OrderLifecycleJournalWriterV2,
)
from scripts.admit_prospective_lifecycle_journal_part import (
    JournalPartAdmissionError,
    _load_performance_context,
    _validate_exact_part_metadata,
    _validate_remote_session_root,
    validate_downloaded_part,
)
from scripts.orchestrate_prospective_lifecycle_remote_release import (
    DEPLOYMENT_BINDING_SCHEMA_VERSION,
    EVIDENCE_SCHEMA_VERSION,
    _canonical_sha256,
    _seal_receipt,
)

EXAMPLE_REMOTE_ROOT = "/srv/example-live/NarrowGate_BTCUSDC"
EXAMPLE_REMOTE_JOURNAL_ROOT = (
    f"{EXAMPLE_REMOTE_ROOT}/formal_collection/order_lifecycle_journal_v2"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _make_part(root: Path) -> tuple[Path, dict[str, object]]:
    session_id = "prospective-test-epoch"
    runtime_identity = {
        "baseline_epoch_id": "prospective-test-epoch",
        "runtime_code_sha256": "a" * 64,
        "execution_abi_sha256": "b" * 64,
    }
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    with OrderLifecycleJournalWriterV2(
        root,
        session_id=session_id,
        runtime_identity=runtime_identity,
        start_heartbeat=False,
    ) as writer:
        bridge = OrderLifecycleJournalRuntimeBridgeV2(writer)
        bridge.register_lifecycle(
            lifecycle_id="prospective-test-epoch:order-1",
            runtime_source="replay",
            client_order_id="order-1",
            exchange_order_id=None,
            symbol="BTCUSDC",
            side="BUY",
        )
        result = bridge.submit_callback(
            lifecycle_id="prospective-test-epoch:order-1",
            lifecycle=lifecycle,
            callback=OrderLifecycleJournalV2SourceCallback(
                callback_id="submit-1",
                callback_type="submit",
                received_ts_ns=1_000_000_000,
                exchange_ts_ns=None,
            ),
        )
        assert result.status == "committed"

    session = root / f"session-{session_id}"
    manifest_path = next((session / "parts").glob("*.manifest.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    data_path = session / "parts" / manifest["data_file"]
    identity_path = session / "runtime_identity.json"
    metadata = {
        "session_root": "/remote/session",
        "session_id": session_id,
        "manifest_relative": f"parts/{manifest_path.name}",
        "manifest_sha256": _sha256(manifest_path),
        "manifest_size_bytes": manifest_path.stat().st_size,
        "data_relative": f"parts/{data_path.name}",
        "data_sha256": _sha256(data_path),
        "data_size_bytes": data_path.stat().st_size,
        "runtime_identity_relative": "runtime_identity.json",
        "runtime_identity_file_sha256": _sha256(identity_path),
        "runtime_identity_size_bytes": identity_path.stat().st_size,
        "batch_id": manifest["batch_id"],
        "row_count": manifest["row_count"],
        "journal_schema_sha256": manifest["journal_schema_sha256"],
        "runtime_identity_sha256": manifest["runtime_identity_sha256"],
        "first_lifecycle_sequence": manifest["first_lifecycle_sequence"],
        "checkpoint_before_last_emitted_sequence": manifest["checkpoint_before"][
            "last_emitted_sequence"
        ],
        "checkpoint_before_last_event_id": manifest["checkpoint_before"][
            "last_event_id"
        ],
        "committed_ts_ns": manifest["committed_ts_ns"],
        "economic_outcomes_read": False,
    }
    return session, metadata


def test_downloaded_part_replays_through_authoritative_writer(tmp_path: Path) -> None:
    session, metadata = _make_part(tmp_path / "source")

    result = validate_downloaded_part(session, metadata)

    assert result["writer_recovery_validated"] is True
    assert result["row_count"] == 1
    assert result["economic_outcomes_read"] is False


def test_downloaded_part_rejects_data_tampering(tmp_path: Path) -> None:
    session, metadata = _make_part(tmp_path / "source")
    data_path = session / str(metadata["data_relative"])
    data_path.write_bytes(data_path.read_bytes() + b"tamper")

    with pytest.raises(JournalPartAdmissionError, match="SHA256"):
        validate_downloaded_part(session, metadata)


def test_single_part_metadata_requires_independently_recoverable_sequence_one(
    tmp_path: Path,
) -> None:
    _session, metadata = _make_part(tmp_path / "source")
    metadata["first_lifecycle_sequence"] = 2

    with pytest.raises(JournalPartAdmissionError, match="requires sequence 1"):
        _validate_exact_part_metadata(metadata)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("batch_id", "../escape", "batch ID"),
        ("manifest_relative", "../part.json", "journal path"),
        ("data_relative", "parts/not-the-bound-part.parquet", "journal path"),
        ("runtime_identity_sha256", "not-a-hash", "SHA256"),
    ],
)
def test_single_part_metadata_rejects_schema_and_path_drift(
    tmp_path: Path,
    field: str,
    value: str,
    message: str,
) -> None:
    _session, metadata = _make_part(tmp_path / "source")
    metadata[field] = value

    with pytest.raises(JournalPartAdmissionError, match=message):
        _validate_exact_part_metadata(metadata)


def test_performance_context_binds_exact_part_and_rejects_symlink(
    tmp_path: Path,
) -> None:
    _session, metadata = _make_part(tmp_path / "source")
    release_sha = "c" * 64
    remote_identity_sha = "d" * 64
    binding = {
        "schema_version": DEPLOYMENT_BINDING_SCHEMA_VERSION,
        "release_manifest_sha256": release_sha,
        "remote_identity_sha256": remote_identity_sha,
        "mutation_plan_identity_sha256": "e" * 64,
        "deployment_instance_id": None,
        "remote": "ec2-user" + "@example.test",
        "remote_root": EXAMPLE_REMOTE_ROOT,
        "isolated_stage_root": f"{EXAMPLE_REMOTE_ROOT}/.releases/test",
        "successor_venv": f"{EXAMPLE_REMOTE_ROOT}/.releases/test/.venv-successor",
        "backup_root": f"{EXAMPLE_REMOTE_ROOT}/deploy_backups/test",
    }
    committed_s = int(metadata["committed_ts_ns"]) / 1_000_000_000
    receipt = _seal_receipt(
        {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "stage": "performance",
            "release_manifest_sha256": release_sha,
            "remote_identity_sha256": remote_identity_sha,
            "mutation_plan_identity_sha256": binding[
                "mutation_plan_identity_sha256"
            ],
            "deployment_instance_id": None,
            "deployment_binding": binding,
            "deployment_binding_sha256": _canonical_sha256(binding),
            "parent_runtime_receipt_identity_sha256": "f" * 64,
            "runtime_receipt_normalization": {
                "runtime_receipt_identity_sha256": "f" * 64,
                "deployment_binding_sha256": _canonical_sha256(binding),
            },
            "evidence": {
                "candidate": {
                    "session_root": metadata["session_root"],
                    "runtime_identity_sha256": metadata["runtime_identity_sha256"],
                    "collection_started_ts": committed_s - 1.0,
                    "collection_ended_ts": committed_s + 1.0,
                    "exact_standalone_part": metadata,
                }
            },
        }
    )
    path = tmp_path / "performance.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")
    context = _load_performance_context(
        path,
        bound={
            "release_manifest_sha256": release_sha,
            "remote_identity_sha256": remote_identity_sha,
        },
    )
    assert context["metadata"] == metadata

    symlink = tmp_path / "performance-link.json"
    symlink.symlink_to(path)
    with pytest.raises(JournalPartAdmissionError, match="must not be a symlink"):
        _load_performance_context(
            symlink,
            bound={
                "release_manifest_sha256": release_sha,
                "remote_identity_sha256": remote_identity_sha,
            },
        )


def test_performance_context_rejects_wrong_receipt_schema(tmp_path: Path) -> None:
    release_sha = "c" * 64
    remote_identity_sha = "d" * 64
    receipt = _seal_receipt(
        {
            "schema_version": "incompatible.performance.receipt.v2",
            "stage": "performance",
            "release_manifest_sha256": release_sha,
            "remote_identity_sha256": remote_identity_sha,
            "evidence": {},
        }
    )
    path = tmp_path / "wrong-schema.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")

    with pytest.raises(JournalPartAdmissionError, match="identity is invalid"):
        _load_performance_context(
            path,
            bound={
                "release_manifest_sha256": release_sha,
                "remote_identity_sha256": remote_identity_sha,
            },
        )


@pytest.mark.parametrize(
    "session",
    [
        EXAMPLE_REMOTE_JOURNAL_ROOT,
        f"{EXAMPLE_REMOTE_JOURNAL_ROOT}/session-other",
        "/tmp/session-prospective-escape",
    ],
)
def test_remote_session_must_be_exact_prospective_child(session: str) -> None:
    with pytest.raises(JournalPartAdmissionError):
        _validate_remote_session_root(
            EXAMPLE_REMOTE_ROOT,
            session,
        )
