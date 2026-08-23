from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_buy_e3_durability_gate as subject


@pytest.fixture(scope="module")
def measurement(
    tmp_path_factory: pytest.TempPathFactory,
) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("f05-buy-e3-durability-measurement")
    return subject.build_measurement_record(work_root=root / "work")


@pytest.fixture(scope="module")
def admitted_harness(
    measurement: dict[str, Any],
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, dict[str, Any]]:
    root = tmp_path_factory.mktemp("f05-buy-e3-durability-harness")
    counts = {
        "collected": len(subject.HARNESS_NODEIDS),
        "executed": len(subject.HARNESS_NODEIDS),
        "passed": len(subject.HARNESS_NODEIDS),
        "failed": 0,
        "errors": 0,
        "skipped": 0,
        "return_code": 0,
    }
    payload = subject.build_harness_receipt(
        measurement=measurement,
        pytest_counts=counts,
    )
    output = root / "durability-harness-receipt.json"
    subject._write_private_receipt_no_replace(output, payload)  # noqa: SLF001
    validated = subject.validate_receipt(output)
    return output, validated


def _write_private_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n",
        encoding="ascii",
    )
    os.chmod(path, 0o600)


def _recanonicalize(payload: dict[str, Any]) -> None:
    payload[subject.RECEIPT_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload,
        subject.RECEIPT_CANONICAL_FIELD,
    )


def test_worker_concurrency_reaches_exact_ten(measurement: dict[str, Any]) -> None:
    assert measurement["configured_worker_count"] == 10
    assert measurement["peak_concurrent_worker_count"] == 10
    assert measurement["submitted_task_count"] == 20
    assert measurement["terminal_task_count"] == 20
    assert measurement["checks"]["exact_worker_count"] is True
    assert measurement["checks"]["intended_concurrency_reached"] is True


def test_mmap_lifetime_waits_for_all_tasks_before_close(
    measurement: dict[str, Any],
) -> None:
    assert measurement["mmap_open_count"] == measurement["mmap_close_count"] == 2
    for case in measurement["probe_measurements"]["cases"].values():
        assert case["terminal_task_count"] == case["submitted_task_count"] == 10
        assert case["terminal_before_pool_shutdown_count"] == 10
        assert case["mmap_mode"] == "read_only"
        assert case["mmap_close_before_terminal_count"] == 0
        assert case["mmap_use_after_close_count"] == 0
        assert case["lifecycle_events"][-3:] == [
            {"sequence": 3, "event": "all_futures_terminal_before_pool_shutdown"},
            {"sequence": 4, "event": "mmap_closed"},
            {"sequence": 5, "event": "pool_shutdown_complete"},
        ]


def test_injected_exception_cancels_and_joins_before_close(
    measurement: dict[str, Any],
) -> None:
    exception = measurement["probe_measurements"]["cases"]["injected_exception"]
    assert exception["expected_exception_observed"] is True
    assert exception["cancel_request_count"] == 10
    assert exception["terminal_task_count"] == 10
    assert exception["terminal_before_pool_shutdown_count"] == 10
    assert exception["pool_shutdown_call_count"] == 1
    assert exception["pool_shutdown_complete"] is True
    assert exception["mmap_close_before_terminal_count"] == 0


def test_atomic_cache_publish_hides_staging(measurement: dict[str, Any]) -> None:
    cache = measurement["cache_measurements"]
    assert cache["staging_observed_before_publish"] is True
    assert cache["public_partial_load_attempt_count"] == 1
    assert cache["public_partial_load_none_count"] == cache["public_partial_load_attempt_count"]
    assert cache["public_partial_load_visible_count"] == 0
    assert cache["public_partial_load_exception_count"] == 0
    assert cache["final_complete_observed"] is True
    assert cache["partial_cache_visibility_count"] == 0
    assert cache["atomic_publish_failure_count"] == 0


def test_cache_namespace_is_exact(measurement: dict[str, Any]) -> None:
    cache = measurement["cache_measurements"]
    assert cache["cache_root_namespace_count"] == 2
    assert cache["probe_cache_namespace_sha256"] == measurement["probe_cache_namespace_sha256"]
    assert cache["cache_key_probe_namespace_sha256"] == measurement["probe_cache_namespace_sha256"]
    assert measurement["probe_run_manifest"]["final_execution_manifest_bound"] is False
    assert measurement["probe_cache_namespace"]["final_execution_manifest_bound"] is False


def test_cache_interruption_resume_is_complete(measurement: dict[str, Any]) -> None:
    cache = measurement["cache_measurements"]
    assert measurement["interruption_resume_count"] == 1
    assert measurement["cache_entry_count"] == 2
    assert measurement["cache_hit_count"] >= 1
    assert cache["interrupted_entry_visible_count"] == 0
    assert cache["stale_partial_after_interruption_count"] == 0
    assert cache["remaining_partial_entry_count"] == 0


def test_repeated_run_hashes_are_deterministic(measurement: dict[str, Any]) -> None:
    repeated = measurement["cache_measurements"]["repeated_run_result_sha256s"]
    assert measurement["repeated_run_count"] == 2
    assert len(repeated) == 2
    assert len(set(repeated)) == 1
    assert measurement["checks"]["repeated_run_deterministic"] is True


def test_harness_receipt_matches_stability_wrapper_schema(
    admitted_harness: tuple[Path, dict[str, Any]],
) -> None:
    path, payload = admitted_harness
    assert set(payload) == subject.RECEIPT_FIELDS
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert path.stat().st_nlink == 1
    assert payload["schema_version"] == subject.stability.DURABILITY_HARNESS_SCHEMA
    assert payload["nodeids"] == list(subject.HARNESS_NODEIDS)
    assert payload["gate_nodeids"] == subject.GATE_NODEIDS
    assert payload["counts"]["passed"] == len(subject.HARNESS_NODEIDS)
    assert payload["measurement_sha256"] == subject.canonical_sha256(payload["measurement"])


def test_measurement_check_tamper_is_rejected_before_harness(
    measurement: dict[str, Any],
) -> None:
    tampered = json.loads(json.dumps(measurement))
    tampered["checks"]["mmap_open_close_balanced"] = False
    counts = {
        "collected": 7,
        "executed": 7,
        "passed": 7,
        "failed": 0,
        "errors": 0,
        "skipped": 0,
        "return_code": 0,
    }

    with pytest.raises(subject.DurabilityGateError, match="measurement contract drifted"):
        subject.build_harness_receipt(
            measurement=tampered,
            pytest_counts=counts,
        )


def test_run_gate_executes_focused_pytest_and_writes_harness(tmp_path: Path) -> None:
    output = tmp_path / "actual-harness.json"
    payload = subject.run_gate(
        output=output,
        work_root=tmp_path / "actual-work",
    )

    assert output.is_file()
    assert payload["counts"] == {
        "collected": 7,
        "executed": 7,
        "passed": 7,
        "failed": 0,
        "errors": 0,
        "skipped": 0,
        "return_code": 0,
    }
    assert payload["run_command"] == [
        str(Path(subject.sys.executable).resolve()),
        "-m",
        "pytest",
        "-q",
        *subject.HARNESS_NODEIDS,
    ]


def test_private_receipt_is_exclusive_and_never_replaced(
    admitted_harness: tuple[Path, dict[str, Any]],
) -> None:
    path, payload = admitted_harness
    original = path.read_bytes()

    with pytest.raises(subject.DurabilityGateError, match="already exists"):
        subject._write_private_receipt_no_replace(path, payload)  # noqa: SLF001

    assert path.read_bytes() == original


def test_recanonicalized_observation_tamper_is_rejected(
    admitted_harness: tuple[Path, dict[str, Any]],
    tmp_path: Path,
) -> None:
    _path, original = admitted_harness
    tampered = json.loads(json.dumps(original))
    tampered["observations"]["terminal_task_count"] = 19
    _recanonicalize(tampered)
    path = tmp_path / "tampered-observation.json"
    _write_private_json(path, tampered)

    with pytest.raises(subject.DurabilityGateError, match="observations fail closed"):
        subject.validate_receipt(path)


def test_recanonicalized_source_binding_tamper_is_rejected(
    admitted_harness: tuple[Path, dict[str, Any]],
    tmp_path: Path,
) -> None:
    _path, original = admitted_harness
    tampered = json.loads(json.dumps(original))
    first = subject.TESTED_SOURCE_RELATIVE_PATHS[0]
    tampered["runtime_sources"][first] = "f" * 64
    _recanonicalize(tampered)
    path = tmp_path / "tampered-source.json"
    _write_private_json(path, tampered)

    with pytest.raises(subject.DurabilityGateError, match="harness receipt drifted"):
        subject.validate_receipt(path)


def test_receipt_symlink_is_rejected(
    admitted_harness: tuple[Path, dict[str, Any]],
    tmp_path: Path,
) -> None:
    source, _payload = admitted_harness
    link = tmp_path / "redirected.json"
    link.symlink_to(source)

    with pytest.raises(subject.DurabilityGateError, match="symbolic link"):
        subject.validate_receipt(link)


def test_expected_cache_namespace_mismatch_fails_before_running(tmp_path: Path) -> None:
    with pytest.raises(subject.DurabilityGateError, match="does not match"):
        subject.build_measurement_record(
            work_root=tmp_path / "work",
            expected_probe_cache_namespace_sha256="f" * 64,
        )

    assert not (tmp_path / "work").exists()


def test_exception_probe_rejects_helper_that_raises_without_waiting(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def broken_helper(*_args: Any, **_kwargs: Any) -> None:
        raise subject._InjectedWorkerFailure("missing sibling wait")  # noqa: SLF001

    monkeypatch.setattr(
        subject.replay_adapter,
        "_consume_arm_futures_before_mmap_close",
        broken_helper,
    )

    with pytest.raises(subject.DurabilityGateError, match="before every future was terminal"):
        subject._exercise_mmap_case("injected_exception", tmp_path / "broken-helper")  # noqa: SLF001


def test_public_partial_loader_visibility_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    original = subject.replay_adapter.DayReplayCache.load_sequential

    def leaky_loader(self: Any, key: Any) -> Any:
        if list(self.entries.glob(".*.partial")) and not self._entry(key).exists():
            return subject._synthetic_cache_frame()  # noqa: SLF001
        return original(self, key)

    monkeypatch.setattr(subject.replay_adapter.DayReplayCache, "load_sequential", leaky_loader)

    with pytest.raises(subject.DurabilityGateError, match="durability gate failed closed"):
        subject.build_measurement_record(work_root=tmp_path / "leaky-cache")


def test_recomputed_raw_measurement_tamper_is_rejected(
    admitted_harness: tuple[Path, dict[str, Any]],
    tmp_path: Path,
) -> None:
    _path, original = admitted_harness
    tampered = json.loads(json.dumps(original))
    case = tampered["measurement"]["probe_measurements"]["cases"]["injected_exception"]
    case["terminal_before_pool_shutdown_count"] = 9
    tampered["measurement"]["event_series_sha256"] = subject.canonical_sha256(
        subject.stability.durability_event_series(tampered["measurement"])
    )
    tampered["measurement_sha256"] = subject.canonical_sha256(tampered["measurement"])
    tampered["event_series_sha256"] = tampered["measurement"]["event_series_sha256"]
    _recanonicalize(tampered)
    path = tmp_path / "tampered-raw-measurement.json"
    _write_private_json(path, tampered)

    with pytest.raises(subject.DurabilityGateError, match="receipt drifted"):
        subject.validate_receipt(path)


def test_source_hash_detects_same_fd_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "mutable-source.py"
    source.write_bytes(b"x" * (2 << 20))
    original_read = subject.stability.os.read
    mutated = False

    def mutating_read(descriptor: int, size: int) -> bytes:
        nonlocal mutated
        chunk = original_read(descriptor, size)
        if chunk and not mutated:
            mutated = True
            with source.open("ab") as handle:
                handle.write(b"changed")
                handle.flush()
                os.fsync(handle.fileno())
        return chunk

    monkeypatch.setattr(subject.stability.os, "read", mutating_read)
    with pytest.raises(subject.DurabilityGateError, match="changed while hashing"):
        subject._file_sha256(source)  # noqa: SLF001


def test_exact_harness_nodeids_reject_fictitious_test_file(
    admitted_harness: tuple[Path, dict[str, Any]],
    tmp_path: Path,
) -> None:
    _path, original = admitted_harness
    tampered = json.loads(json.dumps(original))
    tampered["nodeids"][0] = "tests/test_durability.py::test_worker_concurrency"
    tampered["nodeid_manifest_sha256"] = subject.canonical_sha256(tampered["nodeids"])
    tampered["run_command"][-len(tampered["nodeids"]) :] = tampered["nodeids"]
    _recanonicalize(tampered)
    path = tmp_path / "fictitious-nodeid.json"
    _write_private_json(path, tampered)

    with pytest.raises(subject.DurabilityGateError, match="receipt drifted"):
        subject.validate_receipt(path)


def test_native_signal_from_probe_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def native_fault(*_args: Any, **_kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess([], -11, stdout=b"", stderr=b"")

    monkeypatch.setattr(subject.subprocess, "run", native_fault)

    with pytest.raises(subject.DurabilityGateError, match="native signal 11"):
        subject._run_probe_subprocess(  # noqa: SLF001
            tmp_path / "probe",
            subject.REPOSITORY_ROOT,
        )
