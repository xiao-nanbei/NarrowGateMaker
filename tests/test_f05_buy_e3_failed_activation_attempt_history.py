from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_buy_e3_failed_activation_attempt_history as subject


def _write(path: Path, payload: dict[str, Any], *, mode: int = 0o600) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(mode)
    return path


def _seal(payload: dict[str, Any], field: str) -> dict[str, Any]:
    payload[field] = subject._document_sha256(payload, field)  # noqa: SLF001
    return payload


def _binding(path: Path, field: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="ascii"))
    return {
        "schema_version": payload["schema_version"],
        "status": payload["status"],
        "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "canonical_field": field,
        "canonical_sha256": payload[field],
        "size_bytes": path.stat().st_size,
        "mode": "0600",
    }


def _failed_source(path: Path) -> Path:
    return _write(
        path,
        _seal(
            {
                "schema_version": "fixture.rejected_source.v1",
                "status": "rejected_not_admitted",
                "epoch": {
                    "baseline_epoch_id": subject.FAILED_SESSION_TOKEN,
                    "execution_commit": "1" * 40,
                    "execution_tree": "2" * 40,
                    "config_sha256": "3" * 64,
                    "pid": 57_696,
                    "pid_start_ticks": 3_071_624,
                },
                "rejection": {
                    "error_count": 1,
                    "drop_count": 0,
                    "exchange_error_code": -5022,
                    "formal_collection_valid": False,
                    "formal_admission_allowed": False,
                },
                "authority_boundary": {
                    "successor_runtime_authority": False,
                    "final_active_capture": False,
                    "lifecycle_admission": False,
                    "economic_values_included": False,
                },
            },
            "canonical_rejected_epoch_receipt_sha256",
        ),
    )


def _benchmark(path: Path, *, schema: str, status: str, marker: str) -> Path:
    return _write(
        path,
        _seal(
            {
                "schema_version": schema,
                "status": status,
                "marker": marker,
                "checks": {
                    "aggregate_only_no_action_rows": True,
                    "callback_p99_at_most_2ms": True,
                    "decision_p99_at_most_10ms": True,
                    "exact_four_deployed_files_bound": True,
                    "exactly_1000_decisions": True,
                    "true_2x_observed_callback_rate": True,
                },
                "evidence_boundary": {
                    "aggregate_only": True,
                    "connected_to_live_market_stream": False,
                    "benchmark_action_rows_persisted": False,
                    "economic_values_persisted": False,
                    "new_economic_arm_run": False,
                    "validation_read": False,
                    "sealed_holdout_read": False,
                    "shadow_created": False,
                    "companion_created": False,
                    "hypothetical_live_actions_scored": False,
                    "action_authorized_by_resource_receipt": False,
                    "live_authorized_by_resource_receipt": False,
                },
            },
            "canonical_benchmark_receipt_sha256",
        ),
    )


@pytest.fixture
def sources(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, Path]:
    failed = _failed_source(tmp_path / "failed.json")
    v6 = _benchmark(
        tmp_path / "v6.json",
        schema="fixture.benchmark.v4",
        status="fixture_v4_passed",
        marker="wrong-route",
    )
    v7 = _benchmark(
        tmp_path / "v7-attempt2.json",
        schema="fixture.benchmark.v5",
        status="fixture_v5_passed",
        marker="ooo-failed",
    )
    monkeypatch.setattr(
        subject,
        "FAILED_ACTIVATION_SOURCE",
        _binding(failed, "canonical_rejected_epoch_receipt_sha256"),
    )
    monkeypatch.setattr(
        subject,
        "V6_WRONG_ROUTE_BENCHMARK",
        _binding(v6, "canonical_benchmark_receipt_sha256"),
    )
    monkeypatch.setattr(
        subject,
        "V7_ATTEMPT2_BENCHMARK",
        _binding(v7, "canonical_benchmark_receipt_sha256"),
    )
    return failed, v6, v7


def _inputs(sources: tuple[Path, Path, Path]) -> dict[str, Path]:
    failed, v6, v7 = sources
    return {
        "failed_activation_source_path": failed,
        "v6_wrong_route_benchmark_path": v6,
        "v7_attempt2_benchmark_path": v7,
    }


def test_round_trip_reclassifies_failed_token_and_preserves_attempt_boundaries(
    tmp_path: Path, sources: tuple[Path, Path, Path]
) -> None:
    output = tmp_path / "history.json"
    payload, file_sha = subject.finalize_failed_activation_attempt_history(
        output_path=output,
        generated_utc="2026-08-24T08:00:00Z",
        **_inputs(sources),
    )
    assert hashlib.sha256(output.read_bytes()).hexdigest() == file_sha
    assert subject.validate_failed_activation_attempt_history(output, **_inputs(sources)) == payload
    projection = payload["failed_activation_projection"]
    assert projection["source_reported_unadmitted_session_token"] == subject.FAILED_SESSION_TOKEN
    assert projection["epoch_established"] is False
    assert projection["runtime_authority"] is False
    assert projection["evidence_authority"] is False
    assert projection["reusable_for_current"] is False
    attempts = payload["resource_gate_attempts"]
    assert attempts["resource_v5"]["formal_benchmark_output_created"] is False
    assert attempts["resource_v6"]["formal_resource_receipt_created"] is False
    assert attempts["resource_v7_attempt1"]["benchmark"]["exact7_binding_claimed"] is False
    assert attempts["resource_v7_attempt2"]["window_delta"] == 2
    assert all(attempt["active_process_started"] is False for attempt in attempts.values())
    assert payload["permissions"] == {"research": False, "action": False, "live": False}
    assert not any(payload["evidence_boundary"].values())


def test_validator_rejects_recanonicalized_authority_or_attempt_tamper(
    tmp_path: Path, sources: tuple[Path, Path, Path]
) -> None:
    output = tmp_path / "history.json"
    subject.finalize_failed_activation_attempt_history(
        output_path=output,
        generated_utc="2026-08-24T08:00:00Z",
        **_inputs(sources),
    )
    payload = json.loads(output.read_text(encoding="ascii"))
    payload["failed_activation_projection"]["epoch_established"] = True
    payload[subject.CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload, subject.CANONICAL_FIELD
    )
    _write(output, payload)
    with pytest.raises(subject.FailedActivationHistoryError, match="drifted"):
        subject.validate_failed_activation_attempt_history(output, **_inputs(sources))


def test_source_semantic_tamper_and_extra_exact7_field_fail_closed(
    sources: tuple[Path, Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    failed, v6, _v7 = sources
    payload = json.loads(failed.read_text(encoding="ascii"))
    payload["rejection"]["formal_admission_allowed"] = True
    payload["canonical_rejected_epoch_receipt_sha256"] = subject._document_sha256(  # noqa: SLF001
        payload, "canonical_rejected_epoch_receipt_sha256"
    )
    _write(failed, payload)
    monkeypatch.setattr(
        subject,
        "FAILED_ACTIVATION_SOURCE",
        _binding(failed, "canonical_rejected_epoch_receipt_sha256"),
    )
    with pytest.raises(subject.FailedActivationHistoryError, match="semantics drifted"):
        subject._validate_failed_activation_source(failed)  # noqa: SLF001

    expected = {**subject.V6_WRONG_ROUTE_BENCHMARK, "path": str(v6)}
    with pytest.raises(subject.FailedActivationHistoryError, match="fields drifted"):
        subject._validate_content_source(  # noqa: SLF001
            v6, expected=expected, label="v6"
        )


def test_mode_hardlink_duplicate_key_and_create_only_rejected(
    tmp_path: Path, sources: tuple[Path, Path, Path]
) -> None:
    failed, _v6, _v7 = sources
    failed.chmod(0o644)
    with pytest.raises(subject.FailedActivationHistoryError, match="0600"):
        subject.build_failed_activation_attempt_history(**_inputs(sources))
    failed.chmod(0o600)
    hardlink = tmp_path / "hardlink.json"
    os.link(failed, hardlink)
    with pytest.raises(subject.FailedActivationHistoryError, match="single-link"):
        subject.build_failed_activation_attempt_history(**_inputs(sources))
    hardlink.unlink()

    output = tmp_path / "history.json"
    output.write_text("{}\n", encoding="ascii")
    output.chmod(0o600)
    with pytest.raises(subject.FailedActivationHistoryError, match="create-only"):
        subject.finalize_failed_activation_attempt_history(
            output_path=output,
            **_inputs(sources),
        )


def test_formal_module_route_help_from_repository_cwd() -> None:
    repository = Path(__file__).resolve().parents[1]
    assert subject.FORMAL_MODULE_ROUTE == "scripts.f05_buy_e3_failed_activation_attempt_history"
    completed = subprocess.run(
        [sys.executable, "-m", subject.FORMAL_MODULE_ROUTE, "--help"],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
