from __future__ import annotations

import json
import os
import subprocess
import threading
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from scripts import f05_buy_e3_post_lifecycle_live_health_v1 as subject


@pytest.fixture(scope="module")
def exact_runtime_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    target = tmp_path_factory.mktemp("post-health-exact-eacb-runtime") / "repository"
    subprocess.run(
        ("git", "clone", "--shared", "--no-checkout", str(Path.cwd()), str(target)),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ("git", "checkout", "--detach", subject.RUNTIME_EXECUTION["execution_commit"]),
        cwd=target,
        check=True,
        capture_output=True,
    )
    return target


def _content(
    schema: str,
    status: str | None,
    file_marker: str,
    canonical_marker: str,
    canonical_field: str,
    *,
    mode: str = "0600",
) -> dict[str, Any]:
    return {
        "schema_version": schema,
        "status": status,
        "file_sha256": file_marker * 64,
        "canonical_field": canonical_field,
        "canonical_sha256": canonical_marker * 64,
        "size_bytes": 123,
        "mode": mode,
    }


def _health_line(
    timestamp: str,
    *,
    updates: int,
    evaluations: int,
    supported: int = 0,
    nonbaseline: int = 0,
    fallback: int | None = None,
    windows: int | None = None,
    warm: int = 1,
    position: str = "+0.0000",
    orders: int = 0,
    decision_p99_us: float = 100.0,
    **overrides: Any,
) -> str:
    if fallback is None:
        fallback = evaluations - supported
    shadow = {
        name: 0
        for name in (
            "externalSources",
            *subject.resource_v8.GLOBAL_FLOW_STATE_ZERO_FIELDS,
            *subject.resource_v8.GLOBAL_FLOW_VALUE_ZERO_FIELDS,
            *subject.resource_v8.GLOBAL_REFERENCE_ZERO_FIELDS,
            *subject.resource_v8.GLOBAL_REFERENCE_VALUE_ZERO_FIELDS,
            *subject.resource_v8.GLOBAL_FLOW_ABSOLUTE_ZERO_FIELDS,
        )
    }
    shadow.update(
        {
            "globalFlowReason": subject.resource_v8.SHADOW_DISABLED_REASON,
            "globalRefReason": subject.resource_v8.SHADOW_DISABLED_REASON,
        }
    )
    counters = {name: 0 for name in subject.resource_v8.WINDOW_ZERO_COUNTERS[:-2]}
    values: dict[str, Any] = {
        "pos": position,
        "orders": orders,
        "booleanCooldownEnabled": 1,
        "booleanCooldownUpdates": updates,
        "buyE3CooldownEnabled": 1,
        "buyE3CooldownEval": evaluations,
        "buyE3CooldownSupported": supported,
        "buyE3CooldownNonbaseline": nonbaseline,
        "buyE3CooldownFallback": fallback,
        "buyE3CooldownDecisionP99Us": decision_p99_us,
        "buyE3CooldownWarm": warm,
        "buyE3CooldownWindows": updates if windows is None else windows,
        "buyE3CooldownGapResets": 0,
        "deepBookBuffer": 0,
        **shadow,
        **counters,
    }
    values.update(overrides)
    fields = " ".join(f"{name}={value}" for name, value in values.items())
    return f"{timestamp} [main] INFO HEALTH {fields}\n"


def _lifecycle_line(
    timestamp: str,
    *,
    drops: int = 0,
    errors: int = 0,
) -> str:
    return (
        f"{timestamp} [main] INFO ORDER_LIFECYCLE_JOURNAL_V2_HEALTH "
        f"profile=fixture remoteSpoolValid=1 formalValid=1 queue=0 hwm=1 "
        f"drops={drops} errors={errors} enqueueP99Us=50.0 writeP99Ms=0.200 "
        "maxRssMb=128.0 lastFlushNs=1\n"
    )


def _sample() -> dict[str, Any]:
    return {
        "mem_available_mib": 1024.0,
        "live_rss_mib": 128.0,
        "oom_kill": 3,
        "swap_in": 4,
        "swap_out": 5,
    }


def _capture_bundle(
    tmp_path: Path,
) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], int]:
    log = tmp_path / "live.log"
    log.write_text("constructor preamble\n", encoding="utf-8")
    identity_checks = 0

    def identity() -> tuple[int, int]:
        nonlocal identity_checks
        identity_checks += 1
        return 202, 2_000

    base = datetime.now(tz=UTC) - timedelta(seconds=30)
    first = base.strftime("%Y-%m-%d %H:%M:%S")
    second = (base + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
    lifecycle = (base + timedelta(milliseconds=500)).strftime("%Y-%m-%d %H:%M:%S")
    with subject.FreshHealthTail(log) as tail:
        with log.open("a", encoding="utf-8") as handle:
            handle.write("ignored line\n")
            handle.write(_health_line(first, updates=10, evaluations=20))
            handle.write(_lifecycle_line(lifecycle))
            handle.write(
                _health_line(
                    second,
                    updates=11,
                    evaluations=22,
                    position="+0.0010",
                    orders=2,
                    decision_p99_us=120.0,
                )
            )
        log_capture, main, lifecycle_row, aggregates = subject._capture_fresh_health(  # noqa: SLF001
            tail=tail,
            expected_pid=202,
            expected_start_ticks=2_000,
            stable_process_identity_sha256="f" * 64,
            identity_supplier=identity,
            sample_supplier=_sample,
            timeout_s=1.0,
            poll_interval_s=0.01,
            sleep=lambda _seconds: None,
        )
    return log, log_capture, main, lifecycle_row, aggregates, identity_checks


def _receipt(tmp_path: Path) -> tuple[dict[str, Any], Path]:
    log, log_capture, main, lifecycle_health, aggregates, _checks = _capture_bundle(tmp_path)
    active_process = {
        "pid": 202,
        "pid_start_ticks": 2_000,
        "process_identity_sha256": "1" * 64,
        "stable_process_identity_sha256": "f" * 64,
        "config_sha256": subject.active_capture_v8.ACTIVE_CONFIG_SHA256,
        "runtime_identity_file_sha256": "2" * 64,
        "runtime_identity_canonical_sha256": "3" * 64,
        "runtime_source_manifest_sha256": "4" * 64,
        "runtime_source_files": dict(subject.EXPECTED_STARTUP_SOURCE_SHA256),
        "release_file_sha256": subject.active_capture_v8.DIRECT_SUCCESSOR_RELEASE_FILE_SHA256,
        "release_canonical_sha256": (
            subject.active_capture_v8.DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
        ),
    }
    release_content = _content(
        subject.active_capture_v8.DIRECT_SUCCESSOR_RELEASE_SCHEMA,
        subject.active_capture_v8.DIRECT_SUCCESSOR_RELEASE_STATUS,
        subject.active_capture_v8.DIRECT_SUCCESSOR_RELEASE_FILE_SHA256[0],
        subject.active_capture_v8.DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256[0],
        "canonical_active_release_sha256",
    )
    release_content["file_sha256"] = subject.active_capture_v8.DIRECT_SUCCESSOR_RELEASE_FILE_SHA256
    release_content["canonical_sha256"] = (
        subject.active_capture_v8.DIRECT_SUCCESSOR_RELEASE_CANONICAL_SHA256
    )
    runtime_authority = {
        **release_content,
        "execution": dict(subject.RUNTIME_EXECUTION),
        "runtime_authority": True,
    }
    activation = _content(
        subject.active_capture_v8.SCHEMA_VERSION,
        subject.active_capture_v8.STATUS,
        "5",
        "6",
        subject.active_capture_v8.CANONICAL_FIELD,
    )
    lifecycle = _content(
        subject.lifecycle_io.LIFECYCLE_SCHEMA,
        None,
        "7",
        "8",
        "admission_identity_sha256",
        mode="0644",
    )
    lifecycle_context_receipt = _content(
        subject.lifecycle_context_v1.SCHEMA_VERSION,
        subject.lifecycle_context_v1.STATUS,
        "a",
        "b",
        subject.lifecycle_context_v1.CANONICAL_FIELD,
    )
    admitted = datetime.now(tz=UTC) - timedelta(minutes=2)
    context = {
        "admitted_ts_ns": int(admitted.timestamp() * 1_000_000_000),
        "session_id": "fixture-session",
        "baseline_epoch_id": "prospective-fixture-post-lifecycle",
        "config_sha256": subject.active_capture_v8.ACTIVE_CONFIG_SHA256,
        "runtime_code_sha256": subject.lifecycle_context_v1.RUNTIME_CODE_SHA256,
        "runtime_code_schema_version": subject.lifecycle_context_v1.RUNTIME_CODE_SCHEMA,
        "runtime_source_files": dict(subject.EXPECTED_LIFECYCLE_SOURCE_SHA256),
        "runtime_source_file_count": subject.lifecycle_context_v1.RUNTIME_SOURCE_FILE_COUNT,
        "runtime_source_files_canonical_sha256": (
            subject.lifecycle_context_v1.RUNTIME_SOURCE_FILES_CANONICAL_SHA256
        ),
        "action_enablement_sha256": "9" * 64,
        "epoch_start_ts_ns": int(admitted.timestamp() * 1_000_000_000) - 1,
        "writer_runtime_identity_sha256": "8" * 64,
        "writer_identity_file_sha256": "7" * 64,
        "epoch_manifest_file_sha256": "6" * 64,
        "identity_evidence_file_sha256": "5" * 64,
        "safe_action_state": dict(subject.lifecycle_context_v1.SAFE_ACTION_STATE),
        "action_shadow_enabled_state": dict(
            subject.lifecycle_context_v1.SAFE_ACTION_SHADOW_ENABLED_STATE
        ),
        "external_shadow_only_inert": True,
        "data_source_identity_sha256": "d" * 64,
        "external_source_recording_state": deepcopy(
            subject.lifecycle_context_v1.SAFE_EXTERNAL_SOURCE_RECORDING_STATE
        ),
        "external_source_count": len(
            subject.lifecycle_context_v1.SAFE_EXTERNAL_SOURCE_RECORDING_STATE
        ),
        "source_settings_inert_because_external_master_false": True,
        "record_trades_inert_because_master_false_and_record_enabled_false": True,
        "external_effective_stream_and_recording_disabled": True,
    }
    generated = (datetime.now(tz=UTC) + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    portable = subject._portable_payload(  # noqa: SLF001
        generated_utc=generated,
        runtime_authority=runtime_authority,
        active_process=active_process,
        lifecycle_admission=lifecycle,
        lifecycle_epoch_id=context["baseline_epoch_id"],
        main_health_window=main,
        lifecycle_health=lifecycle_health,
        operational_aggregates=aggregates,
    )
    payload = {
        "schema_version": subject.SCHEMA_VERSION,
        "identity": subject.OWNER,
        "status": subject.STATUS,
        "generated_utc": generated,
        "activation_capture": activation,
        "lifecycle_context_receipt": lifecycle_context_receipt,
        "lifecycle_admission": lifecycle,
        "lifecycle_context": context,
        "runtime_execution": dict(subject.RUNTIME_EXECUTION),
        "runtime_authority": runtime_authority,
        "active_process": active_process,
        "log_capture": log_capture,
        "main_health_window": main,
        "lifecycle_health": lifecycle_health,
        "operational_aggregates": aggregates,
        "lifecycle_process_cross_binding": dict(subject.LIFECYCLE_PROCESS_CROSS_BINDING),
        "portable_projection": portable,
        "checks": dict(subject.CHECKS),
        "permissions": dict(subject.NO_AUTHORITY),
        "evidence_boundary": dict(subject.EVIDENCE_BOUNDARY),
    }
    payload[subject.CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload, subject.CANONICAL_FIELD
    )
    return payload, log


def _recanonicalize(payload: dict[str, Any]) -> None:
    payload[subject.PORTABLE_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload, subject.PORTABLE_CANONICAL_FIELD
    )


def _recanonicalize_receipt(payload: dict[str, Any]) -> None:
    payload[subject.CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload, subject.CANONICAL_FIELD
    )


def test_actual_active_v7_module_and_source_partitions_are_cold() -> None:
    assert subject.active_capture_v8.SCHEMA_VERSION.endswith(".v7")
    assert subject.active_capture_v8.STATUS == (
        "fresh_active_health_proven_all_shadow_evaluators_disabled"
    )
    assert len(subject.EXPECTED_ALL_RUNTIME_SOURCE_SHA256) == 15
    assert len(subject.EXPECTED_STARTUP_SOURCE_SHA256) == 10
    assert len(subject.EXPECTED_LIFECYCLE_SOURCE_SHA256) == 65
    assert set(subject.EXPECTED_STARTUP_SOURCE_SHA256).issubset(
        subject.EXPECTED_LIFECYCLE_SOURCE_SHA256
    )


def test_constructor_boundary_scans_first_fresh_main_pair_and_lifecycle(tmp_path: Path) -> None:
    log, capture, main, lifecycle, aggregates, identity_checks = _capture_bundle(tmp_path)
    assert capture["log_path_provenance"] == str(log.absolute())
    assert capture["scan_end_offset_bytes"] > capture["boundary_offset_bytes"]
    assert main["rows"][0]["fresh_generation"] == 1
    assert main["rows"][1]["fresh_generation"] == 2
    assert main["rows"][1]["projection"]["boolean_cooldown_updates"] == 11
    assert lifecycle["fresh_generation"] == 1
    assert lifecycle["order_lifecycle_v2_drops"] == 0
    assert lifecycle["order_lifecycle_v2_errors"] == 0
    assert aggregates["latency"]["decision_sample_count"] == 2
    assert aggregates["latency"]["formal_performance_authority"] is False
    assert aggregates["latency"]["resource_v8_formal_gate_unchanged"] is True
    assert aggregates["position"] == {
        "main_health_position_projection_completed": True,
        "reported_aggregate_position_flat": False,
        "reported_open_order_count": 2,
        "economic_values_persisted": False,
    }
    assert identity_checks >= 10


def test_constructor_rejects_incomplete_eof_and_symlink(tmp_path: Path) -> None:
    incomplete = tmp_path / "incomplete.log"
    incomplete.write_bytes(b"not newline")
    with pytest.raises(subject.PostLifecycleLiveHealthError, match="EOF"):
        subject.FreshHealthTail(incomplete)
    target = tmp_path / "target.log"
    target.write_text("complete\n", encoding="utf-8")
    link = tmp_path / "link.log"
    link.symlink_to(target)
    with pytest.raises(subject.PostLifecycleLiveHealthError, match="non-symlink"):
        subject.FreshHealthTail(link)
    directory = tmp_path / "real-directory"
    directory.mkdir()
    nested = directory / "nested.log"
    nested.write_text("complete\n", encoding="utf-8")
    directory_link = tmp_path / "directory-link"
    directory_link.symlink_to(directory, target_is_directory=True)
    with pytest.raises(subject.PostLifecycleLiveHealthError, match="symlink ancestor"):
        subject.FreshHealthTail(directory_link / "nested.log")


def test_tail_rejects_path_replaced_after_constructor(tmp_path: Path) -> None:
    log = tmp_path / "live.log"
    log.write_text("complete\n", encoding="utf-8")
    replacement = tmp_path / "replacement.log"
    replacement.write_text("other\n", encoding="utf-8")
    with subject.FreshHealthTail(log) as tail:
        os.replace(replacement, log)
        with pytest.raises(subject.PostLifecycleLiveHealthError, match="device/inode"):
            tail.poll()


def test_pid_start_is_checked_around_each_event_in_one_poll_batch(tmp_path: Path) -> None:
    log = tmp_path / "live.log"
    log.write_text("complete\n", encoding="utf-8")
    now = datetime.now(tz=UTC) - timedelta(seconds=5)
    lines = (
        _health_line(now.strftime("%Y-%m-%d %H:%M:%S"), updates=1, evaluations=1),
        _lifecycle_line((now + timedelta(milliseconds=500)).strftime("%Y-%m-%d %H:%M:%S")),
        _health_line(
            (now + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S"),
            updates=2,
            evaluations=2,
        ),
    )
    with subject.FreshHealthTail(log) as tail:
        with log.open("a", encoding="utf-8") as handle:
            handle.writelines(lines)
        calls = 0

        def changed() -> tuple[int, int]:
            nonlocal calls
            calls += 1
            return (202, 2_000) if calls < 6 else (203, 3_000)

        with pytest.raises(subject.PostLifecycleLiveHealthError, match="PID/start"):
            subject._capture_fresh_health(  # noqa: SLF001
                tail=tail,
                expected_pid=202,
                expected_start_ticks=2_000,
                stable_process_identity_sha256="f" * 64,
                identity_supplier=changed,
                sample_supplier=_sample,
                timeout_s=0.1,
                poll_interval_s=0.01,
                sleep=lambda _seconds: None,
            )
        assert calls == 6


def test_log_validator_reopens_exact_interval_offsets_and_hashes(tmp_path: Path) -> None:
    payload, log = _receipt(tmp_path)
    subject._revalidate_log_bytes(  # noqa: SLF001
        live_log_path=log,
        log_capture=payload["log_capture"],
        main_health_window=payload["main_health_window"],
        lifecycle_health=payload["lifecycle_health"],
        operational_aggregates=payload["operational_aggregates"],
    )
    offset = payload["main_health_window"]["rows"][0]["line_offset_bytes"]
    with log.open("r+b") as handle:
        handle.seek(offset)
        original = handle.read(1)
        handle.seek(offset)
        handle.write(b"X" if original != b"X" else b"Y")
    with pytest.raises(subject.PostLifecycleLiveHealthError, match="interval bytes"):
        subject._revalidate_log_bytes(  # noqa: SLF001
            live_log_path=log,
            log_capture=payload["log_capture"],
            main_health_window=payload["main_health_window"],
            lifecycle_health=payload["lifecycle_health"],
            operational_aggregates=payload["operational_aggregates"],
        )


def test_content_projection_validates_without_reopening_remote_path(tmp_path: Path) -> None:
    payload, log = _receipt(tmp_path)
    log.unlink()
    observed = subject.validate_content_projection(payload)
    portable = subject.portable_projection(payload)
    assert observed == payload
    assert portable == payload["portable_projection"]
    assert portable["main_health_window"]["rows"][1]["projection"]["boolean_cooldown_updates"] == 11
    assert portable["lifecycle_process_cross_binding"] == (subject.LIFECYCLE_PROCESS_CROSS_BINDING)
    assert "log_path_provenance" not in str(portable)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("same_updates", "main-health row"),
        ("warm_not_admitted", "main-health row"),
        ("same_completed_windows", "main-health row"),
        ("shadow_nonzero", "no-shadow"),
        ("lifecycle_error", "lifecycle-health semantics"),
        ("startup_source_extra", "active process identity"),
        ("lifecycle_source_extra", "content identity"),
        ("lifecycle_runtime_code_invalid", "runtime code"),
        ("scan_end_after_selected_rows", "content identity"),
        ("position_amount", "position safety projection"),
        ("economic_persisted", "position safety semantics"),
        ("authority_true", "latency authority"),
        ("generated_before_admission", "predates"),
        ("noncanonical_timezone", "canonical UTC"),
        ("direct_lifecycle_pid_claim", "identity"),
        ("old_active_schema", "content identity"),
    ),
)
def test_content_projection_rejects_tamper_old_schema_and_economic_values(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    payload, _log = _receipt(tmp_path)
    portable = payload["portable_projection"]
    if mutation == "same_updates":
        payload["main_health_window"]["rows"][1]["projection"]["boolean_cooldown_updates"] = 10
        portable["main_health_window"]["rows"][1]["projection"]["boolean_cooldown_updates"] = 10
    elif mutation == "warm_not_admitted":
        payload["main_health_window"]["rows"][1]["readiness"]["warmup_time_admitted"] = False
        portable["main_health_window"]["rows"][1]["readiness"]["warmup_time_admitted"] = False
    elif mutation == "same_completed_windows":
        payload["main_health_window"]["rows"][1]["readiness"]["completed_windows"] = 10
        portable["main_health_window"]["rows"][1]["readiness"]["completed_windows"] = 10
    elif mutation == "shadow_nonzero":
        payload["main_health_window"]["rows"][1]["projection"]["shadow_disabled_state"][
            "globalFlowOOO"
        ] = 1
        portable["main_health_window"]["rows"][1]["projection"]["shadow_disabled_state"][
            "globalFlowOOO"
        ] = 1
    elif mutation == "lifecycle_error":
        payload["lifecycle_health"]["order_lifecycle_v2_errors"] = 1
        portable["lifecycle_health"]["order_lifecycle_v2_errors"] = 1
    elif mutation == "startup_source_extra":
        payload["active_process"]["runtime_source_files"]["unexpected.py"] = "0" * 64
        portable["active_process"]["runtime_source_files"]["unexpected.py"] = "0" * 64
    elif mutation == "lifecycle_source_extra":
        payload["lifecycle_context"]["runtime_source_files"]["unexpected.py"] = "0" * 64
    elif mutation == "lifecycle_runtime_code_invalid":
        payload["lifecycle_context"]["runtime_code_sha256"] = "not-a-sha256"
    elif mutation == "scan_end_after_selected_rows":
        payload["log_capture"]["scan_end_offset_bytes"] += 1
    elif mutation == "position_amount":
        payload["operational_aggregates"]["position"]["positionAmt"] = 0.001
        portable["operational_aggregates"]["position"]["positionAmt"] = 0.001
    elif mutation == "economic_persisted":
        payload["operational_aggregates"]["position"]["economic_values_persisted"] = True
        portable["operational_aggregates"]["position"]["economic_values_persisted"] = True
    elif mutation == "authority_true":
        payload["operational_aggregates"]["latency"]["strategy_result_authority"] = True
        portable["operational_aggregates"]["latency"]["strategy_result_authority"] = True
    elif mutation == "generated_before_admission":
        old = "2020-01-01T00:00:00Z"
        payload["generated_utc"] = old
        portable["generated_utc"] = old
    elif mutation == "noncanonical_timezone":
        offset = str(payload["generated_utc"]).removesuffix("Z") + "+00:00"
        payload["generated_utc"] = offset
        portable["generated_utc"] = offset
    elif mutation == "direct_lifecycle_pid_claim":
        payload["lifecycle_process_cross_binding"][
            "direct_lifecycle_admission_to_active_process_binding_claimed"
        ] = True
        portable["lifecycle_process_cross_binding"][
            "direct_lifecycle_admission_to_active_process_binding_claimed"
        ] = True
    else:
        payload["activation_capture"]["schema_version"] = (
            f"{subject.OWNER}.fresh_all_shadow_evaluators_disabled_active_process_capture.v6"
        )
    _recanonicalize(portable)
    _recanonicalize_receipt(payload)
    with pytest.raises(subject.PostLifecycleLiveHealthError, match=message):
        subject.validate_content_projection(payload)


def test_lifecycle_context_requires_exact_65_source_map_and_0644(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    receipt, _log = _receipt(tmp_path)
    payload = {
        "lifecycle_admission": receipt["lifecycle_admission"],
        "lifecycle_projection": receipt["lifecycle_context"],
    }
    binding = receipt["lifecycle_context_receipt"]
    monkeypatch.setattr(
        subject.lifecycle_context_v1,
        "validate_lifecycle_context",
        lambda _path, **_kwargs: payload,
    )
    monkeypatch.setattr(subject, "_private_binding", lambda *_args, **_kwargs: (payload, binding))
    observed, projected = subject._lifecycle_context(  # noqa: SLF001
        Path("/fixture/context.json"),
        runtime_repository_root=Path.cwd(),
    )
    assert observed == payload
    assert projected["runtime_source_file_count"] == 65
    payload["lifecycle_projection"]["runtime_source_files"]["unexpected.py"] = "0" * 64
    with pytest.raises(subject.PostLifecycleLiveHealthError, match="runtime sources"):
        subject._lifecycle_context(  # noqa: SLF001
            Path("/fixture/context.json"),
            runtime_repository_root=Path.cwd(),
        )


def test_proc_safety_sampler_and_zero_delta_aggregate(tmp_path: Path) -> None:
    proc = tmp_path / "proc"
    (proc / "202").mkdir(parents=True)
    (proc / "meminfo").write_text("MemAvailable: 1048576 kB\n", encoding="utf-8")
    (proc / "202" / "status").write_text("VmRSS: 131072 kB\n", encoding="utf-8")
    (proc / "vmstat").write_text("oom_kill 2\npswpin 3\npswpout 4\n", encoding="utf-8")
    sample = subject.ProcSafetySampler(proc, 202).sample()
    assert sample["mem_available_mib"] == 1024.0
    assert sample["live_rss_mib"] == 128.0
    aggregate = subject._resource_aggregates([sample, deepcopy(sample)])  # noqa: SLF001
    assert aggregate["oom_window_delta"] == 0
    bad = deepcopy(sample)
    bad["oom_kill"] += 1
    with pytest.raises(subject.PostLifecycleLiveHealthError, match="safety counters"):
        subject._resource_aggregates([sample, bad])  # noqa: SLF001


def test_receipt_and_portable_projection_persist_no_forbidden_economic_or_id_fields(
    tmp_path: Path,
) -> None:
    payload, _log = _receipt(tmp_path)
    forbidden = {
        "positionAmt",
        "entryPrice",
        "price",
        "size",
        "quantity",
        "pnl",
        "orderId",
        "clientOrderId",
        "raw_payload",
        "raw_response",
        "supported_sample_count",
        "nonbaseline_sample_count",
        "fallback_sample_count",
    }

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            assert forbidden.isdisjoint(value)
            for nested in value.values():
                walk(nested)
        elif isinstance(value, list):
            for nested in value:
                walk(nested)

    walk(payload)
    assert payload["evidence_boundary"]["economic_values_persisted"] is False
    assert (
        payload["portable_projection"]["operational_aggregates"]["latency"][
            "strategy_result_authority"
        ]
        is False
    )


def test_cli_capture_validate_roundtrip_reopens_log_and_is_create_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    exact_runtime_root: Path,
) -> None:
    seed_root = tmp_path / "seed"
    seed_root.mkdir()
    seed, _seed_log = _receipt(seed_root)
    projected_process = deepcopy(seed["active_process"])
    runtime_authority = deepcopy(seed["runtime_authority"])
    activation_process = {
        "schema_version": "fixture.process.v1",
        "pid": projected_process["pid"],
        "pid_start_ticks": projected_process["pid_start_ticks"],
        "cmdline": ["python", "-m", "live.main"],
        "cmdline_sha256": "1" * 64,
        "cwd": "/fixture/runtime",
        "config_path": "/fixture/config.yaml",
        "config_sha256": projected_process["config_sha256"],
        "python_executable": "/fixture/.venv/bin/python",
        "python_binary_resolved": "/fixture/.venv/bin/python",
        "venv_root": "/fixture/.venv",
        "runtime_identity": "/fixture/runtime_identity.json",
    }
    active = {"active_process": activation_process}
    lifecycle_context_path = tmp_path / "lifecycle_context.json"
    lifecycle_context = {
        "schema_version": subject.lifecycle_context_v1.SCHEMA_VERSION,
        "identity": subject.OWNER,
        "status": subject.lifecycle_context_v1.STATUS,
        "generated_utc": seed["generated_utc"],
        "lifecycle_admission": deepcopy(seed["lifecycle_admission"]),
        "lifecycle_projection": deepcopy(seed["lifecycle_context"]),
        "runtime_execution": dict(subject.RUNTIME_EXECUTION),
        "checks": dict(subject.lifecycle_context_v1.CHECKS),
        "permissions": dict(subject.lifecycle_context_v1.PERMISSIONS),
        "evidence_boundary": dict(subject.lifecycle_context_v1.EVIDENCE_BOUNDARY),
    }
    lifecycle_context[subject.lifecycle_context_v1.CANONICAL_FIELD] = (
        subject.lifecycle_context_v1._document_sha256(  # noqa: SLF001
            lifecycle_context,
            subject.lifecycle_context_v1.CANONICAL_FIELD,
        )
    )
    lifecycle_context_path.write_text(
        json.dumps(lifecycle_context, indent=2, sort_keys=True) + "\n",
        encoding="ascii",
    )
    lifecycle_context_path.chmod(0o600)
    assert (
        subject.lifecycle_context_v1.validate_lifecycle_context(
            lifecycle_context_path,
            runtime_repository_root=exact_runtime_root,
        )
        == lifecycle_context
    )
    monkeypatch.setattr(
        subject,
        "_activation_context",
        lambda **_kwargs: (active, deepcopy(seed["activation_capture"])),
    )
    monkeypatch.setattr(
        subject,
        "_active_projection",
        lambda _active, _lifecycle: (deepcopy(projected_process), deepcopy(runtime_authority)),
    )
    monkeypatch.setattr(subject, "_capture_process", lambda **_kwargs: activation_process)
    monkeypatch.setattr(
        subject,
        "_pid_start_key",
        lambda **_kwargs: (
            projected_process["pid"],
            projected_process["pid_start_ticks"],
        ),
    )

    class FixtureSampler:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def sample(self) -> dict[str, Any]:
            return _sample()

    monkeypatch.setattr(subject, "ProcSafetySampler", FixtureSampler)
    live_log = tmp_path / "live.log"
    live_log.write_text("constructor complete\n", encoding="utf-8")
    output = tmp_path / "post_lifecycle.json"

    def append_windows(*, base_update: int) -> tuple[threading.Thread, threading.Event]:
        stopped = threading.Event()

        def writer() -> None:
            generation = 0
            while not stopped.is_set():
                update = base_update + (generation * 2)
                now = datetime.now(tz=UTC) - timedelta(seconds=2)
                with live_log.open("a", encoding="utf-8") as handle:
                    handle.write(
                        _health_line(
                            now.strftime("%Y-%m-%d %H:%M:%S"),
                            updates=update,
                            windows=update,
                            evaluations=5 + generation,
                            supported=0,
                            nonbaseline=0,
                            fallback=5,
                            decision_p99_us=11_508.0,
                        )
                    )
                    handle.write(
                        _lifecycle_line(
                            (now + timedelta(milliseconds=500)).strftime("%Y-%m-%d %H:%M:%S")
                        )
                    )
                    handle.write(
                        _health_line(
                            (now + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S"),
                            updates=update + 1,
                            windows=update + 1,
                            evaluations=6 + generation,
                            supported=1,
                            nonbaseline=1,
                            fallback=5,
                            decision_p99_us=17_628.7,
                        )
                    )
                generation += 1
                stopped.wait(0.1)

        thread = threading.Thread(target=writer, daemon=True)
        thread.start()
        return thread, stopped

    common = [
        "--runtime-repository-root",
        str(exact_runtime_root),
        "--direct-release",
        str(tmp_path / "release.json"),
        "--resource-receipt",
        str(tmp_path / "resource.json"),
        "--config-correction",
        str(tmp_path / "correction.json"),
        "--active-capture",
        str(tmp_path / "active.json"),
        "--lifecycle-context",
        str(lifecycle_context_path),
        "--pid-file",
        str(tmp_path / "maker.pid"),
        "--config",
        str(tmp_path / "config.yaml"),
        "--python-executable",
        str(tmp_path / ".venv/bin/python"),
        "--venv-root",
        str(tmp_path / ".venv"),
        "--runtime-identity",
        str(tmp_path / "runtime_identity.json"),
        "--live-log",
        str(live_log),
        "--proc-root",
        str(tmp_path / "proc"),
    ]
    writer, stopped = append_windows(base_update=100)
    try:
        assert (
            subject.main(
                [
                    "capture",
                    *common,
                    "--health-timeout-s",
                    "5",
                    "--health-poll-interval-s",
                    "0.01",
                    "--output",
                    str(output),
                ]
            )
            == 0
        )
    finally:
        stopped.set()
    writer.join(timeout=1)
    capture_result = json.loads(capsys.readouterr().out)
    metadata = os.lstat(output)
    assert metadata.st_nlink == 1
    assert metadata.st_mode & 0o777 == 0o600
    assert capture_result["schema_version"] == subject.SCHEMA_VERSION
    persisted = json.loads(output.read_text(encoding="ascii"))
    assert persisted["operational_aggregates"]["latency"] == {
        "decision_sample_count": 1,
        "decision_p99_us": 17_628.7,
        "lifecycle_enqueue_p99_us": 50.0,
        "lifecycle_write_p99_ms": 0.2,
        "small_sample_disclosed": True,
        "strategy_result_authority": False,
        "formal_performance_authority": False,
        "resource_v8_formal_gate_unchanged": True,
        "economic_outcome_claimed": False,
    }
    assert subject.main(["validate", *common, "--receipt", str(output)]) == 0
    validate_result = json.loads(capsys.readouterr().out)
    assert validate_result["canonical_sha256"] == capture_result["canonical_sha256"]

    writer, stopped = append_windows(base_update=200)
    try:
        with pytest.raises(subject.PostLifecycleLiveHealthError, match="create-only"):
            subject.main(
                [
                    "capture",
                    *common,
                    "--health-timeout-s",
                    "5",
                    "--health-poll-interval-s",
                    "0.01",
                    "--output",
                    str(output),
                ]
            )
    finally:
        stopped.set()
    writer.join(timeout=1)
