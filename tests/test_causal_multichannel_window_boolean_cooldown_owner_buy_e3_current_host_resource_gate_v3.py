from __future__ import annotations

import json
import stat
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_current_host_resource_gate_v3 as subject,
)


def _runtime_execution(root: str = "/runtime/repo") -> dict[str, Any]:
    return {
        "repository_root": root,
        "execution_commit": subject.DIRECT_V3_EXECUTION_COMMIT,
        "execution_tree": subject.DIRECT_V3_EXECUTION_TREE,
        "annotated_tag": subject.DIRECT_V3_ANNOTATED_TAG,
        "annotated_tag_object": subject.DIRECT_V3_TAG_OBJECT,
        "tag_peeled_commit": subject.DIRECT_V3_EXECUTION_COMMIT,
        "direct_v3_commit_is_ancestor": True,
        "runtime_authority_checkout": True,
    }


def _collector_execution() -> dict[str, Any]:
    return {
        "repository_root": "/collector/repo",
        "execution_commit": "a" * 40,
        "execution_tree": "b" * 40,
        "annotated_tag": "f05-buy-e3-resource-gate-v3-test",
        "annotated_tag_object": "c" * 40,
        "tag_peeled_commit": "a" * 40,
        "direct_v3_commit_is_ancestor": True,
        "runtime_authority_checkout": False,
    }


def _runtime_sources() -> dict[str, Any]:
    files = {
        role: {
            "role": role,
            "repository_relative_path": frozen["path"],
            "sha256": frozen["sha256"],
            "runtime_working_matches_direct_v3": True,
            "collector_working_matches_direct_v3": True,
            "collector_head_matches_direct_v3": True,
        }
        for role, frozen in subject.CURRENT_V3_RUNTIME_SOURCE_SHA256.items()
    }
    return {
        "direct_v3_execution_commit": subject.DIRECT_V3_EXECUTION_COMMIT,
        "sparse_window_repair_bound": True,
        "pre_sparse_attempt4_runtime_rejected": True,
        "files": files,
        "runtime_source_manifest_sha256": subject.canonical_sha256(files),
    }


def _deployed_files() -> dict[str, Any]:
    names = {
        "manifest": "artifact/artifact_manifest.json",
        "policy": "artifact/policy.json",
        "predicate_bundle": "artifact/predicate_bundle.json",
        "direct_active_release": "active_release.direct_owner.v1.json",
    }
    files = {
        role: {
            "role": role,
            "absolute_path": f"/runtime/repo/live/private/e3/{names[role]}",
            "file_sha256": sha,
            "size_bytes": 100 + index,
            "mode": "0600",
        }
        for index, (role, sha) in enumerate(subject.EXACT_DEPLOYED_FILE_SHA256.items())
    }
    return {
        "artifact_sha256": subject.EXACT_ARTIFACT_SHA256,
        "files": files,
        "direct_release_canonical_sha256": subject.DIRECT_V3_RELEASE_CANONICAL_SHA256,
        "binding_manifest_sha256": subject.canonical_sha256(files),
    }


def _live_identity(pid: int = 202, start_ticks: int = 2_000) -> dict[str, Any]:
    return {
        "pid": pid,
        "pid_start_ticks": start_ticks,
        "cmdline_sha256": "1" * 64,
        "cwd": "/runtime/repo",
        "python_executable": "/runtime/repo/.venv-active/bin/python",
        "config_path": "/runtime/repo/live/private/e3/config.disabled.yaml",
        "config_sha256": subject.EXPECTED_DISABLED_CONFIG_SHA256,
    }


def _prior_process() -> dict[str, Any]:
    return {
        "pid": 101,
        "pid_start_ticks": 1_000,
        "stable_process_identity_sha256": "2" * 64,
    }


def _disabled_process() -> dict[str, Any]:
    live = _live_identity()
    return {
        **live,
        "buy_e3_enabled": False,
        "stable_process_identity_sha256": subject.canonical_sha256(live),
    }


def _full_process_receipt(
    *,
    pid: int,
    start_ticks: int,
    enabled: bool,
    config_path: str,
) -> dict[str, Any]:
    stable = {
        "pid": pid,
        "pid_start_ticks": start_ticks,
        "cmdline_sha256": "8" * 64,
        "cwd": "/runtime/repo",
        "python_executable": "/runtime/repo/.venv-active/bin/python",
        "config_path": config_path,
        "config_sha256": "9" * 64 if enabled else subject.EXPECTED_DISABLED_CONFIG_SHA256,
    }
    payload: dict[str, Any] = {
        "schema_version": subject.PROCESS_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "status": subject.PROCESS_STATUS,
        "captured_utc": "2026-08-24T00:00:00Z",
        **stable,
        "buy_e3_enabled": enabled,
        "runtime_execution": _runtime_execution(),
        "evidence_boundary": deepcopy(subject.EVIDENCE_BOUNDARY),
        "stable_process_identity_sha256": subject.canonical_sha256(stable),
    }
    payload[subject.PROCESS_CANONICAL_FIELD] = subject.document_sha256(
        payload, subject.PROCESS_CANONICAL_FIELD
    )
    return payload


def _counters(*, book_overflow: int = 1_662) -> dict[str, int]:
    return {
        name: (book_overflow if name == "globalFlowBookOverflow" else 0)
        for name in subject.WINDOW_ZERO_COUNTERS
    }


def _health(
    generation: int,
    *,
    counters: dict[str, int] | None = None,
    deep_buffer: int = 0,
) -> dict[str, Any]:
    return {
        "main_generation": generation,
        "main_line_sha256": f"{generation:x}" * 64,
        "lifecycle_generation": generation,
        "lifecycle_line_sha256": f"{generation + 2:x}" * 64,
        "boolean_cooldown_enabled": 1,
        "boolean_cooldown_updates": 10_000 + generation * 1_000,
        "buy_e3_enabled": 0,
        "deep_book_buffer": deep_buffer,
        "counter_values": counters or _counters(),
    }


def _samples(**last_override: Any) -> list[dict[str, Any]]:
    rows = [
        {
            "monotonic_ns": 10,
            "mem_available_mib": 3_000.0,
            "live_rss_mib": 318.0,
            "benchmark_rss_mib": 120.0,
            "oom_kill": 4,
            "swap_in": 11,
            "swap_out": 12,
            "deep_book_buffer": 0,
        },
        {
            "monotonic_ns": 20,
            "mem_available_mib": 2_900.0,
            "live_rss_mib": 320.0,
            "benchmark_rss_mib": 125.0,
            "oom_kill": 4,
            "swap_in": 11,
            "swap_out": 12,
            "deep_book_buffer": 0,
        },
    ]
    rows[-1].update(last_override)
    return rows


def _benchmark(**callback_override: Any) -> dict[str, Any]:
    callback = {
        "observed_live_rate_hz": 100.0,
        "achieved_rate_hz": 205.0,
        "achieved_to_observed_rate": 2.05,
        "latency_p99_us": 1_900.0,
    }
    callback.update(callback_override)
    return {
        "schema_version": subject.BENCHMARK_SCHEMA,
        "status": subject.BENCHMARK_STATUS,
        "callback_benchmark": callback,
        "decision_benchmark": {"decision_count": 1_000, "latency_p99_us": 9_000.0},
        subject.BENCHMARK_CANONICAL_FIELD: "4" * 64,
    }


def _capture(samples: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "collector_pid": 303,
        "benchmark_pid": 404,
        "benchmark_pid_start_ticks": 4_000,
        "live_pid": 202,
        "live_pid_start_ticks": 2_000,
        "benchmark_command_sha256": "5" * 64,
        "benchmark_launch_monotonic_ns": 1_000,
        "benchmark_exit_monotonic_ns": 2_000,
        "baseline_main_health_generation": 1,
        "final_main_health_generation": 2,
        "baseline_main_health_line_sha256": "1" * 64,
        "final_main_health_line_sha256": "2" * 64,
        "baseline_lifecycle_health_line_sha256": "3" * 64,
        "final_lifecycle_health_line_sha256": "4" * 64,
        "benchmark_returncode": 0,
        "benchmark_stdout_sha256": "6" * 64,
        "benchmark_stderr_sha256": "7" * 64,
        "sample_series_sha256": subject.canonical_sha256(samples),
        "sample_count": len(samples),
        "health_source": "existing_aggregate_log_only",
        "market_stream_connection_created": False,
    }


def _resource_payload(
    *,
    samples: list[dict[str, Any]] | None = None,
    baseline: dict[str, Any] | None = None,
    final: dict[str, Any] | None = None,
    benchmark: dict[str, Any] | None = None,
    prior: dict[str, Any] | None = None,
    disabled: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sample_rows = samples or _samples()
    live = _live_identity()
    return subject.build_resource_receipt(
        host={
            "instance_id": subject.CURRENT_INSTANCE_ID,
            "instance_type": subject.CURRENT_INSTANCE_TYPE,
            "logical_cpu_count": 2,
            "mem_total_mib": 3_850.0,
            "instance_identity_source": "linux_dmi_board_asset_tag_and_product_name",
            "dmi_board_asset_tag_sha256": hashlib_sha_text(subject.CURRENT_INSTANCE_ID),
            "dmi_product_name_sha256": hashlib_sha_text(subject.CURRENT_INSTANCE_TYPE),
        },
        runtime_execution=_runtime_execution(),
        collector_execution=_collector_execution(),
        runtime_sources=_runtime_sources(),
        exact_deployed_files=_deployed_files(),
        prior_process=prior or _prior_process(),
        disabled_process=disabled or _disabled_process(),
        pre_live_identity=live,
        post_live_identity=live,
        baseline_health=baseline or _health(1),
        final_health=final or _health(2),
        samples=sample_rows,
        capture=_capture(sample_rows),
        benchmark_receipt=benchmark or _benchmark(),
        generated_utc="2026-08-24T00:00:00Z",
    )


def _write_receipt(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="ascii")
    path.chmod(0o600)


def _recanonicalize_resource(payload: dict[str, Any]) -> None:
    payload[subject.RESOURCE_CANONICAL_FIELD] = subject.document_sha256(
        payload, subject.RESOURCE_CANONICAL_FIELD
    )


def _health_line(timestamp: str, *, book_overflow: int = 1_662) -> str:
    values = {name: 0 for name in subject.WINDOW_ZERO_COUNTERS[:-2]}
    values["globalFlowBookOverflow"] = book_overflow
    base = {
        "booleanCooldownEnabled": 1,
        "booleanCooldownUpdates": 2_000,
        "buyE3CooldownEnabled": 0,
        "deepBookBuffer": 0,
        **values,
    }
    fields = " ".join(f"{name}={value}" for name, value in base.items())
    return f"{timestamp} [main] INFO HEALTH {fields}\n"


def test_schema_surface_and_cycle_break_are_explicit() -> None:
    assert subject.RESOURCE_SCHEMA.endswith("current_host_concurrent_resource_gate.v3")
    assert subject.RESOURCE_STATUS == "fresh_disabled_same_pid_concurrent_gate_passed"
    assert subject.RESOURCE_CANONICAL_FIELD == "canonical_resource_receipt_sha256"
    assert subject.AUTHORITY_DESIGN == {
        "runtime_authority": "immutable_direct_owner_v3_release",
        "runtime_authority_release_file_sha256": subject.DIRECT_V3_RELEASE_FILE_SHA256,
        "runtime_authority_release_canonical_sha256": (subject.DIRECT_V3_RELEASE_CANONICAL_SHA256),
        "resource_receipt_is_post_authority_completion_evidence": True,
        "resource_receipt_is_not_embedded_in_direct_v3_release": True,
        "direct_v3_release_does_not_depend_on_resource_receipt": True,
        "later_evidence_completion_may_bind_resource_receipt": True,
    }


def test_runtime_source_binding_includes_sparse_repair_and_rejects_old_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[1]
    binding = subject.bind_current_v3_runtime_sources(
        runtime_repository_root=root,
        collector_repository_root=root,
    )
    assert binding["sparse_window_repair_bound"] is True
    assert (
        binding["files"]["buy_e3_runtime"]["sha256"]
        == "f5cbcae9f6c69fc20be79516e35b3d966ea32bf973ea0ede1797cbe02f501a9d"
    )

    real_hash = subject.file_sha256

    def drift(path: Path) -> str:
        if path.as_posix().endswith("strategy/boolean_cooldown_buy_e3.py"):
            return "0" * 64
        return real_hash(path)

    monkeypatch.setattr(subject, "file_sha256", drift)
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="pre-sparse"):
        subject.bind_current_v3_runtime_sources(
            runtime_repository_root=root,
            collector_repository_root=root,
        )


def test_health_tail_preserves_nonzero_absolute_counter_baseline(tmp_path: Path) -> None:
    log = tmp_path / "maker.log"
    log.write_text(
        _health_line("2026-08-24 00:00:00")
        + ("2026-08-24 00:00:00 [main] INFO ORDER_LIFECYCLE_JOURNAL_V2_HEALTH drops=7 errors=0\n"),
        encoding="utf-8",
    )
    snapshot = subject.LiveHealthTail(log).snapshot()
    assert snapshot["counter_values"]["globalFlowBookOverflow"] == 1_662
    assert snapshot["counter_values"]["orderLifecycleV2Drops"] == 7
    assert snapshot["buy_e3_enabled"] == 0
    assert snapshot["main_wall_timestamp_s"] > 0


def test_health_parser_fails_closed_on_new_unbound_drop_counter() -> None:
    line = _health_line("2026-08-24 00:00:00").rstrip() + " futureQueueDropped=0"
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="counter set drifted"):
        subject._parse_main_health(line, generation=1)  # noqa: SLF001


def test_counter_window_requires_delta_zero_not_absolute_zero() -> None:
    payload = _resource_payload()
    window = payload["counter_window"]
    assert window["absolute_baseline"]["globalFlowBookOverflow"] == 1_662
    assert window["absolute_final"]["globalFlowBookOverflow"] == 1_662
    assert window["window_delta"]["globalFlowBookOverflow"] == 0
    assert payload["checks"]["all_drop_invalid_overflow_window_deltas_zero"] is True


def test_resource_receipt_round_trip_and_collector_cross_binding(tmp_path: Path) -> None:
    payload = _resource_payload()
    path = tmp_path / "resource.json"
    _write_receipt(path, payload)
    validated = subject.validate_resource_receipt(
        path,
        expected_collector_execution=_collector_execution(),
    )
    assert validated == payload
    assert validated["runtime_execution"]["execution_commit"] == subject.DIRECT_V3_EXECUTION_COMMIT
    assert validated["sample_rows_persisted"] is False
    assert stat.S_IMODE(path.stat().st_mode) == 0o600

    changed = _collector_execution()
    changed["execution_commit"] = "d" * 40
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="cross-binding"):
        subject.validate_resource_receipt(path, expected_collector_execution=changed)


@pytest.mark.parametrize(
    ("sample_override", "failure"),
    (
        ({"mem_available_mib": 511.99}, "min_mem"),
        ({"live_rss_mib": 512.01}, "live_rss"),
        ({"benchmark_rss_mib": 256.01}, "benchmark_rss"),
        ({"live_rss_mib": 512.0, "benchmark_rss_mib": 256.01}, "combined_rss"),
        ({"oom_kill": 5}, "oom_window"),
        ({"swap_in": 12}, "swap_window"),
        ({"deep_book_buffer": 1}, "deep_book"),
    ),
)
def test_resource_builder_rejects_each_resource_limit(
    sample_override: dict[str, Any], failure: str
) -> None:
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match=failure):
        _resource_payload(samples=_samples(**sample_override))


def test_resource_builder_rejects_nonzero_counter_delta() -> None:
    final_counters = _counters()
    final_counters["globalFlowBookOverflow"] += 1
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="drop_invalid"):
        _resource_payload(final=_health(2, counters=final_counters))


@pytest.mark.parametrize(
    ("benchmark", "failure"),
    (
        (_benchmark(achieved_to_observed_rate=1.99), "true_2x"),
        (_benchmark(latency_p99_us=2_000.01), "callback_p99"),
    ),
)
def test_resource_builder_rejects_callback_gate(benchmark: dict[str, Any], failure: str) -> None:
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match=failure):
        _resource_payload(benchmark=benchmark)


def test_resource_builder_rejects_decision_count_and_p99() -> None:
    benchmark = _benchmark()
    benchmark["decision_benchmark"]["decision_count"] = 999
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="exactly_1000"):
        _resource_payload(benchmark=benchmark)
    benchmark["decision_benchmark"]["decision_count"] = 1_000
    benchmark["decision_benchmark"]["latency_p99_us"] = 10_000.01
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="decision_p99"):
        _resource_payload(benchmark=benchmark)


def test_resource_builder_requires_fresh_disabled_pid_and_start_ticks() -> None:
    prior = _prior_process()
    prior["pid"] = 202
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="fresh restart"):
        _resource_payload(prior=prior)
    prior = _prior_process()
    prior["pid_start_ticks"] = 2_000
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="fresh restart"):
        _resource_payload(prior=prior)


def test_resource_validator_rejects_tampered_counter_even_when_recanonicalized(
    tmp_path: Path,
) -> None:
    payload = _resource_payload()
    window = payload["counter_window"]
    window["absolute_final"]["globalFlowBookOverflow"] += 1
    window["window_delta"]["globalFlowBookOverflow"] = 1
    window["final_manifest_sha256"] = subject.canonical_sha256(window["absolute_final"])
    window["window_delta_manifest_sha256"] = subject.canonical_sha256(window["window_delta"])
    _recanonicalize_resource(payload)
    path = tmp_path / "tampered.json"
    _write_receipt(path, payload)
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="counter window"):
        subject.validate_resource_receipt(path)


def test_resource_validator_rejects_extra_field_and_non_0600(tmp_path: Path) -> None:
    payload = _resource_payload()
    payload["unexpected"] = True
    _recanonicalize_resource(payload)
    path = tmp_path / "extra.json"
    _write_receipt(path, payload)
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="identity drifted"):
        subject.validate_resource_receipt(path)
    path.chmod(0o644)
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="mode"):
        subject.validate_resource_receipt(path)


def test_disabled_config_requires_no_shadow_and_exact_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy = {
        "buy_e3_cooldown_policy_enabled": False,
        "boolean_cooldown_policy_enabled": True,
        "buy_fill_selection_shadow_enabled": False,
        "dynamic_fill_hazard_shadow_enabled": False,
        "cross_venue_fair_price_shadow_enabled": False,
        "inventory_campaign_shadow_enabled": False,
        "market_tape_enabled": False,
        "buy_e3_cooldown_artifact_manifest_sha256": subject.EXACT_DEPLOYED_FILE_SHA256["manifest"],
        "buy_e3_cooldown_artifact_sha256": subject.EXACT_ARTIFACT_SHA256,
        "buy_e3_cooldown_policy_sha256": subject.EXACT_DEPLOYED_FILE_SHA256["policy"],
        "buy_e3_cooldown_predicate_bundle_sha256": subject.EXACT_DEPLOYED_FILE_SHA256[
            "predicate_bundle"
        ],
    }
    path = tmp_path / "disabled.yaml"
    path.write_text(yaml.safe_dump({"strategy": strategy}), encoding="utf-8")
    monkeypatch.setattr(subject, "EXPECTED_DISABLED_CONFIG_SHA256", subject.file_sha256(path))
    assert subject._validate_disabled_config(path) == subject.file_sha256(path)  # noqa: SLF001

    strategy["dynamic_fill_hazard_shadow_enabled"] = True
    path.write_text(yaml.safe_dump({"strategy": strategy}), encoding="utf-8")
    monkeypatch.setattr(subject, "EXPECTED_DISABLED_CONFIG_SHA256", subject.file_sha256(path))
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="no-shadow"):
        subject._validate_disabled_config(path)  # noqa: SLF001


def test_host_identity_is_read_directly_from_linux_dmi(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proc = tmp_path / "proc"
    proc.mkdir()
    (proc / "meminfo").write_text(
        "MemTotal:       3942400 kB\nMemAvailable:   3000000 kB\n",
        encoding="ascii",
    )
    dmi = tmp_path / "dmi"
    dmi.mkdir()
    (dmi / "board_asset_tag").write_text(subject.CURRENT_INSTANCE_ID + "\n", encoding="ascii")
    (dmi / "product_name").write_text(subject.CURRENT_INSTANCE_TYPE + "\n", encoding="ascii")
    monkeypatch.setattr(subject.os, "cpu_count", lambda: 2)
    identity = subject.host_identity(
        instance_id=subject.CURRENT_INSTANCE_ID,
        instance_type=subject.CURRENT_INSTANCE_TYPE,
        proc_root=proc,
        dmi_root=dmi,
    )
    assert identity["instance_identity_source"] == "linux_dmi_board_asset_tag_and_product_name"
    assert identity["instance_id"] == subject.CURRENT_INSTANCE_ID
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="disagrees"):
        subject.host_identity(
            instance_id="i-wrong",
            instance_type=subject.CURRENT_INSTANCE_TYPE,
            proc_root=proc,
            dmi_root=dmi,
        )


def test_process_snapshot_and_validator_bind_exact_direct_v3(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    config = runtime / "disabled.yaml"
    config.write_text("strategy: {}\n", encoding="ascii")
    executable = runtime / "python"
    executable.write_text("python", encoding="ascii")
    executable.chmod(0o700)
    pid = 222
    pid_file = tmp_path / "maker.pid"
    pid_file.write_text(str(pid), encoding="ascii")
    proc = tmp_path / "proc"
    process = proc / str(pid)
    process.mkdir(parents=True)
    (process / "cmdline").write_bytes(f"{executable}\0live/main.py\0--config\0{config}\0".encode())
    (process / "stat").write_text(
        f"{pid} (python) " + " ".join(["S", *(["1"] * 30)]),
        encoding="ascii",
    )
    (process / "cwd").symlink_to(runtime, target_is_directory=True)
    (process / "exe").symlink_to(executable)
    monkeypatch.setattr(
        subject, "capture_git_execution", lambda *_args, **_kwargs: _runtime_execution(str(runtime))
    )
    monkeypatch.setattr(
        subject,
        "_validate_disabled_config",
        lambda _path: subject.EXPECTED_DISABLED_CONFIG_SHA256,
    )
    monkeypatch.setattr(
        subject,
        "file_sha256",
        lambda path: (
            subject.EXPECTED_DISABLED_CONFIG_SHA256 if Path(path) == config else hashlib_sha(path)
        ),
    )

    payload = subject.capture_process_snapshot(
        runtime_repository_root=runtime,
        runtime_annotated_tag=subject.DIRECT_V3_ANNOTATED_TAG,
        pid_file=pid_file,
        config_path=config,
        expected_buy_e3_enabled=False,
        proc_root=proc,
        generated_utc="2026-08-24T00:00:00Z",
    )
    path = tmp_path / "process.json"
    _write_receipt(path, payload)
    assert subject.validate_process_snapshot(path) == payload


def hashlib_sha(path: Path) -> str:
    import hashlib

    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def hashlib_sha_text(value: str) -> str:
    import hashlib

    return hashlib.sha256(f"{value}\n".encode("ascii")).hexdigest()


def test_exact_four_file_benchmark_is_aggregate_only_and_exact_1000(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployed = _deployed_files()
    runtime_sources = _runtime_sources()

    class FakePolicy:
        def __init__(self) -> None:
            self.evaluations = 0

        @property
        def artifact_sha256(self) -> str:
            return subject.EXACT_ARTIFACT_SHA256

        def observe_depth(self, **_kwargs: Any) -> None:
            return None

        def evaluate(self, **_kwargs: Any) -> SimpleNamespace:
            self.evaluations += 1
            if self.evaluations == 1:
                return SimpleNamespace(action_id=subject.CONTROL_ACTION, fallback_reason="cold")
            return SimpleNamespace(action_id="COOLDOWN_173S", fallback_reason=None)

    fake_policy = FakePolicy()
    monkeypatch.setattr(subject, "bind_exact_deployed_files", lambda **_kwargs: deployed)
    monkeypatch.setattr(
        subject,
        "bind_current_v3_runtime_sources",
        lambda **_kwargs: runtime_sources,
    )
    monkeypatch.setattr(
        subject.LiveBuyE3CooldownPolicy,
        "from_files",
        lambda **_kwargs: fake_policy,
    )

    class FakeClock:
        value = 0.0

        @classmethod
        def perf_counter(cls) -> float:
            cls.value += 0.00001
            return cls.value

        @classmethod
        def perf_counter_ns(cls) -> int:
            cls.value += 0.000001
            return int(cls.value * 1e9)

        @classmethod
        def process_time(cls) -> float:
            return cls.value / 10.0

        @classmethod
        def sleep(cls, seconds: float) -> None:
            cls.value += max(0.0, seconds)

    monkeypatch.setattr(subject.time, "perf_counter", FakeClock.perf_counter)
    monkeypatch.setattr(subject.time, "perf_counter_ns", FakeClock.perf_counter_ns)
    monkeypatch.setattr(subject.time, "process_time", FakeClock.process_time)
    monkeypatch.setattr(subject.time, "sleep", FakeClock.sleep)
    output = tmp_path / "benchmark.json"
    payload = subject.run_exact_four_file_benchmark(
        collector_repository_root=tmp_path,
        runtime_repository_root=tmp_path,
        manifest_path=tmp_path / "manifest",
        policy_path=tmp_path / "policy",
        predicate_bundle_path=tmp_path / "bundle",
        direct_active_release_path=tmp_path / "release",
        observed_live_rate_hz=40.0,
        output_path=output,
        paced_duration_s=2.0,
        generated_utc="2026-08-24T00:00:00Z",
    )
    assert payload["decision_benchmark"]["decision_count"] == 1_000
    assert "observed_action_ids" not in payload["decision_benchmark"]
    assert payload["evidence_boundary"]["benchmark_action_rows_persisted"] is False
    assert subject.validate_benchmark_receipt(output) == payload


def test_concurrent_capture_orchestrates_fresh_disabled_same_pid_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = tmp_path / "runtime"
    collector_root = tmp_path / "collector"
    runtime_root.mkdir()
    collector_root.mkdir()
    prior_path = tmp_path / "prior.json"
    _write_receipt(
        prior_path,
        _full_process_receipt(
            pid=101,
            start_ticks=1_000,
            enabled=True,
            config_path="/runtime/repo/live/private/e3/config.active.yaml",
        ),
    )
    disabled = _disabled_process()
    runtime_execution = _runtime_execution(str(runtime_root))
    collector_execution = {**_collector_execution(), "repository_root": str(collector_root)}
    deployed = _deployed_files()
    runtime_sources = _runtime_sources()
    host = {
        "instance_id": subject.CURRENT_INSTANCE_ID,
        "instance_type": subject.CURRENT_INSTANCE_TYPE,
        "logical_cpu_count": 2,
        "mem_total_mib": 3_850.0,
        "instance_identity_source": "linux_dmi_board_asset_tag_and_product_name",
        "dmi_board_asset_tag_sha256": hashlib_sha_text(subject.CURRENT_INSTANCE_ID),
        "dmi_product_name_sha256": hashlib_sha_text(subject.CURRENT_INSTANCE_TYPE),
    }
    monkeypatch.setattr(
        subject,
        "capture_git_execution",
        lambda _root, *, annotated_tag, runtime_authority: (
            runtime_execution if runtime_authority else collector_execution
        ),
    )
    monkeypatch.setattr(
        subject,
        "bind_current_v3_runtime_sources",
        lambda **_kwargs: runtime_sources,
    )
    monkeypatch.setattr(subject, "capture_process_snapshot", lambda **_kwargs: disabled)
    monkeypatch.setattr(
        subject,
        "_process_identity_from_live",
        lambda **_kwargs: _live_identity(),
    )
    monkeypatch.setattr(subject, "bind_exact_deployed_files", lambda **_kwargs: deployed)
    monkeypatch.setattr(subject, "host_identity", lambda **_kwargs: host)
    monkeypatch.setattr(subject, "_proc_start_ticks", lambda _root, _pid: 4_000)

    states = [
        {**_health(1), "main_wall_timestamp_s": 1_000.0},
        {**_health(2), "main_wall_timestamp_s": 1_060.0},
        {**_health(2), "main_wall_timestamp_s": 1_060.0},
        {**_health(2), "main_wall_timestamp_s": 1_060.0},
        {**_health(2), "main_wall_timestamp_s": 1_060.0},
        {**_health(3), "main_wall_timestamp_s": 1_120.0},
    ]

    class FakeTail:
        def __init__(self) -> None:
            self.index = 0

        def snapshot(self) -> dict[str, Any]:
            value = states[min(self.index, len(states) - 1)]
            self.index += 1
            return deepcopy(value)

    tail = FakeTail()
    monkeypatch.setattr(subject, "LiveHealthTail", lambda _path: tail)

    class FakeProcess:
        pid = 404
        returncode = 0

        def __init__(self) -> None:
            self.polls = 0

        def poll(self) -> int | None:
            self.polls += 1
            return None if self.polls <= 2 else 0

        def communicate(self, timeout: float) -> tuple[str, str]:
            assert timeout == 15.0
            return "aggregate benchmark complete", ""

    fake_process = FakeProcess()
    samples = _samples()
    sample_index = {"value": 0}

    def sampler(_live_pid: int, _benchmark_pid: int) -> dict[str, Any]:
        row = dict(samples[min(sample_index["value"], 1)])
        sample_index["value"] += 1
        row.pop("monotonic_ns")
        return row

    observed_rate = 1_000 / 60.0
    benchmark = _benchmark(
        observed_live_rate_hz=observed_rate,
        achieved_rate_hz=observed_rate * 2.05,
    )
    benchmark["exact_deployed_files"] = deployed
    benchmark["runtime_sources"] = runtime_sources
    monkeypatch.setattr(subject, "validate_benchmark_receipt", lambda _path: benchmark)
    ticks = {"value": 0}

    def monotonic_ns() -> int:
        ticks["value"] += 1_000_000_000
        return ticks["value"]

    benchmark_output = tmp_path / "benchmark.json"
    output = tmp_path / "resource.json"
    payload = subject.capture_concurrent_resource_gate(
        collector_repository_root=collector_root,
        collector_annotated_tag=collector_execution["annotated_tag"],
        runtime_repository_root=runtime_root,
        pid_file=tmp_path / "maker.pid",
        disabled_config_path=Path(disabled["config_path"]),
        prior_process_receipt_path=prior_path,
        live_log_path=tmp_path / "maker.log",
        manifest_path=tmp_path / "manifest",
        policy_path=tmp_path / "policy",
        predicate_bundle_path=tmp_path / "bundle",
        direct_active_release_path=tmp_path / "release",
        instance_id=subject.CURRENT_INSTANCE_ID,
        instance_type=subject.CURRENT_INSTANCE_TYPE,
        benchmark_output_path=benchmark_output,
        output_path=output,
        python_executable=Path(subject.sys.executable),
        paced_duration_s=2.0,
        sample_interval_s=0.1,
        _popen=lambda *_args, **_kwargs: fake_process,
        _sleep=lambda _seconds: None,
        _monotonic_ns=monotonic_ns,
        _resource_sampler=sampler,
        _health_tail_factory=lambda _path: tail,
    )
    assert payload["status"] == subject.RESOURCE_STATUS
    assert payload["fresh_disabled_process"]["same_pid_pre_post"] is True
    assert payload["observed"]["decision_count"] == 1_000
    assert output.is_file()


def test_cli_exposes_snapshot_benchmark_capture_and_validate() -> None:
    parser = subject._parser()  # noqa: SLF001
    assert (
        parser.parse_args(
            ["validate", "--receipt", "/tmp/resource.json", "--kind", "resource"]
        ).kind
        == "resource"
    )
    help_text = parser.format_help()
    for command in ("snapshot-process", "benchmark", "capture-concurrent", "validate"):
        assert command in help_text


def test_atomic_receipt_is_immutable_mode_0600(tmp_path: Path) -> None:
    path = tmp_path / "receipt.json"
    subject.atomic_write_receipt(path, {"status": "ok"})
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="already exists"):
        subject.atomic_write_receipt(path, {"status": "changed"})


def test_benchmark_validator_rejects_action_rows_even_if_recanonicalized(
    tmp_path: Path,
) -> None:
    payload = {
        "schema_version": subject.BENCHMARK_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "status": subject.BENCHMARK_STATUS,
        "generated_utc": "2026-08-24T00:00:00Z",
        "authority_design": deepcopy(subject.AUTHORITY_DESIGN),
        "runtime_sources": _runtime_sources(),
        "exact_deployed_files": _deployed_files(),
        "thresholds": {
            "min_achieved_to_observed_rate": 2.0,
            "max_callback_p99_us": 2_000.0,
            "exact_decision_count": 1_000,
            "max_decision_p99_us": 10_000.0,
        },
        "callback_benchmark": {
            "observed_live_rate_hz": 100.0,
            "target_rate_hz": 200.0,
            "callback_count": 2_000,
            "duration_s": 10.0,
            "achieved_rate_hz": 200.0,
            "achieved_to_observed_rate": 2.0,
            "latency_p50_us": 10.0,
            "latency_p99_us": 20.0,
            "latency_max_us": 30.0,
            "cpu_percent_total_host_scale": 10.0,
        },
        "decision_benchmark": {
            "decision_count": 1_000,
            "latency_p50_us": 10.0,
            "latency_p99_us": 20.0,
            "latency_max_us": 30.0,
            "action_rows": ["COOLDOWN_173S"],
        },
        "benchmark_process_max_rss_mib": 100.0,
        "checks": {
            "exact_four_deployed_files_bound": True,
            "true_2x_observed_callback_rate": True,
            "callback_p99_at_most_2ms": True,
            "exactly_1000_decisions": True,
            "decision_p99_at_most_10ms": True,
            "aggregate_only_no_action_rows": True,
        },
        "evidence_boundary": deepcopy(subject.EVIDENCE_BOUNDARY),
    }
    payload[subject.BENCHMARK_CANONICAL_FIELD] = subject.document_sha256(
        payload, subject.BENCHMARK_CANONICAL_FIELD
    )
    path = tmp_path / "benchmark-with-actions.json"
    _write_receipt(path, payload)
    with pytest.raises(subject.BuyE3CurrentHostResourceGateError, match="fields drifted"):
        subject.validate_benchmark_receipt(path)
