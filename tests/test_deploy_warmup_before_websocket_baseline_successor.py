from __future__ import annotations

import json
import subprocess
from collections.abc import Sequence
from copy import deepcopy

import pytest

from scripts.deploy_warmup_before_websocket_baseline_successor import (
    DEFAULT_CANDIDATE_MANIFEST,
    DEFAULT_CURRENT_RELEASE_MANIFEST,
    DEFAULT_CURRENT_RUNTIME_RECEIPT,
    EXPECTED_ACTION_ENABLEMENT,
    FORBIDDEN_LOG_MARKERS,
    REQUIRED_ORDERED_LOG_MARKERS,
    STABILITY_SCHEMA_VERSION,
    TARGET_PATHS,
    _atomic_deploy_source,
    _atomic_restore_source,
    _log_checkpoint_source,
    _prepare_stage_source,
    _quiescence_probe_source,
    _runtime_probe_source,
    _seal_receipt,
    _stage_validation_source,
    _startup_stability_source,
    _validate_stability,
    build_plan,
    execute_deploy_transaction,
    execute_stage_validation,
    load_bound_deployment,
)


class QueueRunner:
    def __init__(self, outputs: Sequence[dict | subprocess.CompletedProcess[str]]) -> None:
        self.outputs = list(outputs)
        self.calls: list[list[str]] = []

    def __call__(self, command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(command))
        if not self.outputs:
            raise AssertionError(f"unexpected command: {command}")
        value = self.outputs.pop(0)
        if isinstance(value, subprocess.CompletedProcess):
            return value
        return subprocess.CompletedProcess(
            args=list(command),
            returncode=0,
            stdout=json.dumps(value, sort_keys=True) + "\n",
            stderr="",
        )


@pytest.fixture(scope="module")
def bound():
    return load_bound_deployment(
        DEFAULT_CANDIDATE_MANIFEST,
        DEFAULT_CURRENT_RELEASE_MANIFEST,
        DEFAULT_CURRENT_RUNTIME_RECEIPT,
    )


@pytest.fixture()
def plan(bound):
    return build_plan(bound=bound, deployment_instance_id="pytest-safe-baseline")


def _plain() -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=0, stdout="ok\n", stderr="")


def _runtime_probe(bound, *, candidate: bool, pid: int) -> dict:
    return {
        "schema_version": "warmup_before_websocket_current_runtime_probe.v1",
        "maker_pid": pid,
        "maker_cmdline": (
            f"{bound['current_venv']}/bin/python live/main.py --config live/config.yaml"
        ),
        "active_venv": bound["current_venv"],
        "python_prefix": bound["current_venv"],
        "runtime_files": dict(bound["candidate_hashes"] if candidate else bound["current_hashes"]),
        "journal_enabled": not candidate,
        "strategy_flags": dict(EXPECTED_ACTION_ENABLEMENT),
    }


def _checkpoint(*, offset: int = 100) -> dict:
    return {
        "schema_version": "warmup_before_websocket_log_checkpoint.v1",
        "log_path": "/srv/example-live/NarrowGate_BTCUSDC/logs/maker.log",
        "inode": 42,
        "offset": offset,
    }


def _stability(
    pid: int,
    *,
    duration_s: float = 25.1,
    same_pid: bool = True,
    ordered: bool = True,
    forbidden_hits: list[str] | None = None,
) -> dict:
    hits = [] if forbidden_hits is None else forbidden_hits
    return {
        "schema_version": STABILITY_SCHEMA_VERSION,
        "expected_maker_pid": pid,
        "stability_window_s": 25.0,
        "observed_duration_s": duration_s,
        "canonical_bucket_s": 10.0,
        "minimum_stable_buckets": 2,
        "covered_canonical_buckets": duration_s / 10.0,
        "same_maker_pid": same_pid,
        "sample_count": 101,
        "first_processes": [{"pid": pid}],
        "last_processes": [{"pid": pid}] if same_pid else [],
        "log_identity_stable": True,
        "required_ordered_log_markers": list(REQUIRED_ORDERED_LOG_MARKERS),
        "required_marker_positions": [100, 200, 300],
        "startup_log_order_passed": ordered,
        "forbidden_log_hits": hits,
        "fatal_or_duplicate_grid_absent": not hits,
    }


def _stage_validation(bound) -> dict:
    return {
        "schema_version": "warmup_before_websocket_remote_stage_validation.v1",
        "remote_manifest_file_sha256": bound["candidate_manifest_file_sha256"],
        "remote_canonical_manifest_sha256": bound["candidate_manifest_sha256"],
        "validated_file_count": 6,
        "remote_import_passed": True,
        "candidate_preflight_passed": True,
        "candidate_journal_enabled": False,
        "strategy_config_semantics_unchanged": True,
        "loaded_module_paths": {},
    }


def _staging_receipt(plan, bound) -> dict:
    return _seal_receipt(
        {
            "schema_version": "warmup_before_websocket_baseline_successor_deploy_evidence.v1",
            "stage": "staging",
            "status": "passed",
            "candidate_manifest_sha256": bound["candidate_manifest_sha256"],
            "current_release_manifest_sha256": bound["current_release_manifest_sha256"],
            "current_runtime_receipt_identity_sha256": bound[
                "current_runtime_receipt_identity_sha256"
            ],
            "mutation_plan_identity_sha256": plan["mutation_plan_identity_sha256"],
            "plan_identity_sha256": plan["plan_identity_sha256"],
            "deployment_instance_id": plan["deployment_instance_id"],
            "remote_stage_root": plan["stage_root"],
            "evidence": {"stage_validation": _stage_validation(bound)},
            "production_mutation_performed": False,
            "ssh_executed": True,
            "candidate_journal_enabled": False,
            "strategy_config_semantics_unchanged": True,
        }
    )


def test_bound_inputs_are_exact_attempt6_and_safe_candidate(bound) -> None:
    assert tuple(sorted(bound["candidate_hashes"])) == tuple(sorted(TARGET_PATHS))
    assert tuple(sorted(bound["current_hashes"])) == tuple(sorted(TARGET_PATHS))
    assert all(
        bound["candidate_hashes"][path] != bound["current_hashes"][path] for path in TARGET_PATHS
    )
    assert bound["current_pid"] == 1862917
    assert bound["current_runtime_receipt_identity_sha256"] == (
        "16a9cd221ffef0487d1059755a3e2e43342d37d8db6974574ebf77ef19f321f7"
    )
    assert bound["candidate_journal_enabled"] is False
    assert bound["current_journal_enabled"] is True
    assert bound["strategy_config_semantics_unchanged"] is True


def test_default_plan_is_no_ssh_no_mutation_and_fully_bound(plan, bound) -> None:
    assert plan["mode"] == "plan_only_no_ssh_no_mutation"
    assert plan["execution_performed"] is False
    assert plan["ssh_executed"] is False
    assert plan["production_mutation_performed"] is False
    assert plan["deployment_authorized"] is False
    assert plan["current_target_hashes"] == bound["current_hashes"]
    assert plan["candidate_target_hashes"] == bound["candidate_hashes"]
    assert plan["current_pid"] == bound["current_pid"]
    assert plan["current_venv"] == bound["current_venv"]
    assert plan["candidate_journal_enabled"] is False
    assert plan["strategy_config_semantics_unchanged"] is True
    assert plan["startup_stability_contract"]["stability_window_s"] == 25.0
    assert plan["startup_stability_contract"]["minimum_stable_buckets"] == 2
    assert plan["startup_stability_contract"]["required_ordered_log_markers"] == list(
        REQUIRED_ORDERED_LOG_MARKERS
    )
    assert plan["startup_stability_contract"]["forbidden_log_markers"] == list(
        FORBIDDEN_LOG_MARKERS
    )
    assert plan["stages"]["stage-validate"]["production_mutation"] is False
    assert plan["stages"]["deploy"]["owner_token_required"] is True
    assert plan["stages"]["automatic-restore"]["mandatory_after_any_post_stop_failure"] is True


def test_owner_token_binds_manifest_runtime_and_mutation_plan(bound, plan) -> None:
    binding = plan["owner_confirmation_token_binding"]
    assert binding == {
        "schema_version": "warmup_before_websocket_owner_token_binding.v1",
        "candidate_manifest_sha256": bound["candidate_manifest_sha256"],
        "current_runtime_receipt_identity_sha256": bound["current_runtime_receipt_identity_sha256"],
        "mutation_plan_identity_sha256": plan["mutation_plan_identity_sha256"],
    }
    changed = build_plan(
        bound=bound,
        deployment_instance_id="pytest-safe-baseline-other",
    )
    assert changed["mutation_plan_identity_sha256"] != plan["mutation_plan_identity_sha256"]
    assert changed["owner_confirmation_token"] != plan["owner_confirmation_token"]


def test_stability_window_below_25_seconds_is_rejected(bound) -> None:
    with pytest.raises(ValueError, match="at least 25 seconds"):
        build_plan(
            bound=bound,
            deployment_instance_id="too-short",
            stability_window_s=24.999,
        )


def test_stage_validation_is_isolated_and_machine_readable(plan, bound) -> None:
    runner = QueueRunner(
        [
            _runtime_probe(bound, candidate=False, pid=bound["current_pid"]),
            {
                "schema_version": "warmup_before_websocket_stage_prepare.v1",
                "isolated_stage_created": True,
                "stage_root": plan["stage_root"],
                "payload_root": plan["payload_root"],
            },
            _plain(),
            _stage_validation(bound),
        ]
    )
    receipt = execute_stage_validation(plan=plan, bound=bound, runner=runner)
    assert receipt["stage"] == "staging"
    assert receipt["status"] == "passed"
    assert receipt["production_mutation_performed"] is False
    assert receipt["candidate_journal_enabled"] is False
    assert receipt["strategy_config_semantics_unchanged"] is True
    assert len(runner.calls) == 4
    assert runner.calls[2][0] == "rsync"
    assert all(call[0] in {"ssh", "rsync"} for call in runner.calls)


def test_owner_token_failure_happens_before_any_runner_call(plan, bound) -> None:
    runner = QueueRunner([])
    with pytest.raises(PermissionError, match="owner confirmation token mismatch"):
        execute_deploy_transaction(
            plan=plan,
            bound=bound,
            staging_receipt=_staging_receipt(plan, bound),
            execute=True,
            owner_token="wrong",
            runner=runner,
        )
    assert runner.calls == []


def test_current_pid_drift_fails_before_stop_without_mutation(plan, bound) -> None:
    runner = QueueRunner([_runtime_probe(bound, candidate=False, pid=bound["current_pid"] + 1)])
    receipt = execute_deploy_transaction(
        plan=plan,
        bound=bound,
        staging_receipt=_staging_receipt(plan, bound),
        execute=True,
        owner_token=plan["owner_confirmation_token"],
        runner=runner,
    )
    assert receipt["status"] == "precondition_failed_no_mutation"
    assert receipt["production_mutation_performed"] is False
    assert receipt["automatic_restore_performed"] is False
    assert len(runner.calls) == 1


def test_tampered_staging_receipt_fails_before_any_runner_call(plan, bound) -> None:
    receipt = deepcopy(_staging_receipt(plan, bound))
    receipt["evidence"]["stage_validation"]["remote_import_passed"] = False
    receipt.pop("receipt_identity_sha256")
    receipt = _seal_receipt(receipt)
    runner = QueueRunner([])
    with pytest.raises(PermissionError, match="not deploy eligible"):
        execute_deploy_transaction(
            plan=plan,
            bound=bound,
            staging_receipt=receipt,
            execute=True,
            owner_token=plan["owner_confirmation_token"],
            runner=runner,
        )
    assert runner.calls == []


def test_successful_deploy_rechecks_hashes_pid_logs_and_journal(plan, bound) -> None:
    candidate_pid = 2001001
    runner = QueueRunner(
        [
            _runtime_probe(bound, candidate=False, pid=bound["current_pid"]),
            _checkpoint(),
            _plain(),
            {
                "schema_version": "warmup_before_websocket_quiescence.v1",
                "controlled_stop_quiescent": True,
                "maker_pid_count": 0,
                "exchange_open_order_count": 0,
            },
            {
                "schema_version": "warmup_before_websocket_atomic_deploy.v1",
                "deployment_files_applied": True,
                "deployed_file_count": 6,
                "backup_root": plan["backup_root"],
                "candidate_journal_enabled": False,
                "strategy_config_semantics_unchanged": True,
            },
            _plain(),
            _runtime_probe(bound, candidate=True, pid=candidate_pid),
            _stability(candidate_pid),
            _runtime_probe(bound, candidate=True, pid=candidate_pid),
        ]
    )
    receipt = execute_deploy_transaction(
        plan=plan,
        bound=bound,
        staging_receipt=_staging_receipt(plan, bound),
        execute=True,
        owner_token=plan["owner_confirmation_token"],
        runner=runner,
    )
    assert receipt["status"] == "passed"
    assert receipt["production_mutation_performed"] is True
    assert receipt["automatic_restore_performed"] is False
    evidence = receipt["evidence"]
    assert evidence["candidate_startup_stability_gates"]["minimum_25s_observed"] is True
    assert evidence["candidate_startup_stability_gates"]["startup_log_order_passed"] is True
    assert evidence["stable_candidate_runtime_probe"]["runtime_files"] == bound["candidate_hashes"]
    assert evidence["stable_candidate_runtime_probe"]["journal_enabled"] is False
    assert len(runner.calls) == 9


def test_delayed_candidate_death_at_16s_triggers_exact_attempt6_restore(plan, bound) -> None:
    candidate_pid = 2002001
    restored_pid = 2003001
    delayed_death = _stability(
        candidate_pid,
        duration_s=16.1,
        same_pid=False,
    )
    runner = QueueRunner(
        [
            _runtime_probe(bound, candidate=False, pid=bound["current_pid"]),
            _checkpoint(offset=100),
            _plain(),
            {
                "schema_version": "warmup_before_websocket_quiescence.v1",
                "controlled_stop_quiescent": True,
                "maker_pid_count": 0,
                "exchange_open_order_count": 0,
            },
            {
                "schema_version": "warmup_before_websocket_atomic_deploy.v1",
                "deployment_files_applied": True,
                "deployed_file_count": 6,
                "backup_root": plan["backup_root"],
                "candidate_journal_enabled": False,
                "strategy_config_semantics_unchanged": True,
            },
            _plain(),
            _runtime_probe(bound, candidate=True, pid=candidate_pid),
            delayed_death,
            # Automatic restoration starts here.
            _plain(),
            {
                "schema_version": "warmup_before_websocket_quiescence.v1",
                "controlled_stop_quiescent": True,
                "maker_pid_count": 0,
                "exchange_open_order_count": 0,
            },
            {
                "schema_version": "warmup_before_websocket_atomic_restore.v1",
                "rollback_not_required": False,
                "rollback_files_restored": True,
                "restored_file_count": 6,
                "active_venv_target_restored": bound["current_venv"],
                "current_attempt6_identity_restored": True,
            },
            _checkpoint(offset=500),
            _plain(),
            _runtime_probe(bound, candidate=False, pid=restored_pid),
            _stability(restored_pid),
            _runtime_probe(bound, candidate=False, pid=restored_pid),
        ]
    )
    receipt = execute_deploy_transaction(
        plan=plan,
        bound=bound,
        staging_receipt=_staging_receipt(plan, bound),
        execute=True,
        owner_token=plan["owner_confirmation_token"],
        runner=runner,
    )
    assert receipt["status"] == "candidate_failed_attempt6_restored"
    assert receipt["automatic_restore_performed"] is True
    assert receipt["attempt6_restored_and_stable"] is True
    recovery = receipt["evidence"]["recovery"]
    assert recovery["automatic_restore_succeeded"] is True
    assert recovery["same_stability_contract_used"] is True
    assert recovery["stable_runtime_probe"]["runtime_files"] == bound["current_hashes"]
    assert recovery["stable_runtime_probe"]["journal_enabled"] is True
    assert len(runner.calls) == 16


@pytest.mark.parametrize(
    ("ordered", "hits"),
    [
        (False, []),
        (True, ["Fatal error:"]),
        (True, ["duplicate-grid"]),
    ],
)
def test_stability_rejects_bad_log_order_fatal_and_duplicate_grid(
    plan,
    ordered: bool,
    hits: list[str],
) -> None:
    with pytest.raises(RuntimeError, match="startup stability contract failed"):
        _validate_stability(
            _stability(123, ordered=ordered, forbidden_hits=hits),
            plan=plan,
            expected_pid=123,
        )


def test_atomic_sources_bind_backup_restore_and_do_not_switch_candidate_venv() -> None:
    deploy = _atomic_deploy_source()
    restore = _atomic_restore_source()
    stability = _startup_stability_source()
    assert "os.replace(partial, backup)" in deploy
    assert deploy.index("os.replace(partial, backup)") < deploy.index(
        "for logical in sorted(candidate)"
    )
    assert "active.resolve(strict=True) != expected_venv" in deploy
    assert ".venv-active.successor" not in deploy
    assert "current_attempt6_identity_restored" in restore
    assert "mutation_plan_identity_sha256" in restore
    assert "time.sleep" in stability
    assert "startup_log_order_passed" in stability
    assert "fatal_or_duplicate_grid_absent" in stability


def test_all_embedded_remote_sources_compile() -> None:
    sources = (
        _runtime_probe_source(),
        _prepare_stage_source(),
        _stage_validation_source(),
        _quiescence_probe_source(),
        _atomic_deploy_source(),
        _atomic_restore_source(),
        _log_checkpoint_source(),
        _startup_stability_source(),
    )
    for index, source in enumerate(sources):
        compile(source, f"<remote-source-{index}>", "exec")
