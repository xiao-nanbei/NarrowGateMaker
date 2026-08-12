from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from scripts.orchestrate_prospective_lifecycle_remote_release import (
    BASELINE_PROCESS_RESOURCE,
    BASELINE_QUOTE_LOOP_TELEMETRY,
    BASELINE_QUOTE_LOOP_TELEMETRY_SHA256,
    DEFAULT_REMOTE_ROOT,
    DEFAULT_ROLLBACK_STABILITY_WINDOW_S,
    DEPLOYMENT_BINDING_SCHEMA_VERSION,
    INITIAL_STATE_DOMAINS,
    KNOWN_ACTIVE_PACKAGE_VERSIONS,
    KNOWN_ACTIVE_PYTHON_PREFIX,
    KNOWN_NATIVE_EXTENSION_PATH,
    KNOWN_NATIVE_EXTENSION_SHA256,
    KNOWN_SUCCESSOR_PACKAGE_VERSIONS,
    PERFORMANCE_LIMITS,
    REQUIRED_GATES,
    REQUIRED_PREDECESSOR_STARTUP_CONTRACT,
    ROLLBACK_CANONICAL_BUCKET_S,
    ROLLBACK_MINIMUM_STABLE_BUCKETS,
    _atomic_deploy_source,
    _atomic_rollback_source,
    _deployment_binding_fields,
    _performance_collection_source,
    _rollback_startup_stability_probe_source,
    _runtime_code_files,
    _runtime_probe_source,
    _seal_receipt,
    _validate_evidence_receipt_chain,
    _validate_receipt_identity,
    _validate_strict_rollback_result,
    admit_evidence_atomically,
    build_plan,
    evaluate_runtime_gates,
    execute_deploy_restart_transaction,
    execute_read_only_runtime_probe,
    execute_rollback_drill_transaction,
    load_bound_release,
    main,
    normalize_runtime_receipt_for_plan,
    owner_confirmation_token,
)

ROOT = Path(__file__).resolve().parents[1]
RELEASE = Path(tempfile.gettempdir()) / (
    "narrowgate_prospective_lifecycle_narrow_release_v1_20260805_attempt6/"
    "release_manifest.json"
)
IDENTITY = ROOT / (
    "research/families/f10_live_replay_attribution/docs/"
    "operational_baseline_identity_20260804_v9.json"
)


def test_runtime_probe_excludes_its_own_python_c_command() -> None:
    source = _runtime_probe_source()
    assert "if int(item.name) == os.getpid():" in source
    assert source.index("if int(item.name) == os.getpid():") < source.index(
        '(item / "cmdline").read_bytes()'
    )
    assert 'name.startswith("narrowgate_cpp")' in source
    assert '"narrowgate" in line.split()[-1].lower()' not in source


@pytest.fixture(scope="module")
def bound():
    return load_bound_release(RELEASE, IDENTITY)


def _full_evidence() -> dict:
    return {
        "runtime": {
            "python_version": "3.12.13",
            "remote_python_prefix_bound": True,
            "pyarrow_version": "24.0.0",
            "loaded_native_extensions": [{"path": "/remote/narrowgate_cpp.so", "sha256": "a" * 64}],
            "native_extensions_hash_valid": True,
        },
        "staging": {
            "staged_overlay_import_smoke_passed": True,
            "targeted_lifecycle_tests_passed_on_remote_venv": True,
            "maker_thread_filesystem_calls_zero": True,
        },
        "epoch": {
            "initial_state_domains": list(INITIAL_STATE_DOMAINS),
            "initial_state_domain_complete": {name: True for name in INITIAL_STATE_DOMAINS},
            "pre_epoch_native_events": 0,
        },
        "performance": {
            "baseline": dict(BASELINE_QUOTE_LOOP_TELEMETRY),
            "baseline_process_resource": dict(BASELINE_PROCESS_RESOURCE),
            "candidate": {
                "collection_duration_s": 3600.0,
                "drop_count": 0,
                "error_count": 0,
                "producer_enqueue_p99_us": 100.0,
                "producer_enqueue_max_us": 1000.0,
                "requote_total_us_p99": BASELINE_QUOTE_LOOP_TELEMETRY["requote_total_us_p99"]
                * 1.05,
                "writer_queue_hwm": 2048,
                "writer_cpu_pct_one_core": 10.0,
                "process_cpu_pct_one_core": 15.0,
                "process_rss_kib": BASELINE_PROCESS_RESOURCE["rss_kib"] + 256 * 1024,
                "writer_write_p99_ms": 250.0,
                "maker_thread_filesystem_calls": 0,
            },
        },
        "admission": {"bounded_spool_admission_roundtrip_passed": True},
        "rollback": {"rollback_restart_rehearsed": True},
    }


def _full_receipts(bound, *, plan=None) -> dict[str, dict]:
    active_plan = build_plan(bound=bound) if plan is None else plan
    binding_fields = _deployment_binding_fields(plan=active_plan, bound=bound)
    evidence = _full_evidence()
    staging = _seal_receipt(
        {
            "schema_version": "prospective_lifecycle_remote_release_evidence.v1",
            "stage": "staging",
            "release_manifest_sha256": bound["release_manifest_sha256"],
            "remote_identity_sha256": bound["remote_identity_sha256"],
            **binding_fields,
            "evidence": evidence["staging"],
        }
    )
    runtime = _seal_receipt(
        {
            "schema_version": "prospective_lifecycle_remote_release_evidence.v1",
            "stage": "runtime",
            "release_manifest_sha256": bound["release_manifest_sha256"],
            "remote_identity_sha256": bound["remote_identity_sha256"],
            **binding_fields,
            "parent_staging_receipt_identity_sha256": staging["receipt_identity_sha256"],
            "evidence": evidence["runtime"],
        }
    )
    normalization = {
        "schema_version": DEPLOYMENT_BINDING_SCHEMA_VERSION,
        "deployment_binding": binding_fields["deployment_binding"],
        "deployment_binding_sha256": binding_fields["deployment_binding_sha256"],
        "mutation_plan_identity_sha256": binding_fields["mutation_plan_identity_sha256"],
        "deployment_instance_id": binding_fields["deployment_instance_id"],
        "runtime_receipt_identity_sha256": runtime["receipt_identity_sha256"],
        "legacy_runtime_receipt_normalized_read_only": False,
    }
    performance = _seal_receipt(
        {
            "schema_version": "prospective_lifecycle_remote_release_evidence.v1",
            "stage": "performance",
            "release_manifest_sha256": bound["release_manifest_sha256"],
            "remote_identity_sha256": bound["remote_identity_sha256"],
            **binding_fields,
            "parent_runtime_receipt_identity_sha256": runtime["receipt_identity_sha256"],
            "runtime_receipt_normalization": normalization,
            "evidence": evidence["performance"],
        }
    )
    epoch = _seal_receipt(
        {
            "schema_version": "prospective_lifecycle_remote_release_evidence.v1",
            "stage": "epoch",
            "release_manifest_sha256": bound["release_manifest_sha256"],
            "remote_identity_sha256": bound["remote_identity_sha256"],
            **binding_fields,
            "parent_runtime_receipt_identity_sha256": runtime["receipt_identity_sha256"],
            "evidence": evidence["epoch"],
        }
    )
    admission = _seal_receipt(
        {
            "schema_version": "prospective_lifecycle_remote_release_evidence.v1",
            "stage": "admission",
            "release_manifest_sha256": bound["release_manifest_sha256"],
            "remote_identity_sha256": bound["remote_identity_sha256"],
            **binding_fields,
            "parent_performance_receipt_identity_sha256": performance["receipt_identity_sha256"],
            "evidence": evidence["admission"],
        }
    )
    rollback = _seal_receipt(
        {
            "schema_version": "prospective_lifecycle_remote_release_evidence.v1",
            "stage": "rollback",
            "release_manifest_sha256": bound["release_manifest_sha256"],
            "remote_identity_sha256": bound["remote_identity_sha256"],
            **binding_fields,
            "parent_runtime_receipt_identity_sha256": runtime["receipt_identity_sha256"],
            "runtime_receipt_normalization": normalization,
            "evidence": evidence["rollback"],
        }
    )
    return {
        "runtime": runtime,
        "staging": staging,
        "epoch": epoch,
        "performance": performance,
        "admission": admission,
        "rollback": rollback,
    }


def test_default_plan_is_no_ssh_no_mutation_and_preserves_strategy(bound) -> None:
    plan = build_plan(bound=bound)

    assert plan["mode"] == "dry_run_no_ssh_no_mutation"
    assert plan["execution_performed"] is False
    assert plan["deployment_authorized"] is False
    assert plan["deployment_executed"] is False
    assert plan["make_deploy_allowed"] is False
    assert plan["strategy_parameters_changed"] is False
    assert plan["remote_deployment_scope"] == (
        "v8_runtime_with_config_only_buy_fill_selection_shadow_retirement"
    )
    assert plan["frozen_strategy_flags"] == {
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_shadow_enabled": False,
        "buy_fill_selection_live_enabled": False,
    }
    assert plan["required_gates"] == list(REQUIRED_GATES)
    assert plan["source_payload_current"] is True
    assert plan["source_payload_drift"] == {}
    assert plan["deployment_blockers"] == []
    assert (
        plan["stages"]["rebuild-validate"]["result_must_be_reselected_as_exact_manifest_input"]
        is True
    )
    rendered = json.dumps(plan)
    assert '"deployment_scope": "v8_runtime' not in rendered
    assert "make deploy" in rendered
    assert "make deploy &&" not in rendered
    assert plan["stages"]["runtime-evidence"]["remote_mutation"] is False
    assert plan["stages"]["stage-validate"]["production_mutation"] is False
    assert plan["active_venv_mutation_allowed"] is False
    assert plan["active_runtime_pyarrow_observed"] == "21.0.0"
    assert plan["required_successor_pyarrow_version"] == "24.0.0"
    assert plan["pyarrow_version_mismatch"] is True
    assert plan["isolated_successor_venv_required"] is True
    assert plan["stages"]["stage-validate"]["successor_venv_ready_to_build"] is False
    assert plan["isolated_stage_root"].endswith(
        "/.releases/prospective_lifecycle_journal_v2_20260805_attempt6"
    )
    assert plan["isolated_stage_manifest_file_sha256_fully_bound"] is False
    assert plan["isolated_pyarrow24_import_smoke_observed"] is True
    assert plan["baseline_quote_loop_telemetry_sha256"] == (BASELINE_QUOTE_LOOP_TELEMETRY_SHA256)


def test_owner_tokens_are_stage_specific_and_hash_bound(bound) -> None:
    plan = build_plan(bound=bound)
    mutation_identity = plan["mutation_plan_identity_sha256"]
    deploy = owner_confirmation_token("deploy-restart", bound, mutation_identity)
    rollback = owner_confirmation_token("rollback-drill", bound, mutation_identity)

    assert deploy.startswith("OWNER_CONFIRMED_DEPLOY_RESTART:")
    assert rollback.startswith("OWNER_CONFIRMED_ROLLBACK_DRILL:")
    assert deploy != rollback


def test_owner_token_changes_with_remote_or_stage_identity(bound) -> None:
    baseline = build_plan(bound=bound)
    different_remote = build_plan(
        bound=bound,
        remote="ec2-user" + "@203.0.113.10",
    )
    different_stage = build_plan(
        bound=bound,
        isolated_release_root=(
            f"{DEFAULT_REMOTE_ROOT}/.releases/prospective_lifecycle_journal_v2_other"
        ),
    )
    tokens = {
        owner_confirmation_token("deploy-restart", bound, plan["mutation_plan_identity_sha256"])
        for plan in (baseline, different_remote, different_stage)
    }
    assert len(tokens) == 3


def test_deployment_instance_id_gives_redeploy_a_distinct_backup(bound) -> None:
    baseline = build_plan(bound=bound)
    redeploy = build_plan(bound=bound, deployment_instance_id="post-drill-attempt5")

    assert "deployment_instance_id" not in baseline
    assert redeploy["deployment_instance_id"] == "post-drill-attempt5"
    assert redeploy["backup_root"] == (baseline["backup_root"] + "-post-drill-attempt5")
    assert redeploy["mutation_plan_identity_sha256"] != baseline["mutation_plan_identity_sha256"]


@pytest.mark.parametrize("value", ["", "../escape", "has space", "x" * 65])
def test_deployment_instance_id_rejects_unsafe_values(bound, value: str) -> None:
    with pytest.raises(ValueError, match="deployment_instance_id"):
        build_plan(bound=bound, deployment_instance_id=value)


@pytest.mark.parametrize(
    "stage",
    [
        DEFAULT_REMOTE_ROOT,
        f"{DEFAULT_REMOTE_ROOT}/.releases",
    ],
)
def test_stage_root_must_be_strict_child_of_releases(bound, stage: str) -> None:
    with pytest.raises(ValueError, match="must be a child"):
        build_plan(bound=bound, isolated_release_root=stage)


def test_remote_root_rejects_string_prefix_sibling(bound) -> None:
    with pytest.raises(ValueError, match="escaped the frozen remote repository root"):
        build_plan(
            bound=bound,
            remote_root=f"{DEFAULT_REMOTE_ROOT}-escape",
        )


def test_receipt_identity_rejects_body_tampering() -> None:
    receipt = _seal_receipt({"stage": "runtime", "evidence": {"rows": 1}})
    _validate_receipt_identity(receipt, "runtime")
    receipt["evidence"]["rows"] = 2
    with pytest.raises(ValueError, match="identity SHA256 mismatch"):
        _validate_receipt_identity(receipt, "runtime")


def test_tampered_release_manifest_fails_closed(tmp_path: Path) -> None:
    candidate = tmp_path / "release"
    shutil.copytree(RELEASE.parent, candidate)
    manifest = candidate / "release_manifest.json"
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["files"][0]["sha256"] = "0" * 64
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="canonical hash mismatch"):
        load_bound_release(manifest, IDENTITY)


def test_wrong_remote_identity_fails_closed(tmp_path: Path) -> None:
    identity = tmp_path / "identity.json"
    payload = json.loads(IDENTITY.read_text(encoding="utf-8"))
    payload["baseline_id"] = "wrong"
    identity.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="identity hash does not match release"):
        load_bound_release(RELEASE, identity)


def test_runtime_probe_uses_read_only_ssh_and_validates_exact_versions(bound) -> None:
    plan = build_plan(bound=bound)
    runtime_files, _ = _runtime_code_files(bound["remote_identity"])
    runtime_files.update(
        {
            logical: row["sha256"]
            for logical, row in bound["release_manifest"]["predecessors"].items()
        }
    )
    payload = {
        "schema_version": "prospective_lifecycle_remote_runtime_probe.v1",
        "python_version": "3.12.13",
        "python_prefix": KNOWN_ACTIVE_PYTHON_PREFIX,
        "pyarrow_version": "24.0.0",
        "maker_pid": 123,
        "runtime_files": runtime_files,
        "loaded_native_extensions": [
            {"path": KNOWN_NATIVE_EXTENSION_PATH, "sha256": KNOWN_NATIVE_EXTENSION_SHA256}
        ],
        "package_versions": KNOWN_ACTIVE_PACKAGE_VERSIONS,
    }
    calls: list[list[str]] = []

    def runner(command):
        calls.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(payload), stderr="")

    receipt = execute_read_only_runtime_probe(plan=plan, bound=bound, runner=runner)

    assert calls and calls[0][0] == "ssh"
    assert receipt["ssh_mutation_performed"] is False
    assert receipt["evidence"]["remote_python_3_12_13_verified"] is True
    assert receipt["evidence"]["remote_pyarrow_24_0_0_verified"] is True
    assert receipt["evidence"]["remote_package_set_matches_expected"] is True
    assert receipt["evidence"]["pyarrow_version_mismatch"] is False
    assert receipt["evidence"]["observed_predecessor_pid"] == 123
    assert receipt["evidence"]["predecessor_pid_matches_frozen_identity"] is False
    assert receipt["evidence"]["prospective_process_epoch_required"] is True


def test_runtime_probe_reports_active_pyarrow_mismatch_without_passing_it(bound) -> None:
    plan = build_plan(bound=bound)
    runtime_files, _ = _runtime_code_files(bound["remote_identity"])
    runtime_files.update(
        {
            logical: row["sha256"]
            for logical, row in bound["release_manifest"]["predecessors"].items()
        }
    )

    def runner(command):
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(
                {
                    "python_version": "3.12.13",
                    "python_prefix": KNOWN_ACTIVE_PYTHON_PREFIX,
                    "pyarrow_version": "21.0.0",
                    "maker_pid": 123,
                    "runtime_files": runtime_files,
                    "loaded_native_extensions": [
                        {
                            "path": KNOWN_NATIVE_EXTENSION_PATH,
                            "sha256": KNOWN_NATIVE_EXTENSION_SHA256,
                        }
                    ],
                    "package_versions": KNOWN_ACTIVE_PACKAGE_VERSIONS,
                }
            ),
            stderr="",
        )

    receipt = execute_read_only_runtime_probe(plan=plan, bound=bound, runner=runner)
    assert receipt["evidence"]["remote_pyarrow_24_0_0_verified"] is False
    assert receipt["evidence"]["pyarrow_version_mismatch"] is True
    assert receipt["evidence"]["active_runtime_pyarrow_version"] == "21.0.0"
    assert receipt["evidence"]["required_successor_pyarrow_version"] == "24.0.0"
    assert receipt["evidence"]["successor_venv_required"] is True
    assert receipt["evidence"]["active_venv_mutation_allowed"] is False
    assert receipt["evidence"]["runtime_evidence_passed"] is False


def test_successor_venv_requires_hash_locked_pyarrow24_inputs(tmp_path: Path, bound) -> None:
    wheelhouse = tmp_path / "wheelhouse"
    wheelhouse.mkdir()
    (wheelhouse / "pyarrow-24.0.0-cp312-test.whl").write_bytes(b"wheel")
    bad_lock = tmp_path / "bad.lock"
    bad_lock.write_text("pyarrow==24.0.0\n", encoding="utf-8")
    with pytest.raises(ValueError, match="only hash-bound pyarrow"):
        build_plan(
            bound=bound,
            successor_requirements_lock=bad_lock,
            successor_wheelhouse=wheelhouse,
        )

    good_lock = tmp_path / "good.lock"
    good_lock.write_text("pyarrow==24.0.0 --hash=sha256:" + "a" * 64 + "\n", encoding="utf-8")
    plan = build_plan(
        bound=bound,
        successor_requirements_lock=good_lock,
        successor_wheelhouse=wheelhouse,
    )
    stage = plan["stages"]["stage-validate"]
    assert stage["successor_venv_ready_to_build"] is True
    assert stage["active_venv_mutated"] is False
    rendered = json.dumps(stage)
    assert ".venv-successor" in rendered
    assert "--require-hashes" in rendered
    smoke_command = stage["commands"][-2][-1]
    assert "cd /tmp && env -u PYTHONPATH" in smoke_command
    assert "/bin/python -I -c" in smoke_command
    assert "project_path_index=next" in smoke_command
    assert "sys.path[project_path_index:project_path_index]" in smoke_command
    assert "load_composite_package" in smoke_command
    for package_name in ("execution", "live", "models", "strategy"):
        assert package_name in smoke_command
    assert "strategy/maker_engine.py" in smoke_command
    assert "submodule_search_locations=package_paths" in smoke_command
    assert "PYTHONPATH=" not in smoke_command


def test_all_machine_gates_pass_exact_boundaries() -> None:
    result = evaluate_runtime_gates(_full_evidence())

    assert result["all_runtime_gates_passed"] is True
    assert result["missing_or_failed_gates"] == []
    assert set(result["gates"]) == set(REQUIRED_GATES)
    assert result["deployment_research_authority_granted"] is False


@pytest.mark.parametrize(
    ("field", "value", "gate"),
    [
        ("producer_enqueue_p99_us", 100.001, "producer_enqueue_p99_le_100us"),
        ("producer_enqueue_max_us", 1000.001, "producer_enqueue_max_le_1000us"),
        (
            "requote_total_us_p99",
            BASELINE_QUOTE_LOOP_TELEMETRY["requote_total_us_p99"] * 1.05001,
            "quote_loop_p99_regression_le_5pct",
        ),
        ("writer_queue_hwm", 2049, "writer_queue_hwm_le_2048"),
        ("writer_cpu_pct_one_core", 10.001, "writer_cpu_le_10pct_one_core"),
        (
            "process_rss_kib",
            BASELINE_PROCESS_RESOURCE["rss_kib"] + 256 * 1024 + 1,
            "writer_rss_delta_le_256mib",
        ),
        ("writer_write_p99_ms", 250.001, "writer_write_p99_le_250ms"),
    ],
)
def test_performance_gate_fails_above_frozen_limit(field, value, gate) -> None:
    evidence = _full_evidence()
    evidence["performance"]["candidate"][field] = value
    result = evaluate_runtime_gates(evidence)

    assert result["gates"][gate] is False
    assert result["all_runtime_gates_passed"] is False


def test_initial_state_requires_exact_thirteen_domains() -> None:
    evidence = _full_evidence()
    evidence["epoch"]["initial_state_domains"].pop()
    result = evaluate_runtime_gates(evidence)

    assert result["gates"]["initial_state_13_domain_completeness_passed"] is False


def test_maker_thread_filesystem_gate_comes_from_targeted_runtime_hook_test() -> None:
    evidence = _full_evidence()
    evidence["staging"]["maker_thread_filesystem_calls_zero"] = False
    result = evaluate_runtime_gates(evidence)
    assert result["gates"]["maker_thread_filesystem_calls_zero"] is False


def test_quote_loop_regression_is_recomputed_from_bound_baseline() -> None:
    evidence = _full_evidence()
    result = evaluate_runtime_gates(evidence)
    assert result["baseline_quote_loop_telemetry_bound"] is True
    assert result["quote_loop_p99_regression_pct_computed"] == pytest.approx(5.0)
    assert result["gates"]["quote_loop_p99_regression_le_5pct"] is True

    evidence["performance"]["candidate"]["requote_total_us_p99"] = (
        BASELINE_QUOTE_LOOP_TELEMETRY["requote_total_us_p99"] * 1.05001
    )
    result = evaluate_runtime_gates(evidence)
    assert result["gates"]["quote_loop_p99_regression_le_5pct"] is False


def test_resource_delta_uses_bound_process_baseline_and_reports_cpu_scopes() -> None:
    result = evaluate_runtime_gates(_full_evidence())
    assert result["baseline_process_resource_bound"] is True
    assert result["process_rss_delta_mib_computed"] == pytest.approx(256.0)
    assert result["gates"]["writer_rss_delta_le_256mib"] is True
    assert result["writer_cpu_pct_one_core"] == 10.0
    assert result["process_cpu_pct_one_core"] == 15.0
    assert result["cpu_scope_note"] == "writer_thread_gate_process_overall_diagnostic"

    evidence = _full_evidence()
    evidence["performance"]["baseline"]["requote_rows"] = 600
    result = evaluate_runtime_gates(evidence)
    assert result["baseline_quote_loop_telemetry_bound"] is False
    assert result["gates"]["quote_loop_p99_regression_le_5pct"] is False


def test_atomic_admission_requires_all_evidence_and_binds_hashes(tmp_path: Path, bound) -> None:
    receipts = _full_receipts(bound)
    paths = []
    for stage, receipt in receipts.items():
        path = tmp_path / f"{stage}.input.json"
        path.write_text(json.dumps(receipt), encoding="utf-8")
        paths.append(path)

    output = tmp_path / "admitted"
    manifest_path = admit_evidence_atomically(
        bound=bound,
        evidence_paths=paths,
        admission_root=output,
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["atomic_admission"] is True
    assert manifest["gate_result"]["all_runtime_gates_passed"] is True
    assert manifest["q90_action_enabled"] is False
    assert manifest["buy_fill_selection_enabled"] is False
    assert manifest["deployment_binding_sha256"] == receipts["runtime"]["deployment_binding_sha256"]
    assert not list(tmp_path.glob(".admitted.partial-*"))
    with pytest.raises(FileExistsError):
        admit_evidence_atomically(
            bound=bound,
            evidence_paths=paths,
            admission_root=output,
        )


def test_evidence_chain_rejects_cross_instance_receipt_mix(bound) -> None:
    baseline = _full_receipts(bound)
    redeploy_plan = build_plan(bound=bound, deployment_instance_id="post-drill-attempt5")
    redeploy = _full_receipts(bound, plan=redeploy_plan)
    mixed = dict(baseline)
    mixed["performance"] = redeploy["performance"]

    with pytest.raises(ValueError, match="mixes deployment instances"):
        _validate_evidence_receipt_chain(mixed, bound=bound)


def test_atomic_admission_rejects_symlink_receipt(tmp_path: Path, bound) -> None:
    receipts = _full_receipts(bound)
    paths = []
    for stage, receipt in receipts.items():
        path = tmp_path / f"{stage}.json"
        path.write_text(json.dumps(receipt), encoding="utf-8")
        paths.append(path)
    link = tmp_path / "runtime-link.json"
    link.symlink_to(paths[0])
    paths[0] = link

    with pytest.raises(ValueError, match="must be a regular non-symlink"):
        admit_evidence_atomically(
            bound=bound,
            evidence_paths=paths,
            admission_root=tmp_path / "admitted",
        )


def test_cli_defaults_to_plan_without_command_execution(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("default plan attempted command execution")

    monkeypatch.setattr(subprocess, "run", forbidden)
    assert main(["--release-manifest", str(RELEASE), "--remote-v9-identity", str(IDENTITY)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "dry_run_no_ssh_no_mutation"


def test_production_stages_require_exact_owner_confirmation(bound) -> None:
    plan = build_plan(bound=bound)
    assert plan["stages"]["deploy-restart"]["predecessor_revalidation"][0] == "ssh"
    assert plan["stages"]["rollback-drill"]["predecessor_revalidation_required"] is True
    with pytest.raises(PermissionError, match="requires --execute-production-mutation"):
        main(
            [
                "deploy-restart",
                "--release-manifest",
                str(RELEASE),
                "--remote-v9-identity",
                str(IDENTITY),
            ]
        )
    with pytest.raises(PermissionError, match="owner confirmation token mismatch"):
        main(
            [
                "deploy-restart",
                "--release-manifest",
                str(RELEASE),
                "--remote-v9-identity",
                str(IDENTITY),
                "--execute-production-mutation",
                "--owner-confirmation-token",
                "wrong",
            ]
        )


def test_rollback_drill_rejects_unsafe_predecessor_before_any_runner_call(
    monkeypatch: pytest.MonkeyPatch,
    bound,
) -> None:
    plan = build_plan(bound=bound)
    token = owner_confirmation_token(
        "rollback-drill",
        bound,
        plan["mutation_plan_identity_sha256"],
    )
    calls = []

    def forbidden_runner(command):
        calls.append(command)
        raise AssertionError("unsafe predecessor reached a command runner")

    monkeypatch.setattr(
        "scripts.orchestrate_prospective_lifecycle_remote_release._default_runner",
        forbidden_runner,
    )
    with pytest.raises(
        PermissionError,
        match="startup_contract=warmup_before_websocket.v1",
    ):
        main(
            [
                "rollback-drill",
                "--release-manifest",
                str(RELEASE),
                "--remote-v9-identity",
                str(IDENTITY),
                "--execute-production-mutation",
                "--owner-confirmation-token",
                token,
            ]
        )
    assert calls == []


def test_rollback_stability_window_is_frozen_and_covers_two_buckets(bound) -> None:
    plan = build_plan(bound=bound)
    contract = plan["stages"]["rollback-drill"]["startup_stability_contract"]

    assert contract["stability_window_s"] == DEFAULT_ROLLBACK_STABILITY_WINDOW_S
    assert contract["canonical_bucket_s"] == ROLLBACK_CANONICAL_BUCKET_S
    assert contract["minimum_stable_buckets"] == ROLLBACK_MINIMUM_STABLE_BUCKETS
    assert contract["predecessor_startup_contract_bound"] is False
    assert contract == plan["mutation_plan"]["rollback_startup_stability_contract"]
    assert plan["stages"]["rollback-drill"]["mutation_blocked_before_stop"] is True

    with pytest.raises(ValueError, match="two canonical 10s buckets"):
        build_plan(bound=bound, rollback_stability_window_s=19.999)

    longer = build_plan(bound=bound, rollback_stability_window_s=31.0)
    assert longer["mutation_plan_identity_sha256"] != plan["mutation_plan_identity_sha256"]
    assert owner_confirmation_token(
        "rollback-drill",
        bound,
        longer["mutation_plan_identity_sha256"],
    ) != owner_confirmation_token(
        "rollback-drill",
        bound,
        plan["mutation_plan_identity_sha256"],
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_sha256(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode()
    ).hexdigest()


def test_embedded_deploy_and_rollback_round_trip_is_hash_bound(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    stage = root / ".releases" / "attempt"
    backup = root / "deploy_backups" / "attempt"
    predecessor_venv = root / ".venv-py312"
    successor_venv = stage / ".venv-successor"
    for directory in (
        root / "live",
        root / "strategy",
        stage / "live",
        stage / "strategy",
        predecessor_venv,
        successor_venv / "bin",
    ):
        directory.mkdir(parents=True, exist_ok=True)
    (root / ".venv-active").symlink_to(predecessor_venv)

    baseline_config = {
        "strategy": {
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_shadow_enabled": False,
            "buy_fill_selection_live_enabled": False,
        }
    }
    candidate_config = {
        **baseline_config,
        "lifecycle_journal_v2": {
            "enabled": True,
            "remote_spool_allowlisted_roots": [str(root / "formal_collection")],
        },
    }
    import yaml

    (root / "live/config.yaml").write_text(
        yaml.safe_dump(baseline_config, sort_keys=True), encoding="utf-8"
    )
    (stage / "live/config.yaml").write_text(
        yaml.safe_dump(candidate_config, sort_keys=True), encoding="utf-8"
    )
    (root / "strategy/value.txt").write_text("predecessor\n", encoding="utf-8")
    (stage / "strategy/value.txt").write_text("successor\n", encoding="utf-8")
    (stage / "new_file.txt").write_text("new\n", encoding="utf-8")
    records = [
        {
            "path": logical,
            "sha256": _sha256(stage / logical),
        }
        for logical in ("live/config.yaml", "strategy/value.txt", "new_file.txt")
    ]
    predecessors = {
        logical: _sha256(root / logical) for logical in ("live/config.yaml", "strategy/value.txt")
    }
    release = {"files": records, "release_id": "test"}
    release["manifest_sha256"] = _canonical_sha256(release)
    (stage / "release_manifest.json").write_text(
        json.dumps(release, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )

    packages = {"pyarrow": "24.0.0"}
    native_sha = "a" * 64
    successor_probe = {
        "python": "3.12.13",
        "packages": packages,
        "native_count": 1,
        "native_sha256": native_sha,
    }
    successor_python = successor_venv / "bin/python"
    successor_python.write_text(
        "#!/bin/sh\nprintf '%s\\n' " + repr(json.dumps(successor_probe, sort_keys=True)) + "\n",
        encoding="utf-8",
    )
    successor_python.chmod(0o755)
    deploy = subprocess.run(
        [
            sys.executable,
            "-c",
            _atomic_deploy_source(),
            str(root),
            str(stage),
            str(backup),
            str(successor_venv),
            json.dumps(records, sort_keys=True),
            json.dumps(predecessors, sort_keys=True),
            json.dumps(["new_file.txt"]),
            release["manifest_sha256"],
            _sha256(stage / "release_manifest.json"),
            json.dumps(packages, sort_keys=True),
            native_sha,
            str(predecessor_venv),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    deploy_receipt = json.loads(deploy.stdout)
    assert deploy_receipt["deployment_files_applied"] is True
    assert deploy_receipt["spool_allowlist_root_provisioned"] is True
    assert (root / "formal_collection").is_dir()
    assert (root / "strategy/value.txt").read_text() == "successor\n"
    assert (root / "new_file.txt").read_text() == "new\n"
    assert os.readlink(root / ".venv-active") == str(successor_venv)

    backup_manifest = json.loads((backup / "backup_manifest.json").read_text())
    claimed = backup_manifest.pop("manifest_sha256")
    assert claimed == _canonical_sha256(backup_manifest)
    rollback = subprocess.run(
        [
            sys.executable,
            "-c",
            _atomic_rollback_source(),
            str(root),
            str(backup),
            json.dumps(records, sort_keys=True),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    rollback_receipt = json.loads(rollback.stdout)
    assert rollback_receipt["rollback_files_restored"] is True
    assert (root / "strategy/value.txt").read_text() == "predecessor\n"
    assert not (root / "new_file.txt").exists()
    assert not (root / "formal_collection").exists()
    assert os.readlink(root / ".venv-active") == str(predecessor_venv)


def _completed(command, *, stdout: str = "", returncode: int = 0):
    return subprocess.CompletedProcess(command, returncode, stdout=stdout, stderr="")


def _runtime_probe_payload(
    *,
    runtime_files: dict[str, str],
    packages: dict[str, str],
    prefix: str,
    pyarrow_version: str,
    native_path: str,
) -> dict:
    return {
        "python_version": "3.12.13",
        "python_prefix": prefix,
        "pyarrow_version": pyarrow_version,
        "maker_pid": 123,
        "runtime_files": runtime_files,
        "loaded_native_extensions": [
            {"path": native_path, "sha256": KNOWN_NATIVE_EXTENSION_SHA256}
        ],
        "package_versions": packages,
    }


def _bound_with_safe_startup_contract(bound) -> dict:
    return {
        **bound,
        "remote_identity": {
            **bound["remote_identity"],
            "startup_contract": REQUIRED_PREDECESSOR_STARTUP_CONTRACT,
        },
    }


def _rollback_stability_observation(
    *,
    pid: int = 123,
    observed_duration_s: float = DEFAULT_ROLLBACK_STABILITY_WINDOW_S,
    same_maker_process: bool = True,
    forbidden_log_hits: list[str] | None = None,
) -> dict:
    hits = [] if forbidden_log_hits is None else forbidden_log_hits
    process = {"pid": pid, "cmdline": ".venv-active/bin/python3 live/main.py --config x"}
    return {
        "schema_version": "prospective_lifecycle_rollback_startup_stability.v1",
        "expected_maker_pid": pid,
        "start_maker_processes": [process],
        "end_maker_processes": [process] if same_maker_process else [],
        "same_maker_process": same_maker_process,
        "stability_window_s": DEFAULT_ROLLBACK_STABILITY_WINDOW_S,
        "observed_duration_s": observed_duration_s,
        "canonical_bucket_s": ROLLBACK_CANONICAL_BUCKET_S,
        "minimum_stable_buckets": ROLLBACK_MINIMUM_STABLE_BUCKETS,
        "covered_canonical_buckets": observed_duration_s / ROLLBACK_CANONICAL_BUCKET_S,
        "log_exists_at_start": True,
        "log_exists_at_end": True,
        "log_identity_stable": True,
        "forbidden_log_hits": hits,
        "fatal_or_duplicate_grid_absent": not hits,
    }


def _rollback_transaction_responses(bound, plan, *, stability: dict):
    successor_files = plan["stages"]["deploy-restart"]["expected_successor_runtime_files"]
    predecessor_files, _ = _runtime_code_files(bound["remote_identity"])
    predecessor_files.update(
        {
            logical: row["sha256"]
            for logical, row in bound["release_manifest"]["predecessors"].items()
        }
    )
    successor_probe = _runtime_probe_payload(
        runtime_files=successor_files,
        packages=KNOWN_SUCCESSOR_PACKAGE_VERSIONS,
        prefix=plan["isolated_successor_venv"],
        pyarrow_version="24.0.0",
        native_path="/bound/successor/narrowgate_cpp.so",
    )
    successor_probe["maker_pid"] = 456
    predecessor_probe = _runtime_probe_payload(
        runtime_files=predecessor_files,
        packages=KNOWN_ACTIVE_PACKAGE_VERSIONS,
        prefix=f"{plan['remote_root']}/.venv-py312",
        pyarrow_version="21.0.0",
        native_path=KNOWN_NATIVE_EXTENSION_PATH,
    )
    rollback = {
        "rollback_files_restored": True,
        "rollback_not_required": False,
        "rollback_file_count": len(bound["release_manifest"]["files"]),
        "backup_manifest_identity_sha256": "a" * 64,
        "active_venv_target_restored": ".venv-py312",
    }
    responses = [
        _completed([], stdout=json.dumps(successor_probe)),
        _completed([], stdout="Stopped\n"),
        _completed([], stdout=json.dumps({"controlled_stop_quiescent": True})),
        _completed([], stdout=json.dumps(rollback)),
        _completed([], stdout="Started\n"),
        _completed([], stdout=json.dumps(predecessor_probe)),
        _completed([], stdout=json.dumps(stability)),
        _completed([], stdout=json.dumps(predecessor_probe)),
    ]
    return responses


def test_rollback_transaction_rechecks_same_pid_hashes_after_stability_window(
    bound,
) -> None:
    safe_bound = _bound_with_safe_startup_contract(bound)
    plan = build_plan(bound=safe_bound)
    responses = iter(
        _rollback_transaction_responses(
            safe_bound,
            plan,
            stability=_rollback_stability_observation(),
        )
    )
    calls = []

    def runner(command):
        calls.append(command)
        response = next(responses)
        return subprocess.CompletedProcess(
            command,
            response.returncode,
            stdout=response.stdout,
            stderr=response.stderr,
        )

    transaction = execute_rollback_drill_transaction(
        plan=plan,
        bound=safe_bound,
        runtime_evidence={"maker_pid": 456},
        deployment_evidence={
            "backup_manifest_canonical_sha256": "a" * 64,
            "active_venv_target_before": ".venv-py312",
        },
        runner=runner,
    )

    assert len(calls) == 8
    assert "time.sleep(stability_window_s)" in calls[6][-1]
    assert transaction["startup_stability_gates"] == {
        "stability_schema_bound": True,
        "stability_window_bound": True,
        "two_canonical_buckets_observed": True,
        "same_predecessor_pid_stable": True,
        "maker_log_identity_stable": True,
        "fatal_and_duplicate_grid_absent": True,
    }
    assert transaction["immediate_restored_runtime_probe"]["maker_pid"] == 123
    assert transaction["stable_restored_runtime_probe"]["maker_pid"] == 123
    assert (
        transaction["immediate_restored_runtime_probe"]["runtime_files"]
        == transaction["stable_restored_runtime_probe"]["runtime_files"]
    )


def test_rollback_stability_rejects_predecessor_that_dies_after_sixteen_seconds(
    bound,
) -> None:
    safe_bound = _bound_with_safe_startup_contract(bound)
    plan = build_plan(bound=safe_bound)
    delayed_death = _rollback_stability_observation(
        observed_duration_s=16.1,
        same_maker_process=False,
    )
    responses = iter(
        _rollback_transaction_responses(
            safe_bound,
            plan,
            stability=delayed_death,
        )
    )
    calls = []

    def runner(command):
        calls.append(command)
        response = next(responses)
        return subprocess.CompletedProcess(
            command,
            response.returncode,
            stdout=response.stdout,
            stderr=response.stderr,
        )

    with pytest.raises(RuntimeError, match="frozen startup stability window"):
        execute_rollback_drill_transaction(
            plan=plan,
            bound=safe_bound,
            runtime_evidence={"maker_pid": 456},
            deployment_evidence={
                "backup_manifest_canonical_sha256": "a" * 64,
                "active_venv_target_before": ".venv-py312",
            },
            runner=runner,
        )

    assert len(calls) == 7
    assert "time.sleep(stability_window_s)" in calls[-1][-1]


def test_rollback_stability_rejects_fatal_or_duplicate_grid_log(bound) -> None:
    safe_bound = _bound_with_safe_startup_contract(bound)
    plan = build_plan(bound=safe_bound)
    stability = _rollback_stability_observation(
        forbidden_log_hits=["completed 10s feature bucket lacks an exact causal 1s grid"]
    )
    responses = iter(
        _rollback_transaction_responses(
            safe_bound,
            plan,
            stability=stability,
        )
    )
    calls = []

    def runner(command):
        calls.append(command)
        response = next(responses)
        return subprocess.CompletedProcess(
            command,
            response.returncode,
            stdout=response.stdout,
            stderr=response.stderr,
        )

    with pytest.raises(RuntimeError, match="fatal_and_duplicate_grid_absent"):
        execute_rollback_drill_transaction(
            plan=plan,
            bound=safe_bound,
            runtime_evidence={"maker_pid": 456},
            deployment_evidence={
                "backup_manifest_canonical_sha256": "a" * 64,
                "active_venv_target_before": ".venv-py312",
            },
            runner=runner,
        )
    assert len(calls) == 7


def test_legacy_runtime_receipt_normalizes_read_only_and_rejects_redeploy_plan(
    bound,
) -> None:
    plan = build_plan(bound=bound)
    successor_files = plan["stages"]["deploy-restart"]["expected_successor_runtime_files"]
    probe = _runtime_probe_payload(
        runtime_files=successor_files,
        packages=KNOWN_SUCCESSOR_PACKAGE_VERSIONS,
        prefix=plan["isolated_successor_venv"],
        pyarrow_version="24.0.0",
        native_path="/bound/successor/narrowgate_cpp.so",
    )
    deployment = {
        "deployment_files_applied": True,
        "deployed_file_count": len(bound["release_manifest"]["files"]),
        "backup_root": plan["backup_root"],
        "successor_venv": plan["isolated_successor_venv"],
        "active_venv_target_after": plan["isolated_successor_venv"],
        "active_venv_target_before": ".venv",
        "backup_manifest_canonical_sha256": "a" * 64,
        "backup_manifest_file_sha256": "b" * 64,
    }
    token = owner_confirmation_token(
        "deploy-restart",
        bound,
        plan["mutation_plan_identity_sha256"],
    )
    receipt = _seal_receipt(
        {
            "schema_version": "prospective_lifecycle_remote_release_evidence.v1",
            "stage": "runtime",
            "release_manifest_sha256": bound["release_manifest_sha256"],
            "remote_identity_sha256": bound["remote_identity_sha256"],
            "evidence": {
                **probe,
                "deployment_files_applied": True,
                "deployment": deployment,
                "automatic_rollback_performed": False,
            },
            "production_mutation_performed": True,
            "owner_confirmation_token_sha256": hashlib.sha256(token.encode()).hexdigest(),
        }
    )

    normalized = normalize_runtime_receipt_for_plan(receipt, plan=plan, bound=bound)
    assert normalized["legacy_runtime_receipt_normalized_read_only"] is True
    assert normalized["runtime_receipt_identity_sha256"] == receipt["receipt_identity_sha256"]
    assert "deployment_binding" not in receipt

    redeploy = build_plan(bound=bound, deployment_instance_id="post-drill-attempt5")
    with pytest.raises(ValueError, match="another deployment plan"):
        normalize_runtime_receipt_for_plan(receipt, plan=redeploy, bound=bound)


def test_strict_rollback_rejects_noop_and_binds_exact_backup(bound) -> None:
    deployment = {
        "backup_manifest_canonical_sha256": "a" * 64,
        "active_venv_target_before": ".venv",
    }
    with pytest.raises(RuntimeError, match="no exact file restoration"):
        _validate_strict_rollback_result(
            {
                "rollback_files_restored": False,
                "rollback_not_required": True,
            },
            deployment_evidence=deployment,
            expected_file_count=len(bound["release_manifest"]["files"]),
        )

    _validate_strict_rollback_result(
        {
            "rollback_files_restored": True,
            "rollback_file_count": len(bound["release_manifest"]["files"]),
            "backup_manifest_identity_sha256": "a" * 64,
            "active_venv_target_restored": ".venv",
        },
        deployment_evidence=deployment,
        expected_file_count=len(bound["release_manifest"]["files"]),
    )


def test_deploy_transaction_automatically_restores_predecessor_on_probe_failure(
    bound,
) -> None:
    plan = build_plan(bound=bound)
    successor_files = plan["stages"]["deploy-restart"]["expected_successor_runtime_files"]
    predecessor_files, _ = _runtime_code_files(bound["remote_identity"])
    predecessor_files.update(
        {
            logical: row["sha256"]
            for logical, row in bound["release_manifest"]["predecessors"].items()
        }
    )
    responses = iter(
        [
            _completed([], stdout="Stopped\n"),
            _completed([], stdout=json.dumps({"controlled_stop_quiescent": True})),
            _completed([], stdout=json.dumps({"deployment_files_applied": True})),
            _completed([], stdout="Started\n"),
            _completed(
                [],
                stdout=json.dumps(
                    _runtime_probe_payload(
                        runtime_files=successor_files,
                        packages=KNOWN_ACTIVE_PACKAGE_VERSIONS,
                        prefix=plan["isolated_successor_venv"],
                        pyarrow_version="21.0.0",
                        native_path=KNOWN_NATIVE_EXTENSION_PATH,
                    )
                ),
            ),
            _completed([], stdout="Stopped\n"),
            _completed([], stdout=json.dumps({"controlled_stop_quiescent": True})),
            _completed([], stdout=json.dumps({"rollback_files_restored": True})),
            _completed([], stdout="Started\n"),
            _completed(
                [],
                stdout=json.dumps(
                    _runtime_probe_payload(
                        runtime_files=predecessor_files,
                        packages=KNOWN_ACTIVE_PACKAGE_VERSIONS,
                        prefix=f"{plan['remote_root']}/.venv-py312",
                        pyarrow_version="21.0.0",
                        native_path=KNOWN_NATIVE_EXTENSION_PATH,
                    )
                ),
            ),
        ]
    )
    calls = []

    def runner(command):
        calls.append(command)
        response = next(responses)
        return subprocess.CompletedProcess(
            command,
            response.returncode,
            stdout=response.stdout,
            stderr=response.stderr,
        )

    with pytest.raises(RuntimeError, match="predecessor was restored automatically"):
        execute_deploy_restart_transaction(plan=plan, bound=bound, runner=runner)
    assert len(calls) == 10


def test_limits_are_machine_readable_and_exact() -> None:
    assert PERFORMANCE_LIMITS == {
        "minimum_collection_duration_s": 3500.0,
        "maximum_collection_duration_s": 3700.0,
        "producer_enqueue_p99_us": 100.0,
        "producer_enqueue_max_us": 1000.0,
        "quote_loop_p99_regression_pct": 5.0,
        "writer_queue_hwm": 2048,
        "writer_cpu_pct_one_core": 10.0,
        "writer_rss_delta_mib": 256.0,
        "writer_write_p99_ms": 250.0,
    }


def test_remote_mutation_sources_parse_and_revalidate_before_mutation(bound) -> None:
    for source in (
        _atomic_deploy_source(),
        _atomic_rollback_source(),
        _performance_collection_source(),
        _rollback_startup_stability_probe_source(),
    ):
        ast.parse(source)

    deploy = _atomic_deploy_source()
    rollback = _atomic_rollback_source()
    performance = _performance_collection_source()
    stability = _rollback_startup_stability_probe_source()
    assert deploy.index("predecessor revalidation failed") < deploy.index("backup.mkdir")
    assert "candidate config changed existing v9 strategy semantics" in deploy
    assert "candidate enabled q90 action" in deploy
    assert "candidate enabled BUY selector action" in deploy
    assert "staged canonical manifest revalidation failed" in deploy
    assert "deployed successor/predecessor revalidation failed" in rollback
    assert "time.sleep(duration_s)" in performance
    assert "maker_thread_filesystem_calls" in performance
    assert performance.index("session=latest_session()") < performance.index(
        "time.sleep(duration_s)"
    )
    assert "journal session changed during the performance window" in performance
    assert "maker process identity changed during the performance window" in performance
    assert "exact_standalone_part" in performance
    assert "first_lifecycle_sequence" in performance
    assert "performance window has no independently recoverable sequence-1 part" in performance
    assert "time.sleep(stability_window_s)" in stability
    assert 'start_processes[0]["pid"] == expected_pid' in stability
    assert 'end_processes[0]["pid"] == expected_pid' in stability
    assert "fatal_or_duplicate_grid_absent" in stability

    plan = build_plan(bound=bound)
    commands = json.dumps(plan["stages"])
    assert "make deploy" not in commands.replace('"forbidden_method": "make deploy"', "")
    assert "bash live/run.sh stop" in commands
    assert "bash live/run.sh start" in commands
    assert "controlled_stop_quiescent" in commands
    assert plan["stages"]["deploy-restart"]["commands"][0][0] == "ssh"
    assert plan["stages"]["rollback-drill"]["commands"][0][0] == "ssh"
    assert plan["stages"]["performance"]["commands"][0][0] == "ssh"
