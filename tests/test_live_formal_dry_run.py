from __future__ import annotations

import io
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
import yaml

import live.main as live_main
from live.config import Config
from live.runtime_policy import (
    F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV,
    F05_BUY_E3_OWNER_OVERRIDE_ENV,
)
from strategy.model_contract import REQUIRED_MODEL_HEADS

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_CONFIG = ROOT / "live" / "formal_dry_run_public.yaml"
PUBLIC_BUNDLE = ROOT / "examples" / "public_dry_run_model_bundle"


def _summary(output: str) -> dict:
    lines = output.strip().splitlines()
    assert len(lines) == 1
    return json.loads(lines[0])


def test_formal_dry_run_exits_before_runtime_or_side_effects(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("formal dry-run entered a forbidden runtime path")

    for name in (
        "setup_logging",
        "audit_native_runtime",
        "record_startup_runtime_identity",
        "create_rest_client",
        "MakerEngine",
        "WSHandler",
        "set_engine_ref",
        "install_reload_handler",
    ):
        monkeypatch.setattr(live_main, name, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(
        sys,
        "argv",
        ["live/main.py", "--dry-run", "--config", str(PUBLIC_CONFIG)],
    )

    assert live_main.main() == 0

    result = _summary(capsys.readouterr().out)
    assert result["schema_version"] == "narrowgate.live_dry_run.v1"
    assert result["mode"] == "formal_dry_run"
    assert result["status"] == "passed"
    assert result["exit_code"] == 0
    assert result["termination"] == "completed"
    assert result["timeout_s"] == pytest.approx(30.0)
    assert result["safety"] == {
        "network_allowed": False,
        "exchange_clients_created": 0,
        "threads_started": 0,
        "order_path_entered": False,
        "orders_submitted": 0,
    }
    assert result["authority"] == {
        "scope": "local_validation_only",
        "live_trading_authorized": False,
        "remote_deploy_authorized": False,
    }
    assert result["model_contract"]["ml_enabled"] is False
    assert result["model_contract"]["required_head_count"] == len(
        REQUIRED_MODEL_HEADS
    )
    assert result["model_contract"]["validated_heads"] == sorted(
        REQUIRED_MODEL_HEADS
    )


def test_formal_dry_run_uses_strict_config_validation(tmp_path: Path) -> None:
    config_path = tmp_path / "invalid.yaml"
    config_path.write_text(
        "strategy:\n  removed_simulation_field: 1\n",
        encoding="utf-8",
    )
    output = io.StringIO()

    exit_code = live_main.run_formal_dry_run(config_path, output=output)

    result = _summary(output.getvalue())
    assert exit_code == 1
    assert result["status"] == "failed"
    assert result["exit_code"] == 1
    assert result["error"]["type"] == "ValueError"
    assert "unknown config key" in result["error"]["message"]


@pytest.mark.parametrize(
    ("enabled_field", "path_fields", "override_env"),
    (
        (
            "boolean_cooldown_policy_enabled",
            (
                "boolean_cooldown_policy_path",
                "boolean_cooldown_predicate_bundle_path",
            ),
            F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV,
        ),
        (
            "buy_e3_cooldown_policy_enabled",
            (
                "buy_e3_cooldown_artifact_manifest_path",
                "buy_e3_cooldown_policy_path",
                "buy_e3_cooldown_predicate_bundle_path",
            ),
            F05_BUY_E3_OWNER_OVERRIDE_ENV,
        ),
    ),
)
def test_formal_dry_run_rejects_private_live_cooldown_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    enabled_field: str,
    path_fields: tuple[str, ...],
    override_env: str,
) -> None:
    config = yaml.safe_load(PUBLIC_CONFIG.read_text(encoding="utf-8"))
    config.setdefault("strategy", {}).update(
        {
            "fill_cooldown": 85.0,
            "adaptive_add_cooldown_enabled": False,
            "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
            enabled_field: True,
            **{name: f"/private/{name}.json" for name in path_fields},
        }
    )
    config_path = tmp_path / "private-policy.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    monkeypatch.setenv(override_env, "1")
    output = io.StringIO()

    assert live_main.run_formal_dry_run(config_path, output=output) == 1
    result = _summary(output.getvalue())
    assert result["status"] == "failed"
    assert "does not admit private live cooldown policies" in result["error"][
        "message"
    ]


def test_formal_dry_run_rejects_invalid_model_contract_without_leaking_secrets(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "bundle"
    shutil.copytree(PUBLIC_BUNDLE, bundle)
    metadata_path = bundle / "dir_10s_meta.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["feature_semantics_version"] = -1
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    config = yaml.safe_load(PUBLIC_CONFIG.read_text(encoding="utf-8"))
    config["api"]["key"] = "never-print-this-key"
    config["api"]["secret"] = "never-print-this-secret"
    config["ml"]["model_dir"] = str(bundle)
    config_path = tmp_path / "invalid-model.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    output = io.StringIO()

    exit_code = live_main.run_formal_dry_run(config_path, output=output)

    serialized = output.getvalue()
    result = _summary(serialized)
    assert exit_code == 1
    assert result["status"] == "failed"
    assert "feature_semantics_version=-1" in result["error"]["message"]
    assert "never-print-this-key" not in serialized
    assert "never-print-this-secret" not in serialized


def test_formal_dry_run_rejects_synthetic_model_byte_tampering(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "bundle"
    shutil.copytree(PUBLIC_BUNDLE, bundle)
    model_path = bundle / "dir_10s.txt"
    model_path.write_bytes(model_path.read_bytes() + b"\n")
    config = yaml.safe_load(PUBLIC_CONFIG.read_text(encoding="utf-8"))
    config["ml"]["model_dir"] = str(bundle)
    config_path = tmp_path / "tampered-model.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    output = io.StringIO()

    exit_code = live_main.run_formal_dry_run(config_path, output=output)

    result = _summary(output.getvalue())
    assert exit_code == 1
    assert result["status"] == "failed"
    assert "public synthetic bundle byte count mismatch for dir_10s.txt" in result[
        "error"
    ]["message"]


def test_formal_dry_run_deadline_emits_timeout_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def slow_load(_path: Path):
        time.sleep(1.0)
        raise AssertionError("deadline did not interrupt config loading")

    monkeypatch.setattr(live_main, "load_config", slow_load)
    output = io.StringIO()
    started = time.monotonic()

    exit_code = live_main.run_formal_dry_run(
        PUBLIC_CONFIG,
        timeout_s=0.01,
        output=output,
    )

    result = _summary(output.getvalue())
    assert exit_code == live_main.DRY_RUN_TIMEOUT_EXIT_CODE
    assert time.monotonic() - started < 0.5
    assert result["status"] == "timed_out"
    assert result["exit_code"] == 124
    assert result["termination"] == "deadline_exceeded"
    assert signal.getitimer(signal.ITIMER_REAL)[0] == pytest.approx(0.0)


def test_legacy_rest_simulation_is_not_a_second_dry_run() -> None:
    with pytest.raises(ValueError, match="legacy simulated REST dry-run was removed"):
        live_main.create_rest_client(Config(), dry_run=True)


def test_run_sh_has_one_dry_run_command_and_status_is_process_only() -> None:
    script = (ROOT / "live" / "run.sh").read_text(encoding="utf-8")
    dry_run_body = script.split("dry_run() {", 1)[1].split(
        "\n# Kill a single PID", 1
    )[0]
    status_body = script.split("status() {", 1)[1].split("\nreload() {", 1)[0]

    assert "formal_dry_run_public.yaml" in script
    assert "--dry-run-timeout-s" in dry_run_body
    assert "sys.version_info >= (3, 11)" in dry_run_body
    assert "_load_runtime_environment" not in dry_run_body
    assert "_run_deploy_preflight" not in dry_run_body
    assert "--dry-run" not in status_body
    assert "dry_run" not in status_body
    assert "status)  status" in script
    assert "dry-run) dry_run" in script


def test_public_run_sh_dry_run_emits_one_success_summary() -> None:
    env = os.environ.copy()
    env.pop("NARROWGATE_LIVE_CONFIG", None)
    env.pop("NARROWGATE_DRY_RUN_TIMEOUT_S", None)
    env["PATH"] = os.pathsep.join(
        [str(Path(sys.executable).parent), env.get("PATH", "")]
    )

    completed = subprocess.run(
        ["bash", str(ROOT / "live" / "run.sh"), "dry-run"],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=10.0,
    )

    assert completed.returncode == 0, completed.stderr
    result = _summary(completed.stdout)
    assert result["status"] == "passed"
    assert result["model_contract"]["validated_head_count"] == len(
        REQUIRED_MODEL_HEADS
    )
    assert result["config"]["path"] == str(PUBLIC_CONFIG)
