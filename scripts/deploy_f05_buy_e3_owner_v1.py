"""Transactional planner for the frozen owner-selected BUY E3 runtime.

The default command only writes a deterministic plan.  Any SSH or remote
mutation requires a named phase, an explicit authorization flag, and a secret
whose SHA256 was frozen in the plan.  The planner is external to the c170493e
runtime and never changes the E3 algorithm or the v1 deployment gate.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import shlex
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from live.runtime_policy import (  # noqa: E402
    F05_BUY_E3_OWNER_OVERRIDE_ENV,
    f05_buy_e3_runtime_policy,
)

try:  # noqa: E402
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_amendment_v2 as gate_v2,
    )
except ImportError:
    external_gate = os.environ.get("NARROWGATE_BUY_E3_GATE_V2_PATH", "").strip()
    if not external_gate:
        raise
    specification = importlib.util.spec_from_file_location(
        "narrowgate_buy_e3_gate_v2", external_gate
    )
    if specification is None or specification.loader is None:
        raise ImportError("cannot load external BUY E3 deployment gate amendment") from None
    gate_v2 = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(gate_v2)


PLAN_SCHEMA = "f05_buy_e3_owner_transactional_deploy_plan.v1"
RECEIPT_SCHEMA = "f05_buy_e3_owner_transactional_deploy_receipt.v1"
PREFLIGHT_SCHEMA = "f05_buy_e3_owner_isolated_config_preflight.v1"
POINTER_SCHEMA = "narrowgate_live_remote_pointer.v1"
ACTIVE_POINTER_STATUS = "current_active"

PHASES = ("disabled-deploy", "activate", "rollback-primary", "rollback-deep")
MUTATING_PHASES = frozenset(PHASES)
REMOTE_OVERRIDE_ENV = (
    "NARROWGATE_LIVE_REMOTE",
    "NARROWGATE_LIVE_REMOTE_POINTER",
)
STRICT_SSH_OPTIONS = (
    "BatchMode=yes",
    "StrictHostKeyChecking=yes",
)


class BuyE3TransactionalDeployError(RuntimeError):
    """Raised when a deployment plan or transaction cannot fail closed."""


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]
PreflightRunner = Callable[[Path, Path, bool], Mapping[str, Any]]


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise BuyE3TransactionalDeployError(f"{label} is not a SHA256")
    return normalized


def _read_json(path: Path) -> dict[str, Any]:
    return gate_v2.read_json(path)


def _reject_remote_environment_override() -> None:
    present = [name for name in REMOTE_OVERRIDE_ENV if os.environ.get(name, "").strip()]
    if present:
        raise BuyE3TransactionalDeployError(
            "environment remote override is forbidden: " + ", ".join(present)
        )


def load_sha_bound_active_pointer(
    *,
    pointer_path: Path,
    expected_file_sha256: str,
) -> dict[str, Any]:
    """Resolve only the explicit pointer bytes; environment overrides are rejected."""

    _reject_remote_environment_override()
    path = pointer_path.expanduser()
    if path.is_symlink() or not path.is_file():
        raise BuyE3TransactionalDeployError("active pointer is not a regular file")
    path = path.resolve(strict=True)
    expected = _require_sha256(expected_file_sha256, "active pointer file hash")
    if gate_v2.file_sha256(path) != expected:
        raise BuyE3TransactionalDeployError("active pointer file hash drifted")
    payload = _read_json(path)
    if (
        payload.get("schema_version") != POINTER_SCHEMA
        or payload.get("status") != ACTIVE_POINTER_STATUS
    ):
        raise BuyE3TransactionalDeployError("active pointer identity drifted")
    required = ("ssh_target", "repo_root", "provider", "region", "public_ipv4")
    fields = {key: str(payload.get(key, "")).strip() for key in required}
    if not all(fields.values()):
        raise BuyE3TransactionalDeployError("active pointer lacks required host fields")
    return {
        "path": str(path),
        "file_sha256": expected,
        **fields,
    }


def bind_known_hosts(
    *,
    known_hosts_path: Path,
    expected_file_sha256: str,
    expected_fingerprint: str,
) -> dict[str, Any]:
    path = known_hosts_path.expanduser()
    if path.is_symlink() or not path.is_file():
        raise BuyE3TransactionalDeployError("known-hosts is not a regular file")
    path = path.resolve(strict=True)
    expected_sha = _require_sha256(expected_file_sha256, "known-hosts file hash")
    if gate_v2.file_sha256(path) != expected_sha:
        raise BuyE3TransactionalDeployError("known-hosts file hash drifted")
    fingerprints = gate_v2.ssh_host_key_fingerprints(path)
    fingerprint = str(expected_fingerprint).strip()
    if fingerprint not in fingerprints:
        raise BuyE3TransactionalDeployError("expected host-key fingerprint is absent")
    return {
        "path": str(path),
        "file_sha256": expected_sha,
        "expected_fingerprint": fingerprint,
        "observed_fingerprints": fingerprints,
    }


def _strategy_mapping(config_path: Path) -> dict[str, Any]:
    import yaml

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("strategy"), dict):
        raise BuyE3TransactionalDeployError("private config lacks strategy mapping")
    return payload["strategy"]


def isolated_config_preflight(
    repository_root: Path,
    config_path: Path,
    expected_enabled: bool,
) -> dict[str, Any]:
    """Validate one config in a short-lived process before any live stop.

    The function itself is also useful for the internal child command.  The
    parent planner invokes it twice through ``run_isolated_preflight``.
    """

    root = repository_root.expanduser().resolve(strict=True)
    config = config_path.expanduser().resolve(strict=True)
    previous = os.environ.get(F05_BUY_E3_OWNER_OVERRIDE_ENV)
    try:
        if expected_enabled:
            os.environ[F05_BUY_E3_OWNER_OVERRIDE_ENV] = "1"
        else:
            os.environ.pop(F05_BUY_E3_OWNER_OVERRIDE_ENV, None)
        from live.config import load_config
        from scripts.preflight_live_deploy import validate_deploy_config

        generic = validate_deploy_config(config, root)
        loaded = load_config(config)
        artifact = gate_v2.validate_config_artifact(
            config_path=config,
            repository_root=root,
            expected_enabled=expected_enabled,
        )
        policy = f05_buy_e3_runtime_policy(
            bool(loaded.strategy.buy_e3_cooldown_policy_enabled),
            evidence_route=loaded.strategy.buy_e3_cooldown_evidence_route,
        )
    finally:
        if previous is None:
            os.environ.pop(F05_BUY_E3_OWNER_OVERRIDE_ENV, None)
        else:
            os.environ[F05_BUY_E3_OWNER_OVERRIDE_ENV] = previous
    if bool(policy["f05_buy_e3_owner_override_effective"]) is not bool(expected_enabled):
        raise BuyE3TransactionalDeployError("isolated owner override was not exact")
    receipt: dict[str, Any] = {
        "schema_version": PREFLIGHT_SCHEMA,
        "status": "isolated_config_preflight_passed",
        "expected_enabled": bool(expected_enabled),
        "config_sha256": artifact["config_sha256"],
        "artifact_sha256": artifact["artifact_sha256"],
        "artifact_files": artifact["artifact_files"],
        "artifact_loaded_with_from_files": artifact["artifact_loaded_with_from_files"],
        "generic_preflight_sha256": gate_v2.canonical_sha256(generic),
        "owner_override_requested": policy["f05_buy_e3_owner_override_requested"],
        "owner_override_effective": policy["f05_buy_e3_owner_override_effective"],
        "validation_read": False,
        "sealed_holdout_read": False,
        "economic_values_read": False,
    }
    receipt["canonical_preflight_sha256"] = gate_v2.document_sha256(
        receipt, "canonical_preflight_sha256"
    )
    return receipt


def run_isolated_preflight(
    repository_root: Path,
    config_path: Path,
    expected_enabled: bool,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    python = repository_root / ".venv/bin/python"
    if not python.is_file() or not os.access(python, os.X_OK):
        raise BuyE3TransactionalDeployError("repository virtualenv Python is unavailable")
    command = (
        str(python),
        str(Path(__file__).resolve()),
        "isolated-preflight",
        "--repository-root",
        str(repository_root),
        "--config",
        str(config_path),
        "--expected-enabled",
        "1" if expected_enabled else "0",
    )
    environment = dict(os.environ)
    environment.pop(F05_BUY_E3_OWNER_OVERRIDE_ENV, None)
    completed = runner(
        command,
        cwd=repository_root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise BuyE3TransactionalDeployError("isolated preflight output is not JSON") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != PREFLIGHT_SCHEMA
        or payload.get("status") != "isolated_config_preflight_passed"
        or payload.get("expected_enabled") is not bool(expected_enabled)
        or payload.get("canonical_preflight_sha256")
        != gate_v2.document_sha256(payload, "canonical_preflight_sha256")
    ):
        raise BuyE3TransactionalDeployError("isolated preflight receipt drifted")
    return payload


def capture_runtime_process_probe(
    *,
    repository_root: Path,
    pid_file: Path,
    config_path: Path,
    config_sha256: str,
    python_executable: Path,
    venv_root: Path,
    runtime_identity_path: Path,
    expected_buy_e3_enabled: bool,
    expected_execution_commit: str,
    expected_execution_tree: str,
    expected_artifact_sha256: str | None,
    expected_runtime_code_sha256: str,
) -> dict[str, Any]:
    pid = int(pid_file.expanduser().resolve(strict=True).read_text(encoding="ascii").strip())
    process = gate_v2.capture_actual_process_identity(
        pid=pid,
        expected_repository_root=repository_root,
        expected_config_path=config_path,
        expected_config_sha256=config_sha256,
        expected_python_executable=python_executable,
        expected_venv_root=venv_root,
        runtime_identity_path=runtime_identity_path,
    )
    runtime = gate_v2.read_json(runtime_identity_path)
    enabled = bool(runtime.get("f05_buy_e3_enabled", False))
    effective = bool(runtime.get("f05_buy_e3_owner_override_effective", False))
    if enabled is not expected_buy_e3_enabled or effective is not expected_buy_e3_enabled:
        raise BuyE3TransactionalDeployError("actual process BUY E3 authority drifted")
    artifact_sha = (
        _require_sha256(expected_artifact_sha256, "expected artifact hash")
        if str(expected_artifact_sha256 or "").strip()
        else ""
    )
    runtime_code_sha = _require_sha256(expected_runtime_code_sha256, "expected runtime code hash")
    if artifact_sha and runtime.get("f05_buy_e3_artifact_sha256") != artifact_sha:
        raise BuyE3TransactionalDeployError("actual process artifact identity drifted")
    completed_commit = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    completed_tree = subprocess.run(
        ("git", "rev-parse", "HEAD^{tree}"),
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if completed_commit != expected_execution_commit or completed_tree != expected_execution_tree:
        raise BuyE3TransactionalDeployError("actual process checkout identity drifted")
    process.update(
        {
            "execution_commit": completed_commit,
            "execution_tree": completed_tree,
            "artifact_sha256": artifact_sha,
            "runtime_code_sha256": runtime_code_sha,
            "buy_e3_enabled": enabled,
            "owner_override_effective": effective,
            "initial_buy_deadline_identity": "B0",
            "e3_deadline_imported": False,
        }
    )
    process["canonical_process_identity_sha256"] = gate_v2.document_sha256(
        process, "canonical_process_identity_sha256"
    )
    return process


def _validate_rollback_identity(name: str, raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise BuyE3TransactionalDeployError(f"rollback identity is malformed: {name}")
    required = (
        "identity",
        "execution_commit",
        "execution_tree",
        "config_path",
        "config_sha256",
        "python_executable",
        "venv_root",
        "runtime_code_sha256",
    )
    missing = [field for field in required if not str(raw.get(field, "")).strip()]
    if missing:
        raise BuyE3TransactionalDeployError(f"rollback identity {name} lacks: {', '.join(missing)}")
    if raw.get("buy_e3_enabled") is not False:
        raise BuyE3TransactionalDeployError(f"rollback identity {name} enables BUY E3")
    if raw.get("buy_deadline_identity") != "B0":
        raise BuyE3TransactionalDeployError(f"rollback identity {name} can retain E3 deadline")
    if raw.get("imports_e3_deadline") is not False:
        raise BuyE3TransactionalDeployError(f"rollback identity {name} imports E3 state")
    normalized = dict(raw)
    for field in ("config_sha256", "runtime_code_sha256"):
        normalized[field] = _require_sha256(raw[field], f"rollback {name} {field}")
    return normalized


def _ssh_base(known_hosts: str) -> list[str]:
    return [
        "ssh",
        "-o",
        STRICT_SSH_OPTIONS[0],
        "-o",
        STRICT_SSH_OPTIONS[1],
        "-o",
        f"UserKnownHostsFile={known_hosts}",
    ]


def _ssh_command(*, target: str, known_hosts: str, remote_command: str) -> list[str]:
    return [*_ssh_base(known_hosts), "--", target, remote_command]


def _rsync_command(*, source: str, target: str, known_hosts: str, destination: str) -> list[str]:
    transport = shlex.join(_ssh_base(known_hosts))
    return [
        "rsync",
        "--archive",
        "--checksum",
        "--protect-args",
        "-e",
        transport,
        source,
        f"{target}:{destination}",
    ]


def _remote_external_config_start(repo_root: str, config_path: str, *, owner_override: bool) -> str:
    authority = (
        f"{F05_BUY_E3_OWNER_OVERRIDE_ENV}=1"
        if owner_override
        else f"-u {F05_BUY_E3_OWNER_OVERRIDE_ENV}"
    )
    return (
        f"cd {shlex.quote(repo_root)} && "
        f"env {authority} NARROWGATE_LIVE_CONFIG={shlex.quote(config_path)} "
        "bash live/run.sh start"
    )


def _remote_external_config_stop(repo_root: str, config_path: str) -> str:
    return (
        f"cd {shlex.quote(repo_root)} && "
        f"env -u {F05_BUY_E3_OWNER_OVERRIDE_ENV} "
        f"NARROWGATE_LIVE_CONFIG={shlex.quote(config_path)} bash live/run.sh stop"
    )


def _remote_preflight(
    *,
    repo_root: str,
    external_script: str,
    config_path: str,
    expected_enabled: bool,
    python: str,
    external_gate: str,
) -> str:
    override = f"{F05_BUY_E3_OWNER_OVERRIDE_ENV}=1 " if expected_enabled else ""
    return (
        f"cd {shlex.quote(repo_root)} && env PYTHONPATH={shlex.quote(repo_root)} "
        f"NARROWGATE_BUY_E3_GATE_V2_PATH={shlex.quote(external_gate)} "
        f"{override}{shlex.quote(python)} {shlex.quote(external_script)} isolated-preflight "
        f"--repository-root {shlex.quote(repo_root)} --config {shlex.quote(config_path)} "
        f"--expected-enabled {1 if expected_enabled else 0}"
    )


def _command(
    label: str, argv: Sequence[str], *, mutates: bool, after_stop: bool = False
) -> dict[str, Any]:
    return {
        "label": label,
        "argv": list(argv),
        "command_sha256": gate_v2.canonical_sha256(list(argv)),
        "mutates_remote": bool(mutates),
        "after_stop": bool(after_stop),
    }


def _phase_commands(
    *,
    pointer: Mapping[str, Any],
    known_hosts: Mapping[str, Any],
    host: Mapping[str, Any],
    configs: Mapping[str, Any],
    remote: Mapping[str, Any],
    execution: Mapping[str, Any],
    rollback: Mapping[str, Any],
    runtime_sources: Mapping[str, Any],
    artifact: Mapping[str, Any],
    local_package: Mapping[str, str],
) -> dict[str, list[dict[str, Any]]]:
    target = str(pointer["ssh_target"])
    repo_root = str(pointer["repo_root"])
    known = str(known_hosts["path"])
    python = str(host["python_executable"])
    stage = str(remote["stage_root"])
    external_script = f"{stage}/deploy_f05_buy_e3_owner_v1.py"
    external_gate = f"{stage}/deployment_gate_amendment_v2.py"
    disabled_config = str(remote["disabled_config_path"])
    active_config = str(remote["active_config_path"])
    pid_file = str(remote["pid_file"])
    checkpoint_base = str(remote["startup_checkpoint_path"])
    disabled_checkpoint = f"{checkpoint_base}.disabled"
    active_checkpoint = f"{checkpoint_base}.active"
    staged_names = {
        "deploy_script": "deploy_f05_buy_e3_owner_v1.py",
        "gate_amendment": "deployment_gate_amendment_v2.py",
        "artifact_manifest": "artifact_manifest.json",
        "policy": "policy.json",
        "predicate_bundle": "predicate_bundle.json",
        "disabled_config": "disabled.yaml",
        "active_config": "active.yaml",
    }
    prepare_stage = _command(
        "prepare-isolated-stage",
        _ssh_command(
            target=target,
            known_hosts=known,
            remote_command=(
                f"test ! -L {shlex.quote(stage)} && mkdir -p {shlex.quote(stage)} && "
                f"chmod 700 {shlex.quote(stage)}"
            ),
        ),
        mutates=True,
    )
    transfers = [
        _command(
            f"stage-{role}",
            _rsync_command(
                source=str(local_package[role]),
                target=target,
                known_hosts=known,
                destination=f"{stage}/{staged_names[role]}",
            ),
            mutates=True,
        )
        for role in staged_names
    ]
    installs = (
        ("artifact_manifest", str(remote["artifact_manifest_path"])),
        ("policy", str(remote["policy_path"])),
        ("predicate_bundle", str(remote["predicate_bundle_path"])),
        ("disabled_config", disabled_config),
        ("active_config", active_config),
    )
    install_fragments: list[str] = []
    for role, destination in installs:
        source = f"{stage}/{staged_names[role]}"
        parent = str(Path(destination).parent)
        expected_sha = gate_v2.file_sha256(Path(local_package[role]))
        install_fragments.append(
            f"test \"$(sha256sum {shlex.quote(source)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(expected_sha)} && test ! -L {shlex.quote(destination)} && "
            f"mkdir -p {shlex.quote(parent)} && "
            f"chmod 700 {shlex.quote(parent)} && "
            f"(test ! -e {shlex.quote(destination)} || "
            f"cmp -s {shlex.quote(source)} {shlex.quote(destination)}) && "
            f"install -m 600 {shlex.quote(source)} {shlex.quote(destination)} && "
            f"test \"$(sha256sum {shlex.quote(destination)} | awk '{{print $1}}')\" = "
            f"{shlex.quote(expected_sha)}"
        )
    install_bytes = _command(
        "install-private-artifact-and-config-bytes",
        _ssh_command(
            target=target,
            known_hosts=known,
            remote_command=" && ".join(install_fragments),
        ),
        mutates=True,
    )
    validate_tools = _command(
        "validate-staged-external-tools",
        _ssh_command(
            target=target,
            known_hosts=known,
            remote_command=(
                f"test \"$(sha256sum {shlex.quote(external_script)} | awk '{{print $1}}')\" = "
                f"{shlex.quote(gate_v2.file_sha256(Path(local_package['deploy_script'])))} && "
                f"test \"$(sha256sum {shlex.quote(external_gate)} | awk '{{print $1}}')\" = "
                f"{shlex.quote(gate_v2.file_sha256(Path(local_package['gate_amendment'])))}"
            ),
        ),
        mutates=False,
    )
    checkout = (
        f"cd {shlex.quote(repo_root)} && "
        f'test "$(git cat-file -t refs/tags/{shlex.quote(str(execution["annotated_tag"]))})" '
        f'= tag && test "$(git rev-parse refs/tags/{shlex.quote(str(execution["annotated_tag"]))})" '
        f"= {shlex.quote(str(execution['annotated_tag_object']))} && "
        f'test "$(git rev-parse refs/tags/{shlex.quote(str(execution["annotated_tag"]))}^{{}})" '
        f"= {shlex.quote(str(execution['execution_commit']))} && git checkout --detach "
        f"{shlex.quote(str(execution['execution_commit']))} && "
        f'test "$(git rev-parse HEAD^{{tree}})" = {shlex.quote(str(execution["execution_tree"]))}'
    )
    disabled_preflight = _remote_preflight(
        repo_root=repo_root,
        external_script=external_script,
        config_path=disabled_config,
        expected_enabled=False,
        python=python,
        external_gate=external_gate,
    )
    active_preflight = _remote_preflight(
        repo_root=repo_root,
        external_script=external_script,
        config_path=active_config,
        expected_enabled=True,
        python=python,
        external_gate=external_gate,
    )

    def common_pre_stop(checkpoint_path: str) -> list[dict[str, Any]]:
        log_checkpoint = (
            f"env NARROWGATE_BUY_E3_GATE_V2_PATH={shlex.quote(external_gate)} "
            f"PYTHONPATH={shlex.quote(repo_root)} {shlex.quote(python)} "
            f"{shlex.quote(external_script)} log-checkpoint --log "
            f"{shlex.quote(str(remote['log_path']))} --output "
            f"{shlex.quote(checkpoint_path)}"
        )
        return [
            prepare_stage,
            *transfers,
            validate_tools,
            install_bytes,
            _command(
                "isolated-disabled-preflight",
                _ssh_command(target=target, known_hosts=known, remote_command=disabled_preflight),
                mutates=False,
            ),
            _command(
                "isolated-active-preflight",
                _ssh_command(target=target, known_hosts=known, remote_command=active_preflight),
                mutates=False,
            ),
            _command(
                "capture-old-pid",
                _ssh_command(
                    target=target,
                    known_hosts=known,
                    remote_command=(
                        f"test -s {shlex.quote(pid_file)} && "
                        f"printf '%s\\n' \"$(cat {shlex.quote(pid_file)})\""
                    ),
                ),
                mutates=False,
            ),
            _command(
                "startup-log-checkpoint",
                _ssh_command(
                    target=target,
                    known_hosts=known,
                    remote_command=log_checkpoint,
                ),
                mutates=False,
            ),
        ]

    stop_disabled = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=_remote_external_config_stop(repo_root, disabled_config),
    )
    stop_active = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=_remote_external_config_stop(repo_root, active_config),
    )
    quiescent = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=("test -z \"$(pgrep -f '[l]ive/main.py' || true)\""),
    )
    checkout_command = _ssh_command(target=target, known_hosts=known, remote_command=checkout)
    start_disabled = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=_remote_external_config_start(
            repo_root, disabled_config, owner_override=False
        ),
    )
    start_active = _ssh_command(
        target=target,
        known_hosts=known,
        remote_command=_remote_external_config_start(repo_root, active_config, owner_override=True),
    )

    def process_probe(
        config_path: str,
        config_sha: str,
        enabled: bool,
        *,
        expected_execution: Mapping[str, Any] = execution,
        expected_runtime_code_sha256: str = str(runtime_sources["runtime_code_sha256"]),
        expected_artifact_sha256: str = str(artifact["artifact_sha256"]),
    ) -> list[str]:
        command = (
            f"cd {shlex.quote(repo_root)} && env "
            f"NARROWGATE_BUY_E3_GATE_V2_PATH={shlex.quote(external_gate)} "
            f"PYTHONPATH={shlex.quote(repo_root)} {shlex.quote(python)} "
            f"{shlex.quote(external_script)} process-probe --repository-root "
            f"{shlex.quote(repo_root)} --pid-file {shlex.quote(pid_file)} --config "
            f"{shlex.quote(config_path)} --config-sha256 {shlex.quote(config_sha)} "
            f"--python-executable {shlex.quote(python)} --venv-root "
            f"{shlex.quote(str(host['venv_root']))} --runtime-identity "
            f"{shlex.quote(str(remote['runtime_identity_path']))} --expected-enabled "
            f"{1 if enabled else 0} --execution-commit "
            f"{shlex.quote(str(expected_execution['execution_commit']))} --execution-tree "
            f"{shlex.quote(str(expected_execution['execution_tree']))} "
            f"--runtime-code-sha256 {shlex.quote(expected_runtime_code_sha256)}"
        )
        if expected_artifact_sha256:
            command += f" --artifact-sha256 {shlex.quote(expected_artifact_sha256)}"
        return _ssh_command(target=target, known_hosts=known, remote_command=command)

    def log_validate(checkpoint_path: str) -> list[str]:
        markers = " ".join(
            f"--marker {shlex.quote(str(marker))}" for marker in remote["startup_markers"]
        )
        command = (
            f"env NARROWGATE_BUY_E3_GATE_V2_PATH={shlex.quote(external_gate)} "
            f"PYTHONPATH={shlex.quote(repo_root)} {shlex.quote(python)} "
            f"{shlex.quote(external_script)} log-validate --log "
            f"{shlex.quote(str(remote['log_path']))} --checkpoint "
            f"{shlex.quote(checkpoint_path)} {markers}"
        )
        return _ssh_command(target=target, known_hosts=known, remote_command=command)

    disabled = [
        *common_pre_stop(disabled_checkpoint),
        _command("stop-live", stop_disabled, mutates=True, after_stop=True),
        _command("confirm-quiescent", quiescent, mutates=False, after_stop=True),
        _command("checkout-frozen-runtime", checkout_command, mutates=True, after_stop=True),
        _command("start-disabled", start_disabled, mutates=True, after_stop=True),
        _command(
            "fresh-disabled-process-probe",
            process_probe(disabled_config, str(configs["disabled"]["config_sha256"]), False),
            mutates=False,
            after_stop=True,
        ),
        _command(
            "validate-disabled-startup-log",
            log_validate(disabled_checkpoint),
            mutates=False,
            after_stop=True,
        ),
    ]
    activate = [
        *common_pre_stop(active_checkpoint),
        _command("stop-live", stop_disabled, mutates=True, after_stop=True),
        _command("confirm-quiescent", quiescent, mutates=False, after_stop=True),
        _command("start-active-restart-only", start_active, mutates=True, after_stop=True),
        _command(
            "fresh-active-process-probe",
            process_probe(active_config, str(configs["active"]["config_sha256"]), True),
            mutates=False,
            after_stop=True,
        ),
        _command(
            "validate-active-startup-log",
            log_validate(active_checkpoint),
            mutates=False,
            after_stop=True,
        ),
    ]

    def rollback_commands(name: str, stop_command: Sequence[str]) -> list[dict[str, Any]]:
        identity = rollback[name]
        rollback_checkout = _ssh_command(
            target=target,
            known_hosts=known,
            remote_command=(
                f"cd {shlex.quote(repo_root)} && git checkout --detach "
                f"{shlex.quote(str(identity['execution_commit']))} && "
                f'test "$(git rev-parse HEAD^{{tree}})" = '
                f"{shlex.quote(str(identity['execution_tree']))}"
            ),
        )
        rollback_start = _ssh_command(
            target=target,
            known_hosts=known,
            remote_command=_remote_external_config_start(
                repo_root, str(identity["config_path"]), owner_override=False
            ),
        )
        return [
            _command(
                "capture-old-pid",
                _ssh_command(
                    target=target,
                    known_hosts=known,
                    remote_command=(
                        f"test -s {shlex.quote(pid_file)} && cat {shlex.quote(pid_file)}"
                    ),
                ),
                mutates=False,
            ),
            _command("stop-live", stop_command, mutates=True, after_stop=True),
            _command("confirm-quiescent", quiescent, mutates=False, after_stop=True),
            _command("checkout-rollback-runtime", rollback_checkout, mutates=True, after_stop=True),
            _command("start-rollback-fresh-b0", rollback_start, mutates=True, after_stop=True),
            _command(
                "fresh-rollback-process-probe",
                process_probe(
                    str(identity["config_path"]),
                    str(identity["config_sha256"]),
                    False,
                    expected_execution=identity,
                    expected_runtime_code_sha256=str(identity["runtime_code_sha256"]),
                    expected_artifact_sha256=str(identity.get("artifact_sha256", "")),
                ),
                mutates=False,
                after_stop=True,
            ),
        ]

    return {
        "disabled-deploy": disabled,
        "activate": activate,
        "rollback-primary": rollback_commands("primary_disabled", stop_active),
        "rollback-deep": rollback_commands("deep_predecessor", stop_active),
    }


def build_plan(
    *,
    specification: Mapping[str, Any],
    repository_root: Path,
    preflight_runner: PreflightRunner | None = None,
) -> dict[str, Any]:
    """Build and validate a deterministic plan without any remote command."""

    _reject_remote_environment_override()
    root = repository_root.expanduser().resolve(strict=True)
    execution_raw = specification.get("execution")
    artifact_raw = specification.get("artifact")
    configs_raw = specification.get("configs")
    pointer_raw = specification.get("active_pointer")
    ssh_raw = specification.get("ssh")
    host_raw = specification.get("host")
    remote_raw = specification.get("remote")
    rollback_raw = specification.get("rollback_identities")
    token_raw = specification.get("phase_token_sha256")
    for label, value in (
        ("execution", execution_raw),
        ("artifact", artifact_raw),
        ("configs", configs_raw),
        ("active_pointer", pointer_raw),
        ("ssh", ssh_raw),
        ("host", host_raw),
        ("remote", remote_raw),
        ("rollback_identities", rollback_raw),
        ("phase_token_sha256", token_raw),
    ):
        if not isinstance(value, Mapping):
            raise BuyE3TransactionalDeployError(f"specification lacks {label}")
    execution = gate_v2.verify_execution_git_identity(
        repository_root=root,
        expected_commit=str(execution_raw["commit"]),
        expected_tree=str(execution_raw["tree"]),
        annotated_tag=str(execution_raw["annotated_tag"]),
        expected_tag_object=str(execution_raw["annotated_tag_object"]),
    )
    manifest_path = Path(str(artifact_raw["manifest_path"])).expanduser().resolve(strict=True)
    policy_path = Path(str(artifact_raw["policy_path"])).expanduser().resolve(strict=True)
    bundle_path = Path(str(artifact_raw["predicate_bundle_path"])).expanduser().resolve(strict=True)
    artifact_manifest = _read_json(manifest_path)
    policy_payload = _read_json(policy_path)
    if (
        policy_payload.get("bindings", {}).get("owner_execution_commit")
        != execution["execution_commit"]
    ):
        raise BuyE3TransactionalDeployError("policy artifact binds another execution commit")
    runtime_sources = gate_v2.verify_runtime_sources(
        repository_root=root,
        execution_commit=execution["execution_commit"],
        artifact_manifest=artifact_manifest,
    )
    disabled_config = Path(str(configs_raw["disabled_path"])).expanduser().resolve(strict=True)
    active_config = Path(str(configs_raw["active_path"])).expanduser().resolve(strict=True)
    config_binding = gate_v2.validate_private_config_pair(
        disabled_config_path=disabled_config,
        active_config_path=active_config,
        repository_root=root,
        allowed_diff=tuple(configs_raw.get("allowed_diff", ())),
    )
    if (
        config_binding["disabled"]["artifact_files"]["manifest"]["path"] != str(manifest_path)
        or config_binding["disabled"]["artifact_files"]["policy"]["path"] != str(policy_path)
        or config_binding["disabled"]["artifact_files"]["predicate_bundle"]["path"]
        != str(bundle_path)
    ):
        raise BuyE3TransactionalDeployError("specification artifact paths differ from config")
    pointer = load_sha_bound_active_pointer(
        pointer_path=Path(str(pointer_raw["path"])),
        expected_file_sha256=str(pointer_raw["file_sha256"]),
    )
    known_hosts = bind_known_hosts(
        known_hosts_path=Path(str(ssh_raw["known_hosts_path"])),
        expected_file_sha256=str(ssh_raw["known_hosts_file_sha256"]),
        expected_fingerprint=str(ssh_raw["host_key_fingerprint"]),
    )
    host = dict(host_raw)
    for field in ("logical_host", "repo_root", "python_executable", "venv_root"):
        if not str(host.get(field, "")).strip():
            raise BuyE3TransactionalDeployError(f"host identity lacks {field}")
    if str(host["repo_root"]) != pointer["repo_root"]:
        raise BuyE3TransactionalDeployError("host repo root differs from active pointer")
    remote = dict(remote_raw)
    for field in (
        "stage_root",
        "disabled_config_path",
        "active_config_path",
        "artifact_manifest_path",
        "policy_path",
        "predicate_bundle_path",
        "pid_file",
        "log_path",
        "runtime_identity_path",
        "startup_checkpoint_path",
    ):
        if not str(remote.get(field, "")).startswith("/"):
            raise BuyE3TransactionalDeployError(f"remote path is not absolute: {field}")
    startup_markers = remote.get("startup_markers")
    if (
        not isinstance(startup_markers, list)
        or not startup_markers
        or any(not str(marker).strip() for marker in startup_markers)
    ):
        raise BuyE3TransactionalDeployError("remote startup markers are not frozen")
    disabled_strategy = _strategy_mapping(disabled_config)
    active_strategy = _strategy_mapping(active_config)
    remote_artifact_fields = {
        "buy_e3_cooldown_artifact_manifest_path": "artifact_manifest_path",
        "buy_e3_cooldown_policy_path": "policy_path",
        "buy_e3_cooldown_predicate_bundle_path": "predicate_bundle_path",
    }
    for config_field, remote_field in remote_artifact_fields.items():
        disabled_value = str(disabled_strategy.get(config_field, "")).strip()
        active_value = str(active_strategy.get(config_field, "")).strip()
        if not disabled_value or disabled_value != active_value:
            raise BuyE3TransactionalDeployError(
                f"disabled/active remote artifact path differs: {config_field}"
            )
        relative = PurePosixPath(disabled_value)
        if relative.is_absolute() or ".." in relative.parts:
            raise BuyE3TransactionalDeployError(
                f"deploy config artifact path is not repository-relative: {config_field}"
            )
        expected_remote = str(PurePosixPath(pointer["repo_root"]) / relative)
        if str(remote[remote_field]) != expected_remote:
            raise BuyE3TransactionalDeployError(
                f"remote artifact destination differs from config: {config_field}"
            )
    rollback = {
        "primary_disabled": _validate_rollback_identity(
            "primary_disabled", rollback_raw.get("primary_disabled")
        ),
        "deep_predecessor": _validate_rollback_identity(
            "deep_predecessor", rollback_raw.get("deep_predecessor")
        ),
    }
    if rollback["primary_disabled"]["identity"] == rollback["deep_predecessor"]["identity"]:
        raise BuyE3TransactionalDeployError("dual rollback identities are not distinct")
    primary = rollback["primary_disabled"]
    if (
        primary["execution_commit"] != execution["execution_commit"]
        or primary["execution_tree"] != execution["execution_tree"]
        or primary["config_path"] != remote["disabled_config_path"]
        or primary["config_sha256"] != config_binding["disabled"]["config_sha256"]
        or primary["runtime_code_sha256"] != runtime_sources["runtime_code_sha256"]
        or primary["python_executable"] != host["python_executable"]
        or primary["venv_root"] != host["venv_root"]
    ):
        raise BuyE3TransactionalDeployError("primary disabled rollback is not exact attempt2")
    phase_tokens = {
        phase: _require_sha256(token_raw.get(phase), f"token hash {phase}") for phase in PHASES
    }
    runner = preflight_runner or (
        lambda repo, config, enabled: run_isolated_preflight(repo, config, enabled)
    )
    disabled_preflight = dict(runner(root, disabled_config, False))
    active_preflight = dict(runner(root, active_config, True))
    for payload, enabled in ((disabled_preflight, False), (active_preflight, True)):
        if (
            payload.get("schema_version") != PREFLIGHT_SCHEMA
            or payload.get("status") != "isolated_config_preflight_passed"
            or payload.get("expected_enabled") is not enabled
            or payload.get("artifact_loaded_with_from_files") is not True
        ):
            raise BuyE3TransactionalDeployError("isolated preflight did not pass exactly")
    artifact_binding = {
        "artifact_sha256": config_binding["disabled"]["artifact_sha256"],
        "manifest_path": str(manifest_path),
        "manifest_file_sha256": gate_v2.file_sha256(manifest_path),
        "policy_path": str(policy_path),
        "policy_file_sha256": gate_v2.file_sha256(policy_path),
        "predicate_bundle_path": str(bundle_path),
        "predicate_bundle_file_sha256": gate_v2.file_sha256(bundle_path),
    }
    local_package = {
        "deploy_script": str(Path(__file__).resolve()),
        "gate_amendment": str(Path(gate_v2.__file__).resolve()),
        "artifact_manifest": str(manifest_path),
        "policy": str(policy_path),
        "predicate_bundle": str(bundle_path),
        "disabled_config": str(disabled_config),
        "active_config": str(active_config),
    }
    external_tools = {
        role: {"path": path, "file_sha256": gate_v2.file_sha256(Path(path))}
        for role, path in local_package.items()
    }
    commands = _phase_commands(
        pointer=pointer,
        known_hosts=known_hosts,
        host=host,
        configs=config_binding,
        remote=remote,
        execution=execution,
        rollback=rollback,
        runtime_sources=runtime_sources,
        artifact=artifact_binding,
        local_package=local_package,
    )
    for phase, rows in commands.items():
        stop_positions = [index for index, row in enumerate(rows) if row["label"] == "stop-live"]
        preflights = [index for index, row in enumerate(rows) if "preflight" in row["label"]]
        if phase in {"disabled-deploy", "activate"} and (
            len(stop_positions) != 1 or len(preflights) != 2 or max(preflights) >= stop_positions[0]
        ):
            raise BuyE3TransactionalDeployError("both isolated preflights must precede stop")
        for row in rows:
            argv = row["argv"]
            if argv[0] not in {"ssh", "rsync"} or "StrictHostKeyChecking=yes" not in " ".join(argv):
                raise BuyE3TransactionalDeployError("remote command lacks strict SSH")
    activation_gate_binding: dict[str, Any] | None = None
    activation_gate_raw = specification.get("activation_gate")
    if activation_gate_raw is not None:
        if not isinstance(activation_gate_raw, Mapping):
            raise BuyE3TransactionalDeployError("activation gate binding is malformed")
        activation_path = Path(str(activation_gate_raw.get("path", ""))).expanduser()
        expected_file_sha = _require_sha256(
            activation_gate_raw.get("file_sha256"), "activation gate file hash"
        )
        if gate_v2.file_sha256(activation_path.resolve(strict=True)) != expected_file_sha:
            raise BuyE3TransactionalDeployError("activation gate file hash drifted")
        activation_receipt = gate_v2.validate_amended_gate_receipt(activation_path)
        if (
            activation_receipt.get("execution_identity", {}).get("execution_commit")
            != execution["execution_commit"]
            or activation_receipt.get("artifact_binding", {}).get("artifact_sha256")
            != artifact_binding["artifact_sha256"]
        ):
            raise BuyE3TransactionalDeployError("activation gate binds another runtime")
        activation_gate_binding = {
            "path": str(activation_path.resolve(strict=True)),
            "file_sha256": expected_file_sha,
            "canonical_receipt_sha256": activation_receipt["canonical_amendment_receipt_sha256"],
        }
    plan: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA,
        "status": "plan_only_no_remote_command_executed",
        "planner_repository_root": str(root),
        "execution": execution,
        "runtime_sources": runtime_sources,
        "artifact": artifact_binding,
        "external_tools_and_package": external_tools,
        "configs": config_binding,
        "isolated_preflights": {
            "disabled": disabled_preflight,
            "active": active_preflight,
        },
        "active_pointer": pointer,
        "ssh": {
            **known_hosts,
            "strict_options": list(STRICT_SSH_OPTIONS),
            "environment_pointer_override_allowed": False,
        },
        "host": host,
        "remote": remote,
        "rollback_identities": rollback,
        "phase_token_sha256": phase_tokens,
        "phases": commands,
        "transaction_contract": {
            "default_mode": "dry_run_plan",
            "remote_mutation_requires_explicit_phase": True,
            "remote_mutation_requires_token": True,
            "external_narrowgate_live_config_required": True,
            "activation_restart_only": True,
            "sighup_activation_allowed": False,
            "rollback_requires_fresh_pid": True,
            "rollback_buy_deadline_identity": "B0",
            "rollback_imports_e3_deadline": False,
            "pre_stop_isolated_disabled_and_active_preflight": True,
        },
        "evidence_boundary": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "economic_arms_run": False,
            "hypothetical_live_actions_scored": False,
        },
    }
    if activation_gate_binding is not None:
        plan["activation_gate"] = activation_gate_binding
        plan["activation_gate_receipt_sha256"] = activation_gate_binding["canonical_receipt_sha256"]
    plan["canonical_plan_sha256"] = gate_v2.document_sha256(plan, "canonical_plan_sha256")
    return plan


def validate_plan(plan: Mapping[str, Any]) -> None:
    if (
        plan.get("schema_version") != PLAN_SCHEMA
        or plan.get("status") != "plan_only_no_remote_command_executed"
        or plan.get("canonical_plan_sha256")
        != gate_v2.document_sha256(plan, "canonical_plan_sha256")
        or plan.get("execution", {}).get("execution_commit") != gate_v2.FROZEN_EXECUTION_COMMIT
    ):
        raise BuyE3TransactionalDeployError("deployment plan identity drifted")


def _revalidate_plan_inputs(plan: Mapping[str, Any]) -> None:
    _reject_remote_environment_override()
    tools = plan.get("external_tools_and_package")
    if not isinstance(tools, Mapping):
        raise BuyE3TransactionalDeployError("plan lacks external package bindings")
    for role, binding in tools.items():
        if not isinstance(binding, Mapping):
            raise BuyE3TransactionalDeployError(f"external package binding malformed: {role}")
        path = Path(str(binding.get("path", ""))).expanduser()
        if path.is_symlink() or not path.is_file():
            raise BuyE3TransactionalDeployError(f"external package file unavailable: {role}")
        if gate_v2.file_sha256(path.resolve(strict=True)) != binding.get("file_sha256"):
            raise BuyE3TransactionalDeployError(f"external package file drifted: {role}")
    pointer = plan["active_pointer"]
    load_sha_bound_active_pointer(
        pointer_path=Path(str(pointer["path"])),
        expected_file_sha256=str(pointer["file_sha256"]),
    )
    ssh = plan["ssh"]
    bind_known_hosts(
        known_hosts_path=Path(str(ssh["path"])),
        expected_file_sha256=str(ssh["file_sha256"]),
        expected_fingerprint=str(ssh["expected_fingerprint"]),
    )
    root = Path(str(plan["planner_repository_root"])).expanduser().resolve(strict=True)
    execution = plan["execution"]
    gate_v2.verify_execution_git_identity(
        repository_root=root,
        expected_commit=str(execution["execution_commit"]),
        expected_tree=str(execution["execution_tree"]),
        annotated_tag=str(execution["annotated_tag"]),
        expected_tag_object=str(execution["annotated_tag_object"]),
    )
    manifest = _read_json(Path(str(plan["artifact"]["manifest_path"])))
    runtime = gate_v2.verify_runtime_sources(
        repository_root=root,
        execution_commit=str(execution["execution_commit"]),
        artifact_manifest=manifest,
    )
    if runtime.get("runtime_code_sha256") != plan["runtime_sources"].get("runtime_code_sha256"):
        raise BuyE3TransactionalDeployError("runtime source aggregate drifted")
    activation = plan.get("activation_gate")
    if activation is not None:
        path = Path(str(activation["path"]))
        if gate_v2.file_sha256(path.resolve(strict=True)) != activation["file_sha256"]:
            raise BuyE3TransactionalDeployError("activation gate bytes drifted")
        gate_v2.validate_amended_gate_receipt(path)


def phase_authorization_token_sha256(token: str) -> str:
    if not token:
        raise BuyE3TransactionalDeployError("empty phase token")
    return _sha256_text(token)


def _default_runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, capture_output=True, text=True)


def execute_phase(
    *,
    plan: Mapping[str, Any],
    phase: str,
    token: str,
    authorize_remote_mutation: bool,
    runner: CommandRunner = _default_runner,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """Execute one explicit transaction phase; default callers cannot mutate."""

    validate_plan(plan)
    _revalidate_plan_inputs(plan)
    if phase not in MUTATING_PHASES:
        raise BuyE3TransactionalDeployError("unknown deployment phase")
    if not authorize_remote_mutation:
        raise PermissionError("remote mutation requires --authorize-remote-mutation")
    expected_token = str(plan["phase_token_sha256"][phase])
    if phase_authorization_token_sha256(token) != expected_token:
        raise PermissionError("phase token does not match the frozen plan")
    if phase == "activate" and not plan.get("activation_gate_receipt_sha256"):
        raise PermissionError("activation requires a separately bound amended gate receipt")
    rows = plan["phases"][phase]
    results: list[dict[str, Any]] = []
    stopped = False
    rollback_attempted = False
    old_pid: int | None = None
    try:
        for row in rows:
            completed = runner(tuple(str(value) for value in row["argv"]))
            result = {
                "label": row["label"],
                "command_sha256": row["command_sha256"],
                "returncode": int(completed.returncode),
                "stdout_sha256": _sha256_text(completed.stdout or ""),
                "stderr_sha256": _sha256_text(completed.stderr or ""),
            }
            if completed.returncode == 0 and row["label"] == "capture-old-pid":
                try:
                    old_pid = int((completed.stdout or "").strip())
                except ValueError as exc:
                    raise BuyE3TransactionalDeployError("old PID probe is malformed") from exc
                if old_pid <= 0:
                    raise BuyE3TransactionalDeployError("old PID probe is invalid")
                result["observed_pid"] = old_pid
            if completed.returncode == 0 and "fresh-" in row["label"]:
                if "process-probe" in row["label"]:
                    try:
                        process = json.loads(completed.stdout)
                    except json.JSONDecodeError as exc:
                        raise BuyE3TransactionalDeployError(
                            "fresh process probe is not JSON"
                        ) from exc
                    if not isinstance(process, dict):
                        raise BuyE3TransactionalDeployError("fresh process probe is malformed")
                    fresh_pid = int(process.get("pid", -1))
                    if process.get(
                        "schema_version"
                    ) != gate_v2.PROCESS_IDENTITY_SCHEMA or process.get(
                        "canonical_process_identity_sha256"
                    ) != gate_v2.document_sha256(process, "canonical_process_identity_sha256"):
                        raise BuyE3TransactionalDeployError("fresh process identity hash drifted")
                    expected_enabled = row["label"] == "fresh-active-process-probe"
                    if (
                        process.get("buy_e3_enabled") is not expected_enabled
                        or process.get("owner_override_effective") is not expected_enabled
                        or process.get("initial_buy_deadline_identity") != "B0"
                        or process.get("e3_deadline_imported") is not False
                    ):
                        raise BuyE3TransactionalDeployError(
                            "fresh process activation/deadline identity drifted"
                        )
                else:
                    try:
                        fresh_pid = int((completed.stdout or "").strip())
                    except ValueError as exc:
                        raise BuyE3TransactionalDeployError(
                            "fresh rollback PID probe is malformed"
                        ) from exc
                if old_pid is None or fresh_pid <= 0 or fresh_pid == old_pid:
                    raise BuyE3TransactionalDeployError("restart did not produce a fresh PID")
                result["observed_pid"] = fresh_pid
            results.append(result)
            if row["label"] == "stop-live" and completed.returncode == 0:
                stopped = True
            if completed.returncode != 0:
                raise BuyE3TransactionalDeployError(f"remote phase failed closed at {row['label']}")
    except Exception:
        if stopped and phase not in {"rollback-primary", "rollback-deep"}:
            rollback_attempted = True
            for row in plan["phases"]["rollback-primary"]:
                if row["label"] in {"capture-old-pid", "stop-live"}:
                    continue
                completed = runner(tuple(str(value) for value in row["argv"]))
                results.append(
                    {
                        "label": f"automatic-rollback:{row['label']}",
                        "command_sha256": row["command_sha256"],
                        "returncode": int(completed.returncode),
                        "stdout_sha256": _sha256_text(completed.stdout or ""),
                        "stderr_sha256": _sha256_text(completed.stderr or ""),
                    }
                )
                if completed.returncode != 0:
                    break
        raise
    finally:
        if output_path is not None:
            receipt: dict[str, Any] = {
                "schema_version": RECEIPT_SCHEMA,
                "plan_sha256": plan["canonical_plan_sha256"],
                "phase": phase,
                "remote_mutation_authorized": True,
                "results": results,
                "rollback_attempted": rollback_attempted,
                "validation_read": False,
                "sealed_holdout_read": False,
                "economic_arms_run": False,
            }
            receipt["canonical_receipt_sha256"] = gate_v2.document_sha256(
                receipt, "canonical_receipt_sha256"
            )
            gate_v2.atomic_write_receipt(output_path, receipt)
    return {
        "schema_version": RECEIPT_SCHEMA,
        "plan_sha256": plan["canonical_plan_sha256"],
        "phase": phase,
        "status": "phase_complete",
        "results": results,
        "rollback_attempted": rollback_attempted,
    }


def _build_spec_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--specification", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, default=REPO_ROOT)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    _build_spec_parser(plan)
    plan.add_argument("--output", type=Path, required=True)
    preflight = subparsers.add_parser("isolated-preflight")
    preflight.add_argument("--repository-root", type=Path, required=True)
    preflight.add_argument("--config", type=Path, required=True)
    preflight.add_argument("--expected-enabled", type=int, choices=(0, 1), required=True)
    process = subparsers.add_parser("process-probe")
    process.add_argument("--repository-root", type=Path, required=True)
    process.add_argument("--pid-file", type=Path, required=True)
    process.add_argument("--config", type=Path, required=True)
    process.add_argument("--config-sha256", required=True)
    process.add_argument("--python-executable", type=Path, required=True)
    process.add_argument("--venv-root", type=Path, required=True)
    process.add_argument("--runtime-identity", type=Path, required=True)
    process.add_argument("--expected-enabled", type=int, choices=(0, 1), required=True)
    process.add_argument("--execution-commit", required=True)
    process.add_argument("--execution-tree", required=True)
    process.add_argument("--artifact-sha256", default="")
    process.add_argument("--runtime-code-sha256", required=True)
    checkpoint = subparsers.add_parser("log-checkpoint")
    checkpoint.add_argument("--log", type=Path, required=True)
    checkpoint.add_argument("--output", type=Path, required=True)
    log_validate = subparsers.add_parser("log-validate")
    log_validate.add_argument("--log", type=Path, required=True)
    log_validate.add_argument("--checkpoint", type=Path, required=True)
    log_validate.add_argument("--marker", action="append", required=True)
    execute = subparsers.add_parser("execute")
    execute.add_argument("--plan", type=Path, required=True)
    execute.add_argument("--phase", choices=PHASES, required=True)
    execute.add_argument("--token", required=True)
    execute.add_argument("--authorize-remote-mutation", action="store_true")
    execute.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    command = args.command
    if command == "isolated-preflight":
        payload = isolated_config_preflight(
            args.repository_root,
            args.config,
            bool(args.expected_enabled),
        )
        print(json.dumps(payload, sort_keys=True))
        return 0
    if command == "process-probe":
        payload = capture_runtime_process_probe(
            repository_root=args.repository_root,
            pid_file=args.pid_file,
            config_path=args.config,
            config_sha256=args.config_sha256,
            python_executable=args.python_executable,
            venv_root=args.venv_root,
            runtime_identity_path=args.runtime_identity,
            expected_buy_e3_enabled=bool(args.expected_enabled),
            expected_execution_commit=args.execution_commit,
            expected_execution_tree=args.execution_tree,
            expected_artifact_sha256=args.artifact_sha256,
            expected_runtime_code_sha256=args.runtime_code_sha256,
        )
        print(json.dumps(payload, sort_keys=True))
        return 0
    if command == "log-checkpoint":
        payload = gate_v2.capture_startup_log_checkpoint(args.log)
        gate_v2.atomic_write_receipt(args.output, payload)
        print(json.dumps(payload, sort_keys=True))
        return 0
    if command == "log-validate":
        checkpoint = gate_v2.read_json(args.checkpoint)
        payload = gate_v2.validate_startup_log_after_checkpoint(
            log_path=args.log,
            checkpoint=checkpoint,
            required_markers=tuple(args.marker),
        )
        print(json.dumps(payload, sort_keys=True))
        return 0
    if command == "execute":
        plan = _read_json(args.plan)
        execute_phase(
            plan=plan,
            phase=args.phase,
            token=args.token,
            authorize_remote_mutation=args.authorize_remote_mutation,
            output_path=args.output,
        )
        return 0
    if command != "plan":
        parser.error("a command is required")
    specification = _read_json(args.specification)
    payload = build_plan(specification=specification, repository_root=args.repository_root)
    gate_v2.atomic_write_receipt(args.output, payload)
    print(json.dumps({"status": payload["status"], "plan": payload["canonical_plan_sha256"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ACTIVE_POINTER_STATUS",
    "BuyE3TransactionalDeployError",
    "PHASES",
    "PLAN_SCHEMA",
    "PREFLIGHT_SCHEMA",
    "RECEIPT_SCHEMA",
    "bind_known_hosts",
    "build_plan",
    "execute_phase",
    "capture_runtime_process_probe",
    "isolated_config_preflight",
    "load_sha_bound_active_pointer",
    "phase_authorization_token_sha256",
    "run_isolated_preflight",
    "validate_plan",
]
