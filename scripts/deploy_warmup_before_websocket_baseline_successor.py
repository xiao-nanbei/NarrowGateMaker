"""Deploy the warmup-before-WebSocket baseline successor transactionally.

The default command is plan-only and performs no SSH or mutation.  Remote
staging and production deployment are separate explicit stages.  Production
mutation requires a hash-bound owner token and an admitted staging receipt.

The candidate changes only the six files frozen by
``warmup_before_websocket_baseline_successor.v1``.  It keeps the current
attempt6 Python environment, disables lifecycle journal-v2, and preserves the
v9 strategy/config semantics.  Any failure after the controlled stop restores
the exact attempt6 files and active venv, restarts them, and subjects the
restored runtime to the same delayed startup-stability contract.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shlex
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_warmup_before_websocket_baseline_successor import (  # noqa: E402
    EXPECTED_ACTION_ENABLEMENT,
    STARTUP_CONTRACT,
    validate_staging,
)
from scripts.build_warmup_before_websocket_baseline_successor import (  # noqa: E402
    SCHEMA_VERSION as CANDIDATE_SCHEMA_VERSION,
)
from scripts.live_remote_pointer import active_live_remote_fields  # noqa: E402

SCHEMA_VERSION = "warmup_before_websocket_baseline_successor_deploy.v1"
PLAN_SCHEMA_VERSION = "warmup_before_websocket_baseline_successor_deploy_plan.v1"
EVIDENCE_SCHEMA_VERSION = "warmup_before_websocket_baseline_successor_deploy_evidence.v1"
STABILITY_SCHEMA_VERSION = "warmup_before_websocket_startup_stability.v1"

EPHEMERAL_ROOT = Path(
    os.environ.get("NARROWGATE_EPHEMERAL_ROOT", tempfile.gettempdir())
).expanduser()
DEFAULT_CANDIDATE_MANIFEST = Path(
    EPHEMERAL_ROOT
    / "narrowgate_warmup_before_websocket_baseline_successor_v1_20260805"
    / "baseline_successor_manifest.json"
)
DEFAULT_CURRENT_RELEASE_MANIFEST = Path(
    EPHEMERAL_ROOT
    / "narrowgate_prospective_lifecycle_narrow_release_v1_20260805_attempt6"
    / "release_manifest.json"
)
DEFAULT_CURRENT_RUNTIME_RECEIPT = Path(
    EPHEMERAL_ROOT
    / "narrowgate_prospective_lifecycle_runtime_receipt_attempt6_20260805.json"
)
_ACTIVE_REMOTE = active_live_remote_fields(REPO_ROOT)
DEFAULT_REMOTE = _ACTIVE_REMOTE.get("ssh_target", "")
DEFAULT_REMOTE_ROOT = os.environ.get(
    "NARROWGATE_REMOTE_ROOT",
    _ACTIVE_REMOTE.get("repo_root", str(Path.home() / REPO_ROOT.name)),
)

TARGET_PATHS = (
    "live/config.py",
    "live/config.yaml",
    "live/main.py",
    "live/ws_handler.py",
    "strategy/maker_engine.py",
    "strategy/order_manager.py",
)
REQUIRED_ORDERED_LOG_MARKERS = (
    "MakerEngine started",
    "All WebSocket streams started",
    "Entering main loop...",
)
FORBIDDEN_LOG_MARKERS = (
    "Fatal error:",
    "completed 10s feature bucket lacks an exact causal 1s grid",
    "duplicate-grid",
    "duplicate grid",
)
CANONICAL_BUCKET_S = 10.0
MINIMUM_STABLE_BUCKETS = 2
MINIMUM_STABILITY_WINDOW_S = 25.0
MAKER_LOG_RELATIVE = "logs/maker.log"

CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _read_object(path: Path, label: str) -> dict[str, Any]:
    unresolved = path.expanduser()
    if unresolved.is_symlink():
        raise ValueError(f"{label} must not be a symlink: {unresolved}")
    resolved = unresolved.resolve(strict=True)
    if not resolved.is_file():
        raise ValueError(f"{label} must be a regular file: {resolved}")
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _validate_hex_sha256(value: Any, label: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{label} must be a lowercase SHA256")
    return normalized


def _verify_claimed_identity(
    payload: Mapping[str, Any],
    *,
    identity_field: str,
    label: str,
) -> str:
    claimed = _validate_hex_sha256(payload.get(identity_field), f"{label} {identity_field}")
    body = {key: value for key, value in payload.items() if key != identity_field}
    actual = _canonical_sha256(body)
    if actual != claimed:
        raise ValueError(f"{label} canonical identity mismatch: expected={claimed} actual={actual}")
    return claimed


def _record_map(payload: Mapping[str, Any], label: str) -> dict[str, dict[str, Any]]:
    rows = payload.get("files")
    if not isinstance(rows, list):
        raise ValueError(f"{label} files must be a list")
    result: dict[str, dict[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, dict):
            raise ValueError(f"{label} contains a non-object file record")
        logical = str(raw.get("path", ""))
        if not logical or logical in result:
            raise ValueError(f"{label} contains an empty or duplicate path: {logical!r}")
        pure = PurePosixPath(logical)
        if pure.is_absolute() or ".." in pure.parts:
            raise ValueError(f"{label} contains an unsafe path: {logical}")
        _validate_hex_sha256(raw.get("sha256"), f"{label} {logical} SHA256")
        result[logical] = dict(raw)
    return result


def _strategy_flags(raw_config: Mapping[str, Any]) -> dict[str, Any]:
    strategy = raw_config.get("strategy")
    ml = raw_config.get("ml")
    if not isinstance(strategy, Mapping) or not isinstance(ml, Mapping):
        raise ValueError("config must contain strategy and ml mappings")
    return {
        "ml_enabled": ml.get("enabled"),
        "dynamic_fill_hazard_shadow_enabled": strategy.get("dynamic_fill_hazard_shadow_enabled"),
        "dynamic_fill_hazard_action_enabled": strategy.get("dynamic_fill_hazard_action_enabled"),
        "buy_fill_selection_shadow_enabled": strategy.get("buy_fill_selection_shadow_enabled"),
        "buy_fill_selection_live_enabled": strategy.get("buy_fill_selection_live_enabled"),
        "buy_fill_selection_live_model_path": strategy.get("buy_fill_selection_live_model_path"),
    }


def _candidate_journal_disabled(manifest: Mapping[str, Any]) -> None:
    journal = manifest.get("journal_boundary")
    expected = {
        "lifecycle_journal_enabled": False,
        "lifecycle_journal_config_present": False,
        "lifecycle_journal_runtime_imported": False,
        "journal_payload_files_included": False,
        "economic_outcomes_read": False,
    }
    if journal != expected:
        raise ValueError("candidate lifecycle journal boundary is not exactly OFF")
    equality = manifest.get("strategy_config_semantic_equality")
    if not isinstance(equality, Mapping):
        raise ValueError("candidate lacks strategy/config semantic equality")
    required_true = (
        "passed",
        "config_byte_equal",
        "config_semantic_equal",
        "unchanged_v9_files_byte_equal",
        "model_binding_equal",
        "p3_binding_equal",
        "action_enablement_equal",
    )
    if not all(equality.get(field) is True for field in required_true):
        raise ValueError("candidate strategy/config semantic equality is incomplete")
    if equality.get("strategy_or_quote_parameters_changed") is not False:
        raise ValueError("candidate changes strategy or quote parameters")
    if equality.get("action_enablement") != EXPECTED_ACTION_ENABLEMENT:
        raise ValueError("candidate action enablement differs from frozen v9")


def load_bound_deployment(
    candidate_manifest_path: Path = DEFAULT_CANDIDATE_MANIFEST,
    current_release_manifest_path: Path = DEFAULT_CURRENT_RELEASE_MANIFEST,
    current_runtime_receipt_path: Path = DEFAULT_CURRENT_RUNTIME_RECEIPT,
) -> dict[str, Any]:
    """Validate and bind the local candidate plus exact current attempt6 runtime."""

    candidate_manifest_path = candidate_manifest_path.expanduser().resolve(strict=True)
    current_release_manifest_path = current_release_manifest_path.expanduser().resolve(strict=True)
    current_runtime_receipt_path = current_runtime_receipt_path.expanduser().resolve(strict=True)
    candidate_root = candidate_manifest_path.parent
    candidate_validation = validate_staging(candidate_root)
    candidate = _read_object(candidate_manifest_path, "candidate manifest")
    current_release = _read_object(current_release_manifest_path, "attempt6 release manifest")
    current_receipt = _read_object(current_runtime_receipt_path, "attempt6 runtime receipt")

    if candidate.get("schema_version") != CANDIDATE_SCHEMA_VERSION:
        raise ValueError("candidate manifest schema drifted")
    if candidate.get("startup_contract") != STARTUP_CONTRACT:
        raise ValueError("candidate startup contract drifted")
    candidate_manifest_sha256 = _verify_claimed_identity(
        candidate,
        identity_field="manifest_sha256",
        label="candidate manifest",
    )
    if candidate_validation.get("manifest_sha256") != candidate_manifest_sha256:
        raise ValueError("candidate builder validation identity differs")
    _candidate_journal_disabled(candidate)

    if current_release.get("schema_version") != "prospective_lifecycle_narrow_release.v1":
        raise ValueError("current release is not attempt6 lifecycle release schema")
    current_release_sha256 = _verify_claimed_identity(
        current_release,
        identity_field="manifest_sha256",
        label="attempt6 release manifest",
    )
    if current_receipt.get("schema_version") != "prospective_lifecycle_remote_release_evidence.v1":
        raise ValueError("current runtime receipt schema drifted")
    if current_receipt.get("stage") != "runtime":
        raise ValueError("current receipt is not a runtime receipt")
    current_receipt_identity_sha256 = _verify_claimed_identity(
        current_receipt,
        identity_field="receipt_identity_sha256",
        label="attempt6 runtime receipt",
    )
    if current_receipt.get("release_manifest_sha256") != current_release_sha256:
        raise ValueError("attempt6 runtime receipt does not bind the selected release manifest")
    if current_receipt.get("production_mutation_performed") is not True:
        raise ValueError("attempt6 runtime receipt does not attest a production deployment")

    candidate_records = _record_map(candidate, "candidate manifest")
    if tuple(sorted(candidate_records)) != tuple(sorted(TARGET_PATHS)):
        raise ValueError("candidate must contain exactly the six frozen target files")
    current_release_records = _record_map(current_release, "attempt6 release manifest")
    missing_current = sorted(set(TARGET_PATHS) - set(current_release_records))
    if missing_current:
        raise ValueError("attempt6 release lacks target files: " + ", ".join(missing_current))
    current_records = {path: current_release_records[path] for path in TARGET_PATHS}

    for logical, row in candidate_records.items():
        unresolved = candidate_root / logical
        if unresolved.is_symlink():
            raise ValueError(f"candidate file must not be a symlink: {logical}")
        staged = unresolved.resolve(strict=True)
        if candidate_root not in staged.parents or not staged.is_file():
            raise ValueError(f"candidate file escaped or is not regular: {logical}")
        if _sha256(staged) != row["sha256"]:
            raise ValueError(f"candidate staged file hash drifted: {logical}")

    current_release_root = current_release_manifest_path.parent
    for logical, row in current_records.items():
        unresolved = current_release_root / logical
        if unresolved.is_symlink():
            raise ValueError(f"attempt6 release file must not be a symlink: {logical}")
        release_file = unresolved.resolve(strict=True)
        if current_release_root not in release_file.parents or not release_file.is_file():
            raise ValueError(f"attempt6 release file escaped or is not regular: {logical}")
        if _sha256(release_file) != row["sha256"]:
            raise ValueError(f"attempt6 local release payload hash drifted: {logical}")
    current_config_path = (current_release_root / "live/config.yaml").resolve(strict=True)
    candidate_config_path = (candidate_root / "live/config.yaml").resolve(strict=True)
    current_config = yaml.safe_load(current_config_path.read_text(encoding="utf-8"))
    candidate_config = yaml.safe_load(candidate_config_path.read_text(encoding="utf-8"))
    if not isinstance(current_config, dict) or not isinstance(candidate_config, dict):
        raise ValueError("current/candidate configs must be mappings")
    current_lifecycle = current_config.pop("lifecycle_journal_v2", None)
    if not isinstance(current_lifecycle, dict) or current_lifecycle.get("enabled") is not True:
        raise ValueError("selected current attempt6 config does not enable journal-v2")
    if "lifecycle_journal_v2" in candidate_config:
        raise ValueError("candidate config must omit lifecycle_journal_v2")
    if current_config != candidate_config:
        raise ValueError("candidate changes config semantics beyond removing journal-v2")
    if _strategy_flags(candidate_config) != EXPECTED_ACTION_ENABLEMENT:
        raise ValueError("candidate strategy/action semantics drifted")

    evidence = current_receipt.get("evidence")
    if not isinstance(evidence, Mapping):
        raise ValueError("attempt6 runtime receipt lacks evidence")
    runtime_files = evidence.get("runtime_files")
    deployment = evidence.get("deployment")
    if not isinstance(runtime_files, Mapping) or not isinstance(deployment, Mapping):
        raise ValueError("attempt6 runtime receipt lacks runtime files/deployment")
    current_hashes = {path: str(current_records[path]["sha256"]) for path in TARGET_PATHS}
    for logical, expected in current_hashes.items():
        if runtime_files.get(logical) != expected:
            raise ValueError(f"attempt6 runtime receipt target hash mismatch: {logical}")
    current_pid = int(evidence.get("maker_pid", 0) or 0)
    current_venv = str(evidence.get("python_prefix", ""))
    if current_pid <= 0 or not current_venv.startswith("/"):
        raise ValueError("attempt6 runtime receipt lacks exact PID/active venv")
    if deployment.get("successor_venv") != current_venv:
        raise ValueError("attempt6 deployment/runtime venv differs")
    if deployment.get("active_venv_target_after") != current_venv:
        raise ValueError("attempt6 active venv target differs")
    if deployment.get("deployment_files_applied") is not True:
        raise ValueError("attempt6 files were not attested as applied")
    if evidence.get("automatic_rollback_performed") is not False:
        raise ValueError("attempt6 runtime receipt indicates automatic rollback")
    if deployment.get("strategy_parameters_changed") is not False:
        raise ValueError("attempt6 changed strategy parameters")
    if deployment.get("q90_action_enabled") is not False:
        raise ValueError("attempt6 enabled q90 action")
    if deployment.get("buy_fill_selection_enabled") is not False:
        raise ValueError("attempt6 enabled BUY fill selection")

    return {
        "schema_version": SCHEMA_VERSION,
        "candidate_manifest": candidate,
        "candidate_manifest_path": candidate_manifest_path,
        "candidate_manifest_sha256": candidate_manifest_sha256,
        "candidate_manifest_file_sha256": _sha256(candidate_manifest_path),
        "candidate_root": candidate_root,
        "candidate_records": candidate_records,
        "candidate_hashes": {path: str(candidate_records[path]["sha256"]) for path in TARGET_PATHS},
        "candidate_validation": candidate_validation,
        "current_release_manifest": current_release,
        "current_release_manifest_path": current_release_manifest_path,
        "current_release_manifest_sha256": current_release_sha256,
        "current_release_manifest_file_sha256": _sha256(current_release_manifest_path),
        "current_runtime_receipt": current_receipt,
        "current_runtime_receipt_path": current_runtime_receipt_path,
        "current_runtime_receipt_identity_sha256": current_receipt_identity_sha256,
        "current_runtime_receipt_file_sha256": _sha256(current_runtime_receipt_path),
        "current_hashes": current_hashes,
        "current_pid": current_pid,
        "current_venv": current_venv,
        "current_journal_enabled": True,
        "candidate_journal_enabled": False,
        "strategy_config_semantics_unchanged": True,
    }


def _validate_remote_path(value: str, label: str) -> str:
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{label} must be an absolute safe POSIX path")
    return str(path)


def _validate_instance_id(value: str | None, bound: Mapping[str, Any]) -> str:
    if value is None:
        value = (
            "safe-baseline-"
            + str(bound["candidate_manifest_sha256"])[:10]
            + "-"
            + str(bound["current_runtime_receipt_identity_sha256"])[:10]
        )
    allowed = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    if not 1 <= len(value) <= 64 or any(character not in allowed for character in value):
        raise ValueError("deployment instance id must contain 1-64 safe characters")
    return value


def _validate_stability_window(value: float) -> float:
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < MINIMUM_STABILITY_WINDOW_S:
        raise ValueError("stability window must be at least 25 seconds")
    if normalized < CANONICAL_BUCKET_S * MINIMUM_STABLE_BUCKETS:
        raise ValueError("stability window must cover two canonical 10s buckets")
    return normalized


def _remote_python_command(python: str, source: str, *arguments: str) -> str:
    return " ".join(
        [
            shlex.quote(python),
            "-I",
            "-c",
            shlex.quote(source),
            *(shlex.quote(argument) for argument in arguments),
        ]
    )


def _remote_env_python_command(
    *,
    remote_root: str,
    python: str,
    source: str,
    arguments: Sequence[str],
) -> str:
    inner = " ".join(
        [
            "set -a;",
            f". {shlex.quote(remote_root + '/live/.env')};",
            "set +a;",
            "exec env -u PYTHONPATH",
            shlex.quote(python),
            "-I",
            "-c",
            shlex.quote(source),
            *(shlex.quote(argument) for argument in arguments),
        ]
    )
    return f"bash -lc {shlex.quote(inner)}"


def _ssh_command(remote: str, remote_root: str, command: str) -> list[str]:
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        remote,
        f"cd {shlex.quote(remote_root)} && {command}",
    ]


def _runtime_probe_source() -> str:
    return r"""
import hashlib, json, os, pathlib, sys, time, yaml
root = pathlib.Path(sys.argv[1]).resolve()
expected = json.loads(sys.argv[2])
expected_venv = pathlib.Path(sys.argv[3]).resolve()
expected_pid = int(sys.argv[4])
wait_s = float(sys.argv[5])
def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
def maker_processes():
    rows = []
    for item in pathlib.Path("/proc").iterdir():
        if not item.name.isdigit() or int(item.name) == os.getpid():
            continue
        try:
            argv = (item / "cmdline").read_bytes().split(b"\0")
            decoded = [value.decode(errors="replace") for value in argv if value]
        except OSError:
            continue
        joined = " ".join(decoded)
        if "live/main.py" in joined and "--config" in joined:
            rows.append({"pid": int(item.name), "argv": decoded, "cmdline": joined})
    return sorted(rows, key=lambda row: row["pid"])
deadline = time.monotonic() + max(0.0, wait_s)
while True:
    processes = maker_processes()
    if len(processes) == 1:
        break
    if time.monotonic() >= deadline:
        raise SystemExit(f"expected exactly one maker PID, got {processes}")
    time.sleep(0.1)
process = processes[0]
if expected_pid > 0 and process["pid"] != expected_pid:
    raise SystemExit(f"maker PID drift: expected={expected_pid} observed={process['pid']}")
active = root / ".venv-active"
if not active.is_symlink() or active.resolve(strict=True) != expected_venv:
    raise SystemExit("active venv target drifted")
expected_python = (expected_venv / "bin/python").resolve(strict=True)
if not process["argv"] or pathlib.Path(process["argv"][0]).resolve() != expected_python:
    raise SystemExit("maker process did not start from the bound active venv")
files = {}
for logical, expected_hash in expected.items():
    unresolved = root / logical
    if unresolved.is_symlink():
        raise SystemExit(f"runtime file must not be a symlink: {logical}")
    path = unresolved.resolve()
    if root not in path.parents or not path.is_file():
        raise SystemExit(f"runtime file is missing or unsafe: {logical}")
    actual = sha(path)
    if actual != expected_hash:
        raise SystemExit(f"runtime hash mismatch {logical}: {actual}")
    files[logical] = actual
raw = yaml.safe_load((root / "live/config.yaml").read_text())
if not isinstance(raw, dict):
    raise SystemExit("runtime config is not a mapping")
strategy = raw.get("strategy", {})
ml = raw.get("ml", {})
lifecycle = raw.get("lifecycle_journal_v2")
print(json.dumps({
    "schema_version": "warmup_before_websocket_current_runtime_probe.v1",
    "maker_pid": process["pid"],
    "maker_cmdline": process["cmdline"],
    "active_venv": str(active.resolve(strict=True)),
    "python_prefix": str(pathlib.Path(sys.prefix).resolve()),
    "runtime_files": files,
    "journal_enabled": isinstance(lifecycle, dict) and lifecycle.get("enabled") is True,
    "strategy_flags": {
        "ml_enabled": ml.get("enabled"),
        "dynamic_fill_hazard_shadow_enabled": strategy.get("dynamic_fill_hazard_shadow_enabled"),
        "dynamic_fill_hazard_action_enabled": strategy.get("dynamic_fill_hazard_action_enabled"),
        "buy_fill_selection_shadow_enabled": strategy.get("buy_fill_selection_shadow_enabled"),
        "buy_fill_selection_live_enabled": strategy.get("buy_fill_selection_live_enabled"),
        "buy_fill_selection_live_model_path": strategy.get("buy_fill_selection_live_model_path"),
    },
}, sort_keys=True))
""".strip()


def _prepare_stage_source() -> str:
    return r"""
import json, os, pathlib, sys
stage = pathlib.Path(sys.argv[1]).resolve()
root = pathlib.Path(sys.argv[2]).resolve()
if root not in stage.parents or stage == root:
    raise SystemExit("isolated stage escaped remote root")
if stage.exists():
    raise SystemExit("isolated stage already exists")
stage.mkdir(parents=True)
(stage / "payload").mkdir()
print(json.dumps({
    "schema_version": "warmup_before_websocket_stage_prepare.v1",
    "isolated_stage_created": True,
    "stage_root": str(stage),
    "payload_root": str(stage / "payload"),
}, sort_keys=True))
""".strip()


def _stage_validation_source() -> str:
    return r"""
import ast, hashlib, importlib, importlib.util, json, pathlib, sys, yaml
sys.dont_write_bytecode = True
stage = pathlib.Path(sys.argv[1]).resolve()
remote_root = pathlib.Path(sys.argv[2]).resolve()
records = json.loads(sys.argv[3])
expected_manifest = sys.argv[4]
expected_manifest_file = sys.argv[5]
expected_flags = json.loads(sys.argv[6])
manifest_path = stage / "baseline_successor_manifest.json"
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def canonical(payload):
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode()).hexdigest()
if sha(manifest_path) != expected_manifest_file:
    raise SystemExit("staged candidate manifest file SHA256 mismatch")
manifest = json.loads(manifest_path.read_text())
claimed = manifest.pop("manifest_sha256", None)
if claimed != expected_manifest or canonical(manifest) != expected_manifest:
    raise SystemExit("staged candidate canonical manifest mismatch")
expected_paths = {"baseline_successor_manifest.json", *records}
actual_paths = {
    path.relative_to(stage).as_posix()
    for path in stage.rglob("*") if path.is_file()
}
if actual_paths != expected_paths:
    raise SystemExit(f"staged candidate file set drifted: {sorted(actual_paths)}")
for logical, expected_hash in records.items():
    unresolved = stage / logical
    if unresolved.is_symlink():
        raise SystemExit(f"staged file must not be a symlink: {logical}")
    path = unresolved.resolve()
    if stage not in path.parents or not path.is_file():
        raise SystemExit(f"staged file unsafe: {logical}")
    if sha(path) != expected_hash:
        raise SystemExit(f"staged file hash mismatch: {logical}")
    if path.suffix == ".py":
        ast.parse(path.read_text(), filename=str(path))
raw = yaml.safe_load((stage / "live/config.yaml").read_text())
if not isinstance(raw, dict) or "lifecycle_journal_v2" in raw:
    raise SystemExit("candidate journal-v2 must be absent")
strategy = raw.get("strategy", {})
ml = raw.get("ml", {})
flags = {
    "ml_enabled": ml.get("enabled"),
    "dynamic_fill_hazard_shadow_enabled": strategy.get("dynamic_fill_hazard_shadow_enabled"),
    "dynamic_fill_hazard_action_enabled": strategy.get("dynamic_fill_hazard_action_enabled"),
    "buy_fill_selection_shadow_enabled": strategy.get("buy_fill_selection_shadow_enabled"),
    "buy_fill_selection_live_enabled": strategy.get("buy_fill_selection_live_enabled"),
    "buy_fill_selection_live_model_path": strategy.get("buy_fill_selection_live_model_path"),
}
if flags != expected_flags:
    raise SystemExit(f"candidate strategy flags drifted: {flags}")
project_path_index = next(
    (index for index, value in enumerate(sys.path)
     if "site-packages" in value or "dist-packages" in value),
    len(sys.path),
)
sys.path[project_path_index:project_path_index] = [str(stage), str(remote_root)]
def load_composite_package(name):
    init_path = remote_root / name / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        name,
        init_path,
        submodule_search_locations=[str(stage / name), str(remote_root / name)],
    )
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot construct composite package: {name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
for package in ("live", "strategy"):
    load_composite_package(package)
expected_modules = {
    "live.config": "live/config.py",
    "live.main": "live/main.py",
    "live.ws_handler": "live/ws_handler.py",
    "strategy.maker_engine": "strategy/maker_engine.py",
    "strategy.order_manager": "strategy/order_manager.py",
}
loaded = {}
for module_name, logical in expected_modules.items():
    module = importlib.import_module(module_name)
    actual = pathlib.Path(module.__file__).resolve()
    expected = (stage / logical).resolve()
    if actual != expected:
        raise SystemExit(f"candidate import escaped stage: {module_name} -> {actual}")
    loaded[module_name] = str(actual)
config_module = importlib.import_module("live.config")
parsed = config_module.load_config(stage / "live/config.yaml")
if getattr(parsed.strategy, "dynamic_fill_hazard_action_enabled") is not False:
    raise SystemExit("candidate preflight enabled q90 action")
if getattr(parsed.strategy, "buy_fill_selection_shadow_enabled") is not False:
    raise SystemExit("candidate preflight enabled BUY selector shadow")
if getattr(parsed.strategy, "buy_fill_selection_live_enabled") is not False:
    raise SystemExit("candidate preflight enabled BUY selector action")
print(json.dumps({
    "schema_version": "warmup_before_websocket_remote_stage_validation.v1",
    "remote_manifest_file_sha256": expected_manifest_file,
    "remote_canonical_manifest_sha256": expected_manifest,
    "validated_file_count": len(records),
    "remote_import_passed": True,
    "candidate_preflight_passed": True,
    "candidate_journal_enabled": False,
    "strategy_config_semantics_unchanged": True,
    "loaded_module_paths": loaded,
}, sort_keys=True))
""".strip()


def _quiescence_probe_source() -> str:
    return r"""
import json, os, pathlib, sys
root = pathlib.Path(sys.argv[1]).resolve()
sys.path.insert(0, str(root))
processes = []
for item in pathlib.Path("/proc").iterdir():
    if not item.name.isdigit() or int(item.name) == os.getpid():
        continue
    try:
        cmdline = (item / "cmdline").read_bytes().replace(b"\0", b" ").decode()
    except OSError:
        continue
    if "live/main.py" in cmdline and "--config" in cmdline:
        processes.append({"pid": int(item.name), "cmdline": cmdline})
if processes:
    raise SystemExit(f"maker process remains after controlled stop: {processes}")
from live.config import load_config
from live.main import create_rest_client
cfg = load_config(root / "live/config.yaml")
rest = create_rest_client(cfg, dry_run=False)
orders = rest.get_orders(symbol=cfg.symbol)
if not isinstance(orders, list):
    raise SystemExit("exchange open-order audit returned a non-list")
if orders:
    raise SystemExit("exchange open orders remain after controlled stop")
print(json.dumps({
    "schema_version": "warmup_before_websocket_quiescence.v1",
    "controlled_stop_quiescent": True,
    "maker_pid_count": 0,
    "exchange_open_order_count": 0,
    "symbol": cfg.symbol,
}, sort_keys=True))
""".strip()


def _atomic_deploy_source() -> str:
    return r"""
import hashlib, json, os, pathlib, shutil, sys, uuid, yaml
root = pathlib.Path(sys.argv[1]).resolve()
stage = pathlib.Path(sys.argv[2]).resolve()
backup = pathlib.Path(sys.argv[3]).resolve()
current = json.loads(sys.argv[4])
candidate = json.loads(sys.argv[5])
expected_venv = pathlib.Path(sys.argv[6]).resolve()
candidate_manifest_sha = sys.argv[7]
mutation_plan_sha = sys.argv[8]
def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
def canonical(payload):
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode()).hexdigest()
def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
def atomic_copy(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(
        f".{destination.name}.partial-{os.getpid()}-{uuid.uuid4().hex}"
    )
    shutil.copy2(source, temporary)
    with temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(temporary, destination)
    fsync_dir(destination.parent)
def exact_hashes(base, expected, label):
    observed = {}
    for logical, expected_hash in expected.items():
        unresolved = base / logical
        if unresolved.is_symlink():
            raise RuntimeError(f"{label} file must not be a symlink: {logical}")
        path = unresolved.resolve()
        if base not in path.parents or not path.is_file():
            raise RuntimeError(f"{label} file unsafe: {logical}")
        actual = sha(path)
        if actual != expected_hash:
            raise RuntimeError(f"{label} hash mismatch: {logical}")
        observed[logical] = actual
    return observed
def restore_from_backup():
    manifest_path = backup / "backup_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    claimed = manifest.pop("manifest_sha256", None)
    if claimed != canonical(manifest):
        raise RuntimeError("backup manifest canonical identity failed during local restore")
    for row in manifest["files"]:
        source = backup / "files" / row["path"]
        if sha(source) != row["current_sha256"]:
            raise RuntimeError(f"backup payload hash failed: {row['path']}")
        atomic_copy(source, root / row["path"])
    active = root / ".venv-active"
    temporary = root / f".venv-active.restore-{uuid.uuid4().hex}"
    os.symlink(manifest["active_venv_link_text"], temporary)
    os.replace(temporary, active)
    fsync_dir(root)
    exact_hashes(root, current, "restored current")
exact_hashes(root, current, "current attempt6")
exact_hashes(stage, candidate, "staged candidate")
active = root / ".venv-active"
if not active.is_symlink() or active.resolve(strict=True) != expected_venv:
    raise SystemExit("active attempt6 venv drifted before deployment")
raw = yaml.safe_load((stage / "live/config.yaml").read_text())
if not isinstance(raw, dict) or "lifecycle_journal_v2" in raw:
    raise SystemExit("candidate journal-v2 is not OFF")
strategy = raw.get("strategy", {})
if strategy.get("dynamic_fill_hazard_action_enabled") is not False:
    raise SystemExit("candidate enabled q90 action")
if strategy.get("buy_fill_selection_shadow_enabled") is not False:
    raise SystemExit("candidate enabled BUY selector shadow")
if strategy.get("buy_fill_selection_live_enabled") is not False:
    raise SystemExit("candidate enabled BUY selector action")
if backup.exists():
    raise SystemExit("deployment backup destination already exists")
backup.parent.mkdir(parents=True, exist_ok=True)
partial = backup.parent / f".{backup.name}.partial-{os.getpid()}-{uuid.uuid4().hex}"
partial.mkdir()
active_link_text = os.readlink(active)
manifest = {
    "schema_version": "warmup_before_websocket_atomic_backup.v1",
    "candidate_manifest_sha256": candidate_manifest_sha,
    "mutation_plan_identity_sha256": mutation_plan_sha,
    "active_venv_link_text": active_link_text,
    "active_venv_resolved": str(active.resolve(strict=True)),
    "files": [],
}
try:
    for logical in sorted(current):
        destination = partial / "files" / logical
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / logical, destination)
        with destination.open("rb") as handle:
            os.fsync(handle.fileno())
        manifest["files"].append({
            "path": logical,
            "current_sha256": current[logical],
            "candidate_sha256": candidate[logical],
        })
    manifest["manifest_sha256"] = canonical(manifest)
    manifest_path = partial / "backup_manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
    with manifest_path.open("rb") as handle:
        os.fsync(handle.fileno())
    for directory in sorted(
        {path.parent for path in partial.rglob("*") if path.is_file()},
        key=lambda path: len(path.parts),
        reverse=True,
    ):
        fsync_dir(directory)
    fsync_dir(partial)
    os.replace(partial, backup)
    fsync_dir(backup.parent)
    for logical in sorted(candidate):
        atomic_copy(stage / logical, root / logical)
    exact_hashes(root, candidate, "deployed candidate")
    if active.resolve(strict=True) != expected_venv:
        raise RuntimeError("active venv changed during six-file replacement")
except Exception:
    if backup.is_dir():
        restore_from_backup()
    elif partial.exists():
        shutil.rmtree(partial, ignore_errors=True)
    raise
print(json.dumps({
    "schema_version": "warmup_before_websocket_atomic_deploy.v1",
    "deployment_files_applied": True,
    "deployed_file_count": len(candidate),
    "backup_root": str(backup),
    "backup_manifest_file_sha256": sha(backup / "backup_manifest.json"),
    "backup_manifest_canonical_sha256": manifest["manifest_sha256"],
    "active_venv_target_before": str(expected_venv),
    "active_venv_target_after": str(active.resolve(strict=True)),
    "candidate_journal_enabled": False,
    "strategy_config_semantics_unchanged": True,
    "q90_action_enabled": False,
    "buy_fill_selection_enabled": False,
}, sort_keys=True))
""".strip()


def _atomic_restore_source() -> str:
    return r"""
import hashlib, json, os, pathlib, shutil, sys, uuid
root = pathlib.Path(sys.argv[1]).resolve()
backup = pathlib.Path(sys.argv[2]).resolve()
current = json.loads(sys.argv[3])
candidate = json.loads(sys.argv[4])
expected_venv = pathlib.Path(sys.argv[5]).resolve()
mutation_plan_sha = sys.argv[6]
def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
def canonical(payload):
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode()).hexdigest()
def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
def atomic_copy(source, destination):
    temporary = destination.with_name(
        f".{destination.name}.restore-{os.getpid()}-{uuid.uuid4().hex}"
    )
    shutil.copy2(source, temporary)
    with temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(temporary, destination)
    fsync_dir(destination.parent)
def observed_hashes():
    result = {}
    for logical in current:
        path = root / logical
        result[logical] = sha(path) if path.is_file() else None
    return result
active = root / ".venv-active"
if not backup.is_dir():
    observed = observed_hashes()
    if observed != current or not active.is_symlink() or active.resolve(strict=True) != expected_venv:
        raise SystemExit("backup missing and current attempt6 identity is not intact")
    print(json.dumps({
        "schema_version": "warmup_before_websocket_atomic_restore.v1",
        "rollback_not_required": True,
        "rollback_files_restored": False,
        "restored_file_count": 0,
        "active_venv_target_restored": str(expected_venv),
        "current_attempt6_identity_restored": True,
    }, sort_keys=True))
    raise SystemExit(0)
manifest_path = backup / "backup_manifest.json"
manifest = json.loads(manifest_path.read_text())
claimed = manifest.pop("manifest_sha256", None)
if claimed != canonical(manifest):
    raise SystemExit("backup manifest canonical identity mismatch")
if manifest.get("mutation_plan_identity_sha256") != mutation_plan_sha:
    raise SystemExit("backup mutation plan identity mismatch")
rows = manifest.get("files")
if not isinstance(rows, list) or {row.get("path") for row in rows} != set(current):
    raise SystemExit("backup file set mismatch")
observed = observed_hashes()
for logical, actual in observed.items():
    if actual not in {current[logical], candidate[logical]}:
        raise SystemExit(f"runtime contains neither candidate nor attempt6 hash: {logical}")
for row in rows:
    source = backup / "files" / row["path"]
    if not source.is_file() or sha(source) != current[row["path"]]:
        raise SystemExit(f"backup payload hash mismatch: {row['path']}")
for row in rows:
    atomic_copy(backup / "files" / row["path"], root / row["path"])
temporary = root / f".venv-active.restore-{uuid.uuid4().hex}"
os.symlink(manifest["active_venv_link_text"], temporary)
os.replace(temporary, active)
fsync_dir(root)
if observed_hashes() != current or active.resolve(strict=True) != expected_venv:
    raise SystemExit("attempt6 restore verification failed")
print(json.dumps({
    "schema_version": "warmup_before_websocket_atomic_restore.v1",
    "rollback_not_required": False,
    "rollback_files_restored": True,
    "restored_file_count": len(rows),
    "backup_manifest_identity_sha256": claimed,
    "active_venv_target_restored": str(active.resolve(strict=True)),
    "current_attempt6_identity_restored": True,
}, sort_keys=True))
""".strip()


def _log_checkpoint_source() -> str:
    return r"""
import json, pathlib, sys
root = pathlib.Path(sys.argv[1]).resolve()
relative = pathlib.PurePosixPath(sys.argv[2])
if relative.is_absolute() or ".." in relative.parts:
    raise SystemExit("unsafe maker log path")
path = (root / pathlib.Path(*relative.parts)).resolve()
if root not in path.parents or not path.is_file():
    raise SystemExit("maker log is missing before restart")
stat = path.stat()
print(json.dumps({
    "schema_version": "warmup_before_websocket_log_checkpoint.v1",
    "log_path": str(path),
    "inode": int(stat.st_ino),
    "offset": int(stat.st_size),
}, sort_keys=True))
""".strip()


def _startup_stability_source() -> str:
    return r"""
import json, os, pathlib, sys, time
root = pathlib.Path(sys.argv[1]).resolve()
expected_pid = int(sys.argv[2])
window_s = float(sys.argv[3])
bucket_s = float(sys.argv[4])
minimum_buckets = int(sys.argv[5])
checkpoint = json.loads(sys.argv[6])
required_markers = json.loads(sys.argv[7])
forbidden_markers = json.loads(sys.argv[8])
if expected_pid <= 0 or window_s < 25.0 or bucket_s <= 0 or minimum_buckets < 2:
    raise SystemExit("invalid startup stability inputs")
log_path = pathlib.Path(checkpoint["log_path"]).resolve()
if root not in log_path.parents:
    raise SystemExit("maker log checkpoint escaped root")
def maker_processes():
    rows = []
    for item in pathlib.Path("/proc").iterdir():
        if not item.name.isdigit() or int(item.name) == os.getpid():
            continue
        try:
            cmdline = (item / "cmdline").read_bytes().replace(b"\0", b" ").decode()
        except OSError:
            continue
        if "live/main.py" in cmdline and "--config" in cmdline:
            rows.append({"pid": int(item.name), "cmdline": cmdline})
    return sorted(rows, key=lambda row: row["pid"])
started = time.monotonic()
samples = []
while True:
    elapsed = time.monotonic() - started
    processes = maker_processes()
    samples.append({"elapsed_s": elapsed, "processes": processes})
    if elapsed >= window_s:
        break
    time.sleep(min(0.25, max(0.0, window_s - elapsed)))
observed_duration_s = time.monotonic() - started
stat = log_path.stat()
log_identity_stable = int(stat.st_ino) == int(checkpoint["inode"])
appended = ""
if log_identity_stable and int(stat.st_size) >= int(checkpoint["offset"]):
    with log_path.open("rb") as handle:
        handle.seek(int(checkpoint["offset"]))
        appended = handle.read().decode(errors="replace")
normalized = appended.lower()
positions = [normalized.find(str(marker).lower()) for marker in required_markers]
ordered = all(position >= 0 for position in positions) and positions == sorted(positions)
forbidden_hits = sorted(
    str(marker) for marker in forbidden_markers if str(marker).lower() in normalized
)
same_pid = all(
    len(sample["processes"]) == 1 and sample["processes"][0]["pid"] == expected_pid
    for sample in samples
)
print(json.dumps({
    "schema_version": "warmup_before_websocket_startup_stability.v1",
    "expected_maker_pid": expected_pid,
    "stability_window_s": window_s,
    "observed_duration_s": observed_duration_s,
    "canonical_bucket_s": bucket_s,
    "minimum_stable_buckets": minimum_buckets,
    "covered_canonical_buckets": observed_duration_s / bucket_s,
    "same_maker_pid": same_pid,
    "sample_count": len(samples),
    "first_processes": samples[0]["processes"],
    "last_processes": samples[-1]["processes"],
    "log_identity_stable": log_identity_stable,
    "required_ordered_log_markers": required_markers,
    "required_marker_positions": positions,
    "startup_log_order_passed": ordered,
    "forbidden_log_hits": forbidden_hits,
    "fatal_or_duplicate_grid_absent": not forbidden_hits,
}, sort_keys=True))
""".strip()


def _source_hashes() -> dict[str, str]:
    sources = {
        "runtime_probe": _runtime_probe_source(),
        "prepare_stage": _prepare_stage_source(),
        "stage_validation": _stage_validation_source(),
        "quiescence_probe": _quiescence_probe_source(),
        "atomic_deploy": _atomic_deploy_source(),
        "atomic_restore": _atomic_restore_source(),
        "log_checkpoint": _log_checkpoint_source(),
        "startup_stability": _startup_stability_source(),
    }
    return {
        name: hashlib.sha256(source.encode("utf-8")).hexdigest() for name, source in sources.items()
    }


def _runtime_probe_command(
    *,
    remote: str,
    remote_root: str,
    python: str,
    expected_hashes: Mapping[str, str],
    expected_venv: str,
    expected_pid: int,
    wait_s: float,
) -> list[str]:
    return _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            python,
            _runtime_probe_source(),
            remote_root,
            json.dumps(dict(expected_hashes), sort_keys=True),
            expected_venv,
            str(expected_pid),
            str(wait_s),
        ),
    )


def _stability_command(
    *,
    plan: Mapping[str, Any],
    expected_pid: int,
    checkpoint: Mapping[str, Any],
) -> list[str]:
    contract = plan["startup_stability_contract"]
    return _ssh_command(
        str(plan["remote"]),
        str(plan["remote_root"]),
        _remote_python_command(
            str(plan["current_venv"]) + "/bin/python",
            _startup_stability_source(),
            str(plan["remote_root"]),
            str(expected_pid),
            str(contract["stability_window_s"]),
            str(contract["canonical_bucket_s"]),
            str(contract["minimum_stable_buckets"]),
            json.dumps(dict(checkpoint), sort_keys=True),
            json.dumps(contract["required_ordered_log_markers"]),
            json.dumps(contract["forbidden_log_markers"]),
        ),
    )


def build_plan(
    *,
    bound: Mapping[str, Any],
    remote: str = DEFAULT_REMOTE,
    remote_root: str = DEFAULT_REMOTE_ROOT,
    deployment_instance_id: str | None = None,
    stability_window_s: float = MINIMUM_STABILITY_WINDOW_S,
) -> dict[str, Any]:
    """Build a deterministic no-execution deployment plan."""

    remote_root = _validate_remote_path(remote_root, "remote_root")
    if remote_root != DEFAULT_REMOTE_ROOT:
        raise ValueError("remote_root must equal the frozen production repository root")
    instance = _validate_instance_id(deployment_instance_id, bound)
    stability_window_s = _validate_stability_window(stability_window_s)
    stage_root = f"{remote_root}/.releases/warmup_before_websocket_baseline/{instance}"
    payload_root = f"{stage_root}/payload"
    backup_root = f"{remote_root}/deploy_backups/warmup_before_websocket/{instance}"
    current_venv = str(bound["current_venv"])
    current_python = current_venv + "/bin/python"
    stability_contract = {
        "schema_version": STABILITY_SCHEMA_VERSION,
        "stability_window_s": stability_window_s,
        "canonical_bucket_s": CANONICAL_BUCKET_S,
        "minimum_stable_buckets": MINIMUM_STABLE_BUCKETS,
        "required_ordered_log_markers": list(REQUIRED_ORDERED_LOG_MARKERS),
        "forbidden_log_markers": list(FORBIDDEN_LOG_MARKERS),
        "log_path": MAKER_LOG_RELATIVE,
        "same_pid_required": True,
        "post_window_runtime_hash_recheck_required": True,
    }
    source_hashes = _source_hashes()
    mutation_plan = {
        "schema_version": "warmup_before_websocket_mutation_plan.v1",
        "deployment_instance_id": instance,
        "candidate_manifest_sha256": bound["candidate_manifest_sha256"],
        "candidate_manifest_file_sha256": bound["candidate_manifest_file_sha256"],
        "current_release_manifest_sha256": bound["current_release_manifest_sha256"],
        "current_release_manifest_file_sha256": bound["current_release_manifest_file_sha256"],
        "current_runtime_receipt_identity_sha256": bound["current_runtime_receipt_identity_sha256"],
        "current_runtime_receipt_file_sha256": bound["current_runtime_receipt_file_sha256"],
        "expected_current_pid": bound["current_pid"],
        "expected_current_venv": current_venv,
        "current_target_hashes": dict(bound["current_hashes"]),
        "candidate_target_hashes": dict(bound["candidate_hashes"]),
        "target_paths": list(TARGET_PATHS),
        "remote": remote,
        "remote_root": remote_root,
        "stage_root": stage_root,
        "backup_root": backup_root,
        "startup_contract": STARTUP_CONTRACT,
        "startup_stability_contract": stability_contract,
        "source_hashes": source_hashes,
        "candidate_journal_enabled": False,
        "strategy_config_semantics_unchanged": True,
        "active_venv_changed": False,
    }
    mutation_plan_identity = _canonical_sha256(mutation_plan)
    token_binding = {
        "schema_version": "warmup_before_websocket_owner_token_binding.v1",
        "candidate_manifest_sha256": bound["candidate_manifest_sha256"],
        "current_runtime_receipt_identity_sha256": bound["current_runtime_receipt_identity_sha256"],
        "mutation_plan_identity_sha256": mutation_plan_identity,
    }
    owner_token = "OWNER_CONFIRMED_WARMUP_BEFORE_WEBSOCKET_BASELINE_DEPLOY:" + _canonical_sha256(
        token_binding
    )

    current_probe = _runtime_probe_command(
        remote=remote,
        remote_root=remote_root,
        python=current_python,
        expected_hashes=bound["current_hashes"],
        expected_venv=current_venv,
        expected_pid=int(bound["current_pid"]),
        wait_s=0.0,
    )
    candidate_probe = _runtime_probe_command(
        remote=remote,
        remote_root=remote_root,
        python=current_python,
        expected_hashes=bound["candidate_hashes"],
        expected_venv=current_venv,
        expected_pid=0,
        wait_s=5.0,
    )
    restored_probe = _runtime_probe_command(
        remote=remote,
        remote_root=remote_root,
        python=current_python,
        expected_hashes=bound["current_hashes"],
        expected_venv=current_venv,
        expected_pid=0,
        wait_s=5.0,
    )
    prepare_stage = _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            current_python,
            _prepare_stage_source(),
            stage_root,
            remote_root,
        ),
    )
    rsync = [
        "rsync",
        "-a",
        "--delete",
        "--protect-args",
        str(Path(bound["candidate_root"])) + "/",
        f"{remote}:{payload_root}/",
    ]
    stage_validate = _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            current_python,
            _stage_validation_source(),
            payload_root,
            remote_root,
            json.dumps(bound["candidate_hashes"], sort_keys=True),
            str(bound["candidate_manifest_sha256"]),
            str(bound["candidate_manifest_file_sha256"]),
            json.dumps(EXPECTED_ACTION_ENABLEMENT, sort_keys=True),
        ),
    )
    stop = _ssh_command(remote, remote_root, "bash live/run.sh stop")
    start = _ssh_command(remote, remote_root, "bash live/run.sh start")
    quiescence = _ssh_command(
        remote,
        remote_root,
        _remote_env_python_command(
            remote_root=remote_root,
            python=current_python,
            source=_quiescence_probe_source(),
            arguments=(remote_root,),
        ),
    )
    log_checkpoint = _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            current_python,
            _log_checkpoint_source(),
            remote_root,
            MAKER_LOG_RELATIVE,
        ),
    )
    deploy = _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            current_python,
            _atomic_deploy_source(),
            remote_root,
            payload_root,
            backup_root,
            json.dumps(bound["current_hashes"], sort_keys=True),
            json.dumps(bound["candidate_hashes"], sort_keys=True),
            current_venv,
            str(bound["candidate_manifest_sha256"]),
            mutation_plan_identity,
        ),
    )
    restore = _ssh_command(
        remote,
        remote_root,
        _remote_python_command(
            current_python,
            _atomic_restore_source(),
            remote_root,
            backup_root,
            json.dumps(bound["current_hashes"], sort_keys=True),
            json.dumps(bound["candidate_hashes"], sort_keys=True),
            current_venv,
            mutation_plan_identity,
        ),
    )
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "mode": "plan_only_no_ssh_no_mutation",
        "execution_performed": False,
        "ssh_executed": False,
        "production_mutation_performed": False,
        "deployment_authorized": False,
        "deployment_instance_id": instance,
        "candidate_manifest_path": str(bound["candidate_manifest_path"]),
        "candidate_manifest_sha256": bound["candidate_manifest_sha256"],
        "current_release_manifest_path": str(bound["current_release_manifest_path"]),
        "current_release_manifest_sha256": bound["current_release_manifest_sha256"],
        "current_runtime_receipt_path": str(bound["current_runtime_receipt_path"]),
        "current_runtime_receipt_identity_sha256": bound["current_runtime_receipt_identity_sha256"],
        "current_pid": bound["current_pid"],
        "current_venv": current_venv,
        "remote": remote,
        "remote_root": remote_root,
        "stage_root": stage_root,
        "payload_root": payload_root,
        "backup_root": backup_root,
        "current_target_hashes": dict(bound["current_hashes"]),
        "candidate_target_hashes": dict(bound["candidate_hashes"]),
        "candidate_journal_enabled": False,
        "strategy_config_semantics_unchanged": True,
        "startup_contract": STARTUP_CONTRACT,
        "startup_stability_contract": stability_contract,
        "source_hashes": source_hashes,
        "mutation_plan": mutation_plan,
        "mutation_plan_identity_sha256": mutation_plan_identity,
        "owner_confirmation_token_binding": token_binding,
        "owner_confirmation_token": owner_token,
        "stages": {
            "stage-validate": {
                "remote_scope": "isolated_staging_only",
                "production_mutation": False,
                "commands": [current_probe, prepare_stage, rsync, stage_validate],
            },
            "deploy": {
                "production_mutation": True,
                "owner_token_required": True,
                "staging_receipt_required": True,
                "commands": {
                    "current_runtime_probe": current_probe,
                    "log_checkpoint": log_checkpoint,
                    "stop": stop,
                    "quiescence": quiescence,
                    "atomic_deploy": deploy,
                    "start": start,
                    "candidate_runtime_probe": candidate_probe,
                },
            },
            "automatic-restore": {
                "mandatory_after_any_post_stop_failure": True,
                "commands": {
                    "stop": stop,
                    "quiescence": quiescence,
                    "atomic_restore": restore,
                    "log_checkpoint": log_checkpoint,
                    "start": start,
                    "restored_runtime_probe": restored_probe,
                },
            },
        },
    }
    plan["plan_identity_sha256"] = _canonical_sha256(plan)
    return plan


def _seal_receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    sealed = dict(payload)
    sealed["receipt_identity_sha256"] = _canonical_sha256(sealed)
    return sealed


def _validate_runtime_probe(
    probe: Mapping[str, Any],
    *,
    expected_hashes: Mapping[str, str],
    expected_venv: str,
    expected_pid: int | None,
    expected_journal_enabled: bool,
) -> dict[str, bool]:
    gates = {
        "schema_valid": probe.get("schema_version")
        == "warmup_before_websocket_current_runtime_probe.v1",
        "target_hashes_exact": probe.get("runtime_files") == dict(expected_hashes),
        "active_venv_exact": probe.get("active_venv") == expected_venv,
        "python_prefix_exact": probe.get("python_prefix") == expected_venv,
        "single_pid_bound": isinstance(probe.get("maker_pid"), int) and int(probe["maker_pid"]) > 0,
        "expected_pid_exact": expected_pid is None
        or int(probe.get("maker_pid", -1)) == int(expected_pid),
        "journal_state_exact": probe.get("journal_enabled") is expected_journal_enabled,
        "strategy_flags_exact": probe.get("strategy_flags") == EXPECTED_ACTION_ENABLEMENT,
    }
    if not all(gates.values()):
        raise RuntimeError(
            "runtime identity probe failed: "
            + json.dumps({"gates": gates, "probe": dict(probe)}, sort_keys=True)
        )
    return gates


def _validate_stability(
    observation: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    expected_pid: int,
) -> dict[str, bool]:
    contract = plan["startup_stability_contract"]
    observed_duration = float(observation.get("observed_duration_s", -1.0))
    gates = {
        "schema_valid": observation.get("schema_version") == STABILITY_SCHEMA_VERSION,
        "window_exact": float(observation.get("stability_window_s", -1.0))
        == float(contract["stability_window_s"]),
        "minimum_25s_observed": observed_duration >= MINIMUM_STABILITY_WINDOW_S
        and observed_duration >= float(contract["stability_window_s"]),
        "two_canonical_buckets_observed": float(observation.get("covered_canonical_buckets", -1.0))
        >= MINIMUM_STABLE_BUCKETS,
        "same_pid_stable": observation.get("same_maker_pid") is True
        and int(observation.get("expected_maker_pid", -1)) == expected_pid,
        "log_identity_stable": observation.get("log_identity_stable") is True,
        "startup_log_order_passed": observation.get("startup_log_order_passed") is True,
        "required_markers_exact": observation.get("required_ordered_log_markers")
        == list(REQUIRED_ORDERED_LOG_MARKERS),
        "forbidden_markers_absent": observation.get("forbidden_log_hits") == []
        and observation.get("fatal_or_duplicate_grid_absent") is True,
    }
    if not all(gates.values()):
        raise RuntimeError(
            "startup stability contract failed: "
            + json.dumps({"gates": gates, "observation": dict(observation)}, sort_keys=True)
        )
    return gates


def _default_runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, capture_output=True, text=True)


def _run_json(command: Sequence[str], runner: CommandRunner) -> dict[str, Any]:
    completed = runner(command)
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed rc={completed.returncode}: {shlex.join(command)}\n"
            f"stdout={completed.stdout}\nstderr={completed.stderr}"
        )
    lines = completed.stdout.strip().splitlines()
    if not lines:
        raise RuntimeError("command produced no machine-readable JSON")
    payload = json.loads(lines[-1])
    if not isinstance(payload, dict):
        raise RuntimeError("command did not produce a JSON object")
    return payload


def _run_plain(command: Sequence[str], runner: CommandRunner) -> dict[str, Any]:
    completed = runner(command)
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed rc={completed.returncode}: {shlex.join(command)}\n"
            f"stdout={completed.stdout}\nstderr={completed.stderr}"
        )
    return {
        "command": list(command),
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "returncode": completed.returncode,
    }


def execute_stage_validation(
    *,
    plan: Mapping[str, Any],
    bound: Mapping[str, Any],
    runner: CommandRunner = _default_runner,
) -> dict[str, Any]:
    commands = plan["stages"]["stage-validate"]["commands"]
    current_probe = _run_json(commands[0], runner)
    current_gates = _validate_runtime_probe(
        current_probe,
        expected_hashes=bound["current_hashes"],
        expected_venv=str(bound["current_venv"]),
        expected_pid=int(bound["current_pid"]),
        expected_journal_enabled=True,
    )
    stage_prepare = _run_json(commands[1], runner)
    rsync = _run_plain(commands[2], runner)
    stage_validation = _run_json(commands[3], runner)
    required_stage = {
        "remote_import_passed": True,
        "candidate_preflight_passed": True,
        "candidate_journal_enabled": False,
        "strategy_config_semantics_unchanged": True,
    }
    if any(stage_validation.get(key) != value for key, value in required_stage.items()):
        raise RuntimeError("remote candidate staging/preflight did not pass")
    if (
        stage_validation.get("remote_canonical_manifest_sha256")
        != bound["candidate_manifest_sha256"]
    ):
        raise RuntimeError("remote staged candidate manifest identity differs")
    if (
        stage_validation.get("remote_manifest_file_sha256")
        != bound["candidate_manifest_file_sha256"]
    ):
        raise RuntimeError("remote staged candidate manifest file hash differs")
    return _seal_receipt(
        {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
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
            "evidence": {
                "current_runtime_probe": current_probe,
                "current_runtime_gates": current_gates,
                "stage_prepare": stage_prepare,
                "rsync": rsync,
                "stage_validation": stage_validation,
            },
            "production_mutation_performed": False,
            "ssh_executed": True,
            "candidate_journal_enabled": False,
            "strategy_config_semantics_unchanged": True,
        }
    )


def _validate_staging_receipt(
    receipt: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    bound: Mapping[str, Any],
) -> None:
    _verify_claimed_identity(
        receipt,
        identity_field="receipt_identity_sha256",
        label="staging receipt",
    )
    exact = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "stage": "staging",
        "status": "passed",
        "candidate_manifest_sha256": bound["candidate_manifest_sha256"],
        "current_release_manifest_sha256": bound["current_release_manifest_sha256"],
        "current_runtime_receipt_identity_sha256": bound["current_runtime_receipt_identity_sha256"],
        "mutation_plan_identity_sha256": plan["mutation_plan_identity_sha256"],
        "plan_identity_sha256": plan["plan_identity_sha256"],
        "deployment_instance_id": plan["deployment_instance_id"],
        "remote_stage_root": plan["stage_root"],
        "production_mutation_performed": False,
        "candidate_journal_enabled": False,
        "strategy_config_semantics_unchanged": True,
    }
    for field, expected in exact.items():
        if receipt.get(field) != expected:
            raise PermissionError(f"staging receipt field mismatch: {field}")
    evidence = receipt.get("evidence")
    stage_validation = evidence.get("stage_validation") if isinstance(evidence, Mapping) else None
    if not isinstance(stage_validation, Mapping):
        raise PermissionError("staging receipt lacks remote validation evidence")
    if not (
        stage_validation.get("remote_import_passed") is True
        and stage_validation.get("candidate_preflight_passed") is True
        and stage_validation.get("candidate_journal_enabled") is False
        and stage_validation.get("strategy_config_semantics_unchanged") is True
    ):
        raise PermissionError("staging receipt is not deploy eligible")


def _require_owner_token(plan: Mapping[str, Any], *, execute: bool, token: str | None) -> None:
    if not execute:
        raise PermissionError("deploy requires --execute-production-mutation")
    expected = str(plan["owner_confirmation_token"])
    if token != expected:
        raise PermissionError("owner confirmation token mismatch")


def _execute_recovery(
    *,
    plan: Mapping[str, Any],
    bound: Mapping[str, Any],
    runner: CommandRunner,
) -> dict[str, Any]:
    commands = plan["stages"]["automatic-restore"]["commands"]
    result: dict[str, Any] = {}
    result["stop"] = _run_plain(commands["stop"], runner)
    quiescence = _run_json(commands["quiescence"], runner)
    if quiescence.get("controlled_stop_quiescent") is not True:
        raise RuntimeError("automatic restore could not establish quiescence")
    result["quiescence"] = quiescence
    restore = _run_json(commands["atomic_restore"], runner)
    if restore.get("current_attempt6_identity_restored") is not True:
        raise RuntimeError("automatic restore did not restore attempt6 identity")
    result["restore"] = restore
    checkpoint = _run_json(commands["log_checkpoint"], runner)
    result["log_checkpoint"] = checkpoint
    result["start"] = _run_plain(commands["start"], runner)
    immediate = _run_json(commands["restored_runtime_probe"], runner)
    immediate_gates = _validate_runtime_probe(
        immediate,
        expected_hashes=bound["current_hashes"],
        expected_venv=str(bound["current_venv"]),
        expected_pid=None,
        expected_journal_enabled=True,
    )
    result["immediate_runtime_probe"] = immediate
    result["immediate_runtime_gates"] = immediate_gates
    restored_pid = int(immediate["maker_pid"])
    stability = _run_json(
        _stability_command(plan=plan, expected_pid=restored_pid, checkpoint=checkpoint),
        runner,
    )
    stability_gates = _validate_stability(
        stability,
        plan=plan,
        expected_pid=restored_pid,
    )
    result["startup_stability"] = stability
    result["startup_stability_gates"] = stability_gates
    final_command = _runtime_probe_command(
        remote=str(plan["remote"]),
        remote_root=str(plan["remote_root"]),
        python=str(plan["current_venv"]) + "/bin/python",
        expected_hashes=bound["current_hashes"],
        expected_venv=str(bound["current_venv"]),
        expected_pid=restored_pid,
        wait_s=0.0,
    )
    final_probe = _run_json(final_command, runner)
    final_gates = _validate_runtime_probe(
        final_probe,
        expected_hashes=bound["current_hashes"],
        expected_venv=str(bound["current_venv"]),
        expected_pid=restored_pid,
        expected_journal_enabled=True,
    )
    result["stable_runtime_probe"] = final_probe
    result["stable_runtime_gates"] = final_gates
    result["automatic_restore_succeeded"] = True
    result["same_stability_contract_used"] = True
    return result


def execute_deploy_transaction(
    *,
    plan: Mapping[str, Any],
    bound: Mapping[str, Any],
    staging_receipt: Mapping[str, Any],
    execute: bool,
    owner_token: str | None,
    runner: CommandRunner = _default_runner,
) -> dict[str, Any]:
    """Execute the bounded deployment, returning a receipt on every outcome."""

    # These checks deliberately happen before the first runner/SSH invocation.
    _require_owner_token(plan, execute=execute, token=owner_token)
    _validate_staging_receipt(staging_receipt, plan=plan, bound=bound)
    owner_token_sha256 = hashlib.sha256(str(owner_token).encode("utf-8")).hexdigest()
    commands = plan["stages"]["deploy"]["commands"]
    entered_stop = False
    transaction: dict[str, Any] = {}
    try:
        pre_probe = _run_json(commands["current_runtime_probe"], runner)
        pre_gates = _validate_runtime_probe(
            pre_probe,
            expected_hashes=bound["current_hashes"],
            expected_venv=str(bound["current_venv"]),
            expected_pid=int(bound["current_pid"]),
            expected_journal_enabled=True,
        )
        transaction["pre_deploy_runtime_probe"] = pre_probe
        transaction["pre_deploy_runtime_gates"] = pre_gates
        checkpoint = _run_json(commands["log_checkpoint"], runner)
        transaction["candidate_log_checkpoint"] = checkpoint
        entered_stop = True
        transaction["stop"] = _run_plain(commands["stop"], runner)
        quiescence = _run_json(commands["quiescence"], runner)
        if quiescence.get("controlled_stop_quiescent") is not True:
            raise RuntimeError("controlled stop did not become quiescent")
        transaction["quiescence"] = quiescence
        deployment = _run_json(commands["atomic_deploy"], runner)
        if not (
            deployment.get("deployment_files_applied") is True
            and int(deployment.get("deployed_file_count", -1)) == len(TARGET_PATHS)
            and deployment.get("candidate_journal_enabled") is False
            and deployment.get("strategy_config_semantics_unchanged") is True
        ):
            raise RuntimeError("atomic deploy evidence is incomplete")
        transaction["deployment"] = deployment
        transaction["start"] = _run_plain(commands["start"], runner)
        immediate = _run_json(commands["candidate_runtime_probe"], runner)
        immediate_gates = _validate_runtime_probe(
            immediate,
            expected_hashes=bound["candidate_hashes"],
            expected_venv=str(bound["current_venv"]),
            expected_pid=None,
            expected_journal_enabled=False,
        )
        transaction["immediate_candidate_runtime_probe"] = immediate
        transaction["immediate_candidate_runtime_gates"] = immediate_gates
        candidate_pid = int(immediate["maker_pid"])
        stability = _run_json(
            _stability_command(plan=plan, expected_pid=candidate_pid, checkpoint=checkpoint),
            runner,
        )
        stability_gates = _validate_stability(
            stability,
            plan=plan,
            expected_pid=candidate_pid,
        )
        transaction["candidate_startup_stability"] = stability
        transaction["candidate_startup_stability_gates"] = stability_gates
        final_command = _runtime_probe_command(
            remote=str(plan["remote"]),
            remote_root=str(plan["remote_root"]),
            python=str(plan["current_venv"]) + "/bin/python",
            expected_hashes=bound["candidate_hashes"],
            expected_venv=str(bound["current_venv"]),
            expected_pid=candidate_pid,
            wait_s=0.0,
        )
        final_probe = _run_json(final_command, runner)
        final_gates = _validate_runtime_probe(
            final_probe,
            expected_hashes=bound["candidate_hashes"],
            expected_venv=str(bound["current_venv"]),
            expected_pid=candidate_pid,
            expected_journal_enabled=False,
        )
        transaction["stable_candidate_runtime_probe"] = final_probe
        transaction["stable_candidate_runtime_gates"] = final_gates
        return _seal_receipt(
            {
                "schema_version": EVIDENCE_SCHEMA_VERSION,
                "stage": "deploy",
                "status": "passed",
                "candidate_manifest_sha256": bound["candidate_manifest_sha256"],
                "current_release_manifest_sha256": bound["current_release_manifest_sha256"],
                "current_runtime_receipt_identity_sha256": bound[
                    "current_runtime_receipt_identity_sha256"
                ],
                "mutation_plan_identity_sha256": plan["mutation_plan_identity_sha256"],
                "plan_identity_sha256": plan["plan_identity_sha256"],
                "parent_staging_receipt_identity_sha256": staging_receipt[
                    "receipt_identity_sha256"
                ],
                "deployment_instance_id": plan["deployment_instance_id"],
                "production_mutation_performed": True,
                "automatic_restore_performed": False,
                "candidate_journal_enabled": False,
                "strategy_config_semantics_unchanged": True,
                "owner_confirmation_token_sha256": owner_token_sha256,
                "evidence": transaction,
            }
        )
    except Exception as error:
        failure = repr(error)
        if not entered_stop:
            return _seal_receipt(
                {
                    "schema_version": EVIDENCE_SCHEMA_VERSION,
                    "stage": "deploy",
                    "status": "precondition_failed_no_mutation",
                    "candidate_manifest_sha256": bound["candidate_manifest_sha256"],
                    "current_release_manifest_sha256": bound["current_release_manifest_sha256"],
                    "current_runtime_receipt_identity_sha256": bound[
                        "current_runtime_receipt_identity_sha256"
                    ],
                    "mutation_plan_identity_sha256": plan["mutation_plan_identity_sha256"],
                    "plan_identity_sha256": plan["plan_identity_sha256"],
                    "production_mutation_performed": False,
                    "automatic_restore_performed": False,
                    "owner_confirmation_token_sha256": owner_token_sha256,
                    "failure": failure,
                    "evidence": transaction,
                }
            )
        try:
            recovery = _execute_recovery(plan=plan, bound=bound, runner=runner)
            return _seal_receipt(
                {
                    "schema_version": EVIDENCE_SCHEMA_VERSION,
                    "stage": "deploy",
                    "status": "candidate_failed_attempt6_restored",
                    "candidate_manifest_sha256": bound["candidate_manifest_sha256"],
                    "current_release_manifest_sha256": bound["current_release_manifest_sha256"],
                    "current_runtime_receipt_identity_sha256": bound[
                        "current_runtime_receipt_identity_sha256"
                    ],
                    "mutation_plan_identity_sha256": plan["mutation_plan_identity_sha256"],
                    "plan_identity_sha256": plan["plan_identity_sha256"],
                    "parent_staging_receipt_identity_sha256": staging_receipt[
                        "receipt_identity_sha256"
                    ],
                    "deployment_instance_id": plan["deployment_instance_id"],
                    "production_mutation_performed": True,
                    "automatic_restore_performed": True,
                    "attempt6_restored_and_stable": True,
                    "owner_confirmation_token_sha256": owner_token_sha256,
                    "failure": failure,
                    "evidence": {"candidate_transaction": transaction, "recovery": recovery},
                }
            )
        except Exception as recovery_error:
            return _seal_receipt(
                {
                    "schema_version": EVIDENCE_SCHEMA_VERSION,
                    "stage": "deploy",
                    "status": "recovery_failed",
                    "candidate_manifest_sha256": bound["candidate_manifest_sha256"],
                    "current_release_manifest_sha256": bound["current_release_manifest_sha256"],
                    "current_runtime_receipt_identity_sha256": bound[
                        "current_runtime_receipt_identity_sha256"
                    ],
                    "mutation_plan_identity_sha256": plan["mutation_plan_identity_sha256"],
                    "plan_identity_sha256": plan["plan_identity_sha256"],
                    "production_mutation_performed": True,
                    "automatic_restore_performed": True,
                    "attempt6_restored_and_stable": False,
                    "owner_confirmation_token_sha256": owner_token_sha256,
                    "failure": failure,
                    "recovery_failure": repr(recovery_error),
                    "evidence": transaction,
                }
            )


def _write_optional(path: Path | None, payload: Mapping[str, Any]) -> None:
    if path is None:
        return
    destination = path.expanduser().resolve()
    if destination.exists():
        raise FileExistsError(f"output receipt already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.partial-{os.getpid()}")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2, ensure_ascii=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "stage",
        nargs="?",
        default="plan",
        choices=("plan", "stage-validate", "deploy"),
    )
    parser.add_argument("--candidate-manifest", type=Path, default=DEFAULT_CANDIDATE_MANIFEST)
    parser.add_argument(
        "--current-release-manifest",
        type=Path,
        default=DEFAULT_CURRENT_RELEASE_MANIFEST,
    )
    parser.add_argument(
        "--current-runtime-receipt",
        type=Path,
        default=DEFAULT_CURRENT_RUNTIME_RECEIPT,
    )
    parser.add_argument("--remote", default=DEFAULT_REMOTE)
    parser.add_argument("--remote-root", default=DEFAULT_REMOTE_ROOT)
    parser.add_argument("--deployment-instance-id")
    parser.add_argument(
        "--stability-window-s",
        type=float,
        default=MINIMUM_STABILITY_WINDOW_S,
    )
    parser.add_argument("--execute-staging-ssh", action="store_true")
    parser.add_argument("--execute-production-mutation", action="store_true")
    parser.add_argument("--owner-confirmation-token")
    parser.add_argument("--staging-receipt", type=Path)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    expected_python = (Path(__file__).resolve().parents[1] / ".venv/bin/python").resolve(
        strict=True
    )
    if Path(sys.executable).resolve() != expected_python or sys.version_info < (3, 10):
        raise RuntimeError("orchestrator must run with repository .venv Python >=3.10")
    bound = load_bound_deployment(
        args.candidate_manifest,
        args.current_release_manifest,
        args.current_runtime_receipt,
    )
    plan = build_plan(
        bound=bound,
        remote=args.remote,
        remote_root=args.remote_root,
        deployment_instance_id=args.deployment_instance_id,
        stability_window_s=args.stability_window_s,
    )
    if args.stage == "plan":
        _write_optional(args.output, plan)
        print(json.dumps(plan, sort_keys=True, indent=2))
        return 0
    if args.stage == "stage-validate":
        if not args.execute_staging_ssh:
            raise PermissionError("stage-validate requires --execute-staging-ssh")
        receipt = execute_stage_validation(plan=plan, bound=bound)
        _write_optional(args.output, receipt)
        print(json.dumps(receipt, sort_keys=True, indent=2))
        return 0
    if args.staging_receipt is None:
        raise ValueError("deploy requires --staging-receipt")
    staging_receipt = _read_object(args.staging_receipt, "staging receipt")
    receipt = execute_deploy_transaction(
        plan=plan,
        bound=bound,
        staging_receipt=staging_receipt,
        execute=args.execute_production_mutation,
        owner_token=args.owner_confirmation_token,
    )
    _write_optional(args.output, receipt)
    print(json.dumps(receipt, sort_keys=True, indent=2))
    return 0 if receipt.get("status") == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
