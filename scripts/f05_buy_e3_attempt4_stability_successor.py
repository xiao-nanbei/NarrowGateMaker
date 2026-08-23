#!/usr/bin/env python3
"""Freeze Attempt4 stability when venv and resolved Python paths are equivalent.

This is an additive successor to :mod:`scripts.f05_buy_e3_stability_receipts`.
It preserves every historical source byte and every substantive stability gate.
The sole relaxed cross-receipt condition is literal ``python_executable`` string
equality.  Different lexical paths are accepted only when they are proven to be
the same regular interpreter and the venv entrypoint is bound by ``pyvenv.cfg``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import stat
import subprocess
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from scripts import f05_buy_e3_execution_attempt as legacy_attempt
from scripts import f05_buy_e3_stability_receipts as stability

# This module intentionally composes the frozen v1 implementations through
# their private, byte-exact helpers rather than copying those contracts.
# ruff: noqa: SLF001

OWNER: Final = legacy_attempt.OWNER_IDENTITY
MANIFEST_SCHEMA: Final = (
    f"{OWNER}.compatible_execution_attempt_interpreter_equivalence_successor.v1"
)
MANIFEST_STATUS: Final = (
    "compatible_runtime_frozen_not_activated_interpreter_equivalence"
)
INTERPRETER_SCHEMA: Final = f"{OWNER}.python_interpreter_equivalence_receipt.v1"
INTERPRETER_STATUS: Final = "same_interpreter_and_venv_identity_proved"
DURABILITY_SOURCE_SCHEMA: Final = (
    f"{OWNER}.durability_concurrency_cache_stability_receipt.v2"
)
DURABILITY_SOURCE_STATUS: Final = "durability_concurrency_cache_complete"
VALIDATOR_IDENTITY: Final = (
    "scripts.f05_buy_e3_attempt4_stability_successor."
    "validate_attempt4_successor_manifest.v1"
)
CANONICAL_FIELD: Final = "canonical_execution_attempt_sha256"
INTERPRETER_CANONICAL_FIELD: Final = "canonical_receipt_sha256"

_PROBE_FIELDS: Final = frozenset(
    {
        "sys_executable",
        "python_version",
        "version_info",
        "implementation",
        "cache_tag",
        "sys_prefix",
        "base_prefix",
        "exec_prefix",
        "base_exec_prefix",
    }
)
_INTERPRETER_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "underlying_receipts",
        "lexical_provenance",
        "shared_interpreter",
        "venv_identity",
        "checks",
        "evidence_boundary",
        "permissions",
        INTERPRETER_CANONICAL_FIELD,
    }
)
_DURABILITY_FIELDS: Final = frozenset(
    {
        "schema_version",
        "identity",
        "status",
        "underlying_receipts",
        "interpreter_equivalence_receipt",
        "nodeid_manifest_sha256",
        "tested_source_manifest_sha256",
        "regression_nodeid_manifest_sha256",
        "observations",
        "checks",
        "failure_counts",
        "measurement_sha256",
        "probe_cache_namespace_sha256",
        "probe_run_manifest_sha256",
        "event_series_sha256",
        "evidence_boundary",
        "permissions",
        "canonical_receipt_sha256",
    }
)


class Attempt4SuccessorError(RuntimeError):
    """Raised when the additive Attempt4 successor fails closed."""


def _raise(label: str, exc: Exception | None = None) -> None:
    error = Attempt4SuccessorError(label)
    if exc is None:
        raise error
    raise error from exc


def _lexical_path(value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or not value.startswith("/"):
        _raise(f"{label} lexical path is not absolute")
    path = Path(os.path.abspath(os.path.expanduser(value)))
    if not path.is_file() or not os.access(path, os.X_OK):
        _raise(f"{label} lexical path is not an executable file")
    return path


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


_PROBE_PROGRAM: Final = """
import json
import platform
import sys
print(json.dumps({
    "sys_executable": sys.executable,
    "python_version": platform.python_version(),
    "version_info": list(sys.version_info[:3]),
    "implementation": sys.implementation.name,
    "cache_tag": sys.implementation.cache_tag,
    "sys_prefix": sys.prefix,
    "base_prefix": sys.base_prefix,
    "exec_prefix": sys.exec_prefix,
    "base_exec_prefix": sys.base_exec_prefix,
}, sort_keys=True, separators=(",", ":")))
""".strip()


def _probe_python(path: Path, label: str) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            [str(path), "-I", "-c", _PROBE_PROGRAM],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        _raise(f"{label} Python identity probe failed", exc)
    if completed.returncode != 0 or completed.stderr:
        _raise(f"{label} Python identity probe did not exit cleanly")
    try:
        payload = json.loads(completed.stdout)
    except (json.JSONDecodeError, TypeError) as exc:
        _raise(f"{label} Python identity probe output is malformed", exc)
    if (
        not isinstance(payload, dict)
        or set(payload) != _PROBE_FIELDS
        or not isinstance(payload["version_info"], list)
        or len(payload["version_info"]) != 3
        or any(type(value) is not int for value in payload["version_info"])
        or payload["version_info"] < [3, 10, 0]
        or any(
            not isinstance(payload[name], str) or not payload[name]
            for name in _PROBE_FIELDS - {"version_info"}
        )
    ):
        _raise(f"{label} Python identity probe contract drifted")
    return payload


def _read_pyvenv_cfg(path: Path, *, venv_root: Path, shared_real: Path) -> dict[str, Any]:
    try:
        metadata = path.lstat()
    except OSError as exc:
        _raise("pyvenv.cfg is missing", exc)
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        _raise("pyvenv.cfg must be one regular non-symlink file")
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        _raise("pyvenv.cfg cannot be read exactly", exc)
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        normalized = key.strip().lower()
        if not separator or not normalized or normalized in values:
            _raise("pyvenv.cfg fields are malformed or duplicated")
        values[normalized] = value.strip()
    required = {"home", "version", "executable", "command"}
    if not required.issubset(values):
        _raise("pyvenv.cfg does not bind home, version, executable, and command")
    try:
        configured_home = Path(values["home"]).expanduser().resolve(strict=True)
        configured_executable = Path(values["executable"]).expanduser().resolve(strict=True)
        command = shlex.split(values["command"])
    except (FileNotFoundError, ValueError) as exc:
        _raise("pyvenv.cfg identity cannot be resolved", exc)
    if configured_home != shared_real.parent or configured_executable != shared_real:
        _raise("pyvenv.cfg points to a different base interpreter")
    if len(command) < 4 or command[1:3] != ["-m", "venv"]:
        _raise("pyvenv.cfg creation command is not a Python venv command")
    try:
        command_python = Path(command[0]).expanduser().resolve(strict=True)
        command_venv = Path(command[-1]).expanduser().resolve(strict=True)
    except FileNotFoundError as exc:
        _raise("pyvenv.cfg creation command paths are missing", exc)
    if command_python != shared_real or command_venv != venv_root:
        _raise("pyvenv.cfg creation command does not bind the observed venv")
    return {
        "venv_root": str(venv_root),
        "pyvenv_cfg_path": str(path),
        "pyvenv_cfg_file_sha256": hashlib.sha256(raw).hexdigest(),
        "pyvenv_cfg_size_bytes": len(raw),
        "configured_home": str(configured_home),
        "configured_version": values["version"],
        "configured_executable": str(configured_executable),
        "creation_command": command,
    }


def _interpreter_equivalence_payload(
    harness_record: Any,
    regression_record: Any,
) -> dict[str, Any]:
    harness = harness_record.payload
    regression = regression_record.payload
    harness_path = _lexical_path(harness.get("python_executable"), "durability harness")
    regression_path = _lexical_path(
        regression.get("python_executable"), "runtime regression"
    )
    if harness_path == regression_path:
        _raise("equal lexical Python paths belong to the frozen v1 validator")
    for label, payload, lexical in (
        ("durability harness", harness, harness_path),
        ("runtime regression", regression, regression_path),
    ):
        command = payload.get("run_command")
        if not isinstance(command, list) or not command or command[0] != str(lexical):
            _raise(f"{label} run command does not preserve lexical provenance")
    try:
        harness_real = harness_path.resolve(strict=True)
        regression_real = regression_path.resolve(strict=True)
        real_metadata = harness_real.stat()
    except OSError as exc:
        _raise("Python realpath identity is missing", exc)
    if harness_real != regression_real or not stat.S_ISREG(real_metadata.st_mode):
        _raise("lexical paths do not resolve to the same regular interpreter")
    shared_file_sha = _file_sha256(harness_real)
    if (
        harness.get("python_file_sha256") != shared_file_sha
        or regression.get("python_file_sha256") != shared_file_sha
    ):
        _raise("receipt Python file SHA256 does not match the shared interpreter")
    harness_probe = _probe_python(harness_path, "durability harness")
    regression_probe = _probe_python(regression_path, "runtime regression")
    for label, probe in (
        ("durability harness", harness_probe),
        ("runtime regression", regression_probe),
    ):
        try:
            probed_real = Path(probe["sys_executable"]).expanduser().resolve(strict=True)
        except FileNotFoundError as exc:
            _raise(f"{label} sys.executable is missing", exc)
        if probed_real != harness_real:
            _raise(f"{label} probe resolved to a different interpreter")
    shared_fields = (
        "python_version",
        "version_info",
        "implementation",
        "cache_tag",
        "base_prefix",
        "base_exec_prefix",
    )
    if any(harness_probe[field] != regression_probe[field] for field in shared_fields):
        _raise("Python version, implementation, or base-prefix identity differs")
    if harness_probe["sys_prefix"] != harness_probe["base_prefix"]:
        _raise("durability harness did not execute through the base interpreter identity")
    if regression_path.parent.name != "bin":
        _raise("runtime regression lexical path is not a venv bin entrypoint")
    venv_root = regression_path.parent.parent.resolve(strict=True)
    if Path(regression_probe["sys_prefix"]).resolve(strict=True) != venv_root:
        _raise("runtime regression probe did not activate its lexical venv root")
    if Path(regression_probe["exec_prefix"]).resolve(strict=True) != venv_root:
        _raise("runtime regression exec-prefix does not match its venv root")
    venv_identity = _read_pyvenv_cfg(
        venv_root / "pyvenv.cfg",
        venv_root=venv_root,
        shared_real=harness_real,
    )
    if venv_identity["configured_version"] != harness_probe["python_version"]:
        _raise("pyvenv.cfg version differs from the shared interpreter version")
    checks = {
        "lexical_paths_distinct": True,
        "run_commands_preserve_both_lexical_paths": True,
        "same_regular_realpath": True,
        "same_recomputed_file_sha256": True,
        "same_recorded_file_sha256": True,
        "same_python_version": True,
        "same_python_implementation": True,
        "same_cache_tag": True,
        "same_base_prefix_identity": True,
        "regression_venv_activated": True,
        "pyvenv_cfg_binds_shared_interpreter": True,
        "pyvenv_cfg_binds_regression_venv_root": True,
    }
    payload = {
        "schema_version": INTERPRETER_SCHEMA,
        "identity": OWNER,
        "status": INTERPRETER_STATUS,
        "underlying_receipts": {
            "durability_harness": stability._underlying_binding(
                harness_record, "durability harness receipt"
            ),
            "runtime_regression": stability._underlying_binding(
                regression_record, "runtime regression receipt"
            ),
        },
        "lexical_provenance": {
            "durability_harness": {
                "receipt_python_executable": str(harness_path),
                "run_command_argv0": harness["run_command"][0],
                "probe": harness_probe,
            },
            "runtime_regression": {
                "receipt_python_executable": str(regression_path),
                "run_command_argv0": regression["run_command"][0],
                "probe": regression_probe,
            },
        },
        "shared_interpreter": {
            "realpath": str(harness_real),
            "file_sha256": shared_file_sha,
            "python_version": harness_probe["python_version"],
            "version_info": harness_probe["version_info"],
            "implementation": harness_probe["implementation"],
            "cache_tag": harness_probe["cache_tag"],
            "base_prefix": str(Path(harness_probe["base_prefix"]).resolve(strict=True)),
            "base_exec_prefix": str(
                Path(harness_probe["base_exec_prefix"]).resolve(strict=True)
            ),
        },
        "venv_identity": venv_identity,
        "checks": checks,
        "evidence_boundary": dict(stability.EVIDENCE_BOUNDARY),
        "permissions": dict(stability.PERMISSIONS),
    }
    return stability._with_canonical(payload)


def validate_interpreter_equivalence(
    path: Path,
    *,
    context: stability.StabilityContext,
) -> tuple[dict[str, Any], dict[str, Any], tuple[int, int]]:
    first = stability._read_private_json(path, "interpreter equivalence receipt")
    payload = first.payload
    if (
        set(payload) != _INTERPRETER_FIELDS
        or payload.get("schema_version") != INTERPRETER_SCHEMA
        or payload.get("identity") != OWNER
        or payload.get("status") != INTERPRETER_STATUS
        or payload.get("evidence_boundary") != stability.EVIDENCE_BOUNDARY
        or payload.get("permissions") != stability.PERMISSIONS
        or payload.get(INTERPRETER_CANONICAL_FIELD)
        != stability._document_sha256(payload, INTERPRETER_CANONICAL_FIELD)
    ):
        _raise("interpreter equivalence receipt identity drifted")
    underlying = stability._require_exact_mapping(
        payload.get("underlying_receipts"),
        ("durability_harness", "runtime_regression"),
        "interpreter equivalence underlying receipts",
    )
    harness = stability._revalidate_underlying_binding(
        underlying["durability_harness"], "durability harness receipt"
    )
    regression = stability._revalidate_underlying_binding(
        underlying["runtime_regression"], "runtime regression receipt"
    )
    stability._validate_durability_harness_payload(harness.payload, context.repository_root)
    stability._validate_durability_sources_at_freeze(harness.payload, context)
    stability._validate_regression(regression.path, regression.payload, context)
    if (harness.device, harness.inode) == (regression.device, regression.inode):
        _raise("interpreter equivalence underlying receipts alias one file")
    if payload != _interpreter_equivalence_payload(harness, regression):
        _raise("interpreter equivalence provenance drifted")
    second = stability._read_private_json(first.path, "interpreter equivalence receipt")
    if (
        second.raw != first.raw
        or second.payload != first.payload
        or (second.device, second.inode) != (first.device, first.inode)
    ):
        _raise("interpreter equivalence receipt changed during validation")
    return second.payload, stability._source_binding(second, "interpreter equivalence"), (
        second.device,
        second.inode,
    )


def _durability_source_payload(
    harness_record: Any,
    regression_record: Any,
    equivalence_record: Any,
) -> dict[str, Any]:
    _validate_harness_regression_coverage(
        harness_record.payload, regression_record.payload
    )
    equivalence = _interpreter_equivalence_payload(harness_record, regression_record)
    if equivalence_record.payload != equivalence:
        _raise("interpreter equivalence receipt does not bind the durability sources")
    harness = harness_record.payload
    regression = regression_record.payload
    observations = dict(harness["observations"])
    failures = dict(harness["failure_counts"])
    checks = stability._derived_durability_checks(observations, failures)
    if any(checks[name] is not True for name in stability.DURABILITY_CHECKS):
        _raise("durability harness observations fail a required gate")
    payload = {
        "schema_version": DURABILITY_SOURCE_SCHEMA,
        "identity": OWNER,
        "status": DURABILITY_SOURCE_STATUS,
        "underlying_receipts": {
            "durability_harness": stability._underlying_binding(
                harness_record, "durability harness receipt"
            ),
            "regression": stability._underlying_binding(
                regression_record, "runtime regression receipt"
            ),
        },
        "interpreter_equivalence_receipt": stability._underlying_binding(
            equivalence_record, "interpreter equivalence receipt"
        ),
        "nodeid_manifest_sha256": harness["nodeid_manifest_sha256"],
        "tested_source_manifest_sha256": harness["tested_source_manifest_sha256"],
        "regression_nodeid_manifest_sha256": regression["nodeid_manifest_sha256"],
        "observations": observations,
        "checks": checks,
        "failure_counts": failures,
        "measurement_sha256": harness["measurement_sha256"],
        "probe_cache_namespace_sha256": harness["probe_cache_namespace_sha256"],
        "probe_run_manifest_sha256": harness["probe_run_manifest_sha256"],
        "event_series_sha256": harness["event_series_sha256"],
        "evidence_boundary": dict(stability.EVIDENCE_BOUNDARY),
        "permissions": dict(stability.PERMISSIONS),
    }
    return stability._with_canonical(payload)


def _validate_harness_regression_coverage(
    harness: Mapping[str, Any], regression: Mapping[str, Any]
) -> None:
    """Preserve every frozen cross-receipt check except lexical path equality."""

    regression_nodeids = regression.get("nodeids")
    regression_tests = regression.get("test_files")
    regression_sources = regression.get("runtime_sources")
    if (
        not isinstance(regression_nodeids, list)
        or not set(harness["nodeids"]).issubset(regression_nodeids)
        or not isinstance(regression_tests, Mapping)
        or any(
            regression_tests.get(name) != sha
            for name, sha in harness["test_files"].items()
        )
        or not isinstance(regression_sources, Mapping)
        or any(
            regression_sources.get(name) != sha
            for name, sha in harness["runtime_sources"].items()
        )
        or regression.get("python_file_sha256") != harness.get("python_file_sha256")
    ):
        _raise("durability harness is not covered by regression evidence")


def _validate_durability(
    payload: Mapping[str, Any],
    context: stability.StabilityContext,
) -> None:
    if (
        set(payload) != _DURABILITY_FIELDS
        or payload.get("schema_version") != DURABILITY_SOURCE_SCHEMA
        or payload.get("identity") != OWNER
        or payload.get("status") != DURABILITY_SOURCE_STATUS
        or payload.get("evidence_boundary") != stability.EVIDENCE_BOUNDARY
        or payload.get("permissions") != stability.PERMISSIONS
        or payload.get("canonical_receipt_sha256")
        != stability._document_sha256(payload, "canonical_receipt_sha256")
    ):
        _raise("durability successor receipt identity drifted")
    receipts = stability._require_exact_mapping(
        payload.get("underlying_receipts"),
        ("durability_harness", "regression"),
        "durability underlying receipts",
    )
    harness = stability._revalidate_underlying_binding(
        receipts["durability_harness"], "durability harness receipt"
    )
    regression = stability._revalidate_underlying_binding(
        receipts["regression"], "runtime regression receipt"
    )
    equivalence_binding = payload.get("interpreter_equivalence_receipt")
    equivalence_record = stability._revalidate_underlying_binding(
        equivalence_binding, "interpreter equivalence receipt"
    )
    validate_interpreter_equivalence(equivalence_record.path, context=context)
    identities = {
        (harness.device, harness.inode),
        (regression.device, regression.inode),
        (equivalence_record.device, equivalence_record.inode),
    }
    if len(identities) != 3:
        _raise("durability successor provenance aliases one file")
    stability._validate_durability_harness_payload(harness.payload, context.repository_root)
    stability._validate_durability_sources_at_freeze(harness.payload, context)
    stability._validate_regression(regression.path, regression.payload, context)
    _validate_harness_regression_coverage(harness.payload, regression.payload)
    if payload != _durability_source_payload(harness, regression, equivalence_record):
        _raise("durability successor provenance contract drifted")


def validate_source_receipt(
    role: str,
    path: Path,
    context: stability.StabilityContext,
) -> tuple[dict[str, Any], dict[str, Any], tuple[int, int]]:
    if role != "durability_concurrency_cache":
        try:
            return stability.validate_source_receipt(role, path, context)
        except Exception as exc:
            _raise(f"{role} source receipt failed frozen validation", exc)
    first = stability._read_private_json(path, f"{role} source receipt")
    _validate_durability(first.payload, context)
    second = stability._read_private_json(first.path, f"{role} source receipt")
    if (
        second.raw != first.raw
        or second.payload != first.payload
        or (second.device, second.inode) != (first.device, first.inode)
    ):
        _raise("durability successor source changed during validation")
    return second.payload, stability._source_binding(second, role), (second.device, second.inode)


def _validate_wrapper_record(
    path: Path,
    *,
    role: str,
    context: stability.StabilityContext,
) -> tuple[dict[str, Any], tuple[int, int], tuple[int, int]]:
    wrapper = stability._read_private_json(path, f"{role} wrapper")
    payload = wrapper.payload
    if (
        set(payload) != stability._WRAPPER_FIELDS
        or payload.get("schema_version") != stability.WRAPPER_SCHEMA
        or payload.get("identity") != OWNER
        or payload.get("role") != role
        or payload.get("status") != stability.WRAPPER_STATUS
        or payload.get("evidence_boundary") != stability.EVIDENCE_BOUNDARY
        or payload.get("permissions") != stability.PERMISSIONS
        or payload.get("canonical_receipt_sha256")
        != stability._document_sha256(payload, "canonical_receipt_sha256")
    ):
        _raise(f"{role} wrapper identity drifted")
    binding = payload.get("source_receipt")
    if not isinstance(binding, Mapping) or set(binding) != stability._SOURCE_BINDING_FIELDS:
        _raise(f"{role} source binding fields drifted")
    _source, observed, source_identity = validate_source_receipt(
        role, Path(str(binding.get("path", ""))), context
    )
    if dict(binding) != observed:
        _raise(f"{role} source receipt bytes drifted")
    wrapper_identity = (wrapper.device, wrapper.inode)
    if wrapper_identity == source_identity:
        _raise(f"{role} wrapper aliases its source receipt")
    return payload, wrapper_identity, source_identity


def validate_stability_wrappers(
    *,
    wrappers: Mapping[str, Path],
    context: stability.StabilityContext,
) -> dict[str, dict[str, Any]]:
    paths = stability._normalized_role_paths(wrappers, label="wrapper")
    seen: set[tuple[int, int]] = set()
    validated: dict[str, dict[str, Any]] = {}
    for role in stability.REQUIRED_ROLES:
        payload, wrapper_identity, source_identity = _validate_wrapper_record(
            paths[role], role=role, context=context
        )
        if wrapper_identity in seen or source_identity in seen:
            _raise("one wrapper or source receipt was assigned multiple roles")
        seen.update((wrapper_identity, source_identity))
        validated[role] = payload
    return validated


def materialize_and_build_stability_wrappers(
    *,
    direct_source_receipts: Mapping[str, Path],
    single_day_stage_path: Path,
    zero_economic_stage_path: Path,
    durability_harness_path: Path,
    strict_source_dir: Path,
    output_dir: Path,
    context: stability.StabilityContext,
) -> tuple[dict[str, Path], Path]:
    direct = stability._normalized_role_paths(
        direct_source_receipts,
        label="direct source receipt",
        required_roles=stability.DIRECT_SOURCE_ROLES,
    )
    strict = strict_source_dir.expanduser().absolute()
    wrappers_dir = output_dir.expanduser().absolute()
    if strict == wrappers_dir:
        _raise("strict source and wrapper directories must differ")
    strict_paths = {
        role: strict / f"{role}.json" for role in stability.MATERIALIZED_SOURCE_ROLES
    }
    equivalence_path = strict / "python_interpreter_equivalence.json"
    wrapper_paths = {
        role: wrappers_dir / f"{role}.json" for role in stability.REQUIRED_ROLES
    }
    all_outputs = [*strict_paths.values(), equivalence_path, *wrapper_paths.values()]
    if any(path.exists() or path.is_symlink() for path in all_outputs):
        _raise("one or more immutable successor output paths already exist")

    # Preflight every direct source, including Layer4, before writing any successor byte.
    seen_direct: set[tuple[int, int]] = set()
    for role in stability.DIRECT_SOURCE_ROLES:
        _payload, _binding, identity = validate_source_receipt(role, direct[role], context)
        if identity in seen_direct:
            _raise("one direct source receipt was assigned multiple roles")
        seen_direct.add(identity)
    single_day = stability._stable_legacy_record(
        single_day_stage_path,
        label="single-day mechanics stage receipt",
        validator=stability._validate_legacy_single_day,
    )
    zero_economic = stability._stable_legacy_record(
        zero_economic_stage_path,
        label="all-fold zero-economic stage receipt",
        validator=stability._validate_legacy_zero_economic,
    )
    harness = stability._stable_durability_harness_record(durability_harness_path, context)
    regression = stability._stable_regression_record(direct["regression"], context)
    identities = {
        (record.device, record.inode)
        for record in (single_day, zero_economic, harness, regression)
    }
    if len(identities) != 4 or identities.intersection(seen_direct - {(regression.device, regression.inode)}):
        _raise("underlying Attempt4 evidence aliases another role")

    _validate_harness_regression_coverage(harness.payload, regression.payload)
    equivalence_payload = _interpreter_equivalence_payload(harness, regression)
    stability._exclusive_private_json(equivalence_path, equivalence_payload)
    equivalence_record = stability._read_private_json(
        equivalence_path, "interpreter equivalence receipt"
    )
    strict_payloads = {
        "single_day": stability._single_day_source_payload(single_day),
        "all_fold_zero_economic": stability._zero_economic_source_payload(zero_economic),
        "durability_concurrency_cache": _durability_source_payload(
            harness, regression, equivalence_record
        ),
    }
    for role in stability.MATERIALIZED_SOURCE_ROLES:
        stability._exclusive_private_json(strict_paths[role], strict_payloads[role])
    sources = {**strict_paths, **direct}
    validated_sources: dict[str, tuple[dict[str, Any], dict[str, Any], tuple[int, int]]] = {}
    seen_sources: set[tuple[int, int]] = set()
    for role in stability.REQUIRED_ROLES:
        observed = validate_source_receipt(role, sources[role], context)
        if observed[2] in seen_sources:
            _raise("one source receipt was assigned multiple wrapper roles")
        seen_sources.add(observed[2])
        validated_sources[role] = observed
    for role in stability.REQUIRED_ROLES:
        stability._exclusive_private_json(
            wrapper_paths[role], stability._wrapper_payload(role, validated_sources[role][1])
        )
    validate_stability_wrappers(wrappers=wrapper_paths, context=context)
    validate_interpreter_equivalence(equivalence_path, context=context)
    return wrapper_paths, equivalence_path


def _stability_context_payload(
    *,
    repository_root: Path,
    runtime_execution: Mapping[str, Any],
    layer4_contract_path: Path,
    layer4_day_receipt_dir: Path,
) -> dict[str, Any]:
    try:
        contract = layer4_contract_path.expanduser().resolve(strict=True)
        days = layer4_day_receipt_dir.expanduser().resolve(strict=True)
    except FileNotFoundError as exc:
        _raise("stability validation context path is missing", exc)
    if not contract.is_file() or not days.is_dir():
        _raise("stability validation context shape drifted")
    return {
        "validator": VALIDATOR_IDENTITY,
        "repository_root": str(repository_root),
        "execution_commit": runtime_execution["execution_commit"],
        "execution_tag": runtime_execution["annotated_tag"],
        "layer4_contract_path": str(contract),
        "layer4_day_receipt_dir": str(days),
    }


def _context_from_payload(payload: Mapping[str, Any]) -> stability.StabilityContext:
    fields = {
        "validator",
        "repository_root",
        "execution_commit",
        "execution_tag",
        "layer4_contract_path",
        "layer4_day_receipt_dir",
    }
    if set(payload) != fields or payload.get("validator") != VALIDATOR_IDENTITY:
        _raise("stability validation context fields drifted")
    try:
        return stability.StabilityContext(
            repository_root=Path(str(payload["repository_root"])).resolve(strict=True),
            execution_commit=legacy_attempt._require_git_sha(
                payload["execution_commit"], "stability execution commit"
            ),
            execution_tag=str(payload["execution_tag"]),
            layer4_contract_path=Path(str(payload["layer4_contract_path"])).resolve(
                strict=True
            ),
            layer4_day_receipt_dir=Path(str(payload["layer4_day_receipt_dir"])).resolve(
                strict=True
            ),
        )
    except Exception as exc:
        _raise("stability validation context cannot be resolved", exc)


def _wrapper_bindings(
    wrappers: Mapping[str, Path],
    *,
    context: stability.StabilityContext,
) -> dict[str, dict[str, Any]]:
    validated = validate_stability_wrappers(wrappers=wrappers, context=context)
    result: dict[str, dict[str, Any]] = {}
    for role in stability.REQUIRED_ROLES:
        record = stability._read_private_json(Path(wrappers[role]), f"{role} wrapper")
        binding = stability._source_binding(record, f"{role} wrapper")
        if validated[role]["canonical_receipt_sha256"] != binding["canonical_sha256"]:
            _raise("wrapper validation and binding canonical identities differ")
        result[role] = binding
    return result


def build_manifest(
    *,
    repository_root: Path,
    attempt_id: str,
    annotated_tag: str,
    manifest_path: Path,
    policy_path: Path,
    predicate_bundle_path: Path,
    formal_manifest_path: Path,
    pre_admission_receipt_paths: Mapping[str, Path],
    interpreter_equivalence_path: Path,
    layer4_contract_path: Path,
    layer4_day_receipt_dir: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    root = repository_root.expanduser().resolve(strict=True)
    normalized_attempt = legacy_attempt._require_attempt_id(attempt_id)
    legacy_attempt._require_clean_worktree(root)
    runtime = legacy_attempt._annotated_tag_identity(root, annotated_tag, require_head=True)
    if not legacy_attempt._git_is_ancestor(
        root, legacy_attempt.PRODUCER_COMMIT, runtime["execution_commit"]
    ):
        _raise("Attempt4 runtime is not a descendant of the artifact producer")
    artifact = legacy_attempt._artifact_binding(
        root=root,
        manifest_path=manifest_path,
        policy_path=policy_path,
        predicate_bundle_path=predicate_bundle_path,
        formal_manifest_path=formal_manifest_path,
    )
    runtime_sources = legacy_attempt._source_bindings(root, runtime["execution_commit"])
    context_payload = _stability_context_payload(
        repository_root=root,
        runtime_execution=runtime,
        layer4_contract_path=layer4_contract_path,
        layer4_day_receipt_dir=layer4_day_receipt_dir,
    )
    context = _context_from_payload(context_payload)
    wrapper_paths = stability._normalized_role_paths(
        pre_admission_receipt_paths, label="wrapper"
    )
    wrappers = _wrapper_bindings(wrapper_paths, context=context)
    _equivalence, equivalence_binding, _identity = validate_interpreter_equivalence(
        interpreter_equivalence_path, context=context
    )
    durability_source = Path(
        str(
            stability._read_private_json(
                wrapper_paths["durability_concurrency_cache"], "durability wrapper"
            ).payload["source_receipt"]["path"]
        )
    )
    durability = stability._read_private_json(durability_source, "durability source").payload
    if durability.get("interpreter_equivalence_receipt", {}).get(
        "canonical_sha256"
    ) != equivalence_binding["canonical_sha256"]:
        _raise("manifest equivalence binding differs from durability source")
    payload = {
        "schema_version": MANIFEST_SCHEMA,
        "identity": OWNER,
        "attempt_id": normalized_attempt,
        "status": MANIFEST_STATUS,
        "generated_utc": generated_utc
        or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "research_contract": legacy_attempt._research_contract(),
        "artifact_producer_execution": legacy_attempt._producer_identity(root),
        "runtime_execution": runtime,
        "runtime_sources": {
            "files": runtime_sources,
            "canonical_sha256": legacy_attempt.canonical_sha256(runtime_sources),
        },
        "artifact": artifact,
        "stability_validation_context": context_payload,
        "interpreter_equivalence": equivalence_binding,
        "pre_admission_evidence": wrappers,
        "successor_truth": {
            "historical_receipts_rewritten": False,
            "literal_path_equality_only_condition_replaced": True,
            "all_other_stability_gates_preserved": True,
            "new_economic_arm_run": False,
        },
        "permissions": dict(legacy_attempt.ATTEMPT_PERMISSIONS),
        "evidence_boundary": dict(legacy_attempt.ATTEMPT_EVIDENCE_BOUNDARY),
    }
    payload[CANONICAL_FIELD] = legacy_attempt.canonical_sha256(payload)
    return payload


def validate_manifest(
    path: Path,
    *,
    repository_root: Path,
    require_current_checkout: bool = True,
) -> dict[str, Any]:
    root = repository_root.expanduser().resolve(strict=True)
    payload = legacy_attempt._read_private_json(path, "Attempt4 successor manifest")
    canonical = legacy_attempt._require_sha256(
        payload.get(CANONICAL_FIELD), "Attempt4 successor manifest canonical hash"
    )
    body = dict(payload)
    body.pop(CANONICAL_FIELD, None)
    fields = {
        "schema_version",
        "identity",
        "attempt_id",
        "status",
        "generated_utc",
        "research_contract",
        "artifact_producer_execution",
        "runtime_execution",
        "runtime_sources",
        "artifact",
        "stability_validation_context",
        "interpreter_equivalence",
        "pre_admission_evidence",
        "successor_truth",
        "permissions",
        "evidence_boundary",
        CANONICAL_FIELD,
    }
    if legacy_attempt.canonical_sha256(body) != canonical or set(payload) != fields:
        _raise("Attempt4 successor manifest fields or canonical hash drifted")
    if (
        payload.get("schema_version") != MANIFEST_SCHEMA
        or payload.get("identity") != OWNER
        or payload.get("status") != MANIFEST_STATUS
        or legacy_attempt._require_attempt_id(payload.get("attempt_id"))
        != payload.get("attempt_id")
        or payload.get("research_contract") != legacy_attempt._research_contract()
        or payload.get("artifact_producer_execution")
        != legacy_attempt._producer_identity(root)
        or payload.get("successor_truth")
        != {
            "historical_receipts_rewritten": False,
            "literal_path_equality_only_condition_replaced": True,
            "all_other_stability_gates_preserved": True,
            "new_economic_arm_run": False,
        }
        or payload.get("permissions") != legacy_attempt.ATTEMPT_PERMISSIONS
        or payload.get("evidence_boundary") != legacy_attempt.ATTEMPT_EVIDENCE_BOUNDARY
    ):
        _raise("Attempt4 successor semantic identity drifted")
    if require_current_checkout:
        legacy_attempt._require_clean_worktree(root)
    runtime = payload.get("runtime_execution")
    if not isinstance(runtime, Mapping):
        _raise("Attempt4 successor runtime execution is missing")
    observed_runtime = legacy_attempt._annotated_tag_identity(
        root,
        str(runtime.get("annotated_tag", "")),
        require_head=require_current_checkout,
    )
    if dict(runtime) != observed_runtime or not legacy_attempt._git_is_ancestor(
        root, legacy_attempt.PRODUCER_COMMIT, observed_runtime["execution_commit"]
    ):
        _raise("Attempt4 successor runtime Git identity drifted")
    sources = legacy_attempt._source_bindings(root, observed_runtime["execution_commit"])
    if payload.get("runtime_sources") != {
        "files": sources,
        "canonical_sha256": legacy_attempt.canonical_sha256(sources),
    }:
        _raise("Attempt4 successor runtime sources drifted")
    artifact = payload.get("artifact")
    if not isinstance(artifact, Mapping):
        _raise("Attempt4 successor artifact binding is missing")
    observed_artifact = legacy_attempt._artifact_binding(
        root=root,
        manifest_path=Path(str(artifact.get("files", {}).get("manifest", {}).get("path", ""))),
        policy_path=Path(str(artifact.get("files", {}).get("policy", {}).get("path", ""))),
        predicate_bundle_path=Path(
            str(artifact.get("files", {}).get("predicate_bundle", {}).get("path", ""))
        ),
        formal_manifest_path=Path(str(artifact.get("formal_manifest", {}).get("path", ""))),
    )
    if dict(artifact) != observed_artifact:
        _raise("Attempt4 successor artifact binding drifted")
    context_payload = payload.get("stability_validation_context")
    if not isinstance(context_payload, Mapping):
        _raise("Attempt4 successor stability context is missing")
    expected_context = _stability_context_payload(
        repository_root=root,
        runtime_execution=observed_runtime,
        layer4_contract_path=Path(str(context_payload.get("layer4_contract_path", ""))),
        layer4_day_receipt_dir=Path(str(context_payload.get("layer4_day_receipt_dir", ""))),
    )
    if dict(context_payload) != expected_context:
        _raise("Attempt4 successor stability context drifted")
    context = _context_from_payload(expected_context)
    raw_bindings = payload.get("pre_admission_evidence")
    if not isinstance(raw_bindings, Mapping) or set(raw_bindings) != set(
        stability.REQUIRED_ROLES
    ):
        _raise("Attempt4 successor wrapper role set drifted")
    wrapper_paths = {
        role: Path(str(raw_bindings[role].get("path", "")))
        for role in stability.REQUIRED_ROLES
        if isinstance(raw_bindings[role], Mapping)
    }
    if len(wrapper_paths) != len(stability.REQUIRED_ROLES):
        _raise("Attempt4 successor wrapper binding is malformed")
    rebound = _wrapper_bindings(wrapper_paths, context=context)
    if dict(raw_bindings) != rebound:
        _raise("Attempt4 successor wrapper bytes drifted")
    equivalence_binding = payload.get("interpreter_equivalence")
    if not isinstance(equivalence_binding, Mapping):
        _raise("Attempt4 successor interpreter equivalence binding is missing")
    _equivalence, observed_binding, _identity = validate_interpreter_equivalence(
        Path(str(equivalence_binding.get("path", ""))), context=context
    )
    if dict(equivalence_binding) != observed_binding:
        _raise("Attempt4 successor interpreter equivalence bytes drifted")
    durability_wrapper = stability._read_private_json(
        wrapper_paths["durability_concurrency_cache"], "durability wrapper"
    ).payload
    durability_source = stability._read_private_json(
        Path(str(durability_wrapper["source_receipt"]["path"])), "durability source"
    ).payload
    if durability_source.get("interpreter_equivalence_receipt", {}).get(
        "canonical_sha256"
    ) != observed_binding["canonical_sha256"]:
        _raise("Attempt4 manifest and durability source equivalence bindings differ")
    return payload


def materialize_and_freeze(
    *,
    repository_root: Path,
    attempt_id: str,
    annotated_tag: str,
    manifest_path: Path,
    policy_path: Path,
    predicate_bundle_path: Path,
    formal_manifest_path: Path,
    direct_source_receipts: Mapping[str, Path],
    single_day_stage_path: Path,
    zero_economic_stage_path: Path,
    durability_harness_path: Path,
    strict_source_dir: Path,
    wrapper_dir: Path,
    output_manifest_path: Path,
    layer4_contract_path: Path,
    layer4_day_receipt_dir: Path,
) -> tuple[dict[str, Any], str]:
    root = repository_root.expanduser().resolve(strict=True)
    runtime = legacy_attempt._annotated_tag_identity(root, annotated_tag, require_head=True)
    context = stability.StabilityContext(
        repository_root=root,
        execution_commit=runtime["execution_commit"],
        execution_tag=runtime["annotated_tag"],
        layer4_contract_path=layer4_contract_path,
        layer4_day_receipt_dir=layer4_day_receipt_dir,
    )
    wrappers, equivalence_path = materialize_and_build_stability_wrappers(
        direct_source_receipts=direct_source_receipts,
        single_day_stage_path=single_day_stage_path,
        zero_economic_stage_path=zero_economic_stage_path,
        durability_harness_path=durability_harness_path,
        strict_source_dir=strict_source_dir,
        output_dir=wrapper_dir,
        context=context,
    )
    payload = build_manifest(
        repository_root=root,
        attempt_id=attempt_id,
        annotated_tag=annotated_tag,
        manifest_path=manifest_path,
        policy_path=policy_path,
        predicate_bundle_path=predicate_bundle_path,
        formal_manifest_path=formal_manifest_path,
        pre_admission_receipt_paths=wrappers,
        interpreter_equivalence_path=equivalence_path,
        layer4_contract_path=layer4_contract_path,
        layer4_day_receipt_dir=layer4_day_receipt_dir,
    )
    file_sha = legacy_attempt.atomic_write(output_manifest_path, payload)
    observed = validate_manifest(
        output_manifest_path, repository_root=root, require_current_checkout=True
    )
    if observed != payload:
        _raise("Attempt4 successor manifest changed after immutable write")
    return payload, file_sha


def _parse_roles(values: Sequence[str]) -> dict[str, Path]:
    parsed: dict[str, Path] = {}
    for raw in values:
        role, separator, path = raw.partition("=")
        if not separator or not role or not path or role in parsed:
            _raise("direct source argument must be one unique role=path")
        parsed[role] = Path(path)
    return stability._normalized_role_paths(
        parsed,
        label="direct source",
        required_roles=stability.DIRECT_SOURCE_ROLES,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    materialize = subparsers.add_parser("materialize-and-freeze")
    materialize.add_argument("--repository-root", type=Path, required=True)
    materialize.add_argument("--attempt-id", required=True)
    materialize.add_argument("--annotated-tag", required=True)
    materialize.add_argument("--artifact-manifest", type=Path, required=True)
    materialize.add_argument("--policy", type=Path, required=True)
    materialize.add_argument("--predicate-bundle", type=Path, required=True)
    materialize.add_argument("--formal-manifest", type=Path, required=True)
    materialize.add_argument("--source", action="append", required=True)
    materialize.add_argument("--single-day-stage", type=Path, required=True)
    materialize.add_argument("--zero-economic-stage", type=Path, required=True)
    materialize.add_argument("--durability-harness", type=Path, required=True)
    materialize.add_argument("--strict-source-dir", type=Path, required=True)
    materialize.add_argument("--wrapper-dir", type=Path, required=True)
    materialize.add_argument("--output-manifest", type=Path, required=True)
    materialize.add_argument("--layer4-contract", type=Path, required=True)
    materialize.add_argument("--layer4-day-receipt-dir", type=Path, required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--repository-root", type=Path, required=True)
    validate.add_argument("--manifest", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "materialize-and-freeze":
        payload, file_sha = materialize_and_freeze(
            repository_root=args.repository_root,
            attempt_id=args.attempt_id,
            annotated_tag=args.annotated_tag,
            manifest_path=args.artifact_manifest,
            policy_path=args.policy,
            predicate_bundle_path=args.predicate_bundle,
            formal_manifest_path=args.formal_manifest,
            direct_source_receipts=_parse_roles(args.source),
            single_day_stage_path=args.single_day_stage,
            zero_economic_stage_path=args.zero_economic_stage,
            durability_harness_path=args.durability_harness,
            strict_source_dir=args.strict_source_dir,
            wrapper_dir=args.wrapper_dir,
            output_manifest_path=args.output_manifest,
            layer4_contract_path=args.layer4_contract,
            layer4_day_receipt_dir=args.layer4_day_receipt_dir,
        )
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "attempt_id": payload["attempt_id"],
                    "manifest_file_sha256": file_sha,
                    CANONICAL_FIELD: payload[CANONICAL_FIELD],
                    "economic_outcomes_read": False,
                    "validation_read": False,
                    "sealed_holdout_read": False,
                },
                sort_keys=True,
            )
        )
    else:
        payload = validate_manifest(args.manifest, repository_root=args.repository_root)
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "attempt_id": payload["attempt_id"],
                    CANONICAL_FIELD: payload[CANONICAL_FIELD],
                },
                sort_keys=True,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
