#!/usr/bin/env python3
"""Validate and atomically admit one sealed prospective journal-v2 part."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root  # noqa: E402
from execution.order_lifecycle_journal_writer_v2 import (  # noqa: E402
    OrderLifecycleJournalWriterV2,
)
from scripts.live_remote_pointer import active_live_remote_fields  # noqa: E402
from scripts.orchestrate_prospective_lifecycle_remote_release import (  # noqa: E402
    DEPLOYMENT_BINDING_SCHEMA_VERSION,
    EVIDENCE_SCHEMA_VERSION,
    _seal_receipt,
    _validate_embedded_deployment_binding,
    _validate_stage_receipt,
    load_bound_release,
)

SCHEMA_VERSION = "prospective_lifecycle_journal_part_admission.v1"
_ACTIVE_REMOTE = active_live_remote_fields(ROOT)
DEFAULT_REMOTE = _ACTIVE_REMOTE.get("ssh_target", "")
DEFAULT_REMOTE_ROOT = os.environ.get(
    "NARROWGATE_REMOTE_ROOT",
    _ACTIVE_REMOTE.get("repo_root", str(Path.home() / ROOT.name)),
)
DEFAULT_REMOTE_PYTHON = ".venv-active/bin/python3"
DEFAULT_LOCAL_ROOT = (
    data_root(ROOT)
    / "formal_collection"
    / "prospective_lifecycle_journal_v2_roundtrip"
)


class JournalPartAdmissionError(ValueError):
    """Raised when a remote journal part cannot be admitted exactly."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _read_json(path: Path, *, role: str) -> dict[str, Any]:
    unresolved = path.expanduser()
    if unresolved.is_symlink():
        raise JournalPartAdmissionError(f"{role} must not be a symlink: {unresolved}")
    resolved = unresolved.resolve(strict=True)
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise JournalPartAdmissionError(f"invalid {role}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise JournalPartAdmissionError(f"{role} must be a JSON object")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.partial-{os.getpid()}-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def _validate_remote_session_root(remote_root: str, session_root: str) -> str:
    root = PurePosixPath(remote_root)
    session = PurePosixPath(session_root)
    journal_root = root / "formal_collection/order_lifecycle_journal_v2"
    if not session.is_absolute() or session.parent != journal_root:
        raise JournalPartAdmissionError("remote session escaped the journal-v2 root")
    if not session.name.startswith("session-prospective-") or ".." in session.parts:
        raise JournalPartAdmissionError("remote prospective session path is invalid")
    return str(session)


def _validate_exact_part_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
    batch_id = str(metadata.get("batch_id", ""))
    if re.fullmatch(r"[0-9a-f]{64}", batch_id) is None:
        raise JournalPartAdmissionError("journal batch ID is not a lowercase SHA256")
    expected_paths = {
        "runtime_identity_relative": "runtime_identity.json",
        "manifest_relative": f"parts/part-{batch_id}.manifest.json",
        "data_relative": f"parts/part-{batch_id}.parquet",
    }
    for field, expected in expected_paths.items():
        value = str(metadata.get(field, ""))
        path = PurePosixPath(value)
        if (
            value != expected
            or path.is_absolute()
            or ".." in path.parts
            or "\n" in value
            or "\r" in value
            or "\x00" in value
        ):
            raise JournalPartAdmissionError(f"unsafe or unexpected journal path: {field}")
    if int(metadata.get("first_lifecycle_sequence", -1)) != 1:
        raise JournalPartAdmissionError("single-part admission requires sequence 1")
    if int(metadata.get("checkpoint_before_last_emitted_sequence", -1)) != 0:
        raise JournalPartAdmissionError("single-part admission requires an empty prior cursor")
    if str(metadata.get("checkpoint_before_last_event_id", "")):
        raise JournalPartAdmissionError("single-part admission requires no prior event ID")
    if int(metadata.get("committed_ts_ns", 0)) <= 0:
        raise JournalPartAdmissionError("journal part committed timestamp is invalid")
    for field in (
        "manifest_sha256",
        "data_sha256",
        "runtime_identity_file_sha256",
        "runtime_identity_sha256",
        "journal_schema_sha256",
    ):
        value = str(metadata.get(field, ""))
        if re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise JournalPartAdmissionError(f"invalid journal SHA256 field: {field}")
    for field in (
        "manifest_size_bytes",
        "data_size_bytes",
        "runtime_identity_size_bytes",
        "row_count",
    ):
        if int(metadata.get(field, 0)) <= 0:
            raise JournalPartAdmissionError(f"invalid positive journal field: {field}")
    if bool(metadata.get("economic_outcomes_read")):
        raise JournalPartAdmissionError("journal part claims economic outcomes")
    return dict(metadata)


def _load_performance_context(
    performance_receipt_path: Path,
    *,
    bound: Mapping[str, Any],
) -> dict[str, Any]:
    receipt = _read_json(performance_receipt_path, role="performance receipt")
    try:
        _validate_stage_receipt(receipt, stage="performance", bound=bound)
        binding = _validate_embedded_deployment_binding(receipt)
    except ValueError as exc:
        raise JournalPartAdmissionError("performance receipt identity is invalid") from exc
    if binding.get("schema_version") != DEPLOYMENT_BINDING_SCHEMA_VERSION:
        raise JournalPartAdmissionError("performance deployment binding schema differs")
    if binding.get("release_manifest_sha256") != bound["release_manifest_sha256"]:
        raise JournalPartAdmissionError("performance deployment release identity differs")
    if binding.get("remote_identity_sha256") != bound["remote_identity_sha256"]:
        raise JournalPartAdmissionError("performance deployment baseline identity differs")
    parent_runtime = str(receipt.get("parent_runtime_receipt_identity_sha256", ""))
    if re.fullmatch(r"[0-9a-f]{64}", parent_runtime) is None:
        raise JournalPartAdmissionError("performance receipt lacks its runtime parent")
    normalization = receipt.get("runtime_receipt_normalization")
    if not isinstance(normalization, Mapping):
        raise JournalPartAdmissionError("performance receipt lacks runtime normalization")
    if normalization.get("runtime_receipt_identity_sha256") != parent_runtime:
        raise JournalPartAdmissionError("performance normalized a different runtime receipt")
    if normalization.get("deployment_binding_sha256") != receipt.get(
        "deployment_binding_sha256"
    ):
        raise JournalPartAdmissionError("performance runtime normalization binding differs")
    candidate = receipt.get("evidence", {}).get("candidate", {})
    if not isinstance(candidate, dict):
        raise JournalPartAdmissionError("performance receipt candidate body is invalid")
    session_root = candidate.get("session_root")
    if not isinstance(session_root, str) or not session_root:
        raise JournalPartAdmissionError("performance receipt lacks a session root")
    exact_part = candidate.get("exact_standalone_part")
    if not isinstance(exact_part, Mapping):
        raise JournalPartAdmissionError("performance receipt lacks an exact standalone part")
    metadata = _validate_exact_part_metadata(exact_part)
    if metadata.get("session_root") != session_root:
        raise JournalPartAdmissionError("performance part and session roots differ")
    started_ns = int(float(candidate.get("collection_started_ts", 0.0)) * 1_000_000_000)
    ended_ns = int(float(candidate.get("collection_ended_ts", 0.0)) * 1_000_000_000)
    committed_ns = int(metadata["committed_ts_ns"])
    if started_ns <= 0 or ended_ns < started_ns or not started_ns <= committed_ns <= ended_ns:
        raise JournalPartAdmissionError("journal part was not committed in the performance window")
    if metadata.get("runtime_identity_sha256") != candidate.get(
        "runtime_identity_sha256"
    ):
        raise JournalPartAdmissionError("performance part runtime identity differs")
    return {
        "receipt": receipt,
        "deployment_binding": binding,
        "deployment_binding_sha256": receipt["deployment_binding_sha256"],
        "session_root": session_root,
        "metadata": metadata,
    }


def _remote_selection_source() -> str:
    return r"""
import hashlib,json,re,sys
from pathlib import Path

session=Path(sys.argv[1])
expected=json.loads(sys.argv[2])
if not session.is_absolute() or not session.is_dir() or session.is_symlink():
    raise SystemExit("invalid session root")
parts=session/"parts"
if not parts.is_dir() or parts.is_symlink():
    raise SystemExit("invalid journal parts root")
batch_id=str(expected.get("batch_id",""))
if not re.fullmatch(r"[0-9a-f]{64}",batch_id):
    raise SystemExit("invalid exact journal batch ID")
manifest_path=session/str(expected.get("manifest_relative",""))
data_path=session/str(expected.get("data_relative",""))
if manifest_path != parts/f"part-{batch_id}.manifest.json":
    raise SystemExit("exact manifest path mismatch")
if data_path != parts/f"part-{batch_id}.parquet":
    raise SystemExit("exact data path mismatch")
if not manifest_path.is_file() or manifest_path.is_symlink():
    raise SystemExit("exact journal manifest is missing or unsafe")
if not data_path.is_file() or data_path.is_symlink():
    raise SystemExit("exact journal data is missing or unsafe")
payload=json.loads(manifest_path.read_text(encoding="utf-8"))
if not isinstance(payload,dict):
    raise SystemExit("exact journal manifest is invalid")
if str(payload.get("batch_id","")) != batch_id:
    raise SystemExit("exact journal manifest batch mismatch")
if payload.get("data_file") != data_path.name:
    raise SystemExit("exact journal data filename mismatch")
before=payload.get("checkpoint_before")
if not isinstance(before,dict):
    raise SystemExit("exact journal checkpoint-before is invalid")
if int(payload.get("first_lifecycle_sequence",-1)) != 1:
    raise SystemExit("exact journal part is not sequence 1")
if int(before.get("last_emitted_sequence",-1)) != 0 or str(before.get("last_event_id","")):
    raise SystemExit("exact journal part is not independently recoverable")
def sha(path):
    digest=hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda:handle.read(8<<20),b""):
            digest.update(chunk)
    return digest.hexdigest()
data_sha=sha(data_path)
if data_sha != str(payload.get("data_sha256","")):
    raise SystemExit("exact journal data SHA256 mismatch")
identity_path=session/"runtime_identity.json"
if not identity_path.is_file() or identity_path.is_symlink():
    raise SystemExit("missing runtime identity")
actual={
    "session_root":str(session),
    "session_id":session.name.removeprefix("session-"),
    "manifest_relative":str(manifest_path.relative_to(session)),
    "manifest_sha256":sha(manifest_path),
    "manifest_size_bytes":manifest_path.stat().st_size,
    "data_relative":str(data_path.relative_to(session)),
    "data_sha256":data_sha,
    "data_size_bytes":data_path.stat().st_size,
    "runtime_identity_relative":"runtime_identity.json",
    "runtime_identity_file_sha256":sha(identity_path),
    "runtime_identity_size_bytes":identity_path.stat().st_size,
    "batch_id":str(payload["batch_id"]),
    "row_count":int(payload["row_count"]),
    "journal_schema_sha256":str(payload["journal_schema_sha256"]),
    "runtime_identity_sha256":str(payload["runtime_identity_sha256"]),
    "first_lifecycle_sequence":int(payload["first_lifecycle_sequence"]),
    "checkpoint_before_last_emitted_sequence":int(before["last_emitted_sequence"]),
    "checkpoint_before_last_event_id":str(before["last_event_id"]),
    "committed_ts_ns":int(payload["committed_ts_ns"]),
    "economic_outcomes_read":False,
}
if actual != expected:
    raise SystemExit("remote exact journal part differs from performance receipt")
print(json.dumps(actual,sort_keys=True))
""".strip()


def select_remote_part(
    *,
    remote: str,
    remote_root: str,
    remote_python: str,
    session_root: str,
    expected_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    session_root = _validate_remote_session_root(remote_root, session_root)
    remote_command = (
        f"cd {shlex.quote(remote_root)} && {shlex.quote(remote_python)} -c "
        f"{shlex.quote(_remote_selection_source())} {shlex.quote(session_root)} "
        f"{shlex.quote(json.dumps(dict(expected_metadata), sort_keys=True))}"
    )
    completed = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", remote, remote_command],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"remote part selection failed: {completed.stdout}\n{completed.stderr}"
        )
    lines = completed.stdout.strip().splitlines()
    if not lines:
        raise RuntimeError("remote part selection produced no metadata")
    payload = json.loads(lines[-1])
    if not isinstance(payload, dict):
        raise RuntimeError("remote part selection metadata is not an object")
    return payload


def _copy_remote_files(
    *, remote: str, metadata: Mapping[str, Any], destination: Path
) -> None:
    metadata = _validate_exact_part_metadata(metadata)
    relative_paths = [
        str(metadata["runtime_identity_relative"]),
        str(metadata["manifest_relative"]),
        str(metadata["data_relative"]),
    ]
    for relative in relative_paths:
        path = PurePosixPath(relative)
        if path.is_absolute() or ".." in path.parts:
            raise JournalPartAdmissionError("remote part metadata contains an unsafe path")
    files_from = destination.parent / f".files-from-{uuid.uuid4().hex}"
    files_from.write_text("\n".join(relative_paths) + "\n", encoding="ascii")
    try:
        completed = subprocess.run(
            [
                "rsync",
                "-a",
                "--files-from",
                str(files_from),
                f"{remote}:{metadata['session_root'].rstrip('/')}/",
                f"{destination}/",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"journal part rsync failed: {completed.stdout}\n{completed.stderr}"
            )
    finally:
        files_from.unlink(missing_ok=True)


def validate_downloaded_part(
    download_root: Path, metadata: Mapping[str, Any]
) -> dict[str, Any]:
    metadata = _validate_exact_part_metadata(metadata)
    root = download_root.expanduser().resolve(strict=True)
    candidates = (
        root / str(metadata["runtime_identity_relative"]),
        root / str(metadata["manifest_relative"]),
        root / str(metadata["data_relative"]),
    )
    if (root / "parts").is_symlink() or any(path.is_symlink() for path in candidates):
        raise JournalPartAdmissionError("downloaded journal path contains a symlink")
    identity_path, manifest_path, data_path = (
        path.resolve(strict=True) for path in candidates
    )
    for path in (identity_path, manifest_path, data_path):
        if root != path and root not in path.parents:
            raise JournalPartAdmissionError("downloaded journal path escaped staging root")
    expected = {
        identity_path: ("runtime_identity_file_sha256", "runtime_identity_size_bytes"),
        manifest_path: ("manifest_sha256", "manifest_size_bytes"),
        data_path: ("data_sha256", "data_size_bytes"),
    }
    for path, (sha_field, size_field) in expected.items():
        if not path.is_file() or path.is_symlink():
            raise JournalPartAdmissionError(f"downloaded file is missing or unsafe: {path}")
        if _sha256_file(path) != str(metadata[sha_field]):
            raise JournalPartAdmissionError(f"downloaded SHA256 differs: {path.name}")
        if path.stat().st_size != int(metadata[size_field]):
            raise JournalPartAdmissionError(f"downloaded size differs: {path.name}")

    identity = _read_json(identity_path, role="journal runtime identity")
    runtime_identity = identity.get("runtime_identity")
    if not isinstance(runtime_identity, dict):
        raise JournalPartAdmissionError("journal runtime identity payload is invalid")
    runtime_identity_sha = _canonical_sha256(runtime_identity)
    if runtime_identity_sha != str(identity.get("runtime_identity_sha256", "")):
        raise JournalPartAdmissionError("journal runtime identity canonical hash differs")
    if runtime_identity_sha != str(metadata["runtime_identity_sha256"]):
        raise JournalPartAdmissionError("part and runtime identities differ")

    manifest = _read_json(manifest_path, role="journal part manifest")
    if manifest.get("batch_id") != metadata["batch_id"]:
        raise JournalPartAdmissionError("journal batch identity differs")
    if manifest.get("data_file") != data_path.name:
        raise JournalPartAdmissionError("journal data filename differs")
    if manifest.get("data_sha256") != metadata["data_sha256"]:
        raise JournalPartAdmissionError("journal data hash differs")
    if int(manifest.get("row_count", -1)) != int(metadata["row_count"]):
        raise JournalPartAdmissionError("journal row-count metadata differs")
    checkpoint_before = manifest.get("checkpoint_before")
    if not isinstance(checkpoint_before, Mapping):
        raise JournalPartAdmissionError("journal checkpoint-before metadata is invalid")
    if int(manifest.get("first_lifecycle_sequence", -1)) != 1:
        raise JournalPartAdmissionError("journal part is not sequence 1")
    if int(checkpoint_before.get("last_emitted_sequence", -1)) != 0 or str(
        checkpoint_before.get("last_event_id", "")
    ):
        raise JournalPartAdmissionError("journal part is not independently recoverable")
    if bool(manifest.get("economic_outcomes_read")):
        raise JournalPartAdmissionError("journal part claims economic outcomes")

    validation_root = root.parent / f".writer-validation-{uuid.uuid4().hex}"
    validation_session = validation_root / f"session-{metadata['session_id']}"
    try:
        (validation_session / "parts").mkdir(parents=True)
        shutil.copy2(identity_path, validation_session / "runtime_identity.json")
        shutil.copy2(manifest_path, validation_session / "parts" / manifest_path.name)
        shutil.copy2(data_path, validation_session / "parts" / data_path.name)
        with OrderLifecycleJournalWriterV2(
            validation_root,
            session_id=str(metadata["session_id"]),
            runtime_identity=runtime_identity,
            storage_format="parquet",
            heartbeat_interval_s=60.0,
            start_heartbeat=False,
        ) as writer:
            health = writer.health_snapshot()
        if int(health.get("rows_committed", -1)) != int(metadata["row_count"]):
            raise JournalPartAdmissionError("writer recovery row count differs")
        if int(health.get("rows_dropped", -1)) != 0 or int(health.get("error_count", -1)) != 0:
            raise JournalPartAdmissionError("writer recovery reported drop/error")
    finally:
        shutil.rmtree(validation_root, ignore_errors=True)

    return {
        "batch_id": str(metadata["batch_id"]),
        "row_count": int(metadata["row_count"]),
        "runtime_identity_sha256": runtime_identity_sha,
        "journal_schema_sha256": str(metadata["journal_schema_sha256"]),
        "manifest_sha256": _sha256_file(manifest_path),
        "data_sha256": _sha256_file(data_path),
        "writer_recovery_validated": True,
        "economic_outcomes_read": False,
    }


def admit_roundtrip(
    *,
    release_manifest_path: Path,
    remote_identity_path: Path,
    performance_receipt_path: Path,
    remote: str,
    remote_root: str,
    remote_python: str,
    local_root: Path,
    receipt_output: Path,
) -> dict[str, Any]:
    bound = load_bound_release(release_manifest_path, remote_identity_path)
    performance = _load_performance_context(
        performance_receipt_path,
        bound=bound,
    )
    binding = performance["deployment_binding"]
    if remote != binding.get("remote"):
        raise JournalPartAdmissionError("transfer remote differs from deployment binding")
    if remote_root != binding.get("remote_root"):
        raise JournalPartAdmissionError("transfer remote root differs from deployment binding")
    session_root = str(performance["session_root"])
    session_root = _validate_remote_session_root(remote_root, session_root)
    expected_metadata = dict(performance["metadata"])
    metadata = select_remote_part(
        remote=remote,
        remote_root=remote_root,
        remote_python=remote_python,
        session_root=session_root,
        expected_metadata=expected_metadata,
    )
    if metadata != expected_metadata:
        raise JournalPartAdmissionError("selected remote part differs from performance evidence")
    if metadata.get("session_root") != session_root:
        raise JournalPartAdmissionError("selected remote session differs from performance evidence")

    unresolved_local_root = local_root.expanduser()
    if unresolved_local_root.is_symlink():
        raise JournalPartAdmissionError("local admission root must not be a symlink")
    local_root = unresolved_local_root.resolve()
    local_root.mkdir(parents=True, exist_ok=True)
    staging = local_root / f".roundtrip-partial-{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        _copy_remote_files(remote=remote, metadata=metadata, destination=staging)
        validation = validate_downloaded_part(staging, metadata)
        destination = (
            local_root
            / f"session-{metadata['session_id']}"
            / f"roundtrip-{metadata['batch_id']}"
        )
        if destination.exists():
            raise FileExistsError(f"roundtrip admission already exists: {destination}")
        publication = destination.parent / f".{destination.name}.partial-{uuid.uuid4().hex}"
        publication.mkdir(parents=True)
        try:
            shutil.copytree(staging, publication / "source")
            admission_manifest: dict[str, Any] = {
                "schema_version": SCHEMA_VERSION,
                "release_manifest_sha256": bound["release_manifest_sha256"],
                "remote_identity_sha256": bound["remote_identity_sha256"],
                "deployment_binding": binding,
                "deployment_binding_sha256": performance[
                    "deployment_binding_sha256"
                ],
                "parent_performance_receipt_identity_sha256": performance["receipt"][
                    "receipt_identity_sha256"
                ],
                "remote_session_root": session_root,
                "remote_payload_deleted": False,
                "validation": validation,
                "atomic_admission": True,
                "economic_outcomes_read": False,
            }
            admission_manifest["manifest_sha256"] = _canonical_sha256(admission_manifest)
            _atomic_json(publication / "admission_manifest.json", admission_manifest)
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.replace(publication, destination)
            descriptor = os.open(destination.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except Exception:
            shutil.rmtree(publication, ignore_errors=True)
            raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    manifest_path = destination / "admission_manifest.json"
    receipt = _seal_receipt(
        {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "stage": "admission",
            "release_manifest_sha256": bound["release_manifest_sha256"],
            "remote_identity_sha256": bound["remote_identity_sha256"],
            "mutation_plan_identity_sha256": binding[
                "mutation_plan_identity_sha256"
            ],
            "deployment_instance_id": binding.get("deployment_instance_id"),
            "deployment_binding": binding,
            "deployment_binding_sha256": performance["deployment_binding_sha256"],
            "parent_performance_receipt_identity_sha256": performance["receipt"][
                "receipt_identity_sha256"
            ],
            "evidence": {
                "bounded_spool_admission_roundtrip_passed": True,
                "remote_session_root": session_root,
                "remote_payload_deleted": False,
                "local_admission_manifest_path": str(manifest_path),
                "local_admission_manifest_sha256": _sha256_file(manifest_path),
                **validation,
            },
            "economic_outcomes_read": False,
            "strategy_parameters_changed": False,
        }
    )
    unresolved_receipt_output = receipt_output.expanduser()
    if unresolved_receipt_output.is_symlink():
        raise JournalPartAdmissionError("receipt output must not be a symlink")
    _atomic_json(unresolved_receipt_output.resolve(), receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-manifest", type=Path, required=True)
    parser.add_argument("--remote-v9-identity", type=Path, required=True)
    parser.add_argument("--performance-receipt", type=Path, required=True)
    parser.add_argument("--remote", default=DEFAULT_REMOTE)
    parser.add_argument("--remote-root", default=DEFAULT_REMOTE_ROOT)
    parser.add_argument("--remote-python", default=DEFAULT_REMOTE_PYTHON)
    parser.add_argument("--local-root", type=Path, default=DEFAULT_LOCAL_ROOT)
    parser.add_argument("--receipt-output", type=Path, required=True)
    parser.add_argument("--execute-transfer", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.execute_transfer:
        raise PermissionError("journal admission requires --execute-transfer")
    receipt = admit_roundtrip(
        release_manifest_path=args.release_manifest,
        remote_identity_path=args.remote_v9_identity,
        performance_receipt_path=args.performance_receipt,
        remote=args.remote,
        remote_root=args.remote_root,
        remote_python=args.remote_python,
        local_root=args.local_root,
        receipt_output=args.receipt_output,
    )
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
