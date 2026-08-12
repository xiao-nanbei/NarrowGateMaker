#!/usr/bin/env python3
"""Create and verify immutable identities for NarrowGate experiments."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import subprocess
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "narrowgate_experiment_manifest.v1"


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def file_identity(path: Path, *, logical_path: str | None = None) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    row: dict[str, Any] = {
        "path": logical_path or str(path),
        "resolved_path": str(resolved),
        "exists": resolved.is_file(),
    }
    if resolved.is_file():
        row.update(
            {
                "size_bytes": resolved.stat().st_size,
                "sha256": sha256_file(resolved),
            }
        )
    return row


def _git(repo_root: Path, *args: str, text: bool = False) -> bytes | str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=text,
    )
    return result.stdout


def git_workspace_identity(repo_root: Path) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    head = str(_git(root, "rev-parse", "HEAD", text=True)).strip()
    branch = str(_git(root, "branch", "--show-current", text=True)).strip()
    tracked_patch = bytes(_git(root, "diff", "--binary", "HEAD", "--"))
    status_short = str(_git(root, "status", "--short", text=True)).splitlines()
    raw_untracked = bytes(
        _git(root, "ls-files", "--others", "--exclude-standard", "-z")
    )
    untracked: list[dict[str, Any]] = []
    for encoded in sorted(item for item in raw_untracked.split(b"\0") if item):
        relative = encoded.decode("utf-8", errors="surrogateescape")
        path = root / relative
        if not path.is_file():
            continue
        untracked.append(
            {
                "path": relative,
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    tracked_patch_sha256 = sha256_bytes(tracked_patch)
    workspace_payload = {
        "head": head,
        "tracked_patch_sha256": tracked_patch_sha256,
        "untracked": untracked,
    }
    return {
        "repo_root": str(root),
        "commit": head,
        "branch": branch,
        "dirty": bool(status_short),
        "status_short": status_short,
        "tracked_patch_size_bytes": len(tracked_patch),
        "tracked_patch_sha256": tracked_patch_sha256,
        "untracked_files": untracked,
        "workspace_sha256": sha256_bytes(_canonical_bytes(workspace_payload)),
    }


def _write_deterministic_untracked_tar(
    path: Path,
    *,
    repo_root: Path,
    untracked_files: list[dict[str, Any]],
) -> None:
    with tarfile.open(path, mode="w") as archive:
        for row in sorted(untracked_files, key=lambda item: str(item["path"])):
            relative = str(row["path"])
            source = repo_root / relative
            info = archive.gettarinfo(str(source), arcname=relative)
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mtime = 0
            with source.open("rb") as handle:
                archive.addfile(info, handle)


def write_code_checkpoint(
    checkpoint_dir: Path,
    *,
    repo_root: Path,
    code_identity: dict[str, Any],
) -> dict[str, Any]:
    """Persist enough material to restore a dirty workspace from its HEAD."""

    output = Path(checkpoint_dir).expanduser().resolve()
    metadata_path = output / "checkpoint.json"
    if metadata_path.exists():
        existing = json.loads(metadata_path.read_text(encoding="utf-8"))
        if existing.get("workspace_sha256") != code_identity.get("workspace_sha256"):
            raise FileExistsError(
                f"checkpoint directory belongs to a different workspace: {output}"
            )
        return {
            "directory": str(output),
            "metadata": file_identity(metadata_path),
            "tracked_patch": file_identity(output / "tracked.patch"),
            "untracked_archive": file_identity(output / "untracked_files.tar"),
        }

    output.mkdir(parents=True, exist_ok=False)
    patch_path = output / "tracked.patch"
    patch_path.write_bytes(bytes(_git(repo_root, "diff", "--binary", "HEAD", "--")))
    archive_path = output / "untracked_files.tar"
    _write_deterministic_untracked_tar(
        archive_path,
        repo_root=repo_root,
        untracked_files=list(code_identity.get("untracked_files", [])),
    )
    metadata = {
        "schema_version": "narrowgate_code_checkpoint.v1",
        "base_commit": code_identity.get("commit", ""),
        "workspace_sha256": code_identity.get("workspace_sha256", ""),
        "tracked_patch": file_identity(patch_path),
        "untracked_archive": file_identity(archive_path),
        "restore": [
            f"git switch --detach {code_identity.get('commit', '')}",
            "git apply tracked.patch",
            "tar -xf untracked_files.tar -C <repo-root>",
        ],
    }
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return {
        "directory": str(output),
        "metadata": file_identity(metadata_path),
        "tracked_patch": file_identity(patch_path),
        "untracked_archive": file_identity(archive_path),
    }


def _dataset_summary(path: Path) -> dict[str, Any]:
    identity = file_identity(path)
    if not identity["exists"]:
        return identity
    days: list[str] = []
    with Path(path).open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames and "day" in reader.fieldnames:
            days = [str(row.get("day", "")).strip() for row in reader]
            days = [day for day in days if day]
    unique_days = list(dict.fromkeys(days))
    identity.update(
        {
            "rows": len(days),
            "unique_days": len(unique_days),
            "first_day": unique_days[0] if unique_days else "",
            "last_day": unique_days[-1] if unique_days else "",
            "days": unique_days,
        }
    )
    return identity


def _resolve_path(raw: str, *, repo_root: Path) -> Path:
    expanded = os.path.expandvars(os.path.expanduser(str(raw)))
    path = Path(expanded)
    return path if path.is_absolute() else repo_root / path


def _path_rows(
    raw_paths: list[str],
    *,
    repo_root: Path,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in raw_paths:
        resolved = _resolve_path(raw, repo_root=repo_root)
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            file_identity(
                resolved,
                logical_path=str(raw),
            )
        )
    return rows


def build_manifest(
    spec: dict[str, Any],
    *,
    repo_root: Path,
    code_identity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    required = {
        "experiment_id",
        "config_path",
        "dataset_manifest_path",
        "feature_schema_version",
        "model_versions",
        "label_versions",
        "splits",
        "baseline_definition",
        "action_definition",
        "artifact_paths",
    }
    missing = sorted(required - set(spec))
    if missing:
        raise ValueError(f"experiment spec missing required fields: {missing}")

    root = Path(repo_root).resolve()
    config_raw = str(spec["config_path"])
    dataset_raw = str(spec["dataset_manifest_path"])
    config = file_identity(
        _resolve_path(config_raw, repo_root=root), logical_path=config_raw
    )
    dataset = _dataset_summary(_resolve_path(dataset_raw, repo_root=root))
    dataset["path"] = dataset_raw
    code = code_identity or git_workspace_identity(root)
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": str(spec["experiment_id"]),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "code": code,
        "engine": spec.get("engine", "unspecified"),
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "config": config,
        "dataset_manifest": dataset,
        "feature_schema_version": spec["feature_schema_version"],
        "model_versions": spec["model_versions"],
        "label_versions": spec["label_versions"],
        "splits": spec["splits"],
        "baseline_definition": spec["baseline_definition"],
        "action_definition": spec["action_definition"],
        "scorecard_profile": spec.get("scorecard_profile", {}),
        "commands": list(spec.get("commands", [])),
        "inputs": _path_rows(
            list(spec.get("input_paths", [])), repo_root=root
        ),
        "artifacts": _path_rows(list(spec["artifact_paths"]), repo_root=root),
        "metrics": spec.get("metrics", {}),
        "promotion_status": spec.get("promotion_status", "unreviewed"),
        "notes": spec.get("notes", ""),
    }
    identity_payload = {
        key: value
        for key, value in manifest.items()
        if key not in {"created_at_utc", "manifest_identity_sha256"}
    }
    manifest["manifest_identity_sha256"] = sha256_bytes(
        _canonical_bytes(identity_payload)
    )
    return manifest


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    output = Path(path).expanduser().resolve()
    if output.exists():
        raise FileExistsError(
            f"refusing to overwrite immutable experiment manifest: {output}"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _check_identity(row: dict[str, Any]) -> dict[str, Any]:
    path = Path(str(row.get("resolved_path", "")))
    expected = str(row.get("sha256", ""))
    if not expected:
        return {"path": str(path), "status": "unhashed"}
    if not path.is_file():
        return {"path": str(path), "status": "missing", "expected": expected}
    actual = sha256_file(path)
    return {
        "path": str(path),
        "status": "ok" if actual == expected else "hash_mismatch",
        "expected": expected,
        "actual": actual,
    }


def verify_manifest(path: Path, *, repo_root: Path) -> dict[str, Any]:
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    checks: list[dict[str, Any]] = []
    current_code = git_workspace_identity(repo_root)
    expected_workspace = str(manifest.get("code", {}).get("workspace_sha256", ""))
    checks.append(
        {
            "path": "git-workspace",
            "status": (
                "ok"
                if current_code["workspace_sha256"] == expected_workspace
                else "hash_mismatch"
            ),
            "expected": expected_workspace,
            "actual": current_code["workspace_sha256"],
        }
    )
    checks.append(_check_identity(manifest.get("config", {})))
    checks.append(_check_identity(manifest.get("dataset_manifest", {})))
    checks.extend(_check_identity(row) for row in manifest.get("inputs", []))
    checks.extend(_check_identity(row) for row in manifest.get("artifacts", []))
    failures = [row for row in checks if row["status"] not in {"ok", "unhashed"}]
    return {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": manifest.get("experiment_id", ""),
        "manifest_identity_sha256": manifest.get("manifest_identity_sha256", ""),
        "status": "ok" if not failures else "failed",
        "checks": checks,
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--spec", type=Path)
    mode.add_argument("--verify", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--checkpoint-dir", type=Path)
    parser.add_argument(
        "--repo-root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    args = parser.parse_args()

    if args.verify is not None:
        result = verify_manifest(args.verify, repo_root=args.repo_root)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["status"] == "ok" else 1

    if args.output is None:
        parser.error("--spec requires --output")
    spec = json.loads(args.spec.read_text(encoding="utf-8"))
    code = git_workspace_identity(args.repo_root)
    if args.checkpoint_dir is not None:
        code["checkpoint"] = write_code_checkpoint(
            args.checkpoint_dir,
            repo_root=args.repo_root,
            code_identity=code,
        )
    manifest = build_manifest(
        spec,
        repo_root=args.repo_root,
        code_identity=code,
    )
    write_manifest(args.output, manifest)
    print(
        json.dumps(
            {
                "experiment_id": manifest["experiment_id"],
                "manifest_identity_sha256": manifest["manifest_identity_sha256"],
                "output": str(args.output.expanduser().resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
