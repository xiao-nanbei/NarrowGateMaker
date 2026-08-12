#!/usr/bin/env python3
"""Move Git-ignored machine projections out of the public projection index."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLIC_MANIFEST = Path("docs/public_machine_document_projections.json")
PRIVATE_INDEX = Path(
    "models/private/nonpublished_machine_document_projections.current.local.json"
)
PUBLIC_SCHEMA = "narrowgate_public_machine_document_projections_v1"
PRIVATE_SCHEMA = "narrowgate_nonpublished_machine_document_projections_v1"


def _is_ignored(repo_root: Path, relative_path: str) -> bool:
    return (
        subprocess.run(
            ["git", "check-ignore", "-q", "--", relative_path],
            cwd=repo_root,
            check=False,
        ).returncode
        == 0
    )


def split(repo_root: Path, *, apply: bool) -> dict[str, object]:
    public_path = repo_root / PUBLIC_MANIFEST
    payload = json.loads(public_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != PUBLIC_SCHEMA:
        raise RuntimeError(f"unsupported public projection schema: {PUBLIC_MANIFEST}")
    private_path = repo_root / PRIVATE_INDEX
    existing_private: dict[str, dict[str, object]] = {}
    if private_path.is_file():
        private_payload = json.loads(private_path.read_text(encoding="utf-8"))
        if private_payload.get("schema_version") != PRIVATE_SCHEMA:
            raise RuntimeError(f"unsupported private projection schema: {PRIVATE_INDEX}")
        for raw_entry in private_payload.get("entries", []):
            entry = dict(raw_entry)
            relative = str(entry["public_path"])
            if relative in existing_private:
                raise RuntimeError(f"duplicate non-published projection: {relative}")
            existing_private[relative] = entry

    retained: list[dict[str, object]] = []
    moved: list[dict[str, object]] = []
    for raw_entry in payload.get("entries", []):
        entry = dict(raw_entry)
        relative = str(entry["public_path"])
        if _is_ignored(repo_root, relative):
            if not (repo_root / relative).is_file():
                raise RuntimeError(f"ignored projection bytes are missing: {relative}")
            entry["availability"] = "private_working_tree_projection_not_distributed"
            moved.append(entry)
        else:
            retained.append(entry)
    merged_private = dict(existing_private)
    for entry in moved:
        relative = str(entry["public_path"])
        prior = merged_private.get(relative)
        if prior is not None and prior != entry:
            raise RuntimeError(
                f"non-published projection identity changed without explicit migration: {relative}"
            )
        merged_private[relative] = entry

    result = {
        "schema_version": "narrowgate_nonpublished_projection_split_receipt_v1",
        "applied": apply,
        "public_entries_before": len(payload.get("entries", [])),
        "public_entries_after": len(retained),
        "private_entries": len(merged_private),
        "new_private_entries": len(moved),
        "moved_paths": [str(entry["public_path"]) for entry in moved],
    }
    if not apply:
        return result
    payload["entries"] = sorted(retained, key=lambda row: str(row["public_path"]))
    payload["last_materially_modified"] = "2026-08-12"
    public_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    private_payload = {
        "schema_version": PRIVATE_SCHEMA,
        "visibility": "local_only_do_not_publish",
        "documentation_scope": "local_only_do_not_publish",
        "unit_id": "models",
        "purpose": (
            "Owner-side identity index for sanitized model records that live below "
            "Git-ignored runtime bundle directories and are not GitHub artifacts."
        ),
        "entries": sorted(merged_private.values(), key=lambda row: str(row["public_path"])),
    }
    private_path.parent.mkdir(parents=True, exist_ok=True)
    private_path.write_text(
        json.dumps(private_payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    private_path.chmod(0o600)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = split(args.repo_root.resolve(), apply=args.apply)
    rendered = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
