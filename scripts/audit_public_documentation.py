#!/usr/bin/env python3
"""Fail-closed audit for public documentation and machine records."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import tarfile
import zipfile
from pathlib import Path
from urllib.parse import unquote, urlsplit

PUBLIC_SUFFIXES = {".csv", ".md", ".json", ".yaml", ".yml"}
PUBLIC_SOURCE_SUFFIXES = {
    ".c",
    ".cc",
    ".cpp",
    ".h",
    ".hpp",
    ".py",
    ".sh",
    ".toml",
}
PUBLIC_ARCHIVE_SUFFIXES = (".tar.gz", ".tgz", ".zip")
PRIVATE_LOCATOR_PATTERNS = {
    "personal_home": re.compile(r"/(?:Users|home)/xuantan(?:/|\b)|/home/ec2-user(?:/|\b)"),
    "physical_volume": re.compile(r"/Volumes/(?!<)[^\s`\"']+"),
    "private_tmp": re.compile(r"/private/tmp(?:/|\b)"),
    "ssh_target": re.compile(r"(?:ec2-user|ubuntu|root)@[A-Za-z0-9.-]+"),
    "known_private_ipv4": re.compile(
        r"(?<![0-9])(?:52\.194\.209\.205|167\.179\.114\.39|3\.114\.92\.206)(?![0-9])"
    ),
    "cloud_resource_id": re.compile(
        r"\b(?:i|ami|vol|snap)-[0-9a-f]{8,}\b|\beipalloc-[0-9a-f]+\b"
    ),
}
PROJECTION_MANIFESTS = (
    Path("research/public_machine_document_projections.json"),
    Path("docs/public_machine_document_projections.json"),
)
INLINE_LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
REFERENCE_DEF_RE = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*(\S+)")
FENCE_RE = re.compile(r"^\s{0,3}(`{3,}|~{3,})")
SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
SHA256_RE = re.compile(r"(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])")
HASH_AVAILABILITY_NOTICE = "Evidence availability:"
HEX_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PID_KEY_RE = re.compile(
    r"(?:^|_)(?:pid|process_id|process_pid|maker_pid|parent_pid|child_pid)$",
    re.IGNORECASE,
)
PRIVATE_MACHINE_PATH_RE = re.compile(
    r"(?:^|[^A-Za-z0-9_])(?:docs/private/|research/[^\s\"']+/private/)"
)
REDACTED_PROCESS_IDS = {
    "<private-process-id>",
    "private_process_id_redacted",
    "not_publicly_distributed",
}


def _public_candidates(repo_root: Path) -> list[Path]:
    output = subprocess.check_output(
        ["git", "ls-files", "-co", "--exclude-standard"],
        cwd=repo_root,
        text=True,
    )
    return sorted(path for item in output.splitlines() if (path := repo_root / item).is_file())


def public_files(repo_root: Path) -> list[Path]:
    return [path for path in _public_candidates(repo_root) if path.suffix.lower() in PUBLIC_SUFFIXES]


def _relative(repo_root: Path, path: Path) -> str:
    return path.relative_to(repo_root).as_posix()


def _is_ignored(repo_root: Path, relative_path: str) -> bool:
    return (
        subprocess.run(
            ["git", "check-ignore", "-q", "--", relative_path],
            cwd=repo_root,
            check=False,
        ).returncode
        == 0
    )


def _is_redacted_process_id(value: object) -> bool:
    return value is None or (isinstance(value, str) and value in REDACTED_PROCESS_IDS)


def _audit_machine_values(
    value: object,
    *,
    source_path: Path,
    repo_root: Path,
    findings: list[dict[str, object]],
    location: str = "",
) -> None:
    """Reject operational identifiers and private locators in public records."""

    if isinstance(value, list):
        for index, item in enumerate(value):
            _audit_machine_values(
                item,
                source_path=source_path,
                repo_root=repo_root,
                findings=findings,
                location=f"{location}/{index}",
            )
        return
    if not isinstance(value, dict):
        return
    for key, item in value.items():
        item_location = f"{location}/{key}"
        if PID_KEY_RE.search(str(key)) and not _is_redacted_process_id(item):
            findings.append(
                {
                    "path": _relative(repo_root, source_path),
                    "line": None,
                    "kind": "public_process_identifier",
                    "excerpt": f"{item_location}: process identifier must be redacted",
                }
            )
        if isinstance(item, str) and PRIVATE_MACHINE_PATH_RE.search(item):
            findings.append(
                {
                    "path": _relative(repo_root, source_path),
                    "line": None,
                    "kind": "public_machine_private_locator",
                    "excerpt": f"{item_location}: {item[:200]}",
                }
            )
        _audit_machine_values(
            item,
            source_path=source_path,
            repo_root=repo_root,
            findings=findings,
            location=item_location,
        )


def _archive_members(path: Path) -> list[tuple[str, bytes]]:
    if path.name.endswith((".tar.gz", ".tgz")):
        with tarfile.open(path, "r:gz") as handle:
            return [
                (member.name, extracted.read())
                for member in handle.getmembers()
                if member.isfile() and (extracted := handle.extractfile(member)) is not None
            ]
    if path.name.endswith(".zip"):
        with zipfile.ZipFile(path) as handle:
            return [(name, handle.read(name)) for name in handle.namelist() if not name.endswith("/")]
    return []


def _audit_archive(
    path: Path,
    *,
    repo_root: Path,
    findings: list[dict[str, object]],
) -> int:
    """Inspect public archive names and UTF-8 members for private locators."""

    members = _archive_members(path)
    for member_name, payload in members:
        values = [member_name]
        if len(payload) <= 50 * 1024 * 1024:
            try:
                values.append(payload.decode("utf-8"))
            except UnicodeDecodeError:
                pass
        for text in values:
            for kind, pattern in PRIVATE_LOCATOR_PATTERNS.items():
                if pattern.search(text):
                    findings.append(
                        {
                            "path": _relative(repo_root, path),
                            "line": None,
                            "kind": f"archive_{kind}",
                            "excerpt": member_name,
                        }
                    )
                    break
            else:
                continue
            break
    return len(members)


def _audit_markdown_sha_bindings(
    value: object,
    *,
    source_path: Path,
    repo_root: Path,
    findings: list[dict[str, object]],
    location: str = "",
) -> None:
    """Verify unambiguous JSON path/SHA pairs that point to public Markdown."""

    if isinstance(value, list):
        for index, item in enumerate(value):
            _audit_markdown_sha_bindings(
                item,
                source_path=source_path,
                repo_root=repo_root,
                findings=findings,
                location=f"{location}/{index}",
            )
        return
    if not isinstance(value, dict):
        return
    markdown_paths = [
        (key, item)
        for key, item in value.items()
        if isinstance(item, str)
        and item.endswith(".md")
        and (key == "path" or key.endswith("_path"))
    ]
    sha_values = [
        (key, item)
        for key, item in value.items()
        if isinstance(item, str)
        and HEX_SHA256_RE.fullmatch(item)
        and (key == "sha256" or key.endswith("_sha256"))
    ]
    if len(markdown_paths) == 1 and len(sha_values) == 1:
        _, relative_target = markdown_paths[0]
        sha_key, expected = sha_values[0]
        if not SCHEME_RE.match(relative_target) and "${" not in relative_target:
            target = (repo_root / relative_target).resolve(strict=False)
            try:
                target.relative_to(repo_root)
            except ValueError:
                target = Path()
            if target.is_file():
                observed = hashlib.sha256(target.read_bytes()).hexdigest()
                if observed != expected:
                    findings.append(
                        {
                            "path": str(source_path.relative_to(repo_root)),
                            "line": None,
                            "kind": "public_markdown_sha_mismatch",
                            "excerpt": (
                                f"{location or '/'}: {relative_target} {sha_key} "
                                f"expected={expected} observed={observed}"
                            ),
                        }
                    )
    for key, item in value.items():
        _audit_markdown_sha_bindings(
            item,
            source_path=source_path,
            repo_root=repo_root,
            findings=findings,
            location=f"{location}/{key}",
        )


def audit(repo_root: Path) -> dict[str, object]:
    findings: list[dict[str, object]] = []
    files = public_files(repo_root)
    for path in files:
        text = path.read_text(encoding="utf-8")
        if path.suffix == ".json":
            parsed = json.loads(text)
            _audit_markdown_sha_bindings(
                parsed,
                source_path=path,
                repo_root=repo_root,
                findings=findings,
            )
            if path.relative_to(repo_root) not in PROJECTION_MANIFESTS:
                _audit_machine_values(
                    parsed,
                    source_path=path,
                    repo_root=repo_root,
                    findings=findings,
                )
        if (
            path.suffix.lower() == ".md"
            and SHA256_RE.search(text)
            and HASH_AVAILABILITY_NOTICE not in text
        ):
            findings.append(
                {
                    "path": str(path.relative_to(repo_root)),
                    "line": None,
                    "kind": "hash_availability_notice_missing",
                    "excerpt": (
                        "A public Markdown document containing SHA256 identities must "
                        "state which bytes are publicly available and which are retained "
                        "in the private evidence store."
                    ),
                }
            )
        for line_number, line in enumerate(text.splitlines(), start=1):
            for kind, pattern in PRIVATE_LOCATOR_PATTERNS.items():
                if pattern.search(line):
                    findings.append(
                        {
                            "path": str(path.relative_to(repo_root)),
                            "line": line_number,
                            "kind": kind,
                            "excerpt": line[:240],
                        }
                    )
    projection_entries = 0
    for relative_manifest in PROJECTION_MANIFESTS:
        manifest_path = repo_root / relative_manifest
        if not manifest_path.is_file():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        entries = manifest.get("entries", [])
        projection_entries += len(entries)
        for entry in entries:
            relative_public_path = str(entry["public_path"])
            if _is_ignored(repo_root, relative_public_path):
                findings.append(
                    {
                        "path": str(relative_manifest),
                        "line": None,
                        "kind": "public_projection_is_git_ignored",
                        "excerpt": relative_public_path,
                    }
                )
            public_path = repo_root / entry["public_path"]
            if not public_path.is_file():
                findings.append(
                    {
                        "path": str(relative_manifest),
                        "line": None,
                        "kind": "public_projection_missing",
                        "excerpt": entry["public_path"],
                    }
                )
                continue
            observed = hashlib.sha256(public_path.read_bytes()).hexdigest()
            expected = entry["public_projection_sha256"]
            if observed != expected:
                findings.append(
                    {
                        "path": str(relative_manifest),
                        "line": None,
                        "kind": "public_projection_sha_mismatch",
                            "excerpt": f"{entry['public_path']}: expected={expected} observed={observed}",
                        }
                    )
    source_files_scanned = 0
    for path in _public_candidates(repo_root):
        if path.suffix.lower() not in PUBLIC_SOURCE_SUFFIXES:
            continue
        source_files_scanned += 1
        if path.resolve() == Path(__file__).resolve():
            # The auditor must contain the prohibited patterns that it enforces.
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for line_number, line in enumerate(text.splitlines(), start=1):
            for kind, pattern in PRIVATE_LOCATOR_PATTERNS.items():
                if pattern.search(line):
                    findings.append(
                        {
                            "path": _relative(repo_root, path),
                            "line": line_number,
                            "kind": f"source_{kind}",
                            "excerpt": line[:240],
                        }
                    )
    archive_files_scanned = 0
    archive_members_scanned = 0
    for path in _public_candidates(repo_root):
        if not path.name.endswith(PUBLIC_ARCHIVE_SUFFIXES):
            continue
        archive_files_scanned += 1
        archive_members_scanned += _audit_archive(
            path,
            repo_root=repo_root,
            findings=findings,
        )
    links_checked = 0
    for path in files:
        if path.suffix.lower() != ".md":
            continue
        in_fence = False
        fence_character = ""
        fence_length = 0
        for line_number, original in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            fence = FENCE_RE.match(original)
            if fence:
                token = fence.group(1)
                if not in_fence:
                    in_fence = True
                    fence_character = token[0]
                    fence_length = len(token)
                elif token[0] == fence_character and len(token) >= fence_length:
                    in_fence = False
                continue
            if in_fence:
                continue
            line = re.sub(r"`+[^`]*`+", "", original)
            destinations = [match.group(1) for match in INLINE_LINK_RE.finditer(line)]
            reference = REFERENCE_DEF_RE.match(line)
            if reference:
                destinations.append(reference.group(1))
            for raw_destination in destinations:
                value = raw_destination.strip()
                if value.startswith("<"):
                    end = value.find(">")
                    destination = value[1:end] if end >= 0 else value[1:]
                else:
                    destination = value.split(maxsplit=1)[0]
                target = unquote(destination.strip())
                if (
                    not target
                    or target.startswith(("#", "//"))
                    or "${" in target
                    or "<" in target
                    or ">" in target
                    or SCHEME_RE.match(target)
                ):
                    continue
                path_text = urlsplit(target).path
                if not path_text:
                    continue
                candidate = (
                    repo_root / path_text.lstrip("/")
                    if path_text.startswith("/")
                    else path.parent / path_text
                ).resolve(strict=False)
                links_checked += 1
                if not candidate.exists():
                    findings.append(
                        {
                            "path": str(path.relative_to(repo_root)),
                            "line": line_number,
                            "kind": "broken_repository_link",
                            "excerpt": destination,
                        }
                    )
    return {
        "schema_version": "narrowgate_public_documentation_audit_v2",
        "files_scanned": len(files),
        "source_files_scanned": source_files_scanned,
        "archive_files_scanned": archive_files_scanned,
        "archive_members_scanned": archive_members_scanned,
        "projection_entries_verified": projection_entries,
        "repository_links_checked": links_checked,
        "findings": findings,
        "passed": not findings,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = audit(args.repo_root.resolve())
    rendered = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
