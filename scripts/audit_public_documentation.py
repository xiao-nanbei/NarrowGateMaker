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
from ipaddress import IPv4Address, IPv4Network
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
_POSIX_SEPARATOR = "/"
_ESCAPED_POSIX_SEPARATOR = re.escape(_POSIX_SEPARATOR)
PRIVATE_LOCATOR_PATTERNS = {
    "personal_home": re.compile(
        rf"{_ESCAPED_POSIX_SEPARATOR}(?:Users|home)"
        rf"{_ESCAPED_POSIX_SEPARATOR}(?!<)[A-Za-z0-9._-]+"
        rf"(?:{_ESCAPED_POSIX_SEPARATOR}|\b)"
    ),
    "physical_volume": re.compile(
        rf"{_ESCAPED_POSIX_SEPARATOR}Volumes{_ESCAPED_POSIX_SEPARATOR}"
        r"(?!<)[^\s`\"']+"
    ),
    "private_tmp": re.compile(
        rf"{_ESCAPED_POSIX_SEPARATOR}private{_ESCAPED_POSIX_SEPARATOR}tmp"
        rf"(?:{_ESCAPED_POSIX_SEPARATOR}|\b)"
    ),
    "ssh_target": re.compile(r"(?:ec2-user|ubuntu|root)@[A-Za-z0-9.-]+"),
    "cloud_resource_id": re.compile(
        r"\b(?:i|ami|vol|snap)-[0-9a-f]{8,}\b|\beipalloc-[0-9a-f]+\b"
    ),
}
EXPLICIT_LOCATOR_PLACEHOLDERS = (
    "/Users/...",
    "/home/...",
    "/Volumes/...",
    "/private/tmp/...",
)
IPV4_CANDIDATE_RE = re.compile(
    r"(?<![0-9.])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9.])"
)
EXPLICIT_NON_PUBLIC_IPV4_NETWORKS = (
    IPv4Network("100.64.0.0/10"),  # RFC 6598 shared address space
    IPv4Network("192.0.0.0/24"),  # IETF protocol assignments
    IPv4Network("192.0.2.0/24"),  # RFC 5737 TEST-NET-1
    IPv4Network("198.18.0.0/15"),  # RFC 2544 benchmark testing
    IPv4Network("198.51.100.0/24"),  # RFC 5737 TEST-NET-2
    IPv4Network("203.0.113.0/24"),  # RFC 5737 TEST-NET-3
)
PROJECTION_MANIFESTS = (
    Path("research/public_machine_document_projections.json"),
    Path("docs/public_machine_document_projections.json"),
)
INLINE_LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
REFERENCE_DEF_RE = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*(\S+)")
HTML_HREF_RE = re.compile(r"<a\s+[^>]*href\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
FENCE_RE = re.compile(r"^\s{0,3}(`{3,}|~{3,})")
SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
SHA256_RE = re.compile(r"(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])")
HASH_AVAILABILITY_NOTICES = (
    "Evidence availability:",
    "证据可用性：",
)
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
HAN_CHARACTER_RE = re.compile(
    r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\U00020000-\U0002fa1f]"
)
ZH_CN_MARKDOWN_SUFFIX = ".zh-CN.md"
LAST_MATERIALLY_SYNCHRONIZED_RE = re.compile(
    r"^Last materially synchronized: (\d{4}-\d{2}-\d{2})\s*$",
    re.MULTILINE,
)
REQUIRED_BILINGUAL_DOCUMENTS = (
    Path("README.md"),
    Path("CONTRIBUTING.md"),
    Path("SECURITY.md"),
    Path("docs/opensource/README.md"),
    Path("docs/dev/README.md"),
    Path("docs/ops/README.md"),
    Path("research/README.md"),
    Path("docs/public_private_documentation_contract.md"),
)


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


def _is_public_ipv4_text(value: str) -> bool:
    """Return whether a dotted-decimal candidate is a public IPv4 locator."""

    octets = tuple(int(part, 10) for part in value.split("."))
    if len(octets) != 4 or any(octet > 255 for octet in octets):
        return False
    address = IPv4Address(bytes(octets))
    if any(address in network for network in EXPLICIT_NON_PUBLIC_IPV4_NETWORKS):
        return False
    if (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    ):
        return False
    return address.is_global


def _private_locator_kinds(text: str) -> list[str]:
    """Return generic private-locator findings without owner-specific values."""

    # Keep examples honest without treating a literal ellipsis as an owner path.
    # This is intentionally narrower than ignoring inline/fenced code: a real
    # locator remains a finding even when it appears in a command example.
    for placeholder in EXPLICIT_LOCATOR_PLACEHOLDERS:
        text = text.replace(placeholder, "<private-path-placeholder>")
    kinds = [
        kind
        for kind, pattern in PRIVATE_LOCATOR_PATTERNS.items()
        if pattern.search(text)
    ]
    if any(
        _is_public_ipv4_text(match.group(0))
        for match in IPV4_CANDIDATE_RE.finditer(text)
    ):
        kinds.append("public_ipv4")
    return kinds


def _markdown_repository_links(
    path: Path,
    *,
    repo_root: Path,
) -> list[tuple[int, str, Path]]:
    """Return repository-local Markdown link destinations outside code fences."""

    links: list[tuple[int, str, Path]] = []
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
        destinations.extend(match.group(1) for match in HTML_HREF_RE.finditer(line))
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
            links.append((line_number, destination, candidate))
    return links


def _english_path_for_translation(path: Path) -> Path:
    stem = path.name[: -len(ZH_CN_MARKDOWN_SUFFIX)]
    return path.with_name(f"{stem}.md")


def _translation_path_for_english(path: Path) -> Path:
    return path.with_name(f"{path.stem}{ZH_CN_MARKDOWN_SUFFIX}")


def _audit_bilingual_documents(
    files: list[Path],
    *,
    repo_root: Path,
    findings: list[dict[str, object]],
) -> None:
    """Require maintained bilingual pairs without translating historical evidence."""

    markdown_files = {
        path.resolve(): path for path in files if path.suffix.lower() == ".md"
    }
    english_paths = set(REQUIRED_BILINGUAL_DOCUMENTS)
    for path in markdown_files.values():
        if path.name.endswith(ZH_CN_MARKDOWN_SUFFIX):
            english_paths.add(_english_path_for_translation(path).relative_to(repo_root))

    for relative_english in sorted(english_paths):
        english = repo_root / relative_english
        translation = _translation_path_for_english(english)
        english_is_public = english.resolve() in markdown_files
        translation_is_public = translation.resolve() in markdown_files
        required = relative_english in REQUIRED_BILINGUAL_DOCUMENTS

        if not english_is_public:
            findings.append(
                {
                    "path": _relative(repo_root, english),
                    "line": None,
                    "kind": (
                        "required_bilingual_document_missing"
                        if required
                        else "bilingual_english_document_missing"
                    ),
                    "excerpt": (
                        f"Expected the public English counterpart of "
                        f"{_relative(repo_root, translation)}."
                    ),
                }
            )
        if not translation_is_public and required:
            findings.append(
                {
                    "path": _relative(repo_root, translation),
                    "line": None,
                    "kind": "required_bilingual_document_missing",
                    "excerpt": (
                        f"Expected the Simplified Chinese counterpart of "
                        f"{_relative(repo_root, english)}."
                    ),
                }
            )
        if not english_is_public or not translation_is_public:
            continue

        for source, target in ((english, translation), (translation, english)):
            source_targets = {
                candidate
                for _, _, candidate in _markdown_repository_links(
                    source,
                    repo_root=repo_root,
                )
            }
            if target.resolve() not in source_targets:
                findings.append(
                    {
                        "path": _relative(repo_root, source),
                        "line": None,
                        "kind": "bilingual_counterpart_link_missing",
                        "excerpt": (
                            f"Expected a Markdown link to {_relative(repo_root, target)}."
                        ),
                    }
                )

        english_dates = LAST_MATERIALLY_SYNCHRONIZED_RE.findall(
            english.read_text(encoding="utf-8")
        )
        translation_dates = LAST_MATERIALLY_SYNCHRONIZED_RE.findall(
            translation.read_text(encoding="utf-8")
        )
        for path, dates in ((english, english_dates), (translation, translation_dates)):
            if len(dates) != 1:
                findings.append(
                    {
                        "path": _relative(repo_root, path),
                        "line": None,
                        "kind": "bilingual_sync_marker_missing",
                        "excerpt": (
                            "Expected exactly one `Last materially synchronized: "
                            "YYYY-MM-DD` marker."
                        ),
                    }
                )
        if (
            len(english_dates) == 1
            and len(translation_dates) == 1
            and english_dates[0] != translation_dates[0]
        ):
            findings.append(
                {
                    "path": _relative(repo_root, translation),
                    "line": None,
                    "kind": "bilingual_sync_marker_mismatch",
                    "excerpt": (
                        f"English={english_dates[0]} Simplified-Chinese="
                        f"{translation_dates[0]}"
                    ),
                }
            )


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
            kinds = _private_locator_kinds(text)
            if not kinds:
                continue
            findings.append(
                {
                    "path": _relative(repo_root, path),
                    "line": None,
                    "kind": f"archive_{kinds[0]}",
                    "excerpt": member_name,
                }
            )
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
        relative_path = path.relative_to(repo_root)
        if (
            len(relative_path.parts) >= 3
            and relative_path.parts[:2] == (".github", "skills")
            and relative_path.name == "SKILL.md"
        ):
            for line_number, line in enumerate(text.splitlines(), start=1):
                if HAN_CHARACTER_RE.search(line):
                    findings.append(
                        {
                            "path": relative_path.as_posix(),
                            "line": line_number,
                            "kind": "machine_skill_contains_han",
                            "excerpt": line[:240],
                        }
                    )
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
            and not any(notice in text for notice in HASH_AVAILABILITY_NOTICES)
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
            for kind in _private_locator_kinds(line):
                findings.append(
                    {
                        "path": str(path.relative_to(repo_root)),
                        "line": line_number,
                        "kind": kind,
                        "excerpt": line[:240],
                    }
                )
    _audit_bilingual_documents(
        files,
        repo_root=repo_root,
        findings=findings,
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
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for line_number, line in enumerate(text.splitlines(), start=1):
            for kind in _private_locator_kinds(line):
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
        for line_number, destination, candidate in _markdown_repository_links(
            path,
            repo_root=repo_root,
        ):
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
