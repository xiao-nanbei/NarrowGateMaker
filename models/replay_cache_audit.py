"""Read-only inventory and reference audit for legacy replay-window pickles."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

LEGACY_NAME_RE = re.compile(
    r"(?P<symbol>[a-z0-9]+)_(?P<day>\d{4}-\d{2}-\d{2})_"
    r"tick_window_v(?P<version>10|11|12|13)_(?P<digest>[a-f0-9]+)\.pkl"
)
TEXT_SUFFIXES = {
    ".csv",
    ".json",
    ".md",
    ".py",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}
SELF_AUDIT_PREFIXES = (
    "replay_cache_legacy_reference_audit_",
)


def _reference_class(path: Path) -> str:
    normalized = str(path).lower()
    name = path.name.lower()
    if "/reports/" in normalized or "/research/" in normalized:
        if any(token in name for token in ("manifest", "spec", "report", "audit")):
            return "frozen_or_evidence"
        return "research_documentation"
    if path.suffix == ".py":
        return "active_code"
    if "/docs/" in normalized or path.suffix == ".md":
        return "project_documentation"
    return "other_text_reference"


def _iter_text_files(roots: Iterable[Path]) -> Iterable[Path]:
    seen: set[Path] = set()
    for raw_root in roots:
        root = Path(raw_root).expanduser().resolve()
        if root.is_file():
            candidates = (root,)
        elif root.is_dir():
            candidates = root.rglob("*")
        else:
            continue
        for path in candidates:
            if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES or path in seen:
                continue
            if any(path.name.startswith(prefix) for prefix in SELF_AUDIT_PREFIXES):
                continue
            seen.add(path)
            yield path


def _scan_references(
    basenames: set[str],
    roots: Iterable[Path],
) -> dict[str, list[dict[str, str]]]:
    references: dict[str, list[dict[str, str]]] = defaultdict(list)
    if not basenames:
        return references
    for path in _iter_text_files(roots):
        try:
            if path.stat().st_size > 64 * 1024 * 1024:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        found = set(match.group(0) for match in LEGACY_NAME_RE.finditer(text))
        for basename in sorted(found.intersection(basenames)):
            references[basename].append(
                {
                    "path": str(path),
                    "class": _reference_class(path),
                }
            )
    return references


def audit_legacy_window_caches(
    cache_root: Path,
    *,
    reference_roots: Iterable[Path] = (),
) -> dict[str, Any]:
    """Return a zero-write audit without unpickling or hashing legacy payloads."""

    root = Path(cache_root).expanduser().resolve()
    records: list[dict[str, Any]] = []
    for path in sorted(root.glob("*_tick_window_v*.pkl")):
        match = LEGACY_NAME_RE.fullmatch(path.name)
        if match is None:
            continue
        stat = path.stat()
        records.append(
            {
                "path": str(path),
                "basename": path.name,
                "symbol": match.group("symbol").upper(),
                "day": match.group("day"),
                "version": int(match.group("version")),
                "cache_key_prefix": match.group("digest"),
                "size_bytes": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
            }
        )

    reference_map = _scan_references(
        {record["basename"] for record in records},
        reference_roots,
    )
    by_day: Counter[str] = Counter(record["day"] for record in records)
    by_version: dict[int, dict[str, int]] = defaultdict(
        lambda: {"file_count": 0, "size_bytes": 0, "distinct_days": 0}
    )
    version_days: dict[int, set[str]] = defaultdict(set)
    frozen_count = 0
    referenced_count = 0
    for record in records:
        references = reference_map.get(record["basename"], [])
        classes = sorted({reference["class"] for reference in references})
        frozen = "frozen_or_evidence" in classes
        record["references"] = references
        record["reference_classes"] = classes
        record["frozen_reference"] = frozen
        record["governance_status"] = (
            "preserve_frozen_reference"
            if frozen
            else "unreferenced_candidate_requires_user_approval"
        )
        referenced_count += int(bool(references))
        frozen_count += int(frozen)
        summary = by_version[int(record["version"])]
        summary["file_count"] += 1
        summary["size_bytes"] += int(record["size_bytes"])
        version_days[int(record["version"])].add(str(record["day"]))
    for version, days in version_days.items():
        by_version[version]["distinct_days"] = len(days)

    return {
        "schema_version": "narrowgate.legacy_window_cache_reference_audit.v1",
        "mode": "read_only_zero_write",
        "cache_root": str(root),
        "pickle_payloads_opened": 0,
        "cache_files_modified": 0,
        "cache_files_deleted": 0,
        "summary": {
            "file_count": len(records),
            "size_bytes": sum(int(record["size_bytes"]) for record in records),
            "distinct_days": len(by_day),
            "days_with_variants": sum(count > 1 for count in by_day.values()),
            "extra_same_day_variants": sum(count - 1 for count in by_day.values()),
            "max_variants_per_day": max(by_day.values(), default=0),
            "text_referenced_files": referenced_count,
            "frozen_or_evidence_referenced_files": frozen_count,
        },
        "versions": {f"v{version}": values for version, values in sorted(by_version.items())},
        "days": dict(sorted(by_day.items())),
        "files": records,
    }


def audit_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"
