"""Read-only semantic reference audit for legacy v12 window caches."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CACHE_RE = re.compile(
    r"^btcusdc_(?P<day>\d{4}-\d{2}-\d{2})_tick_window_v12_(?P<prefix>[a-f0-9]{16})\.pkl$"
)
DATE_RE = re.compile(r"\b20\d{2}-\d{2}-\d{2}\b")
TEXT_SUFFIXES = frozenset({".json", ".md", ".txt", ".yaml", ".yml", ".csv", ".manifest", ".sha256"})
MAX_TEXT_BYTES = 64 * 1024 * 1024
AUDIT_OUTPUT_PREFIXES = (
    "replay_cache_legacy_reference_audit_",
    "replay_cache_v12_semantic_reference_audit_",
)

VERSION_FIELD_NAMES = frozenset(
    {
        "baseline_window_cache_version",
        "legacy_window_cache_version",
        "window_cache_version",
    }
)
SEMANTIC_FIELD_TOKENS = (
    "artifact",
    "book",
    "cache",
    "config",
    "execution_trade",
    "feature",
    "formal_quality",
    "load_ml",
    "manifest",
    "model",
    "native_exchange_book",
    "p3",
    "require_historical_bbo",
    "require_ml",
    "run_ml_inference",
    "source",
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        temp_path.replace(path)
    finally:
        temp_path.unlink(missing_ok=True)


def _iter_text_files(roots: Iterable[Path]) -> Iterable[tuple[Path, Path]]:
    seen: set[Path] = set()
    for root_value in roots:
        root = root_value.expanduser().resolve()
        if not root.exists():
            continue
        paths = [root] if root.is_file() else root.rglob("*")
        for path in paths:
            if not path.is_file() or path.is_symlink() or path.suffix.lower() not in TEXT_SUFFIXES:
                continue
            resolved = path.resolve()
            if resolved in seen or path.name.startswith(AUDIT_OUTPUT_PREFIXES):
                continue
            try:
                if path.stat().st_size > MAX_TEXT_BYTES:
                    continue
            except OSError:
                continue
            seen.add(resolved)
            yield root, resolved


def _walk(value: Any, path: tuple[str, ...] = ()) -> Iterable[tuple[tuple[str, ...], Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = (*path, str(key))
            yield child_path, child
            yield from _walk(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            child_path = (*path, str(index))
            yield child_path, child
            yield from _walk(child, child_path)


def _is_v12(value: Any) -> bool:
    return isinstance(value, (int, str)) and str(value).strip() == "12"


def _version_bindings(payload: Any) -> list[dict[str, str]]:
    bindings: list[dict[str, str]] = []
    for path, value in _walk(payload):
        key = path[-1]
        parent = path[-2] if len(path) > 1 else ""
        explicit = key in VERSION_FIELD_NAMES
        nested_window_cache = key == "cache_version" and "window_cache" in parent
        if (explicit or nested_window_cache) and _is_v12(value):
            bindings.append({"field": ".".join(path), "value": str(value)})
    return bindings


def _dates_from_payload(payload: Any) -> list[str]:
    if isinstance(payload, dict) and isinstance(payload.get("days"), list):
        return sorted(
            {
                match.group(0)
                for value in payload["days"]
                if isinstance(value, str)
                for match in DATE_RE.finditer(value)
            }
        )
    if isinstance(payload, dict) and isinstance(payload.get("panels"), dict):
        panel_dates: set[str] = set()
        for panel in payload["panels"].values():
            if not isinstance(panel, dict) or not isinstance(panel.get("days"), list):
                continue
            panel_dates.update(
                match.group(0)
                for value in panel["days"]
                if isinstance(value, str)
                for match in DATE_RE.finditer(value)
            )
        if panel_dates:
            return sorted(panel_dates)
    dates: set[str] = set()
    for path, value in _walk(payload):
        if not isinstance(value, str):
            continue
        semantic_keys = [key.lower() for key in path if not key.isdigit()]
        day_field = any(
            key in {"day", "days"} or key.endswith("_day") or key.endswith("_days")
            for key in semantic_keys
        )
        if not day_field:
            continue
        dates.update(match.group(0) for match in DATE_RE.finditer(value))
    return sorted(dates)


def _identity(payload: Any, path: Path) -> str:
    if isinstance(payload, dict):
        for key in (
            "family_id",
            "experiment_identity",
            "identity",
            "experiment_id",
            "tag",
            "schema_version",
        ):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return path.stem


def _semantic_requirements(payload: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for path, value in _walk(payload):
        if isinstance(value, (dict, list)):
            continue
        key = path[-1].lower()
        if not any(token in key for token in SEMANTIC_FIELD_TOKENS):
            continue
        dotted = ".".join(path)
        if len(result) >= 96:
            result["_truncated"] = True
            break
        result[dotted] = value
    return result


def _text_identity(path: Path, text: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(text) if path.suffix.lower() == ".json" else None
    except json.JSONDecodeError:
        payload = None
    if payload is not None:
        bindings = _version_bindings(payload)
        if not bindings:
            return None
        return {
            "experiment_identity": _identity(payload, path),
            "reference_path": str(path),
            "reference_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "version_bindings": bindings,
            "days": _dates_from_payload(payload),
            "required_semantics": _semantic_requirements(payload),
            "parse_mode": "json_structured",
        }

    version_match = re.search(
        r"(?:baseline_|legacy_)?window_cache_version\s*[:=]\s*[`\"']?12\b",
        text,
        flags=re.IGNORECASE,
    )
    if not version_match:
        return None
    return {
        "experiment_identity": path.stem,
        "reference_path": str(path),
        "reference_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "version_bindings": [{"field": "unstructured_text", "value": "12"}],
        "days": sorted(set(DATE_RE.findall(text))),
        "required_semantics": {},
        "parse_mode": "text_regex",
    }


def _mtime_cohorts(records: list[dict[str, Any]], gap_seconds: int = 3600) -> None:
    ordered = sorted(records, key=lambda item: (item["mtime_ns"], item["basename"]))
    cohort = 0
    previous_ns: int | None = None
    for record in ordered:
        current_ns = int(record["mtime_ns"])
        if previous_ns is None or current_ns - previous_ns > gap_seconds * 1_000_000_000:
            cohort += 1
        record["mtime_cohort"] = f"v12_mtime_cohort_{cohort:02d}"
        previous_ns = current_ns


def _hash_same_day_size_groups(records: list[dict[str, Any]]) -> dict[str, list[str]]:
    groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        groups[(record["day"], int(record["size_bytes"]))].append(record)
    content_groups: dict[str, list[str]] = defaultdict(list)
    for variants in groups.values():
        if len(variants) < 2:
            continue
        for record in variants:
            digest = _sha256_file(Path(record["path"]))
            record["payload_sha256"] = digest
            content_groups[digest].append(record["basename"])
    duplicate_groups = {
        digest: sorted(names) for digest, names in content_groups.items() if len(names) > 1
    }
    for digest, names in duplicate_groups.items():
        group_id = f"sha256:{digest}"
        for record in records:
            if record["basename"] in names:
                record["byte_identical_group"] = group_id
    return duplicate_groups


def audit_v12_semantics(
    cache_root: Path,
    *,
    reference_roots: Iterable[Path],
    hash_same_day_size_groups: bool = False,
    expected_count: int | None = None,
) -> dict[str, Any]:
    root = cache_root.expanduser().resolve(strict=True)
    records: list[dict[str, Any]] = []
    for path in sorted(root.iterdir()):
        match = CACHE_RE.fullmatch(path.name)
        if not match or not path.is_file() or path.is_symlink():
            continue
        stat = path.stat()
        records.append(
            {
                "basename": path.name,
                "cache_key_prefix": match.group("prefix"),
                "day": match.group("day"),
                "mtime_ns": int(stat.st_mtime_ns),
                "path": str(path.resolve()),
                "payload_sha256": None,
                "pickle_unpickled": False,
                "size_bytes": int(stat.st_size),
            }
        )
    if expected_count is not None and len(records) != expected_count:
        raise ValueError(f"expected {expected_count} v12 caches, found {len(records)}")

    _mtime_cohorts(records)
    duplicate_groups = _hash_same_day_size_groups(records) if hash_same_day_size_groups else {}
    exact_refs: dict[str, list[dict[str, str]]] = defaultdict(list)
    identities: list[dict[str, Any]] = []
    scanned_files = 0
    scanned_bytes = 0
    for reference_root, path in _iter_text_files(reference_roots):
        try:
            text = path.read_text(encoding="utf-8", errors="strict")
        except (OSError, UnicodeDecodeError):
            continue
        scanned_files += 1
        scanned_bytes += len(text.encode("utf-8"))
        content_sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
        for record in records:
            matches: list[tuple[str, str]] = []
            if record["basename"] in text:
                matches.append(("exact_basename", record["basename"]))
            if record["cache_key_prefix"] in text:
                matches.append(("exact_cache_key_prefix", record["cache_key_prefix"]))
            payload_sha = record["payload_sha256"]
            if payload_sha is not None and payload_sha in text:
                matches.append(("exact_payload_sha256", payload_sha))
            for match_type, value in matches:
                exact_refs[record["basename"]].append(
                    {
                        "match_type": match_type,
                        "matched_value": value,
                        "reference_path": str(path),
                        "reference_root": str(reference_root),
                        "reference_sha256": content_sha,
                    }
                )
        identity = _text_identity(path, text)
        if identity is not None:
            identities.append(identity)

    identities.sort(key=lambda item: (item["experiment_identity"], item["reference_path"]))
    identity_by_day: dict[str, list[int]] = defaultdict(list)
    global_identities: list[int] = []
    for index, identity in enumerate(identities):
        if identity["days"]:
            for day in identity["days"]:
                identity_by_day[day].append(index)
        else:
            global_identities.append(index)

    for record in records:
        exact = sorted(
            exact_refs.get(record["basename"], []),
            key=lambda item: (item["reference_path"], item["match_type"]),
        )
        indexes = sorted(set(identity_by_day.get(record["day"], []) + global_identities))
        semantic_matches = [
            {
                "experiment_identity": identities[index]["experiment_identity"],
                "reference_path": identities[index]["reference_path"],
                "version_bindings": identities[index]["version_bindings"],
            }
            for index in indexes
        ]
        record["exact_references"] = exact
        record["semantic_identity_matches"] = semantic_matches
        if exact:
            classification = "must_retain_exact_identity"
        elif semantic_matches:
            classification = "duplicate_but_variant_semantics_unresolved"
        else:
            classification = "rebuildable_delete_candidate_unreferenced"
        record["classification"] = classification
        record["deletion_authorized"] = False

    files_by_identity: dict[int, list[str]] = defaultdict(list)
    for record in records:
        matched_paths = {item["reference_path"] for item in record["semantic_identity_matches"]}
        for index, identity in enumerate(identities):
            if identity["reference_path"] in matched_paths:
                files_by_identity[index].append(record["basename"])
    for index, identity in enumerate(identities):
        identity["matched_v12_variants"] = sorted(files_by_identity.get(index, []))

    classifications: dict[str, int] = defaultdict(int)
    classification_bytes: dict[str, int] = defaultdict(int)
    for record in records:
        classification = record["classification"]
        classifications[classification] += 1
        classification_bytes[classification] += int(record["size_bytes"])

    audit = {
        "schema_version": "narrowgate.legacy_window_cache_v12_semantic_reference_audit.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "mode": "read_only_no_cache_mutation",
        "cache_root": str(root),
        "reference_roots": [str(path.expanduser().resolve()) for path in reference_roots],
        "safety": {
            "cache_files_deleted": 0,
            "cache_files_modified": 0,
            "pickle_payloads_unpickled": 0,
            "pickletools_payload_materialization": 0,
            "same_day_same_size_payload_hashing_enabled": bool(hash_same_day_size_groups),
            "classification_is_not_deletion_authority": True,
        },
        "summary": {
            "v12_file_count": len(records),
            "distinct_days": len({record["day"] for record in records}),
            "size_bytes": sum(int(record["size_bytes"]) for record in records),
            "semantic_identity_count": len(identities),
            "reference_files_scanned": scanned_files,
            "reference_bytes_scanned": scanned_bytes,
            "byte_identical_groups": len(duplicate_groups),
            "classification_counts": dict(sorted(classifications.items())),
            "classification_size_bytes": dict(sorted(classification_bytes.items())),
        },
        "classification_contract": {
            "must_retain_exact_identity": "Exact basename, cache-key prefix, or payload SHA256 is referenced.",
            "duplicate_but_variant_semantics_unresolved": "A frozen v12 identity requires the day, but evidence does not bind the exact cache-key variant.",
            "rebuildable_delete_candidate_unreferenced": "No exact or version/day semantic reference was found; still not deletion authority.",
        },
        "semantic_identities": identities,
        "byte_identical_groups": duplicate_groups,
        "files": sorted(records, key=lambda item: (item["day"], item["basename"])),
    }
    audit["canonical_audit_sha256"] = _canonical_sha256(
        {
            key: value
            for key, value in audit.items()
            if key not in {"generated_at_utc", "canonical_audit_sha256"}
        }
    )
    return audit


def render_markdown(audit: dict[str, Any]) -> str:
    summary = audit["summary"]
    counts = summary["classification_counts"]
    gib = summary["size_bytes"] / (1024**3)
    lines = [
        "# Legacy v12 Window Cache Semantic Reference Audit",
        "",
        "Last materially modified: 2026-08-04",
        "",
        "Status: read-only; no cache file was deleted, moved, modified, or unpickled.",
        "",
        "## Result",
        "",
        f"- v12 payloads: {summary['v12_file_count']} across {summary['distinct_days']} UTC days ({gib:.2f} GiB).",
        f"- Exact-identity must retain: {counts.get('must_retain_exact_identity', 0)}.",
        f"- Variant semantics unresolved: {counts.get('duplicate_but_variant_semantics_unresolved', 0)}.",
        f"- Unreferenced rebuildable candidates: {counts.get('rebuildable_delete_candidate_unreferenced', 0)}.",
        f"- Byte-identical same-day groups: {summary['byte_identical_groups']}.",
        "",
        "A byte-identical payload is not automatically deletable: the filename digest is a lookup identity and may encode a different feature-context, inference, quality-day, or source-signature contract.",
        "",
        "## Frozen Semantic Identities",
        "",
        "| Identity | Referenced days | Matched v12 variants | Evidence |",
        "|---|---:|---:|---|",
    ]
    for identity in audit["semantic_identities"]:
        lines.append(
            f"| `{identity['experiment_identity']}` | {len(identity['days'])} | {len(identity['matched_v12_variants'])} | `{identity['reference_path']}` |"
        )
    lines.extend(
        [
            "",
            "## Governance",
            "",
            "`must_retain_exact_identity` requires preservation. `duplicate_but_variant_semantics_unresolved` also remains preservation-required until the historical key can be reconstructed or an exact successor artifact is admitted. `rebuildable_delete_candidate_unreferenced` is only a review queue; deletion still requires a fresh hash/reference audit and an explicit execution receipt.",
            "",
            f"Canonical audit SHA256: `{audit['canonical_audit_sha256']}`",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--reference-root", type=Path, action="append", required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    parser.add_argument("--markdown-out", type=Path, required=True)
    parser.add_argument("--expected-count", type=int)
    parser.add_argument("--hash-same-day-size-groups", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    audit = audit_v12_semantics(
        args.cache_root,
        reference_roots=args.reference_root,
        hash_same_day_size_groups=args.hash_same_day_size_groups,
        expected_count=args.expected_count,
    )
    _atomic_write_text(args.json_out, json.dumps(audit, indent=2, sort_keys=True) + "\n")
    _atomic_write_text(args.markdown_out, render_markdown(audit))
    print(json.dumps(audit["summary"], indent=2, sort_keys=True))
    print(f"canonical_audit_sha256={audit['canonical_audit_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
