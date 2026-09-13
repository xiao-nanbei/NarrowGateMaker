"""Read-only integrity and reference audit for reusable replay-cache nodes."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

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
HEX64_RE = re.compile(r"(?<![a-f0-9])[a-f0-9]{64}(?![a-f0-9])")
ARTIFACT_NAME_RE = re.compile(r"[A-Za-z0-9_.-]+\.(?:json|npz|parquet|csv|gz|zst|pkl)")
DAY_RE = re.compile(r"20\d{2}-\d{2}-\d{2}")
NPZ_KEY_RE = re.compile(r"-([a-f0-9]{64})\.npz$")


@dataclass(frozen=True)
class CacheNodePolicy:
    node_id: str
    relative_path: str
    semantic_layer: str
    classification: str
    current_consumers: tuple[str, ...]
    migration_status: str
    deletion_status: str
    rationale: str


NODE_POLICIES = (
    CacheNodePolicy(
        node_id="native_exchange_book_hour_v1",
        relative_path="replay_dag/native_exchange_book_hour_v1",
        semantic_layer="strategy_independent_native_book_input",
        classification="current_reusable",
        current_consumers=(
            "exact_queue_and_order_lifecycle_replay",
            "F06/F09/F10 native-book mechanics",
        ),
        migration_status="safe_with_atomic_copy_hash_verify_and_compatibility_symlink",
        deletion_status="preserve",
        rationale=(
            "This is the active action-independent hourly native-book DAG node; "
            "it is not tied to a closed strategy action."
        ),
    ),
    CacheNodePolicy(
        node_id="cross_venue_fair_price_trade_1s_v1",
        relative_path="replay_dag/cross_venue_fair_price_trade_1s_v1",
        semantic_layer="historical_provider_clock_fair_price_adapter",
        classification="closed_but_frozen_referenced_with_reusable_base_variants",
        current_consumers=(
            "historical fair-center reproduction",
            "F04 historical sensitivity only",
        ),
        migration_status="safe_with_atomic_copy_hash_verify_and_compatibility_symlink",
        deletion_status="prune_only_unreferenced_superseded_implementation_duplicates",
        rationale=(
            "The fair-center action is closed and this 1s provider-clock adapter "
            "does not replace AWS receive-time BABEL inputs. Current frozen "
            "implementation variants remain reproducible inputs; byte-identical "
            "older implementation variants may be pruned separately."
        ),
    ),
    CacheNodePolicy(
        node_id="external_adverse_quote_edge_guard_mechanics_v1",
        relative_path=("replay_dag/external_adverse_quote_edge_guard_mechanics_v1"),
        semantic_layer="outcome_blind_frozen_mechanics_evidence",
        classification="closed_but_frozen_referenced",
        current_consumers=(
            "F04 P2 v1 historical mechanics evidence",
            "P2 exact-opener successor provenance",
        ),
        migration_status="preserve_in_place_absolute_hash_chain",
        deletion_status="preserve",
        rationale=(
            "The report binds exact mechanics payload paths and SHA256 values. "
            "This is frozen evidence, not the reusable external receive-time source."
        ),
    ),
    CacheNodePolicy(
        node_id="p3_touch_window_context_v1",
        relative_path="p3_touch_window_context_v1",
        semantic_layer="fixed_10s_conditional_p3_context",
        classification="closed_but_frozen_referenced",
        current_consumers=(
            "conditional P3 v4.1 historical reproduction",
            "closed scalar and sparse-value adapter reproduction",
        ),
        migration_status="safe_with_atomic_copy_hash_verify_and_compatibility_symlink",
        deletion_status="preserve",
        rationale=(
            "The fixed-10s context is not the new reach-time authority, but frozen "
            "Specs bind exact files and hashes. Its small footprint does not justify "
            "breaking historical reproduction."
        ),
    ),
    CacheNodePolicy(
        node_id="p3_touch_reaches_v1",
        relative_path="p3_touch_reaches_v1",
        semantic_layer="fixed_10s_aggressive_reach_labels",
        classification="closed_but_frozen_referenced",
        current_consumers=("source-aware P3 v3/v4/v4.1 historical reproduction",),
        migration_status="safe_with_atomic_copy_hash_verify_and_compatibility_symlink",
        deletion_status="preserve",
        rationale=(
            "These fixed-horizon reach labels are superseded for authoritative F02 "
            "work by the full reach-time surface, but remain cheap frozen evidence."
        ),
    ),
    CacheNodePolicy(
        node_id="p3_conditional_quote_overlay_v1",
        relative_path="p3_conditional_quote_overlay_v1",
        semantic_layer="closed_scalar_compression_adapter_overlay",
        classification="deletion_candidate",
        current_consumers=("closed conditional-P3 scalar adapter reproduction only",),
        migration_status="migration_not_worthwhile_small_closed_adapter_cache",
        deletion_status="conditional_after_frozen_reproduction_export",
        rationale=(
            "This overlay is specific to the failed scalar compression mapping, is "
            "not consumed by the full reach-time successor, and is deterministically "
            "regenerable from frozen inputs. It is a candidate, not deletion-ready, "
            "until a reproduction bundle or explicit owner receipt is recorded."
        ),
    ),
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(raw).hexdigest()


def _reference_class(path: Path) -> str:
    normalized = path.as_posix().lower()
    name = path.name.lower()
    if "/research/" in normalized and any(
        token in name for token in ("spec", "report", "manifest", "audit")
    ):
        return "frozen_spec_or_evidence"
    if path.suffix == ".py":
        return "code_or_test"
    if "/docs/" in normalized or path.suffix == ".md":
        return "documentation"
    return "other_text"


def _iter_text_files(
    roots: Iterable[Path],
    *,
    excluded_paths: set[Path],
) -> Iterable[Path]:
    seen: set[Path] = set()
    for raw_root in roots:
        root = Path(raw_root).expanduser().resolve()
        candidates = (root,) if root.is_file() else root.rglob("*")
        for path in candidates:
            if (
                not path.is_file()
                or path.suffix.lower() not in TEXT_SUFFIXES
                or path.resolve() in excluded_paths
                or path in seen
            ):
                continue
            if any(part in {".git", ".venv", "__pycache__"} for part in path.parts):
                continue
            seen.add(path)
            yield path


def _npz_key_status(path: Path) -> dict[str, Any]:
    match = NPZ_KEY_RE.search(path.name)
    filename_key = match.group(1) if match else ""
    try:
        with np.load(path, allow_pickle=False) as cached:
            embedded_key = str(np.asarray(cached["cache_key"]).item())
            fields = sorted(cached.files)
    except Exception as exc:
        return {
            "valid": False,
            "reason": f"npz_read_error:{type(exc).__name__}:{exc}",
            "filename_key": filename_key,
            "embedded_key": "",
            "fields": [],
        }
    return {
        "valid": bool(filename_key and embedded_key == filename_key),
        "reason": "" if embedded_key == filename_key else "cache_key_mismatch",
        "filename_key": filename_key,
        "embedded_key": embedded_key,
        "fields": fields,
    }


def _manifest_payload_path(manifest_path: Path, manifest: Mapping[str, Any]) -> Path:
    name = manifest_path.name
    if name.endswith(".parquet.manifest.json"):
        return manifest_path.with_name(name.removesuffix(".manifest.json"))
    declared = manifest.get("data_path") or manifest.get("output_path")
    if declared:
        adjacent = manifest_path.with_name(
            manifest_path.name.removesuffix(".manifest.json") + ".parquet"
        )
        if adjacent.is_file():
            return adjacent
        return Path(str(declared)).expanduser()
    return manifest_path.with_name(manifest_path.name.removesuffix(".manifest.json") + ".parquet")


def _audit_manifest(
    manifest_path: Path,
    *,
    verify_payload_hashes: bool,
    current_hashes: Mapping[str, str],
) -> dict[str, Any]:
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "manifest_path": str(manifest_path),
            "parse_valid": False,
            "error": f"{type(exc).__name__}:{exc}",
        }
    payload_path = _manifest_payload_path(manifest_path, manifest)
    expected_size = manifest.get("data_size_bytes")
    expected_sha = manifest.get("data_sha256") or manifest.get("output_sha256")
    payload_exists = payload_path.is_file()
    observed_size = payload_path.stat().st_size if payload_exists else -1
    observed_sha = (
        _sha256_file(payload_path)
        if payload_exists and expected_sha and verify_payload_hashes
        else ""
    )
    identity_valid: bool | None = None
    identity_sha = str(
        manifest.get("identity_sha256") or manifest.get("cache_identity_sha256") or ""
    )
    if isinstance(manifest.get("identity"), dict) and identity_sha:
        identity_valid = _canonical_sha256(manifest["identity"]) == identity_sha
    elif manifest.get("cache_identity_sha256"):
        output_keys = {
            "cache_identity_sha256",
            "output_path",
            "output_sha256",
            "rows",
            "valid_rows",
            "valid_fraction",
            "reason_counts",
        }
        identity = {key: value for key, value in manifest.items() if key not in output_keys}
        identity_valid = _canonical_sha256(identity) == identity_sha

    implementation = manifest.get("implementation") or {}
    current_implementation = True
    compared_implementation_hashes = 0
    for field, current_sha in current_hashes.items():
        observed = str(implementation.get(field, ""))
        if observed:
            compared_implementation_hashes += 1
            current_implementation = current_implementation and observed == current_sha
    identity = manifest.get("identity") or {}
    for field, current_sha in current_hashes.items():
        observed = str(identity.get(field, ""))
        if observed:
            compared_implementation_hashes += 1
            current_implementation = current_implementation and observed == current_sha

    return {
        "manifest_path": str(manifest_path),
        "parse_valid": True,
        "payload_path": str(payload_path),
        "payload_basename": payload_path.name,
        "payload_exists": payload_exists,
        "payload_size_bytes": observed_size,
        "declared_size_bytes": int(expected_size) if expected_size is not None else None,
        "size_valid": expected_size is None or int(expected_size) == observed_size,
        "declared_payload_sha256": str(expected_sha or ""),
        "observed_payload_sha256": observed_sha,
        "payload_sha256_valid": (
            None if not verify_payload_hashes or not expected_sha else observed_sha == expected_sha
        ),
        "identity_sha256": identity_sha,
        "identity_valid": identity_valid,
        "current_implementation": (
            current_implementation if compared_implementation_hashes else None
        ),
        "utc_day": str(manifest.get("utc_day", "")),
        "variant": str(manifest.get("omitted_venue", "")) or "all_venues",
    }


def _audit_evidence_hash_chain(root: Path, *, verify_payload_hashes: bool) -> dict[str, Any]:
    report_path = root / "20260802" / "report.json"
    if not report_path.is_file():
        return {"report_present": False, "checks": []}
    report = json.loads(report_path.read_text(encoding="utf-8"))
    checks: list[dict[str, Any]] = []
    identities = report.get("identity_hashes") or {}
    for key, value in sorted(identities.items()):
        if not key.endswith("_path"):
            continue
        prefix = key.removesuffix("_path")
        expected_sha = str(identities.get(f"{prefix}_sha256", ""))
        path = Path(str(value)).expanduser()
        exists = path.is_file()
        observed_sha = (
            _sha256_file(path) if exists and expected_sha and verify_payload_hashes else ""
        )
        checks.append(
            {
                "name": prefix,
                "path": str(path),
                "exists": exists,
                "expected_sha256": expected_sha,
                "observed_sha256": observed_sha,
                "sha256_valid": (
                    None
                    if not verify_payload_hashes or not expected_sha
                    else observed_sha == expected_sha
                ),
            }
        )
    return {
        "report_present": True,
        "report_path": str(report_path),
        "identity": str(report.get("identity", "")),
        "status": str(report.get("status", "")),
        "checks": checks,
    }


def _scan_references(
    nodes: list[dict[str, Any]],
    *,
    reference_roots: Iterable[Path],
    excluded_paths: set[Path],
) -> None:
    basename_owners: dict[str, set[int]] = defaultdict(set)
    hash_owners: dict[str, set[int]] = defaultdict(set)
    for index, node in enumerate(nodes):
        for basename in node.pop("_artifact_basenames"):
            basename_owners[basename].add(index)
        for digest in node.pop("_artifact_hashes"):
            if digest:
                hash_owners[digest].add(index)

    reference_sets: list[dict[tuple[str, str], dict[str, Any]]] = [{} for _ in nodes]
    for path in _iter_text_files(reference_roots, excluded_paths=excluded_paths):
        try:
            if path.stat().st_size > 64 * 1024 * 1024:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        path_class = _reference_class(path)
        names = set(ARTIFACT_NAME_RE.findall(text))
        hashes = set(HEX64_RE.findall(text))
        for index, node in enumerate(nodes):
            matches: set[str] = set()
            if node["node_id"] in text:
                matches.add("node_id")
            if node["root"] in text:
                matches.add("root_path")
            if matches:
                key = (str(path), path_class)
                reference_sets[index][key] = {
                    "path": str(path),
                    "class": path_class,
                    "match_types": sorted(matches),
                }
        for name in names.intersection(basename_owners):
            for index in basename_owners[name]:
                key = (str(path), path_class)
                record = reference_sets[index].setdefault(
                    key,
                    {"path": str(path), "class": path_class, "match_types": []},
                )
                record["match_types"] = sorted(
                    set(record["match_types"]).union({"artifact_basename"})
                )
        for digest in hashes.intersection(hash_owners):
            for index in hash_owners[digest]:
                key = (str(path), path_class)
                record = reference_sets[index].setdefault(
                    key,
                    {"path": str(path), "class": path_class, "match_types": []},
                )
                record["match_types"] = sorted(set(record["match_types"]).union({"artifact_hash"}))

    for node, references in zip(nodes, reference_sets, strict=True):
        rows = sorted(references.values(), key=lambda row: (row["class"], row["path"]))
        node["references"] = rows
        node["reference_summary"] = dict(sorted(Counter(row["class"] for row in rows).items()))


def audit_replay_cache_nodes(
    *,
    cache_root: Path,
    external_cache_root: Path,
    repository_root: Path,
    reference_roots: Iterable[Path],
    verify_payload_hashes: bool = True,
    excluded_paths: Iterable[Path] = (),
) -> dict[str, Any]:
    """Audit selected non-window caches without modifying any cache artifact."""

    cache_base = Path(cache_root).expanduser().resolve()
    external_base = Path(external_cache_root).expanduser().resolve()
    repo = Path(repository_root).expanduser().resolve()
    from models.native_exchange_book_cache import native_book_parser_identity
    from models.replay_cache_dag import REPLAY_WINDOW_CACHE_GRAPH_IDENTITY

    current_hashes_by_node = {
        "native_exchange_book_hour_v1": {
            "parser_identity_sha256": native_book_parser_identity(),
            "dag_identity_sha256": REPLAY_WINDOW_CACHE_GRAPH_IDENTITY,
        },
        "cross_venue_fair_price_trade_1s_v1": {
            "adapter_sha256": _sha256_file(
                repo / "research/families/f04_external_market_alpha/audit/"
                "cross_venue_causal_fair_price.py"
            ),
            "estimator_sha256": _sha256_file(repo / "strategy/cross_venue_fair_price.py"),
        },
    }
    nodes: list[dict[str, Any]] = []
    manifest_records_by_node: dict[str, list[dict[str, Any]]] = {}
    for policy in NODE_POLICIES:
        root = cache_base / policy.relative_path
        files = sorted(path for path in root.rglob("*") if path.is_file()) if root.exists() else []
        manifests = sorted(root.rglob("*.manifest.json")) if root.exists() else []
        manifest_records = [
            _audit_manifest(
                path,
                verify_payload_hashes=verify_payload_hashes,
                current_hashes=current_hashes_by_node.get(policy.node_id, {}),
            )
            for path in manifests
        ]
        manifest_records_by_node[policy.node_id] = manifest_records
        npz_records = [
            {"path": str(path), **_npz_key_status(path)} for path in files if path.suffix == ".npz"
        ]
        artifact_basenames = {path.name for path in files}
        artifact_hashes: set[str] = set()
        for record in manifest_records:
            artifact_hashes.update(
                {
                    str(record.get("declared_payload_sha256", "")),
                    str(record.get("identity_sha256", "")),
                }
            )
        for record in npz_records:
            artifact_hashes.update(
                {
                    str(record.get("filename_key", "")),
                    str(record.get("embedded_key", "")),
                }
            )
        days = {match.group(0) for path in files if (match := DAY_RE.search(path.as_posix()))}
        evidence = (
            _audit_evidence_hash_chain(root, verify_payload_hashes=verify_payload_hashes)
            if policy.node_id == "external_adverse_quote_edge_guard_mechanics_v1"
            else None
        )
        node = {
            "node_id": policy.node_id,
            "root": str(root),
            "external_migration_target": str(external_base / policy.relative_path),
            "present": root.is_dir(),
            "size_bytes": sum(path.stat().st_size for path in files),
            "file_count": len(files),
            "distinct_days": len(days),
            "suffix_counts": dict(sorted(Counter(path.suffix for path in files).items())),
            "manifest_integrity": {
                "manifest_count": len(manifest_records),
                "parse_errors": sum(not row.get("parse_valid", False) for row in manifest_records),
                "missing_payloads": sum(
                    row.get("parse_valid", False) and not row.get("payload_exists", False)
                    for row in manifest_records
                ),
                "size_mismatches": sum(row.get("size_valid") is False for row in manifest_records),
                "payload_hashes_verified": sum(
                    row.get("payload_sha256_valid") is not None for row in manifest_records
                ),
                "payload_hash_mismatches": sum(
                    row.get("payload_sha256_valid") is False for row in manifest_records
                ),
                "identity_hashes_checked": sum(
                    row.get("identity_valid") is not None for row in manifest_records
                ),
                "identity_hash_mismatches": sum(
                    row.get("identity_valid") is False for row in manifest_records
                ),
                "current_implementation_artifacts": sum(
                    row.get("current_implementation") is True for row in manifest_records
                ),
                "superseded_implementation_artifacts": sum(
                    row.get("current_implementation") is False for row in manifest_records
                ),
            },
            "npz_integrity": {
                "npz_count": len(npz_records),
                "embedded_cache_keys_checked": len(npz_records),
                "embedded_cache_key_mismatches": sum(not row["valid"] for row in npz_records),
            },
            "evidence_hash_chain": evidence,
            "semantic_layer": policy.semantic_layer,
            "classification": policy.classification,
            "current_consumers": list(policy.current_consumers),
            "migration_status": policy.migration_status,
            "deletion_status": policy.deletion_status,
            "rationale": policy.rationale,
            "_artifact_basenames": artifact_basenames,
            "_artifact_hashes": artifact_hashes,
        }
        nodes.append(node)

    excluded = {Path(path).expanduser().resolve() for path in excluded_paths}
    _scan_references(
        nodes,
        reference_roots=reference_roots,
        excluded_paths=excluded,
    )

    exact_references_by_node: dict[str, set[str]] = {}
    for node in nodes:
        exact_references_by_node[node["node_id"]] = {
            row["path"]
            for row in node["references"]
            if set(row["match_types"]).intersection({"artifact_basename", "artifact_hash"})
        }

    deletion_candidates: list[dict[str, Any]] = []
    cross_records = manifest_records_by_node["cross_venue_fair_price_trade_1s_v1"]
    current_payload_hashes = {
        row["declared_payload_sha256"]
        for row in cross_records
        if row.get("current_implementation") is True
    }
    cross_exact_refs = exact_references_by_node["cross_venue_fair_price_trade_1s_v1"]
    for row in cross_records:
        if (
            row.get("parse_valid")
            and row.get("current_implementation") is False
            and row.get("declared_payload_sha256") in current_payload_hashes
            and not cross_exact_refs
        ):
            payload_path = Path(row["payload_path"])
            manifest_path = Path(row["manifest_path"])
            deletion_candidates.append(
                {
                    "node_id": "cross_venue_fair_price_trade_1s_v1",
                    "paths": [str(payload_path), str(manifest_path)],
                    "size_bytes": sum(
                        path.stat().st_size for path in (payload_path, manifest_path)
                    ),
                    "reason": (
                        "superseded implementation identity with byte-identical "
                        "current payload and no exact artifact/hash repository reference"
                    ),
                    "read_only_audit_only": True,
                    "owner_approval_still_required": True,
                }
            )

    overlay = next(node for node in nodes if node["node_id"] == "p3_conditional_quote_overlay_v1")
    overlay["conditional_deletion_candidate_summary"] = {
        "file_count": overlay["file_count"],
        "size_bytes": overlay["size_bytes"],
        "deletion_ready": False,
        "required_before_deletion": (
            "freeze a deterministic reproduction bundle or explicit owner receipt"
        ),
    }

    return {
        "schema_version": "narrowgate.non_window_replay_cache_audit.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "mode": "read_only_no_cache_mutation",
        "repository_root": str(repo),
        "cache_root": str(cache_base),
        "external_cache_root": str(external_base),
        "payload_hash_verification_enabled": verify_payload_hashes,
        "cache_files_modified": 0,
        "cache_files_deleted": 0,
        "nodes": nodes,
        "deletion_candidates": deletion_candidates,
        "summary": {
            "node_count": len(nodes),
            "total_size_bytes": sum(node["size_bytes"] for node in nodes),
            "current_reusable_nodes": sum(
                node["classification"] == "current_reusable" for node in nodes
            ),
            "frozen_referenced_nodes": sum(
                "frozen_referenced" in node["classification"] for node in nodes
            ),
            "node_level_deletion_candidates": sum(
                node["classification"] == "deletion_candidate" for node in nodes
            ),
            "file_groups_ready_for_owner_review": len(deletion_candidates),
            "file_group_candidate_bytes": sum(row["size_bytes"] for row in deletion_candidates),
        },
    }


def audit_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def audit_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Non-window Replay Cache Audit",
        "",
        "Last materially modified: 2026-08-04",
        "",
        "This is a read-only audit. No cache file was modified or deleted.",
        "",
        "| Node | Size GiB | Classification | Migration | Deletion |",
        "|---|---:|---|---|---|",
    ]
    for node in payload["nodes"]:
        lines.append(
            "| {node} | {size:.3f} | `{classification}` | `{migration}` | `{deletion}` |".format(
                node=node["node_id"],
                size=node["size_bytes"] / (1024**3),
                classification=node["classification"],
                migration=node["migration_status"],
                deletion=node["deletion_status"],
            )
        )
    lines.extend(
        [
            "",
            "## Findings",
            "",
        ]
    )
    for node in payload["nodes"]:
        integrity = node["manifest_integrity"]
        npz = node["npz_integrity"]
        lines.extend(
            [
                f"### `{node['node_id']}`",
                "",
                node["rationale"],
                "",
                f"- Files: {node['file_count']}; distinct days: {node['distinct_days']}; "
                f"size: {node['size_bytes'] / (1024**3):.3f} GiB.",
                f"- Manifest checks: {integrity['manifest_count']} parsed; "
                f"{integrity['payload_hash_mismatches']} payload hash mismatches; "
                f"{integrity['identity_hash_mismatches']} identity mismatches.",
                f"- NPZ key checks: {npz['embedded_cache_keys_checked']} checked; "
                f"{npz['embedded_cache_key_mismatches']} mismatches.",
                f"- Repository references: {node['reference_summary']}.",
                f"- Migration: `{node['migration_status']}`.",
                f"- Deletion: `{node['deletion_status']}`.",
                "",
            ]
        )
    summary = payload["summary"]
    lines.extend(
        [
            "## Candidate Pruning",
            "",
            f"The audit found {summary['file_groups_ready_for_owner_review']} exact "
            "superseded file groups totaling "
            f"{summary['file_group_candidate_bytes'] / (1024**3):.3f} GiB. They "
            "were not deleted. Each group still requires a fresh content/reference "
            "audit and explicit owner-authorized deletion receipt.",
            "",
            "The entire `p3_conditional_quote_overlay_v1` node is only a conditional "
            "candidate. Its small closed-adapter payload remains in place until a "
            "frozen reproduction bundle or owner receipt exists.",
            "",
            "## BABEL Boundary",
            "",
            "`cross_venue_fair_price_trade_1s_v1` is a transport-unsupported "
            "historical provider-clock adapter. It can reproduce closed fair-center "
            "and historical sensitivity work, but it is not a substitute for F04 "
            "BABEL AWS receive-time tapes. The P2 mechanics cache is frozen derived "
            "evidence; the reusable BABEL base remains the receive-time source tapes "
            "and their ledger on ORICO.",
            "",
        ]
    )
    return "\n".join(lines)
