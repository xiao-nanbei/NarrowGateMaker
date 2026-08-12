#!/usr/bin/env python3
"""Build a conservative, read-only reference graph for internal caches."""

from __future__ import annotations

import argparse
import csv
import json
import stat
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root  # noqa: E402

LEGACY_CACHE_ROOT = Path.home() / "Library/Caches/NarrowGate_BTCUSDC"
ORICO_ROOT = data_root(ROOT)
OLD_DATA_ROOT = Path.home() / "MarketData/NarrowGate_BTCUSDC"
CLASSIFICATIONS = {
    "currently_referenced",
    "frozen_historical_referenced",
    "superseded_but_referenced",
    "safely_unreferenced_deletion_candidate",
    "unknown_manual_review",
}
REFERENCE_SUFFIXES = {".csv", ".json", ".manifest", ".md", ".py", ".sha256", ".toml", ".txt", ".yaml", ".yml"}
MAX_REFERENCE_FILE_BYTES = 64 * 1024 * 1024


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _tree_stats(path: Path) -> dict[str, Any]:
    root_stat = path.lstat()
    if stat.S_ISLNK(root_stat.st_mode):
        raise ValueError(f"cache item must not be a symlink: {path}")
    files = [path] if path.is_file() else [item for item in path.rglob("*") if item.is_file()]
    logical = 0
    allocated = 0
    newest = root_stat.st_mtime_ns
    oldest = root_stat.st_mtime_ns
    inodes: set[tuple[int, int]] = set()
    hardlinks = 0
    for item in files:
        current = item.lstat()
        if stat.S_ISLNK(current.st_mode):
            raise ValueError(f"cache payload must not be a symlink: {item}")
        logical += current.st_size
        allocated += current.st_blocks * 512
        newest = max(newest, current.st_mtime_ns)
        oldest = min(oldest, current.st_mtime_ns)
        inodes.add((current.st_dev, current.st_ino))
        hardlinks += int(current.st_nlink > 1)
    return {
        "path": str(path),
        "realpath": str(path.resolve()),
        "size_bytes": logical,
        "allocated_bytes": allocated,
        "inode_count": len(inodes),
        "file_count": len(files),
        "hardlinked_file_count": hardlinks,
        "oldest_mtime_ns": oldest,
        "newest_mtime_ns": newest,
    }


def _mtime_iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, UTC).isoformat().replace("+00:00", "Z")


def _record(
    path: Path,
    classification: str,
    reason: str,
    *,
    references: list[dict[str, str]] | None = None,
    item_type: str,
    protected_by: list[str] | None = None,
) -> dict[str, Any]:
    if classification not in CLASSIFICATIONS:
        raise ValueError(f"unknown classification: {classification}")
    result = _tree_stats(path)
    result.update(
        {
            "classification": classification,
            "item_type": item_type,
            "reason": reason,
            "references": references or [],
            "protected_by": sorted(set(protected_by or [])),
        }
    )
    return result


def _current_f07_windows(orico_root: Path) -> tuple[set[str], list[dict[str, str]]]:
    protected: set[str] = set()
    evidence: list[dict[str, str]] = []
    pattern = "cache/replay_dag/f07_order_lifecycle_v2_40day_v1_5*/window_cache_index.json"
    for index_path in sorted(orico_root.glob(pattern)):
        payload = _load(index_path)
        days = payload.get("days")
        if not isinstance(days, list):
            raise ValueError(f"invalid F07 window index: {index_path}")
        for row in days:
            if not isinstance(row, dict) or not isinstance(row.get("path"), str):
                raise ValueError(f"invalid F07 window row: {index_path}")
            candidate = Path(row["path"])
            if not candidate.is_file():
                raise ValueError(f"F07 protected window is missing: {candidate}")
            current = candidate.stat()
            if current.st_size != row.get("size_bytes"):
                raise ValueError(f"F07 protected window size drift: {candidate}")
            protected.add(candidate.name)
        evidence.append({"kind": "current_f07_window_index", "path": str(index_path)})
    return protected, evidence


def _legacy_records(
    cache_root: Path,
    legacy_audit: dict[str, Any],
    v12_audit: dict[str, Any],
    current_f07: set[str],
) -> list[dict[str, Any]]:
    direct_by_name = {
        row["basename"]: row
        for row in legacy_audit.get("files", [])
        if isinstance(row, dict) and isinstance(row.get("basename"), str)
    }
    v12_by_name = {
        row["basename"]: row
        for row in v12_audit.get("files", [])
        if isinstance(row, dict) and isinstance(row.get("basename"), str)
    }
    result: list[dict[str, Any]] = []
    for path in sorted((cache_root / "window_cache").glob("*_tick_window_v*.pkl")):
        direct = direct_by_name.get(path.name, {})
        version = int(path.name.split("_tick_window_v", 1)[1].split("_", 1)[0])
        references = list(direct.get("references") or [])
        if path.name in current_f07:
            classification = "currently_referenced"
            reason = "hash/size-bound by the current F07 40-day strict-native input index"
            protected_by = ["F07 current 40-day v13", "current v9 control compatibility"]
        elif version == 13 and references:
            classification = "frozen_historical_referenced"
            reason = "exact basename appears in a frozen/evidence manifest"
            protected_by = ["frozen exact manifest", "v13 compatibility"]
        elif version == 13:
            classification = "unknown_manual_review"
            reason = "v13 compatibility window has no exact reference, but current governance preserves all v13 variants"
            protected_by = ["v13 preserve-all governance"]
        elif version == 12:
            semantic = v12_by_name.get(path.name, {})
            semantic_refs = semantic.get("semantic_references") or semantic.get("semantic_matches") or []
            references.extend(semantic_refs)
            classification = "superseded_but_referenced"
            reason = "v12 variant identity remains unresolved against version/day semantic bindings"
            protected_by = ["F06/F09 v12 semantic contracts"]
        else:
            classification = "unknown_manual_review"
            reason = "unexpected legacy version present after the v10/v11 prune receipt"
            protected_by = ["manual review"]
        result.append(
            _record(
                path,
                classification,
                reason,
                references=references,
                item_type=f"legacy_window_v{version}",
                protected_by=protected_by,
            )
        )
    return result


def _directory_records(cache_root: Path) -> list[dict[str, Any]]:
    window_root = cache_root / "window_cache"
    fixed: dict[str, tuple[str, str, list[str]]] = {
        "active_order_queue_tape_v3": (
            "frozen_historical_referenced",
            "frozen F06 manifests reference the relocated legacy cache root",
            ["F06 exact lifecycle evidence"],
        ),
        "paired_action_resolution_mechanics_v1": (
            "frozen_historical_referenced",
            "frozen F06 feasibility identities and default code path consume this node",
            ["F06 frozen mechanics"],
        ),
        "paired_action_resolution_sparse_tape_v1": (
            "frozen_historical_referenced",
            "frozen F06 feasibility identities and default code path consume this node",
            ["F06 frozen sparse tape"],
        ),
        "paired_placement_mechanics_v2": (
            "frozen_historical_referenced",
            "frozen F06 placement manifests reference the relocated legacy cache root",
            ["F06 exact placement mechanics"],
        ),
        "request_state_mechanics_v1": (
            "superseded_but_referenced",
            "historical request-state cache retained for frozen reproduction",
            ["F06 historical reproduction"],
        ),
        "request_state_mechanics_v2": (
            "frozen_historical_referenced",
            "frozen request-state panel manifests bind this relocated root",
            ["F06 request-state evidence"],
        ),
        "request_state_mechanics_v2_corrected_ml_context": (
            "frozen_historical_referenced",
            "corrected ML-context mechanics cache is part of frozen F06 evidence",
            ["F06 corrected context evidence"],
        ),
        "request_state_mechanics_v3": (
            "frozen_historical_referenced",
            "frozen request-state v3 panel manifests bind this relocated root",
            ["F06 request-state evidence"],
        ),
        "components_v1": (
            "superseded_but_referenced",
            "legacy component reader/default path remains in code and a frozen F09 failure records component reads",
            ["legacy read compatibility", "F09 historical reproduction"],
        ),
    }
    return [
        _record(
            window_root / name,
            classification,
            reason,
            item_type="legacy_directory_cache",
            protected_by=protected,
        )
        for name, (classification, reason, protected) in fixed.items()
        if (window_root / name).exists()
    ]


def _non_window_records(cache_root: Path, audit: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    node_classes = {
        "native_exchange_book_hour_v1": "currently_referenced",
        "cross_venue_fair_price_trade_1s_v1": "frozen_historical_referenced",
        "external_adverse_quote_edge_guard_mechanics_v1": "frozen_historical_referenced",
        "p3_touch_window_context_v1": "frozen_historical_referenced",
        "p3_touch_reaches_v1": "frozen_historical_referenced",
        "p3_conditional_quote_overlay_v1": "unknown_manual_review",
    }
    for node in audit.get("nodes", []):
        node_id = node.get("node_id")
        if node_id not in node_classes:
            continue
        path = Path(node["root"])
        classification = node_classes[node_id]
        reason = str(node.get("rationale") or node.get("classification") or "audited cache node")
        result.append(
            _record(
                path,
                classification,
                reason,
                references=list(node.get("references") or []),
                item_type="replay_dag_node" if "replay_dag" in str(path) else "p3_cache_node",
                protected_by=[str(node.get("classification", "existing node audit"))],
            )
        )
    return result


def _candidate_tokens(group: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for value in group.get("paths", []):
        path = Path(value)
        tokens.update({str(path), path.name})
        if path.name.endswith(".manifest.json") and path.is_file():
            payload = _load(path)
            for key in ("cache_identity_sha256", "output_sha256"):
                token = payload.get(key)
                if isinstance(token, str) and token:
                    tokens.add(token)
    return tokens


def _candidate_reference_hits(
    groups: list[dict[str, Any]],
    reference_roots: list[Path],
) -> tuple[dict[int, list[dict[str, str]]], dict[str, int]]:
    token_owners: dict[str, set[int]] = {}
    excluded: set[Path] = set()
    for index, group in enumerate(groups):
        excluded.update(Path(value).resolve() for value in group.get("paths", []))
        for token in _candidate_tokens(group):
            token_owners.setdefault(token, set()).add(index)
    if not token_owners:
        return {}, {"files_scanned": 0, "bytes_scanned": 0}
    hits: dict[int, list[dict[str, str]]] = {}
    roots = [root for root in reference_roots if root.exists()]
    suffix_globs = [f"*.{suffix.lstrip('.')}" for suffix in sorted(REFERENCE_SUFFIXES)]
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", prefix="narrowgate-cache-tokens-", suffix=".txt") as handle:
        for token in sorted(token_owners):
            handle.write(token + "\n")
        handle.flush()
        command = ["rg", "--json", "--fixed-strings", "--only-matching", "--no-messages"]
        for value in suffix_globs:
            command.extend(["--glob", value])
        command.extend(["--glob", "!**/*replay_cache_*audit*"])
        command.extend(["--file", handle.name])
        command.extend(str(root) for root in roots)
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        assert process.stdout is not None
        matched_paths: set[Path] = set()
        for line in process.stdout:
            event = json.loads(line)
            if event.get("type") != "match":
                continue
            data = event["data"]
            resolved = Path(data["path"]["text"]).resolve()
            if resolved in excluded:
                continue
            if resolved.stat().st_size > MAX_REFERENCE_FILE_BYTES:
                continue
            matched_paths.add(resolved)
            for submatch in data.get("submatches", []):
                token = submatch["match"]["text"]
                for index in token_owners.get(token, ()):
                    hits.setdefault(index, []).append({"path": str(resolved), "matched_token": token})
        stderr = process.stderr.read() if process.stderr is not None else ""
        return_code = process.wait()
        if return_code not in (0, 1):
            raise RuntimeError(f"rg reference scan failed ({return_code}): {stderr.strip()}")
    bytes_scanned = sum(path.stat().st_size for path in matched_paths)
    return hits, {
        "engine": "rg_fixed_strings_json",
        "matched_files": len(matched_paths),
        "matched_file_bytes": bytes_scanned,
        "reference_root_count": len(roots),
        "token_count": len(token_owners),
    }


def _candidate_records(
    non_window_audit: dict[str, Any],
    reference_roots: list[Path],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    result: list[dict[str, Any]] = []
    groups = [group for group in non_window_audit.get("deletion_candidates", []) if isinstance(group, dict)]
    hits, scan_summary = _candidate_reference_hits(groups, reference_roots)
    for index, group in enumerate(groups):
        paths = [Path(value) for value in group.get("paths", [])]
        if not paths or any(not path.is_file() for path in paths):
            continue
        stats = [_tree_stats(path) for path in paths]
        references = sorted(hits.get(index, []), key=lambda row: (row["path"], row["matched_token"]))
        if references:
            classification = "frozen_historical_referenced"
            reason = "candidate token is referenced outside its own payload/manifest; preserve"
            protected_by = ["fresh repo/ORICO reverse-reference scan"]
        else:
            classification = "safely_unreferenced_deletion_candidate"
            reason = str(group.get("reason"))
            protected_by = []
        result.append(
            {
                "path": str(paths[0].parent),
                "paths": [str(path) for path in paths],
                "realpath": str(paths[0].parent.resolve()),
                "size_bytes": sum(row["size_bytes"] for row in stats),
                "allocated_bytes": sum(row["allocated_bytes"] for row in stats),
                "inode_count": sum(row["inode_count"] for row in stats),
                "file_count": len(paths),
                "hardlinked_file_count": sum(row["hardlinked_file_count"] for row in stats),
                "oldest_mtime_ns": min(row["oldest_mtime_ns"] for row in stats),
                "newest_mtime_ns": max(row["newest_mtime_ns"] for row in stats),
                "classification": classification,
                "item_type": "byte_identical_superseded_cross_venue_variant",
                "reason": reason,
                "references": references,
                "protected_by": protected_by,
            }
        )
    return result, scan_summary


def _repo_ephemeral_records(repo_root: Path) -> list[dict[str, Any]]:
    paths = [repo_root / ".pytest_cache", repo_root / ".ruff_cache"]
    paths.extend(
        path
        for path in repo_root.rglob("__pycache__")
        if ".venv" not in path.parts and ".git" not in path.parts
    )
    return [
        _record(
            path,
            "safely_unreferenced_deletion_candidate",
            "interpreter/test/linter cache; reproducible and never a research artifact",
            item_type="repo_ephemeral_cache",
        )
        for path in sorted(set(paths))
        if path.exists()
    ]


def _orico_reference_targets(orico_root: Path) -> list[dict[str, Any]]:
    patterns = {
        "F03 current/in-progress": "cache/replay_dag/f03_causal_v12_1s*",
        "F07 current/in-progress": "cache/replay_dag/f07_order_lifecycle_v2_40day*",
        "current live-held BER control": "reports/current_live_held_ber_baseline_40d_20260809*",
        "q90 frozen diagnostics": "reports/buy_q90_*",
    }
    result: list[dict[str, Any]] = []
    for role, pattern in patterns.items():
        for path in sorted(orico_root.glob(pattern)):
            if not path.exists():
                continue
            stats = _tree_stats(path)
            result.append({"role": role, **stats, "storage_scope": "orico_reference_only"})
    return result


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    cache_root = args.cache_root.expanduser().resolve(strict=True)
    repo_root = args.repo_root.expanduser().resolve(strict=True)
    orico_root = args.orico_root.expanduser().resolve(strict=True)
    legacy_audit = _load(args.legacy_audit.expanduser().resolve(strict=True))
    v12_audit = _load(args.v12_audit.expanduser().resolve(strict=True))
    non_window_audit = _load(args.non_window_audit.expanduser().resolve(strict=True))
    current_f07, f07_evidence = _current_f07_windows(orico_root)

    items = []
    items.extend(_legacy_records(cache_root, legacy_audit, v12_audit, current_f07))
    items.extend(_directory_records(cache_root))
    items.extend(_non_window_records(cache_root, non_window_audit))
    candidate_reference_roots = [
        repo_root,
        orico_root / "audit",
        orico_root / "backtest_results_btcusdc",
        orico_root / "cache",
        orico_root / "model_runs",
        orico_root / "reports",
    ]
    candidate_items, candidate_scan_summary = _candidate_records(
        non_window_audit,
        candidate_reference_roots,
    )
    items.extend(candidate_items)
    items.extend(_repo_ephemeral_records(repo_root))
    items.sort(key=lambda row: (row["classification"], row["item_type"], row["path"]))

    counts = Counter(row["classification"] for row in items)
    sizes = Counter()
    allocated = Counter()
    for row in items:
        sizes[row["classification"]] += int(row["size_bytes"])
        allocated[row["classification"]] += int(row["allocated_bytes"])
    candidates = [row for row in items if row["classification"] == "safely_unreferenced_deletion_candidate"]
    nested_candidate_items = [
        row for row in items if row["item_type"] == "byte_identical_superseded_cross_venue_variant"
    ]
    nested_candidate_bytes = sum(
        int(row["size_bytes"])
        for row in nested_candidate_items
    )
    nested_candidate_allocated = sum(
        int(row["allocated_bytes"])
        for row in nested_candidate_items
    )
    exclusive_sizes = Counter(sizes)
    exclusive_allocated = Counter(allocated)
    exclusive_sizes["frozen_historical_referenced"] -= nested_candidate_bytes
    exclusive_allocated["frozen_historical_referenced"] -= nested_candidate_allocated
    protected_classes = {
        "currently_referenced",
        "frozen_historical_referenced",
        "superseded_but_referenced",
    }
    legacy_items = [row for row in items if row["item_type"].startswith("legacy_window_v")]
    legacy_protected = [row for row in legacy_items if row["classification"] in protected_classes]
    internal_cache_stats = _tree_stats(cache_root)
    return {
        "schema_version": "narrowgate.internal_cache_reference_graph.v1",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "mode": "read_only_zero_large_write",
        "mutations": {"cache_files_deleted": 0, "cache_files_modified": 0, "cache_files_moved": 0},
        "roots": {
            "repo": str(repo_root),
            "internal_cache": str(cache_root),
            "retired_data_root": str(OLD_DATA_ROOT),
            "retired_data_root_exists": OLD_DATA_ROOT.exists(),
            "orico_reference_root": str(orico_root),
        },
        "path_aliases": [
            {
                "recorded_prefix": str(OLD_DATA_ROOT / "window_cache"),
                "resolved_prefix": str(cache_root / "window_cache"),
                "kind": "data_paths.relocate_marketdata_path_compatibility_rule",
                "filesystem_symlink": False,
            },
            {
                "recorded_prefix": str(OLD_DATA_ROOT),
                "resolved_prefix": str(orico_root),
                "kind": "data_paths.relocate_marketdata_path_compatibility_rule",
                "filesystem_symlink": False,
            },
        ],
        "reference_inputs": {
            "legacy_audit": str(args.legacy_audit.resolve()),
            "v12_semantic_audit": str(args.v12_audit.resolve()),
            "non_window_audit": str(args.non_window_audit.resolve()),
            "current_f07_evidence": f07_evidence,
            "candidate_reverse_reference_roots": [str(path) for path in candidate_reference_roots],
            "candidate_reverse_reference_scan": candidate_scan_summary,
        },
        "summary": {
            "internal_cache_size_bytes": internal_cache_stats["size_bytes"],
            "internal_cache_allocated_bytes": internal_cache_stats["allocated_bytes"],
            "item_count": len(items),
            "classification_counts": dict(sorted(counts.items())),
            "classification_size_bytes": dict(sorted(sizes.items())),
            "classification_allocated_bytes": dict(sorted(allocated.items())),
            "classification_exclusive_size_bytes": dict(sorted(exclusive_sizes.items())),
            "classification_exclusive_allocated_bytes": dict(sorted(exclusive_allocated.items())),
            "classification_item_sizes_overlap": True,
            "candidate_count": len(candidates),
            "candidate_size_bytes": sum(int(row["size_bytes"]) for row in candidates),
            "candidate_allocated_bytes": sum(int(row["allocated_bytes"]) for row in candidates),
            "candidate_inode_count": sum(int(row["inode_count"]) for row in candidates),
            "protected_current_f07_window_count": len(current_f07),
            "protected_or_referenced_item_count": sum(counts[name] for name in protected_classes),
            "protected_or_referenced_size_bytes": sum(exclusive_sizes[name] for name in protected_classes),
            "legacy_window_count": len(legacy_items),
            "legacy_window_protected_or_referenced_count": len(legacy_protected),
            "legacy_window_protected_or_referenced_size_bytes": sum(
                int(row["size_bytes"]) for row in legacy_protected
            ),
            "legacy_window_safe_candidate_count": sum(
                row["classification"] == "safely_unreferenced_deletion_candidate" for row in legacy_items
            ),
            "legacy_window_unknown_count": sum(
                row["classification"] == "unknown_manual_review" for row in legacy_items
            ),
        },
        "items": items,
        "orico_reference_targets": _orico_reference_targets(orico_root),
    }


def _write_csv(path: Path, payload: dict[str, Any]) -> None:
    fields = [
        "classification",
        "item_type",
        "path",
        "paths_json",
        "size_bytes",
        "allocated_bytes",
        "inode_count",
        "file_count",
        "oldest_mtime_utc",
        "newest_mtime_utc",
        "reason",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in payload["items"]:
            writer.writerow(
                {
                    "classification": row["classification"],
                    "item_type": row["item_type"],
                    "path": row["path"],
                    "paths_json": json.dumps(row.get("paths", [row["path"]]), separators=(",", ":")),
                    "size_bytes": row["size_bytes"],
                    "allocated_bytes": row["allocated_bytes"],
                    "inode_count": row["inode_count"],
                    "file_count": row["file_count"],
                    "oldest_mtime_utc": _mtime_iso(row["oldest_mtime_ns"]),
                    "newest_mtime_utc": _mtime_iso(row["newest_mtime_ns"]),
                    "reason": row["reason"],
                }
            )


def _gib(value: int) -> str:
    return f"{value / (1024 ** 3):.3f}"


def _markdown(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    by_type = Counter()
    for row in payload["items"]:
        if row["classification"] == "safely_unreferenced_deletion_candidate":
            by_type[row["item_type"]] += int(row["allocated_bytes"])
    lines = [
        "# Internal Cache Reference Audit",
        "",
        "Last materially modified: 2026-08-05",
        "",
        "Status: read-only audit complete; no cache/raw/frozen artifact was deleted, moved, or modified.",
        "",
        "## Result",
        "",
        f"- Internal cache logical size: **{_gib(summary['internal_cache_size_bytes'])} GiB**.",
        f"- Audited deletion candidates: **{_gib(summary['candidate_allocated_bytes'])} GiB allocated**, "
        f"{summary['candidate_inode_count']} inodes.",
        f"- Protected/referenced items: **{summary['protected_or_referenced_item_count']}**, "
        f"{_gib(summary['protected_or_referenced_size_bytes'])} GiB logical after nested-item de-duplication.",
        f"- Monolithic windows: **{summary['legacy_window_protected_or_referenced_count']}/"
        f"{summary['legacy_window_count']} protected**, 0 safe candidates, "
        f"{summary['legacy_window_unknown_count']} manual-review item.",
        f"- Protected current F07 v13 windows: **{summary['protected_current_f07_window_count']}**.",
        "- All v12 windows remain protected because semantic variant ownership is unresolved.",
        "- All v13 windows remain protected; the single no-exact-reference variant is manual review, not a candidate.",
        "",
        "## Classification",
        "",
        "| Class | Items | Logical GiB |",
        "|---|---:|---:|",
    ]
    for name, count in summary["classification_counts"].items():
        lines.append(f"| `{name}` | {count} | {_gib(summary['classification_exclusive_size_bytes'][name])} |")
    lines.extend(["", "## Recommended Batches", ""])
    for index, (name, size) in enumerate(sorted(by_type.items(), key=lambda item: -item[1]), start=1):
        if name == "byte_identical_superseded_cross_venue_variant":
            label = "Byte-identical superseded cross-venue variants"
        else:
            label = "Repo-local interpreter/test/linter caches"
        lines.append(f"{index}. {label}: {_gib(size)} GiB allocated. Re-run the validator immediately before any owner-approved deletion.")
    lines.extend(
        [
            "",
            "## Preserve / Manual Review",
            "",
            "- `window_cache` v12: superseded but semantically referenced; do not delete until each variant is resolved.",
            "- `window_cache` v13: current/frozen compatibility surface; preserve all variants, including the unmatched 2026-07-25 file.",
            "- `components_v1`: code still has a legacy reader/default path and frozen F09 evidence records component reads.",
            "- F06 mechanics/queue/request-state directories: frozen manifests bind their relocated legacy paths.",
            "- Old q90 derived outputs on ORICO are outside this deletion scope; their 40 current v13 inputs are protected.",
            "- F03 failed v1 and partial v2 roots are on ORICO. The current v2 salvage decision is unresolved, so none is an internal-disk deletion candidate.",
            "",
            "## Alias Audit",
            "",
            "The retired `${HOME}/MarketData/NarrowGate_BTCUSDC` tree does not exist and no cache symlink was found. "
            "Legacy window-cache paths are resolved by `data_paths.relocate_marketdata_path()` to the internal cache; "
            "other retired data paths resolve to ORICO. Deleting an internal file can therefore break a frozen manifest even when that manifest records the retired path.",
            "",
            "The companion CSV contains exact paths, sizes, mtimes, inode counts and reasons for every audited item.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--cache-root", type=Path, default=LEGACY_CACHE_ROOT)
    parser.add_argument("--orico-root", type=Path, default=ORICO_ROOT)
    parser.add_argument("--legacy-audit", type=Path, required=True)
    parser.add_argument("--v12-audit", type=Path, required=True)
    parser.add_argument("--non-window-audit", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--output-markdown", type=Path, required=True)
    args = parser.parse_args()
    payload = build_report(args)
    for path in (args.output_json, args.output_csv, args.output_markdown):
        path.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_csv(args.output_csv, payload)
    args.output_markdown.write_text(_markdown(payload), encoding="utf-8")
    print(json.dumps(payload["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
