from __future__ import annotations

import json
from pathlib import Path

from models.replay_cache_v12_semantic_audit import audit_v12_semantics, render_markdown


def _cache(root: Path, day: str, prefix: str, payload: bytes) -> Path:
    path = root / f"btcusdc_{day}_tick_window_v12_{prefix}.pkl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def test_semantic_version_day_reference_preserves_unresolved_variants(tmp_path: Path) -> None:
    cache_root = tmp_path / "cache"
    first = _cache(cache_root, "2026-01-01", "a" * 16, b"same")
    second = _cache(cache_root, "2026-01-01", "b" * 16, b"same")
    spec = tmp_path / "research" / "spec.json"
    spec.parent.mkdir()
    spec.write_text(
        json.dumps(
            {
                "family_id": "f06_test",
                "created_at": "2026-08-04",
                "days": ["2026-01-01"],
                "calibration_days": ["2025-12-31"],
                "cache_contract": {"baseline_window_cache_version": 12},
                "artifact_path": "/reports/generated_2026-07-31.json",
                "feature_context_manifest_sha256": "c" * 64,
            }
        )
    )
    before = {path: path.stat().st_mtime_ns for path in (first, second)}

    audit = audit_v12_semantics(
        cache_root,
        reference_roots=(tmp_path / "research",),
        hash_same_day_size_groups=True,
        expected_count=2,
    )

    assert before == {path: path.stat().st_mtime_ns for path in (first, second)}
    assert audit["safety"]["pickle_payloads_unpickled"] == 0
    assert audit["summary"]["byte_identical_groups"] == 1
    assert audit["summary"]["classification_counts"] == {
        "duplicate_but_variant_semantics_unresolved": 2
    }
    assert audit["semantic_identities"][0]["days"] == ["2026-01-01"]
    assert len(audit["semantic_identities"][0]["matched_v12_variants"]) == 2


def test_exact_prefix_reference_is_must_retain(tmp_path: Path) -> None:
    cache_root = tmp_path / "cache"
    retained = _cache(cache_root, "2026-01-01", "a" * 16, b"retained")
    candidate = _cache(cache_root, "2026-01-02", "b" * 16, b"candidate")
    evidence = tmp_path / "reports" / "manifest.json"
    evidence.parent.mkdir()
    evidence.write_text(json.dumps({"cache_key_prefix": "a" * 16}))

    audit = audit_v12_semantics(cache_root, reference_roots=(evidence.parent,))
    by_name = {record["basename"]: record for record in audit["files"]}

    assert by_name[retained.name]["classification"] == "must_retain_exact_identity"
    assert by_name[candidate.name]["classification"] == "rebuildable_delete_candidate_unreferenced"
    assert all(record["deletion_authorized"] is False for record in audit["files"])


def test_prior_inventory_audit_cannot_self_promote_cache_reference(tmp_path: Path) -> None:
    cache_root = tmp_path / "cache"
    cache = _cache(cache_root, "2026-01-01", "a" * 16, b"payload")
    stale_audit = tmp_path / "docs" / "replay_cache_legacy_reference_audit_20260804.json"
    stale_audit.parent.mkdir()
    stale_audit.write_text(json.dumps({"basename": cache.name}))

    audit = audit_v12_semantics(cache_root, reference_roots=(stale_audit.parent,))

    assert audit["files"][0]["exact_references"] == []
    assert audit["files"][0]["classification"] == "rebuildable_delete_candidate_unreferenced"


def test_markdown_reports_zero_delete_boundary(tmp_path: Path) -> None:
    cache_root = tmp_path / "cache"
    _cache(cache_root, "2026-01-01", "a" * 16, b"payload")
    audit = audit_v12_semantics(cache_root, reference_roots=(tmp_path,))

    markdown = render_markdown(audit)

    assert "no cache file was deleted" in markdown
    assert "Canonical audit SHA256" in markdown
