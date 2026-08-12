from __future__ import annotations

from pathlib import Path

from models.replay_cache_audit import audit_legacy_window_caches


def _legacy(cache_root: Path, day: str, version: int, digest: str, size: int) -> Path:
    path = cache_root / f"btcusdc_{day}_tick_window_v{version}_{digest}.pkl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    return path


def test_zero_write_audit_counts_variants_and_frozen_references(
    tmp_path: Path,
) -> None:
    cache_root = tmp_path / "window_cache"
    first = _legacy(cache_root, "2026-01-01", 12, "a" * 16, 3)
    second = _legacy(cache_root, "2026-01-01", 13, "b" * 16, 5)
    third = _legacy(cache_root, "2026-01-02", 13, "c" * 16, 7)
    evidence = tmp_path / "reports" / "frozen_input_cache_manifest.json"
    evidence.parent.mkdir(parents=True)
    evidence.write_text(f'{{"cache_path": "{second}"}}\n')
    self_audit = tmp_path / "reports" / "replay_cache_legacy_reference_audit_20260803.json"
    self_audit.write_text(f'{{"cache_path": "{first}"}}\n')

    before = {path: path.stat().st_mtime_ns for path in (first, second, third)}
    audit = audit_legacy_window_caches(
        cache_root,
        reference_roots=(tmp_path / "reports",),
    )
    after = {path: path.stat().st_mtime_ns for path in (first, second, third)}

    assert before == after
    assert audit["mode"] == "read_only_zero_write"
    assert audit["pickle_payloads_opened"] == 0
    assert audit["cache_files_modified"] == 0
    assert audit["cache_files_deleted"] == 0
    assert audit["summary"] == {
        "file_count": 3,
        "size_bytes": 15,
        "distinct_days": 2,
        "days_with_variants": 1,
        "extra_same_day_variants": 1,
        "max_variants_per_day": 2,
        "text_referenced_files": 1,
        "frozen_or_evidence_referenced_files": 1,
    }
    by_name = {record["basename"]: record for record in audit["files"]}
    assert by_name[second.name]["frozen_reference"] is True
    assert by_name[second.name]["governance_status"] == "preserve_frozen_reference"
    assert by_name[first.name]["frozen_reference"] is False
    assert audit["versions"]["v13"]["distinct_days"] == 2
