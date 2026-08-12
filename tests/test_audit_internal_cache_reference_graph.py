from pathlib import Path

from scripts.audit_internal_cache_reference_graph import _record, _tree_stats


def test_tree_stats_reports_size_and_inode_count(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    root.mkdir()
    (root / "a.bin").write_bytes(b"abc")
    (root / "b.bin").write_bytes(b"defg")

    result = _tree_stats(root)

    assert result["size_bytes"] == 7
    assert result["inode_count"] == 2
    assert result["file_count"] == 2
    assert result["realpath"] == str(root.resolve())


def test_record_rejects_unknown_classification(tmp_path: Path) -> None:
    path = tmp_path / "cache.bin"
    path.write_bytes(b"x")

    try:
        _record(path, "not-a-class", "bad", item_type="fixture")
    except ValueError as exc:
        assert "unknown classification" in str(exc)
    else:
        raise AssertionError("unknown classification was accepted")
