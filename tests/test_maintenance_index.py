import json

import pytest

from narrowgate import cli
from narrowgate.maintenance import __main__ as maintenance
from narrowgate.maintenance import file_index


def setup_config(tmp_path):
    root = tmp_path.resolve()
    roots = [root / name for name in ("project", "archive-a", "archive-b")]
    for path in roots:
        path.mkdir()
        (path / "sample.txt").write_text("retained")
    pool = roots[0] / "pool.json"
    pool.write_text(json.dumps({"revision": 1, "tasks": {}, "attempts": {}}))
    base = roots[0] / "private-index"
    config = root / "config.json"
    config.write_text(
        json.dumps(
            {
                "roots": [{"path": str(r), "label": r.name} for r in roots],
                "pool_state": str(pool),
                "index_directory": str(base),
            }
        )
    )
    return config, roots, base


def test_index_excludes_git_itself_and_links_and_labels_all_volumes(tmp_path):
    config, roots, base = setup_config(tmp_path)
    (roots[0] / ".git").mkdir()
    (roots[0] / ".git" / "secret").write_text("not indexed")
    (roots[1] / "alias").symlink_to(roots[0], target_is_directory=True)
    report = maintenance.run(config, index_only=True)
    assert report["index_refreshed"] and report["removed_files"] == 0
    summary = json.loads((base / "汇总.json").read_text())
    assert summary["files"] == 4
    assert summary["full_refresh"]["roots"] == list(map(str, roots))
    lines = (base / "全部文件_修改时间升序.jsonl").read_text().splitlines()
    assert len(lines) == 4 and not any("/.git/" in row or "private-index" in row for row in lines)
    js = (base / "轻量分页/search/manifest.js").read_text()
    manifest = json.loads(js.removeprefix("window.NG_FILE_SEARCH=").removesuffix(";\n"))
    assert set(manifest["locations"]) == {"project", "archive-a", "archive-b"}
    assert manifest["count"] == 4
    assert "GLOBAL FILE SEARCH START" in (base / "轻量分页/index.html").read_text()
    assert not list(base.glob(".full-refresh-*"))


def test_no_deletion_means_no_full_refresh(tmp_path, monkeypatch):
    config, _, base = setup_config(tmp_path)

    def unexpected(*a, **kw):
        raise AssertionError("unnecessary full refresh")

    monkeypatch.setattr(file_index, "refresh", unexpected)
    result = maintenance.run(config, apply=True)
    assert result["status"] == "OK" and not result["index_refreshed"]
    assert not (base / "汇总.json").exists()


def test_index_failure_retries_only_index_after_actual_cleanup(tmp_path, monkeypatch):
    config, _, base = setup_config(tmp_path)
    calls = []

    def cleaned(*a, **kw):
        calls.append("clean")
        return {
            "mode": "apply",
            "removed_files": 1 if len(calls) == 1 else 0,
            "records": [],
            "errors": [],
        }

    def failed(*a, **kw):
        raise OSError("volume disconnected")

    monkeypatch.setattr(maintenance.locks, "clean", cleaned)
    monkeypatch.setattr(file_index, "refresh", failed)
    result = maintenance.run(config, apply=True)
    assert result["status"] == "ERROR" and result["index_refresh_pending"]
    monkeypatch.setattr(file_index, "refresh", lambda *a, **kw: {"files": 4, "errors": []})
    result = maintenance.run(config, apply=True)
    assert result["removed_files"] == 0 and result["index_refreshed"]
    assert not result["index_refresh_pending"]
    assert json.loads((base / "maintenance-last-change.json").read_text())["removed_files"] == 1


def test_missing_volume_retains_previous_index_and_cli_scope(tmp_path):
    config, roots, base = setup_config(tmp_path)
    maintenance.run(config, index_only=True)
    previous = (base / "汇总.json").read_bytes()
    (roots[2] / "sample.txt").unlink()
    roots[2].rmdir()
    assert cli.main(["maintenance", "run", "--config", str(config), "--apply"]) == 1
    assert (base / "汇总.json").read_bytes() == previous
    with pytest.raises(SystemExit):
        maintenance.main(["scan", "--config", str(config), "--apply"])


def test_failed_staging_is_disposable_and_does_not_replace_old_index(tmp_path, monkeypatch):
    config, roots, base = setup_config(tmp_path)
    maintenance.run(config, index_only=True)
    previous = (base / "汇总.json").read_bytes()

    def bad(*a):
        raise ValueError("broken category")

    monkeypatch.setattr(file_index, "category", bad)
    result = maintenance.run(config, index_only=True)
    assert result["status"] == "ERROR"
    assert (base / "汇总.json").read_bytes() == previous
    assert not list(base.glob(".full-refresh-*"))


def test_no_overlapping_or_parent_output_scope(tmp_path):
    config, roots, _ = setup_config(tmp_path)
    obj = json.loads(config.read_text())
    obj["index_directory"] = str(tmp_path.resolve())
    config.write_text(json.dumps(obj))
    with pytest.raises(ValueError, match="contain"):
        maintenance.configuration(config)


def test_maintenance_single_writer_and_unknown_search_asset_preserved(tmp_path):
    config, _, base = setup_config(tmp_path)
    with maintenance.maintenance_lock(base):
        with pytest.raises(BlockingIOError):
            maintenance.run(config, apply=True)
    maintenance.run(config, index_only=True)
    unknown = base / "轻量分页/search/owner-notes.txt"
    unknown.write_text("not a generated asset")
    result = maintenance.run(config, index_only=True)
    assert result["status"] == "ERROR"
    assert unknown.read_text() == "not a generated asset"
