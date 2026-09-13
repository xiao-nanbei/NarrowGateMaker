import json

import pytest

from data.daily_schema_cutover import (identity, prepare_transaction, publish_transaction,
                                      rebind_current, rebind_csv, rebind_json)


def test_cutover_resume_and_never_overwrite_unexpected_target(tmp_path, monkeypatch):
    pairs = []
    for i in range(2):
        target, stage = tmp_path / f"target{i}.txt", tmp_path / f"stage{i}.txt"
        target.write_text(f"old{i}")
        stage.write_text(f"new{i}")
        pairs.append((stage, target))
    journal = tmp_path / "publication.json"
    state = prepare_transaction(journal, pairs, day="2025-08-01")
    with pytest.raises(FileExistsError):
        prepare_transaction(journal, pairs, day="2025-08-01")
    import data.daily_schema_cutover as module
    replace = module.os.replace
    def interrupt(source, target):
        if target == pairs[1][1]:
            raise OSError("interrupted")
        return replace(source, target)
    with monkeypatch.context() as patch:
        patch.setattr(module.os, "replace", interrupt)
        with pytest.raises(OSError, match="interrupted"):
            publish_transaction(journal)
    assert pairs[0][1].read_text() == "new0"
    pairs[1][1].write_text("user change")
    with pytest.raises(ValueError, match="neither"):
        publish_transaction(journal)
    assert pairs[1][1].read_text() == "user change"
    pairs[1][1].write_text("old1")
    result = publish_transaction(journal)
    assert result["status"] == "FILES_PUBLISHED"
    assert [identity(p)["sha256"] for _, p in pairs] == [r["after"]["sha256"] for r in state["files"]]
    assert publish_transaction(journal)["status"] == "FILES_PUBLISHED"


def test_current_rebind_preserves_usage_history_and_eligibility(tmp_path):
    target = tmp_path / "data"
    target.write_text("new")
    records = [{"before": {"sha256": "old"}, "after": {
        "path": str(target), "sha256": "new", "size_bytes": 3, "rows": 5, "columns": ["timestamp"]}}]
    before = {"path": str(target), "sha256": "old", "size_bytes": 8, "rows": 9,
              "mtime_ns": 0, "eligible": False, "nested": {"raw_sha256": "old"},
              "research_use": {"validation": "locked", "evidence": "old"},
              "model_input_verification": {"reference_bars": {"sha256": "old"}},
              "prior_sha256": "old", "historical_audit": {"sha256": "old"}}
    after = rebind_current(before, records)
    assert after["sha256"] == after["nested"]["raw_sha256"] == "new"
    assert (after["size_bytes"], after["rows"]) == (3, 5)
    assert after["mtime_ns"] == target.stat().st_mtime_ns
    for key in ("research_use", "prior_sha256", "historical_audit", "eligible", "model_input_verification"):
        assert after[key] == before[key]
    manifest = tmp_path / "current.json"
    manifest.write_text(json.dumps(before))
    assert rebind_json(manifest, records)["before"]["sha256"] != identity(manifest)["sha256"]
    csv_file = tmp_path / "current.csv"
    csv_file.write_text("day,sha256,eligible\n2025-08-01,old,false\n2025-08-02,old,false\n")
    rebind_csv(csv_file, records, day="2025-08-01", updates={"format_checked": "true"})
    assert "2025-08-01,new,false,true" in csv_file.read_text()
    assert "2025-08-02,old,false," in csv_file.read_text()
