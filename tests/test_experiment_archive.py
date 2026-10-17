import copy
import json

import pytest

from research.governance.experiment_archive import GATES, ROLES, draft, inventory, main, validate


def test_inventory_is_compact_non_destructive_and_not_a_content_claim(tmp_path):
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / "x").write_bytes(b"abc")
    (tmp_path / "archive-manifest.json").write_text("old")
    inv = inventory(tmp_path)
    assert inv["files"] == 1
    assert inv["logical_bytes"] == 3
    assert inv["scopes"] == [{"path": "results", "files": 1, "bytes": 3}]
    assert (tmp_path / "results" / "x").read_bytes() == b"abc"


def test_links_are_not_followed(tmp_path):
    (tmp_path / "outside").symlink_to("/does/not/exist")
    assert inventory(tmp_path)["non_regular_paths"] == ["outside"]
    with pytest.raises(ValueError):
        inventory(tmp_path / "outside")


def test_draft_does_not_claim_completion(tmp_path):
    d = draft(tmp_path, "study", "F01")
    assert validate(d, tmp_path) == []
    assert d["economic_status"] == "unknown"
    assert d["archive_status"] == "inventoried"
    d["archive_status"] = "verified_minimal"
    assert len(validate(d)) >= 5


def minimal(tmp_path):
    d = draft(tmp_path, "study", "F01")
    d.update(economic_status="closed_partial", archive_status="verified_minimal", exceptions=[])
    d["roles"] = {r: {"status": "verified", "refs": ["original-receipt.json"], "note": "reviewed"} for r in ROLES}
    d["gates"] = {g: {"status": "pass", "refs": ["closure-receipt.json"]} for g in GATES}
    d["cleanup"] = {"status": "complete", "receipt_refs": ["cleanup.json"]}
    return d


def test_partial_closure_is_distinct_from_economic_completion(tmp_path):
    assert validate(minimal(tmp_path)) == []


@pytest.mark.parametrize("gate", GATES)
def test_no_minimal_archive_with_unknown_gate(tmp_path, gate):
    d = minimal(tmp_path)
    d["gates"][gate]["status"] = "unknown"
    assert validate(d)


def test_evidence_and_dependency_gaps_fail_closed(tmp_path):
    d = minimal(tmp_path)
    d["roles"]["provenance"]["refs"] = []
    d["dependencies"] = [{"role": "model", "locator": "private:missing", "identity": "old-model", "reason": "runtime", "status": "unknown"}]
    assert len(validate(d)) == 2


@pytest.mark.parametrize("name", ["../x", "/x", "a/b", "archive-manifest.json"])
def test_scope_escape_or_self_reference_rejected(tmp_path, name):
    d = draft(tmp_path, "study", "F01")
    d["inventory"]["scopes"] = [{"path": name, "files": 0, "bytes": 0}]
    assert validate(d, tmp_path)


def test_missing_scope_and_totals_mismatch_are_reported(tmp_path):
    d = draft(tmp_path, "study", "F01")
    d["inventory"]["scopes"] = [{"path": "gone", "files": 1, "bytes": 3}]
    assert len(validate(d, tmp_path)) == 2


def test_init_never_overwrites(tmp_path, monkeypatch):
    args = ["archive", "init", str(tmp_path), "--experiment-id", "test", "--owner", "F01", "--write"]
    monkeypatch.setattr("sys.argv", args)
    assert main() == 0
    path = tmp_path / "archive-manifest.json"
    before = path.read_bytes()
    with pytest.raises(SystemExit):
        main()
    assert path.read_bytes() == before
    assert json.loads(before)["archive_status"] == "inventoried"


def test_validation_does_not_mutate_declarations(tmp_path):
    d = draft(tmp_path, "test", "F01")
    before = copy.deepcopy(d)
    validate(d, tmp_path)
    assert d == before


@pytest.mark.parametrize("value", [None, [], {"roles": []}, {"inventory": "wrong"}])
def test_malformed_manifest_is_rejected_without_crashing(value):
    assert validate(value)
