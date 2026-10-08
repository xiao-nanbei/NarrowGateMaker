import copy
import json

import pytest

from narrowgate.studio_research import BINDINGS, SCHEMA, compare, from_summaries, project


def result(complete=False):
    return {"schema_version": SCHEMA, "result_id": "r1", "arm": "equal_weight",
            "currency": "USDC", "economic_complete": complete,
            **dict.fromkeys(BINDINGS, "frozen-v1"),
            "metrics": {"pnl_before_funding": -10, "fee_cost": 2,
                        "funding_cashflow": 1 if complete else None,
                        "all_in_net_pnl": -9 if complete else None,
                        "terminal_inventory": 0.1, "terminal_unrealized_pnl": -3}}


def test_incomplete_view_preserves_known_values_and_unknowns():
    value = project(result())
    assert value["metrics"]["pnl_before_funding"] == -10
    assert value["metrics"]["all_in_net_pnl"] is None
    report = compare(value, value)
    assert report["compatible"] and not report["economic_comparison_complete"]
    assert next(row for row in report["metrics"] if row["metric"] == "all_in_net_pnl")["delta"] is None


@pytest.mark.parametrize("binding", (*BINDINGS, "currency"))
def test_comparison_mismatches_or_missing_identity_never_produce_delta(binding):
    left = project(result(True))
    for replacement in ("different", None):
        right = copy.deepcopy(left)
        right[binding] = replacement
        report = compare(left, right)
        assert binding in report["differences"]
        assert all(row["delta"] is None for row in report["metrics"])


def test_complete_comparison_can_compare_different_models():
    left = project(result(True))
    right = copy.deepcopy(left)
    right.update(arm="time_weighted", model_id="new-model")
    right["metrics"]["all_in_net_pnl"] = -8
    right["metrics"]["pnl_before_funding"] = -9
    report = compare(left, right)
    assert report["economic_comparison_complete"]
    assert next(row for row in report["metrics"] if row["metric"] == "all_in_net_pnl")["delta"] == 1


@pytest.mark.parametrize("value", [True, float("nan"), float("inf"), "0"])
def test_non_numeric_metric_rejected(value):
    raw = result()
    raw["metrics"]["fee_cost"] = value
    with pytest.raises(ValueError):
        project(raw)


def test_net_requires_complete_accounting_and_reconciliation():
    raw = result(True)
    raw["metrics"]["funding_cashflow"] = None
    with pytest.raises(ValueError):
        project(raw)
    raw = result(True)
    raw["metrics"]["all_in_net_pnl"] = 5
    with pytest.raises(ValueError):
        project(raw)
    raw = result(True)
    raw["economic_complete"] = False
    with pytest.raises(ValueError):
        project(raw)


def test_old_summary_remains_viewable_without_fabricated_result():
    assert from_summaries({"legacy.json": {"pnl": 100}}) == (None, [])
    raw = result()
    raw["input_manifest_id"] = "/private/inputs"
    value, issues = from_summaries({"summary.json": raw})
    assert value is None and issues
    assert "/private/inputs" not in str(issues)
    assert from_summaries({"one": result(), "two": result()})[0] is None


def test_registered_report_and_comparison_api(tmp_path, monkeypatch):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from narrowgate import studio
    from narrowgate.studio_execution import report

    files = {
        "execution.json": json.dumps({"plan_id": "p1", "revision": "r1",
                                      "resource_id": "worker", "role": "replay", "required_outputs": []}),
        "environment.json": "{}", "summaries.json": json.dumps({"result.json": result()}),
    }
    assert report(files)["research_result"]["metrics"]["all_in_net_pnl"] is None
    monkeypatch.setattr(studio.Store, "job", lambda self, key: {
        "status": "running" if key == "running" else "completed",
        "plan_id": None if key == "demo" else "p1",
    })
    monkeypatch.setattr(studio.Store, "read_artifacts", lambda self, key: files)
    client = TestClient(studio.create_app(tmp_path / "studio"))
    response = client.get("/api/research-comparison?left=a&right=b")
    assert response.status_code == 200
    assert response.json()["compatible"]
    assert not response.json()["economic_comparison_complete"]
    for left, right in [("a", "a"), ("demo", "a"), ("running", "a")]:
        assert client.get(f"/api/research-comparison?left={left}&right={right}").status_code == 409
    files["summaries.json"] = "{}"
    assert client.get("/api/research-comparison?left=a&right=b").status_code == 409
