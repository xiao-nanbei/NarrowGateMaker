"""Exercise the actual CI summary shell, including legitimate skipped jobs."""

import os
from pathlib import Path
import subprocess

import pytest
import yaml


def test_quality_checks_are_independent_and_share_project_rules():
    root = Path(__file__).resolve().parents[1]
    workflow = yaml.safe_load((root / ".github/workflows/ci.yml").read_text())
    steps = workflow["jobs"]["quality"]["steps"]
    named = {step.get("name"): step for step in steps}
    assert any(step.get("id") == "quality-python" for step in steps)
    assert named["Install quality tools"]["id"] == "quality-tools"
    assert ".[dev]" in named["Install quality tools"]["run"]
    style = named["Public style"]
    assert "--extend-select E,I,UP" in style["run"]
    assert "--select " not in style["run"]
    for name in ("Repository correctness", "Public style", "Audit public documentation once"):
        assert "!cancelled()" in named[name]["if"]
        assert "== 'success'" in named[name]["if"]
        assert not named[name].get("continue-on-error", False)


@pytest.mark.parametrize("overrides,code,annotation", [
    ({}, 0, ""),
    ({"NIGHTLY": "skipped", "FRONTEND": "skipped"}, 0, ""),
    ({"PYTHON": "failure"}, 1, "PYTHON: failure"),
    ({"BASE": "cancelled"}, 1, "BASE: cancelled"),
    ({"CHANGES": "skipped"}, 1, "CHANGES: skipped"),
])
def test_admission_reports_upstream_result(tmp_path, overrides, code, annotation):
    workflow = Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
    script = yaml.safe_load(workflow.read_text())["jobs"]["admission"]["steps"][0]["run"]
    statuses = dict.fromkeys(["CHANGES", "QUALITY", "FRONTEND", "BASE", "PYTHON", "NIGHTLY"], "success")
    statuses.update(overrides)
    summary = tmp_path / "summary.md"
    result = subprocess.run(
        ["bash", "-e", "-c", script], capture_output=True, text=True,
        env={**os.environ, **{f"{key}_RESULT": value for key, value in statuses.items()},
             "GITHUB_STEP_SUMMARY": str(summary)},
    )
    assert result.returncode == code
    assert annotation in result.stdout
    for key, value in statuses.items():
        assert f"{key}: {value}" in summary.read_text()
    if not code:
        assert "::error" not in result.stdout
