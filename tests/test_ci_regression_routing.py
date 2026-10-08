"""Execute the workflow's mirror identity decision with controlled API replies."""

import io
import json
import os
from pathlib import Path
import subprocess
import urllib.request

import pytest
import yaml


@pytest.mark.parametrize(
    "repository,event,primary_tree,expected,api_calls",
    [
        ("NarrowGateMaker", "push", "same", "true", 0),
        ("NarrowGateMaker-private", "push", "same", "false", 1),
        ("NarrowGateMaker-private", "push", "different", "true", 1),
        ("NarrowGateMaker-private", "push", None, "true", 1),
        ("NarrowGateMaker-private", "pull_request", "same", "true", 0),
        ("NarrowGateMaker-private", "workflow_dispatch", "same", "true", 0),
    ],
)
def test_mirror_only_delegates_identical_push(
    tmp_path, monkeypatch, repository, event, primary_tree, expected, api_calls
):
    workflow = yaml.safe_load(
        (Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml").read_text()
    )
    step = next(s for s in workflow["jobs"]["changes"]["steps"] if s.get("id") == "regression")
    script = step["run"].split("python - <<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
    output = tmp_path / "output"
    for key, value in {
        "GITHUB_REPOSITORY": f"xiao-nanbei/{repository}",
        "GITHUB_EVENT_NAME": event,
        "GH_TOKEN": "synthetic-test-token",
        "GITHUB_OUTPUT": str(output),
        "GITHUB_STEP_SUMMARY": str(tmp_path / "summary"),
    }.items():
        monkeypatch.setenv(key, value)
    calls = []

    def fetch(request, timeout):
        calls.append(request.full_url)
        assert timeout == 20
        if primary_tree is None:
            raise TimeoutError("controlled unavailable primary")
        return io.BytesIO(json.dumps({"commit": {"tree": {"sha": primary_tree}}}).encode())

    monkeypatch.setattr(urllib.request, "urlopen", fetch)
    monkeypatch.setattr(subprocess, "check_output", lambda *args, **kwargs: "same\n")
    exec(compile(script, "workflow-regression-routing", "exec"), {})
    assert output.read_text() == f"required={expected}\n"
    assert len(calls) == api_calls


@pytest.mark.parametrize(
    "event,compatibility,full",
    [
        ("workflow_dispatch", "false", "false"),
        ("workflow_dispatch", "true", "true"),
        ("schedule", "false", "true"),
    ],
)
def test_full_compatibility_requires_nightly_or_explicit_selection(
    tmp_path, event, compatibility, full
):
    workflow = yaml.safe_load(
        (Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml").read_text()
    )
    step = next(s for s in workflow["jobs"]["changes"]["steps"] if s.get("id") == "filter")
    output = tmp_path / "classification"
    subprocess.run(
        ["bash", "-c", step["run"]],
        env={
            **os.environ,
            "EVENT_NAME": event,
            "COMPATIBILITY": compatibility,
            "GITHUB_OUTPUT": str(output),
        },
        check=True,
        timeout=10,
    )
    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert values == {
        "docs": "true",
        "python": "true",
        "native": "true",
        "frontend": "true",
        "full": full,
    }
