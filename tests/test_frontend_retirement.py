"""Public CLI and distributions do not expose a retired web service."""

import tomllib
from pathlib import Path

import pytest
import yaml

from narrowgate.cli import build_parser

ROOT = Path(__file__).resolve().parents[1]


def test_cli_rejects_retired_studio_command():
    parser = build_parser()
    assert "studio" not in parser.format_help()
    with pytest.raises(SystemExit) as error:
        parser.parse_args(["studio", "serve"])
    assert error.value.code == 2


def test_no_frontend_distribution_or_build_job():
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())
    extras = config["project"]["optional-dependencies"]
    assert "studio" not in extras
    assert not any(
        dep.startswith(("fastapi", "uvicorn", "httpx")) for deps in extras.values() for dep in deps
    )
    assert not any(
        "studio_static" in item
        for item in config["tool"]["setuptools"]["package-data"]["narrowgate"]
    )
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    assert "frontend" not in workflow["jobs"]
    assert "frontend" not in workflow["jobs"]["admission"]["needs"]
    assert not (ROOT / "frontend").exists()
    assert not (ROOT / "narrowgate/studio_static").exists()
    assert not list((ROOT / "narrowgate").glob("studio*.py"))
