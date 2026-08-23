from __future__ import annotations

import importlib
import io
import json
import shlex
import tomllib
import unittest
from contextlib import redirect_stdout
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any

import yaml

from narrowgate.cli import main as narrowgate_main

ROOT = Path(__file__).resolve().parents[1]


def _project_config() -> dict[str, Any]:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        return tomllib.load(handle)


def _docker_copy_sources() -> set[str]:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    logical_lines = dockerfile.replace("\\\n", " ").splitlines()
    sources: set[str] = set()
    for line in logical_lines:
        if not line.startswith("COPY "):
            continue
        tokens = [token for token in shlex.split(line)[1:] if not token.startswith("--")]
        sources.update(token.removeprefix("./").rstrip("/") for token in tokens[:-1])
    return sources


def _dockerignore_rules() -> list[str]:
    return [
        line.strip()
        for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def _docker_context_includes(relative_path: str) -> bool:
    path = relative_path.strip("/")
    ignored = False
    for rule in _dockerignore_rules():
        negated = rule.startswith("!")
        pattern = rule[1:] if negated else rule
        directory_only = pattern.endswith("/")
        pattern = pattern.lstrip("/").rstrip("/")
        if directory_only:
            matched = path == pattern
        elif "/" in pattern:
            matched = fnmatchcase(path, pattern) or fnmatchcase(path, f"{pattern}/**")
        else:
            matched = any(fnmatchcase(part, pattern) for part in path.split("/"))
        if matched:
            ignored = not negated
    return not ignored


class PublicOnboardingSmokeTest(unittest.TestCase):
    def test_no_data_quote_demo(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output):
            return_code = narrowgate_main(["quote-demo"])

        payload = json.loads(output.getvalue())
        self.assertEqual(return_code, 0)
        self.assertLess(payload["bid_price"], payload["ask_price"])

    def test_base_install_imports_execution_and_research_packages(self) -> None:
        importlib.import_module("execution")
        importlib.import_module("research")
        importlib.import_module("research.families.f03_causal_13_head.audit")

    def test_setuptools_declares_every_public_package(self) -> None:
        config = _project_config()
        declared = set(config["tool"]["setuptools"]["packages"])
        roots = {name.split(".", 1)[0] for name in declared}
        discovered: set[str] = set()

        for root_name in roots:
            for init_file in (ROOT / root_name).rglob("__init__.py"):
                relative = init_file.parent.relative_to(ROOT)
                if "private" in relative.parts:
                    continue
                discovered.add(".".join(relative.parts))

        self.assertEqual(discovered - declared, set())
        self.assertEqual(declared - discovered, set())

    def test_dockerfile_copies_every_declared_package_root_and_module(self) -> None:
        config = _project_config()
        setuptools = config["tool"]["setuptools"]
        package_roots = {name.split(".", 1)[0] for name in setuptools["packages"]}
        module_sources = {f"{name}.py" for name in setuptools["py-modules"]}
        copy_sources = _docker_copy_sources()

        self.assertEqual(package_roots - copy_sources, set())
        self.assertEqual(module_sources - copy_sources, set())
        self.assertIn("execution", copy_sources)
        self.assertIn("research", copy_sources)
        self.assertIn("scripts", copy_sources)

    def test_docker_context_keeps_code_and_synthetic_examples_only(self) -> None:
        setuptools = _project_config()["tool"]["setuptools"]
        required = {
            "LICENSE",
            "README.md",
            "data/README.md",
            "live/formal_dry_run_public.yaml",
            "pyproject.toml",
            "scripts/narrowgate_replay_demo.py",
        }
        required.update(f"{name}.py" for name in setuptools["py-modules"])
        for package in setuptools["packages"]:
            package_dir = ROOT / package.replace(".", "/")
            required.update(
                str(path.relative_to(ROOT)) for path in package_dir.glob("*.py")
            )
        for example in (
            ROOT / "examples" / "live_dry_run_config.yaml",
            ROOT / "examples" / "order_level_score_demo.py",
        ):
            required.add(str(example.relative_to(ROOT)))
        for fixture_dir in (
            ROOT / "examples" / "public_dry_run_model_bundle",
            ROOT / "examples" / "replay_demo",
        ):
            required.update(
                str(path.relative_to(ROOT))
                for path in fixture_dir.rglob("*")
                if path.is_file()
            )
        excluded = (
            ".venv/bin/python",
            ".env",
            "private/root-secret.py",
            "data/raw_trades/BTCUSDC/day.csv",
            "data/quality/private_day.parquet",
            "data/private/secret.py",
            "data/quality/private/secret.py",
            "data/quality/nested/private/secret.py",
            "docs/private/live_config.current.local.yaml",
            "execution/private/runtime.json",
            "examples/private/local-fixture.json",
            "narrowgate/private/local-module.py",
            "research/families/example/private/result.json",
            "models/saved_private/model.txt",
            "logs/maker.log",
            "results/replay/output.json",
        )

        self.assertTrue(all((ROOT / path).is_file() for path in required))
        self.assertTrue(all(_docker_context_includes(path) for path in required))
        self.assertTrue(all(not _docker_context_includes(path) for path in excluded))
        self.assertEqual(
            _dockerignore_rules()[-4:],
            ["private", "private/**", "**/private", "**/private/**"],
        )

    def test_all_extra_is_the_union_of_supported_workflows(self) -> None:
        optional = _project_config()["project"]["optional-dependencies"]
        expected = (
            set(optional["dev"])
            | set(optional["data"])
            | set(optional["research"])
            | set(optional["live"])
        )
        self.assertEqual(set(optional["all"]), expected)
        self.assertEqual(optional["provider-cryptohft"], ["cryptohftdata>=0.2.1"])
        self.assertNotIn("cryptohftdata>=0.2.1", optional["all"])

        requirements = {
            line.strip()
            for line in (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        compatibility_superset = (
            set(_project_config()["project"]["dependencies"])
            | set(optional["data"])
            | set(optional["research"])
            | set(optional["live"])
            | set(optional["provider-cryptohft"])
        )
        self.assertEqual(requirements, compatibility_superset)

    def test_ci_has_base_only_smoke_and_stable_required_check_names(self) -> None:
        workflow = yaml.safe_load(
            (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        )
        jobs = workflow["jobs"]

        base = jobs["base-install-smoke"]
        self.assertEqual(base["name"], "Base install smoke")
        base_install = next(
            step for step in base["steps"] if step.get("name") == "Install base package only"
        )["run"]
        self.assertIn("python -m pip install -e .", base_install)
        self.assertNotIn(".[", base_install)
        base_smoke = next(
            step for step in base["steps"] if step.get("name") == "Base-only public workflow smoke"
        )["run"]
        self.assertIn("narrowgate replay-demo", base_smoke)
        self.assertIn("test_public_onboarding.py", base_smoke)

        self.assertEqual(
            jobs["python"]["name"],
            "Python tests and lint (${{ matrix.python-version }})",
        )
        self.assertEqual(jobs["cpp-build-smoke"]["name"], "C++ extension build smoke")
        pytest_step = next(
            step for step in jobs["python"]["steps"] if step.get("id") == "pytest"
        )
        self.assertIsNot(pytest_step.get("continue-on-error"), True)

        branch_protection = (ROOT / "docs" / "dev" / "branch_protection.md").read_text(
            encoding="utf-8"
        )
        for required_name in (
            "Base install smoke",
            "Python tests and lint (3.11)",
            "Python tests and lint (3.12)",
            "C++ extension build smoke",
        ):
            self.assertIn(f"`{required_name}`", branch_protection)

    def test_devcontainer_uses_all_and_external_data_volumes(self) -> None:
        config = json.loads(
            (ROOT / ".devcontainer" / "devcontainer.json").read_text(encoding="utf-8")
        )

        self.assertEqual(config["build"]["dockerfile"], "../Dockerfile")
        self.assertEqual(config["build"]["context"], "..")
        self.assertEqual(config["build"]["args"]["NARROWGATE_INSTALL_TARGET"], ".[all]")
        self.assertIn("--no-deps", config["postCreateCommand"])
        self.assertIn("--no-build-isolation", config["postCreateCommand"])
        self.assertEqual(
            config["remoteEnv"]["NARROWGATE_DATA_ROOT"],
            "/narrowgate/marketdata/NarrowGate_BTCUSDC",
        )
        self.assertNotEqual(config["remoteEnv"]["NARROWGATE_DATA_ROOT"], "/workspace/data")
        self.assertTrue(any("target=/narrowgate/marketdata" in mount for mount in config["mounts"]))

    def test_readme_quickstart_runs_this_base_only_smoke(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        quickstart = readme.split("## 5-Minute Quickstart", 1)[1].split(
            "## What This Repo Is For", 1
        )[0]

        self.assertIn("python -m pip install -e .", quickstart)
        self.assertIn("test_public_onboarding.py", quickstart)
        self.assertNotIn("test_parameter_selection.py", quickstart)

    def test_readme_has_one_canonical_dry_run_and_replay_demo_route(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        section = readme.split("## Official No-Data Validation", 1)[1].split(
            "## What This Repo Is For", 1
        )[0]

        self.assertEqual(readme.count("bash live/run.sh dry-run"), 1)
        self.assertEqual(readme.count("narrowgate replay-demo"), 1)
        self.assertEqual(readme.count("(docs/ops/live_dry_run.md)"), 1)
        self.assertEqual(readme.count("(examples/replay_demo/README.md)"), 1)
        self.assertIn("docs/ops/live_dry_run.md", section)
        self.assertIn("examples/replay_demo/README.md", section)

    def test_chinese_readme_matches_quickstart_and_canonical_routes(self) -> None:
        readme = (ROOT / "README.zh-CN.md").read_text(encoding="utf-8")
        quickstart = readme.split("## 5 分钟快速开始", 1)[1].split(
            "## 正式无数据验证", 1
        )[0]

        self.assertIn("python -m pip install -e .", quickstart)
        self.assertIn("test_public_onboarding.py", quickstart)
        self.assertNotIn("test_parameter_selection.py", quickstart)
        self.assertEqual(readme.count("bash live/run.sh dry-run"), 1)
        self.assertEqual(readme.count("narrowgate replay-demo"), 1)
        self.assertEqual(readme.count("(docs/ops/live_dry_run.md)"), 1)

    def test_readme_links_public_participation_and_data_tutorial(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("[Contributing](CONTRIBUTING.md)", readme)
        self.assertIn("[Security policy](SECURITY.md)", readme)
        self.assertIn("[Open-source navigation](docs/opensource/README.md)", readme)
        self.assertIn("[one-day data pipeline](docs/opensource/one_day_data_pipeline.md)", readme)
        self.assertIn("[Branch protection](docs/dev/branch_protection.md)", readme)
        self.assertEqual(readme.count("(examples/replay_demo/README.md)"), 1)

    def test_data_dependency_contract_is_documented(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        tutorial = (ROOT / "docs" / "opensource" / "one_day_data_pipeline.md").read_text(
            encoding="utf-8"
        )

        self.assertIn('python -m pip install -e ".[data]"', readme)
        self.assertIn('python -m pip install -e ".[provider-cryptohft]"', readme)
        self.assertIn("requirements.txt", readme)
        self.assertIn("download-agg-trades", tutorial)
        self.assertIn("audit-raw", tutorial)
        self.assertIn("models.backtest_tick", tutorial)


if __name__ == "__main__":
    unittest.main()
