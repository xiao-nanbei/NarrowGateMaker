from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

_AVAILABILITY_PATH = (
    Path(__file__).resolve().parent
    / "fixtures/public_clone_historical_test_availability.json"
)
_AVAILABILITY = json.loads(_AVAILABILITY_PATH.read_text(encoding="utf-8"))
_OPT_IN_ENV = str(_AVAILABILITY["opt_in_environment_variable"])
_OPT_IN_VALUE = str(_AVAILABILITY["opt_in_required_value"])


def _unavailable_modules() -> dict[str, str]:
    unavailable: dict[str, str] = {}
    for category, payload in _AVAILABILITY["categories"].items():
        reason = str(payload["reason"])
        for module in payload["modules"]:
            unavailable[str(module)] = f"{category}: {reason}"
    return unavailable


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    if os.environ.get(_OPT_IN_ENV) == _OPT_IN_VALUE:
        return

    unavailable = _unavailable_modules()
    for item in items:
        module = Path(str(item.path)).name
        reason = unavailable.get(module)
        if reason is None:
            continue
        item.add_marker(
            pytest.mark.skip(
                reason=(
                    f"public-clone historical reproduction unavailable: {reason}; "
                    f"restore the exact owner evidence and set {_OPT_IN_ENV}={_OPT_IN_VALUE} "
                    "to run this module"
                )
            )
        )


def _github_escape(value: object) -> str:
    return (
        str(value)
        .replace("%", "%25")
        .replace("\r", "%0D")
        .replace("\n", "%0A")
    )


def _emit_github_failure(
    *,
    title: str,
    message: object,
    path: object = ".github",
    line: int = 1,
    config: pytest.Config | None = None,
) -> None:
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    annotation = (
        "::error "
        f"file={_github_escape(path)},"
        f"line={max(1, line)},"
        f"title={_github_escape(title)}::"
        f"{_github_escape(message)}"
    )
    capture_manager = (
        config.pluginmanager.getplugin("capturemanager")
        if config is not None
        else None
    )
    if capture_manager is None:
        print(annotation, flush=True)
        return
    with capture_manager.global_and_fixture_disabled():
        print(annotation, flush=True)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item,
    call: pytest.CallInfo[Any],
) -> Any:
    outcome = yield
    report = outcome.get_result()
    if not report.failed:
        return
    path, line, _ = report.location
    _emit_github_failure(
        title=f"Pytest {report.when} failure: {report.nodeid}",
        message=report.longrepr,
        path=path,
        line=int(line) + 1,
        config=item.config,
    )


def pytest_collectreport(report: pytest.CollectReport) -> None:
    if report.failed:
        _emit_github_failure(
            title=f"Pytest collection failure: {report.nodeid}",
            message=report.longrepr,
        )
