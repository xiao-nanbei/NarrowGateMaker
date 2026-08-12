from __future__ import annotations

import json
import os
from pathlib import Path

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
