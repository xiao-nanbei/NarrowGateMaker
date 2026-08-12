#!/usr/bin/env python3
"""Publish concise GitHub annotations for pytest's cached failing node IDs."""

from __future__ import annotations

import json
import os
from pathlib import Path


def _escape(value: object) -> str:
    return (
        str(value)
        .replace("%", "%25")
        .replace("\r", "%0D")
        .replace("\n", "%0A")
    )


def main() -> int:
    cache_path = Path(".pytest_cache/v/cache/lastfailed")
    if not cache_path.is_file():
        print("::error file=.github/workflows/ci.yml,line=1,title=Pytest failed::"
              "pytest did not publish a lastfailed cache")
        return 1

    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    failing = sorted(str(nodeid) for nodeid, failed in payload.items() if failed)
    if not failing:
        print("::error file=.github/workflows/ci.yml,line=1,title=Pytest failed::"
              "pytest reported failure without cached node IDs")
        return 1

    for nodeid in failing:
        path = nodeid.split("::", 1)[0]
        print(
            f"::error file={_escape(path)},line=1,title=Pytest failure::"
            f"{_escape(nodeid)}",
            flush=True,
        )
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as summary:
            summary.write("## Pytest failures\n\n")
            summary.writelines(f"- `{nodeid}`\n" for nodeid in failing)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
