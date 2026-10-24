from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.freeze_p3_reach_time_source_manifest import (
    _eligible_overlap_days,
    _panel_request,
)


def test_overlap_discovery_uses_atomic_quality_jsons(tmp_path: Path) -> None:
    root = tmp_path / "quality"
    root.mkdir()
    for day, eligible in (
        ("2026-01-01", True),
        ("2026-01-02", False),
        ("2026-01-03", True),
    ):
        (root / f"BTCUSDC-{day}.json").write_text(
            json.dumps(
                {
                    "day": day,
                    "provider_normalized_replay_candidate": eligible,
                }
            ),
            encoding="utf-8",
        )
    assert _eligible_overlap_days(
        root, native_days={"2026-01-01", "2026-01-02"}
    ) == ["2026-01-01"]


def test_panel_request_fails_when_inherited_counts_change(tmp_path: Path) -> None:
    inherited = tmp_path / "manifest.json"
    inherited.write_text(json.dumps({"panels": {}}), encoding="utf-8")
    with pytest.raises(ValueError, match="no longer matches"):
        _panel_request(
            inherited_manifest_path=inherited,
            provider_quality_root=tmp_path,
            native_quality_csv=tmp_path / "missing.csv",
        )
