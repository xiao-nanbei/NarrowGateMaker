from __future__ import annotations

from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_feature_panel_batch as batch,
)


def test_staging_telemetry_reports_bytes(tmp_path: Path) -> None:
    staging = tmp_path / ".2026-04-17.staging-7-test"
    staging.mkdir()
    (staging / "features.parquet").write_bytes(b"12345")

    result = batch._staging_telemetry(
        tmp_path, ("2026-04-17", "2026-04-18")
    )

    assert result["2026-04-17"]["staging_bytes"] == 5
    assert result["2026-04-17"]["staging_present"] is True
    assert result["2026-04-18"] == {
        "staging_present": False,
        "staging_bytes": 0,
    }


def test_staging_telemetry_rejects_duplicate_writers(tmp_path: Path) -> None:
    (tmp_path / ".2026-04-17.staging-1-a").mkdir()
    (tmp_path / ".2026-04-17.staging-2-b").mkdir()

    with pytest.raises(batch.ModeledFeaturePanelBatchError, match="multiple staging"):
        batch._staging_telemetry(tmp_path, ("2026-04-17",))
