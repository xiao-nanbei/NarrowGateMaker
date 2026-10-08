from __future__ import annotations

from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_native_observation_cache_batch as batch,
)


def test_staging_telemetry_reports_bytes_without_admitting(tmp_path: Path) -> None:
    staging = tmp_path / ".2026-04-17.staging-1-test"
    staging.mkdir()
    (staging / "part.bin").write_bytes(b"abc")

    result = batch._staging_telemetry(
        tmp_path, ("2026-04-17", "2026-04-18")
    )

    assert result["2026-04-17"] == {
        "staging_present": True,
        "staging_bytes": 3,
        "staging_name": staging.name,
    }
    assert result["2026-04-18"] == {
        "staging_present": False,
        "staging_bytes": 0,
    }


def test_staging_telemetry_rejects_duplicate_writers(tmp_path: Path) -> None:
    (tmp_path / ".2026-04-17.staging-1-a").mkdir()
    (tmp_path / ".2026-04-17.staging-2-b").mkdir()

    with pytest.raises(batch.NativeObservationBatchError, match="multiple staging"):
        batch._staging_telemetry(tmp_path, ("2026-04-17",))
