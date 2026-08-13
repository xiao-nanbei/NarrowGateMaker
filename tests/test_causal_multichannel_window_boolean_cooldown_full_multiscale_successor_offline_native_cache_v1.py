from __future__ import annotations

import json
from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_native_cache_v1 as native,
)


def _layout(tmp_path: Path) -> native.offline.OfflineSourceLayout:
    project = tmp_path / "project-data"
    market = tmp_path / "market-data"
    return native.offline.OfflineSourceLayout(
        project_data_root=project,
        marketdata_root=market,
        raw_orderbook_root=market / "cryptohftdata/binance_futures",
        normalized_roots=(project / "normalized",),
        aggtrades_root=project / "raw",
        individual_trades_root=project / "raw-trades",
        sequence_audit_paths=(),
    )


def _source() -> dict[str, object]:
    return {
        "canonical_manifest_sha256": "a" * 64,
        "selection_sha256": "b" * 64,
        "selected_days": ["2026-01-02"],
        "target_day_receipts": [
            {
                "utc_day": "2026-01-02",
                "source_gate_eligible": True,
                "context_days": {
                    "D_minus_1": "2026-01-01",
                    "D": "2026-01-02",
                    "D_plus_1": "2026-01-03",
                },
            }
        ],
        "permissions": {"economic_outcomes_read": False},
    }


def test_context_days_are_unique_and_chronological(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(native.offline, "REQUIRED_DAYS", 1)
    assert native._context_days(_source()) == (
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
    )


def test_build_manifest_is_outcome_blind_and_binds_hours(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = _layout(tmp_path)
    source_path = layout.project_data_root / "source.json"
    source_path.parent.mkdir(parents=True)
    source_path.write_text(json.dumps(_source()), encoding="utf-8")
    cache_root = layout.project_data_root / "cache/native"
    cache_root.mkdir(parents=True)
    monkeypatch.setattr(native.offline, "REQUIRED_DAYS", 1)
    monkeypatch.setattr(
        native.offline,
        "validate_canonical_manifest",
        lambda *args, **kwargs: _source(),
    )
    monkeypatch.setattr(
        native,
        "storage_preflight",
        lambda *args, **kwargs: {
            "required_hours": 72,
            "existing_hours": 0,
            "missing_hours": 72,
            "average_existing_hour_bytes": 1,
            "estimated_new_final_bytes": 72,
            "free_bytes": 10**12,
            "minimum_free_bytes": 1,
            "required_free_bytes": 1,
            "passed": True,
        },
    )

    def materialize(arguments: tuple[str, str, str]) -> dict[str, object]:
        day, _, cache = arguments
        hours = []
        for index in range(24):
            directory = Path(cache) / native.EXCHANGE / native.SYMBOL / day / f"{index:02d}"
            directory.mkdir(parents=True, exist_ok=True)
            source = layout.raw_orderbook_root / day / f"{index:02d}" / "BTCUSDC_orderbook.parquet.zst"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_bytes(b"source")
            data = directory / "logical_messages_test.parquet"
            manifest = directory / "logical_messages_test.manifest.json"
            data.write_bytes(b"data")
            manifest.write_text("{}", encoding="ascii")
            hours.append(
                {
                    "utc_hour": f"{day}T{index:02d}:00:00Z",
                    "source_path": str(source),
                    "cache_identity_sha256": f"{index:064x}",
                    "data_path": str(data),
                    "data_size_bytes": data.stat().st_size,
                    "data_sha256": native._file_sha256(data),
                    "manifest_path": str(manifest),
                    "manifest_sha256": native._file_sha256(manifest),
                    "event_count": 1,
                    "level_count": 2,
                }
            )
        completeness = {
            "expected_hour_count": 24,
            "complete_hour_count": 24,
            "verify_sha256": True,
            "canonical_identity_sha256": native._canonical_sha256(hours),
            "hours": hours,
        }
        return {"day": day, "cache_stats": {}, "completeness": completeness}

    monkeypatch.setattr(native, "_materialize_context_day", materialize)
    output = layout.project_data_root / "native-cache.json"
    result = native.build_manifest(
        source_manifest_path=source_path,
        cache_root=cache_root,
        output_path=output,
        workers=1,
        layout=layout,
    )
    assert result["context_day_count"] == 3
    assert result["complete_hour_count"] == 72
    assert result["permissions"]["economic_outcomes_read"] is False
    assert result["totals"] == {"event_count": 72, "level_count": 144}


def test_manifest_tamper_fails_before_cache_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = _layout(tmp_path)
    manifest = {
        "schema_version": native.SCHEMA_VERSION,
        "identity": native.IDENTITY,
        "context_days": [],
        "days": [],
        "manifest_sha256": "0" * 64,
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(native.OfflineNativeCacheError, match="manifest hash"):
        native.validate_manifest(path, layout=layout)
