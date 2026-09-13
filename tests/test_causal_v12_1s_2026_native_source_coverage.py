from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_2026_native_source_coverage as coverage,
)


def _days(start: date, count: int) -> list[str]:
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _panel_specs(tmp_path: Path) -> tuple[Path, Path, Path]:
    first = _days(date(2026, 1, 2), 22)
    second = _days(date(2026, 2, 2), 22)
    transport = tmp_path / "transport.json"
    economic = tmp_path / "economic.json"
    forty = tmp_path / "forty.json"
    panels = [
        {"role": "historical_native_transport_development", "days": first},
        {"role": "historical_native_late_diagnostic", "days": second},
    ]
    _write_json(transport, {"panels": panels})
    _write_json(economic, {"panels": panels})
    _write_json(forty, {"panels": {"development_days": _days(date(2026, 3, 2), 40)}})
    return transport, economic, forty


def _roots(tmp_path: Path) -> coverage.HistoricalNativeRoots:
    return coverage.HistoricalNativeRoots(
        market_data_root=tmp_path,
        local_tempo_dir=tmp_path / "tempo",
        local_tempo_manifest=tmp_path / "tempo" / "manifest.json",
        native_l2_dir=tmp_path / "l2",
        native_l2_manifest=tmp_path / "l2" / "manifest.json",
        native_l2_quality=tmp_path / "l2" / "daily_quality.csv",
        metrics_dir=tmp_path / "metrics",
        reference_dir=tmp_path / "reference",
        alternate_reference_dir=tmp_path / "alternate",
        raw_reference_dir=tmp_path / "raw_reference",
    )


def _write_reference(root: Path, day: str, *, source_data_type: str = "trades") -> None:
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"BTCUSDT-1s-{day}.parquet"
    table = pa.table(
        {
            "timestamp": pa.array([0], type=pa.int64()),
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [1.0],
            "buy_volume": [1.0],
            "sell_volume": [0.0],
            "trade_count": [1],
            "buy_count": [1],
            "sell_count": [0],
        }
    )
    pq.write_table(table, path)
    _write_json(
        root / f"BTCUSDT-1s-{day}.parquet.meta.json",
        {
            "schema_version": "binance_individual_trade_bar_1s.v1",
            "symbol": "BTCUSDT",
            "utc_day": day,
            "source_data_type": source_data_type,
            "complete": True,
            "bar_interval": "[t,t+1s)",
            "causal_visible_at": "t+1s",
            "output_sha256": coverage.sha256_file(path),
            "rows": 1,
            "source_path": "/moved/raw/source.csv",
        },
    )


def _valid_day(day: str, *, target: bool = True, warmup: bool = True) -> dict:
    return {
        "day": day,
        "target_role_valid": target,
        "warmup_role_valid": warmup,
        "components": {
            "local_trade_tempo": {"valid": True, "errors": []},
            "native_normalized_l2_and_quality": {
                "valid": True,
                "errors": [],
                "formal_eligible": target,
                "warmup_valid": warmup,
            },
            "metrics": {"valid": True, "errors": []},
            "btcusdt_reference_bars_and_authority": {"valid": True, "errors": []},
        },
    }


def test_load_frozen_panels_binds_22_plus_22_and_40(tmp_path: Path) -> None:
    transport, economic, forty = _panel_specs(tmp_path)
    panels = coverage.load_frozen_panels(
        transport_spec_path=transport,
        economic_spec_path=economic,
        forty_day_spec_path=forty,
    )
    assert [len(panel.days) for panel in panels] == [22, 22, 44, 40]
    assert panels[2].days == panels[0].days + panels[1].days
    assert all(len(panel.spec_sha256) == 64 for panel in panels)


def test_load_frozen_panels_rejects_economic_denominator_drift(tmp_path: Path) -> None:
    transport, economic, forty = _panel_specs(tmp_path)
    payload = json.loads(economic.read_text(encoding="utf-8"))
    payload["panels"][0]["days"][0] = "2025-12-31"
    _write_json(economic, payload)
    with pytest.raises(coverage.CoverageContractError, match="day mismatch"):
        coverage.load_frozen_panels(
            transport_spec_path=transport,
            economic_spec_path=economic,
            forty_day_spec_path=forty,
        )


def test_reference_authority_is_exact_and_hash_bound(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    day = "2026-04-13"
    _write_reference(roots.reference_dir, day)
    result = coverage._audit_reference_day(day, roots, coverage.FileIdentityCache())
    assert result["valid"] is True
    assert result["authority_source_data_type"] == "trades"
    assert result["alternate_artifact_observed_not_used"]["used"] is False


def test_known_alternate_reference_is_observed_but_never_used_as_fallback(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path)
    day = "2026-04-12"
    _write_reference(roots.alternate_reference_dir, day, source_data_type="aggTrades")
    result = coverage._audit_reference_day(day, roots, coverage.FileIdentityCache())
    assert result["valid"] is False
    assert "reference_bar_file_missing" in result["errors"]
    alternate = result["alternate_artifact_observed_not_used"]
    assert alternate["known_exact_alternate_day"] is True
    assert alternate["bar"]["exists"] is True
    assert alternate["used"] is False


def test_reference_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    day = "2026-04-13"
    _write_reference(roots.reference_dir, day)
    meta_path = roots.reference_dir / f"BTCUSDT-1s-{day}.parquet.meta.json"
    payload = json.loads(meta_path.read_text(encoding="utf-8"))
    payload["output_sha256"] = "0" * 64
    _write_json(meta_path, payload)
    result = coverage._audit_reference_day(day, roots, coverage.FileIdentityCache())
    assert result["valid"] is False
    assert "reference_bar_output_sha256_mismatch" in result["errors"]


def test_l2_warmup_validity_is_not_replaced_by_target_quality(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    day = "2026-04-21"
    roots.native_l2_dir.mkdir(parents=True)
    path = roots.native_l2_dir / f"BTCUSDC-l2-{day}.parquet"
    pq.write_table(pa.table({"timestamp": [1]}), path)
    identity = coverage.FileIdentityCache().record(path)
    manifest = {
        day: {
            "day": day,
            "kind": "l2",
            "destination_relative_path": f"l2/BTCUSDC-l2-{day}.parquet",
            "source_identity": {
                "sha256": identity["sha256"],
                "size_bytes": identity["size_bytes"],
            },
        }
    }
    quality = {
        day: {
            "day": day,
            "l2_sha256": identity["sha256"],
            "l2_size_bytes": str(identity["size_bytes"]),
            "sequence_valid": "false",
            "coverage_99_valid": "false",
            "warmup_valid": "true",
            "formal_eligible": "false",
            "target_source_valid": "true",
            "source_formal_capable": "true",
            "cadence_schema_valid": "true",
        }
    }
    result = coverage._audit_l2_day(day, roots, manifest, quality, coverage.FileIdentityCache())
    assert result["valid"] is True
    assert result["warmup_valid"] is True
    assert result["formal_eligible"] is False


def test_panel_coverage_requires_exact_previous_natural_day() -> None:
    panel = coverage.FrozenPanel(
        panel_id="one",
        role="test",
        days=("2026-04-13",),
        spec_path=Path("/spec.json"),
        spec_sha256="a" * 64,
    )
    profile = {
        "days": [
            _valid_day("2026-04-12", warmup=False),
            _valid_day("2026-04-13"),
        ]
    }
    result = coverage.panel_coverage((panel,), profile)[0]
    assert result["complete"] is False
    assert result["accepted_count"] == 0
    assert result["rejected_days"][0]["warmup_day"] == "2026-04-12"
    assert any("warmup_valid_false" in reason for reason in result["rejected_days"][0]["reasons"])


def test_auditor_source_contains_no_glob_discovery() -> None:
    source = Path(coverage.__file__).read_text(encoding="utf-8")
    assert ".glob(" not in source
    assert ".rglob(" not in source


def test_repository_frozen_specs_keep_expected_denominators() -> None:
    panels = coverage.load_frozen_panels(
        transport_spec_path=coverage.DEFAULT_TRANSPORT_SPEC,
        economic_spec_path=coverage.DEFAULT_ECONOMIC_SPEC,
        forty_day_spec_path=coverage.DEFAULT_40_DAY_SPEC,
    )
    assert [len(panel.days) for panel in panels] == [22, 22, 44, 40]
    roles = coverage.required_day_roles(panels)
    assert len({day for panel in panels for day in panel.days}) == 61
    assert len(roles) == 76
