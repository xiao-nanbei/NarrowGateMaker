from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest

from research.families.f02_empirical_p3_touch.audit.p3_reach_time_source_manifest import (
    build_source_day_manifest,
    canonical_manifest_sha256,
    normalize_panels,
    validate_source_day_manifest,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_source_files(root: Path, source: str, day: str) -> tuple[Path, Path]:
    bbo_root = root / source / "bbo"
    bbo_root.mkdir(parents=True, exist_ok=True)
    bbo = bbo_root / f"BTCUSDC-bbo-{day}.parquet"
    bbo.write_bytes(f"{source}-bbo-{day}".encode())
    trades = root / "raw" / f"BTCUSDC-aggTrades-{day}.csv"
    trades.parent.mkdir(parents=True, exist_ok=True)
    if not trades.exists():
        trades.write_text(f"trade-{day}\n", encoding="utf-8")
    return bbo, trades


def _write_provider_quality(
    quality_root: Path,
    *,
    day: str,
    bbo: Path,
    payload_day: str | None = None,
    bbo_hash: str | None = None,
) -> Path:
    quality_root.mkdir(parents=True, exist_ok=True)
    path = quality_root / f"BTCUSDC-{day}.json"
    path.write_text(
        json.dumps(
            {
                "dataset_id": "normalized_tardis_l2_100ms_v1",
                "symbol": "BTCUSDC",
                "day": payload_day or day,
                "clock_source": "tardis_provider_local",
                "clock_unit": "microseconds_since_unix_epoch_utc",
                "provider_normalized_replay_candidate": True,
                "complete_day": True,
                "cross_channel_contract_valid": True,
                "causal_violations": 0,
                "bbo_output": {
                    "path": str(bbo),
                    "sha256": _sha256(bbo) if bbo_hash is None else bbo_hash,
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def _write_native_quality(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "day",
        "coverage_99_valid",
        "formal_eligible",
        "source_label",
        "reconstruction_mode",
        "bbo_source_path",
        "bbo_sha256",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _fixture_tree(tmp_path: Path) -> dict[str, Path]:
    provider_quality = tmp_path / "normalized_tardis_l2_100ms_v1" / "quality"
    provider_bbo_root = tmp_path / "normalized_tardis_l2_100ms_v1" / "bbo"
    native_bbo_root = tmp_path / "normalized_l2_100ms_v2" / "bbo"
    native_quality = tmp_path / "normalized_l2_100ms_v2" / "daily_quality.csv"

    provider_2025, _ = _write_source_files(tmp_path, "normalized_tardis_l2_100ms_v1", "2025-08-01")
    provider_overlap, _ = _write_source_files(
        tmp_path, "normalized_tardis_l2_100ms_v1", "2026-01-02"
    )
    native_overlap, _ = _write_source_files(tmp_path, "normalized_l2_100ms_v2", "2026-01-02")
    _write_provider_quality(provider_quality, day="2025-08-01", bbo=provider_2025)
    _write_provider_quality(provider_quality, day="2026-01-02", bbo=provider_overlap)

    # This stale aggregate is poison by design.  The builder must never read it.
    (provider_quality.parent / "daily_quality.csv").write_text(
        "day,provider_normalized_replay_candidate\n1900-01-01,false\n",
        encoding="utf-8",
    )
    _write_native_quality(
        native_quality,
        [
            {
                "day": "2026-01-02",
                "coverage_99_valid": "True",
                "formal_eligible": "False",
                "source_label": "retained100ms",
                "reconstruction_mode": "delta_converged_120s",
                # A migrated root is legal; the basename/date and hash remain binding.
                "bbo_source_path": "/old/disk/BTCUSDC-bbo-2026-01-02.parquet",
                "bbo_sha256": _sha256(native_overlap),
            }
        ],
    )
    return {
        "provider_quality": provider_quality,
        "provider_bbo": provider_bbo_root,
        "native_quality": native_quality,
        "native_bbo": native_bbo_root,
        "trades": tmp_path / "raw",
    }


def _build(paths: dict[str, Path]) -> dict:
    return build_source_day_manifest(
        provider_quality_root=paths["provider_quality"],
        provider_bbo_root=paths["provider_bbo"],
        native_daily_quality_csv=paths["native_quality"],
        native_bbo_root=paths["native_bbo"],
        official_aggtrades_root=paths["trades"],
        panels=[
            {
                "name": "fit_2025_provider",
                "source": "provider",
                "dates": ["2025-08-01"],
            },
            {
                "name": "native_oof_1",
                "source": "native",
                "dates": ["2026-01-02"],
            },
        ],
        overlap_dates=["2026-01-02"],
    )


def test_builder_freezes_sources_and_weights_overlap_once(tmp_path: Path) -> None:
    paths = _fixture_tree(tmp_path)
    manifest = _build(paths)
    validate_source_day_manifest(manifest)

    assert manifest["economic_inputs_read"] is False
    assert manifest["source_contracts"]["provider"]["combined_provider_quality_csv_read"] is False
    assert [row["date"] for row in manifest["weighted_day_records"]] == [
        "2025-08-01",
        "2026-01-02",
    ]
    overlap = manifest["overlap_records"][0]
    assert overlap["primary_source"] == "native"
    assert overlap["weighting_count"] == 1
    assert overlap["duplicate_weighting"] is False
    assert manifest["provider_records"][0]["source_clock"] == "tardis_provider_local"
    assert manifest["native_records"][0]["source_authority"].startswith("normalized_100ms")
    assert manifest["canonical_manifest_sha256"] == canonical_manifest_sha256(manifest)


def test_manifest_hash_is_deterministic_and_binds_input_bytes(tmp_path: Path) -> None:
    paths = _fixture_tree(tmp_path)
    first = _build(paths)
    second = _build(paths)
    assert first == second

    trade = paths["trades"] / "BTCUSDC-aggTrades-2025-08-01.csv"
    trade.write_text("changed\n", encoding="utf-8")
    changed = _build(paths)
    assert changed["canonical_manifest_sha256"] != first["canonical_manifest_sha256"]


def test_panels_must_be_chronological_and_globally_disjoint() -> None:
    with pytest.raises(ValueError, match="chronological"):
        normalize_panels(
            [
                {
                    "name": "bad",
                    "source": "provider",
                    "dates": ["2025-08-02", "2025-08-01"],
                }
            ]
        )
    with pytest.raises(ValueError, match="disjoint"):
        normalize_panels(
            [
                {"name": "train", "source": "provider", "dates": ["2025-08-01"]},
                {"name": "test", "source": "native", "dates": ["2025-08-01"]},
            ]
        )


def test_missing_2025_official_aggtrades_fails(tmp_path: Path) -> None:
    paths = _fixture_tree(tmp_path)
    (paths["trades"] / "BTCUSDC-aggTrades-2025-08-01.csv").unlink()
    with pytest.raises(FileNotFoundError, match="official aggTrades missing"):
        _build(paths)


def test_provider_quality_missing_hash_fails(tmp_path: Path) -> None:
    paths = _fixture_tree(tmp_path)
    quality = paths["provider_quality"] / "BTCUSDC-2025-08-01.json"
    payload = json.loads(quality.read_text())
    payload["bbo_output"]["sha256"] = ""
    quality.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="missing a valid SHA256"):
        _build(paths)


def test_provider_payload_date_mismatch_fails(tmp_path: Path) -> None:
    paths = _fixture_tree(tmp_path)
    quality = paths["provider_quality"] / "BTCUSDC-2025-08-01.json"
    payload = json.loads(quality.read_text())
    payload["day"] = "2025-08-02"
    quality.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="source date mismatch"):
        _build(paths)


def test_native_declared_bbo_date_mismatch_fails(tmp_path: Path) -> None:
    paths = _fixture_tree(tmp_path)
    rows = list(csv.DictReader(paths["native_quality"].open()))
    rows[0]["bbo_source_path"] = "/old/disk/BTCUSDC-bbo-2026-01-03.parquet"
    _write_native_quality(paths["native_quality"], rows)
    with pytest.raises(ValueError, match="source date mismatch"):
        _build(paths)


def test_tampered_manifest_hash_is_rejected(tmp_path: Path) -> None:
    manifest = _build(_fixture_tree(tmp_path))
    manifest["weighting_contract"]["weighted_date_count"] = 99
    with pytest.raises(ValueError, match="canonical manifest hash mismatch"):
        validate_source_day_manifest(manifest)
