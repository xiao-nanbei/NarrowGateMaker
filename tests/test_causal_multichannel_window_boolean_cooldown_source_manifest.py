from __future__ import annotations

import csv
import json
import stat
from datetime import date, timedelta
from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_source_manifest as source,
)


def _write_csv(path: Path, header: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerow(["1"] * len(header))


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    union = tmp_path / "union"
    agg = tmp_path / "raw"
    individual = tmp_path / "raw_trades" / source.SYMBOL
    target_days = ("2025-08-02", "2025-08-03")
    source_days = ("2025-08-01", *target_days)
    rows = []
    for day in source_days:
        bbo = union / "bbo" / f"{source.SYMBOL}-bbo-{day}.parquet"
        l2 = union / "l2" / f"{source.SYMBOL}-l2-{day}.parquet"
        bbo.parent.mkdir(parents=True, exist_ok=True)
        l2.parent.mkdir(parents=True, exist_ok=True)
        bbo.write_bytes(f"bbo-{day}".encode())
        l2.write_bytes(f"l2-{day}".encode())
        _write_csv(
            agg / f"{source.SYMBOL}-aggTrades-{day}.csv",
            source._AGGTRADE_HEADER,
        )
        _write_csv(
            individual / f"{source.SYMBOL}-trades-{day}.csv",
            source._INDIVIDUAL_TRADE_HEADER,
        )
        rows.append(
            {
                "day": day,
                "source_authority": source.SOURCE_AUTHORITY,
                "bbo_sha256": source.sha256_file(bbo),
                "l2_sha256": source.sha256_file(l2),
            }
        )
    days_path = union / "provider_replay_days.csv"
    days_path.parent.mkdir(parents=True, exist_ok=True)
    with days_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["day"])
        writer.writerows((day,) for day in target_days)
    manifest = {
        "dataset_version": "normalized_l2_research_union_v1",
        "symbol": source.SYMBOL,
        "outputs": {
            "provider_replay_days.csv": {
                "path": str(days_path),
                "sha256": source.sha256_file(days_path),
            }
        },
        "source_files": rows,
    }
    (union / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return union, agg, individual


def test_build_and_validate_source_intersection(tmp_path: Path) -> None:
    union, agg, individual = _fixture(tmp_path)
    manifest = source.build_manifest(
        union_root=union,
        aggtrades_root=agg,
        individual_trades_root=individual,
    )
    assert manifest["target_day_count"] == 2
    assert manifest["unique_source_day_count"] == 3
    assert manifest["clock_contract"]["exact_historical_receive_time_present"] is False
    assert manifest["clock_contract"]["bbo_l2_clock"] == (
        "provider_local_receive_time_right_boundary_100ms"
    )
    assert manifest["clock_contract"]["book_trade_joint_visibility_authority"] is False
    assert manifest["clock_contract"]["trade_derived_M2_action_grade_support"] is False
    assert manifest["permission_boundary"]["economic_outcomes_read"] is False
    source.validate_manifest(manifest)


def test_missing_d_minus_one_fails_closed(tmp_path: Path) -> None:
    union, agg, individual = _fixture(tmp_path)
    prior = date.fromisoformat("2025-08-02") - timedelta(days=1)
    (union / "bbo" / f"{source.SYMBOL}-bbo-{prior.isoformat()}.parquet").unlink()
    with pytest.raises(source.SourceManifestError, match="missing"):
        source.build_manifest(
            union_root=union,
            aggtrades_root=agg,
            individual_trades_root=individual,
        )


def test_trade_schema_drift_fails_closed(tmp_path: Path) -> None:
    union, agg, individual = _fixture(tmp_path)
    path = agg / f"{source.SYMBOL}-aggTrades-2025-08-02.csv"
    _write_csv(path, ("price", "qty"))
    with pytest.raises(source.SourceManifestError, match="schema drifted"):
        source.build_manifest(
            union_root=union,
            aggtrades_root=agg,
            individual_trades_root=individual,
        )


def test_manifest_hash_and_permission_drift_fail_closed(tmp_path: Path) -> None:
    union, agg, individual = _fixture(tmp_path)
    manifest = source.build_manifest(
        union_root=union,
        aggtrades_root=agg,
        individual_trades_root=individual,
    )
    manifest["permission_boundary"]["action_authorized"] = True
    manifest["canonical_manifest_sha256"] = source.canonical_manifest_sha256(manifest)
    with pytest.raises(source.SourceManifestError, match="permission boundary"):
        source.validate_manifest(manifest, rehash_sources=False)

    manifest = source.build_manifest(
        union_root=union,
        aggtrades_root=agg,
        individual_trades_root=individual,
    )
    manifest["clock_contract"]["book_trade_joint_visibility_authority"] = True
    manifest["canonical_manifest_sha256"] = source.canonical_manifest_sha256(manifest)
    with pytest.raises(source.SourceManifestError, match="authority overstated"):
        source.validate_manifest(manifest, rehash_sources=False)


def test_atomic_manifest_write_fsyncs_file_and_parent_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fsync_targets: list[str] = []
    real_fsync = source.os.fsync

    def recording_fsync(descriptor: int) -> None:
        mode = source.os.fstat(descriptor).st_mode
        fsync_targets.append("directory" if stat.S_ISDIR(mode) else "file")
        real_fsync(descriptor)

    monkeypatch.setattr(source.os, "fsync", recording_fsync)
    output = tmp_path / "nested" / "source-manifest.json"
    source._atomic_write_json(output, {"identity": "test"})

    assert json.loads(output.read_text(encoding="utf-8")) == {"identity": "test"}
    assert fsync_targets == ["file", "directory"]
