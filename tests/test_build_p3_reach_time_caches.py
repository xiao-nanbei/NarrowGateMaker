from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from research.families.f02_empirical_p3_touch.audit.p3_reach_time_source_manifest import (
    build_source_day_manifest,
)
from scripts.build_p3_reach_time_caches import (
    CacheBuildParameters,
    load_and_validate_source_manifest,
    run_cache_build,
    select_cache_jobs,
    storage_preflight,
)


def _sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_bbo(path: Path, day: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1_000)
    timestamps = start + np.arange(0, 86_400_000, 60_000, dtype=np.int64)
    phase = np.arange(len(timestamps), dtype=np.int64) % 20
    mid = 100.0 + phase.astype(np.float64) * 0.1
    pd.DataFrame(
        {
            "timestamp": timestamps,
            "best_bid": mid - 0.1,
            "best_ask": mid + 0.1,
        }
    ).to_parquet(path, index=False)


def _write_trades(path: Path, day: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1_000)
    pd.DataFrame(
        {
            "price": [100.0, 100.2, 99.9, 100.3],
            "transact_time": [
                start + 61_000,
                start + 121_000,
                start + 181_000,
                start + 241_000,
            ],
            "is_buyer_maker": [True, False, True, False],
        }
    ).to_csv(path, index=False)


def _write_provider_quality(path: Path, *, day: str, bbo: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "dataset_id": "normalized_tardis_l2_100ms_v1",
                "symbol": "BTCUSDC",
                "day": day,
                "clock_source": "tardis_provider_local",
                "clock_unit": "milliseconds_since_unix_epoch_utc",
                "provider_normalized_replay_candidate": True,
                "complete_day": True,
                "cross_channel_contract_valid": True,
                "causal_violations": 0,
                "bbo_output": {"path": str(bbo), "sha256": _sha256(bbo)},
            }
        ),
        encoding="utf-8",
    )


def _write_native_quality(path: Path, *, day: str, bbo: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "day",
                "coverage_99_valid",
                "formal_eligible",
                "source_label",
                "reconstruction_mode",
                "bbo_source_path",
                "bbo_sha256",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "day": day,
                "coverage_99_valid": "True",
                "formal_eligible": "False",
                "source_label": "retained100ms",
                "reconstruction_mode": "delta_converged_120s",
                "bbo_source_path": str(bbo),
                "bbo_sha256": _sha256(bbo),
            }
        )


def _fixture_manifest(tmp_path: Path) -> tuple[Path, dict]:
    provider_day = "2025-08-01"
    native_day = "2026-01-02"
    provider_root = tmp_path / "provider"
    native_root = tmp_path / "native"
    raw_root = tmp_path / "raw"

    provider_bbo = provider_root / "bbo" / f"BTCUSDC-bbo-{provider_day}.parquet"
    provider_overlap_bbo = provider_root / "bbo" / f"BTCUSDC-bbo-{native_day}.parquet"
    native_bbo = native_root / "bbo" / f"BTCUSDC-bbo-{native_day}.parquet"
    for path, day in (
        (provider_bbo, provider_day),
        (provider_overlap_bbo, native_day),
        (native_bbo, native_day),
    ):
        _write_bbo(path, day)
    for day in (provider_day, native_day):
        _write_trades(raw_root / f"BTCUSDC-aggTrades-{day}.csv", day)

    provider_quality = provider_root / "quality"
    _write_provider_quality(
        provider_quality / f"BTCUSDC-{provider_day}.json",
        day=provider_day,
        bbo=provider_bbo,
    )
    _write_provider_quality(
        provider_quality / f"BTCUSDC-{native_day}.json",
        day=native_day,
        bbo=provider_overlap_bbo,
    )
    native_quality = native_root / "daily_quality.csv"
    _write_native_quality(native_quality, day=native_day, bbo=native_bbo)

    manifest = build_source_day_manifest(
        provider_quality_root=provider_quality,
        provider_bbo_root=provider_root / "bbo",
        native_daily_quality_csv=native_quality,
        native_bbo_root=native_root / "bbo",
        official_aggtrades_root=raw_root,
        panels=[
            {
                "name": "fit_provider",
                "source": "provider",
                "dates": [provider_day],
            },
            {
                "name": "fit_native",
                "source": "native",
                "dates": [native_day],
            },
        ],
        overlap_dates=[native_day],
    )
    path = tmp_path / "source_manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path, manifest


def _parameters() -> CacheBuildParameters:
    return CacheBuildParameters(
        cadence_ms=60_000,
        past_warmup_s=60,
        administrative_censor_ms=60_000,
        max_bbo_age_ms=60_000,
        fast_window_s=10,
        slow_window_s=60,
        time_step_ms=1_000,
        max_distance_ticks=50,
    )


def test_default_selection_is_weighted_and_overlap_is_explicit(tmp_path: Path) -> None:
    _, manifest = _fixture_manifest(tmp_path)
    weighted = select_cache_jobs(manifest)
    assert [(job.day, job.source, job.weighted) for job in weighted] == [
        ("2025-08-01", "provider", True),
        ("2026-01-02", "native", True),
    ]

    overlap = select_cache_jobs(manifest, overlap_source_profiles=["provider", "native"])
    assert [(job.day, job.source, job.weighted) for job in overlap] == [
        ("2026-01-02", "native", False),
        ("2026-01-02", "provider", False),
    ]
    assert select_cache_jobs(manifest, panels=["fit_native"])[0].source == "native"
    with pytest.raises(ValueError, match="outside"):
        select_cache_jobs(manifest, days=["2020-01-01"])


def test_manifest_validation_rehashes_all_source_files(tmp_path: Path) -> None:
    manifest_path, manifest = _fixture_manifest(tmp_path)
    load_and_validate_source_manifest(manifest_path)
    trade = Path(manifest["provider_records"][0]["files"]["official_aggtrades"]["path"])
    trade.write_text("tampered\n", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        load_and_validate_source_manifest(manifest_path)


def test_build_resume_progress_and_atomic_summary(tmp_path: Path) -> None:
    manifest_path, _ = _fixture_manifest(tmp_path)
    cache_root = tmp_path / "internal_cache"
    summary_path = tmp_path / "summary.json"
    progress = io.StringIO()
    first = run_cache_build(
        manifest_path=manifest_path,
        cache_root=cache_root,
        summary_path=summary_path,
        parameters=_parameters(),
        days=["2025-08-01"],
        minimum_free_bytes=1,
        safety_reserve_bytes=0,
        progress_stream=progress,
    )
    assert first["counts"]["context_built"] == 1
    assert first["counts"]["label_built"] == 1
    assert first["economic_outcomes_read"] is False
    assert first["queue_inputs_read"] is False
    assert first["order_lifecycle_inputs_read"] is False
    row = first["jobs"][0]
    assert Path(row["context_path"]).is_file()
    assert Path(row["label_path"]).is_file()
    persisted = json.loads(summary_path.read_text(encoding="utf-8"))
    assert persisted["canonical_summary_sha256"] == first["canonical_summary_sha256"]
    events = [json.loads(line)["event"] for line in progress.getvalue().splitlines()]
    assert events == ["preflight", "day_start", "day_complete", "summary"]

    second = run_cache_build(
        manifest_path=manifest_path,
        cache_root=cache_root,
        summary_path=summary_path,
        parameters=_parameters(),
        days=["2025-08-01"],
        minimum_free_bytes=1,
        safety_reserve_bytes=0,
        progress_stream=None,
    )
    assert second["counts"]["context_loaded"] == 1
    assert second["counts"]["label_loaded"] == 1
    assert first["jobs"][0]["context_cache_key"] == second["jobs"][0]["context_cache_key"]


def test_dry_run_writes_summary_but_not_cache_entries(tmp_path: Path) -> None:
    manifest_path, _ = _fixture_manifest(tmp_path)
    cache_root = tmp_path / "cache"
    summary = run_cache_build(
        manifest_path=manifest_path,
        cache_root=cache_root,
        parameters=_parameters(),
        panels=["fit_native"],
        dry_run=True,
        minimum_free_bytes=1,
        safety_reserve_bytes=0,
        progress_stream=None,
    )
    assert summary["counts"]["context_planned"] == 1
    assert summary["counts"]["label_planned"] == 1
    assert not Path(summary["jobs"][0]["context_path"]).exists()
    assert (cache_root / "p3_reach_time_cache_build_summary_v1.json").is_file()


def test_cache_keys_bind_parameters_not_exposed_by_legacy_key_signature(
    tmp_path: Path,
) -> None:
    manifest_path, _ = _fixture_manifest(tmp_path)
    common = {
        "manifest_path": manifest_path,
        "cache_root": tmp_path / "cache",
        "days": ["2025-08-01"],
        "dry_run": True,
        "minimum_free_bytes": 1,
        "safety_reserve_bytes": 0,
        "progress_stream": None,
    }
    first = run_cache_build(
        **common,
        summary_path=tmp_path / "first.json",
        parameters=_parameters(),
    )
    second = run_cache_build(
        **common,
        summary_path=tmp_path / "second.json",
        parameters=CacheBuildParameters(
            cadence_ms=60_000,
            past_warmup_s=120,
            administrative_censor_ms=60_000,
            max_bbo_age_ms=60_000,
            fast_window_s=10,
            slow_window_s=60,
            time_step_ms=1_000,
            max_distance_ticks=50,
        ),
    )
    assert first["jobs"][0]["context_cache_key"] != second["jobs"][0]["context_cache_key"]
    assert first["jobs"][0]["label_cache_key"] != second["jobs"][0]["label_cache_key"]


def test_cache_root_and_storage_gate_fail_closed(tmp_path: Path, monkeypatch) -> None:
    project_data_root = Path("/srv/example-data/NarrowGate_BTCUSDC")
    external_cache_root = project_data_root / "cache"
    monkeypatch.setattr(
        "scripts.build_p3_reach_time_caches.data_paths.data_root",
        lambda: project_data_root,
    )
    monkeypatch.setattr(
        "scripts.build_p3_reach_time_caches.data_paths.external_cache_root",
        lambda: external_cache_root,
    )
    with pytest.raises(ValueError, match="requires explicit"):
        storage_preflight(
            external_cache_root / "p3",
            minimum_free_bytes=1,
            safety_reserve_bytes=0,
        )

    external = storage_preflight(
        external_cache_root / "p3",
        minimum_free_bytes=1,
        safety_reserve_bytes=0,
        allow_external_cache_root=True,
    )
    assert external["storage_tier"] == "external_removable_cache"

    with pytest.raises(ValueError, match="dedicated cache namespace"):
        storage_preflight(
            project_data_root / "raw",
            minimum_free_bytes=1,
            safety_reserve_bytes=0,
            allow_external_cache_root=True,
        )

    usage = SimpleNamespace(total=100, used=99, free=1)
    monkeypatch.setattr(
        "scripts.build_p3_reach_time_caches.shutil.disk_usage",
        lambda _: usage,
    )
    with pytest.raises(RuntimeError, match="storage gate failed"):
        storage_preflight(
            tmp_path / "cache",
            minimum_free_bytes=2,
            safety_reserve_bytes=0,
        )
    assert usage.free == 1


def test_storage_gate_includes_reserve_and_atomic_peak(tmp_path: Path, monkeypatch) -> None:
    usage = SimpleNamespace(total=1_000, used=500, free=500)
    monkeypatch.setattr(
        "scripts.build_p3_reach_time_caches.shutil.disk_usage",
        lambda _: usage,
    )
    diagnostic = storage_preflight(
        tmp_path / "cache",
        minimum_free_bytes=100,
        safety_reserve_bytes=300,
        estimated_new_final_bytes=100,
        atomic_overlap_multiplier=2.5,
        enforce=False,
    )
    assert diagnostic["required_free_bytes"] == 550
    assert diagnostic["estimated_peak_new_bytes"] == 250
    assert diagnostic["passed"] is False
    with pytest.raises(RuntimeError, match="required=550"):
        storage_preflight(
            tmp_path / "cache",
            minimum_free_bytes=100,
            safety_reserve_bytes=300,
            estimated_new_final_bytes=100,
            atomic_overlap_multiplier=2.5,
        )
