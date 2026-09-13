from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models.audit import native_l2_daily_quality as quality
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_daily_sources as daily,
)

DAY = "2026-07-29"
PREVIOUS_DAY = "2026-07-28"


def _identity(path: Path) -> dict[str, object]:
    return {
        "path": str(path.resolve()),
        "sha256": quality.sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _write_book(root: Path, day: str) -> tuple[Path, Path]:
    start_ms = int(pd.Timestamp(day, tz="UTC").value // 1_000_000)
    timestamps = np.asarray([start_ms + 100, start_ms + 200, start_ms + 300])
    bbo = pd.DataFrame(
        {
            "timestamp": timestamps,
            "best_bid": [65_000.0, 65_000.1, 65_000.2],
            "best_bid_qty": [1.0, 1.1, 1.2],
            "best_ask": [65_000.1, 65_000.2, 65_000.3],
            "best_ask_qty": [1.3, 1.4, 1.5],
        }
    )
    l2_columns: dict[str, object] = {"timestamp": timestamps}
    for level in range(1, 21):
        l2_columns[f"bid_px_{level}"] = bbo["best_bid"] - (level - 1) * 0.1
        l2_columns[f"bid_qty_{level}"] = bbo["best_bid_qty"] + (level - 1) * 0.01
        l2_columns[f"ask_px_{level}"] = bbo["best_ask"] + (level - 1) * 0.1
        l2_columns[f"ask_qty_{level}"] = bbo["best_ask_qty"] + (level - 1) * 0.01
    bbo_path = root / "bbo" / f"BTCUSDC-bbo-{day}.parquet"
    l2_path = root / "l2" / f"BTCUSDC-l2-{day}.parquet"
    bbo_path.parent.mkdir(parents=True, exist_ok=True)
    l2_path.parent.mkdir(parents=True, exist_ok=True)
    bbo.to_parquet(bbo_path, index=False)
    pd.DataFrame(l2_columns).to_parquet(l2_path, index=False)
    return bbo_path, l2_path


def _build_fixture(tmp_path: Path, *, timestamp_source: str = "transaction") -> dict[str, Path]:
    root = tmp_path / "normalized_l2_registry"
    bbo_path, l2_path = _write_book(root, DAY)
    daily_quality = root / "daily_quality.csv"
    pd.DataFrame(
        [
            {
                "day": DAY,
                "sequence_valid": True,
                "warmup_valid": True,
                "target_source_valid": True,
                "formal_eligible": True,
                "bbo_rows": 3,
                "l2_rows": 3,
            }
        ]
    ).to_csv(daily_quality, index=False)
    sequence_summary = tmp_path / "sequence_summary.csv"
    pd.DataFrame(
        [
            {
                "day": DAY,
                "eligible": True,
                "sequence_gaps": 0,
                "initialized_at_start": True,
                "initialization_source": "snapshot",
            }
        ]
    ).to_csv(sequence_summary, index=False)
    availability = tmp_path / "source_availability.csv"
    pd.DataFrame(
        [
            {
                "day": DAY,
                "target_complete": True,
                "warmup_complete": True,
                "target_hours_present": 24,
                "warmup_hours_present": 24,
            }
        ]
    ).to_csv(availability, index=False)
    detail = tmp_path / "sequence_detail.json"
    detail.write_text(
        json.dumps(
            {
                "schema_version": quality.EXPECTED_SEQUENCE_SCHEMA_VERSION,
                "timestamp_source": timestamp_source,
                "snapshot_ms": 100,
                "levels": 20,
                "symbols": {"BTCUSDC": {}},
                "range_audits": [
                    {
                        "sequence_audit": {
                            "day_sequence_audits": {
                                DAY: {
                                    "target_initialized_at_start": True,
                                    "target_initialization_source_at_start": "snapshot",
                                    "target_sequence_gaps": 0,
                                    "target_invalid_sequence_messages": 0,
                                    "target_message_time_reversals": 0,
                                    "target_stale_updates": 0,
                                    "target_duplicate_messages": 0,
                                    "target_accepted_updates": 10,
                                    "target_snapshot_messages": 1,
                                }
                            }
                        }
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    manifest = root / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "dataset_version": quality.EXPECTED_REGISTRY_DATASET_VERSION,
                "contract_version": quality.EXPECTED_REGISTRY_CONTRACT_VERSION,
                "symbol": "BTCUSDC",
                "daily_quality": _identity(daily_quality),
                "inputs": {
                    "sequence_audit": _identity(sequence_summary),
                    "source_availability": _identity(availability),
                },
                "files": [
                    {
                        "day": DAY,
                        "kind": "bbo",
                        "destination_relative_path": str(bbo_path.relative_to(root)),
                        "source_identity": _identity(bbo_path),
                    },
                    {
                        "day": DAY,
                        "kind": "l2",
                        "destination_relative_path": str(l2_path.relative_to(root)),
                        "source_identity": _identity(l2_path),
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    return {
        "root": root,
        "manifest": manifest,
        "detail": detail,
        "bbo": bbo_path,
        "l2": l2_path,
    }


def _build_quality(paths: dict[str, Path]) -> dict[str, object]:
    return quality.build_native_l2_day_quality(
        registry_root=paths["root"],
        registry_manifest_path=paths["manifest"],
        detailed_sequence_audit_path=paths["detail"],
        day=DAY,
        min_coverage=0.0,
        max_warmup_end_age_s=86_400.0,
    )


def test_native_daily_quality_binds_mechanics_and_file_identity(tmp_path: Path) -> None:
    paths = _build_fixture(tmp_path)

    artifact = _build_quality(paths)

    assert artifact["source_kind"] == quality.SOURCE_KIND
    assert artifact["clock_source"] == quality.CLOCK_SOURCE
    assert artifact["provider_normalized_replay_candidate"] is False
    assert artifact["native_sequence_valid"] is True
    assert artifact["normalized_structural_valid"] is True
    assert artifact["target_replay_candidate"] is True
    assert artifact["midnight_warmup_candidate"] is True
    assert artifact["depth_quality"]["available_levels"] == 20
    assert artifact["cross_channel_quality"]["valid"] is True
    assert artifact["l2_output"]["sha256"] == quality.sha256_file(paths["l2"])
    assert artifact["economic_outcomes_read"] is False
    assert artifact["training_authorized"] is False


def test_native_daily_quality_rejects_non_transaction_clock(tmp_path: Path) -> None:
    paths = _build_fixture(tmp_path, timestamp_source="receive")

    with pytest.raises(quality.NativeL2QualityError, match="not transaction-time"):
        _build_quality(paths)


def test_native_daily_quality_rejects_registry_file_hash_drift(tmp_path: Path) -> None:
    paths = _build_fixture(tmp_path)
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    manifest["files"][1]["source_identity"]["sha256"] = "0" * 64
    paths["manifest"].write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(quality.NativeL2QualityError, match="output SHA256 mismatch"):
        _build_quality(paths)


def test_large_internal_gap_can_be_warmup_only() -> None:
    target = quality._admission_reasons(
        normalized_reasons=(),
        sequence_reasons=(),
        source_complete=True,
        cross_channel_valid=True,
        max_gap_s=419.735,
        max_target_gap_s=5.0,
        end_age_s=0.129,
        max_warmup_end_age_s=0.5,
        warmup_role=False,
    )
    warmup = quality._admission_reasons(
        normalized_reasons=(),
        sequence_reasons=(),
        source_complete=True,
        cross_channel_valid=True,
        max_gap_s=419.735,
        max_target_gap_s=5.0,
        end_age_s=0.129,
        max_warmup_end_age_s=0.5,
        warmup_role=True,
    )

    assert target == ["max_contiguous_gap_exceeds_target_limit"]
    assert warmup == []


def test_f03_accepts_native_schema_without_provider_upgrade(tmp_path: Path) -> None:
    paths = _build_fixture(tmp_path)
    artifact = _build_quality(paths)
    quality_path = tmp_path / f"BTCUSDC-{DAY}.json"
    quality_path.write_text(json.dumps(artifact), encoding="utf-8")
    local_path = tmp_path / f"local-{DAY}.txt"
    local_path.write_text("fixture\n", encoding="utf-8")
    bundle = daily.DailySourceBundle(
        utc_day=DAY,
        local_trade_tempo_paths=(local_path,),
        execution_l2_paths=(paths["l2"],),
        execution_l2_quality_paths=(quality_path,),
        execution_l2_clock_identity=quality.CLOCK_SOURCE,
    )

    admitted = daily._validate_l2_quality(bundle, (DAY,))

    assert admitted["valid"] is True
    assert admitted["bound_days"][0]["quality_schema"] == quality.SCHEMA_VERSION
    assert artifact["provider_normalized_replay_candidate"] is False


def test_f03_rejects_native_artifact_identity_drift(tmp_path: Path) -> None:
    paths = _build_fixture(tmp_path)
    artifact = _build_quality(paths)
    artifact["target_replay_candidate"] = False
    quality_path = tmp_path / f"BTCUSDC-{DAY}.json"
    quality_path.write_text(json.dumps(artifact), encoding="utf-8")
    local_path = tmp_path / f"local-{DAY}.txt"
    local_path.write_text("fixture\n", encoding="utf-8")
    bundle = daily.DailySourceBundle(
        utc_day=DAY,
        local_trade_tempo_paths=(local_path,),
        execution_l2_paths=(paths["l2"],),
        execution_l2_quality_paths=(quality_path,),
        execution_l2_clock_identity=quality.CLOCK_SOURCE,
    )

    admitted = daily._validate_l2_quality(bundle, (DAY,))

    assert admitted["valid"] is False
    assert "native L2 quality identity SHA256 mismatch" in admitted["errors"][0]


def test_f03_rejects_native_target_when_only_warmup_is_authorized(tmp_path: Path) -> None:
    paths = _build_fixture(tmp_path)
    artifact = _build_quality(paths)
    artifact["target_replay_candidate"] = False
    artifact.pop("identity_sha256")
    artifact["identity_sha256"] = quality._canonical_sha256(artifact)
    quality_path = tmp_path / f"BTCUSDC-{DAY}.json"
    quality_path.write_text(json.dumps(artifact), encoding="utf-8")
    local_path = tmp_path / f"local-{DAY}.txt"
    local_path.write_text("fixture\n", encoding="utf-8")
    bundle = daily.DailySourceBundle(
        utc_day=DAY,
        local_trade_tempo_paths=(local_path,),
        execution_l2_paths=(paths["l2"],),
        execution_l2_quality_paths=(quality_path,),
        execution_l2_clock_identity=quality.CLOCK_SOURCE,
    )

    admitted = daily._validate_l2_quality(bundle, (DAY,))

    assert admitted["valid"] is False
    assert "native L2 target_replay_candidate is false" in admitted["errors"][0]
