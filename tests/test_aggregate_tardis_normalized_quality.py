from __future__ import annotations

import csv
import hashlib
import json
from datetime import date
from pathlib import Path

from data.aggregate_tardis_normalized_quality import (
    aggregate_quality,
    freeze_manifests,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def test_freeze_keeps_technical_and_formal_overlap_separate(tmp_path: Path) -> None:
    canonical = tmp_path / "daily_quality.csv"
    with canonical.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["day", "formal_eligible", "bbo_sha256", "l2_sha256"],
        )
        writer.writeheader()
        writer.writerows(
            [
                {
                    "day": "2026-01-01",
                    "formal_eligible": "true",
                    "bbo_sha256": _digest("cb"),
                    "l2_sha256": _digest("cl"),
                },
                {
                    "day": "2026-01-02",
                    "formal_eligible": "false",
                    "bbo_sha256": _digest("xb"),
                    "l2_sha256": _digest("xl"),
                },
            ]
        )
    manifest = tmp_path / "downloads.json"
    manifest.write_text(
        json.dumps(
            {
                "complete": True,
                "downloads": [
                    {
                        "day": "2026-01-01",
                        "dataset": dataset,
                        "exists": True,
                        "zstd_valid": True,
                        "size_bytes": 10,
                        "content_length": 10,
                        "sha256": _digest(dataset),
                    }
                    for dataset in ("book_ticker", "incremental_book_L2")
                ],
            }
        ),
        encoding="utf-8",
    )
    technical = tmp_path / "technical.csv"
    overlap = tmp_path / "overlap.csv"
    contract = tmp_path / "contract.json"
    result = freeze_manifests(
        canonical_quality=canonical,
        tardis_manifest=manifest,
        technical_start=date(2025, 12, 30),
        technical_end=date(2025, 12, 31),
        technical_csv=technical,
        overlap_csv=overlap,
        contract_json=contract,
    )
    assert result["technical_target"]["day_count"] == 2
    assert result["formal_overlap"]["day_count"] == 1
    assert list(csv.DictReader(overlap.open()))[0]["day"] == "2026-01-01"
    assert json.loads(contract.read_text())["identity_boundary"][
        "canonical_good_day_modified"
    ] is False


def test_aggregate_reports_candidates_failures_and_dual_source(tmp_path: Path) -> None:
    quality_dir = tmp_path / "quality"
    quality_dir.mkdir()
    technical = tmp_path / "technical.csv"
    technical.write_text("day\n2025-08-01\n2025-08-02\n", encoding="utf-8")
    overlap = tmp_path / "overlap.csv"
    overlap.write_text("day\n2026-01-01\n", encoding="utf-8")
    base = {
        "dataset_id": "normalized_tardis_l2_100ms_v1",
        "source_id": "tardis.0730-beinan.binance-futures.BTCUSDC.v1",
        "complete_day": True,
        "snapshot_seen_at_start": True,
        "causal_violations": 0,
        "local_clock_reversals": 0,
        "invalid_spread_buckets": 0,
        "freshness_union_coverage": 0.999,
        "output_p99_gap_ms": 200,
        "logical_message_gap": {"p99_upper_us": 100_000, "maximum_us": 200_000},
        "cross_channel_contract_valid": True,
        "provider_normalized_replay_candidate": True,
        "policy_visible": False,
        "exact_queue_policy_eligible": False,
        "book_ticker_audit": {
            "book_ticker_comparable_ratio": 1.0,
            "book_ticker_price_exact_ratio": 0.98,
            "book_ticker_price_within_one_tick_ratio": 0.99,
            "book_ticker_quantity_exact_ratio": 0.8,
            "book_ticker_quantity_close_ratio": 0.9,
        },
        "cryptohft_dual_source": {"dual_source_available": False},
    }
    first = dict(base, day="2025-08-01")
    (quality_dir / "BTCUSDC-2025-08-01.json").write_text(json.dumps(first))
    third = dict(base, day="2026-01-01")
    third["cryptohft_dual_source"] = {
        "dual_source_available": True,
        "exchange_time_causal_asof": {
            "matched_ratio": 0.9,
            "top20_price_exact_ratio": 0.95,
        },
    }
    (quality_dir / "BTCUSDC-2026-01-01.json").write_text(json.dumps(third))
    output_json = tmp_path / "summary.json"
    output_csv = tmp_path / "days.csv"
    result = aggregate_quality(
        quality_dir=quality_dir,
        technical_days=technical,
        overlap_days=overlap,
        output_json=output_json,
        output_csv=output_csv,
    )
    assert result["groups"]["all"]["requested_days"] == 3
    assert result["groups"]["all"]["quality_days"] == 2
    assert result["groups"]["technical_target_2025"]["missing_quality_days"] == [
        "2025-08-02"
    ]
    assert result["groups"]["formal_overlap_2026"][
        "dual_source_available_days"
    ] == ["2026-01-01"]
    assert result["groups"]["all"]["metrics"]["freshness_union_coverage"][
        "p50"
    ] == 0.999
    assert output_json.is_file() and output_csv.is_file()
