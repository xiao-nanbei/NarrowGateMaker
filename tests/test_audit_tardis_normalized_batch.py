from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from data.audit_tardis_normalized_batch import audit_batch
from data.normalize_tardis_orderbook import DATASET_ID, SOURCE_ID


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _claim(path: Path) -> dict[str, object]:
    return {
        "path": str(path),
        "sha256": _sha256(path),
        "size_bytes": path.stat().st_size,
    }


def _fixture(tmp_path: Path) -> dict[str, Path]:
    day = "2025-08-01"
    days = tmp_path / "days.csv"
    days.write_text(f"day\n{day}\n", encoding="utf-8")
    raw_rows = []
    raw_paths: dict[str, Path] = {}
    for dataset in ("book_ticker", "incremental_book_L2"):
        path = tmp_path / f"{dataset}.csv.zst"
        path.write_bytes(dataset.encode())
        raw_paths[dataset] = path
        raw_rows.append(
            {
                "day": day,
                "dataset": dataset,
                "exists": True,
                "zstd_valid": True,
                "size_bytes": path.stat().st_size,
                "content_length": path.stat().st_size,
                "sha256": _sha256(path),
                "path": str(path),
            }
        )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps({"complete": True, "downloads": raw_rows}), encoding="utf-8"
    )
    output_root = tmp_path / "normalized"
    quality_dir = output_root / "quality"
    quality_dir.mkdir(parents=True)
    bbo = output_root / "bbo.parquet"
    l2 = output_root / "l2.parquet"
    clock = output_root / "clock.parquet"
    timestamp = 1_754_006_400_100
    pq.write_table(
        pa.table(
            {
                "timestamp": [timestamp],
                "best_bid": [100.0],
                "best_ask": [101.0],
            }
        ),
        bbo,
    )
    l2_values: dict[str, list[float | int]] = {"timestamp": [timestamp]}
    for level in range(1, 21):
        l2_values[f"bid_px_{level}"] = [100.0 - level / 10]
        l2_values[f"bid_qty_{level}"] = [1.0]
        l2_values[f"ask_px_{level}"] = [101.0 + level / 10]
        l2_values[f"ask_qty_{level}"] = [1.0]
    pq.write_table(pa.table(l2_values), l2)
    pq.write_table(
        pa.table(
            {
                "timestamp": [timestamp],
                "exchange_cut_timestamp_us": [timestamp * 1_000 - 2_000],
                "last_provider_local_timestamp_us": [timestamp * 1_000 - 1_000],
                "provider_visibility_delay_us": [1_000],
            }
        ),
        clock,
    )
    manifest_sha = _sha256(manifest)
    quality = {
        "day": day,
        "source_id": SOURCE_ID,
        "dataset_id": DATASET_ID,
        "clock_source": "tardis_provider_local",
        "clock_unit": "microseconds_since_unix_epoch_utc",
        "cadence_ms": 100,
        "levels": 20,
        "complete_day": True,
        "pilot_duration_s": None,
        "snapshot_seen_at_start": True,
        "causal_violations": 0,
        "local_clock_reversals": 0,
        "invalid_spread_buckets": 0,
        "freshness_union_coverage": 0.999,
        "output_p99_gap_ms": 100.0,
        "logical_message_gap": {
            "p99_upper_us": 100_000,
            "maximum_us": 200_000,
        },
        "book_ticker_audit": {
            "book_ticker_comparable_ratio": 1.0,
            "book_ticker_price_exact_ratio": 0.98,
            "book_ticker_price_within_one_tick_ratio": 0.99,
            "book_ticker_causal_violations": 0,
            "book_ticker_local_clock_reversals": 0,
        },
        "normalized_replay_candidate_before_cross_channel": True,
        "cross_channel_contract_valid": True,
        "provider_normalized_replay_candidate": True,
        "policy_visible": False,
        "exact_queue_policy_eligible": False,
        "emitted_rows": 1,
        "first_timestamp_ms": timestamp,
        "last_timestamp_ms": timestamp,
        "download_manifest": {"path": str(manifest), "sha256": manifest_sha},
        "raw_inputs": {
            dataset: {
                "path": str(path),
                "sha256": _sha256(path),
                "size_bytes": path.stat().st_size,
            }
            for dataset, path in raw_paths.items()
        },
        "bbo_output": _claim(bbo),
        "l2_output": _claim(l2),
        "clock_output": _claim(clock),
        "cryptohft_dual_source": {"dual_source_available": False},
    }
    quality_path = quality_dir / f"BTCUSDC-{day}.json"
    quality_path.write_text(json.dumps(quality), encoding="utf-8")
    normalizer = tmp_path / "normalize.py"
    normalizer.write_text("# frozen normalizer\n", encoding="utf-8")
    technical = tmp_path / "technical.csv"
    with technical.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["day", "cohort"])
        writer.writeheader()
        writer.writerow({"day": day, "cohort": "technical"})
    freeze = tmp_path / "freeze.json"
    freeze.write_text(
        json.dumps(
            {
                "technical_target": {
                    "path": str(technical),
                    "sha256": _sha256(technical),
                }
            }
        ),
        encoding="utf-8",
    )
    return {
        "manifest": manifest,
        "days": days,
        "quality_dir": quality_dir,
        "normalizer": normalizer,
        "freeze": freeze,
        "quality": quality_path,
    }


def test_post_batch_audit_rehashes_outputs_and_recomputes_gate(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    result = audit_batch(
        raw_manifest=paths["manifest"],
        days_file=paths["days"],
        quality_dir=paths["quality_dir"],
        normalizer_code=paths["normalizer"],
        expected_normalizer_sha256=_sha256(paths["normalizer"]),
        expected_manifest_sha256=_sha256(paths["manifest"]),
        governance_freeze=paths["freeze"],
        rehash_raw=True,
    )
    assert result["batch_integrity_valid"] is True
    assert result["valid_days"] == 1
    assert result["provider_candidate_days"] == ["2025-08-01"]
    assert result["daily"][0]["raw_inputs"]["book_ticker"]["actual_sha256"]


def test_post_batch_audit_rejects_inconsistent_derived_gate(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    quality = json.loads(paths["quality"].read_text(encoding="utf-8"))
    quality["provider_normalized_replay_candidate"] = False
    paths["quality"].write_text(json.dumps(quality), encoding="utf-8")
    result = audit_batch(
        raw_manifest=paths["manifest"],
        days_file=paths["days"],
        quality_dir=paths["quality_dir"],
        normalizer_code=paths["normalizer"],
    )
    assert result["batch_integrity_valid"] is False
    assert result["invalid_days"] == ["2025-08-01"]
    assert (
        "derived_gate_mismatch:provider_normalized_replay_candidate"
        in result["daily"][0]["errors"]
    )


def test_post_batch_audit_allows_explicit_manifest_superset(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    extra = dict(manifest["downloads"][0])
    extra["day"] = "2025-08-02"
    manifest["downloads"].append(extra)
    paths["manifest"].write_text(json.dumps(manifest), encoding="utf-8")
    quality = json.loads(paths["quality"].read_text(encoding="utf-8"))
    quality["download_manifest"]["sha256"] = _sha256(paths["manifest"])
    paths["quality"].write_text(json.dumps(quality), encoding="utf-8")

    result = audit_batch(
        raw_manifest=paths["manifest"],
        days_file=paths["days"],
        quality_dir=paths["quality_dir"],
        normalizer_code=paths["normalizer"],
        allow_manifest_superset=True,
    )

    assert result["batch_integrity_valid"] is True
    assert result["raw_manifest_superset_allowed"] is True
    assert result["raw_manifest_unrequested_identity_count"] == 1
