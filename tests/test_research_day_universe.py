from __future__ import annotations

import csv
import json
import os
from datetime import date
from pathlib import Path

import pytest

from data.build_research_day_universe import build_universe
from features.feature_engineer import _read_days_file
from models import backtest_tick as bt
from models.data_windows import _enforce_book_source_contract


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _touch(path: Path, payload: bytes = b"x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _materialize_target_inputs(root: Path, day: str) -> None:
    paths = [
        root / "raw_trades" / "BTCUSDC" / f"BTCUSDC-trades-{day}.csv",
        root / "raw" / f"BTCUSDC-aggTrades-{day}.csv",
        root / "bars_1s" / f"BTCUSDC-1s-{day}.parquet",
        root / "bars_1s" / f"BTCUSDT-1s-{day}.parquet",
        root / "bars_1s_spot" / f"BTCUSDC-1s-{day}.parquet",
        root / "bars_1s_spot" / f"BTCUSDT-1s-{day}.parquet",
        root / "metrics_5m" / f"BTCUSDC-metrics-{day}.parquet",
        root / "metrics_5m" / f"BTCUSDT-metrics-{day}.parquet",
        root
        / "trade_features"
        / "BTCUSDC"
        / f"BTCUSDC-trade-tempo-{day}.parquet",
    ]
    for path in paths:
        _touch(path)


def test_provider_days_require_candidate_dminus1_and_publish_hardlinks(
    tmp_path: Path,
) -> None:
    provider_root = tmp_path / "provider"
    provider_quality = tmp_path / "provider_quality.csv"
    days = ["2025-08-01", "2025-08-02", "2025-08-03"]
    _write_csv(
        provider_quality,
        ["day", "provider_normalized_replay_candidate", "freshness_union_coverage"],
        [
            {
                "day": day,
                "provider_normalized_replay_candidate": "true",
                "freshness_union_coverage": "0.999",
            }
            for day in days
        ],
    )
    for day in days:
        _touch(provider_root / "bbo" / f"BTCUSDC-bbo-{day}.parquet", day.encode())
        _touch(provider_root / "l2" / f"BTCUSDC-l2-{day}.parquet", day.encode())

    technical = tmp_path / "technical.csv"
    _write_csv(
        technical,
        ["day", "source_id", "status"],
        [
            {"day": day, "source_id": source, "status": "valid"}
            for day in days
            for source in ("one", "two")
        ],
    )
    native_quality = tmp_path / "native.csv"
    _write_csv(
        native_quality,
        ["day", "quality_grade", "formal_training_replay_eligible"],
        [],
    )
    data = tmp_path / "data"
    for day in days:
        _materialize_target_inputs(data, day)

    output = tmp_path / "union"
    manifest = build_universe(
        start=date(2025, 8, 1),
        end=date(2025, 8, 3),
        provider_quality_csv=provider_quality,
        provider_root=provider_root,
        non_cryptohft_csv=technical,
        native_quality_csv=native_quality,
        native_root=tmp_path / "native",
        project_data_root=data,
        trade_feature_root=data / "trade_features",
        output_root=output,
    )
    assert manifest["day_count"] == 2
    assert manifest["source_counts"]["provider_normalized_causal"] == 2
    good = list(csv.DictReader((output / "good_days.csv").open()))
    assert [row["day"] for row in good] == ["2025-08-02", "2025-08-03"]
    rows = list(csv.DictReader((output / "research_day_universe.csv").open()))
    assert rows[0]["research_good_day"] == "false"
    assert rows[1]["causal_training_eligible"] == "true"
    assert rows[1]["exact_queue_policy_eligible"] == "false"
    linked = output / "l2" / "BTCUSDC-l2-2025-08-02.parquet"
    source = provider_root / "l2" / "BTCUSDC-l2-2025-08-02.parquet"
    assert os.stat(linked).st_ino == os.stat(source).st_ino
    assert json.loads((output / "manifest.json").read_text())[
        "permission_boundary"
    ]["exact_queue_policy_eligible"] is False


def test_feature_days_file_is_exact_and_provider_mode_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days_file = tmp_path / "days.csv"
    days_file.write_text(
        "day\n2025-08-03\n2025-08-02\n2025-08-02\n",
        encoding="utf-8",
    )
    assert _read_days_file(days_file) == ["2025-08-02", "2025-08-03"]

    root = tmp_path / "books"
    (root / "bbo").mkdir(parents=True)
    (root / "l2").mkdir(parents=True)
    (root / "manifest.json").write_text(
        json.dumps({"dataset_version": "normalized_l2_research_union_v1"}),
        encoding="utf-8",
    )
    _write_csv(
        root / "daily_quality.csv",
        [
            "day",
            "source_authority",
            "provider_sensitivity_replay_eligible",
            "exact_queue_policy_eligible",
        ],
        [
            {
                "day": "2025-08-02",
                "source_authority": "provider_normalized_causal",
                "provider_sensitivity_replay_eligible": "true",
                "exact_queue_policy_eligible": "false",
            }
        ],
    )
    monkeypatch.setattr(bt, "BBO_DIR", root / "bbo")
    monkeypatch.setattr(bt, "L2_DIR", root / "l2")
    with pytest.raises(SystemExit, match="provider-normalized book requires"):
        _enforce_book_source_contract(
            "2025-08-02", {"queue_ahead_mode": "exact_level"}
        )
    params = {"queue_ahead_mode": "provider_visible_level"}
    contract = _enforce_book_source_contract("2025-08-02", params)
    assert contract["provider_sensitivity_replay_eligible"] is True
    assert params["_book_exact_queue_policy_eligible"] is False
