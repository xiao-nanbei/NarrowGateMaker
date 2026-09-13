from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_individual_reference_repair as repair,
)


def _write_raw(root: Path, day: str, rows: list[str] | None = None) -> Path:
    raw_root = root / repair.RAW_RELATIVE_ROOT
    raw_root.mkdir(parents=True, exist_ok=True)
    start_ms, _ = repair._day_bounds_ms(day)
    source = raw_root / f"BTCUSDT-trades-{day}.csv"
    source.write_text(
        repair.RAW_HEADER
        + "\n"
        + "\n".join(
            rows
            or [
                f"1,100.0,0.10,10.0,{start_ms + 100},false",
                f"2,101.0,0.20,20.2,{start_ms + 900},true",
                f"3,102.0,0.30,30.6,{start_ms + 1_100},false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return source


def test_materialize_day_binds_raw_clock_rows_hash_and_atomic_marker(tmp_path: Path) -> None:
    day = repair.REPAIR_DAYS[0]
    raw = _write_raw(tmp_path, day)

    result = repair.materialize_day(market_data_root=tmp_path, day=day)

    assert result["status"] == "admitted"
    output = tmp_path / repair.OUTPUT_RELATIVE_ROOT / f"BTCUSDT-1s-{day}.parquet"
    sidecar = output.with_suffix(output.suffix + ".meta.json")
    frame = pd.read_parquet(output)
    metadata = json.loads(sidecar.read_text(encoding="utf-8"))
    assert list(frame.columns) == list(repair.OUTPUT_COLUMNS)
    assert frame["trade_count"].sum() == 3
    assert metadata["schema_version"] == repair.BAR_SCHEMA_VERSION
    assert metadata["admission_schema_version"] == repair.SCHEMA_VERSION
    assert metadata["atomic_admission"] is True
    assert metadata["source_data_type"] == "trades"
    assert metadata["source_authority"] == repair.SOURCE_AUTHORITY
    assert metadata["source_clock"] == repair.SOURCE_CLOCK
    assert metadata["source_sha256"] == repair.sha256_file(raw)
    assert metadata["output_sha256"] == repair.sha256_file(output)
    assert metadata["source_rows"] == 3
    assert metadata["rows"] == 2
    assert metadata["alternate_aggtrade_artifact_used"] is False
    assert not list((tmp_path / repair.OUTPUT_RELATIVE_ROOT).glob("*.partial-*"))


def test_materialize_day_reuses_only_exact_admitted_pair(tmp_path: Path) -> None:
    day = repair.REPAIR_DAYS[0]
    _write_raw(tmp_path, day)
    repair.materialize_day(market_data_root=tmp_path, day=day)

    result = repair.materialize_day(market_data_root=tmp_path, day=day)

    assert result["status"] == "reused"


def test_rebuild_requires_and_replaces_an_exact_admitted_pair(tmp_path: Path) -> None:
    day = repair.REPAIR_DAYS[0]
    _write_raw(tmp_path, day)
    first = repair.materialize_day(market_data_root=tmp_path, day=day)

    rebuilt = repair.materialize_day(
        market_data_root=tmp_path,
        day=day,
        rebuild_admitted=True,
    )

    assert first["metadata"]["source_sha256"] == rebuilt["metadata"]["source_sha256"]
    assert first["metadata"]["output_sha256"] == rebuilt["metadata"]["output_sha256"]
    assert rebuilt["status"] == "admitted"


def test_existing_parquet_without_sidecar_fails_closed(tmp_path: Path) -> None:
    day = repair.REPAIR_DAYS[0]
    _write_raw(tmp_path, day)
    output_root = tmp_path / repair.OUTPUT_RELATIVE_ROOT
    output_root.mkdir(parents=True)
    (output_root / f"BTCUSDT-1s-{day}.parquet").write_bytes(b"orphan")

    with pytest.raises(repair.ReferenceRepairError, match="pair is incomplete"):
        repair.materialize_day(market_data_root=tmp_path, day=day)


def test_rejects_aggtrade_header_in_exact_raw_root(tmp_path: Path) -> None:
    day = repair.REPAIR_DAYS[0]
    raw_root = tmp_path / repair.RAW_RELATIVE_ROOT
    raw_root.mkdir(parents=True)
    (raw_root / f"BTCUSDT-trades-{day}.csv").write_text(
        "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n",
        encoding="utf-8",
    )

    with pytest.raises(repair.ReferenceRepairError, match="header mismatch"):
        repair.materialize_day(market_data_root=tmp_path, day=day)


def test_rejects_out_of_day_or_nonmonotonic_raw_authority(tmp_path: Path) -> None:
    day = repair.REPAIR_DAYS[0]
    start_ms, _ = repair._day_bounds_ms(day)
    _write_raw(
        tmp_path,
        day,
        rows=[
            f"2,100.0,0.1,10.0,{start_ms + 200},false",
            f"1,100.1,0.1,10.01,{start_ms + 100},true",
        ],
    )

    with pytest.raises(repair.ReferenceRepairError, match="trade id is not strictly increasing"):
        repair.materialize_day(market_data_root=tmp_path, day=day)


def test_batch_manifest_is_reference_only_and_outcome_blind(tmp_path: Path) -> None:
    results = []
    for day in repair.REPAIR_DAYS:
        _write_raw(tmp_path, day)
        results.append(repair.materialize_day(market_data_root=tmp_path, day=day))

    manifest = repair.build_batch_manifest(market_data_root=tmp_path, results=results)

    assert manifest["repair_days"] == list(repair.REPAIR_DAYS)
    assert len(manifest["artifacts"]) == 7
    assert manifest["alternate_aggtrade_artifacts_used"] is False
    assert manifest["metrics_repair_included"] is False
    assert manifest["frozen_panels_modified"] is False
    assert manifest["predictions_read"] is False
    assert manifest["economic_outcomes_read"] is False
    assert manifest["permissions"]["transport_scoring_authorized"] is False


def test_run_repair_refuses_any_changed_day_denominator(tmp_path: Path) -> None:
    with pytest.raises(repair.ReferenceRepairError, match="frozen seven-day contract"):
        repair.run_repair(market_data_root=tmp_path, days=repair.REPAIR_DAYS[:-1])


def test_plan_derives_only_exact_raw_and_reference_paths(tmp_path: Path) -> None:
    day = repair.REPAIR_DAYS[0]
    _write_raw(tmp_path, day)
    plan = repair.build_plan(market_data_root=tmp_path)

    assert plan["alternate_aggtrade_artifacts_used"] is False
    assert plan["economic_outcomes_read"] is False
    assert len(plan["days"]) == 7
    assert plan["days"][0]["raw_path"].endswith(f"raw_trades/BTCUSDT/BTCUSDT-trades-{day}.csv")
    assert plan["days"][0]["parquet_path"].endswith(
        f"reference_bars_1s_trades_v1/BTCUSDT-1s-{day}.parquet"
    )
