from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_daily_sources as daily,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_feature_generator as base,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_panel_materializer as materializer,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema

DAY = "2026-05-27"
PREVIOUS_DAY = "2026-05-26"
DAY_START_MS = int(pd.Timestamp(DAY, tz="UTC").timestamp() * 1_000)


def _day_start_ms(day: str) -> int:
    return int(pd.Timestamp(day, tz="UTC").timestamp() * 1_000)


def _local_frame(day: str, *, first_offset_s: int = 0, step_s: int = 10) -> pd.DataFrame:
    day_start = _day_start_ms(day)
    offsets = np.arange(first_offset_s, 86_400, step_s, dtype=np.int64)
    close = 60_000.0 + (offsets % 1_000) * 0.001
    return pd.DataFrame(
        {
            "timestamp": day_start + offsets * 1_000,
            "open": close - 0.05,
            "high": close + 0.1,
            "low": close - 0.1,
            "close": close,
            "volume": np.ones(len(offsets)),
            "buy_qty": np.full(len(offsets), 0.55),
            "sell_qty": np.full(len(offsets), 0.45),
            "trade_count": np.full(len(offsets), 10),
            "buy_trade_count": np.full(len(offsets), 6),
            "sell_trade_count": np.full(len(offsets), 4),
            "buy_quote_qty": close * 0.55,
            "sell_quote_qty": close * 0.45,
            "max_same_side_run": np.full(len(offsets), 3),
            "buy_price_high": close + 0.05,
            "buy_price_low": close - 0.05,
            "sell_price_high": close + 0.05,
            "sell_price_low": close - 0.05,
        }
    )


def _write_local_day(
    path: Path,
    day: str,
    *,
    first_offset_s: int = 0,
    timestamp_as_index: bool = True,
) -> None:
    frame = _local_frame(day, first_offset_s=first_offset_s)
    if timestamp_as_index:
        frame.set_index("timestamp", inplace=True)
    frame.to_parquet(path, index=timestamp_as_index)


def _write_local_window(
    path: Path,
    *,
    removed: set[int] | None = None,
    timestamp_as_index: bool = True,
) -> None:
    removed = removed or set()
    start = DAY_START_MS + 86_400_000 - 401_000
    rows = []
    for index in range(401):
        if index in removed:
            continue
        close = 60_000.0 + 0.1 * index
        rows.append(
            {
                "timestamp": start + index * 1_000,
                "open": close - 0.05,
                "high": close + 0.1,
                "low": close - 0.1,
                "close": close,
                "volume": 1.0,
                "buy_qty": 0.55,
                "sell_qty": 0.45,
                "trade_count": 10,
                "buy_trade_count": 6,
                "sell_trade_count": 4,
                "buy_quote_qty": close * 0.55,
                "sell_quote_qty": close * 0.45,
                "max_same_side_run": 3,
                "buy_price_high": close + 0.05,
                "buy_price_low": close - 0.05,
                "sell_price_high": close + 0.05,
                "sell_price_low": close - 0.05,
            }
        )
    frame = pd.DataFrame(rows)
    if timestamp_as_index:
        frame.set_index("timestamp", inplace=True)
    frame.to_parquet(path, index=timestamp_as_index)


def _write_reference_day(path: Path, day: str, *, first_offset_s: int = 0) -> None:
    day_start = _day_start_ms(day)
    offsets = np.arange(first_offset_s, 86_400, 10, dtype=np.int64)
    close = 60_050.0 + (offsets % 1_000) * 0.0008
    frame = pd.DataFrame(
        {
            "timestamp": day_start + offsets * 1_000,
            "open": close - 0.05,
            "high": close + 0.1,
            "low": close - 0.1,
            "close": close,
            "volume": np.full(len(offsets), 2.0),
            "buy_volume": np.full(len(offsets), 1.1),
            "sell_volume": np.full(len(offsets), 0.9),
            "trade_count": np.full(len(offsets), 15),
            "buy_count": np.full(len(offsets), 8),
            "sell_count": np.full(len(offsets), 7),
        }
    )
    frame.set_index("timestamp", inplace=True)
    frame.to_parquet(path, index=True)


def _write_metrics(
    path: Path,
    day: str,
    *,
    timestamp_semantics: str,
    delayed_row: tuple[int, int] | None = None,
) -> None:
    day_start = pd.Timestamp(day, tz="UTC")
    if timestamp_semantics == "start":
        times = pd.date_range(day_start, periods=288, freq="5min")
    elif timestamp_semantics == "end":
        times = pd.date_range(day_start + pd.Timedelta(minutes=5), periods=288, freq="5min")
    else:
        raise AssertionError(timestamp_semantics)
    if delayed_row is not None:
        row, delay_ms = delayed_row
        times = times.to_series(index=np.arange(len(times)))
        times.iloc[row] += pd.Timedelta(milliseconds=delay_ms)
        times = pd.DatetimeIndex(times)
    index = np.arange(288, dtype=np.float64)
    pd.DataFrame(
        {
            "create_time": times.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": "BTCUSDC",
            "sum_open_interest": 10_000.0 + index,
            "sum_open_interest_value": 600_000_000.0 + index,
            "count_toptrader_long_short_ratio": 1.1 + index * 0.0001,
            "sum_toptrader_long_short_ratio": 1.2 + index * 0.0001,
            "count_long_short_ratio": 0.9 + index * 0.0001,
            "sum_taker_long_short_vol_ratio": 1.0 + index * 0.0001,
        }
    ).to_csv(path, index=False)


def _write_l2(path: Path, day: str, *, target_window: bool) -> None:
    start = _day_start_ms(day)
    if target_window:
        timestamps = range(start + 397_000, start + 401_000, 100)
    else:
        timestamps = range(start + 86_399_000, start + 86_400_000, 100)
    rows = []
    for timestamp in timestamps:
        tick = (timestamp // 100) % 5
        row: dict[str, float | int] = {"timestamp": timestamp}
        for level in range(1, 21):
            row[f"bid_px_{level}"] = 60_040.0 - level * 0.1
            row[f"ask_px_{level}"] = 60_040.0 + level * 0.1
            row[f"bid_qty_{level}"] = 1.0 + level * 0.01 + tick * 0.001
            row[f"ask_qty_{level}"] = 0.9 + level * 0.01
        rows.append(row)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_local_manifest(path: Path, local_paths: tuple[Path, ...]) -> None:
    daily_files = []
    for local_path in local_paths:
        day = daily._path_day(local_path)
        assert day is not None
        daily_files.append(
            {
                "day": day,
                "sidecar_file": str(local_path),
                "sidecar_sha256": daily.sha256_file(local_path),
                "sidecar_size_bytes": local_path.stat().st_size,
                "sidecar_rows": pq.ParquetFile(local_path).metadata.num_rows,
            }
        )
    path.write_text(
        json.dumps(
            {
                "schema": daily.LOCAL_MANIFEST_SCHEMA,
                "symbol": "BTCUSDC",
                "daily_file_count": len(daily_files),
                "daily_files": daily_files,
                "daily_manifest_sha256": "fixture-daily-identity",
            }
        ),
        encoding="utf-8",
    )


def _write_reference_meta(path: Path, bar_path: Path, day: str) -> None:
    path.write_text(
        json.dumps(
            {
                "bar_interval": "[t,t+1s)",
                "causal_visible_at": "t+1s",
                "complete": True,
                "output_sha256": daily.sha256_file(bar_path),
                "rows": pq.ParquetFile(bar_path).metadata.num_rows,
                "schema_version": daily.REFERENCE_MANIFEST_SCHEMA,
                "source_data_type": "aggTrades",
                "symbol": "BTCUSDT",
                "utc_day": day,
            }
        ),
        encoding="utf-8",
    )


def _write_l2_quality(path: Path, l2_path: Path, day: str) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": daily.L2_QUALITY_SCHEMA,
                "dataset_id": "synthetic_fixture_l2_100ms_v1",
                "symbol": "BTCUSDC",
                "day": day,
                "clock_source": "synthetic_fixture_visibility_ms",
                "complete_day": True,
                "cadence_ms": 100,
                "levels": 20,
                "causal_violations": 0,
                "observed_internal_gap_valid": True,
                "cross_channel_contract_valid": True,
                "provider_normalized_replay_candidate": True,
                "live_transport_eligible": False,
                "l2_output": {
                    "path": str(l2_path),
                    "sha256": daily.sha256_file(l2_path),
                    "size_bytes": l2_path.stat().st_size,
                },
            }
        ),
        encoding="utf-8",
    )


@pytest.fixture(scope="module")
def real_layout_bundle(tmp_path_factory: pytest.TempPathFactory) -> daily.DailySourceBundle:
    root = tmp_path_factory.mktemp("causal-v12-1s-real-layout")
    local_paths = tuple(root / f"BTCUSDC-trade-tempo-{day}.parquet" for day in (PREVIOUS_DAY, DAY))
    reference_paths = tuple(root / f"BTCUSDT-1s-{day}.parquet" for day in (PREVIOUS_DAY, DAY))
    metric_paths = tuple(root / f"BTCUSDC-metrics-{day}.csv" for day in (PREVIOUS_DAY, DAY))
    l2_paths = tuple(root / f"BTCUSDC-l2-{day}.parquet" for day in (PREVIOUS_DAY, DAY))
    quality_paths = tuple(root / f"BTCUSDC-{day}.json" for day in (PREVIOUS_DAY, DAY))
    reference_meta_paths = tuple(
        root / f"BTCUSDT-1s-{day}.parquet.meta.json" for day in (PREVIOUS_DAY, DAY)
    )
    local_manifest = root / "tempo-manifest.json"

    _write_local_day(local_paths[0], PREVIOUS_DAY)
    _write_local_day(local_paths[1], DAY, first_offset_s=1)
    _write_reference_day(reference_paths[0], PREVIOUS_DAY)
    _write_reference_day(reference_paths[1], DAY, first_offset_s=1)
    _write_metrics(metric_paths[0], PREVIOUS_DAY, timestamp_semantics="end")
    _write_metrics(metric_paths[1], DAY, timestamp_semantics="start")
    _write_l2(l2_paths[0], PREVIOUS_DAY, target_window=False)
    _write_l2(l2_paths[1], DAY, target_window=True)
    _write_local_manifest(local_manifest, local_paths)
    for path, l2_path, day in zip(quality_paths, l2_paths, (PREVIOUS_DAY, DAY), strict=True):
        _write_l2_quality(path, l2_path, day)
    for path, bar_path, day in zip(
        reference_meta_paths, reference_paths, (PREVIOUS_DAY, DAY), strict=True
    ):
        _write_reference_meta(path, bar_path, day)

    return daily.DailySourceBundle(
        utc_day=DAY,
        local_trade_tempo_paths=local_paths,
        local_source_manifest_paths=(local_manifest,),
        execution_l2_paths=l2_paths,
        execution_l2_quality_paths=quality_paths,
        metric_paths=metric_paths,
        reference_bar_paths=reference_paths,
        reference_bar_manifest_paths=reference_meta_paths,
        execution_l2_clock_identity="synthetic_fixture_visibility_ms",
    )


def test_daily_source_reader_maps_real_physical_schemas(
    real_layout_bundle: daily.DailySourceBundle,
) -> None:
    local = daily.read_local_trade_bars_with_audit(real_layout_bundle.local_trade_tempo_paths)
    reference = daily.read_reference_bars_with_audit(real_layout_bundle.reference_bar_paths)
    metrics = daily.read_metrics_with_audit(real_layout_bundle.metric_paths)
    l2 = daily.read_execution_l2(real_layout_bundle.execution_l2_paths)
    probe = daily.probe_source_bundle(real_layout_bundle)

    assert len(local.bars) == 172_800
    assert local.synthesized_seconds > 0
    assert DAY_START_MS in local.synthesized_start_ts_ms
    assert reference is not None and len(reference.bars) == 172_800
    assert len(metrics.observations) == 576
    assert [item.input_timestamp_semantics for item in metrics.files] == [
        "interval_end_already_causal_ready",
        "interval_start_shifted_to_causal_end",
    ]
    assert len(l2) == 5
    assert tuple(l2[-1].values) == schema.EXECUTION_L2_FEATURES
    assert probe["physical_materialization_eligible"] is True
    assert probe["failure_reasons"] == []
    assert probe["ten_second_feature_rows_accepted"] is False
    assert all(row["schema_supported"] for row in probe["files"])
    l2_rows = [row for row in probe["files"] if row["group"] == "execution_l2"]
    assert {row["available_depth_levels"] for row in l2_rows} == {20}


def test_reader_keeps_timestamp_column_from_real_pandas_index_metadata(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDC-trade-tempo-2026-05-27.parquet"
    _write_local_window(path, timestamp_as_index=True)
    metadata = pq.ParquetFile(path).metadata.metadata
    assert metadata is not None and b"pandas" in metadata
    pandas_metadata = json.loads(metadata[b"pandas"])
    assert pandas_metadata["index_columns"] == ["timestamp"]

    bars = daily.read_local_trade_bars((path,))

    assert len(bars) == 401
    assert bars[0].start_ts_ms == DAY_START_MS + 86_400_000 - 401_000


def test_metrics_csv_normalizes_start_and_end_stamps_to_causal_ready_time(
    real_layout_bundle: daily.DailySourceBundle,
) -> None:
    audit = daily.read_metrics_with_audit(real_layout_bundle.metric_paths)
    target_first = next(item for item in audit.observations if item.source_ts_ms > DAY_START_MS)

    assert target_first.source_ts_ms == DAY_START_MS + 300_000
    assert target_first.feature_ready_ts_ms == DAY_START_MS + 300_000


def test_metrics_csv_preserves_bounded_post_boundary_source_ready_delay(
    tmp_path: Path,
) -> None:
    path = tmp_path / f"BTCUSDC-metrics-{DAY}.csv"
    _write_metrics(path, DAY, timestamp_semantics="end", delayed_row=(255, 2_000))

    audit = daily.read_metrics_with_audit((path,))

    assert audit.files[0].input_timestamp_semantics == (
        "interval_end_with_bounded_source_ready_delay"
    )
    assert audit.files[0].maximum_source_ready_delay_ms == 2_000
    assert audit.observations[255].feature_ready_ts_ms == (
        DAY_START_MS + 256 * 300_000 + 2_000
    )


def test_metrics_csv_rejects_source_ready_delay_beyond_frozen_limit(
    tmp_path: Path,
) -> None:
    path = tmp_path / f"BTCUSDC-metrics-{DAY}.csv"
    _write_metrics(path, DAY, timestamp_semantics="end", delayed_row=(255, 3_000))

    with pytest.raises(base.FeatureContractError, match="bounded source-ready delays"):
        daily.read_metrics_with_audit((path,))


def test_metrics_csv_uses_unique_source_clock_not_physical_row_order(
    tmp_path: Path,
) -> None:
    path = tmp_path / f"BTCUSDC-metrics-{DAY}.csv"
    _write_metrics(path, DAY, timestamp_semantics="start")
    frame = pd.read_csv(path)
    frame = frame.sample(frac=1.0, random_state=17).reset_index(drop=True)
    frame.to_csv(path, index=False)

    audit = daily.read_metrics_with_audit((path,))

    assert audit.files[0].input_rows_reordered is True
    assert audit.files[0].input_clock_inversions > 0
    assert audit.files[0].input_timestamp_semantics == (
        "interval_start_shifted_to_causal_end"
    )
    assert audit.observations[0].feature_ready_ts_ms == DAY_START_MS + 300_000
    assert audit.observations[-1].feature_ready_ts_ms == DAY_START_MS + 86_400_000


def test_metrics_csv_rejects_duplicate_timestamp_even_with_288_rows(
    tmp_path: Path,
) -> None:
    path = tmp_path / f"BTCUSDC-metrics-{DAY}.csv"
    _write_metrics(path, DAY, timestamp_semantics="end")
    frame = pd.read_csv(path)
    frame.loc[7, "create_time"] = frame.loc[6, "create_time"]
    frame.to_csv(path, index=False)

    with pytest.raises(base.FeatureContractError, match="duplicate metrics timestamps"):
        daily.read_metrics_with_audit((path,))


def test_metrics_reader_rejects_non_csv_authority(tmp_path: Path) -> None:
    path = tmp_path / "BTCUSDC-metrics-2026-05-27.parquet"
    pd.DataFrame({"create_time": [DAY]}).to_parquet(path, index=False)

    with pytest.raises(base.FeatureContractError, match="must be raw CSV"):
        daily.read_metrics((path,))


def test_local_source_gap_is_synthesized_with_explicit_lag_state(tmp_path: Path) -> None:
    path = tmp_path / "BTCUSDC-trade-tempo-2026-05-27.parquet"
    _write_local_window(path, removed={200})

    audit = daily.read_local_trade_bars_with_audit((path,))
    synthetic = audit.bars[200]

    assert audit.synthesized_seconds == 1
    assert audit.maximum_missing_run_seconds == 1
    assert synthetic.trade_count == 0
    assert synthetic.volume == 0.0
    assert synthetic.open == synthetic.high == synthetic.low == synthetic.close
    assert synthetic.close == audit.bars[199].close


def test_local_source_gap_above_frozen_support_fails(tmp_path: Path) -> None:
    path = tmp_path / "BTCUSDC-trade-tempo-2026-05-27.parquet"
    _write_local_window(path, removed=set(range(180, 211)))

    with pytest.raises(base.FeatureContractError, match="exceeds frozen 30s support"):
        daily.read_local_trade_bars_with_audit((path,))


def test_materializer_publishes_atomic_hash_bound_panel(
    real_layout_bundle: daily.DailySourceBundle,
) -> None:
    output_dir = real_layout_bundle.local_trade_tempo_paths[0].parent / "published" / f"day={DAY}"
    cutoffs = (DAY_START_MS, DAY_START_MS + 399_000)

    result = materializer.materialize_daily_panel(
        real_layout_bundle,
        output_dir=output_dir,
        cutoffs_ms=cutoffs,
        batch_rows=1,
        engine=materializer.PYTHON_ORACLE_ENGINE,
    )

    assert result.reused is False
    assert result.row_count == 2
    assert (output_dir / materializer.SUCCESS_FILENAME).is_file()
    assert not list(output_dir.parent.glob(f".{output_dir.name}.tmp-*"))
    arrow_schema = pq.ParquetFile(result.panel_path).schema_arrow
    assert arrow_schema.names[9:] == list(schema.TRAINABLE_FEATURE_ORDER)
    assert not any(name.startswith("label_") for name in arrow_schema.names)
    table = pq.read_table(result.panel_path)
    assert table.column("l2_spread_bps").null_count == 0
    assert table.column("cv_ref_perp_available").to_pylist() == [1.0, 1.0]
    assert table.column("local_synthetic_seconds_24h").to_pylist()[0] > 0

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["cache_identity_sha256"] == result.cache_identity_sha256
    assert manifest["panel"]["sha256"] == daily.sha256_file(result.panel_path)
    assert manifest["source_runtime_audit"]["local_trade_tempo"]["synthesized_seconds"] > 0
    assert manifest["target_day_decision_clock"] == {
        "interval": "[D 00:00:00, D+1 00:00:00)",
        "bar_support_rule": "cutoff_minus_1s_completed_local_bar",
        "first_decision_uses_previous_natural_day_warmup": True,
        "next_day_midnight_included": False,
    }
    assert manifest["panel"]["first_cutoff_exclusive_ms"] == DAY_START_MS
    assert manifest["ten_second_feature_rows_accepted"] is False
    assert manifest["training_authorized"] is False
    assert manifest["live_authorized"] is False

    reused = materializer.materialize_daily_panel(
        real_layout_bundle,
        output_dir=output_dir,
        cutoffs_ms=cutoffs,
        batch_rows=2,
        engine=materializer.PYTHON_ORACLE_ENGINE,
    )
    assert reused.reused is True
    assert reused.cache_identity_sha256 == result.cache_identity_sha256


def test_materializer_requires_an_explicit_engine(
    real_layout_bundle: daily.DailySourceBundle, tmp_path: Path
) -> None:
    with pytest.raises(base.FeatureContractError, match="engine must be explicit"):
        materializer.materialize_daily_panel(
            real_layout_bundle,
            output_dir=tmp_path / "missing-engine",
            cutoffs_ms=(DAY_START_MS,),
        )


def test_cpp_batch_materializer_writes_engine_bound_native_rows(
    real_layout_bundle: daily.DailySourceBundle,
) -> None:
    output_dir = (
        real_layout_bundle.local_trade_tempo_paths[0].parent
        / "published-cpp"
        / f"day={DAY}"
    )
    cutoffs = (DAY_START_MS, DAY_START_MS + 399_000)

    result = materializer.materialize_daily_panel(
        real_layout_bundle,
        output_dir=output_dir,
        cutoffs_ms=cutoffs,
        batch_rows=2,
        engine=materializer.CPP_BATCH_ENGINE,
    )

    table = pq.read_table(result.panel_path)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert result.row_count == 2
    assert manifest["engine"]["engine"] == materializer.CPP_BATCH_ENGINE
    assert manifest["engine"]["raw_inputs_only"] is True
    assert manifest["bulk_materialization_authorized"] is True
    assert table.column("cutoff_exclusive_ms").to_pylist() == list(cutoffs)
    assert table.column("close").null_count == 0
    assert table.column("feature_row_fingerprint_sha256").null_count == 0


def test_authoritative_target_day_cutoffs_use_previous_day_bar_and_exclude_next_midnight(
    real_layout_bundle: daily.DailySourceBundle,
) -> None:
    local = daily.read_local_trade_bars(real_layout_bundle.local_trade_tempo_paths)

    cutoffs = materializer._target_cutoffs(real_layout_bundle, local, None)

    assert len(cutoffs) == 86_400
    assert cutoffs[0] == DAY_START_MS
    assert cutoffs[-1] == DAY_START_MS + 86_399_000
    assert DAY_START_MS + 86_400_000 not in cutoffs
    completed_starts = {bar.start_ts_ms for bar in local}
    assert cutoffs[0] - 1_000 in completed_starts


def test_explicit_next_day_midnight_cutoff_is_rejected(
    real_layout_bundle: daily.DailySourceBundle,
) -> None:
    local = daily.read_local_trade_bars(real_layout_bundle.local_trade_tempo_paths)

    with pytest.raises(ValueError, match=r"target-day decision interval \[D,D\+1\)"):
        materializer._target_cutoffs(
            real_layout_bundle,
            local,
            (DAY_START_MS + 86_400_000,),
        )


def test_probe_and_materializer_fail_closed_without_d_minus_one_reference(
    real_layout_bundle: daily.DailySourceBundle,
    tmp_path: Path,
) -> None:
    incomplete = replace(
        real_layout_bundle,
        reference_bar_paths=(real_layout_bundle.reference_bar_paths[1],),
        reference_bar_manifest_paths=(real_layout_bundle.reference_bar_manifest_paths[1],),
    )
    probe = daily.probe_source_bundle(incomplete)

    assert probe["physical_materialization_eligible"] is False
    assert probe["path_day_coverage"]["reference_bars"]["missing_days"] == [PREVIOUS_DAY]
    with pytest.raises(base.FeatureContractError, match="not physically materialization-eligible"):
        materializer.materialize_daily_panel(
            incomplete,
            output_dir=tmp_path / "not-admitted",
            cutoffs_ms=(DAY_START_MS + 400_000,),
            engine=materializer.PYTHON_ORACLE_ENGINE,
        )
    assert not (tmp_path / "not-admitted").exists()


def test_l2_quality_json_must_hash_bind_the_actual_parquet(
    real_layout_bundle: daily.DailySourceBundle,
    tmp_path: Path,
) -> None:
    original = real_layout_bundle.execution_l2_quality_paths[1]
    payload = json.loads(original.read_text(encoding="utf-8"))
    payload["l2_output"]["sha256"] = "0" * 64
    tampered = tmp_path / f"BTCUSDC-{DAY}.json"
    tampered.write_text(json.dumps(payload), encoding="utf-8")
    bundle = replace(
        real_layout_bundle,
        execution_l2_quality_paths=(
            real_layout_bundle.execution_l2_quality_paths[0],
            tampered,
        ),
    )

    probe = daily.probe_source_bundle(bundle)

    assert probe["physical_materialization_eligible"] is False
    assert any(
        "SHA256 mismatch" in error for error in probe["execution_l2_quality_authority"]["errors"]
    )


def test_existing_feature_panel_is_rejected_as_local_source(tmp_path: Path) -> None:
    path = tmp_path / "old-10s-feature-panel-2026-05-27.parquet"
    payload = {name: [1.0] for name in schema.TRAINABLE_FEATURE_ORDER}
    payload.update(
        {
            "timestamp": [DAY_START_MS + 86_399_000],
            "buy_qty": [1.0],
            "sell_qty": [1.0],
            "buy_trade_count": [1.0],
            "sell_trade_count": [1.0],
            "max_same_side_run": [1.0],
            "buy_price_high": [60_000.0],
            "buy_price_low": [60_000.0],
            "sell_price_high": [60_000.0],
            "sell_price_low": [60_000.0],
            "buy_quote_qty": [60_000.0],
            "sell_quote_qty": [60_000.0],
        }
    )
    pd.DataFrame(payload).to_parquet(path, index=False)

    with pytest.raises(base.FeatureContractError, match="feature panel"):
        daily.read_local_trade_bars((path,))


def test_source_spec_rejects_unknown_fields(
    real_layout_bundle: daily.DailySourceBundle, tmp_path: Path
) -> None:
    spec_path = tmp_path / "source.json"
    spec = {
        "utc_day": DAY,
        "local_trade_tempo_paths": [str(real_layout_bundle.local_trade_tempo_paths[1])],
        "execution_l2_clock_identity": "synthetic_fixture_visibility_ms",
        "unexpected": True,
    }
    spec_path.write_text(json.dumps(spec), encoding="utf-8")

    with pytest.raises(base.FeatureContractError, match="unknown daily source"):
        daily.DailySourceBundle.from_json(spec_path)
