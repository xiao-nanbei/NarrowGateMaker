from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data import unify_daily_fields as module

DAY = "2026-01-01"
START = 1_767_225_600_000


def _feature(path: Path, unit="ms"):
    frame = pd.DataFrame({"a": [1.0, np.nan], "b": [4, 5]},
                         index=pd.date_range(DAY, periods=2, freq="10s", tz="UTC").as_unit(unit))
    frame.attrs["causal_contract"] = {"labels": False}
    frame.to_parquet(path)
    return frame


def _bars(path: Path, seconds=(0, 1)):
    frame = pd.DataFrame({
        name: ([1] * len(seconds) if name.endswith("count") else [2.0] * len(seconds))
        for name in module.BAR_COLUMNS if name not in ("timestamp", "last_event_ts_ms")
    }, index=pd.Index([START + s * 1000 for s in seconds], name="timestamp"))
    frame.to_parquet(path)
    return frame


def _trades(path: Path, times=(100, 950, 1200), *, symbol="BTCUSDT", alias_delta=0):
    path.parent.mkdir(parents=True, exist_ok=True)
    timestamps = [START + t for t in times]
    pq.write_table(pa.table({"timestamp": pa.array([t * 1000 for t in timestamps], type=pa.int64()),
        "time": pa.array([t + alias_delta for t in timestamps], type=pa.int64()),
        "id": pa.array(range(len(timestamps)), type=pa.int64()),
        "symbol": pa.array([symbol] * len(timestamps), type=pa.string()),
        "exchange": pa.array(["binance_futures"] * len(timestamps), type=pa.string()),
        "price": ["3.0"] * len(timestamps), "qty": ["0.1"] * len(timestamps),
        "quote_qty": ["0.3"] * len(timestamps),
        "is_buyer_maker": [False] * len(timestamps)}), path)


def test_features_microseconds_preserve_values_and_attributes(tmp_path):
    source, target = tmp_path / "features.parquet", tmp_path / "new.parquet"
    before = _feature(source)
    result = module.unify_feature_day(source, target)
    after = pd.read_parquet(target)
    assert str(after.index.dtype) == "datetime64[us, UTC]"
    pd.testing.assert_frame_equal(before, after, check_index_type=False, check_freq=False)
    assert before.attrs == after.attrs
    assert result["readback_equal"] and result["non_clock_values_equal"]
    assert pq.ParquetFile(source).schema_arrow.field("__index_level_0__").type == pa.timestamp("ms", tz="UTC")


@pytest.mark.parametrize("unit", ["ms", "us", "ns"])
def test_future_producer_uses_one_exact_type(unit):
    frame = pd.DataFrame({"a": [1]}, index=pd.date_range(DAY, periods=1, tz="UTC").as_unit(unit))
    normalized = module.canonical_feature_frame(frame)
    assert normalized.index.unit == "us"
    assert frame.index.unit == unit
    assert normalized["a"].equals(frame["a"])


@pytest.mark.parametrize("unit", ["us", "ns"])
def test_metrics_precision_preserves_six_values_order_and_ready_clock(tmp_path, unit):
    source, target = tmp_path / "metrics.parquet", tmp_path / "new.parquet"
    index = pd.date_range(DAY, periods=3, freq="5min", tz="UTC", name="create_time").as_unit(unit)
    before = pd.DataFrame({name: [3.5, np.nan, -0.0] for name in module.METRICS_COLUMNS}, index=index)
    before.attrs["timestamp_convention"] = "end"
    before.to_parquet(source)
    result = module.unify_metrics_day(source, target)
    after = pd.read_parquet(target)
    assert pq.ParquetFile(target).schema_arrow.field("create_time").type == module.METRICS_INDEX_TYPE
    pd.testing.assert_frame_equal(before, after, check_index_type=False, check_freq=False)
    assert before.attrs == after.attrs
    assert result["non_clock_values_equal"] and result["readback_equal"]
    assert result["operation"] == "metrics_index_utc_microseconds"


def test_metrics_submicrosecond_failure_leaves_original_untouched(tmp_path):
    source = tmp_path / "metrics.parquet"
    index = pd.DatetimeIndex([pd.Timestamp(DAY, tz="UTC") + pd.Timedelta(1, unit="ns")], name="create_time")
    pd.DataFrame({name: [2.0] for name in module.METRICS_COLUMNS}, index=index).to_parquet(source)
    old = source.read_bytes()
    with pytest.raises((pa.ArrowInvalid, ValueError), match="(lose|losslessly)"):
        module.unify_metrics_day(source, source)
    assert source.read_bytes() == old


def test_metrics_rejects_changed_field_shape_before_publication(tmp_path):
    source = tmp_path / "metrics.parquet"
    _feature(source, "ns")
    with pytest.raises(ValueError, match="six ordered double"):
        module.unify_metrics_day(source, tmp_path / "out.parquet")
    assert not (tmp_path / "out.parquet").exists()


def test_submicrosecond_precision_is_not_truncated(tmp_path):
    path = tmp_path / "source.parquet"
    frame = pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(DAY, tz="UTC") + pd.Timedelta(1, unit="ns")]))
    frame.to_parquet(path)
    with pytest.raises((pa.ArrowInvalid, ValueError), match="(lose|losslessly|Cannot losslessly)"):
        module.unify_feature_day(path, tmp_path / "out.parquet")
    with pytest.raises(ValueError):
        module.canonical_feature_frame(frame)
    assert not (tmp_path / "out.parquet").exists()


@pytest.mark.parametrize("timezone", [None, "Asia/Shanghai"])
def test_feature_timezone_is_not_silently_reinterpreted(tmp_path, timezone):
    path = tmp_path / "source.parquet"
    pd.DataFrame({"a": [1]}, index=pd.date_range(DAY, periods=1, tz=timezone)).to_parquet(path)
    with pytest.raises(ValueError, match="UTC"):
        module.unify_feature_day(path, tmp_path / "out.parquet")


def test_bars_rebuild_individual_counts_prices_and_true_last_event(tmp_path):
    bars, trades, target = tmp_path / "bars.parquet", tmp_path / DAY / "trades.parquet", tmp_path / "out.parquet"
    _bars(bars)
    _trades(trades, times=(100, 950, 950, 1200))  # same timestamp is not deduplicated
    result = module.unify_reference_bars(bars, trades, target, day=DAY)
    after = pd.read_parquet(target)
    assert list(pq.ParquetFile(target).schema_arrow.names) == list(module.BAR_COLUMNS)
    assert after.last_event_ts_ms.tolist() == [START + 950, START + 1200]
    assert result["individual_rows"] == 4
    assert after.trade_count.tolist() == [3, 1]
    assert after.open.tolist() == [3.0, 3.0]
    assert result["changed_common_rows_by_field"]["trade_count"] == 1
    assert result["changed_common_rows_by_field"]["open"] == 2
    assert result["dependent_reference_features"] == "REFRESH_REQUIRED"
    assert pq.ParquetFile(target).metadata.metadata[b"narrowgate.individual_source_sha256"].decode() == module._digest(trades)


@pytest.mark.parametrize("times,symbol,alias_delta,match", [
    ((), "BTCUSDT", 0, "no observed"),
    ((950, 1200), "BTCUSDC", 0, "symbol mismatch"),
    ((950, 1200), "BTCUSDT", 1, "aliases disagree"),
    ((-1, 1200), "BTCUSDT", 0, "escape"),
    ((950, 100, 1200), "BTCUSDT", 0, "ordered for OHLC"),
])
def test_bad_trade_support_refuses_invention(tmp_path, times, symbol, alias_delta, match):
    bars, trades = tmp_path / "bars.parquet", tmp_path / DAY / "trades.parquet"
    _bars(bars)
    _trades(trades, times, symbol=symbol, alias_delta=alias_delta)
    with pytest.raises(ValueError, match=match):
        module.unify_reference_bars(bars, trades, tmp_path / "out.parquet", day=DAY)
    assert not (tmp_path / "out.parquet").exists()


def test_dry_run_does_not_create_directories(tmp_path):
    source = tmp_path / "source.parquet"
    _feature(source)
    target = tmp_path / "absent" / "out.parquet"
    assert module.unify_feature_day(source, target, dry_run=True)["status"] == "DRY_RUN"
    assert not target.parent.exists()


def test_atomic_failure_preserves_source_and_removes_partial(tmp_path, monkeypatch):
    source = tmp_path / "source.parquet"
    _feature(source)
    before = source.read_bytes()
    def fail(*args):
        raise OSError("injected replace failure")
    monkeypatch.setattr(module.os, "replace", fail)
    with pytest.raises(OSError, match="injected"):
        module.unify_feature_day(source, source)
    assert source.read_bytes() == before
    assert not list(tmp_path.glob("*.partial"))


def test_input_change_during_write_is_not_overwritten(tmp_path, monkeypatch):
    source = tmp_path / "source.parquet"
    _feature(source)
    write = module.pq.write_table
    def change(table, path, **kwargs):
        write(table, path, **kwargs)
        source.write_bytes(b"concurrent change")
    monkeypatch.setattr(module.pq, "write_table", change)
    with pytest.raises(RuntimeError, match="input changed"):
        module.unify_feature_day(source, source)
    assert source.read_bytes() == b"concurrent change"
    assert not list(tmp_path.glob("*.partial"))


def test_existing_output_and_symlink_parent_are_rejected(tmp_path):
    source, target = tmp_path / "source.parquet", tmp_path / "out.parquet"
    _feature(source)
    target.write_bytes(b"owner bytes")
    with pytest.raises(FileExistsError):
        module.unify_feature_day(source, target)
    alias = tmp_path / "alias"
    alias.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match="symbolic link"):
        module.unify_feature_day(source, alias / "new.parquet")
    assert target.read_bytes() == b"owner bytes"


def test_bar_cannot_overwrite_individual_input(tmp_path):
    bars, trades = tmp_path / "bars.parquet", tmp_path / DAY / "trades.parquet"
    _bars(bars)
    _trades(trades)
    before = trades.read_bytes()
    with pytest.raises(ValueError, match="never replace"):
        module.unify_reference_bars(bars, trades, trades, day=DAY)
    assert trades.read_bytes() == before


def test_rebuild_retains_only_observed_seconds_without_filling_empty_bars(tmp_path):
    bars, trades, target = tmp_path / "bars.parquet", tmp_path / DAY / "trades.parquet", tmp_path / "out.parquet"
    _bars(bars)
    _trades(trades, times=(950, 2200))
    result = module.unify_reference_bars(bars, trades, target, day=DAY)
    after = pd.read_parquet(target)
    assert after.index.tolist() == [START, START + 2000]
    assert result["old_only_seconds"] == 1
    assert result["new_only_seconds"] == 1
    assert after.trade_count.tolist() == [1, 1]


def test_spot_cannot_substitute_for_reference_perpetual(tmp_path):
    bars, trades = tmp_path / "bars.parquet", tmp_path / DAY / "trades.parquet"
    _bars(bars)
    _trades(trades)
    data = pq.read_table(trades)
    data = data.set_column(data.schema.get_field_index("exchange"), "exchange", pa.array(["binance"] * len(data)))
    pq.write_table(data, trades)
    with pytest.raises(ValueError, match="same Binance futures"):
        module.unify_reference_bars(bars, trades, tmp_path / "out.parquet", day=DAY)


def test_cli_defaults_to_dry_run(tmp_path, capsys):
    source, target = tmp_path / "source.parquet", tmp_path / "out.parquet"
    _feature(source)
    assert module.main(["features", "--input", str(source), "--output", str(target)]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "DRY_RUN"
    assert not target.exists()


def test_reference_calendar_resume_validates_identity_without_rebuilding(tmp_path, monkeypatch):
    old, raw, output = tmp_path / "old", tmp_path / "raw", tmp_path / "new"
    old.mkdir()
    _bars(old / f"BTCUSDT-1s-{DAY}.parquet")
    _trades(raw / DAY / "trades.parquet")
    config = dict(start_day=DAY, end_day=DAY, dry_run=False)
    assert module.unify_reference_calendar(old, raw, output, **config)["status"] == "COMPLETED"
    original = module.unify_reference_bars
    def no_call(*args, **kwargs):
        raise AssertionError("completed day should not rebuild")
    monkeypatch.setattr(module, "unify_reference_bars", no_call)
    assert module.unify_reference_calendar(old, raw, output, **config)["status"] == "COMPLETED"
    monkeypatch.setattr(module, "unify_reference_bars", original)
    (raw / DAY / "trades.parquet").write_bytes(b"new owner input")
    with pytest.raises(RuntimeError, match="identity changed"):
        module.unify_reference_calendar(old, raw, output, **config)
    progress = json.loads((output / "current.json").read_text())
    assert progress["status"] == "FAILED"
    assert progress["records"][DAY]["status"] == "VERIFIED"


def test_preexisting_sample_must_equal_fresh_rebuild_for_reuse(tmp_path):
    bars, trades, output = tmp_path / "bars.parquet", tmp_path / DAY / "trades.parquet", tmp_path / "out.parquet"
    _bars(bars)
    _trades(trades)
    first = module.unify_reference_bars(bars, trades, output, day=DAY)
    second = module.unify_reference_bars(bars, trades, output, day=DAY, reuse_verified_output=True)
    assert second["status"] == "VERIFIED_EXISTING"
    assert first["output_sha256"] == second["output_sha256"]
    changed = pq.read_table(output)
    changed = changed.set_column(changed.schema.get_field_index("open"), "open", pa.array([9.0] * len(changed)))
    pq.write_table(changed, output)
    with pytest.raises(FileExistsError):
        module.unify_reference_bars(bars, trades, output, day=DAY, reuse_verified_output=True)
