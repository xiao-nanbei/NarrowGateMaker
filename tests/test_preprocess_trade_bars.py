from __future__ import annotations

import json
import hashlib
import sys
from pathlib import Path

import pandas as pd
import pytest

from features.preprocess import process_file


def test_bar_rebuilds_changed_source_and_reuses_only_exact_binding(tmp_path):
    source = tmp_path / "BTCUSDC-trades-2026-01-01.csv"
    original = (
        "id,price,qty,quote_qty,time,is_buyer_maker\n"
        "1,100.0,0.1,10.0,1767225600100,false\n"
        "2,101.0,0.2,20.2,1767225600900,true\n"
    )
    source.write_text(original)
    output = tmp_path / "bars"
    output.mkdir()
    path, status, _, _ = process_file(source, "BTCUSDC", output, data_type="trades")
    assert status == "ok"
    assert process_file(source, "BTCUSDC", output, data_type="trades")[1] == "skip"
    source.write_text(original.replace("2,101.0,0.2,20.2", "2,102.0,0.2,20.4"))
    assert process_file(source, "BTCUSDC", output, data_type="trades")[1] == "ok"
    assert pd.read_parquet(path).iloc[0]["close"] == 102.
    metadata = json.loads(path.with_suffix(".parquet.meta.json").read_text())
    assert metadata["source_sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert metadata["trade_count_unit"] == "individual_execution"
    # Old unbound files do not survive simply because their names match.
    path.with_suffix(".parquet.meta.json").unlink()
    assert process_file(source, "BTCUSDC", output, data_type="trades")[1] == "ok"


def test_auto_uses_individual_tape_once_and_replaces_native_bars(tmp_path, monkeypatch):
    from features import preprocess

    source = tmp_path / "inputs"
    source.mkdir()
    day = "2026-01-01"
    native = source / f"BTCUSDC-aggTrades-{day}.csv"
    native.write_text(
        "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n"
        "10,100.0,0.3,1,2,1767225600100,false\n"
    )
    trades = source / f"BTCUSDC-trades-{day}.csv"
    trades.write_text(
        "id,price,qty,quote_qty,time,is_buyer_maker\n"
        "1,100.0,0.1,10.0,1767225600100,false\n"
        "2,101.0,0.2,20.2,1767225600900,true\n"
    )
    out = tmp_path / "bars"
    out.mkdir()
    path, _, _, _ = process_file(native, "BTCUSDC", out)
    assert pd.read_parquet(path).iloc[0]["trade_count"] == 1
    assert preprocess._select_daily_inputs([native, trades], prefer_individual=True) == [trades]
    monkeypatch.setattr(sys, "argv", ["preprocess", "--symbol", "BTCUSDC",
                                     "--input-dir", str(source), "--output-dir", str(out)])
    preprocess.main()
    frame = pd.read_parquet(path)
    assert frame.iloc[0]["trade_count"] == 2
    assert frame.iloc[0]["open"] == 100.
    assert frame.iloc[0]["close"] == 101.
    assert json.loads(path.with_suffix(".parquet.meta.json").read_text())["source_data_type"] == "trades"


@pytest.mark.parametrize("data_type", ["trades", "aggTrades"])
def test_bar_builder_rejects_time_bucket_as_raw_tape(tmp_path, data_type):
    path = tmp_path / "2026-01-01" / "trade_aggregates_100ms.parquet"
    path.parent.mkdir()
    pd.DataFrame({"group_id": [1], "bucket_start_ms": [1767225600000],
                  "feature_ready_ts_ms": [1767225600100]}).to_parquet(path)
    with pytest.raises(ValueError, match="self-aggregated flow features"):
        process_file(path, "BTCUSDC", tmp_path, data_type=data_type)


def test_canonical_daily_parquet_builds_identical_bars_and_tempo(tmp_path):
    from research.families.f03_causal_13_head import taker_tempo_features as tempo

    frame = pd.DataFrame({"id": [1, 2], "price": [100., 101.], "qty": [.1, .2],
                          "quote_qty": [10., 20.2], "time": [1767225600100, 1767225600900],
                          "is_buyer_maker": [False, True]})
    csv = tmp_path / "BTCUSDC-trades-2026-01-01.csv"
    frame.to_csv(csv, index=False)
    parquet = tmp_path / "2026-01-01" / "trades.parquet"
    parquet.parent.mkdir()
    canonical = frame.assign(timestamp=frame["time"] * 1000, amount=frame["qty"].astype(str))
    canonical.to_parquet(parquet)
    results = []
    tempos = []
    for index, source in enumerate([csv, parquet]):
        directory = tmp_path / f"bars{index}"
        directory.mkdir()
        path, status, _, count = process_file(source, "BTCUSDC", directory, data_type="trades")
        assert status == "ok" and count == 2
        results.append(pd.read_parquet(path))
        _, count, path = tempo.process_file(source, "BTCUSDC", tmp_path / f"tempo{index}",
                                            chunk_size=1, verbose=False, overwrite=False, dense=False)
        assert count == 2
        tempos.append(pd.read_parquet(path))
    pd.testing.assert_frame_equal(*results)
    pd.testing.assert_frame_equal(*tempos)


def test_individual_trade_bar_writes_atomic_provenance(tmp_path: Path) -> None:
    source = tmp_path / "BTCUSDT-trades-2026-01-01.csv"
    source.write_text(
        "id,price,qty,quote_qty,time,is_buyer_maker\n"
        "1,100.0,0.1,10.0,1767225600100,false\n"
        "2,101.0,0.2,20.2,1767225600900,true\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "bars"
    out_dir.mkdir()

    output, status, rows, source_rows = process_file(
        source,
        "BTCUSDT",
        out_dir,
        data_type="trades",
    )

    assert status == "ok"
    assert rows == 1
    assert source_rows == 2
    frame = pd.read_parquet(output)
    assert frame.iloc[0]["close"] == 101.0
    assert frame.iloc[0]["trade_count"] == 2
    assert frame.iloc[0]["last_event_ts_ms"] == 1_767_225_600_900
    metadata = json.loads(
        output.with_suffix(output.suffix + ".meta.json").read_text(
            encoding="utf-8"
        )
    )
    assert metadata["complete"] is True
    assert metadata["schema_version"] == "binance_individual_trade_bar_1s.v1"
    assert metadata["source_data_type"] == "trades"
    assert metadata["utc_day"] == "2026-01-01"
    assert metadata["causal_visible_at"] == "t+1s"
    assert not list(out_dir.glob("*.tmp"))


def test_spot_canonical_original_microsecond_clock_matches_headerless_csv(tmp_path, monkeypatch):
    from features import preprocess

    day = "2026-01-01"
    values = pd.DataFrame({
        "agg_trade_id": [1, 2], "price": ["100.00", "101.00"], "quantity": ["0.1", "0.2"],
        "first_trade_id": [1, 2], "last_trade_id": [1, 2],
        "transact_time": [1767225600100000, 1767225600900000],
        "is_buyer_maker": ["false", "true"], "is_best_match": ["true", "true"],
    })
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    csv = legacy / f"BTCUSDT-aggTrades-{day}.csv"
    values.to_csv(csv, header=False, index=False)
    path = tmp_path / "raw/binance_spot/BTCUSDT" / day / "aggTrades.parquet"
    path.parent.mkdir(parents=True)
    values.to_parquet(path)
    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    outputs = []
    for mode in ("explicit", "canonical"):
        out = tmp_path / mode
        argv = ["preprocess", "--symbol", "BTCUSDT", "--market-type", "spot",
                "--data-type", "aggTrades", "--file", day, "--output-dir", str(out)]
        if mode == "explicit":
            argv += ["--input-dir", str(legacy)]
        else:
            argv += ["--cleanup-input"]  # Legacy CSV cleanup never deletes canonical raw.
        monkeypatch.setattr(sys, "argv", argv)
        preprocess.main()
        outputs.append(pd.read_parquet(out / f"BTCUSDT-1s-{day}.parquet"))
    pd.testing.assert_frame_equal(*outputs)
    assert outputs[1].index.tolist() == [1767225600000]
    assert csv.exists() and path.exists()
