from pathlib import Path

import pandas as pd
import pytest

from data.audit_raw_trades import buyer_maker_counts, scan_raw_trade_file


@pytest.mark.parametrize("aggregate", [True, False])
def test_self_aggregated_features_cannot_masquerade_as_execution_tape(tmp_path, aggregate):
    from models import backtest_tick as bt

    path = tmp_path / "trade_aggregates_100ms.parquet"
    pd.DataFrame({"group_id": [1], "bucket_start_ms": [1767225600000],
                  "feature_ready_ts_ms": [1767225600100]}).to_parquet(path)
    with pytest.raises(ValueError, match="not a native execution"):
        bt._read_daily_trade_parquet(path, aggregate=aggregate)


def test_canonical_daily_trades_preserve_matching_ids_and_prefer_one_copy(tmp_path, monkeypatch):
    from models import backtest_tick as bt
    from data_paths import daily_market_path

    monkeypatch.delenv("NARROWGATE_DATA_ROOT", raising=False)
    monkeypatch.delenv("MM_RAW_TRADES_DIR", raising=False)
    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    monkeypatch.setattr(bt, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(bt, "RAW_DIR", tmp_path / "raw")
    monkeypatch.setattr(bt, "RAW_TRADES_DIR", tmp_path / "raw_trades")
    monkeypatch.setattr(bt, "SYMBOL", "BTCUSDC")
    day = "2026-01-01"
    timestamp = 1_767_225_600_001_000
    individual = daily_market_path(day, "BTCUSDC", "trades")
    individual.parent.mkdir(parents=True)
    pd.DataFrame({"timestamp": [timestamp, timestamp], "id": [2, 1],
                  "price": ["100", "100"], "amount": ["0.2", "0.1"],
                  "qty": ["0.2", "0.1"], "is_buyer_maker": [False, True]}).to_parquet(individual)
    frame = bt.load_individual_trades([day], quality_allowed_days=[day])
    assert frame["trade_id"].tolist() == [1, 2]
    assert frame["quantity"].tolist() == [.1, .2]
    assert frame["transact_time"].tolist() == [timestamp // 1000] * 2

    aggregate = daily_market_path(day, "BTCUSDC", "aggTrades")
    pd.DataFrame({"timestamp": [timestamp], "agg_trade_id": [10],
                  "first_trade_id": [1], "last_trade_id": [2], "price": ["100"],
                  "quantity": ["0.3"], "is_buyer_maker": [True]}).to_parquet(aggregate)
    legacy = tmp_path / "raw" / f"BTCUSDC-aggTrades-{day}.csv"
    legacy.write_text("agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n"
                      f"11,999,9,3,3,{timestamp//1000},false\n")
    frame = bt.load_aggtrades([day], quality_allowed_days=[day])
    assert frame["price"].tolist() == [100.]
    assert bt._read_aggtrade_csv(aggregate, include_trade_ids=True)["agg_trade_id"].tolist() == [10]
    # An explicit filename filter still requests the legacy fixture.
    assert bt.load_aggtrades([day], files_filter=legacy.name, quality_allowed_days=[day])["price"].tolist() == [999.]


def test_buyer_maker_counts_accepts_bool_and_text() -> None:
    assert buyer_maker_counts(pd.Series([True, False, True])) == (2, 1, 0)
    assert buyer_maker_counts(
        pd.Series(["true", "FALSE", "1", "0", "bad"])
    ) == (2, 2, 1)


@pytest.mark.parametrize("explicit_raw", [True, False])
def test_daily_trade_discovery_honors_separate_raw_root(tmp_path, monkeypatch, explicit_raw):
    from models import backtest_tick as bt
    import data_paths
    from data_paths import daily_market_path

    monkeypatch.setenv("NARROWGATE_DATA_ROOT", str(tmp_path / "derived"))
    monkeypatch.setattr(data_paths, "PRIVATE_STORAGE_ROOTS_PATH", tmp_path / "missing.json")
    monkeypatch.setenv("NARROWGATE_MARKETDATA_ROOT", str(tmp_path / "MarketData"))
    if explicit_raw:
        monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    else:
        monkeypatch.delenv("NARROWGATE_RAW_DATA_ROOT", raising=False)
    monkeypatch.setattr(bt, "DATA_ROOT", tmp_path / "derived")
    monkeypatch.setattr(bt, "SYMBOL", "BTCUSDC")
    day = "2026-01-01"
    path = daily_market_path(day, "BTCUSDC", "trades")
    assert not path.is_relative_to(tmp_path / "derived")
    path.parent.mkdir(parents=True)
    path.touch()
    for days in ([day], None):
        assert bt._prefer_daily_trade_paths(
            [], channel="trades", days=days, enabled=True,
        ) == [path]


def test_raw_trade_scan_requires_both_sides(tmp_path: Path) -> None:
    path = tmp_path / "BTCUSDC-trades-2026-01-01.csv"
    pd.DataFrame(
        {
            "id": [1, 2],
            "time": [1_767_225_600_001, 1_767_225_600_002],
            "is_buyer_maker": [True, False],
        }
    ).to_csv(path, index=False)

    stats = scan_raw_trade_file(path, "BTCUSDC", chunk_size=1)

    assert stats.side_complete is True
    assert stats.buyer_maker_true_count == 1
    assert stats.buyer_maker_false_count == 1
