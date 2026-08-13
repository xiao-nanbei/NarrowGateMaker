from __future__ import annotations

import numpy as np
import pandas as pd

from features import feature_engineer as engineer


def test_process_day_features_only_never_calls_label_builder(monkeypatch) -> None:
    index = pd.date_range("2026-01-01", periods=180, freq="1s", tz="UTC")
    bars = pd.DataFrame(
        {
            "open": np.linspace(100.0, 101.0, len(index)),
            "high": np.linspace(100.1, 101.1, len(index)),
            "low": np.linspace(99.9, 100.9, len(index)),
            "close": np.linspace(100.0, 101.0, len(index)),
            "volume": np.ones(len(index)),
            "buy_volume": np.full(len(index), 0.5),
            "sell_volume": np.full(len(index), 0.5),
            "trade_count": np.ones(len(index)),
            "buy_count": np.ones(len(index)),
            "sell_count": np.ones(len(index)),
        },
        index=index,
    )
    monkeypatch.setattr(
        engineer,
        "add_taker_tempo_features",
        lambda frame, *args, **kwargs: frame,
    )
    monkeypatch.setattr(
        engineer,
        "add_execution_l2_features",
        lambda frame, *args, **kwargs: frame,
    )
    monkeypatch.setattr(engineer, "load_metrics", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        engineer,
        "add_cross_market_features",
        lambda frame, *args, **kwargs: frame,
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("future labels must not be computed")

    monkeypatch.setattr(engineer, "add_labels", forbidden)
    result = engineer.process_day(
        bars,
        "2026-01-01",
        "BTCUSDC",
        market_stage="single",
        include_labels=False,
    )

    assert not any(column.startswith("label_") for column in result.columns)
    assert "sample_weight" in result.columns


def test_features_only_cli_is_explicit() -> None:
    source = engineer.Path(engineer.__file__).read_text(encoding="utf-8")
    assert 'split = {"inference": sorted(daily_tags)}' in source
    assert '"labels_materialized": bool(labels_materialized)' in source


def test_warmup_input_still_emits_complete_target_day_grid() -> None:
    index = pd.DatetimeIndex(
        [
            pd.Timestamp("2026-01-01 23:59:58", tz="UTC"),
            pd.Timestamp("2026-01-02 23:59:48", tz="UTC"),
        ]
    )
    bars = pd.DataFrame(
        {
            "open": [100.0, 101.0],
            "high": [100.0, 101.0],
            "low": [100.0, 101.0],
            "close": [100.0, 101.0],
            "vwap": [100.0, 101.0],
            "volume": [1.0, 1.0],
            "buy_volume": [0.5, 0.5],
            "sell_volume": [0.5, 0.5],
            "trade_count": [1, 1],
            "buy_count": [1, 1],
            "sell_count": [1, 1],
        },
        index=index,
    )

    dense = engineer.densify_bars_1s(
        bars,
        ensure_through_day_tag="2026-01-02",
    )
    target = engineer.resample_to_10s(dense).loc["2026-01-02"]

    assert len(target) == 8_640
    assert target.index[-1] == pd.Timestamp("2026-01-02 23:59:50", tz="UTC")
    assert target.iloc[-1]["close"] == 101.0
    assert target.iloc[-1]["volume"] == 0.0
