import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.families.f04_external_market_alpha.external_consensus_layer import (
    _build_daily_hierarchical,
    _causal_futures_trade_bar,
    build_hierarchical_reference_1s,
)


def _write_complete_metadata(path, day):
    path.with_suffix(path.suffix + ".meta.json").write_text(
        json.dumps({"complete": True, "utc_day": day}),
        encoding="utf-8",
    )


def test_daily_hierarchical_builder_uses_individual_trade_bridge(tmp_path):
    day = "2026-01-01"
    start_ms = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    timestamp = np.arange(start_ms + 1_000, start_ms + 6_000, 1_000)
    consensus = pd.DataFrame(
        {
            "timestamp": timestamp,
            "consensus_ret_1s": [0.0, 0.00001, 0.00001, 0.00001, 0.00001],
            "available_venues": 3,
            "consensus_confidence": 0.8,
            "return_dispersion_bps": 0.2,
            "majority_direction": 1,
        }
    )
    perp_dir = tmp_path / "perp"
    spot_dir = tmp_path / "spot"
    bridge_dir = tmp_path / "bridge"
    bbo_dir = tmp_path / "bbo"
    spot_bar_dir = tmp_path / "spot_bars"
    out_dir = tmp_path / "out"
    for directory in (
        perp_dir,
        spot_dir,
        bridge_dir,
        bbo_dir,
        spot_bar_dir,
    ):
        directory.mkdir()

    for directory, name in (
        (perp_dir, f"BTCUSDT-perp-{day}.parquet"),
        (spot_dir, f"BTCUSDT-spot-{day}.parquet"),
    ):
        path = directory / name
        consensus.to_parquet(path, index=False)
        _write_complete_metadata(path, day)

    bridge_path = bridge_dir / f"BTCUSDT-1s-{day}.parquet"
    bridge = pd.DataFrame(
        {
            "close": np.linspace(100.0, 100.4, 5),
            "trade_count": 1,
            "last_event_ts_ms": timestamp - 100,
        },
        index=pd.Index(timestamp - 1_000, name="timestamp"),
    )
    bridge.to_parquet(bridge_path)
    bridge_sha256 = hashlib.sha256(bridge_path.read_bytes()).hexdigest()
    bridge_path.with_suffix(bridge_path.suffix + ".meta.json").write_text(
        json.dumps(
            {
                "schema_version": "binance_individual_trade_bar_1s.v1",
                "complete": True,
                "utc_day": day,
                "source_data_type": "trades",
                "output_sha256": bridge_sha256,
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        {
            "timestamp": timestamp,
            "best_bid": np.linspace(99.9, 100.3, 5),
            "best_ask": np.linspace(100.1, 100.5, 5),
        }
    ).to_parquet(bbo_dir / f"BTCUSDC-bbo-{day}.parquet", index=False)
    pd.DataFrame(
        {"close": np.linspace(99.8, 100.2, 5)},
        index=pd.Index(timestamp - 1_000, name="timestamp"),
    ).to_parquet(spot_bar_dir / f"BTCUSDC-1s-{day}.parquet")

    result = _build_daily_hierarchical(
        day,
        perp_dir,
        spot_dir,
        bridge_dir,
        bbo_dir,
        spot_bar_dir,
        None,
        out_dir,
        2,
        2,
        2.0,
        2.0,
    )
    metadata = json.loads(
        Path(result["path"])
        .with_suffix(".parquet.meta.json")
        .read_text(encoding="utf-8")
    )

    assert result["status"] == "built"
    assert metadata["binance_bridge_source"] == (
        "official_binance_individual_trade_bar_1s"
    )
    assert metadata["binance_bridge_artifact_sha256"] == bridge_sha256
    assert metadata["output_sha256"]


def test_futures_trade_bar_is_right_edge_visible_and_stale_limited(tmp_path):
    day = "2026-01-01"
    start_ms = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    frame = pd.DataFrame(
        {
            "close": [100.0, 101.0, 999.0],
            "trade_count": [2, 1, 0],
        },
        index=pd.Index(
            [start_ms, start_ms + 3_000, start_ms + 4_000],
            name="timestamp",
        ),
    )
    path = tmp_path / "BTCUSDT-1s-2026-01-01.parquet"
    frame.to_parquet(path)

    actual = _causal_futures_trade_bar(path, day, max_source_age_s=2.0)
    observed = actual.set_index("timestamp")

    assert observed.loc[start_ms + 1_000, "mid"] == 100.0
    assert observed.loc[start_ms + 2_000, "mid"] == 100.0
    assert start_ms + 3_000 not in observed.index
    assert observed.loc[start_ms + 4_000, "mid"] == 101.0
    assert observed.loc[start_ms + 5_000, "mid"] == 101.0
    assert start_ms + 6_000 not in observed.index


def test_futures_trade_bar_uses_exact_last_event_timestamp_when_available(
    tmp_path,
):
    day = "2026-01-01"
    start_ms = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    frame = pd.DataFrame(
        {
            "close": [100.0],
            "trade_count": [1],
            "last_event_ts_ms": [start_ms + 900],
        },
        index=pd.Index([start_ms], name="timestamp"),
    )
    path = tmp_path / "BTCUSDT-1s-2026-01-01.parquet"
    frame.to_parquet(path)

    actual = _causal_futures_trade_bar(path, day, max_source_age_s=2.0)

    assert actual.iloc[0]["timestamp"] == start_ms + 1_000
    assert actual.iloc[0]["source_age_ms"] == 100


def test_hierarchical_reference_uses_external_innovation_not_level_average():
    timestamp = np.arange(1_000, 6_000, 1_000)

    def consensus(move_bps):
        return pd.DataFrame({
            "timestamp": timestamp,
            "consensus_ret_1s": np.asarray(move_bps) / 10_000.0,
            "available_venues": 3,
            "consensus_confidence": 0.8,
            "return_dispersion_bps": 0.2,
            "majority_direction": 1,
            "outlier_venue": "okx",
        })

    def price(name, values):
        return pd.DataFrame({"timestamp": timestamp, name: values})

    frame = build_hierarchical_reference_1s(
        consensus([0.0, 1.0, 1.0, 1.0, 1.0]),
        consensus([0.0, 1.2, 1.2, 1.2, 1.2]),
        price("binance_btcusdt_perp_mid", [60_000.0, 60_003.0, 60_006.0, 60_009.0, 60_012.0]),
        price("execution_btcusdc_perp_mid", [59_940.0] * 5),
        price("binance_btcusdc_spot_mid", [59_930.0] * 5),
        basis_window_s=2,
        basis_min_periods=2,
        max_dispersion_bps=2.0,
    )

    assert frame["global_reference_valid"].iloc[-1] == 1
    assert frame["global_spot_move_bps"].iloc[-1] == 1.2
    assert frame["global_perp_move_bps"].iloc[-1] == 1.0
    assert frame["external_correction_bps"].iloc[-1] > 0.0
    assert frame["bridge_source"].iloc[-1] == "binance_btcusdc_spot"


def test_hierarchical_reference_prefers_usdcusdt_conversion_over_spot_fallback():
    timestamp = np.arange(1_000, 5_000, 1_000)

    def consensus():
        return pd.DataFrame({
            "timestamp": timestamp,
            "consensus_ret_1s": [0.0, 0.00001, 0.00001, 0.00001],
            "available_venues": 3,
            "consensus_confidence": 0.8,
            "return_dispersion_bps": 0.2,
            "majority_direction": 1,
        })

    def price(name, values):
        return pd.DataFrame({"timestamp": timestamp, name: values})

    frame = build_hierarchical_reference_1s(
        consensus(),
        consensus(),
        price("binance_btcusdt_perp_mid", [60_000.0] * 4),
        price("execution_btcusdc_perp_mid", [59_950.0] * 4),
        price("binance_btcusdc_spot_mid", [59_900.0] * 4),
        price("binance_usdcusdt_spot_mid", [1.001] * 4),
        basis_window_s=2,
        basis_min_periods=2,
    )

    assert frame["bridge_source"].iloc[-1] == "binance_btcusdt_perp/usdcusdt"
    assert frame["local_bridge_px_usdc"].iloc[-1] == pytest.approx(60_000.0 / 1.001)
