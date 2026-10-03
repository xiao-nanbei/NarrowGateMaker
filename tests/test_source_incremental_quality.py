from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.families.f04_external_market_alpha.external_venue_features import (  # noqa: E402
    _bars_on_decision_grid,
)
from features.feature_engineer import (  # noqa: E402
    _assert_unique_time_index,
    _basis_residual_bps,
    _cross_market_feature_frame,
    _source_freshness,
)
from research.families.f04_external_market_alpha.external_consensus_layer import (  # noqa: E402
    build_causal_consensus_1s,
    build_spot_perp_state_1s,
)
from research.families.f03_causal_13_head.ml_model import _filter_source_profile  # noqa: E402


def test_source_freshness_uses_bucket_end_and_rejects_stale_values():
    source = pd.DatetimeIndex(["2026-05-15T00:00:09Z"])
    target = pd.date_range("2026-05-15T00:00:00Z", periods=4, freq="10s")

    age, available = _source_freshness(source, target)

    assert age.iloc[0] == pytest.approx(1.0)
    assert available.tolist() == [1.0, 1.0, 1.0, 0.0]
    assert age.iloc[-1] == pytest.approx(31.0)


def test_basis_residual_anchor_does_not_include_current_observation():
    basis = pd.Series([0.0] * 30 + [12.0])

    residual = _basis_residual_bps(basis)

    assert residual.iloc[:30].eq(0.0).all()
    assert residual.iloc[30] == pytest.approx(12.0)


def test_duplicate_feature_timestamps_fail_fast():
    index = pd.DatetimeIndex(["2026-05-15T00:00:00Z", "2026-05-15T00:00:00Z"])
    frame = pd.DataFrame({"close": [1.0, 2.0]}, index=index)

    with pytest.raises(ValueError, match="duplicate timestamps"):
        _assert_unique_time_index(frame, "synthetic")


def test_cross_market_rolling_does_not_bridge_long_source_gap():
    first = pd.date_range("2026-05-15T00:00:00Z", periods=20, freq="1s")
    second = pd.date_range("2026-05-15T00:01:20Z", periods=20, freq="1s")
    index = first.append(second)
    close = np.r_[np.full(len(first), 100.0), np.full(len(second), 110.0)]
    source = pd.DataFrame({
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "vwap": close,
        "volume": 1.0,
        "buy_volume": 0.5,
        "sell_volume": 0.5,
        "trade_count": 1,
        "buy_count": 1,
        "sell_count": 0,
    }, index=index)
    target = pd.date_range("2026-05-15T00:00:00Z", periods=10, freq="10s")
    target_close = pd.Series(100.0, index=target)

    features = _cross_market_feature_frame(source, target, target_close, "cv_test")

    assert features.loc[target[4], "cv_test_available"] == 0.0
    assert features.loc[target[8], "cv_test_ret_10s"] == 0.0


def test_trade_bar_bridge_is_right_edge_visible_and_freshness_bounded(
    tmp_path: Path,
) -> None:
    day = "2026-05-15"
    start = pd.Timestamp(day, tz="UTC")
    start_ms = int(start.timestamp() * 1000)
    frame = pd.DataFrame(
        {
            "close": [100.0, 101.0],
            "buy_volume": [1.0, 1.0],
            "sell_volume": [0.0, 0.0],
            "trade_count": [1, 1],
        },
        index=pd.Index([start_ms, start_ms + 4_000], name="timestamp"),
    )
    path = tmp_path / "BTCUSDT-1s-2026-05-15.parquet"
    frame.to_parquet(path)

    result = _bars_on_decision_grid(
        path,
        day,
        "bridge",
        max_source_age_s=2.0,
    )

    assert result.loc[start + pd.Timedelta(seconds=1), "_bridge_close"] == 100.0
    assert result.loc[start + pd.Timedelta(seconds=2), "_bridge_close"] == 100.0
    assert np.isnan(
        result.loc[start + pd.Timedelta(seconds=3), "_bridge_close"]
    )
    assert result.loc[start + pd.Timedelta(seconds=5), "_bridge_close"] == 101.0
    assert result.loc[
        start + pd.Timedelta(seconds=1), "bridge_source_age_ms"
    ] == 1_000.0


def test_source_profile_keeps_local_and_selected_source_only():
    columns = [
        "close",
        "volume_imbalance",
        "cv_ref_perp_ret_10s",
        "cv_ref_spot_ret_10s",
        "cv_exec_spot_ret_10s",
    ]

    selected = _filter_source_profile(columns, "local_ref_perp")

    assert selected == ["close", "volume_imbalance", "cv_ref_perp_ret_10s"]


def test_causal_1s_consensus_requires_both_fresh_venues_without_future_backfill():
    bitget = pd.DataFrame({
        "timestamp": [1000, 2000],
        "last_event_ts_ms": [900, 1900],
        "close": [100.0, 101.0],
        "flow_imbalance": [0.5, 0.2],
    })
    bybit = pd.DataFrame({
        "timestamp": [1000, 3000],
        "last_event_ts_ms": [1000, 2950],
        "close": [100.0, 103.0],
        "flow_imbalance": [-0.5, 0.8],
    })

    consensus = build_causal_consensus_1s(
        {"bitget": bitget, "bybit": bybit},
        min_venues=2,
        max_source_age_s=1.0,
    )

    assert consensus["timestamp"].tolist() == [1000, 2000]
    # At t=2s Bybit's t=3s observation is not visible; its t=1s close is used.
    assert consensus.loc[1, "bybit_close"] == pytest.approx(100.0)
    assert consensus.loc[1, "close"] == pytest.approx(np.sqrt(101.0 * 100.0))
    assert consensus["available_venues"].eq(2).all()


def test_three_venue_consensus_uses_return_median_and_emits_leave_one_out():
    timestamps = [1000, 2000]
    frames = {
        "bitget": pd.DataFrame({
            "timestamp": timestamps,
            "last_event_ts_ms": [900, 1900],
            "close": [100.0, 100.01],
        }),
        "bybit": pd.DataFrame({
            "timestamp": timestamps,
            "last_event_ts_ms": [900, 1900],
            "close": [101.0, 101.0101],
        }),
        "okx": pd.DataFrame({
            "timestamp": timestamps,
            "last_event_ts_ms": [900, 1900],
            "close": [99.0, 99.99],
        }),
    }

    consensus = build_causal_consensus_1s(frames, min_venues=2)
    row = consensus.loc[consensus["timestamp"].eq(2000)].iloc[0]

    expected = np.median([
        np.log(100.01 / 100.0),
        np.log(101.0101 / 101.0),
        np.log(99.99 / 99.0),
    ])
    assert row["consensus_ret_1s"] == pytest.approx(expected)
    assert row["majority_direction"] == 1
    assert row["outlier_venue"] == "okx"
    assert np.isfinite(row["leave_okx_out_ret_1s"])


def test_spot_perp_state_is_causal_and_separates_leadership():
    timestamps = np.arange(1_000, 41_000, 1_000)
    perp_returns = np.zeros(40)
    spot_returns = np.zeros(40)
    spot_returns[35] = 0.0002
    perp_returns[36] = -0.0002
    spot_returns[37] = 0.00015
    perp_returns[37] = 0.00012
    spot_returns[38] = -0.00015
    perp_returns[38] = 0.00012

    def frame(close, returns):
        return pd.DataFrame({
            "timestamp": timestamps,
            "close": close,
            "consensus_ret_1s": returns,
            "agreement_score": 1.0,
            "dispersion_bps": 0.1,
            "flow_imbalance": 0.0,
            "available_venues": 2,
            "max_source_age_ms": 100.0,
        })

    result = build_spot_perp_state_1s(
        frame(np.full(40, 100.1), perp_returns),
        frame(np.full(40, 100.0), spot_returns),
    )
    assert result.loc[35, "cross_instrument_state"] == "spot_leading_up"
    assert result.loc[36, "cross_instrument_state"] == "perp_only_down"
    assert result.loc[37, "cross_instrument_state"] == "confirmed_up"
    assert result.loc[38, "cross_instrument_state"] == "divergent"
    # The current basis is compared with a shifted history-only anchor.
    assert result.loc[30:, "perp_minus_spot_bps"].abs().max() < 1e-9
