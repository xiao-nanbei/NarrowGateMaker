import gzip
import json
import random

import pytest

from research.system_engineering.audit.market_data_latency import (
    MarketDataLatencySimulator,
    build_latency_profile,
)


def _row(ts_ns, *, lag_ms, market_id="okx:perp:BTCUSDT", event_type="book"):
    receive_ns = ts_ns + int(lag_ms * 1_000_000)
    return {
        "market_id": market_id,
        "event_type": event_type,
        "transport": "websocket",
        "exchange_event_ts_ns": ts_ns,
        "local_receive_ts_ns": receive_ns,
        "feature_ready_ts_ns": receive_ns + 250_000,
        "transport_lag_ms": lag_ms,
        "feature_latency_us": 250.0,
    }


def test_profile_filters_receive_window_and_preserves_environment(tmp_path):
    end_ns = 1_800_000_000_000_000_000
    rows = [
        _row(end_ns - 4_000_000_000, lag_ms=999.0),
        _row(end_ns - 900_000_000, lag_ms=10.0),
        _row(end_ns - 100_000_000, lag_ms=30.0),
    ]
    path = tmp_path / "tape.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.writelines(json.dumps(row) + "\n" for row in rows)

    profile = build_latency_profile(
        [path],
        profile_id="provider_neutral_test",
        environment={"region": "ap-northeast-1", "vcpu": 2},
        window_seconds=2,
        end_receive_ns=end_ns,
        transports={"websocket"},
    )

    assert profile["measurement"]["row_count"] == 2
    assert profile["environment"]["region"] == "ap-northeast-1"
    group = profile["groups"][0]
    assert group["transport_lag_ms_p50"] == pytest.approx(20.0)
    assert group["visibility_lag_ms_p50"] == pytest.approx(20.25)
    assert len(group["simulation_quantile_probabilities"]) == len(
        group["simulation_visibility_lag_ms_quantiles"]
    )


def test_simulator_separates_captured_from_profile_visibility():
    group = {
        "market_id": "okx:perp:BTCUSDT",
        "event_type": "book",
        "transport": "websocket",
        "visibility_lag_ms_p50": 25.0,
        "visibility_lag_ms_p95": 40.0,
        "visibility_lag_ms_p99": 60.0,
        "visibility_lag_ms_p999": 90.0,
        "visibility_lag_ms_max": 100.0,
        "simulation_quantile_probabilities": [0.0, 1.0],
        "simulation_visibility_lag_ms_quantiles": [10.0, 100.0],
    }
    simulator = MarketDataLatencySimulator(
        {
            "schema": "market_data_latency_profile.v1",
            "profile_id": "provider_neutral_test",
            "groups": [group],
        }
    )
    row = {
        "market_id": "okx:perp:BTCUSDT",
        "event_type": "book",
        "transport": "websocket",
        "exchange_event_ts_ns": 1_000_000_000,
        "feature_ready_ts_ns": 1_005_000_000,
    }

    captured_ns, captured_added = simulator.visible_ts_ns(
        row, mode="captured", rng=random.Random(1)
    )
    p50_ns, p50_delay = simulator.visible_ts_ns(
        row, mode="profile_p50", rng=random.Random(1)
    )
    zero_ns, zero_delay = simulator.visible_ts_ns(
        row, mode="exchange_zero", rng=random.Random(1)
    )
    empirical_a = simulator.visible_ts_ns(
        row, mode="profile_empirical", rng=random.Random(7)
    )
    empirical_b = simulator.visible_ts_ns(
        row, mode="profile_empirical", rng=random.Random(7)
    )

    assert captured_ns == 1_005_000_000
    assert captured_added == 0.0
    assert p50_ns == 1_025_000_000
    assert p50_delay == pytest.approx(25.0)
    assert zero_ns == 1_000_000_000
    assert zero_delay == 0.0
    assert empirical_a == empirical_b

    unprofiled = dict(row, market_id="binance:spot:USDCUSDT")
    fallback_ns, fallback_delay = simulator.visible_ts_ns(
        unprofiled, mode="profile_p99", rng=random.Random(1)
    )
    assert fallback_ns == unprofiled["feature_ready_ts_ns"]
    assert fallback_delay != fallback_delay


def test_stable_spike_mode_keeps_core_and_tail_sampling_explicit():
    group = {
        "market_id": "binance:perp:BTCUSDT",
        "event_type": "book",
        "transport": "websocket",
        "simulation_quantile_probabilities": [0.0, 0.95, 0.99, 1.0],
        "simulation_visibility_lag_ms_quantiles": [10.0, 20.0, 100.0, 1000.0],
    }
    simulator = MarketDataLatencySimulator(
        {
            "schema": "market_data_latency_profile.v1",
            "profile_id": "provider_neutral_stable_spike_test",
            "groups": [group],
        }
    )
    row = {
        "market_id": "binance:perp:BTCUSDT",
        "event_type": "book",
        "transport": "websocket",
    }

    class StubRng:
        def __init__(self, values):
            self.values = iter(values)

        def random(self):
            return next(self.values)

        def uniform(self, low, high):
            return low + (high - low) * next(self.values)

    core = simulator.delay_ms(
        row,
        mode="profile_stable_spike",
        rng=StubRng([0.9, 0.5]),
    )
    spike = simulator.delay_ms(
        row,
        mode="profile_stable_spike",
        rng=StubRng([0.0, 0.5]),
    )

    assert 10.0 <= core <= 20.0
    assert 20.0 < spike < 100.0
