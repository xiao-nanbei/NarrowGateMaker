import gzip
import json
import random
import sys

import numpy as np
import pytest

from research.system_engineering.audit.market_data_latency import (
    MarketDataLatencySimulator,
    RawWindowMarketDataLatencySimulator,
    build_latency_profile,
    main,
)


def _raw_window_manifest(tmp_path, *, negative=False, missing=False):
    arrays, windows = {}, []
    for index, day in enumerate(["2026-01-01", "2026-01-01", "2026-01-02"]):
        prefix = f"w{index:03}"
        origin = 1_800_000_000_000_000_000
        exchange = origin + np.array([10, 10, 20, 20, 30, 30], dtype=np.int64)
        transport = np.array([1, 11, 90, 100, 2, 12]) + index
        callback = np.array([3, 13, 7, 17, 4, 14])
        arrays.update({
            f"{prefix}_event_type": np.array([1, 2, 1, 2, 1, 2], dtype=np.uint8),
            f"{prefix}_exchange_ts_ns": exchange,
            f"{prefix}_receive_ts_ns": exchange + transport,
            f"{prefix}_ready_ts_ns": exchange + transport + callback,
            f"{prefix}_timestamp_mask": np.full(6, 7 if missing else 15, dtype=np.uint8),
        })
        if negative:
            arrays[f"{prefix}_receive_ts_ns"][0] = exchange[0] - 1
        windows.append({"utc_day": day, "array_prefix": prefix,
                        "relative_origin_receive_ns": origin})
    np.savez_compressed(tmp_path / "samples.npz", **arrays)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"schema": "ng_source_visibility_messages.v1",
                               "samples_path": "samples.npz", "windows": windows}))
    return path


def test_raw_window_chronology_pairing_shared_source_and_chunk_determinism(tmp_path):
    path = _raw_window_manifest(tmp_path)
    simulator = RawWindowMarketDataLatencySimulator(path, seed=42)
    hour = 500_000 * simulator.BLOCK_NS
    events = hour + np.array([10, 15, 20, 25, 30], dtype=np.int64)
    receive, ready = simulator.align(events, source="book")
    selected = simulator.diagnostics["blocks"][str(hour)]["array_prefix"]
    index = int(selected[1:])
    np.testing.assert_array_equal(receive - events, np.array([1, 1, 90, 90, 2]) + index)
    np.testing.assert_array_equal(ready - receive, [3, 3, 7, 7, 4])
    # The raw chronology includes a recovering spike. Delivery, not sampling,
    # owns HOL; do not hide this by making ready monotone here.
    assert ready[-1] < ready[-2]
    trade_rx, trade_ready = simulator.align(events, source="trade")
    np.testing.assert_array_equal(trade_rx - receive, np.full(5, 10))
    np.testing.assert_array_equal(trade_ready - trade_rx, [13, 13, 17, 17, 14])
    other = RawWindowMarketDataLatencySimulator(path, seed=42)
    pieces = [other.align(part, source="book") for part in [events[:2], events[2:]]]
    np.testing.assert_array_equal(np.concatenate([part[0] for part in pieces]), receive)
    np.testing.assert_array_equal(np.concatenate([part[1] for part in pieces]), ready)


def test_raw_window_edges_and_equal_day_window_selection(tmp_path):
    path = _raw_window_manifest(tmp_path)
    simulator = RawWindowMarketDataLatencySimulator(path, seed=7)
    hour = 500_000 * simulator.BLOCK_NS
    events = hour + np.array([1, 40], dtype=np.int64)
    with pytest.raises(ValueError, match="support"):
        simulator.align(events, source="book")
    held = RawWindowMarketDataLatencySimulator(path, seed=7, edge_policy="hold")
    receive, ready = held.align(events, source="book")
    assert np.all(receive > events)
    np.testing.assert_array_equal(ready - receive, [3, 4])
    assert held.diagnostics["sources"]["book"] == {
        "events": 2, "edge_held_events": 2, "max_extrapolation_ns": 10,
    }
    # Verify the specified hierarchical draw, not a message/window weighted CDF.
    for block in range(10):
        rng = random.Random(f"7:{block}")
        day = ["2026-01-01", "2026-01-02"][rng.randrange(2)]
        prefixes = ["w000", "w001"] if day.endswith("01") else ["w002"]
        assert held._window(block)["array_prefix"] == prefixes[rng.randrange(len(prefixes))]


@pytest.mark.parametrize("negative,missing", [(True, False), (False, True)])
def test_raw_window_rejects_unaligned_data_and_unmeasured_sources(tmp_path, negative, missing):
    simulator = RawWindowMarketDataLatencySimulator(
        _raw_window_manifest(tmp_path, negative=negative, missing=missing), seed=1,
    )
    events = np.array([500_000 * simulator.BLOCK_NS + 10], dtype=np.int64)
    with pytest.raises(ValueError, match="negative|unaligned"):
        simulator.align(events, source="book")
    with pytest.raises(ValueError, match="depth"):
        simulator.align(events, source="depth")
    with pytest.raises(ValueError, match="integer"):
        simulator.align(events.astype(float), source="book")
    with pytest.raises(ValueError, match="regressed"):
        simulator.align(np.array([20, 10]), source="book")


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


def test_source_stratified_sampling_preserves_joint_congestion_and_weights_days(
    tmp_path,
):
    market_id = "binance:perp:BTCUSDC"
    day_one_ns = 1_800_000_000_100_000_000
    day_two_ns = day_one_ns + 86_400_000_000_000

    def write_window(name, ts_ns, book_lag_ms, trade_lag_ms, extra_book_rows=0):
        path = tmp_path / f"{name}.jsonl.gz"
        rows = [
            _row(
                ts_ns,
                lag_ms=book_lag_ms,
                market_id=market_id,
                event_type="book",
            ),
            _row(
                ts_ns,
                lag_ms=trade_lag_ms,
                market_id=market_id,
                event_type="trade",
            ),
        ]
        rows.extend(
            _row(
                ts_ns,
                lag_ms=book_lag_ms,
                market_id=market_id,
                event_type="book",
            )
            for _ in range(extra_book_rows)
        )
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            handle.writelines(json.dumps(row) + "\n" for row in rows)
        return path

    window_a = write_window("day1_a", day_one_ns, 10.0, 100.0, extra_book_rows=50)
    window_b = write_window("day1_b", day_one_ns + 10_000_000_000, 20.0, 200.0)
    window_c = write_window("day2_a", day_two_ns, 30.0, 300.0)
    windows = [
        ("window_a", [window_a]),
        ("window_b", [window_b]),
        ("window_c", [window_c]),
    ]
    profile = build_latency_profile(
        [window_a, window_b, window_c],
        profile_id="source_stratified_test",
        environment={"authority": "diagnostic"},
        source_windows=windows,
        joint_bucket_ms=1_000,
    )

    sampling = profile["source_stratified_sampling"]
    assert sampling["authority"] == "diagnostic_non_authoritative"
    assert sampling["promotion_eligible"] is False
    assert sampling["weighting"] == "utc_day_equal_then_window_equal_then_bucket_equal"
    assert profile["measurement"]["source_stratified_joint_sample_count"] == 3
    assert profile["measurement"]["start_receive_ts_ns"] == day_one_ns + 10_000_000
    assert (
        profile["measurement"]["end_receive_ts_ns"]
        == day_two_ns + 300_000_000
    )

    simulator = MarketDataLatencySimulator(profile)
    book_row = {
        "market_id": market_id,
        "event_type": "book",
        "transport": "websocket",
    }
    trade_row = {**book_row, "event_type": "trade"}

    class StubRng:
        def __init__(self, values):
            self.values = iter(values)

        def random(self):
            return next(self.values)

    # Day one is selected first, then its second window. Reusing the same draw
    # for each event type selects the two components of one congestion state.
    book_delay = simulator.delay_ms(
        book_row,
        mode="profile_source_stratified",
        rng=StubRng([0.1, 0.9, 0.1]),
    )
    trade_delay = simulator.delay_ms(
        trade_row,
        mode="profile_source_stratified",
        rng=StubRng([0.1, 0.9, 0.1]),
    )
    assert book_delay == pytest.approx(20.25)
    assert trade_delay == pytest.approx(200.25)

    # The second UTC day receives half the probability despite having only one
    # window and far fewer raw book events than day one.
    assert simulator.delay_ms(
        book_row,
        mode="profile_source_stratified",
        rng=StubRng([0.9, 0.1, 0.1]),
    ) == pytest.approx(30.25)


def test_source_stratified_profile_rejects_unpaired_samples():
    with pytest.raises(ValueError, match="must be paired"):
        MarketDataLatencySimulator(
            {
                "schema": "market_data_latency_profile.v1",
                "profile_id": "bad_joint_profile",
                "groups": [],
                "source_stratified_sampling": {
                    "schema": "market_data_latency_source_stratified.v1",
                    "joint_bucket_ms": 1_000,
                    "sources": [
                        {
                            "market_id": "binance:perp:BTCUSDC",
                            "transport": "websocket",
                            "strata": [
                                {
                                    "utc_day": "2026-08-26",
                                    "window_id": "window_a",
                                    "book_visibility_lag_ms_samples": [10.0],
                                    "trade_visibility_lag_ms_samples": [],
                                }
                            ],
                        }
                    ],
                },
            }
        )


def test_source_stratified_cli_treats_each_input_as_an_independent_window(
    tmp_path,
    monkeypatch,
):
    ts_ns = 1_800_000_000_100_000_000
    inputs = []
    for index, lag_ms in enumerate((10.0, 20.0), 1):
        path = tmp_path / f"window_{index}.jsonl.gz"
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            handle.write(json.dumps(_row(ts_ns, lag_ms=lag_ms)) + "\n")
            handle.write(
                json.dumps(_row(ts_ns, lag_ms=lag_ms * 2, event_type="trade"))
                + "\n"
            )
        inputs.append(path)
    output = tmp_path / "profile.json"
    argv = [
        "market_data_latency.py",
        "--input",
        str(inputs[0]),
        "--input",
        str(inputs[1]),
        "--output-json",
        str(output),
        "--profile-id",
        "cli_source_stratified_test",
        "--source-stratified",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    assert main() == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["measurement"]["independent_window_count"] == 2
    assert payload["measurement"]["source_stratified_joint_sample_count"] == 2
