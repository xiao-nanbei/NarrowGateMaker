from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models import data_windows


def _bounded_books(tmp_path, monkeypatch, day="2026-01-01", count=1000):
    import hashlib
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq
    from models import backtest_tick as bt

    start = int(pd.Timestamp(day, tz="UTC").value // 1_000_000)
    axis = start + np.arange(count) * 100
    observed = start * 1000 + (np.arange(count) // 3 * 300) * 1000
    for name in ("bbo", "l2", "clock", "quality"):
        (tmp_path / name).mkdir(exist_ok=True)
    paths = {name: tmp_path / name / f"BTCUSDC-{name}-{day}.parquet"
             for name in ("bbo", "l2", "clock")}
    bbo = pd.DataFrame({"timestamp": axis, "best_bid": 100., "best_ask": 101.,
                        "bid_qty": 1., "ask_qty": 2.})
    l2 = pd.DataFrame({"timestamp": axis})
    for level in range(1, 21):
        for side, field, value in (("bid", "px", 101. - level), ("ask", "px", 100. + level),
                                   ("bid", "qty", 1.), ("ask", "qty", 2.)):
            l2[f"{side}_{field}_{level}"] = value
    # A malformed legacy L2 point is dropped; the previous valid snapshot
    # must survive even when the immediately preceding raw row is invalid.
    l2.loc[3, "ask_px_1"] = 99.
    bbo.loc[4, "best_ask"] = 99.
    usable = np.ones(count, dtype=bool)
    usable[4] = False
    kinds = np.where(np.arange(count) % 3 == 0, "source_observed", "carried_forward")
    top_kinds = kinds.astype(object)
    top_kinds[4] = "invalid_top"
    clock = pd.DataFrame({"timestamp": axis, "last_observation_timestamp_us": observed,
                          "observation_age_us": axis * 1000 - observed, "observation_kind": kinds,
                          "bbo_last_observation_timestamp_us": observed,
                          "bbo_observation_age_us": axis * 1000 - observed,
                          "bbo_observation_kind": top_kinds, "bbo_usable": usable})
    for name, frame in (("bbo", bbo), ("l2", l2), ("clock", clock)):
        pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), paths[name], row_group_size=32)
    quality = {"day": day, "symbol": "BTCUSDC", **{
        f"{name}_output": {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        for name, path in paths.items()}}
    (tmp_path / "quality" / f"BTCUSDC-{day}.json").write_text(json.dumps(quality))
    monkeypatch.setattr(bt, "SYMBOL", "BTCUSDC")
    monkeypatch.setattr(bt, "BBO_DIR", tmp_path / "bbo")
    monkeypatch.setattr(bt, "L2_DIR", tmp_path / "l2")
    monkeypatch.setattr(bt, "filter_paths_for_orderbook_quality", lambda paths, *a, **k: paths)
    monkeypatch.setattr(bt, "allowed_timestamp_mask", lambda ts, *a, **k: np.ones(len(ts), dtype=bool))
    return start, paths


@pytest.mark.parametrize("bounds", [(350, 850), (50_050, 50_650), (0, 300)])
def test_bounded_book_and_clock_equal_full_then_slice_without_full_payload(tmp_path, monkeypatch, bounds):
    import pyarrow.parquet as pq
    from models import backtest_tick as bt

    start, _ = _bounded_books(tmp_path, monkeypatch)
    bounds = tuple(start + value for value in bounds)
    full = {kind: getattr(bt, f"load_{kind}_data")(["2026-01-01"]) for kind in ("bbo", "l2")}
    actual_factory = pq.ParquetFile
    payload_rows = []

    class SpyParquet:
        def __init__(self, *args, **kwargs):
            self.inner = actual_factory(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(self.inner, name)

        def iter_batches(self, **kwargs):
            assert kwargs["batch_size"] <= 65_536
            for batch in self.inner.iter_batches(**kwargs):
                if kwargs.get("row_groups") is not None:
                    payload_rows.append(len(batch))
                yield batch

    monkeypatch.setattr(pq, "ParquetFile", SpyParquet)
    monkeypatch.setattr(pq, "read_table", lambda *a, **k: pytest.fail("unbounded table read"))
    monkeypatch.setattr(pd, "read_parquet", lambda *a, **k: pytest.fail("unbounded pandas read"))
    for kind in ("bbo", "l2"):
        actual = getattr(bt, f"load_{kind}_data")(["2026-01-01"], time_bounds_ms=bounds)
        expected = data_windows.slice_history_object(full[kind], *bounds, keep_predecessor=True)
        for name in ("ts_ms", "observation_ts_us", "source_observed",
                     *(('best_bid', 'best_ask', 'usable') if kind == "bbo" else ('bid_px', 'ask_px', 'bid_qty', 'ask_qty'))):
            np.testing.assert_array_equal(getattr(actual, name), getattr(expected, name))
        assert np.all(actual.observation_ts_us <= actual.ts_ms * 1000)
    assert payload_rows and max(payload_rows) <= 32
    assert sum(payload_rows) < 1000  # not even one full daily payload, across both kinds/clock


def test_bounded_books_keep_real_previous_day_clock(tmp_path, monkeypatch):
    from models import backtest_tick as bt
    day1 = "2026-01-01"
    start, _ = _bounded_books(tmp_path, monkeypatch, day1, count=10)
    _bounded_books(tmp_path, monkeypatch, "2026-01-02", count=10)
    boundary = start + 86_400_000
    for kind in ("bbo", "l2"):
        full = getattr(bt, f"load_{kind}_data")([day1, "2026-01-02"])
        actual = getattr(bt, f"load_{kind}_data")([day1, "2026-01-02"], time_bounds_ms=(boundary, boundary + 300))
        expected = data_windows.slice_history_object(full, boundary, boundary + 300, keep_predecessor=True)
        np.testing.assert_array_equal(actual.ts_ms, expected.ts_ms)
        np.testing.assert_array_equal(actual.observation_ts_us, expected.observation_ts_us)
        assert actual.ts_ms[0] < boundary
        assert actual.observation_ts_us[0] < boundary * 1000


@pytest.mark.parametrize("fail_clock", [False, True])
def test_bounded_l2_opens_each_fragmented_footer_once_and_closes_before_next_day(tmp_path, monkeypatch, fail_clock):
    from collections import Counter
    import pyarrow.parquet as pq
    from models import backtest_tick as bt

    start, _ = _bounded_books(tmp_path, monkeypatch, "2026-01-01", count=2048)
    _bounded_books(tmp_path, monkeypatch, "2026-01-02", count=2048)
    actual_factory = pq.ParquetFile
    opens, closes = Counter(), Counter()
    active = set()

    class ScopedSpy:
        def __init__(self, path, **kwargs):
            self.path = Path(path)
            self.is_depth = self.path.parent.name == "l2"
            if self.is_depth:
                assert not active, "two daily L2 footers retained concurrently"
                active.add(self.path)
                opens[self.path.name] += 1
            self.inner = actual_factory(path, **kwargs)

        def __getattr__(self, name):
            return getattr(self.inner, name)

        def close(self):
            self.inner.close()
            if self.is_depth:
                active.remove(self.path)
                closes[self.path.name] += 1

    monkeypatch.setattr(pq, "ParquetFile", ScopedSpy)
    if fail_clock:
        monkeypatch.setattr(bt, "_load_book_observations",
                            lambda *a, **k: (_ for _ in ()).throw(ValueError("clock mismatch")))
        with pytest.raises(ValueError, match="clock mismatch"):
            bt.load_l2_data(["2026-01-01", "2026-01-02"],
                           time_bounds_ms=(start + 86_400_000, start + 86_400_300))
        assert len(opens) == 1
    else:
        result = bt.load_l2_data(["2026-01-01", "2026-01-02"],
                                time_bounds_ms=(start + 86_400_000, start + 86_400_300))
        assert result.bid_px.shape == (4, 20)
        assert len(opens) == 2
    assert opens == closes and all(value == 1 for value in opens.values())
    assert not active


@pytest.mark.parametrize("fault", ["future", "carried_refresh"])
def test_bounded_clock_rejects_future_and_refreshed_carry(tmp_path, monkeypatch, fault):
    import hashlib
    import json
    from models import backtest_tick as bt
    start, paths = _bounded_books(tmp_path, monkeypatch)
    clock = pd.read_parquet(paths["clock"])
    observed = (start + 500) * 1000 + 1 if fault == "future" else (start + 450) * 1000
    clock.loc[5, "last_observation_timestamp_us"] = observed
    clock.loc[5, "observation_age_us"] = (start + 500) * 1000 - observed
    clock.to_parquet(paths["clock"], index=False)
    quality_path = tmp_path / "quality" / "BTCUSDC-2026-01-01.json"
    quality = json.loads(quality_path.read_text())
    quality["clock_output"]["sha256"] = hashlib.sha256(paths["clock"].read_bytes()).hexdigest()
    quality_path.write_text(json.dumps(quality))
    with pytest.raises(ValueError, match="future|refreshed"):
        bt.load_l2_data(["2026-01-01"], time_bounds_ms=(start + 450, start + 650))


def test_delivery_prefix_reads_only_level_one_and_bound_clocks(tmp_path, monkeypatch):
    from models import backtest_tick as bt
    start, _ = _bounded_books(tmp_path, monkeypatch)
    for kind in ("bbo", "l2"):
        full = getattr(bt, f"load_{kind}_data")(["2026-01-01"])
        calls = []
        original = bt._read_parquet_rows
        def narrow(path, rows, *, columns=None, calls=calls, original=original):
            assert columns is not None
            assert not any(name.endswith("_2") for name in columns)
            calls.append(tuple(columns))
            return original(path, rows, columns=columns)
        with monkeypatch.context() as patch:
            patch.setattr(bt, "_read_parquet_rows", narrow)
            prefix = bt.load_book_delivery_prefix(["2026-01-01"], kind, start + 50_000)
        expected = full.ts_ms < start + 50_000
        np.testing.assert_array_equal(prefix["ts_ms"], full.ts_ms[expected])
        np.testing.assert_array_equal(prefix["observation_ts_us"], full.observation_ts_us[expected])
        assert calls


def test_f01_runtime_bound_is_passed_before_daily_load(monkeypatch):
    from research.families.f01_fixed_parameter_racing import campaign_outcome_replay_audit as audit
    from tests.test_python_planned_maintenance_replay import _params
    start = int(pd.Timestamp("2026-01-01", tz="UTC").value // 1_000_000)
    class LoadedBounded(Exception):
        pass
    def load(day, params):
        assert day == "2026-01-01"
        assert params["_runtime_input_bounds_ms"] == (start, start + 3001)
        assert params["replay_event_clock_start_ts_ms"] == start
        assert params["replay_event_clock_end_ts_ms"] == start + 86_400_000 - 1
        raise LoadedBounded
    monkeypatch.setattr(audit.smoke, "_load_window", load)
    monkeypatch.setattr(audit.bt, "configure_symbol", lambda *a, **k: None)
    with pytest.raises(LoadedBounded):
        audit._run_day_campaign_audit(day="2026-01-01", symbol="BTCUSDC", base=_params(),
            arms=[audit.smoke.SmokeArm("B", "test", {}, "")], engine="python", day_initial={},
            day_live_state=None, use_initial_state=False, continuous_days=["2026-01-01"],
            replay_end_ts_ms=start + 4000, runtime_input_bounds=(start, start + 3000),
            checkpoint_at_ts_ms=start + 2000)


def test_bounded_legacy_large_bbo_keeps_bucket_reduction(tmp_path, monkeypatch):
    from models import backtest_tick as bt
    start, _ = _bounded_books(tmp_path, monkeypatch)
    monkeypatch.setattr(bt, "HISTORICAL_BBO_STREAM_THRESHOLD_BYTES", 0)
    full = bt.load_bbo_data(["2026-01-01"])
    actual = bt.load_bbo_data(["2026-01-01"], time_bounds_ms=(start + 1350, start + 2550))
    expected = data_windows.slice_history_object(full, start + 1350, start + 2550, keep_predecessor=True)
    np.testing.assert_array_equal(actual.ts_ms, expected.ts_ms)
    np.testing.assert_array_equal(actual.observation_ts_us, expected.observation_ts_us)


@pytest.mark.parametrize("suffix", ["parquet", "csv", "csv.gz"])
def test_bounded_execution_read_preserves_simultaneous_ids_and_right_open_boundary(tmp_path, monkeypatch, suffix):
    from models import backtest_tick as bt
    start = int(pd.Timestamp("2026-01-01", tz="UTC").value // 1_000_000)
    raw = pd.DataFrame({"id": [10, 11, 12, 13, 14], "time": start + np.array([0, 100, 100, 199, 200]),
                        "price": [100., 101., 102., 103., 104.], "qty": [.1, .2, .3, .4, .5],
                        "is_buyer_maker": [True, True, False, False, True]})
    path = tmp_path / f"source.{suffix}"
    if suffix == "parquet":
        raw.rename(columns={"time": "timestamp"}).assign(timestamp=raw.time * 1000).to_parquet(path)
    else:
        raw.to_csv(path, index=False)
    full = bt._read_individual_trade_csv(path)
    expected = full[(full.transact_time >= start + 100) & (full.transact_time < start + 200)].reset_index(drop=True)
    if suffix == "parquet":
        monkeypatch.setattr(pd, "read_parquet", lambda *a, **k: pytest.fail("unbounded trade read"))
    actual = bt._read_individual_trade_csv(path, time_bounds_ms=(start + 100, start + 200))
    pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected)
    assert actual.trade_id.tolist() == [11, 12, 13]


def test_ordered_l2_concat_avoids_sort_copies_and_preserves_observation_fields(monkeypatch):
    from models.replay.narrowgate_continuous_tick_adapter import _concat_timed_payloads
    from models.tick_data_types import HistoricalL2Data

    def book(times):
        ts = np.array(times, dtype=np.int64)
        matrix = np.column_stack((ts + 10., ts + 20.))
        return HistoricalL2Data(ts, matrix, matrix + 1, matrix + 2, matrix + 3,
                                observation_ts_us=ts * 1000 - 7,
                                source_observed=np.array([True, False]))

    first, second = book([1, 2]), book([3, 4])
    monkeypatch.setattr(np, "argsort", lambda *a, **k: pytest.fail("ordered concat sorted again"))
    result = _concat_timed_payloads([first, second])
    np.testing.assert_array_equal(result.ts_ms, [1, 2, 3, 4])
    np.testing.assert_array_equal(result.bid_px, np.concatenate([first.bid_px, second.bid_px]))
    np.testing.assert_array_equal(result.observation_ts_us, [993, 1993, 2993, 3993])
    np.testing.assert_array_equal(result.source_observed, [True, False, True, False])
    assert not np.shares_memory(result.bid_px, first.bid_px)


def test_overlapping_bbo_concat_still_uses_last_stable_observation():
    from models.replay.narrowgate_continuous_tick_adapter import _concat_timed_payloads
    from models.tick_data_types import HistoricalBBOData

    first = HistoricalBBOData(np.array([2, 1]), *(np.array([20., 10.]) for _ in range(4)))
    second = HistoricalBBOData(np.array([2, 3]), *(np.array([21., 30.]) for _ in range(4)))
    result = _concat_timed_payloads([first, second])
    np.testing.assert_array_equal(result.ts_ms, [1, 2, 3])
    np.testing.assert_array_equal(result.best_bid, [10., 21., 30.])


@pytest.mark.parametrize("quality_flags", [(True, True), (True, False), (False, True), (True, None)])
def test_contiguous_window_keeps_first_preroll_without_replaying_second_preroll(quality_flags):
    day_ms = 86_400_000
    start = int(pd.Timestamp("2026-01-01", tz="UTC").value // 1_000_000)
    windows = []
    for offset, qualified in zip((0, day_ms), quality_flags, strict=True):
        ts = np.array([start + offset - 1000, start + offset, start + offset + 1000])
        windows.append({
            "trades": pd.DataFrame({"transact_time": ts[1:], "price": [100., 101.]}),
            "var_ts_ms": ts, "var_ssq": np.ones(3), "var_ti": None, "var_retsq": None,
            "bbo_data": data_windows.HistoricalBBOData(
                ts, *(np.ones(3) for _ in range(4)), usable=np.array([False, True, False])),
            "l2_data": None, "ml_data": (ts, {"feature": np.array([9., 10., 11.])}),
            "formal_lifecycle_replay_eligible": qualified,
            "provider_sensitivity_replay_eligible": qualified,
            "exact_queue_policy_eligible": qualified,
        })
    merged = data_windows.concatenate_tick_windows(["2026-01-01", "2026-01-02"], windows)
    expected = np.array([start - 1000, start, start + 1000, start + day_ms, start + day_ms + 1000])
    np.testing.assert_array_equal(merged["var_ts_ms"], expected)
    np.testing.assert_array_equal(merged["bbo_data"].ts_ms, expected)
    np.testing.assert_array_equal(merged["bbo_data"].usable, [False, True, False, True, False])
    np.testing.assert_array_equal(merged["ml_data"][0], expected)
    np.testing.assert_array_equal(merged["ml_data"][1]["feature"], [9, 10, 11, 10, 11])
    assert len(merged["trades"]) == 4
    assert len(windows[1]["var_ts_ms"]) == 3  # inputs not mutated
    for name in ("formal_lifecycle_replay_eligible", "provider_sensitivity_replay_eligible",
                 "exact_queue_policy_eligible"):
        assert merged[name] is all(value is True for value in quality_flags)
        assert [row[name] for row in merged["book_quality_by_day"].values()] == list(quality_flags)
    windows[0]["book_source_authority"] = "provider_ordered"
    windows[1]["book_source_authority"] = "exchange_sequence"
    with pytest.raises(ValueError, match="disagree on book_source_authority"):
        data_windows.concatenate_tick_windows(["2026-01-01", "2026-01-02"], windows)
    mixed = data_windows.concatenate_tick_windows(["2026-01-01", "2026-01-02"], windows,
                                                  allow_mixed_book_sources=True)
    assert mixed["book_source_authority"] == "mixed_explicit_daily_sources"
    assert [row["book_source_authority"] for row in mixed["book_quality_by_day"].values()] == [
        "provider_ordered", "exchange_sequence"]
    np.testing.assert_array_equal(mixed["bbo_data"].ts_ms, merged["bbo_data"].ts_ms)
    windows[1]["execution_trade_source"] = "incompatible"
    with pytest.raises(ValueError, match="disagree on execution_trade_source"):
        data_windows.concatenate_tick_windows(["2026-01-01", "2026-01-02"], windows)


@pytest.mark.parametrize("days", [[], ["2026-01-02", "2026-01-01"],
                                 ["2026-01-01", "2026-01-03"]])
def test_contiguous_window_rejects_missing_or_reversed_days(days):
    with pytest.raises(ValueError, match="contiguous"):
        data_windows.concatenate_tick_windows(days, [{} for _ in days])


def test_manifest_backed_quality_days_reach_every_window_loader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed = ("2026-05-06", "2026-05-07")
    observed: dict[str, tuple[str, ...]] = {}
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray([0, 1000], dtype=np.int64),
            "price": np.asarray([100.0, 100.1]),
        }
    )
    bars = pd.DataFrame(
        {"close": [100.0, 100.1], "trade_count": [1.0, 1.0]},
        index=pd.Index([0, 1000], dtype=np.int64),
    )

    def capture(name, value):
        def loader(*_args, quality_allowed_days=(), **_kwargs):
            observed[name] = tuple(quality_allowed_days)
            return value

        return loader

    monkeypatch.setattr(
        data_windows.bt,
        "load_execution_trades",
        capture("trades", trades),
    )
    monkeypatch.setattr(
        data_windows.bt,
        "load_1s_bars",
        capture("bars", bars),
    )
    monkeypatch.setattr(
        data_windows.bt,
        "load_bbo_data",
        capture("bbo", object()),
    )
    monkeypatch.setattr(
        data_windows.bt,
        "load_l2_data",
        capture("l2", object()),
    )
    monkeypatch.setattr(
        data_windows.bt,
        "build_rolling_variance",
        lambda _bars: (np.asarray([0]), np.asarray([1.0])),
    )
    monkeypatch.setattr(
        data_windows.bt,
        "build_trade_intensity",
        lambda _bars: (np.asarray([0]), np.asarray([1.0])),
    )
    monkeypatch.setattr(
        data_windows.bt,
        "build_squared_returns",
        lambda _bars: (np.asarray([0]), np.asarray([0.0])),
    )

    data_windows.load_tick_window(
        "2026-05-07",
        {
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "_formal_quality_allowed_days": list(allowed),
            "_formal_quality_day_manifest_sha256": "frozen-sha",
        },
        load_ml=False,
        require_ml=False,
        require_historical_bbo=False,
    )

    assert observed == {
        "trades": allowed,
        "bars": allowed,
        "bbo": allowed,
        "l2": allowed,
    }


def test_formal_gate_covers_loaded_market_context_days(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_root = tmp_path / "normalized_l2_100ms_v2"
    monkeypatch.setattr(data_windows.bt, "BBO_DIR", dataset_root / "bbo")
    monkeypatch.setattr(data_windows.bt, "L2_DIR", dataset_root / "l2")
    observed: dict[str, object] = {}

    def capture(
        root: Path,
        days: list[str],
        *,
        verify_hashes: bool,
    ) -> None:
        observed.update(
            root=root,
            days=days,
            verify_hashes=verify_hashes,
        )
        raise RuntimeError("formal gate observed")

    monkeypatch.setattr(
        data_windows.l2_registry,
        "require_formal_days",
        capture,
    )

    with pytest.raises(RuntimeError, match="formal gate observed"):
        data_windows.load_tick_window(
            "2026-01-02",
            {
                "market_context_warmup_days": 1,
                "require_formal_l2": True,
            },
            load_ml=False,
            require_ml=False,
            require_formal_l2=True,
            verify_formal_l2_hashes=True,
        )

    assert observed == {
        "root": dataset_root.resolve(),
        "days": ["2026-01-01", "2026-01-02"],
        "verify_hashes": True,
    }
