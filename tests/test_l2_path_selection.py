from pathlib import Path

import pandas as pd
import pytest

from features import feature_engineer as fe
from models import backtest_tick as bt


def _observed_book_fixture(root, *, legacy_clock=False):
    import hashlib
    import json
    import numpy as np
    day = "2026-09-01"
    axis = int(pd.Timestamp(day, tz="UTC").value // 1_000_000) + np.array([100, 200, 300])
    observed = np.full(3, axis[0] * 1000 - 500, dtype=np.int64)
    quality = {"day": day, "symbol": "BTCUSDC", "gap_policy": "carry_forward"}
    for kind in ("bbo", "l2", "clock", "quality"):
        (root / kind).mkdir(parents=True, exist_ok=True)
    bbo = pd.DataFrame({"timestamp": axis, "best_bid": 99.9, "best_ask": 100.1,
                        "bid_qty": 1., "ask_qty": 1.})
    l2 = pd.DataFrame({"timestamp": axis, "bid_px_1": 99.9, "ask_px_1": 100.1,
                       "bid_qty_1": 1., "ask_qty_1": 1.})
    clock = pd.DataFrame({"timestamp": axis, "exchange_cut_timestamp_us": observed,
                          "exchange_resample_age_us": axis * 1000 - observed})
    if not legacy_clock:
        clock["last_observation_timestamp_us"] = observed
        clock["observation_age_us"] = axis * 1000 - observed
        clock["observation_kind"] = ["source_observed", "carried_forward", "carried_forward"]
    for kind, frame in (("bbo", bbo), ("l2", l2), ("clock", clock)):
        path = root / kind / f"BTCUSDC-{kind}-{day}.parquet"
        frame.to_parquet(path, index=False)
        quality[f"{kind}_output"] = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    quality_path = root / "quality" / f"BTCUSDC-{day}.json"
    quality_path.write_text(json.dumps(quality))
    return day, axis, observed, quality_path


@pytest.mark.parametrize("legacy_clock", [False, True])
def test_bound_source_observation_survives_book_loading_and_slicing(tmp_path, monkeypatch, legacy_clock):
    import numpy as np
    from models import data_windows
    from models.tick_data_types import book_observation_status
    day, axis, observed, _ = _observed_book_fixture(tmp_path, legacy_clock=legacy_clock)
    monkeypatch.setattr(bt, "BBO_DIR", tmp_path / "bbo")
    monkeypatch.setattr(bt, "L2_DIR", tmp_path / "l2")
    for book in (bt.load_bbo_data([day], quality_allowed_days=[day]), bt.load_l2_data([day], quality_allowed_days=[day])):
        np.testing.assert_array_equal(book.ts_ms, axis)
        np.testing.assert_array_equal(book.observation_ts_us, observed)
        assert book_observation_status(book) == "BOUND"
        sliced = data_windows.slice_history_object(book, axis[1], axis[-1] + 1)
        np.testing.assert_array_equal(sliced.observation_ts_us, observed[1:])


@pytest.mark.parametrize("defect", ["future", "age", "refresh", "axis", "book_hash", "clock_hash"])
def test_bound_book_rejects_invalid_observation_or_content(tmp_path, defect):
    import hashlib
    import json
    day, axis, _, quality_path = _observed_book_fixture(tmp_path)
    quality = json.loads(quality_path.read_text())
    clock_path = Path(quality["clock_output"]["path"])
    clock = pd.read_parquet(clock_path)
    if defect == "future":
        clock["last_observation_timestamp_us"] = axis * 1000 + 1
    elif defect == "age":
        clock.loc[1, "observation_age_us"] += 1
    elif defect == "refresh":
        clock["last_observation_timestamp_us"] = axis * 1000
        clock["observation_age_us"] = 0
    elif defect == "axis":
        clock.loc[2, "timestamp"] += 1
        clock.loc[2, "observation_age_us"] += 1000
    clock.to_parquet(clock_path, index=False)
    quality["clock_output"]["sha256"] = hashlib.sha256(clock_path.read_bytes()).hexdigest()
    if defect in {"book_hash", "clock_hash"}:
        quality["bbo_output" if defect == "book_hash" else "clock_output"]["sha256"] = "0" * 64
    quality_path.write_text(json.dumps(quality))
    with pytest.raises(ValueError):
        bt._load_book_observations(tmp_path / "bbo" / f"BTCUSDC-bbo-{day}.parquet", axis, "bbo")


def test_missing_observation_clock_is_unknown_not_freshness_proof(tmp_path, monkeypatch):
    import numpy as np
    from models.tick_data_types import book_observation_status, book_observation_times_us
    day, axis, _, quality = _observed_book_fixture(tmp_path)
    quality.unlink()
    monkeypatch.setattr(bt, "BBO_DIR", tmp_path / "bbo")
    book = bt.load_bbo_data([day], quality_allowed_days=[day])
    assert book_observation_status(book) == "UNKNOWN"
    assert book.observation_ts_us.tolist() == [-1, -1, -1]
    np.testing.assert_array_equal(book_observation_times_us(book), axis * 1000)


def test_clock_content_changes_cache_identity_even_with_preserved_mtime(tmp_path, monkeypatch):
    import os
    from models import data_windows
    day, _, _, _ = _observed_book_fixture(tmp_path)
    monkeypatch.setattr(bt, "BBO_DIR", tmp_path / "bbo")
    monkeypatch.setattr(bt, "L2_DIR", tmp_path / "l2")
    path = tmp_path / "clock" / f"BTCUSDC-clock-{day}.parquet"
    before = data_windows._book_observation_content_identity(day, 0)
    stat = path.stat()
    content = bytearray(path.read_bytes())
    content[20] ^= 1
    path.write_bytes(content)
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert data_windows._book_observation_content_identity(day, 0) != before


@pytest.mark.parametrize("kind", ["bbo", "l2"])
def test_cross_midnight_stitch_keeps_prior_day_observation(kind):
    from dataclasses import replace
    import numpy as np
    from models.tick_data_types import HistoricalBBOData, HistoricalL2Data
    from models.replay.narrowgate_continuous_tick_adapter import _concat_timed_payloads
    from models.replay.f05_ema_add_wait_two_day_window import _stitch_bbo, _stitch_l2
    midnight = int(pd.Timestamp("2026-09-01", tz="UTC").value // 1_000_000)
    before = np.array([midnight - 200, midnight - 100])
    observed = np.full(2, (midnight - 201) * 1000)
    if kind == "bbo":
        first = HistoricalBBOData(before, np.full(2, 99.9), np.full(2, 100.1), np.ones(2), np.ones(2),
                                  observation_ts_us=observed, source_observed=np.array([True, False]))
    else:
        first = HistoricalL2Data(before, np.full((2, 1), 99.9), np.ones((2, 1)),
                                 np.full((2, 1), 100.1), np.ones((2, 1)),
                                 observation_ts_us=observed, source_observed=np.array([True, False]))
    second = replace(first, ts_ms=np.array([midnight, midnight + 100]), source_observed=np.zeros(2, dtype=bool))
    for stitched in (_concat_timed_payloads([first, second]),
                     (_stitch_bbo if kind == "bbo" else _stitch_l2)(first, second)):
        np.testing.assert_array_equal(stitched.observation_ts_us, np.r_[observed, observed])
        assert stitched.source_observed.tolist() == [True, False, False, False]
        assert stitched.ts_ms[-1] == midnight + 100


@pytest.mark.parametrize("first_mask,second_mask", [
    (None, None), (None, [False, True]), ([True, False], None),
    ([True, False], [False, True]),
])
def test_bbo_usability_survives_cross_day_concat_and_prefix_stitch(first_mask, second_mask):
    import numpy as np
    from models.tick_data_types import HistoricalBBOData, book_usable_mask
    from models.replay.narrowgate_continuous_tick_adapter import _concat_timed_payloads
    from models.replay.f05_ema_add_wait_two_day_window import _stitch_bbo

    def book(axis, mask):
        return HistoricalBBOData(np.array(axis), *(np.ones(2) for _ in range(4)),
                                 usable=None if mask is None else np.array(mask, dtype=bool))

    first, second = book([100, 200], first_mask), book([300, 400], second_mask)
    expected = np.r_[book_usable_mask(first), book_usable_mask(second)]
    for combined in (_concat_timed_payloads([second, first]), _stitch_bbo(first, second)):
        np.testing.assert_array_equal(combined.ts_ms, [100, 200, 300, 400])
        np.testing.assert_array_equal(book_usable_mask(combined), expected)
        assert combined.usable is None if first_mask is second_mask is None else combined.usable.dtype == bool


def test_bbo_usability_uses_same_duplicate_selection_and_rejects_changed_frozen_prefix():
    from dataclasses import replace
    import numpy as np
    from models.tick_data_types import HistoricalBBOData
    from models.replay.narrowgate_continuous_tick_adapter import _concat_timed_payloads
    from models.replay.f05_ema_add_wait_two_day_window import F05WindowStitchError, _stitch_bbo
    first = HistoricalBBOData(np.array([100, 200]), *(np.ones(2) for _ in range(4)),
                              usable=np.array([True, False]))
    second = replace(first, ts_ms=np.array([200, 300]), usable=np.array([True, False]))
    combined = _concat_timed_payloads([first, second])
    assert combined.ts_ms.tolist() == [100, 200, 300]
    assert combined.usable.tolist() == [True, True, False]
    with pytest.raises(F05WindowStitchError, match="BBO usable overlap changed"):
        _stitch_bbo(first, second)
    second = replace(second, usable=np.array([False, True]))
    assert _stitch_bbo(first, second).usable.tolist() == [True, False, True]


def test_bbo_usability_slicing_preserves_invalid_predecessor():
    import numpy as np
    from models import data_windows
    from models.tick_data_types import HistoricalBBOData
    from models.replay.narrowgate_continuous_tick_adapter import _slice_timed_payload
    book = HistoricalBBOData(np.array([100, 200, 300, 400]), *(np.ones(4) for _ in range(4)),
                             usable=np.array([True, False, True, False]))
    selected = data_windows.slice_history_object(book, 250, 400)
    assert selected.ts_ms.tolist() == [300] and selected.usable.tolist() == [True]
    for selected in (data_windows.slice_history_object(book, 250, 400, keep_predecessor=True),
                     _slice_timed_payload(book, 250, 400)):
        assert selected.ts_ms.tolist() == [200, 300]
        assert selected.usable.tolist() == [False, True]
    assert book.usable.tolist() == [True, False, True, False]


def test_btcusdc_replay_defaults_to_normalized_l2_root(tmp_path: Path) -> None:
    normalized = tmp_path / "normalized_l2_100ms_v2"

    bbo_dir, l2_dir = bt._default_replay_book_dirs(
        normalized_root=normalized,
        environ={},
    )

    assert bbo_dir == (normalized / "bbo").resolve()
    assert l2_dir == (normalized / "l2").resolve()


def test_replay_book_environment_overrides_remain_authoritative(
    tmp_path: Path,
) -> None:
    bbo_override = tmp_path / "custom-bbo"
    l2_override = tmp_path / "custom-l2"

    bbo_dir, l2_dir = bt._default_replay_book_dirs(
        normalized_root=tmp_path / "normalized",
        environ={
            "MM_BBO_DIR": str(bbo_override),
            "MM_L2_DIR": str(l2_override),
        },
    )

    assert bbo_dir == bbo_override.resolve()
    assert l2_dir == l2_override.resolve()


@pytest.mark.parametrize("key", ["MM_BBO_DIR", "MM_L2_DIR"])
def test_replay_rejects_partial_book_override(
    tmp_path: Path,
    key: str,
) -> None:
    with pytest.raises(ValueError, match="must be set together"):
        bt._default_replay_book_dirs(
            normalized_root=tmp_path / "normalized",
            environ={key: str(tmp_path / key.lower())},
        )


def test_feature_books_route_btcusdc_and_btcusdt_separately(
    tmp_path: Path,
) -> None:
    legacy = tmp_path / "legacy"
    normalized = tmp_path / "normalized_l2_100ms_v2"

    btcusdc = fe._book_dirs_for_symbol(
        "BTCUSDC",
        legacy_root=legacy,
        normalized_root=normalized,
        environ={},
    )
    btcusdt = fe._book_dirs_for_symbol(
        "BTCUSDT",
        legacy_root=legacy,
        normalized_root=normalized,
        environ={},
    )

    assert btcusdc == (
        (normalized / "bbo").resolve(),
        (normalized / "l2").resolve(),
    )
    assert btcusdt == (
        (legacy / "bbo").resolve(),
        (legacy / "l2").resolve(),
    )


def test_reference_trade_bars_survive_unrelated_historical_book_exclusion(tmp_path, monkeypatch):
    from data_quality import excluded_orderbook_days

    day = "2026-03-31"
    assert day in excluded_orderbook_days("BTCUSDT")
    index = pd.date_range(day, periods=3, freq="s", tz="UTC")
    bars = pd.DataFrame({"close": [100., 101., 102.], "volume": [1., 2., 3.]}, index=index)
    bars.to_parquet(tmp_path / f"BTCUSDT-1s-{day}.parquet")
    monkeypatch.setattr(fe, "market_bars_dir", lambda *_: tmp_path)
    pd.testing.assert_frame_equal(fe._load_market_bars_for_tag("BTCUSDT", fe.PERP_MARKET, day), bars, check_freq=False)

    book_path = tmp_path / f"BTCUSDT-bbo-{day}.parquet"
    book_path.touch()
    monkeypatch.setattr(fe, "_book_dirs_for_symbol", lambda *_: (tmp_path, tmp_path))
    monkeypatch.setattr(fe, "_load_bbo_10s", lambda *_: bars)
    rejected_book = fe._load_market_bbo_for_tag("BTCUSDT", fe.PERP_MARKET, day)
    assert rejected_book is not None and rejected_book.empty
    assert fe._load_market_bars_for_tag("BTCUSDT", fe.PERP_MARKET, "2026-03-30") is None


def test_feature_book_environment_overrides_apply_to_both_symbols(
    tmp_path: Path,
) -> None:
    overrides = {
        "MM_BBO_DIR": str(tmp_path / "override-bbo"),
        "MM_L2_DIR": str(tmp_path / "override-l2"),
    }

    btcusdc = fe._book_dirs_for_symbol(
        "BTCUSDC",
        legacy_root=tmp_path / "legacy",
        normalized_root=tmp_path / "normalized",
        environ=overrides,
    )
    btcusdt = fe._book_dirs_for_symbol(
        "BTCUSDT",
        legacy_root=tmp_path / "legacy",
        normalized_root=tmp_path / "normalized",
        environ=overrides,
    )

    expected = (
        (tmp_path / "override-bbo").resolve(),
        (tmp_path / "override-l2").resolve(),
    )
    assert btcusdc == expected
    assert btcusdt == expected


@pytest.mark.parametrize("key", ["MM_BBO_DIR", "MM_L2_DIR"])
def test_feature_books_reject_partial_override(
    tmp_path: Path,
    key: str,
) -> None:
    with pytest.raises(ValueError, match="must be set together"):
        fe._book_dirs_for_symbol(
            "BTCUSDC",
            legacy_root=tmp_path / "legacy",
            normalized_root=tmp_path / "normalized",
            environ={key: str(tmp_path / key.lower())},
        )


def test_required_execution_l2_fails_instead_of_zero_fill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fe, "load_l2_summary_1s", lambda *_args, **_kwargs: None)
    frame = pd.DataFrame(index=pd.DatetimeIndex([], tz="UTC"))

    with pytest.raises(RuntimeError, match="required execution L2"):
        fe.add_execution_l2_features(
            frame,
            frame.index,
            "2026-01-01",
            "BTCUSDC",
            require_l2=True,
        )


def test_required_taker_tempo_fails_instead_of_zero_fill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        fe,
        "_load_taker_tempo_features",
        lambda *_args, **_kwargs: None,
    )
    frame = pd.DataFrame(index=pd.DatetimeIndex([], tz="UTC"))

    with pytest.raises(RuntimeError, match="required taker-tempo"):
        fe.add_taker_tempo_features(
            frame,
            "BTCUSDC",
            "2026-01-01",
            require_taker_tempo=True,
        )
