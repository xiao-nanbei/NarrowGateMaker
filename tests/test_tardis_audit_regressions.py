"""Controlled counterexamples, not purchased market samples."""

import json
from types import SimpleNamespace

import numpy as np
import pytest

from data.runtime import PublicInputStream


@pytest.mark.parametrize("dates", [
    ["2026-01-01", "2026-01-03"],
    ["2026-01-01", "2026-01-02", "2026-01-02", "2026-01-03"],
    ["2026-01-01", "2026-01-03", "2026-01-02", "2026-01-03"],
])
def test_calendar_rejects_interior_holes_duplicates_and_reordering(tmp_path, dates):
    from data.runtime import bundle_paths
    (tmp_path / "manifest.json").write_text(json.dumps({
        "schema": "data.full_calendar.v1", "source_profile": "tardis_only",
        "start": "2026-01-01", "end": "2026-01-03", "symbols": ["BTCUSDC"],
        "days": [{"calendar_date": d} for d in dates],
    }))
    with pytest.raises(ValueError, match="missing, duplicated or reordered"):
        bundle_paths(tmp_path)


@pytest.mark.parametrize("missing_channel", [False, True])
def test_calendar_selected_interval_checks_channels_and_relocated_binding(tmp_path, missing_channel):
    from data.facts import _digest
    from data.runtime import bundle_paths
    day = tmp_path / "2026-01-02"
    day.mkdir()
    channels = ["incremental_book_L2"] if missing_channel else ["incremental_book_L2", "trades"]
    (day / "manifest.json").write_text(json.dumps({"schema": "data.facts.v1", "files": [
        {"symbol": "BTCUSDC", "channel": c} for c in channels]}))
    (tmp_path / "manifest.json").write_text(json.dumps({
        "schema": "data.full_calendar.v1", "source_profile": "tardis_only",
        "start": "2026-01-01", "end": "2026-01-03", "symbols": ["BTCUSDC"],
        "days": [{"calendar_date": "2026-01-02", "status": "content_scanned",
                  "bundle": "unavailable-original-location", "manifest_sha256": _digest(day / "manifest.json")}],
    }))
    if missing_channel:
        with pytest.raises(ValueError, match="required channel"):
            bundle_paths(tmp_path, start="2026-01-02", end="2026-01-02", relocated=True)
    else:
        assert bundle_paths(tmp_path, start="2026-01-02", end="2026-01-02", relocated=True) == [day]


@pytest.mark.parametrize("last_min", [100, 101, 102])
def test_empty_bundle_retains_trade_identity_high_water(tmp_path, monkeypatch, last_min):
    bundles = []
    for index, (lo, hi) in enumerate([(100, 101), (None, None), (last_min, 103)]):
        root = tmp_path / str(index)
        root.mkdir()
        specs = [{"channel": "trades", "symbol": "BTCUSDC",
                  "quality": {"trade_id_min": lo, "trade_id_max": hi}}]
        # Another market's IDs must not affect this market's high-water mark.
        specs.append({"channel": "trades", "symbol": "BTCUSDT",
                      "quality": {"trade_id_min": 1, "trade_id_max": 999}})
        (root / "manifest.json").write_text(json.dumps({"files": specs}))
        bundles.append(root)
    monkeypatch.setattr("data.runtime.read_facts", lambda *a, **kw: iter(()))
    stream = PublicInputStream(bundles, profile=SimpleNamespace(clock_policy="strict_exchange"),
                               start_ns=0, end_ns=1_000_000_000,
                               market_id="binance-futures:BTCUSDC", input_contract_id="synthetic")
    if last_min <= 101:
        with pytest.raises(ValueError, match="require reconciled facts"):
            list(stream._channel("trades"))
    else:
        assert list(stream._channel("trades")) == []


def test_reference_request_rejected_before_loading_execution_only_bundle(monkeypatch):
    from models import backtest_tick

    def forbidden(*args, **kwargs):
        pytest.fail("unsupported request must fail before input loading")

    monkeypatch.setattr(backtest_tick, "load_public_inputs", forbidden)
    with pytest.raises(ValueError, match="reference-market"):
        backtest_tick.simulate_public_inputs(None, {"cross_market_enabled": True})


@pytest.mark.parametrize("close", [np.nan, np.inf, -np.inf, 100.0])
def test_toxicity_unknown_markout_remains_unknown(close):
    from features.feature_engineer import _compute_toxicity_pair

    times = np.arange(3, dtype=np.int64) * 1_000_000_000
    bid_end = np.zeros(1, dtype=np.int64)
    ask_end = np.zeros(1, dtype=np.int64)
    bid, ask = _compute_toxicity_pair(
        times, np.array([100., close, 100.]), np.full(3, 102.), np.full(3, 98.),
        np.array([0], dtype=np.int64), np.array([0], dtype=np.int64),
        np.array([99.]), np.array([101.]), 1_000_000_000, bid_end, ask_end,
    )
    if np.isfinite(close):
        assert bid[0] == ask[0] == 0.
        assert bid_end[0] == ask_end[0] == 2_000_000_000
    else:
        assert np.isnan(bid[0]) and np.isnan(ask[0])
        assert bid_end[0] == ask_end[0] == 0
