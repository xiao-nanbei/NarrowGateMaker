import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

from live.config import Config
from strategy.inventory_manager import PositionState
from strategy.maker_engine import MakerEngine
from strategy.signal import DepthSnapshot, Prediction, SignalEngine


def _depth_event(ts_ms: int, bid: float, bid_qty: float, ask: float, ask_qty: float) -> dict:
    return {
        "T": ts_ms,
        "b": [[str(bid), str(bid_qty)]],
        "a": [[str(ask), str(ask_qty)]],
    }


def _book_event(ts_ms: int, bid: float, ask: float, sequence: int) -> dict:
    return {
        "E": ts_ms,
        "s": "BTCUSDC",
        "b": str(bid),
        "B": "1.0",
        "a": str(ask),
        "A": "1.0",
        "u": sequence,
    }


def test_quote_decision_snapshot_is_immutable_across_feed_updates() -> None:
    signal = SignalEngine(enable_ml=False)
    base_ms = 1_800_000_000_000
    first_receive_ns = base_ms * 1_000_000 + 1_000_000
    signal.on_book_ticker(
        _book_event(base_ms, 99.9, 100.1, 11),
        receive_ts_ns=first_receive_ns,
        sequence_number=11,
    )
    signal.on_depth(
        _depth_event(base_ms, 99.0, 9.0, 101.0, 1.0),
        receive_ts_ns=first_receive_ns + 1,
    )
    first = signal.quote_decision_snapshot(
        now_ns=first_receive_ns + 2_000_000
    )

    signal.on_depth(
        _depth_event(base_ms + 100, 109.0, 1.0, 111.0, 9.0),
        receive_ts_ns=first_receive_ns + 3_000_000,
    )
    signal.on_book_ticker(
        _book_event(base_ms + 100, 109.9, 110.1, 12),
        receive_ts_ns=first_receive_ns + 3_000_001,
        sequence_number=12,
    )
    second = signal.quote_decision_snapshot(
        now_ns=first_receive_ns + 4_000_000
    )

    assert first.valid
    assert first.mid == 100.0
    assert first.bids == ((99.0, 9.0),)
    assert first.asks == ((101.0, 1.0),)
    assert first.book_ticker_bid == 99.9
    assert first.book_ticker_ask == 100.1
    assert first.book_ticker_sequence == 11
    assert second.mid == 110.0
    assert second.depth_generation == first.depth_generation + 1
    assert second.book_ticker_generation == first.book_ticker_generation + 1
    assert second.market_generation == first.market_generation + 2


def test_bar_pricing_mid_is_frozen_with_execution_book_snapshot() -> None:
    signal = SignalEngine(enable_ml=False)
    base_ms = 1_800_000_000_000
    receive_ns = base_ms * 1_000_000 + 1_000_000
    with signal._lock:
        signal._close_history.append(100.25)
    signal.on_depth(
        _depth_event(base_ms, 99.0, 1.0, 101.0, 1.0),
        receive_ts_ns=receive_ns,
    )

    frozen = signal.quote_decision_snapshot(now_ns=receive_ns + 1)
    with signal._lock:
        signal._close_history.append(110.25)

    assert frozen.bar_pricing_mid == pytest.approx(100.25)
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    assert (
        engine._quote_snapshot_contract_error(
            frozen,
            use_bar_pricing=True,
            post_only_guard=engine._post_only_guard_for_snapshot(frozen),
        )
        == ""
    )


def test_bar_pricing_requires_a_frozen_positive_mid() -> None:
    signal = SignalEngine(enable_ml=False)
    base_ms = 1_800_000_000_000
    receive_ns = base_ms * 1_000_000 + 1_000_000
    signal.on_depth(
        _depth_event(base_ms, 99.0, 1.0, 101.0, 1.0),
        receive_ts_ns=receive_ns,
    )
    snapshot = signal.quote_decision_snapshot(now_ns=receive_ns + 1)
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()

    assert (
        engine._quote_snapshot_contract_error(
            snapshot,
            use_bar_pricing=True,
            post_only_guard=engine._post_only_guard_for_snapshot(snapshot),
        )
        == "missing_or_invalid_frozen_bar_pricing_mid"
    )


def test_quote_decision_snapshot_rejects_future_receive_clock() -> None:
    signal = SignalEngine(enable_ml=False)
    base_ms = 1_800_000_000_000
    capture_ns = base_ms * 1_000_000 + 1_000_000
    signal.on_depth(
        _depth_event(base_ms, 99.0, 1.0, 101.0, 1.0),
        receive_ts_ns=capture_ns + 1,
    )

    snapshot = signal.quote_decision_snapshot(now_ns=capture_ns)

    assert not snapshot.valid
    assert snapshot.invalid_reason == "depth_receive_after_snapshot"


def test_missing_depth_exchange_clock_is_not_fabricated() -> None:
    signal = SignalEngine(enable_ml=False)
    receive_ns = 1_800_000_000_000_000_000
    event = _depth_event(0, 99.0, 1.0, 101.0, 1.0)
    del event["T"]
    signal.on_depth(event, receive_ts_ns=receive_ns)

    snapshot = signal.quote_decision_snapshot(now_ns=receive_ns + 1)

    assert snapshot.valid is False
    assert snapshot.depth_exchange_ts_ms == 0
    assert snapshot.invalid_reason == "missing_depth_exchange_timestamp"


def test_live_capture_clock_is_taken_after_market_lock_acquisition() -> None:
    signal = SignalEngine(enable_ml=False)
    result = []

    with signal._lock:
        worker = threading.Thread(
            target=lambda: result.append(signal.quote_decision_snapshot())
        )
        worker.start()
        time.sleep(0.01)
        receive_ns = time.time_ns()
        depth = DepthSnapshot(
            ts=receive_ns / 1_000_000.0,
            receive_ts_ns=receive_ns,
            bids=[(99.0, 1.0)],
            asks=[(101.0, 1.0)],
        )
        signal._quote_market_generation += 1
        signal._depth_generation += 1
        signal._last_depth = depth
        signal._depth_history.append(depth)

    worker.join(timeout=1.0)

    assert not worker.is_alive()
    assert result[0].valid
    assert result[0].capture_ts_ns >= receive_ns


def test_invalid_snapshot_block_cancels_active_orders() -> None:
    signal = SignalEngine(enable_ml=False)
    base_ms = 1_800_000_000_000
    capture_ns = base_ms * 1_000_000 + 1_000_000
    signal.on_depth(
        _depth_event(base_ms, 99.0, 1.0, 101.0, 1.0),
        receive_ts_ns=capture_ns + 1,
    )
    snapshot = signal.quote_decision_snapshot(now_ns=capture_ns)
    canceled = []
    engine = object.__new__(MakerEngine)
    engine.orders = SimpleNamespace(active_count=lambda: 2)
    engine._cancel_all_orders = lambda: canceled.append(True)
    engine._last_quote_snapshot_block_log = 0.0

    engine._block_invalid_quote_snapshot(snapshot, snapshot.invalid_reason)

    assert canceled == [True]


def test_policy_l2_metrics_use_frozen_history_cutoff() -> None:
    signal = SignalEngine(enable_ml=False)
    base_ms = 1_800_000_000_000
    receive_ns = base_ms * 1_000_000 + 1_000_000
    signal.on_book_ticker(
        _book_event(base_ms, 99.9, 100.1, 1),
        receive_ts_ns=receive_ns,
        sequence_number=1,
    )
    signal.on_depth(
        _depth_event(base_ms, 99.0, 9.0, 101.0, 1.0),
        receive_ts_ns=receive_ns + 1,
    )
    frozen = signal.quote_decision_snapshot(now_ns=receive_ns + 2_000_000)

    signal.on_depth(
        _depth_event(base_ms + 100, 99.0, 1.0, 101.0, 9.0),
        receive_ts_ns=receive_ns + 3_000_000,
    )

    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.signal = signal
    frozen_metrics = engine._current_l2_policy_metrics(frozen.mid, frozen)
    current_metrics = engine._current_l2_policy_metrics(frozen.mid)

    assert frozen_metrics["microprice_shift_bps"] > 0.0
    assert frozen_metrics["depth_age_s"] == pytest.approx(
        frozen.depth_visible_age_s
    )
    assert current_metrics["microprice_shift_bps"] < 0.0
    bad_mid = replace(frozen, mid=frozen.mid + 0.1)
    assert (
        engine._quote_snapshot_contract_error(
            bad_mid,
            use_bar_pricing=False,
            post_only_guard=engine._post_only_guard_for_snapshot(bad_mid),
        )
        == "depth_mid_identity_mismatch"
    )
    assert (
        engine._quote_snapshot_contract_error(
            bad_mid,
            use_bar_pricing=True,
            post_only_guard=engine._post_only_guard_for_snapshot(bad_mid),
        )
        == "depth_mid_identity_mismatch"
    )


def test_compute_quotes_does_not_read_mutable_signal_depth(monkeypatch) -> None:
    signal = SignalEngine(enable_ml=False)
    base_ms = 1_800_000_000_000
    receive_ns = base_ms * 1_000_000 + 1_000_000
    signal.on_book_ticker(
        _book_event(base_ms, 99.9, 100.1, 1),
        receive_ts_ns=receive_ns,
        sequence_number=1,
    )
    signal.on_depth(
        _depth_event(base_ms, 99.0, 4.0, 101.0, 2.0),
        receive_ts_ns=receive_ns + 1,
    )
    snapshot = signal.quote_decision_snapshot(now_ns=receive_ns + 2_000_000)

    class NoMutableDepthSignal:
        _feat_history = ()
        _close_history = ()
        rolling_variance = 1.0

        @property
        def _last_depth(self):  # pragma: no cover - any access fails the test.
            raise AssertionError("quote path reread mutable _last_depth")

    cfg = Config()
    cfg.strategy.use_bar_pricing = False
    cfg.depth_execution.shadow_enabled = False
    engine = object.__new__(MakerEngine)
    engine.cfg = cfg
    engine._model_dir = ""
    engine.signal = NoMutableDepthSignal()
    engine.inventory = SimpleNamespace(
        snapshot=SimpleNamespace(
            open_time=0.0,
            state=PositionState.FLAT,
            unrealized_pnl=0.0,
        )
    )
    engine._ber_active = False
    engine._mo_ema_all = 0.0
    engine._mo_ema_bid = 0.0
    engine._mo_ema_ask = 0.0
    engine._mo_ref = 50.0
    engine._requote_count = 1
    engine._last_quote_context = {}
    engine._last_quote_diagnostics = {}
    engine._markout_pause_latch_active = lambda side, now: False
    engine._apply_live_local_extreme_guard_context = lambda mid: None
    engine._log_depth_execution_shadow = lambda **kwargs: None
    monkeypatch.setattr("strategy.maker_engine._get_fill_model", lambda _: None)

    bid, ask, spread = engine._compute_quotes(
        snapshot,
        0.0,
        Prediction(),
        pricing_mid=snapshot.mid,
    )

    assert bid < ask
    assert spread == ask - bid
    assert engine._last_quote_diagnostics["quote_snapshot_depth_generation"] == 1
    assert engine._last_quote_diagnostics["quote_snapshot_depth_bid"] == 99.0


def test_snapshot_separates_visible_age_from_source_lag() -> None:
    signal = SignalEngine(enable_ml=False)
    base_ms = 1_800_000_000_000
    receive_ns = base_ms * 1_000_000 + 250_000_000
    signal.on_depth(
        _depth_event(base_ms, 99.0, 1.0, 101.0, 1.0),
        receive_ts_ns=receive_ns,
    )

    snapshot = signal.quote_decision_snapshot(
        now_ns=receive_ns + 750_000_000
    )

    assert snapshot.valid
    assert snapshot.depth_visible_age_s == pytest.approx(0.75)
    assert snapshot.depth_source_lag_s == pytest.approx(0.25)
    assert snapshot.depth_age_s == pytest.approx(1.0)
    assert snapshot.lock_wait_ns >= 0
    assert snapshot.lock_hold_ns >= 0


def test_book_ticker_without_valid_clock_falls_back_to_depth() -> None:
    signal = SignalEngine(enable_ml=False)
    base_ms = 1_800_000_000_000
    receive_ns = base_ms * 1_000_000 + 1_000_000
    signal.on_book_ticker(
        _book_event(base_ms, 99.9, 100.1, 1),
        receive_ts_ns=receive_ns,
        sequence_number=1,
    )
    signal.on_depth(
        _depth_event(base_ms, 99.0, 1.0, 101.0, 1.0),
        receive_ts_ns=receive_ns + 1,
    )
    snapshot = signal.quote_decision_snapshot(now_ns=receive_ns + 2_000_000)

    missing_clock = replace(snapshot, book_ticker_exchange_ts_ms=0)
    guard = missing_clock.post_only_guard(
        max_visible_age_s=5.0,
        max_source_lag_s=5.0,
    )

    assert missing_clock.valid
    assert guard.source == "depth"
    assert guard.fallback_reason == "missing_book_ticker_exchange_timestamp"
    assert guard.best_bid == snapshot.best_bid
    assert guard.best_ask == snapshot.best_ask


def test_missing_book_ticker_exchange_clock_is_not_fabricated() -> None:
    signal = SignalEngine(enable_ml=False)
    base_ms = 1_800_000_000_000
    receive_ns = base_ms * 1_000_000 + 1_000_000
    book = _book_event(base_ms, 99.9, 100.1, 1)
    del book["E"]
    signal.on_book_ticker(
        book,
        receive_ts_ns=receive_ns,
        sequence_number=1,
    )
    signal.on_depth(
        _depth_event(base_ms, 99.0, 1.0, 101.0, 1.0),
        receive_ts_ns=receive_ns + 1,
    )

    snapshot = signal.quote_decision_snapshot(now_ns=receive_ns + 2_000_000)
    guard = snapshot.post_only_guard(
        max_visible_age_s=5.0,
        max_source_lag_s=5.0,
    )

    assert snapshot.book_ticker_exchange_ts_ms == 0
    assert guard.source == "depth"
    assert guard.fallback_reason == "missing_book_ticker_exchange_timestamp"


def test_stale_book_ticker_falls_back_without_invalidating_depth() -> None:
    signal = SignalEngine(enable_ml=False)
    base_ms = 1_800_000_000_000
    book_receive_ns = base_ms * 1_000_000 + 1_000_000
    depth_receive_ns = book_receive_ns + 4_000_000_000
    signal.on_book_ticker(
        _book_event(base_ms, 99.9, 100.1, 1),
        receive_ts_ns=book_receive_ns,
        sequence_number=1,
    )
    signal.on_depth(
        _depth_event(base_ms + 4_000, 99.0, 1.0, 101.0, 1.0),
        receive_ts_ns=depth_receive_ns,
    )
    snapshot = signal.quote_decision_snapshot(now_ns=depth_receive_ns + 1_000_000)

    guard = snapshot.post_only_guard(
        max_visible_age_s=2.0,
        max_source_lag_s=5.0,
    )

    assert snapshot.valid
    assert guard.source == "depth"
    assert guard.fallback_reason == "stale_book_ticker_visible_age"

    delayed_source = replace(
        snapshot,
        book_ticker_exchange_ts_ms=base_ms - 10_000,
        book_ticker_receive_ts_ns=depth_receive_ns,
    )
    delayed_guard = delayed_source.post_only_guard(
        max_visible_age_s=2.0,
        max_source_lag_s=5.0,
    )
    assert delayed_guard.source == "depth"
    assert delayed_guard.fallback_reason == "stale_book_ticker_source_lag"


def test_fresh_clocked_book_ticker_is_selected_for_post_only_guard() -> None:
    signal = SignalEngine(enable_ml=False)
    base_ms = 1_800_000_000_000
    receive_ns = base_ms * 1_000_000 + 2_000_000
    signal.on_book_ticker(
        _book_event(base_ms, 99.9, 100.1, 1),
        receive_ts_ns=receive_ns,
        sequence_number=1,
    )
    signal.on_depth(
        _depth_event(base_ms, 99.0, 1.0, 101.0, 1.0),
        receive_ts_ns=receive_ns + 1,
    )
    snapshot = signal.quote_decision_snapshot(now_ns=receive_ns + 3_000_000)

    guard = snapshot.post_only_guard(
        max_visible_age_s=5.0,
        max_source_lag_s=5.0,
    )

    assert guard.source == "book_ticker"
    assert guard.fallback_reason == ""
    assert guard.best_bid == 99.9
    assert guard.best_ask == 100.1


def test_routing_contract_rejects_post_only_cross_and_non_tick_price() -> None:
    signal = SignalEngine(enable_ml=False)
    base_ms = 1_800_000_000_000
    receive_ns = base_ms * 1_000_000 + 2_000_000
    signal.on_book_ticker(
        _book_event(base_ms, 99.9, 100.1, 1),
        receive_ts_ns=receive_ns,
        sequence_number=1,
    )
    signal.on_depth(
        _depth_event(base_ms, 99.0, 1.0, 101.0, 1.0),
        receive_ts_ns=receive_ns + 1,
    )
    snapshot = signal.quote_decision_snapshot(now_ns=receive_ns + 3_000_000)
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    guard = engine._post_only_guard_for_snapshot(snapshot)

    assert (
        engine._quote_routing_contract_error(
            bid_price=100.0,
            ask_price=100.2,
            can_bid=True,
            can_ask=True,
            post_only_guard=guard,
        )
        == ""
    )
    assert (
        engine._quote_routing_contract_error(
            bid_price=100.1,
            ask_price=100.2,
            can_bid=True,
            can_ask=True,
            post_only_guard=guard,
        )
        == "post_only_buy_crosses_frozen_guard"
    )
    assert engine._quote_routing_contract_error(
        bid_price=100.05,
        ask_price=100.2,
        can_bid=True,
        can_ask=True,
        post_only_guard=guard,
    ).startswith("non_executable_tick_price")
