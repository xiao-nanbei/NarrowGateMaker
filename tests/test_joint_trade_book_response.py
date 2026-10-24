"""Synthetic causal market panel tests; no strategy runs."""

from decimal import Decimal as D
import pickle

import pytest

from data.observation import Observation, TradeContribution
from data.runtime import InputTick, ObservationProfile
from data.tardis_input import BookView
from features.trade_book_response import ResponseState, SECOND, price_quantity
from research.families.f08_side_taker_lifecycle.joint_trade_book_response import DailySample, MarketPanels


def book(version=1, quantity="2", price="100.1"):
    return BookView(version, ((D(price), D(quantity)), (D("99.1"), D("3"))),
                    ((D("101.1"), D("2")), (D("102.1"), D("3"))), None, None, "test", True)


def trade(now, side="sell", qty="1"):
    return TradeContribution(str(now), now, D("100.1"), D(qty), side, 1, None, "test", 1)


def warmed():
    state = ResponseState(trade_coverage="observed")
    state.observe_book(0, 0, book())
    state.observe_book(10 * SECOND, 10 * SECOND, book(2))
    return state


def test_fixed_price_coverage_and_decimal_roundtrip():
    view = book()
    assert price_quantity(view, "bid", 100.1) == 2
    assert price_quantity(view, "bid", 99.5) == 0
    assert price_quantity(view, "bid", 98) is None


def test_ready_windows_not_exchange_windows():
    state = warmed()
    state.observe_trade(10 * SECOND, trade(0, qty="2"))
    assert dict(state.frame("bid").values)["trade_pressure_1s"] == 2
    state.advance(11 * SECOND)
    assert state.volumes[1] == [0, 0]
    assert state.volumes[5] == [0, 2]
    with pytest.raises(ValueError, match="backwards"):
        state.advance(0)


def test_decline_anchor_only_uses_observed_recovery():
    state = warmed()
    state.observe_book(10 * SECOND + 1, 10 * SECOND + 1, book(3, "1"))
    frame = state.frame("bid")
    assert frame.recovery_target == 2
    assert dict(frame.values)["observed_recovery_fraction"] == 0
    state.observe_book(10 * SECOND + 2, 10 * SECOND + 2, book(4, "1.5"))
    assert dict(state.frame("bid").values)["observed_recovery_fraction"] == .5
    state.observe_book(10 * SECOND + 3, 10 * SECOND + 3, book(5, "2"))
    assert state.frame("bid").recovery_target is None


def test_restore_independent_and_older_replacement_ignored():
    state = warmed()
    restored = pickle.loads(pickle.dumps(state))
    assert restored.observe_book(10 * SECOND, 9 * SECOND, book()) == ()
    restored.observe_trade(10 * SECOND, trade(0))
    assert state.cumulative == [0, 0]
    assert restored.cumulative == [0, 1]


def test_missing_neighbor_not_zero_and_future_rejected():
    state = warmed()
    assert dict(state.frame("bid").values)["neighbor_delta_qty"] is None
    with pytest.raises(ValueError, match="future"):
        state.observe_book(10 * SECOND, 11 * SECOND, book())


def test_hash_sample_is_bounded_reproducible_with_probability():
    samples = [DailySample(7), DailySample(7)]
    for sample in samples:
        for i in range(100):
            sample.offer(str(i), {"i": i})
    assert samples[0].selected() == samples[1].selected()
    assert len(samples[0].selected()) == 7
    assert all(row["inclusion_probability"] == .07 for row in samples[0].selected())


def panels():
    profile = ObservationProfile(profile_id="synthetic", clock_policy="source_timestamp_proxy",
                                 market_delay_ns=0, processing_ns=0, allowed_lateness_ns=0,
                                 max_book_age_ns=SECOND, trade_coverage="observed")
    out = MarketPanels(start_ns=0, end_ns=100 * SECOND, profile=profile)
    out.states["visible"] = warmed()
    out.offer("visible", out.states["visible"].frame("bid"))
    return out


def test_labels_asof_equal_timestamp_and_no_future_book():
    out = panels()
    state = out.states["visible"]
    due = 10 * SECOND + 100_000_000
    out.flush(due)
    row = next(iter(out.samples["1970-01-01"].rows.values()))
    assert row[f"missing_{100_000_000}"] == "not_yet_resolved"
    state.observe_book(due, due, book(3, price="100.5"))
    out.flush(due + 1)
    assert row[f"mid_move_{100_000_000}"] == pytest.approx(.2)
    assert row[f"fixed_qty_{100_000_000}"] == 0


def test_reset_censors_pending_outcome():
    out = panels()
    out.states["visible"].observe_book(10 * SECOND + 1, 10 * SECOND + 1, book(3), reset=True)
    out.flush(11 * SECOND)
    row = next(iter(out.samples["1970-01-01"].rows.values()))
    assert row[f"missing_{100_000_000}"] == "reset_or_gap"


def test_compact_pending_matches_identity_eviction_reset_and_restore():
    from dataclasses import replace
    profile = ObservationProfile("test", "source_timestamp_proxy", 0, 0, 0, SECOND,
                                 trade_coverage="observed")
    panels = [MarketPanels(start_ns=0, end_ns=65 * SECOND, profile=profile,
                          limit=17, compact_pending=flag) for flag in (False, True)]
    for i in range(640):
        now = i * 100_000_000
        view = replace(book(i, str(1 + i % 3)), source_asof_us=now // 1000)
        obs = Observation(str(i), "test", "test", "test", now, now, now, now, view, "observed")
        for panel in panels:
            panel.advance(InputTick(now, (), (obs,), (), None, None))
        if i % 101 == 0:
            panels = [pickle.loads(pickle.dumps(p)) for p in panels]
    for panel in panels:
        panel.flush(65 * SECOND)
    assert panels[0].samples["1970-01-01"].selected() == panels[1].samples["1970-01-01"].selected()
    assert panels[0].days == panels[1].days
    assert not panels[1].sample_meta


def test_deferred_features_equal_eager_and_do_not_change_state():
    first, second = warmed(), warmed()
    now = 10 * SECOND + 1
    eager = first.observe_book(now, now, book(3, "1"))
    lazy = second.observe_book(now, now, book(3, "1"), include_values=False)
    assert not lazy[0].values
    assert eager == tuple(second.frame(side) for side in ("bid", "ask"))
    assert pickle.dumps(first) == pickle.dumps(second)


def test_first_recovery_survives_subsequent_decline():
    out = panels()
    state = out.states["visible"]
    state.observe_book(10 * SECOND + 1, 10 * SECOND + 1, book(3, "1"))
    out.offer("visible", state.frame("bid"))
    state.observe_book(10 * SECOND + 2, 10 * SECOND + 2, book(4, "2"))
    state.observe_book(10 * SECOND + 3, 10 * SECOND + 3, book(5, ".5"))
    out.flush(10 * SECOND + 100_000_002)
    row = next(r for r in out.samples["1970-01-01"].rows.values() if r["recovery_target"] is not None)
    assert row[f"recovered_{100_000_000}"] == 0
    assert row[f"recovery_event_{100_000_000}"] == "recovered"
    assert row[f"first_recovery_s_{100_000_000}"] == 1e-9


def test_warmed_parallel_block_matches_continuous_across_midnight():
    from dataclasses import replace

    day = 86400 * SECOND
    profile = ObservationProfile("test", "source_timestamp_proxy", 0, 0, 0, SECOND,
                                 trade_coverage="observed")
    full = MarketPanels(start_ns=day, end_ns=day + 60 * SECOND, profile=profile, limit=17)
    block = MarketPanels(start_ns=day, end_ns=day + 60 * SECOND, profile=profile, limit=17)
    for second in range(-120, 60):
        now = day + second * SECOND
        view = replace(book(second + 1000, str(1 + (second % 3))), source_asof_us=now // 1000)
        observations = (
            Observation(str(now), "test", "test", "test", now, now, now, now, trade(now), "observed"),
            Observation(str(now) + "b", "test", "test", "test", now, now, now, now, view, "observed"),
        )
        tick = InputTick(now, (), observations, (), None, None)
        full.advance(tick)
        if second >= -60:
            block.advance(tick)
    full.flush(day + 60 * SECOND)
    block.flush(day + 60 * SECOND)
    left = next(iter(full.samples.values())).selected()
    right = next(iter(block.samples.values())).selected()
    assert len(left) == len(right) == 17
    for a, b in zip(left, right, strict=True):
        # Absolute volume origins differ, but all causal features and future
        # increments, sample identities, probabilities and censoring agree.
        for name in ("initial_aggressor_qty", "initial_net_qty"):
            a.pop(name)
            b.pop(name)
        assert a == b
