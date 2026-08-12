from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from models.tick_data_types import HistoricalBBOData, HistoricalL2Data
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_windows import (
    PROVIDER_BOOK_PROFILE,
    STRICT_EXCHANGE_TIME_PROFILE,
    WindowExtractionAccumulator,
    WindowExtractionContract,
    WindowExtractionError,
    iter_causal_windows,
    stream_causal_windows,
    window_formula_contract,
)

BASE_NS = 1_800_000_000_000_000_000


def _sources(*, omit_second_bbo: bool = False) -> tuple:
    right_ms = np.asarray(
        [
            (BASE_NS + BASE_WINDOW_WIDTH_NS) // 1_000_000,
            (BASE_NS + 2 * BASE_WINDOW_WIDTH_NS) // 1_000_000,
        ],
        dtype=np.int64,
    )
    # normalized_l2_100ms_v2 stores the final accepted source event timestamp
    # inside each bucket rather than the canonical right edge itself.
    source_ms = right_ms - np.asarray([37, 1], dtype=np.int64)
    bbo_rows = slice(0, 1) if omit_second_bbo else slice(None)
    bbo = HistoricalBBOData(
        ts_ms=source_ms[bbo_rows],
        best_bid=np.asarray([100.0, 101.0])[bbo_rows],
        best_ask=np.asarray([102.0, 103.0])[bbo_rows],
        bid_qty=np.asarray([3.0, 4.0])[bbo_rows],
        ask_qty=np.asarray([1.0, 2.0])[bbo_rows],
    )
    bid_qty = np.tile(np.arange(1.0, 21.0), (2, 1))
    ask_qty = np.tile(np.arange(2.0, 22.0), (2, 1))
    l2 = HistoricalL2Data(
        ts_ms=source_ms,
        bid_px=np.tile(np.arange(100.0, 98.0, -0.1), (2, 1)),
        bid_qty=bid_qty,
        ask_px=np.tile(np.arange(102.0, 104.0, 0.1), (2, 1)),
        ask_qty=ask_qty,
    )
    trades = pd.DataFrame(
        {
            "time": [
                (BASE_NS + 10_000_000) // 1_000_000,
                (BASE_NS + BASE_WINDOW_WIDTH_NS) // 1_000_000,
            ],
            "qty": [0.2, 0.3],
            # false is aggressive BUY; true is aggressive SELL.
            "is_buyer_maker": [False, True],
        }
    )
    return bbo, l2, trades


def _contract(profile: str = STRICT_EXCHANGE_TIME_PROFILE) -> WindowExtractionContract:
    return WindowExtractionContract(
        block="M2",
        source_clock_profile=profile,
        left_ts_ns=BASE_NS,
        right_ts_ns=BASE_NS + 2 * BASE_WINDOW_WIDTH_NS,
    )


def test_strict_joint_window_formulas_and_right_boundary_exclusion() -> None:
    bbo, l2, trades = _sources()
    iterator, stats = iter_causal_windows(
        contract=_contract(), bbo=bbo, l2=l2, trades=trades
    )
    rows = list(iterator)

    first = rows[0].values
    assert first["mid_usdc_per_btc"] == pytest.approx(101.0)
    assert first["spread_bps"] == pytest.approx(10_000.0 * 2.0 / 101.0)
    assert first["bbo_imbalance"] == pytest.approx(0.5)
    assert first["microprice_deviation_bps"] == pytest.approx(
        10_000.0 * (101.5 - 101.0) / 101.0
    )
    assert first["aggressive_buy_qty_btc_per_s"] == pytest.approx(2.0)
    assert first["aggressive_sell_qty_btc_per_s"] == 0.0
    assert first["buy_run_length"] == 1.0
    assert first["last_aggressive_sell_age_s"] is None

    second = rows[1].values
    assert second["aggressive_buy_qty_btc_per_s"] == 0.0
    assert second["aggressive_sell_qty_btc_per_s"] == pytest.approx(3.0)
    assert second["sell_run_length"] == 1.0
    assert second["last_aggressive_buy_age_s"] == pytest.approx(0.19)
    assert second["last_aggressive_sell_age_s"] == pytest.approx(0.1)
    assert second["topk_bid_depth_btc"] == pytest.approx(sum(range(1, 21)))
    assert second["topk_ask_depth_btc"] == pytest.approx(sum(range(2, 22)))
    assert first["topk_bid_displayed_depth_increase_btc_per_s"] is None
    assert second["topk_bid_displayed_depth_increase_btc_per_s"] == 0.0
    assert second["topk_bid_displayed_depth_decrease_btc_per_s"] == 0.0
    assert stats.boundary_trade_exclusion_count == 1
    assert stats.trade_windows_observed == 2
    assert stats.economic_outcomes_read is False


def test_provider_book_clock_never_exposes_exchange_time_trade_channels() -> None:
    bbo, l2, trades = _sources()
    iterator, stats = iter_causal_windows(
        contract=_contract(PROVIDER_BOOK_PROFILE),
        bbo=bbo,
        l2=l2,
        trades=trades,
    )
    rows = list(iterator)
    assert rows[0].values["mid_usdc_per_btc"] == pytest.approx(101.0)
    assert rows[0].values["trade_count_per_s"] is None
    assert rows[1].values["aggressive_sell_qty_btc_per_s"] is None
    assert stats.trade_windows_unobserved == 2
    assert window_formula_contract()[
        "provider_local_book_official_trade_joint_visibility"
    ] is False


def test_missing_book_bucket_is_unobserved_not_forward_filled() -> None:
    bbo, l2, trades = _sources(omit_second_bbo=True)
    iterator, stats = iter_causal_windows(
        contract=_contract(), bbo=bbo, l2=l2, trades=trades
    )
    rows = list(iterator)
    assert rows[1].values["mid_usdc_per_btc"] is None
    assert rows[1].values["best_bid_qty_btc"] is None
    assert stats.missing_bbo_windows == 1


def test_multiple_source_rows_in_one_normalized_bucket_fail_closed() -> None:
    bbo, l2, trades = _sources()
    bad = HistoricalBBOData(
        ts_ms=np.asarray([bbo.ts_ms[0], bbo.ts_ms[0] + 1], dtype=np.int64),
        best_bid=bbo.best_bid,
        best_ask=bbo.best_ask,
        bid_qty=bbo.bid_qty,
        ask_qty=bbo.ask_qty,
    )
    with pytest.raises(WindowExtractionError, match="more than one normalized row"):
        iter_causal_windows(contract=_contract(), bbo=bad, l2=l2, trades=trades)


def test_source_event_at_exact_boundary_enters_the_next_bucket() -> None:
    bbo, l2, trades = _sources()
    boundary_ms = (BASE_NS + BASE_WINDOW_WIDTH_NS) // 1_000_000
    shifted = HistoricalBBOData(
        ts_ms=np.asarray([boundary_ms], dtype=np.int64),
        best_bid=bbo.best_bid[:1],
        best_ask=bbo.best_ask[:1],
        bid_qty=bbo.bid_qty[:1],
        ask_qty=bbo.ask_qty[:1],
    )
    rows, _ = iter_causal_windows(
        contract=_contract(), bbo=shifted, l2=l2, trades=trades
    )
    values = list(rows)
    assert values[0].values["mid_usdc_per_btc"] is None
    assert values[1].values["mid_usdc_per_btc"] == pytest.approx(101.0)


def test_streaming_extractor_updates_audit_only_as_rows_are_consumed() -> None:
    bbo, l2, trades = _sources()
    audit = WindowExtractionAccumulator()
    stream = stream_causal_windows(
        contract=_contract(),
        bbo=bbo,
        l2=l2,
        trades=trades,
        audit=audit,
    )
    first = next(stream)
    assert first.right_ts_ns == BASE_NS + BASE_WINDOW_WIDTH_NS
    assert audit.window_count == 1
    assert list(stream)[0].right_ts_ns == BASE_NS + 2 * BASE_WINDOW_WIDTH_NS
    frozen = audit.freeze()
    assert frozen.window_count == 2
    assert frozen.trade_windows_observed == 2


def test_depth_shape_and_displayed_change_use_explicit_tick_units() -> None:
    bbo, _, trades = _sources()
    x = np.arange(20, dtype=float)
    bid_cumulative = 2.0 + 3.0 * x + 2.0 * np.square(x)
    ask_cumulative = 4.0 + 2.0 * x + 1.5 * np.square(x)
    bid_qty = np.diff(np.concatenate(([0.0], bid_cumulative)))
    ask_qty = np.diff(np.concatenate(([0.0], ask_cumulative)))
    second_bid_qty = bid_qty.copy()
    second_ask_qty = ask_qty.copy()
    second_bid_qty[0] += 2.0
    second_ask_qty[0] -= 1.0
    l2 = HistoricalL2Data(
        ts_ms=bbo.ts_ms.copy(),
        bid_px=np.tile(100.0 - 0.1 * x, (2, 1)),
        bid_qty=np.vstack((bid_qty, second_bid_qty)),
        ask_px=np.tile(102.0 + 0.1 * x, (2, 1)),
        ask_qty=np.vstack((ask_qty, second_ask_qty)),
    )

    iterator, _ = iter_causal_windows(
        contract=_contract(), bbo=bbo, l2=l2, trades=trades
    )
    first, second = list(iterator)
    assert first.values["bid_depth_slope_btc_per_tick"] == pytest.approx(3.0)
    assert first.values["bid_depth_convexity_btc_per_tick2"] == pytest.approx(
        4.0
    )
    assert first.values["ask_depth_slope_btc_per_tick"] == pytest.approx(2.0)
    assert first.values["ask_depth_convexity_btc_per_tick2"] == pytest.approx(
        3.0
    )
    assert first.values["topk_bid_displayed_depth_increase_btc_per_s"] is None
    assert second.values[
        "topk_bid_displayed_depth_increase_btc_per_s"
    ] == pytest.approx(20.0)
    assert second.values[
        "topk_bid_displayed_depth_decrease_btc_per_s"
    ] == 0.0
    assert second.values[
        "topk_ask_displayed_depth_increase_btc_per_s"
    ] == 0.0
    assert second.values[
        "topk_ask_displayed_depth_decrease_btc_per_s"
    ] == pytest.approx(10.0)

    formulas = window_formula_contract()
    assert formulas["price_tick_size_usdc_per_btc"] == pytest.approx(0.1)
    assert formulas["depth_slope_unit"] == "BTC_per_tick"
    assert formulas["depth_convexity_unit"] == "BTC_per_tick2"
    assert formulas["displayed_depth_change_is_exact_depletion_refill"] is False


def test_displayed_change_does_not_cross_gap_or_extractor_restart() -> None:
    width_ms = BASE_WINDOW_WIDTH_NS // 1_000_000
    right_ms = np.asarray(
        [
            (BASE_NS + index * BASE_WINDOW_WIDTH_NS) // 1_000_000
            for index in range(1, 5)
        ],
        dtype=np.int64,
    )
    source_ms = right_ms - 1
    bbo = HistoricalBBOData(
        ts_ms=source_ms,
        best_bid=np.asarray([100.0, 100.0, 100.0, 100.0]),
        best_ask=np.asarray([102.0, 102.0, 102.0, 102.0]),
        bid_qty=np.ones(4),
        ask_qty=np.ones(4),
    )
    level = np.arange(20, dtype=float)
    l2_indices = np.asarray([0, 2, 3])
    quantities = np.tile(np.arange(1.0, 21.0), (3, 1))
    l2 = HistoricalL2Data(
        ts_ms=source_ms[l2_indices],
        bid_px=np.tile(100.0 - 0.1 * level, (3, 1)),
        bid_qty=quantities,
        ask_px=np.tile(102.0 + 0.1 * level, (3, 1)),
        ask_qty=quantities,
    )
    contract = WindowExtractionContract(
        block="M2",
        source_clock_profile=STRICT_EXCHANGE_TIME_PROFILE,
        left_ts_ns=BASE_NS,
        right_ts_ns=BASE_NS + 4 * BASE_WINDOW_WIDTH_NS,
    )
    iterator, _ = iter_causal_windows(
        contract=contract,
        bbo=bbo,
        l2=l2,
        trades=None,
    )
    rows = list(iterator)
    channel = "topk_bid_displayed_depth_increase_btc_per_s"
    assert rows[0].values[channel] is None
    assert rows[1].source_gap is True
    assert rows[1].values[channel] is None
    assert rows[2].values[channel] is None
    assert rows[3].values[channel] == 0.0

    restart_contract = WindowExtractionContract(
        block="M2",
        source_clock_profile=STRICT_EXCHANGE_TIME_PROFILE,
        left_ts_ns=BASE_NS + 2 * BASE_WINDOW_WIDTH_NS,
        right_ts_ns=BASE_NS + 4 * BASE_WINDOW_WIDTH_NS,
    )
    restarted, _ = iter_causal_windows(
        contract=restart_contract,
        bbo=HistoricalBBOData(
            ts_ms=source_ms[2:],
            best_bid=bbo.best_bid[2:],
            best_ask=bbo.best_ask[2:],
            bid_qty=bbo.bid_qty[2:],
            ask_qty=bbo.ask_qty[2:],
        ),
        l2=HistoricalL2Data(
            ts_ms=source_ms[2:],
            bid_px=l2.bid_px[1:],
            bid_qty=l2.bid_qty[1:],
            ask_px=l2.ask_px[1:],
            ask_qty=l2.ask_qty[1:],
        ),
        trades=None,
    )
    restart_rows = list(restarted)
    assert restart_rows[0].values[channel] is None
    assert restart_rows[1].values[channel] == 0.0
    assert width_ms == 100
