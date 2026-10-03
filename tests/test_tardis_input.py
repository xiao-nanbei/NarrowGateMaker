"""Synthetic fixtures only: no purchased market records are distributed."""

from dataclasses import replace
from decimal import Decimal as D

import pyarrow.csv as pacsv
import pytest

from data.tardis_input import (
    ObservableBook, SourceFragment, SourceQuality, iter_book_messages, iter_trade_executions,
)

BOOK_HEADER = "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
TRADE_HEADER = "exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n"


def source(tmp_path, name, rows, *, trade=False, file_id=None, first_row=1, complete=True):
    path = tmp_path / name
    path.write_text((TRADE_HEADER if trade else BOOK_HEADER) + "".join(
        "binance-futures,BTCUSDC," + row + "\n" for row in rows))
    return SourceFragment(path, file_id or name, first_row, complete)


ROWS = ["1000000,1000010,true,bid,100,2", "1000000,1000010,true,ask,102,3",
        "1001000,1001010,false,bid,100,4", "1001000,1001020,false,ask,102,0",
        "1001000,1001020,false,ask,103,5"]


@pytest.mark.parametrize("cut", [1, 2, 3, 4])
def test_physical_shards_and_arrow_batches_preserve_messages(tmp_path, monkeypatch, cut):
    full = source(tmp_path, "full.csv", ROWS, file_id="original")
    expected = list(iter_book_messages([full], symbol="BTCUSDC"))
    a = source(tmp_path, "a.csv", ROWS[:cut], file_id="original", complete=False)
    b = source(tmp_path, "b.csv", ROWS[cut:], file_id="original", first_row=cut + 1)
    import data.normalize_tardis_orderbook as normalizer
    monkeypatch.setattr(normalizer, "_open_csv", lambda path, **kw: pacsv.open_csv(
        path, read_options=pacsv.ReadOptions(block_size=180),
        convert_options=pacsv.ConvertOptions(column_types={"price": "string", "amount": "string"})))
    assert list(iter_book_messages([a, b], symbol="BTCUSDC")) == expected
    assert [len(x.levels) for x in expected] == [2, 1, 2]
    assert all(x.exchange_ts_ns is None for x in expected)
    assert expected[0].first_row == 1 and expected[0].last_row == 2


def test_unfinished_snapshot_not_published(tmp_path):
    fragment = source(tmp_path, "partial.csv", ROWS[:1], complete=False)
    parser = iter_book_messages([fragment], symbol="BTCUSDC")
    with pytest.raises(ValueError, match="incomplete original"):
        next(parser)


def test_non_contiguous_groups_are_not_global_grouped(tmp_path):
    messages = list(iter_book_messages([source(tmp_path, "a.csv", ROWS + [ROWS[2]])], symbol="BTCUSDC"))
    assert len(messages) == 4
    assert messages[-1].provider_group_id != messages[1].provider_group_id


@pytest.mark.parametrize("fault", ["duplicate", "offset", "different_file"])
def test_source_fragment_identity_rejected(tmp_path, fault):
    a = source(tmp_path, "a.csv", ROWS[:1], file_id="original", complete=fault == "duplicate")
    b = source(tmp_path, "b.csv", ROWS[1:], file_id="different" if fault == "different_file" else "original",
               first_row=1 if fault in {"offset", "duplicate"} else 2)
    with pytest.raises(ValueError, match="source"):
        list(iter_book_messages([a, b], symbol="BTCUSDC"))


def test_snapshot_replaces_and_delta_is_absolute_with_immutable_view(tmp_path):
    messages = list(iter_book_messages([source(tmp_path, "a.csv", ROWS)], symbol="BTCUSDC"))
    book = ObservableBook()
    assert not book.apply(messages[1])  # No snapshot yet.
    assert not book.view().valid
    book.apply(messages[0])
    old = book.view()
    book.apply(messages[1])
    book.apply(messages[1])  # Same absolute quantity, never added twice.
    assert book.quantity("bid", D(100)) == D(4)
    assert old.bids == ((D(100), D(2)),)
    book.apply(messages[2])
    assert book.view().bbo == ((D(100), D(4)), (D(103), D(5)))
    assert book.quantity("ask", D(102)) is None
    book.apply(replace(messages[0], levels=(("bid", D(90), D(1)), ("ask", D(110), D(1)))))
    assert book.quantity("bid", D(100)) is None
    assert book.view().state_time_certainty == "snapshot_anchor_not_exact_state"


def test_all_known_depth_and_last_write_then_atomic_validation(tmp_path):
    message = next(iter_book_messages([source(tmp_path, "a.csv", ROWS[:2])], symbol="BTCUSDC"))
    book = ObservableBook()
    book.apply(replace(message, levels=message.levels + (("bid", D(99), D(7)),)))
    saved = book.view(1)
    book.apply(replace(message, kind="delta", levels=(("bid", D(100), D(0)),
        ("bid", D(98), D(0)), ("ask", D(102), D(9)), ("ask", D(102), D(2)))))
    assert book.view(1).bids == ((D(99), D(7)),)
    assert book.view().asks == ((D(102), D(2)),)
    assert saved.bids == ((D(100), D(2)),)
    before = book.view()
    with pytest.raises(ValueError):
        book.apply(replace(message, kind="delta", levels=(("bid", D(99), D(-1)),)))
    assert book.view() == before


def test_best_price_index_never_rounds_decimal_or_leaks_deleted_levels(tmp_path):
    message = next(iter_book_messages([source(tmp_path, "a.csv", ROWS[:2])], symbol="BTCUSDC"))
    price = D("100.123456789012345678901234567890")
    book = ObservableBook()
    book.apply(replace(message, levels=(("bid", price, D(1)), ("ask", D(102), D(1)))))
    assert book.view(1).bids[0][0] == price
    for i in range(2200):
        book.apply(replace(message, kind="delta", levels=(("bid", D(99), D(i % 2)),)))
    assert book.view(1).bids == book.view(20).bids[:1]
    book.apply(replace(message, kind="delta", levels=(("bid", price, D(0)),)))
    assert book.view(1).bids == book.view(20).bids[:1]


def test_observation_and_change_clocks_distinct(tmp_path):
    message = next(iter_book_messages([source(tmp_path, "a.csv", ROWS[:2])], symbol="BTCUSDC"))
    book = ObservableBook()
    book.apply(message)
    first = book.view()
    book.apply(replace(message, kind="delta", source_timestamp_us=2000000))
    second = book.view()
    assert second.source_asof_us == 2000000
    assert second.state_change_us == first.state_change_us
    assert book.view() == second  # Sampling does not refresh either clock.


def test_regression_preserved_and_mapping_not_guessed(tmp_path):
    quality = SourceQuality()
    fragment = source(tmp_path, "a.csv", ROWS[:2] + ["999000,1200000,false,bid,100,1"])
    messages = list(iter_book_messages([fragment], symbol="BTCUSDC", quality=quality,
                                       delta_clock_kind="exchange_event_E"))
    assert messages[0].exchange_ts_ns is None
    assert messages[1].exchange_ts_ns == 999000000
    assert quality.time_regressions[0]["regression_size"] == 1000
    assert quality.sequence_continuity == "unknown"


def test_trade_identity_overlap_exact_decimal_and_count(tmp_path):
    a = source(tmp_path, "a.csv", ["1000001,1000010,10,buy,100.123456789012345678,0.01",
                                  "1000001,1000011,11,sell,102,0.2"], trade=True)
    b = source(tmp_path, "b.csv", ["1000001,2000010,10,buy,100.123456789012345678,0.010",
                                  "999999,2000011,12,buy,103,0.3"], trade=True)
    quality = SourceQuality()
    trades = list(iter_trade_executions([a, b], symbol="BTCUSDC", quality=quality))
    assert [t.trade_id for t in trades] == [10, 11, 12]
    assert trades[0].price == D("100.123456789012345678")
    assert sum(t.quantity for t in trades) == D("0.51")
    assert all(t.normal_quantity is None and t.native_aggregate_packet_count is None for t in trades)
    assert quality.duplicate_trades == 1 and len(quality.time_regressions) == 1
    assert all(t.exchange_ts_ns is None for t in trades)


def test_trade_conflict_fails_without_timestamp_dedup(tmp_path):
    fragment = source(tmp_path, "a.csv", ["1000000,0,1,buy,100,1", "1000000,1,1,sell,100,1"], trade=True)
    quality = SourceQuality()
    with pytest.raises(ValueError, match="conflicting trade identity"):
        list(iter_trade_executions([fragment], symbol="BTCUSDC", quality=quality))
    assert quality.trade_conflicts == 1


def test_adjacent_file_day_does_not_clip_trade(tmp_path):
    fragment = source(tmp_path, "2026-01-02.csv", ["1767311999999999,0,1,buy,100,1"], trade=True)
    trade = next(iter_trade_executions([fragment], symbol="BTCUSDC", clock_kind="exchange_trade_T"))
    assert trade.exchange_ts_ns == 1767311999999999000


def test_mapping_fallback_never_becomes_exchange_time(tmp_path):
    fragment = source(tmp_path, "a.csv", ROWS[:2])
    message = next(iter_book_messages([fragment], symbol="BTCUSDC", snapshot_clock_kind="provider_receive_fallback"))
    assert message.exchange_ts_ns is None
