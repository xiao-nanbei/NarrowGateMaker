#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

#include "narrowgate_cpp/live_market_state.hpp"

namespace {

using narrowgate_cpp::Depth20SideUpdate;
using narrowgate_cpp::LiveMarketState;
using narrowgate_cpp::MarketClockIdentity;
using narrowgate_cpp::MarketStateUpdateStatus;
using narrowgate_cpp::Side;
using narrowgate_cpp::kDestructiveInterferenceBytes;
using narrowgate_cpp::kLiveDepthLevels;

template <Side S>
Depth20SideUpdate make_update(
    std::int64_t best_price_ticks,
    std::uint64_t generation,
    std::uint64_t base_ts_ns,
    std::uint8_t size = 3
) {
    Depth20SideUpdate update;
    update.size = size;
    update.clock = MarketClockIdentity{
        base_ts_ns,
        base_ts_ns + 1,
        base_ts_ns + 2,
        base_ts_ns + 3,
        generation,
    };
    for (std::size_t index = 0; index < size; ++index) {
        if constexpr (S == Side::Buy) {
            update.price_ticks[index] =
                best_price_ticks - static_cast<std::int64_t>(index);
        } else {
            update.price_ticks[index] =
                best_price_ticks + static_cast<std::int64_t>(index);
        }
        update.quantity_lots[index] = 10 + static_cast<std::int64_t>(index);
    }
    return update;
}

void test_cache_layout_contract() {
    static_assert(LiveMarketState::depth_levels() == 20);
    static_assert(
        LiveMarketState::cache_line_bytes() == kDestructiveInterferenceBytes
    );
    static_assert(alignof(LiveMarketState) == kDestructiveInterferenceBytes);
    static_assert(
        alignof(narrowgate_cpp::Depth20SideSnapshot) ==
        kDestructiveInterferenceBytes
    );
    static_assert(
        sizeof(narrowgate_cpp::Depth20SideSnapshot) %
            kDestructiveInterferenceBytes ==
        0
    );
    static_assert(kLiveDepthLevels == 20);
}

void test_single_writer_and_bbo() {
    LiveMarketState state;
    auto writer = state.claim_writer();
    bool duplicate_writer_rejected = false;
    try {
        auto duplicate = state.claim_writer();
        (void)duplicate;
    } catch (const std::logic_error&) {
        duplicate_writer_rejected = true;
    }
    assert(duplicate_writer_rejected);

    const auto bid = make_update<Side::Buy>(633'393, 1, 100);
    const auto ask = make_update<Side::Sell>(633'395, 1, 100);
    assert(writer.replace_book(bid, ask) == MarketStateUpdateStatus::Applied);

    const auto bbo = state.read_bbo();
    assert(bbo.valid());
    assert(bbo.bid_price_ticks == 633'393);
    assert(bbo.bid_quantity_lots == 10);
    assert(bbo.ask_price_ticks == 633'395);
    assert(bbo.ask_quantity_lots == 10);
    assert(bbo.bid_generation == 1);
    assert(bbo.ask_generation == 1);

    const auto bids = state.read<Side::Buy>();
    const auto asks = state.read<Side::Sell>();
    assert(bids.size == 3);
    assert(asks.size == 3);
    assert(bids.price_ticks[2] == 633'391);
    assert(asks.price_ticks[2] == 633'397);
    assert(bids.quantity_lots[2] == 12);
    assert(asks.quantity_lots[2] == 12);
    assert(bids.clock.visible_ts_ns == 103);
}

void test_prefix_read_matches_full_identity() {
    LiveMarketState state;
    auto writer = state.claim_writer();
    const auto bid = make_update<Side::Buy>(633'393, 11, 1'000, 20);
    const auto ask = make_update<Side::Sell>(633'395, 11, 1'000, 20);
    assert(writer.replace_book(bid, ask) == MarketStateUpdateStatus::Applied);

    const auto full = state.read_book();
    const auto prefix = state.read_book_prefix(4);
    assert(full.publication_sequence == prefix.publication_sequence);
    assert(full.publication_sequence == state.publication_sequence());
    assert(prefix.bids.size == 4);
    assert(prefix.asks.size == 4);
    assert(prefix.bbo.bid_price_ticks == full.bbo.bid_price_ticks);
    assert(prefix.bbo.bid_quantity_lots == full.bbo.bid_quantity_lots);
    assert(prefix.bbo.ask_price_ticks == full.bbo.ask_price_ticks);
    assert(prefix.bbo.ask_quantity_lots == full.bbo.ask_quantity_lots);
    assert(prefix.bbo.bid_generation == full.bbo.bid_generation);
    assert(prefix.bbo.ask_generation == full.bbo.ask_generation);
    assert(prefix.bbo.bid_visible_ts_ns == full.bbo.bid_visible_ts_ns);
    assert(prefix.bbo.ask_visible_ts_ns == full.bbo.ask_visible_ts_ns);
    assert(prefix.bids.clock.source_ts_ns == full.bids.clock.source_ts_ns);
    assert(prefix.bids.clock.exchange_ts_ns == full.bids.clock.exchange_ts_ns);
    assert(prefix.bids.clock.receive_ts_ns == full.bids.clock.receive_ts_ns);
    assert(prefix.bids.clock.visible_ts_ns == full.bids.clock.visible_ts_ns);
    assert(prefix.bids.clock.generation == full.bids.clock.generation);
    assert(prefix.asks.clock.source_ts_ns == full.asks.clock.source_ts_ns);
    assert(prefix.asks.clock.exchange_ts_ns == full.asks.clock.exchange_ts_ns);
    assert(prefix.asks.clock.receive_ts_ns == full.asks.clock.receive_ts_ns);
    assert(prefix.asks.clock.visible_ts_ns == full.asks.clock.visible_ts_ns);
    assert(prefix.asks.clock.generation == full.asks.clock.generation);
    for (std::size_t index = 0; index < 4; ++index) {
        assert(prefix.bids.price_ticks[index] == full.bids.price_ticks[index]);
        assert(prefix.bids.quantity_lots[index] == full.bids.quantity_lots[index]);
        assert(prefix.asks.price_ticks[index] == full.asks.price_ticks[index]);
        assert(prefix.asks.quantity_lots[index] == full.asks.quantity_lots[index]);
    }
    for (std::size_t index = 4; index < kLiveDepthLevels; ++index) {
        assert(prefix.bids.price_ticks[index] == 0);
        assert(prefix.bids.quantity_lots[index] == 0);
        assert(prefix.asks.price_ticks[index] == 0);
        assert(prefix.asks.quantity_lots[index] == 0);
    }

    const auto all = state.read_book_prefix(kLiveDepthLevels);
    assert(all.bids.size == full.bids.size);
    assert(all.asks.size == full.asks.size);
    assert(all.publication_sequence == full.publication_sequence);
    for (std::size_t index = 0; index < kLiveDepthLevels; ++index) {
        assert(all.bids.price_ticks[index] == full.bids.price_ticks[index]);
        assert(all.bids.quantity_lots[index] == full.bids.quantity_lots[index]);
        assert(all.asks.price_ticks[index] == full.asks.price_ticks[index]);
        assert(all.asks.quantity_lots[index] == full.asks.quantity_lots[index]);
    }
}

void test_prefix_bounds_are_rejected() {
    LiveMarketState state;
    bool zero_rejected = false;
    try {
        (void)state.read_book_prefix(0);
    } catch (const std::out_of_range&) {
        zero_rejected = true;
    }
    assert(zero_rejected);

    bool oversized_rejected = false;
    try {
        (void)state.read_book_prefix(kLiveDepthLevels + 1);
    } catch (const std::out_of_range&) {
        oversized_rejected = true;
    }
    assert(oversized_rejected);
}

void test_monotonic_clock_and_generation() {
    LiveMarketState state;
    auto writer = state.claim_writer();
    auto initial = make_update<Side::Buy>(100, 7, 1'000);
    assert(
        writer.replace<Side::Buy>(initial) ==
        MarketStateUpdateStatus::Applied
    );

    auto duplicate_generation = make_update<Side::Buy>(99, 7, 1'100);
    assert(
        writer.replace<Side::Buy>(duplicate_generation) ==
        MarketStateUpdateStatus::GenerationRegressed
    );

    auto regressed_clock = make_update<Side::Buy>(99, 8, 1'100);
    regressed_clock.clock.exchange_ts_ns = 900;
    assert(
        writer.replace<Side::Buy>(regressed_clock) ==
        MarketStateUpdateStatus::ClockRegressed
    );

    auto invisible_before_receive = make_update<Side::Buy>(99, 8, 1'100);
    invisible_before_receive.clock.visible_ts_ns =
        invisible_before_receive.clock.receive_ts_ns - 1;
    assert(
        writer.replace<Side::Buy>(invisible_before_receive) ==
        MarketStateUpdateStatus::InvalidClock
    );

    const auto snapshot = state.read<Side::Buy>();
    assert(snapshot.clock.generation == 7);
    assert(snapshot.best_price_ticks() == 100);
}

void test_depth_update_validation_and_crossed_book() {
    LiveMarketState state;
    auto writer = state.claim_writer();
    const auto bid = make_update<Side::Buy>(100, 1, 100);
    const auto ask = make_update<Side::Sell>(102, 1, 100);
    assert(
        writer.replace<Side::Buy>(bid) == MarketStateUpdateStatus::Applied
    );
    assert(
        writer.replace<Side::Sell>(ask) == MarketStateUpdateStatus::Applied
    );

    auto unsorted_bid = make_update<Side::Buy>(100, 2, 200);
    unsorted_bid.price_ticks[1] = 101;
    assert(
        writer.replace<Side::Buy>(unsorted_bid) ==
        MarketStateUpdateStatus::InvalidPriceOrder
    );

    auto crossed_bid = make_update<Side::Buy>(102, 2, 200);
    assert(
        writer.replace<Side::Buy>(crossed_bid) ==
        MarketStateUpdateStatus::CrossedBook
    );

    auto refreshed_ask = make_update<Side::Sell>(103, 2, 200);
    assert(
        writer.replace<Side::Sell>(refreshed_ask) ==
        MarketStateUpdateStatus::Applied
    );
    const auto bbo = state.read_bbo();
    assert(bbo.bid_price_ticks == 100);
    assert(bbo.ask_price_ticks == 103);
    assert(bbo.ask_generation == 2);

    // A rising market can atomically move both sides through the old ask.
    // Applying bid=103 alone would cross old ask=103, but the paired update is
    // valid because the new ask is 105.
    auto moved_bid = make_update<Side::Buy>(103, 2, 300);
    auto moved_ask = make_update<Side::Sell>(105, 3, 300);
    assert(
        writer.replace_book(moved_bid, moved_ask) ==
        MarketStateUpdateStatus::Applied
    );
    const auto moved_bbo = state.read_bbo();
    assert(moved_bbo.bid_price_ticks == 103);
    assert(moved_bbo.ask_price_ticks == 105);
}

}  // namespace

int main() {
    test_cache_layout_contract();
    test_single_writer_and_bbo();
    test_prefix_read_matches_full_identity();
    test_prefix_bounds_are_rejected();
    test_monotonic_clock_and_generation();
    test_depth_update_validation_and_crossed_book();
    return 0;
}
