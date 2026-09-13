#include "narrowgate_cpp/live_runtime_core.hpp"

#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>

using namespace narrowgate_cpp;

namespace {

Depth20SideUpdate side_update(
    Side side,
    std::int64_t best_ticks,
    std::uint64_t generation,
    std::uint64_t timestamp_ns
) {
    Depth20SideUpdate update;
    update.size = 3;
    update.clock = MarketClockIdentity{
        timestamp_ns - 3,
        timestamp_ns - 2,
        timestamp_ns - 1,
        timestamp_ns,
        generation,
    };
    for (std::size_t index = 0; index < update.size; ++index) {
        const auto offset = static_cast<std::int64_t>(index);
        update.price_ticks[index] = side == Side::Buy
            ? best_ticks - offset
            : best_ticks + offset;
        update.quantity_lots[index] = 100 - offset;
    }
    return update;
}

QuoteCoreConfig config() {
    QuoteCoreConfig value;
    value.gamma = 0.046;
    value.eta_inventory = 0.046;
    value.a_spread = 0.046;
    value.inventory_reference_qty = 1.0;
    value.kappa = 0.05;
    value.tick_size = 0.1;
    value.lot_size = 0.001;
    value.order_size = 0.001;
    value.max_inventory = 0.026;
    value.ml_enabled = false;
    value.use_bar_pricing = false;
    value.max_spread_bps = 20.0;
    return value;
}

NativeLiveDecisionInput decision_input(std::uint64_t timestamp_ns) {
    NativeLiveDecisionInput input;
    input.decision_ts_ns = timestamp_ns;
    input.expected_market_publication_sequence = 2;
    input.expected_bid_generation = 1;
    input.expected_ask_generation = 1;
    input.max_book_age_ns = 1'000'000'000;
    input.quote_state.mid = 63'400.05;
    input.quote_state.inventory = 0.001;
    input.quote_state.sigma_sq = 2.0;
    input.quote_state.trade_intensity = 200.0;
    input.quote_state.mo_ref = 50.0;
    input.prediction = QuotePrediction{};
    input.min_qty = 0.001;
    input.min_notional = 5.0;
    input.requote_threshold_bps = 0.1;
    input.buy_policy.exposure_increasing = true;
    input.sell_policy.exposure_increasing = false;
    input.buy_policy.max_book_age_s = 1.0;
    input.sell_policy.max_book_age_s = 1.0;
    return input;
}

}  // namespace

int main() {
    NativeLiveRuntimeCore core(config());
    const auto bids = side_update(Side::Buy, 634'000, 1, 1'000'000'000);
    const auto asks = side_update(Side::Sell, 634'001, 1, 1'000'000'000);
    assert(core.publish_book(bids, asks) == MarketStateUpdateStatus::Applied);

    const auto book = core.book_snapshot();
    assert(book.valid());
    assert(book.bbo.bid_price_ticks == 634'000);
    assert(book.bbo.ask_price_ticks == 634'001);
    assert(book.bids.clock.generation == 1);
    assert(book.asks.clock.generation == 1);

    const auto first = core.decide(decision_input(1'100'000'000));
    assert(first.status == NativeLiveDecisionStatus::Applied);
    assert(first.decision_sequence == 1);
    assert(first.market_publication_sequence > 0);
    assert(first.book_age_ns == 100'000'000);
    assert(std::isfinite(first.quote.bid_price));
    assert(std::isfinite(first.quote.ask_price));
    assert(first.quote.bid_price < first.quote.ask_price);
    assert(first.routing.bid_price < first.routing.ask_price);
    assert(first.routing.bid_size >= 0.0);
    assert(first.routing.ask_size >= 0.0);

    auto invalid = decision_input(1'100'000'001);
    invalid.prediction.tox_bid = std::numeric_limits<double>::quiet_NaN();
    assert(
        core.decide(invalid).status == NativeLiveDecisionStatus::InvalidInput
    );

    auto regressed = decision_input(1'050'000'000);
    assert(
        core.decide(regressed).status ==
        NativeLiveDecisionStatus::DecisionClockRegressed
    );

    auto stale = decision_input(2'100'000'000);
    stale.max_book_age_ns = 1'000'000'000;
    assert(core.decide(stale).status == NativeLiveDecisionStatus::StaleBook);

    auto bad_asks = side_update(
        Side::Sell,
        633'999,
        2,
        1'200'000'000
    );
    auto next_bids = side_update(
        Side::Buy,
        634'000,
        2,
        1'200'000'000
    );
    assert(
        core.publish_book(next_bids, bad_asks) ==
        MarketStateUpdateStatus::CrossedBook
    );
    assert(core.feed_fault_latched());
    assert(
        core.decide(decision_input(1'200'000'001)).status ==
        NativeLiveDecisionStatus::FeedFault
    );
    const auto next_asks = side_update(
        Side::Sell,
        634'001,
        2,
        1'200'000'000
    );
    assert(
        core.publish_book(next_bids, next_asks) ==
        MarketStateUpdateStatus::Applied
    );
    assert(!core.feed_fault_latched());
    assert(core.feed_fault_epoch() == core.feed_resync_epoch());

    assert(core.decision_count() == 1);
    assert(NativeLiveRuntimeCore::cache_line_bytes() == 64 ||
           NativeLiveRuntimeCore::cache_line_bytes() == 128);
    return 0;
}
