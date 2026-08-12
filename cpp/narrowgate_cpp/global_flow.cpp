#include "global_flow.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace narrowgate_cpp {

namespace {

double safe_log_bps(double newer, double older) noexcept {
    return newer > 0.0 && older > 0.0
        ? std::log(newer / older) * 10'000.0
        : std::numeric_limits<double>::quiet_NaN();
}

}  // namespace

void NativeGlobalFlowEngine::MarketSlot::clear() {
    market_id.clear();
    books.clear();
    trades.clear();
    last_book.reset();
    last_receive_ns = 0;
    out_of_order_events = 0;
    stale_trade_events = 0;
    book_overflow_events = 0;
    trade_overflow_events = 0;
}

NativeGlobalFlowEngine::NativeGlobalFlowEngine(
    int retention_ms,
    double max_source_age_ms,
    double max_trade_event_age_ms
) : retention_ns_(std::max(1, retention_ms) * 1'000'000LL),
    max_source_age_ms_(std::max(1.0, max_source_age_ms)),
    max_trade_event_age_ns_(
        static_cast<std::int64_t>(
            std::max(1.0, max_trade_event_age_ms) * 1'000'000.0
        )
    ) {}

void NativeGlobalFlowEngine::clear() {
    const std::scoped_lock lock(mutex_);
    for (auto& market : markets_) {
        market.clear();
    }
    book_events_seen_ = 0;
    book_events_accepted_ = 0;
    trade_batches_ = 0;
    trade_events_seen_ = 0;
    trade_events_accepted_ = 0;
}

NativeGlobalFlowEngine::MarketSlot& NativeGlobalFlowEngine::slot_for(
    std::string_view market_id
) {
    if (market_id.empty()) {
        throw std::invalid_argument("global-flow market_id cannot be empty");
    }
    for (auto& market : markets_) {
        if (market.market_id == market_id) {
            return market;
        }
    }
    for (auto& market : markets_) {
        if (!market.occupied()) {
            market.market_id.assign(market_id);
            return market;
        }
    }
    throw std::runtime_error("native global-flow market capacity exceeded");
}

const NativeGlobalFlowEngine::MarketSlot* NativeGlobalFlowEngine::find_slot(
    std::string_view market_id
) const {
    for (const auto& market : markets_) {
        if (market.market_id == market_id) {
            return &market;
        }
    }
    return nullptr;
}

void NativeGlobalFlowEngine::prune(MarketSlot& slot, std::int64_t now_ns) const {
    const std::int64_t cutoff = now_ns - retention_ns_;
    // Keep one pre-window book anchor for return calculations.
    while (slot.books.size() > 1 && slot.books[1].receive_ns < cutoff) {
        slot.books.pop_front();
    }
    while (!slot.trades.empty() && slot.trades[0].receive_ns < cutoff) {
        slot.trades.pop_front();
    }
}

bool NativeGlobalFlowEngine::on_book(
    std::string_view market_id,
    std::int64_t receive_ts_ns,
    double bid,
    double bid_size,
    double ask,
    double ask_size,
    int gap_flag
) {
    const std::scoped_lock lock(mutex_);
    ++book_events_seen_;
    bid_size = std::max(0.0, bid_size);
    ask_size = std::max(0.0, ask_size);
    if (receive_ts_ns <= 0 || bid <= 0.0 || ask <= bid) {
        return false;
    }

    auto& slot = slot_for(market_id);
    if (receive_ts_ns < slot.last_receive_ns) {
        ++slot.out_of_order_events;
        return false;
    }
    prune(slot, receive_ts_ns);

    double l1_ofi = 0.0;
    double bid_depletion = 0.0;
    double bid_refill = 0.0;
    double ask_depletion = 0.0;
    double ask_refill = 0.0;
    if (slot.last_book.has_value()) {
        const auto& previous = *slot.last_book;
        double bid_component = 0.0;
        if (bid > previous.bid) {
            bid_component = bid_size;
            bid_refill = bid_size;
        } else if (bid < previous.bid) {
            bid_component = -previous.bid_size;
            bid_depletion = previous.bid_size;
        } else {
            bid_component = bid_size - previous.bid_size;
            bid_depletion = std::max(0.0, previous.bid_size - bid_size);
            bid_refill = std::max(0.0, bid_size - previous.bid_size);
        }

        double ask_component = 0.0;
        if (ask < previous.ask) {
            ask_component = -ask_size;
            ask_refill = ask_size;
        } else if (ask > previous.ask) {
            ask_component = previous.ask_size;
            ask_depletion = previous.ask_size;
        } else {
            ask_component = previous.ask_size - ask_size;
            ask_depletion = std::max(0.0, previous.ask_size - ask_size);
            ask_refill = std::max(0.0, ask_size - previous.ask_size);
        }
        l1_ofi = bid_component + ask_component;
    }

    const BookEvent event{
        .receive_ns = receive_ts_ns,
        .bid = bid,
        .bid_size = bid_size,
        .ask = ask,
        .ask_size = ask_size,
        .mid = 0.5 * (bid + ask),
        .l1_ofi = l1_ofi,
        .bid_depletion = bid_depletion,
        .bid_refill = bid_refill,
        .ask_depletion = ask_depletion,
        .ask_refill = ask_refill,
        .gap_flag = static_cast<std::int8_t>(gap_flag < 0 ? -1 : (gap_flag != 0)),
    };
    slot.book_overflow_events += slot.books.push_back(event) ? 1U : 0U;
    slot.last_book = event;
    slot.last_receive_ns = receive_ts_ns;
    ++book_events_accepted_;
    return true;
}

bool NativeGlobalFlowEngine::on_trade_locked(
    MarketSlot& slot,
    std::int64_t receive_ts_ns,
    std::int64_t exchange_ts_ns,
    double price,
    double size,
    bool is_buyer_maker
) {
    if (receive_ts_ns <= 0 || price <= 0.0 || size <= 0.0) {
        return false;
    }
    if (
        exchange_ts_ns > 0 && receive_ts_ns >= exchange_ts_ns &&
        receive_ts_ns - exchange_ts_ns > max_trade_event_age_ns_
    ) {
        ++slot.stale_trade_events;
        return false;
    }
    if (receive_ts_ns < slot.last_receive_ns) {
        ++slot.out_of_order_events;
        return false;
    }
    prune(slot, receive_ts_ns);
    const TradeEvent event{
        .receive_ns = receive_ts_ns,
        .price = price,
        .size = size,
        .aggressor_buy = !is_buyer_maker,
    };
    slot.trade_overflow_events += slot.trades.push_back(event) ? 1U : 0U;
    slot.last_receive_ns = receive_ts_ns;
    return true;
}

bool NativeGlobalFlowEngine::on_trade(
    std::string_view market_id,
    std::int64_t receive_ts_ns,
    std::int64_t exchange_ts_ns,
    double price,
    double size,
    bool is_buyer_maker
) {
    const std::scoped_lock lock(mutex_);
    ++trade_batches_;
    ++trade_events_seen_;
    auto& slot = slot_for(market_id);
    const bool accepted = on_trade_locked(
        slot,
        receive_ts_ns,
        exchange_ts_ns,
        price,
        size,
        is_buyer_maker
    );
    trade_events_accepted_ += accepted ? 1U : 0U;
    return accepted;
}

std::size_t NativeGlobalFlowEngine::on_trade_batch(
    std::string_view market_id,
    std::int64_t receive_ts_ns,
    ArrayView<std::int64_t> exchange_ts_ns,
    ArrayView<double> prices,
    ArrayView<double> sizes,
    ArrayView<std::uint8_t> is_buyer_maker
) {
    const std::size_t count = prices.size();
    if (
        exchange_ts_ns.size() != count || sizes.size() != count ||
        is_buyer_maker.size() != count
    ) {
        throw std::invalid_argument("native global-flow trade arrays must have equal length");
    }
    const std::scoped_lock lock(mutex_);
    ++trade_batches_;
    trade_events_seen_ += count;
    auto& slot = slot_for(market_id);
    std::size_t accepted = 0;
    for (std::size_t index = 0; index < count; ++index) {
        accepted += on_trade_locked(
            slot,
            receive_ts_ns,
            exchange_ts_ns[index],
            prices[index],
            sizes[index],
            is_buyer_maker[index] != 0
        ) ? 1U : 0U;
    }
    trade_events_accepted_ += accepted;
    return accepted;
}

GlobalFlowMarketWindow NativeGlobalFlowEngine::market_window(
    std::string_view market_id,
    std::int64_t now_ns,
    int horizon_ms
) const {
    const std::scoped_lock lock(mutex_);
    return market_window_locked(find_slot(market_id), market_id, now_ns, horizon_ms);
}

GlobalFlowMarketWindow NativeGlobalFlowEngine::market_window_locked(
    const MarketSlot* slot,
    std::string_view market_id,
    std::int64_t now_ns,
    int horizon_ms
) const {
    GlobalFlowMarketWindow out;
    out.market_id.assign(market_id);
    out.horizon_ms = std::max(1, horizon_ms);
    if (slot == nullptr) {
        return out;
    }
    out.out_of_order_events = slot->out_of_order_events;
    out.stale_trade_events = slot->stale_trade_events;
    out.book_overflow_events = slot->book_overflow_events;
    out.trade_overflow_events = slot->trade_overflow_events;

    const std::int64_t cutoff = now_ns - static_cast<std::int64_t>(out.horizon_ms) * 1'000'000LL;
    const BookEvent* prior_book = nullptr;
    const BookEvent* first_window_book = nullptr;
    const BookEvent* latest_book = nullptr;
    double top_depth_sum = 0.0;
    for (std::size_t index = 0; index < slot->books.size(); ++index) {
        const auto& event = slot->books[index];
        if (event.receive_ns <= cutoff) {
            prior_book = &event;
        }
        if (event.receive_ns <= now_ns) {
            latest_book = &event;
        } else {
            break;
        }
        if (event.receive_ns <= cutoff) {
            continue;
        }
        if (first_window_book == nullptr) {
            first_window_book = &event;
        }
        ++out.book_events;
        out.l1_ofi += event.l1_ofi;
        out.bid_depletion += event.bid_depletion;
        out.bid_refill += event.bid_refill;
        out.ask_depletion += event.ask_depletion;
        out.ask_refill += event.ask_refill;
        top_depth_sum += event.bid_size + event.ask_size;
        out.gap_events += event.gap_flag == 1 ? 1U : 0U;
        out.gap_known_events += event.gap_flag >= 0 ? 1U : 0U;
    }
    if (prior_book == nullptr) {
        prior_book = first_window_book;
    }

    const TradeEvent* latest_trade = nullptr;
    for (std::size_t index = 0; index < slot->trades.size(); ++index) {
        const auto& event = slot->trades[index];
        if (event.receive_ns <= now_ns) {
            latest_trade = &event;
        } else {
            break;
        }
        if (event.receive_ns <= cutoff) {
            continue;
        }
        ++out.trade_events;
        if (event.aggressor_buy) {
            out.aggressive_buy_volume += event.size;
        } else {
            out.aggressive_sell_volume += event.size;
        }
    }

    const double total_volume = out.aggressive_buy_volume + out.aggressive_sell_volume;
    out.trade_imbalance = total_volume > 0.0
        ? (out.aggressive_buy_volume - out.aggressive_sell_volume) / total_volume
        : 0.0;
    const double average_top_depth = out.book_events > 0
        ? top_depth_sum / static_cast<double>(out.book_events)
        : (latest_book != nullptr ? latest_book->bid_size + latest_book->ask_size : 0.0);
    out.l1_ofi_normalized = out.l1_ofi / std::max(average_top_depth, 1e-12);
    out.flow_pressure = 0.5 * out.trade_imbalance + 0.5 * std::tanh(out.l1_ofi_normalized);
    if (prior_book != nullptr && latest_book != nullptr) {
        out.mid_move_bps = safe_log_bps(latest_book->mid, prior_book->mid);
    }
    if (latest_book != nullptr) {
        out.book_age_ms = std::max(
            0.0,
            static_cast<double>(now_ns - latest_book->receive_ns) / 1'000'000.0
        );
        out.book_fresh = out.book_age_ms <= max_source_age_ms_;
    }
    if (latest_trade != nullptr) {
        out.trade_age_ms = std::max(
            0.0,
            static_cast<double>(now_ns - latest_trade->receive_ns) / 1'000'000.0
        );
    }
    return out;
}

GlobalFlowStats NativeGlobalFlowEngine::stats() const {
    const std::scoped_lock lock(mutex_);
    GlobalFlowStats out;
    out.book_events_seen = book_events_seen_;
    out.book_events_accepted = book_events_accepted_;
    out.trade_batches = trade_batches_;
    out.trade_events_seen = trade_events_seen_;
    out.trade_events_accepted = trade_events_accepted_;
    for (const auto& market : markets_) {
        if (!market.occupied()) {
            continue;
        }
        ++out.market_count;
        out.out_of_order_events += market.out_of_order_events;
        out.stale_trade_events += market.stale_trade_events;
        out.book_overflow_events += market.book_overflow_events;
        out.trade_overflow_events += market.trade_overflow_events;
    }
    return out;
}

}  // namespace narrowgate_cpp
