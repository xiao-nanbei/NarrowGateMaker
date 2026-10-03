#pragma once

#include "common.hpp"

#include <array>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

namespace narrowgate_cpp {

struct GlobalFlowMarketWindow {
    std::string market_id;
    int horizon_ms = 0;
    std::size_t book_events = 0;
    std::size_t trade_events = 0;
    double book_age_ms = std::numeric_limits<double>::infinity();
    double trade_age_ms = std::numeric_limits<double>::infinity();
    bool book_fresh = false;
    double aggressive_buy_volume = 0.0;
    double aggressive_sell_volume = 0.0;
    double trade_imbalance = 0.0;
    double l1_ofi = 0.0;
    double l1_ofi_normalized = 0.0;
    double bid_depletion = 0.0;
    double bid_refill = 0.0;
    double ask_depletion = 0.0;
    double ask_refill = 0.0;
    double mid_move_bps = std::numeric_limits<double>::quiet_NaN();
    double flow_pressure = 0.0;
    std::size_t gap_events = 0;
    std::size_t gap_known_events = 0;
    std::uint64_t out_of_order_events = 0;
    std::uint64_t stale_trade_events = 0;
    std::uint64_t book_overflow_events = 0;
    std::uint64_t trade_overflow_events = 0;
};

struct GlobalFlowStats {
    std::size_t market_count = 0;
    std::uint64_t book_events_seen = 0;
    std::uint64_t book_events_accepted = 0;
    std::uint64_t trade_batches = 0;
    std::uint64_t trade_events_seen = 0;
    std::uint64_t trade_events_accepted = 0;
    std::uint64_t out_of_order_events = 0;
    std::uint64_t stale_trade_events = 0;
    std::uint64_t book_overflow_events = 0;
    std::uint64_t trade_overflow_events = 0;
};

class NativeGlobalFlowEngine {
public:
    explicit NativeGlobalFlowEngine(
        int retention_ms = 2000,
        double max_source_age_ms = 1000.0,
        double max_trade_event_age_ms = 1000.0
    );

    NativeGlobalFlowEngine(const NativeGlobalFlowEngine&) = delete;
    NativeGlobalFlowEngine& operator=(const NativeGlobalFlowEngine&) = delete;

    void clear();

    bool on_book(
        std::string_view market_id,
        std::int64_t receive_ts_ns,
        double bid,
        double bid_size,
        double ask,
        double ask_size,
        int gap_flag = -1
    );

    bool on_trade(
        std::string_view market_id,
        std::int64_t receive_ts_ns,
        std::int64_t exchange_ts_ns,
        double price,
        double size,
        bool is_buyer_maker
    );

    std::size_t on_trade_batch(
        std::string_view market_id,
        std::int64_t receive_ts_ns,
        ArrayView<std::int64_t> exchange_ts_ns,
        ArrayView<double> prices,
        ArrayView<double> sizes,
        ArrayView<std::uint8_t> is_buyer_maker
    );

    [[nodiscard]] GlobalFlowMarketWindow market_window(
        std::string_view market_id,
        std::int64_t now_ns,
        int horizon_ms
    ) const;

    [[nodiscard]] GlobalFlowStats stats() const;

    static constexpr std::size_t max_markets() noexcept { return kMaxMarkets; }
    static constexpr std::size_t trade_capacity_per_market() noexcept {
        return kTradeCapacity;
    }
    static constexpr std::size_t book_capacity_per_market() noexcept {
        return kBookCapacity;
    }

private:
    struct BookEvent {
        std::int64_t receive_ns = 0;
        double bid = 0.0;
        double bid_size = 0.0;
        double ask = 0.0;
        double ask_size = 0.0;
        double mid = 0.0;
        double l1_ofi = 0.0;
        double bid_depletion = 0.0;
        double bid_refill = 0.0;
        double ask_depletion = 0.0;
        double ask_refill = 0.0;
        std::int8_t gap_flag = -1;
    };

    struct TradeEvent {
        std::int64_t receive_ns = 0;
        double price = 0.0;
        double size = 0.0;
        bool aggressor_buy = false;
    };

    template <typename T, std::size_t Capacity>
    class FixedRing {
    public:
        void clear() noexcept {
            head_ = 0;
            size_ = 0;
        }

        [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
        [[nodiscard]] std::size_t size() const noexcept { return size_; }
        [[nodiscard]] constexpr std::size_t capacity() const noexcept {
            return Capacity;
        }

        [[nodiscard]] const T& operator[](std::size_t index) const noexcept {
            return values_[(head_ + index) % Capacity];
        }

        bool push_back(const T& value) noexcept {
            const bool overflow = size_ == Capacity;
            if (overflow) {
                values_[head_] = value;
                head_ = (head_ + 1) % Capacity;
                return true;
            }
            values_[(head_ + size_) % Capacity] = value;
            ++size_;
            return false;
        }

        void pop_front() noexcept {
            if (size_ == 0) {
                return;
            }
            head_ = (head_ + 1) % Capacity;
            --size_;
        }

    private:
        std::array<T, Capacity> values_{};
        std::size_t head_ = 0;
        std::size_t size_ = 0;
    };

    static constexpr std::size_t kMaxMarkets = 16;
    static constexpr std::size_t kTradeCapacity = 32768;
    static constexpr std::size_t kBookCapacity = 8192;

    struct MarketSlot {
        std::string market_id;
        FixedRing<BookEvent, kBookCapacity> books;
        FixedRing<TradeEvent, kTradeCapacity> trades;
        std::optional<BookEvent> last_book;
        std::int64_t last_receive_ns = 0;
        std::uint64_t out_of_order_events = 0;
        std::uint64_t stale_trade_events = 0;
        std::uint64_t book_overflow_events = 0;
        std::uint64_t trade_overflow_events = 0;

        [[nodiscard]] bool occupied() const noexcept { return !market_id.empty(); }
        void clear();
    };

    MarketSlot& slot_for(std::string_view market_id);
    [[nodiscard]] const MarketSlot* find_slot(std::string_view market_id) const;
    bool on_trade_locked(
        MarketSlot& slot,
        std::int64_t receive_ts_ns,
        std::int64_t exchange_ts_ns,
        double price,
        double size,
        bool is_buyer_maker
    );
    void prune(MarketSlot& slot, std::int64_t now_ns) const;
    [[nodiscard]] GlobalFlowMarketWindow market_window_locked(
        const MarketSlot* slot,
        std::string_view market_id,
        std::int64_t now_ns,
        int horizon_ms
    ) const;

    mutable std::mutex mutex_;
    std::array<MarketSlot, kMaxMarkets> markets_{};
    std::int64_t retention_ns_ = 2'000'000'000LL;
    double max_source_age_ms_ = 1000.0;
    std::int64_t max_trade_event_age_ns_ = 1'000'000'000LL;
    std::uint64_t book_events_seen_ = 0;
    std::uint64_t book_events_accepted_ = 0;
    std::uint64_t trade_batches_ = 0;
    std::uint64_t trade_events_seen_ = 0;
    std::uint64_t trade_events_accepted_ = 0;
};

}  // namespace narrowgate_cpp
