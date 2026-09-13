#pragma once

#include "common.hpp"

#include <cstdint>
#include <vector>

namespace narrowgate_cpp {

struct RequestStateFeatureResult {
    std::size_t rows = 0;
    std::size_t windows = 0;

    std::vector<std::uint8_t> valid_book;
    std::vector<std::int64_t> book_source_ts_ms;
    std::vector<double> book_age_ms;
    std::vector<double> best_bid;
    std::vector<double> best_ask;
    std::vector<double> bid_qty;
    std::vector<double> ask_qty;
    std::vector<double> mid;
    std::vector<double> bbo_spread_ticks;
    std::vector<double> book_imbalance;
    std::vector<double> microprice_shift_bps;
    std::vector<double> l2_near_depth_total;
    std::vector<double> l2_quote_flip_rate;
    std::vector<double> l2_book_refresh_ratio;
    std::vector<double> l2_book_cancel_ratio;

    std::vector<std::int64_t> active_order_count;
    std::vector<std::int64_t> pending_cancel_before_count;
    std::vector<std::int64_t> request_batch_size;

    // Row-major matrices with shape [rows, windows].
    std::vector<double> market_return_bps;
    std::vector<double> aggressive_buy_qty;
    std::vector<double> aggressive_sell_qty;
    std::vector<double> taker_imbalance;
    std::vector<std::int64_t> trade_count;
    std::vector<std::int64_t> book_update_count;
};

[[nodiscard]] RequestStateFeatureResult compute_request_state_features(
    ArrayView<std::int64_t> request_ts_ms,
    ArrayView<std::int64_t> book_cutoff_ts_ms,
    ArrayView<std::int64_t> trade_cutoff_ts_ms,
    ArrayView<std::int64_t> activation_ts_ms,
    ArrayView<std::int64_t> terminal_ts_ms,
    ArrayView<std::int64_t> cancel_ack_ts_ms,
    ArrayView<std::int64_t> bbo_ts_ms,
    ArrayView<double> bbo_best_bid,
    ArrayView<double> bbo_best_ask,
    ArrayView<double> bbo_bid_qty,
    ArrayView<double> bbo_ask_qty,
    ArrayView<std::int64_t> l2_ts_ms,
    MatrixView<double> l2_bid_px,
    MatrixView<double> l2_bid_qty,
    MatrixView<double> l2_ask_px,
    MatrixView<double> l2_ask_qty,
    ArrayView<std::int64_t> trade_ts_ms,
    ArrayView<double> trade_price,
    ArrayView<double> trade_qty,
    ArrayView<std::uint8_t> is_buyer_maker,
    ArrayView<std::int64_t> windows_ms,
    double tick_size,
    std::int64_t depth_levels,
    std::int64_t l2_path_lookback_ms
);

}  // namespace narrowgate_cpp
