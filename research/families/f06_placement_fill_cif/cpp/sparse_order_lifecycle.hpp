#pragma once

#include "common.hpp"

#include <cstdint>
#include <vector>

namespace narrowgate_cpp {

struct SparseOrderLifecycleResult {
    std::vector<std::uint8_t> activation_status;
    std::vector<std::uint8_t> activation_queue_status;
    std::vector<std::uint8_t> queue_path_valid;
    std::vector<std::uint8_t> queue_invalid_reason;
    std::vector<std::int64_t> first_touch_ts_ms;
    std::vector<std::uint8_t> first_touch_type;
    std::vector<std::int64_t> exact_touch_ts_ms;
    std::vector<std::int64_t> through_touch_ts_ms;
    std::vector<std::int64_t> first_fill_ts_ms;
    std::vector<std::uint8_t> first_fill_mechanism;
    std::vector<double> fill_qty;
    std::vector<double> remaining_qty;
    std::vector<std::int64_t> full_fill_ts_ms;
    std::vector<std::int64_t> partial_fill_count;
    std::vector<std::uint8_t> request_state_observed;
    std::vector<std::uint8_t> request_order_state_before;
    std::vector<double> request_order_age_ms;
    std::vector<double> request_remaining_qty;
    std::vector<double> request_queue_left;
    std::vector<std::uint8_t> request_queue_path_valid;
    std::vector<std::int64_t> request_native_cancel_count;
    std::vector<double> request_native_cancel_qty;
    std::vector<std::int64_t> request_native_refill_count;
    std::vector<double> request_native_refill_qty;
    std::vector<std::int64_t> request_native_level_event_count;
    std::vector<std::uint8_t> cancel_acked;
    std::vector<double> fill_while_cancel_pending_qty;
    std::vector<std::int64_t> first_pending_cancel_fill_ts_ms;
    std::vector<std::uint8_t> terminal_state;
    std::vector<std::int64_t> terminal_ts_ms;
    std::vector<std::uint8_t> terminal_reason;
    std::vector<std::int64_t> native_cancel_count;
    std::vector<double> native_cancel_qty;
    std::vector<std::int64_t> native_refill_count;
    std::vector<double> native_refill_qty;
    std::vector<std::int64_t> native_level_event_count;
    std::vector<std::int64_t> same_ms_ambiguity_count;
};

[[nodiscard]] SparseOrderLifecycleResult simulate_sparse_order_lifecycles(
    ArrayView<std::uint8_t> order_side,
    ArrayView<std::int64_t> order_price_tick,
    ArrayView<double> order_quantity,
    ArrayView<std::int64_t> activate_ts_ms,
    ArrayView<std::int64_t> cancel_request_ts_ms,
    ArrayView<std::int64_t> cancel_ack_ts_ms,
    ArrayView<std::int64_t> stop_ts_ms,
    ArrayView<std::uint8_t> seed_status,
    ArrayView<double> seed_qty,
    ArrayView<std::int64_t> seed_best_bid_tick,
    ArrayView<std::int64_t> seed_best_ask_tick,
    ArrayView<std::uint8_t> seed_ambiguous,
    ArrayView<std::int64_t> event_order_index,
    ArrayView<std::int64_t> event_ts_ms,
    ArrayView<double> event_qty_after,
    ArrayView<std::uint8_t> event_code,
    ArrayView<std::uint8_t> event_state_valid,
    ArrayView<std::uint8_t> event_ambiguous,
    ArrayView<std::int64_t> trade_ts_ms,
    ArrayView<std::int64_t> trade_price_tick,
    ArrayView<double> trade_qty,
    ArrayView<std::uint8_t> is_buyer_maker,
    double lot_size,
    double queue_deplete_mult
);

}  // namespace narrowgate_cpp
