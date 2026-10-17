#include "sparse_order_lifecycle.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string>

namespace narrowgate_cpp {
namespace {

enum class OrderState : std::uint8_t {
    open = 1,
    pending_cancel = 2,
    filled = 3,
    cancelled = 4,
    rejected = 5,
    censored = 6,
};

enum class ActivationStatus : std::uint8_t {
    active = 1,
    gtx_reject = 2,
    invalid_book = 3,
    cancelled_before_activation = 4,
};

enum class QueueInvalidReason : std::uint8_t {
    none = 0,
    seed_unavailable = 1,
    same_boundary_activation_book_event = 2,
    same_ms_native_trade_or_activation = 3,
    native_sequence_invalidated = 4,
    native_snapshot_reset = 5,
};

enum class TouchType : std::uint8_t { none = 0, exact = 1, through = 2 };
enum class FillMechanism : std::uint8_t {
    none = 0,
    exact_queue = 1,
    strict_through = 2,
};
enum class TerminalReason : std::uint8_t {
    none = 0,
    exact_queue = 1,
    strict_through = 2,
    cancel_ack = 3,
    administrative_censor = 4,
    gtx_reject = 5,
    invalid_book = 6,
    cancel_ack_before_activation = 7,
};

template <typename... Views>
void require_same_size(std::size_t expected, const Views&... views) {
    if (!((views.size() == expected) && ...)) {
        throw std::invalid_argument("sparse lifecycle order arrays must align");
    }
}

void reserve_result(SparseOrderLifecycleResult& out, std::size_t n) {
    out.activation_status.resize(n);
    out.activation_queue_status.resize(n);
    out.queue_path_valid.resize(n);
    out.queue_invalid_reason.resize(n);
    out.first_touch_ts_ms.resize(n);
    out.first_touch_type.resize(n);
    out.exact_touch_ts_ms.resize(n);
    out.through_touch_ts_ms.resize(n);
    out.first_fill_ts_ms.resize(n);
    out.first_fill_mechanism.resize(n);
    out.fill_qty.resize(n);
    out.remaining_qty.resize(n);
    out.full_fill_ts_ms.resize(n);
    out.partial_fill_count.resize(n);
    out.request_state_observed.resize(n);
    out.request_order_state_before.resize(n);
    out.request_order_age_ms.resize(
        n, std::numeric_limits<double>::quiet_NaN()
    );
    out.request_remaining_qty.resize(
        n, std::numeric_limits<double>::quiet_NaN()
    );
    out.request_queue_left.resize(
        n, std::numeric_limits<double>::quiet_NaN()
    );
    out.request_queue_path_valid.resize(n);
    out.request_native_cancel_count.resize(n);
    out.request_native_cancel_qty.resize(n);
    out.request_native_refill_count.resize(n);
    out.request_native_refill_qty.resize(n);
    out.request_native_level_event_count.resize(n);
    out.cancel_acked.resize(n);
    out.fill_while_cancel_pending_qty.resize(n);
    out.first_pending_cancel_fill_ts_ms.resize(n);
    out.terminal_state.resize(n);
    out.terminal_ts_ms.resize(n);
    out.terminal_reason.resize(n);
    out.native_cancel_count.resize(n);
    out.native_cancel_qty.resize(n);
    out.native_refill_count.resize(n);
    out.native_refill_qty.resize(n);
    out.native_level_event_count.resize(n);
    out.same_ms_ambiguity_count.resize(n);
}

double sparse_floor_lot(double quantity, double lot_size) {
    const double units = std::floor(
        std::max(0.0, quantity) / lot_size + 1e-12
    );
    return units * lot_size;
}

}  // namespace

SparseOrderLifecycleResult simulate_sparse_order_lifecycles(
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
) {
    const std::size_t n = order_side.size();
    require_same_size(
        n,
        order_price_tick,
        order_quantity,
        activate_ts_ms,
        cancel_request_ts_ms,
        cancel_ack_ts_ms,
        stop_ts_ms,
        seed_status,
        seed_qty,
        seed_best_bid_tick,
        seed_best_ask_tick,
        seed_ambiguous
    );
    const std::size_t event_count = event_order_index.size();
    require_same_size(
        event_count,
        event_ts_ms,
        event_qty_after,
        event_code,
        event_state_valid,
        event_ambiguous
    );
    const std::size_t trade_count = trade_ts_ms.size();
    require_same_size(
        trade_count, trade_price_tick, trade_qty, is_buyer_maker
    );
    if (!(lot_size > 0.0) || !(queue_deplete_mult >= 0.0)) {
        throw std::invalid_argument("lot_size and queue_deplete_mult are invalid");
    }
    if (!std::is_sorted(trade_ts_ms.begin(), trade_ts_ms.end())) {
        throw std::invalid_argument("trade timestamps must be sorted");
    }

    std::vector<std::size_t> event_begin(n + 1, 0);
    std::int64_t prior_order = -1;
    std::int64_t prior_ts = -1;
    for (std::size_t i = 0; i < event_count; ++i) {
        const std::int64_t order = event_order_index[i];
        if (order < 0 || static_cast<std::size_t>(order) >= n) {
            throw std::invalid_argument("event order index is out of range");
        }
        if (order < prior_order || (order == prior_order && event_ts_ms[i] < prior_ts)) {
            throw std::invalid_argument("events must be sorted by order and timestamp");
        }
        prior_order = order;
        prior_ts = event_ts_ms[i];
    }
    std::size_t event_cursor = 0;
    for (std::size_t order = 0; order < n; ++order) {
        event_begin[order] = event_cursor;
        while (
            event_cursor < event_count
            && static_cast<std::size_t>(event_order_index[event_cursor]) == order
        ) {
            ++event_cursor;
        }
    }
    event_begin[n] = event_cursor;
    if (event_cursor != event_count) {
        throw std::invalid_argument("event order index exceeds order count");
    }

    SparseOrderLifecycleResult out;
    reserve_result(out, n);
    for (std::size_t row = 0; row < n; ++row) {
        const bool buy = order_side[row] == 1;
        if (!buy && order_side[row] != 2) {
            throw std::invalid_argument("order_side must encode BUY=1 or SELL=2");
        }
        const std::int64_t activate = activate_ts_ms[row];
        const std::int64_t request = cancel_request_ts_ms[row];
        const std::int64_t ack = cancel_ack_ts_ms[row];
        const std::int64_t stop = stop_ts_ms[row];
        if (activate <= 0 || stop <= activate || order_quantity[row] <= 0.0) {
            throw std::invalid_argument("sparse lifecycle timing/quantity is invalid");
        }

        double remaining = order_quantity[row];
        double filled = 0.0;
        double public_qty = std::isfinite(seed_qty[row])
            ? std::max(0.0, seed_qty[row])
            : std::numeric_limits<double>::quiet_NaN();
        double queue_left = public_qty;
        double queue_trade_since_update = 0.0;
        bool queue_valid = seed_status[row] != 0 && seed_ambiguous[row] == 0;
        std::uint8_t invalid_reason = queue_valid
            ? static_cast<std::uint8_t>(QueueInvalidReason::none)
            : static_cast<std::uint8_t>(
                  seed_ambiguous[row] != 0
                      ? QueueInvalidReason::same_boundary_activation_book_event
                      : QueueInvalidReason::seed_unavailable
              );
        std::int64_t cancel_count = 0;
        double cancel_qty = 0.0;
        std::int64_t refill_count = 0;
        double refill_qty = 0.0;
        std::int64_t level_event_count = 0;
        std::int64_t ambiguity_count = seed_ambiguous[row] != 0 ? 1 : 0;
        const bool pre_activation_request = request > 0 && request <= activate;
        bool cancel_requested = pre_activation_request;
        bool request_recorded = pre_activation_request;
        if (pre_activation_request) {
            out.request_state_observed[row] = 1;
            out.request_remaining_qty[row] = remaining;
        }

        const bool invalid_book = seed_best_bid_tick[row] <= 0
            || seed_best_ask_tick[row] <= seed_best_bid_tick[row];
        const bool crosses = buy
            ? order_price_tick[row] >= seed_best_ask_tick[row]
            : order_price_tick[row] <= seed_best_bid_tick[row];
        OrderState state = cancel_requested
            ? OrderState::pending_cancel
            : OrderState::open;
        if (ack > 0 && ack <= activate) {
            out.activation_status[row] = static_cast<std::uint8_t>(
                ActivationStatus::cancelled_before_activation
            );
            state = OrderState::cancelled;
            out.terminal_ts_ms[row] = ack;
            out.terminal_reason[row] = static_cast<std::uint8_t>(
                TerminalReason::cancel_ack_before_activation
            );
        } else if (invalid_book || crosses) {
            out.activation_status[row] = static_cast<std::uint8_t>(
                invalid_book ? ActivationStatus::invalid_book
                             : ActivationStatus::gtx_reject
            );
            state = OrderState::rejected;
            out.terminal_ts_ms[row] = activate;
            out.terminal_reason[row] = static_cast<std::uint8_t>(
                invalid_book ? TerminalReason::invalid_book
                             : TerminalReason::gtx_reject
            );
        } else {
            out.activation_status[row] = static_cast<std::uint8_t>(
                ActivationStatus::active
            );
        }
        out.activation_queue_status[row] = seed_status[row];

        auto invalidate_queue = [&](QueueInvalidReason reason) {
            queue_valid = false;
            if (invalid_reason == static_cast<std::uint8_t>(QueueInvalidReason::none)) {
                invalid_reason = static_cast<std::uint8_t>(reason);
            }
        };
        auto record_request = [&]() {
            if (request_recorded || state == OrderState::filled
                || state == OrderState::cancelled || state == OrderState::rejected
                || state == OrderState::censored) {
                return;
            }
            request_recorded = true;
            cancel_requested = true;
            out.request_state_observed[row] = 1;
            out.request_order_state_before[row] = static_cast<std::uint8_t>(state);
            out.request_order_age_ms[row] = std::max(0.0, static_cast<double>(request - activate));
            out.request_remaining_qty[row] = remaining;
            out.request_queue_left[row] = queue_left;
            out.request_queue_path_valid[row] = queue_valid ? 1 : 0;
            out.request_native_cancel_count[row] = cancel_count;
            out.request_native_cancel_qty[row] = cancel_qty;
            out.request_native_refill_count[row] = refill_count;
            out.request_native_refill_qty[row] = refill_qty;
            out.request_native_level_event_count[row] = level_event_count;
            if (state == OrderState::open) {
                state = OrderState::pending_cancel;
            }
        };

        std::size_t event_cursor = event_begin[row];
        const std::size_t event_end = event_begin[row + 1];
        auto trade_cursor = static_cast<std::size_t>(
            std::lower_bound(trade_ts_ms.begin(), trade_ts_ms.end(), activate)
            - trade_ts_ms.begin()
        );
        while (state == OrderState::open || state == OrderState::pending_cancel) {
            const std::int64_t next_event = event_cursor < event_end
                ? event_ts_ms[event_cursor]
                : std::numeric_limits<std::int64_t>::max();
            const std::int64_t next_trade = trade_cursor < trade_count
                ? trade_ts_ms[trade_cursor]
                : std::numeric_limits<std::int64_t>::max();
            const std::int64_t next_request = !request_recorded && request > activate
                ? request
                : std::numeric_limits<std::int64_t>::max();
            const std::int64_t next_ack = ack > activate
                ? ack
                : std::numeric_limits<std::int64_t>::max();
            const std::int64_t now = std::min(
                {next_event, next_trade, next_request, next_ack, stop}
            );
            if (now == next_request) {
                record_request();
            }
            while (event_cursor < event_end && event_ts_ms[event_cursor] == now) {
                if (event_code[event_cursor] == 1
                    || (event_code[event_cursor] == 0
                        && event_state_valid[event_cursor] == 0)) {
                    invalidate_queue(QueueInvalidReason::native_sequence_invalidated);
                } else if (event_code[event_cursor] == 2) {
                    invalidate_queue(QueueInvalidReason::native_snapshot_reset);
                } else {
                    ++level_event_count;
                    if (event_ambiguous[event_cursor] != 0) {
                        ++ambiguity_count;
                        queue_trade_since_update = 0.0;
                        invalidate_queue(
                            QueueInvalidReason::same_ms_native_trade_or_activation
                        );
                    } else if (queue_valid
                               && std::isfinite(event_qty_after[event_cursor])) {
                    const double after = std::max(0.0, event_qty_after[event_cursor]);
                    const double decrease = std::max(0.0, public_qty - after);
                    const double increase = std::max(0.0, after - public_qty);
                    const double explained_trade = std::min(
                        public_qty, std::max(0.0, queue_trade_since_update)
                    );
                    const double cancellation = std::max(0.0, decrease - explained_trade);
                    if (cancellation > 0.0) {
                        const double ahead = std::max(0.0, queue_left);
                        const double public_after_trade = std::max(0.0, public_qty - explained_trade);
                        const double behind = std::max(0.0, public_after_trade - ahead);
                        const double denominator = ahead + behind;
                        const double ahead_probability = denominator > 0.0
                            ? ahead / denominator
                            : 0.0;
                        queue_left = ahead - std::min(
                            ahead, cancellation * ahead_probability
                        );
                        ++cancel_count;
                        cancel_qty += cancellation;
                    }
                    if (increase > 0.0) {
                        ++refill_count;
                        refill_qty += increase;
                    }
                    public_qty = after;
                    queue_trade_since_update = 0.0;
                    }
                }
                ++event_cursor;
            }

            while (trade_cursor < trade_count && trade_ts_ms[trade_cursor] == now) {
                const bool passive_buy = is_buyer_maker[trade_cursor] != 0;
                if ((state == OrderState::open
                     || state == OrderState::pending_cancel)
                    && passive_buy == buy && trade_qty[trade_cursor] > 0.0) {
                    const bool exact = trade_price_tick[trade_cursor] == order_price_tick[row];
                    const bool through = buy
                        ? trade_price_tick[trade_cursor] < order_price_tick[row]
                        : trade_price_tick[trade_cursor] > order_price_tick[row];
                    if (exact || through) {
                        if (out.first_touch_ts_ms[row] == 0) {
                            out.first_touch_ts_ms[row] = now;
                            out.first_touch_type[row] = static_cast<std::uint8_t>(
                                exact ? TouchType::exact : TouchType::through
                            );
                        }
                        if (exact && out.exact_touch_ts_ms[row] == 0) {
                            out.exact_touch_ts_ms[row] = now;
                        }
                        if (through && out.through_touch_ts_ms[row] == 0) {
                            out.through_touch_ts_ms[row] = now;
                        }
                        double available = 0.0;
                        FillMechanism mechanism = FillMechanism::none;
                        if (through) {
                            available = remaining;
                            queue_left = 0.0;
                            mechanism = FillMechanism::strict_through;
                        } else {
                            queue_trade_since_update += trade_qty[trade_cursor];
                            if (queue_valid && std::isfinite(queue_left)) {
                                available = trade_qty[trade_cursor]
                                    * queue_deplete_mult;
                                const double eaten = std::min(
                                    std::max(0.0, queue_left), available
                                );
                                queue_left = std::max(0.0, queue_left - eaten);
                                available -= eaten;
                                mechanism = FillMechanism::exact_queue;
                            }
                        }
                        const double fill = sparse_floor_lot(
                            std::min(std::max(0.0, available), remaining),
                            lot_size
                        );
                        if (fill >= lot_size) {
                            if (out.first_fill_ts_ms[row] == 0) {
                                out.first_fill_ts_ms[row] = now;
                                out.first_fill_mechanism[row] = static_cast<std::uint8_t>(mechanism);
                            }
                            filled += fill;
                            remaining = std::max(0.0, remaining - fill);
                            if (cancel_requested && out.cancel_acked[row] == 0) {
                                if (out.first_pending_cancel_fill_ts_ms[row] == 0) {
                                    out.first_pending_cancel_fill_ts_ms[row] = now;
                                }
                                out.fill_while_cancel_pending_qty[row] += fill;
                            }
                            if (remaining < lot_size) {
                                remaining = 0.0;
                                out.full_fill_ts_ms[row] = now;
                                state = OrderState::filled;
                                out.terminal_ts_ms[row] = now;
                                out.terminal_reason[row] = static_cast<std::uint8_t>(
                                    mechanism == FillMechanism::strict_through
                                        ? TerminalReason::strict_through
                                        : TerminalReason::exact_queue
                                );
                            } else {
                                ++out.partial_fill_count[row];
                            }
                        }
                    }
                }
                ++trade_cursor;
            }
            if ((state == OrderState::open || state == OrderState::pending_cancel)
                && now == next_ack) {
                out.cancel_acked[row] = 1;
                state = OrderState::cancelled;
                out.terminal_ts_ms[row] = now;
                out.terminal_reason[row] = static_cast<std::uint8_t>(
                    TerminalReason::cancel_ack
                );
            }
            if ((state == OrderState::open || state == OrderState::pending_cancel)
                && now == stop) {
                state = OrderState::censored;
                out.terminal_ts_ms[row] = stop;
                out.terminal_reason[row] = static_cast<std::uint8_t>(
                    TerminalReason::administrative_censor
                );
            }
        }

        out.queue_path_valid[row] = (
            out.activation_status[row]
            == static_cast<std::uint8_t>(ActivationStatus::active)
            && queue_valid
        ) ? 1 : 0;
        out.queue_invalid_reason[row] = invalid_reason;
        out.fill_qty[row] = filled;
        out.remaining_qty[row] = remaining;
        out.terminal_state[row] = static_cast<std::uint8_t>(state);
        out.native_cancel_count[row] = cancel_count;
        out.native_cancel_qty[row] = cancel_qty;
        out.native_refill_count[row] = refill_count;
        out.native_refill_qty[row] = refill_qty;
        out.native_level_event_count[row] = level_event_count;
        out.same_ms_ambiguity_count[row] = ambiguity_count;
    }
    return out;
}

}  // namespace narrowgate_cpp
