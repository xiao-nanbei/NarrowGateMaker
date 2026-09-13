#include "request_state_features.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <string>
#include <stdexcept>
#include <utility>

namespace narrowgate_cpp {
namespace {

template <typename T>
void require_size(ArrayView<T> values, std::size_t expected, const char* name) {
    if (values.size() != expected) {
        throw std::invalid_argument(
            std::string(name) + " length does not match its timestamp array"
        );
    }
}

template <typename T>
void require_sorted(ArrayView<T> values, const char* name) {
    if (!std::is_sorted(values.begin(), values.end())) {
        throw std::invalid_argument(std::string(name) + " must be sorted ascending");
    }
}

std::size_t lower_index(ArrayView<std::int64_t> values, std::int64_t target) {
    return static_cast<std::size_t>(
        std::lower_bound(values.begin(), values.end(), target) - values.begin()
    );
}

std::int64_t count_leq(
    const std::vector<std::int64_t>& values,
    std::int64_t target
) {
    return static_cast<std::int64_t>(
        std::upper_bound(values.begin(), values.end(), target) - values.begin()
    );
}

std::int64_t count_lt(
    const std::vector<std::int64_t>& values,
    std::int64_t target
) {
    return static_cast<std::int64_t>(
        std::lower_bound(values.begin(), values.end(), target) - values.begin()
    );
}

double safe_ratio(double numerator, double denominator) {
    return denominator > 1e-12 ? numerator / denominator : 0.0;
}

}  // namespace

RequestStateFeatureResult compute_request_state_features(
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
) {
    const std::size_t rows = request_ts_ms.size();
    require_size(book_cutoff_ts_ms, rows, "book_cutoff_ts_ms");
    require_size(trade_cutoff_ts_ms, rows, "trade_cutoff_ts_ms");
    require_size(activation_ts_ms, rows, "activation_ts_ms");
    require_size(terminal_ts_ms, rows, "terminal_ts_ms");
    require_size(cancel_ack_ts_ms, rows, "cancel_ack_ts_ms");
    require_size(bbo_best_bid, bbo_ts_ms.size(), "bbo_best_bid");
    require_size(bbo_best_ask, bbo_ts_ms.size(), "bbo_best_ask");
    require_size(bbo_bid_qty, bbo_ts_ms.size(), "bbo_bid_qty");
    require_size(bbo_ask_qty, bbo_ts_ms.size(), "bbo_ask_qty");
    require_size(trade_price, trade_ts_ms.size(), "trade_price");
    require_size(trade_qty, trade_ts_ms.size(), "trade_qty");
    require_size(is_buyer_maker, trade_ts_ms.size(), "is_buyer_maker");
    if (l2_bid_px.rows != l2_ts_ms.size() ||
        l2_bid_qty.rows != l2_ts_ms.size() ||
        l2_ask_px.rows != l2_ts_ms.size() ||
        l2_ask_qty.rows != l2_ts_ms.size()) {
        throw std::invalid_argument("L2 matrix row count does not match l2_ts_ms");
    }
    if (l2_bid_px.cols != l2_bid_qty.cols ||
        l2_ask_px.cols != l2_ask_qty.cols ||
        l2_bid_px.cols != l2_ask_px.cols) {
        throw std::invalid_argument("L2 price/quantity matrices must have equal width");
    }
    if (!(tick_size > 0.0) || depth_levels <= 0 || l2_path_lookback_ms <= 0) {
        throw std::invalid_argument("tick_size, depth_levels, and lookback must be positive");
    }
    require_sorted(request_ts_ms, "request_ts_ms");
    require_sorted(bbo_ts_ms, "bbo_ts_ms");
    require_sorted(l2_ts_ms, "l2_ts_ms");
    require_sorted(trade_ts_ms, "trade_ts_ms");
    require_sorted(windows_ms, "windows_ms");
    for (const auto window : windows_ms) {
        if (window <= 0) {
            throw std::invalid_argument("request-state windows must be positive");
        }
    }

    RequestStateFeatureResult out;
    out.rows = rows;
    out.windows = windows_ms.size();
    const double nan = std::numeric_limits<double>::quiet_NaN();
    out.valid_book.assign(rows, 0);
    out.book_source_ts_ms.assign(rows, 0);
    out.book_age_ms.assign(rows, nan);
    out.best_bid.assign(rows, nan);
    out.best_ask.assign(rows, nan);
    out.bid_qty.assign(rows, nan);
    out.ask_qty.assign(rows, nan);
    out.mid.assign(rows, nan);
    out.bbo_spread_ticks.assign(rows, nan);
    out.book_imbalance.assign(rows, nan);
    out.microprice_shift_bps.assign(rows, nan);
    out.l2_near_depth_total.assign(rows, nan);
    out.l2_quote_flip_rate.assign(rows, nan);
    out.l2_book_refresh_ratio.assign(rows, nan);
    out.l2_book_cancel_ratio.assign(rows, nan);
    out.active_order_count.assign(rows, 0);
    out.pending_cancel_before_count.assign(rows, 0);
    out.request_batch_size.assign(rows, 0);
    const std::size_t matrix_size = rows * windows_ms.size();
    out.market_return_bps.assign(matrix_size, nan);
    out.aggressive_buy_qty.assign(matrix_size, 0.0);
    out.aggressive_sell_qty.assign(matrix_size, 0.0);
    out.taker_imbalance.assign(matrix_size, 0.0);
    out.trade_count.assign(matrix_size, 0);
    out.book_update_count.assign(matrix_size, 0);

    std::vector<double> buy_prefix(trade_ts_ms.size() + 1, 0.0);
    std::vector<double> sell_prefix(trade_ts_ms.size() + 1, 0.0);
    for (std::size_t i = 0; i < trade_ts_ms.size(); ++i) {
        buy_prefix[i + 1] = buy_prefix[i];
        sell_prefix[i + 1] = sell_prefix[i];
        const double qty = std::max(0.0, trade_qty[i]);
        if (is_buyer_maker[i] != 0) {
            sell_prefix[i + 1] += qty;
        } else {
            buy_prefix[i + 1] += qty;
        }
    }

    const std::size_t l2_width = l2_bid_px.cols;
    const std::size_t used_depth = std::min<std::size_t>(
        static_cast<std::size_t>(depth_levels), l2_width
    );
    std::vector<double> l2_total(l2_ts_ms.size(), 0.0);
    std::vector<double> refresh_prefix(l2_ts_ms.size() + 1, 0.0);
    std::vector<double> cancel_prefix(l2_ts_ms.size() + 1, 0.0);
    std::vector<double> flip_prefix(l2_ts_ms.size() + 1, 0.0);
    for (std::size_t i = 0; i < l2_ts_ms.size(); ++i) {
        double total = 0.0;
        for (std::size_t level = 0; level < used_depth; ++level) {
            total += std::max(0.0, l2_bid_qty(i, level));
            total += std::max(0.0, l2_ask_qty(i, level));
        }
        l2_total[i] = total;
        refresh_prefix[i + 1] = refresh_prefix[i];
        cancel_prefix[i + 1] = cancel_prefix[i];
        flip_prefix[i + 1] = flip_prefix[i];
        if (i == 0) {
            continue;
        }
        const double previous = l2_total[i - 1];
        if (previous > 1e-12) {
            const double change = total - previous;
            if (change > 0.0) {
                refresh_prefix[i + 1] += change / previous;
            } else if (change < 0.0) {
                cancel_prefix[i + 1] += -change / previous;
            }
        }
        if (l2_bid_px(i, 0) != l2_bid_px(i - 1, 0) ||
            l2_ask_px(i, 0) != l2_ask_px(i - 1, 0)) {
            flip_prefix[i + 1] += 1.0;
        }
    }

    std::vector<std::int64_t> activations;
    std::vector<std::int64_t> terminals;
    std::vector<std::int64_t> requests;
    std::vector<std::int64_t> pending_ends;
    activations.reserve(rows);
    terminals.reserve(rows);
    requests.reserve(rows);
    pending_ends.reserve(rows);
    for (std::size_t i = 0; i < rows; ++i) {
        if (activation_ts_ms[i] > 0) {
            activations.push_back(activation_ts_ms[i]);
        }
        if (terminal_ts_ms[i] > 0) {
            terminals.push_back(terminal_ts_ms[i]);
        }
        if (request_ts_ms[i] > 0) {
            requests.push_back(request_ts_ms[i]);
            std::int64_t end = cancel_ack_ts_ms[i] > 0
                ? cancel_ack_ts_ms[i]
                : terminal_ts_ms[i];
            if (end > 0) {
                pending_ends.push_back(end);
            }
        }
    }
    std::sort(activations.begin(), activations.end());
    std::sort(terminals.begin(), terminals.end());
    std::sort(requests.begin(), requests.end());
    std::sort(pending_ends.begin(), pending_ends.end());

    for (std::size_t row = 0; row < rows; ++row) {
        const std::int64_t request_ts = request_ts_ms[row];
        if (request_ts <= 0) {
            continue;
        }
        const std::int64_t book_cutoff_ts = std::min(
            request_ts, book_cutoff_ts_ms[row]
        );
        const std::int64_t trade_cutoff_ts = std::min(
            request_ts, trade_cutoff_ts_ms[row]
        );
        out.active_order_count[row] = std::max<std::int64_t>(
            0,
            count_leq(activations, request_ts) - count_leq(terminals, request_ts)
        );
        out.pending_cancel_before_count[row] = std::max<std::int64_t>(
            0,
            count_lt(requests, request_ts) - count_leq(pending_ends, request_ts)
        );
        const auto batch_begin = std::lower_bound(
            requests.begin(), requests.end(), request_ts
        );
        const auto batch_end = std::upper_bound(
            batch_begin, requests.end(), request_ts
        );
        out.request_batch_size[row] = static_cast<std::int64_t>(
            batch_end - batch_begin
        );

        // Strictly-before visibility avoids assigning an exchange event with an
        // unresolved same-millisecond ordering to the cancel request.
        const std::size_t bbo_end = lower_index(bbo_ts_ms, book_cutoff_ts);
        const std::size_t l2_end = lower_index(l2_ts_ms, book_cutoff_ts);
        const std::size_t bbo_idx = bbo_end > 0 ? bbo_end - 1 : bbo_ts_ms.size();
        const std::size_t l2_idx = l2_end > 0 ? l2_end - 1 : l2_ts_ms.size();
        if (bbo_idx < bbo_ts_ms.size()) {
            const double bid = bbo_best_bid[bbo_idx];
            const double ask = bbo_best_ask[bbo_idx];
            const double bid_size = std::max(0.0, bbo_bid_qty[bbo_idx]);
            const double ask_size = std::max(0.0, bbo_ask_qty[bbo_idx]);
            if (bid > 0.0 && ask > bid) {
                const double mid = 0.5 * (bid + ask);
                const double size_total = bid_size + ask_size;
                const double microprice = size_total > 1e-12
                    ? (ask * bid_size + bid * ask_size) / size_total
                    : mid;
                out.valid_book[row] = 1;
                out.book_source_ts_ms[row] = bbo_ts_ms[bbo_idx];
                out.book_age_ms[row] = static_cast<double>(request_ts - bbo_ts_ms[bbo_idx]);
                out.best_bid[row] = bid;
                out.best_ask[row] = ask;
                out.bid_qty[row] = bid_size;
                out.ask_qty[row] = ask_size;
                out.mid[row] = mid;
                out.bbo_spread_ticks[row] = (ask - bid) / tick_size;
                out.book_imbalance[row] = safe_ratio(
                    bid_size - ask_size, size_total
                );
                out.microprice_shift_bps[row] =
                    (microprice - mid) / mid * 10'000.0;
            }
        }
        if (l2_idx < l2_ts_ms.size()) {
            double near_depth = 0.0;
            for (std::size_t level = 0; level < used_depth; ++level) {
                near_depth += std::max(0.0, l2_bid_qty(l2_idx, level));
                near_depth += std::max(0.0, l2_ask_qty(l2_idx, level));
            }
            out.l2_near_depth_total[row] = near_depth;
            const std::size_t path_begin_unbounded = lower_index(
                l2_ts_ms, request_ts - l2_path_lookback_ms
            );
            const std::size_t path_begin = std::min(
                path_begin_unbounded, l2_end
            );
            const std::size_t sample_count = l2_end > path_begin
                ? l2_end - path_begin
                : 0;
            if (sample_count > 0) {
                out.l2_book_refresh_ratio[row] =
                    (refresh_prefix[l2_end] - refresh_prefix[path_begin]) /
                    static_cast<double>(sample_count);
                out.l2_book_cancel_ratio[row] =
                    (cancel_prefix[l2_end] - cancel_prefix[path_begin]) /
                    static_cast<double>(sample_count);
                out.l2_quote_flip_rate[row] =
                    (flip_prefix[l2_end] - flip_prefix[path_begin]) /
                    static_cast<double>(sample_count);
            }
        }

        const std::size_t trade_end = lower_index(
            trade_ts_ms, trade_cutoff_ts
        );
        for (std::size_t window_index = 0; window_index < windows_ms.size(); ++window_index) {
            const std::int64_t window = windows_ms[window_index];
            const std::size_t trade_begin_unbounded = lower_index(
                trade_ts_ms, request_ts - window
            );
            const std::size_t trade_begin = std::min(
                trade_begin_unbounded, trade_end
            );
            const std::size_t offset = row * windows_ms.size() + window_index;
            const double buy_qty = buy_prefix[trade_end] - buy_prefix[trade_begin];
            const double sell_qty = sell_prefix[trade_end] - sell_prefix[trade_begin];
            out.aggressive_buy_qty[offset] = buy_qty;
            out.aggressive_sell_qty[offset] = sell_qty;
            out.taker_imbalance[offset] = safe_ratio(
                buy_qty - sell_qty, buy_qty + sell_qty
            );
            out.trade_count[offset] = static_cast<std::int64_t>(
                trade_end - trade_begin
            );
            const std::size_t book_begin_unbounded = lower_index(
                l2_ts_ms, request_ts - window
            );
            const std::size_t book_begin = std::min(
                book_begin_unbounded, l2_end
            );
            out.book_update_count[offset] = static_cast<std::int64_t>(
                l2_end - book_begin
            );
            if (trade_end > 0) {
                const std::size_t current_trade = trade_end - 1;
                const std::size_t reference_trade = trade_begin > 0
                    ? trade_begin - 1
                    : (trade_begin < trade_end ? trade_begin : current_trade);
                const double current_price = trade_price[current_trade];
                const double reference_price = trade_price[reference_trade];
                if (current_price > 0.0 && reference_price > 0.0) {
                    out.market_return_bps[offset] =
                        std::log(current_price / reference_price) * 10'000.0;
                }
            }
        }
    }
    return out;
}

}  // namespace narrowgate_cpp
