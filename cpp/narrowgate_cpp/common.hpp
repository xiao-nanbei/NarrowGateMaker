#pragma once

#include <algorithm>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <tuple>
#include <vector>

namespace narrowgate_cpp {

enum class Side : std::uint8_t {
    Buy = 0,
    Sell = 1,
};

template <typename T>
concept Arithmetic = std::is_arithmetic_v<T>;

template <Arithmetic T>
using ArrayView = std::span<const T>;

// 这些 View 都不拥有底层内存；Python/pybind 侧必须保证 numpy buffer
// 在 C++ 调用返回前仍然存活。不要把 View 存进跨调用的对象里。
template <Arithmetic T>
struct MatrixView {
    const T* data = nullptr;
    std::size_t rows = 0;
    std::size_t cols = 0;

    constexpr bool empty() const noexcept { return rows == 0 || cols == 0; }

    const T& operator()(std::size_t row, std::size_t col) const {
        if (row >= rows || col >= cols) {
            throw std::out_of_range("MatrixView index out of range");
        }
        return data[row * cols + col];
    }

    [[nodiscard]] ArrayView<T> row(std::size_t row_index) const {
        if (row_index >= rows) {
            throw std::out_of_range("MatrixView row out of range");
        }
        return ArrayView<T>{data + row_index * cols, cols};
    }
};

template <typename T>
constexpr T clamp(T value, T lo, T hi) {
    return value < lo ? lo : (value > hi ? hi : value);
}

constexpr const char* side_name(Side side) {
    return side == Side::Buy ? "BUY" : "SELL";
}

template <Side S>
constexpr bool is_buy_v = S == Side::Buy;

template <Side S>
constexpr double inventory_delta_sign() {
    return is_buy_v<S> ? 1.0 : -1.0;
}

template <Side S>
constexpr double adverse_signal_sign() {
    return is_buy_v<S> ? -1.0 : 1.0;
}

template <Side S>
constexpr double defense_signal_sign() {
    return is_buy_v<S> ? 1.0 : -1.0;
}

struct DepthLevel {
    double price = 0.0;
    double qty = 0.0;
};

struct DepthSideView {
    std::span<const DepthLevel> levels;
    ArrayView<double> prices;
    ArrayView<double> quantities;

    [[nodiscard]] std::size_t size() const noexcept {
        return !levels.empty() ? levels.size() : std::min(prices.size(), quantities.size());
    }

    [[nodiscard]] double price(std::size_t index) const noexcept {
        return !levels.empty() ? levels[index].price : prices[index];
    }

    [[nodiscard]] double quantity(std::size_t index) const noexcept {
        return !levels.empty() ? levels[index].qty : quantities[index];
    }

    [[nodiscard]] bool valid(std::size_t index) const noexcept {
        return price(index) > 0.0 && quantity(index) > 0.0;
    }

    [[nodiscard]] double best_price() const noexcept {
        for (std::size_t i = 0; i < size(); ++i) {
            if (valid(i)) {
                return price(i);
            }
        }
        return 0.0;
    }
};

struct DepthView {
    DepthSideView bids;
    DepthSideView asks;

    [[nodiscard]] bool has_book() const noexcept {
        const double bid = best_bid();
        const double ask = best_ask();
        return bid > 0.0 && ask > bid;
    }

    [[nodiscard]] double best_bid() const noexcept { return bids.best_price(); }
    [[nodiscard]] double best_ask() const noexcept { return asks.best_price(); }
};

struct DepthSnapshot {
    std::vector<DepthLevel> bids;
    std::vector<DepthLevel> asks;

    [[nodiscard]] bool has_book() const {
        return !bids.empty() && !asks.empty() && bids.front().price > 0.0 &&
               asks.front().price > bids.front().price;
    }

    [[nodiscard]] double best_bid() const {
        return bids.empty() ? 0.0 : bids.front().price;
    }

    [[nodiscard]] double best_ask() const {
        return asks.empty() ? 0.0 : asks.front().price;
    }

    [[nodiscard]] DepthView view() const noexcept {
        return DepthView{
            DepthSideView{std::span<const DepthLevel>{bids}, {}, {}},
            DepthSideView{std::span<const DepthLevel>{asks}, {}, {}},
        };
    }
};

struct QuotePrediction {
    double dir_10s = 0.5;
    double vol_10s = 0.0;
    double ret_10s = 0.0;
    double tox_bid = 0.5;
    double tox_ask = 0.5;
};

struct QuoteState {
    double mid = 0.0;
    double inventory = 0.0;
    double sigma_sq = 0.0;
    double trade_intensity = 0.0;
    double best_bid = 0.0;
    double best_ask = 0.0;
    bool ber_active = false;
    double mo_ema_all = 0.0;
    double mo_ema_bid = 0.0;
    double mo_ema_ask = 0.0;
    bool bid_adverse_markout_pause_latch = false;
    bool ask_adverse_markout_pause_latch = false;
    double mo_ref = 50.0;
    bool position_open = false;
    double hold_time_s = 0.0;
    double unrealized_pnl = 0.0;
};

inline double floor_tick(double price, double tick) {
    const double t = std::max(std::abs(tick), 1e-12);
    double units = price / t;
    const double nearest = std::round(units);
    if (std::abs(units - nearest) <= 1e-9) {
        units = nearest;
    }
    return std::floor(units) * t;
}

inline double ceil_tick(double price, double tick) {
    const double t = std::max(std::abs(tick), 1e-12);
    double units = price / t;
    const double nearest = std::round(units);
    if (std::abs(units - nearest) <= 1e-9) {
        units = nearest;
    }
    return std::ceil(units) * t;
}

inline double price_tick_alignment_tolerance(double price, double tick) {
    const double t = std::max(std::abs(tick), 1e-12);
    return std::max(
        t * 1e-9,
        64.0 * std::numeric_limits<double>::epsilon() *
            std::max({1.0, std::abs(price), t})
    );
}

inline std::int64_t price_to_tick(double price, double tick) {
    const double t = std::abs(tick);
    if (!std::isfinite(price) || price <= 0.0 ||
        !std::isfinite(t) || t <= 0.0) {
        throw std::invalid_argument("price and tick must be finite and positive");
    }
    const double units = price / t;
    if (units < static_cast<double>(std::numeric_limits<std::int64_t>::min()) ||
        units > static_cast<double>(std::numeric_limits<std::int64_t>::max())) {
        throw std::overflow_error("price tick index exceeds int64 range");
    }
    const auto price_tick = static_cast<std::int64_t>(std::llround(units));
    const double reconstructed = static_cast<double>(price_tick) * t;
    if (std::abs(price - reconstructed) >
        price_tick_alignment_tolerance(price, t)) {
        throw std::invalid_argument("executable price is not aligned to tick size");
    }
    return price_tick;
}

inline std::int64_t price_to_tick_unchecked(double price, double tick) noexcept {
    return static_cast<std::int64_t>(std::llround(price / tick));
}

inline bool same_price_tick(double lhs, double rhs, double tick) {
    return price_to_tick(lhs, tick) == price_to_tick(rhs, tick);
}

inline double safe_div(double numerator, double denominator, double fallback = 0.0) {
    if (std::abs(denominator) <= 1e-12 || !std::isfinite(denominator)) {
        return fallback;
    }
    const double out = numerator / denominator;
    return std::isfinite(out) ? out : fallback;
}

inline double floor_lot(double qty, double lot_size) {
    const double lot = std::max(std::abs(lot_size), 1e-12);
    return std::floor(qty / lot) * lot;
}

template <Side S>
[[nodiscard]] double side_distance_to_mid(double mid, double price, double tick) {
    if constexpr (is_buy_v<S>) {
        return std::max(mid - price, tick);
    } else {
        return std::max(price - mid, tick);
    }
}

template <Side S>
[[nodiscard]] double side_cash_delta(double price, double qty, double maker_fee) {
    if constexpr (is_buy_v<S>) {
        return -price * qty * (1.0 + maker_fee);
    } else {
        return price * qty * (1.0 - maker_fee);
    }
}

}  // namespace narrowgate_cpp
