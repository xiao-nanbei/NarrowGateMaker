#include "narrowgate_cpp/live_runtime_core.hpp"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>

#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>
#endif

using namespace narrowgate_cpp;

namespace {

Depth20SideUpdate side_update(
    Side side,
    std::int64_t best_ticks,
    std::uint64_t timestamp_ns
) {
    Depth20SideUpdate update;
    update.size = kLiveDepthLevels;
    update.clock = MarketClockIdentity{
        timestamp_ns - 3,
        timestamp_ns - 2,
        timestamp_ns - 1,
        timestamp_ns,
        1,
    };
    for (std::size_t index = 0; index < kLiveDepthLevels; ++index) {
        const auto offset = static_cast<std::int64_t>(index);
        update.price_ticks[index] = side == Side::Buy
            ? best_ticks - offset
            : best_ticks + offset;
        update.quantity_lots[index] = 100 + offset;
    }
    return update;
}

QuoteCoreConfig config() {
    QuoteCoreConfig value;
    value.gamma = 0.046;
    value.eta_inventory = 0.046;
    value.a_spread = 0.046;
    value.kappa = 0.05;
    value.tick_size = 0.1;
    value.lot_size = 0.001;
    value.order_size = 0.001;
    value.max_inventory = 0.026;
    value.ml_enabled = false;
    value.use_bar_pricing = false;
    value.use_depth_microprice = true;
    value.use_depth_kappa = true;
    value.book_imb_strength = 0.02;
    return value;
}

NativeLiveDecisionInput input() {
    NativeLiveDecisionInput value;
    value.decision_ts_ns = 1'100'000'000;
    value.expected_market_publication_sequence = 2;
    value.expected_bid_generation = 1;
    value.expected_ask_generation = 1;
    value.quote_state.mid = 63'400.05;
    value.quote_state.inventory = 0.001;
    value.quote_state.sigma_sq = 2.0;
    value.quote_state.trade_intensity = 200.0;
    value.quote_state.mo_ref = 50.0;
    value.min_qty = 0.001;
    value.min_notional = 5.0;
    value.requote_threshold_bps = 0.1;
    value.buy_policy.exposure_increasing = true;
    value.sell_policy.exposure_increasing = false;
    return value;
}

#if defined(__x86_64__) || defined(_M_X64)
[[nodiscard]] std::uint64_t cycle_clock_begin() noexcept {
    _mm_lfence();
    return __rdtsc();
}

[[nodiscard]] std::uint64_t cycle_clock_end() noexcept {
    unsigned int auxiliary = 0;
    const auto value = __rdtscp(&auxiliary);
    _mm_lfence();
    return value;
}
#endif

}  // namespace

int main(int argc, char** argv) {
    const std::uint64_t iterations = argc > 1
        ? std::strtoull(argv[1], nullptr, 10)
        : 1'000'000ULL;
    NativeLiveRuntimeCore core(config());
    const auto bids = side_update(Side::Buy, 634'000, 1'000'000'000);
    const auto asks = side_update(Side::Sell, 634'001, 1'000'000'000);
    if (core.publish_book(bids, asks) != MarketStateUpdateStatus::Applied) {
        return 2;
    }
    auto decision = input();
    double checksum = 0.0;
    constexpr std::uint64_t warmup_iterations = 50'000;
    for (std::uint64_t index = 0; index < warmup_iterations; ++index) {
        decision.decision_ts_ns += 1;
        decision.quote_state.inventory = static_cast<double>(
            static_cast<std::int64_t>(index % 17U) - 8
        ) * 0.001;
        decision.quote_state.sigma_sq = 0.5 +
            static_cast<double>(index % 31U) * 0.125;
        decision.quote_state.trade_intensity = 20.0 +
            static_cast<double>(index % 257U);
        const auto result = core.decide(decision);
        if (result.status != NativeLiveDecisionStatus::Applied) {
            return 3;
        }
        checksum += result.routing.bid_price + result.routing.ask_price;
    }

#if defined(__x86_64__) || defined(_M_X64)
    const auto cycles_begin = cycle_clock_begin();
#endif
    const auto begin = std::chrono::steady_clock::now();
    for (std::uint64_t index = 0; index < iterations; ++index) {
        decision.decision_ts_ns += 1;
        decision.quote_state.inventory = static_cast<double>(
            static_cast<std::int64_t>(index % 17U) - 8
        ) * 0.001;
        decision.quote_state.sigma_sq = 0.5 +
            static_cast<double>(index % 31U) * 0.125;
        decision.quote_state.trade_intensity = 20.0 +
            static_cast<double>(index % 257U);
        const auto result = core.decide(decision);
        if (result.status != NativeLiveDecisionStatus::Applied) {
            return 3;
        }
        checksum += result.routing.bid_price + result.routing.ask_price +
            result.quote.reservation_price +
            result.buy_policy.spread_mult + result.sell_policy.size_mult +
            static_cast<double>(result.routing.can_bid) +
            static_cast<double>(result.routing.can_ask) +
            static_cast<double>(result.routing.bid_needs_update) +
            static_cast<double>(result.routing.ask_needs_update);
    }
    const auto end = std::chrono::steady_clock::now();
#if defined(__x86_64__) || defined(_M_X64)
    const auto cycles_end = cycle_clock_end();
#endif

    const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        end - begin
    ).count();
    const double ns_per_decision = static_cast<double>(elapsed_ns) /
        static_cast<double>(iterations);
    std::cout << std::fixed << std::setprecision(3)
              << "iterations=" << iterations
              << " ns_per_decision=" << ns_per_decision;
#if defined(__x86_64__) || defined(_M_X64)
    std::cout << " cycles_per_decision="
              << static_cast<double>(cycles_end - cycles_begin) /
                    static_cast<double>(iterations);
#endif
    std::cout << " checksum=" << checksum
              << " core_bytes=" << NativeLiveRuntimeCore::core_size_bytes()
              << " cache_line=" << NativeLiveRuntimeCore::cache_line_bytes()
              << '\n';
    return 0;
}
