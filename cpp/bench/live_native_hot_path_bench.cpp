#include "narrowgate_cpp/live_order_action_plan.hpp"
#include "narrowgate_cpp/live_runtime_core.hpp"
#include "narrowgate_cpp/order_gateway_core.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string_view>

#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>
#endif

#ifndef NARROWGATE_LIVE_CPU_PROFILE_NAME
#define NARROWGATE_LIVE_CPU_PROFILE_NAME "unspecified"
#endif

using namespace narrowgate_cpp;

namespace {

constexpr std::uint64_t kDefaultIterations = 1'000'000ULL;
constexpr std::uint64_t kMaximumWarmupIterations = 50'000ULL;

template <typename Value>
inline void do_not_optimize(const Value& value) noexcept {
#if defined(__GNUC__) || defined(__clang__)
    asm volatile("" : : "g"(&value) : "memory");
#else
    (void)value;
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

inline void clobber_memory() noexcept {
#if defined(__GNUC__) || defined(__clang__)
    asm volatile("" : : : "memory");
#else
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
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

struct Measurement {
    std::string_view name;
    std::uint64_t iterations = 0;
    std::uint64_t elapsed_ns = 0;
    std::uint64_t cycles = 0;
    std::uint64_t checksum = 0;
};

template <typename Operation>
[[nodiscard]] Measurement measure(
    std::string_view name,
    std::uint64_t iterations,
    Operation&& operation
) {
    std::uint64_t checksum = 0;
    clobber_memory();
#if defined(__x86_64__) || defined(_M_X64)
    const auto cycles_begin = cycle_clock_begin();
#endif
    const auto begin = std::chrono::steady_clock::now();
    for (std::uint64_t index = 0; index < iterations; ++index) {
        checksum += operation(index);
        do_not_optimize(checksum);
    }
    const auto end = std::chrono::steady_clock::now();
#if defined(__x86_64__) || defined(_M_X64)
    const auto cycles_end = cycle_clock_end();
#endif
    clobber_memory();

    Measurement result{
        .name = name,
        .iterations = iterations,
        .elapsed_ns = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
                .count()
        ),
        .checksum = checksum,
    };
#if defined(__x86_64__) || defined(_M_X64)
    result.cycles = cycles_end - cycles_begin;
#endif
    return result;
}

void print_measurement(const Measurement& value) {
    const double iterations = static_cast<double>(value.iterations);
    const double ns_per_op = static_cast<double>(value.elapsed_ns) / iterations;
    const double operations_per_second = ns_per_op > 0.0
        ? 1'000'000'000.0 / ns_per_op
        : std::numeric_limits<double>::infinity();

    std::cout << value.name << ',' << value.iterations << ','
              << ns_per_op << ',';
#if defined(__x86_64__) || defined(_M_X64)
    std::cout << static_cast<double>(value.cycles) / iterations;
#else
    std::cout << "na";
#endif
    std::cout << ',' << operations_per_second << ',' << value.checksum << '\n';
}

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

QuoteCoreConfig quote_config() {
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
    value.use_depth_microprice = true;
    value.use_depth_kappa = true;
    value.book_imb_strength = 0.02;
    value.max_spread_bps = 20.0;
    return value;
}

NativeLiveDecisionInput decision_input() {
    NativeLiveDecisionInput value;
    value.decision_ts_ns = 1'100'000'000;
    value.expected_market_publication_sequence = 2;
    value.expected_bid_generation = 1;
    value.expected_ask_generation = 1;
    value.max_book_age_ns = std::numeric_limits<std::uint64_t>::max();
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
    value.buy_policy.max_book_age_s = 1.0;
    value.sell_policy.max_book_age_s = 1.0;
    return value;
}

LiveOrderPlannerContext planner_context() {
    return LiveOrderPlannerContext{
        .inventory_lots = 0,
        .max_inventory_lots = 26,
        .max_position_value_quote_atoms = 120'000'000'000LL,
        .mid_notional_quote_atoms_per_lot = 6'340'000'000LL,
        .quote_atoms_per_price_tick_lot = 10'000,
        .min_quantity_lots = 1,
        .min_notional_quote_atoms = 500'000'000,
        .requote_threshold_bps = 0.1,
    };
}

LiveOrderReplaceConfig replace_config() {
    LiveOrderReplaceConfig value{};
    value.tick_size = 0.1;
    value.lot_size = 0.001;
    value.min_notional = 5.0;
    value.add_min_price_change_ticks = 2.0;
    value.reducing_min_price_change_ticks = 1.0;
    value.add_min_interval_ms = 100.0;
    value.reducing_min_interval_ms = 50.0;
    value.flags = LiveOrderReplacePendingCoalesce |
        LiveOrderReplaceCancelFirstExposureIncrease;
    return value;
}

LiveSideOrderActionInput planner_side(
    std::int64_t price_ticks,
    LivePlannerOrderState state
) {
    return LiveSideOrderActionInput{
        .target_price_ticks = price_ticks,
        .desired_quantity_lots = 1,
        .exposure_probe_quantity_lots = 1,
        .existing_price_ticks = state == LivePlannerOrderState::Empty
            ? 0
            : price_ticks,
        .existing_remaining_lots = state == LivePlannerOrderState::Empty
            ? 0
            : 1,
        .order_age_ms = 150.0,
        .order_ttl_ms = 0.0,
        .order_state = state,
        .flags = LiveOrderSideInputRouteAllowed |
            LiveOrderSideInputAllowPost |
            LiveOrderSideInputAllowExposureIncrease,
    };
}

CanonicalOrderIntent gateway_intent() {
    CanonicalOrderIntent intent;
    intent.request_id = "bench-place";
    intent.decision_id = "bench-decision";
    intent.client_order_id = "bench-client";
    intent.symbol = "BTCUSDC";
    intent.side = CanonicalSide::Buy;
    intent.order_type = CanonicalOrderType::Limit;
    intent.time_in_force = CanonicalTimeInForce::Gtx;
    intent.price = 63'400.0;
    intent.quantity = 0.001;
    intent.post_only = true;
    intent.expected_ownership_generation = 7;
    return intent;
}

void warm_up(std::uint64_t iterations) {
    std::uint64_t value = 0;
    for (std::uint64_t index = 0; index < iterations; ++index) {
        value ^= index * 0x9e3779b97f4a7c15ULL;
        do_not_optimize(value);
    }
}

}  // namespace

int main(int argc, char** argv) {
    const std::uint64_t iterations = argc > 1
        ? std::strtoull(argv[1], nullptr, 10)
        : kDefaultIterations;
    if (iterations == 0) {
        std::cerr << "iterations must be positive\n";
        return 2;
    }
    const auto warmup_iterations = std::min(
        kMaximumWarmupIterations,
        std::max<std::uint64_t>(1, iterations / 20)
    );
    warm_up(warmup_iterations);

    auto context = planner_context();
    const auto replace = replace_config();
    auto buy = planner_side(634'000, LivePlannerOrderState::Active);
    auto sell = planner_side(634'001, LivePlannerOrderState::Active);
    const auto action_plan = measure(
        "compute_live_order_action_plan",
        iterations,
        [&](std::uint64_t index) {
            context.inventory_lots = static_cast<std::int64_t>(index % 17) - 8;
            buy.target_price_ticks = 634'000 +
                static_cast<std::int64_t>(index % 5);
            buy.existing_price_ticks = 634'000;
            buy.order_age_ms = 50.0 + static_cast<double>(index % 5) * 40.0;
            sell.target_price_ticks = 634'001 -
                static_cast<std::int64_t>(index % 5);
            sell.existing_price_ticks = 634'001;
            sell.order_age_ms = buy.order_age_ms;
            const auto result = compute_live_order_action_plan(
                context,
                replace,
                buy,
                sell
            );
            return static_cast<std::uint64_t>(result.buy.reason_mask) +
                static_cast<std::uint64_t>(result.sell.reason_mask) +
                static_cast<std::uint64_t>(result.buy.action) * 17ULL +
                static_cast<std::uint64_t>(result.sell.action) * 31ULL +
                static_cast<std::uint64_t>(result.buy.target_quantity_lots + 1) +
                static_cast<std::uint64_t>(result.sell.target_quantity_lots + 1);
        }
    );

    NativeLiveRuntimeCore runtime(quote_config());
    const auto bids = side_update(Side::Buy, 634'000, 1'000'000'000);
    const auto asks = side_update(Side::Sell, 634'001, 1'000'000'000);
    if (runtime.publish_book(bids, asks) != MarketStateUpdateStatus::Applied) {
        std::cerr << "failed to publish benchmark book\n";
        return 3;
    }
    auto decision = decision_input();
    const auto runtime_decide = measure(
        "NativeLiveRuntimeCore::decide",
        iterations,
        [&](std::uint64_t index) {
            ++decision.decision_ts_ns;
            decision.quote_state.inventory = static_cast<double>(
                static_cast<std::int64_t>(index % 17) - 8
            ) * 0.001;
            decision.quote_state.sigma_sq = 0.5 +
                static_cast<double>(index % 31) * 0.125;
            decision.quote_state.trade_intensity = 20.0 +
                static_cast<double>(index % 257);
            const auto result = runtime.decide(decision);
            if (result.status != NativeLiveDecisionStatus::Applied) {
                throw std::runtime_error("native runtime decision was rejected");
            }
            return static_cast<std::uint64_t>(
                std::llround(result.routing.bid_price * 10.0)
            ) + static_cast<std::uint64_t>(
                std::llround(result.routing.ask_price * 10.0)
            ) + result.decision_sequence +
                static_cast<std::uint64_t>(result.routing.can_bid) * 7ULL +
                static_cast<std::uint64_t>(result.routing.can_ask) * 11ULL;
        }
    );

    NativeUsdMOrderGatewayCore empty_gateway(
        TransportBackendKind::CppUsdmRest
    );
    const auto gateway_empty = measure(
        "NativeUsdMOrderGatewayCore::empty_poll",
        iterations,
        [&](std::uint64_t index) {
            const auto result = empty_gateway.begin_next(index + 1, 1, 7);
            do_not_optimize(result);
            return 1ULL +
                static_cast<std::uint64_t>(result.request.has_value()) +
                static_cast<std::uint64_t>(result.invalidations.size()) +
                static_cast<std::uint64_t>(result.invalidations.capacity());
        }
    );

    SpscRing<NativeGatewayWireRequest, kNativeGatewayQueueCapacity> ring;
    NativeGatewayWireRequest wire_request;
    wire_request.operation = NativeGatewayOperation::Place;
    wire_request.side = CanonicalSide::Buy;
    wire_request.price = 63'400.0;
    wire_request.quantity = 0.001;
    NativeGatewayWireRequest wire_result;
    const auto gateway_ring = measure(
        "gateway_SpscRing::push_pop_pair",
        iterations,
        [&](std::uint64_t index) {
            wire_request.enqueue_time_ns = index + 1;
            if (!ring.try_push(wire_request) || !ring.try_pop(wire_result)) {
                throw std::runtime_error("gateway ring push/pop failed");
            }
            return wire_result.enqueue_time_ns +
                static_cast<std::uint64_t>(wire_result.operation);
        }
    );

    NativeUsdMOrderGatewayCore lifecycle_gateway(
        TransportBackendKind::CppUsdmRest
    );
    const auto intent = gateway_intent();
    const auto gateway_lifecycle = measure(
        "gateway_enqueue_begin_complete",
        iterations,
        [&](std::uint64_t index) {
            const std::uint64_t decision_time = index * 4 + 1;
            const std::uint64_t enqueue_time = decision_time + 1;
            const std::uint64_t dequeue_time = enqueue_time + 1;
            const std::uint64_t completion_time = dequeue_time + 1;
            const auto enqueued = lifecycle_gateway.enqueue_order(
                intent,
                decision_time,
                enqueue_time
            );
            const auto dequeued = lifecycle_gateway.begin_next(
                dequeue_time,
                index + 1,
                7
            );
            if (!enqueued.admitted || !dequeued.request.has_value()) {
                throw std::runtime_error("gateway lifecycle was not admitted");
            }
            const auto completed = lifecycle_gateway.mark_confirmed_not_dispatched(
                completion_time,
                "benchmark_complete"
            );
            return enqueued.enqueue_time_ns + dequeued.request->dequeue_time_ns +
                completed.completion_time_ns + completed.generation;
        }
    );

    std::cout << "cpu_profile=" << NARROWGATE_LIVE_CPU_PROFILE_NAME
              << " warmup_iterations=" << warmup_iterations
              << " cache_line_bytes=" << kNativeGatewayCacheLineBytes
              << " planner_context_bytes=" << sizeof(LiveOrderPlannerContext)
              << " dual_action_plan_bytes=" << sizeof(LiveDualOrderActionPlan)
              << " gateway_wire_request_bytes="
              << sizeof(NativeGatewayWireRequest)
              << " runtime_core_bytes="
              << NativeLiveRuntimeCore::core_size_bytes() << '\n';
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "benchmark,iterations,ns_per_op,tsc_cycles_per_op,ops_per_second,checksum\n";
    print_measurement(action_plan);
    print_measurement(runtime_decide);
    print_measurement(gateway_empty);
    print_measurement(gateway_ring);
    print_measurement(gateway_lifecycle);
    return 0;
}
