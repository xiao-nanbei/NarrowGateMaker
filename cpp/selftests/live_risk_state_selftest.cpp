#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>

#include "narrowgate_cpp/live_risk_state.hpp"

namespace {

using narrowgate_cpp::LiveCapacityReasonInventory;
using narrowgate_cpp::LiveCapacityReasonPositionValue;
using narrowgate_cpp::LiveHardRiskReason;
using narrowgate_cpp::LiveRiskConfig;
using narrowgate_cpp::LiveRiskFault;
using narrowgate_cpp::LiveRiskFillKind;
using narrowgate_cpp::LiveRiskPositionState;
using narrowgate_cpp::LiveRiskState;
using narrowgate_cpp::LiveRiskUpdateStatus;
using narrowgate_cpp::Side;
using narrowgate_cpp::kDestructiveInterferenceBytes;

constexpr std::int64_t kDayMs = 86'400'000;

LiveRiskConfig config(
    std::int64_t max_inventory_lots = 10,
    std::int64_t max_position_value = 8'000,
    std::int64_t max_daily_loss = 10'000,
    std::int64_t emergency_drawdown = 10'000
) {
    return LiveRiskConfig{
        max_inventory_lots,
        max_position_value,
        max_daily_loss,
        emergency_drawdown,
        10,
    };
}

void test_layout_and_invalid_config_fail_closed() {
    static_assert(
        alignof(LiveRiskState) == kDestructiveInterferenceBytes
    );
    static_assert(
        sizeof(LiveRiskState) % kDestructiveInterferenceBytes == 0
    );

    LiveRiskState invalid(LiveRiskConfig{}, 0);
    assert(invalid.faulted());
    assert(invalid.snapshot().fault == LiveRiskFault::InvalidConfig);
    assert(
        invalid.evaluate_hard_risk(100) ==
        LiveHardRiskReason::InvalidState
    );
    const auto cap = invalid.cap_exposure_order<Side::Buy>(1, 100);
    (void)cap;
    assert(!cap.valid());
    assert(cap.allowed_lots == 0);
}

void test_fixed_point_open_add_reduce_flip_and_close() {
    LiveRiskState risk(config(), 0);
    auto fill = risk.apply_fill<Side::Buy>(2, 100, 20, 1);
    assert(fill.status == LiveRiskUpdateStatus::Applied);
    assert(fill.kind == LiveRiskFillKind::Open);
    assert(fill.opening_lots == 2);
    auto snap = risk.snapshot();
    assert(snap.position_lots == 2);
    assert(snap.open_cost_basis_quote_atoms == 2'000);
    assert(snap.open_commission_quote_atoms == 20);
    assert(snap.total_marked_pnl_quote_atoms == -20);

    assert(risk.update_mark(105, 2) == LiveRiskUpdateStatus::Applied);
    snap = risk.snapshot();
    assert(snap.unrealized_pnl_quote_atoms == 100);
    assert(snap.total_marked_pnl_quote_atoms == 80);
    assert(snap.session_high_water_quote_atoms == 80);

    fill = risk.apply_fill<Side::Buy>(2, 110, 20, 3);
    assert(fill.kind == LiveRiskFillKind::Add);
    snap = risk.snapshot();
    assert(snap.position_lots == 4);
    assert(snap.open_cost_basis_quote_atoms == 4'200);
    assert(snap.open_commission_quote_atoms == 40);
    assert(snap.unrealized_pnl_quote_atoms == 0);
    assert(snap.total_marked_pnl_quote_atoms == -40);

    fill = risk.apply_fill<Side::Sell>(2, 120, 20, 4);
    assert(fill.kind == LiveRiskFillKind::Reduce);
    assert(fill.closing_lots == 2);
    assert(fill.realized_delta_quote_atoms == 260);
    snap = risk.snapshot();
    assert(snap.position_lots == 2);
    assert(snap.open_cost_basis_quote_atoms == 2'100);
    assert(snap.open_commission_quote_atoms == 20);
    assert(snap.realized_pnl_quote_atoms == 260);
    assert(snap.round_trip_realized_pnl_quote_atoms == 260);
    assert(snap.total_marked_pnl_quote_atoms == 240);

    fill = risk.apply_fill<Side::Sell>(3, 90, 30, 5);
    assert(fill.kind == LiveRiskFillKind::Flip);
    assert(fill.round_trip_closed);
    assert(fill.closing_lots == 2);
    assert(fill.opening_lots == 1);
    assert(fill.realized_delta_quote_atoms == -340);
    snap = risk.snapshot();
    assert(snap.position_lots == -1);
    assert(snap.open_cost_basis_quote_atoms == 900);
    assert(snap.open_commission_quote_atoms == 10);
    assert(snap.realized_pnl_quote_atoms == -80);
    assert(snap.unrealized_pnl_quote_atoms == -150);
    assert(snap.total_marked_pnl_quote_atoms == -240);
    assert(snap.consecutive_losses == 1);

    assert(risk.update_mark(80, 6) == LiveRiskUpdateStatus::Applied);
    snap = risk.snapshot();
    assert(snap.unrealized_pnl_quote_atoms == 100);
    assert(snap.total_marked_pnl_quote_atoms == 10);

    fill = risk.apply_fill<Side::Buy>(1, 80, 10, 7);
    assert(fill.kind == LiveRiskFillKind::Close);
    assert(fill.round_trip_closed);
    assert(fill.realized_delta_quote_atoms == 80);
    snap = risk.snapshot();
    assert(snap.position_lots == 0);
    assert(snap.position_state == LiveRiskPositionState::Flat);
    assert(snap.realized_pnl_quote_atoms == 0);
    assert(snap.total_marked_pnl_quote_atoms == 0);
    assert(snap.consecutive_losses == 0);
}

void test_timeout_state_survives_partial_close_and_flip() {
    LiveRiskState risk(config(), 0);
    assert(
        risk.apply_fill<Side::Buy>(2, 100, 0, 1).status ==
        LiveRiskUpdateStatus::Applied
    );
    assert(
        risk.set_timeout_closing() == LiveRiskUpdateStatus::Applied
    );
    assert(
        risk.apply_fill<Side::Sell>(1, 100, 0, 2).kind ==
        LiveRiskFillKind::Reduce
    );
    assert(
        risk.snapshot().position_state ==
        LiveRiskPositionState::TimeoutClosing
    );
    assert(
        risk.apply_fill<Side::Sell>(2, 100, 0, 3).kind ==
        LiveRiskFillKind::Flip
    );
    assert(
        risk.snapshot().position_state ==
        LiveRiskPositionState::TimeoutClosing
    );
}

void test_partial_allocation_rounds_and_conserves_quote_atoms() {
    LiveRiskState risk(config(), 0);
    assert(
        risk.apply_fill<Side::Buy>(3, 100, 2, 1).status ==
        LiveRiskUpdateStatus::Applied
    );
    auto first_close = risk.apply_fill<Side::Sell>(1, 100, 1, 2);
    (void)first_close;
    assert(first_close.realized_delta_quote_atoms == -2);
    auto snap = risk.snapshot();
    assert(snap.position_lots == 2);
    assert(snap.open_cost_basis_quote_atoms == 2'000);
    assert(snap.open_commission_quote_atoms == 1);

    auto final_close = risk.apply_fill<Side::Sell>(2, 100, 1, 3);
    (void)final_close;
    assert(final_close.realized_delta_quote_atoms == -2);
    snap = risk.snapshot();
    assert(snap.position_lots == 0);
    assert(snap.realized_pnl_quote_atoms == -4);
    // 2 opening atoms + 1 + 1 closing atoms are conserved exactly.
    assert(snap.total_marked_pnl_quote_atoms == -4);
}

void test_daily_rollover_uses_pre_event_marked_baseline() {
    LiveRiskState risk(config(), kDayMs - 10);
    assert(
        risk.apply_fill<Side::Buy>(1, 100, 0, kDayMs - 9).status ==
        LiveRiskUpdateStatus::Applied
    );
    assert(
        risk.update_mark(110, kDayMs - 8) ==
        LiveRiskUpdateStatus::Applied
    );
    auto snap = risk.snapshot();
    assert(snap.total_marked_pnl_quote_atoms == 100);
    assert(snap.session_high_water_quote_atoms == 100);

    assert(
        risk.update_mark(90, kDayMs + 1) ==
        LiveRiskUpdateStatus::Applied
    );
    snap = risk.snapshot();
    assert(snap.utc_day == 1);
    assert(snap.day_start_total_pnl_quote_atoms == 100);
    assert(snap.total_marked_pnl_quote_atoms == -100);

    // A delayed prior-day fill cannot rewind the accounting day.
    assert(
        risk.apply_fill<Side::Buy>(1, 90, 0, kDayMs - 1).status ==
        LiveRiskUpdateStatus::Applied
    );
    assert(risk.snapshot().utc_day == 1);
}

void test_template_capacity_including_cross_zero() {
    LiveRiskState long_risk(config(), 0);
    assert(
        long_risk.apply_fill<Side::Buy>(6, 100, 0, 1).status ==
        LiveRiskUpdateStatus::Applied
    );
    auto cap = long_risk.cap_exposure_order<Side::Buy>(5, 100);
    assert(cap.valid());
    assert(cap.allowed_lots == 2);
    assert(cap.inventory_room_lots == 4);
    assert(cap.position_value_room_lots == 2);
    assert((cap.reason_mask & LiveCapacityReasonPositionValue) != 0);

    LiveRiskState short_risk(config(), 0);
    assert(
        short_risk.apply_fill<Side::Sell>(6, 100, 0, 1).status ==
        LiveRiskUpdateStatus::Applied
    );
    cap = short_risk.cap_exposure_order<Side::Sell>(5, 100);
    assert(cap.allowed_lots == 2);
    cap = short_risk.cap_exposure_order<Side::Buy>(20, 100);
    assert(cap.allowed_lots == 14);
    assert(cap.inventory_room_lots == 16);
    assert(cap.position_value_room_lots == 14);
    assert((cap.reason_mask & LiveCapacityReasonPositionValue) != 0);

    LiveRiskState inventory_limited(config(3, 100'000), 0);
    cap = inventory_limited.cap_exposure_order<Side::Buy>(5, 100);
    assert(cap.allowed_lots == 3);
    assert((cap.reason_mask & LiveCapacityReasonInventory) != 0);
}

void test_hard_risk_ordering_and_strict_boundaries() {
    LiveRiskState equality(config(10, 1'000, 50, 1'000), 0);
    assert(
        equality.apply_fill<Side::Buy>(1, 100, 50, 1).status ==
        LiveRiskUpdateStatus::Applied
    );
    // Daily PnL == -limit and position value == limit are both allowed.
    assert(
        equality.evaluate_hard_risk(100) == LiveHardRiskReason::None
    );

    LiveRiskState daily(config(10, 100'000, 50, 50), 0);
    assert(
        daily.apply_fill<Side::Buy>(1, 100, 0, 1).status ==
        LiveRiskUpdateStatus::Applied
    );
    assert(daily.update_mark(90, 2) == LiveRiskUpdateStatus::Applied);
    // Both daily loss and drawdown are breached; daily loss wins.
    assert(
        daily.evaluate_hard_risk(90) == LiveHardRiskReason::DailyLoss
    );

    LiveRiskState position(config(10, 8'000), 0);
    assert(
        position.apply_fill<Side::Buy>(9, 100, 0, 1).status ==
        LiveRiskUpdateStatus::Applied
    );
    assert(
        position.evaluate_hard_risk(100) ==
        LiveHardRiskReason::PositionValue
    );

    LiveRiskState drawdown(config(10, 100'000, 10'000, 50), 0);
    assert(
        drawdown.apply_fill<Side::Buy>(1, 100, 0, 1).status ==
        LiveRiskUpdateStatus::Applied
    );
    assert(drawdown.update_mark(110, 2) == LiveRiskUpdateStatus::Applied);
    assert(drawdown.update_mark(105, 3) == LiveRiskUpdateStatus::Applied);
    // Equality at 50 atoms does not trip, matching hard_risk_reason.
    assert(
        drawdown.evaluate_hard_risk(105) == LiveHardRiskReason::None
    );
    assert(drawdown.update_mark(104, 4) == LiveRiskUpdateStatus::Applied);
    assert(
        drawdown.evaluate_hard_risk(104) ==
        LiveHardRiskReason::EmergencyDrawdown
    );
}

void test_restore_and_overflow_fail_closed() {
    LiveRiskState source(config(), 0);
    assert(
        source.apply_fill<Side::Sell>(2, 100, -4, 1).status ==
        LiveRiskUpdateStatus::Applied
    );
    assert(source.update_mark(90, 2) == LiveRiskUpdateStatus::Applied);
    const auto saved = source.snapshot();

    LiveRiskState restored(config(), 0);
    assert(restored.restore(saved, 2) == LiveRiskUpdateStatus::Applied);
    const auto round_trip = restored.snapshot();
    (void)round_trip;
    assert(round_trip.position_lots == saved.position_lots);
    assert(
        round_trip.open_cost_basis_quote_atoms ==
        saved.open_cost_basis_quote_atoms
    );
    assert(
        round_trip.open_commission_quote_atoms ==
        saved.open_commission_quote_atoms
    );
    assert(
        round_trip.realized_pnl_quote_atoms ==
        saved.realized_pnl_quote_atoms
    );
    assert(
        round_trip.unrealized_pnl_quote_atoms ==
        saved.unrealized_pnl_quote_atoms
    );
    assert(
        round_trip.total_marked_pnl_quote_atoms ==
        saved.total_marked_pnl_quote_atoms
    );
    assert(
        round_trip.session_high_water_quote_atoms ==
        saved.session_high_water_quote_atoms
    );
    assert(round_trip.utc_day == saved.utc_day);
    assert(round_trip.position_state == saved.position_state);
    assert(round_trip.fault == saved.fault);

    auto broken = saved;
    ++broken.unrealized_pnl_quote_atoms;
    LiveRiskState invalid_restore(config(), 0);
    assert(
        invalid_restore.restore(broken, 2) ==
        LiveRiskUpdateStatus::RejectedInvalidInput
    );
    assert(
        invalid_restore.snapshot().fault == LiveRiskFault::InvalidRestore
    );
    assert(
        invalid_restore.evaluate_hard_risk(100) ==
        LiveHardRiskReason::InvalidState
    );

    LiveRiskState overflow(config(), 0);
    const auto failed = overflow.apply_fill<Side::Buy>(
        std::numeric_limits<std::int64_t>::max(),
        2,
        0,
        1
    );
    (void)failed;
    assert(failed.status == LiveRiskUpdateStatus::RejectedOverflow);
    assert(failed.applied_lots == 0);
    assert(overflow.snapshot().fault == LiveRiskFault::ArithmeticOverflow);
    assert(
        overflow.evaluate_hard_risk(100) ==
        LiveHardRiskReason::InvalidState
    );
}

volatile std::int64_t benchmark_sink = 0;

void run_benchmark() {
    constexpr std::int64_t iterations = 2'000'000;
    constexpr std::int64_t warmup_iterations = 100'000;
    LiveRiskState mark_risk(
        config(1'000, 1'000'000'000, 1'000'000'000, 1'000'000'000),
        0
    );
    if (mark_risk.apply_fill<Side::Buy>(1, 600'000, 0, 1).status !=
        LiveRiskUpdateStatus::Applied) {
        std::abort();
    }
    for (std::int64_t index = 0; index < warmup_iterations; ++index) {
        if (mark_risk.update_mark(
                600'000 + (index & 127),
                2 + index
            ) != LiveRiskUpdateStatus::Applied) {
            std::abort();
        }
    }
    const auto mark_start = std::chrono::steady_clock::now();
    for (std::int64_t index = 0; index < iterations; ++index) {
        const auto status = mark_risk.update_mark(
            600'000 + (index & 127),
            2 + warmup_iterations + index
        );
        if (status != LiveRiskUpdateStatus::Applied) {
            std::abort();
        }
    }
    const auto mark_end = std::chrono::steady_clock::now();
    benchmark_sink = mark_risk.snapshot().total_marked_pnl_quote_atoms;

    LiveRiskState fill_risk(
        config(1'000, 1'000'000'000, 1'000'000'000, 1'000'000'000),
        0
    );
    for (std::int64_t index = 0; index < warmup_iterations / 2; ++index) {
        if (fill_risk.apply_fill<Side::Buy>(1, 100, 0, index * 2 + 1).status !=
                LiveRiskUpdateStatus::Applied ||
            fill_risk.apply_fill<Side::Sell>(1, 101, 0, index * 2 + 2).status !=
                LiveRiskUpdateStatus::Applied) {
            std::abort();
        }
    }
    const auto fill_start = std::chrono::steady_clock::now();
    for (std::int64_t index = 0; index < iterations / 2; ++index) {
        const std::int64_t base_ts = warmup_iterations + index * 2;
        if (fill_risk.apply_fill<Side::Buy>(1, 100, 0, base_ts + 1).status !=
                LiveRiskUpdateStatus::Applied ||
            fill_risk.apply_fill<Side::Sell>(1, 101, 0, base_ts + 2).status !=
                LiveRiskUpdateStatus::Applied) {
            std::abort();
        }
    }
    const auto fill_end = std::chrono::steady_clock::now();
    benchmark_sink = fill_risk.snapshot().realized_pnl_quote_atoms;

    LiveRiskState cap_risk(config(1'000, 1'000'000'000), 0);
    for (std::int64_t index = 0; index < warmup_iterations; ++index) {
        const auto result = cap_risk.cap_exposure_order<Side::Buy>(
            1 + (index & 15),
            600'000 + (index & 31)
        );
        benchmark_sink = result.allowed_lots;
    }
    const auto cap_start = std::chrono::steady_clock::now();
    std::int64_t capacity_sum = 0;
    for (std::int64_t index = 0; index < iterations; ++index) {
        const auto result = cap_risk.cap_exposure_order<Side::Buy>(
            1 + (index & 15),
            600'000 + (index & 31)
        );
        capacity_sum += result.allowed_lots;
    }
    const auto cap_end = std::chrono::steady_clock::now();
    benchmark_sink = capacity_sum;

    const auto ns_per_operation = [](auto start, auto end, double count) {
        return static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count()
        ) / count;
    };
    std::cout << std::fixed << std::setprecision(2)
              << "footprint_bytes=" << LiveRiskState::footprint_bytes()
              << " cache_isolation_bytes="
              << LiveRiskState::cache_isolation_bytes() << '\n'
              << "mark_update_ns_per_op="
              << ns_per_operation(mark_start, mark_end, iterations) << '\n'
              << "fill_ns_per_op="
              << ns_per_operation(fill_start, fill_end, iterations) << '\n'
              << "capacity_ns_per_op="
              << ns_per_operation(cap_start, cap_end, iterations) << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    test_layout_and_invalid_config_fail_closed();
    test_fixed_point_open_add_reduce_flip_and_close();
    test_timeout_state_survives_partial_close_and_flip();
    test_partial_allocation_rounds_and_conserves_quote_atoms();
    test_daily_rollover_uses_pre_event_marked_baseline();
    test_template_capacity_including_cross_zero();
    test_hard_risk_ordering_and_strict_boundaries();
    test_restore_and_overflow_fail_closed();
    if (argc == 2 && std::strcmp(argv[1], "--benchmark") == 0) {
        run_benchmark();
    }
    return 0;
}
