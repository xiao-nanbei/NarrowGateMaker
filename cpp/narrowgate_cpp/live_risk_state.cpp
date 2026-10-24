#include "live_risk_state.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>

namespace narrowgate_cpp {
namespace {

constexpr std::int64_t kMillisecondsPerUtcDay = 86'400'000;

}  // namespace

LiveRiskState::LiveRiskState(
    LiveRiskConfig config,
    std::int64_t start_ts_ms
) noexcept : config_(config) {
    if (!valid_config(config_)) {
        state_.fault = LiveRiskFault::InvalidConfig;
        return;
    }
    if (!valid_accounting_timestamp(start_ts_ms)) {
        state_.fault = LiveRiskFault::InvalidInput;
        return;
    }
    state_.utc_day = static_cast<std::uint32_t>(
        start_ts_ms / kMillisecondsPerUtcDay
    );
}

bool LiveRiskState::valid_config(const LiveRiskConfig& config) noexcept {
    return config.max_inventory_lots > 0 &&
        config.max_position_value_quote_atoms > 0 &&
        config.max_daily_loss_quote_atoms > 0 &&
        config.emergency_close_drawdown_quote_atoms > 0 &&
        config.quote_atoms_per_price_tick_lot > 0;
}

bool LiveRiskState::valid_accounting_timestamp(
    std::int64_t accounting_ts_ms
) noexcept {
    if (accounting_ts_ms < 0) {
        return false;
    }
    const auto day = static_cast<std::uint64_t>(accounting_ts_ms) /
        static_cast<std::uint64_t>(kMillisecondsPerUtcDay);
    return day <= std::numeric_limits<std::uint32_t>::max();
}

bool LiveRiskState::checked_add(
    std::int64_t left,
    std::int64_t right,
    std::int64_t& result
) noexcept {
    return !__builtin_add_overflow(left, right, &result);
}

bool LiveRiskState::checked_sub(
    std::int64_t left,
    std::int64_t right,
    std::int64_t& result
) noexcept {
    return !__builtin_sub_overflow(left, right, &result);
}

bool LiveRiskState::checked_mul(
    std::int64_t left,
    std::int64_t right,
    std::int64_t& result
) noexcept {
    return !__builtin_mul_overflow(left, right, &result);
}

bool LiveRiskState::checked_abs(
    std::int64_t value,
    std::int64_t& result
) noexcept {
    if (value == std::numeric_limits<std::int64_t>::min()) {
        return false;
    }
    result = value < 0 ? -value : value;
    return true;
}

bool LiveRiskState::proportional_share(
    std::int64_t total,
    std::int64_t part,
    std::int64_t whole,
    std::int64_t& result
) noexcept {
    if (part < 0 || whole <= 0 || part > whole) {
        return false;
    }
    if (part == 0 || total == 0) {
        result = 0;
        return true;
    }
    if (part == whole) {
        result = total;
        return true;
    }

    using Wide = __int128_t;
    Wide numerator = static_cast<Wide>(total) * static_cast<Wide>(part);
    const bool negative = numerator < 0;
    if (negative) {
        numerator = -numerator;
    }
    const Wide denominator = static_cast<Wide>(whole);
    Wide magnitude = numerator / denominator;
    const Wide remainder = numerator % denominator;
    if (remainder * 2 >= denominator) {
        ++magnitude;
    }

    constexpr Wide kPositiveLimit = static_cast<Wide>(
        std::numeric_limits<std::int64_t>::max()
    );
    constexpr Wide kNegativeMagnitudeLimit = kPositiveLimit + 1;
    if ((!negative && magnitude > kPositiveLimit) ||
        (negative && magnitude > kNegativeMagnitudeLimit)) {
        return false;
    }
    if (negative && magnitude == kNegativeMagnitudeLimit) {
        result = std::numeric_limits<std::int64_t>::min();
    } else {
        const auto narrowed = static_cast<std::int64_t>(magnitude);
        result = negative ? -narrowed : narrowed;
    }
    return true;
}

bool LiveRiskState::fill_notional(
    std::int64_t quantity_lots,
    std::int64_t price_ticks,
    std::int64_t& result
) const noexcept {
    std::int64_t price_lot_units = 0;
    return quantity_lots > 0 && price_ticks > 0 &&
        checked_mul(quantity_lots, price_ticks, price_lot_units) &&
        checked_mul(
            price_lot_units,
            config_.quote_atoms_per_price_tick_lot,
            result
        );
}

bool LiveRiskState::roll_daily(
    State& next,
    std::int64_t accounting_ts_ms
) const noexcept {
    if (!valid_accounting_timestamp(accounting_ts_ms)) {
        return false;
    }
    const auto day = static_cast<std::uint32_t>(
        accounting_ts_ms / kMillisecondsPerUtcDay
    );
    // Delayed exchange fills never rewind the accounting day.
    if (day > next.utc_day) {
        next.utc_day = day;
        next.day_start_total_pnl_quote_atoms =
            next.total_marked_pnl_quote_atoms;
    }
    return true;
}

bool LiveRiskState::recompute_marked(State& next) const noexcept {
    std::int64_t absolute_position = 0;
    if (!checked_abs(next.position_lots, absolute_position)) {
        return false;
    }

    std::int64_t unrealized = 0;
    if (absolute_position > 0 && next.mark_price_ticks > 0) {
        std::int64_t marked_notional = 0;
        if (!fill_notional(
                absolute_position,
                next.mark_price_ticks,
                marked_notional
            )) {
            return false;
        }
        if (next.position_lots > 0) {
            if (!checked_sub(
                    marked_notional,
                    next.open_cost_basis_quote_atoms,
                    unrealized
                )) {
                return false;
            }
        } else if (!checked_sub(
                       next.open_cost_basis_quote_atoms,
                       marked_notional,
                       unrealized
                   )) {
            return false;
        }
    }
    next.unrealized_pnl_quote_atoms = unrealized;

    std::int64_t marked = 0;
    if (!checked_add(
            next.realized_pnl_quote_atoms,
            next.unrealized_pnl_quote_atoms,
            marked
        ) || !checked_sub(
            marked,
            next.open_commission_quote_atoms,
            marked
        )) {
        return false;
    }
    next.total_marked_pnl_quote_atoms = marked;
    next.session_high_water_quote_atoms = std::max(
        next.session_high_water_quote_atoms,
        next.total_marked_pnl_quote_atoms
    );

    std::int64_t drawdown = 0;
    return checked_sub(
        next.session_high_water_quote_atoms,
        next.total_marked_pnl_quote_atoms,
        drawdown
    ) && drawdown >= 0;
}

void LiveRiskState::latch_fault(LiveRiskFault fault) noexcept {
    if (state_.fault == LiveRiskFault::None) {
        state_.fault = fault;
    }
}

template <Side S>
LiveRiskFillResult LiveRiskState::apply_fill(
    std::int64_t quantity_lots,
    std::int64_t price_ticks,
    std::int64_t commission_quote_atoms,
    std::int64_t accounting_ts_ms
) noexcept {
    return apply_fill_side<S>(
        quantity_lots,
        price_ticks,
        commission_quote_atoms,
        accounting_ts_ms
    );
}

template <Side S>
LiveRiskFillResult LiveRiskState::apply_fill_side(
    std::int64_t quantity_lots,
    std::int64_t price_ticks,
    std::int64_t commission_quote_atoms,
    std::int64_t accounting_ts_ms
) noexcept {
    static_assert(S == Side::Buy || S == Side::Sell);
    LiveRiskFillResult result;
    const auto reject = [this](
                            LiveRiskFault fault,
                            LiveRiskUpdateStatus status
                        ) noexcept {
        latch_fault(fault);
        LiveRiskFillResult rejected;
        rejected.status = status;
        return rejected;
    };
    if (state_.fault != LiveRiskFault::None) {
        return result;
    }
    if (quantity_lots <= 0 || price_ticks <= 0 ||
        !valid_accounting_timestamp(accounting_ts_ms)) {
        return reject(
            LiveRiskFault::InvalidInput,
            LiveRiskUpdateStatus::RejectedInvalidInput
        );
    }

    State next = state_;
    if (!roll_daily(next, accounting_ts_ms)) {
        return reject(
            LiveRiskFault::InvalidInput,
            LiveRiskUpdateStatus::RejectedInvalidInput
        );
    }

    std::int64_t fill_value = 0;
    if (!fill_notional(quantity_lots, price_ticks, fill_value) ||
        !checked_add(
            next.total_traded_lots,
            quantity_lots,
            next.total_traded_lots
        )) {
        return reject(
            LiveRiskFault::ArithmeticOverflow,
            LiveRiskUpdateStatus::RejectedOverflow
        );
    }

    const std::int64_t old_position = next.position_lots;
    const LiveRiskPositionState old_state = next.position_state;
    std::int64_t old_absolute = 0;
    if (!checked_abs(old_position, old_absolute)) {
        return reject(
            LiveRiskFault::ArithmeticOverflow,
            LiveRiskUpdateStatus::RejectedOverflow
        );
    }

    constexpr std::int64_t kSideSign = is_buy_v<S> ? 1 : -1;
    std::int64_t signed_quantity = quantity_lots;
    if constexpr (!is_buy_v<S>) {
        signed_quantity = -quantity_lots;
    }

    result.applied_lots = quantity_lots;
    next.last_trade_realized_pnl_quote_atoms = 0;
    if (old_position == 0) {
        next.position_lots = signed_quantity;
        next.open_cost_basis_quote_atoms = fill_value;
        next.open_commission_quote_atoms = commission_quote_atoms;
        next.round_trip_realized_pnl_quote_atoms = 0;
        next.position_state = LiveRiskPositionState::Open;
        result.kind = LiveRiskFillKind::Open;
        result.opening_lots = quantity_lots;
    } else if ((old_position > 0) == is_buy_v<S>) {
        if (!checked_add(
                old_position,
                signed_quantity,
                next.position_lots
            ) || !checked_add(
                next.open_cost_basis_quote_atoms,
                fill_value,
                next.open_cost_basis_quote_atoms
            ) || !checked_add(
                next.open_commission_quote_atoms,
                commission_quote_atoms,
                next.open_commission_quote_atoms
            )) {
            return reject(
                LiveRiskFault::ArithmeticOverflow,
                LiveRiskUpdateStatus::RejectedOverflow
            );
        }
        result.kind = LiveRiskFillKind::Add;
        result.opening_lots = quantity_lots;
    } else {
        const std::int64_t closing_lots = std::min(quantity_lots, old_absolute);
        const std::int64_t opening_lots = quantity_lots - closing_lots;
        std::int64_t closing_commission = 0;
        std::int64_t opening_commission = 0;
        std::int64_t open_commission_share = 0;
        std::int64_t cost_basis_share = 0;
        std::int64_t closing_value = 0;
        if (!proportional_share(
                commission_quote_atoms,
                closing_lots,
                quantity_lots,
                closing_commission
            ) || !checked_sub(
                commission_quote_atoms,
                closing_commission,
                opening_commission
            ) || !proportional_share(
                next.open_commission_quote_atoms,
                closing_lots,
                old_absolute,
                open_commission_share
            ) || !proportional_share(
                next.open_cost_basis_quote_atoms,
                closing_lots,
                old_absolute,
                cost_basis_share
            ) || !fill_notional(
                closing_lots,
                price_ticks,
                closing_value
            )) {
            return reject(
                LiveRiskFault::ArithmeticOverflow,
                LiveRiskUpdateStatus::RejectedOverflow
            );
        }

        std::int64_t gross_realized = 0;
        if constexpr (S == Side::Sell) {
            if (!checked_sub(
                    closing_value,
                    cost_basis_share,
                    gross_realized
                )) {
                return reject(
                    LiveRiskFault::ArithmeticOverflow,
                    LiveRiskUpdateStatus::RejectedOverflow
                );
            }
        } else if (!checked_sub(
                       cost_basis_share,
                       closing_value,
                       gross_realized
                   )) {
            return reject(
                LiveRiskFault::ArithmeticOverflow,
                LiveRiskUpdateStatus::RejectedOverflow
            );
        }

        std::int64_t realized_delta = 0;
        if (!checked_sub(
                gross_realized,
                closing_commission,
                realized_delta
            ) || !checked_sub(
                realized_delta,
                open_commission_share,
                realized_delta
            ) || !checked_add(
                next.realized_pnl_quote_atoms,
                realized_delta,
                next.realized_pnl_quote_atoms
            ) || !checked_add(
                next.round_trip_realized_pnl_quote_atoms,
                realized_delta,
                next.round_trip_realized_pnl_quote_atoms
            )) {
            return reject(
                LiveRiskFault::ArithmeticOverflow,
                LiveRiskUpdateStatus::RejectedOverflow
            );
        }
        next.last_trade_realized_pnl_quote_atoms = realized_delta;
        result.realized_delta_quote_atoms = realized_delta;
        result.closing_lots = closing_lots;
        result.opening_lots = opening_lots;

        if (closing_lots == old_absolute) {
            result.round_trip_closed = true;
            if (next.round_trip_realized_pnl_quote_atoms < 0) {
                if (next.consecutive_losses ==
                    std::numeric_limits<std::uint32_t>::max()) {
                    return reject(
                        LiveRiskFault::ArithmeticOverflow,
                        LiveRiskUpdateStatus::RejectedOverflow
                    );
                }
                ++next.consecutive_losses;
            } else {
                next.consecutive_losses = 0;
            }
            next.round_trip_realized_pnl_quote_atoms = 0;

            if (opening_lots > 0) {
                std::int64_t opening_value = 0;
                if (!fill_notional(opening_lots, price_ticks, opening_value)) {
                    return reject(
                        LiveRiskFault::ArithmeticOverflow,
                        LiveRiskUpdateStatus::RejectedOverflow
                    );
                }
                next.position_lots = kSideSign * opening_lots;
                next.open_cost_basis_quote_atoms = opening_value;
                next.open_commission_quote_atoms = opening_commission;
                next.position_state = old_state ==
                        LiveRiskPositionState::TimeoutClosing
                    ? LiveRiskPositionState::TimeoutClosing
                    : LiveRiskPositionState::Open;
                result.kind = LiveRiskFillKind::Flip;
            } else {
                next.position_lots = 0;
                next.open_cost_basis_quote_atoms = 0;
                next.open_commission_quote_atoms = 0;
                next.unrealized_pnl_quote_atoms = 0;
                next.position_state = LiveRiskPositionState::Flat;
                result.kind = LiveRiskFillKind::Close;
            }
        } else {
            if (!checked_add(
                    old_position,
                    signed_quantity,
                    next.position_lots
                ) || !checked_sub(
                    next.open_cost_basis_quote_atoms,
                    cost_basis_share,
                    next.open_cost_basis_quote_atoms
                ) || !checked_sub(
                    next.open_commission_quote_atoms,
                    open_commission_share,
                    next.open_commission_quote_atoms
                )) {
                return reject(
                    LiveRiskFault::ArithmeticOverflow,
                    LiveRiskUpdateStatus::RejectedOverflow
                );
            }
            result.kind = LiveRiskFillKind::Reduce;
        }
    }

    if (!recompute_marked(next)) {
        return reject(
            LiveRiskFault::ArithmeticOverflow,
            LiveRiskUpdateStatus::RejectedOverflow
        );
    }

    std::int64_t absolute_position = 0;
    if (!checked_abs(next.position_lots, absolute_position)) {
        return reject(
            LiveRiskFault::ArithmeticOverflow,
            LiveRiskUpdateStatus::RejectedOverflow
        );
    }
    result.inventory_limit_exceeded =
        absolute_position > config_.max_inventory_lots;
    state_ = next;
    result.status = LiveRiskUpdateStatus::Applied;
    return result;
}

LiveRiskUpdateStatus LiveRiskState::update_mark(
    std::int64_t price_ticks,
    std::int64_t accounting_ts_ms
) noexcept {
    if (state_.fault != LiveRiskFault::None) {
        return LiveRiskUpdateStatus::RejectedFaulted;
    }
    if (price_ticks <= 0 || !valid_accounting_timestamp(accounting_ts_ms)) {
        latch_fault(LiveRiskFault::InvalidInput);
        return LiveRiskUpdateStatus::RejectedInvalidInput;
    }
    State next = state_;
    if (!roll_daily(next, accounting_ts_ms)) {
        latch_fault(LiveRiskFault::InvalidInput);
        return LiveRiskUpdateStatus::RejectedInvalidInput;
    }
    next.mark_price_ticks = price_ticks;
    if (!recompute_marked(next)) {
        latch_fault(LiveRiskFault::ArithmeticOverflow);
        return LiveRiskUpdateStatus::RejectedOverflow;
    }
    state_ = next;
    return LiveRiskUpdateStatus::Applied;
}

LiveRiskUpdateStatus LiveRiskState::set_timeout_closing() noexcept {
    if (state_.fault != LiveRiskFault::None) {
        return LiveRiskUpdateStatus::RejectedFaulted;
    }
    if (state_.position_state == LiveRiskPositionState::Open) {
        state_.position_state = LiveRiskPositionState::TimeoutClosing;
    }
    return LiveRiskUpdateStatus::Applied;
}

LiveHardRiskReason LiveRiskState::evaluate_hard_risk(
    std::int64_t mid_price_ticks
) noexcept {
    if (state_.fault != LiveRiskFault::None || mid_price_ticks <= 0) {
        if (state_.fault == LiveRiskFault::None) {
            latch_fault(LiveRiskFault::InvalidInput);
        }
        return LiveHardRiskReason::InvalidState;
    }

    std::int64_t daily_pnl = 0;
    std::int64_t negative_daily_limit = 0;
    std::int64_t absolute_position = 0;
    std::int64_t position_value = 0;
    std::int64_t drawdown = 0;
    if (!checked_sub(
            state_.total_marked_pnl_quote_atoms,
            state_.day_start_total_pnl_quote_atoms,
            daily_pnl
        ) || !checked_sub(
            0,
            config_.max_daily_loss_quote_atoms,
            negative_daily_limit
        ) || !checked_abs(
            state_.position_lots,
            absolute_position
        )) {
        latch_fault(LiveRiskFault::ArithmeticOverflow);
        return LiveHardRiskReason::InvalidState;
    }
    if (absolute_position > 0 && !fill_notional(
            absolute_position,
            mid_price_ticks,
            position_value
        )) {
        latch_fault(LiveRiskFault::ArithmeticOverflow);
        return LiveHardRiskReason::InvalidState;
    }
    if (!checked_sub(
            state_.session_high_water_quote_atoms,
            state_.total_marked_pnl_quote_atoms,
            drawdown
        ) || drawdown < 0) {
        latch_fault(LiveRiskFault::ArithmeticOverflow);
        return LiveHardRiskReason::InvalidState;
    }

    if (daily_pnl < negative_daily_limit) {
        return LiveHardRiskReason::DailyLoss;
    }
    if (position_value > config_.max_position_value_quote_atoms) {
        return LiveHardRiskReason::PositionValue;
    }
    if (drawdown > config_.emergency_close_drawdown_quote_atoms) {
        return LiveHardRiskReason::EmergencyDrawdown;
    }
    return LiveHardRiskReason::None;
}

std::size_t LiveRiskState::footprint_bytes() noexcept {
    return sizeof(LiveRiskState);
}

LiveRiskSnapshot LiveRiskState::snapshot() const noexcept {
    return LiveRiskSnapshot{
        state_.position_lots,
        state_.open_cost_basis_quote_atoms,
        state_.open_commission_quote_atoms,
        state_.realized_pnl_quote_atoms,
        state_.unrealized_pnl_quote_atoms,
        state_.round_trip_realized_pnl_quote_atoms,
        state_.last_trade_realized_pnl_quote_atoms,
        state_.mark_price_ticks,
        state_.total_marked_pnl_quote_atoms,
        state_.day_start_total_pnl_quote_atoms,
        state_.session_high_water_quote_atoms,
        state_.total_traded_lots,
        state_.utc_day,
        state_.consecutive_losses,
        state_.position_state,
        state_.fault,
    };
}

bool LiveRiskState::validate_restore_state(
    const State& restored,
    std::int64_t start_ts_ms
) const noexcept {
    if (!valid_accounting_timestamp(start_ts_ms) ||
        restored.fault != LiveRiskFault::None ||
        restored.utc_day > static_cast<std::uint32_t>(
            start_ts_ms / kMillisecondsPerUtcDay
        ) || restored.mark_price_ticks < 0 ||
        restored.total_traded_lots < 0 ||
        restored.position_lots == std::numeric_limits<std::int64_t>::min()) {
        return false;
    }

    if (restored.position_lots == 0) {
        if (restored.open_cost_basis_quote_atoms != 0 ||
            restored.open_commission_quote_atoms != 0 ||
            restored.unrealized_pnl_quote_atoms != 0 ||
            restored.round_trip_realized_pnl_quote_atoms != 0 ||
            restored.position_state != LiveRiskPositionState::Flat) {
            return false;
        }
    } else if (restored.open_cost_basis_quote_atoms <= 0 ||
               restored.position_state == LiveRiskPositionState::Flat) {
        return false;
    }

    State recomputed = restored;
    const std::int64_t supplied_high_water =
        restored.session_high_water_quote_atoms;
    recomputed.session_high_water_quote_atoms =
        restored.total_marked_pnl_quote_atoms;
    if (!recompute_marked(recomputed) ||
        recomputed.unrealized_pnl_quote_atoms !=
            restored.unrealized_pnl_quote_atoms ||
        recomputed.total_marked_pnl_quote_atoms !=
            restored.total_marked_pnl_quote_atoms ||
        supplied_high_water < restored.total_marked_pnl_quote_atoms) {
        return false;
    }
    std::int64_t drawdown = 0;
    return checked_sub(
        supplied_high_water,
        restored.total_marked_pnl_quote_atoms,
        drawdown
    ) && drawdown >= 0;
}

LiveRiskUpdateStatus LiveRiskState::restore(
    const LiveRiskSnapshot& snapshot_value,
    std::int64_t start_ts_ms
) noexcept {
    if (state_.fault != LiveRiskFault::None) {
        return LiveRiskUpdateStatus::RejectedFaulted;
    }
    State restored{
        snapshot_value.position_lots,
        snapshot_value.open_cost_basis_quote_atoms,
        snapshot_value.open_commission_quote_atoms,
        snapshot_value.realized_pnl_quote_atoms,
        snapshot_value.unrealized_pnl_quote_atoms,
        snapshot_value.round_trip_realized_pnl_quote_atoms,
        snapshot_value.last_trade_realized_pnl_quote_atoms,
        snapshot_value.mark_price_ticks,
        snapshot_value.total_marked_pnl_quote_atoms,
        snapshot_value.day_start_total_pnl_quote_atoms,
        snapshot_value.session_high_water_quote_atoms,
        snapshot_value.total_traded_lots,
        snapshot_value.utc_day,
        snapshot_value.consecutive_losses,
        snapshot_value.position_state,
        snapshot_value.fault,
    };
    if (!validate_restore_state(restored, start_ts_ms)) {
        latch_fault(LiveRiskFault::InvalidRestore);
        return LiveRiskUpdateStatus::RejectedInvalidInput;
    }
    state_ = restored;
    return LiveRiskUpdateStatus::Applied;
}

template LiveRiskFillResult LiveRiskState::apply_fill<Side::Buy>(
    std::int64_t,
    std::int64_t,
    std::int64_t,
    std::int64_t
) noexcept;
template LiveRiskFillResult LiveRiskState::apply_fill<Side::Sell>(
    std::int64_t,
    std::int64_t,
    std::int64_t,
    std::int64_t
) noexcept;

template LiveRiskFillResult LiveRiskState::apply_fill_side<Side::Buy>(
    std::int64_t,
    std::int64_t,
    std::int64_t,
    std::int64_t
) noexcept;
template LiveRiskFillResult LiveRiskState::apply_fill_side<Side::Sell>(
    std::int64_t,
    std::int64_t,
    std::int64_t,
    std::int64_t
) noexcept;

}  // namespace narrowgate_cpp
