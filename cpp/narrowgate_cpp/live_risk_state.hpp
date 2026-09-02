#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "common.hpp"

namespace narrowgate_cpp {

// Native live inventory accounting uses an explicit integer lattice:
//
//   quantity      = integer exchange lots
//   price         = integer exchange ticks
//   money / PnL   = integer quote atoms
//   fill notional = lots * ticks * quote_atoms_per_price_tick_lot
//
// The caller chooses a quote-atom scale fine enough that one price-tick/lot
// product is integral. Partial fee/cost allocation rounds to the nearest quote
// atom (ties away from zero) while conserving the original total. This is a
// new native fixed-point contract; it does not claim sub-atom identity with
// InventoryManager's binary64 bookkeeping.
struct LiveRiskConfig {
    std::int64_t max_inventory_lots = 0;
    std::int64_t max_position_value_quote_atoms = 0;
    std::int64_t max_daily_loss_quote_atoms = 0;
    std::int64_t emergency_close_drawdown_quote_atoms = 0;
    std::int64_t quote_atoms_per_price_tick_lot = 0;
};

static_assert(std::is_trivially_copyable_v<LiveRiskConfig>);

enum class LiveRiskFault : std::uint8_t {
    None = 0,
    InvalidConfig = 1,
    InvalidInput = 2,
    ArithmeticOverflow = 3,
    InvalidRestore = 4,
};

enum class LiveRiskUpdateStatus : std::uint8_t {
    Applied = 0,
    RejectedInvalidInput = 1,
    RejectedOverflow = 2,
    RejectedFaulted = 3,
};

enum class LiveRiskPositionState : std::uint8_t {
    Flat = 0,
    Open = 1,
    TimeoutClosing = 2,
};

enum class LiveRiskFillKind : std::uint8_t {
    Open = 0,
    Add = 1,
    Reduce = 2,
    Close = 3,
    Flip = 4,
};

// Ordering mirrors replay_controls.hard_risk_reason for valid state. Invalid
// or arithmetically unrepresentable state is checked first and fails closed.
enum class LiveHardRiskReason : std::uint8_t {
    None = 0,
    InvalidState = 1,
    DailyLoss = 2,
    PositionValue = 3,
    EmergencyDrawdown = 4,
};

enum LiveCapacityReason : std::uint8_t {
    LiveCapacityReasonNone = 0,
    LiveCapacityReasonInvalid = 1U << 0,
    LiveCapacityReasonInventory = 1U << 1,
    LiveCapacityReasonPositionValue = 1U << 2,
};

struct LiveRiskFillResult {
    LiveRiskUpdateStatus status = LiveRiskUpdateStatus::RejectedFaulted;
    LiveRiskFillKind kind = LiveRiskFillKind::Open;
    std::int64_t applied_lots = 0;
    std::int64_t closing_lots = 0;
    std::int64_t opening_lots = 0;
    std::int64_t realized_delta_quote_atoms = 0;
    bool round_trip_closed = false;
    bool inventory_limit_exceeded = false;
};

struct LiveOrderCapacity {
    std::int64_t allowed_lots = 0;
    std::int64_t inventory_room_lots = 0;
    std::int64_t position_value_room_lots = 0;
    std::uint8_t reason_mask = LiveCapacityReasonNone;

    [[nodiscard]] bool valid() const noexcept {
        return (reason_mask & LiveCapacityReasonInvalid) == 0;
    }
};

struct LiveRiskSnapshot {
    std::int64_t position_lots = 0;
    std::int64_t open_cost_basis_quote_atoms = 0;
    std::int64_t open_commission_quote_atoms = 0;
    std::int64_t realized_pnl_quote_atoms = 0;
    std::int64_t unrealized_pnl_quote_atoms = 0;
    std::int64_t round_trip_realized_pnl_quote_atoms = 0;
    std::int64_t last_trade_realized_pnl_quote_atoms = 0;
    std::int64_t mark_price_ticks = 0;
    std::int64_t total_marked_pnl_quote_atoms = 0;
    std::int64_t day_start_total_pnl_quote_atoms = 0;
    std::int64_t session_high_water_quote_atoms = 0;
    std::int64_t total_traded_lots = 0;
    std::uint32_t utc_day = 0;
    std::uint32_t consecutive_losses = 0;
    LiveRiskPositionState position_state = LiveRiskPositionState::Flat;
    LiveRiskFault fault = LiveRiskFault::None;

    [[nodiscard]] bool valid() const noexcept {
        return fault == LiveRiskFault::None;
    }
};

static_assert(std::is_trivially_copyable_v<LiveRiskFillResult>);
static_assert(std::is_trivially_copyable_v<LiveOrderCapacity>);
static_assert(std::is_trivially_copyable_v<LiveRiskSnapshot>);

// Single-writer hot inventory/risk state. It contains no mutex, atomic,
// string, vector or other owning allocation. A RuntimeCore may publish the POD
// snapshot to readers; concurrent access to this object itself is not allowed.
// The whole component is isolated from adjacent independently-mutated runtime
// cells at the architecture-specific boundary from common.hpp. Internal fields
// are deliberately packed rather than individually over-aligned so sequential
// fill/mark accounting does not inflate its cache working set.
class alignas(kDestructiveInterferenceBytes) LiveRiskState final {
public:
    explicit LiveRiskState(
        LiveRiskConfig config,
        std::int64_t start_ts_ms
    ) noexcept;

    LiveRiskState(const LiveRiskState&) = default;
    LiveRiskState& operator=(const LiveRiskState&) = default;

    template <Side S>
    [[nodiscard]] LiveRiskFillResult apply_fill(
        std::int64_t quantity_lots,
        std::int64_t price_ticks,
        std::int64_t commission_quote_atoms,
        std::int64_t accounting_ts_ms
    ) noexcept;

    [[nodiscard]] LiveRiskUpdateStatus update_mark(
        std::int64_t price_ticks,
        std::int64_t accounting_ts_ms
    ) noexcept;

    [[nodiscard]] LiveRiskUpdateStatus set_timeout_closing() noexcept;

    [[nodiscard]] LiveHardRiskReason evaluate_hard_risk(
        std::int64_t mid_price_ticks
    ) noexcept;

    // Mirrors replay_controls.cap_exposure_qty_by_position_value on the
    // integer lattice and additionally applies max_inventory. A cross-zero
    // request may consume the lots needed to flatten plus the opening room on
    // the other side. MakerEngine's separate close-only/no-flip policy, active
    // order reservation and exchange minQty/minNotional filters remain caller
    // responsibilities.
    template <Side S>
    [[nodiscard]] LiveOrderCapacity cap_exposure_order(
        std::int64_t requested_lots,
        std::int64_t mid_price_ticks
    ) const noexcept {
        static_assert(S == Side::Buy || S == Side::Sell);
        LiveOrderCapacity result;
        if (state_.fault != LiveRiskFault::None || requested_lots <= 0 ||
            mid_price_ticks <= 0) {
            result.reason_mask = LiveCapacityReasonInvalid;
            return result;
        }

        std::int64_t per_lot_notional = 0;
        if (!checked_mul(
                mid_price_ticks,
                config_.quote_atoms_per_price_tick_lot,
                per_lot_notional
            ) || per_lot_notional <= 0) {
            result.reason_mask = LiveCapacityReasonInvalid;
            return result;
        }
        const std::int64_t value_limit_lots =
            config_.max_position_value_quote_atoms / per_lot_notional;

        std::int64_t inventory_room = 0;
        std::int64_t value_room = 0;
        if constexpr (S == Side::Buy) {
            if (!checked_sub(
                    config_.max_inventory_lots,
                    state_.position_lots,
                    inventory_room
                ) || !checked_sub(
                    value_limit_lots,
                    state_.position_lots,
                    value_room
                )) {
                result.reason_mask = LiveCapacityReasonInvalid;
                return result;
            }
        } else {
            if (!checked_add(
                    config_.max_inventory_lots,
                    state_.position_lots,
                    inventory_room
                ) || !checked_add(
                    value_limit_lots,
                    state_.position_lots,
                    value_room
                )) {
                result.reason_mask = LiveCapacityReasonInvalid;
                return result;
            }
        }

        result.inventory_room_lots = std::max<std::int64_t>(0, inventory_room);
        result.position_value_room_lots = std::max<std::int64_t>(0, value_room);
        result.allowed_lots = std::min({
            requested_lots,
            result.inventory_room_lots,
            result.position_value_room_lots,
        });
        if (result.allowed_lots < requested_lots) {
            if (result.inventory_room_lots <= result.allowed_lots) {
                result.reason_mask |= LiveCapacityReasonInventory;
            }
            if (result.position_value_room_lots <= result.allowed_lots) {
                result.reason_mask |= LiveCapacityReasonPositionValue;
            }
        }
        return result;
    }

    [[nodiscard]] LiveRiskSnapshot snapshot() const noexcept;

    // Restore is intentionally strict. Any inconsistent or future-day payload
    // latches InvalidRestore instead of inventing accounting baselines.
    [[nodiscard]] LiveRiskUpdateStatus restore(
        const LiveRiskSnapshot& snapshot,
        std::int64_t start_ts_ms
    ) noexcept;

    [[nodiscard]] const LiveRiskConfig& config() const noexcept {
        return config_;
    }

    [[nodiscard]] bool faulted() const noexcept {
        return state_.fault != LiveRiskFault::None;
    }

    static constexpr std::size_t cache_isolation_bytes() noexcept {
        return kDestructiveInterferenceBytes;
    }
    static std::size_t footprint_bytes() noexcept;

private:
    struct State {
        std::int64_t position_lots = 0;
        std::int64_t open_cost_basis_quote_atoms = 0;
        std::int64_t open_commission_quote_atoms = 0;
        std::int64_t realized_pnl_quote_atoms = 0;
        std::int64_t unrealized_pnl_quote_atoms = 0;
        std::int64_t round_trip_realized_pnl_quote_atoms = 0;
        std::int64_t last_trade_realized_pnl_quote_atoms = 0;
        std::int64_t mark_price_ticks = 0;
        std::int64_t total_marked_pnl_quote_atoms = 0;
        std::int64_t day_start_total_pnl_quote_atoms = 0;
        std::int64_t session_high_water_quote_atoms = 0;
        std::int64_t total_traded_lots = 0;
        std::uint32_t utc_day = 0;
        std::uint32_t consecutive_losses = 0;
        LiveRiskPositionState position_state = LiveRiskPositionState::Flat;
        LiveRiskFault fault = LiveRiskFault::None;
    };

    static_assert(std::is_trivially_copyable_v<State>);

    template <Side S>
    [[nodiscard]] LiveRiskFillResult apply_fill_side(
        std::int64_t quantity_lots,
        std::int64_t price_ticks,
        std::int64_t commission_quote_atoms,
        std::int64_t accounting_ts_ms
    ) noexcept;

    [[nodiscard]] static bool valid_config(
        const LiveRiskConfig& config
    ) noexcept;
    [[nodiscard]] static bool valid_accounting_timestamp(
        std::int64_t accounting_ts_ms
    ) noexcept;
    [[nodiscard]] static bool checked_add(
        std::int64_t left,
        std::int64_t right,
        std::int64_t& result
    ) noexcept;
    [[nodiscard]] static bool checked_sub(
        std::int64_t left,
        std::int64_t right,
        std::int64_t& result
    ) noexcept;
    [[nodiscard]] static bool checked_mul(
        std::int64_t left,
        std::int64_t right,
        std::int64_t& result
    ) noexcept;
    [[nodiscard]] static bool checked_abs(
        std::int64_t value,
        std::int64_t& result
    ) noexcept;
    [[nodiscard]] static bool proportional_share(
        std::int64_t total,
        std::int64_t part,
        std::int64_t whole,
        std::int64_t& result
    ) noexcept;

    [[nodiscard]] bool fill_notional(
        std::int64_t quantity_lots,
        std::int64_t price_ticks,
        std::int64_t& result
    ) const noexcept;
    [[nodiscard]] bool roll_daily(
        State& next,
        std::int64_t accounting_ts_ms
    ) const noexcept;
    [[nodiscard]] bool recompute_marked(State& next) const noexcept;
    [[nodiscard]] bool validate_restore_state(
        const State& restored,
        std::int64_t start_ts_ms
    ) const noexcept;
    void latch_fault(LiveRiskFault fault) noexcept;

    LiveRiskConfig config_{};
    State state_{};
};

static_assert(alignof(LiveRiskState) == kDestructiveInterferenceBytes);
static_assert(sizeof(LiveRiskState) % kDestructiveInterferenceBytes == 0);
static_assert(std::is_trivially_copyable_v<LiveRiskState>);
static_assert(std::is_trivially_destructible_v<LiveRiskState>);

extern template LiveRiskFillResult LiveRiskState::apply_fill<Side::Buy>(
    std::int64_t,
    std::int64_t,
    std::int64_t,
    std::int64_t
) noexcept;
extern template LiveRiskFillResult LiveRiskState::apply_fill<Side::Sell>(
    std::int64_t,
    std::int64_t,
    std::int64_t,
    std::int64_t
) noexcept;

}  // namespace narrowgate_cpp
