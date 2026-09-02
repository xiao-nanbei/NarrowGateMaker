#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "common.hpp"

namespace narrowgate_cpp {

// The planner sits after final quote-price transforms and before any network
// request.  Every price and quantity that crosses this boundary is already on
// the exchange lattice: price ticks and quantity lots.  Notional values use a
// caller-selected integer quote-atom scale.  The only binary64 fields are
// immutable policy thresholds needed to reproduce B0 arithmetic; no wire
// price/quantity, strings, containers, pointers or owning allocations enter
// this ABI. The one retained dynamic binary64 price is accompanied by, and
// checked against, its integer tick coordinate; it exists solely to preserve
// B0's exact min-notional multiplication at an equality boundary.

enum class LivePlannerOrderState : std::uint8_t {
    Empty = 0,
    PendingNew = 1,
    Active = 2,
    PendingCancel = 3,
    Terminal = 4,
};

enum class LiveOrderAction : std::uint8_t {
    None = 0,
    Keep = 1,
    Place = 2,
    CancelFirst = 3,
    Pending = 4,
    Pause = 5,
    SkipFilter = 6,
    RouteDisabled = 7,
    Invalid = 8,
};

enum LiveOrderPlanReason : std::uint32_t {
    LiveOrderPlanReasonNone = 0,
    LiveOrderPlanReasonRouteDisabled = 1U << 0,
    LiveOrderPlanReasonInventoryLimit = 1U << 1,
    LiveOrderPlanReasonPolicyPost = 1U << 2,
    LiveOrderPlanReasonPolicyExposure = 1U << 3,
    LiveOrderPlanReasonPriceDrift = 1U << 4,
    LiveOrderPlanReasonTtl = 1U << 5,
    LiveOrderPlanReasonExistingPositionValue = 1U << 6,
    LiveOrderPlanReasonThrottlePrice = 1U << 7,
    LiveOrderPlanReasonThrottleAge = 1U << 8,
    LiveOrderPlanReasonPendingLifecycle = 1U << 9,
    LiveOrderPlanReasonMinQuantity = 1U << 10,
    LiveOrderPlanReasonMinNotional = 1U << 11,
    LiveOrderPlanReasonPositionValueCap = 1U << 12,
    LiveOrderPlanReasonInventoryQuantityCap = 1U << 13,
    LiveOrderPlanReasonCloseQuantityCap = 1U << 14,
    LiveOrderPlanReasonConfiguredCancelFirst = 1U << 15,
    LiveOrderPlanReasonInvalidInput = 1U << 31,
};

enum LiveOrderSideInputFlag : std::uint8_t {
    LiveOrderSideInputRouteAllowed = 1U << 0,
    LiveOrderSideInputAllowPost = 1U << 1,
    LiveOrderSideInputAllowExposureIncrease = 1U << 2,
    // P3 active-order floor, state-conditioned quote policy and other already
    // evaluated safeguards may force a replacement through the throttle.
    LiveOrderSideInputForceUpdate = 1U << 3,
    // MakerEngine currently evaluates its initial drift/TTL decision before
    // later state/P3 transforms.  A gated native caller supplies that frozen
    // B0 decision so the planner does not recompute it from a later price.
    LiveOrderSideInputUseProvidedNeedsUpdate = 1U << 4,
    LiveOrderSideInputProvidedNeedsUpdate = 1U << 5,
};

enum LiveOrderReplaceFlag : std::uint8_t {
    LiveOrderReplacePendingCoalesce = 1U << 0,
    LiveOrderReplaceCancelFirstExposureIncrease = 1U << 1,
};

// One shared x86 cache line.  Raw binary64 risk inputs are retained so the
// native planner can execute B0's subtraction/division/floor operations in
// their original order; independently rounded integer atoms change exact and
// nextafter boundary behavior.  inventory_lots remains the checked lattice
// identity used by the rest of the planner.
struct alignas(64) LiveOrderPlannerContext {
    double inventory;
    double max_inventory;
    double max_position_value;
    double mid;
    double lot_size;
    std::int64_t inventory_lots;
    std::int64_t min_quantity_lots;
    // Kept as the configured bps binary64 because B0 divides this value by
    // 10,000 before comparing price drift.  Prices themselves remain ticks.
    double requote_threshold_bps;
};

// Add/reducing replace thresholds are separated because MakerEngine currently
// selects them by inventory sign, not by cross-zero order quantity.
struct alignas(64) LiveOrderReplaceConfig {
    // These are immutable config scalars, not wire prices.  Reconstructing a
    // tick-aligned price with tick_size preserves MakerEngine's binary64
    // `price_delta_ticks + 1e-9` boundary exactly.
    double tick_size;
    double lot_size;
    // Preserve the exact B0 binary64 `quantity * price < min_notional`
    // boundary. Integer quote atoms remain in the context as the validated
    // economic identity, but cannot reproduce a binary64 equality that lands
    // one ULP below the same decimal value.
    double min_notional;
    double add_min_price_change_ticks;
    double reducing_min_price_change_ticks;
    // Retain configured binary64 milliseconds so the '< interval' boundary
    // is bit-for-bit the same as MakerEngine B0.
    double add_min_interval_ms;
    double reducing_min_interval_ms;
    std::uint8_t flags;
    std::uint8_t reserved[7];
};

struct alignas(64) LiveSideOrderActionInput {
    std::int64_t target_price_ticks;
    // Quantity after eta/symmetric/policy size shaping, before inventory and
    // position-value caps.
    std::int64_t desired_quantity_lots;
    // Current B0 classifies policy exposure from base order_size rather than
    // desired_quantity_lots.  Keeping this explicit preserves that order.
    std::int64_t exposure_probe_quantity_lots;
    union {
        // Used when this component derives price drift itself.
        std::int64_t existing_price_ticks;
        // Used with UseProvidedNeedsUpdate. This is the exact final B0 price
        // used for the min-notional multiplication.
        double target_price;
    };
    std::int64_t existing_remaining_lots;
    // B0 derives age in binary64 milliseconds from time.time(). Re-encoding
    // to integer nanoseconds can flip a comparison at the exact boundary.
    double order_age_ms;
    union {
        // Used when the caller asks this component to derive drift/TTL.
        double order_ttl_ms;
        // Used with UseProvidedNeedsUpdate. This is B0's already-computed
        // binary64 price_delta_ticks for exact +1e-9 throttle semantics.
        double provided_price_delta_ticks;
    };
    LivePlannerOrderState order_state;
    std::uint8_t flags;
    std::uint8_t reserved[6];
};

// A side result is exactly one 64-byte x86 cache line.  action describes the
// next safe operation.  An active order that must change always returns
// CancelFirst; no stale decision can directly authorize a replacement submit.
struct alignas(64) LiveSideOrderActionPlan {
    std::int64_t target_price_ticks;
    std::int64_t target_quantity_lots;
    std::int64_t inventory_room_lots;
    std::int64_t position_value_room_lots;
    std::int64_t existing_remaining_lots;
    std::uint32_t reason_mask;
    LiveOrderAction action;
    bool exposure_increasing;
    bool can_post_after_inventory;
    bool can_post;
    bool needs_update;
    bool force_update;
    bool order_active;
    bool order_pending;
    bool filter_valid;
    bool cancel_existing;
    std::uint8_t reserved[6];
};

struct alignas(64) LiveDualOrderActionPlan {
    LiveSideOrderActionPlan buy;
    LiveSideOrderActionPlan sell;
};

static_assert(std::is_trivial_v<LiveOrderPlannerContext>);
static_assert(std::is_trivial_v<LiveOrderReplaceConfig>);
static_assert(std::is_trivial_v<LiveSideOrderActionInput>);
static_assert(std::is_trivial_v<LiveSideOrderActionPlan>);
static_assert(std::is_trivial_v<LiveDualOrderActionPlan>);
static_assert(std::is_standard_layout_v<LiveOrderPlannerContext>);
static_assert(std::is_standard_layout_v<LiveOrderReplaceConfig>);
static_assert(std::is_standard_layout_v<LiveSideOrderActionInput>);
static_assert(std::is_standard_layout_v<LiveSideOrderActionPlan>);
static_assert(std::is_standard_layout_v<LiveDualOrderActionPlan>);
static_assert(std::is_trivially_copyable_v<LiveOrderPlannerContext>);
static_assert(std::is_trivially_copyable_v<LiveOrderReplaceConfig>);
static_assert(std::is_trivially_copyable_v<LiveSideOrderActionInput>);
static_assert(std::is_trivially_copyable_v<LiveSideOrderActionPlan>);
static_assert(std::is_trivially_copyable_v<LiveDualOrderActionPlan>);
static_assert(sizeof(LiveOrderPlannerContext) == 64);
static_assert(sizeof(LiveOrderReplaceConfig) == 64);
static_assert(sizeof(LiveSideOrderActionInput) == 64);
static_assert(sizeof(LiveSideOrderActionPlan) == 64);
static_assert(sizeof(LiveDualOrderActionPlan) == 128);
static_assert(alignof(LiveDualOrderActionPlan) == 64);

template <Side S>
[[nodiscard]] LiveSideOrderActionPlan compute_live_side_order_action_plan(
    const LiveOrderPlannerContext& context,
    const LiveOrderReplaceConfig& replace,
    const LiveSideOrderActionInput& input
) noexcept;

[[nodiscard]] LiveDualOrderActionPlan compute_live_order_action_plan(
    const LiveOrderPlannerContext& context,
    const LiveOrderReplaceConfig& replace,
    const LiveSideOrderActionInput& buy,
    const LiveSideOrderActionInput& sell
) noexcept;

[[nodiscard]] constexpr const char* live_order_action_name(
    LiveOrderAction action
) noexcept {
    switch (action) {
        case LiveOrderAction::None:
            return "none";
        case LiveOrderAction::Keep:
            return "keep";
        case LiveOrderAction::Place:
            return "place";
        case LiveOrderAction::CancelFirst:
            return "cancel_first";
        case LiveOrderAction::Pending:
            return "pending";
        case LiveOrderAction::Pause:
            return "pause";
        case LiveOrderAction::SkipFilter:
            return "skip_filter";
        case LiveOrderAction::RouteDisabled:
            return "route_disabled";
        case LiveOrderAction::Invalid:
            return "invalid";
    }
    return "invalid";
}

}  // namespace narrowgate_cpp
