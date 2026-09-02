#include "live_order_action_plan.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

namespace narrowgate_cpp {
namespace {

[[nodiscard]] constexpr bool has_flag(
    std::uint8_t value,
    std::uint8_t flag
) noexcept {
    return (value & flag) != 0;
}

[[nodiscard]] constexpr bool valid_order_state(
    LivePlannerOrderState state
) noexcept {
    return state >= LivePlannerOrderState::Empty &&
        state <= LivePlannerOrderState::Terminal;
}

[[nodiscard]] constexpr bool order_is_active(
    LivePlannerOrderState state
) noexcept {
    return state == LivePlannerOrderState::PendingNew ||
        state == LivePlannerOrderState::Active;
}

[[nodiscard]] constexpr bool order_is_pending(
    LivePlannerOrderState state
) noexcept {
    return state == LivePlannerOrderState::PendingNew ||
        state == LivePlannerOrderState::PendingCancel;
}

[[nodiscard]] constexpr std::int64_t saturating_abs(
    std::int64_t value
) noexcept {
    if (value == std::numeric_limits<std::int64_t>::min()) {
        return std::numeric_limits<std::int64_t>::max();
    }
    return value < 0 ? -value : value;
}

[[nodiscard]] bool notional_at_least(
    std::int64_t quantity_lots,
    double target_price,
    const LiveOrderReplaceConfig& replace
) noexcept {
    if (replace.min_notional <= 0.0) {
        return true;
    }
    if (quantity_lots <= 0 || !std::isfinite(target_price) ||
        target_price <= 0.0) {
        return false;
    }
    const double quantity = static_cast<double>(quantity_lots) *
        replace.lot_size;
    return quantity * target_price >= replace.min_notional;
}

[[nodiscard]] double target_price_for_b0(
    const LiveOrderReplaceConfig& replace,
    const LiveSideOrderActionInput& input
) noexcept {
    if (has_flag(input.flags, LiveOrderSideInputUseProvidedNeedsUpdate)) {
        return input.target_price;
    }
    return static_cast<double>(input.target_price_ticks) * replace.tick_size;
}

[[nodiscard]] bool price_drift_exceeds(
    std::int64_t target_ticks,
    std::int64_t existing_ticks,
    double tick_size,
    double threshold_bps
) noexcept {
    if (target_ticks <= 0 || existing_ticks <= 0 ||
        !std::isfinite(tick_size) || tick_size <= 0.0 ||
        !std::isfinite(threshold_bps) || threshold_bps < 0.0) {
        return true;
    }
    const double target = static_cast<double>(target_ticks) * tick_size;
    const double existing = static_cast<double>(existing_ticks) * tick_size;
    const double drift = std::abs(target - existing) / existing;
    const double threshold = threshold_bps / 10000.0;
    return drift > threshold;
}

template <Side S>
[[nodiscard]] constexpr bool exposure_increasing(
    std::int64_t inventory_lots,
    std::int64_t quantity_lots
) noexcept {
    static_assert(S == Side::Buy || S == Side::Sell);
    if (quantity_lots <= 0) {
        return true;
    }
    if constexpr (S == Side::Buy) {
        return inventory_lots >= 0 || quantity_lots > -inventory_lots;
    } else {
        return inventory_lots <= 0 || quantity_lots > inventory_lots;
    }
}

template <Side S>
[[nodiscard]] constexpr bool sign_classified_as_add(
    std::int64_t inventory_lots
) noexcept {
    static_assert(S == Side::Buy || S == Side::Sell);
    if constexpr (S == Side::Buy) {
        return inventory_lots >= 0;
    } else {
        return inventory_lots <= 0;
    }
}

[[nodiscard]] bool valid_context(
    const LiveOrderPlannerContext& context
) noexcept {
    return context.max_inventory_lots > 0 &&
        context.max_position_value_quote_atoms >= 0 &&
        context.mid_notional_quote_atoms_per_lot > 0 &&
        context.quote_atoms_per_price_tick_lot > 0 &&
        context.min_quantity_lots > 0 &&
        context.min_notional_quote_atoms >= 0 &&
        std::isfinite(context.requote_threshold_bps) &&
        context.requote_threshold_bps >= 0.0 &&
        context.inventory_lots != std::numeric_limits<std::int64_t>::min();
}

[[nodiscard]] bool valid_replace(
    const LiveOrderReplaceConfig& replace
) noexcept {
    return std::isfinite(replace.tick_size) && replace.tick_size > 0.0 &&
        std::isfinite(replace.lot_size) && replace.lot_size > 0.0 &&
        std::isfinite(replace.min_notional) && replace.min_notional >= 0.0 &&
        std::isfinite(replace.add_min_price_change_ticks) &&
        replace.add_min_price_change_ticks >= 0.0 &&
        std::isfinite(replace.reducing_min_price_change_ticks) &&
        replace.reducing_min_price_change_ticks >= 0.0 &&
        std::isfinite(replace.add_min_interval_ms) &&
        replace.add_min_interval_ms >= 0.0 &&
        std::isfinite(replace.reducing_min_interval_ms) &&
        replace.reducing_min_interval_ms >= 0.0 &&
        (replace.flags & ~(LiveOrderReplacePendingCoalesce |
            LiveOrderReplaceCancelFirstExposureIncrease)) == 0;
}

[[nodiscard]] bool valid_side_input(
    const LiveSideOrderActionInput& input
) noexcept {
    constexpr std::uint8_t known_flags = LiveOrderSideInputRouteAllowed |
        LiveOrderSideInputAllowPost |
        LiveOrderSideInputAllowExposureIncrease |
        LiveOrderSideInputForceUpdate |
        LiveOrderSideInputUseProvidedNeedsUpdate |
        LiveOrderSideInputProvidedNeedsUpdate;
    const bool uses_provided_update = has_flag(
        input.flags,
        LiveOrderSideInputUseProvidedNeedsUpdate
    );
    const double update_scalar = uses_provided_update
        ? input.provided_price_delta_ticks
        : input.order_ttl_ms;
    const bool valid_price_identity = uses_provided_update
        ? std::isfinite(input.target_price) && input.target_price > 0.0
        : input.existing_price_ticks >= 0;
    return input.target_price_ticks > 0 &&
        input.desired_quantity_lots >= 0 &&
        input.exposure_probe_quantity_lots > 0 &&
        valid_price_identity &&
        input.existing_remaining_lots >= 0 &&
        std::isfinite(input.order_age_ms) && input.order_age_ms >= 0.0 &&
        std::isfinite(update_scalar) && update_scalar >= 0.0 &&
        valid_order_state(input.order_state) &&
        (input.flags & ~known_flags) == 0;
}

template <Side S>
[[nodiscard]] std::int64_t inventory_room(
    const LiveOrderPlannerContext& context
) noexcept {
    const auto max_inventory = static_cast<__int128>(
        context.max_inventory_lots
    );
    const auto inventory = static_cast<__int128>(context.inventory_lots);
    const auto room = is_buy_v<S>
        ? max_inventory - inventory
        : max_inventory + inventory;
    if (room <= 0) {
        return 0;
    }
    if (room > std::numeric_limits<std::int64_t>::max()) {
        return std::numeric_limits<std::int64_t>::max();
    }
    return static_cast<std::int64_t>(room);
}

template <Side S>
[[nodiscard]] std::int64_t position_value_room(
    const LiveOrderPlannerContext& context
) noexcept {
    // Match cap_exposure_qty_by_position_value's two explicit sentinels:
    // zero disables an exposure-labelled submit, while +infinity leaves the
    // requested quantity unchanged regardless of the current inventory sign.
    if (context.max_position_value_quote_atoms == 0) {
        return 0;
    }
    if (context.max_position_value_quote_atoms ==
        std::numeric_limits<std::int64_t>::max()) {
        return std::numeric_limits<std::int64_t>::max();
    }
    const std::int64_t value_limit_lots =
        context.max_position_value_quote_atoms /
        context.mid_notional_quote_atoms_per_lot;
    const auto limit = static_cast<__int128>(value_limit_lots);
    const auto inventory = static_cast<__int128>(context.inventory_lots);
    const auto room = is_buy_v<S> ? limit - inventory : limit + inventory;
    if (room <= 0) {
        return 0;
    }
    if (room > std::numeric_limits<std::int64_t>::max()) {
        return std::numeric_limits<std::int64_t>::max();
    }
    return static_cast<std::int64_t>(room);
}

template <Side S>
[[nodiscard]] std::int64_t cap_new_quantity(
    const LiveOrderPlannerContext& context,
    const LiveOrderReplaceConfig& replace,
    const LiveSideOrderActionInput& input,
    LiveSideOrderActionPlan& out
) noexcept {
    std::int64_t quantity = input.desired_quantity_lots;
    const std::int64_t inventory = context.inventory_lots;
    const double target_price = target_price_for_b0(replace, input);

    if constexpr (S == Side::Buy) {
        if (inventory > 0) {
            const std::int64_t room = inventory_room<S>(context);
            if (room >= 1) {
                if (quantity > room) {
                    out.reason_mask |= LiveOrderPlanReasonInventoryQuantityCap;
                    quantity = room;
                }
            } else {
                if (quantity > 0) {
                    out.reason_mask |= LiveOrderPlanReasonInventoryQuantityCap;
                }
                quantity = 0;
            }
        } else if (inventory < -1) {
            const std::int64_t close_cap = saturating_abs(inventory);
            if (close_cap >= context.min_quantity_lots && notional_at_least(
                    close_cap,
                    target_price,
                    replace
                ) && quantity > close_cap) {
                out.reason_mask |= LiveOrderPlanReasonCloseQuantityCap;
                quantity = close_cap;
            }
        }
    } else {
        if (inventory < 0) {
            const std::int64_t room = inventory_room<S>(context);
            if (room >= 1) {
                if (quantity > room) {
                    out.reason_mask |= LiveOrderPlanReasonInventoryQuantityCap;
                    quantity = room;
                }
            } else {
                if (quantity > 0) {
                    out.reason_mask |= LiveOrderPlanReasonInventoryQuantityCap;
                }
                quantity = 0;
            }
        } else if (inventory > 1) {
            const std::int64_t close_cap = inventory;
            if (close_cap >= context.min_quantity_lots && notional_at_least(
                    close_cap,
                    target_price,
                    replace
                ) && quantity > close_cap) {
                out.reason_mask |= LiveOrderPlanReasonCloseQuantityCap;
                quantity = close_cap;
            }
        }
    }

    if (out.exposure_increasing) {
        const std::int64_t value_room = position_value_room<S>(context);
        if (quantity > value_room) {
            out.reason_mask |= LiveOrderPlanReasonPositionValueCap;
            quantity = value_room;
        }
    }
    return std::max<std::int64_t>(0, quantity);
}

template <Side S>
[[nodiscard]] LiveSideOrderActionPlan invalid_plan(
    const LiveSideOrderActionInput& input
) noexcept {
    LiveSideOrderActionPlan out{};
    out.target_price_ticks = input.target_price_ticks;
    out.existing_remaining_lots = input.existing_remaining_lots;
    out.reason_mask = LiveOrderPlanReasonInvalidInput;
    out.action = LiveOrderAction::Invalid;
    return out;
}

}  // namespace

template <Side S>
LiveSideOrderActionPlan compute_live_side_order_action_plan(
    const LiveOrderPlannerContext& context,
    const LiveOrderReplaceConfig& replace,
    const LiveSideOrderActionInput& input
) noexcept {
    static_assert(S == Side::Buy || S == Side::Sell);
    if (!valid_context(context) || !valid_replace(replace) ||
        !valid_side_input(input)) {
        return invalid_plan<S>(input);
    }

    LiveSideOrderActionPlan out{};
    out.target_price_ticks = input.target_price_ticks;
    out.existing_remaining_lots = input.existing_remaining_lots;
    out.order_active = order_is_active(input.order_state);
    out.order_pending = order_is_pending(input.order_state);
    out.exposure_increasing = exposure_increasing<S>(
        context.inventory_lots,
        input.exposure_probe_quantity_lots
    );
    out.inventory_room_lots = inventory_room<S>(context);
    out.position_value_room_lots = position_value_room<S>(context);
    out.can_post_after_inventory = is_buy_v<S>
        ? context.inventory_lots < context.max_inventory_lots
        : context.inventory_lots > -context.max_inventory_lots;

    const bool route_allowed = has_flag(
        input.flags,
        LiveOrderSideInputRouteAllowed
    );
    const bool allow_post = has_flag(input.flags, LiveOrderSideInputAllowPost);
    const bool allow_exposure = has_flag(
        input.flags,
        LiveOrderSideInputAllowExposureIncrease
    );
    out.force_update = has_flag(input.flags, LiveOrderSideInputForceUpdate);
    out.can_post = out.can_post_after_inventory && allow_post &&
        (allow_exposure || !out.exposure_increasing);

    if (!out.can_post_after_inventory) {
        out.reason_mask |= LiveOrderPlanReasonInventoryLimit;
    }
    if (!allow_post) {
        out.reason_mask |= LiveOrderPlanReasonPolicyPost;
    }
    if (!allow_exposure && out.exposure_increasing) {
        out.reason_mask |= LiveOrderPlanReasonPolicyExposure;
    }

    const bool use_provided_needs_update = has_flag(
        input.flags,
        LiveOrderSideInputUseProvidedNeedsUpdate
    );
    if (use_provided_needs_update) {
        out.needs_update = has_flag(
            input.flags,
            LiveOrderSideInputProvidedNeedsUpdate
        );
    } else {
        out.needs_update = true;
        if (out.order_active && input.existing_price_ticks > 0) {
            out.needs_update = price_drift_exceeds(
                input.target_price_ticks,
                input.existing_price_ticks,
                replace.tick_size,
                context.requote_threshold_bps
            );
            if (out.needs_update) {
                out.reason_mask |= LiveOrderPlanReasonPriceDrift;
            }
            if (input.order_ttl_ms > 0.0 &&
                input.order_age_ms >= input.order_ttl_ms) {
                out.needs_update = true;
                out.force_update = true;
                out.reason_mask |= LiveOrderPlanReasonTtl;
            }
        }
    }
    // A caller-supplied force decision always means replace, even when the
    // final transformed price remains within the ordinary drift threshold.
    if (out.force_update) {
        out.needs_update = true;
    }

    out.target_quantity_lots = cap_new_quantity<S>(
        context,
        replace,
        input,
        out
    );

    // Match MakerEngine's second check on an otherwise keepable remaining
    // order.  A mark move or intervening fill can make the live remainder too
    // large for max_position_value even though price drift is small.
    if (out.order_active && input.existing_remaining_lots > 0 &&
        exposure_increasing<S>(
            context.inventory_lots,
            input.existing_remaining_lots
        )) {
        const std::int64_t allowed_existing = std::min(
            input.existing_remaining_lots,
            position_value_room<S>(context)
        );
        if (allowed_existing < input.existing_remaining_lots) {
            out.needs_update = true;
            out.force_update = true;
            out.reason_mask |= LiveOrderPlanReasonExistingPositionValue;
        }
    }

    const bool add_replace_class = sign_classified_as_add<S>(
        context.inventory_lots
    );
    const double min_delta_ticks = add_replace_class
        ? replace.add_min_price_change_ticks
        : replace.reducing_min_price_change_ticks;
    const double min_interval_ms = add_replace_class
        ? replace.add_min_interval_ms
        : replace.reducing_min_interval_ms;
    if (out.needs_update && !out.force_update && out.order_active &&
        (use_provided_needs_update || input.existing_price_ticks > 0)) {
        const double price_delta_ticks = use_provided_needs_update
            ? input.provided_price_delta_ticks
            : std::abs(
                static_cast<double>(input.target_price_ticks) * replace.tick_size -
                static_cast<double>(input.existing_price_ticks) * replace.tick_size
            ) / replace.tick_size;
        const bool throttle_price = min_delta_ticks > 0.0 &&
            price_delta_ticks + 1e-9 < min_delta_ticks;
        const bool throttle_age = min_interval_ms > 0.0 &&
            input.order_age_ms < min_interval_ms;
        if (throttle_price) {
            out.reason_mask |= LiveOrderPlanReasonThrottlePrice;
        }
        if (throttle_age) {
            out.reason_mask |= LiveOrderPlanReasonThrottleAge;
        }
        if (throttle_price || throttle_age) {
            out.needs_update = false;
        }
    }

    const bool quantity_ok =
        out.target_quantity_lots >= context.min_quantity_lots;
    const bool notional_ok = notional_at_least(
        out.target_quantity_lots,
        target_price_for_b0(replace, input),
        replace
    );
    out.filter_valid = quantity_ok && notional_ok;
    if (!quantity_ok) {
        out.reason_mask |= LiveOrderPlanReasonMinQuantity;
    }
    if (!notional_ok) {
        out.reason_mask |= LiveOrderPlanReasonMinNotional;
    }

    if (!route_allowed) {
        out.action = LiveOrderAction::RouteDisabled;
        out.reason_mask |= LiveOrderPlanReasonRouteDisabled;
        out.needs_update = false;
        return out;
    }
    if (!out.can_post) {
        out.action = LiveOrderAction::Pause;
        // Match _update_orders: a policy pause does not by itself cancel an
        // otherwise keepable order.  If the active order already requires an
        // update, the existing cancel path runs before the final PAUSE label.
        // PENDING_CANCEL already owns a cancel and must not request another.
        out.cancel_existing = out.needs_update && out.order_active;
        return out;
    }
    if (!out.needs_update && out.order_active) {
        out.action = LiveOrderAction::Keep;
        return out;
    }
    if (out.needs_update && out.order_pending && has_flag(
            replace.flags,
            LiveOrderReplacePendingCoalesce
        )) {
        out.action = LiveOrderAction::Pending;
        out.reason_mask |= LiveOrderPlanReasonPendingLifecycle;
        return out;
    }
    if (out.needs_update && out.order_active) {
        out.action = LiveOrderAction::CancelFirst;
        out.cancel_existing = true;
        if (!out.force_update && add_replace_class && has_flag(
                replace.flags,
                LiveOrderReplaceCancelFirstExposureIncrease
            )) {
            out.reason_mask |= LiveOrderPlanReasonConfiguredCancelFirst;
        }
        return out;
    }
    if (out.needs_update && !out.filter_valid) {
        out.action = LiveOrderAction::SkipFilter;
        return out;
    }
    if (out.needs_update) {
        out.action = LiveOrderAction::Place;
        return out;
    }

    out.action = LiveOrderAction::None;
    return out;
}

LiveDualOrderActionPlan compute_live_order_action_plan(
    const LiveOrderPlannerContext& context,
    const LiveOrderReplaceConfig& replace,
    const LiveSideOrderActionInput& buy,
    const LiveSideOrderActionInput& sell
) noexcept {
    // These are true compile-time BUY/SELL instantiations.  There is no side
    // branch in either hot specialization.
    return LiveDualOrderActionPlan{
        .buy = compute_live_side_order_action_plan<Side::Buy>(
            context,
            replace,
            buy
        ),
        .sell = compute_live_side_order_action_plan<Side::Sell>(
            context,
            replace,
            sell
        ),
    };
}

template LiveSideOrderActionPlan compute_live_side_order_action_plan<Side::Buy>(
    const LiveOrderPlannerContext&,
    const LiveOrderReplaceConfig&,
    const LiveSideOrderActionInput&
) noexcept;
template LiveSideOrderActionPlan compute_live_side_order_action_plan<Side::Sell>(
    const LiveOrderPlannerContext&,
    const LiveOrderReplaceConfig&,
    const LiveSideOrderActionInput&
) noexcept;

}  // namespace narrowgate_cpp
