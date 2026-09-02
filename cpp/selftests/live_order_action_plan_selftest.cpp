#include <cassert>
#include <cstdint>

#include "narrowgate_cpp/live_order_action_plan.hpp"

using namespace narrowgate_cpp;

namespace {

LiveOrderPlannerContext context(std::int64_t inventory_lots = 0) {
    return LiveOrderPlannerContext{
        .inventory_lots = inventory_lots,
        .max_inventory_lots = 26,
        .max_position_value_quote_atoms = 120'000'000'000LL,
        .mid_notional_quote_atoms_per_lot = 6'000'000'000LL,
        .quote_atoms_per_price_tick_lot = 10'000,
        .min_quantity_lots = 1,
        .min_notional_quote_atoms = 500'000'000,
        .requote_threshold_bps = 0.1,
    };
}

LiveSideOrderActionInput side(
    LivePlannerOrderState state = LivePlannerOrderState::Empty
) {
    return LiveSideOrderActionInput{
        .target_price_ticks = 600'000,
        .desired_quantity_lots = 1,
        .exposure_probe_quantity_lots = 1,
        .existing_price_ticks = state == LivePlannerOrderState::Empty
            ? 0
            : 600'000,
        .existing_remaining_lots = state == LivePlannerOrderState::Empty
            ? 0
            : 1,
        .order_age_ms = 1'000.0,
        .order_ttl_ms = 0.0,
        .order_state = state,
        .flags = LiveOrderSideInputRouteAllowed |
            LiveOrderSideInputAllowPost |
            LiveOrderSideInputAllowExposureIncrease,
    };
}

}  // namespace

int main() {
    LiveOrderReplaceConfig replace{};
    replace.tick_size = 0.1;
    replace.lot_size = 0.001;
    replace.min_notional = 5.0;
    replace.flags = LiveOrderReplacePendingCoalesce;

    {
        const auto result = compute_live_order_action_plan(
            context(),
            replace,
            side(),
            side()
        );
        assert(result.buy.action == LiveOrderAction::Place);
        assert(result.sell.action == LiveOrderAction::Place);
        assert(result.buy.exposure_increasing);
        assert(result.sell.exposure_increasing);
        assert(result.buy.target_quantity_lots == 1);
        assert(result.sell.target_quantity_lots == 1);
    }

    {
        auto buy = side(LivePlannerOrderState::Active);
        auto sell = side(LivePlannerOrderState::Active);
        const auto result = compute_live_order_action_plan(
            context(),
            replace,
            buy,
            sell
        );
        assert(result.buy.action == LiveOrderAction::Keep);
        assert(result.sell.action == LiveOrderAction::Keep);
    }

    {
        auto buy = side(LivePlannerOrderState::Active);
        buy.target_price_ticks += 10;
        const auto result = compute_live_order_action_plan(
            context(),
            replace,
            buy,
            side()
        );
        assert(result.buy.action == LiveOrderAction::CancelFirst);
        assert(result.buy.cancel_existing);
        assert(result.buy.needs_update);
        assert(result.buy.reason_mask & LiveOrderPlanReasonPriceDrift);
    }

    {
        auto buy = side(LivePlannerOrderState::Active);
        buy.order_ttl_ms = 1'000.0;
        const auto result = compute_live_order_action_plan(
            context(),
            replace,
            buy,
            side()
        );
        assert(result.buy.action == LiveOrderAction::CancelFirst);
        assert(result.buy.force_update);
        assert(result.buy.reason_mask & LiveOrderPlanReasonTtl);
    }

    {
        auto buy = side(LivePlannerOrderState::PendingCancel);
        const auto result = compute_live_order_action_plan(
            context(),
            replace,
            buy,
            side()
        );
        assert(result.buy.action == LiveOrderAction::Pending);
        assert(result.buy.order_pending);
        assert(!result.buy.order_active);
    }

    {
        auto buy = side();
        buy.flags &= ~LiveOrderSideInputAllowExposureIncrease;
        const auto result = compute_live_order_action_plan(
            context(),
            replace,
            buy,
            side()
        );
        assert(result.buy.action == LiveOrderAction::Pause);
        assert(!result.buy.can_post);
    }

    {
        const auto result = compute_live_order_action_plan(
            context(26),
            replace,
            side(LivePlannerOrderState::Active),
            side()
        );
        assert(result.buy.action == LiveOrderAction::Pause);
        assert(result.buy.cancel_existing);
        assert(result.sell.action == LiveOrderAction::Place);
        assert(!result.sell.exposure_increasing);
    }

    {
        auto buy = side();
        buy.desired_quantity_lots = 2;
        buy.exposure_probe_quantity_lots = 2;
        const auto result = compute_live_order_action_plan(
            context(-2),
            replace,
            buy,
            side()
        );
        assert(!result.buy.exposure_increasing);
        assert(result.buy.target_quantity_lots == 2);
    }

    {
        auto buy = side();
        buy.desired_quantity_lots = 3;
        buy.exposure_probe_quantity_lots = 3;
        buy.flags &= ~LiveOrderSideInputAllowExposureIncrease;
        const auto result = compute_live_order_action_plan(
            context(-2),
            replace,
            buy,
            side()
        );
        assert(result.buy.exposure_increasing);
        assert(result.buy.action == LiveOrderAction::Pause);
    }

    {
        auto limited = context();
        limited.max_position_value_quote_atoms = 3'000'000'000LL;
        auto buy = side();
        const auto result = compute_live_order_action_plan(
            limited,
            replace,
            buy,
            side()
        );
        assert(result.buy.target_quantity_lots == 0);
        assert(result.buy.action == LiveOrderAction::SkipFilter);
        assert(result.buy.reason_mask & LiveOrderPlanReasonPositionValueCap);
        assert(result.buy.reason_mask & LiveOrderPlanReasonMinQuantity);
    }

    {
        auto limited = context();
        limited.max_position_value_quote_atoms = 60'000'000'000LL;
        auto buy = side(LivePlannerOrderState::Active);
        buy.existing_remaining_lots = 2;
        const auto result = compute_live_order_action_plan(
            limited,
            replace,
            buy,
            side()
        );
        assert(result.buy.action == LiveOrderAction::Keep);

        limited.inventory_lots = 10;
        const auto forced = compute_live_order_action_plan(
            limited,
            replace,
            buy,
            side()
        );
        assert(forced.buy.action == LiveOrderAction::CancelFirst);
        assert(forced.buy.force_update);
        assert(
            forced.buy.reason_mask &
            LiveOrderPlanReasonExistingPositionValue
        );
    }

    {
        auto throttled_replace = replace;
        throttled_replace.add_min_interval_ms = 2'000.0;
        auto buy = side(LivePlannerOrderState::Active);
        buy.target_price_ticks += 10;
        const auto result = compute_live_order_action_plan(
            context(),
            throttled_replace,
            buy,
            side()
        );
        assert(result.buy.action == LiveOrderAction::Keep);
        assert(!result.buy.needs_update);
        assert(result.buy.reason_mask & LiveOrderPlanReasonThrottleAge);
    }

    {
        auto invalid = context();
        invalid.mid_notional_quote_atoms_per_lot = 0;
        const auto result = compute_live_order_action_plan(
            invalid,
            replace,
            side(),
            side()
        );
        assert(result.buy.action == LiveOrderAction::Invalid);
        assert(result.sell.action == LiveOrderAction::Invalid);
    }
}
