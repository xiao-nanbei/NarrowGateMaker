#include "live_runtime_core.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <initializer_list>
#include <span>
#include <stdexcept>
#include <utility>

namespace narrowgate_cpp {
namespace {

[[nodiscard]] bool all_finite(
    std::initializer_list<double> values
) noexcept {
    for (const double value : values) {
        if (!std::isfinite(value)) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool in_unit_interval(double value) noexcept {
    return std::isfinite(value) && value >= 0.0 && value <= 1.0;
}

[[nodiscard]] bool aligned_to_increment(
    double value,
    double increment
) noexcept {
    if (!std::isfinite(value) || !std::isfinite(increment) ||
        value < 0.0 || increment <= 0.0) {
        return false;
    }
    const double aligned = std::round(value / increment) * increment;
    const double tolerance = std::max(
        increment * 1e-9,
        64.0 * std::numeric_limits<double>::epsilon() *
            std::max({1.0, std::abs(value), increment})
    );
    return std::abs(value - aligned) <= tolerance;
}

[[nodiscard]] bool valid_side_quote_context(
    const SideQuoteContext& value
) noexcept {
    return all_finite({
        value.raw_price,
        value.pre_guard_price,
        value.final_price,
        value.raw_quote_delta_to_bbo,
        value.pre_guard_delta_to_bbo,
        value.final_quote_delta_to_bbo,
        value.raw_distance_to_mid,
        value.final_distance_to_mid,
        value.final_pair_spread,
        value.final_quote_skew,
        value.spread_mult,
        value.defense_spread_mult,
        value.near_depth_total,
        value.order_ttl_ms,
        value.local_extreme_rank,
        value.local_extreme_low,
        value.local_extreme_high,
        value.local_extreme_window_s,
        value.local_extreme_spread_mult,
        value.l2_quote_flip_rate,
        value.l2_book_refresh_ratio,
        value.l2_book_cancel_ratio,
        value.l2_near_depth_total,
        value.buy_fill_selection_live_score,
    }) && value.final_price > 0.0 &&
        value.final_pair_spread > 0.0 && value.spread_mult >= 1.0 &&
        value.defense_spread_mult >= 1.0 &&
        value.order_ttl_ms >= 0.0 && value.local_extreme_window_s >= 0.0 &&
        value.local_extreme_spread_mult >= 0.0 &&
        value.l2_quote_flip_rate >= 0.0 &&
        value.l2_book_refresh_ratio >= 0.0 &&
        value.l2_book_cancel_ratio >= 0.0 &&
        value.l2_near_depth_total >= 0.0;
}

[[nodiscard]] std::uint8_t bounded_live_depth_levels(int levels) noexcept {
    return static_cast<std::uint8_t>(std::clamp(
        levels,
        1,
        static_cast<int>(kLiveDepthLevels)
    ));
}

[[nodiscard]] std::uint8_t required_depth_prefix_levels(
    const QuoteCoreConfig& config
) noexcept {
    // trace near-depth and trace imbalance are always evaluated.  Preserve
    // their slightly different legacy handling of a non-positive cutoff:
    // near-depth defaults to ten while imbalance clamps to one.
    std::uint8_t required = bounded_live_depth_levels(
        config.trace_book_imb_levels > 0
            ? config.trace_book_imb_levels
            : 10
    );
    required = std::max(
        required,
        bounded_live_depth_levels(config.trace_book_imb_levels)
    );
    if (config.use_depth_microprice) {
        required = std::max(
            required,
            bounded_live_depth_levels(config.microprice_levels)
        );
    }
    if (config.use_depth_kappa) {
        required = std::max(
            required,
            bounded_live_depth_levels(config.kappa_levels)
        );
    }
    if (config.depth_tox_enabled) {
        required = std::max(
            required,
            bounded_live_depth_levels(config.depth_tox_levels)
        );
        required = std::max(required, std::uint8_t{3});
    }
    if (config.book_imb_strength > 0.0) {
        required = std::max(
            required,
            bounded_live_depth_levels(config.book_imb_levels)
        );
    }
    return required;
}

[[nodiscard]] std::size_t build_quantity_prefix(
    const Depth20SideSnapshot& source,
    double lot_size,
    std::uint8_t requested_levels,
    std::array<double, kLiveDepthLevels>& destination
) noexcept {
    const std::size_t size = std::min(
        static_cast<std::size_t>(source.size),
        static_cast<std::size_t>(requested_levels)
    );
    double sum = 0.0;
    for (std::size_t index = 0; index < size; ++index) {
        // This exact expression and forward order are part of B0.  Do not
        // replace it with an integer sum, reassociation or SIMD reduction.
        sum += static_cast<double>(source.quantity_lots[index]) * lot_size;
        destination[index] = sum;
    }
    return size;
}

[[nodiscard]] DepthSideView direct_depth_side_view(
    const Depth20SideSnapshot& source,
    double tick_size,
    double lot_size,
    const std::array<double, kLiveDepthLevels>& quantity_prefix,
    std::size_t prefix_size
) noexcept {
    const std::size_t size = static_cast<std::size_t>(source.size);
    DepthSideView view{};
    view.validated_unique_sorted = true;
    view.price_ticks = ArrayView<std::int64_t>{
        source.price_ticks.data(),
        size,
    };
    view.quantity_lots = ArrayView<std::int64_t>{
        source.quantity_lots.data(),
        size,
    };
    view.quantity_prefix = ArrayView<double>{
        quantity_prefix.data(),
        prefix_size,
    };
    view.tick_size = tick_size;
    view.lot_size = lot_size;
    view.cached_best_price = size > 0
        ? static_cast<double>(source.price_ticks[0]) * tick_size
        : 0.0;
    return view;
}

[[nodiscard]] DepthView direct_depth_view(
    const LiveDepth20BookSnapshot& book,
    double tick_size,
    double lot_size,
    const std::array<double, kLiveDepthLevels>& bid_quantity_prefix,
    std::size_t bid_prefix_size,
    const std::array<double, kLiveDepthLevels>& ask_quantity_prefix,
    std::size_t ask_prefix_size
) noexcept {
    return DepthView{
        direct_depth_side_view(
            book.bids,
            tick_size,
            lot_size,
            bid_quantity_prefix,
            bid_prefix_size
        ),
        direct_depth_side_view(
            book.asks,
            tick_size,
            lot_size,
            ask_quantity_prefix,
            ask_prefix_size
        ),
    };
}

}  // namespace

NativeLiveRuntimeCore::NativeLiveRuntimeCore(QuoteCoreConfig config)
    : market_writer_(market_.claim_writer()),
      config_(std::move(config)),
      quote_hot_plan_(make_quote_hot_plan(config_)),
      depth_prefix_levels_(required_depth_prefix_levels(config_)) {
    if (!finite_positive(config_.tick_size) ||
        !finite_positive(config_.lot_size) ||
        !finite_positive(config_.order_size) ||
        !finite_positive(config_.max_inventory)) {
        throw std::invalid_argument(
            "native live runtime requires positive tick, lot, order size and "
            "max inventory"
        );
    }
}

NativeQuotePolicyStage::NativeQuotePolicyStage(QuoteCoreConfig config)
    : config_(std::move(config)),
      quote_hot_plan_(make_quote_hot_plan(config_)) {}

template <LivePolicySide SideValue>
CommonSidePolicyResultPod NativeQuotePolicyStage::finish_policy(
    CommonSidePolicyInputPod policy,
    const QuoteState& state,
    const QuotePrediction& prediction,
    const QuoteCoreResult& quote
) const noexcept {
    static_assert(
        SideValue == LivePolicySide::Buy || SideValue == LivePolicySide::Sell
    );
    const SideQuoteContext& side = SideValue == LivePolicySide::Buy
        ? quote.buy
        : quote.sell;
    policy.inventory_ratio = std::min(
        std::abs(state.inventory) / std::max(config_.max_inventory, 1e-12),
        1.0
    );
    policy.toxicity = SideValue == LivePolicySide::Buy
        ? prediction.tox_bid
        : prediction.tox_ask;
    policy.markout_ema = SideValue == LivePolicySide::Buy
        ? state.mo_ema_bid
        : state.mo_ema_ask;
    policy.markout_spread_scale = config_.markout_spread_scale;
    policy.markout_reference = state.mo_ref;
    policy.microprice_shift_bps = quote.microprice_shift_bps;
    policy.kappa_depth_baseline = config_.kappa_depth_baseline;
    policy.side_adverse = side.side_adverse;
    policy.side_adverse_pause = side.side_adverse_pause;
    policy.defense_guard = side.defense_guard;
    policy.defense_spread_mult = side.defense_spread_mult;
    policy.defense_pause = side.defense_pause;
    return evaluate_common_side_policy(policy);
}

NativeQuotePolicyStageResult NativeQuotePolicyStage::compute(
    const QuoteState& state,
    const QuotePrediction& prediction,
    const DepthView& depth,
    CommonSidePolicyInputPod buy_policy,
    CommonSidePolicyInputPod sell_policy
) const {
    NativeQuotePolicyStageResult output;
    output.quote = compute_quote_core(
        state, config_, prediction, depth, quote_hot_plan_
    );
    output.buy_policy = finish_policy<LivePolicySide::Buy>(
        buy_policy, state, prediction, output.quote
    );
    output.sell_policy = finish_policy<LivePolicySide::Sell>(
        sell_policy, state, prediction, output.quote
    );
    return output;
}

template CommonSidePolicyResultPod
NativeQuotePolicyStage::finish_policy<LivePolicySide::Buy>(
    CommonSidePolicyInputPod,
    const QuoteState&,
    const QuotePrediction&,
    const QuoteCoreResult&
) const noexcept;
template CommonSidePolicyResultPod
NativeQuotePolicyStage::finish_policy<LivePolicySide::Sell>(
    CommonSidePolicyInputPod,
    const QuoteState&,
    const QuotePrediction&,
    const QuoteCoreResult&
) const noexcept;

MarketStateUpdateStatus NativeLiveRuntimeCore::publish_book(
    const Depth20SideUpdate& bids,
    const Depth20SideUpdate& asks
) noexcept {
    const std::uint64_t fault_epoch_before = feed_fault_epoch_.load(
        std::memory_order_acquire
    );
    const std::uint64_t publication_before = market_.publication_sequence();
    const MarketStateUpdateStatus status = market_writer_.replace_book(
        bids,
        asks
    );
    if (status != MarketStateUpdateStatus::Applied) {
        feed_fault_epoch_.fetch_add(1, std::memory_order_acq_rel);
        return status;
    }

    // An empty or partially invalidated book is a legitimate market-state
    // publication, but it cannot prove recovery from a rejected feed event.
    // Clear only after this exact call published a fresh, complete pair.
    const LiveDepth20BookSnapshot book = market_.read_book();
    const bool complete_fresh_publication =
        bids.size > 0 && asks.size > 0 && book.valid() &&
        book.publication_sequence > publication_before &&
        book.bids.clock.generation == bids.clock.generation &&
        book.asks.clock.generation == asks.clock.generation &&
        book.bids.clock.visible_ts_ns == bids.clock.visible_ts_ns &&
        book.asks.clock.visible_ts_ns == asks.clock.visible_ts_ns;
    const std::uint64_t fault_epoch_after = feed_fault_epoch_.load(
        std::memory_order_acquire
    );
    if (complete_fresh_publication &&
        fault_epoch_after == fault_epoch_before) {
        feed_resync_epoch_.store(fault_epoch_after, std::memory_order_release);
    }
    return status;
}

bool NativeLiveRuntimeCore::finite_non_negative(double value) noexcept {
    return std::isfinite(value) && value >= 0.0;
}

bool NativeLiveRuntimeCore::finite_positive(double value) noexcept {
    return std::isfinite(value) && value > 0.0;
}

bool NativeLiveRuntimeCore::feed_fault_latched() const noexcept {
    const std::uint64_t admitted = feed_resync_epoch_.load(
        std::memory_order_acquire
    );
    const std::uint64_t fault = feed_fault_epoch_.load(
        std::memory_order_acquire
    );
    return admitted != fault;
}

bool NativeLiveRuntimeCore::valid_quote_state(
    const QuoteState& value
) noexcept {
    if (!all_finite({
            value.mid,
            value.inventory,
            value.sigma_sq,
            value.trade_intensity,
            value.best_bid,
            value.best_ask,
            value.mo_ema_all,
            value.mo_ema_bid,
            value.mo_ema_ask,
            value.mo_ref,
            value.hold_time_s,
            value.unrealized_pnl,
        }) ||
        value.mid <= 0.0 || value.sigma_sq < 0.0 ||
        value.trade_intensity < 0.0 || value.best_bid < 0.0 ||
        value.best_ask < 0.0 || value.mo_ref <= 0.0 ||
        value.hold_time_s < 0.0) {
        return false;
    }
    const bool has_no_input_bbo = value.best_bid == 0.0 &&
        value.best_ask == 0.0;
    const bool has_valid_input_bbo = value.best_bid > 0.0 &&
        value.best_ask > value.best_bid;
    return has_no_input_bbo || has_valid_input_bbo;
}

bool NativeLiveRuntimeCore::valid_prediction(
    const QuotePrediction& value
) noexcept {
    return in_unit_interval(value.dir_10s) &&
        finite_non_negative(value.vol_10s) &&
        std::isfinite(value.ret_10s) &&
        in_unit_interval(value.tox_bid) &&
        in_unit_interval(value.tox_ask);
}

bool NativeLiveRuntimeCore::valid_policy_input(
    const CommonSidePolicyInputPod& value
) noexcept {
    return in_unit_interval(value.inventory_ratio) &&
        finite_non_negative(value.depth_age_s) &&
        finite_non_negative(value.max_book_age_s) &&
        in_unit_interval(value.toxicity) &&
        std::isfinite(value.markout_ema) &&
        finite_non_negative(value.markout_spread_scale) &&
        finite_positive(value.markout_reference) &&
        std::isfinite(value.microprice_shift_bps) &&
        in_unit_interval(value.l2_quote_flip_rate) &&
        in_unit_interval(value.l2_book_cancel_ratio) &&
        finite_non_negative(value.l2_near_depth_total) &&
        finite_non_negative(value.thin_depth_threshold) &&
        finite_positive(value.kappa_depth_baseline) &&
        finite_positive(value.local_extreme_spread_mult) &&
        finite_positive(value.defense_spread_mult);
}

bool NativeLiveRuntimeCore::valid_quote_output(
    const QuoteCoreResult& value
) const noexcept {
    if (!all_finite({
            value.bid_price,
            value.ask_price,
            value.spread,
            value.raw_half_spread,
            value.capped_half_spread,
            value.raw_mid_shift,
            value.fair,
            value.cap_bps,
            value.max_spread,
            value.reservation_price,
            value.sigma_sq_raw,
            value.sigma_sq_blended,
            value.delta_raw,
            value.delta_after_regime,
            value.delta_pre_cap,
            value.delta_after_cap,
            value.final_cap_excess,
            value.half_d,
            value.asym,
            value.raw_reservation_shift,
            value.raw_asym_shift,
            value.raw_quote_skew,
            value.book_imb,
            value.microprice_shift_bps,
            value.near_depth_total,
            value.kappa_before_depth,
            value.kappa_used,
            value.depth_tox_mult,
        }) ||
        !finite_positive(value.bid_price) ||
        !finite_positive(value.ask_price) || value.bid_price >= value.ask_price ||
        !finite_positive(value.spread) || value.sigma_sq_raw < 0.0 ||
        value.sigma_sq_blended <= 0.0 || value.max_spread < 0.0 ||
        !valid_side_quote_context(value.buy) ||
        !valid_side_quote_context(value.sell)) {
        return false;
    }
    return aligned_to_increment(value.bid_price, config_.tick_size) &&
        aligned_to_increment(value.ask_price, config_.tick_size);
}

bool NativeLiveRuntimeCore::valid_policy_output(
    const CommonSidePolicyResultPod& value
) noexcept {
    return finite_positive(value.spread_mult) && value.spread_mult >= 1.0 &&
        in_unit_interval(value.size_mult);
}

bool NativeLiveRuntimeCore::valid_routing_output(
    const LiveRoutingResult& value,
    const LiveDepth20BookSnapshot& book
) const noexcept {
    const double best_bid = static_cast<double>(book.bbo.bid_price_ticks) *
        config_.tick_size;
    const double best_ask = static_cast<double>(book.bbo.ask_price_ticks) *
        config_.tick_size;
    return all_finite({
               value.bid_price,
               value.ask_price,
               value.bid_size,
               value.ask_size,
               best_bid,
               best_ask,
           }) &&
        finite_positive(value.bid_price) && finite_positive(value.ask_price) &&
        value.bid_price < value.ask_price && value.bid_price < best_ask &&
        value.ask_price > best_bid && finite_non_negative(value.bid_size) &&
        finite_non_negative(value.ask_size) &&
        aligned_to_increment(value.bid_price, config_.tick_size) &&
        aligned_to_increment(value.ask_price, config_.tick_size) &&
        aligned_to_increment(value.bid_size, config_.lot_size) &&
        aligned_to_increment(value.ask_size, config_.lot_size);
}

template <LivePolicySide SideValue>
CommonSidePolicyResultPod NativeLiveRuntimeCore::evaluate_policy(
    const NativeLiveDecisionInput& input,
    const QuoteCoreResult& quote,
    double depth_age_s
) const noexcept {
    static_assert(
        SideValue == LivePolicySide::Buy || SideValue == LivePolicySide::Sell
    );
    const auto& quote_side = [&]() -> const SideQuoteContext& {
        if constexpr (SideValue == LivePolicySide::Buy) {
            return quote.buy;
        } else {
            return quote.sell;
        }
    }();
    CommonSidePolicyInputPod policy = [&]() {
        if constexpr (SideValue == LivePolicySide::Buy) {
            return input.buy_policy;
        } else {
            return input.sell_policy;
        }
    }();
    // Preserve the current live common-policy contract exactly: the caller
    // supplies the sign-based exposure classification used by B0.  The
    // stricter templated cross-zero classifier is available for a separately
    // replayed policy migration, not silently activated by this performance
    // refactor.
    policy.inventory_ratio = std::min(
        std::abs(input.quote_state.inventory) /
            std::max(config_.max_inventory, 1e-12),
        1.0
    );
    policy.depth_age_s = depth_age_s;
    policy.toxicity = SideValue == LivePolicySide::Buy
        ? input.prediction.tox_bid
        : input.prediction.tox_ask;
    policy.markout_ema = SideValue == LivePolicySide::Buy
        ? input.quote_state.mo_ema_bid
        : input.quote_state.mo_ema_ask;
    policy.markout_spread_scale = config_.markout_spread_scale;
    policy.markout_reference = input.quote_state.mo_ref;
    policy.microprice_shift_bps = quote.microprice_shift_bps;
    policy.kappa_depth_baseline = config_.kappa_depth_baseline;
    policy.side_adverse = quote_side.side_adverse;
    policy.side_adverse_pause = quote_side.side_adverse_pause;
    policy.defense_guard = quote_side.defense_guard;
    policy.defense_spread_mult = quote_side.defense_spread_mult;
    policy.defense_pause = quote_side.defense_pause;
    return evaluate_common_side_policy(policy);
}

NativeLiveDecisionResult NativeLiveRuntimeCore::decide(
    const NativeLiveDecisionInput& input
) {
    NativeLiveDecisionResult output;
    DecisionLease lease(decision_busy_);
    if (!lease.acquired()) {
        output.status = NativeLiveDecisionStatus::Busy;
        return output;
    }
    if (input.decision_ts_ns == 0 ||
        input.expected_market_publication_sequence == 0 ||
        input.expected_bid_generation == 0 ||
        input.expected_ask_generation == 0 ||
        !valid_quote_state(input.quote_state) ||
        !valid_prediction(input.prediction) ||
        !valid_policy_input(input.buy_policy) ||
        !valid_policy_input(input.sell_policy) ||
        !finite_non_negative(input.min_qty) ||
        !finite_non_negative(input.min_notional) ||
        !finite_non_negative(input.size_eta) ||
        !finite_non_negative(input.requote_threshold_bps) ||
        !finite_non_negative(input.routing_max_spread) ||
        !finite_non_negative(input.bid_active_price) ||
        !finite_non_negative(input.bid_age_ms) ||
        !finite_non_negative(input.ask_active_price) ||
        !finite_non_negative(input.ask_age_ms) ||
        !finite_non_negative(input.bid_order_ttl_ms) ||
        !finite_non_negative(input.ask_order_ttl_ms) ||
        (input.bid_active && !finite_positive(input.bid_active_price)) ||
        (input.ask_active && !finite_positive(input.ask_active_price)) ||
        (input.bid_active &&
         !aligned_to_increment(input.bid_active_price, config_.tick_size)) ||
        (input.ask_active &&
         !aligned_to_increment(input.ask_active_price, config_.tick_size))) {
        output.status = NativeLiveDecisionStatus::InvalidInput;
        return output;
    }

    if (feed_fault_latched()) {
        output.status = NativeLiveDecisionStatus::FeedFault;
        return output;
    }

    const auto previous_decision = last_decision_ts_ns_.load(
        std::memory_order_relaxed
    );
    if (input.decision_ts_ns < previous_decision) {
        output.status = NativeLiveDecisionStatus::DecisionClockRegressed;
        return output;
    }

    const auto book = market_.read_book_prefix(depth_prefix_levels_);
    output.market_publication_sequence = book.publication_sequence;
    if (!book.valid()) {
        output.status = NativeLiveDecisionStatus::NoBook;
        return output;
    }
    if (book.publication_sequence !=
            input.expected_market_publication_sequence ||
        book.bbo.bid_generation != input.expected_bid_generation ||
        book.bbo.ask_generation != input.expected_ask_generation) {
        output.status = NativeLiveDecisionStatus::MarketIdentityMismatch;
        return output;
    }
    const auto oldest_visible_ts_ns = std::min(
        book.bbo.bid_visible_ts_ns,
        book.bbo.ask_visible_ts_ns
    );
    if (oldest_visible_ts_ns == 0 ||
        input.decision_ts_ns < oldest_visible_ts_ns) {
        output.status = NativeLiveDecisionStatus::DecisionClockRegressed;
        return output;
    }
    output.book_age_ns = input.decision_ts_ns - oldest_visible_ts_ns;
    if (input.max_book_age_ns > 0 &&
        output.book_age_ns >= input.max_book_age_ns) {
        output.status = NativeLiveDecisionStatus::StaleBook;
        return output;
    }

    // Only the prefix span returned below is exposed.  Avoid clearing unused
    // tail entries on every decision when the configured cutoff is shallow.
    std::array<double, kLiveDepthLevels> bid_quantity_prefix;
    std::array<double, kLiveDepthLevels> ask_quantity_prefix;
    const std::size_t bid_prefix_size = build_quantity_prefix(
        book.bids,
        config_.lot_size,
        depth_prefix_levels_,
        bid_quantity_prefix
    );
    const std::size_t ask_prefix_size = build_quantity_prefix(
        book.asks,
        config_.lot_size,
        depth_prefix_levels_,
        ask_quantity_prefix
    );

    QuoteState quote_state = input.quote_state;
    quote_state.best_bid = static_cast<double>(book.bbo.bid_price_ticks) *
        config_.tick_size;
    quote_state.best_ask = static_cast<double>(book.bbo.ask_price_ticks) *
        config_.tick_size;
    try {
        output.quote = compute_quote_core(
            quote_state,
            config_,
            input.prediction,
            direct_depth_view(
                book,
                config_.tick_size,
                config_.lot_size,
                bid_quantity_prefix,
                bid_prefix_size,
                ask_quantity_prefix,
                ask_prefix_size
            ),
            quote_hot_plan_
        );
    } catch (...) {
        output.status = NativeLiveDecisionStatus::InvalidOutput;
        return output;
    }
    if (!valid_quote_output(output.quote)) {
        output.status = NativeLiveDecisionStatus::InvalidOutput;
        return output;
    }

    const double depth_age_s = static_cast<double>(output.book_age_ns) / 1e9;
    output.buy_policy = evaluate_policy<LivePolicySide::Buy>(
        input,
        output.quote,
        depth_age_s
    );
    output.sell_policy = evaluate_policy<LivePolicySide::Sell>(
        input,
        output.quote,
        depth_age_s
    );
    if (!valid_policy_output(output.buy_policy) ||
        !valid_policy_output(output.sell_policy)) {
        output.status = NativeLiveDecisionStatus::InvalidOutput;
        return output;
    }

    // Only the compress mode is allowed to alter final routed prices.  In
    // pause_exposure and observe modes quote.max_spread remains diagnostic or
    // an exposure gate, exactly as in the Python B0 path.
    const double max_spread = config_.spread_cap_mode == 0
        ? (input.routing_max_spread > 0.0
               ? input.routing_max_spread
               : output.quote.max_spread)
        : 0.0;
    LiveRoutingInput routing{
        .mid = quote_state.mid,
        .inventory = quote_state.inventory,
        .base_bid_price = output.quote.bid_price,
        .base_ask_price = output.quote.ask_price,
        .best_bid = quote_state.best_bid,
        .best_ask = quote_state.best_ask,
        .tick_size = config_.tick_size,
        .lot_size = config_.lot_size,
        .min_qty = input.min_qty,
        .min_notional = input.min_notional,
        .order_size = config_.order_size,
        .max_inventory = config_.max_inventory,
        .eta = input.size_eta,
        .requote_threshold_bps = input.requote_threshold_bps,
        .max_spread = max_spread,
        .bid_active_price = input.bid_active_price,
        .bid_age_ms = input.bid_age_ms,
        .ask_active_price = input.ask_active_price,
        .ask_age_ms = input.ask_age_ms,
        .symmetric_size = input.symmetric_size,
        .bid_active = input.bid_active,
        .ask_active = input.ask_active,
    };
    output.routing = compute_live_routing_decision(
        routing,
        LiveRoutingPolicy{
            output.buy_policy.allow_post,
            output.buy_policy.allow_exposure_increase,
            output.buy_policy.spread_mult,
            output.buy_policy.size_mult,
            input.bid_order_ttl_ms,
        },
        LiveRoutingPolicy{
            output.sell_policy.allow_post,
            output.sell_policy.allow_exposure_increase,
            output.sell_policy.spread_mult,
            output.sell_policy.size_mult,
            input.ask_order_ttl_ms,
        }
    );
    if (!valid_routing_output(output.routing, book)) {
        output.status = NativeLiveDecisionStatus::InvalidOutput;
        return output;
    }

    // A rejected feed update racing with quote computation invalidates the
    // otherwise coherent old-book decision. Leave sequencing untouched so a
    // later explicit resync can retry this decision timestamp.
    if (feed_fault_latched()) {
        output.status = NativeLiveDecisionStatus::FeedFault;
        return output;
    }

    last_decision_ts_ns_.store(input.decision_ts_ns, std::memory_order_relaxed);
    output.decision_sequence = decision_sequence_.fetch_add(
        1,
        std::memory_order_relaxed
    ) + 1;
    output.status = NativeLiveDecisionStatus::Applied;
    return output;
}

std::size_t NativeLiveRuntimeCore::core_size_bytes() noexcept {
    return sizeof(NativeLiveRuntimeCore);
}

std::size_t NativeLiveRuntimeCore::core_alignment_bytes() noexcept {
    return alignof(NativeLiveRuntimeCore);
}

template CommonSidePolicyResultPod
NativeLiveRuntimeCore::evaluate_policy<LivePolicySide::Buy>(
    const NativeLiveDecisionInput&,
    const QuoteCoreResult&,
    double
) const noexcept;
template CommonSidePolicyResultPod
NativeLiveRuntimeCore::evaluate_policy<LivePolicySide::Sell>(
    const NativeLiveDecisionInput&,
    const QuoteCoreResult&,
    double
) const noexcept;

}  // namespace narrowgate_cpp
