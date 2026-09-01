#include "dynamic_fill_hazard.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <utility>

namespace narrowgate_cpp {
namespace {

enum class TerminalPolicyRouteCpp {
    ProspectiveCancelReentry,
    TerminalComplete,
    BaselineResubmit,
    ShutdownNoReentry,
    Unsupported,
};

[[nodiscard]] TerminalPolicyRouteCpp terminal_policy_route(
    const std::string& reason,
    double remaining_after
) {
    if ((std::isfinite(remaining_after) && remaining_after <= 1e-12) ||
        reason == "full_fill" || reason == "filled_before_cancel_ack") {
        return TerminalPolicyRouteCpp::TerminalComplete;
    }
    if (reason == "cancel_ack" || reason == "cancel_ack_reconciled") {
        return TerminalPolicyRouteCpp::ProspectiveCancelReentry;
    }
    if (reason == "expired" || reason == "rejected") {
        return TerminalPolicyRouteCpp::BaselineResubmit;
    }
    if (reason == "administrative_cancel" ||
        reason == "local_shutdown_cancel" || reason == "shutdown") {
        return TerminalPolicyRouteCpp::ShutdownNoReentry;
    }
    return TerminalPolicyRouteCpp::Unsupported;
}

[[nodiscard]] double clamp_value(double value, double lower, double upper) {
    return std::max(lower, std::min(upper, value));
}

[[nodiscard]] std::pair<double, double> predict_head(
    const DynamicFillHazardHead& head,
    const std::array<double, kDynamicFillHazardFeatureCount>& features,
    double exposure_ms
) {
    double eta = head.intercept;
    for (std::size_t i = 0; i < features.size(); ++i) {
        if (!std::isfinite(features[i]) || !std::isfinite(head.feature_mean[i]) ||
            !std::isfinite(head.feature_scale[i]) ||
            !std::isfinite(head.coefficients[i])) {
            throw std::invalid_argument("dynamic fill-hazard feature/model is non-finite");
        }
        const double standardized = clamp_value(
            (features[i] - head.feature_mean[i]) /
                std::max(head.feature_scale[i], 1e-12),
            -12.0,
            12.0
        );
        eta += standardized * head.coefficients[i];
    }
    eta = clamp_value(eta, -25.0, 20.0);
    const double exposure_s = std::max(0.001, exposure_ms / 1000.0);
    const double raw_hazard = std::min(50.0, std::exp(eta) * exposure_s);
    const double raw = -std::expm1(-raw_hazard);
    if (!head.has_calibrator) {
        return {raw, raw};
    }
    const double clipped = clamp_value(
        raw,
        head.probability_clip_lower,
        head.probability_clip_upper
    );
    const double score = std::log(-std::log1p(-clipped));
    const double calibrated_eta = head.calibrator_intercept +
        head.calibrator_slope * score;
    const double cumulative_hazard = std::exp(
        clamp_value(calibrated_eta, -25.0, 20.0)
    );
    const double calibrated = -std::expm1(-std::min(50.0, cumulative_hazard));
    return {raw, calibrated};
}

[[nodiscard]] std::int64_t optional_message_ts_ms(
    std::int64_t transaction_time_ms,
    std::int64_t event_time_ms,
    std::int64_t receive_ts_ns
) {
    if (transaction_time_ms > 0) {
        return transaction_time_ms;
    }
    if (event_time_ms > 0) {
        return event_time_ms;
    }
    return receive_ts_ns > 0 ? receive_ts_ns / 1'000'000 : 0;
}

[[nodiscard]] double safe_log1p(double value) {
    return std::log1p(std::max(0.0, value));
}

}  // namespace

DynamicFillHazardPrediction predict_dynamic_fill_hazard(
    const DynamicFillHazardModel& model,
    const std::array<double, kDynamicFillHazardFeatureCount>& features,
    double exposure_ms
) {
    const auto [favorable_raw, favorable] = predict_head(
        model.favorable, features, exposure_ms
    );
    const auto [adverse_raw, adverse] = predict_head(
        model.adverse, features, exposure_ms
    );
    return DynamicFillHazardPrediction{
        favorable_raw,
        favorable,
        adverse_raw,
        adverse,
        adverse - favorable,
    };
}

NativeExchangeBookSchedulerCpp::NativeExchangeBookSchedulerCpp(
    bool strict_sequence,
    std::int64_t strict_after_ns,
    bool allow_delta_bootstrap
) : strict_sequence_(strict_sequence),
    strict_after_ns_(std::max<std::int64_t>(0, strict_after_ns)),
    allow_delta_bootstrap_(allow_delta_bootstrap) {}

bool NativeExchangeBookSchedulerCpp::strict_at(
    std::int64_t exchange_ts_ns
) const noexcept {
    return strict_sequence_ && exchange_ts_ns >= strict_after_ns_;
}

void NativeExchangeBookSchedulerCpp::reset_book() {
    bids_.clear();
    asks_.clear();
}

void NativeExchangeBookSchedulerCpp::invalidate(bool count_gap) {
    reset_book();
    initialized_ = false;
    bridge_pending_ = false;
    last_update_id_ = -1;
    segment_id_ = 0;
    known_bids_.clear();
    known_asks_.clear();
    bid_snapshot_range_.reset();
    ask_snapshot_range_.reset();
    if (count_gap) {
        ++stats_.sequence_gaps;
    }
}

const NativeExchangeBookSchedulerCpp::BookSide&
NativeExchangeBookSchedulerCpp::side_book(bool is_bid) const noexcept {
    return is_bid ? bids_ : asks_;
}

NativeExchangeBookSchedulerCpp::BookSide&
NativeExchangeBookSchedulerCpp::side_book(bool is_bid) noexcept {
    return is_bid ? bids_ : asks_;
}

const std::unordered_set<std::int64_t>&
NativeExchangeBookSchedulerCpp::known_ticks(bool is_bid) const noexcept {
    return is_bid ? known_bids_ : known_asks_;
}

std::unordered_set<std::int64_t>&
NativeExchangeBookSchedulerCpp::known_ticks(bool is_bid) noexcept {
    return is_bid ? known_bids_ : known_asks_;
}

void NativeExchangeBookSchedulerCpp::apply_level(const NativeBookLevel& level) {
    if (level.price_tick <= 0 || !std::isfinite(level.quantity) ||
        level.quantity < 0.0) {
        throw std::invalid_argument("native book level units are invalid");
    }
    auto& book = side_book(level.is_bid);
    if (level.quantity <= 0.0) {
        book.erase(level.price_tick);
    } else {
        book[level.price_tick] = level.quantity;
    }
}

void NativeExchangeBookSchedulerCpp::rebuild_snapshot_identity() {
    known_bids_.clear();
    known_asks_.clear();
    for (const auto& [tick, quantity] : bids_) {
        if (quantity > 0.0) {
            known_bids_.insert(tick);
        }
    }
    for (const auto& [tick, quantity] : asks_) {
        if (quantity > 0.0) {
            known_asks_.insert(tick);
        }
    }
    bid_snapshot_range_ = bids_.empty()
        ? std::optional<std::pair<std::int64_t, std::int64_t>>{}
        : std::make_optional(std::make_pair(bids_.begin()->first, bids_.rbegin()->first));
    ask_snapshot_range_ = asks_.empty()
        ? std::optional<std::pair<std::int64_t, std::int64_t>>{}
        : std::make_optional(std::make_pair(asks_.begin()->first, asks_.rbegin()->first));
}

bool NativeExchangeBookSchedulerCpp::begin_message(
    NativeBookEventType event_type,
    std::int64_t exchange_ts_ns,
    std::int64_t receive_ts_ns,
    std::int64_t event_time_ms,
    std::int64_t transaction_time_ms,
    std::int64_t first_update_id,
    std::int64_t final_update_id,
    std::int64_t previous_final_update_id,
    std::int64_t last_update_id
) {
    ++stats_.logical_messages;
    const std::int64_t message_ts_ms = optional_message_ts_ms(
        transaction_time_ms, event_time_ms, receive_ts_ns
    );
    if (message_ts_ms > 0 && previous_message_ts_ms_ > 0 &&
        message_ts_ms < previous_message_ts_ms_) {
        ++stats_.message_time_reversals;
    }
    if (message_ts_ms > 0) {
        previous_message_ts_ms_ = message_ts_ms;
    }

    if (event_type == NativeBookEventType::Snapshot) {
        const std::int64_t snapshot_update_id =
            last_update_id >= 0 ? last_update_id : final_update_id;
        if (snapshot_update_id < 0) {
            ++stats_.invalid_sequence_messages;
            invalidate();
            return false;
        }
        const SnapshotKey key{event_time_ms, snapshot_update_id};
        if (last_snapshot_key_ && *last_snapshot_key_ == key) {
            ++stats_.duplicate_messages;
            ++stats_.duplicate_snapshots;
            return false;
        }
        reset_book();
        initialized_ = true;
        bridge_pending_ = true;
        last_update_id_ = snapshot_update_id;
        last_snapshot_key_ = key;
        ++stats_.snapshot_messages;
        return true;
    }

    ++stats_.update_messages;
    if (final_update_id < 0) {
        ++stats_.invalid_sequence_messages;
        invalidate();
        return false;
    }
    if (!initialized_ || last_update_id_ < 0) {
        if (allow_delta_bootstrap_ && previous_final_update_id >= 0) {
            reset_book();
            initialized_ = true;
            bridge_pending_ = false;
            last_update_id_ = previous_final_update_id;
            ++stats_.delta_bootstrap_messages;
        } else {
            ++stats_.ignored_before_snapshot;
            return false;
        }
    }

    if (final_update_id <= last_update_id_) {
        ++stats_.duplicate_messages;
        ++stats_.stale_updates;
        return false;
    }
    if (bridge_pending_) {
        const bool spans_snapshot = first_update_id >= 0 &&
            first_update_id <= last_update_id_ && last_update_id_ <= final_update_id;
        const bool follows_snapshot = previous_final_update_id >= 0 &&
            previous_final_update_id == last_update_id_;
        if (!spans_snapshot && !follows_snapshot) {
            invalidate();
            return false;
        }
        bridge_pending_ = false;
    } else if (previous_final_update_id >= 0) {
        if (previous_final_update_id != last_update_id_) {
            invalidate();
            return false;
        }
    } else if (first_update_id >= 0 && first_update_id > last_update_id_ + 1) {
        invalidate();
        return false;
    }
    last_update_id_ = final_update_id;
    ++stats_.accepted_updates;
    static_cast<void>(exchange_ts_ns);
    return true;
}

NativeBookApplyResult NativeExchangeBookSchedulerCpp::apply_message(
    NativeBookEventType event_type,
    std::int64_t exchange_ts_ns,
    std::int64_t receive_ts_ns,
    std::int64_t event_time_ms,
    std::int64_t transaction_time_ms,
    std::int64_t first_update_id,
    std::int64_t final_update_id,
    std::int64_t previous_final_update_id,
    std::int64_t last_update_id,
    const std::vector<NativeBookLevel>& levels
) {
    if (exchange_ts_ns <= 0) {
        throw std::invalid_argument("native book exchange timestamp must be positive");
    }
    if (exchange_ts_ns < last_source_ts_ns_) {
        ++stats_.message_time_reversals;
        invalidate();
        if (strict_at(exchange_ts_ns)) {
            throw std::runtime_error("native exchange-book time regressed");
        }
        return NativeBookApplyResult{false, false, true, {}};
    }
    last_source_ts_ns_ = exchange_ts_ns;
    last_exchange_ts_ns_ = std::max(last_exchange_ts_ns_, exchange_ts_ns);
    last_receive_ts_ns_ = std::max(last_receive_ts_ns_, receive_ts_ns);

    if (event_type == NativeBookEventType::SourceGap) {
        const bool was_initialized = initialized_;
        invalidate();
        previous_message_ts_ms_ = 0;
        if (strict_at(exchange_ts_ns)) {
            throw std::runtime_error("native exchange-book source gap");
        }
        return NativeBookApplyResult{false, false, was_initialized, {}};
    }
    if (levels.empty()) {
        throw std::invalid_argument("native snapshot/delta message has no levels");
    }

    std::map<std::pair<bool, std::int64_t>, double> before;
    if (event_type == NativeBookEventType::Delta) {
        for (const auto& level : levels) {
            const auto& book = side_book(level.is_bid);
            const auto found = book.find(level.price_tick);
            before[{level.is_bid, level.price_tick}] =
                found == book.end() ? 0.0 : found->second;
        }
    }
    const std::int64_t gaps_before = stats_.sequence_gaps;
    const bool accepted = begin_message(
        event_type,
        exchange_ts_ns,
        receive_ts_ns,
        event_time_ms,
        transaction_time_ms,
        first_update_id,
        final_update_id,
        previous_final_update_id,
        last_update_id
    );
    const bool invalidated = stats_.sequence_gaps > gaps_before;
    if (!accepted) {
        if (invalidated && strict_at(exchange_ts_ns)) {
            throw std::runtime_error("native exchange-book sequence gap");
        }
        return NativeBookApplyResult{false, false, invalidated, {}};
    }

    if (event_type == NativeBookEventType::Delta && segment_id_ == 0) {
        ++segment_count_;
        segment_id_ = segment_count_;
        known_bids_.clear();
        known_asks_.clear();
        bid_snapshot_range_.reset();
        ask_snapshot_range_.reset();
    }
    for (const auto& level : levels) {
        apply_level(level);
    }
    if (event_type == NativeBookEventType::Snapshot) {
        ++segment_count_;
        segment_id_ = segment_count_;
        rebuild_snapshot_identity();
        for (const auto& level : levels) {
            known_ticks(level.is_bid).insert(level.price_tick);
        }
        return NativeBookApplyResult{true, true, false, {}};
    }

    std::map<std::pair<bool, std::int64_t>, double> final_quantities;
    for (const auto& level : levels) {
        known_ticks(level.is_bid).insert(level.price_tick);
        final_quantities[{level.is_bid, level.price_tick}] = level.quantity;
    }
    NativeBookApplyResult result;
    result.accepted = true;
    result.changes.reserve(final_quantities.size());
    for (const auto& [key, after] : final_quantities) {
        const double prior = before[key];
        if (std::abs(prior - after) <= 1e-15) {
            continue;
        }
        result.changes.push_back(NativeBookLevelChange{
            key.first,
            key.second,
            prior,
            after,
            exchange_ts_ns,
            receive_ts_ns,
            final_update_id,
            segment_id_,
        });
    }
    return result;
}

NativeBookLookup NativeExchangeBookSchedulerCpp::lookup(
    bool is_bid,
    std::int64_t price_tick
) const {
    NativeBookLookup value;
    value.asof_exchange_ts_ns = last_exchange_ts_ns_;
    value.segment_id = segment_id_;
    if (!initialized_ || segment_id_ <= 0) {
        return value;
    }
    const auto& book = side_book(is_bid);
    const auto found = book.find(price_tick);
    const auto& range = is_bid ? bid_snapshot_range_ : ask_snapshot_range_;
    const auto& opposite_range = is_bid
        ? ask_snapshot_range_
        : bid_snapshot_range_;
    const bool snapshot_uncrossed =
        bid_snapshot_range_.has_value()
        && ask_snapshot_range_.has_value()
        && bid_snapshot_range_->second < ask_snapshot_range_->first;
    if (range) {
        value.snapshot_range_known = true;
        value.snapshot_min_tick = range->first;
        value.snapshot_max_tick = range->second;
    }
    if (found != book.end() && found->second > 0.0) {
        value.status = "exact";
        value.reason = "visible_quantity";
        value.quantity = found->second;
        value.quantity_known = true;
        return value;
    }
    if (known_ticks(is_bid).contains(price_tick)) {
        value.status = "known_zero";
        value.reason = "explicit_zero_or_removed_level";
        value.quantity_known = true;
        return value;
    }
    if (range && range->first <= price_tick && price_tick <= range->second) {
        value.status = "known_zero";
        value.reason = "inside_snapshot_range_absent";
        value.quantity_known = true;
        return value;
    }
    if (
        snapshot_uncrossed
        && opposite_range
        && (
            (is_bid && price_tick >= opposite_range->first)
            || (!is_bid && price_tick <= opposite_range->second)
        )
    ) {
        value.status = "known_zero";
        value.reason = "opposite_top_structural_zero";
        value.quantity_known = true;
        return value;
    }
    value.reason = "outside_snapshot_range";
    return value;
}

NativeBookTop NativeExchangeBookSchedulerCpp::top() const {
    NativeBookTop value;
    value.last_exchange_ts_ns = last_exchange_ts_ns_;
    value.last_receive_ts_ns = last_receive_ts_ns_;
    value.segment_id = segment_id_;
    if (!initialized_ || bids_.empty() || asks_.empty()) {
        return value;
    }
    const auto bid = bids_.rbegin();
    const auto ask = asks_.begin();
    if (ask->first <= bid->first) {
        return value;
    }
    value.valid = true;
    value.best_bid_tick = bid->first;
    value.best_ask_tick = ask->first;
    value.best_bid_qty = bid->second;
    value.best_ask_qty = ask->second;
    return value;
}

double DynamicFillHazardRuntimeCpp::OrderPath::inferred_cancel_qty() const noexcept {
    return std::max(0.0, decrease_qty - exact_price_trade_qty);
}

std::int64_t DynamicFillHazardRuntimeCpp::OrderPath::inferred_cancel_events() const noexcept {
    return inferred_cancel_qty() > 1e-12 ? decrease_events : 0;
}

double DynamicFillHazardRuntimeCpp::OrderPath::queue_ahead_estimate() const noexcept {
    const double initial = std::max(0.0, initial_visible_qty);
    const double attributed_trade = std::min(decrease_qty, exact_price_trade_qty);
    const double after_trade = std::max(0.0, initial - attributed_trade);
    const double cancellation = inferred_cancel_qty();
    const double lower = std::max(0.0, after_trade - cancellation);
    const double public_before_cancel = std::max(
        0.0, initial - attributed_trade + refill_qty
    );
    const double ahead_share = public_before_cancel > 1e-12
        ? after_trade / public_before_cancel
        : 0.0;
    return std::max(
        lower,
        std::min(after_trade, after_trade - cancellation * ahead_share)
    );
}

void DynamicFillHazardRuntimeCpp::OrderPath::invalidate(
    const std::string& reason
) {
    valid = false;
    if (invalid_reason.empty()) {
        invalid_reason = reason;
    }
}

DynamicFillHazardRuntimeCpp::DynamicFillHazardRuntimeCpp(
    DynamicFillHazardModel model,
    DynamicFillHazardRuntimeConfig config
) : model_(std::move(model)),
    config_(config),
    book_(config.strict_sequence, config.strict_after_ns, false) {
    if (!std::isfinite(config_.tick_size) || config_.tick_size <= 0.0 ||
        !std::isfinite(config_.lot_size) || config_.lot_size <= 0.0 ||
        !std::isfinite(config_.exposure_ms) || config_.exposure_ms <= 0.0 ||
        !std::isfinite(config_.price_jump_ticks) ||
        config_.price_jump_ticks <= 0.0 ||
        !std::isfinite(config_.evaluation_interval_ms) ||
        config_.evaluation_interval_ms <= 0.0 ||
        !std::isfinite(config_.entry_threshold) ||
        config_.entry_threshold <= 0.0) {
        throw std::invalid_argument("dynamic fill-hazard runtime config is invalid");
    }
}

NativeBookApplyResult DynamicFillHazardRuntimeCpp::apply_book_message(
    NativeBookEventType event_type,
    std::int64_t exchange_ts_ns,
    std::int64_t provider_receive_ts_ns,
    std::int64_t feature_ready_ts_ns,
    std::int64_t event_time_ms,
    std::int64_t transaction_time_ms,
    std::int64_t first_update_id,
    std::int64_t final_update_id,
    std::int64_t previous_final_update_id,
    std::int64_t last_update_id,
    const std::vector<NativeBookLevel>& levels,
    bool execution_trade_same_ms
) {
    const std::int64_t visible_ts_ns = feature_ready_ts_ns > 0
        ? feature_ready_ts_ns
        : provider_receive_ts_ns;
    last_provider_receive_ts_ns_ = std::max(
        last_provider_receive_ts_ns_, provider_receive_ts_ns
    );
    NativeBookApplyResult result = book_.apply_message(
        event_type,
        exchange_ts_ns,
        visible_ts_ns,
        event_time_ms,
        transaction_time_ms,
        first_update_id,
        final_update_id,
        previous_final_update_id,
        last_update_id,
        levels
    );
    if (result.snapshot_reset || result.invalidated) {
        const std::string reason = result.invalidated
            ? "native_sequence_invalidated"
            : "native_snapshot_reset";
        for (auto& [id, path] : paths_) {
            static_cast<void>(id);
            path.invalidate(reason);
        }
    }
    for (const auto& change : result.changes) {
        for (auto& [id, path] : paths_) {
            static_cast<void>(id);
            if (change.is_bid != true || path.price_tick != change.price_tick) {
                continue;
            }
            if (path.generation != change.segment_id) {
                path.invalidate("deep_book_generation_changed");
            }
            const bool same_ready_activation =
                path.activation_ts_ns == change.receive_ts_ns;
            if (execution_trade_same_ms || same_ready_activation) {
                path.invalidate("same_ms_exchange_book_ambiguity");
            }
            const double before = std::max(0.0, change.quantity_before);
            const double after = std::max(0.0, change.quantity_after);
            if (after < before - 1e-12) {
                ++path.decrease_events;
                path.decrease_qty += before - after;
            } else if (after > before + 1e-12) {
                ++path.refill_events;
                path.refill_qty += after - before;
            }
            path.current_visible_qty = after;
            path.receive_ts_ns = std::max(
                path.receive_ts_ns, provider_receive_ts_ns
            );
            path.feature_ready_ts_ns = std::max(
                path.feature_ready_ts_ns, change.receive_ts_ns
            );
        }
    }
    return result;
}

NativeBookLookup DynamicFillHazardRuntimeCpp::activate_order(
    const std::string& client_order_id,
    double order_price,
    std::int64_t activation_ts_ns
) {
    if (client_order_id.empty() || !std::isfinite(order_price) ||
        order_price <= 0.0 || activation_ts_ns <= 0) {
        throw std::invalid_argument("dynamic fill-hazard activation is invalid");
    }
    const double scaled = order_price / config_.tick_size;
    const auto price_tick = static_cast<std::int64_t>(std::llround(scaled));
    if (std::abs(order_price - static_cast<double>(price_tick) * config_.tick_size) >
        std::max(1e-9, config_.tick_size * 1e-8)) {
        throw std::invalid_argument("dynamic fill-hazard order price is off tick");
    }
    const NativeBookLookup seed = book_.lookup(true, price_tick);
    const double quantity = seed.quantity_known ? std::max(0.0, seed.quantity) : 0.0;
    paths_[client_order_id] = OrderPath{
        client_order_id,
        order_price,
        price_tick,
        activation_ts_ns,
        seed.segment_id,
        quantity,
        quantity,
        std::max(book_.last_exchange_ts_ns(), last_provider_receive_ts_ns_),
        book_.last_receive_ts_ns(),
        seed.strict_usable(),
        seed.strict_usable() ? std::string{} : seed.reason,
    };
    evaluation_states_.erase(client_order_id);
    return seed;
}

void DynamicFillHazardRuntimeCpp::invalidate_order(
    const std::string& client_order_id,
    const std::string& reason
) {
    const auto found = paths_.find(client_order_id);
    if (found != paths_.end()) {
        found->second.invalidate(
            reason.empty() ? "external_path_invalidation" : reason
        );
    }
}

void DynamicFillHazardRuntimeCpp::drop_inactive(
    const std::vector<std::string>& active_client_order_ids
) {
    std::unordered_set<std::string> active(
        active_client_order_ids.begin(), active_client_order_ids.end()
    );
    for (auto it = paths_.begin(); it != paths_.end();) {
        if (!active.contains(it->first)) {
            evaluation_states_.erase(it->first);
            it = paths_.erase(it);
        } else {
            ++it;
        }
    }
}

void DynamicFillHazardRuntimeCpp::observe_trade(
    bool is_sell_trade,
    double trade_price,
    double quantity,
    std::int64_t provider_receive_ts_ns,
    std::int64_t feature_ready_ts_ns
) {
    const std::int64_t visible_ts_ns = feature_ready_ts_ns > 0
        ? feature_ready_ts_ns
        : provider_receive_ts_ns;
    if (!std::isfinite(trade_price) || !std::isfinite(quantity) ||
        quantity <= 0.0 || provider_receive_ts_ns <= 0 ||
        visible_ts_ns <= 0) {
        return;
    }
    if (!is_sell_trade) {
        return;
    }
    const double tolerance = config_.tick_size * 0.51;
    for (auto& [id, path] : paths_) {
        static_cast<void>(id);
        if (std::abs(path.price - trade_price) > tolerance) {
            continue;
        }
        ++path.exact_price_trade_events;
        path.exact_price_trade_qty += quantity;
        path.receive_ts_ns = std::max(
            path.receive_ts_ns, provider_receive_ts_ns
        );
        path.feature_ready_ts_ns = std::max(
            path.feature_ready_ts_ns, visible_ts_ns
        );
    }
}

std::string DynamicFillHazardRuntimeCpp::inventory_role(
    double inventory,
    double lot_size
) {
    if (std::abs(inventory) < std::max(std::abs(lot_size) * 0.5, 1e-12)) {
        return "opener";
    }
    return inventory > 0.0 ? "add" : "reducing";
}

std::string DynamicFillHazardRuntimeCpp::hold_phase() const {
    if (!hold_) {
        return "NONE";
    }
    switch (hold_->phase) {
    case HoldState::Phase::CancelPending:
        return "CANCEL_PENDING";
    case HoldState::Phase::ExchangeTerminal:
        return "EXCHANGE_TERMINAL";
    case HoldState::Phase::PostCancelRecovery:
        return "POST_CANCEL_RECOVERY";
    case HoldState::Phase::ReentryEligible:
        return "REENTRY_ELIGIBLE";
    }
    return "UNKNOWN";
}

DynamicFillHazardEvaluation DynamicFillHazardRuntimeCpp::build_observation(
    OrderPath& path,
    double inventory,
    std::int64_t now_ns
) {
    DynamicFillHazardEvaluation value;
    const NativeBookTop top = book_.top();
    const bool valid_book = top.valid && top.last_receive_ts_ns > 0 &&
        top.last_receive_ts_ns <= now_ns;
    const double best_bid = static_cast<double>(top.best_bid_tick) * config_.tick_size;
    const double best_ask = static_cast<double>(top.best_ask_tick) * config_.tick_size;
    const double mid = valid_book ? 0.5 * (best_bid + best_ask) : 0.0;
    const double size_sum = top.best_bid_qty + top.best_ask_qty;
    const double microprice = valid_book && size_sum > 1e-12
        ? (best_ask * top.best_bid_qty + best_bid * top.best_ask_qty) / size_sum
        : mid;

    auto [state_it, inserted] = evaluation_states_.try_emplace(path.client_order_id);
    EvaluationState& state = state_it->second;
    if (inserted) {
        state.activation_ts_ns = path.activation_ts_ns;
        state.anchor_mid = mid;
        state.anchor_microprice = microprice;
        state.anchor_top_size = top.best_bid_qty;
    }
    value.elapsed_ms = std::max(
        0.0,
        static_cast<double>(now_ns - state.activation_ts_ns) / 1'000'000.0
    );
    const auto edge_index = static_cast<std::int64_t>(
        std::floor(value.elapsed_ms / config_.evaluation_interval_ms)
    );
    if (edge_index <= state.last_edge_index) {
        return value;
    }
    value.emitted = true;
    value.edge_ms = static_cast<std::int64_t>(std::llround(
        static_cast<double>(edge_index) * config_.evaluation_interval_ms
    ));
    value.missed_edges = std::max<std::int64_t>(
        0, edge_index - state.last_edge_index - 1
    );
    state.last_edge_index = edge_index;

    double price_adverse = 0.0;
    double microprice_adverse = 0.0;
    if (valid_book && state.anchor_mid > 0.0) {
        price_adverse = std::max(
            0.0, (state.anchor_mid - mid) / config_.tick_size
        );
        microprice_adverse = std::max(
            0.0, (state.anchor_microprice - microprice) / config_.tick_size
        );
    }
    state.worst_adverse_ticks = std::max(
        state.worst_adverse_ticks, price_adverse
    );
    state.worst_microprice_adverse_ticks = std::max(
        state.worst_microprice_adverse_ticks, microprice_adverse
    );
    if (state.adverse_jump_ts_ns <= 0 &&
        price_adverse >= config_.price_jump_ticks) {
        state.adverse_jump_ts_ns = now_ns;
    }

    const double queue_initial = std::max(0.0, path.initial_visible_qty);
    const double queue_remaining = std::max(0.0, path.queue_ahead_estimate());
    const double queue_fraction = queue_initial > 1e-12
        ? std::min(1.0, queue_remaining / queue_initial)
        : 0.0;
    const std::int64_t cancel_events = path.inferred_cancel_events();
    const double cancel_qty = path.inferred_cancel_qty();
    const std::int64_t path_events = cancel_events + path.refill_events;
    const double depth_recovery = state.anchor_top_size > 1e-12
        ? std::min(2.0, top.best_bid_qty / state.anchor_top_size)
        : 0.0;
    const std::int64_t seconds = (now_ns / 1'000'000'000) % 86'400;
    constexpr double pi = 3.141592653589793238462643383279502884;
    const double angle = 2.0 * pi * static_cast<double>(seconds) / 86'400.0;
    const double quote_distance = std::max(0.0, mid - path.price) /
        config_.tick_size;
    const double deep_age_ms = top.last_receive_ts_ns > 0
        ? std::max(0.0, static_cast<double>(now_ns - top.last_receive_ts_ns) /
            1'000'000.0)
        : std::numeric_limits<double>::infinity();
    const double path_age_ms = path.feature_ready_ts_ns > 0
        ? std::max(0.0, static_cast<double>(now_ns - path.feature_ready_ts_ns) /
            1'000'000.0)
        : std::numeric_limits<double>::infinity();
    const std::int64_t feature_source_ts_ns = path.receive_ts_ns;
    const std::int64_t feature_ready_ts_ns = std::max(
        path.feature_ready_ts_ns, top.last_receive_ts_ns
    );
    const std::string role = inventory_role(inventory, config_.lot_size);

    value.valid = valid_book && path.valid && feature_source_ts_ns > 0 &&
        feature_ready_ts_ns > 0 && feature_ready_ts_ns <= now_ns &&
        state.activation_ts_ns <= now_ns;
    value.reason = "ok";
    if (!valid_book) {
        value.reason = "deep_book_invalid";
    } else if (!path.valid) {
        value.reason = path.invalid_reason.empty()
            ? "deep_path_invalid"
            : path.invalid_reason;
    } else if (feature_source_ts_ns <= 0) {
        value.reason = "missing_feature_source_time";
    } else if (feature_ready_ts_ns <= 0) {
        value.reason = "missing_feature_ready_time";
    } else if (feature_ready_ts_ns > now_ns) {
        value.reason = "future_feature_time";
    } else if (state.activation_ts_ns > now_ns) {
        value.reason = "future_activation_time";
    }
    value.inventory_role = role;
    value.feature_source_ts_ns = feature_source_ts_ns;
    value.feature_ready_ts_ns = feature_ready_ts_ns;
    value.deep_generation = path.generation;
    value.deep_age_ms = deep_age_ms;
    value.order_price = path.price;
    value.mid = mid;
    value.microprice = microprice;
    value.queue_initial = queue_initial;
    value.queue_remaining = queue_remaining;
    value.cancel_events = cancel_events;
    value.cancel_qty = cancel_qty;
    value.refill_events = path.refill_events;
    value.refill_qty = path.refill_qty;

    if (!value.valid) {
        return value;
    }
    const double price_recovery = state.worst_adverse_ticks > 1e-12
        ? clamp_value(
            1.0 - price_adverse / std::max(state.worst_adverse_ticks, 1e-12),
            0.0,
            1.0
        )
        : 1.0;
    const double microprice_recovery = state.worst_microprice_adverse_ticks > 1e-12
        ? clamp_value(
            1.0 - microprice_adverse /
                std::max(state.worst_microprice_adverse_ticks, 1e-12),
            0.0,
            1.0
        )
        : 1.0;
    const double jump_age_ms = state.adverse_jump_ts_ns > 0
        ? std::max(0.0, static_cast<double>(now_ns - state.adverse_jump_ts_ns) /
            1'000'000.0)
        : -1.0;
    const std::array<double, kDynamicFillHazardFeatureCount> features{
        safe_log1p(value.elapsed_ms),
        safe_log1p(std::max(deep_age_ms, path_age_ms)),
        static_cast<double>(top.best_ask_tick - top.best_bid_tick),
        quote_distance,
        safe_log1p(top.best_bid_qty),
        safe_log1p(top.best_ask_qty),
        size_sum > 1e-12 ? (top.best_bid_qty - top.best_ask_qty) / size_sum : 0.0,
        (mid - microprice) / config_.tick_size,
        safe_log1p(queue_initial),
        safe_log1p(queue_remaining),
        queue_fraction,
        std::max(0.0, 1.0 - queue_fraction),
        safe_log1p(static_cast<double>(cancel_events)),
        safe_log1p(cancel_qty),
        safe_log1p(static_cast<double>(path.refill_events)),
        safe_log1p(path.refill_qty),
        path_events > 0
            ? static_cast<double>(path.refill_events) /
                static_cast<double>(path_events)
            : 0.5,
        price_adverse,
        state.worst_adverse_ticks,
        price_recovery,
        microprice_adverse,
        state.worst_microprice_adverse_ticks,
        microprice_recovery,
        depth_recovery,
        state.adverse_jump_ts_ns > 0 ? 1.0 : 0.0,
        jump_age_ms >= 0.0 ? safe_log1p(jump_age_ms) : 0.0,
        std::sin(angle),
        std::cos(angle),
        role == "opener" ? 1.0 : 0.0,
        role == "add" ? 1.0 : 0.0,
        role == "reducing" ? 1.0 : 0.0,
    };
    value.prediction = predict_dynamic_fill_hazard(
        model_, features, config_.exposure_ms
    );
    return value;
}

bool DynamicFillHazardRuntimeCpp::eligible(
    const DynamicFillHazardEvaluation& value
) const {
    return value.valid &&
        (value.inventory_role == "opener" || value.inventory_role == "add");
}

std::string DynamicFillHazardRuntimeCpp::release_hold(bool terminal) {
    if (!hold_) {
        return "none";
    }
    const std::string id = hold_->client_order_id;
    paths_.erase(id);
    evaluation_states_.erase(id);
    hold_.reset();
    if (terminal) {
        ++counters_.reentry_count;
        return "reenter";
    }
    return "release";
}

DynamicFillHazardEvaluation
DynamicFillHazardRuntimeCpp::evaluate_prospective_cancel_reentry(
    double candidate_price,
    double inventory,
    std::int64_t now_ns
) {
    if (!hold_ || hold_->phase != HoldState::Phase::PostCancelRecovery) {
        throw std::invalid_argument(
            "prospective q90 evaluation requires POST_CANCEL_RECOVERY"
        );
    }
    if (!std::isfinite(candidate_price) || candidate_price <= 0.0 ||
        now_ns <= 0) {
        throw std::invalid_argument(
            "prospective q90 candidate identity is invalid"
        );
    }

    DynamicFillHazardEvaluation value;
    value.emitted = true;
    value.edge_ms = 0;
    value.elapsed_ms = 0.0;
    const NativeBookTop top = book_.top();
    const bool valid_book = top.valid && top.last_receive_ts_ns > 0 &&
        top.last_receive_ts_ns <= now_ns;
    const double best_bid =
        static_cast<double>(top.best_bid_tick) * config_.tick_size;
    const double best_ask =
        static_cast<double>(top.best_ask_tick) * config_.tick_size;
    const double mid = valid_book ? 0.5 * (best_bid + best_ask) : 0.0;
    const double size_sum = top.best_bid_qty + top.best_ask_qty;
    const double microprice = valid_book && size_sum > 1e-12
        ? (best_ask * top.best_bid_qty + best_bid * top.best_ask_qty) /
            size_sum
        : mid;
    const double scaled = candidate_price / config_.tick_size;
    const auto candidate_tick = static_cast<std::int64_t>(std::llround(scaled));
    const bool on_tick = std::abs(
        candidate_price - static_cast<double>(candidate_tick) * config_.tick_size
    ) <= std::max(1e-9, config_.tick_size * 1e-8);
    const bool inside_spread = valid_book &&
        candidate_tick > top.best_bid_tick &&
        candidate_tick < top.best_ask_tick;
    const NativeBookLookup level = book_.lookup(true, candidate_tick);
    const bool queue_known = inside_spread || level.strict_usable();
    const double queue_at_tail = inside_spread
        ? 0.0
        : (level.quantity_known ? std::max(0.0, level.quantity) : 0.0);
    const bool gtx_eligible = valid_book && on_tick &&
        candidate_tick < top.best_ask_tick;
    const std::int64_t feature_source_ts_ns = last_provider_receive_ts_ns_;
    const std::int64_t feature_ready_ts_ns = top.last_receive_ts_ns;
    const double deep_age_ms = top.last_receive_ts_ns > 0
        ? std::max(
            0.0,
            static_cast<double>(now_ns - top.last_receive_ts_ns) / 1'000'000.0
        )
        : std::numeric_limits<double>::infinity();
    const std::string role = inventory_role(inventory, config_.lot_size);

    value.valid = valid_book && queue_known && gtx_eligible &&
        feature_source_ts_ns > 0 && feature_ready_ts_ns > 0 &&
        feature_ready_ts_ns <= now_ns && std::isfinite(deep_age_ms);
    value.reason = "ok";
    if (!valid_book) {
        value.reason = "deep_book_invalid";
    } else if (!on_tick) {
        value.reason = "candidate_price_off_tick";
    } else if (!gtx_eligible) {
        value.reason = "candidate_gtx_reject";
    } else if (!queue_known) {
        value.reason = "candidate_level_not_causally_covered";
    } else if (feature_source_ts_ns <= 0) {
        value.reason = "missing_feature_source_time";
    } else if (feature_ready_ts_ns <= 0) {
        value.reason = "missing_feature_ready_time";
    } else if (feature_ready_ts_ns > now_ns) {
        value.reason = "future_feature_time";
    } else if (!std::isfinite(deep_age_ms)) {
        value.reason = "candidate_level_age_invalid";
    }
    value.inventory_role = role;
    value.feature_source_ts_ns = feature_source_ts_ns;
    value.feature_ready_ts_ns = feature_ready_ts_ns;
    value.deep_generation = top.segment_id;
    value.deep_age_ms = deep_age_ms;
    value.order_price = candidate_price;
    value.mid = mid;
    value.microprice = microprice;
    value.queue_initial = queue_at_tail;
    value.queue_remaining = queue_at_tail;

    ++counters_.prospective_eval_count;
    if (!value.valid) {
        ++counters_.prospective_invalid_count;
        value.action = "hold_invalid";
        return value;
    }
    ++counters_.prospective_valid_count;

    const std::int64_t seconds = (now_ns / 1'000'000'000) % 86'400;
    constexpr double pi = 3.141592653589793238462643383279502884;
    const double angle = 2.0 * pi * static_cast<double>(seconds) / 86'400.0;
    const double quote_distance = std::max(0.0, mid - candidate_price) /
        config_.tick_size;
    const double queue_fraction = queue_at_tail > 1e-12 ? 1.0 : 0.0;
    const std::array<double, kDynamicFillHazardFeatureCount> features{
        0.0,
        safe_log1p(deep_age_ms),
        static_cast<double>(top.best_ask_tick - top.best_bid_tick),
        quote_distance,
        safe_log1p(top.best_bid_qty),
        safe_log1p(top.best_ask_qty),
        size_sum > 1e-12
            ? (top.best_bid_qty - top.best_ask_qty) / size_sum
            : 0.0,
        (mid - microprice) / config_.tick_size,
        safe_log1p(queue_at_tail),
        safe_log1p(queue_at_tail),
        queue_fraction,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.5,
        0.0,
        0.0,
        1.0,
        0.0,
        0.0,
        1.0,
        1.0,
        0.0,
        0.0,
        std::sin(angle),
        std::cos(angle),
        role == "opener" ? 1.0 : 0.0,
        role == "add" ? 1.0 : 0.0,
        role == "reducing" ? 1.0 : 0.0,
    };
    value.prediction = predict_dynamic_fill_hazard(
        model_, features, config_.exposure_ms
    );
    const bool recovered = role == "reducing" ||
        (eligible(value) && value.prediction.score < config_.entry_threshold);
    if (!recovered) {
        value.action = "hold";
        return value;
    }
    if (!hold_->recovered) {
        ++counters_.recovery_count;
    }
    hold_->recovered = true;
    hold_->phase = HoldState::Phase::ReentryEligible;
    value.action = release_hold(true);
    return value;
}

DynamicFillHazardEvaluation DynamicFillHazardRuntimeCpp::evaluate(
    const std::string& client_order_id,
    double inventory,
    std::int64_t now_ns
) {
    DynamicFillHazardEvaluation value;
    if (hold_ && inventory < -std::max(config_.lot_size * 0.5, 1e-12)) {
        if (hold_->phase == HoldState::Phase::PostCancelRecovery) {
            hold_->phase = HoldState::Phase::ReentryEligible;
        }
        value.action = release_hold(hold_->terminal);
        return value;
    }
    if (hold_ && hold_->client_order_id == client_order_id &&
        (hold_->phase == HoldState::Phase::ExchangeTerminal ||
         hold_->phase == HoldState::Phase::PostCancelRecovery)) {
        value.reason = "prospective_placement_state_required";
        value.action = "post_cancel_recovery_requires_prospective_placement";
        return value;
    }
    const auto found = paths_.find(client_order_id);
    if (found == paths_.end()) {
        value.reason = "order_path_missing";
        return value;
    }
    value = build_observation(found->second, inventory, now_ns);
    if (!value.emitted) {
        return value;
    }
    ++counters_.eval_count;
    if (value.valid) {
        ++counters_.valid_eval_count;
    } else {
        ++counters_.invalid_eval_count;
    }

    if (!hold_) {
        if (!eligible(value)) {
            return value;
        }
        if (value.prediction.score < config_.entry_threshold) {
            ++counters_.keep_count;
            value.action = "keep";
            return value;
        }
        hold_ = HoldState{
            client_order_id,
            found->second.price,
            value.prediction.score,
            now_ns,
            HoldState::Phase::CancelPending,
            false,
            false,
        };
        found->second.cancel_pending = true;
        ++counters_.cancel_request_count;
        value.action = "cancel";
        return value;
    }

    if (hold_->client_order_id != client_order_id) {
        return value;
    }
    if (!value.valid) {
        ++counters_.retain_invalid_count;
        value.action = "hold_invalid";
        return value;
    }
    const bool recovered = value.inventory_role == "reducing" ||
        (eligible(value) && value.prediction.score < config_.entry_threshold);
    if (!recovered) {
        value.action = "hold";
        return value;
    }
    if (!hold_->recovered) {
        ++counters_.recovery_count;
    }
    hold_->recovered = true;
    const bool terminal = found->second.terminal || hold_->terminal;
    if (!found->second.cancel_pending) {
        value.action = release_hold(terminal);
    } else {
        value.action = "recover_wait_ack";
    }
    return value;
}

std::string DynamicFillHazardRuntimeCpp::on_fill(
    const std::string& client_order_id,
    double remaining_after,
    std::int64_t now_ns
) {
    static_cast<void>(now_ns);
    const auto found = paths_.find(client_order_id);
    if (found == paths_.end()) {
        return "none";
    }
    if (hold_ && hold_->client_order_id == client_order_id &&
        found->second.cancel_pending) {
        ++counters_.pre_ack_fill_count;
    }
    if (remaining_after < config_.lot_size) {
        found->second.terminal = true;
        if (hold_ && hold_->client_order_id == client_order_id) {
            hold_->terminal = true;
            hold_->phase = HoldState::Phase::ExchangeTerminal;
            paths_.erase(client_order_id);
            evaluation_states_.erase(client_order_id);
            hold_.reset();
            return "terminal_complete_no_reentry";
        }
        paths_.erase(client_order_id);
        evaluation_states_.erase(client_order_id);
    }
    return "none";
}

std::string DynamicFillHazardRuntimeCpp::on_cancel_ack(
    const std::string& client_order_id,
    std::int64_t now_ns,
    double remaining_after
) {
    static_cast<void>(now_ns);
    if (!std::isfinite(remaining_after)) {
        throw std::invalid_argument(
            "cancel ACK requires an explicit remaining quantity"
        );
    }
    paths_.erase(client_order_id);
    evaluation_states_.erase(client_order_id);
    if (!hold_ || hold_->client_order_id != client_order_id) {
        return "none";
    }
    ++counters_.cancel_ack_count;
    hold_->terminal = true;
    hold_->phase = HoldState::Phase::ExchangeTerminal;
    if (remaining_after <= 1e-12) {
        hold_.reset();
        return "terminal_complete_no_reentry";
    }
    hold_->phase = HoldState::Phase::PostCancelRecovery;
    return "post_cancel_recovery";
}

std::string DynamicFillHazardRuntimeCpp::on_order_terminal(
    const std::string& client_order_id,
    std::int64_t now_ns,
    const std::string& reason,
    double remaining_after
) {
    static_cast<void>(now_ns);
    const TerminalPolicyRouteCpp route = terminal_policy_route(
        reason,
        remaining_after
    );
    if (route == TerminalPolicyRouteCpp::Unsupported) {
        throw std::invalid_argument("unsupported q90 terminal reason: " + reason);
    }
    paths_.erase(client_order_id);
    evaluation_states_.erase(client_order_id);
    if (!hold_ || hold_->client_order_id != client_order_id) {
        return "none";
    }
    hold_->terminal = true;
    hold_->phase = HoldState::Phase::ExchangeTerminal;
    switch (route) {
    case TerminalPolicyRouteCpp::ProspectiveCancelReentry:
        hold_->phase = HoldState::Phase::PostCancelRecovery;
        return "post_cancel_recovery";
    case TerminalPolicyRouteCpp::TerminalComplete:
        hold_.reset();
        return "terminal_complete_no_reentry";
    case TerminalPolicyRouteCpp::BaselineResubmit:
        hold_.reset();
        return "baseline_resubmit";
    case TerminalPolicyRouteCpp::ShutdownNoReentry:
        hold_.reset();
        return "shutdown_no_reentry";
    case TerminalPolicyRouteCpp::Unsupported:
        break;
    }
    throw std::logic_error("unreachable q90 terminal policy route");
}

}  // namespace narrowgate_cpp
