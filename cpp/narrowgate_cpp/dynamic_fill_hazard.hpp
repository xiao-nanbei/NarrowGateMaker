#pragma once

#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace narrowgate_cpp {

inline constexpr std::size_t kDynamicFillHazardFeatureCount = 31;
inline constexpr std::array<std::string_view, kDynamicFillHazardFeatureCount>
    kDynamicFillHazardFeatureNames{
        "elapsed_log1p",
        "visible_state_age_log1p",
        "spread_ticks",
        "quote_distance_ticks",
        "top_bid_size_log1p",
        "top_ask_size_log1p",
        "book_imbalance",
        "side_microprice_adverse_ticks",
        "queue_initial_log1p",
        "queue_remaining_log1p",
        "policy_queue_fraction_left",
        "policy_queue_progress",
        "visible_cancel_events_log1p",
        "visible_cancel_size_log1p",
        "visible_refill_events_log1p",
        "visible_refill_size_log1p",
        "visible_refill_event_share",
        "price_adverse_ticks",
        "price_worst_adverse_ticks",
        "price_recovery_ratio",
        "microprice_adverse_ticks",
        "microprice_worst_adverse_ticks",
        "microprice_recovery_ratio",
        "visible_depth_recovery_ratio",
        "native_adverse_jump_seen",
        "time_since_native_adverse_jump_log1p",
        "clock_hour_sin",
        "clock_hour_cos",
        "role_opener",
        "role_add",
        "role_reducing",
    };

struct DynamicFillHazardHead {
    std::array<double, kDynamicFillHazardFeatureCount> feature_mean{};
    std::array<double, kDynamicFillHazardFeatureCount> feature_scale{};
    std::array<double, kDynamicFillHazardFeatureCount> coefficients{};
    double intercept = 0.0;
    bool has_calibrator = false;
    double calibrator_intercept = 0.0;
    double calibrator_slope = 1.0;
    double probability_clip_lower = 1e-12;
    double probability_clip_upper = 1.0 - 1e-12;
};

struct DynamicFillHazardModel {
    DynamicFillHazardHead favorable;
    DynamicFillHazardHead adverse;
};

struct DynamicFillHazardPrediction {
    double favorable_raw_probability = 0.0;
    double favorable_probability = 0.0;
    double adverse_raw_probability = 0.0;
    double adverse_probability = 0.0;
    double score = 0.0;
};

[[nodiscard]] DynamicFillHazardPrediction predict_dynamic_fill_hazard(
    const DynamicFillHazardModel& model,
    const std::array<double, kDynamicFillHazardFeatureCount>& features,
    double exposure_ms
);

enum class NativeBookEventType : std::uint8_t {
    Snapshot = 1,
    Delta = 2,
    SourceGap = 3,
};

struct NativeBookLevel {
    bool is_bid = true;
    std::int64_t price_tick = 0;
    double quantity = 0.0;
};

struct NativeBookLevelChange {
    bool is_bid = true;
    std::int64_t price_tick = 0;
    double quantity_before = 0.0;
    double quantity_after = 0.0;
    std::int64_t exchange_ts_ns = 0;
    std::int64_t receive_ts_ns = 0;
    std::int64_t update_id = -1;
    std::int64_t segment_id = 0;
};

struct NativeBookApplyResult {
    bool accepted = false;
    bool snapshot_reset = false;
    bool invalidated = false;
    std::vector<NativeBookLevelChange> changes;
};

struct NativeBookLookup {
    std::string status = "unknown";
    std::string reason = "sequence_unavailable";
    double quantity = 0.0;
    bool quantity_known = false;
    std::int64_t asof_exchange_ts_ns = 0;
    std::int64_t segment_id = 0;
    std::int64_t snapshot_min_tick = 0;
    std::int64_t snapshot_max_tick = 0;
    bool snapshot_range_known = false;

    [[nodiscard]] bool strict_usable() const noexcept {
        return quantity_known &&
            (status == "exact" || status == "known_zero") &&
            quantity >= 0.0;
    }
};

struct NativeBookTop {
    bool valid = false;
    std::int64_t best_bid_tick = 0;
    std::int64_t best_ask_tick = 0;
    double best_bid_qty = 0.0;
    double best_ask_qty = 0.0;
    std::int64_t last_exchange_ts_ns = 0;
    std::int64_t last_receive_ts_ns = 0;
    std::int64_t segment_id = 0;
};

struct NativeBookSequenceStats {
    std::int64_t logical_messages = 0;
    std::int64_t snapshot_messages = 0;
    std::int64_t update_messages = 0;
    std::int64_t duplicate_messages = 0;
    std::int64_t duplicate_snapshots = 0;
    std::int64_t stale_updates = 0;
    std::int64_t ignored_before_snapshot = 0;
    std::int64_t sequence_gaps = 0;
    std::int64_t invalid_sequence_messages = 0;
    std::int64_t accepted_updates = 0;
    std::int64_t delta_bootstrap_messages = 0;
    std::int64_t message_time_reversals = 0;
};

class NativeExchangeBookSchedulerCpp {
public:
    explicit NativeExchangeBookSchedulerCpp(
        bool strict_sequence = true,
        std::int64_t strict_after_ns = 0,
        bool allow_delta_bootstrap = false
    );

    [[nodiscard]] NativeBookApplyResult apply_message(
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
    );

    [[nodiscard]] NativeBookLookup lookup(
        bool is_bid,
        std::int64_t price_tick
    ) const;
    [[nodiscard]] NativeBookTop top() const;
    [[nodiscard]] const NativeBookSequenceStats& stats() const noexcept {
        return stats_;
    }
    [[nodiscard]] bool initialized() const noexcept { return initialized_; }
    [[nodiscard]] std::int64_t segment_id() const noexcept { return segment_id_; }
    [[nodiscard]] std::int64_t last_exchange_ts_ns() const noexcept {
        return last_exchange_ts_ns_;
    }
    [[nodiscard]] std::int64_t last_receive_ts_ns() const noexcept {
        return last_receive_ts_ns_;
    }

private:
    struct SnapshotKey {
        std::int64_t event_time_ms = 0;
        std::int64_t update_id = -1;

        [[nodiscard]] bool operator==(const SnapshotKey& other) const noexcept {
            return event_time_ms == other.event_time_ms &&
                update_id == other.update_id;
        }
    };

    using BookSide = std::map<std::int64_t, double>;

    [[nodiscard]] bool strict_at(std::int64_t exchange_ts_ns) const noexcept;
    [[nodiscard]] bool begin_message(
        NativeBookEventType event_type,
        std::int64_t exchange_ts_ns,
        std::int64_t receive_ts_ns,
        std::int64_t event_time_ms,
        std::int64_t transaction_time_ms,
        std::int64_t first_update_id,
        std::int64_t final_update_id,
        std::int64_t previous_final_update_id,
        std::int64_t last_update_id
    );
    void invalidate(bool count_gap = true);
    void reset_book();
    void rebuild_snapshot_identity();
    void apply_level(const NativeBookLevel& level);
    [[nodiscard]] const BookSide& side_book(bool is_bid) const noexcept;
    [[nodiscard]] BookSide& side_book(bool is_bid) noexcept;
    [[nodiscard]] const std::unordered_set<std::int64_t>& known_ticks(
        bool is_bid
    ) const noexcept;
    [[nodiscard]] std::unordered_set<std::int64_t>& known_ticks(
        bool is_bid
    ) noexcept;

    bool strict_sequence_ = true;
    std::int64_t strict_after_ns_ = 0;
    bool allow_delta_bootstrap_ = false;
    bool initialized_ = false;
    bool bridge_pending_ = false;
    std::int64_t last_update_id_ = -1;
    std::optional<SnapshotKey> last_snapshot_key_;
    std::int64_t previous_message_ts_ms_ = 0;
    std::int64_t last_source_ts_ns_ = 0;
    std::int64_t last_exchange_ts_ns_ = 0;
    std::int64_t last_receive_ts_ns_ = 0;
    std::int64_t segment_id_ = 0;
    std::int64_t segment_count_ = 0;
    BookSide bids_;
    BookSide asks_;
    std::unordered_set<std::int64_t> known_bids_;
    std::unordered_set<std::int64_t> known_asks_;
    std::optional<std::pair<std::int64_t, std::int64_t>> bid_snapshot_range_;
    std::optional<std::pair<std::int64_t, std::int64_t>> ask_snapshot_range_;
    NativeBookSequenceStats stats_;
};

struct DynamicFillHazardRuntimeConfig {
    double tick_size = 0.1;
    double lot_size = 0.001;
    double exposure_ms = 100.0;
    double price_jump_ticks = 1.0;
    double evaluation_interval_ms = 100.0;
    double entry_threshold = 0.0;
    bool strict_sequence = true;
    std::int64_t strict_after_ns = 0;
};

struct DynamicFillHazardEvaluation {
    bool emitted = false;
    bool valid = false;
    std::string reason;
    std::string inventory_role;
    std::string action = "none";
    std::int64_t edge_ms = 0;
    double elapsed_ms = 0.0;
    std::int64_t missed_edges = 0;
    std::int64_t feature_source_ts_ns = 0;
    std::int64_t feature_ready_ts_ns = 0;
    std::int64_t deep_generation = 0;
    double deep_age_ms = 0.0;
    double order_price = 0.0;
    double mid = 0.0;
    double microprice = 0.0;
    double queue_initial = 0.0;
    double queue_remaining = 0.0;
    std::int64_t cancel_events = 0;
    double cancel_qty = 0.0;
    std::int64_t refill_events = 0;
    double refill_qty = 0.0;
    DynamicFillHazardPrediction prediction;
};

struct DynamicFillHazardRuntimeCounters {
    std::int64_t eval_count = 0;
    std::int64_t valid_eval_count = 0;
    std::int64_t invalid_eval_count = 0;
    std::int64_t keep_count = 0;
    std::int64_t cancel_request_count = 0;
    std::int64_t cancel_ack_count = 0;
    std::int64_t pre_ack_fill_count = 0;
    std::int64_t recovery_count = 0;
    std::int64_t reentry_count = 0;
    std::int64_t retain_invalid_count = 0;
    std::int64_t prospective_eval_count = 0;
    std::int64_t prospective_valid_count = 0;
    std::int64_t prospective_invalid_count = 0;
};

class DynamicFillHazardRuntimeCpp {
public:
    DynamicFillHazardRuntimeCpp(
        DynamicFillHazardModel model,
        DynamicFillHazardRuntimeConfig config
    );

    [[nodiscard]] NativeBookApplyResult apply_book_message(
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
        bool execution_trade_same_ms = false
    );
    [[nodiscard]] NativeBookLookup activate_order(
        const std::string& client_order_id,
        double order_price,
        std::int64_t activation_ts_ns
    );
    void invalidate_order(
        const std::string& client_order_id,
        const std::string& reason
    );
    void drop_inactive(const std::vector<std::string>& active_client_order_ids);
    void observe_trade(
        bool is_sell_trade,
        double trade_price,
        double quantity,
        std::int64_t provider_receive_ts_ns,
        std::int64_t feature_ready_ts_ns
    );
    [[nodiscard]] DynamicFillHazardEvaluation evaluate(
        const std::string& client_order_id,
        double inventory,
        std::int64_t now_ns
    );
    [[nodiscard]] DynamicFillHazardEvaluation
    evaluate_prospective_cancel_reentry(
        double candidate_price,
        double inventory,
        std::int64_t now_ns
    );
    [[nodiscard]] std::string on_fill(
        const std::string& client_order_id,
        double remaining_after,
        std::int64_t now_ns
    );
    [[nodiscard]] std::string on_cancel_ack(
        const std::string& client_order_id,
        std::int64_t now_ns,
        double remaining_after
    );
    [[nodiscard]] std::string on_order_terminal(
        const std::string& client_order_id,
        std::int64_t now_ns,
        const std::string& reason,
        double remaining_after
    );

    [[nodiscard]] NativeBookTop top() const { return book_.top(); }
    [[nodiscard]] NativeBookLookup lookup(bool is_bid, std::int64_t price_tick) const {
        return book_.lookup(is_bid, price_tick);
    }
    [[nodiscard]] const NativeBookSequenceStats& sequence_stats() const noexcept {
        return book_.stats();
    }
    [[nodiscard]] const DynamicFillHazardRuntimeCounters& counters() const noexcept {
        return counters_;
    }
    [[nodiscard]] bool hold_active() const noexcept { return hold_.has_value(); }
    [[nodiscard]] std::string hold_order_id() const {
        return hold_ ? hold_->client_order_id : std::string{};
    }
    [[nodiscard]] std::string hold_phase() const;
    [[nodiscard]] std::size_t tracked_path_count() const noexcept {
        return paths_.size();
    }
    [[nodiscard]] std::size_t evaluation_state_count() const noexcept {
        return evaluation_states_.size();
    }
    [[nodiscard]] bool has_tracked_path(
        const std::string& client_order_id
    ) const noexcept {
        return paths_.contains(client_order_id);
    }

private:
    struct OrderPath {
        std::string client_order_id;
        double price = 0.0;
        std::int64_t price_tick = 0;
        std::int64_t activation_ts_ns = 0;
        std::int64_t generation = 0;
        double initial_visible_qty = 0.0;
        double current_visible_qty = 0.0;
        std::int64_t receive_ts_ns = 0;
        std::int64_t feature_ready_ts_ns = 0;
        bool valid = true;
        std::string invalid_reason;
        std::int64_t decrease_events = 0;
        double decrease_qty = 0.0;
        std::int64_t exact_price_trade_events = 0;
        double exact_price_trade_qty = 0.0;
        std::int64_t refill_events = 0;
        double refill_qty = 0.0;
        bool cancel_pending = false;
        bool terminal = false;

        [[nodiscard]] double inferred_cancel_qty() const noexcept;
        [[nodiscard]] std::int64_t inferred_cancel_events() const noexcept;
        [[nodiscard]] double queue_ahead_estimate() const noexcept;
        void invalidate(const std::string& reason);
    };

    struct EvaluationState {
        std::int64_t activation_ts_ns = 0;
        std::int64_t last_edge_index = -1;
        double anchor_mid = 0.0;
        double anchor_microprice = 0.0;
        double anchor_top_size = 0.0;
        double worst_adverse_ticks = 0.0;
        double worst_microprice_adverse_ticks = 0.0;
        std::int64_t adverse_jump_ts_ns = 0;
    };

    struct HoldState {
        enum class Phase {
            CancelPending,
            ExchangeTerminal,
            PostCancelRecovery,
            ReentryEligible,
        };

        std::string client_order_id;
        double order_price = 0.0;
        double entry_score = 0.0;
        std::int64_t entered_ts_ns = 0;
        Phase phase = Phase::CancelPending;
        bool recovered = false;
        bool terminal = false;
    };

    [[nodiscard]] static std::string inventory_role(
        double inventory,
        double lot_size
    );
    [[nodiscard]] bool eligible(const DynamicFillHazardEvaluation& value) const;
    [[nodiscard]] std::string release_hold(bool terminal);
    [[nodiscard]] DynamicFillHazardEvaluation build_observation(
        OrderPath& path,
        double inventory,
        std::int64_t now_ns
    );

    DynamicFillHazardModel model_;
    DynamicFillHazardRuntimeConfig config_;
    NativeExchangeBookSchedulerCpp book_;
    std::int64_t last_provider_receive_ts_ns_ = 0;
    std::unordered_map<std::string, OrderPath> paths_;
    std::unordered_map<std::string, EvaluationState> evaluation_states_;
    std::optional<HoldState> hold_;
    DynamicFillHazardRuntimeCounters counters_;
};

}  // namespace narrowgate_cpp
