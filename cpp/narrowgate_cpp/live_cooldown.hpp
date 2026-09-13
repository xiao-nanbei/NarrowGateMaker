#pragma once

#include "f05_cooldown_types.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <mutex>

namespace narrowgate_cpp {

// The production policies are deliberately bounded.  The current BUY E3
// artifact has 126 predicates, 95 clauses and fewer than 512 literals; the
// SELL policy has three predicates.  Configuration is compiled into these
// arrays once at startup.  No policy string, vector or map is retained by the
// decision-time object.
inline constexpr std::size_t kLiveCooldownMaxEma = 10;
inline constexpr std::size_t kLiveCooldownMaxPairs = 45;
inline constexpr std::size_t kLiveCooldownMaxPredicates = 128;
inline constexpr std::size_t kLiveCooldownMaxRules = 8;
inline constexpr std::size_t kLiveCooldownMaxClauses = 128;
inline constexpr std::size_t kLiveCooldownMaxLiterals = 768;

enum class LiveCooldownProfile : std::uint8_t {
    SellSelected = 0,
    BuyE3 = 1,
};

enum class LiveCooldownObserveStatus : std::uint8_t {
    PendingOnly = 0,
    Applied = 1,
    OutOfOrderIgnored = 2,
    GapReset = 3,
    InvalidCallback = 4,
};

enum class LiveCooldownDecisionStatus : std::uint8_t {
    RuleMatched = 0,
    NoRuleMatched = 1,
    NoCompletedWindow = 2,
    WarmupIncomplete = 3,
    FeatureStateStale = 4,
    LatestWindowUnobserved = 5,
    SelectedPredicateUnobserved = 6,
    RuleUnobserved = 7,
};

struct LiveCooldownDecisionPod {
    std::int64_t duration_ms = 0;
    std::int32_t matched_rule_index = -1;
    std::int32_t detail_index = -1;
    std::int64_t feature_ready_ts_ns = 0;
    double feature_age_ms = 0.0;
    bool support_valid = false;
    LiveCooldownDecisionStatus status =
        LiveCooldownDecisionStatus::NoCompletedWindow;
    std::uint16_t predicate_count = 0;
    std::array<std::int8_t, kLiveCooldownMaxPredicates> predicate_values{};
};

struct LiveCooldownAuditPod {
    std::uint64_t updates = 0;
    std::uint64_t completed_windows = 0;
    std::uint64_t gap_windows = 0;
    std::uint64_t resets = 0;
    std::uint64_t invalid_updates = 0;
    std::uint64_t out_of_order_updates = 0;
    std::uint64_t gap_resets = 0;
    bool warmup_admitted = false;
    std::int64_t feature_ready_ts_ns = 0;
    std::int64_t warmup_start_right_ts_ns = 0;
    std::int64_t last_window_right_ts_ns = 0;
};

struct LiveCooldownFeatureSnapshotPod {
    std::uint8_t ema_count = 0;
    std::uint8_t pair_count = 0;
    bool current_window_observed = false;
    bool ema_initialized = false;
    std::int64_t last_observed_ts_ns = 0;
    std::array<double, kLiveCooldownMaxEma> ema{};
    std::array<double, kLiveCooldownMaxEma> velocity{};
    std::array<double, kLiveCooldownMaxEma> acceleration{};
    std::array<std::int8_t, kLiveCooldownMaxPairs> effective_sign{};
    std::array<std::int8_t, kLiveCooldownMaxPairs> last_cross_direction{};
    std::array<std::int64_t, kLiveCooldownMaxPairs> arrangement_start_ts_ns{};
    std::array<std::int64_t, kLiveCooldownMaxPairs> last_cross_ts_ns{};
};

class alignas(64) NativeLiveCooldownHotPath {
public:
    NativeLiveCooldownHotPath(
        LiveCooldownProfile profile,
        const F05BooleanPolicy& policy,
        double warmup_s,
        double max_feature_age_s
    );

    [[nodiscard]] LiveCooldownObserveStatus observe_depth(
        std::int64_t receive_ts_ns,
        double best_bid,
        double best_ask
    ) noexcept;

    [[nodiscard]] LiveCooldownDecisionPod evaluate(
        std::int64_t decision_ts_ns,
        double campaign_age_s,
        std::int64_t baseline_duration_ms
    ) noexcept;

    void reset() noexcept;

    [[nodiscard]] LiveCooldownAuditPod audit() const noexcept;
    [[nodiscard]] LiveCooldownFeatureSnapshotPod feature_snapshot() const noexcept;
    [[nodiscard]] LiveCooldownProfile profile() const noexcept { return profile_; }
    [[nodiscard]] std::size_t core_size_bytes() const noexcept {
        return sizeof(*this);
    }
    [[nodiscard]] static constexpr std::size_t cache_line_bytes() noexcept {
        return 64;
    }

private:
    struct LiteralPod {
        std::uint16_t predicate_index = 0;
        bool negated = false;
    };

    struct ClausePod {
        std::uint16_t literal_offset = 0;
        std::uint16_t literal_count = 0;
    };

    struct RulePod {
        std::uint16_t clause_offset = 0;
        std::uint16_t clause_count = 0;
        std::int64_t duration_ms = 0;
    };

    struct PairIndexPod {
        std::uint8_t fast = 0;
        std::uint8_t slow = 0;
    };

    struct DefinitionPod {
        F05PredicateMetric metric = F05PredicateMetric::CampaignAgeGtControl;
        std::uint8_t pair_index = 0;
        bool threshold_enabled = false;
        bool configured = false;
        double threshold = 0.0;
    };

    struct PairStatePod {
        std::int8_t effective_sign = 0;
        std::int8_t last_cross_direction = 0;
        std::int64_t arrangement_start_ts_ns = 0;
        std::int64_t last_cross_ts_ns = 0;
    };

    template <LiveCooldownProfile Profile>
    void update_ema(std::int64_t timestamp_ns, double value) noexcept;

    template <LiveCooldownProfile Profile>
    LiveCooldownDecisionPod evaluate_impl(
        std::int64_t decision_ts_ns,
        double campaign_age_s,
        std::int64_t baseline_duration_ms
    ) noexcept;

    void compile_policy(const F05BooleanPolicy& policy);
    void emit_window(
        std::int64_t left_ns,
        std::int64_t feature_ready_ts_ns,
        double mid,
        bool source_gap
    ) noexcept;
    void reset_feature_state() noexcept;
    void update_pair(
        std::size_t pair_index,
        double fast,
        double slow,
        std::int64_t timestamp_ns
    ) noexcept;
    mutable std::mutex mutex_;
    LiveCooldownProfile profile_ = LiveCooldownProfile::SellSelected;
    double warmup_s_ = 0.0;
    double max_feature_age_s_ = 0.0;

    std::uint16_t predicate_count_ = 0;
    std::uint16_t rule_count_ = 0;
    std::uint16_t clause_count_ = 0;
    std::uint16_t literal_count_ = 0;
    std::uint8_t ema_count_ = 0;
    std::uint8_t pair_count_ = 0;
    std::array<LiteralPod, kLiveCooldownMaxLiterals> literals_{};
    std::array<ClausePod, kLiveCooldownMaxClauses> clauses_{};
    std::array<RulePod, kLiveCooldownMaxRules> rules_{};
    std::array<PairIndexPod, kLiveCooldownMaxPairs> pair_indices_{};
    std::array<DefinitionPod, kLiveCooldownMaxPredicates> definitions_{};
    std::array<double, kLiveCooldownMaxEma> half_lives_s_{};
    std::int16_t sell_short_predicate_index_ = -1;
    std::int16_t sell_long_predicate_index_ = -1;
    std::int16_t sell_campaign_predicate_index_ = -1;

    bool pending_ = false;
    std::int64_t pending_left_ns_ = 0;
    double pending_mid_ = 0.0;
    std::int64_t feature_ready_ts_ns_ = 0;
    std::int64_t warmup_start_right_ns_ = 0;
    std::int64_t last_window_right_ns_ = 0;
    bool ema_initialized_ = false;
    bool current_window_observed_ = false;
    std::int64_t last_observed_ts_ns_ = 0;
    std::array<double, kLiveCooldownMaxEma> ema_{};
    std::array<double, kLiveCooldownMaxEma> velocity_{};
    std::array<double, kLiveCooldownMaxEma> acceleration_{};
    std::array<PairStatePod, kLiveCooldownMaxPairs> pairs_{};
    LiveCooldownAuditPod audit_{};
};

}  // namespace narrowgate_cpp
