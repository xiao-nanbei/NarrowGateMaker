#pragma once

#include <array>
#include <cstdint>
#include <iosfwd>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace narrowgate::f05::parity {

inline constexpr const char* kProtocolIdentity =
    "F05_STREAMING_BOOLEAN_COOLDOWN_PARITY_V2";
inline constexpr const char* kOwnerPolicyIdentity =
    "causal_multichannel_window_boolean_cooldown_owner_policy_v1";
inline constexpr const char* kOwnerPolicySchema =
    "causal_multichannel_window_boolean_cooldown_owner_policy_v1.artifact.v1";
inline constexpr const char* kControlAction = "CONTROL_85N";
inline constexpr std::int64_t kMinimumControlDurationMs = 85'000;
inline constexpr std::int64_t kBaseWindowWidthNs = 100'000'000;
inline constexpr const char* kShortCrossPredicate =
    "predicate::ema_pair_h4s_h16s:cross_age_le_slow";
inline constexpr const char* kLongCrossPredicate =
    "predicate::ema_pair_h16s_h256s:cross_age_le_fast";
inline constexpr const char* kCampaignAgePredicate =
    "predicate::m0::campaign_age_gt_control_duration";

enum class TriState : std::int8_t {
    Unobserved = -1,
    False = 0,
    True = 1,
};

struct Literal {
    std::string predicate;
    bool negated{false};
};

struct Clause {
    std::vector<Literal> literals;
};

struct Rule {
    std::string action;
    std::vector<Clause> clauses;
};

struct Policy {
    std::string identity;
    std::string schema_version;
    std::string file_sha256;
    std::string expected_file_sha256;
    std::string side;
    std::string default_action;
    std::string buy_selection;
    bool action_authorized{false};
    bool live_authorized{false};
    std::vector<std::string> predicate_columns;
    std::vector<Rule> rules;
};

struct CaseInput {
    std::string case_id;
    std::string snapshot_id;
    std::string feature_side;
    std::string m0_side;
    std::string role_at_fill;
    std::int64_t baseline_duration_ms{kMinimumControlDurationMs};
    std::int64_t snapshot_baseline_duration_ms{kMinimumControlDurationMs};
    bool policy_input_valid{false};
    std::string feature_block;
    bool support_valid{false};
    bool channel_support_valid{false};
    std::string snapshot_fallback_reason;
    std::map<std::string, TriState> predicates;
};

struct Decision {
    std::string case_id;
    std::string snapshot_id;
    std::string action_id{kControlAction};
    std::int64_t duration_ms{kMinimumControlDurationMs};
    std::optional<std::size_t> matched_rule_index;
    bool support_valid{false};
    std::string policy_sha256;
    std::string fallback_reason;
};

struct StreamingPairState {
    int effective_sign{0};
    std::optional<std::int64_t> arrangement_start_ts_ns;
    std::optional<std::int64_t> last_cross_ts_ns;
    int last_cross_direction{0};
};

struct StreamingObservation {
    std::string case_id;
    std::string snapshot_id;
    std::int64_t left_ts_ns{0};
    std::int64_t right_ts_ns{0};
    std::int64_t feature_ready_ts_ns{0};
    std::int64_t decision_ts_ns{0};
    std::int64_t market_generation{0};
    std::int64_t depth_generation{0};
    std::optional<double> mid_usdc_per_btc;
    bool source_gap{false};
    bool source_stale{false};
    std::string feature_side;
    std::string m0_side;
    std::string role_at_fill;
    std::int64_t baseline_duration_ms{kMinimumControlDurationMs};
    std::int64_t snapshot_baseline_duration_ms{kMinimumControlDurationMs};
    std::optional<double> campaign_age_s;
    bool policy_input_valid{false};
    std::string feature_block;
};

struct StreamingCheckpoint {
    bool warmup_admitted{false};
    std::string warmup_identity;
    std::optional<std::int64_t> last_right_ts_ns;
    std::optional<std::int64_t> last_feature_ready_ts_ns;
    std::optional<std::int64_t> last_market_generation;
    std::optional<std::int64_t> last_depth_generation;
    std::size_t window_count{0};
    std::size_t gap_window_count{0};
    bool ema_initialized{false};
    bool current_window_observed{false};
    std::optional<std::int64_t> last_observed_ts_ns;
    std::array<double, 3> ema{0.0, 0.0, 0.0};
    StreamingPairState short_pair;
    StreamingPairState long_pair;
};

struct StreamingSnapshot {
    std::string case_id;
    std::string snapshot_id;
    std::int64_t right_ts_ns{0};
    std::int64_t feature_ready_ts_ns{0};
    std::int64_t decision_ts_ns{0};
    std::int64_t market_generation{0};
    std::int64_t depth_generation{0};
    std::size_t window_count{0};
    std::size_t gap_window_count{0};
    bool current_window_observed{false};
    bool warmup_admitted{false};
    bool support_valid{false};
    std::optional<std::int64_t> last_observed_ts_ns;
    std::array<std::optional<double>, 3> ema;
    StreamingPairState short_pair;
    StreamingPairState long_pair;
    std::optional<double> short_cross_age_s;
    std::optional<double> long_cross_age_s;
    std::map<std::string, TriState> predicates;
    Decision decision;
};

class Evaluator;

class MinimalMidStreamingState {
  public:
    MinimalMidStreamingState(bool warmup_admitted, std::string warmup_identity);

    void update(const StreamingObservation& observation);
    [[nodiscard]] StreamingCheckpoint checkpoint() const;
    void restore(const StreamingCheckpoint& checkpoint);
    void reset_unbound();
    [[nodiscard]] StreamingSnapshot materialize(
        const StreamingObservation& observation,
        const Evaluator& evaluator
    ) const;

  private:
    StreamingCheckpoint state_;
};

class Evaluator {
  public:
    explicit Evaluator(Policy policy);

    [[nodiscard]] Decision evaluate(const CaseInput& input) const;
    [[nodiscard]] const std::string& binding_error() const noexcept;

  private:
    Policy policy_;
    std::string binding_error_;
};

int run_cli(std::istream& input, std::ostream& output, std::ostream& error);

}  // namespace narrowgate::f05::parity
