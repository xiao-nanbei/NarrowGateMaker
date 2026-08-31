#pragma once

#include "common.hpp"
#include "quote_core.hpp"

#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace narrowgate_cpp {

inline constexpr std::string_view kF05RepeatedBooleanCooldownAbi =
    "f05_repeated_boolean_cooldown_streaming.v1";
inline constexpr std::string_view kF05BooleanCooldownControlAction =
    "CONTROL_85N";
inline constexpr std::int64_t kF05BooleanCooldownWindowWidthNs = 100'000'000;
inline constexpr std::int64_t kF05BooleanCooldownControlUnitMs = 85'000;

enum class F05TriState : std::int8_t {
  Unobserved = -1,
  False = 0,
  True = 1,
};

enum class F05CooldownFillRole : std::uint8_t {
  Opener = 0,
  Add = 1,
  Reducing = 2,
};

struct F05BooleanLiteral {
  std::size_t predicate_index = 0;
  bool negated = false;
};

struct F05BooleanClause {
  std::vector<F05BooleanLiteral> literals;
};

struct F05BooleanRule {
  std::string action_id;
  std::int64_t duration_ms = 0;
  std::vector<F05BooleanClause> clauses;
};

struct F05BooleanPolicy {
  std::string policy_sha256;
  std::string predicate_bundle_sha256;
  std::vector<std::string> predicate_columns;
  std::vector<F05BooleanRule> rules;
  std::string default_action = std::string(kF05BooleanCooldownControlAction);
};

struct F05RepeatedBooleanCooldownConfig {
  bool parity_qualified = false;
  std::string parity_qualification_sha256;
  std::string qualification_scope = "synthetic_mechanics_only";
  std::string feature_clock_semantics = "receive_time_selected_mid_v1";
  double warmup_s = 2048.0;
  double max_feature_age_s = 5.0;
  F05BooleanPolicy policy;
};

struct F05CooldownWindowObservation {
  std::int64_t left_ts_ns = 0;
  std::int64_t right_ts_ns = 0;
  std::int64_t feature_ready_ts_ns = 0;
  std::int64_t market_generation = 0;
  std::int64_t depth_generation = 0;
  std::optional<double> mid_usdc_per_btc;
  bool source_gap = false;
  bool source_stale = false;
  bool warmup_admitted = false;
  bool channel_support_valid = false;
};

struct F05CooldownWindowTape {
  std::vector<F05CooldownWindowObservation> observations;
  std::string content_sha256;
};

struct F05CooldownFillInput {
  std::string snapshot_id;
  Side side = Side::Buy;
  F05CooldownFillRole role = F05CooldownFillRole::Opener;
  std::int64_t fill_ts_ms = 0;
  std::int64_t decision_ts_ns = 0;
  std::int64_t campaign_id = 0;
  double campaign_age_s = 0.0;
  double inventory_before_fill_btc = 0.0;
  double inventory_after_fill_btc = 0.0;
  double consecutive_units_after = 1.0;
  std::int64_t baseline_duration_ms = kF05BooleanCooldownControlUnitMs;
  bool policy_input_valid = true;
  bool support_valid = true;
  bool channel_support_valid = true;
  std::string snapshot_fallback_reason;
  // Empty means materialize the selected owner mid-EMA predicates. A full
  // vector is an exact, ordered multichannel predicate ABI for later parity.
  std::vector<F05TriState> predicate_values;
};

struct F05CooldownPredicateRow {
  std::int64_t exposure_fill_ordinal = 0;
  std::int64_t fill_ts_ms = 0;
  Side side = Side::Buy;
  std::int64_t campaign_id = 0;
  std::string snapshot_id;
  bool policy_input_valid = true;
  bool support_valid = true;
  bool channel_support_valid = true;
  std::string snapshot_fallback_reason;
  std::vector<F05TriState> predicate_values;
};

void validate_f05_cooldown_predicate_rows(
    const F05RepeatedBooleanCooldownConfig &config,
    const std::vector<F05CooldownPredicateRow> &rows);

struct F05CooldownDecision {
  std::string snapshot_id;
  Side side = Side::Buy;
  F05CooldownFillRole role = F05CooldownFillRole::Opener;
  std::int64_t exposure_fill_ordinal = 0;
  std::int64_t fill_ts_ms = 0;
  std::int64_t campaign_id = 0;
  double consecutive_units_after = 0.0;
  std::string action_id = std::string(kF05BooleanCooldownControlAction);
  std::int64_t baseline_duration_ms = kF05BooleanCooldownControlUnitMs;
  std::int64_t duration_ms = kF05BooleanCooldownControlUnitMs;
  std::int64_t deadline_ts_ms = 0;
  std::int64_t lineage_revision = 0;
  std::optional<std::size_t> matched_rule_index;
  bool support_valid = false;
  bool lineage_applied = false;
  std::string coverage_reason_code;
  std::string fallback_reason;
  std::string policy_sha256;
  std::string predicate_bundle_sha256;
  std::int64_t feature_ready_ts_ns = 0;
  double feature_age_ms = 0.0;
};

struct F05CooldownPairState {
  int effective_sign = 0;
  std::optional<std::int64_t> arrangement_start_ts_ns;
  std::optional<std::int64_t> last_cross_ts_ns;
  int last_cross_direction = 0;
};

struct F05CooldownLineageState {
  bool active = false;
  Side side = Side::Buy;
  std::int64_t revision = 0;
  std::int64_t campaign_id = 0;
  std::int64_t fill_ts_ms = 0;
  std::int64_t deadline_ts_ms = 0;
  double consecutive_units_after = 0.0;
  std::int64_t duration_ms = 0;
  std::string action_id;
  std::string coverage_reason_code;
};

struct F05CooldownRuntimeAudit {
  std::int64_t window_count = 0;
  std::int64_t gap_window_count = 0;
  std::int64_t feature_state_reset_count = 0;
  std::int64_t evaluation_count = 0;
  std::int64_t supported_count = 0;
  std::int64_t fallback_count = 0;
  std::int64_t nonbaseline_count = 0;
  std::int64_t buy_control_count = 0;
  std::int64_t reducing_bypass_count = 0;
  std::int64_t lineage_count = 0;
  std::int64_t lineage_clear_count = 0;
};

struct F05RepeatedBooleanCooldownCheckpoint {
  std::string abi_version;
  std::string parity_qualification_sha256;
  std::string qualification_scope;
  std::string feature_clock_semantics;
  std::string policy_sha256;
  std::string predicate_bundle_sha256;
  double warmup_s = 0.0;
  double max_feature_age_s = 0.0;
  bool warmup_admitted = false;
  std::optional<std::int64_t> warmup_start_right_ts_ns;
  std::optional<std::int64_t> last_right_ts_ns;
  std::optional<std::int64_t> last_feature_ready_ts_ns;
  std::optional<std::int64_t> last_market_generation;
  std::optional<std::int64_t> last_depth_generation;
  bool ema_initialized = false;
  bool current_window_observed = false;
  bool current_channel_support_valid = false;
  std::optional<std::int64_t> last_observed_ts_ns;
  std::array<double, 3> ema{0.0, 0.0, 0.0};
  F05CooldownPairState short_pair;
  F05CooldownPairState long_pair;
  F05CooldownLineageState buy_lineage;
  F05CooldownLineageState sell_lineage{false, Side::Sell};
  F05CooldownRuntimeAudit audit;
  std::string canonical_payload;
  std::string checkpoint_sha256;
};

class F05RepeatedBooleanCooldownRuntime {
public:
  explicit F05RepeatedBooleanCooldownRuntime(
      F05RepeatedBooleanCooldownConfig config);

  void update_window(const F05CooldownWindowObservation &observation);
  [[nodiscard]] F05CooldownDecision
  apply_fill(const F05CooldownFillInput &input);
  void override_active_lineage_duration(
      Side side,
      std::int64_t fill_ts_ms,
      std::int64_t campaign_id,
      std::int64_t duration_ms,
      std::string action_id,
      std::string coverage_reason_code);
  void advance_time(std::int64_t now_ms);
  [[nodiscard]] bool add_blocked(Side side, std::int64_t now_ms) const;
  [[nodiscard]] F05CooldownLineageState lineage(Side side) const;
  [[nodiscard]] F05CooldownRuntimeAudit audit() const noexcept;
  [[nodiscard]] F05RepeatedBooleanCooldownCheckpoint checkpoint() const;
  void restore(const F05RepeatedBooleanCooldownCheckpoint &checkpoint);
  [[nodiscard]] bool parity_qualified() const noexcept;
  [[nodiscard]] const std::string &binding_error() const noexcept;
  [[nodiscard]] const F05RepeatedBooleanCooldownConfig &config() const noexcept;

private:
  F05RepeatedBooleanCooldownConfig config_;
  std::string binding_error_;
  bool warmup_admitted_ = false;
  std::optional<std::int64_t> warmup_start_right_ts_ns_;
  std::optional<std::int64_t> last_right_ts_ns_;
  std::optional<std::int64_t> last_feature_ready_ts_ns_;
  std::optional<std::int64_t> last_market_generation_;
  std::optional<std::int64_t> last_depth_generation_;
  bool ema_initialized_ = false;
  bool current_window_observed_ = false;
  bool current_channel_support_valid_ = false;
  std::optional<std::int64_t> last_observed_ts_ns_;
  std::array<double, 3> ema_{0.0, 0.0, 0.0};
  F05CooldownPairState short_pair_;
  F05CooldownPairState long_pair_;
  F05CooldownLineageState buy_lineage_;
  F05CooldownLineageState sell_lineage_{false, Side::Sell};
  F05CooldownRuntimeAudit audit_;
};

enum class OrderState : std::uint8_t {
    PendingNew = 0,
    Open = 1,
    PendingCancel = 2,
};

enum class BiasSide : std::uint8_t {
    Balanced = 0,
    Buy = 1,
    Sell = 2,
};

enum class TraceOutcome : std::uint8_t {
    None = 0,
    Cancel = 1,
    Fill = 2,
    OpenEnd = 3,
};

enum class CancelReason : std::uint8_t {
    None = 0,
    Requote = 1,
    FragileTtl = 2,
    Fill = 3,
    EndOfWindow = 4,
    InventoryLimit = 5,
    PositionTimeout = 6,
    StaleBook = 7,
    EmergencyTakerClose = 8,
    RequoteReplace = 9,
    FillCooldown = 10,
    CircuitBreaker = 11,
    SideDisabled = 12,
    CircuitBreakerCloseRequote = 13,
    IocFill = 14,
    ConsecutiveLossCooldown = 15,
    SyncAdjustDegrade = 16,
    PlannedMaintenance = 17,
    CooldownDurationWashout = 18,
    DailyLoss = 19,
    PositionValue = 20,
    EmergencyDrawdown = 21,
};

[[nodiscard]] std::int64_t sample_keyed_latency_ms(
    std::int64_t base_ms,
    std::int64_t jitter_ms,
    const std::vector<double>& samples,
    std::int64_t seed,
    std::int64_t event_ts_ms,
    bool is_buy,
    std::uint64_t operation,
    std::int64_t order_ts_ms,
    bool stress_enabled = false,
    double stress_spike_probability = 0.0,
    double stress_spike_multiplier = 1.0
);

[[nodiscard]] double sample_keyed_random_passive_unit(
    std::int64_t seed,
    std::int64_t event_ts_ms,
    std::int64_t action_identity,
    std::uint64_t operation
);

[[nodiscard]] constexpr std::string_view bias_side_name(BiasSide side) noexcept {
    switch (side) {
        case BiasSide::Buy: return "BUY";
        case BiasSide::Sell: return "SELL";
        default: return "balanced";
    }
}

[[nodiscard]] constexpr std::string_view trace_outcome_name(TraceOutcome outcome) noexcept {
    switch (outcome) {
        case TraceOutcome::Cancel: return "cancel";
        case TraceOutcome::Fill: return "fill";
        case TraceOutcome::OpenEnd: return "open_end";
        default: return "";
    }
}

[[nodiscard]] constexpr std::string_view cancel_reason_name(CancelReason reason) noexcept {
    switch (reason) {
        case CancelReason::Requote: return "requote";
        case CancelReason::FragileTtl: return "fragile_ttl";
        case CancelReason::Fill: return "fill";
        case CancelReason::EndOfWindow: return "end_of_window";
        case CancelReason::InventoryLimit: return "inventory_limit";
        case CancelReason::PositionTimeout: return "position_timeout";
        case CancelReason::StaleBook: return "stale_book";
        case CancelReason::EmergencyTakerClose: return "emergency_taker_close";
        case CancelReason::RequoteReplace: return "requote_replace";
        case CancelReason::FillCooldown: return "fill_cooldown";
        case CancelReason::DailyLoss: return "daily_loss";
        case CancelReason::PositionValue: return "position_value";
        case CancelReason::EmergencyDrawdown: return "emergency_drawdown";
        case CancelReason::CircuitBreaker: return "circuit_breaker";
        case CancelReason::SideDisabled: return "side_disabled";
        case CancelReason::CircuitBreakerCloseRequote:
            return "circuit_breaker_close_requote";
        case CancelReason::IocFill: return "ioc_fill";
        case CancelReason::ConsecutiveLossCooldown:
            return "consecutive_loss_cooldown";
        case CancelReason::SyncAdjustDegrade: return "sync_adjust_degrade";
        case CancelReason::PlannedMaintenance: return "planned_maintenance";
        case CancelReason::CooldownDurationWashout:
            return "cooldown_duration_joint_washout";
        default: return "";
    }
}

struct TickReplayInput {
    // 所有输入都是对 Python numpy/array buffer 的只读视图，不拥有内存。
    // simulate_tick_arrays() 只能在一次调用内使用这些 View，不能缓存到全局或异步线程。
    ArrayView<std::int64_t> trade_ts_ms;
    ArrayView<double> trade_price;
    ArrayView<double> trade_qty;
    ArrayView<std::uint8_t> is_buyer_maker;

    ArrayView<std::int64_t> var_ts_ms;
    ArrayView<double> var_ssq;
    ArrayView<double> var_ti;
    ArrayView<double> var_retsq;

    ArrayView<std::int64_t> ml_ts_ms;
    ArrayView<double> ml_dir_10s;
    ArrayView<double> ml_vol_10s;
    ArrayView<double> ml_ret_10s;
    ArrayView<double> ml_tox_bid;
    ArrayView<double> ml_tox_ask;
    // Optional causal 10-second conditional-P3 overlay.  The replay keeps the
    // static QuoteCoreConfig values until the first ready row, then holds the
    // latest row exactly like the 10-second ML prediction surface.
    ArrayView<std::int64_t> p3_ts_ms;
    ArrayView<double> p3_delta_star;
    ArrayView<double> p3_kappa_eff;
    // Per-ready-ML-row contribution from scorer features that are not
    // overwritten by the current quote context. Python compiles categorical
    // values and arbitrary Prediction.feature_dict fields once; C++ combines
    // them with dynamic quote-time fields without carrying Python objects.
    MatrixView<double> buy_fill_static_logit_delta;
    MatrixView<double> buy_fill_static_missing;
    MatrixView<double> buy_fill_static_used;
    // Optional action-bound conditional-P3 reach gate. Rows align exactly to
    // ml_ts_ms. Columns are four side/role blocks (BUY opener, BUY add,
    // SELL opener, SELL add), each indexed by executable distance ticks.
    // Values are 0=unsupported, 1=supported/gate-fail, 2=gate-pass.
    ArrayView<std::int64_t> p3_reach_gate_ts_ms;
    MatrixView<std::uint8_t> p3_reach_gate_status;
    // Frozen owner-route adaptive reach-budget selection. Rows align exactly
    // to canonical 10-second ml_ts_ms. Columns are four equal distance-grid
    // 1180-column blocks (BUY opener/add, SELL opener/add), matching the
    // fixed-gate ABI and its five-tick baseline-distance origin.
    // Values are 0=no action, 1..16=outward tick penalty, 255=unsupported.
    // The matrix owns P3/band selection; side-specific toxicity p90 remains a
    // separate causal trigger.
    ArrayView<std::int64_t> p3_reach_budget_ts_ms;
    MatrixView<std::uint8_t> p3_reach_budget_selected_k;

    // Causal cross-venue fair-price tape. The feature-ready clock is the only
    // action clock; C++ recomputes the center displacement from the current
    // decision mid so control and candidate share the same local quote state.
    ArrayView<std::int64_t> fair_center_ts_ms;
    ArrayView<double> fair_center_price;
    ArrayView<double> fair_center_gain;
    ArrayView<std::uint8_t> fair_center_valid;

    ArrayView<std::int64_t> bbo_ts_ms;
    ArrayView<double> bbo_best_bid;
    ArrayView<double> bbo_best_ask;
    ArrayView<double> bbo_bid_qty;
    ArrayView<double> bbo_ask_qty;

    ArrayView<std::int64_t> l2_ts_ms;
    MatrixView<double> l2_bid_px;
    MatrixView<double> l2_bid_qty;
    MatrixView<double> l2_ask_px;
    MatrixView<double> l2_ask_qty;

    ArrayView<double> queue_base_by_trade;
    ArrayView<double> queue_decay_by_trade;
    ArrayView<double> buy_fill_prob_by_trade;
    ArrayView<double> sell_fill_prob_by_trade;
    ArrayView<double> buy_queue_deplete_mult_by_trade;
    ArrayView<double> sell_queue_deplete_mult_by_trade;

    void validate() const;
};

struct TickReplayParams {
    QuoteCoreConfig quote;
    double order_size = 0.001;
    double max_inventory = 0.01;
    double eta = 0.0;
    bool symmetric_size = false;
    double requote_interval_s = 1.0;
    double rq_min_s = 0.0;
    double rq_max_s = 0.0;
    bool requote_clock_fixed = false;
    bool empirical_requote_clock = false;
    double maker_fee = 0.0;
    double taker_fee = 0.0;
    double queue_base = 0.0;
    double queue_decay = 0.0;
    // Deprecated ABI field. Executable price identity and queue matching use
    // integer tick indices; this value no longer defines an exchange level.
    double queue_price_tolerance = 0.051;
    double maker_fill_prob = 1.0;
    double buy_fill_prob = 1.0;
    double sell_fill_prob = 1.0;
    double queue_ahead_base_mult = 1.0;
    double queue_deplete_base_mult = 1.0;
    bool queue_l2_cancel_ahead_enabled = false;
    double queue_ahead_buy_exposure_mult = 1.0;
    double queue_ahead_buy_reducing_mult = 1.0;
    double queue_ahead_sell_exposure_mult = 1.0;
    double queue_ahead_sell_reducing_mult = 1.0;
    std::vector<double> queue_regime_distance_edges;
    std::vector<double> queue_regime_rank_edges;
    std::vector<double> queue_regime_buy_mult;
    std::vector<double> queue_regime_sell_mult;
    std::vector<double> queue_deplete_rank_edges;
    std::vector<double> queue_deplete_buy_mult;
    std::vector<double> queue_deplete_sell_mult;
    std::vector<double> queue_mo_edges;
    std::vector<double> queue_mo_buy_mult;
    std::vector<double> queue_mo_sell_mult;
    double requote_threshold_bps = 0.0;
    double replace_min_price_change_ticks = 0.0;
    double replace_min_price_change_ticks_reducing = 0.0;
    double replace_min_interval_ms = 0.0;
    double replace_min_interval_ms_reducing = 0.0;
    bool replace_pending_coalesce = false;
    bool replace_cancel_first_exposure_increasing = false;
    std::int64_t new_order_latency_ms = 0;
    std::int64_t cancel_order_latency_ms = 0;
    std::int64_t latency_jitter_ms = 0;
    std::int64_t decision_to_gateway_latency_seed = -1;
    std::vector<double> decision_to_gateway_latency_samples_ms;
    std::vector<double> new_order_latency_samples_ms;
    std::vector<double> new_order_exchange_effective_latency_samples_ms;
    std::vector<double> cancel_order_latency_samples_ms;
    std::vector<double> cancel_exchange_effective_latency_samples_ms;
    std::vector<double> cancel_ack_visibility_latency_samples_ms;
    std::vector<double> exec_book_visibility_delay_samples_ms;
    double exec_book_visibility_delay_mean_ms = 0.0;
    double exec_book_visibility_delay_jitter_ms = 0.0;
    std::int64_t exec_book_visibility_delay_seed = 20260718;
    std::int64_t max_exec_book_age_ms = 5000;
    double ber_guard_thresh = 0.0;
    bool ber_exposure_add_only = false;
    double fill_cooldown_s = 0.0;
    bool fill_cooldown_apply_reducing = false;
    // Empty preserves the frozen legacy boolean ABI. New formal replay must
    // provide one of the two explicit policy names.
    std::string fill_cooldown_consecutive_reset_policy;
    bool fill_cooldown_reset_consec_on_expiry = true;
    double fill_cooldown_reducing_s = 0.0;
    bool fill_cooldown_reducing_campaign_only = false;
    double fill_cooldown_reducing_inv_threshold = 0.0;
    double fill_cooldown_reducing_inv_ratio = 0.0;
    double fill_cooldown_reducing_age_s = 0.0;
    double fill_cooldown_reducing_vol_ref = 0.0;
    double fill_cooldown_reducing_vol_min_mult = 0.5;
    double fill_cooldown_reducing_vol_max_mult = 2.0;
    int max_consecutive_losses = 0;
    double cooldown_after_loss_s = 0.0;
    std::string consecutive_loss_cooldown_semantics;
    bool consecutive_loss_snapshot_enabled = false;
    std::string consecutive_loss_snapshot_schema;
    double initial_loss_open_commission = 0.0;
    double initial_loss_round_trip_pnl = 0.0;
    int initial_loss_consecutive_losses = 0;
    std::int64_t initial_loss_cooldown_until_ms = 0;
    std::int64_t initial_loss_last_cancel_ts_ms = -1;
    bool initial_loss_threshold_pending = false;
    std::int64_t initial_loss_trigger_count = 0;
    std::int64_t initial_loss_expiry_count = 0;
    std::int64_t initial_loss_losing_round_trips = 0;
    std::int64_t initial_loss_winning_or_flat_round_trips = 0;
    int initial_loss_max_observed_consecutive_losses = 0;
    bool sync_adjust_degrade_enabled = false;
    double sync_adjust_pause_s = 0.0;
    bool sync_adjust_cancel_orders = true;
    std::string sync_adjust_replay_mode = "disabled";
    std::string sync_adjust_semantics;
    double thin_depth_threshold = 0.0;
    // Match live SignalEngine._snapshot_l2_state top-three near depth.
    int l2_refill_cancel_near_levels = 3;
    // The strategy feed may carry top-20, while the live policy state currently
    // aggregates the first ten levels. Python passes the active definition.
    int l2_policy_depth_levels = 10;
    double l2_refill_cancel_lookback_s = 10.0;
    int markout_ema_span_fills = 0;
    double markout_horizon_s = 10.0;
    bool adverse_markout_pause_hybrid = false;
    double adverse_markout_pause_base_s = 120.0;
    double adverse_markout_pause_min_s = 120.0;
    double adverse_markout_pause_max_s = 900.0;
    double adverse_markout_decay_tau_s = 0.0;
    double adverse_markout_max_resolve_gap_s = 30.0;
    bool use_bar_pricing = true;
    int ret_demean_halflife = 0;
    double initial_inventory = 0.0;
    double max_daily_loss = std::numeric_limits<double>::infinity();
    double max_position_value = std::numeric_limits<double>::infinity();
    double emergency_close_dd = std::numeric_limits<double>::infinity();
    bool initial_risk_state_enabled = false;
    std::int64_t initial_risk_utc_day = 0;
    double initial_risk_day_start_total_pnl = 0.0;
    double initial_risk_session_peak_pnl = 0.0;
    double initial_risk_last_total_pnl = 0.0;
    double initial_risk_total_pnl_offset = 0.0;
    double initial_entry_price = 0.0;
    // Stop submitting quotes at the first replay event on or after this
    // timestamp, cancel every live order with the frozen latency sampler, and
    // keep processing the tape so ACK-before-gap fills remain observable.
    std::int64_t planned_quote_stop_ts_ms = 0;
    double initial_sigma_sq = 1.0;
    std::int64_t rng_seed = 42;
    std::int64_t latency_seed = -1;
    std::string replay_contract_sha256;
    std::string latency_sampler_version = "keyed_splitmix64_v1";
    bool latency_stress_enabled = false;
    double latency_stress_spike_probability = 0.0;
    double latency_stress_spike_multiplier = 1.0;
    bool random_passive_enabled = false;
    std::int64_t random_passive_seed = 10045;
    double random_passive_side_mirror_prob = 0.5;
    double random_passive_timing_jitter_fraction = 0.35;
    bool random_passive_preserve_inventory_skew = true;
    bool fixed_spread_probe_enabled = false;
    double fixed_spread_probe_ticks = 0.0;
    bool paired_fixed_spread_probe_enabled = false;
    std::vector<double> paired_fixed_spread_probe_ticks;
    bool paired_fixed_spread_fail_on_violation = true;
    std::int64_t paired_fixed_spread_max_recorded_violations = 100;

    bool local_extreme_guard_enabled = false;
    double local_extreme_window_s = 120.0;
    double local_extreme_rank_threshold = 0.80;
    bool local_extreme_require_thin_depth = true;
    double local_extreme_thin_depth_threshold = 0.0;
    double local_extreme_spread_mult = 1.0;
    bool local_extreme_pause = false;
    double fragile_order_ttl_s = 0.0;

    bool adaptive_add_cooldown_enabled = false;
    double adaptive_add_cooldown_min_mult = 0.5;
    double adaptive_add_cooldown_max_mult = 2.5;
    double adaptive_add_cooldown_w_markout = 0.0;
    double adaptive_add_cooldown_w_flow = 0.0;
    double adaptive_add_cooldown_w_campaign = 0.0;
    double adaptive_add_cooldown_w_trend = 0.0;
    double adaptive_add_cooldown_w_refill_weak = 0.0;
    double adaptive_add_cooldown_w_refill_good = 0.0;
    double adaptive_add_cooldown_w_reversion = 0.0;
    double adaptive_add_cooldown_mo_ref = 50.0;
    double adaptive_add_cooldown_flow_ref = 2.0;
    double adaptive_add_cooldown_campaign_inv_ref = 0.006;
    double adaptive_add_cooldown_campaign_age_ref_s = 3600.0;
    double adaptive_add_cooldown_trend_ret_ref = 0.00002;
    double adaptive_add_cooldown_refill_ref = 0.10;
    double adaptive_add_cooldown_reversion_ref = 1.0;
    bool adaptive_add_cooldown_gate_enabled = false;
    double adaptive_add_cooldown_gate_mult = 1.75;
    double adaptive_add_cooldown_gate_campaign_score = 1.0;
    double adaptive_add_cooldown_gate_trend_score = 1.0;
    double adaptive_add_cooldown_gate_refill_edge_max = 0.0;
    double adaptive_add_cooldown_gate_reversion_max = 0.5;
    std::string adaptive_add_cooldown_gate_side = "BOTH";

    double flat_unilateral_max_s = 0.0;

    bool campaign_stop_add_enabled = false;
    double campaign_stop_add_inv_threshold = 0.0;
    double campaign_stop_add_age_s = 0.0;
    bool campaign_soft_control_enabled = false;
    double campaign_soft_inv_threshold = 0.0;
    double campaign_soft_age_s = 0.0;
    double campaign_soft_spread_mult = 1.0;
    bool campaign_soft_gate_enabled = false;
    double campaign_soft_gate_campaign_inv_ref = 0.006;
    double campaign_soft_gate_campaign_age_ref_s = 3600.0;
    double campaign_soft_gate_trend_ret_ref = 0.00002;
    double campaign_soft_gate_refill_ref = 0.10;
    double campaign_soft_gate_campaign_score = 1.0;
    double campaign_soft_gate_trend_score = 1.0;
    double campaign_soft_gate_refill_edge_max = 0.0;
    double campaign_soft_gate_reversion_max = 0.5;
    std::string campaign_soft_gate_side = "BOTH";

    double position_timeout_s = 0.0;
    double circuit_breaker_sigma = 0.0;
    bool circuit_breaker_maker_close = true;
    bool emergency_taker_close_enabled = false;

    struct FillSelectionFoldModel {
        double base_logit = 0.0;
        double contribution_scale = 0.35;
        std::unordered_map<std::string, std::vector<double>> numeric_cuts;
        std::unordered_map<std::string, std::unordered_map<std::string, double>> contributions;
        std::vector<std::string> categorical_features;
    };
    bool buy_fill_selection_live_enabled = false;
    double buy_fill_selection_live_score_threshold = 0.50;
    double buy_fill_selection_live_spread_mult_cap = 1.0;
    bool buy_fill_selection_live_apply_reducing = false;
    int buy_fill_selection_live_max_missing_features = 99;
    std::vector<FillSelectionFoldModel> buy_fill_selection_models;

    bool buy_soft_widen_release_probe_enabled = false;
    bool buy_soft_widen_release_probe_apply_candidate = true;
    std::int64_t buy_soft_widen_release_target_ts_ms = 0;
    std::string buy_soft_widen_release_target_role;
    double buy_soft_widen_release_spread_mult_cap = 1.0;

    bool conditional_p3_reach_gate_enabled = false;
    int conditional_p3_reach_gate_outward_ticks = 16;
    int conditional_p3_reach_gate_grid_min_ticks = 5;
    double conditional_p3_reach_gate_buy_toxicity_threshold = 2.0;
    double conditional_p3_reach_gate_sell_toxicity_threshold = 2.0;
    bool conditional_p3_reach_budget_policy_enabled = false;
    int conditional_p3_reach_budget_grid_min_ticks = 5;
    double conditional_p3_reach_budget_buy_toxicity_threshold = 1.0;
    double conditional_p3_reach_budget_sell_toxicity_threshold = 1.0;

    bool cross_venue_fair_center_shift_enabled = false;
    double cross_venue_fair_center_max_state_age_ms = 2000.0;

    std::int64_t trace_fills_max = 0;
    std::int64_t trace_quotes_max = 0;
    std::int64_t trace_p3_reach_decisions_max = 0;
    std::int64_t trace_cooldown_duration_opportunities_max = 0;
    bool cooldown_duration_fork_enabled = false;
    std::string cooldown_duration_fork_action;
    std::int64_t cooldown_duration_fork_target_ordinal = 0;
    std::int64_t cooldown_duration_fork_target_ts_ms = 0;
    std::string cooldown_duration_fork_target_side;
    std::int64_t cooldown_duration_fork_target_order_id = -1;
    std::int64_t cooldown_duration_fork_target_campaign_id = 0;
    double cooldown_duration_fork_expected_baseline_ms = 0.0;
    double cooldown_duration_fork_fixed_ms = 0.0;
    bool cooldown_duration_fork_baseline_policy_enabled = false;
    std::string cooldown_duration_fork_expected_owner_action;
    std::string cooldown_duration_fork_expected_owner_policy_sha256;
    std::shared_ptr<F05RepeatedBooleanCooldownRuntime>
        f05_repeated_cooldown_runtime;
    std::vector<F05CooldownWindowObservation> f05_cooldown_window_tape;
    std::shared_ptr<F05CooldownWindowTape> f05_cooldown_window_tape_shared;
    std::vector<F05CooldownPredicateRow> f05_cooldown_predicate_rows;
    std::int64_t trace_window_ms = 10'000;
    // 大 sweep 通常只需要 summary；关闭曲线可以避免 PnL/inventory path 占用内存和 Python 转换时间。
    bool collect_curves = true;
};

// TraceOrderRow 是冷诊断对象：字段很多，只应在 trace_*_max > 0 时物化。
// 热循环里的订单状态请放 ReplayOrder，避免每个订单都把诊断字段带进 cache。
struct TraceOrderRow {
    std::int64_t order_id = -1;
    Side side = Side::Buy;
    std::int64_t submit_ts = 0;
    std::int64_t activate_ts = 0;
    std::int64_t quote_ts = 0;
    double price = 0.0;
    double quantity = 0.0;
    double raw_half_spread = 0.0;
    double capped_half_spread = 0.0;
    double raw_mid_shift = 0.0;
    double raw_reservation_shift = 0.0;
    double raw_asym_shift = 0.0;
    double asym = 0.0;
    double inventory = 0.0;
    double dir_signal = 0.0;
    double pred_dir = 0.5;
    double pred_ret = 0.0;
    double tox_bid = 0.5;
    double tox_ask = 0.5;
    double book_imb = 0.0;
    double microprice_shift_bps = 0.0;
    double near_depth_total = 0.0;
    double l2_near_depth_total = 0.0;
    double l2_quote_flip_rate = 0.0;
    double l2_book_refresh_ratio = 0.0;
    double l2_book_cancel_ratio = 0.0;
    double mo_ema_bid = 0.0;
    double mo_ema_ask = 0.0;
    double fair = 0.0;
    double mid = 0.0;
    double best_bid = 0.0;
    double best_ask = 0.0;
    double raw_pair_spread = 0.0;
    double capped_pair_spread = 0.0;
    double final_pair_spread = 0.0;
    double raw_price = 0.0;
    double pre_guard_price = 0.0;
    double final_price = 0.0;
    double raw_quote_delta_to_bbo = 0.0;
    double pre_guard_delta_to_bbo = 0.0;
    double final_quote_delta_to_bbo = 0.0;
    double raw_distance_to_mid = 0.0;
    double final_distance_to_mid = 0.0;
    double raw_quote_skew = 0.0;
    double final_quote_skew = 0.0;
    BiasSide raw_bias_side = BiasSide::Balanced;
    BiasSide final_bias_side = BiasSide::Balanced;
    bool favored_by_raw_shift = false;
    bool delta_cap = false;
    bool mid_guard = false;
    bool post_only = false;
    bool side_adverse = false;
    bool side_adverse_pause = false;
    bool adverse_toxicity = false;
    bool adverse_markout = false;
    bool adverse_direction = false;
    bool adverse_ret = false;
    bool adverse_microprice = false;
    bool adverse_thin_depth = false;
    bool local_extreme_guard = false;
    bool local_extreme_pause = false;
    double local_extreme_rank = 0.5;
    double local_extreme_window_s = 0.0;
    bool defense_guard = false;
    bool defense_pause = false;
    bool defense_reducing = false;
    bool defense_emergency = false;
    bool defense_markout = false;
    bool defense_direction = false;
    bool defense_ret = false;
    bool defense_microprice = false;
    double defense_spread_mult = 1.0;
    bool final_compressed = false;
    bool bid_adverse = false;
    bool ask_adverse = false;
    double buy_fill_selection_live_score = 0.0;
    bool buy_fill_selection_live_hit = false;
    int buy_fill_selection_live_missing_features = 0;
    bool random_passive_mirrored = false;
    bool final_guard_changed = false;
    bool any_constraint_changed = false;
    TraceOutcome outcome = TraceOutcome::None;
    std::int64_t outcome_ts = 0;
    std::int64_t lifetime_ms = 0;
    CancelReason cancel_reason = CancelReason::None;
    double fill_qty = 0.0;
    double remaining = 0.0;
    double queue_init = 0.0;
    double queue_left = 0.0;
    bool pending_cancel = false;
};

struct TraceFillRow {
    // Zero-based, result-local execution order.  Unlike timestamps or order
    // identifiers this is assigned only when the physical fill is appended.
    std::int64_t fill_sequence = -1;
    Side side = Side::Buy;
    std::int64_t fill_ts = 0;
    std::int64_t quote_ts = 0;
    std::int64_t age_ms = 0;
    double quote_mid = 0.0;
    double quote_px = 0.0;
    double fill_trade_px = 0.0;
    double quote_dist = 0.0;
    double quote_window_extreme = 0.0;
    double quote_window_move = 0.0;
    double window5_min = 0.0;
    double window5_max = 0.0;
    double window10_min = 0.0;
    double window10_max = 0.0;
    double window120_min = 0.0;
    double window120_max = 0.0;
    double window120_rank = 0.5;
    double move_from_quote_mid_to_fill = 0.0;
    double queue_init = 0.0;
    double queue_before = 0.0;
    double rem_before = 0.0;
    double fill_qty = 0.0;
    double fill_fee_rate = 0.0;
    double fill_fee_usdc = 0.0;
    double inventory_before_fill = 0.0;
    double inventory_after_fill = 0.0;
    double markout_1s = 0.0;
    double markout_5s = 0.0;
    double markout_20s = 0.0;
    double markout_30s = 0.0;
    double ev_1s = 0.0;
    double ev_5s = 0.0;
    double ev_20s = 0.0;
    double ev_30s = 0.0;
    bool toxic_1s = false;
    bool toxic_5s = false;
    bool toxic_20s = false;
    bool toxic_30s = false;
    TraceOrderRow order;
};

struct P3ReachDecisionRow {
    std::int64_t decision_ts_ms = 0;
    std::int64_t prediction_ts_ms = 0;
    Side side = Side::Buy;
    bool opener = true;
    bool exposure_increasing = false;
    bool baseline_eligible = false;
    double toxicity_score = 0.0;
    double toxicity_threshold = 0.0;
    double best_bid = 0.0;
    double best_ask = 0.0;
    double inventory_btc = 0.0;
    double baseline_price = 0.0;
    double candidate_price = 0.0;
    double side_policy_spread_mult = 1.0;
    bool side_policy_allow_post = true;
    bool side_policy_allow_exposure_increase = true;
    std::uint32_t side_policy_reason_mask = 0;
    std::int64_t baseline_distance_ticks = 0;
    std::uint8_t reach_gate_status = 0;
    bool reach_gate_requested = false;
    bool price_changed = false;
    bool spread_cap_noop = false;
};

struct CooldownDurationOpportunityRow {
    std::string schema_version =
        "multiscale_ema_boolean_cooldown_duration_opportunity.v1";
    std::string fill_clock_semantics =
        "native_exchange_event_revealed_at_replay_event_clock_"
        "no_live_receive_time_claim";
    bool live_receive_time_authority = false;
    std::int64_t exposure_fill_ordinal = 0;
    std::int64_t fill_visible_ts_ms = 0;
    std::int64_t fill_exchange_ts_ms = 0;
    Side side = Side::Buy;
    bool opener = true;
    std::int64_t order_id = -1;
    std::int64_t campaign_id = 0;
    double inventory_before_fill_btc = 0.0;
    double inventory_after_fill_btc = 0.0;
    double fill_qty_btc = 0.0;
    double unit_qty_btc = 0.0;
    double consecutive_units_before = 0.0;
    double consecutive_units_after = 0.0;
    std::int64_t prior_deadline_ts_ms = 0;
    double baseline_duration_ms = 0.0;
    std::int64_t baseline_deadline_ts_ms = 0;
    double canonical_mid = 0.0;
    double best_bid = 0.0;
    double best_ask = 0.0;
    std::int64_t decision_visible_bbo_index = -1;
    std::int64_t decision_visible_l2_index = -1;
    std::int64_t market_event_index = -1;
    double assignment_equity_usdc = 0.0;
};

struct CooldownDurationFillPathRow {
    std::int64_t path_fill_ordinal = 0;
    std::int64_t fill_visible_ts_ms = 0;
    Side side = Side::Buy;
    std::int64_t order_id = -1;
    std::int64_t campaign_id = 0;
    bool exposure_increasing = false;
    bool target_fill = false;
    double fill_price_usdc_per_btc = 0.0;
    double fill_qty_btc = 0.0;
    double inventory_before_fill_btc = 0.0;
    double inventory_after_fill_btc = 0.0;
    double cash_after_fill_usdc = 0.0;
    double baseline_duration_ms = 0.0;
    double applied_duration_ms = 0.0;
    std::int64_t applied_deadline_ts_ms = 0;
};

struct CooldownDurationForkTrace {
    bool enabled = false;
    std::string schema_version =
        "multiscale_ema_boolean_cooldown_duration_fork_trace.v2";
    std::string action;
    Side side = Side::Buy;
    std::int64_t campaign_id = 0;
    std::int64_t target_exposure_fill_ordinal = 0;
    std::int64_t target_order_id = -1;
    std::int64_t assignment_ts_ms = 0;
    double assignment_inventory_btc = 0.0;
    double assignment_equity_usdc = 0.0;
    double baseline_duration_ms = 0.0;
    double applied_duration_ms = 0.0;
    std::int64_t applied_deadline_ts_ms = 0;
    bool exact_owner_baseline_policy_enabled = false;
    std::string exact_owner_action;
    std::string exact_owner_policy_sha256;
    double exact_owner_baseline_duration_ms = 0.0;
    bool quarantine_entered = false;
    std::int64_t quarantine_ts_ms = 0;
    std::string washout_protocol =
        "first_flat_exposure_quarantine_scheduler_drained_v2";
    bool control_path_exact_until_quarantine = true;
    std::int64_t exposure_permission_change_count = 0;
    std::int64_t reducing_permission_control_checks = 0;
    std::int64_t reducing_quote_change_count = 0;
    std::int64_t second_assignment_count = 0;
    bool arm_washout_complete = false;
    std::int64_t terminal_ts_ms = 0;
    std::string terminal_reason = "not_reached";
    bool right_censored = false;
    double terminal_inventory_btc = 0.0;
    double terminal_mid_usdc_per_btc = 0.0;
    double final_cash_usdc = 0.0;
    double final_pnl_usdc = 0.0;
    double accounting_residual_usdc = 0.0;
    std::optional<double> assignment_to_washout_value_usdc;
    std::optional<double> censor_time_mid_mark_usdc;
    std::optional<double> censor_time_executable_mark_usdc;
    bool censor_marks_are_terminal_bounds = false;
    std::int64_t post_assignment_buy_fill_count = 0;
    std::int64_t post_assignment_sell_fill_count = 0;
    double inventory_time_btc_s = 0.0;
    double mae_usdc = 0.0;
    double max_abs_inventory_btc = 0.0;
    std::int64_t active_or_pending_order_count = 0;
    std::int64_t pending_submit_count = 0;
    std::int64_t pending_cancel_count = 0;
    std::int64_t pending_ack_count = 0;
    bool campaign_active = false;
    std::int64_t cursor_owner_count = 0;
    std::int64_t hazard_owner_count = 0;
};

struct PmrTraceDeleter {
    std::pmr::memory_resource* resource = nullptr;

    void operator()(TraceOrderRow* trace) const noexcept {
        if (trace == nullptr || resource == nullptr) {
            return;
        }
        std::destroy_at(trace);
        resource->deallocate(trace, sizeof(TraceOrderRow), alignof(TraceOrderRow));
    }
};

using TraceOrderPtr = std::unique_ptr<TraceOrderRow, PmrTraceDeleter>;

struct ReplayOrder {
    // 热订单状态：撮合、撤单、TTL、queue 只依赖这些字段。
    // trace 是可选冷指针，默认关闭 trace 时不分配 TraceOrderRow。
    Side side = Side::Buy;
    double price = 0.0;
    std::int64_t price_tick = 0;
    double quantity = 0.0;
    double remaining = 0.0;
    double queue_left = 0.0;
    double queue_init = 0.0;
    double queue_visible_prev = -1.0;
    double queue_trade_since_l2 = 0.0;
    std::int64_t queue_l2_seen_idx = -1;
    std::int64_t quote_ts = 0;
    std::int64_t activate_ts = 0;
    std::int64_t new_ack_ts = 0;
    std::int64_t cancel_effective_ts = 0;
    std::int64_t cancel_ack_ts = 0;
    CancelReason pending_cancel_reason = CancelReason::None;
    std::int64_t ttl_ms = 0;
    double mid_at_quote = 0.0;
    OrderState state = OrderState::PendingNew;
    bool side_adverse = false;
    bool defense_guard = false;
    bool local_extreme_guard = false;
    bool final_compressed = false;
    bool reduce_only = false;
    bool circuit_breaker_close = false;
    bool immediate_or_cancel = false;
    bool emergency_market = false;
    bool exchange_accepted = false;
    bool activation_rejected = false;
    bool fixed_spread_probe = false;
    bool fixed_spread_probe_activated = false;
    bool fixed_spread_probe_touched = false;
    bool fixed_spread_probe_filled = false;
    std::uint8_t queue_seed_source = 0;
    TraceOrderPtr trace{nullptr, PmrTraceDeleter{}};
};

struct TickReplaySummary {
    double pnl = 0.0;
    double sharpe = 0.0;
    double max_drawdown = 0.0;
    double cash = 0.0;
    double terminal_mark_price = 0.0;
    double mtm_before_terminal_fee = 0.0;
    double terminal_fee_drag = 0.0;
    double terminal_liquidation_fee_estimate = 0.0;
    double inventory_pnl = 0.0;
    double final_inventory = 0.0;
    double max_abs_inventory = 0.0;
    bool planned_quote_stop_triggered = false;
    std::int64_t planned_quote_stop_trigger_ts_ms = 0;
    std::int64_t planned_shutdown_orders_at_trigger = 0;
    std::int64_t planned_shutdown_open_order_count = 0;
    std::int64_t planned_shutdown_pending_new_order_count = 0;
    std::int64_t planned_shutdown_pending_cancel_order_count = 0;
    std::int64_t fills_bid = 0;
    std::int64_t fills_ask = 0;
    std::int64_t fills_total = 0;
    std::int64_t integer_tick_crossing_recovered_bid_candidates = 0;
    std::int64_t integer_tick_crossing_recovered_ask_candidates = 0;
    std::int64_t integer_tick_crossing_recovered_bid_fills = 0;
    std::int64_t integer_tick_crossing_recovered_ask_fills = 0;
    double avg_markout = 0.0;
    double avg_markout_bid = 0.0;
    double avg_markout_ask = 0.0;
    std::int64_t markout_count = 0;
    double markout_qty_btc = 0.0;
    double markout_qty_bid_btc = 0.0;
    double markout_qty_ask_btc = 0.0;
    std::int64_t n_requotes = 0;
    double avg_rq_ms = 0.0;
    double avg_spread = 0.0;
    double avg_final_spread = 0.0;
    std::int64_t n_final_spread = 0;
    double signed_inventory_time_s = 0.0;
    double abs_inventory_time_s = 0.0;
    double sq_inventory_time_s = 0.0;
    double signed_notional_inventory_time_s = 0.0;
    double notional_inventory_time_s = 0.0;
    std::int64_t cap_hit_count = 0;
    std::int64_t delta_cap_hit_count = 0;
    std::int64_t final_cap_compress_count = 0;
    std::int64_t final_cap_rounding_count = 0;
    std::int64_t final_cap_mid_guard_count = 0;
    std::int64_t final_cap_post_only_count = 0;
    std::int64_t final_cap_delta_count = 0;
    std::int64_t cap_exposure_block_count = 0;
    std::int64_t bid_cap_exposure_block_count = 0;
    std::int64_t ask_cap_exposure_block_count = 0;
    std::int64_t random_passive_mirror_count = 0;
    std::int64_t random_passive_mirror_eligible_count = 0;
    std::int64_t random_passive_timing_jitter_count = 0;
    std::int64_t exec_book_visibility_delay_applied_count = 0;
    double exec_book_visibility_delay_sum_ms = 0.0;
    std::int64_t exec_book_visibility_delay_max_ms = 0;
    std::int64_t fills_bid_final_compressed = 0;
    std::int64_t fills_ask_final_compressed = 0;
    std::int64_t fills_bid_not_final_compressed = 0;
    std::int64_t fills_ask_not_final_compressed = 0;
    double avg_markout_final_compressed = 0.0;
    double avg_markout_not_final_compressed = 0.0;
    double markout_qty_final_compressed_btc = 0.0;
    double markout_qty_not_final_compressed_btc = 0.0;
    std::int64_t quote_spread_lt_100_count = 0;
    std::int64_t quote_spread_lt_150_count = 0;
    std::int64_t final_spread_lt_100_count = 0;
    std::int64_t final_spread_lt_150_count = 0;
    std::int64_t stale_book_skip_count = 0;
    std::int64_t ber_active_count = 0;
    std::int64_t ber_feature_publish_count = 0;
    std::int64_t ber_role_safe_decision_count = 0;
    std::int64_t ber_role_safe_buy_add_count = 0;
    std::int64_t ber_role_safe_sell_add_count = 0;
    std::int64_t ber_role_safe_flat_bypass_count = 0;
    std::int64_t ber_role_safe_mixed_fail_closed_count = 0;
    std::int64_t ber_role_safe_pair_change_count = 0;
    std::int64_t ber_role_safe_bid_change_count = 0;
    std::int64_t ber_role_safe_ask_change_count = 0;
    std::int64_t ber_role_safe_source_mismatch_count = 0;
    std::int64_t ber_role_safe_cap_collision_count = 0;
    std::int64_t ber_role_safe_cap_infeasible_count = 0;
    double ber_held_input_end = 0.0;
    double ber_ema_fast_end = 0.0;
    double ber_ema_slow_end = 0.0;
    bool ber_active_end = false;
    std::int64_t fill_cooldown_bid_block_count = 0;
    std::int64_t fill_cooldown_ask_block_count = 0;
    std::int64_t consecutive_loss_cooldown_trigger_count = 0;
    std::int64_t consecutive_loss_cooldown_expiry_count = 0;
    std::int64_t consecutive_loss_cooldown_block_count = 0;
    std::int64_t consecutive_loss_cooldown_cancel_count = 0;
    std::int64_t consecutive_loss_round_trip_loss_count = 0;
    std::int64_t consecutive_loss_round_trip_nonloss_count = 0;
    std::int64_t consecutive_loss_count_end = 0;
    std::int64_t consecutive_loss_count_max = 0;
    std::int64_t consecutive_loss_cooldown_until_ms = 0;
    std::int64_t consecutive_loss_last_cancel_ts_end = -1;
    std::string consecutive_loss_snapshot_schema;
    double consecutive_loss_inventory_end = 0.0;
    double consecutive_loss_avg_entry_end = 0.0;
    double consecutive_loss_open_commission_end = 0.0;
    double consecutive_loss_round_trip_pnl_end = 0.0;
    bool consecutive_loss_threshold_pending_end = false;
    std::int64_t sync_adjust_degrade_trigger_count = 0;
    std::int64_t sync_adjust_degrade_block_bid_count = 0;
    std::int64_t sync_adjust_degrade_block_ask_count = 0;
    std::int64_t sync_adjust_degrade_until_ms = 0;
    bool sync_adjust_censored = false;
    std::int64_t sync_adjust_censor_ts_ms = 0;
    std::int64_t post_only_guard_hits = 0;
    std::int64_t adverse_guard_count = 0;
    std::int64_t adverse_pause_count = 0;
    std::int64_t adverse_markout_stale_drop_count = 0;
    std::int64_t bid_adverse_markout_pause_extend_count = 0;
    std::int64_t ask_adverse_markout_pause_extend_count = 0;
    std::int64_t defense_guard_count = 0;
    std::int64_t defense_pause_count = 0;
    std::int64_t pending_cancel_fills = 0;
    std::int64_t queue_l2_cancel_ahead_event_count = 0;
    std::int64_t queue_l2_cancel_ahead_bid_event_count = 0;
    std::int64_t queue_l2_cancel_ahead_ask_event_count = 0;
    double queue_l2_cancel_ahead_qty = 0.0;
    std::int64_t local_extreme_guard_count = 0;
    std::int64_t local_extreme_pause_count = 0;
    std::int64_t bid_local_extreme_guard_count = 0;
    std::int64_t ask_local_extreme_guard_count = 0;
    std::int64_t fills_bid_local_extreme_guard = 0;
    std::int64_t fills_ask_local_extreme_guard = 0;
    std::int64_t fragile_ttl_cancel_count = 0;
    std::int64_t flat_unilateral_release_count = 0;
    std::int64_t flat_unilateral_bid_release_count = 0;
    std::int64_t flat_unilateral_ask_release_count = 0;
    std::int64_t position_timeout_count = 0;
    std::int64_t circuit_breaker_count = 0;
    std::int64_t risk_daily_loss_block_count = 0;
    std::int64_t risk_position_value_block_count = 0;
    std::int64_t risk_emergency_close_count = 0;
    std::int64_t risk_notional_cap_count = 0;
    bool risk_emergency_latched = false;
    std::int64_t risk_utc_day = 0;
    double risk_day_start_total_pnl = 0.0;
    double risk_session_peak_pnl = 0.0;
    double risk_last_total_pnl = 0.0;
    double risk_total_pnl_offset = 0.0;
    std::int64_t circuit_breaker_close_place_count = 0;
    std::int64_t circuit_breaker_close_keep_count = 0;
    std::int64_t circuit_breaker_close_fill_count = 0;
    std::int64_t circuit_breaker_close_gtx_reject_count = 0;
    std::int64_t circuit_breaker_close_ioc_place_count = 0;
    std::int64_t circuit_breaker_close_ioc_fill_count = 0;
    std::int64_t circuit_breaker_close_ioc_expire_count = 0;
    bool circuit_breaker_closing = false;
    std::int64_t emergency_close_count = 0;
    std::int64_t replace_throttle_count = 0;
    std::int64_t replace_throttle_bid_count = 0;
    std::int64_t replace_throttle_ask_count = 0;
    std::int64_t replace_throttle_price_count = 0;
    std::int64_t replace_throttle_age_count = 0;
    std::int64_t campaign_stop_add_count = 0;
    std::int64_t bid_campaign_stop_add_count = 0;
    std::int64_t ask_campaign_stop_add_count = 0;
    std::int64_t campaign_soft_control_count = 0;
    std::int64_t bid_campaign_soft_control_count = 0;
    std::int64_t ask_campaign_soft_control_count = 0;
    std::int64_t adaptive_add_cooldown_hit_count = 0;
    std::int64_t adaptive_add_cooldown_bid_hit_count = 0;
    std::int64_t adaptive_add_cooldown_ask_hit_count = 0;
    std::int64_t decision_place_count = 0;
    std::int64_t decision_replace_count = 0;
    std::int64_t decision_keep_count = 0;
    std::int64_t decision_pause_count = 0;
    std::int64_t decision_none_count = 0;
    std::int64_t decision_pending_coalesce_count = 0;
    std::int64_t decision_cancel_first_count = 0;
    std::int64_t max_pending_new_orders = 0;
    std::int64_t max_pending_cancel_orders = 0;
    std::int64_t buy_fill_selection_live_eval_count = 0;
    std::int64_t buy_fill_selection_live_hit_count = 0;
    double buy_fill_selection_live_score_sum = 0.0;
    double buy_fill_selection_live_score_max = 0.0;
    std::int64_t buy_fill_selection_live_score_ge_042 = 0;
    std::int64_t buy_fill_selection_live_score_ge_043 = 0;
    std::int64_t buy_fill_selection_live_score_ge_044 = 0;
    std::int64_t buy_fill_selection_live_score_ge_045 = 0;
    std::int64_t buy_soft_widen_release_target_reached_count = 0;
    std::int64_t buy_soft_widen_release_eligible_count = 0;
    std::int64_t buy_soft_widen_release_requested_count = 0;
    std::int64_t buy_soft_widen_release_effective_mult_count = 0;
    std::int64_t buy_soft_widen_release_effective_price_count = 0;
    std::string buy_soft_widen_release_role_observed;
    std::string buy_soft_widen_release_reason = "not_reached";
    double buy_soft_widen_release_baseline_spread_mult = 0.0;
    double buy_soft_widen_release_selected_spread_mult = 0.0;
    double buy_soft_widen_release_baseline_bid_price = 0.0;
    double buy_soft_widen_release_candidate_bid_price = 0.0;
    std::int64_t p3_reach_gate_eval_count = 0;
    std::int64_t p3_reach_gate_toxicity_trigger_count = 0;
    std::int64_t p3_reach_gate_supported_count = 0;
    std::int64_t p3_reach_gate_pass_count = 0;
    std::int64_t p3_reach_gate_price_change_count = 0;
    std::int64_t p3_reach_gate_spread_cap_noop_count = 0;
    std::int64_t p3_reach_gate_buy_price_change_count = 0;
    std::int64_t p3_reach_gate_sell_price_change_count = 0;
    std::int64_t p3_reach_budget_bucket_eval_count = 0;
    std::int64_t p3_reach_budget_toxicity_trigger_count = 0;
    std::int64_t p3_reach_budget_activation_count = 0;
    std::int64_t p3_reach_budget_buy_activation_count = 0;
    std::int64_t p3_reach_budget_sell_activation_count = 0;
    std::int64_t p3_reach_budget_no_action_count = 0;
    std::int64_t p3_reach_budget_unsupported_count = 0;
    std::int64_t p3_reach_budget_reuse_count = 0;
    std::int64_t p3_reach_budget_exposure_decision_count = 0;
    std::int64_t p3_reach_budget_price_change_count = 0;
    std::int64_t p3_reach_budget_buy_price_change_count = 0;
    std::int64_t p3_reach_budget_sell_price_change_count = 0;
    std::int64_t p3_reach_budget_hard_safety_suppressed_count = 0;
    std::int64_t p3_reach_budget_reducing_unchanged_count = 0;
    std::int64_t p3_reach_budget_spread_cap_noop_count = 0;
    std::int64_t p3_reach_budget_bucket_expiry_count = 0;
    std::int64_t p3_reach_budget_flat_reset_count = 0;
    std::int64_t p3_reach_budget_selected_k_sum = 0;
    std::int64_t p3_reach_budget_selected_k_max = 0;
    std::int64_t p3_reach_budget_active_end_count = 0;
    std::int64_t p3_reach_budget_buy_selected_k_end = 0;
    std::int64_t p3_reach_budget_sell_selected_k_end = 0;
    std::int64_t fair_center_eval_count = 0;
    std::int64_t fair_center_valid_count = 0;
    std::int64_t fair_center_invalid_count = 0;
    std::int64_t fair_center_nonzero_request_count = 0;
    std::int64_t fair_center_price_change_count = 0;
    std::int64_t fair_center_gtx_clamp_count = 0;
    std::int64_t fair_center_no_pair_support_count = 0;
    std::int64_t fair_center_effective_shift_ticks_abs_sum = 0;
    std::int64_t fair_center_effective_shift_ticks_abs_max = 0;
    std::int64_t fixed_spread_probe_bid_submitted_orders = 0;
    std::int64_t fixed_spread_probe_ask_submitted_orders = 0;
    std::int64_t fixed_spread_probe_bid_activation_gtx_rejects = 0;
    std::int64_t fixed_spread_probe_ask_activation_gtx_rejects = 0;
    std::int64_t fixed_spread_probe_bid_placed_orders = 0;
    std::int64_t fixed_spread_probe_ask_placed_orders = 0;
    std::int64_t fixed_spread_probe_bid_queue_visible_positive_orders = 0;
    std::int64_t fixed_spread_probe_ask_queue_visible_positive_orders = 0;
    std::int64_t fixed_spread_probe_bid_queue_known_zero_orders = 0;
    std::int64_t fixed_spread_probe_ask_queue_known_zero_orders = 0;
    std::int64_t fixed_spread_probe_bid_queue_fallback_orders = 0;
    std::int64_t fixed_spread_probe_ask_queue_fallback_orders = 0;
    std::int64_t fixed_spread_probe_bid_active_touched_orders = 0;
    std::int64_t fixed_spread_probe_ask_active_touched_orders = 0;
    std::int64_t fixed_spread_probe_bid_filled_orders = 0;
    std::int64_t fixed_spread_probe_ask_filled_orders = 0;
    std::int64_t fixed_spread_probe_bid_fully_filled_orders = 0;
    std::int64_t fixed_spread_probe_ask_fully_filled_orders = 0;
    std::int64_t fixed_spread_probe_bid_first_fill_pending_cancel_orders = 0;
    std::int64_t fixed_spread_probe_ask_first_fill_pending_cancel_orders = 0;
    std::int64_t fixed_spread_probe_bid_filled_within_1s = 0;
    std::int64_t fixed_spread_probe_ask_filled_within_1s = 0;
    std::int64_t fixed_spread_probe_bid_filled_within_5s = 0;
    std::int64_t fixed_spread_probe_ask_filled_within_5s = 0;
    std::int64_t fixed_spread_probe_bid_filled_within_10s = 0;
    std::int64_t fixed_spread_probe_ask_filled_within_10s = 0;
    std::int64_t fixed_spread_probe_bid_end_censored_unfilled = 0;
    std::int64_t fixed_spread_probe_ask_end_censored_unfilled = 0;
    std::int64_t fixed_spread_probe_bid_end_censored_before_1s = 0;
    std::int64_t fixed_spread_probe_ask_end_censored_before_1s = 0;
    std::int64_t fixed_spread_probe_bid_end_censored_before_5s = 0;
    std::int64_t fixed_spread_probe_ask_end_censored_before_5s = 0;
    std::int64_t fixed_spread_probe_bid_end_censored_before_10s = 0;
    std::int64_t fixed_spread_probe_ask_end_censored_before_10s = 0;
    double fixed_spread_probe_bid_fill_qty = 0.0;
    double fixed_spread_probe_ask_fill_qty = 0.0;
};

struct PairedFixedSpreadProbeRow {
    Side side = Side::Buy;
    double distance_ticks = 0.0;
    std::int64_t submitted_orders = 0;
    std::int64_t activation_gtx_rejects = 0;
    std::int64_t cancelled_before_activation = 0;
    std::int64_t placed_orders = 0;
    std::int64_t queue_visible_positive_orders = 0;
    std::int64_t queue_known_zero_orders = 0;
    std::int64_t queue_fallback_orders = 0;
    std::int64_t exact_touched_orders = 0;
    std::int64_t through_touched_orders = 0;
    std::int64_t any_touched_orders = 0;
    std::int64_t filled_orders = 0;
    std::int64_t fully_filled_orders = 0;
    std::int64_t filled_via_exact_orders = 0;
    std::int64_t filled_via_through_orders = 0;
    std::int64_t through_forced_fill_orders = 0;
    std::int64_t first_fill_pending_cancel_orders = 0;
    std::int64_t filled_within_1s = 0;
    std::int64_t filled_within_5s = 0;
    std::int64_t filled_within_10s = 0;
    std::int64_t cancelled_unfilled_orders = 0;
    std::int64_t observed_lifecycle_orders = 0;
    std::int64_t observed_1s_orders = 0;
    std::int64_t observed_5s_orders = 0;
    std::int64_t observed_10s_orders = 0;
    std::int64_t end_censored_orders = 0;
    std::int64_t end_censored_before_1s = 0;
    std::int64_t end_censored_before_5s = 0;
    std::int64_t end_censored_before_10s = 0;
    double fill_qty = 0.0;
};

struct PairedFixedSpreadViolationRow {
    std::int64_t cohort_id = -1;
    Side side = Side::Buy;
    std::int64_t event_ts_ms = 0;
    double shallower_distance_ticks = 0.0;
    double deeper_distance_ticks = 0.0;
};

struct TickReplayResult {
    TickReplaySummary summary;
    std::vector<std::int64_t> pnl_ts_ms;
    std::vector<double> pnl;
    std::vector<double> inventory;
    std::vector<TraceOrderRow> quote_trace;
    std::vector<TraceFillRow> fill_trace;
    std::vector<P3ReachDecisionRow> p3_reach_decision_trace;
    std::vector<CooldownDurationOpportunityRow>
        cooldown_duration_opportunity_trace;
    std::vector<CooldownDurationFillPathRow> cooldown_duration_fill_path;
    CooldownDurationForkTrace cooldown_duration_fork_trace;
    std::vector<F05CooldownDecision> f05_repeated_cooldown_decisions;
    std::optional<F05RepeatedBooleanCooldownCheckpoint>
        f05_repeated_cooldown_checkpoint;
    std::vector<PairedFixedSpreadProbeRow> paired_fixed_spread_rows;
    std::vector<PairedFixedSpreadViolationRow> paired_fixed_spread_violations;
};

[[nodiscard]] TickReplayResult simulate_tick_arrays(
    const TickReplayInput& input,
    const TickReplayParams& params
);

}  // namespace narrowgate_cpp
