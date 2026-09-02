#pragma once

#include <optional>

#include "common.hpp"

namespace narrowgate_cpp {

// QuoteCoreConfig 是 Python/C++ quote parity 的核心 ABI。
// 新增/删除字段时必须同步 bindings.cpp、strategy/quote_core.py 的字段列表和 parity tests。
struct QuoteCoreConfig {
    // Legacy compatibility input. NaN split fields inherit this value exactly.
    double gamma = 0.01;
    double kappa = 1.0;
    double tick_size = 0.1;
    double lot_size = 0.001;
    double maker_fee = 0.0;
    double order_size = 0.001;
    double max_inventory = 0.01;
    double position_timeout_s = 0.0;
    double quote_horizon_s = 1.0;
    double pnl_volatility_horizon_s = 1.0;

    bool ml_enabled = true;
    double vol_blend = 0.0;
    double dir_threshold = 0.05;
    double gamma_dir_bonus = 0.0;
    double skew_strength = 0.0;
    double asym_strength = 0.0;
    double ret_skew = 0.0;
    double ret_shift_max_pct = 0.3;

    bool regime_enabled = false;
    double vol_baseline = 3.0;
    double gamma_scale_min = 0.5;
    double gamma_scale_max = 2.0;
    double liq_baseline = 200.0;
    double gamma_liq_scale_min = 0.5;
    double gamma_liq_scale_max = 3.0;
    double vol_power = 1.0;

    double kappa_ratio = 0.3;
    double p3_delta_star = 0.0;
    double p3_kappa_eff = 0.0;

    bool use_bar_pricing = true;
    bool use_depth_microprice = false;
    bool use_depth_kappa = false;
    int microprice_levels = 3;
    int kappa_levels = 5;
    double kappa_depth_baseline = 50.0;
    double depth_kappa_ratio = 0.3;

    double ber_spread_mult = 2.0;
    double markout_spread_scale = 0.0;
    double markout_side_asymmetry_sign = 1.0;
    double inventory_skew_strength = 0.0;
    double inventory_asym_strength = 0.0;
    double inventory_signal_fade_strength = 0.0;

    double book_imb_strength = 0.0;
    int book_imb_levels = 20;
    int trace_book_imb_levels = 10;

    bool depth_tox_enabled = false;
    int depth_tox_levels = 20;
    double depth_tox_imbalance_threshold = 0.65;
    double depth_tox_microprice_shift_bps = 1.0;
    double depth_tox_spread_mult = 1.25;

    bool dynamic_cap_enabled = false;
    double max_spread_bps = 0.0;
    double dynamic_cap_base_bps = 0.0;
    double dynamic_cap_alpha = 0.5;
    double dynamic_cap_max_mult = 2.0;
    double dynamic_cap_var_baseline = 0.0;
    double dynamic_cap_liq_beta = 0.0;
    double dynamic_cap_liq_baseline = 0.0;
    double dynamic_cap_min_mult = 1.0;
    int spread_cap_mode = 1;  // 0 explicit compress arm, 1 pause exposure, 2 observe

    double exit_urgency_strength = 0.0;
    double urgency_time_weight = 0.3;
    double urgency_pnl_weight = 0.3;
    double urgency_signal_weight = 0.4;

    bool adverse_guard_enabled = false;
    double adverse_toxicity_threshold = 0.70;
    double adverse_markout_threshold = 5.0;
    double adverse_markout_pause_threshold = 0.0;
    bool adverse_markout_pause_hybrid = false;
    double adverse_dir_threshold = 0.0;
    double adverse_ret_bps_threshold = 0.0;
    double adverse_microprice_shift_bps = 0.0;
    double adverse_spread_mult = 1.10;
    double adverse_thin_depth_threshold = 0.0;
    double adverse_thin_depth_mult = 1.0;
    bool adverse_pause = true;

    bool defense_guard_enabled = false;
    double defense_markout_threshold = 2.0;
    double defense_dir_threshold = 0.05;
    double defense_ret_bps_threshold = 0.0;
    double defense_microprice_shift_bps = 0.0;
    double defense_spread_mult = 1.35;
    bool defense_pause = true;
    double defense_emergency_inventory_ratio = 0.50;
    double defense_emergency_loss = 5.0;

    // Appended after every legacy field to preserve the aggregate layout.
    // inventory_reference_qty is q_ref in base units; order_size is z.
    // Both split coefficients use inverse-price units (base/quote).
    double inventory_reference_qty = 1.0;
    double eta_inventory = std::numeric_limits<double>::quiet_NaN();
    double a_spread = std::numeric_limits<double>::quiet_NaN();
    double f03_ret_action_horizon_s = 0.0;
    bool f03_ret_action_compatible = false;
    double risk_per_order = std::numeric_limits<double>::quiet_NaN();
    double execution_intensity_slope = std::numeric_limits<double>::quiet_NaN();
    double risk_horizon_s = std::numeric_limits<double>::quiet_NaN();
    bool historical_p3_scalar_adapter_enabled = false;
    bool p3_side_bbo_floor_enabled = false;
    bool p3_identity_required = false;
    std::string p3_event_type;
    double p3_horizon_s = 0.0;
    std::string p3_distance_origin;
    std::string p3_distance_unit;
    std::string p3_side;
    std::optional<bool> p3_queue_included;
    std::string p3_artifact_sha256;
    double trade_intensity_acceleration_spread_mult =
        std::numeric_limits<double>::quiet_NaN();

};

struct SideQuoteContext {
    double raw_price = 0.0;
    double pre_guard_price = 0.0;
    double final_price = 0.0;
    double raw_quote_delta_to_bbo = 0.0;
    double pre_guard_delta_to_bbo = 0.0;
    double final_quote_delta_to_bbo = 0.0;
    double raw_distance_to_mid = 0.0;
    double final_distance_to_mid = 0.0;
    double final_pair_spread = 0.0;
    double final_quote_skew = 0.0;
    double spread_mult = 1.0;
    bool side_adverse = false;
    bool side_adverse_pause = false;
    bool adverse_toxicity = false;
    bool adverse_markout = false;
    bool adverse_direction = false;
    bool adverse_ret = false;
    bool adverse_microprice = false;
    bool adverse_thin_depth = false;
    bool defense_guard = false;
    bool defense_pause = false;
    bool defense_reducing = false;
    bool defense_emergency = false;
    bool defense_markout = false;
    bool defense_direction = false;
    bool defense_ret = false;
    bool defense_microprice = false;
    double defense_spread_mult = 1.0;
    bool mid_guard = false;
    bool post_only = false;
    bool cap_exposure_block = false;

    double near_depth_total = 0.0;
    double order_ttl_ms = 0.0;
    bool local_extreme_guard = false;
    bool local_extreme_pause = false;
    bool local_extreme_thin_depth = false;
    double local_extreme_rank = 0.5;
    double local_extreme_low = 0.0;
    double local_extreme_high = 0.0;
    double local_extreme_window_s = 0.0;
    double local_extreme_spread_mult = 1.0;

    double l2_quote_flip_rate = 0.0;
    double l2_book_refresh_ratio = 0.0;
    double l2_book_cancel_ratio = 0.0;
    double l2_near_depth_total = 0.0;
    double buy_fill_selection_live_score = 0.0;
    bool buy_fill_selection_live_hit = false;
    int buy_fill_selection_live_missing_features = 0;
    bool final_guard_changed = false;
    bool any_constraint_changed = false;
};

struct QuoteFlags {
    bool cap_hit = false;
    bool delta_cap = false;
    bool final_compressed = false;
    bool mid_guard = false;
    bool post_only = false;
    bool bid_adverse = false;
    bool ask_adverse = false;
    bool defense_guard = false;
    bool cap_exposure_block = false;
};

struct QuoteCoreResult {
    double bid_price = 0.0;
    double ask_price = 0.0;
    double spread = 0.0;
    double raw_half_spread = 0.0;
    double capped_half_spread = 0.0;
    double raw_mid_shift = 0.0;
    double fair = 0.0;
    double cap_bps = 0.0;
    double max_spread = 0.0;
    double reservation_price = 0.0;
    double sigma_sq_raw = 0.0;
    double sigma_sq_blended = 0.0;
    double delta_raw = 0.0;
    double delta_after_regime = 0.0;
    double delta_pre_cap = 0.0;
    double delta_after_cap = 0.0;
    double final_cap_excess = 0.0;
    double half_d = 0.0;
    double asym = 0.0;
    double raw_reservation_shift = 0.0;
    double raw_asym_shift = 0.0;
    double raw_quote_skew = 0.0;
    double book_imb = 0.0;
    double microprice_shift_bps = 0.0;
    double near_depth_total = 0.0;
    double kappa_before_depth = 0.0;
    double kappa_used = 0.0;
    double depth_tox_mult = 1.0;
    bool final_cap_rounding = false;
    bool final_cap_mid_guard = false;
    bool final_cap_post_only = false;
    bool final_cap_delta = false;
    bool mid_guard_bid = false;
    bool mid_guard_ask = false;
    bool post_only_bid = false;
    bool post_only_ask = false;
    SideQuoteContext buy;
    SideQuoteContext sell;
    QuoteFlags flags;
};

// Immutable derived configuration for the native live quote hot path.
//
// QuoteCoreConfig remains the public Python/C++ ABI.  Generic callers keep
// using the four-argument compute_quote_core() entry point, which validates
// that ABI on every call.  NativeLiveRuntimeCore owns an immutable config, so
// repeating the same P3/F03 identity checks and legacy-field projections on
// every decision is wasted hot-path work.  QuoteHotPlan freezes those exact
// results once without changing any floating-point expression used by the
// quote calculation itself.
class QuoteHotPlan final {
public:
    QuoteHotPlan(const QuoteHotPlan&) = default;
    QuoteHotPlan(QuoteHotPlan&&) noexcept = default;
    QuoteHotPlan& operator=(const QuoteHotPlan&) = delete;
    QuoteHotPlan& operator=(QuoteHotPlan&&) = delete;

    [[nodiscard]] double tick() const noexcept { return tick_; }
    [[nodiscard]] double inventory_reference_qty() const noexcept {
        return inventory_reference_qty_;
    }
    [[nodiscard]] double eta_inventory() const noexcept {
        return eta_inventory_;
    }
    [[nodiscard]] double risk_per_order() const noexcept {
        return risk_per_order_;
    }
    [[nodiscard]] double risk_horizon_s() const noexcept {
        return risk_horizon_s_;
    }
    [[nodiscard]] double acceleration_spread_mult() const noexcept {
        return acceleration_spread_mult_;
    }
    [[nodiscard]] double kappa_base() const noexcept { return kappa_base_; }
    [[nodiscard]] int spread_cap_mode() const noexcept {
        return spread_cap_mode_;
    }
    [[nodiscard]] bool historical_p3_pair_floor_active() const noexcept {
        return historical_p3_pair_floor_active_;
    }
    [[nodiscard]] bool p3_side_bbo_floor_active() const noexcept {
        return p3_side_bbo_floor_active_;
    }

private:
    friend QuoteHotPlan make_quote_hot_plan(const QuoteCoreConfig& cfg);

    QuoteHotPlan(
        double tick,
        double inventory_reference_qty,
        double eta_inventory,
        double risk_per_order,
        double risk_horizon_s,
        double acceleration_spread_mult,
        double kappa_base,
        int spread_cap_mode,
        bool historical_p3_pair_floor_active,
        bool p3_side_bbo_floor_active
    ) noexcept
        : tick_(tick),
          inventory_reference_qty_(inventory_reference_qty),
          eta_inventory_(eta_inventory),
          risk_per_order_(risk_per_order),
          risk_horizon_s_(risk_horizon_s),
          acceleration_spread_mult_(acceleration_spread_mult),
          kappa_base_(kappa_base),
          spread_cap_mode_(spread_cap_mode),
          historical_p3_pair_floor_active_(historical_p3_pair_floor_active),
          p3_side_bbo_floor_active_(p3_side_bbo_floor_active) {}

    double tick_;
    double inventory_reference_qty_;
    double eta_inventory_;
    double risk_per_order_;
    double risk_horizon_s_;
    double acceleration_spread_mult_;
    double kappa_base_;
    int spread_cap_mode_;
    bool historical_p3_pair_floor_active_;
    bool p3_side_bbo_floor_active_;
};

static_assert(sizeof(QuoteHotPlan) <= kDestructiveInterferenceBytes);

struct LiveRoutingPolicy {
    bool allow_post = true;
    bool allow_exposure_increase = true;
    double spread_mult = 1.0;
    double size_mult = 1.0;
    double order_ttl_ms = 0.0;
};

// live routing 只接收扁平、低基数的上下文，避免 Python dataclass/dict 在热路径里反复物化。
// WebSocket/REST/order lifecycle 仍留在 Python；这里仅负责 post-policy price/size/update 判定。
struct LiveRoutingInput {
    double mid = 0.0;
    double inventory = 0.0;
    double base_bid_price = 0.0;
    double base_ask_price = 0.0;
    double best_bid = 0.0;
    double best_ask = 0.0;
    double tick_size = 0.1;
    double lot_size = 0.001;
    double min_qty = 0.001;
    double min_notional = 0.0;
    double order_size = 0.001;
    double max_inventory = 0.01;
    double eta = 0.0;
    double requote_threshold_bps = 0.0;
    double max_spread = 0.0;
    double bid_active_price = 0.0;
    double bid_age_ms = 0.0;
    double ask_active_price = 0.0;
    double ask_age_ms = 0.0;
    bool symmetric_size = false;
    bool bid_active = false;
    bool ask_active = false;
};

struct LiveRoutingResult {
    double bid_price = 0.0;
    double ask_price = 0.0;
    double bid_size = 0.0;
    double ask_size = 0.0;
    bool post_policy_cap_hit = false;
    bool can_bid_after_inventory = false;
    bool can_ask_after_inventory = false;
    bool can_bid = false;
    bool can_ask = false;
    bool bid_needs_update = true;
    bool ask_needs_update = true;
};

[[nodiscard]] std::tuple<double, double, bool, double> apply_final_spread_cap(
    double mid,
    double bid_price,
    double ask_price,
    double max_spread,
    double tick_size
);

[[nodiscard]] LiveRoutingResult compute_live_routing_decision(
    const LiveRoutingInput& input,
    const LiveRoutingPolicy& bid_policy,
    const LiveRoutingPolicy& ask_policy
);

// Validate the immutable quote configuration and freeze its derived legacy
// projections.  Throws the same std::invalid_argument errors as the public
// quote entry point for a valid-mid decision.
[[nodiscard]] QuoteHotPlan make_quote_hot_plan(const QuoteCoreConfig& cfg);

// Prevalidated overload for owners that guarantee cfg cannot mutate during
// the plan lifetime (NativeLiveRuntimeCore).  Generic/public callers must use
// the four-argument overload below.
[[nodiscard]] QuoteCoreResult compute_quote_core(
    const QuoteState& state,
    const QuoteCoreConfig& cfg,
    const QuotePrediction& pred,
    const DepthView& depth,
    const QuoteHotPlan& plan
);

[[nodiscard]] QuoteCoreResult compute_quote_core(
    const QuoteState& state,
    const QuoteCoreConfig& cfg,
    const QuotePrediction& pred,
    const DepthView& depth
);

[[nodiscard]] inline QuoteCoreResult compute_quote_core(
    const QuoteState& state,
    const QuoteCoreConfig& cfg,
    const QuotePrediction& pred,
    const DepthSnapshot& depth = DepthSnapshot{}
) {
    return compute_quote_core(state, cfg, pred, depth.view());
}

}  // namespace narrowgate_cpp
