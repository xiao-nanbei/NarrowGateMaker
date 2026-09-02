#pragma once

#include <cstdint>
#include <type_traits>

namespace narrowgate_cpp {

enum class LivePolicySide : std::uint8_t { Buy = 1, Sell = 2 };

enum CommonPolicyReason : std::uint32_t {
    CommonPolicyReasonFillCooldown = 1U << 0,
    CommonPolicyReasonMarkout = 1U << 2,
    CommonPolicyReasonStaleWarn = 1U << 3,
    CommonPolicyReasonStaleHard = 1U << 4,
    CommonPolicyReasonBurst = 1U << 5,
    CommonPolicyReasonThinDepth = 1U << 6,
    CommonPolicyReasonInventoryLimit = 1U << 7,
    CommonPolicyReasonAdverse = 1U << 9,
    CommonPolicyReasonDefense = 1U << 12,
};

struct CommonSidePolicyInputPod {
    bool exposure_increasing = false;
    bool fill_cooldown_active = false;
    bool side_adverse = false;
    bool side_adverse_pause = false;
    bool local_extreme_guard = false;
    bool local_extreme_pause = false;
    bool defense_guard = false;
    bool defense_pause = false;
    double inventory_ratio = 0.0;
    double depth_age_s = 0.0;
    double max_book_age_s = 0.0;
    double toxicity = 0.5;
    double markout_ema = 0.0;
    double markout_spread_scale = 0.0;
    double markout_reference = 1.0;
    double microprice_shift_bps = 0.0;
    double l2_quote_flip_rate = 0.0;
    double l2_book_cancel_ratio = 0.0;
    double l2_near_depth_total = 0.0;
    double thin_depth_threshold = 0.0;
    double kappa_depth_baseline = 50.0;
    double local_extreme_spread_mult = 1.0;
    double defense_spread_mult = 1.0;
};

struct CommonSidePolicyResultPod {
    bool allow_post = true;
    bool allow_exposure_increase = true;
    double spread_mult = 1.0;
    double size_mult = 1.0;
    std::uint32_t reason_mask = 0;
};

static_assert(std::is_trivially_copyable_v<CommonSidePolicyInputPod>);
static_assert(std::is_trivially_copyable_v<CommonSidePolicyResultPod>);

// True when an order contains any opening/add leg. A cross-zero order is
// exposure-increasing once quantity exceeds the inventory needed to flatten.
template <LivePolicySide Side>
[[nodiscard]] constexpr bool is_exposure_increasing(
    double inventory,
    double order_quantity,
    double tolerance = 1e-10
) noexcept {
    static_assert(Side == LivePolicySide::Buy || Side == LivePolicySide::Sell);
    const double qty = order_quantity > 0.0 ? order_quantity : 0.0;
    const double tol = tolerance > 0.0 ? tolerance : 0.0;
    if constexpr (Side == LivePolicySide::Buy) {
        return inventory >= -tol || qty > -inventory + tol;
    } else {
        return inventory <= tol || qty > inventory + tol;
    }
}

[[nodiscard]] CommonSidePolicyResultPod evaluate_common_side_policy(
    const CommonSidePolicyInputPod& input
) noexcept;

}  // namespace narrowgate_cpp
