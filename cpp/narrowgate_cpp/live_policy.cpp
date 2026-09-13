#include "live_policy.hpp"

#include <algorithm>
#include <cmath>

namespace narrowgate_cpp {
namespace {

[[nodiscard]] constexpr double clamp(double value, double lo, double hi) noexcept {
    return std::max(lo, std::min(hi, value));
}

}  // namespace

CommonSidePolicyResultPod evaluate_common_side_policy(
    const CommonSidePolicyInputPod& input
) noexcept {
    CommonSidePolicyResultPod out;
    if (input.fill_cooldown_active) {
        out.allow_post = false;
        out.reason_mask |= CommonPolicyReasonFillCooldown;
    }
    if (input.max_book_age_s > 0.0) {
        if (!std::isfinite(input.depth_age_s) || input.depth_age_s < 0.0 ||
            input.depth_age_s >= input.max_book_age_s) {
            out.allow_post = false;
            out.reason_mask |= CommonPolicyReasonStaleHard;
        } else if (input.depth_age_s >= 0.5 * input.max_book_age_s) {
            out.spread_mult = std::max(out.spread_mult, 1.25);
            out.size_mult = std::min(out.size_mult, 0.65);
            out.reason_mask |= CommonPolicyReasonStaleWarn;
        }
    }
    if (input.markout_ema < 0.0 && input.markout_spread_scale > 0.0) {
        const double severity = std::min(
            std::abs(input.markout_ema) /
                std::max(input.markout_reference, 1e-6),
            1.0
        );
        out.spread_mult = std::max(out.spread_mult, 1.05 + 0.25 * severity);
        out.size_mult = std::min(out.size_mult, 0.85 - 0.35 * severity);
        out.reason_mask |= CommonPolicyReasonMarkout;
    }
    if (input.side_adverse) {
        out.size_mult = std::min(out.size_mult, 0.70);
        out.reason_mask |= CommonPolicyReasonAdverse;
        if (input.side_adverse_pause) {
            out.allow_exposure_increase = false;
        }
    }
    if (input.local_extreme_guard) {
        out.spread_mult = std::max(
            out.spread_mult,
            std::max(1.0, input.local_extreme_spread_mult)
        );
        out.reason_mask |= CommonPolicyReasonAdverse;
        if (input.local_extreme_pause) {
            out.allow_exposure_increase = false;
        }
    }
    if (input.defense_guard) {
        out.spread_mult = std::max(
            out.spread_mult,
            std::max(1.0, input.defense_spread_mult)
        );
        out.size_mult = std::min(out.size_mult, 0.70);
        out.reason_mask |= CommonPolicyReasonDefense;
        if (input.defense_pause) {
            out.allow_post = false;
        }
    }
    if (input.l2_quote_flip_rate >= 0.35 &&
        input.l2_book_cancel_ratio >= 0.04 &&
        std::abs(input.microprice_shift_bps) >= 0.5) {
        out.allow_exposure_increase = false;
        out.spread_mult = std::max(out.spread_mult, 1.35);
        out.size_mult = std::min(out.size_mult, 0.45);
        out.reason_mask |= CommonPolicyReasonBurst;
    }
    const double thin_threshold = input.thin_depth_threshold > 0.0
        ? input.thin_depth_threshold
        : std::max(1.0, input.kappa_depth_baseline * 0.5);
    if (input.l2_near_depth_total > 0.0 &&
        input.l2_near_depth_total < thin_threshold) {
        out.spread_mult = std::max(out.spread_mult, 1.10);
        out.size_mult = std::min(out.size_mult, 0.75);
        out.reason_mask |= CommonPolicyReasonThinDepth;
    }
    if (input.inventory_ratio >= 0.98 && input.exposure_increasing) {
        out.allow_exposure_increase = false;
        out.reason_mask |= CommonPolicyReasonInventoryLimit;
    }
    out.spread_mult = std::max(1.0, out.spread_mult);
    out.size_mult = clamp(out.size_mult, 0.0, 1.0);
    return out;
}

}  // namespace narrowgate_cpp
