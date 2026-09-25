#pragma once

#include <array>
#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace narrowgate_cpp {

inline constexpr std::array<std::string_view, 13> kLightgbmBundleHeadNames = {
    "touch_conditioned_up_probability_10000ms",
    "touch_conditioned_up_probability_30000ms",
    "touch_conditioned_up_probability_60000ms",
    "absolute_price_variance_rate_10000ms",
    "absolute_price_variance_rate_30000ms",
    "absolute_price_variance_rate_60000ms",
    "touch_conditioned_price_change_fraction_10000ms",
    "touch_conditioned_price_change_fraction_30000ms",
    "touch_conditioned_price_change_fraction_60000ms",
    "touch_side_adverse_probability_bid_5000ms",
    "touch_side_adverse_probability_ask_5000ms",
    "touch_side_adverse_probability_bid_10000ms",
    "touch_side_adverse_probability_ask_10000ms",
};

class LightgbmBundleInference {
public:
    LightgbmBundleInference(
        std::string library_path,
        const std::vector<std::string>& model_paths,
        std::size_t feature_count
    );
    ~LightgbmBundleInference();

    LightgbmBundleInference(const LightgbmBundleInference&) = delete;
    LightgbmBundleInference& operator=(const LightgbmBundleInference&) = delete;
    LightgbmBundleInference(LightgbmBundleInference&&) = delete;
    LightgbmBundleInference& operator=(LightgbmBundleInference&&) = delete;

    void predict(
        std::span<const double> row,
        std::span<double> output
    ) const;

    [[nodiscard]] std::size_t feature_count() const noexcept;
    [[nodiscard]] const std::string& library_path() const noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
    std::size_t feature_count_ = 0;
    std::string library_path_;
};

}  // namespace narrowgate_cpp
