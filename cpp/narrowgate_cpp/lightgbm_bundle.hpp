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
    "dir_10s",
    "dir_30s",
    "dir_60s",
    "vol_10s",
    "vol_30s",
    "vol_60s",
    "ret_10s",
    "ret_30s",
    "ret_60s",
    "tox_bid_5s",
    "tox_ask_5s",
    "tox_bid_10s",
    "tox_ask_10s",
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
