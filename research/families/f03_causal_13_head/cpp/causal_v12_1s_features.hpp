#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace narrowgate_cpp::f03 {

inline constexpr std::string_view kCausalV12OneSecondFeatureAbiVersion =
    "causal_v12_1s_cpp_feature_parity.v1";
inline constexpr std::string_view kCausalV12OneSecondBatchAbiVersion =
    "causal_v12_1s_cpp_daily_batch.v1";
inline constexpr std::string_view kCausalV12OneSecondFeatureOrderSha256 =
    "5a6947850dfabefbf4e36bdbe986e39c96324e3714efb16d3410a4443ea1b797";
inline constexpr std::size_t kCausalV12OneSecondFeatureCount = 173;
inline constexpr std::size_t kExecutionL2FeatureCount = 13;

struct OneSecondBar {
    std::int64_t start_ts_ms = 0;
    std::int64_t finalized_ts_ms = 0;
    double open = 0.0;
    double high = 0.0;
    double low = 0.0;
    double close = 0.0;
    double volume = 0.0;
    double buy_volume = 0.0;
    double sell_volume = 0.0;
    std::int64_t trade_count = 0;
    std::int64_t buy_count = 0;
    std::int64_t sell_count = 0;
    double buy_quote_qty = 0.0;
    double sell_quote_qty = 0.0;
    std::int64_t max_same_side_run = 0;
    double buy_price_high = 0.0;
    double buy_price_low = 0.0;
    double sell_price_high = 0.0;
    double sell_price_low = 0.0;
};

struct ExecutionL2Observation {
    std::int64_t bucket_start_ts_ms = 0;
    std::int64_t feature_ready_ts_ms = 0;
    std::array<double, kExecutionL2FeatureCount> values{};
};

struct MetricObservation {
    std::int64_t source_ts_ms = 0;
    std::int64_t feature_ready_ts_ms = 0;
    double sum_open_interest = 0.0;
    double toptrader_ls_ratio = 0.0;
    double crowd_ls_ratio = 0.0;
    double taker_ls_ratio = 0.0;
};

struct FeatureValue {
    std::optional<double> value;
    std::optional<std::int64_t> source_latest_ts_ms;
    std::optional<std::int64_t> feature_ready_ts_ms;
    std::int64_t observation_count = 0;
    std::string lag_state;
};

struct FeatureRow {
    std::int64_t cutoff_exclusive_ms = 0;
    std::int64_t decision_ts_ms = 0;
    std::int64_t feature_ready_ts_ms = 0;
    std::array<FeatureValue, kCausalV12OneSecondFeatureCount> values{};
};

struct FeatureBatch {
    std::size_t row_count = 0;
    std::vector<std::int64_t> cutoff_exclusive_ms;
    std::vector<std::int64_t> decision_ts_ms;
    std::vector<std::int64_t> feature_ready_ts_ms;
    std::vector<double> values;
    std::vector<std::uint8_t> valid;
    std::vector<std::int64_t> source_latest_ts_ms;
    std::vector<std::int64_t> feature_ready_ts_ms_by_feature;
    std::vector<std::int64_t> observation_count;
    std::vector<std::uint8_t> lag_state_code;
};

inline constexpr std::array<std::string_view, 7> kFeatureLagStateVocabulary = {
    "ready",
    "warmup_insufficient",
    "undefined_or_zero_denominator",
    "execution_l2_exact_bucket_missing_no_carry",
    "execution_l2_late_at_cutoff",
    "metrics_missing_or_stale_no_default",
    "cross_market_missing_or_stale_no_forward_fill",
};

class CausalV12OneSecondBatchEngine {
public:
    CausalV12OneSecondBatchEngine(
        std::vector<OneSecondBar> local_bars,
        std::vector<ExecutionL2Observation> execution_l2 = {},
        std::vector<MetricObservation> metrics = {},
        std::vector<OneSecondBar> reference_bars = {}
    );
    ~CausalV12OneSecondBatchEngine();

    CausalV12OneSecondBatchEngine(CausalV12OneSecondBatchEngine&&) noexcept;
    CausalV12OneSecondBatchEngine& operator=(CausalV12OneSecondBatchEngine&&) noexcept;
    CausalV12OneSecondBatchEngine(const CausalV12OneSecondBatchEngine&) = delete;
    CausalV12OneSecondBatchEngine& operator=(const CausalV12OneSecondBatchEngine&) = delete;

    FeatureBatch compute(
        const std::vector<std::int64_t>& cutoffs_exclusive_ms,
        const std::vector<std::int64_t>& decision_ts_ms = {}
    ) const;

    std::size_t local_bar_count() const noexcept;
    std::size_t reference_bar_count() const noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

extern const std::array<std::string_view, kCausalV12OneSecondFeatureCount>
    kCausalV12OneSecondFeatureNames;

FeatureRow compute_causal_v12_one_second_features(
    const std::vector<OneSecondBar>& local_bars,
    std::int64_t cutoff_exclusive_ms,
    std::int64_t decision_ts_ms,
    const std::vector<ExecutionL2Observation>& execution_l2 = {},
    const std::vector<MetricObservation>& metrics = {},
    const std::vector<OneSecondBar>& reference_bars = {}
);

}  // namespace narrowgate_cpp::f03
