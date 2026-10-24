#include "causal_v12_1s_features.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace narrowgate_cpp::f03 {
namespace {

using BarView = std::vector<const OneSecondBar*>;
using FeatureMap = std::unordered_map<std::string, FeatureValue>;

constexpr std::int64_t kCadenceMs = 1'000;
constexpr std::int64_t kMetricsMaxAgeMs = 300'000;
constexpr std::int64_t kCrossSourceMaxAgeMs = 30'000;

FeatureValue feature_value(
    std::optional<double> value,
    std::optional<std::int64_t> source_ts_ms,
    std::optional<std::int64_t> ready_ts_ms,
    std::int64_t observation_count,
    std::int64_t required_observations = 1,
    std::string missing_state = "warmup_insufficient"
) {
    if (observation_count < required_observations) {
        return FeatureValue{
            std::nullopt,
            source_ts_ms,
            ready_ts_ms,
            observation_count,
            std::move(missing_state),
        };
    }
    if (!value.has_value() || !std::isfinite(*value)) {
        return FeatureValue{
            std::nullopt,
            source_ts_ms,
            ready_ts_ms,
            observation_count,
            "undefined_or_zero_denominator",
        };
    }
    return FeatureValue{
        *value,
        source_ts_ms,
        ready_ts_ms,
        observation_count,
        "ready",
    };
}

FeatureValue missing_value(std::string reason) {
    return FeatureValue{std::nullopt, std::nullopt, std::nullopt, 0, std::move(reason)};
}

void validate_bar(const OneSecondBar& bar, const char* source_name) {
    if (bar.start_ts_ms <= 0 || bar.start_ts_ms % kCadenceMs != 0) {
        throw std::invalid_argument(std::string(source_name) +
            " 1s bar start timestamp must be positive and aligned");
    }
    if (bar.finalized_ts_ms < bar.start_ts_ms + kCadenceMs) {
        throw std::invalid_argument(std::string(source_name) +
            " 1s bar cannot be ready before its interval ends");
    }
    const std::array prices{bar.open, bar.high, bar.low, bar.close};
    for (const double value : prices) {
        if (!std::isfinite(value) || value <= 0.0) {
            throw std::invalid_argument(std::string(source_name) +
                " 1s OHLC prices must be finite and positive");
        }
    }
    if (bar.high < std::max(bar.open, bar.close) ||
        bar.low > std::min(bar.open, bar.close)) {
        throw std::invalid_argument(std::string(source_name) +
            " 1s OHLC bounds are inconsistent");
    }
    const std::array quantities{
        bar.volume,
        bar.buy_volume,
        bar.sell_volume,
        bar.buy_quote_qty,
        bar.sell_quote_qty,
    };
    for (const double value : quantities) {
        if (!std::isfinite(value) || value < 0.0) {
            throw std::invalid_argument(std::string(source_name) +
                " 1s quantities must be finite and non-negative");
        }
    }
    if (bar.trade_count < 0 || bar.buy_count < 0 || bar.sell_count < 0 ||
        bar.max_same_side_run < 0) {
        throw std::invalid_argument(std::string(source_name) +
            " 1s trade counts must be non-negative");
    }
    const std::array side_prices{
        bar.buy_price_high,
        bar.buy_price_low,
        bar.sell_price_high,
        bar.sell_price_low,
    };
    for (const double value : side_prices) {
        if (!std::isfinite(value) || value < 0.0) {
            throw std::invalid_argument(std::string(source_name) +
                " 1s side prices must be finite and non-negative");
        }
    }
}

BarView cutoff_view(
    const std::vector<OneSecondBar>& bars,
    std::int64_t cutoff_exclusive_ms,
    const char* source_name
) {
    BarView candidates;
    candidates.reserve(bars.size());
    std::unordered_set<std::int64_t> starts;
    for (const auto& bar : bars) {
        validate_bar(bar, source_name);
        if (bar.start_ts_ms >= cutoff_exclusive_ms) {
            continue;
        }
        if (!starts.insert(bar.start_ts_ms).second) {
            throw std::invalid_argument(std::string("duplicate 1s source clock in ") +
                source_name);
        }
        if (bar.start_ts_ms + kCadenceMs <= cutoff_exclusive_ms &&
            bar.finalized_ts_ms <= cutoff_exclusive_ms) {
            candidates.push_back(&bar);
        }
    }
    std::sort(candidates.begin(), candidates.end(), [](const auto* lhs, const auto* rhs) {
        return lhs->start_ts_ms < rhs->start_ts_ms;
    });
    if (candidates.empty()) {
        throw std::invalid_argument(std::string("no causal 1s bars are visible for ") +
            source_name);
    }
    if (candidates.back()->start_ts_ms != cutoff_exclusive_ms - kCadenceMs) {
        throw std::invalid_argument(std::string("latest completed 1s bar is missing or late for ") +
            source_name);
    }
    for (std::size_t index = 1; index < candidates.size(); ++index) {
        if (candidates[index]->start_ts_ms !=
            candidates[index - 1]->start_ts_ms + kCadenceMs) {
            throw std::invalid_argument(std::string("1s gap detected in ") + source_name);
        }
    }
    return candidates;
}

std::size_t tail_start(const std::size_t size, const std::size_t count) {
    return size > count ? size - count : 0;
}

long double precise_sum(const std::vector<double>& values) {
    long double result = 0.0L;
    for (const double value : values) {
        result += static_cast<long double>(value);
    }
    return result;
}

std::optional<double> mean(const std::vector<double>& values) {
    if (values.empty()) {
        return std::nullopt;
    }
    return static_cast<double>(precise_sum(values) /
        static_cast<long double>(values.size()));
}

std::optional<double> sample_std(const std::vector<double>& values) {
    if (values.size() < 2) {
        return std::nullopt;
    }
    // Python's math.fsum oracle rounds the mean to binary64 before centering.
    // Keep that rounding point explicit so x86 extended precision and platforms
    // where long double equals double produce the same feature values.
    const double average = mean(values).value();
    long double squared = 0.0L;
    for (const double value : values) {
        const double delta = value - average;
        const double term = delta * delta;
        squared += static_cast<long double>(term);
    }
    const double variance = static_cast<double>(squared) /
        static_cast<double>(values.size() - 1);
    return std::sqrt(std::max(0.0, variance));
}

std::optional<double> skew(const std::vector<double>& values) {
    if (values.size() < 5) {
        return std::nullopt;
    }
    const double average = mean(values).value();
    long double second = 0.0L;
    long double third = 0.0L;
    for (const double value : values) {
        const double delta = value - average;
        const double squared = delta * delta;
        second += static_cast<long double>(squared);
        third += static_cast<long double>(squared * delta);
    }
    const double second_moment = static_cast<double>(second) /
        static_cast<double>(values.size());
    const double third_moment = static_cast<double>(third) /
        static_cast<double>(values.size());
    if (second_moment <= 0.0) {
        return 0.0;
    }
    return third_moment / std::pow(second_moment, 1.5);
}

std::optional<double> kurtosis(const std::vector<double>& values) {
    if (values.size() < 5) {
        return std::nullopt;
    }
    const double average = mean(values).value();
    long double second = 0.0L;
    long double fourth = 0.0L;
    for (const double value : values) {
        const double delta = value - average;
        const double squared = delta * delta;
        second += static_cast<long double>(squared);
        fourth += static_cast<long double>(squared * squared);
    }
    const double second_moment = static_cast<double>(second) /
        static_cast<double>(values.size());
    const double fourth_moment = static_cast<double>(fourth) /
        static_cast<double>(values.size());
    if (second_moment <= 0.0) {
        return 0.0;
    }
    return fourth_moment / (second_moment * second_moment) - 3.0;
}

std::optional<double> ratio(const double numerator, const double denominator) {
    if (denominator == 0.0) {
        return std::nullopt;
    }
    return numerator / denominator;
}

std::vector<double> close_diffs(const BarView& view) {
    std::vector<double> values;
    values.reserve(view.size() > 0 ? view.size() - 1 : 0);
    for (std::size_t index = 1; index < view.size(); ++index) {
        values.push_back(view[index]->close - view[index - 1]->close);
    }
    return values;
}

std::vector<double> log_returns(
    const BarView& view,
    const std::size_t start_index = 0
) {
    std::vector<double> values;
    if (view.size() <= start_index + 1) {
        return values;
    }
    values.reserve(view.size() - start_index - 1);
    for (std::size_t index = start_index + 1; index < view.size(); ++index) {
        values.push_back(std::log(view[index]->close / view[index - 1]->close));
    }
    return values;
}

std::vector<double> signs_from_diffs(const std::vector<double>& diffs) {
    std::vector<double> signs;
    signs.reserve(diffs.size());
    for (const double value : diffs) {
        signs.push_back(value > 0.0 ? 1.0 : value < 0.0 ? -1.0 : 0.0);
    }
    return signs;
}

std::optional<double> ewm_last(const std::vector<double>& values, const int span) {
    if (values.empty()) {
        return std::nullopt;
    }
    const double alpha = 2.0 / (static_cast<double>(span) + 1.0);
    double result = values.front();
    for (std::size_t index = 1; index < values.size(); ++index) {
        result = alpha * values[index] + (1.0 - alpha) * result;
    }
    return result;
}

double sweep_bps(const OneSecondBar& bar, const bool buy) {
    const double high = buy ? bar.buy_price_high : bar.sell_price_high;
    const double low = buy ? bar.buy_price_low : bar.sell_price_low;
    const double quote = buy ? bar.buy_quote_qty : bar.sell_quote_qty;
    if (high <= 0.0 || low <= 0.0 || quote <= 0.0) {
        return 0.0;
    }
    const double midpoint = 0.5 * (high + low);
    return midpoint > 0.0 ? (high - low) / midpoint * 10'000.0 : 0.0;
}

void put(
    FeatureMap& result,
    std::string name,
    std::optional<double> value,
    std::int64_t source_ts,
    std::int64_t ready_ts,
    std::int64_t count,
    std::int64_t required = 1
) {
    result.emplace(std::move(name), feature_value(
        value, source_ts, ready_ts, count, required));
}

}  // namespace

const std::array<std::string_view, kCausalV12OneSecondFeatureCount>
    kCausalV12OneSecondFeatureNames = {
        "close", "volume", "buy_volume", "sell_volume", "trade_count", "buy_count",
        "sell_count", "tick_streak", "tick_mom_3s", "tick_mom_5s", "tick_mom_10s",
        "tick_ewm_3s", "tick_ewm_10s", "micro_ret_std", "micro_ret_skew",
        "micro_ret_kurt", "tick_reversal_freq", "flow_velocity", "flow_acceleration",
        "tick_streak_max", "tick_mom_range", "taker_quote_imbalance_5s",
        "taker_quote_imbalance_10s", "taker_quote_imbalance_30s",
        "taker_quote_imbalance_60s", "taker_signed_quote_sum_5s",
        "taker_signed_quote_sum_10s", "taker_signed_quote_sum_30s",
        "taker_signed_quote_sum_60s", "taker_trade_count_sum_5s",
        "taker_trade_count_sum_10s", "taker_trade_count_sum_30s",
        "taker_trade_count_sum_60s", "taker_max_same_side_run_5s",
        "taker_max_same_side_run_10s", "taker_max_same_side_run_30s",
        "taker_max_same_side_run_60s", "taker_buy_sweep_score_5s",
        "taker_buy_sweep_score_10s", "taker_buy_sweep_score_30s",
        "taker_buy_sweep_score_60s", "taker_sell_sweep_score_5s",
        "taker_sell_sweep_score_10s", "taker_sell_sweep_score_30s",
        "taker_sell_sweep_score_60s", "taker_buy_iceberg_pressure_sum_5s",
        "taker_buy_iceberg_pressure_sum_10s", "taker_buy_iceberg_pressure_sum_30s",
        "taker_buy_iceberg_pressure_sum_60s", "taker_sell_iceberg_pressure_sum_5s",
        "taker_sell_iceberg_pressure_sum_10s", "taker_sell_iceberg_pressure_sum_30s",
        "taker_sell_iceberg_pressure_sum_60s", "l2_spread_bps",
        "l2_microprice_offset_bps", "l2_imbalance_l1", "l2_imbalance_l3",
        "l2_imbalance_l5", "l2_imbalance_l10", "l2_near_depth_total",
        "l2_depth_slope", "l2_depth_convexity", "l2_queue_concentration",
        "l2_quote_flip_rate", "l2_book_refresh_ratio", "l2_book_cancel_ratio",
        "oi_log", "oi_pct_change", "oi_zscore_1h", "oi_zscore_6h", "oi_momentum",
        "toptrader_ls_ratio", "crowd_ls_ratio", "taker_ls_ratio",
        "toptrader_ls_zscore", "crowd_ls_zscore", "taker_ls_zscore",
        "taker_ls_momentum", "oi_price_divergence", "volatility_30s",
        "volatility_60s", "volatility_300s", "volume_imbalance",
        "volume_imbalance_30s", "volume_imbalance_60s", "volume_imbalance_300s",
        "trade_intensity_30s", "trade_intensity_60s", "trade_intensity_300s",
        "vpin_30s", "vpin_60s", "vpin_300s", "price_velocity",
        "price_acceleration", "price_change_30s", "price_change_60s",
        "price_change_300s", "volatility_5s", "volume_imbalance_5s",
        "trade_intensity_5s", "vpin_5s", "price_change_5s", "avg_trade_size",
        "avg_trade_size_60s", "large_trade_ratio", "volume_zscore", "bar_spread",
        "bar_spread_bps", "return_1", "return_abs", "vol_regime_6h",
        "vol_regime_24h", "vol_regime_zscore", "cv_ref_perp_basis_bps",
        "cv_ref_perp_ret_10s", "cv_ref_perp_ret_30s", "cv_ref_perp_ret_60s",
        "cv_ref_perp_volatility_60s", "cv_ref_perp_volume_imbalance",
        "cv_ref_perp_trade_intensity_60s", "cv_ref_perp_vpin_60s",
        "cv_ref_perp_basis_residual_bps", "cv_ref_perp_age_s",
        "cv_ref_perp_available", "cal_utc_hour", "cal_utc_weekday",
        "cal_utc_is_weekend", "cal_hour_sin", "cal_hour_cos", "cal_dow_sin",
        "cal_dow_cos", "cal_session_asia", "cal_session_tokyo",
        "cal_session_singapore_hk", "cal_session_europe", "cal_session_london",
        "cal_session_america", "cal_session_us_extended",
        "cal_session_asia_europe_overlap", "cal_session_europe_america_overlap",
        "cal_session_tokyo_singapore_overlap", "cal_session_london_us_overlap",
        "cal_session_active_count", "cal_cn_hour", "cal_cn_weekday",
        "cal_cn_is_weekend", "cal_cn_is_holiday", "cal_cn_is_adjusted_workday",
        "cal_cn_is_workday", "cal_cn_is_holiday_eve", "cal_cn_is_post_holiday",
        "cal_us_hour", "cal_us_weekday", "cal_us_is_weekend", "cal_us_is_sunday",
        "cal_us_is_sunday_evening", "cal_us_is_federal_holiday",
        "cal_us_is_nyse_trading_day", "cal_us_is_regular_hours",
        "cal_us_is_premarket", "cal_us_is_afterhours", "cal_us_is_holiday_eve",
        "cal_us_is_post_holiday", "cal_minutes_to_us_open", "cal_minutes_to_us_close",
        "cal_is_weekday_us_rth", "cal_is_weekend_core", "minutes_to_funding",
        "funding_phase", "funding_sin", "funding_cos", "dist_to_hour",
        "near_candle_close",
    };

}  // namespace narrowgate_cpp::f03

namespace narrowgate_cpp::f03 {
namespace {

constexpr std::size_t kBatchRecentLocalBars = 301;
constexpr std::size_t kBatchMaximumLocalBars = 604'801;
constexpr std::size_t kBatchMaximumReferenceBars = 3'601;
constexpr std::size_t kBatchMaximumMetricRows = 80;

constexpr std::array<std::string_view, kExecutionL2FeatureCount> kExecutionL2Names = {
    "l2_spread_bps",
    "l2_microprice_offset_bps",
    "l2_imbalance_l1",
    "l2_imbalance_l3",
    "l2_imbalance_l5",
    "l2_imbalance_l10",
    "l2_near_depth_total",
    "l2_depth_slope",
    "l2_depth_convexity",
    "l2_queue_concentration",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
};

std::optional<double> zscore(const std::vector<double>& values);
FeatureMap compute_trade_features(const BarView& view);
FeatureMap compute_local_microstructure(const BarView& view);
FeatureMap compute_calendar(std::int64_t cutoff);
void merge_features(FeatureMap& destination, FeatureMap source);

std::uint8_t lag_state_code(const std::string& value) {
    for (std::size_t index = 0; index < kFeatureLagStateVocabulary.size(); ++index) {
        if (value == kFeatureLagStateVocabulary[index]) {
            return static_cast<std::uint8_t>(index);
        }
    }
    throw std::logic_error("unknown F03 feature lag state: " + value);
}

template <typename Item, typename Timestamp>
void sort_unique_by_timestamp(
    std::vector<Item>& values,
    Timestamp timestamp,
    const char* source_name
) {
    std::sort(values.begin(), values.end(), [&](const Item& lhs, const Item& rhs) {
        return timestamp(lhs) < timestamp(rhs);
    });
    for (std::size_t index = 1; index < values.size(); ++index) {
        if (timestamp(values[index - 1]) == timestamp(values[index])) {
            throw std::invalid_argument(std::string("duplicate source clock in ") + source_name);
        }
    }
}

void validate_dense_daily_bars(
    std::vector<OneSecondBar>& bars,
    const char* source_name,
    const std::size_t maximum_rows
) {
    if (bars.empty()) {
        return;
    }
    if (bars.size() > maximum_rows) {
        throw std::invalid_argument(std::string(source_name) +
            " exceeds the frozen daily batch history bound");
    }
    sort_unique_by_timestamp(
        bars,
        [](const OneSecondBar& value) { return value.start_ts_ms; },
        source_name
    );
    for (std::size_t index = 0; index < bars.size(); ++index) {
        validate_bar(bars[index], source_name);
        if (bars[index].finalized_ts_ms != bars[index].start_ts_ms + kCadenceMs) {
            throw std::invalid_argument(std::string(source_name) +
                " daily batch requires completed bars visible exactly at t+1s");
        }
        if (index > 0 &&
            bars[index].start_ts_ms != bars[index - 1].start_ts_ms + kCadenceMs) {
            throw std::invalid_argument(std::string(source_name) +
                " daily batch requires a dense causal 1s source");
        }
    }
}

class RollingUpperMedian {
public:
    void insert(const double value) {
        if (!upper_.empty() && value >= *upper_.begin()) {
            upper_.insert(value);
        } else {
            lower_.insert(value);
        }
        rebalance();
    }

    void erase(const double value) {
        auto found = lower_.find(value);
        if (found != lower_.end()) {
            lower_.erase(found);
        } else {
            found = upper_.find(value);
            if (found == upper_.end()) {
                throw std::logic_error("rolling basis median erase missed its value");
            }
            upper_.erase(found);
        }
        rebalance();
    }

    std::size_t size() const noexcept {
        return lower_.size() + upper_.size();
    }

    double median() const {
        if (upper_.empty()) {
            throw std::logic_error("rolling basis median is empty");
        }
        return *upper_.begin();
    }

private:
    void rebalance() {
        const std::size_t target_lower = size() / 2;
        while (lower_.size() > target_lower) {
            auto item = std::prev(lower_.end());
            upper_.insert(*item);
            lower_.erase(item);
        }
        while (lower_.size() < target_lower) {
            if (upper_.empty()) {
                throw std::logic_error("rolling basis median rebalance underflow");
            }
            auto item = upper_.begin();
            lower_.insert(*item);
            upper_.erase(item);
        }
        if (!lower_.empty() && !upper_.empty() &&
            *std::prev(lower_.end()) > *upper_.begin()) {
            const double low = *std::prev(lower_.end());
            const double high = *upper_.begin();
            lower_.erase(std::prev(lower_.end()));
            upper_.erase(upper_.begin());
            lower_.insert(high);
            upper_.insert(low);
        }
    }

    std::multiset<double> lower_;
    std::multiset<double> upper_;
};

FeatureMap compute_metrics_batch(
    const std::vector<MetricObservation>& observations,
    const std::vector<OneSecondBar>& local_bars,
    const std::vector<std::int64_t>& local_starts,
    const std::size_t local_end,
    const std::int64_t cutoff
) {
    constexpr std::array<std::string_view, 13> metric_names = {
        "oi_log", "oi_pct_change", "oi_zscore_1h", "oi_zscore_6h", "oi_momentum",
        "toptrader_ls_ratio", "crowd_ls_ratio", "taker_ls_ratio",
        "toptrader_ls_zscore", "crowd_ls_zscore", "taker_ls_zscore",
        "taker_ls_momentum", "oi_price_divergence",
    };
    const auto source_end = std::lower_bound(
        observations.begin(), observations.end(), cutoff,
        [](const MetricObservation& item, const std::int64_t value) {
            return item.source_ts_ms < value;
        }
    );
    const std::size_t end = static_cast<std::size_t>(source_end - observations.begin());
    const std::size_t start = end > kBatchMaximumMetricRows
        ? end - kBatchMaximumMetricRows : 0;
    std::vector<const MetricObservation*> visible;
    visible.reserve(end - start);
    for (std::size_t index = start; index < end; ++index) {
        if (observations[index].feature_ready_ts_ms <= cutoff) {
            visible.push_back(&observations[index]);
        }
    }
    FeatureMap result;
    if (visible.empty() || cutoff - visible.back()->source_ts_ms > kMetricsMaxAgeMs) {
        for (const auto name : metric_names) {
            result.emplace(std::string(name),
                missing_value("metrics_missing_or_stale_no_default"));
        }
        return result;
    }

    const MetricObservation& current = *visible.back();
    const MetricObservation* prior = visible.size() >= 2 ? visible[visible.size() - 2] : nullptr;
    std::vector<const MetricObservation*> one_hour;
    std::vector<const MetricObservation*> six_hour;
    for (const auto* item : visible) {
        if (current.source_ts_ms - item->source_ts_ms <= 3'600'000) {
            one_hour.push_back(item);
        }
        if (current.source_ts_ms - item->source_ts_ms <= 21'600'000) {
            six_hour.push_back(item);
        }
    }
    auto collect = [](const auto& rows, auto member) {
        std::vector<double> values;
        values.reserve(rows.size());
        for (const auto* item : rows) {
            values.push_back(item->*member);
        }
        return values;
    };
    const std::vector<double> oi1 = collect(one_hour, &MetricObservation::sum_open_interest);
    const std::vector<double> oi6 = collect(six_hour, &MetricObservation::sum_open_interest);
    const std::vector<double> top6 = collect(six_hour, &MetricObservation::toptrader_ls_ratio);
    const std::vector<double> crowd6 = collect(six_hour, &MetricObservation::crowd_ls_ratio);
    const std::vector<double> taker1 = collect(one_hour, &MetricObservation::taker_ls_ratio);
    const std::vector<double> taker6 = collect(six_hour, &MetricObservation::taker_ls_ratio);
    const double previous_oi = prior == nullptr ? 0.0 : prior->sum_open_interest;
    const auto oi_pct = ratio(current.sum_open_interest - previous_oi, previous_oi);
    const auto oi_short = mean(oi1);
    const auto oi_long = mean(oi6);

    std::unordered_map<std::string, std::optional<double>> values;
    values["oi_log"] = current.sum_open_interest > 0.0
        ? std::optional<double>{std::log(current.sum_open_interest)} : std::nullopt;
    values["oi_pct_change"] = oi_pct;
    values["oi_zscore_1h"] = zscore(oi1);
    values["oi_zscore_6h"] = zscore(oi6);
    values["oi_momentum"] = ratio(
        oi_short.value_or(0.0) - oi_long.value_or(0.0), oi_long.value_or(0.0));
    values["toptrader_ls_ratio"] = current.toptrader_ls_ratio;
    values["crowd_ls_ratio"] = current.crowd_ls_ratio;
    values["taker_ls_ratio"] = current.taker_ls_ratio;
    values["toptrader_ls_zscore"] = zscore(top6);
    values["crowd_ls_zscore"] = zscore(crowd6);
    values["taker_ls_zscore"] = zscore(taker6);
    values["taker_ls_momentum"] =
        mean(taker1).value_or(0.0) - mean(taker6).value_or(0.0);
    values["oi_price_divergence"] = std::nullopt;
    if (prior != nullptr && previous_oi != 0.0) {
        const auto close_end = std::upper_bound(
            local_starts.begin(), local_starts.begin() +
                static_cast<std::ptrdiff_t>(local_end), prior->source_ts_ms);
        if (close_end != local_starts.begin()) {
            const std::size_t close_index = static_cast<std::size_t>(
                close_end - local_starts.begin() - 1);
            const double previous_close = local_bars[close_index].close;
            if (previous_close != 0.0 && oi_pct.has_value()) {
                const double price_change =
                    local_bars[local_end - 1].close / previous_close - 1.0;
                values["oi_price_divergence"] = *oi_pct - price_change;
            }
        }
    }
    for (const auto name : metric_names) {
        put(result, std::string(name), values[std::string(name)], current.source_ts_ms,
            current.feature_ready_ts_ms, static_cast<std::int64_t>(visible.size()));
    }
    return result;
}

FeatureMap compute_cross_market_batch(
    const std::vector<OneSecondBar>& reference_bars,
    const std::vector<std::int64_t>& reference_starts,
    const std::vector<std::optional<double>>& basis_median_before,
    const OneSecondBar& local_current,
    const std::int64_t cutoff
) {
    constexpr std::array<std::string_view, 11> names = {
        "cv_ref_perp_basis_bps", "cv_ref_perp_ret_10s", "cv_ref_perp_ret_30s",
        "cv_ref_perp_ret_60s", "cv_ref_perp_volatility_60s",
        "cv_ref_perp_volume_imbalance", "cv_ref_perp_trade_intensity_60s",
        "cv_ref_perp_vpin_60s", "cv_ref_perp_basis_residual_bps",
        "cv_ref_perp_age_s", "cv_ref_perp_available",
    };
    FeatureMap result;
    const auto end_it = std::lower_bound(reference_starts.begin(), reference_starts.end(), cutoff);
    const std::size_t end = static_cast<std::size_t>(end_it - reference_starts.begin());
    if (end == 0 ||
        cutoff - (reference_bars[end - 1].start_ts_ms + kCadenceMs) >
            kCrossSourceMaxAgeMs) {
        for (const auto name : names) {
            result.emplace(std::string(name),
                missing_value("cross_market_missing_or_stale_no_forward_fill"));
        }
        return result;
    }
    const std::size_t start = end > kBatchMaximumReferenceBars
        ? end - kBatchMaximumReferenceBars : 0;
    const OneSecondBar& latest = reference_bars[end - 1];
    const std::size_t count = end - start;
    const double local_close = local_current.close;
    const double basis = (latest.close - local_close) / local_close * 10'000.0;
    std::unordered_map<std::string, std::optional<double>> values;
    values["cv_ref_perp_basis_bps"] = basis;
    values["cv_ref_perp_age_s"] = std::max(
        0.0, static_cast<double>(cutoff - latest.finalized_ts_ms) / 1'000.0);
    values["cv_ref_perp_available"] = 1.0;
    for (const int seconds : {10, 30, 60}) {
        const std::size_t needed = static_cast<std::size_t>(seconds + 1);
        values["cv_ref_perp_ret_" + std::to_string(seconds) + "s"] = count >= needed
            ? std::optional<double>{latest.close / reference_bars[end - needed].close - 1.0}
            : std::nullopt;
    }
    const std::size_t return_start = count > 61 ? end - 61 : start;
    std::vector<double> returns60;
    returns60.reserve(end - return_start);
    for (std::size_t index = return_start + 1; index < end; ++index) {
        returns60.push_back(std::log(
            reference_bars[index].close / reference_bars[index - 1].close));
    }
    values["cv_ref_perp_volatility_60s"] = returns60.size() >= 2
        ? std::optional<double>{sample_std(returns60).value_or(0.0) * std::sqrt(60.0)}
        : std::nullopt;
    const std::size_t volume_start = count > 60 ? end - 60 : start;
    long double buy = 0.0L;
    long double sell = 0.0L;
    long double intensity = 0.0L;
    long double absolute = 0.0L;
    for (std::size_t index = volume_start; index < end; ++index) {
        const auto& bar = reference_bars[index];
        buy += bar.buy_volume;
        sell += bar.sell_volume;
        intensity += static_cast<double>(bar.trade_count);
        absolute += std::abs(bar.buy_volume - bar.sell_volume);
    }
    const double buy_value = static_cast<double>(buy);
    const double sell_value = static_cast<double>(sell);
    const double total = buy_value + sell_value;
    values["cv_ref_perp_volume_imbalance"] = ratio(buy_value - sell_value, total);
    values["cv_ref_perp_trade_intensity_60s"] = static_cast<double>(
        intensity / static_cast<long double>(end - volume_start));
    values["cv_ref_perp_vpin_60s"] = ratio(static_cast<double>(absolute), total);
    values["cv_ref_perp_basis_residual_bps"] = basis_median_before[end - 1].has_value()
        ? std::optional<double>{basis - *basis_median_before[end - 1]} : std::nullopt;
    for (const auto name : names) {
        put(result, std::string(name), values[std::string(name)], latest.start_ts_ms,
            latest.finalized_ts_ms, static_cast<std::int64_t>(count));
    }
    return result;
}

}  // namespace

class CausalV12OneSecondBatchEngine::Impl {
public:
    Impl(
        std::vector<OneSecondBar> local_bars,
        std::vector<ExecutionL2Observation> execution_l2,
        std::vector<MetricObservation> metrics,
        std::vector<OneSecondBar> reference_bars
    ) : local_bars_(std::move(local_bars)),
        execution_l2_(std::move(execution_l2)),
        metrics_(std::move(metrics)),
        reference_bars_(std::move(reference_bars)) {
        validate_dense_daily_bars(
            local_bars_, "binance_futures_btcusdc_completed_1s_bar",
            kBatchMaximumLocalBars);
        if (local_bars_.empty()) {
            throw std::invalid_argument("F03 daily batch requires local 1s bars");
        }
        validate_dense_daily_bars(
            reference_bars_, "reference_perp_completed_1s_bar",
            kBatchMaximumLocalBars);
        validate_auxiliary_sources();
        prepare_local_state();
        prepare_reference_state();
    }

    FeatureRow compute_one(const std::int64_t cutoff, const std::int64_t decision) const {
        if (cutoff <= 0 || cutoff % kCadenceMs != 0) {
            throw std::invalid_argument(
                "cutoff_exclusive_ms must be a positive canonical 1s edge");
        }
        if (decision < cutoff) {
            throw std::invalid_argument("decision precedes the feature cutoff");
        }
        const auto end_it = std::lower_bound(local_starts_.begin(), local_starts_.end(), cutoff);
        const std::size_t end = static_cast<std::size_t>(end_it - local_starts_.begin());
        if (end == 0 || local_bars_[end - 1].start_ts_ms != cutoff - kCadenceMs) {
            throw std::invalid_argument(
                "latest completed 1s bar is missing or late for daily batch");
        }
        const std::size_t recent_start = end > kBatchRecentLocalBars
            ? end - kBatchRecentLocalBars : 0;
        BarView recent_view;
        recent_view.reserve(end - recent_start);
        for (std::size_t index = recent_start; index < end; ++index) {
            recent_view.push_back(&local_bars_[index]);
        }
        FeatureMap values = compute_trade_features(recent_view);
        const OneSecondBar& current = local_bars_[end - 1];
        const std::int64_t source_ts = current.start_ts_ms;
        const std::int64_t ready_ts = current.finalized_ts_ms;
        values["tick_streak"] = feature_value(
            end >= 2 ? std::optional<double>{streak_[end - 1]} : std::nullopt,
            source_ts, ready_ts, static_cast<std::int64_t>(end), 2);
        values["tick_ewm_3s"] = feature_value(
            end >= 2 ? std::optional<double>{ewm3_[end - 1]} : std::nullopt,
            source_ts, ready_ts, static_cast<std::int64_t>(end), 2);
        values["tick_ewm_10s"] = feature_value(
            end >= 2 ? std::optional<double>{ewm10_[end - 1]} : std::nullopt,
            source_ts, ready_ts, static_cast<std::int64_t>(end), 2);
        std::optional<double> maximum_streak;
        const std::size_t streak_start = end > 11 ? end - 10 : 1;
        for (std::size_t index = streak_start; index < end; ++index) {
            maximum_streak = std::max(
                maximum_streak.value_or(0.0), std::abs(streak_[index]));
        }
        values["tick_streak_max"] = feature_value(
            maximum_streak, source_ts, ready_ts,
            static_cast<std::int64_t>(end > 1 ? std::min<std::size_t>(10, end - 1) : 0));
        values["flow_acceleration"].observation_count = static_cast<std::int64_t>(end);

        merge_features(values, compute_execution_l2_batch(cutoff));
        merge_features(values, compute_metrics_batch(
            metrics_, local_bars_, local_starts_, end, cutoff));
        FeatureMap local = compute_local_microstructure(recent_view);
        for (const auto name : {
                 "price_velocity", "price_acceleration", "return_1", "return_abs"}) {
            local[name].observation_count = static_cast<std::int64_t>(end);
        }
        apply_volatility_regime(local, end, source_ts, ready_ts);
        merge_features(values, std::move(local));
        merge_features(values, compute_cross_market_batch(
            reference_bars_, reference_starts_, reference_basis_median_before_,
            current, cutoff));
        merge_features(values, compute_calendar(cutoff));
        if (values.size() != kCausalV12OneSecondFeatureCount) {
            throw std::logic_error("F03 daily batch did not generate 173 features");
        }

        FeatureRow row;
        row.cutoff_exclusive_ms = cutoff;
        row.decision_ts_ms = decision;
        std::int64_t row_ready = 0;
        for (std::size_t index = 0; index < kCausalV12OneSecondFeatureCount; ++index) {
            const std::string name{kCausalV12OneSecondFeatureNames[index]};
            const auto found = values.find(name);
            if (found == values.end()) {
                throw std::logic_error("missing F03 daily batch feature: " + name);
            }
            row.values[index] = found->second;
            if (found->second.feature_ready_ts_ms.has_value()) {
                row_ready = std::max(row_ready, *found->second.feature_ready_ts_ms);
            }
        }
        if (row_ready > cutoff || row_ready > decision) {
            throw std::logic_error("F03 daily batch violates feature-ready causality");
        }
        row.feature_ready_ts_ms = row_ready;
        return row;
    }

    std::size_t local_bar_count() const noexcept { return local_bars_.size(); }
    std::size_t reference_bar_count() const noexcept { return reference_bars_.size(); }

private:
    void validate_auxiliary_sources() {
        sort_unique_by_timestamp(
            execution_l2_,
            [](const ExecutionL2Observation& value) { return value.bucket_start_ts_ms; },
            "execution_l2"
        );
        for (std::size_t index = 0; index < execution_l2_.size(); ++index) {
            const auto& item = execution_l2_[index];
            if (item.bucket_start_ts_ms <= 0 || item.bucket_start_ts_ms % kCadenceMs != 0) {
                throw std::invalid_argument("execution L2 bucket must be a canonical 1s edge");
            }
            if (item.feature_ready_ts_ms < item.bucket_start_ts_ms + kCadenceMs) {
                throw std::invalid_argument("execution L2 cannot be ready before bucket end");
            }
            for (const double value : item.values) {
                if (!std::isfinite(value)) {
                    throw std::invalid_argument("execution L2 values must be finite");
                }
            }
            l2_index_.emplace(item.bucket_start_ts_ms, index);
        }
        sort_unique_by_timestamp(
            metrics_,
            [](const MetricObservation& value) { return value.source_ts_ms; },
            "metrics"
        );
        for (const auto& item : metrics_) {
            if (item.source_ts_ms <= 0 || item.feature_ready_ts_ms < item.source_ts_ms) {
                throw std::invalid_argument("invalid metrics causal clock");
            }
            const std::array metric_values{
                item.sum_open_interest, item.toptrader_ls_ratio,
                item.crowd_ls_ratio, item.taker_ls_ratio};
            for (const double value : metric_values) {
                if (!std::isfinite(value) || value < 0.0) {
                    throw std::invalid_argument(
                        "metric values must be finite and non-negative");
                }
            }
        }
    }

    void prepare_local_state() {
        const std::size_t count = local_bars_.size();
        local_starts_.reserve(count);
        diff_.assign(count, 0.0);
        streak_.assign(count, 0.0);
        ewm3_.assign(count, 0.0);
        ewm10_.assign(count, 0.0);
        absolute_return_prefix_.assign(count, 0.0L);
        double prior_sign = 0.0;
        for (std::size_t index = 0; index < count; ++index) {
            local_starts_.push_back(local_bars_[index].start_ts_ms);
            if (index == 0) {
                continue;
            }
            const double value = local_bars_[index].close - local_bars_[index - 1].close;
            diff_[index] = value;
            const double sign = value > 0.0 ? 1.0 : value < 0.0 ? -1.0 : 0.0;
            streak_[index] = sign != 0.0 && sign == prior_sign
                ? streak_[index - 1] + sign : sign;
            prior_sign = sign;
            ewm3_[index] = index == 1 ? value : 0.5 * value + 0.5 * ewm3_[index - 1];
            constexpr double alpha10 = 2.0 / 11.0;
            ewm10_[index] = index == 1 ? value
                : alpha10 * value + (1.0 - alpha10) * ewm10_[index - 1];
            absolute_return_prefix_[index] = absolute_return_prefix_[index - 1] +
                std::abs(std::log(
                    local_bars_[index].close / local_bars_[index - 1].close));
        }
        for (std::size_t end = 21'600; end < count; end += 3'600) {
            const long double total = absolute_return_prefix_[end] -
                absolute_return_prefix_[end - 21'600];
            volatility_block_ends_.push_back(end);
            volatility_block_means_.push_back(static_cast<double>(total / 21'600.0L));
        }
    }

    void prepare_reference_state() {
        reference_starts_.reserve(reference_bars_.size());
        std::unordered_map<std::int64_t, double> local_close;
        local_close.reserve(local_bars_.size());
        for (const auto& bar : local_bars_) {
            local_close.emplace(bar.start_ts_ms, bar.close);
        }
        std::vector<std::optional<double>> basis(reference_bars_.size());
        for (std::size_t index = 0; index < reference_bars_.size(); ++index) {
            const auto& bar = reference_bars_[index];
            reference_starts_.push_back(bar.start_ts_ms);
            const auto found = local_close.find(bar.start_ts_ms);
            if (found != local_close.end() && found->second != 0.0) {
                basis[index] = (bar.close - found->second) / found->second * 10'000.0;
            }
        }
        reference_basis_median_before_.resize(reference_bars_.size());
        RollingUpperMedian median;
        for (std::size_t index = 0; index < reference_bars_.size(); ++index) {
            if (index > 0 && basis[index - 1].has_value()) {
                median.insert(*basis[index - 1]);
            }
            if (index > 3'600 && basis[index - 3'601].has_value()) {
                median.erase(*basis[index - 3'601]);
            }
            if (median.size() >= 30) {
                reference_basis_median_before_[index] = median.median();
            }
        }
    }

    FeatureMap compute_execution_l2_batch(const std::int64_t cutoff) const {
        const std::int64_t target = cutoff - kCadenceMs;
        const auto found = l2_index_.find(target);
        FeatureMap result;
        if (found == l2_index_.end()) {
            for (const auto name : kExecutionL2Names) {
                result.emplace(std::string(name),
                    missing_value("execution_l2_exact_bucket_missing_no_carry"));
            }
            return result;
        }
        const auto& item = execution_l2_[found->second];
        if (item.feature_ready_ts_ms > cutoff) {
            for (const auto name : kExecutionL2Names) {
                result.emplace(std::string(name),
                    missing_value("execution_l2_late_at_cutoff"));
            }
            return result;
        }
        for (std::size_t index = 0; index < kExecutionL2Names.size(); ++index) {
            put(result, std::string(kExecutionL2Names[index]), item.values[index],
                item.bucket_start_ts_ms, item.feature_ready_ts_ms, 1);
        }
        return result;
    }

    void apply_volatility_regime(
        FeatureMap& values,
        const std::size_t end,
        const std::int64_t source_ts,
        const std::int64_t ready_ts
    ) const {
        const std::size_t absolute_count = end - 1;
        auto trailing_mean = [&](const std::size_t maximum) -> std::optional<double> {
            const std::size_t count = std::min(absolute_count, maximum);
            if (count == 0) {
                return std::nullopt;
            }
            const long double total = absolute_return_prefix_[absolute_count] -
                absolute_return_prefix_[absolute_count - count];
            return static_cast<double>(total / static_cast<long double>(count));
        };
        const std::size_t count6 = std::min<std::size_t>(absolute_count, 21'600);
        const std::size_t count24 = std::min<std::size_t>(absolute_count, 86'400);
        const auto vol6 = trailing_mean(21'600);
        const auto vol24 = trailing_mean(86'400);
        values["vol_regime_6h"] = feature_value(
            vol6, source_ts, ready_ts, static_cast<std::int64_t>(count6), 3'600);
        values["vol_regime_24h"] = feature_value(
            vol24, source_ts, ready_ts, static_cast<std::int64_t>(count24), 21'600);

        const auto block_end = std::upper_bound(
            volatility_block_ends_.begin(), volatility_block_ends_.end(), absolute_count);
        const std::size_t block_count = static_cast<std::size_t>(
            block_end - volatility_block_ends_.begin());
        std::vector<double> blocks(
            volatility_block_means_.begin(),
            volatility_block_means_.begin() + static_cast<std::ptrdiff_t>(block_count));
        const auto average = mean(blocks);
        const auto deviation = sample_std(blocks);
        std::optional<double> zvalue;
        if (vol6.has_value() && average.has_value() && deviation.has_value() &&
            *deviation != 0.0) {
            zvalue = (*vol6 - *average) / *deviation;
        }
        values["vol_regime_zscore"] = feature_value(
            zvalue, source_ts, ready_ts, static_cast<std::int64_t>(block_count), 24);
    }

    std::vector<OneSecondBar> local_bars_;
    std::vector<ExecutionL2Observation> execution_l2_;
    std::vector<MetricObservation> metrics_;
    std::vector<OneSecondBar> reference_bars_;
    std::vector<std::int64_t> local_starts_;
    std::vector<std::int64_t> reference_starts_;
    std::unordered_map<std::int64_t, std::size_t> l2_index_;
    std::vector<double> diff_;
    std::vector<double> streak_;
    std::vector<double> ewm3_;
    std::vector<double> ewm10_;
    std::vector<long double> absolute_return_prefix_;
    std::vector<std::size_t> volatility_block_ends_;
    std::vector<double> volatility_block_means_;
    std::vector<std::optional<double>> reference_basis_median_before_;
};

CausalV12OneSecondBatchEngine::CausalV12OneSecondBatchEngine(
    std::vector<OneSecondBar> local_bars,
    std::vector<ExecutionL2Observation> execution_l2,
    std::vector<MetricObservation> metrics,
    std::vector<OneSecondBar> reference_bars
) : impl_(std::make_unique<Impl>(
        std::move(local_bars), std::move(execution_l2),
        std::move(metrics), std::move(reference_bars))) {}

CausalV12OneSecondBatchEngine::~CausalV12OneSecondBatchEngine() = default;
CausalV12OneSecondBatchEngine::CausalV12OneSecondBatchEngine(
    CausalV12OneSecondBatchEngine&&) noexcept = default;
CausalV12OneSecondBatchEngine& CausalV12OneSecondBatchEngine::operator=(
    CausalV12OneSecondBatchEngine&&) noexcept = default;

FeatureBatch CausalV12OneSecondBatchEngine::compute(
    const std::vector<std::int64_t>& cutoffs,
    const std::vector<std::int64_t>& decisions
) const {
    if (!decisions.empty() && decisions.size() != cutoffs.size()) {
        throw std::invalid_argument("decision timestamp batch length mismatch");
    }
    for (std::size_t index = 1; index < cutoffs.size(); ++index) {
        if (cutoffs[index] <= cutoffs[index - 1]) {
            throw std::invalid_argument("batch cutoffs must be unique and strictly increasing");
        }
    }
    FeatureBatch output;
    output.row_count = cutoffs.size();
    output.cutoff_exclusive_ms = cutoffs;
    output.decision_ts_ms.reserve(cutoffs.size());
    output.feature_ready_ts_ms.reserve(cutoffs.size());
    const std::size_t cells = cutoffs.size() * kCausalV12OneSecondFeatureCount;
    output.values.reserve(cells);
    output.valid.reserve(cells);
    output.source_latest_ts_ms.reserve(cells);
    output.feature_ready_ts_ms_by_feature.reserve(cells);
    output.observation_count.reserve(cells);
    output.lag_state_code.reserve(cells);
    for (std::size_t row_index = 0; row_index < cutoffs.size(); ++row_index) {
        const std::int64_t decision = decisions.empty()
            ? cutoffs[row_index] : decisions[row_index];
        const FeatureRow row = impl_->compute_one(cutoffs[row_index], decision);
        output.decision_ts_ms.push_back(row.decision_ts_ms);
        output.feature_ready_ts_ms.push_back(row.feature_ready_ts_ms);
        for (const auto& item : row.values) {
            output.values.push_back(item.value.value_or(
                std::numeric_limits<double>::quiet_NaN()));
            output.valid.push_back(item.value.has_value() ? 1 : 0);
            output.source_latest_ts_ms.push_back(
                item.source_latest_ts_ms.value_or(-1));
            output.feature_ready_ts_ms_by_feature.push_back(
                item.feature_ready_ts_ms.value_or(-1));
            output.observation_count.push_back(item.observation_count);
            output.lag_state_code.push_back(lag_state_code(item.lag_state));
        }
    }
    return output;
}

std::size_t CausalV12OneSecondBatchEngine::local_bar_count() const noexcept {
    return impl_->local_bar_count();
}

std::size_t CausalV12OneSecondBatchEngine::reference_bar_count() const noexcept {
    return impl_->reference_bar_count();
}

}  // namespace narrowgate_cpp::f03

namespace narrowgate_cpp::f03 {
namespace {

FeatureMap compute_execution_l2(
    const std::vector<ExecutionL2Observation>& observations,
    const std::int64_t cutoff
) {
    std::unordered_set<std::int64_t> starts;
    const ExecutionL2Observation* matching = nullptr;
    const std::int64_t target = cutoff - kCadenceMs;
    for (const auto& item : observations) {
        if (item.bucket_start_ts_ms <= 0 || item.bucket_start_ts_ms % kCadenceMs != 0) {
            throw std::invalid_argument("execution L2 bucket must be a canonical 1s edge");
        }
        if (item.feature_ready_ts_ms < item.bucket_start_ts_ms + kCadenceMs) {
            throw std::invalid_argument("execution L2 cannot be ready before bucket end");
        }
        if (!starts.insert(item.bucket_start_ts_ms).second) {
            throw std::invalid_argument("duplicate execution L2 source clock");
        }
        for (const double value : item.values) {
            if (!std::isfinite(value)) {
                throw std::invalid_argument("execution L2 values must be finite");
            }
        }
        if (item.bucket_start_ts_ms == target) {
            matching = &item;
        }
    }
    FeatureMap result;
    if (matching == nullptr) {
        for (const auto name : kExecutionL2Names) {
            result.emplace(std::string(name),
                missing_value("execution_l2_exact_bucket_missing_no_carry"));
        }
        return result;
    }
    if (matching->feature_ready_ts_ms > cutoff) {
        for (const auto name : kExecutionL2Names) {
            result.emplace(std::string(name), missing_value("execution_l2_late_at_cutoff"));
        }
        return result;
    }
    for (std::size_t index = 0; index < kExecutionL2Names.size(); ++index) {
        put(result, std::string(kExecutionL2Names[index]), matching->values[index],
            matching->bucket_start_ts_ms, matching->feature_ready_ts_ms, 1);
    }
    return result;
}

std::optional<double> zscore(const std::vector<double>& values) {
    const auto average = mean(values);
    const auto stddev = sample_std(values);
    if (!average.has_value() || !stddev.has_value() || *stddev == 0.0) {
        return std::nullopt;
    }
    return (values.back() - *average) / *stddev;
}

std::optional<double> close_at_or_before(
    const BarView& view,
    const std::int64_t timestamp_ms
) {
    std::optional<double> result;
    for (const auto* bar : view) {
        if (bar->start_ts_ms > timestamp_ms) {
            break;
        }
        result = bar->close;
    }
    return result;
}

FeatureMap compute_metrics(
    const std::vector<MetricObservation>& observations,
    const BarView& local_view,
    const std::int64_t cutoff
) {
    std::unordered_set<std::int64_t> source_times;
    std::vector<const MetricObservation*> visible;
    for (const auto& item : observations) {
        if (item.source_ts_ms <= 0) {
            throw std::invalid_argument("metric source timestamp must be positive");
        }
        if (item.feature_ready_ts_ms < item.source_ts_ms) {
            throw std::invalid_argument("metric feature-ready time precedes source time");
        }
        const std::array values{
            item.sum_open_interest,
            item.toptrader_ls_ratio,
            item.crowd_ls_ratio,
            item.taker_ls_ratio,
        };
        for (const double value : values) {
            if (!std::isfinite(value) || value < 0.0) {
                throw std::invalid_argument(
                    "metric values must be finite and non-negative");
            }
        }
        if (!source_times.insert(item.source_ts_ms).second) {
            throw std::invalid_argument("duplicate metrics source clock");
        }
        if (item.source_ts_ms < cutoff && item.feature_ready_ts_ms <= cutoff) {
            visible.push_back(&item);
        }
    }
    std::sort(visible.begin(), visible.end(), [](const auto* lhs, const auto* rhs) {
        return lhs->source_ts_ms < rhs->source_ts_ms;
    });

    constexpr std::array<std::string_view, 13> metric_names = {
        "oi_log",
        "oi_pct_change",
        "oi_zscore_1h",
        "oi_zscore_6h",
        "oi_momentum",
        "toptrader_ls_ratio",
        "crowd_ls_ratio",
        "taker_ls_ratio",
        "toptrader_ls_zscore",
        "crowd_ls_zscore",
        "taker_ls_zscore",
        "taker_ls_momentum",
        "oi_price_divergence",
    };
    FeatureMap result;
    if (visible.empty() || cutoff - visible.back()->source_ts_ms > kMetricsMaxAgeMs) {
        for (const auto name : metric_names) {
            result.emplace(std::string(name),
                missing_value("metrics_missing_or_stale_no_default"));
        }
        return result;
    }

    const MetricObservation& current = *visible.back();
    const MetricObservation* prior = visible.size() >= 2 ? visible[visible.size() - 2] : nullptr;
    std::vector<const MetricObservation*> one_hour;
    std::vector<const MetricObservation*> six_hour;
    for (const auto* item : visible) {
        if (current.source_ts_ms - item->source_ts_ms <= 3'600'000) {
            one_hour.push_back(item);
        }
        if (current.source_ts_ms - item->source_ts_ms <= 21'600'000) {
            six_hour.push_back(item);
        }
    }
    auto collect = [](const auto& rows, auto member) {
        std::vector<double> values;
        values.reserve(rows.size());
        for (const auto* item : rows) {
            values.push_back(item->*member);
        }
        return values;
    };
    const std::vector<double> oi1 = collect(one_hour, &MetricObservation::sum_open_interest);
    const std::vector<double> oi6 = collect(six_hour, &MetricObservation::sum_open_interest);
    const std::vector<double> top6 = collect(six_hour, &MetricObservation::toptrader_ls_ratio);
    const std::vector<double> crowd6 = collect(six_hour, &MetricObservation::crowd_ls_ratio);
    const std::vector<double> taker1 = collect(one_hour, &MetricObservation::taker_ls_ratio);
    const std::vector<double> taker6 = collect(six_hour, &MetricObservation::taker_ls_ratio);
    const double previous_oi = prior == nullptr ? 0.0 : prior->sum_open_interest;
    const auto oi_pct = ratio(current.sum_open_interest - previous_oi, previous_oi);
    const auto oi_short = mean(oi1);
    const auto oi_long = mean(oi6);

    std::unordered_map<std::string, std::optional<double>> values;
    values["oi_log"] = current.sum_open_interest > 0.0
        ? std::optional<double>{std::log(current.sum_open_interest)}
        : std::nullopt;
    values["oi_pct_change"] = oi_pct;
    values["oi_zscore_1h"] = zscore(oi1);
    values["oi_zscore_6h"] = zscore(oi6);
    values["oi_momentum"] = ratio(
        oi_short.value_or(0.0) - oi_long.value_or(0.0), oi_long.value_or(0.0));
    values["toptrader_ls_ratio"] = current.toptrader_ls_ratio;
    values["crowd_ls_ratio"] = current.crowd_ls_ratio;
    values["taker_ls_ratio"] = current.taker_ls_ratio;
    values["toptrader_ls_zscore"] = zscore(top6);
    values["crowd_ls_zscore"] = zscore(crowd6);
    values["taker_ls_zscore"] = zscore(taker6);
    values["taker_ls_momentum"] =
        mean(taker1).value_or(0.0) - mean(taker6).value_or(0.0);
    values["oi_price_divergence"] = std::nullopt;
    if (prior != nullptr && previous_oi != 0.0) {
        const auto prior_close = close_at_or_before(local_view, prior->source_ts_ms);
        if (prior_close.has_value() && *prior_close != 0.0 && oi_pct.has_value()) {
            const double price_change = local_view.back()->close / *prior_close - 1.0;
            values["oi_price_divergence"] = *oi_pct - price_change;
        }
    }
    for (const auto name : metric_names) {
        put(result, std::string(name), values[std::string(name)], current.source_ts_ms,
            current.feature_ready_ts_ms, static_cast<std::int64_t>(visible.size()));
    }
    return result;
}

std::optional<BarView> optional_reference_tail(
    const std::vector<OneSecondBar>& bars,
    const std::int64_t cutoff
) {
    BarView candidates;
    std::unordered_set<std::int64_t> starts;
    for (const auto& bar : bars) {
        validate_bar(bar, "reference_perp_completed_1s_bar");
        if (bar.start_ts_ms >= cutoff) {
            continue;
        }
        if (!starts.insert(bar.start_ts_ms).second) {
            throw std::invalid_argument("duplicate reference source clock");
        }
        if (bar.finalized_ts_ms <= cutoff) {
            candidates.push_back(&bar);
        }
    }
    std::sort(candidates.begin(), candidates.end(), [](const auto* lhs, const auto* rhs) {
        return lhs->start_ts_ms < rhs->start_ts_ms;
    });
    if (candidates.empty()) {
        return std::nullopt;
    }
    const std::int64_t latest_end = candidates.back()->start_ts_ms + kCadenceMs;
    if (cutoff - latest_end > kCrossSourceMaxAgeMs) {
        return std::nullopt;
    }
    std::size_t start = candidates.size() - 1;
    while (start > 0 &&
        candidates[start]->start_ts_ms == candidates[start - 1]->start_ts_ms + kCadenceMs) {
        --start;
    }
    return BarView(candidates.begin() + static_cast<std::ptrdiff_t>(start), candidates.end());
}

FeatureMap compute_cross_market(
    const std::vector<OneSecondBar>& reference_bars,
    const BarView& local_view,
    const std::int64_t cutoff
) {
    constexpr std::array<std::string_view, 11> names = {
        "cv_ref_perp_basis_bps",
        "cv_ref_perp_ret_10s",
        "cv_ref_perp_ret_30s",
        "cv_ref_perp_ret_60s",
        "cv_ref_perp_volatility_60s",
        "cv_ref_perp_volume_imbalance",
        "cv_ref_perp_trade_intensity_60s",
        "cv_ref_perp_vpin_60s",
        "cv_ref_perp_basis_residual_bps",
        "cv_ref_perp_age_s",
        "cv_ref_perp_available",
    };
    FeatureMap result;
    const auto optional_view = optional_reference_tail(reference_bars, cutoff);
    if (!optional_view.has_value()) {
        for (const auto name : names) {
            result.emplace(std::string(name),
                missing_value("cross_market_missing_or_stale_no_forward_fill"));
        }
        return result;
    }
    const BarView& view = *optional_view;
    const OneSecondBar& latest = *view.back();
    const double local_close = local_view.back()->close;
    const double basis = (latest.close - local_close) / local_close * 10'000.0;
    std::unordered_map<std::string, std::optional<double>> values;
    values["cv_ref_perp_basis_bps"] = basis;
    values["cv_ref_perp_age_s"] = std::max(
        0.0, static_cast<double>(cutoff - latest.finalized_ts_ms) / 1'000.0);
    values["cv_ref_perp_available"] = 1.0;
    for (const int seconds : {10, 30, 60}) {
        const std::size_t count = std::min<std::size_t>(view.size(), seconds + 1);
        values["cv_ref_perp_ret_" + std::to_string(seconds) + "s"] =
            count == static_cast<std::size_t>(seconds + 1)
                ? std::optional<double>{
                      view.back()->close / view[view.size() - count]->close - 1.0}
                : std::nullopt;
    }
    const std::size_t return_start = tail_start(view.size(), 61);
    const std::vector<double> returns60 = log_returns(view, return_start);
    values["cv_ref_perp_volatility_60s"] = returns60.size() >= 2
        ? std::optional<double>{sample_std(returns60).value_or(0.0) * std::sqrt(60.0)}
        : std::nullopt;
    const std::size_t volume_start = tail_start(view.size(), 60);
    double buy = 0.0;
    double sell = 0.0;
    double intensity = 0.0;
    double absolute = 0.0;
    for (std::size_t index = volume_start; index < view.size(); ++index) {
        const auto& bar = *view[index];
        buy += bar.buy_volume;
        sell += bar.sell_volume;
        intensity += static_cast<double>(bar.trade_count);
        absolute += std::abs(bar.buy_volume - bar.sell_volume);
    }
    const double total = buy + sell;
    values["cv_ref_perp_volume_imbalance"] = ratio(buy - sell, total);
    values["cv_ref_perp_trade_intensity_60s"] =
        intensity / static_cast<double>(view.size() - volume_start);
    values["cv_ref_perp_vpin_60s"] = ratio(absolute, total);

    std::unordered_map<std::int64_t, double> local_by_ts;
    for (const auto* bar : local_view) {
        local_by_ts.emplace(bar->start_ts_ms, bar->close);
    }
    std::vector<double> historical_basis;
    const std::size_t history_end = view.size() - 1;
    const std::size_t history_start = history_end > 3'600 ? history_end - 3'600 : 0;
    for (std::size_t index = history_start; index < history_end; ++index) {
        const auto found = local_by_ts.find(view[index]->start_ts_ms);
        if (found == local_by_ts.end()) {
            continue;
        }
        historical_basis.push_back(
            (view[index]->close - found->second) / found->second * 10'000.0);
    }
    if (historical_basis.size() >= 30) {
        std::sort(historical_basis.begin(), historical_basis.end());
        values["cv_ref_perp_basis_residual_bps"] =
            basis - historical_basis[historical_basis.size() / 2];
    } else {
        values["cv_ref_perp_basis_residual_bps"] = std::nullopt;
    }
    for (const auto name : names) {
        put(result, std::string(name), values[std::string(name)], latest.start_ts_ms,
            latest.finalized_ts_ms, static_cast<std::int64_t>(view.size()));
    }
    return result;
}

}  // namespace
}  // namespace narrowgate_cpp::f03

namespace narrowgate_cpp::f03 {
namespace {

FeatureMap compute_trade_features(const BarView& view) {
    FeatureMap result;
    const OneSecondBar& current = *view.back();
    const std::int64_t source_ts = current.start_ts_ms;
    const std::int64_t ready_ts = current.finalized_ts_ms;
    put(result, "close", current.close, source_ts, ready_ts, 1);
    put(result, "volume", current.volume, source_ts, ready_ts, 1);
    put(result, "buy_volume", current.buy_volume, source_ts, ready_ts, 1);
    put(result, "sell_volume", current.sell_volume, source_ts, ready_ts, 1);
    put(result, "trade_count", static_cast<double>(current.trade_count), source_ts, ready_ts, 1);
    put(result, "buy_count", static_cast<double>(current.buy_count), source_ts, ready_ts, 1);
    put(result, "sell_count", static_cast<double>(current.sell_count), source_ts, ready_ts, 1);

    const std::vector<double> diffs = close_diffs(view);
    const std::vector<double> signs = signs_from_diffs(diffs);
    double streak = 0.0;
    double previous = 0.0;
    std::vector<double> streak_history;
    streak_history.reserve(signs.size());
    for (const double sign : signs) {
        streak = sign != 0.0 && sign == previous ? streak + sign : sign;
        streak_history.push_back(streak);
        previous = sign;
    }
    put(
        result,
        "tick_streak",
        streak_history.empty() ? std::nullopt : std::optional<double>{streak_history.back()},
        source_ts,
        ready_ts,
        static_cast<std::int64_t>(view.size()),
        2
    );
    for (const int seconds : {3, 5, 10}) {
        const std::size_t count = std::min<std::size_t>(view.size(), seconds + 1);
        const std::size_t start = view.size() - count;
        double momentum = 0.0;
        for (std::size_t index = start + 1; index < view.size(); ++index) {
            const double change = view[index]->close - view[index - 1]->close;
            momentum += change > 0.0 ? 1.0 : change < 0.0 ? -1.0 : 0.0;
        }
        put(
            result,
            "tick_mom_" + std::to_string(seconds) + "s",
            count == static_cast<std::size_t>(seconds + 1)
                ? std::optional<double>{momentum}
                : std::nullopt,
            source_ts,
            ready_ts,
            static_cast<std::int64_t>(count),
            seconds + 1
        );
    }
    for (const int span : {3, 10}) {
        put(
            result,
            "tick_ewm_" + std::to_string(span) + "s",
            ewm_last(diffs, span),
            source_ts,
            ready_ts,
            static_cast<std::int64_t>(view.size()),
            2
        );
    }
    const std::size_t micro_start = tail_start(diffs.size(), 10);
    const std::vector<double> micro(diffs.begin() + static_cast<std::ptrdiff_t>(micro_start),
        diffs.end());
    put(result, "micro_ret_std", sample_std(micro), source_ts, ready_ts,
        static_cast<std::int64_t>(micro.size()), 3);
    put(result, "micro_ret_skew", skew(micro), source_ts, ready_ts,
        static_cast<std::int64_t>(micro.size()), 5);
    put(result, "micro_ret_kurt", kurtosis(micro), source_ts, ready_ts,
        static_cast<std::int64_t>(micro.size()), 5);

    const std::size_t recent_start = tail_start(signs.size(), 10);
    std::vector<double> reversals;
    for (std::size_t index = recent_start + 1; index < signs.size(); ++index) {
        reversals.push_back(signs[index] != signs[index - 1] ? 1.0 : 0.0);
    }
    put(result, "tick_reversal_freq", mean(reversals), source_ts, ready_ts,
        static_cast<std::int64_t>(signs.size() - recent_start), 3);

    std::vector<double> signed_volume;
    signed_volume.reserve(view.size());
    for (const auto* bar : view) {
        signed_volume.push_back(bar->buy_volume - bar->sell_volume);
    }
    put(result, "flow_velocity", signed_volume.back(), source_ts, ready_ts, 1);
    put(
        result,
        "flow_acceleration",
        signed_volume.size() >= 2
            ? std::optional<double>{
                  signed_volume.back() - signed_volume[signed_volume.size() - 2]}
            : std::nullopt,
        source_ts,
        ready_ts,
        static_cast<std::int64_t>(signed_volume.size()),
        2
    );
    std::optional<double> max_streak;
    const std::size_t streak_start = tail_start(streak_history.size(), 10);
    for (std::size_t index = streak_start; index < streak_history.size(); ++index) {
        max_streak = std::max(max_streak.value_or(0.0), std::abs(streak_history[index]));
    }
    put(result, "tick_streak_max", max_streak, source_ts, ready_ts,
        static_cast<std::int64_t>(streak_history.size() - streak_start));

    std::vector<double> momentum5;
    for (std::size_t end = tail_start(signs.size(), 10); end < signs.size(); ++end) {
        const std::size_t start = end >= 4 ? end - 4 : 0;
        double value = 0.0;
        for (std::size_t index = start; index <= end; ++index) {
            value += signs[index];
        }
        momentum5.push_back(value);
    }
    std::optional<double> momentum_range;
    if (!momentum5.empty()) {
        const auto [minimum, maximum] = std::minmax_element(momentum5.begin(), momentum5.end());
        momentum_range = *maximum - *minimum;
    }
    put(result, "tick_mom_range", momentum_range, source_ts, ready_ts,
        static_cast<std::int64_t>(momentum5.size()));

    for (const int window : {5, 10, 30, 60}) {
        const std::size_t start = tail_start(view.size(), window);
        double buy_quote = 0.0;
        double sell_quote = 0.0;
        double trade_count = 0.0;
        double max_run = 0.0;
        double max_buy_sweep = 0.0;
        double max_sell_sweep = 0.0;
        double buy_iceberg = 0.0;
        double sell_iceberg = 0.0;
        for (std::size_t index = start; index < view.size(); ++index) {
            const auto& bar = *view[index];
            buy_quote += bar.buy_quote_qty;
            sell_quote += bar.sell_quote_qty;
            trade_count += static_cast<double>(bar.trade_count);
            max_run = std::max(max_run, static_cast<double>(bar.max_same_side_run));
            const double buy_sweep = sweep_bps(bar, true);
            const double sell_sweep = sweep_bps(bar, false);
            max_buy_sweep = std::max(max_buy_sweep, buy_sweep);
            max_sell_sweep = std::max(max_sell_sweep, sell_sweep);
            buy_iceberg += static_cast<double>(bar.buy_count) / (1.0 + buy_sweep);
            sell_iceberg += static_cast<double>(bar.sell_count) / (1.0 + sell_sweep);
        }
        const std::int64_t count = static_cast<std::int64_t>(view.size() - start);
        const std::string suffix = "_" + std::to_string(window) + "s";
        put(result, "taker_quote_imbalance" + suffix,
            ratio(buy_quote - sell_quote, buy_quote + sell_quote),
            source_ts, ready_ts, count, window);
        put(result, "taker_signed_quote_sum" + suffix, buy_quote - sell_quote,
            source_ts, ready_ts, count, window);
        put(result, "taker_trade_count_sum" + suffix, trade_count,
            source_ts, ready_ts, count, window);
        put(result, "taker_max_same_side_run" + suffix, max_run,
            source_ts, ready_ts, count, window);
        put(result, "taker_buy_sweep_score" + suffix,
            max_buy_sweep * std::log1p(std::max(0.0, buy_quote)),
            source_ts, ready_ts, count, window);
        put(result, "taker_sell_sweep_score" + suffix,
            max_sell_sweep * std::log1p(std::max(0.0, sell_quote)),
            source_ts, ready_ts, count, window);
        put(result, "taker_buy_iceberg_pressure_sum" + suffix, buy_iceberg,
            source_ts, ready_ts, count, window);
        put(result, "taker_sell_iceberg_pressure_sum" + suffix, sell_iceberg,
            source_ts, ready_ts, count, window);
    }
    return result;
}

FeatureMap compute_local_microstructure(const BarView& view) {
    FeatureMap result;
    const OneSecondBar& current = *view.back();
    const std::int64_t source_ts = current.start_ts_ms;
    const std::int64_t ready_ts = current.finalized_ts_ms;
    put(result, "volume_imbalance",
        ratio(current.buy_volume - current.sell_volume,
            current.buy_volume + current.sell_volume),
        source_ts, ready_ts, 1);

    for (const int seconds : {5, 30, 60, 300}) {
        const std::size_t bar_start = tail_start(view.size(), seconds);
        const std::size_t return_start = tail_start(view.size(), seconds + 1);
        double buy = 0.0;
        double sell = 0.0;
        double absolute = 0.0;
        double intensity = 0.0;
        for (std::size_t index = bar_start; index < view.size(); ++index) {
            const auto& bar = *view[index];
            buy += bar.buy_volume;
            sell += bar.sell_volume;
            absolute += std::abs(bar.buy_volume - bar.sell_volume);
            intensity += static_cast<double>(bar.trade_count);
        }
        const std::int64_t bar_count = static_cast<std::int64_t>(view.size() - bar_start);
        std::vector<double> returns = log_returns(view, return_start);
        std::optional<double> volatility;
        if (returns.size() >= 2) {
            volatility = sample_std(returns).value_or(0.0) *
                std::sqrt(static_cast<double>(seconds));
        }
        const std::int64_t return_bar_count =
            static_cast<std::int64_t>(view.size() - return_start);
        put(result, "volatility_" + std::to_string(seconds) + "s", volatility,
            source_ts, ready_ts, return_bar_count, 3);
        put(result, "volume_imbalance_" + std::to_string(seconds) + "s",
            ratio(buy - sell, buy + sell), source_ts, ready_ts, bar_count);
        put(result, "trade_intensity_" + std::to_string(seconds) + "s",
            bar_count > 0 ? std::optional<double>{intensity / static_cast<double>(bar_count)}
                          : std::nullopt,
            source_ts, ready_ts, bar_count);
        put(result, "vpin_" + std::to_string(seconds) + "s",
            ratio(absolute, buy + sell), source_ts, ready_ts, bar_count);
        put(result, "price_change_" + std::to_string(seconds) + "s",
            return_bar_count == seconds + 1
                ? std::optional<double>{
                      view.back()->close / view[return_start]->close - 1.0}
                : std::nullopt,
            source_ts, ready_ts, return_bar_count, seconds + 1);
    }

    const std::vector<double> diffs = close_diffs(view);
    put(result, "price_velocity",
        diffs.empty() ? std::nullopt : std::optional<double>{diffs.back()},
        source_ts, ready_ts, static_cast<std::int64_t>(view.size()), 2);
    put(result, "price_acceleration",
        diffs.size() >= 2
            ? std::optional<double>{diffs.back() - diffs[diffs.size() - 2]}
            : std::nullopt,
        source_ts, ready_ts, static_cast<std::int64_t>(view.size()), 3);

    const auto current_average = ratio(current.volume, static_cast<double>(current.trade_count));
    const std::size_t size_start = tail_start(view.size(), 60);
    std::vector<double> sizes;
    for (std::size_t index = size_start; index < view.size(); ++index) {
        const auto value = ratio(
            view[index]->volume, static_cast<double>(view[index]->trade_count));
        if (value.has_value()) {
            sizes.push_back(*value);
        }
    }
    const auto average_size = mean(sizes);
    const std::int64_t size_count = static_cast<std::int64_t>(view.size() - size_start);
    put(result, "avg_trade_size", current_average, source_ts, ready_ts, 1);
    put(result, "avg_trade_size_60s", average_size, source_ts, ready_ts, size_count);
    put(result, "large_trade_ratio",
        ratio(current_average.value_or(0.0), average_size.value_or(0.0)),
        source_ts, ready_ts, size_count);

    const std::size_t volume_start = tail_start(view.size(), 300);
    std::vector<double> volumes;
    for (std::size_t index = volume_start; index < view.size(); ++index) {
        volumes.push_back(view[index]->volume);
    }
    const auto volume_mean = mean(volumes);
    const auto volume_std = sample_std(volumes);
    std::optional<double> volume_zscore;
    if (volume_mean.has_value() && volume_std.has_value() && *volume_std != 0.0) {
        volume_zscore = (current.volume - *volume_mean) / *volume_std;
    }
    put(result, "volume_zscore", volume_zscore, source_ts, ready_ts,
        static_cast<std::int64_t>(volumes.size()), 2);
    const double spread = current.high - current.low;
    put(result, "bar_spread", spread, source_ts, ready_ts, 1);
    put(result, "bar_spread_bps", spread / current.close * 10'000.0,
        source_ts, ready_ts, 1);

    std::optional<double> one_return;
    if (view.size() >= 2) {
        one_return = std::log(current.close / view[view.size() - 2]->close);
    }
    put(result, "return_1", one_return, source_ts, ready_ts,
        static_cast<std::int64_t>(view.size()), 2);
    put(result, "return_abs",
        one_return.has_value() ? std::optional<double>{std::abs(*one_return)} : std::nullopt,
        source_ts, ready_ts, static_cast<std::int64_t>(view.size()), 2);

    std::vector<double> absolute_returns = log_returns(view);
    for (double& value : absolute_returns) {
        value = std::abs(value);
    }
    const std::size_t vol6_start = tail_start(absolute_returns.size(), 21'600);
    const std::size_t vol24_start = tail_start(absolute_returns.size(), 86'400);
    const std::vector<double> vol6_values(
        absolute_returns.begin() + static_cast<std::ptrdiff_t>(vol6_start),
        absolute_returns.end());
    const std::vector<double> vol24_values(
        absolute_returns.begin() + static_cast<std::ptrdiff_t>(vol24_start),
        absolute_returns.end());
    const auto vol6 = mean(vol6_values);
    const auto vol24 = mean(vol24_values);
    put(result, "vol_regime_6h", vol6, source_ts, ready_ts,
        static_cast<std::int64_t>(vol6_values.size()), 3'600);
    put(result, "vol_regime_24h", vol24, source_ts, ready_ts,
        static_cast<std::int64_t>(vol24_values.size()), 21'600);

    std::vector<double> supported_blocks;
    const std::size_t first_end = std::max<std::size_t>(
        21'600,
        absolute_returns.size() > 604'800 ? absolute_returns.size() - 604'800 : 0
    );
    for (std::size_t end = first_end; end <= absolute_returns.size(); end += 3'600) {
        const std::size_t start = end > 21'600 ? end - 21'600 : 0;
        std::vector<double> block(
            absolute_returns.begin() + static_cast<std::ptrdiff_t>(start),
            absolute_returns.begin() + static_cast<std::ptrdiff_t>(end));
        const auto value = mean(block);
        if (value.has_value()) {
            supported_blocks.push_back(*value);
        }
        if (absolute_returns.size() - end < 3'600) {
            break;
        }
    }
    const auto block_mean = mean(supported_blocks);
    const auto block_std = sample_std(supported_blocks);
    std::optional<double> regime_zscore;
    if (vol6.has_value() && block_mean.has_value() && block_std.has_value() &&
        *block_std != 0.0) {
        regime_zscore = (*vol6 - *block_mean) / *block_std;
    }
    put(result, "vol_regime_zscore", regime_zscore, source_ts, ready_ts,
        static_cast<std::int64_t>(supported_blocks.size()), 24);
    return result;
}

}  // namespace
}  // namespace narrowgate_cpp::f03

namespace narrowgate_cpp::f03 {
namespace {

struct CalendarParts {
    int year = 0;
    unsigned month = 0;
    unsigned day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    int weekday = 0;
};

CalendarParts calendar_parts(const std::chrono::sys_seconds instant) {
    using namespace std::chrono;
    const sys_days date = floor<days>(instant);
    const year_month_day ymd{date};
    const hh_mm_ss time{instant - date};
    const unsigned sunday_based = weekday{date}.c_encoding();
    return CalendarParts{
        static_cast<int>(ymd.year()),
        static_cast<unsigned>(ymd.month()),
        static_cast<unsigned>(ymd.day()),
        static_cast<int>(time.hours().count()),
        static_cast<int>(time.minutes().count()),
        static_cast<int>(time.seconds().count()),
        static_cast<int>((sunday_based + 6U) % 7U),
    };
}

int date_int(const std::chrono::sys_days date) {
    const std::chrono::year_month_day ymd{date};
    return static_cast<int>(ymd.year()) * 10'000 +
        static_cast<int>(static_cast<unsigned>(ymd.month())) * 100 +
        static_cast<int>(static_cast<unsigned>(ymd.day()));
}

int date_int(const CalendarParts& value) {
    return value.year * 10'000 + static_cast<int>(value.month) * 100 +
        static_cast<int>(value.day);
}

std::chrono::sys_days day_from_parts(const CalendarParts& value) {
    using namespace std::chrono;
    return sys_days{year{value.year} / month{value.month} / day{value.day}};
}

std::chrono::sys_seconds nth_sunday_utc(
    const int year_value,
    const unsigned month_value,
    const int nth,
    const int utc_hour
) {
    using namespace std::chrono;
    const sys_days first{year{year_value} / month{month_value} / day{1}};
    const unsigned first_weekday = weekday{first}.c_encoding();
    const int to_sunday = static_cast<int>((7U - first_weekday) % 7U);
    return sys_seconds{first + days{to_sunday + (nth - 1) * 7}} + hours{utc_hour};
}

int new_york_utc_offset_hours(const std::chrono::sys_seconds instant) {
    const int utc_year = calendar_parts(instant).year;
    const auto dst_start = nth_sunday_utc(utc_year, 3, 2, 7);
    const auto dst_end = nth_sunday_utc(utc_year, 11, 1, 6);
    return instant >= dst_start && instant < dst_end ? -4 : -5;
}

bool contains_date(const std::unordered_set<int>& dates, const int date) {
    return dates.find(date) != dates.end();
}

const std::unordered_set<int>& nyse_holidays() {
    static const std::unordered_set<int> values = {
        20250101, 20250109, 20250120, 20250217, 20250418, 20250526,
        20250619, 20250704, 20250901, 20251127, 20251225,
        20260101, 20260119, 20260216, 20260403, 20260525, 20260619,
        20260703, 20260907, 20261126, 20261225,
    };
    return values;
}

const std::unordered_set<int>& federal_holidays() {
    static const std::unordered_set<int> values = {
        20250101, 20250109, 20250120, 20250217, 20250526, 20250619,
        20250704, 20250901, 20251013, 20251111, 20251127, 20251225,
        20260101, 20260119, 20260216, 20260525, 20260619, 20260703,
        20260907, 20261012, 20261111, 20261126, 20261225,
    };
    return values;
}

const std::unordered_set<int>& cn_holidays() {
    static const std::unordered_set<int> values = {
        20250101, 20250128, 20250129, 20250130, 20250131, 20250201,
        20250202, 20250203, 20250204, 20250404, 20250405, 20250406,
        20250501, 20250502, 20250503, 20250504, 20250505, 20250531,
        20250601, 20250602, 20251001, 20251002, 20251003, 20251004,
        20251005, 20251006, 20251007, 20251008,
        20260101, 20260102, 20260103, 20260215, 20260216, 20260217,
        20260218, 20260219, 20260220, 20260221, 20260222, 20260223,
        20260404, 20260405, 20260406, 20260501, 20260502, 20260503,
        20260504, 20260505, 20260619, 20260620, 20260621, 20260925,
        20260926, 20260927, 20261001, 20261002, 20261003, 20261004,
        20261005, 20261006, 20261007,
    };
    return values;
}

const std::unordered_set<int>& cn_adjusted_workdays() {
    static const std::unordered_set<int> values = {
        20250126, 20250208, 20250427, 20250928, 20251011,
        20260104, 20260214, 20260228, 20260426, 20260509, 20260920,
        20261010,
    };
    return values;
}

FeatureMap compute_calendar(const std::int64_t cutoff) {
    using namespace std::chrono;
    const sys_seconds utc_instant{seconds{cutoff / 1'000}};
    const CalendarParts utc = calendar_parts(utc_instant);
    const CalendarParts cn = calendar_parts(utc_instant + hours{8});
    const CalendarParts us = calendar_parts(
        utc_instant + hours{new_york_utc_offset_hours(utc_instant)});
    if ((cn.year != 2025 && cn.year != 2026) ||
        (us.year != 2025 && us.year != 2026)) {
        throw std::invalid_argument(
            "calendar year outside supported local-year range; supported=[2025,2026]");
    }

    const int cn_date = date_int(cn);
    const int us_date = date_int(us);
    const int cn_previous = date_int(day_from_parts(cn) - days{1});
    const int cn_next = date_int(day_from_parts(cn) + days{1});
    const int us_previous = date_int(day_from_parts(us) - days{1});
    const int us_next = date_int(day_from_parts(us) + days{1});
    const bool cn_holiday = contains_date(cn_holidays(), cn_date);
    const bool cn_adjusted = contains_date(cn_adjusted_workdays(), cn_date);
    const bool cn_workday = (cn.weekday < 5 && !cn_holiday) || cn_adjusted;
    const double us_minutes = static_cast<double>(us.hour * 60 + us.minute) +
        static_cast<double>(us.second) / 60.0;
    const bool us_trading = us.weekday < 5 && !contains_date(nyse_holidays(), us_date);
    const bool us_rth = us_trading && us_minutes >= 570.0 && us_minutes < 960.0;
    const bool us_pre = us_trading && us_minutes >= 240.0 && us_minutes < 570.0;
    const bool us_after = us_trading && us_minutes >= 960.0 && us_minutes < 1'200.0;
    const bool tokyo = utc.hour >= 0 && utc.hour < 6;
    const bool singapore_hk = utc.hour >= 1 && utc.hour < 9;
    const bool london = utc.hour >= 8 && utc.hour < 16;
    const bool america = utc.hour >= 13 && utc.hour < 21;
    const bool asia = tokyo || singapore_hk;
    const bool asia_europe_overlap = singapore_hk && london;
    const bool europe_america_overlap = london && america;
    const bool tokyo_singapore_overlap = tokyo && singapore_hk;
    const int active_count = static_cast<int>(tokyo) + static_cast<int>(singapore_hk) +
        static_cast<int>(london) + static_cast<int>(america);
    const double pi = std::acos(-1.0);
    const double hour_fraction = static_cast<double>(utc.hour) +
        static_cast<double>(utc.minute) / 60.0 + static_cast<double>(utc.second) / 3'600.0;

    std::unordered_map<std::string, double> values = {
        {"cal_utc_hour", static_cast<double>(utc.hour)},
        {"cal_utc_weekday", static_cast<double>(utc.weekday)},
        {"cal_utc_is_weekend", static_cast<double>(utc.weekday >= 5)},
        {"cal_hour_sin", std::sin(2.0 * pi * hour_fraction / 24.0)},
        {"cal_hour_cos", std::cos(2.0 * pi * hour_fraction / 24.0)},
        {"cal_dow_sin", std::sin(2.0 * pi * static_cast<double>(utc.weekday) / 7.0)},
        {"cal_dow_cos", std::cos(2.0 * pi * static_cast<double>(utc.weekday) / 7.0)},
        {"cal_session_asia", static_cast<double>(asia)},
        {"cal_session_tokyo", static_cast<double>(tokyo)},
        {"cal_session_singapore_hk", static_cast<double>(singapore_hk)},
        {"cal_session_europe", static_cast<double>(london)},
        {"cal_session_london", static_cast<double>(london)},
        {"cal_session_america", static_cast<double>(america)},
        {"cal_session_us_extended", static_cast<double>(us_pre || us_after)},
        {"cal_session_asia_europe_overlap", static_cast<double>(asia_europe_overlap)},
        {"cal_session_europe_america_overlap", static_cast<double>(europe_america_overlap)},
        {"cal_session_tokyo_singapore_overlap", static_cast<double>(tokyo_singapore_overlap)},
        {"cal_session_london_us_overlap", static_cast<double>(europe_america_overlap)},
        {"cal_session_active_count", static_cast<double>(active_count)},
        {"cal_cn_hour", static_cast<double>(cn.hour)},
        {"cal_cn_weekday", static_cast<double>(cn.weekday)},
        {"cal_cn_is_weekend", static_cast<double>(cn.weekday >= 5)},
        {"cal_cn_is_holiday", static_cast<double>(cn_holiday)},
        {"cal_cn_is_adjusted_workday", static_cast<double>(cn_adjusted)},
        {"cal_cn_is_workday", static_cast<double>(cn_workday)},
        {"cal_cn_is_holiday_eve", static_cast<double>(contains_date(cn_holidays(), cn_next))},
        {"cal_cn_is_post_holiday", static_cast<double>(contains_date(cn_holidays(), cn_previous))},
        {"cal_us_hour", static_cast<double>(us.hour)},
        {"cal_us_weekday", static_cast<double>(us.weekday)},
        {"cal_us_is_weekend", static_cast<double>(us.weekday >= 5)},
        {"cal_us_is_sunday", static_cast<double>(us.weekday == 6)},
        {"cal_us_is_sunday_evening", static_cast<double>(us.weekday == 6 && us_minutes >= 1'080.0)},
        {"cal_us_is_federal_holiday", static_cast<double>(contains_date(federal_holidays(), us_date))},
        {"cal_us_is_nyse_trading_day", static_cast<double>(us_trading)},
        {"cal_us_is_regular_hours", static_cast<double>(us_rth)},
        {"cal_us_is_premarket", static_cast<double>(us_pre)},
        {"cal_us_is_afterhours", static_cast<double>(us_after)},
        {"cal_us_is_holiday_eve", static_cast<double>(contains_date(nyse_holidays(), us_next))},
        {"cal_us_is_post_holiday", static_cast<double>(contains_date(nyse_holidays(), us_previous))},
        {"cal_minutes_to_us_open", us_trading && us_minutes < 570.0
            ? (570.0 - us_minutes) / 570.0 : 0.0},
        {"cal_minutes_to_us_close", us_rth ? (960.0 - us_minutes) / 390.0 : 0.0},
        {"cal_is_weekday_us_rth", static_cast<double>(us_rth)},
        {"cal_is_weekend_core", static_cast<double>(utc.weekday >= 5)},
    };
    const int minutes_in_day = utc.hour * 60 + utc.minute;
    double funding = 480.0;
    for (const int edge : {480, 960, 1'440}) {
        if (edge > minutes_in_day) {
            funding = static_cast<double>(edge - minutes_in_day);
            break;
        }
    }
    const double minute_of_hour = static_cast<double>(utc.minute) +
        static_cast<double>(utc.second) / 60.0;
    const double distance = std::min({
        minute_of_hour,
        60.0 - minute_of_hour,
        std::abs(minute_of_hour - 30.0),
    });
    values["minutes_to_funding"] = funding;
    values["funding_phase"] = funding / 480.0;
    values["funding_sin"] = std::sin(2.0 * pi * (1.0 - funding / 480.0));
    values["funding_cos"] = std::cos(2.0 * pi * (1.0 - funding / 480.0));
    values["dist_to_hour"] = distance;
    values["near_candle_close"] = distance < 2.0 ? 1.0 : 0.0;

    FeatureMap result;
    for (const auto& [name, value] : values) {
        put(result, name, value, cutoff, cutoff, 1);
    }
    return result;
}

void merge_features(FeatureMap& destination, FeatureMap source) {
    for (auto& [name, value] : source) {
        if (!destination.emplace(name, std::move(value)).second) {
            throw std::logic_error("duplicate F03 feature output: " + name);
        }
    }
}

}  // namespace

FeatureRow compute_causal_v12_one_second_features(
    const std::vector<OneSecondBar>& local_bars,
    const std::int64_t cutoff_exclusive_ms,
    const std::int64_t decision_ts_ms,
    const std::vector<ExecutionL2Observation>& execution_l2,
    const std::vector<MetricObservation>& metrics,
    const std::vector<OneSecondBar>& reference_bars
) {
    if (cutoff_exclusive_ms <= 0 || cutoff_exclusive_ms % kCadenceMs != 0) {
        throw std::invalid_argument(
            "cutoff_exclusive_ms must be a positive canonical 1s edge");
    }
    if (decision_ts_ms < cutoff_exclusive_ms) {
        throw std::invalid_argument("decision precedes the feature cutoff");
    }
    const BarView local_view = cutoff_view(
        local_bars, cutoff_exclusive_ms, "binance_futures_btcusdc_completed_1s_bar");
    FeatureMap values = compute_trade_features(local_view);
    merge_features(values, compute_execution_l2(execution_l2, cutoff_exclusive_ms));
    merge_features(values, compute_metrics(metrics, local_view, cutoff_exclusive_ms));
    merge_features(values, compute_local_microstructure(local_view));
    merge_features(values, compute_cross_market(
        reference_bars, local_view, cutoff_exclusive_ms));
    merge_features(values, compute_calendar(cutoff_exclusive_ms));
    if (values.size() != kCausalV12OneSecondFeatureCount) {
        throw std::logic_error("F03 generated schema does not contain 173 features");
    }

    FeatureRow row;
    row.cutoff_exclusive_ms = cutoff_exclusive_ms;
    row.decision_ts_ms = decision_ts_ms;
    std::int64_t ready = 0;
    for (std::size_t index = 0; index < kCausalV12OneSecondFeatureNames.size(); ++index) {
        const std::string name{kCausalV12OneSecondFeatureNames[index]};
        const auto found = values.find(name);
        if (found == values.end()) {
            throw std::logic_error("missing F03 feature output: " + name);
        }
        row.values[index] = found->second;
        if (found->second.feature_ready_ts_ms.has_value()) {
            ready = std::max(ready, *found->second.feature_ready_ts_ms);
        }
    }
    if (ready > cutoff_exclusive_ms || ready > decision_ts_ms) {
        throw std::logic_error("F03 feature row violates feature-ready causality");
    }
    row.feature_ready_ts_ms = ready;
    return row;
}

}  // namespace narrowgate_cpp::f03
