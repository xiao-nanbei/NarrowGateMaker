#pragma once

#include "common.hpp"

#include <array>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace narrowgate_cpp {

struct Bar1s {
    std::int64_t ts_ms = 0;
    double open = 0.0;
    double high = 0.0;
    double low = 0.0;
    double close = 0.0;
    double volume = 0.0;
    double buy_volume = 0.0;
    double sell_volume = 0.0;
    double trade_count = 0.0;
    double buy_count = 0.0;
    double sell_count = 0.0;
    double quote_qty = 0.0;
    double buy_quote_qty = 0.0;
    double sell_quote_qty = 0.0;
    double max_same_side_run = 0.0;
    double max_buy_run = 0.0;
    double max_sell_run = 0.0;
    double buy_price_high = 0.0;
    double buy_price_low = 0.0;
    double sell_price_high = 0.0;
    double sell_price_low = 0.0;
};

struct FeatureHistoryRow {
    double close = 0.0;
    double volume = 0.0;
    double buy_volume = 0.0;
    double sell_volume = 0.0;
    double trade_count = 0.0;
    double flow_velocity = 0.0;
    double avg_trade_size = 0.0;
    double price_velocity = 0.0;
    double return_abs = 0.0;
    double vol_regime_6h = 0.0;
};

inline constexpr std::size_t kSignalExecutionL2MaxDepth = 10;

struct SignalExecutionL2Snapshot {
    double ts_ms = 0.0;
    std::size_t depth = 0;
    std::array<double, kSignalExecutionL2MaxDepth> bid_price{};
    std::array<double, kSignalExecutionL2MaxDepth> bid_quantity{};
    std::array<double, kSignalExecutionL2MaxDepth> ask_price{};
    std::array<double, kSignalExecutionL2MaxDepth> ask_quantity{};
};

inline constexpr std::array<std::string_view, 13> kSignalExecutionL2FeatureNames = {
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

using SignalExecutionL2FeatureValues =
    std::array<double, kSignalExecutionL2FeatureNames.size()>;

inline constexpr std::array<std::string_view, 4>
    kSignalExecutionL2PolicyMetricNames = {
        "l2_quote_flip_rate",
        "l2_book_refresh_ratio",
        "l2_book_cancel_ratio",
        "l2_near_depth_total",
    };

using SignalExecutionL2PolicyMetricValues =
    std::array<double, kSignalExecutionL2PolicyMetricNames.size()>;

inline constexpr std::array<std::string_view, 11> kSignalRefPerpFeatureNames = {
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

using SignalRefPerpFeatureValues =
    std::array<double, kSignalRefPerpFeatureNames.size()>;

// Canonical live model row.  This order is the frozen 173-column schema used
// by the source-aware causal-v12 bundle.  Keeping the order in the native ABI
// lets the hot path hand one aligned row directly to LightGBM; Python mappings
// remain a diagnostic view rather than the model input representation.
inline constexpr std::array<std::string_view, 173> kSignalModelFeatureNames = {
    "close", "volume", "buy_volume", "sell_volume", "trade_count",
    "buy_count", "sell_count", "tick_streak", "tick_mom_3s",
    "tick_mom_5s", "tick_mom_10s", "tick_ewm_3s", "tick_ewm_10s",
    "micro_ret_std", "micro_ret_skew", "micro_ret_kurt",
    "tick_reversal_freq", "flow_velocity", "flow_acceleration",
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
    "taker_buy_iceberg_pressure_sum_10s",
    "taker_buy_iceberg_pressure_sum_30s",
    "taker_buy_iceberg_pressure_sum_60s",
    "taker_sell_iceberg_pressure_sum_5s",
    "taker_sell_iceberg_pressure_sum_10s",
    "taker_sell_iceberg_pressure_sum_30s",
    "taker_sell_iceberg_pressure_sum_60s", "l2_spread_bps",
    "l2_microprice_offset_bps", "l2_imbalance_l1", "l2_imbalance_l3",
    "l2_imbalance_l5", "l2_imbalance_l10", "l2_near_depth_total",
    "l2_depth_slope", "l2_depth_convexity", "l2_queue_concentration",
    "l2_quote_flip_rate", "l2_book_refresh_ratio", "l2_book_cancel_ratio",
    "oi_log", "oi_pct_change", "oi_zscore_1h", "oi_zscore_6h",
    "oi_momentum", "toptrader_ls_ratio", "crowd_ls_ratio",
    "taker_ls_ratio", "toptrader_ls_zscore", "crowd_ls_zscore",
    "taker_ls_zscore", "taker_ls_momentum", "oi_price_divergence",
    "volatility_30s", "volatility_60s", "volatility_300s",
    "volume_imbalance", "volume_imbalance_30s", "volume_imbalance_60s",
    "volume_imbalance_300s", "trade_intensity_30s",
    "trade_intensity_60s", "trade_intensity_300s", "vpin_30s",
    "vpin_60s", "vpin_300s", "price_velocity", "price_acceleration",
    "price_change_30s", "price_change_60s", "price_change_300s",
    "volatility_5s", "volume_imbalance_5s", "trade_intensity_5s",
    "vpin_5s", "price_change_5s", "avg_trade_size",
    "avg_trade_size_60s", "large_trade_ratio", "volume_zscore",
    "bar_spread", "bar_spread_bps", "return_1", "return_abs",
    "vol_regime_6h", "vol_regime_24h", "vol_regime_zscore",
    "cv_ref_perp_basis_bps", "cv_ref_perp_ret_10s",
    "cv_ref_perp_ret_30s", "cv_ref_perp_ret_60s",
    "cv_ref_perp_volatility_60s", "cv_ref_perp_volume_imbalance",
    "cv_ref_perp_trade_intensity_60s", "cv_ref_perp_vpin_60s",
    "cv_ref_perp_basis_residual_bps", "cv_ref_perp_age_s",
    "cv_ref_perp_available", "cal_utc_hour", "cal_utc_weekday",
    "cal_utc_is_weekend", "cal_hour_sin", "cal_hour_cos", "cal_dow_sin",
    "cal_dow_cos", "cal_session_asia", "cal_session_tokyo",
    "cal_session_singapore_hk", "cal_session_europe",
    "cal_session_london", "cal_session_america",
    "cal_session_us_extended", "cal_session_asia_europe_overlap",
    "cal_session_europe_america_overlap",
    "cal_session_tokyo_singapore_overlap", "cal_session_london_us_overlap",
    "cal_session_active_count", "cal_cn_hour", "cal_cn_weekday",
    "cal_cn_is_weekend", "cal_cn_is_holiday",
    "cal_cn_is_adjusted_workday", "cal_cn_is_workday",
    "cal_cn_is_holiday_eve", "cal_cn_is_post_holiday", "cal_us_hour",
    "cal_us_weekday", "cal_us_is_weekend", "cal_us_is_sunday",
    "cal_us_is_sunday_evening", "cal_us_is_federal_holiday",
    "cal_us_is_nyse_trading_day", "cal_us_is_regular_hours",
    "cal_us_is_premarket", "cal_us_is_afterhours", "cal_us_is_holiday_eve",
    "cal_us_is_post_holiday", "cal_minutes_to_us_open",
    "cal_minutes_to_us_close", "cal_is_weekday_us_rth",
    "cal_is_weekend_core", "minutes_to_funding", "funding_phase",
    "funding_sin", "funding_cos", "dist_to_hour", "near_candle_close",
};

inline constexpr std::array<std::string_view, 13> kSignalMetricFeatureNames = {
    "oi_log", "oi_pct_change", "oi_zscore_1h", "oi_zscore_6h",
    "oi_momentum", "toptrader_ls_ratio", "crowd_ls_ratio",
    "taker_ls_ratio", "toptrader_ls_zscore", "crowd_ls_zscore",
    "taker_ls_zscore", "taker_ls_momentum", "oi_price_divergence",
};

inline constexpr std::array<std::string_view, 49> kSignalTimeFeatureNames = {
    "cal_utc_hour", "cal_utc_weekday", "cal_utc_is_weekend",
    "cal_hour_sin", "cal_hour_cos", "cal_dow_sin", "cal_dow_cos",
    "cal_session_asia", "cal_session_tokyo", "cal_session_singapore_hk",
    "cal_session_europe", "cal_session_london", "cal_session_america",
    "cal_session_us_extended", "cal_session_asia_europe_overlap",
    "cal_session_europe_america_overlap",
    "cal_session_tokyo_singapore_overlap", "cal_session_london_us_overlap",
    "cal_session_active_count", "cal_cn_hour", "cal_cn_weekday",
    "cal_cn_is_weekend", "cal_cn_is_holiday",
    "cal_cn_is_adjusted_workday", "cal_cn_is_workday",
    "cal_cn_is_holiday_eve", "cal_cn_is_post_holiday", "cal_us_hour",
    "cal_us_weekday", "cal_us_is_weekend", "cal_us_is_sunday",
    "cal_us_is_sunday_evening", "cal_us_is_federal_holiday",
    "cal_us_is_nyse_trading_day", "cal_us_is_regular_hours",
    "cal_us_is_premarket", "cal_us_is_afterhours", "cal_us_is_holiday_eve",
    "cal_us_is_post_holiday", "cal_minutes_to_us_open",
    "cal_minutes_to_us_close", "cal_is_weekday_us_rth",
    "cal_is_weekend_core", "minutes_to_funding", "funding_phase",
    "funding_sin", "funding_cos", "dist_to_hour", "near_candle_close",
};

using SignalMetricFeatureValues =
    std::array<double, kSignalMetricFeatureNames.size()>;
using SignalTimeFeatureValues =
    std::array<double, kSignalTimeFeatureNames.size()>;

struct SignalRefPerpPrepared {
    SignalRefPerpFeatureValues values{};
    std::int64_t target_bucket = 0;
    double basis_bps = 0.0;
    std::uint64_t revision = 0;
    std::uintptr_t owner_token = 0;
    bool basis_available = false;
};

[[nodiscard]] SignalExecutionL2FeatureValues compute_signal_execution_l2_features(
    std::span<const SignalExecutionL2Snapshot> snapshots,
    double bucket_end_ms
);

[[nodiscard]] SignalExecutionL2PolicyMetricValues
compute_signal_execution_l2_policy_metrics(
    std::span<const SignalExecutionL2Snapshot> snapshots,
    double end_exchange_ms
);

template <typename T>
class SegmentedSpanView {
public:
    SegmentedSpanView() = default;
    explicit SegmentedSpanView(
        std::span<const T> first,
        std::span<const T> second = {}) noexcept
        : first_(first), second_(second) {}

    [[nodiscard]] bool empty() const noexcept { return size() == 0; }
    [[nodiscard]] std::size_t size() const noexcept {
        return first_.size() + second_.size();
    }
    [[nodiscard]] const T& operator[](std::size_t index) const noexcept {
        return index < first_.size()
            ? first_[index]
            : second_[index - first_.size()];
    }
    [[nodiscard]] const T& back() const noexcept { return (*this)[size() - 1]; }

    [[nodiscard]] SegmentedSpanView<T> subview(
        std::size_t offset,
        std::size_t count
    ) const {
        if (offset > size() || count > size() - offset) {
            throw std::out_of_range("SegmentedSpanView subview out of range");
        }
        if (count == 0) {
            return {};
        }
        if (offset < first_.size()) {
            const std::size_t first_count = std::min(count, first_.size() - offset);
            return SegmentedSpanView<T>{
                first_.subspan(offset, first_count),
                second_.first(count - first_count),
            };
        }
        return SegmentedSpanView<T>{
            second_.subspan(offset - first_.size(), count)
        };
    }

private:
    std::span<const T> first_;
    std::span<const T> second_;
};

template <typename T>
class CircularBuffer {
public:
    explicit CircularBuffer(std::size_t capacity)
        : capacity_(std::max<std::size_t>(1, capacity)) {
        storage_.reserve(capacity_);
    }

    void clear() noexcept {
        storage_.clear();
        head_ = 0;
    }

    void push_back(const T& value) {
        if (storage_.size() < capacity_) {
            storage_.push_back(value);
            return;
        }
        storage_[head_] = value;
        head_ = (head_ + 1) % capacity_;
    }

    [[nodiscard]] bool empty() const noexcept { return storage_.empty(); }
    [[nodiscard]] std::size_t size() const noexcept { return storage_.size(); }
    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

    [[nodiscard]] const T& operator[](std::size_t index) const {
        if (index >= storage_.size()) {
            throw std::out_of_range("CircularBuffer index out of range");
        }
        return storage_[(head_ + index) % storage_.size()];
    }

    [[nodiscard]] const T& back() const {
        return (*this)[storage_.size() - 1];
    }

    T& mutable_back() {
        if (storage_.empty()) {
            throw std::out_of_range("CircularBuffer is empty");
        }
        return storage_[(head_ + storage_.size() - 1) % storage_.size()];
    }

    [[nodiscard]] SegmentedSpanView<T> view() const noexcept {
        // Expose at most two contiguous spans in chronological order instead
        // of copying the wrapped ring into a temporary array.
        const std::span<const T> storage{storage_.data(), storage_.size()};
        if (storage_.empty() || head_ == 0) {
            return SegmentedSpanView<T>{storage};
        }
        return SegmentedSpanView<T>{storage.subspan(head_), storage.first(head_)};
    }

private:
    std::size_t capacity_ = 1;
    std::size_t head_ = 0;
    std::vector<T> storage_;
};

struct SignalRefPerpBookTicker {
    std::int64_t bucket_ts_ms = 0;
    double bid = 0.0;
    double ask = 0.0;
    double event_ts_ms = 0.0;
    double receive_ts_ms = 0.0;
};

struct SignalRefPerpBasisObservation {
    std::int64_t bucket = 0;
    double basis_bps = 0.0;
};

class SignalExecutionL2Engine {
public:
    explicit SignalExecutionL2Engine(std::size_t max_snapshots = 300)
        : snapshots_(max_snapshots) {}

    void reset();
    void push_snapshot(const SignalExecutionL2Snapshot& snapshot);
    [[nodiscard]] SignalExecutionL2FeatureValues compute_features(
        double bucket_end_ms
    ) const;
    [[nodiscard]] SignalExecutionL2PolicyMetricValues compute_policy_metrics(
        double end_exchange_ms
    ) const;
    [[nodiscard]] std::size_t snapshot_count() const;

private:
    mutable std::mutex mutex_;
    CircularBuffer<SignalExecutionL2Snapshot> snapshots_;
};

class CountRollingMoments {
public:
    explicit CountRollingMoments(std::size_t capacity)
        : values_(capacity) {}

    void clear() noexcept {
        values_.clear();
        mean_ = 0.0;
        m2_ = 0.0;
    }

    void push(double value) {
        if (values_.size() == values_.capacity()) {
            remove_value(values_[0], values_.size(), mean_, m2_);
        }
        values_.push_back(value);
        add_value(value, values_.size(), mean_, m2_);
    }

    [[nodiscard]] std::size_t count() const noexcept { return values_.size(); }
    [[nodiscard]] double mean() const noexcept { return values_.empty() ? 0.0 : mean_; }
    [[nodiscard]] double stddev() const noexcept {
        if (values_.empty()) {
            return 0.0;
        }
        return std::sqrt(std::max(0.0, m2_ / static_cast<double>(values_.size())));
    }
    [[nodiscard]] double mean_with(double value) const noexcept {
        double next_mean = mean_;
        double next_m2 = m2_;
        std::size_t count = values_.size();
        if (count == values_.capacity()) {
            remove_value(values_[0], count, next_mean, next_m2);
            --count;
        }
        add_value(value, count + 1, next_mean, next_m2);
        return next_mean;
    }
    [[nodiscard]] double stddev_with(double value) const noexcept {
        double next_mean = mean_;
        double next_m2 = m2_;
        std::size_t count = values_.size();
        if (count == values_.capacity()) {
            remove_value(values_[0], count, next_mean, next_m2);
            --count;
        }
        ++count;
        add_value(value, count, next_mean, next_m2);
        return std::sqrt(std::max(0.0, next_m2 / static_cast<double>(count)));
    }

private:
    static void add_value(
        double value, std::size_t next_count, double& mean, double& m2) noexcept {
        const double delta = value - mean;
        mean += delta / static_cast<double>(next_count);
        m2 += delta * (value - mean);
    }

    static void remove_value(
        double value, std::size_t count, double& mean, double& m2) noexcept {
        if (count <= 1) {
            mean = 0.0;
            m2 = 0.0;
            return;
        }
        const double next_mean = (static_cast<double>(count) * mean - value) /
            static_cast<double>(count - 1);
        m2 = std::max(0.0, m2 - (value - mean) * (value - next_mean));
        mean = next_mean;
    }

    CircularBuffer<double> values_;
    double mean_ = 0.0;
    double m2_ = 0.0;
};

enum class SignalFeatureId : std::uint8_t {
    AvgTradeSize,
    AvgTradeSize60s,
    BarSpread,
    BarSpreadBps,
    FlowAcceleration,
    FlowVelocity,
    LargeTradeRatio,
    MicroRetKurt,
    MicroRetSkew,
    MicroRetStd,
    PriceAcceleration,
    PriceChange300s,
    PriceChange30s,
    PriceChange5s,
    PriceChange60s,
    PriceVelocity,
    Return1,
    ReturnAbs,
    TakerBuyIceberg10s,
    TakerBuyIceberg30s,
    TakerBuyIceberg5s,
    TakerBuyIceberg60s,
    TakerBuySweep10s,
    TakerBuySweep30s,
    TakerBuySweep5s,
    TakerBuySweep60s,
    TakerMaxRun10s,
    TakerMaxRun30s,
    TakerMaxRun5s,
    TakerMaxRun60s,
    TakerQuoteImbalance10s,
    TakerQuoteImbalance30s,
    TakerQuoteImbalance5s,
    TakerQuoteImbalance60s,
    TakerSellIceberg10s,
    TakerSellIceberg30s,
    TakerSellIceberg5s,
    TakerSellIceberg60s,
    TakerSellSweep10s,
    TakerSellSweep30s,
    TakerSellSweep5s,
    TakerSellSweep60s,
    TakerSignedQuote10s,
    TakerSignedQuote30s,
    TakerSignedQuote5s,
    TakerSignedQuote60s,
    TakerTradeCount10s,
    TakerTradeCount30s,
    TakerTradeCount5s,
    TakerTradeCount60s,
    TickEwm10s,
    TickEwm3s,
    TickMom10s,
    TickMom3s,
    TickMom5s,
    TickMomRange,
    TickReversalFreq,
    TickStreak,
    TickStreakMax,
    TradeIntensity300s,
    TradeIntensity30s,
    TradeIntensity5s,
    TradeIntensity60s,
    VolRegime24h,
    VolRegime6h,
    VolRegimeZscore,
    Volatility300s,
    Volatility30s,
    Volatility5s,
    Volatility60s,
    VolumeImbalance,
    VolumeImbalance300s,
    VolumeImbalance30s,
    VolumeImbalance5s,
    VolumeImbalance60s,
    VolumeZscore,
    Vpin300s,
    Vpin30s,
    Vpin5s,
    Vpin60s,
    Count,
};

inline constexpr std::size_t kSignalFeatureCount =
    static_cast<std::size_t>(SignalFeatureId::Count);

inline constexpr std::array<std::string_view, kSignalFeatureCount> kSignalFeatureNames = {
    "avg_trade_size", "avg_trade_size_60s", "bar_spread", "bar_spread_bps",
    "flow_acceleration", "flow_velocity", "large_trade_ratio", "micro_ret_kurt",
    "micro_ret_skew", "micro_ret_std", "price_acceleration", "price_change_300s",
    "price_change_30s", "price_change_5s", "price_change_60s", "price_velocity",
    "return_1", "return_abs", "taker_buy_iceberg_pressure_sum_10s",
    "taker_buy_iceberg_pressure_sum_30s", "taker_buy_iceberg_pressure_sum_5s",
    "taker_buy_iceberg_pressure_sum_60s", "taker_buy_sweep_score_10s",
    "taker_buy_sweep_score_30s", "taker_buy_sweep_score_5s", "taker_buy_sweep_score_60s",
    "taker_max_same_side_run_10s", "taker_max_same_side_run_30s",
    "taker_max_same_side_run_5s", "taker_max_same_side_run_60s",
    "taker_quote_imbalance_10s", "taker_quote_imbalance_30s",
    "taker_quote_imbalance_5s", "taker_quote_imbalance_60s",
    "taker_sell_iceberg_pressure_sum_10s", "taker_sell_iceberg_pressure_sum_30s",
    "taker_sell_iceberg_pressure_sum_5s", "taker_sell_iceberg_pressure_sum_60s",
    "taker_sell_sweep_score_10s", "taker_sell_sweep_score_30s",
    "taker_sell_sweep_score_5s", "taker_sell_sweep_score_60s",
    "taker_signed_quote_sum_10s", "taker_signed_quote_sum_30s",
    "taker_signed_quote_sum_5s", "taker_signed_quote_sum_60s",
    "taker_trade_count_sum_10s", "taker_trade_count_sum_30s",
    "taker_trade_count_sum_5s", "taker_trade_count_sum_60s", "tick_ewm_10s",
    "tick_ewm_3s", "tick_mom_10s", "tick_mom_3s", "tick_mom_5s",
    "tick_mom_range", "tick_reversal_freq", "tick_streak", "tick_streak_max",
    "trade_intensity_300s", "trade_intensity_30s", "trade_intensity_5s",
    "trade_intensity_60s", "vol_regime_24h", "vol_regime_6h", "vol_regime_zscore",
    "volatility_300s", "volatility_30s", "volatility_5s", "volatility_60s",
    "volume_imbalance", "volume_imbalance_300s", "volume_imbalance_30s",
    "volume_imbalance_5s", "volume_imbalance_60s", "volume_zscore", "vpin_300s",
    "vpin_30s", "vpin_5s", "vpin_60s",
};

class SignalFeatureVector {
public:
    // live/benchmark 热路径应优先使用 fixed-order array；名字只在 Python 边界或调试时解析。
    [[nodiscard]] double& operator[](SignalFeatureId id) noexcept {
        return values_[static_cast<std::size_t>(id)];
    }

    [[nodiscard]] const double& operator[](SignalFeatureId id) const noexcept {
        return values_[static_cast<std::size_t>(id)];
    }

    [[nodiscard]] const std::array<double, kSignalFeatureCount>& values() const noexcept {
        return values_;
    }

    [[nodiscard]] std::map<std::string, double> to_map() const;

private:
    std::array<double, kSignalFeatureCount> values_{};
};

struct SignalFeatureBucketPrepared {
    Bar1s aggregate{};
    SignalFeatureVector core{};
};

class alignas(64) SignalModelFeatureRow {
public:
    [[nodiscard]] const std::array<double, kSignalModelFeatureNames.size()>&
    values() const noexcept {
        return values_;
    }
    [[nodiscard]] double value_at(std::size_t index) const;
    [[nodiscard]] std::array<double, 88> legacy_base_values() const noexcept;

private:
    friend SignalModelFeatureRow assemble_signal_model_feature_row(
        const SignalFeatureBucketPrepared&,
        const SignalExecutionL2FeatureValues&,
        const SignalMetricFeatureValues&,
        const SignalRefPerpFeatureValues&,
        const SignalTimeFeatureValues&
    );
    std::array<double, kSignalModelFeatureNames.size()> values_{};
};

[[nodiscard]] SignalModelFeatureRow assemble_signal_model_feature_row(
    const SignalFeatureBucketPrepared& bucket,
    const SignalExecutionL2FeatureValues& execution_l2,
    const SignalMetricFeatureValues& metrics,
    const SignalRefPerpFeatureValues& ref_perp,
    const SignalTimeFeatureValues& time
);

class TradeBarAggregator {
public:
    explicit TradeBarAggregator(bool track_runs = true);

    void reset();
    [[nodiscard]] std::vector<Bar1s> update(
        std::int64_t ts_ms, double price, double qty, bool is_buyer_maker);
    [[nodiscard]] std::vector<Bar1s> update_batch(
        ArrayView<std::int64_t> ts_ms,
        ArrayView<double> prices,
        ArrayView<double> quantities,
        ArrayView<std::uint8_t> is_buyer_maker
    );
    [[nodiscard]] std::optional<Bar1s> current_bar() const;
    std::int64_t current_bucket_ms() const { return current_bucket_ms_; }

private:
    void apply_trade(Bar1s& bar, double price, double qty, bool is_buyer_maker, int run_len);
    void update_one(
        std::int64_t ts_ms,
        double price,
        double qty,
        bool is_buyer_maker,
        std::vector<Bar1s>& completed
    );
    static Bar1s flat_bar(std::int64_t ts_ms, double close);

    bool track_runs_ = true;
    bool has_current_ = false;
    Bar1s current_;
    std::int64_t current_bucket_ms_ = 0;
    int last_trade_side_ = 0;
    int last_trade_run_len_ = 0;
};

class SignalRefPerpFeatureEngine {
public:
    SignalRefPerpFeatureEngine(
        std::size_t max_bars = 3700,
        std::size_t max_book_tickers = 3600,
        std::size_t max_basis = 360,
        std::size_t basis_min_periods = 30,
        double source_max_age_ms = 30000.0
    );

    void reset();
    void update_trade_batch(
        ArrayView<std::int64_t> ts_ms,
        ArrayView<double> prices,
        ArrayView<double> quantities,
        ArrayView<std::uint8_t> is_buyer_maker
    );
    void update_book_ticker(
        double event_ts_ms,
        double receive_ts_ms,
        double bid,
        double ask
    );
    [[nodiscard]] SignalRefPerpPrepared prepare(
        std::int64_t target_ts_ms,
        double target_close
    ) const;
    void commit(const SignalRefPerpPrepared& prepared);
    [[nodiscard]] std::size_t bar_count() const;
    [[nodiscard]] std::size_t book_ticker_count() const;
    [[nodiscard]] std::size_t basis_count() const;

private:
    [[nodiscard]] std::optional<SignalRefPerpBookTicker> book_ticker_at(
        double target_ts_ms
    ) const;

    mutable std::mutex mutex_;
    TradeBarAggregator trade_aggregator_{false};
    CircularBuffer<Bar1s> bars_;
    CircularBuffer<SignalRefPerpBookTicker> book_tickers_;
    CircularBuffer<SignalRefPerpBasisObservation> basis_history_;
    mutable std::vector<double> basis_scratch_;
    std::size_t basis_min_periods_ = 30;
    double source_max_age_ms_ = 30000.0;
    std::uint64_t revision_ = 0;
};

class SignalFeatureEngine {
public:
    SignalFeatureEngine(std::size_t max_bars = 320, std::size_t max_history = 60480);

    void reset();
    void push_bar(const Bar1s& bar);
    void push_history(const FeatureHistoryRow& row);
    [[nodiscard]] SignalFeatureVector compute(const Bar1s& bar_10s) const;
    [[nodiscard]] SignalFeatureVector compute_at_cutoff(
        const Bar1s& bar_10s,
        std::int64_t cutoff_exclusive_ms
    ) const;
    [[nodiscard]] std::pair<Bar1s, SignalFeatureVector> compute_bucket(
        std::int64_t bucket_start_ms
    ) const;
    [[nodiscard]] SignalFeatureBucketPrepared prepare_bucket(
        std::int64_t bucket_start_ms
    ) const;
    std::size_t bar_count() const { return bars_.size(); }
    std::size_t history_count() const { return history_.size(); }

private:
    std::size_t max_bars_ = 320;
    std::size_t max_history_ = 60480;
    CircularBuffer<Bar1s> bars_;
    CircularBuffer<FeatureHistoryRow> history_;
    CountRollingMoments return_abs_2160_;
    CountRollingMoments return_abs_8640_;
    CountRollingMoments vol_regime_6h_60480_;
};

[[nodiscard]] std::map<std::string, double> compute_signal_feature_overlay(
    const std::vector<Bar1s>& all_bars,
    const std::vector<FeatureHistoryRow>& feature_history,
    const Bar1s& bar_10s
);

}  // namespace narrowgate_cpp
