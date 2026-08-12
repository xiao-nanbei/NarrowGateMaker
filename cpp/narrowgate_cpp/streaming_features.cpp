#include "streaming_features.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cmath>
#include <memory_resource>
#include <numeric>
#include <stdexcept>

namespace narrowgate_cpp {
namespace {

inline constexpr auto kWindow10s = std::to_array<int>({3, 6, 30});
inline constexpr auto kTakerWindows = std::to_array<int>({5, 10, 30, 60});

inline constexpr auto kVolatilityFeatures = std::to_array<SignalFeatureId>({
    SignalFeatureId::Volatility30s,
    SignalFeatureId::Volatility60s,
    SignalFeatureId::Volatility300s,
});
inline constexpr auto kVolumeImbalanceFeatures = std::to_array<SignalFeatureId>({
    SignalFeatureId::VolumeImbalance30s,
    SignalFeatureId::VolumeImbalance60s,
    SignalFeatureId::VolumeImbalance300s,
});
inline constexpr auto kTradeIntensityFeatures = std::to_array<SignalFeatureId>({
    SignalFeatureId::TradeIntensity30s,
    SignalFeatureId::TradeIntensity60s,
    SignalFeatureId::TradeIntensity300s,
});
inline constexpr auto kVpinFeatures = std::to_array<SignalFeatureId>({
    SignalFeatureId::Vpin30s,
    SignalFeatureId::Vpin60s,
    SignalFeatureId::Vpin300s,
});
inline constexpr auto kPriceChangeFeatures = std::to_array<SignalFeatureId>({
    SignalFeatureId::PriceChange30s,
    SignalFeatureId::PriceChange60s,
    SignalFeatureId::PriceChange300s,
});
inline constexpr auto kTakerQuoteImbalanceFeatures = std::to_array<SignalFeatureId>({
    SignalFeatureId::TakerQuoteImbalance5s,
    SignalFeatureId::TakerQuoteImbalance10s,
    SignalFeatureId::TakerQuoteImbalance30s,
    SignalFeatureId::TakerQuoteImbalance60s,
});
inline constexpr auto kTakerSignedQuoteFeatures = std::to_array<SignalFeatureId>({
    SignalFeatureId::TakerSignedQuote5s,
    SignalFeatureId::TakerSignedQuote10s,
    SignalFeatureId::TakerSignedQuote30s,
    SignalFeatureId::TakerSignedQuote60s,
});
inline constexpr auto kTakerTradeCountFeatures = std::to_array<SignalFeatureId>({
    SignalFeatureId::TakerTradeCount5s,
    SignalFeatureId::TakerTradeCount10s,
    SignalFeatureId::TakerTradeCount30s,
    SignalFeatureId::TakerTradeCount60s,
});
inline constexpr auto kTakerMaxRunFeatures = std::to_array<SignalFeatureId>({
    SignalFeatureId::TakerMaxRun5s,
    SignalFeatureId::TakerMaxRun10s,
    SignalFeatureId::TakerMaxRun30s,
    SignalFeatureId::TakerMaxRun60s,
});
inline constexpr auto kTakerBuySweepFeatures = std::to_array<SignalFeatureId>({
    SignalFeatureId::TakerBuySweep5s,
    SignalFeatureId::TakerBuySweep10s,
    SignalFeatureId::TakerBuySweep30s,
    SignalFeatureId::TakerBuySweep60s,
});
inline constexpr auto kTakerSellSweepFeatures = std::to_array<SignalFeatureId>({
    SignalFeatureId::TakerSellSweep5s,
    SignalFeatureId::TakerSellSweep10s,
    SignalFeatureId::TakerSellSweep30s,
    SignalFeatureId::TakerSellSweep60s,
});
inline constexpr auto kTakerBuyIcebergFeatures = std::to_array<SignalFeatureId>({
    SignalFeatureId::TakerBuyIceberg5s,
    SignalFeatureId::TakerBuyIceberg10s,
    SignalFeatureId::TakerBuyIceberg30s,
    SignalFeatureId::TakerBuyIceberg60s,
});
inline constexpr auto kTakerSellIcebergFeatures = std::to_array<SignalFeatureId>({
    SignalFeatureId::TakerSellIceberg5s,
    SignalFeatureId::TakerSellIceberg10s,
    SignalFeatureId::TakerSellIceberg30s,
    SignalFeatureId::TakerSellIceberg60s,
});

double safe_log_return(double newer, double older) {
    return (newer > 0.0 && older > 0.0) ? std::log(newer / older) : 0.0;
}

double ewm_close_diff(std::span<const double> closes, int span) {
    if (closes.size() < 2) {
        return 0.0;
    }
    const double alpha = 2.0 / (static_cast<double>(span) + 1.0);
    double total = 0.0;
    double weight_sum = 0.0;
    double weight = 1.0;
    for (std::size_t i = closes.size() - 1; i > 0; --i) {
        total += weight * (closes[i] - closes[i - 1]);
        weight_sum += weight;
        weight *= (1.0 - alpha);
    }
    return weight_sum > 0.0 ? total / weight_sum : 0.0;
}

template <typename Getter>
double indexed_mean(std::size_t count, Getter&& get) {
    if (count == 0) {
        return 0.0;
    }
    double total = 0.0;
    for (std::size_t i = 0; i < count; ++i) {
        total += get(i);
    }
    return total / static_cast<double>(count);
}

template <typename Getter>
double indexed_stddev(std::size_t count, Getter&& get, bool sample) {
    if (count < (sample ? 2U : 1U)) {
        return 0.0;
    }
    const double mean = indexed_mean(count, get);
    double ss = 0.0;
    for (std::size_t i = 0; i < count; ++i) {
        const double delta = get(i) - mean;
        ss += delta * delta;
    }
    const double denominator = static_cast<double>(sample ? count - 1 : count);
    return std::sqrt(ss / denominator);
}

double side_sweep_bps(const Bar1s& bar, bool buy_side) {
    const double high = buy_side ? bar.buy_price_high : bar.sell_price_high;
    const double low = buy_side ? bar.buy_price_low : bar.sell_price_low;
    const double quote_qty = buy_side ? bar.buy_quote_qty : bar.sell_quote_qty;
    if (high <= 0.0 || low <= 0.0 || quote_qty <= 0.0) {
        return 0.0;
    }
    const double mid = 0.5 * (high + low);
    return mid > 0.0 ? (high - low) / mid * 10000.0 : 0.0;
}

}  // namespace

TradeBarAggregator::TradeBarAggregator(bool track_runs)
    : track_runs_(track_runs) {}

void TradeBarAggregator::reset() {
    has_current_ = false;
    current_ = Bar1s{};
    current_bucket_ms_ = 0;
    last_trade_side_ = 0;
    last_trade_run_len_ = 0;
}

Bar1s TradeBarAggregator::flat_bar(std::int64_t ts_ms, double close) {
    Bar1s bar;
    bar.ts_ms = ts_ms;
    bar.open = close;
    bar.high = close;
    bar.low = close;
    bar.close = close;
    return bar;
}

void TradeBarAggregator::apply_trade(
    Bar1s& bar,
    double price,
    double qty,
    bool is_buyer_maker,
    int run_len
) {
    bar.close = price;
    bar.high = std::max(bar.high, price);
    bar.low = std::min(bar.low, price);
    bar.volume += qty;
    bar.trade_count += 1.0;
    const double quote_qty = price * qty;
    bar.quote_qty += quote_qty;
    bar.max_same_side_run = std::max(bar.max_same_side_run, static_cast<double>(run_len));

    if (is_buyer_maker) {
        bar.sell_volume += qty;
        bar.sell_count += 1.0;
        bar.sell_quote_qty += quote_qty;
        bar.max_sell_run = std::max(bar.max_sell_run, static_cast<double>(run_len));
        bar.sell_price_high = std::max(bar.sell_price_high, price);
        bar.sell_price_low = bar.sell_price_low <= 0.0 ? price : std::min(bar.sell_price_low, price);
    } else {
        bar.buy_volume += qty;
        bar.buy_count += 1.0;
        bar.buy_quote_qty += quote_qty;
        bar.max_buy_run = std::max(bar.max_buy_run, static_cast<double>(run_len));
        bar.buy_price_high = std::max(bar.buy_price_high, price);
        bar.buy_price_low = bar.buy_price_low <= 0.0 ? price : std::min(bar.buy_price_low, price);
    }
}

std::vector<Bar1s> TradeBarAggregator::update(
    std::int64_t ts_ms,
    double price,
    double qty,
    bool is_buyer_maker
) {
    std::vector<Bar1s> completed;
    update_one(ts_ms, price, qty, is_buyer_maker, completed);
    return completed;
}

std::vector<Bar1s> TradeBarAggregator::update_batch(
    ArrayView<std::int64_t> ts_ms,
    ArrayView<double> prices,
    ArrayView<double> quantities,
    ArrayView<std::uint8_t> is_buyer_maker
) {
    const std::size_t count = ts_ms.size();
    if (
        prices.size() != count || quantities.size() != count ||
        is_buyer_maker.size() != count
    ) {
        throw std::invalid_argument("trade-bar batch arrays must have equal length");
    }
    std::vector<Bar1s> completed;
    completed.reserve(std::min<std::size_t>(count, 16));
    for (std::size_t index = 0; index < count; ++index) {
        update_one(
            ts_ms[index],
            prices[index],
            quantities[index],
            is_buyer_maker[index] != 0,
            completed
        );
    }
    return completed;
}

void TradeBarAggregator::update_one(
    std::int64_t ts_ms,
    double price,
    double qty,
    bool is_buyer_maker,
    std::vector<Bar1s>& completed
) {
    if (ts_ms <= 0 || price <= 0.0 || qty <= 0.0) {
        return;
    }

    const std::int64_t bucket = (ts_ms / 1000) * 1000;
    const int side = is_buyer_maker ? -1 : 1;
    int run_len = 1;
    if (track_runs_) {
        if (side == last_trade_side_) {
            ++last_trade_run_len_;
        } else {
            last_trade_side_ = side;
            last_trade_run_len_ = 1;
        }
        run_len = last_trade_run_len_;
    }

    if (!has_current_ || bucket != current_bucket_ms_) {
        if (has_current_) {
            completed.push_back(current_);
            std::int64_t gap_bucket = current_bucket_ms_ + 1000;
            while (gap_bucket < bucket) {
                completed.push_back(flat_bar(gap_bucket, current_.close));
                gap_bucket += 1000;
            }
        }
        current_ = Bar1s{};
        current_.ts_ms = bucket;
        current_.open = price;
        current_.high = price;
        current_.low = price;
        current_.close = price;
        current_bucket_ms_ = bucket;
        has_current_ = true;
    }

    apply_trade(current_, price, qty, is_buyer_maker, run_len);
}

std::optional<Bar1s> TradeBarAggregator::current_bar() const {
    if (!has_current_) {
        return std::nullopt;
    }
    return current_;
}

std::map<std::string, double> SignalFeatureVector::to_map() const {
    // 冷路径：给 Python dict/日志/测试用。live scalar hot path 不应每 tick 调这个函数。
    std::map<std::string, double> out;
    for (std::size_t i = 0; i < values_.size(); ++i) {
        out.emplace(kSignalFeatureNames[i], values_[i]);
    }
    return out;
}

SignalFeatureEngine::SignalFeatureEngine(std::size_t max_bars, std::size_t max_history)
    : max_bars_(std::max<std::size_t>(1, max_bars)),
      max_history_(std::max<std::size_t>(1, max_history)),
      bars_(max_bars_),
      history_(max_history_),
      return_abs_2160_(std::min<std::size_t>(max_history_, 2160)),
      return_abs_8640_(std::min<std::size_t>(max_history_, 8640)),
      vol_regime_6h_60480_(std::min<std::size_t>(max_history_, 60480)) {}

void SignalFeatureEngine::reset() {
    bars_.clear();
    history_.clear();
    return_abs_2160_.clear();
    return_abs_8640_.clear();
    vol_regime_6h_60480_.clear();
}

void SignalFeatureEngine::push_bar(const Bar1s& bar) {
    bars_.push_back(bar);
}

void SignalFeatureEngine::push_history(const FeatureHistoryRow& row) {
    history_.push_back(row);
    return_abs_2160_.push(row.return_abs);
    return_abs_8640_.push(row.return_abs);
    vol_regime_6h_60480_.push(row.vol_regime_6h);
}

SignalFeatureVector compute_signal_feature_vector(
    SegmentedSpanView<Bar1s> all_bars,
    SegmentedSpanView<FeatureHistoryRow> feature_history,
    const Bar1s& bar_10s,
    const CountRollingMoments* return_abs_2160 = nullptr,
    const CountRollingMoments* return_abs_8640 = nullptr,
    const CountRollingMoments* vol_regime_6h_60480 = nullptr
) {
    SignalFeatureVector f;
    const double close = bar_10s.close;

    std::array<std::byte, 16 * 1024> scratch{};
    std::pmr::monotonic_buffer_resource scratch_resource(scratch.data(), scratch.size());
    // 兼容旧的“给一段 bars 现场算特征”接口，仍会构造 closes/signs scratch。
    // 真正低延时方向应继续把更多窗口改为 SignalFeatureEngine 增量统计。
    std::pmr::vector<double> closes{&scratch_resource};
    std::pmr::vector<double> signs{&scratch_resource};
    closes.reserve(all_bars.size());
    signs.reserve(all_bars.size());
    for (std::size_t i = 0; i < all_bars.size(); ++i) {
        const double current_close = all_bars[i].close;
        closes.push_back(current_close);
        if (i == 0) {
            signs.push_back(0.0);
        } else {
            const double diff = current_close - all_bars[i - 1].close;
            signs.push_back(diff > 0.0 ? 1.0 : (diff < 0.0 ? -1.0 : 0.0));
        }
    }
    const std::span<const double> close_view{closes.data(), closes.size()};
    const std::span<const double> sign_view{signs.data(), signs.size()};
    const std::size_t n_signs = sign_view.size();

    double streak = 0.0;
    if (n_signs >= 2) {
        streak = sign_view.back();
        for (std::size_t rev = n_signs - 1; rev > 0; --rev) {
            if (sign_view[rev] == sign_view[rev - 1] && sign_view[rev - 1] != 0.0) {
                streak += sign_view[rev - 1];
            } else {
                break;
            }
        }
    } else if (n_signs == 1) {
        streak = sign_view.back();
    }
    f[SignalFeatureId::TickStreak] = streak;

    const auto sum_last_signs = [&](std::size_t count) {
        const auto tail = sign_view.last(std::min(sign_view.size(), count));
        return std::accumulate(tail.begin(), tail.end(), 0.0);
    };
    f[SignalFeatureId::TickMom3s] = sum_last_signs(3);
    f[SignalFeatureId::TickMom5s] = sum_last_signs(5);
    f[SignalFeatureId::TickMom10s] = sum_last_signs(10);
    f[SignalFeatureId::TickEwm3s] = ewm_close_diff(close_view, 3);
    f[SignalFeatureId::TickEwm10s] = ewm_close_diff(close_view, 10);

    const std::size_t ret_count = close_view.size() > 1
        ? std::min<std::size_t>(10, close_view.size() - 1)
        : 0;
    const std::size_t ret_start = close_view.size() - ret_count;
    const auto micro_ret = [&](std::size_t i) {
        const std::size_t newer = ret_start + i;
        return close_view[newer] - close_view[newer - 1];
    };
    if (ret_count >= 3) {
        const double mean = indexed_mean(ret_count, micro_ret);
        const double sd = indexed_stddev(ret_count, micro_ret, false);
        f[SignalFeatureId::MicroRetStd] = sd;
        if (ret_count >= 5 && sd > 1e-12) {
            double skew = 0.0;
            double kurt = 0.0;
            for (std::size_t i = 0; i < ret_count; ++i) {
                const double z = (micro_ret(i) - mean) / sd;
                skew += z * z * z;
                kurt += z * z * z * z;
            }
            f[SignalFeatureId::MicroRetSkew] = skew / static_cast<double>(ret_count);
            f[SignalFeatureId::MicroRetKurt] = kurt / static_cast<double>(ret_count) - 3.0;
        }
    }

    if (n_signs >= 3) {
        const std::size_t limit = std::min<std::size_t>(10, n_signs);
        int changes = 0;
        for (std::size_t i = 1; i < limit; ++i) {
            if (sign_view[n_signs - i] != sign_view[n_signs - i - 1]) {
                ++changes;
            }
        }
        f[SignalFeatureId::TickReversalFreq] = static_cast<double>(changes) /
            static_cast<double>(std::min<std::size_t>(10, n_signs - 1));
    }

    const Bar1s* last_1s = all_bars.empty() ? nullptr : &all_bars[all_bars.size() - 1];
    f[SignalFeatureId::FlowVelocity] = last_1s
        ? last_1s->buy_volume - last_1s->sell_volume
        : 0.0;
    f[SignalFeatureId::FlowAcceleration] = feature_history.empty()
        ? 0.0
        : f[SignalFeatureId::FlowVelocity] - feature_history.back().flow_velocity;

    if (n_signs >= 10) {
        const std::size_t start = n_signs - 10;
        double run = sign_view[start];
        double max_abs_streak = std::abs(run);
        double min_mom5 = std::numeric_limits<double>::infinity();
        double max_mom5 = -std::numeric_limits<double>::infinity();
        for (std::size_t i = start; i < n_signs; ++i) {
            if (i > start) {
                run = sign_view[i] == sign_view[i - 1] && sign_view[i] != 0.0
                    ? run + sign_view[i]
                    : sign_view[i];
                max_abs_streak = std::max(max_abs_streak, std::abs(run));
            }
            const std::size_t mom_start = i >= 4 ? i - 4 : 0;
            double mom5 = 0.0;
            for (std::size_t j = mom_start; j <= i; ++j) {
                mom5 += sign_view[j];
            }
            min_mom5 = std::min(min_mom5, mom5);
            max_mom5 = std::max(max_mom5, mom5);
        }
        f[SignalFeatureId::TickStreakMax] = max_abs_streak;
        f[SignalFeatureId::TickMomRange] = max_mom5 - min_mom5;
    } else {
        f[SignalFeatureId::TickStreakMax] = std::abs(streak);
    }

    const std::size_t history_size = feature_history.size();
    const auto history_close = [&](std::size_t i) {
        return feature_history[i].close > 0.0 ? feature_history[i].close : close;
    };
    const double log_ret = history_size > 0
        ? safe_log_return(close, history_close(history_size - 1))
        : 0.0;
    const auto log_return_at = [&](std::size_t i) {
        const double older = history_close(i);
        const double newer = i + 1 < history_size ? history_close(i + 1) : close;
        return safe_log_return(newer, older);
    };

    // True trailing-five-second state from the causal 1s stream.  The former
    // implementation coerced a one-row 10s window to two rows (20 seconds).
    const std::size_t five_take = std::min<std::size_t>(5, all_bars.size());
    const std::size_t five_start = all_bars.size() - five_take;
    double buy_5s = 0.0;
    double sell_5s = 0.0;
    double trades_5s = 0.0;
    double abs_flow_5s = 0.0;
    for (std::size_t i = five_start; i < all_bars.size(); ++i) {
        buy_5s += all_bars[i].buy_volume;
        sell_5s += all_bars[i].sell_volume;
        trades_5s += all_bars[i].trade_count;
        abs_flow_5s += std::abs(all_bars[i].buy_volume - all_bars[i].sell_volume);
    }
    const double total_5s = buy_5s + sell_5s;
    f[SignalFeatureId::VolumeImbalance5s] = total_5s > 0.0
        ? (buy_5s - sell_5s) / total_5s : 0.0;
    f[SignalFeatureId::TradeIntensity5s] = five_take > 0
        ? trades_5s / static_cast<double>(five_take) : 0.0;
    f[SignalFeatureId::Vpin5s] = total_5s > 0.0 ? abs_flow_5s / total_5s : 0.0;

    const std::size_t five_return_count = all_bars.size() > 1
        ? std::min<std::size_t>(5, all_bars.size() - 1) : 0;
    const std::size_t five_return_start = all_bars.size() - five_return_count;
    const auto get_five_return = [&](std::size_t i) {
        const std::size_t newer = five_return_start + i;
        return safe_log_return(all_bars[newer].close, all_bars[newer - 1].close);
    };
    f[SignalFeatureId::Volatility5s] = five_return_count >= 2
        ? indexed_stddev(five_return_count, get_five_return, true) * std::sqrt(5.0)
        : 0.0;
    f[SignalFeatureId::PriceChange5s] = all_bars.size() >= 6 &&
        all_bars[all_bars.size() - 6].close > 0.0
        ? all_bars.back().close / all_bars[all_bars.size() - 6].close - 1.0
        : 0.0;

    for (std::size_t idx = 0; idx < kWindow10s.size(); ++idx) {
        const std::size_t window = static_cast<std::size_t>(kWindow10s[idx]);
        const std::size_t take = std::min(history_size, window);
        const std::size_t offset = history_size - take;
        const auto get_window_return = [&](std::size_t i) { return log_return_at(offset + i); };
        f[kVolatilityFeatures[idx]] = take >= 2
            ? indexed_stddev(take, get_window_return, true) * std::sqrt(static_cast<double>(take))
            : 0.0;
    }

    const double total_vol = bar_10s.buy_volume + bar_10s.sell_volume;
    f[SignalFeatureId::VolumeImbalance] = total_vol > 0.0
        ? (bar_10s.buy_volume - bar_10s.sell_volume) / total_vol
        : 0.0;

    for (std::size_t idx = 0; idx < kWindow10s.size(); ++idx) {
        const std::size_t window = static_cast<std::size_t>(kWindow10s[idx]);
        const std::size_t hist_take = std::min(history_size, window - 1);
        const std::size_t start = history_size - hist_take;
        double buy_sum = bar_10s.buy_volume;
        double sell_sum = bar_10s.sell_volume;
        double trade_count_sum = bar_10s.trade_count;
        double sum_abs = std::abs(bar_10s.buy_volume - bar_10s.sell_volume);
        double sum_total = total_vol;
        for (std::size_t j = start; j < history_size; ++j) {
            const auto& row = feature_history[j];
            buy_sum += row.buy_volume;
            sell_sum += row.sell_volume;
            trade_count_sum += row.trade_count;
            sum_abs += std::abs(row.buy_volume - row.sell_volume);
            sum_total += row.buy_volume + row.sell_volume;
        }
        const double total = buy_sum + sell_sum;
        f[kVolumeImbalanceFeatures[idx]] = total > 0.0 ? (buy_sum - sell_sum) / total : 0.0;
        f[kTradeIntensityFeatures[idx]] = trade_count_sum / static_cast<double>(hist_take + 1);
        f[kVpinFeatures[idx]] = sum_total > 0.0 ? sum_abs / sum_total : 0.0;
    }

    for (std::size_t idx = 0; idx < kTakerWindows.size(); ++idx) {
        const std::size_t take = std::min<std::size_t>(
            all_bars.size(), static_cast<std::size_t>(kTakerWindows[idx]));
        const std::size_t start = all_bars.size() - take;
        double buy_quote = 0.0;
        double sell_quote = 0.0;
        double trade_count = 0.0;
        double max_run = 0.0;
        double buy_sweep = 0.0;
        double sell_sweep = 0.0;
        double buy_iceberg = 0.0;
        double sell_iceberg = 0.0;
        for (std::size_t j = start; j < all_bars.size(); ++j) {
            const auto& bar = all_bars[j];
            buy_quote += bar.buy_quote_qty;
            sell_quote += bar.sell_quote_qty;
            trade_count += bar.trade_count;
            max_run = std::max(max_run, bar.max_same_side_run);
            const double buy_sweep_bps = side_sweep_bps(bar, true);
            const double sell_sweep_bps = side_sweep_bps(bar, false);
            buy_sweep = std::max(buy_sweep, buy_sweep_bps);
            sell_sweep = std::max(sell_sweep, sell_sweep_bps);
            buy_iceberg += bar.buy_count / (1.0 + buy_sweep_bps);
            sell_iceberg += bar.sell_count / (1.0 + sell_sweep_bps);
        }
        const double signed_quote = buy_quote - sell_quote;
        const double total_quote = buy_quote + sell_quote;
        f[kTakerQuoteImbalanceFeatures[idx]] = total_quote > 0.0
            ? signed_quote / total_quote
            : 0.0;
        f[kTakerSignedQuoteFeatures[idx]] = signed_quote;
        f[kTakerTradeCountFeatures[idx]] = trade_count;
        f[kTakerMaxRunFeatures[idx]] = max_run;
        f[kTakerBuySweepFeatures[idx]] = buy_sweep * std::log1p(std::max(buy_quote, 0.0));
        f[kTakerSellSweepFeatures[idx]] = sell_sweep * std::log1p(std::max(sell_quote, 0.0));
        f[kTakerBuyIcebergFeatures[idx]] = buy_iceberg;
        f[kTakerSellIcebergFeatures[idx]] = sell_iceberg;
    }

    f[SignalFeatureId::PriceVelocity] = history_size > 0
        ? close - history_close(history_size - 1)
        : 0.0;
    const double prev_velocity = history_size > 0
        ? feature_history.back().price_velocity
        : 0.0;
    f[SignalFeatureId::PriceAcceleration] = f[SignalFeatureId::PriceVelocity] - prev_velocity;

    for (std::size_t idx = 0; idx < kWindow10s.size(); ++idx) {
        const std::size_t window = static_cast<std::size_t>(std::max(kWindow10s[idx], 2));
        double value = 0.0;
        if (history_size >= window) {
            const double old = history_close(history_size - window);
            value = old > 0.0 ? (close - old) / old : 0.0;
        }
        f[kPriceChangeFeatures[idx]] = value;
    }

    const double avg_size = bar_10s.trade_count > 0.0
        ? bar_10s.volume / bar_10s.trade_count
        : 0.0;
    f[SignalFeatureId::AvgTradeSize] = avg_size;
    const std::size_t size_take = std::min<std::size_t>(history_size, 5);
    const std::size_t size_start = history_size - size_take;
    double size_sum = avg_size;
    for (std::size_t i = size_start; i < history_size; ++i) {
        size_sum += feature_history[i].avg_trade_size;
    }
    f[SignalFeatureId::AvgTradeSize60s] = size_sum / static_cast<double>(size_take + 1);
    f[SignalFeatureId::LargeTradeRatio] = f[SignalFeatureId::AvgTradeSize60s] > 0.0
        ? avg_size / f[SignalFeatureId::AvgTradeSize60s]
        : 1.0;

    const std::size_t volume_take = std::min<std::size_t>(history_size, 29);
    const std::size_t volume_count = volume_take + 1;
    const std::size_t volume_start = history_size - volume_take;
    const auto volume_at = [&](std::size_t i) {
        return i < volume_take ? feature_history[volume_start + i].volume : bar_10s.volume;
    };
    if (volume_count >= 3) {
        const double mean = indexed_mean(volume_count, volume_at);
        const double sd = indexed_stddev(volume_count, volume_at, false);
        f[SignalFeatureId::VolumeZscore] = sd > 0.0 ? (bar_10s.volume - mean) / sd : 0.0;
    }

    f[SignalFeatureId::BarSpread] = bar_10s.high - bar_10s.low;
    f[SignalFeatureId::BarSpreadBps] = close > 0.0
        ? f[SignalFeatureId::BarSpread] / close * 10000.0
        : 0.0;
    f[SignalFeatureId::Return1] = log_ret;
    f[SignalFeatureId::ReturnAbs] = std::abs(log_ret);

    const std::size_t abs_count = history_size + 1;
    const auto abs_return_at = [&](std::size_t i) {
        return i < history_size ? feature_history[i].return_abs : std::abs(log_ret);
    };
    if (abs_count >= 360) {
        if (return_abs_2160 != nullptr) {
            f[SignalFeatureId::VolRegime6h] = return_abs_2160->mean_with(std::abs(log_ret));
        } else {
            const std::size_t take = std::min<std::size_t>(abs_count, 2160);
            const std::size_t offset = abs_count - take;
            f[SignalFeatureId::VolRegime6h] = indexed_mean(
                take, [&](std::size_t i) { return abs_return_at(offset + i); });
        }
    } else {
        f[SignalFeatureId::VolRegime6h] = abs_count >= 3
            ? indexed_mean(abs_count, abs_return_at)
            : std::abs(log_ret);
    }
    if (abs_count >= 2160) {
        if (return_abs_8640 != nullptr) {
            f[SignalFeatureId::VolRegime24h] = return_abs_8640->mean_with(std::abs(log_ret));
        } else {
            const std::size_t take = std::min<std::size_t>(abs_count, 8640);
            const std::size_t offset = abs_count - take;
            f[SignalFeatureId::VolRegime24h] = indexed_mean(
                take, [&](std::size_t i) { return abs_return_at(offset + i); });
        }
    } else {
        f[SignalFeatureId::VolRegime24h] = f[SignalFeatureId::VolRegime6h];
    }

    if (history_size >= 8640) {
        double mean = 0.0;
        double sd = 0.0;
        if (vol_regime_6h_60480 != nullptr) {
            mean = vol_regime_6h_60480->mean();
            sd = vol_regime_6h_60480->stddev();
        } else {
            const std::size_t take = std::min<std::size_t>(history_size, 60480);
            const std::size_t offset = history_size - take;
            const auto vol6h_at = [&](std::size_t i) {
                return feature_history[offset + i].vol_regime_6h;
            };
            mean = indexed_mean(take, vol6h_at);
            sd = indexed_stddev(take, vol6h_at, false);
        }
        f[SignalFeatureId::VolRegimeZscore] = sd > 0.0
            ? (f[SignalFeatureId::VolRegime6h] - mean) / sd
            : 0.0;
    } else if (abs_count >= 3) {
        const double mean = indexed_mean(abs_count, abs_return_at);
        const double sd = indexed_stddev(abs_count, abs_return_at, false);
        f[SignalFeatureId::VolRegimeZscore] = sd > 0.0
            ? (std::abs(log_ret) - mean) / sd
            : 0.0;
    }

    return f;
}

SignalFeatureVector SignalFeatureEngine::compute(const Bar1s& bar_10s) const {
    return compute_signal_feature_vector(
        bars_.view(), history_.view(), bar_10s,
        &return_abs_2160_, &return_abs_8640_, &vol_regime_6h_60480_);
}

SignalFeatureVector SignalFeatureEngine::compute_at_cutoff(
    const Bar1s& bar_10s,
    std::int64_t cutoff_exclusive_ms
) const {
    if (cutoff_exclusive_ms <= 0) {
        throw std::invalid_argument("feature cutoff must be positive");
    }
    const auto all = bars_.view();
    std::size_t visible_count = 0;
    for (std::size_t i = 0; i < all.size(); ++i) {
        if (all[i].ts_ms < cutoff_exclusive_ms) {
            ++visible_count;
        }
    }

    // Python's causal close/sign state is capped at 320 finalized 1s bars.
    // Retain a larger persistent ring for catch-up, then expose the same tail.
    constexpr std::size_t kCausalBarLookback = 320;
    const std::size_t skip = visible_count > kCausalBarLookback
        ? visible_count - kCausalBarLookback
        : 0;
    std::vector<Bar1s> visible;
    visible.reserve(std::min(visible_count, kCausalBarLookback));
    std::size_t seen = 0;
    for (std::size_t i = 0; i < all.size(); ++i) {
        if (all[i].ts_ms >= cutoff_exclusive_ms) {
            continue;
        }
        if (seen++ < skip) {
            continue;
        }
        visible.push_back(all[i]);
    }
    return compute_signal_feature_vector(
        SegmentedSpanView<Bar1s>{
            std::span<const Bar1s>{visible.data(), visible.size()}},
        history_.view(),
        bar_10s,
        &return_abs_2160_, &return_abs_8640_, &vol_regime_6h_60480_);
}

std::map<std::string, double> compute_signal_feature_overlay(
    const std::vector<Bar1s>& all_bars,
    const std::vector<FeatureHistoryRow>& feature_history,
    const Bar1s& bar_10s
) {
    return compute_signal_feature_vector(
        SegmentedSpanView<Bar1s>{
            std::span<const Bar1s>{all_bars.data(), all_bars.size()}},
        SegmentedSpanView<FeatureHistoryRow>{
            std::span<const FeatureHistoryRow>{feature_history.data(), feature_history.size()}},
        bar_10s
    ).to_map();
}

}  // namespace narrowgate_cpp
