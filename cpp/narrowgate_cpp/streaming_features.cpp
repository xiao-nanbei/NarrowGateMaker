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

SignalRefPerpFeatureEngine::SignalRefPerpFeatureEngine(
    std::size_t max_bars,
    std::size_t max_book_tickers,
    std::size_t max_basis,
    std::size_t basis_min_periods,
    double source_max_age_ms
)
    : bars_(max_bars),
      book_tickers_(max_book_tickers),
      basis_history_(max_basis),
      basis_min_periods_(basis_min_periods),
      source_max_age_ms_(source_max_age_ms) {
    if (!std::isfinite(source_max_age_ms_) || source_max_age_ms_ < 0.0) {
        throw std::invalid_argument("source_max_age_ms must be finite and non-negative");
    }
    basis_scratch_.reserve(basis_history_.capacity());
}

void SignalRefPerpFeatureEngine::reset() {
    std::lock_guard lock(mutex_);
    trade_aggregator_.reset();
    bars_.clear();
    book_tickers_.clear();
    basis_history_.clear();
    basis_scratch_.clear();
    ++revision_;
}

void SignalRefPerpFeatureEngine::update_trade_batch(
    ArrayView<std::int64_t> ts_ms,
    ArrayView<double> prices,
    ArrayView<double> quantities,
    ArrayView<std::uint8_t> is_buyer_maker
) {
    std::lock_guard lock(mutex_);
    const auto completed = trade_aggregator_.update_batch(
        ts_ms, prices, quantities, is_buyer_maker
    );
    for (const auto& bar : completed) {
        bars_.push_back(bar);
    }
    if (!ts_ms.empty()) {
        ++revision_;
    }
}

void SignalRefPerpFeatureEngine::update_book_ticker(
    double event_ts_ms,
    double receive_ts_ms,
    double bid,
    double ask
) {
    if (!(bid > 0.0) || !(ask > 0.0)) {
        return;
    }
    std::lock_guard lock(mutex_);
    const auto bucket = static_cast<std::int64_t>(
        std::floor(event_ts_ms / 1000.0)
    ) * 1000;
    const SignalRefPerpBookTicker snapshot{
        bucket, bid, ask, event_ts_ms, receive_ts_ms
    };
    if (!book_tickers_.empty() && book_tickers_.back().bucket_ts_ms == bucket) {
        book_tickers_.mutable_back() = snapshot;
    } else {
        book_tickers_.push_back(snapshot);
    }
    ++revision_;
}

std::optional<SignalRefPerpBookTicker>
SignalRefPerpFeatureEngine::book_ticker_at(double target_ts_ms) const {
    const auto view = book_tickers_.view();
    for (std::size_t offset = 0; offset < view.size(); ++offset) {
        const auto& snapshot = view[view.size() - 1 - offset];
        if (static_cast<double>(snapshot.bucket_ts_ms) > target_ts_ms) {
            continue;
        }
        if (snapshot.receive_ts_ms > target_ts_ms) {
            continue;
        }
        if (
            target_ts_ms - snapshot.event_ts_ms > source_max_age_ms_ ||
            target_ts_ms - snapshot.receive_ts_ms > source_max_age_ms_
        ) {
            continue;
        }
        if (snapshot.bid > 0.0 && snapshot.ask > snapshot.bid) {
            return snapshot;
        }
        return std::nullopt;
    }
    return std::nullopt;
}

SignalRefPerpPrepared SignalRefPerpFeatureEngine::prepare(
    std::int64_t target_ts_ms,
    double target_close
) const {
    std::lock_guard lock(mutex_);
    SignalRefPerpPrepared prepared;
    prepared.target_bucket = target_ts_ms / 10000;
    prepared.revision = revision_;
    prepared.owner_token = reinterpret_cast<std::uintptr_t>(this);
    prepared.values.fill(0.0);
    prepared.values[9] = source_max_age_ms_ / 1000.0 + 10.0;

    const auto current_ticker = book_ticker_at(static_cast<double>(target_ts_ms));
    if (current_ticker.has_value() && target_close > 0.0) {
        const double bid = current_ticker->bid;
        const double ask = current_ticker->ask;
        if (bid > 0.0 && ask > bid) {
            const double mid = 0.5 * (bid + ask);
            prepared.values[9] = std::max(
                0.0,
                (static_cast<double>(target_ts_ms) - current_ticker->event_ts_ms) /
                    1000.0
            );
            prepared.values[10] = 1.0;
            prepared.values[0] = (mid - target_close) / target_close * 10000.0;
            const auto fill_return = [&](std::int64_t lookback_ms, std::size_t index) {
                const auto previous = book_ticker_at(
                    static_cast<double>(target_ts_ms - lookback_ms)
                );
                if (previous.has_value()) {
                    const double previous_mid = 0.5 * (previous->bid + previous->ask);
                    prepared.values[index] = safe_log_return(mid, previous_mid);
                }
            };
            fill_return(10000, 1);
            fill_return(30000, 2);
            fill_return(60000, 3);

            std::array<double, 7> mids{};
            std::size_t mid_count = 0;
            for (int step = 6; step >= 0; --step) {
                const auto snapshot = book_ticker_at(
                    static_cast<double>(target_ts_ms) -
                    static_cast<double>(step) * 10000.0
                );
                if (snapshot.has_value()) {
                    mids[mid_count++] = 0.5 * (snapshot->bid + snapshot->ask);
                }
            }
            if (mid_count >= 3) {
                std::array<double, 6> returns{};
                const std::size_t return_count = mid_count - 1;
                for (std::size_t index = 0; index < return_count; ++index) {
                    returns[index] = safe_log_return(mids[index + 1], mids[index]);
                }
                if (return_count >= 2) {
                    prepared.values[4] = indexed_stddev(
                        return_count,
                        [&](std::size_t index) { return returns[index]; },
                        true
                    );
                }
            }
        }
    }

    const auto bars = bars_.view();
    const auto current_bar = trade_aggregator_.current_bar();
    double buy_10s = 0.0;
    double sell_10s = 0.0;
    double latest_trade_ts = -std::numeric_limits<double>::infinity();
    double latest_trade_close = 0.0;
    const std::int64_t start_10s = target_ts_ms - 10000 + 1;
    std::size_t first_10s = bars.size();
    while (first_10s > 0 && bars[first_10s - 1].ts_ms >= start_10s) {
        --first_10s;
    }
    const auto consume_10s = [&](const Bar1s& bar) {
        if (bar.ts_ms < start_10s || bar.ts_ms > target_ts_ms) {
            return;
        }
        buy_10s += bar.buy_volume;
        sell_10s += bar.sell_volume;
        if (static_cast<double>(bar.ts_ms) >= latest_trade_ts) {
            latest_trade_ts = static_cast<double>(bar.ts_ms);
            latest_trade_close = bar.close;
        }
    };
    for (std::size_t index = first_10s; index < bars.size(); ++index) {
        consume_10s(bars[index]);
    }
    if (current_bar.has_value()) {
        consume_10s(*current_bar);
    }
    if (std::isfinite(latest_trade_ts)) {
        const double age_s = std::max(
            0.0,
            (static_cast<double>(target_ts_ms) - latest_trade_ts) / 1000.0
        );
        if (age_s <= source_max_age_ms_ / 1000.0 && prepared.values[10] <= 0.0) {
            prepared.values[9] = age_s;
            prepared.values[10] = 1.0;
            if (latest_trade_close > 0.0 && target_close > 0.0) {
                prepared.values[0] =
                    (latest_trade_close - target_close) / target_close * 10000.0;
            }
        }
        const double total_10s = buy_10s + sell_10s;
        if (total_10s > 0.0) {
            prepared.values[5] = (buy_10s - sell_10s) / total_10s;
        }
    }

    std::array<double, 7> bucket_trade_counts{};
    std::size_t bucket_count = 0;
    std::int64_t last_bucket = std::numeric_limits<std::int64_t>::min();
    double total_volume = 0.0;
    double abs_imbalance = 0.0;
    double bucket_buy = 0.0;
    double bucket_sell = 0.0;
    double bucket_trades = 0.0;
    const auto flush_bucket = [&]() {
        if (last_bucket == std::numeric_limits<std::int64_t>::min()) {
            return;
        }
        if (bucket_count < bucket_trade_counts.size()) {
            bucket_trade_counts[bucket_count++] = bucket_trades;
        }
        total_volume += bucket_buy + bucket_sell;
        abs_imbalance += std::abs(bucket_buy - bucket_sell);
    };

    // The retained source ring is chronological. Iterate its bounded 60-second
    // tail in chronological order so floating-point reduction matches Python.
    const std::int64_t start_60s = target_ts_ms - 60000 + 1;
    std::size_t first = bars.size();
    while (first > 0 && bars[first - 1].ts_ms >= start_60s) {
        --first;
    }
    const auto consume_flow = [&](const Bar1s& bar) {
        if (bar.ts_ms < start_60s || bar.ts_ms > target_ts_ms) {
            return;
        }
        const std::int64_t bucket = bar.ts_ms / 10000;
        if (bucket != last_bucket) {
            flush_bucket();
            last_bucket = bucket;
            bucket_buy = 0.0;
            bucket_sell = 0.0;
            bucket_trades = 0.0;
        }
        bucket_buy += bar.buy_volume;
        bucket_sell += bar.sell_volume;
        bucket_trades += bar.trade_count;
    };
    for (std::size_t index = first; index < bars.size(); ++index) {
        consume_flow(bars[index]);
    }
    if (current_bar.has_value()) {
        consume_flow(*current_bar);
    }
    flush_bucket();
    if (bucket_count > 0) {
        prepared.values[6] = indexed_mean(
            bucket_count,
            [&](std::size_t index) { return bucket_trade_counts[index]; }
        );
        if (total_volume > 0.0) {
            prepared.values[7] = abs_imbalance / total_volume;
        }
    }

    if (prepared.values[10] > 0.0 && std::isfinite(prepared.values[0])) {
        prepared.basis_available = true;
        prepared.basis_bps = prepared.values[0];
        if (basis_history_.size() >= basis_min_periods_) {
            basis_scratch_.clear();
            for (std::size_t index = 0; index < basis_history_.size(); ++index) {
                basis_scratch_.push_back(basis_history_[index].basis_bps);
            }
            std::sort(basis_scratch_.begin(), basis_scratch_.end());
            const std::size_t middle = basis_scratch_.size() / 2;
            const double median = basis_scratch_.size() % 2 == 0
                ? 0.5 * (basis_scratch_[middle - 1] + basis_scratch_[middle])
                : basis_scratch_[middle];
            prepared.values[8] = prepared.basis_bps - median;
        }
    }
    return prepared;
}

void SignalRefPerpFeatureEngine::commit(
    const SignalRefPerpPrepared& prepared
) {
    std::lock_guard lock(mutex_);
    if (
        prepared.owner_token != reinterpret_cast<std::uintptr_t>(this) ||
        prepared.revision != revision_
    ) {
        throw std::runtime_error("ref-perp prepared state is stale");
    }
    if (prepared.basis_available) {
        const SignalRefPerpBasisObservation observation{
            prepared.target_bucket, prepared.basis_bps
        };
        if (
            !basis_history_.empty() &&
            basis_history_.back().bucket == prepared.target_bucket
        ) {
            basis_history_.mutable_back() = observation;
        } else {
            basis_history_.push_back(observation);
        }
    }
    ++revision_;
}

std::size_t SignalRefPerpFeatureEngine::bar_count() const {
    std::lock_guard lock(mutex_);
    return bars_.size() + (trade_aggregator_.current_bar().has_value() ? 1 : 0);
}

std::size_t SignalRefPerpFeatureEngine::book_ticker_count() const {
    std::lock_guard lock(mutex_);
    return book_tickers_.size();
}

std::size_t SignalRefPerpFeatureEngine::basis_count() const {
    std::lock_guard lock(mutex_);
    return basis_history_.size();
}

std::map<std::string, double> SignalFeatureVector::to_map() const {
    // 冷路径：给 Python dict/日志/测试用。live scalar hot path 不应每 tick 调这个函数。
    std::map<std::string, double> out;
    for (std::size_t i = 0; i < values_.size(); ++i) {
        out.emplace(kSignalFeatureNames[i], values_[i]);
    }
    return out;
}

double SignalModelFeatureRow::value_at(std::size_t index) const {
    if (index >= values_.size()) {
        throw std::out_of_range("signal model feature row index out of range");
    }
    return values_[index];
}

std::array<double, 88> SignalModelFeatureRow::legacy_base_values() const noexcept {
    // FEATURE_NAMES_BASE compatibility projection.  Its legacy calendar names
    // point at the corresponding canonical cal_* slots in the 173-row ABI.
    static constexpr std::array<std::size_t, 88> kProjection = {
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
        17, 18, 19, 20, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77,
        78, 97, 79, 80, 81, 82, 98, 83, 84, 85, 99, 86, 87, 88, 100,
        89, 90, 91, 92, 93, 101, 94, 95, 96, 102, 103, 104, 105, 106,
        107, 108, 109, 110, 111, 112, 127, 128, 131, 134, 136, 138,
        139, 129, 130, 167, 168, 169, 170, 171, 172, 157, 158, 159,
        163, 164,
    };
    std::array<double, kProjection.size()> out{};
    for (std::size_t index = 0; index < kProjection.size(); ++index) {
        out[index] = values_[kProjection[index]];
    }
    return out;
}

SignalModelFeatureRow assemble_signal_model_feature_row(
    const SignalFeatureBucketPrepared& bucket,
    const SignalExecutionL2FeatureValues& execution_l2,
    const SignalMetricFeatureValues& metrics,
    const SignalRefPerpFeatureValues& ref_perp,
    const SignalTimeFeatureValues& time
) {
    // kSignalFeatureNames is intentionally grouped for native computation,
    // while the model bundle retains its frozen training order.  This
    // compile-time scatter table is the only translation between them.
    static constexpr std::array<std::size_t, kSignalFeatureCount>
        kCoreToModel = {
            102, 103, 106, 107, 18, 17, 104, 15, 14, 13,
            93, 96, 94, 101, 95, 92, 108, 109, 46, 47,
            45, 48, 38, 39, 37, 40, 34, 35, 33, 36,
            22, 23, 21, 24, 50, 51, 49, 52, 42, 43,
            41, 44, 26, 27, 25, 28, 30, 31, 29, 32,
            12, 11, 10, 8, 9, 20, 16, 7, 19, 88,
            86, 99, 87, 111, 110, 112, 81, 79, 97, 80,
            82, 85, 83, 98, 84, 105, 91, 89, 100, 90,
        };
    static_assert(kCoreToModel.size() == kSignalFeatureCount);

    SignalModelFeatureRow row;
    auto& values = row.values_;
    values[0] = bucket.aggregate.close;
    values[1] = bucket.aggregate.volume;
    values[2] = bucket.aggregate.buy_volume;
    values[3] = bucket.aggregate.sell_volume;
    values[4] = bucket.aggregate.trade_count;
    values[5] = bucket.aggregate.buy_count;
    values[6] = bucket.aggregate.sell_count;

    const auto& core = bucket.core.values();
    for (std::size_t index = 0; index < core.size(); ++index) {
        values[kCoreToModel[index]] = core[index];
    }
    std::copy(
        execution_l2.begin(), execution_l2.end(), values.begin() + 53);
    std::copy(metrics.begin(), metrics.end(), values.begin() + 66);
    std::copy(ref_perp.begin(), ref_perp.end(), values.begin() + 113);
    std::copy(time.begin(), time.end(), values.begin() + 124);
    return row;
}

SignalFeatureEngine::SignalFeatureEngine(std::size_t max_bars, std::size_t max_history)
    : max_bars_(std::max<std::size_t>(1, max_bars)),
      max_history_(std::max<std::size_t>(1, max_history)),
      bars_(max_bars_),
      history_(max_history_),
      return_abs_2160_(std::min<std::size_t>(max_history_, 2160)),
      return_abs_8640_(std::min<std::size_t>(max_history_, 8640)),
      vol_regime_6h_60480_(std::min<std::size_t>(max_history_, 60480)) {}

namespace {

struct SignalExecutionL2Summary {
    SignalExecutionL2FeatureValues state{};
    double total_depth = 0.0;
    double best_bid = 0.0;
    double best_ask = 0.0;
};

std::optional<SignalExecutionL2Summary> summarize_signal_execution_l2_snapshot(
    const SignalExecutionL2Snapshot& snapshot
) {
    const std::size_t depth = std::min(snapshot.depth, kSignalExecutionL2MaxDepth);
    if (depth == 0) {
        return std::nullopt;
    }

    std::array<double, kSignalExecutionL2MaxDepth> bid_cumulative{};
    std::array<double, kSignalExecutionL2MaxDepth> ask_cumulative{};
    std::array<double, kSignalExecutionL2MaxDepth> level_quantity{};
    double bid_total = 0.0;
    double ask_total = 0.0;
    for (std::size_t index = 0; index < depth; ++index) {
        const double bid_quantity = std::max(snapshot.bid_quantity[index], 0.0);
        const double ask_quantity = std::max(snapshot.ask_quantity[index], 0.0);
        bid_total += bid_quantity;
        ask_total += ask_quantity;
        bid_cumulative[index] = bid_total;
        ask_cumulative[index] = ask_total;
        level_quantity[index] = bid_quantity + ask_quantity;
    }

    const double best_bid = snapshot.bid_price[0];
    const double best_ask = snapshot.ask_price[0];
    const double mid = 0.5 * (best_bid + best_ask);
    if (best_bid <= 0.0 || best_ask <= best_bid || mid <= 0.0) {
        return std::nullopt;
    }

    const auto imbalance = [&](std::size_t level) {
        const std::size_t index = std::min(level, depth) - 1;
        const double total = bid_cumulative[index] + ask_cumulative[index];
        return total > 0.0
            ? (bid_cumulative[index] - ask_cumulative[index]) / total
            : 0.0;
    };
    const std::size_t near_index = std::min<std::size_t>(3, depth) - 1;
    const double near_depth = bid_cumulative[near_index] + ask_cumulative[near_index];
    const double total_depth = bid_cumulative[depth - 1] + ask_cumulative[depth - 1];
    const double best_bid_quantity = bid_cumulative[0];
    const double best_ask_quantity = ask_cumulative[0];
    const double micro_den = best_bid_quantity + best_ask_quantity;
    // Keep the two products materialized separately. NumPy's scalar ufuncs in
    // the established Python implementation round each multiplication before
    // the addition; allowing compiler contraction here changes the result by
    // a few ULPs and violates exact feature parity.
    volatile double weighted_bid = best_ask * best_bid_quantity;
    volatile double weighted_ask = best_bid * best_ask_quantity;
    const double microprice = micro_den > 0.0
        ? (weighted_bid + weighted_ask) / micro_den
        : mid;

    const auto mean_range = [&](std::size_t begin, std::size_t end) {
        if (begin >= end) {
            return 0.0;
        }
        double total = 0.0;
        for (std::size_t index = begin; index < end; ++index) {
            total += level_quantity[index];
        }
        return total / static_cast<double>(end - begin);
    };
    const double front_mean = mean_range(0, std::min<std::size_t>(3, depth));
    const double middle_mean = mean_range(3, std::min<std::size_t>(7, depth));
    const double back_mean = mean_range(7, depth);
    const double convexity_den = front_mean + middle_mean + back_mean;

    SignalExecutionL2Summary summary;
    summary.total_depth = total_depth;
    summary.best_bid = best_bid;
    summary.best_ask = best_ask;
    summary.state[0] = (best_ask - best_bid) / mid * 10'000.0;
    summary.state[1] = (microprice - mid) / mid * 10'000.0;
    summary.state[2] = imbalance(1);
    summary.state[3] = imbalance(3);
    summary.state[4] = imbalance(5);
    summary.state[5] = imbalance(10);
    summary.state[6] = near_depth;
    summary.state[7] = total_depth > 0.0 ? near_depth / total_depth : 0.0;
    summary.state[8] = convexity_den > 0.0
        ? (front_mean - 2.0 * middle_mean + back_mean) / convexity_den
        : 0.0;
    summary.state[9] = near_depth > 0.0 ? level_quantity[0] / near_depth : 0.0;
    return summary;
}

}  // namespace

template <typename SnapshotView>
SignalExecutionL2FeatureValues compute_signal_execution_l2_features_view(
    const SnapshotView& snapshots,
    double bucket_end_ms
) {
    SignalExecutionL2FeatureValues values{};
    if (snapshots.empty()) {
        return values;
    }
    const double bucket_start_ms = bucket_end_ms - 10'000.0;

    const SignalExecutionL2Snapshot* state_snapshot = nullptr;
    for (std::size_t offset = snapshots.size(); offset > 0; --offset) {
        const auto& snapshot = snapshots[offset - 1];
        if (snapshot.ts_ms < bucket_end_ms) {
            state_snapshot = &snapshot;
            break;
        }
    }
    const auto state_summary = state_snapshot == nullptr
        ? std::nullopt
        : summarize_signal_execution_l2_snapshot(*state_snapshot);
    if (!state_summary.has_value()) {
        return values;
    }
    std::copy_n(state_summary->state.begin(), 10, values.begin());

    std::optional<SignalExecutionL2Summary> previous;
    for (std::size_t offset = snapshots.size(); offset > 0; --offset) {
        const auto& snapshot = snapshots[offset - 1];
        if (snapshot.ts_ms < bucket_start_ms) {
            previous = summarize_signal_execution_l2_snapshot(snapshot);
            break;
        }
    }

    double flip_count = 0.0;
    double refresh_sum = 0.0;
    double cancel_sum = 0.0;
    std::size_t sample_count = 0;
    for (std::size_t index = 0; index < snapshots.size(); ++index) {
        const auto& snapshot = snapshots[index];
        if (snapshot.ts_ms < bucket_start_ms || snapshot.ts_ms >= bucket_end_ms) {
            continue;
        }
        const auto summary = summarize_signal_execution_l2_snapshot(snapshot);
        if (!summary.has_value()) {
            continue;
        }
        if (previous.has_value()) {
            if (summary->best_bid != previous->best_bid ||
                summary->best_ask != previous->best_ask) {
                flip_count += 1.0;
            }
            if (previous->total_depth > 0.0) {
                const double delta_depth = summary->total_depth - previous->total_depth;
                if (delta_depth > 0.0) {
                    refresh_sum += delta_depth / previous->total_depth;
                } else if (delta_depth < 0.0) {
                    cancel_sum += -delta_depth / previous->total_depth;
                }
            }
        }
        previous = summary;
        ++sample_count;
    }

    if (sample_count > 0) {
        const double denominator = static_cast<double>(sample_count);
        values[10] = flip_count / denominator;
        values[11] = refresh_sum / denominator;
        values[12] = cancel_sum / denominator;
    }
    return values;
}

SignalExecutionL2FeatureValues compute_signal_execution_l2_features(
    std::span<const SignalExecutionL2Snapshot> snapshots,
    double bucket_end_ms
) {
    return compute_signal_execution_l2_features_view(snapshots, bucket_end_ms);
}

template <typename SnapshotView>
SignalExecutionL2PolicyMetricValues
compute_signal_execution_l2_policy_metrics_view(
    const SnapshotView& snapshots,
    double end_exchange_ms
) {
    SignalExecutionL2PolicyMetricValues values{};
    const double start_exchange_ms = end_exchange_ms - 10'000.0;

    std::optional<SignalExecutionL2Summary> previous;
    double flip_count = 0.0;
    double refresh_sum = 0.0;
    double cancel_sum = 0.0;
    std::size_t sample_count = 0;
    for (std::size_t index = 0; index < snapshots.size(); ++index) {
        const auto& snapshot = snapshots[index];
        // Match MakerEngine._current_l2_policy_metrics exactly: both ends of
        // the exchange-time window are inclusive and no pre-window snapshot
        // seeds the first in-window delta.
        if (snapshot.ts_ms < start_exchange_ms ||
            snapshot.ts_ms > end_exchange_ms) {
            continue;
        }
        const auto summary = summarize_signal_execution_l2_snapshot(snapshot);
        if (!summary.has_value()) {
            continue;
        }
        values[3] = summary->state[6];
        if (previous.has_value()) {
            if (summary->best_bid != previous->best_bid ||
                summary->best_ask != previous->best_ask) {
                flip_count += 1.0;
            }
            if (previous->total_depth > 0.0) {
                const double delta_depth = summary->total_depth - previous->total_depth;
                if (delta_depth > 0.0) {
                    refresh_sum += delta_depth / previous->total_depth;
                } else if (delta_depth < 0.0) {
                    cancel_sum += -delta_depth / previous->total_depth;
                }
            }
        }
        previous = summary;
        ++sample_count;
    }

    if (sample_count > 0) {
        const double denominator = static_cast<double>(sample_count);
        values[0] = flip_count / denominator;
        values[1] = refresh_sum / denominator;
        values[2] = cancel_sum / denominator;
    }
    return values;
}

SignalExecutionL2PolicyMetricValues
compute_signal_execution_l2_policy_metrics(
    std::span<const SignalExecutionL2Snapshot> snapshots,
    double end_exchange_ms
) {
    return compute_signal_execution_l2_policy_metrics_view(
        snapshots,
        end_exchange_ms
    );
}

void SignalExecutionL2Engine::reset() {
    std::lock_guard lock(mutex_);
    snapshots_.clear();
}

void SignalExecutionL2Engine::push_snapshot(
    const SignalExecutionL2Snapshot& snapshot
) {
    std::lock_guard lock(mutex_);
    snapshots_.push_back(snapshot);
}

SignalExecutionL2FeatureValues SignalExecutionL2Engine::compute_features(
    double bucket_end_ms
) const {
    std::lock_guard lock(mutex_);
    return compute_signal_execution_l2_features_view(
        snapshots_.view(),
        bucket_end_ms
    );
}

SignalExecutionL2PolicyMetricValues
SignalExecutionL2Engine::compute_policy_metrics(double end_exchange_ms) const {
    std::lock_guard lock(mutex_);
    return compute_signal_execution_l2_policy_metrics_view(
        snapshots_.view(),
        end_exchange_ms
    );
}

std::size_t SignalExecutionL2Engine::snapshot_count() const {
    std::lock_guard lock(mutex_);
    return snapshots_.size();
}

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
    std::size_t low = 0;
    std::size_t high = all.size();
    while (low < high) {
        const std::size_t middle = low + (high - low) / 2;
        if (all[middle].ts_ms < cutoff_exclusive_ms) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    const std::size_t visible_count = low;

    // Python's causal close/sign state is capped at 320 finalized 1s bars.
    // Retain a larger persistent ring for catch-up, then expose the same tail.
    constexpr std::size_t kCausalBarLookback = 320;
    const std::size_t skip = visible_count > kCausalBarLookback
        ? visible_count - kCausalBarLookback
        : 0;
    return compute_signal_feature_vector(
        all.subview(skip, visible_count - skip),
        history_.view(),
        bar_10s,
        &return_abs_2160_, &return_abs_8640_, &vol_regime_6h_60480_);
}

std::pair<Bar1s, SignalFeatureVector> SignalFeatureEngine::compute_bucket(
    std::int64_t bucket_start_ms
) const {
    if (bucket_start_ms < 0 || bucket_start_ms % 10'000 != 0) {
        throw std::invalid_argument("signal bucket start must be 10s-aligned");
    }
    const auto all = bars_.view();
    std::size_t low = 0;
    std::size_t high = all.size();
    while (low < high) {
        const std::size_t middle = low + (high - low) / 2;
        if (all[middle].ts_ms < bucket_start_ms) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    const std::size_t start = low;
    constexpr std::size_t kBucketBars = 10;
    if (all.size() - start < kBucketBars) {
        throw std::runtime_error("completed signal bucket is missing 1s bars");
    }

    Bar1s aggregate = all[start];
    aggregate.ts_ms = bucket_start_ms + 9'000;
    aggregate.volume = 0.0;
    aggregate.buy_volume = 0.0;
    aggregate.sell_volume = 0.0;
    aggregate.trade_count = 0.0;
    aggregate.buy_count = 0.0;
    aggregate.sell_count = 0.0;
    aggregate.quote_qty = 0.0;
    aggregate.buy_quote_qty = 0.0;
    aggregate.sell_quote_qty = 0.0;
    aggregate.max_same_side_run = 0.0;
    aggregate.max_buy_run = 0.0;
    aggregate.max_sell_run = 0.0;
    aggregate.buy_price_high = 0.0;
    aggregate.buy_price_low = 0.0;
    aggregate.sell_price_high = 0.0;
    aggregate.sell_price_low = 0.0;
    for (std::size_t index = 0; index < kBucketBars; ++index) {
        const auto& bar = all[start + index];
        const std::int64_t expected_ts = bucket_start_ms +
            static_cast<std::int64_t>(index) * 1'000;
        if (bar.ts_ms != expected_ts) {
            throw std::runtime_error(
                "completed signal bucket lacks an exact causal 1s grid");
        }
        aggregate.high = std::max(aggregate.high, bar.high);
        aggregate.low = std::min(aggregate.low, bar.low);
        aggregate.close = bar.close;
        aggregate.volume += bar.volume;
        aggregate.buy_volume += bar.buy_volume;
        aggregate.sell_volume += bar.sell_volume;
        aggregate.trade_count += bar.trade_count;
        aggregate.buy_count += bar.buy_count;
        aggregate.sell_count += bar.sell_count;
        aggregate.quote_qty += bar.quote_qty;
        aggregate.buy_quote_qty += bar.buy_quote_qty;
        aggregate.sell_quote_qty += bar.sell_quote_qty;
        aggregate.max_same_side_run = std::max(
            aggregate.max_same_side_run, bar.max_same_side_run);
        aggregate.max_buy_run = std::max(aggregate.max_buy_run, bar.max_buy_run);
        aggregate.max_sell_run = std::max(aggregate.max_sell_run, bar.max_sell_run);
        aggregate.buy_price_high = std::max(
            aggregate.buy_price_high, bar.buy_price_high);
        if (bar.buy_price_low > 0.0) {
            aggregate.buy_price_low = aggregate.buy_price_low > 0.0
                ? std::min(aggregate.buy_price_low, bar.buy_price_low)
                : bar.buy_price_low;
        }
        aggregate.sell_price_high = std::max(
            aggregate.sell_price_high, bar.sell_price_high);
        if (bar.sell_price_low > 0.0) {
            aggregate.sell_price_low = aggregate.sell_price_low > 0.0
                ? std::min(aggregate.sell_price_low, bar.sell_price_low)
                : bar.sell_price_low;
        }
    }
    return {
        aggregate,
        compute_at_cutoff(aggregate, bucket_start_ms + 10'000),
    };
}

SignalFeatureBucketPrepared SignalFeatureEngine::prepare_bucket(
    std::int64_t bucket_start_ms
) const {
    auto [aggregate, core] = compute_bucket(bucket_start_ms);
    return SignalFeatureBucketPrepared{
        .aggregate = aggregate,
        .core = core,
    };
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
