#define NARROWGATE_BINDING_COMMON_DYNAMIC 1

#include <cstdint>
#include <algorithm>
#include <array>
#include <cmath>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#if NARROWGATE_BUILD_HAS_RESEARCH_RUNTIME
#include "dynamic_fill_hazard.hpp"
#endif
#include "quote_core.hpp"
#include "binding_registry.hpp"

namespace py = pybind11;

namespace narrowgate_cpp {
namespace {


#if NARROWGATE_BUILD_HAS_RESEARCH_RUNTIME || \
    defined(NARROWGATE_BINDING_TICK_REPLAY)
template <typename T>
using CArray = py::array_t<T, py::array::c_style | py::array::forcecast>;

template <typename T>
ArrayView<T> view_from_array(const CArray<T>& array) {
    return ArrayView<T>{array.data(), static_cast<std::size_t>(array.size())};
}

template <typename T>
MatrixView<T> matrix_from_array(const CArray<T>& array, const char* name) {
    if (array.ndim() != 2) {
        throw std::invalid_argument(std::string(name) + " must be a 2D C-contiguous array");
    }
    return MatrixView<T>{
        array.data(),
        static_cast<std::size_t>(array.shape(0)),
        static_cast<std::size_t>(array.shape(1)),
    };
}

template <typename T>
py::array_t<T> vector_array(const std::vector<T>& values) {
    py::array_t<T> output(values.size());
    std::copy(values.begin(), values.end(), output.mutable_data());
    return output;
}

template <typename T>
py::array_t<T> vector_matrix(
    const std::vector<T>& values,
    std::size_t rows,
    std::size_t columns
) {
    if (values.size() != rows * columns) {
        throw std::invalid_argument("native feature matrix shape mismatch");
    }
    py::array_t<T> output({rows, columns});
    std::copy(values.begin(), values.end(), output.mutable_data());
    return output;
}
#endif

#if defined(NARROWGATE_BINDING_TRANSPORT_QUOTE) || \
    defined(NARROWGATE_BINDING_LIVE_RUNTIME)
DepthSnapshot depth_from_python_levels(py::handle bids_obj, py::handle asks_obj) {
    DepthSnapshot depth;
    auto append = [](py::handle rows_obj, std::vector<DepthLevel>& out) {
        if (rows_obj.is_none()) {
            return;
        }
        for (py::handle row_obj : py::reinterpret_borrow<py::iterable>(rows_obj)) {
            const auto row = py::reinterpret_borrow<py::sequence>(row_obj);
            if (py::len(row) < 2) {
                continue;
            }
            const double price = py::cast<double>(row[0]);
            const double qty = py::cast<double>(row[1]);
            if (price > 0.0 && qty > 0.0) {
                out.push_back(DepthLevel{price, qty});
            }
        }
    };
    append(bids_obj, depth.bids);
    append(asks_obj, depth.asks);
    return depth;
}
#endif

#if defined(NARROWGATE_BINDING_STREAMING)
py::dict signal_feature_dict(const SignalFeatureVector& features) {
    py::dict out;
    const auto& values = features.values();
    for (std::size_t i = 0; i < values.size(); ++i) {
        const auto name = kSignalFeatureNames[i];
        out[py::str(name.data(), name.size())] = values[i];
    }
    return out;
}

py::array_t<double> signal_feature_array(const SignalFeatureVector& features) {
    py::array_t<double> out(kSignalFeatureCount);
    const auto& values = features.values();
    std::copy(values.begin(), values.end(), out.mutable_data());
    return out;
}

SignalExecutionL2Snapshot signal_execution_l2_snapshot_from_python(
    double ts_ms,
    py::handle bids_obj,
    py::handle asks_obj
) {
    SignalExecutionL2Snapshot snapshot;
    snapshot.ts_ms = ts_ms;
    if (!bids_obj.is_none() && !asks_obj.is_none()) {
        const auto bids = py::reinterpret_borrow<py::sequence>(bids_obj);
        const auto asks = py::reinterpret_borrow<py::sequence>(asks_obj);
        snapshot.depth = std::min<std::size_t>({
            static_cast<std::size_t>(py::len(bids)),
            static_cast<std::size_t>(py::len(asks)),
            kSignalExecutionL2MaxDepth,
        });
        for (std::size_t index = 0; index < snapshot.depth; ++index) {
            const auto bid = py::reinterpret_borrow<py::sequence>(bids[index]);
            const auto ask = py::reinterpret_borrow<py::sequence>(asks[index]);
            if (py::len(bid) < 2 || py::len(ask) < 2) {
                throw std::invalid_argument(
                    "execution L2 price levels must contain price and quantity"
                );
            }
            snapshot.bid_price[index] = py::cast<double>(bid[0]);
            snapshot.bid_quantity[index] = py::cast<double>(bid[1]);
            snapshot.ask_price[index] = py::cast<double>(ask[0]);
            snapshot.ask_quantity[index] = py::cast<double>(ask[1]);
        }
    }
    return snapshot;
}

std::vector<SignalExecutionL2Snapshot> signal_execution_l2_from_python(
    py::handle snapshots_obj
) {
    std::vector<SignalExecutionL2Snapshot> snapshots;
    for (py::handle snapshot_obj :
         py::reinterpret_borrow<py::iterable>(snapshots_obj)) {
        snapshots.push_back(signal_execution_l2_snapshot_from_python(
            py::cast<double>(py::getattr(snapshot_obj, "ts")),
            py::getattr(snapshot_obj, "bids"),
            py::getattr(snapshot_obj, "asks")
        ));
    }
    return snapshots;
}

py::array_t<double> signal_execution_l2_feature_array(
    const SignalExecutionL2FeatureValues& features
) {
    py::array_t<double> out(features.size());
    std::copy(features.begin(), features.end(), out.mutable_data());
    return out;
}

py::array_t<double> signal_execution_l2_policy_metric_array(
    const SignalExecutionL2PolicyMetricValues& metrics
) {
    py::array_t<double> out(metrics.size());
    std::copy(metrics.begin(), metrics.end(), out.mutable_data());
    return out;
}

#endif

#if defined(NARROWGATE_BINDING_RESEARCH)
py::dict sparse_order_lifecycle_dict(
    const SparseOrderLifecycleResult& result
) {
    py::dict out;
    out["activation_status"] = vector_array(result.activation_status);
    out["activation_queue_status"] = vector_array(
        result.activation_queue_status
    );
    out["queue_path_valid"] = vector_array(result.queue_path_valid);
    out["queue_invalid_reason"] = vector_array(result.queue_invalid_reason);
    out["first_touch_ts_ms"] = vector_array(result.first_touch_ts_ms);
    out["first_touch_type"] = vector_array(result.first_touch_type);
    out["exact_touch_ts_ms"] = vector_array(result.exact_touch_ts_ms);
    out["through_touch_ts_ms"] = vector_array(result.through_touch_ts_ms);
    out["first_fill_ts_ms"] = vector_array(result.first_fill_ts_ms);
    out["first_fill_mechanism"] = vector_array(
        result.first_fill_mechanism
    );
    out["fill_qty"] = vector_array(result.fill_qty);
    out["remaining_qty"] = vector_array(result.remaining_qty);
    out["full_fill_ts_ms"] = vector_array(result.full_fill_ts_ms);
    out["partial_fill_count"] = vector_array(result.partial_fill_count);
    out["request_state_observed"] = vector_array(
        result.request_state_observed
    );
    out["request_order_state_before"] = vector_array(
        result.request_order_state_before
    );
    out["request_order_age_ms"] = vector_array(result.request_order_age_ms);
    out["request_remaining_qty"] = vector_array(
        result.request_remaining_qty
    );
    out["request_queue_left"] = vector_array(result.request_queue_left);
    out["request_queue_path_valid"] = vector_array(
        result.request_queue_path_valid
    );
    out["request_native_cancel_count"] = vector_array(
        result.request_native_cancel_count
    );
    out["request_native_cancel_qty"] = vector_array(
        result.request_native_cancel_qty
    );
    out["request_native_refill_count"] = vector_array(
        result.request_native_refill_count
    );
    out["request_native_refill_qty"] = vector_array(
        result.request_native_refill_qty
    );
    out["request_native_level_event_count"] = vector_array(
        result.request_native_level_event_count
    );
    out["cancel_acked"] = vector_array(result.cancel_acked);
    out["fill_while_cancel_pending_qty"] = vector_array(
        result.fill_while_cancel_pending_qty
    );
    out["first_pending_cancel_fill_ts_ms"] = vector_array(
        result.first_pending_cancel_fill_ts_ms
    );
    out["terminal_state"] = vector_array(result.terminal_state);
    out["terminal_ts_ms"] = vector_array(result.terminal_ts_ms);
    out["terminal_reason"] = vector_array(result.terminal_reason);
    out["native_cancel_count"] = vector_array(result.native_cancel_count);
    out["native_cancel_qty"] = vector_array(result.native_cancel_qty);
    out["native_refill_count"] = vector_array(result.native_refill_count);
    out["native_refill_qty"] = vector_array(result.native_refill_qty);
    out["native_level_event_count"] = vector_array(
        result.native_level_event_count
    );
    out["same_ms_ambiguity_count"] = vector_array(
        result.same_ms_ambiguity_count
    );
    return out;
}

#endif

#if defined(NARROWGATE_BINDING_STREAMING)
py::dict global_flow_window_dict(const GlobalFlowMarketWindow& row) {
    py::dict out;
    out["market_id"] = row.market_id;
    out["horizon_ms"] = row.horizon_ms;
    out["book_events"] = row.book_events;
    out["trade_events"] = row.trade_events;
    out["book_age_ms"] = row.book_age_ms;
    out["trade_age_ms"] = row.trade_age_ms;
    out["book_fresh"] = static_cast<int>(row.book_fresh);
    out["aggressive_buy_volume"] = row.aggressive_buy_volume;
    out["aggressive_sell_volume"] = row.aggressive_sell_volume;
    out["trade_imbalance"] = row.trade_imbalance;
    out["l1_ofi"] = row.l1_ofi;
    out["l1_ofi_normalized"] = row.l1_ofi_normalized;
    out["bid_depletion"] = row.bid_depletion;
    out["bid_refill"] = row.bid_refill;
    out["ask_depletion"] = row.ask_depletion;
    out["ask_refill"] = row.ask_refill;
    out["mid_move_bps"] = row.mid_move_bps;
    out["flow_pressure"] = row.flow_pressure;
    out["gap_events"] = row.gap_events;
    out["gap_known_events"] = row.gap_known_events;
    out["out_of_order_events"] = row.out_of_order_events;
    out["stale_trade_events"] = row.stale_trade_events;
    out["book_overflow_events"] = row.book_overflow_events;
    out["trade_overflow_events"] = row.trade_overflow_events;
    return out;
}

py::dict global_flow_stats_dict(const GlobalFlowStats& stats) {
    py::dict out;
    out["market_count"] = stats.market_count;
    out["book_events_seen"] = stats.book_events_seen;
    out["book_events_accepted"] = stats.book_events_accepted;
    out["trade_batches"] = stats.trade_batches;
    out["trade_events_seen"] = stats.trade_events_seen;
    out["trade_events_accepted"] = stats.trade_events_accepted;
    out["out_of_order_events"] = stats.out_of_order_events;
    out["stale_trade_events"] = stats.stale_trade_events;
    out["book_overflow_events"] = stats.book_overflow_events;
    out["trade_overflow_events"] = stats.trade_overflow_events;
    return out;
}

#endif

#if defined(NARROWGATE_BINDING_TICK_REPLAY)
void set_trade_arrays(
    TickReplayInput& input,
    const CArray<std::int64_t>& trade_ts_ms,
    const CArray<double>& trade_price,
    const CArray<double>& trade_qty,
    const CArray<std::uint8_t>& is_buyer_maker
) {
    input.trade_ts_ms = view_from_array(trade_ts_ms);
    input.trade_price = view_from_array(trade_price);
    input.trade_qty = view_from_array(trade_qty);
    input.is_buyer_maker = view_from_array(is_buyer_maker);
}

void set_feature_arrays(
    TickReplayInput& input,
    const CArray<std::int64_t>& var_ts_ms,
    const CArray<double>& var_ssq,
    const CArray<double>& var_ti,
    const CArray<double>& var_retsq,
    const CArray<std::int64_t>& ml_ts_ms,
    const CArray<double>& ml_dir_10s,
    const CArray<double>& ml_vol_10s,
    const CArray<double>& ml_ret_10s,
    const CArray<double>& ml_tox_bid,
    const CArray<double>& ml_tox_ask
) {
    input.var_ts_ms = view_from_array(var_ts_ms);
    input.var_ssq = view_from_array(var_ssq);
    input.var_ti = view_from_array(var_ti);
    input.var_retsq = view_from_array(var_retsq);
    input.ml_ts_ms = view_from_array(ml_ts_ms);
    input.ml_dir_10s = view_from_array(ml_dir_10s);
    input.ml_vol_10s = view_from_array(ml_vol_10s);
    input.ml_ret_10s = view_from_array(ml_ret_10s);
    input.ml_tox_bid = view_from_array(ml_tox_bid);
    input.ml_tox_ask = view_from_array(ml_tox_ask);
}

void set_conditional_p3_arrays(
    TickReplayInput& input,
    const CArray<std::int64_t>& p3_ts_ms,
    const CArray<double>& p3_delta_star,
    const CArray<double>& p3_kappa_eff
) {
    input.p3_ts_ms = view_from_array(p3_ts_ms);
    input.p3_delta_star = view_from_array(p3_delta_star);
    input.p3_kappa_eff = view_from_array(p3_kappa_eff);
}

void set_conditional_p3_reach_gate_arrays(
    TickReplayInput& input,
    const CArray<std::int64_t>& timestamps_ms,
    const CArray<std::uint8_t>& status
) {
    input.p3_reach_gate_ts_ms = view_from_array(timestamps_ms);
    input.p3_reach_gate_status = matrix_from_array(
        status, "conditional P3 reach gate status"
    );
}

void set_conditional_p3_reach_budget_arrays(
    TickReplayInput& input,
    const CArray<std::int64_t>& timestamps_ms,
    const CArray<std::uint8_t>& selected_k
) {
    input.p3_reach_budget_ts_ms = view_from_array(timestamps_ms);
    input.p3_reach_budget_selected_k = matrix_from_array(
        selected_k, "conditional P3 reach budget selected-k"
    );
}

void set_cross_venue_fair_center_arrays(
    TickReplayInput& input,
    const CArray<std::int64_t>& timestamps_ms,
    const CArray<double>& fair_price,
    const CArray<double>& gain,
    const CArray<std::uint8_t>& valid
) {
    input.fair_center_ts_ms = view_from_array(timestamps_ms);
    input.fair_center_price = view_from_array(fair_price);
    input.fair_center_gain = view_from_array(gain);
    input.fair_center_valid = view_from_array(valid);
}

void set_buy_fill_selection_static_arrays(
    TickReplayInput& input,
    const CArray<double>& logit_delta,
    const CArray<double>& missing,
    const CArray<double>& used
) {
    input.buy_fill_static_logit_delta =
        matrix_from_array(logit_delta, "buy_fill_static_logit_delta");
    input.buy_fill_static_missing =
        matrix_from_array(missing, "buy_fill_static_missing");
    input.buy_fill_static_used =
        matrix_from_array(used, "buy_fill_static_used");
}

void set_book_arrays(
    TickReplayInput& input,
    const CArray<std::int64_t>& bbo_ts_ms,
    const CArray<double>& bbo_best_bid,
    const CArray<double>& bbo_best_ask,
    const CArray<double>& bbo_bid_qty,
    const CArray<double>& bbo_ask_qty,
    const CArray<std::int64_t>& l2_ts_ms,
    const CArray<double>& l2_bid_px,
    const CArray<double>& l2_bid_qty,
    const CArray<double>& l2_ask_px,
    const CArray<double>& l2_ask_qty
) {
    input.bbo_ts_ms = view_from_array(bbo_ts_ms);
    input.bbo_best_bid = view_from_array(bbo_best_bid);
    input.bbo_best_ask = view_from_array(bbo_best_ask);
    input.bbo_bid_qty = view_from_array(bbo_bid_qty);
    input.bbo_ask_qty = view_from_array(bbo_ask_qty);
    input.l2_ts_ms = view_from_array(l2_ts_ms);
    input.l2_bid_px = matrix_from_array(l2_bid_px, "l2_bid_px");
    input.l2_bid_qty = matrix_from_array(l2_bid_qty, "l2_bid_qty");
    input.l2_ask_px = matrix_from_array(l2_ask_px, "l2_ask_px");
    input.l2_ask_qty = matrix_from_array(l2_ask_qty, "l2_ask_qty");
}

void set_per_trade_policy_arrays(
    TickReplayInput& input,
    const CArray<double>& queue_base_by_trade,
    const CArray<double>& queue_decay_by_trade,
    const CArray<double>& buy_fill_prob_by_trade,
    const CArray<double>& sell_fill_prob_by_trade,
    const CArray<double>& buy_queue_deplete_mult_by_trade,
    const CArray<double>& sell_queue_deplete_mult_by_trade
) {
    input.queue_base_by_trade = view_from_array(queue_base_by_trade);
    input.queue_decay_by_trade = view_from_array(queue_decay_by_trade);
    input.buy_fill_prob_by_trade = view_from_array(buy_fill_prob_by_trade);
    input.sell_fill_prob_by_trade = view_from_array(sell_fill_prob_by_trade);
    input.buy_queue_deplete_mult_by_trade = view_from_array(buy_queue_deplete_mult_by_trade);
    input.sell_queue_deplete_mult_by_trade = view_from_array(sell_queue_deplete_mult_by_trade);
}

#endif

#if NARROWGATE_BUILD_HAS_RESEARCH_RUNTIME
template <typename T>
T required_dict_value(const py::dict& payload, const char* key) {
    const py::str name(key);
    if (!payload.contains(name)) {
        throw std::invalid_argument(
            std::string("dynamic fill-hazard payload is missing ") + key
        );
    }
    return py::cast<T>(payload[name]);
}

template <typename T>
T optional_dict_value(
    const py::dict& payload,
    const char* key,
    const T& fallback
) {
    const py::str name(key);
    if (!payload.contains(name) || payload[name].is_none()) {
        return fallback;
    }
    return py::cast<T>(payload[name]);
}

std::int64_t optional_update_id(const py::handle& value) {
    return value.is_none() ? -1 : py::cast<std::int64_t>(value);
}

NativeBookEventType native_book_event_type(const std::string& value) {
    if (value == "snapshot") {
        return NativeBookEventType::Snapshot;
    }
    if (value == "delta") {
        return NativeBookEventType::Delta;
    }
    if (value == "source_gap") {
        return NativeBookEventType::SourceGap;
    }
    throw std::invalid_argument("unsupported native book event type: " + value);
}

std::vector<NativeBookLevel> native_book_levels(py::handle rows_obj) {
    std::vector<NativeBookLevel> levels;
    if (rows_obj.is_none()) {
        return levels;
    }
    for (py::handle row_obj : py::reinterpret_borrow<py::iterable>(rows_obj)) {
        const auto row = py::reinterpret_borrow<py::sequence>(row_obj);
        if (py::len(row) != 3) {
            throw std::invalid_argument(
                "native book levels must be (side, price_tick, quantity)"
            );
        }
        const std::string side = py::cast<std::string>(row[0]);
        const bool is_bid = side == "bid" || side == "BUY" || side == "buy";
        if (!is_bid && side != "ask" && side != "SELL" && side != "sell") {
            throw std::invalid_argument("native book level side is invalid");
        }
        levels.push_back(NativeBookLevel{
            is_bid,
            py::cast<std::int64_t>(row[1]),
            py::cast<double>(row[2]),
        });
    }
    return levels;
}

template <std::size_t Size>
std::array<double, Size> fixed_double_array(
    py::handle value,
    const char* name
) {
    const auto sequence = py::reinterpret_borrow<py::sequence>(value);
    if (static_cast<std::size_t>(py::len(sequence)) != Size) {
        throw std::invalid_argument(std::string(name) + " has the wrong length");
    }
    std::array<double, Size> output{};
    for (std::size_t index = 0; index < Size; ++index) {
        output[index] = py::cast<double>(sequence[index]);
    }
    return output;
}

DynamicFillHazardHead dynamic_fill_hazard_head(const py::dict& payload) {
    const auto names = py::reinterpret_borrow<py::sequence>(
        payload[py::str("feature_names")]
    );
    if (
        static_cast<std::size_t>(py::len(names)) !=
        kDynamicFillHazardFeatureCount
    ) {
        throw std::invalid_argument(
            "dynamic fill-hazard feature schema has the wrong length"
        );
    }
    for (std::size_t index = 0; index < kDynamicFillHazardFeatureCount; ++index) {
        if (py::cast<std::string>(names[index]) !=
            kDynamicFillHazardFeatureNames[index]) {
            throw std::invalid_argument(
                "dynamic fill-hazard feature schema/order changed"
            );
        }
    }

    DynamicFillHazardHead head;
    head.feature_mean = fixed_double_array<kDynamicFillHazardFeatureCount>(
        payload[py::str("feature_mean")], "feature_mean"
    );
    head.feature_scale = fixed_double_array<kDynamicFillHazardFeatureCount>(
        payload[py::str("feature_scale")], "feature_scale"
    );
    head.coefficients = fixed_double_array<kDynamicFillHazardFeatureCount>(
        payload[py::str("coefficients")], "coefficients"
    );
    head.intercept = required_dict_value<double>(payload, "intercept");
    const py::str calibrator_name("nested_calibrator");
    if (payload.contains(calibrator_name) && !payload[calibrator_name].is_none()) {
        const auto calibrator = py::cast<py::dict>(payload[calibrator_name]);
        const auto contract = required_dict_value<py::dict>(
            calibrator, "contract"
        );
        const auto clip = py::reinterpret_borrow<py::sequence>(
            contract[py::str("probability_clip")]
        );
        if (py::len(clip) != 2) {
            throw std::invalid_argument(
                "dynamic fill-hazard probability clip is invalid"
            );
        }
        head.has_calibrator = true;
        head.calibrator_intercept = required_dict_value<double>(
            calibrator, "intercept"
        );
        head.calibrator_slope = required_dict_value<double>(
            calibrator, "slope"
        );
        head.probability_clip_lower = py::cast<double>(clip[0]);
        head.probability_clip_upper = py::cast<double>(clip[1]);
    }
    return head;
}

DynamicFillHazardModel dynamic_fill_hazard_model(const py::dict& payload) {
    return DynamicFillHazardModel{
        dynamic_fill_hazard_head(
            required_dict_value<py::dict>(payload, "favorable_fill")
        ),
        dynamic_fill_hazard_head(
            required_dict_value<py::dict>(payload, "adverse_fill")
        ),
    };
}

DynamicFillHazardRuntimeConfig dynamic_fill_hazard_config(
    const py::dict& payload
) {
    DynamicFillHazardRuntimeConfig config;
    config.tick_size = required_dict_value<double>(payload, "tick_size");
    config.lot_size = required_dict_value<double>(payload, "lot_size");
    config.exposure_ms = required_dict_value<double>(payload, "exposure_ms");
    config.price_jump_ticks = required_dict_value<double>(
        payload, "price_jump_ticks"
    );
    config.evaluation_interval_ms = required_dict_value<double>(
        payload, "evaluation_interval_ms"
    );
    config.entry_threshold = required_dict_value<double>(
        payload, "entry_threshold"
    );
    config.strict_sequence = optional_dict_value<bool>(
        payload, "strict_sequence", true
    );
    config.strict_after_ns = optional_dict_value<std::int64_t>(
        payload, "strict_after_ns", 0
    );
    return config;
}

py::dict dynamic_fill_hazard_prediction_dict(
    const DynamicFillHazardPrediction& value
) {
    py::dict out;
    out["favorable_raw_probability"] = value.favorable_raw_probability;
    out["favorable_probability"] = value.favorable_probability;
    out["adverse_raw_probability"] = value.adverse_raw_probability;
    out["adverse_probability"] = value.adverse_probability;
    out["score"] = value.score;
    return out;
}

py::dict native_book_lookup_dict(const NativeBookLookup& value) {
    py::dict out;
    out["status"] = value.status;
    out["reason"] = value.reason;
    out["quantity"] = value.quantity_known
        ? py::cast(value.quantity)
        : py::none();
    out["strict_usable"] = value.strict_usable();
    out["asof_exchange_ts_ns"] = value.asof_exchange_ts_ns;
    out["segment_id"] = value.segment_id;
    out["snapshot_min_tick"] = value.snapshot_range_known
        ? py::cast(value.snapshot_min_tick)
        : py::none();
    out["snapshot_max_tick"] = value.snapshot_range_known
        ? py::cast(value.snapshot_max_tick)
        : py::none();
    return out;
}

py::dict native_book_top_dict(const NativeBookTop& value) {
    py::dict out;
    out["valid"] = value.valid;
    out["best_bid_tick"] = value.best_bid_tick;
    out["best_ask_tick"] = value.best_ask_tick;
    out["best_bid_qty"] = value.best_bid_qty;
    out["best_ask_qty"] = value.best_ask_qty;
    out["last_exchange_ts_ns"] = value.last_exchange_ts_ns;
    out["last_receive_ts_ns"] = value.last_receive_ts_ns;
    out["segment_id"] = value.segment_id;
    return out;
}

py::dict native_book_apply_result_dict(const NativeBookApplyResult& value) {
    py::dict out;
    out["accepted"] = value.accepted;
    out["snapshot_reset"] = value.snapshot_reset;
    out["invalidated"] = value.invalidated;
    py::list changes;
    for (const auto& change : value.changes) {
        py::dict row;
        row["side"] = change.is_bid ? "bid" : "ask";
        row["price_tick"] = change.price_tick;
        row["quantity_before"] = change.quantity_before;
        row["quantity_after"] = change.quantity_after;
        row["exchange_ts_ns"] = change.exchange_ts_ns;
        row["receive_ts_ns"] = change.receive_ts_ns;
        row["update_id"] = change.update_id;
        row["segment_id"] = change.segment_id;
        changes.append(std::move(row));
    }
    out["changes"] = std::move(changes);
    return out;
}

py::dict native_book_stats_dict(const NativeBookSequenceStats& value) {
    py::dict out;
    out["logical_messages"] = value.logical_messages;
    out["snapshot_messages"] = value.snapshot_messages;
    out["update_messages"] = value.update_messages;
    out["duplicate_messages"] = value.duplicate_messages;
    out["duplicate_snapshots"] = value.duplicate_snapshots;
    out["stale_updates"] = value.stale_updates;
    out["ignored_before_snapshot"] = value.ignored_before_snapshot;
    out["sequence_gaps"] = value.sequence_gaps;
    out["invalid_sequence_messages"] = value.invalid_sequence_messages;
    out["accepted_updates"] = value.accepted_updates;
    out["delta_bootstrap_messages"] = value.delta_bootstrap_messages;
    out["message_time_reversals"] = value.message_time_reversals;
    return out;
}

py::dict dynamic_fill_hazard_evaluation_dict(
    const DynamicFillHazardEvaluation& value
) {
    py::dict out;
    out["emitted"] = value.emitted;
    out["valid"] = value.valid;
    out["reason"] = value.reason;
    out["inventory_role"] = value.inventory_role;
    out["action"] = value.action;
    out["edge_ms"] = value.edge_ms;
    out["elapsed_ms"] = value.elapsed_ms;
    out["missed_edges"] = value.missed_edges;
    out["feature_source_ts_ns"] = value.feature_source_ts_ns;
    out["feature_ready_ts_ns"] = value.feature_ready_ts_ns;
    out["deep_generation"] = value.deep_generation;
    out["deep_age_ms"] = value.deep_age_ms;
    out["order_price"] = value.order_price;
    out["mid"] = value.mid;
    out["microprice"] = value.microprice;
    out["queue_initial"] = value.queue_initial;
    out["queue_remaining"] = value.queue_remaining;
    out["cancel_events"] = value.cancel_events;
    out["cancel_qty"] = value.cancel_qty;
    out["refill_events"] = value.refill_events;
    out["refill_qty"] = value.refill_qty;
    out["prediction"] = dynamic_fill_hazard_prediction_dict(value.prediction);
    return out;
}

py::dict dynamic_fill_hazard_counters_dict(
    const DynamicFillHazardRuntimeCounters& value
) {
    py::dict out;
    out["eval_count"] = value.eval_count;
    out["valid_eval_count"] = value.valid_eval_count;
    out["invalid_eval_count"] = value.invalid_eval_count;
    out["keep_count"] = value.keep_count;
    out["cancel_request_count"] = value.cancel_request_count;
    out["cancel_ack_count"] = value.cancel_ack_count;
    out["pre_ack_fill_count"] = value.pre_ack_fill_count;
    out["recovery_count"] = value.recovery_count;
    out["reentry_count"] = value.reentry_count;
    out["retain_invalid_count"] = value.retain_invalid_count;
    out["prospective_eval_count"] = value.prospective_eval_count;
    out["prospective_valid_count"] = value.prospective_valid_count;
    out["prospective_invalid_count"] = value.prospective_invalid_count;
    return out;
}
#endif

}  // namespace

void bind_common(py::module_& m) {
    py::enum_<Side>(m, "Side")
        .value("Buy", Side::Buy)
        .value("Sell", Side::Sell)
        .export_values();

    m.def(
        "price_to_tick",
        &price_to_tick,
        py::arg("price"),
        py::arg("tick_size")
    );
    m.def(
        "same_price_tick",
        &same_price_tick,
        py::arg("lhs"),
        py::arg("rhs"),
        py::arg("tick_size")
    );
    m.def(
        "trade_crosses_order_ticks",
        [](Side side, double trade_price, double order_price, double tick_size) {
            const auto trade_tick = price_to_tick(trade_price, tick_size);
            const auto order_tick = price_to_tick(order_price, tick_size);
            return side == Side::Buy
                ? trade_tick <= order_tick
                : trade_tick >= order_tick;
        },
        py::arg("side"),
        py::arg("trade_price"),
        py::arg("order_price"),
        py::arg("tick_size")
    );

    py::class_<DepthLevel>(m, "DepthLevel")
        .def(py::init<>())
        .def_readwrite("price", &DepthLevel::price)
        .def_readwrite("qty", &DepthLevel::qty);

    py::class_<DepthSnapshot>(m, "DepthSnapshot")
        .def(py::init<>())
        .def_readwrite("bids", &DepthSnapshot::bids)
        .def_readwrite("asks", &DepthSnapshot::asks)
        .def("has_book", &DepthSnapshot::has_book)
        .def("best_bid", &DepthSnapshot::best_bid)
        .def("best_ask", &DepthSnapshot::best_ask);

    py::class_<QuotePrediction>(m, "QuotePrediction")
        .def(py::init<>())
        .def_readwrite("dir_10s", &QuotePrediction::dir_10s)
        .def_readwrite("vol_10s", &QuotePrediction::vol_10s)
        .def_readwrite("ret_10s", &QuotePrediction::ret_10s)
        .def_readwrite("tox_bid", &QuotePrediction::tox_bid)
        .def_readwrite("tox_ask", &QuotePrediction::tox_ask);

    py::class_<QuoteState>(m, "QuoteState")
        .def(py::init<>())
        .def_readwrite("mid", &QuoteState::mid)
        .def_readwrite("inventory", &QuoteState::inventory)
        .def_readwrite("sigma_sq", &QuoteState::sigma_sq)
        .def_readwrite("trade_intensity", &QuoteState::trade_intensity)
        .def_readwrite("best_bid", &QuoteState::best_bid)
        .def_readwrite("best_ask", &QuoteState::best_ask)
        .def_readwrite("ber_active", &QuoteState::ber_active)
        .def_readwrite("mo_ema_all", &QuoteState::mo_ema_all)
        .def_readwrite("mo_ema_bid", &QuoteState::mo_ema_bid)
        .def_readwrite("mo_ema_ask", &QuoteState::mo_ema_ask)
        .def_readwrite("bid_adverse_markout_pause_latch", &QuoteState::bid_adverse_markout_pause_latch)
        .def_readwrite("ask_adverse_markout_pause_latch", &QuoteState::ask_adverse_markout_pause_latch)
        .def_readwrite("mo_ref", &QuoteState::mo_ref)
        .def_readwrite("position_open", &QuoteState::position_open)
        .def_readwrite("hold_time_s", &QuoteState::hold_time_s)
        .def_readwrite("unrealized_pnl", &QuoteState::unrealized_pnl);

#if NARROWGATE_BUILD_HAS_RESEARCH_RUNTIME
    m.def(
        "integrate_variance_time_episode",
        [](
            const CArray<std::int64_t>& feature_ready_ts_ms,
            const CArray<double>& mid_price,
            const CArray<double>& sigma_sq_price_per_s,
            const CArray<std::uint8_t>& valid,
            std::int64_t episode_start_ts_ms,
            double budget_bps2,
            std::int64_t minimum_wall_time_ms,
            std::int64_t maximum_wall_time_ms,
            std::int64_t max_feature_age_ms,
            std::int64_t censor_ts_ms
        ) {
            const auto count = feature_ready_ts_ms.size();
            if (
                mid_price.size() != count ||
                sigma_sq_price_per_s.size() != count ||
                valid.size() != count
            ) {
                throw std::invalid_argument("variance-time arrays must have equal length");
            }
            if (
                maximum_wall_time_ms <= 0 ||
                minimum_wall_time_ms < 0 ||
                maximum_wall_time_ms < minimum_wall_time_ms
            ) {
                throw std::invalid_argument("invalid variance-time wall bounds");
            }
            if (!std::isfinite(budget_bps2) || budget_bps2 <= 0.0) {
                throw std::invalid_argument("variance-time budget must be positive");
            }
            for (py::ssize_t index = 1; index < count; ++index) {
                if (feature_ready_ts_ms.data()[index] < feature_ready_ts_ms.data()[index - 1]) {
                    throw std::invalid_argument("variance-time timestamps must be sorted");
                }
            }
            const std::int64_t deadline = episode_start_ts_ms + maximum_wall_time_ms;
            const std::int64_t censor = censor_ts_ms >= 0 ? censor_ts_ms : deadline;
            const std::int64_t stop = std::min(deadline, censor);
            const std::int64_t max_age = std::max<std::int64_t>(0, max_feature_age_ms);
            double qv = 0.0;
            double stale_ms = 0.0;
            double valid_ms = 0.0;
            double budget_hit = -1.0;
            std::int64_t covered_until = episode_start_ts_ms;
            for (py::ssize_t index = 0; index < count; ++index) {
                const std::int64_t ready = feature_ready_ts_ms.data()[index];
                if (ready >= stop) {
                    break;
                }
                const std::int64_t interval_start = std::max(episode_start_ts_ms, ready);
                const std::int64_t next_ready = index + 1 < count
                    ? feature_ready_ts_ms.data()[index + 1]
                    : stop;
                const std::int64_t interval_end = std::min(stop, next_ready);
                if (interval_end <= interval_start) {
                    continue;
                }
                if (interval_start > covered_until) {
                    stale_ms += static_cast<double>(interval_start - covered_until);
                }
                const std::int64_t fresh_end = std::min(interval_end, ready + max_age);
                const std::int64_t valid_end = std::max(interval_start, fresh_end);
                const double mid = mid_price.data()[index];
                const double sigma_sq = sigma_sq_price_per_s.data()[index];
                const bool interval_valid =
                    valid.data()[index] != 0 &&
                    ready <= interval_start &&
                    std::isfinite(mid) && mid > 0.0 &&
                    std::isfinite(sigma_sq) && sigma_sq >= 0.0 &&
                    valid_end > interval_start;
                if (interval_valid) {
                    const double duration_ms = static_cast<double>(valid_end - interval_start);
                    const double rate = 1.0e8 * sigma_sq / (mid * mid);
                    const double increment = rate * duration_ms / 1000.0;
                    if (rate > 0.0 && qv + increment >= budget_bps2 && budget_hit < 0.0) {
                        budget_hit = interval_start + (budget_bps2 - qv) / rate * 1000.0;
                    }
                    qv += increment;
                    valid_ms += duration_ms;
                    if (interval_end > valid_end) {
                        stale_ms += static_cast<double>(interval_end - valid_end);
                    }
                } else {
                    stale_ms += static_cast<double>(interval_end - interval_start);
                }
                covered_until = std::max(covered_until, interval_end);
                if (budget_hit >= 0.0) {
                    const double release = std::max(
                        static_cast<double>(episode_start_ts_ms + minimum_wall_time_ms),
                        budget_hit
                    );
                    if (release <= static_cast<double>(stop)) {
                        py::dict out;
                        out["rearm_ts_ms"] = static_cast<std::int64_t>(std::ceil(release));
                        out["rearm_elapsed_ms"] = release - episode_start_ts_ms;
                        out["reason"] = "variance_budget";
                        out["accumulated_qv_bps2"] = qv;
                        out["stale_frozen_ms"] = stale_ms;
                        out["valid_interval_ms"] = valid_ms;
                        out["budget_reached_ts_ms"] = budget_hit;
                        return out;
                    }
                }
            }
            if (covered_until < stop) {
                stale_ms += static_cast<double>(stop - covered_until);
            }
            py::dict out;
            out["accumulated_qv_bps2"] = qv;
            out["stale_frozen_ms"] = stale_ms;
            out["valid_interval_ms"] = valid_ms;
            if (budget_hit >= 0.0) {
                out["budget_reached_ts_ms"] = budget_hit;
            } else {
                out["budget_reached_ts_ms"] = py::none();
            }
            if (deadline <= censor) {
                out["rearm_ts_ms"] = deadline;
                out["rearm_elapsed_ms"] = static_cast<double>(maximum_wall_time_ms);
                out["reason"] = "maximum_wall_time";
            } else {
                out["rearm_ts_ms"] = py::none();
                out["rearm_elapsed_ms"] = py::none();
                out["reason"] = "censored";
            }
            return out;
        },
        py::arg("feature_ready_ts_ms"),
        py::arg("mid_price"),
        py::arg("sigma_sq_price_per_s"),
        py::arg("valid"),
        py::arg("episode_start_ts_ms"),
        py::arg("budget_bps2"),
        py::arg("minimum_wall_time_ms"),
        py::arg("maximum_wall_time_ms"),
        py::arg("max_feature_age_ms"),
        py::arg("censor_ts_ms") = -1
    );
#endif
}

#if NARROWGATE_BUILD_HAS_RESEARCH_RUNTIME
void bind_dynamic_fill_hazard(py::module_& m) {
    m.attr("DYNAMIC_FILL_HAZARD_ABI_VERSION") =
        "dynamic_fill_hazard_native_book_q90.v4";
    m.def(
        "dynamic_fill_hazard_predict",
        [](
            const py::dict& model_payload,
            py::handle feature_values,
            double exposure_ms
        ) {
            const auto features =
                fixed_double_array<kDynamicFillHazardFeatureCount>(
                    feature_values, "dynamic fill-hazard features"
                );
            return dynamic_fill_hazard_prediction_dict(
                predict_dynamic_fill_hazard(
                    dynamic_fill_hazard_model(model_payload),
                    features,
                    exposure_ms
                )
            );
        },
        py::arg("model_payload"),
        py::arg("feature_values"),
        py::arg("exposure_ms")
    );

    py::class_<NativeExchangeBookSchedulerCpp>(
        m, "NativeExchangeBookScheduler"
    )
        .def(
            py::init<bool, std::int64_t, bool>(),
            py::arg("strict_sequence") = true,
            py::arg("strict_after_ns") = 0,
            py::arg("allow_delta_bootstrap") = false
        )
        .def(
            "apply_message",
            [](
                NativeExchangeBookSchedulerCpp& scheduler,
                const std::string& event_type,
                std::int64_t exchange_ts_ns,
                std::int64_t receive_ts_ns,
                std::int64_t event_time_ms,
                std::int64_t transaction_time_ms,
                py::object first_update_id,
                py::object final_update_id,
                py::object previous_final_update_id,
                py::object last_update_id,
                py::handle levels
            ) {
                return native_book_apply_result_dict(scheduler.apply_message(
                    native_book_event_type(event_type),
                    exchange_ts_ns,
                    receive_ts_ns,
                    event_time_ms,
                    transaction_time_ms,
                    optional_update_id(first_update_id),
                    optional_update_id(final_update_id),
                    optional_update_id(previous_final_update_id),
                    optional_update_id(last_update_id),
                    native_book_levels(levels)
                ));
            },
            py::arg("event_type"),
            py::arg("exchange_ts_ns"),
            py::arg("receive_ts_ns") = 0,
            py::arg("event_time_ms") = 0,
            py::arg("transaction_time_ms") = 0,
            py::arg("first_update_id") = py::none(),
            py::arg("final_update_id") = py::none(),
            py::arg("previous_final_update_id") = py::none(),
            py::arg("last_update_id") = py::none(),
            py::arg("levels") = py::tuple()
        )
        .def(
            "lookup",
            [](const NativeExchangeBookSchedulerCpp& scheduler,
               const std::string& side,
               std::int64_t price_tick) {
                const bool is_bid = side == "bid" || side == "BUY" ||
                    side == "buy";
                if (!is_bid && side != "ask" && side != "SELL" &&
                    side != "sell") {
                    throw std::invalid_argument("native book lookup side is invalid");
                }
                return native_book_lookup_dict(
                    scheduler.lookup(is_bid, price_tick)
                );
            },
            py::arg("side"),
            py::arg("price_tick")
        )
        .def("top", [](const NativeExchangeBookSchedulerCpp& scheduler) {
            return native_book_top_dict(scheduler.top());
        })
        .def("stats", [](const NativeExchangeBookSchedulerCpp& scheduler) {
            return native_book_stats_dict(scheduler.stats());
        })
        .def_property_readonly(
            "initialized", &NativeExchangeBookSchedulerCpp::initialized
        )
        .def_property_readonly(
            "segment_id", &NativeExchangeBookSchedulerCpp::segment_id
        )
        .def_property_readonly(
            "last_exchange_ts_ns",
            &NativeExchangeBookSchedulerCpp::last_exchange_ts_ns
        )
        .def_property_readonly(
            "last_receive_ts_ns",
            &NativeExchangeBookSchedulerCpp::last_receive_ts_ns
        );

    py::class_<DynamicFillHazardRuntimeCpp>(m, "DynamicFillHazardRuntime")
        .def(py::init([](const py::dict& model_payload, const py::dict& config) {
            return std::make_unique<DynamicFillHazardRuntimeCpp>(
                dynamic_fill_hazard_model(model_payload),
                dynamic_fill_hazard_config(config)
            );
        }))
        .def(
            "apply_book_message",
            [](
                DynamicFillHazardRuntimeCpp& runtime,
                const std::string& event_type,
                std::int64_t exchange_ts_ns,
                std::int64_t provider_receive_ts_ns,
                std::int64_t feature_ready_ts_ns,
                std::int64_t event_time_ms,
                std::int64_t transaction_time_ms,
                py::object first_update_id,
                py::object final_update_id,
                py::object previous_final_update_id,
                py::object last_update_id,
                py::handle levels,
                bool execution_trade_same_ms
            ) {
                return native_book_apply_result_dict(runtime.apply_book_message(
                    native_book_event_type(event_type),
                    exchange_ts_ns,
                    provider_receive_ts_ns,
                    feature_ready_ts_ns,
                    event_time_ms,
                    transaction_time_ms,
                    optional_update_id(first_update_id),
                    optional_update_id(final_update_id),
                    optional_update_id(previous_final_update_id),
                    optional_update_id(last_update_id),
                    native_book_levels(levels),
                    execution_trade_same_ms
                ));
            },
            py::arg("event_type"),
            py::arg("exchange_ts_ns"),
            py::arg("receive_ts_ns") = 0,
            py::arg("feature_ready_ts_ns") = 0,
            py::arg("event_time_ms") = 0,
            py::arg("transaction_time_ms") = 0,
            py::arg("first_update_id") = py::none(),
            py::arg("final_update_id") = py::none(),
            py::arg("previous_final_update_id") = py::none(),
            py::arg("last_update_id") = py::none(),
            py::arg("levels") = py::tuple(),
            py::arg("execution_trade_same_ms") = false
        )
        .def(
            "activate_order",
            [](DynamicFillHazardRuntimeCpp& runtime,
               const std::string& client_order_id,
               double order_price,
               std::int64_t activation_ts_ns) {
                return native_book_lookup_dict(runtime.activate_order(
                    client_order_id, order_price, activation_ts_ns
                ));
            },
            py::arg("client_order_id"),
            py::arg("order_price"),
            py::arg("activation_ts_ns")
        )
        .def(
            "invalidate_order",
            &DynamicFillHazardRuntimeCpp::invalidate_order,
            py::arg("client_order_id"),
            py::arg("reason")
        )
        .def(
            "drop_inactive",
            &DynamicFillHazardRuntimeCpp::drop_inactive,
            py::arg("active_client_order_ids")
        )
        .def(
            "observe_trade",
            &DynamicFillHazardRuntimeCpp::observe_trade,
            py::arg("is_sell_trade"),
            py::arg("trade_price"),
            py::arg("quantity"),
            py::arg("provider_receive_ts_ns"),
            py::arg("feature_ready_ts_ns") = 0
        )
        .def(
            "evaluate",
            [](DynamicFillHazardRuntimeCpp& runtime,
               const std::string& client_order_id,
               double inventory,
               std::int64_t now_ns) {
                return dynamic_fill_hazard_evaluation_dict(
                    runtime.evaluate(client_order_id, inventory, now_ns)
                );
            },
            py::arg("client_order_id"),
            py::arg("inventory"),
            py::arg("now_ns")
        )
        .def(
            "evaluate_prospective_cancel_reentry",
            [](DynamicFillHazardRuntimeCpp& runtime,
               double candidate_price,
               double inventory,
               std::int64_t now_ns) {
                return dynamic_fill_hazard_evaluation_dict(
                    runtime.evaluate_prospective_cancel_reentry(
                        candidate_price,
                        inventory,
                        now_ns
                    )
                );
            },
            py::arg("candidate_price"),
            py::arg("inventory"),
            py::arg("now_ns")
        )
        .def(
            "on_fill",
            &DynamicFillHazardRuntimeCpp::on_fill,
            py::arg("client_order_id"),
            py::arg("remaining_after"),
            py::arg("now_ns")
        )
        .def(
            "on_cancel_ack",
            [](DynamicFillHazardRuntimeCpp& runtime,
               const std::string& client_order_id,
               std::int64_t now_ns,
               double remaining_after) {
                return runtime.on_cancel_ack(
                    client_order_id,
                    now_ns,
                    remaining_after
                );
            },
            py::arg("client_order_id"),
            py::arg("now_ns"),
            py::arg("remaining_after") =
                std::numeric_limits<double>::quiet_NaN()
        )
        .def(
            "on_order_terminal",
            [](DynamicFillHazardRuntimeCpp& runtime,
               const std::string& client_order_id,
               std::int64_t now_ns,
               const std::string& reason,
               double remaining_after) {
                return runtime.on_order_terminal(
                    client_order_id,
                    now_ns,
                    reason,
                    remaining_after
                );
            },
            py::arg("client_order_id"),
            py::arg("now_ns"),
            py::arg("reason") = "unsupported",
            py::arg("remaining_after") =
                std::numeric_limits<double>::quiet_NaN()
        )
        .def("top", [](const DynamicFillHazardRuntimeCpp& runtime) {
            return native_book_top_dict(runtime.top());
        })
        .def(
            "lookup",
            [](const DynamicFillHazardRuntimeCpp& runtime,
               const std::string& side,
               std::int64_t price_tick) {
                const bool is_bid = side == "bid" || side == "BUY" ||
                    side == "buy";
                if (!is_bid && side != "ask" && side != "SELL" &&
                    side != "sell") {
                    throw std::invalid_argument("native book lookup side is invalid");
                }
                return native_book_lookup_dict(runtime.lookup(is_bid, price_tick));
            },
            py::arg("side"),
            py::arg("price_tick")
        )
        .def("sequence_stats", [](const DynamicFillHazardRuntimeCpp& runtime) {
            return native_book_stats_dict(runtime.sequence_stats());
        })
        .def("counters", [](const DynamicFillHazardRuntimeCpp& runtime) {
            return dynamic_fill_hazard_counters_dict(runtime.counters());
        })
        .def_property_readonly(
            "hold_active", &DynamicFillHazardRuntimeCpp::hold_active
        )
        .def_property_readonly(
            "hold_order_id", &DynamicFillHazardRuntimeCpp::hold_order_id
        )
        .def_property_readonly(
            "hold_phase", &DynamicFillHazardRuntimeCpp::hold_phase
        )
        .def_property_readonly(
            "tracked_path_count",
            &DynamicFillHazardRuntimeCpp::tracked_path_count
        )
        .def_property_readonly(
            "evaluation_state_count",
            &DynamicFillHazardRuntimeCpp::evaluation_state_count
        )
        .def(
            "has_tracked_path",
            &DynamicFillHazardRuntimeCpp::has_tracked_path,
            py::arg("client_order_id")
        );
}
#endif


}  // namespace narrowgate_cpp
