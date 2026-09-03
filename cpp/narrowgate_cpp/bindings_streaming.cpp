#define NARROWGATE_BINDING_STREAMING 1

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
#include "causal_v12_1s_features.hpp"
#endif
#include "lightgbm_bundle.hpp"
#include "global_flow.hpp"
#include "streaming_features.hpp"
#include "binding_registry.hpp"

namespace py = pybind11;

namespace narrowgate_cpp {
namespace {


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

py::array_t<double> signal_ref_perp_feature_array(
    const SignalRefPerpFeatureValues& values
) {
    py::array_t<double> out(values.size());
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

#if defined(NARROWGATE_BINDING_COMMON_DYNAMIC) || \
    defined(NARROWGATE_BINDING_TICK_REPLAY)
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

void bind_global_flow(py::module_& m) {
    py::class_<NativeGlobalFlowEngine>(m, "NativeGlobalFlowEngine")
        .def(
            py::init<int, double, double>(),
            py::arg("retention_ms") = 2000,
            py::arg("max_source_age_ms") = 1000.0,
            py::arg("max_trade_event_age_ms") = 1000.0
        )
        .def(
            "clear",
            &NativeGlobalFlowEngine::clear,
            py::call_guard<py::gil_scoped_release>()
        )
        .def(
            "on_book",
            &NativeGlobalFlowEngine::on_book,
            py::arg("market_id"),
            py::arg("receive_ts_ns"),
            py::arg("bid"),
            py::arg("bid_size"),
            py::arg("ask"),
            py::arg("ask_size"),
            py::arg("gap_flag") = -1,
            py::call_guard<py::gil_scoped_release>()
        )
        .def(
            "on_trade",
            &NativeGlobalFlowEngine::on_trade,
            py::arg("market_id"),
            py::arg("receive_ts_ns"),
            py::arg("exchange_ts_ns"),
            py::arg("price"),
            py::arg("size"),
            py::arg("is_buyer_maker"),
            py::call_guard<py::gil_scoped_release>()
        )
        .def(
            "on_trade_batch",
            [](
                NativeGlobalFlowEngine& engine,
                const std::string& market_id,
                std::int64_t receive_ts_ns,
                CArray<std::int64_t> exchange_ts_ns,
                CArray<double> prices,
                CArray<double> sizes,
                CArray<std::uint8_t> is_buyer_maker
            ) {
                std::size_t accepted = 0;
                {
                    py::gil_scoped_release release;
                    accepted = engine.on_trade_batch(
                        market_id,
                        receive_ts_ns,
                        view_from_array(exchange_ts_ns),
                        view_from_array(prices),
                        view_from_array(sizes),
                        view_from_array(is_buyer_maker)
                    );
                }
                return accepted;
            },
            py::arg("market_id"),
            py::arg("receive_ts_ns"),
            py::arg("exchange_ts_ns"),
            py::arg("prices"),
            py::arg("sizes"),
            py::arg("is_buyer_maker")
        )
        .def(
            "market_window",
            [](
                const NativeGlobalFlowEngine& engine,
                const std::string& market_id,
                std::int64_t now_ns,
                int horizon_ms
            ) {
                GlobalFlowMarketWindow row;
                {
                    py::gil_scoped_release release;
                    row = engine.market_window(market_id, now_ns, horizon_ms);
                }
                return global_flow_window_dict(row);
            },
            py::arg("market_id"),
            py::arg("now_ns"),
            py::arg("horizon_ms")
        )
        .def(
            "stats",
            [](const NativeGlobalFlowEngine& engine) {
                GlobalFlowStats stats;
                {
                    py::gil_scoped_release release;
                    stats = engine.stats();
                }
                return global_flow_stats_dict(stats);
            }
        )
        .def_property_readonly_static(
            "max_markets",
            [](py::object) { return NativeGlobalFlowEngine::max_markets(); }
        )
        .def_property_readonly_static(
            "trade_capacity_per_market",
            [](py::object) {
                return NativeGlobalFlowEngine::trade_capacity_per_market();
            }
        )
        .def_property_readonly_static(
            "book_capacity_per_market",
            [](py::object) {
                return NativeGlobalFlowEngine::book_capacity_per_market();
            }
        );
}

void bind_streaming_features(py::module_& m) {
    m.attr("SIGNAL_FEATURE_ABI_VERSION") = "signal_feature_cutoff.v1";
    m.attr("NATIVE_LIGHTGBM_BUNDLE_INFERENCE_AVAILABLE") = true;
    py::tuple lightgbm_head_names(kLightgbmBundleHeadNames.size());
    for (std::size_t i = 0; i < kLightgbmBundleHeadNames.size(); ++i) {
        const auto name = kLightgbmBundleHeadNames[i];
        lightgbm_head_names[i] = py::str(name.data(), name.size());
    }
    m.attr("LIGHTGBM_BUNDLE_HEAD_NAMES") = std::move(lightgbm_head_names);
    py::class_<LightgbmBundleInference>(m, "NativeLightgbmBundle")
        .def(
            py::init<std::string, const std::vector<std::string>&, std::size_t>(),
            py::arg("library_path"),
            py::arg("model_paths"),
            py::arg("feature_count")
        )
        .def(
            "predict",
            [](const LightgbmBundleInference& bundle, CArray<double> row) {
                if (
                    row.ndim() != 1 &&
                    !(row.ndim() == 2 && row.shape(0) == 1)
                ) {
                    throw std::invalid_argument(
                        "LightGBM input must be one C-contiguous row"
                    );
                }
                py::array_t<double> output(kLightgbmBundleHeadNames.size());
                double* output_data = output.mutable_data();
                {
                    py::gil_scoped_release release;
                    bundle.predict(
                        std::span<const double>(
                            row.data(),
                            static_cast<std::size_t>(row.size())
                        ),
                        std::span<double>(
                            output_data,
                            kLightgbmBundleHeadNames.size()
                        )
                    );
                }
                return output;
            },
            py::arg("row")
        )
        .def_property_readonly(
            "feature_count",
            &LightgbmBundleInference::feature_count
        )
        .def_property_readonly(
            "library_path",
            &LightgbmBundleInference::library_path
        )
        .def_property_readonly_static(
            "head_count",
            [](py::object) { return kLightgbmBundleHeadNames.size(); }
        )
        .def_property_readonly_static(
            "num_threads",
            [](py::object) { return 1; }
        );
    py::tuple feature_names(kSignalFeatureCount);
    for (std::size_t i = 0; i < kSignalFeatureCount; ++i) {
        feature_names[i] = py::str(kSignalFeatureNames[i].data(), kSignalFeatureNames[i].size());
    }
    m.attr("SIGNAL_FEATURE_NAMES") = std::move(feature_names);
    m.attr("SIGNAL_EXECUTION_L2_FEATURE_ABI_VERSION") =
        "signal_execution_l2_window.v1";
    py::tuple execution_l2_feature_names(kSignalExecutionL2FeatureNames.size());
    for (std::size_t i = 0; i < kSignalExecutionL2FeatureNames.size(); ++i) {
        execution_l2_feature_names[i] = py::str(
            kSignalExecutionL2FeatureNames[i].data(),
            kSignalExecutionL2FeatureNames[i].size()
        );
    }
    m.attr("SIGNAL_EXECUTION_L2_FEATURE_NAMES") =
        std::move(execution_l2_feature_names);
    m.attr("SIGNAL_EXECUTION_L2_POLICY_METRIC_ABI_VERSION") =
        "signal_execution_l2_policy_window.v1";
    py::tuple execution_l2_policy_metric_names(
        kSignalExecutionL2PolicyMetricNames.size()
    );
    for (std::size_t i = 0; i < kSignalExecutionL2PolicyMetricNames.size(); ++i) {
        execution_l2_policy_metric_names[i] = py::str(
            kSignalExecutionL2PolicyMetricNames[i].data(),
            kSignalExecutionL2PolicyMetricNames[i].size()
        );
    }
    m.attr("SIGNAL_EXECUTION_L2_POLICY_METRIC_NAMES") =
        std::move(execution_l2_policy_metric_names);
    m.attr("SIGNAL_REF_PERP_FEATURE_ABI_VERSION") =
        "signal_ref_perp_incremental.v1";
    py::tuple ref_perp_feature_names(kSignalRefPerpFeatureNames.size());
    for (std::size_t i = 0; i < kSignalRefPerpFeatureNames.size(); ++i) {
        ref_perp_feature_names[i] = py::str(
            kSignalRefPerpFeatureNames[i].data(),
            kSignalRefPerpFeatureNames[i].size()
        );
    }
    m.attr("SIGNAL_REF_PERP_FEATURE_NAMES") =
        std::move(ref_perp_feature_names);

    py::class_<Bar1s>(m, "Bar1s")
        .def(py::init<>())
        .def_readwrite("ts_ms", &Bar1s::ts_ms)
        .def_readwrite("open", &Bar1s::open)
        .def_readwrite("high", &Bar1s::high)
        .def_readwrite("low", &Bar1s::low)
        .def_readwrite("close", &Bar1s::close)
        .def_readwrite("volume", &Bar1s::volume)
        .def_readwrite("buy_volume", &Bar1s::buy_volume)
        .def_readwrite("sell_volume", &Bar1s::sell_volume)
        .def_readwrite("trade_count", &Bar1s::trade_count)
        .def_readwrite("buy_count", &Bar1s::buy_count)
        .def_readwrite("sell_count", &Bar1s::sell_count)
        .def_readwrite("quote_qty", &Bar1s::quote_qty)
        .def_readwrite("buy_quote_qty", &Bar1s::buy_quote_qty)
        .def_readwrite("sell_quote_qty", &Bar1s::sell_quote_qty)
        .def_readwrite("max_same_side_run", &Bar1s::max_same_side_run)
        .def_readwrite("max_buy_run", &Bar1s::max_buy_run)
        .def_readwrite("max_sell_run", &Bar1s::max_sell_run)
        .def_readwrite("buy_price_high", &Bar1s::buy_price_high)
        .def_readwrite("buy_price_low", &Bar1s::buy_price_low)
        .def_readwrite("sell_price_high", &Bar1s::sell_price_high)
        .def_readwrite("sell_price_low", &Bar1s::sell_price_low);

    py::class_<FeatureHistoryRow>(m, "FeatureHistoryRow")
        .def(py::init<>())
        .def_readwrite("close", &FeatureHistoryRow::close)
        .def_readwrite("volume", &FeatureHistoryRow::volume)
        .def_readwrite("buy_volume", &FeatureHistoryRow::buy_volume)
        .def_readwrite("sell_volume", &FeatureHistoryRow::sell_volume)
        .def_readwrite("trade_count", &FeatureHistoryRow::trade_count)
        .def_readwrite("flow_velocity", &FeatureHistoryRow::flow_velocity)
        .def_readwrite("avg_trade_size", &FeatureHistoryRow::avg_trade_size)
        .def_readwrite("price_velocity", &FeatureHistoryRow::price_velocity)
        .def_readwrite("return_abs", &FeatureHistoryRow::return_abs)
        .def_readwrite("vol_regime_6h", &FeatureHistoryRow::vol_regime_6h);

    py::class_<TradeBarAggregator>(m, "TradeBarAggregator")
        .def(py::init<bool>(), py::arg("track_runs") = true)
        .def("reset", &TradeBarAggregator::reset)
        .def("update", &TradeBarAggregator::update)
        .def(
            "update_batch",
            [](
                TradeBarAggregator& aggregator,
                CArray<std::int64_t> ts_ms,
                CArray<double> prices,
                CArray<double> quantities,
                CArray<std::uint8_t> is_buyer_maker
            ) {
                std::vector<Bar1s> completed;
                {
                    py::gil_scoped_release release;
                    completed = aggregator.update_batch(
                        view_from_array(ts_ms),
                        view_from_array(prices),
                        view_from_array(quantities),
                        view_from_array(is_buyer_maker)
                    );
                }
                return completed;
            },
            py::arg("ts_ms"),
            py::arg("prices"),
            py::arg("quantities"),
            py::arg("is_buyer_maker")
        )
        .def("current_bar", &TradeBarAggregator::current_bar)
        .def("current_bucket_ms", &TradeBarAggregator::current_bucket_ms);

    py::class_<SignalRefPerpPrepared>(m, "SignalRefPerpPrepared")
        .def_property_readonly(
            "values",
            [](const SignalRefPerpPrepared& prepared) {
                return signal_ref_perp_feature_array(prepared.values);
            }
        )
        .def_readonly("target_bucket", &SignalRefPerpPrepared::target_bucket)
        .def_readonly("basis_bps", &SignalRefPerpPrepared::basis_bps)
        .def_readonly("revision", &SignalRefPerpPrepared::revision)
        .def_readonly("basis_available", &SignalRefPerpPrepared::basis_available);

    py::class_<SignalRefPerpFeatureEngine>(m, "SignalRefPerpFeatureEngine")
        .def(
            py::init<std::size_t, std::size_t, std::size_t, std::size_t, double>(),
            py::arg("max_bars") = 3700,
            py::arg("max_book_tickers") = 3600,
            py::arg("max_basis") = 360,
            py::arg("basis_min_periods") = 30,
            py::arg("source_max_age_ms") = 30000.0
        )
        .def("reset", &SignalRefPerpFeatureEngine::reset)
        .def(
            "update_trade_batch",
            [](
                SignalRefPerpFeatureEngine& engine,
                CArray<std::int64_t> ts_ms,
                CArray<double> prices,
                CArray<double> quantities,
                CArray<std::uint8_t> is_buyer_maker
            ) {
                py::gil_scoped_release release;
                engine.update_trade_batch(
                    view_from_array(ts_ms),
                    view_from_array(prices),
                    view_from_array(quantities),
                    view_from_array(is_buyer_maker)
                );
            },
            py::arg("ts_ms"),
            py::arg("prices"),
            py::arg("quantities"),
            py::arg("is_buyer_maker")
        )
        .def(
            "update_book_ticker",
            &SignalRefPerpFeatureEngine::update_book_ticker,
            py::arg("event_ts_ms"),
            py::arg("receive_ts_ms"),
            py::arg("bid"),
            py::arg("ask")
        )
        .def(
            "prepare",
            &SignalRefPerpFeatureEngine::prepare,
            py::arg("target_ts_ms"),
            py::arg("target_close")
        )
        .def("commit", &SignalRefPerpFeatureEngine::commit)
        .def("bar_count", &SignalRefPerpFeatureEngine::bar_count)
        .def("book_ticker_count", &SignalRefPerpFeatureEngine::book_ticker_count)
        .def("basis_count", &SignalRefPerpFeatureEngine::basis_count);

    py::class_<SignalExecutionL2Engine>(m, "SignalExecutionL2Engine")
        .def(py::init<std::size_t>(), py::arg("max_snapshots") = 300)
        .def("reset", &SignalExecutionL2Engine::reset)
        .def(
            "push_snapshot",
            [](SignalExecutionL2Engine& engine, double ts_ms,
               py::handle bids, py::handle asks) {
                engine.push_snapshot(signal_execution_l2_snapshot_from_python(
                    ts_ms, bids, asks));
            },
            py::arg("ts_ms"),
            py::arg("bids"),
            py::arg("asks")
        )
        .def(
            "compute_feature_values",
            [](const SignalExecutionL2Engine& engine, double bucket_end_ms) {
                SignalExecutionL2FeatureValues values;
                {
                    py::gil_scoped_release release;
                    values = engine.compute_features(bucket_end_ms);
                }
                return signal_execution_l2_feature_array(values);
            },
            py::arg("bucket_end_ms")
        )
        .def(
            "compute_policy_metric_values",
            [](const SignalExecutionL2Engine& engine, double end_exchange_ms) {
                SignalExecutionL2PolicyMetricValues values;
                {
                    py::gil_scoped_release release;
                    values = engine.compute_policy_metrics(end_exchange_ms);
                }
                return signal_execution_l2_policy_metric_array(values);
            },
            py::arg("end_exchange_ms")
        )
        .def("snapshot_count", &SignalExecutionL2Engine::snapshot_count);

    py::class_<SignalFeatureEngine>(m, "SignalFeatureEngine")
        .def(
            py::init<std::size_t, std::size_t>(),
            py::arg("max_bars") = 320,
            py::arg("max_history") = 60480
        )
        .def("reset", &SignalFeatureEngine::reset)
        .def("push_bar", &SignalFeatureEngine::push_bar)
        .def("push_history", &SignalFeatureEngine::push_history)
        .def(
            "compute",
            [](const SignalFeatureEngine& engine, const Bar1s& bar_10s) {
                return signal_feature_dict(engine.compute(bar_10s));
            },
            py::arg("bar_10s")
        )
        .def(
            "compute_values",
            [](const SignalFeatureEngine& engine, const Bar1s& bar_10s) {
                return signal_feature_array(engine.compute(bar_10s));
            },
            py::arg("bar_10s")
        )
        .def(
            "compute_at_cutoff",
            [](const SignalFeatureEngine& engine, const Bar1s& bar_10s,
               std::int64_t cutoff_exclusive_ms) {
                return signal_feature_dict(
                    engine.compute_at_cutoff(bar_10s, cutoff_exclusive_ms));
            },
            py::arg("bar_10s"),
            py::arg("cutoff_exclusive_ms")
        )
        .def(
            "compute_values_at_cutoff",
            [](const SignalFeatureEngine& engine, const Bar1s& bar_10s,
               std::int64_t cutoff_exclusive_ms) {
                return signal_feature_array(
                    engine.compute_at_cutoff(bar_10s, cutoff_exclusive_ms));
            },
            py::arg("bar_10s"),
            py::arg("cutoff_exclusive_ms")
        )
        .def(
            "compute_bucket_values",
            [](const SignalFeatureEngine& engine, std::int64_t bucket_start_ms) {
                std::pair<Bar1s, SignalFeatureVector> result;
                {
                    py::gil_scoped_release release;
                    result = engine.compute_bucket(bucket_start_ms);
                }
                return py::make_tuple(
                    result.first,
                    signal_feature_array(result.second)
                );
            },
            py::arg("bucket_start_ms")
        )
        .def("bar_count", &SignalFeatureEngine::bar_count)
        .def("history_count", &SignalFeatureEngine::history_count);

    m.def(
        "compute_signal_feature_overlay",
        &compute_signal_feature_overlay,
        py::arg("all_bars"),
        py::arg("feature_history"),
        py::arg("bar_10s")
    );

    m.def(
        "compute_signal_execution_l2_feature_values",
        [](py::iterable snapshots, double bucket_end_ms) {
            const auto native_snapshots = signal_execution_l2_from_python(snapshots);
            SignalExecutionL2FeatureValues values;
            {
                py::gil_scoped_release release;
                values = compute_signal_execution_l2_features(
                    native_snapshots,
                    bucket_end_ms
                );
            }
            return signal_execution_l2_feature_array(values);
        },
        py::arg("snapshots"),
        py::arg("bucket_end_ms")
    );

    m.def(
        "compute_signal_execution_l2_policy_metric_values",
        [](py::iterable snapshots, double end_exchange_ms) {
            const auto native_snapshots = signal_execution_l2_from_python(snapshots);
            SignalExecutionL2PolicyMetricValues values;
            {
                py::gil_scoped_release release;
                values = compute_signal_execution_l2_policy_metrics(
                    native_snapshots,
                    end_exchange_ms
                );
            }
            return signal_execution_l2_policy_metric_array(values);
        },
        py::arg("snapshots"),
        py::arg("end_exchange_ms")
    );

}

#if NARROWGATE_BUILD_HAS_RESEARCH_RUNTIME
void bind_f03_causal_v12_one_second_features(py::module_& m) {
    m.attr("F03_CAUSAL_V12_1S_FEATURE_ABI_VERSION") =
        std::string(f03::kCausalV12OneSecondFeatureAbiVersion);
    m.attr("F03_CAUSAL_V12_1S_FEATURE_ORDER_SHA256") =
        std::string(f03::kCausalV12OneSecondFeatureOrderSha256);
    m.attr("F03_CAUSAL_V12_1S_BATCH_ABI_VERSION") =
        std::string(f03::kCausalV12OneSecondBatchAbiVersion);
    py::tuple feature_names(f03::kCausalV12OneSecondFeatureCount);
    for (std::size_t index = 0; index < f03::kCausalV12OneSecondFeatureCount; ++index) {
        const auto name = f03::kCausalV12OneSecondFeatureNames[index];
        feature_names[index] = py::str(name.data(), name.size());
    }
    m.attr("F03_CAUSAL_V12_1S_FEATURE_NAMES") = std::move(feature_names);
    py::tuple lag_states(f03::kFeatureLagStateVocabulary.size());
    for (std::size_t index = 0; index < f03::kFeatureLagStateVocabulary.size(); ++index) {
        const auto value = f03::kFeatureLagStateVocabulary[index];
        lag_states[index] = py::str(value.data(), value.size());
    }
    m.attr("F03_CAUSAL_V12_1S_LAG_STATE_VOCABULARY") = std::move(lag_states);

    py::class_<f03::OneSecondBar>(m, "F03CausalV12OneSecondBar")
        .def(py::init<>())
        .def_readwrite("start_ts_ms", &f03::OneSecondBar::start_ts_ms)
        .def_readwrite("finalized_ts_ms", &f03::OneSecondBar::finalized_ts_ms)
        .def_readwrite("open", &f03::OneSecondBar::open)
        .def_readwrite("high", &f03::OneSecondBar::high)
        .def_readwrite("low", &f03::OneSecondBar::low)
        .def_readwrite("close", &f03::OneSecondBar::close)
        .def_readwrite("volume", &f03::OneSecondBar::volume)
        .def_readwrite("buy_volume", &f03::OneSecondBar::buy_volume)
        .def_readwrite("sell_volume", &f03::OneSecondBar::sell_volume)
        .def_readwrite("trade_count", &f03::OneSecondBar::trade_count)
        .def_readwrite("buy_count", &f03::OneSecondBar::buy_count)
        .def_readwrite("sell_count", &f03::OneSecondBar::sell_count)
        .def_readwrite("buy_quote_qty", &f03::OneSecondBar::buy_quote_qty)
        .def_readwrite("sell_quote_qty", &f03::OneSecondBar::sell_quote_qty)
        .def_readwrite("max_same_side_run", &f03::OneSecondBar::max_same_side_run)
        .def_readwrite("buy_price_high", &f03::OneSecondBar::buy_price_high)
        .def_readwrite("buy_price_low", &f03::OneSecondBar::buy_price_low)
        .def_readwrite("sell_price_high", &f03::OneSecondBar::sell_price_high)
        .def_readwrite("sell_price_low", &f03::OneSecondBar::sell_price_low);

    py::class_<f03::ExecutionL2Observation>(m, "F03CausalV12ExecutionL2Observation")
        .def(py::init<>())
        .def_readwrite("bucket_start_ts_ms", &f03::ExecutionL2Observation::bucket_start_ts_ms)
        .def_readwrite("feature_ready_ts_ms", &f03::ExecutionL2Observation::feature_ready_ts_ms)
        .def_readwrite("values", &f03::ExecutionL2Observation::values);

    py::class_<f03::MetricObservation>(m, "F03CausalV12MetricObservation")
        .def(py::init<>())
        .def_readwrite("source_ts_ms", &f03::MetricObservation::source_ts_ms)
        .def_readwrite("feature_ready_ts_ms", &f03::MetricObservation::feature_ready_ts_ms)
        .def_readwrite("sum_open_interest", &f03::MetricObservation::sum_open_interest)
        .def_readwrite("toptrader_ls_ratio", &f03::MetricObservation::toptrader_ls_ratio)
        .def_readwrite("crowd_ls_ratio", &f03::MetricObservation::crowd_ls_ratio)
        .def_readwrite("taker_ls_ratio", &f03::MetricObservation::taker_ls_ratio);

    auto bars_from_arrays = [](
        const CArray<std::int64_t>& integers,
        const CArray<double>& floating,
        const char* name
    ) {
        const auto integer_view = matrix_from_array(integers, name);
        const auto floating_view = matrix_from_array(floating, name);
        if (integer_view.cols != 6 || floating_view.cols != 13 ||
            integer_view.rows != floating_view.rows) {
            throw std::invalid_argument(std::string(name) +
                " requires int64[N,6] and float64[N,13]");
        }
        std::vector<f03::OneSecondBar> output(integer_view.rows);
        for (std::size_t row = 0; row < integer_view.rows; ++row) {
            auto& item = output[row];
            item.start_ts_ms = integer_view(row, 0);
            item.finalized_ts_ms = integer_view(row, 1);
            item.trade_count = integer_view(row, 2);
            item.buy_count = integer_view(row, 3);
            item.sell_count = integer_view(row, 4);
            item.max_same_side_run = integer_view(row, 5);
            item.open = floating_view(row, 0);
            item.high = floating_view(row, 1);
            item.low = floating_view(row, 2);
            item.close = floating_view(row, 3);
            item.volume = floating_view(row, 4);
            item.buy_volume = floating_view(row, 5);
            item.sell_volume = floating_view(row, 6);
            item.buy_quote_qty = floating_view(row, 7);
            item.sell_quote_qty = floating_view(row, 8);
            item.buy_price_high = floating_view(row, 9);
            item.buy_price_low = floating_view(row, 10);
            item.sell_price_high = floating_view(row, 11);
            item.sell_price_low = floating_view(row, 12);
        }
        return output;
    };

    py::class_<f03::CausalV12OneSecondBatchEngine>(
        m, "F03CausalV12OneSecondBatchEngine")
        .def(
            py::init([bars_from_arrays](
                const CArray<std::int64_t>& local_integers,
                const CArray<double>& local_floating,
                const CArray<std::int64_t>& l2_clocks,
                const CArray<double>& l2_values,
                const CArray<std::int64_t>& metric_clocks,
                const CArray<double>& metric_values,
                const CArray<std::int64_t>& reference_integers,
                const CArray<double>& reference_floating
            ) {
                auto local = bars_from_arrays(
                    local_integers, local_floating, "local_bars");
                auto reference = bars_from_arrays(
                    reference_integers, reference_floating, "reference_bars");
                const auto l2_clock_view = matrix_from_array(l2_clocks, "execution_l2_clocks");
                const auto l2_value_view = matrix_from_array(l2_values, "execution_l2_values");
                if (l2_clock_view.cols != 2 ||
                    l2_value_view.cols != f03::kExecutionL2FeatureCount ||
                    l2_clock_view.rows != l2_value_view.rows) {
                    throw std::invalid_argument(
                        "execution L2 requires int64[N,2] and float64[N,13]");
                }
                std::vector<f03::ExecutionL2Observation> l2(l2_clock_view.rows);
                for (std::size_t row = 0; row < l2_clock_view.rows; ++row) {
                    l2[row].bucket_start_ts_ms = l2_clock_view(row, 0);
                    l2[row].feature_ready_ts_ms = l2_clock_view(row, 1);
                    for (std::size_t column = 0;
                         column < f03::kExecutionL2FeatureCount; ++column) {
                        l2[row].values[column] = l2_value_view(row, column);
                    }
                }
                const auto metric_clock_view = matrix_from_array(
                    metric_clocks, "metric_clocks");
                const auto metric_value_view = matrix_from_array(
                    metric_values, "metric_values");
                if (metric_clock_view.cols != 2 || metric_value_view.cols != 4 ||
                    metric_clock_view.rows != metric_value_view.rows) {
                    throw std::invalid_argument(
                        "metrics require int64[N,2] and float64[N,4]");
                }
                std::vector<f03::MetricObservation> metrics(metric_clock_view.rows);
                for (std::size_t row = 0; row < metric_clock_view.rows; ++row) {
                    metrics[row].source_ts_ms = metric_clock_view(row, 0);
                    metrics[row].feature_ready_ts_ms = metric_clock_view(row, 1);
                    metrics[row].sum_open_interest = metric_value_view(row, 0);
                    metrics[row].toptrader_ls_ratio = metric_value_view(row, 1);
                    metrics[row].crowd_ls_ratio = metric_value_view(row, 2);
                    metrics[row].taker_ls_ratio = metric_value_view(row, 3);
                }
                py::gil_scoped_release release;
                return std::make_unique<f03::CausalV12OneSecondBatchEngine>(
                    std::move(local), std::move(l2), std::move(metrics),
                    std::move(reference));
            }),
            py::arg("local_integers"),
            py::arg("local_floating"),
            py::arg("l2_clocks"),
            py::arg("l2_values"),
            py::arg("metric_clocks"),
            py::arg("metric_values"),
            py::arg("reference_integers"),
            py::arg("reference_floating")
        )
        .def(
            "compute",
            [](const f03::CausalV12OneSecondBatchEngine& engine,
               const CArray<std::int64_t>& cutoffs,
               py::object decisions_object) {
                if (cutoffs.ndim() != 1) {
                    throw std::invalid_argument("cutoffs must be a one-dimensional int64 array");
                }
                std::vector<std::int64_t> cutoff_values(
                    cutoffs.data(), cutoffs.data() + cutoffs.size());
                std::vector<std::int64_t> decisions;
                if (!decisions_object.is_none()) {
                    const auto decision_array = py::cast<CArray<std::int64_t>>(
                        decisions_object);
                    if (decision_array.ndim() != 1) {
                        throw std::invalid_argument(
                            "decision timestamps must be a one-dimensional int64 array");
                    }
                    decisions.assign(
                        decision_array.data(), decision_array.data() + decision_array.size());
                }
                f03::FeatureBatch batch;
                {
                    py::gil_scoped_release release;
                    batch = engine.compute(cutoff_values, decisions);
                }
                py::dict output;
                output["cutoff_exclusive_ms"] = vector_array(batch.cutoff_exclusive_ms);
                output["decision_ts_ms"] = vector_array(batch.decision_ts_ms);
                output["feature_ready_ts_ms"] = vector_array(batch.feature_ready_ts_ms);
                output["values"] = vector_matrix(
                    batch.values, batch.row_count, f03::kCausalV12OneSecondFeatureCount);
                output["valid"] = vector_matrix(
                    batch.valid, batch.row_count, f03::kCausalV12OneSecondFeatureCount);
                output["source_latest_ts_ms"] = vector_matrix(
                    batch.source_latest_ts_ms, batch.row_count,
                    f03::kCausalV12OneSecondFeatureCount);
                output["feature_ready_ts_ms_by_feature"] = vector_matrix(
                    batch.feature_ready_ts_ms_by_feature, batch.row_count,
                    f03::kCausalV12OneSecondFeatureCount);
                output["observation_count"] = vector_matrix(
                    batch.observation_count, batch.row_count,
                    f03::kCausalV12OneSecondFeatureCount);
                output["lag_state_code"] = vector_matrix(
                    batch.lag_state_code, batch.row_count,
                    f03::kCausalV12OneSecondFeatureCount);
                output["feature_order_sha256"] =
                    std::string(f03::kCausalV12OneSecondFeatureOrderSha256);
                output["schema_version"] =
                    std::string(f03::kCausalV12OneSecondBatchAbiVersion);
                return output;
            },
            py::arg("cutoffs_exclusive_ms"),
            py::arg("decision_ts_ms") = py::none()
        )
        .def("local_bar_count", &f03::CausalV12OneSecondBatchEngine::local_bar_count)
        .def("reference_bar_count", &f03::CausalV12OneSecondBatchEngine::reference_bar_count);

    m.def(
        "compute_f03_causal_v12_1s_features",
        [](
            const std::vector<f03::OneSecondBar>& local_bars,
            std::int64_t cutoff_exclusive_ms,
            py::object decision_ts_ms,
            const std::vector<f03::ExecutionL2Observation>& execution_l2,
            const std::vector<f03::MetricObservation>& metrics,
            const std::vector<f03::OneSecondBar>& reference_bars
        ) {
            const std::int64_t decision = decision_ts_ms.is_none()
                ? cutoff_exclusive_ms
                : py::cast<std::int64_t>(decision_ts_ms);
            f03::FeatureRow row;
            {
                py::gil_scoped_release release;
                row = f03::compute_causal_v12_one_second_features(
                    local_bars,
                    cutoff_exclusive_ms,
                    decision,
                    execution_l2,
                    metrics,
                    reference_bars
                );
            }
            py::array_t<double> values(f03::kCausalV12OneSecondFeatureCount);
            py::array_t<std::uint8_t> valid(f03::kCausalV12OneSecondFeatureCount);
            py::array_t<std::int64_t> source_ts(f03::kCausalV12OneSecondFeatureCount);
            py::array_t<std::int64_t> ready_ts(f03::kCausalV12OneSecondFeatureCount);
            py::array_t<std::int64_t> counts(f03::kCausalV12OneSecondFeatureCount);
            py::list lag_states;
            for (std::size_t index = 0; index < row.values.size(); ++index) {
                const auto& item = row.values[index];
                values.mutable_at(index) = item.value.value_or(
                    std::numeric_limits<double>::quiet_NaN());
                valid.mutable_at(index) = item.value.has_value() ? 1 : 0;
                source_ts.mutable_at(index) = item.source_latest_ts_ms.value_or(-1);
                ready_ts.mutable_at(index) = item.feature_ready_ts_ms.value_or(-1);
                counts.mutable_at(index) = item.observation_count;
                lag_states.append(item.lag_state);
            }
            py::dict output;
            output["cutoff_exclusive_ms"] = row.cutoff_exclusive_ms;
            output["decision_ts_ms"] = row.decision_ts_ms;
            output["feature_ready_ts_ms"] = row.feature_ready_ts_ms;
            output["values"] = std::move(values);
            output["valid"] = std::move(valid);
            output["source_latest_ts_ms"] = std::move(source_ts);
            output["feature_ready_ts_ms_by_feature"] = std::move(ready_ts);
            output["observation_count"] = std::move(counts);
            output["lag_state"] = std::move(lag_states);
            output["feature_order_sha256"] =
                std::string(f03::kCausalV12OneSecondFeatureOrderSha256);
            output["schema_version"] = std::string(f03::kCausalV12OneSecondFeatureAbiVersion);
            return output;
        },
        py::arg("local_bars"),
        py::arg("cutoff_exclusive_ms"),
        py::arg("decision_ts_ms") = py::none(),
        py::arg("execution_l2") = std::vector<f03::ExecutionL2Observation>{},
        py::arg("metrics") = std::vector<f03::MetricObservation>{},
        py::arg("reference_bars") = std::vector<f03::OneSecondBar>{}
    );
}
#endif


}  // namespace narrowgate_cpp
