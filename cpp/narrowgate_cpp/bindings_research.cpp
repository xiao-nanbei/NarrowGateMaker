#define NARROWGATE_BINDING_RESEARCH 1

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

#include "active_order_competing_risk_cif.hpp"
#include "order_lifecycle_journal_v2_mirror.hpp"
#include "request_state_features.hpp"
#include "risk_set_expansion.hpp"
#include "sparse_order_lifecycle.hpp"
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

void bind_request_state_features(py::module_& m) {
    m.def(
        "compute_request_state_features",
        [](
            const CArray<std::int64_t>& request_ts_ms,
            const CArray<std::int64_t>& book_cutoff_ts_ms,
            const CArray<std::int64_t>& trade_cutoff_ts_ms,
            const CArray<std::int64_t>& activation_ts_ms,
            const CArray<std::int64_t>& terminal_ts_ms,
            const CArray<std::int64_t>& cancel_ack_ts_ms,
            const CArray<std::int64_t>& bbo_ts_ms,
            const CArray<double>& bbo_best_bid,
            const CArray<double>& bbo_best_ask,
            const CArray<double>& bbo_bid_qty,
            const CArray<double>& bbo_ask_qty,
            const CArray<std::int64_t>& l2_ts_ms,
            const CArray<double>& l2_bid_px,
            const CArray<double>& l2_bid_qty,
            const CArray<double>& l2_ask_px,
            const CArray<double>& l2_ask_qty,
            const CArray<std::int64_t>& trade_ts_ms,
            const CArray<double>& trade_price,
            const CArray<double>& trade_qty,
            const CArray<std::uint8_t>& is_buyer_maker,
            const CArray<std::int64_t>& windows_ms,
            double tick_size,
            std::int64_t depth_levels,
            std::int64_t l2_path_lookback_ms
        ) {
            RequestStateFeatureResult result;
            {
                py::gil_scoped_release release;
                result = compute_request_state_features(
                    view_from_array(request_ts_ms),
                    view_from_array(book_cutoff_ts_ms),
                    view_from_array(trade_cutoff_ts_ms),
                    view_from_array(activation_ts_ms),
                    view_from_array(terminal_ts_ms),
                    view_from_array(cancel_ack_ts_ms),
                    view_from_array(bbo_ts_ms),
                    view_from_array(bbo_best_bid),
                    view_from_array(bbo_best_ask),
                    view_from_array(bbo_bid_qty),
                    view_from_array(bbo_ask_qty),
                    view_from_array(l2_ts_ms),
                    matrix_from_array(l2_bid_px, "l2_bid_px"),
                    matrix_from_array(l2_bid_qty, "l2_bid_qty"),
                    matrix_from_array(l2_ask_px, "l2_ask_px"),
                    matrix_from_array(l2_ask_qty, "l2_ask_qty"),
                    view_from_array(trade_ts_ms),
                    view_from_array(trade_price),
                    view_from_array(trade_qty),
                    view_from_array(is_buyer_maker),
                    view_from_array(windows_ms),
                    tick_size,
                    depth_levels,
                    l2_path_lookback_ms
                );
            }
            py::dict out;
            out["schema_version"] = "request_state_features.v1";
            out["windows_ms"] = vector_array(
                std::vector<std::int64_t>(windows_ms.data(), windows_ms.data() + windows_ms.size())
            );
            out["valid_book"] = vector_array(result.valid_book);
            out["book_source_ts_ms"] = vector_array(result.book_source_ts_ms);
            out["book_age_ms"] = vector_array(result.book_age_ms);
            out["best_bid"] = vector_array(result.best_bid);
            out["best_ask"] = vector_array(result.best_ask);
            out["bid_qty"] = vector_array(result.bid_qty);
            out["ask_qty"] = vector_array(result.ask_qty);
            out["mid"] = vector_array(result.mid);
            out["bbo_spread_ticks"] = vector_array(result.bbo_spread_ticks);
            out["book_imbalance"] = vector_array(result.book_imbalance);
            out["microprice_shift_bps"] = vector_array(result.microprice_shift_bps);
            out["l2_near_depth_total"] = vector_array(result.l2_near_depth_total);
            out["l2_quote_flip_rate"] = vector_array(result.l2_quote_flip_rate);
            out["l2_book_refresh_ratio"] = vector_array(result.l2_book_refresh_ratio);
            out["l2_book_cancel_ratio"] = vector_array(result.l2_book_cancel_ratio);
            out["active_order_count"] = vector_array(result.active_order_count);
            out["pending_cancel_before_count"] = vector_array(
                result.pending_cancel_before_count
            );
            out["request_batch_size"] = vector_array(result.request_batch_size);
            out["market_return_bps"] = vector_matrix(
                result.market_return_bps, result.rows, result.windows
            );
            out["aggressive_buy_qty"] = vector_matrix(
                result.aggressive_buy_qty, result.rows, result.windows
            );
            out["aggressive_sell_qty"] = vector_matrix(
                result.aggressive_sell_qty, result.rows, result.windows
            );
            out["taker_imbalance"] = vector_matrix(
                result.taker_imbalance, result.rows, result.windows
            );
            out["trade_count"] = vector_matrix(
                result.trade_count, result.rows, result.windows
            );
            out["book_update_count"] = vector_matrix(
                result.book_update_count, result.rows, result.windows
            );
            return out;
        },
        py::arg("request_ts_ms"),
        py::arg("book_cutoff_ts_ms"),
        py::arg("trade_cutoff_ts_ms"),
        py::arg("activation_ts_ms"),
        py::arg("terminal_ts_ms"),
        py::arg("cancel_ack_ts_ms"),
        py::arg("bbo_ts_ms"),
        py::arg("bbo_best_bid"),
        py::arg("bbo_best_ask"),
        py::arg("bbo_bid_qty"),
        py::arg("bbo_ask_qty"),
        py::arg("l2_ts_ms"),
        py::arg("l2_bid_px"),
        py::arg("l2_bid_qty"),
        py::arg("l2_ask_px"),
        py::arg("l2_ask_qty"),
        py::arg("trade_ts_ms"),
        py::arg("trade_price"),
        py::arg("trade_qty"),
        py::arg("is_buyer_maker"),
        py::arg("windows_ms"),
        py::arg("tick_size") = 0.1,
        py::arg("depth_levels") = 5,
        py::arg("l2_path_lookback_ms") = 1000
    );
}

void bind_risk_set_expansion(py::module_& m) {
    m.def(
        "expand_competing_risk_intervals",
        [](
            const CArray<double>& duration_ms,
            const CArray<std::uint8_t>& event_kind,
            const CArray<double>& bin_edges_ms
        ) {
            RiskSetExpansionResult result;
            {
                py::gil_scoped_release release;
                result = expand_competing_risk_intervals(
                    view_from_array(duration_ms),
                    view_from_array(event_kind),
                    view_from_array(bin_edges_ms)
                );
            }
            py::dict out;
            out["schema_version"] = "competing_risk_intervals.v1";
            out["row_index"] = vector_array(result.row_index);
            out["bin_index"] = vector_array(result.bin_index);
            out["interval_start_ms"] = vector_array(result.interval_start_ms);
            out["interval_end_ms"] = vector_array(result.interval_end_ms);
            out["exposure_fraction"] = vector_array(result.exposure_fraction);
            out["fill_target"] = vector_array(result.fill_target);
            out["ack_target"] = vector_array(result.ack_target);
            return out;
        },
        py::arg("duration_ms"),
        py::arg("event_kind"),
        py::arg("bin_edges_ms")
    );
}

void bind_sparse_order_lifecycle(py::module_& m) {
    m.def(
        "simulate_sparse_order_lifecycles",
        [](
            const CArray<std::uint8_t>& order_side,
            const CArray<std::int64_t>& order_price_tick,
            const CArray<double>& order_quantity,
            const CArray<std::int64_t>& activate_ts_ms,
            const CArray<std::int64_t>& cancel_request_ts_ms,
            const CArray<std::int64_t>& cancel_ack_ts_ms,
            const CArray<std::int64_t>& stop_ts_ms,
            const CArray<std::uint8_t>& seed_status,
            const CArray<double>& seed_qty,
            const CArray<std::int64_t>& seed_best_bid_tick,
            const CArray<std::int64_t>& seed_best_ask_tick,
            const CArray<std::uint8_t>& seed_ambiguous,
            const CArray<std::int64_t>& event_order_index,
            const CArray<std::int64_t>& event_ts_ms,
            const CArray<double>& event_qty_after,
            const CArray<std::uint8_t>& event_code,
            const CArray<std::uint8_t>& event_state_valid,
            const CArray<std::uint8_t>& event_ambiguous,
            const CArray<std::int64_t>& trade_ts_ms,
            const CArray<std::int64_t>& trade_price_tick,
            const CArray<double>& trade_qty,
            const CArray<std::uint8_t>& is_buyer_maker,
            double lot_size,
            double queue_deplete_mult
        ) {
            SparseOrderLifecycleResult result;
            {
                py::gil_scoped_release release;
                result = simulate_sparse_order_lifecycles(
                    view_from_array(order_side),
                    view_from_array(order_price_tick),
                    view_from_array(order_quantity),
                    view_from_array(activate_ts_ms),
                    view_from_array(cancel_request_ts_ms),
                    view_from_array(cancel_ack_ts_ms),
                    view_from_array(stop_ts_ms),
                    view_from_array(seed_status),
                    view_from_array(seed_qty),
                    view_from_array(seed_best_bid_tick),
                    view_from_array(seed_best_ask_tick),
                    view_from_array(seed_ambiguous),
                    view_from_array(event_order_index),
                    view_from_array(event_ts_ms),
                    view_from_array(event_qty_after),
                    view_from_array(event_code),
                    view_from_array(event_state_valid),
                    view_from_array(event_ambiguous),
                    view_from_array(trade_ts_ms),
                    view_from_array(trade_price_tick),
                    view_from_array(trade_qty),
                    view_from_array(is_buyer_maker),
                    lot_size,
                    queue_deplete_mult
                );
            }
            py::dict out = sparse_order_lifecycle_dict(result);
            out["schema_version"] = "sparse_order_lifecycle.v1";
            return out;
        },
        py::arg("order_side"),
        py::arg("order_price_tick"),
        py::arg("order_quantity"),
        py::arg("activate_ts_ms"),
        py::arg("cancel_request_ts_ms"),
        py::arg("cancel_ack_ts_ms"),
        py::arg("stop_ts_ms"),
        py::arg("seed_status"),
        py::arg("seed_qty"),
        py::arg("seed_best_bid_tick"),
        py::arg("seed_best_ask_tick"),
        py::arg("seed_ambiguous"),
        py::arg("event_order_index"),
        py::arg("event_ts_ms"),
        py::arg("event_qty_after"),
        py::arg("event_code"),
        py::arg("event_state_valid"),
        py::arg("event_ambiguous"),
        py::arg("trade_ts_ms"),
        py::arg("trade_price_tick"),
        py::arg("trade_qty"),
        py::arg("is_buyer_maker"),
        py::arg("lot_size") = 0.001,
        py::arg("queue_deplete_mult") = 1.0
    );
}

void bind_active_order_competing_risk_cif(py::module_& m) {
    m.def(
        "update_active_order_competing_risk_cif",
        [](
            const CArray<std::int64_t>& edges,
            const CArray<double>& rates_per_s,
            std::int64_t initial_last_edge,
            double initial_survival,
            const CArray<double>& initial_cif
        ) {
            if (edges.ndim() != 1) {
                throw std::invalid_argument("edges must be a 1D array");
            }
            if (rates_per_s.ndim() != 2 || rates_per_s.shape(1) != 4 ||
                rates_per_s.shape(0) != edges.shape(0)) {
                throw std::invalid_argument("rates_per_s must have shape (n_edges, 4)");
            }
            if (initial_cif.ndim() != 1 || initial_cif.shape(0) != 4) {
                throw std::invalid_argument("initial_cif must have shape (4,)");
            }

            std::vector<std::int64_t> edge_values(
                edges.data(), edges.data() + edges.size()
            );
            std::vector<double> rate_values(
                rates_per_s.data(), rates_per_s.data() + rates_per_s.size()
            );
            std::array<double, kActiveOrderCifCauseCount> cif_values{};
            std::copy_n(initial_cif.data(), cif_values.size(), cif_values.begin());

            ActiveOrderCifBatchResult result;
            {
                py::gil_scoped_release release;
                result = update_active_order_competing_risk_cif(
                    edge_values,
                    rate_values,
                    initial_last_edge,
                    initial_survival,
                    cif_values
                );
            }

            py::dict out;
            out["schema_version"] = "active_order_competing_risk_cif_cpp.v1";
            out["grid_interval_ms"] = 100;
            out["causes"] = py::make_tuple(
                "favorable_fill", "adverse_fill", "cancel_ack", "other_terminal"
            );
            out["edges"] = vector_array(result.edges);
            out["hazards"] = vector_matrix(
                result.hazards, result.edges.size(), kActiveOrderCifCauseCount
            );
            out["no_event_probability"] = vector_array(
                result.no_event_probability
            );
            out["survival_before"] = vector_array(result.survival_before);
            out["survival_after"] = vector_array(result.survival_after);
            out["cif_before"] = vector_matrix(
                result.cif_before, result.edges.size(), kActiveOrderCifCauseCount
            );
            out["cif_after"] = vector_matrix(
                result.cif_after, result.edges.size(), kActiveOrderCifCauseCount
            );
            out["final_last_edge"] = result.final_last_edge;
            out["final_survival"] = result.final_survival;
            py::array_t<double> final_cif(kActiveOrderCifCauseCount);
            std::copy(
                result.final_cif.begin(),
                result.final_cif.end(),
                final_cif.mutable_data()
            );
            out["final_cif"] = std::move(final_cif);
            return out;
        },
        py::arg("edges"),
        py::arg("rates_per_s"),
        py::arg("initial_last_edge"),
        py::arg("initial_survival"),
        py::arg("initial_cif")
    );
}

void bind_order_lifecycle_journal_v2_mirror(py::module_& m) {
    m.attr("ORDER_LIFECYCLE_JOURNAL_V2_MIRROR_ABI_VERSION") =
        std::string(kOrderLifecycleJournalV2MirrorAbi);
    m.def(
        "mirror_order_lifecycle_journal_v2_event_stream",
        [](py::iterable rows_obj) {
            std::vector<OrderLifecycleJournalV2MirrorInput> inputs;
            std::optional<bool> strict_native_schema;
            for (py::handle row_obj : rows_obj) {
                if (!py::isinstance<py::dict>(row_obj)) {
                    throw std::invalid_argument("journal-v2 mirror rows must be dictionaries");
                }
                const auto row = py::reinterpret_borrow<py::dict>(row_obj);
                const auto row_size = static_cast<std::size_t>(py::len(row));
                const bool row_uses_strict_native_schema =
                    row_size == kOrderLifecycleJournalV2Columns.size();
                if (!row_uses_strict_native_schema &&
                    row_size != kHistoricalOrderLifecycleJournalV2Columns.size()) {
                    throw std::invalid_argument("journal-v2 mirror row schema size mismatch");
                }
                if (strict_native_schema.has_value() &&
                    strict_native_schema.value() != row_uses_strict_native_schema) {
                    throw std::invalid_argument("journal-v2 mirror mixes schema generations");
                }
                strict_native_schema = row_uses_strict_native_schema;
                const auto require_column = [&row](std::string_view column) {
                    const py::str key(column.data(), column.size());
                    if (!row.contains(key)) {
                        throw std::invalid_argument(
                            "journal-v2 mirror row is missing column " +
                            std::string(column)
                        );
                    }
                };
                if (row_uses_strict_native_schema) {
                    for (const auto column : kOrderLifecycleJournalV2Columns) {
                        require_column(column);
                    }
                } else {
                    for (const auto column : kHistoricalOrderLifecycleJournalV2Columns) {
                        require_column(column);
                    }
                }

                auto optional_timestamp = [](py::handle value) {
                    if (value.is_none()) {
                        return std::optional<std::int64_t>{};
                    }
                    return std::optional<std::int64_t>{py::cast<std::int64_t>(value)};
                };
                auto optional_bool = [](py::handle value) {
                    if (value.is_none()) {
                        return std::optional<bool>{};
                    }
                    return std::optional<bool>{py::cast<bool>(value)};
                };

                inputs.push_back(OrderLifecycleJournalV2MirrorInput{
                    .event_id = py::cast<std::string>(row["event_id"]),
                    .lifecycle_id = py::cast<std::string>(row["lifecycle_id"]),
                    .client_order_id = py::cast<std::string>(row["client_order_id"]),
                    .lifecycle_sequence = py::cast<std::int64_t>(
                        row["lifecycle_sequence"]
                    ),
                    .lifecycle_event = py::cast<std::string>(row["lifecycle_event"]),
                    .event_visibility_ts_ns = py::cast<std::int64_t>(
                        row["event_visibility_ts_ns"]
                    ),
                    .event_exchange_ts_ns = optional_timestamp(
                        row["event_exchange_ts_ns"]
                    ),
                    .phase_before = py::cast<std::string>(row["phase_before"]),
                    .phase_after = py::cast<std::string>(row["phase_after"]),
                    .event_reason = py::cast<std::string>(row["event_reason"]),
                    .terminal_observation = py::cast<std::string>(
                        row["terminal_observation"]
                    ),
                    .exchange_terminal_reason = py::cast<std::string>(
                        row["exchange_terminal_reason"]
                    ),
                    .local_censor_reason = py::cast<std::string>(
                        row["local_censor_reason"]
                    ),
                    .initial_quantity = py::cast<double>(row["initial_quantity"]),
                    .remaining_quantity_before = py::cast<double>(
                        row["remaining_quantity_before"]
                    ),
                    .remaining_quantity_after = py::cast<double>(
                        row["remaining_quantity_after"]
                    ),
                    .fill_risk_active_after = optional_bool(
                        row["fill_risk_active_after"]
                    ),
                    .simulator_queue_source = row_uses_strict_native_schema
                        ? py::cast<std::string>(row["simulator_queue_source"])
                        : "not_recorded",
                    .exact_queue_path_valid = row_uses_strict_native_schema
                        ? py::cast<bool>(row["exact_queue_path_valid"])
                        : false,
                });
            }

            OrderLifecycleJournalV2MirrorResult result;
            {
                py::gil_scoped_release release;
                result = mirror_order_lifecycle_journal_v2_event_stream(inputs);
            }

            const bool use_strict_native_schema = strict_native_schema.value_or(false);
            const auto column_count = use_strict_native_schema
                ? kOrderLifecycleJournalV2Columns.size()
                : kHistoricalOrderLifecycleJournalV2Columns.size();
            py::tuple columns(column_count);
            for (std::size_t index = 0; index < column_count; ++index) {
                const auto column = use_strict_native_schema
                    ? kOrderLifecycleJournalV2Columns[index]
                    : kHistoricalOrderLifecycleJournalV2Columns[index];
                columns[index] = py::str(column.data(), column.size());
            }
            py::list rows;
            for (const auto& row : result.rows) {
                py::dict projected;
                projected["event_id"] = row.event_id;
                projected["lifecycle_id"] = row.lifecycle_id;
                projected["client_order_id"] = row.client_order_id;
                projected["lifecycle_sequence"] = row.lifecycle_sequence;
                projected["lifecycle_event"] = row.lifecycle_event;
                projected["event_visibility_ts_ns"] = row.event_visibility_ts_ns;
                projected["event_exchange_ts_ns"] = row.event_exchange_ts_ns.has_value()
                    ? py::cast(row.event_exchange_ts_ns.value())
                    : py::none();
                projected["phase_before"] = row.phase_before;
                projected["phase_after"] = row.phase_after;
                projected["event_reason"] = row.event_reason;
                projected["terminal_observation"] = row.terminal_observation;
                projected["exchange_terminal_reason"] =
                    row.exchange_terminal_reason;
                projected["local_censor_reason"] = row.local_censor_reason;
                projected["terminal_policy_route"] = row.terminal_policy_route;
                projected["initial_quantity"] = row.initial_quantity;
                projected["remaining_quantity_before"] =
                    row.remaining_quantity_before;
                projected["remaining_quantity_after"] =
                    row.remaining_quantity_after;
                projected["fill_risk_active_after"] =
                    row.fill_risk_active_after.has_value()
                    ? py::cast(row.fill_risk_active_after.value())
                    : py::none();
                if (use_strict_native_schema) {
                    projected["simulator_queue_source"] =
                        row.simulator_queue_source;
                    projected["exact_queue_path_valid"] =
                        row.exact_queue_path_valid;
                }
                rows.append(std::move(projected));
            }

            py::dict out;
            out["schema_version"] = "order_lifecycle_journal_v2_cpp_mirror_result.v1";
            out["abi_version"] = use_strict_native_schema
                ? std::string(kOrderLifecycleJournalV2MirrorAbi)
                : "order_lifecycle_journal_v2_cpp_event_stream_mirror.v1";
            out["journal_schema_version"] =
                std::string(kOrderLifecycleJournalV2SchemaVersion);
            out["journal_columns"] = std::move(columns);
            out["quantity_contract_id"] =
                std::string(kOrderLifecycleQuantityContractId);
            out["terminal_remainder_abs_tolerance_btc"] =
                kTerminalRemainderAbsToleranceBtc;
            out["rows"] = std::move(rows);
            out["cancel_reject_active_count"] = result.cancel_reject_active_count;
            out["cancel_reject_partially_filled_count"] =
                result.cancel_reject_partially_filled_count;
            out["exchange_terminal_count"] = result.exchange_terminal_count;
            out["local_shutdown_censor_count"] =
                result.local_shutdown_censor_count;
            return out;
        },
        py::arg("rows")
    );
}


}  // namespace narrowgate_cpp
