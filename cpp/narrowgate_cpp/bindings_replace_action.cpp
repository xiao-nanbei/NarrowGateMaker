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

#include "dynamic_fill_hazard.hpp"
#include "active_order_competing_risk_cif.hpp"
#include "causal_v12_1s_features.hpp"
#include "order_lifecycle_journal_v2_mirror.hpp"
#include "live_order_action_plan.hpp"
#include "live_order_state.hpp"
#include "live_cooldown.hpp"
#include "live_policy.hpp"
#include "live_runtime_core.hpp"
#include "order_gateway_core.hpp"
#include "quote_core.hpp"
#include "replace_continuation.hpp"
#include "request_state_features.hpp"
#include "risk_set_expansion.hpp"
#include "sparse_order_lifecycle.hpp"
#include "global_flow.hpp"
#include "streaming_features.hpp"
#include "tick_replay.hpp"
#include "transport_contract.hpp"
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


}  // namespace

void bind_replace_continuation(py::module_& m) {
    py::enum_<ReplaceContinuationPhase>(m, "ReplaceContinuationPhase")
        .value("Empty", ReplaceContinuationPhase::Empty)
        .value("Armed", ReplaceContinuationPhase::Armed)
        .value("Ready", ReplaceContinuationPhase::Ready)
        .value("InFlight", ReplaceContinuationPhase::InFlight);

    py::enum_<ReplaceContinuationEventKind>(
        m,
        "ReplaceContinuationEventKind"
    )
        .value("Arm", ReplaceContinuationEventKind::Arm)
        .value("Publish", ReplaceContinuationEventKind::Publish)
        .value("Decision", ReplaceContinuationEventKind::Decision)
        .value("Drop", ReplaceContinuationEventKind::Drop);

    py::class_<ReplaceContinuationIntent>(m, "ReplaceContinuationIntent")
        .def_readonly("side", &ReplaceContinuationIntent::side)
        .def_readonly(
            "client_order_id",
            &ReplaceContinuationIntent::client_order_id
        )
        .def_readonly("generation", &ReplaceContinuationIntent::generation)
        .def_readonly("armed_ts_ns", &ReplaceContinuationIntent::armed_ts_ns)
        .def_readonly(
            "terminal_visible_ts_ns",
            &ReplaceContinuationIntent::terminal_visible_ts_ns
        );

    py::class_<ReplaceContinuationEvent>(m, "ReplaceContinuationEvent")
        .def_readonly("kind", &ReplaceContinuationEvent::kind)
        .def_readonly("sequence", &ReplaceContinuationEvent::sequence)
        .def_readonly("side", &ReplaceContinuationEvent::side)
        .def_readonly("generation", &ReplaceContinuationEvent::generation)
        .def_readonly(
            "client_order_id",
            &ReplaceContinuationEvent::client_order_id
        )
        .def_readonly("armed_ts_ns", &ReplaceContinuationEvent::armed_ts_ns)
        .def_readonly(
            "terminal_visible_ts_ns",
            &ReplaceContinuationEvent::terminal_visible_ts_ns
        )
        .def_readonly(
            "decision_start_ts_ns",
            &ReplaceContinuationEvent::decision_start_ts_ns
        )
        .def_readonly(
            "decision_latency_ns",
            &ReplaceContinuationEvent::decision_latency_ns
        )
        .def_readonly("reason", &ReplaceContinuationEvent::reason);

    py::class_<ReplaceContinuationTransition>(
        m,
        "ReplaceContinuationTransition"
    )
        .def_readonly("accepted", &ReplaceContinuationTransition::accepted)
        .def_readonly("generation", &ReplaceContinuationTransition::generation)
        .def_readonly("events", &ReplaceContinuationTransition::events);

    py::class_<ReplaceContinuationSideSnapshot>(
        m,
        "ReplaceContinuationSideSnapshot"
    )
        .def_readonly("side", &ReplaceContinuationSideSnapshot::side)
        .def_readonly(
            "generation_counter",
            &ReplaceContinuationSideSnapshot::generation_counter
        )
        .def_readonly(
            "pending_phase",
            &ReplaceContinuationSideSnapshot::pending_phase
        )
        .def_readonly("pending", &ReplaceContinuationSideSnapshot::pending)
        .def_readonly("in_flight", &ReplaceContinuationSideSnapshot::in_flight);

    py::class_<ReplaceContinuationTelemetry>(
        m,
        "ReplaceContinuationTelemetry"
    )
        .def_readonly("arm_count", &ReplaceContinuationTelemetry::arm_count)
        .def_readonly(
            "publish_count",
            &ReplaceContinuationTelemetry::publish_count
        )
        .def_readonly(
            "decision_count",
            &ReplaceContinuationTelemetry::decision_count
        )
        .def_readonly("drop_count", &ReplaceContinuationTelemetry::drop_count)
        .def_readonly(
            "buy_decision_count",
            &ReplaceContinuationTelemetry::buy_decision_count
        )
        .def_readonly(
            "sell_decision_count",
            &ReplaceContinuationTelemetry::sell_decision_count
        )
        .def_readonly(
            "decision_latency_sum_ns",
            &ReplaceContinuationTelemetry::decision_latency_sum_ns
        )
        .def_readonly(
            "decision_latency_max_ns",
            &ReplaceContinuationTelemetry::decision_latency_max_ns
        )
        .def_readonly(
            "pending_count",
            &ReplaceContinuationTelemetry::pending_count
        )
        .def_readonly(
            "in_flight_count",
            &ReplaceContinuationTelemetry::in_flight_count
        )
        .def_readonly(
            "event_sequence",
            &ReplaceContinuationTelemetry::event_sequence
        );

    py::class_<NativeReplaceContinuationState>(
        m,
        "NativeReplaceContinuationState"
    )
        .def(py::init<bool>(), py::arg("enabled") = true)
        .def_property_readonly(
            "enabled",
            &NativeReplaceContinuationState::enabled
        )
        .def(
            "arm",
            [](NativeReplaceContinuationState& self,
               Side side,
               const std::string& client_order_id,
               std::int64_t armed_ts_ns,
               bool can_post) {
                py::gil_scoped_release release;
                return self.arm(side, client_order_id, armed_ts_ns, can_post);
            },
            py::arg("side"),
            py::arg("client_order_id"),
            py::arg("armed_ts_ns"),
            py::arg("can_post") = true
        )
        .def(
            "publish",
            [](NativeReplaceContinuationState& self,
               Side side,
               const std::string& client_order_id,
               std::uint64_t generation,
               std::int64_t terminal_visible_ts_ns) {
                py::gil_scoped_release release;
                return self.publish(
                    side,
                    client_order_id,
                    generation,
                    terminal_visible_ts_ns
                );
            },
            py::arg("side"),
            py::arg("client_order_id"),
            py::arg("generation"),
            py::arg("terminal_visible_ts_ns")
        )
        .def(
            "clear_exact",
            [](NativeReplaceContinuationState& self,
               Side side,
               const std::string& client_order_id,
               std::uint64_t generation,
               std::int64_t event_ts_ns,
               const std::string& reason) {
                py::gil_scoped_release release;
                return self.clear_exact(
                    side,
                    client_order_id,
                    generation,
                    event_ts_ns,
                    reason
                );
            },
            py::arg("side"),
            py::arg("client_order_id"),
            py::arg("generation") = 0,
            py::arg("event_ts_ns") = 0,
            py::arg("reason") = "cleared"
        )
        .def(
            "clear_side",
            [](NativeReplaceContinuationState& self,
               Side side,
               const std::string& reason) {
                py::gil_scoped_release release;
                return self.clear_side(side, reason);
            },
            py::arg("side"),
            py::arg("reason") = "side_superseded"
        )
        .def(
            "clear_unready",
            [](NativeReplaceContinuationState& self,
               Side side,
               const std::string& client_order_id,
               std::uint64_t generation,
               const std::string& reason) {
                py::gil_scoped_release release;
                return self.clear_unready(
                    side,
                    client_order_id,
                    generation,
                    reason
                );
            },
            py::arg("side"),
            py::arg("client_order_id"),
            py::arg("generation"),
            py::arg("reason") = "terminal_before_callback"
        )
        .def(
            "take_ready",
            [](NativeReplaceContinuationState& self) {
                py::gil_scoped_release release;
                return self.take_ready();
            }
        )
        .def(
            "finalize_decision",
            [](NativeReplaceContinuationState& self,
               Side side,
               std::uint64_t generation,
               std::int64_t decision_start_ts_ns) {
                py::gil_scoped_release release;
                return self.finalize_decision(
                    side,
                    generation,
                    decision_start_ts_ns
                );
            },
            py::arg("side"),
            py::arg("generation"),
            py::arg("decision_start_ts_ns")
        )
        .def(
            "drop_in_flight",
            [](NativeReplaceContinuationState& self,
               Side side,
               std::uint64_t generation,
               const std::string& reason) {
                py::gil_scoped_release release;
                return self.drop_in_flight(side, generation, reason);
            },
            py::arg("side"),
            py::arg("generation"),
            py::arg("reason")
        )
        .def(
            "clear_all",
            [](NativeReplaceContinuationState& self, const std::string& reason) {
                py::gil_scoped_release release;
                return self.clear_all(reason);
            },
            py::arg("reason") = "clear_all"
        )
        .def(
            "side_snapshot",
            &NativeReplaceContinuationState::side_snapshot,
            py::arg("side")
        )
        .def("telemetry", &NativeReplaceContinuationState::telemetry)
        .def_property_readonly_static(
            "cache_line_bytes",
            [](py::object) {
                return NativeReplaceContinuationState::cache_line_bytes();
            }
        )
        .def_property_readonly_static(
            "max_client_order_id_bytes",
            [](py::object) {
                return NativeReplaceContinuationState::max_client_order_id_bytes();
            }
        );
}

void bind_live_order_action_plan(py::module_& m) {
    py::enum_<LivePlannerOrderState>(m, "LivePlannerOrderState")
        .value("Empty", LivePlannerOrderState::Empty)
        .value("PendingNew", LivePlannerOrderState::PendingNew)
        .value("Active", LivePlannerOrderState::Active)
        .value("PendingCancel", LivePlannerOrderState::PendingCancel)
        .value("Terminal", LivePlannerOrderState::Terminal);

    py::enum_<LiveOrderAction>(m, "LiveOrderAction")
        .value("None", LiveOrderAction::None)
        .value("Keep", LiveOrderAction::Keep)
        .value("Place", LiveOrderAction::Place)
        .value("CancelFirst", LiveOrderAction::CancelFirst)
        .value("Pending", LiveOrderAction::Pending)
        .value("Pause", LiveOrderAction::Pause)
        .value("SkipFilter", LiveOrderAction::SkipFilter)
        .value("RouteDisabled", LiveOrderAction::RouteDisabled)
        .value("Invalid", LiveOrderAction::Invalid);

    py::enum_<LiveFinalOrderPlanStatus>(m, "LiveFinalOrderPlanStatus")
        .value("Ok", LiveFinalOrderPlanStatus::Ok)
        .value("InvalidInput", LiveFinalOrderPlanStatus::InvalidInput)
        .value("InvalidTickPrice", LiveFinalOrderPlanStatus::InvalidTickPrice)
        .value("PostOnlyBuyCrosses", LiveFinalOrderPlanStatus::PostOnlyBuyCrosses)
        .value("PostOnlySellCrosses", LiveFinalOrderPlanStatus::PostOnlySellCrosses)
        .value("InvalidActionPlan", LiveFinalOrderPlanStatus::InvalidActionPlan);

    py::class_<LiveSideOrderActionPlan>(m, "LiveSideOrderActionPlan")
        .def_readonly(
            "target_price_ticks",
            &LiveSideOrderActionPlan::target_price_ticks
        )
        .def_readonly(
            "target_quantity_lots",
            &LiveSideOrderActionPlan::target_quantity_lots
        )
        .def_readonly(
            "inventory_room_lots",
            &LiveSideOrderActionPlan::inventory_room_lots
        )
        .def_readonly(
            "position_value_room_lots",
            &LiveSideOrderActionPlan::position_value_room_lots
        )
        .def_readonly(
            "existing_remaining_lots",
            &LiveSideOrderActionPlan::existing_remaining_lots
        )
        .def_readonly("reason_mask", &LiveSideOrderActionPlan::reason_mask)
        .def_readonly("action", &LiveSideOrderActionPlan::action)
        .def_property_readonly(
            "action_name",
            [](const LiveSideOrderActionPlan& self) {
                return live_order_action_name(self.action);
            }
        )
        .def_readonly(
            "exposure_increasing",
            &LiveSideOrderActionPlan::exposure_increasing
        )
        .def_readonly(
            "can_post_after_inventory",
            &LiveSideOrderActionPlan::can_post_after_inventory
        )
        .def_readonly("can_post", &LiveSideOrderActionPlan::can_post)
        .def_readonly("needs_update", &LiveSideOrderActionPlan::needs_update)
        .def_readonly("force_update", &LiveSideOrderActionPlan::force_update)
        .def_readonly("order_active", &LiveSideOrderActionPlan::order_active)
        .def_readonly("order_pending", &LiveSideOrderActionPlan::order_pending)
        .def_readonly("filter_valid", &LiveSideOrderActionPlan::filter_valid)
        .def_readonly(
            "cancel_existing",
            &LiveSideOrderActionPlan::cancel_existing
        );

    py::class_<LiveDualOrderActionPlan>(m, "LiveDualOrderActionPlan")
        .def_readonly("buy", &LiveDualOrderActionPlan::buy)
        .def_readonly("sell", &LiveDualOrderActionPlan::sell);

    py::class_<LiveFinalOrderPlan>(m, "LiveFinalOrderPlan")
        .def_readonly("orders", &LiveFinalOrderPlan::orders)
        .def_readonly("bid_price", &LiveFinalOrderPlan::bid_price)
        .def_readonly("ask_price", &LiveFinalOrderPlan::ask_price)
        .def_readonly(
            "p3_buy_floor_price",
            &LiveFinalOrderPlan::p3_buy_floor_price
        )
        .def_readonly(
            "p3_sell_floor_price",
            &LiveFinalOrderPlan::p3_sell_floor_price
        )
        .def_readonly("status", &LiveFinalOrderPlan::status)
        .def_readonly("p3_bid_changed", &LiveFinalOrderPlan::p3_bid_changed)
        .def_readonly("p3_ask_changed", &LiveFinalOrderPlan::p3_ask_changed)
        .def_readonly(
            "bid_active_floor_unsafe",
            &LiveFinalOrderPlan::bid_active_floor_unsafe
        )
        .def_readonly(
            "ask_active_floor_unsafe",
            &LiveFinalOrderPlan::ask_active_floor_unsafe
        );

    m.def(
        "compute_live_order_action_plan",
        [](py::sequence context_values,
           py::sequence replace_values,
           py::sequence buy_values,
           py::sequence sell_values) {
            if (py::len(context_values) != 9 ||
                py::len(replace_values) != 8 ||
                py::len(buy_values) != 9 ||
                py::len(sell_values) != 9) {
                throw std::invalid_argument(
                    "live order action planner compact input length mismatch"
                );
            }

            LiveOrderPlannerContext context{};
            if (py::cast<std::uint32_t>(context_values[0]) != 2U) {
                throw std::invalid_argument(
                    "live order action planner context ABI mismatch"
                );
            }
            context.inventory = py::cast<double>(context_values[1]);
            context.max_inventory = py::cast<double>(context_values[2]);
            context.max_position_value = py::cast<double>(context_values[3]);
            context.mid = py::cast<double>(context_values[4]);
            context.lot_size = py::cast<double>(context_values[5]);
            context.inventory_lots =
                py::cast<std::int64_t>(context_values[6]);
            context.min_quantity_lots =
                py::cast<std::int64_t>(context_values[7]);
            context.requote_threshold_bps =
                py::cast<double>(context_values[8]);

            LiveOrderReplaceConfig replace{};
            replace.tick_size = py::cast<double>(replace_values[0]);
            replace.lot_size = py::cast<double>(replace_values[1]);
            replace.min_notional = py::cast<double>(replace_values[2]);
            replace.add_min_price_change_ticks =
                py::cast<double>(replace_values[3]);
            replace.reducing_min_price_change_ticks =
                py::cast<double>(replace_values[4]);
            replace.add_min_interval_ms =
                py::cast<double>(replace_values[5]);
            replace.reducing_min_interval_ms =
                py::cast<double>(replace_values[6]);
            replace.flags = py::cast<std::uint8_t>(replace_values[7]);

            const auto read_side = [](const py::sequence& values) {
                LiveSideOrderActionInput input{};
                input.target_price_ticks =
                    py::cast<std::int64_t>(values[0]);
                input.desired_quantity_lots =
                    py::cast<std::int64_t>(values[1]);
                input.exposure_probe_quantity_lots =
                    py::cast<std::int64_t>(values[2]);
                input.existing_remaining_lots =
                    py::cast<std::int64_t>(values[4]);
                input.order_state = py::cast<LivePlannerOrderState>(values[7]);
                input.flags = py::cast<std::uint8_t>(values[8]);
                input.order_age_ms = py::cast<double>(values[5]);
                if ((input.flags & LiveOrderSideInputUseProvidedNeedsUpdate) != 0) {
                    input.target_price = py::cast<double>(values[3]);
                    input.provided_price_delta_ticks =
                        py::cast<double>(values[6]);
                } else {
                    input.existing_price_ticks =
                        py::cast<std::int64_t>(values[3]);
                    input.order_ttl_ms = py::cast<double>(values[6]);
                }
                return input;
            };

            return compute_live_order_action_plan(
                context,
                replace,
                read_side(buy_values),
                read_side(sell_values)
            );
        },
        py::arg("context_values"),
        py::arg("replace_values"),
        py::arg("buy_values"),
        py::arg("sell_values")
    );

    m.def(
        "compute_live_final_order_plan",
        [](py::sequence context_values,
           py::sequence replace_values,
           py::sequence boundary_values,
           py::sequence buy_values,
           py::sequence sell_values) {
            if (py::len(context_values) != 9 ||
                py::len(replace_values) != 8 ||
                py::len(boundary_values) != 9 ||
                py::len(buy_values) != 9 ||
                py::len(sell_values) != 9) {
                throw std::invalid_argument(
                    "live final order planner compact input length mismatch"
                );
            }
            if (py::cast<std::uint32_t>(context_values[0]) != 2U) {
                throw std::invalid_argument(
                    "live final order planner context ABI mismatch"
                );
            }
            if (py::cast<std::uint32_t>(boundary_values[0]) != 1U) {
                throw std::invalid_argument(
                    "live final order planner boundary ABI mismatch"
                );
            }

            LiveOrderPlannerContext context{};
            context.inventory = py::cast<double>(context_values[1]);
            context.max_inventory = py::cast<double>(context_values[2]);
            context.max_position_value = py::cast<double>(context_values[3]);
            context.mid = py::cast<double>(context_values[4]);
            context.lot_size = py::cast<double>(context_values[5]);
            context.inventory_lots =
                py::cast<std::int64_t>(context_values[6]);
            context.min_quantity_lots =
                py::cast<std::int64_t>(context_values[7]);
            context.requote_threshold_bps =
                py::cast<double>(context_values[8]);

            LiveOrderReplaceConfig replace{};
            replace.tick_size = py::cast<double>(replace_values[0]);
            replace.lot_size = py::cast<double>(replace_values[1]);
            replace.min_notional = py::cast<double>(replace_values[2]);
            replace.add_min_price_change_ticks =
                py::cast<double>(replace_values[3]);
            replace.reducing_min_price_change_ticks =
                py::cast<double>(replace_values[4]);
            replace.add_min_interval_ms =
                py::cast<double>(replace_values[5]);
            replace.reducing_min_interval_ms =
                py::cast<double>(replace_values[6]);
            replace.flags = py::cast<std::uint8_t>(replace_values[7]);

            LiveFinalOrderBoundary boundary{};
            boundary.bid_price = py::cast<double>(boundary_values[1]);
            boundary.ask_price = py::cast<double>(boundary_values[2]);
            boundary.best_bid = py::cast<double>(boundary_values[3]);
            boundary.best_ask = py::cast<double>(boundary_values[4]);
            boundary.p3_delta_star = py::cast<double>(boundary_values[5]);
            boundary.bid_existing_price = py::cast<double>(boundary_values[6]);
            boundary.ask_existing_price = py::cast<double>(boundary_values[7]);
            boundary.flags = py::cast<std::uint8_t>(boundary_values[8]);

            const auto read_side = [](const py::sequence& values) {
                LiveSideOrderActionInput input{};
                input.target_price_ticks =
                    py::cast<std::int64_t>(values[0]);
                input.desired_quantity_lots =
                    py::cast<std::int64_t>(values[1]);
                input.exposure_probe_quantity_lots =
                    py::cast<std::int64_t>(values[2]);
                input.existing_remaining_lots =
                    py::cast<std::int64_t>(values[4]);
                input.order_state = py::cast<LivePlannerOrderState>(values[7]);
                input.flags = py::cast<std::uint8_t>(values[8]);
                input.order_age_ms = py::cast<double>(values[5]);
                if ((input.flags & LiveOrderSideInputUseProvidedNeedsUpdate) != 0) {
                    input.target_price = py::cast<double>(values[3]);
                    input.provided_price_delta_ticks =
                        py::cast<double>(values[6]);
                } else {
                    input.existing_price_ticks =
                        py::cast<std::int64_t>(values[3]);
                    input.order_ttl_ms = py::cast<double>(values[6]);
                }
                return input;
            };

            return compute_live_final_order_plan(
                context,
                replace,
                boundary,
                read_side(buy_values),
                read_side(sell_values)
            );
        },
        py::arg("context_values"),
        py::arg("replace_values"),
        py::arg("boundary_values"),
        py::arg("buy_values"),
        py::arg("sell_values")
    );

    m.attr("LIVE_ORDER_ACTION_PLAN_CONTEXT_BYTES") = py::int_(
        sizeof(LiveOrderPlannerContext)
    );
    m.attr("LIVE_ORDER_ACTION_PLAN_CONTEXT_ABI") = py::int_(2);
    m.attr("LIVE_ORDER_ACTION_PLAN_REPLACE_BYTES") = py::int_(
        sizeof(LiveOrderReplaceConfig)
    );
    m.attr("LIVE_ORDER_ACTION_PLAN_SIDE_INPUT_BYTES") = py::int_(
        sizeof(LiveSideOrderActionInput)
    );
    m.attr("LIVE_ORDER_ACTION_PLAN_SIDE_RESULT_BYTES") = py::int_(
        sizeof(LiveSideOrderActionPlan)
    );
    m.attr("LIVE_ORDER_ACTION_PLAN_DUAL_RESULT_BYTES") = py::int_(
        sizeof(LiveDualOrderActionPlan)
    );
    m.attr("LIVE_FINAL_ORDER_PLAN_BOUNDARY_BYTES") = py::int_(
        sizeof(LiveFinalOrderBoundary)
    );
    m.attr("LIVE_FINAL_ORDER_PLAN_RESULT_BYTES") = py::int_(
        sizeof(LiveFinalOrderPlan)
    );
    m.attr("LIVE_FINAL_ORDER_PLAN_BOUNDARY_ABI") = py::int_(1);
    m.attr("LIVE_FINAL_ORDER_BOUNDARY_FLAG_P3_SIDE_BBO_FLOOR") = py::int_(
        static_cast<std::uint8_t>(LiveFinalOrderBoundaryP3SideBboFloor)
    );
    m.attr("LIVE_ORDER_SIDE_FLAG_ROUTE_ALLOWED") = py::int_(
        static_cast<std::uint8_t>(LiveOrderSideInputRouteAllowed)
    );
    m.attr("LIVE_ORDER_SIDE_FLAG_ALLOW_POST") = py::int_(
        static_cast<std::uint8_t>(LiveOrderSideInputAllowPost)
    );
    m.attr("LIVE_ORDER_SIDE_FLAG_ALLOW_EXPOSURE") = py::int_(
        static_cast<std::uint8_t>(LiveOrderSideInputAllowExposureIncrease)
    );
    m.attr("LIVE_ORDER_SIDE_FLAG_FORCE_UPDATE") = py::int_(
        static_cast<std::uint8_t>(LiveOrderSideInputForceUpdate)
    );
    m.attr("LIVE_ORDER_SIDE_FLAG_USE_PROVIDED_NEEDS_UPDATE") = py::int_(
        static_cast<std::uint8_t>(
            LiveOrderSideInputUseProvidedNeedsUpdate
        )
    );
    m.attr("LIVE_ORDER_SIDE_FLAG_PROVIDED_NEEDS_UPDATE") = py::int_(
        static_cast<std::uint8_t>(LiveOrderSideInputProvidedNeedsUpdate)
    );
    m.attr("LIVE_ORDER_REASON_THROTTLE_PRICE") = py::int_(
        static_cast<std::uint32_t>(LiveOrderPlanReasonThrottlePrice)
    );
    m.attr("LIVE_ORDER_REASON_THROTTLE_AGE") = py::int_(
        static_cast<std::uint32_t>(LiveOrderPlanReasonThrottleAge)
    );
    m.attr("LIVE_ORDER_REASON_PENDING_LIFECYCLE") = py::int_(
        static_cast<std::uint32_t>(LiveOrderPlanReasonPendingLifecycle)
    );
    m.attr("LIVE_ORDER_REASON_CONFIGURED_CANCEL_FIRST") = py::int_(
        static_cast<std::uint32_t>(
            LiveOrderPlanReasonConfiguredCancelFirst
        )
    );
    m.attr("LIVE_ORDER_REPLACE_FLAG_PENDING_COALESCE") = py::int_(
        static_cast<std::uint8_t>(LiveOrderReplacePendingCoalesce)
    );
    m.attr("LIVE_ORDER_REPLACE_FLAG_CANCEL_FIRST_EXPOSURE") = py::int_(
        static_cast<std::uint8_t>(
            LiveOrderReplaceCancelFirstExposureIncrease
        )
    );
    m.attr("NATIVE_LIVE_ORDER_ACTION_PLAN_AVAILABLE") = py::bool_(true);
    m.attr("NATIVE_LIVE_FINAL_ORDER_PLAN_AVAILABLE") = py::bool_(true);
}


}  // namespace narrowgate_cpp
